#!/usr/bin/env python3
"""Matched real-FUSE fsmonitor history benchmark; dry-run by default.

Default 12,000 same-path chmod mutations exceed the proposed 4,096-generation
window without repeated payload I/O. Old, recent and current-token queries are
separate git-vfs fsmonitor v2 processes; durations include process startup and
owner-service communication. No additive timing claim, Cargo, retries or network.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import statistics
import time
import traceback

import benchmark_vfs_iteration as common


JOURNAL_FIELDS = ('generation', 'retained_floor', 'retained_records', 'retained_bytes',
                  'dirty_paths', 'dirty_path_bytes', 'dirty_wire_bytes')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def parse_response(data):
    fields = data.split(b'\0')
    require(len(fields) >= 2 and fields[-1] == b'' and fields[0], 'malformed fsmonitor v2 response')
    token = fields[0].decode('ascii')
    paths = [field.decode('utf-8') for field in fields[1:-1]]
    require(all(paths) and len(paths) == len(set(paths)), 'empty or duplicate fsmonitor paths')
    return token, paths


class JournalCell(common.Cell):
    def __init__(self, directory, binary, git, fusermount, args):
        super().__init__(directory, binary, git, fusermount, args, 'same-path-journal')
        self.result.update(mutation_seconds=[], queries={'old': [], 'recent': [], 'current': []})

    def prepare(self):
        self.directory.mkdir()
        (self.directory / 'home').mkdir()
        self.mount.mkdir()
        source = self.directory / 'source'
        self.git_run('init', source)
        self.git_run('-C', source, 'config', 'user.name', 'Fixture')
        self.git_run('-C', source, 'config', 'user.email', 'fixture@example.invalid')
        self.env.update({'GIT_AUTHOR_DATE': '2000-01-01T00:00:00Z',
                         'GIT_COMMITTER_DATE': '2000-01-01T00:00:00Z'})
        for name, data in {'tracked.txt': b'unchanged base\n', 'changed.txt': b'original contents\n',
                           'deleted.txt': b'deletion must stay masked\n'}.items():
            (source / name).write_bytes(data)
        self.git_run('-C', source, 'add', '.')
        self.git_run('-C', source, 'commit', '-m', 'journal fixture')
        self.git_run('clone', '--bare', source, self.repo)
        self.result['fixture_commit'] = self.git_run('-C', source, 'rev-parse', 'HEAD').decode().strip()
        self.save()

    def query(self, token):
        started = time.monotonic()
        data = common.run([self.binary, '--git', self.git, 'fsmonitor', '--overlay', self.overlay, '2', token],
                          self.env, timeout=self.args.query_budget)
        elapsed = time.monotonic() - started
        new_token, paths = parse_response(data)
        return {'seconds': elapsed, 'token': new_token, 'paths': paths, 'response_bytes': len(data)}

    def execute(self):
        self.prepare()
        self.start()
        changed = self.mount / 'changed.txt'
        changed.write_bytes(b'edited contents survive remount\n')
        (self.mount / 'deleted.txt').unlink()
        old = self.query('')['token']
        self.result['initial_token'] = old
        self.sample('before_mutations')
        recent = None
        recent_at = self.args.mutations - self.args.recent_generations
        for index in range(self.args.mutations):
            if index == recent_at:
                recent = self.query(old)['token']
                self.result['recent_token'] = recent
            started = time.monotonic()
            os.chmod(changed, 0o755 if index % 2 == 0 else 0o644)
            self.result['mutation_seconds'].append(time.monotonic() - started)
            if (index + 1) % 1024 == 0:
                self.save()
        require(recent is not None and recent != old, 'recent token was not captured after mutations')
        self.sample('after_mutations')
        current = None
        for _index in range(self.args.query_repeats):
            response = self.query(old)
            self.result['queries']['old'].append(response)
            allowed = [{'changed.txt'}]
            if self.result['arm'] == 'candidate':
                allowed.append({'/'})
            require(set(response['paths']) in allowed, 'old-token query lost or invented changed paths')
            if current is None:
                current = response['token']
            require(response['token'] == current, 'query unexpectedly mutated journal generation')
        self.result['old_token_expired'] = self.result['queries']['old'][0]['paths'] == ['/']
        for _index in range(self.args.query_repeats):
            response = self.query(recent)
            self.result['queries']['recent'].append(response)
            require(set(response['paths']) == {'changed.txt'}, 'recent query is incomplete or invalidated unnecessarily')
            require(response['token'] == current, 'recent query returned inconsistent generation')
        for _index in range(self.args.query_repeats):
            response = self.query(current)
            self.result['queries']['current'].append(response)
            require(response['paths'] == [] and response['token'] == current, 'current token must return exact empty changes')
        self.result['current_token'] = current
        expected_status = [' M changed.txt', ' D deleted.txt']
        require(self.git_run('-C', self.mount, 'status', '--porcelain').decode().splitlines() == expected_status,
                'Git status differs from edited/deleted fixture')
        self.stop()
        self.start()
        self.sample('after_remount')
        require(changed.read_bytes() == b'edited contents survive remount\n', 'linked edit lost after remount')
        require(not (self.mount / 'deleted.txt').exists(), 'deleted base reappeared after remount')
        require((self.mount / 'tracked.txt').read_bytes() == b'unchanged base\n', 'unchanged base altered')
        require(self.git_run('-C', self.mount, 'status', '--porcelain').decode().splitlines() == expected_status,
                'Git status changed after remount')
        after_remount = self.query(current)
        self.result['remount_current_query'] = after_remount
        require(after_remount['paths'] == [] and after_remount['token'] == current,
                'remount must preserve journal identity and current generation')
        self.result['correctness'] = {'old_token_complete_or_explicit_full_invalidation': True,
            'recent_token_exact_same_path': True, 'current_token_exact_empty': True,
            'queries_do_not_mutate_generation': True, 'linked_edit_survives_remount': True,
            'base_deletion_mask_survives_remount': True, 'unchanged_base_preserved': True,
            'git_status_exact': True, 'journal_identity_and_generation_survive_remount': True}
        self.stop()
        self.result['status'] = 'complete'
        self.save()

    def collect_stats(self):
        states = {name: {'reports': [], 'parse_errors': []} for name in ('workload', 'remount')}
        for index, log in enumerate(self.logs):
            state = states[('workload', 'remount')[index]]
            state['log'] = log.name
            for number, line in enumerate(log.read_text(errors='replace').splitlines(), 1):
                if 'Overlay state: ' not in line:
                    continue
                try:
                    report = json.loads(line.split('Overlay state: ', 1)[1])
                    if not isinstance(report, dict):
                        raise ValueError('not an object')
                    state['reports'].append(report)
                except ValueError as error:
                    state['parse_errors'].append({'line': number, 'error': str(error)})
        self.result['overlay_shutdown_states'] = states
        required = self.args.require_candidate_stats and self.result['arm'] == 'candidate'
        proof = {'required': required, 'passed': None, 'checks': {},
                 'limits': {'records': 8192, 'retained_bytes': 4194304, 'generations': 4096,
                            'dirty_paths': 4096, 'dirty_path_bytes': 1048576,
                            'dirty_wire_bytes': 8388352}}
        self.result['mechanism_validation'] = proof
        if not required:
            return
        valid = all(len(value['reports']) == 1 and not value['parse_errors'] and
                    all(type(value['reports'][0].get(field)) is int and
                        0 <= value['reports'][0][field] <= 18446744073709551615
                        for field in JOURNAL_FIELDS)
                    for value in states.values())
        proof['checks']['exactly_one_valid_journal_report_per_session'] = valid
        if valid:
            journals = [value['reports'][0] for value in states.values()]
            proof['checks'].update({
                'history_record_bound': all(0 < state['retained_records'] <= 8192 for state in journals),
                'history_byte_bound': all(state['retained_bytes'] <= 4194304 for state in journals),
                'retained_floor_valid': all(state['retained_floor'] <= state['generation'] for state in journals),
                'history_generation_bound': all(state['generation'] - state['retained_floor'] <= 4096 for state in journals),
                'dirty_path_count_bound': all(state['dirty_paths'] <= 4096 for state in journals),
                'dirty_raw_byte_bound': all(state['dirty_path_bytes'] <= 1048576 for state in journals),
                'dirty_wire_byte_bound': all(state['dirty_wire_bytes'] <= 8388352 for state in journals),
                'same_path_dirty_set_exact': all(state['dirty_paths'] == 2 and
                    state['dirty_path_bytes'] == 22 for state in journals),
            })
            try:
                initial_prefix, initial_generation = self.result['initial_token'].rsplit(':', 1)
                current_prefix, current_generation = self.result['current_token'].rsplit(':', 1)
                if not initial_generation.isascii() or not initial_generation.isdigit() or not current_generation.isascii() or not current_generation.isdigit():
                    raise ValueError('non-numeric generation')
                initial_generation, current_generation = int(initial_generation), int(current_generation)
                generations = current_generation - initial_generation
                proof['observed_generation_delta'] = generations
                proof['checks']['token_generations_valid'] = initial_prefix == current_prefix and generations >= 0
                proof['checks']['statistics_match_current_token'] = all(
                    state['generation'] == current_generation for state in journals)
                expired = generations > 4096
                proof['expected_old_token_expired'] = expired
                old = self.result['queries']['old']
                recent = self.result['queries']['recent']
                current = self.result['queries']['current']
                proof['checks']['old_token_expiry_matches_window'] = bool(old) and all(
                    response['paths'] == (['/'] if expired else ['changed.txt']) for response in old)
                proof['checks']['recent_token_exact_changes'] = bool(recent) and all(
                    response['paths'] == ['changed.txt'] and response['token'] == self.result['current_token']
                    for response in recent)
                proof['checks']['current_token_exact_empty'] = bool(current) and all(
                    response['paths'] == [] and response['token'] == self.result['current_token']
                    for response in current)
                proof['checks']['remount_did_not_advance_generation'] = journals[0]['generation'] == journals[1]['generation']
            except (KeyError, ValueError, TypeError):
                proof['checks']['token_generations_valid'] = False
        proof['passed'] = all(proof['checks'].values())
        if not proof['passed']:
            self.result['status'] = 'failed'
            self.result['mechanism_error'] = 'candidate journal retention proof failed or statistics missing'


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--baseline-source', default='unknown (no source claim supplied)')
    parser.add_argument('--candidate-source', default='unknown (no source claim supplied)')
    parser.add_argument('--git', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--mutations', type=int, default=12000)
    parser.add_argument('--recent-generations', type=int, default=8)
    parser.add_argument('--query-repeats', type=int, default=8)
    parser.add_argument('--query-budget', type=float, default=10)
    parser.add_argument('--mount-budget', type=float, default=30)
    parser.add_argument('--cell-budget', type=int, default=300)
    parser.add_argument('--require-candidate-stats', action='store_true',
                        help='require complete bounded journal statistics and token-expiry proof')
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if not (1 <= args.trials <= 20 and 3 <= args.mutations <= 100000 and
            1 <= args.recent_generations < args.mutations and 1 <= args.query_repeats <= 128 and
            1 <= args.query_budget <= 60 and 1 <= args.mount_budget <= 120 and 30 <= args.cell_budget <= 3600):
        parser.error('bounds: trials1..20; mutations3..100000; recent1..mutations-1; repeats1..128; query1..60s; mount1..120s; cell30..3600s')
    args.output, args.git = args.output.resolve(), args.git.resolve(strict=True)
    binaries = {name: getattr(args, name).resolve(strict=True) for name in ('baseline', 'candidate')}
    for path in [args.git, *binaries.values()]:
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f'not an executable file: {path}')
    if args.output.exists():
        parser.error('--output must be fresh and nonexistent')
    helper = Path(common.__file__).resolve()
    result = {'schema': 1, 'status': 'dry-run', 'kind': 'actual-FUSE same-path journal retention; not Bazel latency',
        'cells': [], 'config': {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        'binaries': {name: {'path': str(path), 'sha256': common.sha256(path),
                            'source_claim': getattr(args, f'{name}_source')} for name, path in binaries.items()},
        'git': {'path': str(args.git), 'sha256': common.sha256(args.git)},
        'host': {'system': platform.system(), 'release': platform.release(), 'machine': platform.machine(),
                 'python': platform.python_version(), 'cpu_count': os.cpu_count()},
        'harness_sha256': common.sha256(Path(__file__)), 'helper_sha256': common.sha256(helper),
        'timing_contract': 'each query is an independent complete subprocess wall interval, not additive phases; chmod syscall timings recorded separately',
        'history_contract': 'baseline old token returns exact path; candidate may return exact path or explicit slash invalidation; recent/current exact on both'}
    if not args.run:
        print(json.dumps(result, indent=2))
        return 0
    fusermount = shutil.which('fusermount3')
    if not fusermount or not Path('/dev/fuse').exists():
        parser.error('actual runs require fusermount3 and /dev/fuse')
    args.output.mkdir(parents=True)
    frozen = args.output / 'binaries'
    frozen.mkdir()
    for name, path in binaries.items():
        destination = frozen / name
        shutil.copyfile(path, destination)
        destination.chmod(0o555)
        require(common.sha256(destination) == result['binaries'][name]['sha256'], 'binary changed while freezing')
        binaries[name] = destination
    shutil.copyfile(__file__, args.output / 'benchmark_vfs_journal.py')
    shutil.copyfile(helper, args.output / 'benchmark_vfs_iteration.py')
    result['status'] = 'running'
    common.checkpoint(args.output / 'results.json', result)
    signal.signal(signal.SIGALRM, common.deadline_handler)
    for trial in range(args.trials):
        order = list(binaries) if trial % 2 == 0 else list(reversed(binaries))
        for arm in order:
            directory = args.output / f'trial-{trial + 1}-{arm}'
            cell = JournalCell(directory, binaries[arm], args.git, fusermount, args)
            cell.result.update(trial=trial + 1, arm=arm)
            result['cells'].append(cell.result)
            print(f'start {directory.name}', flush=True)
            signal.alarm(args.cell_budget)
            try:
                cell.execute()
            except BaseException as error:
                cell.result.update(status='failed', error_type=type(error).__name__, error=str(error), traceback=traceback.format_exc())
            finally:
                signal.alarm(0)
                try:
                    cell.stop()
                except BaseException as error:
                    cell.result.update(status='failed', cleanup_error=f'{type(error).__name__}: {error}')
                cell.collect_stats()
                if directory.exists():
                    cell.save()
                common.checkpoint(args.output / 'results.json', result)
            print(f'finish {directory.name}: {cell.result["status"]}', flush=True)
            if cell.result['status'] != 'complete':
                result['status'] = 'failed'
                common.checkpoint(args.output / 'results.json', result)
                return 1
    result['status'] = 'complete'
    result['summary'] = {}
    for arm in binaries:
        cells = [cell for cell in result['cells'] if cell['arm'] == arm]
        summary = result['summary'][arm] = {'candidate_expired_old_token_trials':
            sum(cell['old_token_expired'] for cell in cells), 'query_cell_medians': {}}
        for kind in ('old', 'recent', 'current'):
            values = [statistics.median(query['seconds'] for query in cell['queries'][kind]) for cell in cells]
            summary['query_cell_medians'][kind] = {'median_seconds': statistics.median(values),
                'min_seconds': min(values), 'max_seconds': max(values), 'trials': len(values)}
        for label in ('before_mutations', 'after_mutations', 'after_remount'):
            summary[label] = {}
            for metric in ('rss_bytes', 'overlay_allocated_bytes', 'overlay_file_bytes'):
                values = [next(sample[metric] for sample in cell['samples'] if sample['label'] == label) for cell in cells]
                summary[label][metric] = {'median': statistics.median(values), 'min': min(values), 'max': max(values)}
    common.checkpoint(args.output / 'results.json', result)
    print(json.dumps(result['summary'], indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
