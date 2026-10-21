#!/usr/bin/env python3
"""Matched actual-FUSE large-file range I/O; dry-run by default.

Example:
  python3 tools/benchmark_vfs_ranges.py --baseline /path/old/git-vfs \
    --candidate bazel-bin/crates/workspace-fuse/git-vfs-bin --git /path/git --output artifacts/ranges \
    --baseline-source c0687ee --candidate-source working-tree --trials 3 --run

First copy-up is separate from warm 4KiB reads/appends. No network, cache drops,
Cargo, or retries. Payload generation, correctness checks, opening/closing handles,
and fixed RSS settling intervals are outside operation timers. Both arms use fresh
private local repositories and overlays. RSS/disk include Fjall/WAL/cache overhead.
"""
from __future__ import annotations

import argparse
import hashlib
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

SESSIONS = ('initial_io', 'remount_after_io', 'remount_after_sparse')
WORK_FIELDS = ('payload_read_bytes', 'payload_write_bytes', 'payload_copy_bytes',
               'payload_hash_bytes', 'payload_syncs', 'recovered_operations')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


class RangeCell(common.Cell):
    def __init__(self, directory, binary, git, fusermount, args):
        super().__init__(directory, binary, git, fusermount, args, 'large-file-ranges')
        self.result['operation_seconds'] = {'warm_reads': [], 'appends': []}
        self.result['operation_offsets'] = {'warm_reads': [], 'appends': []}

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
        (source / 'tracked.txt').write_bytes(b'unchanged base\n')
        self.expected = bytearray(common.payload(90001, self.args.file_bytes))
        (source / 'large.bin').write_bytes(self.expected)
        # A distinct untouched base file is used for zero-truncation correctness.
        (source / 'truncate-base.bin').write_bytes(common.payload(90002, self.args.file_bytes))
        self.result['base_payload_sha256'] = hashlib.sha256(self.expected).hexdigest()
        self.git_run('-C', source, 'add', '.')
        self.git_run('-C', source, 'commit', '-m', 'range I/O fixture')
        self.git_run('clone', '--bare', source, self.repo)
        self.result['fixture_commit'] = self.git_run('-C', source, 'rev-parse', 'HEAD').decode().strip()
        self.save()

    def verify_file(self):
        actual = (self.mount / 'large.bin').read_bytes()
        require(actual == self.expected, 'large.bin byte mismatch')
        require((self.mount / 'tracked.txt').read_bytes() == b'unchanged base\n', 'base file changed')
        return hashlib.sha256(actual).hexdigest()

    def execute(self):
        self.prepare()
        patch = common.payload(90003, self.args.range_bytes)
        append_blocks = [common.payload(91000 + index, self.args.range_bytes)
                         for index in range(self.args.iterations)]
        self.result['append_sequence_sha256'] = hashlib.sha256(b''.join(append_blocks)).hexdigest()
        self.start()
        self.sample('before_first_copyup')
        fd = os.open(self.mount / 'large.bin', os.O_RDWR)
        try:
            offset = self.args.file_bytes // 2
            started = time.monotonic()
            written = os.pwrite(fd, patch, offset)
            self.result['first_copyup_seconds'] = time.monotonic() - started
            require(written == len(patch), 'short initial copy-up write')
            self.expected[offset:offset + len(patch)] = patch
            require(os.pread(fd, len(patch), offset) == patch, 'copy-up bytes mismatch')
            self.result['copyup'] = {'offset': offset, 'bytes': len(patch),
                                     'includes_first_base_hydration': True}
            self.sample('after_first_copyup')
            for index in range(self.args.iterations):
                offset = (index * 104729) % (len(self.expected) - self.args.range_bytes + 1)
                started = time.monotonic()
                data = os.pread(fd, self.args.range_bytes, offset)
                self.result['operation_seconds']['warm_reads'].append(time.monotonic() - started)
                self.result['operation_offsets']['warm_reads'].append(offset)
                require(data == self.expected[offset:offset + self.args.range_bytes], 'range read mismatch')
            self.sample('after_warm_reads')
            for index, block in enumerate(append_blocks):
                offset = len(self.expected)
                started = time.monotonic()
                written = os.pwrite(fd, block, offset)
                self.result['operation_seconds']['appends'].append(time.monotonic() - started)
                self.result['operation_offsets']['appends'].append(offset)
                require(written == len(block), 'short append write')
                self.expected.extend(block)
                # Verify each returned write outside its timer, without a whole-file read.
                require(os.pread(fd, len(block), offset) == block, 'append bytes mismatch')
                if (index + 1) % 16 == 0:
                    self.save()
        finally:
            os.close(fd)
        self.result['after_io_sha256'] = self.verify_file()
        self.sample('after_appends')
        self.stop()
        self.start()
        self.sample('after_io_remount')
        require(self.verify_file() == self.result['after_io_sha256'], 'I/O remount hash changed')
        self.result['correctness'] = {'range_reads_match': True, 'appends_match': True,
                                     'linked_io_survives_remount': True}
        # Shrink then extend must expose zeros, never old truncated bytes.
        path = self.mount / 'large.bin'
        fd = os.open(path, os.O_RDWR)
        try:
            shrink = self.args.file_bytes // 2
            os.ftruncate(fd, shrink)
            del self.expected[shrink:]
            extend = self.args.file_bytes + self.args.range_bytes
            os.ftruncate(fd, extend)
            self.expected.extend(bytes(extend - len(self.expected)))
            require(os.pread(fd, extend - shrink, shrink) == bytes(extend - shrink),
                    'truncate extension exposed old bytes')
            sparse_offset = extend + 2 * self.args.range_bytes
            marker = common.payload(99999, self.args.range_bytes)
            require(os.pwrite(fd, marker, sparse_offset) == len(marker), 'short sparse write')
            self.expected.extend(bytes(sparse_offset - len(self.expected)))
            self.expected.extend(marker)
            require(os.pread(fd, 2 * self.args.range_bytes, extend) == bytes(2 * self.args.range_bytes),
                    'sparse hole was not zero-filled')
        finally:
            os.close(fd)
        with (self.mount / 'truncate-base.bin').open('wb'):
            pass
        require((self.mount / 'truncate-base.bin').stat().st_size == 0, 'base zero-truncate failed')
        self.result['after_sparse_sha256'] = self.verify_file()
        self.result['correctness'].update(shrink_extend_zero_fill=True, sparse_write_zero_fill=True,
                                          untouched_base_zero_truncate=True)
        self.stop()
        self.start()
        self.sample('after_sparse_remount')
        require(self.verify_file() == self.result['after_sparse_sha256'], 'sparse remount hash changed')
        require((self.mount / 'truncate-base.bin').read_bytes() == b'', 'zero-truncate remount mismatch')
        status = self.git_run('-C', self.mount, 'status', '--porcelain').decode().splitlines()
        require(sorted(status) == [' M large.bin', ' M truncate-base.bin'], 'unexpected Git status')
        self.result['correctness'].update(sparse_and_truncate_survive_remount=True,
                                          git_status_exact=True, unchanged_base_preserved=True)
        self.stop()
        self.result['final_payload_sha256'] = hashlib.sha256(self.expected).hexdigest()
        self.result['phase_seconds'] = {name: sum(values)
                                       for name, values in self.result['operation_seconds'].items()}
        self.result['status'] = 'complete'
        self.save()

    def collect_stats(self):
        # Retain complete optional dictionaries, including newly added work counters.
        # Missing reports are explicit, not interpreted as zero mechanism work.
        sessions = {name: {'reports': [], 'parse_errors': []} for name in SESSIONS}
        for index, log in enumerate(self.logs):
            entry = sessions[SESSIONS[index]]
            entry['log'] = log.name
            for number, line in enumerate(log.read_text(errors='replace').splitlines(), 1):
                if 'Overlay state: ' not in line:
                    continue
                try:
                    report = json.loads(line.split('Overlay state: ', 1)[1])
                    if not isinstance(report, dict):
                        raise ValueError('not an object')
                    entry['reports'].append(report)
                except (ValueError, json.JSONDecodeError) as error:
                    entry['parse_errors'].append({'line': number, 'error': str(error)})
        self.result['overlay_shutdown_states'] = sessions
        required = self.args.require_candidate_stats and self.result['arm'] == 'candidate'
        proof = {'required': required, 'passed': None, 'checks': {},
                 'scope': 'Initial-session cumulative payload work includes copy-up, warm reads, '
                          'appends, per-operation correctness reads and one full-file verification; '
                          'not isolated timed-operation counters.'}
        self.result['mechanism_validation'] = proof
        if not required:
            return
        valid = all(len(value['reports']) == 1 and not value['parse_errors'] and
                    all(type(value['reports'][0].get(field)) is int and
                        value['reports'][0][field] >= 0
                        for field in (*common.STAT_FIELDS, *WORK_FIELDS))
                    for value in sessions.values())
        proof['checks']['exactly_one_valid_report_per_session'] = valid
        if valid:
            initial = sessions['initial_io']['reports'][0]
            sparse = sessions['remount_after_io']['reports'][0]
            final = sessions['remount_after_sparse']['reports'][0]
            write_bytes = (self.args.iterations + 1) * self.args.range_bytes
            read_budget = 2 * self.args.file_bytes + 4 * write_bytes
            proof['budgets'] = {'expected_write_bytes': write_bytes,
                'maximum_copy_bytes': self.args.file_bytes,
                'minimum_read_bytes': self.args.iterations * self.args.range_bytes,
                'maximum_read_bytes': read_budget,
                'initial_linked_payload_bytes': self.args.file_bytes +
                    self.args.iterations * self.args.range_bytes,
                'sparse_linked_payload_bytes': self.args.file_bytes + 4 * self.args.range_bytes}
            proof['checks'].update({
                'writes_match_requested_ranges': initial['payload_write_bytes'] == write_bytes,
                'at_most_one_full_copyup': initial['payload_copy_bytes'] <= self.args.file_bytes,
                'reads_cover_requested_ranges': initial['payload_read_bytes'] >=
                    self.args.iterations * self.args.range_bytes,
                'reads_within_scope_budget': initial['payload_read_bytes'] <= read_budget,
                'file_backed_live_bytes_accounted': initial['live_payload_bytes'] ==
                    proof['budgets']['initial_linked_payload_bytes'],
                'persisted_sparse_live_bytes_accounted': sparse['live_payload_bytes'] ==
                    final['live_payload_bytes'] == proof['budgets']['sparse_linked_payload_bytes'],
                'linked_bytes_survive_remount': self.result.get('correctness', {}).get(
                    'linked_io_survives_remount', False) and self.result.get('correctness', {}).get(
                    'sparse_and_truncate_survive_remount', False),
            })
        proof['passed'] = all(proof['checks'].values())
        if not proof['passed']:
            self.result['status'] = 'failed'
            self.result['mechanism_error'] = 'candidate range-I/O proof failed or statistics missing'


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--baseline-source', default='unknown (no source claim supplied)')
    parser.add_argument('--candidate-source', default='unknown (no source claim supplied)')
    parser.add_argument('--git', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--file-bytes', type=int, default=16 * 1024 * 1024)
    parser.add_argument('--range-bytes', type=int, default=4096)
    parser.add_argument('--iterations', type=int, default=64)
    parser.add_argument('--mount-budget', type=float, default=15)
    parser.add_argument('--cell-budget', type=int, default=300)
    parser.add_argument('--require-candidate-stats', action='store_true',
                        help='fail candidate cells without complete bounded range-I/O work proof')
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if not (1 <= args.trials <= 20 and 1 <= args.iterations <= 4096 and
            4096 <= args.file_bytes <= 128 * 1024 * 1024 and
            1 <= args.range_bytes <= min(1024 * 1024, args.file_bytes // 2) and
            args.iterations * args.range_bytes <= 64 * 1024 * 1024 and
            1 <= args.mount_budget <= 60 and 30 <= args.cell_budget <= 3600):
        parser.error('bounds: trials1..20; iterations1..4096; file4KiB..128MiB; range1..min(1MiB,file/2); appends<=64MiB; mount1..60s; cell30..3600s')
    args.output, args.git = args.output.resolve(), args.git.resolve(strict=True)
    binaries = {name: getattr(args, name).resolve(strict=True) for name in ('baseline', 'candidate')}
    for path in [args.git, *binaries.values()]:
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f'not an executable file: {path}')
    if args.output.exists():
        parser.error('--output must be fresh and nonexistent')
    helper = Path(common.__file__).resolve()
    result = {'schema': 1, 'kind': 'local actual-FUSE large-file ranges; not Bazel latency',
        'status': 'dry-run', 'cells': [],
        'config': {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        'binaries': {name: {'path': str(path), 'sha256': common.sha256(path),
                            'source_claim': getattr(args, f'{name}_source')} for name, path in binaries.items()},
        'git': {'path': str(args.git), 'sha256': common.sha256(args.git)},
        'host': {'system': platform.system(), 'release': platform.release(), 'machine': platform.machine(),
                 'python': platform.python_version(), 'cpu_count': os.cpu_count()},
        'harness_sha256': common.sha256(Path(__file__)), 'helper_sha256': common.sha256(helper),
        'mount_session_order': list(SESSIONS),
        'timing_contract': 'one pwrite first copy-up; warm pread/pwrite per operation; no timed verification, generation, open/close, fsync, or mount',
        'work_counter_contract': 'optional shutdown dictionaries retained verbatim; absence is unknown, not zero'}
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
    shutil.copyfile(__file__, args.output / 'benchmark_vfs_ranges.py')
    shutil.copyfile(helper, args.output / 'benchmark_vfs_iteration.py')
    result['status'] = 'running'
    common.checkpoint(args.output / 'results.json', result)
    signal.signal(signal.SIGALRM, common.deadline_handler)
    for trial in range(args.trials):
        order = list(binaries) if trial % 2 == 0 else list(reversed(binaries))
        for arm in order:
            directory = args.output / f'trial-{trial + 1}-{arm}'
            cell = RangeCell(directory, binaries[arm], args.git, fusermount, args)
            cell.result.update(trial=trial + 1, arm=arm)
            result['cells'].append(cell.result)
            print(f'start trial={trial + 1} arm={arm}', flush=True)
            signal.alarm(args.cell_budget)
            try:
                cell.execute()
            except BaseException as error:
                cell.result.update(status='failed', error_type=type(error).__name__,
                                   error=str(error), traceback=traceback.format_exc())
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
        result['summary'][arm] = {}
        for phase in ('first_copyup', 'warm_reads', 'appends'):
            values = [cell['first_copyup_seconds'] if phase == 'first_copyup' else cell['phase_seconds'][phase]
                      for cell in cells]
            result['summary'][arm][phase] = {'median_seconds': statistics.median(values),
                'min_seconds': min(values), 'max_seconds': max(values), 'trials': len(values)}
        result['summary'][arm]['resources'] = {}
        labels = [sample['label'] for sample in cells[0]['samples']]
        for label in labels:
            result['summary'][arm]['resources'][label] = {}
            for metric in ('rss_bytes', 'overlay_allocated_bytes', 'overlay_file_bytes', 'visible_payload_bytes'):
                values = [next(sample[metric] for sample in cell['samples'] if sample['label'] == label)
                          for cell in cells]
                result['summary'][arm]['resources'][label][metric] = {
                    'median': statistics.median(values), 'min': min(values), 'max': max(values)}
        result['summary'][arm]['mount_seconds'] = {
            session: {'median': statistics.median(values), 'min': min(values), 'max': max(values)}
            for index, session in enumerate(SESSIONS)
            if (values := [cell['mount_seconds'][index] for cell in cells])}
    common.checkpoint(args.output / 'results.json', result)
    print(json.dumps(result['summary'], indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
