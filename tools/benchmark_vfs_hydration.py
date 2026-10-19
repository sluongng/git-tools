#!/usr/bin/env python3
"""Controlled native-Git stall through actual Linux FUSE; dry-run by default.

This measures scheduling and cancellation, not network speed. Both arms perform
real Git object fetches from a fresh file:// partial-clone fixture after release.
No Cargo commands, credentials, shared repositories or global cache operations.
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
import subprocess
import sys
import time
import traceback

import benchmark_vfs_iteration as common


CONTENT_FIELDS = ('outstanding_requests', 'reply_bytes', 'cache_resident_bytes',
    'cache_pinned_bytes', 'cache_inflight_bytes', 'cache_total_bytes', 'cache_peak_bytes',
    'cache_files', 'coalesced_requests', 'foreground_promotions', 'cache_hits',
    'cache_bypasses', 'fetch_batches', 'fetched_objects', 'local_bytes',
    'rejected_requests', 'callback_panics', 'worker_failures', 'queued_local', 'queued_large',
    'queued_foreground', 'queued_speculative', 'inflight_oids')
DRAINED_FIELDS = ('outstanding_requests', 'reply_bytes', 'cache_resident_bytes',
    'cache_pinned_bytes', 'cache_inflight_bytes', 'cache_total_bytes', 'cache_files',
    'queued_local', 'queued_large', 'queued_foreground', 'queued_speculative', 'inflight_oids')


def identity(pid):
    """Linux identity, parsed after the final ')' because comm may contain spaces."""
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return {'pid': int(pid), 'pgid': int(fields[2]), 'start_ticks': int(fields[19]),
                'state': fields[0]}
    except (FileNotFoundError, ProcessLookupError):
        return None


def same_process(record):
    current = identity(record['pid'])
    return current is not None and all(current[key] == record[key]
                                       for key in ('pid', 'pgid', 'start_ticks'))


def group_members(pgid):
    # Observation only: never use this numeric PGID as a signaling target.
    members = []
    for path in Path('/proc').iterdir():
        if path.name.isdigit():
            current = identity(int(path.name))
            if current and current['pgid'] == pgid:
                members.append(current)
    return members


def known_baseline_native_failure(result, record):
    return (result.get('arm') == 'baseline' and result.get('workload') == 'cancellation'
            and result.get('mode') == 'demand' and record.get('kind') == 'legacy'
            and record == result.get('blocked_native')
            and result.get('cancellation', {}).get('bounded_cancellation_passed') is False)


def child_operation(operation, path, result_path):
    started = time.monotonic()
    record = {'operation': operation, 'started_monotonic': started}
    try:
        if operation == 'stat':
            record['size'] = path.stat().st_size
        else:
            with path.open('rb') as stream:
                result_path.with_suffix('.opened').touch()
                data = stream.read()
            record.update({'size': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
        record['status'] = 'complete'
    except BaseException as error:
        record.update({'status': 'failed', 'error_type': type(error).__name__,
                       'error': str(error)})
    record['finished_monotonic'] = time.monotonic()
    record['operation_seconds'] = record['finished_monotonic'] - started
    common.checkpoint(result_path, record)
    return int(record['status'] != 'complete')


WRAPPER = r'''#!/usr/bin/env python3
import json, os, pathlib, sys, time
root = pathlib.Path(__file__).parent
args = sys.argv[1:]
is_fetch = 'fetch' in args and '--stdin' in args
is_legacy = args[-2:] == ['cat-file', '--batch'] and os.environ.get('GIT_NO_LAZY_FETCH') != '1'
if (root / 'armed').exists():
    fields = pathlib.Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()
    record = {'pid': os.getpid(), 'pgid': os.getpgrp(), 'start_ticks': int(fields[19]),
              'kind': 'fetch' if is_fetch else 'legacy' if is_legacy else 'other',
              'args': args, 'started_monotonic': time.monotonic()}
    if is_fetch:
        # The fixture's explicit-want input is bounded. Preserve it for real Git.
        data = sys.stdin.buffer.read(65537)
        if len(data) > 65536:
            raise RuntimeError('fixture fetch wants exceed fixed bound')
        record['wanted_oids'] = data.decode('ascii').splitlines()
        import tempfile
        stream = tempfile.TemporaryFile()
        stream.write(data); stream.seek(0)
        os.dup2(stream.fileno(), 0)
    with (root / 'wrapper.jsonl').open('a') as log:
        log.write(json.dumps(record) + '\n')
    if is_fetch or is_legacy:
        staged = root / ('blocked.' + str(os.getpid()) + '.tmp')
        staged.write_text(json.dumps(record))
        try:
            os.link(staged, root / 'blocked.json')
        except FileExistsError:
            pass
        finally:
            staged.unlink()
        while not (root / 'release').exists():
            time.sleep(.01)
real = json.loads((root / 'real-git.json').read_text())
os.execv(real, [real, *args])
'''


class Cell(common.Cell):
    def __init__(self, directory, binary, git, fusermount, args, workload, mode):
        super().__init__(directory, binary, git, fusermount, args, workload)
        self.mode = mode
        self.result.update({'mode': mode, 'probes': {}, 'children': []})
        self.children = []
        self.native_handles = []
        self.wrapper = directory / 'controlled-git'
        self.payloads = {'local.txt': common.payload(0, 32768),
                         'missing.txt': common.payload(1, 32768),
                         'BUILD.bazel': b'exports_files(["local.txt", "missing.txt"])\n'}

    def prepare(self):
        self.directory.mkdir()
        (self.directory / 'home').mkdir()
        self.mount.mkdir()
        source = self.directory / 'source'
        self.git_run('init', '--initial-branch=main', source)
        for key, value in [('user.name', 'Fixture'), ('user.email', 'fixture@example.invalid'),
                           ('uploadpack.allowFilter', 'true'),
                           ('uploadpack.allowAnySHA1InWant', 'true'),
                           ('transfer.advertiseObjectInfo', 'true')]:
            self.git_run('-C', source, 'config', key, value)
        self.env.update({'GIT_AUTHOR_DATE': '2000-01-01T00:00:00Z',
                         'GIT_COMMITTER_DATE': '2000-01-01T00:00:00Z'})
        for name, data in self.payloads.items():
            (source / name).write_bytes(data)
        self.git_run('-C', source, 'add', '.')
        self.git_run('-C', source, 'commit', '-m', 'hydration fixture')
        self.git_run('clone', '--bare', '--filter=blob:none', source.as_uri(), self.repo)
        oids = {name: self.git_run('-C', source, 'rev-parse', f'HEAD:{name}').decode().strip()
                for name in self.payloads}
        self.result['fixture'] = {'commit': self.git_run('-C', source, 'rev-parse', 'HEAD').decode().strip(),
                                  'oids': oids, 'expected_explicit_missing_wants': [oids['missing.txt']],
                                  'unique_blobs': 3, 'prehydrated_blobs': 2,
                                  'payload_sha256': {name: hashlib.sha256(data).hexdigest()
                                                     for name, data in self.payloads.items()}}
        for name in ('local.txt', 'BUILD.bazel'):
            if self.git_run('--git-dir', self.repo, 'cat-file', 'blob', oids[name]) != self.payloads[name]:
                raise RuntimeError('fixture hydration differs from source')
        # Explicit no-fetch verification: a broken filter fixture must not pass.
        checked = subprocess.run([str(self.git), '--git-dir', str(self.repo), 'cat-file',
                                  '--batch-check=%(objectname) %(objecttype)'],
                                 input=(oids['missing.txt'] + '\n').encode(),
                                 env={**self.env, 'GIT_NO_LAZY_FETCH': '1'},
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, check=True)
        if checked.stdout != f'{oids["missing.txt"]} missing\n'.encode():
            raise RuntimeError('missing fixture blob was unexpectedly hydrated')
        self.result['metadata_warm_report'] = json.loads(common.run(
            [self.binary, '--git', self.git, 'warm-sizes', '--repo', self.repo,
             '--size-source', 'git'], self.env))
        checked_after = subprocess.run(checked.args, input=(oids['missing.txt'] + '\n').encode(),
                                       env={**self.env, 'GIT_NO_LAZY_FETCH': '1'},
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       timeout=10, check=True)
        if checked_after.stdout != checked.stdout:
            raise RuntimeError('size warmup unexpectedly hydrated fixture content')
        self.result['missing_after_metadata_warmup'] = True
        self.env['GIT_TRACE2_EVENT'] = str(self.directory / 'native-git.trace2.jsonl')
        self.wrapper.write_text(WRAPPER)
        self.wrapper.chmod(0o755)
        (self.directory / 'real-git.json').write_text(json.dumps(str(self.git)))
        # Arm only after source creation, clone, local hydration and metadata warmup.
        (self.directory / 'armed').touch()
        self.save()

    def start(self):
        path = self.directory / 'mount.log'
        self.logs.append(path)
        self.log = path.open('wb')
        command = [self.binary, '--git', self.wrapper, 'mount', '--repo', self.repo,
                   '--overlay', self.overlay, '--size-source', 'git']
        if self.mode == 'prefetch':
            command += ['--prefetch-bazel', '--prefetch-no-discover',
                        '--prefetch-include', 'missing.txt']
        command.append(self.mount)
        started = time.monotonic()
        self.proc = subprocess.Popen([str(x) for x in command], env=self.env,
                                     stdout=self.log, stderr=self.log, start_new_session=True)
        self.result['owned_pids'] = [self.proc.pid]
        self.wait(lambda: os.path.ismount(self.mount), self.args.mount_budget, 'mount readiness')
        self.result['mount_seconds'].append(time.monotonic() - started)

    def wait(self, predicate, budget, description):
        deadline = time.monotonic() + budget
        while not predicate():
            if self.proc is not None and self.proc.poll() is not None:
                raise RuntimeError(f'daemon exited during {description}: {self.proc.returncode}')
            if time.monotonic() >= deadline:
                raise TimeoutError(f'{description} budget exceeded')
            time.sleep(.01)

    def spawn_probe(self, label, operation, filename):
        result_path = self.directory / f'{label}.json'
        log = (self.directory / f'{label}.log').open('wb')
        started = time.monotonic()
        child = subprocess.Popen([sys.executable, str(self.args.output / 'benchmark_vfs_hydration.py'), '--child',
                                  operation, str(self.mount / filename), str(result_path)],
                                 env=self.env, stdout=log, stderr=log, start_new_session=True)
        log.close()
        handle = os.pidfd_open(child.pid)
        record = {'label': label, 'pid': child.pid, 'started_monotonic': started}
        self.result['children'].append(record)
        item = (child, handle, result_path, record)
        self.children.append(item)
        return item

    def blocked(self):
        marker = self.directory / 'blocked.json'
        self.wait(marker.exists, self.args.mount_budget, 'controlled native stall')
        record = json.loads(marker.read_text())
        if not same_process(record):
            raise RuntimeError('controlled native job disappeared before capture')
        self.native_handles.append((record, os.pidfd_open(record['pid'])))
        self.result['blocked_native'] = record
        self.save()

    def reader_is_blocked(self, item):
        child = item[0]
        if child.poll() is not None:
            raise RuntimeError('missing reader exited before the controlled stall')
        # Verify an actual read syscall on this mount, not merely child startup.
        syscall = Path(f'/proc/{child.pid}/syscall').read_text().split()
        read_number = {'x86_64': 0, 'aarch64': 63, 'riscv64': 63}[platform.machine()]
        if not syscall or syscall[0] != str(read_number):
            return False
        descriptor = int(syscall[1], 16)
        try:
            return Path(os.readlink(f'/proc/{child.pid}/fd/{descriptor}')) == self.mount / 'missing.txt'
        except FileNotFoundError:
            return False

    def finish_probe(self, item, expected, allow_error=False):
        child, _handle, path, record = item
        child.wait(timeout=10)
        if not path.exists():
            raise RuntimeError(f'probe produced no result: {record["label"]}')
        value = json.loads(path.read_text())
        value['spawn_to_completion_seconds'] = value['finished_monotonic'] - record['started_monotonic']
        record.update({'exit_code': child.returncode, 'result': value})
        if allow_error:
            if child.returncode == 0:
                raise RuntimeError('blocked reader unexpectedly succeeded during cancellation')
            return
        if child.returncode != 0 or value['status'] != 'complete':
            raise RuntimeError(f'probe failed: {record["label"]}')
        if value['size'] != len(expected):
            raise RuntimeError('probe size mismatch')
        if 'sha256' in value and value['sha256'] != hashlib.sha256(expected).hexdigest():
            raise RuntimeError('probe content mismatch')

    def responsiveness(self):
        missing = self.spawn_probe('missing-read', 'read', 'missing.txt')
        self.blocked()
        self.wait(lambda: self.reader_is_blocked(missing), 5, 'missing reader in FUSE read')
        self.result['missing_reader_verified_in_read'] = True
        probes = [('local-stat', self.spawn_probe('local-stat', 'stat', 'local.txt')),
                  ('local-read', self.spawn_probe('local-read', 'read', 'local.txt'))]
        deadline = time.monotonic() + self.args.probe_budget
        while time.monotonic() < deadline and any(item[0].poll() is None for _, item in probes):
            time.sleep(.005)
        release_time = time.monotonic()
        for label, item in probes:
            child, _, path, record = item
            completed = child.poll() is not None and path.exists()
            self.result['probes'][label] = {'completed_before_release': completed,
                'budget_seconds': self.args.probe_budget}
            if completed:
                value = json.loads(path.read_text())
                self.result['probes'][label]['spawn_to_completion_seconds'] = (
                    value['finished_monotonic'] - record['started_monotonic'])
        self.result['barrier_release_monotonic'] = release_time
        self.save()
        (self.directory / 'release').touch()
        self.finish_probe(missing, self.payloads['missing.txt'])
        for _, item in probes:
            self.finish_probe(item, self.payloads['local.txt'])
        status = common.run([self.git, '-C', self.mount, 'status', '--porcelain'], self.env)
        self.result['git_status_clean'] = status == b''
        if status:
            raise RuntimeError('fixture Git status is dirty')
        responsive = all(value['completed_before_release'] and
                         value['spawn_to_completion_seconds'] <= self.args.probe_budget
                         for value in self.result['probes'].values())
        self.result['responsiveness_passed'] = responsive
        if self.result['arm'] == 'candidate' and not responsive:
            raise RuntimeError('candidate local operations did not finish within the stall budget')

    def cancellation(self):
        missing = self.spawn_probe('cancelled-read', 'read', 'missing.txt')
        self.blocked()
        self.wait(lambda: self.reader_is_blocked(missing), 5, 'cancelled reader in FUSE read')
        self.result['missing_reader_verified_in_read'] = True
        started = time.monotonic()
        self.proc.send_signal(signal.SIGINT)
        deadline = started + self.args.shutdown_budget
        native = self.result['blocked_native']
        while time.monotonic() < deadline:
            daemon_done = self.proc.poll() is not None
            reader_done = missing[0].poll() is not None
            native_members = group_members(native['pgid'])
            if daemon_done and reader_done and not native_members:
                break
            time.sleep(.01)
        native_members = group_members(native['pgid'])
        record = {'seconds': time.monotonic() - started,
                  'budget_seconds': self.args.shutdown_budget,
                  'daemon_exited': self.proc.poll() is not None,
                  'reader_exited': missing[0].poll() is not None,
                  'daemon_exit_code': self.proc.poll(),
                  'release_created': (self.directory / 'release').exists(),
                  'native_group_members_at_deadline': native_members}
        record['bounded_cancellation_passed'] = (record['daemon_exited'] and record['reader_exited']
                                                and not native_members and record['daemon_exit_code'] == 0
                                                and record['seconds'] <= self.args.shutdown_budget)
        record['observation_censored_at_budget'] = (not record['bounded_cancellation_passed'] and
                                                   record['seconds'] >= self.args.shutdown_budget)
        self.result['cancellation'] = record
        self.save()
        if not record['bounded_cancellation_passed']:
            if not (known_baseline_native_failure(self.result, native) and record['daemon_exited']
                    and record['reader_exited'] and record['daemon_exit_code'] == 0 and native_members):
                raise TimeoutError('bounded cancellation did not terminate daemon, reader and native process group')
            record['known_baseline_failure'] = 'demand legacy reader outlives daemon; exact pidfd cleanup required'
        self.finish_probe(missing, b'', allow_error=True)
        if record['daemon_exit_code'] != 0:
            raise RuntimeError('daemon failed during cancellation')

    def collect(self):
        path = self.directory / 'wrapper.jsonl'
        calls = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        self.result['native_wrapper_calls'] = calls
        trace_path = self.directory / 'native-git.trace2.jsonl'
        if trace_path.exists():
            trace = [json.loads(line) for line in trace_path.read_text().splitlines()]
            self.result['native_trace2_summary'] = {
                'events': len(trace),
                'fetch_command_count': sum(event.get('event') == 'cmd_name' and
                                           event.get('name') == 'fetch' for event in trace),
                'file': trace_path.name}
        self.result['explicit_fetch_calls'] = sum(call['kind'] == 'fetch' for call in calls)
        self.result['legacy_reader_calls'] = sum(call['kind'] == 'legacy' for call in calls)
        # Optional candidate statistics retain only actual reports, never invented zeros.
        reports = {}
        for log in self.logs:
            for line in log.read_text(errors='replace').splitlines():
                for prefix in ('Content state: ', 'Hydration state: ', 'Overlay state: '):
                    if line.startswith(prefix):
                        reports.setdefault(prefix.strip(': '), []).append(json.loads(line[len(prefix):]))
        self.result['optional_shutdown_reports'] = reports
        remaining = []
        for call in calls:
            if call['kind'] not in ('fetch', 'legacy'):
                continue
            members = group_members(call['pgid'])
            if members:
                remaining.append({'original_identity': call, 'members': members})
        self.result['native_groups_remaining'] = remaining
        if remaining:
            raise RuntimeError('native process groups remain; retained identities, no numeric PGID signaling')
        self.validate_candidate_stats()

    def validate_candidate_stats(self):
        required = self.args.require_candidate_stats and self.result['arm'] == 'candidate'
        proof = {'required': required, 'passed': None, 'checks': {},
                 'shared_content_cache_limit_bytes': 64 * 1024 * 1024,
                 'separate_reply_admission_limit_bytes': 64 * 1024 * 1024,
                 'scope': 'Final Content state is collected after owned workers join. Shared '
                          'cache peak is separate from reply admission bytes; all final charges drain. '
                          'Controlled three-blob fixture is not an overload or cache-thrash benchmark.'}
        self.result['mechanism_validation'] = proof
        if not required:
            return
        reports = self.result.get('optional_shutdown_reports', {}).get('Content state', [])
        valid = (len(reports) == 1 and isinstance(reports[0], dict) and
                 all(type(reports[0].get(field)) is int and
                     0 <= reports[0][field] <= 18446744073709551615 for field in CONTENT_FIELDS))
        proof['checks']['exactly_one_valid_content_report'] = valid
        if valid:
            content = reports[0]
            fetches = [call for call in self.result['native_wrapper_calls'] if call['kind'] == 'fetch']
            expected_wants = self.result['fixture']['expected_explicit_missing_wants']
            proof['checks'].update({
                'all_shutdown_charges_and_queues_drained': all(content[field] == 0 for field in DRAINED_FIELDS),
                'shared_cache_peak_within_limit': content['cache_peak_bytes'] <= proof['shared_content_cache_limit_bytes'],
                'no_callback_panics': content['callback_panics'] == 0,
                'no_worker_failures': content['worker_failures'] == 0,
                'uncongested_fixture_no_rejections': content['rejected_requests'] == 0,
                'one_explicit_fetch_with_exact_missing_oid': len(fetches) == 1 and
                    fetches[0].get('wanted_oids') == expected_wants,
                'no_legacy_implicit_fetch_reader': self.result['legacy_reader_calls'] == 0,
                'no_candidate_forced_cleanup': not self.result.get('native_forced_cleanups') and
                    not any(child.get('forced_cleanup') for child in self.result['children']) and
                    not any(shutdown['forced'] for shutdown in self.result['shutdowns']),
            })
            if self.mode == 'prefetch' and self.workload == 'responsiveness':
                proof['checks']['prefetch_and_demand_share_oid_work'] = content['coalesced_requests'] >= 1
                proof['promotion_contract'] = 'Queued work may be promoted; an already-active RPC need not report a promotion.'
            if self.workload == 'responsiveness':
                proof['checks'].update({
                    'single_missing_object_fetch_completed': content['fetch_batches'] == 1 and content['fetched_objects'] == 1,
                    'local_requests_complete_before_release': self.result.get('responsiveness_passed') is True,
                })
            else:
                proof['checks'].update({
                    'bounded_cancellation_includes_native_group': self.result.get('cancellation', {}).get(
                        'bounded_cancellation_passed') is True,
                    'cancellation_does_not_claim_unfinished_fetches': content['fetch_batches'] <= 1 and
                        content['fetched_objects'] <= 1,
                })
        proof['passed'] = all(proof['checks'].values())
        if not proof['passed']:
            self.result['status'] = 'failed'
            self.result['mechanism_error'] = 'candidate hydration scheduling/cancellation proof failed or statistics missing'

    def stop(self):
        errors = []
        try:
            super().stop()
        except BaseException as error:
            errors.append(str(error))
        for child, handle, _path, record in self.children:
            try:
                if child.poll() is None:
                    # pidfd pins the exact process; never signal a recycled numeric PID/PGID.
                    signal.pidfd_send_signal(handle, signal.SIGKILL)
                    child.wait(timeout=3)
                    record['forced_cleanup'] = True
                    errors.append(f'probe required forced cleanup: {record["label"]}')
            finally:
                os.close(handle)
        self.children.clear()
        for record, handle in self.native_handles:
            try:
                if same_process(record):
                    signal.pidfd_send_signal(handle, signal.SIGKILL)
                    cleanup = {'identity': record, 'signal': 'SIGKILL', 'mechanism': 'pidfd',
                               'known_baseline_failure': known_baseline_native_failure(self.result, record)}
                    self.result.setdefault('native_forced_cleanups', []).append(cleanup)
                    if not cleanup['known_baseline_failure']:
                        errors.append('native wrapper required forced pidfd cleanup')
                    deadline = time.monotonic() + 3
                    while same_process(record) and time.monotonic() < deadline:
                        time.sleep(.01)
                    cleanup['exact_identity_remaining'] = same_process(record)
                    if cleanup['exact_identity_remaining']:
                        errors.append('native wrapper identity remains after forced pidfd cleanup')
            except ProcessLookupError:
                pass
            finally:
                os.close(handle)
        self.native_handles.clear()
        try:
            self.collect()
        except BaseException as error:
            errors.append(str(error))
        self.save()
        if errors:
            raise RuntimeError('; '.join(errors))

    def execute(self):
        self.prepare()
        self.start()
        if self.workload == 'responsiveness':
            self.responsiveness()
        else:
            self.cancellation()
        self.result['status'] = 'complete'
        self.save()


def main():
    if len(sys.argv) == 5 and sys.argv[1] == '--child':
        return child_operation(sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--baseline-only', action='store_true')
    parser.add_argument('--baseline-source', required=True)
    parser.add_argument('--candidate-source')
    parser.add_argument('--git', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--modes', nargs='+', choices=('demand', 'prefetch'), default=['demand', 'prefetch'])
    parser.add_argument('--workloads', nargs='+', choices=('responsiveness', 'cancellation'),
                        default=['responsiveness', 'cancellation'])
    parser.add_argument('--probe-budget', type=float, default=1)
    parser.add_argument('--shutdown-budget', type=float, default=5)
    parser.add_argument('--mount-budget', type=float, default=15)
    parser.add_argument('--cell-budget', type=int, default=120)
    parser.add_argument('--require-candidate-stats', action='store_true',
                        help='require final drained scheduler state and exact controlled-fetch proof')
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if args.baseline_only == (args.candidate is not None):
        parser.error('choose --candidate or --baseline-only')
    if args.baseline_only and args.require_candidate_stats:
        parser.error('--require-candidate-stats needs a candidate arm')
    if args.candidate and not args.candidate_source:
        parser.error('--candidate-source is required with --candidate')
    if not (1 <= args.trials <= 10 and .1 <= args.probe_budget <= 5 and
            1 <= args.shutdown_budget <= 5 and 1 <= args.mount_budget <= 30 and
            30 <= args.cell_budget <= 600):
        parser.error('bounds: trials1..10, probe.1..5s, shutdown1..5s, mount1..30s, cell30..600s')
    args.output = args.output.resolve()
    args.git = args.git.resolve(strict=True)
    binaries = {'baseline': args.baseline.resolve(strict=True)}
    if args.candidate:
        binaries['candidate'] = args.candidate.resolve(strict=True)
    if args.output.exists():
        parser.error('--output must be a fresh nonexistent path')
    for path in [args.git, *binaries.values()]:
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f'not executable: {path}')
    scripts = [Path(__file__).resolve(), Path(common.__file__).resolve()]
    result = {'schema': 2, 'status': 'dry-run',
              'kind': 'actual FUSE controlled native-Git stall; scheduling, not network-speed comparison',
              'config': {key: str(value) if isinstance(value, Path) else value
                         for key, value in vars(args).items()},
              'binaries': {arm: {'path': str(path), 'sha256': common.sha256(path),
                                 'source_claim': getattr(args, f'{arm}_source')}
                           for arm, path in binaries.items()},
              'git': {'path': str(args.git), 'sha256': common.sha256(args.git)},
              'harnesses': {path.name: common.sha256(path) for path in scripts},
              'host': {'system': platform.system(), 'release': platform.release(),
                       'machine': platform.machine(), 'python': platform.python_version()},
              'baseline_failure_contract': 'Only observed demand/legacy cancellation orphan may require exact-pidfd cleanup; recorded as censored outcome. All residual/unknown cleanup failures and every candidate forced cleanup fail.',
              'cells': []}
    if not args.run:
        print(json.dumps(result, indent=2))
        return 0
    fusermount = shutil.which('fusermount3')
    if platform.machine() not in ('x86_64', 'aarch64', 'riscv64'):
        parser.error('read-syscall verification supports x86_64, aarch64 and riscv64')
    if platform.system() != 'Linux' or not Path('/dev/fuse').exists() or not fusermount:
        parser.error('actual runs require Linux /dev/fuse and fusermount3')
    if not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        parser.error('actual runs require Python pidfd support')
    args.output.mkdir(parents=True)
    frozen = args.output / 'binaries'
    frozen.mkdir()
    for arm, path in binaries.items():
        destination = frozen / arm
        shutil.copyfile(path, destination)
        destination.chmod(0o555)
        if common.sha256(destination) != result['binaries'][arm]['sha256']:
            raise RuntimeError('binary changed while freezing')
        binaries[arm] = destination
    for path in scripts:
        shutil.copyfile(path, args.output / path.name)
        if common.sha256(args.output / path.name) != result['harnesses'][path.name]:
            raise RuntimeError('harness changed while freezing')
    result['status'] = 'running'
    common.checkpoint(args.output / 'results.json', result)
    signal.signal(signal.SIGALRM, common.deadline_handler)
    for trial in range(args.trials):
        order = list(binaries)
        if trial % 2:
            order.reverse()
        for mode in args.modes:
            for workload in args.workloads:
                for arm in order:
                    directory = args.output / f'trial-{trial + 1}-{mode}-{workload}-{arm}'
                    cell = Cell(directory, binaries[arm], args.git, fusermount, args, workload, mode)
                    cell.result.update({'trial': trial + 1, 'arm': arm})
                    result['cells'].append(cell.result)
                    print(f'start {directory.name}', flush=True)
                    signal.alarm(args.cell_budget)
                    try:
                        cell.execute()
                    except BaseException as error:
                        cell.result.update({'status': 'failed', 'error_type': type(error).__name__,
                                            'error': str(error), 'traceback': traceback.format_exc()})
                    finally:
                        signal.alarm(0)
                        try:
                            cell.stop()
                        except BaseException as error:
                            cell.result.update({'status': 'failed', 'cleanup_error': str(error)})
                        if directory.exists():
                            cell.save()
                        common.checkpoint(args.output / 'results.json', result)
                    print(f'finish {directory.name}: {cell.result["status"]}', flush=True)
                    if cell.result['status'] != 'complete':
                        result['status'] = 'failed'
                        common.checkpoint(args.output / 'results.json', result)
                        return 1
    result['status'] = 'complete'
    result['summary'] = {arm: {mode: {
        'responsiveness_trials': len(cells),
        'passed_before_release': sum(cell['responsiveness_passed'] for cell in cells),
        'raw_probe_samples': [cell['probes'] for cell in cells]}
        for mode in args.modes
        if (cells := [cell for cell in result['cells'] if cell['arm'] == arm and
                      cell['mode'] == mode and cell['workload'] == 'responsiveness'])}
        for arm in binaries}
    result['cancellation_summary'] = {arm: {mode: {
        'trials': len(cells),
        'bounded_cancellation_passed': sum(cell['cancellation']['bounded_cancellation_passed'] for cell in cells),
        'censored_at_budget': sum(cell['cancellation']['observation_censored_at_budget'] for cell in cells),
        'forced_native_cleanup_count': sum(len(cell.get('native_forced_cleanups', [])) for cell in cells),
        'raw_samples': [cell['cancellation'] for cell in cells]}
        for mode in args.modes
        if (cells := [cell for cell in result['cells'] if cell['arm'] == arm and
                      cell['mode'] == mode and cell['workload'] == 'cancellation'])}
        for arm in binaries}
    common.checkpoint(args.output / 'results.json', result)
    print(json.dumps(result['summary'], indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
