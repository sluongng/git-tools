#!/usr/bin/env python3
"""Matched real-FUSE overlay churn. Dry-run by default; no network or Cargo jobs.

Example:
  python3 tools/benchmark_vfs_iteration.py --baseline /path/old/git-vfs \
    --candidate target/release/git-vfs --git /path/git --output artifacts/churn \
    --baseline-source c0687ee --candidate-source working-tree --trials 3 --run

RSS and allocated disk include Fjall/cache/WAL effects, not just dead payload.
Shutdown statistics are optional: older baselines need not emit Overlay state.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import signal
import statistics
import stat
import subprocess
import time
import traceback


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def payload(iteration, size):
    # SHAKE produces reproducible, effectively incompressible, distinct versions.
    return hashlib.shake_256(f'git-vfs-iteration-v1:{iteration}'.encode()).digest(size)


def checkpoint(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def run(command, env, cwd=None, timeout=15):
    return subprocess.run([str(x) for x in command], env=env, cwd=cwd,
                          check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout).stdout


SESSIONS = ('initial', 'remount_after_churn', 'correctness_only')
ONLINE_FIELDS = ('online_reclaimed_payload_bytes', 'reclaimed_entries',
                 'retirement_pending_items', 'retirement_pending_bytes',
                 'retirement_failures', 'lookup_accounting_errors')
STAT_FIELDS = ('entry_count', 'linked_entries', 'tombstone_entries',
               'live_payload_bytes', 'dead_payload_bytes', 'reclaimed_payload_bytes')


def deadline_handler(_signal, _frame):
    raise TimeoutError('fixed cell budget exceeded')


class Cell:
    def __init__(self, directory, binary, git, fusermount, args, workload):
        self.directory, self.binary, self.git = directory, binary, git
        self.fusermount, self.args, self.workload = fusermount, args, workload
        self.repo, self.overlay, self.mount = (directory / name for name in
                                               ('repo.git', 'overlay', 'mount'))
        self.proc = None
        self.log = None
        self.logs = []
        self.result = {'workload': workload, 'samples': [], 'mount_seconds': [],
                       'shutdowns': [], 'status': 'started'}
        self.env = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'HOME': str(directory / 'home'),
                    'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8', 'TZ': 'UTC',
                    'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null',
                    'GIT_TERMINAL_PROMPT': '0'}

    def save(self):
        checkpoint(self.directory / 'result.json', self.result)

    def git_run(self, *args):
        return run([self.git, *args], self.env)

    def prepare(self):
        self.directory.mkdir()
        (self.directory / 'home').mkdir()
        self.mount.mkdir()
        source = self.directory / 'source'
        self.git_run('init', source)
        self.git_run('-C', source, 'config', 'user.name', 'Fixture')
        self.git_run('-C', source, 'config', 'user.email', 'fixture@example.invalid')
        (source / 'tracked.txt').write_bytes(b'unchanged base\n')
        self.env.update({'GIT_AUTHOR_DATE': '2000-01-01T00:00:00Z',
                         'GIT_COMMITTER_DATE': '2000-01-01T00:00:00Z'})
        self.git_run('-C', source, 'add', '.')
        self.git_run('-C', source, 'commit', '-m', 'iteration fixture')
        self.git_run('clone', '--bare', source, self.repo)
        self.result['fixture_commit'] = self.git_run('-C', source, 'rev-parse', 'HEAD').decode().strip()
        self.save()

    def start(self):
        log_path = self.directory / f'mount-{len(self.logs)}.log'
        self.logs.append(log_path)
        self.log = log_path.open('wb')
        started = time.monotonic()
        self.proc = subprocess.Popen(
            [str(self.binary), '--git', str(self.git), 'mount', '--repo', str(self.repo),
             '--overlay', str(self.overlay), '--size-source', 'git', str(self.mount)],
            env=self.env, stdout=self.log, stderr=self.log, start_new_session=True)
        self.result.setdefault('owned_pids', []).append(self.proc.pid)
        while not os.path.ismount(self.mount):
            if self.proc.poll() is not None:
                raise RuntimeError(f'mount exited with {self.proc.returncode}; see {log_path.name}')
            if time.monotonic() - started > self.args.mount_budget:
                raise TimeoutError('mount readiness budget exceeded')
            time.sleep(.02)
        self.result['mount_seconds'].append(time.monotonic() - started)

    def stop(self):
        if self.proc is None:
            return
        started = time.monotonic()
        proc, forced = self.proc, False
        cleanup_errors = []
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                forced = True
        if os.path.ismount(self.mount):
            forced = True
            try:
                subprocess.run([self.fusermount, '-u', str(self.mount)], env=self.env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            except subprocess.TimeoutExpired:
                cleanup_errors.append('regular unmount timed out')
        # A child PID remains reserved only while its leader is unreaped.
        # Never signal an old PGID after poll()/wait() observes leader exit: it
        # could have been recycled. Surviving descendants are reported, not killed
        # using an identity we can no longer prove belongs to this cell.
        for sig, budget in ((signal.SIGTERM, 3), (signal.SIGKILL, 3)):
            if proc.poll() is not None:
                break
            forced = True
            try:
                os.killpg(proc.pid, sig)
            except ProcessLookupError:
                pass
            try:
                proc.wait(timeout=budget)
            except subprocess.TimeoutExpired:
                continue
        if os.path.ismount(self.mount):
            try:
                subprocess.run([self.fusermount, '-uz', str(self.mount)], env=self.env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            except subprocess.TimeoutExpired:
                cleanup_errors.append('lazy unmount timed out')
        if self.log is not None:
            self.log.close()
            self.log = None
        try:
            os.killpg(proc.pid, 0)
            group_remaining = True
        except ProcessLookupError:
            group_remaining = False
        self.result['shutdowns'].append({'seconds': time.monotonic() - started,
                                         'exit_code': proc.poll(), 'forced': forced,
                                         'mount_remaining': os.path.ismount(self.mount),
                                         'process_group_remaining': group_remaining,
                                         'cleanup_errors': cleanup_errors})
        # A nonzero group probe after leader exit is only a residual warning:
        # the PGID is no longer a verified signaling target. Do not retry it.
        if proc.poll() is not None:
            self.proc = None
        self.save()
        if group_remaining or os.path.ismount(self.mount):
            raise RuntimeError('owned mount or process group remains after cleanup')
        if forced or proc.returncode != 0 or cleanup_errors:
            raise RuntimeError('mount required forced cleanup or exited unsuccessfully')

    def collect_stats(self):
        sessions = {name: {'reports': [], 'parse_errors': []} for name in SESSIONS}
        for index, log in enumerate(self.logs):
            session = sessions[SESSIONS[index]]
            session['log'] = log.name
            for line_number, line in enumerate(log.read_text(errors='replace').splitlines(), 1):
                if 'Overlay state: ' not in line:
                    continue
                try:
                    state = json.loads(line.split('Overlay state: ', 1)[1])
                    if not isinstance(state, dict) or any(
                            type(state.get(field)) is not int or state[field] < 0
                            for field in STAT_FIELDS):
                        raise ValueError('missing or invalid nonnegative integer fields')
                    session['reports'].append(state)
                except (json.JSONDecodeError, ValueError) as error:
                    session['parse_errors'].append({'line': line_number, 'error': str(error)})
        self.result['overlay_shutdown_states'] = sessions
        online = getattr(self.args, 'require_online_gc', False)
        required = (self.args.require_candidate_stats or online) and self.result['arm'] == 'candidate'
        proof = {'required': required, 'mode': 'online_gc' if online else 'remount_only',
                 'passed': None, 'checks': {}}
        self.result['mechanism_validation'] = proof
        if not required:
            return
        reports_valid = all(len(value['reports']) == 1 and not value['parse_errors']
                            for value in sessions.values())
        if online and reports_valid:
            reports_valid = all(all(type(value['reports'][0].get(field)) is int and
                                    value['reports'][0][field] >= 0 for field in ONLINE_FIELDS)
                                for value in sessions.values())
        proof['checks']['exactly_one_valid_report_per_session'] = reports_valid
        if reports_valid:
            initial = sessions['initial']['reports'][0]
            remount = sessions['remount_after_churn']['reports'][0]
            expected_dead = self.args.iterations * self.args.payload_bytes
            proof['expected_minimum_reclaimed_bytes'] = expected_dead
            if online:
                proof['checks'].update({
                    'initial_and_remount_dead_payload_zero': initial['dead_payload_bytes'] ==
                        remount['dead_payload_bytes'] == 0,
                    'linked_payload_preserved': initial['live_payload_bytes'] ==
                        remount['live_payload_bytes'] == self.args.payload_bytes,
                    'online_reclaimed_expected_churn_payload':
                        initial['online_reclaimed_payload_bytes'] >= expected_dead,
                    'created_churn_entries_reclaimed': initial['reclaimed_entries'] >= self.args.iterations,
                    'physical_payload_drained_while_mounted': len(clean_samples := [
                        sample for sample in self.result['samples'] if sample['label'] == 'after_churn_clean'
                    ]) == 1 and
                        type(clean_samples[0].get('backing_payload_files')) is int and
                        type(clean_samples[0].get('backing_payload_bytes')) is int and
                        clean_samples[0]['backing_payload_files'] == 0 and
                        clean_samples[0]['backing_payload_bytes'] == 0,
                    'all_sessions_retirement_drained': all(
                        value['reports'][0]['retirement_pending_items'] == 0 and
                        value['reports'][0]['retirement_pending_bytes'] == 0
                        for value in sessions.values()),
                    'all_sessions_no_retirement_failures': all(
                        value['reports'][0]['retirement_failures'] == 0 for value in sessions.values()),
                    'all_sessions_lookup_accounting_valid': all(
                        value['reports'][0]['lookup_accounting_errors'] == 0 for value in sessions.values()),
                })
            else:
                proof['checks'].update({
                    'remount_dead_payload_zero': remount['dead_payload_bytes'] == 0,
                    'linked_payload_preserved': initial['live_payload_bytes'] ==
                        remount['live_payload_bytes'] == self.args.payload_bytes,
                    'reclaimed_expected_churn_payload': remount['reclaimed_payload_bytes'] >= expected_dead,
                    'initial_dead_payload_covers_churn': initial['dead_payload_bytes'] >= expected_dead,
                })
        proof['passed'] = all(proof['checks'].values())
        if not proof['passed']:
            self.result['status'] = 'failed'
            self.result['mechanism_error'] = 'candidate reclamation proof failed or statistics missing'

    def sample(self, label):
        time.sleep(.25)  # Fixed settling interval, never inside workload timer.
        status = Path(f'/proc/{self.proc.pid}/status').read_text()
        rss = next(int(line.split()[1]) * 1024 for line in status.splitlines()
                   if line.startswith('VmRSS:'))
        files = []
        for path in self.overlay.rglob('*'):
            try:
                metadata = path.stat()
            except FileNotFoundError:
                continue  # The retirement worker may delete between enumeration and stat.
            if stat.S_ISREG(metadata.st_mode):
                files.append(metadata)
        payload_directory = self.overlay / 'payloads'
        backing_files = backing_bytes = None
        if payload_directory.is_dir():
            backing_files = backing_bytes = 0
            for path in payload_directory.iterdir():
                if path.name == 'redo.data':
                    continue
                try:
                    metadata = path.stat()
                except FileNotFoundError:
                    continue
                if stat.S_ISREG(metadata.st_mode):
                    backing_files += 1
                    backing_bytes += metadata.st_size
        visible = [path for path in self.mount.rglob('*')
                   if '.git' not in path.relative_to(self.mount).parts and path.is_file()]
        self.result['samples'].append({'label': label, 'rss_bytes': rss,
            'backing_payload_files': backing_files, 'backing_payload_bytes': backing_bytes,
            'overlay_file_bytes': sum(stat.st_size for stat in files),
            'overlay_allocated_bytes': sum(stat.st_blocks * 512 for stat in files),
            'visible_payload_bytes': sum(path.stat().st_size for path in visible),
            'visible_files': sorted(str(path.relative_to(self.mount)) for path in visible)})
        self.save()

    def correctness(self):
        assert (self.mount / 'tracked.txt').read_bytes() == b'unchanged base\n'
        assert self.git_run('-C', self.mount, 'status', '--porcelain') == b''
        path = self.mount / 'open-unlinked.bin'
        path.write_bytes(b'held open')
        fd = os.open(path, os.O_RDWR)
        try:
            inode = os.fstat(fd).st_ino
            path.unlink()
            assert os.read(fd, 9) == b'held open'
            os.lseek(fd, 0, os.SEEK_SET)
            assert os.write(fd, b'kept open') == 9
            path.write_bytes(b'new inode')
            assert path.stat().st_ino != inode
            os.lseek(fd, 0, os.SEEK_SET)
            assert os.read(fd, 9) == b'kept open'
        finally:
            os.close(fd)
        path.unlink()
        # Atomic replacement must preserve an already-open destination descriptor.
        path.write_bytes(b'old target')
        with path.open('rb') as opened:
            replacement = self.mount / 'replacement.bin'
            replacement.write_bytes(b'new target')
            source_inode = replacement.stat().st_ino
            replacement.replace(path)
            assert path.stat().st_ino == source_inode
            assert opened.read() == b'old target'
            assert path.read_bytes() == b'new target'
        path.unlink()
        self.result['correctness'] = {'clean_git_status': True, 'base_unchanged': True,
            'open_unlink_read_write': True, 'recreated_inode_distinct': True,
            'overwritten_open_descriptor': True, 'rename_preserves_source_inode': True}

    def execute(self):
        self.prepare()
        self.start()
        self.sample('before_churn')
        # Prepare all versions before timing; hashing/generation is not VFS work.
        versions = [payload(index, self.args.payload_bytes) for index in range(self.args.iterations)]
        self.result['payload_sequence_sha256'] = hashlib.sha256(b''.join(versions)).hexdigest()
        target, temporary = self.mount / 'edited.bin', self.mount / 'temporary.bin'
        if self.workload == 'atomic-save':
            target.write_bytes(b'initial')
        durations = []
        self.result['operation_seconds'] = durations
        for index, data in enumerate(versions):
            started = time.monotonic()
            if self.workload == 'unlink':
                temporary.write_bytes(data)
                temporary.unlink()
            else:
                temporary.write_bytes(data)
                temporary.replace(target)
            durations.append(time.monotonic() - started)
            # Verification is outside the measured interval.
            assert not temporary.exists()
            if self.workload == 'atomic-save':
                assert target.read_bytes() == data
            if index + 1 in {self.args.iterations // 4, self.args.iterations // 2, self.args.iterations}:
                self.sample(f'after_{index + 1}_operations')
        self.result['churn_seconds'] = sum(durations)
        if target.exists():
            target.unlink()
        self.sample('after_churn_clean')
        survivor = self.mount / 'linked-survivor.bin'
        survivor.write_bytes(payload(99999, self.args.payload_bytes))
        self.stop()
        self.start()
        self.sample('after_remount')
        assert survivor.read_bytes() == payload(99999, self.args.payload_bytes)
        # Capture remount reclamation stats before correctness probes create new
        # dead inodes. The third session is untimed control work, not remount latency.
        self.stop()
        self.start()
        survivor.unlink()
        self.correctness()
        self.result['correctness']['linked_edit_survives_remount'] = True
        self.stop()
        self.result['status'] = 'complete'
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--baseline-only', action='store_true')
    parser.add_argument('--require-candidate-stats', action='store_true',
                        help='fail candidate cells without complete per-session reclamation proof')
    parser.add_argument('--require-online-gc', action='store_true',
                        help='require candidate online reclamation and drained retirement proof; replaces remount-only proof')
    parser.add_argument('--baseline-source', default='unknown (caller did not supply a source claim)')
    parser.add_argument('--candidate-source', default='unknown (caller did not supply a source claim)')
    parser.add_argument('--git', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--iterations', type=int, default=64)
    parser.add_argument('--payload-bytes', type=int, default=262144)
    parser.add_argument('--workloads', nargs='+', choices=['unlink', 'atomic-save'], default=['unlink', 'atomic-save'])
    parser.add_argument('--mount-budget', type=float, default=15)
    parser.add_argument('--cell-budget', type=int, default=180)
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if not args.baseline_only and args.candidate is None:
        parser.error('--candidate is required unless --baseline-only')
    if args.baseline_only and (args.require_candidate_stats or args.require_online_gc):
        parser.error('--require-candidate-stats/--require-online-gc need a candidate arm')
    if args.baseline_only and args.candidate is not None:
        parser.error('--candidate and --baseline-only are mutually exclusive')
    if not (1 <= args.trials <= 20 and 1 <= args.iterations <= 4096 and
            1 <= args.payload_bytes <= 1048576 and args.iterations * args.payload_bytes <= 268435456
            and 1 <= args.mount_budget <= 60 and 30 <= args.cell_budget <= 3600):
        parser.error('invalid bounds: trials1..20, iterations1..4096, payload1..1MiB, total<=256MiB, mount1..60s, cell30..3600s')
    args.output = args.output.resolve()
    args.git = args.git.resolve(strict=True)
    binaries = {'baseline': args.baseline.resolve(strict=True)}
    if not args.baseline_only:
        binaries['candidate'] = args.candidate.resolve(strict=True)
    for path in [args.git, *binaries.values()]:
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f'not an executable file: {path}')
    if args.output.exists():
        parser.error('--output must be a fresh nonexistent path')
    result = {'schema': 2, 'kind': 'local actual-FUSE churn; not EdenFS or Bazel latency',
              'config': {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
              'binaries': {name: {'path': str(path), 'sha256': sha256(path),
                                    'source_claim': getattr(args, f'{name}_source')}
                           for name, path in binaries.items()},
              'host': {'system': platform.system(), 'release': platform.release(),
                       'machine': platform.machine(), 'python': platform.python_version(),
                       'cpu_count': os.cpu_count()},
              'mount_session_order': list(SESSIONS),
              'git': {'path': str(args.git), 'sha256': sha256(args.git)},
              'harness_sha256': sha256(Path(__file__)), 'cells': [], 'status': 'dry-run'}
    if not args.run:
        print(json.dumps(result, indent=2))
        return 0
    fusermount = shutil.which('fusermount3')
    if not fusermount or not Path('/dev/fuse').exists():
        parser.error('actual runs require fusermount3 and /dev/fuse')
    args.output.mkdir(parents=True)
    frozen = args.output / 'binaries'
    frozen.mkdir()
    # Freeze BOTH binaries before creating a fixture or starting either arm.
    for name, path in binaries.items():
        destination = frozen / name
        shutil.copyfile(path, destination)
        destination.chmod(0o555)
        if sha256(destination) != result['binaries'][name]['sha256']:
            raise RuntimeError('binary changed while freezing; no fixtures have been created')
        binaries[name] = destination
    shutil.copyfile(__file__, args.output / 'benchmark_vfs_iteration.py')
    result['status'] = 'running'
    checkpoint(args.output / 'results.json', result)
    signal.signal(signal.SIGALRM, deadline_handler)
    failed = False
    for trial in range(args.trials):
        order = list(binaries)
        if trial % 2:
            order.reverse()
        for workload in args.workloads:
            for arm in order:
                directory = args.output / f'trial-{trial + 1}-{workload}-{arm}'
                cell = Cell(directory, binaries[arm], args.git, fusermount, args, workload)
                cell.result.update({'trial': trial + 1, 'arm': arm})
                result['cells'].append(cell.result)
                print(f'start trial={trial + 1} workload={workload} arm={arm}', flush=True)
                signal.alarm(args.cell_budget)
                try:
                    cell.execute()
                except BaseException as error:
                    failed = True
                    cell.result.update({'status': 'failed', 'error_type': type(error).__name__,
                                        'error': str(error), 'traceback': traceback.format_exc()})
                finally:
                    signal.alarm(0)
                    try:
                        cell.stop()
                    except BaseException as error:
                        failed = True
                        cell.result['status'] = 'failed'
                        cell.result['cleanup_error'] = f'{type(error).__name__}: {error}'
                    cell.collect_stats()
                    if cell.result['status'] != 'complete':
                        failed = True
                    if directory.exists():
                        cell.save()
                    checkpoint(args.output / 'results.json', result)
                print(f'finish {directory.name}: {cell.result["status"]}', flush=True)
                if cell.result['status'] != 'complete':
                    # Preserve failures without retrying; abort before starting more mounts.
                    result['status'] = 'failed'
                    checkpoint(args.output / 'results.json', result)
                    return 1
    result['status'] = 'failed' if failed else 'complete'
    result['summary'] = {workload: {arm: {
        'trials': len(values), 'churn_seconds_median': statistics.median(values),
        'churn_seconds_min': min(values), 'churn_seconds_max': max(values)}
        for arm in binaries
        if (values := [cell['churn_seconds'] for cell in result['cells']
                       if cell['arm'] == arm and cell['workload'] == workload and cell['status'] == 'complete'])}
        for workload in args.workloads}
    checkpoint(args.output / 'results.json', result)
    print(json.dumps(result['summary'], indent=2))
    return int(failed)


if __name__ == '__main__':
    raise SystemExit(main())
