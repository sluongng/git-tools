#!/usr/bin/env python3
"""Actual-FUSE immutable content pressure, local Git objects; dry-run default.

20 distinct 4MiB blobs exceed the default 64MiB shared disk-content cache.
Sequential 4KiB ranges run forward/reverse/forward, followed by one range at the
tail of an 80MiB blob. The tail forces a full native Git stream without requiring
a whole-blob VFS buffer. No network, Cargo, retries, edits or global cache drops.
RSS is sampled for the owned VFS PID, not native-Git children or cgroup memory.
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
import subprocess
import threading
import time
import traceback

import benchmark_vfs_iteration as common

BLOCK = 256 * 1024
CACHE_BYTES = 64 * 1024 * 1024
CONTENT_FIELDS = ('outstanding_requests', 'reply_bytes', 'cache_resident_bytes',
    'cache_pinned_bytes', 'cache_inflight_bytes', 'cache_total_bytes', 'cache_peak_bytes',
    'cache_files', 'coalesced_requests', 'foreground_promotions', 'cache_hits',
    'cache_bypasses', 'fetch_batches', 'fetched_objects', 'local_bytes',
    'rejected_requests', 'callback_panics', 'worker_failures', 'queued_local',
    'queued_large', 'queued_foreground', 'queued_speculative', 'inflight_oids')
DRAINED_FIELDS = ('outstanding_requests', 'reply_bytes', 'cache_resident_bytes',
    'cache_pinned_bytes', 'cache_inflight_bytes', 'cache_total_bytes', 'cache_files',
    'queued_local', 'queued_large', 'queued_foreground', 'queued_speculative', 'inflight_oids')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def block_data(blob, block, length):
    return hashlib.shake_256(f'git-vfs-content-v1:{blob}:{block}'.encode()).digest(length)


def expected_range(blob, total, offset, length):
    require(0 <= offset <= total - length, 'range outside fixture')
    data = bytearray()
    while len(data) < length:
        position = offset + len(data)
        block = position // BLOCK
        relative = position % BLOCK
        chunk = block_data(blob, block, min(BLOCK, total - block * BLOCK))
        data.extend(chunk[relative:relative + length - len(data)])
    return bytes(data)


def rss(pid):
    status = Path(f'/proc/{pid}/status').read_text()
    return next(int(line.split()[1]) * 1024 for line in status.splitlines() if line.startswith('VmRSS:'))


class RssSampler:
    def __init__(self, pid):
        self.pid = pid
        self.samples = []
        self.errors = []
        self.stop_event = threading.Event()
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self.sample, daemon=False)

    def sample(self):
        while not self.stop_event.is_set():
            try:
                self.samples.append({'monotonic': time.monotonic(), 'rss_bytes': rss(self.pid)})
            except (OSError, StopIteration) as error:
                self.errors.append(str(error))
            finally:
                self.ready.set()
            self.stop_event.wait(.02)

    def __enter__(self):
        self.thread.start()
        if not self.ready.wait(2):
            self.stop_event.set()
            self.thread.join(timeout=2)
            raise RuntimeError('RSS sampler failed to start')
        return self

    def __exit__(self, *_args):
        self.stop_event.set()
        self.thread.join(timeout=2)
        require(not self.thread.is_alive(), 'owned RSS sampling thread remains')


class ContentCell(common.Cell):
    def __init__(self, directory, binary, git, fusermount, args):
        super().__init__(directory, binary, git, fusermount, args, 'local-content-pressure')
        self.result.update(phases=[], fixture_blobs=[])

    def prepare(self):
        self.directory.mkdir()
        (self.directory / 'home').mkdir()
        self.mount.mkdir()
        self.git_run('init', '--bare', self.repo)
        self.git_run('-C', self.repo, 'symbolic-ref', 'HEAD', 'refs/heads/main')
        import_log = (self.directory / 'fast-import.log').open('wb')
        process = subprocess.Popen([str(self.git), '-C', str(self.repo), 'fast-import', '--quiet'],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=import_log, env=self.env,
            start_new_session=True)
        stream_hash = hashlib.sha256()
        def emit(data):
            process.stdin.write(data)
            stream_hash.update(data)
        blobs = [(index, self.args.object_bytes, f'objects/{index:03d}.bin')
                 for index in range(self.args.object_count)]
        blobs.append((self.args.object_count, self.args.oversize_bytes, 'oversize.bin'))
        try:
            for mark, (blob, length, name) in enumerate(blobs, 1):
                emit(f'blob\nmark :{mark}\ndata {length}\n'.encode())
                sha1 = hashlib.sha1(f'blob {length}\0'.encode())
                sha256 = hashlib.sha256()
                for block, offset in enumerate(range(0, length, BLOCK)):
                    data = block_data(blob, block, min(BLOCK, length - offset))
                    sha1.update(data)
                    sha256.update(data)
                    emit(data)
                emit(b'\n')
                self.result['fixture_blobs'].append({'name': name, 'bytes': length,
                    'oid': sha1.hexdigest(), 'sha256': sha256.hexdigest()})
            message = b'content fixture\n'
            emit(b'commit refs/heads/main\nauthor Fixture <fixture@example.invalid> 946684800 +0000\ncommitter Fixture <fixture@example.invalid> 946684800 +0000\n')
            emit(f'data {len(message)}\n'.encode() + message)
            for mark, (_blob, _length, name) in enumerate(blobs, 1):
                emit(f'M 100644 :{mark} {name}\n'.encode())
            emit(b'\ndone\n')
            process.stdin.close()
            require(process.wait(timeout=60) == 0, 'Git fast-import failed; see log')
        finally:
            if process.poll() is None:
                process.kill()  # unreaped direct child identity, never an old PGID
                process.wait(timeout=3)
            if not process.stdin.closed:
                process.stdin.close()
            import_log.close()
        self.result['fixture_stream_sha256'] = stream_hash.hexdigest()
        self.result['fixture_commit'] = self.git_run('-C', self.repo, 'rev-parse', 'HEAD').decode().strip()
        self.result['input_blob_bytes'] = sum(blob['bytes'] for blob in self.result['fixture_blobs'])
        self.save()

    def read_one(self, blob, length, offset, filename, expected):
        fd = os.open(self.mount / filename, os.O_RDONLY)
        try:
            started = time.monotonic()
            data = os.pread(fd, self.args.range_bytes, offset)
            elapsed = time.monotonic() - started
        finally:
            os.close(fd)
        require(data == expected, f'content range mismatch: {filename}:{offset}')
        return {'blob': blob, 'offset': offset, 'bytes': len(data), 'seconds': elapsed}

    def execute(self):
        self.prepare()
        self.start()
        self.sample('before_content_reads')
        order = list(range(self.args.object_count))
        for number, (label, indices) in enumerate((('forward', order), ('reverse', order[::-1]), ('forward_again', order))):
            prepared = []
            for blob in indices:
                offset = ((blob + number * 17) * 104729) % (self.args.object_bytes - self.args.range_bytes + 1)
                prepared.append((blob, offset, expected_range(blob, self.args.object_bytes, offset, self.args.range_bytes)))
            phase = {'label': label, 'reads': []}
            self.result['phases'].append(phase)
            started = time.monotonic()
            for blob, offset, expected in prepared:
                phase['reads'].append(self.read_one(blob, self.args.object_bytes, offset,
                    f'objects/{blob:03d}.bin', expected))
            phase['wall_seconds'] = time.monotonic() - started
            self.sample(f'after_{label}')
        offset = self.args.oversize_bytes - self.args.range_bytes
        expected = expected_range(self.args.object_count, self.args.oversize_bytes, offset, self.args.range_bytes)
        before_rss = rss(self.proc.pid)
        phase = {'label': 'oversize_tail', 'reads': []}
        self.result['phases'].append(phase)
        with RssSampler(self.proc.pid) as sampler:
            phase['rss_samples'] = sampler.samples
            phase['rss_sampling_errors'] = sampler.errors
            started = time.monotonic()
            phase['reads'].append(self.read_one(self.args.object_count, self.args.oversize_bytes, offset, 'oversize.bin', expected))
            phase['wall_seconds'] = time.monotonic() - started
        phase['rss_samples'] = sampler.samples
        phase['rss_sampling_errors'] = sampler.errors
        require(sampler.samples and not sampler.errors, 'RSS sampling failed during oversize read')
        phase['before_rss_bytes'] = before_rss
        phase['observed_peak_rss_bytes'] = max(sample['rss_bytes'] for sample in sampler.samples)
        phase['observed_rss_growth_bytes'] = max(0, phase['observed_peak_rss_bytes'] - before_rss)
        if self.args.oversize_rss_growth_limit is not None:
            require(phase['observed_rss_growth_bytes'] <= self.args.oversize_rss_growth_limit,
                    'observed RSS growth exceeds caller-supplied bound')
        self.sample('after_oversize_tail')
        require(self.git_run('-C', self.mount, 'status', '--porcelain') == b'', 'content reads changed Git status')
        self.stop()
        self.start()
        for blob in (0, self.args.object_count - 1):
            require(self.read_one(blob, self.args.object_bytes, 0, f'objects/{blob:03d}.bin',
                expected_range(blob, self.args.object_bytes, 0, self.args.range_bytes))['bytes'] == self.args.range_bytes,
                'remount local range failed')
        self.read_one(self.args.object_count, self.args.oversize_bytes, offset, 'oversize.bin', expected)
        require(self.git_run('-C', self.mount, 'status', '--porcelain') == b'', 'remount Git status changed')
        self.sample('after_remount_verification')
        self.result['correctness'] = {'all_requested_bytes_match': True, 'no_workspace_edits': True,
                                     'remount_bytes_match': True, 'git_status_clean_both_sessions': True}
        self.stop()
        self.result['status'] = 'complete'
        self.save()

    def collect_stats(self):
        sessions = {name: {'reports': [], 'parse_errors': []} for name in ('workload', 'remount')}
        for index, log in enumerate(self.logs):
            entry = sessions[('workload', 'remount')[index]]
            entry['log'] = log.name
            for number, line in enumerate(log.read_text(errors='replace').splitlines(), 1):
                if line.startswith('Content state: '):
                    try:
                        report = json.loads(line[len('Content state: '):])
                        if not isinstance(report, dict):
                            raise ValueError('not an object')
                        entry['reports'].append(report)
                    except ValueError as error:
                        entry['parse_errors'].append({'line': number, 'error': str(error)})
        self.result['content_shutdown_states'] = sessions
        required = self.args.require_candidate_stats and self.result['arm'] == 'candidate'
        proof = {'required': required, 'passed': None, 'checks': {},
                 'shared_disk_content_cache_limit_bytes': CACHE_BYTES,
                 'separate_reply_admission_limit_bytes': CACHE_BYTES,
                 'rss_contract': 'Observed VFS-only RSS/20ms peaks are reported, not conflated with logical disk-cache bytes. No default allocator-dependent RSS assertion.'}
        self.result['mechanism_validation'] = proof
        if not required:
            return
        valid = all(len(value['reports']) == 1 and not value['parse_errors'] and
            all(type(value['reports'][0].get(field)) is int and
                0 <= value['reports'][0][field] <= 18446744073709551615 for field in CONTENT_FIELDS)
            for value in sessions.values())
        proof['checks']['exactly_one_valid_content_report_per_session'] = valid
        if valid:
            reports = [value['reports'][0] for value in sessions.values()]
            initial = reports[0]
            proof['checks'].update({
                'fixture_exceeds_shared_cache_budget': self.args.object_count * self.args.object_bytes > CACHE_BYTES,
                'oversize_object_exceeds_cache_budget': self.args.oversize_bytes > CACHE_BYTES,
                'all_final_charges_and_queues_drained': all(state[field] == 0 for state in reports for field in DRAINED_FIELDS),
                'shared_cache_peak_within_limit': all(state['cache_peak_bytes'] <= CACHE_BYTES for state in reports),
                'warm_content_cache_hit_observed': initial['cache_hits'] > 0,
                'oversize_bypass_observed': initial['cache_bypasses'] > 0,
                'input_stream_accounted': initial['local_bytes'] >= self.result['input_blob_bytes'],
                'no_remote_fetches_for_local_fixture': all(state['fetch_batches'] == state['fetched_objects'] == 0 for state in reports),
                'sequential_fixture_no_rejections': all(state['rejected_requests'] == 0 for state in reports),
                'no_worker_or_callback_failures': all(state['worker_failures'] == state['callback_panics'] == 0 for state in reports),
            })
        proof['passed'] = all(proof['checks'].values())
        if not proof['passed']:
            self.result.update(status='failed', mechanism_error='candidate content pressure proof failed or statistics missing')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--baseline-source', required=True)
    parser.add_argument('--candidate-source', required=True)
    parser.add_argument('--git', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--object-count', type=int, default=20)
    parser.add_argument('--object-bytes', type=int, default=4 * 1024 * 1024)
    parser.add_argument('--oversize-bytes', type=int, default=80 * 1024 * 1024)
    parser.add_argument('--range-bytes', type=int, default=4096)
    parser.add_argument('--oversize-rss-growth-limit', type=int, help='optional caller-selected observed VFS RSS delta bound; no default')
    parser.add_argument('--require-candidate-stats', action='store_true')
    parser.add_argument('--mount-budget', type=float, default=30)
    parser.add_argument('--cell-budget', type=int, default=600)
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if not (1 <= args.trials <= 10 and 2 <= args.object_count <= 64 and
            4096 <= args.object_bytes <= 16 * 1024 * 1024 and 4096 <= args.oversize_bytes <= 256 * 1024 * 1024 and
            1 <= args.range_bytes <= min(65536, args.object_bytes, args.oversize_bytes) and
            args.object_count * args.object_bytes + args.oversize_bytes <= 512 * 1024 * 1024 and
            1 <= args.mount_budget <= 120 and 30 <= args.cell_budget <= 3600 and
            (args.oversize_rss_growth_limit is None or args.oversize_rss_growth_limit >= 0)):
        parser.error('invalid bounded workload settings; see --help')
    args.output, args.git = args.output.resolve(), args.git.resolve(strict=True)
    binaries = {name: getattr(args, name).resolve(strict=True) for name in ('baseline', 'candidate')}
    for path in [args.git, *binaries.values()]:
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f'not an executable file: {path}')
    if args.output.exists():
        parser.error('--output must be fresh and nonexistent')
    helper = Path(common.__file__).resolve()
    result = {'schema': 1, 'status': 'dry-run', 'kind': 'actual-FUSE local content pressure; not network or Bazel latency',
        'cells': [], 'config': {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        'binaries': {name: {'path': str(path), 'sha256': common.sha256(path), 'source_claim': getattr(args, f'{name}_source')}
                     for name, path in binaries.items()},
        'git': {'path': str(args.git), 'sha256': common.sha256(args.git)},
        'harness_sha256': common.sha256(Path(__file__)), 'helper_sha256': common.sha256(helper),
        'host': {'system': platform.system(), 'release': platform.release(), 'machine': platform.machine(), 'python': platform.python_version()},
        'timing_contract': 'phase wall includes sequential open/pread/close and byte comparisons; payload generation outside phase; raw pread timers exclude comparison',
        'memory_contract': '20ms sampled VFS PID RSS excludes Git children; shared cache is disk content, not a RAM-sized Vec; separate64MiB reply admission budget'}
    if not args.run:
        print(json.dumps(result, indent=2))
        return 0
    fusermount = shutil.which('fusermount3')
    if not fusermount or not Path('/dev/fuse').exists():
        parser.error('actual runs require fusermount3 and /dev/fuse')
    args.output.mkdir(parents=True)
    frozen = args.output / 'binaries';frozen.mkdir()
    for name, path in binaries.items():
        destination = frozen / name
        shutil.copyfile(path, destination);destination.chmod(0o555)
        require(common.sha256(destination) == result['binaries'][name]['sha256'], 'binary changed while freezing')
        binaries[name] = destination
    shutil.copyfile(__file__, args.output / 'benchmark_vfs_content.py')
    shutil.copyfile(helper, args.output / 'benchmark_vfs_iteration.py')
    result['status'] = 'running'
    common.checkpoint(args.output / 'results.json', result)
    signal.signal(signal.SIGALRM, common.deadline_handler)
    for trial in range(args.trials):
        order = list(binaries) if trial % 2 == 0 else list(reversed(binaries))
        for arm in order:
            directory = args.output / f'trial-{trial + 1}-{arm}'
            cell = ContentCell(directory, binaries[arm], args.git, fusermount, args)
            cell.result.update(trial=trial + 1, arm=arm);result['cells'].append(cell.result)
            print(f'start {directory.name}', flush=True)
            signal.alarm(args.cell_budget)
            try:
                cell.execute()
            except BaseException as error:
                cell.result.update(status='failed', error_type=type(error).__name__, error=str(error), traceback=traceback.format_exc())
            finally:
                signal.alarm(0)
                try:cell.stop()
                except BaseException as error:cell.result.update(status='failed', cleanup_error=str(error))
                cell.collect_stats()
                if directory.exists():cell.save()
                common.checkpoint(args.output / 'results.json', result)
            print(f'finish {directory.name}: {cell.result["status"]}', flush=True)
            if cell.result['status'] != 'complete':
                result['status']='failed';common.checkpoint(args.output/'results.json',result);return 1
    result['status']='complete';result['summary']={}
    for arm in binaries:
        cells=[cell for cell in result['cells'] if cell['arm']==arm]
        summary=result['summary'][arm]={}
        for label in ('forward','reverse','forward_again','oversize_tail'):
            phases=[next(phase for phase in cell['phases'] if phase['label']==label) for cell in cells]
            values=[phase['wall_seconds'] for phase in phases]
            summary[label]={'median_seconds':statistics.median(values),'min_seconds':min(values),'max_seconds':max(values),'trials':len(values)}
            if label=='oversize_tail':
                for metric in ('observed_peak_rss_bytes','observed_rss_growth_bytes'):
                    values=[phase[metric] for phase in phases]
                    summary[label][metric]={'median':statistics.median(values),'min':min(values),'max':max(values)}
        summary['sample_resources']={}
        for label in (sample['label'] for sample in cells[0]['samples']):
            summary['sample_resources'][label]={}
            for metric in ('rss_bytes','overlay_allocated_bytes','overlay_file_bytes'):
                values=[next(sample[metric] for sample in cell['samples'] if sample['label']==label) for cell in cells]
                summary['sample_resources'][label][metric]={'median':statistics.median(values),'min':min(values),'max':max(values)}
    common.checkpoint(args.output/'results.json',result)
    print(json.dumps(result['summary'],indent=2));return 0


if __name__=='__main__':
    raise SystemExit(main())
