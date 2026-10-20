#!/usr/bin/env python3
"""Matched real-FUSE namespace/digest benchmark; dry-run default.

Rename fixtures use Git fast-import (1k/10k/100k entries, no host file per entry).
Only rename syscalls are timed. Digest size queries use libc getxattr(size=0),
not os.getxattr, which fetches values. Edited SHA1 and BLAKE3 values are checked
against independent hashes. Loopback metadata fixtures enable BLAKE3 without a
real proxy or external network. No Cargo, cache drops, retries or remote actions.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import platform
import resource
import shutil
import signal
import statistics
import subprocess
import threading
import time
import traceback

import benchmark_vfs_iteration as common

LIBC = ctypes.CDLL(None, use_errno=True)
LIBC.getxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t]
LIBC.getxattr.restype = ctypes.c_ssize_t
ATTRS = {'user.gitsha1': 20, 'user.blake3': 32}
WORK_FIELDS = ('rename_visited_entries', 'digest_hash_operations', 'digest_hash_bytes',
               'digest_cache_hits', 'digest_cache_entries')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def gitsha(data):
    return hashlib.sha1(f'blob {len(data)}\0'.encode() + data).digest()


def getxattr_size(path, name):
    size = LIBC.getxattr(os.fsencode(path), name.encode(), None, 0)
    if size < 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), str(path))
    return size


class MappingFixture:
    """Known local blob mappings only; never contacts an upstream."""
    def __init__(self, mappings):
        self.requests = 0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(2)

            def do_POST(self):
                length = int(self.headers.get('Content-Length', '0'))
                if self.path != '/api/v1/repos/fixture/objects' or not 0 < length <= 1048576:
                    self.send_error(400)
                    return
                try:
                    request = json.loads(self.rfile.read(length))
                    oids = request['oids']
                    if request.get('blake3') is not True or not isinstance(oids, list) or not oids:
                        raise ValueError('invalid request')
                    response = json.dumps({'objects': [mappings[oid] for oid in oids]}).encode()
                except (KeyError, ValueError, TypeError):
                    self.send_error(400)
                    return
                owner.requests += 1
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(response)))
                self.end_headers()
                self.wfile.write(response)

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = False
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        require(not self.thread.is_alive(), 'owned mapping fixture thread remains')


class MetadataCell(common.Cell):
    def __init__(self, directory, binary, git, fusermount, args, workload, entries):
        super().__init__(directory, binary, git, fusermount, args, workload)
        self.entries = entries
        self.mapping = None
        self.result.update(entries=entries, operation_seconds={})
        if workload == 'startup':
            self.env['GIT_TOOLS_METADATA_TIMINGS'] = '1'

    def blake3(self, data):
        output = subprocess.run([str(self.args.b3sum), '--no-names'], input=data,
                                env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=20, check=True).stdout
        digest = bytes.fromhex(output.decode().strip())
        require(len(digest) == 32, 'independent BLAKE3 hash has wrong length')
        return digest

    def prepare(self):
        self.directory.mkdir()
        (self.directory / 'home').mkdir()
        self.mount.mkdir()
        self.git_run('init', '--bare', self.repo)
        self.git_run('-C', self.repo, 'symbolic-ref', 'HEAD', 'refs/heads/main')
        contents = {'tracked.txt': b'unchanged base\n', 'moving/a.txt': b'first descendant\n',
                    'moving/deep/b.txt': b'second descendant\n'}
        if self.workload == 'digest':
            self.expected = bytearray(common.payload(70000, self.args.file_bytes))
            contents['large.bin'] = bytes(self.expected)
        mappings, marks = {}, {}
        import_path = self.directory / 'fixture.fast-import'
        with import_path.open('wb') as stream:
            for mark, (path, data) in enumerate(contents.items(), 1):
                marks[path] = mark
                stream.write(f'blob\nmark :{mark}\ndata {len(data)}\n'.encode() + data + b'\n')
                if self.workload in ('digest', 'startup'):
                    oid = gitsha(data).hex()
                    mappings[oid] = {'oid': oid, 'object_type': 'blob', 'size_bytes': len(data),
                                     'blake3': {'hash': self.blake3(data).hex(), 'size_bytes': len(data)}}
            stream.write(b'commit refs/heads/main\nauthor Fixture <fixture@example.invalid> 946684800 +0000\ncommitter Fixture <fixture@example.invalid> 946684800 +0000\ndata 17\nmetadata fixture\n')
            for path, mark in marks.items():
                stream.write(f'M 100644 :{mark} {path}\n'.encode())
            for index in range(self.entries):
                path = (f'unrelated/{index // 1000:04d}/{index:07d}.txt'
                        if self.workload == 'startup' else f'unrelated/{index:06d}.txt')
                stream.write(f'M 100644 :1 {path}\n'.encode())
            stream.write(b'\ndone\n')
        with import_path.open('rb') as stream:
            subprocess.run([str(self.git), '-C', str(self.repo), 'fast-import', '--quiet'],
                           stdin=stream, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           env=self.env, timeout=self.args.cell_budget, check=True)
        self.result['fixture_commit'] = self.git_run('-C', self.repo, 'rev-parse', 'HEAD').decode().strip()
        self.result['fixture_import_sha256'] = common.sha256(import_path)
        if self.workload in ('digest', 'startup'):
            self.mapping = MappingFixture(mappings)
            self.result['blake3_mapping_source'] = 'owned loopback fixture; independently hashed local base blobs'
        self.save()

    def start(self):
        if self.mapping is None:
            return super().start()
        log = self.directory / f'mount-{len(self.logs)}.log'
        self.logs.append(log)
        self.log = log.open('wb')
        started = time.monotonic()
        self.proc = subprocess.Popen([str(self.binary), '--git', str(self.git), 'mount',
            '--repo', str(self.repo), '--overlay', str(self.overlay), '--size-source', 'git',
            '--blake3-proxy-url', self.mapping.url, '--blake3-proxy-repo', 'fixture', str(self.mount)],
            env=self.env, stdout=self.log, stderr=self.log, start_new_session=True)
        self.result.setdefault('owned_pids', []).append(self.proc.pid)
        while not os.path.ismount(self.mount):
            require(self.proc.poll() is None, 'digest mount exited; see mount log')
            if time.monotonic() - started > self.args.mount_budget:
                raise TimeoutError('mount readiness budget exceeded')
            time.sleep(.02)
        self.result['mount_seconds'].append(time.monotonic() - started)

    def sample(self, label):
        # Never enumerate the mounted 100k-entry fixture for measurement.
        time.sleep(.25)
        status = Path(f'/proc/{self.proc.pid}/status').read_text()
        rss = next(int(line.split()[1]) * 1024 for line in status.splitlines() if line.startswith('VmRSS:'))
        files = [path.stat() for path in self.overlay.rglob('*') if path.is_file()]
        peak_rss = next(int(line.split()[1]) * 1024 for line in status.splitlines() if line.startswith('VmHWM:'))
        self.result['samples'].append({'label': label, 'rss_bytes': rss, 'process_peak_rss_bytes': peak_rss,
            'overlay_file_bytes': sum(stat.st_size for stat in files),
            'overlay_allocated_bytes': sum(stat.st_blocks * 512 for stat in files)})
        self.save()

    def rename_workload(self):
        target = self.mount / 'target.bin'
        temporary = self.mount / 'replacement.bin'
        current = b'initial target'
        target.write_bytes(current)
        timings = self.result['operation_seconds']
        timings['file_replace'] = []
        timings['directory_rename'] = []
        for index in range(self.args.iterations):
            replacement = common.payload(71000 + index, 4096)
            temporary.write_bytes(replacement)
            source_inode = temporary.stat().st_ino
            with target.open('rb') as opened:
                started = time.monotonic()
                temporary.replace(target)
                timings['file_replace'].append(time.monotonic() - started)
                require(opened.read() == current, 'overwritten-open descriptor changed')
            require(target.stat().st_ino == source_inode, 'rename changed source inode')
            require(target.read_bytes() == replacement, 'replacement bytes differ')
            current = replacement
        self.sample('after_file_replacements')
        current_dir = self.mount / 'moving'
        for _index in range(self.args.iterations):
            destination = self.mount / ('renamed' if current_dir.name == 'moving' else 'moving')
            inode = current_dir.stat().st_ino
            started = time.monotonic()
            current_dir.rename(destination)
            timings['directory_rename'].append(time.monotonic() - started)
            require(not current_dir.exists(), 'old directory still visible')
            require(destination.stat().st_ino == inode, 'directory rename changed inode')
            require((destination / 'a.txt').read_bytes() == b'first descendant\n', 'descendant changed')
            require((destination / 'deep/b.txt').read_bytes() == b'second descendant\n', 'nested descendant changed')
            current_dir = destination
        # Leave the directory renamed to verify old/new descendant Git status.
        if current_dir.name == 'moving':
            current_dir.rename(self.mount / 'renamed')
        expected_status = {' D moving/a.txt', ' D moving/deep/b.txt', '?? renamed/a.txt',
                           '?? renamed/deep/b.txt', '?? target.bin'}
        status = set(self.git_run('-C', self.mount, 'status', '--porcelain', '--untracked-files=all').decode().splitlines())
        require(status == expected_status, f'unexpected directory Git status: {sorted(status)}')
        require((self.mount / 'tracked.txt').read_bytes() == b'unchanged base\n', 'tracked base changed')
        for index in (0, self.entries - 1):
            require((self.mount / f'unrelated/{index:06d}.txt').read_bytes() == b'unchanged base\n',
                    'unrelated tree bytes changed')
        self.result['final_target_sha256'] = hashlib.sha256(current).hexdigest()
        self.result['correctness'] = {'overwritten_open_preserved': True, 'file_source_inode_preserved': True,
                                     'directory_inode_preserved': True, 'old_new_descendant_status': True,
                                     'unchanged_base_preserved': True}
        self.sample('after_directory_renames')
        self.stop()
        self.start()
        require((self.mount / 'target.bin').read_bytes() == current, 'target remount bytes differ')
        require(not (self.mount / 'moving').exists(), 'old directory reappeared after remount')
        require((self.mount / 'renamed/a.txt').read_bytes() == b'first descendant\n', 'remount descendant differs')
        require((self.mount / 'renamed/deep/b.txt').read_bytes() == b'second descendant\n', 'remount nested descendant differs')
        require(set(self.git_run('-C', self.mount, 'status', '--porcelain', '--untracked-files=all').decode().splitlines()) == expected_status,
                'Git status changed after remount')
        self.result['correctness']['remount_persistence'] = True

    def digest_workload(self):
        path = self.mount / 'large.bin'
        phases = self.result['digest_phases'] = []
        for epoch in range(2):
            # Both content generations differ from the base, with unchanged size.
            self.expected[0] ^= 128 if epoch == 0 else 1
            fd = os.open(path, os.O_RDWR)
            try:
                require(os.pwrite(fd, self.expected[:1], 0) == 1, 'short same-size edit')
            finally:
                os.close(fd)
            expected = {'user.gitsha1': gitsha(self.expected), 'user.blake3': self.blake3(self.expected)}
            phase = {'epoch': epoch, 'expected_digests': {name: value.hex() for name, value in expected.items()},
                     'size_only_seconds': {name: [] for name in ATTRS}, 'value_seconds': {name: [] for name in ATTRS}}
            phases.append(phase)
            for name, size in ATTRS.items():
                for _index in range(self.args.iterations):
                    started = time.monotonic()
                    actual_size = getxattr_size(path, name)
                    phase['size_only_seconds'][name].append(time.monotonic() - started)
                    require(actual_size == size, 'digest size query mismatch')
                for _index in range(self.args.iterations):
                    started = time.monotonic()
                    actual = os.getxattr(path, name)
                    phase['value_seconds'][name].append(time.monotonic() - started)
                    require(actual == expected[name], 'digest differs from current edited content')
            os.chmod(path, 0o755 if epoch == 0 else 0o644)
            for name, size in ATTRS.items():
                require(getxattr_size(path, name) == size and os.getxattr(path, name) == expected[name],
                        'metadata-only chmod changed digest')
            self.sample(f'after_digest_epoch_{epoch}')
        require(path.read_bytes() == self.expected, 'edited bytes mismatch')
        require(phases[0]['expected_digests'] != phases[1]['expected_digests'], 'same-size edit did not change expected hashes')
        self.result['correctness'] = {'digest_size_queries': True, 'sha1_matches_git_blob': True,
            'blake3_matches_independent_hash': True, 'same_size_edit_invalidates': True, 'chmod_preserves_digest': True}
        self.stop()
        self.start()
        require(path.read_bytes() == self.expected, 'digest file remount bytes differ')
        for name in ATTRS:
            require(os.getxattr(path, name).hex() == phases[-1]['expected_digests'][name], 'remount digest differs')
        require((self.mount / 'tracked.txt').read_bytes() == b'unchanged base\n', 'tracked base changed')
        require(self.git_run('-C', self.mount, 'status', '--porcelain').decode().splitlines() == [' M large.bin'],
                'unexpected digest fixture Git status')
        self.result['correctness'].update(remount_persistence=True, unchanged_base_preserved=True)

    def startup_workload(self):
        """Separate snapshot, mapping and overlay costs, no full namespace walk."""
        self.result['startup'] = []
        self.result['fixture_scope'] = ('Many paths share three independently hashed blobs; '
            'Startup paths are sharded into at most 1000 files/directory to avoid quadratic Git fast-import fixture preparation. '
            'Path-scaling probe, NOT high unique-blob-cardinality mapping throughput. '
            'Distinct revision changes commit identity with the same tree; no tree-diff claim.')
        for revision in self.args.startup_revisions:
            if revision == 'distinct':
                old = self.git_run('-C', self.repo, 'rev-parse', 'HEAD').decode().strip()
                tree = self.git_run('-C', self.repo, 'rev-parse', 'HEAD^{tree}').decode().strip()
                commit = subprocess.run([str(self.git), '-C', str(self.repo), 'commit-tree', tree,
                    '-p', old], input=b'distinct startup identity\n', env=dict(self.env,
                        GIT_AUTHOR_NAME='Fixture', GIT_AUTHOR_EMAIL='fixture@example.invalid',
                        GIT_COMMITTER_NAME='Fixture', GIT_COMMITTER_EMAIL='fixture@example.invalid',
                        GIT_AUTHOR_DATE='2000-01-01T00:00:00Z', GIT_COMMITTER_DATE='2000-01-01T00:00:00Z'),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, timeout=30).stdout.strip()
                self.git_run('-C', self.repo, 'update-ref', 'refs/heads/main', commit.decode())
                self.overlay = self.directory / 'overlay-distinct'
            item = {'revision_kind': revision, 'commit': self.git_run('-C', self.repo, 'rev-parse', 'HEAD').decode().strip(),
                    'mapping_commands': [], 'mount_seconds': []}
            self.result['startup'].append(item)
            for attempt in range(2):
                label = f'{revision}-mapping-{attempt + 1}'
                log = self.directory / (label + '.stderr')
                started = time.monotonic()
                with log.open('wb') as err:
                    subprocess.run([str(self.binary), '--git', str(self.git), 'warm-blake3',
                        '--repo', str(self.repo), '--proxy-url', self.mapping.url, '--proxy-repo', 'fixture'],
                        env=self.env, stdout=subprocess.DEVNULL, stderr=err, check=True,
                        timeout=self.args.cell_budget)
                item['mapping_commands'].append({'seconds': time.monotonic() - started,
                    'attempt': attempt + 1, 'timings': self.metadata_timings(log)})
            for attempt in range(2):
                self.start()
                item['mount_seconds'].append(self.result['mount_seconds'][-1])
                self.sample(f'{revision}-mount-{attempt + 1}')
                self.stop()
                item.setdefault('mount_timings', []).append(self.metadata_timings(self.logs[-1]))
            self.save()

    @staticmethod
    def metadata_timings(path):
        return [json.loads(line.split('METADATA_TIMING ', 1)[1])
                for line in path.read_text(errors='replace').splitlines() if line.startswith('METADATA_TIMING ')]

    def execute(self):
        prepare_started = time.monotonic()
        self.prepare()
        self.result['fixture_prepare_seconds'] = time.monotonic() - prepare_started
        if self.workload == 'startup':
            started = time.monotonic()
            self.startup_workload()
            self.result['startup_continuous_seconds'] = time.monotonic() - started
            self.result['startup_continuous_scope'] = 'All same/distinct mapping commands, two mounts each, samples, shutdown and audits; excludes fixture generation. Individual phases are not summed as this wall duration.'
            self.result['status'] = 'complete'
            self.save()
            return
        self.start()
        self.sample('before_workload')
        if self.workload == 'rename':
            self.rename_workload()
        else:
            self.digest_workload()
        self.sample('after_remount')
        self.stop()
        self.result['status'] = 'complete'
        self.save()

    def collect_stats(self):
        if self.workload == 'startup':
            self.result['metadata_timings_available'] = any(
                item.get('mount_timings') and any(item['mount_timings'])
                for item in self.result.get('startup', []))
            return
        sessions = {'workload': {'reports': [], 'parse_errors': []},
                    'remount_verification': {'reports': [], 'parse_errors': []}}
        for index, log in enumerate(self.logs):
            session = sessions[('workload', 'remount_verification')[index]]
            session['log'] = log.name
            for number, line in enumerate(log.read_text(errors='replace').splitlines(), 1):
                if 'Overlay state: ' not in line:
                    continue
                try:
                    report = json.loads(line.split('Overlay state: ', 1)[1])
                    if not isinstance(report, dict):
                        raise ValueError('not an object')
                    session['reports'].append(report)
                except ValueError as error:
                    session['parse_errors'].append({'line': number, 'error': str(error)})
        self.result['overlay_shutdown_states'] = sessions
        required = self.args.require_candidate_stats and self.result['arm'] == 'candidate'
        proof = {'required': required, 'passed': None, 'checks': {},
                 'scope': 'Shutdown counters include untimed setup, verification and chmod probes. '
                          'Exact digest hash/hit totals distinguish size-only probes from value retrievals.'}
        self.result['mechanism_validation'] = proof
        if not required:
            return
        valid = all(len(value['reports']) == 1 and not value['parse_errors'] and
                    all(type(value['reports'][0].get(field)) is int and
                        0 <= value['reports'][0][field] <= 18446744073709551615
                        for field in WORK_FIELDS)
                    for value in sessions.values())
        proof['checks']['exactly_one_valid_report_per_session'] = valid
        if valid:
            initial = sessions['workload']['reports'][0]
            remount = sessions['remount_verification']['reports'][0]
            if self.workload == 'digest':
                # Per generation: 2N timed value calls, 2 chmod value probes,
                # one shared SHA1/BLAKE3 cache miss. Size-only probes contribute
                # neither a hash operation nor a cache hit. Two generations total.
                expected_hits = 4 * self.args.iterations + 2
                proof['expected'] = {'workload_hash_operations': 2,
                    'workload_hash_bytes': 2 * self.args.file_bytes,
                    'workload_cache_hits': expected_hits, 'cache_entries': 1,
                    'remount_hash_operations': 1, 'remount_hash_bytes': self.args.file_bytes,
                    'remount_cache_hits': 1}
                proof['checks'].update({
                    'one_paired_hash_per_content_generation': initial['digest_hash_operations'] == 2,
                    'one_stream_per_generation': initial['digest_hash_bytes'] == 2 * self.args.file_bytes,
                    'size_probes_do_not_hash_or_hit_value_cache': initial['digest_cache_hits'] == expected_hits,
                    'single_inode_cache_bounded': initial['digest_cache_entries'] == remount['digest_cache_entries'] == 1,
                    'remount_hashes_once_then_reuses_pair': remount['digest_hash_operations'] == 1 and
                        remount['digest_hash_bytes'] == self.args.file_bytes and remount['digest_cache_hits'] == 1,
                    'same_size_edits_and_chmod_correct': self.result.get('correctness', {}).get(
                        'same_size_edit_invalidates', False) and self.result.get('correctness', {}).get(
                        'chmod_preserves_digest', False),
                })
            else:
                # Each file replacement visits one source inode. The directory
                # fixture has four source nodes (root, file, subdirectory, file).
                # Include the untimed final move when an even number of toggles
                # leaves the directory at its original path.
                directory_moves = self.args.iterations + int(self.args.iterations % 2 == 0)
                expected_visits = self.args.iterations + 4 * directory_moves
                proof['expected_visited_entries'] = expected_visits
                proof['checks'].update({
                    'namespace_work_exact_affected_entries': initial['rename_visited_entries'] == expected_visits,
                    'verification_remount_does_not_rename': remount['rename_visited_entries'] == 0,
                })
        proof['passed'] = all(proof['checks'].values())
        if not proof['passed']:
            self.result['status'] = 'failed'
            self.result['mechanism_error'] = 'candidate namespace/digest work proof failed or statistics missing'


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--baseline-source', default='unknown (no source claim supplied)')
    parser.add_argument('--candidate-source', default='unknown (no source claim supplied)')
    parser.add_argument('--git', type=Path, required=True)
    parser.add_argument('--b3sum', type=Path, default=Path(shutil.which('b3sum') or '/usr/bin/b3sum'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--workloads', choices=['rename', 'digest', 'startup'], nargs='+', default=['rename', 'digest'])
    parser.add_argument('--entry-counts', type=int, nargs='+', default=[1000, 10000, 100000])
    parser.add_argument('--startup-revisions', nargs='+', choices=['same', 'distinct'], default=['same', 'distinct'])
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--iterations', type=int, default=64)
    parser.add_argument('--file-bytes', type=int, default=16 * 1024 * 1024)
    parser.add_argument('--mount-budget', type=float, default=60)
    parser.add_argument('--cell-budget', type=int, default=600)
    parser.add_argument('--require-candidate-stats', action='store_true',
                        help='require complete candidate bounded namespace/digest mechanism proof')
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if not (1 <= args.trials <= 20 and 1 <= args.iterations <= 4096 and
            4096 <= args.file_bytes <= 128 * 1024 * 1024 and
            1 <= args.mount_budget <= 120 and 30 <= args.cell_budget <= 3600 and
            all(1 <= count <= (1000000 if args.workloads == ['startup'] else 100000) for count in args.entry_counts) and
            len(set(args.entry_counts)) == len(args.entry_counts) and len(set(args.workloads)) == len(args.workloads)):
        parser.error('invalid bounds: trials1..20; iterations1..4096; file4KiB..128MiB; mount1..120s; cell30..3600s; unique entries1..100000 (startup-only1..1000000)/workloads')
    args.output = args.output.resolve()
    args.git = args.git.resolve(strict=True)
    binaries = {name: getattr(args, name).resolve(strict=True) for name in ('baseline', 'candidate')}
    paths = [args.git, *binaries.values()]
    if set(args.workloads) & {'digest', 'startup'}:
        args.b3sum = args.b3sum.resolve(strict=True)
        paths.append(args.b3sum)
    for path in paths:
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f'not an executable file: {path}')
    if args.output.exists():
        parser.error('--output must be fresh and nonexistent')
    helper = Path(common.__file__).resolve()
    result = {'schema': 1, 'kind': 'local actual-FUSE namespace/digest operations; not Bazel latency',
        'status': 'dry-run', 'cells': [],
        'config': {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        'binaries': {name: {'path': str(path), 'sha256': common.sha256(path),
                            'source_claim': getattr(args, f'{name}_source')} for name, path in binaries.items()},
        'git': {'path': str(args.git), 'sha256': common.sha256(args.git)},
        'b3sum': {'path': str(args.b3sum), 'sha256': common.sha256(args.b3sum)} if set(args.workloads) & {'digest', 'startup'} else None,
        'host': {'system': platform.system(), 'release': platform.release(), 'machine': platform.machine(),
                 'python': platform.python_version(), 'cpu_count': os.cpu_count()},
        'harness_sha256': common.sha256(Path(__file__)), 'helper_sha256': common.sha256(helper),
        'timing_contract': 'Startup: command/mount readiness plus opt-in internal phase telemetry; no additive overlapping sums. Other workloads: rename syscall only; digest getxattr query/value syscall only; all preparation/hashing/verification outside timers',
        'counter_scope': 'shutdown counters include untimed preparation, verification and chmod probes; optional, never synthesize zeros'}
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
    shutil.copyfile(__file__, args.output / 'benchmark_vfs_metadata.py')
    shutil.copyfile(helper, args.output / 'benchmark_vfs_iteration.py')
    result['status'] = 'running'
    common.checkpoint(args.output / 'results.json', result)
    signal.signal(signal.SIGALRM, common.deadline_handler)
    for trial in range(args.trials):
        order = list(binaries) if trial % 2 == 0 else list(reversed(binaries))
        for workload in args.workloads:
            counts = args.entry_counts if workload in ('rename', 'startup') else [0]
            for count in counts:
                for arm in order:
                    directory = args.output / f'trial-{trial + 1}-{workload}-{count}-{arm}'
                    cell = MetadataCell(directory, binaries[arm], args.git, fusermount, args, workload, count)
                    cell.result.update(trial=trial + 1, arm=arm)
                    result['cells'].append(cell.result)
                    print(f'start {directory.name}', flush=True)
                    signal.alarm(args.cell_budget)
                    cell_started = time.monotonic()
                    resources_before = resource.getrusage(resource.RUSAGE_CHILDREN)
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
                        if cell.mapping is not None:
                            try:
                                cell.result['mapping_fixture_requests'] = cell.mapping.requests
                                cell.mapping.close()
                            except BaseException as error:
                                cell.result.update(status='failed', mapping_cleanup_error=str(error))
                        cell.collect_stats()
                        cell.result['cell_wall_seconds'] = time.monotonic() - cell_started
                        resources_after = resource.getrusage(resource.RUSAGE_CHILDREN)
                        cell.result['reaped_child_resources'] = {
                            'user_cpu_seconds': resources_after.ru_utime - resources_before.ru_utime,
                            'system_cpu_seconds': resources_after.ru_stime - resources_before.ru_stime,
                            'block_inputs': resources_after.ru_inblock - resources_before.ru_inblock,
                            'block_outputs': resources_after.ru_oublock - resources_before.ru_oublock}
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
    for cell in result['cells']:
        key = f'{cell["workload"]}-{cell["entries"]}-{cell["arm"]}'
        summary = result['summary'].setdefault(key, {})
        values = dict(cell['operation_seconds'])
        for phase in cell.get('digest_phases', []):
            for kind in ('size_only_seconds', 'value_seconds'):
                for name, times in phase[kind].items():
                    values[f'epoch{phase["epoch"]}:{kind}:{name}'] = times
        for name, times in values.items():
            summary.setdefault(name, []).append(sum(times))
    for cell in result['cells']:
        for item in cell.get('startup', []):
            key = f'startup-{cell["entries"]}-{cell["arm"]}-{item["revision_kind"]}'
            summary = result['summary'].setdefault(key, {})
            for index, seconds in enumerate(item['mount_seconds']):
                summary.setdefault(f'mount-{index + 1}', []).append(seconds)
            for index, command in enumerate(item['mapping_commands']):
                summary.setdefault(f'mapping-command-{index + 1}', []).append(command['seconds'])
    for summary in result['summary'].values():
        for name, values in summary.items():
            summary[name] = {'median_seconds': statistics.median(values), 'min_seconds': min(values),
                             'max_seconds': max(values), 'trials': len(values),
                             'p95_seconds': sorted(values)[max(0, math.ceil(.95 * len(values)) - 1)],
                             'p95_note': 'nearest-rank; small trial counts do not establish stable tail latency'}
    common.checkpoint(args.output / 'results.json', result)
    print(json.dumps(result['summary'], indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
