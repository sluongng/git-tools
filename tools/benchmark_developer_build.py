#!/usr/bin/env python3
"""Bazel developer-build cells; print the plan unless --run is supplied.

Example: tools/benchmark_developer_build.py --bazel bazel --config rbe \
  --output artifacts/developer-build --source-edit crates/git-vfs/src/main.rs \
  crates/git-workspace/src/backend.rs --run //:git-vfs //:git-proxy

Population runs once per arm: a first normal build, NOT a forced cold cache.
Each arm gets a private output base; --trials repeats the other cells.
Repository, disk, and remote caches are never purged. Other
cells record a prerequisite build, then measure clean-local reuse, a no-op, or
one deterministic Rust edit. Public-const edits (default) change exported Rust
metadata; --edit-kind comment checks comment-only edits. Both test rebuild/cache
boundaries, not business logic or runtime throughput. No failed command is retried or discarded.
Run with no competing builds. Credentials are neither inspected nor recorded.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

PHASES = ('population', 'remote-reuse', 'noop', 'frontend-edit', 'lowerlayer-edit')
SECRET = re.compile(
    r'(?i)((?:x-buildbuddy-api-key|authorization|(?:remote|bes)_header|'
    r'api[_-]?key|access[_-]?token)\s*[:=]\s*)(?:Bearer\s+)?(?:"[^"\n]*"|[^\s\n]+)')


def redact(text):
    return SECRET.sub(r'\1[REDACTED]', text)



def scrub(value):
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, list):
        return [scrub(item) for item in value]
    if isinstance(value, dict):
        result = {key: scrub(item) for key, item in value.items()}
        sensitive = re.compile(r'(?i)(header|authorization|api.?key|access.?token)')
        for key in value:
            if sensitive.search(key):
                result[key] = '[REDACTED]'
        if sensitive.search(str(value.get('optionName', ''))):
            for key in ('optionValue', 'combinedForm'):
                if key in result:
                    result[key] = '[REDACTED]'
        return result
    return value


def sanitize_artifact(path):
    if not path.exists():
        return
    compressed = path.suffix == '.gz'
    data = gzip.decompress(path.read_bytes()) if compressed else path.read_bytes()
    text = data.decode(errors='replace')
    try:
        safe = json.dumps(scrub(json.loads(text))) + '\n'
    except json.JSONDecodeError:
        lines = []
        for line in text.splitlines():
            try:
                lines.append(json.dumps(scrub(json.loads(line))))
            except json.JSONDecodeError:
                lines.append(redact(line))
        safe = '\n'.join(lines) + '\n'
    path.write_bytes(gzip.compress(safe.encode()) if compressed else safe.encode())

def git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), *args])


def sha(data):
    return hashlib.sha256(data).hexdigest()


def source_state(root):
    """Hash tracked and nonignored untracked files; do not inspect ignored rc."""
    names = sorted(set(git(root, 'ls-files', '-z', '--cached', '--others',
                           '--exclude-standard').split(b'\0')) - {b''})
    digest = hashlib.sha256()
    count = 0
    for name in names:
        path = root / os.fsdecode(name)
        digest.update(name + b'\0')
        if path.is_symlink():
            digest.update(b'link\0' + os.fsencode(os.readlink(path)))
        elif path.is_file():
            digest.update(b'file\0' + path.read_bytes())
        else:
            digest.update(b'missing\0')
        count += 1
    return {'head': git(root, 'rev-parse', 'HEAD').decode().strip(),
            'diff_sha256': sha(git(root, 'diff', '--binary', 'HEAD')),
            'source_sha256': digest.hexdigest(), 'files': count}


def bep_metrics(path):
    metrics = {'buildMetrics': [], 'finished': [], 'parse_errors': []}
    if not path.exists():
        return metrics
    for index, line in enumerate(path.read_text(errors='replace').splitlines(), 1):
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            metrics['parse_errors'].append({'line': index, 'error': str(exc)})
            continue
        if 'buildMetrics' in event:
            value = event['buildMetrics']
            metrics['buildMetrics'].append({key: value[key] for key in (
                'actionSummary', 'actionCacheStatistics', 'timingMetrics',
                'memoryMetrics', 'artifactMetrics', 'buildGraphMetrics') if key in value})
        if 'finished' in event:
            metrics['finished'].append(event['finished'])
    return metrics


class Benchmark:
    def __init__(self, args, root, plan):
        self.args, self.root, self.plan = args, root, plan
        self.result = {'schema_version': 1, 'plan': plan, 'commands': [],
                       'notes': ['No global caches purged; population is not forced cold.',
                                 'Preparation builds and cleans are timed but not measured cells.',
                                 'BEP metrics are raw counters, not inferred cache-hit rates.',
                                 'remote-reuse is clean-local reuse; the local arm has no remote cache.',
                                 'Edits test rebuild boundaries, not business logic or runtime throughput.',
                                 'Public-const edits change exported Rust metadata; comments may not.']}
        self.sequence = 0
        self.result['edits'] = []

    def save(self):
        temporary = self.args.output / 'results.json.tmp'
        temporary.write_text(json.dumps(self.result, indent=2) + '\n')
        temporary.replace(self.args.output / 'results.json')

    def command(self, command, label, *, arm=None, phase=None, trial=None,
                measured=False, bep=None):
        self.sequence += 1
        log = self.args.output / f'{self.sequence:03d}-{label}.log'
        entry = {'command': command, 'label': label, 'arm': arm, 'phase': phase,
                 'trial': trial, 'measured': measured, 'log': log.name}
        self.result['commands'].append(entry)
        self.save()
        start = time.monotonic()
        # Capture output before writing, so key-bearing diagnostics are redacted.
        process = subprocess.run(command, cwd=self.root, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, check=False)
        entry.update(wall_seconds=time.monotonic() - start, exit_code=process.returncode)
        output = redact(process.stdout.decode(errors='replace'))
        log.write_text(output)
        if bep is not None:
            for artifact in bep.parent.iterdir():
                if artifact.is_file():
                    sanitize_artifact(artifact)
            entry['bep'] = str(bep.relative_to(self.args.output))
            entry['metrics'] = bep_metrics(bep)
        self.save()
        if process.returncode:
            raise RuntimeError(f'{label} failed with exit {process.returncode}; see {log}')
        return output

    def bazel_command(self, arm, verb):
        command = [self.args.bazel, f'--output_base={self.args.output / ("bazel-" + arm)}', verb]
        if verb == 'build':
            command += ['--noannounce_rc', '--color=no', '--curses=no']
            if arm != 'local':
                command.append(f'--config={arm}')
            else:
                # Disable inherited remote endpoints for the local comparison arm.
                command += ['--remote_executor=', '--remote_cache=', '--disk_cache=',
                            '--remote_download_outputs=all']
            command.append(f'--jobs={self.args.jobs}')
        return command

    def build(self, arm, phase, trial, role):
        label = f'{arm}-{phase}-{trial}-{role}'
        directory = self.args.output / label
        directory.mkdir()
        bep, profile = directory / 'bep.json', directory / 'profile.json.gz'
        command = self.bazel_command(arm, 'build') + [
            f'--build_event_json_file={bep}', f'--profile={profile}']
        if self.args.execution_log:
            command.append(f'--execution_log_json_file={directory / "execution.json"}')
        command += self.args.targets
        self.command(command, label, arm=arm, phase=phase, trial=trial,
                     measured=role == 'measured', bep=bep)

    def run(self):
        # Bazel may write expanded credentials into raw BEP/profile data before
        # post-command scrubbing. Keep the entire live capture private too.
        self.args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
        self.result['source_before'] = source_state(self.root)
        self.save()
        try:
            self.result['bazel_version'] = self.command([self.args.bazel, '--version'], 'bazel-version').strip()
            self.result['git_version'] = self.command(['git', '--version'], 'git-version').strip()
            self.result['python_version'] = sys.version
            if self.args.execution_log:
                help_text = self.command([self.args.bazel, 'help', 'build'], 'bazel-help-build')
                if '--execution_log_json_file' not in help_text:
                    raise RuntimeError('This Bazel does not advertise --execution_log_json_file')
            for trial, trial_arms in enumerate(self.plan['trial_arm_order'], 1):
                for arm in trial_arms:
                    for phase in self.plan['phases']:
                        if phase == 'population' and trial != 1:
                            continue
                        if phase != 'population':
                            self.build(arm, phase, trial, 'prepare')
                        if phase == 'remote-reuse':
                            self.command(self.bazel_command(arm, 'clean'),
                                         f'{arm}-{phase}-{trial}-clean', arm=arm,
                                         phase=phase, trial=trial)
                        path = self.edit_path(phase)
                        if path is None:
                            self.build(arm, phase, trial, 'measured')
                        else:
                            original = path.read_bytes()
                            marker = f'{phase}-trial-{trial}'
                            if self.args.edit_kind == 'public-const':
                                addition = f'pub const GIT_TOOLS_BENCHMARK_REVISION: &str = "{marker}";'
                            else:
                                addition = f'// developer-build benchmark: {marker}'
                            edited = original + b'\n' + addition.encode() + b'\n'
                            self.result['edits'].append({
                                'arm': arm, 'phase': phase, 'trial': trial,
                                'path': str(path.relative_to(self.root)), 'marker': marker,
                                'edit_kind': self.args.edit_kind,
                                'original_sha256': sha(original), 'edited_sha256': sha(edited)})
                            self.save()
                            try:
                                path.write_bytes(edited)
                                self.build(arm, phase, trial, 'measured')
                            finally:
                                path.write_bytes(original)
                                if path.read_bytes() != original:
                                    raise RuntimeError(f'Could not restore source: {path}')
            self.result['status'] = 'completed'
        except BaseException as exc:
            self.result['status'] = 'failed'
            self.result['error'] = redact(f'{type(exc).__name__}: {exc}')
            raise
        finally:
            self.result['source_after'] = source_state(self.root)
            self.result['source_restored'] = self.result['source_after'] == self.result['source_before']
            if not self.result['source_restored']:
                self.result['status'] = 'failed'
                self.result['source_error'] = 'Source tree changed during the benchmark'
            self.save()
            if not self.result['source_restored']:
                raise RuntimeError('Source tree changed during the benchmark; see results.json')

    def edit_path(self, phase):
        if phase not in ('frontend-edit', 'lowerlayer-edit'):
            return None
        index = 0 if phase == 'frontend-edit' else 1
        return self.root / self.args.source_edit[index]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--bazel', default='bazel')
    parser.add_argument('--output', required=True, type=Path, help='Fresh artifact directory')
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--jobs', type=int, default=16, help='Matched build concurrency after all configs')
    parser.add_argument('--config', help='Add this configured arm alongside local, e.g. rbe')
    parser.add_argument('--arm', action='append', help='Override arms; repeat for local/config names')
    parser.add_argument('--phase', action='append', choices=PHASES, help='Run only selected phase(s)')
    parser.add_argument('--source-edit', nargs=2, metavar=('FRONTEND_RS', 'LOWERLAYER_RS'))
    parser.add_argument('--edit-kind', choices=('comment', 'public-const'), default='public-const',
                        help='Exported-metadata change (default), or comment-only boundary check')
    parser.add_argument('--execution-log', action='store_true', help='Require supported JSON execution-log flag')
    parser.add_argument('--run', action='store_true')
    parser.add_argument('targets', nargs='*', default=['//:git-vfs', '//:git-proxy'])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    args.output = args.output.resolve()
    args.bazel = shutil.which(args.bazel) or str(Path(args.bazel).resolve())
    if args.output.exists():
        parser.error('--output must not exist')
    if args.output.is_relative_to(root):
        ignored = subprocess.run(['git', '-C', str(root), 'check-ignore', '--quiet',
                                  str(args.output / 'benchmark-output')], check=False)
        if ignored.returncode != 0:
            parser.error('in-repository --output must be gitignored (or use an external directory)')
    if args.trials < 1:
        parser.error('--trials must be positive')
    if args.jobs < 1:
        parser.error('--jobs must be positive')
    phases = args.phase or list(PHASES)
    arms = args.arm or (['local', args.config] if args.config else ['local'])
    if len(set(phases)) != len(phases):
        parser.error('phases must be unique')
    if len(set(arms)) != len(arms) or any(not re.fullmatch(r'[A-Za-z0-9_-]+', arm) for arm in arms):
        parser.error('arms must be unique simple config names')
    if any(phase.endswith('-edit') for phase in phases):
        if not args.source_edit:
            parser.error('edit phases require --source-edit FRONTEND_RS LOWERLAYER_RS')
        for name in args.source_edit:
            path = (root / name).resolve()
            if not path.is_relative_to(root) or path.suffix != '.rs' or not path.is_file():
                parser.error(f'edit source must be an existing in-repository Rust file: {name}')
            relative = str(path.relative_to(root))
            if not git(root, 'ls-files', '--error-unmatch', '--', relative):
                parser.error(f'edit source must be tracked: {name}')
            if git(root, 'diff', '--name-only', 'HEAD', '--', relative):
                parser.error(f'edit source must have clean index and working-tree bytes: {name}')
    plan = {'root': str(root), 'bazel': args.bazel, 'output': str(args.output),
            'arms': arms, 'phases': phases, 'trials': args.trials, 'jobs': args.jobs,
            'targets': args.targets, 'source_edit': args.source_edit, 'edit_kind': args.edit_kind,
            'trial_arm_order': [arms[index % len(arms):] + arms[:index % len(arms)]
                                for index in range(args.trials)],
            'execution_log': args.execution_log,
            'population_runs_per_arm': 1 if 'population' in phases else 0}
    print(json.dumps(plan, indent=2))
    if args.run:
        Benchmark(args, root, plan).run()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(redact(str(error)), file=sys.stderr)
        sys.exit(1)
