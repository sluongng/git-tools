#!/usr/bin/env python3
"""Matched Bazel remote-cache endpoint cells; private artifacts, dry-run by default.

The plan JSON contains cwd, source_revision, and command (an argv array). Use
{output_base}, {output_user_root}, {artifacts}, and {endpoint} placeholders.
Each invocation owns a new output root. --noop additionally measures the same
server with local state; it is not another remote-cache reuse sample. This does
not purge remote/OS/repository caches and never claims they are cold.
"""
from __future__ import annotations
import argparse
import hashlib
import gzip
import glob
import json
import os
import platform
from pathlib import Path
import subprocess
import statistics
import time

from benchmark_developer_build import bep_metrics, redact, sanitize_artifact


def run(args):
    plan = json.loads(args.plan.read_text())
    if not isinstance(plan.get('command'), list) or not all(isinstance(x, str) for x in plan['command']):
        raise ValueError('command must be an argv array')
    root = args.output.resolve()
    values = {'output_base': str(root / 'output-base'),
              'output_user_root': str(root / 'output-user-root'),
              'artifacts': str(root), 'endpoint': args.endpoint}
    command = [value.format_map(values) for value in plan['command']]
    if any(any(flag in x for flag in ('--remote_header=', '--remote_cache_header=', '--remote_exec_header=', '--bes_header=')) for x in command):
        raise ValueError('Use credential helpers, never credentials in argv')
    result = {'schema_version': 1, 'label': args.label, 'source_revision': plan['source_revision'],
              'cwd': plan['cwd'], 'endpoint': args.endpoint, 'command': command,
              'plan_sha256': hashlib.sha256(args.plan.read_bytes()).hexdigest(),
              'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'cache_state': args.cache_state, 'samples': [],
              'host': {'platform':platform.platform(), 'logical_cpus':os.cpu_count()},
              'notes': ['Fresh private output root per cell; no remote/OS cache purge.',
                        'Repository/archive caches are external shared preparation; remote cache preexists.',
                        'Command wall includes client startup and finalization; profile spans overlap.',
                        'Same-server no-op is local incremental performance, not remote cache reuse.']}
    if not args.run:
        print(json.dumps(result, indent=2)); return
    root.mkdir(parents=True, exist_ok=False)
    os.chmod(root, 0o700)
    def save():
        (root / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    def daemon_metrics():
        if args.status_command_json is None:
            return None
        status_command = json.loads(args.status_command_json.read_text())
        try:
            response = subprocess.run(status_command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                      timeout=15, check=True)
            report = json.loads(response.stdout).get('report', {})
            return {key:report[key] for key in ('cache','proxy') if key in report}
        except (subprocess.SubprocessError, ValueError):
            return {'unavailable': True}
    def execute(argv, label):
        before = daemon_metrics()
        start = time.perf_counter()
        with (root / (label + '.log')).open('wb') as output:
            timed_out = False
            try:
                process = subprocess.run(argv, cwd=plan['cwd'], stdout=output, stderr=subprocess.STDOUT,
                                         timeout=args.timeout, check=False)
                status = process.returncode
            except subprocess.TimeoutExpired:
                timed_out = True
                status = 124
        wall = time.perf_counter() - start
        log = root / (label + '.log')
        log.write_text(redact(log.read_text(errors='replace')))
        sample = {'phase': label, 'wall_seconds': wall, 'exit_code': status, 'timed_out': timed_out}
        patterns = plan.get('verify_outputs', [])
        if status == 0 and patterns:
            read_start = time.perf_counter()
            verified = []
            for pattern in patterns:
                paths = sorted(set(glob.glob(pattern.format_map(values))))
                if not paths:
                    sample['output_error'] = 'Expected output glob did not match'
                    status = 1
                for name in paths:
                    path = Path(name)
                    try:
                        with path.open('rb') as content:
                            digest = hashlib.file_digest(content, 'sha256').hexdigest()
                        verified.append({'path': str(path), 'bytes': path.stat().st_size, 'sha256':digest})
                    except OSError:
                        sample['output_error'] = 'Expected output could not be read'
                        status = 1
            sample['first_output_read'] = {'wall_seconds':time.perf_counter()-read_start,
                                           'outputs':verified}
            sample['command_plus_first_read_seconds'] = wall + sample['first_output_read']['wall_seconds']
            sample['command_through_first_output_seconds'] = time.perf_counter()-start
            sample['exit_code'] = status
        if before is not None:
            sample['daemon_before'] = before
            sample['daemon_after'] = daemon_metrics()
        bep = root / 'build.bep.jsonl'
        profile = root / 'build.profile.gz'
        if profile.exists():
            try:
                events = json.loads(gzip.decompress(profile.read_bytes())).get('traceEvents', [])
            except (OSError, ValueError):
                events = []
                sample['profile_error'] = 'incomplete or invalid profile; failed attempt retained'
            costs = {}
            for event in events:
                if event.get('ph') != 'X' or not isinstance(event.get('dur'), (int,float)):
                    continue
                name = event.get('name', '')
                if not any(term in name.lower() for term in ('remote cache', 'remote execution', 'download', 'merkle', 'check cache hit')):
                    continue
                value = costs.setdefault(name, {'spans':0, 'cumulative_seconds':0, '_intervals':[], '_durations':[]})
                value['spans'] += 1
                value['cumulative_seconds'] += event['dur'] / 1e6
                value['_durations'].append(event['dur']/1e6)
                value['_intervals'].append((event['ts'],event['ts']+event['dur']))
            for value in costs.values():
                intervals = sorted(value.pop('_intervals'))
                begin,end = intervals[0]
                total = 0
                for left,right in intervals[1:]:
                    if left > end:
                        total += end-begin
                        begin,end = left,right
                    else:
                        end = max(end,right)
                value['union_seconds'] = (total+end-begin)/1e6
                durations = sorted(value.pop('_durations'))
                value['median_seconds'] = statistics.median(durations)
                value['p99_seconds'] = durations[min(len(durations)-1,int(len(durations)*.99))]
            sample['profile_costs'] = costs
            sample['profile_costs_note'] = 'Cumulative spans include concurrency; unions for different names also overlap and are not additive.'
        if bep.exists():
            sample['bep'] = bep_metrics(bep)
            summaries = []
            test_runs = {}
            for line in bep.read_text().splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue  # bep_metrics retains parse errors for failed/truncated runs.
                if 'testResult' in event:
                    strategy = event['testResult'].get('executionInfo', {}).get('strategy', 'unknown')
                    test_runs[strategy] = test_runs.get(strategy, 0) + 1
                if 'testSummary' in event:
                    s = event['testSummary']; summaries.append({k:s[k] for k in
                        ('overallStatus','totalRunCount','localActionCached','remoteActionCached') if k in s})
            sample['test_summaries'] = summaries
            sample['test_run_strategies'] = test_runs
        result['samples'].append(sample)
        for path in root.iterdir():
            if path.is_file() and path.suffix in ('.jsonl', '.gz'):
                sanitize_artifact(path)
        save()
        return status
    save()
    try:
        status = execute(command, 'fresh-server')
        if status == 0 and args.noop:
            for name in ('build.bep.jsonl', 'build.profile.gz'):
                source = root / name
                if source.exists(): source.rename(root / ('fresh-' + name))
            execute(command, 'same-server-noop')
    finally:
        # Only our own root: no bazel clean, no main-checkout server disruption.
        index = next(i for i,x in enumerate(command) if x in ('build','test'))
        shutdown = ([value.format_map(values) for value in plan['shutdown_command']]
                    if 'shutdown_command' in plan else command[:index] + ['shutdown'])
        start = time.perf_counter()
        with (root / 'shutdown.log').open('wb') as output:
            stopped = subprocess.run(shutdown, cwd=plan['cwd'], stdout=output, stderr=subprocess.STDOUT,
                                     timeout=90, check=False)
        result['shutdown'] = {'exit_code': stopped.returncode, 'wall_seconds': time.perf_counter()-start}
        save()
    print(json.dumps({'result': str(root / 'result.json'), 'samples': [
        {k:s[k] for k in ('phase','wall_seconds','exit_code')} for s in result['samples']]}))
    if any(sample['exit_code'] for sample in result['samples']):
        raise SystemExit(1)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--endpoint',required=True)
    p.add_argument('--label',required=True)
    p.add_argument('--cache-state',required=True)
    p.add_argument('--timeout',type=int,default=1200)
    p.add_argument('--status-command-json',type=Path,help='Optional workspace status argv JSON; retain only cache/proxy counters')
    p.add_argument('--noop',action='store_true')
    p.add_argument('--run',action='store_true')
    run(p.parse_args())

if __name__ == '__main__': main()
