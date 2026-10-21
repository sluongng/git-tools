#!/usr/bin/env python3
"""Matched Bazel remote-cache endpoint cells; private artifacts, dry-run by default.

The plan JSON contains cwd, source_revision, and command (an argv array). Use
{output_base}, {output_user_root}, {artifacts}, and {endpoint} placeholders.
The command must use --output_base={output_base} before build/test and exactly
one --remote_cache={endpoint}. Optional --remote_executor must match (or be
empty for local execution). Endpoint options are moved after other build
options, including --config. Startup --bazelrc files and Bazel-compatible
executable wrappers are trusted; shell command strings are unsupported.
Use --name=value for build/test options not listed by this harness. Common
boolean options and split values such as --config NAME and --test_arg VALUE
are supported; ambiguous bare options are rejected before execution.
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
import re
from pathlib import Path
import subprocess
import statistics
import time

from benchmark_developer_build import bep_metrics, redact, sanitize_artifact


STARTUP_VALUES = set(('bazelrc command_port connect_timeout_secs '
    'local_startup_timeout_secs digest_function unix_digest_hash_attribute_name '
    'server_javabase host_jvm_args host_jvm_profile io_nice_level install_base '
    'macos_qos_class max_idle_secs output_base output_user_root server_jvm_out '
    'failure_detail_out experimental_cgroup_parent extra_classpath').split())
STARTUP_BOOLS = set(('batch batch_cpu_scheduling block_for_lock quiet client_debug '
    'preemptible fatal_event_bus_exceptions host_jvm_debug autodetect_server_javabase '
    'idle_server_tasks shutdown_on_low_sys_mem '
    'ignore_all_rc_files unlimit_coredumps write_command_log windows_enable_symlinks '
    'experimental_remote_repo_contents_cache '
    'experimental_run_in_user_cgroup home_rc system_rc workspace_rc').split())
# Bazel 9.2 FlagInfo.requires_value=false includes boolean, TriState,
# BoolOrEnumConverter (subcommands), and Void expansion options.
BUILD_NULLARY = set(('show_progress show_loading_progress subcommands '
    'sandbox_debug verbose_failures keep_going '
    'build_event_publish_all_actions '
    'remote_accept_cached remote_upload_local_results remote_cache_compression '
    'remote_download_toplevel remote_download_minimal remote_download_all '
    'google_default_credentials '
    'test_keep_going cache_test_results build_tests_only check_up_to_date '
    'check_tests_up_to_date').split())
BUILD_VALUES = set(('config test_arg test_env test_filter test_output test_timeout '
    'show_result '
    'test_size_filters test_tag_filters build_tag_filters runs_per_test '
    'profile build_event_json_file build_event_binary_file build_event_text_file '
    'execution_log_json_file execution_log_binary_file execution_log_compact_file '
    'repository_cache repo_contents_cache disk_cache jobs color curses '
    'remote_instance_name remote_timeout remote_download_outputs credential_helper '
    'strategy spawn_strategy compilation_mode platform_suffix platforms '
    'extra_execution_platforms extra_toolchains modify_execution_info '
    'experimental_remote_cache_ttl experimental_workspace_rules_log_file').split())


def owned_commands(plan, values):
    """Validate the Bazel argv interface, not an arbitrary shell command.

    A direct executable may be Bazelisk or a trusted Bazel-compatible wrapper.
    Transparent env assignments are supported; shell/interpreter command prefixes
    are not. RC files remain caller-trusted (including credential helpers).
    """
    if not values['endpoint']:
        raise ValueError('A nonempty remote cache endpoint is required')
    raw = plan.get('command')
    if (not isinstance(raw, list) or not raw or not all(isinstance(x, str) for x in raw)
            or not raw[0]):
        raise ValueError('command must be a nonempty argv array')
    command = [value.format_map(values) for value in raw]
    if any(x.split('=', 1)[0] in ('--remote_header', '--remote_cache_header',
            '--remote_exec_header', '--bes_header') for x in command):
        raise ValueError('Use credential helpers, never credentials in argv')
    index = 1
    if Path(command[0]).name == 'env':
        while index < len(command) and re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*=.*', command[index], re.DOTALL):
            index += 1
        if index == len(command) or not command[index] or command[index].startswith('-'):
            raise ValueError('env prefix must contain only assignments and a Bazel executable')
        index += 1
    startup = {}
    while index < len(command) and command[index].startswith('--'):
        option, separator, value = command[index][2:].partition('=')
        boolean = option in STARTUP_BOOLS or (option.startswith('no') and option[2:] in STARTUP_BOOLS)
        if boolean and not separator:
            if option == 'batch':
                raise ValueError('Fresh-server cells require server mode, not --batch')
            index += 1
            continue
        if option not in STARTUP_VALUES:
            raise ValueError('Unsupported Bazel startup option; use a Bazel-compatible argv interface')
        if not separator:
            index += 1
            if index == len(command) or command[index].startswith('--'):
                raise ValueError('Missing Bazel startup option value')
            value = command[index]
        startup.setdefault(option, []).append(value)
        index += 1
    if index == len(command) or command[index] not in ('build', 'test'):
        raise ValueError('command must invoke Bazel build or test after startup options')
    for option in ('output_base', 'output_user_root'):
        supplied = startup.get(option, [])
        if (option == 'output_base' or supplied) and supplied != [values[option]]:
            raise ValueError(f'--{option} must select its owned placeholder exactly once')
    prefix = command[:index]
    build = command[index:]
    # Parse arity before moving endpoints: Bazel consumes the next token as a
    # value even when it looks like an option or the -- target separator.
    selected = {}
    retained, targets = [build[0]], []
    index = 1
    while index < len(build):
        item = build[index]
        if item == '--':
            targets = build[index:]
            break
        option, separator, value = item.partition('=')
        if option in ('--output_base', '--output_user_root', '--invocation_policy', '--flagfile'):
            raise ValueError('Ownership/startup or opaque option overrides are not supported after build/test')
        if option in ('--remote_cache', '--remote_executor'):
            if not separator:
                index += 1
                if index == len(build):
                    raise ValueError('Missing remote endpoint value')
                value = build[index]
            selected.setdefault(option, []).append(value)
        else:
            retained.append(item)
            if item.startswith('-'):
                name = option.removeprefix('--')
                boolean = name in BUILD_NULLARY or (name.startswith('no') and name[2:] in BUILD_NULLARY)
                if not item.startswith('--'):
                    raise ValueError('Use long --name=value build/test options, not short options')
                if not separator and name in BUILD_VALUES:
                    index += 1
                    if index == len(build):
                        raise ValueError('Missing build/test option value')
                    retained.append(build[index])
                elif not separator and not boolean:
                    raise ValueError('Ambiguous build/test option; use --name=value form')
        index += 1
    if selected.get('--remote_cache') != [values['endpoint']]:
        raise ValueError('--remote_cache must select the cell endpoint exactly once')
    if '--remote_executor' in selected and selected['--remote_executor'] not in ([values['endpoint']], ['']):
        raise ValueError('--remote_executor must match the cell endpoint exactly once')
    command = prefix + retained + [f'{key}={items[0]}' for key, items in selected.items()] + targets
    shutdown = prefix + ['shutdown']
    if 'shutdown_command' in plan:
        supplied = plan['shutdown_command']
        if (not isinstance(supplied, list) or not all(isinstance(x, str) for x in supplied)
                or [x.format_map(values) for x in supplied] != shutdown):
            raise ValueError('shutdown_command must equal the validated owned startup plus shutdown')
    return command, shutdown


def run(args):
    plan = json.loads(args.plan.read_text())
    root = args.output.resolve()
    values = {'output_base': str(root / 'output-base'),
              'output_user_root': str(root / 'output-user-root'),
              'artifacts': str(root), 'endpoint': args.endpoint}
    command, shutdown = owned_commands(plan, values)
    result = {'schema_version': 1, 'label': args.label, 'source_revision': plan['source_revision'],
              'cwd': plan['cwd'], 'endpoint': args.endpoint, 'command': command, 'shutdown_command': shutdown,
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
