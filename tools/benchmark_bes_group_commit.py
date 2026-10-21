#!/usr/bin/env python3
"""Matched optimized BES admission samples. No production backend is contacted."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True,
                        help='optimized bes-proxy integration_tests binary built by Bazel')
    parser.add_argument('--output', type=Path, required=True, help='fresh private artifact directory')
    parser.add_argument('--trials', type=int, default=5)
    parser.add_argument('--modes', nargs='+', default=['1:0', '64:0', '64:1'],
                        help='batch event limit:maximum wait milliseconds')
    parser.add_argument('--workloads', nargs='+', default=['burst', 'concurrent', 'large', 'sparse'],
                        choices=['burst', 'concurrent', 'large', 'sparse'])
    args = parser.parse_args()
    if args.trials <= 0:
        parser.error('--trials must be positive')
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    state = args.output.resolve() / 'state'
    state.mkdir()
    workloads = {
        'burst': (1, 4096, 1024, False),
        'concurrent': (8, 512, 1024, False),
        'large': (1, 256, 65536, False),
        'sparse': (1, 128, 1024, True),
    }
    modes = [tuple(map(int, mode.split(':'))) for mode in args.modes]
    report = {'scope': 'component: Unix ingress, gzip, real SyncAll on chosen filesystem; fake upstream ACK gate',
              'binary': str(args.binary.resolve()),
              'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'diff_sha256': hashlib.sha256(subprocess.check_output(['git', 'diff', 'HEAD'])).hexdigest(),
              'harness_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'batch_max_bytes': 256 * 1024,
              'system': platform.uname()._asdict(),
              'cpus': os.cpu_count(), 'filesystem': subprocess.check_output(
                  ['findmnt', '-T', str(state), '-n', '-o', 'SOURCE,FSTYPE,OPTIONS'], text=True).strip(),
              'revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
              'samples': [], 'summary': []}

    def save():
        (args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')

    save()
    for workload in args.workloads:
        streams, count, payload, sparse = workloads[workload]
        for trial in range(args.trials):
            # Alternate order; fresh process/spool per sample. No warmup sample
            # is discarded, and setup/remote drain are reported separately.
            for events, delay in (modes if trial % 2 == 0 else list(reversed(modes))):
                label = f'{workload}-{trial}-batch{events}-wait{delay}'
                log = args.output / (label + '.log')
                resources = args.output / (label + '.resources')
                env = os.environ.copy()
                env.update(TMPDIR=str(state), BES_BENCH_STREAMS=str(streams), BES_BENCH_EVENTS=str(count),
                           BES_BENCH_PAYLOAD_BYTES=str(payload), BES_BENCH_SPARSE=str(int(sparse)),
                           BES_BENCH_BATCH_EVENTS=str(events), BES_BENCH_DELAY_MS=str(delay),
                           BES_BENCH_BATCH_BYTES=str(256 * 1024))
                command = [str(args.binary.resolve()), '--ignored', '--exact',
                           'benchmark_group_commit', '--nocapture']
                with log.open('wb') as output:
                    process = subprocess.Popen(command, env=env, stdout=output, stderr=subprocess.STDOUT)
                    deadline = time.monotonic() + 180
                    timed_out = False
                    while True:
                        pid, status, usage = os.wait4(process.pid, os.WNOHANG)
                        if pid:
                            break
                        if time.monotonic() >= deadline:
                            timed_out = True
                            process.kill()
                            _, status, usage = os.wait4(process.pid, 0)
                            break
                        time.sleep(.02)
                    process.returncode = os.waitstatus_to_exitcode(status)
                exit_code = 124 if timed_out else process.returncode
                sample = {'workload': workload, 'trial': trial, 'batch_events': events,
                          'delay_ms': delay, 'exit_code': exit_code, 'timed_out': timed_out,
                          'load': os.getloadavg()}
                for line in log.read_text(errors='replace').splitlines():
                    if line.startswith('BES_BENCH '):
                        sample.update(json.loads(line[len('BES_BENCH '):]))
                sample.update(process_cpu_seconds=usage.ru_utime + usage.ru_stime, max_rss_kib=usage.ru_maxrss)
                resources.write_text(json.dumps({'user_seconds':usage.ru_utime,
                    'system_seconds':usage.ru_stime,'max_rss_kib':usage.ru_maxrss}) + '\n')
                report['samples'].append(sample)
                save()
                if exit_code != 0 or 'wall_seconds' not in sample:
                    raise SystemExit(f'{label} failed; raw attempt preserved at {log}')
                print(json.dumps(sample), flush=True)
    for workload in args.workloads:
        for events, delay in modes:
            samples = [s for s in report['samples'] if s['workload'] == workload
                       and s['batch_events'] == events and s['delay_ms'] == delay]
            summary = {'workload': workload, 'batch_events': events, 'delay_ms': delay,
                       'n': len(samples)}
            for key in ('wall_seconds', 'events_per_second', 'ack_p50_ms', 'ack_p95_ms', 'ack_p99_ms',
                        'admission_commits', 'drain_seconds', 'admit_and_drain_seconds',
                        'process_cpu_seconds', 'max_rss_kib'):
                values = [s[key] for s in samples]
                summary[key] = {'median': statistics.median(values), 'min': min(values), 'max': max(values)}
            report['summary'].append(summary)
    save()
    print(json.dumps(report['summary'], indent=2))


if __name__ == '__main__':
    main()
