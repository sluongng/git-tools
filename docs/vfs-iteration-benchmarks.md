# Iterative VFS workspaces

This campaign implements the [lifecycle design](design/vfs-iteration-lifecycle.md):
ranged mutable files, reference-safe reclamation, bounded fsmonitor history,
subtree-scoped rename, generation-cached digests and scheduled immutable reads.
The final comparison uses frozen `c0687ee` and candidate `9be0196`, not EdenFS.
The local table also includes earlier stage checkpoints.

## Local results

Three rotated trials per workload, actual Linux FUSE, fresh private overlays.

| Workload | Baseline | Candidate |
|---|---:|---:|
| RSS after 256 create/delete operations | 203.14 MiB | 9.99 MiB |
| 64 file replacements among 100k entries | 808.59 ms | 17.06 ms |
| 64 SHA1 size probes on a 16 MiB edit | 407.96 ms | 0.40 ms |
| Unrelated local stat/read during a stalled fetch | Blocked beyond 1 s | Completed before release |
| Repeated range pass under cache pressure | 154.39 ms | 38.90 ms |
| Sampled VFS RSS peak reading an 80 MiB blob | 90.92 MiB | 7.53 MiB |
| 64 mutable 4 KiB appends | 831.43 ms | 106.12 ms |

These measurements come from successive stage checkpoints, not one combined
end-to-end workload. Final integrated mutable range/churn checks also passed.
Cold immutable reads were 8% slower; 12,000 metadata changes were 9% slower and
used more WAL/RSS. Bounds on logical history do not imply physical LSM compaction.
The design records each stage's samples, mechanism counters and retained failures.

The immutable cache is anonymous disk content: 64 MiB / 256 files by default.
Reply buffers have a separate 64 MiB admission budget. RSS figures sample the VFS
PID, excluding native Git children. Mutable first copy-up remains serialized and
uses the compatibility whole-blob reader; namespace metadata remains eager.

## BuildBuddy RBE acceptance

The [primary samples and proof](benchmarks/2026-10-19-vfs-iterative-rbe.json) passed:
**54 builds / 162 uncached remote BLAKE3 actions**, without failures, retries or
excluded samples. Matching edits produced matching compiled outputs in all arms
and trials. Each startup observed 34 successful r2c2 repository lookups; all 48 warm
edit builds made zero new repository lookup requests.

| Seconds, p50 / p95 | Baseline | Candidate |
|---|---:|---:|
| Clone to first successful build (3 per arm) | 25.295 / 26.187 | 25.030 / 25.083 |
| Edit → status → build (24 per arm) | 3.535 / 4.386 | 3.433 / 3.858 |
| Edit worker + fsync | 0.050 / 0.054 | 0.052 / 0.057 |
| Git status | 0.012 / 0.067 | 0.012 / 0.068 |
| Warm build process | 3.472 / 4.321 | 3.369 / 3.726 |

The pooled cycle median is 2.9% lower, **not a demonstrated reliable RBE speedup**.
Per-trial cycle medians were 4.325/3.439/3.435 s on baseline and
3.417/3.447/3.491 s on candidate: candidate improved trial 1 but was slightly slower
in trials 2 and 3. Its 5.077 s outlier remains included and recorded a 1.659 s compile
queue. Percentiles use median p50 and nearest-rank p95; all samples are retained.
Bazel/remote work dominates this narrow build, so large local syscall gains do not
translate directly into end-to-end gains.

Startup phase medians were clone 63/64 ms, size/BLAKE3 warming 164/165 ms,
mount/ready 147/163 ms, and initial Bazel process 24.919/24.637 s (baseline/candidate).
The candidate adds bounded scheduler/store initialization; its observed mount
phase is 16 ms slower, with touching trial ranges. Independently computed phase
medians are not exactly additive. Each cell had 47 base digest hits and eight local
hashes, with no misses; candidate cache hits were zero. This RBE workload does not
establish digest-cache reuse—the local digest benchmark does.

A separate six-build functional pilot passed earlier, but is excluded from these
latency results because compilation overlap was allowed. The final primary ran
without overlapping compilation or other benchmarks. All six mounts and the proxy
stopped cleanly, with no owned processes/mounts or credential helpers left behind.

Protocol: three rotated baseline/candidate pairs, eight semantic edits per cell.
Each cell gets a fresh clone, overlay, mount, Bazel server/output base and local
repository-content cache. Both arms share a prewarmed proxy/CAS, pinned toolchain
and remote r2c2 policy, using BLAKE3 xattrs. Server preparation is excluded.

Startup measures clone to first successful build. Warm cycles measure edit-worker
launch through durable edit, native Git status and successful remote build exit.
Output/log parsing is outside that timer. Initial source materialization is a
separate phase; first-write copy-up remains inside the first edited cycle.

The workload is pinned `bazel.git`, target `//src/main/cpp/util:strings`, with one
changed source byte per cycle. Every measured build must execute three uncached
remote BLAKE3 compile/archive/link actions. Equal edits must produce equal outputs
across arms/trials, and successive edits must change outputs. Startup r2c2 RPC
reuse is checked separately; zero RPCs during warm cycles mean no new lookup,
not an invented remote hit. All samples, outliers and failures are retained.
