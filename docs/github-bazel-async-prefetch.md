# Background prefetch: GitHub and Bazel

Measured on 2026-10-16 with direct GitHub HTTPS, real writable Linux FUSE,
native Git, and Bazel 9.2.0. **Starting without a clone, background-prefetch VFS
completed the query in 4.964 s median versus 8.384 s for its paired shallow
checkouts: 3.421 s saved, or 40.8% less elapsed time.**

This is principally a source-setup saving, not faster Bazel evaluation.
Moving prefetch after mount made setup noticeably shorter, but most transfer
time reappeared as file-read waits inside the query.

## Workload and method

- Source: `https://github.com/bazelbuild/bazel.git`, commit
  `c9af12d1db12d1b2df2137feb9ea8d39e3a25c79`.
- Query: `//...`, exactly 4,213 labels, with the same golden sorted-label hash
  as the [Oct 15 comparison](github-bazel-performance.md).
- Five trials per prefetch mode. Each trial includes a fresh shallow checkout
  and fresh bare `blob:none` clone, overlay, and Bazel output base. Modes and
  checkout/VFS order alternate across trials; all commands run serially.
- Shared Bazel installation and dependency/archive caches are warm. OS caches
  are not dropped. There is no proxy or benchmark-only metadata adapter.
- The same release binary is used in both modes, built from
  `fdbbd2845d79e84b2ff6027673f0bfe5c6b189e0`; SHA256
  `fcb393f331e248dfc9b2ee9286a9b9b60ce84f3c03bb1de7265bea688b8f81bd`.
- Continuous elapsed time starts at clone launch and stops at successful first
  query exit. Background prefetch is not subtracted from this interval. Object
  audits, correctness checks, warm queries, and shutdown follow the timer.

All 20 cells completed without a failed or excluded benchmark trial. All 40
cold/warm queries matched the golden labels. No build-speed claim is made by
this query-only experiment.

## Total elapsed time

| Workflow | Shallow checkout median (range), s | VFS median (range), s |
|---|---:|---:|
| Blocking prefetch before mount | 8.642 (8.102–9.293) | 5.192 (5.077–6.582) |
| Background prefetch after mount | 8.384 (8.290–8.792) | 4.964 (4.506–5.267) |

Comparing VFS modes gives a **0.228 s / 4.4% reduction of medians**. Background
mode wins four of five same-trial comparisons; median paired saving is 0.201 s.
The third trial is 0.076 s slower. These five-trial, single-machine results
support a modest observed saving, not a guaranteed speedup or a claim that all
prefetch latency is hidden.

## Phase breakdown

| Phase median, seconds | Shallow checkout | Blocking VFS | Background VFS |
|---|---:|---:|---:|
| Clone, including checkout when applicable | 5.949 | 0.757 | 0.859 |
| Warm exact file-size metadata | — | 0.323 | 0.321 |
| Blocking content prefetch | — | 1.340 | — |
| Mount startup | — | 0.137 | 0.142 |
| Bazel query process, including any file-read waits | 2.612 | 2.689 | 3.619 |
| **Measured continuous clone-to-query** | **8.384** | **5.192** | **4.964** |

The checkout column uses background mode's paired checkouts. Each value is an
independent median; phase medians need not sum to the median continuous total.
Continuous timing also includes phase handovers. Raw per-trial timestamps are
retained in the [machine-readable summary](benchmarks/github-bazel-async-prefetch-20261016.json).

Setup before Bazel launch falls from 2.562 s to 1.331 s median. Background
prefetch takes 1.270 s median **overlapping** the query; do not add it to the
background column. Bazel's first reads can still wait for those blobs, which
explains why query-process time rises while total time falls only modestly.
Warm query medians are 64.8 ms for background VFS versus 66.5 ms for its paired
checkout; this is effectively parity, not evidence of faster query evaluation.

## Behavior and limits

Enable mount-owned background prefetch with:

```sh
target/release/git-vfs mount --repo /tmp/project.git \
  --prefetch-bazel --overlay /tmp/project-overlay /tmp/project-mount
```

GitHub size metadata remains synchronous. Background content selection uses
the existing policy, including symlink blobs and bounded literal MODULE/rc input
discovery. `--prefetch-path`, `--prefetch-include`, and `--prefetch-no-discover`
correspond to the standalone policy options. Ordinary mounts remain fully lazy.

The worker runs inside the mount process so it can share Fjall ownership.
Demand reads and each prefetch batch share a content-transfer lock, not a
filesystem-wide or size-cache lock. A read arriving during an active batch waits
and reuses the objects. Because FUSE dispatch is serial, that waiting read also
queues later filesystem requests. Already-local reads currently wait as well.

An early demand read can beat the first background batch. This happened in two
of five trials: one blob was already present when prefetch acquired the lock,
with three native fetches total rather than two. There was no extra unique
content: every VFS trial ended with the same 696 unique blobs / 699 paths /
3,495,118 logical bytes, zero private-only snapshot blobs, and two background
prefetch batches. Logical bytes are not measured encrypted network transfer.

Invalid includes fail before mounting. Speculative fetch failure is nonfatal;
ordinary demand reads remain available. Shutdown cancels both worker and mounted
native Git process groups before joining the worker, preventing a read released
from a cancelled batch from starting another fetch.

## Validation and artifacts

- 19 unit tests, 13 ordinary integration tests, and all seven real-FUSE tests
  passed. The new real-FUSE tests cover live metadata during a paused batch,
  scoped symlink/metadata/includes, exact concurrent reads without duplicate
  in-flight fetching, nonfatal failure, and unmount with a blocked demand read.
- Independent source review found no remaining actionable defects.
- One parallel ordinary-suite invocation failed launching the existing
  size-warming test wrapper with `ETXTBSY` (`Text file busy`) in `GitRepo::open`.
  The targeted test and full serial suite passed afterward. No timeout/retry
  workaround or unrelated repair was added; the failed invocation is retained
  here rather than represented as a clean first pass.
- Full raw artifacts: `artifacts/github-bazel-async-prefetch-20261016/`, with
  separate `before-mount/` and `after-mount/` results, frozen binaries, native Git
  TRACE2, Bazel profiles, fetched-object manifests, and command logs. The saved
  `run_comparison.py` records the interleaving driver.

The [benchmark harness](../tools/README.md) accepts `prefetch_mode` values
`before-mount` and `after-mount`. The checked-in JSON summary retains per-trial
phase timestamps, object counts, background status, native fetch counts, source
revision, binary hash, raw-results hashes, and validation caveats.
