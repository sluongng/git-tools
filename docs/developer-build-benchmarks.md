# Developer build benchmarks

October 19, 2026. This measures **building git-tools itself**, not running a
Bazel query/build through the VFS.

**Use warm remote caching for clean workspaces; use local Bazel for small warm
edits on this host.** The split gives useful rebuild boundaries, but remote
execution is not a small-edit speedup in this experiment.

## Matched concurrency: 16 jobs

Client command wall time, three samples per cell; medians with ranges:

| Phase | Local Bazel | BuildBuddy RBE | Latency change |
|---|---:|---:|---:|
| Build after `bazel clean` | 38.275 s (36.143–39.065) | 25.406 s (23.583–26.797) | −33.62% |
| Local-output no-op | 0.200 s (0.193–0.203) | 1.574 s (1.539–1.778) | +686.89% |
| VFS CLI metadata edit | 1.239 s (1.207–1.258) | 7.030 s (5.219–8.926) | +467.33% |
| Git-layer metadata edit | 4.127 s (4.028–4.176) | 19.505 s (17.512–22.878) | +372.65% |

Change is `(RBE / local − 1) × 100`, calculated before rounding.
The clean-output case is **1.51× faster**, not a cold remote-compilation result.
Every corrected RBE replay had **1,307 remote-cache hits, zero disk-cache hits
and zero remote executions**. The local arm executed 1,307 sandboxed processes.
BEP's `actionsExecuted` also counts cached actions; runner counters establish
which actually executed.

One fresh-output-base observation was 39.080 s local versus 28.532 s warm remote
cache at 16 jobs. These are single observations, not three-sample medians.
Toolchain archives and the remote cache were already warm.

## Configured RBE default: 64 jobs

The same clean-output replay at 64 jobs took **16.071 s** median
(16.029–16.153, three samples), again with 1,307 remote-cache hits and no execution.
That is 36.74% below the 16-job RBE median. It is **2.38× faster than the 16-job
local rebuild**, but this comparison changes concurrency. The checked-in RBE
configuration uses 64 jobs; the matched table above deliberately uses 16 for both.

## What the crate split buys

The CLI edit executed exactly one Rust action: `git-vfs-bin`.
The lower-layer edit executed exactly four, in dependency order:

1. `git-workspace`
2. `git-overlay`
3. `git-vfs`
4. `git-vfs-bin`

Neither edit rebuilt the proxy, pack cache, generated REAPI bindings or third-party
dependencies. All three samples had the same action set. The edits add a public
constant, changing exported Rust metadata without changing CLI behavior. They
test rebuild boundaries, not application throughput. There is no equivalent
monolithic Bazel baseline here, so this does **not** quantify the split's speedup.

The slower remote lower-layer edit is a serial chain. Profiles report 9.333–13.053 s
of worker process wall time across its four actions and only 0.250–0.258 s of
queueing. Client/worker transfers and coordination add more time; this is not
evidence of a queue-capacity bottleneck. Profile categories can overlap and must
not be summed as independent client phases.

## Protocol and provenance

- Bazel 9.2.0, Rust 1.98.1, rules_rs 0.1.0, LLVM 0.8.25; optimized builds of
  `//:git-vfs //:git-proxy`. The remote Ubuntu image is digest-pinned in
  [BUILD.bazel](../BUILD.bazel).
- Local host: AMD Ryzen AI Max+ 395, 32 logical CPUs. Remote worker hardware
  was not controlled; do not generalize these small-edit ratios to other hosts.
- Initial matrix revision `07f144d`; corrected cache harness revision `d6dda20`.
  Compiled crate, FUSE, generator, module and Cargo inputs are identical across
  the runs, verified by Git object IDs in the [public results](benchmarks/2026-10-19-developer-builds.json).
- Private output bases; warm archive/remote caches; no global cache flush.
  Prerequisite base builds and `clean` are outside measured intervals. Client
  launch, analysis, action/cache work, downloads and command completion are inside.
  No other task compilation or benchmark ran concurrently. Normal host background
  applications were not isolated; remote worker hardware was not controlled.
- Local and RBE order rotated across the original three trials. Initial RBE
  bootstrap cells inherited a host disk cache and are retained but **excluded**
  from pure remote-cache claims. Only those affected bootstrap cells were rerun
  with disk caching explicitly disabled. Existing local and edit/no-op cells
  were retained; measured RBE edits had zero cache hits and exactly one/four
  remote executions. Their prepared dependency outputs were already local.
- A heavyweight JSON execution-log diagnostic produced a 686 MiB log and was
  interrupted during postprocessing. It is excluded from latency results.
  Headline runs use BEP counters and lightweight profiles, not execution logs.
  All completed latency commands succeeded; source restoration was verified.
- Raw commands, logs, BEPs, profiles and preparation timings are retained in the
  ignored `artifacts/developer-build-*20261019` directories. Credentials are
  private during capture and scrubbed afterward. The public JSON contains only
  selected counters, timings and source identifiers.

Reproduce from a committed, clean source tree using the ignored credential rc:

```sh
python3 tools/benchmark_developer_build.py --config rbe \
  --output artifacts/my-build-benchmark --trials 3 --jobs 16 \
  --source-edit crates/git-vfs/src/main.rs crates/git-workspace/src/backend.rs
# Review the plan, then repeat with --run. Omit --execution-log for timings.
```

The harness disables inherited disk caches in both arms and uses a fresh edit
namespace per invocation. Reusing `--edit-seed` intentionally can replay old
cached edits; inspect runner counts before calling them uncached executions.

## Validation and known failure

Cargo: 85 unit, 23 ordinary integration and 9 real-FUSE cases passed; scoped
clippy and formatting checks passed. Bazel: 23 ordinary integration and 9 real-FUSE
cases passed against Bazel-built binaries. Native Git/FUSE execution stays local;
test compilation remains eligible for RBE. A full RBE validation also observed
1,310 remote processes and an actual remotely executed pack-cache unit suite.

The Bazel unit run passed **84/85**, not all green. The existing
`large_local_stream_does_not_occupy_small_or_cached_read_lane` fixture flushes a
header before recording its marker; the scheduler can kill the deferred child
before that marker is written. Btrfs scratch reproduced the race frequently;
tmpfs did not reliably eliminate it. One initial `ETXTBSY` was not reproduced in
the focused diagnostics. No Rust fixture repair, test filtering, retry-until-green
or timeout increase was included in this scoped build migration.
