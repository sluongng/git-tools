# Benchmarks and tradeoffs

Single-machine results on pinned repositories, not universal guarantees. Source
startup, first build and warm iteration are separate workloads. Shared archive
caches were warm; OS page caches were not dropped. Numbers below are historical
measurements, not a benchmark of every later commit.

## Shared daemon (Oct 20)

Same source, fixed Bazel client, flags and preexisting remote cache; fresh private
Bazel roots per cell. This isolates **direct remote cache versus the daemon cache
endpoint**, with source VFS/BOS off and shared dependency caches already prepared.
Times include client startup and build-event upload finalization.

| Workload | Direct | Daemon CAS cold | Daemon CAS warm |
| --- | ---: | ---: | ---: |
| BuildBuddy CI `build //...` | 93.23 / 97.82s | 86.17s | 91.72s |
| Bazel `//src/main/cpp/util:strings` | 5.78s | 6.82s | 5.67s |

Only two direct BuildBuddy samples and one cell per other policy: **no reliable
general speedup established**. BuildBuddy matched 38,289 remote action hits and
14 executions in every arm; about 38,300 action-cache checks dominate. Their
overlapping wall-span union was 83–87s direct and 74–78s through the daemon;
these spans are not additive phase costs. AC TTL stayed zero throughout.

Bazel matched three action-cache hits and zero executions. Cold added 1.05s;
warm was essentially tied. The initial 9.13s direct cell executed two actions and
is **population, not a comparison baseline**. This is not a full Bazel self-build.
Pins: BuildBuddy `880460ccd17c`; Bazel/client `78b8a87bc6bd` from `rbe/master`.
BuildBuddy used `--check_direct_dependencies=off` in both arms for this client's
builtin module versions. The client includes the required Unix-authority fix.

Separate full BuildBuddy test validation took 1,022s: 736 passed (three flaky),
three failed on worker prerequisites, one macOS-only skip. No filters
or worker-environment fixes were applied. A full Bazel self-build readiness
attempt was stopped at a cold compiler graph; do not compare older-client full
self-build timings with this matrix. Raw failures and private reports stay
outside Git; reproduce endpoint cells with `benchmark_shared_daemon.py`.

The agent RBE benchmark JSON config accepts `rbe_endpoint`,
`rbe_instance_name`, and `rbe_extra_flags`. Defaults use the public endpoint
`grpcs://remote.buildbuddy.io` and an empty instance; supply your own toolchain
flags through `rbe_extra_flags`. Keep site settings and credentials outside Git.

### Full source/output path

Forced real remote work, identical output hashes; command plus first output read:

| Workload | Prepared native checkout | Source + proxy, BOS off | Source + proxy + lazy BOS |
| --- | ---: | ---: | ---: |
| BuildBuddy `retry_test`, one real RBE test | 24.47s | 29.74s | 55.19s |
| Bazel strings, three real RBE actions | 8.93s | 10.33s | 8.33s after resume |

These are single-cell integration measurements, not a clone comparison. Native
checkout preparation is excluded. Source creation added 6.91s BOS-off / 7.09s
BOS-on for BuildBuddy; these separately timed phases exclude mirror population,
daemon startup and orchestration pauses. The BOS-on 20.1MB binary first read alone
took 2.33s. The original Bazel BOS-on cell needed hydration/resume: `git show -s`
attempted merge rename-detection blob reads. The metadata-only timestamp fix
passed a fresh default/BOS-off session: 21.34s create + 10.33s build, matching
native output. This is a phase sum, not continuous wall time; source preparation
includes mapping metadata and is not free. The old resumed cell is not clean
startup evidence.

**BOS is opt-in by default.** Its BuildBuddy command took 52.87s versus 29.73s
without BOS. Profiles show staging/stat RPC convoys and durable generated-file
writes; the next optimization is batched metadata publication, not disabling
durability. Source + proxy alone was still 5.26s slower than native here.

## Checkout versus VFS

| Workload / date | Shallow checkout | VFS | Interpretation |
| --- | ---: | ---: | --- |
| Direct GitHub clone → query, Oct 16, 5 pairs | 8.384s | 4.964s | 40.8% lower median; principally source setup |
| Warm proxy clone → BLAKE3 query, Oct 19, 3 trials | 3.277s | 2.896s | Excludes mirror/mapping preparation |
| Warm proxy/CAS clone → narrow RBE build, Oct 19, 3 trials | 24.969s | 24.316s | 2.62% lower, overlapping ranges; no reliable broad win |

The common snapshot is `c9af12d1db12d1b2df2137feb9ea8d39e3a25c79`, using Bazel
9.2.0. Query is `//...` (4,213 labels); the narrow build is
`//src/main/cpp/util:strings`, **not a full Bazel build**. Query-only speedups do
not prove action-input digest reuse. The warm proxy query's xattrs saved only
27ms versus normal VFS hashing: not a reliable digest-specific benefit.

### Where direct-GitHub query time goes

| Median phase | Checkout | Background-prefetch VFS |
| --- | ---: | ---: |
| Clone (including checkout where applicable) | 5.949s | 0.859s |
| Exact size metadata | — | 0.321s |
| Mount | — | 0.142s |
| Query including file-read waits | 2.612s | 3.619s |
| Continuous clone → query | **8.384s** | **4.964s** |

Phase medians need not add to the continuous median. Content prefetch overlaps
query; it is not subtracted. Blocking-prefetch VFS took 5.192s versus 4.964s
background: 0.228s lower median, not all transfer latency hidden.

The query loaded 696 unique blobs / 699 paths / 3,495,118 logical bytes, including
Starlark metadata, symlink targets, 14 module patches, `requirements.txt` and
`bazel_downloader.cfg`. The [complete fetched-file list](benchmarks/github-bazel-fetched-files-20261015.json)
records reasons. Logical object sizes are **not measured network bytes**.

### Proxy preparation is not free

A historical cold proxy trial spent 97.234s creating a full mirror and 106.096s
through first query. That used an older client; it is not a current cold total.
Later warm-proxy experiments exclude mirror population.

From a warm full mirror, mapping 11,850 blobs / 138,576,774 logical bytes took
12.481s median; warm lookup took 86ms (3 cold/restart trials). This includes Git
extraction, HTTP and persistence, not standalone BLAKE3 throughput. Warm mapped
RBE startup loaded 696 blobs versus 11,850 for checkout; three uncached C++ actions
executed in every cell and matched output bytes. Dependency materialization and
Bazel startup dominated that narrow workload.

## Repeated edits

The final lifecycle comparison (`c0687ee` → `9be0196`) completed 54 builds / 162
uncached remote actions. Edit → status → build median was 3.535s → 3.433s, but
per-trial results disagreed: **no reliable RBE speedup established**.

Local component checkpoints improved reclamation (203.14 → 9.99MiB RSS after
256 create/delete operations), scoped replacement (808.59 → 17.06ms for 64
replacements among 100k entries) and repeated edited-digest probes (407.96 →
0.40ms). These were separate stage workloads, not one end-to-end speedup.
Cold immutable reads were 8% slower; 12,000 metadata changes were 9% slower.
RSS samples exclude native Git children; logical journal bounds do not imply
physical LSM compaction or a whole-mount memory bound.

## Multiple fresh agents (Oct 20)

Three trials per count, warm proxy/CAS, private source/Bazel/r2c2 client roots.
Median assignment → first narrow RBE build (not a full Bazel bootstrap):

| Concurrent agents | Shallow checkout | VFS | Change |
| --- | ---: | ---: | ---: |
| 1 | 25.46s | 25.25s | -0.8% |
| 2 | 26.36s | 25.88s | -1.8% |
| 4 | 27.90s | 26.88s | -3.7% |
| 8 | 31.57s | 27.93s | -11.5% |

All 180 builds executed **540 uncached remote BLAKE3 actions** and matched
outputs; cleanup left no owned mounts/processes. The eight-agent startup win
mostly removes checkout work: clone median 4.71s versus VFS phase medians of
0.12s clone, 0.48s metadata and 0.27s mount. These phase medians are not additive
continuous latency. Build-phase medians remained about 27s in both arms.

Tradeoffs: at eight agents, edit → build → native save was **3.94s checkout /
4.34s VFS**; reaped client CPU per cohort was **16.38s / 22.11s** (shared proxy
excluded). Retained client roots used about **546MB / 295MB per agent**, including
outputs/tools/receipts, not workspace-only or exclusive physical blocks. Shared
service allocation was 2.25GB and preparation 22.36s, both excluded from clients;
upstream mirror population was not measured.

Keep all outliers: a VFS edit hit cold remote setup and took 35.7s. Three trials
and pooled agent quantiles are not production tail estimates. The cold pilot's
90s checkout /25s VFS result is **not** a source speedup. Native save still permits
Git hydration; post-audit output reads are not first physical BOS hydration.
See [samples, scope and failures](benchmarks/2026-10-20-agent-scaling.json).

## Host-shared repository contents (Oct 20)

Same VFS/proxy, three trials, **remote r2c2 on in both policies**. Only the local
repository-contents cache changes: private versus separately primed host-shared.
This isolates cache policy, not a new shallow-checkout comparison. Median
assignment → first narrow RBE build:

| Concurrent agents | Private local contents | Shared local contents | Change |
| --- | ---: | ---: | ---: |
| 1 | 25.08s | 8.05s | -67.9% |
| 8 | 28.16s | 14.07s | -50.1% |

Initial-build RPCs fell **3,369 → 22**, with ByteStream reads **3,316 → 3**.
Every shared client reused 25 actual cached repository symlinks. All 108 measured
builds /324 uncached remote BLAKE3 actions matched outputs, plus 3 seed actions;
cleanup left no owned processes/mounts. At eight agents, edit → saved medians
were 4.33s /3.86s; reaped client CPU was about 22s per cohort in either policy.

**Preparation is separate:** a one-off r2c2-off seed took 9.00s and retained
168MB; normal CAS/RBE stayed on. Archives/OS caches were warm, not cold internet.
Remote hits do not populate this local tree cache: the empty-shared control took
27.39s and retained zero repository entries. Shared trees are mutable symlink
targets, so use one trusted agent pool and retain private source/output roots.
Bazel owns age/deduplication GC, not a hard byte quota. See [scope and samples](benchmarks/2026-10-20-repo-cache-sharing.json).

A separate shared Bazel disk cache can reuse lazy remote downloads, but that
performance is **not measured here**; Bazel 9.2 ignores it with remote output
service enabled. See [session configuration](../crates/agent-session/README.md).

## Evidence and reproduction

Use [benchmark tools](../tools/README.md) and their `--help`; they default to dry
runs. Keep raw failures/outliers and exact binary hashes. Historical summaries:

- [GitHub query/build](benchmarks/github-bazel-20261015.json) · [async prefetch](benchmarks/github-bazel-async-prefetch-20261016.json)
- [Agent RBE](benchmarks/2026-10-19-agent-rbe.json) · [iterative RBE](benchmarks/2026-10-19-vfs-iterative-rbe.json)
- Local lifecycle: [ranges](benchmarks/2026-10-19-vfs-ranges.json), [journal/GC](benchmarks/2026-10-19-vfs-online-gc-journal.json), [metadata](benchmarks/2026-10-19-vfs-metadata.json), [hydration](benchmarks/2026-10-19-vfs-hydration.json), [remount](benchmarks/2026-10-19-vfs-remount.json)
- [Developer build matrix](benchmarks/2026-10-19-developer-builds.json)

Raw campaigns and private follow-up reports stay outside tracked documentation.
The summaries retain historical validation failures; a later passing rerun does
not erase a failed attempt. Do not infer current native support from old timings.
