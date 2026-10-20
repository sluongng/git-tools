# Benchmarks and tradeoffs

Single-machine results on pinned `bazel.git`, not universal guarantees. Source
startup, first build and warm iteration are separate workloads. Shared archive
caches were warm; OS page caches were not dropped. Numbers below are historical
measurements, not a benchmark of every later commit.

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
