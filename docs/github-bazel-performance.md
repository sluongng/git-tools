# GitHub / Bazel performance

Measured on 2026-10-15 with a real Linux FUSE mount, native Git
`2.55.0.1095.g6de20f6092`, and Bazel 9.2.0. These are single-machine
measurements, not a claim of a universal optimum.

See the [Oct 16 background-prefetch follow-up](github-bazel-async-prefetch.md)
for the newer nonblocking mount option and its measured setup/query tradeoff.

## Workload and timing boundaries

- Source: `https://github.com/bazelbuild/bazel.git`, commit
  `c9af12d1db12d1b2df2137feb9ea8d39e3a25c79`, tree
  `27ef12c1012f715ecd20b35f7492198ac0433093`.
- Both arms use depth-one clones. Checkout downloads the snapshot normally;
  VFS uses a bare `--filter=blob:none` clone and a fresh writable overlay.
- Query: `//...`; exactly 4,213 labels with sorted-label SHA256
  `56d7a2f6140674159f5252bccc16411b6ec59f5eb30bb46571381491a32883fa`.
- Build: `//src/main/cpp/util:strings`, jobs=8, local sandbox, no remote
  executor/cache or disk cache. Compare exact `.a` and `.so` output hashes.
- GITSHA1, `user.gitsha1`, and `--watchfs` are enabled in both arms.
- Fresh source clone and Bazel output base per cell. Shared repository/archive
  and Bazel installation caches are warm; OS page cache is not dropped.
- Cold wall starts at clone launch and stops at successful first command exit.
  It includes GitHub metadata, all prefetch, mount startup and handovers.
  Hashing, object audits and disk accounting occur afterward.
- Query-first builds are explicitly **not** clone-to-first-build. The separate
  build-first workflow invokes no query before its first build.

Use the [benchmark harness](../tools/README.md) and its explicit JSON config to
repeat the experiment. Every run freezes the binary and command/config evidence.
Do not run the matrix concurrently with compilation, another matrix or profiling.

## Replicated final comparison

Final VFS source: `75790ef90f69d9c6a8f5bd3beed1decd51e2fb94`; release SHA256:
`7156c37bae6cb86dbe5b3df159efceeaaf31532fd01b02c026cbd988b125ebb4`.
Host: Linux `unspecified`, 32 logical CPUs. Each row has five rotated pairs;
all fifteen pairs favored VFS, with no failed or excluded trials.

| Continuous wall, seconds | Checkout median (range) | VFS median (range) | Reduction of medians |
|---|---:|---:|---:|
| Default query A | 8.92909 (8.54482–9.96817) | **5.91071 (5.68517–6.43923)** | 33.80% |
| Default query B | 9.12764 (8.68771–9.43077) | **5.38373 (5.29211–5.97885)** | 41.02% |
| Target-tuned first build | 9.65353 (9.27186–9.87251) | **6.03423 (5.65510–6.22152)** | 37.49% |

The query rows use the default prefetch policy. The build row uses **21 explicit
project/target includes**, frozen before timing; it is not a universal automatic
prefetch result. Its first Bazel invocation is build, with no earlier query.
Median paired savings are 2.96160 s, 3.62004 s and 3.43200 s, respectively.
These are descriptive single-machine measurements, not latency guarantees.

Warm query medians are 51.75–51.76 ms for VFS versus 53.51–53.80 ms for checkout.
Verified content-noop build medians are 70.65 ms versus 69.76 ms. Across the primary
matrices, **90 queries**, **60 golden-output build commands**, and **30 content-noop
builds** passed. A no-op may execute workspace-status bookkeeping; it executes no
content actions and preserves exact output hashes.

The [machine-readable results](benchmarks/github-bazel-20261015.json) retain all
paired timings, ranges, tool/binary provenance, output references, request counts,
cache conditions and cleanup evidence. The
[complete query file list](benchmarks/github-bazel-fetched-files-20261015.json)
contains every selected source-relative path, OID, mode, size and reason.

## Why it became faster

1. **Snapshot metadata, not content:** one pinned GitHub recursive-tree request
   obtains blob sizes. The result is checked against the local tree manifest,
   then committed atomically to Fjall. Truncated responses use bounded subtree
   requests. Metadata warming does not hydrate file contents.
2. **Indexed namespace:** parent/name lookup, directory enumeration and link
   counts use derived indexes instead of scanning the entire overlay namespace
   on each FUSE operation. The index-only ablation kept durability and zero cache
   TTL unchanged, isolating the index's effect.
3. **Batched Bazel inputs:** metadata and symlink blobs are fetched together.
   Bounded literal MODULE / rc discovery fetches repository-loading inputs in
   another batch when necessary. Explicit `--include` paths join the first batch.
4. **Persistent native reader:** each repository owns one serialized
   `git cat-file --batch` process rather than launching one per blob read.
   It validates response framing and discards a failed process without silently
   retrying. It does not add a content cache.
5. **Kernel-coherent positive metadata caching:** one-second positive entry and
   attribute validity avoids repeated FUSE lookups. Linux invalidates these for
   mutations through the mount; DIRECT_IO and uncached missing-name replies stay
   enabled. This relies on one overlay owner and no out-of-band content changes.
6. **Larger bounded native size batches:** native object-info requests use 4,096
   OIDs, within command/proxy limits, rather than 128. GitHub REST stays one request.
   Failed batches never publish partial sizes or silently fetch file content.

### Causal evidence and limits

Five rotated populated-source pairs isolated the namespace index: query median
fell from 23.08019 s to 3.67463 s; daemon CPU median from 20.01 s to 0.97 s.
Both arms still launched 1,250 cat-file processes and zero fetches. All labels
matched. These are reused-content queries, **not** fresh GitHub clone timings.

A separate five-pair persistent-reader ablation reduced query cat-file starts
from 1,250 to one. Query medians were 3.54756 s and 3.32001 s, but the persistent
reader won only three pairs, with median paired saving 0.09010 s. Its standalone
wall-time gain is **not established beyond noise**. Daemon CPU median fell from
0.86 s to 0.69 s; that excludes the still-live native child's CPU.

Five further populated-source pairs isolated positive metadata validity, holding
4,096-object batching and the reader constant. Query median fell from 3.41154 s
to 2.63526 s; median paired improvement was 22.75%, with all pairs favorable.
Daemon CPU median fell from 0.66 s to 0.15 s. These runs fetched no additional
blobs and preserved the 699-blob source set.

Device-header-only FUSE tracing explains the request reduction: cold query
requests fell from **412,954 to 37,458** (90.93% fewer), and warm requests from
59 to 20. GETATTR fell from 212,513 to 2,026; LOOKUP from 184,280 to 19,271.
All successful headers were parsed; the single shutdown read without a returned
header is explicitly unparsed. Traced wall time is not used as performance data.
Normal kernel mutation invalidation is supported by the primary Linux
[directory](https://github.com/torvalds/linux/blob/master/fs/fuse/dir.c) and
[DIRECT_IO write](https://github.com/torvalds/linux/blob/master/fs/fuse/file.c)
implementations and by immediate real-kernel regression/build tests.

The production provider/index/discovery bundle, before the persistent reader,
completed five fresh paired runs in 6.36475 s median versus 8.36157 s for checkout.
All five VFS cells won. The refreshed starting code took 32.43778 s median using
a diagnostic REST import adapter; its native GitHub size warming failed without
hydrating blobs. The starting-to-production comparison is time-separated and
bundled; it is not a single-feature causal estimate.

## Fetched files and accounting

Default prefetch selects 699 paths / 696 unique blobs at this snapshot, in two
native fetch batches. Cold query needs no further fetch. Besides Bazel metadata
filenames and symlink targets, literal discovery selects fourteen `third_party`
patches from MODULE `patches`, `requirements.txt` from `requirements_lock`, and
`bazel_downloader.cfg` from the rc downloader setting. Each path's reason is
included in the prefetch report.

Query availability is 696 unique blobs / 3,495,118 logical bytes. The build adds
only `src/main/cpp/util/{exit_code.h,strings.cc,strings.h}`, bringing availability
to 699 / 3,513,935 bytes. Audits cover the **union** of the base clone and private
Git admin, including alternates; every measured query/build has zero private-only
snapshot objects. Logical blob bytes are not encrypted transport bytes.

The metadata response is 4,217,464 HTTP body bytes per fresh VFS client, one
request. Native Git's exact encrypted transfer bytes are not measured. In the
phase-2 matrix, closed source + overlay files occupied 7,028,736 allocated bytes
versus 283,459,584 for checkout, excluding Bazel output/cache trees and directory
overhead. Private availability includes alternates and is not duplicated storage.

### Project-configured single-batch inputs

A separate five-pair fresh-GitHub ablation, before positive TTL, compared default
discovery with 18 configured patch/requirements/downloader includes.
Clone-to-query medians were **6.40867 s versus 5.41920 s**; all five pairs improved,
with median paired improvement 12.97%. Prefetch used one batch instead of two.
Only two speculative patches were extra: `third_party/apple_support.patch` and
`third_party/def_parser/def_parser.patch`, totaling **9,260 logical bytes**.
This does not measure the combined positive-TTL/query-tuning effect.

The final build configuration adds the three observed C++ inputs. It fetches
**701 unique blobs / 704 paths / 3,523,195 logical bytes in one batch**, then
requires **zero first-build demand fetches**. Its speculative cost remains those
two patches. Exact lists are in the machine-readable result; other projects
should choose their own scope rather than copy this pinned-workload selection.

After size warming, the pinned Bazel snapshot can use the checked-in list through
the supported repeatable `--include` interface:

```sh
python3 - /tmp/bazel-vfs.git <<'PY'
import json, subprocess, sys
with open("docs/benchmarks/github-bazel-20261015.json") as f:
    paths = json.load(f)["project_prefetch"]["strings_build"]["paths"]
argv = ["target/release/git-vfs", "prefetch-bazel", "--repo", sys.argv[1]]
for entry in paths:
    argv += ["--include", entry["path"]]
subprocess.run(argv, check=True)
PY
```

## Optional proxy: cold cost and warm benefit

The proxy populates a **full all-refs mirror**. An actual GitHub-origin cold trial
spent **97.234 s cloning the mirror** and **106.096 s through first query**, with
1,329,303 packed objects / 1,793,964,858 pack bytes. That total used the earlier
128-OID, zero-TTL client; it is **not** the final client's remeasured cold total.
No partial cache was relabeled cold or warmed off the clock.

That trial revealed native metadata warming as the warm-client bottleneck. Five
rotated native size-only pairs on the same warm mirror measured **4.91996 s to
0.33157 s** after batching increased from 128 to 4,096 OIDs. Remote requests fell
from 93 to three. All 11,850 sizes were exact; warming hydrated zero blobs and
preserved refs, HEAD and FETCH_HEAD. Startup refresh was accounted separately.

With the final client, three fresh-client runs against one warm-mirror proxy took
**3.41270, 3.26249 and 3.05941 s** clone-to-query (median **3.26249 s**). Native size
warming was 0.329–0.333 s, warm queries 54.48 ms median, and first build after
queries 1.38623 s median. All 12 queries and three builds matched their goldens.
Server refresh/fixture setup took 1.74190 s separately: these are **warm proxy /
cold client**, not cold-GitHub results or a paired direct-path comparison.

Server cache allocation was 1,844,244,480 bytes, versus 7,053,312 per client plus
overlay. The fixture primed only the clone response; other entries populated
during the first measured client. Later clients reused them. Mirror population
and storage must be considered alongside warm savings: direct GitHub is the
substantially cheaper first-touch path.

## Correctness and remaining tradeoffs

- Real builds and two same-file source edits execute compilation/archive/link
  actions, change output hashes, and preserve matching Git SHA1 xattrs. A no-op
  rebuild executes no content actions. This exercises `--watchfs` behavior; it
  does not isolate notification use from Bazel's fallback invalidation.
- Native status/add/commit/log and remount work with isolated refs/index/objects.
  **Native commit is not content-lazy:** stock Git prefetches missing full-index
  objects during cache-tree integrity checks, including unchanged blobs. A first
  commit can hydrate the whole snapshot into the private admin. This predates
  these changes; a base-only audit would conceal it. No skip-worktree or weakened
  integrity-check workaround is enabled.
- DIRECT_IO, serialized backend operations and whole-blob reads remain. Positive
  metadata caching is verified with immediately warmed-cache mutations and actual
  same-size Bazel input edits, not a wait for timeout expiry. Large files, high
  write rates and other repositories need their own measurements. Local digest
  checks do not establish remote CAS/RBE behavior.
- Exact includes can remove discovery round trips when a project knows its
  loading inputs. A blanket extension heuristic is not enabled by default:
  unrelated patch/config files in another monorepo are not automatically justified.

Workspace tests, all-target Clippy, formatting, and all four explicit kernel-FUSE
tests passed. One initial workspace run reported a namespace-reopen test failure
with its primary error chain truncated in tool output. A targeted rerun, ten
normal integration reproduction suites, and the complete workspace rerun passed.
Its cause remains unknown; no speculative storage fix, retry loop, or timeout
budget change was applied.

A final live `/proc`/mountinfo audit found one leftover, disconnected tracing
mount after its daemon exited. An ordinary untraced `fusermount3 -u` removed it;
the repeated audit found no owned artifact mounts or processes. The retained
diagnostic cleanup erratum supersedes its earlier teardown flag. This did not
change request counts, query outputs, or any timed matrix.

## Remaining cost and stopping point

Namespace scans, process churn, demand-fetch round trips, positive metadata
caching and native size batches were evaluated with causal experiments. Remaining
cold time is largely actual Bazel work plus clone/metadata/prefetch setup; warm
commands are near checkout. Whole-blob materialization and serialized I/O remain
important for other workloads.

A three-pair HTTP-only compression probe reduced encoded metadata bodies from
4,217,464 to 738,838 bytes, but median request time only from 0.18033 s to 0.11917 s.
That roughly 61 ms is about 1% of the primary clone-to-query wall, not an established
5% end-to-end opportunity. No production compression change was made. Similarly,
overlapping this measured HTTP request with cloning has less than 0.2 s median
network work available to hide; validation/persistence still require the local
tree. A new clone orchestrator was not justified by these measurements.

Larger speculative source prefetch changes transfer policy without further target
evidence. Longer/negative cache validity cannot remove actual Bazel work and needs
its own invalidation proof. No content-cache/writeback change or out-of-band
writer support was introduced. Future snapshot replacement would need explicit
invalidation. These remain separate designs, not a claim of global optimality.

## Evidence index

Raw local artifacts are retained under `artifacts/` (not committed source):

- `github-bazel-performance-baseline-20261015/`: refreshed starting code, failed
  native size probe, paired raw records and explicit replacement-pair rationale.
- `namespace-index-ablation-20261015/`: five-pair single-factor index experiment.
- `github-provider-live-20261015/`: metadata-only warming, cached/offline reuse,
  automatic mount, Git operations and corrected private-admin hydration audit.
- `github-bazel-production-matrix-1-20261015/`: five-pair provider/index/prefetch
  production comparison, complete fetched-path manifests and TRACE2.
- `persistent-reader-ablation-20261015/`: process reuse / resource experiment.
- `bazel-watchfs-live-20261015/`: actual source edits, builds, xattrs and remount.
- `github-bazel-ttl-final-verification-20261015/`: final replicated default queries,
  target-tuned first build, paired TTL experiment and publication reproducer.
- `prefetch-includes-ablation-20261015/`: configured single-batch experiment.
- `github-bazel-proxy-pilot-20261015/`: truly cold mirror and earlier client.
- `native-size-batch-ablation-20261015/`: five-pair native metadata comparison.
- `github-bazel-warm-proxy-final-20261015/`: final warm-proxy end-to-end workflows.
- `bazel-watchfs-ttl-20261015/`: final same-size edits, warmed attrs and build inputs.
- `fuse-request-profile-parent-attach-20261015/` and
  `fuse-request-profile-ttl-20261015/`: device-header counts and parse coverage;
  retained tracing failures and explicit untraced unmount cleanup.
- `github-tree-compression-20261015/`: transfer-only probe, not an end-to-end win.

Each report distinguishes valid primary trials, diagnostics, raw failures and
cache conditions. Preserve those distinctions when comparing or quoting results.
