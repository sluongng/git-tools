# GitHub/Bazel VFS performance harness

`benchmark_github_bazel.py` compares fresh depth-one checkouts with fresh blobless,
writable real-FUSE mounts at the pinned Bazel snapshot. It defaults to five
rotated pairs and three warm queries per cell. Every cell uses an independent
Bazel output base and the explicitly warmed shared repository/archive cache.
System/home Bazel rc files are disabled; workspace rc remains enabled.
Prewarm the Bazel installation using the **same `output_user_root`** before the
primary matrix. `bazel --version` does not extract the installation; a pilot in
a different output-user root does not warm it. Retain/exclude any diagnostic
first pair that performs installation extraction and replace the entire pair.

Copy [`benchmark_github_bazel.example.json`](benchmark_github_bazel.example.json)
and select the actual Git, Bazel and release-binary paths. Run from the project root
when using relative paths. The pinned workload requires Bazel 9.2.0. Verify the
Bazel/build/dependency pilot before setting `pilot_dependencies_warm_verified`.
The config identifies native Git and Bazel binaries, shared cache/output roots,
query golden count/hash, VFS release binary, metadata mode and optional build.

```sh
python3 tools/benchmark_github_bazel.py --config CONFIG.json --output FRESH_OUTPUT
python3 tools/benchmark_github_bazel.py --config CONFIG.json --output FRESH_OUTPUT --native-probe --run
```

The first command parses configuration without starting subprocesses. The second
explicitly starts the matrix. Existing `results.json` is never overwritten; use
a fresh output directory for each repeat. The harness freezes the VFS binary
and records its SHA256 before any measurements. A source revision is recorded
only when explicitly supplied as independently verified provenance, not inferred
from a possibly moving worktree HEAD.

## Metadata modes

- `artifact-rest-import`: diagnostic refreshed-starting-code baseline only. Each
  VFS cell downloads its pinned recursive GitHub tree and imports it with a
  frozen, SHA256-recorded historical helper. Download and import are timed.
  This adapter is **not production VFS support**.
- `production-warm-sizes`: runs `warm_sizes_command` as a timed production
  operation. Default: `["{vfs}", "--git", "{git}", "warm-sizes", "--repo", "{repo}"]`.
  The current CLI automatically selects GitHub's tree-size provider; the array can
  explicitly add `--size-source=github` if desired.

`prefetch_command` is similarly parameterized. Placeholders are `{vfs}`, `{git}`,
and `{repo}`; commands are argv arrays, never shell strings. `--native-probe`
first preserves a separate fresh-clone native `warm-sizes` attempt (including
failure) and before/after object-presence audits, outside all matrix timing.
In production mode this probe explicitly uses `--size-source git`, so it tests
native protocol support rather than the automatic GitHub API fallback. Historical
artifact mode keeps arguments compatible with the older frozen binary. Use
`native_warm_sizes_command` to override the probe with an explicit argv array.

## Prefetch scheduling

`prefetch_mode` defaults to `"before-mount"`: the harness runs the standalone
`prefetch_command`, waits for it to finish, then mounts and starts Bazel. To
compare background prefetch with the same policy, set
`"prefetch_mode": "after-mount"`. Size warming still finishes before mount, but
the mount starts its own prefetch worker and Bazel starts as soon as FUSE is
mounted. Reads of files not yet fetched use the VFS demand-fetch path.

After-mount mode requires the canonical `prefetch_command` prefix:

```json
["{vfs}", "--git", "{git}", "prefetch-bazel", "--repo", "{repo}"]
```

It passes `--prefetch-bazel` to `mount` and translates suffix options as follows:

| Standalone prefetch option | Mount option |
|---|---|
| `--path VALUE` (repeatable) | `--prefetch-path VALUE` |
| `--include VALUE` (repeatable) | `--prefetch-include VALUE` |
| `--no-discover` | `--prefetch-no-discover` |

Both `--option VALUE` and `--option=VALUE` work for path/include values. The
harness rejects custom command prefixes and unsupported suffix options in
after-mount mode, including during dry-run, rather than silently changing the
prefetch policy. Before-mount mode still accepts custom commands.

The continuous clone-to-first-success timer is unchanged. Background prefetch
overlaps mount/Bazel and is **not** added to `phases` or summed setup time.
`background_prefetch_at_first_command_exit` records whether its completion log
was available after the first command; `background_prefetch` captures final
status, duration and report after mount shutdown. Mount stderr preserves the
raw completion record. These are diagnostic observations outside the timer,
not additional blocking phases. A cancelled/pending background worker is not
reported as a completed prefetch. Mount shutdown owns worker cleanup.

## Evidence and boundaries

Continuous clone-to-first-query wall includes size discovery, prefetch, mount,
startup and query exit. No object audit interrupts this timer. Raw commands,
exit status, stdout/stderr, native Git TRACE2 events (including mount children),
object-presence/path/size manifests and source/overlay disk footprints remain
under the output directory. `GIT_NO_LAZY_FETCH=1` prevents audit probes fetching
missing blobs. Per-path classifications are provisional; non-Starlark content
is explicitly flagged for investigation instead of presumed justified.

Mounted VFS audits inspect the configured fixed snapshot through both the base
repository and `overlay/git`. Legacy `present_*` fields and `hydrated_entries`
remain **base-only** for compatibility. Use `union_present_*` for total snapshot
availability and `private_only_*` / `private_only_entries` to detect blobs fetched
only into the writable Git admin. `private_available_*` includes base alternates;
it is not a count of physically duplicated objects. Physical private-object and
overlay allocation are reported separately in the footprint. These distinctions
matter for native Git operations such as commit, which can hydrate missing blobs
without changing the base repository's availability.

HTTP header/curl tracing is deliberately disabled. TRACE2 captures native
process/fetch activity but not exact encrypted Git transport bytes; logical blob
sizes are clearly labeled and must not be called network bytes. Artifact REST
requests record curl response status, payload size and time. Avoid adding
credentials or secret HTTP headers to command arrays or trace configuration.

An optional `build_target` (or `--build-target //src/main/cpp/util:strings`) runs
**after** cold and warm queries, with profile and BEP evidence. This is first
build from an already queried output base, **not clone-to-first-build**. The
parent pilot must freeze `build_flags` and `build_output_paths`; configured output
files get SHA256/size manifests. No build correctness or output equality should
be claimed from an empty output manifest. Failures stop the matrix and retain
evidence; the harness does not retry, filter query targets or pad timeouts.

The harness shuts down owned Bazel servers and FUSE mounts on success/failure;
a failed cleanup is recorded. It intentionally retains source repos/output
bases as raw evidence. Do not run matrices concurrently with builds/profilers.

## True clone-to-first-build workflow

The default `--workflow query` retains the existing query-first protocol. To
measure the first build directly from a fresh clone/output base instead:

```sh
python3 tools/benchmark_github_bazel.py --config CONFIG.json --output FRESH_BUILD_OUTPUT \
  --workflow build-first --warm-builds 3 --run
```

`build-first` requires a build target and nonempty `build_output_paths`. Its first
Bazel command is **build**, with no earlier query. Continuous
`real_wall_clone_to_first_build_seconds` includes clone, size warming, prefetch,
mount, startup and successful build process exit. Hashing, BEP inspection and
base/private union availability audits occur afterward, outside that timer.

Three warm builds follow by default. Each has raw command/status, profile, BEP,
output hashes and availability audit. A warm build is accepted as a content
no-op only when its exact output manifest is unchanged and BEP action metrics
show **zero executed content actions**. Unconditional
`BazelWorkspaceStatusAction` bookkeeping may still execute; total and per-mnemonic
action counts are retained. Missing/inconsistent metrics or executing content
actions fail the trial rather than being called a no-op. `verified_noop_builds`
records the accepted count. Use `--warm-builds 0` to omit these repeats.

A golden `query //...` follows all builds as a separate correctness check. It is
not part of first-build timing. Reports distinguish the build-first workflow
from a build performed after queries; do not combine their cold intervals.

Build correctness is checked across **all arms and trials**, not only within
warm repeats. Configure `golden_build_outputs` as a list of
`{"path": "bazel-bin/...", "sha256": "64 lowercase hex digits", "size_bytes": 123}`
entries to pin a previously verified reference. Otherwise the first successful
checkout build establishes the reference; a VFS build cannot establish it.
Every build must match exact paths, hashes and sizes before the matrix can be
marked complete. `build_output_reference`, `build_output_comparisons`, and
`build_output_consistency` publish the evidence. Configured builds require a
nonempty output-path list in either workflow.

No-op verification requires a genuine nonempty, recognized BEP action summary
with valid counts and a complete per-mnemonic accounting of executed actions.
Missing, empty, unknown, malformed or inconsistent summaries cannot prove a
no-op. Zero fields omitted by protobuf JSON remain valid inside an otherwise
recognized summary.

## Proxy Git SHA1 → BLAKE3 mapping benchmark

`benchmark_proxy_mapping.py` measures the existing mapping endpoint, not standalone
hash-library throughput. Supply a **closed, fully populated, self-contained bare
GitHub mirror** and an independently identified release proxy binary. The harness
copies only that mirror into its fresh artifact directory using `cp -a
--reflink=auto`; it never copies an existing proxy digest database or changes the
original mirror. Per-workload, per-trial mirror clones share the owned fixture
through Git alternates. Copying, alternates setup, readiness, and correctness
checks are outside request timings; initial mirror population is excluded.

```sh
python3 tools/benchmark_proxy_mapping.py \
  --output FRESH_OUTPUT --proxy-binary target/release/git-proxy --git /usr/bin/git \
  --mirror CLOSED_CACHE/bazel.git --source-revision VERIFIED_BINARY_SOURCE_SHA \
  --subset-json QUERY_PRESENCE.json --trials 3
```

This only prints configuration. Add `--run` to execute serially. The output
directory must not exist. The optional query presence JSON uses `present_oids`
or `hydrated_entries` from the VFS harness. Its pinned revision must match
`--revision`; the default is the same Bazel snapshot as the earlier experiments.
Without that input, only the full-snapshot workload runs. For the established
query manifest, the two workloads contain 696 and 11,850 unique blobs.

Each workload/trial starts with a warm Git mirror but a fresh mapping database:

1. One HTTP POST requests every workload mapping, including cold digest creation.
2. Three identical requests measure cached lookups.
3. The owned proxy stops and restarts; its first request checks durable reuse.

`--metadata-baseline` adds a size/type-only request before cold digest creation.
That optional baseline warms the metadata path and should be identified when
comparing runs. Readiness uses the inert cache-stats endpoint and cannot refresh
Git refs. The proxy uses a loopback ephemeral port, enables BLAKE3, and has a
3,600-second refresh TTL. No Git transport request or upstream refresh belongs
in these mapping timings.

The harness freezes and SHA256-records the supplied binary. Raw response JSON,
logs, Git TRACE2 events, manifest, invocation, and checkpointed `results.json`
remain available after success or failure. Every response must match exact OID
order/cardinality, blob types, sizes, and valid BLAKE3 hashes. Cached and restart
responses must match the cold mapping. Independent native Git plus `b3sum`
checks validate at least eight deterministic samples per workload, including
smallest/largest blobs and empty, symlink, text/binary candidates where available;
this is sampled hash correctness, not a full independent rehash of every blob.

Results report HTTP wall time, objects/s, logical uncompressed blob MiB/s,
response bytes, native Git blob-process counts, and Fjall disk-allocation growth.
Cold creation must start one native `cat-file blob` per requested blob; cached
and metadata-only requests must start none. Logical MiB/s includes subprocess,
Git decompression, HTTP, and durable cache-write costs, and must not be described
as BLAKE3 algorithm throughput or network throughput. TRACE2 is enabled
consistently. Linux `/proc` CPU deltas include the proxy and its reaped Git
children; RSS is the proxy's lifetime high-water mark, not aggregate family RSS.
OS caches are not dropped. Run separately from other benchmarks/builds.

## Warm proxy + VFS BLAKE3 Bazel benchmark

`benchmark_warm_proxy_bazel.py` compares three fresh-client workflows against an
owned warm proxy. Start with `benchmark_github_bazel.example.json`, set actual
`git`, `bazel`, `vfs_binary`, `repository_cache`, and `output_user_root` paths,
and add `"proxy_binary": "/absolute/path/to/git-proxy"`. Use the pinned Bazel
snapshot, Bazel 9.2.0, and golden 4,213-label query already in that example.
Verify dependency/archive cache warmth in a separate pilot before setting
`pilot_dependencies_warm_verified` to `true`.

```sh
python3 tools/benchmark_warm_proxy_bazel.py \
  --config CONFIG.json --output FRESH_OUTPUT \
  --mirror CLOSED_CACHE/bazel.git --trials 3 --warm-queries 1 --build-proof
```

Without `--run`, this prints the configuration and starts no processes. Add
`--run` to execute. The output directory must be new or empty. The full bare
mirror must be self-contained, closed, SHA1-formatted, and contain the pinned
snapshot. It is copied with `cp --reflink=auto -a`; only that owned copy changes.
The output must not be inside the fixture.

The harness separately records mirror-copy time, proxy startup, initial
smart-HTTP upstream refresh, creation of BLAKE3 mappings for all 11,850 snapshot
blobs, and clone/prefetch pack-cache priming. Initial full-mirror population is
**not measured**. Mapping prewarm here is one-time server preparation, not part
of the fresh-client latency; use the mapping benchmark above for repeated
mapping measurements and resource accounting.

Trials rotate these arms:

| Arm | Source view | Bazel digest function | Bazel digest attribute |
|---|---|---|---|
| `checkout` | Shallow checkout through proxy | BLAKE3 | Disabled |
| `vfs-blake3-normal` | Writable VFS through proxy | BLAKE3 | Disabled |
| `vfs-blake3-xattr` | Writable VFS through proxy | BLAKE3 | `user.blake3` |

Both VFS arms run `warm-blake3` to import the same sizes and mappings, use the
same blocking Bazel/symlink prefetch, and mount with BLAKE3 mappings enabled.
The normal arm is an **ablation of Bazel xattr use**, not the natural
unconfigured VFS path. This holds metadata work fixed while testing digest
reuse. Background prefetch is excluded to avoid a separate overlap effect.

Each cell has a fresh clone, overlay, local mapping cache, and Bazel output
base. Continuous clone-to-first-query time includes client mapping import,
prefetch, mount, and query completion. Shared dependency/archive caches are
warm; OS caches are not dropped. Object-presence audits, digest probes, and
optional builds run after that timer. Raw results include frozen binary hashes,
Git/Bazel executable provenance, golden label checks, and proxy cache counters.

`query` may not request any file digest: selecting BLAKE3 alone does not prove
that Bazel consumed a mapping. The harness retains VFS xattr counters, allowing
zero query hits. An explicit post-query xattr probe checks a still-unfetched
source header against the proxy mapping and requires unchanged base/private
blob availability. Its known hit is subtracted from counters attributed to
Bazel.

`--build-proof` additionally builds `//src/main/cpp/util:strings` once per arm in
trial 1 and compares exact output paths, SHA256 hashes, and sizes. It then
changes one byte in the compiled `strings.cc` parser separator constant without
changing file length, rebuilds, and requires executed content actions and
changed outputs matching the edited checkout reference. Original source bytes
are restored in `finally`. The candidate must report both base-mapping hits
and local-content hashing for the edited overlay. Those counters cover queries
plus both builds, not the query alone. These diagnostics are outside query
timings; their raw commands, profiles, events, and output evidence are retained.
Restoring the source does not rebuild its outputs; artifact directories retain
the edited build outputs alongside both recorded manifests.

The October 19 run still performed mutable Go SDK catalogue downloads during
module extension evaluation despite warm archive/dependency caches. Record such
profile events as query costs, retain outliers, and do not call this an offline
Bazel benchmark. Earlier GITSHA1 build manifests are not automatically valid
BLAKE3 goldens: use a current matched checkout reference when configuring a new
comparison and retain any rejected historical-reference pilot separately.
