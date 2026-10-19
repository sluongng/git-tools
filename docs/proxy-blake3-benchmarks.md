# Proxy BLAKE3 mappings and warm-proxy Bazel query

Measured October 19, 2026, on one Linux machine: AMD Ryzen AI MAX+ 395,
16 cores / 32 threads, ext4. Requests and benchmark cells ran serially. OS page
caches were not dropped. These results cover the pinned GitHub Bazel snapshot
`c9af12d1db12d1b2df2137feb9ea8d39e3a25c79`, not all historical repository blobs.

**Result:** cached mappings work through the VFS and Bazel requests them. The
warm-proxy VFS had a lower clone-to-query median than shallow checkout. Enabling
mapped digests added only a small, noisy improvement over normal VFS hashing;
this experiment does not establish a reliable query-speed benefit from digest
reuse itself.

## Mapping endpoint

The full Git mirror already exists. Each cold trial has a fresh Fjall digest
DB, with Git objects and OS pages warm. A metadata-only request precedes mapping
creation. The measured interval is one HTTP POST through the last response byte;
client parsing/audits, fixture copies, process startup, and mirror population are
excluded. TRACE2 is enabled consistently.

The endpoint is `POST /api/v1/repos/bazel/objects`, with `oids` and
`blake3: true`. It returns sizes and BLAKE3 hashes of **raw blob content**, without
Git object headers. The full snapshot fits in one request.

| Workload | Unique blobs | Logical raw bytes | Cold generation | Warm request | First request after restart |
|---|---:|---:|---:|---:|---:|
| Query-fetched set | 696 | 3,495,118 | 799.44 ms | 8.28 ms | 11.13 ms |
| Full snapshot | 11,850 | 138,576,774 | 12,481.08 ms | 86.06 ms | 85.88 ms |

These are medians of three independent cold/restart trials and nine warm
requests per workload (three per trial). Restart reuses persisted mappings but
also warm OS pages; it is not a cold-disk test. Server startup is not timed.

| Workload / phase | Min–max |
|---|---:|
| Query set, cold | 766.17–812.99 ms |
| Query set, warm | 6.94–10.05 ms |
| Query set, restart | 10.84–11.66 ms |
| Snapshot, cold | 12,470.13–12,546.06 ms |
| Snapshot, warm | 82.47–88.01 ms |
| Snapshot, restart | 85.36–89.19 ms |

The size/type-only baseline was 10.13 ms for the query set and 80.09 ms for the
snapshot. Cold mapping requests started exactly one `git cat-file blob` process
per blob. Warm/restart requests started **zero blob-extraction processes**, but
still ran two batch metadata checks and one configuration command per request.
They also serialize and transfer the complete JSON mapping response.

For the cold full-snapshot request:

- Median CPU time was 8.54 s, including the proxy and reaped Git children.
- Incremental allocated Fjall storage was 1,269,760 bytes at the measurement
  boundary. Allocation can change with background compaction.
- The JSON response was 2,291,005 bytes.
- Maximum proxy-only high-water RSS was 20,930,560 bytes during cold requests
  and 29,786,112 bytes across warm requests. Child memory is not included.

Each response is checked for exact requested identities/order/cardinality, blob
type, sizes, and valid hash encoding. Cached/restart responses must match cold
results. At least eight deterministic samples per workload/trial are checked
independently with native Git and `b3sum`, including minimum/maximum size and
available content categories. This is not an independent full rehash.

The current cold-miss implementation starts one Git process and commits one
`SyncAll` Fjall transaction per blob. Persistent batch extraction and batched
durable writes are plausible optimizations; neither was implemented or measured
here. The measured throughput includes extraction, decompression, hashing,
persistence and HTTP. It is **not BLAKE3 algorithm throughput**. Individual
subprocess and fsync costs were not separately profiled.

## Warm proxy + fresh clients + Bazel 9.2.0

Three arms rotate order over three trials. Each cell gets a fresh source clone,
VFS cache/overlay where applicable, and Bazel output base. All use BLAKE3.

- **Checkout:** depth-one checkout through the warm proxy; digest xattr disabled.
- **VFS, normal hashing:** depth-one bare `blob:none` clone; import mappings and
  sizes; blocking Bazel prefetch; mount with BLAKE3 available, but disable Bazel's
  digest xattr setting.
- **VFS, mapped digests:** identical VFS setup; set
  `--unix_digest_hash_attribute_name=user.blake3`.

This VFS-to-VFS comparison changes only Bazel's digest-attribute setting. The
normal-hashing arm still pays mapping import; it is not a natural unconfigured
VFS baseline. Blocking prefetch is intentional for this ablation, not a claim
that it is the fastest available startup policy.

The total is continuous **clone launch to first successful `query //...` exit**,
including mapping import, prefetch and mount. Presence audits and build
diagnostics run afterward. Shared installation/archive caches are warm, but
mutable Go SDK catalogue downloads remain in module extension evaluation.

| Arm | Clone-to-query median | Min–max | Change vs checkout | Query process median | Warm query median |
|---|---:|---:|---:|---:|---:|
| Checkout | 3.277 s | 3.246–9.284 s | — | 2.530 s | 73 ms |
| VFS, normal hashing | 2.922 s | 2.851–3.378 s | −10.82% | 2.416 s | 68 ms |
| VFS, mapped digests | 2.896 s | 2.818–3.203 s | −11.63% | 2.380 s | 70 ms |

Mapped digests reduce the median by **26.67 ms (0.91%)** versus normal VFS
hashing. They win two of three paired trials; that is insufficient evidence for
a reliable additional query-speed improvement. Warm queries show no improvement.

### Phase breakdown

| Phase, median | Checkout | VFS, normal hashing | VFS, mapped digests |
|---|---:|---:|---:|
| Clone | 0.751 s | 0.062 s | 0.063 s |
| Import all mappings and sizes | — | 0.157 s | 0.158 s |
| Blocking content prefetch | — | 0.146 s | 0.146 s |
| Mount ready | — | 0.152 s | 0.147 s |
| Query process | 2.530 s | 2.416 s | 2.380 s |
| Continuous clone-to-query | **3.277 s** | **2.922 s** | **2.896 s** |

Independent phase medians need not sum to the median continuous total. Small
inter-phase setup gaps are included in the continuous total.

All trials are retained. Checkout trial 3 took 9.284 s, including an 8.533 s
query process. Its profile records 7.141 s in `fetch_sdks_by_version`, including
a 7.118 s Starlark `download` event. Other cells spent 0.911–1.340 s in that
function. This external metadata work contributes noise and prevents treating
the small digest-setting difference as causal. It was not attributed to Git
transfer, discarded, or hidden by reporting only the fastest runs.

### One-time server preparation, excluded above

For the matched run, preparation recorded:

| Phase | Time |
|---|---:|
| Copy existing full mirror with reflink support | 0.295 s |
| Start proxy | 0.016 s |
| Initial smart-HTTP refresh against actual GitHub origin | 14.478 s |
| Generate full-snapshot mappings into fresh digest DB | 12.494 s |
| Prime shallow-checkout pack | 1.281 s |
| Prime blobless clone | 0.134 s |
| Prime client mapping import | 0.163 s |
| Prime prefetch pack | 0.252 s |

**Initial full-mirror population is unmeasured**, not free. The proxy cache is a
copied full, all-refs mirror, rather than a lazy upstream object store. These are
warm local-proxy client results, not a cold-proxy or direct-GitHub comparison.
Diagnostic builds can additionally populate packs outside query timing.

## Correctness and actual digest consumption

Every cold and warm query returned exactly 4,213 sorted labels with SHA256
`56d7a2f6140674159f5252bccc16411b6ec59f5eb30bb46571381491a32883fa`.

Both VFS arms hydrated **696 unique blobs / 3,495,118 logical bytes**, versus
11,850 / 138,576,774 in checkout. Digest reuse did not reduce that prefetched
content set. Starlark parsing still needs file bytes.

The two query-only mapped-digest cells each recorded **617 cached base-digest
xattr calls, zero local hashes, zero misses**. These counts cover cold plus warm
query and count calls, not unique files. A separate post-query xattr probe was
subtracted from each cell's Bazel counters. That probe matched the proxy digest
for an absent header without hydrating it. Counters are measured after consumers
stop; arbitrary active-mount shutdown counters are best-effort snapshots.

First-trial diagnostics built `//src/main/cpp/util:strings` in all arms. Both
VFS arms matched the current checkout's pristine output paths, sizes and hashes.
A one-byte change to the parser separator in `strings.cc` preserved file size,
executed content actions, and changed output hashes identically in all arms.
The mapped-digest mount recorded 627 base hits and one local-content hash across
queries and both builds, excluding the explicit probe. This proves aggregate
base/edited digest use and same-size edit/rebuild correctness, not per-file
counter attribution or same-size-and-same-mtime invalidation.

Original source bytes were restored afterward; there was no restoration build.
Retained build outputs therefore correspond to the edited build, with both
pristine and edited manifests recorded. Build times are diagnostic only, not a
clone-to-first-build benchmark. No remote CAS or REAPI executor was tested.

### Preserved failed pilot

The first attempt stopped because the current checkout's `.so` hash differed
from the October 15 historical golden, despite matching size; `.a` was identical.
The historical mismatch's cause was not isolated. The failed artifacts remain
separate. The matched run uses a current checkout reference, then requires exact
pristine and edited output equality across arms. No VFS mismatch was waived.

## Reproduce and inspect

See [the harness guide](../tools/README.md) for runnable commands and input
requirements. The two tools refuse to overwrite prior results and operate on
owned copies, not the supplied closed mirror. Do not run benchmarks concurrently
with compilation, profiling, or another matrix.

Measured source revisions:

- Proxy: `f11b10b87cfe9ac2ed80927f0003190a479d846d`, release binary SHA256
  `e1982bbf28178f8f4472d7d190ef6ab2603ac7f0d4b7067e1c2ab5f4b58901e7`.
- VFS: `9882041f416355077ce935c809e38bdb32e796b7`, release binary SHA256
  `d222f3aed3344c6843cdaf78dc6cba351054804998e824c98c7798f3fa7679e3`.
- Executed harness: `b706d73`.

Raw artifacts remain local, outside Git:

- `artifacts/proxy-blake3-mapping-20261019/`: six mapping trials, raw responses,
  sampled independent hash checks, Fjall allocation/CPU/RSS metrics, TRACE2 logs.
- `artifacts/warm-proxy-blake3-bazel-20261019/`: rejected historical-golden pilot.
- `artifacts/warm-proxy-blake3-bazel-matched-20261019/`: complete nine-cell matrix,
  frozen tools/configs, profiles, command logs, presence manifests, and pristine
  plus edited output evidence.
