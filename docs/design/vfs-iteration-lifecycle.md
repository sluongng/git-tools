# Bounded VFS state for iterative coding agents

Status: implementation plan. Baseline: `c0687ee` (2026-10-19).
Audience: git-tools implementers and operators running multiple Bazel/RBE agents.

## Outcome

An agent must be able to repeat edit → status → remote build → replace/delete
without retaining every old file version or making a small operation proportional
to the entire file/repository. Keep first-build startup competitive, preserve
acknowledged writes, and keep each agent's writable state isolated.

This is application-level filesystem reclamation, not a Rust garbage collector.
Fjall remains the metadata engine. Replacing the database is not this proposal.

## Evidence and motivation

At the baseline, the overlay stores full mutable contents inside `Entry.data`, persists
unlinked entries indefinitely, and reloads all their payloads on remount. Every
write copies/serializes the full value; ranged reads load the full contents.
Rename scans the full entry map. Mutation history is append-only. Edited-file
SHA1/BLAKE3 xattrs rehash contents on every request.

A baseline real-FUSE probe created/closed/deleted 64 × 256 KiB temporary files,
leaving a clean Git status. Source inspection establishes 16 MiB of retained dead
payload. RSS was 7.02 → 58.92 MiB and 54.79 MiB after remount; allocated overlay
storage was 0.11 → 32.29 MiB. Fjall WAL/cache/allocation also contribute, so the
entire increase is not attributed to orphan contents. Evidence remains at
`artifacts/edenfs-study-20261019/`; this is not an EdenFS runtime comparison.

Earlier first-build measurements were nearly at parity with shallow checkout:
24.316 vs 24.969 s medians, N=3 with overlapping ranges. Those measurements do not
cover repeated edit/build growth. See [startup results](../proxy-agent-startup.md).

EdenFS supplies useful invariants, not settings to copy blindly:

- [Reference-safe overlay reclamation](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/inodes/InodeMap.cpp#L1253-L1288).
- [Bounded background deletion and drain](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/inodes/Overlay.cpp#L1304-L1338).
- [Journal history-loss contract](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/service/EdenServiceHandler.cpp#L2547-L2551).
- [Foreground versus prefetch priorities](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/store/ImportPriority.h#L160-L169).

Its optional Linux pressure-GC path is not default-on at this pin. Windows
ProjectedFS dehydration is not the same operation as Linux FUSE inode eviction.

## Resource model and invariants

| Resource | Authority | Reclamation rule |
|---|---|---|
| Base Git blob/tree | Native Git + configured origin | Refetchable cache policy only; never delete objects behind Git's back |
| Linked modified payload | Private overlay | Never evict as a cache |
| Unlinked payload | Open/lookup/in-flight users, then overlay collector | Reclaim only after references cease, or exclusive fresh mount |
| Inode/tombstone metadata | Persistent namespace | Preserve masking, identity and monotonic allocation |
| Change journal | Incremental consumer history | Bound history; expired token means full rescan |
| Digest/cache entries | Content identity or generation | Byte bounds and explicit invalidation/pins |

Always preserve:

1. Open-unlinked and overwritten-open descriptors remain readable/writable until
   their last required reference is released.
2. Rename does not change the surviving source inode identity. Allocated inode IDs
   are not reused within a live mount; persistent high-water prevents reuse after GC.
3. Same-size edits invalidate digest caches. Base mappings never override edited bytes.
4. Linked edits and deletion masking survive remount/recovery. Unknown/corrupt state
   fails closed; it is not interpreted as an empty file or silently discarded.
5. Namespace, journal and payload recovery have one coherent ordering. Never silently
   weaken the existing SyncAll acknowledged-mutation contract.
6. No network access under metadata/cache locks; bounded in-flight work and shutdown
   cancellation cover the full native-process lifetime.
7. Agent clones/overlays/output bases remain private; only immutable content and the
   existing proxy may be shared within the authorized cache namespace.

## Required dispatch-lifetime boundary

Early benchmark instrumentation exposed an existing fuser0.16 lifecycle issue:
`BackgroundSession` drop detaches dispatch; simply calling its `join` can hang
while a client retains an idle file/directory descriptor after lazy unmount.
A real idle-FD probe reproduced that behavior. Bring this shutdown prerequisite
forward before trusting per-session reclamation counters.

Retain the upstream MIT fuser0.16 source with a narrow Linux `Session::run_until`
patch: nonblocking receive and finite poll with stop checks, original descriptor
flags restored by an owned guard. Existing upstream `run` remains unchanged.
Own Session/dispatch thread/stop/unmount explicitly in git-vfs. Cancel native Git
first, detach while dispatch can serve final unmount requests, then stop dispatch
and join filesystem/database teardown
before releasing the working-tree owner. Idle-client references must not prevent
exit; an uncancellable host sync already in progress is not a hard wall-clock bound.
No fusectl access, guessed connection IDs, thread kills or client termination.
Validate idle file+directory handles, stalled reads with/without prefetch,
external unmount and immediate remount/acknowledged-data persistence.

## Stage A: baseline harness and conservative reclamation

Freeze baseline binaries before rebuilding. Promote the disposable churn probe into
an explicit-CLI harness with fresh roots, deterministic payloads, binary/source
hashes, rotating baseline/candidate runs, checkpointed failures and owned cleanup.
Measure RSS, allocated disk (not sparse file length), mount/remount time, logical
live/dead payload bytes, tombstone count and journal size/generation.

At exclusive overlay open, before serving a fresh FUSE session, strip payloads from
unlinked non-control records. Retain lightweight tombstones, base identity and
parent/name metadata. Do not alter journal identity, generations or dirty paths.
Prune as records are decoded, not after accumulating all dead data in RAM; use
bounded maintenance transactions. Reject direct reads/digests of reclaimed dead
entries instead of falling through to base content (`data=None` currently means
base fallback). Maintenance is idempotent and does not emit user mutations.

This stage bounds dead *payload* at remount, not tombstone count or live-session
churn. Replacing values makes old LSM versions reclaimable; physical disk shrink
follows compaction and is measured separately.

### Stage A measured checkpoint (2026-10-19)

Three rotated actual-FUSE trials per arm, 64 operations of 256 KiB, against
frozen `c0687ee`. [Samples and correctness checks](../benchmarks/2026-10-19-vfs-remount.json).
Every candidate session emitted valid counters; remount retained the linked
256 KiB edit and reclaimed 16 MiB of dead payload (plus seven bytes for atomic saves).

| Median | Baseline | Stage A |
|---|---:|---:|
| Unlink churn | 125.23 ms | 133.81 ms |
| Atomic-save churn | 138.20 ms | 138.35 ms |
| Remount RSS, unlink | 55.26 MiB | 39.50 MiB |
| Remount RSS, atomic-save | 71.23 MiB | 55.43 MiB |

Allocated disk stayed approximately 32.52/48.59 MiB respectively; immediate
physical shrink is not established. Live-session retention is unchanged.
One unlink candidate took 228 ms; it remains in the samples. Earlier diagnostic
runs exposed missing shutdown counters and an idle-handle join hang; those runs
were not accepted as complete mechanism verification. The final dispatch fix also
passed shutdown/remount with idle file and directory handles as normal user and
inside an isolated root user/mount namespace. This is not a Bazel latency result.

## Stage B: mutable data as ranged backing files

Introduce a versioned payload abstraction with base-blob, legacy-inline and owned
file-backed states; callers request range reads, writes, truncate and digest streams
rather than cloning an `Entry`'s complete bytes. Keep metadata in Fjall and payload
files in the private overlay, addressed by validated inode/content identities—not
user-controlled paths. Use `pread`/`pwrite` and preserve zero-truncate without hydration.

Migration occurs under exclusive ownership, incrementally and idempotently. Persist
new payload data and its directory before publishing a metadata reference. Retain
legacy records until the replacement is durably published. An older binary must
fail closed on the new format, never create conflicting inline/file-backed states.

In-place range updates need explicit recovery: a durable bounded redo intent records
inode, content generation, offset/length or truncate, new metadata and journal paths.
Persist intent before modifying payload; sync payload before publishing matching
Fjall metadata; remove the intent atomically with durable metadata/journal publication. The intent
contains expected pre/post generations; recovery distinguishes pending from already
committed operations, rejects stale conflicts and never emits duplicate journal
events. Admit no later mutation until this protocol has completed. Recovery replays
prepared operations idempotently before serving, including an operation that was
in-flight at crash. Any error after an intent becomes durable poisons mutable access until recovery;
do not continue serving partially updated bytes with old metadata. New-format
records must make older binaries reject the overlay (a new meta key alone is not
sufficient). Base-digest selection must use payload state, not `data.is_none()`.
Namespace operations remain transactional in Fjall. Initial
implementation keeps the single serialized mutation order; concurrent writers may
not bypass it. Specify and test each failure boundary before enabling this format.
Large intents stream to owned files rather than duplicating unbounded buffers.

An empty newly created file may remain inline until its first content change.
For Base/Inline to File copy-up only, prepare the complete backing file plus the
requested range/truncate, sync that file and its directory, then publish its
reference with one metadata/journal transaction. The prior database content is
authoritative until publication; a failed preparation cannot damage acknowledged
bytes. This is a one-time transition, not full-file COW on later range writes.
Keep the fixed redo scratch pathname between operations: sync its new contents
before each intent, but do not create/unlink and sync the directory per write.
Only reset unreferenced scratch after tracing any pending intent during recovery.

Do not use full-file COW on every write as the normal fallback: that keeps the
amplification this stage is intended to remove. Document any filesystem-specific
optimization (e.g. reflink) as optional, not a correctness prerequisite.

### Stage B measured checkpoint (2026-10-19)

Three rotated trials against the same frozen baseline; 16 MiB edited file and
64 × 4 KiB operations. [Samples, counters and retained regression](../benchmarks/2026-10-19-vfs-ranges.json).

| Median | Baseline | Stage B |
|---|---:|---:|
| First hydration/copy-up | 42.42 ms | 38.90 ms |
| 64 warm range reads | 24.42 ms | 0.47 ms |
| 64 appends | 1,034.25 ms | 105.95 ms |
| RSS after appends | 441.91 MiB | 8.07 MiB |
| Allocated overlay after appends | 81.36 MiB | 16.45 MiB |
| 64 create/delete operations, 256 KiB | 126.97 ms | 125.38 ms |
| 64 atomic saves, 256 KiB | 150.09 ms | 126.39 ms |

All 18 final range/churn cells passed byte, remount and mechanism checks. An
earlier version regressed churn by 73–86%; unpublished first copy-up and a
persistent redo pathname removed that regression without relaxing durability.
The first-copy-up path still obtains base bytes through the whole-blob Git API.
Logical payload counters exclude redo scratch traffic; metadata counters count
configured commit-barrier attempts, not kernel syscalls. Warm range timing excludes
verification reads, which remain included in cumulative counters. This does not
establish a Bazel latency improvement or online GC bound.

## Stage C: online lifetime accounting and bounded journal

Track FUSE lookup/forget references, open-file/directory references and asynchronous
pins. Unlinked data is eligible only when all relevant users are gone. Persist an
inode allocation high-water before deleting high-numbered records. Preserve compact
base deletion tombstones independently of disposable dead payloads/records.

Move physical deletion out of namespace locks through a bounded queue with explicit
drain/cancellation; saturation must apply bounded backpressure or safe synchronous
fallback, never drop work or build an unbounded backlog. Closing cached descriptors
is part of reclamation. Crash cleanup traces durable metadata/intents and never
age-deletes unknown potential user edits.

Initial queued/in-flight deletion admission is 256 jobs and 32 MiB of durable
pending payload bytes, including failed retirements. A single oversized payload
uses synchronous deletion alone after draining other jobs; its bytes remain
reported rather than hidden outside accounting. Persist retirement
with payload-reference removal; unlink and sync the payload directory before
retiring that ledger item. Saturation uses safe synchronous deletion outside the
metadata transaction; an undrainable backlog applies backpressure rather than
accepting unbounded new cleanup work. Inode origin comes from the pinned snapshot,
not blob-OID presence (base directories have no blob OID). Retained-child
dependencies and directory snapshot pins prevent removing a parent still needed
by an unlinked child. Excess forget accounting quarantines an inode from GC.

Bound journal history by accounted serialized bytes and record/generation limits.
Persist the retained-generation floor atomically with trimming. Tokens below the
floor return `/` full invalidation; tokens at/above it return complete changes.
Maintain a separate compact dirty-path set needed by interrupted initial Git-index
setup. Bound that dirty set by path count and encoded bytes too; overflow durably
collapses to `/` invalidate-all until a safe index checkpoint. Response-construction
overflow also returns `/`, never a truncated success. Bound response construction
as well as wire encoding. Retention defaults and
limits are explicit and observable; they are not inferred from remote CAS TTL.

Initial logical limits: 8,192 retained journal records, 4 MiB of key-plus-path
bytes and a 4,096-generation window; 4,096 unique dirty/query paths, 1 MiB of
raw path bytes and an 8 MiB encoded response. Evict complete generations. The
floor is the oldest acceptable input token: records at or below it are gone,
but a token exactly at the floor still receives all later changes. Overflow
returns `/`, never a partial successful response. These limits do not bound
physical WAL/LSM allocation.

Activate bounded history atomically with a new generation key and remove the
legacy `generation` key. Older offline fsmonitor helpers bypass entry decoding;
the payload-format sentinel alone cannot stop them from returning incomplete
history after trimming. New-format activation invalidates prior tokens and
conservatively seeds the dirty set before old history is retired. Keep dirty
candidates (or the bounded `/` marker) indefinitely until a separately durable
Git-index checkpoint protocol exists; a merely present `git-ready` marker is
not such a checkpoint.

### Stage C measured checkpoint (2026-10-19)

Three rotated trials per workload against frozen `c0687ee`.
[Samples, counters and retained regression](../benchmarks/2026-10-19-vfs-online-gc-journal.json).

| Median | Baseline | Stage C |
|---|---:|---:|
| 64 create/delete operations, 256 KiB | 127.38 ms | 137.13 ms |
| 64 atomic saves, 256 KiB | 144.38 ms | 140.70 ms |
| 256 create/delete operations | 451.37 ms | 482.41 ms |
| 256 atomic saves | 517.16 ms | 497.08 ms |
| RSS after 256 create/delete operations | 203.14 MiB | 9.99 MiB |
| RSS after 256 atomic saves | 203.86 MiB | 10.37 MiB |
| Allocated overlay after 256 create/delete operations | 192.93 MiB | 0.60 MiB |
| 12,000 same-path metadata changes | 2.757 s | 3.007 s |
| Expired/old-token query, including CLI startup | 3.12 ms | 0.90 ms |
| Current-token query, including CLI startup | 0.73 ms | 0.73 ms |
| RSS after 12,000 metadata changes | 13.41 MiB | 17.51 MiB |
| Allocated overlay after 12,000 metadata changes | 3.97 MiB | 6.15 MiB |

All 24 churn and six final journal cells passed correctness/mechanism checks.
The churn runs measured zero temporary backing files while still mounted, before
shutdown; the linked survivor and open-unlink/overwritten-open checks survived.
The 12,000-change journal retained 4,096 records / 110,592 key-plus-path bytes;
expired tokens requested full invalidation and current tokens stayed exact.

An earlier journal implementation took 8.18 seconds: each trim repeatedly crossed
an accumulating deleted-key prefix. Seeking from the previous durable floor removed
that ramp; the final metadata-change total is still 9% above baseline. Durable
retention bookkeeping and deletion tombstones increase WAL allocation and RSS in
this workload. Logical history bounds do **not** imply physical LSM/WAL or RSS
reclamation. Expired-token full invalidation is not an equivalent incremental
response. The longer churn measurement precedes the journal seek-only fix; final 64-operation
churn includes both that fix and explicit owner-lock release after shutdown. Binary
hashes and all attempted runs are retained. These are component results, not an end-to-end
Bazel improvement.

## Stage D: namespace and digest work elimination

For ordinary file rename, journal the old/new paths without scanning unrelated
entries. For directories, walk the indexed affected subtree; preserve existing
invalidation semantics until a tested subtree-invalidation contract replaces them.

Cache edited SHA1/BLAKE3 by inode/content generation, not size/mtime alone. Increment
content generation on every successful write/truncate; metadata-only changes do not
force a rehash. Size probes return fixed digest length without hashing. Stream file
hashes rather than loading an entire mutable file into a Vec. Instrument bytes hashed
and hits/misses so mechanism movement can be verified.

## Stage E: hydration scheduling and reusable content

Move potentially blocking content I/O out of serial FUSE dispatch using a bounded
work queue and pinned inode/content snapshots. Namespace mutations retain their
ordering and open-handle semantics. Per-OID single-flight deduplicates demand and
prefetch. Queued foreground requests promote matching speculative requests; already
local unrelated reads do not wait behind a network fetch. Large speculative work is
split at bounded batch boundaries; do not claim preemption of an active Git RPC.

Cache immutable content under byte-accounted bounds with separate resident,
reader-pinned and in-flight counters. A minimum item count must not quietly retain
many huge objects. Admission, eviction and shutdown account for the full lifetime.
Keep remote CAS presence observations, metadata caches and native Git object disk
retention separate. No global drop-caches or direct deletion of Git pack members.

## Benchmark ladder and acceptance

Run focused baseline/candidate comparisons after each material stage, not the full
remote matrix after every small edit. No Cargo/heavy build jobs during timed cells.
Retain failed attempts/outliers, exact configurations, binary hashes and raw samples.
Change one substantive factor when attributing an effect.

| Workload | Measured boundary | Correctness / finish criterion |
|---|---|---|
| Create/delete + atomic-save churn, 64 and longer fixed runs | Live session and remount | No linked-data loss; remount dead payload bytes zero; online eligible payload drains; RSS/disk reported separately |
| Large edited file, range reads and small appends | First materialization separate from warm I/O | Returned bytes and sparse/truncate behavior match; range work proportional to requested bytes, not whole-file copies |
| Journal old/recent tokens, repeated same-path edits | Query and retention work | Explicit byte/record bounds; expired token full scan; current token exact changes; remount/recovery unchanged |
| Atomic replacement in 1k/10k/100k-entry trees | Rename latency and visited-entry counts | Files do not scan repo; directory work limited to affected subtree; inode/open-FD behavior preserved |
| Repeated digest size/value requests + same-size edit | Hash time/bytes and counters | One content hash per generation; fixed size probes do not hash; digest matches current bytes |
| Stalled prefetch plus unrelated local stat/read | Individual request completion | Metadata/local reads complete while remote content is stalled; single-flight and bounded cancellation verified |
| Pinned Bazel repo edit → status → BuildBuddy RBE build | Continuous per-cycle wall time | Real remote actions and actual r2c2 evidence; same edit sequence and matching outputs across candidates |

Use at least three rotated trials for headline local comparisons. Remote cells have
fixed budgets and explicit warm proxy/CAS/toolchain/r2c2 boundaries; report executor
setup variance rather than filter it. Keep first-build/startup checks distinct from
warm incremental iteration. A reproducible >10% local-median regression outside
variance is investigated before retaining a change; noisy remote samples are not a
license to claim a win. End-to-end latency improvement is measured, not guaranteed.

## Tests, rollout and completion

Extend existing overlay/FUSE integration coverage rather than duplicate suites.
Test open-unlink, overwritten-open rename, higher distinct inode allocation,
linked-edit persistence, interrupted migration/redo recovery (including another crash during replay and
short-write/ENOSPC/sync/commit failures), expired fsmonitor
history, same-size digests, queue saturation and shutdown. Fixtures must exercise
real FUSE; mock RPC/process fixtures supplement, not replace, the mounted workflow.

Complete when all stages pass their correctness/mechanism verifiers, relevant
workspace/FUSE tests and clippy pass, matched benchmark evidence is retained and
summarized, and the documented edit/status/real-RBE workflow succeeds. Each stage
gets independent review before its task-only commit; publish the final stack to
local Gerrit. Upstream pushes are a separate explicit action, not automatic rollout.

Non-goals: general checkout/merge/rebase, replacing Fjall, Windows/macOS ports,
shared writable agent state, default aggressive clean-inode invalidation, lazy
whole-tree projection, automatic Git pack eviction, and server/cgroup cache tuning.
Those require evidence or separate design rather than expanding this goal silently.
