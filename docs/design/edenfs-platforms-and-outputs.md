# EdenFS lessons for native workspaces and Bazel outputs

Reviewed October 19, 2026 against current upstream EdenFS
[`79071c2`](https://github.com/facebook/sapling/tree/79071c2a3ea20c36ed71332608535e5801101bba)
and Bazel **9.2.0**.
Existing [lifecycle work](vfs-iteration-lifecycle.md) and
[platform contracts](platform-frontends.md) remain in effect.
Recommendations below are not measured EdenFS performance comparisons.

## Decision

Keep NFSv3 on macOS and ProjFS on Windows.
Share content retrieval and identity rules, not a Unix-shaped mutable overlay on every platform.
Keep build scratch and the output base outside the source projection.

The Bazel output service retains **eager local-directory storage by default**.
Opt-in native lazy projections now use the shared `output-core`, `output-nfs`
and `output-projfs` crates. Native portability and lazy acceptance remain
separate: a successful build alone does not establish metadata-only staging.

### Native baseline verified in this follow-up

| Surface | Evidence | Scope |
| --- | --- | --- |
| macOS NFS workspace | Native mount, local locks, lazy read, Git commit and two remount/restarts pass without sudo. | Existing writable Git projection; not live socket takeover. |
| macOS eager output service | 6 native protocol tests and 12 real Bazel/BuildBuddy CAS phases pass. | Both digest functions, output execution, two daemon restarts, repair and clean. |
| Windows eager output service | 5 native protocol tests and the same 12 real CAS phases pass. | Includes read-only replacement, sharing conflict and junction boundaries; native command execution. |

The CAS runs observed six staged files per host and no cleanup errors.
They are correctness smoke tests, not comparative performance benchmarks.

## Native lazy acceptance

The opt-in adapters pass **12 real Bazel 9.2 / BuildBuddy CAS phases per host**:
SHA256 and BLAKE3, two actual daemon restarts, output execution, tamper repair and
clean. Each initial cached build stages both artifacts with **zero payload
fetches before the first consumer**. No local fallback or cleanup errors occurred.

Single correctness-run timings for the two-output fixture, not an eager/lazy
benchmark or a large-repository result:

| Native lazy client | Digest | Cached build | Through read + execution | Payload fetched before consumer |
| --- | --- | ---: | ---: | ---: |
| macOS NFS | SHA256 | 1.893 s | 2.471 s | 0 B |
| macOS NFS | BLAKE3 | 1.947 s | 2.451 s | 0 B |
| Windows ProjFS | SHA256 | 2.922 s | 3.422 s | 0 B |
| Windows ProjFS | BLAKE3 | 2.984 s | 3.500 s | 0 B |

First-consumer times include up to 50 ms diagnostic sampling latency. Consumers
read and execute all fixture outputs, fetching 65 B per digest on macOS and
83 B on Windows. Across staging, repair and restart, six files are staged but
only four blobs fetched: 130 B on macOS, 166 B on Windows. These tiny fixtures
establish deferred hydration, not a bandwidth or end-to-end speedup claim.

Focused coverage: core 11 tests, BOS 8, NFS protocol 4 plus native mounted 5,
ProjFS policy 1 plus native 3. Native channels cover generation races, negative
caches, local edits and clean; Windows additionally verifies 32 readers during
shutdown with successful payloads checked. Native cancellation attempts all
completed normally: **the CancelCommand callback was not observed**. Graceful
ProjFS shutdown drains existing verified reads before stopping; the configured
CAS adapter supplies a total RPC deadline. Generic BlobSource implementations
must also provide a deadline. Forced process termination is not graceful shutdown.

The acceptance run exposed and fixed publication beneath native Bazel-created
parents, Windows metadata-only changes forcing unnecessary hashing, cached NFS
missing ancestors and synchronous realpath/statfs re-entry in StartBuild.

## Lessons that change our priorities

| Priority | Current gap | Change | Acceptance |
| --- | --- | --- | --- |
| P0 | NFS holds one state mutex through blob I/O; ProjFS holds its repository mutex through full-blob reads. | Capture immutable identity under a short lock, then use bounded, asynchronous, coalesced retrieval outside namespace locks. | A stalled cold read must not stall unrelated warm metadata; delayed old reads cannot publish over a newer generation. |
| P0 for outputs | Source providers have fixed bases and no out-of-band publication barrier. | Give each channel explicit publication/invalidation completion. | Cached missing paths and cached old bytes change immediately after successful staging, without TTL sleeps. |
| P1 | NFS directory pages clone/search all children and reject a removed cursor. | Page through our existing ordered inode index; batch unknown size metadata for the page. | Resume after cursor deletion; no duplicates among surviving entries; metadata enumeration fetches no regular-file content. |
| P1 | ProjFS callbacks and directory enumeration perform blocking work under shared locks. | Pending-command lifecycle, per-enumeration state, cancellation and byte/request budgets. | Bounded callbacks and shutdown under concurrent cold reads; one cancelled waiter does not cancel another reader's fetch. |
| P2 | Native caches retain working-set state beyond a short agent task. | Channel-specific clean-cache reclamation, separate from authoritative edits and explicit clean. | Repeated edit/delete/build sessions reclaim eligible state without losing dirty files, tombstones, open streams or cwd. |

Eden's current NFS dispatcher uses asynchronous reads and bounded request admission.
Its cached readdir index is mutation-validated; we already have an ordered child index and need not copy that extra cache.
Its readdir predictor warms trees/auxiliary metadata separately from file contents.
See [NFS dispatch](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/inodes/NfsDispatcherImpl.cpp#L139-L152),
[directory pagination](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/inodes/TreeInode.cpp#L3721-L3849),
[metadata prefetch](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/inodes/TreeInode.cpp#L8013-L8180),
and [request admission](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/nfs/Nfsd3.cpp#L2556-L2574).

## macOS: distinguish publication, GC and restart

**Local locking works without NLM or sudo.**
Our native verifier now exercises separate-process `flock` and `fcntl` exclusion/release using `locallocks,noresvport`.
These are locks on one Mac, not distributed locking.
Eden also selects [client-local locks](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/privhelper/PrivHelperServer.cpp#L804-L812).

**Publication requires an invalidation barrier.**
NFSv3 has no direct push invalidation.
Eden updates parent mtimes and asynchronously performs same-mode chmod through the mount so the client observes changed post-operation attributes.
It waits for completion outside filesystem locks because the operation re-enters its server.
Open/close alone does not force macOS to discard cached data.
Our output control plane must not acknowledge a new namespace generation before its visibility barrier completes.
Synthetic invalidation must not accidentally dirty the workspace or recurse while holding our state mutex.
See [the channel contract](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/nfs/Nfsd3.h#L265-L357).

**GC invalidation is different.**
Current Eden deliberately returns STALE for its recognized GC SETATTR to discard cached directory names when mtime has not changed.
Its pressure-GC path can scan process cwd, descriptors and mappings.
This is a snapshot, not a race-free pin guarantee; missing or uncertain pin evidence must make our reclamation conservative.
This is more involved than its older non-materialized-directory GC.
Do not wire our FUSE zero-pin retirement directly into NFS: NFS gives us no close/forget notifications.
Retain conservative reclamation until open-FD, mmap and cwd behavior is verified natively.
See [GC SETATTR](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/nfs/Nfsd3.cpp#L627-L645)
and [current lifetime design](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/docs/InodeGarbageCollection.md#L115-L249).

**Restart currently means unmount/remount.**
Our server deliberately expires old process-generation filehandles.
Eden supports socket takeover and restored inode identity; removing our generation check would not reproduce that guarantee.
A live-upgrade milestone needs persistent export identity, non-reused inode generations, replay rules and mutation draining.

Do not copy every Eden option.
READDIRPLUS and Unix-domain NFS remain disabled by default in its configuration; the latter documents kernel read errors.
Its filesystem-name override is not a supported Bazel watcher integration.
Use an explicit batch metadata/digest API rather than pretend NFSv3 has digest xattrs.
Stock Bazel consuming that API is a separate integration.

## Windows: NTFS is the writable authority

Our fixed-base ProjFS provider already preserves instance GUID, native edits/tombstones and immutable Git blob ContentID.
Keep that model rather than duplicate writable file contents in Fjall.
Native Git reads actual disk state, so Eden's full startup inode reconstruction is not automatically necessary for us.
Add offline-edit/kill/restart coverage before introducing cached dirty-state or digest answers.

Move cold retrieval off callback threads with the documented
[pending completion/cancellation contract](https://learn.microsoft.com/en-us/windows/win32/projfs/asynchronous-callback-handling).
Deep-copy callback data, track command lifetime and complete exactly once.
Eden uses asynchronous reads, but its cancellation implementation still has an interruption TODO; it is not a complete cancellation template.

For cache GC, evict only known clean placeholders with flags that cannot remove dirty data, dirty metadata or tombstones.
Keep entries on sharing violations or uncertainty.
Do not copy Eden's `ALLOW_DIRTY_DATA` flags: its own GC source describes a race that can delete newly materialized user data.
See [invalidation flags](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/prjfs/PrjfsChannel.cpp#L808-L814)
and [the race warning](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/inodes/TreeInode.cpp#L7829-L7845).

Do not reuse all workspace policy for outputs.
Our source provider restricts directory renames, external symlinks and revision changes.
Generated output directories need their own semantics.
Preserve actual Windows name collation; reject alternate streams, reserved devices and ambiguous names at the API boundary.

## Implemented lazy output architecture

The CLI defaults to `--storage lazy`, selecting Linux FUSE, macOS NFS or Windows
ProjFS. Lazy modes require a durable `--state-dir` outside the mount root.
Eager downloads are available only through the CLI-only `--debug-eager` switch,
not normal storage selection or TOML. See the complete
[configuration example](../../examples/bazel-output-service.toml).
Responsibilities are split into separate crates:

| Component | Owns | Must not own |
| --- | --- | --- |
| `output-core` | Durable per-output-base namespace, generations, dirty/local state, tombstones, CAS locators and cache leases | Git index/bootstrap, platform callbacks |
| `output-nfs` | NFS operations, mounted publication barrier and NFS handle lifetime | Git object identity or CAS authentication policy |
| `output-projfs` | Placeholder versions, pending requests, view updates, native dirty state and negative-cache invalidation | A second copy of authoritative NTFS edits |
| `bazel-output-service` | Build lifecycle, locator validation, lazy backend selection and explicit debug mode | Pretending staging completion proves kernel visibility |

CAS outputs are not modeled as Git blobs. Shared NFS transport has a per-provider
request budget; the output channel reserves metadata capacity under cold-read load.
Fjall transactions update individual namespace records and persistent indexes,
not a serialized copy of the whole tree.

### Identity and publication

1. A content key includes configured backend identity, instance, digest algorithm, hash and size.
2. A durable entry ID/generation binds that content to a path in one output base.
3. Stage reserves a new binding and records recoverable publication state.
4. The channel updates affected views and completes invalidation outside namespace locks.
5. Only then does staging report success.
6. Hydration pins its immutable content/entry generation; late completion cannot overwrite a newer binding.

On Windows, versioned placeholder ContentID resolves an immutable record, not the latest digest at the old path.
Clear negative cache when new paths become visible.
Surface dirty/open-handle update conflicts; never advertise unconditional success while the kernel still serves an incompatible old view.
Use the platform's [view-change APIs](https://learn.microsoft.com/en-us/windows/win32/projfs/handling-view-changes).

On macOS, use fresh non-reused entry identity for replacement and preserve the lifetime of old handles.
Do not equate path reuse with inode reuse.
GC cannot reclaim a generation merely because a new path binding exists.

### Integrity and digest trust

The implementation fetches each requested CAS blob into a bounded disk cache,
verifies the whole digest/size, then serves ranges. Concurrent readers coalesce;
one canceled waiter does not cancel the shared fetch. Fingerprint-stable warm
ranges reuse whole-object verification. Declared blobs exceeding cache/in-flight
budgets fail before publication.
A 4 KiB range request does not authenticate an arbitrary range of a SHA256/BLAKE3 blob.
Chunked transport can follow once verified chunk identities and whole-object composition are available.

`BatchStat` may answer without hydration only for an untouched, generation-matched remote entry.
Local content writes, truncation, replacement and offline edits invalidate that
digest authority. On Windows a dirty placeholder indicates metadata-only changes,
not modified content; a full file indicates writable content. Native file identity
and rename distrust must still match before retaining its locator. See
[Microsoft's cache-state contract](https://learn.microsoft.com/en-us/windows/win32/projfs/cache-state).
Hash local contents or return an unknown locator conservatively.
Bazel 9.2 uses BOS locators as fast digests, so an incorrect metadata answer can produce an incorrect action key.
Its client also does not yet consume `modified_path_prefixes` for incremental invalidation; a journal alone will not remove its full-rescan fallback.
See [the pinned Bazel client](https://github.com/bazelbuild/bazel/blob/9.2.0/src/main/java/com/google/devtools/build/lib/remote/BazelOutputService.java).

### Native acceptance and benchmarks

Eager portability acceptance uses the same real Bazel/BuildBuddy CAS workflow on each host: both digest functions, cache-hit staging, execution, incremental build, two daemon restarts, tamper repair and clean.
Windows uses a native command artifact, not Unix execute-bit assertions.
Directory junction adoption does not require treating paths as file URIs.
Native cache-client acceptance does not provision Darwin or Windows RBE workers; target/execution-platform compatibility remains operator configuration.

Native acceptance checks require:

- Stage and metadata stat cause zero payload downloads; first read verifies one fetch and later reads reuse it.
- Cached ENOENT → stage and same-sized A → B replacement are immediately visible after publication.
- Delayed A hydration races B staging without publishing stale bytes.
- Dirty local outputs return correct digests; offline changes survive provider restart.
- Open descriptors, mappings, directory handles and cwd survive permitted GC/replace operations.
- CAS missing/corrupt/expired data fails honestly without accepting unverified bytes.
- File/directory replacement, deletion/recreation, separate output bases and clean behave correctly.

Compare eager and lazy variants on the same native host and cache state.
Report stage/build completion, first usable output, first-read latency, network bytes, retained disk and metadata p50/p95 separately.
Use 1/8/32 concurrent cold reads plus warm stat/list traffic, large-file random reads, repeated edit/build cycles and cancellation under load.
Include the cost moved from build completion to the first consumer; metadata-only staging is not automatically a faster usable build.

### Deliberate lifetime limits

The verified cache is bounded, but retired IDs and authoritative local payloads
are retained conservatively. Cache leases protect active reads, not indefinitely
open NFS descriptors: eviction followed by CAS expiration can make a later read
fail. It cannot resolve a new generation at the old identity. There is no claim
of indefinite offline FD/mmap preservation or live NFS daemon takeover.

Windows keeps writable data on NTFS. Persisted native file identities and rename
distrust prevent moved/offline-changed files from retaining the old path digest.
Ordinary native temporary-directory renames work; directories containing projected
remote descendants cannot move until subtree rebasing is implemented. Native
publication journals are atomically replaced and recovered before other mutators.

The CLI always attempts cleanup after a successful mount. A failed unmount keeps
the filesystem server alive until the user releases handles and interrupts again.
Generic core-local-payload power-loss durability is only implemented for Unix;
ProjFS does not use that payload path. Sudden power loss was not fault-injected.
