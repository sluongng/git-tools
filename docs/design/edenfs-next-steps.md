# Next lessons from EdenFS

Source review: October 19, 2026, at Sapling/EdenFS revision
[`79071c2`](https://github.com/facebook/sapling/tree/79071c2a3ea20c36ed71332608535e5801101bba).
These are design hypotheses, not an EdenFS runtime comparison. The completed
[iteration lifecycle](vfs-iteration-lifecycle.md) covers payload GC, bounded
history, scoped rename, digest reuse and demand-prioritized hydration.

## Priorities

| Priority | Observed gap | Proposed improvement | Required evidence |
|---|---|---|---|
| 1 | `Snapshot::load` recursively lists all entries; the overlay copies/indexes them. Native Git bootstrap also walks the snapshot. | Load directories by immutable tree ID; allocate durable inode identities when exposed. Consider compact/shared immutable metadata first. | Measure mount-ready and Git-ready separately as tree size grows; compare eager projection, remount identity, directory cookies, rename and crash recovery. |
| 2 | Reclamation only collects unlinked inodes; linked metadata stays resident. | Separate remembered identity from loaded inode/directory state, then bound reconstructible userspace caches. | Broad traversal followed by narrow access should plateau; test cwd, open handles, forget/reload and concurrent rename. |
| 3 | Generated fetch responses are discarded on proxy restart; mirrors and mapping databases have no overall retention policy. | Restart-reusable completed responses with a versioned durable manifest; separate budgets for refetchable caches and authoritative agent state. | Restart hit rate, interrupted publication, leased readers, disk-full, refreshed refs and pinned agent commits. |
| 4 | Fetch counters do not show speculative usefulness or demand wait cost. | Bounded cause/origin/byte and queue/service-time attribution before adaptive prefetch. | Compare identical whole-repo and scoped builds; report early useful prefetch, demand promotions and unused bytes over a defined window. |
| 5 | Bootstrap dirty paths overflow to sticky `/`; there is no acknowledged index checkpoint. | Bind acknowledgement to overlay identity, index/commit identity and journal generation, retaining later mutations. | Concurrent edits, killed index publication, replaced/reset indexes, expired tokens and remount. |

Lazy FUSE projection alone does not remove the full-tree work:
`WorkingTree::prepare` runs `read-tree` and initializes fsmonitor-valid bits.
Git index bootstrap needs a deliberate strategy too. Likewise, sticky `/` does
**not** make every normal status command rescan: current-token requests remain
incremental; the penalty concerns bootstrap and fresh-index recovery.

## Transferable contracts

- Eden separates known directory entries from lazily instantiated inodes and
  remembers allocated identities: [inode model](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/docs/Inodes.md#L65-L114).
- Loaded objects can be discarded without discarding their identity:
  [inode lifetime](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/docs/InodeLifetime.md#L188-L201).
  Start with userspace unloading, not proactive kernel directory invalidation;
  pinned cwd/root directories require special care:
  [pressure-GC safety](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/config/EdenConfig.h#L743-L759).
- Distinguish permanent authority from rotating, refetchable data:
  [store ownership](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/scm/lib/revisionstore/src/indexedlogdatastore.rs#L240-L289).
  Never remove native Git packfiles directly for quota enforcement. Use Git
  maintenance; eviction needs leases and a proven refetch path. The writable
  overlay and agent commits are not disposable caches.
- Fetch cause and origin are separate dimensions:
  [fetch context](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/store/ObjectFetchContext.h#L86-L123).
  Retain foreground priority and avoid high-cardinality path/PID labels. Current
  speculative reads without demand waiters already bypass anonymous content
  cache population; preserve that behavior.
- Cache validity needs root and journal identity:
  [status validation](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/store/ScmStatusCache.cpp#L109-L147).
  A successful status command or `git-ready` marker is not index acknowledgement.

## Boundaries and suggested order

Keep Fjall and strict byte bounds; copy ownership contracts, not Eden's engines
or minimum-item settings. Keep Bazel outputs outside the source mount; Eden also
[redirects generated writes](https://github.com/facebook/sapling/blob/79071c2a3ea20c36ed71332608535e5801101bba/eden/fs/docs/Redirections.md#L25-L40).

After the Bazel/crate split, fetch attribution and restart-persistent responses
are bounded first steps. Then tackle lazy metadata together with native Git
bootstrap, followed by linked-inode unloading. Index checkpoints are less urgent.
