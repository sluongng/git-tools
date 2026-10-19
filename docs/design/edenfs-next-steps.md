# Next lessons from EdenFS

The [native platform and output-service follow-up](edenfs-platforms-and-outputs.md)
updates the macOS/Windows lessons, including current NFS GC invalidation and
the boundary between eager output-service portability and future lazy mounts.

Source review: October 19, 2026, at Sapling/EdenFS revision
[`79071c2`](https://github.com/facebook/sapling/tree/79071c2a3ea20c36ed71332608535e5801101bba).
The remaining proposals are design hypotheses, not an EdenFS runtime comparison. The completed
[iteration lifecycle](vfs-iteration-lifecycle.md) covers payload GC, bounded
history, scoped rename, digest reuse and demand-prioritized hydration.

## Priorities

| Priority | Observed gap | Proposed improvement | Required evidence |
|---|---|---|---|
| 1 | `Snapshot::load`, namespace indexes and native Git bootstrap still walk all entries. Snapshot clones now share immutable data. | Load directories by immutable tree ID and allocate durable inode identities when exposed; coordinate this with native Git bootstrap. | Measure mount-ready and Git-ready separately as tree size grows; compare eager projection, remount identity, directory cookies, rename and crash recovery. |
| 2 | Decoded overlay entries now have bounded residency, but namespace summaries/indexes and the immutable snapshot remain eager. | Separate durable identity from remaining loaded directory/index state. | Broad traversal followed by narrow access should plateau; test cwd, open handles, forget/reload and concurrent rename. |
| 3 | Completed schema-v2 responses now survive restart; storage accounting is read-only. Mirrors and mapping databases still have no overall retention policy. | Add separate budgets and ownership/lease rules for refetchable caches and authoritative agent state. | Restart hit rate, interrupted publication, leased readers, disk-full, refreshed refs and pinned agent commits. |
| 4 | Dispatch-cause bytes, demand latency buckets and bounded speculative-usefulness observation are implemented; native Git reads remain outside that observation. | Use attribution to choose prefetch scope; add origin detail only where it supports a measured decision. | Compare identical whole-repo and scoped builds; report early useful prefetch, demand promotions and bytes unobserved within a defined window. |
| 5 | Bootstrap dirty paths overflow to sticky `/`; there is no acknowledged index checkpoint. | Bind acknowledgement to overlay identity, index/commit identity and journal generation, retaining later mutations. | Concurrent edits, killed index publication, replaced/reset indexes, expired tokens and remount. |

Lazy FUSE projection alone does not remove the full-tree work:
`WorkingTree::prepare` runs `read-tree` and initializes fsmonitor-valid bits.
Git index bootstrap needs a deliberate strategy too. Likewise, sticky `/` does
**not** make every normal status command rescan: current-token requests remain
incremental; the penalty concerns bootstrap and fresh-index recovery.

## Bounded follow-ups implemented

- Snapshot clones share immutable nodes and path mappings; initial loading remains
  O(repository entries).
- Decoded overlay-entry LRU defaults to 4,096 entries / 4 MiB estimated bytes,
  reloads evicted entries from Fjall and preserves identities. It does not bound
  namespace indexes, snapshot data, Fjall pages or temporary caller-held guards.
- Generated-response manifests preserve original TTL and validate body integrity
  within startup budgets. Stable ref/config identities preserve unchanged-view
  reuse. Startup checksum I/O and synced publication are explicit costs; old
  schema directories remain untouched and outside the active quota.
- Fetch attribution separates demand/prefetch blob bytes and records bounded
  later-demand observations. Unobserved speculation is not proof of waste.
- Read-only storage accounting exposes bounded partial estimates, not eviction or
  repository leases. Writable overlays and agent commits remain authoritative.
- Remount reads managed Git config once and skips setters only for one exactly
  matching value. Changed values still use native Git; custom config is retained.
  Git readiness remains blocking and still requires native index bootstrap.

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

Next, measure scoped prefetch and restart reuse with the new attribution, then
tackle lazy metadata together with native Git bootstrap and remaining linked-inode
state. Storage retention requires leases before eviction. Index checkpoints are
less urgent. No whole-mount memory bound or EdenFS performance parity is claimed.
