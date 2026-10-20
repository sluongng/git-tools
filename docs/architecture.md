# Architecture and cache ownership

Git provides transport, object storage and history. Fjall stores metadata and
writable namespace state. Native filesystem adapters expose the workspace;
BuildBuddy supplies remote execution and CAS separately.

![Architecture: private agents, shared caches and BuildBuddy](diagrams/architecture.svg)

The diagram is a logical ownership map, not a required deployment topology.
Purple components are built in this project; gray components are external.
Cylinders represent storage; boxes represent services and mount frontends.
Each agent keeps writable source, Git admin and Bazel output roots private.
`workspace` hosts Linux source mounts, BOS, Git transport and the REAPI cache
front end in one daemon; `agent-session` remains the standalone lifecycle client.
Native source/output adapters also support macOS NFS and Windows ProjFS.

- **Source:** the proxy serves filtered Git fetches and object metadata; edits
  remain in the agent's private overlay/admin state. Native commit can hydrate blobs.
- **Dependencies:** a primed host repository-tree cache and remote r2c2 are
  independent. Remote hits stay lazy and do not populate the host tree cache.
  Bazel's optional disk cache is separate and is ignored with BOS in Bazel 9.2.
- **Execution:** Bazel sends `Execute` to BuildBuddy’s execution service; the
  scheduler dispatches tasks to RBE workers. Optional proxy CAS sync publishes
  main-workspace blobs asynchronously; queued is not uploaded.
- **Outputs:** Bazel sends BOS paths/digests, not payloads. BOS checks remote
  presence, then fetches and verifies complete blobs on first content access.
  The daemon shares one `Arc<LocalCas>` across source reads, Git CAS sync, BOS and
  REAPI downloads; standalone proxy/BOS processes still need separate directories.

## What can be evicted?

| Storage | Ownership and cleanup |
| --- | --- |
| Workspace overlay, private Git admin, local commits | Authoritative user work; never delete as a cache |
| BOS state and locally edited outputs | Authoritative mutations; retain across restart; explicit Bazel clean is destructive |
| Git object databases/mirrors | Native Git format; no proxy mirror quota/automatic eviction; use Git-aware maintenance |
| Fjall size/BLAKE3 mappings | Disposable immutable metadata; remove only while no owner uses it |
| Generated Git responses | Separate request/ref-view cache; quota/TTL/LRU skip active readers |
| `local-cas` payloads | Verified immutable bytes; byte-bounded LRU skips active leases/download reservations |

Each `local-cas` directory has **one process owner**. Library consumers share
`Arc<LocalCas>`; separate proxy/BOS processes need separate directories.
Cache quotas cover logical payloads, not metadata, filesystem blocks or RSS.
The daemon's AC metadata cache is separate, memory-bounded, and disabled by
default (TTL zero). Configuring a positive TTL requires a retention assumption.
CAS writes and `FindMissingBlobs` reach the upstream before claiming availability
to Bazel: a local hit alone cannot satisfy a remote executor's input requirement.

Local CAS keeps maintained byte totals and an ordered unpinned-entry index.
Recency checkpoints are batched and restored on restart; contents are still
reverified. Crashes may lose recent touches. Active leases protect paths until
read/upload completion. Unlink remains serialized with lookup/pinning; asynchronous
deletion needs an explicit retiring state first. See the [crate API](../crates/local-cas/README.md).

## Boundaries worth preserving

- Digest identity includes backend authorization scope, instance, algorithm,
  hash and size. A raw Git SHA1 includes the object header; REAPI BLAKE3 does not.
- Staging output metadata never proves content availability. BOS checks retained
  remote outputs at `StartBuild`; a proxy TTL hint cannot replace that check.
- Native output publication must invalidate old bytes and missing-path caches
  before acknowledging success. A late old-generation read cannot replace new data.
- NFS has no close/forget callbacks or digest xattrs. ProjFS keeps writable data
  on NTFS. Do not copy Linux inode-GC rules blindly across platforms.
- Mirror and mapping retention need leases/refetch guarantees before eviction.
  The verified raw CAS cannot replace Git packs or authoritative writable files.

## Design sources

We borrow repository coordination from [Goblet](https://github.com/canva-public/goblet),
separate origin refresh/serving from [gitretriever](https://www.datadoghq.com/blog/engineering/gitretriever/),
and complete-response caching from [Packhorse](https://gitlab.com/gitlab-org/packhorse).
[Origin](https://cursor.com/blog/git-at-any-scale) reinforces native Git storage;
we do not implement its distributed write/replication layer.

Local eviction follows [EdenFS byte totals and recency indexing](https://github.com/facebook/sapling/blob/f787ffaceddd5a350212542fbe676702360f694a/eden/fs/store/ObjectCache-inl.h)
and [Buck2 batched access-time persistence](https://github.com/facebook/buck2/blob/5cb30436e4070ecf0770fef256e157fd467ec734/app/buck2_execute_impl/src/materializers/deferred/command_processor.rs).
Our hard file leases are stronger than Eden's in-memory interest handles.

[actiond](https://github.com/hermeticbuild/actiond/blob/430aefe62e02d180ff9bd84bf1b29b8c2d499c56/ARCHITECTURE.md)
adds a useful comparison: lazy Directory lookup, backing-file reads and same-disk
CAS publication avoid eager input forests and duplicate copies. It is a local
Linux-VM executor, not a replacement for native source mounts or BuildBuddy RBE.
Its inspected version uses SHA256 and lacks remote-cache compression; do not
assume it accepts this project's BLAKE3 configuration. We borrow the boundaries,
not its kernel module or a new execution backend.
