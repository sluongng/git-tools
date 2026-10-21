# output-core

Library for the durable, generation-aware output namespace shared by native
Bazel output mounts. `Tree` stages metadata without fetching; `BlobSource`
supplies CAS retrieval/presence checks. Reads verify whole blobs and coalesce
concurrent fetches through [`local-cas`](../local-cas/README.md).

Immutable entry IDs keep old handles from resolving replacement bytes.
Local changes drop remote digest authority. Fetch counts, cache bytes and
in-flight bytes are bounded; oversized files can stage but fail read admission.

Adapters implement `Projection` publication and native mutations.
**Preserve namespace state and local edits**; only immutable cache bytes are
disposable. Explicit bounded retirement collection requires inode/lookup leases;
linked local edits and pending publication are never collected. A durable ledger
replays only owned payload deletions after restart. IDs are never reused.

FUSE supplies real lookup/open lifetimes and collects retired generations after
release. NFSv3 retains every generation for the entire server session; collection
is safe only after unmount/remount expires its handles, not after an idle period
or BOS finalization. ProjFS conservatively retains generations across restart
because persisted placeholders may still hydrate them; native edited files are
never cache-evicted. These retention domains are not a total disk quota.
See [BOS setup](../bazel-output-service/README.md).
