# output-core

Library for durable, generation-aware namespaces shared by native Bazel output
and source mounts. `Tree` stages metadata without fetching; `BlobSource`
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
or BOS finalization. ProjFS retains legacy placeholder IDs across restart; new
remote placeholders carry immutable CAS locators so retired metadata can be
reclaimed without deleting native edits. Complete namespaces use schema 3 and
read schema 2. Explicit lazy source namespaces use schema 4; older binaries must
reject them. Preserve state rather than downgrade it.
These retention domains are not a total disk quota.
See [BOS setup](../bazel-output-service/README.md).

Lazy source roots verify one whole directory at a time and preserve unopened
subtree IDs. Capture them with `SourceDeltaSnapshot`, not a partial
`SourceManifest`. Checked acknowledgements retain local edits and require exact
canonical ancestry. The first clean-to-dirty write records durable intent before
changing bytes; ambiguous failures require reopen/recovery, not a clean snapshot.
