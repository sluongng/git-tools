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
disposable. Retired state is conservatively retained, and cache leases do not
guarantee indefinitely open NFS handles. See [BOS setup](../bazel-output-service/README.md).
