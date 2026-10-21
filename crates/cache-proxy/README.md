# cache-proxy

Cache, execution and Remote Asset forwarding for the workspace daemon.
It shares one `Arc<local_cas::LocalCas>` with source/output services. The daemon owns listeners, credentials, authorization scope and shutdown.

Verified reads reuse local bytes. Writes succeed only after verification and
upstream commitment. `FindMissingBlobs` checks upstream, not local residence.
Downloader mapping hits require verified bytes and upstream presence; metadata
is bounded by TTL/LRU. SHA256/BLAKE3 and compressed transports are supported.
Execution streams retain caller deadlines and leave retries to Bazel.

Embedded action-result caching defaults off; a positive TTL requires an upstream
retention assumption. Optional background maintenance refreshes used AC/CAS
entries and repairs missing remote blobs from verified resident bytes, without
changing foreground presence semantics. See [policy](../../docs/reapi-policy.md).

Unix sockets require a private per-user directory; TCP requires bearer auth.
Do not share a daemon across authorization boundaries. Incoming headers never
select upstream credentials. Bazel clients need the [Unix authority fix](https://github.com/bazelbuild/bazel/commit/242563f1a14378801725e5f6b0ee397ef0d307f2).

Embedders stop forwarding, await `wait_for_maintenance_shutdown`, then drain
`LocalCas::wait_for_idle` before releasing the shared cache owner.
