# local-cas

Reusable verified SHA256/BLAKE3 disk-cache library. Open a directory once with
`LocalCas::open(path, source, config)` and share its `Arc<LocalCas>` across consumers.
`BlobSource` supplies bytes; `content(artifact).await` returns a verified
`Arc<CachedBlob>` lease whose file remains readable until the lease is dropped.

Byte-bounded LRU eviction skips active leases and reserves in-flight bytes.
Concurrent fetches coalesce; count, byte and timeout budgets are configurable.
Retained bytes are reverified after restart. Recency hints survive graceful
shutdown, but crashes may lose recent touches. Keys include authorization scope,
instance and digest; callers must assign distinct identities to distinct scopes.

`cached_content` probes verified local bytes without fetching;
`cached_content_for_maintenance` also suppresses user-access notifications.
`content_from` supplies a per-request source such as native Git extraction.
Stop producers and callers, then await `wait_for_idle()` before shutdown;
caller timeout alone does not prove background work has stopped.

A directory has one owning process; independent processes need separate directories.
Store **only disposable immutable blobs** here, never edits or namespace databases.
The [`workspace` daemon](../workspace-daemon/README.md) shares this owner across
source/output services, Git uploads and its REAPI cache front end.
