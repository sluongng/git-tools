# Local CAS

Reusable verified SHA256/BLAKE3 disk cache. Open a directory once with
`LocalCas::open(path, source, config)` and share its `Arc<LocalCas>` across
consumers. `BlobSource` supplies remote bytes; `content(artifact).await` returns
an `Arc<CachedBlob>` lease whose path is safe to read until the lease is dropped.

- Exact hash/size verification and atomic publication; retained bytes are
  reverified after restart.
- Byte-bounded LRU eviction skips active leases and reserves in-flight bytes;
  maintained byte totals and an ordered index avoid full-cache victim scans.
- Concurrent reads coalesce; a cancelled/timed-out waiter does not cancel another
  reader's admitted fetch. Concurrency, byte and timeout budgets are configurable.
- Keys include backend authorization scope, instance, digest function, hash and
  size. Callers must give distinct backend identities to distinct auth scopes.
- One owner holds the cache directory lock, including outstanding leases and
  verification work. Independent processes need separate directories; this is a
  shared library/handle, not a multi-process cache daemon.
- Recency hints are checkpointed at most once per five seconds and on graceful
  last-owner drop. Restart restores hints, not verification authority. Use
  `flush_recency()` for an explicit checkpoint; crashes can lose recent touches.

`content_from(artifact, source)` supports an owned per-request source, such as
Git extraction for proxy uploads. Shutdown must stop submissions, cancel/drain
its producers and callers, then await `wait_for_idle()`; timing out a caller is
not proof that admitted work stopped. Public leases must still be dropped.

Store **only disposable, immutable CAS blobs** here. Writable workspace/output
payloads and namespace databases must remain separate. `output-core` supports
`Tree::open_with_cache` to reuse content without sharing writable authority.
See [cache ownership and eviction policy](../../docs/architecture.md).
