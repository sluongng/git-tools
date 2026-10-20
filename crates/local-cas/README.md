# Local CAS

Reusable verified SHA256/BLAKE3 disk cache. Open a directory once with
`LocalCas::open(path, source, config)` and share its `Arc<LocalCas>` across
consumers. `BlobSource` supplies remote bytes; `content(artifact).await` returns
an `Arc<CachedBlob>` lease whose path is safe to read until the lease is dropped.

- Exact hash/size verification and atomic publication; retained bytes are
  reverified after restart.
- Byte-bounded LRU eviction skips active leases and reserves in-flight bytes.
- Concurrent reads coalesce; a cancelled/timed-out waiter does not cancel another
  reader's admitted fetch. Concurrency, byte and timeout budgets are configurable.
- Keys include backend authorization scope, instance, digest function, hash and
  size. Callers must give distinct backend identities to distinct auth scopes.
- One owner holds the cache directory lock, including outstanding leases and
  verification work. Independent processes need separate directories; this is a
  shared library/handle, not a multi-process cache daemon. LRU recency is
  process-local and is not restored after restart.

Store **only disposable, immutable CAS blobs** here. Writable workspace/output
payloads and namespace databases must remain separate. `output-core` supports
`Tree::open_with_cache` to reuse content without sharing writable authority.
