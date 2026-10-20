# Local CAS eviction and reuse

The cache stores disposable, verified SHA256/BLAKE3 bytes. Namespace databases,
Git objects and writable files have different ownership rules and remain outside
it. One process owns each directory; consumers share an `Arc<LocalCas>`.

## Lessons applied

| Source | Lesson | Local CAS decision |
| --- | --- | --- |
| EdenFS object cache | Maintain total bytes and a recency queue; do not scan all entries for every victim | Maintain resident bytes and an ordered index of unpinned entries |
| Buck2 deferred materializer | Update recency in memory immediately and coalesce persisted access times | Atomic recency checkpoints, not payload-mtime updates or per-hit fsync |
| Buck2 cleanup | Invalidate/version cleanup work before asynchronous deletion | Keep unlink serialized until an explicit retiring state exists |

Eden's cache is in memory and its interest handles are not hard pins. Our file
leases must be stronger: a leased path cannot disappear during an upload or read.
Recency hints never establish content integrity or remote CAS presence.

Sources: [EdenFS object cache](https://github.com/facebook/sapling/blob/f787ffaceddd5a350212542fbe676702360f694a/eden/fs/store/ObjectCache-inl.h),
[Buck2 access-time batching](https://github.com/facebook/buck2/blob/5cb30436e4070ecf0770fef256e157fd467ec734/app/buck2_execute_impl/src/materializers/deferred/command_processor.rs),
and [versioned cleanup](https://github.com/facebook/buck2/blob/5cb30436e4070ecf0770fef256e157fd467ec734/app/buck2_execute_impl/src/materializers/deferred/clean_stale.rs).

## Policy and safety

- Quota covers resident payload bytes plus reservations for admitted downloads.
  Publication converts the reservation to resident bytes atomically.
- Only unpinned entries are eviction candidates. Admission fails if leases and
  active downloads leave insufficient room; authoritative files are never GC'd.
- Ordered victim selection replaces the previous O(entries × victims) search
  with O(victims × log(entries)). Index maintenance adds work to lease transitions.
- Recency checkpoints are optimization metadata. Missing, stale or corrupt hints
  must not bypass payload verification. Restart still verifies retained content.
- Checkpoint writers must serialize; an older snapshot cannot replace a newer
  one. Checkpoint work must finish before dropping cache ownership, with no
  autonomous worker prolonging the directory lock.
- The quota is a logical-payload budget, not a hard bound on filesystem allocation:
  metadata, recency checkpoints and allocation-block overhead are additional.
- Deletion remains serialized with lookup/pinning. Efficient victim selection
  removes repeated scans, but slow filesystem unlink operations can still delay
  other cache operations. Background deletion needs tombstone/generation state.

## Reuse boundaries

`output-fuse`, `output-nfs` and `output-projfs` already use the cache through
`output-core`. They must not each open the same directory independently.

The next consumer is proxy CAS-upload staging. Missing remote blobs can be
extracted from Git into a verified leased cache entry, retained across retries
and later republication. Upload/CDC/retry code holds the lease until completion;
it never unlinks a cache-owned path. Cache hits do not suppress remote presence
checks. The proxy and BOS remain separate owners with separate directories.

The source scheduler is not a direct replacement candidate: it supports Git-SHA1
identity, range reads and a cache-pressure bypass. Requiring BLAKE3 mappings and
whole-blob disk admission would change ordinary Git-read behavior. Generated
pack responses also have request/ref-generation identities, not raw CAS digests.

## Benchmark protocol

Run the same optimized public-API driver against frozen baseline and candidate
libraries with fresh directories. Use deterministic local bytes, no network,
retained OS page cache and three repetitions. Separate fixture generation and
population from measured operations.

Measure warm hits, eight concurrent readers, quota churn, a large incoming blob
that evicts many small files, and restart verification at 1k/10k/30k entries.
Check source-fetch counts, lease survival, logical quota and temporary-file leaks.
Include warm-hit overhead and restart/checkpoint cost, not only the best eviction
case. These component measurements do not establish a BuildBuddy build speedup.

Harness: [`cache_benchmark`](../../crates/local-cas/examples/cache_benchmark.rs).
Receipts and the private comparison report are kept outside Git.

```sh
bazel run --config=rbe //crates/local-cas:cache_benchmark -- /tmp/fresh-cas-bench candidate
```
