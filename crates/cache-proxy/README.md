# cache-proxy

Private Unix-socket REAPI cache, scheduler and Remote Asset proxy for the workspace daemon, with optional
bearer-authenticated loopback TCP. Share one
`Arc<local_cas::LocalCas>` with source/output services; `Upstream` supplies verified
CAS hydration. The daemon owns listener, credentials, cache scope and shutdown.

- SHA256/BLAKE3; identity/zstd blob transport; gzip gRPC; upstream CDC forwarding.
- Reads reuse locally verified blobs. Writes are verified and committed upstream
  before success. `FindMissingBlobs` always checks upstream, not local residence,
  so direct-to-BuildBuddy execution cannot mistake local-only inputs for uploads.
- Embedded action-result caching defaults to **off** (the workspace daemon selects
  a short configurable TTL). Configure a positive TTL only with
  an upstream retention guarantee; entries include inline request options and
  are bounded by count and 64 MiB. Blob eviction uses shared CAS policy.
- ByteStream uploads stage bounded temporary files (four admitted uploads), then
  verify and forward. Incomplete uploads restart at offset zero; staging is not
  a persistent resumable-upload service. Completed status is forwarded upstream.

Execution streams preserve caller deadlines without cache-derived idle timeouts
or hidden retries. Operations are forwarded unchanged, including `UNIMPLEMENTED`.
Bounded routing metadata is forwarded; upstream credentials remain daemon-owned.
Downloader mapping hits require verified local CAS bytes and upstream presence;
metadata has fixed TTL/count/byte bounds and LRU eviction. No blob leases are
retained. Freshness-constrained requests and directory requests always forward.
Optional retention maintenance uses one separate background connection for used
AC entries and shared-CAS reads. Renewal validates output references; failures
never extend validity. Local blob leases and foreground presence semantics are
unchanged. See [policy](../../docs/reapi-policy.md#background-retention-maintenance).

Unix sockets must live in a private per-user directory; their filesystem
permissions replace bearer authentication. Configure a local bearer token for
TCP when embedding; the daemon generates one. Do not
share a daemon or backend identity across authorization boundaries. Upstream
headers are daemon-owned and incoming headers never select upstream credentials.

Bazel Unix-socket clients must include the [Unix authority fix](https://github.com/bazelbuild/bazel/commit/242563f1a14378801725e5f6b0ee397ef0d307f2). Older clients can fail before RPC dispatch with HTTP/2 `PROTOCOL_ERROR`; their socket path is incorrectly sent as the HTTP authority.

Forwarded RPCs preserve only the bounded standard REAPI request-attribution
header; upstream credentials remain daemon-owned. Shared/coalesced CAS hydration
is daemon-owned, without per-invocation attribution. RPC concurrency defaults to
128 per connection, independently of eight CAS fetches and four staged uploads;
HTTP/2 receive windows are bounded to prevent queued uploads starving writers.
