# Background workspace CAS sync

The proxy can publish Git blob content to a **BLAKE3 REAPI CAS** while agents
start. Use Bazel's same endpoint, instance and authorization namespace. This is
main-workspace content; r2c2 handles external repositories separately.

## Enable

Supply the selected secret through `AGENT_CAS_KEY`; pass its name, not value:

```sh
git-proxy --cache-dir /var/tmp/git-proxy \
  --repo bazel=https://github.com/bazelbuild/bazel.git --enable-blake3 \
  --cas-endpoint https://CACHE_HOST --cas-instance-name INSTANCE \
  --cas-header-env x-buildbuddy-api-key=AGENT_CAS_KEY \
  --config /etc/git-tools/reapi.toml
```

The endpoint is an HTTP(S) origin equivalent to Bazel's grpc(s) cache address.
The [shared TOML example](../examples/reapi-policy.toml) controls deadlines,
retries and presence hints; see [policy](reapi-policy.md). No config discovery,
environment expansion or credentials in TOML. Native Git children do not inherit
selected CAS credentials. The unauthenticated Git frontend remains trusted-local.

BLAKE3 `/objects` requests automatically queue blob publication; `warm-blake3`
therefore initiates work without awaiting uploads. Queue saturation does not break
ordinary Git/mapping requests. Prewarm before an agent burst to avoid contention.

To queue IDs explicitly:

```sh
curl --fail-with-body http://127.0.0.1:8082/api/v1/repos/bazel/cas-sync \
  -H 'Content-Type: application/json' \
  -d '{"oids":["FULL_40_HEX_BLOB_ID"],"force":false}'
curl --fail-with-body http://127.0.0.1:8082/api/v1/cache/stats
```

Limits: 16,384 IDs / 2MiB per request. **202 means queued, not uploaded.**
Stats distinguish accepted/completed/failed/cancelled work, omissions, confirmed
objects and transport calls. Jobs can partially succeed before failing.
`force:true` rechecks known remote loss; it is rejected for already-pending IDs.

## Upload and reuse

| Operation | Policy |
| --- | --- |
| FindMissingBlobs | Up to 1,000 digests/request |
| Small upload batches | Up to 2MiB decoded bytes, flush before overflow/drain |
| Larger blobs | ByteStream; honor advertised serialized-message limits |
| Compression | Negotiate batch/stream ZSTD separately; metadata accepts gzip |
| CDC | Negotiate FastCDC 2020 + unary SpliceBlob; otherwise upload whole blob |

These follow BuildBuddy Executor's batching heuristic, not its complete parallel
upload policy. Uploads are serialized. CDC uses advertised seed/normalization2,
average/4 minimum and average*4 maximum, for blobs larger than average*4.
Supported averages: 1KiB–4MiB; at most 32,768 chunks. Missing-chunk splice gets
one recheck/repair; unsupported RPCs fall back, not authentication/corruption.
Streaming-only REAPI chunk registration is not implemented.

Git-extracted content is BLAKE3-verified in `cas-content-v1/` using
[`local-cas`](../crates/local-cas/README.md). Upload/CDC/retry readers hold leases;
LRU GC cannot unlink their bytes. `--cas-cache-max-bytes` defaults to 4GiB and must
cover `--cas-staging-limit-bytes` (64MiB default). Larger individual blobs are
omitted and fail this optimization's job; Bazel can upload them normally.
Retained bytes can repair later remote eviction without another Git extraction,
but a local hit **never replaces FindMissingBlobs or a successful upload**.
This quota does not cap mirror/pack-response storage.

Digests, quota and `uploaded_bytes` describe decoded content.
`uploaded_wire_bytes` is emitted application payload, not HTTP/protobuf traffic.
Tiny ZSTD frames can exceed identity size; already-compressed payload RPCs do not
add gRPC compression. Unary CDC capability flags matter, not only API version.

## Presence, deadlines and shutdown

Positive suppression defaults to **zero TTL**: recheck authoritative CAS state.
A nonzero `presence_ttl_seconds` is advisory, not a lease or negotiated retention.
Only use it with understood backend retention/repair; local hits do not extend it.
Hints are process-local, scoped to endpoint/instance/auth/digest/size, and expire
from request start. Restart rechecks. Mapping records persist separately.
Bazel still checks missing inputs and uploads edits, directories and action protos.
A Git symlink blob does not replace REAPI symlink metadata.

Retries retain the original operation budget and partial batch successes. Stream
retries reopen immutable content at offset zero with a new upload ID. Foreground
and background budgets are separate; progress-based stream deadlines do not mean
an unlimited job. Authentication, malformed acknowledgements and integrity errors
never establish presence. See [the complete policy](reapi-policy.md).

Queue: 8 jobs, 32,768 pending IDs, 65,536 presence observations. Pending work is
best-effort, not durable: requeue after restart. Ctrl-C stops HTTP and cancels/joins
workers and native Git process groups; cache ownership outlives admitted work.
Embedders retain `router_with_cas_shutdown`'s owner and drain it; legacy `router`
rejects CAS configuration. Do not manually clean active cache directories.

## Agent integration

Use the [cloud-agent skill](../examples/skills/cloud-bazel-agent/SKILL.md).
Bazel's r2c2/digest/xattr settings are startup options; executor/cache/instance
settings are command options. Configure them independently of this daemon.
A successful upload is useful preparation, not proof of a faster first build;
see [measurements](benchmarks.md) and [reproduction](../tools/README.md).

Protocol/source provenance: [REAPI pins](../crates/reapi/proto/SOURCES.md),
[BuildBuddy uploader](https://github.com/buildbuddy-io/buildbuddy/blob/171043b63cb257653511c3ece31c7be62c7ab316/server/remote_cache/cachetools/cachetools.go),
[Executor presence batching](https://github.com/buildbuddy-io/buildbuddy/blob/171043b63cb257653511c3ece31c7be62c7ab316/server/cache/dirtools/dirtools.go).
