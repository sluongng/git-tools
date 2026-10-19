# Asynchronous workspace CAS sync

The proxy can publish Git blob contents to a BLAKE3 REAPI CAS while agents start
working. Use the same cache endpoint, instance and authorization namespace as
Bazel. The VFS still owns a private writable overlay per agent; r2c2 handles
external repositories, while this uploader handles main-workspace Git blobs.

## Configure

Have the host's secret manager provide `AGENT_CAS_KEY`. Pass its **variable name**,
not the key value. Start a dedicated proxy owner:

```sh
target/release/git-proxy --cache-dir /var/tmp/git-proxy \
  --repo bazel=https://github.com/bazelbuild/bazel.git --enable-blake3 \
  --cas-endpoint https://CACHE_HOST \
  --cas-instance-name INSTANCE \
  --cas-header-env x-buildbuddy-api-key=AGENT_CAS_KEY \
  --cas-remote-ttl-seconds 10800
```

The CAS endpoint is an HTTP(S) **origin**, equivalent to Bazel's `grpc(s)` cache
address. This does not configure a remote executor or expose a new CAS server.
Only explicitly selected environment variables supply credentials; their values
are not logged, persisted in Git, or inherited by native Git subprocesses.
Do not expose the unauthenticated Git/metadata frontend to untrusted clients.

With CAS configured, a BLAKE3 `/objects` request automatically queues its blob
IDs. `git-vfs warm-blake3` therefore starts publication without waiting for it.
Git/mapping requests do not await CAS uploads; queue saturation leaves normal
Git and digest lookup working. Background work can still compete for CPU, disk,
and Git repository locks. Warm a shared proxy before launching a burst of agents.

To queue selected blob IDs directly, without waiting for mapping generation:

```sh
curl --fail-with-body http://127.0.0.1:8082/api/v1/repos/bazel/cas-sync \
  -H 'Content-Type: application/json' \
  -d '{"oids":["FULL_40_HEX_BLOB_ID"],"force":false}'
curl --fail-with-body http://127.0.0.1:8082/api/v1/cache/stats
```

Requests permit up to 16,384 IDs / 2 MiB. A **202 means queued, not uploaded**.
`cas_sync` stats expose accepted/completed/failed/cancelled jobs, pending objects,
TTL hits, confirmed objects, uploads, size-limit omissions and transport calls.
`last_error` is a fixed stage label, not server-supplied error text. A failed job
may have successfully published earlier batches; it is not a successful full
sync. `force:true` bypasses presence observations for the requested objects;
it is rejected while those IDs are already pending. Reissue after completion
when repairing a known remote loss.

## Upload policy

The ordinary-blob batching heuristic follows BuildBuddy Executor:

| Operation | Policy |
|---|---|
| FindMissingBlobs | At most 1,000 digests per request |
| Small uploads | Sum actual payload bytes up to 2 MiB; flush before overflow and at drain |
| Large uploads | Raw size >2 MiB uses ByteStream, in 256,000-byte frames |
| Advertised limits | Reduce batch limit and respect serialized gRPC-message bounds |

Identity compression is currently used; ZSTD and parallel upload tuning are not
implemented. Uploads are serialized. Payload accounting therefore equals raw
size; this is the same batching rule, not identical compression or concurrency
settings to the Executor. Lookup failures fail the background job rather than
assuming presence or uploading the entire uncertain batch.

GetCapabilities must advertise BLAKE3. For large eligible blobs, the client
negotiates FastCDC 2020 parameters and unary SpliceBlob support. It uses the
advertised seed, normalization2, minimum average/4 and maximum average*4; only
blobs strictly larger than average*4 are chunked. Existing chunks are reused,
missing chunks upload, and whole-blob presence is confirmed only after a valid
splice acknowledgment. A missing-chunk splice gets one recheck/repair attempt.
Unsupported CDC RPCs fall back to ordinary whole-blob uploads. Authentication,
malformed responses, and digest mismatches never count as successful uploads.

The initial client supports the Bazel-compatible **unary** CDC profile, not
streaming-only REAPI2.13 chunk registration. Capability booleans select it;
BuildBuddy can advertise API2.11 while supporting those unary RPCs. FastCDC
averages outside the supported 1KiB–4MiB range and mappings beyond 32,768 chunks
fall back to whole blobs. Chunk upload transport is not a byte-for-byte copy of
BuildBuddy's compression/parallel ByteStream policy.

## TTL and completion

Set `--cas-remote-ttl-seconds` to match Bazel's
`--experimental_remote_cache_ttl` (default **10800 seconds / 3h**). REAPI does
**not** negotiate this TTL. It must be slightly below the cache deployment's
actual minimum retention guarantee; it is not a promise against early eviction.

Positive observations are scoped to this process's configured endpoint,
instance, credentials, BLAKE3 hash and raw size. They expire from **request start**,
not response arrival. Within TTL, sync skips FindMissing; after expiry it checks
again before reading/uploading content. Restart conservatively rechecks.
Mapping entries are persistent; remote-presence observations are not.

Only a valid FindMissing response omitting a digest or a fully validated upload
acknowledgment records presence. Queue acceptance, mapping generation, errors,
and chunk upload before splice do not. All per-blob batch statuses and stream
committed sizes are checked. Git content is verified against its mapped BLAKE3
hash before upload. Bazel still performs its own missing-input checks, uploads
edited/new files and directory/action protos, and repairs cache misses normally.
Uploading a symlink's Git target-text blob is harmless but does not replace
REAPI's symlink metadata.

## Bounds and shutdown

The process queues at most eight jobs and 32,768 pending IDs; observations are
capped at 65,536 digests. A single worker stages at most 64MiB by default under
`cas-spool/`, flushing before exceeding that budget. Set
`--cas-staging-limit-bytes` to change it. Larger individual blobs are omitted
from this optimization and fail the sync job; Bazel can upload them normally.
Temporary files are removed on completion, failure or cancellation. Crashed
processes can leave staging directories; clean those only with the proxy stopped.

RPC deadlines cover response bodies as well as connection/header waits. Ctrl-C
stops HTTP and cancels/joins the CAS worker, killing in-flight native Git process
groups. Pending work is best-effort, not a durable queue: restart requires
requeueing. Embedders must use `router_with_cas_shutdown` and retain its owner
handle; the legacy `router` rejects CAS-enabled configurations. Dropping the
owner also cancels work.

## Agent startup and evidence

Use [the cloud-agent skill](../examples/skills/cloud-bazel-agent/SKILL.md).
Bazel's `--experimental_remote_repo_contents_cache`, `--digest_function=BLAKE3`
and `--unix_digest_hash_attribute_name=user.blake3` are **startup** options.
Cache/executor/instance/TTL settings are command options. R2c2 uses the ordinary
remote AC/CAS configuration, not a separate dependency-cache URL. Its first miss
may still evaluate a repository rule and upload synchronously.

For a fair first-build benchmark, use private output bases and local repository
contents caches, count actual remote actions/cache hits, and record proxy/CAS
prewarming separately. Disabling `--remote_accept_cached` also disables r2c2
reads in Bazel9.2; selective action `no-remote-cache` requirements are needed to
force execution without disabling dependency reuse.

Sources: [BuildBuddy uploader](https://github.com/buildbuddy-io/buildbuddy/blob/171043b63cb257653511c3ece31c7be62c7ab316/server/remote_cache/cachetools/cachetools.go),
[Executor presence batches](https://github.com/buildbuddy-io/buildbuddy/blob/171043b63cb257653511c3ece31c7be62c7ab316/server/cache/dirtools/dirtools.go),
[REAPI schema and pins](../crates/git-reapi/proto/SOURCES.md),
[Bazel TTL](https://github.com/bazelbuild/bazel/blob/9.2.0/src/main/java/com/google/devtools/build/lib/remote/options/CommonRemoteOptions.java).
