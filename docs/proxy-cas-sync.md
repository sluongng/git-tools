# Background workspace CAS sync

The Git proxy can publish workspace blobs to a **BLAKE3 REAPI CAS** while agents
start. Match Bazel's endpoint, instance and authorization scope. This prepares main
workspace content; remote repository-content caching (r2c2) handles external repos.

## Enable

Provide the selected key through `AGENT_CAS_KEY`; pass its name, never its value:

```sh
git-proxy --cache-dir /var/tmp/git-proxy \
  --repo project=https://github.com/bazelbuild/bazel.git --enable-blake3 \
  --cas-endpoint https://CACHE_HOST --cas-instance-name INSTANCE \
  --cas-header-env x-buildbuddy-api-key=AGENT_CAS_KEY \
  --config /path/to/reapi-policy.toml
```

Use an HTTP(S) origin equivalent to Bazel's grpc(s) cache address. The
[policy TOML](../examples/reapi-policy.toml) configures [timeouts and retries](reapi-policy.md).
Credentials stay outside TOML/argv and are not inherited by native Git children.
The Git frontend remains trusted-local and unauthenticated.

BLAKE3 object-mapping requests queue publication automatically; `warm-blake3`
initiates work without waiting for uploads. To queue explicit IDs:

```sh
curl --fail-with-body http://127.0.0.1:8082/api/v1/repos/project/cas-sync \
  -H 'Content-Type: application/json' \
  -d '{"oids":["FULL_40_HEX_BLOB_ID"],"force":false}'
curl --fail-with-body http://127.0.0.1:8082/api/v1/cache/stats
```

**202 means queued, not uploaded.** Jobs may partially succeed; inspect confirmed,
failed and omitted-object counters. `force:true` rechecks known remote loss but
rejects IDs already pending. Queue saturation does not fail ordinary Git requests.

## Upload and storage behavior

Presence checks batch up to 1,000 digests. Small uploads batch up to 2MiB decoded
bytes; larger blobs use ByteStream. ZSTD and optional FastCDC/unary SpliceBlob are
negotiated independently, with whole-blob fallback for unsupported features.
Uploads are serialized; streaming-only chunk registration is unsupported.

Verified bytes use [local-cas](../crates/local-cas/README.md). Upload/retry readers
hold leases that exclude active bytes from LRU eviction. The default 4GiB
`--cas-cache-max-bytes` must cover the 64MiB default `--cas-staging-limit-bytes`.
Larger individual blobs are omitted from this optimization; Bazel can upload them
normally. This quota excludes Git mirrors and generated response packs.

Presence suppression defaults to zero. Any positive TTL is advisory, not a remote
retention lease; local hits do not extend it. Only authoritative presence or a
successful upload confirms availability. Digests persist separately from these
process-local hints. Bazel still uploads edits, directory/action protos and any
missing inputs; Git symlink bytes do not replace REAPI symlink metadata.

Queued jobs are best-effort, **not durable**: requeue after restart. Shutdown
cancels and joins workers; do not clean active cache directories. Embedders must
retain and drain the owner returned by `router_with_cas_shutdown`; legacy `router`
rejects CAS configuration. See the [agent workflow](../examples/skills/cloud-bazel-agent/SKILL.md).
