# git-proxy

Read-only Git smart-HTTP pull-through cache backed by native Git bare mirrors.
It shares generated fetch responses and serves batch object sizes plus optional
persistent Git SHA1 → BLAKE3 content digests. Fjall is the only database.

## Run

From the project root:

```sh
cargo build --release -p git-proxy
target/release/git-proxy --cache-dir /var/tmp/git-proxy \
  --repo bazel=https://github.com/bazelbuild/bazel.git --enable-blake3

git clone --depth 1 --filter=blob:none --no-checkout \
  http://127.0.0.1:8082/git/bazel.git
```

`--repo ALIAS=UPSTREAM` is repeatable; clients cannot choose arbitrary upstreams.
Use `--git /path/to/git` to select native Git. Cache directories have one owner;
changing an existing alias's upstream is rejected.

## Cache behavior

The first request creates a **full all-refs bare mirror**, even for shallow
clients. Initial population can be slower than direct cloning; later clients
share it. Mirrors have no disk quota, eviction, or garbage-collection policy.

Advertisements refresh refs after `--refresh-ttl-seconds` (default 10).
Refresh failures fail the request; restart resets freshness and contacts origin.
Native Git handles negotiation, filtering, shallow clones, and pack production.
Native `object-info` requires a Git build implementing that extension; the JSON
metadata API works independently of it.

Eligible complete protocol-v2 fetch responses share a Packhorse-inspired cache.
The producer streams while filling; matching followers wait for completion.
Unknown requests and intermediate negotiation bypass it; partial/error responses
are never cached. Restart discards response files, not mirrors/digest mappings.

| Option | Default |
|---|---:|
| `--pack-cache-max-bytes` | 1 GiB |
| `--pack-cache-max-entry-bytes` | 64 MiB |
| `--pack-cache-max-entries` | 1,024 |
| `--pack-cache-ttl-seconds` | 300 |

A zero byte/entry quota disables response caching. TTL/LRU eviction skips active
readers. See [cache admission and consistency](../../docs/proxy-reference.md#generated-fetch-response-cache)
for keying, deadlines, coalescing, and fallback guarantees.

## Object metadata and monitoring

```sh
curl -s http://127.0.0.1:8082/api/v1/repos/bazel/objects \
  -H 'Content-Type: application/json' \
  -d '{"oids":["FULL_40_HEX_OBJECT_ID"],"blake3":true}'
```

Up to 16,384 SHA1 IDs / 2 MiB request. Responses contain `oid`, `size_bytes`,
`object_type`, and optional `blake3: {hash, size_bytes}`. Missing objects fail the
whole batch. LFS/annex sizes describe pointer blobs, not external payloads.
BLAKE3 hashes raw blob content, excluding Git's header; persisted mappings survive
restart. Mapping generation alone does not prove CAS residency. Optional
[asynchronous CAS sync](../../docs/proxy-cas-sync.md) publishes raw blobs using
REAPI, negotiated FastCDC, and bounded Executor-style upload batches.
Use `--cas-endpoint` to opt in; its presence TTL must match Bazel configuration.

`GET /api/v1/cache/stats` returns cache JSON; `GET /metrics` exposes Prometheus
metrics. Fetches report `X-Git-Proxy-Pack-Cache` as `MISS`, `HIT`, `COALESCED`, or
`BYPASS`.

## Deployment boundary

Defaults to **127.0.0.1:8082**, without TLS or frontend authentication.
All cached objects, including unreachable ones, are readable by clients.
Keep private upstreams on trusted networks. No frontend credentials are forwarded;
origin access uses server-local transport identity with interactive prompts off.
No push or dumb-HTTP file serving is exposed.

- [Full reference](../../docs/proxy-reference.md): API validation, auth,
  cancellation, resource limits, durability, and tests.
- [Design and research](../../docs/proxy-design.md): Goblet, Origin,
  Gitretriever, and Packhorse.
- [Mapping benchmarks](../../docs/proxy-blake3-benchmarks.md).
- Test: `cargo test -p git-pack-cache -p git-reapi -p git-proxy`;
  select Git with `GIT_PROXY_TEST_GIT`.
