# git-proxy

Library and read-only smart-HTTP Git proxy executable. Native bare mirrors and
cached fetch responses serve multiple clients; object metadata APIs expose
sizes and optional persistent Git SHA1 → BLAKE3 mappings.

```sh
bazel build --config=rbe //:git-proxy
bazel-bin/crates/git-proxy/git-proxy-bin --cache-dir /var/tmp/git-proxy \
  --repo bazel=https://github.com/bazelbuild/bazel.git --enable-blake3

git clone --depth 1 --filter=blob:none --no-checkout \
  http://127.0.0.1:8082/git/bazel.git
```

`--repo ALIAS=UPSTREAM` is repeatable; clients cannot select arbitrary origins.
The first request creates a **full all-refs mirror**, even for a shallow clone.
Prepare it before assigning agents with `POST /api/v1/repos/ALIAS/prepare`;
`GET /api/v1/repos/ALIAS/status` reports readiness and ref age. A usable mirror
is not necessarily fresh, and failed ref refreshes do not silently serve stale ads.

Mirrors and digest mappings persist separately from the bounded TTL/LRU fetch
response cache; that quota does not bound total filesystem usage. Mirrors and
digest mappings have no service-managed disk quota or eviction, and the proxy
does not run mirror GC. Proxy fetches disable native Git automatic maintenance.
Stop the cache owner before running offline mirror maintenance.

Cache directories have one owner; changing an existing alias's origin is
rejected. A legacy empty mirror without trustworthy HEAD provenance fails
refresh rather than guessing its default branch. With the cache owner stopped,
recreate only that disposable repository mirror to establish fresh provenance;
leave the other cache databases intact.

`POST /api/v1/repos/ALIAS/objects` accepts batched object IDs and optional BLAKE3
lookup. LFS/annex metadata describes pointer blobs, not external payloads.
Mappings do not prove remote CAS residency. Optional [CAS sync](../../docs/proxy-cas-sync.md)
publishes raw contents; its verified local CAS has a separate byte quota.
`GET /api/v1/cache/stats`, `/api/v1/storage` and `/metrics` expose usage.

The default loopback listener has **no frontend authentication or TLS**.
All cached objects, including unreachable ones, are readable by clients.
Use only within a trusted boundary. Origin access uses server-local transport
identity; frontend credentials are not forwarded. Push and dumb HTTP are unsupported.
See the [API/configuration reference](../../docs/proxy-reference.md) and `--help`.
