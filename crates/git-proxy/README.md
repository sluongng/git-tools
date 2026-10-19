# git-proxy

A bounded, single-machine Rust prototype: a read-only pull-through smart HTTP Git
proxy backed by native Git bare mirrors, with batch object metadata and optional
persistent Git SHA1 → REAPI BLAKE3 translation. Fjall is the only database.

```sh
cargo run -p git-proxy -- \
  --cache-dir /var/tmp/git-proxy \
  --git /path/to/git \
  --repo bazel=https://bazel.googlesource.com/bazel \
  --enable-blake3

git clone --filter=blob:none --no-checkout \
  http://127.0.0.1:8082/git/bazel.git
```

`--repo ALIAS=UPSTREAM` is repeatable. Aliases contain only ASCII letters, digits,
`-` and `_`; clients cannot supply an upstream URL or filesystem path. Cache
ownership is enforced by a nonblocking OS file lock, including across processes.
Changing an existing alias's upstream is rejected rather than mixing objects or
serving a previous repository under a new identity.

## Cache and consistency

The first request clones a **full bare mirror**, without checkout, into a temporary
directory and atomically renames it into place. Concurrent requests share one
in-process repository lock. This does **not** reduce first-touch upstream transfer
compared to a full mirror; the benefit is sharing subsequent reads and providing
server-side metadata. Generated fetch responses have a separate bounded cache
described below. Bare mirrors themselves have no eviction or garbage-collection
policy; distributed replication and repository deduplication are not implemented.

Advertisements atomically refresh all refs and update symbolic HEAD after `--refresh-ttl-seconds` (default 10).
Objects are fetched before refs become visible by Git's normal fetch transaction.
Refresh failures fail that request rather than silently advertising stale refs.
Requests inside the TTL can use an offline origin. TTLs reset after process
restart; a fresh advertisement then contacts origin. The upstream default branch is queried explicitly after each successful refresh.

The only Git endpoints are `GET /git/ALIAS.git/info/refs?service=git-upload-pack`
and `POST /git/ALIAS.git/git-upload-pack`. No pushes or dumb HTTP filesystem
reads are exposed. `Git-Protocol` is forwarded to native `git http-backend` as
`GIT_PROTOCOL`. Native Git implements negotiation, shallow clones, filtering,
object-info, pack production, and backpressure. Response packs are streamed in
64 KiB chunks, not accumulated in Rust memory. Missing full-SHA1 `want` requests
trigger an object-only upstream fetch with ref-preserving flags, then native Git
serves the request. This permits pinned commits no longer present in current refs
when upstream policy permits fetching them.

## Generated fetch-response cache

The Packhorse-inspired cache reuses **complete generated Git responses**, not
arbitrary POST bodies or individual on-disk Git packs. Eligible protocol-v2 fetches
with full object-ID wants and a final `done` share a producer. The first client
streams the native response while it is written to a temporary file; matching
concurrent clients wait for successful completion, then read the published file.
Later requests avoid `git http-backend` and pack generation entirely.

Keys include the repository identity and refresh generation, protocol capabilities,
and exact fetch arguments, including wants, haves, shallow/deepen state, filters,
and pack options. Only top-level agent/session IDs are ignored. Unknown extensions,
intermediate negotiation rounds, ref-name wants, and packfile-URI requests bypass
the cache. Refresh attempts invalidate prior generations, including when fetched
refs changed but the subsequent HEAD refresh failed.

Admission requires HTTP 200, Git's upload-pack content type, successful native Git
exit, complete packet framing and pack section, and no protocol error. This framing
validation is not an independent pack checksum verification. Partial/error responses
are never published. A disconnected first client can leave a useful fill running;
the fill retains its HTTP slot and has a 600-second absolute deadline, including
time blocked by that client's backpressure. Oversize responses, disk errors, and
quota exhaustion fall back to uncached serving without truncating a valid response.
Followers of a failed fill generate their own uncached response.

| Option | Default | Meaning |
| --- | --- | --- |
| `--pack-cache-max-bytes` | 1 GiB | Completed files plus reserved in-flight capacity; `0` disables caching. |
| `--pack-cache-max-entry-bytes` | 64 MiB | Maximum response size, reserved before generation. |
| `--pack-cache-max-entries` | 1,024 | Completed entries plus in-flight producers; `0` disables caching. |
| `--pack-cache-ttl-seconds` | 300 | Completed-entry lifetime. |

LRU eviction and TTL expiry skip files pinned by active readers. The index is
process-local: restart discards generated-response files in the cache's own schema
directory. Mirrors and Fjall digest mappings persist. The process owner lock also
covers detached fills and readers. This is the local cache/coalescing part of the
Packhorse approach; object-storage offload via `packfile-uris` and live fan-out to
followers are not implemented.

Responses expose `X-Git-Proxy-Pack-Cache: MISS`, `HIT`, `COALESCED`, or `BYPASS`.
`GET /api/v1/cache/stats` returns JSON; `GET /metrics` exposes Prometheus counters
and gauges. These include hits, misses, coalesced requests, capacity/disabled-cache
bypasses, bytes generated/served, evictions, errors, entries, reserved/completed
bytes, in-flight fills, native HTTP backend calls, and eligible fetch generations.
Unrecognized/non-fetch requests bypass without entering these cache counters.

The mirrors set `uploadpack.allowFilter=true`,
`uploadpack.allowAnySHA1InWant=true`, and `transfer.advertiseObjectInfo=true`.
Object-info depends on a native Git version implementing that extension; use the
same Git build as git-vfs's `remote-object-info` implementation. The service does
not reimplement this wire extension in Rust. The JSON API below works independently
of whether the selected Git advertises object-info.

## Batch object metadata API

```sh
curl -s http://127.0.0.1:8082/api/v1/repos/bazel/objects \
  -H 'Content-Type: application/json' \
  -d '{"oids":["0123456789012345678901234567890123456789"],"blake3":true}'
```

```json
{"objects":[{"oid":"0123456789012345678901234567890123456789","size_bytes":123,"object_type":"blob","blake3":{"hash":"64 lowercase hexadecimal characters","size_bytes":123}}]}
```

At most 16,384 full 40-hex SHA1 IDs per request. IDs are canonicalized to lowercase,
deduplicated, and returned in first-occurrence order. Unknown fields or invalid
IDs return 400; a missing object returns 404 for the entire batch, never a partial
success. Objects absent locally are requested from the configured upstream.
Sizes describe the **raw Git object content**, excluding Git's object header.
For LFS/annex pointers, that means the pointer blob, not external payload content.

BLAKE3 must be explicitly enabled on the server (`--enable-blake3`) and requested
with `"blake3":true`. Only blobs receive a digest; trees/commits/tags return null.
Hashing reads raw blob content through `git cat-file`, bounded to 64 KiB at a time,
with no Git header. The result is the REAPI BLAKE3 digest `(hash, size_bytes)`;
it does not upload anything to a CAS or prove CAS presence. Fjall stores raw
20-byte Git SHA1 keys and 40-byte values (32-byte BLAKE3 plus big-endian u64 size),
using synchronous durable commits. Persisted mappings are reused after restart.

## Operational boundary

Default listen address is **127.0.0.1:8082**. This is a trusted-local prototype,
not a multi-tenant or authenticated hosting service. Each configured upstream is
trusted and all its cached objects are readable by frontend clients, including
objects not reachable from advertised refs. No client credentials or authorization
headers are forwarded. Origin access uses the server Git process's own configured
transport identity; interactive credential prompts are disabled and global/system
Git config is intentionally not read. SSH configuration remains server-local.
Do not expose a private upstream cache to untrusted clients.

Limits: 16 active HTTP requests, 2 MiB buffered request bodies, 16,384 IDs/wants, 30-second request-body deadline,
32 KiB CGI headers (8 KiB per line), 30-second CGI header timeout, and 600-second
Git operation/stream deadlines. Gzip request bodies are decoded with the same 2 MiB limit.
Cache-mutating operations are serialized per repo;
read requests can run concurrently. Child process groups are killed on cancellation,
timeout, or response disconnect, except for bounded shared cache fills described above. Origin errors return 502 with details confined to
server logs. Stream deadlines are checked when the body is polled. A stalled client under
backpressure can retain its slot until polling resumes or it disconnects;
there is no reverse-proxy-grade rate limiting, mirror disk quota, retry policy, or TLS.

## Inspiration and scope

[Canva Goblet](https://github.com/canva-public/goblet) and
[Cursor Origin](https://cursor.com/blog/git-at-any-scale) motivate keeping native
Git's object/pack machinery while separating cache management from request serving.
[Datadog Gitretriever](https://www.datadoghq.com/blog/engineering/gitretriever/) illustrates reusable
server-side source retrieval for tooling.
[GitLab Packhorse](https://gitlab.com/gitlab-org/packhorse) motivates semantic
fetch-response caching and concurrent-request coalescing. This is not an implementation or
performance claim for any of those systems: it intentionally starts with ordinary
local bare mirrors, explicit freshness, and an optional digest index, not their
distributed storage architectures.

## Validation

`cargo test -p git-proxy` runs local-only integration tests: actual smart HTTP
blobless shallow clone and native object-info query, concurrent first touch,
owner-lock rejection, pinned-object fetch without ref mutation, offline cache reuse within TTL, ref and default-branch refresh, metadata
validation/missing objects, push/dumb-read rejection, persistent BLAKE3 results,
and correctness in the presence of Git replacement refs. Cache tests cover real
clone reuse, filter isolation, decoded gzip equivalence, concurrent coalescing,
oversize fallback, refresh-failure invalidation, malformed/error responses,
deadline/disconnect behavior, reader pinning, quotas, LRU/TTL, and disk failure.
Set `GIT_PROXY_TEST_GIT` to override the native Git executable used by the tests.
