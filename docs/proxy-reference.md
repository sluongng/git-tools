# Git proxy reference

[Setup](../crates/git-proxy/README.md) · [Architecture](../README.md#architecture)

## Repository and access model

Repeat `--repo ALIAS=UPSTREAM` for allowed repositories. Aliases accept ASCII
letters, digits, `-` and `_`; clients cannot choose upstream URLs or disk paths.
Changing an alias's upstream is rejected. A nonblocking process lock gives each
cache directory one owner.

The first request creates a **full all-refs bare mirror** without checkout.
This costs a full mirror transfer before clients benefit from shared content.
Mirrors have no automatic quota or eviction. Ref advertisements refresh after
`--refresh-ttl-seconds` (default 10); refresh failure fails the request rather than
silently advertising stale refs. Requests within the TTL can use an offline origin.

This is a **trusted-local, unauthenticated** service, listening on
`127.0.0.1:8082` by default. Every frontend client can read all objects in configured
repositories, including unreachable cached objects. Origin credentials belong to
the server's native Git transport, not the client; interactive prompts are disabled
and global/system Git config is not read. Do not expose private caches to untrusted
clients. TLS, multi-tenant authorization and reverse-proxy-grade rate limits are
not provided.

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /git/ALIAS.git/info/refs?service=git-upload-pack` | Advertise refs/capabilities |
| `POST /git/ALIAS.git/git-upload-pack` | Native smart Git fetch |
| `POST /api/v1/repos/ALIAS/objects` | Batch object sizes and optional BLAKE3 mappings |
| `POST /api/v1/repos/ALIAS/cas-sync` | Queue optional [CAS publication](proxy-cas-sync.md) |
| `GET /api/v1/cache/stats`, `GET /metrics` | JSON/Prometheus cache metrics |
| `GET /api/v1/storage` | Bounded storage accounting; check `complete` and errors |

Pushes and dumb filesystem HTTP reads are rejected. Native Git handles filtering,
shallow negotiation, pack generation and object-info. A missing pinned SHA1 may
be fetched without moving mirror refs, subject to upstream policy. Protocol
object-info requires support in the selected native Git; the JSON API does not.

```sh
curl --fail-with-body http://127.0.0.1:8082/api/v1/repos/project/objects \
  -H 'Content-Type: application/json' \
  -d '{"oids":["FULL_40_HEX_OBJECT_ID"],"blake3":true}'
```

Requests accept at most 16,384 full SHA1 IDs, deduplicate them and return objects
in first-occurrence order. Invalid requests return 400; a missing object fails
the whole batch with 404. Sizes exclude Git's object header. LFS/annex pointer
sizes describe the pointer, not its external payload.

BLAKE3 requires server `--enable-blake3` and request `"blake3":true`. Only blobs
receive a digest; Fjall persists `(hash, size_bytes)` mappings across restart.
The digest hashes raw content, not Git headers. A mapping neither proves remote
CAS presence nor waits for a queued upload.

## Generated fetch-response cache

The Packhorse-inspired cache stores complete successful protocol-v2 fetch
responses. Matching requests reuse generation; concurrent followers wait for
completion. Keys include repository/ref identity and exact negotiation arguments.
Unsupported or intermediate negotiation bypasses the cache. Failed, partial and
oversize responses are not published; capacity failures fall back to native serving.

| Option | Default |
| --- | ---: |
| `--pack-cache-max-bytes` | 1GiB |
| `--pack-cache-max-entry-bytes` | 64MiB |
| `--pack-cache-max-entries` | 1,024 |
| `--pack-cache-ttl-seconds` | 300 |

Zero byte/entry capacity disables caching. TTL/LRU skip active readers. Completed
responses survive restart after validation without renewing expiry. Headers expose
`X-Git-Proxy-Pack-Cache: MISS`, `HIT`, `COALESCED` or `BYPASS`.

Mirror objects, digest metadata and old cache schemas are outside this response
quota; it is not a filesystem-wide disk limit. Remove obsolete schemas only while
unused. Storage accounting is an estimate, not exclusive physical usage.
The cache does not provide object-storage offload or `packfile-uris`.

Requests are bounded to 16 active operations and 2MiB bodies; native Git operations
have a 600s deadline. Slow clients can retain slots under backpressure. Keep free
disk space and frontend access controlled independently of these logical limits.
