# workspace

One host process owns source mounts, lazy Bazel output mounts, the Git proxy,
an authenticated REAPI cache/execution/downloader proxy and a durable BuildBuddy BES relay. Each session retains its own writable overlay,
Git state and Bazel output base. Only verified immutable bytes enter the shared,
LRU-evicted CAS; edits and commits are never eviction candidates.

```sh
workspace init
# Edit ~/.config/workspace/config.toml: cache upstream, repository aliases, bazelrc.
export BUILDBUDDY_API_KEY=... # Prefer your normal secret provisioning.
workspace create /absolute/path/agent-1 --repo bazel --revision FULL_COMMIT_SHA
workspace test /absolute/path/agent-1 -- //...
workspace save /absolute/path/agent-1 -- -m 'Describe the staged change'
workspace stop /absolute/path/agent-1
workspace resume /absolute/path/agent-1
workspace status
```

`create` starts the daemon automatically. `workspace serve` runs it in the
foreground for service managers; `--config /absolute/path/config.toml` selects an
isolated trust scope. Configured origins are read-only through the proxy. Stage
changes with native Git before `save`; normal Git/JJ state stays session-private.
Select `session.bazel` explicitly when the repository pins an older client:
private Unix sockets require Bazel's [authority fix](https://github.com/bazelbuild/bazel/commit/242563f1a14378801725e5f6b0ee397ef0d307f2),
which is absent from 9.3.0rc2. BLAKE3/xattr/r2c2 support is also required.

## Defaults and boundaries

- 4 GiB shared CAS, eight concurrent fetches, 512 MiB maximum blob/in-flight size.
  Cache connections allow 128 concurrent RPCs; configure
  `cache.max_concurrent_rpcs_per_connection` within 1–1024.
- Source mounts and the shared cache are enabled by default. BOS is opt-in
  (`bos.enabled = true`) until metadata publication is batched; when enabled,
  outputs remain strictly lazy with no eager fallback. All run in this process.
- BES is enabled by default, inheriting the cache upstream and credential
  environment variable unless `[bes]` overrides them. Multiple builds upload
  independently. Bazel receives ACKs only after local durable persistence; this
  means **locally accepted**, not yet visible in BuildBuddy. Pending events stay
  outside LRU eviction; upstream ACKs delete payloads. `workspace status` shows
  the backlog and forwarding failures; `workspace retry-bes` retries blocked
  delivery without stopping mounts. Keep the state directory across restarts.
- Remote AC metadata has a fixed 60-second TTL; local hits do not renew it.
  Set `cache.action_cache_ttl_seconds = 0` without a backend-retention assumption.
- Downloader mappings have bounded TTL/LRU metadata and require verified local
  bytes plus upstream presence. Bytes share CAS eviction; freshness requests and
  directory fetches always reach upstream. Execution retries remain Bazel-owned.
- Source prefetch runs after mounting. Native Git packs and repository metadata
  have their own storage policies and are **not** covered by the CAS byte quota.
- Cache, Execution/Operations, Remote Asset, BES and optional BOS share `rbe.sock`;
  the CLI alone uses `control.sock` (no cache token in Bazel process arguments). Git uses a private capability URL. State must be owned by the
  current user with mode `0700`; status does not print credentials.
- Mount lifecycle changes are serialized; cache/download RPCs remain concurrent.
  Stop source sessions before `workspace shutdown`. After a crash, registry
  entries become `recovery_required`; bytes and edits are retained. A stale native
  mount must be repaired with `workspace recover SESSION` before `resume`.
  Recovery checks the persisted mount identity and a bounded ENOTCONN probe; it
  refuses live, replaced or unverified mounts. It also repairs the daemon's stale
  output mount while holding its exclusive lock. Use `workspace recover` without
  a session to repair only that output mount, even before any session was created.
  Old registries without mount
  identities require manual inspection/unmount; transparent restart is not promised.

The unified CLI currently hosts Linux source mounts. Reusable macOS NFS and
Windows ProjFS adapters exist separately; unified Windows control and automatic
native macOS source mounting are not yet provided by this command.
