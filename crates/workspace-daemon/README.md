# workspace

The main CLI and host daemon: private source sessions, Git proxy, shared local CAS,
REAPI cache/execution/downloader proxy, durable BES relay and optional lazy BOS.
Each session retains its own edits, Git state and Bazel output base.

```sh
workspace init
# Edit ~/.config/workspace/config.toml; provision credentials outside Git.
workspace create /path/to/agent-1 --repo bazel --revision FULL_COMMIT_SHA
workspace test /path/to/agent-1 -- //...
workspace save /path/to/agent-1 -- -m 'Describe the staged change'
workspace stop /path/to/agent-1
workspace resume /path/to/agent-1
workspace status
```

`create` starts the daemon automatically; `workspace serve` runs in the foreground.
Use `--config /path/to/private/config.toml` for an isolated trust scope.
Stage intended changes with Git before `save`; commits and edits are never
cache-eviction candidates. Keep the state directory across restarts.

The [TOML example](../../examples/workspace.toml) documents defaults and limits.
BOS is opt-in and strictly lazy. Cache, execution, Remote Asset, BES and BOS share
`rbe.sock`; CLI control uses `control.sock`. Bazel needs the
[Unix authority fix](https://github.com/bazelbuild/bazel/commit/242563f1a14378801725e5f6b0ee397ef0d307f2)
and required BLAKE3/xattr/r2c2 support. Select a compatible client with `session.bazel`.

BES ACK means durable local acceptance, not upstream delivery. Pending events are
never LRU-evicted; inspect `status` and use `retry-bes` after a forwarding failure.
AC validity and best-effort background retention are configurable assumptions;
see [policy](../../docs/reapi-policy.md). Git packs/metadata are outside the CAS quota.

Stop sessions before `workspace shutdown`. After a crash, preserve state and use
`workspace recover SESSION` before `resume`; unverified mounts require manual
inspection. `workspace recover` without a session targets the stale output mount.
Unified source-session lifecycle is currently Linux-only; native NFS/ProjFS
adapters have separate [platform setup](../../docs/platforms.md).
