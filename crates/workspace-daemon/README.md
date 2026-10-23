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
Use `--config /path/to/private/config.toml` to select configuration. Daemon reuse
requires matching service settings and credential scope. Session executable and
bazelrc choices may differ, except `session.git` also selects the daemon's Git
executable when `git_proxy.repositories` is configured. To change service settings
or rotate credentials, shut down with the original configuration and credentials
first, or select a separate `state_dir`. Selecting a different config file alone does not isolate it.
Before upgrading an active daemon across identity versions, shut it down with its
existing binary and configuration.
For a standalone Git proxy, leave `git_proxy.repositories` empty and pass
`--proxy-url http://127.0.0.1:8082` to `workspace create`. Each process must own
separate cache storage.
Stage intended changes with Git before `save`; commits and edits are never
cache-eviction candidates. Keep the state directory across restarts.

The [TOML example](../../examples/workspace.toml) documents defaults and limits.
BOS is opt-in and strictly lazy. Cache, execution, Remote Asset, BES and BOS share
`rbe.sock`; CLI control uses `control.sock`. Optional native repository aliases
also use `rbe.sock` for shared object I/O and native reference control. Configure
`native.repositories.ALIAS.remote` in the same TOML, then use
[`tinytree-jj`](../jj-native-cli) for materialized native clone/fetch/push.
Native source mounts and client GC/offloading are not yet implemented.

Bazel needs the
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
