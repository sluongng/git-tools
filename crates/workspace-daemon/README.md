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
[`tinytree-jj`](../jj-native-cli) for native clone/fetch/push through daemon-owned
lazy Linux mounts. Native client GC/offloading is not automatic.

## Explicit native collection

On Linux, opt into `[native.workspaces.gc] policy = "local_complete"` only when
initializing a genuinely fresh native cohort under `state_dir`, with exactly one
fully local repository alias, legacy index mode and capture/upload policies
disabled. Existing untracked stores cannot be upgraded this way. Reopening a managed cohort requires
its recorded policy and alias; do not disable the policy or rename the alias.

```sh
workspace --config CONFIG native-gc --store example          # Preview only.
workspace --config CONFIG native-gc --store example --apply  # Bounded sweep.
```

The preview is nonmutating, not deletion permission; apply revalidates the roots.
Only unreachable objects past the grace period are eligible. Collection is never
automatic: no reachable offloading, undo expiry or recovery-ticket retirement.
See the [TOML example](../../examples/workspace.toml) for conservative defaults.

## Retained captures

New remote native `init`/`clone` workspaces and convenience `cloud restore`
destinations on Linux use `"auto"` capture and upload policies by default. Creation requires authenticated query and captured
cloud support before it creates a destination. After each command, the daemon
queues a durable local capture; a background worker publishes it without moving
public references. Local aliases and workspaces created before this automatic
enrollment remain unchanged. Older pending restore intents and the lower-level
exact-restore compatibility route also remain unenrolled. New cloud restores
freeze their eligibility with the exact selected version before destination
creation; their required query readiness uses that retained private source,
not an ordinary public-root query. Captured index types are never converted by
later creation-policy changes.

Set capture `policy = "disabled"` to disable capture and automatic upload, or
upload `policy = "disabled"` to retain local captures without uploading them.
For explicit opt-in on existing workspaces, use capture
`"after_command_best_effort"` and upload `"publish_retaining_roots"` (or the
retirement policy below). Capture adds command-completion work; upload runs in
the background. Status distinguishes local queuing from historical publication;
neither publication nor a catalog row proves current remote availability or a
Clean transition.
Use the native CLI's [restore guide](../jj-native-cli#restore-a-retained-capture)
for an exact selected capture and scoped current-state resume.

Automatic upload for enrolled workspaces uses the same bounded retirement
rules as explicit `"publish_retiring_redundant"`, allowing an older saved local
capsule to become remote-only. `"publish_retaining_roots"` instead keeps every
local ticket and pack. Retirement requires at least two capture slots and
retains the newest capsule for each workspace. Retirement needs a freshly verified
exact remote capture, a newer durable capture for the same registration, and exact
OP/VIEW/index representation coverage in that newer capture. Native roots must
remain reachable through verified local canonical metadata; unavailable proof
never triggers background history or file hydration. Unsynced, uncertain
or uncovered capsules remain local. A durable cleanup obligation protects
unlink/fsync/restart recovery; status reports `cleanup_pending` and
`retired_local_capture_count` (this daemon incarnation). This policy does not
clean workspace state, delete native objects or operations, or promise bounded
total operation history. Independent workspaces each need retained capacity plus
headroom for a successor; exhausting the configured quota still rejects capture.

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

## Retire immutable check candidates

After unmounting a Linux immutable check candidate, run
`workspace native-candidate-retire --workspace-id ID --expected-commit COMMIT`.
This frees its private namespace and workspace slot, not ordinary editable jj
workspaces, shared objects/CAS, server holds or retention history. Nonempty mount
directories are preserved. Exact retries and retired-ID reservations survive
restart. Downgrading a state directory after retirement is unsupported. Receipts
are capped at 4096 per daemon; retirement fails closed at that bound (pruning is
not yet implemented).
