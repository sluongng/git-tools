---
name: cloud-bazel-agent
description: Start and resume isolated Linux coding-agent sessions for Bazel using agent-session, a shared Git proxy, and provisioned RBE plus remote repository contents caching instead of worktrees.
---

# Cloud Bazel agent

Give every agent private source, overlay and Bazel roots. Share the warm Git
proxy, RBE and remote caches—not mutable workspaces or output bases.

## Provisioning

Use Linux with `/dev/fuse`, `fusermount3`, `agent-session`, `git-vfs`, native Git
supporting `cat-file remote-object-info`, and compatible Bazel with BLAKE3 digest
xattrs and r2c2 (shown workflow targets Bazel 9.2). The proxy needs the configured
repository and trusted BLAKE3 mappings warmed. Mirror preparation is available
through `POST /api/v1/repos/ALIAS/prepare`; it does not populate every mapping or
prove CAS residency. Keep the unauthenticated proxy on a trusted network.

Obtain these inputs from the scheduler or provisioned environment:

- `PROXY_URL`: HTTP(S) origin without credentials; `PROXY_REPO`: configured alias.
- `TASK_REF`: clone branch/tag; `TASK_REVISION`: full immutable commit SHA.
- `TASK_TARGET`: narrow build target.
- `AGENT_SESSION`: new absolute private session path outside all mounts.
- `AGENT_BAZELRC`: existing absolute-path rc outside the repository, configuring
  RBE, a BLAKE3-capable cache, r2c2 and compatible execution platforms.

Use existing credential helpers/secret files, never keys in URLs or flags.
Provision SDKs and generated execution-platform configuration in the agent image;
r2c2 does not make host SDK detection or arbitrary generators reproducible.
Report missing provisioning rather than claiming RBE/r2c2 is active.

## Create and build

```sh
set -eu
: "${PROXY_URL:?}" "${PROXY_REPO:?}" "${TASK_REF:?}" "${TASK_REVISION:?}"
: "${AGENT_SESSION:?}" "${AGENT_BAZELRC:?}" "${TASK_TARGET:?}"
agent-session create "$AGENT_SESSION" \
  --proxy-url "$PROXY_URL" --proxy-repo "$PROXY_REPO" \
  --reference "$TASK_REF" --revision "$TASK_REVISION" \
  --bazelrc "$AGENT_BAZELRC"
agent-session build "$AGENT_SESSION" -- "$TASK_TARGET"
cd "$AGENT_SESSION/workspace"
git status --short
```

Create requires a nonexistent session directory; do not clone over retained
state. If the provisioned rc uses a named config, pass it after `--` in `build`.
The CLI supplies private Bazel roots and BLAKE3/r2c2 startup flags. Results are
JSON; subprocess output goes to stderr. `status` exposes edit readiness and
retained phase timings; a successful mount does not prove a successful RBE build.
Inspect the first invocation's execution/cache evidence. Avoid broad queries when
the task already provides a target.

Size/mapping import remains blocking metadata work. Likely Bazel inputs and
symlink blobs prefetch asynchronously after mount; other contents load on demand.
For task-specific prefetch scopes, use the component CLI described in the
[VFS reference](../../../docs/vfs-reference.md), not an invented session flag.

## Save, stop and resume

Stage only the intended changes, then commit when requested:

```sh
git add path/to/changed-file
agent-session save "$AGENT_SESSION" -- -m 'Describe the change'
agent-session stop "$AGENT_SESSION"
agent-session resume "$AGENT_SESSION"
```

`save` is native Git commit: hooks, signing and staged/unstaged semantics remain
native. It does not stage, export or push automatically. Stock Git may hydrate
all missing indexed blobs during commit; do not promise a lazy save. Retain a
patch instead when that is the requested deliverable. JJ is a separate workflow.

Stop preserves edits, index and local commits, shutting down only this session's
Bazel and verified VFS. Resume uses the original clone/overlay/revision. Never
delete the overlay or stop the shared proxy as routine cleanup. A copied session
manifest cannot claim the original process; inspect ownership failures manually.
The session CLI is currently Linux-only; native NFS/ProjFS frontends have their
own lifecycle commands. See [session limits](../../../crates/agent-session/README.md).

## Cache policy

Provision Bazel's remote TTL from the backend's actual retention contract. It is
an operator assumption, not a REAPI lease; omitting it keeps Bazel's own default,
not zero. Proxy presence suppression is separate and defaults to zero (recheck).
Never infer either policy from the other's interval or a successful upload.

Base digests reuse trusted mappings; edited files hash current overlay bytes.
A mapping is not remote CAS residency: Bazel still uploads missing inputs.
[Proxy CAS sync](../../../docs/proxy-cas-sync.md) is optional asynchronous
publication; queueing is not completion. Preserve these distinctions in reports.
