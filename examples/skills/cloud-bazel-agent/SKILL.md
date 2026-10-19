---
name: cloud-bazel-agent
description: Start an isolated cloud coding-agent workspace for a Bazel repository using git-vfs, a shared Git proxy, and provisioned RBE plus remote repository contents caching, instead of a Git worktree.
---

# Cloud Bazel agent

Give each agent a private writable mount over a blobless clone; share the warm
Git proxy and remote caches, not mutable workspaces or Bazel output bases.
Do not create a worktree by default.

## Before starting

Use a provisioned Linux host with `/dev/fuse`, `git-vfs`, native Git supporting
`cat-file --batch-command`'s `remote-object-info`, and a repository-compatible
Bazel with BLAKE3 digest-xattr support (8.7+ or 9.2+ on those release lines).
The shared proxy must have this repository and BLAKE3 mappings warmed with
`--enable-blake3`; a cold mapping import can block startup.
Keep the unauthenticated proxy on a trusted network, not the public Internet.

Obtain these task inputs from the scheduler or existing environment:

- `PROXY_URL`: proxy origin, such as `http://127.0.0.1:8082`, without credentials.
- `PROXY_REPO`: configured repository alias, such as `bazel`.
- `TASK_REF`: branch or tag to clone; `TASK_REVISION`: full immutable commit SHA.
- `AGENT_STATE_ROOT`: writable directory outside all mounts.
- `AGENT_BAZELRC`: provisioned absolute-path rc file outside the repository,
  configuring the organization's RBE, BLAKE3-capable remote cache, and remote
  repository contents cache (r2c2), including its required execution platforms.

Use the organization's existing credential helper or mounted secret files.
Do not put credentials in Git URLs, command-line flags, or the source tree.
Do not guess r2c2 flags: the host's Bazel version and deployed service determine
its configuration. If provisioning is missing, report that dependency rather
than claiming remote execution or dependency caching is active.

## Start one workspace

Run in a persistent agent shell; retain `TASK_DIR` and `VFS_PID` for cleanup.
Use a fresh directory for every agent, even when agents share a commit.

```sh
set -eu
: "${PROXY_URL:?}" "${PROXY_REPO:?}" "${TASK_REF:?}" "${TASK_REVISION:?}"
: "${AGENT_STATE_ROOT:?}" "${AGENT_BAZELRC:?}"
TASK_DIR=$(mktemp -d "${AGENT_STATE_ROOT%/}/git-agent.XXXXXXXX")
mkdir "$TASK_DIR/workspace"
git-vfs clone --depth 1 --branch "$TASK_REF" \
  "${PROXY_URL%/}/git/$PROXY_REPO.git" "$TASK_DIR/repo.git"
# The scheduled commit may no longer be the branch tip.
if ! git -C "$TASK_DIR/repo.git" cat-file -e "$TASK_REVISION^{commit}"; then
  git -C "$TASK_DIR/repo.git" fetch --depth=1 origin "$TASK_REVISION"
fi
git-vfs warm-blake3 --repo "$TASK_DIR/repo.git" --revision "$TASK_REVISION" \
  --proxy-url "$PROXY_URL" --proxy-repo "$PROXY_REPO"
git-vfs mount --repo "$TASK_DIR/repo.git" --revision "$TASK_REVISION" \
  --overlay "$TASK_DIR/overlay" \
  --blake3-proxy-url "$PROXY_URL" --blake3-proxy-repo "$PROXY_REPO" \
  --prefetch-bazel "$TASK_DIR/workspace" >"$TASK_DIR/vfs.log" 2>&1 &
VFS_PID=$!
for attempt in $(seq 1 300); do
  mountpoint -q "$TASK_DIR/workspace" && break
  kill -0 "$VFS_PID" 2>/dev/null || { cat "$TASK_DIR/vfs.log"; exit 1; }
  sleep 0.1
done
mountpoint -q "$TASK_DIR/workspace"
cd "$TASK_DIR/workspace"
```

Mapping/size import is blocking metadata work; `--prefetch-bazel` fetches likely
Bazel files and symlink blobs asynchronously after mount readiness.
Other file contents load on demand. Use repeated `--prefetch-include` arguments
for known task-specific inputs; do not fetch the entire source tree by default.

## Work and build

Define this wrapper in the same shell. Keep the repository's normal rc files;
add the provisioned service configuration without sharing mutable Bazel state.

```sh
agent_bazel() {
  bazel --nosystem_rc --nohome_rc --bazelrc="$AGENT_BAZELRC" \
    --output_user_root="$TASK_DIR/bazel-user" \
    --output_base="$TASK_DIR/bazel-output" \
    --experimental_remote_repo_contents_cache \
    --digest_function=BLAKE3 --unix_digest_hash_attribute_name=user.blake3 "$@"
}
agent_bazel query //... --experimental_remote_cache_ttl=3h
# Use --experimental_remote_cache_ttl=3h on task build/test commands too.
git status --short
```

The three-hour remote TTL matches the proxy CAS-sync default (10,800 seconds).
Check the first invocation's execution/cache evidence before claiming RBE or
r2c2 is used. Avoid a broad query when the task already identifies narrow targets.
Base-file digests come from cached mappings; edited files hash current overlay
bytes. A mapping is **not CAS residency**: Bazel still uploads missing inputs.
Proxy-side asynchronous uploads are a separate optimization; see the
[CAS sync guide](../../../docs/proxy-cas-sync.md) for supported configuration.
Never invent upload flags or treat background upload enqueueing as completion.

## Finish or suspend

Before unmounting, retain the requested patch/commits and inspect `git status`.
Stop only this agent's Bazel server and mount, from the shell that owns them:

```sh
agent_bazel shutdown
cd "$TASK_DIR"
kill -INT "$VFS_PID"
wait "$VFS_PID"
```

Keep `repo.git` and `overlay` until edits and Git index state are deliberately
saved or discarded. Remount them at the same revision to resume; do not rerun
clone into an existing directory or delete the overlay as routine cleanup.
Do not stop the shared proxy or another agent's mount/server.
