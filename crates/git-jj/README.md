# git-jj

Compiled Jujutsu 0.46 working-copy backend for an active writable `git-vfs`
mount. Initialize from the mount root. Stock `jj` cannot load this backend: use `git-jj` for all commands in
these workspaces.
For native Git origins, mount with `--size-source=git` to batch size metadata
before the initial status scan. That metadata phase is included in mount startup.

```sh
# Start git-vfs mount with --overlay /state/agent first, then:
cd /workspace/agent
git-jj init-vfs --overlay /state/agent
git-jj status
git-jj describe -m 'Implement change'
git-jj new
```

The backend adopts the mount's base without checking it out, consults its
bounded change journal, and reuses **base-only** SHA-1 metadata. Dirty/new
contents are stored in jj's Git backend before a snapshot is committed.
Checkout uses jj's differential disk implementation, so the mount can keep
its original base while changed files live in the durable overlay. Keep both
`.jj` state and overlay; neither is disposable cache.

Initial scope: Linux writable mounts, UTF-8 paths, regular files, executable
bits and symlinks. Repositories containing Git submodules are rejected before
initialization. Sparse checkout and `workspace add` are rejected. Conflicted
paths cannot yet be snapshotted; do not use this backend for merge/conflict
resolution. Journal overflow safely falls back to a metadata walk. This is not
a general stock-jj plugin or a guarantee that all jj commands are supported.

Real-mount verification:

```sh
GIT_VFS_BIN="$PWD/target/debug/git-vfs" \
  cargo +1.98.1 test -p git-jj --test lifecycle -- --ignored
# Or compile with Bazel/RBE and run the mount fixture on this Linux host:
bazel test --config=rbe //crates/git-jj:lifecycle --test_arg=--ignored
```

Upstream extension API: [custom working copy example](https://github.com/jj-vcs/jj/blob/v0.46.0/cli/examples/custom-working-copy/main.rs).
