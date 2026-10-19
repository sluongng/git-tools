# git-projfs

Windows 10 **2004+**, NTFS and the **Client-ProjFS** optional feature are required.
This is a writable, fixed-commit projection: Git reads blobs lazily through the
shared `git-workspace` backend; NTFS/ProjFS retain local writes, renames and
deletion tombstones. Stopping the provider does not remove local data.

```powershell
git clone --bare --filter=blob:none https://github.com/bazelbuild/bazel.git C:\cache\bazel.git
cargo run -p git-projfs -- mount --repo C:\cache\bazel.git --root C:\work\bazel --state C:\state\bazel
# In another terminal:
Get-Content C:\work\bazel\MODULE.bazel
cargo run -p git-projfs -- digest C:\work\bazel\MODULE.bazel
```

Before starting the provider, sizes are batch-warmed from the durable cache,
local Git, GitHub tree API (for github.com), or protocol-v2 object-info. Missing
metadata fails rather than downloading file blobs merely to list a directory.
This metadata phase is blocking; file-content hydration remains lazy. For private
GitHub repos pass `--github-token-env GITHUB_TOKEN` (token value never in flags).

The root must initially be empty. Keep state and the bare repository outside the
root. Restart with the same root, state and resolved commit; a changed base is
rejected to protect local edits. Use a new pair of directories for another base.
The provider never calls destructive placeholder-update APIs. Native `git status`,
`git add`, `git commit`, and `git log` use an isolated admin directory and index
in state, with a `.git` pointer in the root and alternates to the source objects.
Restart preserves staged changes and local commits; source refs are unchanged.
Commits do not rebase the fixed projected base: use a new root/state to mount
another base. Checkout, merge and rebase are not supported.

Directory renames and hardlinks are deliberately rejected: preserving all lazy
children after those operations needs a view-rebasing design. File renames are
supported, including renamed placeholders that have not yet hydrated. Symlink
placeholders use the native ProjFS 2004+ API; only relative, snapshot-resolvable targets
are supported. Direct directory targets are typed correctly, and direct symlink chains are
depth/cycle checked. Intermediate symlink components, dangling/external targets
and non-UTF-8/Windows-ambiguous targets are rejected on access. Gitlinks are empty
directories; submodules are not mounted. Invalid Win32 names and case collisions fail at
startup. Digests hash actual file bytes, so computing one hydrates that file and
never returns the old digest for an edited file. A concurrent same-size write
can race digest computation, as with normal filesystem hashing.

## Native smoke check

On a Windows machine with Client-ProjFS already enabled, run:

```powershell
cargo test -p git-projfs
cargo test -p git-projfs --test native -- --ignored --nocapture
```

The ignored test starts the **real** provider on a temporary NTFS directory. It
checks enumeration, lazy reads, native writes, rename-before-hydration, delete,
and provider restart persistence. Linux unit tests and cross-compilation do not
substitute for this test. Enabling the optional Windows feature is an OS change
and is intentionally not done by the CLI or test.

API references: [ProjFS callbacks](https://learn.microsoft.com/en-us/windows/win32/projfs/projfs-callback-functions),
[file data and symlinks](https://learn.microsoft.com/en-us/windows/win32/projfs/providing-file-data),
[on-disk states](https://learn.microsoft.com/en-us/windows/win32/projfs/file-states-and-operations).

The native Git admin test can run without the ProjFS feature installed:
`cargo test -p git-projfs --no-default-features --test worktree`. This checks Git
index/ref preservation only, not projection callbacks.
