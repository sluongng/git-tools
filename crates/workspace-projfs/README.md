# workspace-projfs

Library and `git-projfs` executable for writable, fixed-commit Git projections.
Requires Windows 10 2004+, NTFS and the **Client-ProjFS** optional feature.
The CLI does not enable that OS feature. NTFS retains local edits and tombstones.

```powershell
# Use a native Windows git-projfs.exe on PATH.
git clone --bare --filter=blob:none https://github.com/bazelbuild/bazel.git C:\cache\bazel.git
git-projfs.exe mount --repo C:\cache\bazel.git --root C:\work\bazel --state C:\state\bazel
Get-Content C:\work\bazel\MODULE.bazel
```

Sizes are warmed before serving from local metadata, GitHub's tree API or native
object-info; missing sizes fail rather than download contents for a listing.
Private GitHub metadata uses `--github-token-env NAME`. Content remains lazy.
Start with an empty root; keep the clone and state outside it. Restart the same
root/state/base to retain edits, Git index and local commits. Use a new root/state
for a changed base; general checkout/merge/rebase is unsupported.

Native Git status/add/commit/log use private admin state. Directory renames and
hardlinks are unsupported; file renames are supported. Symlinks must be relative
and snapshot-resolvable; dangling/external targets and intermediate symlink components are rejected.
Invalid Win32 names and case collisions fail at startup; submodules are not mounted.
The digest command hashes actual bytes and hydrates that file, including edits.
See [platform setup and native checks](../../docs/platforms.md).
