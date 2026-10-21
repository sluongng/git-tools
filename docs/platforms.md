# Platform support

All frontends are experimental, fixed-base projections. Unified `workspace`
source-session lifecycle is Linux-only; macOS and Windows use standalone adapters.
Client filesystem support does not provision compatible remote execution workers.

| Surface | Setup | Important limits |
| --- | --- | --- |
| [Linux source FUSE](../crates/workspace-fuse/README.md) | `/dev/fuse`, `fusermount3`, native Git | SHA1 repositories; no general checkout/merge/rebase |
| [macOS source NFS](../crates/workspace-nfs/README.md) | Built-in NFS client, loopback export | No digest xattrs, hardlinks or distributed locking |
| [Windows source ProjFS](../crates/workspace-projfs/README.md) | NTFS, Windows 10 2004+, Client-ProjFS | Rejects ambiguous names, directory renames, unsupported symlinks |
| [Jujutsu](../crates/jj-vfs/README.md) | Linux writable mount + custom `git-jj` | No submodules, sparse checkout, `workspace add`, conflict snapshots |
| [Bazel outputs](../crates/bazel-output-service/README.md) | Linux FUSE / macOS NFS / Windows ProjFS | Whole-blob verified reads; no TreeArtifactLocator staging |

macOS mounts without sudo as the mountpoint owner using
`locallocks,noresvport`. Locks coordinate that Mac's processes, not multiple
clients. Restart requires unmount/remount; live server takeover is not supported.

Windows keeps local edits and deletions on NTFS. Restart with the same
root/state/commit. Source directory renames are unsupported; output scratch
renames require no projected remote descendants to rebase.

Output adapters stage metadata lazily; the first reader downloads and verifies a
whole blob. Active-read leases do not protect an indefinitely open NFS descriptor
after cache expiry. Cache eviction must never delete local edits or tombstones.
Keep a busy NFS server running until unmount succeeds.

Use each crate's native acceptance commands on the actual platform. Linux
protocol tests or cross-compilation are not substitutes.

## Native Bazel builds and acceptance

Dependency resolution includes Linux x86-64, macOS ARM64/x86-64 and Windows
x86-64 MSVC. Rust and LLVM versions remain pinned by the repository. The LLVM
macOS toolchain downloads an Apple SDK; Windows MSVC uses the Microsoft runtime
and Windows SDK. Cross-compilation is useful build evidence, not native acceptance.

### macOS

Install Bazelisk and use a native macOS terminal. For Apple Silicon:

```sh
bazel build --platforms=@llvm//platforms:macos_aarch64 \
  //:git-nfs //:git-output-service
bazel test --platforms=@llvm//platforms:macos_aarch64 \
  --extra_execution_platforms=@llvm//platforms:macos_aarch64 \
  //crates/workspace-nfs:native_tests //crates/output-nfs:native_tests --test_output=all
```

On Intel Macs replace `macos_aarch64` with `macos_x86_64`. To inspect the output
acceptance case first, select only `//crates/output-nfs:native_tests` and append
`--test_arg=--list`; expect `native_publication_and_mutation` (one ignored native
case). Source acceptance runs the existing smoke script with declared Git/Python
fixtures and the Bazel-built NFS server. Both checks use the actual macOS kernel
NFS client; keep the server alive if unmount fails.

### Windows

Use native Windows x86-64, NTFS, Bazelisk and Bazel's required MSYS2 Bash setup.
Enable the Windows optional feature `Client-ProjFS` before provider acceptance.
The pinned LLVM MSVC toolchain requires explicit acceptance of the Microsoft
Visual Studio runtime and Windows SDK licenses. After accepting those licenses,
set the following repository environment options for each build/test command
(or in ignored local Bazel configuration):

```text
--repo_env=BAZEL_MSVC_RUNTIME_VISUAL_STUDIO_EULA=1
--repo_env=BAZEL_WINDOWS_SDK_EULA=1
```

Then use PowerShell (the license options above must also be supplied):

```powershell
bazel build --platforms=@llvm//platforms:windows_x86_64_msvc //:git-projfs //:git-output-service
$env:OUTPUT_PROJFS_TEST_PYTHON = (Get-Command python).Source
bazel test --platforms=@llvm//platforms:windows_x86_64_msvc `
  --extra_execution_platforms=@llvm//platforms:windows_x86_64_msvc `
  //crates/workspace-projfs:native_tests //crates/output-projfs:native_tests `
  --test_output=all
```

Native acceptance intentionally inherits PATH for installed Git/Python and the
Windows system tools; ordinary portable tests still use declared fixtures.
Git must be on PATH and Python must be native Python 3. Append
`--test_arg=--list` before acceptance: expect one source case and three output
cases, not zero. The source target explicitly enables the `native` feature.
These manual, Windows-constrained targets run ignored provider tests only and
are excluded from wildcard test runs. A successful build/list without running
the cases does not verify ProjFS behavior.
