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
