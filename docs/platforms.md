# Platform support

All frontends are experimental, fixed-base projections. Compilation and protocol
tests do not establish native runtime acceptance. macOS/Windows client support
also does not provision compatible RBE execution workers.

| Surface | Setup | Important limits |
| --- | --- | --- |
| [Linux source FUSE](../crates/workspace-fuse/README.md) | `/dev/fuse`, `fusermount3`, native Git | SHA1 repositories; no general checkout/merge/rebase |
| [macOS source NFS](../crates/workspace-nfs/README.md) | Built-in NFS client, loopback export | No digest xattrs, hardlinks or distributed locking |
| [Windows source ProjFS](../crates/workspace-projfs/README.md) | NTFS, Windows 10 2004+, Client-ProjFS | Rejects ambiguous names, directory renames, unsupported symlinks |
| [Jujutsu](../crates/jj-vfs/README.md) | Linux writable mount + custom `git-jj` | No submodules, sparse checkout, `workspace add`, conflict snapshots |
| [Bazel outputs](../crates/bazel-output-service/README.md) | Linux FUSE / macOS NFS / Windows ProjFS | Whole-blob verified reads; no TreeArtifactLocator staging |

## Native behavior

macOS mounts as the owner of the mountpoint using `locallocks,noresvport`, without
sudo. Locks coordinate processes on that Mac, not multiple clients. Restart
requires unmount/remount; live daemon takeover is not implemented. NFS publication
barriers refresh the mounted client outside namespace locks. Do not reclaim old
NFS identities merely because Linux would have received a forget/close callback.

Windows preserves local edits and tombstones on NTFS. Restart uses the same
root/state/commit. Reject case aliases and invalid Win32 names rather than rewrite
them. Source directory renames remain unsupported; output scratch-directory
renames work only when no projected remote descendants need rebasing.

All output adapters stage metadata without downloading payloads. The first reader
pays whole-object verification; file leases protect active reads, not an
indefinitely open NFS descriptor after CAS expiry. Cache GC must not delete local
edits or tombstones. Busy unmounts must retain a functioning server for retry.

## Acceptance and verification

Historical native macOS source acceptance passed lazy reads, writes, Git
status/add/commit, local locks and remount without sudo. Native macOS NFS and
Windows ProjFS output adapters passed a 12-phase Bazel/BuildBuddy workflow:
SHA256/BLAKE3, metadata-only cached builds, execution, restart, repair and clean.
These were correctness fixtures, not cross-platform speed comparisons. Windows
cancellation attempts completed normally; observing a cancellation callback was
not established by that run.

Use the current crate instructions for actual native tests. Linux tests or
cross-compilation are not substitutes. For performance, measure mount readiness,
first usable file/output, repeated edits/builds, retained storage and download bytes
separately; a zero-download stage can simply move latency to its first consumer.
