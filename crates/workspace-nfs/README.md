# workspace-nfs

Writable lazy Git projection through a loopback NFSv3 server, using the native
macOS client instead of macFUSE. Overlay storage and Git objects stay outside the
mount. The pinned, locally hardened `nfsserve` 0.11.0 supplies NFS/MOUNT RPC
(BSD-3-Clause); see [patch notes](../../third_party/nfsserve/PATCHES.md).

```sh
cargo build -p workspace-nfs
# First create a bare blobless clone, e.g. git clone --bare --filter=blob:none URL repo.git
mkdir -p /tmp/git-workspace
target/debug/git-nfs --repo repo.git --overlay state --mountpoint /tmp/git-workspace
# macOS, in another terminal (default TCP port 11111):
mount_nfs -o vers=3,tcp,locallocks,noresvport,port=11111,mountport=11111,nobrowse,nosuid,nodev \
  127.0.0.1:/ /tmp/git-workspace
```

Native macOS acceptance: read tracked files and symlinks; create/write/truncate,
rename and remove a file and directory; run `git status`, `git add` and `git commit`;
unmount with `umount /tmp/git-workspace`, stop/restart the server and remount,
and verify edited data and status survive. **Passed on macOS 27.2 arm64 without sudo.**
Linux wire tests exercise actual TCP RPC without requiring privileged mounts.

Writes are flushed before the server acknowledges FILE_SYNC. Hardlinks, distributed locking,
chown to another identity, NFS xattrs and remote exports are unsupported. Base Git SHA-1 lookup remains available through the existing `git-vfs digest`
CLI, not NFS xattrs; that command does not describe edited overlay contents.
Use `git hash-object <mounted-path>` for edited files. The NFS binary includes
its own journal-backed fsmonitor hook, so macOS does not need a FUSE binary.
GitHub size metadata is warmed before serving, so directory attributes do not
fetch every blob. Other origins need object-info support or prewarmed sizes;
`--fetch-missing-sizes` explicitly permits content downloads for missing sizes.

This is single-user local IPC: any local process can connect; AUTH_SYS is not an
isolation boundary. Use `nosuid,nodev` client options where supported. Directory
pagination can return BAD_COOKIE if the namespace changes between pages. Mount
and unmount manually; never remove the overlay to clear a cache.

Mount and unmount as the user who owns the mountpoint; no sudo is needed for
this loopback export. `noresvport` avoids the root-only reserved source port.
`locallocks` supports locks between processes on this Mac, not across clients.

Automated native verifier: `GIT_NFS_BIN=/absolute/git-nfs ./crates/workspace-nfs/scripts/macos-smoke.sh`.
