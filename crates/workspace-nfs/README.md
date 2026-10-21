# workspace-nfs

Library and `git-nfs` executable exposing writable lazy Git files through a
single-user loopback NFSv3 server for the native macOS client; no macFUSE required.
Keep the bare blobless clone and persistent overlay outside the mount.

```sh
# Use a native macOS git-nfs binary on PATH.
# Create repo.git with git clone --bare --filter=blob:none URL repo.git first.
mkdir -p /tmp/git-workspace
git-nfs --repo repo.git --overlay state --mountpoint /tmp/git-workspace
# In another macOS terminal:
mount_nfs -o vers=3,tcp,locallocks,noresvport,port=11111,mountport=11111,nobrowse,nosuid,nodev \
  127.0.0.1:/ /tmp/git-workspace
```

Mount/unmount manually as the mountpoint owner; no sudo is needed for this export.
`locallocks` supports only processes on this Mac. Writes are flushed before
FILE_SYNC ACK. The binary provides its own journal-backed Git fsmonitor hook.
GitHub size metadata is warmed before serving; other origins need object-info
or prewarmed sizes. `--fetch-missing-sizes` explicitly allows content downloads.

Any local process can connect; AUTH_SYS is not an isolation boundary. Remote
exports, hardlinks, distributed locks, cross-identity chown and NFS xattrs are
unsupported. Git SHA1 metadata is available via `git-vfs digest`; hash an edited
mounted file with `git hash-object` instead. Never delete overlay state as cache.
See [platform setup and native checks](../../docs/platforms.md).
