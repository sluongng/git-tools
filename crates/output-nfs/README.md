# output-nfs

Lazy, writable CAS output projection for the macOS built-in NFS client.
`NfsMount::start(tree, empty_mountpoint)` starts a loopback server and mounts it
as the current user with `locallocks,noresvport`; no sudo or NLM is needed.
Existing mountpoints are rejected using the nonblocking kernel mount table;
shutdown identifies its own export and does not unmount a replacement.
Call `Projection::shutdown` before dropping the mount. An unmount failure keeps
the server running and is returned to the caller.

Staging downloads no bytes. First reads use output-core's verified bounded
cache; writes become durable local content. Publication waits for same-mode
chmod through the mounted client outside namespace locks. Ancestors are first
refreshed top-down to clear cached missing parents, then the file and its
ancestors complete invalidation bottom-up. This refreshes cached content and missing names without TTL sleeps.
Sixty-four frontend payload permits cover reads and inode-serialized mutations;
128 transport slots reserve capacity for metadata and publication barriers.
Excess payload requests receive retryable NFS JUKEBOX. Native clients can back
off substantially before retrying, so this protects overload rather than making
arbitrary bursts low-latency. The verified cache separately limits actual CAS
fetches. Tests can lower the payload limit with `OutputNfs::with_read_limit`.
Retired inode records preserve existing handles; restart requires unmount and
remount. Hardlinks and distributed locks are unsupported. This is a trusted
single-user local export, not an authenticated remote filesystem.

Checks: `cargo test -p output-nfs`. On a Mac, run the native mounted acceptance
with `cargo test -p output-nfs --test projection -- --include-ignored`.
