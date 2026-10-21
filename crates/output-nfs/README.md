# output-nfs

macOS library projecting lazy writable CAS outputs through the built-in NFS client.
`NfsMount::start(tree, empty_mountpoint)` starts a trusted single-user loopback
export and mounts as the current user with `locallocks,noresvport`; no sudo or NLM.

Metadata staging downloads no payloads. First reads use the shared verified CAS;
writes become authoritative local content. Publication refreshes the mounted
client before acknowledgement. Payload concurrency is bounded; excess requests
receive retryable NFS JUKEBOX and may incur client backoff.

Call `Projection::shutdown` before dropping the mount. Failed unmount keeps the
server available; restart requires unmount/remount. Hardlinks and distributed
locks are unsupported. This is not an authenticated remote filesystem.
See [BOS setup](../bazel-output-service/README.md) and
[platform support](../../docs/platforms.md).
