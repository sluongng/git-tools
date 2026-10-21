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

## Explicit maintenance

`NfsMount::maintain_quiescent(transaction_budget)` rotates the native mount/server
and reclaims retired generations. Stop RPC admission and quiesce Bazel and all
other native filesystem users first: rotation briefly unmounts the root. It is
not automatic build finalization. The in-process output host exposes the same
operation as `OutputHost::maintain_quiescent`. The standalone CLI and daemon do
not expose or automatically invoke this operation; an embedding owner must call
it at a genuinely quiescent boundary.

An ordinary busy unmount returns `BusyRetained`, preserving the exact server and
old descriptor contents. Successful rotation drains all transport/RPC tasks and
expires old file handles before collection. Linked local edits are never GC
candidates. The budget bounds each collection transaction, not live disk usage.

If collection fails, maintenance still attempts to restore the native mount and
returns the error. Remount/ownership errors retain a recovery-required session;
retry maintenance after resolving the cause. `NfsMount::state` reports the last
lifecycle result. A failed start may return `NfsStartError`, whose `recovery()`
handle owns any possibly-mounted server for an explicit retry or shutdown.
Never replace a recovery-required root or discard its state directory.
