# fuser 0.16.0 local patch

Upstream: https://github.com/cberner/fuser

Published source revision: `d39b15200d2509db6bf712346d2cceade3a3f2fd`.
Source archive: https://static.crates.io/crates/fuser/fuser-0.16.0.crate
Archive SHA-256: `0bb29a3ae32279fe3e79a958fe01899f5fb23eadccee919cf88e145b54ed9367`.
License: [MIT](LICENSE.md).

This copy includes the complete library, build script, manifests, examples and
integration tests from the published crate. Packaging/CI scripts and the upstream
lockfile are omitted. The enclosing workspace lockfile pins production builds;
this copy also includes a separate generated lockfile for standalone lifecycle
unit tests. The file
`UPSTREAM-SHA256SUMS` records hashes of the retained files before patching.

## Local changes

- `src/session.rs`: Linux-only `Session::run_until(&AtomicBool)` uses a finite
  100 ms poll and nonblocking reads to stop an idle session without closing a
  descriptor from another thread. Socket-backed tests cover cancellation, peer
  closure and preservation of a ready request when already cancelled.
- `src/channel.rs`: an owning RAII guard enables and restores file status flags,
  retrying interrupted `fcntl` calls. No changes to `ChannelSender::send`.

- `src/mnt/fuse_pure.rs`: Linux `EBUSY` from ordinary unmount uses the existing
  lazy-unmount fallback, just as permission-denied errors already do. This also
  lets root detach a busy mount before stopping its session.

`run()`, background-session handling and non-Linux behavior are unchanged.
The caller owns unmounting and joining the thread. This API bounds idle receive
waits, not filesystem callbacks or replies already in progress. File status flags
are shared with cloned reply senders; Linux FUSE writes do not wait for inbound
requests. Concurrent status-flag changes or a second receiver are unsupported.
Keep the local patch small so it can be replaced by an upstream cancellation API.
