# native-vfs-rpc · library

Private projection control between jj processes and daemon-owned native mounts.
It reuses authenticated Unix sockets and owner-only Windows named pipes; there is
no network or materialized-workspace fallback.

- `ProjectionService` registers trusted mount/controller bindings and exposes a
  bounded `rpc_service()` for the daemon's existing local router.
- `ClientConnector` implements the jj-vfs connector without requiring a caller
  Tokio runtime. Inspection verifies the exact root, external state directory,
  and repository binding before returning a controller.
- Mutation sessions retain the controller fence. EOF revokes new admission;
  already-started physical work drains with its owners and reservations intact.
- Snapshot inventory and policy decisions stream under the same frozen source
  snapshot. Client-side jj matchers, base ignores, size policy and progress
  callbacks stay local. Only frozen `.gitignore` payloads may hydrate for policy;
  unchanged regular files are not read for hashing.

All RPCs negotiate zstd. Configurable limits bound frames, policy bytes/counts,
concurrent memory admission, action count, per-action deadlines, session lifetime
and idle time. Missing receipts remain unknown, never proof of non-application.
Only explicit controller rejection is returned as `ProjectionFailure::Rejected`.

`//crates/native-vfs-rpc:rpc_tests` uses the real controller and source namespace
with an explicit unmounted protocol fixture. It does not establish native FUSE,
NFS or ProjFS acceptance.
