# native-retention (library)

Durable workspace root claims and command/collector fencing for a shared native
object store. It does **not** delete objects, validate object closure, or contact
remote services.

- Register an opaque workspace and operation-store identity; registrations start
  dirty and remain pinned after crashes. Explicit forget leaves a durable,
  non-reusable tombstone. There is no expiry or heartbeat-based forgetting.
- Hold a command owner for the session. Every accepted physical object job must
  retain a resolved job guard, including background and non-cancellable work.
- Checkpoint typed object roots plus opaque operation IDs after workspace writes
  finish. It freezes new job admission, drains accepted jobs, then atomically
  persists the complete root claim and clean state with `SyncAll`. Owned
  checkpoint callbacks keep physical capture guards through durable publication,
  even when the requester is cancelled.
- `capture_dirty` seals/drains a command and atomically records its roots plus
  an independent immutable-bundle ticket, leaving the workspace dirty. The host
  must sync the bundle before callback success. Pending tickets survive newer
  checkpoints and explicit forget; no upload or acknowledgement is implied.
- A trusted host may retire an older redundant capsule only after freshly
  verifying its exact retained remote version and the successor capsule's full
  operation/view/index dependency coverage. `retire_redundant_capture` rechecks
  both exact rows, the same registration, strict age ordering and typed-root
  inclusion. One `SyncAll` transaction removes the old ticket and records one
  bounded cleanup obligation; it never cleans a workspace or deletes native,
  operation or capsule bytes. This is not general remote offloading or GC.
- `retire_redundant_capture_with_coverage` can instead prove typed-root reachability
  through bounded canonical metadata, for example an old working-copy tree named
  by a retained historical commit. Witnesses are verified and traversed only from
  exact successor roots, never arbitrary matching objects. FILE payloads,
  unanchored or incomplete paths, and excessive object/byte/edge work are rejected.
  `OwnedCaptureCoverage` retains the admitted payload owner through validation and
  durable publication after cancellation. Proof blobs are not stored in the ledger.
- Physical cleanup uses `try_capture_cleanup` and retains its exclusive guard
  through exact file validation, unlink and directory sync, then calls
  `acknowledge_capture_cleanup`. The obligation binds exact upload-record bytes;
  shared pack digests are not unlinkable. A pending slot blocks further retirement
  and new tickets for a pack awaiting unlink. Errors preserve the slot for replay,
  not an unbounded list of cleanup tombstones. Retirement does not require Clean
  workspaces, but active commands and capture readers return Busy.
- Collector admission requires no active/draining commands, no dirty workspace,
  and no untracked access. Its exclusive guard pins a coherent bounded union of
  roots, including the real empty tree. The intrinsic zero commit is a root marker,
  not a stored blob. Collectors must validate canonical closure before deleting
  anything.

`ManagedFresh` is a host attestation that all accesses are managed, never an
automatic migration of an existing store. `Untracked` (or the monotonic untracked
pin) permanently blocks collection. Persist the registry identity alongside the
configured store; reopening requires exact identity and format agreement.

Limits cover workspace records (including tombstones), root counts/bytes,
operation IDs, command jobs, pending capture roots/bundle descriptors, and logical
encoded ledger bytes. They are **not** a physical disk quota for Fjall files or
its journal. Filesystem work runs on an
owned runtime and survives requester cancellation; callers need no Tokio runtime.

`storage_stats()` reports successful transaction count and encoded write bytes
since open (not physical I/O). `:checkpoint_benchmarks` compares ordinary
checkpoints with queued captures at 100, 1,000 and 10,000 roots, excluding bundle
export, staging and networking. Both rewrite workspace roots; captures also
retain independent ticket roots. This is not an immutable-root-set optimization.

Registries with pending captures require a capture-aware reader. Older readers
fail closed on the versioned inventory rather than omitting its root claims.
The first capsule retirement advances the durable manifest to version 3, even
after cleanup acknowledgement. Pre-retirement readers reject it. Reopening checks
both capture and cleanup commitments; process restart does not acknowledge cleanup.
