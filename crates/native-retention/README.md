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
