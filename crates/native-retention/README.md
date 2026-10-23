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
  persists the complete root claim and clean state with `SyncAll`.
- Collector admission requires no active/draining commands, no dirty workspace,
  and no untracked access. Its exclusive guard pins a coherent bounded union of
  roots, including the real empty tree. The intrinsic zero commit is a root marker,
  not a stored blob. A future collector must validate canonical closure before
  deleting anything.

`ManagedFresh` is a host attestation that all accesses are managed, never an
automatic migration of an existing store. `Untracked` (or the monotonic untracked
pin) permanently blocks collection. Persist the registry identity alongside the
configured store; reopening requires exact identity and format agreement.

Limits cover workspace records (including tombstones), root counts/bytes,
operation IDs, command jobs, and logical encoded ledger bytes. They are **not** a
physical disk quota for Fjall files or its journal. Filesystem work runs on an
owned runtime and survives requester cancellation; callers need no Tokio runtime.

`storage_stats()` reports successful transaction count and encoded write bytes
since open (not physical I/O). `:checkpoint_benchmarks` measures stable commands
with 100, 1,000 and 10,000 roots. The current format rewrites the full retained
root record at both dirty admission and clean checkpoint; this baseline is not
an optimized immutable-root-set format.
