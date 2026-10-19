# Platform frontends and remote outputs

The object/cache backend remains shared. Each frontend lives in a separate crate so platform dependencies do not enter the Linux FUSE binary.

| Crate | User-facing surface | Mutable state |
| --- | --- | --- |
| `workspace-nfs` | NFSv3 server for the built-in macOS client | Existing durable `workspace-overlay` |
| `workspace-projfs` | Windows Projected File System provider | NTFS local files and ProjFS tombstones |
| `jj-vfs` | Custom Jujutsu CLI and working-copy implementation | Jujutsu state plus the mounted durable overlay |
| `bazel-output-service` | Bazel Output Service gRPC, backed by BuildBuddy CAS | Dedicated local output trees |

## Contracts

All input frontends pin a Git commit and hydrate content on demand. Windows must reject paths that cannot be represented uniquely on NTFS, including case aliases; it must not silently rewrite names. ProjFS keeps edited files authoritative across provider restarts. Unix frontends retain the existing overlay lifetime and recovery rules.

NFSv3 does not carry our digest xattrs. A successful NFS mount is therefore not proof that Bazel can skip content hashing. Digest-aware integrations need an explicit metadata path or the existing proxy API; measure their effect separately from filesystem startup.

Jujutsu working-copy integration is compiled into a custom CLI, not dynamically installed into stock `jj`. Revision changes must reconcile the mounted namespace without overwriting dirty content outside Jujutsu's normal update rules. Status/snapshot must distinguish unchanged base objects from new or modified files; a fast path must fall back conservatively when journal continuity is lost.

The output service implements Bazel's output-tree protocol, not remote execution. The initial backend eagerly downloads and verifies staged files from a configured CAS. This is useful as a correctness baseline; it does **not** provide lazy output hydration. Keep CAS endpoint and credentials operator-controlled, confine all paths to dedicated output trees, and invalidate conservatively rather than advertise unimplemented dirty tracking. A future lazy output frontend can retain the same protocol boundary.

## Acceptance

Compilation, protocol tests and actual platform workflows are separate evidence. Native macOS acceptance requires the built-in NFS client; native Windows acceptance requires ProjFS enabled and an actual provider run. Jujutsu needs real mounted edit/snapshot/revision workflows. The output service needs an actual Bazel build using BuildBuddy, including subsequent builds and output-byte checks. See each crate's usage and verification instructions for the supported subset and current limitations.

Benchmarks should report startup, first read, iterative edit/status/build, downloaded bytes and retained local storage. Compare against a matched ordinary checkout and existing Linux frontend; do not infer cross-platform speedups from RPC-only timings.
