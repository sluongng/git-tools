# output-projfs

Lazy Windows CAS outputs for `bazel-output-service`. Enable Windows **Client-ProjFS**;
no filesystem driver is bundled. The service selects this frontend in lazy-output
mode. Provider state and the shared `output-core` cache must live outside the mount.

Metadata does not download payloads. First reads use whole-object verified CAS
cache leases and aligned, bounded native writes. The embedding Tokio runtime must
be multithreaded and stay alive through `shutdown`; shutdown drains page-ins before
stopping ProjFS to preserve correct bytes for existing readers. `BlobSource` must
enforce download deadlines; shutdown does not force-stop a stalled source. Immutable content IDs survive
file renames; NTFS file identities and persisted rename epochs prevent stale digest
reuse after replacement or restart. Native edits remain in NTFS, not a second overlay.

Staging rejects locally modified data and sharing-conflicted files. Metadata-only
chmod/mtime changes preserve digest reuse. Removing a modified file permits Bazel
to repair its deletion tombstone. Local action scratch directories can be renamed; directories containing
projected descendants require namespace rebasing and currently reject rename.
Remote symlinks are unsupported; native relative output symlinks must stay confined.
`clean` explicitly removes outputs and tombstones; it is not background GC.

Native tests (Windows, enabled ProjFS, Python 3 for separate-process clients):

```powershell
$env:OUTPUT_PROJFS_TEST_PYTHON = (Get-Command python).Source
cargo test -p output-projfs --test native -- --ignored --nocapture
```

The fixture covers lazy reads, generation replacement, native renames/edits,
restart/recovery, sharing failures, cancellation attempts, shutdown under load,
and junction-safe clean. Native cancellation can race normal completion; counters
report observed cancellations instead of assuming that every attempt aborts I/O.
