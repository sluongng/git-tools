# output-core

Durable, generation-aware CAS output namespace, shared by native output mounts.
Fjall stores per-entry metadata; immutable IDs prevent a late read from resolving
new bytes at an old path. Local mutations drop remote digest authority.

`BlobSource` supplies operator-selected CAS retrieval and presence checks.
`Tree` stages metadata without fetching, verifies whole blobs before serving
ranges, and coalesces concurrent readers. Disk, in-flight bytes and fetch counts
are bounded; oversized artifacts can stage metadata but fail content admission. Cache leases protect
active reads, not indefinitely open NFS descriptors. Retired IDs and local edits
are conservatively retained; deleting this state destroys authoritative edits.

Adapters implement `Projection` publication and native dirty-state semantics.
Windows ProjFS uses NTFS for writable data, not this crate's local payload path.
Checks: `cargo test -p output-core`.
