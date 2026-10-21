# output-projfs

Windows library for lazy writable BOS outputs using the **Client-ProjFS** optional
feature. Keep provider state and shared CAS storage outside the projected root.

Metadata does not hydrate content; first reads use verified whole-object cache
leases. NTFS retains edits. Immutable content IDs, file identities and persisted
rename epochs prevent digest reuse after replacement/restart.

Remote placeholders carry complete digest/size locators backed by a durable,
bounded origin manifest. Retired metadata can be collected without deleting
NTFS payloads or invalidating old immutable reads. Legacy placeholder IDs stay
protected. Keep the original provider state; manifest/namespace upgrades reject
older binaries rather than permitting unsafe downgrade.

Keep a multithreaded Tokio runtime alive through `shutdown`, which drains page-ins
before stopping ProjFS. `BlobSource` must enforce download deadlines.
Staging rejects local modifications and sharing conflicts. Directory renames
containing projected descendants and remote symlinks are unsupported; native
relative output symlinks must stay confined. `clean` explicitly removes outputs
and tombstones; it is not background GC.
See [BOS setup](../bazel-output-service/README.md) and
[platform support/tests](../../docs/platforms.md).
