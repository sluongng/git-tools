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

Experimental native source capture inventories authoritative NTFS edits and
excludes `.jj`. It reuses unchanged BLAKE3 locators and privately copies dirty
contents under kernel change-detection fences; busy or unsupported captures fail
closed. Persisted source roots require their exact v4 root identity. Live source
checkout remains unsupported; native Windows acceptance is still required.

Native source identity markers are real, read-leased NTFS control files, not
sparse placeholders. Missing or changed recognized markers fail startup/capture
without repair; administration namespace changes invalidate acknowledgment.
