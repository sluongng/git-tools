use crate::proto as p;
use tinytree_model::{Descriptor, Kind, ObjectId, ObjectKey};
use tonic::Status;
pub(crate) fn key(kind: u32, id: &[u8]) -> Result<ObjectKey, Status> {
    let kind = match kind {
        1 => Kind::File,
        2 => Kind::Tree,
        3 => Kind::Commit,
        4 => Kind::Symlink,
        5 => Kind::CopyHistory,
        _ => {
            return Err(Status::invalid_argument(
                "invalid native broker object kind",
            ));
        }
    };
    let id: [u8; 32] = id
        .try_into()
        .map_err(|_| Status::invalid_argument("invalid native broker object ID"))?;
    if kind == Kind::Commit && id == [0; 32] {
        return Err(Status::invalid_argument(
            "intrinsic root has no broker payload",
        ));
    }
    Ok(ObjectKey {
        kind,
        id: ObjectId(id),
    })
}
pub(crate) fn descriptor(value: p::ObjectDescriptor) -> Result<Descriptor, Status> {
    if value.format_version != tinytree_model::FORMAT_VERSION as u32 {
        return Err(Status::data_loss("unsupported broker object format"));
    }
    Ok(Descriptor {
        key: key(value.kind, &value.id)?,
        size_bytes: value.size_bytes,
        format_version: tinytree_model::FORMAT_VERSION,
    })
}
pub(crate) fn describe(value: Descriptor) -> p::ObjectDescriptor {
    p::ObjectDescriptor {
        kind: value.key.kind as u32,
        id: value.key.id.0.to_vec(),
        size_bytes: value.size_bytes,
        format_version: value.format_version as u32,
    }
}
pub(crate) fn id(value: &str) -> Result<(), Status> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Status::invalid_argument("invalid native broker store ID"));
    }
    Ok(())
}
pub(crate) fn catalog_hash(
    hash: &mut blake3::Hasher,
    metadata: &jj_native_backend::VerifiedMetadata,
) {
    let d = metadata.descriptor();
    hash.update(&[d.key.kind as u8]);
    hash.update(&d.key.id.0);
    hash.update(&d.size_bytes.to_be_bytes());
    hash.update(metadata.bytes());
}
pub(crate) fn backend(error: jj_lib::backend::BackendError) -> Status {
    let _ = error;
    Status::failed_precondition("native broker provider operation failed")
}
pub(crate) fn other(
    error: impl std::error::Error + Send + Sync + 'static,
) -> jj_lib::backend::BackendError {
    jj_lib::backend::BackendError::Other(Box::new(error))
}
