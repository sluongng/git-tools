//! Lossless native jj operation/view recovery objects, not a cloud service.
//!
//! Semantic operation/view IDs and raw transfer hashes occupy different domains.
//! Callers own authorization, checkpoint fencing, root retention and atomic
//! installation of the restored workspace. Importing these objects alone does
//! not publish a workspace or authorize deleting a sole local copy.
mod manifest;
pub use manifest::*;

use jj_lib::{
    backend::CommitId,
    object_id::ObjectId as _,
    op_store::{OpStore, Operation, OperationId, RefTarget, View, ViewId},
    simple_op_store::{SimpleOpStore, decode_cloud_operation, decode_cloud_view},
};
use std::{collections::BTreeSet, io};

pub use jj_lib::simple_op_store::CloudOpLimits as Limits;
pub const CODEC: &str = "jj-simple-op-store-0.46-v1";
pub const NATIVE_COMMIT_ID_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ObjectId {
    Operation([u8; 64]),
    View([u8; 64]),
}
impl ObjectId {
    pub fn is_intrinsic(self) -> bool {
        self.bytes().iter().all(|byte| *byte == 0)
    }
    pub fn bytes(&self) -> &[u8; 64] {
        match self {
            Self::Operation(id) | Self::View(id) => id,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobDigest {
    pub blake3: [u8; 32],
    pub size_bytes: u64,
}
impl BlobDigest {
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            blake3: *blake3::hash(bytes).as_bytes(),
            size_bytes: bytes.len() as u64,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dependencies {
    /// Includes intrinsic zero dependencies; callers recognize them as markers.
    pub operations: Vec<[u8; 64]>,
    pub views: Vec<[u8; 64]>,
    pub native_commits: Vec<[u8; NATIVE_COMMIT_ID_BYTES]>,
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid native operation-cloud object: {0}")]
    Invalid(&'static str),
    #[error("native operation-cloud resource limit: {0}")]
    Limit(&'static str),
    #[error("unsupported native operation-cloud feature: {0}")]
    Unsupported(&'static str),
    #[error("operation-cloud codec: {0}")]
    Codec(#[from] io::Error),
    #[error("operation-store access failed")]
    Store(#[source] jj_lib::op_store::OpStoreError),
}
pub type Result<T> = std::result::Result<T, Error>;

enum State {
    Operation(Operation),
    View(View),
}
/// Owned validated recovery data. Its private state cannot be swapped after verification.
pub struct VerifiedObject {
    id: ObjectId,
    bytes: Option<Vec<u8>>,
    digest: Option<BlobDigest>,
    state: State,
    dependencies: Dependencies,
    limits: Limits,
}
impl std::fmt::Debug for VerifiedObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedObject")
            .field("id", &self.id)
            .field("digest", &self.digest)
            .field("dependencies", &self.dependencies)
            .finish()
    }
}
impl VerifiedObject {
    pub fn id(&self) -> ObjectId {
        self.id
    }
    pub fn payload(&self) -> Option<&[u8]> {
        self.bytes.as_deref()
    }
    pub fn digest(&self) -> Option<BlobDigest> {
        self.digest
    }
    pub fn dependencies(&self) -> &Dependencies {
        &self.dependencies
    }
    pub fn operation(&self) -> Option<&Operation> {
        match &self.state {
            State::Operation(value) => Some(value),
            _ => None,
        }
    }
    pub fn view(&self) -> Option<&View> {
        match &self.state {
            State::View(value) => Some(value),
            _ => None,
        }
    }
    /// Decode an independently selected semantic object and verify its raw transfer digest.
    pub fn verify(id: ObjectId, digest: BlobDigest, bytes: &[u8], limits: Limits) -> Result<Self> {
        native_limits(limits)?;
        if id.is_intrinsic() {
            return Err(Error::Invalid("intrinsic objects have no payload"));
        }
        if bytes.len() > limits.max_payload_bytes || BlobDigest::of(bytes) != digest {
            return Err(Error::Invalid("raw payload digest or size mismatch"));
        }
        let state = match id {
            ObjectId::Operation(raw) => State::Operation(decode_cloud_operation(
                &OperationId::from_bytes(&raw),
                bytes,
                limits,
            )?),
            ObjectId::View(raw) => {
                State::View(decode_cloud_view(&ViewId::from_bytes(&raw), bytes, limits)?)
            }
        };
        let dependencies = dependencies(&state)?;
        Ok(Self {
            id,
            bytes: Some(bytes.to_vec()),
            digest: Some(digest),
            state,
            dependencies,
            limits,
        })
    }
    /// Construct the exact native synthetic root without inventing a stored hash.
    pub fn intrinsic(id: ObjectId) -> Result<Self> {
        if !id.is_intrinsic() {
            return Err(Error::Invalid("nonzero intrinsic identity"));
        }
        let state = match id {
            ObjectId::Operation(_) => {
                State::Operation(Operation::make_root(ViewId::from_bytes(&[0; 64])))
            }
            ObjectId::View(_) => State::View(View::make_root(CommitId::from_bytes(&[0; 32]))),
        };
        let dependencies = dependencies(&state)?;
        Ok(Self {
            id,
            bytes: None,
            digest: None,
            state,
            dependencies,
            limits: Limits::default(),
        })
    }
    /// Install through the real OpStore writer and sync before returning success.
    ///
    /// This does not check dependency residency, advance operation heads, install a
    /// working copy or fence concurrent commands. The caller must do those under
    /// its restore transaction. A sync failure is an error, never a durable ACK.
    pub async fn import(&self, store: &SimpleOpStore) -> Result<ObjectId> {
        native_store(store).await?;
        if self.id.is_intrinsic() {
            return Ok(self.id);
        }
        let actual = match &self.state {
            State::Operation(operation) => {
                let expected = OperationId::from_bytes(self.id.bytes());
                // Unix OpStore writers replace existing files. Never turn a corrupt
                // slot into an acknowledged import by silently overwriting it.
                if store.cloud_operation_is_present(&expected, self.limits)? {
                    store.sync_cloud_operation(&expected)?;
                    return Ok(self.id);
                }
                let id = store
                    .write_operation(operation)
                    .await
                    .map_err(Error::Store)?;
                let actual = ObjectId::Operation(array(id.as_bytes())?);
                if actual != self.id {
                    return Err(Error::Invalid("import changed operation identity"));
                }
                // The writer may reuse an existing content-addressed file.
                // Re-read it before ACK so corrupt existing bytes are not trusted.
                store.read_cloud_operation_payload(&id, self.limits)?;
                store.sync_cloud_operation(&id)?;
                actual
            }
            State::View(view) => {
                let expected = ViewId::from_bytes(self.id.bytes());
                if store.cloud_view_is_present(&expected, self.limits)? {
                    store.sync_cloud_view(&expected)?;
                    return Ok(self.id);
                }
                let id = store.write_view(view).await.map_err(Error::Store)?;
                let actual = ObjectId::View(array(id.as_bytes())?);
                if actual != self.id {
                    return Err(Error::Invalid("import changed view identity"));
                }
                store.read_cloud_view_payload(&id, self.limits)?;
                store.sync_cloud_view(&id)?;
                actual
            }
        };
        Ok(actual)
    }
}
fn native_limits(limits: Limits) -> Result<()> {
    limits.validate()?;
    if limits.commit_id_bytes != NATIVE_COMMIT_ID_BYTES {
        return Err(Error::Invalid(
            "codec requires native 32-byte commit identities",
        ));
    }
    Ok(())
}
fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| Error::Invalid("identity width mismatch"))
}
async fn native_store(store: &SimpleOpStore) -> Result<()> {
    let root = store
        .read_view(&ViewId::from_bytes(&[0; 64]))
        .await
        .map_err(Error::Store)?;
    if root != View::make_root(CommitId::from_bytes(&[0; NATIVE_COMMIT_ID_BYTES])) {
        return Err(Error::Invalid("operation store is not native-rooted"));
    }
    Ok(())
}
/// Export a bounded operation using the pinned upstream codec and semantic identity.
pub async fn export_operation(
    store: &SimpleOpStore,
    id: &OperationId,
    limits: Limits,
) -> Result<VerifiedObject> {
    native_limits(limits)?;
    native_store(store).await?;
    let key = ObjectId::Operation(array(id.as_bytes())?);
    if key.is_intrinsic() {
        return VerifiedObject::intrinsic(key);
    }
    let bytes = store.read_cloud_operation_payload(id, limits)?;
    VerifiedObject::verify(key, BlobDigest::of(&bytes), &bytes, limits)
}
/// Export a bounded view using the pinned upstream codec and semantic identity.
pub async fn export_view(
    store: &SimpleOpStore,
    id: &ViewId,
    limits: Limits,
) -> Result<VerifiedObject> {
    native_limits(limits)?;
    native_store(store).await?;
    let key = ObjectId::View(array(id.as_bytes())?);
    if key.is_intrinsic() {
        return VerifiedObject::intrinsic(key);
    }
    let bytes = store.read_cloud_view_payload(id, limits)?;
    VerifiedObject::verify(key, BlobDigest::of(&bytes), &bytes, limits)
}
fn dependencies(state: &State) -> Result<Dependencies> {
    let mut operations = BTreeSet::new();
    let mut views = BTreeSet::new();
    let mut commits = BTreeSet::new();
    match state {
        State::Operation(operation) => {
            for id in &operation.parents {
                operations.insert(array(id.as_bytes())?);
            }
            views.insert(array(operation.view_id.as_bytes())?);
            if let Some(predecessors) = &operation.commit_predecessors {
                for (id, parents) in predecessors {
                    commits.insert(array(id.as_bytes())?);
                    for parent in parents {
                        commits.insert(array(parent.as_bytes())?);
                    }
                }
            }
        }
        State::View(view) => {
            for id in &view.head_ids {
                commits.insert(array(id.as_bytes())?);
            }
            for id in view.wc_commit_ids.values() {
                commits.insert(array(id.as_bytes())?);
            }
            let mut target = |value: &RefTarget| -> Result<()> {
                for id in value.iter().flatten() {
                    commits.insert(array(id.as_bytes())?);
                }
                Ok(())
            };
            for value in view
                .local_bookmarks
                .values()
                .chain(view.local_tags.values())
            {
                target(value)?;
            }
            for value in view.git_refs.values().chain(view.git_heads.values()) {
                target(value)?;
            }
            for remote in view.remote_views.values() {
                for reference in remote.bookmarks.values().chain(remote.tags.values()) {
                    target(&reference.target)?;
                }
            }
        }
    }
    Ok(Dependencies {
        operations: operations.into_iter().collect(),
        views: views.into_iter().collect(),
        native_commits: commits.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests;
