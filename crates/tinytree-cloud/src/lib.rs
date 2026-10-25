//! Lossless native jj operation/view recovery objects, not a cloud service.
//!
//! Semantic operation/view IDs and raw transfer hashes occupy different domains.
//! Callers own authorization, checkpoint fencing, root retention and atomic
//! installation of the restored workspace. Importing these objects alone does
//! not publish a workspace or authorize deleting a sole local copy.
mod landing_recovery;
pub use landing_recovery::*;
mod capture;
pub use capture::*;
mod capture_manifest;
pub use capture_manifest::*;
mod index_artifact;
pub use index_artifact::*;
mod index_snapshot;
pub use index_snapshot::*;
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
    /// Conservative retained-allocation admission charge for this verified object.
    ///
    /// Includes the inline object, owned payload/dependency capacities and decoded
    /// native containers. This is not RSS or a bound on transient decoding work;
    /// callers must separately bound the worker which constructs the object.
    /// Opaque native buffers include a wire-backed reserve for retained capacity
    /// from overwritten protobuf fields. Revisit this estimate when changing the
    /// pinned decoder or native containers.
    pub fn retained_size_upper_bound(&self) -> Result<usize> {
        let mut charge = RetainedCharge(std::mem::size_of::<Self>());
        if let Some(bytes) = &self.bytes {
            charge.vector(bytes)?;
            // Prost can clear a large singular bytes/string field and overwrite
            // it with a shorter duplicate without shrinking the old allocation.
            // Distinct surviving opaque buffers consume disjoint byte-field
            // histories in the wire payload. Twice the whole wire length bounds
            // their retained growth capacity, independently of the raw copy.
            // Native conversions move these buffers; any extra clones are of
            // final logical contents and charged again by opaque()/target().
            charge.product(bytes.len(), 2)?;
        }
        charge.vector(&self.dependencies.operations)?;
        charge.vector(&self.dependencies.views)?;
        charge.vector(&self.dependencies.native_commits)?;
        match &self.state {
            State::Operation(operation) => {
                charge.opaque(operation.view_id.as_bytes().len())?;
                charge.vector(&operation.parents)?;
                for id in &operation.parents {
                    charge.opaque(id.as_bytes().len())?;
                }
                let metadata = &operation.metadata;
                charge.string(&metadata.description)?;
                charge.string(&metadata.hostname)?;
                charge.string(&metadata.username)?;
                if let Some(name) = &metadata.workspace_name {
                    charge.opaque(name.as_str().len())?;
                }
                charge.map(&metadata.attributes)?;
                for (key, value) in &metadata.attributes {
                    charge.string(key)?;
                    charge.string(value)?;
                }
                if let Some(predecessors) = &operation.commit_predecessors {
                    charge.map(predecessors)?;
                    for (id, parents) in predecessors {
                        charge.opaque(id.as_bytes().len())?;
                        charge.vector(parents)?;
                        for parent in parents {
                            charge.opaque(parent.as_bytes().len())?;
                        }
                    }
                }
            }
            State::View(view) => {
                // SwissTable buckets/control bytes, rounded above its public
                // capacity. No assumptions about the number of live heads.
                charge.product(
                    view.head_ids
                        .capacity()
                        .checked_add(1)
                        .ok_or(RETAINED_OVERFLOW)?,
                    2 * (std::mem::size_of::<CommitId>() + 16),
                )?;
                charge.add(256)?;
                for id in &view.head_ids {
                    charge.opaque(id.as_bytes().len())?;
                }
                charge.targets(&view.local_bookmarks)?;
                charge.targets(&view.local_tags)?;
                charge.targets(&view.git_refs)?;
                charge.targets(&view.git_heads)?;
                charge.map(&view.wc_commit_ids)?;
                for (name, id) in &view.wc_commit_ids {
                    charge.opaque(name.as_str().len())?;
                    charge.opaque(id.as_bytes().len())?;
                }
                charge.map(&view.remote_views)?;
                for (name, remote) in &view.remote_views {
                    charge.opaque(name.as_str().len())?;
                    for references in [&remote.bookmarks, &remote.tags] {
                        charge.map(references)?;
                        for (name, reference) in references {
                            charge.opaque(name.as_str().len())?;
                            charge.target(&reference.target)?;
                        }
                    }
                }
            }
        }
        Ok(charge.0)
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
    /// Install the exact verified payload and sync before returning success.
    ///
    /// This does not check dependency residency, advance operation heads, install a
    /// working copy or fence concurrent commands. The caller must do those under
    /// its restore transaction. A sync failure is an error, never a durable ACK.
    pub async fn import(&self, store: &SimpleOpStore) -> Result<ObjectId> {
        native_store(store).await?;
        if self.id.is_intrinsic() {
            return Ok(self.id);
        }
        let bytes = self
            .bytes
            .as_deref()
            .ok_or(Error::Invalid("verified payload missing"))?;
        match self.id {
            ObjectId::Operation(id) => store.import_cloud_operation_payload(
                &OperationId::from_bytes(&id),
                bytes,
                self.limits,
            )?,
            ObjectId::View(id) => {
                store.import_cloud_view_payload(&ViewId::from_bytes(&id), bytes, self.limits)?
            }
        }
        Ok(self.id)
    }
}
const RETAINED_OVERFLOW: Error = Error::Limit("retained allocation accounting overflow");
struct RetainedCharge(usize);
impl RetainedCharge {
    fn add(&mut self, bytes: usize) -> Result<()> {
        self.0 = self.0.checked_add(bytes).ok_or(RETAINED_OVERFLOW)?;
        Ok(())
    }
    fn product(&mut self, count: usize, bytes: usize) -> Result<()> {
        self.add(count.checked_mul(bytes).ok_or(RETAINED_OVERFLOW)?)
    }
    fn vector<T>(&mut self, values: &Vec<T>) -> Result<()> {
        self.product(values.capacity(), std::mem::size_of::<T>())?;
        self.add(32)
    }
    fn string(&mut self, value: &String) -> Result<()> {
        self.add(value.capacity())?;
        self.add(32)
    }
    fn opaque(&mut self, len: usize) -> Result<()> {
        // IDs/name capacities are private upstream. Charge logical contents and
        // minimum growth/allocator overhead here; retained high-water capacity
        // from overwritten protobuf fields has a separate wire-backed reserve.
        self.product(len, 2)?;
        self.add(32)
    }
    fn map<K, V>(&mut self, values: &std::collections::BTreeMap<K, V>) -> Result<()> {
        // Pinned std BTree nodes hold at most 11 entries. Charging every entry
        // (plus one) as a full 16-slot node also covers sparse internal nodes,
        // edges, alignment and allocator metadata without inspecting internals.
        let node = std::mem::size_of::<(K, V)>()
            .checked_mul(16)
            .and_then(|bytes| bytes.checked_add(256))
            .ok_or(RETAINED_OVERFLOW)?;
        self.product(values.len().checked_add(1).ok_or(RETAINED_OVERFLOW)?, node)
    }
    fn target(&mut self, target: &RefTarget) -> Result<()> {
        // The native Merge uses SmallVec<[Option<CommitId>; 1]>; the decoder
        // builds its exact ordered terms without subsequently simplifying them.
        self.product(
            target.as_slice().len(),
            2 * std::mem::size_of::<Option<CommitId>>(),
        )?;
        self.add(32)?;
        for id in target.iter().flatten() {
            self.opaque(id.as_bytes().len())?;
        }
        Ok(())
    }
    fn targets<K: AsRef<str>>(
        &mut self,
        values: &std::collections::BTreeMap<K, RefTarget>,
    ) -> Result<()> {
        self.map(values)?;
        for (name, target) in values {
            self.opaque(name.as_ref().len())?;
            self.target(target)?;
        }
        Ok(())
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
