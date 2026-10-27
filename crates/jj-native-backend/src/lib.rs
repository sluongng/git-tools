//! Native jj backend with no runtime Git dependency or automatic collection.
//! Explicit host-owned GC only reclaims unreachable local objects; no reachable
//! bytes are offloaded. Direct Store ownership is exclusive across processes;
//! shared clients require an authoritative I/O broker.
mod convert;
mod copies;
mod io;
mod maintenance;
pub use maintenance::{NativeClientGcError, NativeClientGcOptions};
mod provider;
pub use provider::{
    CopyCatalogLimits, CopyCatalogSnapshot, DirectObjectIo, FileRead, NativeObjectIo,
    ObjectIoResources, VerifiedMetadata,
};

use async_trait::async_trait;
use futures::{AsyncRead, stream::BoxStream};
use jj_lib::{
    backend::{
        Backend, BackendError, BackendInitError, BackendLoadError, BackendResult, ChangeId, Commit,
        CommitId, CopyHistory, CopyId, CopyRecord, FileId, RelatedCopy, SigningFn, SymlinkId, Tree,
        TreeId, TreeValue,
    },
    object_id::ObjectId as _,
    repo::StoreFactories,
    repo_path::{RepoPath, RepoPathBuf, RepoPathComponentBuf},
};
use std::{fmt, path::Path, pin::Pin, sync::Arc};
use tinytree_model as m;
use tinytree_store::Config as StoreConfig;

const REPOSITORY: &str = "local";
#[derive(Clone, Debug)]
pub struct Config {
    pub store: StoreConfig,
    pub max_graph_commits: usize,
    pub max_tree_entries: usize,
    pub max_copy_histories: usize,
    pub max_copy_records: usize,
    pub max_query_objects: usize,
    pub max_query_metadata_bytes: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            store: StoreConfig::default(),
            max_graph_commits: 100_000,
            max_tree_entries: 1_000_000,
            max_copy_histories: 100_000,
            max_copy_records: 100_000,
            max_query_objects: 100_000,
            max_query_metadata_bytes: 64 * 1024 * 1024,
        }
    }
}
pub struct NativeBackend {
    io: Arc<dyn NativeObjectIo>,
    config: Config,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
}
impl fmt::Debug for NativeBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeBackend").finish_non_exhaustive()
    }
}
pub(crate) fn other(error: impl std::error::Error + Send + Sync + 'static) -> BackendError {
    BackendError::Other(Box::new(error))
}
pub(crate) fn invalid(message: &'static str) -> BackendError {
    other(std::io::Error::other(message))
}
pub(crate) fn model_id(bytes: &[u8]) -> BackendResult<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| invalid("native object ID must contain 32 bytes"))
}
fn key(kind: m::Kind, bytes: &[u8]) -> BackendResult<m::ObjectKey> {
    Ok(m::ObjectKey {
        kind,
        id: m::ObjectId(model_id(bytes)?),
    })
}
fn now() -> BackendResult<i64> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(other)?
            .as_secs(),
    )
    .map_err(other)
}
/// Bounded source-checked private metadata; no FILE body is retained.
pub struct LocalAnnotationPath {
    pub parents: Vec<m::CommitId>,
    pub value: Option<m::TreeValue>,
    pub charged_bytes: usize,
    pub objects: usize,
}
impl NativeBackend {
    pub const NAME: &'static str = "tinytree-native-v1";
    pub fn init(path: &Path, config: Config) -> Result<Self, BackendInitError> {
        Self::validate_config(&config)
            .and_then(|()| {
                DirectObjectIo::init(path, config.clone())
                    .map(|io| Self::assemble(Arc::new(io), config))
            })
            .map_err(|error| BackendInitError(Box::new(error)))
    }
    pub fn load(path: &Path, config: Config) -> Result<Self, BackendLoadError> {
        Self::validate_config(&config)
            .and_then(|()| {
                DirectObjectIo::load(path, config.clone())
                    .map(|io| Self::assemble(Arc::new(io), config))
            })
            .map_err(|error| BackendLoadError(Box::new(error)))
    }
    fn validate_config(config: &Config) -> BackendResult<()> {
        if [
            config.max_graph_commits,
            config.max_tree_entries,
            config.max_copy_histories,
            config.max_copy_records,
            config.max_query_objects,
            config.max_query_metadata_bytes,
        ]
        .contains(&0)
        {
            return Err(invalid("native graph budgets must be positive"));
        }
        Ok(())
    }
    fn assemble(io: Arc<dyn NativeObjectIo>, config: Config) -> Self {
        let empty = m::empty_tree();
        Self {
            io,
            config,
            root_commit_id: CommitId::from_bytes(&[0; 32]),
            root_change_id: ChangeId::from_bytes(&[0; 16]),
            empty_tree_id: TreeId::from_bytes(&empty.descriptor.key.id.0),
        }
    }
    /// Attach to an authoritative provider only after checking ACTUAL verified
    /// empty-tree bytes. A descriptor promise alone cannot initialize a Backend.
    pub async fn from_io(io: Arc<dyn NativeObjectIo>, config: Config) -> BackendResult<Self> {
        Self::validate_config(&config)?;
        let empty = m::empty_tree();
        let verified = io.read_metadata(empty.descriptor.key).await?;
        if verified.descriptor() != empty.descriptor || verified.bytes() != empty.bytes {
            return Err(invalid("native provider empty tree mismatch"));
        }
        Ok(Self::assemble(io, config))
    }
    async fn read_object(&self, object: m::ObjectKey) -> BackendResult<m::Object> {
        let verified = self.io.read_metadata(object).await?;
        if verified.descriptor().key != object {
            return Err(invalid("native provider metadata key mismatch"));
        }
        Ok(verified.into_object())
    }
    async fn put_object(&self, object: m::Object) -> BackendResult<m::Descriptor> {
        self.io.write_metadata(object).await
    }
    /// Read a source-checked private path value only from resident local data.
    /// Provider opt-in fences the exact size before any spool/body work. This
    /// intentionally refuses unsupported/nonresident inputs rather than using
    /// generic canonical FILE authority or hydrating a different path.
    pub async fn local_annotation_file(
        &self, value:&m::TreeValue, max_bytes:u64,
        owner:Arc<dyn std::any::Any+Send+Sync>,
    )->BackendResult<Pin<Box<dyn AsyncRead+Send>>> {
        let m::TreeValue::File{id,size_bytes,..}=value else {return Err(invalid("annotation input is not a FILE"));};
        if *size_bytes>max_bytes {return Err(invalid("annotation FILE exceeds caller limit"));}
        let expected=m::Descriptor {key:m::ObjectKey{kind:m::Kind::File,id:m::ObjectId(id.0)},size_bytes:*size_bytes,format_version:m::FORMAT_VERSION};
        let read=self.io.read_local_file_bounded(expected,*size_bytes,owner).await?;
        let (actual,reader)=read.into_parts();
        if actual!=expected {return Err(invalid("bounded annotation FILE descriptor changed"));}
        Ok(reader)
    }
    /// Read one private COMMIT/path projection without reading any FILE body.
    /// The caller first establishes local operation-checkpoint membership. Every
    /// immutable descriptor is charged before metadata decode/clone; this does
    /// not grant canonical authority to graph overlays or arbitrary FILE IDs.
    pub async fn local_annotation_path(
        &self, id: m::CommitId, path: &[&str], max_charged_bytes: usize, max_objects: usize,
    ) -> BackendResult<LocalAnnotationPath> {
        if id == m::CommitId::ROOT || path.is_empty() || path.len() > 128 || path.iter().any(|p|p.is_empty() || *p == "." || *p == ".." || p.contains('/')) {
            return Err(invalid("invalid private annotation path"));
        }
        let mut charged = 0usize;
        let mut objects = 0usize;
        let mut read = async |key| {
            let descriptor = self.descriptor(key).await?;
            let amount = usize::try_from(descriptor.size_bytes).ok()
                .and_then(|n| n.checked_mul(128)).and_then(|n|n.checked_add(4096))
                .ok_or_else(|| invalid("private annotation metadata bound overflow"))?;
            charged = charged.checked_add(amount).filter(|n|*n<=max_charged_bytes)
                .ok_or_else(||invalid("private annotation metadata reservation exhausted"))?;
            objects = objects.checked_add(1).filter(|n|*n<=max_objects)
                .ok_or_else(||invalid("private annotation object reservation exhausted"))?;
            let verified = self.io.read_metadata(key).await?;
            if verified.descriptor() != descriptor {return Err(invalid("private annotation metadata descriptor changed"));}
            Ok::<_, BackendError>(verified.into_object())
        };
        let m::Object::Commit(commit) = read(m::ObjectKey {kind:m::Kind::Commit,id:m::ObjectId(id.0)}).await? else {
            return Err(invalid("private annotation COMMIT kind mismatch"));
        };
        let [tree] = commit.root_tree.terms.as_slice() else {return Err(invalid("private annotation conflicted tree unsupported"));};
        let mut tree = *tree;
        let mut value = None;
        for (n, part) in path.iter().enumerate() {
            let m::Object::Tree(entries) = read(m::ObjectKey {kind:m::Kind::Tree,id:m::ObjectId(tree.0)}).await? else {
                return Err(invalid("private annotation TREE kind mismatch"));
            };
            let entry = entries.entries.into_iter().find(|entry|entry.name==*part);
            let Some(entry) = entry else {break;};
            if n+1==path.len() {value=Some(entry.value);break;}
            let m::TreeValue::Tree(next)=entry.value else {break;};
            tree=next;
        }
        drop(read);
        Ok(LocalAnnotationPath {parents:commit.parents,value,charged_bytes:charged,objects})
    }
    async fn descriptor(&self, object: m::ObjectKey) -> BackendResult<m::Descriptor> {
        let descriptor = self
            .io
            .descriptor(object)
            .await?
            .ok_or_else(|| invalid("native referenced object is unavailable"))?;
        if descriptor.key != object || descriptor.format_version != m::FORMAT_VERSION {
            return Err(invalid("native provider descriptor mismatch"));
        }
        Ok(descriptor)
    }
    async fn model_commit(&self, id: m::CommitId) -> BackendResult<m::Commit> {
        if id == m::CommitId::ROOT {
            return Ok(m::synthetic_root());
        }
        match self
            .read_object(m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(id.0),
            })
            .await?
        {
            m::Object::Commit(value) => Ok(value),
            _ => Err(invalid("native commit kind mismatch")),
        }
    }
    async fn model_copy(&self, id: m::CopyId) -> BackendResult<m::CopyHistory> {
        match self
            .read_object(m::ObjectKey {
                kind: m::Kind::CopyHistory,
                id: m::ObjectId(id.0),
            })
            .await?
        {
            m::Object::CopyHistory(value) => Ok(value),
            _ => Err(invalid("native copy kind mismatch")),
        }
    }
}
/// Add the native loader without changing default local operation/index/
/// workspace factories. Initializers explicitly choose NativeBackend::init.
pub fn register(factories: &mut StoreFactories, config: Config) {
    factories.add_backend(
        NativeBackend::NAME,
        Box::new(move |_, path| Ok(Box::new(NativeBackend::load(path, config.clone())?))),
    );
}
#[async_trait]
impl Backend for NativeBackend {
    fn name(&self) -> &str {
        Self::NAME
    }
    fn commit_id_length(&self) -> usize {
        32
    }
    fn change_id_length(&self) -> usize {
        16
    }
    fn root_commit_id(&self) -> &CommitId {
        &self.root_commit_id
    }
    fn root_change_id(&self) -> &ChangeId {
        &self.root_change_id
    }
    fn empty_tree_id(&self) -> &TreeId {
        &self.empty_tree_id
    }
    fn concurrency(&self) -> usize {
        8
    }
    async fn read_file(
        &self,
        _: &RepoPath,
        id: &FileId,
    ) -> BackendResult<Pin<Box<dyn AsyncRead + Send>>> {
        let id = m::FileId(model_id(id.as_bytes())?);
        let (descriptor, reader) = self.io.read_file(id).await?.into_parts();
        if descriptor.key
            != (m::ObjectKey {
                kind: m::Kind::File,
                id: m::ObjectId(id.0),
            })
            || descriptor.size_bytes > self.config.store.max_file_bytes
        {
            return Err(invalid("native provider file descriptor mismatch or limit"));
        }
        Ok(reader)
    }
    async fn write_file(
        &self,
        _: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let id = self
            .io
            .write_file(contents, self.config.store.max_file_bytes)
            .await?;
        Ok(FileId::from_bytes(&id.0))
    }
    async fn read_symlink(&self, _: &RepoPath, id: &SymlinkId) -> BackendResult<String> {
        match self
            .read_object(key(m::Kind::Symlink, id.as_bytes())?)
            .await?
        {
            m::Object::Symlink(target) => Ok(target),
            _ => Err(invalid("native symlink kind mismatch")),
        }
    }
    async fn write_symlink(&self, _: &RepoPath, target: &str) -> BackendResult<SymlinkId> {
        Ok(SymlinkId::from_bytes(
            &self
                .put_object(m::Object::Symlink(target.into()))
                .await?
                .key
                .id
                .0,
        ))
    }
    async fn read_copy(&self, id: &CopyId) -> BackendResult<CopyHistory> {
        convert::copy(self.model_copy(m::CopyId(model_id(id.as_bytes())?)).await?)
    }
    async fn write_copy(&self, copy: &CopyHistory) -> BackendResult<CopyId> {
        Ok(CopyId::from_bytes(
            &self
                .put_object(m::Object::CopyHistory(convert::model_copy(copy)?))
                .await?
                .key
                .id
                .0,
        ))
    }
    async fn get_related_copies(&self, id: &CopyId) -> BackendResult<Vec<RelatedCopy>> {
        self.related_copies(m::CopyId(model_id(id.as_bytes())?))
            .await
    }
    async fn read_tree(&self, _: &RepoPath, id: &TreeId) -> BackendResult<Tree> {
        let m::Object::Tree(tree) = self.read_object(key(m::Kind::Tree, id.as_bytes())?).await?
        else {
            return Err(invalid("native tree kind mismatch"));
        };
        let entries = tree
            .entries
            .into_iter()
            .map(|entry| {
                let name = RepoPathComponentBuf::new(entry.name).map_err(other)?;
                let value = match entry.value {
                    m::TreeValue::File {
                        id,
                        executable,
                        copy_id,
                        ..
                    } => TreeValue::File {
                        id: FileId::from_bytes(&id.0),
                        executable,
                        copy_id: copy_id
                            .map_or_else(CopyId::placeholder, |id| CopyId::from_bytes(&id.0)),
                    },
                    m::TreeValue::Symlink { id, .. } => {
                        TreeValue::Symlink(SymlinkId::from_bytes(&id.0))
                    }
                    m::TreeValue::Tree(id) => TreeValue::Tree(TreeId::from_bytes(&id.0)),
                };
                Ok((name, value))
            })
            .collect::<BackendResult<Vec<_>>>()?;
        Ok(Tree::from_sorted_entries(entries))
    }
    async fn write_tree(&self, _: &RepoPath, tree: &Tree) -> BackendResult<TreeId> {
        let mut entries = Vec::new();
        for entry in tree.entries() {
            let value = match entry.value() {
                TreeValue::File {
                    id,
                    executable,
                    copy_id,
                } => {
                    let object = key(m::Kind::File, id.as_bytes())?;
                    let size = self.descriptor(object).await?.size_bytes;
                    m::TreeValue::File {
                        id: m::FileId(object.id.0),
                        executable: *executable,
                        size_bytes: size,
                        copy_id: if copy_id.as_bytes().is_empty() {
                            None
                        } else {
                            Some(m::CopyId(model_id(copy_id.as_bytes())?))
                        },
                    }
                }
                TreeValue::Symlink(id) => {
                    let target = self.read_symlink(RepoPath::root(), id).await?;
                    m::TreeValue::Symlink {
                        id: m::SymlinkId(model_id(id.as_bytes())?),
                        target_bytes: target.len() as u64,
                    }
                }
                TreeValue::Tree(id) => m::TreeValue::Tree(m::TreeId(model_id(id.as_bytes())?)),
                TreeValue::GitSubmodule(_) => {
                    return Err(BackendError::Unsupported(
                        m::Error::UnsupportedGitSubmodule.to_string(),
                    ));
                }
            };
            entries.push(m::TreeEntry {
                name: entry.name().as_internal_str().to_owned(),
                value,
            });
        }
        Ok(TreeId::from_bytes(
            &self
                .put_object(m::Object::Tree(m::Tree { entries }))
                .await?
                .key
                .id
                .0,
        ))
    }
    async fn read_commit(&self, id: &CommitId) -> BackendResult<Commit> {
        Ok(convert::commit(
            self.model_commit(m::CommitId(model_id(id.as_bytes())?))
                .await?,
        ))
    }
    async fn write_commit(
        &self,
        mut contents: Commit,
        sign_with: Option<&mut SigningFn>,
    ) -> BackendResult<(CommitId, Commit)> {
        let mut native = convert::model_commit(&contents)?;
        // Report the same semantic label normalization used by read_commit.
        contents.conflict_labels =
            jj_lib::conflict_labels::ConflictLabels::from_merge(contents.conflict_labels)
                .into_merge();
        if let Some(sign) = sign_with {
            let data =
                m::unsigned_commit_bytes(&native, self.config.store.model_limits).map_err(other)?;
            let sig = sign(&data).map_err(other)?;
            native.secure_signature = Some(m::SecureSignature {
                data: data.clone(),
                signature: sig.clone(),
            });
            contents.secure_sig = Some(jj_lib::backend::SecureSig { data, sig });
        }
        let descriptor = self.put_object(m::Object::Commit(native)).await?;
        Ok((CommitId::from_bytes(&descriptor.key.id.0), contents))
    }
    fn get_copy_records(
        &self,
        paths: Option<&[RepoPathBuf]>,
        root: &CommitId,
        head: &CommitId,
    ) -> BackendResult<BoxStream<'_, BackendResult<CopyRecord>>> {
        let root = m::CommitId(model_id(root.as_bytes())?);
        let head = m::CommitId(model_id(head.as_bytes())?);
        let paths = paths.map(<[RepoPathBuf]>::to_vec);
        Ok(Box::pin(
            async_stream::try_stream! { for record in self.copy_records(paths,root,head).await? { yield record; } },
        ))
    }
}

#[cfg(test)]
mod tests;
