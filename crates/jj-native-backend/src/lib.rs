//! Local-only native jj Backend prework. No Git dependency at runtime, remote
//! upload, eviction or GC. Store ownership is currently exclusive across
//! processes; final multi-process use requires a local authoritative I/O broker.
mod convert;
mod copies;
mod io;

use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, stream::BoxStream};
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
use std::{
    fmt, fs,
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};
use tinytree_model as m;
use tinytree_store::{Config as StoreConfig, Store};

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
    store: Arc<Store>,
    path: PathBuf,
    config: Config,
    root_commit_id: CommitId,
    root_change_id: ChangeId,
    empty_tree_id: TreeId,
    copy_gate: Arc<tokio::sync::Mutex<()>>,
    staging_bytes: Arc<tokio::sync::Semaphore>,
    staging_uploads: Arc<tokio::sync::Semaphore>,
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
impl NativeBackend {
    pub const NAME: &'static str = "tinytree-native-v1";
    pub fn init(path: &Path, config: Config) -> Result<Self, BackendInitError> {
        Self::open(path, config, true).map_err(|error| BackendInitError(Box::new(error)))
    }
    pub fn load(path: &Path, config: Config) -> Result<Self, BackendLoadError> {
        Self::open(path, config, false).map_err(|error| BackendLoadError(Box::new(error)))
    }
    fn open(path: &Path, config: Config, fresh: bool) -> BackendResult<Self> {
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
        if config.store.max_staging_bytes > u32::MAX as u64
            || config.store.max_staging_uploads > tokio::sync::Semaphore::MAX_PERMITS
            || config
                .store
                .max_file_bytes
                .checked_mul(2)
                .is_none_or(|size| size > config.store.max_staging_bytes)
        {
            return Err(invalid(
                "native staging requires room for both file spools and bounded semaphore counts",
            ));
        }
        let native = path.join("native-objects");
        let staging = path.join("native-file-staging");
        if fresh {
            if native.exists() || staging.exists() {
                return Err(invalid(
                    "native backend initialization requires fresh paths",
                ));
            }
            fs::create_dir(&staging).map_err(other)?;
            fs::File::open(path)
                .and_then(|file| file.sync_all())
                .map_err(other)?;
        } else if !native.is_dir() || !fs::symlink_metadata(&staging).map_err(other)?.is_dir() {
            return Err(invalid("native backend storage is missing"));
        }
        let store = if fresh {
            Store::open(native, config.store.clone())
        } else {
            Store::open_existing(native, config.store.clone())
        }
        .map_err(other)?;
        if fresh {
            store.create_repository(REPOSITORY).map_err(other)?;
        }
        let empty = m::empty_tree();
        if store
            .descriptor(REPOSITORY, empty.descriptor.key)
            .map_err(other)?
            != Some(empty.descriptor)
        {
            return Err(invalid("native empty tree unavailable"));
        }
        Ok(Self {
            store: Arc::new(store),
            path: staging,
            staging_bytes: Arc::new(tokio::sync::Semaphore::new(
                config.store.max_staging_bytes as usize,
            )),
            staging_uploads: Arc::new(tokio::sync::Semaphore::new(
                config.store.max_staging_uploads,
            )),
            config,
            root_commit_id: CommitId::from_bytes(&[0; 32]),
            root_change_id: ChangeId::from_bytes(&[0; 16]),
            empty_tree_id: TreeId::from_bytes(&empty.descriptor.key.id.0),
            copy_gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }
    async fn read_object(&self, object: m::ObjectKey) -> BackendResult<m::Object> {
        let store = self.store.clone();
        let limits = self.config.store.model_limits;
        io::blocking(move || {
            let (_, bytes) = store.read_metadata(REPOSITORY, object).map_err(other)?;
            m::decode(&bytes, limits).map_err(other)
        })
        .await
    }
    async fn reserve_staging(
        &self,
        bytes: u64,
    ) -> BackendResult<(
        tokio::sync::OwnedSemaphorePermit,
        tokio::sync::OwnedSemaphorePermit,
    )> {
        if bytes > self.config.store.max_staging_bytes {
            return Err(invalid("native staging budget exceeded"));
        }
        let slot = self
            .staging_uploads
            .clone()
            .acquire_owned()
            .await
            .map_err(other)?;
        let bytes = self
            .staging_bytes
            .clone()
            .acquire_many_owned(u32::try_from(bytes).map_err(other)?)
            .await
            .map_err(other)?;
        Ok((slot, bytes))
    }
    async fn put_object(&self, object: m::Object) -> BackendResult<m::Descriptor> {
        let copy_guard = if matches!(&object, m::Object::CopyHistory(_)) {
            Some(self.copy_gate.clone().lock_owned().await)
        } else {
            None
        };
        let limits = self.config.store.model_limits;
        let encoded = io::blocking(move || m::encode(&object, limits).map_err(other)).await?;
        let reservation = self.reserve_staging(encoded.descriptor.size_bytes).await?;
        let store = self.store.clone();
        io::blocking(move || {
            let _copy_guard = copy_guard;
            let _reservation = reservation;
            store
                .put(REPOSITORY, encoded.descriptor, &encoded.bytes, now()?)
                .map_err(other)?;
            Ok(encoded.descriptor)
        })
        .await
    }
    async fn descriptor(&self, object: m::ObjectKey) -> BackendResult<m::Descriptor> {
        let store = self.store.clone();
        io::blocking(move || {
            store
                .descriptor(REPOSITORY, object)
                .map_err(other)?
                .ok_or_else(|| invalid("native referenced object is unavailable"))
        })
        .await
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
        let object = key(m::Kind::File, id.as_bytes())?;
        let store = self.store.clone();
        let lease = io::blocking(move || store.read(REPOSITORY, object).map_err(other)).await?;
        if lease.descriptor().size_bytes > self.config.store.max_file_bytes {
            return Err(invalid("native file exceeds configured limit"));
        }
        Ok(Box::pin(io::Reader::new(lease)))
    }
    async fn write_file(
        &self,
        _: &RepoPath,
        contents: &mut (dyn AsyncRead + Send + Unpin),
    ) -> BackendResult<FileId> {
        let reservation = self
            .reserve_staging(
                self.config
                    .store
                    .max_file_bytes
                    .checked_mul(2)
                    .ok_or_else(|| invalid("native staging budget overflow"))?,
            )
            .await?;
        let path = self.path.clone();
        let mut temporary = io::blocking(move || {
            Ok((
                tempfile::NamedTempFile::new_in(path).map_err(other)?,
                reservation,
            ))
        })
        .await?;
        let mut hash = blake3::Hasher::new();
        let mut size = 0u64;
        let mut buffer = vec![0; 64 * 1024];
        loop {
            let count = contents.read(&mut buffer).await.map_err(other)?;
            if count == 0 {
                break;
            }
            size = size
                .checked_add(count as u64)
                .ok_or_else(|| invalid("native file size overflow"))?;
            if size > self.config.store.max_file_bytes {
                return Err(invalid("native file exceeds configured limit"));
            }
            hash.update(&buffer[..count]);
            let chunk = buffer[..count].to_vec();
            temporary = io::blocking(move || {
                temporary.0.write_all(&chunk).map_err(other)?;
                Ok(temporary)
            })
            .await?;
        }
        let id = *hash.finalize().as_bytes();
        let store = self.store.clone();
        io::blocking(move || {
            let mut staged = temporary;
            staged
                .0
                .as_file_mut()
                .seek(SeekFrom::Start(0))
                .map_err(other)?;
            let descriptor = m::Descriptor {
                key: m::ObjectKey {
                    kind: m::Kind::File,
                    id: m::ObjectId(id),
                },
                size_bytes: size,
                format_version: m::FORMAT_VERSION,
            };
            store
                .put_stream(REPOSITORY, descriptor, staged.0.as_file_mut(), now()?)
                .map_err(other)?;
            Ok(())
        })
        .await?;
        Ok(FileId::from_bytes(&id))
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
        for parent in &copy.parents {
            self.model_copy(m::CopyId(model_id(parent.as_bytes())?))
                .await?;
        }
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
