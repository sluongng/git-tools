//! Local authoritative I/O boundary. A descriptor is only a claim about an
//! object, never proof of residency, durability, verification or retention.
use crate::*;
use futures::AsyncReadExt;
use std::{
    collections::BTreeSet,
    fs,
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use tinytree_store::Store;

/// Owned canonical metadata whose exact descriptor/hash/bytes were checked.
/// Construction does not prove remote retention or local authoritative residency.
pub struct VerifiedMetadata {
    descriptor: m::Descriptor,
    bytes: Vec<u8>,
    object: m::Object,
}
impl fmt::Debug for VerifiedMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedMetadata")
            .field("kind", &self.descriptor.key.kind)
            .field("size_bytes", &self.descriptor.size_bytes)
            .finish_non_exhaustive()
    }
}
impl VerifiedMetadata {
    pub fn verify(
        descriptor: m::Descriptor,
        bytes: Vec<u8>,
        limits: m::Limits,
    ) -> BackendResult<Self> {
        if descriptor.key.kind == m::Kind::File {
            return Err(invalid("files require verified pinned streaming sources"));
        }
        m::verify(&descriptor, &bytes, limits).map_err(other)?;
        let object = m::decode(&bytes, limits).map_err(other)?;
        Ok(Self {
            descriptor,
            bytes,
            object,
        })
    }
    pub fn descriptor(&self) -> m::Descriptor {
        self.descriptor
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn into_object(self) -> m::Object {
        self.object
    }
}
/// A provider-attested preverified, immutable, pinned byte source. Unlike a
/// descriptor response this conveys an actual readable local source lifetime.
/// No remote promise/placeholder is a valid implementation of this contract.
pub struct FileRead {
    descriptor: m::Descriptor,
    reader: Pin<Box<dyn AsyncRead + Send>>,
}
impl fmt::Debug for FileRead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileRead")
            .field("size_bytes", &self.descriptor.size_bytes)
            .finish_non_exhaustive()
    }
}
impl FileRead {
    /// This is NOT a verification function. Implementors must first verify ALL
    /// bytes and retain immutable/pinned ownership through EOF/drop, including
    /// cancelled blocking jobs. Future broker hydration must finish first.
    pub fn from_preverified_pinned_source(
        descriptor: m::Descriptor,
        reader: Pin<Box<dyn AsyncRead + Send>>,
    ) -> BackendResult<Self> {
        if descriptor.key.kind != m::Kind::File || descriptor.format_version != m::FORMAT_VERSION {
            return Err(invalid("invalid native file source descriptor"));
        }
        Ok(Self { descriptor, reader })
    }
    pub fn into_parts(self) -> (m::Descriptor, Pin<Box<dyn AsyncRead + Send>>) {
        (self.descriptor, self.reader)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct CopyCatalogLimits {
    pub max_objects: usize,
    pub max_metadata_bytes: usize,
    pub max_edges: usize,
}
/// A COMPLETE bounded consistent view, not one independently timed page. Owned
/// verified metadata remains usable after the provider's snapshot gate drops.
pub struct CopyCatalogSnapshot {
    entries: Vec<VerifiedMetadata>,
}
impl fmt::Debug for CopyCatalogSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyCatalogSnapshot")
            .field("entries", &self.entries.len())
            .finish_non_exhaustive()
    }
}
impl CopyCatalogSnapshot {
    /// Shape/bounds verification is separate from the provider's obligation to
    /// take one cross-writer-consistent view. No client mutex substitutes for it.
    pub fn from_verified(
        entries: Vec<VerifiedMetadata>,
        limits: CopyCatalogLimits,
    ) -> BackendResult<Self> {
        let mut bytes = 0usize;
        let mut edges = 0usize;
        let mut previous = None;
        if entries.len() > limits.max_objects {
            return Err(invalid("native copy catalog budget exceeded"));
        }
        for entry in &entries {
            let descriptor = entry.descriptor();
            if descriptor.key.kind != m::Kind::CopyHistory
                || previous.is_some_and(|id| id >= descriptor.key.id)
            {
                return Err(invalid("invalid native copy catalog order or kind"));
            }
            previous = Some(descriptor.key.id);
            bytes = bytes
                .checked_add(usize::try_from(descriptor.size_bytes).map_err(other)?)
                .ok_or_else(|| invalid("native copy catalog bytes exceeded"))?;
            let m::Object::CopyHistory(copy) = &entry.object else {
                return Err(invalid("native copy catalog kind mismatch"));
            };
            edges = edges
                .checked_add(copy.parents.len())
                .ok_or_else(|| invalid("native copy catalog edges exceeded"))?;
            if bytes > limits.max_metadata_bytes || edges > limits.max_edges {
                return Err(invalid("native copy catalog budget exceeded"));
            }
        }
        Ok(Self { entries })
    }
    pub fn into_entries(self) -> Vec<VerifiedMetadata> {
        self.entries
    }
}
/// Peak physical work retained by one metadata operation, including dependency
/// hydration, decoding, and temporary staging across background jobs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectIoResources {
    pub memory_bytes: u64,
    pub temporary_bytes: u64,
}
/// One instance is scoped to one authoritative repository; IDs do not select
/// another repository. Implementations own durable ACK semantics,
/// verified/pinned reads, aggregate staging admission and cross-writer catalog
/// coordination. There is deliberately no upload/offload/retention/network API.
#[async_trait]
pub trait NativeObjectIo: Send + Sync + fmt::Debug {
    /// None promises descriptor/metadata reads fit direct one-object bounds and
    /// catalog reads fit caller-supplied catalog bounds, without transitive work.
    /// Providers doing dependency hydration must declare the whole workset here.
    fn metadata_work_budget(&self) -> Option<ObjectIoResources> {
        None
    }
    /// Like owned metadata reads, retain the owner through physical work after cancellation.
    async fn descriptor_with_owner(
        &self,
        _key: m::ObjectKey,
        _owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<Option<m::Descriptor>> {
        Err(other(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "native provider does not support owned descriptor reads",
        )))
    }
    /// Like owned metadata reads, retain the owner through physical work after cancellation.
    async fn copy_catalog_snapshot_with_owner(
        &self,
        _limits: CopyCatalogLimits,
        _owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<CopyCatalogSnapshot> {
        Err(other(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "native provider does not support owned catalog reads",
        )))
    }
    /// Metadata claim only: not a read lease, residency proof or durable ACK.
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>>;
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata>;
    /// A source-load read whose resource/lifetime owner follows every physical
    /// read and verification job, including work left after caller cancellation.
    /// Providers must opt in: retaining the owner only in the awaiting future
    /// does not satisfy this contract when they detach background work.
    async fn read_metadata_with_owner(
        &self,
        _key: m::ObjectKey,
        _owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<VerifiedMetadata> {
        Err(other(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "native provider does not support owned metadata reads",
        )))
    }
    async fn write_metadata(&self, object: m::Object) -> BackendResult<m::Descriptor>;
    async fn read_file(&self, id: m::FileId) -> BackendResult<FileRead>;
    /// Delivery into an already admitted cache fetch. Implementations MUST NOT
    /// hydrate through that cache (including under another artifact scope).
    /// `owner` must follow physical work and pinned readers past cancellation.
    /// Opt in only when this non-recursive delivery contract is implemented.
    async fn read_file_for_cache(
        &self,
        _id: m::FileId,
        _owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<FileRead> {
        Err(other(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "native provider does not support non-recursive cache delivery",
        )))
    }

    async fn write_file(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
    ) -> BackendResult<m::FileId>;
    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot>;
}
/// Direct same-process provider. It retains the exclusive Store owner lock;
/// this does not implement the future multi-process authoritative I/O broker.
pub struct DirectObjectIo {
    store: Arc<Store>,
    path: PathBuf,
    config: Config,
    copy_gate: Arc<tokio::sync::Mutex<()>>,
    staging_bytes: Arc<tokio::sync::Semaphore>,
    staging_uploads: Arc<tokio::sync::Semaphore>,
    #[cfg(test)]
    metadata_put_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}
// Field order is part of the admission contract: destroy payloads and their
// physical storage/staging owners before releasing the external operation owner.
type MetadataOwner = Arc<dyn std::any::Any + Send + Sync>;
struct MetadataEncodeInput {
    object: Option<m::Object>,
    copy_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    owner: Option<MetadataOwner>,
}
struct MetadataEncoded {
    encoded: m::EncodedObject,
    copy_parents: BTreeSet<m::CopyId>,
    copy_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    owner: MetadataOwner,
}
struct MetadataPutJob {
    encoded: m::EncodedObject,
    copy_parents: BTreeSet<m::CopyId>,
    store: Arc<Store>,
    _reservation: (
        tokio::sync::OwnedSemaphorePermit,
        tokio::sync::OwnedSemaphorePermit,
    ),
    _copy_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    #[cfg(test)]
    hook: Option<Arc<dyn Fn() + Send + Sync>>,
    _owner: MetadataOwner,
}
impl fmt::Debug for DirectObjectIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectObjectIo").finish_non_exhaustive()
    }
}
impl DirectObjectIo {
    #[cfg(test)]
    pub(crate) fn set_metadata_put_hook(&mut self, hook: Arc<dyn Fn() + Send + Sync>) {
        self.metadata_put_hook = Some(hook);
    }
    /// Install fetched or authored immutable bytes while retaining the caller's
    /// complete admission/lifetime owner through actual blocking encode/put.
    /// This does not publish operation roots or create retention authority.
    pub async fn write_metadata_with_owner(
        &self,
        object: m::Object,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<m::Descriptor> {
        let mut input = MetadataEncodeInput {
            object: Some(object),
            copy_guard: None,
            owner: Some(owner),
        };
        if matches!(input.object.as_ref(), Some(m::Object::CopyHistory(_))) {
            input.copy_guard = Some(self.copy_gate.clone().lock_owned().await);
        }
        let limits = self.config.store.model_limits;
        let encoded = io::blocking(move || {
            // Move the whole aggregate, preventing disjoint closure captures
            // from selecting a different field destruction order.
            let mut input = input;
            let object = input.object.as_ref().expect("owned metadata input");
            let encoded = m::encode(object, limits).map_err(other)?;
            let copy_parents = match object {
                m::Object::CopyHistory(copy) => copy.parents.iter().copied().collect(),
                _ => BTreeSet::new(),
            };
            // Drop decoded input before transferring the owner to the result.
            // A cancelled join waiter can leave this result owned by Tokio.
            drop(input.object.take());
            Ok(MetadataEncoded {
                encoded,
                copy_parents,
                copy_guard: input.copy_guard.take(),
                owner: input.owner.take().expect("owned metadata admission"),
            })
        })
        .await?;
        let reservation = self
            .reserve_staging(encoded.encoded.descriptor.size_bytes)
            .await?;
        let job = MetadataPutJob {
            encoded: encoded.encoded,
            copy_parents: encoded.copy_parents,
            store: self.store.clone(),
            _reservation: reservation,
            _copy_guard: encoded.copy_guard,
            #[cfg(test)]
            hook: self.metadata_put_hook.clone(),
            _owner: encoded.owner,
        };
        io::blocking(move || {
            let job = job;
            #[cfg(test)]
            if let Some(hook) = &job.hook {
                hook();
            }
            for id in &job.copy_parents {
                let key = m::ObjectKey {
                    kind: m::Kind::CopyHistory,
                    id: m::ObjectId(id.0),
                };
                let (descriptor, bytes) =
                    job.store.read_metadata(REPOSITORY, key).map_err(other)?;
                VerifiedMetadata::verify(descriptor, bytes, limits)?;
            }
            job.store
                .put(
                    REPOSITORY,
                    job.encoded.descriptor,
                    &job.encoded.bytes,
                    now()?,
                )
                .map_err(other)?;
            Ok(job.encoded.descriptor)
        })
        .await
    }

    pub fn init(path: &Path, config: Config) -> BackendResult<Self> {
        Self::open(path, config, true)
    }
    pub fn load(path: &Path, config: Config) -> BackendResult<Self> {
        Self::open(path, config, false)
    }
    fn open(path: &Path, config: Config, fresh: bool) -> BackendResult<Self> {
        if config.store.max_staging_bytes > u32::MAX as u64
            || config.store.max_staging_uploads > tokio::sync::Semaphore::MAX_PERMITS
            || config
                .store
                .max_file_bytes
                .checked_mul(2)
                .is_none_or(|n| n > config.store.max_staging_bytes)
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
                .and_then(|f| f.sync_all())
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
        let (descriptor, bytes) = store
            .read_metadata(REPOSITORY, empty.descriptor.key)
            .map_err(other)?;
        let verified = VerifiedMetadata::verify(descriptor, bytes, config.store.model_limits)?;
        if verified.descriptor() != empty.descriptor || verified.bytes() != empty.bytes {
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
            copy_gate: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(test)]
            metadata_put_hook: None,
        })
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
}
#[async_trait]
impl NativeObjectIo for DirectObjectIo {
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        self.descriptor_with_owner(key, Arc::new(())).await
    }
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<Option<m::Descriptor>> {
        let store = self.store.clone();
        io::blocking(move || {
            let _owner = owner;
            store.descriptor(REPOSITORY, key).map_err(other)
        })
        .await
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.read_metadata_with_owner(key, Arc::new(())).await
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<VerifiedMetadata> {
        let store = self.store.clone();
        let limits = self.config.store.model_limits;
        io::blocking(move || {
            let _owner = owner;
            let (descriptor, bytes) = store.read_metadata(REPOSITORY, key).map_err(other)?;
            VerifiedMetadata::verify(descriptor, bytes, limits)
        })
        .await
    }
    async fn write_metadata(&self, object: m::Object) -> BackendResult<m::Descriptor> {
        self.write_metadata_with_owner(object, Arc::new(())).await
    }
    async fn read_file(&self, id: m::FileId) -> BackendResult<FileRead> {
        let store = self.store.clone();
        let key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(id.0),
        };
        let lease = io::blocking(move || store.read(REPOSITORY, key).map_err(other)).await?;
        let descriptor = lease.descriptor();
        if descriptor.size_bytes > self.config.store.max_file_bytes {
            return Err(invalid("native file exceeds configured limit"));
        }
        FileRead::from_preverified_pinned_source(descriptor, Box::pin(io::Reader::new(lease)))
    }
    async fn read_file_for_cache(
        &self,
        id: m::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<FileRead> {
        let store = self.store.clone();
        let key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(id.0),
        };
        let (lease, owner) = io::blocking(move || {
            let result = store.read(REPOSITORY, key).map_err(other);
            result.map(|lease| (lease, owner))
        })
        .await?;
        let descriptor = lease.descriptor();
        if descriptor.size_bytes > self.config.store.max_file_bytes {
            return Err(invalid("native file exceeds configured limit"));
        }
        FileRead::from_preverified_pinned_source(
            descriptor,
            Box::pin(io::Reader::for_delivery(lease, owner)),
        )
    }
    async fn write_file(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
    ) -> BackendResult<m::FileId> {
        let max_bytes = max_bytes.min(self.config.store.max_file_bytes);
        let reservation = self
            .reserve_staging(
                max_bytes
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
            let count = input.read(&mut buffer).await.map_err(other)?;
            if count == 0 {
                break;
            }
            size = size
                .checked_add(count as u64)
                .ok_or_else(|| invalid("native file size overflow"))?;
            if size > max_bytes {
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
        Ok(m::FileId(id))
    }
    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot> {
        self.copy_catalog_snapshot_with_owner(limits, Arc::new(()))
            .await
    }
    async fn copy_catalog_snapshot_with_owner(
        &self,
        limits: CopyCatalogLimits,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<CopyCatalogSnapshot> {
        let limits = CopyCatalogLimits {
            max_objects: limits
                .max_objects
                .min(self.config.max_copy_histories)
                .min(self.config.max_query_objects),
            max_metadata_bytes: limits
                .max_metadata_bytes
                .min(self.config.max_query_metadata_bytes),
            max_edges: limits.max_edges.min(self.config.store.max_closure_edges),
        };
        let guard = self.copy_gate.clone().lock_owned().await;
        let store = self.store.clone();
        let model_limits = self.config.store.model_limits;
        io::blocking(move || {
            let _owner = owner;
            let _guard = guard;
            let mut entries = Vec::new();
            let mut after = None;
            let mut bytes = 0usize;
            loop {
                let page = store
                    .list_objects(REPOSITORY, m::Kind::CopyHistory, after, 1024)
                    .map_err(other)?;
                if page.is_empty() {
                    break;
                }
                for descriptor in &page {
                    if entries.len() >= limits.max_objects {
                        return Err(invalid("native copy catalog budget exceeded"));
                    }
                    bytes = bytes
                        .checked_add(usize::try_from(descriptor.size_bytes).map_err(other)?)
                        .ok_or_else(|| invalid("native copy catalog bytes exceeded"))?;
                    if bytes > limits.max_metadata_bytes {
                        return Err(invalid("native copy catalog bytes exceeded"));
                    }
                    let (descriptor, bytes) = store
                        .read_metadata(REPOSITORY, descriptor.key)
                        .map_err(other)?;
                    entries.push(VerifiedMetadata::verify(descriptor, bytes, model_limits)?);
                }
                after = page.last().map(|d| d.key.id);
            }
            CopyCatalogSnapshot::from_verified(entries, limits)
        })
        .await
    }
}
