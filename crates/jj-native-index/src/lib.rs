//! A complete logical jj index backed by immutable checkpoints and broker scopes.
//! Persisted descriptors do not prove object residency or remote retention.
mod cleanup;
pub use cleanup::CleanupHandle;
mod local_export;
mod memo;
mod persistence;
mod portable;
pub use local_export::LocalIndexExportReader;
mod portable_import;
pub use portable_import::{PortableImportLimits, PortableImportReceipt, PortableImportVersion};
mod source;
pub use portable::{
    ExportArtifact, IndexArtifactSink, PortableExportLimits, PortableExportWork,
    PortableIndexExport,
};
use source::CheckpointSource;
mod prefix;
mod query;
mod revsets;
mod selected;
use async_trait::async_trait;
use jj_lib::{
    backend::{ChangeId, CommitId},
    commit::Commit,
    index::*,
    object_id::{HexPrefix, ObjectId as _, PrefixResolution},
    operation::Operation,
    repo_path::RepoPathBuf,
    revset::{ResolvedExpression, Revset, RevsetEvaluationError},
    store::Store,
};
use jj_native_backend::NativeObjectIo;
use native_broker::query_protocol as q;
use native_broker::{NativeQueryIo, QueryOwner};
use persistence::{SegmentId, Storage};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tinytree_model as m;

pub const NAME: &str = "tinytree-native-index-v1";
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "transport", content = "address", rename_all = "snake_case")]
pub enum Endpoint {
    UnixSocket(PathBuf),
    WindowsNamedPipe(String),
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub endpoint: Endpoint,
    pub store_alias: String,
    pub provider_fingerprint: [u8; 32],
}
impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        let endpoint = match &self.endpoint {
            Endpoint::UnixSocket(p) => native_broker::LocalEndpoint::UnixSocket(p.clone()),
            Endpoint::WindowsNamedPipe(n) => {
                native_broker::LocalEndpoint::WindowsNamedPipe(n.clone())
            }
        };
        endpoint.validate().map_err(|_| Error::Invalid)?;
        if self.store_alias.is_empty()
            || self.store_alias.len() > 128
            || !self
                .store_alias
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            || self.provider_fingerprint == [0; 32]
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct Limits {
    /// Shared immutable-answer cache. Either zero disables memoization.
    pub fact_cache_bytes: usize,
    pub fact_cache_entries: usize,
    pub max_segments: usize,
    pub max_roots: usize,
    pub max_drafts: usize,
    pub max_parents: usize,
    pub max_depth: usize,
    pub max_segment_bytes: usize,
    pub max_checkpoint_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_query_pages: usize,
    pub max_query_rows: usize,
    pub max_query_edges: usize,
    pub max_expression_nodes: usize,
    pub max_expression_depth: usize,
    pub page_size: u32,
    pub cleanup_capacity: usize,
    pub cleanup_timeout_ms: u64,
    /// Synchronous jj boolean query wait; accepted jobs keep owners after timeout.
    pub sync_query_timeout_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            fact_cache_bytes: 1024 * 1024,
            fact_cache_entries: 1024,
            max_segments: 4096,
            max_roots: 4096,
            max_drafts: 4096,
            max_parents: 4096,
            max_depth: 512,
            max_segment_bytes: 4 * 1024 * 1024,
            max_checkpoint_bytes: 16 * 1024 * 1024,
            max_metadata_bytes: 256 * 1024,
            max_query_pages: 4096,
            max_query_rows: 100_000,
            max_query_edges: 100_000,
            max_expression_nodes: 64,
            max_expression_depth: 32,
            page_size: 128,
            cleanup_capacity: 64,
            cleanup_timeout_ms: 5000,
            sync_query_timeout_ms: 120_000,
        }
    }
}
impl Limits {
    fn validate(&self) -> Result<(), Error> {
        if self.fact_cache_bytes > 64 * 1024 * 1024
            || self.fact_cache_entries > 65_536
            || self.max_segments == 0
            || self.max_segments > 65_536
            || self.max_roots == 0
            || self.max_roots > 4096
            || self.max_drafts == 0
            || self.max_drafts > 65_536
            || self.max_parents == 0
            || self.max_parents > 4096
            || self.max_depth == 0
            || self.max_depth > 65_536
            || self.max_segment_bytes < 128
            || self.max_segment_bytes > 64 * 1024 * 1024
            || self.max_checkpoint_bytes < self.max_segment_bytes
            || self.max_checkpoint_bytes > 256 * 1024 * 1024
            || self.max_metadata_bytes < 128
            || self.max_metadata_bytes > 16 * 1024 * 1024
            || self.max_query_pages == 0
            || self.max_query_pages > 100_000
            || self.max_query_rows == 0
            || self.max_query_rows > 1_000_000
            || self.max_query_edges == 0
            || self.max_query_edges > 1_000_000
            || self.max_expression_nodes == 0
            || self.max_expression_nodes > 1024
            || self.max_expression_depth == 0
            || self.max_expression_depth > 64
            || self.page_size == 0
            || self.page_size > 4096
            || self.cleanup_capacity == 0
            || self.cleanup_capacity > 4096
            || self.cleanup_timeout_ms == 0
            || self.cleanup_timeout_ms > 60_000
            || self.sync_query_timeout_ms == 0
            || self.sync_query_timeout_ms > 600_000
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid native index configuration or identity")]
    Invalid,
    #[error("native index metadata is corrupt")]
    Corrupt,
    #[error("unsupported native index format or expression")]
    Unsupported,
    #[error("unsupported native index expression: {0}")]
    UnsupportedExpression(&'static str),
    #[error("native index resource limit exceeded")]
    Limit,
    #[error("native index operation association is missing")]
    MissingOperation,
    #[error("index belongs to another native repository")]
    Foreign,
    #[error("native index storage failure")]
    Io(#[from] std::io::Error),
    #[error("native query failed ({0:?})")]
    Query(tonic::Code),
    #[error("native canonical commit verification failed")]
    Canonical,
}
fn err(e: Error) -> IndexError {
    IndexError::Other(Box::new(e))
}
fn rev_err(e: Error) -> RevsetEvaluationError {
    RevsetEvaluationError::Other(Box::new(e))
}
fn status(e: tonic::Status) -> Error {
    Error::Query(e.code())
}
fn model_id(id: &CommitId) -> Result<m::CommitId, Error> {
    Ok(m::CommitId(
        id.as_bytes().try_into().map_err(|_| Error::Invalid)?,
    ))
}
fn jj_id(id: m::CommitId) -> CommitId {
    CommitId::from_bytes(&id.0)
}
fn jj_change(id: m::ChangeId) -> ChangeId {
    ChangeId::from_bytes(&id.0)
}
fn random() -> [u8; 32] {
    let mut out = [0; 32];
    out[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    out[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    out
}
fn now() -> Result<i64, Error> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Invalid)?
        .as_secs()
        .try_into()
        .map_err(|_| Error::Invalid)?)
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Draft {
    order: u64,
    descriptor: m::Descriptor,
    change: m::ChangeId,
    parents: Vec<m::CommitId>,
}
impl Draft {
    fn canonical_eq(&self, other: &Self) -> bool {
        self.descriptor == other.descriptor
            && self.change == other.change
            && self.parents == other.parents
    }
    fn id(&self) -> m::CommitId {
        m::CommitId(self.descriptor.key.id.0)
    }
}
struct CachedScope {
    value: native_broker::QueryScope,
    _owner: QueryOwner,
    cleanup: cleanup::Queue,
    close_once: cleanup::CloseOnce,
    parent: Option<Arc<CachedScope>>,
    timeout: Duration,
}
struct ScopeState {
    cached: Option<Arc<CachedScope>>,
    root_only: Option<Arc<CachedScope>>,
    replay: [u8; 32],
}
struct Checkpoint {
    memo_id: [u8; 32],
    tips: Vec<SegmentId>,
    roots: Vec<m::CommitId>,
    drafts: Vec<Arc<Draft>>,
    source: CheckpointSource,
    baseline: BTreeSet<m::CommitId>,
    scope: tokio::sync::Mutex<ScopeState>,
}
impl Checkpoint {
    fn new(
        tips: Vec<SegmentId>,
        roots: Vec<m::CommitId>,
        drafts: Vec<Arc<Draft>>,
        source: CheckpointSource,
        baseline: BTreeSet<m::CommitId>,
    ) -> Self {
        Self {
            memo_id: source.memo_key(memo::checkpoint(&roots, &drafts)),
            source,
            baseline,
            tips,
            roots,
            drafts,
            scope: tokio::sync::Mutex::new(ScopeState {
                cached: None,
                root_only: None,
                replay: random(),
            }),
        }
    }
}
#[derive(Clone)]
pub struct NativeIndexStore {
    core: Arc<Core>,
}
struct Core {
    memo: memo::Memo,
    storage: Arc<Storage>,
    query: Arc<dyn NativeQueryIo>,
    objects: Arc<dyn NativeObjectIo>,
    limits: Limits,
    cleanup: cleanup::Queue,
}
impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeIndexContext").finish_non_exhaustive()
    }
}
impl std::fmt::Debug for NativeIndexStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeIndexStore").finish_non_exhaustive()
    }
}
impl Core {
    fn owner(&self) -> QueryOwner {
        Arc::new((self.storage.clone(), self.objects.clone()))
    }
    fn enqueue(&self, handle: [u8; 32], scope: bool, owner: QueryOwner) {
        self.cleanup.send(
            handle,
            scope,
            owner,
            Duration::from_millis(self.limits.cleanup_timeout_ms),
        );
    }
}
impl NativeIndexStore {
    /// Initialize a new private index. Roots are an explicit immutable seed,
    /// never inferred from repository refs or a fetched object subset.
    ///
    /// Query and object providers MUST own their I/O futures and channel tasks
    /// independently of the caller's runtime. CLI hosts inject the matching
    /// BrokerQueryClient/BrokerObjectIo, never a caller-runtime remote channel.
    /// This contract is required by jj's synchronous `Revset::is_empty` bridge.
    pub fn init(
        path: &Path,
        config: Config,
        limits: Limits,
        query: Arc<dyn NativeQueryIo>,
        objects: Arc<dyn NativeObjectIo>,
        roots: Vec<m::CommitId>,
    ) -> Result<Self, Error> {
        limits.validate()?;
        config.validate()?;
        let storage = Storage::init(path, config, &limits, roots)?;
        Self::with_storage(storage, limits, query, objects)
    }
    pub fn open(
        path: &Path,
        expected: &Config,
        limits: Limits,
        query: Arc<dyn NativeQueryIo>,
        objects: Arc<dyn NativeObjectIo>,
    ) -> Result<Self, Error> {
        limits.validate()?;
        expected.validate()?;
        Self::with_storage(
            Storage::open(path, expected, &limits)?,
            limits,
            query,
            objects,
        )
    }
    fn with_storage(
        storage: Storage,
        limits: Limits,
        query: Arc<dyn NativeQueryIo>,
        objects: Arc<dyn NativeObjectIo>,
    ) -> Result<Self, Error> {
        let cleanup = cleanup::Queue::start(query.clone(), &limits)?;
        Ok(Self {
            core: Arc::new(Core {
                memo: memo::Memo::new(limits.fact_cache_bytes, limits.fact_cache_entries),
                storage: Arc::new(storage),
                query,
                objects,
                limits,
                cleanup,
            }),
        })
    }
    pub fn cleanup_handle(&self) -> CleanupHandle {
        self.core.cleanup.handle()
    }
    /// Only for an initialization-journal-owned, proven-fresh empty directory.
    /// Existing stores, partial stores and foreign contents are never repaired.
    pub fn init_fresh_for_root_operation(
        path: &Path,
        config: Config,
        limits: Limits,
        query: Arc<dyn NativeQueryIo>,
        objects: Arc<dyn NativeObjectIo>,
        roots: Vec<m::CommitId>,
        root_operation: &[u8; 64],
    ) -> Result<Self, Error> {
        limits.validate()?;
        config.validate()?;
        if roots.len() > limits.max_roots {
            return Err(Error::Limit);
        }
        persistence::prepare_fresh_directory(path)?;
        let store = Self::init(path, config, limits, query, objects, roots)?;
        let seed = store.seed_index()?;
        store.associate_operation(root_operation, &seed)?;
        Ok(store)
    }
    pub fn seed_index(&self) -> Result<NativeReadonlyIndex, Error> {
        if self.core.storage.imported() {
            self.index_at_operation(&[0; 64])
        } else {
            self.core.load(vec![self.core.storage.seed()?])
        }
    }
    /// Exact 64-byte operation association. Missing associations fail closed.
    pub fn index_at_operation(&self, id: &[u8; 64]) -> Result<NativeReadonlyIndex, Error> {
        let (tips, source) = self.core.storage.association(id, &self.core.limits)?;
        self.core.load_source(tips, source)
    }
    /// Persist this compatible mutable checkpoint before associating an operation.
    /// This never publishes canonical objects or changes remote references.
    pub fn persist_index(
        &self,
        index: Box<dyn MutableIndex>,
    ) -> Result<NativeReadonlyIndex, Error> {
        let mutable = index
            .downcast::<NativeMutableIndex>()
            .ok_or(Error::Foreign)?;
        self.core.same(&mutable.inner.core)?;
        let tips = self.core.storage.persist_checkpoint(
            &mutable.parents,
            &[],
            &mutable.pending,
            &self.core.limits,
        )?;
        self.core
            .load_source(tips, mutable.inner.checkpoint.source.clone())
    }
    pub fn associate_operation(
        &self,
        id: &[u8; 64],
        index: &NativeReadonlyIndex,
    ) -> Result<(), Error> {
        self.core.same(&index.core)?;
        self.core.load_source(
            index.checkpoint.tips.clone(),
            index.checkpoint.source.clone(),
        )?;
        self.core.storage.associate_source(
            id,
            &index.checkpoint.tips,
            &index.checkpoint.source,
            &self.core.limits,
        )
    }
}
#[derive(Clone)]
pub struct NativeReadonlyIndex {
    core: Arc<Core>,
    checkpoint: Arc<Checkpoint>,
}
impl std::fmt::Debug for NativeReadonlyIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeReadonlyIndex")
            .field("roots", &self.checkpoint.roots.len())
            .field("drafts", &self.checkpoint.drafts.len())
            .finish()
    }
}
impl CachedScope {
    fn request_close(&self) {
        self.close_once.schedule(
            &self.cleanup,
            self.value.handle,
            self._owner.clone(),
            self.timeout,
        );
        if let Some(parent) = &self.parent {
            parent.request_close();
        }
    }
}
impl Drop for Checkpoint {
    fn drop(&mut self) {
        let state = self.scope.get_mut();
        if let Some(scope) = &state.cached {
            scope.request_close()
        }
        if let Some(scope) = &state.root_only {
            scope.request_close()
        }
    }
}
struct NativeMutableIndex {
    inner: NativeReadonlyIndex,
    parents: Vec<SegmentId>,
    pending: Vec<Arc<Draft>>,
}
impl Core {
    fn same(&self, other: &Core) -> Result<(), Error> {
        if self.storage.identity() != other.storage.identity()
            || self.storage.location() != other.storage.location()
        {
            Err(Error::Foreign)
        } else {
            Ok(())
        }
    }
    fn load(self: &Arc<Self>, tips: Vec<SegmentId>) -> Result<NativeReadonlyIndex, Error> {
        self.load_source(tips, CheckpointSource::LiveRoots)
    }
    fn load_source(
        self: &Arc<Self>,
        tips: Vec<SegmentId>,
        source: CheckpointSource,
    ) -> Result<NativeReadonlyIndex, Error> {
        let (roots, drafts) = self.storage.load(&tips, &self.limits)?;
        let baseline = source.validate_checkpoint(&self.storage, &roots, &drafts, &self.limits)?;
        Ok(NativeReadonlyIndex {
            core: self.clone(),
            checkpoint: Arc::new(Checkpoint::new(tips, roots, drafts, source, baseline)),
        })
    }
}
#[async_trait(?Send)]
impl IndexStore for NativeIndexStore {
    fn name(&self) -> &str {
        NAME
    }
    async fn get_index_at_op(
        &self,
        op: &Operation,
        _store: &Arc<Store>,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        let id = op
            .id()
            .as_bytes()
            .try_into()
            .map_err(|_| IndexStoreError::Read(Box::new(Error::Invalid)))?;
        self.index_at_operation(&id)
            .map(|i| Box::new(i) as Box<dyn ReadonlyIndex>)
            .map_err(|e| IndexStoreError::Read(Box::new(e)))
    }
    fn write_index(
        &self,
        index: Box<dyn MutableIndex>,
        op: &Operation,
    ) -> IndexStoreResult<Box<dyn ReadonlyIndex>> {
        let result = (|| {
            let ready = self.persist_index(index)?;
            let id = op.id().as_bytes().try_into().map_err(|_| Error::Invalid)?;
            self.associate_operation(&id, &ready)?;
            Ok(Box::new(ready) as Box<dyn ReadonlyIndex>)
        })();
        result.map_err(|e: Error| IndexStoreError::Write(Box::new(e)))
    }
}
impl ReadonlyIndex for NativeReadonlyIndex {
    fn as_index(&self) -> &dyn Index {
        self
    }
    fn change_id_index(
        &self,
        heads: &mut dyn Iterator<Item = &CommitId>,
    ) -> IndexResult<Box<dyn ChangeIdIndex>> {
        Ok(Box::new(ChangeIndex {
            index: self.clone(),
            heads: bounded_ids(heads, &self.core.limits).map_err(err)?,
        }))
    }
    fn start_modification(&self) -> Box<dyn MutableIndex> {
        Box::new(NativeMutableIndex {
            inner: self.clone(),
            parents: self.checkpoint.tips.clone(),
            pending: Vec::new(),
        })
    }
}
fn bounded_ids<'a>(
    ids: &mut dyn Iterator<Item = &'a CommitId>,
    limits: &Limits,
) -> Result<Vec<m::CommitId>, Error> {
    let mut out = Vec::new();
    for id in ids {
        if out.len() >= limits.max_roots {
            return Err(Error::Limit);
        }
        out.push(model_id(id)?)
    }
    Ok(out)
}
#[async_trait]
impl MutableIndex for NativeMutableIndex {
    fn as_index(&self) -> &dyn Index {
        &self.inner
    }
    fn change_id_index(
        &self,
        heads: &mut dyn Iterator<Item = &CommitId>,
    ) -> IndexResult<Box<dyn ChangeIdIndex + '_>> {
        Ok(Box::new(ChangeIndex {
            index: self.inner.clone(),
            heads: bounded_ids(heads, &self.inner.core.limits).map_err(err)?,
        }))
    }
    async fn add_commit(&mut self, commit: &Commit) -> IndexResult<()> {
        let id = model_id(commit.id()).map_err(err)?;
        if self.inner.has_id(commit.id()).await? {
            return Ok(());
        }
        let owner = self.inner.core.owner();
        let metadata = self
            .inner
            .core
            .objects
            .read_metadata_with_owner(
                m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(id.0),
                },
                owner,
            )
            .await
            .map_err(|_| err(Error::Canonical))?;
        let mut draft = self
            .inner
            .verify_draft(metadata.descriptor(), metadata.bytes(), id)
            .map_err(err)?;
        if draft.change.0.as_slice() != commit.change_id().as_bytes()
            || draft
                .parents
                .iter()
                .map(|p| p.0.as_slice())
                .ne(commit.parent_ids().iter().map(|p| p.as_bytes()))
        {
            return Err(err(Error::Canonical));
        }
        for parent in &draft.parents {
            if !self.inner.has_id(&jj_id(*parent)).await? {
                return Err(err(Error::Canonical));
            }
        }
        if self.inner.checkpoint.drafts.len() >= self.inner.core.limits.max_drafts {
            return Err(err(Error::Limit));
        }
        draft.order = self
            .inner
            .checkpoint
            .drafts
            .iter()
            .map(|d| d.order)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| err(Error::Limit))?;
        let mut drafts = self.inner.checkpoint.drafts.clone();
        let draft = Arc::new(draft);
        drafts.push(draft.clone());
        self.pending.push(draft);
        self.inner.checkpoint = Arc::new(Checkpoint::new(
            self.inner.checkpoint.tips.clone(),
            self.inner.checkpoint.roots.clone(),
            drafts,
            self.inner.checkpoint.source.clone(),
            self.inner.checkpoint.baseline.clone(),
        ));
        Ok(())
    }
    fn merge_in(&mut self, other: &dyn ReadonlyIndex) -> IndexResult<()> {
        let other = other
            .downcast_ref::<NativeReadonlyIndex>()
            .ok_or_else(|| err(Error::Foreign))?;
        self.inner.core.same(&other.core).map_err(err)?;
        if self.inner.checkpoint.source != other.checkpoint.source {
            return Err(err(Error::Unsupported));
        }
        let mut roots: BTreeSet<_> = self.inner.checkpoint.roots.iter().copied().collect();
        roots.extend(other.checkpoint.roots.iter().copied());
        let mut drafts: BTreeMap<_, _> = self
            .inner
            .checkpoint
            .drafts
            .iter()
            .map(|d| (d.id(), d.clone()))
            .collect();
        for draft in &other.checkpoint.drafts {
            if let Some(existing) = drafts.insert(draft.id(), draft.clone()) {
                if !existing.canonical_eq(draft) {
                    return Err(err(Error::Corrupt));
                }
                if existing.order < draft.order {
                    drafts.insert(draft.id(), existing);
                }
            }
        }
        if roots.len() > self.inner.core.limits.max_roots
            || drafts.len() > self.inner.core.limits.max_drafts
        {
            return Err(err(Error::Limit));
        }
        let mut parents: BTreeSet<_> = self.parents.iter().copied().collect();
        parents.extend(other.checkpoint.tips.iter().copied());
        if parents.len() > self.inner.core.limits.max_segments {
            return Err(err(Error::Limit));
        }
        let drafts = persistence::order_drafts(drafts, &self.inner.core.limits).map_err(err)?;
        self.parents = parents.into_iter().collect();
        self.inner.checkpoint = Arc::new(Checkpoint::new(
            self.parents.clone(),
            roots.into_iter().collect(),
            drafts,
            self.inner.checkpoint.source.clone(),
            self.inner.checkpoint.baseline.clone(),
        ));
        Ok(())
    }
}
struct ChangeIndex {
    index: NativeReadonlyIndex,
    heads: Vec<m::CommitId>,
}
#[cfg(all(test, unix))]
mod tests;
