//! Generation-aware output metadata and a verified, bounded disk cache.
//! Namespace records are authoritative; clean CAS bytes are expendable. Retired
//! records remain addressable until explicit lifetime-aware reclamation. IDs are never reused.
pub mod source;
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
pub use local_cas::{
    Artifact, BlobSource, CacheConfig, CacheLimitError, CacheMetrics, CacheTimeoutError,
    CachedBlob, LocalCas,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
pub use source::{
    SourceContent, SourceEntry, SourceFreezeBusy, SourceFreezeTimeout, SourceFrontendFence,
    SourceLimits, SourceManifest, SourceOperation, SourceReceipt, SourceSnapshot,
    SourceSnapshotEntry, SourceVersion,
};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex as AsyncMutex;

/// Portable namespace failures for adapters to translate into native statuses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreError {
    NotFound,
    AlreadyExists,
    NotDirectory,
    IsDirectory,
    NotEmpty,
    InvalidInput,
    NotSync,
    Stale,
}
impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CoreError {}
fn require(condition: bool, error: CoreError) -> Result<()> {
    if condition { Ok(()) } else { Err(error.into()) }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NodeKind {
    File,
    Directory,
    Symlink,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: u64,
    pub parent: u64,
    pub name: String,
    pub kind: NodeKind,
    pub mode: u32,
    pub size: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub remote: Option<Artifact>,
    pub generation: u64,
    pub linked: bool,
}
/// Remote refresh result, tied to one tree and immutable namespace snapshots.
/// Preparing this plan never requires a native projection publication lock.
pub struct RefreshPlan {
    identity: String,
    candidates: Vec<(Node, String)>,
    missing: HashSet<Artifact>,
}
impl RefreshPlan {
    /// Native frontends can preserve locally authoritative files before pruning.
    pub fn retain_candidates(
        &mut self,
        mut keep: impl FnMut(&str, &Node) -> Result<bool>,
    ) -> Result<()> {
        let mut retained = Vec::with_capacity(self.candidates.len());
        for (node, path) in self.candidates.drain(..) {
            if keep(&path, &node)? {
                retained.push((node, path));
            }
        }
        self.candidates = retained;
        Ok(())
    }
}
#[derive(Clone, Debug, Default)]
pub struct SetAttr {
    pub mode: Option<u32>,
    pub size: Option<u64>,
    pub mtime: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TreeStat {
    Directory,
    Symlink { target: String },
    File { artifact: Option<Artifact> },
    Unknown,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreparedStage {
    pub node: Node,
    pub path: String,
    pub previous: Option<Node>,
}
#[async_trait]
pub trait Projection: Send + Sync {
    fn root(&self) -> &Path;
    async fn ensure_dir(&self, path: &str) -> Result<()>;
    async fn stage(&self, path: &str, artifact: Artifact) -> Result<()>;
    async fn stat(&self, path: &str, digest_function: i32) -> Result<Option<TreeStat>>;
    async fn refresh(&self, prefix: &str) -> Result<()>;
    async fn clean(&self, prefix: &str) -> Result<()>;
    /// Publish a complete source root through the native frontend. Output-only
    /// providers must fail explicitly rather than emulate checkout by staging
    /// individual paths or exposing an unfenced namespace replacement.
    async fn publish_source(
        &self,
        _manifest: SourceManifest,
        _expected: SourceVersion,
        _request_id: String,
        _limits: SourceLimits,
    ) -> Result<SourceReceipt> {
        anyhow::bail!("native source publication is unsupported by this provider")
    }
    /// The caller must quiesce native filesystem users and RPC admission.
    /// This is an explicit maintenance boundary, not build finalization.
    async fn maintain_quiescent(&self, _transaction_budget: usize) -> Result<MaintenanceOutcome> {
        Ok(MaintenanceOutcome::Unsupported)
    }
    /// Drain already-admitted source publication before an owner proves that
    /// an unresolved request cannot still publish. This never cancels work.
    async fn wait_source_idle(&self) -> Result<()> {
        anyhow::bail!("native source publication drain is unsupported by this provider")
    }
    async fn shutdown(&self) -> Result<()>;
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceOutcome {
    Unsupported,
    BusyRetained,
    Rotated { collection: Collection },
}
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
struct Record {
    node: Node,
    target: Option<String>,
    verifier: Option<[u8; 8]>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Pending {
    parent: u64,
    name: String,
    previous: Option<u64>,
}
// Persistent maps give rollbackable transaction snapshots without copying the
// namespace. Dirty keys bound Fjall write amplification to the actual mutation.
#[derive(Clone)]
struct Table<T: Clone> {
    map: im::OrdMap<u64, T>,
    dirty: std::collections::BTreeSet<u64>,
}
impl<T: Clone> Default for Table<T> {
    fn default() -> Self {
        Self {
            map: im::OrdMap::new(),
            dirty: Default::default(),
        }
    }
}
impl<T: Clone> Table<T> {
    fn get(&self, id: &u64) -> Option<&T> {
        self.map.get(id)
    }
    fn get_mut(&mut self, id: &u64) -> Option<&mut T> {
        self.dirty.insert(*id);
        self.map.get_mut(id)
    }
    fn insert(&mut self, id: u64, value: T) {
        self.dirty.insert(id);
        self.map.insert(id, value);
    }
    fn remove(&mut self, id: &u64) {
        self.dirty.insert(*id);
        self.map.remove(id);
    }
    fn values(&self) -> impl Iterator<Item = &T> {
        self.map.values()
    }
}
#[derive(Clone)]
struct State {
    source: Option<source::SourceMeta>,
    identity: String,
    backend: String,
    next_id: u64,
    retain_placeholders: bool,
    placeholder_legacy_ceiling: u64,
    nodes: Table<Record>,
    pending: Table<Pending>,
    garbage: Table<()>,
    retired: im::OrdSet<u64>,
    dependents: im::OrdMap<u64, usize>,
    names: im::OrdMap<(u64, String), u64>,
    children: im::OrdMap<(u64, u64), ()>,
}
#[derive(Serialize, Deserialize)]
struct Header {
    #[serde(default)]
    source: Option<source::SourceMeta>,
    #[serde(default)]
    identity: String,
    version: u32,
    backend: String,
    next_id: u64,
    #[serde(default)]
    retain_placeholders: bool,
    #[serde(default)]
    placeholder_legacy_ceiling: u64,
}
fn index_remove(s: &mut State, n: &Node) {
    if n.linked && n.id != 1 {
        if s.names.get(&(n.parent, n.name.clone())) == Some(&n.id) {
            s.names.remove(&(n.parent, n.name.clone()));
        }
        s.children.remove(&(n.parent, n.id));
    }
}
fn index_insert(s: &mut State, n: &Node) {
    if n.linked && n.id != 1 {
        s.names.insert((n.parent, n.name.clone()), n.id);
        s.children.insert((n.parent, n.id), ());
    }
}
struct Namespace {
    state: State,
    db: SingleWriterTxDatabase,
    keyspace: SingleWriterTxKeyspace,
    pins: HashMap<u64, u64>,
    lookups: HashMap<u64, u64>,
    generations: usize,
    gc_cursor: u64,
    failed: bool,
}
pub struct Tree {
    source_gate: Arc<Mutex<source::Gate>>,
    identity: String,
    namespace: Mutex<Namespace>,
    state_dir: PathBuf,
    backend: String,
    source: Arc<dyn BlobSource>,
    cache: Arc<LocalCas>,
    mutation_locks: Mutex<HashMap<u64, Weak<AsyncMutex<()>>>>,
}
/// A frontend-owned reference to one immutable entry ID. Reclamation cannot
/// remove its metadata or local payload until the last lease has been dropped.
pub struct NodeLease {
    tree: Weak<Tree>,
    id: u64,
}
impl Drop for NodeLease {
    fn drop(&mut self) {
        if let Some(tree) = self.tree.upgrade() {
            let mut ns = tree.namespace.lock().unwrap();
            if let Some(count) = ns.pins.get_mut(&self.id) {
                *count -= 1;
                if *count == 0 {
                    ns.pins.remove(&self.id);
                }
            }
        }
    }
}
/// NFSv3 has no CLOSE/last-handle signal. This lease protects the entire server
/// generation, not merely requests currently executing. ProjFS also needs this
/// fence while persisted placeholders may issue hydration callbacks.
pub struct GenerationLease {
    tree: Weak<Tree>,
}
impl Drop for GenerationLease {
    fn drop(&mut self) {
        if let Some(tree) = self.tree.upgrade() {
            tree.namespace.lock().unwrap().generations -= 1;
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RetirementMetrics {
    pub retained_nodes: usize,
    pub retained_local_bytes: u64,
    pub pending_payload_deletions: usize,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Collection {
    pub visited: usize,
    pub reclaimed_nodes: usize,
    pub reclaimed_local_bytes: u64,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}
fn component(name: &str) -> Result<()> {
    require(
        !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', '\0']),
        CoreError::InvalidInput,
    )?;
    Ok(())
}
fn parts(path: &str) -> Result<Vec<&str>> {
    if path.is_empty() {
        return Ok(vec![]);
    }
    let result: Vec<_> = path.split('/').collect();
    for name in &result {
        component(name)?;
    }
    Ok(result)
}
fn child(s: &State, parent: u64, name: &str) -> Option<u64> {
    s.names.get(&(parent, name.to_owned())).copied()
}
fn record(s: &State, id: u64) -> Result<&Record> {
    s.nodes.get(&id).ok_or_else(|| CoreError::Stale.into())
}
fn directory(s: &State, id: u64) -> Result<()> {
    require(
        record(s, id)?.node.kind == NodeKind::Directory,
        CoreError::NotDirectory,
    )?;
    Ok(())
}
fn resolve_state(s: &State, path: &str) -> Result<Option<u64>> {
    let mut id = 1;
    for part in parts(path)? {
        directory(s, id)?;
        let Some(next) = child(s, id, part) else {
            return Ok(None);
        };
        id = next;
    }
    Ok(Some(id))
}
fn allocate(
    s: &mut State,
    parent: u64,
    name: &str,
    kind: NodeKind,
    mode: u32,
    remote: Option<Artifact>,
    linked: bool,
) -> Result<Node> {
    let id = s.next_id;
    s.next_id = id.checked_add(1).context("output inode space exhausted")?;
    let timestamp = now();
    let node = Node {
        id,
        parent,
        name: name.into(),
        kind,
        mode,
        size: remote.as_ref().map_or(0, |a| a.size),
        mtime: timestamp,
        ctime: timestamp,
        remote,
        generation: id,
        linked,
    };
    index_insert(s, &node);
    s.nodes.insert(
        id,
        Record {
            node: node.clone(),
            target: None,
            verifier: None,
        },
    );
    Ok(node)
}
fn touch(s: &mut State, id: u64) {
    if let Some(r) = s.nodes.get_mut(&id) {
        let t = now().max(r.node.ctime.saturating_add(1));
        r.node.mtime = t;
        r.node.ctime = t;
    }
}
impl Tree {
    pub fn open(
        state_dir: impl AsRef<Path>,
        backend_identity: impl Into<String>,
        source: Arc<dyn BlobSource>,
        config: CacheConfig,
    ) -> Result<Arc<Self>> {
        let cache = LocalCas::open(state_dir.as_ref().join("cache"), source, config)?;
        Self::open_with_cache(state_dir, backend_identity, cache)
    }
    /// Open an independent durable namespace using a shared clean-content cache.
    /// Namespace identity and writable payloads remain isolated in `state_dir`.
    pub fn open_with_cache(
        state_dir: impl AsRef<Path>,
        backend_identity: impl Into<String>,
        cache: Arc<LocalCas>,
    ) -> Result<Arc<Self>> {
        let source = cache.source();
        Self::open_with_cache_and_source(state_dir, backend_identity, cache, source)
    }
    /// Share cache ownership while retaining this namespace's authorized source.
    pub fn open_with_cache_and_source(
        state_dir: impl AsRef<Path>,
        backend_identity: impl Into<String>,
        cache: Arc<LocalCas>,
        source: Arc<dyn BlobSource>,
    ) -> Result<Arc<Self>> {
        Self::open_namespace(
            state_dir.as_ref(),
            backend_identity.into(),
            cache,
            source,
            false,
        )
    }
    /// Reopen a recognized namespace without creating missing directories,
    /// keyspaces, headers, or root records. Partial/unrelated state is an error.
    pub fn open_existing_with_cache_and_source(
        state_dir: impl AsRef<Path>,
        backend_identity: impl Into<String>,
        cache: Arc<LocalCas>,
        source: Arc<dyn BlobSource>,
    ) -> Result<Arc<Self>> {
        Self::open_namespace(
            state_dir.as_ref(),
            backend_identity.into(),
            cache,
            source,
            true,
        )
    }
    fn open_namespace(
        state_dir: &Path,
        backend_identity: String,
        cache: Arc<LocalCas>,
        source: Arc<dyn BlobSource>,
        require_existing: bool,
    ) -> Result<Arc<Self>> {
        if require_existing {
            for path in [
                state_dir.to_owned(),
                state_dir.join("local"),
                state_dir.join("namespace.fjall"),
            ] {
                ensure!(
                    fs::symlink_metadata(path)?.file_type().is_dir(),
                    "existing namespace directory is missing or invalid"
                );
            }
            // Fjall 3's builder creates a database if the version marker is
            // absent. Mandatory format preflight prevents that create path.
            let version_path = state_dir.join("namespace.fjall/version");
            ensure!(
                fs::symlink_metadata(&version_path)?.file_type().is_file(),
                "existing namespace format marker missing"
            );
            let mut version = Vec::new();
            File::open(version_path)?
                .take(5)
                .read_to_end(&mut version)?;
            ensure!(
                version == b"FJL\x03",
                "unrecognized existing namespace database"
            );
        } else {
            fs::create_dir_all(state_dir)?;
            fs::create_dir_all(state_dir.join("local"))?;
        }
        let dir = state_dir.canonicalize()?;
        let db = SingleWriterTxDatabase::builder(dir.join("namespace.fjall")).open()?;
        ensure!(
            !require_existing || db.keyspace_exists("namespace"),
            "existing namespace keyspace missing"
        );
        let keyspace = db.keyspace("namespace", KeyspaceCreateOptions::default)?;
        ensure!(
            !require_existing || keyspace.get(b"header")?.is_some(),
            "existing namespace header missing"
        );
        let backend = backend_identity;
        let mut state = if let Some(bytes) = keyspace.get(b"header")? {
            let header: Header = serde_json::from_slice(&bytes)?;
            ensure!(
                !require_existing || !header.identity.is_empty(),
                "existing namespace identity missing"
            );
            ensure!(
                matches!(header.version, 2 | 3) && header.backend == backend,
                "output state backend/version mismatch"
            );
            let mut state = State {
                source: header.source,
                identity: if header.identity.is_empty() {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    header.identity
                },
                backend: backend.clone(),
                next_id: header.next_id,
                retain_placeholders: header.retain_placeholders,
                placeholder_legacy_ceiling: header.placeholder_legacy_ceiling,
                nodes: Table::default(),
                pending: Table::default(),
                garbage: Table::default(),
                retired: im::OrdSet::new(),
                dependents: im::OrdMap::new(),
                names: im::OrdMap::new(),
                children: im::OrdMap::new(),
            };
            for item in db.read_tx().prefix(&keyspace, b"n") {
                let (_, bytes) = item.into_inner()?;
                let record: Record = serde_json::from_slice(&bytes)?;
                index_insert(&mut state, &record.node);
                state.nodes.insert(record.node.id, record);
            }
            for item in db.read_tx().prefix(&keyspace, b"p") {
                let (key, bytes) = item.into_inner()?;
                let id = u64::from_be_bytes(key[1..].try_into()?);
                state.pending.insert(id, serde_json::from_slice(&bytes)?);
            }
            for item in db.read_tx().prefix(&keyspace, b"g") {
                let (key, _) = item.into_inner()?;
                let id = u64::from_be_bytes(key[1..].try_into()?);
                state.garbage.insert(id, ());
            }
            state.garbage.dirty.clear();
            state.nodes.dirty.clear();
            state.pending.dirty.clear();
            state
        } else {
            ensure!(
                keyspace.get(b"state")?.is_none(),
                "obsolete development output state; use a fresh state directory"
            );
            let t = now();
            let mut nodes = Table::default();
            nodes.insert(
                1,
                Record {
                    node: Node {
                        id: 1,
                        parent: 1,
                        name: String::new(),
                        kind: NodeKind::Directory,
                        mode: 0o755,
                        size: 0,
                        mtime: t,
                        ctime: t,
                        remote: None,
                        generation: 1,
                        linked: true,
                    },
                    target: None,
                    verifier: None,
                },
            );
            State {
                source: None,
                identity: uuid::Uuid::new_v4().to_string(),
                backend: backend.clone(),
                next_id: 2,
                retain_placeholders: false,
                placeholder_legacy_ceiling: 0,
                nodes,
                pending: Table::default(),
                garbage: Table::default(),
                retired: im::OrdSet::new(),
                dependents: im::OrdMap::new(),
                names: im::OrdMap::new(),
                children: im::OrdMap::new(),
            }
        };
        if require_existing {
            let root = state
                .nodes
                .get(&1)
                .context("existing namespace root missing")?;
            ensure!(
                root.node.id == 1
                    && root.node.parent == 1
                    && root.node.name.is_empty()
                    && root.node.kind == NodeKind::Directory
                    && root.node.linked
                    && root.node.remote.is_none(),
                "invalid existing namespace root"
            );
        }
        for record in state.nodes.values() {
            if !record.node.linked {
                state.retired.insert(record.node.id);
            }
            if record.node.id != 1 {
                *state.dependents.entry(record.node.parent).or_insert(0) += 1;
            }
        }
        // A crash can leave a local payload whose namespace commit did not land.
        // Never reuse its identity on restart.
        for entry in fs::read_dir(dir.join("local"))? {
            let entry = entry?;
            if let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|n| n.parse::<u64>().ok())
            {
                state.next_id = state
                    .next_id
                    .max(id.checked_add(1).context("inode highwater exhausted")?);
            }
        }
        // A data fsync may precede its metadata transaction at a crash. Local
        // bytes are authoritative; reconcile length before serving any client.
        let local_ids: Vec<_> = state
            .nodes
            .values()
            .filter(|r| r.node.kind == NodeKind::File && r.node.remote.is_none())
            .map(|r| r.node.id)
            .collect();
        for id in local_ids {
            let len = fs::metadata(dir.join("local").join(id.to_string()))
                .context("authoritative local payload missing")?
                .len();
            if state.nodes.get(&id).unwrap().node.size != len {
                ensure!(
                    !require_existing,
                    "existing authoritative local payload size mismatch"
                );
                let node = &mut state.nodes.get_mut(&id).unwrap().node;
                node.size = len;
                node.mtime = now();
                node.ctime = node.mtime;
            }
        }
        let tree = Arc::new(Self {
            source_gate: Arc::new(Mutex::new(source::Gate::default())),
            identity: state.identity.clone(),
            namespace: Mutex::new(Namespace {
                state,
                db,
                keyspace,
                pins: HashMap::new(),
                lookups: HashMap::new(),
                generations: 0,
                gc_cursor: 0,
                failed: false,
            }),
            state_dir: dir,
            backend,
            source,
            cache,
            mutation_locks: Mutex::new(HashMap::new()),
        });
        tree.change(|_| Ok(()))?;
        tree.drain_retired_payloads(usize::MAX)?;
        Ok(tree)
    }
    fn change<T>(&self, f: impl FnOnce(&mut State) -> Result<T>) -> Result<T> {
        let gate = self.source_gate.lock().unwrap();
        ensure!(!gate.frozen, "source namespace is frozen");
        let mut ns = self.namespace.lock().unwrap();
        Self::change_locked(&mut ns, |next| {
            let previous = next.clone();
            let result = f(next)?;
            source::validate_control_change(&previous, next)?;
            Ok(result)
        })
    }
    fn change_locked<T>(ns: &mut Namespace, f: impl FnOnce(&mut State) -> Result<T>) -> Result<T> {
        ensure!(
            !ns.failed,
            "output namespace commit failed; reopen for recovery"
        );
        let mut next = ns.state.clone();
        let result = f(&mut next)?;
        if !next.nodes.dirty.is_empty() {
            if let (Some(old), Some(new)) = (&ns.state.source, &mut next.source) {
                if new.revision == old.revision {
                    new.revision = new
                        .revision
                        .checked_add(1)
                        .context("source revision exhausted")?;
                }
            }
        }
        let changed: Vec<_> = next.nodes.dirty.iter().copied().collect();
        for id in &changed {
            if let Some(old) = ns.state.nodes.get(id) {
                index_remove(&mut next, &old.node);
                next.retired.remove(id);
                if old.node.id != 1 {
                    let count = next.dependents.get_mut(&old.node.parent).unwrap();
                    *count -= 1;
                    if *count == 0 {
                        next.dependents.remove(&old.node.parent);
                    }
                }
            }
        }
        for id in &changed {
            if let Some(new) = next.nodes.get(id).map(|r| r.node.clone()) {
                index_insert(&mut next, &new);
                if !new.linked {
                    next.retired.insert(new.id);
                }
                if new.id != 1 {
                    *next.dependents.entry(new.parent).or_insert(0) += 1;
                }
            }
        }
        let mut tx = ns.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(
            &ns.keyspace,
            b"header",
            serde_json::to_vec(&Header {
                source: next.source.clone(),
                identity: next.identity.clone(),
                version: 3,
                backend: next.backend.clone(),
                next_id: next.next_id,
                retain_placeholders: next.retain_placeholders,
                placeholder_legacy_ceiling: next.placeholder_legacy_ceiling,
            })?,
        );
        for id in &next.nodes.dirty {
            let mut key = vec![b'n'];
            key.extend_from_slice(&id.to_be_bytes());
            if let Some(r) = next.nodes.get(id) {
                tx.insert(&ns.keyspace, key, serde_json::to_vec(r)?);
            } else {
                tx.remove(&ns.keyspace, key);
            }
        }
        for id in &next.pending.dirty {
            let mut key = vec![b'p'];
            key.extend_from_slice(&id.to_be_bytes());
            if let Some(r) = next.pending.get(id) {
                tx.insert(&ns.keyspace, key, serde_json::to_vec(r)?);
            } else {
                tx.remove(&ns.keyspace, key);
            }
        }
        for id in &next.garbage.dirty {
            let mut key = vec![b'g'];
            key.extend_from_slice(&id.to_be_bytes());
            if next.garbage.get(id).is_some() {
                tx.insert(&ns.keyspace, key, []);
            } else {
                tx.remove(&ns.keyspace, key);
            }
        }
        if let Err(error) = tx.commit() {
            // Durability errors may be ambiguous. Do not resurrect a removed
            // record over its persisted deletion intent in a later transaction.
            ns.failed = true;
            return Err(error.into());
        }
        next.nodes.dirty.clear();
        next.pending.dirty.clear();
        next.garbage.dirty.clear();
        ns.state = next;
        Ok(result)
    }
    fn inode_lock(&self, id: u64) -> Arc<AsyncMutex<()>> {
        let mut map = self.mutation_locks.lock().unwrap();
        if let Some(lock) = map.get(&id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        map.insert(id, Arc::downgrade(&lock));
        lock
    }
    fn local_path(&self, id: u64) -> PathBuf {
        self.state_dir.join("local").join(id.to_string())
    }
    /// Persisted ProjFS placeholders survive provider restart. Until native
    /// reconciliation proves them retired, preserve every generation, including
    /// across later opens. This does not grant ownership of native edited files.
    pub fn retain_placeholder_generations(&self) -> Result<()> {
        self.change(|s| {
            s.retain_placeholders = true;
            Ok(())
        })
    }
    /// Capture while the native provider is stopped or holds its publication
    /// barrier. Persist the corresponding provider manifest before enabling
    /// self-contained immutable locators; this value protects older formats.
    pub fn allocation_highwater(&self) -> u64 {
        self.namespace.lock().unwrap().state.next_id - 1
    }
    /// The caller owns the provider and its publication barrier. Its durable
    /// manifest must already identify self-contained locators and record the
    /// legacy ceiling. Old-format publishers must retain an inode before they
    /// expose it. Existing finite protection can only grow, never shrink.
    pub fn enable_immutable_placeholder_locators(&self, legacy_ceiling: u64) -> Result<u64> {
        self.change(|s| {
            ensure!(
                legacy_ceiling < s.next_id,
                "legacy placeholder ceiling exceeds allocated IDs"
            );
            let ceiling = if s.retain_placeholders {
                // A legacy provider may have used this tree after an earlier
                // manifest was written. Cover all IDs it could have published.
                s.next_id - 1
            } else {
                legacy_ceiling
            };
            s.placeholder_legacy_ceiling = s.placeholder_legacy_ceiling.max(ceiling);
            s.retain_placeholders = false;
            Ok(s.placeholder_legacy_ceiling)
        })
    }
    /// Persist protection before publishing a non-self-contained placeholder.
    /// Native full/edited files are not claimed or removed by this operation.
    pub fn retain_placeholder_inode(&self, id: u64) -> Result<()> {
        self.change(|s| {
            record(s, id)?;
            s.placeholder_legacy_ceiling = s.placeholder_legacy_ceiling.max(id);
            Ok(())
        })
    }
    pub fn pin(self: &Arc<Self>, id: u64) -> Result<NodeLease> {
        let mut ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed,
            "output namespace commit failed; reopen for recovery"
        );
        record(&ns.state, id)?;
        let count = ns.pins.entry(id).or_insert(0);
        *count = count
            .checked_add(1)
            .context("inode lease count exhausted")?;
        Ok(NodeLease {
            tree: Arc::downgrade(self),
            id,
        })
    }
    pub fn pin_generation(self: &Arc<Self>) -> GenerationLease {
        let mut ns = self.namespace.lock().unwrap();
        ns.generations = ns
            .generations
            .checked_add(1)
            .expect("generation lease count exhausted");
        GenerationLease {
            tree: Arc::downgrade(self),
        }
    }
    /// Record the kernel reference in the same critical section as lookup.
    pub fn lookup_referenced(&self, parent: u64, name: &str) -> Result<Node> {
        component(name)?;
        let mut ns = self.namespace.lock().unwrap();
        directory(&ns.state, parent)?;
        let id = child(&ns.state, parent, name).ok_or(CoreError::NotFound)?;
        let node = record(&ns.state, id)?.node.clone();
        Self::retain_lookup_locked(&mut ns, id)?;
        Ok(node)
    }
    fn retain_lookup_locked(ns: &mut Namespace, id: u64) -> Result<()> {
        record(&ns.state, id)?;
        let count = ns.lookups.entry(id).or_insert(0);
        *count = count
            .checked_add(1)
            .context("kernel lookup count exhausted")?;
        Ok(())
    }
    pub fn retain_lookup(&self, id: u64) -> Result<()> {
        Self::retain_lookup_locked(&mut self.namespace.lock().unwrap(), id)
    }
    pub fn forget(&self, id: u64, count: u64) {
        let mut ns = self.namespace.lock().unwrap();
        if let Some(current) = ns.lookups.get_mut(&id) {
            *current = current.saturating_sub(count);
            if *current == 0 {
                ns.lookups.remove(&id);
            }
        }
    }
    pub fn retirement_metrics(&self) -> RetirementMetrics {
        let ns = self.namespace.lock().unwrap();
        RetirementMetrics {
            retained_nodes: ns.state.retired.len(),
            retained_local_bytes: ns
                .state
                .retired
                .iter()
                .filter_map(|id| ns.state.nodes.get(id))
                .filter(|r| r.node.kind == NodeKind::File && r.node.remote.is_none())
                .map(|r| r.node.size)
                .sum(),
            pending_payload_deletions: ns.state.garbage.map.len(),
        }
    }
    /// Explicit lifetime boundary. Raw-ID users must retain leases before
    /// calling this; it is never run implicitly by Tree mutations or reopen.
    /// Native NFS may call it only after unmount/remount invalidates old handles.
    /// ProjFS must not call it across restart: persisted placeholders retain IDs.
    /// The budget bounds candidates visited and deletion work, not live edits.
    pub fn reclaim_retired(&self, budget: usize) -> Result<Collection> {
        let _job = self.source_job()?;
        let candidates = {
            let mut ns = self.namespace.lock().unwrap();
            ensure!(
                !ns.failed,
                "output namespace commit failed; reopen for recovery"
            );
            if ns.generations != 0 || ns.state.retain_placeholders || budget == 0 {
                return Ok(Collection::default());
            }
            let ids: Vec<_> = ns
                .state
                .retired
                // Children normally have newer IDs than their parents. Prefer
                // them first, avoiding one whole sweep per directory level.
                // Renames can reverse that order; boundary collection still
                // repeats complete rounds until no dependency progress occurs.
                .range(..ns.gc_cursor)
                .rev()
                .chain(ns.state.retired.range(ns.gc_cursor..).rev())
                .take(budget)
                .copied()
                .collect();
            if let Some(id) = ids.last() {
                ns.gc_cursor = *id;
            }
            ids
        };
        let mut result = Collection::default();
        for id in candidates {
            result.visited += 1;
            let lock = self.inode_lock(id);
            let Ok(_guard) = lock.try_lock() else {
                continue;
            };
            let mut ns = self.namespace.lock().unwrap();
            if ns.generations != 0
                || ns.state.retain_placeholders
                || id <= ns.state.placeholder_legacy_ceiling
                || ns.pins.contains_key(&id)
                || ns.lookups.contains_key(&id)
                || ns.state.pending.get(&id).is_some()
                || ns.state.pending.values().any(|p| p.previous == Some(id))
                || ns.state.dependents.get(&id).copied().unwrap_or(0) != 0
            {
                continue;
            }
            let Some(node) = ns.state.nodes.get(&id).map(|r| r.node.clone()) else {
                continue;
            };
            if node.linked || id == 1 {
                continue;
            }
            let bytes = if node.kind == NodeKind::File && node.remote.is_none() {
                node.size
            } else {
                0
            };
            Self::change_locked(&mut ns, |s| {
                s.nodes.remove(&id);
                // Every removed ID authorizes deletion of its own payload only.
                // This also covers a remote-to-local copy whose metadata commit
                // lost the crash race, without guessing about unknown files.
                s.garbage.insert(id, ());
                Ok(())
            })?;
            self.mutation_locks.lock().unwrap().remove(&id);
            result.reclaimed_nodes += 1;
            result.reclaimed_local_bytes += bytes;
        }
        self.drain_retired_payloads(budget)?;
        Ok(result)
    }
    /// Drain reclaimable entries at an explicit frontend lifetime boundary.
    /// Each transaction remains bounded, but the complete operation may require
    /// multiple rounds: retired parents cannot disappear before their children.
    /// A pinned prefix must not hide later candidates from the first round.
    pub fn reclaim_retired_at_boundary(&self, transaction_budget: usize) -> Result<Collection> {
        let mut total = Collection::default();
        if transaction_budget == 0 {
            return Ok(total);
        }
        loop {
            let pending_before = self.namespace.lock().unwrap().state.garbage.map.len();
            // Retry an already-authorized deletion even when its metadata was
            // the last retired entry. A transient unlink failure must not need
            // a new retired inode (or restart) to make maintenance progress.
            self.drain_retired_payloads(transaction_budget)?;
            let mut remaining = self.namespace.lock().unwrap().state.retired.len();
            let mut progress = 0;
            while remaining != 0 {
                let result = self.reclaim_retired(transaction_budget.min(remaining))?;
                if result.visited == 0 {
                    break;
                }
                remaining -= result.visited;
                progress += result.reclaimed_nodes;
                total.visited += result.visited;
                total.reclaimed_nodes += result.reclaimed_nodes;
                total.reclaimed_local_bytes += result.reclaimed_local_bytes;
                std::thread::yield_now();
            }
            let pending_after = self.namespace.lock().unwrap().state.garbage.map.len();
            if progress == 0 && (pending_after == 0 || pending_after >= pending_before) {
                return Ok(total);
            }
        }
    }
    fn drain_retired_payloads(&self, budget: usize) -> Result<()> {
        let _job = self.source_job()?;
        let ids: Vec<_> = self
            .namespace
            .lock()
            .unwrap()
            .state
            .garbage
            .map
            .keys()
            .take(budget)
            .copied()
            .collect();
        for id in ids {
            match fs::remove_file(self.local_path(id)) {
                Ok(()) => sync_directory(&self.state_dir.join("local"))?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            self.change(|s| {
                s.garbage.remove(&id);
                Ok(())
            })?;
        }
        Ok(())
    }
    pub fn identity(&self) -> &str {
        &self.identity
    }
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }
    pub fn validate_artifact(&self, artifact: &Artifact) -> Result<()> {
        artifact.validate()?;
        ensure!(
            self.namespace.lock().unwrap().state.source.is_none() || artifact.digest_function == 9,
            "source files require raw BLAKE3"
        );
        ensure!(
            artifact.backend == self.backend,
            "CAS backend identity mismatch"
        );
        Ok(())
    }
    pub fn root_id(&self) -> u64 {
        1
    }
    pub fn get(&self, id: u64) -> Result<Node> {
        let ns = self.namespace.lock().unwrap();
        ensure!(
            !ns.failed,
            "output namespace commit failed; reopen for recovery"
        );
        Ok(record(&ns.state, id)?.node.clone())
    }
    pub fn lookup(&self, parent: u64, name: &str) -> Result<Node> {
        component(name)?;
        let ns = self.namespace.lock().unwrap();
        directory(&ns.state, parent)?;
        let id = child(&ns.state, parent, name).ok_or(CoreError::NotFound)?;
        Ok(record(&ns.state, id)?.node.clone())
    }
    pub fn resolve(&self, path: &str) -> Result<Node> {
        let ns = self.namespace.lock().unwrap();
        let id = resolve_state(&ns.state, path)?.ok_or(CoreError::NotFound)?;
        Ok(record(&ns.state, id)?.node.clone())
    }
    pub fn list(&self, parent: u64, after: u64, limit: usize) -> Result<Vec<Node>> {
        let ns = self.namespace.lock().unwrap();
        directory(&ns.state, parent)?;
        Ok(ns
            .state
            .children
            .range((
                std::ops::Bound::Excluded((parent, after)),
                std::ops::Bound::Included((parent, u64::MAX)),
            ))
            .take(limit)
            .map(|((_, id), _)| ns.state.nodes.get(id).unwrap().node.clone())
            .collect())
    }

    pub fn readlink(&self, id: u64) -> Result<String> {
        let ns = self.namespace.lock().unwrap();
        let r = record(&ns.state, id)?;
        ensure!(r.node.kind == NodeKind::Symlink, "not a symlink");
        r.target.clone().context("missing symlink target")
    }
    /// Maximum immutable artifact size admitted by the configured cache lanes.
    /// Metadata-only staging may represent larger artifacts without fetching.
    pub fn readable_limit(&self) -> u64 {
        self.cache.config().readable_limit()
    }
    pub fn cache_metrics(&self) -> CacheMetrics {
        self.cache.metrics()
    }
    pub async fn content(&self, artifact: &Artifact) -> Result<Arc<CachedBlob>> {
        ensure!(
            artifact.backend == self.backend,
            "CAS backend identity mismatch"
        );
        self.cache
            .clone()
            .content_from(artifact.clone(), self.source.clone())
            .await
    }
    pub async fn ensure_dir(&self, path: &str) -> Result<Node> {
        let names = parts(path)?;
        self.source_writable_path(path)?;
        {
            let ns = self.namespace.lock().unwrap();
            if let Some(id) = resolve_state(&ns.state, path)? {
                directory(&ns.state, id)?;
                return Ok(record(&ns.state, id)?.node.clone());
            }
        }
        self.change(|s| {
            let mut id = 1;
            for name in names {
                id = if let Some(id) = child(s, id, name) {
                    directory(s, id)?;
                    id
                } else {
                    let n = allocate(s, id, name, NodeKind::Directory, 0o755, None, true)?;
                    touch(s, id);
                    n.id
                };
            }
            Ok(record(s, id)?.node.clone())
        })
    }
    fn create_record(
        &self,
        parent: u64,
        name: &str,
        kind: NodeKind,
        mode: u32,
        target: Option<&str>,
        verifier: Option<[u8; 8]>,
    ) -> Result<Node> {
        component(name)?;
        self.source_writable_child(parent, name)?;
        self.change(|s| {
            directory(s, parent)?;
            if let Some(id) = child(s, parent, name) {
                let r = record(s, id)?;
                if verifier.is_some() && r.verifier == verifier && r.node.kind == NodeKind::File {
                    return Ok(r.node.clone());
                }
                return Err(CoreError::AlreadyExists.into());
            }
            let n = allocate(s, parent, name, kind, mode, None, true)?;
            if kind == NodeKind::File {
                let f = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(self.local_path(n.id))?;
                f.sync_all()?;
                sync_directory(&self.state_dir.join("local"))?;
            }
            let r = s.nodes.get_mut(&n.id).unwrap();
            r.target = target.map(str::to_owned);
            r.node.size = target.map_or(0, |t| t.len() as u64);
            r.verifier = verifier;
            let n = r.node.clone();
            touch(s, parent);
            Ok(n)
        })
    }
    pub async fn create(&self, parent: u64, name: &str, mode: u32) -> Result<Node> {
        self.create_record(parent, name, NodeKind::File, mode, None, None)
    }
    pub async fn create_exclusive(
        &self,
        parent: u64,
        name: &str,
        mode: u32,
        verifier: [u8; 8],
    ) -> Result<Node> {
        self.create_record(parent, name, NodeKind::File, mode, None, Some(verifier))
    }
    pub async fn mkdir(&self, parent: u64, name: &str, mode: u32) -> Result<Node> {
        self.create_record(parent, name, NodeKind::Directory, mode, None, None)
    }
    pub async fn symlink(&self, parent: u64, name: &str, target: &str) -> Result<Node> {
        ensure!(!target.contains('\0'), "invalid symlink target");
        self.create_record(parent, name, NodeKind::Symlink, 0o777, Some(target), None)
    }
    fn unlink_kind(&self, parent: u64, name: &str, kind: Option<NodeKind>) -> Result<()> {
        component(name)?;
        self.change(|s| {
            let id = child(s, parent, name).ok_or(CoreError::NotFound)?;
            let node = record(s, id)?.node.clone();
            if let Some(kind) = kind {
                if kind == NodeKind::Directory {
                    require(node.kind == kind, CoreError::NotDirectory)?
                } else {
                    require(node.kind != NodeKind::Directory, CoreError::IsDirectory)?
                }
            }
            if node.kind == NodeKind::Directory {
                require(
                    s.children.range((id, 0)..=(id, u64::MAX)).next().is_none(),
                    CoreError::NotEmpty,
                )?;
            }
            s.nodes.get_mut(&id).unwrap().node.linked = false;
            touch(s, parent);
            Ok(())
        })
    }
    pub async fn unlink(&self, parent: u64, name: &str) -> Result<()> {
        self.unlink_kind(parent, name, None)
    }
    pub async fn unlink_file(&self, parent: u64, name: &str) -> Result<()> {
        self.unlink_kind(parent, name, Some(NodeKind::File))
    }
    pub async fn unlink_directory(&self, parent: u64, name: &str) -> Result<()> {
        self.unlink_kind(parent, name, Some(NodeKind::Directory))
    }
    pub async fn rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
    ) -> Result<()> {
        component(name)?;
        component(new_name)?;
        let expected = self.lookup(parent, name)?.id;
        let lock = self.inode_lock(expected);
        let _guard = lock.lock().await;
        self.change(|s| {
            directory(s, new_parent)?;
            let id = child(s, parent, name).ok_or(CoreError::NotFound)?;
            require(id == expected, CoreError::Stale)?;
            if parent == new_parent && name == new_name {
                return Ok(());
            }
            let moving = record(s, id)?.node.clone();
            let mut ancestor = new_parent;
            loop {
                require(ancestor != id, CoreError::InvalidInput)?;
                if ancestor == 1 {
                    break;
                }
                ancestor = record(s, ancestor)?.node.parent;
            }
            if let Some(old) = child(s, new_parent, new_name) {
                let oldnode = record(s, old)?.node.clone();
                require(
                    (oldnode.kind == NodeKind::Directory) == (moving.kind == NodeKind::Directory),
                    if moving.kind == NodeKind::Directory {
                        CoreError::NotDirectory
                    } else {
                        CoreError::IsDirectory
                    },
                )?;
                if oldnode.kind == NodeKind::Directory {
                    require(
                        s.children
                            .range((old, 0)..=(old, u64::MAX))
                            .next()
                            .is_none(),
                        CoreError::NotEmpty,
                    )?;
                }
                s.nodes.get_mut(&old).unwrap().node.linked = false;
            }
            let r = s.nodes.get_mut(&id).unwrap();
            r.node.parent = new_parent;
            r.node.name = new_name.into();
            r.node.ctime = now().max(r.node.ctime.saturating_add(1));
            touch(s, parent);
            touch(s, new_parent);
            Ok(())
        })
    }
    pub async fn prepare_stage(&self, path: &str, artifact: Artifact) -> Result<PreparedStage> {
        self.source_writable_path(path)?;
        self.validate_artifact(&artifact)?;
        let names = parts(path)?;
        ensure!(!names.is_empty(), "cannot stage root");
        let name = *names.last().unwrap();
        let prefix = names[..names.len() - 1].join("/");
        let parent = self.ensure_dir(&prefix).await?;
        self.change(|s| {
            directory(s, parent.id)?;
            let previous = child(s, parent.id, name).map(|id| record(s, id).unwrap().node.clone());
            if let Some(n) = &previous {
                ensure!(n.kind != NodeKind::Directory, "cannot stage over directory");
            }
            let node = allocate(
                s,
                parent.id,
                name,
                NodeKind::File,
                0o755,
                Some(artifact),
                false,
            )?;
            s.pending.insert(
                node.id,
                Pending {
                    parent: parent.id,
                    name: name.into(),
                    previous: previous.as_ref().map(|n| n.id),
                },
            );
            Ok(PreparedStage {
                node,
                path: path.into(),
                previous,
            })
        })
    }
    /// Validate a replay journal before changing the native view. Aborted,
    /// cleaned or superseded generations must never be republished.
    pub fn prepared_stage_valid(&self, prepared: &PreparedStage) -> Result<bool> {
        prepared_valid(&self.namespace.lock().unwrap().state, prepared)
    }
    pub async fn commit_stage(&self, prepared: PreparedStage) -> Result<Node> {
        self.change(|s| {
            ensure!(
                prepared_valid(s, &prepared)?,
                "stage generation no longer valid"
            );
            if let Some(r) = s.nodes.get(&prepared.node.id)
                && r.node.linked
                && resolve_state(s, &prepared.path)? == Some(r.node.id)
            {
                return Ok(r.node.clone());
            }
            let parent_path = parts(&prepared.path)?;
            let parent_path = parent_path[..parent_path.len().saturating_sub(1)].join("/");
            let p = s
                .pending
                .get(&prepared.node.id)
                .context("stage no longer pending")?
                .clone();
            ensure!(
                resolve_state(s, &parent_path)? == Some(p.parent)
                    && child(s, p.parent, &p.name) == p.previous,
                "stage binding changed during publication"
            );
            ensure!(record(s, p.parent)?.node.linked, "stage parent was removed");
            if let Some(old) = p.previous {
                s.nodes.get_mut(&old).unwrap().node.linked = false;
            }
            let n = &mut s
                .nodes
                .get_mut(&prepared.node.id)
                .context("prepared inode missing")?
                .node;
            n.linked = true;
            let result = n.clone();
            s.pending.remove(&prepared.node.id);
            touch(s, p.parent);
            Ok(result)
        })
    }
    pub async fn abort_stage(&self, prepared: PreparedStage) -> Result<()> {
        self.change(|s| {
            s.pending.remove(&prepared.node.id);
            Ok(())
        })
    }
    /// Atomic namespace-only publication. Channels needing a pre-publication
    /// native update must instead use prepare_stage/commit_stage.
    pub async fn stage(&self, path: &str, artifact: Artifact) -> Result<Node> {
        self.source_writable_path(path)?;
        self.validate_artifact(&artifact)?;
        let names = parts(path)?;
        require(!names.is_empty(), CoreError::InvalidInput)?;
        let name = *names.last().unwrap();
        let prefix = names[..names.len() - 1].join("/");
        let parent = self.ensure_dir(&prefix).await?;
        self.change(|s| {
            directory(s, parent.id)?;
            require(record(s, parent.id)?.node.linked, CoreError::Stale)?;
            if let Some(old) = child(s, parent.id, name) {
                require(
                    record(s, old)?.node.kind != NodeKind::Directory,
                    CoreError::IsDirectory,
                )?;
                s.nodes.get_mut(&old).unwrap().node.linked = false;
            }
            let node = allocate(
                s,
                parent.id,
                name,
                NodeKind::File,
                0o755,
                Some(artifact),
                true,
            )?;
            touch(s, parent.id);
            Ok(node)
        })
    }
    async fn bounded_operation<T>(
        &self,
        deadline: tokio::time::Instant,
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        let _job = self.source_job()?;
        match tokio::time::timeout_at(deadline, future).await {
            // Cache admission may report the same expiry in this poll. Keep
            // its typed result rather than counting one reader at both layers.
            Ok(Err(error)) if error.downcast_ref::<CacheTimeoutError>().is_some() => Err(error),
            Ok(result) if tokio::time::Instant::now() < deadline => result,
            _ => {
                self.cache.record_waiter_timeout();
                Err(CacheTimeoutError.into())
            }
        }
    }
    pub async fn read(&self, id: u64, offset: u64, count: usize) -> Result<Vec<u8>> {
        let deadline = tokio::time::Instant::now() + self.cache.config().operation_timeout;
        self.bounded_operation(deadline, self.read_inner(id, offset, count))
            .await
    }
    async fn read_inner(&self, id: u64, offset: u64, count: usize) -> Result<Vec<u8>> {
        let lock = self.inode_lock(id);
        let _guard = lock.lock().await;
        let node = self.get(id)?;
        ensure!(node.kind == NodeKind::File, "not a regular file");
        let lease = if let Some(a) = &node.remote {
            Some(self.content(a).await?)
        } else {
            None
        };
        let path = lease
            .as_ref()
            .map_or_else(|| self.local_path(id), |l| l.path().to_owned());
        read_range(&path, offset, count)
    }
    async fn make_local(&self, id: u64, deadline: tokio::time::Instant) -> Result<()> {
        let node = self.get(id)?;
        if let Some(artifact) = &node.remote {
            let lease = self.content(artifact).await?;
            let temp = self
                .state_dir
                .join("local")
                .join(format!("{id}-{}.tmp", uuid::Uuid::new_v4()));
            let cancelled = Arc::new(AtomicBool::new(false));
            let _cancel_on_drop = CancelWork(cancelled.clone());
            // The worker owns both the cache lease and unique temporary payload.
            // Cancelling the caller cannot unpin its source, reuse its path or
            // delete its destination while a detached copy is still writing.
            let physical_job = self.source_job()?;
            let mut guard = tokio::task::spawn_blocking(move || {
                let _physical_job = physical_job;
                let guard = Temporary(temp);
                let mut input = File::open(lease.path())?;
                let mut output = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&guard.0)?;
                let mut buffer = [0u8; 128 * 1024];
                loop {
                    if cancelled.load(Ordering::Relaxed) || tokio::time::Instant::now() >= deadline
                    {
                        return Err(CacheTimeoutError.into());
                    }
                    let count = input.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    output.write_all(&buffer[..count])?;
                }
                output.sync_all()?;
                drop(output);
                Ok::<_, anyhow::Error>(guard)
            })
            .await
            .context("local snapshot task failed")??;
            if tokio::time::Instant::now() >= deadline {
                return Err(CacheTimeoutError.into());
            }
            let temp = guard.0.clone();
            #[cfg(windows)]
            if self.local_path(id).exists() {
                fs::remove_file(self.local_path(id))?;
            }
            fs::rename(&temp, self.local_path(id))?;
            sync_directory(&self.state_dir.join("local"))?;
            guard.0 = PathBuf::new();
            self.change(|s| {
                let n = &mut s.nodes.get_mut(&id).context("inode disappeared")?.node;
                ensure!(n.remote.as_ref() == Some(artifact), "inode content changed");
                n.remote = None;
                Ok(())
            })?;
        }
        Ok(())
    }
    pub async fn write(&self, id: u64, offset: u64, bytes: &[u8]) -> Result<usize> {
        let deadline = tokio::time::Instant::now() + self.cache.config().operation_timeout;
        self.bounded_operation(deadline, self.write_inner(id, offset, bytes, deadline))
            .await
    }
    async fn write_inner(
        &self,
        id: u64,
        offset: u64,
        bytes: &[u8],
        deadline: tokio::time::Instant,
    ) -> Result<usize> {
        self.source_writable_node(id)?;
        let lock = self.inode_lock(id);
        let _guard = lock.lock().await;
        ensure!(self.get(id)?.kind == NodeKind::File, "not a regular file");
        if bytes.is_empty() {
            return Ok(0);
        }
        self.make_local(id, deadline).await?;
        let mut file = OpenOptions::new().write(true).open(self.local_path(id))?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        let size = file.metadata()?.len();
        self.change(|s| {
            let n = &mut s.nodes.get_mut(&id).unwrap().node;
            n.size = size;
            touch(s, id);
            Ok(())
        })?;
        Ok(bytes.len())
    }
    pub async fn setattr(&self, id: u64, attrs: SetAttr) -> Result<Node> {
        self.setattr_guarded(id, attrs, None).await
    }
    pub async fn setattr_guarded(
        &self,
        id: u64,
        attrs: SetAttr,
        expected_ctime: Option<u64>,
    ) -> Result<Node> {
        let deadline = tokio::time::Instant::now() + self.cache.config().operation_timeout;
        self.bounded_operation(
            deadline,
            self.setattr_inner(id, attrs, expected_ctime, deadline),
        )
        .await
    }
    async fn setattr_inner(
        &self,
        id: u64,
        attrs: SetAttr,
        expected_ctime: Option<u64>,
        deadline: tokio::time::Instant,
    ) -> Result<Node> {
        self.source_writable_node(id)?;
        let lock = self.inode_lock(id);
        let _guard = lock.lock().await;
        let node = self.get(id)?;
        if let Some(t) = expected_ctime {
            require(node.ctime == t, CoreError::NotSync)?;
        }
        if let Some(size) = attrs.size {
            ensure!(node.kind == NodeKind::File, "cannot resize non-file");
            if size == 0 && node.remote.is_some() {
                let f = File::create(self.local_path(id))?;
                f.sync_all()?;
                sync_directory(&self.state_dir.join("local"))?;
                self.change(|s| {
                    let n = &mut s.nodes.get_mut(&id).context("inode disappeared")?.node;
                    n.remote = None;
                    n.size = 0;
                    Ok(())
                })?;
            } else {
                self.make_local(id, deadline).await?;
            }
            let f = OpenOptions::new().write(true).open(self.local_path(id))?;
            f.set_len(size)?;
            f.sync_all()?;
        }
        self.change(|s| {
            let n = &mut s.nodes.get_mut(&id).context("inode disappeared")?.node;
            if let Some(t) = expected_ctime {
                require(n.ctime == t, CoreError::NotSync)?;
            }
            if let Some(mode) = attrs.mode {
                n.mode = mode & 0o7777;
            }
            if let Some(size) = attrs.size {
                n.size = size;
            }
            let t = now().max(n.ctime.saturating_add(1));
            n.ctime = t;
            if let Some(mtime) = attrs.mtime {
                n.mtime = mtime;
            } else if attrs.size.is_some() {
                n.mtime = t;
            }
            Ok(n.clone())
        })
    }
    pub fn known_remotes(&self, prefix: &str) -> Result<Vec<Artifact>> {
        let ns = self.namespace.lock().unwrap();
        let Some(root) = resolve_state(&ns.state, prefix)? else {
            return Ok(vec![]);
        };
        let ids = descendants(&ns.state, root);
        let mut seen = HashSet::new();
        Ok(ids
            .into_iter()
            .filter_map(|id| record(&ns.state, id).ok()?.node.remote.clone())
            .filter(|a| seen.insert(a.clone()))
            .collect())
    }
    pub async fn prepare_refresh(&self, prefix: &str) -> Result<RefreshPlan> {
        let candidates = {
            let ns = self.namespace.lock().unwrap();
            let mut candidates = Vec::new();
            if let Some(root) = resolve_state(&ns.state, prefix)? {
                for id in descendants(&ns.state, root) {
                    let node = &record(&ns.state, id)?.node;
                    if node.remote.is_some() {
                        candidates.push((node.clone(), path_for(&ns.state, id)?));
                    }
                }
            }
            candidates
        };
        let artifacts: HashSet<_> = candidates
            .iter()
            .filter_map(|(node, _)| node.remote.clone())
            .collect();
        let missing: HashSet<_> = if artifacts.is_empty() {
            HashSet::new()
        } else {
            self.source
                .missing(&artifacts.iter().cloned().collect::<Vec<_>>())
                .await?
                .into_iter()
                .collect()
        };
        ensure!(
            missing.is_subset(&artifacts),
            "CAS refresh returned an unrequested artifact"
        );
        Ok(RefreshPlan {
            identity: self.identity.clone(),
            candidates,
            missing,
        })
    }
    pub async fn apply_refresh(&self, plan: RefreshPlan) -> Result<Vec<String>> {
        ensure!(
            plan.identity == self.identity,
            "refresh plan belongs to another tree"
        );
        self.change(|s| {
            let mut removed = vec![];
            for (expected, path) in &plan.candidates {
                let Some(artifact) = &expected.remote else {
                    continue;
                };
                if !plan.missing.contains(artifact) {
                    continue;
                }
                let Some(current) = s.nodes.get(&expected.id) else {
                    continue;
                };
                // Replacements, local writes, setattr and ancestor renames all
                // invalidate stale plans. Never prune a newly bound native path.
                if current.node != *expected
                    || resolve_state(s, path)? != Some(expected.id)
                    || path_for(s, expected.id)? != *path
                {
                    continue;
                }
                removed.push(path.clone());
                s.nodes.get_mut(&expected.id).unwrap().node.linked = false;
                touch(s, expected.parent);
            }
            Ok(removed)
        })
    }
    pub async fn refresh(&self, prefix: &str) -> Result<Vec<String>> {
        let plan = self.prepare_refresh(prefix).await?;
        self.apply_refresh(plan).await
    }
    pub async fn clean(&self, prefix: &str) -> Result<()> {
        parts(prefix)?;
        self.change(|s| {
            // Clean also cancels unpublished records, even if no linked entry
            // exists yet. A stale native journal may reappear after a crash.
            let pending: Vec<_> = s
                .pending
                .map
                .iter()
                .filter_map(|(id, p)| {
                    let path = path_for(s, p.parent).ok().map(|parent| {
                        if parent.is_empty() {
                            p.name.clone()
                        } else {
                            format!("{parent}/{}", p.name)
                        }
                    })?;
                    (prefix.is_empty()
                        || path == prefix
                        || path
                            .strip_prefix(prefix)
                            .is_some_and(|suffix| suffix.starts_with('/')))
                    .then_some(*id)
                })
                .collect();
            for id in pending {
                s.pending.remove(&id);
            }
            let Some(root) = resolve_state(s, prefix)? else {
                return Ok(());
            };
            let ids = descendants(s, root);
            let parent = record(s, root)?.node.parent;
            for id in ids {
                if id != 1 {
                    s.nodes.get_mut(&id).unwrap().node.linked = false;
                }
            }
            touch(s, parent);
            Ok(())
        })
    }
    pub async fn stat(&self, path: &str, digest_function: i32) -> Result<Option<TreeStat>> {
        let deadline = tokio::time::Instant::now() + self.cache.config().operation_timeout;
        self.bounded_operation(deadline, self.stat_inner(path, digest_function, deadline))
            .await
    }
    async fn stat_inner(
        &self,
        path: &str,
        digest_function: i32,
        deadline: tokio::time::Instant,
    ) -> Result<Option<TreeStat>> {
        let node = {
            let ns = self.namespace.lock().unwrap();
            let Some(id) = resolve_state(&ns.state, path)? else {
                return Ok(None);
            };
            record(&ns.state, id)?.node.clone()
        };
        if node.kind == NodeKind::File && node.remote.is_some() {
            let ns = self.namespace.lock().unwrap();
            if resolve_state(&ns.state, path)? == Some(node.id)
                && record(&ns.state, node.id)?.node.remote == node.remote
            {
                let artifact = node.remote.filter(|a| a.digest_function == digest_function);
                return Ok(Some(TreeStat::File { artifact }));
            }
        }
        match node.kind {
            NodeKind::Directory => Ok(Some(TreeStat::Directory)),
            NodeKind::Symlink => Ok(Some(TreeStat::Symlink {
                target: self.readlink(node.id)?,
            })),
            NodeKind::File => {
                let lock = self.inode_lock(node.id);
                let _guard = lock.lock().await;
                let current = self.get(node.id)?;
                let artifact = if let Some(a) = current.remote {
                    if a.digest_function == digest_function {
                        Some(a)
                    } else {
                        None
                    }
                } else {
                    let local = self.local_path(current.id);
                    let physical_job = self.source_job()?;
                    let (hash, size) = tokio::task::spawn_blocking(move || {
                        let _physical_job = physical_job;
                        hash_file_until(&local, digest_function, Some(deadline))
                    })
                    .await
                    .context("local hash task failed")??;
                    Some(Artifact {
                        backend: self.backend.clone(),
                        instance: String::new(),
                        digest_function,
                        hash,
                        size,
                    })
                };
                let ns = self.namespace.lock().unwrap();
                if resolve_state(&ns.state, path)? != Some(current.id) {
                    return Ok(Some(TreeStat::Unknown));
                }
                Ok(Some(TreeStat::File { artifact }))
            }
        }
    }
}
fn prepared_valid(s: &State, prepared: &PreparedStage) -> Result<bool> {
    let names = parts(&prepared.path)?;
    if names.is_empty() {
        return Ok(false);
    }
    let Some(stored) = s.nodes.get(&prepared.node.id) else {
        return Ok(false);
    };
    let mut identity = stored.node.clone();
    identity.linked = false;
    if prepared.node.linked || identity != prepared.node {
        return Ok(false);
    }
    if stored.node.linked {
        return Ok(resolve_state(s, &prepared.path)? == Some(stored.node.id));
    }
    let Some(pending) = s.pending.get(&prepared.node.id) else {
        return Ok(false);
    };
    if pending.parent != prepared.node.parent
        || pending.name != prepared.node.name
        || names.last().copied() != Some(pending.name.as_str())
        || pending.previous != prepared.previous.as_ref().map(|n| n.id)
    {
        return Ok(false);
    }
    let prefix = names[..names.len() - 1].join("/");
    if resolve_state(s, &prefix)? != Some(pending.parent)
        || child(s, pending.parent, &pending.name) != pending.previous
    {
        return Ok(false);
    }
    if let Some(previous) = &prepared.previous
        && s.nodes
            .get(&previous.id)
            .is_none_or(|r| r.node != *previous)
    {
        return Ok(false);
    }
    Ok(record(s, pending.parent)?.node.linked)
}
fn descendants(s: &State, root: u64) -> Vec<u64> {
    let mut ids = vec![root];
    let mut i = 0;
    while i < ids.len() {
        let parent = ids[i];
        ids.extend(
            s.children
                .range((parent, 0)..=(parent, u64::MAX))
                .map(|((_, id), _)| *id),
        );
        i += 1;
    }
    ids
}
fn path_for(s: &State, id: u64) -> Result<String> {
    let mut names = vec![];
    let mut id = id;
    while id != 1 {
        let n = &record(s, id)?.node;
        names.push(n.name.clone());
        id = n.parent;
    }
    names.reverse();
    Ok(names.join("/"))
}
fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    // ProjFS keeps writable authority on NTFS and does not use core local
    // payloads. Generic Tree local-payload power-loss durability on Windows is
    // not guaranteed until a native directory publication protocol is added.
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}
fn read_range(path: &Path, offset: u64, count: usize) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![];
    file.take(count as u64).read_to_end(&mut bytes)?;
    Ok(bytes)
}
fn hash_file_until(
    path: &Path,
    function: i32,
    deadline: Option<tokio::time::Instant>,
) -> Result<(String, u64)> {
    let mut file = File::open(path)?;
    let mut sha = Sha256::new();
    let mut blake = blake3::Hasher::new();
    ensure!(matches!(function, 1 | 9), "unsupported digest function");
    let mut size = 0;
    let mut buf = [0u8; 128 * 1024];
    loop {
        // Blocking OS reads cannot be preempted, but an expired verification
        // must stop between chunks rather than continue detached over the file.
        if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
            return Err(CacheTimeoutError.into());
        }
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        size += n as u64;
        if function == 1 {
            sha.update(&buf[..n])
        } else {
            blake.update(&buf[..n]);
        }
    }
    Ok((
        if function == 1 {
            format!("{:x}", sha.finalize())
        } else {
            blake.finalize().to_hex().to_string()
        },
        size,
    ))
}
struct CancelWork(Arc<AtomicBool>);
impl Drop for CancelWork {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.0);
        }
    }
}

#[cfg(test)]
mod tests;
