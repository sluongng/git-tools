//! Generation-aware output metadata and a verified, bounded disk cache.
//! Namespace records are authoritative; clean CAS bytes are expendable. Retired
//! records remain addressable so an old kernel handle never resolves a new path.
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

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
#[derive(Clone, Debug)]
pub struct CacheLimitError {
    pub size: u64,
    pub limit: u64,
}
impl std::fmt::Display for CacheLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "artifact size {} exceeds hydration budget {}",
            self.size, self.limit
        )
    }
}
impl std::error::Error for CacheLimitError {}
fn require(condition: bool, error: CoreError) -> Result<()> {
    if condition { Ok(()) } else { Err(error.into()) }
}
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Artifact {
    pub backend: String,
    pub instance: String,
    pub digest_function: i32,
    pub hash: String,
    pub size: u64,
}
impl Artifact {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.digest_function, 1 | 9),
            "unsupported digest function"
        );
        ensure!(
            self.hash.len() == 64
                && self
                    .hash
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "invalid digest hash"
        );
        Ok(())
    }
}
#[async_trait]
pub trait BlobSource: Send + Sync {
    /// Stream at most artifact.size bytes; reject an overlong response before
    /// writing it. The cache reserves this declared disk/transfer budget and
    /// independently verifies both complete hash and exact final size.
    async fn fetch(&self, artifact: &Artifact, target: &mut File) -> Result<()>;
    async fn missing(&self, artifacts: &[Artifact]) -> Result<Vec<Artifact>>;
}
#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub max_bytes: u64,
    pub max_concurrent_fetches: usize,
    pub max_inflight_bytes: u64,
}
impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 4 * 1024 * 1024 * 1024,
            max_concurrent_fetches: 8,
            max_inflight_bytes: 512 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct CacheMetrics {
    pub fetches: u64,
    pub verified_bytes: u64,
    pub verification_bytes: u64,
    pub hits: u64,
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
    async fn shutdown(&self) -> Result<()>;
}
#[derive(Clone, Serialize, Deserialize)]
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
    identity: String,
    backend: String,
    next_id: u64,
    nodes: Table<Record>,
    pending: Table<Pending>,
    names: im::OrdMap<(u64, String), u64>,
    children: im::OrdMap<(u64, u64), ()>,
}
#[derive(Serialize, Deserialize)]
struct Header {
    #[serde(default)]
    identity: String,
    version: u32,
    backend: String,
    next_id: u64,
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
}
pub struct Tree {
    identity: String,
    namespace: Mutex<Namespace>,
    state_dir: PathBuf,
    backend: String,
    source: Arc<dyn BlobSource>,
    cache: Arc<Cache>,
    mutation_locks: Mutex<HashMap<u64, Weak<AsyncMutex<()>>>>,
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
        ensure!(
            config.max_bytes > 0
                && config.max_concurrent_fetches > 0
                && config.max_inflight_bytes > 0,
            "cache budgets must be positive"
        );
        fs::create_dir_all(state_dir.as_ref())?;
        let dir = state_dir.as_ref().canonicalize()?;
        fs::create_dir_all(dir.join("local"))?;
        fs::create_dir_all(dir.join("cache"))?;
        let db = SingleWriterTxDatabase::builder(dir.join("namespace.fjall")).open()?;
        let keyspace = db.keyspace("namespace", KeyspaceCreateOptions::default)?;
        let backend = backend_identity.into();
        let mut state = if let Some(bytes) = keyspace.get(b"header")? {
            let header: Header = serde_json::from_slice(&bytes)?;
            ensure!(
                header.version == 2 && header.backend == backend,
                "output state backend/version mismatch"
            );
            let mut state = State {
                identity: if header.identity.is_empty() {
                    uuid::Uuid::new_v4().to_string()
                } else {
                    header.identity
                },
                backend: backend.clone(),
                next_id: header.next_id,
                nodes: Table::default(),
                pending: Table::default(),
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
                identity: uuid::Uuid::new_v4().to_string(),
                backend: backend.clone(),
                next_id: 2,
                nodes,
                pending: Table::default(),
                names: im::OrdMap::new(),
                children: im::OrdMap::new(),
            }
        };
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
                let node = &mut state.nodes.get_mut(&id).unwrap().node;
                node.size = len;
                node.mtime = now();
                node.ctime = node.mtime;
            }
        }
        let cache = Arc::new(Cache::new(dir.join("cache"), source.clone(), config)?);
        let tree = Arc::new(Self {
            identity: state.identity.clone(),
            namespace: Mutex::new(Namespace {
                state,
                db,
                keyspace,
            }),
            state_dir: dir,
            backend,
            source,
            cache,
            mutation_locks: Mutex::new(HashMap::new()),
        });
        tree.change(|_| Ok(()))?;
        Ok(tree)
    }
    fn change<T>(&self, f: impl FnOnce(&mut State) -> Result<T>) -> Result<T> {
        let mut ns = self.namespace.lock().unwrap();
        let mut next = ns.state.clone();
        let result = f(&mut next)?;
        let changed: Vec<_> = next.nodes.dirty.iter().copied().collect();
        for id in &changed {
            if let Some(old) = ns.state.nodes.get(id) {
                index_remove(&mut next, &old.node);
            }
        }
        for id in &changed {
            if let Some(new) = next.nodes.get(id).map(|r| r.node.clone()) {
                index_insert(&mut next, &new);
            }
        }
        let mut tx = ns.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(
            &ns.keyspace,
            b"header",
            serde_json::to_vec(&Header {
                identity: next.identity.clone(),
                version: 2,
                backend: next.backend.clone(),
                next_id: next.next_id,
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
        tx.commit()?;
        next.nodes.dirty.clear();
        next.pending.dirty.clear();
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
    pub fn identity(&self) -> &str {
        &self.identity
    }
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }
    pub fn validate_artifact(&self, artifact: &Artifact) -> Result<()> {
        artifact.validate()?;
        ensure!(
            artifact.backend == self.backend,
            "CAS backend identity mismatch"
        );
        let limit = self
            .cache
            .config
            .max_bytes
            .min(self.cache.config.max_inflight_bytes);
        if artifact.size > limit {
            return Err(CacheLimitError {
                size: artifact.size,
                limit,
            }
            .into());
        }
        Ok(())
    }
    pub fn root_id(&self) -> u64 {
        1
    }
    pub fn get(&self, id: u64) -> Result<Node> {
        Ok(record(&self.namespace.lock().unwrap().state, id)?
            .node
            .clone())
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
    pub fn cache_metrics(&self) -> CacheMetrics {
        self.cache.metrics()
    }
    pub async fn content(&self, artifact: &Artifact) -> Result<Arc<CachedBlob>> {
        ensure!(
            artifact.backend == self.backend,
            "CAS backend identity mismatch"
        );
        self.cache.clone().content(artifact.clone()).await
    }
    pub async fn ensure_dir(&self, path: &str) -> Result<Node> {
        let names = parts(path)?;
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
    pub async fn read(&self, id: u64, offset: u64, count: usize) -> Result<Vec<u8>> {
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
    async fn make_local(&self, id: u64) -> Result<()> {
        let node = self.get(id)?;
        if let Some(artifact) = &node.remote {
            let lease = self.content(artifact).await?;
            let temp = self.state_dir.join("local").join(format!("{id}.tmp"));
            let mut guard = Temporary(temp.clone());
            let source = lease.path().to_owned();
            let destination = temp.clone();
            tokio::task::spawn_blocking(move || fs::copy(source, destination))
                .await
                .context("local snapshot task failed")??;
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&temp)?
                .sync_all()?;
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
        let lock = self.inode_lock(id);
        let _guard = lock.lock().await;
        ensure!(self.get(id)?.kind == NodeKind::File, "not a regular file");
        if bytes.is_empty() {
            return Ok(0);
        }
        self.make_local(id).await?;
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
                self.make_local(id).await?;
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
    pub async fn refresh(&self, prefix: &str) -> Result<Vec<String>> {
        let candidates = {
            let ns = self.namespace.lock().unwrap();
            let Some(root) = resolve_state(&ns.state, prefix)? else {
                return Ok(vec![]);
            };
            descendants(&ns.state, root)
                .into_iter()
                .filter_map(|id| {
                    let n = &record(&ns.state, id).ok()?.node;
                    n.remote.clone().map(|a| (id, a))
                })
                .collect::<Vec<_>>()
        };
        let artifacts = candidates
            .iter()
            .map(|(_, a)| a.clone())
            .collect::<Vec<_>>();
        if artifacts.is_empty() {
            return Ok(vec![]);
        }
        let missing: HashSet<_> = self.source.missing(&artifacts).await?.into_iter().collect();
        self.change(|s| {
            let mut removed = vec![];
            for (id, a) in &candidates {
                let node = &record(s, *id)?.node;
                if node.linked && node.remote.as_ref() == Some(a) && missing.contains(a) {
                    removed.push(path_for(s, *id)?);
                    let parent = node.parent;
                    s.nodes.get_mut(id).unwrap().node.linked = false;
                    touch(s, parent);
                }
            }
            Ok(removed)
        })
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
                    let (hash, size) =
                        tokio::task::spawn_blocking(move || hash_file(&local, digest_function))
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
fn hash_file(path: &Path, function: i32) -> Result<(String, u64)> {
    let mut file = File::open(path)?;
    let mut sha = Sha256::new();
    let mut blake = blake3::Hasher::new();
    ensure!(matches!(function, 1 | 9), "unsupported digest function");
    let mut size = 0;
    let mut buf = [0u8; 128 * 1024];
    loop {
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
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.0);
        }
    }
}

/// A cache file lease. The file is verified before publication and cannot be
/// reclaimed while any clone of this Arc remains alive.
pub struct CachedBlob {
    path: PathBuf,
    cache: Weak<Cache>,
}
impl CachedBlob {
    pub fn path(&self) -> &Path {
        &self.path
    }
}
impl Drop for CachedBlob {
    fn drop(&mut self) {
        if let Some(cache) = self.cache.upgrade() {
            let mut state = cache.files.lock().unwrap();
            if let Some(entry) = state.entries.get_mut(&self.path) {
                entry.pins = entry.pins.saturating_sub(1);
            }
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Fingerprint {
    len: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    identity: (u64, u64, u64, u64),
}
fn fingerprint(path: &Path) -> Result<Fingerprint> {
    let m = fs::metadata(path)?;
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        (m.dev(), m.ino(), m.ctime() as u64, m.ctime_nsec() as u64)
    };
    #[cfg(not(unix))]
    let identity = (0, 0, 0, 0);
    Ok(Fingerprint {
        len: m.len(),
        modified: m.modified().ok(),
        created: m.created().ok(),
        identity,
    })
}
struct CacheEntry {
    bytes: u64,
    pins: usize,
    accessed: u64,
    verified: Option<Fingerprint>,
}
#[derive(Default)]
struct CacheFiles {
    entries: HashMap<PathBuf, CacheEntry>,
    reserved: u64,
}
struct Flight {
    result: Mutex<Option<std::result::Result<Arc<CachedBlob>, String>>>,
    ready: tokio::sync::Notify,
}
impl Flight {
    async fn wait(&self) -> Result<Arc<CachedBlob>> {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.result.lock().unwrap().as_ref() {
                return result.clone().map_err(anyhow::Error::msg);
            }
            notified.await;
        }
    }
}
struct Reservation {
    cache: Weak<Cache>,
    bytes: u64,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(cache) = self.cache.upgrade() {
            cache.files.lock().unwrap().reserved -= self.bytes;
        }
    }
}
struct Cache {
    dir: PathBuf,
    source: Arc<dyn BlobSource>,
    config: CacheConfig,
    requests: Arc<Semaphore>,
    bytes: Semaphore,
    byte_units: u32,
    flights: Mutex<HashMap<String, Arc<Flight>>>,
    files: Mutex<CacheFiles>,
    serial: AtomicU64,
    fetches: AtomicU64,
    verified_bytes: AtomicU64,
    verification_bytes: AtomicU64,
    hits: AtomicU64,
}
impl Cache {
    fn new(dir: PathBuf, source: Arc<dyn BlobSource>, config: CacheConfig) -> Result<Self> {
        let byte_units = (config.max_inflight_bytes / 4096).clamp(1, u32::MAX as u64) as u32;
        let mut files = CacheFiles::default();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_file() {
                if path.extension().is_some_and(|e| e == "tmp") {
                    fs::remove_file(path)?;
                } else {
                    files.entries.insert(
                        path,
                        CacheEntry {
                            bytes: entry.metadata()?.len(),
                            pins: 0,
                            accessed: 0,
                            verified: None,
                        },
                    );
                }
            }
        }
        // A reduced quota on restart also applies to previously cached bytes.
        // No leases exist yet, so every existing clean cache file is eligible.
        let mut retained = files.entries.values().map(|e| e.bytes).sum::<u64>();
        while retained > config.max_bytes {
            let (path, size) = files
                .entries
                .iter()
                .next()
                .map(|(p, e)| (p.clone(), e.bytes))
                .context("cache size accounting mismatch")?;
            fs::remove_file(&path)?;
            files.entries.remove(&path);
            retained = retained.saturating_sub(size);
        }
        Ok(Self {
            dir,
            source,
            requests: Arc::new(Semaphore::new(config.max_concurrent_fetches)),
            bytes: Semaphore::new(byte_units as usize),
            byte_units,
            config,
            flights: Mutex::new(HashMap::new()),
            files: Mutex::new(files),
            serial: AtomicU64::new(1),
            fetches: AtomicU64::new(0),
            verified_bytes: AtomicU64::new(0),
            verification_bytes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        })
    }
    fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            fetches: self.fetches.load(Ordering::Relaxed),
            verified_bytes: self.verified_bytes.load(Ordering::Relaxed),
            verification_bytes: self.verification_bytes.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
        }
    }
    fn key(artifact: &Artifact) -> Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(artifact)?)
            .to_hex()
            .to_string())
    }
    async fn content(self: Arc<Self>, artifact: Artifact) -> Result<Arc<CachedBlob>> {
        artifact.validate()?;
        let limit = self.config.max_bytes.min(self.config.max_inflight_bytes);
        if artifact.size > limit {
            return Err(CacheLimitError {
                size: artifact.size,
                limit,
            }
            .into());
        }
        let key = Self::key(&artifact)?;
        let existing = self.flights.lock().unwrap().get(&key).cloned();
        if let Some(flight) = existing {
            return flight.wait().await;
        }
        // Admission happens before spawning; queued callers consume no detached
        // tasks or temporary files. Only one admitted task exists per key.
        let request = self
            .requests
            .clone()
            .acquire_owned()
            .await
            .context("cache shutting down")?;
        let (flight, start) = {
            let mut flights = self.flights.lock().unwrap();
            if let Some(flight) = flights.get(&key) {
                (flight.clone(), false)
            } else {
                let flight = Arc::new(Flight {
                    result: Mutex::new(None),
                    ready: tokio::sync::Notify::new(),
                });
                flights.insert(key.clone(), flight.clone());
                (flight, true)
            }
        };
        if start {
            let cache = self.clone();
            let flight = flight.clone();
            tokio::spawn(async move {
                let _request = request;
                let worker = cache.clone();
                let result = match tokio::spawn(async move { worker.obtain(&artifact).await }).await
                {
                    Ok(result) => result.map_err(|e| format!("{e:#}")),
                    Err(error) => Err(format!("cache worker failed: {error}")),
                };
                *flight.result.lock().unwrap() = Some(result);
                cache.flights.lock().unwrap().remove(&key);
                flight.ready.notify_waiters();
            });
        } else {
            drop(request);
        }
        flight.wait().await
    }
    fn reserve(self: &Arc<Self>, size: u64) -> Result<Reservation> {
        let mut state = self.files.lock().unwrap();
        let mut total = state.entries.values().map(|e| e.bytes).sum::<u64>();
        while total.saturating_add(state.reserved).saturating_add(size) > self.config.max_bytes {
            let victim = state
                .entries
                .iter()
                .filter(|(_, e)| e.pins == 0)
                .min_by_key(|(_, e)| e.accessed)
                .map(|(p, e)| (p.clone(), e.bytes))
                .context("disk cache budget occupied by pinned content or active downloads")?;
            fs::remove_file(&victim.0)?;
            state.entries.remove(&victim.0);
            total = total.saturating_sub(victim.1);
        }
        state.reserved += size;
        Ok(Reservation {
            cache: Arc::downgrade(self),
            bytes: size,
        })
    }
    async fn verify(&self, path: &Path, function: i32) -> Result<(String, u64)> {
        let path = path.to_owned();
        let result = tokio::task::spawn_blocking(move || hash_file(&path, function))
            .await
            .context("verification task failed")??;
        self.verification_bytes
            .fetch_add(result.1, Ordering::Relaxed);
        Ok(result)
    }
    async fn obtain(self: Arc<Self>, artifact: &Artifact) -> Result<Arc<CachedBlob>> {
        let path = self.dir.join(Self::key(artifact)?);
        let verified = {
            let mut state = self.files.lock().unwrap();
            if let Some(entry) = state.entries.get_mut(&path) {
                entry.pins += 1;
                Some(entry.verified.clone())
            } else {
                None
            }
        };
        if let Some(known) = verified {
            let lease = Arc::new(CachedBlob {
                path: path.clone(),
                cache: Arc::downgrade(&self),
            });
            // Hash only once per process/fingerprint, and never under the global
            // cache mutex. Range reads of a large verified blob are O(range).
            let current = fingerprint(&path);
            let valid = match current.as_ref() {
                Ok(current) if known.as_ref() == Some(current) => true,
                Ok(before) => match self.verify(&path, artifact.digest_function).await {
                    Ok((hash, size)) if hash == artifact.hash && size == artifact.size => {
                        fingerprint(&path).as_ref().ok() == Some(before)
                    }
                    _ => false,
                },
                Err(_) => false,
            };
            if valid {
                let mut state = self.files.lock().unwrap();
                let entry = state
                    .entries
                    .get_mut(&path)
                    .context("pinned cache entry disappeared")?;
                entry.verified = current.ok();
                entry.accessed = self.serial.fetch_add(1, Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(lease);
            }
            drop(lease);
            let mut state = self.files.lock().unwrap();
            ensure!(
                state.entries.get(&path).is_none_or(|e| e.pins == 0),
                "pinned cache content corrupted"
            );
            fs::remove_file(&path).or_else(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(e)
                }
            })?;
            state.entries.remove(&path);
        }
        let units = artifact
            .size
            .div_ceil(4096)
            .clamp(1, self.byte_units as u64) as u32;
        let _bytes = self
            .bytes
            .acquire_many(units)
            .await
            .context("cache shutting down")?;
        let _reservation = self.reserve(artifact.size)?;
        let temp_path = self.dir.join(format!(
            "{}-{}.tmp",
            std::process::id(),
            self.serial.fetch_add(1, Ordering::Relaxed)
        ));
        let _temp = Temporary(temp_path.clone());
        let mut target = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        self.fetches.fetch_add(1, Ordering::Relaxed);
        self.source.fetch(artifact, &mut target).await?;
        target.sync_all()?;
        drop(target);
        let (hash, size) = self.verify(&temp_path, artifact.digest_function).await?;
        ensure!(
            size == artifact.size && hash == artifact.hash,
            "CAS blob digest or size mismatch"
        );
        fs::rename(&temp_path, &path)?;
        let verified = fingerprint(&path)?;
        self.files.lock().unwrap().entries.insert(
            path.clone(),
            CacheEntry {
                bytes: size,
                pins: 1,
                accessed: self.serial.fetch_add(1, Ordering::Relaxed),
                verified: Some(verified),
            },
        );
        self.verified_bytes.fetch_add(size, Ordering::Relaxed);
        Ok(Arc::new(CachedBlob {
            path,
            cache: Arc::downgrade(&self),
        }))
    }
}

#[cfg(test)]
mod tests;
