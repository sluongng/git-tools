//! Shared rebuildable graph facts. Neither an index node nor a scope is a
//! canonical-object residency, authorization, or remote-retention proof.
use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
use fs2::FileExt;
use private_fs::PrivateDir;
mod codec;
mod scoped;
use codec::{RecordCodec, decode, encode};
pub use scoped::ScopedNode;
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs::File,
    io::Read,
    path::Path,
    sync::{Arc, Mutex},
};
use tinytree_model as m;

pub const STORAGE_SCHEMA: u32 = 3;

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid index input: {0}")]
    Invalid(&'static str),
    #[error("derived index is corrupt; explicit rebuild required")]
    Corrupt,
    #[error("derived index schema is unsupported; rebuild into a new directory")]
    UnsupportedSchema,
    #[error("canonical parent or scoped root is not indexed yet")]
    NotReady,
    #[error("exact query work budget exhausted")]
    BudgetExceeded,
    #[error("index persistence failed; reopen required")]
    Unavailable,
    #[error("index storage operation failed: {0}")]
    Storage(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Model(#[from] m::Error),
}
fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_repositories: usize,
    pub max_nodes: usize,
    pub max_record_bytes: usize,
    pub max_index_bytes: u64,
    pub max_roots: usize,
    /// Also caps every canonical commit vector before native decoding.
    pub max_parents: usize,
    pub model: m::Limits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_repositories: 1024,
            max_nodes: 1_000_000,
            max_record_bytes: 256 * 1024,
            max_index_bytes: 1024 * 1024 * 1024,
            max_roots: 4096,
            max_parents: 4096,
            model: m::Limits::default(),
        }
    }
}
impl Limits {
    fn validate(&self) -> Result<()> {
        if self.max_repositories == 0
            || self.max_nodes == 0
            || self.max_record_bytes < 1024
            || self.max_index_bytes == 0
            || self.max_roots == 0
            || self.max_parents == 0
        {
            return Err(Error::Invalid("zero or insufficient storage limit"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct WorkBudget {
    pub max_nodes: u64,
    pub max_edges: u64,
    pub max_bytes: u64,
    pub max_frontier: usize,
    nodes: u64,
    edges: u64,
    bytes: u64,
}
impl Default for WorkBudget {
    fn default() -> Self {
        Self {
            max_nodes: 100_000,
            max_edges: 1_000_000,
            max_bytes: 64 * 1024 * 1024,
            max_frontier: 100_000,
            nodes: 0,
            edges: 0,
            bytes: 0,
        }
    }
}
impl WorkBudget {
    pub fn new(max_nodes: u64, max_edges: u64, max_bytes: u64, max_frontier: usize) -> Self {
        Self {
            max_nodes,
            max_edges,
            max_bytes,
            max_frontier,
            nodes: 0,
            edges: 0,
            bytes: 0,
        }
    }
    /// Charged metadata probes, including position-row lookups for compact
    /// skip links as well as node rows (and intrinsic root probes).
    pub fn nodes_read(&self) -> u64 {
        self.nodes
    }
    pub fn edges_read(&self) -> u64 {
        self.edges
    }
    pub fn bytes_read(&self) -> u64 {
        self.bytes
    }
    /// Atomically account for record probes, traversed edges and bytes of read
    /// work, including work performed by a host's private graph overlay.
    /// Exhaustion leaves the accumulated counters unchanged.
    pub fn charge(&mut self, nodes: u64, edges: u64, bytes: u64) -> Result<()> {
        let next = (
            self.nodes.checked_add(nodes),
            self.edges.checked_add(edges),
            self.bytes.checked_add(bytes),
        );
        match next {
            (Some(n), Some(e), Some(b))
                if n <= self.max_nodes && e <= self.max_edges && b <= self.max_bytes =>
            {
                self.nodes = n;
                self.edges = e;
                self.bytes = b;
                Ok(())
            }
            _ => Err(Error::BudgetExceeded),
        }
    }
    /// Check an aggregate retained frontier before growing it. This is not a
    /// memory reservation; the host still owns allocation and lifetime quotas.
    pub fn frontier(&self, size: usize) -> Result<()> {
        if size > self.max_frontier {
            Err(Error::BudgetExceeded)
        } else {
            Ok(())
        }
    }
}
/// Durable logical record bytes exclude the fixed header and Fjall's physical
/// logs/tables. Physical storage usage is host/filesystem-specific.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexStats {
    pub repositories: u64,
    pub nodes: u64,
    pub logical_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitNode {
    pub commit: m::CommitId,
    pub change_id: m::ChangeId,
    pub parents: Vec<m::CommitId>,
    pub position: u64,
    pub generation: u64,
    pub committer_millis: i64,
    /// Verified canonical root-tree merge has multiple terms, not parents.
    pub has_conflict: bool,
    pub descriptor: m::Descriptor,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrefixResolution<T> {
    NoMatch,
    Single(T),
    Ambiguous,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeTargets {
    pub change_id: m::ChangeId,
    pub commits: Vec<m::CommitId>,
}

#[derive(Clone)]
struct Header {
    schema: u32,
    native_format: u16,
    epoch: [u8; 16],
    repositories: u64,
    nodes: u64,
    bytes: u64,
}
#[derive(Clone)]
struct Repository {
    name: String,
    high_water: u64,
    generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredNode {
    commit: [u8; 32],
    change: [u8; 16],
    parents: Vec<[u8; 32]>,
    position: u64,
    generation: u64,
    size: u64,
    committer_millis: i64,
    has_conflict: bool,
    skips: Vec<u64>,
}
impl StoredNode {
    fn public(&self) -> CommitNode {
        CommitNode {
            commit: m::CommitId(self.commit),
            change_id: m::ChangeId(self.change),
            parents: self.parents.iter().copied().map(m::CommitId).collect(),
            position: self.position,
            generation: self.generation,
            committer_millis: self.committer_millis,
            has_conflict: self.has_conflict,
            descriptor: m::Descriptor {
                key: m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(self.commit),
                },
                size_bytes: self.size,
                format_version: m::FORMAT_VERSION,
            },
        }
    }
}
fn repo_hash(repo: &str) -> Result<[u8; 32]> {
    if repo.is_empty() || repo.len() > 512 || repo.chars().any(char::is_control) {
        return Err(Error::Invalid("repository identity"));
    }
    Ok(*blake3::hash(repo.as_bytes()).as_bytes())
}
fn key(tag: u8, repo: &[u8; 32], suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(33 + suffix.len());
    key.push(tag);
    key.extend(repo);
    key.extend(suffix);
    key
}
fn change_key(repo: &[u8; 32], change: &[u8; 16], commit: &[u8; 32]) -> Vec<u8> {
    let mut suffix = change.to_vec();
    suffix.extend(commit);
    key(b'c', repo, &suffix)
}
fn synthetic() -> StoredNode {
    StoredNode {
        commit: [0; 32],
        change: [0; 16],
        parents: Vec::new(),
        position: 0,
        generation: 0,
        size: 0,
        committer_millis: 0,
        has_conflict: false,
        skips: Vec::new(),
    }
}
struct State {
    db: SingleWriterTxDatabase,
    table: SingleWriterTxKeyspace,
    header: Header,
    failed: bool,
}
pub struct Index {
    // Drop the database before releasing exclusive/private path ownership.
    state: Mutex<State>,
    limits: Limits,
    _lock: File,
    _directory: PrivateDir,
}
pub struct Scope {
    index: Arc<Index>,
    repository: String,
    repo: [u8; 32],
    roots: Vec<m::CommitId>,
    epoch: [u8; 16],
    high_water: u64,
    generation: u64,
}

impl State {
    fn get<T: RecordCodec>(&self, key: &[u8], limits: &Limits) -> Result<Option<T>> {
        if self.failed {
            return Err(Error::Unavailable);
        }
        self.table
            .get(key)
            .map_err(storage)?
            .map(|bytes| decode(key, &bytes, limits))
            .transpose()
    }
    fn position_id(
        &self,
        repo: &[u8; 32],
        position: u64,
        limits: &Limits,
        budget: &mut WorkBudget,
    ) -> Result<Option<m::CommitId>> {
        budget.charge(1, 0, 0)?;
        if self.failed {
            return Err(Error::Unavailable);
        }
        if position == 0 {
            return Ok(Some(m::CommitId::ROOT));
        }
        let storage_key = key(b'p', repo, &position.to_be_bytes());
        let Some(bytes) = self.table.get(&storage_key).map_err(storage)? else {
            return Ok(None);
        };
        budget.charge(0, 0, bytes.len() as u64)?;
        let id: [u8; 32] = decode(&storage_key, &bytes, limits)?;
        Ok(Some(m::CommitId(id)))
    }
    fn node_at_position(
        &self,
        repo: &[u8; 32],
        position: u64,
        limits: &Limits,
        budget: &mut WorkBudget,
    ) -> Result<Option<StoredNode>> {
        let Some(id) = self.position_id(repo, position, limits, budget)? else {
            return Ok(None);
        };
        let node = self.node(repo, id, limits, budget)?.ok_or(Error::Corrupt)?;
        if node.position != position {
            return Err(Error::Corrupt);
        }
        Ok(Some(node))
    }
    fn repository(
        &self,
        repo: &str,
        hash: &[u8; 32],
        limits: &Limits,
    ) -> Result<Option<Repository>> {
        let result: Option<Repository> = self.get(&key(b'r', hash, b""), limits)?;
        if result.as_ref().is_some_and(|value| value.name != repo) {
            return Err(Error::Corrupt);
        }
        Ok(result)
    }
    fn node(
        &self,
        repo: &[u8; 32],
        id: m::CommitId,
        limits: &Limits,
        budget: &mut WorkBudget,
    ) -> Result<Option<StoredNode>> {
        budget.charge(1, 0, 0)?;
        if self.failed {
            return Err(Error::Unavailable);
        }
        if id == m::CommitId::ROOT {
            return Ok(Some(synthetic()));
        }
        let Some(bytes) = self.table.get(key(b'n', repo, &id.0)).map_err(storage)? else {
            return Ok(None);
        };
        budget.charge(0, 0, bytes.len() as u64)?;
        let node: StoredNode = decode(&key(b'n', repo, &id.0), &bytes, limits)?;
        if node.commit != id.0
            || node.position == 0
            || node.generation == 0
            || node.skips.len() > 64
        {
            return Err(Error::Corrupt);
        }
        Ok(Some(node))
    }
}
impl Index {
    pub fn open(path: impl AsRef<Path>, limits: &Limits) -> Result<Arc<Self>> {
        limits.validate()?;
        let directory = PrivateDir::create(path)?;
        let lock = directory.open_lock("index.lock")?;
        lock.try_lock_exclusive()?;
        // Only an absent data directory may acquire a new logical schema.
        // Fjall otherwise initializes a missing marker itself; doing that to
        // partial/foreign derived state would silently adopt it.
        let data_path = directory.path().join("index.fjall");
        let fresh = match std::fs::symlink_metadata(&data_path) {
            Ok(metadata) => {
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(Error::Corrupt);
                }
                let marker_path = data_path.join("version");
                let marker = std::fs::symlink_metadata(&marker_path).map_err(|_| Error::Corrupt)?;
                if !marker.is_file() || marker.len() != 4 {
                    return Err(Error::Corrupt);
                }
                let file = File::open(marker_path).map_err(|_| Error::Corrupt)?;
                let opened = file.metadata().map_err(|_| Error::Corrupt)?;
                if !opened.is_file() || opened.len() != 4 {
                    return Err(Error::Corrupt);
                }
                let mut bytes = Vec::with_capacity(5);
                file.take(5)
                    .read_to_end(&mut bytes)
                    .map_err(|_| Error::Corrupt)?;
                if bytes != b"FJL\x03" {
                    return Err(Error::Corrupt);
                }
                false
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => return Err(error.into()),
        };
        let db = SingleWriterTxDatabase::builder(data_path)
            .open()
            .map_err(storage)?;
        if !fresh && !db.keyspace_exists("graph") {
            return Err(Error::Corrupt);
        }
        let table = db
            .keyspace("graph", KeyspaceCreateOptions::default)
            .map_err(storage)?;
        let header = if let Some(bytes) = table.get(b"header").map_err(storage)? {
            decode(b"header", &bytes, limits)?
        } else {
            if !fresh || db.read_tx().prefix(&table, b"").next().is_some() {
                return Err(Error::Corrupt);
            }
            let header = Header {
                schema: STORAGE_SCHEMA,
                native_format: m::FORMAT_VERSION,
                epoch: *uuid::Uuid::new_v4().as_bytes(),
                repositories: 0,
                nodes: 0,
                bytes: 0,
            };
            let bytes = encode(b"header", &header)?;
            if bytes.len() > limits.max_record_bytes {
                return Err(Error::Invalid("derived header size"));
            }
            let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
            tx.insert(&table, b"header", bytes);
            tx.commit().map_err(storage)?;
            header
        };
        if header.schema != STORAGE_SCHEMA || header.native_format != m::FORMAT_VERSION {
            return Err(Error::UnsupportedSchema);
        }
        if header.epoch == [0; 16]
            || header.nodes > limits.max_nodes as u64
            || header.repositories > limits.max_repositories as u64
            || header.bytes > limits.max_index_bytes
        {
            return Err(Error::Corrupt);
        }
        let state = State {
            db,
            table,
            header,
            failed: false,
        };
        validate_storage(&state, limits)?;
        Ok(Arc::new(Self {
            _directory: directory,
            _lock: lock,
            limits: limits.clone(),
            state: Mutex::new(state),
        }))
    }
    pub fn epoch(&self) -> [u8; 16] {
        self.state.lock().unwrap().header.epoch
    }
    pub fn stats(&self) -> Result<IndexStats> {
        let state = self.state.lock().unwrap();
        if state.failed {
            return Err(Error::Unavailable);
        }
        Ok(IndexStats {
            repositories: state.header.repositories,
            nodes: state.header.nodes,
            logical_bytes: state.header.bytes,
        })
    }

    pub fn ingest(
        &self,
        repository: &str,
        descriptor: m::Descriptor,
        bytes: &[u8],
    ) -> Result<CommitNode> {
        let repo = repo_hash(repository)?;
        if descriptor.key.kind != m::Kind::Commit || descriptor.key.id.0 == [0; 32] {
            return Err(Error::Invalid("canonical commit descriptor"));
        }
        let model = m::Limits {
            max_items: self.limits.model.max_items.min(self.limits.max_parents),
            ..self.limits.model
        };
        m::verify(&descriptor, bytes, model)?;
        let m::Object::Commit(commit) = m::decode(bytes, model)? else {
            return Err(Error::Invalid("canonical commit kind"));
        };
        let canonical = m::encode(&m::Object::Commit(commit.clone()), model)?;
        if canonical.bytes != bytes || canonical.descriptor != descriptor {
            return Err(Error::Invalid("noncanonical commit"));
        }
        if commit.parents.len() > self.limits.max_parents {
            return Err(Error::Invalid("parent count"));
        }
        let mut state = self.state.lock().unwrap();
        let mut budget = WorkBudget::new(
            u64::MAX,
            u64::MAX,
            self.limits.max_index_bytes,
            self.limits.max_nodes,
        );
        let old = state.node(
            &repo,
            m::CommitId(descriptor.key.id.0),
            &self.limits,
            &mut budget,
        )?;
        if let Some(old) = old {
            if old.change != commit.change_id.0
                || old.parents != commit.parents.iter().map(|id| id.0).collect::<Vec<_>>()
                || old.size != descriptor.size_bytes
                || old.committer_millis != commit.committer.timestamp.millis_since_epoch
                || old.has_conflict != (commit.root_tree.terms.len() > 1)
            {
                return Err(Error::Corrupt);
            }
            return Ok(old.public());
        }
        let existing = state.repository(repository, &repo, &self.limits)?;
        let mut metadata = existing.clone().unwrap_or(Repository {
            name: repository.into(),
            high_water: 0,
            generation: 0,
        });
        // Merge fan-in must not retain each parent's own ancestry vectors.
        // At most one bounded record is live per iteration; only a sole parent
        // is retained for single-chain skip construction.
        let mut parent_generation = 0u64;
        let mut sole_parent = None;
        for id in &commit.parents {
            let parent = state
                .node(&repo, *id, &self.limits, &mut budget)?
                .ok_or(Error::NotReady)?;
            if parent.position > metadata.high_water {
                return Err(Error::Corrupt);
            }
            parent_generation = parent_generation.max(parent.generation);
            if commit.parents.len() == 1 {
                sole_parent = Some(parent);
            }
        }
        let position = metadata.high_water.checked_add(1).ok_or(Error::Corrupt)?;
        let generation = parent_generation.checked_add(1).ok_or(Error::Corrupt)?;
        let skips = build_skips(
            &state,
            &repo,
            sole_parent.as_ref(),
            &self.limits,
            &mut budget,
        )?;
        let node = StoredNode {
            commit: descriptor.key.id.0,
            change: commit.change_id.0,
            parents: commit.parents.iter().map(|id| id.0).collect(),
            position,
            generation,
            size: descriptor.size_bytes,
            committer_millis: commit.committer.timestamp.millis_since_epoch,
            has_conflict: commit.root_tree.terms.len() > 1,
            skips,
        };
        let node_bytes = encode(&key(b'n', &repo, &node.commit), &node)?;
        if node_bytes.len() > self.limits.max_record_bytes {
            return Err(Error::Invalid("derived node size"));
        }
        metadata.high_water = position;
        metadata.generation = metadata.generation.checked_add(1).ok_or(Error::Corrupt)?;
        let mut header = state.header.clone();
        header.nodes = header.nodes.checked_add(1).ok_or(Error::Corrupt)?;
        header.repositories = header
            .repositories
            .checked_add(u64::from(existing.is_none()))
            .ok_or(Error::Corrupt)?;
        let position_bytes = encode(&key(b'p', &repo, &position.to_be_bytes()), &node.commit)?;
        let change_bytes = encode(&change_key(&repo, &node.change, &node.commit), &position)?;
        let metadata_bytes = encode(&key(b'r', &repo, b""), &metadata)?;
        let added = node_bytes.len()
            + 65
            + position_bytes.len()
            + 41
            + change_bytes.len()
            + 81
            + metadata_bytes.len()
            + 33;
        let removed = existing
            .as_ref()
            .map(|before| encode(&key(b'r', &repo, b""), before).map(|bytes| bytes.len() + 33))
            .transpose()?
            .unwrap_or(0);
        header.bytes = header
            .bytes
            .checked_sub(removed as u64)
            .and_then(|bytes| bytes.checked_add(added as u64))
            .ok_or(Error::Corrupt)?;
        if header.nodes > self.limits.max_nodes as u64
            || header.repositories > self.limits.max_repositories as u64
            || header.bytes > self.limits.max_index_bytes
        {
            return Err(Error::Invalid("persistent graph budget"));
        }
        let header_bytes = encode(b"header", &header)?;
        if [
            &node_bytes,
            &position_bytes,
            &change_bytes,
            &metadata_bytes,
            &header_bytes,
        ]
        .iter()
        .any(|bytes| bytes.len() > self.limits.max_record_bytes)
        {
            return Err(Error::Invalid("derived record size"));
        }
        let mut tx = state.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&state.table, key(b'n', &repo, &node.commit), node_bytes);
        tx.insert(
            &state.table,
            key(b'p', &repo, &position.to_be_bytes()),
            position_bytes,
        );
        tx.insert(
            &state.table,
            change_key(&repo, &node.change, &node.commit),
            change_bytes,
        );
        tx.insert(&state.table, key(b'r', &repo, b""), metadata_bytes);
        tx.insert(&state.table, b"header", header_bytes);
        if tx.commit().is_err() {
            state.failed = true;
            return Err(Error::Unavailable);
        }
        state.header = header;
        Ok(node.public())
    }
    pub fn node(&self, repository: &str, id: m::CommitId) -> Result<Option<CommitNode>> {
        let repo = repo_hash(repository)?;
        let state = self.state.lock().unwrap();
        state.repository(repository, &repo, &self.limits)?;
        let mut budget = WorkBudget::new(
            u64::MAX,
            u64::MAX,
            self.limits.max_index_bytes,
            self.limits.max_nodes,
        );
        Ok(state
            .node(&repo, id, &self.limits, &mut budget)?
            .map(|node| node.public()))
    }
    pub fn scope(self: &Arc<Self>, repository: &str, roots: &[m::CommitId]) -> Result<Scope> {
        let repo = repo_hash(repository)?;
        if roots.is_empty() || roots.len() > self.limits.max_roots {
            return Err(Error::Invalid("scope root count"));
        }
        let state = self.state.lock().unwrap();
        let metadata = state
            .repository(repository, &repo, &self.limits)?
            .unwrap_or(Repository {
                name: repository.into(),
                high_water: 0,
                generation: 0,
            });
        let mut sorted = roots.to_vec();
        sorted.sort();
        sorted.dedup();
        let mut budget = WorkBudget::new(
            self.limits.max_roots as u64,
            u64::MAX,
            self.limits.max_index_bytes,
            self.limits.max_roots,
        );
        for root in &sorted {
            state
                .node(&repo, *root, &self.limits, &mut budget)?
                .ok_or(Error::NotReady)?;
        }
        Ok(Scope {
            index: self.clone(),
            repository: repository.into(),
            repo,
            roots: sorted,
            epoch: state.header.epoch,
            high_water: metadata.high_water,
            generation: metadata.generation,
        })
    }
}

fn build_skips(
    state: &State,
    repo: &[u8; 32],
    parent: Option<&StoredNode>,
    limits: &Limits,
    budget: &mut WorkBudget,
) -> Result<Vec<u64>> {
    let Some(parent) = parent else {
        return Ok(Vec::new());
    };
    let mut skips = vec![parent.position];
    for level in 1..64 {
        let loaded;
        let middle = if level == 1 {
            parent
        } else {
            loaded = state
                .node_at_position(repo, skips[level - 1], limits, budget)?
                .ok_or(Error::Corrupt)?;
            &loaded
        };
        let Some(target) = middle.skips.get(level - 1) else {
            break;
        };
        if *target >= skips[level - 1] {
            return Err(Error::Corrupt);
        }
        skips.push(*target);
    }
    Ok(skips)
}
fn validate_storage(state: &State, limits: &Limits) -> Result<()> {
    let mut repositories = BTreeMap::new();
    let mut counts = [0u64; 3];
    let mut total = 0u64;
    let mut rows = 0usize;
    for item in state.db.read_tx().prefix(&state.table, b"") {
        let (key_bytes, value) = item.into_inner().map_err(storage)?;
        if key_bytes.as_ref() == b"header" {
            continue;
        }
        rows = rows.checked_add(1).ok_or(Error::Corrupt)?;
        if rows
            > limits
                .max_nodes
                .saturating_mul(3)
                .saturating_add(limits.max_repositories)
        {
            return Err(Error::Corrupt);
        }
        total = total
            .checked_add((key_bytes.len() + value.len()) as u64)
            .ok_or(Error::Corrupt)?;
        if total > limits.max_index_bytes
            || value.len() > limits.max_record_bytes
            || key_bytes.len() < 33
        {
            return Err(Error::Corrupt);
        }
        let repo: [u8; 32] = key_bytes[1..33].try_into().map_err(|_| Error::Corrupt)?;
        match key_bytes[0] {
            b'r' if key_bytes.len() == 33 => {
                let metadata: Repository = decode(&key_bytes, &value, limits)?;
                if repo_hash(&metadata.name)? != repo
                    || metadata.high_water == 0
                    || metadata.generation != metadata.high_water
                    || metadata.high_water > limits.max_nodes as u64
                {
                    return Err(Error::Corrupt);
                }
                repositories.insert(repo, metadata);
                if repositories.len() > limits.max_repositories {
                    return Err(Error::Corrupt);
                }
            }
            b'n' if key_bytes.len() == 65 => {
                let _: StoredNode = decode(&key_bytes, &value, limits)?;
                counts[0] += 1;
            }
            b'p' if key_bytes.len() == 41 => {
                let _: [u8; 32] = decode(&key_bytes, &value, limits)?;
                counts[1] += 1;
            }
            b'c' if key_bytes.len() == 81 => {
                let _: u64 = decode(&key_bytes, &value, limits)?;
                counts[2] += 1;
            }
            _ => return Err(Error::Corrupt),
        }
    }
    if total != state.header.bytes
        || counts != [state.header.nodes; 3]
        || repositories.len() as u64 != state.header.repositories
    {
        return Err(Error::Corrupt);
    }
    let mut nodes = 0u64;
    let mut budget = WorkBudget::new(u64::MAX, u64::MAX, u64::MAX, limits.max_nodes);
    for (repo, metadata) in repositories {
        nodes = nodes
            .checked_add(metadata.high_water)
            .ok_or(Error::Corrupt)?;
        for position in 1..=metadata.high_water {
            let id: [u8; 32] = state
                .get(&key(b'p', &repo, &position.to_be_bytes()), limits)?
                .ok_or(Error::Corrupt)?;
            let node = state
                .node(&repo, m::CommitId(id), limits, &mut budget)?
                .ok_or(Error::Corrupt)?;
            if node.position != position
                || node.parents.len() > limits.max_parents
                || node.size > limits.model.max_object_bytes as u64
            {
                return Err(Error::Corrupt);
            }
            let indexed: u64 = state
                .get(&change_key(&repo, &node.change, &node.commit), limits)?
                .ok_or(Error::Corrupt)?;
            if indexed != position {
                return Err(Error::Corrupt);
            }
            let mut parent_generation = 0u64;
            let mut sole_parent = None;
            for parent in &node.parents {
                let parent = state
                    .node(&repo, m::CommitId(*parent), limits, &mut budget)?
                    .ok_or(Error::Corrupt)?;
                if parent.position >= position {
                    return Err(Error::Corrupt);
                }
                parent_generation = parent_generation.max(parent.generation);
                if node.parents.len() == 1 {
                    sole_parent = Some(parent);
                }
            }
            let generation = parent_generation.checked_add(1).ok_or(Error::Corrupt)?;
            if node.generation != generation
                || node.skips
                    != build_skips(state, &repo, sole_parent.as_ref(), limits, &mut budget)?
            {
                return Err(Error::Corrupt);
            }
        }
    }
    if nodes != state.header.nodes {
        return Err(Error::Corrupt);
    }
    Ok(())
}

enum AncestorLocator {
    Id(m::CommitId, Option<u64>),
    Position(u64),
}

impl Scope {
    pub fn repository(&self) -> &str {
        &self.repository
    }
    pub fn epoch(&self) -> [u8; 16] {
        self.epoch
    }
    pub fn high_water(&self) -> u64 {
        self.high_water
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn roots(&self) -> &[m::CommitId] {
        &self.roots
    }
    fn read(
        &self,
        state: &State,
        id: m::CommitId,
        budget: &mut WorkBudget,
    ) -> Result<Option<StoredNode>> {
        if state.header.epoch != self.epoch {
            return Err(Error::Unavailable);
        }
        Ok(state
            .node(&self.repo, id, &self.index.limits, budget)?
            .filter(|node| node.position <= self.high_water))
    }
    fn ancestor(
        &self,
        state: &State,
        ancestor: m::CommitId,
        descendant: m::CommitId,
        budget: &mut WorkBudget,
    ) -> Result<bool> {
        let Some(target) = self.read(state, ancestor, budget)? else {
            return Ok(false);
        };
        let target_key = (target.commit, target.position, target.generation);
        drop(target);
        let Some(start) = self.read(state, descendant, budget)? else {
            return Ok(false);
        };
        let mut frontier = vec![AncestorLocator::Id(descendant, Some(start.position))];
        let mut initial = Some(start);
        let mut visited = HashSet::new();
        while let Some(locator) = frontier.pop() {
            let (id, expected) = match locator {
                AncestorLocator::Id(id, expected) => (id, expected),
                AncestorLocator::Position(position) => {
                    if position > self.high_water {
                        return Err(Error::Corrupt);
                    }
                    (
                        state
                            .position_id(&self.repo, position, &self.index.limits, budget)?
                            .ok_or(Error::Corrupt)?,
                        Some(position),
                    )
                }
            };
            if !visited.insert(id.0) {
                continue;
            }
            budget.frontier(visited.len().saturating_add(frontier.len()))?;
            let node = if id == descendant {
                initial.take().ok_or(Error::Corrupt)?
            } else {
                self.read(state, id, budget)?.ok_or(Error::Corrupt)?
            };
            if expected.is_some_and(|position| node.position != position) {
                return Err(Error::Corrupt);
            }
            if node.commit == target_key.0 {
                return Ok(true);
            }
            if node.generation <= target_key.2 || node.position <= target_key.1 {
                continue;
            }
            if node.parents.len() == 1 {
                let Some((level, next)) = node
                    .skips
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, position)| **position >= target_key.1)
                else {
                    continue;
                };
                if *next >= node.position {
                    return Err(Error::Corrupt);
                }
                budget.charge(0, 1, 0)?;
                budget.frontier(
                    visited
                        .len()
                        .saturating_add(frontier.len())
                        .saturating_add(1),
                )?;
                if level == 0 {
                    let id = m::CommitId(node.parents[0]);
                    if !visited.contains(&id.0) {
                        frontier.push(AncestorLocator::Id(id, Some(*next)));
                    }
                } else {
                    frontier.push(AncestorLocator::Position(*next));
                }
            } else {
                budget.charge(0, node.parents.len() as u64, 0)?;
                let pending = node
                    .parents
                    .iter()
                    .filter(|parent| !visited.contains(*parent))
                    .count();
                budget.frontier(
                    visited
                        .len()
                        .saturating_add(frontier.len())
                        .saturating_add(pending),
                )?;
                for parent in node.parents {
                    if !visited.contains(&parent) {
                        frontier.push(AncestorLocator::Id(m::CommitId(parent), None));
                    }
                }
            }
        }
        Ok(false)
    }
    fn member(&self, state: &State, id: m::CommitId, budget: &mut WorkBudget) -> Result<bool> {
        for root in &self.roots {
            if self.ancestor(state, id, *root, budget)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub fn has_id(&self, id: m::CommitId, budget: &mut WorkBudget) -> Result<bool> {
        let state = self.index.state.lock().unwrap();
        if state.failed || state.header.epoch != self.epoch {
            return Err(Error::Unavailable);
        }
        self.member(&state, id, budget)
    }
    pub fn is_ancestor(
        &self,
        ancestor: m::CommitId,
        descendant: m::CommitId,
        budget: &mut WorkBudget,
    ) -> Result<bool> {
        let state = self.index.state.lock().unwrap();
        if state.failed || state.header.epoch != self.epoch {
            return Err(Error::Unavailable);
        }
        if !self.member(&state, ancestor, budget)? || !self.member(&state, descendant, budget)? {
            return Ok(false);
        }
        self.ancestor(&state, ancestor, descendant, budget)
    }
    pub fn heads(
        &self,
        commits: &[m::CommitId],
        budget: &mut WorkBudget,
    ) -> Result<Vec<m::CommitId>> {
        let state = self.index.state.lock().unwrap();
        if state.failed || state.header.epoch != self.epoch {
            return Err(Error::Unavailable);
        }
        self.heads_locked(&state, commits, budget)
    }
    fn heads_locked(
        &self,
        state: &State,
        commits: &[m::CommitId],
        budget: &mut WorkBudget,
    ) -> Result<Vec<m::CommitId>> {
        budget.frontier(commits.len())?;
        let candidates: BTreeSet<_> = commits.iter().copied().collect();
        for id in &candidates {
            if !self.member(state, *id, budget)? {
                return Err(Error::NotReady);
            }
        }
        let mut result = Vec::new();
        'candidate: for candidate in &candidates {
            for other in &candidates {
                if candidate != other && self.ancestor(state, *candidate, *other, budget)? {
                    continue 'candidate;
                }
            }
            result.push(*candidate);
        }
        Ok(result)
    }
    /// Maximal intersection of the ancestors of each input set's union.
    /// Inputs outside this frozen logical graph are rejected. Descending stable
    /// positions combine every incoming color before a node is visited; emitted
    /// common nodes dominate their ancestors, which need not become results.
    pub fn common_ancestors(
        &self,
        left: &[m::CommitId],
        right: &[m::CommitId],
        budget: &mut WorkBudget,
    ) -> Result<Vec<m::CommitId>> {
        budget.frontier(
            left.len()
                .checked_add(right.len())
                .ok_or(Error::BudgetExceeded)?,
        )?;
        let state = self.index.state.lock().unwrap();
        if state.failed || state.header.epoch != self.epoch {
            return Err(Error::Unavailable);
        }
        for id in left.iter().chain(right) {
            if !self.member(&state, *id, budget)? {
                return Err(Error::NotReady);
            }
        }
        if left.is_empty() || right.is_empty() {
            return Ok(Vec::new());
        }
        // Linear histories and ancestor-related roots should use the verified
        // chain accelerators instead of enumerating their public ancestry.
        if let ([left], [right]) = (left, right) {
            if self.ancestor(&state, *left, *right, budget)? {
                return Ok(vec![*left]);
            }
            if self.ancestor(&state, *right, *left, budget)? {
                return Ok(vec![*right]);
            }
        }
        let mut frontier = CommonFrontier::default();
        for (roots, color) in [(left, LEFT_COLOR), (right, RIGHT_COLOR)] {
            for id in roots {
                let node = self.read(&state, *id, budget)?.ok_or(Error::NotReady)?;
                frontier.push(&node, color, 0, budget)?;
            }
        }
        let mut result = Vec::new();
        while frontier.undominated != 0 {
            let (position, (id, mut flags)) = frontier.pop().ok_or(Error::Corrupt)?;
            if flags & BOTH_COLORS == BOTH_COLORS && flags & DOMINATED == 0 {
                budget.frontier(
                    frontier
                        .pending
                        .len()
                        .checked_add(result.len())
                        .and_then(|n| n.checked_add(1))
                        .ok_or(Error::BudgetExceeded)?,
                )?;
                result.push(id);
                flags |= DOMINATED;
            }
            if flags & DOMINATED != 0 && frontier.undominated == 0 {
                break;
            }
            let node = self.read(&state, id, budget)?.ok_or(Error::Corrupt)?;
            if node.position != position {
                return Err(Error::Corrupt);
            }
            budget.charge(0, node.parents.len() as u64, 0)?;
            for parent in node.parents {
                let parent = self
                    .read(&state, m::CommitId(parent), budget)?
                    .ok_or(Error::Corrupt)?;
                if parent.position >= position {
                    return Err(Error::Corrupt);
                }
                frontier.push(&parent, flags, result.len(), budget)?;
            }
        }
        result.sort();
        Ok(result)
    }
    pub fn resolve_commit_prefix(
        &self,
        prefix: &str,
        budget: &mut WorkBudget,
    ) -> Result<PrefixResolution<m::CommitId>> {
        validate_prefix(prefix, 64)?;
        let state = self.index.state.lock().unwrap();
        if state.failed || state.header.epoch != self.epoch {
            return Err(Error::Unavailable);
        }
        let mut found = None;
        // Synthetic root participates in the same exact prefix domain.
        if hex(&[0; 32]).starts_with(prefix) && self.member(&state, m::CommitId::ROOT, budget)? {
            found = Some(m::CommitId::ROOT)
        }
        let key_prefix = key(b'n', &self.repo, &prefix_bytes(prefix));
        for item in state.db.read_tx().prefix(&state.table, &key_prefix) {
            let (key, value) = item.into_inner().map_err(storage)?;
            budget.charge(1, 0, (key.len() + value.len()) as u64)?;
            if key.len() != 65 {
                return Err(Error::Corrupt);
            }
            let id = m::CommitId(key[33..].try_into().map_err(|_| Error::Corrupt)?);
            if !hex(&id.0).starts_with(prefix) {
                continue;
            }
            if self.member(&state, id, budget)? {
                if found.is_some_and(|before| before != id) {
                    return Ok(PrefixResolution::Ambiguous);
                }
                found = Some(id);
            }
        }
        Ok(found.map_or(PrefixResolution::NoMatch, PrefixResolution::Single))
    }
    pub fn resolve_change_prefix(
        &self,
        prefix: &str,
        budget: &mut WorkBudget,
    ) -> Result<PrefixResolution<ChangeTargets>> {
        validate_prefix(prefix, 32)?;
        let state = self.index.state.lock().unwrap();
        if state.failed || state.header.epoch != self.epoch {
            return Err(Error::Unavailable);
        }
        let mut change = None;
        let mut commits = Vec::new();
        if hex(&[0; 16]).starts_with(prefix) && self.member(&state, m::CommitId::ROOT, budget)? {
            change = Some(m::ChangeId::ROOT);
            commits.push(m::CommitId::ROOT);
        }
        let key_prefix = key(b'c', &self.repo, &prefix_bytes(prefix));
        for item in state.db.read_tx().prefix(&state.table, &key_prefix) {
            let (key, value) = item.into_inner().map_err(storage)?;
            budget.charge(1, 0, (key.len() + value.len()) as u64)?;
            if key.len() != 81 {
                return Err(Error::Corrupt);
            }
            // The change row participates in this read even though scope
            // membership is subsequently verified through the primary graph.
            let _: u64 = decode(&key, &value, &self.index.limits)?;
            let candidate = m::ChangeId(key[33..49].try_into().map_err(|_| Error::Corrupt)?);
            let id = m::CommitId(key[49..].try_into().map_err(|_| Error::Corrupt)?);
            if !hex(&candidate.0).starts_with(prefix) {
                continue;
            }
            if self.member(&state, id, budget)? {
                if change.is_some_and(|before| before != candidate) {
                    return Ok(PrefixResolution::Ambiguous);
                }
                change = Some(candidate);
                budget.frontier(commits.len().saturating_add(1))?;
                commits.push(id);
            }
        }
        commits.sort();
        commits.dedup();
        Ok(change.map_or(PrefixResolution::NoMatch, |change_id| {
            PrefixResolution::Single(ChangeTargets { change_id, commits })
        }))
    }
    /// Minimum hexadecimal length distinguishing this ID, including absent IDs.
    /// Zero is valid when the empty prefix already resolves uniquely.
    pub fn shortest_commit_prefix(
        &self,
        id: m::CommitId,
        budget: &mut WorkBudget,
    ) -> Result<usize> {
        let text = hex(&id.0);
        for length in 0..=64 {
            match self.resolve_commit_prefix(&text[..length], budget)? {
                PrefixResolution::NoMatch => return Ok(length),
                PrefixResolution::Single(candidate) if candidate == id => return Ok(length),
                _ => {}
            }
        }
        Err(Error::Corrupt)
    }
    pub fn shortest_change_prefix(
        &self,
        id: m::ChangeId,
        budget: &mut WorkBudget,
    ) -> Result<usize> {
        let text = hex(&id.0);
        for length in 0..=32 {
            match self.resolve_change_prefix(&text[..length], budget)? {
                PrefixResolution::NoMatch => return Ok(length),
                PrefixResolution::Single(targets) if targets.change_id == id => return Ok(length),
                _ => {}
            }
        }
        Err(Error::Corrupt)
    }
}

const LEFT_COLOR: u8 = 1;
const RIGHT_COLOR: u8 = 2;
const BOTH_COLORS: u8 = LEFT_COLOR | RIGHT_COLOR;
const DOMINATED: u8 = 4;
/// Only immutable IDs, topological positions and flags are queued, never full
/// parent records. A parent is strictly older than its child, so all incoming
/// flags are combined before pop and a popped node cannot be reinserted.
#[derive(Default)]
struct CommonFrontier {
    pending: BTreeMap<u64, (m::CommitId, u8)>,
    undominated: usize,
}
impl CommonFrontier {
    fn push(
        &mut self,
        node: &StoredNode,
        flags: u8,
        output_len: usize,
        budget: &WorkBudget,
    ) -> Result<()> {
        if let Some((id, current)) = self.pending.get_mut(&node.position) {
            if id.0 != node.commit {
                return Err(Error::Corrupt);
            }
            if *current & DOMINATED == 0 && flags & DOMINATED != 0 {
                self.undominated -= 1;
            }
            *current |= flags;
        } else {
            budget.frontier(
                self.pending
                    .len()
                    .checked_add(output_len)
                    .and_then(|n| n.checked_add(1))
                    .ok_or(Error::BudgetExceeded)?,
            )?;
            self.pending
                .insert(node.position, (m::CommitId(node.commit), flags));
            if flags & DOMINATED == 0 {
                self.undominated += 1;
            }
        }
        Ok(())
    }
    fn pop(&mut self) -> Option<(u64, (m::CommitId, u8))> {
        let item = self.pending.pop_last()?;
        if item.1.1 & DOMINATED == 0 {
            self.undominated -= 1;
        }
        Some(item)
    }
}
fn validate_prefix(prefix: &str, width: usize) -> Result<()> {
    if prefix.len() > width
        || !prefix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::Invalid("lowercase hexadecimal prefix"));
    }
    Ok(())
}
fn prefix_bytes(prefix: &str) -> Vec<u8> {
    prefix
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let nibble = |byte: u8| {
                if byte <= b'9' {
                    byte - b'0'
                } else {
                    byte - b'a' + 10
                }
            };
            (nibble(pair[0]) << 4) | nibble(pair[1])
        })
        .collect()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests;
