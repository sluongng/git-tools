//! Shared rebuildable graph facts. Neither an index node nor a scope is a
//! canonical-object residency, authorization, or remote-retention proof.
use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
use fs2::FileExt;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs::File,
    path::Path,
    sync::{Arc, Mutex},
};
use tinytree_model as m;

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid index input: {0}")]
    Invalid(&'static str),
    #[error("derived index is corrupt; explicit rebuild required")]
    Corrupt,
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
    pub fn nodes_read(&self) -> u64 {
        self.nodes
    }
    pub fn edges_read(&self) -> u64 {
        self.edges
    }
    pub fn bytes_read(&self) -> u64 {
        self.bytes
    }
    fn charge(&mut self, nodes: u64, edges: u64, bytes: u64) -> Result<()> {
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
    fn frontier(&self, size: usize) -> Result<()> {
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

#[derive(Clone, Serialize, Deserialize)]
struct Header {
    schema: u32,
    epoch: [u8; 16],
    repositories: u64,
    nodes: u64,
    bytes: u64,
}
#[derive(Clone, Serialize, Deserialize)]
struct Repository {
    name: String,
    high_water: u64,
    generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Skip {
    commit: [u8; 32],
    position: u64,
    generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredNode {
    commit: [u8; 32],
    change: [u8; 16],
    parents: Vec<[u8; 32]>,
    position: u64,
    generation: u64,
    size: u64,
    skips: Vec<Skip>,
}
impl StoredNode {
    fn public(&self) -> CommitNode {
        CommitNode {
            commit: m::CommitId(self.commit),
            change_id: m::ChangeId(self.change),
            parents: self.parents.iter().copied().map(m::CommitId).collect(),
            position: self.position,
            generation: self.generation,
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
#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    value: T,
    checksum: [u8; 32],
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Corrupt)?;
    serde_json::to_vec(&Envelope {
        value,
        checksum: *blake3::hash(&bytes).as_bytes(),
    })
    .map_err(|_| Error::Corrupt)
}
fn decode<T: Serialize + DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T> {
    if bytes.len() > limit {
        return Err(Error::Corrupt);
    }
    let envelope: Envelope<T> = serde_json::from_slice(bytes).map_err(|_| Error::Corrupt)?;
    if *blake3::hash(&serde_json::to_vec(&envelope.value).map_err(|_| Error::Corrupt)?).as_bytes()
        != envelope.checksum
    {
        return Err(Error::Corrupt);
    }
    Ok(envelope.value)
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
    fn get<T: Serialize + DeserializeOwned>(&self, key: &[u8], limit: usize) -> Result<Option<T>> {
        if self.failed {
            return Err(Error::Unavailable);
        }
        self.table
            .get(key)
            .map_err(storage)?
            .map(|bytes| decode(&bytes, limit))
            .transpose()
    }
    fn repository(&self, repo: &str, hash: &[u8; 32], limit: usize) -> Result<Option<Repository>> {
        let result: Option<Repository> = self.get(&key(b'r', hash, b""), limit)?;
        if result.as_ref().is_some_and(|value| value.name != repo) {
            return Err(Error::Corrupt);
        }
        Ok(result)
    }
    fn node(
        &self,
        repo: &[u8; 32],
        id: m::CommitId,
        limit: usize,
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
        let node: StoredNode = decode(&bytes, limit)?;
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
        let db = SingleWriterTxDatabase::builder(directory.path().join("index.fjall"))
            .open()
            .map_err(storage)?;
        let table = db
            .keyspace("graph", KeyspaceCreateOptions::default)
            .map_err(storage)?;
        let header = if let Some(bytes) = table.get(b"header").map_err(storage)? {
            decode(&bytes, limits.max_record_bytes)?
        } else {
            if db.read_tx().prefix(&table, b"").next().is_some() {
                return Err(Error::Corrupt);
            }
            let header = Header {
                schema: 1,
                epoch: *uuid::Uuid::new_v4().as_bytes(),
                repositories: 0,
                nodes: 0,
                bytes: 0,
            };
            let bytes = encode(&header)?;
            if bytes.len() > limits.max_record_bytes {
                return Err(Error::Invalid("derived header size"));
            }
            let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
            tx.insert(&table, b"header", bytes);
            tx.commit().map_err(storage)?;
            header
        };
        if header.schema != 1
            || header.epoch == [0; 16]
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
            self.limits.max_record_bytes,
            &mut budget,
        )?;
        if let Some(old) = old {
            if old.change != commit.change_id.0
                || old.parents != commit.parents.iter().map(|id| id.0).collect::<Vec<_>>()
                || old.size != descriptor.size_bytes
            {
                return Err(Error::Corrupt);
            }
            return Ok(old.public());
        }
        let existing = state.repository(repository, &repo, self.limits.max_record_bytes)?;
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
                .node(&repo, *id, self.limits.max_record_bytes, &mut budget)?
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
            skips,
        };
        let node_bytes = encode(&node)?;
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
        let position_bytes = encode(&node.commit)?;
        let change_bytes = encode(&position)?;
        let metadata_bytes = encode(&metadata)?;
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
            .map(|before| encode(before).map(|bytes| bytes.len() + 33))
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
        let header_bytes = encode(&header)?;
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
        state.repository(repository, &repo, self.limits.max_record_bytes)?;
        let mut budget = WorkBudget::new(
            u64::MAX,
            u64::MAX,
            self.limits.max_index_bytes,
            self.limits.max_nodes,
        );
        Ok(state
            .node(&repo, id, self.limits.max_record_bytes, &mut budget)?
            .map(|node| node.public()))
    }
    pub fn scope(self: &Arc<Self>, repository: &str, roots: &[m::CommitId]) -> Result<Scope> {
        let repo = repo_hash(repository)?;
        if roots.is_empty() || roots.len() > self.limits.max_roots {
            return Err(Error::Invalid("scope root count"));
        }
        let state = self.state.lock().unwrap();
        let metadata = state
            .repository(repository, &repo, self.limits.max_record_bytes)?
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
                .node(&repo, *root, self.limits.max_record_bytes, &mut budget)?
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
) -> Result<Vec<Skip>> {
    let Some(parent) = parent else {
        return Ok(Vec::new());
    };
    let mut skips = vec![Skip {
        commit: parent.commit,
        position: parent.position,
        generation: parent.generation,
    }];
    for level in 1..64 {
        let middle = state
            .node(
                repo,
                m::CommitId(skips[level - 1].commit),
                limits.max_record_bytes,
                budget,
            )?
            .ok_or(Error::Corrupt)?;
        let Some(target) = middle.skips.get(level - 1) else {
            break;
        };
        if target.position >= skips[level - 1].position {
            return Err(Error::Corrupt);
        }
        skips.push(target.clone());
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
                let metadata: Repository = decode(&value, limits.max_record_bytes)?;
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
                let _: StoredNode = decode(&value, limits.max_record_bytes)?;
                counts[0] += 1;
            }
            b'p' if key_bytes.len() == 41 => {
                let _: [u8; 32] = decode(&value, limits.max_record_bytes)?;
                counts[1] += 1;
            }
            b'c' if key_bytes.len() == 81 => {
                let _: u64 = decode(&value, limits.max_record_bytes)?;
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
                .get(
                    &key(b'p', &repo, &position.to_be_bytes()),
                    limits.max_record_bytes,
                )?
                .ok_or(Error::Corrupt)?;
            let node = state
                .node(&repo, m::CommitId(id), limits.max_record_bytes, &mut budget)?
                .ok_or(Error::Corrupt)?;
            if node.position != position
                || node.parents.len() > limits.max_parents
                || node.size > limits.model.max_object_bytes as u64
            {
                return Err(Error::Corrupt);
            }
            let indexed: u64 = state
                .get(
                    &change_key(&repo, &node.change, &node.commit),
                    limits.max_record_bytes,
                )?
                .ok_or(Error::Corrupt)?;
            if indexed != position {
                return Err(Error::Corrupt);
            }
            let mut parent_generation = 0u64;
            let mut sole_parent = None;
            for parent in &node.parents {
                let parent = state
                    .node(
                        &repo,
                        m::CommitId(*parent),
                        limits.max_record_bytes,
                        &mut budget,
                    )?
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
            .node(&self.repo, id, self.index.limits.max_record_bytes, budget)?
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
        let Some(start) = self.read(state, descendant, budget)? else {
            return Ok(false);
        };
        let mut frontier = vec![start];
        let mut visited = HashSet::new();
        while let Some(node) = frontier.pop() {
            if !visited.insert(node.commit) {
                continue;
            }
            budget.frontier(visited.len().saturating_add(frontier.len()))?;
            if node.commit == target.commit {
                return Ok(true);
            }
            if node.generation <= target.generation || node.position <= target.position {
                continue;
            }
            if node.parents.len() == 1 {
                let Some(next) = node.skips.iter().rev().find(|skip| {
                    skip.generation >= target.generation && skip.position >= target.position
                }) else {
                    // A later insertion on another branch can have a
                    // higher position than every node on this parent chain.
                    // No eligible jump then proves this branch cannot reach
                    // the target; it is not a malformed skip table.
                    continue;
                };
                budget.charge(0, 1, 0)?;
                frontier.push(
                    self.read(state, m::CommitId(next.commit), budget)?
                        .ok_or(Error::Corrupt)?,
                );
            } else {
                budget.charge(0, node.parents.len() as u64, 0)?;
                budget.frontier(
                    visited
                        .len()
                        .saturating_add(frontier.len())
                        .saturating_add(node.parents.len()),
                )?;
                for parent in node.parents {
                    frontier.push(
                        self.read(state, m::CommitId(parent), budget)?
                            .ok_or(Error::Corrupt)?,
                    );
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
    fn ancestors(
        &self,
        state: &State,
        roots: &[m::CommitId],
        budget: &mut WorkBudget,
    ) -> Result<BTreeSet<m::CommitId>> {
        let mut frontier = roots.to_vec();
        let mut result = BTreeSet::new();
        budget.frontier(frontier.len())?;
        while let Some(id) = frontier.pop() {
            if result.contains(&id) {
                continue;
            }
            let node = self.read(state, id, budget)?.ok_or(Error::NotReady)?;
            budget.charge(0, node.parents.len() as u64, 0)?;
            budget.frontier(
                result
                    .len()
                    .saturating_add(frontier.len())
                    .saturating_add(node.parents.len())
                    .saturating_add(1),
            )?;
            frontier.extend(node.parents.into_iter().map(m::CommitId));
            result.insert(id);
        }
        Ok(result)
    }
    /// Maximal intersection of the ancestors of each input set's union.
    /// Inputs outside this frozen logical graph are rejected.
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
        let left = self.ancestors(&state, left, budget)?;
        let right = self.ancestors(&state, right, budget)?;
        let common: Vec<_> = left.intersection(&right).copied().collect();
        self.heads_locked(&state, &common, budget)
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
