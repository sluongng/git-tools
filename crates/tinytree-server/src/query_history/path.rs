//! Matched-path merge evaluation over source-checked disposable jj storage.
use super::*;
use async_trait::async_trait;
use futures::AsyncReadExt as _;
use jj_lib::{
    backend::{BackendError, BackendResult, CommitId},
    diff::{LineDiffBudget, LineDiffLimits},
    matchers::Matcher,
    repo_path::{RepoPath, RepoPathBuf},
    store::Store,
};
use jj_native_backend::{NativeObjectIo, VerifiedMetadata};
use prost::Message as _;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tinytree_annotate::Budget as _;
use tokio::sync::OwnedSemaphorePermit;

pub(crate) struct PathState {
    diff: Option<LineDiffBudget>,
    read_bytes: usize,
    failed: bool,
    memo: Option<PrefixMemo>,
    filesets: Option<CompiledFilesets>,
}
const FILESET_OUTPUT_BYTES: usize = 8 * 1024 * 1024;
const FILESET_KEY_BYTES: usize = 256 * 1024;
pub(crate) const FILESET_MEMORY_BYTES: usize = FILESET_OUTPUT_BYTES + FILESET_KEY_BYTES;
struct OwnedMatcher {
    inner: tinytree_annotate::fileset::CompiledFileset,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl std::fmt::Debug for OwnedMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedFilesetMatcher")
            .finish_non_exhaustive()
    }
}
impl Matcher for OwnedMatcher {
    fn matches(&self, path: &RepoPath) -> bool {
        self.inner.matches(path)
    }
    fn visit(&self, path: &RepoPath) -> jj_lib::matchers::Visit {
        self.inner.visit(path)
    }
}
struct CompiledFilesets {
    rows: BTreeMap<Vec<u8>, Arc<OwnedMatcher>>,
    budget: jj_lib::matchers::MatcherBuildBudget,
    output_bytes: usize,
    key_bytes: usize,
    // Every escaping matcher handle shares this same physical reservation.
    _permit: Arc<OwnedSemaphorePermit>,
}
fn fileset_error(error: tinytree_annotate::fileset::Error) -> Status {
    match error {
        tinytree_annotate::fileset::Error::Invalid => {
            Status::invalid_argument("invalid fileset matcher")
        }
        tinytree_annotate::fileset::Error::Limit => {
            Status::resource_exhausted("fileset compiler limit exceeded")
        }
    }
}
// Query-local memoization is enabled only for repeated prefix probes. The
// owning PrefixSelection reserves this exact bounded memory independently of
// the actual graph/read/diff work already spent on producing each answer.
struct PrefixMemo {
    scope: Arc<Scoped>,
    rows: BTreeMap<(m::CommitId, Vec<u8>, String, bool), bool>,
    bytes: usize,
    maximum: usize,
    entries: usize,
}
pub(crate) fn prefix_memo_memory(config: &QueryConfig) -> usize {
    config.max_request_bytes.saturating_mul(4).max(65_536)
}
impl PathState {
    pub(crate) fn new(config: &QueryConfig) -> Self {
        Self {
            diff: Some(LineDiffBudget::new(LineDiffLimits {
                max_input_bytes: config.max_history_read_bytes,
                max_lines: config.max_blame_lines,
                max_work: config.max_blame_diff_steps,
                max_allocation_bytes: config.max_blame_diff_memory_bytes,
                max_recursion_depth: config.max_blame_diff_depth,
            })),
            read_bytes: 0,
            failed: false,
            memo: None,
            filesets: None,
        }
    }
    pub(crate) fn clear_filesets(&mut self) {
        self.filesets = None;
    }
    pub(crate) fn prepare_filesets(
        &mut self,
        expression: &q::GraphExpr,
        cursors: &crate::query_cursor::CursorHost,
        budget: &mut WorkBudget,
        deadline: Instant,
    ) -> Result<(), Status> {
        let inputs = crate::query_eval::fileset_inputs(expression).map_err(|e| match e {
            crate::query_eval::EvalError::Invalid(m) => Status::invalid_argument(m),
            _ => limit(),
        })?;
        for wire in inputs {
            check(deadline)?;
            // Structural/constructor preflight precedes encoding, stock parsing
            // and persistent allocation. The shared compiler ledger caps ALL
            // filters, including facts with multiple distinct expressions.
            let construction =
                tinytree_annotate::fileset::construction_bytes(wire).map_err(fileset_error)?;
            let stats = tinytree_proto::validate_fileset(wire).map_err(|_| limit())?;
            if self.filesets.is_none() {
                let permit = cursors.reserve_filesets(FILESET_MEMORY_BYTES)?;
                self.filesets = Some(CompiledFilesets {
                    rows: BTreeMap::new(),
                    budget: jj_lib::matchers::MatcherBuildBudget::new(
                        jj_lib::matchers::MatcherBuildLimits::default(),
                    ),
                    output_bytes: 0,
                    key_bytes: 0,
                    _permit: Arc::new(permit),
                });
            }
            let cache = self.filesets.as_mut().unwrap();
            let key_bytes = wire
                .encoded_len()
                .checked_mul(2)
                .and_then(|n| n.checked_add(512))
                .ok_or_else(limit)?;
            if cache
                .key_bytes
                .checked_add(key_bytes)
                .is_none_or(|n| n > FILESET_KEY_BYTES)
            {
                return Err(limit());
            }
            budget
                .charge(0, stats.nodes as u64, key_bytes as u64)
                .map_err(index_error)?;
            let key = wire.encode_to_vec();
            if cache.rows.contains_key(&key) {
                continue;
            }
            if cache
                .output_bytes
                .checked_add(construction)
                .is_none_or(|n| n > FILESET_OUTPUT_BYTES)
            {
                return Err(limit());
            }
            budget
                .charge(0, stats.nodes as u64, construction as u64)
                .map_err(index_error)?;
            let matcher = tinytree_annotate::fileset::compile_with_budget(wire, &mut cache.budget)
                .map_err(fileset_error)?;
            check(deadline)?;
            cache.output_bytes += construction;
            cache.key_bytes += key_bytes;
            cache.rows.insert(
                key,
                Arc::new(OwnedMatcher {
                    inner: matcher,
                    _permit: cache._permit.clone(),
                }),
            );
        }
        Ok(())
    }
    pub(crate) fn prefix(config: &QueryConfig, scope: &Arc<Scoped>) -> Self {
        let mut state = Self::new(config);
        state.memo = Some(PrefixMemo {
            scope: scope.clone(),
            rows: BTreeMap::new(),
            bytes: 0,
            maximum: prefix_memo_memory(config),
            entries: config.max_result_ids,
        });
        state
    }
}
fn limit() -> Status {
    Status::resource_exhausted("merged path work budget exhausted")
}
fn corrupt() -> Status {
    Status::data_loss("merged path source metadata mismatch")
}
fn backend(error: Status) -> BackendError {
    BackendError::Other(Box::new(error))
}
fn from_backend(error: BackendError) -> Status {
    if let BackendError::Other(inner) = &error {
        if let Some(status) = inner.downcast_ref::<Status>() {
            return status.clone();
        }
    }
    Status::data_loss("merged path canonical backend failed")
}
fn failure(error: Failure) -> Status {
    match error {
        Failure::Status(status) => status,
        Failure::Boundary(Coverage::HistoryReadLimit) => limit(),
        Failure::Boundary(_) => {
            Status::unimplemented("merged path requires canonical source ancestry")
        }
    }
}
struct Ledger {
    budget: WorkBudget,
    read_bytes: usize,
    tree_terms: usize,
}
type Shared = Arc<Mutex<Option<Ledger>>>;
fn with<R>(shared: &Shared, f: impl FnOnce(&mut Ledger) -> Result<R, Status>) -> Result<R, Status> {
    let mut guard = shared
        .lock()
        .map_err(|_| Status::internal("merged path ledger unavailable"))?;
    f(guard
        .as_mut()
        .ok_or_else(|| Status::internal("merged path ledger is not owned"))?)
}
fn check(deadline: Instant) -> Result<(), Status> {
    if Instant::now() >= deadline {
        Err(Status::deadline_exceeded("merged path deadline exceeded"))
    } else {
        Ok(())
    }
}
fn charge(shared: &Shared, deadline: Instant, items: u64, bytes: u64) -> Result<(), Status> {
    check(deadline)?;
    with(shared, |ledger| {
        ledger.budget.charge(0, items, bytes).map_err(index_error)
    })
}
#[derive(Clone, Copy)]
struct Grant {
    size: Option<u64>,
    prefix: usize,
}
fn grant_key(key: m::ObjectKey) -> (u8, [u8; 32]) {
    (key.kind as u8, key.id.0)
}
struct Io {
    host: Arc<QueryHost>,
    scope: Arc<Scoped>,
    repository: String,
    shared: Shared,
    deadline: Instant,
    // Only edges from verified source COMMIT/TREE objects confer payload reads.
    allowed: Mutex<BTreeMap<(u8, [u8; 32]), Grant>>,
    files: Mutex<BTreeMap<m::FileId, Arc<[u8]>>>,
}
impl std::fmt::Debug for Io {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergedPathScratchIo")
            .finish_non_exhaustive()
    }
}
impl Io {
    fn limits(&self) -> m::Limits {
        m::Limits {
            max_object_bytes: self
                .host
                .config
                .max_commit_bytes
                .max(self.host.config.max_history_object_bytes),
            max_items: 65_536,
            max_field_bytes: self
                .host
                .config
                .max_commit_bytes
                .max(self.host.config.max_history_object_bytes),
            max_path_bytes: MAX_PATH_BYTES,
        }
    }
    fn scoped_commit(
        &self,
        id: m::CommitId,
    ) -> Result<(m::Descriptor, m::ChangeId, bool, i64, Vec<m::CommitId>), Status> {
        with(&self.shared, |ledger| {
            let mut reader = Reader {
                host: &self.host,
                scope: &self.scope,
                repository: &self.repository,
                budget: &mut ledger.budget,
                deadline: self.deadline,
                read_bytes: ledger.read_bytes,
                read_limit: None,
            };
            let source = reader.canonical_source(id).map_err(failure)?;
            let descriptor = source.descriptor();
            let (change, conflict, millis, parents) = match source {
                CanonicalSource::Remote(node) => (
                    node.change_id(),
                    node.has_conflict(),
                    node.committer_millis(),
                    self.scope
                        .scope
                        .parent_nodes(&node, reader.budget)
                        .map_err(index_error)?
                        .into_iter()
                        .map(|n| n.commit())
                        .collect(),
                ),
                CanonicalSource::Frozen(node) => (
                    node.change_id(),
                    node.has_conflict(),
                    node.committer_millis(),
                    self.scope
                        .graph()
                        .parent_nodes(&node, reader.budget)
                        .map_err(index_error)?
                        .into_iter()
                        .map(|n| n.commit())
                        .collect(),
                ),
            };
            Ok((descriptor, change, conflict, millis, parents))
        })
    }
    fn authorized(&self, key: m::ObjectKey) -> Result<Grant, Status> {
        self.allowed
            .lock()
            .map_err(|_| corrupt())?
            .get(&grant_key(key))
            .copied()
            .ok_or_else(|| {
                Status::permission_denied("merged path object lacks a verified source edge")
            })
    }
    fn admit(&self, descriptor: m::Descriptor) -> Result<(), Status> {
        check(self.deadline)?;
        with(&self.shared, |ledger| {
            let size = usize::try_from(descriptor.size_bytes).map_err(|_| limit())?;
            let maximum = if descriptor.key.kind == m::Kind::File {
                self.host.config.max_blame_file_bytes
            } else {
                self.limits().max_object_bytes
            };
            let read = ledger.read_bytes.checked_add(size).ok_or_else(limit)?;
            if size > maximum || read > self.host.config.max_history_read_bytes {
                return Err(limit());
            }
            // Canonical bytes and both decoded native models are bounded before
            // physical reads. Tree-specific stock scratch is admitted from the
            // verified entry count below, before conversion or diff iteration.
            let bytes = size
                .checked_mul(16)
                .and_then(|n| n.checked_add(4096))
                .ok_or_else(limit)?;
            ledger
                .budget
                .charge(1, 1, bytes as u64)
                .map_err(index_error)?;
            ledger.read_bytes = read;
            Ok(())
        })
    }
    fn grant(
        &self,
        key: m::ObjectKey,
        object: &m::Object,
        tree_scratch: bool,
    ) -> Result<(), Status> {
        let prefix = if key.kind == m::Kind::Tree {
            self.authorized(key)?.prefix
        } else {
            0
        };
        if let m::Object::Tree(tree) = object
            && tree_scratch
        {
            let terms = with(&self.shared, |l| Ok(l.tree_terms.max(1)))?;
            let items = tree
                .entries
                .len()
                .checked_mul(terms)
                .and_then(|n| n.checked_mul(terms))
                .and_then(|n| n.checked_mul(4))
                .ok_or_else(limit)?;
            let bytes = tree.entries.iter().try_fold(0usize, |sum, entry| {
                let full_path = prefix.checked_add(entry.name.len()).ok_or_else(limit)?;
                let one = full_path
                    .checked_add(256)
                    .and_then(|n| n.checked_mul(terms))
                    .and_then(|n| n.checked_mul(8))
                    .ok_or_else(limit)?;
                sum.checked_add(one).ok_or_else(limit)
            })?;
            charge(&self.shared, self.deadline, items as u64, bytes as u64)?;
        }
        let count = match object {
            m::Object::Commit(c) => c.root_tree.terms.len(),
            m::Object::Tree(t) => t.entries.len().saturating_mul(2),
            _ => 0,
        };
        charge(
            &self.shared,
            self.deadline,
            count as u64,
            (count as u64).checked_mul(256).ok_or_else(limit)?,
        )?;
        let mut allowed = self.allowed.lock().map_err(|_| corrupt())?;
        match object {
            m::Object::Commit(c) => {
                for id in &c.root_tree.terms {
                    allowed
                        .entry(grant_key(m::ObjectKey {
                            kind: m::Kind::Tree,
                            id: m::ObjectId(id.0),
                        }))
                        .or_insert(Grant {
                            size: None,
                            prefix: 0,
                        });
                }
            }
            m::Object::Tree(t) => {
                for entry in &t.entries {
                    let (key, size) = match &entry.value {
                        m::TreeValue::Tree(id) => (
                            m::ObjectKey {
                                kind: m::Kind::Tree,
                                id: m::ObjectId(id.0),
                            },
                            None,
                        ),
                        m::TreeValue::Symlink { id, target_bytes } => (
                            m::ObjectKey {
                                kind: m::Kind::Symlink,
                                id: m::ObjectId(id.0),
                            },
                            Some(*target_bytes),
                        ),
                        m::TreeValue::File { id, size_bytes, .. } => (
                            m::ObjectKey {
                                kind: m::Kind::File,
                                id: m::ObjectId(id.0),
                            },
                            Some(*size_bytes),
                        ),
                    };
                    let prefix = prefix
                        .saturating_add(entry.name.len())
                        .saturating_add(1)
                        .min(MAX_PATH_BYTES);
                    if let Some(previous) = allowed.get_mut(&grant_key(key)) {
                        if previous.size != size {
                            return Err(corrupt());
                        }
                        previous.prefix = previous.prefix.max(prefix);
                    } else {
                        allowed.insert(grant_key(key), Grant { size, prefix });
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn selected_entry(
        &self,
        commit: &m::Commit,
        path: &str,
    ) -> Result<Option<m::TreeValue>, Status> {
        let &[mut id] = commit.root_tree.terms.as_slice() else {
            return Err(corrupt());
        };
        let depth = path.split('/').count();
        if depth > self.host.config.max_history_path_depth {
            return Err(limit());
        }
        for (position, component) in path.split('/').enumerate() {
            let key = m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(id.0),
            };
            let value = self.metadata_for(key, false).map_err(from_backend)?;
            let m::Object::Tree(tree) = value.into_object() else {
                return Err(corrupt());
            };
            let Some(entry) = tree
                .entries
                .into_iter()
                .find(|entry| entry.name == component)
            else {
                return Ok(None);
            };
            charge(
                &self.shared,
                self.deadline,
                1,
                entry.name.len() as u64 + 128,
            )?;
            let next = entry.value.clone();
            self.grant(
                key,
                &m::Object::Tree(m::Tree {
                    entries: vec![entry],
                }),
                false,
            )?;
            if position + 1 == depth {
                return Ok(Some(next));
            }
            match next {
                m::TreeValue::Tree(child) => id = child,
                value @ m::TreeValue::Symlink { .. } => return Ok(Some(value)),
                m::TreeValue::File { .. } => return Ok(None),
            }
        }
        Err(Status::invalid_argument("invalid merged path"))
    }
    fn simple_file_change(&self, id: m::CommitId, path: &str) -> Result<Option<bool>, Status> {
        let key = m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(id.0),
        };
        let m::Object::Commit(current) = self
            .metadata_for(key, false)
            .map_err(from_backend)?
            .into_object()
        else {
            return Err(corrupt());
        };
        if current.parents.len() != 1 || current.root_tree.terms.len() != 1 {
            return Ok(None);
        }
        let parent = if current.parents[0] == m::CommitId::ROOT {
            None
        } else {
            let key = m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(current.parents[0].0),
            };
            let m::Object::Commit(parent) = self
                .metadata_for(key, false)
                .map_err(from_backend)?
                .into_object()
            else {
                return Err(corrupt());
            };
            if parent.root_tree.terms.len() != 1 {
                return Ok(None);
            }
            Some(parent)
        };
        let after = self.selected_entry(&current, path)?;
        let before = match parent {
            Some(parent) => self.selected_entry(&parent, path)?,
            None => None,
        };
        // Native size claims are not jj TreeValue fields. Copy lineage and mode
        // are semantic; directory, symlink and unresolved cases stay in stock.
        charge(&self.shared, self.deadline, 2, 256)?;
        let normalize = |value: Option<m::TreeValue>| match value {
            None => Some(None),
            Some(m::TreeValue::File {
                id,
                executable,
                copy_id,
                ..
            }) => Some(Some((
                id,
                executable,
                copy_id.map_or_else(jj_lib::backend::CopyId::placeholder, |id| {
                    jj_lib::backend::CopyId::from_bytes(&id.0)
                }),
            ))),
            _ => None,
        };
        match (normalize(before), normalize(after)) {
            (Some(before), Some(after)) => Ok(Some(before != after)),
            _ => Ok(None),
        }
    }
    fn metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.metadata_for(key, true)
    }
    fn metadata_for(
        &self,
        key: m::ObjectKey,
        tree_scratch: bool,
    ) -> BackendResult<VerifiedMetadata> {
        check(self.deadline).map_err(backend)?;
        let source = if key.kind == m::Kind::Commit {
            Some(self.scoped_commit(m::CommitId(key.id.0)).map_err(backend)?)
        } else {
            self.authorized(key).map_err(backend)?;
            None
        };
        // Descriptor lookup itself is bounded and charged before Store I/O.
        charge(&self.shared, self.deadline, 1, 256).map_err(backend)?;
        let descriptor = self
            .host
            .store
            .descriptor(&self.repository, key)
            .map_err(|_| backend(corrupt()))?
            .ok_or_else(|| backend(corrupt()))?;
        if descriptor.key != key
            || descriptor.format_version != m::FORMAT_VERSION
            || source.as_ref().is_some_and(|s| s.0 != descriptor)
        {
            return Err(backend(corrupt()));
        }
        self.admit(descriptor).map_err(backend)?;
        let (actual, bytes) = self
            .host
            .store
            .read_metadata(&self.repository, key)
            .map_err(|_| backend(corrupt()))?;
        check(self.deadline).map_err(backend)?;
        if actual != descriptor {
            return Err(backend(corrupt()));
        }
        m::verify(&descriptor, &bytes, self.limits()).map_err(|_| backend(corrupt()))?;
        let object = m::decode(&bytes, self.limits()).map_err(|_| backend(corrupt()))?;
        if let Some((_, change, conflict, millis, parents)) = source {
            let m::Object::Commit(c) = &object else {
                return Err(backend(corrupt()));
            };
            if c.change_id != change
                || (c.root_tree.terms.len() > 1) != conflict
                || c.committer.timestamp.millis_since_epoch != millis
                || c.parents != parents
            {
                return Err(backend(corrupt()));
            }
        }
        if tree_scratch || key.kind != m::Kind::Tree {
            self.grant(key, &object, tree_scratch).map_err(backend)?;
        } else if let m::Object::Tree(tree) = &object {
            // A direct one-path walk never converts this TREE into jj's merged
            // entry maps. Meter its native linear lookup; grant only the chosen
            // edge below, not an unused catalog of siblings.
            charge(&self.shared, self.deadline, tree.entries.len() as u64, 0).map_err(backend)?;
        }
        VerifiedMetadata::verify(descriptor, bytes, self.limits())
    }
}
#[async_trait]
impl NativeObjectIo for Io {
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        if key.kind != m::Kind::Commit {
            self.authorized(key).map_err(backend)?;
        } else {
            self.scoped_commit(m::CommitId(key.id.0)).map_err(backend)?;
        }
        charge(&self.shared, self.deadline, 1, 256).map_err(backend)?;
        self.host
            .store
            .descriptor(&self.repository, key)
            .map_err(|_| backend(corrupt()))
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.metadata(key)
    }
    async fn write_metadata(&self, _: m::Object) -> BackendResult<m::Descriptor> {
        Err(backend(Status::permission_denied(
            "merged path scratch metadata is read only",
        )))
    }
    async fn read_file(&self, id: m::FileId) -> BackendResult<jj_native_backend::FileRead> {
        let key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(id.0),
        };
        let derived = self
            .files
            .lock()
            .map_err(|_| backend(corrupt()))?
            .get(&id)
            .cloned();
        let bytes = if let Some(bytes) = derived {
            bytes
        } else {
            let size = self
                .authorized(key)
                .map_err(backend)?
                .size
                .ok_or_else(|| backend(corrupt()))?;
            charge(&self.shared, self.deadline, 1, 256).map_err(backend)?;
            let descriptor = self
                .host
                .store
                .descriptor(&self.repository, key)
                .map_err(|_| backend(corrupt()))?
                .ok_or_else(|| backend(corrupt()))?;
            if descriptor.key != key
                || descriptor.size_bytes != size
                || descriptor.format_version != m::FORMAT_VERSION
            {
                return Err(backend(corrupt()));
            }
            self.admit(descriptor).map_err(backend)?;
            let mut source = self
                .host
                .store
                .read(&self.repository, key)
                .map_err(|_| backend(corrupt()))?;
            let mut bytes = vec![0; usize::try_from(size).map_err(|_| backend(limit()))?];
            source
                .read_exact(&mut bytes)
                .map_err(|_| backend(corrupt()))?;
            let mut trailing = [0];
            if source.read(&mut trailing).map_err(|_| backend(corrupt()))? != 0
                || m::describe_file(&bytes) != descriptor
            {
                return Err(backend(corrupt()));
            }
            check(self.deadline).map_err(backend)?;
            Arc::<[u8]>::from(bytes)
        };
        let descriptor = m::describe_file(&bytes);
        jj_native_backend::FileRead::from_preverified_pinned_source(
            descriptor,
            Box::pin(futures::io::Cursor::new(bytes)),
        )
    }
    async fn write_file(
        &self,
        input: &mut (dyn futures::io::AsyncRead + Send + Unpin),
        maximum: u64,
    ) -> BackendResult<m::FileId> {
        let maximum = maximum.min(self.host.config.max_blame_file_bytes as u64);
        charge(&self.shared, self.deadline, 1, 65536).map_err(backend)?;
        let mut bytes = Vec::new();
        let mut chunk = [0; 65536];
        loop {
            check(self.deadline).map_err(backend)?;
            let n = input
                .read(&mut chunk)
                .await
                .map_err(|e| BackendError::Other(Box::new(e)))?;
            if n == 0 {
                break;
            }
            if (bytes.len() as u64)
                .checked_add(n as u64)
                .is_none_or(|n| n > maximum)
            {
                return Err(backend(limit()));
            }
            charge(
                &self.shared,
                self.deadline,
                1,
                (n as u64).checked_mul(16).ok_or_else(|| backend(limit()))?,
            )
            .map_err(backend)?;
            bytes.try_reserve_exact(n).map_err(|_| backend(limit()))?;
            bytes.extend_from_slice(&chunk[..n]);
        }
        let id = m::FileId(m::describe_file(&bytes).key.id.0);
        charge(&self.shared, self.deadline, 1, 256).map_err(backend)?;
        let mut files = self.files.lock().map_err(|_| backend(corrupt()))?;
        if let Some(previous) = files.get(&id) {
            if previous.as_ref() != bytes.as_slice() {
                return Err(backend(corrupt()));
            }
        } else {
            files.insert(id, Arc::from(bytes));
        }
        Ok(id)
    }
    async fn copy_catalog_snapshot(
        &self,
        _: jj_native_backend::CopyCatalogLimits,
    ) -> BackendResult<jj_native_backend::CopyCatalogSnapshot> {
        Err(backend(Status::unimplemented(
            "merged path does not enumerate copy history",
        )))
    }
}
struct PathMatcher {
    inner: Arc<dyn Matcher>,
    shared: Shared,
    deadline: Instant,
    failed: AtomicBool,
    depth: usize,
}
impl std::fmt::Debug for PathMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergedPathMatcher").finish_non_exhaustive()
    }
}
impl PathMatcher {
    fn accept(&self, path: &RepoPath) -> bool {
        let text = path.as_internal_file_string();
        let ok = text.len() <= MAX_PATH_BYTES
            && text.split('/').count() <= self.depth
            && charge(&self.shared, self.deadline, 1, (text.len() + 64) as u64).is_ok();
        if !ok {
            self.failed.store(true, Ordering::Release);
        }
        ok
    }
}
impl Matcher for PathMatcher {
    fn matches(&self, path: &RepoPath) -> bool {
        self.accept(path) && self.inner.matches(path)
    }
    fn visit(&self, path: &RepoPath) -> jj_lib::matchers::Visit {
        if self.accept(path) {
            self.inner.visit(path)
        } else {
            jj_lib::matchers::Visit::Nothing
        }
    }
}
struct Graph<'a> {
    scope: &'a Arc<Scoped>,
    shared: &'a Shared,
    matcher: &'a PathMatcher,
    deadline: Instant,
}
impl tinytree_annotate::Budget for Graph<'_> {
    type Error = Status;
    fn check(&self) -> Result<(), Status> {
        check(self.deadline)?;
        if self.matcher.failed.load(Ordering::Acquire) {
            Err(limit())
        } else {
            Ok(())
        }
    }
    fn charge(&mut self, items: u64, bytes: u64) -> Result<(), Status> {
        self.check()?;
        charge(self.shared, self.deadline, items, bytes)
    }
    fn frontier(&mut self, n: usize) -> Result<(), Status> {
        self.check()?;
        with(self.shared, |l| {
            if n > l.budget.max_frontier {
                Err(limit())
            } else {
                Ok(())
            }
        })
    }
    fn diff_active(&self) -> bool {
        self.check().is_ok()
    }
}
impl tinytree_annotate::path::ParentGraph for Graph<'_> {
    fn prepare_diff(&mut self, terms: usize) -> Result<(), Status> {
        use tinytree_annotate::Budget as _;
        self.frontier(terms)?;
        with(self.shared, |l| {
            l.tree_terms = terms;
            Ok(())
        })
    }
    async fn common_ancestors(
        &mut self,
        left: &[CommitId],
        right: &[CommitId],
    ) -> Result<Vec<CommitId>, Status> {
        use jj_lib::object_id::ObjectId as _;
        self.charge(
            (left.len() + right.len()) as u64,
            ((left.len() + right.len()) as u64)
                .checked_mul(128)
                .ok_or_else(limit)?,
        )?;
        let convert = |id: &CommitId| {
            id.as_bytes()
                .try_into()
                .map(m::CommitId)
                .map_err(|_| corrupt())
        };
        let left = left.iter().map(convert).collect::<Result<Vec<_>, _>>()?;
        let right = right.iter().map(convert).collect::<Result<Vec<_>, _>>()?;
        let bases = with(self.shared, |l| {
            self.scope
                .graph()
                .common_ancestors(&left, &right, &mut l.budget)
                .map_err(index_error)
        })?;
        self.charge(
            bases.len() as u64,
            (bases.len() as u64).checked_mul(128).ok_or_else(limit)?,
        )?;
        Ok(bases.iter().map(|id| CommitId::from_bytes(&id.0)).collect())
    }
}
fn path_error(error: tinytree_annotate::path::Error<Status>) -> Status {
    match error {
        tinytree_annotate::path::Error::Source(e) => e,
        tinytree_annotate::path::Error::Backend(e) => from_backend(e),
        tinytree_annotate::path::Error::Resolve(_) => limit(),
        tinytree_annotate::path::Error::Limit => limit(),
    }
}
pub(crate) fn changed(
    host: Arc<QueryHost>,
    scope: Arc<Scoped>,
    repository: &str,
    id: m::CommitId,
    path: &str,
    exact_file: bool,
    fileset_key: Option<&[u8]>,
    budget: &mut WorkBudget,
    deadline: Instant,
    state: &Arc<Mutex<PathState>>,
) -> Result<bool, Status> {
    check(deadline)?;
    if id == m::CommitId::ROOT {
        let mut reader = Reader {
            host: &host,
            scope: &scope,
            repository,
            budget,
            deadline,
            read_bytes: 0,
            read_limit: None,
        };
        reader.canonical_source(id).map_err(failure)?;
        return Ok(false);
    }
    let mut memo_key = None;
    let (mut diff, read_bytes, compiled) = {
        let mut state = state.lock().map_err(|_| corrupt())?;
        if state.failed {
            return Err(limit());
        }
        let compiled = if let Some(key) = fileset_key {
            budget
                .charge(0, key.len() as u64 + 1, 64)
                .map_err(index_error)?;
            Some(
                state
                    .filesets
                    .as_ref()
                    .and_then(|c| c.rows.get(key))
                    .cloned()
                    .ok_or_else(|| Status::internal("fileset matcher was not precompiled"))?,
            )
        } else {
            None
        };
        if let Some(memo) = &state.memo {
            if !Arc::ptr_eq(&memo.scope, &scope) {
                return Err(Status::permission_denied("merged path memo scope mismatch"));
            }
            budget
                .charge(
                    0,
                    1,
                    (path.len() + fileset_key.map_or(0, <[u8]>::len) + 64) as u64,
                )
                .map_err(index_error)?;
            let key = (
                id,
                fileset_key.unwrap_or_default().to_vec(),
                path.to_owned(),
                exact_file,
            );
            if let Some(answer) = memo.rows.get(&key) {
                return Ok(*answer);
            }
            memo_key = Some(key);
        }
        (
            state
                .diff
                .take()
                .ok_or_else(|| Status::internal("merged path operation already active"))?,
            state.read_bytes,
            compiled,
        )
    };
    let initial = std::mem::replace(budget, WorkBudget::new(0, 0, 0, 0));
    let shared = Arc::new(Mutex::new(Some(Ledger {
        budget: initial,
        read_bytes,
        tree_terms: 1,
    })));
    let result = futures::executor::block_on(async {
        let mut allowed = BTreeMap::new();
        allowed.insert(
            grant_key(m::empty_tree().descriptor.key),
            Grant {
                size: None,
                prefix: 0,
            },
        );
        let io = Arc::new(Io {
            host: host.clone(),
            scope: scope.clone(),
            repository: repository.into(),
            shared: shared.clone(),
            deadline,
            allowed: Mutex::new(allowed),
            files: Mutex::new(BTreeMap::new()),
        });
        if fileset_key.is_none() {
            if let Some(changed) = io.simple_file_change(id, path)? {
                return Ok(changed);
            }
        }
        let mut config = jj_native_backend::Config::default();
        config.store.model_limits.max_object_bytes = host
            .config
            .max_commit_bytes
            .max(host.config.max_history_object_bytes);
        config.store.max_file_bytes = host.config.max_blame_file_bytes as u64;
        config.max_tree_entries = host.config.max_work_edges.try_into().map_err(|_| limit())?;
        let backend = jj_native_backend::NativeBackend::from_io(io, config)
            .await
            .map_err(from_backend)?;
        let store = Store::new(
            Box::new(backend),
            jj_lib::signing::Signer::new(None, vec![]),
            jj_lib::tree_merge::MergeOptions {
                hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
                same_change: jj_lib::merge::SameChange::Accept,
            },
        );
        let commit = store
            .get_commit_async(&CommitId::from_bytes(&id.0))
            .await
            .map_err(from_backend)?;
        let path = RepoPathBuf::from_internal_string(path)
            .map_err(|_| Status::invalid_argument("invalid merged path"))?;
        let inner: Arc<dyn Matcher> = if let Some(compiled) = compiled {
            compiled
        } else if exact_file {
            Arc::new(jj_lib::matchers::FilesMatcher::new([path.clone()]))
        } else {
            Arc::new(jj_lib::matchers::PrefixMatcher::new([path.clone()]))
        };
        let matcher = PathMatcher {
            inner,
            shared: shared.clone(),
            deadline,
            failed: AtomicBool::new(false),
            depth: host.config.max_history_path_depth,
        };
        let mut graph = Graph {
            scope: &scope,
            shared: &shared,
            matcher: &matcher,
            deadline,
        };
        if fileset_key.is_none() && exact_file {
            use tinytree_annotate::{Budget as _, path::ParentGraph as _};
            let parents = commit.parents().await.map_err(from_backend)?;
            let before = tinytree_annotate::path::merged_parent_tree(&store, &parents, &mut graph)
                .await
                .map_err(path_error)?;
            graph.prepare_diff(
                before
                    .tree_ids()
                    .iter()
                    .len()
                    .checked_add(commit.tree_ids().iter().len())
                    .ok_or_else(limit)?,
            )?;
            for tree in [before, commit.tree()] {
                graph.check()?;
                let value = tree.path_value(&path).await.map_err(from_backend)?;
                if value
                    .iter()
                    .any(|t| matches!(t, Some(jj_lib::backend::TreeValue::Tree(_))))
                {
                    return Err(Status::unimplemented(
                        "exact file filter encountered a directory",
                    ));
                }
            }
        }
        tinytree_annotate::path::changed_with_matcher(
            &store, &commit, &matcher, &mut graph, &mut diff,
        )
        .await
        .map_err(path_error)
    });
    // All scratch futures, Stores and cache payloads have quiesced above. Never
    // return a fresh budget when any physical reader still owns the spent one.
    let ledger = Arc::try_unwrap(shared)
        .map_err(|_| Status::internal("merged path readers did not quiesce"))
        .and_then(|m| m.into_inner().map_err(|_| corrupt()))
        .and_then(|v| v.ok_or_else(corrupt));
    let mut state = state.lock().map_err(|_| corrupt())?;
    match ledger {
        Ok(ledger) => {
            *budget = ledger.budget;
            state.read_bytes = ledger.read_bytes;
            state.diff = Some(diff);
        }
        Err(error) => {
            state.failed = true;
            return Err(error);
        }
    }
    if let (Ok(answer), Some(key), Some(memo)) = (&result, memo_key, state.memo.as_mut()) {
        // Cover BTree node growth/splits and owned key capacity before insertion.
        let cost = key
            .1
            .len()
            .checked_add(key.2.len())
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(limit)?;
        let next = memo.bytes.checked_add(cost).ok_or_else(limit)?;
        if next <= memo.maximum
            && memo.rows.len() < memo.entries
            && budget.charge(0, 1, cost as u64).is_ok()
        {
            // Optional cache growth never turns an already computed bounded
            // answer into refusal. charge() is atomic: refusal allocates nothing
            // and neither refunds nor resets the actual work already spent.
            memo.rows.insert(key, *answer);
            memo.bytes = next;
        }
    }
    result
}
