//! One private/canonical filtered graph; private IDs and bodies never go upstream.
use super::*;
use native_broker::{OpenAnnotationFrontier, ReadAnnotationInput};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use tinytree_annotate as engine;
use tinytree_model as m;

// Reserve the remote half before ANY private capture. Effective operator caps
// may be smaller; unused reservation is deliberately never refunded or renewed.
const SLICE_BYTES: usize = MAX_LOCAL_BYTES / 2;
const SLICE_WORK: usize = MAX_DIFF_WORK / 2;
const SLICE_FRONTIER: usize = 8192;
fn remote_limits() -> q::AnnotationLimits {
    q::AnnotationLimits {
        work_nodes: SLICE_WORK as u64,
        work_edges: SLICE_WORK as u64,
        work_bytes: SLICE_BYTES as u64,
        frontier: SLICE_FRONTIER as u64,
        read_bytes: SLICE_BYTES as u64,
        diff_input_bytes: SLICE_BYTES as u64,
        diff_lines: (MAX_LINES * 128) as u64,
        diff_work: SLICE_WORK as u64,
        diff_allocation_bytes: SLICE_BYTES as u64,
        diff_depth: 64,
    }
}
#[derive(Clone)]
struct Node {
    key: (u32, u64),
    parents: Vec<CommitId>,
    value: Option<m::TreeValue>,
    affects: Option<bool>,
}
struct Budget {
    started: Instant,
    cancelled: Arc<AtomicBool>,
    items: u64,
    bytes: u64,
    read_bytes: usize,
}
impl Budget {
    fn new(started: Instant, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            started,
            cancelled,
            items: 0,
            bytes: 0,
            read_bytes: 0,
        }
    }
    fn active(&self) -> bool {
        !self.cancelled.load(Ordering::Acquire) && self.started.elapsed() < MAX_COMMAND_TIME
    }
    fn charge(&mut self, items: u64, bytes: u64) -> Result<(), CommandError> {
        if !self.active() {
            return Err(user_error("native mixed annotation deadline exceeded"));
        }
        let items = self
            .items
            .checked_add(items)
            .ok_or_else(|| user_error("annotation work overflow"))?;
        let bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| user_error("annotation allocation overflow"))?;
        if items > SLICE_WORK as u64 || bytes > SLICE_BYTES as u64 {
            return Err(user_error(
                "native mixed annotation local reservation exhausted",
            ));
        }
        self.items = items;
        self.bytes = bytes;
        Ok(())
    }
}
fn wire_id(value: Option<&q::CommitId>) -> Result<CommitId, CommandError> {
    CommitId::from_slice(
        &value
            .ok_or_else(|| user_error("annotation revision missing"))?
            .value,
    )
    .map_err(failure)
}
fn model_value(value: &q::AnnotationPathValue) -> Result<Option<m::TreeValue>, CommandError> {
    let state = value
        .state
        .as_ref()
        .ok_or_else(|| user_error("annotation path state missing"))?;
    let id = || {
        state
            .native_id
            .as_slice()
            .try_into()
            .map_err(|_| user_error("annotation path ID invalid"))
    };
    Ok(match state.kind {
        1 => None,
        2 => Some(m::TreeValue::File {
            id: m::FileId(id()?),
            executable: state.executable,
            size_bytes: state.size_bytes,
            copy_id: if value.copy_id.is_empty() {
                None
            } else {
                Some(m::CopyId(
                    value
                        .copy_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| user_error("annotation copy ID invalid"))?,
                ))
            },
        }),
        3 => Some(m::TreeValue::Symlink {
            id: m::SymlinkId(id()?),
            target_bytes: state.size_bytes,
        }),
        4 => Some(m::TreeValue::Tree(m::TreeId(id()?))),
        _ => return Err(user_error("annotation path kind invalid")),
    })
}
fn private_ids(captured: &BTreeMap<CommitId, Node>) -> BTreeSet<CommitId> {
    captured.keys().copied().collect()
}

pub(super) struct Prepared {
    budget: Budget,
    canonical: BTreeSet<CommitId>,
}
/// Derive canonical candidates only from captured roots and locally verified
/// immutable parent links. Never send a private HasId probe to the server.
pub(super) fn prepare(
    index: &jj_native_index::NativeReadonlyIndex,
    roots: &[CommitId],
    revision: CommitId,
    started: Instant,
) -> Result<Option<Prepared>, CommandError> {
    let mut budget = Budget::new(started, Arc::new(AtomicBool::new(false)));
    budget.charge(index.local_query_lookup_items() as u64, 0)?;
    if index.local_query_parents(revision).is_none() {
        return Ok(None);
    }
    budget.charge(roots.len() as u64, (roots.len() * 128) as u64)?;
    let mut pending = roots.to_vec();
    let mut canonical = BTreeSet::new();
    while let Some(id) = pending.pop() {
        budget.charge(1, 128)?;
        if !canonical.insert(id) {
            continue;
        }
        budget.charge(index.local_query_lookup_items() as u64, 0)?;
        if let Some((_, parents)) = index.local_query_parents(id) {
            budget.charge(parents.len() as u64, (parents.len() * 64) as u64)?;
            pending.extend(parents.iter().copied());
        }
        if pending.len() + canonical.len() > SLICE_FRONTIER {
            return Err(user_error(
                "canonical candidate frontier reservation exhausted",
            ));
        }
    }
    if canonical.contains(&revision) {
        return Ok(None);
    }
    Ok(Some(Prepared { budget, canonical }))
}

pub(super) async fn enabled(
    query: &BrokerQueryClient,
    store: &Store,
) -> Result<bool, CommandError> {
    let options = store.merge_options();
    if options.hunk_level != jj_lib::files::FileMergeHunkLevel::Line
        || options.same_change != jj_lib::merge::SameChange::Accept
    {
        return Ok(false);
    }
    use native_broker::BlameCapabilities as C;
    match query.annotation_capabilities(Arc::new(())).await {
        Ok(reply) => match reply.value {
            C::Explicit {
                authorized: false, ..
            } => Err(failure(tonic::Status::permission_denied(
                "annotation frontier is not authorized",
            ))),
            C::Explicit {
                supported: true,
                authorized: true,
            } => Ok(true),
            C::Explicit {
                supported: false,
                authorized: true,
            }
            | C::LegacyUnnegotiated => Ok(false),
        },
        Err(error) if error.code() == tonic::Code::Unimplemented => Ok(false),
        Err(error) => Err(failure(error)),
    }
}

pub(super) struct ResultWithOwners {
    pub output: BlameOutput,
    pub private_ids: BTreeSet<CommitId>,
    // Keep verified source, snapshot and returned retained allocations through
    // origin lookup/template rendering. No RPC/inflight permit is held idle.
    _frontier: QueryReply<q::AnnotationFrontier>,
}

pub(super) async fn annotate(
    query: &BrokerQueryClient,
    scope: &QueryReply<QueryScope>,
    index: &jj_native_index::NativeReadonlyIndex,
    revision: CommitId,
    path: &RepoPath,
    store: &Arc<Store>,
    args: &PathArgs,
    started: Instant,
    prepared: Prepared,
) -> Result<ResultWithOwners, CommandError> {
    let Prepared {
        mut budget,
        canonical: known_canonical,
    } = prepared;
    let cancelled = budget.cancelled.clone();
    // These fixed remote caps are reserved independently of actual usage and
    // stay spent for the command. The local half can never borrow them.
    let remote_reservation = remote_limits();
    budget.charge(1, 1024)?;
    let backend = store
        .backend()
        .downcast_ref::<jj_native_backend::NativeBackend>()
        .ok_or_else(|| user_error("native annotation requires the native backend"))?;
    let parts = path
        .as_internal_file_string()
        .split('/')
        .collect::<Vec<_>>();
    if parts.len() > 128 {
        return Err(user_error("native annotation path depth exceeded"));
    }
    let mut nodes = BTreeMap::new();
    let mut pending = vec![revision];
    let mut seen = BTreeSet::new();
    let mut anchors = BTreeSet::new();
    while let Some(id) = pending.pop() {
        budget.charge(1, 128)?;
        if !seen.insert(id) {
            continue;
        }
        if known_canonical.contains(&id) {
            anchors.insert(id);
            continue;
        }
        budget.charge(index.local_query_lookup_items() as u64, 0)?;
        let Some((order, parents)) = index.local_query_parents(id) else {
            anchors.insert(id);
            continue;
        };
        if nodes.len() >= args.max_commits as usize {
            return Err(user_error("native private annotation scan limit reached"));
        }
        budget.charge(parents.len() as u64, (parents.len() * 128 + 1024) as u64)?;
        let parents = if parents.is_empty() {
            vec![CommitId::ROOT]
        } else {
            parents.to_vec()
        };
        // Read bounded exact COMMIT->TREE metadata, never a private FILE body.
        // The index and source objects are immutable for this operation.
        let remaining = SLICE_BYTES.saturating_sub(budget.bytes as usize);
        let metadata = backend
            .local_annotation_path(id, &parts, remaining, SLICE_FRONTIER)
            .await
            .map_err(failure)?;
        let mut actual = metadata.parents;
        if actual.is_empty() {
            actual.push(CommitId::ROOT);
        }
        if actual != parents {
            return Err(user_error(
                "private annotation COMMIT/index parents disagree",
            ));
        }
        budget.charge(metadata.objects as u64, metadata.charged_bytes as u64)?;
        pending.extend(parents.iter().copied());
        if pending.len() + seen.len() > SLICE_FRONTIER {
            return Err(user_error(
                "native annotation frontier reservation exhausted",
            ));
        }
        nodes.insert(
            id,
            Node {
                key: (2, order),
                parents,
                value: metadata.value,
                affects: None,
            },
        );
    }
    let private = private_ids(&nodes);
    let remaining = args.max_commits.saturating_sub(nodes.len() as u32);
    if remaining == 0 && anchors.iter().any(|id| *id != CommitId::ROOT) {
        return Err(user_error("native combined annotation scan limit reached"));
    }
    let frontier = query
        .open_annotation_frontier(
            OpenAnnotationFrontier {
                scope: scope.value.handle,
                anchors: anchors.into_iter().collect(),
                path: path.as_internal_file_string().to_owned(),
                max_commits: remaining.max(1),
                limits: Some(remote_reservation),
            },
            Arc::new(()),
        )
        .await
        .map_err(failure)?;
    if frontier.value.coverage != q::HistoryCoverage::HistoryComplete as i32 {
        return Err(user_error(format!(
            "native mixed annotation requires a complete canonical frontier ({})",
            coverage(frontier.value.coverage)?
        )));
    }
    if frontier.value.scanned_commits as usize + nodes.len() > args.max_commits as usize {
        return Err(user_error("native combined annotation scan limit reached"));
    }
    let mut canonical = BTreeMap::new();
    for wire in &frontier.value.nodes {
        budget.charge(1, 1024 + wire.parents.len() as u64 * 128)?;
        let id = wire_id(wire.revision.as_ref())?;
        if nodes.contains_key(&id) {
            return Err(user_error("annotation source layers overlap"));
        }
        let value = wire
            .value
            .as_ref()
            .ok_or_else(|| user_error("annotation canonical path missing"))?;
        let value_model = model_value(value)?;
        if let Some(m::TreeValue::File { id: file, .. }) = &value_model {
            canonical
                .entry(*file)
                .or_insert_with(|| (id, value.clone()));
        }
        nodes.insert(
            id,
            Node {
                key: (wire.source_layer, wire.topological_key),
                parents: wire
                    .parents
                    .iter()
                    .map(|id| wire_id(Some(id)))
                    .collect::<Result<_, _>>()?,
                value: value_model,
                affects: wire.affects_path,
            },
        );
    }
    let context = scope.value.handle;
    let runtime = tokio::runtime::Handle::current();
    let query = query.clone();
    let scope_owner = scope.owner.clone();
    let store = store.clone();
    let path = path.to_owned();
    let target_value = nodes
        .get(&revision)
        .and_then(|n| n.value.clone())
        .ok_or_else(|| user_error("native annotation target absent"))?;
    let m::TreeValue::File { id: target_id, .. } = target_value else {
        return Err(user_error(
            "native annotation requires a resolved regular file",
        ));
    };
    struct Cancel(Arc<AtomicBool>);
    impl Drop for Cancel {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let cancel = Cancel(cancelled);
    let remote_scanned = frontier.value.scanned_commits;
    let local_commits = private.len();
    let worker = tokio::task::spawn_blocking(move || {
        let mut reader = Reader {
            nodes,
            canonical,
            query,
            context,
            store,
            path,
            budget,
            runtime,
            _source: scope_owner,
        };
        let target = engine::ContentSource::read_file(&mut reader, &target_value)?;
        reader.budget.charge(
            count_lines(&target) as u64,
            (target.len() * 3 + count_lines(&target) * 128) as u64,
        )?;
        let mut origins = vec![None; count_lines(&target)];
        let mut scanned = 0;
        let mut diff = LineDiffLimits {
            max_input_bytes: SLICE_BYTES,
            max_lines: MAX_LINES * 128,
            max_work: SLICE_WORK,
            max_allocation_bytes: SLICE_BYTES,
            max_recursion_depth: 64,
        };
        engine::annotate(
            &mut reader,
            revision,
            &target,
            target_id,
            engine::Limits {
                max_commits: args_max(local_commits, remote_scanned),
                max_lines: MAX_LINES,
                max_file_bytes: MAX_FILE_BYTES,
            },
            &mut diff,
            &mut scanned,
            &mut origins,
        )
        .map_err(engine_error)?;
        if origins.iter().any(Option::is_none) {
            return Err(user_error(
                "complete mixed annotation has unresolved origins",
            ));
        }
        let output = BlameOutput {
            version: 1,
            revision: hex(&revision.0),
            path: reader.path.as_internal_file_string().to_owned(),
            policy: "jj_resolved_fixed_path_v1",
            coverage: "complete",
            local_commits,
            remote_scanned_commits: remote_scanned,
            target_id: hex(&target_id.0),
            lines: target
                .split_inclusive(|b| *b == b'\n')
                .enumerate()
                .map(|(line, bytes)| {
                    let origin = origins[line].unwrap();
                    AnnotatedLine {
                        line: line + 1,
                        origin: Some(Origin {
                            commit: hex(&origin.commit.0),
                            original_line: origin.line + 1,
                        }),
                        bytes: bytes.to_vec(),
                    }
                })
                .collect(),
        };
        Ok::<_, CommandError>(output)
    });
    let output = worker
        .await
        .map_err(|_| user_error("mixed annotation worker failed"))??;
    drop(cancel);
    Ok(ResultWithOwners {
        output,
        private_ids: private,
        _frontier: frontier,
    })
}
fn args_max(local: usize, remote: u32) -> usize {
    local + remote as usize
}
fn engine_error(error: engine::Error<CommandError>) -> CommandError {
    match error {
        engine::Error::Source(e) => e,
        engine::Error::Boundary(engine::Boundary::ScanLimit) => {
            user_error("mixed annotation scan limit reached")
        }
        engine::Error::Boundary(engine::Boundary::ReadLimit) => {
            user_error("mixed annotation read limit reached")
        }
        engine::Error::Boundary(engine::Boundary::DiffLimit) => {
            user_error("mixed annotation diff limit reached")
        }
        engine::Error::DiffDeadline => user_error("mixed annotation diff deadline exceeded"),
        engine::Error::InvalidGraph(_) | engine::Error::Internal(_) => {
            user_error("invalid mixed annotation graph")
        }
    }
}
struct Reader {
    nodes: BTreeMap<CommitId, Node>,
    canonical: BTreeMap<m::FileId, (CommitId, q::AnnotationPathValue)>,
    query: BrokerQueryClient,
    context: [u8; 32],
    store: Arc<Store>,
    path: jj_lib::repo_path::RepoPathBuf,
    budget: Budget,
    runtime: tokio::runtime::Handle,
    _source: native_broker::QueryOwner,
}
impl Reader {
    fn ancestors(&mut self, heads: &[CommitId]) -> Result<BTreeSet<CommitId>, CommandError> {
        self.budget
            .charge(heads.len() as u64, (heads.len() * 128) as u64)?;
        let mut todo = heads.to_vec();
        let mut result = BTreeSet::new();
        while let Some(id) = todo.pop() {
            self.budget.charge(1, 128)?;
            if !result.insert(id) {
                continue;
            }
            let node = self
                .nodes
                .get(&id)
                .ok_or_else(|| user_error("annotation ancestor missing"))?;
            self.budget
                .charge(node.parents.len() as u64, (node.parents.len() * 64) as u64)?;
            todo.extend(node.parents.iter().copied());
            if todo.len() + result.len() > SLICE_FRONTIER {
                return Err(user_error("mixed annotation ancestor frontier exhausted"));
            }
        }
        Ok(result)
    }
}
impl engine::Budget for Reader {
    type Error = CommandError;
    fn check(&self) -> Result<(), CommandError> {
        if self.budget.active() {
            Ok(())
        } else {
            Err(user_error("mixed annotation deadline exceeded"))
        }
    }
    fn charge(&mut self, items: u64, bytes: u64) -> Result<(), CommandError> {
        self.budget.charge(items, bytes)
    }
    fn frontier(&mut self, n: usize) -> Result<(), CommandError> {
        if n <= SLICE_FRONTIER {
            Ok(())
        } else {
            Err(user_error("mixed annotation graph frontier exhausted"))
        }
    }
    fn diff_active(&self) -> bool {
        self.budget.active()
    }
}
impl engine::GraphSource for Reader {
    type Key = (u32, u64);
    fn node(&mut self, id: CommitId) -> Result<engine::SourceNode<Self::Key>, CommandError> {
        let n = self
            .nodes
            .get(&id)
            .ok_or_else(|| user_error("mixed annotation node missing"))?;
        self.budget
            .charge(n.parents.len() as u64, (n.parents.len() * 64 + 256) as u64)?;
        Ok(engine::SourceNode {
            key: n.key,
            parents: n.parents.clone(),
            value: n.value.clone(),
        })
    }
    fn affects_path(&mut self, id: CommitId) -> Result<Option<bool>, CommandError> {
        self.budget.charge(1, 0)?;
        Ok(self
            .nodes
            .get(&id)
            .ok_or_else(|| user_error("mixed annotation flag missing"))?
            .affects)
    }
    fn is_ancestor(
        &mut self,
        ancestor: CommitId,
        descendant: CommitId,
    ) -> Result<bool, CommandError> {
        Ok(self.ancestors(&[descendant])?.contains(&ancestor))
    }
    fn common_ancestors(
        &mut self,
        left: &[CommitId],
        right: &[CommitId],
    ) -> Result<Vec<CommitId>, CommandError> {
        let a = self.ancestors(left)?;
        let b = self.ancestors(right)?;
        self.budget.charge(
            (a.len() + b.len()) as u64,
            ((a.len() + b.len()) * 64) as u64,
        )?;
        let common = a.intersection(&b).copied().collect::<Vec<_>>();
        let mut result = Vec::new();
        for id in &common {
            let mut dominated = false;
            for other in &common {
                if id != other && engine::GraphSource::is_ancestor(self, *id, *other)? {
                    dominated = true;
                    break;
                }
            }
            if !dominated {
                self.budget.charge(1, 64)?;
                result.push(*id);
            }
        }
        self.budget.charge(
            (result
                .len()
                .saturating_mul(result.len().max(1).ilog2() as usize + 1)) as u64,
            (result.len() * 32) as u64,
        )?;
        result.sort();
        Ok(result)
    }
}
impl engine::ContentSource for Reader {
    fn read_file(&mut self, value: &m::TreeValue) -> Result<Vec<u8>, CommandError> {
        let m::TreeValue::File { id, size_bytes, .. } = value else {
            return Err(user_error("mixed annotation input is not FILE"));
        };
        let size = usize::try_from(*size_bytes).map_err(failure)?;
        if size > MAX_FILE_BYTES
            || self
                .budget
                .read_bytes
                .checked_add(size)
                .is_none_or(|n| n > SLICE_BYTES)
        {
            return Err(user_error("mixed annotation content reservation exhausted"));
        }
        self.budget.charge(1, (size * 2 + 512) as u64)?;
        self.budget.read_bytes += size;
        let canonical = self.canonical.get(id).cloned();
        let bytes = if let Some((revision, expected)) = canonical {
            let reply = self
                .runtime
                .block_on(self.query.read_annotation_input(
                    ReadAnnotationInput {
                        scope: self.context,
                        context: self.context,
                        revision,
                        expected,
                    },
                    Arc::new(()),
                ))
                .map_err(failure)?;
            reply.value.data.clone()
        } else {
            self.budget.charge(self.nodes.len() as u64, 0)?;
            if !self
                .nodes
                .values()
                .any(|node| node.affects.is_none() && node.value.as_ref() == Some(value))
            {
                return Err(user_error(
                    "mixed annotation FILE lacks exact private path source",
                ));
            }
            self.runtime.block_on(async {
                tokio::time::timeout_at(
                    tokio::time::Instant::from_std(self.budget.started + MAX_COMMAND_TIME),
                    async {
                        let backend = self
                            .store
                            .backend()
                            .downcast_ref::<jj_native_backend::NativeBackend>()
                            .ok_or_else(|| user_error("native annotation backend unavailable"))?;
                        let reader = backend
                            .local_annotation_file(
                                value,
                                MAX_FILE_BYTES as u64,
                                self._source.clone(),
                            )
                            .await
                            .map_err(failure)?;
                        let mut bytes = Vec::with_capacity(size);
                        reader
                            .take((size + 1) as u64)
                            .read_to_end(&mut bytes)
                            .await
                            .map_err(failure)?;
                        Ok::<_, CommandError>(bytes)
                    },
                )
                .await
                .map_err(|_| user_error("mixed annotation private input deadline exceeded"))?
            })?
        };
        if bytes.len() != size
            || m::describe_file(&bytes).key.id.0 != id.0
            || count_lines(&bytes) > MAX_LINES
        {
            return Err(user_error(
                "mixed annotation exact FILE verification failed",
            ));
        }
        if !self.budget.active() {
            return Err(user_error("mixed annotation content deadline exceeded"));
        }
        Ok(bytes)
    }
}
