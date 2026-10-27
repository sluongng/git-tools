//! Checked canonical annotation frontier and demand-input operations.
use super::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicBool, Ordering},
};

struct Frontier {
    checked: Arc<OwnedHistory<q::AnnotationFrontier>>,
    scope: CheckedScopeHandle,
    deadline: Instant,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct CheckedAnnotationFrontier(Arc<Frontier>);
#[derive(Clone)]
pub struct CheckedAnnotationInput(Arc<OwnedHistory<q::AnnotationInput>>);
#[derive(Clone)]
pub struct CheckedAnnotationBlame(Arc<AnnotationBlame>);
struct AnnotationBlame {
    context: Vec<u8>,
    checked: Arc<OwnedHistory<q::BlameResult>>,
    scope: CheckedScopeHandle,
    deadline: Instant,
    expires_at_unix_seconds: i64,
    closed: AtomicBool,
}
impl CheckedAnnotationBlame {
    pub fn result(&self) -> &q::BlameResult {
        &self.0.checked.value
    }
    pub fn data_deadline(&self) -> Instant {
        self.0.deadline
    }
    pub fn expires_at_unix_seconds(&self) -> i64 {
        self.0.expires_at_unix_seconds
    }
}
impl std::fmt::Debug for CheckedAnnotationBlame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedAnnotationBlame")
            .field("ranges", &self.result().ranges.len())
            .finish_non_exhaustive()
    }
}
impl CheckedAnnotationFrontier {
    pub fn result(&self) -> &q::AnnotationFrontier {
        &self.0.checked.value
    }
    /// Original data deadline, including negotiation and every collected page.
    pub fn data_deadline(&self) -> Instant {
        self.0.deadline
    }
}
impl CheckedAnnotationInput {
    pub fn result(&self) -> &q::AnnotationInput {
        &self.0.value
    }
}
impl std::fmt::Debug for CheckedAnnotationFrontier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedAnnotationFrontier")
            .field("nodes", &self.result().nodes.len())
            .field("coverage", &self.result().coverage)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for CheckedAnnotationInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedAnnotationInput")
            .field("bytes", &self.result().data.len())
            .finish_non_exhaustive()
    }
}
fn path_value(value: Option<&q::AnnotationPathValue>) -> Result<usize, Status> {
    let value = value.ok_or_else(integrity)?;
    let retained = state(value.state.as_ref())?;
    if !(value.copy_id.is_empty() || value.copy_id.len() == 32)
        || (!value.copy_id.is_empty()
            && value.state.as_ref().unwrap().kind != q::HistoryPathKind::HistoryPathFile as i32)
    {
        return Err(integrity());
    }
    Ok(retained + value.copy_id.capacity() + std::mem::size_of::<q::AnnotationPathValue>())
}
fn check_frontier(
    scope: &CheckedScopeHandle,
    anchors: &[CommitId],
    path: &str,
    maximum: usize,
    reply: &q::AnnotationFrontier,
    limits: &crate::Limits,
) -> Result<usize, Status> {
    if reply.context.len() != 32
        || reply.context == [0; 32]
        || reply.scope_digest != scope.content_digest()
        || reply.index_epoch != scope.index_epoch()
        || !matches!(reply.version, 1 | 2 | 3)
        || reply.path != path
        || reply.expires_at_unix_seconds <= 0
        || reply.expires_at_unix_seconds > scope.expires_at_unix_seconds()
        || reply
            .anchors
            .iter()
            .map(|v| id(Some(v)))
            .collect::<Result<Vec<_>, _>>()?
            != anchors
        || reply.scanned_commits as usize > maximum
        || reply.coverage == 0
        || q::HistoryCoverage::try_from(reply.coverage).is_err()
    {
        return Err(integrity());
    }
    let complete = reply.coverage == q::HistoryCoverage::HistoryComplete as i32;
    if !complete && !reply.nodes.is_empty() {
        return Err(integrity());
    }
    if complete
        && (reply.nodes.len() != reply.scanned_commits as usize + 1
            || reply.nodes.len() > maximum.saturating_add(1))
    {
        return Err(integrity());
    }
    let mut retained = reply.context.capacity()
        + reply.scope_digest.capacity()
        + reply.index_epoch.capacity()
        + reply.path.capacity()
        + reply.anchors.capacity() * std::mem::size_of::<q::CommitId>()
        + reply.nodes.capacity() * std::mem::size_of::<q::AnnotationNode>()
        + 1024;
    for anchor in &reply.anchors {
        retained += anchor.value.capacity();
    }
    let mut positions = BTreeMap::new();
    let mut previous = None;
    let mut edges = 0usize;
    for node in &reply.nodes {
        let revision = id(node.revision.as_ref())?;
        let key = (node.source_layer, node.topological_key);
        if node.source_layer > 1
            || previous.is_some_and(|old| old >= key)
            || positions.insert(revision, key).is_some()
            || node.affects_path.is_none()
        {
            return Err(integrity());
        }
        retained += node.revision.as_ref().unwrap().value.capacity()
            + node.parents.capacity() * std::mem::size_of::<q::CommitId>()
            + path_value(node.value.as_ref())?;
        previous = Some(key);
        edges = edges
            .checked_add(node.parents.len())
            .ok_or_else(integrity)?;
        if edges > limits.max_query_edges {
            return Err(Status::resource_exhausted(
                "annotation frontier edge limit exceeded",
            ));
        }
        if revision == CommitId::ROOT {
            if !node.parents.is_empty()
                || node.affects_path != Some(false)
                || node.value.as_ref().unwrap().state.as_ref().unwrap().kind
                    != q::HistoryPathKind::HistoryPathAbsent as i32
            {
                return Err(integrity());
            }
        } else if node.parents.is_empty() {
            return Err(integrity());
        }
        for parent in &node.parents {
            let parent_id = id(Some(parent))?;
            if positions
                .get(&parent_id)
                .is_none_or(|parent_key| *parent_key >= key)
            {
                return Err(integrity());
            }
            retained += parent.value.capacity();
        }
    }
    if complete
        && (!positions.contains_key(&CommitId::ROOT)
            || anchors.iter().any(|id| !positions.contains_key(id)))
    {
        return Err(integrity());
    }
    Ok(retained)
}
fn annotation_limits(value: &q::AnnotationLimits) -> [u64; 10] {
    [
        value.work_nodes,
        value.work_edges,
        value.work_bytes,
        value.frontier,
        value.read_bytes,
        value.diff_input_bytes,
        value.diff_lines,
        value.diff_work,
        value.diff_allocation_bytes,
        value.diff_depth,
    ]
}
fn lowered_limits(effective: &q::AnnotationLimits, requested: &q::AnnotationLimits) -> bool {
    annotation_limits(effective)
        .into_iter()
        .zip(annotation_limits(requested))
        .all(|(value, cap)| value > 0 && value <= cap && value <= usize::MAX as u64)
}
struct FrontierCollection {
    requested: q::OpenAnnotationFrontierRequest,
    paging: (u32, u32),
    result: Option<q::AnnotationFrontier>,
    positions: BTreeMap<CommitId, (u32, u64)>,
    cursors: BTreeSet<Vec<u8>>,
    previous: Option<(u64, u64, i32, q::AnnotationUsage, u32, u32, u32, u32)>,
    edges: usize,
    retained: usize,
    maximum_bytes: usize,
    server_attribution: bool,
    deadline: Instant,
    blame: Option<q::BlameResult>,
    // Payloads and validation scratch drop before their source and admission.
    scope: CheckedScopeHandle,
    memory: HistoryMemory,
}
impl FrontierCollection {
    fn finish_memory(&mut self, retained: usize) -> Result<(), Status> {
        let retained = retained
            .checked_add(8192)
            .and_then(|bytes| bytes.checked_add(self.requested.anchors.len().checked_mul(128)?))
            .ok_or_else(integrity)?;
        if retained > self.maximum_bytes {
            return Err(Status::resource_exhausted(
                "annotation retained result exceeds reservation",
            ));
        }
        // Validation scratch is gone before unused aggregate admission returns.
        // Do not make a tiny completed attribution retain its worst-case pool.
        self.positions = BTreeMap::new();
        self.cursors = BTreeSet::new();
        let state = self.memory._state.split(retained).ok_or_else(integrity)?;
        self.memory._state = state;
        let bytes = self.memory._bytes.split(retained).ok_or_else(integrity)?;
        self.memory._bytes = bytes;
        Ok(())
    }
    fn accept(
        &mut self,
        mut page: q::AnnotationFrontierPage,
        limits: &crate::Limits,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, Status> {
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded(
                "annotation collection deadline exceeded",
            ));
        }
        let mut frontier = page.frontier.take().ok_or_else(integrity)?;
        let requested = &self.requested;
        let effective = frontier.limits.as_ref().ok_or_else(integrity)?;
        let usage = page.usage.as_ref().ok_or_else(integrity)?;
        let maximum = page.max_commits as usize;
        if page.sequence >= limits.max_graph_pages as u64 {
            return Err(Status::resource_exhausted(
                "annotation page count exceeds client reservation",
            ));
        }
        if (!self.server_attribution && page.blame.is_some())
            || (self.server_attribution
                && (!frontier.nodes.is_empty()
                    || page.emitted_nodes != 0
                    || page.blame.is_some() == page.has_more))
        {
            return Err(integrity());
        }
        if frontier.context.len() != 32
            || frontier.context == [0; 32]
            || frontier.scope_digest != self.scope.content_digest()
            || frontier.index_epoch != self.scope.index_epoch()
            || frontier.version != 3
            || frontier.path != requested.path
            || frontier.anchors != requested.anchors
            || frontier.expires_at_unix_seconds <= 0
            || frontier.expires_at_unix_seconds > self.scope.expires_at_unix_seconds()
            || !lowered_limits(effective, requested.limits.as_ref().ok_or_else(integrity)?)
            || maximum == 0
            || page.max_commits > requested.max_commits
            || maximum > limits.max_graph_objects
            || frontier.scanned_commits as usize > maximum
            || page.page_size == 0
            || page.page_size > self.paging.0
            || page.scan_quantum == 0
            || page.scan_quantum > self.paging.1
            || frontier.nodes.len() > page.page_size as usize
            || !(1..=4).contains(&page.phase)
            || page.has_more != !page.next_cursor.is_empty()
            || (page.has_more
                && (page.next_cursor.len() != 32
                    || page.next_cursor == [0; 32]
                    || frontier.coverage != 0
                    || page.phase == 4))
            || (!page.has_more
                && (frontier.coverage != q::HistoryCoverage::HistoryComplete as i32
                    || page.phase != 4))
            || (page.phase < 3 && !frontier.nodes.is_empty())
            || usage.work_nodes > effective.work_nodes
            || usage.work_edges > effective.work_edges
            || usage.work_bytes > effective.work_bytes
            || usage.read_bytes > effective.read_bytes
            || page.operations > (maximum as u64 + 1).saturating_mul(4).saturating_add(4)
        {
            return Err(integrity());
        }
        if let Some(header) = &self.result {
            if frontier.context != header.context
                || frontier.scope_digest != header.scope_digest
                || frontier.index_epoch != header.index_epoch
                || frontier.expires_at_unix_seconds != header.expires_at_unix_seconds
                || frontier.limits != header.limits
                || frontier.scanned_commits < header.scanned_commits
            {
                return Err(integrity());
            }
        }
        // A server may lower the context lifetime below the command deadline.
        // Freeze that first absolute expiry too; later pages cannot renew it.
        if self.result.is_none() {
            let wall_now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| Status::failed_precondition("native query clock unavailable"))?;
            let remaining = std::time::Duration::from_secs(frontier.expires_at_unix_seconds as u64)
                .checked_sub(wall_now)
                .filter(|value| !value.is_zero())
                .ok_or_else(|| Status::deadline_exceeded("annotation context expired"))?;
            self.deadline = self.deadline.min(
                Instant::now()
                    .checked_add(remaining)
                    .unwrap_or(self.deadline),
            );
        }
        let deadline = deadline.min(self.deadline);
        if let Some((
            sequence,
            operations,
            phase,
            previous,
            max_commits,
            page_size,
            quantum,
            emitted,
        )) = &self.previous
        {
            if page.sequence != sequence.checked_add(1).ok_or_else(integrity)?
                || page.operations < *operations
                || (page.operations == *operations
                    && page.emitted_nodes == *emitted
                    && page.phase == *phase)
                || page.phase < *phase
                || usage.work_nodes < previous.work_nodes
                || usage.work_edges < previous.work_edges
                || usage.work_bytes < previous.work_bytes
                || usage.read_bytes < previous.read_bytes
                || (page.max_commits, page.page_size, page.scan_quantum)
                    != (*max_commits, *page_size, *quantum)
                || page.emitted_nodes
                    != emitted
                        .checked_add(frontier.nodes.len() as u32)
                        .ok_or_else(integrity)?
            {
                return Err(integrity());
            }
        } else if page.sequence != 0
            || (page.operations == 0
                && !(self.server_attribution
                    && !page.has_more
                    && page.blame.as_ref().is_some_and(|blame| {
                        blame.target_bytes.is_empty()
                            && blame.ranges.is_empty()
                            && blame.scanned_commits == 0
                    })))
            || page.emitted_nodes as usize != frontier.nodes.len()
        {
            return Err(integrity());
        }
        if page.has_more {
            if self.cursors.contains(&page.next_cursor) {
                return Err(integrity());
            }
            // An initially sparse B-tree still owns eleven Vec key slots,
            // twelve edge slots, headers/alignment and the copied cursor body.
            let cursor_bytes = std::mem::size_of::<Vec<u8>>()
                .saturating_mul(11)
                .saturating_add(std::mem::size_of::<usize>().saturating_mul(12))
                .saturating_add(256 + std::mem::align_of::<Vec<u8>>() * 4)
                .saturating_add(page.next_cursor.len().saturating_mul(2));
            let retained = self
                .retained
                .checked_add(cursor_bytes)
                .ok_or_else(integrity)?;
            if retained > self.maximum_bytes {
                return Err(Status::resource_exhausted(
                    "annotation cursor allocation exceeds aggregate reservation",
                ));
            }
            self.retained = retained;
            self.cursors.insert(page.next_cursor.clone());
        }
        if self
            .positions
            .len()
            .checked_add(frontier.nodes.len())
            .is_none_or(|count| count > maximum + 1)
            || page.emitted_nodes as usize != self.positions.len() + frontier.nodes.len()
            || page.emitted_nodes > frontier.scanned_commits.saturating_add(1)
        {
            return Err(integrity());
        }
        let mut previous_key = self
            .result
            .as_ref()
            .and_then(|result| result.nodes.last())
            .map(|node| (node.source_layer, node.topological_key));
        for node in &frontier.nodes {
            if Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(
                    "annotation validation deadline exceeded",
                ));
            }
            let revision = id(node.revision.as_ref())?;
            let key = (node.source_layer, node.topological_key);
            if node.source_layer > 1
                || previous_key.is_some_and(|old| old >= key)
                || self.positions.contains_key(&revision)
                || node.affects_path.is_none()
            {
                return Err(integrity());
            }
            let value_bytes = path_value(node.value.as_ref())?;
            self.edges = self
                .edges
                .checked_add(node.parents.len())
                .ok_or_else(integrity)?;
            if self.edges > limits.max_query_edges {
                return Err(Status::resource_exhausted(
                    "annotation aggregate edge limit exceeded",
                ));
            }
            if revision == CommitId::ROOT {
                if !node.parents.is_empty()
                    || node.affects_path != Some(false)
                    || node.value.as_ref().unwrap().state.as_ref().unwrap().kind
                        != q::HistoryPathKind::HistoryPathAbsent as i32
                {
                    return Err(integrity());
                }
            } else if node.parents.is_empty() {
                return Err(integrity());
            }
            self.retained = self
                .retained
                .checked_add(
                    value_bytes
                        + node.revision.as_ref().unwrap().value.capacity()
                        + node.parents.capacity() * std::mem::size_of::<q::CommitId>()
                        // Full sparse B-tree node, including unused key/value
                        // slots and edges, before inserting validation state.
                        + (std::mem::size_of::<CommitId>()
                            + std::mem::size_of::<(u32, u64)>()) * 11
                        + std::mem::size_of::<usize>() * 12 + 256 + 32,
                )
                .ok_or_else(integrity)?;
            for parent in &node.parents {
                let parent_id = id(Some(parent))?;
                if self
                    .positions
                    .get(&parent_id)
                    .is_none_or(|parent_key| *parent_key >= key)
                {
                    return Err(integrity());
                }
                self.retained = self
                    .retained
                    .checked_add(parent.value.capacity())
                    .ok_or_else(integrity)?;
            }
            if self.retained > self.maximum_bytes {
                return Err(Status::resource_exhausted(
                    "annotation aggregate allocation exceeded reservation",
                ));
            }
            self.positions.insert(revision, key);
            previous_key = Some(key);
        }
        self.previous = Some((
            page.sequence,
            page.operations,
            page.phase,
            usage.clone(),
            page.max_commits,
            page.page_size,
            page.scan_quantum,
            page.emitted_nodes,
        ));
        let nodes = std::mem::take(&mut frontier.nodes);
        if self.result.is_none() {
            frontier.nodes = if self.server_attribution {
                Vec::new()
            } else {
                Vec::with_capacity(requested.max_commits as usize + 1)
            };
            self.result = Some(frontier);
        } else {
            let result = self.result.as_mut().unwrap();
            result.scanned_commits = frontier.scanned_commits;
            result.coverage = frontier.coverage;
        }
        self.result.as_mut().unwrap().nodes.extend(nodes);
        if !page.has_more {
            if self.server_attribution {
                let blame = page.blame.take().ok_or_else(integrity)?;
                if blame.coverage != q::HistoryCoverage::HistoryComplete as i32
                    || blame.scanned_commits != self.result.as_ref().unwrap().scanned_commits
                {
                    return Err(integrity());
                }
                let retained = check_blame_result(
                    &self.scope,
                    id(requested.anchors.first())?,
                    &requested.path,
                    q::HistoryPolicy::JjResolvedFixedPathV1,
                    page.max_commits,
                    &blame,
                    limits,
                )?;
                if retained
                    .checked_add(4096)
                    .is_none_or(|bytes| bytes > self.maximum_bytes)
                {
                    return Err(Status::resource_exhausted(
                        "annotation attribution allocation exceeded reservation",
                    ));
                }
                self.blame = Some(blame);
                self.finish_memory(retained)?;
                return Ok(None);
            }
            if !self.positions.contains_key(&CommitId::ROOT)
                || requested.anchors.iter().any(|anchor| {
                    id(Some(anchor)).map_or(true, |id| !self.positions.contains_key(&id))
                })
            {
                return Err(integrity());
            }
            let result = self.result.as_ref().unwrap();
            if result.nodes.len() != result.scanned_commits as usize + 1 {
                return Err(integrity());
            }
            // Identity, ordering and every parent were checked once on their
            // arriving page; closure was checked above. Measure only payload
            // capacity here, without allocating a second validation graph.
            let mut retained = result.context.capacity()
                + result.scope_digest.capacity()
                + result.index_epoch.capacity()
                + result.path.capacity()
                + result.anchors.capacity() * std::mem::size_of::<q::CommitId>()
                + result.nodes.capacity() * std::mem::size_of::<q::AnnotationNode>()
                + 1024;
            for anchor in &result.anchors {
                retained += anchor.value.capacity();
            }
            for node in &result.nodes {
                retained += node.revision.as_ref().unwrap().value.capacity()
                    + node.parents.capacity() * std::mem::size_of::<q::CommitId>()
                    + path_value(node.value.as_ref())?;
                for parent in &node.parents {
                    retained += parent.value.capacity();
                }
            }
            if retained > self.maximum_bytes {
                return Err(Status::resource_exhausted(
                    "annotation aggregate result exceeds reservation",
                ));
            }
            self.finish_memory(retained)?;
            Ok(None)
        } else {
            Ok(Some(page.next_cursor))
        }
    }
}

fn collection_deadline(collection: &FrontierCollection) -> Result<Instant, Status> {
    if Instant::now() >= collection.deadline {
        return Err(Status::deadline_exceeded(
            "annotation original deadline exceeded",
        ));
    }
    Ok(collection.deadline)
}
impl NativeClient {
    /// Open one bounded complete canonical union for this path and captured
    /// scope, or expose explicit incomplete coverage. Anchors are not forced.
    /// The same scope cannot reset this context by closing/reopening it.
    pub async fn open_annotation_frontier(
        &self,
        scope: &CheckedScopeHandle,
        anchors: &[CommitId],
        path: &str,
        max_commits: u32,
    ) -> Result<CheckedAnnotationFrontier, Status> {
        self.open_annotation_frontier_with_limits(scope, anchors, path, max_commits, None)
            .await
    }
    pub async fn open_annotation_frontier_with_limits(
        &self,
        scope: &CheckedScopeHandle,
        anchors: &[CommitId],
        path: &str,
        max_commits: u32,
        requested_limits: Option<q::AnnotationLimits>,
    ) -> Result<CheckedAnnotationFrontier, Status> {
        self.history_input(
            scope,
            path,
            q::HistoryPolicy::LinearFixedPathV1,
            max_commits,
        )?;
        if anchors.is_empty()
            || anchors.len() > self.inner.config.limits.max_graph_objects
            || anchors.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Status::invalid_argument(
                "annotation anchors must be bounded sorted unique IDs",
            ));
        }
        let deadline = self.deadline();
        let capability = self
            .clone()
            .with_operation_deadline(deadline)
            .annotation_frontier_capability()
            .await?
            .ok_or_else(|| {
                Status::unimplemented("server annotation frontier capability unavailable")
            })?;
        if !capability.authorized {
            return Err(Status::permission_denied(
                "annotation frontier is not authorized",
            ));
        }
        if !capability.supported {
            return Err(Status::unimplemented(
                "server annotation frontier capability unavailable",
            ));
        }
        let (budget, window) = self.history_budget(deadline).await?;
        let request = q::OpenAnnotationFrontierRequest {
            limits: requested_limits.clone(),
            scope: Some(scope.key()),
            version: if requested_limits.is_some() { 2 } else { 1 },
            anchors: anchors
                .iter()
                .map(|id| q::CommitId {
                    value: id.0.to_vec(),
                })
                .collect(),
            path: path.into(),
            max_commits,
        };
        self.bounded(&request)?;
        let reply = self
            .retry(deadline, false, |allowance| {
                let mut grpc = self.rpc_client(budget.clone());
                let request = request.clone();
                async move {
                    grpc.open_annotation_frontier(self.request(request, allowance))
                        .await
                        .map(|v| v.into_inner())
                }
            })
            .await?;
        if reply.version != if requested_limits.is_some() { 2 } else { 1 } {
            return Err(integrity());
        }
        if let Some(requested) = requested_limits {
            let effective = reply.limits.as_ref().ok_or_else(integrity)?;
            let effective = [
                effective.work_nodes,
                effective.work_edges,
                effective.work_bytes,
                effective.frontier,
                effective.read_bytes,
                effective.diff_input_bytes,
                effective.diff_lines,
                effective.diff_work,
                effective.diff_allocation_bytes,
                effective.diff_depth,
            ];
            let requested = [
                requested.work_nodes,
                requested.work_edges,
                requested.work_bytes,
                requested.frontier,
                requested.read_bytes,
                requested.diff_input_bytes,
                requested.diff_lines,
                requested.diff_work,
                requested.diff_allocation_bytes,
                requested.diff_depth,
            ];
            if effective
                .into_iter()
                .zip(requested)
                .any(|(a, b)| a == 0 || a > b || a > usize::MAX as u64)
            {
                return Err(integrity());
            }
        } else if reply.limits.is_some() {
            return Err(integrity());
        }
        let captured = scope.clone();
        let expected_anchors = anchors.to_vec();
        let path = path.to_owned();
        let limits = self.inner.config.limits.clone();
        let worker_owner = budget.clone();
        let (reply, retained, _owner) = tokio::time::timeout_at(
            deadline,
            tokio::task::spawn_blocking(move || {
                let maximum = if max_commits == 0 {
                    limits.max_graph_objects
                } else {
                    max_commits as usize
                };
                let retained = check_frontier(
                    &captured,
                    &expected_anchors,
                    &path,
                    maximum,
                    &reply,
                    &limits,
                );
                (reply, retained, worker_owner)
            }),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("annotation frontier validation deadline exceeded"))?
        .map_err(|_| Status::internal("annotation frontier validation worker failed"))?;
        let checked = self.retain_history(reply, retained?, &window)?;
        Ok(CheckedAnnotationFrontier(Arc::new(Frontier {
            checked,
            scope: scope.clone(),
            deadline,
            closed: AtomicBool::new(false),
        })))
    }
    /// Collect one resumable exact-path union under a fixed scope, reservation
    /// and absolute deadline. No boundary restart or per-page budget renewal.
    pub async fn collect_annotation_frontier(
        &self,
        scope: &CheckedScopeHandle,
        anchors: &[CommitId],
        path: &str,
        max_commits: u32,
        limits: q::AnnotationLimits,
    ) -> Result<CheckedAnnotationFrontier, Status> {
        self.collect_annotation_frontier_with_paging(
            scope,
            anchors,
            path,
            max_commits,
            limits,
            128,
            128,
        )
        .await
    }
    pub async fn collect_annotation_frontier_with_paging(
        &self,
        scope: &CheckedScopeHandle,
        anchors: &[CommitId],
        path: &str,
        max_commits: u32,
        limits: q::AnnotationLimits,
        page_size: u32,
        scan_quantum: u32,
    ) -> Result<CheckedAnnotationFrontier, Status> {
        let mut collection = self
            .collect_annotation_state(
                scope,
                anchors,
                path,
                max_commits,
                limits,
                page_size,
                scan_quantum,
                false,
            )
            .await?;
        let deadline = collection_deadline(&collection)?;
        Ok(CheckedAnnotationFrontier(Arc::new(Frontier {
            checked: Arc::new(OwnedHistory {
                value: collection.result.take().ok_or_else(integrity)?,
                _memory: collection.memory,
            }),
            scope: scope.clone(),
            deadline,
            closed: AtomicBool::new(false),
        })))
    }
    pub async fn collect_annotation_blame(
        &self,
        scope: &CheckedScopeHandle,
        revision: CommitId,
        path: &str,
        max_commits: u32,
        limits: q::AnnotationLimits,
    ) -> Result<CheckedAnnotationBlame, Status> {
        let mut collection = self
            .collect_annotation_state(
                scope,
                &[revision],
                path,
                max_commits,
                limits,
                128,
                128,
                true,
            )
            .await?;
        let deadline = collection_deadline(&collection)?;
        let context = collection
            .result
            .as_ref()
            .ok_or_else(integrity)?
            .context
            .clone();
        let expires_at_unix_seconds = collection
            .result
            .as_ref()
            .ok_or_else(integrity)?
            .expires_at_unix_seconds;
        Ok(CheckedAnnotationBlame(Arc::new(AnnotationBlame {
            checked: Arc::new(OwnedHistory {
                value: collection.blame.take().ok_or_else(integrity)?,
                _memory: collection.memory,
            }),
            scope: scope.clone(),
            context,
            deadline,
            expires_at_unix_seconds,
            closed: AtomicBool::new(false),
        })))
    }
    async fn collect_annotation_state(
        &self,
        scope: &CheckedScopeHandle,
        anchors: &[CommitId],
        path: &str,
        max_commits: u32,
        limits: q::AnnotationLimits,
        page_size: u32,
        scan_quantum: u32,
        server_attribution: bool,
    ) -> Result<FrontierCollection, Status> {
        self.history_input(
            scope,
            path,
            q::HistoryPolicy::JjResolvedFixedPathV1,
            max_commits,
        )?;
        if max_commits == 0
            || page_size == 0
            || page_size > 4096
            || scan_quantum == 0
            || scan_quantum > 4096
            || !annotation_limits(&limits)
                .into_iter()
                .all(|value| value > 0 && value <= usize::MAX as u64)
            || anchors.is_empty()
            || anchors.len() > self.inner.config.limits.max_graph_objects
            || anchors.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Status::invalid_argument(
                "positive bounded annotation continuation inputs required",
            ));
        }
        let mut deadline = self.deadline();
        let client = self.clone().with_operation_deadline(deadline);
        let capability = client
            .annotation_frontier_paging_capability()
            .await?
            .ok_or_else(|| {
                Status::unimplemented("annotation continuation capability unavailable")
            })?;
        if !capability.authorized {
            return Err(Status::permission_denied(
                "annotation continuation is not authorized",
            ));
        }
        if !capability.supported {
            return Err(Status::unimplemented(
                "annotation continuation is unsupported",
            ));
        }
        // Reserve aggregate node vectors, checked identity/cursor tables and
        // bounded parent metadata BEFORE allocating any aggregate collection.
        // The independent RPC window still accounts decoded physical pages.
        let frontier_reserved = (max_commits as usize + 1)
            .checked_mul(1024)
            .and_then(|value| {
                self.inner
                    .config
                    .limits
                    .max_query_edges
                    .checked_mul(256)
                    .and_then(|edges| value.checked_add(edges))
            })
            .and_then(|value| value.checked_add(path.len() * 4 + anchors.len() * 256 + 8192))
            .ok_or_else(integrity)?;
        let aggregate_cap = self
            .inner
            .config
            .limits
            .max_batch_bytes
            .checked_mul(2)
            .ok_or_else(integrity)?;
        // The graph may be sparse even when its theoretical edge maximum is
        // large. Admit only the bounded pool, then refuse actual retained
        // nodes/edges/cursors before they exceed it; never raise host policy.
        let reserved = if server_attribution {
            aggregate_cap
        } else {
            frontier_reserved.min(aggregate_cap)
        }
        .min(self.inner.query_state.available_permits());
        // Other checked results may legitimately retain part of the state
        // pool. Bound this collection by the remaining admitted capacity, not
        // its worst-case request alone. Both pool acquisitions stay nonblocking;
        // a concurrent winner can still make either one fail closed.
        let initial_retained = (if server_attribution {
            0
        } else {
            (max_commits as usize + 1) * std::mem::size_of::<q::AnnotationNode>()
        })
        .checked_add(4096 + path.len() * 4 + anchors.len() * 256)
        .ok_or_else(integrity)?;
        if initial_retained > reserved {
            return Err(Status::resource_exhausted(
                "annotation aggregate header exceeds reservation",
            ));
        }
        let amount = u32::try_from(reserved).map_err(|_| integrity())?;
        let state = self
            .inner
            .query_state
            .clone()
            .try_acquire_many_owned(amount)
            .map_err(|_| {
                Status::resource_exhausted("annotation aggregate exhausts query headroom")
            })?;
        let bytes = self
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(amount)
            .map_err(|_| {
                Status::resource_exhausted("annotation aggregate exhausts byte headroom")
            })?;
        let requested = q::OpenAnnotationFrontierRequest {
            scope: Some(scope.key()),
            version: 3,
            anchors: anchors
                .iter()
                .map(|id| q::CommitId {
                    value: id.0.to_vec(),
                })
                .collect(),
            path: path.into(),
            max_commits,
            limits: Some(limits),
        };
        let mut collection = FrontierCollection {
            scope: scope.clone(),
            requested: requested.clone(),
            paging: (page_size, scan_quantum),
            result: None,
            positions: BTreeMap::new(),
            cursors: BTreeSet::new(),
            previous: None,
            edges: 0,
            retained: initial_retained,
            maximum_bytes: reserved,
            server_attribution,
            deadline,
            blame: None,
            memory: HistoryMemory {
                _guards: self.guards.clone(),
                _state: state,
                _bytes: bytes,
            },
        };
        let open = q::OpenAnnotationFrontierPagedRequest {
            input: Some(requested),
            page_size,
            scan_quantum,
            server_attribution,
        };
        self.bounded(&open)?;
        let mut continuation: Option<Vec<u8>> = None;
        loop {
            client.checked_scope(scope, false)?;
            let (budget, _window) = client.history_budget(deadline).await?;
            let page = if let Some(cursor) = &continuation {
                let request = q::ContinueAnnotationFrontierRequest {
                    scope: Some(scope.key()),
                    context: collection
                        .result
                        .as_ref()
                        .ok_or_else(integrity)?
                        .context
                        .clone(),
                    cursor: cursor.clone(),
                };
                client.bounded(&request)?;
                client
                    .retry(deadline, false, |allowance| {
                        let mut grpc = client.rpc_client(budget.clone());
                        let request = client.request(request.clone(), allowance);
                        async move {
                            grpc.continue_annotation_frontier(request)
                                .await
                                .map(|reply| reply.into_inner())
                        }
                    })
                    .await?
            } else {
                client
                    .retry(deadline, false, |allowance| {
                        let mut grpc = client.rpc_client(budget.clone());
                        let _ = allowance; // retry() still bounds the logical attempt.
                        let request = client.request(
                            open.clone(),
                            deadline.saturating_duration_since(Instant::now()),
                        );
                        async move {
                            grpc.open_annotation_frontier_paged(request)
                                .await
                                .map(|reply| reply.into_inner())
                        }
                    })
                    .await?
            };
            let validation_limits = self.inner.config.limits.clone();
            let (next, returned, _physical_owner) = tokio::time::timeout_at(
                deadline,
                tokio::task::spawn_blocking(move || {
                    let next = collection.accept(page, &validation_limits, deadline);
                    (next, collection, budget)
                }),
            )
            .await
            .map_err(|_| {
                Status::deadline_exceeded("annotation collection validation deadline exceeded")
            })?
            .map_err(|_| Status::internal("annotation collection validation worker failed"))?;
            collection = returned;
            deadline = deadline.min(collection.deadline);
            continuation = next?;
            if continuation.is_none() {
                break;
            }
        }
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded(
                "annotation collection deadline exceeded",
            ));
        }
        Ok(collection)
    }
    /// Read one exact snapshot path input, revalidated by the server through the
    /// captured canonical/frozen source. FILE ids alone never grant this read.
    pub async fn read_annotation_input(
        &self,
        frontier: &CheckedAnnotationFrontier,
        revision: CommitId,
        expected: &q::AnnotationPathValue,
    ) -> Result<CheckedAnnotationInput, Status> {
        self.checked_scope(&frontier.0.scope, false)?;
        if frontier.0.closed.load(Ordering::Acquire) {
            return Err(Status::failed_precondition("annotation context is closed"));
        }
        let snapshot = frontier.result();
        if snapshot.coverage != q::HistoryCoverage::HistoryComplete as i32 {
            return Err(Status::failed_precondition(
                "annotation frontier is incomplete",
            ));
        }
        let selected = snapshot
            .nodes
            .iter()
            .find(|node| {
                node.revision
                    .as_ref()
                    .is_some_and(|id| id.value == revision.0)
            })
            .ok_or_else(|| Status::invalid_argument("annotation input is outside frontier"))?;
        path_value(Some(expected))?;
        if selected.value.as_ref() != Some(expected)
            || expected.state.as_ref().unwrap().kind != q::HistoryPathKind::HistoryPathFile as i32
        {
            return Err(Status::invalid_argument(
                "annotation input does not match exact regular-file state",
            ));
        }
        let deadline = self.deadline().min(frontier.0.deadline);
        let (budget, window) = self.history_budget(deadline).await?;
        let request = q::ReadAnnotationInputRequest {
            scope: Some(frontier.0.scope.key()),
            context: snapshot.context.clone(),
            revision: Some(q::CommitId {
                value: revision.0.to_vec(),
            }),
            expected: Some(expected.clone()),
        };
        self.bounded(&request)?;
        let reply = self
            .retry(deadline, false, |allowance| {
                let mut grpc = self.rpc_client(budget.clone());
                let request = request.clone();
                async move {
                    grpc.read_annotation_input(self.request(request, allowance))
                        .await
                        .map(|v| v.into_inner())
                }
            })
            .await?;
        let selected = expected.clone();
        let captured = frontier.clone();
        let limits = self.inner.config.limits.clone();
        let worker_owner = budget.clone();
        let (reply, retained, _owner) = tokio::time::timeout_at(
            deadline,
            tokio::task::spawn_blocking(move || {
                let checked = (|| {
                    if reply.context != captured.result().context
                        || id(reply.revision.as_ref())? != revision
                        || reply.value.as_ref() != Some(&selected)
                    {
                        return Err(integrity());
                    }
                    let descriptor =
                        parse_descriptor(reply.descriptor.as_ref().ok_or_else(integrity)?)?;
                    let state = selected.state.as_ref().unwrap();
                    if descriptor != tinytree_model::describe_file(&reply.data)
                        || descriptor.key.kind != Kind::File
                        || descriptor.key.id.0.as_slice() != state.native_id
                        || descriptor.size_bytes != state.size_bytes
                        || descriptor.size_bytes > limits.max_file_bytes
                    {
                        return Err(integrity());
                    }
                    Ok(reply.context.capacity()
                        + reply.revision.as_ref().unwrap().value.capacity()
                        + reply.data.capacity()
                        + path_value(reply.value.as_ref())?
                        + 2048)
                })();
                (reply, checked, worker_owner)
            }),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("annotation input validation deadline exceeded"))?
        .map_err(|_| Status::internal("annotation input validation worker failed"))?;
        Ok(CheckedAnnotationInput(
            self.retain_history(reply, retained?, &window)?,
        ))
    }
    pub async fn close_annotation_blame(
        &self,
        blame: &CheckedAnnotationBlame,
    ) -> Result<(), Status> {
        self.checked_scope(&blame.0.scope, false)?;
        let deadline = self.deadline();
        let budget = self.admit(deadline, self.unary_memory()).await?;
        let request = q::CloseAnnotationFrontierRequest {
            scope: Some(blame.0.scope.key()),
            context: blame.0.context.clone(),
        };
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.rpc_client(budget.clone());
            let request = self.request(request.clone(), allowance);
            async move { grpc.close_annotation_frontier(request).await.map(|_| ()) }
        })
        .await?;
        blame.0.closed.store(true, Ordering::Release);
        Ok(())
    }
    /// Logical cleanup uses its own caller-bounded allowance but cannot renew
    /// the original data deadline or reset the server's consumed context budget.
    pub async fn close_annotation_frontier(
        &self,
        frontier: &CheckedAnnotationFrontier,
    ) -> Result<(), Status> {
        self.checked_scope(&frontier.0.scope, false)?;
        let deadline = self.deadline();
        let budget = self.admit(deadline, self.unary_memory()).await?;
        let request = q::CloseAnnotationFrontierRequest {
            scope: Some(frontier.0.scope.key()),
            context: frontier.result().context.clone(),
        };
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.rpc_client(budget.clone());
            let request = request.clone();
            async move {
                grpc.close_annotation_frontier(self.request(request, allowance))
                    .await
                    .map(|_| ())
            }
        })
        .await?;
        frontier.0.closed.store(true, Ordering::Release);
        Ok(())
    }
}
