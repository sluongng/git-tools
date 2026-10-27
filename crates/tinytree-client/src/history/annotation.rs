//! Checked canonical annotation frontier and demand-input operations.
use super::*;
use std::{
    collections::BTreeMap,
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
impl CheckedAnnotationFrontier {
    pub fn result(&self) -> &q::AnnotationFrontier {
        &self.0.checked.value
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
        || !matches!(reply.version, 1 | 2)
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
        self.open_annotation_frontier_with_limits(scope, anchors, path, max_commits, None).await
    }
    pub async fn open_annotation_frontier_with_limits(
        &self, scope: &CheckedScopeHandle, anchors: &[CommitId], path: &str,
        max_commits: u32, requested_limits: Option<q::AnnotationLimits>,
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
        if reply.version != if requested_limits.is_some() { 2 } else { 1 } { return Err(integrity()); }
        if let Some(requested) = requested_limits {
            let effective = reply.limits.as_ref().ok_or_else(integrity)?;
            let effective = [effective.work_nodes, effective.work_edges, effective.work_bytes,
                effective.frontier, effective.read_bytes, effective.diff_input_bytes,
                effective.diff_lines, effective.diff_work, effective.diff_allocation_bytes, effective.diff_depth];
            let requested = [requested.work_nodes, requested.work_edges, requested.work_bytes,
                requested.frontier, requested.read_bytes, requested.diff_input_bytes,
                requested.diff_lines, requested.diff_work, requested.diff_allocation_bytes, requested.diff_depth];
            if effective.into_iter().zip(requested).any(|(a,b)| a == 0 || a > b || a > usize::MAX as u64) {
                return Err(integrity());
            }
        } else if reply.limits.is_some() { return Err(integrity()); }
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
