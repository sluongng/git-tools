//! One immutable, cumulatively charged annotation frontier per captured scope.
use super::*;
use crate::query_overlay::GraphKey;
use prost::Message;
use tokio::sync::OwnedSemaphorePermit;

pub(crate) struct Context {
    request: q::OpenAnnotationFrontierRequest,
    id: [u8; 32],
    deadline: Instant,
    budget: WorkBudget,
    read_bytes: usize,
    read_limit: usize,
    diff_limits: jj_lib::diff::LineDiffLimits,
    result: Option<Result<q::AnnotationFrontier, Status>>,
    closed: bool,
    // Payload and cumulative state drop before the independent idle reservation.
    memory: Option<OwnedSemaphorePermit>,
}
fn value(value: Option<&m::TreeValue>) -> q::AnnotationPathValue {
    let (state, copy_id) = match value {
        None => (absent(), Vec::new()),
        Some(m::TreeValue::File {
            id,
            executable,
            size_bytes,
            copy_id,
        }) => (
            q::HistoryPathState {
                kind: PathKind::HistoryPathFile as i32,
                native_id: id.0.to_vec(),
                size_bytes: *size_bytes,
                executable: *executable,
            },
            copy_id.map_or_else(Vec::new, |id| id.0.to_vec()),
        ),
        Some(m::TreeValue::Symlink { id, target_bytes }) => (
            q::HistoryPathState {
                kind: PathKind::HistoryPathSymlink as i32,
                native_id: id.0.to_vec(),
                size_bytes: *target_bytes,
                executable: false,
            },
            Vec::new(),
        ),
        Some(m::TreeValue::Tree(id)) => (
            q::HistoryPathState {
                kind: PathKind::HistoryPathTree as i32,
                native_id: id.0.to_vec(),
                size_bytes: 0,
                executable: false,
            },
            Vec::new(),
        ),
    };
    q::AnnotationPathValue {
        state: Some(state),
        copy_id,
    }
}
fn failure(error: Failure) -> Status {
    match error {
        Failure::Status(status) => status,
        Failure::Boundary(_) => {
            Status::resource_exhausted("annotation cumulative history budget exhausted")
        }
    }
}
fn context<'a>(
    state: &'a mut Option<Context>,
    id: &[u8],
    data: bool,
) -> Result<&'a mut Context, Status> {
    let context = state
        .as_mut()
        .ok_or_else(|| Status::failed_precondition("annotation context unavailable"))?;
    if id != context.id {
        return Err(Status::invalid_argument(
            "annotation context does not match captured scope",
        ));
    }
    if context.closed {
        return Err(Status::failed_precondition("annotation context is closed"));
    }
    if data && Instant::now() >= context.deadline {
        return Err(Status::deadline_exceeded(
            "annotation original deadline exceeded",
        ));
    }
    Ok(context)
}

fn effective_limits(host: &QueryHost, requested: Option<&q::AnnotationLimits>, maximum: usize) -> Result<Option<q::AnnotationLimits>, Status> {
    let Some(r) = requested else { return Ok(None); };
    let values = [r.work_nodes, r.work_edges, r.work_bytes, r.frontier, r.read_bytes,
        r.diff_input_bytes, r.diff_lines, r.diff_work, r.diff_allocation_bytes, r.diff_depth];
    if values.into_iter().any(|n| n == 0 || n > usize::MAX as u64) {
        return Err(Status::invalid_argument("positive bounded annotation limits required"));
    }
    let c = &host.config;
    Ok(Some(q::AnnotationLimits {
        work_nodes: r.work_nodes.min(c.max_work_nodes), work_edges: r.work_edges.min(c.max_work_edges),
        work_bytes: r.work_bytes.min(c.max_work_bytes), frontier: r.frontier.min(c.max_frontier as u64),
        read_bytes: r.read_bytes.min(c.max_history_read_bytes as u64),
        diff_input_bytes: r.diff_input_bytes.min(c.max_history_read_bytes as u64),
        diff_lines: r.diff_lines.min(c.max_blame_lines.saturating_mul(maximum).saturating_mul(2) as u64),
        diff_work: r.diff_work.min(c.max_blame_diff_steps as u64),
        diff_allocation_bytes: r.diff_allocation_bytes.min(c.max_blame_diff_memory_bytes as u64),
        diff_depth: r.diff_depth.min(c.max_blame_diff_depth as u64),
    }))
}

pub(crate) fn open(
    host: &QueryHost,
    scope: &Scoped,
    repository: &str,
    input: q::OpenAnnotationFrontierRequest,
    deadline: Instant,
) -> Result<q::AnnotationFrontier, Status> {
    if !matches!(input.version, 1 | 2) {
        return Err(Status::unimplemented(
            "annotation frontier version unsupported",
        ));
    }
    if (input.version == 2) != input.limits.is_some() {
        return Err(Status::invalid_argument("annotation version2 requires explicit limits; version1 uses operator caps"));
    }
    let (path, maximum) = validate(
        &input.path,
        q::HistoryPolicy::LinearFixedPathV1 as i32,
        input.max_commits,
        &host.config,
    )?;
    let limits = effective_limits(host, input.limits.as_ref(), maximum)?;
    if input.anchors.is_empty() || input.anchors.len() > host.config.max_roots {
        return Err(Status::invalid_argument(
            "annotation anchors must be bounded and nonempty",
        ));
    }
    let anchors = input
        .anchors
        .iter()
        .cloned()
        .map(|id| parse_revision(Some(id)))
        .collect::<Result<Vec<_>, _>>()?;
    if anchors.windows(2).any(|p| p[0] >= p[1]) {
        return Err(Status::invalid_argument(
            "annotation anchors must be sorted unique IDs",
        ));
    }
    let mut slot = scope
        .annotation
        .try_lock()
        .map_err(|_| Status::resource_exhausted("annotation context is busy"))?;
    if let Some(context) = slot.as_mut() {
        if context.closed {
            return Err(Status::failed_precondition(
                "annotation context cannot reopen after close",
            ));
        }
        if context.request != input {
            return Err(Status::failed_precondition(
                "captured scope already has another annotation context",
            ));
        }
        if Instant::now() >= context.deadline {
            return Err(Status::deadline_exceeded(
                "annotation original deadline exceeded",
            ));
        }
        return context.result.as_ref().cloned().unwrap_or_else(|| {
            Err(Status::failed_precondition(
                "annotation context did not complete opening",
            ))
        });
    }
    // No inflight reservation is retained by idle state. Scope roots own source
    // retention, and this independent cursor-pool reservation owns raw metadata.
    let reservation = host
        .config
        .max_response_bytes
        .saturating_mul(4)
        .saturating_add(input.encoded_len().saturating_mul(4))
        .saturating_add(8192);
    let memory = host.cursors.reserve_annotation(reservation)?;
    let now = crate::query_index::now()?;
    let seconds = deadline
        .saturating_duration_since(Instant::now())
        .as_secs()
        .saturating_add(1);
    let expires = scope
        .wire
        .expires_at_unix_seconds
        .min(now.saturating_add(seconds as i64));
    let mut id = [0; 32];
    id[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    id[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    *slot = Some(Context {
        request: input.clone(),
        id,
        deadline,
        budget: limits.as_ref().map_or_else(|| host.config.budget(), |v| WorkBudget::new(v.work_nodes, v.work_edges, v.work_bytes, v.frontier as usize)),
        read_bytes: 0,
        read_limit: limits.as_ref().map_or(host.config.max_history_read_bytes, |v| v.read_bytes as usize),
        diff_limits: jj_lib::diff::LineDiffLimits {
            max_input_bytes: limits.as_ref().map_or(host.config.max_history_read_bytes, |v| v.diff_input_bytes as usize),
            max_lines: limits.as_ref().map_or(host.config.max_blame_lines.saturating_mul(maximum).saturating_mul(2), |v| v.diff_lines as usize),
            max_work: limits.as_ref().map_or(host.config.max_blame_diff_steps, |v| v.diff_work as usize),
            max_allocation_bytes: limits.as_ref().map_or(host.config.max_blame_diff_memory_bytes, |v| v.diff_allocation_bytes as usize),
            max_recursion_depth: limits.as_ref().map_or(host.config.max_blame_diff_depth, |v| v.diff_depth as usize),
        },
        result: None,
        closed: false,
        memory: Some(memory),
    });
    let context = slot.as_mut().unwrap();
    let result = (|| {
        let _dag_memory = host.admit_dag_history()?;
        let mut result = q::AnnotationFrontier {
            limits: limits.clone(),
            context: id.to_vec(),
            scope_digest: scope.wire.content_digest.clone(),
            index_epoch: scope.wire.index_epoch.clone(),
            version: input.version,
            anchors: input.anchors.clone(),
            path: input.path.clone(),
            expires_at_unix_seconds: expires,
            coverage: Coverage::HistoryScanLimit as i32,
            scanned_commits: 0,
            nodes: Vec::new(),
        };
        let mut reader = Reader {
            host,
            scope,
            repository,
            budget: &mut context.budget,
            deadline,
            read_bytes: context.read_bytes,
            read_limit: Some(context.read_limit),
        };
        let step = (|| -> Step<()> {
            reader
                .budget
                .charge(1, 0, input.encoded_len() as u64)
                .map_err(index_error)?;
            let nodes = dag::frontier(
                &mut reader,
                &path,
                &anchors,
                maximum,
                &mut context.diff_limits,
                &mut result.scanned_commits,
            )?;
            let mut wire_bytes = result.encoded_len();
            for node in nodes {
                reader.check()?;
                let (source_layer, topological_key) = match node.key {
                    GraphKey::Remote(key) => (0, key),
                    GraphKey::Draft(key) => (1, key),
                };
                let node = q::AnnotationNode {
                    revision: Some(wire(node.id)),
                    parents: node.parents.into_iter().map(wire).collect(),
                    source_layer,
                    topological_key,
                    value: Some(value(node.value.as_ref())),
                    affects_path: Some(node.affects_path),
                };
                // Charge output before retaining another node. Physical history
                // admission owns scratch; the context reservation owns this copy.
                reader
                    .budget
                    .charge(1, 0, node.encoded_len() as u64)
                    .map_err(index_error)?;
                wire_bytes = wire_bytes
                    .saturating_add(node.encoded_len())
                    .saturating_add(16);
                if wire_bytes > host.config.max_response_bytes {
                    return Err(Failure::Boundary(Coverage::HistoryReadLimit));
                }
                result.nodes.push(node);
            }
            Ok(())
        })();
        context.read_bytes = reader.read_bytes;
        match step {
            Ok(()) => result.coverage = Coverage::HistoryComplete as i32,
            Err(error) => {
                result.nodes = Vec::new();
                result.coverage = finish_failure(error)? as i32;
            }
        }
        Ok(result)
    })();
    context.result = Some(result.clone());
    result
}

pub(crate) fn read(
    host: &QueryHost,
    scope: &Scoped,
    repository: &str,
    input: q::ReadAnnotationInputRequest,
    deadline: Instant,
) -> Result<q::AnnotationInput, Status> {
    let mut slot = scope
        .annotation
        .try_lock()
        .map_err(|_| Status::resource_exhausted("annotation context is busy"))?;
    let context = context(&mut slot, &input.context, true)?;
    let snapshot = context
        .result
        .as_ref()
        .and_then(|r| r.as_ref().ok())
        .filter(|r| r.coverage == Coverage::HistoryComplete as i32)
        .ok_or_else(|| Status::failed_precondition("annotation frontier is incomplete"))?;
    let revision = parse_revision(input.revision.clone())?;
    context
        .budget
        .charge(1, 0, input.encoded_len() as u64)
        .map_err(index_error)?;
    let node = snapshot
        .nodes
        .iter()
        .find(|node| {
            node.revision
                .as_ref()
                .is_some_and(|id| id.value == revision.0)
        })
        .ok_or_else(|| Status::invalid_argument("annotation input is outside frontier"))?;
    if input.expected.is_none() || input.expected != node.value {
        return Err(Status::invalid_argument(
            "annotation input expected state mismatch",
        ));
    }
    let expected = input.expected.as_ref().unwrap();
    if expected
        .state
        .as_ref()
        .is_none_or(|s| s.kind != PathKind::HistoryPathFile as i32)
    {
        return Err(Status::failed_precondition(
            "annotation comparison input requires a regular FILE",
        ));
    }
    let path = context.request.path.split('/').collect::<Vec<_>>();
    let mut reader = Reader {
        host,
        scope,
        repository,
        budget: &mut context.budget,
        deadline: deadline.min(context.deadline),
        read_bytes: context.read_bytes,
            read_limit: Some(context.read_limit),
    };
    let result = (|| -> Step<_> {
        let commit = reader.commit(revision)?;
        let actual = reader.path_entry(&commit, &path)?;
        let actual = value(actual.as_ref());
        if &actual != expected {
            return Err(Status::data_loss("annotation immutable path source changed").into());
        }
        let (descriptor, data) = reader.file(actual.state.as_ref().unwrap())?;
        Ok(q::AnnotationInput {
            context: context.id.to_vec(),
            revision: Some(wire(revision)),
            value: Some(actual),
            descriptor: Some(wire_descriptor(descriptor)),
            data,
        })
    })();
    context.read_bytes = reader.read_bytes;
    result.map_err(failure)
}

pub(crate) fn close(
    scope: &Scoped,
    input: q::CloseAnnotationFrontierRequest,
) -> Result<(), Status> {
    let mut slot = scope
        .annotation
        .try_lock()
        .map_err(|_| Status::resource_exhausted("annotation context is busy"))?;
    let Some(context) = slot.as_mut() else {
        return Err(Status::failed_precondition(
            "annotation context unavailable",
        ));
    };
    if input.context != context.id {
        return Err(Status::invalid_argument(
            "annotation context does not match captured scope",
        ));
    }
    if !context.closed {
        context.closed = true;
        context.result = None;
        context.request = Default::default();
        // Retain a charged tombstone, not the released snapshot's whole arena.
        if let Some(mut memory) = context.memory.take() {
            context.memory = memory.split(4096);
        }
    }
    Ok(())
}
