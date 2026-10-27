//! Stateful bounded advances over one immutable annotation context.
use super::*;
use std::{collections::BTreeMap, num::NonZeroUsize};
use tinytree_annotate::{Frontier, FrontierNode, FrontierPhase};

pub(super) struct State {
    engine: Option<Frontier<GraphKey>>,
    server_attribution: bool,
    target: Option<Target>,
    pending: Option<FrontierNode<GraphKey>>,
    grants: BTreeMap<m::CommitId, q::AnnotationPathValue>,
    maximum: u32,
    page_size: u32,
    quantum: u32,
    expires: i64,
    sequence: u64,
    emitted: u32,
    next: Vec<u8>,
    first: Option<Result<q::AnnotationFrontierPage, Status>>,
    last: Option<(Vec<u8>, Result<q::AnnotationFrontierPage, Status>)>,
    failed: Option<Status>,
    complete: bool,
}
impl State {
    pub(super) fn input_value(&self, id: m::CommitId) -> Result<&q::AnnotationPathValue, Status> {
        if self.server_attribution || self.failed.is_some() || !self.complete {
            return Err(Status::failed_precondition(
                "annotation frontier is incomplete",
            ));
        }
        self.grants
            .get(&id)
            .ok_or_else(|| Status::invalid_argument("annotation input is outside frontier"))
    }
    fn poison(&mut self, error: Status) {
        self.engine = None;
        self.target = None;
        self.pending = None;
        self.grants.clear();
        self.complete = false;
        self.failed = Some(error);
    }
}
fn cursor() -> Vec<u8> {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes
}
fn parameter(value: u32) -> Result<u32, Status> {
    let value = if value == 0 { 128 } else { value };
    if value > 1024 {
        return Err(Status::invalid_argument(
            "annotation page parameters exceed limit",
        ));
    }
    Ok(value)
}
fn reserve(host: &QueryHost, context: &mut Context, bytes: usize) -> Result<(), Status> {
    let held = context.memory.as_ref().map_or(0, |p| p.num_permits());
    if bytes > held {
        let extra = host.cursors.reserve_annotation(bytes - held)?;
        context
            .memory
            .as_mut()
            .ok_or_else(|| Status::internal("annotation memory missing"))?
            .merge(extra);
    }
    Ok(())
}
fn retained(context: &Context, page: &q::AnnotationFrontierPage) -> usize {
    let state = context.paged.as_ref().unwrap();
    let engine = state
        .engine
        .as_ref()
        .map_or(0, |e| e.progress().retained_bytes_upper_bound);
    let cache = state
        .first
        .as_ref()
        .and_then(|v| v.as_ref().ok())
        .map_or(0, Message::encoded_len)
        .saturating_add(
            state
                .last
                .as_ref()
                .and_then(|(_, v)| v.as_ref().ok())
                .map_or(0, Message::encoded_len),
        );
    usize::try_from(engine)
        .unwrap_or(usize::MAX)
        .saturating_add(
            state
                .target
                .as_ref()
                .map_or(0, |v| v.bytes.capacity().saturating_add(256)),
        )
        .saturating_add(state.grants.len().saturating_mul(512))
        .saturating_add(context.request.encoded_len().saturating_mul(8))
        .saturating_add(cache.saturating_add(page.encoded_len()).saturating_mul(4))
        .saturating_add(8192)
}

pub(crate) fn open_paged(
    host: &QueryHost,
    scope: &Scoped,
    repository: &str,
    outer: q::OpenAnnotationFrontierPagedRequest,
    deadline: Instant,
) -> Result<q::AnnotationFrontierPage, Status> {
    let page_size = parameter(outer.page_size)?;
    let quantum = parameter(outer.scan_quantum)?;
    let server_attribution = outer.server_attribution;
    let input = outer
        .input
        .ok_or_else(|| Status::invalid_argument("annotation input required"))?;
    if input.version != 3 || input.limits.is_none() {
        return Err(Status::invalid_argument(
            "paged annotation requires version3 explicit limits",
        ));
    }
    let mut policy = host.config.clone();
    policy.max_history_commits = policy.max_annotation_commits;
    let (_, maximum) = validate(
        &input.path,
        q::HistoryPolicy::LinearFixedPathV1 as i32,
        input.max_commits,
        &policy,
    )?;
    if server_attribution && input.anchors.len() != 1 {
        return Err(Status::invalid_argument(
            "server attribution requires one target anchor",
        ));
    }
    if input.anchors.is_empty() || input.anchors.len() > host.config.max_roots {
        return Err(Status::invalid_argument(
            "annotation anchors must be bounded and nonempty",
        ));
    }
    let anchors = input
        .anchors
        .iter()
        .cloned()
        .map(|v| parse_revision(Some(v)))
        .collect::<Result<Vec<_>, _>>()?;
    if anchors.windows(2).any(|p| p[0] >= p[1]) {
        return Err(Status::invalid_argument(
            "annotation anchors must be sorted unique IDs",
        ));
    }
    let limits = effective_limits(host, input.limits.as_ref(), maximum)?.unwrap();
    let mut slot = scope
        .annotation
        .try_lock()
        .map_err(|_| Status::resource_exhausted("annotation context is busy"))?;
    if let Some(context) = slot.as_ref() {
        if context.closed {
            return Err(Status::failed_precondition(
                "annotation context cannot reopen after close",
            ));
        }
        if context.request != input
            || context.paged.as_ref().is_none_or(|s| {
                s.page_size != page_size
                    || s.quantum != quantum
                    || s.server_attribution != server_attribution
            })
        {
            return Err(Status::failed_precondition(
                "captured scope already has another annotation context",
            ));
        }
        if Instant::now() >= context.deadline {
            return Err(Status::deadline_exceeded(
                "annotation original deadline exceeded",
            ));
        }
        return context
            .paged
            .as_ref()
            .unwrap()
            .first
            .clone()
            .unwrap_or_else(|| {
                Err(Status::failed_precondition(
                    "annotation opening did not complete",
                ))
            });
    }
    // Independent idle residency is transferred only while physical DAG work owns scratch.
    let initial = input.encoded_len().saturating_mul(8).saturating_add(8192);
    let memory = host.cursors.reserve_annotation(initial)?;
    let now = crate::query_index::now()?;
    let expires = scope.wire.expires_at_unix_seconds.min(
        now.saturating_add(
            deadline
                .saturating_duration_since(Instant::now())
                .as_secs()
                .saturating_add(1) as i64,
        ),
    );
    let bytes = cursor();
    let id: [u8; 32] = bytes.as_slice().try_into().unwrap();
    *slot = Some(Context {
        request: input,
        id,
        deadline,
        budget: WorkBudget::new(
            limits.work_nodes,
            limits.work_edges,
            limits.work_bytes,
            limits.frontier as usize,
        ),
        read_bytes: 0,
        read_limit: limits.read_bytes as usize,
        diff_limits: jj_lib::diff::LineDiffLimits {
            max_input_bytes: limits.diff_input_bytes as usize,
            max_lines: limits.diff_lines as usize,
            max_work: limits.diff_work as usize,
            max_allocation_bytes: limits.diff_allocation_bytes as usize,
            max_recursion_depth: limits.diff_depth as usize,
        },
        result: None,
        closed: false,
        memory: Some(memory),
        paged: Some(State {
            engine: None,
            server_attribution,
            target: None,
            pending: None,
            grants: BTreeMap::new(),
            maximum: maximum as u32,
            page_size,
            quantum,
            expires,
            sequence: 0,
            emitted: 0,
            next: Vec::new(),
            first: None,
            last: None,
            failed: None,
            complete: false,
        }),
    });
    let context = slot.as_mut().unwrap();
    let result = advance(host, scope, repository, context, deadline, Some(&anchors));
    if let Err(error) = &result {
        context.paged.as_mut().unwrap().poison(error.clone());
    }
    context.paged.as_mut().unwrap().first = Some(result.clone());
    result
}

pub(crate) fn continue_frontier(
    host: &QueryHost,
    scope: &Scoped,
    repository: &str,
    input: q::ContinueAnnotationFrontierRequest,
    deadline: Instant,
) -> Result<q::AnnotationFrontierPage, Status> {
    let mut slot = scope
        .annotation
        .try_lock()
        .map_err(|_| Status::resource_exhausted("annotation context is busy"))?;
    let context = context(&mut slot, &input.context, true)?;
    let state = context
        .paged
        .as_mut()
        .ok_or_else(|| Status::failed_precondition("annotation context is not paged"))?;
    if input.cursor.len() != 32 {
        return Err(Status::invalid_argument("invalid annotation cursor"));
    }
    if let Some((previous, reply)) = &state.last {
        if input.cursor == *previous {
            return reply.clone();
        }
    }
    if let Some(error) = &state.failed {
        return Err(error.clone());
    }
    if state.complete || input.cursor != state.next {
        return Err(Status::invalid_argument(
            "annotation cursor is stale or does not match context",
        ));
    }
    state.sequence = state
        .sequence
        .checked_add(1)
        .ok_or_else(|| Status::resource_exhausted("annotation sequence exhausted"))?;
    let result = advance(host, scope, repository, context, deadline, None);
    let state = context.paged.as_mut().unwrap();
    if let Err(error) = &result {
        state.poison(error.clone());
    }
    state.last = Some((input.cursor, result.clone()));
    result
}

fn advance(
    host: &QueryHost,
    scope: &Scoped,
    repository: &str,
    context: &mut Context,
    rpc_deadline: Instant,
    anchors: Option<&[m::CommitId]>,
) -> Result<q::AnnotationFrontierPage, Status> {
    let _physical = host.admit_dag_history()?;
    let limits = effective_limits(
        host,
        context.request.limits.as_ref(),
        context.paged.as_ref().unwrap().maximum as usize,
    )?
    .unwrap();
    let path = context.request.path.split('/').collect::<Vec<_>>();
    let mut reader = Reader {
        host,
        scope,
        repository,
        budget: &mut context.budget,
        deadline: context.deadline.min(rpc_deadline),
        read_bytes: context.read_bytes,
        read_limit: Some(context.read_limit),
    };
    let step = (|| -> Step<q::AnnotationFrontierPage> {
        let state = context.paged.as_mut().unwrap();
        let mut adapter = dag::CanonicalDagReader {
            reader: &mut reader,
            path: &path,
        };
        if let Some(anchors) = anchors {
            adapter
                .reader
                .budget
                .charge(1, 0, context.request.encoded_len() as u64)
                .map_err(index_error)?;
            if state.server_attribution {
                state.target = Some(load_target(&mut adapter, anchors[0])?);
                state.complete = state.target.as_ref().unwrap().bytes.is_empty();
            }
            if !state.complete {
                state.engine = Some(
                    Frontier::new(
                        &mut adapter,
                        anchors,
                        tinytree_annotate::Limits {
                            max_commits: state.maximum as usize,
                            max_lines: host.config.max_blame_lines,
                            max_file_bytes: host.config.max_blame_file_bytes,
                        },
                        context.diff_limits,
                    )
                    .map_err(dag::engine_failure)?,
                );
            }
        }
        if let Some(engine) = state.engine.as_mut() {
            engine
                .advance(
                    &mut adapter,
                    NonZeroUsize::new(state.quantum as usize).unwrap(),
                )
                .map_err(dag::engine_failure)?;
        }
        let mut page = q::AnnotationFrontierPage {
            frontier: Some(q::AnnotationFrontier {
                context: context.id.to_vec(),
                scope_digest: scope.wire.content_digest.clone(),
                index_epoch: scope.wire.index_epoch.clone(),
                version: 3,
                anchors: context.request.anchors.clone(),
                path: context.request.path.clone(),
                expires_at_unix_seconds: state.expires,
                coverage: 0,
                scanned_commits: state
                    .engine
                    .as_ref()
                    .map_or(0, |engine| engine.progress().scanned_commits),
                nodes: Vec::new(),
                limits: Some(limits),
            }),
            next_cursor: Vec::new(),
            sequence: state.sequence,
            has_more: true,
            operations: 0,
            usage: None,
            max_commits: state.maximum,
            page_size: state.page_size,
            scan_quantum: state.quantum,
            phase: 0,
            emitted_nodes: state.emitted,
            blame: None,
        };
        if state.server_attribution
            && (state.complete
                || state
                    .engine
                    .as_ref()
                    .is_some_and(|engine| engine.progress().phase == FrontierPhase::Ready))
        {
            let revision = parse_revision(context.request.anchors.first().cloned())?;
            let target = state
                .target
                .take()
                .ok_or_else(|| Status::internal("annotation target missing"))?;
            page.blame = Some(attribute(
                &mut adapter,
                state.engine.as_mut(),
                target,
                revision,
                &context.request.path,
            )?);
        }
        if !state.server_attribution
            && state.engine.as_ref().is_some_and(|engine| {
                matches!(
                    engine.progress().phase,
                    FrontierPhase::Ready | FrontierPhase::Done
                )
            })
        {
            let engine = state.engine.as_mut().unwrap();
            while page.frontier.as_ref().unwrap().nodes.len() < state.page_size as usize {
                let next = match state.pending.take() {
                    Some(v) => Some(v),
                    None => engine
                        .next_node(&mut adapter)
                        .map_err(dag::engine_failure)?,
                };
                let Some(node) = next else {
                    break;
                };
                let (source_layer, topological_key) = match node.key {
                    GraphKey::Remote(k) => (0, k),
                    GraphKey::Draft(k) => (1, k),
                };
                let wire = q::AnnotationNode {
                    revision: Some(super::wire(node.id)),
                    parents: node.parents.iter().copied().map(super::wire).collect(),
                    source_layer,
                    topological_key,
                    value: Some(value(node.value.as_ref())),
                    affects_path: Some(node.affects_path),
                };
                // Leave room for fixed final cursor/counters added below.
                let size = page
                    .encoded_len()
                    .saturating_add(wire.encoded_len())
                    .saturating_add(128);
                if size > host.config.max_response_bytes {
                    if page.frontier.as_ref().unwrap().nodes.is_empty() {
                        return Err(Failure::Boundary(Coverage::HistoryReadLimit));
                    }
                    state.pending = Some(node);
                    break;
                }
                adapter
                    .reader
                    .budget
                    .charge(
                        1,
                        0,
                        (wire.encoded_len().saturating_mul(4).saturating_add(512)) as u64,
                    )
                    .map_err(index_error)?;
                state
                    .grants
                    .insert(node.id, wire.value.as_ref().unwrap().clone());
                page.frontier.as_mut().unwrap().nodes.push(wire);
                state.emitted = state
                    .emitted
                    .checked_add(1)
                    .ok_or_else(|| Status::resource_exhausted("annotation output overflow"))?;
            }
        }
        let progress = state.engine.as_ref().map(|engine| engine.progress());
        state.complete = if state.server_attribution {
            page.blame.is_some()
        } else {
            progress.is_some_and(|p| p.phase == FrontierPhase::Done) && state.pending.is_none()
        };
        page.has_more = !state.complete;
        page.phase = if state.complete {
            q::AnnotationFrontierPhase::AnnotationFrontierComplete
        } else {
            match progress.unwrap().phase {
                FrontierPhase::Discovering => {
                    q::AnnotationFrontierPhase::AnnotationFrontierDiscovering
                }
                FrontierPhase::Selecting => q::AnnotationFrontierPhase::AnnotationFrontierSelecting,
                _ => q::AnnotationFrontierPhase::AnnotationFrontierEmitting,
            }
        } as i32;
        page.operations = progress.map_or(0, |p| p.operations);
        page.emitted_nodes = state.emitted;
        page.usage = Some(q::AnnotationUsage {
            work_nodes: adapter.reader.budget.nodes_read(),
            work_edges: adapter.reader.budget.edges_read(),
            work_bytes: adapter.reader.budget.bytes_read(),
            read_bytes: adapter.reader.read_bytes as u64,
        });
        if state.complete {
            page.frontier.as_mut().unwrap().coverage = Coverage::HistoryComplete as i32;
        } else {
            page.next_cursor = cursor();
        }
        state.next = page.next_cursor.clone();
        if state.server_attribution && state.complete {
            // The final result, not the canonical graph, survives physical work.
            state.engine = None;
        }
        Ok(page)
    })();
    context.read_bytes = reader.read_bytes;
    let result = step.map_err(failure).and_then(|page| {
        reserve(host, context, retained(context, &page))?;
        if page.encoded_len() > host.config.max_response_bytes {
            return Err(Status::resource_exhausted(
                "annotation page exceeds response limit",
            ));
        }
        Ok(page)
    });
    // Teardown must precede releasing physical DAG admission on every refusal.
    // Successful state is covered by the independent idle reservation above.
    if let Err(error) = &result {
        context.paged.as_mut().unwrap().poison(error.clone());
    }
    result
}

struct Target {
    descriptor: m::Descriptor,
    bytes: Vec<u8>,
}

// Only the selected path is validated/read before graph discovery.
fn load_target(
    adapter: &mut dag::CanonicalDagReader<'_, '_>,
    revision: m::CommitId,
) -> Step<Target> {
    if revision == m::CommitId::ROOT {
        adapter.reader.canonical_source(revision)?;
        return Err(Status::not_found("blame path is absent at intrinsic root").into());
    }
    let commit = adapter.reader.commit(revision)?;
    let selected = adapter.reader.path_entry(&commit, adapter.path)?;
    let state = value(selected.as_ref()).state.unwrap();
    if state.kind == PathKind::HistoryPathAbsent as i32 {
        return Err(Status::not_found("blame path is absent at selected revision").into());
    }
    if state.kind != PathKind::HistoryPathFile as i32 {
        return Err(Status::failed_precondition(
            "blame requires a regular FILE at selected revision",
        )
        .into());
    }
    let (descriptor, target) = adapter.reader.file(&state)?;
    let lines = count_lines(&target);
    if lines > adapter.reader.host.config.max_blame_lines {
        return Err(Failure::Boundary(Coverage::HistoryDiffLimit));
    }
    Ok(Target {
        descriptor,
        bytes: target,
    })
}

// Reuse the retained graph, target and cumulative budget. Empty targets need no
// graph at all, as in ordinary one-shot annotation.
fn attribute(
    adapter: &mut dag::CanonicalDagReader<'_, '_>,
    engine: Option<&mut Frontier<GraphKey>>,
    selected: Target,
    revision: m::CommitId,
    path: &str,
) -> Step<q::BlameResult> {
    let Target {
        descriptor,
        bytes: target,
    } = selected;
    let lines = count_lines(&target);
    let scanned = engine.as_ref().map_or(0, |v| v.progress().scanned_commits);
    adapter
        .reader
        .budget
        .charge(
            lines as u64,
            0,
            lines
                .saturating_mul(std::mem::size_of::<Option<tinytree_annotate::Origin>>())
                .saturating_add(target.len()) as u64,
        )
        .map_err(index_error)?;
    let mut origins = vec![None; lines];
    if let Some(engine) = engine {
        engine
            .annotate(
                adapter,
                revision,
                &target,
                m::FileId(descriptor.key.id.0),
                &mut origins,
            )
            .map_err(dag::engine_failure)?;
    }
    let mut result = q::BlameResult {
        scope_digest: adapter.reader.scope.wire.content_digest.clone(),
        index_epoch: adapter.reader.scope.wire.index_epoch.clone(),
        revision: Some(super::wire(revision)),
        path: path.to_owned(),
        policy: q::HistoryPolicy::JjResolvedFixedPathV1 as i32,
        coverage: Coverage::HistoryComplete as i32,
        scanned_commits: scanned,
        boundary: None,
        target: Some(wire_descriptor(descriptor)),
        target_bytes: target,
        ranges: Vec::new(),
    };
    adapter
        .reader
        .budget
        .charge(
            lines as u64,
            0,
            lines.saturating_mul(std::mem::size_of::<q::BlameRange>().saturating_add(64)) as u64,
        )
        .map_err(index_error)?;
    for (line, origin) in origins.into_iter().enumerate() {
        adapter.reader.check()?;
        let origin = origin
            .ok_or_else(|| Status::internal("complete annotation has an unresolved origin"))?;
        if let Some(last) = result.ranges.last_mut() {
            if last
                .origin
                .as_ref()
                .is_some_and(|id| id.value == origin.commit.0)
                && last.original_line as usize + (last.end_line - last.start_line) as usize
                    == origin.line
            {
                last.end_line += 1;
                continue;
            }
        }
        result.ranges.push(q::BlameRange {
            start_line: line as u32,
            end_line: line as u32 + 1,
            origin: Some(super::wire(origin.commit)),
            original_line: origin.line as u32,
        });
    }
    Ok(result)
}
