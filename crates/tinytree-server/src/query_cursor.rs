//! Process-local, bounded query cursors. HTTP admission is never retained by
//! idle cursors; accepted physical jobs and response bodies retain their owners.
use crate::{
    config::ConfigError,
    query_eval::{EvalError, EvalLimits, EvalStep, Evaluator},
    query_graph::{EdgeKind, GraphEvaluator, GraphLimits, GraphStep, SelectedRow},
    query_index::{QueryConfig, QueryHost, Scoped, index_error},
    query_overlay::GraphNode,
};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tinytree_index::WorkBudget;
use tinytree_model as m;
use tinytree_proto::query_v1 as q;
use tinytree_proto::v1 as p;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

fn bound_id(repository: &str, scope_handle: &[u8; 32], query_id: &[u8; 32]) -> Result<(), Status> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-query-scope-id-v2\0");
    hash.update(&(repository.len() as u64).to_be_bytes());
    hash.update(repository.as_bytes());
    hash.update(scope_handle);
    hash.update(&query_id[..16]);
    if query_id[16..] != hash.finalize().as_bytes()[..16] {
        return Err(Status::invalid_argument(
            "query identity belongs to another scope",
        ));
    }
    Ok(())
}
fn clock() -> Result<i64, Status> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|v| i64::try_from(v.as_secs()).ok())
        .ok_or_else(|| Status::internal("query clock unavailable"))
}
fn page_window(c: &QueryConfig) -> usize {
    c.max_response_bytes
        .saturating_mul(5)
        .saturating_add(
            c.max_page_size
                .saturating_mul(std::mem::size_of::<q::GraphRow>())
                .saturating_mul(2),
        )
        .saturating_add(4096)
}
fn active_memory(c: &QueryConfig, projection: i32) -> usize {
    let graph = if projection == q::Projection::Ids as i32 {
        0
    } else {
        graph_limits(c).memory_bytes().unwrap_or(usize::MAX)
    };
    limits(c)
        .memory_bytes()
        .unwrap_or(usize::MAX)
        .saturating_add(graph)
        .saturating_add(page_window(c).saturating_mul(3))
        .saturating_add(if projection == q::Projection::Canonical as i32 {
            c.max_commit_bytes.saturating_mul(8)
        } else {
            0
        })
}
fn graph_limits(c: &QueryConfig) -> GraphLimits {
    GraphLimits {
        max_nodes: c.max_graph_query_nodes,
        max_edges: c.max_graph_query_edges,
        max_keys: c.max_graph_query_keys,
        max_frames: c.max_graph_query_frames,
        max_step_work: c.max_graph_query_step_work,
        max_steps: c.max_graph_query_steps as u64,
    }
}
fn history_memory(config: &QueryConfig) -> usize {
    config
        .max_page_size
        .saturating_mul(256)
        .saturating_add(config.max_request_bytes.saturating_mul(2))
        .saturating_add(4096)
}
pub(crate) fn validate_config(c: &QueryConfig) -> Result<(), ConfigError> {
    if !(1..=4096).contains(&c.max_queries)
        || c.max_queries_per_principal == 0
        || c.max_queries_per_principal > c.max_queries
        || c.max_queries_per_repository == 0
        || c.max_queries_per_repository > c.max_queries
        || !(1..=65_536).contains(&c.max_query_replays)
        || c.max_query_replays_per_principal == 0
        || c.max_query_replays_per_principal > c.max_query_replays
        || c.max_query_replays_per_repository == 0
        || c.max_query_replays_per_repository > c.max_query_replays
        || !(1..=86_400).contains(&c.query_ttl_seconds)
        || !(1..=4096).contains(&c.max_page_size)
        || !(1..=1024).contains(&c.max_eval_ast_nodes)
        || !(1..=64).contains(&c.max_eval_ast_depth)
        || !(1..=100_000).contains(&c.max_eval_literals)
        || !(1..=100_000).contains(&c.max_eval_frontier)
        || !(1..=1_000_000).contains(&c.max_eval_intervals)
        || !(1..=100_000).contains(&c.max_eval_latest)
        || !(1..=4096).contains(&c.max_eval_step_work)
        || !(1..=100_000_000).contains(&c.max_eval_steps)
        || !(1..=4096).contains(&c.max_eval_quantum_steps)
        || c.max_cursor_memory_bytes > 1024 * 1024 * 1024
        || !(1..=100_000).contains(&c.max_graph_query_nodes)
        || !(1..=1_000_000).contains(&c.max_graph_query_edges)
        || !(1..=1_000_000).contains(&c.max_graph_query_keys)
        || !(1..=100_000).contains(&c.max_graph_query_frames)
        || !(1..=4096).contains(&c.max_graph_query_step_work)
        || !(1..=100_000_000).contains(&c.max_graph_query_steps)
        || active_memory(c, q::Projection::Canonical as i32).saturating_add(history_memory(c))
            > c.max_cursor_memory_bytes
    {
        return Err(ConfigError("invalid query cursor limits"));
    }
    Ok(())
}
fn limits(c: &QueryConfig) -> EvalLimits {
    EvalLimits {
        max_ast_nodes: c.max_eval_ast_nodes,
        max_ast_depth: c.max_eval_ast_depth,
        max_literals: c.max_eval_literals,
        max_frontier: c.max_eval_frontier,
        max_intervals: c.max_eval_intervals,
        max_latest: c.max_eval_latest,
        max_step_work: c.max_eval_step_work,
        max_steps: c.max_eval_steps as u64,
    }
}
fn eval_error(error: EvalError) -> Status {
    match error {
        EvalError::Graph(error) => index_error(error),
        EvalError::Invalid(_) => Status::invalid_argument("invalid query expression"),
        EvalError::Unsupported(_) => Status::unimplemented("query operator is not implemented"),
    }
}
struct ActiveReservation {
    permit: Mutex<OwnedSemaphorePermit>,
}
impl ActiveReservation {
    fn retain(&self, bytes: usize) -> Result<OwnedSemaphorePermit, Status> {
        self.permit.lock().unwrap().split(bytes).ok_or_else(|| {
            Status::internal("cursor retained memory accounting exceeded reservation")
        })
    }
}
enum RunningEvaluator {
    Ids(Evaluator),
    Graph(GraphEvaluator),
}
struct State {
    pending: Option<q::GraphExpr>,
    evaluator: Option<RunningEvaluator>,
    pending_row: Option<q::GraphRow>,
    scope: Option<Arc<Scoped>>,
    budget: WorkBudget,
    next: u64,
    last: Option<(u64, Result<q::EvaluatePage, Status>)>,
    // Destroy evaluator, pending/raw rows and replay bytes before permits.
    replay_memory: Option<OwnedSemaphorePermit>,
    active_memory: Option<Arc<ActiveReservation>>,
}
struct Garbage {
    state: Mutex<State>,
    _memory: Arc<OwnedSemaphorePermit>,
}
struct Record {
    owner: String,
    repository: String,
    query_id: [u8; 32],
    binding: Option<[u8; 32]>,
    scope_handle: [u8; 32],
    query_identity_version: u32,
    closed: AtomicBool,
    id: [u8; 32],
    scope_digest: [u8; 32],
    epoch: [u8; 16],
    expires: i64,
    active: AtomicBool,
    state: Option<Mutex<State>>,
    memory: Option<Arc<OwnedSemaphorePermit>>,
    cleanup: std::sync::mpsc::Sender<Garbage>,
}
impl Drop for Record {
    fn drop(&mut self) {
        let garbage = Garbage {
            state: self.state.take().expect("cursor state"),
            _memory: self.memory.take().expect("cursor memory"),
        };
        if let Err(error) = self.cleanup.send(garbage) {
            let _ = std::thread::Builder::new()
                .name("tinytree-cursor-cleanup-fallback".into())
                .spawn(move || drop(error.0));
        }
    }
}
struct PageOwner {
    _record: Arc<Record>,
    _scope: Option<Arc<Scoped>>,
    _active_memory: Option<Arc<ActiveReservation>>,
}
pub(crate) struct CursorPage {
    pub page: q::EvaluatePage,
    pub owner: Arc<dyn std::any::Any + Send + Sync>,
}
pub(crate) struct CursorHost {
    records: Mutex<HashMap<[u8; 32], Arc<Record>>>,
    memory: Arc<Semaphore>,
    key: [u8; 32],
    cleanup: std::sync::mpsc::Sender<Garbage>,
}
impl CursorHost {
    pub fn new(config: &QueryConfig) -> Result<Self, ConfigError> {
        let (cleanup, receiver) = std::sync::mpsc::channel::<Garbage>();
        std::thread::Builder::new()
            .name("tinytree-cursor-cleanup".into())
            .spawn(move || {
                while let Ok(garbage) = receiver.recv() {
                    drop(garbage.state);
                    drop(garbage._memory);
                }
            })
            .map_err(|_| ConfigError("cannot start cursor cleanup owner"))?;
        Ok(Self {
            records: Mutex::new(HashMap::new()),
            memory: Arc::new(Semaphore::new(config.max_cursor_memory_bytes)),
            key: random(),
            cleanup,
        })
    }
    pub fn expire(&self, now: i64) {
        let removed = {
            let mut registry = self.records.lock().unwrap();
            let expired: Vec<_> = registry
                .iter()
                .filter(|(_, r)| r.expires <= now)
                .map(|(id, _)| *id)
                .collect();
            expired
                .into_iter()
                .filter_map(|id| registry.remove(&id))
                .collect::<Vec<_>>()
        };
        drop(removed);
    }
    fn mac(&self, owner: &str, binding: &[u8; 32], prefix: &[u8]) -> blake3::Hash {
        let mut hash = blake3::Hasher::new_keyed(&self.key);
        hash.update(b"tinytree-query-cursor-v1\0");
        hash.update(&(owner.len() as u64).to_be_bytes());
        hash.update(owner.as_bytes());
        hash.update(binding);
        hash.update(prefix);
        hash.finalize()
    }
    fn token(&self, record: &Record, sequence: u64) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(128);
        bytes.extend_from_slice(&record.id);
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.extend_from_slice(&record.expires.to_be_bytes());
        bytes.extend_from_slice(&record.scope_digest);
        bytes.extend_from_slice(&record.epoch);
        let mac = self.mac(
            &record.owner,
            record.binding.as_ref().expect("open query binding"),
            &bytes,
        );
        bytes.extend_from_slice(mac.as_bytes());
        bytes
    }
    fn room(
        &self,
        c: &QueryConfig,
        owner: &str,
        repo: &str,
        records: &HashMap<[u8; 32], Arc<Record>>,
    ) -> Result<(), Status> {
        let active = |r: &&Arc<Record>| r.active.load(Ordering::Acquire);
        if records.values().filter(active).count() >= c.max_queries
            || records
                .values()
                .filter(active)
                .filter(|r| r.owner == owner)
                .count()
                >= c.max_queries_per_principal
            || records
                .values()
                .filter(active)
                .filter(|r| r.repository == repo)
                .count()
                >= c.max_queries_per_repository
        {
            return Err(Status::resource_exhausted("active query quota exceeded"));
        }
        self.history_room(c, owner, repo, records)
    }
    fn history_room(
        &self,
        c: &QueryConfig,
        owner: &str,
        repo: &str,
        records: &HashMap<[u8; 32], Arc<Record>>,
    ) -> Result<(), Status> {
        if records.len() >= c.max_query_replays
            || records.values().filter(|r| r.owner == owner).count()
                >= c.max_query_replays_per_principal
            || records.values().filter(|r| r.repository == repo).count()
                >= c.max_query_replays_per_repository
        {
            return Err(Status::resource_exhausted(
                "query replay history quota exceeded",
            ));
        }
        Ok(())
    }
    pub(crate) fn retire_closed(&self, owner: &str, repository: &str, scope: [u8; 32]) {
        let removed = {
            let mut records = self.records.lock().unwrap();
            let ids: Vec<_> = records
                .iter()
                .filter(|(_, r)| {
                    r.owner == owner
                        && r.repository == repository
                        && r.scope_handle == scope
                        && r.query_identity_version == 2
                        && r.closed.load(Ordering::Acquire)
                })
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| records.remove(&id))
                .collect::<Vec<_>>()
        };
        // Physical response/page owners keep their own Arc after registry GC.
        drop(removed);
    }
    pub fn close(
        &self,
        host: &QueryHost,
        owner: &str,
        key: &q::ScopeKey,
        query_id: &[u8],
    ) -> Result<(), Status> {
        let query_id: [u8; 32] = query_id
            .try_into()
            .map_err(|_| Status::invalid_argument("query ID must be 32 bytes"))?;
        let scope_handle: [u8; 32] = key
            .handle
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid scope handle"))?;
        let status = host.scope_status(owner, &key.repository_id, &scope_handle)?;
        if status.is_some_and(|(version, _)| version == 2) {
            bound_id(&key.repository_id, &scope_handle, &query_id)?;
        }
        let now = clock()?;
        self.expire(now);
        let record = {
            let mut records = self.records.lock().unwrap();
            if let Some(record) = records.values().find(|record| {
                record.owner == owner
                    && record.repository == key.repository_id
                    && record.query_id == query_id
            }) {
                if record.scope_handle != scope_handle {
                    return Err(Status::permission_denied("query scope binding mismatch"));
                }
                if record.query_identity_version == 2 {
                    bound_id(&key.repository_id, &scope_handle, &query_id)?;
                }
                record.clone()
            } else {
                if status == Some((2, true)) {
                    return Ok(());
                }
                // Closing before the first Evaluate is an ordered cancellation,
                // not a no-op. Its bounded tombstone prevents a late creation.
                let scope = match host.lookup(owner, &key.repository_id, &key.handle) {
                    Ok(scope) => scope,
                    // A closed/expired/missing scope cannot authorize any late
                    // creation. Cleanup is already complete; foreign ownership
                    // remains a permission error, not an existence oracle.
                    Err(error) if error.code() == tonic::Code::FailedPrecondition => return Ok(()),
                    Err(error) => return Err(error),
                };
                if scope.wire.query_identity_version == 2 {
                    bound_id(&key.repository_id, &scope_handle, &query_id)?;
                }
                self.history_room(&host.config, owner, &key.repository_id, &records)?;
                let charge = 4096usize
                    .saturating_add(owner.len().saturating_mul(2))
                    .saturating_add(key.repository_id.len().saturating_mul(2));
                let permit = self
                    .memory
                    .clone()
                    .try_acquire_many_owned(u32::try_from(charge).map_err(|_| {
                        Status::resource_exhausted("closed query metadata exceeds budget")
                    })?)
                    .map_err(|_| Status::resource_exhausted("query history memory exhausted"))?;
                let id = random();
                let record = Arc::new(Record {
                    owner: owner.into(),
                    repository: key.repository_id.clone(),
                    query_id,
                    binding: None,
                    scope_handle,
                    query_identity_version: scope.wire.query_identity_version,
                    closed: AtomicBool::new(true),
                    id,
                    scope_digest: scope
                        .wire
                        .content_digest
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::internal("invalid scope digest"))?,
                    epoch: scope
                        .wire
                        .index_epoch
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::internal("invalid scope epoch"))?,
                    expires: (now + host.config.query_ttl_seconds as i64)
                        .min(scope.wire.expires_at_unix_seconds),
                    active: AtomicBool::new(false),
                    state: Some(Mutex::new(State {
                        pending: None,
                        evaluator: None,
                        pending_row: None,
                        scope: None,
                        budget: host.config.budget(),
                        next: 0,
                        last: None,
                        replay_memory: None,
                        active_memory: None,
                    })),
                    memory: Some(Arc::new(permit)),
                    cleanup: self.cleanup.clone(),
                });
                records.insert(id, record.clone());
                record
            }
        };
        let mut state = record.state.as_ref().unwrap().lock().unwrap();
        record.closed.store(true, Ordering::Release);
        // This method only runs in an owned blocking job. Physical previously
        // returned pages independently retain their scope and active reservation.
        state.evaluator = None;
        state.pending = None;
        state.pending_row = None;
        state.last = None;
        state.replay_memory = None;
        state.scope = None;
        state.active_memory = None;
        record.active.store(false, Ordering::Release);
        drop(state);
        if record.query_identity_version == 2
            && host.scope_status(owner, &key.repository_id, &scope_handle)? == Some((2, true))
        {
            self.retire_closed(owner, &key.repository_id, scope_handle);
        }
        Ok(())
    }
    /// Ephemeral fact evaluation shares the cursor allocation pool but owns no
    /// Record, query ID, replay history or tombstone. Scope/Work ownership comes
    /// from QueryFacts' accepted blocking job, including after cancellation.
    pub fn membership(
        &self,
        scope: &Scoped,
        expression: q::GraphExpr,
        candidate: m::CommitId,
        budget: &mut WorkBudget,
        config: &QueryConfig,
        deadline: tokio::time::Instant,
        remaining_steps: &mut u64,
        inputs: &mut usize,
    ) -> Result<bool, Status> {
        if *remaining_steps == 0 {
            return Err(Status::resource_exhausted(
                "query membership step budget exhausted",
            ));
        }
        let mut policy = limits(config);
        policy.max_steps = *remaining_steps;
        let bytes = policy
            .memory_bytes()
            .and_then(|n| n.checked_add(4096))
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| Status::resource_exhausted("query membership memory budget exceeded"))?;
        // Declare this owner before the evaluator so all compiled state is
        // destroyed before its permit, also on graph errors or timeout.
        let _memory = self
            .memory
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| Status::resource_exhausted("query membership memory exhausted"))?;
        let mut evaluator =
            Evaluator::membership(scope.graph().clone(), expression, candidate, policy)
                .map_err(eval_error)?;
        *inputs = inputs
            .saturating_add(evaluator.literal_count())
            .saturating_add(1);
        if *inputs > config.max_input_ids {
            return Err(Status::resource_exhausted(
                "query aggregate ID count exceeded",
            ));
        }
        // Full AST compilation precedes an exact negative scope answer.
        if !scope
            .graph()
            .has_id(candidate, budget)
            .map_err(index_error)?
        {
            return Ok(false);
        }
        let answer = loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(
                    "query membership deadline exceeded",
                ));
            }
            match evaluator.step(budget).map_err(eval_error)? {
                EvalStep::Progress => {}
                EvalStep::Done => break false,
                EvalStep::Row(row) => {
                    if row.commit() != candidate {
                        return Err(Status::data_loss(
                            "query membership result identity mismatch",
                        ));
                    }
                    break true;
                }
            }
        };
        *remaining_steps = remaining_steps
            .checked_sub(evaluator.steps())
            .ok_or_else(|| Status::resource_exhausted("query membership step budget exhausted"))?;
        Ok(answer)
    }
    pub fn evaluate(
        &self,
        host: &QueryHost,
        owner: &str,
        mut request: q::EvaluateRequest,
    ) -> Result<CursorPage, Status> {
        let key = request
            .scope
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("query scope missing"))?;
        if ![
            q::Projection::Ids as i32,
            q::Projection::Graph as i32,
            q::Projection::Canonical as i32,
        ]
        .contains(&request.projection)
        {
            return Err(Status::invalid_argument(
                "query projection missing or invalid",
            ));
        }
        if request.page_size == 0 || request.page_size as usize > host.config.max_page_size {
            return Err(Status::resource_exhausted("query page size exceeds budget"));
        }
        let query_id: [u8; 32] = request
            .query_id
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("query ID must be 32 bytes"))?;
        let key = key.clone();
        let cursor = std::mem::take(&mut request.cursor);
        let request_bytes = request.encoded_len();
        let binding = *blake3::hash(&request.encode_to_vec()).as_bytes();
        let now = clock()?;
        let (record, sequence) = if cursor.is_empty() {
            self.expire(now);
            let mut records = self.records.lock().unwrap();
            if let Some(existing) = records.values().find(|r| {
                r.owner == owner && r.repository == key.repository_id && r.query_id == query_id
            }) {
                if existing.query_identity_version == 2 {
                    let handle: [u8; 32] = key
                        .handle
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::invalid_argument("invalid scope handle"))?;
                    bound_id(&key.repository_id, &handle, &query_id)?;
                }
                if existing.closed.load(Ordering::Acquire) {
                    return Err(Status::failed_precondition("query is closed"));
                }
                if existing.binding != Some(binding) {
                    return Err(Status::already_exists(
                        "query ID reused with different inputs",
                    ));
                }
                (existing.clone(), 0)
            } else {
                self.room(&host.config, owner, &key.repository_id, &records)?;
                let scope = host.lookup(owner, &key.repository_id, &key.handle)?;
                if scope.wire.query_identity_version == 2 {
                    let handle: [u8; 32] = key
                        .handle
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::invalid_argument("invalid scope handle"))?;
                    bound_id(&key.repository_id, &handle, &query_id)?;
                }
                let expression = request
                    .expression
                    .take()
                    .ok_or_else(|| Status::invalid_argument("query expression missing"))?;
                let history = self
                    .memory
                    .clone()
                    .try_acquire_many_owned(
                        u32::try_from(
                            request_bytes
                                .saturating_mul(2)
                                .saturating_add(owner.len().saturating_mul(2))
                                .saturating_add(4096),
                        )
                        .map_err(|_| {
                            Status::resource_exhausted("query history memory exceeds budget")
                        })?,
                    )
                    .map_err(|_| Status::resource_exhausted("query history memory exhausted"))?;
                let active = self
                    .memory
                    .clone()
                    .try_acquire_many_owned(
                        u32::try_from(active_memory(&host.config, request.projection)).map_err(
                            |_| Status::resource_exhausted("query cursor memory exceeds budget"),
                        )?,
                    )
                    .map_err(|_| Status::resource_exhausted("query cursor memory exhausted"))?;
                let expires = (now + host.config.query_ttl_seconds as i64)
                    .min(scope.wire.expires_at_unix_seconds);
                let id = random();
                let record = Arc::new(Record {
                    owner: owner.into(),
                    repository: key.repository_id.clone(),
                    query_id,
                    binding: Some(binding),
                    scope_handle: key
                        .handle
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::invalid_argument("invalid scope handle"))?,
                    query_identity_version: scope.wire.query_identity_version,
                    closed: AtomicBool::new(false),
                    id,
                    scope_digest: scope
                        .wire
                        .content_digest
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::internal("invalid scope digest"))?,
                    epoch: scope
                        .wire
                        .index_epoch
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::internal("invalid scope epoch"))?,
                    expires,
                    active: AtomicBool::new(true),
                    state: Some(Mutex::new(State {
                        pending: Some(expression),
                        evaluator: None,
                        pending_row: None,
                        replay_memory: None,
                        scope: Some(scope),
                        active_memory: Some(Arc::new(ActiveReservation {
                            permit: Mutex::new(active),
                        })),
                        budget: host.config.budget(),
                        next: 0,
                        last: None,
                    })),
                    memory: Some(Arc::new(history)),
                    cleanup: self.cleanup.clone(),
                });
                records.insert(id, record.clone());
                (record, 0)
            }
        } else {
            if cursor.len() != 128 {
                return Err(Status::permission_denied("invalid query cursor"));
            }
            let mac = blake3::Hash::from(<[u8; 32]>::try_from(&cursor[96..]).unwrap());
            if self.mac(owner, &binding, &cursor[..96]) != mac {
                return Err(Status::permission_denied("invalid query cursor"));
            }
            let expires = i64::from_be_bytes(cursor[40..48].try_into().unwrap());
            if expires <= now {
                return Err(Status::failed_precondition("query cursor expired"));
            }
            let id: [u8; 32] = cursor[..32].try_into().unwrap();
            let record = self
                .records
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .ok_or_else(|| Status::failed_precondition("query cursor unavailable"))?;
            if record.closed.load(Ordering::Acquire) {
                return Err(Status::failed_precondition("query is closed"));
            }
            if record.owner != owner
                || record.repository != key.repository_id
                || record.binding != Some(binding)
                || record.scope_digest.as_slice() != &cursor[48..80]
                || record.epoch.as_slice() != &cursor[80..96]
                || record.expires != expires
            {
                return Err(Status::permission_denied("query cursor binding mismatch"));
            }
            (
                record,
                u64::from_be_bytes(cursor[32..40].try_into().unwrap()),
            )
        };
        if record.expires <= clock()? {
            return Err(Status::failed_precondition("query cursor expired"));
        }
        let mut state = record.state.as_ref().unwrap().lock().unwrap();
        if record.closed.load(Ordering::Acquire) {
            return Err(Status::failed_precondition("query is closed"));
        }

        if record.expires <= clock()? {
            return Err(Status::failed_precondition("query cursor expired"));
        }

        if let Some((prior, result)) = &state.last {
            if *prior == sequence {
                let page = result.clone()?;
                let response_owner = Arc::new(PageOwner {
                    _record: record.clone(),
                    _scope: state.scope.clone(),
                    _active_memory: state.active_memory.clone(),
                });
                return Ok(CursorPage {
                    page,
                    owner: response_owner,
                });
            }
        }
        if sequence != state.next || !record.active.load(Ordering::Acquire) {
            return Err(Status::failed_precondition(
                "query cursor replay is no longer retained",
            ));
        }
        let scope = state.scope.clone();
        let memory = state.active_memory.clone();
        #[cfg(test)]
        host.pause_query()?;
        let result = (|| {
            if let Some(expression) = state.pending.take() {
                let graph = scope.as_ref().unwrap().graph().clone();
                let ids = Evaluator::new(graph.clone(), expression, limits(&host.config))
                    .map_err(eval_error)?;
                state.evaluator = Some(if request.projection == q::Projection::Ids as i32 {
                    RunningEvaluator::Ids(ids)
                } else {
                    RunningEvaluator::Graph(
                        GraphEvaluator::new(graph, ids, graph_limits(&host.config))
                            .map_err(eval_error)?,
                    )
                });
            }
            let next_sequence = sequence
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("query sequence exhausted"))?;
            let mut page = q::EvaluatePage {
                scope_digest: record.scope_digest.to_vec(),
                index_epoch: record.epoch.to_vec(),
                sequence,
                rows: Vec::with_capacity(request.page_size as usize),
                cursor: self.token(&record, next_sequence),
                exhausted: Some(false),
            };
            let mut encoded = page.encoded_len();
            for _ in 0..host.config.max_eval_quantum_steps {
                if record.expires <= clock()? {
                    return Err(Status::failed_precondition(
                        "query cursor expired during evaluation",
                    ));
                }
                let row = if let Some(row) = state.pending_row.take() {
                    Some(row)
                } else {
                    let State {
                        evaluator, budget, ..
                    } = &mut *state;
                    match evaluator.as_mut().unwrap() {
                        RunningEvaluator::Ids(evaluator) => {
                            match evaluator.step(budget).map_err(eval_error)? {
                                EvalStep::Row(node) => Some(ids_row(&node)),
                                EvalStep::Progress => None,
                                EvalStep::Done => {
                                    page.exhausted = Some(true);
                                    page.cursor.clear();
                                    break;
                                }
                            }
                        }
                        RunningEvaluator::Graph(evaluator) => {
                            match evaluator.step(budget).map_err(eval_error)? {
                                GraphStep::Row(row) => Some(graph_row(
                                    host,
                                    &record.repository,
                                    scope.as_ref().unwrap(),
                                    row,
                                    request.projection == q::Projection::Canonical as i32,
                                    budget,
                                    encoded_header(&record, self, next_sequence),
                                )?),
                                GraphStep::Progress => None,
                                GraphStep::Done => {
                                    page.exhausted = Some(true);
                                    page.cursor.clear();
                                    break;
                                }
                            }
                        }
                    }
                };
                let Some(row) = row else { continue };
                let addition = field_bytes(row.encoded_len());
                if encoded.saturating_add(addition) > host.config.max_response_bytes {
                    if page.rows.is_empty() {
                        return Err(Status::resource_exhausted(
                            "one query row exceeds response budget",
                        ));
                    }
                    if row_memory(&row) > page_window(&host.config) {
                        return Err(Status::resource_exhausted(
                            "pending row allocation exceeds window",
                        ));
                    }
                    state.pending_row = Some(row);
                    break;
                }
                encoded += addition;
                page.rows.push(row);
                if page_memory(&page) > page_window(&host.config) {
                    return Err(Status::resource_exhausted(
                        "query page allocation exceeds window",
                    ));
                }
                if page.rows.len() >= request.page_size as usize {
                    break;
                }
            }
            let next = sequence
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("query sequence exhausted"))?;
            if page.encoded_len() > host.config.max_response_bytes {
                return Err(Status::resource_exhausted(
                    "query page exceeds response budget",
                ));
            }
            state.next = next;
            Ok(page)
        })();
        let terminal = result
            .as_ref()
            .map_or(true, |page| page.exhausted == Some(true));
        let retained = result.clone();
        if terminal {
            let bytes = retained.as_ref().map_or(0, page_memory);
            state.replay_memory = if bytes > 0 {
                Some(memory.as_ref().unwrap().retain(bytes)?)
            } else {
                None
            };
        }
        state.last = Some((sequence, retained));
        if terminal {
            state.evaluator = None;
            state.pending = None;
            state.pending_row = None;
            state.scope = None;
            state.active_memory = None;
            record.active.store(false, Ordering::Release);
        }
        let page = result?;
        drop(state);
        Ok(CursorPage {
            page,
            owner: Arc::new(PageOwner {
                _record: record,
                _scope: scope,
                _active_memory: memory,
            }),
        })
    }
}
fn field_bytes(bytes: usize) -> usize {
    1usize
        .saturating_add(prost::length_delimiter_len(bytes))
        .saturating_add(bytes)
}
fn ids_row(node: &GraphNode) -> q::GraphRow {
    q::GraphRow {
        commit: Some(q::CommitId {
            value: node.commit().0.to_vec(),
        }),
        change: Some(q::ChangeId {
            value: node.change_id().0.to_vec(),
        }),
        edges: Vec::new(),
        canonical: None,
    }
}
fn encoded_header(record: &Record, host: &CursorHost, sequence: u64) -> usize {
    q::EvaluatePage {
        scope_digest: record.scope_digest.to_vec(),
        index_epoch: record.epoch.to_vec(),
        sequence,
        rows: Vec::new(),
        cursor: host.token(record, sequence),
        exhausted: Some(false),
    }
    .encoded_len()
}
fn wire_descriptor(descriptor: m::Descriptor) -> p::ObjectDescriptor {
    p::ObjectDescriptor {
        key: Some(p::ObjectKey {
            kind: descriptor.key.kind as i32,
            native_id: descriptor.key.id.0.to_vec(),
        }),
        content_blake3: descriptor.key.id.0.to_vec(),
        size_bytes: descriptor.size_bytes,
        object_format_version: descriptor.format_version as u32,
    }
}
fn graph_row(
    host: &QueryHost,
    repository: &str,
    scope: &Scoped,
    selected: SelectedRow,
    canonical: bool,
    budget: &mut WorkBudget,
    header: usize,
) -> Result<q::GraphRow, Status> {
    let mut row = ids_row(&selected.node);
    // Native IDs are fixed-width; every selected-edge variant has the same
    // encoded/allocation shape. Preflight before allocating a large edge Vec.
    let edge_shape = q::SelectedEdge {
        edge: Some(q::selected_edge::Edge::Direct(q::DirectEdge {
            target: Some(q::CommitId { value: vec![0; 32] }),
        })),
    };
    let mut encoded = row
        .encoded_len()
        .saturating_add(field_bytes(edge_shape.encoded_len()).saturating_mul(selected.edges.len()));
    let expected = selected.node.descriptor();
    let has_payload = canonical && selected.node.commit() != m::CommitId::ROOT;
    if has_payload {
        let size = usize::try_from(expected.size_bytes)
            .map_err(|_| Status::resource_exhausted("canonical row size overflow"))?;
        if size > host.config.max_commit_bytes {
            return Err(Status::resource_exhausted(
                "canonical row exceeds commit byte budget",
            ));
        }
        let descriptor_bytes = p::ObjectData {
            descriptor: Some(wire_descriptor(expected)),
            canonical_envelope: Vec::new(),
        }
        .encoded_len();
        encoded = encoded.saturating_add(field_bytes(
            descriptor_bytes.saturating_add(field_bytes(size)),
        ));
    }
    if header.saturating_add(field_bytes(encoded)) > host.config.max_response_bytes {
        return Err(Status::resource_exhausted(
            "one query row exceeds response budget",
        ));
    }
    let mut edges = Vec::with_capacity(selected.edges.len());
    for edge in &selected.edges {
        let target = Some(q::CommitId {
            value: edge.target.commit().0.to_vec(),
        });
        edges.push(q::SelectedEdge {
            edge: Some(match edge.kind {
                EdgeKind::Direct => q::selected_edge::Edge::Direct(q::DirectEdge { target }),
                EdgeKind::Indirect => q::selected_edge::Edge::Indirect(q::IndirectEdge { target }),
                EdgeKind::Missing => {
                    q::selected_edge::Edge::Missing(q::MissingEdge { boundary: target })
                }
            }),
        });
    }
    row.edges = edges.into_boxed_slice().into_vec();
    if has_payload {
        let data = if let Some(bytes) = scope
            .graph()
            .draft_bytes(&selected.node, budget)
            .map_err(index_error)?
        {
            bytes.to_vec()
        } else {
            let descriptor = host
                .store
                .descriptor(repository, expected.key)
                .map_err(|_| Status::unavailable("canonical query object unavailable"))?
                .ok_or_else(|| Status::data_loss("retained canonical query object missing"))?;
            if descriptor != expected {
                return Err(Status::data_loss(
                    "canonical query descriptor does not match scoped graph",
                ));
            }
            budget
                .charge(0, 0, expected.size_bytes)
                .map_err(index_error)?;
            let (descriptor, data) = host
                .store
                .read_metadata(repository, expected.key)
                .map_err(|_| Status::data_loss("canonical query object read failed"))?;
            if descriptor != expected {
                return Err(Status::data_loss(
                    "canonical query descriptor changed during read",
                ));
            }
            data
        };
        if expected.key.kind != m::Kind::Commit
            || expected.key.id.0 != selected.node.commit().0
            || expected.format_version != m::FORMAT_VERSION
            || data.len() as u64 != expected.size_bytes
            || blake3::hash(&data).as_bytes() != &expected.key.id.0
        {
            return Err(Status::data_loss(
                "canonical query object identity mismatch",
            ));
        }
        let object = m::decode(
            &data,
            m::Limits {
                max_object_bytes: host.config.max_commit_bytes,
                max_items: host.config.max_commit_parents,
                ..Default::default()
            },
        )
        .map_err(|error| match error {
            m::Error::Limit => {
                Status::resource_exhausted("canonical query shape exceeds configured limits")
            }
            _ => Status::data_loss("invalid canonical query object"),
        })?;
        let m::Object::Commit(commit) = object else {
            return Err(Status::data_loss("canonical query object type mismatch"));
        };
        let parents = scope
            .graph()
            .parent_nodes(&selected.node, budget)
            .map_err(index_error)?;
        if commit.change_id != selected.node.change_id()
            || (commit.root_tree.terms.len() > 1) != selected.node.has_conflict()
            || commit.committer.timestamp.millis_since_epoch != selected.node.committer_millis()
            || !commit
                .parents
                .iter()
                .copied()
                .eq(parents.iter().map(GraphNode::commit))
        {
            return Err(Status::data_loss(
                "canonical query metadata does not match scoped graph",
            ));
        }
        row.canonical = Some(p::ObjectData {
            descriptor: Some(wire_descriptor(expected)),
            canonical_envelope: data.into_boxed_slice().into_vec(),
        });
    }
    if row_memory(&row) > page_window(&host.config) {
        return Err(Status::resource_exhausted(
            "query row allocation exceeds window",
        ));
    }
    Ok(row)
}
fn allocation(capacity: usize, size: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        capacity.saturating_mul(size).saturating_add(32)
    }
}
fn row_heap(row: &q::GraphRow) -> usize {
    let mut bytes = row
        .commit
        .as_ref()
        .map_or(0, |id| allocation(id.value.capacity(), 1))
        .saturating_add(
            row.change
                .as_ref()
                .map_or(0, |id| allocation(id.value.capacity(), 1)),
        )
        .saturating_add(allocation(
            row.edges.capacity(),
            std::mem::size_of::<q::SelectedEdge>(),
        ));
    for edge in &row.edges {
        let target = match &edge.edge {
            Some(q::selected_edge::Edge::Direct(v)) => v.target.as_ref(),
            Some(q::selected_edge::Edge::Indirect(v)) => v.target.as_ref(),
            Some(q::selected_edge::Edge::Missing(v)) => v.boundary.as_ref(),
            None => None,
        };
        bytes = bytes.saturating_add(target.map_or(0, |id| allocation(id.value.capacity(), 1)));
    }
    if let Some(object) = &row.canonical {
        bytes = bytes.saturating_add(allocation(object.canonical_envelope.capacity(), 1));
        if let Some(descriptor) = &object.descriptor {
            bytes = bytes.saturating_add(allocation(descriptor.content_blake3.capacity(), 1));
            if let Some(key) = &descriptor.key {
                bytes = bytes.saturating_add(allocation(key.native_id.capacity(), 1));
            }
        }
    }
    bytes
}
fn row_memory(row: &q::GraphRow) -> usize {
    std::mem::size_of::<q::GraphRow>()
        .saturating_add(row_heap(row))
        .saturating_mul(2)
        .saturating_add(1024)
}
fn page_memory(page: &q::EvaluatePage) -> usize {
    let mut bytes = std::mem::size_of::<q::EvaluatePage>()
        .saturating_add(allocation(page.scope_digest.capacity(), 1))
        .saturating_add(allocation(page.index_epoch.capacity(), 1))
        .saturating_add(allocation(page.cursor.capacity(), 1))
        .saturating_add(allocation(
            page.rows.capacity(),
            std::mem::size_of::<q::GraphRow>(),
        ));
    for row in &page.rows {
        bytes = bytes.saturating_add(row_heap(row));
    }
    bytes.saturating_mul(2).saturating_add(1024)
}
fn random() -> [u8; 32] {
    let mut value = [0; 32];
    value[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    value[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    value
}
