//! Process-local, bounded IDs query cursors. HTTP admission is never retained by
//! idle cursors; accepted physical jobs and response bodies retain their owners.
use crate::{
    config::ConfigError,
    query_eval::{EvalError, EvalLimits, EvalStep, Evaluator},
    query_index::{QueryConfig, QueryHost, Scoped, index_error},
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
use tinytree_proto::query_v1 as q;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

fn clock() -> Result<i64, Status> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|v| i64::try_from(v.as_secs()).ok())
        .ok_or_else(|| Status::internal("query clock unavailable"))
}
fn active_memory(config: &QueryConfig) -> usize {
    limits(config).memory_bytes().unwrap_or(usize::MAX)
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
        || active_memory(c).saturating_add(history_memory(c)) > c.max_cursor_memory_bytes
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
struct State {
    pending: Option<q::GraphExpr>,
    evaluator: Option<Evaluator>,
    scope: Option<Arc<Scoped>>,
    active_memory: Option<Arc<OwnedSemaphorePermit>>,
    budget: WorkBudget,
    next: u64,
    last: Option<(u64, Result<q::EvaluatePage, Status>)>,
}
struct Garbage {
    state: Mutex<State>,
    _memory: Arc<OwnedSemaphorePermit>,
}
struct Record {
    owner: String,
    repository: String,
    query_id: [u8; 32],
    binding: [u8; 32],
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
    _active_memory: Option<Arc<OwnedSemaphorePermit>>,
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
        let mac = self.mac(&record.owner, &record.binding, &bytes);
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
        if request.projection != q::Projection::Ids as i32 {
            return Err(
                if request.projection == q::Projection::Graph as i32
                    || request.projection == q::Projection::Canonical as i32
                {
                    Status::unimplemented("query projection is not implemented")
                } else {
                    Status::invalid_argument("query projection missing or invalid")
                },
            );
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
        let binding = *blake3::hash(&request.encode_to_vec()).as_bytes();
        let now = clock()?;
        let (record, sequence) = if cursor.is_empty() {
            self.expire(now);
            let mut records = self.records.lock().unwrap();
            if let Some(existing) = records.values().find(|r| {
                r.owner == owner && r.repository == key.repository_id && r.query_id == query_id
            }) {
                if existing.binding != binding {
                    return Err(Status::already_exists(
                        "query ID reused with different inputs",
                    ));
                }
                (existing.clone(), 0)
            } else {
                self.room(&host.config, owner, &key.repository_id, &records)?;
                let scope = host.lookup(owner, &key.repository_id, &key.handle)?;
                let expression = request
                    .expression
                    .take()
                    .ok_or_else(|| Status::invalid_argument("query expression missing"))?;
                let history = self
                    .memory
                    .clone()
                    .try_acquire_many_owned(u32::try_from(history_memory(&host.config)).map_err(
                        |_| Status::resource_exhausted("query history memory exceeds budget"),
                    )?)
                    .map_err(|_| Status::resource_exhausted("query history memory exhausted"))?;
                let active = self
                    .memory
                    .clone()
                    .try_acquire_many_owned(u32::try_from(active_memory(&host.config)).map_err(
                        |_| Status::resource_exhausted("query cursor memory exceeds budget"),
                    )?)
                    .map_err(|_| Status::resource_exhausted("query cursor memory exhausted"))?;
                let expires = (now + host.config.query_ttl_seconds as i64)
                    .min(scope.wire.expires_at_unix_seconds);
                let id = random();
                let record = Arc::new(Record {
                    owner: owner.into(),
                    repository: key.repository_id.clone(),
                    query_id,
                    binding,
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
                        scope: Some(scope),
                        active_memory: Some(Arc::new(active)),
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
            if record.owner != owner
                || record.repository != key.repository_id
                || record.binding != binding
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
                state.evaluator = Some(
                    Evaluator::new(
                        scope.as_ref().unwrap().graph().clone(),
                        expression,
                        limits(&host.config),
                    )
                    .map_err(eval_error)?,
                );
            }
            let mut rows = Vec::new();
            let mut exhausted = false;
            for _ in 0..host.config.max_eval_quantum_steps {
                if record.expires <= clock()? {
                    return Err(Status::failed_precondition(
                        "query cursor expired during evaluation",
                    ));
                }

                let State {
                    evaluator, budget, ..
                } = &mut *state;
                match evaluator
                    .as_mut()
                    .unwrap()
                    .step(budget)
                    .map_err(eval_error)?
                {
                    EvalStep::Row(node) => {
                        rows.push(q::GraphRow {
                            commit: Some(q::CommitId {
                                value: node.commit().0.to_vec(),
                            }),
                            change: Some(q::ChangeId {
                                value: node.change_id().0.to_vec(),
                            }),
                            edges: Vec::new(),
                            canonical: None,
                        });
                        if rows.len() >= request.page_size as usize {
                            break;
                        }
                    }
                    EvalStep::Progress => {}
                    EvalStep::Done => {
                        exhausted = true;
                        break;
                    }
                }
            }
            let next = sequence
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("query sequence exhausted"))?;
            let page = q::EvaluatePage {
                scope_digest: record.scope_digest.to_vec(),
                index_epoch: record.epoch.to_vec(),
                sequence,
                rows,
                cursor: if exhausted {
                    Vec::new()
                } else {
                    self.token(&record, next)
                },
                exhausted: Some(exhausted),
            };
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
        state.last = Some((sequence, result.clone()));
        if terminal {
            state.evaluator = None;
            state.pending = None;
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
fn random() -> [u8; 32] {
    let mut value = [0; 32];
    value[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    value[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    value
}
