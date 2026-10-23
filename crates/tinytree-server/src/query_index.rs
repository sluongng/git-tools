//! Persistent derived graph hosting. Canonical object ACKs never wait for this
//! queue. Process-local scope handles expire; durable verified nodes are reused.
use crate::{ServiceConfig, config::ConfigError};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tinytree_index::{Index, Scope, WorkBudget};
use tinytree_model as m;
use tinytree_proto::query_v1 as q;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

#[derive(Clone, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueryConfig {
    pub max_scopes: usize,
    pub max_scopes_per_principal: usize,
    pub max_scopes_per_repository: usize,
    pub max_scope_memory_bytes: usize,
    pub scope_ttl_seconds: u64,
    pub expiry_interval_ms: u64,
    pub max_roots: usize,
    pub max_request_bytes: usize,
    pub max_inflight: usize,
    pub max_request_memory_bytes: usize,
    pub max_facts: usize,
    pub max_input_ids: usize,
    pub max_result_ids: usize,
    pub max_response_bytes: usize,
    pub max_work_nodes: u64,
    pub max_work_edges: u64,
    pub max_work_bytes: u64,
    pub max_frontier: usize,
    pub max_pending_warms: usize,
    pub max_warm_jobs: usize,
    pub max_warm_memory_bytes: usize,
    pub max_warm_nodes: usize,
    pub max_warm_edges: usize,
    pub max_warm_bytes: usize,
    pub max_warm_frontier: usize,
    pub max_commit_bytes: usize,
    pub max_commit_parents: usize,
    pub warm_timeout_seconds: u64,
    pub max_index_nodes: usize,
    pub max_index_record_bytes: usize,
    pub max_index_bytes: u64,
}
impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_scopes: 128,
            max_scopes_per_principal: 16,
            max_scopes_per_repository: 64,
            max_scope_memory_bytes: 8 * 1024 * 1024,
            scope_ttl_seconds: 300,
            expiry_interval_ms: 1000,
            max_roots: 64,
            max_request_bytes: 256 * 1024,
            max_inflight: 2,
            max_request_memory_bytes: 256 * 1024 * 1024,
            max_facts: 64,
            max_input_ids: 1024,
            max_result_ids: 16_384,
            max_response_bytes: 2 * 1024 * 1024,
            max_work_nodes: 100_000,
            max_work_edges: 1_000_000,
            max_work_bytes: 32 * 1024 * 1024,
            max_frontier: 100_000,
            max_pending_warms: 32,
            max_warm_jobs: 1,
            max_warm_memory_bytes: 128 * 1024 * 1024,
            max_warm_nodes: 100_000,
            max_warm_edges: 200_000,
            max_warm_bytes: 16 * 1024 * 1024,
            max_warm_frontier: 100_000,
            max_commit_bytes: 256 * 1024,
            max_commit_parents: 4096,
            warm_timeout_seconds: 300,
            max_index_nodes: 1_000_000,
            max_index_record_bytes: 1024 * 1024,
            max_index_bytes: 1024 * 1024 * 1024,
        }
    }
}
impl QueryConfig {
    pub(crate) fn request_memory(&self, request_bytes: usize) -> usize {
        request_bytes
            .min(self.max_request_bytes)
            .saturating_mul(128)
            .saturating_add(self.max_frontier.saturating_mul(256))
            .saturating_add(self.max_response_bytes.saturating_mul(4))
            .saturating_add(self.max_index_record_bytes.saturating_mul(4))
            .saturating_add(8 * 1024 * 1024)
    }
    fn warm_memory(&self) -> usize {
        self.max_warm_bytes
            .saturating_mul(4)
            .saturating_add(self.max_warm_nodes.saturating_mul(128))
            .saturating_add(self.max_warm_edges.saturating_mul(64))
            .saturating_add(self.max_commit_bytes.saturating_mul(8))
            .saturating_add(8 * 1024 * 1024)
    }
    pub(crate) fn validate(&self, request_bytes: usize) -> Result<(), ConfigError> {
        if !(1..=4096).contains(&self.max_scopes)
            || self.max_scopes_per_principal == 0
            || self.max_scopes_per_principal > self.max_scopes
            || self.max_scopes_per_repository == 0
            || self.max_scopes_per_repository > self.max_scopes
            || !(1024..=1024 * 1024 * 1024).contains(&self.max_scope_memory_bytes)
            || !(1..=86_400).contains(&self.scope_ttl_seconds)
            || !(10..=60_000).contains(&self.expiry_interval_ms)
            || !(1..=4096).contains(&self.max_roots)
            || !(1024..=1024 * 1024).contains(&self.max_request_bytes)
            || !(1..=32).contains(&self.max_inflight)
            || self.max_request_memory_bytes > 1024 * 1024 * 1024
            || self.request_memory(request_bytes) > self.max_request_memory_bytes
            || !(1..=1024).contains(&self.max_facts)
            || !(1..=100_000).contains(&self.max_input_ids)
            || !(1..=100_000).contains(&self.max_result_ids)
            || !(1024..=16 * 1024 * 1024).contains(&self.max_response_bytes)
            || self.max_work_nodes == 0
            || self.max_work_nodes > 1_000_000
            || self.max_work_edges == 0
            || self.max_work_edges > 10_000_000
            || self.max_work_bytes == 0
            || self.max_work_bytes > 1024 * 1024 * 1024
            || self.max_frontier == 0
            || self.max_frontier > 1_000_000
            || !(1..=1024).contains(&self.max_pending_warms)
            || !(1..=4).contains(&self.max_warm_jobs)
            || self.max_warm_memory_bytes > 1024 * 1024 * 1024
            || self.warm_memory().saturating_mul(self.max_warm_jobs) > self.max_warm_memory_bytes
            || self.max_warm_nodes == 0
            || self.max_warm_nodes > 1_000_000
            || self.max_warm_edges == 0
            || self.max_warm_edges > 10_000_000
            || self.max_warm_bytes == 0
            || self.max_warm_bytes > 256 * 1024 * 1024
            || self.max_warm_frontier == 0
            || self.max_warm_frontier > 1_000_000
            || !(1024..=16 * 1024 * 1024).contains(&self.max_commit_bytes)
            || !(1..=65_536).contains(&self.max_commit_parents)
            || !(1..=3600).contains(&self.warm_timeout_seconds)
            || self.max_index_nodes == 0
            || self.max_index_nodes > 10_000_000
            || !(1024..=16 * 1024 * 1024).contains(&self.max_index_record_bytes)
            || self.max_index_bytes == 0
            || self.max_index_bytes > 64 * 1024 * 1024 * 1024
        {
            return Err(ConfigError("invalid derived query limits"));
        }
        Ok(())
    }
    pub(crate) fn index_limits(&self) -> tinytree_index::Limits {
        tinytree_index::Limits {
            max_nodes: self.max_index_nodes,
            max_record_bytes: self.max_index_record_bytes,
            max_index_bytes: self.max_index_bytes,
            max_roots: self.max_roots,
            max_parents: self.max_commit_parents,
            model: m::Limits {
                max_object_bytes: self.max_commit_bytes,
                max_items: self.max_commit_parents,
                ..Default::default()
            },
            ..Default::default()
        }
    }
    pub(crate) fn budget(&self) -> WorkBudget {
        WorkBudget::new(
            self.max_work_nodes,
            self.max_work_edges,
            self.max_work_bytes,
            self.max_frontier,
        )
    }
}
pub(crate) fn now() -> Result<i64, Status> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("query clock unavailable"))?
            .as_secs(),
    )
    .map_err(|_| Status::internal("query clock unavailable"))
}
pub(crate) fn index_error(error: tinytree_index::Error) -> Status {
    use tinytree_index::Error;
    match error {
        Error::NotReady | Error::Unavailable => {
            Status::unavailable("derived graph coverage is not ready")
        }
        Error::BudgetExceeded => Status::resource_exhausted("exact graph work budget exhausted"),
        Error::Invalid(_) => Status::invalid_argument("invalid derived query input"),
        Error::Corrupt => Status::data_loss("derived graph index requires explicit rebuild"),
        Error::Model(_) => Status::data_loss("invalid canonical graph metadata"),
        _ => Status::unavailable("derived graph storage unavailable"),
    }
}
pub(crate) struct Scoped {
    pub scope: Arc<Scope>,
    pub wire: q::QueryScope,
    roots: Vec<tinytree_store::RootReadGuard>,
    memory: Option<OwnedSemaphorePermit>,
    cleanup: std::sync::mpsc::Sender<Cleanup>,
}
struct Cleanup {
    roots: Vec<tinytree_store::RootReadGuard>,
    scope: Arc<Scope>,
    memory: Option<OwnedSemaphorePermit>,
}
impl Drop for Scoped {
    fn drop(&mut self) {
        let cleanup = Cleanup {
            roots: std::mem::take(&mut self.roots),
            scope: self.scope.clone(),
            memory: self.memory.take(),
        };
        // The cleanup receiver lives on a blocking owner thread until every
        // sender disappears. Its queue is charged by each scope's held permit.
        if let Err(error) = self.cleanup.send(cleanup) {
            // A panicked cleanup owner must never block an async worker behind
            // Store GC. Preserve the owners until a fallback blocking drop.
            let _ = std::thread::Builder::new()
                .name("tinytree-query-cleanup-fallback".into())
                .spawn(move || drop(error.0));
        }
    }
}
struct Record {
    owner: String,
    repository: String,
    replay: [u8; 32],
    digest: [u8; 32],
    expires: i64,
    scope: Option<Arc<Scoped>>,
}
#[derive(Default)]
struct Registry {
    records: HashMap<[u8; 32], Record>,
}
struct WarmJob {
    repository: String,
    root: m::CommitId,
    _pin: tinytree_store::RootReadGuard,
}
#[derive(Default)]
struct Warming {
    pending: HashSet<(String, m::CommitId)>,
    failures: HashMap<(String, m::CommitId), (Status, std::time::Instant)>,
}
struct WarmRuntime {
    index: Arc<Index>,
    store: tinytree_store::Store,
    config: QueryConfig,
}
#[cfg(test)]
pub(crate) struct QueryPause {
    pub entered: std::sync::mpsc::SyncSender<()>,
    pub resume: std::sync::mpsc::Receiver<()>,
}
pub(crate) struct QueryHost {
    pub index: Arc<Index>,
    pub config: QueryConfig,
    pub store: tinytree_store::Store,
    registry: Mutex<Registry>,
    scope_memory: Arc<Semaphore>,
    inflight: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    request_memory: usize,
    warm: std::sync::mpsc::SyncSender<WarmJob>,
    warming: Arc<Mutex<Warming>>,
    cleanup: std::sync::mpsc::Sender<Cleanup>,
    #[cfg(test)]
    pub pause: Mutex<Option<QueryPause>>,
}
pub(crate) struct Work {
    _host: Arc<QueryHost>,
    pub deadline: tokio::time::Instant,
    _inflight: OwnedSemaphorePermit,
    _memory: OwnedSemaphorePermit,
    pub scopes: Mutex<Vec<Arc<Scoped>>>,
}
impl Work {
    pub fn belongs_to(&self, host: &Arc<QueryHost>) -> bool {
        Arc::ptr_eq(&self._host, host)
    }
}
impl QueryHost {
    pub fn new(
        index: Arc<Index>,
        store: tinytree_store::Store,
        service: &ServiceConfig,
    ) -> Result<Arc<Self>, ConfigError> {
        let config = service.query.clone();
        config.validate(service.max_request_bytes)?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| ConfigError("query host requires an active runtime"))?;
        let (sender, receiver) = std::sync::mpsc::sync_channel::<WarmJob>(config.max_pending_warms);
        let receiver = Arc::new(Mutex::new(receiver));
        let warming = Arc::new(Mutex::new(Warming::default()));
        let (cleanup, cleanups) = std::sync::mpsc::channel::<Cleanup>();
        std::thread::Builder::new()
            .name("tinytree-query-cleanup".into())
            .spawn(move || {
                while let Ok(batch) = cleanups.recv() {
                    // Root destruction can wait behind GC. Quota ownership remains
                    // charged until roots and the derived scope physically drop.
                    drop(batch.roots);
                    drop(batch.scope);
                    drop(batch.memory);
                }
            })
            .map_err(|_| ConfigError("cannot start query cleanup owner"))?;
        for _ in 0..config.max_warm_jobs {
            let receiver = receiver.clone();
            let warming = warming.clone();
            let engine = WarmRuntime {
                index: index.clone(),
                store: store.clone(),
                config: config.clone(),
            };
            std::thread::Builder::new()
                .name("tinytree-query-warmer".into())
                .spawn(move || {
                    loop {
                        let job = match receiver.lock().unwrap().recv() {
                            Ok(job) => job,
                            Err(_) => break,
                        };
                        let key = (job.repository.clone(), job.root);
                        let result = engine.build(&job);
                        drop(job);
                        let mut state = warming.lock().unwrap();
                        state.pending.remove(&key);
                        match result {
                            Ok(()) => {
                                state.failures.remove(&key);
                            }
                            Err(error) => {
                                if state.failures.len() >= engine.config.max_pending_warms {
                                    if let Some(oldest) = state
                                        .failures
                                        .iter()
                                        .min_by_key(|(_, (_, when))| *when)
                                        .map(|(key, _)| key.clone())
                                    {
                                        state.failures.remove(&oldest);
                                    }
                                }
                                state
                                    .failures
                                    .insert(key, (error, std::time::Instant::now()));
                            }
                        }
                    }
                })
                .map_err(|_| ConfigError("cannot start derived query warmer"))?;
        }
        let host = Arc::new(Self {
            index,
            store,
            request_memory: config.request_memory(service.max_request_bytes),
            scope_memory: Arc::new(Semaphore::new(config.max_scope_memory_bytes)),
            inflight: Arc::new(Semaphore::new(config.max_inflight)),
            memory: Arc::new(Semaphore::new(config.max_request_memory_bytes)),
            config,
            registry: Mutex::new(Registry::default()),
            warm: sender,
            warming,
            cleanup,
            #[cfg(test)]
            pause: Mutex::new(None),
        });
        let weak = Arc::downgrade(&host);
        let interval = host.config.expiry_interval_ms;
        runtime.spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_millis(interval));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                let Some(host) = weak.upgrade() else { break };
                // Root guard destruction may wait behind Store GC. Never do it
                // on an async worker, and await each bounded cleanup batch.
                let _ = tokio::task::spawn_blocking(move || {
                    if let Ok(now) = now() {
                        host.expire(now);
                    }
                })
                .await;
            }
        });
        Ok(host)
    }
    #[cfg(test)]
    pub fn pause_query(&self) -> Result<(), Status> {
        let pause = self.pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause
                .entered
                .send(())
                .map_err(|_| Status::internal("query test observer stopped"))?;
            pause
                .resume
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| Status::deadline_exceeded("query test interleaving expired"))?;
        }
        Ok(())
    }
    pub async fn admit(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<Work>, Status> {
        let permit = tokio::time::timeout_at(deadline, self.inflight.clone().acquire_owned())
            .await
            .map_err(|_| Status::deadline_exceeded("query admission expired"))?
            .map_err(|_| Status::unavailable("query host draining"))?;
        let memory = tokio::time::timeout_at(
            deadline,
            self.memory
                .clone()
                .acquire_many_owned(self.request_memory as u32),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("query admission expired"))?
        .map_err(|_| Status::unavailable("query host draining"))?;
        Ok(Arc::new(Work {
            _host: self.clone(),
            deadline,
            _inflight: permit,
            _memory: memory,
            scopes: Mutex::new(Vec::new()),
        }))
    }
    fn expire(&self, now: i64) {
        let removed = {
            let mut registry = self.registry.lock().unwrap();
            let expired: Vec<_> = registry
                .records
                .iter()
                .filter(|(_, record)| record.expires <= now)
                .map(|(id, _)| *id)
                .collect();
            expired
                .into_iter()
                .filter_map(|id| registry.records.remove(&id))
                .collect::<Vec<_>>()
        };
        drop(removed);
    }
    pub fn lookup(
        &self,
        owner: &str,
        repository: &str,
        handle: &[u8],
    ) -> Result<Arc<Scoped>, Status> {
        let id: [u8; 32] = handle
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid scope handle"))?;
        let registry = self.registry.lock().unwrap();
        let record = registry
            .records
            .get(&id)
            .ok_or_else(|| Status::failed_precondition("query scope unavailable or expired"))?;
        if record.owner != owner || record.repository != repository {
            return Err(Status::permission_denied("query scope access denied"));
        }
        if record.expires <= now()? {
            return Err(Status::failed_precondition("query scope expired"));
        }
        record
            .scope
            .clone()
            .ok_or_else(|| Status::failed_precondition("query scope is closed"))
    }
    pub fn close(&self, owner: &str, repository: &str, handle: &[u8]) -> Result<(), Status> {
        let id: [u8; 32] = handle
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid scope handle"))?;
        let scope = {
            let mut registry = self.registry.lock().unwrap();
            let Some(record) = registry.records.get_mut(&id) else {
                return Ok(());
            };
            if record.owner != owner || record.repository != repository {
                return Err(Status::permission_denied("query scope access denied"));
            }
            record.scope.take()
        };
        drop(scope);
        Ok(())
    }
    pub fn open(
        &self,
        owner: &str,
        repository: &str,
        replay: [u8; 32],
        roots: Vec<m::CommitId>,
    ) -> Result<q::QueryScope, Status> {
        self.store
            .references(repository, &[])
            .map_err(|_| Status::not_found("native repository unavailable"))?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-root-query-scope-v1\0");
        hash.update(repository.as_bytes());
        hash.update(&self.index.epoch());
        for root in &roots {
            hash.update(&root.0);
        }
        let digest = *hash.finalize().as_bytes();
        let now = now()?;
        self.expire(now);
        {
            let registry = self.registry.lock().unwrap();
            if let Some(record) = registry.records.values().find(|record| {
                record.owner == owner && record.repository == repository && record.replay == replay
            }) {
                if record.digest != digest {
                    return Err(Status::already_exists(
                        "scope replay key reused with different content",
                    ));
                }
                return record
                    .scope
                    .as_ref()
                    .map(|scope| scope.wire.clone())
                    .ok_or_else(|| Status::failed_precondition("scope replay is closed"));
            }
        }
        let mut pins = Vec::new();
        for root in &roots {
            if *root != m::CommitId::ROOT {
                pins.push((
                    *root,
                    self.store
                        .pin_root(
                            repository,
                            m::ObjectKey {
                                kind: m::Kind::Commit,
                                id: m::ObjectId(root.0),
                            },
                        )
                        .map_err(|_| Status::not_found("query root unavailable"))?,
                ));
            }
        }
        let scope = match self.index.scope(repository, &roots) {
            Ok(scope) => scope,
            Err(tinytree_index::Error::NotReady) => {
                for (root, pin) in pins {
                    self.schedule(repository, root, pin)?;
                }
                return Err(Status::unavailable(
                    "derived graph warming; retry the same logical roots",
                ));
            }
            Err(error) => return Err(index_error(error)),
        };
        let charge = roots
            .len()
            .saturating_mul(
                4 * std::mem::size_of::<tinytree_store::RootReadGuard>()
                    + 4 * std::mem::size_of::<m::CommitId>()
                    + 128,
            )
            .saturating_add((owner.len() + repository.len()).saturating_mul(4))
            .saturating_add(2048);
        let memory = self
            .scope_memory
            .clone()
            .try_acquire_many_owned(
                u32::try_from(charge)
                    .map_err(|_| Status::resource_exhausted("scope memory exceeded"))?,
            )
            .map_err(|_| Status::resource_exhausted("scope memory exceeded"))?;
        let mut registry = self.registry.lock().unwrap();
        if let Some(record) = registry.records.values().find(|record| {
            record.owner == owner && record.repository == repository && record.replay == replay
        }) {
            if record.digest != digest {
                return Err(Status::already_exists(
                    "scope replay key reused with different content",
                ));
            }
            return record
                .scope
                .as_ref()
                .map(|scope| scope.wire.clone())
                .ok_or_else(|| Status::failed_precondition("scope replay is closed"));
        }
        if registry.records.len() >= self.config.max_scopes
            || registry
                .records
                .values()
                .filter(|record| record.owner == owner)
                .count()
                >= self.config.max_scopes_per_principal
            || registry
                .records
                .values()
                .filter(|record| record.repository == repository)
                .count()
                >= self.config.max_scopes_per_repository
        {
            return Err(Status::resource_exhausted("query scope quota exceeded"));
        }
        let mut handle = [0; 32];
        handle[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        handle[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        let expires = now
            .checked_add(self.config.scope_ttl_seconds as i64)
            .ok_or_else(|| Status::internal("scope expiry overflow"))?;
        let wire = q::QueryScope {
            key: Some(q::ScopeKey {
                repository_id: repository.into(),
                handle: handle.to_vec(),
            }),
            content_digest: digest.to_vec(),
            index_epoch: scope.epoch().to_vec(),
            high_water: scope.high_water(),
            expires_at_unix_seconds: expires,
        };
        let scoped = Arc::new(Scoped {
            scope: Arc::new(scope),
            wire: wire.clone(),
            roots: pins.into_iter().map(|(_, pin)| pin).collect(),
            memory: Some(memory),
            cleanup: self.cleanup.clone(),
        });
        registry.records.insert(
            handle,
            Record {
                owner: owner.into(),
                repository: repository.into(),
                replay,
                digest,
                expires,
                scope: Some(scoped),
            },
        );
        Ok(wire)
    }
    fn schedule(
        &self,
        repository: &str,
        root: m::CommitId,
        pin: tinytree_store::RootReadGuard,
    ) -> Result<(), Status> {
        if self
            .index
            .node(repository, root)
            .map_err(index_error)?
            .is_some()
        {
            return Ok(());
        }
        let key = (repository.to_owned(), root);
        let mut state = self.warming.lock().unwrap();
        if let Some((failure, when)) = state.failures.get(&key) {
            if when.elapsed() < Duration::from_secs(1) {
                return Err(failure.clone());
            }
        }
        state.failures.remove(&key);
        if state.pending.contains(&key) {
            return Ok(());
        }
        if state.pending.len() >= self.config.max_pending_warms {
            return Err(Status::resource_exhausted("derived warm queue full"));
        }
        state.pending.insert(key.clone());
        if self
            .warm
            .try_send(WarmJob {
                repository: repository.into(),
                root,
                _pin: pin,
            })
            .is_err()
        {
            state.pending.remove(&key);
            return Err(Status::resource_exhausted("derived warm queue full"));
        }
        Ok(())
    }
}
impl WarmRuntime {
    fn build(&self, job: &WarmJob) -> Result<(), Status> {
        let deadline =
            std::time::Instant::now() + Duration::from_secs(self.config.warm_timeout_seconds);
        let mut stack = vec![(job.root, false)];
        let mut active = HashSet::new();
        let mut done = HashSet::new();
        let mut nodes: HashMap<m::CommitId, (m::Descriptor, Vec<u8>)> = HashMap::new();
        let mut bytes = 0usize;
        let mut edges = 0usize;
        while let Some((id, exit)) = stack.pop() {
            if std::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded("derived warm work expired"));
            }
            if id == m::CommitId::ROOT || done.contains(&id) {
                continue;
            }
            if exit {
                let (descriptor, data) = nodes
                    .remove(&id)
                    .ok_or_else(|| Status::data_loss("derived warm node missing"))?;
                self.index
                    .ingest(&job.repository, descriptor, &data)
                    .map_err(|error| match error {
                        tinytree_index::Error::Invalid(reason)
                            if reason.contains("budget")
                                || reason.contains("size")
                                || reason.contains("count") =>
                        {
                            Status::resource_exhausted("derived index capacity exceeded")
                        }
                        error => index_error(error),
                    })?;
                active.remove(&id);
                done.insert(id);
                continue;
            }
            if active.len().saturating_add(done.len()).saturating_add(1)
                > self.config.max_warm_nodes
            {
                return Err(Status::resource_exhausted(
                    "derived warm node budget exceeded",
                ));
            }
            if self
                .index
                .node(&job.repository, id)
                .map_err(index_error)?
                .is_some()
            {
                done.insert(id);
                continue;
            }
            if !active.insert(id) {
                return Err(Status::data_loss("canonical graph cycle"));
            }
            if active.len().saturating_add(done.len()) > self.config.max_warm_nodes {
                return Err(Status::resource_exhausted(
                    "derived warm node budget exceeded",
                ));
            }
            let key = m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(id.0),
            };
            let descriptor = self
                .store
                .descriptor(&job.repository, key)
                .map_err(|_| Status::unavailable("canonical graph unavailable"))?
                .ok_or_else(|| Status::not_found("canonical graph incomplete"))?;
            if descriptor.size_bytes > self.config.max_commit_bytes as u64 {
                return Err(Status::resource_exhausted(
                    "derived commit byte limit exceeded",
                ));
            }
            let (descriptor, data) = self
                .store
                .read_metadata(&job.repository, key)
                .map_err(|_| Status::data_loss("canonical graph read failed"))?;
            bytes = bytes
                .checked_add(data.len())
                .ok_or_else(|| Status::resource_exhausted("derived warm byte overflow"))?;
            if bytes > self.config.max_warm_bytes {
                return Err(Status::resource_exhausted(
                    "derived warm byte budget exceeded",
                ));
            }
            let object = m::decode(
                &data,
                m::Limits {
                    max_object_bytes: self.config.max_commit_bytes,
                    max_items: self.config.max_commit_parents,
                    ..Default::default()
                },
            )
            .map_err(|error| match error {
                m::Error::Limit => {
                    Status::resource_exhausted("derived commit shape exceeds configured limits")
                }
                _ => Status::data_loss("invalid canonical graph metadata"),
            })?;
            let m::Object::Commit(commit) = object else {
                return Err(Status::data_loss("canonical graph type mismatch"));
            };
            edges = edges
                .checked_add(commit.parents.len())
                .ok_or_else(|| Status::resource_exhausted("derived warm edge overflow"))?;
            if edges > self.config.max_warm_edges
                || stack.len().saturating_add(commit.parents.len() + 1)
                    > self.config.max_warm_frontier
            {
                return Err(Status::resource_exhausted(
                    "derived warm frontier budget exceeded",
                ));
            }
            stack.push((id, true));
            for parent in commit.parents.into_iter().rev() {
                stack.push((parent, false));
            }
            nodes.insert(id, (descriptor, data));
        }
        Ok(())
    }
}
