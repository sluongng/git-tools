//! Revision-pinned content search. Immutable manifests and verified per-FILE
//! Bloom filters are rebuildable accelerators, never canonical read authority.
use crate::{
    config::ConfigError,
    query_history::{SearchChild, SearchRead, SearchTree, search_root},
    query_index::{QueryConfig, QueryHost, Scoped},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tinytree_index::{Index, WorkBudget, content::ContentEntry};
use tinytree_model as m;
use tinytree_proto::query_v1 as q;
use tinytree_search::{FilterBuilder, MatchBudget, MatchLimits, Pattern, SearchMode};
use tokio::{sync::Semaphore, time::Instant};
use tonic::{Code, Status};

/// Local diagnostic counters, not a network API or a retention guarantee.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct SearchStats {
    pub warm_jobs_completed: u64,
    pub warm_jobs_failed: u64,
    pub warm_files_built: u64,
    pub warm_files_reused: u64,
    /// Logical canonical FILE payloads opened. Store verification and packed
    /// reconstruction can perform additional IO; this is not physical disk IO.
    pub warm_file_bytes: u64,
}
#[derive(Default)]
struct Counters {
    completed: AtomicU64,
    failed: AtomicU64,
    built: AtomicU64,
    reused: AtomicU64,
    bytes: AtomicU64,
}
impl Counters {
    fn snapshot(&self) -> SearchStats {
        SearchStats {
            warm_jobs_completed: self.completed.load(Ordering::Relaxed),
            warm_jobs_failed: self.failed.load(Ordering::Relaxed),
            warm_files_built: self.built.load(Ordering::Relaxed),
            warm_files_reused: self.reused.load(Ordering::Relaxed),
            warm_file_bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}
type Key = (String, m::TreeId);
#[cfg(test)]
struct Pause {
    entered: std::sync::mpsc::SyncSender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}
struct Job {
    repository: String,
    scope: Arc<Scoped>,
    root: SearchTree,
    deadline: Instant,
}
#[derive(Default)]
struct Warming {
    pending: HashSet<Key>,
    failed: HashMap<Key, (Code, std::time::Instant)>,
}
struct Engine {
    index: Arc<Index>,
    store: tinytree_store::Store,
    config: QueryConfig,
    closing: Arc<AtomicBool>,
    memory: Arc<Semaphore>,
    counters: Arc<Counters>,
    #[cfg(test)]
    pause: Arc<Mutex<Option<Pause>>>,
}
pub(crate) struct SearchHost {
    sender: Mutex<Option<std::sync::mpsc::SyncSender<Job>>>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    state: Arc<Mutex<Warming>>,
    key: [u8; 32],
    counters: Arc<Counters>,
    config: QueryConfig,
    closing: Arc<AtomicBool>,
    #[cfg(test)]
    pause: Arc<Mutex<Option<Pause>>>,
}
impl SearchHost {
    pub fn new(
        index: Arc<Index>,
        store: tinytree_store::Store,
        config: QueryConfig,
        closing: Arc<AtomicBool>,
    ) -> Result<Arc<Self>, ConfigError> {
        let (sender, receiver) =
            std::sync::mpsc::sync_channel::<Job>(config.max_pending_search_warms);
        let state = Arc::new(Mutex::new(Warming::default()));
        let states = state.clone();
        let counters = Arc::new(Counters::default());
        #[cfg(test)]
        let pause = Arc::new(Mutex::new(None));
        let engine = Engine {
            index,
            store,
            counters: counters.clone(),
            memory: Arc::new(Semaphore::new(config.max_search_warm_memory_bytes)),
            config: config.clone(),
            closing: closing.clone(),
            #[cfg(test)]
            pause: pause.clone(),
        };
        let worker = std::thread::Builder::new()
            .name("tinytree-content-warmer".into())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    let key = (job.repository.clone(), job.root.id());
                    let result = engine.build(&job);
                    // Root destruction may wait behind GC. The worker, not an HTTP
                    // executor or cancelled request, physically owns that cleanup.
                    drop(job);
                    let mut state = states.lock().unwrap();
                    state.pending.remove(&key);
                    if let Err(error) = result {
                        engine.counters.failed.fetch_add(1, Ordering::Relaxed);
                        if state.failed.len() >= engine.config.max_pending_search_warms {
                            if let Some(old) = state
                                .failed
                                .iter()
                                .min_by_key(|(_, (_, time))| *time)
                                .map(|(key, _)| key.clone())
                            {
                                state.failed.remove(&old);
                            }
                        }
                        state
                            .failed
                            .insert(key, (error.code(), std::time::Instant::now()));
                    } else {
                        engine.counters.completed.fetch_add(1, Ordering::Relaxed);
                        state.failed.remove(&key);
                    }
                }
            })
            .map_err(|_| ConfigError("cannot start search warmer"))?;
        let mut key = [0; 32];
        key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        Ok(Arc::new(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
            state,
            key,
            counters,
            config,
            closing,
            #[cfg(test)]
            pause,
        }))
    }
    pub fn stats(&self) -> SearchStats {
        self.counters.snapshot()
    }
    pub async fn drain(&self) -> bool {
        self.sender.lock().unwrap().take();
        let worker = self.worker.lock().unwrap().take();
        match worker {
            Some(worker) => tokio::task::spawn_blocking(move || worker.join().is_ok())
                .await
                .unwrap_or(false),
            None => true,
        }
    }
    fn schedule(
        &self,
        repository: &str,
        scope: Arc<Scoped>,
        root: SearchTree,
    ) -> Result<(), Status> {
        if self.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("search host draining"));
        }
        let key = (repository.to_owned(), root.id());
        let mut state = self.state.lock().unwrap();
        if state.pending.contains(&key) {
            return Ok(());
        }
        if let Some((code, at)) = state.failed.get(&key) {
            if at.elapsed() < Duration::from_secs(1) {
                return Err(Status::new(*code, "search index warming did not complete"));
            }
        }
        if state.pending.len() >= self.config.max_pending_search_warms {
            return Err(Status::resource_exhausted("search warm queue full"));
        }
        let now = unix()?;
        let seconds = u64::try_from(scope.wire.expires_at_unix_seconds.saturating_sub(now))
            .unwrap_or(0)
            .min(self.config.search_warm_timeout_seconds);
        if seconds == 0 {
            return Err(Status::failed_precondition("search scope expired"));
        }
        let job = Job {
            repository: repository.into(),
            scope,
            root,
            deadline: Instant::now() + Duration::from_secs(seconds),
        };
        let sender = self.sender.lock().unwrap();
        let sender = sender
            .as_ref()
            .ok_or_else(|| Status::unavailable("search host draining"))?;
        sender
            .try_send(job)
            .map_err(|_| Status::resource_exhausted("search warm queue full"))?;
        state.pending.insert(key);
        Ok(())
    }
}
impl Drop for SearchHost {
    fn drop(&mut self) {
        self.sender.get_mut().unwrap().take();
    }
}
impl Engine {
    fn check(&self, job: &Job) -> Result<(), Status> {
        if self.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("search host draining"));
        }
        if Instant::now() >= job.deadline || unix()? >= job.scope.wire.expires_at_unix_seconds {
            return Err(Status::deadline_exceeded("search warm deadline exceeded"));
        }
        Ok(())
    }
    fn build(&self, job: &Job) -> Result<(), Status> {
        // One worker and one complete reservation. Never retain request Work or
        // upgrade a request reservation while waiting for background memory.
        let required = self.config.search_warm_memory();
        if required > self.config.max_search_warm_memory_bytes {
            return Err(Status::resource_exhausted(
                "search warm working set exceeds configured pool",
            ));
        }
        let _memory = self
            .memory
            .clone()
            .try_acquire_many_owned(required as u32)
            .map_err(|_| Status::resource_exhausted("search warm memory unavailable"))?;
        self.check(job)?;
        #[cfg(test)]
        {
            let pause = self.pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause
                    .entered
                    .send(())
                    .map_err(|_| Status::internal("search observer stopped"))?;
                pause
                    .resume
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|_| Status::deadline_exceeded("search observer expired"))?;
            }
        }
        let limits = self.config.search_content_limits();
        let read = SearchRead {
            index: &self.index,
            store: &self.store,
            metadata_max: self.config.max_history_object_bytes,
        };
        let mut budget = WorkBudget::new(
            self.config.max_search_entries as u64 * 4 + 16,
            self.config.max_search_entries as u64 * 4 + 16,
            self.config.max_search_warm_read_bytes,
            self.config.max_search_entries,
        );
        let mut pending = vec![(String::new(), job.root.clone(), 0usize)];
        let mut records = Vec::new();
        let mut path_bytes = 0usize;
        let mut visited = 0usize;
        while let Some((prefix, tree, depth)) = pending.pop() {
            self.check(job)?;
            path_bytes = path_bytes.saturating_sub(prefix.len());
            if depth > self.config.max_search_path_depth {
                return Err(Status::resource_exhausted(
                    "search tree depth limit exceeded",
                ));
            }
            for child in tree.entries(
                &read,
                &job.scope,
                &job.repository,
                &mut budget,
                job.deadline,
            )? {
                self.check(job)?;
                visited = visited
                    .checked_add(1)
                    .ok_or_else(|| Status::resource_exhausted("search entry count overflow"))?;
                if visited > self.config.max_search_entries {
                    return Err(Status::resource_exhausted(
                        "search manifest entry limit exceeded",
                    ));
                }
                let size = prefix
                    .len()
                    .saturating_add(child.name.len())
                    .saturating_add(usize::from(!prefix.is_empty()));
                if size > 4096
                    || path_bytes.saturating_add(size) > self.config.max_search_manifest_bytes
                {
                    return Err(Status::resource_exhausted(
                        "search manifest path bytes exceeded",
                    ));
                }
                let path = if prefix.is_empty() {
                    child.name
                } else {
                    format!("{prefix}/{}", child.name)
                };
                match child.child {
                    SearchChild::Tree(tree) => {
                        path_bytes += path.len();
                        pending.push((path, tree, depth + 1));
                    }
                    SearchChild::File(file) => {
                        path_bytes += path.len();
                        records.push((
                            ContentEntry {
                                path,
                                file: file.id(),
                                size: file.size(),
                            },
                            file,
                        ));
                    }
                    SearchChild::Other => {}
                }
            }
        }
        records.sort_by(|a, b| a.0.path.cmp(&b.0.path));
        let (entries, files): (Vec<_>, Vec<_>) = records.into_iter().unzip();
        // An impossible cache policy is terminal before downloading any FILE.
        // Otherwise FIFO churn could endlessly evict the first filter while
        // trying to prepare a manifest that can never fit its own dependencies.
        limits
            .check_manifest(job.root.id(), &entries)
            .map_err(index_storage)?;
        for file in files {
            self.check(job)?;
            if self
                .index
                .content_file(&job.repository, file.id(), file.size(), &limits)
                .map_err(index_storage)?
                .is_none()
            {
                let bytes = file.read(
                    &read,
                    &job.scope,
                    &job.repository,
                    self.config.max_search_warm_file_bytes,
                    &mut budget,
                    job.deadline,
                )?;
                self.counters
                    .bytes
                    .fetch_add(file.size(), Ordering::Relaxed);
                let mut filter = FilterBuilder::new();
                filter
                    .update_checked(&bytes, || self.check(job).is_ok())
                    .map_err(|_| Status::deadline_exceeded("search filter build cancelled"))?;
                self.index
                    .put_content_file(
                        &job.repository,
                        file.id(),
                        file.size(),
                        filter.finish(),
                        &limits,
                    )
                    .map_err(index_storage)?;
                self.counters.built.fetch_add(1, Ordering::Relaxed);
            } else {
                self.counters.reused.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.check(job)?;
        self.index
            .put_content_manifest(&job.repository, job.root.id(), &entries, &limits)
            .map_err(index_storage)?;
        Ok(())
    }
}
fn index_storage(error: tinytree_index::Error) -> Status {
    match error {
        tinytree_index::Error::BudgetExceeded => {
            Status::resource_exhausted("search derived cache limit exceeded")
        }
        _ => Status::unavailable("search derived cache unavailable"),
    }
}
fn unix() -> Result<i64, Status> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("invalid system clock"))?
            .as_secs(),
    )
    .map_err(|_| Status::internal("invalid system clock"))
}
fn commit(value: Option<&q::CommitId>) -> Result<m::CommitId, Status> {
    Ok(m::CommitId(
        value
            .ok_or_else(|| Status::invalid_argument("search revision required"))?
            .value
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid search revision"))?,
    ))
}
fn wire(id: m::CommitId) -> q::CommitId {
    q::CommitId {
        value: id.0.to_vec(),
    }
}
fn valid_prefix(prefix: &str) -> bool {
    prefix.is_empty()
        || (prefix.len() <= 4096
            && !prefix.contains(['\\', '\0'])
            && prefix
                .split('/')
                .all(|part| !matches!(part, "" | "." | "..")))
}
fn prefix_matches(path: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

#[derive(Clone, Copy)]
struct Position {
    index: u64,
    offset: u64,
    entries: u64,
    bytes: u64,
    work: u64,
    expiry: i64,
}
const TOKEN_BYTES: usize = 1 + 6 * 8 + 32 + 32 + 32;
impl SearchHost {
    fn binding(
        &self,
        owner: &str,
        repository: &str,
        scope: &Scoped,
        input: &q::SearchRequest,
        root: m::TreeId,
        manifest: &[u8; 32],
        page_size: u32,
        expiry: i64,
    ) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-content-search-cursor-v1\0");
        let revision = input
            .revision
            .as_ref()
            .map_or(&[][..], |id| id.value.as_slice());
        for part in [
            owner.as_bytes(),
            repository.as_bytes(),
            scope
                .wire
                .key
                .as_ref()
                .map_or(&[][..], |key| key.handle.as_slice()),
            scope.wire.content_digest.as_slice(),
            scope.wire.index_epoch.as_slice(),
            revision,
            root.0.as_slice(),
            manifest.as_slice(),
            input.pattern.as_slice(),
            input.path_prefix.as_bytes(),
        ] {
            hash.update(&(part.len() as u64).to_be_bytes());
            hash.update(part);
        }
        hash.update(&input.policy.to_be_bytes());
        hash.update(&page_size.to_be_bytes());
        hash.update(&expiry.to_be_bytes());
        *hash.finalize().as_bytes()
    }
    fn token(&self, position: Position, manifest: [u8; 32], binding: [u8; 32]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(TOKEN_BYTES);
        bytes.push(1);
        for value in [
            position.index,
            position.offset,
            position.entries,
            position.bytes,
            position.work,
            position.expiry as u64,
        ] {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        bytes.extend_from_slice(&manifest);
        bytes.extend_from_slice(&binding);
        let mac = blake3::keyed_hash(&self.key, &bytes);
        bytes.extend_from_slice(mac.as_bytes());
        bytes
    }
    fn position(&self, token: &[u8]) -> Result<(Position, [u8; 32], [u8; 32]), Status> {
        if token.len() != TOKEN_BYTES || token[0] != 1 {
            return Err(Status::invalid_argument("invalid search cursor"));
        }
        let (payload, mac) = token.split_at(TOKEN_BYTES - 32);
        let expected = blake3::keyed_hash(&self.key, payload);
        if mac
            .iter()
            .zip(expected.as_bytes())
            .fold(0u8, |value, (a, b)| value | (a ^ b))
            != 0
        {
            return Err(Status::invalid_argument("invalid search cursor"));
        }
        let mut values = [0u64; 6];
        for (i, value) in values.iter_mut().enumerate() {
            *value = u64::from_be_bytes(token[1 + i * 8..9 + i * 8].try_into().unwrap());
        }
        let position = Position {
            index: values[0],
            offset: values[1],
            entries: values[2],
            bytes: values[3],
            work: values[4],
            expiry: values[5] as i64,
        };
        if unix()? >= position.expiry {
            return Err(Status::failed_precondition("search cursor expired"));
        }
        Ok((
            position,
            token[49..81].try_into().unwrap(),
            token[81..113].try_into().unwrap(),
        ))
    }
    pub fn query(
        &self,
        host: &QueryHost,
        scope: Arc<Scoped>,
        repository: &str,
        owner: &str,
        input: q::SearchRequest,
        budget: &mut WorkBudget,
        deadline: Instant,
    ) -> Result<q::SearchResult, Status> {
        let mode = match q::SearchPolicy::try_from(input.policy) {
            Ok(q::SearchPolicy::LiteralBytesV1) => SearchMode::Literal,
            Ok(q::SearchPolicy::RegexBytesV1) => SearchMode::Regex,
            _ => return Err(Status::unimplemented("search policy unsupported")),
        };
        let pattern = Pattern::new(&input.pattern, mode)
            .map_err(|_| Status::invalid_argument("invalid or over-budget search pattern"))?;
        if !valid_prefix(&input.path_prefix) {
            return Err(Status::invalid_argument("invalid search path prefix"));
        }
        let page_size = if input.page_size == 0 {
            host.config.max_page_size as u32
        } else {
            input.page_size
        };
        if page_size == 0 || page_size as usize > host.config.max_page_size {
            return Err(Status::invalid_argument("search page size exceeds limit"));
        }
        let revision = commit(input.revision.as_ref())?;
        let mut result = q::SearchResult {
            scope_digest: scope.wire.content_digest.clone(),
            index_epoch: scope.wire.index_epoch.clone(),
            revision: Some(wire(revision)),
            root_tree: vec![],
            manifest_digest: vec![],
            pattern: input.pattern.clone(),
            path_prefix: input.path_prefix.clone(),
            policy: input.policy,
            coverage: q::SearchCoverage::SearchWarming as i32,
            hits: vec![],
            cursor: vec![],
            entries_examined: 0,
            file_bytes_read: 0,
            page_size,
        };
        let root = match search_root(host, &scope, repository, revision, budget, deadline) {
            Ok(root) => root,
            Err(error) => {
                result.coverage = match error.code() {
                    Code::Unimplemented => q::SearchCoverage::SearchPrivateOverlay,
                    Code::FailedPrecondition => q::SearchCoverage::SearchConflict,
                    Code::ResourceExhausted => q::SearchCoverage::SearchReadLimit,
                    _ => return Err(error),
                } as i32;
                return Ok(result);
            }
        };
        result.root_tree = root.id().0.to_vec();
        let read = SearchRead {
            index: &host.index,
            store: &host.store,
            metadata_max: host.config.max_history_object_bytes,
        };
        let limits = host.config.search_content_limits();
        let resumed = if input.cursor.is_empty() {
            None
        } else {
            let (position, manifest, binding) = self.position(&input.cursor)?;
            if self.binding(
                owner,
                repository,
                &scope,
                &input,
                root.id(),
                &manifest,
                page_size,
                position.expiry,
            ) != binding
            {
                return Err(Status::invalid_argument(
                    "search cursor does not match immutable inputs",
                ));
            }
            Some((position, manifest))
        };
        let Some(manifest) = root.manifest(&read, &scope, repository, &limits)? else {
            return self.warming(repository, scope, root, result, !input.cursor.is_empty());
        };
        let value = manifest.value();
        let now = unix()?;
        let mut position = if let Some((position, digest)) = resumed {
            if digest != value.digest {
                return Err(Status::failed_precondition("search manifest changed"));
            }
            position
        } else {
            let expiry = scope
                .wire
                .expires_at_unix_seconds
                .min(now.saturating_add(host.config.query_ttl_seconds as i64));
            Position {
                index: 0,
                offset: 0,
                entries: 0,
                bytes: 0,
                work: 0,
                expiry,
            }
        };
        let binding = self.binding(
            owner,
            repository,
            &scope,
            &input,
            root.id(),
            &value.digest,
            page_size,
            position.expiry,
        );
        if position.index > value.entries.len() as u64
            || (position.index == value.entries.len() as u64 && position.offset != 0)
        {
            return Err(Status::invalid_argument("search cursor position mismatch"));
        }
        if position.expiry > scope.wire.expires_at_unix_seconds {
            return Err(Status::invalid_argument("search cursor expiry mismatch"));
        }
        result.manifest_digest = value.digest.to_vec();
        let mut page_entries = 0usize;
        let mut page_bytes = 0usize;
        let mut page_work = 0usize;
        let mut output_remaining = host.config.max_response_bytes / 2;
        result.coverage = q::SearchCoverage::SearchMore as i32;
        while (position.index as usize) < value.entries.len()
            && result.hits.len() < page_size as usize
        {
            if Instant::now() >= deadline {
                return Err(Status::deadline_exceeded("search deadline exceeded"));
            }
            if position.entries >= host.config.max_search_total_entries
                || position.work >= host.config.max_search_total_work
            {
                result.coverage = q::SearchCoverage::SearchWorkLimit as i32;
                break;
            }
            if page_entries >= host.config.max_search_entries_per_page {
                break;
            }
            let index = position.index as usize;
            let entry = &value.entries[index];
            // Charge only entries actually visited. Reserving every possible
            // entry first would starve the matcher even on one ordinary FILE.
            let entry_work = input
                .pattern
                .len()
                .saturating_mul(16)
                .saturating_add(entry.path.len().saturating_mul(2))
                .saturating_add(input.path_prefix.len().saturating_mul(2))
                .saturating_add(64);
            if position.work.saturating_add(entry_work as u64) > host.config.max_search_total_work {
                result.coverage = q::SearchCoverage::SearchWorkLimit as i32;
                break;
            }
            if page_work.saturating_add(entry_work) > host.config.max_search_match_work {
                if page_entries == 0 {
                    result.coverage = q::SearchCoverage::SearchWorkLimit as i32;
                }
                break;
            }
            page_work += entry_work;
            position.work += entry_work as u64;
            if position.offset > entry.size {
                return Err(Status::invalid_argument("search cursor offset mismatch"));
            }
            if !prefix_matches(&entry.path, &input.path_prefix) {
                position.entries += 1;
                page_entries += 1;
                position.index += 1;
                position.offset = 0;
                continue;
            }
            let Some(filter) = host
                .index
                .content_file(repository, entry.file, entry.size, &limits)
                .map_err(index_storage)?
            else {
                return self.warming(
                    repository,
                    scope,
                    root,
                    q::SearchResult {
                        hits: vec![],
                        manifest_digest: vec![],
                        entries_examined: 0,
                        file_bytes_read: 0,
                        ..result
                    },
                    !input.cursor.is_empty(),
                );
            };
            if !pattern.may_match(&filter.filter) {
                position.entries += 1;
                page_entries += 1;
                position.index += 1;
                position.offset = 0;
                continue;
            }
            if entry.size > host.config.max_search_file_bytes as u64
                || position.bytes.saturating_add(entry.size)
                    > host.config.max_search_total_read_bytes
            {
                result.coverage = q::SearchCoverage::SearchReadLimit as i32;
                break;
            }
            if page_bytes.saturating_add(entry.size as usize) > host.config.max_search_read_bytes {
                break;
            }
            let file = manifest
                .file(index)
                .ok_or_else(|| Status::data_loss("search manifest entry missing"))?;
            let bytes = file.read(
                &read,
                &scope,
                repository,
                host.config.max_search_file_bytes,
                budget,
                deadline,
            )?;
            position.entries += 1;
            page_entries += 1;
            position.bytes += entry.size;
            page_bytes += entry.size as usize;
            let mut matches = MatchBudget::new(MatchLimits {
                max_input_bytes: host
                    .config
                    .max_search_read_bytes
                    .saturating_sub(page_bytes.saturating_sub(entry.size as usize)),
                max_work: host
                    .config
                    .max_search_total_work
                    .saturating_sub(position.work)
                    .min(host.config.max_search_match_work.saturating_sub(page_work) as u64)
                    as usize,
                max_output_bytes: output_remaining,
            });
            let scanned = pattern.scan(
                &bytes,
                position.offset,
                page_size as usize - result.hits.len(),
                &mut matches,
                || Instant::now() < deadline,
            );
            let usage = matches.usage();
            position.work = position.work.saturating_add(usage.work as u64);
            page_work = page_work.saturating_add(usage.work);
            output_remaining = output_remaining.saturating_sub(usage.output_bytes);
            let page = match scanned {
                Ok(page) => page,
                Err(tinytree_search::Error::Cancelled) => {
                    return Err(Status::deadline_exceeded("search match deadline exceeded"));
                }
                Err(_) => {
                    result.coverage = q::SearchCoverage::SearchWorkLimit as i32;
                    break;
                }
            };
            let wire_charge = page
                .hits
                .len()
                .saturating_mul(entry.path.len().saturating_add(128));
            if wire_charge > output_remaining {
                result.coverage = q::SearchCoverage::SearchWorkLimit as i32;
                break;
            }
            output_remaining -= wire_charge;
            for hit in page.hits {
                result.hits.push(q::SearchHit {
                    path: entry.path.clone(),
                    file_id: entry.file.0.to_vec(),
                    file_size: entry.size,
                    byte_offset: hit.offset,
                    match_bytes: hit.length,
                    line: hit.line,
                    byte_column: hit
                        .column
                        .checked_sub(1)
                        .ok_or_else(|| Status::internal("search column underflow"))?,
                    snippet_offset: hit.snippet_offset,
                    snippet: hit.snippet,
                });
            }
            match page.next_offset {
                Some(offset) if offset < entry.size => position.offset = offset,
                _ => {
                    position.index += 1;
                    position.offset = 0;
                }
            }
        }
        result.entries_examined = position.entries;
        result.file_bytes_read = position.bytes;
        if position.index == value.entries.len() as u64 {
            result.coverage = q::SearchCoverage::SearchComplete as i32;
        }
        if result.coverage == q::SearchCoverage::SearchMore as i32 {
            result.cursor = self.token(position, value.digest, binding);
        }
        Ok(result)
    }
    fn warming(
        &self,
        repository: &str,
        scope: Arc<Scoped>,
        root: SearchTree,
        mut result: q::SearchResult,
        continuation: bool,
    ) -> Result<q::SearchResult, Status> {
        result.coverage = q::SearchCoverage::SearchWarming as i32;
        result.hits.clear();
        result.cursor.clear();
        result.manifest_digest.clear();
        result.entries_examined = 0;
        result.file_bytes_read = 0;
        if let Err(error) = self.schedule(repository, scope, root) {
            if error.code() == Code::ResourceExhausted {
                result.coverage = q::SearchCoverage::SearchIndexLimit as i32;
            } else {
                return Err(error);
            }
        }
        if continuation {
            // A successful continuation has immutable page identity. Cache
            // eviction is retryable unavailability, never a successful page
            // whose cumulative counters move backwards or whose hits disappear.
            return Err(Status::unavailable(
                "search index warming; retry the same continuation",
            ));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RepositoryService, ServiceConfig};
    use q::repository_query_server::RepositoryQuery;
    use tonic::Request;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn background_search_warm_owns_roots_not_http_admission_and_releases_them_after_completion()
     {
        let temporary = tempfile::tempdir().unwrap();
        let store =
            tinytree_store::Store::open(temporary.path().join("objects"), Default::default())
                .unwrap();
        store.create_repository("example").unwrap();
        let bytes = b"needle retained while warming";
        let file = m::describe_file(bytes);
        store.put("example", file, bytes, 0).unwrap();
        let tree = m::encode(
            &m::Object::Tree(m::Tree {
                entries: vec![m::TreeEntry {
                    name: "file".into(),
                    value: m::TreeValue::File {
                        id: m::FileId(file.key.id.0),
                        executable: false,
                        size_bytes: bytes.len() as u64,
                        copy_id: None,
                    },
                }],
            }),
            Default::default(),
        )
        .unwrap();
        store
            .put("example", tree.descriptor, &tree.bytes, 0)
            .unwrap();
        let mut commit = m::synthetic_root();
        commit.parents = vec![m::CommitId::ROOT];
        commit.change_id = m::ChangeId([71; 16]);
        commit.root_tree.terms = vec![m::TreeId(tree.descriptor.key.id.0)];
        let commit = m::encode(&m::Object::Commit(commit), Default::default()).unwrap();
        store
            .put("example", commit.descriptor, &commit.bytes, 0)
            .unwrap();
        let index = Index::open(temporary.path().join("index"), &Default::default()).unwrap();
        index
            .ingest("example", commit.descriptor, &commit.bytes)
            .unwrap();
        let mut config = ServiceConfig::default();
        config.query.max_inflight = 1;
        let repository =
            RepositoryService::new(store.clone(), config, vec!["example".into()], Vec::new())
                .unwrap()
                .with_query_index(index)
                .unwrap();
        let host = repository.query_host().unwrap();
        let opened = host.clone();
        let scope = tokio::task::spawn_blocking(move || {
            opened.open(
                "local-owner",
                "example",
                [72; 32],
                vec![m::CommitId(commit.descriptor.key.id.0)],
            )
        })
        .await
        .unwrap()
        .unwrap();
        let (entered, started) = std::sync::mpsc::sync_channel(1);
        let (resume, wait) = std::sync::mpsc::sync_channel(1);
        *host.search.pause.lock().unwrap() = Some(Pause {
            entered,
            resume: wait,
        });
        let service = repository.query_service();
        let warming = service
            .search(Request::new(q::SearchRequest {
                scope: scope.key.clone(),
                revision: Some(wire(m::CommitId(commit.descriptor.key.id.0))),
                pattern: b"needle".to_vec(),
                path_prefix: String::new(),
                policy: q::SearchPolicy::LiteralBytesV1 as i32,
                page_size: 1,
                cursor: vec![],
            }))
            .await
            .unwrap();
        assert_eq!(
            warming.get_ref().coverage,
            q::SearchCoverage::SearchWarming as i32
        );
        drop(warming);
        tokio::task::spawn_blocking(move || started.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        let mut facts = Request::new(q::QueryFactsRequest {
            scope: scope.key.clone(),
            facts: vec![],
        });
        facts.set_timeout(Duration::from_secs(1));
        service.query_facts(facts).await.unwrap();
        service
            .close_scope(Request::new(q::CloseScopeRequest { scope: scope.key }))
            .await
            .unwrap();
        let retained = store.clone();
        tokio::task::spawn_blocking(move || {
            retained.gc("example", i64::MAX - 1).unwrap();
            assert!(
                retained.descriptor("example", file.key).unwrap().is_some(),
                "accepted warmer retains native roots after logical scope close"
            );
        })
        .await
        .unwrap();
        resume.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let retained = store.clone();
                let exists = tokio::task::spawn_blocking(move || {
                    retained.gc("example", i64::MAX - 1).unwrap();
                    retained.descriptor("example", file.key).unwrap().is_some()
                })
                .await
                .unwrap();
                if !exists {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        host.drain().await.unwrap();
    }
}
