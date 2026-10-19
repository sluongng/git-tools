//! Bounded immutable content work. No worker can mutate the projected namespace.
use super::{BackgroundCancellation, BlobReader, GitRepo, canonical_oid};
use anyhow::{Result, ensure};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Independent bounds for callbacks, reply buffers, and anonymous cache files.
#[derive(Clone, Debug)]
pub struct ContentConfig {
    pub max_requests: usize,
    pub reply_bytes: u64,
    pub cache_bytes: u64,
    pub cache_entries: usize,
    pub max_prefetch: usize,
    pub fetch_batch: usize,
}
impl Default for ContentConfig {
    fn default() -> Self {
        Self {
            max_requests: 64,
            reply_bytes: 64 << 20,
            cache_bytes: 64 << 20,
            cache_entries: 256,
            max_prefetch: 256,
            fetch_batch: 32,
        }
    }
}

/// Fixed safe errors; never includes Git diagnostics or user-controlled paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentError {
    Invalid,
    Saturated,
    Cancelled,
    Io,
    Missing,
}
impl ContentError {
    pub fn errno(self) -> i32 {
        match self {
            Self::Invalid => libc::EINVAL,
            Self::Saturated => libc::EAGAIN,
            Self::Cancelled => libc::EINTR,
            Self::Io | Self::Missing => libc::EIO,
        }
    }
}
impl std::fmt::Display for ContentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ContentError {}

type Callback = Box<dyn FnOnce(std::result::Result<ContentData, ContentError>) + Send + 'static>;
#[derive(Default)]
struct Counters {
    requests: AtomicU64,
    reply_bytes: AtomicU64,
    cache_resident: AtomicU64,
    cache_pinned: AtomicU64,
    cache_inflight: AtomicU64,
    cache_total: AtomicU64,
    cache_items: AtomicU64,
    cache_peak: AtomicU64,
    coalesced: AtomicU64,
    promotions: AtomicU64,
    cache_hits: AtomicU64,
    cache_bypasses: AtomicU64,
    fetched_batches: AtomicU64,
    fetched_objects: AtomicU64,
    local_bytes: AtomicU64,
    rejected: AtomicU64,
    callback_panics: AtomicU64,
    worker_failures: AtomicU64,
}
#[derive(Clone, Debug, Serialize)]
pub struct ContentStats {
    pub outstanding_requests: u64,
    pub reply_bytes: u64,
    pub queued_local: usize,
    pub queued_large: usize,
    pub queued_foreground: usize,
    pub queued_speculative: usize,
    pub inflight_oids: usize,
    pub cache_files: u64,
    pub cache_resident_bytes: u64,
    pub cache_pinned_bytes: u64,
    pub cache_inflight_bytes: u64,
    pub cache_total_bytes: u64,
    pub cache_peak_bytes: u64,
    pub coalesced_requests: u64,
    pub foreground_promotions: u64,
    pub cache_hits: u64,
    pub cache_bypasses: u64,
    pub fetch_batches: u64,
    pub fetched_objects: u64,
    pub local_bytes: u64,
    pub rejected_requests: u64,
    pub callback_panics: u64,
    pub worker_failures: u64,
}
struct Permit {
    counters: Arc<Counters>,
    bytes: u64,
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.counters.requests.fetch_sub(1, Ordering::Relaxed);
        self.counters
            .reply_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}
/// Owned reply bytes. Admission remains charged until these bytes are dropped,
/// even if a callback transfers them to another task.
pub struct ContentData {
    bytes: Vec<u8>,
    _permit: Permit,
}
impl std::ops::Deref for ContentData {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsRef<[u8]> for ContentData {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
struct Waiter {
    offset: u64,
    size: u32,
    callback: Option<Callback>,
    permit: Option<Permit>,
}
impl Waiter {
    fn finish(mut self, result: std::result::Result<Vec<u8>, ContentError>) {
        let counters = Arc::clone(&self.permit.as_ref().unwrap().counters);
        let permit = self.permit.take().unwrap();
        let mut error_permit = None;
        let result = match result {
            Ok(bytes) => Ok(ContentData {
                bytes,
                _permit: permit,
            }),
            Err(error) => {
                error_permit = Some(permit);
                Err(error)
            }
        };
        let callback = self.callback.take().unwrap();
        invoke(callback, result, &counters);
        drop(error_permit);
    }
}
impl Drop for Waiter {
    fn drop(&mut self) {
        // Also covers an unexpected worker unwind after taking an active job
        // out of the flight table. Never silently discard an accepted reply.
        if let Some(callback) = self.callback.take() {
            let counters = &self.permit.as_ref().unwrap().counters;
            invoke(callback, Err(ContentError::Io), counters);
        }
    }
}
fn invoke(
    callback: Callback,
    result: std::result::Result<ContentData, ContentError>,
    counters: &Counters,
) {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(result))).is_err() {
        counters.callback_panics.fetch_add(1, Ordering::Relaxed);
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Local,
    Reading,
    Fetch,
    Fetching,
}
#[derive(Default)]
struct Completion {
    result: Mutex<Option<std::result::Result<(), ContentError>>>,
    wake: Condvar,
}
impl Completion {
    fn finish(&self, result: std::result::Result<(), ContentError>) {
        *self.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
        self.wake.notify_all();
    }
}
struct Flight {
    phase: Phase,
    foreground: bool,
    fetched: bool,
    completion: Option<Arc<Completion>>,
    waiters: Vec<Waiter>,
}
#[derive(Default)]
struct State {
    stopped: bool,
    flights: HashMap<String, Flight>,
    local: VecDeque<String>,
    large: VecDeque<String>,
    foreground: VecDeque<String>,
    speculative: VecDeque<String>,
}
struct Inner {
    state: Mutex<State>,
    wake: Condvar,
    config: ContentConfig,
    counters: Arc<Counters>,
    cancellation: BackgroundCancellation,
}

/// Cloneable submission endpoint. Accepted callbacks run exactly once, off dispatch.
#[derive(Clone)]
pub struct ContentHandle(Arc<Inner>);
impl ContentHandle {
    /// Nonblocking admission. Every callback executes exactly once: accepted jobs
    /// run off dispatch; rejected jobs receive their error synchronously, outside
    /// the state lock. The return value is for observability, not a second reply.
    /// The closure must not call blocking scheduler methods. No artificial per-read
    /// limit: legal kernel ranges consume the reply budget.
    pub fn read(
        &self,
        oid: &str,
        offset: u64,
        size: u32,
        callback: impl FnOnce(std::result::Result<ContentData, ContentError>) + Send + 'static,
    ) -> std::result::Result<(), ContentError> {
        let oid = match canonical_oid(oid) {
            Ok(oid) => oid,
            Err(_) => {
                invoke(
                    Box::new(callback),
                    Err(ContentError::Invalid),
                    &self.0.counters,
                );
                return Err(ContentError::Invalid);
            }
        };
        let inner = &self.0;
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.stopped || inner.cancellation.is_cancelled() {
            drop(state);
            invoke(
                Box::new(callback),
                Err(ContentError::Cancelled),
                &inner.counters,
            );
            return Err(ContentError::Cancelled);
        }
        let counters = &inner.counters;
        if (!state.flights.contains_key(&oid)
            && state.flights.values().filter(|f| f.foreground).count() >= inner.config.max_requests)
            || counters.requests.load(Ordering::Relaxed) >= inner.config.max_requests as u64
            || counters
                .reply_bytes
                .load(Ordering::Relaxed)
                .saturating_add(size as u64)
                > inner.config.reply_bytes
        {
            counters.rejected.fetch_add(1, Ordering::Relaxed);
            drop(state);
            invoke(Box::new(callback), Err(ContentError::Saturated), counters);
            return Err(ContentError::Saturated);
        }
        counters.requests.fetch_add(1, Ordering::Relaxed);
        counters
            .reply_bytes
            .fetch_add(size as u64, Ordering::Relaxed);
        let waiter = Waiter {
            offset,
            size,
            callback: Some(Box::new(callback)),
            permit: Some(Permit {
                counters: Arc::clone(counters),
                bytes: size as u64,
            }),
        };
        if let Some(flight) = state.flights.get_mut(&oid) {
            counters.coalesced.fetch_add(1, Ordering::Relaxed);
            let promote = !flight.foreground && flight.phase == Phase::Fetch;
            flight.foreground = true;
            flight.waiters.push(waiter);
            if promote {
                counters.promotions.fetch_add(1, Ordering::Relaxed);
                state.speculative.retain(|queued| queued != &oid);
                state.foreground.push_back(oid);
            }
        } else {
            state.flights.insert(
                oid.clone(),
                Flight {
                    phase: Phase::Local,
                    foreground: true,
                    fetched: false,
                    completion: None,
                    waiters: vec![waiter],
                },
            );
            state.local.push_back(oid);
        }
        inner.wake.notify_all();
        Ok(())
    }

    /// Best-effort speculative admission. Returns the number newly queued.
    /// A large selection is admitted incrementally rather than retained unbounded.
    pub fn prefetch(&self, oids: &[String]) -> std::result::Result<usize, ContentError> {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.stopped || self.0.cancellation.is_cancelled() {
            return Err(ContentError::Cancelled);
        }
        let mut added = 0;
        for oid in oids {
            let oid = canonical_oid(oid).map_err(|_| ContentError::Invalid)?;
            if state.flights.contains_key(&oid) {
                continue;
            }
            let speculative = state.flights.values().filter(|f| !f.foreground).count();
            if speculative >= self.0.config.max_prefetch {
                break;
            }
            state.flights.insert(
                oid.clone(),
                Flight {
                    phase: Phase::Fetch,
                    foreground: false,
                    fetched: false,
                    completion: None,
                    waiters: Vec::new(),
                },
            );
            state.speculative.push_back(oid);
            added += 1;
        }
        self.0.wake.notify_all();
        Ok(added)
    }

    /// Background-only barrier. Uses bounded chunks and one shared completion per
    /// OID, without consuming foreground reply credits. Never call from dispatch.
    pub fn prefetch_wait(&self, oids: &[String]) -> std::result::Result<(), ContentError> {
        self.available_wait(oids, false)
    }
    /// Foreground, completion-only barrier for serialized mutable copy-up. This
    /// preserves dispatch ordering while sharing queued/in-flight remote wants.
    pub fn ensure_available(&self, oids: &[String]) -> std::result::Result<(), ContentError> {
        self.available_wait(oids, true)
    }
    fn available_wait(
        &self,
        oids: &[String],
        foreground: bool,
    ) -> std::result::Result<(), ContentError> {
        for chunk in oids.chunks(self.0.config.fetch_batch.min(self.0.config.max_prefetch)) {
            let mut pending = Vec::new();
            for oid in chunk {
                let oid = canonical_oid(oid).map_err(|_| ContentError::Invalid)?;
                let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
                loop {
                    if stopped(self, &state) {
                        return Err(ContentError::Cancelled);
                    }
                    let count = state
                        .flights
                        .values()
                        .filter(|f| f.foreground == foreground)
                        .count();
                    let bound = if foreground {
                        self.0.config.max_requests
                    } else {
                        self.0.config.max_prefetch
                    };
                    if state.flights.contains_key(&oid) || count < bound {
                        break;
                    }
                    state = self
                        .0
                        .wake
                        .wait_timeout(state, Duration::from_millis(100))
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                }
                let existing = state.flights.contains_key(&oid);
                if foreground && existing {
                    self.0.counters.coalesced.fetch_add(1, Ordering::Relaxed);
                }
                if !existing {
                    state.flights.insert(
                        oid.clone(),
                        Flight {
                            phase: Phase::Fetch,
                            foreground,
                            fetched: false,
                            completion: None,
                            waiters: Vec::new(),
                        },
                    );
                    if foreground {
                        state.foreground.push_back(oid.clone());
                    } else {
                        state.speculative.push_back(oid.clone());
                    }
                }
                let promote = foreground
                    && state
                        .flights
                        .get(&oid)
                        .is_some_and(|f| !f.foreground && f.phase == Phase::Fetch);
                if foreground {
                    state.flights.get_mut(&oid).unwrap().foreground = true;
                }
                if promote {
                    self.0.counters.promotions.fetch_add(1, Ordering::Relaxed);
                    state.speculative.retain(|queued| queued != &oid);
                    state.foreground.push_back(oid.clone());
                }
                let flight = state.flights.get_mut(&oid).unwrap();
                let completion = flight
                    .completion
                    .get_or_insert_with(|| Arc::new(Completion::default()));
                pending.push(Arc::clone(completion));
            }
            self.0.wake.notify_all();
            for completion in pending {
                let mut result = completion.result.lock().unwrap_or_else(|e| e.into_inner());
                while result.is_none() {
                    if self.0.cancellation.is_cancelled() {
                        return Err(ContentError::Cancelled);
                    }
                    result = completion
                        .wake
                        .wait_timeout(result, Duration::from_millis(100))
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                }
                (*result).unwrap()?;
            }
        }
        Ok(())
    }

    /// Prevent new admission and cancel only the native Git groups owned by this mount.
    pub fn cancel(&self) {
        self.0.cancellation.cancel();
        let waiters = {
            let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            state.stopped = true;
            state.local.clear();
            state.large.clear();
            state.foreground.clear();
            state.speculative.clear();
            state
                .flights
                .drain()
                .flat_map(|(_, f)| f.waiters)
                .collect::<Vec<_>>()
        };
        self.0.wake.notify_all();
        for waiter in waiters {
            waiter.finish(Err(ContentError::Cancelled));
        }
    }

    pub fn stats(&self) -> ContentStats {
        let c = &self.0.counters;
        let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let get = |value: &AtomicU64| value.load(Ordering::Relaxed);
        ContentStats {
            outstanding_requests: get(&c.requests),
            reply_bytes: get(&c.reply_bytes),
            queued_local: state.local.len(),
            queued_large: state.large.len(),
            queued_foreground: state.foreground.len(),
            queued_speculative: state.speculative.len(),
            inflight_oids: state.flights.len(),
            cache_files: get(&c.cache_items),
            cache_resident_bytes: get(&c.cache_resident),
            cache_pinned_bytes: get(&c.cache_pinned),
            cache_inflight_bytes: get(&c.cache_inflight),
            cache_total_bytes: get(&c.cache_total),
            cache_peak_bytes: get(&c.cache_peak),
            coalesced_requests: get(&c.coalesced),
            foreground_promotions: get(&c.promotions),
            cache_hits: get(&c.cache_hits),
            cache_bypasses: get(&c.cache_bypasses),
            fetch_batches: get(&c.fetched_batches),
            fetched_objects: get(&c.fetched_objects),
            local_bytes: get(&c.local_bytes),
            rejected_requests: get(&c.rejected),
            callback_panics: get(&c.callback_panics),
            worker_failures: get(&c.worker_failures),
        }
    }
}

/// Mount-owned worker guard. Never construct one per request.
/// Drop cancels the shared mount Git scope and joins all three workers.
/// Explicit shutdown may be split: cancel before unmount, join after dispatch stops.
pub struct ContentScheduler {
    handle: ContentHandle,
    workers: Vec<JoinHandle<()>>,
}
impl ContentScheduler {
    pub fn new(repo: &mut GitRepo, config: ContentConfig) -> Result<Self> {
        ensure!(
            config.max_requests > 0
                && config.cache_entries > 0
                && config.reply_bytes >= 16 << 20
                && config.max_prefetch > 0
                && (1..=128).contains(&config.fetch_batch),
            "invalid content scheduler bounds"
        );
        let (local_repo, cancellation) = repo.background_handle()?;
        let (fetch_repo, _) = repo.background_handle()?;
        let directory = SpoolDirectory::new(repo.repository_path())?;
        let inner = Arc::new(Inner {
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            config,
            counters: Arc::new(Counters::default()),
            cancellation,
        });
        let handle = ContentHandle(inner);
        let mut owner = Self {
            handle,
            workers: Vec::new(),
        };
        let cache = Arc::new(Mutex::new(Cache {
            entries: VecDeque::new(),
            inner: Arc::clone(&owner.handle.0),
            directory,
        }));
        let local = owner.handle.clone();
        let local_cache = Arc::clone(&cache);
        owner.workers.push(
            std::thread::Builder::new()
                .name("vfs-local-content".into())
                .spawn(move || local_worker(local, local_repo, local_cache, false))?,
        );
        let (large_repo, _) = repo.background_handle()?;
        let large = owner.handle.clone();
        owner.workers.push(
            std::thread::Builder::new()
                .name("vfs-large-content".into())
                .spawn(move || local_worker(large, large_repo, cache, true))?,
        );
        let fetch = owner.handle.clone();
        owner.workers.push(
            std::thread::Builder::new()
                .name("vfs-fetch-content".into())
                .spawn(move || fetch_worker(fetch, fetch_repo))?,
        );
        Ok(owner)
    }
    pub fn handle(&self) -> ContentHandle {
        self.handle.clone()
    }
    pub fn cancel(&self) {
        self.handle.cancel();
    }
    pub fn join(&mut self) -> Result<()> {
        let mut panicked = false;
        for worker in self.workers.drain(..) {
            panicked |= worker.join().is_err();
        }
        ensure!(!panicked, "content worker panicked");
        Ok(())
    }
    pub fn shutdown(&mut self) -> Result<()> {
        self.cancel();
        self.join()
    }
}
impl Drop for ContentScheduler {
    fn drop(&mut self) {
        if self.shutdown().is_err() {
            eprintln!("git-vfs: content worker failed during shutdown");
        }
        if let Ok(report) = serde_json::to_string(&self.handle.stats()) {
            eprintln!("Content state: {report}");
        }
    }
}

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
struct SpoolDirectory(PathBuf);
impl SpoolDirectory {
    fn new(repo: &std::path::Path) -> Result<Self> {
        use std::os::unix::fs::DirBuilderExt;
        for _ in 0..1024 {
            let directory = repo.join(format!(
                "vfs-content-{}-{}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
            match std::fs::DirBuilder::new().mode(0o700).create(&directory) {
                Ok(()) => return Ok(Self(directory)),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.into()),
            }
        }
        anyhow::bail!("content spool directory collision limit exceeded")
    }
    fn anonymous(&self) -> Result<File> {
        let path = self.0.join(format!(
            "object-{}",
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)?;
        std::fs::remove_file(path)?;
        Ok(file)
    }
}
impl Drop for SpoolDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.0);
    }
}
#[derive(Clone, Copy)]
enum Category {
    Inflight,
    Resident,
    Pinned,
}
struct CachePermit {
    size: u64,
    counters: Arc<Counters>,
    category: Category,
}
impl CachePermit {
    fn counter(&self) -> &AtomicU64 {
        match self.category {
            Category::Inflight => &self.counters.cache_inflight,
            Category::Resident => &self.counters.cache_resident,
            Category::Pinned => &self.counters.cache_pinned,
        }
    }
    fn change(&mut self, category: Category) {
        self.counter().fetch_sub(self.size, Ordering::Relaxed);
        self.category = category;
        self.counter().fetch_add(self.size, Ordering::Relaxed);
    }
}
impl Drop for CachePermit {
    fn drop(&mut self) {
        self.counter().fetch_sub(self.size, Ordering::Relaxed);
        self.counters
            .cache_total
            .fetch_sub(self.size, Ordering::Relaxed);
        self.counters.cache_items.fetch_sub(1, Ordering::Relaxed);
    }
}
struct Cached {
    oid: String,
    file: File,
    permit: CachePermit,
}
struct Cache {
    entries: VecDeque<Cached>,
    inner: Arc<Inner>,
    directory: SpoolDirectory,
}
impl Cache {
    fn begin(&mut self, oid: &str, size: u64) -> Result<Option<Cached>> {
        if self.inner.config.cache_bytes == 0 || size > self.inner.config.cache_bytes {
            return Ok(None);
        }
        while self
            .inner
            .counters
            .cache_total
            .load(Ordering::Relaxed)
            .saturating_add(size)
            > self.inner.config.cache_bytes
            || self.inner.counters.cache_items.load(Ordering::Relaxed)
                >= self.inner.config.cache_entries as u64
        {
            if self.entries.pop_front().is_none() {
                return Ok(None);
            }
        }
        let file = self.directory.anonymous()?;
        let c = Arc::clone(&self.inner.counters);
        let total = c.cache_total.fetch_add(size, Ordering::Relaxed) + size;
        c.cache_peak.fetch_max(total, Ordering::Relaxed);
        c.cache_inflight.fetch_add(size, Ordering::Relaxed);
        c.cache_items.fetch_add(1, Ordering::Relaxed);
        Ok(Some(Cached {
            oid: oid.into(),
            file,
            permit: CachePermit {
                size,
                counters: c,
                category: Category::Inflight,
            },
        }))
    }
    fn take(&mut self, oid: &str) -> Option<Cached> {
        let position = self.entries.iter().position(|e| e.oid == oid)?;
        let mut entry = self.entries.remove(position)?;
        entry.permit.change(Category::Pinned);
        Some(entry)
    }
    fn insert(&mut self, mut entry: Cached) {
        entry.permit.change(Category::Resident);
        self.entries.push_back(entry);
    }
}

#[derive(Debug)]
struct DeferLarge;
impl std::fmt::Display for DeferLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("defer large local stream")
    }
}
impl std::error::Error for DeferLarge {}
struct WorkerFailure(ContentHandle);
impl Drop for WorkerFailure {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        self.0
            .0
            .counters
            .worker_failures
            .fetch_add(1, Ordering::Relaxed);
        self.0.0.cancellation.cancel();
        let waiters = {
            let mut state = self.0.0.state.lock().unwrap_or_else(|e| e.into_inner());
            state.stopped = true;
            state.local.clear();
            state.large.clear();
            state.foreground.clear();
            state.speculative.clear();
            state
                .flights
                .drain()
                .flat_map(|(_, f)| f.waiters)
                .collect::<Vec<_>>()
        };
        self.0.0.wake.notify_all();
        for waiter in waiters {
            waiter.finish(Err(ContentError::Io));
        }
    }
}

fn stopped(handle: &ContentHandle, state: &State) -> bool {
    state.stopped || handle.0.cancellation.is_cancelled()
}
fn finish_flight(handle: &ContentHandle, oid: &str, error: ContentError) {
    let waiters = handle
        .0
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .flights
        .remove(oid)
        .map(|f| {
            if let Some(done) = f.completion {
                done.finish(Err(error));
            }
            f.waiters
        })
        .unwrap_or_default();
    for waiter in waiters {
        waiter.finish(Err(error));
    }
}
fn local_worker(handle: ContentHandle, repo: GitRepo, cache: Arc<Mutex<Cache>>, large: bool) {
    let _failure = WorkerFailure(handle.clone());
    let mut reader = None;
    loop {
        let (oid, waiters) = {
            let mut state = handle.0.state.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if stopped(&handle, &state) {
                    return;
                }
                let next = if large {
                    state.large.pop_front()
                } else {
                    state.local.pop_front()
                };
                if let Some(oid) = next
                    && let Some(flight) = state.flights.get_mut(&oid)
                {
                    flight.phase = Phase::Reading;
                    break (oid, std::mem::take(&mut flight.waiters));
                }
                state = handle
                    .0
                    .wake
                    .wait_timeout(state, Duration::from_millis(100))
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        };
        let result = read_local(&handle, &repo, &mut reader, &cache, &oid, &waiters, large);
        match result {
            Ok(Some(buffers)) => {
                let cancelled = handle.0.cancellation.is_cancelled();
                for (waiter, bytes) in waiters.into_iter().zip(buffers) {
                    waiter.finish(if cancelled {
                        Err(ContentError::Cancelled)
                    } else {
                        Ok(bytes)
                    });
                }
                let mut state = handle.0.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(flight) = state.flights.get_mut(&oid) {
                    if let Some(done) = flight.completion.take() {
                        done.finish(Ok(()));
                    }
                    if flight.waiters.is_empty() {
                        state.flights.remove(&oid);
                    } else {
                        flight.phase = Phase::Local;
                        state.local.push_back(oid);
                        handle.0.wake.notify_all();
                    }
                }
            }
            Ok(None) => {
                let mut state = handle.0.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(flight) = state.flights.get_mut(&oid) {
                    if flight.fetched {
                        drop(state);
                        for waiter in waiters {
                            waiter.finish(Err(ContentError::Missing));
                        }
                        finish_flight(&handle, &oid, ContentError::Missing);
                        continue;
                    }
                    flight.waiters.extend(waiters);
                    flight.phase = Phase::Fetch;
                    state.foreground.push_back(oid);
                    handle.0.wake.notify_all();
                } else {
                    drop(state);
                    for waiter in waiters {
                        waiter.finish(Err(ContentError::Cancelled));
                    }
                }
            }
            Err(error) => {
                reader = None;
                if error.downcast_ref::<DeferLarge>().is_some() {
                    let mut state = handle.0.state.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(flight) = state.flights.get_mut(&oid) {
                        flight.waiters.extend(waiters);
                        flight.phase = Phase::Local;
                        state.large.push_back(oid);
                        handle.0.wake.notify_all();
                    } else {
                        drop(state);
                        for waiter in waiters {
                            waiter.finish(Err(ContentError::Cancelled));
                        }
                    }
                    continue;
                }
                let error = if handle.0.cancellation.is_cancelled() {
                    ContentError::Cancelled
                } else {
                    ContentError::Io
                };
                for waiter in waiters {
                    waiter.finish(Err(error));
                }
                finish_flight(&handle, &oid, error);
            }
        }
    }
}

fn read_range(file: &File, offset: u64, size: u32, length: u64) -> Result<Vec<u8>> {
    let count = length.saturating_sub(offset).min(size as u64) as usize;
    let mut data = vec![0; count];
    file.read_exact_at(&mut data, offset)?;
    Ok(data)
}
fn read_local(
    handle: &ContentHandle,
    repo: &GitRepo,
    reader: &mut Option<BlobReader>,
    cache: &Mutex<Cache>,
    oid: &str,
    waiters: &[Waiter],
    large: bool,
) -> Result<Option<Vec<Vec<u8>>>> {
    let cached = cache.lock().unwrap_or_else(|e| e.into_inner()).take(oid);
    if let Some(entry) = cached {
        handle
            .0
            .counters
            .cache_hits
            .fetch_add(waiters.len() as u64, Ordering::Relaxed);
        let output = waiters
            .iter()
            .map(|w| read_range(&entry.file, w.offset, w.size, entry.permit.size))
            .collect::<Result<Vec<_>>>();
        cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(entry);
        return output.map(Some);
    }
    if reader.is_none() {
        *reader = Some(BlobReader::start_local(
            &repo.repository,
            &repo.git_binary,
            repo.cancellation.as_ref(),
        )?);
    }
    let mut buffers = waiters
        .iter()
        .map(|w| Vec::with_capacity(w.size as usize))
        .collect::<Vec<_>>();
    let mut staged = None;
    let result = reader
        .as_mut()
        .unwrap()
        .stream_local(oid, |size, position, bytes| {
            if bytes.is_empty() {
                if !large && size > 1 << 20 {
                    return Err(DeferLarge.into());
                }
                staged = cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .begin(oid, size)
                    .unwrap_or(None);
                if staged.is_none() {
                    handle
                        .0
                        .counters
                        .cache_bypasses
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            if let Some(entry) = &mut staged
                && entry.file.write_all(bytes).is_err()
            {
                staged = None;
                handle
                    .0
                    .counters
                    .cache_bypasses
                    .fetch_add(1, Ordering::Relaxed);
            }
            handle
                .0
                .counters
                .local_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            for (waiter, buffer) in waiters.iter().zip(buffers.iter_mut()) {
                let start = position.max(waiter.offset);
                let end = position
                    .saturating_add(bytes.len() as u64)
                    .min(waiter.offset.saturating_add(waiter.size as u64));
                if start < end {
                    buffer.extend_from_slice(
                        &bytes[(start - position) as usize..(end - position) as usize],
                    );
                }
            }
            Ok(())
        })?;
    if result.is_none() {
        return Ok(None);
    }
    if let Some(entry) = staged {
        cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(entry);
    }
    Ok(Some(buffers))
}

fn fetch_worker(handle: ContentHandle, mut repo: GitRepo) {
    let _failure = WorkerFailure(handle.clone());
    loop {
        let batch = {
            let mut state = handle.0.state.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if stopped(&handle, &state) {
                    return;
                }
                let mut batch = Vec::new();
                let foreground = !state.foreground.is_empty();
                while batch.len() < handle.0.config.fetch_batch {
                    let next = if foreground {
                        state.foreground.pop_front()
                    } else {
                        state.speculative.pop_front()
                    };
                    let Some(oid) = next else {
                        break;
                    };
                    if let Some(flight) = state.flights.get_mut(&oid)
                        && flight.phase == Phase::Fetch
                    {
                        flight.phase = Phase::Fetching;
                        batch.push(oid);
                    }
                }
                if !batch.is_empty() {
                    break batch;
                }
                state = handle
                    .0
                    .wake
                    .wait_timeout(state, Duration::from_millis(100))
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        };
        let result =
            repo.prefetch_blobs_scheduled(&batch, Some(&handle.0.counters.fetched_batches));
        if let Ok(report) = &result {
            handle
                .0
                .counters
                .fetched_objects
                .fetch_add(report.fetched_blobs as u64, Ordering::Relaxed);
        }
        for oid in batch {
            if result.is_err() {
                finish_flight(
                    &handle,
                    &oid,
                    if handle.0.cancellation.is_cancelled() {
                        ContentError::Cancelled
                    } else {
                        ContentError::Io
                    },
                );
                continue;
            }
            let mut state = handle.0.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(flight) = state.flights.get_mut(&oid) {
                flight.fetched = true;
                if let Some(done) = flight.completion.take() {
                    done.finish(Ok(()));
                }
                if flight.waiters.is_empty() {
                    state.flights.remove(&oid);
                } else {
                    flight.phase = Phase::Local;
                    state.local.push_back(oid);
                }
            }
            handle.0.wake.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::process::Command;
    use std::sync::mpsc;
    use std::time::Instant;
    use tempfile::TempDir;

    fn git(path: &Path, args: &[&str]) -> Vec<u8> {
        let output = Command::new("git")
            .arg("-C")
            .arg(path)
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .unwrap();
        assert!(output.status.success(), "git fixture command failed");
        output.stdout
    }
    struct Fixture {
        root: TempDir,
        repo: GitRepo,
        local: String,
        missing: String,
    }
    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            std::fs::create_dir(&source).unwrap();
            git(&source, &["init", "--initial-branch=main"]);
            for (key, value) in [
                ("user.name", "Fixture"),
                ("user.email", "fixture@example.invalid"),
                ("uploadpack.allowFilter", "true"),
                ("uploadpack.allowAnySHA1InWant", "true"),
            ] {
                git(&source, &["config", key, value]);
            }
            std::fs::write(source.join("local"), b"already local content").unwrap();
            std::fs::write(source.join("empty"), b"").unwrap();
            std::fs::write(source.join("missing"), vec![b'x'; 256 * 1024]).unwrap();
            std::fs::write(source.join("other"), b"other missing object").unwrap();
            std::fs::write(source.join("large"), vec![b'x'; 2 * 1024 * 1024]).unwrap();
            git(&source, &["add", "."]);
            git(&source, &["commit", "-m", "fixture"]);
            let local = String::from_utf8(git(&source, &["rev-parse", "HEAD:local"]))
                .unwrap()
                .trim()
                .to_owned();
            let missing = String::from_utf8(git(&source, &["rev-parse", "HEAD:missing"]))
                .unwrap()
                .trim()
                .to_owned();
            let bare = root.path().join("repo.git");
            git(
                root.path(),
                &[
                    "clone",
                    "--bare",
                    "--filter=blob:none",
                    &format!("file://{}", source.display()),
                    bare.to_str().unwrap(),
                ],
            );
            let wrapper = root.path().join("controlled-git");
            std::fs::write(
                &wrapper,
                r#"#!/usr/bin/env python3
import os,pathlib,sys,time
root=pathlib.Path(__file__).parent
if 'fetch' in sys.argv and '--stdin' in sys.argv:
    (root/'started').touch()
    while not (root/'release').exists(): time.sleep(.01)
os.execvp('git',['git',*sys.argv[1:]])
"#,
            )
            .unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
            let mut repo =
                GitRepo::open(bare, wrapper, "origin", super::super::SizeFallback::Error).unwrap();
            assert_eq!(repo.read_blob(&local).unwrap(), b"already local content");
            Self {
                root,
                repo,
                local,
                missing,
            }
        }
        fn wait_fetch(&self) {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !self.root.path().join("started").exists() {
                assert!(Instant::now() < deadline, "fetch did not start");
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        fn release(&self) {
            std::fs::write(self.root.path().join("release"), b"ok").unwrap();
        }
    }
    fn request(
        handle: &ContentHandle,
        oid: &str,
        offset: u64,
        size: u32,
    ) -> mpsc::Receiver<std::result::Result<ContentData, ContentError>> {
        let (tx, rx) = mpsc::channel();
        handle
            .read(oid, offset, size, move |result| {
                let _ = tx.send(result);
            })
            .unwrap();
        rx
    }
    fn receive(rx: &mpsc::Receiver<std::result::Result<ContentData, ContentError>>) -> ContentData {
        rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap()
    }

    #[test]
    fn blocked_fetch_leaves_local_reads_responsive_and_coalesces_remote_oid() {
        let mut fixture = Fixture::new();
        let mut scheduler =
            ContentScheduler::new(&mut fixture.repo, ContentConfig::default()).unwrap();
        let handle = scheduler.handle();
        let first = request(&handle, &fixture.missing, 13, 4096);
        fixture.wait_fetch();
        let duplicate = request(&handle, &fixture.missing, 0, 32);
        let local = receive(&request(&handle, &fixture.local, 8, 5));
        assert_eq!(&*local, b"local");
        assert!(first.try_recv().is_err());
        fixture.release();
        assert_eq!(&*receive(&first), &[b'x'; 4096]);
        assert_eq!(&*receive(&duplicate), &[b'x'; 32]);
        assert_eq!(handle.stats().fetch_batches, 1);
        assert_eq!(handle.stats().fetched_objects, 1);
        assert!(handle.stats().coalesced_requests >= 1);
        drop(local);
        scheduler.shutdown().unwrap();
        assert_eq!(handle.stats().outstanding_requests, 0);
        assert_eq!(handle.stats().cache_total_bytes, 0);
    }

    #[test]
    fn oversized_content_streams_ranges_and_retained_replies_keep_admission_charged() {
        let mut fixture = Fixture::new();
        fixture.release();
        let config = ContentConfig {
            cache_bytes: 0,
            max_requests: 1,
            ..ContentConfig::default()
        };
        let mut scheduler = ContentScheduler::new(&mut fixture.repo, config).unwrap();
        let handle = scheduler.handle();
        let data = receive(&request(&handle, &fixture.missing, 65530, 32));
        assert_eq!(&*data, &[b'x'; 32]);
        assert_eq!(handle.stats().cache_total_bytes, 0);
        assert!(handle.stats().cache_bypasses > 0);
        assert_eq!(handle.stats().outstanding_requests, 1);
        let (rejected_tx, rejected_rx) = mpsc::channel();
        assert_eq!(
            handle.read(&fixture.local, 0, 1, move |result| {
                rejected_tx.send(result.err()).unwrap();
            }),
            Err(ContentError::Saturated)
        );
        assert_eq!(
            rejected_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Some(ContentError::Saturated)
        );
        drop(data);
        assert_eq!(handle.stats().outstanding_requests, 0);
        assert_eq!(handle.stats().reply_bytes, 0);
        let beyond = receive(&request(&handle, &fixture.local, u64::MAX, 16));
        assert!(beyond.is_empty());
        drop(beyond);
        let empty = String::from_utf8(git(
            &fixture.root.path().join("source"),
            &["rev-parse", "HEAD:empty"],
        ))
        .unwrap()
        .trim()
        .to_owned();
        drop(receive(&request(&handle, &empty, 0, 16)));
        assert_eq!(
            handle.stats().cache_files,
            0,
            "cache disabled includes zero-sized objects"
        );
        scheduler.shutdown().unwrap();
    }

    #[test]
    fn shutdown_cancels_blocked_fetch_and_completes_accepted_callbacks() {
        let mut fixture = Fixture::new();
        let mut scheduler =
            ContentScheduler::new(&mut fixture.repo, ContentConfig::default()).unwrap();
        let handle = scheduler.handle();
        let missing = request(&handle, &fixture.missing, 0, 32);
        fixture.wait_fetch();
        let started = Instant::now();
        scheduler.shutdown().unwrap();
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(matches!(
            missing.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(ContentError::Cancelled)
        ));
        assert_eq!(handle.stats().outstanding_requests, 0);
        assert_eq!(
            handle.read(&fixture.local, 0, 1, |_| {}),
            Err(ContentError::Cancelled)
        );
    }

    #[test]
    fn cache_reuses_local_content_and_enforces_byte_and_entry_limits() {
        let mut fixture = Fixture::new();
        let config = ContentConfig {
            cache_bytes: 512 << 10,
            cache_entries: 1,
            ..ContentConfig::default()
        };
        let mut scheduler = ContentScheduler::new(&mut fixture.repo, config).unwrap();
        let handle = scheduler.handle();
        drop(receive(&request(&handle, &fixture.local, 0, 21)));
        drop(receive(&request(&handle, &fixture.local, 1, 3)));
        assert!(handle.stats().cache_hits >= 1);
        fixture.release();
        drop(receive(&request(&handle, &fixture.missing, 0, 1)));
        assert_eq!(handle.stats().cache_files, 1);
        let before = handle.stats().local_bytes;
        drop(receive(&request(&handle, &fixture.local, 0, 1)));
        assert!(
            handle.stats().local_bytes > before,
            "entry eviction requires a new local pass"
        );
        assert_eq!(handle.stats().cache_files, 1);
        assert!(handle.stats().cache_peak_bytes <= 512 << 10);
        assert!(handle.stats().cache_resident_bytes <= 512 << 10);
        scheduler.shutdown().unwrap();
        assert_eq!(handle.stats().cache_total_bytes, 0);
    }
    #[test]
    fn queued_prefetch_is_promoted_and_background_barrier_shares_fetch() {
        let mut fixture = Fixture::new();
        let mut scheduler =
            ContentScheduler::new(&mut fixture.repo, ContentConfig::default()).unwrap();
        let handle = scheduler.handle();
        let first = request(&handle, &fixture.missing, 0, 1);
        fixture.wait_fetch();
        let other = String::from_utf8(git(
            &fixture.root.path().join("source"),
            &["rev-parse", "HEAD:other"],
        ))
        .unwrap()
        .trim()
        .to_owned();
        assert_eq!(handle.prefetch(std::slice::from_ref(&other)).unwrap(), 1);
        let second = request(&handle, &other, 0, 5);
        assert_eq!(handle.stats().foreground_promotions, 1);
        assert_eq!(handle.stats().queued_speculative, 0);
        let background = handle.clone();
        let first_oid = fixture.missing.clone();
        let barrier = std::thread::spawn(move || background.prefetch_wait(&[first_oid]));
        fixture.release();
        assert_eq!(&*receive(&first), b"x");
        assert_eq!(&*receive(&second), b"other");
        barrier.join().unwrap().unwrap();
        assert_eq!(handle.stats().fetched_objects, 2);
        scheduler.shutdown().unwrap();
    }

    #[test]
    fn callback_panic_releases_admission_and_does_not_strand_workers() {
        let mut fixture = Fixture::new();
        let mut scheduler =
            ContentScheduler::new(&mut fixture.repo, ContentConfig::default()).unwrap();
        let handle = scheduler.handle();
        handle
            .read(&fixture.local, 0, 4, |_| {
                panic!("injected reply consumer failure")
            })
            .unwrap();
        let result = receive(&request(&handle, &fixture.local, 0, 4));
        assert_eq!(&*result, b"alre");
        drop(result);
        assert_eq!(handle.stats().callback_panics, 1);
        scheduler.shutdown().unwrap();
        assert_eq!(handle.stats().outstanding_requests, 0);
    }

    #[test]
    fn error_callback_keeps_admission_charged_until_it_returns() {
        let mut fixture = Fixture::new();
        fixture.release();
        let mut scheduler =
            ContentScheduler::new(&mut fixture.repo, ContentConfig::default()).unwrap();
        let handle = scheduler.handle();
        let observer = handle.clone();
        let (tx, rx) = mpsc::channel();
        handle
            .read(
                "0000000000000000000000000000000000000000",
                0,
                16,
                move |result| {
                    assert!(result.is_err());
                    let stats = observer.stats();
                    tx.send((stats.outstanding_requests, stats.reply_bytes))
                        .unwrap();
                },
            )
            .unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), (1, 16));
        scheduler.shutdown().unwrap();
        assert_eq!(handle.stats().outstanding_requests, 0);
    }
    #[test]
    fn mounted_copyup_barrier_shares_fetch_then_uses_no_fetch_reader() {
        let mut fixture = Fixture::new();
        let mut scheduler =
            ContentScheduler::new(&mut fixture.repo, ContentConfig::default()).unwrap();
        let handle = scheduler.handle();
        fixture.repo.set_content_handle(handle.clone());
        let first = request(&handle, &fixture.missing, 0, 1);
        fixture.wait_fetch();
        let (mut copyup_repo, _) = fixture.repo.background_handle().unwrap();
        let oid = fixture.missing.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let copyup = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            copyup_repo.read_blob(&oid)
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while handle.stats().coalesced_requests == 0 {
            assert!(
                Instant::now() < deadline,
                "copy-up did not join the active OID flight"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        fixture.release();
        assert_eq!(&*receive(&first), b"x");
        assert_eq!(copyup.join().unwrap().unwrap(), vec![b'x'; 256 * 1024]);
        assert_eq!(handle.stats().fetch_batches, 1);
        // A second already-local copy-up barrier performs metadata checks only.
        assert_eq!(
            fixture.repo.read_blob(&fixture.local).unwrap(),
            b"already local content"
        );
        assert_eq!(handle.stats().fetch_batches, 1);
        scheduler.shutdown().unwrap();
    }

    #[test]
    fn active_waiter_unwind_completes_reply_and_stops_other_workers() {
        let mut fixture = Fixture::new();
        let mut scheduler =
            ContentScheduler::new(&mut fixture.repo, ContentConfig::default()).unwrap();
        let handle = scheduler.handle();
        let counters = Arc::clone(&handle.0.counters);
        counters.requests.fetch_add(1, Ordering::Relaxed);
        counters.reply_bytes.fetch_add(4, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel();
        let waiter = Waiter {
            offset: 0,
            size: 4,
            callback: Some(Box::new(move |value| {
                tx.send(value.err()).unwrap();
            })),
            permit: Some(Permit { counters, bytes: 4 }),
        };
        let worker_handle = handle.clone();
        scheduler.workers.push(std::thread::spawn(move || {
            let _failure = WorkerFailure(worker_handle);
            let _active = waiter;
            panic!("injected worker failure after taking active reply");
        }));
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Some(ContentError::Io)
        );
        assert!(scheduler.join().is_err());
        assert_eq!(handle.stats().worker_failures, 1);
        assert_eq!(handle.stats().outstanding_requests, 0);
        assert_eq!(
            handle.read(&fixture.local, 0, 1, |_| {}),
            Err(ContentError::Cancelled)
        );
    }

    #[test]
    fn large_local_stream_does_not_occupy_small_or_cached_read_lane() {
        let mut fixture = Fixture::new();
        fixture.release();
        // Hydrate the object before replacing the wrapper with a controlled local
        // batch protocol. The scheduling boundary is body streaming, not network.
        let large_oid = String::from_utf8(git(
            &fixture.root.path().join("source"),
            &["rev-parse", "HEAD:large"],
        ))
        .unwrap()
        .trim()
        .to_owned();
        fixture.repo.read_blob(&large_oid).unwrap();
        let wrapper = fixture.root.path().join("controlled-git");
        // Delay the real fixture's 2 MiB body after its valid header; ordinary
        // OIDs are still read by native no-lazy Git from the fixture.
        let script = format!(
            r#"#!/usr/bin/env python3
import os,pathlib,subprocess,sys,time
root=pathlib.Path(__file__).parent
if sys.argv[-2:]==['cat-file','--batch'] and os.environ.get('GIT_NO_LAZY_FETCH')=='1':
    for line in sys.stdin.buffer:
        oid=line.decode().strip()
        if oid=={oid}:
            sys.stdout.buffer.write((oid+' blob 2097152\n').encode());sys.stdout.buffer.flush()
            with (root/'large-attempts').open('a') as f: f.write('header\n')
            while not (root/'large-release').exists(): time.sleep(.01)
            data=b'x'*2097152
        else:
            prefix=sys.argv[1:sys.argv.index('cat-file')]
            data=subprocess.run(['git',*prefix,'cat-file','blob',oid],check=True,stdout=subprocess.PIPE).stdout
            sys.stdout.buffer.write((oid+' blob '+str(len(data))+'\n').encode())
        sys.stdout.buffer.write(data+b'\n');sys.stdout.buffer.flush()
    sys.exit(0)
os.execvp('git',['git',*sys.argv[1:]])
"#,
            oid = serde_json::to_string(&large_oid).unwrap()
        );
        std::fs::write(&wrapper, script).unwrap();
        let mut scheduler =
            ContentScheduler::new(&mut fixture.repo, ContentConfig::default()).unwrap();
        let handle = scheduler.handle();
        let large = request(&handle, &large_oid, 2097140, 12);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let headers = std::fs::read_to_string(fixture.root.path().join("large-attempts"))
                .unwrap_or_default()
                .lines()
                .count();
            if headers >= 2 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "large worker never acquired deferred stream"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let local = request(&handle, &fixture.local, 0, 4);
        assert_eq!(
            &*local.recv_timeout(Duration::from_secs(1)).unwrap().unwrap(),
            b"alre"
        );
        assert!(large.try_recv().is_err());
        std::fs::write(fixture.root.path().join("large-release"), b"ok").unwrap();
        assert_eq!(&*receive(&large), &[b'x'; 12]);
        scheduler.shutdown().unwrap();
    }
    #[test]
    fn stale_spool_directories_do_not_prevent_restart_or_get_deleted() {
        let root = tempfile::tempdir().unwrap();
        let start = NEXT_DIRECTORY.load(Ordering::Relaxed);
        let stale = (start..start + 32)
            .map(|sequence| {
                root.path()
                    .join(format!("vfs-content-{}-{sequence}", std::process::id()))
            })
            .collect::<Vec<_>>();
        for path in &stale {
            std::fs::create_dir(path).unwrap();
        }
        let directory = SpoolDirectory::new(root.path()).unwrap();
        assert!(!stale.contains(&directory.0));
        drop(directory);
        assert!(stale.iter().all(|path| path.is_dir()));
    }
}
