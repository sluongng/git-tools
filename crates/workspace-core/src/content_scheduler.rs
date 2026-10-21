//! Bounded immutable content work. No worker can mutate the projected namespace.
use super::{BackgroundCancellation, BlobReader, GitRepo, canonical_oid};
use anyhow::{Result, ensure};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{FileExt, OpenOptionsExt};
#[cfg(windows)]
use std::os::windows::fs::{FileExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Independent bounds for callbacks, reply buffers, and anonymous cache files.
#[derive(Clone, Debug)]
pub struct ContentConfig {
    pub max_requests: usize,
    pub reply_bytes: u64,
    pub cache_bytes: u64,
    /// Separate mount-owned anonymous storage for blobs exceeding cache_bytes.
    /// None allows 512 MiB when the cache is enabled, and no spool when it is
    /// disabled. Some(0) disables this pool; Some(n) explicitly bounds its total
    /// reserved bytes (and therefore each object), including with cache_bytes=0.
    pub large_blob_spool_bytes: Option<u64>,
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
            large_blob_spool_bytes: None,
            cache_entries: 256,
            max_prefetch: 256,
            fetch_batch: 32,
        }
    }
}
impl ContentConfig {
    fn large_blob_spool_limit(&self) -> u64 {
        self.large_blob_spool_bytes
            .unwrap_or(if self.cache_bytes == 0 { 0 } else { 512 << 20 })
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
    spool_total: AtomicU64,
    spool_peak: AtomicU64,
    spool_items: AtomicU64,
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
    demand_fetch_objects: AtomicU64,
    demand_fetch_bytes: AtomicU64,
    prefetch_fetch_objects: AtomicU64,
    prefetch_fetch_bytes: AtomicU64,
    prefetch_inflight_demand_joins: AtomicU64,
    prefetch_completed_before_demand_objects: AtomicU64,
    prefetch_completed_before_demand_bytes: AtomicU64,
    useful_prefetch_objects: AtomicU64,
    useful_prefetch_bytes: AtomicU64,
    prefetch_unobserved_evicted_objects: AtomicU64,
    prefetch_unobserved_evicted_bytes: AtomicU64,
    demand_initial_queue: Histogram,
    demand_completion: Histogram,
    demand_fetch_queue: Histogram,
    demand_fetch_service: Histogram,
}

// Disjoint buckets: <=100 us, <=1 ms, <=10 ms, <=100 ms, <=1 s,
// <=10 s, and >10 s. Fixed cardinality regardless of repository size.
#[derive(Default)]
struct Histogram([AtomicU64; 7]);
impl Histogram {
    fn observe(&self, elapsed: Duration) {
        let bucket = [100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000]
            .iter()
            .position(|bound| elapsed <= Duration::from_micros(*bound))
            .unwrap_or(6);
        self.0[bucket].fetch_add(1, Ordering::Relaxed);
    }
    fn snapshot(&self) -> [u64; 7] {
        std::array::from_fn(|index| self.0[index].load(Ordering::Relaxed))
    }
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
    /// Reserved anonymous large-blob storage, independent of cache_total_bytes.
    /// In-flight writes and pinned readers remain charged until their file closes.
    pub large_blob_spool_bytes: u64,
    pub large_blob_spool_limit_bytes: u64,
    pub large_blob_spool_peak_bytes: u64,
    pub large_blob_spool_files: u64,
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
    /// Successful remotely fetched objects, attributed at fetch dispatch.
    /// Bytes are uncompressed Git blob bytes, not wire or pack bytes.
    pub demand_fetch_objects: u64,
    pub demand_fetch_bytes: u64,
    pub prefetch_fetch_objects: u64,
    pub prefetch_fetch_bytes: u64,
    /// First foreground consumer joining an already dispatched speculative fetch.
    pub prefetch_inflight_demand_joins: u64,
    /// Successful speculative fetches with no foreground consumer at completion.
    pub prefetch_completed_before_demand_objects: u64,
    pub prefetch_completed_before_demand_bytes: u64,
    /// First demand for an object fetched speculatively before that demand,
    /// observed within the last max_prefetch completed, unjoined speculative objects.
    pub useful_prefetch_objects: u64,
    pub useful_prefetch_bytes: u64,
    pub prefetch_observation_capacity: usize,
    pub prefetch_unobserved_objects: usize,
    pub prefetch_unobserved_bytes: u64,
    /// Eviction means demand was not observed in the bounded window, not that
    /// the object will never be used. Native Git reads bypass this observation.
    pub prefetch_unobserved_evicted_objects: u64,
    pub prefetch_unobserved_evicted_bytes: u64,
    /// Disjoint latency buckets in microseconds: <=100, <=1000, <=10000,
    /// <=100000, <=1000000, <=10000000, >10000000.
    /// Initial ready-for-local-read queue wait, one sample per accepted read
    /// that reaches a local worker. Excludes time waiting on an in-flight fetch.
    pub demand_initial_queue_us_buckets: [u64; 7],
    /// Admission to callback start, including errors, excluding callback work.
    pub demand_completion_us_buckets: [u64; 7],
    /// Per-OID demand fetch queue wait, from missing detection/promotion to dispatch.
    pub demand_fetch_queue_us_buckets: [u64; 7],
    /// Per demand batch: local checks, optional Git fetch and size persistence,
    /// including failed batches. Not a pure network-latency measurement.
    pub demand_fetch_service_us_buckets: [u64; 7],
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
    submitted: Instant,
    initial_queue_recorded: bool,
    ready_since: Option<Instant>,
    offset: u64,
    size: u32,
    callback: Option<Callback>,
    permit: Option<Permit>,
}
impl Waiter {
    fn finish(mut self, result: std::result::Result<Vec<u8>, ContentError>) {
        let counters = Arc::clone(&self.permit.as_ref().unwrap().counters);
        counters.demand_completion.observe(self.submitted.elapsed());
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
            counters.demand_completion.observe(self.submitted.elapsed());
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
    fetch_queued: Instant,
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
    // Attribution only: never holds blob bodies or adds work to either queue.
    prefetched: VecDeque<(String, u64, bool)>,
}
impl State {
    fn observe_demand(&mut self, oid: &str, counters: &Counters) {
        if let Some((_, bytes, observed)) = self
            .prefetched
            .iter_mut()
            .find(|(key, _, observed)| key == oid && !*observed)
        {
            *observed = true;
            counters
                .useful_prefetch_objects
                .fetch_add(1, Ordering::Relaxed);
            counters
                .useful_prefetch_bytes
                .fetch_add(*bytes, Ordering::Relaxed);
        }
    }
    fn observe_prefetch(&mut self, oid: String, bytes: u64, capacity: usize, counters: &Counters) {
        // A second successful fetch of the same object cannot normally occur,
        // but remove it to preserve a strict unique-object observation window.
        self.prefetched.retain(|(key, _, _)| key != &oid);
        if self.prefetched.len() == capacity {
            let (_, evicted, observed) = self.prefetched.pop_front().unwrap();
            if !observed {
                counters
                    .prefetch_unobserved_evicted_objects
                    .fetch_add(1, Ordering::Relaxed);
                counters
                    .prefetch_unobserved_evicted_bytes
                    .fetch_add(evicted, Ordering::Relaxed);
            }
        }
        counters
            .prefetch_completed_before_demand_objects
            .fetch_add(1, Ordering::Relaxed);
        counters
            .prefetch_completed_before_demand_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.prefetched.push_back((oid, bytes, false));
    }
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
        state.observe_demand(&oid, counters);
        let mut waiter = Waiter {
            submitted: Instant::now(),
            initial_queue_recorded: false,
            ready_since: Some(Instant::now()),
            offset,
            size,
            callback: Some(Box::new(callback)),
            permit: Some(Permit {
                counters: Arc::clone(counters),
                bytes: size as u64,
            }),
        };
        if let Some(flight) = state.flights.get_mut(&oid) {
            if matches!(flight.phase, Phase::Fetch | Phase::Fetching) {
                waiter.ready_since = None;
            }
            counters.coalesced.fetch_add(1, Ordering::Relaxed);
            if !flight.foreground && flight.phase == Phase::Fetching {
                counters
                    .prefetch_inflight_demand_joins
                    .fetch_add(1, Ordering::Relaxed);
            }
            let promote = !flight.foreground && flight.phase == Phase::Fetch;
            flight.foreground = true;
            flight.waiters.push(waiter);
            if promote {
                flight.fetch_queued = Instant::now();
                counters.promotions.fetch_add(1, Ordering::Relaxed);
                state.speculative.retain(|queued| queued != &oid);
                state.foreground.push_back(oid);
            }
        } else {
            state.flights.insert(
                oid.clone(),
                Flight {
                    fetch_queued: Instant::now(),
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

    /// Synchronous adapter for native callbacks. Never call from a scheduler
    /// callback or while holding a namespace/repository lock. Dropping a caller
    /// does not cancel shared work; only the mount owner cancels the scheduler.
    pub fn read_blocking(
        &self,
        oid: &str,
        offset: u64,
        size: u32,
    ) -> std::result::Result<ContentData, ContentError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let _ = self.read(oid, offset, size, move |result| {
            let _ = tx.send(result);
        });
        rx.recv().unwrap_or(Err(ContentError::Io))
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
                    fetch_queued: Instant::now(),
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
                if foreground {
                    state.observe_demand(&oid, &self.0.counters);
                }
                let existing = state.flights.contains_key(&oid);
                if foreground && existing {
                    self.0.counters.coalesced.fetch_add(1, Ordering::Relaxed);
                }
                if !existing {
                    state.flights.insert(
                        oid.clone(),
                        Flight {
                            fetch_queued: Instant::now(),
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
                    if state
                        .flights
                        .get(&oid)
                        .is_some_and(|f| !f.foreground && f.phase == Phase::Fetching)
                    {
                        self.0
                            .counters
                            .prefetch_inflight_demand_joins
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    state.flights.get_mut(&oid).unwrap().foreground = true;
                }
                if promote {
                    state.flights.get_mut(&oid).unwrap().fetch_queued = Instant::now();
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
            large_blob_spool_bytes: get(&c.spool_total),
            large_blob_spool_limit_bytes: self.0.config.large_blob_spool_limit(),
            large_blob_spool_peak_bytes: get(&c.spool_peak),
            large_blob_spool_files: get(&c.spool_items),
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
            demand_fetch_objects: get(&c.demand_fetch_objects),
            demand_fetch_bytes: get(&c.demand_fetch_bytes),
            prefetch_fetch_objects: get(&c.prefetch_fetch_objects),
            prefetch_fetch_bytes: get(&c.prefetch_fetch_bytes),
            prefetch_inflight_demand_joins: get(&c.prefetch_inflight_demand_joins),
            prefetch_completed_before_demand_objects: get(
                &c.prefetch_completed_before_demand_objects
            ),
            prefetch_completed_before_demand_bytes: get(&c.prefetch_completed_before_demand_bytes),
            useful_prefetch_objects: get(&c.useful_prefetch_objects),
            useful_prefetch_bytes: get(&c.useful_prefetch_bytes),
            prefetch_observation_capacity: self.0.config.max_prefetch,
            prefetch_unobserved_objects: state
                .prefetched
                .iter()
                .filter(|(_, _, observed)| !observed)
                .count(),
            prefetch_unobserved_bytes: state
                .prefetched
                .iter()
                .filter(|(_, _, observed)| !observed)
                .map(|(_, bytes, _)| *bytes)
                .sum(),
            prefetch_unobserved_evicted_objects: get(&c.prefetch_unobserved_evicted_objects),
            prefetch_unobserved_evicted_bytes: get(&c.prefetch_unobserved_evicted_bytes),
            demand_initial_queue_us_buckets: c.demand_initial_queue.snapshot(),
            demand_completion_us_buckets: c.demand_completion.snapshot(),
            demand_fetch_queue_us_buckets: c.demand_fetch_queue.snapshot(),
            demand_fetch_service_us_buckets: c.demand_fetch_service.snapshot(),
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
        #[cfg(unix)]
        use std::os::unix::fs::DirBuilderExt;
        for _ in 0..1024 {
            let directory = repo.join(format!(
                "vfs-content-{}-{}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
            #[cfg(unix)]
            let created = std::fs::DirBuilder::new().mode(0o700).create(&directory);
            #[cfg(windows)]
            let created = std::fs::create_dir(&directory);
            match created {
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
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        // CREATE_NEW refuses existing files, including reparse points. Keep
        // the spool anonymous through a delete-on-close handle on Windows.
        #[cfg(windows)]
        options
            .custom_flags(
                windows_sys::Win32::Storage::FileSystem::FILE_FLAG_DELETE_ON_CLOSE
                    | windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT,
            )
            .access_mode(
                windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_READ
                    | windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_WRITE
                    | windows_sys::Win32::Storage::FileSystem::DELETE,
            )
            .share_mode(0);
        let file = options.open(&path)?;
        #[cfg(unix)]
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
    spool: bool,
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
        if self.spool {
            return;
        }
        self.counter().fetch_sub(self.size, Ordering::Relaxed);
        self.category = category;
        self.counter().fetch_add(self.size, Ordering::Relaxed);
    }
}
impl Drop for CachePermit {
    fn drop(&mut self) {
        if self.spool {
            self.counters
                .spool_total
                .fetch_sub(self.size, Ordering::Relaxed);
            self.counters.spool_items.fetch_sub(1, Ordering::Relaxed);
            return;
        }
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
        let spool = self.inner.config.cache_bytes == 0 || size > self.inner.config.cache_bytes;
        let limit = if spool {
            self.inner.config.large_blob_spool_limit()
        } else {
            self.inner.config.cache_bytes
        };
        if limit == 0 || size > limit {
            return Ok(None);
        }
        let c = Arc::clone(&self.inner.counters);
        let total_counter = if spool {
            &c.spool_total
        } else {
            &c.cache_total
        };
        while total_counter
            .load(Ordering::Relaxed)
            .checked_add(size)
            .is_none_or(|total| total > limit)
            || c.cache_items.load(Ordering::Relaxed) + c.spool_items.load(Ordering::Relaxed)
                >= self.inner.config.cache_entries as u64
        {
            if self.entries.pop_front().is_none() {
                return Ok(None);
            }
        }
        let file = self.directory.anonymous()?;
        let total = total_counter.fetch_add(size, Ordering::Relaxed) + size;
        if spool {
            c.spool_peak.fetch_max(total, Ordering::Relaxed);
            c.spool_items.fetch_add(1, Ordering::Relaxed);
        } else {
            c.cache_peak.fetch_max(total, Ordering::Relaxed);
            c.cache_inflight.fetch_add(size, Ordering::Relaxed);
            c.cache_items.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Some(Cached {
            oid: oid.into(),
            file,
            permit: CachePermit {
                size,
                counters: c,
                category: Category::Inflight,
                spool,
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
        let (oid, waiters) =
            {
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
                        let mut waiters = std::mem::take(&mut flight.waiters);
                        for waiter in &mut waiters {
                            if !waiter.initial_queue_recorded {
                                handle.0.counters.demand_initial_queue.observe(
                                    waiter.ready_since.unwrap_or_else(Instant::now).elapsed(),
                                );
                                waiter.initial_queue_recorded = true;
                            }
                        }
                        break (oid, waiters);
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
                    // Reads joining an active local probe have not entered a
                    // worker yet. Their local-ready queue clock must stop while
                    // the missing object is fetched, just like Fetching joiners.
                    for waiter in &mut flight.waiters {
                        if !waiter.initial_queue_recorded {
                            waiter.ready_since = None;
                        }
                    }
                    flight.phase = Phase::Fetch;
                    flight.fetch_queued = Instant::now();
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
    #[cfg(unix)]
    file.read_exact_at(&mut data, offset)?;
    #[cfg(windows)]
    {
        let mut read = 0;
        while read < data.len() {
            let count = file.seek_read(&mut data[read..], offset + read as u64)?;
            ensure!(count != 0, "unexpected end of content spool");
            read += count;
        }
    }
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
    if repo.shared_cas.is_some() {
        if !large
            && repo
                .cache_lock()?
                .blake3_many(&[oid.to_owned()])?
                .get(oid)
                .is_some_and(|(size, _)| *size > 1 << 20)
        {
            return Err(DeferLarge.into());
        }
        match repo.shared_blob(oid, false) {
            Ok(Some(blob)) => {
                let file = File::open(blob.path())?;
                let length = file.metadata()?.len();
                if !large && length > 1 << 20 {
                    return Err(DeferLarge.into());
                }
                let output = waiters
                    .iter()
                    .map(|w| read_range(&file, w.offset, w.size, length))
                    .collect::<Result<Vec<_>>>()?;
                handle
                    .0
                    .counters
                    .cache_hits
                    .fetch_add(waiters.len() as u64, Ordering::Relaxed);
                return Ok(Some(output));
            }
            Ok(None) => {}
            // Cache capacity, corruption and misses never strand an otherwise
            // readable Git object. Native streaming below verifies its Git
            // identity and detects conflicting mappings before publication.
            Err(_) => {}
        }
    }
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
    let mut blake3 = repo.shared_cas.as_ref().map(|_| blake3::Hasher::new());
    let mut git_sha1 = repo
        .shared_cas
        .as_ref()
        .map(|_| <sha1::Sha1 as sha1::Digest>::new());
    let result = reader
        .as_mut()
        .unwrap()
        .stream_local(oid, |size, position, bytes| {
            if let Some(hash) = &mut blake3 {
                hash.update(bytes);
            }
            if let Some(hash) = &mut git_sha1 {
                use sha1::Digest;
                if bytes.is_empty() {
                    hash.update(format!("blob {size}\0"));
                }
                hash.update(bytes);
            }
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
    if let (Some(hash), Some(git)) = (blake3, git_sha1) {
        use sha1::Digest;
        ensure!(
            hex::encode(git.finalize()) == oid,
            "Git blob SHA1 verification failed"
        );
        repo.learn_shared_blob(oid, result.unwrap(), *hash.finalize().as_bytes())?;
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
        let (batch, foreground) = {
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
                        if foreground {
                            handle
                                .0
                                .counters
                                .demand_fetch_queue
                                .observe(flight.fetch_queued.elapsed());
                        }
                        batch.push(oid);
                    }
                }
                if !batch.is_empty() {
                    break (batch, foreground);
                }
                state = handle
                    .0
                    .wake
                    .wait_timeout(state, Duration::from_millis(100))
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        };
        let started = Instant::now();
        let result =
            repo.prefetch_blobs_scheduled(&batch, Some(&handle.0.counters.fetched_batches));
        if foreground {
            // One sample per dispatched demand batch, including failed service.
            handle
                .0
                .counters
                .demand_fetch_service
                .observe(started.elapsed());
        }
        if let Ok((report, fetched_sizes)) = &result {
            let counters = &handle.0.counters;
            let (objects, bytes) = if foreground {
                (&counters.demand_fetch_objects, &counters.demand_fetch_bytes)
            } else {
                (
                    &counters.prefetch_fetch_objects,
                    &counters.prefetch_fetch_bytes,
                )
            };
            objects.fetch_add(report.fetched_blobs as u64, Ordering::Relaxed);
            bytes.fetch_add(fetched_sizes.values().sum(), Ordering::Relaxed);
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
                let observed_demand = flight.foreground;
                flight.fetched = true;
                if let Some(done) = flight.completion.take() {
                    done.finish(Ok(()));
                }
                if flight.waiters.is_empty() {
                    state.flights.remove(&oid);
                } else {
                    flight.phase = Phase::Local;
                    for waiter in &mut flight.waiters {
                        waiter.ready_since.get_or_insert_with(Instant::now);
                    }
                    state.local.push_back(oid.clone());
                }
                if !foreground
                    && !observed_demand
                    && let Some(bytes) = result.as_ref().unwrap().1.get(&oid)
                {
                    state.observe_prefetch(
                        oid,
                        *bytes,
                        handle.0.config.max_prefetch,
                        &handle.0.counters,
                    );
                }
            }
            handle.0.wake.notify_all();
        }
    }
}

#[cfg(all(test, unix))]
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

    // Run explicitly with --ignored --nocapture. The fixture uses native Git,
    // serialized callbacks and no shared CAS, so neither coalescing nor a mapped
    // shared object can hide repeated native decoding.
    #[test]
    #[ignore = "native Git range-read benchmark"]
    fn oversized_native_git_range_benchmark() {
        fn cpu_seconds() -> f64 {
            let mut total = 0.0;
            for who in [libc::RUSAGE_SELF, libc::RUSAGE_CHILDREN] {
                let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
                assert_eq!(unsafe { libc::getrusage(who, usage.as_mut_ptr()) }, 0);
                let usage = unsafe { usage.assume_init() };
                for time in [usage.ru_utime, usage.ru_stime] {
                    total += time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
                }
            }
            total
        }
        const LENGTH: usize = 8 << 20;
        const RANGE: u32 = 128 << 10;
        for (cache_bytes, large_blob_spool_bytes) in
            [(0, None), (512 << 10, None), (0, Some(16 << 20))]
        {
            for pattern in ["sequential", "random"] {
                let mut fixture = Fixture::new();
                let bytes = (0..LENGTH)
                    .map(|offset| ((offset / 4096 + offset % 251) % 256) as u8)
                    .collect::<Vec<_>>();
                let path = fixture.root.path().join("benchmark-blob");
                std::fs::write(&path, &bytes).unwrap();
                let oid = String::from_utf8(git(
                    fixture.repo.repository_path(),
                    &["hash-object", "-w", path.to_str().unwrap()],
                ))
                .unwrap()
                .trim()
                .to_owned();
                let mut scheduler = ContentScheduler::new(
                    &mut fixture.repo,
                    ContentConfig {
                        cache_bytes,
                        large_blob_spool_bytes,
                        ..ContentConfig::default()
                    },
                )
                .unwrap();
                let handle = scheduler.handle();
                let cpu = cpu_seconds();
                let started = Instant::now();
                for index in 0..LENGTH / RANGE as usize {
                    // Odd multiplication permutes all 64 chunks deterministically.
                    let chunk = if pattern == "random" {
                        (index * 37) % 64
                    } else {
                        index
                    };
                    let offset = chunk * RANGE as usize;
                    let data = receive(&request(&handle, &oid, offset as u64, RANGE));
                    assert_eq!(&*data, &bytes[offset..offset + RANGE as usize]);
                }
                let wall_seconds = started.elapsed().as_secs_f64();
                let stats = handle.stats();
                scheduler.shutdown().unwrap();
                eprintln!(
                    "OVERSIZED_BENCH {}",
                    serde_json::json!({
                        "cache_bytes": cache_bytes, "pattern": pattern,
                    "configured_spool_bytes": large_blob_spool_bytes,
                        "blob_bytes": LENGTH, "range_bytes": RANGE, "reads": 64,
                        "wall_seconds": wall_seconds, "cpu_seconds": cpu_seconds() - cpu,
                        "decoded_bytes": stats.local_bytes,
                        "cache_peak_bytes": stats.cache_peak_bytes,
                    "spool_peak_bytes": stats.large_blob_spool_peak_bytes,
                        "cache_hits": stats.cache_hits,
                        "post_shutdown_bytes": handle.stats().cache_total_bytes,
                    "post_shutdown_spool_bytes": handle.stats().large_blob_spool_bytes,
                    })
                );
            }
        }
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
        assert_eq!(handle.stats().demand_fetch_objects, 2);
        assert_eq!(handle.stats().demand_fetch_bytes, 256 * 1024 + 20);
        assert_eq!(handle.stats().prefetch_fetch_objects, 0);
        assert_eq!(
            handle
                .stats()
                .demand_fetch_queue_us_buckets
                .iter()
                .sum::<u64>(),
            2
        );
        assert_eq!(
            handle
                .stats()
                .demand_fetch_service_us_buckets
                .iter()
                .sum::<u64>(),
            2
        );
        scheduler.shutdown().unwrap();
    }

    #[test]
    fn prefetch_attribution_has_a_bounded_observation_window_without_staging_content() {
        let mut fixture = Fixture::new();
        fixture.release();
        let config = ContentConfig {
            max_prefetch: 1,
            ..ContentConfig::default()
        };
        let mut scheduler = ContentScheduler::new(&mut fixture.repo, config).unwrap();
        let handle = scheduler.handle();
        let object = |name: &str| {
            String::from_utf8(git(
                &fixture.root.path().join("source"),
                &["rev-parse", name],
            ))
            .unwrap()
            .trim()
            .to_owned()
        };
        let other = object("HEAD:other");
        let large = object("HEAD:large");
        handle
            .prefetch_wait(std::slice::from_ref(&fixture.missing))
            .unwrap();
        let before = handle.stats();
        assert_eq!(before.prefetch_fetch_objects, 1);
        assert_eq!(before.prefetch_fetch_bytes, 256 * 1024);
        assert_eq!(before.prefetch_unobserved_bytes, 256 * 1024);
        assert_eq!(
            before.cache_files, 0,
            "speculation does not stage anonymous content"
        );
        drop(receive(&request(&handle, &fixture.missing, 0, 1)));
        drop(receive(&request(&handle, &fixture.missing, 0, 1)));
        assert_eq!(handle.stats().useful_prefetch_objects, 1);
        assert_eq!(handle.stats().useful_prefetch_bytes, 256 * 1024);
        assert_eq!(handle.stats().prefetch_unobserved_objects, 0);
        handle.prefetch_wait(std::slice::from_ref(&other)).unwrap();
        assert_eq!(handle.stats().prefetch_unobserved_evicted_objects, 0);
        handle.prefetch_wait(std::slice::from_ref(&large)).unwrap();
        let after = handle.stats();
        assert_eq!(after.prefetch_observation_capacity, 1);
        assert_eq!(after.prefetch_unobserved_objects, 1);
        assert_eq!(after.prefetch_unobserved_bytes, 2 * 1024 * 1024);
        assert_eq!(after.prefetch_unobserved_evicted_objects, 1);
        assert_eq!(after.prefetch_unobserved_evicted_bytes, 20);
        assert_eq!(
            after.prefetch_completed_before_demand_objects,
            after.useful_prefetch_objects
                + after.prefetch_unobserved_objects as u64
                + after.prefetch_unobserved_evicted_objects
        );
        assert_eq!(
            after.prefetch_completed_before_demand_bytes,
            after.useful_prefetch_bytes
                + after.prefetch_unobserved_bytes
                + after.prefetch_unobserved_evicted_bytes
        );
        drop(receive(&request(&handle, &other, 0, 1)));
        assert_eq!(
            handle.stats().useful_prefetch_objects,
            1,
            "late demand is outside the observation window"
        );
        handle
            .prefetch_wait(std::slice::from_ref(&fixture.local))
            .unwrap();
        assert_eq!(
            handle.stats().prefetch_fetch_objects,
            3,
            "already-local objects are not fetched bytes"
        );
        assert_eq!(handle.stats().demand_fetch_objects, 0);
        let stats = handle.stats();
        assert_eq!(stats.demand_initial_queue_us_buckets.iter().sum::<u64>(), 3);
        assert_eq!(stats.demand_completion_us_buckets.iter().sum::<u64>(), 3);
        scheduler.shutdown().unwrap();
    }

    #[test]
    fn demand_joining_a_local_miss_does_not_count_fetch_service_as_local_queue_wait() {
        let mut fixture = Fixture::new();
        let wrapper = fixture.root.path().join("controlled-git");
        std::fs::write(
            &wrapper,
            format!(
                r#"#!/usr/bin/env python3
import os,pathlib,subprocess,sys,time
root=pathlib.Path(__file__).parent
if sys.argv[-2:]==['cat-file','--batch'] and os.environ.get('GIT_NO_LAZY_FETCH')=='1':
    for line in sys.stdin.buffer:
        if line.decode().strip()=={oid} and not (root/'probe-started').exists():
            (root/'probe-started').touch()
            while not (root/'probe-release').exists(): time.sleep(.001)
        result=subprocess.run(['git',*sys.argv[1:]],input=line,stdout=subprocess.PIPE,check=True)
        sys.stdout.buffer.write(result.stdout);sys.stdout.buffer.flush()
    sys.exit(0)
if 'fetch' in sys.argv and '--stdin' in sys.argv:
    (root/'started').touch()
    while not (root/'release').exists(): time.sleep(.001)
os.execvp('git',['git',*sys.argv[1:]])
"#,
                oid = serde_json::to_string(&fixture.missing).unwrap()
            ),
        )
        .unwrap();
        let mut scheduler =
            ContentScheduler::new(&mut fixture.repo, ContentConfig::default()).unwrap();
        let handle = scheduler.handle();
        let first = request(&handle, &fixture.missing, 0, 1);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !fixture.root.path().join("probe-started").exists() {
            assert!(
                Instant::now() < deadline,
                "controlled local probe did not start"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let joined = request(&handle, &fixture.missing, 0, 1);
        std::fs::write(fixture.root.path().join("probe-release"), b"ok").unwrap();
        fixture.wait_fetch();
        // Cross the 100 ms histogram boundary while only remote service is held.
        std::thread::sleep(Duration::from_millis(120));
        fixture.release();
        drop(receive(&first));
        drop(receive(&joined));
        let stats = handle.stats();
        assert_eq!(stats.demand_initial_queue_us_buckets.iter().sum::<u64>(), 2);
        assert_eq!(
            stats.demand_initial_queue_us_buckets[4..]
                .iter()
                .sum::<u64>(),
            0,
            "time held in fetch service must not become local-ready queue time"
        );
        assert_eq!(
            stats.demand_completion_us_buckets[4..].iter().sum::<u64>(),
            2,
            "completion latency still includes the controlled fetch delay"
        );
        scheduler.shutdown().unwrap();
    }

    #[test]
    fn demand_joining_an_inflight_prefetch_keeps_dispatch_origin_and_counts_join() {
        let mut fixture = Fixture::new();
        let mut scheduler =
            ContentScheduler::new(&mut fixture.repo, ContentConfig::default()).unwrap();
        let handle = scheduler.handle();
        assert_eq!(
            handle
                .prefetch(std::slice::from_ref(&fixture.missing))
                .unwrap(),
            1
        );
        fixture.wait_fetch();
        let read = request(&handle, &fixture.missing, 0, 1);
        assert_eq!(handle.stats().prefetch_inflight_demand_joins, 1);
        fixture.release();
        drop(receive(&read));
        let stats = handle.stats();
        assert_eq!(stats.prefetch_fetch_objects, 1);
        assert_eq!(stats.prefetch_fetch_bytes, 256 * 1024);
        assert_eq!(stats.demand_fetch_objects, 0);
        assert_eq!(
            stats.useful_prefetch_objects, 0,
            "prefetch did not finish before demand"
        );
        assert_eq!(
            stats.prefetch_unobserved_objects, 0,
            "joined demand is known, not unused"
        );
        assert_eq!(stats.demand_completion_us_buckets.iter().sum::<u64>(), 1);
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
            submitted: Instant::now(),
            initial_queue_recorded: false,
            ready_since: Some(Instant::now()),
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
        for complete_stream in [true, false] {
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
            with (root/'large-attempts').open('a') as f: f.write('header\n')
            sys.stdout.buffer.write((oid+' blob 2097152\n').encode());sys.stdout.buffer.flush()
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
            let mut scheduler = ContentScheduler::new(
                &mut fixture.repo,
                ContentConfig {
                    cache_bytes: 512 << 10,
                    large_blob_spool_bytes: Some(2 << 20),
                    ..ContentConfig::default()
                },
            )
            .unwrap();
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
            while handle.stats().large_blob_spool_bytes != 2 << 20 {
                assert!(
                    Instant::now() < deadline,
                    "large stream did not reserve its spool budget"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            if complete_stream {
                std::fs::write(fixture.root.path().join("large-release"), b"ok").unwrap();
                assert_eq!(&*receive(&large), &[b'x'; 12]);
            } else {
                scheduler.shutdown().unwrap();
                assert!(matches!(
                    large.recv_timeout(Duration::from_secs(1)).unwrap(),
                    Err(ContentError::Cancelled)
                ));
            }
            scheduler.shutdown().unwrap();
            assert_eq!(handle.stats().large_blob_spool_bytes, 0);
            assert_eq!(handle.stats().large_blob_spool_files, 0);
        }
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

// Unlike the controlled-promisor Unix fixtures above, this public scheduler
// workflow uses only native Git and runs on Windows as well as Unix.
#[cfg(test)]
mod portable_tests {
    use super::*;
    use std::process::{Command, Stdio};

    #[test]
    fn native_git_cached_ranges_and_shutdown_remove_spool_files() {
        let root = tempfile::tempdir().unwrap();
        let bare = root.path().join("objects ü with spaces.git");
        let status = Command::new("git")
            .args(["init", "--bare", "--object-format=sha1"])
            .arg(&bare)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success());
        let bytes: Vec<_> = (0..(2 * 1024 * 1024)).map(|i| (i % 251) as u8).collect();
        let store = |contents: &[u8]| {
            let mut child = Command::new("git")
                .arg("--git-dir")
                .arg(&bare)
                .args(["hash-object", "-w", "--stdin"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(contents).unwrap();
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success());
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        };
        let oid = store(&bytes);
        let mut other_bytes = bytes.clone();
        other_bytes[0] = 255;
        let other = store(&other_bytes);
        struct Case {
            name: &'static str,
            config: ContentConfig,
            cached: bool,
            spooled: bool,
        }
        for case in [
            Case {
                name: "ordinary cache",
                config: ContentConfig::default(),
                cached: true,
                spooled: false,
            },
            Case {
                name: "oversized default spool",
                config: ContentConfig {
                    cache_bytes: 4096,
                    cache_entries: 1,
                    ..ContentConfig::default()
                },
                cached: false,
                spooled: true,
            },
            Case {
                name: "explicit spool with cache disabled",
                config: ContentConfig {
                    cache_bytes: 0,
                    large_blob_spool_bytes: Some(bytes.len() as u64),
                    ..ContentConfig::default()
                },
                cached: false,
                spooled: true,
            },
            Case {
                name: "all caching disabled",
                config: ContentConfig {
                    cache_bytes: 0,
                    ..ContentConfig::default()
                },
                cached: false,
                spooled: false,
            },
            Case {
                name: "oversized spool explicitly disabled",
                config: ContentConfig {
                    cache_bytes: 4096,
                    large_blob_spool_bytes: Some(0),
                    ..ContentConfig::default()
                },
                cached: false,
                spooled: false,
            },
            Case {
                name: "object exceeds spool limit",
                config: ContentConfig {
                    cache_bytes: 4096,
                    large_blob_spool_bytes: Some(4096),
                    ..ContentConfig::default()
                },
                cached: false,
                spooled: false,
            },
        ] {
            let mut repo =
                GitRepo::open(&bare, "git", "origin", super::super::SizeFallback::Error).unwrap();
            let cache_limit = case.config.cache_bytes;
            let spool_limit = case.config.large_blob_spool_limit();
            let mut scheduler = ContentScheduler::new(&mut repo, case.config).unwrap();
            let handle = scheduler.handle();
            for (offset, requested) in [
                (0, 512),
                (65_529, 64),
                (bytes.len() - 7, 64),
                (bytes.len() + 1, 8),
            ] {
                let data = handle
                    .read_blocking(&oid, offset as u64, requested)
                    .unwrap();
                let start = offset.min(bytes.len());
                let end = offset.saturating_add(requested as usize).min(bytes.len());
                assert_eq!(data.as_ref(), &bytes[start..end]);
            }
            let stats = handle.stats();
            assert_eq!(stats.cache_files, u64::from(case.cached), "{}", case.name);
            assert_eq!(
                stats.large_blob_spool_files,
                u64::from(case.spooled),
                "{}",
                case.name
            );
            assert_eq!(
                stats.cache_hits,
                if case.cached || case.spooled { 3 } else { 0 },
                "{}",
                case.name
            );
            assert_eq!(
                stats.local_bytes,
                bytes.len() as u64 * if case.cached || case.spooled { 1 } else { 4 },
                "{}",
                case.name
            );
            assert!(stats.cache_peak_bytes <= cache_limit, "{}", case.name);
            assert!(
                stats.large_blob_spool_peak_bytes <= spool_limit,
                "{}",
                case.name
            );
            if case.spooled {
                assert_eq!(handle.read_blocking(&other, 0, 1).unwrap().as_ref(), &[255]);
                assert_eq!(handle.read_blocking(&oid, 0, 1).unwrap().as_ref(), &[0]);
                let evicted = handle.stats();
                assert_eq!(evicted.local_bytes, 3 * bytes.len() as u64, "{}", case.name);
                assert_eq!(evicted.large_blob_spool_files, 1, "{}", case.name);
                assert_eq!(
                    evicted.large_blob_spool_bytes,
                    bytes.len() as u64,
                    "{}",
                    case.name
                );
                assert!(
                    evicted.large_blob_spool_peak_bytes <= spool_limit,
                    "{}",
                    case.name
                );
            }
            assert!(matches!(
                handle.read_blocking("not-an-oid", 0, 1),
                Err(ContentError::Invalid)
            ));
            scheduler.shutdown().unwrap();
            assert!(matches!(
                handle.read_blocking(&oid, 0, 1),
                Err(ContentError::Cancelled)
            ));
            assert_eq!(handle.stats().cache_total_bytes, 0);
            assert_eq!(handle.stats().large_blob_spool_bytes, 0);
            assert!(
                !std::fs::read_dir(&bare).unwrap().any(|entry| entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("vfs-content-")),
                "shutdown must close delete-on-close handles before removing the spool directory"
            );
        }
    }
}
