//! Shared verified content-addressable disk cache with byte-bounded LRU GC.
//! Only clean CAS bytes belong here; writable workspace data must live elsewhere.
//! Keys include the backend security scope, instance, digest function, hash and
//! size. Backend identities must distinguish credentials/tenants when their
//! authorization differs, even when they use the same network endpoint.
//! Open a directory once and share its `Arc<LocalCas>` across consumers. Separate
//! processes must use distinct directories; the ownership lock deliberately
//! rejects unsafe concurrent GC rather than claiming multi-process coherence.
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
#[derive(Clone, Debug)]
pub struct CacheLimitError {
    pub size: u64,
    pub limit: u64,
}
impl std::fmt::Display for CacheLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "artifact size {} exceeds effective readable limit {} (increase cache and/or hydration byte budgets)",
            self.size, self.limit
        )
    }
}
impl std::error::Error for CacheLimitError {}
/// A bounded hydration operation expired. This does not authorize tearing down
/// native paging reads or cancelling a shared fetch on behalf of one waiter.
#[derive(Clone, Copy, Debug)]
pub struct CacheTimeoutError;
impl std::fmt::Display for CacheTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "output cache operation deadline exceeded")
    }
}
impl std::error::Error for CacheTimeoutError {}
/// Authored failure boundaries only: no artifact, path, endpoint or error text.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub enum FailureStage {
    CacheCreate,
    CacheFetch,
    CacheSync,
    CacheVerificationRead,
    CacheVerification,
    CachePublicationRename,
    CachePublicationSync,
    CachePublicationFingerprint,
    CacheLookupFingerprint,
    CacheLookupVerification,
    DownloadTargetClone,
    DownloadTargetReset,
    DownloadHeaders,
    DownloadMessage,
    DownloadWireSize,
    DownloadDecoderStart,
    DownloadDecode,
    DownloadDecodeFinish,
    DownloadWrite,
    DownloadSize,
    Unattributed,
}
const FAILURE_STAGES: [FailureStage; 21] = [
    FailureStage::CacheCreate,
    FailureStage::CacheFetch,
    FailureStage::CacheSync,
    FailureStage::CacheVerificationRead,
    FailureStage::CacheVerification,
    FailureStage::CachePublicationRename,
    FailureStage::CachePublicationSync,
    FailureStage::CachePublicationFingerprint,
    FailureStage::CacheLookupFingerprint,
    FailureStage::CacheLookupVerification,
    FailureStage::DownloadTargetClone,
    FailureStage::DownloadTargetReset,
    FailureStage::DownloadHeaders,
    FailureStage::DownloadMessage,
    FailureStage::DownloadWireSize,
    FailureStage::DownloadDecoderStart,
    FailureStage::DownloadDecode,
    FailureStage::DownloadDecodeFinish,
    FailureStage::DownloadWrite,
    FailureStage::DownloadSize,
    FailureStage::Unattributed,
];
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Io,
    Timeout,
    Limit,
    SizeMismatch,
    DigestMismatch,
    FingerprintChanged,
    EncodedSizeLimit,
    Decode,
    GrpcStatus,
    Unattributed,
}
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureEncoding {
    Identity,
    Zstd,
}
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureIoKind {
    NotFound,
    PermissionDenied,
    AlreadyExists,
    WouldBlock,
    InvalidInput,
    InvalidData,
    TimedOut,
    WriteZero,
    Interrupted,
    UnexpectedEof,
    OutOfMemory,
    StorageFull,
    Other,
}
#[derive(Clone, Copy, Debug, Serialize)]
pub struct FailureDiagnostic {
    pub stage: FailureStage,
    pub class: FailureClass,
    pub io_error_kind: Option<FailureIoKind>,
    pub raw_os_error: Option<i32>,
    pub expected_size: Option<u64>,
    pub actual_size: Option<u64>,
    pub digest_function: Option<i32>,
    pub encoding: Option<FailureEncoding>,
    pub grpc_code: Option<i32>,
}
impl FailureDiagnostic {
    pub fn new(stage: FailureStage, class: FailureClass) -> Self {
        Self {
            stage,
            class,
            io_error_kind: None,
            raw_os_error: None,
            expected_size: None,
            actual_size: None,
            digest_function: None,
            encoding: None,
            grpc_code: None,
        }
    }
    pub fn from_io(stage: FailureStage, error: &std::io::Error) -> Self {
        use std::io::ErrorKind;
        let mut diagnostic = Self::new(stage, FailureClass::Io);
        diagnostic.io_error_kind = Some(match error.kind() {
            ErrorKind::NotFound => FailureIoKind::NotFound,
            ErrorKind::PermissionDenied => FailureIoKind::PermissionDenied,
            ErrorKind::AlreadyExists => FailureIoKind::AlreadyExists,
            ErrorKind::WouldBlock => FailureIoKind::WouldBlock,
            ErrorKind::InvalidInput => FailureIoKind::InvalidInput,
            ErrorKind::InvalidData => FailureIoKind::InvalidData,
            ErrorKind::TimedOut => FailureIoKind::TimedOut,
            ErrorKind::WriteZero => FailureIoKind::WriteZero,
            ErrorKind::Interrupted => FailureIoKind::Interrupted,
            ErrorKind::UnexpectedEof => FailureIoKind::UnexpectedEof,
            ErrorKind::OutOfMemory => FailureIoKind::OutOfMemory,
            ErrorKind::StorageFull => FailureIoKind::StorageFull,
            _ => FailureIoKind::Other,
        });
        diagnostic.raw_os_error = error.raw_os_error();
        diagnostic
    }
    pub fn from_error(stage: FailureStage, error: &anyhow::Error) -> Self {
        for cause in error.chain() {
            if cause.is::<CacheTimeoutError>() {
                return Self::new(stage, FailureClass::Timeout);
            }
            if cause.is::<CacheLimitError>() {
                return Self::new(stage, FailureClass::Limit);
            }
            if let Some(error) = cause.downcast_ref::<std::io::Error>() {
                return Self::from_io(stage, error);
            }
        }
        Self::new(stage, FailureClass::Unattributed)
    }
}
#[derive(Clone, Copy, Debug, Serialize)]
pub struct FailureStageCount {
    pub stage: FailureStage,
    pub count: u64,
}
/// Fixed-size process-local observations, not unique failures or event logs.
/// A propagated error can be observed at more than one boundary. Samples never
/// retain an error chain or any caller-supplied string.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct FailureMetrics {
    pub total: u64,
    pub stages: [FailureStageCount; 21],
    pub recent: [Option<FailureDiagnostic>; 16],
}
impl Default for FailureMetrics {
    fn default() -> Self {
        Self {
            total: 0,
            stages: std::array::from_fn(|i| FailureStageCount {
                stage: FAILURE_STAGES[i],
                count: 0,
            }),
            recent: [None; 16],
        }
    }
}
struct FailureState {
    total: u64,
    counts: [u64; 21],
    recent: [Option<FailureDiagnostic>; 16],
    next: usize,
}
impl Default for FailureState {
    fn default() -> Self {
        Self {
            total: 0,
            counts: [0; 21],
            recent: [None; 16],
            next: 0,
        }
    }
}
#[derive(Clone, Default)]
pub struct FailureDiagnostics(Arc<Mutex<FailureState>>);
impl FailureDiagnostics {
    pub fn record(&self, diagnostic: FailureDiagnostic) {
        // Diagnostics must not turn an existing error into a poisoning panic.
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.total = state.total.saturating_add(1);
        let count = &mut state.counts[diagnostic.stage as usize];
        *count = count.saturating_add(1);
        let next = state.next;
        state.recent[next] = Some(diagnostic);
        state.next = (next + 1) % 16;
    }
    pub fn metrics(&self) -> FailureMetrics {
        let state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        FailureMetrics {
            total: state.total,
            stages: std::array::from_fn(|i| FailureStageCount {
                stage: FAILURE_STAGES[i],
                count: state.counts[i],
            }),
            recent: std::array::from_fn(|i| state.recent[(state.next + i) % 16]),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Artifact {
    pub backend: String,
    pub instance: String,
    pub digest_function: i32,
    pub hash: String,
    pub size: u64,
}
impl Artifact {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.digest_function, 1 | 9),
            "unsupported digest function"
        );
        ensure!(
            self.hash.len() == 64
                && self
                    .hash
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "invalid digest hash"
        );
        Ok(())
    }
}
/// Opaque ownership of cache admission, disk reservation, temporary path and
/// directory lock. A source detaching physical I/O must move a clone into that
/// job and keep it until its target file handle is closed. Caller cancellation
/// or a deadline does not release these resources while a job retains a clone.
#[derive(Clone)]
pub struct FetchContext {
    _resources: Arc<FetchResources>,
}
impl std::fmt::Debug for FetchContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchContext").finish_non_exhaustive()
    }
}
struct FetchResources {
    // Clean temporary paths before releasing reservation/admission/ownership.
    _temporary: Temporary,
    _reservation: Arc<Mutex<Reservation>>,
    _bytes: OwnedSemaphorePermit,
    _cache: Arc<LocalCas>,
    // Last: wait_for_idle is an ownership barrier, not just a worker counter.
    _request: Arc<OwnedSemaphorePermit>,
}

#[async_trait]
pub trait BlobSource: Send + Sync {
    /// Stream at most artifact.size bytes; reject an overlong response before
    /// writing it. The cache reserves this declared disk/transfer budget and
    /// independently verifies both complete hash and exact final size.
    async fn fetch(&self, artifact: &Artifact, target: &mut File) -> Result<()>;
    /// Borrow-only sources can use this default. A source cloning the target or
    /// detaching physical work MUST override this and retain the context through
    /// physical completion, even when its fetch future is dropped.
    async fn fetch_with_context(
        &self,
        artifact: &Artifact,
        target: &mut File,
        context: FetchContext,
    ) -> Result<()> {
        let _context = context;
        self.fetch(artifact, target).await
    }
    async fn missing(&self, artifacts: &[Artifact]) -> Result<Vec<Artifact>>;
    /// Best-effort notification after a verified local read. Implementations
    /// must not block or treat this as evidence of upstream presence.
    fn accessed(&self, _artifact: &Artifact, _cache: Weak<LocalCas>) {}
    /// Called on the fetch source only after full hash/size verification and
    /// publication. An unused per-request source receives no confirmation.
    fn verified(&self, _artifact: &Artifact, _fetch_started: std::time::Instant) {}
}
#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub max_bytes: u64,
    /// Hydration limit; foreground cache-only verification has a separate lane
    /// with this same limit, while maintenance verification has one slot.
    pub max_concurrent_fetches: usize,
    pub max_inflight_bytes: u64,
    /// Separate disk/transfer admission lane for blobs larger than the normal
    /// in-flight budget. Zero disables it. Still bounded by max_bytes, shared
    /// fetch slots, full-object verification, deadlines and cache leases.
    pub max_oversized_inflight_bytes: u64,
    /// Per-waiter budget, including fetch-slot admission and a shared result.
    pub operation_timeout: Duration,
    /// Independent admitted-fetch lifetime, including byte admission and verify.
    pub fetch_timeout: Duration,
}
impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 4 * 1024 * 1024 * 1024,
            max_concurrent_fetches: 8,
            max_inflight_bytes: 512 * 1024 * 1024,
            max_oversized_inflight_bytes: 0,
            operation_timeout: Duration::from_secs(120),
            fetch_timeout: Duration::from_secs(120),
        }
    }
}
impl CacheConfig {
    /// Largest artifact whose bytes may be hydrated. Staging metadata does not
    /// require hydration and intentionally accepts larger artifacts.
    pub fn readable_limit(&self) -> u64 {
        self.max_bytes.min(
            self.max_inflight_bytes
                .max(self.max_oversized_inflight_bytes),
        )
    }
}
/// Elapsed nanosecond counters accumulate concurrent, failed and cancelled work;
/// they are not an additive wall-time breakdown.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct CacheMetrics {
    pub fetches: u64,
    pub verified_bytes: u64,
    pub verification_bytes: u64,
    pub hits: u64,
    pub timed_out_waiters: u64,
    pub timed_out_fetches: u64,
    pub admission_wait_ns: u64,
    pub fetch_ns: u64,
    pub sync_ns: u64,
    pub verification_ns: u64,
    pub publication_ns: u64,
    pub resident_bytes: u64,
    pub reserved_bytes: u64,
    pub evictions: u64,
    pub evicted_bytes: u64,
    pub recency_errors: u64,
    pub failures: FailureMetrics,
}
fn hash_file_until(
    path: &Path,
    function: i32,
    deadline: Option<tokio::time::Instant>,
    cancelled: &AtomicBool,
) -> Result<(String, u64)> {
    if cancelled.load(Ordering::Acquire) {
        return Err(CacheTimeoutError.into());
    }
    let mut file = File::open(path)?;
    let mut sha = Sha256::new();
    let mut blake = blake3::Hasher::new();
    ensure!(matches!(function, 1 | 9), "unsupported digest function");
    let mut size = 0;
    let mut buf = [0u8; 128 * 1024];
    loop {
        // Blocking OS reads cannot be preempted, but an expired verification
        // must stop between chunks rather than continue detached over the file.
        if cancelled.load(Ordering::Acquire)
            || deadline.is_some_and(|d| tokio::time::Instant::now() >= d)
        {
            return Err(CacheTimeoutError.into());
        }
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        size += n as u64;
        if function == 1 {
            sha.update(&buf[..n])
        } else {
            blake.update(&buf[..n]);
        }
    }
    Ok((
        if function == 1 {
            format!("{:x}", sha.finalize())
        } else {
            blake.finalize().to_hex().to_string()
        },
        size,
    ))
}
struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        if !self.0.as_os_str().is_empty() {
            let _ = fs::remove_file(&self.0);
        }
    }
}

/// A cache file lease. The file is verified before publication and cannot be
/// reclaimed while any clone of this Arc remains alive.
pub struct CachedBlob {
    path: PathBuf,
    cache: Arc<LocalCas>,
}
impl CachedBlob {
    pub fn path(&self) -> &Path {
        &self.path
    }
}
impl Drop for CachedBlob {
    fn drop(&mut self) {
        let mut state = self.cache.files.lock().unwrap();
        if let Some(entry) = state.entries.get_mut(&self.path) {
            entry.pins = entry.pins.saturating_sub(1);
            if entry.pins == 0 {
                let rank = entry.accessed;
                state.evictable.insert((rank, self.path.clone()));
            }
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Fingerprint {
    len: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    identity: (u64, u64, u64, u64),
}
fn fingerprint(path: &Path) -> Result<Fingerprint> {
    let m = fs::metadata(path)?;
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        (m.dev(), m.ino(), m.ctime() as u64, m.ctime_nsec() as u64)
    };
    #[cfg(not(unix))]
    let identity = (0, 0, 0, 0);
    Ok(Fingerprint {
        len: m.len(),
        modified: m.modified().ok(),
        created: m.created().ok(),
        identity,
    })
}
struct CacheEntry {
    bytes: u64,
    pins: usize,
    accessed: u64,
    verified: Option<Fingerprint>,
}
#[derive(Default)]
struct CacheFiles {
    entries: HashMap<PathBuf, CacheEntry>,
    evictable: BTreeSet<(u64, PathBuf)>,
    resident: u64,
    reserved: u64,
    next_rank: u64,
    revision: u64,
    dirty: bool,
}
impl CacheFiles {
    fn changed(&mut self) {
        self.revision = self.revision.wrapping_add(1);
        self.dirty = true;
    }
    fn rank(&mut self) -> u64 {
        if self.next_rank == u64::MAX {
            let mut order: Vec<_> = self
                .entries
                .iter()
                .map(|(p, e)| (e.accessed, p.clone()))
                .collect();
            order.sort();
            self.evictable.clear();
            for (rank, (_, path)) in order.into_iter().enumerate() {
                let entry = self.entries.get_mut(&path).unwrap();
                entry.accessed = rank as u64;
                if entry.pins == 0 {
                    self.evictable.insert((entry.accessed, path));
                }
            }
            self.next_rank = self.entries.len() as u64;
        }
        let rank = self.next_rank;
        self.next_rank += 1;
        rank
    }
    fn remove(&mut self, path: &Path) {
        if let Some(entry) = self.entries.remove(path) {
            self.evictable.remove(&(entry.accessed, path.to_owned()));
            self.resident -= entry.bytes;
            self.changed();
        }
    }
}
#[derive(Serialize, Deserialize)]
struct Recency {
    version: u32,
    entries: HashMap<String, u64>,
}
struct Checkpoint {
    last: std::time::Instant,
}
// Preserve typed source errors across coalesced waiters without coupling this
// crate to a transport. Adapters can inspect e.g. tonic::Status in the chain.
#[derive(Debug)]
struct SharedFailure(Arc<anyhow::Error>);
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}
#[derive(Clone)]
enum FlightFailure {
    Timeout,
    Other(Arc<anyhow::Error>),
}
impl FlightFailure {
    fn from_error(error: anyhow::Error) -> Self {
        if error.downcast_ref::<CacheTimeoutError>().is_some() {
            Self::Timeout
        } else {
            Self::Other(Arc::new(error))
        }
    }
    fn into_error(self) -> anyhow::Error {
        match self {
            Self::Timeout => CacheTimeoutError.into(),
            Self::Other(error) => anyhow::Error::new(SharedFailure(error)),
        }
    }
}
struct Flight {
    result: Mutex<Option<std::result::Result<Arc<CachedBlob>, FlightFailure>>>,
    complete: Arc<AtomicBool>,
    ready: Arc<tokio::sync::Notify>,
}
impl Flight {
    async fn wait(&self) -> Result<Arc<CachedBlob>> {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            // A visible result alone is not completion: the worker must also
            // release its own cache/lease references before callers can drop
            // their handles and immediately reopen the cache directory.
            if self.complete.load(Ordering::Acquire) {
                return self
                    .result
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("completed cache flight has a result")
                    .clone()
                    .map_err(FlightFailure::into_error);
            }
            notified.await;
        }
    }
}
struct Reservation {
    cache: Weak<LocalCas>,
    bytes: u64,
}
enum TimerStart {
    Runtime(tokio::time::Instant),
    Wall(std::time::Instant),
}
impl TimerStart {
    fn elapsed(&self) -> Duration {
        match self {
            Self::Runtime(started) => started.elapsed(),
            Self::Wall(started) => started.elapsed(),
        }
    }
}
struct ElapsedTimer<'a> {
    elapsed: &'a AtomicU64,
    started: TimerStart,
}
impl<'a> ElapsedTimer<'a> {
    fn start(elapsed: &'a AtomicU64) -> Self {
        Self {
            elapsed,
            started: TimerStart::Wall(std::time::Instant::now()),
        }
    }
}
impl Drop for ElapsedTimer<'_> {
    fn drop(&mut self) {
        self.elapsed.fetch_add(
            self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(cache) = self.cache.upgrade() {
            cache.files.lock().unwrap().reserved -= self.bytes;
        }
    }
}
struct DirectoryOwnership(File);
impl Drop for DirectoryOwnership {
    fn drop(&mut self) {
        // Closing our descriptor alone can leave an OS lock alive in an
        // unrelated child between fork and exec. End logical ownership now,
        // even if that child still holds the inherited open-file description.
        let _ = self.0.unlock();
    }
}
struct CancelVerification(Arc<AtomicBool>);
impl Drop for CancelVerification {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
pub struct LocalCas {
    dir: PathBuf,
    _ownership: DirectoryOwnership,
    source: Arc<dyn BlobSource>,
    config: CacheConfig,
    requests: Arc<Semaphore>,
    lookup_requests: Arc<Semaphore>,
    maintenance_requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    byte_units: u32,
    oversized_bytes: Arc<Semaphore>,
    oversized_byte_units: u32,
    flights: Mutex<HashMap<String, Arc<Flight>>>,
    files: Mutex<CacheFiles>,
    serial: AtomicU64,
    fetches: AtomicU64,
    verified_bytes: AtomicU64,
    verification_bytes: AtomicU64,
    hits: AtomicU64,
    timed_out_waiters: AtomicU64,
    timed_out_fetches: AtomicU64,
    admission_wait_ns: AtomicU64,
    fetch_ns: AtomicU64,
    sync_ns: AtomicU64,
    verification_ns: AtomicU64,
    publication_ns: AtomicU64,
    failures: FailureDiagnostics,
    evictions: AtomicU64,
    evicted_bytes: AtomicU64,
    recency_errors: AtomicU64,
    checkpoint: Mutex<Checkpoint>,
}
impl LocalCas {
    fn observe_error(&self, stage: FailureStage, error: &anyhow::Error, artifact: &Artifact) {
        let mut diagnostic = FailureDiagnostic::from_error(stage, error);
        diagnostic.expected_size = Some(artifact.size);
        diagnostic.digest_function = Some(artifact.digest_function);
        self.failures.record(diagnostic);
    }
    fn observe_io(&self, stage: FailureStage, error: &std::io::Error, artifact: &Artifact) {
        let mut diagnostic = FailureDiagnostic::from_io(stage, error);
        diagnostic.expected_size = Some(artifact.size);
        diagnostic.digest_function = Some(artifact.digest_function);
        self.failures.record(diagnostic);
    }
    fn observe_verification(
        &self,
        stage: FailureStage,
        artifact: &Artifact,
        size: u64,
        hash_matches: bool,
    ) {
        let class = if size != artifact.size {
            FailureClass::SizeMismatch
        } else if !hash_matches {
            FailureClass::DigestMismatch
        } else {
            return;
        };
        let mut diagnostic = FailureDiagnostic::new(stage, class);
        diagnostic.expected_size = Some(artifact.size);
        diagnostic.actual_size = Some(size);
        diagnostic.digest_function = Some(artifact.digest_function);
        self.failures.record(diagnostic);
    }
    /// Open an exclusively owned cache directory. Share the returned handle,
    /// rather than opening the same directory again, to reuse bytes across
    /// namespaces. The lock is retained by fetches and file leases too.
    pub fn open(
        dir: impl AsRef<Path>,
        source: Arc<dyn BlobSource>,
        config: CacheConfig,
    ) -> Result<Arc<Self>> {
        ensure!(
            config.max_bytes > 0
                && config.max_concurrent_fetches > 0
                && config.max_concurrent_fetches <= Semaphore::MAX_PERMITS.min(u32::MAX as usize)
                && config.max_inflight_bytes > 0
                && !config.operation_timeout.is_zero()
                && !config.fetch_timeout.is_zero()
                && config.operation_timeout <= Duration::from_secs(86_400)
                && config.fetch_timeout <= Duration::from_secs(86_400),
            "cache budgets must be positive and within supported limits"
        );
        fs::create_dir_all(dir.as_ref())?;
        let dir = dir.as_ref().canonicalize()?;
        let ownership = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(".owner.lock"))?;
        ownership
            .try_lock()
            .context("local CAS directory already owned by another handle or process")?;
        let ownership = DirectoryOwnership(ownership);
        let byte_units = (config.max_inflight_bytes / 4096).clamp(1, u32::MAX as u64) as u32;
        let oversized_byte_units =
            (config.max_oversized_inflight_bytes / 4096).clamp(1, u32::MAX as u64) as u32;
        let mut recency_errors = 0;
        let hints = match fs::read(dir.join("recency.json")) {
            Ok(bytes) => match serde_json::from_slice::<Recency>(&bytes) {
                Ok(hints) if hints.version == 1 => hints.entries,
                _ => {
                    recency_errors += 1;
                    HashMap::new()
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(_) => {
                recency_errors += 1;
                HashMap::new()
            }
        };
        let mut files = CacheFiles::default();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_file() {
                if path.extension().is_some_and(|e| e == "tmp") {
                    fs::remove_file(path)?;
                } else if entry.file_name().to_str().is_some_and(|name| {
                    name.len() == 64
                        && name
                            .bytes()
                            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                }) {
                    files.entries.insert(
                        path,
                        CacheEntry {
                            bytes: entry.metadata()?.len(),
                            pins: 0,
                            accessed: hints
                                .get(entry.file_name().to_str().unwrap())
                                .copied()
                                .unwrap_or(0),
                            verified: None,
                        },
                    );
                }
            }
        }
        // Sidecar ranks are untrusted hints, never byte-integrity authority.
        // Normalize arbitrary/duplicate/u64::MAX hints into a deterministic
        // total order, with filenames breaking ties and cold entries first.
        let mut order: Vec<_> = files
            .entries
            .iter()
            .map(|(p, e)| {
                (
                    hints.contains_key(p.file_name().unwrap().to_str().unwrap()),
                    e.accessed,
                    p.clone(),
                )
            })
            .collect();
        order.sort();
        for (_, _, path) in order {
            let rank = files.rank();
            let entry = files.entries.get_mut(&path).unwrap();
            entry.accessed = rank;
            files.resident = files
                .resident
                .checked_add(entry.bytes)
                .context("cache byte accounting overflow")?;
            files.evictable.insert((rank, path));
        }
        let mut evictions = 0;
        let mut evicted_bytes = 0;
        while files.resident > config.max_bytes {
            let (_, path) = files
                .evictable
                .first()
                .cloned()
                .context("cache size accounting mismatch")?;
            let bytes = files.entries[&path].bytes;
            fs::remove_file(&path)?;
            files.remove(&path);
            evictions += 1;
            evicted_bytes += bytes;
        }
        Ok(Arc::new(Self {
            dir,
            _ownership: ownership,
            source,
            requests: Arc::new(Semaphore::new(config.max_concurrent_fetches)),
            lookup_requests: Arc::new(Semaphore::new(config.max_concurrent_fetches)),
            maintenance_requests: Arc::new(Semaphore::new(1)),
            bytes: Arc::new(Semaphore::new(byte_units as usize)),
            byte_units,
            oversized_bytes: Arc::new(Semaphore::new(oversized_byte_units as usize)),
            oversized_byte_units,
            config,
            flights: Mutex::new(HashMap::new()),
            files: Mutex::new(files),
            serial: AtomicU64::new(1),
            fetches: AtomicU64::new(0),
            verified_bytes: AtomicU64::new(0),
            verification_bytes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            timed_out_waiters: AtomicU64::new(0),
            timed_out_fetches: AtomicU64::new(0),
            admission_wait_ns: AtomicU64::new(0),
            fetch_ns: AtomicU64::new(0),
            sync_ns: AtomicU64::new(0),
            verification_ns: AtomicU64::new(0),
            publication_ns: AtomicU64::new(0),
            failures: FailureDiagnostics::default(),
            evictions: AtomicU64::new(evictions),
            evicted_bytes: AtomicU64::new(evicted_bytes),
            recency_errors: AtomicU64::new(recency_errors),
            checkpoint: Mutex::new(Checkpoint {
                last: std::time::Instant::now(),
            }),
        }))
    }
    pub fn metrics(&self) -> CacheMetrics {
        let files = self.files.lock().unwrap();
        CacheMetrics {
            fetches: self.fetches.load(Ordering::Relaxed),
            verified_bytes: self.verified_bytes.load(Ordering::Relaxed),
            verification_bytes: self.verification_bytes.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            timed_out_waiters: self.timed_out_waiters.load(Ordering::Relaxed),
            timed_out_fetches: self.timed_out_fetches.load(Ordering::Relaxed),
            admission_wait_ns: self.admission_wait_ns.load(Ordering::Relaxed),
            fetch_ns: self.fetch_ns.load(Ordering::Relaxed),
            sync_ns: self.sync_ns.load(Ordering::Relaxed),
            verification_ns: self.verification_ns.load(Ordering::Relaxed),
            publication_ns: self.publication_ns.load(Ordering::Relaxed),
            resident_bytes: files.resident,
            reserved_bytes: files.reserved,
            evictions: self.evictions.load(Ordering::Relaxed),
            evicted_bytes: self.evicted_bytes.load(Ordering::Relaxed),
            recency_errors: self.recency_errors.load(Ordering::Relaxed),
            failures: self.failures.metrics(),
        }
    }
    /// Wait until admitted fetch and verification workers release ownership
    /// and permits across the fetch, foreground lookup, and maintenance lanes.
    /// Callers must first stop submitting work and cancel/drain their own
    /// producers. This does not close admission, cancel other readers, or wait
    /// for public blob leases. Unlike a waiter deadline, this proves worker
    /// teardown even when a fetch or verifier outlived its initiating caller.
    pub async fn wait_for_idle(&self) -> Result<()> {
        let _idle = self
            .requests
            .clone()
            .acquire_many_owned(self.config.max_concurrent_fetches as u32)
            .await
            .context("cache shutting down")?;
        let _lookups = self
            .lookup_requests
            .clone()
            .acquire_many_owned(self.config.max_concurrent_fetches as u32)
            .await
            .context("cache shutting down")?;
        let _maintenance = self
            .maintenance_requests
            .clone()
            .acquire_owned()
            .await
            .context("cache shutting down")?;
        Ok(())
    }
    /// Persist LRU hints explicitly. This does not mark cached bytes verified.
    /// Normal operations coalesce checkpoints to at most once per five seconds.
    pub fn flush_recency(&self) -> Result<()> {
        self.checkpoint_recency(true)
    }
    async fn checkpoint_if_due(self: &Arc<Self>) {
        // Never wait for a checkpoint writer on an async runtime thread. The
        // slow path is awaited in this existing admitted worker, not detached.
        let due = self
            .checkpoint
            .try_lock()
            .is_ok_and(|checkpoint| checkpoint.last.elapsed() >= Duration::from_secs(5));
        if !due {
            return;
        }
        let owner = self.clone();
        if tokio::task::spawn_blocking(move || {
            let result = owner.checkpoint_recency(false);
            // A task may expose its result before destroying its captures.
            drop(owner);
            result
        })
        .await
        .is_err()
        {
            self.recency_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn checkpoint_recency(&self, force: bool) -> Result<()> {
        let result = self.write_recency(force);
        if result.is_err() {
            self.recency_errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
    fn write_recency(&self, force: bool) -> Result<()> {
        // Serialize writers independently of the hot cache mutex. Only the
        // metadata snapshot is taken under that mutex; all IO happens outside.
        let mut checkpoint = self
            .checkpoint
            .lock()
            .map_err(|_| anyhow::anyhow!("recency checkpoint mutex poisoned"))?;
        if !force && checkpoint.last.elapsed() < Duration::from_secs(5) {
            return Ok(());
        }
        checkpoint.last = std::time::Instant::now();
        let (revision, entries) = {
            let files = self
                .files
                .lock()
                .map_err(|_| anyhow::anyhow!("cache metadata mutex poisoned"))?;
            if !files.dirty {
                return Ok(());
            }
            (
                files.revision,
                files
                    .entries
                    .iter()
                    .map(|(p, e)| {
                        (
                            p.file_name().unwrap().to_str().unwrap().to_owned(),
                            e.accessed,
                        )
                    })
                    .collect(),
            )
        };
        let bytes = serde_json::to_vec(&Recency {
            version: 1,
            entries,
        })?;
        let path = self.dir.join("recency.tmp");
        let _temporary = Temporary(path.clone());
        let mut target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        target.write_all(&bytes)?;
        target.sync_all()?;
        drop(target);
        fs::rename(path, self.dir.join("recency.json"))?;
        #[cfg(unix)]
        File::open(&self.dir)?.sync_all()?;
        let mut files = self
            .files
            .lock()
            .map_err(|_| anyhow::anyhow!("cache metadata mutex poisoned"))?;
        if files.revision == revision {
            files.dirty = false;
        }
        Ok(())
    }
    /// Canonical cache directory, for rejecting overlap with projected roots.
    /// The caller does not gain ownership or permission to mutate cache files.
    pub fn root_path(&self) -> &Path {
        &self.dir
    }
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }
    pub fn source(&self) -> Arc<dyn BlobSource> {
        self.source.clone()
    }
    /// Account for a namespace operation whose outer admission budget expired.
    pub fn record_waiter_timeout(&self) {
        self.timed_out_waiters.fetch_add(1, Ordering::Relaxed);
    }
    fn key(artifact: &Artifact) -> Result<String> {
        Ok(blake3::hash(&serde_json::to_vec(artifact)?)
            .to_hex()
            .to_string())
    }
    pub async fn content(self: Arc<Self>, artifact: Artifact) -> Result<Arc<CachedBlob>> {
        let source = self.source.clone();
        self.content_from(artifact, source).await
    }
    /// Fetch through an owned request-specific source (for example, a spool
    /// descriptor). Hits and coalesced callers do not invoke unused sources.
    pub async fn content_from(
        self: Arc<Self>,
        artifact: Artifact,
        source: Arc<dyn BlobSource>,
    ) -> Result<Arc<CachedBlob>> {
        let deadline = tokio::time::Instant::now() + self.config.operation_timeout;
        let observed = artifact.clone();
        let result =
            match tokio::time::timeout_at(deadline, self.clone().content_inner(artifact, source))
                .await
            {
                Ok(result) if tokio::time::Instant::now() < deadline => result,
                _ => {
                    self.timed_out_waiters.fetch_add(1, Ordering::Relaxed);
                    Err(CacheTimeoutError.into())
                }
            };
        if result.is_ok() {
            self.source.accessed(&observed, Arc::downgrade(&self));
        }
        result
    }
    async fn content_inner(
        self: Arc<Self>,
        artifact: Artifact,
        source: Arc<dyn BlobSource>,
    ) -> Result<Arc<CachedBlob>> {
        artifact.validate()?;
        let limit = self.config.readable_limit();
        if artifact.size > limit {
            return Err(CacheLimitError {
                size: artifact.size,
                limit,
            }
            .into());
        }
        let key = Self::key(&artifact)?;
        let existing = self.flights.lock().unwrap().get(&key).cloned();
        if let Some(flight) = existing {
            return flight.wait().await;
        }
        // Admission happens before spawning; queued callers consume no detached
        // tasks or temporary files. Only one admitted task exists per key.
        let admission = ElapsedTimer {
            elapsed: &self.admission_wait_ns,
            started: TimerStart::Runtime(tokio::time::Instant::now()),
        };
        let request = self
            .requests
            .clone()
            .acquire_owned()
            .await
            .context("cache shutting down")?;
        let request = Arc::new(request);
        drop(admission);
        let (flight, start) = {
            let mut flights = self.flights.lock().unwrap();
            if let Some(flight) = flights.get(&key) {
                (flight.clone(), false)
            } else {
                let flight = Arc::new(Flight {
                    result: Mutex::new(None),
                    complete: Arc::new(AtomicBool::new(false)),
                    ready: Arc::new(tokio::sync::Notify::new()),
                });
                flights.insert(key.clone(), flight.clone());
                (flight, true)
            }
        };
        if start {
            let cache = self.clone();
            let flight = flight.clone();
            let fetch_deadline = tokio::time::Instant::now() + self.config.fetch_timeout;
            tokio::spawn(async move {
                let request = request;
                let worker = cache.clone();
                let worker_request = request.clone();
                // The deadline is inside the worker: timing out a JoinHandle
                // alone would detach work that could publish after expiry.
                let result = match tokio::spawn(async move {
                    let result = match tokio::time::timeout_at(
                        fetch_deadline,
                        worker.clone().obtain(
                            &artifact,
                            fetch_deadline,
                            source.clone(),
                            worker_request,
                        ),
                    )
                    .await
                    {
                        Ok(result) if tokio::time::Instant::now() < fetch_deadline => result,
                        _ => Err(CacheTimeoutError.into()),
                    };
                    if result
                        .as_ref()
                        .is_err_and(|error| error.downcast_ref::<CacheTimeoutError>().is_some())
                    {
                        worker.timed_out_fetches.fetch_add(1, Ordering::Relaxed);
                    }
                    drop(source);
                    worker.checkpoint_if_due().await;
                    // Tokio may expose a task result before dropping its
                    // completed future. Do not leave ownership in that frame.
                    drop(worker);
                    result
                })
                .await
                {
                    Ok(result) => result.map_err(FlightFailure::from_error),
                    Err(error) => Err(FlightFailure::Other(Arc::new(
                        anyhow::Error::new(error).context("cache worker failed"),
                    ))),
                };
                let complete = flight.complete.clone();
                let ready = flight.ready.clone();
                *flight.result.lock().unwrap() = Some(result);
                cache.flights.lock().unwrap().remove(&key);
                drop(cache);
                drop(flight);
                // The all-permits idle barrier may return as soon as this
                // permit is released; worker-owned cache/leases must be gone.
                drop(request);
                // These two handles contain no cache or blob references. No
                // worker ownership may remain when a waiter observes complete.
                complete.store(true, Ordering::Release);
                ready.notify_waiters();
            });
        } else {
            drop(request);
        }
        flight.wait().await
    }
    fn reserve(self: &Arc<Self>, size: u64) -> Result<Reservation> {
        let mut state = self.files.lock().unwrap();
        while state
            .resident
            .checked_add(state.reserved)
            .and_then(|used| used.checked_add(size))
            .is_none_or(|total| total > self.config.max_bytes)
        {
            let (_, path) = state
                .evictable
                .first()
                .cloned()
                .context("disk cache budget occupied by pinned content or active downloads")?;
            let bytes = state.entries[&path].bytes;
            fs::remove_file(&path)?;
            state.remove(&path);
            self.evictions.fetch_add(1, Ordering::Relaxed);
            self.evicted_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        state.reserved = state
            .reserved
            .checked_add(size)
            .context("cache reservation overflow")?;
        Ok(Reservation {
            cache: Arc::downgrade(self),
            bytes: size,
        })
    }
    async fn verify(
        self: &Arc<Self>,
        path: &Path,
        function: i32,
        deadline: tokio::time::Instant,
        request: Arc<OwnedSemaphorePermit>,
        fetch_context: Option<FetchContext>,
    ) -> Result<(String, u64)> {
        let path = path.to_owned();
        // Tuple fields drop left-to-right, including during unwinding: cache
        // ownership ends before this verifier releases its fetch, foreground
        // lookup, or maintenance lane permit clone.
        let ownership = (fetch_context, self.clone(), request);
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelVerification(cancelled.clone());
        let result = tokio::task::spawn_blocking(move || {
            let result = {
                let _verification = ElapsedTimer::start(&ownership.1.verification_ns);
                hash_file_until(&path, function, Some(deadline), &cancelled)
            };
            if let Err(error) = &result {
                let mut diagnostic =
                    FailureDiagnostic::from_error(FailureStage::CacheVerificationRead, error);
                diagnostic.digest_function = Some(function);
                ownership.1.failures.record(diagnostic);
            }
            drop(ownership);
            result
        })
        .await
        .context("verification task failed")??;
        self.verification_bytes
            .fetch_add(result.1, Ordering::Relaxed);
        Ok(result)
    }
    /// Return an existing verified lease without joining or creating a hydration
    /// flight. A local-only consumer cannot publish its source's miss/failure to
    /// another consumer that can hydrate the same artifact from upstream.
    /// A separate bounded foreground lane prevents upstream fetches from
    /// blocking resident lookups; this path never fetches missing content.
    pub async fn cached_content(
        self: Arc<Self>,
        artifact: Artifact,
    ) -> Result<Option<Arc<CachedBlob>>> {
        let deadline = tokio::time::Instant::now() + self.config.operation_timeout;
        let result = self
            .clone()
            .cached_content_admitted(artifact.clone(), self.lookup_requests.clone(), deadline)
            .await;
        if result.as_ref().is_ok_and(|blob| blob.is_some()) {
            self.source.accessed(&artifact, Arc::downgrade(&self));
        }
        result
    }
    /// Verify and lease resident bytes without hydration or a user-access
    /// notification. Background uploads must not manufacture new retention use.
    /// A separate single-slot lane preserves foreground admission. The caller's
    /// deadline includes admission and cancellable verification.
    pub async fn cached_content_for_maintenance(
        self: Arc<Self>,
        artifact: Artifact,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Arc<CachedBlob>>> {
        let deadline = deadline.min(tokio::time::Instant::now() + self.config.operation_timeout);
        let admission = self.maintenance_requests.clone();
        self.cached_content_admitted(artifact, admission, deadline)
            .await
    }
    async fn cached_content_admitted(
        self: Arc<Self>,
        artifact: Artifact,
        admission: Arc<Semaphore>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Arc<CachedBlob>>> {
        artifact.validate()?;
        let path = self.dir.join(Self::key(&artifact)?);
        if tokio::time::Instant::now() >= deadline {
            return Err(CacheTimeoutError.into());
        }
        // A known miss needs no admission, filesystem I/O, or detached work.
        // Entries may change while admission is queued: cached_inner rechecks
        // and pins the resident entry before inspecting its fingerprint.
        if !self.files.lock().unwrap().entries.contains_key(&path) {
            return Ok(None);
        }
        tokio::time::timeout_at(deadline, async {
            let request = Arc::new(
                admission
                    .acquire_owned()
                    .await
                    .context("cache shutting down")?,
            );
            self.clone()
                .cached_inner(&artifact, deadline, request)
                .await
        })
        .await
        .map_err(|_| anyhow::Error::from(CacheTimeoutError))?
    }
    async fn cached_inner(
        self: Arc<Self>,
        artifact: &Artifact,
        deadline: tokio::time::Instant,
        request: Arc<OwnedSemaphorePermit>,
    ) -> Result<Option<Arc<CachedBlob>>> {
        let path = self.dir.join(Self::key(artifact)?);
        let verified = {
            let mut state = self.files.lock().unwrap();
            if let Some(entry) = state.entries.get_mut(&path) {
                let rank = entry.accessed;
                let known = entry.verified.clone();
                entry.pins += 1;
                state.evictable.remove(&(rank, path.clone()));
                Some(known)
            } else {
                None
            }
        };
        if let Some(known) = verified {
            let lease = Arc::new(CachedBlob {
                path: path.clone(),
                cache: self.clone(),
            });
            // Hash only once per process/fingerprint, and never under the global
            // cache mutex. Range reads of a large verified blob are O(range).
            let current = fingerprint(&path).inspect_err(|error| {
                self.observe_error(FailureStage::CacheLookupFingerprint, error, artifact);
            });
            let valid = match current.as_ref() {
                Ok(current) if known.as_ref() == Some(current) => true,
                Ok(before) => match self
                    .verify(
                        &path,
                        artifact.digest_function,
                        deadline,
                        request.clone(),
                        None,
                    )
                    .await
                {
                    Ok((hash, size)) => {
                        self.observe_verification(
                            FailureStage::CacheLookupVerification,
                            artifact,
                            size,
                            hash == artifact.hash,
                        );
                        if hash == artifact.hash && size == artifact.size {
                            let after = fingerprint(&path).inspect_err(|error| {
                                self.observe_error(
                                    FailureStage::CacheLookupFingerprint,
                                    error,
                                    artifact,
                                );
                            });
                            let same = after.as_ref().ok() == Some(before);
                            if !same && after.is_ok() {
                                let mut diagnostic = FailureDiagnostic::new(
                                    FailureStage::CacheLookupFingerprint,
                                    FailureClass::FingerprintChanged,
                                );
                                diagnostic.digest_function = Some(artifact.digest_function);
                                self.failures.record(diagnostic);
                            }
                            same
                        } else {
                            false
                        }
                    }
                    Err(error) => {
                        self.observe_error(FailureStage::CacheLookupVerification, &error, artifact);
                        false
                    }
                },
                Err(_) => false,
            };
            if valid {
                if tokio::time::Instant::now() >= deadline {
                    return Err(CacheTimeoutError.into());
                }
                let mut state = self.files.lock().unwrap();
                let rank = state.rank();
                let entry = state
                    .entries
                    .get_mut(&path)
                    .context("pinned cache entry disappeared")?;
                entry.verified = current.ok();
                entry.accessed = rank;
                state.changed();
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(lease));
            }
            drop(lease);
            let mut state = self.files.lock().unwrap();
            ensure!(
                state.entries.get(&path).is_none_or(|e| e.pins == 0),
                "pinned cache content corrupted"
            );
            fs::remove_file(&path).or_else(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(e)
                }
            })?;
            state.remove(&path);
        }
        Ok(None)
    }
    async fn obtain(
        self: Arc<Self>,
        artifact: &Artifact,
        deadline: tokio::time::Instant,
        source: Arc<dyn BlobSource>,
        request: Arc<OwnedSemaphorePermit>,
    ) -> Result<Arc<CachedBlob>> {
        if let Some(blob) = self
            .clone()
            .cached_inner(artifact, deadline, request.clone())
            .await?
        {
            return Ok(blob);
        }
        let path = self.dir.join(Self::key(artifact)?);
        // Large streamed objects get their own explicitly budgeted lane. They
        // do not consume the normal lane's byte permits, but share the global
        // request cap and the on-disk reservation accounting.
        let (budget, units) = if artifact.size > self.config.max_inflight_bytes {
            (&self.oversized_bytes, self.oversized_byte_units)
        } else {
            (&self.bytes, self.byte_units)
        };
        let units = artifact.size.div_ceil(4096).clamp(1, units as u64) as u32;
        let byte_permit = budget
            .clone()
            .acquire_many_owned(units)
            .await
            .context("cache shutting down")?;
        let reservation = Arc::new(Mutex::new(self.reserve(artifact.size)?));
        let temp_path = self.dir.join(format!(
            "{}-{}.tmp",
            std::process::id(),
            self.serial.fetch_add(1, Ordering::Relaxed)
        ));
        let fetch_context = FetchContext {
            _resources: Arc::new(FetchResources {
                _temporary: Temporary(temp_path.clone()),
                _reservation: reservation.clone(),
                _bytes: byte_permit,
                _request: request.clone(),
                _cache: self.clone(),
            }),
        };
        let mut target = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .inspect_err(|error| self.observe_io(FailureStage::CacheCreate, error, artifact))?;
        self.fetches.fetch_add(1, Ordering::Relaxed);
        let fetch_started = std::time::Instant::now();
        {
            let _fetch = ElapsedTimer::start(&self.fetch_ns);
            source
                .fetch_with_context(artifact, &mut target, fetch_context.clone())
                .await
                .inspect_err(|error| {
                    self.observe_error(FailureStage::CacheFetch, error, artifact)
                })?;
        }
        {
            let _sync = ElapsedTimer::start(&self.sync_ns);
            target
                .sync_all()
                .inspect_err(|error| self.observe_io(FailureStage::CacheSync, error, artifact))?;
        }
        drop(target);
        let (hash, size) = self
            .verify(
                &temp_path,
                artifact.digest_function,
                deadline,
                request.clone(),
                Some(fetch_context.clone()),
            )
            .await?;
        self.observe_verification(
            FailureStage::CacheVerification,
            artifact,
            size,
            hash == artifact.hash,
        );
        ensure!(
            size == artifact.size && hash == artifact.hash,
            "CAS blob digest or size mismatch"
        );
        if tokio::time::Instant::now() >= deadline {
            return Err(CacheTimeoutError.into());
        }
        let verified = {
            let _publication = ElapsedTimer::start(&self.publication_ns);
            fs::rename(&temp_path, &path).inspect_err(|error| {
                self.observe_io(FailureStage::CachePublicationRename, error, artifact);
            })?;
            #[cfg(unix)]
            if let Err(error) = File::open(&self.dir).and_then(|dir| dir.sync_all()) {
                self.observe_io(FailureStage::CachePublicationSync, &error, artifact);
                let _ = fs::remove_file(&path);
                return Err(error.into());
            }
            fingerprint(&path).inspect_err(|error| {
                self.observe_error(FailureStage::CachePublicationFingerprint, error, artifact);
            })?
        };
        {
            let mut state = self.files.lock().unwrap();
            let rank = state.rank();
            state.entries.insert(
                path.clone(),
                CacheEntry {
                    bytes: size,
                    pins: 1,
                    accessed: rank,
                    verified: Some(verified),
                },
            );
            // Convert reserved bytes to resident bytes in one transaction.
            let mut reservation = reservation.lock().unwrap();
            state.reserved -= reservation.bytes;
            reservation.bytes = 0;
            state.resident += size;
            state.changed();
        }
        self.verified_bytes.fetch_add(size, Ordering::Relaxed);
        source.verified(artifact, fetch_started);
        Ok(Arc::new(CachedBlob {
            path,
            cache: self.clone(),
        }))
    }
}

impl Drop for LocalCas {
    fn drop(&mut self) {
        // No autonomous checkpoint task retains the last owner. Keep the
        // directory lock through this best-effort final metadata publication.
        let _ = self.checkpoint_recency(true);
    }
}

#[cfg(test)]
mod tests;
