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
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::Semaphore;
#[derive(Clone, Debug)]
pub struct CacheLimitError {
    pub size: u64,
    pub limit: u64,
}
impl std::fmt::Display for CacheLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "artifact size {} exceeds hydration budget {}",
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
#[async_trait]
pub trait BlobSource: Send + Sync {
    /// Stream at most artifact.size bytes; reject an overlong response before
    /// writing it. The cache reserves this declared disk/transfer budget and
    /// independently verifies both complete hash and exact final size.
    async fn fetch(&self, artifact: &Artifact, target: &mut File) -> Result<()>;
    async fn missing(&self, artifacts: &[Artifact]) -> Result<Vec<Artifact>>;
}
#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub max_bytes: u64,
    pub max_concurrent_fetches: usize,
    pub max_inflight_bytes: u64,
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
            operation_timeout: Duration::from_secs(120),
            fetch_timeout: Duration::from_secs(120),
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct CacheMetrics {
    pub fetches: u64,
    pub verified_bytes: u64,
    pub verification_bytes: u64,
    pub hits: u64,
    pub timed_out_waiters: u64,
    pub timed_out_fetches: u64,
    pub admission_wait_ns: u64,
}
fn hash_file_until(
    path: &Path,
    function: i32,
    deadline: Option<tokio::time::Instant>,
) -> Result<(String, u64)> {
    let mut file = File::open(path)?;
    let mut sha = Sha256::new();
    let mut blake = blake3::Hasher::new();
    ensure!(matches!(function, 1 | 9), "unsupported digest function");
    let mut size = 0;
    let mut buf = [0u8; 128 * 1024];
    loop {
        // Blocking OS reads cannot be preempted, but an expired verification
        // must stop between chunks rather than continue detached over the file.
        if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
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
    reserved: u64,
}
#[derive(Clone)]
enum FlightFailure {
    Timeout,
    Other(String),
}
impl FlightFailure {
    fn from_error(error: anyhow::Error) -> Self {
        if error.downcast_ref::<CacheTimeoutError>().is_some() {
            Self::Timeout
        } else {
            Self::Other(format!("{error:#}"))
        }
    }
    fn into_error(self) -> anyhow::Error {
        match self {
            Self::Timeout => CacheTimeoutError.into(),
            Self::Other(message) => anyhow::Error::msg(message),
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
struct AdmissionTimer<'a> {
    elapsed: &'a AtomicU64,
    started: tokio::time::Instant,
}
impl Drop for AdmissionTimer<'_> {
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
pub struct LocalCas {
    dir: PathBuf,
    _ownership: DirectoryOwnership,
    source: Arc<dyn BlobSource>,
    config: CacheConfig,
    requests: Arc<Semaphore>,
    bytes: Semaphore,
    byte_units: u32,
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
}
impl LocalCas {
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
                && config.max_concurrent_fetches <= Semaphore::MAX_PERMITS
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
                            accessed: 0,
                            verified: None,
                        },
                    );
                }
            }
        }
        // A reduced quota on restart also applies to previously cached bytes.
        // No leases exist yet, so every existing clean cache file is eligible.
        let mut retained = files.entries.values().map(|e| e.bytes).sum::<u64>();
        while retained > config.max_bytes {
            let (path, size) = files
                .entries
                .iter()
                .next()
                .map(|(p, e)| (p.clone(), e.bytes))
                .context("cache size accounting mismatch")?;
            fs::remove_file(&path)?;
            files.entries.remove(&path);
            retained = retained.saturating_sub(size);
        }
        Ok(Arc::new(Self {
            dir,
            _ownership: ownership,
            source,
            requests: Arc::new(Semaphore::new(config.max_concurrent_fetches)),
            bytes: Semaphore::new(byte_units as usize),
            byte_units,
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
        }))
    }
    pub fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            fetches: self.fetches.load(Ordering::Relaxed),
            verified_bytes: self.verified_bytes.load(Ordering::Relaxed),
            verification_bytes: self.verification_bytes.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            timed_out_waiters: self.timed_out_waiters.load(Ordering::Relaxed),
            timed_out_fetches: self.timed_out_fetches.load(Ordering::Relaxed),
            admission_wait_ns: self.admission_wait_ns.load(Ordering::Relaxed),
        }
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
        let deadline = tokio::time::Instant::now() + self.config.operation_timeout;
        match tokio::time::timeout_at(deadline, self.clone().content_inner(artifact)).await {
            Ok(result) if tokio::time::Instant::now() < deadline => result,
            _ => {
                self.timed_out_waiters.fetch_add(1, Ordering::Relaxed);
                Err(CacheTimeoutError.into())
            }
        }
    }
    async fn content_inner(self: Arc<Self>, artifact: Artifact) -> Result<Arc<CachedBlob>> {
        artifact.validate()?;
        let limit = self.config.max_bytes.min(self.config.max_inflight_bytes);
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
        let admission = AdmissionTimer {
            elapsed: &self.admission_wait_ns,
            started: tokio::time::Instant::now(),
        };
        let request = self
            .requests
            .clone()
            .acquire_owned()
            .await
            .context("cache shutting down")?;
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
                // The deadline is inside the worker: timing out a JoinHandle
                // alone would detach work that could publish after expiry.
                let result = match tokio::spawn(async move {
                    let result = match tokio::time::timeout_at(
                        fetch_deadline,
                        worker.clone().obtain(&artifact, fetch_deadline),
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
                    // Tokio may expose a task result before dropping its
                    // completed future. Do not leave ownership in that frame.
                    drop(worker);
                    result
                })
                .await
                {
                    Ok(result) => result.map_err(FlightFailure::from_error),
                    Err(error) => Err(FlightFailure::Other(format!(
                        "cache worker failed: {error}"
                    ))),
                };
                let complete = flight.complete.clone();
                let ready = flight.ready.clone();
                *flight.result.lock().unwrap() = Some(result);
                cache.flights.lock().unwrap().remove(&key);
                drop(cache);
                drop(request);
                drop(flight);
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
        let mut total = state.entries.values().map(|e| e.bytes).sum::<u64>();
        while total.saturating_add(state.reserved).saturating_add(size) > self.config.max_bytes {
            let victim = state
                .entries
                .iter()
                .filter(|(_, e)| e.pins == 0)
                .min_by_key(|(_, e)| e.accessed)
                .map(|(p, e)| (p.clone(), e.bytes))
                .context("disk cache budget occupied by pinned content or active downloads")?;
            fs::remove_file(&victim.0)?;
            state.entries.remove(&victim.0);
            total = total.saturating_sub(victim.1);
        }
        state.reserved += size;
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
    ) -> Result<(String, u64)> {
        let path = path.to_owned();
        let ownership = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _ownership = ownership;
            hash_file_until(&path, function, Some(deadline))
        })
        .await
        .context("verification task failed")??;
        self.verification_bytes
            .fetch_add(result.1, Ordering::Relaxed);
        Ok(result)
    }
    async fn obtain(
        self: Arc<Self>,
        artifact: &Artifact,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<CachedBlob>> {
        let path = self.dir.join(Self::key(artifact)?);
        let verified = {
            let mut state = self.files.lock().unwrap();
            if let Some(entry) = state.entries.get_mut(&path) {
                entry.pins += 1;
                Some(entry.verified.clone())
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
            let current = fingerprint(&path);
            let valid = match current.as_ref() {
                Ok(current) if known.as_ref() == Some(current) => true,
                Ok(before) => match self.verify(&path, artifact.digest_function, deadline).await {
                    Ok((hash, size)) if hash == artifact.hash && size == artifact.size => {
                        fingerprint(&path).as_ref().ok() == Some(before)
                    }
                    _ => false,
                },
                Err(_) => false,
            };
            if valid {
                if tokio::time::Instant::now() >= deadline {
                    return Err(CacheTimeoutError.into());
                }
                let mut state = self.files.lock().unwrap();
                let entry = state
                    .entries
                    .get_mut(&path)
                    .context("pinned cache entry disappeared")?;
                entry.verified = current.ok();
                entry.accessed = self.serial.fetch_add(1, Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(lease);
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
            state.entries.remove(&path);
        }
        let units = artifact
            .size
            .div_ceil(4096)
            .clamp(1, self.byte_units as u64) as u32;
        let _bytes = self
            .bytes
            .acquire_many(units)
            .await
            .context("cache shutting down")?;
        let _reservation = self.reserve(artifact.size)?;
        let temp_path = self.dir.join(format!(
            "{}-{}.tmp",
            std::process::id(),
            self.serial.fetch_add(1, Ordering::Relaxed)
        ));
        let _temp = Temporary(temp_path.clone());
        let mut target = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        self.fetches.fetch_add(1, Ordering::Relaxed);
        self.source.fetch(artifact, &mut target).await?;
        target.sync_all()?;
        drop(target);
        let (hash, size) = self
            .verify(&temp_path, artifact.digest_function, deadline)
            .await?;
        ensure!(
            size == artifact.size && hash == artifact.hash,
            "CAS blob digest or size mismatch"
        );
        if tokio::time::Instant::now() >= deadline {
            return Err(CacheTimeoutError.into());
        }
        fs::rename(&temp_path, &path)?;
        #[cfg(unix)]
        if let Err(error) = File::open(&self.dir).and_then(|dir| dir.sync_all()) {
            let _ = fs::remove_file(&path);
            return Err(error.into());
        }
        let verified = fingerprint(&path)?;
        self.files.lock().unwrap().entries.insert(
            path.clone(),
            CacheEntry {
                bytes: size,
                pins: 1,
                accessed: self.serial.fetch_add(1, Ordering::Relaxed),
                verified: Some(verified),
            },
        );
        self.verified_bytes.fetch_add(size, Ordering::Relaxed);
        Ok(Arc::new(CachedBlob {
            path,
            cache: self.clone(),
        }))
    }
}

#[cfg(test)]
mod tests;
