//! Bounded, process-local index of complete generated smart-HTTP fetch responses.
use anyhow::{Result, ensure};
use axum::{
    body::Body,
    http::{HeaderMap, StatusCode, Version},
    response::Response,
};
use bytes::Bytes;
use futures_core::Stream;
use serde::Serialize;
use std::{
    collections::HashMap,
    future::poll_fn,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, mpsc, watch},
};

#[derive(Clone, Debug)]
pub struct PackCacheConfig {
    pub max_bytes: u64,
    pub max_entry_bytes: u64,
    pub max_entries: usize,
    pub ttl: Duration,
}
impl Default for PackCacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024 * 1024,
            max_entry_bytes: 64 * 1024 * 1024,
            max_entries: 1024,
            ttl: Duration::from_secs(300),
        }
    }
}
#[derive(Default, Serialize)]
pub struct PackCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub coalesced: u64,
    pub bypasses: u64,
    pub generated_bytes: u64,
    pub served_bytes: u64,
    pub evictions: u64,
    pub errors: u64,
    pub current_bytes: u64,
    pub entries: usize,
    pub inflight: usize,
}
#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    coalesced: AtomicU64,
    bypasses: AtomicU64,
    generated_bytes: AtomicU64,
    served_bytes: AtomicU64,
    evictions: AtomicU64,
    errors: AtomicU64,
}
struct Entry {
    path: PathBuf,
    size: u64,
    created: Instant,
    status: StatusCode,
    version: Version,
    headers: HeaderMap,
}
struct Slot {
    entry: Arc<Entry>,
    used: Instant,
}
struct Pending {
    id: u64,
    result: watch::Sender<Option<Arc<Entry>>>,
}
#[derive(Default)]
struct State {
    entries: HashMap<String, Slot>,
    pending: HashMap<String, Pending>,
    bytes: u64,
    reserved: u64,
    next_id: u64,
}
pub struct PackCache {
    _owner: Arc<std::fs::File>,
    dir: PathBuf,
    config: PackCacheConfig,
    state: Mutex<State>,
    counters: Counters,
}
pub enum Lookup {
    Hit(CachedResponse),
    Produce(ProducerPermit),
    Wait(Flight),
    Bypass,
}
pub struct CachedResponse {
    entry: Arc<Entry>,
    cache: Arc<PackCache>,
}
pub struct Flight {
    result: watch::Receiver<Option<Arc<Entry>>>,
    cache: Arc<PackCache>,
}
pub struct ProducerPermit {
    cache: Arc<PackCache>,
    key: String,
    id: u64,
    done: bool,
    temporary: Option<PathBuf>,
}

impl PackCache {
    pub fn new(
        root: PathBuf,
        config: PackCacheConfig,
        owner: Arc<std::fs::File>,
    ) -> Result<Arc<Self>> {
        ensure!(
            config.max_bytes == 0 || config.max_entry_bytes > 0,
            "pack cache entry limit must be positive"
        );
        let dir = root.join("generated-packs/schema-v1");
        std::fs::create_dir_all(&dir)?;
        // The enclosing proxy cache lock guarantees one owner. Only remove our own
        // schema's recognized generated files; never walk arbitrary directories.
        for item in std::fs::read_dir(&dir)? {
            let item = item?;
            let name = item.file_name();
            let name = name.to_string_lossy();
            let recognized = name
                .strip_suffix(".tmp")
                .or_else(|| name.strip_suffix(".response"))
                .is_some_and(|stem| {
                    stem.len() == 64 && stem.bytes().all(|b| b.is_ascii_hexdigit())
                });
            if recognized && item.file_type()?.is_file() {
                std::fs::remove_file(item.path())?;
            }
        }
        Ok(Arc::new(Self {
            _owner: owner,
            dir,
            config,
            state: Mutex::new(State::default()),
            counters: Counters::default(),
        }))
    }
    pub async fn lookup(self: &Arc<Self>, key: &str) -> Lookup {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        // Expired pinned entries remain accounted for until all readers release them.
        let expired: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, s)| {
                now.duration_since(s.entry.created) >= self.config.ttl
                    && Arc::strong_count(&s.entry) == 1
            })
            .map(|(k, _)| k.clone())
            .collect();
        for k in expired {
            let _ = self.evict(&mut state, &k);
        }
        if let Some(slot) = state.entries.get_mut(key) {
            if now.duration_since(slot.entry.created) < self.config.ttl {
                slot.used = now;
                self.counters.hits.fetch_add(1, Ordering::Relaxed);
                return Lookup::Hit(CachedResponse {
                    entry: slot.entry.clone(),
                    cache: self.clone(),
                });
            }
            // An expired entry still leased to readers cannot be overwritten.
            self.counters.bypasses.fetch_add(1, Ordering::Relaxed);
            return Lookup::Bypass;
        }
        if let Some(pending) = state.pending.get(key) {
            self.counters.coalesced.fetch_add(1, Ordering::Relaxed);
            return Lookup::Wait(Flight {
                result: pending.result.subscribe(),
                cache: self.clone(),
            });
        }
        let reserve = self.config.max_entry_bytes;
        if self.config.max_bytes == 0
            || self.config.max_entries == 0
            || reserve > self.config.max_bytes
        {
            self.counters.bypasses.fetch_add(1, Ordering::Relaxed);
            return Lookup::Bypass;
        }
        while state
            .bytes
            .saturating_add(state.reserved)
            .saturating_add(reserve)
            > self.config.max_bytes
            || state.entries.len() + state.pending.len() >= self.config.max_entries
        {
            let oldest = state
                .entries
                .iter()
                .filter(|(_, s)| Arc::strong_count(&s.entry) == 1)
                .min_by_key(|(_, s)| s.used)
                .map(|(k, _)| k.clone());
            if !oldest.is_some_and(|k| self.evict(&mut state, &k)) {
                self.counters.bypasses.fetch_add(1, Ordering::Relaxed);
                return Lookup::Bypass;
            }
        }
        state.next_id += 1;
        let id = state.next_id;
        let (result, _) = watch::channel(None);
        state.pending.insert(key.to_owned(), Pending { id, result });
        state.reserved += reserve;
        self.counters.misses.fetch_add(1, Ordering::Relaxed);
        Lookup::Produce(ProducerPermit {
            cache: self.clone(),
            key: key.to_owned(),
            id,
            done: false,
            temporary: None,
        })
    }
    fn evict(&self, state: &mut State, key: &str) -> bool {
        if let Some(slot) = state.entries.get(key)
            && std::fs::remove_file(&slot.entry.path).is_err()
        {
            return false;
        }
        if let Some(slot) = state.entries.remove(key) {
            state.bytes -= slot.entry.size;
            self.counters.evictions.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }
    pub fn snapshot(&self) -> PackCacheStats {
        let s = self.state.lock().unwrap();
        let c = &self.counters;
        PackCacheStats {
            hits: c.hits.load(Ordering::Relaxed),
            misses: c.misses.load(Ordering::Relaxed),
            coalesced: c.coalesced.load(Ordering::Relaxed),
            bypasses: c.bypasses.load(Ordering::Relaxed),
            generated_bytes: c.generated_bytes.load(Ordering::Relaxed),
            served_bytes: c.served_bytes.load(Ordering::Relaxed),
            evictions: c.evictions.load(Ordering::Relaxed),
            errors: c.errors.load(Ordering::Relaxed),
            current_bytes: s.bytes + s.reserved,
            entries: s.entries.len(),
            inflight: s.pending.len(),
        }
    }
}
impl Flight {
    pub async fn wait(mut self) -> Option<CachedResponse> {
        loop {
            if let Some(entry) = self.result.borrow_and_update().clone() {
                return Some(CachedResponse {
                    entry,
                    cache: self.cache.clone(),
                });
            }
            if self.result.changed().await.is_err() {
                return None;
            }
        }
    }
}
impl CachedResponse {
    pub fn response(self, cache_status: &'static str) -> Response {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = self.entry.status;
        *response.version_mut() = self.entry.version;
        *response.headers_mut() = self.entry.headers.clone();
        response
            .headers_mut()
            .insert("x-git-proxy-pack-cache", cache_status.parse().unwrap());
        let body = async_stream::try_stream! {
            // Keep the lease for the entire stream, including before the first poll.
            let lease = self;
            let mut file = tokio::fs::File::open(&lease.entry.path).await?;
            loop {
                let mut buffer = vec![0u8; 64 * 1024];
                let n = file.read(&mut buffer).await?;
                if n == 0 { break; }
                buffer.truncate(n);
                lease.cache.counters.served_bytes.fetch_add(n as u64, Ordering::Relaxed);
                yield Bytes::from(buffer);
            }
            drop(lease);
        };
        *response.body_mut() = Body::from_stream(Box::pin(body)
            as std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>);
        response
    }
}
impl ProducerPermit {
    fn complete(&mut self, entry: Option<Arc<Entry>>) {
        if self.done {
            return;
        }
        let mut state = self.cache.state.lock().unwrap();
        if state
            .pending
            .get(&self.key)
            .is_some_and(|p| p.id == self.id)
        {
            let pending = state.pending.remove(&self.key).unwrap();
            state.reserved -= self.cache.config.max_entry_bytes;
            // On an unlink failure retain a conservative reservation permanently.
            // Losing capacity is safer than silently exceeding the disk quota.
            if entry.is_none()
                && let Some(path) = &self.temporary
            {
                match std::fs::remove_file(path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => {
                        state.bytes += self.cache.config.max_entry_bytes;
                    }
                }
            }
            if let Some(entry) = &entry {
                state.bytes += entry.size;
                state.entries.insert(
                    self.key.clone(),
                    Slot {
                        entry: entry.clone(),
                        used: Instant::now(),
                    },
                );
            }
            let _ = pending.result.send(entry);
        }
        self.done = true;
    }
    /// Stream the generated response while filling the cache until `deadline`.
    /// The caller supplies its native Git operation deadline and HTTP slot; the
    /// producer retains the slot until completion even if its reader disconnects.
    pub fn stream_with_deadline(
        mut self,
        response: Response,
        http_permit: OwnedSemaphorePermit,
        deadline: tokio::time::Instant,
    ) -> Response {
        let (mut parts, body) = response.into_parts();
        parts
            .headers
            .insert("x-git-proxy-pack-cache", "MISS".parse().unwrap());
        let status = parts.status;
        let version = parts.version;
        let headers = parts.headers.clone();
        let (tx, mut rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(4);
        let failed = Arc::new(AtomicBool::new(false));
        let failure = failed.clone();
        let cache = self.cache.clone();
        tokio::spawn(async move {
            // Keep the channel open until timeout handling records its outcome.
            // Otherwise the reader could observe EOF before the failure flag.
            let completion = tx.clone();
            let producer_failure = failure.clone();
            let fill = async move {
                let _http_permit = http_permit;
                let mut hasher = blake3::Hasher::new();
                hasher.update(self.key.as_bytes());
                hasher.update(&self.id.to_le_bytes());
                let name = hasher.finalize().to_hex().to_string();
                let temporary = self.cache.dir.join(format!("{name}.tmp"));
                let destination = self.cache.dir.join(format!("{name}.response"));
                self.temporary = Some(temporary.clone());
                let bounded_headers = headers.len() <= 64
                    && headers
                        .iter()
                        .map(|(k, v)| k.as_str().len() + v.len())
                        .sum::<usize>()
                        <= 16 * 1024;
                let mut file = if status.is_success() && bounded_headers {
                    tokio::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&temporary)
                        .await
                        .ok()
                } else {
                    None
                };
                if file.is_none() {
                    self.cache.counters.errors.fetch_add(1, Ordering::Relaxed);
                    self.complete(None);
                }
                let mut source = Box::pin(body.into_data_stream());
                let mut size = 0u64;
                let mut receiver = true;
                let mut successful = true;
                while let Some(chunk) = poll_fn(|cx| source.as_mut().poll_next(cx)).await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(_error) => {
                            successful = false;
                            producer_failure.store(true, Ordering::Release);
                            file = None;
                            let _ = tokio::fs::remove_file(&temporary).await;
                            self.cache.counters.errors.fetch_add(1, Ordering::Relaxed);
                            self.complete(None);
                            break;
                        }
                    };
                    self.cache
                        .counters
                        .generated_bytes
                        .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    size = size.saturating_add(chunk.len() as u64);
                    if let Some(writer) = &mut file {
                        let oversized = size > self.cache.config.max_entry_bytes;
                        let write_failed = !oversized && writer.write_all(&chunk).await.is_err();
                        if oversized || write_failed {
                            if write_failed {
                                self.cache.counters.errors.fetch_add(1, Ordering::Relaxed);
                            }
                            file = None;
                            let _ = tokio::fs::remove_file(&temporary).await;
                            self.complete(None);
                        }
                    }
                    if receiver {
                        let n = chunk.len();
                        receiver = tx.send(Ok(chunk)).await.is_ok();
                        if receiver {
                            self.cache
                                .counters
                                .served_bytes
                                .fetch_add(n as u64, Ordering::Relaxed);
                        }
                    }
                    if !receiver && file.is_none() {
                        successful = false;
                        break;
                    }
                }
                if successful && let Some(mut file) = file {
                    let flushed = file.flush().await.is_ok();
                    drop(file);
                    // Keep rename and index publication in one non-yielding step.
                    // Cancelling an asynchronous rename could otherwise leave an
                    // unaccounted completed file after the fill deadline.
                    if flushed && std::fs::rename(&temporary, &destination).is_ok() {
                        self.complete(Some(Arc::new(Entry {
                            path: destination,
                            size,
                            created: Instant::now(),
                            status,
                            version,
                            headers,
                        })));
                    } else {
                        let _ = tokio::fs::remove_file(&temporary).await;
                        self.cache.counters.errors.fetch_add(1, Ordering::Relaxed);
                        self.complete(None);
                    }
                } else {
                    self.complete(None);
                }
                drop(source);
            };
            // One absolute deadline includes native reads, disk I/O, and the
            // bounded channel's backpressure. Dropping a timed-out fill invokes
            // ProducerPermit::drop and drops the native body/process guard.
            if tokio::time::timeout_at(deadline, fill).await.is_err() {
                cache.counters.errors.fetch_add(1, Ordering::Relaxed);
                failure.store(true, Ordering::Release);
            }
            drop(completion);
        });
        let output = async_stream::stream! {
            while let Some(item) = rx.recv().await { yield item; }
            if failed.load(Ordering::Acquire) {
                yield Err(std::io::Error::other("Git pack response generation failed or timed out"));
            }
        };
        Response::from_parts(parts, Body::from_stream(output))
    }
}
impl Drop for ProducerPermit {
    fn drop(&mut self) {
        self.complete(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use tokio::sync::{Notify, Semaphore};

    fn cache(max_bytes: u64, max_entry_bytes: u64) -> (tempfile::TempDir, Arc<PackCache>) {
        let dir = tempfile::tempdir().unwrap();
        let cache = PackCache::new(
            dir.path().to_owned(),
            PackCacheConfig {
                max_bytes,
                max_entry_bytes,
                max_entries: 8,
                ttl: Duration::from_secs(60),
            },
            Arc::new(tempfile::tempfile().unwrap()),
        )
        .unwrap();
        (dir, cache)
    }
    async fn permit() -> OwnedSemaphorePermit {
        Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap()
    }
    async fn producer(cache: &Arc<PackCache>, key: &str) -> ProducerPermit {
        match cache.lookup(key).await {
            Lookup::Produce(p) => p,
            _ => panic!("expected cache producer"),
        }
    }
    async fn fill(cache: &Arc<PackCache>, key: &str, bytes: &'static [u8]) {
        let response = producer(cache, key).await.stream_with_deadline(
            Response::new(Body::from(bytes)),
            permit().await,
            tokio::time::Instant::now() + Duration::from_secs(600),
        );
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX).await.unwrap(),
            bytes
        );
    }
    async fn hit(cache: &Arc<PackCache>, key: &str) -> CachedResponse {
        match cache.lookup(key).await {
            Lookup::Hit(hit) => hit,
            _ => panic!("expected cache hit"),
        }
    }
    #[tokio::test]
    async fn complete_response_is_reused_with_headers_and_counters() {
        let (_dir, cache) = cache(64, 32);
        let p = producer(&cache, "a").await;
        let mut original = Response::new(Body::from("pack bytes"));
        original.headers_mut().insert(
            "content-type",
            "application/x-git-upload-pack-result".parse().unwrap(),
        );
        let response = p.stream_with_deadline(
            original,
            permit().await,
            tokio::time::Instant::now() + Duration::from_secs(600),
        );
        assert_eq!(response.headers()["x-git-proxy-pack-cache"], "MISS");
        assert_eq!(
            to_bytes(response.into_body(), 100).await.unwrap(),
            "pack bytes"
        );
        let response = hit(&cache, "a").await.response("HIT");
        assert_eq!(
            response.headers()["content-type"],
            "application/x-git-upload-pack-result"
        );
        assert_eq!(response.headers()["x-git-proxy-pack-cache"], "HIT");
        assert_eq!(
            to_bytes(response.into_body(), 100).await.unwrap(),
            "pack bytes"
        );
        let stats = cache.snapshot();
        assert_eq!(
            (stats.hits, stats.misses, stats.entries, stats.current_bytes),
            (1, 1, 1, 10)
        );
        assert_eq!(stats.generated_bytes, 10);
        assert_eq!(stats.served_bytes, 20);
    }
    #[tokio::test]
    async fn equivalent_requests_wait_for_complete_response_and_survive_disconnect() {
        let (_dir, cache) = cache(64, 32);
        let p = producer(&cache, "a").await;
        let Lookup::Wait(waiter) = cache.lookup("a").await else {
            panic!("expected singleflight waiter")
        };
        let ready = Arc::new(Notify::new());
        let release = ready.clone();
        let source = async_stream::stream! {
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"first"));
            release.notified().await;
            yield Ok(Bytes::from_static(b"second"));
        };
        let response = p.stream_with_deadline(
            Response::new(Body::from_stream(source)),
            permit().await,
            tokio::time::Instant::now() + Duration::from_secs(600),
        );
        drop(response);
        ready.notify_one();
        let cached = waiter.wait().await.unwrap();
        assert_eq!(
            to_bytes(cached.response("COALESCED").into_body(), 100)
                .await
                .unwrap(),
            "firstsecond"
        );
        assert_eq!(cache.snapshot().coalesced, 1);
    }
    #[tokio::test]
    async fn producer_drop_and_partial_failure_never_admit_and_release_waiters() {
        let (_dir, cache) = cache(64, 32);
        let p = producer(&cache, "a").await;
        let Lookup::Wait(waiter) = cache.lookup("a").await else {
            panic!()
        };
        drop(p);
        assert!(waiter.wait().await.is_none());
        let p = producer(&cache, "a").await;
        let Lookup::Wait(waiter) = cache.lookup("a").await else {
            panic!()
        };
        let source = async_stream::stream! {
            yield Ok(Bytes::from_static(b"partial"));
            yield Err(std::io::Error::other("backend exited unsuccessfully"));
        };
        let response = p.stream_with_deadline(
            Response::new(Body::from_stream(source)),
            permit().await,
            tokio::time::Instant::now() + Duration::from_secs(600),
        );
        assert!(to_bytes(response.into_body(), 100).await.is_err());
        assert!(waiter.wait().await.is_none());
        assert_eq!(
            (cache.snapshot().entries, cache.snapshot().current_bytes),
            (0, 0)
        );
        assert!(matches!(cache.lookup("a").await, Lookup::Produce(_)));
    }
    #[tokio::test]
    async fn oversize_response_streams_uncached_and_releases_waiters() {
        let (_dir, cache) = cache(8, 4);
        let p = producer(&cache, "a").await;
        let Lookup::Wait(waiter) = cache.lookup("a").await else {
            panic!()
        };
        let response = p.stream_with_deadline(
            Response::new(Body::from("oversized response")),
            permit().await,
            tokio::time::Instant::now() + Duration::from_secs(600),
        );
        assert_eq!(
            to_bytes(response.into_body(), 100).await.unwrap(),
            "oversized response"
        );
        assert!(waiter.wait().await.is_none());
        assert_eq!(
            (cache.snapshot().entries, cache.snapshot().current_bytes),
            (0, 0)
        );
    }
    #[tokio::test]
    async fn quota_evicts_lru_but_not_leased_entries_or_reserved_writers() {
        let (_dir, cache) = cache(12, 8);
        fill(&cache, "a", b"aaaa").await;
        let lease = hit(&cache, "a").await;
        fill(&cache, "b", b"bbbb").await;
        assert!(matches!(cache.lookup("c").await, Lookup::Produce(_)));
        assert_eq!(cache.snapshot().evictions, 1); // b, never pinned a
        let active = producer(&cache, "c").await;
        assert!(matches!(cache.lookup("d").await, Lookup::Bypass));
        assert!(cache.snapshot().current_bytes <= 12);
        drop(active);
        assert_eq!(
            to_bytes(lease.response("HIT").into_body(), 10)
                .await
                .unwrap(),
            "aaaa"
        );
    }
    #[tokio::test]
    async fn expired_entries_are_not_served_and_entry_count_is_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let cache = PackCache::new(
            dir.path().to_owned(),
            PackCacheConfig {
                max_bytes: 100,
                max_entry_bytes: 10,
                max_entries: 1,
                ttl: Duration::ZERO,
            },
            Arc::new(tempfile::tempfile().unwrap()),
        )
        .unwrap();
        fill(&cache, "a", b"a").await;
        let p = producer(&cache, "a").await;
        assert_eq!(cache.snapshot().evictions, 1);
        assert!(matches!(cache.lookup("b").await, Lookup::Bypass));
        drop(p);
        assert!(matches!(cache.lookup("b").await, Lookup::Produce(_)));
    }
    #[tokio::test]
    async fn cache_disk_failure_preserves_original_stream_and_releases_waiters() {
        let (_dir, cache) = cache(64, 32);
        let p = producer(&cache, "a").await;
        let Lookup::Wait(waiter) = cache.lookup("a").await else {
            panic!()
        };
        std::fs::remove_dir(&cache.dir).unwrap();
        let response = p.stream_with_deadline(
            Response::new(Body::from("uncached content")),
            permit().await,
            tokio::time::Instant::now() + Duration::from_secs(600),
        );
        assert_eq!(
            to_bytes(response.into_body(), 100).await.unwrap(),
            "uncached content"
        );
        assert!(waiter.wait().await.is_none());
        assert_eq!(cache.snapshot().current_bytes, 0);
        assert_eq!(cache.snapshot().errors, 1);
    }
    #[tokio::test]
    async fn disconnected_leader_keeps_http_slot_until_fill_finishes() {
        let (_dir, cache) = cache(64, 32);
        let p = producer(&cache, "a").await;
        let Lookup::Wait(waiter) = cache.lookup("a").await else {
            panic!()
        };
        let slots = Arc::new(Semaphore::new(1));
        let slot = slots.clone().acquire_owned().await.unwrap();
        let release = Arc::new(Notify::new());
        let gate = release.clone();
        let source = async_stream::stream! {
            gate.notified().await;
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"pack"));
        };
        drop(p.stream_with_deadline(
            Response::new(Body::from_stream(source)),
            slot,
            tokio::time::Instant::now() + Duration::from_secs(600),
        ));
        assert_eq!(slots.available_permits(), 0);
        release.notify_one();
        let response = waiter.wait().await.unwrap().response("COALESCED");
        assert_eq!(to_bytes(response.into_body(), 100).await.unwrap(), "pack");
        // Admission completion precedes the producer's final cleanup. Acquiring
        // the permit waits for that cleanup without a timing-dependent sleep.
        let _slot = slots.acquire().await.unwrap();
        assert_eq!(cache.snapshot().inflight, 0);
    }
    #[test]
    fn startup_only_purges_own_schema_files() {
        let dir = tempfile::tempdir().unwrap();
        let owned = dir.path().join("generated-packs/schema-v1");
        std::fs::create_dir_all(&owned).unwrap();
        let stale = owned.join(format!("{}.tmp", "a".repeat(64)));
        let completed = owned.join(format!("{}.response", "b".repeat(64)));
        std::fs::write(&stale, b"partial").unwrap();
        std::fs::write(&completed, b"stale pack").unwrap();
        std::fs::write(owned.join("unrelated.tmp"), b"preserve").unwrap();
        let _cache = PackCache::new(
            dir.path().to_owned(),
            PackCacheConfig::default(),
            Arc::new(tempfile::tempfile().unwrap()),
        )
        .unwrap();
        assert!(!stale.exists());
        assert!(!completed.exists());
        assert!(owned.join("unrelated.tmp").exists());
    }
    #[tokio::test]
    async fn expired_fill_deadline_releases_waiters_and_errors_original_body() {
        let (_dir, cache) = cache(64, 32);
        let p = producer(&cache, "a").await;
        let Lookup::Wait(waiter) = cache.lookup("a").await else {
            panic!()
        };
        let gate = Arc::new(Notify::new());
        let source = async_stream::stream! {
            gate.notified().await;
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"unreachable"));
        };
        let response = p.stream_with_deadline(
            Response::new(Body::from_stream(source)),
            permit().await,
            tokio::time::Instant::now() - Duration::from_secs(1),
        );
        assert!(waiter.wait().await.is_none());
        assert!(to_bytes(response.into_body(), 100).await.is_err());
        assert_eq!(cache.snapshot().inflight, 0);
        assert_eq!(cache.snapshot().current_bytes, 0);
        assert_eq!(cache.snapshot().errors, 1);
    }
    #[tokio::test]
    async fn source_error_releases_waiters_even_when_leader_channel_is_full() {
        let (_dir, cache) = cache(64, 32);
        let p = producer(&cache, "a").await;
        let Lookup::Wait(waiter) = cache.lookup("a").await else {
            panic!()
        };
        let source = async_stream::stream! {
            for _ in 0..4 { yield Ok(Bytes::from_static(b"chunk")); }
            yield Err(std::io::Error::other("native process failed"));
        };
        let response = p.stream_with_deadline(
            Response::new(Body::from_stream(source)),
            permit().await,
            tokio::time::Instant::now() + Duration::from_secs(600),
        );
        // Do not poll the leader response: the bounded data channel is full.
        assert!(
            tokio::time::timeout(Duration::from_secs(5), waiter.wait())
                .await
                .unwrap()
                .is_none()
        );
        assert!(to_bytes(response.into_body(), 100).await.is_err());
        assert_eq!(cache.snapshot().entries, 0);
    }
}
