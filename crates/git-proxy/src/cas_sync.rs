//! Bounded, best-effort workspace publication, independent of foreground Git.
use crate::{
    App, ObjectRequest,
    cas_client::{CasClient, CasConfig, Digest, UploadBlob},
};
use anyhow::{Context, Result, ensure};
use local_cas::{Artifact, BlobSource, CacheConfig, CachedBlob, LocalCas};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, watch},
    task::JoinHandle,
};

const QUEUED_JOBS: usize = 8;
const MAX_PENDING: usize = 32_768;
const MAX_PRESENCE: usize = 65_536;
const PRESENCE_BATCH: usize = 1000;
const DEFAULT_STAGING: u64 = 64 * 1024 * 1024;
type Key = (String, i64);

/// One fixed CAS target/authorization identity per proxy process. Credentials
/// are selected explicitly by environment-variable name, never supplied by HTTP.
#[derive(Clone, Debug)]
pub struct CasSyncConfig {
    pub endpoint: String,
    pub instance_name: String,
    pub header_env: Vec<(String, String)>,
    pub policy: ::reapi::policy::Policy,
    pub staging_limit_bytes: u64,
    pub cache_max_bytes: u64,
}
impl CasSyncConfig {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            instance_name: String::new(),
            header_env: Vec::new(),
            policy: Default::default(),
            staging_limit_bytes: DEFAULT_STAGING,
            cache_max_bytes: 4 * 1024 * 1024 * 1024,
        }
    }
    fn client_config(&self) -> Result<CasConfig> {
        let mut headers = Vec::new();
        for (name, env) in &self.header_env {
            let value = std::env::var(env).map_err(|_| {
                anyhow::anyhow!("configured CAS credential variable is unavailable")
            })?;
            ensure!(!value.is_empty(), "configured CAS credential is empty");
            headers.push((name.clone(), value));
        }
        Ok(CasConfig {
            endpoint: self.endpoint.clone(),
            instance_name: self.instance_name.clone(),
            headers,
            policy: self.policy.clone(),
        })
    }
}
#[derive(Clone, Default, Serialize)]
pub struct CasSyncStats {
    pub accepted_jobs: u64,
    pub completed_jobs: u64,
    pub failed_jobs: u64,
    pub cancelled_jobs: u64,
    pub accepted_objects: u64,
    pub pending_objects: usize,
    pub coalesced_objects: u64,
    pub queue_rejections: u64,
    pub ttl_hits: u64,
    pub confirmed_objects: u64,
    pub uploaded_objects: u64,
    pub uploaded_logical_bytes: u64,
    pub oversize_objects: u64,
    pub last_error: Option<&'static str>,
    pub transport: crate::cas_client::UploadStats,
    pub local_cache: serde_json::Value,
}
#[derive(Serialize)]
pub struct Enqueued {
    pub queued_objects: usize,
    pub coalesced_objects: usize,
    pub note: &'static str,
}
#[derive(Default)]
struct Control {
    pending: HashSet<(String, String)>,
    present: HashMap<Key, Instant>,
    stats: CasSyncStats,
}
struct Shared {
    control: Mutex<Control>,
}
impl Shared {
    fn with<T>(&self, f: impl FnOnce(&mut Control) -> T) -> T {
        f(&mut self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
    }
    fn confirm(&self, digests: &[Digest], started: Instant, ttl: Duration) {
        self.with(|c| {
            c.present.retain(|_, expires| *expires > Instant::now());
            // Presence is only an optimization. Dropping observations is safer
            // than unbounded memory or incorrectly extending their lifetime.
            if c.present.len().saturating_add(digests.len()) > MAX_PRESENCE {
                c.present.clear();
            }
            if let Some(expires) = started.checked_add(ttl)
                && expires > Instant::now()
            {
                for d in digests {
                    c.present.insert((d.hash.clone(), d.size_bytes), expires);
                }
            }
            c.stats.confirmed_objects += digests.len() as u64;
        });
    }
}
pub(crate) struct Job {
    alias: String,
    oids: Vec<String>,
    force: bool,
}
struct PendingGuard {
    shared: Arc<Shared>,
    alias: String,
    oids: Vec<String>,
}
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.shared.with(|c| {
            for oid in &self.oids {
                c.pending.remove(&(self.alias.clone(), oid.clone()));
            }
        });
    }
}
pub(crate) struct CasSync {
    config: CasSyncConfig,
    cache: Arc<LocalCas>,
    backend: String,
    shared: Arc<Shared>,
    sender: mpsc::Sender<Job>,
    shutdown: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    hydrations: Arc<Mutex<Vec<JoinHandle<()>>>>,
}
/// Main/service owners should stop CAS work after stopping new HTTP traffic.
pub struct CasShutdown {
    inner: Arc<CasSync>,
}
impl Drop for CasShutdown {
    fn drop(&mut self) {
        // The worker never owns this service-owner handle. Even an accidentally
        // dropped owner must cancel active Git/network work and release App.
        let _ = self.inner.shutdown.send(true);
    }
}
impl CasShutdown {
    pub async fn shutdown(self) {
        let _ = self.inner.shutdown.send(true);
        let worker = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            let _ = worker.await;
        }
        // Outer cancellation must not detach cache's admitted native Git fetch.
        // Waiters remain alive through LocalCas completion's ownership barrier.
        let hydrations = std::mem::take(&mut *self.inner.hydrations.lock().unwrap());
        for hydration in hydrations {
            let _ = hydration.await;
        }
        self.inner
            .cache
            .wait_for_idle()
            .await
            .expect("local CAS admission barrier remains open");
    }
}
impl Drop for CasSync {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            worker.abort();
        }
    }
}
impl CasSync {
    pub(crate) fn new(
        config: CasSyncConfig,
        cache_dir: &Path,
    ) -> Result<(Arc<Self>, mpsc::Receiver<Job>, watch::Receiver<bool>)> {
        ensure!(
            config.staging_limit_bytes > 0,
            "CAS staging limit must be positive"
        );
        ensure!(
            config.staging_limit_bytes <= i64::MAX as u64,
            "CAS staging limit exceeds REAPI size range"
        );
        // Validate credential selection before accepting traffic, without
        // retaining values in Debug-printable server configuration.
        config.policy.validate()?;
        let client = config.client_config()?;
        ensure!(
            config.cache_max_bytes >= config.staging_limit_bytes,
            "CAS content cache must fit the staging byte budget"
        );
        let mut scope = blake3::Hasher::new();
        scope.update(b"git-proxy-cas-content-v1");
        for value in std::iter::once(client.endpoint.as_str())
            .chain(std::iter::once(client.instance_name.as_str()))
            .chain(
                client
                    .headers
                    .iter()
                    .flat_map(|(name, value)| [name.as_str(), value.as_str()]),
            )
        {
            scope.update(&(value.len() as u64).to_le_bytes());
            scope.update(value.as_bytes());
        }
        let backend = scope.finalize().to_hex().to_string();
        let cache = LocalCas::open(
            cache_dir.join("cas-content-v1"),
            Arc::new(NoDefaultSource),
            CacheConfig {
                max_bytes: config.cache_max_bytes,
                max_inflight_bytes: config.staging_limit_bytes,
                max_concurrent_fetches: 1,
                operation_timeout: config.policy.background_timeout(),
                fetch_timeout: config.policy.background_timeout(),
            },
        )?;
        let (sender, receiver) = mpsc::channel(QUEUED_JOBS);
        let (shutdown, cancelled) = watch::channel(false);
        Ok((
            Arc::new(Self {
                config,
                cache,
                backend,
                shared: Arc::new(Shared {
                    control: Mutex::new(Control::default()),
                }),
                sender,
                shutdown,
                worker: Mutex::new(None),
                hydrations: Arc::new(Mutex::new(Vec::new())),
            }),
            receiver,
            cancelled,
        ))
    }
    pub(crate) fn start(
        self: &Arc<Self>,
        app: Weak<App>,
        mut jobs: mpsc::Receiver<Job>,
        mut cancelled: watch::Receiver<bool>,
    ) -> CasShutdown {
        let shared = self.shared.clone();
        let config = self.config.clone();
        let cache = self.cache.clone();
        let backend = self.backend.clone();
        let hydrations = self.hydrations.clone();
        let source_cancelled = cancelled.clone();
        let handle = tokio::spawn(async move {
            let mut client: Option<CasClient> = None;
            loop {
                let job = tokio::select! { biased;
                    _ = cancelled.changed() => break,
                    job = jobs.recv() => match job { Some(job) => job, None => break },
                };
                let guard = PendingGuard {
                    shared: shared.clone(),
                    alias: job.alias.clone(),
                    oids: job.oids.clone(),
                };
                let deadline = tokio::time::Instant::now() + config.policy.background_timeout();
                let result = tokio::select! { biased;
                    _ = cancelled.changed() => { shared.with(|c| c.stats.cancelled_jobs += 1); drop(guard); break; },
                    result = tokio::time::timeout_at(deadline, run_job(&app, &shared, &config, &cache, &backend, &hydrations, &source_cancelled, &mut client, job, deadline)) => result.unwrap_or(Err("background_deadline")),
                };
                shared.with(|c| {
                    if let Some(client) = &client {
                        c.stats.transport = client.stats();
                    }
                    match result {
                        Ok(()) => c.stats.completed_jobs += 1,
                        Err(stage) => {
                            c.stats.failed_jobs += 1;
                            c.stats.last_error = Some(stage);
                        }
                    }
                });
                drop(guard);
            }
            if let Some(client) = &client {
                client.wait_for_chunk_idle().await;
            }
            while let Ok(job) = jobs.try_recv() {
                shared.with(|c| c.stats.cancelled_jobs += 1);
                drop(PendingGuard {
                    shared: shared.clone(),
                    alias: job.alias,
                    oids: job.oids,
                });
            }
        });
        *self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle);
        CasShutdown {
            inner: self.clone(),
        }
    }
    pub(crate) fn snapshot(&self) -> CasSyncStats {
        self.shared.with(|c| {
            let mut stats = c.stats.clone();
            stats.pending_objects = c.pending.len();
            let cache = self.cache.metrics();
            stats.local_cache = serde_json::json!({
                "fetches":cache.fetches, "hits":cache.hits,
                "verified_bytes":cache.verified_bytes,
                "verification_bytes":cache.verification_bytes,
                "resident_bytes":cache.resident_bytes,
                "reserved_bytes":cache.reserved_bytes,
                "evictions":cache.evictions,
                "evicted_bytes":cache.evicted_bytes,
                "recency_errors":cache.recency_errors,
                "max_bytes":self.config.cache_max_bytes,
            });
            stats
        })
    }
    pub(crate) fn enqueue(&self, alias: &str, oids: Vec<String>, force: bool) -> Result<Enqueued> {
        ensure!(!*self.shutdown.borrow(), "CAS sync is shutting down");
        self.shared.with(|c| {
            let mut unique = HashSet::new();
            let oids: Vec<_> = oids
                .into_iter()
                .map(|oid| oid.to_ascii_lowercase())
                .filter(|oid| unique.insert(oid.clone()))
                .collect();
            let coalesced = oids
                .iter()
                .filter(|oid| c.pending.contains(&(alias.to_owned(), (*oid).clone())))
                .count();
            ensure!(
                !force || coalesced == 0,
                "requested blobs already pending; retry force after completion"
            );
            let oids: Vec<_> = oids
                .into_iter()
                .filter(|oid| !c.pending.contains(&(alias.to_owned(), oid.clone())))
                .collect();
            if c.pending.len().saturating_add(oids.len()) > MAX_PENDING {
                c.stats.queue_rejections += 1;
                anyhow::bail!("CAS pending-object limit reached");
            }
            let count = oids.len();
            if count > 0 {
                // Insertion and try_send share this short lock, so the worker
                // cannot complete a job before its pending entries exist.
                for oid in &oids {
                    c.pending.insert((alias.to_owned(), oid.clone()));
                }
                if self
                    .sender
                    .try_send(Job {
                        alias: alias.to_owned(),
                        oids: oids.clone(),
                        force,
                    })
                    .is_err()
                {
                    for oid in &oids {
                        c.pending.remove(&(alias.to_owned(), oid.clone()));
                    }
                    c.stats.queue_rejections += 1;
                    anyhow::bail!("CAS job queue is full");
                }
                c.stats.accepted_jobs += 1;
                c.stats.accepted_objects += count as u64;
            }
            c.stats.coalesced_objects += coalesced as u64;
            Ok(Enqueued {
                queued_objects: count,
                coalesced_objects: coalesced,
                note: "accepted, not proof of CAS publication; inspect cache stats",
            })
        })
    }
}
async fn run_job(
    app: &Weak<App>,
    shared: &Shared,
    config: &CasSyncConfig,
    cache: &Arc<LocalCas>,
    backend: &str,
    hydrations: &Arc<Mutex<Vec<JoinHandle<()>>>>,
    cancelled: &watch::Receiver<bool>,
    client: &mut Option<CasClient>,
    job: Job,
    deadline: tokio::time::Instant,
) -> Result<(), &'static str> {
    if client.is_none() {
        let cfg = config
            .client_config()
            .map_err(|_| "credential_configuration")?;
        *client = Some(
            CasClient::connect(cfg)
                .await
                .map_err(|_| "connect_or_capabilities")?,
        );
    }
    let client = client.as_ref().unwrap().with_deadline(deadline);
    for oids in job.oids.chunks(PRESENCE_BATCH) {
        let app = app.upgrade().ok_or("proxy_stopped")?;
        let repo = app
            .repositories
            .get(&job.alias)
            .ok_or("repository_removed")?;
        crate::ensure_repo(&app, repo, false)
            .await
            .map_err(|_| "git_metadata")?;
        let objects = crate::metadata(
            &app,
            repo,
            ObjectRequest {
                oids: oids.to_vec(),
                blake3: true,
            },
        )
        .await
        .map_err(|_| "git_metadata")?;
        let mut candidates = HashMap::new();
        for object in objects {
            let digest = object.blake3.ok_or("not_a_blob")?;
            let digest = Digest {
                hash: digest.hash,
                size_bytes: i64::try_from(digest.size_bytes).map_err(|_| "blob_size")?,
            };
            candidates
                .entry((digest.hash.clone(), digest.size_bytes))
                .or_insert((object.oid, digest));
        }
        let stale: Vec<_> = shared.with(|c| {
            candidates
                .into_values()
                .filter(|(_, d)| {
                    let key = (d.hash.clone(), d.size_bytes);
                    if job.force {
                        c.present.remove(&key);
                    }
                    if c.present
                        .get(&key)
                        .is_some_and(|expires| *expires > Instant::now())
                    {
                        c.stats.ttl_hits += 1;
                        false
                    } else {
                        true
                    }
                })
                .collect()
        });
        if stale.is_empty() {
            continue;
        }
        let started = Instant::now();
        let digests: Vec<_> = stale.iter().map(|(_, d)| d.clone()).collect();
        let missing = client
            .find_missing(&digests)
            .await
            .map_err(|_| "find_missing")?;
        let missing: HashSet<Key> = missing
            .into_iter()
            .map(|d| (d.hash, d.size_bytes))
            .collect();
        shared.with(|c| {
            for digest in &missing {
                c.present.remove(digest);
            }
        });
        let present: Vec<_> = digests
            .iter()
            .filter(|d| !missing.contains(&(d.hash.clone(), d.size_bytes)))
            .cloned()
            .collect();
        shared.confirm(&present, started, config.policy.presence_ttl());
        let mut batch = Vec::new();
        let mut leases = Vec::new();
        let mut staged_bytes = 0u64;
        let mut oversize = false;
        for (oid, digest) in stale {
            if !missing.contains(&(digest.hash.clone(), digest.size_bytes)) {
                continue;
            }
            let size = digest.size_bytes as u64;
            if size > config.staging_limit_bytes {
                shared.with(|c| c.stats.oversize_objects += 1);
                oversize = true;
                continue;
            }
            if staged_bytes + size > config.staging_limit_bytes {
                upload_batch(&client, shared, config, &mut batch, &mut leases, started).await?;
                staged_bytes = 0;
            }
            let artifact = Artifact {
                backend: backend.to_owned(),
                instance: config.instance_name.clone(),
                digest_function: 9,
                hash: digest.hash.clone(),
                size,
            };
            let source = Arc::new(GitBlobSource {
                config: app.config.clone(),
                repo: repo.path.clone(),
                oid,
                _owner: app._lock.clone(),
                cancelled: cancelled.clone(),
            });
            let (sent, result) = tokio::sync::oneshot::channel();
            let cache = cache.clone();
            let task = tokio::spawn(async move {
                let value = cache.content_from(artifact, source).await;
                let _ = sent.send(value);
            });
            {
                let mut active = hydrations.lock().unwrap();
                active.retain(|task| !task.is_finished());
                active.push(task);
            }
            let lease = result
                .await
                .map_err(|_| "cache_worker")?
                .map_err(|_| "spool_or_digest_mismatch")?;
            batch.push(UploadBlob {
                digest,
                path: lease.path().to_owned(),
            });
            leases.push(lease);
            staged_bytes += size;
        }
        // Release the proxy/mirror owner before the remaining network work.
        drop(app);
        upload_batch(&client, shared, config, &mut batch, &mut leases, started).await?;
        if oversize {
            return Err("blob_exceeds_staging_limit");
        }
    }
    Ok(())
}
async fn upload_batch(
    client: &CasClient,
    shared: &Shared,
    config: &CasSyncConfig,
    batch: &mut Vec<UploadBlob>,
    leases: &mut Vec<Arc<CachedBlob>>,
    started: Instant,
) -> Result<(), &'static str> {
    if batch.is_empty() {
        return Ok(());
    }
    client
        .upload_missing_leased(batch, leases)
        .await
        .map_err(|_| "upload")?;
    let digests: Vec<_> = batch.iter().map(|b| b.digest.clone()).collect();
    shared.confirm(&digests, started, config.policy.presence_ttl());
    shared.with(|c| {
        c.stats.uploaded_objects += batch.len() as u64;
        c.stats.uploaded_logical_bytes += digests.iter().map(|d| d.size_bytes as u64).sum::<u64>();
    });
    // Keep leases through all upload retries / CDC reads. Cache owns unlinking.
    batch.clear();
    leases.clear();
    Ok(())
}
struct NoDefaultSource;
#[async_trait::async_trait]
impl BlobSource for NoDefaultSource {
    async fn fetch(&self, _: &Artifact, _: &mut std::fs::File) -> Result<()> {
        anyhow::bail!("Git CAS content requires a concrete repository source")
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        anyhow::bail!("local payload cache does not establish remote presence")
    }
}
struct GitBlobSource {
    config: crate::Config,
    repo: std::path::PathBuf,
    oid: String,
    _owner: Arc<std::fs::File>,
    cancelled: watch::Receiver<bool>,
}
#[async_trait::async_trait]
impl BlobSource for GitBlobSource {
    async fn fetch(&self, artifact: &Artifact, target: &mut std::fs::File) -> Result<()> {
        self.fetch_inner(artifact, target).await
    }

    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        anyhow::bail!("Git source cannot establish remote CAS presence")
    }
}
impl GitBlobSource {
    async fn fetch_inner(&self, artifact: &Artifact, target: &mut std::fs::File) -> Result<()> {
        ensure!(
            !*self.cancelled.borrow(),
            "Git payload fetch cancelled at service shutdown"
        );
        let mut command = crate::git_config(&self.config, Some(&self.repo));
        command
            .args(["cat-file", "blob", &self.oid])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let mut child = command.spawn().context("read Git blob")?;
        let _group = crate::group(&child);
        let mut input = child.stdout.take().unwrap();
        let mut output = tokio::fs::File::from_std(target.try_clone()?);
        let deadline = tokio::time::Instant::now() + crate::GIT_TIMEOUT;
        let mut cancelled = self.cancelled.clone();
        let result = tokio::select! { biased;
            _ = async {
                if !*cancelled.borrow() { let _ = cancelled.changed().await; }
            } => Err(anyhow::anyhow!("Git payload fetch cancelled at service shutdown")),
            result = async {
        let mut size = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let n = tokio::time::timeout_at(deadline, input.read(&mut buffer)).await??;
            if n == 0 {
                break;
            }
            size = size.checked_add(n as u64).context("blob size overflow")?;
            ensure!(size <= artifact.size, "Git blob exceeds mapped size");
            output.write_all(&buffer[..n]).await?;
        }
        ensure!(
            tokio::time::timeout_at(deadline, child.wait())
                .await??
                .success()
                && size == artifact.size,
            "Git blob does not match mapped size"
        );
        output.flush().await?;
        Ok(())
            } => result,
        };
        // A graceful owner shutdown reaps the actual child, not only the cache
        // waiter. The group guard still protects timeout/drop paths.
        if result.is_err() {
            drop(_group);
            let _ = child.kill().await;
        }
        // LocalCas independently verifies complete hash/size before publication.
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, PackCacheConfig, cas_client::tests as wire};
    use serde_json::{Value, json};
    use std::{path::PathBuf, process::Command};

    fn fixture(root: &Path) -> (PathBuf, Vec<String>, Vec<Digest>) {
        fixture_data(root, &[b"first blob\n".to_vec(), b"second blob\n".to_vec()])
    }
    fn fixture_data(root: &Path, data: &[Vec<u8>]) -> (PathBuf, Vec<String>, Vec<Digest>) {
        let repo = root.join("source");
        std::fs::create_dir(&repo).unwrap();
        let git = |args: &[&str]| {
            let out = Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(args)
                .output()
                .unwrap();
            assert!(out.status.success(), "fixture Git command failed");
            String::from_utf8(out.stdout).unwrap().trim().to_owned()
        };
        git(&["init", "-q"]);
        git(&["config", "user.name", "Fixture"]);
        git(&["config", "user.email", "fixture@example.invalid"]);
        for (index, bytes) in data.iter().enumerate() {
            std::fs::write(repo.join(format!("file{index}")), bytes).unwrap();
        }
        git(&["add", "."]);
        git(&["commit", "-qm", "fixture"]);
        let ids = (0..data.len())
            .map(|index| git(&["rev-parse", &format!("HEAD:file{index}")]))
            .collect();
        let digests = data
            .iter()
            .map(|bytes| Digest {
                hash: blake3::hash(bytes).to_hex().to_string(),
                size_bytes: bytes.len() as i64,
            })
            .collect();
        (repo, ids, digests)
    }
    fn configuration(root: &Path, source: &Path, endpoint: String, ttl: Duration) -> Config {
        let mut cas = CasSyncConfig::new(endpoint);
        cas.instance_name = "test/instance".into();
        cas.policy.presence_ttl_seconds = ttl.as_secs();
        Config {
            cache_dir: root.join("cache"),
            git: "git".into(),
            repositories: vec![("repo".into(), source.to_string_lossy().into_owned())],
            refresh_ttl: Duration::from_secs(60),
            enable_blake3: true,
            pack_cache: PackCacheConfig::default(),
            cas_sync: Some(cas),
        }
    }
    struct HttpServer {
        origin: String,
        done: Option<tokio::sync::oneshot::Sender<()>>,
        task: tokio::task::JoinHandle<()>,
        cas: Option<CasShutdown>,
    }
    impl HttpServer {
        async fn start(config: Config) -> Self {
            let (router, cas) = crate::router_with_cas_shutdown(config).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let (done, stopped) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            });
            Self {
                origin,
                done: Some(done),
                task,
                cas,
            }
        }
        async fn post(&self, suffix: &str, value: Value) -> Value {
            let response = reqwest::Client::new()
                .post(format!("{}/api/v1/repos/repo/{suffix}", self.origin))
                .json(&value)
                .send()
                .await
                .unwrap();
            assert!(response.status().is_success(), "sync HTTP request failed");
            response.json().await.unwrap()
        }
        async fn settled(&self) -> Value {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let stats: Value = reqwest::get(format!("{}/api/v1/cache/stats", self.origin))
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    let cas = stats["cas_sync"].clone();
                    if cas["accepted_jobs"].as_u64().unwrap() > 0 && cas["pending_objects"] == 0 {
                        return cas;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("CAS job did not settle")
        }
        async fn stop(mut self) {
            if let Some(done) = self.done.take() {
                let _ = done.send(());
            }
            if let Some(cas) = self.cas.take() {
                cas.shutdown().await;
            }
            let _ = (&mut self.task).await;
        }
    }
    impl Drop for HttpServer {
        fn drop(&mut self) {
            if let Some(done) = self.done.take() {
                let _ = done.send(());
            }
            self.cas.take();
            self.task.abort();
        }
    }

    /// Opt-in, same mock-remote workload for before/after retained-publication comparisons.
    #[tokio::test]
    #[ignore = "benchmark; runs real Git and repeated mock-CAS publication"]
    async fn benchmark_warm_mapping_remote_eviction() {
        let root = tempfile::tempdir().unwrap();
        let profile =
            std::env::var("GIT_PROXY_BENCH_PROFILE").unwrap_or_else(|_| "medium_blobs".into());
        let (objects, bytes) = match profile.as_str() {
            "small_sources" => (256, 16 * 1024),
            "medium_blobs" => (32, 256 * 1024),
            _ => panic!("unknown publication benchmark profile"),
        };
        let data: Vec<Vec<u8>> = (0..objects)
            .map(|index| {
                let mut state = index as u64 + 1;
                (0..bytes)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        state as u8
                    })
                    .collect()
            })
            .collect();
        let (source, oids, digests) = fixture_data(root.path(), &data);
        let remote = wire::server(false, 2 * 1024 * 1024, "no-auth").await;
        let server = HttpServer::start(configuration(
            root.path(),
            &source,
            remote.endpoint.clone(),
            Duration::ZERO,
        ))
        .await;
        // First publication includes mapping generation; later phases keep mappings warm.
        let first_started = Instant::now();
        server.post("cas-sync", json!({"oids":oids})).await;
        assert_eq!(server.settled().await["failed_jobs"], 0);
        let first_seconds = first_started.elapsed().as_secs_f64();
        let mut present_seconds = Vec::new();
        for _ in 0..7 {
            let started = Instant::now();
            server.post("cas-sync", json!({"oids":oids})).await;
            assert_eq!(server.settled().await["failed_jobs"], 0);
            present_seconds.push(started.elapsed().as_secs_f64());
        }
        let mut elapsed = Vec::new();
        for _ in 0..7 {
            for digest in &digests {
                remote.evict(digest);
            }
            let started = Instant::now();
            server.post("cas-sync", json!({"oids":oids})).await;
            let stats = server.settled().await;
            assert_eq!(stats["failed_jobs"], 0);
            elapsed.push(started.elapsed().as_secs_f64());
            for digest in &digests {
                assert!(remote.contains(digest));
            }
        }
        let final_stats = server.settled().await;
        println!(
            "PUBLICATION_BENCHMARK {}",
            json!({"profile":profile, "objects":digests.len(),
            "logical_bytes":data.iter().map(Vec::len).sum::<usize>(),
            "local_cache":final_stats["local_cache"],
            "first_publish_seconds":first_seconds, "all_present_seconds":present_seconds, "republish_seconds":elapsed})
        );
        server.stop().await;
    }

    #[tokio::test]
    async fn metadata_publication_reuses_ttl_rechecks_expiry_force_and_restart() {
        let root = tempfile::tempdir().unwrap();
        let (source, oids, digests) = fixture(root.path());
        let remote = wire::server(false, 2 * 1024 * 1024, "no-auth").await;
        let config = configuration(
            root.path(),
            &source,
            remote.endpoint.clone(),
            Duration::from_secs(2),
        );
        let server = HttpServer::start(config.clone()).await;
        let mapping = server
            .post("objects", json!({"oids":oids,"blake3":true}))
            .await;
        assert_eq!(mapping["objects"].as_array().unwrap().len(), 2);
        let first = server.settled().await;
        assert_eq!(first["completed_jobs"], 1);
        assert_eq!(first["uploaded_objects"], 2);
        assert_eq!(first["local_cache"]["fetches"], 2);
        for digest in &digests {
            assert!(remote.contains(digest));
        }
        let checks = remote.find_missing_count();
        server
            .post("objects", json!({"oids":oids,"blake3":true}))
            .await;
        let cached = server.settled().await;
        assert_eq!(cached["ttl_hits"], 2);
        assert_eq!(remote.find_missing_count(), checks);
        // A known loss must override even a still-young local observation.
        remote.evict(&digests[0]);
        server
            .post("cas-sync", json!({"oids":oids,"force":true}))
            .await;
        let repaired = server.settled().await;
        assert_eq!(repaired["failed_jobs"], 0);
        assert_eq!(repaired["local_cache"]["fetches"], 2);
        assert_eq!(repaired["local_cache"]["hits"], 1);
        assert!(remote.contains(&digests[0]));
        assert_eq!(remote.find_missing_count(), checks + 1);
        tokio::time::sleep(Duration::from_millis(2050)).await;
        let uploads = remote.upload_count();
        server.post("cas-sync", json!({"oids":oids})).await;
        server.settled().await;
        assert_eq!(remote.find_missing_count(), checks + 2);
        assert_eq!(remote.upload_count(), uploads);
        server.stop().await;
        for digest in &digests {
            remote.evict(digest);
        }
        let server = HttpServer::start(config.clone()).await;
        server.post("cas-sync", json!({"oids":oids})).await;
        let restarted = server.settled().await;
        assert_eq!(restarted["uploaded_objects"], 2);
        assert_eq!(restarted["local_cache"]["fetches"], 0);
        assert_eq!(restarted["local_cache"]["hits"], 2);
        assert_eq!(remote.find_missing_count(), checks + 3);
        server.stop().await;
        // Retained bytes are untrusted after restart, not blindly reused.
        for entry in std::fs::read_dir(root.path().join("cache/cas-content-v1")).unwrap() {
            let path = entry.unwrap().path();
            if path.file_name().unwrap().to_string_lossy().len() == 64 {
                let len = std::fs::metadata(&path).unwrap().len() as usize;
                std::fs::write(path, vec![0; len]).unwrap();
            }
        }
        for digest in &digests {
            remote.evict(digest);
        }
        let server = HttpServer::start(config).await;
        server.post("cas-sync", json!({"oids":oids})).await;
        let repaired = server.settled().await;
        assert_eq!(repaired["failed_jobs"], 0);
        assert_eq!(repaired["local_cache"]["fetches"], 2);
        for digest in &digests {
            assert!(remote.contains(digest));
        }
        server.stop().await;
    }

    #[tokio::test]
    async fn default_zero_presence_ttl_rechecks_and_repairs_without_force() {
        assert_eq!(
            CasSyncConfig::new("http://127.0.0.1:1".into())
                .policy
                .presence_ttl(),
            Duration::ZERO
        );
        let root = tempfile::tempdir().unwrap();
        let (source, oids, digests) = fixture(root.path());
        let remote = wire::server(false, 2 * 1024 * 1024, "no-auth").await;
        let server = HttpServer::start(configuration(
            root.path(),
            &source,
            remote.endpoint.clone(),
            Duration::ZERO,
        ))
        .await;
        server.post("cas-sync", json!({"oids":oids})).await;
        server.settled().await;
        let checks = remote.find_missing_count();
        remote.evict(&digests[0]);
        server.post("cas-sync", json!({"oids":oids})).await;
        let repaired = server.settled().await;
        assert_eq!(repaired["failed_jobs"], 0);
        assert_eq!(repaired["ttl_hits"], 0);
        assert_eq!(remote.find_missing_count(), checks + 1);
        assert!(remote.contains(&digests[0]));
        server.stop().await;
    }

    #[tokio::test]
    async fn publication_releases_batch_leases_for_quota_eviction() {
        let root = tempfile::tempdir().unwrap();
        let (source, oids, digests) = fixture(root.path());
        let remote = wire::server(false, 2 * 1024 * 1024, "no-auth").await;
        let mut config = configuration(
            root.path(),
            &source,
            remote.endpoint.clone(),
            Duration::ZERO,
        );
        let cas = config.cas_sync.as_mut().unwrap();
        cas.staging_limit_bytes = 12;
        cas.cache_max_bytes = 12;
        let server = HttpServer::start(config).await;
        server.post("cas-sync", json!({"oids":oids})).await;
        let first = server.settled().await;
        assert_eq!(first["failed_jobs"], 0);
        assert_eq!(first["uploaded_objects"], 2);
        for digest in &digests {
            assert!(remote.contains(digest));
        }
        assert!(
            first["local_cache"]["resident_bytes"].as_u64().unwrap() <= 12,
            "quota must hold after batch leases release"
        );
        // Previously reclaimed bytes can still be reconstructed from Git.
        for digest in &digests {
            remote.evict(digest);
        }
        server.post("cas-sync", json!({"oids":oids})).await;
        let second = server.settled().await;
        assert_eq!(second["failed_jobs"], 0);
        assert_eq!(second["uploaded_objects"], 4);
        assert!(second["local_cache"]["fetches"].as_u64().unwrap() > 2);
        server.stop().await;
    }

    #[tokio::test]
    async fn rejected_upload_never_creates_presence_and_retains_verified_payloads() {
        let root = tempfile::tempdir().unwrap();
        let (source, oids, digests) = fixture(root.path());
        let remote = wire::server(false, 2 * 1024 * 1024, "no-auth").await;
        remote.set_mode("batch-error");
        let server = HttpServer::start(configuration(
            root.path(),
            &source,
            remote.endpoint.clone(),
            Duration::from_secs(3600),
        ))
        .await;
        server.post("cas-sync", json!({"oids":oids})).await;
        let failed = server.settled().await;
        assert_eq!(failed["failed_jobs"], 1);
        assert_eq!(failed["confirmed_objects"], 0);
        assert_eq!(failed["uploaded_objects"], 0);
        let checks = remote.find_missing_count();
        remote.set_mode("no-auth");
        for digest in &digests {
            remote.evict(digest);
        }
        server.post("cas-sync", json!({"oids":oids})).await;
        let repaired = server.settled().await;
        assert_eq!(remote.find_missing_count(), checks + 1);
        assert_eq!(repaired["ttl_hits"], 0);
        assert_eq!(repaired["completed_jobs"], 1);
        assert_eq!(repaired["local_cache"]["fetches"], 2);
        assert_eq!(repaired["local_cache"]["hits"], 2);
        server.stop().await;
    }

    #[tokio::test]
    async fn bounded_queue_force_conflicts_and_owner_drop_cancel_pending_work() {
        let config = CasSyncConfig::new("http://127.0.0.1:1".into());
        let cache_root = tempfile::tempdir().unwrap();
        let (sync, jobs, cancelled) = CasSync::new(config, cache_root.path()).unwrap();
        // No receiver is started yet, making queue/backpressure deterministic.
        let first = vec!["a".repeat(40)];
        assert_eq!(
            sync.enqueue("repo", first.clone(), false)
                .unwrap()
                .queued_objects,
            1
        );
        assert_eq!(
            sync.enqueue("repo", first.clone(), false)
                .unwrap()
                .coalesced_objects,
            1
        );
        assert!(sync.enqueue("repo", first, true).is_err());
        for i in 1..QUEUED_JOBS {
            sync.enqueue("repo", vec![format!("{i:040x}")], false)
                .unwrap();
        }
        assert!(sync.enqueue("repo", vec!["b".repeat(40)], false).is_err());
        assert_eq!(sync.snapshot().pending_objects, QUEUED_JOBS);
        let owner = sync.start(Weak::new(), jobs, cancelled);
        drop(owner); // Cancels even while no Router/App can trigger Drop.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if sync.snapshot().pending_objects == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(sync.enqueue("repo", vec!["c".repeat(40)], false).is_err());
        assert_eq!(sync.snapshot().cancelled_jobs, QUEUED_JOBS as u64);
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_cancels_payload_fetch_and_releases_cache_and_mirror_ownership() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let (source, oids, _) = fixture(root.path());
        let remote = wire::server(false, 2 * 1024 * 1024, "no-auth").await;
        let marker = root.path().join("payload-git.pid");
        let wrapper = root.path().join("payload-git");
        std::fs::write(
            &wrapper,
            format!(
                r#"#!/bin/sh
previous=
for argument in "$@"; do
  if [ "$previous" = cat-file ] && [ "$argument" = blob ]; then
    echo $$ > '{}'; exec sleep 60
  fi
  previous="$argument"
done
exec git "$@"
"#,
                marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut config = configuration(
            root.path(),
            &source,
            remote.endpoint.clone(),
            Duration::ZERO,
        );
        config.git = wrapper;
        let server = HttpServer::start(config.clone()).await;
        server.post("cas-sync", json!({"oids":oids})).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !marker.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let pid: i32 = std::fs::read_to_string(&marker)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), server.stop())
            .await
            .unwrap();
        assert_ne!(
            unsafe { libc::kill(pid, 0) },
            0,
            "payload Git must be stopped before shutdown returns"
        );
        let restarted = HttpServer::start(config).await;
        restarted.stop().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_service_owner_cancels_active_git_and_releases_pending() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let (source, oids, _) = fixture(root.path());
        let remote = wire::server(false, 2 * 1024 * 1024, "no-auth").await;
        let marker = root.path().join("blocked-git.pid");
        let wrapper = root.path().join("slow-git");
        std::fs::write(
            &wrapper,
            format!(
                r#"#!/bin/sh
for argument in "$@"; do
  case "$argument" in
    --batch-check=*) echo $$ > '{}'; exec sleep 60 ;;
  esac
done
exec git "$@"
"#,
                marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut config = configuration(
            root.path(),
            &source,
            remote.endpoint.clone(),
            Duration::from_secs(60),
        );
        config.git = wrapper;
        assert!(
            crate::router(config.clone()).is_err(),
            "legacy API must not discard CAS shutdown ownership"
        );
        let mut server = HttpServer::start(config).await;
        server.post("cas-sync", json!({"oids":oids})).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !marker.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let pid = std::fs::read_to_string(marker).unwrap().trim().to_owned();
        drop(server.cas.take());
        let stopped = server.settled().await;
        assert_eq!(stopped["cancelled_jobs"], 1);
        assert_eq!(stopped["confirmed_objects"], 0);
        tokio::time::timeout(Duration::from_secs(2), async {
            while Path::new("/proc").join(&pid).exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("blocked Git process was not reaped");
        server.stop().await;
    }
}
