//! Optional shared immutable source-byte cache. Git identities and CAS identities
//! are deliberately separate: SHA1 authenticates Git's framed blob, while BLAKE3
//! authenticates the raw payload shared with BOS. Mutable overlays never enter it.
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use local_cas::{Artifact, BlobSource, CachedBlob, FetchContext, LocalCas};
use sha1::{Digest, Sha1};
use std::{
    fs::File,
    io::{Seek, Write},
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
};
use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore},
};

use crate::backend::{BackgroundCancellation, BlobReader};

type Job = Box<dyn FnOnce(&Handle, OwnedSemaphorePermit) + Send>;

/// One facade per daemon/cache scope, shared by all its source mounts. The
/// dedicated runtime keeps synchronous native callbacks off the daemon reactor.
/// Both queued and active bridge requests are bounded (32 each); LocalCas also
/// bounds admitted fetches and bytes. No per-read runtimes or whole-file spools.
pub struct SourceCas {
    cache: Arc<LocalCas>,
    backend: String,
    instance: String,
    jobs: Mutex<Option<mpsc::SyncSender<Job>>>,
    worker: Mutex<BridgeWorker>,
    runtime_id: tokio::runtime::Id,
}
struct BridgeWorker {
    handle: Option<std::thread::JoinHandle<()>>,
    failed: bool,
}
impl SourceCas {
    /// `backend` and `instance` must match the authorized BOS/cache-proxy scope
    /// to permit byte reuse. Never use a public endpoint alone as tenant identity.
    pub fn new(cache: Arc<LocalCas>, backend: String, instance: String) -> Result<Arc<Self>> {
        ensure!(!backend.is_empty(), "source CAS requires a security scope");
        let (jobs, receiver) = mpsc::sync_channel::<Job>(32);
        let (ready, initialized) = mpsc::sync_channel(1);
        let owned_cache = cache.clone();
        let worker = std::thread::Builder::new()
            .name("source-cas-bridge".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build();
                match runtime {
                    Ok(runtime) => {
                        let _ = ready.send(Ok(runtime.handle().id()));
                        let slots = Arc::new(Semaphore::new(32));
                        while let Ok(job) = receiver.recv() {
                            let permit = runtime.block_on(slots.clone().acquire_owned()).unwrap();
                            job(runtime.handle(), permit);
                        }
                        // Retain the reactor until consumers and cache-owned fetches
                        // finish, even if the last mount drops its facade mid-fetch.
                        runtime.block_on(async {
                            let _all = slots.acquire_many(32).await;
                            let _ = owned_cache.wait_for_idle().await;
                        });
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error));
                    }
                }
            })?;
        let runtime_id = initialized
            .recv()
            .context("initialize source CAS runtime")??;
        Ok(Arc::new(Self {
            cache,
            backend,
            instance,
            jobs: Mutex::new(Some(jobs)),
            worker: Mutex::new(BridgeWorker {
                handle: Some(worker),
                failed: false,
            }),
            runtime_id,
        }))
    }
    fn submit(&self, job: Job) -> Result<()> {
        let sender = self
            .jobs
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .context("source CAS bridge stopped")?;
        sender
            .send(job)
            .map_err(|_| anyhow::anyhow!("source CAS bridge stopped"))
    }
    /// Stop admission and join the bridge after all accepted work drains. This
    /// blocking, idempotent operation must run outside the bridge runtime, after
    /// source consumers stop. Ordinary Drop remains nonblocking for active reads.
    pub fn shutdown(&self) -> Result<()> {
        ensure!(
            Handle::try_current().map_or(true, |handle| handle.id() != self.runtime_id),
            "cannot shut down source CAS from its own runtime"
        );
        let mut worker = self.worker.lock().unwrap();
        ensure!(!worker.failed, "source CAS bridge failed during shutdown");
        drop(self.jobs.lock().unwrap().take());
        if let Some(handle) = worker.handle.take() {
            if handle.join().is_err() {
                worker.failed = true;
                anyhow::bail!("source CAS bridge failed during shutdown");
            }
        }
        Ok(())
    }
    pub fn artifact(&self, hash: [u8; 32], size: u64) -> Artifact {
        Artifact {
            backend: self.backend.clone(),
            instance: self.instance.clone(),
            digest_function: 9,
            hash: hex::encode(hash),
            size,
        }
    }
    pub(crate) fn readable(&self, size: u64) -> bool {
        size <= self.cache.config().readable_limit()
    }
    pub(crate) fn cached(&self, artifact: Artifact) -> Result<Option<Arc<CachedBlob>>> {
        let cache = self.cache.clone();
        let (send, receive) = mpsc::sync_channel(1);
        self.submit(Box::new(move |runtime, permit| {
            runtime.spawn(async move {
                let _permit = permit;
                let _ = send.send(cache.cached_content(artifact).await);
            });
        }))?;
        receive.recv().context("source CAS worker stopped")?
    }
    pub(crate) fn fallback(&self) -> Arc<dyn BlobSource> {
        self.cache.source()
    }
    pub(crate) fn content(
        &self,
        artifact: Artifact,
        source: Arc<dyn BlobSource>,
    ) -> Result<Arc<CachedBlob>> {
        let cache = self.cache.clone();
        let (send, receive) = mpsc::sync_channel(1);
        self.submit(Box::new(move |runtime, permit| {
            runtime.spawn(async move {
                let _permit = permit;
                let result = cache.content_from(artifact, source).await;
                let _ = send.send(result);
            });
        }))?;
        receive.recv().context("source CAS worker stopped")?
    }
}

pub(crate) struct GitBlobSource {
    pub repository: PathBuf,
    pub binary: PathBuf,
    pub oid: String,
    pub fallback: Arc<dyn BlobSource>,
    pub allow_fetch: bool,
}
#[derive(Debug)]
pub(crate) struct MissingGitBlob;
impl std::fmt::Display for MissingGitBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Git blob is not local")
    }
}
impl std::error::Error for MissingGitBlob {}

impl GitBlobSource {
    async fn fetch_impl(
        &self,
        artifact: &Artifact,
        target: &mut File,
        context: Option<FetchContext>,
    ) -> Result<()> {
        let repository = self.repository.clone();
        let binary = self.binary.clone();
        let oid = self.oid.clone();
        // This fetch belongs to the shared cache flight, not any one mount.
        // Only aborting the cache-owned fetch future cancels its native reader.
        struct CancelOnDrop(BackgroundCancellation);
        impl Drop for CancelOnDrop {
            fn drop(&mut self) {
                self.0.cancel();
            }
        }
        let cancellation = BackgroundCancellation::default();
        let _cancel = CancelOnDrop(cancellation.clone());
        let allow_fetch = self.allow_fetch;
        let expected_size = artifact.size;
        let git_target = target.try_clone()?;
        // Capture ONE ordered owner before dispatch, so even dropping queued
        // work closes its target before releasing cache lifetime resources.
        struct GitWork {
            target: File,
            _context: Option<FetchContext>,
        }
        let work = GitWork {
            target: git_target,
            _context: context.clone(),
        };
        let result = tokio::task::spawn_blocking(move || {
            let mut work = work;
            let mut reader = if allow_fetch {
                BlobReader::start(&repository, &binary, Some(&cancellation))?
            } else {
                BlobReader::start_local(&repository, &binary, Some(&cancellation))?
            };
            let mut hash = Sha1::new();
            let found = reader.stream_local(&oid, |size, _, bytes| {
                ensure!(size == expected_size, "Git/CAS mapping size mismatch");
                if bytes.is_empty() {
                    hash.update(format!("blob {size}\0").as_bytes());
                }
                hash.update(bytes);
                work.target.write_all(bytes)?;
                Ok(())
            })?;
            ensure!(found.is_some(), MissingGitBlob);
            ensure!(
                hex::encode(hash.finalize()) == oid,
                "Git blob SHA1 verification failed"
            );
            Ok(())
        })
        .await
        .context("source Git reader panicked")?;
        if result.is_ok() {
            return result;
        }
        // A Git repository can disappear or be GC'd while a shared flight is
        // active. Recover directly through its authorized upstream source.
        target.set_len(0)?;
        target.rewind()?;
        match context {
            Some(context) => {
                self.fallback
                    .fetch_with_context(artifact, target, context)
                    .await
            }
            None => self.fallback.fetch(artifact, target).await,
        }
    }
}
#[async_trait]
impl BlobSource for GitBlobSource {
    async fn fetch(&self, artifact: &Artifact, target: &mut File) -> Result<()> {
        self.fetch_impl(artifact, target, None).await
    }
    async fn fetch_with_context(
        &self,
        artifact: &Artifact,
        target: &mut File,
        context: FetchContext,
    ) -> Result<()> {
        self.fetch_impl(artifact, target, Some(context)).await
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        anyhow::bail!("Git source is not a remote CAS presence oracle")
    }
}

pub(crate) fn verify_git_blob(oid: &str, bytes: &[u8]) -> Result<[u8; 32]> {
    let mut git = Sha1::new();
    git.update(format!("blob {}\0", bytes.len()));
    git.update(bytes);
    ensure!(
        hex::encode(git.finalize()) == oid,
        "Git blob SHA1 verification failed"
    );
    Ok(*blake3::hash(bytes).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{GitRepo, SizeFallback};
    use local_cas::CacheConfig;
    use std::{
        io::Write,
        process::{Command, Stdio},
    };

    struct Bytes(Vec<u8>);
    #[async_trait]
    impl BlobSource for Bytes {
        async fn fetch(&self, _: &Artifact, file: &mut File) -> Result<()> {
            file.write_all(&self.0)?;
            Ok(())
        }
        async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
            Ok(vec![])
        }
    }
    fn fixture() -> (tempfile::TempDir, GitRepo, String) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("repo.git");
        assert!(
            Command::new("git")
                .args(["init", "--bare", "--quiet"])
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );
        let mut child = Command::new("git")
            .arg("--git-dir")
            .arg(&path)
            .args(["hash-object", "-w", "--stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(b"base").unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        let oid = String::from_utf8(output.stdout).unwrap().trim().to_owned();
        let repo = GitRepo::open(path, "git", "origin", SizeFallback::Error).unwrap();
        (temp, repo, oid)
    }
    #[test]
    fn git_reads_share_output_bytes_and_recover_after_eviction() {
        let (temp, mut repo, oid) = fixture();
        let source: Arc<dyn BlobSource> = Arc::new(Bytes(b"other".to_vec()));
        let cache = LocalCas::open(
            temp.path().join("cas"),
            source,
            CacheConfig {
                max_bytes: 8,
                ..Default::default()
            },
        )
        .unwrap();
        let shared = SourceCas::new(cache.clone(), "tenant-a".into(), "build".into()).unwrap();
        repo.set_shared_cas(shared.clone());
        assert_eq!(repo.read_blob(&oid).unwrap(), b"base");
        assert_eq!(cache.metrics().fetches, 1);
        // A BOS/output consumer uses raw BLAKE3, not Git's framed SHA1.
        let artifact = shared.artifact(*blake3::hash(b"base").as_bytes(), 4);
        let lease = shared
            .content(artifact, Arc::new(Bytes(b"wrong bytes".to_vec())))
            .unwrap();
        assert_eq!(std::fs::read(lease.path()).unwrap(), b"base");
        drop(lease);
        assert_eq!(cache.metrics().fetches, 1);
        // A caller may mutate its returned buffer without changing cached bytes.
        let mut edited = repo.read_blob(&oid).unwrap();
        edited[0] = b'X';
        assert_eq!(repo.read_blob(&oid).unwrap(), b"base");
        let other = shared.artifact(*blake3::hash(b"other").as_bytes(), 5);
        drop(
            shared
                .content(other, Arc::new(Bytes(b"other".to_vec())))
                .unwrap(),
        );
        assert!(cache.metrics().evictions >= 1);
        assert_eq!(repo.read_blob(&oid).unwrap(), b"base");
        assert_eq!(cache.metrics().fetches, 3);
        drop(repo);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| shared.shutdown().unwrap());
            let second = scope.spawn(|| shared.shutdown().unwrap());
            first.join().unwrap();
            second.join().unwrap();
        });
        assert!(
            shared
                .cached(shared.artifact(*blake3::hash(b"base").as_bytes(), 4))
                .is_err()
        );
        let ownership = Arc::downgrade(&cache);
        drop(cache);
        drop(shared);
        assert!(
            ownership.upgrade().is_none(),
            "shutdown must join the cache owner"
        );
        LocalCas::open(
            temp.path().join("cas"),
            Arc::new(Bytes(vec![])),
            CacheConfig::default(),
        )
        .unwrap();
    }
    #[test]
    fn mapped_git_bytes_are_not_interchangeable_with_unverified_payloads() {
        let (temp, mut repo, oid) = fixture();
        let source: Arc<dyn BlobSource> = Arc::new(Bytes(vec![]));
        let cache =
            LocalCas::open(temp.path().join("cas"), source, CacheConfig::default()).unwrap();
        let shared = SourceCas::new(cache, "tenant-a".into(), "".into()).unwrap();
        repo.set_shared_cas(shared.clone());
        // A trusted mapping can still be stale/corrupt. Git's correct bytes
        // must not be published under an unrelated BLAKE3 digest.
        let source = Arc::new(GitBlobSource {
            repository: repo.repository_path().to_owned(),
            binary: "git".into(),
            oid,
            fallback: Arc::new(Bytes(vec![])),
            allow_fetch: false,
        });
        assert!(
            shared
                .content(
                    shared.artifact(*blake3::hash(b"fake").as_bytes(), 4),
                    source
                )
                .is_err()
        );
        // Missing local Git objects may fall back to a source which detaches
        // physical I/O. The cache context must reach that source, not the
        // borrow-only compatibility method.
        struct ContextFallback;
        #[async_trait]
        impl BlobSource for ContextFallback {
            async fn fetch(&self, _: &Artifact, _: &mut File) -> Result<()> {
                anyhow::bail!("cache lifetime context was not forwarded to fallback")
            }
            async fn fetch_with_context(
                &self,
                _: &Artifact,
                target: &mut File,
                context: FetchContext,
            ) -> Result<()> {
                let _context = context;
                target.write_all(b"base")?;
                Ok(())
            }
            async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
                anyhow::bail!("unexpected fallback presence query")
            }
        }
        let recovered = shared
            .content(
                shared.artifact(*blake3::hash(b"base").as_bytes(), 4),
                Arc::new(GitBlobSource {
                    repository: repo.repository_path().to_owned(),
                    binary: "git".into(),
                    oid: "f".repeat(40),
                    fallback: Arc::new(ContextFallback),
                    allow_fetch: false,
                }),
            )
            .unwrap();
        assert_eq!(std::fs::read(recovered.path()).unwrap(), b"base");
        assert_ne!(
            shared.artifact(*blake3::hash(b"base").as_bytes(), 4),
            Artifact {
                backend: "tenant-b".into(),
                ..shared.artifact(*blake3::hash(b"base").as_bytes(), 4)
            }
        );
    }
    #[test]
    fn scheduled_source_ranges_use_shared_cas_without_native_git_objects() {
        let (temp, mut repo, oid) = fixture();
        let cache = LocalCas::open(
            temp.path().join("cas"),
            Arc::new(Bytes(vec![])),
            CacheConfig::default(),
        )
        .unwrap();
        repo.set_shared_cas(SourceCas::new(cache.clone(), "tenant-a".into(), "".into()).unwrap());
        let mut scheduler =
            crate::backend::ContentScheduler::new(&mut repo, Default::default()).unwrap();
        let handle = scheduler.handle();
        // Unknown mappings are learned by the streaming scheduler path.
        assert_eq!(&*handle.read_blocking(&oid, 1, 2).unwrap(), b"as");
        assert!(repo.cached_blake3(&oid).unwrap().is_some());
        assert_eq!(cache.metrics().fetches, 1);
        // Simulate Git storage being unavailable. No promisor fetch is needed
        // when a verified payload is already retained in the shared cache.
        std::fs::remove_file(
            repo.repository_path()
                .join("objects")
                .join(&oid[..2])
                .join(&oid[2..]),
        )
        .unwrap();
        assert_eq!(&*handle.read_blocking(&oid, 2, 2).unwrap(), b"se");
        assert_eq!(cache.metrics().fetches, 1);
        scheduler.shutdown().unwrap();
    }

    struct GatedBytes {
        entered: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
    }
    #[async_trait]
    impl BlobSource for GatedBytes {
        async fn fetch(&self, _: &Artifact, file: &mut File) -> Result<()> {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            file.write_all(b"base")?;
            Ok(())
        }
        async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
            Ok(vec![])
        }
    }
    #[tokio::test]
    async fn explicit_bridge_shutdown_drains_active_work_and_releases_cache_ownership() {
        let temp = tempfile::tempdir().unwrap();
        let source = Arc::new(GatedBytes {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let cache = LocalCas::open(temp.path(), source.clone(), CacheConfig::default()).unwrap();
        let ownership = Arc::downgrade(&cache);
        let shared = SourceCas::new(cache.clone(), "tenant-a".into(), "".into()).unwrap();
        let artifact = shared.artifact(*blake3::hash(b"base").as_bytes(), 4);
        let reader = shared.clone();
        let bytes = source.clone();
        let hydration = tokio::task::spawn_blocking(move || reader.content(artifact, bytes));
        source.entered.acquire().await.unwrap().forget();
        let first = shared.clone();
        let mut first = tokio::task::spawn_blocking(move || {
            let result = first.shutdown();
            drop(first);
            result
        });
        let second = shared.clone();
        let second = tokio::task::spawn_blocking(move || {
            let result = second.shutdown();
            drop(second);
            result
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), &mut first)
                .await
                .is_err()
        );
        assert!(!second.is_finished());
        source.release.add_permits(1);
        let blob = hydration.await.unwrap().unwrap();
        assert_eq!(std::fs::read(blob.path()).unwrap(), b"base");
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        drop(blob);
        drop(shared);
        drop(cache);
        assert!(ownership.upgrade().is_none());
        LocalCas::open(temp.path(), source, CacheConfig::default()).unwrap();
    }

    #[tokio::test]
    async fn local_only_probe_does_not_join_or_poison_upstream_hydration() {
        let temp = tempfile::tempdir().unwrap();
        let source = Arc::new(GatedBytes {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let cache = LocalCas::open(temp.path(), source.clone(), CacheConfig::default()).unwrap();
        let artifact = Artifact {
            backend: "tenant-a".into(),
            instance: "".into(),
            digest_function: 9,
            hash: blake3::hash(b"base").to_hex().to_string(),
            size: 4,
        };
        let hydrate = tokio::spawn(cache.clone().content(artifact.clone()));
        source.entered.acquire().await.unwrap().forget();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                cache.clone().cached_content(artifact.clone())
            )
            .await
            .unwrap()
            .unwrap()
            .is_none()
        );
        assert!(!hydrate.is_finished());
        source.release.add_permits(1);
        let blob = hydrate.await.unwrap().unwrap();
        assert_eq!(std::fs::read(blob.path()).unwrap(), b"base");
        assert_eq!(cache.metrics().fetches, 1);
        assert!(cache.cached_content(artifact).await.unwrap().is_some());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelling_one_mount_does_not_cancel_a_shared_native_git_flight() {
        use std::os::unix::fs::PermissionsExt;
        let (temp, mut repo, oid) = fixture();
        let cache = LocalCas::open(
            temp.path().join("cas"),
            Arc::new(Bytes(b"other".to_vec())),
            CacheConfig {
                max_bytes: 8,
                ..Default::default()
            },
        )
        .unwrap();
        let shared = SourceCas::new(cache.clone(), "tenant-a".into(), "".into()).unwrap();
        repo.set_shared_cas(shared.clone());
        assert_eq!(repo.read_blob(&oid).unwrap(), b"base");
        drop(
            shared
                .content(
                    shared.artifact(*blake3::hash(b"other").as_bytes(), 5),
                    Arc::new(Bytes(b"other".to_vec())),
                )
                .unwrap(),
        );
        let wrapper = temp.path().join("controlled-git");
        std::fs::write(
            &wrapper,
            r#"#!/usr/bin/env python3
import os,pathlib,sys,time
root=pathlib.Path(__file__).parent
if 'cat-file' in sys.argv:
    (root/'started').touch()
    while not (root/'release').exists(): time.sleep(.01)
os.execvp('git',['git',*sys.argv[1:]])
"#,
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut mounted = GitRepo::open(
            repo.repository_path(),
            &wrapper,
            "origin",
            SizeFallback::Error,
        )
        .unwrap();
        mounted.set_shared_cas(shared.clone());
        let (_, cancel_mount) = mounted.background_handle().unwrap();
        let read_oid = oid.clone();
        let source_read = tokio::task::spawn_blocking(move || mounted.read_blob(&read_oid));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !temp.path().join("started").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let output_read = tokio::spawn(
            cache
                .clone()
                .content(shared.artifact(*blake3::hash(b"base").as_bytes(), 4)),
        );
        cancel_mount.cancel();
        std::fs::write(temp.path().join("release"), b"go").unwrap();
        assert_eq!(source_read.await.unwrap().unwrap(), b"base");
        assert_eq!(
            std::fs::read(output_read.await.unwrap().unwrap().path()).unwrap(),
            b"base"
        );
        assert_eq!(cache.metrics().fetches, 3);
    }

    #[test]
    fn synchronous_source_read_is_safe_inside_current_thread_runtime() {
        let (temp, mut repo, oid) = fixture();
        let cache = LocalCas::open(
            temp.path().join("cas"),
            Arc::new(Bytes(vec![])),
            CacheConfig::default(),
        )
        .unwrap();
        repo.set_shared_cas(SourceCas::new(cache, "tenant-a".into(), "".into()).unwrap());
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                assert_eq!(repo.read_blob(&oid).unwrap(), b"base");
                assert_eq!(repo.read_blob(&oid).unwrap(), b"base");
            });
    }
}
