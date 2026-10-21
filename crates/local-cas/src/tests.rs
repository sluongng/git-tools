use super::*;
use std::{io::Write, process::Command, sync::atomic::AtomicBool};

#[derive(Default)]
struct Source {
    calls: AtomicU64,
    accesses: AtomicU64,
    reject: AtomicBool,
}
#[async_trait]
impl BlobSource for Source {
    async fn fetch(&self, artifact: &Artifact, target: &mut File) -> Result<()> {
        ensure!(!self.reject.load(Ordering::Relaxed), "unexpected fetch");
        self.calls.fetch_add(1, Ordering::Relaxed);
        let bytes = match artifact.hash.as_str() {
            h if h == blake3::hash(b"aaaa").to_hex().as_str() => b"aaaa",
            h if h == blake3::hash(b"bbbb").to_hex().as_str() => b"bbbb",
            _ => b"cccc",
        };
        target.write_all(bytes)?;
        Ok(())
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        Ok(vec![])
    }
    fn accessed(&self, _: &Artifact, _: Weak<LocalCas>) {
        self.accesses.fetch_add(1, Ordering::Relaxed);
    }
}
fn artifact(bytes: &[u8]) -> Artifact {
    Artifact {
        backend: "https://cache.example.test/auth-scope-a".into(),
        instance: "instance".into(),
        digest_function: 9,
        hash: blake3::hash(bytes).to_hex().to_string(),
        size: bytes.len() as u64,
    }
}

#[tokio::test]
async fn shared_handles_apply_lru_without_evicting_live_leases() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Source::default());
    let cache = LocalCas::open(
        dir.path(),
        source.clone(),
        CacheConfig {
            max_bytes: 8,
            ..Default::default()
        },
    )
    .unwrap();
    let first = artifact(b"aaaa");
    let second = artifact(b"bbbb");
    let third = artifact(b"cccc");
    let first_path = cache
        .clone()
        .content(first.clone())
        .await
        .unwrap()
        .path()
        .to_owned();
    let second_path = cache
        .clone()
        .content(second.clone())
        .await
        .unwrap()
        .path()
        .to_owned();
    let lease = cache.clone().content(first.clone()).await.unwrap();
    let third_lease = cache.clone().content(third).await.unwrap();
    assert!(first_path.exists());
    assert!(
        !second_path.exists(),
        "least recently used unpinned blob is evicted"
    );
    assert!(
        cache.clone().content(second.clone()).await.is_err(),
        "all cache bytes are pinned"
    );
    assert_eq!(fs::read(lease.path()).unwrap(), b"aaaa");
    drop(lease);
    drop(third_lease);
    let second_lease = cache.clone().content(second).await.unwrap();
    assert_eq!(fs::read(second_lease.path()).unwrap(), b"bbbb");
    assert_eq!(source.calls.load(Ordering::Relaxed), 4);
}

#[test]
#[ignore = "subprocess fixture for the exclusive ownership test"]
fn ownership_probe_child() {
    let path = std::env::var_os("LOCAL_CAS_LOCK_PROBE").expect("probe cache directory");
    assert!(
        LocalCas::open(
            PathBuf::from(path),
            Arc::new(Source::default()),
            CacheConfig::default()
        )
        .is_err()
    );
}

#[tokio::test]
async fn ownership_lock_survives_handle_drop_until_last_lease_is_released() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Source::default());
    let cache = LocalCas::open(dir.path(), source.clone(), CacheConfig::default()).unwrap();
    let lease = cache.clone().content(artifact(b"aaaa")).await.unwrap();
    drop(cache);
    assert!(LocalCas::open(dir.path(), source.clone(), CacheConfig::default()).is_err());
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "tests::ownership_probe_child", "--ignored"])
        .env("LOCAL_CAS_LOCK_PROBE", dir.path())
        .status()
        .unwrap();
    assert!(
        status.success(),
        "separate processes must respect directory ownership"
    );
    assert_eq!(fs::read(lease.path()).unwrap(), b"aaaa");
    drop(lease);
    LocalCas::open(dir.path(), source, CacheConfig::default()).unwrap();
}

#[tokio::test]
async fn restart_reverifies_bytes_and_cleans_only_abandoned_temporaries() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Source::default());
    let cache = LocalCas::open(dir.path(), source.clone(), CacheConfig::default()).unwrap();
    let a = artifact(b"aaaa");
    let path = cache
        .clone()
        .content(a.clone())
        .await
        .unwrap()
        .path()
        .to_owned();
    drop(cache);
    fs::write(&path, b"bad!").unwrap();
    fs::write(dir.path().join("123-1.tmp"), b"interrupted").unwrap();
    fs::write(dir.path().join("not-a-cache-blob"), b"unrelated").unwrap();
    let cache = LocalCas::open(dir.path(), source.clone(), CacheConfig::default()).unwrap();
    assert!(!dir.path().join("123-1.tmp").exists());
    assert!(dir.path().join("not-a-cache-blob").exists());
    let repaired = cache.content(a).await.unwrap();
    assert_eq!(fs::read(repaired.path()).unwrap(), b"aaaa");
    assert_eq!(source.calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn cache_keys_separate_backend_authorization_scopes_and_instances() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Source::default());
    let cache = LocalCas::open(dir.path(), source.clone(), CacheConfig::default()).unwrap();
    let a = artifact(b"aaaa");
    let mut other_instance = a.clone();
    other_instance.instance = "another-instance".into();
    let mut other_scope = a.clone();
    other_scope.backend = "https://cache.example.test/auth-scope-b".into();
    let first = cache.clone().content(a.clone()).await.unwrap();
    let second = cache.clone().content(other_instance).await.unwrap();
    let third = cache.clone().content(other_scope).await.unwrap();
    assert_ne!(first.path(), second.path());
    assert_ne!(first.path(), third.path());
    assert_eq!(cache.clone().content(a).await.unwrap().path(), first.path());
    assert_eq!(source.calls.load(Ordering::Relaxed), 3);
    assert_eq!(cache.metrics().hits, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_fetch_and_hit_release_ownership_before_returning() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Source::default());
    for _ in 0..64 {
        let cache = LocalCas::open(dir.path(), source.clone(), CacheConfig::default()).unwrap();
        let weak = Arc::downgrade(&cache);
        let a = artifact(b"aaaa");
        let fetched = cache.clone().content(a.clone()).await.unwrap();
        let hit = cache.clone().content(a).await.unwrap();
        let path = fetched.path().to_owned();
        drop(cache);
        assert!(weak.upgrade().is_some(), "leases retain ownership");
        drop(fetched);
        drop(hit);
        assert!(
            weak.upgrade().is_none(),
            "a completed worker must not retain ownership"
        );
        // Force the next iteration down the fetch path rather than only hits.
        fs::remove_file(path).unwrap();
    }
    LocalCas::open(dir.path(), source, CacheConfig::default()).unwrap();
}

#[test]
fn duplicate_descriptor_does_not_extend_logical_cache_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Source::default());
    let cache = LocalCas::open(dir.path(), source.clone(), CacheConfig::default()).unwrap();
    // Model a descriptor briefly inherited by another thread's fork/exec.
    let inherited = cache._ownership.0.try_clone().unwrap();
    drop(cache);
    let reopened = LocalCas::open(dir.path(), source.clone(), CacheConfig::default()).unwrap();
    drop(inherited);
    assert!(
        LocalCas::open(dir.path(), source, CacheConfig::default()).is_err(),
        "closing the old descriptor must not unlock the new owner"
    );
    drop(reopened);
}

struct BytesSource(Vec<u8>);
#[async_trait]
impl BlobSource for BytesSource {
    async fn fetch(&self, _: &Artifact, target: &mut File) -> Result<()> {
        target.write_all(&self.0)?;
        Ok(())
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        Ok(vec![])
    }
}
async fn bytes(cache: &Arc<LocalCas>, data: &[u8]) -> Arc<CachedBlob> {
    cache
        .clone()
        .content_from(artifact(data), Arc::new(BytesSource(data.to_vec())))
        .await
        .unwrap()
}

#[tokio::test]
async fn multi_victim_lru_preserves_pins_and_restores_unpinned_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let cache = LocalCas::open(
        dir.path(),
        Arc::new(Source::default()),
        CacheConfig {
            max_bytes: 16,
            ..Default::default()
        },
    )
    .unwrap();
    let mut paths = vec![];
    for data in [b"aaaa", b"bbbb", b"cccc", b"dddd"] {
        paths.push(bytes(&cache, data).await.path().to_owned());
    }
    let pinned = bytes(&cache, b"bbbb").await;
    drop(bytes(&cache, b"aaaa").await);
    let large = bytes(&cache, b"abcdefghijkl").await;
    assert_eq!(cache.metrics().evictions, 3);
    assert_eq!(cache.metrics().evicted_bytes, 12);
    assert_eq!(cache.metrics().resident_bytes, 16);
    assert_eq!(cache.metrics().reserved_bytes, 0);
    assert!(paths[1].exists());
    for index in [0, 2, 3] {
        assert!(!paths[index].exists());
    }
    drop(pinned);
    let replacement = bytes(&cache, b"eeee").await;
    assert!(!paths[1].exists());
    assert_eq!(fs::read(large.path()).unwrap(), b"abcdefghijkl");
    assert_eq!(fs::read(replacement.path()).unwrap(), b"eeee");
    assert_eq!(cache.metrics().evictions, 4);
}

#[tokio::test]
async fn checkpoint_preserves_hot_entries_under_reduced_restart_quota() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Source::default());
    let cache = LocalCas::open(
        dir.path(),
        source.clone(),
        CacheConfig {
            max_bytes: 12,
            ..Default::default()
        },
    )
    .unwrap();
    let mut paths = vec![];
    for data in [b"aaaa", b"bbbb", b"cccc"] {
        paths.push(bytes(&cache, data).await.path().to_owned());
    }
    let modified = fs::metadata(&paths[0]).unwrap().modified().unwrap();
    // Advance only the checkpoint's clock; no sleeps are needed to exercise
    // the real worker's periodic blocking-IO path and its ownership barrier.
    cache.checkpoint.lock().unwrap().last = std::time::Instant::now() - Duration::from_secs(6);
    drop(bytes(&cache, b"aaaa").await);
    assert!(dir.path().join("recency.json").exists());
    assert_eq!(
        fs::metadata(&paths[0]).unwrap().modified().unwrap(),
        modified,
        "cache reads must not touch payload mtimes"
    );
    drop(cache);
    let cache = LocalCas::open(
        dir.path(),
        source.clone(),
        CacheConfig {
            max_bytes: 4,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(paths[0].exists());
    assert!(!paths[1].exists());
    assert!(!paths[2].exists());
    source.reject.store(true, Ordering::Relaxed);
    assert_eq!(
        fs::read(
            cache
                .clone()
                .content(artifact(b"aaaa"))
                .await
                .unwrap()
                .path()
        )
        .unwrap(),
        b"aaaa"
    );
    assert_eq!(
        cache.metrics().verification_bytes,
        4,
        "recency never grants integrity authority"
    );
    assert_eq!(cache.metrics().resident_bytes, 4);
}

#[tokio::test]
async fn corrupt_or_unsupported_recency_uses_deterministic_safe_fallback() {
    for hint in [
        b"not-json".as_slice(),
        br#"{"version":99,"entries":{}}"#.as_slice(),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalCas::open(
            dir.path(),
            Arc::new(Source::default()),
            CacheConfig::default(),
        )
        .unwrap();
        let mut paths = vec![];
        for data in [b"aaaa", b"bbbb", b"cccc"] {
            paths.push(bytes(&cache, data).await.path().to_owned());
        }
        drop(cache);
        fs::write(dir.path().join("recency.json"), hint).unwrap();
        let cache = LocalCas::open(
            dir.path(),
            Arc::new(Source::default()),
            CacheConfig {
                max_bytes: 4,
                ..Default::default()
            },
        )
        .unwrap();
        paths.sort();
        assert!(!paths[0].exists());
        assert!(!paths[1].exists());
        assert!(paths[2].exists());
        assert_eq!(cache.metrics().recency_errors, 1);
        assert_eq!(cache.metrics().resident_bytes, 4);
    }
}

struct HeldSource {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
}
impl Drop for HeldSource {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
#[async_trait]
impl BlobSource for HeldSource {
    async fn fetch(&self, _: &Artifact, target: &mut File) -> Result<()> {
        self.entered.notify_one();
        self.release.notified().await;
        target.write_all(b"aaaa")?;
        Ok(())
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn source_override_survives_cancelled_waiter_and_coalesces_without_default_source() {
    let dir = tempfile::tempdir().unwrap();
    let default_source = Arc::new(Source::default());
    default_source.reject.store(true, Ordering::Relaxed);
    let cache = LocalCas::open(
        dir.path(),
        default_source.clone(),
        CacheConfig {
            max_inflight_bytes: 2,
            max_oversized_inflight_bytes: 4,
            ..Default::default()
        },
    )
    .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let source = Arc::new(HeldSource {
        entered: entered.clone(),
        release: release.clone(),
        dropped: dropped.clone(),
    });
    let first = tokio::spawn(cache.clone().content_from(artifact(b"aaaa"), source));
    entered.notified().await;
    first.abort();
    assert!(first.await.err().unwrap().is_cancelled());
    assert!(
        !dropped.load(Ordering::SeqCst),
        "the admitted fetch owns its source independently"
    );
    let second = cache
        .clone()
        .content_from(artifact(b"aaaa"), default_source.clone());
    tokio::pin!(second);
    // Poll the second caller into the existing flight before releasing it.
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(second.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    release.notify_one();
    let lease = second.await.unwrap();
    assert_eq!(fs::read(lease.path()).unwrap(), b"aaaa");
    assert!(
        dropped.load(Ordering::SeqCst),
        "source ownership ends before completion is visible"
    );
    assert_eq!(cache.metrics().fetches, 1);
    assert_eq!(default_source.calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        default_source.accesses.load(Ordering::Relaxed),
        1,
        "a source override still notifies the shared cache's access observer"
    );
    drop(
        cache
            .clone()
            .cached_content(artifact(b"aaaa"))
            .await
            .unwrap(),
    );
    assert_eq!(default_source.accesses.load(Ordering::Relaxed), 2);
    drop(
        cache
            .clone()
            .cached_content_for_maintenance(
                artifact(b"aaaa"),
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap(),
    );
    assert_eq!(
        default_source.accesses.load(Ordering::Relaxed),
        2,
        "background verification cannot manufacture another user access"
    );
}

struct FailedSource;
#[test]
fn maintenance_verification_has_separate_admission_and_stops_when_cancelled() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalCas::open(
            dir.path(),
            Arc::new(Source::default()),
            CacheConfig {
                max_concurrent_fetches: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let a = bytes(&cache, b"aaaa").await;
        let path = a.path().to_owned();
        drop(a);
        drop(bytes(&cache, b"bbbb").await);
        let before = cache.metrics().verification_bytes;
        std::fs::write(path, b"aaaa").unwrap();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            wait.recv().unwrap();
        });
        started.await.unwrap();
        // Poll until verification is queued behind the deliberately occupied
        // blocking lane. The resident cache file has a changed fingerprint.
        let mut maintenance = Box::pin(cache.clone().cached_content_for_maintenance(
            artifact(b"aaaa"),
            tokio::time::Instant::now() + Duration::from_secs(1),
        ));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(maintenance.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let foreground = tokio::time::timeout(
            Duration::from_millis(200),
            cache.clone().cached_content(artifact(b"bbbb")),
        )
        .await;
        drop(maintenance);
        release.send(()).unwrap();
        blocker.await.unwrap();
        cache.wait_for_idle().await.unwrap();
        assert!(
            foreground
                .expect("maintenance must not own the foreground admission slot")
                .unwrap()
                .is_some()
        );
        assert_eq!(
            cache.metrics().verification_bytes,
            before,
            "cancelled queued verification must not hash the file after it starts"
        );
    });
}
#[async_trait]
impl BlobSource for FailedSource {
    async fn fetch(&self, _: &Artifact, target: &mut File) -> Result<()> {
        target.write_all(b"par")?;
        anyhow::bail!("source failed")
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        Ok(vec![])
    }
}
#[tokio::test]
async fn failed_fetch_releases_reservation_and_publication_never_double_charges_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let cache = LocalCas::open(
        dir.path(),
        Arc::new(Source::default()),
        CacheConfig {
            max_bytes: 4,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        cache
            .clone()
            .content_from(artifact(b"aaaa"), Arc::new(FailedSource))
            .await
            .is_err()
    );
    assert_eq!(cache.metrics().reserved_bytes, 0);
    assert_eq!(cache.metrics().resident_bytes, 0);
    let first = bytes(&cache, b"aaaa").await;
    assert_eq!(cache.metrics().reserved_bytes, 0);
    assert_eq!(cache.metrics().resident_bytes, 4);
    drop(first);
    let second = bytes(&cache, b"bbbb").await;
    assert_eq!(cache.metrics().resident_bytes, 4);
    assert_eq!(cache.metrics().reserved_bytes, 0);
    assert_eq!(fs::read(second.path()).unwrap(), b"bbbb");
}

#[tokio::test]
async fn stale_and_maximal_recency_hints_are_normalized_without_trusting_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let cache = LocalCas::open(
        dir.path(),
        Arc::new(Source::default()),
        CacheConfig::default(),
    )
    .unwrap();
    let hot_path = bytes(&cache, b"aaaa").await.path().to_owned();
    let cold_path = bytes(&cache, b"bbbb").await.path().to_owned();
    drop(cache);
    let mut entries = HashMap::new();
    entries.insert(
        hot_path.file_name().unwrap().to_str().unwrap().to_owned(),
        u64::MAX,
    );
    entries.insert("f".repeat(64), u64::MAX); // stale hint, no payload
    fs::write(
        dir.path().join("recency.json"),
        serde_json::to_vec(&Recency {
            version: 1,
            entries,
        })
        .unwrap(),
    )
    .unwrap();
    let cache = LocalCas::open(
        dir.path(),
        Arc::new(Source::default()),
        CacheConfig {
            max_bytes: 4,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(hot_path.exists());
    assert!(!cold_path.exists());
    let lease = cache.clone().content(artifact(b"aaaa")).await.unwrap();
    assert_eq!(cache.metrics().verification_bytes, 4);
    assert_eq!(fs::read(lease.path()).unwrap(), b"aaaa");
    cache.flush_recency().unwrap();
    let normalized: Recency =
        serde_json::from_slice(&fs::read(dir.path().join("recency.json")).unwrap()).unwrap();
    assert_eq!(normalized.entries.len(), 1);
    assert!(normalized.entries.values().all(|rank| *rank < 100));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_barrier_waits_for_fetch_teardown_after_its_waiter_times_out() {
    let dir = tempfile::tempdir().unwrap();
    let cache = LocalCas::open(
        dir.path(),
        Arc::new(Source::default()),
        CacheConfig {
            max_concurrent_fetches: 1,
            max_inflight_bytes: 2,
            max_oversized_inflight_bytes: 4,
            operation_timeout: Duration::from_millis(10),
            fetch_timeout: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let source = Arc::new(HeldSource {
        entered: entered.clone(),
        release: release.clone(),
        dropped: dropped.clone(),
    });
    let caller = tokio::spawn(cache.clone().content_from(artifact(b"aaaa"), source));
    entered.notified().await;
    let error = caller.await.unwrap().err().unwrap();
    assert!(error.downcast_ref::<CacheTimeoutError>().is_some());
    assert!(!dropped.load(Ordering::SeqCst));
    {
        let idle = cache.wait_for_idle();
        tokio::pin!(idle);
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(idle.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        release.notify_one();
        idle.as_mut().await.unwrap();
        assert!(dropped.load(Ordering::SeqCst));
    }
    drop(cache);
    LocalCas::open(
        dir.path(),
        Arc::new(Source::default()),
        CacheConfig::default(),
    )
    .unwrap();
}

#[test]
fn idle_barrier_includes_blocking_verification_after_fetch_timeout() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let (release, blocked) = std::sync::mpsc::channel();
    let (entered, started) = std::sync::mpsc::channel();
    let blocker = runtime.spawn_blocking(move || {
        entered.send(()).unwrap();
        blocked.recv().unwrap();
    });
    started.recv().unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalCas::open(
            dir.path(),
            Arc::new(Source::default()),
            CacheConfig {
                max_concurrent_fetches: 1,
                operation_timeout: Duration::from_secs(2),
                fetch_timeout: Duration::from_millis(20),
                ..Default::default()
            },
        )
        .unwrap();
        // Fetch completes immediately, but verification queues behind blocker.
        let error = cache
            .clone()
            .content(artifact(b"aaaa"))
            .await
            .err()
            .unwrap();
        assert!(error.downcast_ref::<CacheTimeoutError>().is_some());
        assert_eq!(cache.metrics().fetches, 1);
        assert_eq!(cache.metrics().timed_out_fetches, 1);
        {
            let idle = cache.wait_for_idle();
            tokio::pin!(idle);
            std::future::poll_fn(|cx| {
                assert!(
                    std::future::Future::poll(idle.as_mut(), cx).is_pending(),
                    "a detached verifier must retain its fetch permit"
                );
                std::task::Poll::Ready(())
            })
            .await;
            release.send(()).unwrap();
            idle.await.unwrap();
        }
        blocker.await.unwrap();
        drop(cache);
        LocalCas::open(
            dir.path(),
            Arc::new(Source::default()),
            CacheConfig::default(),
        )
        .unwrap();
    });
}

#[tokio::test]
async fn oversized_admission_is_opt_in_and_never_exceeds_disk_quota() {
    for (normal, oversized, disk, expected) in [(4, 0, 16, 4), (4, 8, 16, 8), (4, 8, 6, 6)] {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Source::default());
        let config = CacheConfig {
            max_bytes: disk,
            max_inflight_bytes: normal,
            max_oversized_inflight_bytes: oversized,
            ..Default::default()
        };
        assert_eq!(config.readable_limit(), expected);
        let cache = LocalCas::open(dir.path(), source.clone(), config).unwrap();
        let result = cache
            .clone()
            .content_from(
                artifact(b"abcdefgh"),
                Arc::new(BytesSource(b"abcdefgh".to_vec())),
            )
            .await;
        if expected < 8 {
            let error = result.err().unwrap();
            let limit = error.downcast_ref::<CacheLimitError>().unwrap();
            assert_eq!(limit.limit, expected);
            assert!(error.to_string().contains("effective readable limit"));
            assert_eq!(cache.metrics().fetches, 0);
        } else {
            let lease = result.unwrap();
            assert_eq!(fs::read(lease.path()).unwrap(), b"abcdefgh");
            assert_eq!(cache.metrics().resident_bytes, 8);
        }
        assert_eq!(cache.metrics().reserved_bytes, 0);
    }
}

#[tokio::test]
async fn oversized_blobs_verify_completely_before_publication_and_obey_pinned_lru() {
    let dir = tempfile::tempdir().unwrap();
    let cache = LocalCas::open(
        dir.path(),
        Arc::new(Source::default()),
        CacheConfig {
            max_bytes: 8,
            max_inflight_bytes: 4,
            max_oversized_inflight_bytes: 8,
            ..Default::default()
        },
    )
    .unwrap();
    for invalid in [b"abcdefg".as_slice(), b"abcdefghX", b"12345678"] {
        assert!(
            cache
                .clone()
                .content_from(
                    artifact(b"abcdefgh"),
                    Arc::new(BytesSource(invalid.to_vec()))
                )
                .await
                .is_err()
        );
        assert_eq!(cache.metrics().resident_bytes, 0);
        assert_eq!(cache.metrics().reserved_bytes, 0);
        assert!(!fs::read_dir(dir.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|ext| ext == "tmp")
        }));
    }
    let lease = bytes(&cache, b"abcdefgh").await;
    let path = lease.path().to_owned();
    assert!(
        cache.clone().content(artifact(b"aaaa")).await.is_err(),
        "oversized lease cannot be reclaimed"
    );
    drop(lease);
    let replacement = bytes(&cache, b"aaaa").await;
    assert!(!path.exists());
    assert_eq!(fs::read(replacement.path()).unwrap(), b"aaaa");
    assert_eq!(cache.metrics().evicted_bytes, 8);
}

#[tokio::test]
async fn extreme_declared_size_cannot_wrap_reserved_disk_accounting() {
    let dir = tempfile::tempdir().unwrap();
    let default_source = Arc::new(Source::default());
    let cache = LocalCas::open(
        dir.path(),
        default_source.clone(),
        CacheConfig {
            max_bytes: u64::MAX,
            max_inflight_bytes: 4,
            max_oversized_inflight_bytes: u64::MAX,
            ..Default::default()
        },
    )
    .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut enormous = artifact(b"aaaa");
    enormous.size = u64::MAX; // Explicit invalid-source/budget boundary fixture.
    let caller = tokio::spawn(cache.clone().content_from(
        enormous,
        Arc::new(HeldSource {
            entered: entered.clone(),
            release: release.clone(),
            dropped: Arc::new(AtomicBool::new(false)),
        }),
    ));
    entered.notified().await;
    assert_eq!(cache.metrics().reserved_bytes, u64::MAX);
    assert!(cache.clone().content(artifact(b"bbbb")).await.is_err());
    assert_eq!(cache.metrics().reserved_bytes, u64::MAX);
    assert_eq!(default_source.calls.load(Ordering::Relaxed), 0);
    release.notify_one();
    assert!(
        caller.await.unwrap().is_err(),
        "truncated oversized source is never published"
    );
    cache.wait_for_idle().await.unwrap();
    assert_eq!(cache.metrics().reserved_bytes, 0);
    assert_eq!(cache.metrics().resident_bytes, 0);
}
