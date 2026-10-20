use super::*;
use std::{io::Write, process::Command, sync::atomic::AtomicBool};

#[derive(Default)]
struct Source {
    calls: AtomicU64,
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
