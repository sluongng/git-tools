use super::*;
use async_trait::async_trait;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tempfile::TempDir;
#[derive(Default)]
struct Source {
    data: Mutex<HashMap<Artifact, Vec<u8>>>,
    calls: AtomicU64,
    corrupt: AtomicBool,
    delay: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    missing_delay: AtomicBool,
    missing_entered: tokio::sync::Notify,
    missing_release: tokio::sync::Notify,
    unrequested_missing: AtomicBool,
}
impl Source {
    fn artifact(&self, bytes: &[u8]) -> Artifact {
        let a = Artifact {
            backend: "test".into(),
            instance: "instance".into(),
            digest_function: 9,
            hash: blake3::hash(bytes).to_hex().to_string(),
            size: bytes.len() as u64,
        };
        self.data.lock().unwrap().insert(a.clone(), bytes.to_vec());
        a
    }
}
#[async_trait]
impl BlobSource for Source {
    async fn fetch(&self, a: &Artifact, target: &mut File) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.delay.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let mut bytes = self
            .data
            .lock()
            .unwrap()
            .get(a)
            .context("missing CAS object")?
            .clone();
        if self.corrupt.load(Ordering::SeqCst) {
            bytes.push(0);
        }
        target.write_all(&bytes)?;
        Ok(())
    }
    async fn missing(&self, artifacts: &[Artifact]) -> Result<Vec<Artifact>> {
        if self.missing_delay.swap(false, Ordering::SeqCst) {
            self.missing_entered.notify_one();
            self.missing_release.notified().await;
        }
        if self.unrequested_missing.load(Ordering::SeqCst) {
            return Ok(vec![self.artifact(b"unrequested")]);
        }
        let data = self.data.lock().unwrap();
        Ok(artifacts
            .iter()
            .filter(|a| !data.contains_key(*a))
            .cloned()
            .collect())
    }
}
fn setup(config: CacheConfig) -> (TempDir, Arc<Source>, Arc<Tree>) {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Source::default());
    let tree = Tree::open(dir.path(), "test", source.clone(), config).unwrap();
    (dir, source, tree)
}
#[tokio::test]
async fn metadata_only_stage_then_verified_coalesced_first_reads() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let a = source.artifact(b"remote bytes");
    let n = tree.stage("nested/output", a.clone()).await.unwrap();
    assert_eq!(
        tree.stat("nested/output", 9).await.unwrap(),
        Some(TreeStat::File { artifact: Some(a) })
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    let futures = (0..16).map(|_| {
        let tree = tree.clone();
        tokio::spawn(async move { tree.read(n.id, 0, 99).await.unwrap() })
    });
    for future in futures {
        assert_eq!(future.await.unwrap(), b"remote bytes");
    }
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert_eq!(tree.cache_metrics().verified_bytes, 12);
}
#[tokio::test]
async fn delayed_old_generation_never_overwrites_replacement_or_blocks_metadata() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let a = source.artifact(b"old");
    let b = source.artifact(b"new");
    let old = tree.stage("out", a).await.unwrap();
    source.delay.store(true, Ordering::SeqCst);
    let task = {
        let tree = tree.clone();
        tokio::spawn(async move { tree.read(old.id, 0, 3).await.unwrap() })
    };
    source.entered.notified().await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), tree.stat("out", 9))
            .await
            .unwrap()
            .unwrap()
            .is_some()
    );
    let replacement = tree.stage("out", b.clone()).await.unwrap();
    assert_ne!(old.id, replacement.id);
    assert_eq!(
        tree.stat("out", 9).await.unwrap(),
        Some(TreeStat::File { artifact: Some(b) })
    );
    source.release.notify_one();
    assert_eq!(task.await.unwrap(), b"old");
    assert_eq!(tree.read(replacement.id, 0, 3).await.unwrap(), b"new");
    assert!(!tree.get(old.id).unwrap().linked);
}
#[tokio::test]
async fn cancellation_of_first_waiter_does_not_cancel_shared_fetch() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let a = source.artifact(b"survives");
    let n = tree.stage("out", a).await.unwrap();
    source.delay.store(true, Ordering::SeqCst);
    let task = {
        let tree = tree.clone();
        tokio::spawn(async move { tree.read(n.id, 0, 99).await })
    };
    source.entered.notified().await;
    task.abort();
    let second = {
        let tree = tree.clone();
        tokio::spawn(async move { tree.read(n.id, 0, 99).await.unwrap() })
    };
    source.release.notify_one();
    assert_eq!(second.await.unwrap(), b"survives");
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn waiter_deadline_does_not_cancel_independent_shared_fetch() {
    let (_dir, source, tree) = setup(CacheConfig {
        operation_timeout: Duration::from_millis(1000),
        fetch_timeout: Duration::from_secs(5),
        ..Default::default()
    });
    let node = tree
        .stage("out", source.artifact(b"survives timeout"))
        .await
        .unwrap();
    source.delay.store(true, Ordering::SeqCst);
    let first = {
        let tree = tree.clone();
        tokio::spawn(async move { tree.read(node.id, 0, 99).await })
    };
    source.entered.notified().await;
    let error = first.await.unwrap().unwrap_err();
    assert!(error.downcast_ref::<CacheTimeoutError>().is_some());
    assert_eq!(tree.cache_metrics().timed_out_waiters, 1);
    // The first caller's deadline has already expired. A new waiter can still
    // join that fetch, which has its own lifetime and retains its permits.
    source.release.notify_one();
    assert_eq!(
        tree.read(node.id, 0, 99).await.unwrap(),
        b"survives timeout"
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn fetch_slot_wait_is_inside_foreground_deadline() {
    let (_dir, source, tree) = setup(CacheConfig {
        max_concurrent_fetches: 1,
        operation_timeout: Duration::from_millis(1000),
        fetch_timeout: Duration::from_secs(5),
        ..Default::default()
    });
    let first_artifact = source.artifact(b"blocked");
    let second_artifact = source.artifact(b"queued");
    source.delay.store(true, Ordering::SeqCst);
    let first = {
        let tree = tree.clone();
        let artifact = first_artifact.clone();
        tokio::spawn(async move { tree.content(&artifact).await })
    };
    source.entered.notified().await;
    let error = tree.content(&second_artifact).await.err().unwrap();
    assert!(error.downcast_ref::<CacheTimeoutError>().is_some());
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert!(first.await.unwrap().is_err());
    source.release.notify_one();
    let first_bytes = tree.content(&first_artifact).await.unwrap();
    assert_eq!(fs::read(first_bytes.path()).unwrap(), b"blocked");
    let second_bytes = tree.content(&second_artifact).await.unwrap();
    assert_eq!(fs::read(second_bytes.path()).unwrap(), b"queued");
    assert!(tree.cache_metrics().admission_wait_ns > 0);
}
#[tokio::test]
async fn shared_fetch_expiry_releases_temporary_bytes_and_allows_retry() {
    let (dir, source, tree) = setup(CacheConfig {
        operation_timeout: Duration::from_secs(5),
        fetch_timeout: Duration::from_millis(1000),
        ..Default::default()
    });
    let artifact = source.artifact(b"bounded fetch");
    source.delay.store(true, Ordering::SeqCst);
    let error = tree.content(&artifact).await.err().unwrap();
    assert!(error.downcast_ref::<CacheTimeoutError>().is_some());
    assert_eq!(tree.cache_metrics().timed_out_fetches, 1);
    assert_eq!(fs::read_dir(dir.path().join("cache")).unwrap().count(), 1);
    let bytes = tree.content(&artifact).await.unwrap();
    assert_eq!(fs::read(bytes.path()).unwrap(), b"bounded fetch");
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn byte_admission_is_inside_foreground_deadline() {
    let (_dir, source, tree) = setup(CacheConfig {
        max_concurrent_fetches: 2,
        max_inflight_bytes: 4,
        max_oversized_inflight_bytes: 4096,
        operation_timeout: Duration::from_millis(1000),
        fetch_timeout: Duration::from_secs(5),
        ..Default::default()
    });
    let first_artifact = source.artifact(b"blocked");
    let second_artifact = source.artifact(b"queued");
    source.delay.store(true, Ordering::SeqCst);
    let first = {
        let tree = tree.clone();
        let artifact = first_artifact.clone();
        tokio::spawn(async move { tree.content(&artifact).await })
    };
    source.entered.notified().await;
    // Two fetch slots are available, but each small object occupies the only
    // 4KiB transfer unit. The second operation times out before source.fetch.
    let error = tree.content(&second_artifact).await.err().unwrap();
    assert!(error.downcast_ref::<CacheTimeoutError>().is_some());
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert!(first.await.unwrap().is_err());
    source.release.notify_one();
    let bytes = tree.content(&second_artifact).await.unwrap();
    assert_eq!(fs::read(bytes.path()).unwrap(), b"queued");
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn corrupt_payload_is_not_published_and_retry_is_possible() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let a = source.artifact(b"valid");
    let n = tree.stage("out", a).await.unwrap();
    source.corrupt.store(true, Ordering::SeqCst);
    assert!(tree.read(n.id, 0, 99).await.is_err());
    assert_eq!(fs::read_dir(dir.path().join("cache")).unwrap().count(), 1);
    source.corrupt.store(false, Ordering::SeqCst);
    assert_eq!(tree.read(n.id, 0, 99).await.unwrap(), b"valid");
}
#[tokio::test]
async fn writes_invalidate_digest_and_survive_restart_and_missing_prune() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let a = source.artifact(b"original");
    let n = tree.stage("dirty", a.clone()).await.unwrap();
    tree.write(n.id, 0, b"edited").await.unwrap();
    tree.setattr(
        n.id,
        SetAttr {
            size: Some(6),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let expected = source.artifact(b"edited");
    assert_eq!(
        tree.stat("dirty", 9).await.unwrap(),
        Some(TreeStat::File {
            artifact: Some(Artifact {
                instance: String::new(),
                ..expected
            })
        })
    );
    tree.stage("missing", a.clone()).await.unwrap();
    source.data.lock().unwrap().remove(&a);
    let before = n.id;
    drop(tree);
    let tree = Tree::open(dir.path(), "test", source, CacheConfig::default()).unwrap();
    assert_eq!(tree.resolve("dirty").unwrap().id, before);
    assert_eq!(tree.read(before, 0, 99).await.unwrap(), b"edited");
    assert_eq!(tree.refresh("").await.unwrap(), vec!["missing"]);
    assert!(tree.resolve("missing").is_err());
    assert!(tree.resolve("dirty").is_ok());
    tree.clean("").await.unwrap();
    assert!(tree.list(1, 0, 99).unwrap().is_empty());
    assert_eq!(tree.read(before, 0, 99).await.unwrap(), b"edited");
}
#[tokio::test]
async fn prepared_publication_conflicts_abort_and_restart_replay() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let a = source.artifact(b"first");
    let b = source.artifact(b"second");
    let pending = tree.prepare_stage("out", a.clone()).await.unwrap();
    assert!(tree.resolve("out").is_err());
    assert!(tree.prepared_stage_valid(&pending).unwrap());
    assert_eq!(tree.read(pending.node.id, 0, 99).await.unwrap(), b"first");
    let serialized = serde_json::to_vec(&pending).unwrap();
    drop(tree);
    let tree = Tree::open(dir.path(), "test", source.clone(), CacheConfig::default()).unwrap();
    let pending: PreparedStage = serde_json::from_slice(&serialized).unwrap();
    assert!(tree.prepared_stage_valid(&pending).unwrap());
    tree.commit_stage(pending.clone()).await.unwrap();
    assert!(tree.prepared_stage_valid(&pending).unwrap());
    tree.commit_stage(pending.clone()).await.unwrap();
    let mut forged = pending.clone();
    forged.node.remote.as_mut().unwrap().size += 1;
    assert!(!tree.prepared_stage_valid(&forged).unwrap());
    let race = tree.prepare_stage("out", a).await.unwrap();
    tree.stage("out", b.clone()).await.unwrap();
    assert!(!tree.prepared_stage_valid(&pending).unwrap());
    assert!(!tree.prepared_stage_valid(&race).unwrap());
    assert!(tree.commit_stage(race.clone()).await.is_err());
    tree.abort_stage(race.clone()).await.unwrap();
    assert!(!tree.prepared_stage_valid(&race).unwrap());
    let aborted = tree.prepare_stage("new", b.clone()).await.unwrap();
    tree.abort_stage(aborted.clone()).await.unwrap();
    assert!(!tree.prepared_stage_valid(&aborted).unwrap());
    let clean_pending = tree.prepare_stage("unpublished", b).await.unwrap();
    tree.clean("unpublished").await.unwrap();
    assert!(!tree.prepared_stage_valid(&clean_pending).unwrap());
    tree.clean("out").await.unwrap();
    assert!(!tree.prepared_stage_valid(&pending).unwrap());
    drop(tree);
    let tree = Tree::open(dir.path(), "test", source, CacheConfig::default()).unwrap();
    assert!(!tree.prepared_stage_valid(&aborted).unwrap());
    assert!(!tree.prepared_stage_valid(&clean_pending).unwrap());
    assert!(!tree.prepared_stage_valid(&pending).unwrap());
}
#[tokio::test]
async fn cache_budget_respects_leases_without_reclaiming_dirty_files() {
    let (dir, source, tree) = setup(CacheConfig {
        max_bytes: 5,
        ..Default::default()
    });
    let oversized = source.artifact(b"too large");
    let oversized_node = tree.stage("oversized", oversized.clone()).await.unwrap();
    assert_eq!(
        tree.stat("oversized", 9).await.unwrap(),
        Some(TreeStat::File {
            artifact: Some(oversized)
        })
    );
    assert!(
        tree.read(oversized_node.id, 0, 9)
            .await
            .unwrap_err()
            .downcast_ref::<CacheLimitError>()
            .is_some()
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    tree.setattr(
        oversized_node.id,
        SetAttr {
            size: Some(0),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(tree.read(oversized_node.id, 0, 9).await.unwrap().is_empty());
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    let a = source.artifact(b"first");
    let b = source.artifact(b"other");
    let local = tree.create(1, "local", 0o600).await.unwrap();
    tree.write(local.id, 0, b"authoritative local bytes")
        .await
        .unwrap();
    let lease = tree.content(&a).await.unwrap();
    assert!(tree.content(&b).await.is_err());
    assert!(lease.path().exists());
    drop(lease);
    let second = tree.content(&b).await.unwrap();
    assert_eq!(fs::read(second.path()).unwrap(), b"other");
    assert_eq!(fs::read_dir(dir.path().join("cache")).unwrap().count(), 2);
    assert_eq!(
        tree.read(local.id, 0, 99).await.unwrap(),
        b"authoritative local bytes"
    );
}
#[tokio::test]
async fn directory_cursor_survives_deletion_and_mutations_enforce_types() {
    let (_dir, _source, tree) = setup(CacheConfig::default());
    let first = tree.create(1, "a", 0o600).await.unwrap();
    let second = tree.create(1, "b", 0o600).await.unwrap();
    tree.unlink_file(1, "a").await.unwrap();
    assert_eq!(tree.list(1, first.id, 1).unwrap()[0].id, second.id);
    assert!(
        tree.create(1, "b", 0o600)
            .await
            .unwrap_err()
            .downcast_ref::<CoreError>()
            == Some(&CoreError::AlreadyExists)
    );
    let d = tree.mkdir(1, "dir", 0o755).await.unwrap();
    assert_eq!(
        tree.unlink_file(1, "dir")
            .await
            .unwrap_err()
            .downcast_ref::<CoreError>(),
        Some(&CoreError::IsDirectory)
    );
    tree.create(d.id, "child", 0o644).await.unwrap();
    assert_eq!(
        tree.unlink_directory(1, "dir")
            .await
            .unwrap_err()
            .downcast_ref::<CoreError>(),
        Some(&CoreError::NotEmpty)
    );
    assert!(tree.ensure_dir("../escape").await.is_err());
    tree.rename(1, "b", 1, "dir").await.unwrap_err();
    let target = tree.create(1, "replace-target", 0o600).await.unwrap();
    tree.rename(1, "b", 1, "replace-target").await.unwrap();
    assert_eq!(tree.resolve("replace-target").unwrap().id, second.id);
    assert!(!tree.get(target.id).unwrap().linked);
    let old = tree.get(second.id).unwrap();
    assert_eq!(
        tree.setattr_guarded(
            second.id,
            SetAttr {
                mode: Some(0o777),
                ..Default::default()
            },
            Some(old.ctime + 1)
        )
        .await
        .unwrap_err()
        .downcast_ref::<CoreError>(),
        Some(&CoreError::NotSync)
    );
    assert_eq!(tree.get(second.id).unwrap().mode, 0o600);
}
#[tokio::test]
async fn exclusive_create_replays_only_matching_persisted_verifier() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let n = tree
        .create_exclusive(1, "exclusive", 0o600, [7; 8])
        .await
        .unwrap();
    drop(tree);
    let tree = Tree::open(dir.path(), "test", source, CacheConfig::default()).unwrap();
    assert_eq!(
        tree.create_exclusive(1, "exclusive", 0o600, [7; 8])
            .await
            .unwrap()
            .id,
        n.id
    );
    assert!(
        tree.create_exclusive(1, "exclusive", 0o600, [8; 8])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn verified_range_reads_reuse_integrity_work_and_detect_changed_cache_bytes() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let bytes = vec![7u8; 1024 * 1024];
    let a = source.artifact(&bytes);
    let n = tree.stage("large", a.clone()).await.unwrap();
    for offset in (0..bytes.len()).step_by(4096) {
        assert_eq!(
            tree.read(n.id, offset as u64, 4096).await.unwrap(),
            vec![7u8; 4096]
        );
    }
    assert_eq!(tree.cache_metrics().verification_bytes, bytes.len() as u64);
    let path = {
        let lease = tree.content(&a).await.unwrap();
        lease.path().to_owned()
    };
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.write_all(b"corrupt").unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert_eq!(tree.read(n.id, 0, 7).await.unwrap(), vec![7u8; 7]);
    assert_eq!(tree.cache_metrics().fetches, 2);
}
#[tokio::test]
async fn truncate_zero_needs_no_remote_content_and_tree_identity_survives_restart() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let identity = tree.identity().to_owned();
    let a = source.artifact(b"never fetched");
    let n = tree.stage("out", a).await.unwrap();
    tree.setattr(
        n.id,
        SetAttr {
            size: Some(0),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tree.read(n.id, 0, 99).await.unwrap(), b"");
    // Simulate a data fsync before the subsequent metadata transaction.
    fs::write(tree.local_path(n.id), b"recovered bytes").unwrap();
    drop(tree);
    let tree = Tree::open(dir.path(), "test", source, CacheConfig::default()).unwrap();
    assert_eq!(tree.identity(), identity);
    assert_eq!(tree.get(n.id).unwrap().size, 15);
}

#[tokio::test]
async fn blocked_refresh_does_not_block_other_base_and_revalidates_mutations() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let missing = source.artifact(b"expired");
    let replacement = tree.stage("a/replaced", missing.clone()).await.unwrap();
    let dirty = tree.stage("a/dirty", missing.clone()).await.unwrap();
    let renamed = tree.stage("a/sub/file", missing.clone()).await.unwrap();
    tree.stage("a/unchanged", missing.clone()).await.unwrap();
    source.data.lock().unwrap().remove(&missing);
    source.missing_delay.store(true, Ordering::SeqCst);
    let pending = {
        let tree = tree.clone();
        tokio::spawn(async move { tree.prepare_refresh("a").await.unwrap() })
    };
    source.missing_entered.notified().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        let other = source.artifact(b"other base");
        tree.stage("b/out", other.clone()).await.unwrap();
        assert_eq!(
            tree.stat("b/out", 9).await.unwrap(),
            Some(TreeStat::File {
                artifact: Some(other)
            })
        );
        let new = tree.stage("a/replaced", missing.clone()).await.unwrap();
        assert_ne!(new.id, replacement.id);
        tree.setattr(
            dirty.id,
            SetAttr {
                size: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let parent = tree.resolve("a").unwrap().id;
        tree.rename(parent, "sub", parent, "renamed").await.unwrap();
    })
    .await
    .unwrap();
    source.missing_release.notify_one();
    let plan = pending.await.unwrap();
    assert_eq!(tree.apply_refresh(plan).await.unwrap(), vec!["a/unchanged"]);
    assert!(tree.resolve("a/replaced").is_ok());
    assert_eq!(tree.resolve("a/renamed/file").unwrap().id, renamed.id);
    assert!(tree.get(dirty.id).unwrap().remote.is_none());
    assert_eq!(tree.read(dirty.id, 0, 99).await.unwrap(), b"");
}

#[tokio::test]
async fn refresh_rejects_unrequested_presence_results_without_mutating_namespace() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let artifact = source.artifact(b"retained");
    let node = tree.stage("out", artifact).await.unwrap();
    source.unrequested_missing.store(true, Ordering::SeqCst);
    assert!(tree.prepare_refresh("").await.is_err());
    assert_eq!(tree.resolve("out").unwrap(), node);
}

#[tokio::test]
async fn same_inode_readers_share_one_foreground_admission_budget() {
    let (_dir, source, tree) = setup(CacheConfig {
        operation_timeout: Duration::from_millis(100),
        fetch_timeout: Duration::from_secs(5),
        ..Default::default()
    });
    let node = tree
        .stage("out", source.artifact(b"delayed"))
        .await
        .unwrap();
    source.delay.store(true, Ordering::SeqCst);
    let readers: Vec<_> = (0..16)
        .map(|_| {
            let tree = tree.clone();
            tokio::spawn(async move { tree.read(node.id, 0, 99).await })
        })
        .collect();
    source.entered.notified().await;
    tokio::time::timeout(Duration::from_secs(1), async {
        for reader in readers {
            let error = reader.await.unwrap().unwrap_err();
            assert!(error.downcast_ref::<CacheTimeoutError>().is_some());
        }
    })
    .await
    .expect("inode admission must not multiply the foreground budget");
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    source.release.notify_one();
}

#[tokio::test]
async fn independent_namespaces_share_content_cache_without_sharing_writable_authority() {
    let root = tempfile::tempdir().unwrap();
    let source = Arc::new(Source::default());
    let default_source = Arc::new(Source::default());
    let cache = LocalCas::open(
        root.path().join("shared-cas"),
        default_source.clone(),
        CacheConfig::default(),
    )
    .unwrap();
    let first = Tree::open_with_cache_and_source(
        root.path().join("first"),
        "test",
        cache.clone(),
        source.clone(),
    )
    .unwrap();
    let second =
        Tree::open_with_cache_and_source(root.path().join("second"), "test", cache, source.clone())
            .unwrap();
    let artifact = source.artifact(b"shared clean output");
    let first_node = first.stage("out", artifact.clone()).await.unwrap();
    let second_node = second.stage("different/path", artifact).await.unwrap();
    assert_eq!(
        first.read(first_node.id, 0, 99).await.unwrap(),
        b"shared clean output"
    );
    assert_eq!(
        second.read(second_node.id, 0, 99).await.unwrap(),
        b"shared clean output"
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        default_source.calls.load(Ordering::SeqCst),
        0,
        "namespace hydration must use its authorized source, not the cache default"
    );
    first.write(first_node.id, 0, b"edited").await.unwrap();
    first.clean("").await.unwrap();
    assert_eq!(
        second.read(second_node.id, 0, 99).await.unwrap(),
        b"shared clean output"
    );
    assert!(second.resolve("different/path").is_ok());
}

#[tokio::test]
async fn delayed_oversized_read_does_not_occupy_normal_byte_admission() {
    let (_dir, source, tree) = setup(CacheConfig {
        max_bytes: 12,
        max_inflight_bytes: 4,
        max_oversized_inflight_bytes: 8,
        max_concurrent_fetches: 2,
        ..Default::default()
    });
    let large = tree
        .stage("large", source.artifact(b"abcdefgh"))
        .await
        .unwrap();
    let small = tree.stage("small", source.artifact(b"tiny")).await.unwrap();
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        0,
        "staging remains metadata-only"
    );
    source.delay.store(true, Ordering::SeqCst);
    let reader = {
        let tree = tree.clone();
        tokio::spawn(async move { tree.read(large.id, 0, 8).await })
    };
    source.entered.notified().await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), tree.read(small.id, 0, 4))
            .await
            .unwrap()
            .unwrap(),
        b"tiny"
    );
    assert!(!reader.is_finished(), "large transfer remains stalled");
    source.release.notify_one();
    assert_eq!(reader.await.unwrap().unwrap(), b"abcdefgh");
    assert_eq!(tree.read(large.id, 2, 3).await.unwrap(), b"cde");
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(tree.cache_metrics().resident_bytes, 12);
}

#[tokio::test]
#[ignore = "explicit iteration/disk benchmark"]
async fn retired_output_iteration_measurement() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let started = std::time::Instant::now();
    let payload = vec![0x5a; 32 * 1024];
    let mut ids = Vec::new();
    let collect = std::env::var_os("OUTPUT_RETIREMENT_GC").is_some();
    let placeholders = std::env::var("OUTPUT_PLACEHOLDER_LOCATORS").ok();
    let remote = placeholders.as_ref().map(|mode| {
        match mode.as_str() {
            "legacy" => tree.retain_placeholder_generations().unwrap(),
            "immutable" => {
                tree.enable_immutable_placeholder_locators(0).unwrap();
            }
            _ => panic!("unknown explicit placeholder benchmark mode"),
        }
        source.artifact(&payload)
    });
    let mut gc_us = 0;
    for _ in 0..128 {
        let n = if let Some(artifact) = &remote {
            tree.stage("output", artifact.clone()).await.unwrap()
        } else {
            let n = tree.create(1, "output", 0o644).await.unwrap();
            tree.write(n.id, 0, &payload).await.unwrap();
            n
        };
        tree.unlink(1, "output").await.unwrap();
        ids.push(n.id);
        if collect {
            let gc = std::time::Instant::now();
            tree.reclaim_retired(64).unwrap();
            gc_us += gc.elapsed().as_micros();
        }
    }
    let local_bytes: u64 = fs::read_dir(dir.path().join("local"))
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum();
    let retained = ids.iter().filter(|id| tree.get(**id).is_ok()).count();
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        0,
        "metadata iterations never fetch content"
    );
    fn disk(path: &Path) -> u64 {
        fs::read_dir(path)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                let m = e.metadata().unwrap();
                if m.is_dir() { disk(&e.path()) } else { m.len() }
            })
            .sum()
    }
    eprintln!(
        "OUTPUT_ITERATIONS count=128 payload_bytes=32768 wall_us={} local_bytes={} state_bytes={} retained_nodes={} gc_us={}",
        started.elapsed().as_micros(),
        local_bytes,
        disk(dir.path()),
        retained,
        gc_us
    );
}

#[tokio::test]
async fn collection_preserves_leases_lookup_counts_and_authoritative_edits() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let old = tree
        .stage("output", source.artifact(b"remote"))
        .await
        .unwrap();
    let lease = tree.pin(old.id).unwrap();
    let looked_up = tree.lookup_referenced(1, "output").unwrap();
    assert_eq!(looked_up.id, old.id);
    tree.write(old.id, 0, b"edited").await.unwrap();
    // No remaining remote copy: neither retention nor GC may assume an upload
    // succeeded merely because the old entry once had a CAS digest.
    source.data.lock().unwrap().clear();
    let replacement = tree.create(1, "replacement", 0o644).await.unwrap();
    tree.write(replacement.id, 0, b"unpublished edit")
        .await
        .unwrap();
    tree.rename(1, "replacement", 1, "output").await.unwrap();
    assert_eq!(tree.reclaim_retired(64).unwrap().reclaimed_nodes, 0);
    assert_eq!(tree.read(old.id, 0, 99).await.unwrap(), b"edited");
    drop(lease);
    assert_eq!(tree.reclaim_retired(64).unwrap().reclaimed_nodes, 0);
    tree.forget(old.id, 1);
    assert_eq!(tree.reclaim_retired(64).unwrap().reclaimed_local_bytes, 6);
    assert!(tree.get(old.id).is_err());
    assert!(!dir.path().join("local").join(old.id.to_string()).exists());
    assert_eq!(
        tree.read(replacement.id, 0, 99).await.unwrap(),
        b"unpublished edit"
    );
    assert_eq!(tree.resolve("output").unwrap().id, replacement.id);
    let newest = tree.stage("output", source.artifact(b"new")).await.unwrap();
    assert!(
        newest.id > replacement.id,
        "retired IDs must never alias replacements"
    );
}

#[tokio::test]
async fn collection_skips_inflight_io_pending_publication_and_retained_parents() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let old = tree
        .stage("parent/output", source.artifact(b"old"))
        .await
        .unwrap();
    source.delay.store(true, Ordering::SeqCst);
    let reader = {
        let tree = tree.clone();
        tokio::spawn(async move { tree.read(old.id, 0, 3).await.unwrap() })
    };
    source.entered.notified().await;
    let prepared = tree
        .prepare_stage("parent/output", source.artifact(b"new"))
        .await
        .unwrap();
    assert_eq!(tree.reclaim_retired(64).unwrap().reclaimed_nodes, 0);
    tree.commit_stage(prepared).await.unwrap();
    tree.clean("parent").await.unwrap();
    tree.reclaim_retired(64).unwrap();
    assert!(tree.get(old.id).is_ok(), "in-flight reader owns old bytes");
    source.release.notify_one();
    assert_eq!(reader.await.unwrap(), b"old");
    tree.reclaim_retired_at_boundary(2).unwrap();
    assert_eq!(tree.retirement_metrics().retained_nodes, 0);
    // A prefix larger than a collection batch consists entirely of parents.
    // Stopping after that batch makes every later restart miss the leaf too.
    let deep = (0..65).map(|_| "nested").collect::<Vec<_>>().join("/");
    tree.ensure_dir(&deep).await.unwrap();
    tree.stage(&format!("{deep}/leaf"), source.artifact(b"leaf"))
        .await
        .unwrap();
    tree.clean("nested").await.unwrap();
    let collected = tree.reclaim_retired_at_boundary(2).unwrap();
    assert_eq!(collected.reclaimed_nodes, 66);
    assert!(
        collected.visited <= 132,
        "a natural directory chain must not require quadratic candidate visits"
    );
    assert_eq!(tree.retirement_metrics().retained_nodes, 0);
    // Renames can make a parent newer than its child. Traversal preference is
    // not a dependency guarantee; complete rounds must still reclaim both.
    let old_parent = tree.ensure_dir("old/child").await.unwrap().parent;
    let newer_parent = tree.ensure_dir("new").await.unwrap();
    tree.rename(old_parent, "child", newer_parent.id, "child")
        .await
        .unwrap();
    tree.clean("old").await.unwrap();
    tree.clean("new").await.unwrap();
    assert_eq!(
        tree.reclaim_retired_at_boundary(2).unwrap().reclaimed_nodes,
        3
    );
    assert_eq!(tree.retirement_metrics().retained_nodes, 0);
}

#[tokio::test]
async fn failed_payload_deletion_recovers_from_owned_ledger_without_guessing() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let n = tree.create(1, "output", 0o644).await.unwrap();
    tree.write(n.id, 0, b"retired").await.unwrap();
    tree.unlink(1, "output").await.unwrap();
    let payload = dir.path().join("local").join(n.id.to_string());
    fs::remove_file(&payload).unwrap();
    fs::create_dir(&payload).unwrap();
    assert!(tree.reclaim_retired(64).is_err());
    assert!(
        tree.get(n.id).is_err(),
        "metadata retirement precedes unlink"
    );
    assert_eq!(tree.retirement_metrics().pending_payload_deletions, 1);
    fs::remove_dir(&payload).unwrap();
    fs::write(&payload, b"known retired payload").unwrap();
    tree.reclaim_retired_at_boundary(1).unwrap();
    assert!(
        !payload.exists(),
        "an empty retired index still retries its ledger"
    );
    assert_eq!(tree.retirement_metrics().pending_payload_deletions, 0);
    // Repeat the distinct recovery path with the persisted intent left behind.
    let n = tree.create(1, "recovery", 0o644).await.unwrap();
    tree.write(n.id, 0, b"retired").await.unwrap();
    tree.unlink(1, "recovery").await.unwrap();
    let payload = dir.path().join("local").join(n.id.to_string());
    fs::remove_file(&payload).unwrap();
    fs::create_dir(&payload).unwrap();
    assert!(tree.reclaim_retired(64).is_err());
    drop(tree);
    fs::remove_dir(&payload).unwrap();
    fs::write(&payload, b"known retired payload").unwrap();
    let unknown = dir.path().join("local/9999");
    fs::write(&unknown, b"unknown ownership").unwrap();
    let tree = Tree::open(dir.path(), "test", source, CacheConfig::default()).unwrap();
    assert!(!payload.exists(), "durable retirement is replayed");
    assert_eq!(fs::read(&unknown).unwrap(), b"unknown ownership");
    assert_eq!(tree.retirement_metrics().pending_payload_deletions, 0);
    let newest = tree.create(1, "new", 0o644).await.unwrap();
    assert!(newest.id > 9999);
    tree.unlink(1, "new").await.unwrap();
    assert_eq!(tree.reclaim_retired(1).unwrap().reclaimed_nodes, 1);
    drop(tree);
    let source = Arc::new(Source::default());
    let tree = Tree::open(dir.path(), "test", source, CacheConfig::default()).unwrap();
    assert!(
        tree.create(1, "after-reopen", 0o644).await.unwrap().id > newest.id,
        "the durable highwater, not extant payload files, prevents identity reuse"
    );
}

#[tokio::test]
async fn server_generation_and_persisted_placeholder_fences_are_not_request_counts() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let node = tree.create(1, "output", 0o644).await.unwrap();
    tree.write(node.id, 0, b"old handle").await.unwrap();
    let generation = tree.pin_generation();
    tree.unlink(1, "output").await.unwrap();
    assert_eq!(tree.reclaim_retired(64).unwrap().reclaimed_nodes, 0);
    assert_eq!(tree.read(node.id, 0, 99).await.unwrap(), b"old handle");
    drop(generation);
    tree.retain_placeholder_generations().unwrap();
    drop(tree);
    {
        // Compatibility fixture: the prior schema had only an all-generations
        // boolean. Its omission of a finite boundary must remain conservative.
        let db = SingleWriterTxDatabase::builder(dir.path().join("namespace.fjall"))
            .open()
            .unwrap();
        let keyspace = db
            .keyspace("namespace", KeyspaceCreateOptions::default)
            .unwrap();
        let mut header: serde_json::Value =
            serde_json::from_slice(&keyspace.get(b"header").unwrap().unwrap()).unwrap();
        header["version"] = 2.into();
        header
            .as_object_mut()
            .unwrap()
            .remove("placeholder_legacy_ceiling");
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&keyspace, b"header", serde_json::to_vec(&header).unwrap());
        tx.commit().unwrap();
    }
    let tree = Tree::open(dir.path(), "test", source.clone(), CacheConfig::default()).unwrap();
    assert_eq!(tree.reclaim_retired(64).unwrap().reclaimed_nodes, 0);
    assert_eq!(tree.read(node.id, 0, 99).await.unwrap(), b"old handle");

    // A synced provider manifest enables self-contained locators only after it
    // captures the old domain. All→finite conversion covers current highwater,
    // even when the supplied manifest ceiling is older.
    assert_eq!(
        tree.enable_immutable_placeholder_locators(0).unwrap(),
        node.id
    );
    let old_artifact = source.artifact(b"immutable old remote");
    let remote = tree.stage("remote", old_artifact.clone()).await.unwrap();
    tree.stage("remote", source.artifact(b"new remote"))
        .await
        .unwrap();
    assert_eq!(
        tree.reclaim_retired_at_boundary(2).unwrap().reclaimed_nodes,
        1
    );
    assert!(tree.get(remote.id).is_err());
    let blob = tree.content(&old_artifact).await.unwrap();
    assert_eq!(fs::read(blob.path()).unwrap(), b"immutable old remote");
    assert_eq!(tree.read(node.id, 0, 99).await.unwrap(), b"old handle");
    // Explicit non-self-contained publication raises protection before native
    // exposure. Lower subsequent ceilings cannot revoke that ownership.
    let local = tree.create(1, "legacy-local", 0o644).await.unwrap();
    tree.write(local.id, 0, b"native legacy").await.unwrap();
    tree.retain_placeholder_inode(local.id).unwrap();
    tree.unlink(1, "legacy-local").await.unwrap();
    assert_eq!(
        tree.enable_immutable_placeholder_locators(0).unwrap(),
        local.id
    );
    drop(blob);
    drop(tree);
    let tree = Tree::open(dir.path(), "test", source, CacheConfig::default()).unwrap();
    assert_eq!(
        tree.reclaim_retired_at_boundary(1).unwrap().reclaimed_nodes,
        0
    );
    assert_eq!(tree.read(local.id, 0, 99).await.unwrap(), b"native legacy");
    assert_eq!(tree.read(node.id, 0, 99).await.unwrap(), b"old handle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_snapshot_wait_drains_a_gated_write_and_retains_stable_dirty_bytes() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let limits = SourceLimits::default();
    tree.install_source(
        SourceManifest {
            identity: tree.identity().into(),
            root: [1; 32],
            entries: vec![SourceEntry {
                path: "file".into(),
                mode: 0o644,
                content: SourceContent::File(source.artifact(b"base")),
            }],
        },
        None,
        "initial",
        &limits,
    )
    .unwrap();
    let id = tree.resolve("file").unwrap().id;
    source.delay.store(true, Ordering::SeqCst);
    let writer_tree = tree.clone();
    let writer = tokio::spawn(async move { writer_tree.write(id, 0, b"edit").await });
    source.entered.notified().await;
    let error = tree
        .freeze_source(SourceFrontendFence::TreeOnly, &limits)
        .err()
        .unwrap();
    assert_eq!(
        error.downcast_ref::<SourceFreezeBusy>(),
        Some(&SourceFreezeBusy::ActiveOperations)
    );
    let capture = tree.freeze_source_wait(
        SourceFrontendFence::TreeOnly,
        &limits,
        tokio::time::Instant::now() + Duration::from_secs(5),
    );
    tokio::pin!(capture);
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(capture.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    source.release.notify_one();
    assert_eq!(writer.await.unwrap().unwrap(), 4);
    let snapshot = capture.await.unwrap();
    assert_eq!(snapshot.read_local(id, 0, 4).unwrap(), b"edit");
    assert!(tree.write(id, 0, b"race").await.is_err());
    assert_eq!(snapshot.read_local(id, 0, 4).unwrap(), b"edit");
    // An existing snapshot is a distinct typed contention condition. Its Drop
    // must wake a registered waiter just as a physical job's Drop does.
    let error = tree
        .freeze_source(SourceFrontendFence::TreeOnly, &limits)
        .err()
        .unwrap();
    assert_eq!(
        error.downcast_ref::<SourceFreezeBusy>(),
        Some(&SourceFreezeBusy::Frozen)
    );
    let capture = tree.freeze_source_wait(
        SourceFrontendFence::TreeOnly,
        &limits,
        tokio::time::Instant::now() + Duration::from_secs(5),
    );
    tokio::pin!(capture);
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(capture.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(snapshot);
    let next = capture.await.unwrap();
    assert_eq!(next.read_local(id, 0, 4).unwrap(), b"edit");
    drop(next);
    assert_eq!(tree.write(id, 0, b"next").await.unwrap(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_snapshot_wait_expiry_does_not_cancel_physical_work_or_leak_a_fence() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let limits = SourceLimits::default();
    tree.install_source(
        SourceManifest {
            identity: tree.identity().into(),
            root: [1; 32],
            entries: vec![SourceEntry {
                path: "file".into(),
                mode: 0o644,
                content: SourceContent::File(source.artifact(b"base")),
            }],
        },
        None,
        "initial",
        &limits,
    )
    .unwrap();
    let id = tree.resolve("file").unwrap().id;
    source.delay.store(true, Ordering::SeqCst);
    let writer_tree = tree.clone();
    let writer = tokio::spawn(async move { writer_tree.write(id, 0, b"edit").await });
    source.entered.notified().await;
    let error = tree
        .freeze_source_wait(
            SourceFrontendFence::TreeOnly,
            &limits,
            tokio::time::Instant::now() + Duration::from_millis(20),
        )
        .await
        .err()
        .unwrap();
    assert!(error.downcast_ref::<SourceFreezeTimeout>().is_some());
    assert!(
        !writer.is_finished(),
        "snapshot expiry must not cancel admitted work"
    );
    let error = tree
        .freeze_source(SourceFrontendFence::TreeOnly, &limits)
        .err()
        .unwrap();
    assert_eq!(
        error.downcast_ref::<SourceFreezeBusy>(),
        Some(&SourceFreezeBusy::ActiveOperations)
    );
    // Invalid limits must not be mistaken for contention, even with active IO.
    let invalid = SourceLimits {
        max_entries: 0,
        ..limits.clone()
    };
    let error = tree
        .freeze_source_wait(
            SourceFrontendFence::TreeOnly,
            &invalid,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .err()
        .unwrap();
    assert!(error.downcast_ref::<SourceFreezeTimeout>().is_none());
    assert!(error.downcast_ref::<SourceFreezeBusy>().is_none());
    source.release.notify_one();
    assert_eq!(writer.await.unwrap().unwrap(), 4);
    let snapshot = tree
        .freeze_source_wait(
            SourceFrontendFence::TreeOnly,
            &limits,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(snapshot.read_local(id, 0, 4).unwrap(), b"edit");
    drop(snapshot);
    assert_eq!(tree.write(id, 0, b"next").await.unwrap(), 4);
}

#[tokio::test]
async fn source_manifest_is_atomic_lazy_durable_and_preserves_dirty_generations() {
    use crate::source::*;
    let (dir, source, tree) = setup(CacheConfig::default());
    let limits = SourceLimits::default();
    let artifact = source.artifact(b"base");
    let manifest = SourceManifest {
        identity: tree.identity().into(),
        root: [1; 32],
        entries: vec![
            SourceEntry {
                path: "dir".into(),
                mode: 0o755,
                content: SourceContent::Directory,
            },
            SourceEntry {
                path: "dir/file".into(),
                mode: 0o644,
                content: SourceContent::File(artifact.clone()),
            },
            SourceEntry {
                path: "link".into(),
                mode: 0o777,
                content: SourceContent::Symlink("dir/file".into()),
            },
        ],
    };
    let installed = tree
        .install_source(manifest.clone(), None, "initial", &limits)
        .unwrap();
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        0,
        "manifest must never hydrate files"
    );
    let file = tree.resolve("dir/file").unwrap();
    assert_eq!(
        tree.readlink(tree.resolve("link").unwrap().id).unwrap(),
        "dir/file"
    );
    let pin = tree.pin(file.id).unwrap();
    let mut invalid = manifest.clone();
    invalid.entries.push(SourceEntry {
        path: "invalid/child".into(),
        mode: 0o644,
        content: SourceContent::File(artifact.clone()),
    });
    assert!(
        tree.install_source(invalid, Some(installed.version.clone()), "invalid", &limits)
            .is_err()
    );
    assert_eq!(tree.resolve("dir/file").unwrap().id, file.id);
    assert!(tree.resolve("invalid").is_err());
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    let mut oversized = manifest.clone();
    oversized.entries.push(SourceEntry {
        path: "a/".repeat(100_000),
        mode: 0o644,
        content: SourceContent::File(artifact.clone()),
    });
    let low_limits = SourceLimits {
        max_path_bytes: 16,
        max_depth: 2,
        ..limits.clone()
    };
    assert!(
        tree.install_source(
            oversized,
            Some(installed.version.clone()),
            "slash-rich",
            &low_limits
        )
        .is_err()
    );
    assert_eq!(tree.resolve("dir/file").unwrap().id, file.id);
    assert!(tree.source_receipt("slash-rich").unwrap().is_none());
    tree.write(file.id, 0, b"edit").await.unwrap();
    assert_eq!(tree.read(file.id, 0, 4).await.unwrap(), b"edit");
    assert!(
        tree.install_source(
            manifest.clone(),
            Some(installed.version.clone()),
            "stale",
            &limits
        )
        .is_err()
    );
    // Replaying a known request returns its original durable receipt, not a
    // claim that this receipt still describes the current dirty namespace.
    assert_eq!(
        tree.install_source(manifest.clone(), None, "initial", &limits)
            .unwrap(),
        installed
    );
    let locator = tree
        .install_source_control(b"opaque-locator", &limits)
        .unwrap();
    assert_eq!(
        tree.install_source_control(b"opaque-locator", &limits)
            .unwrap()
            .id,
        locator.id
    );
    assert!(tree.install_source_control(b"other", &limits).is_err());
    assert!(tree.rename(1, ".jj", 1, "leaked-control").await.is_err());
    assert!(tree.unlink(1, ".jj").await.is_err());
    assert!(tree.write(locator.id, 0, b"modified").await.is_err());
    let snapshot = tree
        .freeze_source(SourceFrontendFence::TreeOnly, &limits)
        .unwrap();
    assert!(snapshot.version().revision > installed.version.revision);
    assert_eq!(snapshot.read_local(file.id, 0, 4).unwrap(), b"edit");
    assert!(
        !snapshot
            .entries()
            .iter()
            .any(|entry| entry.path.starts_with(".jj"))
    );
    assert!(tree.create(1, "racing", 0o644).await.is_err());
    assert!(tree.write(file.id, 0, b"race").await.is_err());
    assert!(tree.unlink(1, "link").await.is_err());
    assert!(tree.rename(1, "link", 1, "moved").await.is_err());
    assert!(
        tree.setattr(
            file.id,
            SetAttr {
                size: Some(0),
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
    assert_eq!(snapshot.read_local(file.id, 0, 4).unwrap(), b"edit");
    let expected = snapshot.version().clone();
    let mut next = manifest.clone();
    next.root = [2; 32];
    let switched = snapshot.install(next.clone(), "switch", &limits).unwrap();
    assert_eq!(
        switched.version.generation,
        installed.version.generation + 1
    );
    assert_ne!(tree.resolve("dir/file").unwrap().id, file.id);
    assert_eq!(
        tree.read(file.id, 0, 4).await.unwrap(),
        b"edit",
        "old pinned generations retain local edits"
    );
    assert_eq!(tree.resolve(".jj/native-workspace").unwrap().id, locator.id);
    assert_eq!(
        tree.read(locator.id, 0, 32).await.unwrap(),
        b"opaque-locator"
    );
    assert_eq!(
        tree.install_source(next.clone(), Some(expected.clone()), "switch", &limits)
            .unwrap(),
        switched
    );
    let current = tree.resolve("dir/file").unwrap();
    tree.write(current.id, 0, b"kept").await.unwrap();
    let untracked = tree.create(1, "untracked", 0o600).await.unwrap();
    tree.write(untracked.id, 0, b"ignored local bytes")
        .await
        .unwrap();
    let snapshot = tree
        .freeze_source(SourceFrontendFence::TreeOnly, &limits)
        .unwrap();
    let acknowledge_expected = snapshot.version().clone();
    let acknowledged = snapshot
        .acknowledge_root(
            [3; 32],
            acknowledge_expected.clone(),
            "acknowledge",
            &limits,
        )
        .unwrap();
    assert_eq!(acknowledged.operation, SourceOperation::Acknowledge);
    assert_eq!(acknowledged.root, [3; 32]);
    assert_eq!(acknowledged.expected.as_ref(), Some(&acknowledge_expected));
    assert_eq!(
        tree.source_receipt("acknowledge").unwrap(),
        Some(acknowledged.clone())
    );
    assert_eq!(tree.resolve("dir/file").unwrap().id, current.id);
    assert!(tree.get(current.id).unwrap().remote.is_none());
    assert_eq!(tree.read(current.id, 0, 4).await.unwrap(), b"kept");
    assert_eq!(tree.resolve("untracked").unwrap().id, untracked.id);
    assert_eq!(
        tree.read(untracked.id, 0, 64).await.unwrap(),
        b"ignored local bytes"
    );
    let snapshot = tree
        .freeze_source(SourceFrontendFence::TreeOnly, &limits)
        .unwrap();
    assert_eq!(snapshot.base_root(), [3; 32]);
    assert_eq!(
        snapshot
            .acknowledge_root(
                [3; 32],
                acknowledge_expected.clone(),
                "acknowledge",
                &limits
            )
            .unwrap(),
        acknowledged
    );
    let snapshot = tree
        .freeze_source(SourceFrontendFence::TreeOnly, &limits)
        .unwrap();
    assert!(
        snapshot
            .acknowledge_root([4; 32], acknowledge_expected, "acknowledge", &limits)
            .is_err()
    );
    assert!(
        tree.install_source(next.clone(), Some(expected.clone()), "acknowledge", &limits)
            .is_err()
    );
    drop(pin);
    let cache = tree.cache.clone();
    drop(tree);
    let reopened =
        Tree::open_existing_with_cache_and_source(dir.path(), "test", cache, source.clone())
            .unwrap();
    assert_eq!(
        reopened.source_receipt("acknowledge").unwrap(),
        Some(acknowledged)
    );
    assert_eq!(reopened.resolve("untracked").unwrap().id, untracked.id);
    assert_eq!(
        reopened.read(untracked.id, 0, 64).await.unwrap(),
        b"ignored local bytes"
    );
    assert_eq!(
        reopened
            .install_source(next, Some(expected), "switch", &limits)
            .unwrap(),
        switched
    );
    assert_eq!(
        reopened
            .read(reopened.resolve(".jj/native-workspace").unwrap().id, 0, 32)
            .await
            .unwrap(),
        b"opaque-locator"
    );
    let mut reserved = manifest;
    reserved.identity = reopened.identity().into();
    reserved.entries.push(SourceEntry {
        path: ".JJ".into(),
        mode: 0o755,
        content: SourceContent::Directory,
    });
    let version = reopened
        .freeze_source(SourceFrontendFence::TreeOnly, &limits)
        .unwrap()
        .version()
        .clone();
    assert!(
        reopened
            .install_source(reserved, Some(version), "reserved", &limits)
            .is_err()
    );
    let cache = reopened.cache.clone();
    drop(reopened);
    {
        let db = SingleWriterTxDatabase::builder(dir.path().join("namespace.fjall"))
            .open()
            .unwrap();
        let keyspace = db
            .keyspace("namespace", KeyspaceCreateOptions::default)
            .unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.remove(&keyspace, b"header");
        tx.commit().unwrap();
    }
    assert!(
        Tree::open_existing_with_cache_and_source(
            dir.path(),
            "test",
            cache.clone(),
            source.clone()
        )
        .is_err()
    );
    {
        let db = SingleWriterTxDatabase::builder(dir.path().join("namespace.fjall"))
            .open()
            .unwrap();
        let keyspace = db
            .keyspace("namespace", KeyspaceCreateOptions::default)
            .unwrap();
        assert!(
            keyspace.get(b"header").unwrap().is_none(),
            "strict reopen must not reset a missing header"
        );
    }
    let missing = dir.path().join("absent");
    assert!(Tree::open_existing_with_cache_and_source(&missing, "test", cache, source).is_err());
    assert!(
        !missing.exists(),
        "strict reopen must not create missing state"
    );
}

#[tokio::test]
async fn source_freeze_rejects_inflight_cow_and_nonempty_initial_namespace() {
    use crate::source::*;
    let (_dir, source, tree) = setup(CacheConfig::default());
    let limits = SourceLimits::default();
    let artifact = source.artifact(b"remote");
    let manifest = SourceManifest {
        identity: tree.identity().into(),
        root: [3; 32],
        entries: vec![SourceEntry {
            path: "file".into(),
            mode: 0o644,
            content: SourceContent::File(artifact),
        }],
    };
    let installed = tree
        .install_source(manifest, None, "initial", &limits)
        .unwrap();
    let file = tree.resolve("file").unwrap();
    source.delay.store(true, Ordering::SeqCst);
    let writer_tree = tree.clone();
    let write = tokio::spawn(async move { writer_tree.write(file.id, 0, b"local").await });
    source.entered.notified().await;
    assert!(
        tree.freeze_source(SourceFrontendFence::TreeOnly, &limits)
            .is_err()
    );
    assert!(
        tree.retire_source_receipts(std::slice::from_ref(&installed), &limits)
            .is_err()
    );
    assert_eq!(
        tree.source_receipt("initial").unwrap(),
        Some(installed.clone())
    );
    source.release.notify_one();
    write.await.unwrap().unwrap();
    let snapshot = tree
        .freeze_source(SourceFrontendFence::TreeOnly, &limits)
        .unwrap();
    assert_eq!(snapshot.read_local(file.id, 0, 6).unwrap(), b"locale");
    drop(snapshot);
    let small = SourceLimits {
        max_receipts: 3,
        ..limits.clone()
    };
    // More iterations than the receipt quota remain possible once the trusted
    // controller durably acknowledges and retires each completed receipt.
    for index in 0..6 {
        let snapshot = tree
            .freeze_source(SourceFrontendFence::TreeOnly, &small)
            .unwrap();
        let expected = snapshot.version().clone();
        let root = [10 + index; 32];
        let request_id = format!("iteration-{index}");
        let receipt = snapshot
            .acknowledge_root(root, expected.clone(), &request_id, &small)
            .unwrap();
        let held = tree
            .freeze_source(SourceFrontendFence::TreeOnly, &small)
            .unwrap();
        assert!(
            tree.retire_source_receipts(std::slice::from_ref(&receipt), &small)
                .is_err()
        );
        drop(held);
        let mut missing = receipt.clone();
        missing.request_id = "missing-receipt".into();
        assert!(
            tree.retire_source_receipts(&[receipt.clone(), missing], &small)
                .is_err()
        );
        assert_eq!(
            tree.source_receipt(&request_id).unwrap(),
            Some(receipt.clone())
        );
        let baseline = tree.source_baseline().unwrap();
        assert_eq!(
            tree.retire_source_receipts(std::slice::from_ref(&receipt), &small)
                .unwrap(),
            1
        );
        assert_eq!(
            tree.source_baseline().unwrap(),
            baseline,
            "receipt cleanup must not change source baseline"
        );
        assert!(tree.source_receipt(&request_id).unwrap().is_none());
        let snapshot = tree
            .freeze_source(SourceFrontendFence::TreeOnly, &small)
            .unwrap();
        assert!(
            snapshot
                .acknowledge_root(root, expected, &request_id, &small)
                .is_err(),
            "forgotten old intent must fail stale, not apply anew"
        );
        assert_eq!(tree.read(file.id, 0, 6).await.unwrap(), b"locale");
    }
    let (_other_dir, _, other) = setup(CacheConfig::default());
    other.create(1, "existing", 0o644).await.unwrap();
    let empty = SourceManifest {
        identity: other.identity().into(),
        root: [4; 32],
        entries: vec![],
    };
    assert!(
        other
            .install_source(empty, None, "initial", &limits)
            .is_err()
    );
    assert!(other.resolve("existing").is_ok());
}

#[derive(Default)]
struct Directories {
    panic: AtomicBool,
    directories: Mutex<HashMap<[u8; 32], Vec<SourceDirectoryEntry>>>,
    calls: Mutex<Vec<([u8; 32], String)>>,
    delay: AtomicBool,
    fail: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    held_owner: Mutex<Option<Arc<dyn std::any::Any + Send + Sync>>>,
    retain_owner: AtomicBool,
}
#[async_trait]
impl SourceDirectoryProvider for Directories {
    async fn read_directory(
        &self,
        _: SourcePin,
        tree_id: [u8; 32],
        base_path: String,
        _: SourceLoadLimits,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<VerifiedSourceDirectory> {
        self.calls.lock().unwrap().push((tree_id, base_path));
        assert!(
            !self.panic.swap(false, Ordering::SeqCst),
            "injected provider panic"
        );
        if self.retain_owner.swap(false, Ordering::SeqCst) {
            *self.held_owner.lock().unwrap() = Some(owner);
        }
        if self.delay.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        ensure!(!self.fail.load(Ordering::SeqCst), "directory unavailable");
        let entries = self
            .directories
            .lock()
            .unwrap()
            .get(&tree_id)
            .context("missing directory")?
            .clone();
        Ok(VerifiedSourceDirectory { tree_id, entries })
    }
    async fn read_symlink(
        &self,
        _: SourcePin,
        _: [u8; 32],
        _: u64,
        _: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<String> {
        Ok("target".into())
    }
}
fn directory_entry(name: &str, tree_id: [u8; 32]) -> SourceDirectoryEntry {
    SourceDirectoryEntry {
        name: name.into(),
        mode: 0o755,
        kind: SourceDirectoryEntryKind::Directory { tree_id },
    }
}
fn file_entry(name: &str, artifact: Artifact, copy_id: Option<[u8; 32]>) -> SourceDirectoryEntry {
    SourceDirectoryEntry {
        name: name.into(),
        mode: 0o644,
        kind: SourceDirectoryEntryKind::File { artifact, copy_id },
    }
}
fn lazy_fixture(tree: &Tree, source: &Source, directories: &Arc<Directories>) -> LazySourceRoot {
    directories.directories.lock().unwrap().insert(
        [1; 32],
        vec![
            directory_entry("a", [2; 32]),
            file_entry("root.txt", source.artifact(b"root"), None),
            directory_entry("unvisited", [3; 32]),
        ],
    );
    directories.directories.lock().unwrap().insert(
        [2; 32],
        vec![file_entry(
            "tracked.txt",
            source.artifact(b"before"),
            Some([9; 32]),
        )],
    );
    directories.directories.lock().unwrap().insert(
        [3; 32],
        vec![file_entry(
            "untouched.txt",
            source.artifact(b"untouched"),
            None,
        )],
    );
    LazySourceRoot {
        identity: tree.identity().into(),
        root_tree: [1; 32],
        pin: SourcePin {
            root: [1; 32],
            generation: 1,
        },
        provider: directories.clone(),
        load_limits: SourceLoadLimits::default(),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_directories_are_not_absent_and_population_is_not_an_edit() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let directories = Arc::new(Directories::default());
    tree.install_lazy_source(
        lazy_fixture(&tree, &source, &directories),
        None,
        "install",
        &SourceLimits::default(),
    )
    .unwrap();
    assert!(
        tree.lookup(1, "a")
            .unwrap_err()
            .downcast_ref::<SourceNotLoaded>()
            .is_some()
    );
    assert!(
        tree.list(1, 0, 10)
            .unwrap_err()
            .downcast_ref::<SourceNotLoaded>()
            .is_some()
    );
    assert!(
        tree.freeze_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
            .is_err(),
        "a full inventory must never silently become partial"
    );
    let before = tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    let version = before.version().clone();
    drop(before);
    let a = tree.lookup_source(1, "a").await.unwrap();
    assert_eq!(directories.calls.lock().unwrap().len(), 1);
    let file = tree.lookup_source(a.id, "tracked.txt").await.unwrap();
    let after = tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    assert_eq!(after.version(), &version);
    assert!(after.changes().is_empty() && after.entries().is_empty());
    assert!(
        after
            .directories()
            .iter()
            .any(|d| d.path == "unvisited" && !d.loaded)
    );
    drop(after);
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tree.read(file.id, 0, 99).await.unwrap(), b"before");
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *directories.calls.lock().unwrap(),
        vec![([1; 32], String::new()), ([2; 32], "a".into())]
    );
    tree.rename(1, "unvisited", 1, "moved").await.unwrap();
    let moved = tree.lookup_source(1, "moved").await.unwrap();
    let snapshot = tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    assert!(
        snapshot
            .directories()
            .iter()
            .any(|d| d.path == "moved" && d.tree_id == [3; 32] && !d.loaded)
    );
    assert!(snapshot.changes().iter().any(|change| matches!(change,
        SourceNamespaceChange::Move { from, to, origin: Some(origin) }
        if from == "unvisited" && to == "moved" && origin.path == "unvisited")));
    let moved_version = snapshot.version().clone();
    drop(snapshot);
    tree.lookup_source(moved.id, "untouched.txt").await.unwrap();
    assert_eq!(
        directories.calls.lock().unwrap().last().unwrap(),
        &([3; 32], "unvisited".into())
    );
    let loaded = tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    assert_eq!(
        loaded.version(),
        &moved_version,
        "population under a moved directory is still not an edit"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_directory_fetch_singleflight_and_physical_owner_outlive_cancelled_waiters() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let directories = Arc::new(Directories::default());
    tree.install_lazy_source(
        lazy_fixture(&tree, &source, &directories),
        None,
        "install",
        &SourceLimits::default(),
    )
    .unwrap();
    directories.delay.store(true, Ordering::SeqCst);
    directories.retain_owner.store(true, Ordering::SeqCst);
    let first = {
        let tree = tree.clone();
        tokio::spawn(async move { tree.ensure_directory_loaded(1).await })
    };
    directories.entered.notified().await;
    first.abort();
    let _ = first.await;
    let second = {
        let tree = tree.clone();
        tokio::spawn(async move { tree.ensure_directory_loaded(1).await })
    };
    directories.release.notify_one();
    second.await.unwrap().unwrap();
    assert_eq!(directories.calls.lock().unwrap().len(), 1);
    assert!(
        tree.freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
            .unwrap_err()
            .downcast_ref::<SourceFreezeBusy>()
            .is_some(),
        "physical metadata workers retain the source fence"
    );
    directories.held_owner.lock().unwrap().take();
    tree.freeze_lazy_source_wait(
        SourceFrontendFence::TreeOnly,
        &SourceLimits::default(),
        tokio::time::Instant::now() + Duration::from_secs(2),
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn lazy_directory_failure_bounds_and_real_emptiness_never_publish_a_subset() {
    // Canonical prefixes are repeated in every child's provenance. The wire
    // object is small, but its expanded resident Records exceed this budget.
    {
        let (_dir, source, bounded) = setup(CacheConfig::default());
        let directories = Arc::new(Directories::default());
        let mut root = lazy_fixture(&bounded, &source, &directories);
        root.load_limits.max_directory_bytes = 64 * 1024;
        let component = "p".repeat(200);
        for index in 1..=10u8 {
            directories.directories.lock().unwrap().insert(
                [index; 32],
                vec![directory_entry(&component, [index + 1; 32])],
            );
        }
        directories.directories.lock().unwrap().insert(
            [11; 32],
            (0..16)
                .map(|index| file_entry(&format!("f{index:03}"), source.artifact(b"file"), None))
                .collect(),
        );
        let limits = SourceLimits {
            max_metadata_bytes: 128 * 1024,
            ..SourceLimits::default()
        };
        bounded
            .install_lazy_source(root, None, "prefix-bound", &limits)
            .unwrap();
        let mut parent = 1;
        for _ in 0..10 {
            parent = bounded.lookup_source(parent, &component).await.unwrap().id;
        }
        let before = bounded.known_source_nodes(&limits).unwrap().len();
        assert!(bounded.ensure_directory_loaded(parent).await.is_err());
        assert_eq!(
            bounded.known_source_nodes(&limits).unwrap().len(),
            before,
            "expanded-prefix failure must not publish or allocate a child subset"
        );
        assert!(
            bounded
                .lookup(parent, "f000")
                .unwrap_err()
                .downcast_ref::<SourceNotLoaded>()
                .is_some()
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    }
    {
        let (_dir, source, bounded) = setup(CacheConfig::default());
        let directories = Arc::new(Directories::default());
        let root = lazy_fixture(&bounded, &source, &directories);
        let limits = SourceLimits {
            max_entries: 3,
            ..SourceLimits::default()
        };
        bounded
            .install_lazy_source(root, None, "resident-bound", &limits)
            .unwrap();
        bounded.ensure_directory_loaded(1).await.unwrap();
        assert!(bounded.create(1, "new.txt", 0o644).await.is_err());
        bounded.unlink_file(1, "root.txt").await.unwrap();
        bounded.reclaim_retired(10).unwrap();
        bounded.create(1, "new.txt", 0o644).await.unwrap();
    }
    // A canonical object above the configured direct-entry limit must leave
    // the entire directory unknown, never publish a permitted prefix.
    {
        let (_dir, source, bounded) = setup(CacheConfig::default());
        let directories = Arc::new(Directories::default());
        let mut root = lazy_fixture(&bounded, &source, &directories);
        root.load_limits.max_directory_entries = 2;
        bounded
            .install_lazy_source(root, None, "bounded", &SourceLimits::default())
            .unwrap();
        assert!(bounded.ensure_directory_loaded(1).await.is_err());
        assert!(
            bounded
                .lookup(1, "a")
                .unwrap_err()
                .downcast_ref::<SourceNotLoaded>()
                .is_some()
        );
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    }
    let (_dir, source, tree) = setup(CacheConfig::default());
    let directories = Arc::new(Directories::default());
    tree.install_lazy_source(
        lazy_fixture(&tree, &source, &directories),
        None,
        "install",
        &SourceLimits::default(),
    )
    .unwrap();
    directories.panic.store(true, Ordering::SeqCst);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), tree.ensure_directory_loaded(1))
            .await
            .unwrap()
            .is_err()
    );
    directories.fail.store(true, Ordering::SeqCst);
    assert!(tree.ensure_directory_loaded(1).await.is_err());
    assert!(
        tree.list(1, 0, 10)
            .unwrap_err()
            .downcast_ref::<SourceNotLoaded>()
            .is_some()
    );
    directories.fail.store(false, Ordering::SeqCst);
    directories
        .directories
        .lock()
        .unwrap()
        .get_mut(&[1; 32])
        .unwrap()
        .push(directory_entry("a", [2; 32]));
    assert!(tree.ensure_directory_loaded(1).await.is_err());
    assert!(
        tree.lookup(1, "a")
            .unwrap_err()
            .downcast_ref::<SourceNotLoaded>()
            .is_some()
    );
    directories
        .directories
        .lock()
        .unwrap()
        .get_mut(&[1; 32])
        .unwrap()
        .pop();
    let unvisited = tree.lookup_source(1, "unvisited").await.unwrap();
    assert_eq!(
        *tree
            .unlink_directory(1, "unvisited")
            .await
            .unwrap_err()
            .downcast_ref::<CoreError>()
            .unwrap(),
        CoreError::NotEmpty
    );
    assert!(
        tree.create(unvisited.id, "untouched.txt", 0o644)
            .await
            .unwrap_err()
            .downcast_ref::<CoreError>()
            == Some(&CoreError::AlreadyExists)
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn lazy_delta_reopen_and_acknowledgement_keep_unvisited_siblings_and_local_authority() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let directories = Arc::new(Directories::default());
    tree.install_lazy_source(
        lazy_fixture(&tree, &source, &directories),
        None,
        "install",
        &SourceLimits::default(),
    )
    .unwrap();
    let a = tree.lookup_source(1, "a").await.unwrap();
    let file = tree.lookup_source(a.id, "tracked.txt").await.unwrap();
    tree.setattr(
        file.id,
        SetAttr {
            size: Some(0),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    tree.write(file.id, 0, b"edited").await.unwrap();
    tree.rename(a.id, "tracked.txt", a.id, "renamed.txt")
        .await
        .unwrap();
    let new = tree.create(a.id, "new.txt", 0o644).await.unwrap();
    tree.write(new.id, 0, b"new").await.unwrap();
    tree.unlink_file(1, "root.txt").await.unwrap();
    tree.reclaim_retired(100).unwrap();
    let before = tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    assert!(before.changes().iter().any(|c|matches!(c,SourceNamespaceChange::Move{from,to,..} if from=="a/tracked.txt"&&to=="a/renamed.txt")));
    assert!(
        before
            .changes()
            .iter()
            .any(|c| matches!(c,SourceNamespaceChange::Remove{path,..} if path=="root.txt"))
    );
    assert!(
        before
            .directories()
            .iter()
            .any(|d| d.path == "unvisited" && !d.loaded)
    );
    assert_eq!(before.read_local(file.id, 0, 99).unwrap(), b"edited");
    let expected = before.version().clone();
    let same_root = LazySourceRoot {
        identity: tree.identity().into(),
        root_tree: [1; 32],
        pin: SourcePin {
            root: [1; 32],
            generation: 2,
        },
        provider: directories.clone(),
        load_limits: SourceLoadLimits::default(),
    };
    assert!(
        before
            .acknowledge_root(
                same_root,
                Vec::new(),
                expected.clone(),
                "missing-ancestry",
                &SourceLimits::default()
            )
            .is_err()
    );
    assert!(tree.source_receipt("missing-ancestry").unwrap().is_none());
    drop(tree);
    let tree = Tree::open(dir.path(), "test", source.clone(), CacheConfig::default()).unwrap();
    tree.attach_lazy_source(
        directories.clone(),
        SourcePin {
            root: [1; 32],
            generation: 1,
        },
    )
    .unwrap();
    assert!(
        tree.lookup_source(1, "root.txt")
            .await
            .unwrap_err()
            .downcast_ref::<CoreError>()
            == Some(&CoreError::NotFound)
    );
    let snapshot = tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    assert_eq!(snapshot.version(), &expected);
    let new_entries = vec![
        file_entry("new.txt", source.artifact(b"new"), None),
        file_entry("renamed.txt", source.artifact(b"edited"), Some([10; 32])),
    ];
    let root_entries = vec![
        directory_entry("a", [12; 32]),
        directory_entry("unvisited", [3; 32]),
    ];
    directories
        .directories
        .lock()
        .unwrap()
        .insert([11; 32], root_entries.clone());
    directories
        .directories
        .lock()
        .unwrap()
        .insert([12; 32], new_entries.clone());
    let next = LazySourceRoot {
        identity: tree.identity().into(),
        root_tree: [11; 32],
        pin: SourcePin {
            root: [11; 32],
            generation: 2,
        },
        provider: directories.clone(),
        load_limits: SourceLoadLimits::default(),
    };
    let receipt = snapshot
        .acknowledge_root(
            next,
            vec![
                SourceBaselineUpdate {
                    symlink_targets: Default::default(),
                    path: String::new(),
                    directory: VerifiedSourceDirectory {
                        tree_id: [11; 32],
                        entries: root_entries,
                    },
                },
                SourceBaselineUpdate {
                    symlink_targets: Default::default(),
                    path: "a".into(),
                    directory: VerifiedSourceDirectory {
                        tree_id: [12; 32],
                        entries: new_entries,
                    },
                },
            ],
            expected,
            "ack",
            &SourceLimits::default(),
        )
        .unwrap();
    assert_eq!(receipt.version.generation, 2);
    assert!(
        tree.get(file.id).unwrap().remote.is_none(),
        "ACK never offloads local authoritative bytes"
    );
    let after = tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    assert!(after.entries().is_empty() && after.changes().is_empty());
    assert!(
        after
            .directories()
            .iter()
            .any(|d| d.path == "unvisited" && d.tree_id == [3; 32] && !d.loaded)
    );
    drop(after);
    assert_eq!(tree.read(file.id, 0, 99).await.unwrap(), b"edited");
    assert!(
        !directories
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(id, _)| *id == [3; 32])
    );
}
#[tokio::test]
async fn lazy_directory_cursor_rejects_mutations_and_root_replacement() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let directories = Arc::new(Directories::default());
    tree.install_lazy_source(
        lazy_fixture(&tree, &source, &directories),
        None,
        "install",
        &SourceLimits::default(),
    )
    .unwrap();
    let page = tree.list_source_page(1, None, 1).await.unwrap();
    let cursor = page.next.unwrap();
    tree.create(1, "local", 0o644).await.unwrap();
    assert_eq!(
        *tree
            .list_source_page(1, Some(cursor), 1)
            .await
            .unwrap_err()
            .downcast_ref::<CoreError>()
            .unwrap(),
        CoreError::Stale
    );
    let snapshot = tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    let next = LazySourceRoot {
        identity: tree.identity().into(),
        root_tree: [1; 32],
        pin: SourcePin {
            root: [1; 32],
            generation: 2,
        },
        provider: directories,
        load_limits: SourceLoadLimits::default(),
    };
    snapshot
        .install(next, "replace", &SourceLimits::default())
        .unwrap();
    assert!(
        tree.lookup(1, "a")
            .unwrap_err()
            .downcast_ref::<SourceNotLoaded>()
            .is_some()
    );
}

#[tokio::test]
async fn lazy_symlink_acknowledgement_rejects_same_size_different_target() {
    let (_dir, source, tree) = setup(CacheConfig::default());
    let directories = Arc::new(Directories::default());
    let initial = lazy_fixture(&tree, &source, &directories);
    directories
        .directories
        .lock()
        .unwrap()
        .insert([1; 32], Vec::new());
    tree.install_lazy_source(initial, None, "install", &SourceLimits::default())
        .unwrap();
    let link = tree.symlink(1, "link", "left").await.unwrap();
    let snapshot = tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    let expected = snapshot.version().clone();
    let root = LazySourceRoot {
        identity: tree.identity().into(),
        root_tree: [11; 32],
        pin: SourcePin {
            root: [11; 32],
            generation: 2,
        },
        provider: directories,
        load_limits: SourceLoadLimits::default(),
    };
    let update = SourceBaselineUpdate {
        path: String::new(),
        directory: VerifiedSourceDirectory {
            tree_id: [11; 32],
            entries: vec![SourceDirectoryEntry {
                name: "link".into(),
                mode: 0o777,
                kind: SourceDirectoryEntryKind::Symlink {
                    object_id: [66; 32],
                    target_size: 4,
                },
            }],
        },
        symlink_targets: std::collections::BTreeMap::from([("link".into(), "else".into())]),
    };
    assert!(
        snapshot
            .acknowledge_root(
                root,
                vec![update],
                expected.clone(),
                "wrong-target",
                &SourceLimits::default()
            )
            .is_err()
    );
    assert_eq!(tree.readlink(link.id).unwrap(), "left");
    assert!(tree.source_receipt("wrong-target").unwrap().is_none());
    let unchanged = tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    assert_eq!(unchanged.version(), &expected);
    assert_eq!(
        unchanged
            .entries()
            .iter()
            .find(|entry| entry.node.id == link.id)
            .unwrap()
            .target
            .as_deref(),
        Some("left")
    );
}

#[tokio::test]
async fn lazy_authoritative_payload_recovery_keeps_same_size_and_growing_edits_dirty() {
    for (behavior, bytes) in [
        ("same-size edit", b"abcdefghi".as_slice()),
        ("size repair", b"abcdefghij".as_slice()),
    ] {
        let (dir, source, tree) = setup(CacheConfig::default());
        let directories = Arc::new(Directories::default());
        let root = lazy_fixture(&tree, &source, &directories);
        directories.directories.lock().unwrap().insert(
            [1; 32],
            vec![file_entry("tracked", source.artifact(b"123456789"), None)],
        );
        tree.install_lazy_source(root, None, "install", &SourceLimits::default())
            .unwrap();
        let file = tree.lookup_source(1, "tracked").await.unwrap();
        // This public write copies up clean bytes but cannot complete its seek
        // or write at the unrepresentable file offset. It must have persisted
        // dirty intent before attempting physical mutation, not only after a
        // successful metadata commit. Replace bytes to model the crash window.
        assert!(tree.write(file.id, u64::MAX, b"x").await.is_err());
        assert!(tree.lookup(1, "tracked").is_err());
        assert!(tree.list(1, 0, 10).is_err());
        assert!(tree.resolve_source("tracked").await.is_err());
        assert!(
            tree.freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
                .is_err()
        );
        fs::write(dir.path().join("local").join(file.id.to_string()), bytes).unwrap();
        drop(tree);
        let reopened = Tree::open(dir.path(), "test", source, CacheConfig::default()).unwrap();
        let snapshot = reopened
            .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
            .unwrap();
        let entry = snapshot
            .entries()
            .iter()
            .find(|entry| entry.node.id == file.id)
            .unwrap_or_else(|| panic!("{behavior} must remain dirty after restart"));
        assert_eq!(entry.node.size, bytes.len() as u64, "{behavior}");
        assert_eq!(
            snapshot.read_local(file.id, 0, 99).unwrap(),
            bytes,
            "{behavior}"
        );
    }
}
