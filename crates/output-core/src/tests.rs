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
        max_inflight_bytes: 4096,
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
    let cache = LocalCas::open(
        root.path().join("shared-cas"),
        source.clone(),
        CacheConfig::default(),
    )
    .unwrap();
    let first = Tree::open_with_cache(root.path().join("first"), "test", cache.clone()).unwrap();
    let second = Tree::open_with_cache(root.path().join("second"), "test", cache).unwrap();
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
    first.write(first_node.id, 0, b"edited").await.unwrap();
    first.clean("").await.unwrap();
    assert_eq!(
        second.read(second_node.id, 0, 99).await.unwrap(),
        b"shared clean output"
    );
    assert!(second.resolve("different/path").is_ok());
}
