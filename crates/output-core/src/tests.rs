use super::*;
use std::sync::atomic::AtomicBool;
use tempfile::TempDir;
#[derive(Default)]
struct Source {
    data: Mutex<HashMap<Artifact, Vec<u8>>>,
    calls: AtomicU64,
    corrupt: AtomicBool,
    delay: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
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
async fn corrupt_payload_is_not_published_and_retry_is_possible() {
    let (dir, source, tree) = setup(CacheConfig::default());
    let a = source.artifact(b"valid");
    let n = tree.stage("out", a).await.unwrap();
    source.corrupt.store(true, Ordering::SeqCst);
    assert!(tree.read(n.id, 0, 99).await.is_err());
    assert_eq!(fs::read_dir(dir.path().join("cache")).unwrap().count(), 0);
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
    assert!(
        tree.prepare_stage("oversized", oversized)
            .await
            .unwrap_err()
            .downcast_ref::<CacheLimitError>()
            .is_some()
    );
    assert!(tree.resolve("oversized").is_err());
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
    assert_eq!(fs::read_dir(dir.path().join("cache")).unwrap().count(), 1);
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
