use super::*;
use futures::{AsyncRead, AsyncReadExt, io::Cursor};
use jj_lib::{
    backend::BackendResult,
    gitignore::GitIgnoreFile,
    matchers::{EverythingMatcher, NothingMatcher},
};
use jj_native_backend::{
    CopyCatalogLimits, CopyCatalogSnapshot, DirectObjectIo, FileRead, VerifiedMetadata,
};
use local_cas::{CacheConfig, LocalCas};
use native_workspace::{Limits, Scope};
use output_core::{NodeKind, SourceFrontendFence};
use std::sync::atomic::AtomicUsize;

#[derive(Debug)]
struct CountingIo {
    direct: Arc<DirectObjectIo>,
    reads: AtomicUsize,
    uploads: AtomicUsize,
    metadata_reads: Mutex<Vec<m::ObjectKey>>,
}
#[async_trait]
impl NativeObjectIo for CountingIo {
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        self.direct.descriptor(key).await
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.direct.read_metadata(key).await
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<VerifiedMetadata> {
        self.metadata_reads.lock().unwrap().push(key);
        self.direct.read_metadata_with_owner(key, owner).await
    }
    async fn write_metadata(&self, object: m::Object) -> BackendResult<m::Descriptor> {
        self.direct.write_metadata(object).await
    }
    async fn read_file_for_cache(
        &self,
        id: m::FileId,
        _owner: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<FileRead> {
        self.read_file(id).await
    }
    async fn read_file(&self, id: m::FileId) -> BackendResult<FileRead> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.direct.read_file(id).await
    }
    async fn write_file(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max: u64,
    ) -> BackendResult<m::FileId> {
        self.uploads.fetch_add(1, Ordering::SeqCst);
        self.direct.write_file(input, max).await
    }
    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot> {
        self.direct.copy_catalog_snapshot(limits).await
    }
}
struct Fixture {
    _directory: tempfile::TempDir,
    root: PathBuf,
    state: PathBuf,
    working_copy: PathBuf,
    provider: Arc<CountingIo>,
    compiler: Arc<Compiler>,
    tree: Arc<Tree>,
    _cache: Arc<LocalCas>,
    scope: Scope,
    base: m::TreeId,
}
impl Fixture {
    async fn new(ignore: bool) -> Self {
        Self::with_mode(ignore, false).await
    }
    async fn lazy(ignore: bool) -> Self {
        Self::with_mode(ignore, true).await
    }
    async fn with_mode(ignore: bool, lazy: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("mount");
        fs::create_dir(&root).unwrap();
        let state = directory.path().join("state");
        let _state = PrivateDir::create(&state).unwrap();
        let working_copy = state.join("jj");
        let _working = PrivateDir::create(&working_copy).unwrap();
        let objects = directory.path().join("objects");
        let _objects = PrivateDir::create(&objects).unwrap();
        let direct =
            Arc::new(DirectObjectIo::init(&objects, jj_native_backend::Config::default()).unwrap());
        let provider = Arc::new(CountingIo {
            direct,
            reads: AtomicUsize::new(0),
            uploads: AtomicUsize::new(0),
            metadata_reads: Mutex::new(Vec::new()),
        });
        let mut entries = Vec::new();
        for (name, bytes, executable) in [
            ("tracked", b"base".as_slice(), true),
            ("untouched", b"quiet".as_slice(), false),
        ] {
            let id = provider
                .write_file(&mut Cursor::new(bytes), bytes.len() as u64)
                .await
                .unwrap();
            entries.push(m::TreeEntry {
                name: name.into(),
                value: m::TreeValue::File {
                    id,
                    executable,
                    size_bytes: bytes.len() as u64,
                    copy_id: None,
                },
            });
        }
        if ignore {
            let bytes = b"ignored\n";
            let id = provider
                .write_file(&mut Cursor::new(bytes), bytes.len() as u64)
                .await
                .unwrap();
            entries.push(m::TreeEntry {
                name: ".gitignore".into(),
                value: m::TreeValue::File {
                    id,
                    executable: false,
                    size_bytes: bytes.len() as u64,
                    copy_id: None,
                },
            });
        }
        if lazy {
            let copy = provider
                .write_metadata(m::Object::CopyHistory(m::CopyHistory {
                    current_path: "tracked".into(),
                    parents: Vec::new(),
                    salt: Vec::new(),
                }))
                .await
                .unwrap();
            if let m::TreeValue::File { copy_id, .. } = &mut entries
                .iter_mut()
                .find(|entry| entry.name == "tracked")
                .unwrap()
                .value
            {
                *copy_id = Some(m::CopyId(copy.key.id.0));
            }
            let subtree = provider
                .write_metadata(m::Object::Tree(m::Tree {
                    entries: vec![m::TreeEntry {
                        name: "deep".into(),
                        value: entries
                            .iter()
                            .find(|entry| entry.name == "untouched")
                            .unwrap()
                            .value
                            .clone(),
                    }],
                }))
                .await
                .unwrap();
            entries.push(m::TreeEntry {
                name: "unvisited".into(),
                value: m::TreeValue::Tree(m::TreeId(subtree.key.id.0)),
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let base = m::TreeId(
            provider
                .write_metadata(m::Object::Tree(m::Tree { entries }))
                .await
                .unwrap()
                .key
                .id
                .0,
        );
        let scope = Scope::from_configured_identity("fixture", b"public-test-profile").unwrap();
        let compiler =
            Arc::new(Compiler::new(provider.clone(), scope.clone(), Limits::default()).unwrap());
        let cache = LocalCas::open(
            directory.path().join("cache"),
            compiler.source(),
            CacheConfig::default(),
        )
        .unwrap();
        let namespace = state.join("namespace");
        let _namespace = PrivateDir::create(&namespace).unwrap();
        let tree = Tree::open_with_cache_and_source(
            namespace,
            scope.backend(),
            cache.clone(),
            compiler.source(),
        )
        .unwrap();
        if lazy {
            let source = compiler
                .lazy_tree_for_source(base, 1, SourceLimits::default())
                .unwrap();
            tree.install_lazy_source(
                source.source_root(tree.identity()),
                None,
                "bootstrap",
                &SourceLimits::default(),
            )
            .unwrap();
        } else {
            let compiled = compiler.compile_tree(base).await.unwrap();
            tree.install_source(
                compiled.manifest(tree.identity()),
                None,
                "bootstrap",
                &SourceLimits::default(),
            )
            .unwrap();
        }
        provider.reads.store(0, Ordering::SeqCst);
        provider.uploads.store(0, Ordering::SeqCst);
        Self {
            root,
            state,
            working_copy,
            provider,
            compiler,
            tree,
            _cache: cache,
            scope,
            base,
            _directory: directory,
        }
    }
    async fn open(&self, config: Option<Config>) -> Controller {
        Controller::open_host(
            Host::Protocol {
                tree: self.tree.clone(),
                compiler: self.compiler.clone(),
                root: self.root.clone(),
                state: self.state.clone(),
                identity: [7; 32],
                backend: self.scope.backend().into(),
            },
            self.provider.clone(),
            config.unwrap_or_else(|| Config::new("fixture".into(), self.working_copy.clone())),
        )
        .await
        .unwrap()
    }
    async fn local(&self, name: &str, bytes: &[u8]) -> u64 {
        let node = self
            .tree
            .create(self.tree.root_id(), name, 0o644)
            .await
            .unwrap();
        self.tree.write(node.id, 0, bytes).await.unwrap();
        node.id
    }
    fn reset_counts(&self) {
        self.provider.reads.store(0, Ordering::SeqCst);
        self.provider.uploads.store(0, Ordering::SeqCst);
        self.provider.metadata_reads.lock().unwrap().clear();
    }
}
fn options() -> SnapshotOptions<'static> {
    SnapshotOptions {
        base_ignores: GitIgnoreFile::empty(),
        progress: None,
        start_tracking_matcher: &EverythingMatcher,
        force_tracking_matcher: &NothingMatcher,
        max_new_file_size: 1024,
    }
}
fn operation() -> OperationId {
    OperationId::new(vec![3; 64])
}
async fn bytes(provider: &dyn NativeObjectIo, id: m::FileId) -> Vec<u8> {
    let (_, mut reader) = provider.read_file(id).await.unwrap().into_parts();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    bytes
}
async fn files(provider: &dyn NativeObjectIo, id: m::TreeId) -> BTreeMap<String, m::TreeValue> {
    match provider
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(id.0),
        })
        .await
        .unwrap()
        .into_object()
    {
        m::Object::Tree(tree) => tree
            .entries
            .into_iter()
            .map(|entry| (entry.name, entry.value))
            .collect(),
        _ => unreachable!(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_reuses_clean_native_ids_and_does_not_reupload_unchanged_local_payloads() {
    let fixture = Fixture::new(false).await;
    let controller = fixture.open(None).await;
    let before = controller.inspect().unwrap();
    let mut mutation = controller.begin(&before).await.unwrap();
    let (receipt, _) = mutation.snapshot("clean", &options()).await.unwrap();
    assert_eq!(receipt.after.tree.terms, vec![fixture.base.0]);
    assert_eq!(fixture.provider.reads.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 0);
    mutation
        .finish(&receipt.after, &operation(), Some(&receipt))
        .await
        .unwrap();
    drop(mutation);
    let tracked = fixture.tree.resolve("tracked").unwrap();
    fixture.tree.write(tracked.id, 0, b"edit").await.unwrap();
    fixture.reset_counts();
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (receipt, _) = mutation.snapshot("dirty", &options()).await.unwrap();
    assert_eq!(fixture.provider.reads.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tree.resolve("tracked").unwrap().id, tracked.id);
    assert!(
        fixture.tree.get(tracked.id).unwrap().remote.is_none(),
        "snapshot must not offload local authoritative bytes"
    );
    mutation
        .finish(&receipt.after, &operation(), Some(&receipt))
        .await
        .unwrap();
    drop(mutation);
    fixture.reset_counts();
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (receipt, _) = mutation.snapshot("same-local", &options()).await.unwrap();
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.reads.load(Ordering::SeqCst), 0);
    let actual = files(
        fixture.provider.as_ref(),
        m::TreeId(receipt.after.tree.terms[0]),
    )
    .await;
    let m::TreeValue::File { id, executable, .. } = actual["tracked"] else {
        panic!()
    };
    assert!(executable);
    assert_eq!(bytes(fixture.provider.as_ref(), id).await, b"edit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_policy_preserves_ignored_and_large_untracked_files_without_uploading_them() {
    let fixture = Fixture::new(true).await;
    let controller = fixture.open(None).await;
    let ignored = fixture.local("ignored", b"private ignored bytes").await;
    let large = fixture.local("large", b"oversized").await;
    let tracked_new = fixture.local("new", b"ok").await;
    fixture.reset_counts();
    let mut policy = options();
    policy.max_new_file_size = 2;
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (receipt, stats) = mutation.snapshot("policy", &policy).await.unwrap();
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.provider.reads.load(Ordering::SeqCst),
        1,
        "only .gitignore policy contents are needed"
    );
    assert!(
        stats
            .untracked_paths
            .contains_key(&jj_lib::repo_path::RepoPathBuf::from_internal_string("large").unwrap())
    );
    let actual = files(
        fixture.provider.as_ref(),
        m::TreeId(receipt.after.tree.terms[0]),
    )
    .await;
    assert!(actual.contains_key("new"));
    assert!(!actual.contains_key("ignored"));
    assert!(!actual.contains_key("large"));
    assert_eq!(fixture.tree.resolve("ignored").unwrap().id, ignored);
    assert_eq!(fixture.tree.resolve("large").unwrap().id, large);
    assert_eq!(fixture.tree.resolve("new").unwrap().id, tracked_new);
    assert_eq!(
        fixture.tree.read(ignored, 0, 100).await.unwrap(),
        b"private ignored bytes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkout_rejects_untracked_collisions_and_keeps_old_generation_readers() {
    let fixture = Fixture::new(false).await;
    let controller = fixture.open(None).await;
    let empty = NativeTree {
        terms: vec![m::empty_tree().descriptor.key.id.0],
        labels: Vec::new(),
    };
    let local = fixture.local("untracked", b"keep").await;
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        mutation.check_out("unsafe", &empty).await,
        Err(ProjectionFailure::Rejected(_))
    ));
    assert_eq!(fixture.tree.read(local, 0, 100).await.unwrap(), b"keep");
    drop(mutation);
    fixture
        .tree
        .unlink_file(fixture.tree.root_id(), "untracked")
        .await
        .unwrap();
    let old = fixture.tree.resolve("untouched").unwrap();
    let _pin = fixture.tree.pin(old.id).unwrap();
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (receipt, stats) = mutation.check_out("safe", &empty).await.unwrap();
    assert_eq!(stats.removed_files, 2);
    assert!(fixture.tree.resolve("untouched").is_err());
    assert_eq!(fixture.tree.read(old.id, 0, 100).await.unwrap(), b"quiet");
    assert_eq!(receipt.after.tree, empty);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_source_ack_recovers_controller_wal_gap_and_missing_receipts_stay_unknown() {
    let fixture = Fixture::new(false).await;
    let controller = fixture.open(None).await;
    let before = controller.inspect().unwrap();
    let frozen = fixture
        .tree
        .freeze_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    let expected = frozen.version().clone();
    let root = m::empty_tree().descriptor.key.id.0;
    let target = NativeTree {
        terms: vec![root],
        labels: Vec::new(),
    };
    controller
        .prepare(
            "ack-gap",
            &before,
            Action::Reset {
                tree: target.clone(),
                recover: true,
            },
            expected.clone(),
        )
        .unwrap();
    frozen
        .acknowledge_root(root, expected, "ack-gap", &SourceLimits::default())
        .unwrap();
    // Models the two durable-file boundary; not a power-loss or SIGKILL test.
    drop(controller);
    let controller = fixture.open(None).await;
    let receipt = match controller.receipt("ack-gap").await.unwrap() {
        ProjectionOutcome::Applied(receipt) => receipt,
        _ => panic!("durable source proof must recover"),
    };
    assert_eq!(receipt.before, before);
    assert_eq!(receipt.after.tree, target);
    assert!(matches!(
        controller.receipt("absent").await.unwrap(),
        ProjectionOutcome::Unknown
    ));
    assert_eq!(
        fixture.tree.resolve("tracked").unwrap().kind,
        NodeKind::File,
        "reset/ack never replaces the namespace"
    );
    drop(controller);

    // The lazy checkout's core receipt can land before the controller records
    // native publication completion. Recovery must replay the ORIGINAL expected
    // generation, not derive a new request from the already-advanced pin.
    let fixture = Fixture::lazy(false).await;
    fixture
        .provider
        .write_metadata(m::Object::Tree(m::Tree::default()))
        .await
        .unwrap();
    let controller = fixture.open(None).await;
    let before = controller.inspect().unwrap();
    let retained = fixture.local("before-publication", b"retained").await;
    let capture = fixture
        .tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    let expected = capture.version().clone();
    drop(capture);
    controller
        .prepare(
            "lazy-checkout-gap",
            &before,
            Action::CheckOut {
                tree: target.clone(),
            },
            expected.clone(),
        )
        .unwrap();
    let source = controller
        .inner
        .host
        .publish(
            &target,
            expected.clone(),
            "lazy-checkout-gap",
            &SourceLimits::default(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(source.version.generation, expected.generation + 1);
    assert_eq!(
        fixture
            .tree
            .resolve_source("before-publication")
            .await
            .unwrap()
            .id,
        retained
    );
    assert_eq!(
        fixture.tree.read(retained, 0, 100).await.unwrap(),
        b"retained"
    );
    let local = fixture.local("after-publication", b"preserved").await;
    drop(controller);
    let controller = fixture.open(None).await;
    let receipt = match controller.receipt("lazy-checkout-gap").await.unwrap() {
        ProjectionOutcome::Applied(receipt) => receipt,
        _ => panic!("durable lazy checkout proof must recover"),
    };
    assert_eq!(receipt.before, before);
    assert_eq!(receipt.after.tree, target);
    assert_eq!(receipt.after.generation, source.version.generation);
    assert_eq!(
        fixture.tree.source_receipt("lazy-checkout-gap").unwrap(),
        Some(source)
    );
    assert_eq!(
        fixture
            .tree
            .resolve_source("after-publication")
            .await
            .unwrap()
            .id,
        local
    );
    assert_eq!(
        fixture.tree.read(local, 0, 100).await.unwrap(),
        b"preserved"
    );
    drop(controller);
    let controller = fixture.open(None).await;
    assert_eq!(
        controller.inspect().unwrap(),
        receipt.after,
        "repeated reopen must not advance the checkout generation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_receipt_retirement_keeps_repeated_iterations_within_a_small_budget() {
    let fixture = Fixture::new(false).await;
    let mut config = Config::new("fixture".into(), fixture.working_copy.clone());
    config.source_limits.max_receipts = 2;
    let controller = fixture.open(Some(config)).await;
    for index in 0..8 {
        let mut mutation = controller
            .begin(&controller.inspect().unwrap())
            .await
            .unwrap();
        let (receipt, _) = mutation
            .snapshot(&format!("iteration-{index}"), &options())
            .await
            .unwrap();
        mutation
            .finish(&receipt.after, &operation(), Some(&receipt))
            .await
            .unwrap();
        drop(mutation);
        assert!(
            fixture
                .tree
                .source_receipt(&format!("iteration-{index}"))
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(fixture.provider.reads.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracking_matchers_and_preflight_limits_fail_without_losing_local_data() {
    let fixture = Fixture::new(false).await;
    let controller = fixture.open(None).await;
    let local = fixture.local("new", b"data").await;
    let mut policy = options();
    policy.start_tracking_matcher = &NothingMatcher;
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (receipt, stats) = mutation.snapshot("no-auto-track", &policy).await.unwrap();
    assert_eq!(stats.untracked_paths.len(), 1);
    assert_eq!(receipt.after.tree.terms, vec![fixture.base.0]);
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tree.read(local, 0, 100).await.unwrap(), b"data");
    let unresolved = NativeTree {
        terms: vec![fixture.base.0, fixture.base.0, fixture.base.0],
        labels: vec!["left".into(), "base".into(), "right".into()],
    };
    assert!(matches!(
        mutation.check_out("unresolved", &unresolved).await,
        Err(ProjectionFailure::Rejected(_))
    ));
    assert_eq!(fixture.tree.resolve("new").unwrap().id, local);
}

#[test]
fn transport_policy_evaluator_preserves_layered_ignores_force_tracking_and_progress() {
    let mut policy = options();
    policy.base_ignores = GitIgnoreFile::empty()
        .chain(
            jj_lib::repo_path::RepoPath::root(),
            Path::new("global-ignore"),
            b"*.tmp\n",
        )
        .unwrap();
    policy.max_new_file_size = 4;
    let forced = jj_lib::matchers::PrefixMatcher::new(&[
        jj_lib::repo_path::RepoPathBuf::from_internal_string("forced.tmp").unwrap(),
    ]);
    policy.force_tracking_matcher = &forced;
    let progress = Mutex::new(Vec::new());
    let callback = |path: &jj_lib::repo_path::RepoPath| {
        progress
            .lock()
            .unwrap()
            .push(path.as_internal_file_string().to_owned())
    };
    policy.progress = Some(&callback);
    let inventory = PolicyInventory {
        candidates: [
            ("allow.tmp", false, 2),
            ("base.tmp", false, 2),
            ("existing.tmp", true, 20),
            ("forced.tmp", false, 20),
            ("ignored/reinclude", false, 2),
            ("large", false, 20),
        ]
        .into_iter()
        .map(|(path, tracked, size)| PolicyCandidate {
            path: path.into(),
            kind: PolicyKind::File,
            tracked,
            size,
        })
        .collect(),
        ignore_files: vec![
            PolicyIgnore {
                directory: "".into(),
                contents: b"!allow.tmp\nignored/\n".to_vec(),
            },
            PolicyIgnore {
                directory: "ignored".into(),
                contents: b"!reinclude\n".to_vec(),
            },
        ],
    };
    assert_eq!(
        evaluate_inventory(&policy, &inventory).unwrap(),
        vec![
            PolicyDecision::Track,
            PolicyDecision::Ignored,
            PolicyDecision::Track,
            PolicyDecision::Track,
            PolicyDecision::Ignored,
            PolicyDecision::TooLarge { max_size: 4 }
        ]
    );
    assert_eq!(
        *progress.lock().unwrap(),
        vec!["allow.tmp", "existing.tmp", "forced.tmp"]
    );
    // Force permits an ignored/large path, not a path excluded from automatic
    // tracking. This matches jj's independent start-tracking gate.
    policy.start_tracking_matcher = &NothingMatcher;
    let blocked = evaluate_inventory(&policy, &inventory).unwrap();
    assert_eq!(blocked[3], PolicyDecision::NotAutoTracked);
    assert_eq!(blocked[2], PolicyDecision::Track);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_policy_wait_keeps_the_same_frozen_bytes_and_rejects_missing_tracked_paths() {
    struct Policy {
        ready: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
    }
    #[async_trait]
    impl SnapshotPolicy for Policy {
        fn fingerprint(&self) -> [u8; 32] {
            [9; 32]
        }
        async fn evaluate(&self, inventory: &PolicyInventory) -> Result<Vec<PolicyDecision>> {
            self.ready.notify_one();
            let _permit = self.release.acquire().await?;
            Ok(inventory
                .candidates
                .iter()
                .map(|_| PolicyDecision::Ignored)
                .collect())
        }
    }
    let fixture = Fixture::new(false).await;
    let controller = fixture.open(None).await;
    let local = fixture.local("new", b"stable").await;
    let policy = Arc::new(Policy {
        ready: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let owned = policy.clone();
    let task = tokio::spawn(async move {
        let mut session = controller
            .begin_session(&controller.inspect().unwrap())
            .await
            .unwrap();
        session
            .snapshot_with_policy("bad-policy", owned.as_ref())
            .await
    });
    policy.ready.notified().await;
    assert!(
        fixture.tree.write(local, 0, b"changed").await.is_err(),
        "policy evaluation must not release the frozen byte view"
    );
    policy.release.add_permits(1);
    assert!(matches!(
        task.await.unwrap(),
        Err(ProjectionFailure::Rejected(_))
    ));
    assert_eq!(fixture.tree.read(local, 0, 100).await.unwrap(), b"stable");
    assert_eq!(fixture.tree.source_baseline().unwrap().0, fixture.base.0);
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_factory_and_controller_agree_on_unlabeled_snapshot_and_checkout() {
    use jj_lib::{
        config::StackedConfig, settings::UserSettings, signing::Signer,
        working_copy::WorkingCopyFactory,
    };
    let f = Fixture::new(false).await;
    let controller = f.open(None).await;
    let backend = jj_native_backend::NativeBackend::from_io(f.provider.clone(), Default::default())
        .await
        .unwrap();
    let store = jj_lib::store::Store::new(
        Box::new(backend),
        Signer::new(None, vec![]),
        jj_lib::tree_merge::MergeOptions::from_settings(
            &UserSettings::from_config_and_home_dir(StackedConfig::with_defaults(), None).unwrap(),
        )
        .unwrap(),
    );
    let settings =
        UserSettings::from_config_and_home_dir(StackedConfig::with_defaults(), None).unwrap();
    let factory = jj_vfs::native::NativeVfsWorkingCopyFactory::new(
        "fixture".into(),
        Arc::new(controller.clone()),
    );
    let wc = factory
        .init_working_copy(
            store.clone(),
            f.root.clone(),
            f.working_copy.clone(),
            operation(),
            jj_lib::ref_name::WorkspaceName::DEFAULT.to_owned(),
            &settings,
        )
        .unwrap();
    assert!(wc.tree().unwrap().labels().as_slice().is_empty());
    let mut locked = wc.start_mutation().await.unwrap();
    let (snapshotted, _) = locked.snapshot(&options()).await.unwrap();
    assert!(snapshotted.labels().as_slice().is_empty());
    assert_eq!(
        snapshotted.tree_ids().as_resolved().unwrap().as_bytes(),
        &f.base.0
    );
    locked.check_out(&store.root_commit()).await.unwrap();
    let next_op = OperationId::new(vec![4; 64]);
    let wc = locked.finish(next_op.clone()).await.unwrap();
    assert_eq!(wc.operation_id(), &next_op);
    assert!(wc.tree().unwrap().labels().as_slice().is_empty());
    assert_eq!(
        wc.tree().unwrap().tree_ids().as_resolved().unwrap(),
        store.empty_tree_id()
    );
    assert_eq!(f.provider.reads.load(Ordering::SeqCst), 0);
    assert_eq!(f.provider.uploads.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_directory_prefix_does_not_hide_selected_descendant() {
    let f = Fixture::new(false).await;
    let empty = m::TreeId(m::empty_tree().descriptor.key.id.0);
    let root = f
        .provider
        .write_metadata(m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "a".into(),
                value: m::TreeValue::Tree(empty),
            }],
        }))
        .await
        .unwrap();
    let compiled = f
        .compiler
        .compile_tree(m::TreeId(root.key.id.0))
        .await
        .unwrap();
    let snapshot = f
        .tree
        .freeze_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    snapshot
        .install(
            compiled.manifest(f.tree.identity()),
            "empty-directory-baseline",
            &SourceLimits::default(),
        )
        .unwrap();
    f.local("a-b", b"sibling").await;
    let a = f.tree.resolve("a").unwrap();
    let x = f.tree.create(a.id, "x", 0o644).await.unwrap();
    f.tree.write(x.id, 0, b"descendant").await.unwrap();
    let controller = f.open(None).await;
    let before = controller.inspect().unwrap();
    let mut session = controller.begin_session(&before).await.unwrap();
    let (receipt, _) = session
        .snapshot("empty-directory-descendant", &options())
        .await
        .unwrap();
    let metadata = f
        .provider
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(receipt.after.tree.terms[0]),
        })
        .await
        .unwrap();
    let m::Object::Tree(tree) = metadata.into_object() else {
        panic!("expected canonical tree")
    };
    let m::TreeValue::Tree(child) = tree.entries.iter().find(|e| e.name == "a").unwrap().value
    else {
        panic!("expected directory")
    };
    assert_ne!(child, empty);
    assert!(tree.entries.iter().any(|e| e.name == "a-b"));
}

mod scope_tests;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_snapshot_preserves_unvisited_subtrees_and_replays_without_inventory_expansion() {
    let fixture = Fixture::lazy(false).await;
    let before_files = files(fixture.provider.as_ref(), fixture.base).await;
    let m::TreeValue::Tree(hidden) = before_files["unvisited"] else {
        panic!()
    };
    let controller = fixture.open(None).await;
    fixture.reset_counts();
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (clean, _) = mutation.snapshot("lazy-clean", &options()).await.unwrap();
    assert_eq!(clean.after.tree.terms, vec![fixture.base.0]);
    assert!(
        fixture.provider.metadata_reads.lock().unwrap().is_empty(),
        "clean snapshot must not enumerate a directory"
    );
    mutation
        .finish(&clean.after, &operation(), Some(&clean))
        .await
        .unwrap();
    drop(mutation);
    let tracked = fixture.tree.resolve_source("tracked").await.unwrap();
    fixture.tree.write(tracked.id, 0, b"edit").await.unwrap();
    fixture.reset_counts();
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (receipt, _) = mutation.snapshot("lazy-edit", &options()).await.unwrap();
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 1);
    assert!(
        !fixture
            .provider
            .metadata_reads
            .lock()
            .unwrap()
            .contains(&m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(hidden.0)
            })
    );
    let actual = files(
        fixture.provider.as_ref(),
        m::TreeId(receipt.after.tree.terms[0]),
    )
    .await;
    assert_eq!(actual["unvisited"], m::TreeValue::Tree(hidden));
    assert_eq!(
        fixture.tree.resolve_source("tracked").await.unwrap().id,
        tracked.id
    );
    assert!(fixture.tree.get(tracked.id).unwrap().remote.is_none());
    fixture.reset_counts();
    let (replayed, _) = mutation.snapshot("lazy-edit", &options()).await.unwrap();
    assert_eq!(replayed, receipt);
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 0);
    assert!(fixture.provider.metadata_reads.lock().unwrap().is_empty());
    mutation
        .finish(&receipt.after, &operation(), Some(&receipt))
        .await
        .unwrap();
    drop(mutation);
    fixture.reset_counts();
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    mutation.snapshot("lazy-same", &options()).await.unwrap();
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 0);
    assert!(fixture.provider.metadata_reads.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_policy_keeps_ignored_local_entries_and_verified_symlink_targets() {
    let fixture = Fixture::lazy(true).await;
    let controller = fixture.open(None).await;
    let ignored = fixture.local("ignored", b"not uploaded").await;
    let new = fixture.local("new", b"ok").await;
    fixture
        .tree
        .symlink(fixture.tree.root_id(), "link", "new")
        .await
        .unwrap();
    fixture.reset_counts();
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (receipt, _) = mutation.snapshot("lazy-policy", &options()).await.unwrap();
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.provider.reads.load(Ordering::SeqCst),
        1,
        "only ignore-policy bytes are read"
    );
    let actual = files(
        fixture.provider.as_ref(),
        m::TreeId(receipt.after.tree.terms[0]),
    )
    .await;
    assert!(actual.contains_key("new") && actual.contains_key("link"));
    assert!(!actual.contains_key("ignored"));
    assert_eq!(
        fixture.tree.resolve_source("ignored").await.unwrap().id,
        ignored
    );
    assert_eq!(fixture.tree.resolve_source("new").await.unwrap().id, new);
    assert_eq!(
        fixture.tree.read(ignored, 0, 100).await.unwrap(),
        b"not uploaded"
    );
    let capture = fixture
        .tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    assert_eq!(
        capture
            .entries()
            .iter()
            .filter(|entry| entry.path == "ignored")
            .count(),
        1,
        "excluded content remains a dirty overlay"
    );
    drop(capture);
    let target = fixture
        .provider
        .write_metadata(m::Object::Tree(m::Tree::default()))
        .await
        .unwrap();
    let native = NativeTree {
        terms: vec![target.key.id.0],
        labels: Vec::new(),
    };
    let (checkout, _) = mutation
        .check_out("lazy-policy-checkout", &native)
        .await
        .unwrap();
    assert_eq!(checkout.after.tree, native);
    assert_eq!(
        fixture.tree.resolve_source("ignored").await.unwrap().id,
        ignored
    );
    assert_eq!(
        fixture.tree.read(ignored, 0, 100).await.unwrap(),
        b"not uploaded"
    );
    assert!(fixture.tree.resolve_source("new").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_snapshot_exclusion_proof_survives_retirement_but_not_writes_or_checkout() {
    let fixture = Fixture::lazy(true).await;
    let controller = fixture.open(None).await;
    let ignored = fixture.local("ignored", b"excluded bytes").await;
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (receipt, _) = mutation
        .snapshot("exclude-for-capture", &options())
        .await
        .unwrap();
    mutation
        .finish(&receipt.after, &operation(), Some(&receipt))
        .await
        .unwrap();
    drop(mutation);
    let assert_ready = |controller: &Controller, expected| {
        let snapshot = fixture
            .tree
            .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
            .unwrap();
        let ledger = controller.inner.ledger.lock().unwrap();
        let ack = ledger.acknowledgement.as_ref().unwrap();
        assert!(
            ack.retire.is_empty(),
            "receipt retirement retains exclusion provenance"
        );
        assert_eq!(
            canonical_capture_ready(&snapshot, ack.snapshot_source_version.as_ref()),
            expected
        );
    };
    assert_ready(&controller, true);
    {
        let snapshot = fixture
            .tree
            .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
            .unwrap();
        let ledger = controller.inner.ledger.lock().unwrap();
        let mut old_record =
            serde_json::to_value(ledger.acknowledgement.as_ref().unwrap()).unwrap();
        old_record
            .as_object_mut()
            .unwrap()
            .remove("snapshot_source_version");
        let old: Acknowledgement = serde_json::from_value(old_record).unwrap();
        assert!(
            !canonical_capture_ready(&snapshot, old.snapshot_source_version.as_ref()),
            "old acknowledgements do not authorize excluded data"
        );
    }
    drop(controller);
    let controller = fixture.open(None).await;
    assert_ready(&controller, true);
    controller
        .acknowledge(&receipt.after, &operation(), None)
        .await
        .unwrap();
    assert_ready(&controller, true);
    fixture
        .tree
        .write(ignored, 0, b"new ignored bytes")
        .await
        .unwrap();
    assert_ready(&controller, false);
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (refreshed, _) = mutation
        .snapshot("refresh-exclusion", &options())
        .await
        .unwrap();
    mutation
        .finish(&refreshed.after, &operation(), Some(&refreshed))
        .await
        .unwrap();
    drop(mutation);
    assert_ready(&controller, true);
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (checkout, _) = mutation
        .check_out("checkout-excluded", &refreshed.after.tree)
        .await
        .unwrap();
    mutation
        .finish(&checkout.after, &operation(), Some(&checkout))
        .await
        .unwrap();
    drop(mutation);
    assert_ready(&controller, false);
    assert_eq!(
        fixture.tree.read(ignored, 0, 100).await.unwrap(),
        b"new ignored bytes"
    );

    // Reset also produces Acknowledge receipts, but is not a policy snapshot.
    let fixture = Fixture::lazy(false).await;
    let controller = fixture.open(None).await;
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (snapshot, _) = mutation
        .snapshot("clean-snapshot", &options())
        .await
        .unwrap();
    mutation
        .finish(&snapshot.after, &operation(), Some(&snapshot))
        .await
        .unwrap();
    drop(mutation);
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let reset = mutation
        .reset("clean-reset", &snapshot.after.tree, false)
        .await
        .unwrap();
    mutation
        .finish(&reset.after, &operation(), Some(&reset))
        .await
        .unwrap();
    drop(mutation);
    assert!(
        controller
            .inner
            .ledger
            .lock()
            .unwrap()
            .acknowledgement
            .as_ref()
            .unwrap()
            .snapshot_source_version
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_file_rename_rewrites_copy_lineage_and_directory_rename_rejects_before_intent() {
    let fixture = Fixture::lazy(false).await;
    let controller = fixture.open(None).await;
    fixture
        .tree
        .rename(
            fixture.tree.root_id(),
            "tracked",
            fixture.tree.root_id(),
            "renamed",
        )
        .await
        .unwrap();
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (receipt, _) = mutation.snapshot("lazy-rename", &options()).await.unwrap();
    let actual = files(
        fixture.provider.as_ref(),
        m::TreeId(receipt.after.tree.terms[0]),
    )
    .await;
    assert!(!actual.contains_key("tracked"));
    let m::TreeValue::File {
        copy_id: Some(copy_id),
        ..
    } = actual["renamed"]
    else {
        panic!()
    };
    let copy = fixture
        .provider
        .read_metadata(m::ObjectKey {
            kind: m::Kind::CopyHistory,
            id: m::ObjectId(copy_id.0),
        })
        .await
        .unwrap()
        .into_object();
    let m::Object::CopyHistory(copy) = copy else {
        panic!()
    };
    assert_eq!(copy.current_path, "renamed");
    assert_eq!(copy.parents.len(), 1);
    mutation
        .finish(&receipt.after, &operation(), Some(&receipt))
        .await
        .unwrap();
    drop(mutation);
    fixture
        .tree
        .rename(
            fixture.tree.root_id(),
            "unvisited",
            fixture.tree.root_id(),
            "moved-directory",
        )
        .await
        .unwrap();
    let before = controller.inspect().unwrap();
    let mut mutation = controller.begin(&before).await.unwrap();
    let error = mutation
        .snapshot("unsupported-directory", &options())
        .await
        .unwrap_err();
    assert!(
        matches!(&error,ProjectionFailure::Rejected(error) if error.downcast_ref::<LazyMutationUnsupported>()==Some(&LazyMutationUnsupported::DirectoryRename))
    );
    assert_eq!(controller.inspect().unwrap(), before);
    assert!(matches!(
        controller.receipt("unsupported-directory").await.unwrap(),
        ProjectionOutcome::Unknown
    ));
    assert!(fixture.tree.resolve_source("moved-directory").await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_checkout_counts_changed_leaves_and_reset_rejects_unsafe_rebaseline_without_intent() {
    let fixture = Fixture::lazy(false).await;
    let controller = fixture.open(None).await;
    let base_files = files(fixture.provider.as_ref(), fixture.base).await;
    let mut entries: Vec<_> = base_files
        .into_iter()
        .map(|(name, value)| m::TreeEntry { name, value })
        .collect();
    let file = fixture
        .provider
        .write_file(&mut Cursor::new(b"next"), 4)
        .await
        .unwrap();
    if let m::TreeValue::File { id, .. } = &mut entries
        .iter_mut()
        .find(|entry| entry.name == "untouched")
        .unwrap()
        .value
    {
        *id = file;
    }
    let target = fixture
        .provider
        .write_metadata(m::Object::Tree(m::Tree { entries }))
        .await
        .unwrap();
    let native = NativeTree {
        terms: vec![target.key.id.0],
        labels: Vec::new(),
    };
    let before = controller.inspect().unwrap();
    let mut mutation = controller.begin(&before).await.unwrap();
    let error = mutation
        .reset("unsafe-reset", &native, false)
        .await
        .unwrap_err();
    assert!(
        matches!(&error,ProjectionFailure::Rejected(error) if error.downcast_ref::<LazyMutationUnsupported>()==Some(&LazyMutationUnsupported::ResetWithDifferentContents))
    );
    assert_eq!(controller.inspect().unwrap(), before);
    assert!(matches!(
        controller.receipt("unsafe-reset").await.unwrap(),
        ProjectionOutcome::Unknown
    ));
    let local = fixture.local("untracked", b"keep local").await;
    let parent = fixture.tree.resolve_source("unvisited").await.unwrap();
    let nested = fixture
        .tree
        .create(parent.id, "local", 0o644)
        .await
        .unwrap();
    fixture
        .tree
        .write(nested.id, 0, b"nested local")
        .await
        .unwrap();
    let (receipt, stats) = mutation.check_out("lazy-checkout", &native).await.unwrap();
    assert_eq!(
        fixture.tree.resolve_source("untracked").await.unwrap().id,
        local
    );
    assert_eq!(
        fixture.tree.read(local, 0, 100).await.unwrap(),
        b"keep local"
    );
    assert_eq!(
        fixture
            .tree
            .resolve_source("unvisited/local")
            .await
            .unwrap()
            .id,
        nested.id
    );
    assert_eq!(
        fixture.tree.read(nested.id, 0, 100).await.unwrap(),
        b"nested local"
    );
    assert!(fixture.tree.resolve_source("unvisited/deep").await.is_ok());
    assert_eq!(stats.updated_files, 1);
    assert_eq!(stats.added_files, 0);
    assert_eq!(stats.removed_files, 0);
    assert_eq!(receipt.after.tree, native);
    assert_eq!(fixture.provider.reads.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_checkout_rejects_local_path_and_ancestor_collisions_without_loss() {
    for local_directory in [false, true] {
        let fixture = Fixture::lazy(false).await;
        let controller = fixture.open(None).await;
        let local = if local_directory {
            let directory = fixture
                .tree
                .mkdir(fixture.tree.root_id(), "collision", 0o755)
                .await
                .unwrap();
            let node = fixture
                .tree
                .create(directory.id, "note", 0o644)
                .await
                .unwrap();
            fixture.tree.write(node.id, 0, b"keep local").await.unwrap();
            node.id
        } else {
            fixture.local("collision", b"keep local").await
        };
        let value = files(fixture.provider.as_ref(), fixture.base).await["tracked"].clone();
        let target = fixture
            .provider
            .write_metadata(m::Object::Tree(m::Tree {
                entries: vec![m::TreeEntry {
                    name: "collision".into(),
                    value,
                }],
            }))
            .await
            .unwrap();
        let native = NativeTree {
            terms: vec![target.key.id.0],
            labels: Vec::new(),
        };
        let before = controller.inspect().unwrap();
        let mut mutation = controller.begin(&before).await.unwrap();
        let error = mutation
            .check_out("local-collision", &native)
            .await
            .unwrap_err();
        assert!(matches!(error, ProjectionFailure::Rejected(_)));
        assert_eq!(controller.inspect().unwrap(), before);
        assert!(matches!(
            controller.receipt("local-collision").await.unwrap(),
            ProjectionOutcome::Unknown
        ));
        let path = if local_directory {
            "collision/note"
        } else {
            "collision"
        };
        assert_eq!(fixture.tree.resolve_source(path).await.unwrap().id, local);
        assert_eq!(
            fixture.tree.read(local, 0, 100).await.unwrap(),
            b"keep local"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_recreated_tracked_file_keeps_policy_and_original_copy_lineage() {
    let fixture = Fixture::lazy(true).await;
    let controller = fixture.open(None).await;
    let original = files(fixture.provider.as_ref(), fixture.base).await["tracked"].clone();
    fixture
        .tree
        .unlink_file(fixture.tree.root_id(), "tracked")
        .await
        .unwrap();
    let recreated = fixture.local("tracked", b"replacement").await;
    let mut policy = options();
    policy.max_new_file_size = 0;
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (receipt, stats) = mutation
        .snapshot("tracked-recreate", &policy)
        .await
        .unwrap();
    assert!(
        stats.untracked_paths.is_empty(),
        "recreated tracked paths are not new-file policy candidates"
    );
    let actual = files(
        fixture.provider.as_ref(),
        m::TreeId(receipt.after.tree.terms[0]),
    )
    .await;
    let m::TreeValue::File {
        copy_id: previous, ..
    } = original
    else {
        panic!()
    };
    let m::TreeValue::File {
        copy_id: current, ..
    } = actual["tracked"]
    else {
        panic!()
    };
    assert_eq!(current, previous);
    assert_eq!(
        fixture.tree.resolve_source("tracked").await.unwrap().id,
        recreated
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_excluded_nested_local_directory_can_be_tracked_on_a_later_snapshot() {
    let fixture = Fixture::lazy(false).await;
    let controller = fixture.open(None).await;
    let directory = fixture
        .tree
        .mkdir(fixture.tree.root_id(), "work", 0o755)
        .await
        .unwrap();
    let node = fixture
        .tree
        .create(directory.id, "note", 0o644)
        .await
        .unwrap();
    fixture.tree.write(node.id, 0, b"local note").await.unwrap();
    let mut policy = options();
    policy.max_new_file_size = 0;
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (first, _) = mutation.snapshot("exclude-nested", &policy).await.unwrap();
    assert_eq!(first.after.tree.terms, vec![fixture.base.0]);
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 0);
    mutation
        .finish(&first.after, &operation(), Some(&first))
        .await
        .unwrap();
    drop(mutation);
    let mut mutation = controller
        .begin(&controller.inspect().unwrap())
        .await
        .unwrap();
    let (second, _) = mutation.snapshot("track-nested", &options()).await.unwrap();
    let root = files(
        fixture.provider.as_ref(),
        m::TreeId(second.after.tree.terms[0]),
    )
    .await;
    let m::TreeValue::Tree(work) = root["work"] else {
        panic!()
    };
    let leaves = files(fixture.provider.as_ref(), work).await;
    assert!(leaves.contains_key("note"));
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.tree.resolve_source("work/note").await.unwrap().id,
        node.id
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_capture_protocol_state_cannot_attest_native_frontend_quiescence() {
    let fixture = Fixture::new(true).await;
    let controller = fixture.open(None).await;
    let before = controller.inspect().unwrap();
    let reads = fixture.provider.reads.load(Ordering::SeqCst);
    let uploads = fixture.provider.uploads.load(Ordering::SeqCst);
    let error = controller.capture_for_backup(&before).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::Unsupported
    );
    assert_eq!(controller.inspect().unwrap(), before);
    assert_eq!(fixture.provider.reads.load(Ordering::SeqCst), reads);
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), uploads);
    // Rejection must release exclusive controller admission. This is a protocol
    // fixture, not evidence of a native FUSE capture or a portable capsule.
    let session = controller.begin_session(&before).await.unwrap();
    drop(session);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_capture_cancelled_or_busy_admission_does_not_leak_the_controller_session() {
    let fixture = Fixture::new(true).await;
    let mut config = Config::new("fixture".into(), fixture.working_copy.clone());
    config.admission_timeout = Duration::from_millis(100);
    let controller = fixture.open(Some(config)).await;
    let before = controller.inspect().unwrap();
    let session = controller.begin_session(&before).await.unwrap();
    let error = controller
        .capture_for_backup_until(
            &before,
            tokio::time::Instant::now() + Duration::from_millis(20),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("capture admission deadline elapsed")
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            controller.capture_for_backup(&before)
        )
        .await
        .is_err(),
        "cancel while still waiting for admission"
    );
    let error = controller.capture_for_backup(&before).await.unwrap_err();
    assert!(error.to_string().contains("admission timed out"));
    drop(session);
    let error = controller.capture_for_backup(&before).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::Unsupported
    );
    let next = controller.begin_session(&before).await.unwrap();
    drop(next);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_capture_rejects_stale_unhealthy_and_pending_state_before_frontend_capture() {
    let fixture = Fixture::lazy(true).await;
    let controller = fixture.open(None).await;
    let before = controller.inspect().unwrap();
    let mut stale = before.clone();
    stale.generation += 1;
    assert!(
        controller
            .capture_for_backup(&stale)
            .await
            .unwrap_err()
            .to_string()
            .contains("baseline is stale")
    );
    controller.inner.poisoned.store(true, Ordering::Release);
    assert!(
        controller
            .capture_for_backup(&before)
            .await
            .unwrap_err()
            .to_string()
            .contains("durable state is uncertain")
    );
    // Explicit protocol corruption/recovery-boundary fixture only. Production
    // never clears poison; it reopens the controller to reconcile durable state.
    controller.inner.poisoned.store(false, Ordering::Release);
    let frozen = fixture
        .tree
        .freeze_lazy_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    let expected = frozen.version().clone();
    drop(frozen);
    // Reuse the durable intent boundary exercised by recovery tests. No frontend
    // capture guard or successful portable proof is constructed by this fixture.
    controller
        .prepare(
            "backup-pending",
            &before,
            Action::Snapshot { policy: [1; 32] },
            expected,
        )
        .unwrap();
    assert!(
        controller
            .capture_for_backup(&before)
            .await
            .unwrap_err()
            .to_string()
            .contains("requires recovery before backup capture")
    );
    assert!(controller.requires_reconciliation());
}

#[cfg(target_os = "linux")]
/// Actual Controller -> NativeMount -> FUSE capture acceptance. No protocol
/// fixture, serialized attestation or manually supplied frontend fence is used.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires usable /dev/fuse and fusermount3"]
async fn controller_backup_capture_owns_the_actual_frontend_pause_and_resumes_on_drop() {
    use std::{sync::Arc, time::Duration};
    let fixture = Fixture::new(false).await;
    let provider = fixture.provider.clone();
    let config = native_vfs::Config {
        root: fixture._directory.path().join("native-root"),
        state: fixture._directory.path().join("native-state"),
        workspace_identity: [88; 32],
        scope: fixture.scope.clone(),
        root_merge: m::TreeMerge {
            terms: vec![fixture.base],
            labels: vec![String::new()],
        },
        compiler_limits: Limits::default(),
        source_limits: SourceLimits::default(),
    };
    let cache = fixture._cache.clone();
    let mount = Arc::new(
        native_vfs::start(config.clone(), provider.clone(), cache)
            .await
            .unwrap(),
    );
    let working_copy = private_fs::PrivateDir::create(config.state.join("working-copy")).unwrap();
    let mut controller_config = Config::new(
        "native-mount-fixture".into(),
        working_copy.path().to_owned(),
    );
    controller_config.source_limits = config.source_limits.clone();
    let controller = Controller::open(mount.clone(), provider.clone(), controller_config)
        .await
        .unwrap();
    let expected = controller.inspect().unwrap();
    assert_eq!(
        expected.generation,
        mount.tree().source_baseline().unwrap().1
    );
    assert_eq!(provider.reads.load(Ordering::SeqCst), 0);
    let actual_root = config.root.canonicalize().unwrap();
    let actual_working_copy = working_copy.path().to_owned();
    let capture = controller
        .capture_for_backup_until(
            &expected,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(capture.projection_state(), &expected);
    assert_eq!(capture.binding().root(), actual_root);
    assert_eq!(
        capture.binding().workspace_identity(),
        config.workspace_identity
    );
    assert_eq!(capture.binding().working_copy_state(), actual_working_copy);
    assert_eq!(
        capture.binding().backend_identity(),
        mount.backend_identity()
    );
    assert_eq!(capture.binding().repository(), expected.repository);
    // A second actual mount deliberately shares the tree/generation/repository.
    // Its different native workspace and WC-state identities must stay visible;
    // neither a matching projection nor capture of A can attest to B.
    let mut other_config = config.clone();
    other_config.root = fixture._directory.path().join("other-native-root");
    other_config.state = fixture._directory.path().join("other-native-state");
    other_config.workspace_identity = [89; 32];
    let other_mount = Arc::new(
        native_vfs::start(
            other_config.clone(),
            provider.clone(),
            fixture._cache.clone(),
        )
        .await
        .unwrap(),
    );
    let other_working_copy = PrivateDir::create(other_config.state.join("working-copy")).unwrap();
    let other = Controller::open(
        other_mount.clone(),
        provider.clone(),
        Config::new(
            expected.repository.clone(),
            other_working_copy.path().to_owned(),
        ),
    )
    .await
    .unwrap();
    let other_expected = other.inspect().unwrap();
    assert_eq!(other_expected, expected);
    // Pausing A must not globally stop B's independent native source writers.
    fs::write(other_config.root.join("other-local"), b"independent mount").unwrap();
    let other_capture = other.capture_for_backup(&other_expected).await.unwrap();
    assert_eq!(other_capture.projection_state(), capture.projection_state());
    assert_ne!(other_capture.binding().root(), capture.binding().root());
    assert_ne!(
        other_capture.binding().workspace_identity(),
        capture.binding().workspace_identity()
    );
    assert_ne!(
        other_capture.binding().working_copy_state(),
        capture.binding().working_copy_state()
    );
    assert_eq!(
        other_capture.binding().backend_identity(),
        capture.binding().backend_identity()
    );
    assert_eq!(
        other_capture.binding().workspace_identity(),
        other_config.workspace_identity
    );
    drop(other_capture);
    drop(other);
    other_mount.shutdown().await.unwrap();

    assert_eq!(
        capture.snapshot().baseline_tree(),
        config.root_merge.terms[0].0
    );
    assert_eq!(capture.snapshot().version().generation, expected.generation);
    assert_eq!(
        provider.reads.load(Ordering::SeqCst),
        0,
        "fresh capture without a prior ACK must not hydrate source FILEs"
    );
    assert_eq!(
        fs::File::open(config.root.join("tracked"))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock,
        "an actual held capture rejects new kernel opens"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            controller.capture_for_backup(&expected)
        )
        .await
        .is_err(),
        "controller admission remains owned for the complete capture lifetime"
    );
    drop(capture);
    drop(fs::File::open(config.root.join("tracked")).unwrap());
    assert_eq!(
        provider.reads.load(Ordering::SeqCst),
        0,
        "reopening alone does not read payloads"
    );
    fs::write(config.root.join("local"), b"captured local bytes").unwrap();
    let dirty = controller.capture_for_backup(&expected).await.unwrap();
    let local = dirty
        .snapshot()
        .entries()
        .iter()
        .find(|entry| entry.path == "local")
        .unwrap();
    assert_eq!(
        dirty.snapshot().read_local(local.node.id, 0, 64).unwrap(),
        b"captured local bytes"
    );
    assert_eq!(provider.reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(config.root.join("blocked"))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    drop(dirty);
    let mut stale = expected.clone();
    stale.generation += 1;
    assert!(
        controller
            .capture_for_backup(&stale)
            .await
            .unwrap_err()
            .to_string()
            .contains("baseline is stale")
    );
    fs::write(config.root.join("after"), b"writer admission resumed").unwrap();
    // The cancelled waiter did not steal admission or leave the frontend paused.
    let final_capture = controller.capture_for_backup(&expected).await.unwrap();
    assert!(
        final_capture
            .snapshot()
            .entries()
            .iter()
            .any(|entry| entry.path == "after")
    );
    drop(final_capture);
    assert_eq!(
        controller.inspect().unwrap(),
        expected,
        "backup capture does not ACK or rebaseline dirty state"
    );
    let mut policy = options();
    policy.max_new_file_size = 0;
    let mut mutation = controller.begin(&expected).await.unwrap();
    let (receipt, _) = mutation
        .snapshot("excluded-native-capture", &policy)
        .await
        .unwrap();
    mutation
        .finish(&receipt.after, &operation(), Some(&receipt))
        .await
        .unwrap();
    drop(mutation);
    let capture = controller.capture_for_backup(&receipt.after).await.unwrap();
    assert!(capture.canonical_capture_ready());
    assert!(!capture.snapshot().entries().is_empty());
    drop(capture);
    fs::write(config.root.join("local"), b"new unsnapshotted bytes").unwrap();
    let capture = controller.capture_for_backup(&receipt.after).await.unwrap();
    assert!(
        !capture.canonical_capture_ready(),
        "writes invalidate the exact snapshot ACK"
    );
    drop(capture);
    mount.shutdown().await.unwrap();
    assert!(
        controller.capture_for_backup(&receipt.after).await.is_err(),
        "an actual stopped mount cannot produce another sealed capture"
    );
}
