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
}
#[async_trait]
impl NativeObjectIo for CountingIo {
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        self.direct.descriptor(key).await
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.direct.read_metadata(key).await
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
        let compiled = compiler.compile_tree(base).await.unwrap();
        tree.install_source(
            compiled.manifest(tree.identity()),
            None,
            "bootstrap",
            &SourceLimits::default(),
        )
        .unwrap();
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
