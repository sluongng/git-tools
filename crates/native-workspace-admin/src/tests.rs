use super::*;
use async_trait::async_trait;
use jj_lib::{
    backend::{Backend, BackendInitError},
    config::{ConfigLayer, ConfigSource, StackedConfig},
    op_store::OperationId,
    ref_name::WorkspaceName,
};
use jj_native_backend::{Config as BackendConfig, NativeBackend, NativeObjectIo};
use jj_vfs::native::{
    NativeProjectionController, NativeProjectionMutation, NativeTree, ProjectionOutcome,
    ProjectionReceipt, ProjectionState,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Connector {
    ready: AtomicBool,
    calls: AtomicUsize,
    projected: ProjectionState,
}
impl ProjectionConnector for Connector {
    fn connect(&self, binding: &ProjectionBinding) -> Result<Arc<dyn NativeProjectionController>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ensure!(
            self.ready.load(Ordering::SeqCst),
            "fixture provider is not mounted"
        );
        ensure!(
            binding.repository == self.projected.repository,
            "fixture scope mismatch"
        );
        Ok(Arc::new(Controller(self.projected.clone())))
    }
}
struct Controller(ProjectionState);
#[async_trait]
impl NativeProjectionController for Controller {
    fn inspect(&self) -> Result<ProjectionState> {
        Ok(self.0.clone())
    }
    async fn begin(&self, expected: &ProjectionState) -> Result<Box<dyn NativeProjectionMutation>> {
        ensure!(expected == &self.0, "fixture mutation baseline changed");
        Ok(Box::new(FinishOnlyMutation(self.0.clone())))
    }
    async fn receipt(&self, _: &str) -> Result<ProjectionOutcome> {
        Ok(ProjectionOutcome::Unknown)
    }
    async fn acknowledge(
        &self,
        _: &ProjectionState,
        _: &OperationId,
        _: Option<&ProjectionReceipt>,
    ) -> Result<()> {
        Ok(())
    }
}

// The registry tests advance only OP acknowledgement with an unchanged native
// tree. Any attempted projection or FILE work remains a fixture failure.
struct FinishOnlyMutation(ProjectionState);
#[async_trait]
impl NativeProjectionMutation for FinishOnlyMutation {
    async fn snapshot(
        &mut self,
        _: &str,
        _: &jj_lib::working_copy::SnapshotOptions<'_>,
    ) -> std::result::Result<
        (ProjectionReceipt, jj_lib::working_copy::SnapshotStats),
        jj_vfs::native::ProjectionFailure,
    > {
        panic!("registry journal must not snapshot user files")
    }
    async fn check_out(
        &mut self,
        _: &str,
        _: &NativeTree,
    ) -> std::result::Result<
        (ProjectionReceipt, jj_lib::working_copy::CheckoutStats),
        jj_vfs::native::ProjectionFailure,
    > {
        panic!("registry journal must not change the projection")
    }
    async fn reset(
        &mut self,
        _: &str,
        _: &NativeTree,
        _: bool,
    ) -> std::result::Result<ProjectionReceipt, jj_vfs::native::ProjectionFailure> {
        panic!("registry journal must not reset the projection")
    }
    async fn finish(
        &mut self,
        state: &ProjectionState,
        _: &OperationId,
        receipt: Option<&ProjectionReceipt>,
    ) -> Result<()> {
        ensure!(
            state == &self.0 && receipt.is_none(),
            "fixture requires unchanged acknowledged projection"
        );
        Ok(())
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    registry: Arc<Registry>,
    binding: Binding,
    connector: Arc<Connector>,
    settings: UserSettings,
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let registry = Registry::create(base.join("registry")).unwrap();
        let root = base.join("projection");
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join(".jj")).unwrap();
        let id = [7; 32];
        fs::write(root.join(MARKER), hex(&id)).unwrap();
        let binding = registry
            .binding(
                id,
                &root,
                "fixture".into(),
                LocalEndpoint::UnixSocket(base.join("rbe.sock")),
                "fixture-mount".into(),
            )
            .unwrap();
        let seed = base.join("identity-fixture");
        fs::create_dir(&seed).unwrap();
        let backend = NativeBackend::init(&seed, BackendConfig::default()).unwrap();
        let tree = NativeTree {
            terms: vec![backend.empty_tree_id().as_bytes().try_into().unwrap()],
            labels: vec![],
        };
        drop(backend);
        let connector = Arc::new(Connector {
            ready: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
            projected: ProjectionState {
                repository: "fixture".into(),
                generation: 1,
                tree,
            },
        });
        let mut config = StackedConfig::with_defaults();
        config.add_layer(ConfigLayer::parse(ConfigSource::User,
            "[user]\nname='Native Fixture'\nemail='fixture@example.invalid'\n[operation]\nhostname='test'\nusername='test'\n").unwrap());
        let settings = UserSettings::from_config_and_home_dir(config, None).unwrap();
        Self {
            _directory: directory,
            registry,
            binding,
            connector,
            settings,
        }
    }
    async fn initialize(&self) -> Result<(Workspace, Arc<ReadonlyRepo>)> {
        let initializer = |_: &UserSettings,
                           path: &Path|
         -> std::result::Result<Box<dyn Backend>, BackendInitError> {
            Ok(Box::new(NativeBackend::init(
                path,
                BackendConfig::default(),
            )?))
        };
        initialize(
            &self.registry,
            &self.binding,
            &self.settings,
            &initializer,
            Signer::new(None, vec![]),
            self.connector.clone(),
            &CommitId::new(vec![0; 32]),
            WorkspaceName::DEFAULT.to_owned(),
        )
        .await
    }
}

#[test]
fn native_initialization_keeps_all_jj_administration_external_and_loader_is_native_only() {
    futures::executor::block_on(async {
        let fixture = Fixture::new();
        fixture.registry.provision(&fixture.binding).unwrap();
        let pending = std::collections::BTreeSet::from([fixture.binding.workspace_id]);
        let initializer = fixture
            .registry
            .initialization_lock(&fixture.binding)
            .unwrap();
        assert!(
            fixture
                .registry
                .inspect_enrollments(&fixture.binding.endpoint, &pending, 128, 1024 * 1024, 8)
                .is_err(),
            "initializer before journal publication must be fenced"
        );
        drop(initializer);
        let census = fixture
            .registry
            .inspect_enrollments(&fixture.binding.endpoint, &pending, 128, 1024 * 1024, 8)
            .unwrap();
        assert_eq!(census.lock_count(), 2);
        assert!(census.bindings.is_empty());
        drop(census);
        assert!(!fixture.binding.admin.join(JOURNAL).exists());
        let captured_root = std::cell::RefCell::new(None);
        let backend_initializer =
            |_: &UserSettings,
             path: &Path|
             -> std::result::Result<Box<dyn Backend>, BackendInitError> {
                Ok(Box::new(NativeBackend::init(
                    path,
                    BackendConfig::default(),
                )?))
            };
        let index_initializer = |settings: &UserSettings, path: &Path, root: &OperationId| {
            assert!(
                captured_root.borrow().is_none(),
                "initializer is called once"
            );
            assert!(
                fixture.binding.repo.join("op_store/type").is_file(),
                "actual op store exists before the index hook"
            );
            *captured_root.borrow_mut() = Some(root.clone());
            ReadonlyRepo::default_index_store_initializer()(settings, path)
        };
        let (workspace, repo) = initialize_with_index(
            &fixture.registry,
            &fixture.binding,
            &fixture.settings,
            &backend_initializer,
            Signer::new(None, vec![]),
            fixture.connector.clone(),
            &CommitId::new(vec![0; 32]),
            WorkspaceName::DEFAULT.to_owned(),
            &index_initializer,
        )
        .await
        .unwrap();
        let before = fs::read(fixture.binding.admin.join(JOURNAL)).unwrap();
        let calls = fixture.connector.calls.load(Ordering::SeqCst);
        {
            let census = fixture
                .registry
                .inspect_enrollments(
                    &fixture.binding.endpoint,
                    &Default::default(),
                    128,
                    1024 * 1024,
                    8,
                )
                .unwrap();
            assert_eq!(census.bindings, vec![fixture.binding.clone()]);
            assert!(
                fixture
                    .registry
                    .inspect_enrollments(
                        &fixture.binding.endpoint,
                        &Default::default(),
                        128,
                        1024 * 1024,
                        8
                    )
                    .is_err(),
                "census fences concurrent initialization"
            );
        }
        assert_eq!(
            fixture.connector.calls.load(Ordering::SeqCst),
            calls,
            "census must not query a projection"
        );
        assert_eq!(
            fs::read(fixture.binding.admin.join(JOURNAL)).unwrap(),
            before
        );
        assert!(
            fixture
                .registry
                .inspect_enrollments(&fixture.binding.endpoint, &Default::default(), 128, 1, 8)
                .is_err()
        );
        let admin = PrivateDir::open_existing(&fixture.binding.admin).unwrap();
        let records = PrivateDir::open_existing(fixture.registry.path().join("records")).unwrap();
        let row_name = record_name(&fixture.binding.workspace_id);
        let row_before = records.read(&row_name, MAX_RECORD).unwrap();
        let mut foreign = fixture.binding.clone();
        foreign.endpoint = Endpoint::UnixSocket {
            path: fixture._directory.path().join("foreign.sock"),
        };
        let mut journal: Initialization = serde_json::from_slice(&before).unwrap();
        journal.binding = foreign.clone();
        admin
            .replace(JOURNAL, &serde_json::to_vec(&journal).unwrap())
            .unwrap();
        assert!(
            fixture
                .registry
                .inspect_enrollments(
                    &fixture.binding.endpoint,
                    &Default::default(),
                    128,
                    1024 * 1024,
                    8
                )
                .is_err(),
            "same-ID ready record and foreign journal must conflict before filtering"
        );
        records
            .replace(&row_name, &serde_json::to_vec(&foreign).unwrap())
            .unwrap();
        let census = fixture
            .registry
            .inspect_enrollments(
                &fixture.binding.endpoint,
                &Default::default(),
                128,
                1024 * 1024,
                8,
            )
            .unwrap();
        assert!(
            census.bindings.is_empty(),
            "coherent foreign enrollment is excluded"
        );
        drop(census);
        admin.replace(JOURNAL, &before).unwrap();
        assert!(
            fixture
                .registry
                .inspect_enrollments(
                    &fixture.binding.endpoint,
                    &Default::default(),
                    128,
                    1024 * 1024,
                    8
                )
                .is_err(),
            "foreign ready record and our journal must also conflict"
        );
        records.replace(&row_name, &row_before).unwrap();
        assert_eq!(
            captured_root.borrow().as_ref(),
            Some(repo.op_store().root_operation_id())
        );
        assert_eq!(workspace.workspace_root(), fixture.binding.root);
        assert_eq!(workspace.repo_path(), fixture.binding.repo);
        assert_eq!(workspace.working_copy().name(), jj_vfs::native::NAME);
        assert_eq!(repo.store().backend().name(), NativeBackend::NAME);
        assert!(fixture.binding.repo.join("op_store").is_dir());
        assert!(fixture.binding.repo.join("index").is_dir());
        assert!(
            fixture
                .binding
                .working_copy
                .join("native-vfs.json")
                .is_file()
        );
        assert!(!fixture.binding.root.join(".jj/repo").exists());
        assert!(!fixture.binding.root.join(".jj/working_copy").exists());
        assert!(!fixture.binding.working_copy.join("checkout").exists());
        assert_eq!(
            fs::read(fixture.binding.root.join(MARKER)).unwrap(),
            hex(&fixture.binding.workspace_id).as_bytes()
        );
        assert_eq!(
            fixture.registry.lookup(&fixture.binding.root).unwrap(),
            fixture.binding
        );
        let original_op = workspace.working_copy().operation_id().clone();
        drop(workspace);
        drop(repo);
        let connector = fixture.connector.clone();
        let provider: Arc<dyn ConnectorProvider> =
            Arc::new(move |_: &Binding| -> Result<Arc<dyn ProjectionConnector>> {
                Ok(connector.clone())
            });
        let factory = LoaderFactory::new(fixture.registry.clone(), provider);
        let dynamic = factory.working_copy_factory();
        let loader = factory.create(&fixture.binding.root).unwrap();
        assert_eq!(
            loader.get_working_copy_type().unwrap(),
            jj_vfs::native::NAME
        );
        let mut stores = jj_lib::default_backend_factories::default_backend_factories();
        jj_native_backend::register(&mut stores, BackendConfig::default());
        let loaded = loader
            .load(&fixture.settings, &stores, &WorkingCopyFactories::new())
            .unwrap();
        assert_eq!(loaded.working_copy().name(), jj_vfs::native::NAME);
        assert_eq!(loaded.working_copy().operation_id(), &original_op);
        let store = loaded.repo_loader().store().clone();
        let dynamic_loaded = dynamic
            .load_working_copy(
                store.clone(),
                fixture.binding.root.clone(),
                fixture.binding.working_copy.clone(),
                &fixture.settings,
            )
            .unwrap();
        assert_eq!(dynamic_loaded.name(), jj_vfs::native::NAME);
        assert!(
            dynamic
                .init_working_copy(
                    store,
                    fixture.binding.root.clone(),
                    fixture.binding.working_copy.clone(),
                    original_op,
                    WorkspaceName::DEFAULT.to_owned(),
                    &fixture.settings
                )
                .is_err()
        );
        drop(dynamic_loaded);
        drop(loaded);
        fixture.connector.ready.store(false, Ordering::SeqCst);
        assert!(factory.create(&fixture.binding.root).is_err());
        assert!(!fixture.binding.root.join(".jj/repo").exists());
    });
}

#[test]
fn missing_provider_and_unknown_state_never_create_or_reset_jj_administration() {
    futures::executor::block_on(async {
        let fixture = Fixture::new();
        fixture.connector.ready.store(false, Ordering::SeqCst);
        assert!(fixture.initialize().await.is_err());
        assert!(!fixture.binding.admin.exists());
        assert!(fixture.connector.calls.load(Ordering::SeqCst) > 0);
        fixture.connector.ready.store(true, Ordering::SeqCst);
        let directory = PrivateDir::create(&fixture.binding.admin).unwrap();
        directory
            .replace("unknown", b"preserve private state")
            .unwrap();
        assert!(fixture.initialize().await.is_err());
        assert_eq!(
            directory.read("unknown", 64).unwrap(),
            b"preserve private state"
        );
        assert!(!fixture.binding.repo.exists());
        assert!(!fixture.binding.admin.join(JOURNAL).exists());
        assert!(fixture.registry.lookup(&fixture.binding.root).is_err());
    });
}

#[test]
fn interrupted_initialization_is_journaled_and_completed_publication_can_be_recovered_explicitly() {
    futures::executor::block_on(async {
        let failed = Fixture::new();
        let initializer = |_: &UserSettings,
                           _: &Path|
         -> std::result::Result<Box<dyn Backend>, BackendInitError> {
            Err(BackendInitError(
                std::io::Error::other("fixture backend unavailable").into(),
            ))
        };
        assert!(
            initialize(
                &failed.registry,
                &failed.binding,
                &failed.settings,
                &initializer,
                Signer::new(None, vec![]),
                failed.connector.clone(),
                &CommitId::new(vec![0; 32]),
                WorkspaceName::DEFAULT.to_owned()
            )
            .await
            .is_err()
        );
        let journal = fs::read(failed.binding.admin.join(JOURNAL)).unwrap();
        assert!(
            !serde_json::from_slice::<Initialization>(&journal)
                .unwrap()
                .ready
        );
        assert!(failed.binding.repo.is_dir());
        assert!(failed.initialize().await.is_err());
        assert_eq!(
            fs::read(failed.binding.admin.join(JOURNAL)).unwrap(),
            journal
        );
        assert!(failed.registry.publish_ready(&failed.binding).is_err());
        let complete = Fixture::new();
        let (workspace, repo) = complete.initialize().await.unwrap();
        drop(workspace);
        drop(repo);
        fs::remove_file(
            complete
                .registry
                .records
                .path()
                .join(record_name(&complete.binding.workspace_id)),
        )
        .unwrap();
        assert!(complete.registry.lookup(&complete.binding.root).is_err());
        complete.registry.publish_ready(&complete.binding).unwrap();
        assert_eq!(
            complete.registry.lookup(&complete.binding.root).unwrap(),
            complete.binding
        );
        // An exact publication retry changes no operation or working-copy data.
        let before = fs::read(complete.binding.working_copy.join("native-vfs.json")).unwrap();
        complete.registry.publish_ready(&complete.binding).unwrap();
        assert_eq!(
            fs::read(complete.binding.working_copy.join("native-vfs.json")).unwrap(),
            before
        );
    });
}

#[test]
fn opaque_marker_and_registry_binding_cannot_select_another_root_or_external_path() {
    futures::executor::block_on(async {
        let fixture = Fixture::new();
        let (workspace, repo) = fixture.initialize().await.unwrap();
        drop(workspace);
        drop(repo);
        let record = fixture
            .registry
            .records
            .path()
            .join(record_name(&fixture.binding.workspace_id));
        let original = fs::read(&record).unwrap();
        let mut substituted = fixture.binding.clone();
        substituted.repo = fixture.binding.root.join("attacker-selected-repo");
        fs::write(&record, serde_json::to_vec(&substituted).unwrap()).unwrap();
        assert!(fixture.registry.lookup(&fixture.binding.root).is_err());
        fs::write(&record, original).unwrap();
        fs::write(fixture.binding.root.join(MARKER), b"../../private-admin").unwrap();
        assert!(fixture.registry.lookup(&fixture.binding.root).is_err());
        assert!(fixture.binding.repo.is_dir());
        assert!(!substituted.repo.exists());
        let unknown = fixture
            .registry
            .path()
            .parent()
            .unwrap()
            .join("unknown-registry");
        let private = PrivateDir::create(&unknown).unwrap();
        private.replace("keep", b"not a registry").unwrap();
        assert!(Registry::create(&unknown).is_err());
        assert_eq!(private.read("keep", 64).unwrap(), b"not a registry");
        assert!(!unknown.join("registry.json").exists());
    });
}

#[test]
fn provisioning_intent_survives_restart_and_conflicting_requests_never_replace_it() {
    let fixture = Fixture::new();
    let root = &fixture.binding.root;
    let head = CommitId::new(vec![0; 32]);
    let endpoint = fixture.binding.endpoint.local();
    assert!(
        fixture
            .registry
            .pending_provisioning(root)
            .unwrap()
            .is_none()
    );
    let plan = native_broker::CreationPlan {
        provider_identity: [7; 32],
        root: [0; 32],
        index_mode: native_broker::ProviderIndexMode::Federated,
        policy: native_broker::CreationPolicy {
            digest: [8; 32],
            capture_enabled: true,
            upload_enabled: true,
            auto_capture: true,
            auto_upload: true,
        },
    };
    let intent = fixture
        .registry
        .begin_provisioning_with_plan(root, "fixture".into(), endpoint.clone(), &head, plan)
        .unwrap();
    let id = intent.workspace_id();
    assert_ne!(id, [0; 32]);
    assert!(!fixture.binding.admin.exists());
    drop(intent);
    let reopened = Registry::open_existing(fixture.registry.path()).unwrap();
    let pending = reopened.pending_provisioning(root).unwrap().unwrap();
    assert_eq!(pending.workspace_id(), id);
    assert_eq!(pending.initial_commit(), head);
    assert_eq!(pending.repository(), "fixture");
    assert_eq!(pending.endpoint(), endpoint);
    assert_eq!(pending.creation_plan().unwrap(), Some(plan));
    drop(pending);
    let retry = reopened
        .begin_provisioning(root, "fixture".into(), endpoint.clone(), &head)
        .unwrap();
    assert_eq!(retry.workspace_id(), id);
    drop(retry);
    assert!(
        reopened
            .begin_provisioning(root, "different".into(), endpoint.clone(), &head)
            .is_err()
    );
    assert!(
        reopened
            .begin_provisioning(
                root,
                "fixture".into(),
                endpoint.clone(),
                &CommitId::new(vec![3; 32])
            )
            .is_err()
    );
    assert!(
        reopened
            .begin_provisioning(
                root,
                "fixture".into(),
                LocalEndpoint::UnixSocket(root.parent().unwrap().join("other.sock")),
                &head
            )
            .is_err()
    );
    let mut changed = plan;
    changed.index_mode = native_broker::ProviderIndexMode::Legacy;
    assert!(
        reopened
            .begin_provisioning_with_plan(root, "fixture".into(), endpoint.clone(), &head, changed)
            .is_err()
    );
    let mut pending = reopened.pending_provisioning(root).unwrap().unwrap();
    assert_eq!(pending.workspace_id(), id);
    assert!(pending.complete(&fixture.binding).is_err());
    drop(pending);
    assert_eq!(
        reopened
            .pending_provisioning(root)
            .unwrap()
            .unwrap()
            .workspace_id(),
        id
    );
}

#[test]
fn ready_resume_needs_no_marker_but_normal_loader_still_does_and_intent_completion_is_durable() {
    futures::executor::block_on(async {
        let mut fixture = Fixture::new();
        let mut intent = fixture
            .registry
            .begin_provisioning(
                &fixture.binding.root,
                fixture.binding.repository.clone(),
                fixture.binding.endpoint.local(),
                &CommitId::new(vec![0; 32]),
            )
            .unwrap();
        fixture.binding = fixture
            .registry
            .binding(
                intent.workspace_id(),
                &fixture.binding.root,
                fixture.binding.repository.clone(),
                fixture.binding.endpoint.local(),
                fixture.binding.rpc_mount_id.clone(),
            )
            .unwrap();
        fs::write(fixture.binding.root.join(MARKER), fixture.binding.id()).unwrap();
        assert!(fixture.registry.resume_info(&fixture.binding.root).is_err());
        let (workspace, repo) = fixture.initialize().await.unwrap();
        drop(workspace);
        drop(repo);
        fs::remove_file(fixture.binding.root.join(MARKER)).unwrap();
        let resumed = fixture.registry.resume_info(&fixture.binding.root).unwrap();
        assert_eq!(resumed.binding, fixture.binding);
        assert_eq!(resumed.initial_commit, CommitId::new(vec![0; 32]));
        assert!(fixture.registry.lookup(&fixture.binding.root).is_err());
        intent.complete(&fixture.binding).unwrap();
        drop(intent);
        let reopened = Registry::open_existing(fixture.registry.path()).unwrap();
        assert!(
            reopened
                .pending_provisioning(&fixture.binding.root)
                .unwrap()
                .is_none()
        );
        assert!(
            reopened
                .begin_provisioning(
                    &fixture.binding.root,
                    fixture.binding.repository.clone(),
                    fixture.binding.endpoint.local(),
                    &CommitId::new(vec![0; 32])
                )
                .is_err()
        );
    });
}

#[test]
fn concurrent_first_use_registry_creation_preserves_published_provisioning_intent() {
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().canonicalize().unwrap();
    let registry_path = base.join("shared-registry");
    let root = base.join("projection");
    fs::create_dir(&root).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let registry_path = registry_path.clone();
            let root = root.clone();
            let endpoint = base.join("rbe.sock");
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let registry = Registry::create(registry_path).unwrap();
                let intent = registry
                    .begin_provisioning(
                        &root,
                        "fixture".into(),
                        LocalEndpoint::UnixSocket(endpoint),
                        &CommitId::new(vec![0; 32]),
                    )
                    .unwrap();
                intent.workspace_id()
            })
        })
        .collect();
    let ids: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert!(ids.iter().all(|id| *id == ids[0]));
    // Repeated create cannot downgrade an already enabled registry header.
    for _ in 0..8 {
        let registry = Registry::create(&registry_path).unwrap();
        let intent = registry.pending_provisioning(&root).unwrap().unwrap();
        assert_eq!(intent.workspace_id(), ids[0]);
    }
}

#[test]
fn remote_capture_inputs_retain_archived_operation_ancestry_without_a_command() {
    use crate::remote_state::{Kind, Pending, RemoteScope, State};
    let scope = RemoteScope::new(PathBuf::from("/private/broker.sock"), "example".into()).unwrap();
    let mut state = State::new(&scope);
    state.archived_pushes.push(Pending {
        kind: Kind::Push,
        reference: "refs/heads/main".into(),
        bookmark: "main".into(),
        generation: 3,
        head: Some("0b".repeat(32)),
        request_id: "0c".repeat(16),
        base_operation: "0d".repeat(64),
        confirmed_generation: Some(4),
    });
    let encoded = toml::to_string(&state).unwrap();
    let decoded = remote_state::decode(encoded.as_bytes(), &scope).unwrap();
    assert!(decoded.pending.is_none());
    assert_eq!(
        decoded.retained_operations().collect::<Vec<_>>(),
        vec!["0d".repeat(64)]
    );
    assert_eq!(
        decoded.retained_commit_heads().collect::<Vec<_>>(),
        vec!["0b".repeat(32)]
    );
    let foreign =
        RemoteScope::new(PathBuf::from("/private/foreign.sock"), "example".into()).unwrap();
    assert!(remote_state::decode(encoded.as_bytes(), &foreign).is_err());
    assert!(RemoteScope::new(PathBuf::from("/private/../broker.sock"), "example".into()).is_err());
}

// Setup supports the intrinsic root only. Any accidental provider/factory query
// during capture is observable immediately, rather than returning invented data.
#[derive(Debug, Default)]
struct NoCaptureQueries {
    deny_prepare: AtomicBool,
    deny_close: AtomicBool,
    bad_cloud_echo: AtomicBool,
    closes: AtomicUsize,
    deny_cloud: AtomicBool,
    capture: AtomicBool,
    calls: AtomicUsize,
}
impl NoCaptureQueries {
    fn setup_call(&self) {
        assert!(
            !self.capture.load(Ordering::Acquire),
            "provider query must not run during local capture"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl native_broker::NativeQueryIo for NoCaptureQueries {
    fn resources(&self) -> native_broker::QueryResources {
        native_broker::QueryResources {
            memory_bytes: 4096,
            temporary_bytes: 0,
            persistent_memory_bytes: 4096,
        }
    }
    async fn prepare_cloud_authorization(
        &self,
        _: native_broker::CloudAuthorizationSource,
        _: native_broker::QueryOwner,
    ) -> std::result::Result<
        Option<native_broker::QueryReply<Arc<dyn native_broker::NativeQueryIo>>>,
        tonic::Status,
    > {
        self.setup_call();
        if self.deny_prepare.load(Ordering::Acquire) {
            return Err(tonic::Status::permission_denied(
                "fixture authorization preparation denied",
            ));
        }
        Ok(None)
    }
    async fn open_cloud_scope(
        &self,
        input: native_broker::OpenCloudQueryScope,
        _: native_broker::QueryOwner,
    ) -> std::result::Result<native_broker::QueryReply<native_broker::QueryScope>, tonic::Status>
    {
        self.setup_call();
        if self.deny_cloud.load(Ordering::Acquire) {
            return Err(tonic::Status::permission_denied(
                "fixture cloud source denied",
            ));
        }
        assert_eq!(
            input.source.expected_anchors,
            vec![tinytree_model::CommitId::ROOT]
        );
        assert!(input.overlay.is_empty());
        Ok(native_broker::QueryReply {
            value: native_broker::QueryScope {
                handle: input.replay_key,
                expires_at_unix_seconds: i64::MAX,
                query_identity_version: 2,
                cloud_source_digest: (!self.bad_cloud_echo.load(Ordering::Acquire))
                    .then(|| input.source.digest()),
            },
            owner: Arc::new(()),
        })
    }
    async fn open_scope(
        &self,
        _i: native_broker::OpenQueryScope,
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<native_broker::QueryReply<native_broker::QueryScope>, tonic::Status>
    {
        self.setup_call();
        assert_eq!(_i.roots, vec![tinytree_model::CommitId::ROOT]);
        assert!(
            _i.overlay.is_empty(),
            "setup supports only actual intrinsic ROOT membership"
        );
        Ok(native_broker::QueryReply {
            value: native_broker::QueryScope {
                handle: _i.replay_key,
                expires_at_unix_seconds: i64::MAX,
                query_identity_version: _i.query_identity_version,
                cloud_source_digest: None,
            },
            owner: Arc::new(()),
        })
    }
    async fn fork_scope(
        &self,
        _i: native_broker::ForkQueryScope,
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<native_broker::QueryReply<native_broker::QueryScope>, tonic::Status>
    {
        panic!("provider query must not run during local capture: fork_scope")
    }
    async fn facts(
        &self,
        _s: [u8; 32],
        _f: Vec<native_broker::query_protocol::GraphFact>,
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<
        native_broker::QueryReply<native_broker::query_protocol::GraphFacts>,
        tonic::Status,
    > {
        self.setup_call();
        use native_broker::query_protocol as q;
        let answers = _f
            .into_iter()
            .map(|fact| match fact.fact.unwrap() {
                q::graph_fact::Fact::HasId(value) => {
                    let id = value.commit.unwrap().value;
                    assert_eq!(id.len(), 32);
                    q::GraphFactAnswer {
                        answer: Some(q::graph_fact_answer::Answer::HasId(id == [0; 32])),
                    }
                }
                _ => panic!("setup supports only intrinsic ROOT HasId facts"),
            })
            .collect();
        Ok(native_broker::QueryReply {
            value: q::GraphFacts {
                answers,
                scope_digest: vec![1; 32],
                index_epoch: vec![2; 16],
            },
            owner: Arc::new(()),
        })
    }
    async fn resolve_commit_prefix(
        &self,
        _s: [u8; 32],
        _p: String,
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<
        native_broker::QueryReply<native_broker::query_protocol::CommitPrefixResult>,
        tonic::Status,
    > {
        panic!("provider query must not run during local capture: resolve_commit_prefix")
    }
    async fn resolve_change_prefix(
        &self,
        _s: [u8; 32],
        _p: String,
        _h: Vec<tinytree_model::CommitId>,
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<
        native_broker::QueryReply<native_broker::query_protocol::ChangePrefixResult>,
        tonic::Status,
    > {
        panic!("provider query must not run during local capture: resolve_change_prefix")
    }
    async fn shortest_prefix(
        &self,
        _s: [u8; 32],
        _t: native_broker::query_protocol::shortest_prefix_request::Target,
        _h: Vec<tinytree_model::CommitId>,
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<
        native_broker::QueryReply<native_broker::query_protocol::ShortestPrefixResult>,
        tonic::Status,
    > {
        panic!("provider query must not run during local capture: shortest_prefix")
    }
    async fn begin(
        &self,
        _i: native_broker::BeginQuery,
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<native_broker::QueryReply<native_broker::QueryHandle>, tonic::Status>
    {
        panic!("provider query must not run during local capture: begin")
    }
    async fn page(
        &self,
        _s: [u8; 32],
        _n: u64,
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<native_broker::QueryReply<native_broker::QueryPage>, tonic::Status>
    {
        panic!("provider query must not run during local capture: page")
    }
    async fn read_commit(
        &self,
        _s: [u8; 32],
        _i: tinytree_model::CommitId,
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<
        native_broker::QueryReply<jj_native_backend::VerifiedMetadata>,
        tonic::Status,
    > {
        panic!("provider query must not run during local capture: read_commit")
    }
    async fn close_query(
        &self,
        _s: [u8; 32],
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<(), tonic::Status> {
        panic!("provider query must not run during local capture: close_query")
    }
    async fn close_scope(
        &self,
        _s: [u8; 32],
        _o: native_broker::QueryOwner,
    ) -> std::result::Result<(), tonic::Status> {
        self.setup_call();
        self.closes.fetch_add(1, Ordering::SeqCst);
        if self.deny_close.load(Ordering::Acquire) {
            return Err(tonic::Status::unavailable("fixture close ACK unavailable"));
        }
        Ok(())
    }
}

struct NoCaptureSource;
#[async_trait]
impl output_core::BlobSource for NoCaptureSource {
    async fn fetch(&self, _: &output_core::Artifact, _: &mut fs::File) -> Result<()> {
        panic!("capture must not hydrate source FILE bytes")
    }
    async fn missing(&self, _: &[output_core::Artifact]) -> Result<Vec<output_core::Artifact>> {
        panic!("capture must not perform source availability requests")
    }
}
#[async_trait]
impl output_core::SourceDirectoryProvider for NoCaptureSource {
    async fn read_directory(
        &self,
        _: output_core::SourcePin,
        _: [u8; 32],
        _: String,
        _: output_core::SourceLoadLimits,
        _: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<output_core::VerifiedSourceDirectory> {
        panic!("capture must not hydrate source directories")
    }
    async fn read_symlink(
        &self,
        _: output_core::SourcePin,
        _: [u8; 32],
        _: u64,
        _: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<String> {
        panic!("capture must not hydrate symlinks")
    }
}
#[derive(Default)]
struct CaptureSink {
    blobs: usize,
}
#[async_trait]
impl capture::LocalCaptureSink for CaptureSink {
    async fn write(&mut self, value: capture::OwnedCaptureBlob) -> Result<()> {
        assert!(!value.bytes().is_empty());
        self.blobs += 1;
        Ok(())
    }
}

#[test]
fn local_capture_reads_exact_existing_state_without_loaders_or_provider_queries() {
    futures::executor::block_on(async {
        use jj_lib::index::IndexStore;
        let fixture = Fixture::new();
        let index_config = jj_native_index::Config {
            endpoint: jj_native_index::Endpoint::UnixSocket(
                fixture.binding.admin.join("broker.sock"),
            ),
            store_alias: "example".into(),
            provider_fingerprint: [7; 32],
        };
        let shared_objects = std::cell::RefCell::new(None);
        let setup_queries = Arc::new(NoCaptureQueries::default());
        let setup_cleanup = std::cell::RefCell::new(None);
        let initializer = |_: &UserSettings,
                           path: &Path|
         -> std::result::Result<Box<dyn Backend>, BackendInitError> {
            let objects = Arc::new(
                jj_native_backend::DirectObjectIo::init(path, Default::default())
                    .map_err(|e| BackendInitError(Box::new(e)))?,
            );
            *shared_objects.borrow_mut() = Some(objects.clone());
            // The factory is synchronous inside jj's futures executor. Join one
            // bounded setup worker rather than nesting that executor or opening
            // a second owner of the same local object database.
            let backend = std::thread::spawn(move || {
                futures::executor::block_on(NativeBackend::from_io(
                    objects,
                    BackendConfig::default(),
                ))
            })
            .join()
            .unwrap()
            .map_err(|e| BackendInitError(Box::new(e)))?;
            Ok(Box::new(backend))
        };
        let index_initializer = |_: &UserSettings, path: &Path, root: &OperationId| {
            let objects = shared_objects.borrow().as_ref().unwrap().clone();
            jj_native_index::NativeIndexStore::init_fresh_for_root_operation(
                path,
                index_config.clone(),
                Default::default(),
                setup_queries.clone(),
                objects,
                vec![],
                &root.as_bytes().try_into().unwrap(),
            )
            .map(|s| {
                *setup_cleanup.borrow_mut() = Some(s.cleanup_handle());
                Box::new(s) as Box<dyn IndexStore>
            })
            .map_err(|e| BackendInitError(Box::new(e)))
        };
        let (workspace, repo) = initialize_with_index(
            &fixture.registry,
            &fixture.binding,
            &fixture.settings,
            &initializer,
            Signer::new(None, vec![]),
            fixture.connector.clone(),
            &CommitId::from_bytes(&[0; 32]),
            WorkspaceName::DEFAULT.to_owned(),
            &index_initializer,
        )
        .await
        .unwrap();
        let exact_op = workspace.working_copy().operation_id().clone();
        assert_ne!(
            exact_op.as_bytes(),
            &[0; 64],
            "fixture preserves real initial working-copy operation"
        );
        drop(workspace);
        drop(repo);
        setup_cleanup
            .borrow()
            .as_ref()
            .unwrap()
            .wait(std::time::Duration::from_secs(5))
            .unwrap();
        let setup_calls = setup_queries.calls.load(Ordering::SeqCst);
        assert!(
            setup_calls > 0,
            "ordinary initialization classified its new draft against ROOT"
        );
        setup_queries.capture.store(true, Ordering::Release);
        let objects = shared_objects.borrow().as_ref().unwrap().clone();
        let scope = remote_state::RemoteScope::new(
            fixture.binding.admin.join("broker.sock"),
            "example".into(),
        )
        .unwrap();
        // Match the actual broker initializer: jj owns the ordinary-mode store
        // directory beneath the owner-private external repository.
        fs::write(
            fixture.binding.repo.join("store/native-broker.toml"),
            format!("version=1\nsocket={:?}\nstore='example'\n", scope.socket),
        )
        .unwrap();
        let source = Arc::new(NoCaptureSource);
        let tree = output_core::Tree::open(
            fixture.binding.admin.join("capture-source"),
            "capture",
            source.clone(),
            Default::default(),
        )
        .unwrap();
        let projection = fixture.connector.projected.clone();
        tree.install_lazy_source(
            output_core::LazySourceRoot {
                identity: tree.identity().to_owned(),
                root_tree: projection.tree.terms[0],
                pin: output_core::SourcePin {
                    root: projection.tree.terms[0],
                    generation: projection.generation,
                },
                provider: source,
                load_limits: Default::default(),
            },
            None,
            "initial",
            &Default::default(),
        )
        .unwrap();
        // Host retry provenance lives outside administration captured below.
        // Use a setup worker, as for backend initialization, without nesting
        // this test's futures executor or opening a provider scope.
        let host_registry = fixture.registry.clone();
        let host_binding = fixture.binding.clone();
        let context = resolution_context(&fixture);
        let admission = resolution_admission();
        let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
        let resolution = std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let base = host_registry
                    .landing_resolution_base(&host_binding, &proofs, &admission)
                    .await
                    .unwrap();
                host_registry
                    .begin_landing_resolution(&host_binding, &context, &base, &proofs, &admission)
                    .await
                    .unwrap()
            })
        })
        .join()
        .unwrap();
        let before = fixture.connector.calls.load(Ordering::SeqCst);
        let snapshot = tree
            .freeze_lazy_source(
                output_core::SourceFrontendFence::TreeOnly,
                &Default::default(),
            )
            .unwrap();
        let permit =
            capture::TreeFreezePermit::from_snapshot(snapshot, projection.clone(), Arc::new(()))
                .unwrap();
        let mut sink = CaptureSink::default();
        let candidate = capture::read_candidate(
            capture::CaptureInput {
                registry: fixture.registry.clone(),
                binding: fixture.binding.clone(),
                index_config: index_config.clone(),
                local_objects: objects.clone(),
                remote_scope: scope.clone(),
            },
            permit,
            Default::default(),
            &mut sink,
        )
        .await
        .unwrap();
        assert!(sink.blobs > 0);
        let host_nonce = hex(&resolution.nonce());
        assert!(!String::from_utf8_lossy(candidate.encoded_data()).contains(&host_nonce));
        assert!(
            !String::from_utf8_lossy(candidate.private_working_copy_bytes()).contains(&host_nonce)
        );
        assert!(
            !candidate
                .private_remote_bytes()
                .is_some_and(|bytes| String::from_utf8_lossy(bytes).contains(&host_nonce))
        );
        assert_eq!(
            setup_queries.calls.load(Ordering::SeqCst),
            setup_calls,
            "capture never opens or closes provider scopes"
        );
        assert_eq!(candidate.data().working_copy.wc_operation, exact_op.hex());
        assert!(
            candidate
                .data()
                .operations
                .iter()
                .any(|op| op.operation == "00".repeat(64))
        );
        assert_eq!(
            candidate.data().operations.len(),
            candidate.data().indexes.len()
        );
        assert_eq!(
            fixture.connector.calls.load(Ordering::SeqCst),
            before,
            "no normal working-copy loader/connector during capture"
        );
        let mut private_wc: serde_json::Value =
            serde_json::from_slice(candidate.private_working_copy_bytes()).unwrap();
        assert!(
            candidate.data().working_copy.tree.labels.is_empty(),
            "supported jj WC uses normalized no-label state"
        );
        assert!(
            candidate.seal_quiescent().is_err(),
            "TreeOnly never manufactures frontend quiescence"
        );
        let snapshot = tree
            .freeze_lazy_source(
                output_core::SourceFrontendFence::TreeOnly,
                &Default::default(),
            )
            .unwrap();
        let inventory = capture::read_local_root_inventory(
            capture::CaptureInput {
                registry: fixture.registry.clone(),
                binding: fixture.binding.clone(),
                index_config: index_config.clone(),
                local_objects: objects.clone(),
                remote_scope: scope.clone(),
            },
            capture::TreeFreezePermit::from_snapshot(snapshot, projection.clone(), Arc::new(()))
                .unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(inventory.working_copy_operation(), &exact_op);
        assert!(inventory.retained_operations().contains(&exact_op));
        assert!(
            inventory
                .roots()
                .iter()
                .any(|root| root.kind == tinytree_model::Kind::Tree)
        );
        assert!(
            inventory.seal_quiescent().is_err(),
            "TreeOnly inventory cannot make Clean"
        );
        assert_eq!(setup_queries.calls.load(Ordering::SeqCst), setup_calls);
        // Explicit private-state corruption boundary: canonical no-label state
        // must not accidentally match a nonempty projected label. This is still
        // a TreeOnly candidate, never a manufactured host capture proof.
        private_wc["projection"]["tree"]["labels"] = serde_json::json!(["unexpected-label"]);
        PrivateDir::open_existing(&fixture.binding.working_copy)
            .unwrap()
            .replace("native-vfs.json", &serde_json::to_vec(&private_wc).unwrap())
            .unwrap();
        let mut changed = projection;
        changed.tree.labels = vec!["unexpected-label".into()];
        let snapshot = tree
            .freeze_lazy_source(
                output_core::SourceFrontendFence::TreeOnly,
                &Default::default(),
            )
            .unwrap();
        let permit =
            capture::TreeFreezePermit::from_snapshot(snapshot, changed, Arc::new(())).unwrap();
        let error = capture::read_candidate(
            capture::CaptureInput {
                registry: fixture.registry.clone(),
                binding: fixture.binding.clone(),
                index_config,
                local_objects: objects,
                remote_scope: scope,
            },
            permit,
            Default::default(),
            &mut sink,
        )
        .await
        .err()
        .unwrap();
        assert!(
            error
                .to_string()
                .contains("working-copy commit tree mismatch")
        );
        assert_eq!(setup_queries.calls.load(Ordering::SeqCst), setup_calls);
    });
}

#[test]
fn local_default_inventory_retains_hidden_indexes_and_pending_archived_intents_without_repair() {
    futures::executor::block_on(async {
        use crate::remote_state::{Kind, Pending, State};
        fn index_files(path: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
            fn walk(
                base: &Path,
                here: &Path,
                out: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>,
            ) {
                for entry in fs::read_dir(here).unwrap() {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        walk(base, &path, out);
                    } else {
                        out.insert(
                            path.strip_prefix(base).unwrap().to_path_buf(),
                            fs::read(path).unwrap(),
                        );
                    }
                }
            }
            let mut files = Default::default();
            walk(path, path, &mut files);
            files
        }
        let fixture = Fixture::new();
        let shared = std::cell::RefCell::new(None);
        let initializer = |_: &UserSettings,
                           path: &Path|
         -> std::result::Result<Box<dyn Backend>, BackendInitError> {
            let objects = Arc::new(
                jj_native_backend::DirectObjectIo::init(path, Default::default())
                    .map_err(|e| BackendInitError(Box::new(e)))?,
            );
            *shared.borrow_mut() = Some(objects.clone());
            let backend = std::thread::spawn(move || {
                futures::executor::block_on(NativeBackend::from_io(
                    objects,
                    BackendConfig::default(),
                ))
            })
            .join()
            .unwrap()
            .map_err(|e| BackendInitError(Box::new(e)))?;
            Ok(Box::new(backend))
        };
        let (workspace, repo) = initialize(
            &fixture.registry,
            &fixture.binding,
            &fixture.settings,
            &initializer,
            Signer::new(None, vec![]),
            fixture.connector.clone(),
            &CommitId::from_bytes(&[0; 32]),
            WorkspaceName::DEFAULT.to_owned(),
        )
        .await
        .unwrap();
        let wc_operation = workspace.working_copy().operation_id().clone();
        let current = repo
            .store()
            .get_commit_async(
                repo.view()
                    .get_wc_commit_id(WorkspaceName::DEFAULT)
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut hidden_data = current.store_commit().as_ref().clone();
        hidden_data.parents = vec![repo.store().root_commit_id().clone()];
        hidden_data.change_id = jj_lib::backend::ChangeId::from_bytes(&[13; 16]);
        hidden_data.description = "hidden indexed draft".into();
        let hidden = repo
            .store()
            .write_commit(hidden_data.clone(), None)
            .await
            .unwrap();
        let mut tx = repo.start_transaction();
        tx.repo_mut()
            .index_commits(std::slice::from_ref(&hidden))
            .await
            .unwrap();
        let unpublished = tx.write("retain unpublished indexed draft").await.unwrap();
        let hidden_operation = unpublished.operation().id().clone();
        assert!(
            !repo.view().heads().contains(hidden.id()),
            "hidden object is not a view head"
        );
        hidden_data.description = "pending intent draft".into();
        let pending_commit = repo
            .store()
            .write_commit(hidden_data.clone(), None)
            .await
            .unwrap();
        hidden_data.description = "archived intent draft".into();
        let archived_commit = repo.store().write_commit(hidden_data, None).await.unwrap();
        let objects = shared.borrow().as_ref().unwrap().clone();
        drop(unpublished);
        drop(workspace);
        drop(repo);
        let scope = remote_state::RemoteScope::new(
            fixture.binding.admin.join("broker.sock"),
            "example".into(),
        )
        .unwrap();
        fs::write(
            fixture.binding.repo.join("store/native-broker.toml"),
            format!("version=1\nsocket={:?}\nstore='example'\n", scope.socket),
        )
        .unwrap();
        let mut remote = State::new(&scope);
        remote.pending = Some(Pending {
            kind: Kind::Fetch,
            reference: "refs/heads/main".into(),
            bookmark: "main".into(),
            generation: 0,
            head: Some(pending_commit.id().hex()),
            request_id: "11".repeat(16),
            base_operation: wc_operation.hex(),
            confirmed_generation: None,
        });
        remote.archived_pushes.push(Pending {
            kind: Kind::Push,
            reference: "refs/heads/topic".into(),
            bookmark: "topic".into(),
            generation: 2,
            head: Some(archived_commit.id().hex()),
            request_id: "12".repeat(16),
            base_operation: hidden_operation.hex(),
            confirmed_generation: Some(3),
        });
        remote.validate(&scope).unwrap();
        PrivateDir::open_existing(&fixture.binding.working_copy)
            .unwrap()
            .replace(
                "native-remote.toml",
                toml::to_string(&remote).unwrap().as_bytes(),
            )
            .unwrap();
        let source = Arc::new(NoCaptureSource);
        let tree = output_core::Tree::open(
            fixture.binding.admin.join("inventory-source"),
            "inventory",
            source.clone(),
            Default::default(),
        )
        .unwrap();
        let projection = fixture.connector.projected.clone();
        tree.install_lazy_source(
            output_core::LazySourceRoot {
                identity: tree.identity().into(),
                root_tree: projection.tree.terms[0],
                pin: output_core::SourcePin {
                    root: projection.tree.terms[0],
                    generation: projection.generation,
                },
                provider: source,
                load_limits: Default::default(),
            },
            None,
            "initial",
            &Default::default(),
        )
        .unwrap();
        let input = || capture::CaptureInput {
            registry: fixture.registry.clone(),
            binding: fixture.binding.clone(),
            index_config: jj_native_index::Config {
                endpoint: jj_native_index::Endpoint::UnixSocket(scope.socket.clone()),
                store_alias: scope.store.clone(),
                provider_fingerprint: [7; 32],
            },
            local_objects: objects.clone(),
            remote_scope: scope.clone(),
        };
        let permit = || {
            capture::TreeFreezePermit::from_snapshot(
                tree.freeze_lazy_source(
                    output_core::SourceFrontendFence::TreeOnly,
                    &Default::default(),
                )
                .unwrap(),
                projection.clone(),
                Arc::new(()),
            )
            .unwrap()
        };
        let index = fixture.binding.repo.join("index");
        // Many real retained operations can name one unchanged index snapshot.
        // A per-call validated-segment memo must charge parsed rows/segments
        // once, while still reading and bounding every operation association.
        use jj_lib::op_store::OpStore as _;
        let operation_store = jj_lib::simple_op_store::SimpleOpStore::load(
            &fixture.binding.repo.join("op_store"),
            jj_lib::op_store::RootOperationData {
                root_commit_id: CommitId::from_bytes(&[0; 32]),
            },
        );
        let original_operation = operation_store
            .read_operation_bounded(&hidden_operation, 4 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap();
        let shared_control = fs::read(index.join("op_links").join(hidden_operation.hex())).unwrap();
        let mut shared_operations = Vec::new();
        for number in 0..64 {
            let mut operation = original_operation.clone();
            operation.metadata.description = format!("retained unchanged index {number}");
            let operation = operation_store.write_operation(&operation).await.unwrap();
            fs::write(
                index.join("op_links").join(operation.hex()),
                &shared_control,
            )
            .unwrap();
            shared_operations.push(operation);
        }
        assert_eq!(
            shared_operations
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            64
        );
        let before = index_files(&index);
        assert_eq!(&shared_control[..2], &[0x0a, 64]);
        let mut segment_name = hex(&shared_control[2..66]);
        let mut unique_segments = std::collections::BTreeSet::new();
        loop {
            assert!(unique_segments.insert(segment_name.clone()));
            let bytes = before
                .get(&PathBuf::from("segments").join(&segment_name))
                .unwrap();
            let parent_length = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
            if parent_length == 0 {
                break;
            }
            segment_name = std::str::from_utf8(&bytes[8..8 + parent_length])
                .unwrap()
                .to_owned();
        }
        let expected_index_roots = [
            CommitId::from_bytes(&[0; 32]),
            current.id().clone(),
            hidden.id().clone(),
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
        let tight_index_limits = jj_lib::default_index::DefaultIndexGcLimits {
            max_operations: shared_operations.len(),
            max_segments: unique_segments.len(),
            max_commits: expected_index_roots.len(),
            max_roots: expected_index_roots.len(),
            ..Default::default()
        };
        let readonly_index = jj_lib::default_index::DefaultIndexStore::load(&index);
        let read_shared = || {
            readonly_index.gc_roots_readonly(
                &shared_operations,
                &OperationId::from_bytes(&[0; 64]),
                &CommitId::from_bytes(&[0; 32]),
                16,
                tight_index_limits,
            )
        };
        assert_eq!(
            read_shared()
                .unwrap()
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            expected_index_roots
        );
        assert_eq!(index_files(&index), before);
        let calls = fixture.connector.calls.load(Ordering::SeqCst);
        let inventory = capture::read_local_root_inventory(input(), permit(), Default::default())
            .await
            .unwrap();
        assert_eq!(inventory.working_copy_operation(), &wc_operation);
        assert!(inventory.retained_operations().contains(&hidden_operation));
        assert!(
            shared_operations
                .iter()
                .all(|operation| inventory.retained_operations().contains(operation)),
            "every actual retained operation must be inventoried even when its index is shared"
        );
        assert!(
            !inventory.operation_heads().contains(&hidden_operation),
            "unpublished operation is not a current head"
        );
        for commit in [&hidden, &pending_commit, &archived_commit] {
            assert!(
                inventory.roots().contains(&tinytree_model::ObjectKey {
                    kind: tinytree_model::Kind::Commit,
                    id: tinytree_model::ObjectId(commit.id().as_bytes().try_into().unwrap())
                }),
                "all indexed and intent objects remain roots"
            );
        }
        assert!(inventory.seal_quiescent().is_err());
        assert_eq!(index_files(&index), before);
        assert_eq!(
            fixture.connector.calls.load(Ordering::SeqCst),
            calls,
            "inventory never loads a working copy"
        );
        let mut limited = capture::CaptureLimits::default();
        limited.max_operations = 1;
        assert!(
            capture::read_local_root_inventory(input(), permit(), limited)
                .await
                .is_err()
        );
        assert_eq!(index_files(&index), before);
        let association = PathBuf::from("op_links").join(hidden_operation.hex());
        let association_bytes = before.get(&association).unwrap();
        fs::remove_file(index.join(&association)).unwrap();
        let missing = index_files(&index);
        assert!(
            capture::read_local_root_inventory(input(), permit(), Default::default())
                .await
                .is_err()
        );
        assert_eq!(
            index_files(&index),
            missing,
            "missing association must not rebuild"
        );
        fs::write(index.join(&association), association_bytes).unwrap();
        // Select the exact commit segment named by this retained operation,
        // not an arbitrary orphan left behind by index compaction.
        assert_eq!(&association_bytes[..2], &[0x0a, 64]);
        let segment = PathBuf::from("segments").join(hex(&association_bytes[2..66]));
        let segment_bytes = before.get(&segment).unwrap();
        fs::write(index.join(&segment), b"corrupt retained segment").unwrap();
        let corrupt = index_files(&index);
        assert_eq!(
            read_shared().unwrap_err().kind(),
            std::io::ErrorKind::InvalidData,
            "a new read must not reuse another call's validated segment memo"
        );
        assert_eq!(index_files(&index), corrupt);
        assert!(
            capture::read_local_root_inventory(input(), permit(), Default::default())
                .await
                .is_err()
        );
        assert_eq!(
            index_files(&index),
            corrupt,
            "corrupt index must not reinitialize"
        );
        fs::write(index.join(&segment), segment_bytes).unwrap();
        assert_eq!(index_files(&index), before);
        // Archived ancestry is required, not optional diagnostic metadata.
        remote.archived_pushes[0].base_operation = "ff".repeat(64);
        PrivateDir::open_existing(&fixture.binding.working_copy)
            .unwrap()
            .replace(
                "native-remote.toml",
                toml::to_string(&remote).unwrap().as_bytes(),
            )
            .unwrap();
        assert!(
            capture::read_local_root_inventory(input(), permit(), Default::default())
                .await
                .is_err()
        );
        assert_eq!(index_files(&index), before);
    });
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires usable /dev/fuse and fusermount3"]
async fn native_capture_rejects_another_workspace_before_private_reads() {
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().canonicalize().unwrap();
    let registry = Registry::create(base.join("registry")).unwrap();
    let objects_path = PrivateDir::create(base.join("objects")).unwrap();
    let objects = Arc::new(
        jj_native_backend::DirectObjectIo::init(objects_path.path(), Default::default()).unwrap(),
    );
    let source = Arc::new(NoCaptureSource);
    let cache =
        output_core::LocalCas::open(base.join("cache"), source, Default::default()).unwrap();
    let index_config = jj_native_index::Config {
        endpoint: jj_native_index::Endpoint::UnixSocket(base.join("broker.sock")),
        store_alias: "example".into(),
        provider_fingerprint: [7; 32],
    };
    let scope = native_workspace::Scope::from_configured_identity(
        &index_config.store_alias,
        &index_config.provider_fingerprint,
    )
    .unwrap();
    let empty = tinytree_model::TreeId(tinytree_model::empty_tree().descriptor.key.id.0);
    let mut mounted = Vec::new();
    for (name, id) in [("first", [41; 32]), ("second", [42; 32])] {
        let root = base.join(format!("{name}-root"));
        let config = native_vfs::Config {
            root: root.clone(),
            state: base.join(format!("{name}-source")),
            workspace_identity: id,
            scope: scope.clone(),
            root_merge: tinytree_model::TreeMerge {
                terms: vec![empty],
                labels: vec![String::new()],
            },
            compiler_limits: Default::default(),
            source_limits: Default::default(),
        };
        let mount = Arc::new(
            native_vfs::start(config, objects.clone(), cache.clone())
                .await
                .unwrap(),
        );
        let binding = registry
            .binding(
                id,
                &root,
                "example".into(),
                LocalEndpoint::UnixSocket(base.join("rbe.sock")),
                name.into(),
            )
            .unwrap();
        let _admin = PrivateDir::create(&binding.admin).unwrap();
        let _wc = PrivateDir::create(&binding.working_copy).unwrap();
        let controller = native_vfs_controller::Controller::open(
            mount.clone(),
            objects.clone(),
            native_vfs_controller::Config::new("example".into(), binding.working_copy.clone()),
        )
        .await
        .unwrap();
        mounted.push((mount, controller, binding));
    }
    let (mount_a, controller_a, binding_a) = mounted.remove(0);
    let (mount_b, controller_b, binding_b) = mounted.remove(0);
    let projection = controller_a.inspect().unwrap();
    assert_eq!(
        controller_b.inspect().unwrap(),
        projection,
        "same repository/tree/generation is not workspace identity"
    );
    let actual = controller_a.capture_for_backup(&projection).await.unwrap();
    assert_eq!(actual.binding().root(), binding_a.root);
    fs::write(
        binding_b.root.join("independent"),
        b"second workspace remains writable",
    )
    .unwrap();
    let permit = capture::TreeFreezePermit::from_controller(actual, Arc::new(())).unwrap();
    let mut sink = CaptureSink::default();
    let error = capture::read_candidate(
        capture::CaptureInput {
            registry,
            binding: binding_b.clone(),
            index_config,
            local_objects: objects,
            remote_scope: remote_state::RemoteScope::new(
                base.join("broker.sock"),
                "example".into(),
            )
            .unwrap(),
        },
        permit,
        Default::default(),
        &mut sink,
    )
    .await
    .err()
    .unwrap();
    assert!(
        error
            .to_string()
            .contains("proof belongs to another workspace"),
        "reject actual cross-workspace proof, not later absent OP/Ready state: {error}"
    );
    assert_eq!(sink.blobs, 0);
    assert!(
        !binding_b.repo.exists(),
        "target OP/VIEW store deliberately absent; mismatch must precede every private read"
    );
    fs::write(
        binding_a.root.join("resumed"),
        b"capture released on rejection",
    )
    .unwrap();
    let options = jj_lib::working_copy::SnapshotOptions {
        base_ignores: jj_lib::gitignore::GitIgnoreFile::empty(),
        progress: None,
        start_tracking_matcher: &jj_lib::matchers::EverythingMatcher,
        force_tracking_matcher: &jj_lib::matchers::NothingMatcher,
        max_new_file_size: 0,
    };
    let mut mutation = controller_a.begin(&projection).await.unwrap();
    let (receipt, _) = mutation
        .snapshot("exclude-local-capture", &options)
        .await
        .unwrap();
    mutation
        .finish(
            &receipt.after,
            &OperationId::from_bytes(&[1; 64]),
            Some(&receipt),
        )
        .await
        .unwrap();
    drop(mutation);
    let actual = controller_a
        .capture_for_backup(&receipt.after)
        .await
        .unwrap();
    assert!(!actual.snapshot().entries().is_empty());
    let permit = capture::TreeFreezePermit::from_controller(actual, Arc::new(())).unwrap();
    drop(permit);
    fs::write(binding_a.root.join("resumed"), b"new unsnapshotted bytes").unwrap();
    let actual = controller_a
        .capture_for_backup(&receipt.after)
        .await
        .unwrap();
    assert!(capture::TreeFreezePermit::from_controller(actual, Arc::new(())).is_err());
    drop(controller_a);
    drop(controller_b);
    mount_a.shutdown().await.unwrap();
    mount_b.shutdown().await.unwrap();
}

// This loopback source tests authenticated SDK capabilities plus exact admin
// staging. It is not a kernel mount or physical capture-policy attestation.
#[derive(Clone)]
struct RestoreCloudFixture {
    version: tinytree_client::cloud_protocol::Version,
    capture: Arc<Vec<u8>>,
}
#[tonic::async_trait]
impl tinytree_client::cloud_protocol::workspace_cloud_server::WorkspaceCloud
    for RestoreCloudFixture
{
    async fn acquire_version_pin(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::AcquireVersionPinRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::VersionPin>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "legacy admin fixture has no durable version pins",
        ))
    }
    async fn release_version_pin(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::ReleaseVersionPinRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::VersionPinReleased>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "legacy admin fixture has no durable version pins",
        ))
    }
    async fn put_object_representation(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::PutObjectRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::WriteReceipt>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "fixture has no exact representation writer",
        ))
    }
    type ReadObjectRepresentationStream = Self::ReadObjectStream;
    async fn read_object_representation(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::ReadObjectRepresentationRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReadObjectRepresentationStream>, tonic::Status>
    {
        Err(tonic::Status::unimplemented(
            "fixture has no exact representation reader",
        ))
    }

    async fn list_workspaces(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::ListWorkspacesRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::ListWorkspacesResponse>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "legacy exact-restore fixture has no discovery service",
        ))
    }
    async fn list_versions(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::ListVersionsRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::ListVersionsResponse>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "legacy exact-restore fixture has no discovery service",
        ))
    }

    async fn put_object(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::PutObjectRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::WriteReceipt>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    async fn put_index_artifact(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::PutIndexArtifactRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::IndexArtifactWriteReceipt>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    async fn lookup_index_artifacts(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::LookupIndexArtifactsRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::LookupIndexArtifactsResponse>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    async fn lookup_objects(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::LookupObjectsRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::LookupObjectsResponse>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    async fn publish_version(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::PublishVersionRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::Publication>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    async fn get_workspace(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::GetWorkspaceRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::Workspace>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    async fn get_publication(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::GetPublicationRequest>,
    ) -> std::result::Result<
        tonic::Response<tinytree_client::cloud_protocol::GetPublicationResponse>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    type ReadIndexArtifactStream = futures::stream::BoxStream<
        'static,
        std::result::Result<tinytree_client::cloud_protocol::IndexArtifactChunk, tonic::Status>,
    >;
    async fn read_index_artifact(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::ReadIndexArtifactRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReadIndexArtifactStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    type ReadObjectStream = futures::stream::BoxStream<
        'static,
        std::result::Result<tinytree_client::cloud_protocol::ObjectChunk, tonic::Status>,
    >;
    async fn read_object(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::ReadObjectRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReadObjectStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    type ReadNativeClosureStream = futures::stream::BoxStream<
        'static,
        std::result::Result<tinytree_client::cloud_protocol::NativeClosureChunk, tonic::Status>,
    >;
    async fn read_native_closure(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::ReadNativeClosureRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReadNativeClosureStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    async fn get_latest_captured_version(
        &self,
        _: tonic::Request<tinytree_client::cloud_protocol::GetLatestCapturedVersionRequest>,
    ) -> std::result::Result<tonic::Response<tinytree_client::cloud_protocol::Version>, tonic::Status>
    {
        Err(tonic::Status::unimplemented(
            "fixture does not serve this method",
        ))
    }
    async fn get_version(
        &self,
        input: tonic::Request<tinytree_client::cloud_protocol::GetVersionRequest>,
    ) -> std::result::Result<tonic::Response<tinytree_client::cloud_protocol::Version>, tonic::Status>
    {
        let input = input.into_inner();
        assert_eq!(input.repository_id, "fixture");
        assert_eq!(input.workspace_id, self.version.workspace_id);
        assert_eq!(input.generation, 1);
        Ok(tonic::Response::new(self.version.clone()))
    }
    type ReadCaptureStream = futures::stream::BoxStream<
        'static,
        std::result::Result<tinytree_client::cloud_protocol::CaptureChunk, tonic::Status>,
    >;
    async fn read_capture(
        &self,
        input: tonic::Request<tinytree_client::cloud_protocol::ReadCaptureRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReadCaptureStream>, tonic::Status> {
        use futures::StreamExt;
        use tinytree_client::cloud_protocol as wire;
        let descriptor = tinytree_cloud::CaptureDescriptor::of(&self.capture);
        let input = input.into_inner();
        assert_eq!(input.repository_id, "fixture");
        assert_eq!(input.expected_capture_id, descriptor.id.0);
        let selected = wire::RawDigest {
            blake3: descriptor.selected.blake3.to_vec(),
            size_bytes: descriptor.selected.size_bytes,
        };
        Ok(tonic::Response::new(
            futures::stream::iter([Ok(wire::CaptureChunk {
                descriptor: Some(wire::CaptureDescriptor {
                    capture_id: descriptor.id.0.to_vec(),
                    selected: Some(selected),
                }),
                offset: 0,
                data: self.capture.as_ref().clone(),
                eof: true,
                manifest_id: self.version.manifest_id.clone(),
            })])
            .boxed(),
        ))
    }
}

#[derive(Debug)]
struct OwnedOnlyRestoreIo {
    inner: Arc<jj_native_backend::DirectObjectIo>,
    owned_reads: AtomicUsize,
}
#[async_trait]
impl jj_native_backend::NativeObjectIo for OwnedOnlyRestoreIo {
    async fn descriptor(
        &self,
        _: tinytree_model::ObjectKey,
    ) -> jj_lib::backend::BackendResult<Option<tinytree_model::Descriptor>> {
        panic!("restore must use an owner-aware descriptor read")
    }
    async fn descriptor_with_owner(
        &self,
        key: tinytree_model::ObjectKey,
        owner: native_broker::QueryOwner,
    ) -> jj_lib::backend::BackendResult<Option<tinytree_model::Descriptor>> {
        self.inner.descriptor_with_owner(key, owner).await
    }
    async fn read_metadata(
        &self,
        _: tinytree_model::ObjectKey,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        panic!("restore must use an owner-aware metadata read")
    }
    async fn read_metadata_with_owner(
        &self,
        key: tinytree_model::ObjectKey,
        owner: native_broker::QueryOwner,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.owned_reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_metadata_with_owner(key, owner).await
    }
    async fn write_metadata(
        &self,
        _: tinytree_model::Object,
    ) -> jj_lib::backend::BackendResult<tinytree_model::Descriptor> {
        panic!("restore must not author native objects")
    }
    async fn read_file(
        &self,
        _: tinytree_model::FileId,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::FileRead> {
        panic!("restore must not read FILE bodies")
    }
    async fn write_file(
        &self,
        _: &mut (dyn futures::AsyncRead + Send + Unpin),
        _: u64,
    ) -> jj_lib::backend::BackendResult<tinytree_model::FileId> {
        panic!("restore must not write FILE bodies")
    }
    async fn copy_catalog_snapshot(
        &self,
        _: jj_native_backend::CopyCatalogLimits,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::CopyCatalogSnapshot> {
        panic!("restore must not enumerate repository copies")
    }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_restore_stages_original_plural_operations_and_wc_without_publishing_ready() {
    use jj_lib::{
        op_store::{OpStore, Operation, RootOperationData, View},
        simple_op_store::SimpleOpStore,
    };
    use tinytree_cloud as c;
    use tinytree_model as m;
    let fixture = Fixture::new();
    let source_directory = fixture._directory.path().join("source-operations");
    fs::create_dir(&source_directory).unwrap();
    let store = SimpleOpStore::init(
        &source_directory,
        RootOperationData {
            root_commit_id: CommitId::from_bytes(&[0; 32]),
        },
    )
    .unwrap();
    let mut view = View::make_root(CommitId::from_bytes(&[0; 32]));
    view.wc_commit_ids
        .insert("original-workspace".into(), CommitId::from_bytes(&[0; 32]));
    view.wc_commit_ids
        .insert("retained-peer".into(), CommitId::from_bytes(&[0; 32]));
    let view_id = store.write_view(&view).await.unwrap();
    let mut old = Operation::make_root(view_id.clone());
    old.parents = vec![OperationId::from_bytes(&[0; 64])];
    old.metadata.description = "retained original working copy".into();
    old.metadata
        .attributes
        .insert("command".into(), "snapshot".into());
    old.metadata
        .attributes
        .insert("mode".into(), "retained".into());
    let wc_operation = store.write_operation(&old).await.unwrap();
    let mut current = old.clone();
    current.metadata.description = "retained independent snapshot operation".into();
    let snapshot_operation = store.write_operation(&current).await.unwrap();
    let mut objects = vec![
        c::export_view(&store, &view_id, Default::default())
            .await
            .unwrap(),
        c::export_operation(&store, &wc_operation, Default::default())
            .await
            .unwrap(),
        c::export_operation(&store, &snapshot_operation, Default::default())
            .await
            .unwrap(),
    ];
    // Supported codec compatibility case: the final view_id wins on decode,
    // and restore must retain this exact selected protobuf representation.
    let old_at = objects
        .iter()
        .position(|object| {
            object.id() == c::ObjectId::Operation(wc_operation.as_bytes().try_into().unwrap())
        })
        .unwrap();
    let mut alternate = vec![0x0a, 64];
    alternate.extend_from_slice(&[1; 64]);
    alternate.extend_from_slice(objects[old_at].payload().unwrap());
    objects[old_at] = c::VerifiedObject::verify(
        objects[old_at].id(),
        c::BlobDigest::of(&alternate),
        &alternate,
        Default::default(),
    )
    .unwrap();
    assert_eq!(objects[old_at].operation(), Some(&old));
    let view_at = objects
        .iter()
        .position(|object| matches!(object.id(), c::ObjectId::View(_)))
        .unwrap();
    let alternate_view = [&[0xf8, 0x07, 0x01][..], objects[view_at].payload().unwrap()].concat();
    objects[view_at] = c::VerifiedObject::verify(
        objects[view_at].id(),
        c::BlobDigest::of(&alternate_view),
        &alternate_view,
        Default::default(),
    )
    .unwrap();
    let source_records = objects
        .iter()
        .map(|object| {
            (
                object.id(),
                object.digest().unwrap(),
                object.payload().unwrap().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    objects.sort_by_key(|object| object.id());
    let encoded = c::build_index_artifacts(
        &c::PortableIndexSnapshot {
            roots: vec![m::CommitId::ROOT],
            drafts: vec![],
        },
        Default::default(),
    )
    .unwrap();
    let artifacts = encoded
        .artifacts
        .iter()
        .map(|artifact| {
            c::decode_index_artifact(&artifact.bytes, artifact.descriptor, Default::default())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let roots = vec![encoded.root];
    let graph = c::verify_index_artifact_graph(&roots, &artifacts, Default::default()).unwrap();
    let mut heads = vec![wc_operation.clone(), snapshot_operation.clone()];
    heads.sort();
    let mut operations = vec![c::OperationRecord {
        operation: "00".repeat(64),
        view: "00".repeat(64),
        parents: vec![],
    }];
    operations.extend(heads.iter().map(|id| c::OperationRecord {
        operation: id.hex(),
        view: view_id.hex(),
        parents: vec!["00".repeat(64)],
    }));
    let mut blobs = objects
        .iter()
        .map(|object| c::BlobRecord {
            kind: match object.id() {
                c::ObjectId::Operation(_) => 1,
                c::ObjectId::View(_) => 2,
            },
            id: hex(object.id().bytes()),
            blake3: object.digest().unwrap().blake3,
            size: object.digest().unwrap().size_bytes,
        })
        .collect::<Vec<_>>();
    for artifact in &encoded.artifacts {
        let (kind, id) = match artifact.descriptor.id {
            c::IndexArtifactId::Root(id) => (3, id.0),
            c::IndexArtifactId::Node(id) => (4, id.0),
        };
        blobs.push(c::BlobRecord {
            kind,
            id: hex(&id),
            blake3: artifact.descriptor.selected.blake3,
            size: artifact.descriptor.selected.size_bytes,
        });
    }
    blobs.sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
    let data = c::CaptureData {
        version: 1,
        policy: c::CapturePolicy::HostQuiescentV1,
        codec: c::CODEC.into(),
        operation_heads: heads.iter().map(|id| id.hex()).collect(),
        working_copy: c::PortableWorkingCopy {
            workspace: "original-workspace".into(),
            wc_operation: wc_operation.hex(),
            snapshot_operation: snapshot_operation.hex(),
            snapshot_commit: [0; 32],
            tree: c::CaptureTree {
                terms: fixture.connector.projected.tree.terms.clone(),
                labels: vec![],
            },
        },
        operations,
        indexes: std::iter::once([0; 64])
            .chain(heads.iter().map(|id| id.as_bytes().try_into().unwrap()))
            .map(|operation| c::IndexRecord {
                operation: hex(&operation),
                root: encoded.root.id.0,
            })
            .collect(),
        blobs,
        roots: vec![c::CapturedRoot {
            kind: m::Kind::Tree as u8,
            id: fixture.connector.projected.tree.terms[0],
        }],
        remote: Some(c::PortableRemoteState {
            observations: vec![c::RemoteObservation {
                reference: "main".into(),
                bookmark: "main".into(),
                generation: 0,
                head: None,
            }],
            archived_pushes: vec![],
            landing: None,
        }),
    };
    let capsule = c::encode_capture_data(&data, Default::default()).unwrap();
    let prepared_manifest = c::prepare_capture_manifest(
        &c::CaptureExportIdentity {
            workspace: [42; 32],
            exporter: [43; 16],
            sequence: 1,
            parents: vec![],
        },
        &capsule,
        &objects,
        &artifacts,
        &graph,
        Default::default(),
    )
    .unwrap();
    let raw = c::BlobDigest::of(prepared_manifest.bytes());
    let authority = m::cloud_native_binding_digest(
        "fixture",
        [44; 32],
        [42; 32],
        1,
        prepared_manifest.id().0,
        raw.blake3,
        raw.size_bytes,
    );
    let service = RestoreCloudFixture {
        version: tinytree_client::cloud_protocol::Version {
            workspace_id: vec![42; 32],
            generation: 1,
            manifest_id: prepared_manifest.id().0.to_vec(),
            previous_manifest_id: None,
            manifest: prepared_manifest.bytes().to_vec(),
            native_authority: Some(tinytree_client::cloud_protocol::NativeVersionAuthority {
                version: 1,
                owner_binding: vec![44; 32],
                binding_digest: authority.to_vec(),
                manifest_selected: Some(tinytree_client::cloud_protocol::RawDigest {
                    blake3: raw.blake3.to_vec(),
                    size_bytes: raw.size_bytes,
                }),
            }),
        },
        capture: Arc::new(capsule),
    };
    drop(graph);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |listener| async move {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(
                tinytree_client::cloud_protocol::workspace_cloud_server::WorkspaceCloudServer::new(
                    service,
                ),
            )
            .serve_with_incoming(incoming),
    );
    let client = tinytree_client::NativeClient::connect(tinytree_client::Config {
        endpoint: format!("http://{address}"),
        repository_id: "fixture".into(),
        compression: false,
        ..Default::default()
    })
    .await
    .unwrap();
    let source_native = fixture._directory.path().join("source-native");
    fs::create_dir(&source_native).unwrap();
    let objects_io = Arc::new(
        jj_native_backend::DirectObjectIo::init(&source_native, Default::default()).unwrap(),
    );
    let objects_io = Arc::new(OwnedOnlyRestoreIo {
        inner: objects_io,
        owned_reads: AtomicUsize::new(0),
    });
    let queries = Arc::new(NoCaptureQueries::default());
    let provider = restore::RestoreProvider {
        config: jj_native_index::Config {
            endpoint: match &fixture.binding.endpoint {
                Endpoint::UnixSocket { path } => {
                    jj_native_index::Endpoint::UnixSocket(path.clone())
                }
                Endpoint::WindowsNamedPipe { name } => {
                    jj_native_index::Endpoint::WindowsNamedPipe(name.clone())
                }
            },
            store_alias: "fixture".into(),
            provider_fingerprint: [7; 32],
        },
        index_limits: Default::default(),
        query: queries.clone(),
        objects: objects_io.clone(),
    };
    // A configured provider refusal must occur before journal/admin creation,
    // even though the authenticated selected version and raw proof are valid.
    for (name, deny_prepare, deny_cloud, bad_echo, deny_close, expected_closes) in [
        ("prepare_refusal_no_fallback", true, false, false, false, 0),
        ("unprepared_scope_refusal", false, true, false, false, 0),
        (
            "validation_refusal_closes_scope",
            false,
            false,
            true,
            false,
            1,
        ),
        ("close_ack_refusal", false, false, false, true, 1),
    ] {
        queries.deny_prepare.store(deny_prepare, Ordering::Release);
        queries.deny_cloud.store(deny_cloud, Ordering::Release);
        queries.bad_cloud_echo.store(bad_echo, Ordering::Release);
        queries.deny_close.store(deny_close, Ordering::Release);
        let calls_before = queries.calls.load(Ordering::SeqCst);
        let closes_before = queries.closes.load(Ordering::SeqCst);
        let denied_version = client
            .cloud_captured_version(client.cloud_retained_version([42; 32], 1).await.unwrap())
            .unwrap();
        let denied_objects = objects
            .iter()
            .map(|object| {
                c::VerifiedObject::verify(
                    object.id(),
                    object.digest().unwrap(),
                    object.payload().unwrap(),
                    Default::default(),
                )
                .unwrap()
            })
            .collect();
        let denied_artifacts = encoded
            .artifacts
            .iter()
            .map(|artifact| {
                c::decode_index_artifact(&artifact.bytes, artifact.descriptor, Default::default())
                    .unwrap()
            })
            .collect();
        assert!(
            restore::prepare_restore(
                client.clone(),
                restore::RestoreInputs {
                    version: denied_version,
                    objects: denied_objects,
                    artifacts: denied_artifacts
                },
                provider.clone(),
                Default::default(),
                Arc::new(()),
            )
            .await
            .is_err()
        );
        assert!(!fixture.binding.admin.exists(), "{name}");
        assert_eq!(
            queries.closes.load(Ordering::SeqCst) - closes_before,
            expected_closes,
            "{name}"
        );
        if deny_prepare {
            assert_eq!(
                queries.calls.load(Ordering::SeqCst) - calls_before,
                1,
                "preparation error cannot invoke the per-root fallback"
            );
        }
    }
    queries.deny_prepare.store(false, Ordering::Release);
    queries.deny_cloud.store(false, Ordering::Release);
    queries.bad_cloud_echo.store(false, Ordering::Release);
    queries.deny_close.store(false, Ordering::Release);
    let version = client
        .cloud_captured_version(client.cloud_retained_version([42; 32], 1).await.unwrap())
        .unwrap();
    let inputs = restore::RestoreInputs {
        version,
        objects,
        artifacts,
    };
    let source_permit = Arc::new(tokio::sync::Semaphore::new(1));
    let source_owner = Arc::new(source_permit.clone().acquire_owned().await.unwrap());
    let source_weak = Arc::downgrade(&source_owner);
    let prepared = restore::prepare_restore(
        client.clone(),
        inputs,
        provider.clone(),
        Default::default(),
        source_owner.clone(),
    )
    .await
    .unwrap();
    assert!(
        !fixture.binding.admin.exists(),
        "source preparation must not initialize the destination"
    );
    assert_eq!(prepared.projection_seed().wc_operation, wc_operation.hex());
    assert_eq!(
        prepared.projection_seed().snapshot_operation,
        snapshot_operation.hex()
    );
    assert_eq!(prepared.source_identity().workspace_id(), [42; 32]);
    assert_ne!(
        prepared.source_identity().workspace_id(),
        fixture.binding.workspace_id
    );
    let mut intent = fixture
        .registry
        .begin_restore(&fixture.binding, &prepared)
        .unwrap();
    intent.prepare_working_copy_directory().unwrap();
    let busy = fixture
        .registry
        .begin_restore(&fixture.binding, &prepared)
        .err()
        .expect("second restore admission must fail without blocking");
    assert!(busy.is::<restore::RestoreBusy>());
    struct ReleaseRestorePause(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
    impl Drop for ReleaseRestorePause {
        fn drop(&mut self) {
            *self.0.0.lock().unwrap_or_else(|e| e.into_inner()) = true;
            self.0.1.notify_all();
        }
    }
    let pause = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release = ReleaseRestorePause(pause.clone());
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let host = restore::RestoreHost {
        remote_scope: remote_state::RemoteScope::new(
            PathBuf::from("/private/broker.sock"),
            fixture.binding.repository.clone(),
        )
        .unwrap(),
        settings: fixture.settings.clone(),
        signer: Signer::new(None, vec![]),
        connector: fixture.connector.clone(),
        backend_config: Default::default(),
        backend_metadata: Arc::new(move |_| {
            if let Some(entered) = entered.lock().unwrap().take() {
                let _ = entered.send(());
            }
            let mut released = pause.0.lock().unwrap();
            while !*released {
                released = pause.1.wait(released).unwrap();
            }
            Ok(())
        }),
    };
    let task = tokio::spawn(restore::initialize_restored(intent, prepared, host));
    if waiting.await.is_err() {
        panic!("restore stage ended before pause: {:?}", task.await);
    }
    drop(source_owner);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(
        source_weak.upgrade().is_some(),
        "cancelled stage retains its physical source owner"
    );
    assert_eq!(source_permit.available_permits(), 0);
    assert!(
        restore::wait_for_restore_work_until(
            std::time::Instant::now() + std::time::Duration::from_millis(5)
        )
        .await
        .is_err()
    );
    drop(release);
    restore::wait_for_restore_work_until(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert!(
        !fixture.binding.admin.join("initialization.json").exists(),
        "cancelled stage cannot acknowledge Ready"
    );

    // The accepted worker completed exact staging after waiter cancellation.
    // A fresh authenticated source is required to reconcile its lost receipt.
    let version = client
        .cloud_captured_version(client.cloud_retained_version([42; 32], 1).await.unwrap())
        .unwrap();
    let fresh_objects = source_records
        .iter()
        .map(|(id, raw, bytes)| {
            c::VerifiedObject::verify(*id, *raw, bytes, Default::default()).unwrap()
        })
        .collect();
    let fresh_artifacts = encoded
        .artifacts
        .iter()
        .map(|artifact| {
            c::decode_index_artifact(&artifact.bytes, artifact.descriptor, Default::default())
                .unwrap()
        })
        .collect();
    let prepared = restore::prepare_restore(
        client.clone(),
        restore::RestoreInputs {
            version,
            objects: fresh_objects,
            artifacts: fresh_artifacts,
        },
        provider.clone(),
        Default::default(),
        Arc::new(()),
    )
    .await
    .unwrap();
    assert!(source_weak.upgrade().is_none());
    assert_eq!(source_permit.available_permits(), 1);
    let mut receipt = restore::reopen_staged_restore(
        fixture.registry.clone(),
        fixture.binding.clone(),
        prepared,
        restore::RestoreReadbackHost {
            remote_scope: remote_state::RemoteScope::new(
                PathBuf::from("/private/broker.sock"),
                fixture.binding.repository.clone(),
            )
            .unwrap(),
            settings: fixture.settings.clone(),
            connector: fixture.connector.clone(),
            backend_config: Default::default(),
            backend_metadata_check: Arc::new(|_| Ok(())),
        },
    )
    .await
    .unwrap();
    assert!(
        objects_io.owned_reads.load(Ordering::SeqCst) > 0,
        "actual Backend attachment must use owned physical I/O"
    );
    let sidecar_path = fixture.binding.working_copy.join("native-remote.toml");
    let sidecar = fs::read(&sidecar_path).unwrap();
    let destination_scope = remote_state::RemoteScope::new(
        PathBuf::from("/private/broker.sock"),
        fixture.binding.repository.clone(),
    )
    .unwrap();
    let recovered_remote = remote_state::decode(&sidecar, &destination_scope).unwrap();
    assert_eq!(recovered_remote.observations.len(), 1);
    assert_eq!(recovered_remote.observations[0].reference, "main");
    assert_eq!(receipt.wc_operation.as_slice(), wc_operation.as_bytes());
    assert_eq!(
        receipt.snapshot_operation.as_slice(),
        snapshot_operation.as_bytes()
    );
    assert_ne!(receipt.wc_operation, receipt.snapshot_operation);
    assert_eq!(receipt.tree, fixture.connector.projected.tree);
    let restored = SimpleOpStore::load(
        &fixture.binding.repo.join("op_store"),
        RootOperationData {
            root_commit_id: CommitId::from_bytes(&[0; 32]),
        },
    );
    assert_eq!(restored.read_operation(&wc_operation).await.unwrap(), old);
    assert_eq!(
        restored.read_operation(&snapshot_operation).await.unwrap(),
        current
    );
    assert_eq!(restored.read_view(&view_id).await.unwrap(), view);
    let local_raw = restored
        .read_cloud_operation_payload(&wc_operation, Default::default())
        .unwrap();
    let original_raw = source_records
        .iter()
        .find(|(id, _, _)| {
            *id == c::ObjectId::Operation(wc_operation.as_bytes().try_into().unwrap())
        })
        .unwrap()
        .1;
    assert_eq!(
        c::BlobDigest::of(&local_raw),
        original_raw,
        "restore must preserve the immutable selected representation"
    );
    // Ordinary writes of unchanged state must not replace restored map/unknown
    // field order with newly encoded bytes before the next backup.
    restored.write_view(&view).await.unwrap();
    restored.write_operation(&old).await.unwrap();
    restored.write_operation(&current).await.unwrap();
    let mut captured_objects = Vec::new();
    for (id, digest, bytes) in &source_records {
        let actual = match id {
            c::ObjectId::Operation(id) => {
                c::export_operation(&restored, &OperationId::from_bytes(id), Default::default())
                    .await
                    .unwrap()
            }
            c::ObjectId::View(id) => c::export_view(
                &restored,
                &jj_lib::op_store::ViewId::from_bytes(id),
                Default::default(),
            )
            .await
            .unwrap(),
        };
        assert_eq!(actual.payload(), Some(bytes.as_slice()));
        assert_eq!(actual.digest(), Some(*digest));
        captured_objects.push(actual);
    }
    captured_objects.sort_by_key(|object| object.id());
    let captured_artifacts = encoded
        .artifacts
        .iter()
        .map(|artifact| {
            c::decode_index_artifact(&artifact.bytes, artifact.descriptor, Default::default())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let captured_graph =
        c::verify_index_artifact_graph(&roots, &captured_artifacts, Default::default()).unwrap();
    let recaptured = c::encode_capture_data(&data, Default::default()).unwrap();
    let republished = c::prepare_capture_manifest(
        &c::CaptureExportIdentity {
            workspace: [42; 32],
            exporter: [43; 16],
            sequence: 1,
            parents: vec![],
        },
        &recaptured,
        &captured_objects,
        &captured_artifacts,
        &captured_graph,
        Default::default(),
    )
    .unwrap();
    assert_eq!(republished.id(), prepared_manifest.id());
    assert_eq!(republished.bytes(), prepared_manifest.bytes());
    drop(captured_graph);
    drop((captured_objects, captured_artifacts));

    assert_eq!(
        jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(
            &fixture.binding.repo.join("op_heads")
        )
        .get_op_heads_bounded(2)
        .unwrap(),
        heads
    );
    assert!(
        fixture.registry.lookup(&fixture.binding.root).is_err(),
        "staged metadata must never publish Ready"
    );
    assert!(fixture.registry.resume_info(&fixture.binding.root).is_err());
    assert!(fixture.binding.admin.join("restore-stage.json").exists());
    assert!(!fixture.binding.admin.join("initialization.json").exists());
    // Caller-editable diagnostic fields are never publication authority.
    receipt.wc_operation[0] ^= 1;
    let readback_host = || restore::RestoreReadbackHost {
        remote_scope: remote_state::RemoteScope::new(
            PathBuf::from("/private/broker.sock"),
            fixture.binding.repository.clone(),
        )
        .unwrap(),
        settings: fixture.settings.clone(),
        connector: fixture.connector.clone(),
        backend_config: Default::default(),
        backend_metadata_check: Arc::new(|_| Ok(())),
    };
    assert!(
        restore::finalize_staged_restore(receipt, readback_host())
            .await
            .is_err()
    );
    assert!(!fixture.binding.admin.join("initialization.json").exists());
    let journal_path = fixture.binding.admin.join("restore-stage.json");
    let journal_bytes = fs::read(&journal_path).unwrap();
    let mut legacy: serde_json::Value = serde_json::from_slice(&journal_bytes).unwrap();
    legacy.as_object_mut().unwrap().remove("index_fingerprint");
    fs::write(&journal_path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    assert!(
        fixture
            .registry
            .restore_resume_eligibility(&fixture.binding, [7; 32])
            .unwrap_err()
            .is::<restore::RestoreRecoveryRequired>()
    );
    fs::write(&journal_path, &journal_bytes).unwrap();

    let normalized = store
        .read_cloud_operation_payload(&wc_operation, Default::default())
        .unwrap();
    let staged_slot = fixture
        .binding
        .repo
        .join("op_store/operations")
        .join(wc_operation.hex());
    let selected = fs::read(&staged_slot).unwrap();
    assert_ne!(normalized, selected);
    fs::write(&staged_slot, &normalized).unwrap();
    let before_mismatch = fs::read(&journal_path).unwrap();
    let calls = queries.calls.load(Ordering::SeqCst);
    let version = client
        .cloud_captured_version(client.cloud_retained_version([42; 32], 1).await.unwrap())
        .unwrap();
    let fresh_objects = source_records
        .iter()
        .map(|(id, raw, bytes)| {
            c::VerifiedObject::verify(*id, *raw, bytes, Default::default()).unwrap()
        })
        .collect();
    let fresh_artifacts = encoded
        .artifacts
        .iter()
        .map(|artifact| {
            c::decode_index_artifact(&artifact.bytes, artifact.descriptor, Default::default())
                .unwrap()
        })
        .collect();
    let prepared = restore::prepare_restore_until(
        client.clone(),
        restore::RestoreInputs {
            version,
            objects: fresh_objects,
            artifacts: fresh_artifacts,
        },
        provider.clone(),
        Default::default(),
        Arc::new(()),
        std::time::Instant::now() + std::time::Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert!(
        queries.calls.load(Ordering::SeqCst) > calls,
        "reopen requires fresh current provider authorization"
    );
    let mismatch = restore::reopen_staged_restore(
        fixture.registry.clone(),
        fixture.binding.clone(),
        prepared,
        readback_host(),
    )
    .await;
    assert!(mismatch.is_err());
    assert_eq!(
        fs::read(&staged_slot).unwrap(),
        normalized,
        "failed pre-Ready readback must preserve mismatched state"
    );
    assert_eq!(fs::read(&journal_path).unwrap(), before_mismatch);
    assert!(!fixture.binding.admin.join("initialization.json").exists());
    // Reset only this test's injected corruption to exercise valid continuation.
    fs::write(&staged_slot, selected).unwrap();
    let calls = queries.calls.load(Ordering::SeqCst);
    let version = client
        .cloud_captured_version(client.cloud_retained_version([42; 32], 1).await.unwrap())
        .unwrap();
    let fresh_objects = source_records
        .iter()
        .map(|(id, raw, bytes)| {
            c::VerifiedObject::verify(*id, *raw, bytes, Default::default()).unwrap()
        })
        .collect();
    let fresh_artifacts = encoded
        .artifacts
        .iter()
        .map(|artifact| {
            c::decode_index_artifact(&artifact.bytes, artifact.descriptor, Default::default())
                .unwrap()
        })
        .collect();
    let prepared = restore::prepare_restore_until(
        client.clone(),
        restore::RestoreInputs {
            version,
            objects: fresh_objects,
            artifacts: fresh_artifacts,
        },
        provider.clone(),
        Default::default(),
        Arc::new(()),
        std::time::Instant::now() + std::time::Duration::from_secs(30),
    )
    .await
    .unwrap();
    assert!(
        queries.calls.load(Ordering::SeqCst) > calls,
        "reopen requires fresh current provider authorization"
    );
    let reopened = restore::reopen_staged_restore(
        fixture.registry.clone(),
        fixture.binding.clone(),
        prepared,
        readback_host(),
    )
    .await
    .unwrap();
    let ready = restore::finalize_staged_restore(reopened, readback_host())
        .await
        .unwrap();
    assert_eq!(ready.binding(), &fixture.binding);
    assert_eq!(ready.source_identity().workspace_id(), [42; 32]);
    assert_eq!(
        fixture.registry.lookup(&fixture.binding.root).unwrap(),
        fixture.binding
    );
    // The independent normal-initialization origin prevents a missing capture
    // attachment from silently downgrading an eligible restore to clone mode.
    let finalized_journal = fs::read(&journal_path).unwrap();
    fs::remove_file(&journal_path).unwrap();
    assert!(fixture.registry.lookup(&fixture.binding.root).is_err());
    assert!(
        fixture
            .registry
            .resume_info(&fixture.binding.root)
            .unwrap_err()
            .is::<restore::RestoreRecoveryRequired>()
    );
    assert!(
        fixture
            .registry
            .restore_resume_eligibility(&fixture.binding, [7; 32])
            .unwrap_err()
            .is::<restore::RestoreRecoveryRequired>()
    );
    fs::write(&journal_path, &finalized_journal).unwrap();
    assert!(
        fixture.registry.resume_info(&fixture.binding.root).is_err(),
        "clone resume must not admit a restored origin"
    );

    // Eligibility is monotone. Subsequent ordinary state must not be compared
    // to or rewritten from the captured plural heads or fetched old source.
    use jj_lib::op_heads_store::OpHeadsStore as _;
    let current_heads = jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(
        &fixture.binding.repo.join("op_heads"),
    );
    current_heads
        .update_op_heads(&heads, &snapshot_operation)
        .await
        .unwrap();
    queries.deny_cloud.store(true, Ordering::Release);
    let calls = queries.calls.load(Ordering::SeqCst);
    assert!(
        fixture
            .registry
            .restore_resume_eligibility(&fixture.binding, [7; 32])
            .unwrap()
            .is_some()
    );
    assert_eq!(
        fixture
            .registry
            .restored_resume_info(&fixture.binding.root, [7; 32])
            .unwrap()
            .binding,
        fixture.binding
    );
    assert_eq!(
        current_heads.get_op_heads_bounded(2).unwrap(),
        vec![snapshot_operation.clone()]
    );
    assert!(
        fixture
            .registry
            .restore_resume_eligibility(&fixture.binding, [8; 32])
            .is_err()
    );
    assert_eq!(queries.calls.load(Ordering::SeqCst), calls);

    let missing_admin = fixture.binding.admin.with_extension("preserved");
    fs::rename(&fixture.binding.admin, &missing_admin).unwrap();
    assert!(
        fixture
            .registry
            .restore_resume_eligibility(&fixture.binding, [7; 32])
            .unwrap_err()
            .is::<restore::RestoreRecoveryRequired>()
    );
    fs::rename(&missing_admin, &fixture.binding.admin).unwrap();
    let record = fixture
        .registry
        .path()
        .join("records")
        .join(format!("{}.json", fixture.binding.id()));
    fs::remove_file(record).unwrap();
    assert!(
        fixture
            .registry
            .restore_resume_eligibility(&fixture.binding, [7; 32])
            .unwrap()
            .is_some(),
        "Finalized before registry ACK is ordinary-resume eligible"
    );
    fixture.registry.publish_ready(&fixture.binding).unwrap();
    assert_eq!(
        current_heads.get_op_heads_bounded(2).unwrap(),
        vec![snapshot_operation]
    );
    assert_eq!(queries.calls.load(Ordering::SeqCst), calls);
    server.abort();
}

#[test]
fn candidate_retirement_removes_only_uninitialized_administration() {
    let fixture = Fixture::new();
    fixture.registry.provision(&fixture.binding).unwrap();
    let guard = fixture
        .registry
        .lock_candidate_retirement(&fixture.binding)
        .unwrap();
    fixture
        .registry
        .remove_candidate_admin(&fixture.binding, &guard)
        .unwrap();
    fixture
        .registry
        .remove_candidate_admin(&fixture.binding, &guard)
        .unwrap();
    assert!(!fixture.binding.admin.exists());
    assert!(fixture.binding.root.join(MARKER).exists());
    drop(guard);
    futures::executor::block_on(async {
        let initialized = Fixture::new();
        let (_workspace, _repo) = initialized.initialize().await.unwrap();
        let before = fs::read(initialized.binding.admin.join(JOURNAL)).unwrap();
        assert!(
            initialized
                .registry
                .lock_candidate_retirement(&initialized.binding)
                .is_err()
        );
        assert_eq!(
            fs::read(initialized.binding.admin.join(JOURNAL)).unwrap(),
            before
        );
        assert!(initialized.binding.repo.exists());
        assert_eq!(
            initialized
                .registry
                .lookup(&initialized.binding.root)
                .unwrap(),
            initialized.binding
        );
    });
}

#[test]
fn landing_intent_fences_legacy_readers_and_retains_frozen_and_unpublished_roots() {
    use crate::remote_state::{LandingPending, RemoteScope, State};
    let scope = RemoteScope::new(PathBuf::from("/private/broker.sock"), "example".into()).unwrap();
    let mut state = State::new(&scope);
    let ordinary = toml::to_string(&state).unwrap();
    assert!(ordinary.contains("version = 1"));
    assert!(!ordinary.contains("landing"));
    let intent = LandingPending {
        observed: None,
        restored_lookup_only: false,
        request_id: "12".repeat(16),
        source_base: "00".repeat(32),
        source_head: "13".repeat(32),
        reference: "main".into(),
        bookmark: "main".into(),
        generation: 1,
        expected_head: "14".repeat(32),
        profile_name: "checks".into(),
        profile_version: "v1".into(),
        source_operation: "15".repeat(64),
        source_stack: vec!["13".repeat(32)],
        fork_operation: Some("16".repeat(64)),
        fork_acknowledged: true,
        job_id: Some("17".repeat(32)),
        result_roots: vec!["18".repeat(32)],
        candidate_head: Some("18".repeat(32)),
        apply_operation: Some("19".repeat(64)),
        operation_writing: None,
        acknowledged: false,
        terminal: None,
    };
    state.begin_landing(intent.clone()).unwrap();
    let encoded = toml::to_string(&state).unwrap();
    assert!(encoded.contains("version = 3"));
    assert!(
        remote_state::decode(
            encoded.replace("version = 3", "version = 2").as_bytes(),
            &scope
        )
        .is_ok()
    );
    let decoded = remote_state::decode(encoded.as_bytes(), &scope).unwrap();
    assert_eq!(
        decoded.retained_operations().collect::<Vec<_>>(),
        vec!["15".repeat(64), "16".repeat(64), "19".repeat(64)]
    );
    assert!(
        decoded
            .retained_commit_heads()
            .any(|id| id == "13".repeat(32))
    );
    assert!(
        decoded
            .retained_commit_heads()
            .any(|id| id == "18".repeat(32))
    );
    assert!(
        !decoded
            .retained_commit_heads()
            .any(|id| id == "00".repeat(32))
    );
    assert!(
        remote_state::decode(
            encoded.replace("version = 3", "version = 1").as_bytes(),
            &scope
        )
        .is_err()
    );
    assert!(
        state.begin_landing(intent.clone()).is_err(),
        "uncertain intent cannot be replaced"
    );
    let portable = tinytree_cloud::PortableRemoteState {
        observations: vec![],
        archived_pushes: vec![],
        landing: Some(intent.clone()),
    };
    let destination =
        RemoteScope::new(PathBuf::from("/another/broker.sock"), "example".into()).unwrap();
    let restored = State::from_portable(&destination, &portable).unwrap();
    assert!(restored.landing.as_ref().unwrap().restored_lookup_only);
    assert_eq!(
        restored.landing.as_ref().unwrap().request_id,
        intent.request_id
    );
    assert_eq!(restored.landing.as_ref().unwrap().job_id, intent.job_id);
    assert!(
        remote_state::decode(toml::to_string(&restored).unwrap().as_bytes(), &scope).is_err(),
        "restored sidecar binds current destination, not source socket"
    );
    let mut duplicate = intent.clone();
    duplicate.source_stack.push(duplicate.source_head.clone());
    assert!(State::new(&scope).begin_landing(duplicate).is_err());
    let mut bad = intent;
    bad.profile_name = "not.allowed".into();
    assert!(State::new(&scope).begin_landing(bad).is_err());
}

fn resolution_admission() -> Arc<landing_resolution::ReadAdmission> {
    let limits = landing_resolution::WorkingLimits::default();
    let bytes = limits.required_read_bytes().unwrap();
    let pool = Arc::new(tokio::sync::Semaphore::new(bytes));
    let permit = Arc::new(
        pool.try_acquire_many_owned(bytes.try_into().unwrap())
            .unwrap(),
    );
    landing_resolution::ReadAdmission::new(limits, bytes, permit).unwrap()
}
async fn resolution_initialize(
    fixture: &Fixture,
) -> (
    Workspace,
    Arc<ReadonlyRepo>,
    Arc<jj_native_backend::DirectObjectIo>,
) {
    let shared = std::cell::RefCell::new(None);
    let initializer = |_: &UserSettings,
                       path: &Path|
     -> std::result::Result<Box<dyn Backend>, BackendInitError> {
        let objects = Arc::new(
            jj_native_backend::DirectObjectIo::init(path, BackendConfig::default())
                .map_err(|error| BackendInitError(Box::new(error)))?,
        );
        *shared.borrow_mut() = Some(objects.clone());
        let backend = std::thread::spawn(move || {
            futures::executor::block_on(NativeBackend::from_io(objects, BackendConfig::default()))
        })
        .join()
        .unwrap()
        .map_err(|error| BackendInitError(Box::new(error)))?;
        Ok(Box::new(backend))
    };
    let (workspace, repo) = initialize(
        &fixture.registry,
        &fixture.binding,
        &fixture.settings,
        &initializer,
        Signer::new(None, vec![]),
        fixture.connector.clone(),
        &CommitId::from_bytes(&[0; 32]),
        WorkspaceName::DEFAULT.to_owned(),
    )
    .await
    .unwrap();
    let objects = shared.into_inner().unwrap();
    (workspace, repo, objects)
}
async fn resolution_proofs(
    binding: &Binding,
    objects: &jj_native_backend::DirectObjectIo,
    admission: &Arc<landing_resolution::ReadAdmission>,
) -> Vec<landing_resolution::CommitProof> {
    let (state, _) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 4 * 1024 * 1024)
            .unwrap();
    let store = jj_lib::simple_op_store::SimpleOpStore::load(
        &binding.repo.join("op_store"),
        jj_lib::op_store::RootOperationData {
            root_commit_id: CommitId::from_bytes(&[0; 32]),
        },
    );
    let operation = store
        .read_operation_bounded(&state.operation, 4 * 1024 * 1024, 128 * 1024 * 1024)
        .unwrap();
    let view = store
        .read_view_bounded(&operation.view_id, 4 * 1024 * 1024, 128 * 1024 * 1024)
        .unwrap();
    let commit = view.wc_commit_ids.get(&state.workspace).unwrap();
    if commit.as_bytes() == [0; 32] {
        return vec![landing_resolution::CommitProof::root(admission).unwrap()];
    }
    let value = objects
        .read_metadata(tinytree_model::ObjectKey {
            kind: tinytree_model::Kind::Commit,
            id: tinytree_model::ObjectId(commit.as_bytes().try_into().unwrap()),
        })
        .await
        .unwrap();
    vec![landing_resolution::CommitProof::from_metadata(&value, &admission).unwrap()]
}
fn resolution_context(fixture: &Fixture) -> landing_resolution::Context {
    landing_resolution::Context {
        provider_identity: [9; 32],
        scope: remote_state::RemoteScope::new(
            fixture.binding.admin.join("broker.sock"),
            fixture.binding.repository.clone(),
        )
        .unwrap(),
        request_id: hex(&[1; 16]),
        request_digest: [2; 32],
        purpose: landing_resolution::Purpose::Apply {
            job_id: hex(&[3; 32]),
            candidate_head: hex(&[4; 32]),
            candidate_digest: [5; 32],
        },
    }
}
async fn resolution_operation(
    fixture: &Fixture,
    journal: &landing_resolution::Journal,
    tagged: bool,
) -> OperationId {
    use jj_lib::op_store::OpStore as _;
    let store = jj_lib::simple_op_store::SimpleOpStore::load(
        &fixture.binding.repo.join("op_store"),
        jj_lib::op_store::RootOperationData {
            root_commit_id: CommitId::from_bytes(&[0; 32]),
        },
    );
    let mut raw = store
        .read_operation_bounded(
            &OperationId::try_from_hex(journal.base().operation()).unwrap(),
            4 * 1024 * 1024,
            128 * 1024 * 1024,
        )
        .unwrap();
    raw.parents = vec![OperationId::try_from_hex(journal.base().operation()).unwrap()];
    raw.commit_predecessors = Some(Default::default());
    raw.metadata.description = "resolve landing locally".into();
    if tagged {
        raw.metadata
            .attributes
            .extend(journal.operation_attributes());
    }
    store.write_operation(&raw).await.unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_resolution_journal_reuses_nonce_and_tracks_only_exact_owned_progress() {
    use jj_lib::op_heads_store::OpHeadsStore as _;
    use landing_resolution::Position;
    for checkout_first in [false, true] {
        let fixture = Fixture::new();
        let (mut workspace, _, objects) = resolution_initialize(&fixture).await;
        let admission = resolution_admission();
        let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
        assert!(
            landing_resolution::CommitProof::verify([9; 32], b"invalid native commit", &admission)
                .is_err()
        );
        assert!(
            landing_resolution::CommitProof::verify(
                [9; 32],
                &vec![0; 4 * 1024 * 1024 + 1],
                &admission
            )
            .is_err()
        );
        assert!(
            fixture
                .registry
                .landing_resolution_base(&fixture.binding, &[], &admission)
                .await
                .is_err()
        );
        let context = resolution_context(&fixture);
        let base = fixture
            .registry
            .landing_resolution_base(&fixture.binding, &proofs, &admission)
            .await
            .unwrap();
        assert!(
            fixture
                .registry
                .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
                .await
                .unwrap()
                .is_none()
        );
        let journal = fixture
            .registry
            .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
            .await
            .unwrap();
        assert_ne!(journal.nonce(), [0; 32]);
        assert_eq!(journal.position(), Position::AtBase);
        assert_eq!(
            fixture
                .registry
                .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
                .await
                .unwrap()
                .nonce(),
            journal.nonce()
        );
        let operation = resolution_operation(&fixture, &journal, true).await;
        let untagged = resolution_operation(&fixture, &journal, false).await;
        assert!(
            fixture
                .registry
                .record_landing_resolution_operation(
                    &fixture.binding,
                    &context,
                    journal.nonce(),
                    &untagged.hex(),
                    &proofs,
                    &admission
                )
                .await
                .is_err()
        );
        assert!(
            fixture
                .registry
                .record_landing_resolution_operation(
                    &fixture.binding,
                    &context,
                    [33; 32],
                    &operation.hex(),
                    &proofs,
                    &admission
                )
                .await
                .is_err()
        );
        let written = fixture
            .registry
            .record_landing_resolution_operation(
                &fixture.binding,
                &context,
                journal.nonce(),
                &operation.hex(),
                &proofs,
                &admission,
            )
            .await
            .unwrap();
        assert_eq!(written.position(), Position::OperationWritten);
        assert_eq!(written.owned_operation(), Some(operation.hex().as_str()));
        assert_eq!(
            fixture
                .registry
                .record_landing_resolution_operation(
                    &fixture.binding,
                    &context,
                    journal.nonce(),
                    &operation.hex(),
                    &proofs,
                    &admission
                )
                .await
                .unwrap()
                .nonce(),
            journal.nonce()
        );
        assert!(
            fixture
                .registry
                .record_landing_resolution_operation(
                    &fixture.binding,
                    &context,
                    journal.nonce(),
                    &untagged.hex(),
                    &proofs,
                    &admission
                )
                .await
                .is_err()
        );
        assert!(
            fixture
                .registry
                .complete_landing_resolution(
                    &fixture.binding,
                    &context,
                    journal.nonce(),
                    &proofs,
                    &admission
                )
                .await
                .is_err()
        );
        let heads = jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(
            &fixture.binding.repo.join("op_heads"),
        );
        let parent = OperationId::try_from_hex(base.operation()).unwrap();
        if checkout_first {
            workspace
                .start_working_copy_mutation()
                .await
                .unwrap()
                .finish(operation.clone())
                .await
                .unwrap();
        } else {
            heads
                .update_op_heads(std::slice::from_ref(&parent), &operation)
                .await
                .unwrap();
        }
        let partial = fixture
            .registry
            .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            partial.position(),
            if checkout_first {
                Position::WorkingCopyAdvanced
            } else {
                Position::OperationPublished
            }
        );
        assert_eq!(
            fixture
                .registry
                .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
                .await
                .unwrap()
                .nonce(),
            journal.nonce()
        );
        if checkout_first {
            heads.update_op_heads(&[], &operation).await.unwrap();
            assert_eq!(
                fixture
                    .registry
                    .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
                    .await
                    .unwrap()
                    .unwrap()
                    .position(),
                Position::RecoveryRequired
            );
            assert!(
                fixture
                    .registry
                    .complete_landing_resolution(
                        &fixture.binding,
                        &context,
                        journal.nonce(),
                        &proofs,
                        &admission
                    )
                    .await
                    .is_err()
            );
            heads
                .update_op_heads(std::slice::from_ref(&parent), &operation)
                .await
                .unwrap();
        } else {
            workspace
                .start_working_copy_mutation()
                .await
                .unwrap()
                .finish(operation.clone())
                .await
                .unwrap();
        }
        assert_eq!(
            fixture
                .registry
                .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
                .await
                .unwrap()
                .unwrap()
                .position(),
            Position::Applied
        );
        let completed = fixture
            .registry
            .complete_landing_resolution(
                &fixture.binding,
                &context,
                journal.nonce(),
                &proofs,
                &admission,
            )
            .await
            .unwrap();
        assert_eq!(completed.position(), Position::Completed);
        assert_eq!(
            fixture
                .registry
                .complete_landing_resolution(
                    &fixture.binding,
                    &context,
                    journal.nonce(),
                    &proofs,
                    &admission
                )
                .await
                .unwrap()
                .nonce(),
            journal.nonce()
        );
        let reopened = Registry::open_existing(fixture.registry.path()).unwrap();
        assert_eq!(
            reopened
                .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
                .await
                .unwrap()
                .unwrap()
                .position(),
            Position::Completed
        );
        assert_eq!(
            fs::read_dir(fixture.registry.path().join("records"))
                .unwrap()
                .count(),
            1
        );
        assert!(!fixture.binding.admin.join("landing-resolutions").exists());
        assert!(
            !fixture
                .binding
                .working_copy
                .join("landing-resolutions")
                .exists()
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_resolution_context_mismatches_preserve_original_attempt() {
    let fixture = Fixture::new();
    let (_workspace, _, objects) = resolution_initialize(&fixture).await;
    let admission = resolution_admission();
    let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
    let context = resolution_context(&fixture);
    let base = fixture
        .registry
        .landing_resolution_base(&fixture.binding, &proofs, &admission)
        .await
        .unwrap();
    let journal = fixture
        .registry
        .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
        .await
        .unwrap();
    let path = fixture
        .registry
        .path()
        .join("landing-resolutions")
        .join(fixture.binding.id())
        .join(format!("{}-apply.json", context.request_id));
    let original = fs::read(&path).unwrap();
    let mut cases = Vec::new();
    let mut changed = context.clone();
    changed.provider_identity = [8; 32];
    cases.push(("provider", changed));
    let mut changed = context.clone();
    changed.scope.socket = fixture.binding.admin.join("other.sock");
    cases.push(("socket", changed));
    let mut changed = context.clone();
    changed.scope.store = "other".into();
    cases.push(("store", changed));
    let mut changed = context.clone();
    changed.request_digest = [8; 32];
    cases.push(("original request", changed));
    let mut changed = context.clone();
    if let landing_resolution::Purpose::Apply { job_id, .. } = &mut changed.purpose {
        *job_id = hex(&[8; 32]);
    }
    cases.push(("job", changed));
    let mut changed = context.clone();
    if let landing_resolution::Purpose::Apply { candidate_head, .. } = &mut changed.purpose {
        *candidate_head = hex(&[8; 32]);
    }
    cases.push(("candidate head", changed));
    let mut changed = context.clone();
    if let landing_resolution::Purpose::Apply {
        candidate_digest, ..
    } = &mut changed.purpose
    {
        *candidate_digest = [8; 32];
    }
    cases.push(("complete candidate", changed));
    for (name, changed) in cases {
        assert!(
            fixture
                .registry
                .load_landing_resolution(&fixture.binding, &changed, &proofs, &admission)
                .await
                .is_err(),
            "{name}"
        );
        assert!(
            fixture
                .registry
                .begin_landing_resolution(&fixture.binding, &changed, &base, &proofs, &admission)
                .await
                .is_err(),
            "{name}"
        );
        assert_eq!(fs::read(&path).unwrap(), original, "{name}");
    }
    // Opaque immutable Base values cannot be caller-overwritten. Exercise the
    // corresponding host-record corruption boundary through the public API.
    let base_cases = [
        ("operation", serde_json::json!(hex(&[8; 64]))),
        ("working_copy_commit", serde_json::json!(hex(&[8; 32]))),
        (
            "tree",
            serde_json::json!({"terms":vec![[8;32]],"labels":[]}),
        ),
        (
            "projection_generation",
            serde_json::json!(base.projection_generation() + 1),
        ),
        ("state_fingerprint", serde_json::json!(hex(&[8; 64]))),
    ];
    let directory = PrivateDir::open_existing(path.parent().unwrap()).unwrap();
    for (field, changed) in base_cases {
        let mut record: serde_json::Value = serde_json::from_slice(&original).unwrap();
        record["base"][field] = changed;
        let malformed = serde_json::to_vec(&record).unwrap();
        directory
            .replace(path.file_name().unwrap(), &malformed)
            .unwrap();
        assert!(
            fixture
                .registry
                .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
                .await
                .is_err(),
            "{field}"
        );
        assert!(
            fixture
                .registry
                .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
                .await
                .is_err(),
            "{field}"
        );
        assert_eq!(fs::read(&path).unwrap(), malformed, "{field}");
    }
    directory
        .replace(path.file_name().unwrap(), &original)
        .unwrap();
    let mut changed_binding = fixture.binding.clone();
    changed_binding.workspace_id = [8; 32];
    assert!(
        fixture
            .registry
            .load_landing_resolution(&changed_binding, &context, &proofs, &admission)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), original);
    assert_eq!(
        fixture
            .registry
            .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
            .await
            .unwrap()
            .unwrap()
            .nonce(),
        journal.nonce()
    );
    let mut changed_request = context.clone();
    changed_request.request_id = hex(&[8; 16]);
    assert!(
        fixture
            .registry
            .load_landing_resolution(&fixture.binding, &changed_request, &proofs, &admission)
            .await
            .unwrap()
            .is_none()
    );
    // A different new host identity has no access to this workspace's record.
    let mut restored = Fixture::new();
    fs::write(restored.binding.root.join(MARKER), hex(&[8; 32])).unwrap();
    restored.binding = restored
        .registry
        .binding(
            [8; 32],
            &restored.binding.root,
            "fixture".into(),
            restored.binding.endpoint.local(),
            "fresh-workspace".into(),
        )
        .unwrap();
    restored.initialize().await.unwrap();
    assert!(
        restored
            .registry
            .load_landing_resolution(&restored.binding, &context, &proofs, &admission)
            .await
            .unwrap()
            .is_none()
    );
    // A changed live marker is never repaired or adopted by the journal API.
    fs::write(fixture.binding.root.join(MARKER), hex(&[8; 32])).unwrap();
    assert!(
        fixture
            .registry
            .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), original);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_resolution_rejects_malformed_oversize_and_linked_records_without_repair() {
    let fixture = Fixture::new();
    let (_workspace, _, objects) = resolution_initialize(&fixture).await;
    let admission = resolution_admission();
    let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
    let context = resolution_context(&fixture);
    let base = fixture
        .registry
        .landing_resolution_base(&fixture.binding, &proofs, &admission)
        .await
        .unwrap();
    fixture
        .registry
        .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
        .await
        .unwrap();
    let directory = PrivateDir::open_existing(
        fixture
            .registry
            .path()
            .join("landing-resolutions")
            .join(fixture.binding.id()),
    )
    .unwrap();
    let name = format!("{}-apply.json", context.request_id);
    for malformed in [b"{".to_vec(), vec![b' '; 64 * 1024 + 1]] {
        directory.replace(&name, &malformed).unwrap();
        assert!(
            fixture
                .registry
                .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
                .await
                .is_err()
        );
        assert!(
            fixture
                .registry
                .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
                .await
                .is_err()
        );
        assert!(
            fixture
                .registry
                .landing_resolution_presence(
                    &fixture.binding,
                    &resolution_common(&context),
                    &admission
                )
                .await
                .is_err()
        );
        assert_eq!(fs::read(directory.path().join(&name)).unwrap(), malformed);
    }
    #[cfg(unix)]
    {
        let target = fixture.registry.path().join("untrusted.json");
        fs::write(&target, b"preserve target").unwrap();
        fs::remove_file(directory.path().join(&name)).unwrap();
        std::os::unix::fs::symlink(&target, directory.path().join(&name)).unwrap();
        assert!(
            fixture
                .registry
                .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
                .await
                .is_err()
        );
        assert!(
            fixture
                .registry
                .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
                .await
                .is_err()
        );
        assert!(
            fs::symlink_metadata(directory.path().join(&name))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            fixture
                .registry
                .landing_resolution_presence(
                    &fixture.binding,
                    &resolution_common(&context),
                    &admission
                )
                .await
                .is_err()
        );
        assert_eq!(fs::read(target).unwrap(), b"preserve target");
    }
}

fn resolution_common(context: &landing_resolution::Context) -> landing_resolution::CommonContext {
    landing_resolution::CommonContext {
        provider_identity: context.provider_identity,
        scope: context.scope.clone(),
        request_id: context.request_id.clone(),
        request_digest: context.request_digest,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_resolution_presence_needs_no_source_proof_and_preserves_mismatches() {
    use landing_resolution::{ReadAdmission, Stage, WorkingLimits};
    let fixture = Fixture::new();
    let (_workspace, _, objects) = resolution_initialize(&fixture).await;
    let context = resolution_context(&fixture);
    let common = resolution_common(&context);
    let limits = WorkingLimits::default();
    let bytes = limits.required_read_bytes().unwrap();
    let pool = Arc::new(tokio::sync::Semaphore::new(bytes));
    let owner = Arc::new(
        pool.clone()
            .try_acquire_many_owned(bytes.try_into().unwrap())
            .unwrap(),
    );
    let admission = ReadAdmission::new(limits, bytes, owner).unwrap();
    let absent = fixture
        .registry
        .landing_resolution_presence(&fixture.binding, &common, &admission)
        .await
        .unwrap();
    assert!(absent.is_absent());
    assert!(!fixture.registry.path().join("landing-resolutions").exists());
    drop(admission);
    assert_eq!(
        pool.available_permits(),
        bytes,
        "value-only presence retains no reservation"
    );
    let admission = resolution_admission();
    let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
    let base = fixture
        .registry
        .landing_resolution_base(&fixture.binding, &proofs, &admission)
        .await
        .unwrap();
    fixture
        .registry
        .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
        .await
        .unwrap();
    drop(base);
    drop(proofs);
    let present = fixture
        .registry
        .landing_resolution_presence(&fixture.binding, &common, &admission)
        .await
        .unwrap();
    assert_eq!(present.apply(), Some(Stage::Prepared));
    assert_eq!(present.freeze(), None);
    let path = fixture
        .registry
        .path()
        .join("landing-resolutions")
        .join(fixture.binding.id())
        .join(format!("{}-apply.json", common.request_id));
    let original = fs::read(&path).unwrap();
    let mut provider = common.clone();
    provider.provider_identity[0] ^= 1;
    let mut scope = common.clone();
    scope.scope.store = "other-store".into();
    let mut digest = common.clone();
    digest.request_digest = [9; 32];
    for changed in [provider, scope, digest] {
        assert!(
            fixture
                .registry
                .landing_resolution_presence(&fixture.binding, &changed, &admission)
                .await
                .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), original);
    }
    let directory = PrivateDir::open_existing(path.parent().unwrap()).unwrap();
    for (field, pointer, replacement) in [
        ("zero nonce", "/nonce", serde_json::json!(vec![0; 32])),
        (
            "wrong purpose file",
            "/context/purpose",
            serde_json::json!({"purpose":"freeze", "observed_job":null}),
        ),
        (
            "malformed written stage",
            "/stage",
            serde_json::json!({"stage":"written", "operation":"invalid"}),
        ),
        (
            "wrong workspace",
            "/binding/workspace_id",
            serde_json::json!(vec![0; 32]),
        ),
    ] {
        let mut malformed: serde_json::Value = serde_json::from_slice(&original).unwrap();
        *malformed.pointer_mut(pointer).unwrap() = replacement;
        let encoded = serde_json::to_vec(&malformed).unwrap();
        directory
            .replace(path.file_name().unwrap(), &encoded)
            .unwrap();
        assert!(
            fixture
                .registry
                .landing_resolution_presence(&fixture.binding, &common, &admission)
                .await
                .is_err(),
            "{field}"
        );
        assert_eq!(fs::read(&path).unwrap(), encoded);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_freeze_resolution_does_not_fabricate_or_adopt_an_apply_candidate() {
    use jj_lib::op_heads_store::OpHeadsStore as _;
    use landing_resolution::{Position, Purpose};
    for observed_job in [None, Some(hex(&[3; 32]))] {
        let fixture = Fixture::new();
        let (mut workspace, _, objects) = resolution_initialize(&fixture).await;
        let admission = resolution_admission();
        let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
        let apply = resolution_context(&fixture);
        let mut freeze = apply.clone();
        freeze.purpose = Purpose::Freeze {
            observed_job: observed_job.clone(),
        };
        let common = resolution_common(&freeze);
        assert_eq!(
            fixture
                .registry
                .freeze_landing_resolution_selection(&fixture.binding, &common, &admission)
                .await
                .unwrap(),
            None
        );
        assert!(!fixture.registry.path().join("landing-resolutions").exists());
        let base = fixture
            .registry
            .landing_resolution_base(&fixture.binding, &proofs, &admission)
            .await
            .unwrap();
        let journal = fixture
            .registry
            .begin_landing_resolution(&fixture.binding, &freeze, &base, &proofs, &admission)
            .await
            .unwrap();
        let apply_journal = fixture
            .registry
            .begin_landing_resolution(&fixture.binding, &apply, &base, &proofs, &admission)
            .await
            .unwrap();
        assert_ne!(journal.nonce(), apply_journal.nonce());
        let presence = fixture
            .registry
            .landing_resolution_presence(&fixture.binding, &common, &admission)
            .await
            .unwrap();
        assert_eq!(presence.freeze(), Some(landing_resolution::Stage::Prepared));
        assert_eq!(presence.apply(), Some(landing_resolution::Stage::Prepared));
        let selection = fixture
            .registry
            .freeze_landing_resolution_selection(&fixture.binding, &common, &admission)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selection.stage(), landing_resolution::Stage::Prepared);
        assert_eq!(
            selection.observed_job(),
            observed_job.as_ref().map(|_| [3; 32])
        );
        let mut changed = common.clone();
        changed.request_digest[0] ^= 1;
        assert!(
            fixture
                .registry
                .freeze_landing_resolution_selection(&fixture.binding, &changed, &admission)
                .await
                .is_err()
        );
        let attributes = journal.operation_attributes();
        assert_eq!(
            attributes.get("tinytree.landing.job"),
            observed_job.as_ref()
        );
        assert!(!attributes.contains_key("tinytree.landing.candidate"));
        assert!(!attributes.contains_key("tinytree.landing.resolution_candidate"));
        let operation = resolution_operation(&fixture, &journal, true).await;
        assert!(
            fixture
                .registry
                .record_landing_resolution_operation(
                    &fixture.binding,
                    &apply,
                    apply_journal.nonce(),
                    &operation.hex(),
                    &proofs,
                    &admission
                )
                .await
                .is_err()
        );
        fixture
            .registry
            .record_landing_resolution_operation(
                &fixture.binding,
                &freeze,
                journal.nonce(),
                &operation.hex(),
                &proofs,
                &admission,
            )
            .await
            .unwrap();
        assert_eq!(
            fixture
                .registry
                .landing_resolution_presence(&fixture.binding, &common, &admission)
                .await
                .unwrap()
                .freeze(),
            Some(landing_resolution::Stage::Written)
        );
        assert!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(
                    &fixture.binding,
                    &freeze,
                    journal.nonce(),
                    &admission
                )
                .await
                .is_err()
        );
        workspace
            .start_working_copy_mutation()
            .await
            .unwrap()
            .finish(operation.clone())
            .await
            .unwrap();
        jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(
            &fixture.binding.repo.join("op_heads"),
        )
        .update_op_heads(
            &[OperationId::try_from_hex(base.operation()).unwrap()],
            &operation,
        )
        .await
        .unwrap();
        let completed = fixture
            .registry
            .complete_landing_resolution(
                &fixture.binding,
                &freeze,
                journal.nonce(),
                &proofs,
                &admission,
            )
            .await
            .unwrap();
        assert_eq!(completed.position(), Position::Completed);
        assert_eq!(
            fixture
                .registry
                .landing_resolution_presence(&fixture.binding, &common, &admission)
                .await
                .unwrap()
                .freeze(),
            Some(landing_resolution::Stage::Completed)
        );
        assert_eq!(completed.purpose(), &freeze.purpose);
        let selection = fixture
            .registry
            .freeze_landing_resolution_selection(&fixture.binding, &common, &admission)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selection.stage(), landing_resolution::Stage::Completed);
        assert_eq!(
            selection.observed_job(),
            observed_job.as_ref().map(|_| [3; 32])
        );
        assert!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(
                    &fixture.binding,
                    &freeze,
                    journal.nonce(),
                    &admission
                )
                .await
                .is_err()
        );
        assert!(
            fixture
                .registry
                .load_landing_resolution(&fixture.binding, &apply, &proofs, &admission)
                .await
                .is_err(),
            "a completed freeze cannot promote the independent apply attempt"
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolution_read_reservation_bounds_live_proofs_and_outlives_returned_results() {
    use landing_resolution::{CommitProof, ReadAdmission, WorkingLimits};
    let fixture = Fixture::new();
    let (_workspace, _, objects) = resolution_initialize(&fixture).await;
    let limits = WorkingLimits::default();
    let bytes = limits.required_read_bytes().unwrap();
    let pool = Arc::new(tokio::sync::Semaphore::new(bytes));
    let owner = Arc::new(
        pool.clone()
            .try_acquire_many_owned(bytes.try_into().unwrap())
            .unwrap(),
    );
    assert!(ReadAdmission::new(limits.clone(), bytes - 1, owner.clone()).is_err());
    assert!(!fixture.registry.path().join("landing-resolutions").exists());
    let admission = ReadAdmission::new(limits, bytes, owner.clone()).unwrap();
    drop(owner);
    let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
    let extra = CommitProof::root(&admission).unwrap();
    assert!(
        CommitProof::root(&admission).is_err(),
        "live proof count is charged before decoding"
    );
    drop(extra);
    let extra = CommitProof::root(&admission).unwrap();
    drop(extra);
    let base = fixture
        .registry
        .landing_resolution_base(&fixture.binding, &proofs, &admission)
        .await
        .unwrap();
    let journal = fixture
        .registry
        .begin_landing_resolution(
            &fixture.binding,
            &resolution_context(&fixture),
            &base,
            &proofs,
            &admission,
        )
        .await
        .unwrap();
    drop(proofs);
    drop(admission);
    assert_eq!(
        pool.available_permits(),
        0,
        "returned base/journal retain the physical memory reservation"
    );
    let shared_base = base.clone();
    let shared_journal = journal.clone();
    drop(base);
    drop(journal);
    assert_eq!(
        pool.available_permits(),
        0,
        "cheap immutable clones keep the same charged payload alive"
    );
    drop(shared_journal);
    assert_eq!(pool.available_permits(), 0);
    drop(shared_base);
    assert_eq!(pool.available_permits(), bytes);

    // Independently charge cumulative canonical input bytes, not just each
    // object's cap or the count. One dropped proof releases its exact charge.
    let mut commit = tinytree_model::synthetic_root();
    commit.parents = vec![tinytree_model::CommitId::ROOT];
    commit.change_id = tinytree_model::ChangeId([9; 16]);
    commit.description = "bounded canonical proof fixture".into();
    let encoded =
        tinytree_model::encode(&tinytree_model::Object::Commit(commit), Default::default())
            .unwrap();
    let mut limits = WorkingLimits::default();
    limits.max_proof_bytes = encoded.bytes.len();
    limits.max_proof_input_bytes = encoded.bytes.len();
    let bytes = limits.required_read_bytes().unwrap();
    let pool = Arc::new(tokio::sync::Semaphore::new(bytes));
    let owner = Arc::new(
        pool.clone()
            .try_acquire_many_owned(bytes.try_into().unwrap())
            .unwrap(),
    );
    let admission = ReadAdmission::new(limits, bytes, owner).unwrap();
    let proof =
        CommitProof::verify(encoded.descriptor.key.id.0, &encoded.bytes, &admission).unwrap();
    assert!(CommitProof::verify(encoded.descriptor.key.id.0, &encoded.bytes, &admission).is_err());
    drop(proof);
    let proof =
        CommitProof::verify(encoded.descriptor.key.id.0, &encoded.bytes, &admission).unwrap();
    drop(admission);
    assert_eq!(pool.available_permits(), 0);
    drop(proof);
    assert_eq!(pool.available_permits(), bytes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_resolution_selects_exact_proofs_after_owned_op_before_sidecar_crash() {
    use jj_lib::op_store::OpStore as _;
    use landing_resolution::{CommitProof, Position};
    let fixture = Fixture::new();
    let (_workspace, repo, objects) = resolution_initialize(&fixture).await;
    let admission = resolution_admission();
    let context = resolution_context(&fixture);
    let mut proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
    let base = fixture
        .registry
        .landing_resolution_base(&fixture.binding, &proofs, &admission)
        .await
        .unwrap();
    let journal = fixture
        .registry
        .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
        .await
        .unwrap();
    let current = repo
        .store()
        .get_commit_async(&CommitId::try_from_hex(base.working_copy_commit()).unwrap())
        .await
        .unwrap();
    let mut contents = current.store_commit().as_ref().clone();
    contents.description = "host-owned retry result before sidecar persistence".into();
    contents.change_id = jj_lib::backend::ChangeId::from_bytes(&[19; 16]);
    let result = repo.store().write_commit(contents, None).await.unwrap();
    let store = jj_lib::simple_op_store::SimpleOpStore::load(
        &fixture.binding.repo.join("op_store"),
        jj_lib::op_store::RootOperationData {
            root_commit_id: CommitId::from_bytes(&[0; 32]),
        },
    );
    let mut operation = store
        .read_operation_bounded(
            &OperationId::try_from_hex(base.operation()).unwrap(),
            4 * 1024 * 1024,
            128 * 1024 * 1024,
        )
        .unwrap();
    let mut view = store
        .read_view_bounded(&operation.view_id, 4 * 1024 * 1024, 128 * 1024 * 1024)
        .unwrap();
    view.wc_commit_ids
        .insert(WorkspaceName::DEFAULT.to_owned(), result.id().clone());
    operation.view_id = store.write_view(&view).await.unwrap();
    operation.parents = vec![OperationId::try_from_hex(base.operation()).unwrap()];
    operation.commit_predecessors = Some(Default::default());
    operation
        .metadata
        .attributes
        .extend(journal.operation_attributes());
    let owned = store.write_operation(&operation).await.unwrap();
    let value = objects
        .read_metadata_with_owner(
            tinytree_model::ObjectKey {
                kind: tinytree_model::Kind::Commit,
                id: tinytree_model::ObjectId(result.id().as_bytes().try_into().unwrap()),
            },
            admission.clone(),
        )
        .await
        .unwrap();
    proofs.push(CommitProof::from_metadata(&value, &admission).unwrap());
    drop(value);
    fixture
        .registry
        .record_landing_resolution_operation(
            &fixture.binding,
            &context,
            journal.nonce(),
            &owned.hex(),
            &proofs,
            &admission,
        )
        .await
        .unwrap();
    drop(proofs);
    drop(journal);
    // No portable sidecar operation/result claim is supplied on retry. Only the
    // fresh actual WC and host-created journal may select metadata for reading.
    let reopened = Registry::open_existing(fixture.registry.path()).unwrap();
    let selectors = reopened
        .resolution_proof_ids(&fixture.binding, &context, &admission)
        .await
        .unwrap();
    let old_id: [u8; 32] = CommitId::try_from_hex(base.working_copy_commit())
        .unwrap()
        .as_bytes()
        .try_into()
        .unwrap();
    let result_id: [u8; 32] = result.id().as_bytes().try_into().unwrap();
    assert_eq!(selectors.ids(), &[old_id, result_id]);
    let mut restored_proofs = Vec::new();
    for id in selectors.ids() {
        let value = objects
            .read_metadata_with_owner(
                tinytree_model::ObjectKey {
                    kind: tinytree_model::Kind::Commit,
                    id: tinytree_model::ObjectId(*id),
                },
                admission.clone(),
            )
            .await
            .unwrap();
        restored_proofs.push(CommitProof::from_metadata(&value, &admission).unwrap());
    }
    let resumed = reopened
        .load_landing_resolution(&fixture.binding, &context, &restored_proofs, &admission)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.owned_operation(), Some(owned.hex().as_str()));
    assert_eq!(resumed.position(), Position::OperationWritten);
    let path = fixture
        .registry
        .path()
        .join("landing-resolutions")
        .join(fixture.binding.id())
        .join(format!("{}-apply.json", context.request_id));
    let original = fs::read(&path).unwrap();
    let mut changed = context.clone();
    changed.request_digest = [99; 32];
    assert!(
        reopened
            .resolution_proof_ids(&fixture.binding, &changed, &admission)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), original);
    // A syntactically valid alternate result in the host record still cannot
    // select an ID absent from the exact nonce-stamped owned OP's view.
    let mut malformed: serde_json::Value = serde_json::from_slice(&original).unwrap();
    malformed["stage"]["working_copy_commit"] = serde_json::json!(hex(&[99; 32]));
    let malformed = serde_json::to_vec(&malformed).unwrap();
    PrivateDir::open_existing(path.parent().unwrap())
        .unwrap()
        .replace(path.file_name().unwrap(), &malformed)
        .unwrap();
    assert!(
        reopened
            .resolution_proof_ids(&fixture.binding, &context, &admission)
            .await
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), malformed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_replacement_preserves_prior_attempt_and_never_adopts_its_orphan() {
    use jj_lib::op_heads_store::OpHeadsStore as _;
    use landing_resolution::{Position, Stage};
    let fixture = Fixture::new();
    let (mut workspace, _, objects) = resolution_initialize(&fixture).await;
    let admission = resolution_admission();
    let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
    let context = resolution_context(&fixture);
    let base = fixture
        .registry
        .landing_resolution_base(&fixture.binding, &proofs, &admission)
        .await
        .unwrap();
    let prior = fixture
        .registry
        .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
        .await
        .unwrap();
    let orphan = resolution_operation(&fixture, &prior, true).await;
    let ordinary = resolution_operation(&fixture, &prior, false).await;
    let directory = PrivateDir::open_existing(
        fixture
            .registry
            .path()
            .join("landing-resolutions")
            .join(fixture.binding.id()),
    )
    .unwrap();
    let active = format!("{}-apply.json", context.request_id);
    let archive = format!(
        "{}-apply-{}-superseded.json",
        context.request_id,
        hex(&prior.nonce())
    );
    let original = directory.read(&active, 65536).unwrap();
    // An ordinary acknowledged operation changes the trusted actual baseline,
    // not the provenance or ownership of the interrupted landing attempt.
    workspace
        .start_working_copy_mutation()
        .await
        .unwrap()
        .finish(ordinary.clone())
        .await
        .unwrap();
    let heads = jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(
        &fixture.binding.repo.join("op_heads"),
    );
    heads
        .update_op_heads(
            &[OperationId::try_from_hex(base.operation()).unwrap()],
            &ordinary,
        )
        .await
        .unwrap();
    assert!(
        fixture
            .registry
            .replace_prepared_landing_resolution(
                &fixture.binding,
                &context,
                prior.nonce(),
                &base,
                &proofs,
                &admission
            )
            .await
            .is_err()
    );
    assert!(
        fixture
            .registry
            .load_landing_resolution(&fixture.binding, &context, &proofs, &admission)
            .await
            .is_err()
    );
    let selected = fixture
        .registry
        .prepared_landing_resolution_nonce(&fixture.binding, &context, &admission)
        .await
        .unwrap();
    assert_eq!(selected.nonce(), prior.nonce());
    let fresh = fixture
        .registry
        .landing_resolution_base(&fixture.binding, &proofs, &admission)
        .await
        .unwrap();
    assert_ne!(fresh.operation(), base.operation());
    assert!(
        fixture
            .registry
            .begin_landing_resolution(&fixture.binding, &context, &fresh, &proofs, &admission)
            .await
            .is_err()
    );
    let mut changed = context.clone();
    changed.request_digest[0] ^= 1;
    assert!(
        fixture
            .registry
            .prepared_landing_resolution_nonce(&fixture.binding, &changed, &admission)
            .await
            .is_err()
    );
    assert!(
        fixture
            .registry
            .replace_prepared_landing_resolution(
                &fixture.binding,
                &changed,
                prior.nonce(),
                &fresh,
                &proofs,
                &admission
            )
            .await
            .is_err()
    );
    assert!(
        fixture
            .registry
            .replace_prepared_landing_resolution(
                &fixture.binding,
                &context,
                [8; 32],
                &fresh,
                &proofs,
                &admission
            )
            .await
            .is_err()
    );
    assert_eq!(directory.read(&active, 65536).unwrap(), original);
    assert!(!directory.path().join(&archive).exists());
    directory
        .replace(&archive, b"unrecognized archive")
        .unwrap();
    assert!(
        fixture
            .registry
            .replace_prepared_landing_resolution(
                &fixture.binding,
                &context,
                prior.nonce(),
                &fresh,
                &proofs,
                &admission
            )
            .await
            .is_err()
    );
    assert_eq!(
        directory.read(&archive, 65536).unwrap(),
        b"unrecognized archive"
    );
    assert_eq!(directory.read(&active, 65536).unwrap(), original);
    // Exact archive-only crash prefix is retryable; the active old record is
    // still authoritative until the new nonce/base swap is durably complete.
    directory.replace(&archive, &original).unwrap();
    let replacement = fixture
        .registry
        .replace_prepared_landing_resolution(
            &fixture.binding,
            &context,
            selected.nonce(),
            &fresh,
            &proofs,
            &admission,
        )
        .await
        .unwrap();
    assert_ne!(replacement.nonce(), prior.nonce());
    assert_eq!(replacement.base(), fresh);
    assert_eq!(replacement.position(), Position::AtBase);
    assert_eq!(directory.read(&archive, 65536).unwrap(), original);
    let replaced = directory.read(&active, 65536).unwrap();
    assert!(
        fixture
            .registry
            .replace_prepared_landing_resolution(
                &fixture.binding,
                &context,
                prior.nonce(),
                &fresh,
                &proofs,
                &admission
            )
            .await
            .is_err()
    );
    assert!(
        fixture
            .registry
            .record_landing_resolution_operation(
                &fixture.binding,
                &context,
                replacement.nonce(),
                &orphan.hex(),
                &proofs,
                &admission
            )
            .await
            .is_err()
    );
    assert_eq!(directory.read(&active, 65536).unwrap(), replaced);
    let operation = resolution_operation(&fixture, &replacement, true).await;
    fixture
        .registry
        .record_landing_resolution_operation(
            &fixture.binding,
            &context,
            replacement.nonce(),
            &operation.hex(),
            &proofs,
            &admission,
        )
        .await
        .unwrap();
    let written = directory.read(&active, 65536).unwrap();
    assert!(
        fixture
            .registry
            .prepared_landing_resolution_nonce(&fixture.binding, &context, &admission)
            .await
            .is_err()
    );
    assert!(
        fixture
            .registry
            .replace_prepared_landing_resolution(
                &fixture.binding,
                &context,
                replacement.nonce(),
                &fresh,
                &proofs,
                &admission
            )
            .await
            .is_err()
    );
    assert_eq!(directory.read(&active, 65536).unwrap(), written);
    workspace
        .start_working_copy_mutation()
        .await
        .unwrap()
        .finish(operation.clone())
        .await
        .unwrap();
    heads
        .update_op_heads(&[ordinary], &operation)
        .await
        .unwrap();
    let completed = fixture
        .registry
        .complete_landing_resolution(
            &fixture.binding,
            &context,
            replacement.nonce(),
            &proofs,
            &admission,
        )
        .await
        .unwrap();
    assert_eq!(completed.stage(), Stage::Completed);
    let finished = directory.read(&active, 65536).unwrap();
    assert!(
        fixture
            .registry
            .prepared_landing_resolution_nonce(&fixture.binding, &context, &admission)
            .await
            .is_err()
    );
    assert!(
        fixture
            .registry
            .replace_prepared_landing_resolution(
                &fixture.binding,
                &context,
                replacement.nonce(),
                &completed.base(),
                &proofs,
                &admission
            )
            .await
            .is_err()
    );
    assert_eq!(directory.read(&active, 65536).unwrap(), finished);
    assert_eq!(directory.read(&archive, 65536).unwrap(), original);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_freeze_retirement_requires_exact_archive_and_preserves_other_records() {
    use landing_resolution::{PreparedRetirement, Purpose};
    for archive_only_crash in [false, true] {
        let fixture = Fixture::new();
        let (_workspace, _, objects) = resolution_initialize(&fixture).await;
        let admission = resolution_admission();
        let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
        let mut freeze = resolution_context(&fixture);
        freeze.purpose = Purpose::Freeze { observed_job: None };
        assert!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(&fixture.binding, &freeze, [8; 32], &admission)
                .await
                .is_err()
        );
        assert!(!fixture.registry.path().join("landing-resolutions").exists());
        let base = fixture
            .registry
            .landing_resolution_base(&fixture.binding, &proofs, &admission)
            .await
            .unwrap();
        let journal = fixture
            .registry
            .begin_landing_resolution(&fixture.binding, &freeze, &base, &proofs, &admission)
            .await
            .unwrap();
        let apply = resolution_context(&fixture);
        fixture
            .registry
            .begin_landing_resolution(&fixture.binding, &apply, &base, &proofs, &admission)
            .await
            .unwrap();
        let directory = PrivateDir::open_existing(
            fixture
                .registry
                .path()
                .join("landing-resolutions")
                .join(fixture.binding.id()),
        )
        .unwrap();
        let active = format!("{}-freeze.json", freeze.request_id);
        let archive = format!(
            "{}-freeze-{}-retired.json",
            freeze.request_id,
            hex(&journal.nonce())
        );
        let original = directory.read(&active, 65536).unwrap();
        directory.replace(&active, b"{").unwrap();
        assert!(
            fixture
                .registry
                .freeze_landing_resolution_selection(
                    &fixture.binding,
                    &resolution_common(&freeze),
                    &admission
                )
                .await
                .is_err()
        );
        assert_eq!(directory.read(&active, 65536).unwrap(), b"{");
        directory.replace(&active, &original).unwrap();
        let apply_name = format!("{}-apply.json", freeze.request_id);
        let apply_bytes = directory.read(&apply_name, 65536).unwrap();
        let mut changed = freeze.clone();
        changed.request_digest[0] ^= 1;
        assert!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(
                    &fixture.binding,
                    &changed,
                    journal.nonce(),
                    &admission
                )
                .await
                .is_err()
        );
        assert!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(&fixture.binding, &freeze, [8; 32], &admission)
                .await
                .is_err()
        );
        assert!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(
                    &fixture.binding,
                    &apply,
                    journal.nonce(),
                    &admission
                )
                .await
                .is_err()
        );
        assert_eq!(directory.read(&active, 65536).unwrap(), original);
        assert!(!directory.path().join(&archive).exists());
        directory.replace(&archive, b"unknown archive").unwrap();
        assert!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(
                    &fixture.binding,
                    &freeze,
                    journal.nonce(),
                    &admission
                )
                .await
                .is_err()
        );
        assert_eq!(directory.read(&active, 65536).unwrap(), original);
        assert_eq!(directory.read(&archive, 65536).unwrap(), b"unknown archive");
        if archive_only_crash {
            directory.replace(&archive, &original).unwrap();
        } else {
            fs::remove_file(directory.path().join(&archive)).unwrap();
        }
        assert_eq!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(
                    &fixture.binding,
                    &freeze,
                    journal.nonce(),
                    &admission
                )
                .await
                .unwrap(),
            PreparedRetirement::Retired
        );
        assert!(!directory.path().join(&active).exists());
        assert_eq!(directory.read(&archive, 65536).unwrap(), original);
        assert_eq!(directory.read(&apply_name, 65536).unwrap(), apply_bytes);
        // Active absence alone is not success; exact archive is revalidated and
        // its durability barrier is repeated on every already-retired retry.
        assert_eq!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(
                    &fixture.binding,
                    &freeze,
                    journal.nonce(),
                    &admission
                )
                .await
                .unwrap(),
            PreparedRetirement::AlreadyRetired
        );
        directory.replace(&archive, b"{").unwrap();
        assert!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(
                    &fixture.binding,
                    &freeze,
                    journal.nonce(),
                    &admission
                )
                .await
                .is_err()
        );
        assert_eq!(directory.read(&archive, 65536).unwrap(), b"{");
        directory.replace(&archive, &original).unwrap();
        assert!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(
                    &fixture.binding,
                    &changed,
                    journal.nonce(),
                    &admission
                )
                .await
                .is_err()
        );
        assert_eq!(directory.read(&archive, 65536).unwrap(), original);
        fs::remove_file(directory.path().join(&archive)).unwrap();
        assert!(
            fixture
                .registry
                .retire_prepared_freeze_resolution(
                    &fixture.binding,
                    &freeze,
                    journal.nonce(),
                    &admission
                )
                .await
                .is_err()
        );
    }
}

fn cleanup_pending(
    base: &landing_resolution::Base,
    request_id: String,
) -> remote_state::LandingPending {
    remote_state::LandingPending {
        observed: None,
        restored_lookup_only: false,
        request_id,
        source_base: hex(&[0; 32]),
        source_head: base.working_copy_commit().into(),
        reference: "main".into(),
        bookmark: "main".into(),
        generation: 1,
        expected_head: hex(&[6; 32]),
        profile_name: "checks".into(),
        profile_version: "v1".into(),
        source_operation: base.operation().into(),
        source_stack: vec![base.working_copy_commit().into()],
        fork_operation: Some(base.operation().into()),
        fork_acknowledged: true,
        job_id: Some(hex(&[3; 32])),
        result_roots: vec![],
        candidate_head: None,
        apply_operation: None,
        operation_writing: None,
        acknowledged: false,
        terminal: None,
    }
}
fn cleanup_intent(p: &remote_state::LandingPending) -> native_broker::LandingIntent {
    let decode = |id: &str| -> Vec<u8> {
        id.as_bytes()
            .chunks_exact(2)
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
            .collect()
    };
    native_broker::LandingIntent {
        request: native_broker::landing_protocol::StartLandingRequest {
            repository_id: String::new(),
            idempotency_key: p.request_id.clone(),
            source_base: decode(&p.source_base),
            source_head: decode(&p.source_head),
            destination: p.reference.clone(),
            expected_generation: p.generation,
            expected_head: decode(&p.expected_head),
            profile_name: p.profile_name.clone(),
            expected_profile_version: p.profile_version.clone(),
        },
        source_stack: p
            .source_stack
            .iter()
            .map(|id| native_broker::CommitId(decode(id).try_into().unwrap()))
            .collect(),
        observed_job: p.job_id.as_deref().map(|id| decode(id).try_into().unwrap()),
    }
}
fn cleanup_context(
    fixture: &Fixture,
    p: &remote_state::LandingPending,
) -> landing_resolution::CommonContext {
    let original = resolution_context(fixture);
    landing_resolution::CommonContext {
        provider_identity: original.provider_identity,
        scope: original.scope,
        request_id: p.request_id.clone(),
        request_digest: landing_resolution::request_digest(&cleanup_intent(p)).unwrap(),
    }
}
fn cleanup_job(
    p: &remote_state::LandingPending,
    tree: [u8; 32],
    committed: bool,
) -> native_broker::landing_protocol::LandingJob {
    use native_broker::landing_protocol as l;
    let mut job = l::LandingJob {
        id: vec![3; 32],
        phase: if committed {
            l::Phase::Committed
        } else {
            l::Phase::Unsupported
        } as i32,
        roots_retained: committed,
        expires_at_seconds: 100,
        profile_name: p.profile_name.clone(),
        profile_version: p.profile_version.clone(),
        candidate: None,
        receipt: None,
        conflict_path: None,
        preparation: l::PreparationAvailability::ProfileConfigured as i32,
    };
    if committed {
        let head = cleanup_intent(p).request.source_head;
        job.candidate = Some(l::Candidate {
            head: head.clone(),
            root_tree: tree.to_vec(),
            rewrites: vec![],
        });
        let mut receipt = l::Receipt {
            reference: Some(Default::default()),
            rewrites: vec![],
            evidence: vec![8; 32],
        };
        let reference = receipt.reference.as_mut().unwrap();
        reference.name = p.reference.clone();
        reference.generation = 2;
        reference.commit = Some(Default::default());
        let descriptor = reference.commit.as_mut().unwrap();
        descriptor.size_bytes = 128;
        descriptor.object_format_version = tinytree_model::FORMAT_VERSION.into();
        descriptor.content_blake3 = head.clone();
        descriptor.key = Some(Default::default());
        let key = descriptor.key.as_mut().unwrap();
        key.kind = 3;
        key.native_id = head;
        job.receipt = Some(receipt);
    }
    job
}
fn save_closed_state(
    binding: &Binding,
    scope: &remote_state::RemoteScope,
    mut p: remote_state::LandingPending,
    job: &native_broker::landing_protocol::LandingJob,
) -> [u8; 32] {
    use std::io::Write as _;
    p.acknowledged = job.phase == native_broker::landing_protocol::Phase::Committed as i32;
    p.terminal = (!p.acknowledged).then(|| "unsupported".into());
    p.observed = Some(remote_state::historical_landing_job(job).unwrap());
    let mut state = remote_state::State::new(scope);
    state.begin_landing(p).unwrap();
    let bytes = toml::to_string(&state).unwrap();
    let mut temporary = tempfile::NamedTempFile::new_in(&binding.working_copy).unwrap();
    temporary.write_all(bytes.as_bytes()).unwrap();
    temporary.as_file().sync_all().unwrap();
    temporary
        .persist(binding.working_copy.join("native-remote.toml"))
        .unwrap();
    fs::File::open(&binding.working_copy)
        .unwrap()
        .sync_all()
        .unwrap();
    *blake3::hash(bytes.as_bytes()).as_bytes()
}
fn cleanup_files(path: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, result: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(root, &entry.path(), result);
            } else {
                result.insert(
                    entry.path().strip_prefix(root).unwrap().to_owned(),
                    fs::read(entry.path()).unwrap(),
                );
            }
        }
    }
    let mut result = Default::default();
    walk(path, path, &mut result);
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_terminal_cleanup_keeps_one_workspace_bounded_beyond_journal_capacity() {
    use landing_resolution::Purpose;
    let fixture = Fixture::new();
    let (_workspace, _, objects) = resolution_initialize(&fixture).await;
    let admission = resolution_admission();
    let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
    let base = fixture
        .registry
        .landing_resolution_base(&fixture.binding, &proofs, &admission)
        .await
        .unwrap();
    let operations = cleanup_files(&fixture.binding.repo.join("op_store"));
    let indexes = cleanup_files(&fixture.binding.repo.join("index"));
    let calls = fixture.connector.calls.load(Ordering::SeqCst);
    for sequence in 1..=257u64 {
        let p = cleanup_pending(&base, format!("{sequence:032x}"));
        let common = cleanup_context(&fixture, &p);
        let context = landing_resolution::Context {
            provider_identity: common.provider_identity,
            scope: common.scope.clone(),
            request_id: common.request_id.clone(),
            request_digest: common.request_digest,
            purpose: Purpose::Freeze { observed_job: None },
        };
        let first = fixture
            .registry
            .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
            .await
            .unwrap();
        let next = fixture
            .registry
            .replace_prepared_landing_resolution(
                &fixture.binding,
                &context,
                first.nonce(),
                &base,
                &proofs,
                &admission,
            )
            .await
            .unwrap();
        fixture
            .registry
            .retire_prepared_freeze_resolution(&fixture.binding, &context, next.nonce(), &admission)
            .await
            .unwrap();
        let job = cleanup_job(&p, base.tree().terms[0], false);
        let saved = save_closed_state(&fixture.binding, &common.scope, p, &job);
        assert_eq!(
            fixture
                .registry
                .cleanup_closed_landing_resolution(
                    &fixture.binding,
                    &common,
                    saved,
                    &job,
                    &admission
                )
                .await
                .unwrap(),
            2
        );
        let namespace = fixture
            .registry
            .path()
            .join("landing-resolutions")
            .join(fixture.binding.id());
        assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
        assert_eq!(
            fixture
                .registry
                .cleanup_closed_landing_resolution(
                    &fixture.binding,
                    &common,
                    saved,
                    &job,
                    &admission
                )
                .await
                .unwrap(),
            0
        );
    }
    assert_eq!(
        cleanup_files(&fixture.binding.repo.join("op_store")),
        operations
    );
    assert_eq!(cleanup_files(&fixture.binding.repo.join("index")), indexes);
    assert_eq!(
        fixture.connector.calls.load(Ordering::SeqCst),
        calls,
        "cleanup never contacts the projection provider"
    );
    let after = resolution_proofs(&fixture.binding, &objects, &admission).await;
    assert_eq!(after[0].id(), proofs[0].id());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_cleanup_validates_all_records_and_retries_partial_physical_deletion() {
    use jj_lib::op_heads_store::OpHeadsStore as _;
    use landing_resolution::Purpose;
    let fixture = Fixture::new();
    let (mut workspace, _, objects) = resolution_initialize(&fixture).await;
    let admission = resolution_admission();
    let proofs = resolution_proofs(&fixture.binding, &objects, &admission).await;
    let base = fixture
        .registry
        .landing_resolution_base(&fixture.binding, &proofs, &admission)
        .await
        .unwrap();
    let mut pending = cleanup_pending(&base, hex(&[1; 16]));
    let common = cleanup_context(&fixture, &pending);
    let job = cleanup_job(&pending, base.tree().terms[0], true);
    let mut context = landing_resolution::Context {
        provider_identity: common.provider_identity,
        scope: common.scope.clone(),
        request_id: common.request_id.clone(),
        request_digest: common.request_digest,
        purpose: Purpose::Freeze { observed_job: None },
    };
    let freeze = fixture
        .registry
        .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
        .await
        .unwrap();
    fixture
        .registry
        .retire_prepared_freeze_resolution(&fixture.binding, &context, freeze.nonce(), &admission)
        .await
        .unwrap();
    context.purpose = Purpose::Apply {
        job_id: hex(&job.id),
        candidate_head: hex(&job.candidate.as_ref().unwrap().head),
        candidate_digest: landing_resolution::candidate_digest(job.candidate.as_ref().unwrap()),
    };
    let first = fixture
        .registry
        .begin_landing_resolution(&fixture.binding, &context, &base, &proofs, &admission)
        .await
        .unwrap();
    let current = fixture
        .registry
        .replace_prepared_landing_resolution(
            &fixture.binding,
            &context,
            first.nonce(),
            &base,
            &proofs,
            &admission,
        )
        .await
        .unwrap();
    let operation = resolution_operation(&fixture, &current, true).await;
    pending.apply_operation = Some(operation.hex());
    let saved = save_closed_state(&fixture.binding, &common.scope, pending.clone(), &job);
    let directory = PrivateDir::open_existing(
        fixture
            .registry
            .path()
            .join("landing-resolutions")
            .join(fixture.binding.id()),
    )
    .unwrap();
    let unchanged = cleanup_files(directory.path());
    assert!(
        fixture
            .registry
            .cleanup_closed_landing_resolution(&fixture.binding, &common, saved, &job, &admission)
            .await
            .is_err(),
        "portable acknowledgement cannot discard a Prepared host attempt"
    );
    assert_eq!(cleanup_files(directory.path()), unchanged);
    fixture
        .registry
        .record_landing_resolution_operation(
            &fixture.binding,
            &context,
            current.nonce(),
            &operation.hex(),
            &proofs,
            &admission,
        )
        .await
        .unwrap();
    assert!(
        fixture
            .registry
            .cleanup_closed_landing_resolution(&fixture.binding, &common, saved, &job, &admission)
            .await
            .is_err(),
        "Written ownership must be reconciled, not discarded"
    );
    workspace
        .start_working_copy_mutation()
        .await
        .unwrap()
        .finish(operation.clone())
        .await
        .unwrap();
    jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(&fixture.binding.repo.join("op_heads"))
        .update_op_heads(
            &[OperationId::try_from_hex(base.operation()).unwrap()],
            &operation,
        )
        .await
        .unwrap();
    fixture
        .registry
        .complete_landing_resolution(
            &fixture.binding,
            &context,
            current.nonce(),
            &proofs,
            &admission,
        )
        .await
        .unwrap();
    let operations = cleanup_files(&fixture.binding.repo.join("op_store"));
    let indexes = cleanup_files(&fixture.binding.repo.join("index"));
    let original = cleanup_files(directory.path());
    for changed in [
        landing_resolution::CommonContext {
            provider_identity: [8; 32],
            ..common.clone()
        },
        landing_resolution::CommonContext {
            request_digest: [8; 32],
            ..common.clone()
        },
        landing_resolution::CommonContext {
            scope: remote_state::RemoteScope::new(common.scope.socket.clone(), "other".into())
                .unwrap(),
            ..common.clone()
        },
    ] {
        assert!(
            fixture
                .registry
                .cleanup_closed_landing_resolution(
                    &fixture.binding,
                    &changed,
                    saved,
                    &job,
                    &admission
                )
                .await
                .is_err()
        );
        assert_eq!(cleanup_files(directory.path()), original);
    }
    assert!(
        fixture
            .registry
            .cleanup_closed_landing_resolution(&fixture.binding, &common, [8; 32], &job, &admission)
            .await
            .is_err()
    );
    let unknown = format!("{}-unknown.json", common.request_id);
    directory.replace(&unknown, b"{}").unwrap();
    let unknown_state = cleanup_files(directory.path());
    assert!(
        fixture
            .registry
            .cleanup_closed_landing_resolution(&fixture.binding, &common, saved, &job, &admission)
            .await
            .is_err()
    );
    assert_eq!(
        cleanup_files(directory.path()),
        unknown_state,
        "all records validate before any deletion"
    );
    fs::remove_file(directory.path().join(unknown)).unwrap();
    let archive = format!(
        "{}-apply-{}-superseded.json",
        common.request_id,
        hex(&first.nonce())
    );
    let bytes = directory.read(&archive, 65536).unwrap();
    directory.replace(&archive, b"{").unwrap();
    let malformed = cleanup_files(directory.path());
    assert!(
        fixture
            .registry
            .cleanup_closed_landing_resolution(&fixture.binding, &common, saved, &job, &admission)
            .await
            .is_err()
    );
    assert_eq!(cleanup_files(directory.path()), malformed);
    directory.replace(&archive, &bytes).unwrap();
    admission.interrupt_cleanup_after(1);
    assert!(
        fixture
            .registry
            .cleanup_closed_landing_resolution(&fixture.binding, &common, saved, &job, &admission)
            .await
            .is_err()
    );
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 2);
    admission.interrupt_cleanup_after(2);
    assert!(
        fixture
            .registry
            .cleanup_closed_landing_resolution(&fixture.binding, &common, saved, &job, &admission)
            .await
            .is_err(),
        "interruption after all unlinks precedes directory durability acknowledgement"
    );
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    admission.interrupt_cleanup_after(usize::MAX);
    assert_eq!(
        fixture
            .registry
            .cleanup_closed_landing_resolution(&fixture.binding, &common, saved, &job, &admission)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        cleanup_files(&fixture.binding.repo.join("op_store")),
        operations
    );
    assert_eq!(cleanup_files(&fixture.binding.repo.join("index")), indexes);
}

#[test]
fn shared_landing_digests_preserve_v1_wire_recipe_and_ignore_observed_job() {
    use native_broker::landing_protocol as l;
    let mut intent = native_broker::LandingIntent {
        request: l::StartLandingRequest {
            repository_id: String::new(),
            idempotency_key: "01".repeat(16),
            source_base: vec![0; 32],
            source_head: vec![3; 32],
            destination: "main".into(),
            expected_generation: 7,
            expected_head: vec![4; 32],
            profile_name: "checks".into(),
            expected_profile_version: "v1".into(),
        },
        source_stack: vec![
            native_broker::CommitId([2; 32]),
            native_broker::CommitId([3; 32]),
        ],
        observed_job: None,
    };
    // Independent v1 vectors fix the domain, field framing, numeric encoding,
    // empty broker repository field and ordered source stack across migration.
    assert_eq!(
        hex(&landing_resolution::request_digest(&intent).unwrap()),
        "283238ee6b4ec9bd69e5c376f528cfd596b554667a6c2b52207e3bb011e9885b"
    );
    intent.observed_job = Some([5; 32]);
    assert_eq!(
        hex(&landing_resolution::request_digest(&intent).unwrap()),
        "283238ee6b4ec9bd69e5c376f528cfd596b554667a6c2b52207e3bb011e9885b"
    );
    let mut candidate = l::Candidate {
        head: vec![9; 32],
        root_tree: vec![8; 32],
        rewrites: vec![
            l::Rewrite {
                old_commit: vec![3; 32],
                new_commit: vec![9; 32],
                change_id: vec![6; 16],
            },
            l::Rewrite {
                old_commit: vec![2; 32],
                new_commit: vec![7; 32],
                change_id: vec![5; 16],
            },
        ],
    };
    assert_eq!(
        hex(&landing_resolution::candidate_digest(&candidate)),
        "391d55607ba06b7401be6b91c5413c84f42d5b3c207324027750ecbbf71c6d42"
    );
    candidate.rewrites.reverse();
    assert_eq!(
        hex(&landing_resolution::candidate_digest(&candidate)),
        "391d55607ba06b7401be6b91c5413c84f42d5b3c207324027750ecbbf71c6d42"
    );
}

#[test]
fn forge_review_journal_retains_exact_sources_and_rejects_rebound_replay() {
    use crate::forge_pending::{PendingReview, State};
    let original = PendingReview {
        request_id: "review-example".into(),
        scope_hash: [1; 32],
        change_id: "66".repeat(16),
        patchset_id: None,
        review_dispatched: false,
        superseded: false,
        apply_write_started: false,
        source_operation: "11".repeat(64),
        source_base: "22".repeat(32),
        source_head: "33".repeat(32),
        view_version: [4; 32],
        landed_projected_head: None,
        apply_operation: None,
    };
    let mut state = State::default();
    state.retain(original.clone()).unwrap();
    state.retain(original.clone()).unwrap();
    assert_eq!(state.requests.len(), 1);
    let encoded = state.encode().unwrap();
    let decoded = State::decode(&encoded).unwrap();
    assert_eq!(
        decoded.commit_heads().collect::<Vec<_>>(),
        vec![original.source_base.as_str(), original.source_head.as_str()]
    );
    assert_eq!(
        decoded.operations().collect::<Vec<_>>(),
        vec![original.source_operation.as_str()]
    );
    // Re-reading A while B is in flight must not erase B's uncertainty.
    let mut grouped = state.clone();
    assert!(
        grouped
            .acknowledge("review-example", &"aa".repeat(32), true)
            .unwrap()
    );
    let mut replacement = original.clone();
    replacement.request_id = "replacement-example".into();
    replacement.source_head = "77".repeat(32);
    replacement.review_dispatched = true;
    grouped.retain(replacement).unwrap();
    assert!(
        !grouped
            .acknowledge("review-example", &"aa".repeat(32), true)
            .unwrap()
    );
    assert!(!grouped.requests[1].superseded);
    assert!(grouped.requests[1].patchset_id.is_none());
    assert!(
        grouped
            .acknowledge("replacement-example", &"bb".repeat(32), true)
            .unwrap()
    );
    assert!(grouped.requests[0].superseded);
    assert!(!grouped.requests[1].superseded);
    assert!(
        !grouped
            .acknowledge("review-example", &"aa".repeat(32), false)
            .unwrap()
    );
    assert!(!grouped.requests[1].superseded);
    assert_eq!(grouped.commit_heads().count(), 4);
    let mut rebound = original.clone();
    rebound.source_head = "55".repeat(32);
    assert!(state.retain(rebound).is_err());
    assert_eq!(state.encode().unwrap(), encoded);
    let mut invalid = original;
    invalid.source_operation = "11".repeat(128);
    let mut empty = State::default();
    assert!(empty.retain(invalid).is_err());
    let mut untrusted: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    untrusted["actor"] = serde_json::json!("owner");
    assert!(State::decode(&serde_json::to_vec(&untrusted).unwrap()).is_err());
}
