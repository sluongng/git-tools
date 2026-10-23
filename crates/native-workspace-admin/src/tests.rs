use super::*;
use async_trait::async_trait;
use jj_lib::{
    backend::{Backend, BackendInitError},
    config::{ConfigLayer, ConfigSource, StackedConfig},
    op_store::OperationId,
    ref_name::WorkspaceName,
};
use jj_native_backend::{Config as BackendConfig, NativeBackend};
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
    async fn begin(&self, _: &ProjectionState) -> Result<Box<dyn NativeProjectionMutation>> {
        anyhow::bail!("not used by external administration tests")
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
        let (workspace, repo) = fixture.initialize().await.unwrap();
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
    let intent = fixture
        .registry
        .begin_provisioning(root, "fixture".into(), endpoint.clone(), &head)
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
