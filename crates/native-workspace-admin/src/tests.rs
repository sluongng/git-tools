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
    drop(controller_a);
    drop(controller_b);
    mount_a.shutdown().await.unwrap();
    mount_b.shutdown().await.unwrap();
}
