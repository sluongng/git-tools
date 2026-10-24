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
                cloud_source_digest: Some(input.source.digest()),
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
    let view_id = store.write_view(&view).await.unwrap();
    let mut old = Operation::make_root(view_id.clone());
    old.parents = vec![OperationId::from_bytes(&[0; 64])];
    old.metadata.description = "retained original working copy".into();
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
    // while local import normalizes the selected raw protobuf representation.
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
        remote: None,
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
    queries.deny_cloud.store(true, Ordering::Release);
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
    assert!(!fixture.binding.admin.exists());
    queries.deny_cloud.store(false, Ordering::Release);
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
    waiting.await.unwrap();
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
    assert_ne!(
        c::BlobDigest::of(&local_raw),
        original_raw,
        "local codec normalization must preserve semantic contents rather than the source wire representation"
    );

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
