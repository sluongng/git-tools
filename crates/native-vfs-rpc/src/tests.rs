//! Real controller/namespace protocol fixtures; no native mount acceptance claim.
use super::*;
use crate::wire::{Input, Output};
use async_trait::async_trait;
use futures::{AsyncRead, io::Cursor};
use jj_lib::{
    gitignore::GitIgnoreFile,
    matchers::{DifferenceMatcher, EverythingMatcher, FilesMatcher, NothingMatcher},
    op_store::OperationId,
    repo_path::{RepoPath, RepoPathBuf},
    working_copy::{SnapshotOptions, UntrackedReason},
};
use jj_native_backend::{
    CopyCatalogLimits, CopyCatalogSnapshot, DirectObjectIo, FileRead, NativeObjectIo,
    VerifiedMetadata,
};
use jj_vfs::native::{
    NativeProjectionController, ProjectionBinding, ProjectionConnector, ProjectionOutcome,
};
use local_cas::{CacheConfig, LocalCas};
use native_vfs_controller::{Controller, ProtocolFixture};
use native_workspace::{Compiler, Scope};
use output_core::{SourceLimits, Tree};
use private_fs::PrivateDir;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tinytree_model as m;
#[derive(Debug)]
struct TransferGate {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    completed: tokio::sync::Semaphore,
}
#[derive(Debug)]
struct Counting {
    gate: std::sync::Mutex<Option<Arc<TransferGate>>>,
    direct: Arc<DirectObjectIo>,
    reads: AtomicUsize,
    writes: AtomicUsize,
}
#[async_trait]
impl NativeObjectIo for Counting {
    async fn descriptor(
        &self,
        key: m::ObjectKey,
    ) -> jj_lib::backend::BackendResult<Option<m::Descriptor>> {
        self.direct.descriptor(key).await
    }
    async fn read_metadata(
        &self,
        key: m::ObjectKey,
    ) -> jj_lib::backend::BackendResult<VerifiedMetadata> {
        self.direct.read_metadata(key).await
    }
    async fn write_metadata(
        &self,
        value: m::Object,
    ) -> jj_lib::backend::BackendResult<m::Descriptor> {
        self.direct.write_metadata(value).await
    }
    async fn read_file_for_cache(
        &self,
        id: m::FileId,
        _owner: std::sync::Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<FileRead> {
        self.read_file(id).await
    }
    async fn read_file(&self, id: m::FileId) -> jj_lib::backend::BackendResult<FileRead> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.direct.read_file(id).await
    }
    async fn write_file(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max: u64,
    ) -> jj_lib::backend::BackendResult<m::FileId> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        let gate = self.gate.lock().unwrap().clone();
        if let Some(gate) = &gate {
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
        let result = self.direct.write_file(input, max).await;
        if let Some(gate) = gate {
            gate.completed.add_permits(1);
        }
        result
    }
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<Option<m::Descriptor>> {
        self.direct.descriptor_with_owner(key, owner).await
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<VerifiedMetadata> {
        self.direct.read_metadata_with_owner(key, owner).await
    }
    async fn read_file_with_owner(
        &self,
        id: m::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<FileRead> {
        self.direct.read_file_with_owner(id, owner).await
    }
    async fn write_metadata_with_owner(
        &self,
        object: m::Object,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<m::Descriptor> {
        self.direct.write_metadata_with_owner(object, owner).await
    }
    async fn write_file_with_owner(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max: u64,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<m::FileId> {
        let gate = self.gate.lock().unwrap().clone();
        let Some(gate) = gate else {
            return self.direct.write_file_with_owner(input, max, owner).await;
        };
        // A provider may return its transport timeout while the already-owned
        // physical write continues. Stage the borrowed input before detaching.
        use futures::AsyncReadExt;
        let mut bytes = Vec::new();
        input
            .take(max + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| jj_lib::backend::BackendError::Other(error.into()))?;
        if bytes.len() as u64 > max {
            return Err(jj_lib::backend::BackendError::Other(
                std::io::Error::other("fixture upload exceeds limit").into(),
            ));
        }
        let direct = self.direct.clone();
        tokio::spawn(async move {
            let _owner = owner.clone();
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
            direct
                .write_file_with_owner(&mut Cursor::new(bytes), max, owner)
                .await
                .unwrap();
            gate.completed.add_permits(1);
        });
        Err(jj_lib::backend::BackendError::Other(
            std::io::Error::new(std::io::ErrorKind::TimedOut, "fixture provider timeout").into(),
        ))
    }
    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> jj_lib::backend::BackendResult<CopyCatalogSnapshot> {
        self.direct.copy_catalog_snapshot(limits).await
    }
}
struct Fixture {
    _directory: tempfile::TempDir,
    tree: Arc<Tree>,
    provider: Arc<Counting>,
    controller: Arc<Controller>,
    binding: ProjectionBinding,
    endpoint: LocalEndpoint,
    service: ProjectionService,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<(), tonic::Status>>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
impl Fixture {
    async fn new() -> Self {
        Self::with_limits(Limits::default()).await
    }
    async fn with_limits(limits: Limits) -> Self {
        Self::with_delays(limits, Duration::ZERO, Duration::ZERO).await
    }
    async fn with_delays(limits: Limits, header_delay: Duration, body_delay: Duration) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let private = directory.path().join("private");
        drop(PrivateDir::create(&private).unwrap());
        let root = private.join("projection");
        std::fs::create_dir(&root).unwrap();
        let state = private.join("state");
        drop(PrivateDir::create(&state).unwrap());
        let wc = state.join("jj");
        drop(PrivateDir::create(&wc).unwrap());
        let objects = private.join("objects");
        drop(PrivateDir::create(&objects).unwrap());
        let provider = Arc::new(Counting {
            gate: std::sync::Mutex::new(None),
            direct: Arc::new(
                DirectObjectIo::init(&objects, jj_native_backend::Config::default()).unwrap(),
            ),
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
        });
        let mut entries = Vec::new();
        for (name, bytes) in [
            (".gitignore", b"ignored\nforced\n".as_slice()),
            ("tracked", b"base".as_slice()),
            ("untouched", b"quiet".as_slice()),
        ] {
            let id = provider
                .write_file(&mut Cursor::new(bytes), bytes.len() as u64)
                .await
                .unwrap();
            entries.push(m::TreeEntry {
                name: name.into(),
                value: m::TreeValue::File {
                    id,
                    size_bytes: bytes.len() as u64,
                    executable: false,
                    copy_id: None,
                },
            });
        }
        let base = m::TreeId(
            provider
                .write_metadata(m::Object::Tree(m::Tree { entries }))
                .await
                .unwrap()
                .key
                .id
                .0,
        );
        let scope = Scope::from_configured_identity("fixture", b"public-protocol-profile").unwrap();
        let compiler = Arc::new(
            Compiler::new(
                provider.clone(),
                scope.clone(),
                native_workspace::Limits::default(),
            )
            .unwrap(),
        );
        let cache = LocalCas::open(
            private.join("cache"),
            compiler.source(),
            CacheConfig::default(),
        )
        .unwrap();
        let namespace = state.join("namespace");
        drop(PrivateDir::create(&namespace).unwrap());
        let tree =
            Tree::open_with_cache_and_source(namespace, scope.backend(), cache, compiler.source())
                .unwrap();
        let compiled = compiler.compile_tree(base).await.unwrap();
        tree.install_source(
            compiled.manifest(tree.identity()),
            None,
            "bootstrap",
            &SourceLimits::default(),
        )
        .unwrap();
        let controller = Arc::new(
            Controller::open_protocol_fixture(
                ProtocolFixture {
                    tree: tree.clone(),
                    compiler,
                    root: root.clone(),
                    state: state.clone(),
                    workspace_identity: [7; 32],
                    backend: scope.backend().into(),
                },
                provider.clone(),
                native_vfs_controller::Config::new("fixture".into(), wc.clone()),
            )
            .await
            .unwrap(),
        );
        let binding = ProjectionBinding {
            root,
            state_path: wc,
            repository: "fixture".into(),
        };
        let service = ProjectionService::new(limits).unwrap();
        service
            .register("workspace", controller.clone(), binding.clone())
            .unwrap();
        let endpoint = LocalEndpoint::UnixSocket(private.join("rbe.sock"));
        let (stop, signal) = tokio::sync::oneshot::channel();
        let address = endpoint.clone();
        let router = tonic::transport::Server::builder().add_service(DelayedService {
            inner: service.rpc_service(),
            header_delay,
            body_delay,
        });
        let host = service.clone();
        let task = tokio::spawn(async move {
            let result = native_broker::serve_local_router(&address, router, async {
                let _ = signal.await;
            })
            .await;
            host.drain().await?;
            result
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !matches!(&endpoint,LocalEndpoint::UnixSocket(path) if path.exists()) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        provider.reads.store(0, Ordering::SeqCst);
        provider.writes.store(0, Ordering::SeqCst);
        Self {
            _directory: directory,
            tree,
            provider,
            controller,
            binding,
            endpoint,
            service,
            stop: Some(stop),
            task: Some(task),
        }
    }
    async fn registration(&self) -> wire::Registration {
        let limits = Limits::default();
        let channel =
            native_broker::connect_local_channel(self.endpoint.clone(), &limits.transport)
                .await
                .unwrap();
        let mut client = proto::projection_client::ProjectionClient::new(channel);
        let reply = client
            .inspect(
                wire::encode(
                    &wire::Unary::Inspect {
                        mount: "workspace".into(),
                        expected: None,
                    },
                    &limits,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .into_inner();
        let wire::Reply::Inspected {
            binding,
            incarnation,
            ..
        } = wire::decode(reply, &limits).unwrap()
        else {
            panic!()
        };
        wire::Registration {
            binding,
            incarnation,
        }
    }
    async fn connect(&self) -> Arc<dyn NativeProjectionController> {
        let connector =
            ClientConnector::new(self.endpoint.clone(), "workspace", Limits::default()).unwrap();
        let binding = self.binding.clone();
        tokio::task::spawn_blocking(move || connector.connect(&binding))
            .await
            .unwrap()
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
    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
const SNAPSHOT: &str = "00000000-0000-4000-8000-000000000001";
const CHECKOUT: &str = "00000000-0000-4000-8000-000000000002";
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_snapshot_policy_roundtrips_without_hydrating_clean_files() {
    let fixture = Fixture::new().await;
    for (name, bytes) in [
        ("added", b"ok".as_slice()),
        ("ignored", b"ignored bytes".as_slice()),
        ("base-hidden", b"base ignore".as_slice()),
        ("large", b"oversized".as_slice()),
        ("manual", b"not auto".as_slice()),
        ("forced", b"forced and large".as_slice()),
    ] {
        fixture.local(name, bytes).await;
    }
    let remote = fixture.connect().await;
    let before = remote.inspect().unwrap();
    let result = tokio::task::spawn_blocking(move || {
        futures::executor::block_on(async move {
            let start = DifferenceMatcher::new(
                EverythingMatcher,
                FilesMatcher::new([RepoPathBuf::from_internal_string("manual").unwrap()]),
            );
            let force = FilesMatcher::new([RepoPathBuf::from_internal_string("forced").unwrap()]);
            let options = SnapshotOptions {
                base_ignores: GitIgnoreFile::empty()
                    .chain(
                        RepoPath::root(),
                        std::path::Path::new("policy"),
                        b"base-hidden\n",
                    )
                    .unwrap(),
                progress: None,
                start_tracking_matcher: &start,
                force_tracking_matcher: &force,
                max_new_file_size: 2,
            };
            let mut session = remote.begin(&before).await.unwrap();
            let (receipt, stats) = session.snapshot(SNAPSHOT, &options).await.unwrap();
            assert!(matches!(
                stats
                    .untracked_paths
                    .get(&RepoPathBuf::from_internal_string("manual").unwrap()),
                Some(UntrackedReason::FileNotAutoTracked)
            ));
            assert!(matches!(
                stats
                    .untracked_paths
                    .get(&RepoPathBuf::from_internal_string("large").unwrap()),
                Some(UntrackedReason::FileTooLarge { max_size: 2, .. })
            ));
            // An exact replay may return the durable receipt without recapturing an
            // inventory; the adapter must accept it without a new policy callback.
            let (replay, _) = session.snapshot(SNAPSHOT, &options).await.unwrap();
            assert_eq!(receipt, replay);
            session
                .finish(
                    &receipt.after,
                    &OperationId::new(vec![3; 64]),
                    Some(&receipt),
                )
                .await
                .unwrap();
            drop(session);
            assert!(matches!(
                remote.receipt(SNAPSHOT).await.unwrap(),
                ProjectionOutcome::Unknown
            ));
            receipt
        })
    })
    .await
    .unwrap();
    assert_eq!(
        fixture.provider.reads.load(Ordering::SeqCst),
        1,
        "only frozen .gitignore policy bytes may hydrate"
    );
    assert_eq!(fixture.provider.writes.load(Ordering::SeqCst), 2);
    let object = fixture
        .provider
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(result.after.tree.terms[0]),
        })
        .await
        .unwrap()
        .into_object();
    let m::Object::Tree(tree) = object else {
        panic!()
    };
    let names: Vec<_> = tree
        .entries
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    assert_eq!(
        names,
        vec![".gitignore", "added", "forced", "tracked", "untouched"]
    );
    fixture.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_binding_checkout_receipts_and_explicit_unregister_drain_sessions() {
    let fixture = Fixture::new().await;
    let mut wrong = fixture.binding.clone();
    wrong.state_path = wrong.state_path.join("wrong");
    let connector =
        ClientConnector::new(fixture.endpoint.clone(), "workspace", Limits::default()).unwrap();
    assert!(connector.connect(&wrong).is_err());
    assert!(!wrong.state_path.exists());
    let remote = fixture.connect().await;
    let before = remote.inspect().unwrap();
    let mut session = remote.begin(&before).await.unwrap();
    let (receipt, stats) = session.check_out(CHECKOUT, &before.tree).await.unwrap();
    assert_eq!(stats.added_files, 0);
    assert_eq!(receipt.before, before);
    match tokio::time::timeout(Duration::from_secs(2), remote.receipt(CHECKOUT))
        .await
        .unwrap()
        .unwrap()
    {
        ProjectionOutcome::Applied(saved) => assert_eq!(saved, receipt),
        _ => panic!("terminal receipt query must not wait for the caller's own live session"),
    }
    assert!(matches!(
        remote
            .receipt("00000000-0000-4000-8000-000000000099")
            .await
            .unwrap(),
        ProjectionOutcome::Unknown
    ));
    let service = fixture.service.clone();
    let mut remove = tokio::spawn(async move { service.unregister("workspace").await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut remove)
            .await
            .is_err()
    );
    drop(session);
    tokio::time::timeout(Duration::from_secs(5), remove)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(remote.inspect().is_err());
    fixture.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn half_closed_policy_stream_revokes_admission_and_releases_the_same_snapshot_freeze() {
    let fixture = Fixture::new().await;
    fixture.local("new", b"new bytes").await;
    let before = fixture.controller.inspect().unwrap();
    let limits = Limits::default();
    let channel = native_broker::connect_local_channel(fixture.endpoint.clone(), &limits.transport)
        .await
        .unwrap();
    let mut client = proto::projection_client::ProjectionClient::new(channel)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd)
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
    let (send, receive) = tokio::sync::mpsc::channel(8);
    send.send(
        wire::encode(
            &Input::Begin {
                mount: "workspace".into(),
                registration: fixture.registration().await,
                expected: before.clone(),
            },
            &limits,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let mut body = client
        .session(tokio_stream::wrappers::ReceiverStream::new(receive))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        wire::decode::<Output>(body.message().await.unwrap().unwrap(), &limits).unwrap(),
        Output::Began { .. }
    ));
    send.send(
        wire::encode(
            &Input::Snapshot {
                sequence: 1,
                request_id: SNAPSHOT.into(),
                fingerprint: [9; 32],
            },
            &limits,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    loop {
        let output =
            wire::decode::<Output>(body.message().await.unwrap().unwrap(), &limits).unwrap();
        if matches!(output, Output::InventoryComplete { .. }) {
            break;
        }
    }
    let new = fixture.tree.resolve("new").unwrap();
    assert!(
        fixture.tree.write(new.id, 0, b"blocked").await.is_err(),
        "inventory and decisions must share the same source freeze"
    );
    drop(send);
    let output = tokio::time::timeout(Duration::from_secs(5), body.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        wire::decode::<Output>(output, &limits).unwrap(),
        Output::Failure { rejected: true, .. }
    ));
    drop(body);
    assert_eq!(fixture.controller.inspect().unwrap(), before);
    fixture.tree.write(new.id, 0, b"allowed").await.unwrap();
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compressed_label_bomb_is_rejected_before_decoded_session_queue_amplification() {
    let limits = Limits {
        max_frame_bytes: 4 * 1024 * 1024,
        max_policy_bytes: 1024,
        max_memory_bytes: 64 * 1024 * 1024,
        ..Limits::default()
    };
    limits.validate().unwrap();
    let fixture = Fixture::with_limits(limits.clone()).await;
    let before = fixture.controller.inspect().unwrap();
    let channel = native_broker::connect_local_channel(fixture.endpoint.clone(), &limits.transport)
        .await
        .unwrap();
    let mut client = proto::projection_client::ProjectionClient::new(channel)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd)
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .max_encoding_message_size(limits.max_frame_bytes + 4096);
    let (send, receive) = tokio::sync::mpsc::channel(8);
    send.send(
        wire::encode(
            &Input::Begin {
                mount: "workspace".into(),
                registration: fixture.registration().await,
                expected: before.clone(),
            },
            &limits,
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let mut body = client
        .session(tokio_stream::wrappers::ReceiverStream::new(receive))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        wire::decode::<Output>(body.message().await.unwrap().unwrap(), &limits).unwrap(),
        Output::Began { .. }
    ));
    // Tiny wire representation, huge potential String-slot allocation. This is
    // syntactically valid compressed JSON, not a protobuf-size-limit rejection.
    let mut payload = format!(
        r#"{{"Checkout":{{"sequence":1,"request_id":"{}","tree":{{"terms":{},"labels":["#,
        CHECKOUT,
        serde_json::to_string(&before.tree.terms).unwrap()
    );
    payload.push_str(&"\"\",".repeat(1_300_000));
    payload.pop();
    payload.push_str("]}}}");
    assert!(payload.len() < limits.max_frame_bytes);
    let frame = proto::Envelope {
        payload: payload.into_bytes(),
    };
    for _ in 0..8 {
        let _ = send.try_send(frame.clone());
    }
    let result = tokio::time::timeout(Duration::from_secs(5), body.message())
        .await
        .unwrap();
    let error =
        result.expect_err("oversized decoded collections must fail before controller admission");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        error.message(),
        "projection frame exceeds structural decode limits"
    );
    drop(send);
    drop(body);
    assert_eq!(fixture.controller.inspect().unwrap(), before);
    let remote = fixture.connect().await;
    assert_eq!(remote.inspect().unwrap(), before);
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_clients_cannot_rebind_when_mount_alias_is_reused_for_another_controller() {
    let original = Fixture::new().await;
    let replacement = Fixture::new().await;
    let client = original.connect().await;
    let before = client.inspect().unwrap();
    assert_eq!(
        replacement.controller.inspect().unwrap(),
        before,
        "same repository and baseline do not identify a registration"
    );
    original.service.unregister("workspace").await.unwrap();
    original
        .service
        .register(
            "workspace",
            replacement.controller.clone(),
            replacement.binding.clone(),
        )
        .unwrap();
    assert!(client.inspect().is_err());
    assert!(
        client.receipt(SNAPSHOT).await.is_err(),
        "stale receipt lookup must not become Unknown on the replacement"
    );
    assert!(
        client
            .acknowledge(&before, &OperationId::new(vec![3; 64]), None)
            .await
            .is_err()
    );
    assert!(client.begin(&before).await.is_err());
    assert_eq!(replacement.controller.inspect().unwrap(), before);
    // A freshly attested connector may use the replacement; the old connector
    // binding must still fail even though its alias and baseline match.
    let stale =
        ClientConnector::new(original.endpoint.clone(), "workspace", Limits::default()).unwrap();
    assert!(stale.connect(&original.binding).is_err());
    assert!(stale.connect(&replacement.binding).is_ok());
    original.stop().await;
    replacement.stop().await;
}

#[derive(Clone)]
struct DelayedService<S> {
    inner: S,
    header_delay: Duration,
    body_delay: Duration,
}
impl<S> tonic::server::NamedService for DelayedService<S> {
    const NAME: &'static str = "native.vfs.v1.Projection";
}
impl<S> tower::Service<http::Request<tonic::body::Body>> for DelayedService<S>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let delayed = request.uri().path().ends_with("/Session");
        let response = self.inner.call(request);
        let header = self.header_delay;
        let body = self.body_delay;
        Box::pin(async move {
            let response = response.await?;
            if delayed {
                tokio::time::sleep(header).await;
                Ok(response.map(|body_inner| {
                    tonic::body::Body::new(DelayedBody {
                        body: body_inner,
                        timer: Box::pin(tokio::time::sleep(body)),
                        waiting: true,
                    })
                }))
            } else {
                Ok(response)
            }
        })
    }
}
struct DelayedBody {
    body: tonic::body::Body,
    timer: std::pin::Pin<Box<tokio::time::Sleep>>,
    waiting: bool,
}
impl http_body::Body for DelayedBody {
    type Data = bytes::Bytes;
    type Error = tonic::Status;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use std::future::Future;
        let this = self.get_mut();
        if this.waiting {
            std::task::ready!(this.timer.as_mut().poll(cx));
            this.waiting = false;
        }
        std::pin::Pin::new(&mut this.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_begin_headers_and_began_share_one_original_deadline() {
    let limits = Limits {
        operation_timeout: Duration::from_millis(300),
        ..Limits::default()
    };
    let fixture = Fixture::with_delays(
        limits.clone(),
        Duration::from_millis(200),
        Duration::from_millis(200),
    )
    .await;
    let connector = ClientConnector::new(fixture.endpoint.clone(), "workspace", limits).unwrap();
    let binding = fixture.binding.clone();
    let remote = tokio::task::spawn_blocking(move || connector.connect(&binding))
        .await
        .unwrap()
        .unwrap();
    let before = remote.inspect().unwrap();
    assert!(
        remote.begin(&before).await.is_err(),
        "headers must not renew the budget before the Began frame"
    );
    fixture.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_or_disconnected_actions_drain_admitted_file_write_without_root_publication() {
    for eof in [false, true] {
        let operation_timeout = if eof {
            Duration::from_secs(5)
        } else {
            Duration::from_millis(100)
        };
        let fixture = Fixture::with_limits(Limits {
            operation_timeout,
            ..Limits::default()
        })
        .await;
        fixture.local("new", b"new payload").await;
        let before = fixture.tree.source_baseline().unwrap();
        let gate = Arc::new(TransferGate {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            completed: tokio::sync::Semaphore::new(0),
        });
        *fixture.provider.gate.lock().unwrap() = Some(gate.clone());
        let remote = fixture.connect().await;
        let expected = remote.inspect().unwrap();
        let mut task = Some(tokio::spawn(async move {
            let mut session = remote.begin(&expected).await.unwrap();
            let options = SnapshotOptions {
                base_ignores: GitIgnoreFile::empty(),
                progress: None,
                start_tracking_matcher: &EverythingMatcher,
                force_tracking_matcher: &NothingMatcher,
                max_new_file_size: 1024,
            };
            let result = session.snapshot(SNAPSHOT, &options).await;
            drop(session);
            result
        }));
        tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        if eof {
            let task = task.take().unwrap();
            task.abort();
            let _ = task.await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert_eq!(fixture.tree.source_baseline().unwrap(), before);
        let service = fixture.service.clone();
        let mut draining = tokio::spawn(async move { service.unregister("workspace").await });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut draining)
                .await
                .is_err(),
            "physical write still owns its session fence"
        );
        gate.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), gate.completed.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        if !eof {
            assert!(task.take().unwrap().await.unwrap().is_err());
        }
        tokio::time::timeout(Duration::from_secs(5), draining)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            fixture.tree.source_baseline().unwrap(),
            before,
            "no later root publication may follow the drained write"
        );
        fixture.stop().await;
    }
}

struct CommandFixture {
    active: Arc<AtomicUsize>,
}
struct CommandFixtureGuard(Arc<AtomicUsize>);
impl Drop for CommandFixtureGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl CommandHost for CommandFixture {
    async fn prepare(
        &self,
        mount: &str,
        _replay: [u8; 32],
    ) -> Result<CommandPreparation, tonic::Status> {
        if mount != "workspace" {
            return Err(tonic::Status::not_found("fixture mount"));
        }
        Ok(CommandPreparation {
            incarnation: [1; 16],
            nonce: [2; 32],
            expected_epoch: 0,
        })
    }
    async fn begin(
        &self,
        mount: &str,
        _replay: [u8; 32],
        preparation: CommandPreparation,
    ) -> Result<CommandStarted, tonic::Status> {
        if preparation.incarnation != [1; 16]
            || preparation.nonce != [2; 32]
            || preparation.expected_epoch != 0
        {
            return Err(tonic::Status::failed_precondition("fixture preparation"));
        }
        if mount != "workspace" {
            return Err(tonic::Status::not_found("fixture mount"));
        }
        Ok(CommandStarted {
            token: [4; 32],
            epoch: 1,
        })
    }
    async fn finish_dirty(&self, mount: &str, token: [u8; 32]) -> Result<(), tonic::Status> {
        if mount != "workspace" || token != [4; 32] {
            return Err(tonic::Status::permission_denied("fixture command"));
        }
        Ok(())
    }
    fn resolve(&self, mount: &str, token: [u8; 32]) -> Result<CommandJob, tonic::Status> {
        if mount != "workspace" || token != [4; 32] {
            return Err(tonic::Status::permission_denied("fixture command"));
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(CommandFixtureGuard(self.active.clone())))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_command_wire_and_guard_survive_provider_timeout_until_physical_write_finishes() {
    let fixture = Fixture::new().await;
    let active = Arc::new(AtomicUsize::new(0));
    fixture
        .service
        .set_command_host(Arc::new(CommandFixture {
            active: active.clone(),
        }))
        .unwrap();
    let command =
        CommandClient::new(fixture.endpoint.clone(), "workspace", Limits::default()).unwrap();
    let channel = native_broker::connect_local_channel(
        fixture.endpoint.clone(),
        &Limits::default().transport,
    )
    .await
    .unwrap();
    let mut raw = client::client(channel, &Limits::default());
    let oversized =
        serde_json::json!({"Prepare":{"version":1,"mount":"workspace","replay":vec![5u8;33]}});
    assert_eq!(
        raw.command(tonic::Request::new(proto::Envelope {
            payload: serde_json::to_vec(&oversized).unwrap()
        }))
        .await
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
    let preparation = command.prepare([5; 32]).await.unwrap();
    let started = command.begin([5; 32], preparation).await.unwrap();
    assert_eq!(started.epoch, 1);
    let missing =
        ClientConnector::new(fixture.endpoint.clone(), "workspace", Limits::default()).unwrap();
    assert!(missing.connect(&fixture.binding).is_err());
    let wrong = missing.clone().with_command_token([6; 32]);
    assert!(wrong.connect(&fixture.binding).is_err());
    let connector = missing.with_command_token(started.token);
    let binding = fixture.binding.clone();
    let remote = tokio::task::spawn_blocking(move || connector.connect(&binding))
        .await
        .unwrap()
        .unwrap();
    fixture.local("new", b"owned bytes").await;
    let expected = remote.inspect().unwrap();
    let gate = Arc::new(TransferGate {
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
        completed: tokio::sync::Semaphore::new(0),
    });
    *fixture.provider.gate.lock().unwrap() = Some(gate.clone());
    let operation = tokio::spawn(async move {
        let mut session = remote.begin(&expected).await.unwrap();
        let options = SnapshotOptions {
            base_ignores: GitIgnoreFile::empty(),
            progress: None,
            start_tracking_matcher: &EverythingMatcher,
            force_tracking_matcher: &NothingMatcher,
            max_new_file_size: 1024,
        };
        session.snapshot(SNAPSHOT, &options).await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), operation)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(
        active.load(Ordering::SeqCst) > 0,
        "physical provider job must retain its command after returning a timeout"
    );
    gate.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), gate.completed.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    tokio::time::timeout(Duration::from_secs(5), async {
        while active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    command.finish_dirty(started.token).await.unwrap();
    fixture.stop().await;
}
