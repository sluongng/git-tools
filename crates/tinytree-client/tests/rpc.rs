use futures_util::StreamExt;
use p::repository_server::{Repository, RepositoryServer};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tinytree_client::{Config, MetadataOutcome, NativeClient, protocol as p};
use tinytree_model::{Kind, ObjectId, ObjectKey};
use tinytree_server::{Principal, RepositoryService, ServiceConfig};
use tinytree_store::Store;
use tonic::{Code, Request, Response, Status, Streaming};

fn private_tempdir() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    directory
}
const TOKEN: &str = "client-loopback-test-token-not-a-secret";
struct LifetimeProbe(Arc<AtomicUsize>);
impl Drop for LifetimeProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
async fn assert_released(counter: &AtomicUsize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while counter.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
struct Running {
    address: std::net::SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
    metadata: Arc<AtomicUsize>,
    graph: Arc<AtomicUsize>,
    canonical_bytes: Arc<AtomicUsize>,
    files: Arc<AtomicUsize>,
    queries: Arc<QueryCounts>,
}
impl Running {
    async fn start(service: RepositoryService, mode: Mode) -> Self {
        Self::start_with_socket_policy(service, mode, false).await
    }
    async fn start_with_socket_policy(
        service: RepositoryService,
        mode: Mode,
        nodelay: bool,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let metadata = Arc::new(AtomicUsize::new(0));
        let graph = Arc::new(AtomicUsize::new(0));
        let canonical_bytes = Arc::new(AtomicUsize::new(0));
        let files = Arc::new(AtomicUsize::new(0));
        let queries = Arc::new(QueryCounts::default());
        let query_service = RepositoryQueryServer::new(QueryFaults {
            service: service.query_service(),
            mode: QueryMode::Normal,
            counts: queries.clone(),
            first_scope: Arc::new(std::sync::Mutex::new(None)),
            first_page: Arc::new(std::sync::Mutex::new(None)),
        })
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd);
        let faults = Faults {
            service,
            mode,
            reads: files.clone(),
            writes: Arc::new(AtomicUsize::new(0)),
            metadata: metadata.clone(),
            graph: graph.clone(),
            canonical_bytes: canonical_bytes.clone(),
            publications: Arc::new(AtomicUsize::new(0)),
        };
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    RepositoryServer::new(faults)
                        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                        .send_compressed(tonic::codec::CompressionEncoding::Zstd)
                        .max_decoding_message_size(20 * 1024 * 1024)
                        .max_encoding_message_size(20 * 1024 * 1024),
                )
                .add_service(query_service)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener).map(move |result| {
                        result.and_then(|stream| {
                            if nodelay {
                                stream.set_nodelay(true)?;
                            }
                            Ok(stream)
                        })
                    }),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        });
        Self {
            address,
            stop: Some(stop),
            task,
            metadata,
            graph,
            canonical_bytes,
            files,
            queries,
        }
    }
    fn config(&self) -> Config {
        let mut config = Config::default();
        config.endpoint = format!("http://{}", self.address);
        config.repository_id = "example".into();
        config.token_env = Some("TINYTREE_CLIENT_TEST_TOKEN".into());
        config.rpc.retry.initial_backoff_ms = 1;
        config.rpc.retry.max_backoff_ms = 5;
        config
    }
    async fn client(&self) -> NativeClient {
        NativeClient::connect(self.config()).await.unwrap()
    }
    async fn stop(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
fn setup() -> (tempfile::TempDir, Store, RepositoryService) {
    let dir = private_tempdir();
    let store = Store::open(dir.path(), Default::default()).unwrap();
    store.create_repository("example").unwrap();
    let service = RepositoryService::new(
        store.clone(),
        ServiceConfig::default(),
        vec!["example".into()],
        vec![Principal::new("test", TOKEN, vec!["example".into()], true).unwrap()],
    )
    .unwrap();
    (dir, store, service)
}
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Normal,
    RetryFile,
    CorruptFile,
    TruncateFile,
    DelayFile,
    PartialMetadata,
    WrongGraph,
    LegacyGraph,
    MissingGraphObject,
    CorruptGraphEnvelope,
    OversizedGraphEnvelope,
    GraphCanonicalShape,
    LostPublication,
    RetryUpload,
    DelayedPublication,
    BeforePublication,
    OversizedMetadata,
    EmptyReferences,
    TooManyReferences,
    EmptyGraphEntries,
    EmptyMissingKeys,
}
#[derive(Clone)]
struct Faults {
    service: RepositoryService,
    mode: Mode,
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    metadata: Arc<AtomicUsize>,
    graph: Arc<AtomicUsize>,
    canonical_bytes: Arc<AtomicUsize>,
    publications: Arc<AtomicUsize>,
}
type Stream<T> = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<T, Status>> + Send>>;
#[tonic::async_trait]
impl Repository for Faults {
    async fn open_repository(
        &self,
        r: Request<p::OpenRepositoryRequest>,
    ) -> Result<Response<p::RepositoryView>, Status> {
        let mut response = self.service.open_repository(r).await?;
        if self.mode == Mode::EmptyReferences {
            response.get_mut().references = vec![p::Reference::default(); 257];
        }
        if self.mode == Mode::TooManyReferences {
            response.get_mut().references = (0..257)
                .map(|i| p::Reference {
                    name: format!("reference-{i}"),
                    ..Default::default()
                })
                .collect();
        }
        Ok(response)
    }
    async fn get_commit_graph(
        &self,
        r: Request<p::GetCommitGraphRequest>,
    ) -> Result<Response<p::CommitGraphPage>, Status> {
        self.graph.fetch_add(1, Ordering::SeqCst);
        let mut response = self.service.get_commit_graph(r).await?;
        if self.mode == Mode::EmptyGraphEntries {
            response.get_mut().entries = vec![p::CommitGraphEntry::default(); 257];
        }
        if self.mode == Mode::WrongGraph {
            if let Some(entry) = response.get_mut().entries.first_mut() {
                entry.parents.clear();
            }
        }
        if matches!(self.mode, Mode::LegacyGraph | Mode::MissingGraphObject) {
            for entry in &mut response.get_mut().entries {
                entry.canonical_envelope.clear();
            }
        }
        if self.mode == Mode::CorruptGraphEnvelope {
            response.get_mut().entries[0].canonical_envelope[0] ^= 1;
        }
        if self.mode == Mode::OversizedGraphEnvelope {
            response.get_mut().entries[0].canonical_envelope = vec![0; 128 * 1024];
        }
        if self.mode == Mode::GraphCanonicalShape {
            let entry = &mut response.get_mut().entries[0];
            let tinytree_model::Object::Commit(mut commit) =
                tinytree_model::decode(&entry.canonical_envelope, Default::default()).unwrap()
            else {
                unreachable!()
            };
            commit.root_tree.terms = vec![commit.root_tree.terms[0]; 4001];
            commit.root_tree.labels = vec![String::new(); 4001];
            let canonical =
                tinytree_model::encode(&tinytree_model::Object::Commit(commit), Default::default())
                    .unwrap();
            tinytree_model::verify(&canonical.descriptor, &canonical.bytes, Default::default())
                .unwrap();
            entry.commit = Some(p::ObjectDescriptor {
                key: Some(p::ObjectKey {
                    kind: Kind::Commit as i32,
                    native_id: canonical.descriptor.key.id.0.to_vec(),
                }),
                content_blake3: canonical.descriptor.key.id.0.to_vec(),
                size_bytes: canonical.descriptor.size_bytes,
                object_format_version: canonical.descriptor.format_version as u32,
            });
            entry.canonical_envelope = canonical.bytes;
        }
        self.canonical_bytes.fetch_add(
            response
                .get_ref()
                .entries
                .iter()
                .map(|entry| entry.canonical_envelope.len())
                .sum::<usize>(),
            Ordering::SeqCst,
        );
        Ok(response)
    }
    type BatchReadObjectsStream = Stream<p::ObjectResult>;
    async fn batch_read_objects(
        &self,
        r: Request<p::BatchReadObjectsRequest>,
    ) -> Result<Response<Self::BatchReadObjectsStream>, Status> {
        let first = self.metadata.fetch_add(1, Ordering::SeqCst) == 0;
        let requested = r.get_ref().keys.len();
        let mut body = self.service.batch_read_objects(r).await?.into_inner();
        if self.mode == Mode::PartialMetadata && first {
            let item = body.next().await.unwrap()?;
            return Ok(Response::new(Box::pin(tokio_stream::iter(vec![
                Ok(item),
                Err(Status::unavailable("injected partial transport failure")),
            ]))));
        }
        if self.mode == Mode::PartialMetadata {
            assert_eq!(requested, 1, "retry must request only unfinished metadata");
        }
        let mut items = Vec::new();
        while let Some(item) = body.next().await {
            items.push(item);
        }
        if self.mode == Mode::OversizedMetadata {
            for item in &mut items {
                if let Ok(item) = item {
                    if let Some(p::object_result::Result::Object(data)) = &mut item.result {
                        data.canonical_envelope = vec![0; 128 * 1024];
                    }
                }
            }
        }
        if self.mode == Mode::MissingGraphObject {
            for item in &mut items {
                if let Ok(item) = item {
                    item.result = Some(p::object_result::Result::Error(p::ItemError {
                        code: p::item_error::Code::NotFound as i32,
                        message: String::new(),
                    }));
                }
            }
        }
        self.canonical_bytes.fetch_add(
            items
                .iter()
                .filter_map(|item| item.as_ref().ok())
                .filter_map(|item| match &item.result {
                    Some(p::object_result::Result::Object(data)) => {
                        Some(data.canonical_envelope.len())
                    }
                    _ => None,
                })
                .sum::<usize>(),
            Ordering::SeqCst,
        );
        items.reverse();
        Ok(Response::new(Box::pin(tokio_stream::iter(items))))
    }
    async fn find_missing_objects(
        &self,
        r: Request<p::FindMissingObjectsRequest>,
    ) -> Result<Response<p::MissingObjects>, Status> {
        let mut response = self.service.find_missing_objects(r).await?;
        if self.mode == Mode::EmptyMissingKeys {
            response.get_mut().keys = vec![p::ObjectKey::default(); 257];
        }
        Ok(response)
    }
    type ReadFileStream = Stream<p::FileFrame>;
    async fn read_file(
        &self,
        r: Request<p::ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let first = self.reads.fetch_add(1, Ordering::SeqCst) == 0;
        if self.mode == Mode::RetryFile && first {
            return Err(Status::unavailable("injected retryable read failure"));
        }
        let mut body = self.service.read_file(r).await?.into_inner();
        let mode = self.mode;
        Ok(Response::new(Box::pin(
            async_stream::try_stream! {let mut chunks=0;while let Some(frame)=body.next().await {let mut frame=frame?;if let Some(p::file_frame::Frame::Chunk(chunk))=&mut frame.frame {chunks+=1;if mode==Mode::DelayFile {tokio::time::sleep(Duration::from_millis(500)).await;}if mode==Mode::TruncateFile {break;}if mode==Mode::CorruptFile&&chunks==1 {chunk.data[0]^=1;}}yield frame;}},
        )))
    }
    async fn write_object(
        &self,
        r: Request<Streaming<p::WriteFrame>>,
    ) -> Result<Response<p::WriteReceipt>, Status> {
        if self.mode == Mode::RetryUpload && self.writes.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(Status::unavailable("injected replay-safe upload failure"));
        }
        self.service.write_object(r).await
    }
    async fn publish_reference(
        &self,
        r: Request<p::PublishReferenceRequest>,
    ) -> Result<Response<p::Publication>, Status> {
        let first = self.publications.fetch_add(1, Ordering::SeqCst) == 0;
        if self.mode == Mode::BeforePublication && first {
            return Err(Status::unavailable(
                "injected failure before idempotency input check",
            ));
        }
        let response = self.service.publish_reference(r).await?;
        if self.mode == Mode::DelayedPublication && first {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if self.mode == Mode::LostPublication && first {
            return Err(Status::unavailable(
                "injected lost durable publication acknowledgment",
            ));
        }
        Ok(response)
    }
    async fn get_publication(
        &self,
        r: Request<p::GetPublicationRequest>,
    ) -> Result<Response<p::Publication>, Status> {
        self.service.get_publication(r).await
    }
}
fn fixture() -> tinytree_fixture::Fixture {
    tinytree_fixture::build_fixture(tinytree_fixture::FixtureConfig {
        unused_files: 2,
        bytes_per_file: 3000,
    })
    .unwrap()
}
fn key(wire: &p::ObjectKey) -> ObjectKey {
    ObjectKey {
        kind: Kind::try_from(wire.kind as u8).unwrap(),
        id: ObjectId::from_slice(&wire.native_id).unwrap(),
    }
}

#[tokio::test]
async fn compressed_verified_roundtrip_snapshot_and_publication_reconciliation() {
    let (_dir, _store, service) = setup();
    let running = Running::start(service, Mode::LostPublication).await;
    let client = running.client().await;
    let parent = private_tempdir();
    let fixture = fixture();
    let mut leases = Vec::new();
    let mut commit = None;
    let mut file = None;
    for object in fixture.objects() {
        let receipt = if object.descriptor.key.kind == Kind::File {
            let path = parent.path().join("mutable-source");
            std::fs::write(&path, &object.bytes).unwrap();
            let source = client
                .prepare_upload_file(&path, object.descriptor, parent.path())
                .await
                .unwrap();
            std::fs::write(path, b"changed after snapshot").unwrap();
            file = Some((object.descriptor, object.bytes.clone()));
            client.upload_file(source).await.unwrap()
        } else {
            let metadata = client
                .verify_metadata(object.descriptor, object.bytes.clone())
                .await
                .unwrap();
            if object.descriptor.key.kind == Kind::Commit {
                commit = Some(object.descriptor.key);
            }
            client.upload_metadata(metadata).await.unwrap()
        };
        leases.push(receipt.staging_lease);
    }
    let commit = commit.unwrap();
    let request = p::PublishReferenceRequest {
        repository_id: "example".into(),
        idempotency_key: "client-publication-roundtrip".into(),
        reference_name: "main".into(),
        expected_generation: 0,
        new_commit: Some(p::ObjectKey {
            kind: commit.kind as i32,
            native_id: commit.id.0.to_vec(),
        }),
        staging_leases: leases,
    };
    let receipt = client.publish(request.clone()).await.unwrap();
    assert_eq!(receipt.reference.as_ref().unwrap().generation, 1);
    assert_eq!(
        client.publication(&request.idempotency_key).await.unwrap(),
        receipt
    );
    assert_eq!(client.publish(request.clone()).await.unwrap(), receipt);
    let mut stale = request;
    stale.idempotency_key = "independent-stale-cas".into();
    assert_eq!(
        client.publish(stale).await.unwrap_err().code(),
        Code::Aborted
    );
    let view = client
        .open(vec!["main".into(), "absent".into()])
        .await
        .unwrap();
    assert_eq!(view.references.len(), 2);
    assert!(client.find_missing(vec![commit]).await.unwrap().is_empty());
    let graph = client
        .canonical_commit_summaries(vec![commit], vec![])
        .await
        .unwrap();
    assert_eq!(graph.entries.len(), 1);
    let (descriptor, bytes) = file.unwrap();
    let verified = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .unwrap();
    assert_eq!(verified.descriptor(), descriptor);
    let destination = parent.path().join("installed");
    verified.persist_noclobber(&destination).unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), bytes);
    let verified = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .unwrap();
    assert_eq!(
        verified.persist_noclobber(&destination).unwrap_err().code(),
        Code::AlreadyExists
    );
    assert_eq!(std::fs::read(destination).unwrap(), bytes);
    running.stop().await;

    // Unary request capacity is independent of streamed file-frame capacity.
    // This supported request exceeds the old frame-derived 128 KiB encoder cap.
    let mut server_config = ServiceConfig::default();
    server_config.max_batch_items = 1024;
    server_config.max_request_bytes = 1024 * 1024;
    let service = RepositoryService::new(
        _store,
        server_config,
        vec!["example".into()],
        vec![Principal::new("test", TOKEN, vec!["example".into()], true).unwrap()],
    )
    .unwrap();
    let running = Running::start(service, Mode::Normal).await;
    let mut config = running.config();
    config.limits.max_batch_items = 1024;
    config.limits.max_request_bytes = 1024 * 1024;
    let client = NativeClient::connect(config).await.unwrap();
    let names: Vec<_> = (0..1024)
        .map(|i| format!("bookmark-{i:04}-{}", "x".repeat(186)))
        .collect();
    let view = client.open(names.clone()).await.unwrap();
    assert_eq!(view.references.len(), names.len());
    assert!(
        view.references
            .iter()
            .all(|reference| reference.commit.is_none())
    );
    assert_eq!(
        view.references
            .iter()
            .map(|reference| reference.name.clone())
            .collect::<std::collections::HashSet<_>>(),
        names.into_iter().collect()
    );
    running.stop().await;
}

#[tokio::test]
async fn file_verification_retry_and_failed_outputs_are_never_installed() {
    for mode in [
        Mode::RetryFile,
        Mode::CorruptFile,
        Mode::TruncateFile,
        Mode::DelayFile,
    ] {
        let (_dir, store, service) = setup();
        let bytes = vec![23; 3000];
        let descriptor = tinytree_model::describe_file(&bytes);
        store.put("example", descriptor, &bytes, 0).unwrap();
        let running = Running::start(service, mode).await;
        let mut config = running.config();
        config.rpc.operation_timeout_ms = 150;
        config.rpc.stream_idle_timeout_ms = 50;
        config.rpc.retry.max_attempts = 2;
        let client = NativeClient::connect(config).await.unwrap();
        let parent = private_tempdir();
        let result = client.download_file(descriptor.key.id, parent.path()).await;
        if mode == Mode::RetryFile {
            let verified = result.unwrap();
            assert_eq!(verified.descriptor(), descriptor);
            drop(verified);
        } else {
            let status = result
                .err()
                .expect("malformed or timed-out file must not verify");
            assert!(matches!(
                status.code(),
                Code::DataLoss | Code::DeadlineExceeded
            ));
        }
        assert_eq!(
            std::fs::read_dir(parent.path()).unwrap().count(),
            0,
            "failed and dropped downloads clean private staging files"
        );
        running.stop().await;
    }
}

#[tokio::test]
async fn partial_metadata_retries_only_unfinished_keys_and_keeps_item_errors_explicit() {
    let (_dir, store, service) = setup();
    let fixture = fixture();
    let objects: Vec<_> = fixture
        .objects()
        .iter()
        .filter(|object| object.descriptor.key.kind != Kind::File)
        .take(2)
        .collect();
    for object in &objects {
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let running = Running::start(service, Mode::PartialMetadata).await;
    let client = running.client().await;
    let outcomes = client
        .read_objects(objects.iter().map(|object| object.descriptor.key).collect())
        .await
        .unwrap();
    for (outcome, object) in outcomes.into_iter().zip(&objects) {
        match outcome {
            MetadataOutcome::Object(verified) => {
                assert_eq!(verified.canonical_bytes(), object.bytes)
            }
            _ => panic!("expected verified metadata"),
        }
    }
    let unknown = ObjectKey {
        kind: Kind::Tree,
        id: ObjectId([61; 32]),
    };
    let outcomes = client.read_objects(vec![unknown]).await.unwrap();
    assert!(matches!(
        outcomes.first(),
        Some(MetadataOutcome::Error {
            code: p::item_error::Code::NotFound
        })
    ));
    running.stop().await;
}

#[tokio::test]
async fn canonical_graph_verification_preserves_legacy_compatibility_without_trusting_payloads() {
    struct Case {
        name: &'static str,
        mode: Mode,
        error: Option<Code>,
        metadata_rpcs: usize,
        graph_limit: Option<usize>,
        metadata_limit: usize,
        error_message: Option<&'static str>,
    }
    for case in [
        Case {
            name: "canonical payload avoids metadata RPC",
            mode: Mode::Normal,
            error: None,
            metadata_rpcs: 0,
            graph_limit: None,
            metadata_limit: 1024,
            error_message: None,
        },
        Case {
            name: "old server fetches canonical object",
            mode: Mode::LegacyGraph,
            error: None,
            metadata_rpcs: 1,
            graph_limit: None,
            metadata_limit: 1024,
            error_message: None,
        },
        Case {
            name: "missing old-server canonical object fails",
            mode: Mode::MissingGraphObject,
            error: Some(Code::NotFound),
            metadata_rpcs: 1,
            graph_limit: None,
            metadata_limit: 1024,
            error_message: None,
        },
        Case {
            name: "forged parent summary fails",
            mode: Mode::WrongGraph,
            error: Some(Code::DataLoss),
            metadata_rpcs: 0,
            graph_limit: None,
            metadata_limit: 1024,
            error_message: None,
        },
        Case {
            name: "tampered payload never falls back",
            mode: Mode::CorruptGraphEnvelope,
            error: Some(Code::DataLoss),
            metadata_rpcs: 0,
            graph_limit: None,
            metadata_limit: 1024,
            error_message: None,
        },
        Case {
            name: "payload bounded before allocation",
            mode: Mode::OversizedGraphEnvelope,
            error: Some(Code::ResourceExhausted),
            metadata_rpcs: 0,
            graph_limit: Some(1024 * 1024),
            metadata_limit: 1024,
            error_message: None,
        },
        Case {
            name: "retained graph allocation bounded",
            mode: Mode::Normal,
            error: Some(Code::ResourceExhausted),
            metadata_rpcs: 0,
            graph_limit: Some(1024),
            metadata_limit: 1024,
            error_message: None,
        },
        Case {
            name: "canonical empty-label vectors bounded before decode",
            mode: Mode::GraphCanonicalShape,
            error: Some(Code::ResourceExhausted),
            metadata_rpcs: 0,
            graph_limit: Some(1024 * 1024),
            metadata_limit: 256 * 1024,
            error_message: Some("canonical shape exceeds allocation budget"),
        },
    ] {
        let (_dir, store, service) = setup();
        let fixture = fixture();
        let mut commit = None;
        for object in fixture.objects() {
            store
                .put("example", object.descriptor, &object.bytes, 0)
                .unwrap();
            if object.descriptor.key.kind == Kind::Commit {
                commit = Some(object.descriptor.key);
            }
        }
        let running = Running::start(service, case.mode).await;
        let mut config = running.config();
        if let Some(limit) = case.graph_limit {
            config.limits.max_metadata_bytes = case.metadata_limit;
            config.limits.max_graph_bytes = limit;
        }
        let client = NativeClient::connect(config).await.unwrap();
        let result = client
            .canonical_commit_summaries(vec![commit.unwrap()], vec![])
            .await;
        match case.error {
            Some(code) => {
                let error = result.err().expect(case.name);
                assert_eq!(error.code(), code, "{}", case.name);
                if let Some(message) = case.error_message {
                    assert!(error.message().contains(message), "{}: {error}", case.name);
                }
            }
            None => {
                let graph = result.unwrap();
                assert_eq!(graph.entries.len(), 1, "{}", case.name);
                assert_eq!(
                    graph.entries[0].canonical_envelope.is_empty(),
                    case.mode == Mode::LegacyGraph
                );
            }
        }
        assert_eq!(
            running.metadata.load(Ordering::SeqCst),
            case.metadata_rpcs,
            "{}",
            case.name
        );
        running.stop().await;
    }
}

#[tokio::test]
async fn owned_upload_snapshot_replays_and_caller_mutation_cannot_change_it() {
    let (_dir, _store, service) = setup();
    let running = Running::start(service, Mode::RetryUpload).await;
    let client = running.client().await;
    let parent = private_tempdir();
    let path = parent.path().join("input");
    let bytes = vec![49; 10000];
    std::fs::write(&path, &bytes).unwrap();
    let descriptor = tinytree_model::describe_file(&bytes);
    let released = Arc::new(AtomicUsize::new(0));
    let source = client
        .prepare_upload_file(&path, descriptor, parent.path())
        .await
        .unwrap()
        .with_lifetime_guard(Arc::new(LifetimeProbe(released.clone())));
    let held_source = source.clone();
    std::fs::write(path, b"mutable caller content").unwrap();
    let receipt = client.upload_file(source).await.unwrap();
    assert_eq!(released.load(Ordering::SeqCst), 0);
    drop(held_source);
    assert_released(&released).await;
    assert_eq!(
        key(receipt.descriptor.as_ref().unwrap().key.as_ref().unwrap()),
        descriptor.key
    );
    let verified = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .unwrap();
    let mut actual = Vec::new();
    std::io::Read::read_to_end(&mut verified.open().unwrap(), &mut actual).unwrap();
    assert_eq!(actual, bytes);
    running.stop().await;
}

#[tokio::test]
async fn operation_deadline_covers_disk_admission_and_cancel_releases_quota() {
    let (_dir, store, service) = setup();
    let bytes = vec![11; 3000];
    let descriptor = tinytree_model::describe_file(&bytes);
    store.put("example", descriptor, &bytes, 0).unwrap();
    let running = Running::start(service, Mode::Normal).await;
    let mut config = running.config();
    config.limits.max_file_bytes = 4096;
    config.limits.max_temporary_bytes = 4096;
    config.rpc.operation_timeout_ms = 100;
    let client = NativeClient::connect(config).await.unwrap();
    let parent = private_tempdir();
    let verified = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .unwrap();
    let error = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    drop(verified);
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    let next = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .unwrap();
    drop(next);
    running.stop().await;
}

#[tokio::test]
async fn cancelling_stream_cleans_temp_and_later_operations_still_run() {
    let (_dir, store, service) = setup();
    let bytes = vec![17; 3000];
    let descriptor = tinytree_model::describe_file(&bytes);
    store.put("example", descriptor, &bytes, 0).unwrap();
    let running = Running::start(service, Mode::DelayFile).await;
    let mut config = running.config();
    config.limits.max_file_bytes = 4096;
    config.limits.max_temporary_bytes = 4096;
    config.rpc.operation_timeout_ms = 2000;
    config.rpc.stream_idle_timeout_ms = 1000;
    let client = NativeClient::connect(config).await.unwrap();
    let parent = private_tempdir();
    let path = parent.path().to_path_buf();
    let background = client.clone();
    let task =
        tokio::spawn(async move { background.download_file(descriptor.key.id, &path).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    let verified = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .unwrap();
    drop(verified);
    running.stop().await;
}

#[tokio::test]
async fn unauthenticated_requests_and_unsafe_config_fail_closed() {
    let (_dir, _store, service) = setup();
    let running = Running::start(service, Mode::Normal).await;
    let mut config = running.config();
    config.token_env = None;
    let client = NativeClient::connect(config).await.unwrap();
    assert_eq!(
        client.open(vec!["main".into()]).await.unwrap_err().code(),
        Code::Unauthenticated
    );
    let mut config = running.config();
    config.endpoint = "http://192.0.2.1:8093".into();
    assert!(config.validate().is_err());
    config.endpoint = "http://127.0.0.1:8093/path".into();
    assert!(config.validate().is_err());
    let parent = private_tempdir();
    let path = parent.path().join("config.toml");
    std::fs::write(&path, "unknown_secret = 'not-a-real-secret'").unwrap();
    let error = Config::load(&path).err().unwrap().to_string();
    assert!(!error.contains("not-a-real-secret"));
    running.stop().await;
}

#[tokio::test]
async fn tls_client_verifies_explicit_roots_and_server_name() {
    let (_dir, _store, service) = setup();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let identity = tonic::transport::Identity::from_pem(
        std::fs::read(env!("TLS_SERVER_CERT")).unwrap(),
        std::fs::read(env!("TLS_SERVER_KEY")).unwrap(),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(tinytree_server::serve_tcp(
        listener,
        service,
        Some(identity),
        async {
            let _ = stopped.await;
        },
    ));
    let mut config = Config::default();
    config.endpoint = format!("https://{address}");
    config.repository_id = "example".into();
    config.token_env = Some("TINYTREE_CLIENT_TEST_TOKEN".into());
    config.tls = Some(tinytree_client::Tls {
        ca_certificate: env!("TLS_CA_CERT").into(),
        domain_name: Some("localhost".into()),
    });
    config.rpc.retry.max_attempts = 1;
    let client = NativeClient::connect(config.clone()).await.unwrap();
    assert_eq!(
        client
            .open(vec!["main".into()])
            .await
            .unwrap()
            .repository_id,
        "example"
    );
    drop(client);
    config.tls.as_mut().unwrap().domain_name = Some("wrong.example".into());
    assert!(NativeClient::connect(config.clone()).await.is_err());
    config.tls.as_mut().unwrap().domain_name = Some("localhost".into());
    config.tls.as_mut().unwrap().ca_certificate = env!("TLS_WRONG_CA_CERT").into();
    assert!(NativeClient::connect(config.clone()).await.is_err());
    let roots: Arc<[u8]> = std::fs::read(env!("TLS_CA_CERT")).unwrap().into();
    let frozen = NativeClient::connect_with_ca_certificate(config.clone(), roots.clone())
        .await
        .unwrap();
    assert_eq!(
        frozen
            .open(vec!["main".into()])
            .await
            .unwrap()
            .repository_id,
        "example"
    );
    drop(frozen);
    config.tls.as_mut().unwrap().domain_name = Some("wrong.example".into());
    assert!(
        NativeClient::connect_with_ca_certificate(config, roots)
            .await
            .is_err()
    );
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn private_unix_client_connects_and_rejects_public_parent() {
    use std::os::unix::fs::PermissionsExt;
    let (_dir, _store, service) = setup();
    let parent = private_tempdir();
    let directory = parent.path().canonicalize().unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = directory.join("repository.sock");
    let path = socket.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        tinytree_server::serve_unix(&path, service, async {
            let _ = stopped.await;
        })
        .await
    });
    let mut config = Config::default();
    config.endpoint = format!("unix://{}", socket.display());
    config.repository_id = "example".into();
    config.token_env = Some("TINYTREE_CLIENT_TEST_TOKEN".into());
    let client = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if task.is_finished() {
                panic!("Unix listener exited before readiness");
            }
            if let Ok(client) = NativeClient::connect(config.clone()).await {
                break client;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        client
            .open(vec!["main".into()])
            .await
            .unwrap()
            .repository_id,
        "example"
    );
    drop(client);
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(NativeClient::connect(config).await.is_err());
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn fifo_upload_source_without_writer_is_rejected_and_releases_all_quota() {
    use std::os::unix::ffi::OsStrExt;
    let (_dir, store, service) = setup();
    let running = Running::start(service, Mode::Normal).await;
    let mut config = running.config();
    config.limits.max_inflight = 1;
    config.limits.max_file_bytes = 4096;
    config.limits.max_temporary_bytes = 4096;
    config.rpc.operation_timeout_ms = 100;
    let client = NativeClient::connect(config).await.unwrap();
    let parent = private_tempdir();
    let path = parent.path().join("no-writer-fifo");
    let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let descriptor = tinytree_model::describe_file(b"regular bytes");
    let error = tokio::time::timeout(
        Duration::from_millis(250),
        client.prepare_upload_file(&path, descriptor, parent.path()),
    )
    .await
    .expect("FIFO open must not strand a blocking thread")
    .err()
    .unwrap();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        std::fs::read_dir(parent.path()).unwrap().count(),
        1,
        "only caller-owned FIFO remains"
    );
    store
        .put("example", descriptor, b"regular bytes", 0)
        .unwrap();
    let verified = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .unwrap();
    drop(verified);
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 1);
    running.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn staging_requires_private_parent_and_replaced_temp_never_opens_or_installs() {
    use std::os::unix::fs::PermissionsExt;
    let (_dir, store, service) = setup();
    let bytes = b"verified native content";
    let descriptor = tinytree_model::describe_file(bytes);
    store.put("example", descriptor, bytes, 0).unwrap();
    let running = Running::start(service, Mode::Normal).await;
    let client = running.client().await;
    let parent = private_tempdir();
    std::fs::set_permissions(parent.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
    assert_eq!(
        client
            .download_file(descriptor.key.id, parent.path())
            .await
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    std::fs::set_permissions(parent.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let verified = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .unwrap();
    let original = std::fs::read_dir(parent.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let moved = parent.path().join("caller-moved-verified");
    std::fs::rename(&original, &moved).unwrap();
    std::fs::write(&original, b"substituted bytes").unwrap();
    assert_eq!(verified.open().err().unwrap().code(), Code::DataLoss);
    let destination = parent.path().join("must-not-exist");
    assert_eq!(
        verified.persist_noclobber(&destination).unwrap_err().code(),
        Code::DataLoss
    );
    assert!(!destination.exists());
    assert_eq!(
        std::fs::read(&original).unwrap(),
        b"substituted bytes",
        "cleanup must preserve a replacement path"
    );
    running.stop().await;
}

#[tokio::test]
async fn cancelled_cross_client_upload_keeps_queued_worker_owners() {
    let (_dir, _store, service) = setup();
    let running = Running::start(service, Mode::Normal).await;
    let producer = running.client().await;
    let parent = private_tempdir();
    let path = parent.path().join("input");
    let bytes = b"cross-client upload";
    std::fs::write(&path, bytes).unwrap();
    let source = producer
        .prepare_upload_file(&path, tinytree_model::describe_file(bytes), parent.path())
        .await
        .unwrap();
    let config = running.config();
    let (done, finished) = tokio::sync::oneshot::channel();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async move {
            let released = Arc::new(AtomicUsize::new(0));
            let consumer = NativeClient::connect(config)
                .await
                .unwrap()
                .with_lifetime_guard(Arc::new(LifetimeProbe(released.clone())));
            let (started, ready) = tokio::sync::oneshot::channel();
            let (unblock, blocked) = std::sync::mpsc::channel();
            let occupied = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
            });
            ready.await.unwrap();
            {
                let upload = consumer.upload_file(source);
                tokio::pin!(upload);
                assert!(futures_util::poll!(&mut upload).is_pending());
            }
            drop(consumer);
            // The receiving client, not the source's producer, owns this guard.
            // Its cancelled upload has queued physical work behind the barrier.
            assert_eq!(released.load(Ordering::SeqCst), 0);
            unblock.send(()).unwrap();
            occupied.await.unwrap();
            assert_released(&released).await;
        });
        done.send(()).unwrap();
    });
    finished.await.unwrap();
    worker.join().unwrap();
    running.stop().await;
}

#[tokio::test]
async fn readonly_reader_keeps_disk_budget_until_reader_is_dropped() {
    let (_dir, store, service) = setup();
    let bytes = b"native reader bytes";
    let descriptor = tinytree_model::describe_file(bytes);
    store.put("example", descriptor, bytes, 0).unwrap();
    let running = Running::start(service, Mode::Normal).await;
    let mut config = running.config();
    config.limits.max_file_bytes = 4096;
    config.limits.max_temporary_bytes = 4096;
    config.rpc.operation_timeout_ms = 100;
    let released = Arc::new(AtomicUsize::new(0));
    let client = NativeClient::connect(config)
        .await
        .unwrap()
        .with_lifetime_guard(Arc::new(LifetimeProbe(released.clone())));
    let parent = private_tempdir();
    let verified = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .unwrap();
    let reader = verified.open().unwrap();
    drop(verified);
    assert_eq!(
        client
            .download_file(descriptor.key.id, parent.path())
            .await
            .err()
            .unwrap()
            .code(),
        Code::DeadlineExceeded
    );
    drop(reader);
    let verified = client
        .download_file(descriptor.key.id, parent.path())
        .await
        .unwrap();
    let final_reader = verified.open().unwrap();
    drop(verified);
    drop(client);
    assert_eq!(released.load(Ordering::SeqCst), 0);
    drop(final_reader);
    assert_released(&released).await;
    running.stop().await;
}

#[tokio::test]
async fn committed_publication_with_withheld_ack_reconciles_after_rpc_attempt_timeout() {
    let (_dir, store, service) = setup();
    let fixture = fixture();
    let mut commit = None;
    for object in fixture.objects() {
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
        if object.descriptor.key.kind == Kind::Commit {
            commit = Some(object.descriptor.key);
        }
    }
    let running = Running::start(service, Mode::DelayedPublication).await;
    let mut config = running.config();
    config.rpc.rpc_timeout_ms = 20;
    config.rpc.operation_timeout_ms = 500;
    config.rpc.retry.max_attempts = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let commit = commit.unwrap();
    let request = p::PublishReferenceRequest {
        repository_id: "example".into(),
        idempotency_key: "delayed-durable-publication".into(),
        reference_name: "main".into(),
        expected_generation: 0,
        new_commit: Some(p::ObjectKey {
            kind: Kind::Commit as i32,
            native_id: commit.id.0.to_vec(),
        }),
        staging_leases: vec![],
    };
    let receipt = client.publish(request.clone()).await.unwrap();
    assert_eq!(receipt.reference.as_ref().unwrap().generation, 1);
    assert_eq!(
        client.publication(&request.idempotency_key).await.unwrap(),
        receipt
    );
    running.stop().await;
}

#[tokio::test]
async fn compressed_oversized_metadata_is_rejected_by_method_decoder_limit() {
    let (_dir, store, service) = setup();
    let object = tinytree_model::empty_tree();
    store
        .put("example", object.descriptor, &object.bytes, 0)
        .unwrap();
    let running = Running::start(service, Mode::OversizedMetadata).await;
    let mut config = running.config();
    config.limits.max_metadata_bytes = 1024;
    config.limits.max_batch_bytes = 1024;
    config.limits.max_graph_bytes = 64 * 1024 * 1024;
    let client = NativeClient::connect(config).await.unwrap();
    let status = client
        .read_objects(vec![object.descriptor.key])
        .await
        .err()
        .unwrap();
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(
        client
            .open(vec!["main".into()])
            .await
            .unwrap()
            .references
            .len(),
        1
    );
    running.stop().await;
}

#[tokio::test]
async fn reconciliation_replays_full_inputs_instead_of_accepting_incomplete_receipt_binding() {
    let (_dir, store, service) = setup();
    let fixture = fixture();
    let mut commit = None;
    for object in fixture.objects() {
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
        if object.descriptor.key.kind == Kind::Commit {
            commit = Some(object.descriptor.key);
        }
    }
    let running = Running::start(service.clone(), Mode::Normal).await;
    let client = running.client().await;
    let commit = commit.unwrap();
    let mut request = p::PublishReferenceRequest {
        repository_id: "example".into(),
        idempotency_key: "full-input-idempotency".into(),
        reference_name: "main".into(),
        expected_generation: 0,
        new_commit: Some(p::ObjectKey {
            kind: Kind::Commit as i32,
            native_id: commit.id.0.to_vec(),
        }),
        staging_leases: vec![],
    };
    client.publish(request.clone()).await.unwrap();
    drop(client);
    running.stop().await;
    let running = Running::start(service, Mode::BeforePublication).await;
    let mut config = running.config();
    config.rpc.retry.max_attempts = 1;
    let client = NativeClient::connect(config).await.unwrap();
    request.staging_leases = vec!["different-original-lease-input".into()];
    assert_eq!(
        client.publish(request).await.unwrap_err().code(),
        Code::AlreadyExists
    );
    running.stop().await;
}

#[tokio::test]
async fn compressed_repeated_collection_shapes_are_bounded_before_prost_allocation() {
    struct Case {
        mode: Mode,
        expected: Code,
    }
    for case in [
        Case {
            mode: Mode::EmptyReferences,
            expected: Code::DataLoss,
        },
        Case {
            mode: Mode::TooManyReferences,
            expected: Code::ResourceExhausted,
        },
        Case {
            mode: Mode::EmptyGraphEntries,
            expected: Code::DataLoss,
        },
        Case {
            mode: Mode::EmptyMissingKeys,
            expected: Code::DataLoss,
        },
    ] {
        let (_dir, _store, service) = setup();
        let running = Running::start(service, case.mode).await;
        let client = running.client().await;
        let status = match case.mode {
            Mode::EmptyReferences | Mode::TooManyReferences => {
                client.open(vec!["main".into()]).await.err().unwrap()
            }
            Mode::EmptyGraphEntries => client
                .canonical_commit_summaries(
                    vec![ObjectKey {
                        kind: Kind::Commit,
                        id: ObjectId([0; 32]),
                    }],
                    vec![],
                )
                .await
                .err()
                .unwrap(),
            Mode::EmptyMissingKeys => client.find_missing(vec![]).await.err().unwrap(),
            _ => unreachable!(),
        };
        assert_eq!(status.code(), case.expected);
        running.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual same-server graph wire A/B benchmark; no full-first-page cost claim"]
async fn canonical_graph_payload_benchmark() {
    for depth in [16usize, 128, 512] {
        for mode in [Mode::LegacyGraph, Mode::Normal] {
            let (_directory, store, service) = setup();
            let mut commit = tinytree_model::synthetic_root();
            let mut head = tinytree_model::CommitId::ROOT;
            for index in 0..depth {
                commit.parents = vec![head];
                commit.description = format!("history commit {index}");
                commit.change_id = tinytree_model::ChangeId([index as u8; 16]);
                let encoded = tinytree_model::encode(
                    &tinytree_model::Object::Commit(commit.clone()),
                    Default::default(),
                )
                .unwrap();
                store
                    .put("example", encoded.descriptor, &encoded.bytes, 0)
                    .unwrap();
                head = tinytree_model::CommitId(encoded.descriptor.key.id.0);
            }
            // Production's accepted TCP sockets use TCP_NODELAY. Apply the same
            // policy to both arms; this compares additive wire payloads on the
            // SAME current server, not old/new server source or bounded work.
            let running = Running::start_with_socket_policy(service, mode, true).await;
            let client = running.client().await;
            let mut samples = Vec::new();
            for _ in 0..3 {
                let started = std::time::Instant::now();
                let graph = client
                    .canonical_commit_summaries(
                        vec![ObjectKey {
                            kind: Kind::Commit,
                            id: ObjectId(head.0),
                        }],
                        Vec::new(),
                    )
                    .await
                    .unwrap();
                samples.push(started.elapsed().as_nanos());
                assert_eq!(graph.entries.len(), depth);
            }
            let mut ordered = samples.clone();
            ordered.sort_unstable();
            assert_eq!(running.files.load(Ordering::SeqCst), 0);
            assert_eq!(
                running.metadata.load(Ordering::SeqCst),
                if mode == Mode::Normal { 0 } else { depth * 3 }
            );
            let arm = if mode == Mode::Normal {
                "canonical_payload"
            } else {
                "legacy_missing_payload"
            };
            eprintln!(
                "{{\"schema\":1,\"benchmark\":\"canonical_graph_wire_ab\",\"arm\":\"{arm}\",\"history_commits\":{depth},\"trials\":3,\"samples_ns\":{samples:?},\"median_ns\":{},\"graph_rpcs\":{},\"metadata_rpcs\":{},\"canonical_bytes\":{},\"file_rpcs\":{},\"tcp_nodelay\":true}}",
                ordered[1],
                running.graph.load(Ordering::SeqCst),
                running.metadata.load(Ordering::SeqCst),
                running.canonical_bytes.load(Ordering::SeqCst),
                running.files.load(Ordering::SeqCst)
            );
            running.stop().await;
        }
    }
}

// Query fixtures exercise the real authenticated scope/index service. Faults
// only alter the transport reply after ordinary scope admission/authorization.
use q::repository_query_server::{RepositoryQuery, RepositoryQueryServer};
use tinytree_client::{ChangePrefix, CommitPrefix, FactAnswer, PrefixTarget, QueryFact};
use tinytree_proto::query_v1 as q;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryMode {
    Normal,
    CloudMissingBinding,
    CloudWrongBinding,
    CloudBindingWidth,
    CloudLegacyVersion,
    CloudRetry,
    CloudPause,
    RetryOpen,
    RetryOverlay,
    PauseFork,
    ForkWrongEpoch,
    ForkWrongHighWater,
    ForkExtendedExpiry,
    ForkParentHandle,
    MalformedScope,
    LegacyScopeVersion,
    MissingFact,
    ExtraFact,
    MissingAnswer,
    WrongAnswer,
    WrongDigest,
    WrongEpoch,
    RepeatedEmptyIds,
    MissingVisibility,
    DuplicateTarget,
    WrongCommitWidth,
    WrongShortestKind,
    LongestOverflow,
    PrefixSelectionIgnored,
    PrefixSelectionDigestWrong,
    ShortestSelectionMissing,
    ShortestFalseWithLength,
    EvaluateRetry,
    EvaluateRetryAfterScopeClose,
    EvaluatePauseAfter,
    EvaluatePause,
    EvaluateDelay,
    EvaluateMissingExhausted,
    EvaluateWrongDigest,
    EvaluateWrongEpoch,
    EvaluateWrongSequence,
    EvaluateCommitWidth,
    EvaluateChangeWidth,
    EvaluateEdges,
    EvaluateCanonical,
    EvaluateCursorMismatch,
    EvaluateDuplicate,
    LiteralOverReturn,
    GraphMissingCanonical,
    GraphBadCanonical,
    GraphCanonicalChange,
    GraphEdgeWidth,
    GraphUnknownDirect,
    GraphDuplicateEdges,
    CloseQueryRetry,
    CloseQueryPause,
    CloseQueryUnavailable,
    CloseQueryInitialReject,
}
#[derive(Default)]
struct QueryCounts {
    opens: Arc<AtomicUsize>,
    forks: AtomicUsize,
    fork_entered: tokio::sync::Notify,
    fork_release: tokio::sync::Notify,
    closes: AtomicUsize,
    facts: AtomicUsize,
    commit_prefixes: AtomicUsize,
    change_prefixes: AtomicUsize,
    shortest_prefixes: AtomicUsize,
    evaluations: AtomicUsize,
    lost_final_attempts: AtomicUsize,
    query_closes: AtomicUsize,
    close_query_entered: tokio::sync::Notify,
    close_query_release: tokio::sync::Notify,
    evaluate_entered: tokio::sync::Notify,
    evaluate_release: tokio::sync::Notify,
}
#[derive(Clone)]
struct QueryFaults {
    service: tinytree_server::QueryService,
    mode: QueryMode,
    counts: Arc<QueryCounts>,
    first_scope: Arc<std::sync::Mutex<Option<q::QueryScope>>>,
    first_page: Arc<std::sync::Mutex<Option<q::EvaluatePage>>>,
}
#[tonic::async_trait]
impl RepositoryQuery for QueryFaults {
    // SDK transport boundary fixture: no claim that this synthetic echo
    // authorizes a real stored portable index. Server acceptance is separate.
    async fn open_cloud_scope(
        &self,
        request: Request<Streaming<q::OpenScopeFrame>>,
    ) -> Result<Response<q::QueryScope>, Status> {
        let mut stream = request.into_inner();
        let Some(q::OpenScopeFrame {
            frame: Some(q::open_scope_frame::Frame::Header(header)),
        }) = stream.message().await?
        else {
            return Err(Status::invalid_argument("missing header"));
        };
        assert_eq!(header.version, 2);
        assert!(
            header.roots.is_empty(),
            "expected anchors must never be wire grants"
        );
        let source = header.cloud_source.expect("explicit retained source");
        assert!(!source.index_root_ids.is_empty());
        assert!(source.index_root_ids.windows(2).all(|ids| ids[0] < ids[1]));
        let mut ended = false;
        while let Some(frame) = stream.message().await? {
            if matches!(frame.frame, Some(q::open_scope_frame::Frame::ScopeEnd(_))) {
                ended = true;
            }
        }
        assert!(ended);
        let attempt = self.counts.opens.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::CloudPause {
            self.counts.fork_entered.notify_one();
            tokio::time::timeout(Duration::from_secs(5), self.counts.fork_release.notified())
                .await
                .map_err(|_| {
                    Status::deadline_exceeded("cloud scope fixture release not signalled")
                })?;
        }
        let epoch = [3; 16];
        let selected: Vec<[u8; 32]> = source
            .index_root_ids
            .iter()
            .map(|id| id.as_slice().try_into().unwrap())
            .collect();
        let binding = tinytree_model::cloud_query_source_binding(
            source
                .authority
                .unwrap()
                .binding_digest
                .as_slice()
                .try_into()
                .unwrap(),
            &selected,
            &[tinytree_model::CommitId::ROOT],
            epoch,
        );
        let mut scope = q::QueryScope {
            key: Some(q::ScopeKey {
                repository_id: header.repository_id,
                handle: vec![9; 32],
            }),
            content_digest: vec![4; 32],
            index_epoch: epoch.to_vec(),
            high_water: 0,
            expires_at_unix_seconds: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                + 60,
            query_identity_version: 2,
            cloud_source_binding: binding.to_vec(),
        };
        if self.mode == QueryMode::CloudRetry {
            if attempt == 0 {
                *self.first_scope.lock().unwrap() = Some(scope);
                return Err(Status::unavailable("cloud scope ACK withheld"));
            }
            scope = self.first_scope.lock().unwrap().clone().unwrap();
        }
        match self.mode {
            QueryMode::CloudMissingBinding => scope.cloud_source_binding.clear(),
            QueryMode::CloudWrongBinding => scope.cloud_source_binding[0] ^= 1,
            QueryMode::CloudBindingWidth => {
                scope.cloud_source_binding.pop();
            }
            QueryMode::CloudLegacyVersion => scope.query_identity_version = 1,
            _ => {}
        }
        *self.first_scope.lock().unwrap() = Some(scope.clone());
        Ok(Response::new(scope))
    }
    async fn open_scope(
        &self,
        request: Request<Streaming<q::OpenScopeFrame>>,
    ) -> Result<Response<q::QueryScope>, Status> {
        let mut reply = self.service.open_scope(request).await?;
        let attempt = self.counts.opens.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::LegacyScopeVersion {
            reply.get_mut().query_identity_version = 0;
        }
        if matches!(self.mode, QueryMode::RetryOpen | QueryMode::RetryOverlay) {
            if attempt == 0 {
                *self.first_scope.lock().unwrap() = Some(reply.get_ref().clone());
                return Err(Status::unavailable("scope ACK withheld for replay test"));
            }
            if attempt == 1 && self.first_scope.lock().unwrap().as_ref() != Some(reply.get_ref()) {
                return Err(Status::data_loss("scope retry changed replay identity"));
            }
        }
        if self.mode == QueryMode::ForkParentHandle {
            *self.first_scope.lock().unwrap() = Some(reply.get_ref().clone());
        }
        if self.mode == QueryMode::MalformedScope {
            reply.get_mut().key.as_mut().unwrap().handle.pop();
        }
        Ok(reply)
    }
    async fn fork_scope(
        &self,
        request: Request<Streaming<q::ForkScopeFrame>>,
    ) -> Result<Response<q::QueryScope>, Status> {
        let cloud_parent = self
            .first_scope
            .lock()
            .unwrap()
            .clone()
            .filter(|scope| !scope.cloud_source_binding.is_empty());
        if let Some(mut scope) = cloud_parent {
            let mut stream = request.into_inner();
            let Some(q::ForkScopeFrame {
                frame: Some(q::fork_scope_frame::Frame::Header(header)),
            }) = stream.message().await?
            else {
                return Err(Status::invalid_argument("missing fork header"));
            };
            let source = header.scope.unwrap();
            assert!(source.cloud_source.is_none());
            assert!(source.roots.is_empty());
            assert_eq!(source.version, 2);
            assert_eq!(header.parent_scope, scope.key);
            while stream.message().await?.is_some() {}
            self.counts.forks.fetch_add(1, Ordering::SeqCst);
            scope.key.as_mut().unwrap().handle = vec![10; 32];
            return Ok(Response::new(scope));
        }
        let attempt = self.counts.forks.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::PauseFork {
            self.counts.fork_entered.notify_one();
            self.counts.fork_release.notified().await;
        }
        let mut response = self.service.fork_scope(request).await?;
        if self.mode == QueryMode::RetryOverlay {
            if attempt == 0 {
                *self.first_scope.lock().unwrap() = Some(response.get_ref().clone());
                return Err(Status::unavailable("fork ACK withheld for replay test"));
            }
            if attempt == 1 && self.first_scope.lock().unwrap().as_ref() != Some(response.get_ref())
            {
                return Err(Status::data_loss("fork retry changed replay identity"));
            }
        }
        match self.mode {
            QueryMode::ForkWrongEpoch => response.get_mut().index_epoch[0] ^= 1,
            QueryMode::ForkWrongHighWater => response.get_mut().high_water += 1,
            QueryMode::ForkExtendedExpiry => response.get_mut().expires_at_unix_seconds += 86400,
            QueryMode::ForkParentHandle => {
                response.get_mut().key.as_mut().unwrap().handle = self
                    .first_scope
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .key
                    .as_ref()
                    .unwrap()
                    .handle
                    .clone()
            }
            _ => {}
        }
        Ok(response)
    }
    async fn close_scope(
        &self,
        request: Request<q::CloseScopeRequest>,
    ) -> Result<Response<q::CloseScopeResponse>, Status> {
        self.counts.closes.fetch_add(1, Ordering::SeqCst);
        self.service.close_scope(request).await
    }
    async fn close_query(
        &self,
        request: Request<q::CloseQueryRequest>,
    ) -> Result<Response<q::CloseQueryResponse>, Status> {
        let attempt = self.counts.query_closes.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::CloseQueryPause {
            self.counts.close_query_entered.notify_one();
            self.counts.close_query_release.notified().await;
        }
        if self.mode == QueryMode::CloseQueryInitialReject && attempt < 3 {
            return Err(Status::permission_denied(
                "initial close intentionally rejected",
            ));
        }
        if self.mode == QueryMode::CloseQueryUnavailable {
            return Err(Status::unavailable("query close intentionally unavailable"));
        }
        let response = self.service.close_query(request).await?;
        if self.mode == QueryMode::CloseQueryRetry && attempt == 0 {
            return Err(Status::unavailable("query close ACK withheld"));
        }
        Ok(response)
    }
    async fn query_facts(
        &self,
        request: Request<q::QueryFactsRequest>,
    ) -> Result<Response<q::GraphFacts>, Status> {
        self.counts.facts.fetch_add(1, Ordering::SeqCst);
        let mut reply = self.service.query_facts(request).await?;
        let value = reply.get_mut();
        match self.mode {
            QueryMode::MissingFact => {
                value.answers.pop();
            }
            QueryMode::ExtraFact => value.answers.push(value.answers[0].clone()),
            QueryMode::MissingAnswer => value.answers[0].answer = None,
            QueryMode::WrongAnswer => {
                value.answers[0].answer = Some(q::graph_fact_answer::Answer::IsAncestor(false))
            }
            QueryMode::WrongDigest => value.scope_digest[0] ^= 1,
            QueryMode::WrongEpoch => value.index_epoch[0] ^= 1,
            QueryMode::RepeatedEmptyIds => {
                value.answers[0].answer = Some(q::graph_fact_answer::Answer::Heads(q::CommitSet {
                    commits: vec![q::CommitId::default(); 257],
                }))
            }
            _ => {}
        }
        Ok(reply)
    }
    async fn resolve_commit_prefix(
        &self,
        mut request: Request<q::ResolveCommitPrefixRequest>,
    ) -> Result<Response<q::CommitPrefixResult>, Status> {
        self.counts.commit_prefixes.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::PrefixSelectionIgnored {
            request.get_mut().selection = None;
        }
        let mut reply = self.service.resolve_commit_prefix(request).await?;
        if self.mode == QueryMode::PrefixSelectionDigestWrong {
            reply.get_mut().selection_digest = Some(vec![9; 32]);
        }

        if self.mode == QueryMode::WrongCommitWidth {
            if let Some(q::commit_prefix_result::Result::Single(value)) =
                &mut reply.get_mut().result
            {
                value.commit.as_mut().unwrap().value.pop();
            }
        }
        Ok(reply)
    }
    async fn resolve_change_prefix(
        &self,
        mut request: Request<q::ResolveChangePrefixRequest>,
    ) -> Result<Response<q::ChangePrefixResult>, Status> {
        self.counts.change_prefixes.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::PrefixSelectionIgnored {
            request.get_mut().selection = None;
        }
        let mut reply = self.service.resolve_change_prefix(request).await?;
        if self.mode == QueryMode::PrefixSelectionDigestWrong {
            reply.get_mut().selection_digest = Some(vec![9; 32]);
        }

        if let Some(q::change_prefix_result::Result::Single(value)) = &mut reply.get_mut().result {
            match self.mode {
                QueryMode::MissingVisibility => value.targets[0].visible = None,
                QueryMode::DuplicateTarget => value.targets.push(value.targets[0].clone()),
                _ => {}
            }
        }
        Ok(reply)
    }
    async fn shortest_prefix(
        &self,
        mut request: Request<q::ShortestPrefixRequest>,
    ) -> Result<Response<q::ShortestPrefixResult>, Status> {
        self.counts.shortest_prefixes.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::PrefixSelectionIgnored {
            request.get_mut().selection = None;
        }
        let mut reply = self.service.shortest_prefix(request).await?;
        if self.mode == QueryMode::PrefixSelectionDigestWrong {
            reply.get_mut().selection_digest = Some(vec![9; 32]);
        }

        if self.mode == QueryMode::WrongShortestKind {
            reply.get_mut().kind = q::PrefixKind::ChangePrefix as i32;
        }
        if self.mode == QueryMode::LongestOverflow {
            reply.get_mut().hex_length = 65;
        }
        if self.mode == QueryMode::ShortestSelectionMissing {
            reply.get_mut().in_selection = None;
        }
        if self.mode == QueryMode::ShortestFalseWithLength {
            reply.get_mut().in_selection = Some(false);
            reply.get_mut().hex_length = 1;
        }
        Ok(reply)
    }
    async fn evaluate(
        &self,
        request: Request<q::EvaluateRequest>,
    ) -> Result<Response<q::EvaluatePage>, Status> {
        let attempt = self.counts.evaluations.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::EvaluatePause {
            self.counts.evaluate_entered.notify_one();
            self.counts.evaluate_release.notified().await;
        }
        if self.mode == QueryMode::EvaluateDelay {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let close_key = request.get_ref().scope.clone();
        let close_metadata = request.metadata().clone();
        let mut response = self.service.evaluate(request).await?;
        if self.mode == QueryMode::EvaluatePauseAfter && attempt == 0 {
            self.counts.evaluate_entered.notify_one();
            self.counts.evaluate_release.notified().await;
        }
        if self.mode == QueryMode::EvaluateRetry {
            if attempt == 0 {
                *self.first_page.lock().unwrap() = Some(response.get_ref().clone());
                return Err(Status::unavailable("Evaluate ACK withheld for replay test"));
            }
            if attempt == 1 && self.first_page.lock().unwrap().as_ref() != Some(response.get_ref())
            {
                return Err(Status::data_loss("Evaluate retry changed page bytes"));
            }
        }
        if self.mode == QueryMode::EvaluateRetryAfterScopeClose
            && response.get_ref().exhausted == Some(true)
        {
            let final_attempt = self
                .counts
                .lost_final_attempts
                .fetch_add(1, Ordering::SeqCst);
            if final_attempt == 0 {
                *self.first_page.lock().unwrap() = Some(response.get_ref().clone());
                let mut close = Request::new(q::CloseScopeRequest { scope: close_key });
                *close.metadata_mut() = close_metadata;
                self.service.close_scope(close).await?;
                return Err(Status::unavailable(
                    "final Evaluate ACK withheld after scope close",
                ));
            }
            if final_attempt == 1
                && self.first_page.lock().unwrap().as_ref() != Some(response.get_ref())
            {
                return Err(Status::data_loss(
                    "closed-scope final replay changed page bytes",
                ));
            }
        }
        let page = response.get_mut();
        match self.mode {
            QueryMode::EvaluateMissingExhausted => page.exhausted = None,
            QueryMode::EvaluateWrongDigest => page.scope_digest[0] ^= 1,
            QueryMode::EvaluateWrongEpoch => page.index_epoch[0] ^= 1,
            QueryMode::EvaluateWrongSequence => page.sequence += 1,
            QueryMode::EvaluateCommitWidth => {
                if let Some(row) = page.rows.first_mut() {
                    row.commit.as_mut().unwrap().value.pop();
                }
            }
            QueryMode::EvaluateChangeWidth => {
                if let Some(row) = page.rows.first_mut() {
                    row.change.as_mut().unwrap().value.pop();
                }
            }
            QueryMode::EvaluateEdges => {
                if let Some(row) = page.rows.first_mut() {
                    row.edges = vec![q::SelectedEdge { edge: None }; 100_000];
                }
            }
            QueryMode::EvaluateCanonical => {
                if let Some(row) = page.rows.first_mut() {
                    row.canonical = Some(p::ObjectData::default());
                }
            }
            QueryMode::EvaluateCursorMismatch => {
                page.exhausted = Some(page.exhausted != Some(true))
            }
            QueryMode::GraphMissingCanonical => {
                if let Some(row) = page.rows.first_mut() {
                    row.canonical = None;
                }
            }
            QueryMode::GraphBadCanonical => {
                if let Some(data) = page.rows.first_mut().and_then(|r| r.canonical.as_mut()) {
                    if let Some(byte) = data.canonical_envelope.last_mut() {
                        *byte ^= 1;
                    }
                }
            }
            QueryMode::GraphCanonicalChange => {
                if let Some(change) = page.rows.first_mut().and_then(|r| r.change.as_mut()) {
                    change.value[0] ^= 1;
                }
            }
            QueryMode::GraphEdgeWidth | QueryMode::GraphUnknownDirect => {
                if let Some(row) = page.rows.first_mut() {
                    row.edges = vec![q::SelectedEdge {
                        edge: Some(q::selected_edge::Edge::Direct(q::DirectEdge {
                            target: Some(q::CommitId {
                                value: vec![
                                    255;
                                    if self.mode == QueryMode::GraphEdgeWidth {
                                        31
                                    } else {
                                        32
                                    }
                                ],
                            }),
                        })),
                    }];
                }
            }
            QueryMode::GraphDuplicateEdges => {
                if let Some(row) = page.rows.first_mut() {
                    if let Some(edge) = row.edges.first().cloned() {
                        row.edges.push(edge);
                    }
                }
            }
            QueryMode::LiteralOverReturn => {
                if let Some(row) = page.rows.first().cloned() {
                    page.rows.push(row);
                }
            }
            QueryMode::EvaluateDuplicate => {
                let mut first_page = self.first_page.lock().unwrap();
                if let Some(first) = first_page.as_ref() {
                    if let (Some(row), Some(previous)) = (page.rows.first_mut(), first.rows.first())
                    {
                        *row = previous.clone();
                    }
                } else {
                    *first_page = Some(page.clone());
                }
            }
            _ => {}
        }
        Ok(response)
    }
}
struct QueryRunning {
    config: Config,
    opens: Arc<AtomicUsize>,
    counts: Arc<QueryCounts>,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl QueryRunning {
    async fn start(service: RepositoryService, mode: QueryMode) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let counts = Arc::new(QueryCounts::default());
        let opens = counts.opens.clone();
        let faults = QueryFaults {
            service: service.query_service(),
            mode,
            counts: counts.clone(),
            first_scope: Arc::new(std::sync::Mutex::new(None)),
            first_page: Arc::new(std::sync::Mutex::new(None)),
        };
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let cloud = CloudFaults::default();
        cloud.mode.store(30, Ordering::SeqCst);
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    RepositoryQueryServer::new(faults)
                        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                        .send_compressed(tonic::codec::CompressionEncoding::Zstd),
                )
                .add_service(tinytree_client::cloud_protocol::workspace_cloud_server::WorkspaceCloudServer::new(cloud)
                    .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                    .send_compressed(tonic::codec::CompressionEncoding::Zstd))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        });
        let mut config = Config::default();
        config.endpoint = format!("http://{address}");
        config.repository_id = "example".into();
        config.token_env = Some("TINYTREE_CLIENT_TEST_TOKEN".into());
        config.rpc.operation_timeout_ms = 3000;
        config.rpc.rpc_timeout_ms = 1000;
        config.rpc.retry.initial_backoff_ms = 1;
        config.rpc.retry.max_backoff_ms = 5;
        Self {
            config,
            opens,
            counts,
            stop,
            task,
        }
    }
    async fn client(&self) -> NativeClient {
        NativeClient::connect(self.config.clone()).await.unwrap()
    }
    async fn stop(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap();
    }
}
struct QueryFixture {
    store_directory: tempfile::TempDir,
    index_directory: tempfile::TempDir,
    store: Store,
    index: Arc<tinytree_index::Index>,
    service: RepositoryService,
    root: tinytree_model::CommitId,
    change: tinytree_model::ChangeId,
}
fn setup_query_retained() -> QueryFixture {
    setup_query_configured(ServiceConfig::default())
}
fn setup_query_configured(config: ServiceConfig) -> QueryFixture {
    let store_dir = private_tempdir();
    let store = Store::open(store_dir.path(), Default::default()).unwrap();
    store.create_repository("example").unwrap();
    let service = RepositoryService::new(
        store.clone(),
        config,
        vec!["example".into()],
        vec![Principal::new("test", TOKEN, vec!["example".into()], true).unwrap()],
    )
    .unwrap();
    let index_dir = private_tempdir();
    let index = tinytree_index::Index::open(index_dir.path(), &Default::default()).unwrap();
    let fixture = fixture();
    let mut commit = None;
    for object in fixture.objects() {
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
        if object.descriptor.key.kind == Kind::Commit {
            let node = index
                .ingest("example", object.descriptor, &object.bytes)
                .unwrap();
            commit = Some((node.commit, node.change_id));
        }
    }
    let (root, change) = commit.unwrap();
    QueryFixture {
        store_directory: store_dir,
        index_directory: index_dir,
        store,
        index: index.clone(),
        service: service.with_query_index(index).unwrap(),
        root,
        change,
    }
}
fn setup_query() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    RepositoryService,
    tinytree_model::CommitId,
    tinytree_model::ChangeId,
) {
    let QueryFixture {
        store_directory,
        index_directory,
        service,
        root,
        change,
        ..
    } = setup_query_retained();
    (store_directory, index_directory, service, root, change)
}

fn change_hex(id: tinytree_model::ChangeId) -> String {
    id.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[tokio::test]
async fn compressed_query_scopes_replay_exact_facts_and_explicit_visibility() {
    use tinytree_model::{ChangeId, CommitId};
    let (_store, _index, service, root, change) = setup_query();
    let running = QueryRunning::start(service, QueryMode::RetryOpen).await;
    let client = running.client().await;
    // The real server rejects noncanonical roots; the client canonicalizes sets.
    let scope = client
        .open_query_scope(vec![root, CommitId::ROOT, root])
        .await
        .unwrap();
    assert_eq!(
        running.opens.load(Ordering::SeqCst),
        2,
        "lost ACK retries the same immutable replay key"
    );
    assert_eq!(scope.repository_id(), "example");
    let unknown = CommitId([99; 32]);
    assert_eq!(
        client
            .query_facts(
                &scope,
                vec![
                    QueryFact::HasId(root),
                    QueryFact::HasId(unknown),
                    QueryFact::IsAncestor {
                        ancestor: CommitId::ROOT,
                        descendant: root
                    },
                    QueryFact::Heads(vec![CommitId::ROOT, root]),
                    QueryFact::CommonAncestors {
                        left: vec![root],
                        right: vec![CommitId::ROOT]
                    },
                ]
            )
            .await
            .unwrap(),
        vec![
            FactAnswer::HasId(true),
            FactAnswer::HasId(false),
            FactAnswer::IsAncestor(true),
            FactAnswer::Heads(vec![root]),
            FactAnswer::CommonAncestors(vec![CommitId::ROOT])
        ]
    );
    let text = blake3::Hash::from(root.0).to_hex().to_string();
    assert_eq!(
        client.resolve_commit_prefix(&scope, &text).await.unwrap(),
        CommitPrefix::Single(root)
    );
    assert_eq!(
        client.resolve_commit_prefix(&scope, "").await.unwrap(),
        CommitPrefix::Ambiguous
    );
    assert_eq!(
        client
            .resolve_commit_prefix(&scope, &"99".repeat(32))
            .await
            .unwrap(),
        CommitPrefix::NoMatch
    );
    let ChangePrefix::Single(found) = client
        .resolve_change_prefix(&scope, &change_hex(change), vec![root])
        .await
        .unwrap()
    else {
        panic!("expected unique change");
    };
    assert_eq!(found.change, change);
    assert_eq!(found.targets.len(), 1);
    assert!(found.targets[0].visible);
    let ChangePrefix::Single(hidden) = client
        .resolve_change_prefix(&scope, &change_hex(change), vec![])
        .await
        .unwrap()
    else {
        panic!("expected hidden change target");
    };
    assert!(
        !hidden.targets[0].visible,
        "Some(false) is present, not an omitted field"
    );
    assert!(
        client
            .shortest_prefix(&scope, PrefixTarget::Commit(unknown), vec![])
            .await
            .unwrap()
            <= 64
    );
    assert!(
        client
            .shortest_prefix(&scope, PrefixTarget::Change(ChangeId([88; 16])), vec![root])
            .await
            .unwrap()
            <= 32
    );
    client.close_query_scope(&scope).await.unwrap();
    assert_eq!(
        client
            .query_facts(&scope, vec![QueryFact::HasId(root)])
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let singleton = client.open_query_scope(vec![CommitId::ROOT]).await.unwrap();
    assert_eq!(
        client
            .shortest_prefix(&singleton, PrefixTarget::Commit(CommitId::ROOT), vec![])
            .await
            .unwrap(),
        0
    );
    client.close_query_scope(&singleton).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn query_replies_reject_malformed_scopes_facts_prefixes_and_collection_shapes() {
    use tinytree_model::CommitId;
    for mode in [
        QueryMode::MalformedScope,
        QueryMode::MissingFact,
        QueryMode::MissingAnswer,
        QueryMode::WrongAnswer,
        QueryMode::WrongDigest,
        QueryMode::WrongEpoch,
        QueryMode::RepeatedEmptyIds,
        QueryMode::MissingVisibility,
        QueryMode::DuplicateTarget,
        QueryMode::WrongCommitWidth,
        QueryMode::WrongShortestKind,
        QueryMode::LongestOverflow,
    ] {
        let (_store, _index, service, root, change) = setup_query();
        let running = QueryRunning::start(service, mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![root]).await;
        if mode == QueryMode::MalformedScope {
            assert_eq!(scope.unwrap_err().code(), Code::DataLoss);
        } else {
            let scope = scope.unwrap();
            let failure = match mode {
                QueryMode::MissingVisibility | QueryMode::DuplicateTarget => client
                    .resolve_change_prefix(&scope, &change_hex(change), vec![root])
                    .await
                    .unwrap_err(),
                QueryMode::WrongCommitWidth => client
                    .resolve_commit_prefix(&scope, &blake3::Hash::from(root.0).to_hex())
                    .await
                    .unwrap_err(),
                QueryMode::WrongShortestKind | QueryMode::LongestOverflow => client
                    .shortest_prefix(&scope, PrefixTarget::Commit(root), vec![])
                    .await
                    .unwrap_err(),
                QueryMode::RepeatedEmptyIds => client
                    .query_facts(&scope, vec![QueryFact::Heads(vec![root])])
                    .await
                    .unwrap_err(),
                _ => client
                    .query_facts(&scope, vec![QueryFact::HasId(CommitId([99; 32]))])
                    .await
                    .unwrap_err(),
            };
            assert!(
                matches!(failure.code(), Code::DataLoss | Code::ResourceExhausted),
                "{mode:?}: {failure}"
            );
            client.close_query_scope(&scope).await.unwrap();
        }
        running.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual indexed-fact versus full-history workloads; not equivalent jj log/clone/Bazel latency"]
async fn indexed_scope_fact_workload_benchmark() {
    use tinytree_model::{ChangeId, CommitId};
    for depth in [64usize, 512, 4096] {
        let (_store_directory, store, service) = setup();
        let index_directory = private_tempdir();
        let empty = tinytree_model::empty_tree();
        store
            .put("example", empty.descriptor, &empty.bytes, 0)
            .unwrap();
        let mut commit = tinytree_model::synthetic_root();
        let mut head = CommitId::ROOT;
        let mut canonical = Vec::with_capacity(depth);
        let mut source_commit_bytes = 0u64;
        for position in 0..depth {
            commit.parents = vec![head];
            commit.description = format!("linear history commit {position}");
            let mut change = [0u8; 16];
            change[..8].copy_from_slice(&((position + 1) as u64).to_be_bytes());
            commit.change_id = ChangeId(change);
            let encoded = tinytree_model::encode(
                &tinytree_model::Object::Commit(commit.clone()),
                Default::default(),
            )
            .unwrap();
            source_commit_bytes += encoded.bytes.len() as u64;
            store
                .put("example", encoded.descriptor, &encoded.bytes, 0)
                .unwrap();
            head = CommitId(encoded.descriptor.key.id.0);
            canonical.push(encoded);
        }
        // Index readiness is paid once and reported separately, not hidden in
        // first-scope latency or presented as a free cold-server operation.
        let indexing_started = std::time::Instant::now();
        let index =
            tinytree_index::Index::open(index_directory.path(), &Default::default()).unwrap();
        for encoded in &canonical {
            index
                .ingest("example", encoded.descriptor, &encoded.bytes)
                .unwrap();
        }
        let indexing_ns = indexing_started.elapsed().as_nanos();
        let service = service.with_query_index(index).unwrap();
        // Same authenticated, zstd-enabled current server/channel for both
        // workloads, with production-equivalent accepted TCP_NODELAY sockets.
        let running = Running::start_with_socket_policy(service, Mode::Normal, true).await;
        let client = running.client().await;
        let mut full_graph_ns = Vec::new();
        for _ in 0..3 {
            let started = std::time::Instant::now();
            let graph = client
                .canonical_commit_summaries(
                    vec![ObjectKey {
                        kind: Kind::Commit,
                        id: ObjectId(head.0),
                    }],
                    vec![],
                )
                .await
                .unwrap();
            full_graph_ns.push(started.elapsed().as_nanos());
            assert_eq!(graph.entries.len(), depth);
        }
        let full_graph_rpcs = running.graph.load(Ordering::SeqCst);
        let full_graph_canonical_bytes = running.canonical_bytes.load(Ordering::SeqCst);
        let scope_started = std::time::Instant::now();
        let scope = client.open_query_scope(vec![head]).await.unwrap();
        let first_scope_ns = scope_started.elapsed().as_nanos();
        let mut reopen_ns = Vec::new();
        for _ in 0..3 {
            let started = std::time::Instant::now();
            let reopened = client.open_query_scope(vec![head]).await.unwrap();
            reopen_ns.push(started.elapsed().as_nanos());
            assert_eq!(reopened.content_digest(), scope.content_digest());
            client.close_query_scope(&reopened).await.unwrap();
        }
        let mut facts_ns = Vec::new();
        let mut prefix_ns = Vec::new();
        let head_hex = blake3::Hash::from(head.0).to_hex().to_string();
        let head_change_hex = change_hex(commit.change_id);
        for _ in 0..3 {
            let started = std::time::Instant::now();
            let facts = client
                .query_facts(
                    &scope,
                    vec![
                        QueryFact::HasId(head),
                        QueryFact::IsAncestor {
                            ancestor: CommitId::ROOT,
                            descendant: head,
                        },
                        QueryFact::Heads(vec![CommitId::ROOT, head]),
                        QueryFact::CommonAncestors {
                            left: vec![head],
                            right: vec![CommitId::ROOT],
                        },
                    ],
                )
                .await
                .unwrap();
            facts_ns.push(started.elapsed().as_nanos());
            assert_eq!(
                facts,
                vec![
                    FactAnswer::HasId(true),
                    FactAnswer::IsAncestor(true),
                    FactAnswer::Heads(vec![head]),
                    FactAnswer::CommonAncestors(vec![CommitId::ROOT])
                ]
            );
            let started = std::time::Instant::now();
            assert_eq!(
                client
                    .resolve_commit_prefix(&scope, &head_hex)
                    .await
                    .unwrap(),
                CommitPrefix::Single(head)
            );
            assert!(matches!(
                client
                    .resolve_change_prefix(&scope, &head_change_hex, vec![head])
                    .await
                    .unwrap(),
                ChangePrefix::Single(_)
            ));
            assert!(
                client
                    .shortest_prefix(&scope, PrefixTarget::Commit(head), vec![])
                    .await
                    .unwrap()
                    <= 64
            );
            prefix_ns.push(started.elapsed().as_nanos());
        }
        client.close_query_scope(&scope).await.unwrap();
        assert_eq!(running.files.load(Ordering::SeqCst), 0);
        assert_eq!(running.metadata.load(Ordering::SeqCst), 0);
        assert_eq!(
            running.graph.load(Ordering::SeqCst),
            full_graph_rpcs,
            "small query phase must not fetch canonical public ancestry"
        );
        assert_eq!(
            running.canonical_bytes.load(Ordering::SeqCst),
            full_graph_canonical_bytes
        );
        assert_eq!(running.queries.opens.load(Ordering::SeqCst), 4);
        assert_eq!(running.queries.facts.load(Ordering::SeqCst), 3);
        eprintln!(
            concat!(
                "{{\"schema\":1,\"benchmark\":\"indexed_scope_fact_workloads\",",
                "\"comparison\":\"different_workloads_not_jj_log_clone_or_bazel_speedup\",",
                "\"shape\":\"single_parent_unique_changes_empty_tree_no_files\",",
                "\"history_commits\":{},\"source_commit_bytes\":{},\"trials\":3,",
                "\"server_indexing_ns\":{},\"index_ready_before_requests\":true,",
                "\"full_graph_samples_ns\":{:?},\"first_scope_ns\":{},\"reopen_samples_ns\":{:?},",
                "\"four_fact_samples_ns\":{:?},\"three_prefix_rpc_samples_ns\":{:?},",
                "\"full_graph_rpcs\":{},\"full_graph_canonical_envelope_bytes\":{},",
                "\"scope_open_rpcs\":{},\"scope_close_rpcs\":{},\"fact_rpcs\":{},",
                "\"commit_prefix_rpcs\":{},\"change_prefix_rpcs\":{},\"shortest_prefix_rpcs\":{},",
                "\"query_phase_canonical_envelope_bytes\":0,\"metadata_rpcs\":0,\"file_rpcs\":0,",
                "\"authenticated\":true,\"zstd\":true,\"tcp_nodelay\":true}}"
            ),
            depth,
            source_commit_bytes,
            indexing_ns,
            full_graph_ns,
            first_scope_ns,
            reopen_ns,
            facts_ns,
            prefix_ns,
            full_graph_rpcs,
            full_graph_canonical_bytes,
            running.queries.opens.load(Ordering::SeqCst),
            running.queries.closes.load(Ordering::SeqCst),
            running.queries.facts.load(Ordering::SeqCst),
            running.queries.commit_prefixes.load(Ordering::SeqCst),
            running.queries.change_prefixes.load(Ordering::SeqCst),
            running.queries.shortest_prefixes.load(Ordering::SeqCst)
        );
        running.stop().await;
    }
}

fn private_draft(
    parent: tinytree_model::CommitId,
    change: tinytree_model::ChangeId,
    description: &str,
) -> tinytree_model::EncodedObject {
    let mut commit = tinytree_model::synthetic_root();
    commit.parents = vec![parent];
    commit.change_id = change;
    commit.description = description.into();
    tinytree_model::encode(&tinytree_model::Object::Commit(commit), Default::default()).unwrap()
}
async fn checked_draft(
    client: &NativeClient,
    object: &tinytree_model::EncodedObject,
) -> tinytree_client::VerifiedMetadata {
    client
        .verify_metadata(object.descriptor, object.bytes.clone())
        .await
        .unwrap()
}
#[tokio::test]
async fn private_overlay_forks_replay_without_publication_and_preserve_hidden_divergence() {
    use tinytree_model::{ChangeId, CommitId};
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::RetryOverlay).await;
    let client = running.client().await;
    let change = ChangeId([42; 16]);
    let left = private_draft(fixture.root, change, "left private draft");
    let right = private_draft(fixture.root, change, "divergent hidden draft");
    let left_id = CommitId(left.descriptor.key.id.0);
    let right_id = CommitId(right.descriptor.key.id.0);
    let scope = client
        .open_query_scope_with_overlay(
            vec![fixture.root],
            vec![
                checked_draft(&client, &left).await,
                checked_draft(&client, &right).await,
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        running.opens.load(Ordering::SeqCst),
        2,
        "overlay Open replays exact immutable bytes after lost ACK"
    );
    assert_eq!(scope.remote_roots(), &[fixture.root]);
    let ChangePrefix::Single(divergent) = client
        .resolve_change_prefix(&scope, &change_hex(change), vec![left_id])
        .await
        .unwrap()
    else {
        panic!("expected divergent targets under one ChangeId");
    };
    let visibility: std::collections::BTreeMap<_, _> = divergent
        .targets
        .into_iter()
        .map(|target| (target.commit, target.visible))
        .collect();
    assert_eq!(
        visibility,
        std::collections::BTreeMap::from([(left_id, true), (right_id, false)])
    );
    let child = private_draft(left_id, ChangeId([43; 16]), "child private draft");
    let child_id = CommitId(child.descriptor.key.id.0);
    let fork = client
        .fork_query_scope(&scope, vec![checked_draft(&client, &child).await])
        .await
        .unwrap();
    assert_eq!(
        running.counts.forks.load(Ordering::SeqCst),
        2,
        "Fork replays parent-bound stream without installing another version"
    );
    assert_eq!(fork.remote_roots(), scope.remote_roots());
    assert_eq!(fork.index_epoch(), scope.index_epoch());
    assert_eq!(fork.high_water(), scope.high_water());
    assert!(fork.expires_at_unix_seconds() <= scope.expires_at_unix_seconds());
    assert_ne!(fork.content_digest(), scope.content_digest());
    client.close_query_scope(&scope).await.unwrap();
    assert_eq!(
        client
            .query_facts(
                &fork,
                vec![
                    QueryFact::HasId(right_id),
                    QueryFact::IsAncestor {
                        ancestor: left_id,
                        descendant: child_id
                    },
                    QueryFact::IsAncestor {
                        ancestor: right_id,
                        descendant: child_id
                    }
                ]
            )
            .await
            .unwrap(),
        vec![
            FactAnswer::HasId(true),
            FactAnswer::IsAncestor(true),
            FactAnswer::IsAncestor(false)
        ]
    );
    let remote = client.open_query_scope(vec![fixture.root]).await.unwrap();
    assert_eq!(
        client
            .query_facts(
                &remote,
                vec![QueryFact::HasId(left_id), QueryFact::HasId(child_id)]
            )
            .await
            .unwrap(),
        vec![FactAnswer::HasId(false), FactAnswer::HasId(false)]
    );
    for object in [&left, &right, &child] {
        assert!(
            fixture
                .store
                .descriptor("example", object.descriptor.key)
                .unwrap()
                .is_none(),
            "query drafts must never become authoritative objects"
        );
        assert!(
            fixture
                .index
                .node("example", CommitId(object.descriptor.key.id.0))
                .unwrap()
                .is_none(),
            "private drafts must not enter the global derived index"
        );
    }
    client.close_query_scope(&remote).await.unwrap();
    client.close_query_scope(&fork).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn overlay_preflight_and_fork_reply_validation_fail_without_hydrating_or_publishing() {
    use tinytree_model::{ChangeId, CommitId};
    let (_store, _index, service, root, _change) = setup_query();
    let running = QueryRunning::start(service, QueryMode::Normal).await;
    let client = running.client().await;
    let parent = private_draft(root, ChangeId([51; 16]), "parent draft");
    let child = private_draft(
        CommitId(parent.descriptor.key.id.0),
        ChangeId([52; 16]),
        "child draft",
    );
    for overlay in [
        vec![
            checked_draft(&client, &child).await,
            checked_draft(&client, &parent).await,
        ],
        vec![
            checked_draft(&client, &parent).await,
            checked_draft(&client, &parent).await,
        ],
    ] {
        assert_eq!(
            client
                .open_query_scope_with_overlay(vec![root], overlay)
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    assert_eq!(
        running.opens.load(Ordering::SeqCst),
        0,
        "locally provable invalid overlay never starts an RPC"
    );
    let foreign = private_draft(
        CommitId([81; 32]),
        ChangeId([53; 16]),
        "unknown scoped parent",
    );
    assert_eq!(
        client
            .open_query_scope_with_overlay(vec![root], vec![checked_draft(&client, &foreign).await])
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument,
        "unknown membership is rejected by server, not silently hydrated"
    );
    running.stop().await;
    for mode in [
        QueryMode::ForkWrongEpoch,
        QueryMode::ForkWrongHighWater,
        QueryMode::ForkExtendedExpiry,
        QueryMode::ForkParentHandle,
    ] {
        let (_store, _index, service, root, _change) = setup_query();
        let running = QueryRunning::start(service, mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![root]).await.unwrap();
        let draft = private_draft(root, ChangeId([54; 16]), "fork reply validation");
        assert_eq!(
            client
                .fork_query_scope(&scope, vec![checked_draft(&client, &draft).await])
                .await
                .unwrap_err()
                .code(),
            Code::DataLoss,
            "{mode:?}"
        );
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn cancelled_overlay_fork_releases_client_owners_and_keeps_parent_usable() {
    use tinytree_model::ChangeId;
    let (_store, _index, service, root, _change) = setup_query();
    let running = QueryRunning::start(service, QueryMode::PauseFork).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let draft = private_draft(root, ChangeId([55; 16]), "cancelled private draft");
    let verified = checked_draft(&client, &draft).await;
    let dropped = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(dropped.clone())));
    let parent = scope.clone();
    let task = tokio::spawn(async move { guarded.fork_query_scope(&parent, vec![verified]).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        running.counts.fork_entered.notified(),
    )
    .await
    .unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    task.abort();
    let _ = task.await;
    running.counts.fork_release.notify_one();
    assert_released(&dropped).await;
    assert_eq!(
        client
            .query_facts(&scope, vec![QueryFact::HasId(root)])
            .await
            .unwrap(),
        vec![FactAnswer::HasId(true)]
    );
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual private-overlay FullOpen versus shared Fork benchmark; not native jj startup"]
async fn private_overlay_full_open_versus_incremental_fork_benchmark() {
    use tinytree_model::{ChangeId, CommitId};
    const PUBLIC_HISTORY: usize = 128;
    const CLIENT_BATCH_ITEMS: usize = 1024;
    for private_count in [10usize, 100, 1000] {
        let (_store_directory, store, service) = setup();
        let index_directory = private_tempdir();
        let source_started = std::time::Instant::now();
        let empty = tinytree_model::empty_tree();
        store
            .put("example", empty.descriptor, &empty.bytes, 0)
            .unwrap();
        let mut public = Vec::with_capacity(PUBLIC_HISTORY);
        let mut remote_root = CommitId::ROOT;
        for position in 0..PUBLIC_HISTORY {
            let mut change = [0u8; 16];
            change[..8].copy_from_slice(&((position + 1) as u64).to_be_bytes());
            change[15] = 1;
            let object = private_draft(
                remote_root,
                ChangeId(change),
                &format!("public linear commit {position}"),
            );
            store
                .put("example", object.descriptor, &object.bytes, 0)
                .unwrap();
            remote_root = CommitId(object.descriptor.key.id.0);
            public.push(object);
        }
        let public_source_setup_ns = source_started.elapsed().as_nanos();
        let indexing_started = std::time::Instant::now();
        let index =
            tinytree_index::Index::open(index_directory.path(), &Default::default()).unwrap();
        for object in &public {
            index
                .ingest("example", object.descriptor, &object.bytes)
                .unwrap();
        }
        let ready_index_ns = indexing_started.elapsed().as_nanos();
        let service = service.with_query_index(index.clone()).unwrap();
        let running = Running::start_with_socket_policy(service, Mode::Normal, true).await;
        let mut config = running.config();
        config.limits.max_batch_items = CLIENT_BATCH_ITEMS;
        let client = NativeClient::connect(config).await.unwrap();
        let mut drafts = Vec::with_capacity(private_count + 1);
        let mut parent = remote_root;
        for position in 0..=private_count {
            let mut change = [0u8; 16];
            change[..8].copy_from_slice(&((position + 1) as u64).to_be_bytes());
            change[15] = 2;
            let object = private_draft(
                parent,
                ChangeId(change),
                &format!("private linear draft {position}"),
            );
            parent = CommitId(object.descriptor.key.id.0);
            drafts.push(object);
        }
        let base_head = CommitId(drafts[private_count - 1].descriptor.key.id.0);
        let final_head = CommitId(drafts[private_count].descriptor.key.id.0);
        let base_bytes: usize = drafts[..private_count]
            .iter()
            .map(|object| object.bytes.len())
            .sum();
        let extension_bytes = drafts[private_count].bytes.len();
        let first_verify_started = std::time::Instant::now();
        let mut base_overlay = Vec::with_capacity(private_count);
        for object in &drafts[..private_count] {
            base_overlay.push(checked_draft(&client, object).await);
        }
        let first_base_verify_ns = first_verify_started.elapsed().as_nanos();
        let first_base_started = std::time::Instant::now();
        let base = client
            .open_query_scope_with_overlay(vec![remote_root], base_overlay)
            .await
            .unwrap();
        let first_base_open_ns = first_base_started.elapsed().as_nanos();
        let expected = vec![
            FactAnswer::HasId(true),
            FactAnswer::IsAncestor(true),
            FactAnswer::IsAncestor(true),
        ];
        let facts = || {
            vec![
                QueryFact::HasId(final_head),
                QueryFact::IsAncestor {
                    ancestor: remote_root,
                    descendant: final_head,
                },
                QueryFact::IsAncestor {
                    ancestor: base_head,
                    descendant: final_head,
                },
            ]
        };
        let mut full_verify_ns = Vec::new();
        let mut full_open_ns = Vec::new();
        let mut fork_verify_ns = Vec::new();
        let mut fork_ns = Vec::new();
        let mut fact_validation_ns = Vec::new();
        let mut scope_close_ns = Vec::new();
        for _ in 0..3 {
            let verify_started = std::time::Instant::now();
            let mut overlay = Vec::with_capacity(drafts.len());
            for object in &drafts {
                overlay.push(checked_draft(&client, object).await);
            }
            full_verify_ns.push(verify_started.elapsed().as_nanos());
            let started = std::time::Instant::now();
            let full = client
                .open_query_scope_with_overlay(vec![remote_root], overlay)
                .await
                .unwrap();
            full_open_ns.push(started.elapsed().as_nanos());
            let verify_started = std::time::Instant::now();
            let addition = checked_draft(&client, &drafts[private_count]).await;
            fork_verify_ns.push(verify_started.elapsed().as_nanos());
            let started = std::time::Instant::now();
            let fork = client
                .fork_query_scope(&base, vec![addition])
                .await
                .unwrap();
            fork_ns.push(started.elapsed().as_nanos());
            let facts_started = std::time::Instant::now();
            assert_eq!(client.query_facts(&full, facts()).await.unwrap(), expected);
            assert_eq!(client.query_facts(&fork, facts()).await.unwrap(), expected);
            assert_eq!(
                client
                    .query_facts(
                        &base,
                        vec![QueryFact::HasId(base_head), QueryFact::HasId(final_head)]
                    )
                    .await
                    .unwrap(),
                vec![FactAnswer::HasId(true), FactAnswer::HasId(false)]
            );
            fact_validation_ns.push(facts_started.elapsed().as_nanos());
            // Fork binding can legitimately change a scope's content digest;
            // compare exact graph facts, not representation-specific digests.
            assert_eq!(fork.remote_roots(), base.remote_roots());
            assert_eq!(fork.high_water(), base.high_water());
            assert_eq!(fork.index_epoch(), base.index_epoch());
            let close_started = std::time::Instant::now();
            client.close_query_scope(&full).await.unwrap();
            client.close_query_scope(&fork).await.unwrap();
            scope_close_ns.push(close_started.elapsed().as_nanos());
        }
        let base_close_started = std::time::Instant::now();
        client.close_query_scope(&base).await.unwrap();
        let base_close_ns = base_close_started.elapsed().as_nanos();
        for object in &drafts {
            assert!(
                store
                    .descriptor("example", object.descriptor.key)
                    .unwrap()
                    .is_none()
            );
            assert!(
                index
                    .node("example", CommitId(object.descriptor.key.id.0))
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(running.graph.load(Ordering::SeqCst), 0);
        assert_eq!(running.metadata.load(Ordering::SeqCst), 0);
        assert_eq!(running.canonical_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(running.files.load(Ordering::SeqCst), 0);
        assert_eq!(running.queries.opens.load(Ordering::SeqCst), 4);
        assert_eq!(running.queries.forks.load(Ordering::SeqCst), 3);
        assert_eq!(running.queries.closes.load(Ordering::SeqCst), 7);
        assert_eq!(running.queries.facts.load(Ordering::SeqCst), 9);
        eprintln!(
            concat!(
                "{{\"schema\":1,\"benchmark\":\"private_overlay_full_open_vs_fork\",",
                "\"claim\":\"same_final_private_graph_not_native_jj_startup\",",
                "\"shape\":\"linear_unique_changes_empty_tree_no_files\",",
                "\"public_history_commits\":{},\"private_base_commits\":{},\"added_commits\":1,",
                "\"client_max_batch_items\":{},\"trials\":3,\"index_ready_before_requests\":true,",
                "\"public_source_setup_ns\":{},\"ready_index_ns\":{},",
                "\"first_base_verify_ns\":{},\"first_base_open_ns\":{},",
                "\"full_verify_samples_ns\":{:?},\"full_open_samples_ns\":{:?},",
                "\"fork_verify_samples_ns\":{:?},\"fork_samples_ns\":{:?},",
                "\"three_fact_validation_rpc_samples_ns\":{:?},\"two_scope_close_rpc_samples_ns\":{:?},\"base_close_ns\":{},",
                "\"payload_measure\":\"logical_canonical_envelopes_not_zstd_wire_bytes\",",
                "\"first_base_payload_bytes\":{},\"full_open_payload_bytes_per_trial\":{},\"fork_payload_bytes_per_trial\":{},",
                "\"open_rpcs\":4,\"fork_rpcs\":3,\"close_rpcs\":7,\"fact_rpcs\":9,",
                "\"public_canonical_envelope_bytes_fetched\":0,\"graph_rpcs\":0,\"metadata_rpcs\":0,\"file_rpcs\":0,",
                "\"private_objects_published\":0,\"private_objects_global_indexed\":0,",
                "\"authenticated\":true,\"zstd\":true,\"tcp_nodelay\":true}}"
            ),
            PUBLIC_HISTORY,
            private_count,
            CLIENT_BATCH_ITEMS,
            public_source_setup_ns,
            ready_index_ns,
            first_base_verify_ns,
            first_base_open_ns,
            full_verify_ns,
            full_open_ns,
            fork_verify_ns,
            fork_ns,
            fact_validation_ns,
            scope_close_ns,
            base_close_ns,
            base_bytes,
            base_bytes + extension_bytes,
            extension_bytes
        );
        running.stop().await;
    }
}

fn ids_commits(ids: &[tinytree_model::CommitId]) -> q::GraphExpr {
    q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
            commits: ids
                .iter()
                .map(|id| q::CommitId {
                    value: id.0.to_vec(),
                })
                .collect(),
        })),
    }
}
fn ids_ancestors(head: tinytree_model::CommitId) -> q::GraphExpr {
    q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Ancestors(Box::new(
            q::AncestorsExpr {
                heads: Some(Box::new(ids_commits(&[head]))),
                generations: None,
                parents: None,
            },
        ))),
    }
}
#[tokio::test]
async fn checked_ids_pages_replay_and_retain_private_scope_until_exact_eof() {
    use tinytree_model::{ChangeId, CommitId};
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::EvaluateRetry).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let mut parent = fixture.root;
    let mut drafts = Vec::new();
    let mut expected = vec![CommitId::ROOT, fixture.root];
    for index in 0..3 {
        let draft = private_draft(parent, ChangeId([index + 61; 16]), "paged private draft");
        parent = CommitId(draft.descriptor.key.id.0);
        expected.push(parent);
        drafts.push(checked_draft(&client, &draft).await);
    }
    let scope = client
        .open_query_scope_with_overlay(vec![fixture.root], drafts)
        .await
        .unwrap();
    let expression = ids_ancestors(parent);
    let mut query = client.begin_query(&scope, &expression, 2).await.unwrap();
    // Idle query state holds byte accounting, never the sole inflight permit.
    assert_eq!(
        client
            .query_facts(&scope, vec![QueryFact::HasId(parent)])
            .await
            .unwrap(),
        vec![FactAnswer::HasId(true)]
    );
    let first = client.evaluate_page(&mut query).await.unwrap();
    assert_eq!(first.sequence, 0);
    assert_eq!(
        running.counts.evaluations.load(Ordering::SeqCst),
        2,
        "lost ACK replays the same query/cursor identity"
    );
    let mut found: Vec<_> = first.rows.iter().map(|row| row.commit).collect();
    client.close_query_scope(&scope).await.unwrap();
    let mut sequence = 1;
    while !query.is_exhausted() {
        let page = client.evaluate_page(&mut query).await.unwrap();
        assert_eq!(page.sequence, sequence);
        sequence += 1;
        found.extend(page.rows.into_iter().map(|row| row.commit));
    }
    expected.reverse();
    assert_eq!(found, expected);
    let calls = running.counts.evaluations.load(Ordering::SeqCst);
    assert_eq!(
        client.evaluate_page(&mut query).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        running.counts.evaluations.load(Ordering::SeqCst),
        calls,
        "EOF must not restart the query"
    );
    running.stop().await;
}

#[tokio::test]
async fn ids_progress_pages_and_original_deadline_are_not_reset() {
    let mut config = ServiceConfig::default();
    config.query.max_eval_quantum_steps = 1;
    config.query.max_eval_step_work = 1;
    let fixture = setup_query_configured(config);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let expression = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Latest(Box::new(q::LatestExpr {
            candidates: Some(Box::new(ids_ancestors(fixture.root))),
            count: 1,
        }))),
    };
    let mut query = client.begin_query(&scope, &expression, 1).await.unwrap();
    let mut progress = false;
    let mut found = Vec::new();
    while !query.is_exhausted() {
        let page = client.evaluate_page(&mut query).await.unwrap();
        progress |= page.is_progress();
        found.extend(page.rows.into_iter().map(|row| row.commit));
    }
    assert!(
        progress,
        "one-step engine quanta must expose bounded progress instead of false completion"
    );
    assert_eq!(found, vec![fixture.root]);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
    let (_store, _index, service, root, _change) = setup_query();
    let running = QueryRunning::start(service, QueryMode::EvaluateDelay).await;
    let mut config = running.config.clone();
    config.rpc.operation_timeout_ms = 350;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let mut query = client
        .begin_query(&scope, &ids_ancestors(root), 1)
        .await
        .unwrap();
    assert!(!client.evaluate_page(&mut query).await.unwrap().exhausted);
    assert_eq!(
        client.evaluate_page(&mut query).await.unwrap_err().code(),
        Code::DeadlineExceeded,
        "second page uses the first operation's remaining allowance"
    );
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn ids_pages_reject_malformed_replies_and_duplicates_without_advancing() {
    for mode in [
        QueryMode::EvaluateMissingExhausted,
        QueryMode::EvaluateWrongDigest,
        QueryMode::EvaluateWrongEpoch,
        QueryMode::EvaluateWrongSequence,
        QueryMode::EvaluateCommitWidth,
        QueryMode::EvaluateChangeWidth,
        QueryMode::EvaluateEdges,
        QueryMode::EvaluateCanonical,
        QueryMode::EvaluateCursorMismatch,
        QueryMode::EvaluateDuplicate,
    ] {
        let (_store, _index, service, root, _change) = setup_query();
        let running = QueryRunning::start(service, mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![root]).await.unwrap();
        let mut query = client
            .begin_query(&scope, &ids_ancestors(root), 1)
            .await
            .unwrap();
        if mode == QueryMode::EvaluateDuplicate {
            assert!(!client.evaluate_page(&mut query).await.unwrap().exhausted);
        }
        let error = client.evaluate_page(&mut query).await.unwrap_err();
        assert!(
            matches!(error.code(), Code::DataLoss | Code::ResourceExhausted),
            "{mode:?}: {error}"
        );
        assert!(!query.is_exhausted());
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn cancelled_ids_page_releases_owned_state_and_aggregate_idle_queries_leave_page_headroom() {
    let (_store, _index, service, root, _change) = setup_query();
    let running = QueryRunning::start(service, QueryMode::EvaluatePause).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(dropped.clone())));
    let mut query = guarded
        .begin_query(&scope, &ids_ancestors(root), 1)
        .await
        .unwrap();
    let task = tokio::spawn(async move { guarded.evaluate_page(&mut query).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        running.counts.evaluate_entered.notified(),
    )
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    running.counts.evaluate_release.notify_one();
    assert_released(&dropped).await;
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
    let (_store, _index, service, root, _change) = setup_query();
    let running = QueryRunning::start(service, QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_graph_objects = 300_000;
    config.limits.max_graph_bytes = 32 * 1024 * 1024;
    // Valid ordinary RPC bounds leave 128 MiB page admission plus 48 MiB
    // persistent state: one ~38.4 MB query fits, while a second must wait.
    config.limits.max_buffered_bytes = 176 * 1024 * 1024;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let expression = ids_ancestors(root);
    let mut first = client.begin_query(&scope, &expression, 1).await.unwrap();
    let mut pending = Box::pin(client.begin_query(&scope, &expression, 1));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut pending)
            .await
            .is_err(),
        "second idle query should wait on the bounded state subpool"
    );
    assert!(
        !client
            .evaluate_page(&mut first)
            .await
            .unwrap()
            .rows
            .is_empty(),
        "pending state admission must leave page memory available"
    );
    drop(pending);
    drop(first);
    let _reused = client.begin_query(&scope, &expression, 1).await.unwrap();
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

struct IdsTiming {
    begin_ns: u128,
    first_window_ns: u128,
    after_window_ns: u128,
    total_ns: u128,
    first_window_rows: usize,
    ids: std::collections::BTreeSet<tinytree_model::CommitId>,
}
async fn measure_ids_window_and_completion(
    client: &NativeClient,
    scope: &tinytree_client::CheckedScopeHandle,
    head: tinytree_model::CommitId,
) -> IdsTiming {
    use tinytree_model::CommitId;
    let begin = std::time::Instant::now();
    let mut query = client
        .begin_query(scope, &ids_ancestors(head), 40)
        .await
        .unwrap();
    let begin_ns = begin.elapsed().as_nanos();
    let started = std::time::Instant::now();
    let mut ids = std::collections::BTreeSet::new();
    while ids.len() < 40 && !query.is_exhausted() {
        let page = client.evaluate_page(&mut query).await.unwrap();
        ids.extend(
            page.rows
                .into_iter()
                .map(|row| row.commit)
                .filter(|id| *id != CommitId::ROOT),
        );
    }
    let first_window_ns = started.elapsed().as_nanos();
    let first_window_rows = ids.len();
    let tail = std::time::Instant::now();
    while !query.is_exhausted() {
        let page = client.evaluate_page(&mut query).await.unwrap();
        ids.extend(
            page.rows
                .into_iter()
                .map(|row| row.commit)
                .filter(|id| *id != CommitId::ROOT),
        );
    }
    let after_window_ns = tail.elapsed().as_nanos();
    IdsTiming {
        begin_ns,
        first_window_ns,
        after_window_ns,
        total_ns: started.elapsed().as_nanos(),
        first_window_rows,
        ids,
    }
}
fn linear_ids_fixture(
    depth: usize,
) -> (
    QueryFixture,
    u128,
    u128,
    std::collections::BTreeSet<tinytree_model::CommitId>,
) {
    use tinytree_model::{ChangeId, CommitId};
    let (store_directory, store, service) = setup();
    let index_directory = private_tempdir();
    let started = std::time::Instant::now();
    let empty = tinytree_model::empty_tree();
    store
        .put("example", empty.descriptor, &empty.bytes, 0)
        .unwrap();
    let mut encoded = Vec::with_capacity(depth);
    let mut root = CommitId::ROOT;
    let mut change = ChangeId::ROOT;
    let mut ids = std::collections::BTreeSet::new();
    for position in 0..depth {
        let mut value = [0u8; 16];
        value[..8].copy_from_slice(&((position + 1) as u64).to_be_bytes());
        value[15] = 11;
        change = ChangeId(value);
        let object = private_draft(root, change, &format!("public indexed commit {position}"));
        root = CommitId(object.descriptor.key.id.0);
        ids.insert(root);
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
        encoded.push(object);
    }
    let source_ns = started.elapsed().as_nanos();
    let started = std::time::Instant::now();
    let index = tinytree_index::Index::open(index_directory.path(), &Default::default()).unwrap();
    for object in encoded {
        index
            .ingest("example", object.descriptor, &object.bytes)
            .unwrap();
    }
    let index_ns = started.elapsed().as_nanos();
    let service = service.with_query_index(index.clone()).unwrap();
    (
        QueryFixture {
            store_directory,
            index_directory,
            store,
            index,
            service,
            root,
            change,
        },
        source_ns,
        index_ns,
        ids,
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual IDS first-window/full enumeration benchmark; old public API materializes all pages"]
async fn checked_ids_first_window_and_completion_benchmark() {
    use tinytree_model::{ChangeId, CommitId};
    for (public_count, private_count, compare_old) in [
        (128usize, 0usize, true),
        (1024, 0, true),
        (4096, 0, false),
        (128, 10, false),
        (128, 100, false),
        (128, 1000, false),
    ] {
        let (fixture, source_ns, index_ns, mut expected) = linear_ids_fixture(public_count);
        let running =
            Running::start_with_socket_policy(fixture.service.clone(), Mode::Normal, true).await;
        let mut config = running.config();
        config.limits.max_batch_items = if private_count == 0 { 40 } else { 1024 };
        let configured_batch_items = config.limits.max_batch_items;
        let client = NativeClient::connect(config).await.unwrap();
        let mut head = fixture.root;
        let mut canonical_drafts = Vec::with_capacity(private_count);
        for position in 0..private_count {
            let mut value = [0u8; 16];
            value[..8].copy_from_slice(&((position + 1) as u64).to_be_bytes());
            value[15] = 12;
            let object = private_draft(
                head,
                ChangeId(value),
                &format!("private indexed draft {position}"),
            );
            head = CommitId(object.descriptor.key.id.0);
            expected.insert(head);
            canonical_drafts.push(object);
        }
        let verify_started = std::time::Instant::now();
        let mut drafts = Vec::with_capacity(private_count);
        for object in &canonical_drafts {
            drafts.push(checked_draft(&client, object).await);
        }
        let local_overlay_verify_ns = verify_started.elapsed().as_nanos();
        let open_started = std::time::Instant::now();
        let scope = client
            .open_query_scope_with_overlay(vec![fixture.root], drafts)
            .await
            .unwrap();
        let scope_open_ns = open_started.elapsed().as_nanos();
        let mut old_ns = Vec::new();
        let mut begin_ns = Vec::new();
        let mut first_ns = Vec::new();
        let mut after_ns = Vec::new();
        let mut total_ns = Vec::new();
        let mut first_rows = Vec::new();
        let old_graph_before = running.graph.load(Ordering::SeqCst);
        let old_bytes_before = running.canonical_bytes.load(Ordering::SeqCst);
        // Old checked API has no first-page return: its first usable 40 entries
        // arrive only after complete canonical materialization. Do not label this
        // a fixed-40 network comparison. Limit old whole-closure work to1024.
        if compare_old {
            for _ in 0..3 {
                let started = std::time::Instant::now();
                let graph = client
                    .canonical_commit_summaries(
                        vec![ObjectKey {
                            kind: Kind::Commit,
                            id: ObjectId(head.0),
                        }],
                        vec![],
                    )
                    .await
                    .unwrap();
                assert!(graph.entries.len() >= 40);
                let _first40 = &graph.entries[..40];
                old_ns.push(started.elapsed().as_nanos());
                let actual: std::collections::BTreeSet<_> = graph
                    .entries
                    .iter()
                    .map(|entry| {
                        CommitId(
                            key(entry.commit.as_ref().unwrap().key.as_ref().unwrap())
                                .id
                                .0,
                        )
                    })
                    .collect();
                assert_eq!(actual, expected);
            }
        }
        let old_graph_rpcs = running.graph.load(Ordering::SeqCst) - old_graph_before;
        let old_canonical_bytes = running.canonical_bytes.load(Ordering::SeqCst) - old_bytes_before;
        let ids_rpc_before = running.queries.evaluations.load(Ordering::SeqCst);
        for _ in 0..3 {
            let timing = measure_ids_window_and_completion(&client, &scope, head).await;
            assert_eq!(
                timing.ids, expected,
                "complete IDS result normalized by removing intrinsic root must match the identical canonical/private fixture"
            );
            begin_ns.push(timing.begin_ns);
            first_ns.push(timing.first_window_ns);
            after_ns.push(timing.after_window_ns);
            total_ns.push(timing.total_ns);
            first_rows.push(timing.first_window_rows);
        }
        let ids_rpcs = running.queries.evaluations.load(Ordering::SeqCst) - ids_rpc_before;
        assert_eq!(
            running.graph.load(Ordering::SeqCst) - old_graph_before,
            old_graph_rpcs
        );
        assert_eq!(
            running.canonical_bytes.load(Ordering::SeqCst) - old_bytes_before,
            old_canonical_bytes
        );
        assert_eq!(running.metadata.load(Ordering::SeqCst), 0);
        assert_eq!(running.files.load(Ordering::SeqCst), 0);
        for object in &canonical_drafts {
            assert!(
                fixture
                    .store
                    .descriptor("example", object.descriptor.key)
                    .unwrap()
                    .is_none()
            );
            assert!(
                fixture
                    .index
                    .node("example", CommitId(object.descriptor.key.id.0))
                    .unwrap()
                    .is_none()
            );
        }
        let close_started = std::time::Instant::now();
        client.close_query_scope(&scope).await.unwrap();
        let close_ns = close_started.elapsed().as_nanos();
        eprintln!(
            concat!(
                "{{\"schema\":1,\"benchmark\":\"checked_ids_first_window_and_completion\",",
                "\"public_history_commits\":{},\"private_commits\":{},\"client_max_batch_items\":{},\"page_size\":40,\"trials\":3,",
                "\"old_compared\":{},\"old_boundary\":\"public_api_materializes_entire_graph_before_first40\",",
                "\"intrinsic_root\":\"new_includes_old_excludes_removed_for_complete_ID_set_comparison\",",
                "\"public_source_setup_ns\":{},\"ready_index_ns\":{},\"index_ready_before_requests\":true,",
                "\"local_overlay_verification_ns\":{},\"scope_open_ns\":{},\"scope_close_ns\":{},",
                "\"old_full_materialization_to_first40_samples_ns\":{:?},",
                "\"new_local_query_preparation_samples_ns\":{:?},\"new_first_window_call_samples_ns\":{:?},",
                "\"new_first_window_fetched_nonroot_IDs\":{:?},\"new_completion_after_window_samples_ns\":{:?},\"new_complete_call_samples_ns\":{:?},",
                "\"call_timings_include_client_decode_and_verification\":true,\"decode_separately_instrumented\":false,",
                "\"old_graph_rpcs\":{},\"new_IDS_rpcs\":{},\"old_logical_canonical_envelope_bytes\":{},",
                "\"new_canonical_public_bytes_fetched\":0,\"metadata_rpcs\":0,\"file_rpcs\":0,",
                "\"byte_measure\":\"logical_envelopes_not_zstd_wire_bytes\",\"new_only_cases_have_no_old_speed_ratio\":true,",
                "\"authenticated\":true,\"zstd\":true,\"tcp_nodelay\":true}}"
            ),
            public_count,
            private_count,
            configured_batch_items,
            compare_old,
            source_ns,
            index_ns,
            local_overlay_verify_ns,
            scope_open_ns,
            close_ns,
            old_ns,
            begin_ns,
            first_ns,
            first_rows,
            after_ns,
            total_ns,
            old_graph_rpcs,
            ids_rpcs,
            old_canonical_bytes
        );
        running.stop().await;
    }
}

#[tokio::test]
async fn checked_graph_and_canonical_pages_replay_close_scope_and_keep_single_inflight_live() {
    use tinytree_client::GraphEdge;
    use tinytree_model::{ChangeId, CommitId, Object};
    for canonical in [false, true] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), QueryMode::EvaluateRetry).await;
        let mut config = running.config.clone();
        config.limits.max_inflight = 1;
        let client = NativeClient::connect(config).await.unwrap();
        let draft = private_draft(fixture.root, ChangeId([83; 16]), "selected private draft");
        let head = CommitId(draft.descriptor.key.id.0);
        let scope = client
            .open_query_scope_with_overlay(
                vec![fixture.root],
                vec![checked_draft(&client, &draft).await],
            )
            .await
            .unwrap();
        let mut query = client
            .begin_graph_query(&scope, &ids_ancestors(head), 1, canonical)
            .await
            .unwrap();
        let first = client.evaluate_graph_page(&mut query).await.unwrap();
        assert_eq!(first.sequence, 0);
        assert_eq!(first.rows().len(), 1);
        assert_eq!(first.rows()[0].commit, head);
        assert_eq!(
            first.rows()[0].edges(),
            vec![GraphEdge::Direct(fixture.root)]
        );
        assert_eq!(first.rows()[0].canonical.is_some(), canonical);
        assert_eq!(running.counts.evaluations.load(Ordering::SeqCst), 2);
        client.close_query_scope(&scope).await.unwrap();
        let mut ids = vec![head];
        // Keep the first page alive across the second call: retained page bytes
        // must not retain the client's only inflight permit.
        while !query.is_exhausted() {
            let page = client.evaluate_graph_page(&mut query).await.unwrap();
            for row in page.rows() {
                assert_eq!(
                    row.canonical.is_some(),
                    canonical && row.commit != CommitId::ROOT
                );
                if let Some(metadata) = &row.canonical {
                    assert_eq!(metadata.descriptor().key.id.0, row.commit.0);
                    let Object::Commit(commit) = metadata.object() else {
                        panic!("canonical row must be a commit")
                    };
                    assert_eq!(commit.change_id, row.change);
                }
                ids.push(row.commit);
            }
        }
        assert_eq!(ids, vec![head, fixture.root, CommitId::ROOT]);
        assert_eq!(first.rows()[0].commit, head);
        assert_eq!(
            client
                .evaluate_graph_page(&mut query)
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        // The endpoint in this fixture implements only RepositoryQuery: any
        // graph/metadata/FILE hydration would fail rather than hide extra I/O.
        drop(first);
        running.stop().await;
    }
}

#[tokio::test]
async fn graph_pages_reject_bad_edges_canonical_and_aggregate_collections_atomically() {
    struct Case {
        name: &'static str,
        mode: QueryMode,
        canonical: bool,
        code: Code,
    }
    for case in [
        Case {
            name: "missing canonical commit",
            mode: QueryMode::GraphMissingCanonical,
            canonical: true,
            code: Code::DataLoss,
        },
        Case {
            name: "canonical content hash",
            mode: QueryMode::GraphBadCanonical,
            canonical: true,
            code: Code::DataLoss,
        },
        Case {
            name: "canonical change identity",
            mode: QueryMode::GraphCanonicalChange,
            canonical: true,
            code: Code::DataLoss,
        },
        Case {
            name: "edge target width",
            mode: QueryMode::GraphEdgeWidth,
            canonical: false,
            code: Code::DataLoss,
        },
        Case {
            name: "direct edge not parent",
            mode: QueryMode::GraphUnknownDirect,
            canonical: true,
            code: Code::DataLoss,
        },
        Case {
            name: "missing edge oneof",
            mode: QueryMode::EvaluateEdges,
            canonical: false,
            code: Code::DataLoss,
        },
        Case {
            name: "graph forbids canonical",
            mode: QueryMode::EvaluateCanonical,
            canonical: false,
            code: Code::DataLoss,
        },
    ] {
        let (_store, _index, service, root, _change) = setup_query();
        let running = QueryRunning::start(service, case.mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![root]).await.unwrap();
        let mut query = client
            .begin_graph_query(&scope, &ids_ancestors(root), 1, case.canonical)
            .await
            .unwrap();
        let id = *query.query_id();
        let first = client.evaluate_graph_page(&mut query).await.unwrap_err();
        assert_eq!(first.code(), case.code, "{}: {first}", case.name);
        let retry = client.evaluate_graph_page(&mut query).await.unwrap_err();
        assert_eq!(retry.code(), case.code, "{}: {retry}", case.name);
        assert_eq!(*query.query_id(), id);
        assert!(!query.is_exhausted());
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
    let (_store, _index, service, root, _change) = setup_query();
    let running = QueryRunning::start(service, QueryMode::GraphDuplicateEdges).await;
    let mut config = running.config.clone();
    config.limits.max_query_edges = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let mut query = client
        .begin_graph_query(&scope, &ids_ancestors(root), 1, false)
        .await
        .unwrap();
    assert_eq!(
        client
            .evaluate_graph_page(&mut query)
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn graph_pages_preserve_duplicate_edge_order_and_cancel_owned_admission() {
    let (_store, _index, service, root, _change) = setup_query();
    let running = QueryRunning::start(service, QueryMode::GraphDuplicateEdges).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let mut query = client
        .begin_graph_query(&scope, &ids_ancestors(root), 1, true)
        .await
        .unwrap();
    let page = client.evaluate_graph_page(&mut query).await.unwrap();
    assert_eq!(page.rows()[0].edges().len(), 2);
    assert_eq!(page.rows()[0].edges()[0], page.rows()[0].edges()[1]);
    drop(page);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;

    let (_store, _index, service, root, _change) = setup_query();
    let running = QueryRunning::start(service, QueryMode::EvaluatePause).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(dropped.clone())));
    let mut query = guarded
        .begin_graph_query(&scope, &ids_ancestors(root), 1, true)
        .await
        .unwrap();
    let task = tokio::spawn(async move { guarded.evaluate_graph_page(&mut query).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        running.counts.evaluate_entered.notified(),
    )
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    running.counts.evaluate_release.notify_one();
    assert_released(&dropped).await;
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn retained_graph_pages_fail_headroom_without_waiting_or_advancing() {
    use tinytree_model::{ChangeId, CommitId};
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    config.limits.max_frame_bytes = 4096;
    config.limits.max_batch_bytes = 32768;
    config.limits.max_metadata_bytes = 16384;
    config.limits.max_request_bytes = 1024;
    config.limits.max_graph_bytes = 16384;
    config.limits.max_buffered_bytes = 262144;
    config.limits.max_batch_items = 16;
    config.limits.max_graph_objects = 800;
    let client = NativeClient::connect(config).await.unwrap();
    // Large valid canonical descriptions make two application-held pages exceed
    // persistent headroom while each page and wire request still fit admission.
    let description = "x".repeat(8192);
    let first = private_draft(fixture.root, ChangeId([87; 16]), &description);
    let first_id = CommitId(first.descriptor.key.id.0);
    let second = private_draft(first_id, ChangeId([88; 16]), &description);
    let second_id = CommitId(second.descriptor.key.id.0);
    let overlay = vec![
        checked_draft(&client, &first).await,
        checked_draft(&client, &second).await,
    ];
    let scope = client
        .open_query_scope_with_overlay(vec![fixture.root], overlay)
        .await
        .unwrap();
    // Keep this retained-page test on the general-expression state budget.
    // Exact literal queries intentionally reserve only their literal capacity.
    let expression = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Union(q::SetExpr {
            inputs: vec![ids_commits(&[first_id, second_id])],
        })),
    };
    let mut query = client
        .begin_graph_query(&scope, &expression, 1, true)
        .await
        .unwrap();
    let page = client.evaluate_graph_page(&mut query).await.unwrap();
    assert_eq!(page.rows()[0].commit, second_id);
    let failed = client.evaluate_graph_page(&mut query).await.unwrap_err();
    assert_eq!(failed.code(), Code::ResourceExhausted);
    assert!(!query.is_exhausted());
    // Retaining an extracted canonical object retains the page reservation too.
    let mut rows = page.into_rows();
    let mut row = rows.next().unwrap();
    let canonical = row.canonical.take().unwrap();
    drop(row);
    drop(rows);
    assert_eq!(
        client
            .evaluate_graph_page(&mut query)
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    drop(canonical);
    let retry = client.evaluate_graph_page(&mut query).await.unwrap();
    assert_eq!(retry.sequence, 1);
    assert_eq!(retry.rows()[0].commit, first_id);
    drop(retry);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn graph_progress_pages_and_original_deadline_are_not_renewed() {
    let mut config = ServiceConfig::default();
    config.query.max_eval_quantum_steps = 1;
    config.query.max_eval_step_work = 1;
    let fixture = setup_query_configured(config);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut query = client
        .begin_graph_query(&scope, &ids_ancestors(fixture.root), 1, true)
        .await
        .unwrap();
    let mut progress = false;
    let mut found = Vec::new();
    while !query.is_exhausted() {
        let page = client.evaluate_graph_page(&mut query).await.unwrap();
        progress |= page.is_progress();
        found.extend(page.rows().iter().map(|r| r.commit));
    }
    assert!(progress);
    assert_eq!(found, vec![fixture.root, tinytree_model::CommitId::ROOT]);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;

    let (_store, _index, service, root, _change) = setup_query();
    let running = QueryRunning::start(service, QueryMode::EvaluateDelay).await;
    let mut config = running.config.clone();
    config.rpc.operation_timeout_ms = 350;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let mut query = client
        .begin_graph_query(&scope, &ids_ancestors(root), 1, true)
        .await
        .unwrap();
    assert!(
        !client
            .evaluate_graph_page(&mut query)
            .await
            .unwrap()
            .exhausted
    );
    assert_eq!(
        client
            .evaluate_graph_page(&mut query)
            .await
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn graph_rows_and_extracted_canonical_keep_begin_client_guards_through_teardown() {
    for canonical in [false, true] {
        let (_store, _index, service, root, _change) = setup_query();
        let running = QueryRunning::start(service, QueryMode::Normal).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![root]).await.unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let guarded = client
            .clone()
            .with_lifetime_guard(Arc::new(LifetimeProbe(dropped.clone())));
        let mut query = guarded
            .begin_graph_query(&scope, &ids_ancestors(root), 1, canonical)
            .await
            .unwrap();
        drop(guarded);
        // Evaluation through a same-inner, unguarded clone must preserve owners
        // captured at begin_query, without retaining its state/inflight budget.
        let page = client.evaluate_graph_page(&mut query).await.unwrap();
        drop(query);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        let mut rows = page.into_rows();
        let mut row = rows.next().unwrap();
        assert!(rows.next().is_none());
        assert!(!row.edges().is_empty());
        let metadata = row.canonical.take();
        drop(row);
        // The exhausted iterator still owns the original SDK Vec allocation.
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(rows);
        if canonical {
            assert!(metadata.is_some());
            assert_eq!(dropped.load(Ordering::SeqCst), 0);
            drop(metadata);
        }
        assert_released(&dropped).await;
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn empty_graph_progress_pages_consume_retained_headroom() {
    let mut service_config = ServiceConfig::default();
    service_config.query.max_eval_quantum_steps = 1;
    service_config.query.max_eval_step_work = 1;
    let fixture = setup_query_configured(service_config);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_frame_bytes = 4096;
    config.limits.max_batch_bytes = 32768;
    config.limits.max_metadata_bytes = 1024;
    config.limits.max_request_bytes = 1024;
    config.limits.max_graph_bytes = 1024;
    config.limits.max_buffered_bytes = 262144;
    config.limits.max_batch_items = 16;
    config.limits.max_graph_objects = 950;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut query = client
        .begin_graph_query(&scope, &ids_ancestors(fixture.root), 1, false)
        .await
        .unwrap();
    let first = client.evaluate_graph_page(&mut query).await.unwrap();
    assert!(first.is_progress());
    assert_eq!(
        client
            .evaluate_graph_page(&mut query)
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert!(!query.is_exhausted());
    drop(first);
    let retry = client.evaluate_graph_page(&mut query).await.unwrap();
    assert_eq!(retry.sequence, 1);
    drop(retry);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

/// Component/payload comparison, not native jj startup or compressed wire size.
/// Every query is drained. Drop cleanup is bounded best effort, not an ACK;
/// a partial cursor can remain active until expiry if that cleanup cannot run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual completed default-log IDS/GRAPH/CANONICAL component benchmark"]
async fn checked_query_projection_payload_benchmark() {
    use tinytree_client::GraphEdge;
    use tinytree_model::{ChangeId, CommitId};
    for public_count in [128, 1024, 4096] {
        let (fixture, source_ns, index_ns, _) = linear_ids_fixture(public_count);
        let public_parent = fixture
            .index
            .node("example", fixture.root)
            .unwrap()
            .unwrap()
            .parents[0];
        let connected = std::time::Instant::now();
        let running =
            Running::start_with_socket_policy(fixture.service.clone(), Mode::Normal, true).await;
        let client = running.client().await;
        let connection_ns = connected.elapsed().as_nanos();
        let draft_started = std::time::Instant::now();
        let mut head = fixture.root;
        let mut draft_ids = Vec::with_capacity(10);
        let mut canonical_drafts = Vec::with_capacity(10);
        for position in 0..10 {
            let draft = private_draft(
                head,
                ChangeId([position + 101; 16]),
                "private default-log draft",
            );
            head = CommitId(draft.descriptor.key.id.0);
            draft_ids.push(head);
            canonical_drafts.push(draft);
        }
        let private_source_ns = draft_started.elapsed().as_nanos();
        let overlay_bytes: usize = canonical_drafts
            .iter()
            .map(|object| object.bytes.len())
            .sum();
        let started = std::time::Instant::now();
        let mut overlay = Vec::with_capacity(10);
        for object in &canonical_drafts {
            overlay.push(checked_draft(&client, object).await);
        }
        let verify_ns = started.elapsed().as_nanos();
        let started = std::time::Instant::now();
        let scope = client
            .open_query_scope_with_overlay(vec![fixture.root], overlay)
            .await
            .unwrap();
        let scope_open_ns = started.elapsed().as_nanos();
        let range = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Range(Box::new(q::RangeExpr {
                roots: Some(Box::new(ids_commits(&[fixture.root]))),
                heads: Some(Box::new(ids_commits(&[head, fixture.root]))),
                generations: None,
                parents: None,
            }))),
        };
        let expression = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Union(q::SetExpr {
                inputs: vec![
                    ids_commits(&[head]),
                    q::GraphExpr {
                        version: 1,
                        expression: Some(q::graph_expr::Expression::Ancestors(Box::new(
                            q::AncestorsExpr {
                                heads: Some(Box::new(range)),
                                generations: Some(q::GenerationRange {
                                    start: 0,
                                    end_exclusive: Some(2),
                                }),
                                parents: None,
                            },
                        ))),
                    },
                    ids_commits(&[fixture.root]),
                ],
            })),
        };
        let mut expected: Vec<_> = draft_ids.iter().rev().copied().collect();
        expected.push(fixture.root);
        assert_eq!(expected.len(), 11);
        eprintln!(
            concat!(
                "{{\"schema\":1,\"benchmark\":\"checked_query_projection_payload\",\"record\":\"setup\",",
                "\"public_history_commits\":{},\"private_commits\":10,\"selected_rows\":11,\"page_size\":40,\"trials_per_projection\":3,",
                "\"public_source_setup_ns\":{},\"ready_index_ns\":{},\"connection_setup_ns\":{},",
                "\"private_source_setup_ns\":{},\"overlay_verify_ns\":{},\"scope_open_ns\":{},\"overlay_logical_envelope_bytes\":{},",
                "\"index_ready_before_requests\":true,\"trial_order\":\"IDS_then_GRAPH_then_CANONICAL\",",
                "\"transport\":\"authenticated_zstd_loopback_tcp_nodelay\",\"native_jj_startup_measured\":false}}"
            ),
            public_count,
            source_ns,
            index_ns,
            connection_ns,
            private_source_ns,
            verify_ns,
            scope_open_ns,
            overlay_bytes
        );
        for projection in [
            q::Projection::Ids,
            q::Projection::Graph,
            q::Projection::Canonical,
        ] {
            for trial in 0..3 {
                let rpc_before = running.queries.evaluations.load(Ordering::SeqCst);
                let prepared = std::time::Instant::now();
                let mut ids = Vec::with_capacity(11);
                let mut page_count = 0usize;
                let mut progress_pages = 0usize;
                let mut edge_count = 0usize;
                let mut canonical_bytes = 0usize;
                let mut first_page_ns = None;
                let mut first_data_ns = None;
                let mut first_page_rows = 0usize;
                let begin_ns;
                let completed_ns;
                if projection == q::Projection::Ids {
                    let mut query = client.begin_query(&scope, &expression, 40).await.unwrap();
                    begin_ns = prepared.elapsed().as_nanos();
                    let started = std::time::Instant::now();
                    while !query.is_exhausted() {
                        let page = client.evaluate_page(&mut query).await.unwrap();
                        page_count += 1;
                        if first_page_ns.is_none() {
                            first_page_ns = Some(started.elapsed().as_nanos());
                            first_page_rows = page.rows.len();
                        }
                        if page.is_progress() {
                            progress_pages += 1;
                        }
                        if !page.rows.is_empty() && first_data_ns.is_none() {
                            first_data_ns = Some(started.elapsed().as_nanos());
                        }
                        ids.extend(page.rows.into_iter().map(|row| row.commit));
                    }
                    assert!(query.is_exhausted());
                    completed_ns = started.elapsed().as_nanos();
                } else {
                    let canonical = projection == q::Projection::Canonical;
                    let mut query = client
                        .begin_graph_query(&scope, &expression, 40, canonical)
                        .await
                        .unwrap();
                    begin_ns = prepared.elapsed().as_nanos();
                    let started = std::time::Instant::now();
                    while !query.is_exhausted() {
                        let page = client.evaluate_graph_page(&mut query).await.unwrap();
                        page_count += 1;
                        if first_page_ns.is_none() {
                            first_page_ns = Some(started.elapsed().as_nanos());
                            first_page_rows = page.rows().len();
                        }
                        if page.is_progress() {
                            progress_pages += 1;
                        }
                        if !page.rows().is_empty() && first_data_ns.is_none() {
                            first_data_ns = Some(started.elapsed().as_nanos());
                        }
                        for row in page.rows() {
                            assert_eq!(
                                row.canonical.is_some(),
                                canonical && row.commit != CommitId::ROOT
                            );
                            edge_count += row.edges().len();
                            canonical_bytes += row
                                .canonical
                                .as_ref()
                                .map_or(0, |metadata| metadata.canonical_bytes().len());
                            if row.commit == fixture.root {
                                assert_eq!(
                                    row.edges(),
                                    &[GraphEdge::Missing(public_parent)],
                                    "public boundary must not hydrate the omitted public tail"
                                );
                            }
                            ids.push(row.commit);
                        }
                        // Release each response before asking for the next quantum.
                        drop(page);
                    }
                    assert!(query.is_exhausted());
                    completed_ns = started.elapsed().as_nanos();
                }
                assert_eq!(
                    ids, expected,
                    "identical ordered result and proven EOF for every projection"
                );
                assert_eq!(ids.len(), 11);
                if projection == q::Projection::Ids {
                    assert_eq!(edge_count, 0);
                }
                if projection != q::Projection::Canonical {
                    assert_eq!(canonical_bytes, 0);
                }
                let rpcs = running.queries.evaluations.load(Ordering::SeqCst) - rpc_before;
                assert_eq!(rpcs, page_count);
                assert_eq!(running.graph.load(Ordering::SeqCst), 0);
                assert_eq!(running.metadata.load(Ordering::SeqCst), 0);
                assert_eq!(running.files.load(Ordering::SeqCst), 0);
                assert_eq!(running.canonical_bytes.load(Ordering::SeqCst), 0);
                eprintln!(
                    concat!(
                        "{{\"schema\":1,\"benchmark\":\"checked_query_projection_payload\",\"record\":\"query\",",
                        "\"public_history_commits\":{},\"private_commits\":10,\"projection\":\"{}\",\"trial\":{},\"page_size\":40,",
                        "\"begin_ns\":{},\"first_page_ns\":{},\"first_data_ns\":{},\"done_ns\":{},\"begin_to_done_ns\":{},",
                        "\"evaluate_rpcs\":{},\"pages\":{},\"progress_pages\":{},\"first_page_rows\":{},\"rows\":{},",
                        "\"ordered_edges\":{},\"logical_canonical_envelope_bytes\":{},\"repo_graph_rpcs\":0,\"repo_metadata_rpcs\":0,\"file_rpcs\":0,",
                        "\"proven_eof\":true,\"call_times_include_decode_and_verification\":true,",
                        "\"byte_measure\":\"logical_canonical_envelopes_not_zstd_wire_bytes\",",
                        "\"lifecycle\":\"drained_to_EOF_Drop_cleanup_best_effort_expiry_fallback\",\"native_jj_startup_measured\":false}}"
                    ),
                    public_count,
                    projection.as_str_name(),
                    trial,
                    begin_ns,
                    first_page_ns.unwrap(),
                    first_data_ns.unwrap(),
                    completed_ns,
                    begin_ns + completed_ns,
                    rpcs,
                    page_count,
                    progress_pages,
                    first_page_rows,
                    ids.len(),
                    edge_count,
                    canonical_bytes
                );
            }
        }
        for draft in &canonical_drafts {
            assert!(
                fixture
                    .store
                    .descriptor("example", draft.descriptor.key)
                    .unwrap()
                    .is_none()
            );
            assert!(
                fixture
                    .index
                    .node("example", CommitId(draft.descriptor.key.id.0))
                    .unwrap()
                    .is_none()
            );
        }
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

async fn wait_query_close_count(counts: &QueryCounts, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while counts.query_closes.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn explicit_query_close_is_idempotent_not_eof_and_releases_partial_query_admission() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::CloseQueryRetry).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let expression = ids_ancestors(fixture.root);
    let mut ids = client.begin_query(&scope, &expression, 1).await.unwrap();
    let first = client.evaluate_page(&mut ids).await.unwrap();
    assert!(!first.exhausted);
    client.close_ids_query(&mut ids).await.unwrap();
    assert!(ids.is_closed());
    assert!(!ids.is_exhausted());
    assert_eq!(
        running.counts.query_closes.load(Ordering::SeqCst),
        2,
        "lost ACK retries same close identity"
    );
    client.close_ids_query(&mut ids).await.unwrap();
    assert_eq!(running.counts.query_closes.load(Ordering::SeqCst), 2);
    assert_eq!(
        client.evaluate_page(&mut ids).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    for canonical in [false, true] {
        for _ in 0..12 {
            let mut query = client
                .begin_graph_query(&scope, &expression, 1, canonical)
                .await
                .unwrap();
            let page = client.evaluate_graph_page(&mut query).await.unwrap();
            assert!(!page.exhausted);
            client.close_graph_query(&mut query).await.unwrap();
            assert!(query.is_closed());
            assert!(!query.is_exhausted());
            assert_eq!(
                client
                    .evaluate_graph_page(&mut query)
                    .await
                    .unwrap_err()
                    .code(),
                Code::FailedPrecondition
            );
            // Held response quota must not retain the sole inflight permit.
            assert_eq!(page.rows()[0].commit, fixture.root);
            drop(page);
        }
    }
    let before = running.counts.query_closes.load(Ordering::SeqCst);
    let mut never = client.begin_query(&scope, &expression, 1).await.unwrap();
    client.close_ids_query(&mut never).await.unwrap();
    assert!(never.is_closed());
    assert!(!never.is_exhausted());
    drop(never);
    let idle = client
        .begin_graph_query(&scope, &expression, 1, false)
        .await
        .unwrap();
    drop(idle);
    assert_eq!(
        running.counts.query_closes.load(Ordering::SeqCst),
        before,
        "unattempted queries need no cleanup RPC"
    );
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn drop_query_cleanup_is_bounded_and_drains_after_last_client_drop() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::CloseQueryPause).await;
    let mut config = running.config.clone();
    config.limits.query_cleanup_capacity = 1;
    config.query_cleanup_timeout_ms = 2000;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let first_dropped = Arc::new(AtomicUsize::new(0));
    let queued_dropped = Arc::new(AtomicUsize::new(0));
    let overflow_dropped = Arc::new(AtomicUsize::new(0));
    for (probe, wait_for_active) in [
        (&first_dropped, true),
        (&queued_dropped, false),
        (&overflow_dropped, false),
    ] {
        let guarded = client
            .clone()
            .with_lifetime_guard(Arc::new(LifetimeProbe(probe.clone())));
        let mut query = guarded
            .begin_query(&scope, &ids_ancestors(fixture.root), 1)
            .await
            .unwrap();
        let page = guarded.evaluate_page(&mut query).await.unwrap();
        drop(page);
        drop(query);
        drop(guarded);
        if wait_for_active {
            tokio::time::timeout(
                Duration::from_secs(2),
                running.counts.close_query_entered.notified(),
            )
            .await
            .unwrap();
        }
    }
    assert_eq!(first_dropped.load(Ordering::SeqCst), 0);
    assert_eq!(queued_dropped.load(Ordering::SeqCst), 0);
    assert_released(&overflow_dropped).await;
    // The worker owns transport/semaphores, not Inner, and drains queued work
    // after the final connection handle disappears.
    drop(scope);
    drop(client);
    running.counts.close_query_release.notify_one();
    tokio::time::timeout(
        Duration::from_secs(2),
        running.counts.close_query_entered.notified(),
    )
    .await
    .unwrap();
    running.counts.close_query_release.notify_one();
    assert_released(&first_dropped).await;
    assert_released(&queued_dropped).await;
    running.stop().await;
}

#[tokio::test]
async fn cancelled_evaluate_and_failed_explicit_close_keep_bounded_cleanup_owners() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::EvaluatePause).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(dropped.clone())));
    let mut query = guarded
        .begin_graph_query(&scope, &ids_ancestors(fixture.root), 1, true)
        .await
        .unwrap();
    let task = tokio::spawn(async move { guarded.evaluate_graph_page(&mut query).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        running.counts.evaluate_entered.notified(),
    )
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    wait_query_close_count(&running.counts, 1).await;
    running.counts.evaluate_release.notify_one();
    assert_released(&dropped).await;
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;

    let fixture = setup_query_retained();
    let running =
        QueryRunning::start(fixture.service.clone(), QueryMode::CloseQueryUnavailable).await;
    let mut config = running.config.clone();
    config.query_cleanup_timeout_ms = 40;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(dropped.clone())));
    let mut query = guarded
        .begin_query(&scope, &ids_ancestors(fixture.root), 1)
        .await
        .unwrap();
    guarded.evaluate_page(&mut query).await.unwrap();
    let error = client.close_ids_query(&mut query).await.unwrap_err();
    assert!(matches!(
        error.code(),
        Code::DeadlineExceeded | Code::Unavailable
    ));
    assert!(query.is_closed());
    assert!(!query.is_exhausted());
    let before = running.counts.query_closes.load(Ordering::SeqCst);
    drop(query);
    drop(guarded);
    wait_query_close_count(&running.counts, before + 1).await;
    assert_released(&dropped).await;
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn query_cleanup_runtime_exit_releases_owners_without_unbounded_retry_tasks() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::CloseQueryPause).await;
    let config = running.config.clone();
    let root = fixture.root;
    let counts = running.counts.clone();
    let dropped = Arc::new(AtomicUsize::new(0));
    let probe = dropped.clone();
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let client = NativeClient::connect(config)
                .await
                .unwrap()
                .with_lifetime_guard(Arc::new(LifetimeProbe(probe)));
            let scope = client.open_query_scope(vec![root]).await.unwrap();
            let mut query = client
                .begin_query(&scope, &ids_ancestors(root), 1)
                .await
                .unwrap();
            client.evaluate_page(&mut query).await.unwrap();
            drop(query);
            drop(client);
            wait_query_close_count(&counts, 1).await;
        });
        // Aborting this runtime drops the one worker and its queued/active
        // owners; no background thread or per-Drop runtime is created.
        drop(runtime);
    })
    .await
    .unwrap();
    assert_released(&dropped).await;
    running.counts.close_query_release.notify_one();
    running.stop().await;
}

#[tokio::test]
async fn drop_cleanup_cannot_steal_reserved_page_headroom_and_deadlock_sole_inflight_slot() {
    let fixture = setup_query_retained();
    let running =
        QueryRunning::start(fixture.service.clone(), QueryMode::CloseQueryUnavailable).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    config.limits.max_frame_bytes = 4096;
    config.limits.max_batch_bytes = 32768;
    config.limits.max_metadata_bytes = 1024;
    config.limits.max_request_bytes = 1024;
    config.limits.max_graph_bytes = 1024;
    config.limits.max_buffered_bytes = 262144;
    config.limits.max_batch_items = 16;
    config.limits.max_graph_objects = 950;
    config.query_cleanup_timeout_ms = 40;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let expression = ids_ancestors(fixture.root);
    let mut uncertain = client.begin_query(&scope, &expression, 1).await.unwrap();
    client.evaluate_page(&mut uncertain).await.unwrap();
    assert!(client.close_ids_query(&mut uncertain).await.is_err());
    assert!(uncertain.is_closed());
    // The closed handle has released its local query reservation but still owes
    // uncertain cleanup. A new query leaves less than a queued item's 1024B in
    // persistent headroom; Drop must use expiry, not steal the unary allowance.
    let mut fresh = client.begin_query(&scope, &expression, 1).await.unwrap();
    let close_calls = running.counts.query_closes.load(Ordering::SeqCst);
    drop(uncertain);
    let page = client.evaluate_page(&mut fresh).await.unwrap();
    assert_eq!(page.rows[0].commit, fixture.root);
    assert_eq!(
        running.counts.query_closes.load(Ordering::SeqCst),
        close_calls
    );
    let _ = client.close_ids_query(&mut fresh).await;
    drop(fresh);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn acknowledged_query_close_releases_begin_guards_even_with_closed_handle_retained() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    for attempted in [false, true] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let guarded = client
            .clone()
            .with_lifetime_guard(Arc::new(LifetimeProbe(dropped.clone())));
        let mut query = guarded
            .begin_query(&scope, &ids_ancestors(fixture.root), 1)
            .await
            .unwrap();
        if attempted {
            guarded.evaluate_page(&mut query).await.unwrap();
        }
        drop(guarded);
        client.close_ids_query(&mut query).await.unwrap();
        assert!(query.is_closed());
        assert!(!query.is_exhausted());
        assert_released(&dropped).await;
        // Keeping this closed handle must retain neither owners nor state quota.
        assert!(query.is_closed());
    }
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn smallest_page_config_reserves_larger_cleanup_headroom_without_inflight_stall() {
    let fixture = setup_query_retained();
    let running =
        QueryRunning::start(fixture.service.clone(), QueryMode::CloseQueryInitialReject).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    config.limits.max_frame_bytes = 1024;
    config.limits.max_batch_bytes = 1024;
    config.limits.max_metadata_bytes = 1024;
    config.limits.max_request_bytes = 1024;
    config.limits.max_graph_bytes = 1024;
    config.limits.max_buffered_bytes = 16384;
    config.limits.max_batch_items = 4;
    config.limits.max_graph_objects = 1;
    config.query_cleanup_timeout_ms = 2000;
    config.rpc.operation_timeout_ms = 350;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let expression = ids_ancestors(fixture.root);
    let mut uncertain = Vec::new();
    for _ in 0..3 {
        let mut query = client.begin_query(&scope, &expression, 1).await.unwrap();
        client.evaluate_page(&mut query).await.unwrap();
        assert_eq!(
            client.close_ids_query(&mut query).await.unwrap_err().code(),
            Code::PermissionDenied
        );
        uncertain.push(query);
    }
    let mut active = client.begin_query(&scope, &expression, 1).await.unwrap();
    // The old unary-only 4KiB reserve admits three queued items, then its
    // cleanup worker holds the sole inflight slot waiting for 6KiB. Reserving
    // max(page,cleanup) admits only one item and leaves its entire allowance.
    drop(uncertain);
    tokio::task::yield_now().await;
    let page = client.evaluate_page(&mut active).await.unwrap();
    assert_eq!(page.rows[0].commit, fixture.root);
    assert_eq!(running.counts.query_closes.load(Ordering::SeqCst), 4);
    client.close_ids_query(&mut active).await.unwrap();
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

// BEGIN TINYTREE_PARTIAL_QUERY_DROP_BENCH_V1
// Matched before/after workload. This measures checked SDK partial-query
// lifecycle behavior, not native CLI startup. Keep this block compatible with
// the pre-cleanup API: no explicit query-close RPCs or new lifecycle fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual matched partial-query Drop cleanup and bounded GC convergence benchmark"]
async fn partial_graph_query_drop_cleanup_benchmark() {
    const HISTORY: usize = 128;
    const REQUESTED: usize = 24;
    const GRACE_MS: u64 = 1000;
    const POLL_MS: u64 = 10;
    for canonical in [false, true] {
        let (fixture, source_ns, index_ns, _) = linear_ids_fixture(HISTORY);
        let connect_started = std::time::Instant::now();
        let running =
            Running::start_with_socket_policy(fixture.service.clone(), Mode::Normal, true).await;
        let client = running.client().await;
        let connect_ns = connect_started.elapsed().as_nanos();
        let scope_started = std::time::Instant::now();
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let scope_open_ns = scope_started.elapsed().as_nanos();
        let expression = ids_ancestors(fixture.root);
        let rpc_before = running.queries.evaluations.load(Ordering::SeqCst);
        let workload_started = std::time::Instant::now();
        let mut attempted = 0usize;
        let mut completed = 0usize;
        let mut retries = 0usize;
        let mut samples_ns = Vec::new();
        let mut first_page_rows = Vec::new();
        let mut outcome = "completed";
        let mut error_code = "none";
        for _ in 0..REQUESTED {
            attempted += 1;
            let started = std::time::Instant::now();
            let deadline = tokio::time::Instant::now() + Duration::from_millis(GRACE_MS);
            let mut query = match tokio::time::timeout_at(
                deadline,
                client.begin_graph_query(&scope, &expression, 1, canonical),
            )
            .await
            {
                Ok(Ok(query)) => query,
                Ok(Err(error)) => {
                    outcome = "begin_error";
                    error_code = match error.code() {
                        Code::ResourceExhausted => "ResourceExhausted",
                        Code::DeadlineExceeded => "DeadlineExceeded",
                        _ => "OtherStatus",
                    };
                    break;
                }
                Err(_) => {
                    outcome = "begin_grace_expired";
                    error_code = "AppGraceExpired";
                    break;
                }
            };
            let mut iteration_completed = false;
            loop {
                match tokio::time::timeout_at(deadline, client.evaluate_graph_page(&mut query))
                    .await
                {
                    Ok(Ok(page)) => {
                        // Exactly one page, including a legitimate progress page;
                        // never drain the history to manufacture cleanup success.
                        assert!(!page.exhausted, "history128/page1 must remain partial");
                        first_page_rows.push(page.rows().len());
                        drop(page);
                        iteration_completed = true;
                        break;
                    }
                    Ok(Err(error)) if error.code() == Code::ResourceExhausted => {
                        error_code = "ResourceExhausted";
                        // Retry the SAME checked query identity/cursor. A fresh
                        // query ID on each retry would distort the quota test.
                        if tokio::time::Instant::now() + Duration::from_millis(POLL_MS) >= deadline
                        {
                            outcome = "resource_exhausted_after_grace";
                            break;
                        }
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(POLL_MS)).await;
                    }
                    Ok(Err(error)) => {
                        outcome = "evaluate_error";
                        error_code = match error.code() {
                            Code::DeadlineExceeded => "DeadlineExceeded",
                            Code::FailedPrecondition => "FailedPrecondition",
                            Code::DataLoss => "DataLoss",
                            _ => "OtherStatus",
                        };
                        break;
                    }
                    Err(_) => {
                        outcome = "evaluate_grace_expired";
                        if error_code != "ResourceExhausted" {
                            error_code = "AppGraceExpired";
                        }
                        break;
                    }
                }
            }
            drop(query);
            if !iteration_completed {
                break;
            }
            completed += 1;
            samples_ns.push(started.elapsed().as_nanos());
            error_code = "none";
        }
        let workload_ns = workload_started.elapsed().as_nanos();
        let close_started = std::time::Instant::now();
        let scope_closed = matches!(
            tokio::time::timeout(
                Duration::from_millis(GRACE_MS),
                client.close_query_scope(&scope),
            )
            .await,
            Ok(Ok(()))
        );
        let scope_close_ns = close_started.elapsed().as_nanos();
        drop(scope);
        let root = tinytree_model::ObjectKey {
            kind: tinytree_model::Kind::Commit,
            id: tinytree_model::ObjectId(fixture.root.0),
        };
        let gc_started = std::time::Instant::now();
        let gc_deadline = tokio::time::Instant::now() + Duration::from_millis(GRACE_MS);
        let mut gc_passes = 0usize;
        let mut gc_failed = false;
        let mut root_live = true;
        let mut deleted_objects = 0u64;
        loop {
            let store = fixture.store.clone();
            // GC is synchronous physical work: retain/await its owned worker,
            // even if one pass crosses the bounded polling-grace deadline.
            let result = tokio::task::spawn_blocking(move || {
                let report = store.gc("example", i64::MAX - 1)?;
                let live = store.descriptor("example", root)?.is_some();
                Ok::<_, tinytree_store::Error>((report, live))
            })
            .await
            .unwrap();
            gc_passes += 1;
            match result {
                Ok((report, live)) => {
                    deleted_objects += report.deleted_objects;
                    root_live = live;
                }
                Err(_) => {
                    gc_failed = true;
                    break;
                }
            }
            if !root_live || tokio::time::Instant::now() >= gc_deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(POLL_MS)).await;
        }
        let gc_ns = gc_started.elapsed().as_nanos();
        let projection = if canonical { "CANONICAL" } else { "GRAPH" };
        assert_eq!(running.files.load(Ordering::SeqCst), 0);
        eprintln!(
            concat!(
                "{{\"schema\":1,\"benchmark\":\"partial_graph_query_drop_cleanup\",",
                "\"projection\":\"{}\",\"history_commits\":{},\"page_size\":1,\"query_ttl_seconds_unchanged\":120,",
                "\"iterations_requested\":{},\"iterations_attempted\":{},\"iterations_completed\":{},",
                "\"outcome\":\"{}\",\"last_error_code\":\"{}\",\"resource_retries_same_query\":{},",
                "\"iteration_grace_ms\":{},\"retry_poll_ms\":{},\"evaluation_rpcs\":{},",
                "\"source_setup_ns\":{},\"ready_index_ns\":{},\"connect_ns\":{},\"scope_open_ns\":{},",
                "\"completed_iteration_samples_ns\":{:?},\"first_page_rows\":{:?},\"workload_ns\":{},",
                "\"scope_closed\":{},\"scope_close_ns\":{},\"gc_poll_grace_ms\":{},\"gc_ns\":{},",
                "\"gc_passes\":{},\"gc_failed\":{},\"gc_deleted_objects\":{},\"gc_root_live_after_grace\":{},",
                "\"orphan_root_inference_valid\":{},\"gc_grace_is_poll_budget_not_physical_job_cancellation\":true,",
                "\"file_rpcs\":0,\"authenticated\":true,\"zstd\":true,\"tcp_nodelay\":true,",
                "\"claim\":\"partial_checked_query_lifecycle_not_native_cli_startup\",\"failed_vs_success_timing_ratio_valid\":false}}"
            ),
            projection,
            HISTORY,
            REQUESTED,
            attempted,
            completed,
            outcome,
            error_code,
            retries,
            GRACE_MS,
            POLL_MS,
            running.queries.evaluations.load(Ordering::SeqCst) - rpc_before,
            source_ns,
            index_ns,
            connect_ns,
            scope_open_ns,
            samples_ns,
            first_page_rows,
            workload_ns,
            scope_closed,
            scope_close_ns,
            GRACE_MS,
            gc_ns,
            gc_passes,
            gc_failed,
            deleted_objects,
            root_live,
            scope_closed && !gc_failed
        );
        running.stop().await;
    }
}
// END TINYTREE_PARTIAL_QUERY_DROP_BENCH_V1

#[tokio::test]
async fn rpc_only_guard_follows_cancelled_verification_worker_not_verified_metadata() {
    let (_dir, _store, service) = setup();
    let running = Running::start(service, Mode::Normal).await;
    let mut config = running.config();
    config.limits.max_inflight = 1;
    config.rpc.operation_timeout_ms = 1000;
    let (done, finished) = tokio::sync::oneshot::channel();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async move {
            let client = NativeClient::connect(config).await.unwrap();
            let drops = Arc::new(AtomicUsize::new(0));
            let guarded = client
                .clone()
                .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
            let object = tinytree_model::encode(
                &tinytree_model::Object::Tree(tinytree_model::Tree { entries: vec![] }),
                Default::default(),
            )
            .unwrap();
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (unblock, blocked) = std::sync::mpsc::channel();
            let occupied = tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                let _ = blocked.recv();
            });
            ready.await.unwrap();
            {
                let verify = guarded.verify_metadata(object.descriptor, object.bytes.clone());
                tokio::pin!(verify);
                assert!(futures_util::poll!(&mut verify).is_pending());
            }
            drop(guarded);
            // The actual SDK verifier is queued behind the occupied worker.
            // Cancelling its waiter cannot release its external owner or slot.
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            unblock.send(()).unwrap();
            occupied.await.unwrap();
            assert_released(&drops).await;
            let drops = Arc::new(AtomicUsize::new(0));
            let guarded = client
                .clone()
                .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
            let metadata = guarded
                .verify_metadata(object.descriptor, object.bytes)
                .await
                .unwrap();
            drop(guarded);
            assert_released(&drops).await;
            assert_eq!(
                metadata.descriptor(),
                object.descriptor,
                "completed metadata does not retain a physical request guard"
            );
            drop(metadata);
        });
        done.send(()).unwrap();
    });
    finished.await.unwrap();
    worker.join().unwrap();
    running.stop().await;
}

#[tokio::test]
async fn rpc_only_guards_do_not_enter_query_state_pages_or_uncertain_drop_cleanup() {
    for canonical in [false, true] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
        let mut config = running.config.clone();
        config.limits.max_inflight = 1;
        let client = NativeClient::connect(config).await.unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let guarded = client
            .clone()
            .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
        let scope = guarded.open_query_scope(vec![fixture.root]).await.unwrap();
        drop(guarded);
        assert_released(&drops).await;
        let drops = Arc::new(AtomicUsize::new(0));
        let guarded = client
            .clone()
            .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
        let mut query = guarded
            .begin_graph_query(&scope, &ids_ancestors(fixture.root), 1, canonical)
            .await
            .unwrap();
        drop(guarded);
        assert_released(&drops).await;
        let drops = Arc::new(AtomicUsize::new(0));
        let guarded = client
            .clone()
            .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
        let page = guarded.evaluate_graph_page(&mut query).await.unwrap();
        drop(guarded);
        assert_released(&drops).await;
        assert!(!page.exhausted);
        let drops = Arc::new(AtomicUsize::new(0));
        let guarded = client
            .clone()
            .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
        let facts = guarded
            .query_facts(
                &scope,
                vec![tinytree_client::QueryFact::HasId(fixture.root)],
            )
            .await
            .unwrap();
        drop(guarded);
        assert_released(&drops).await;
        assert_eq!(facts, vec![tinytree_client::FactAnswer::HasId(true)]);
        let drops = Arc::new(AtomicUsize::new(0));
        let guarded = client
            .clone()
            .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
        guarded.close_graph_query(&mut query).await.unwrap();
        drop(guarded);
        assert_released(&drops).await;
        assert!(query.is_closed());
        assert!(!query.is_exhausted());
        assert_eq!(page.rows()[0].commit, fixture.root);
        drop((page, query));
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
    let fixture = setup_query_retained();
    let running =
        QueryRunning::start(fixture.service.clone(), QueryMode::CloseQueryInitialReject).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut query = client
        .begin_query(&scope, &ids_ancestors(fixture.root), 1)
        .await
        .unwrap();
    client.evaluate_page(&mut query).await.unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
    assert_eq!(
        guarded
            .close_ids_query(&mut query)
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    drop(guarded);
    assert_released(&drops).await;
    let before = running.counts.query_closes.load(Ordering::SeqCst);
    drop(query);
    wait_query_close_count(&running.counts, before + 1).await;
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

// Opaque raw transport fixture, intentionally not a native manifest/body claim.
fn capture_transport_fixture(mode: usize) -> (Vec<u8>, Vec<u8>) {
    let data = b"portable capture transport DATA".to_vec();
    let mut manifest = b"TTCLOUD\0".to_vec();
    manifest.extend_from_slice(&if mode == 53 { 2u16 } else { 3u16 }.to_le_bytes());
    if mode == 54 {
        return (data, manifest);
    }
    manifest.extend_from_slice(b"opaque native body is verified by native callers");
    let mut id = tinytree_model::cloud_capture_data_id(&data);
    if mode == 58 {
        id[0] ^= 1;
    }
    manifest.extend_from_slice(&id);
    manifest.extend_from_slice(blake3::hash(&data).as_bytes());
    manifest.extend_from_slice(&(data.len() as u64).to_le_bytes());
    (data, manifest)
}
#[derive(Clone, Default)]
struct CloudFaults {
    mode: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    publication: Arc<std::sync::Mutex<Option<tinytree_client::cloud_protocol::Publication>>>,
    index_entered: Arc<tokio::sync::Notify>,
    index_release: Arc<tokio::sync::Notify>,
    native_snapshot_calls: Arc<AtomicUsize>,
}
#[tonic::async_trait]
impl tinytree_client::cloud_protocol::workspace_cloud_server::WorkspaceCloud for CloudFaults {
    async fn put_object(
        &self,
        request: Request<tinytree_client::cloud_protocol::PutObjectRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::WriteReceipt>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        let mut descriptor = request.descriptor.unwrap();
        let proposed = tinytree_client::CloudRawDigest::of(&request.payload);
        assert_eq!(
            descriptor.selected.as_ref().unwrap().blake3,
            proposed.blake3
        );
        assert_eq!(
            descriptor.selected.as_ref().unwrap().size_bytes,
            proposed.size_bytes
        );
        if self.mode.load(Ordering::SeqCst) == 7 {
            let selected = tinytree_client::CloudRawDigest::of(b"cloud raw bytes");
            descriptor.selected = Some(tinytree_client::cloud_protocol::RawDigest {
                blake3: selected.blake3.to_vec(),
                size_bytes: selected.size_bytes,
            });
        }
        if self.mode.load(Ordering::SeqCst) == 8 {
            descriptor.id.as_mut().unwrap().semantic_id = vec![9; 64];
        }
        Ok(Response::new(
            tinytree_client::cloud_protocol::WriteReceipt {
                descriptor: Some(descriptor),
                staging_lease: "a".repeat(64),
                expires_at_unix_seconds: 2_000_000_000,
            },
        ))
    }
    async fn lookup_objects(
        &self,
        request: Request<tinytree_client::cloud_protocol::LookupObjectsRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::LookupObjectsResponse>, Status> {
        use tinytree_client::cloud_protocol as c;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let selections = request
            .into_inner()
            .ids
            .into_iter()
            .map(|mut id| {
                if self.mode.load(Ordering::SeqCst) == 4 {
                    id.semantic_id = vec![9; 64]
                }
                c::ObjectSelection {
                    descriptor: Some(c::ObjectDescriptor {
                        id: Some(id),
                        selected: Some(c::RawDigest {
                            blake3: blake3::hash(b"cloud raw bytes").as_bytes().to_vec(),
                            size_bytes: 15,
                        }),
                    }),
                }
            })
            .collect();
        Ok(Response::new(c::LookupObjectsResponse { selections }))
    }
    type ReadObjectStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<
                    Item = Result<tinytree_client::cloud_protocol::ObjectChunk, Status>,
                > + Send,
        >,
    >;
    async fn read_object(
        &self,
        request: Request<tinytree_client::cloud_protocol::ReadObjectRequest>,
    ) -> Result<Response<Self::ReadObjectStream>, Status> {
        use tinytree_client::cloud_protocol as c;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut bytes = b"cloud raw bytes".to_vec();
        let mode = self.mode.load(Ordering::SeqCst);
        let digest = if mode == 2 {
            vec![0; 32]
        } else {
            blake3::hash(&bytes).as_bytes().to_vec()
        };
        if mode == 6 {
            bytes[0] ^= 1;
        }
        let descriptor = Some(c::ObjectDescriptor {
            id: request.into_inner().id,
            selected: Some(c::RawDigest {
                blake3: digest,
                size_bytes: bytes.len() as u64,
            }),
        });
        let frame = c::ObjectChunk {
            descriptor,
            offset: 0,
            data: bytes,
            eof: mode != 1,
        };
        let mut frames = vec![Ok(frame.clone())];
        if mode == 3 {
            frames.push(Ok(frame))
        }
        Ok(Response::new(Box::pin(futures_util::stream::iter(frames))))
    }
    async fn put_index_artifact(
        &self,
        request: Request<tinytree_client::cloud_protocol::PutIndexArtifactRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::IndexArtifactWriteReceipt>, Status> {
        use tinytree_client::cloud_protocol as c;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        let mut descriptor = request.descriptor.unwrap();
        assert_eq!(
            descriptor.selected.as_ref().unwrap().blake3,
            blake3::hash(&request.payload).as_bytes().to_vec()
        );
        if self.mode.load(Ordering::SeqCst) == 7 {
            descriptor.selected.as_mut().unwrap().blake3 = vec![0; 32];
        }
        Ok(Response::new(c::IndexArtifactWriteReceipt {
            descriptor: Some(descriptor),
            staging_lease: "b".repeat(64),
            expires_at_unix_seconds: 2_000_000_000,
        }))
    }
    async fn lookup_index_artifacts(
        &self,
        request: Request<tinytree_client::cloud_protocol::LookupIndexArtifactsRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::LookupIndexArtifactsResponse>, Status>
    {
        use tinytree_client::cloud_protocol as c;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = self.mode.load(Ordering::SeqCst);
        let mut selections: Vec<_> = request
            .into_inner()
            .ids
            .into_iter()
            .map(|mut id| {
                if id.semantic_id.iter().all(|b| *b == 0) {
                    return c::IndexArtifactSelection { descriptor: None };
                }
                if mode == 10 {
                    id.kind = if id.kind == c::IndexArtifactKind::Root as i32 {
                        c::IndexArtifactKind::Node as i32
                    } else {
                        c::IndexArtifactKind::Root as i32
                    };
                }
                c::IndexArtifactSelection {
                    descriptor: Some(c::IndexArtifactDescriptor {
                        id: Some(id),
                        selected: Some(c::RawDigest {
                            blake3: blake3::hash(b"portable index transport payload")
                                .as_bytes()
                                .to_vec(),
                            size_bytes: b"portable index transport payload".len() as u64,
                        }),
                    }),
                }
            })
            .collect();
        if mode == 4 {
            selections.push(selections[0].clone());
        }
        Ok(Response::new(c::LookupIndexArtifactsResponse {
            selections,
        }))
    }
    type ReadIndexArtifactStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<
                    Item = Result<tinytree_client::cloud_protocol::IndexArtifactChunk, Status>,
                > + Send,
        >,
    >;
    async fn read_index_artifact(
        &self,
        request: Request<tinytree_client::cloud_protocol::ReadIndexArtifactRequest>,
    ) -> Result<Response<Self::ReadIndexArtifactStream>, Status> {
        use tinytree_client::cloud_protocol as c;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = self.mode.load(Ordering::SeqCst);
        let mut id = request.into_inner().id.unwrap();
        if mode == 10 {
            id.kind = if id.kind == c::IndexArtifactKind::Root as i32 {
                c::IndexArtifactKind::Node as i32
            } else {
                c::IndexArtifactKind::Root as i32
            };
        }
        if mode == 11 {
            id.semantic_id.pop();
        }
        if mode == 12 {
            id.semantic_id.push(1);
        }
        let mut bytes = b"portable index transport payload".to_vec();
        let digest = blake3::hash(&bytes).as_bytes().to_vec();
        if mode == 6 {
            bytes[0] ^= 1;
        }
        let descriptor = Some(c::IndexArtifactDescriptor {
            id: Some(id),
            selected: Some(c::RawDigest {
                blake3: digest,
                size_bytes: bytes.len() as u64,
            }),
        });
        if mode == 9 {
            let entered = self.index_entered.clone();
            let release = self.index_release.clone();
            return Ok(Response::new(Box::pin(async_stream::stream! {
                let split=bytes.len()/2;
                yield Ok(c::IndexArtifactChunk{descriptor:descriptor.clone(),offset:0,data:bytes[..split].to_vec(),eof:false});
                entered.notify_one();release.notified().await;
                yield Ok(c::IndexArtifactChunk{descriptor,offset:split as u64,data:bytes[split..].to_vec(),eof:true});
            })));
        }
        let frame = c::IndexArtifactChunk {
            descriptor,
            offset: 0,
            data: bytes,
            eof: mode != 1,
        };
        let mut frames = vec![Ok(frame.clone())];
        if mode == 3 {
            frames.push(Ok(frame))
        }
        Ok(Response::new(Box::pin(futures_util::stream::iter(frames))))
    }
    async fn publish_version(
        &self,
        request: Request<tinytree_client::cloud_protocol::PublishVersionRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::Publication>, Status> {
        use tinytree_client::cloud_protocol as c;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        if request.manifest.get(8..10) == Some(3u16.to_le_bytes().as_slice()) {
            assert_eq!(request.capture_data, capture_transport_fixture(40).0);
        } else {
            assert!(
                request.capture_data.is_empty(),
                "legacy publication sends no capture DATA"
            );
        }
        let value = c::Publication {
            idempotency_key: request.idempotency_key,
            workspace: Some(c::Workspace {
                workspace_id: request.workspace_id,
                generation: request.expected_generation + 1,
                manifest_id: Some(request.manifest_id),
            }),
        };
        *self.publication.lock().unwrap() = Some(value.clone());
        if matches!(self.mode.load(Ordering::SeqCst), 5 | 55) {
            return Err(Status::unavailable("publication reply lost"));
        }
        Ok(Response::new(value))
    }
    async fn get_workspace(
        &self,
        request: Request<tinytree_client::cloud_protocol::GetWorkspaceRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::Workspace>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(tinytree_client::cloud_protocol::Workspace {
            workspace_id: request.into_inner().workspace_id,
            generation: 0,
            manifest_id: None,
        }))
    }
    async fn get_version(
        &self,
        request: Request<tinytree_client::cloud_protocol::GetVersionRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::Version>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        let manifest = if self.mode.load(Ordering::SeqCst) >= 40 {
            capture_transport_fixture(self.mode.load(Ordering::SeqCst)).1
        } else {
            b"raw manifest transport fixture".to_vec()
        };
        let native_authority =
            if self.mode.load(Ordering::SeqCst) >= 30 && self.mode.load(Ordering::SeqCst) != 35 {
                let raw = tinytree_client::CloudRawDigest::of(&manifest);
                let workspace: [u8; 32] = request.workspace_id.as_slice().try_into().unwrap();
                let binding = tinytree_model::cloud_native_binding_digest(
                    &request.repository_id,
                    [7; 32],
                    workspace,
                    request.generation,
                    tinytree_client::CloudManifestId::of(&manifest).0,
                    raw.blake3,
                    raw.size_bytes,
                );
                Some(tinytree_client::cloud_protocol::NativeVersionAuthority {
                    version: 1,
                    owner_binding: vec![7; 32],
                    binding_digest: binding.to_vec(),
                    manifest_selected: Some(tinytree_client::cloud_protocol::RawDigest {
                        blake3: raw.blake3.to_vec(),
                        size_bytes: raw.size_bytes,
                    }),
                })
            } else {
                None
            };
        Ok(Response::new(tinytree_client::cloud_protocol::Version {
            workspace_id: request.workspace_id,
            generation: request.generation,
            manifest_id: tinytree_client::CloudManifestId::of(&manifest).0.to_vec(),
            previous_manifest_id: if request.generation == 1 {
                None
            } else {
                Some(vec![3; 32])
            },
            manifest,
            native_authority,
        }))
    }
    type ReadNativeClosureStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<
                    Item = Result<tinytree_client::cloud_protocol::NativeClosureChunk, Status>,
                > + Send,
        >,
    >;
    async fn read_native_closure(
        &self,
        request: Request<tinytree_client::cloud_protocol::ReadNativeClosureRequest>,
    ) -> Result<Response<Self::ReadNativeClosureStream>, Status> {
        use tinytree_client::cloud_protocol as c;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        let mode = self.mode.load(Ordering::SeqCst);
        if let Some(c::native_closure_proof::Terminal::SnapshotCommit(terminal)) = request
            .proof
            .as_ref()
            .and_then(|proof| proof.terminal.as_ref())
        {
            self.native_snapshot_calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(terminal.index_root_ids, vec![vec![11; 32], vec![12; 32]]);
            assert_eq!(terminal.commit_id, vec![13; 32]);
            if mode == 36 {
                return Err(Status::unimplemented(
                    "snapshot native terminal unavailable",
                ));
            }
        }
        let requested = request.requested.unwrap();
        let (descriptor, mut bytes) = if requested.kind == Kind::File as i32 {
            let bytes = vec![0x5a; 5 * 1024 * 1024];
            (tinytree_model::describe_file(&bytes), bytes)
        } else {
            let encoded = tinytree_model::encode(
                &tinytree_model::Object::Symlink("retained target".into()),
                Default::default(),
            )
            .unwrap();
            (encoded.descriptor, encoded.bytes)
        };
        assert_eq!(requested.native_id, descriptor.key.id.0);
        let binding = if mode == 32 {
            vec![0; 32]
        } else {
            request.authority.unwrap().binding_digest
        };
        if mode == 34 {
            bytes[0] ^= 1;
        }
        let wire = p::ObjectDescriptor {
            key: Some(p::ObjectKey {
                kind: descriptor.key.kind as i32,
                native_id: descriptor.key.id.0.to_vec(),
            }),
            size_bytes: descriptor.size_bytes,
            content_blake3: descriptor.key.id.0.to_vec(),
            object_format_version: u32::from(descriptor.format_version),
        };
        let stream = async_stream::stream! {
            yield Ok(c::NativeClosureChunk{descriptor:Some(wire.clone()),offset:0,data:vec![],eof:false,source_binding_digest:binding.clone()});
            for (n,chunk) in bytes.chunks(64*1024).enumerate(){yield Ok(c::NativeClosureChunk{descriptor:Some(wire.clone()),offset:(n*64*1024) as u64,data:chunk.to_vec(),eof:false,source_binding_digest:binding.clone()});}
            if mode!=33 {yield Ok(c::NativeClosureChunk{descriptor:Some(wire),offset:bytes.len() as u64,data:vec![],eof:true,source_binding_digest:binding});}
        };
        Ok(Response::new(Box::pin(stream)))
    }
    type ReadCaptureStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<
                    Item = Result<tinytree_client::cloud_protocol::CaptureChunk, Status>,
                > + Send,
        >,
    >;
    async fn read_capture(
        &self,
        request: Request<tinytree_client::cloud_protocol::ReadCaptureRequest>,
    ) -> Result<Response<Self::ReadCaptureStream>, Status> {
        use tinytree_client::cloud_protocol as c;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        let mode = self.mode.load(Ordering::SeqCst);
        let (mut data, manifest) = capture_transport_fixture(mode);
        let selector = request.selector.unwrap();
        let manifest_id = tinytree_client::CloudManifestId::of(&manifest).0;
        assert_eq!(selector.manifest_id, manifest_id);
        let raw = request.manifest_selected.unwrap();
        assert_eq!(raw.blake3, blake3::hash(&manifest).as_bytes());
        assert_eq!(raw.size_bytes, manifest.len() as u64);
        let selected = tinytree_client::CloudRawDigest::of(&data);
        let mut descriptor = c::CaptureDescriptor {
            capture_id: request.expected_capture_id,
            selected: Some(c::RawDigest {
                blake3: selected.blake3.to_vec(),
                size_bytes: selected.size_bytes,
            }),
        };
        if mode == 42 {
            descriptor.capture_id[0] ^= 1;
        }
        if mode == 60 {
            descriptor.capture_id.pop();
        }
        if mode == 44 {
            data[0] ^= 1;
        }
        let mut echo = manifest_id.to_vec();
        if mode == 43 {
            echo[0] ^= 1;
        }
        if mode == 59 {
            echo.clear();
        }
        let chunk = |offset, data, eof| c::CaptureChunk {
            descriptor: Some(descriptor.clone()),
            offset,
            data,
            eof,
            manifest_id: echo.clone(),
        };
        let mut frames = Vec::new();
        if matches!(mode, 49 | 51) {
            frames.push(Ok(chunk(0, vec![], false)));
        }
        if mode == 49 {
            frames.push(Ok(chunk(0, vec![], false)));
        }
        if mode == 50 {
            frames.push(Ok(chunk(0, vec![0; 64 * 1024 + 1], true)));
        } else {
            frames.push(Ok(chunk(
                if mode == 45 { 1 } else { 0 },
                data.clone(),
                !matches!(mode, 41 | 48),
            )));
            if mode == 46 {
                frames.push(Ok(chunk(data.len() as u64, vec![], true)));
            }
        }
        if mode == 47 {
            let first = chunk(0, data[..data.len() / 2].to_vec(), false);
            let last = chunk(
                (data.len() / 2) as u64,
                data[data.len() / 2..].to_vec(),
                true,
            );
            let entered = self.index_entered.clone();
            let release = self.index_release.clone();
            return Ok(Response::new(Box::pin(async_stream::stream! {
                yield Ok(first);entered.notify_one();release.notified().await;yield Ok(last);
            })));
        }
        Ok(Response::new(Box::pin(futures_util::stream::iter(frames))))
    }
    async fn get_publication(
        &self,
        _: Request<tinytree_client::cloud_protocol::GetPublicationRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::GetPublicationResponse>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(
            tinytree_client::cloud_protocol::GetPublicationResponse {
                publication: self.publication.lock().unwrap().clone(),
            },
        ))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workspace_cloud_checks_stream_selection_reconciles_intent_and_releases_call_owners() {
    use tinytree_client::cloud_protocol::workspace_cloud_server::WorkspaceCloudServer;
    use tinytree_client::{CloudDescriptor, CloudObjectId, CloudPublishIntent, CloudRawDigest};
    let faults = CloudFaults {
        mode: Arc::new(AtomicUsize::new(0)),
        calls: Arc::new(AtomicUsize::new(0)),
        publication: Arc::new(std::sync::Mutex::new(None)),
        ..Default::default()
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = async_stream::stream! {loop{match listener.accept().await{Ok((stream,_))=>yield Ok::<_,std::io::Error>(stream),Err(error)=>{yield Err(error);break}}}};
    let service = WorkspaceCloudServer::new(faults.clone())
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd);
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap()
    });
    let mut config = Config::default();
    config.endpoint = format!("http://{address}");
    config.limits.max_inflight = 1;
    config.rpc.retry.max_attempts = 1;
    let serialized = toml::to_string(&config).unwrap();
    assert!(
        !serialized.contains("[cloud]"),
        "default cloud limits preserve existing serialized provider identity"
    );
    let client = NativeClient::connect(config.clone()).await.unwrap();
    let id = CloudObjectId::Operation([8; 64]);
    let descriptor = CloudDescriptor {
        id,
        selected: CloudRawDigest::of(b"cloud raw bytes"),
    };
    assert_eq!(
        client
            .cloud_put_object(id, b"cloud raw bytes")
            .await
            .unwrap()
            .descriptor,
        descriptor
    );
    assert_eq!(
        client.cloud_lookup_objects(&[id]).await.unwrap(),
        vec![Some(descriptor)]
    );
    // A verified server keeps the first selected raw representation for an
    // existing semantic ID, even when a later upload proposes another encoding.
    faults.mode.store(7, Ordering::SeqCst);
    let alternate = b"later proposed representation";
    let selected = client.cloud_put_object(id, alternate).await.unwrap();
    assert_eq!(
        selected.descriptor,
        CloudDescriptor {
            id,
            selected: CloudRawDigest::of(b"cloud raw bytes")
        }
    );
    assert_ne!(selected.descriptor.selected, CloudRawDigest::of(alternate));
    faults.mode.store(8, Ordering::SeqCst);
    assert_eq!(
        client
            .cloud_put_object(id, b"cloud raw bytes")
            .await
            .unwrap_err()
            .code(),
        Code::DataLoss
    );
    faults.mode.store(0, Ordering::SeqCst);

    let physical = Arc::new(AtomicUsize::new(0));
    let persistent = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(persistent.clone())))
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(physical.clone())));
    let object = guarded.cloud_read_object(descriptor).await.unwrap();
    assert_eq!(object.payload(), b"cloud raw bytes");
    drop(guarded);
    assert_released(&physical).await;
    assert_eq!(persistent.load(Ordering::SeqCst), 0);
    assert_eq!(
        client.cloud_workspace([1; 32]).await.unwrap().generation,
        0,
        "holding raw result must not retain the only call slot"
    );
    drop(object);
    assert_released(&persistent).await;
    for mode in [1, 2, 3, 6] {
        faults.mode.store(mode, Ordering::SeqCst);
        assert_eq!(
            client
                .cloud_read_object(descriptor)
                .await
                .unwrap_err()
                .code(),
            Code::DataLoss
        );
    }
    faults.mode.store(4, Ordering::SeqCst);
    assert_eq!(
        client.cloud_lookup_objects(&[id]).await.unwrap_err().code(),
        Code::DataLoss
    );
    faults.mode.store(5, Ordering::SeqCst);
    let intent = CloudPublishIntent {
        workspace_id: [4; 32],
        expected_generation: 7,
        idempotency_key: "original-publication-intent".into(),
        manifest: b"raw manifest transport fixture".to_vec(),
        staging_leases: vec!["a".repeat(64)],
    };
    let publication = client.cloud_publish_version(&intent).await.unwrap();
    assert_eq!(publication.workspace.generation, 8);
    assert_eq!(publication.idempotency_key, intent.idempotency_key);
    let version = client.cloud_version([4; 32], 8).await.unwrap();
    assert_eq!(version.manifest(), intent.manifest);
    let before = faults.calls.load(Ordering::SeqCst);
    assert_eq!(
        client
            .cloud_put_object(id, &vec![0; config.cloud.max_object_bytes + 1])
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        client
            .cloud_lookup_objects(&vec![id; config.cloud.max_lookup_items + 1])
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        faults.calls.load(Ordering::SeqCst),
        before,
        "oversized inputs fail before any RPC"
    );
    let mut tiny = config.clone();
    tiny.limits.max_frame_bytes = 1024;
    tiny.limits.max_batch_bytes = 1024;
    tiny.limits.max_metadata_bytes = 1024;
    tiny.limits.max_request_bytes = 1024;
    tiny.limits.max_graph_bytes = 1024;
    tiny.limits.max_buffered_bytes = 16384;
    tiny.limits.max_batch_items = 4;
    tiny.limits.max_graph_objects = 1;
    tiny.validate().unwrap();
    let tiny = NativeClient::connect(tiny).await.unwrap();
    assert_eq!(
        tiny.cloud_read_object(descriptor).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        faults.calls.load(Ordering::SeqCst),
        before,
        "a small existing native config stays valid but rejects cloud work before RPC"
    );
    server.abort();
    let _ = server.await;
}

fn scoped_singleton_expressions(
    root: tinytree_model::CommitId,
) -> Vec<(&'static str, q::GraphExpr)> {
    let intersection = |inputs| q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Intersection(q::SetExpr {
            inputs,
        })),
    };
    vec![
        ("literal", ids_commits(&[root])),
        (
            "general before literal",
            intersection(vec![ids_ancestors(root), ids_commits(&[root])]),
        ),
        (
            "literal before general",
            intersection(vec![ids_commits(&[root]), ids_ancestors(root)]),
        ),
        (
            "nested intersection",
            intersection(vec![
                ids_ancestors(root),
                intersection(vec![ids_commits(&[root]), ids_ancestors(root)]),
            ]),
        ),
    ]
}

#[tokio::test]
async fn held_wide_graph_query_leaves_capacity_for_a_scoped_literal_without_larger_pool() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    config.limits.max_metadata_bytes = 4 * 1024 * 1024;
    config.limits.max_batch_bytes = 4 * 1024 * 1024;
    config.limits.max_graph_bytes = 8 * 1024 * 1024;
    config.limits.max_graph_objects = 65536;
    config.limits.max_buffered_bytes = 32 * 1024 * 1024;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut wide = client
        .begin_graph_query(&scope, &ids_ancestors(fixture.root), 1, true)
        .await
        .unwrap();
    let held = client.evaluate_graph_page(&mut wide).await.unwrap();
    assert!(!held.rows().is_empty());
    for (name, expression) in scoped_singleton_expressions(fixture.root) {
        let mut literal = client
            .begin_graph_query(&scope, &expression, 1, true)
            .await
            .unwrap();
        let page = client.evaluate_graph_page(&mut literal).await.unwrap();
        assert_eq!(
            page.rows().iter().map(|r| r.commit).collect::<Vec<_>>(),
            vec![fixture.root],
            "{name}"
        );
        assert!(page.rows()[0].canonical.is_some(), "{name}");
        client.close_graph_query(&mut literal).await.unwrap();
    }
    client.close_graph_query(&mut wide).await.unwrap();
    drop(held);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn literal_queries_reject_overreturned_rows_without_advancing_ids_or_graph_state() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::LiteralOverReturn).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    // Page size deliberately permits two rows; the tighter structural bound,
    // not the page cap or duplicate check, must reject before advancing state.
    for (name, expression) in scoped_singleton_expressions(fixture.root) {
        let mut ids = client.begin_query(&scope, &expression, 2).await.unwrap();
        for _ in 0..2 {
            assert_eq!(
                client.evaluate_page(&mut ids).await.unwrap_err().code(),
                Code::ResourceExhausted,
                "{name}"
            );
            assert!(!ids.is_exhausted(), "{name}");
        }
        client.close_ids_query(&mut ids).await.unwrap();
        let mut graph = client
            .begin_graph_query(&scope, &expression, 2, true)
            .await
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                client
                    .evaluate_graph_page(&mut graph)
                    .await
                    .unwrap_err()
                    .code(),
                Code::ResourceExhausted,
                "{name}"
            );
            assert!(!graph.is_exhausted(), "{name}");
        }
        client.close_graph_query(&mut graph).await.unwrap();
    }
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn membership_facts_validate_original_expression_before_rpc_even_for_absent_candidates() {
    use tinytree_model::CommitId;
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut wrong_version = ids_commits(&[fixture.root]);
    wrong_version.version = 2;
    let missing = q::GraphExpr {
        version: 1,
        expression: None,
    };
    let huge_id = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
            commits: vec![q::CommitId {
                value: vec![1; running.config.limits.max_request_bytes + 1],
            }],
        })),
    };
    let reversed = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Ancestors(Box::new(
            q::AncestorsExpr {
                heads: Some(Box::new(ids_commits(&[fixture.root]))),
                generations: Some(q::GenerationRange {
                    start: 2,
                    end_exclusive: Some(1),
                }),
                parents: None,
            },
        ))),
    };
    let too_wide = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Union(q::SetExpr {
            inputs: vec![ids_commits(&[]); running.config.limits.max_batch_items],
        })),
    };
    for (name, expression, expected) in [
        ("unknown version", wrong_version, Code::InvalidArgument),
        ("missing operator", missing, Code::InvalidArgument),
        ("oversized malformed ID", huge_id, Code::InvalidArgument),
        (
            "reversed generation interval",
            reversed,
            Code::InvalidArgument,
        ),
        (
            "aggregate expression nodes",
            too_wide,
            Code::ResourceExhausted,
        ),
    ] {
        let error = client
            .query_facts(
                &scope,
                vec![QueryFact::Membership {
                    expression,
                    candidate: CommitId([99; 32]),
                }],
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), expected, "{name}");
    }
    assert_eq!(
        running.counts.facts.load(Ordering::SeqCst),
        0,
        "invalid original expressions must not be cloned into transport requests"
    );
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn membership_facts_reject_missing_wrong_extra_and_unbound_answers() {
    for mode in [
        QueryMode::MissingFact,
        QueryMode::MissingAnswer,
        QueryMode::WrongAnswer,
        QueryMode::ExtraFact,
        QueryMode::WrongDigest,
        QueryMode::WrongEpoch,
        QueryMode::RepeatedEmptyIds,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let error = client
            .query_facts(
                &scope,
                vec![QueryFact::Membership {
                    expression: ids_ancestors(fixture.root),
                    candidate: fixture.root,
                }],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error.code(), Code::DataLoss | Code::ResourceExhausted),
            "{mode:?}: {error}"
        );
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn repeated_membership_facts_do_not_consume_resumable_query_history() {
    use tinytree_model::CommitId;
    let mut config = ServiceConfig::default();
    config.query.max_queries = 2;
    config.query.max_queries_per_principal = 2;
    config.query.max_queries_per_repository = 2;
    config.query.max_query_replays = 128;
    config.query.max_query_replays_per_principal = 128;
    config.query.max_query_replays_per_repository = 128;
    let fixture = setup_query_configured(config);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    for _ in 0..160 {
        assert_eq!(
            client
                .query_facts(
                    &scope,
                    vec![
                        QueryFact::Membership {
                            expression: ids_ancestors(fixture.root),
                            candidate: fixture.root
                        },
                        QueryFact::Membership {
                            expression: ids_commits(&[fixture.root]),
                            candidate: CommitId::ROOT
                        },
                        QueryFact::Membership {
                            expression: ids_ancestors(fixture.root),
                            candidate: CommitId([99; 32])
                        },
                    ]
                )
                .await
                .unwrap(),
            vec![
                FactAnswer::Membership(true),
                FactAnswer::Membership(false),
                FactAnswer::Membership(false)
            ]
        );
    }
    assert_eq!(running.counts.facts.load(Ordering::SeqCst), 160);
    assert_eq!(running.counts.evaluations.load(Ordering::SeqCst), 0);
    assert_eq!(running.counts.query_closes.load(Ordering::SeqCst), 0);
    // The real resumable registry remains usable beyond its replay quota.
    let mut query = client
        .begin_query(&scope, &ids_commits(&[fixture.root]), 1)
        .await
        .unwrap();
    let page = client.evaluate_page(&mut query).await.unwrap();
    assert_eq!(page.rows[0].commit, fixture.root);
    client.close_ids_query(&mut query).await.unwrap();
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

fn single_query_history_config() -> ServiceConfig {
    let mut config = ServiceConfig::default();
    config.query.max_queries = 1;
    config.query.max_queries_per_principal = 1;
    config.query.max_queries_per_repository = 1;
    config.query.max_query_replays = 1;
    config.query.max_query_replays_per_principal = 1;
    config.query.max_query_replays_per_repository = 1;
    config
}

#[tokio::test]
async fn v2_query_identity_negotiation_binds_exact_scope_and_preserves_legacy_bytes() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let legacy = client.open_query_scope(vec![fixture.root]).await.unwrap();
    assert_eq!(legacy.query_identity_version(), 1);
    let literal = ids_commits(&[fixture.root]);
    let exact = [37; 32];
    let legacy_query = client
        .begin_query_with_id(&legacy, &literal, 1, exact)
        .await
        .unwrap();
    assert_eq!(legacy_query.query_id(), &exact);
    drop(legacy_query);
    let first = client
        .open_query_scope_v2(vec![fixture.root])
        .await
        .unwrap();
    let second = client
        .open_query_scope_v2(vec![fixture.root])
        .await
        .unwrap();
    assert_eq!(first.query_identity_version(), 2);
    let id = first.bound_query_id([11; 16]).unwrap();
    assert_eq!(&id[..16], &[11; 16]);
    assert_ne!(id, second.bound_query_id([11; 16]).unwrap());
    let mut query = client
        .begin_query_with_id(&first, &literal, 1, id)
        .await
        .unwrap();
    assert_eq!(query.query_id(), &id);
    assert_eq!(
        client
            .begin_query_with_id(&second, &literal, 1, id)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .begin_query_with_id(&first, &literal, 1, exact)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    client.evaluate_page(&mut query).await.unwrap();
    client.close_ids_query(&mut query).await.unwrap();
    let child = client.fork_query_scope(&first, vec![]).await.unwrap();
    assert_eq!(child.query_identity_version(), 2);
    assert_ne!(child.bound_query_id([11; 16]).unwrap(), id);
    for scope in [&legacy, &first, &second, &child] {
        client.close_query_scope(scope).await.unwrap();
    }
    // Mode is part of the identity namespace. Retiring closed v2 history does
    // not revoke legacy v1's ability to use arbitrary raw identity bytes.
    let legacy_reuse = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut reused = client
        .begin_query_with_id(&legacy_reuse, &literal, 1, id)
        .await
        .unwrap();
    assert_eq!(reused.query_id(), &id);
    assert_eq!(
        client.evaluate_page(&mut reused).await.unwrap().rows[0].commit,
        fixture.root
    );
    client.close_ids_query(&mut reused).await.unwrap();
    client.close_query_scope(&legacy_reuse).await.unwrap();
    running.stop().await;
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::LegacyScopeVersion).await;
    let client = running.client().await;
    let legacy = client.open_query_scope(vec![fixture.root]).await.unwrap();
    assert_eq!(legacy.query_identity_version(), 1);
    assert_eq!(
        client
            .open_query_scope_v2(vec![fixture.root])
            .await
            .unwrap_err()
            .code(),
        Code::DataLoss,
        "explicit v2 must not accept an unnegotiated response"
    );
    client.close_query_scope(&legacy).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn closed_v2_scope_retires_only_explicitly_closed_queries_in_both_close_orders() {
    let fixture = setup_query_configured(single_query_history_config());
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    for scope_first in [false, true] {
        for _ in 0..8 {
            let scope = client
                .open_query_scope_v2(vec![fixture.root])
                .await
                .unwrap();
            let mut query = client
                .begin_query(&scope, &ids_commits(&[fixture.root]), 1)
                .await
                .unwrap();
            let id = *query.query_id();
            client.evaluate_page(&mut query).await.unwrap();
            if scope_first {
                client.close_query_scope(&scope).await.unwrap();
            }
            client.close_ids_query(&mut query).await.unwrap();
            if !scope_first {
                client.close_query_scope(&scope).await.unwrap();
            }
            let mut late = client
                .begin_query_with_id(&scope, &ids_commits(&[fixture.root]), 1, id)
                .await
                .unwrap();
            assert_eq!(
                client.evaluate_page(&mut late).await.unwrap_err().code(),
                Code::FailedPrecondition
            );
            client.close_ids_query(&mut late).await.unwrap();
        }
    }
    // A raw pre-Evaluate close creates an ordered tombstone; the SDK correctly
    // avoids sending cleanup for a locally prepared but unattempted query.
    let channel = tonic::transport::Endpoint::from_shared(running.config.endpoint.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut raw = q::repository_query_client::RepositoryQueryClient::new(channel)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd)
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
    for _ in 0..4 {
        let scope = client
            .open_query_scope_v2(vec![fixture.root])
            .await
            .unwrap();
        let id = scope.bound_query_id([55; 16]).unwrap();
        let close_request = || {
            let mut request = Request::new(q::CloseQueryRequest {
                scope: Some(q::ScopeKey {
                    repository_id: scope.repository_id().into(),
                    handle: scope.handle().to_vec(),
                }),
                query_id: id.to_vec(),
            });
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
            request
        };
        raw.close_query(close_request()).await.unwrap();
        let mut late = client
            .begin_query_with_id(&scope, &ids_commits(&[fixture.root]), 1, id)
            .await
            .unwrap();
        assert_eq!(
            client.evaluate_page(&mut late).await.unwrap_err().code(),
            Code::FailedPrecondition,
            "pre-Evaluate tombstone must prevent a late first request"
        );
        client.close_query_scope(&scope).await.unwrap();
        raw.close_query(close_request()).await.unwrap();
        client.close_ids_query(&mut late).await.unwrap();
    }
    running.stop().await;
    // Legacy closed history intentionally remains fenced until original expiry.
    let fixture = setup_query_configured(single_query_history_config());
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut query = client
        .begin_query(&scope, &ids_commits(&[fixture.root]), 1)
        .await
        .unwrap();
    client.evaluate_page(&mut query).await.unwrap();
    client.close_ids_query(&mut query).await.unwrap();
    client.close_query_scope(&scope).await.unwrap();
    let next = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut query = client
        .begin_query(&next, &ids_commits(&[fixture.root]), 1)
        .await
        .unwrap();
    assert_eq!(
        client.evaluate_page(&mut query).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    client.close_query_scope(&next).await.unwrap();
    drop(query);
    running.stop().await;
}

#[tokio::test]
async fn closed_v2_scope_keeps_lost_final_replay_until_explicit_query_close() {
    let fixture = setup_query_configured(single_query_history_config());
    let running = QueryRunning::start(
        fixture.service.clone(),
        QueryMode::EvaluateRetryAfterScopeClose,
    )
    .await;
    let client = running.client().await;
    let scope = client
        .open_query_scope_v2(vec![fixture.root])
        .await
        .unwrap();
    let mut query = client
        .begin_query(&scope, &ids_commits(&[fixture.root]), 1)
        .await
        .unwrap();
    let mut selected = Vec::new();
    while !query.is_exhausted() {
        let page = client.evaluate_page(&mut query).await.unwrap();
        selected.extend(page.rows.into_iter().map(|row| row.commit));
    }
    assert_eq!(selected, vec![fixture.root]);
    assert_eq!(
        running.counts.lost_final_attempts.load(Ordering::SeqCst),
        2,
        "the actual final response must replay exactly once after its ACK is withheld"
    );
    let next = client
        .open_query_scope_v2(vec![fixture.root])
        .await
        .unwrap();
    let mut blocked = client
        .begin_query(&next, &ids_commits(&[fixture.root]), 1)
        .await
        .unwrap();
    assert_eq!(
        client.evaluate_page(&mut blocked).await.unwrap_err().code(),
        Code::ResourceExhausted,
        "EOF alone must not release lost-page replay history"
    );
    client.close_ids_query(&mut query).await.unwrap();
    let mut selected = Vec::new();
    while !blocked.is_exhausted() {
        selected.extend(
            client
                .evaluate_page(&mut blocked)
                .await
                .unwrap()
                .rows
                .into_iter()
                .map(|row| row.commit),
        );
    }
    assert_eq!(selected, vec![fixture.root]);
    client.close_ids_query(&mut blocked).await.unwrap();
    client.close_query_scope(&next).await.unwrap();
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v2_scope_close_orders_against_inflight_creation_and_existing_pages() {
    for (mode, created) in [
        (QueryMode::EvaluatePause, false),
        (QueryMode::EvaluatePauseAfter, true),
    ] {
        let fixture = setup_query_configured(single_query_history_config());
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let control = running.client().await;
        let scope = client
            .open_query_scope_v2(vec![fixture.root])
            .await
            .unwrap();
        let mut query = client
            .begin_query(&scope, &ids_ancestors(fixture.root), 1)
            .await
            .unwrap();
        let caller = client.clone();
        let task = tokio::spawn(async move {
            let result = caller.evaluate_page(&mut query).await;
            (result, query)
        });
        tokio::time::timeout(
            Duration::from_secs(2),
            running.counts.evaluate_entered.notified(),
        )
        .await
        .unwrap();
        // The paused Evaluate holds its physical byte budget. A separate
        // control connection exercises the server race without waiting for
        // another full unary reservation in that deliberately paused pool.
        control.close_query_scope(&scope).await.unwrap();
        running.counts.evaluate_release.notify_one();
        let (result, mut query) = task.await.unwrap();
        if created {
            assert!(!result.unwrap().exhausted);
            while !query.is_exhausted() {
                client.evaluate_page(&mut query).await.unwrap();
            }
        } else {
            assert_eq!(result.unwrap_err().code(), Code::FailedPrecondition);
        }
        client.close_ids_query(&mut query).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn selected_prefixes_keep_subsets_divergence_visibility_and_explicit_absence() {
    use tinytree_model::{ChangeId, CommitId};
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let a = private_draft(
        fixture.root,
        ChangeId([0xab; 16]),
        "selected visible sibling",
    );
    let aid = CommitId(a.descriptor.key.id.0);
    let b = private_draft(
        fixture.root,
        ChangeId([0xab; 16]),
        "selected hidden sibling",
    );
    let bid = CommitId(b.descriptor.key.id.0);
    let c = private_draft(fixture.root, ChangeId([0xac; 16]), "other change sibling");
    let cid = CommitId(c.descriptor.key.id.0);
    let scope = client
        .open_query_scope_with_overlay(
            vec![fixture.root],
            vec![
                checked_draft(&client, &a).await,
                checked_draft(&client, &b).await,
                checked_draft(&client, &c).await,
            ],
        )
        .await
        .unwrap();
    let all = ids_commits(&[CommitId::ROOT, fixture.root, aid, bid, cid]);
    for prefix in [
        "".to_string(),
        blake3::Hash::from(aid.0).to_hex().to_string(),
    ] {
        assert_eq!(
            client
                .resolve_commit_prefix_selected(&scope, &prefix, &all)
                .await
                .unwrap(),
            client.resolve_commit_prefix(&scope, &prefix).await.unwrap()
        );
    }
    for prefix in ["a", "ab"] {
        assert_eq!(
            client
                .resolve_change_prefix_selected(&scope, prefix, vec![aid], &all)
                .await
                .unwrap(),
            client
                .resolve_change_prefix(&scope, prefix, vec![aid])
                .await
                .unwrap()
        );
    }
    let only_a = ids_commits(&[aid]);
    assert_eq!(
        client
            .resolve_commit_prefix_selected(&scope, "", &only_a)
            .await
            .unwrap(),
        CommitPrefix::Single(aid)
    );
    assert_eq!(
        client
            .resolve_commit_prefix_selected(&scope, &blake3::Hash::from(bid.0).to_hex(), &only_a)
            .await
            .unwrap(),
        CommitPrefix::NoMatch
    );
    assert_eq!(
        client
            .resolve_commit_prefix(&scope, &blake3::Hash::from(bid.0).to_hex())
            .await
            .unwrap(),
        CommitPrefix::Single(bid)
    );
    let siblings = ids_commits(&[aid, bid]);
    let ChangePrefix::Single(selected) = client
        .resolve_change_prefix_selected(&scope, "a", vec![aid], &siblings)
        .await
        .unwrap()
    else {
        panic!("one distinct selected change must not be ambiguous")
    };
    assert_eq!(selected.change, ChangeId([0xab; 16]));
    let flags: std::collections::BTreeMap<_, _> = selected
        .targets
        .iter()
        .map(|v| (v.commit, v.visible))
        .collect();
    assert_eq!(
        flags,
        std::collections::BTreeMap::from([(aid, true), (bid, false)])
    );
    assert_eq!(
        client
            .resolve_change_prefix(&scope, "a", vec![aid])
            .await
            .unwrap(),
        ChangePrefix::Ambiguous
    );
    for (selection, target, expected) in [
        (&only_a, PrefixTarget::Commit(aid), Some(0)),
        (&only_a, PrefixTarget::Commit(bid), None),
        (&only_a, PrefixTarget::Change(ChangeId([0xab; 16])), Some(0)),
        (&only_a, PrefixTarget::Change(ChangeId([0xac; 16])), None),
    ] {
        assert_eq!(
            client
                .shortest_prefix_selected(&scope, target, vec![], selection)
                .await
                .unwrap(),
            expected
        );
    }
    assert!(
        client
            .shortest_prefix(&scope, PrefixTarget::Commit(bid), vec![])
            .await
            .unwrap()
            > 0,
        "the caller may explicitly request global fallback for a target absent from selection"
    );
    let empty = ids_commits(&[]);
    assert_eq!(
        client
            .resolve_commit_prefix_selected(&scope, "", &empty)
            .await
            .unwrap(),
        CommitPrefix::NoMatch
    );
    assert_eq!(
        client
            .shortest_prefix_selected(&scope, PrefixTarget::Commit(aid), vec![], &empty)
            .await
            .unwrap(),
        None
    );
    let root = ids_commits(&[CommitId::ROOT]);
    assert_eq!(
        client
            .resolve_commit_prefix_selected(&scope, "", &root)
            .await
            .unwrap(),
        CommitPrefix::Single(CommitId::ROOT)
    );
    assert_eq!(
        client
            .shortest_prefix_selected(&scope, PrefixTarget::Commit(CommitId::ROOT), vec![], &root)
            .await
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        running.counts.evaluations.load(Ordering::SeqCst),
        0,
        "selected prefix methods are bounded unary operations, not resumable history materialization"
    );
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn selected_prefixes_reject_ignored_selection_digest_and_inconsistent_presence() {
    for mode in [
        QueryMode::PrefixSelectionIgnored,
        QueryMode::PrefixSelectionDigestWrong,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let selection = ids_commits(&[fixture.root]);
        assert_eq!(
            client
                .resolve_commit_prefix_selected(&scope, "", &selection)
                .await
                .unwrap_err()
                .code(),
            Code::DataLoss
        );
        assert_eq!(
            client
                .resolve_change_prefix_selected(&scope, "", vec![], &selection)
                .await
                .unwrap_err()
                .code(),
            Code::DataLoss
        );
        assert_eq!(
            client
                .shortest_prefix_selected(
                    &scope,
                    PrefixTarget::Commit(fixture.root),
                    vec![],
                    &selection
                )
                .await
                .unwrap_err()
                .code(),
            Code::DataLoss
        );
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
    for mode in [
        QueryMode::ShortestSelectionMissing,
        QueryMode::ShortestFalseWithLength,
        QueryMode::LongestOverflow,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        assert_eq!(
            client
                .shortest_prefix_selected(
                    &scope,
                    PrefixTarget::Commit(fixture.root),
                    vec![],
                    &ids_commits(&[fixture.root])
                )
                .await
                .unwrap_err()
                .code(),
            Code::DataLoss
        );
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn selected_prefixes_validate_original_ast_and_propagate_work_exhaustion() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut malformed = ids_commits(&[]);
    malformed.version = 2;
    assert_eq!(
        client
            .resolve_commit_prefix_selected(&scope, "", &malformed)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .resolve_change_prefix_selected(&scope, "", vec![], &malformed)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .shortest_prefix_selected(
                &scope,
                PrefixTarget::Commit(tinytree_model::CommitId([99; 32])),
                vec![],
                &malformed
            )
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        running.counts.commit_prefixes.load(Ordering::SeqCst)
            + running.counts.change_prefixes.load(Ordering::SeqCst)
            + running.counts.shortest_prefixes.load(Ordering::SeqCst),
        0
    );
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
    let mut config = ServiceConfig::default();
    config.query.max_eval_steps = 1;
    let fixture = setup_query_configured(config);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    assert_eq!(
        client
            .resolve_commit_prefix_selected(&scope, "", &ids_ancestors(fixture.root))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted,
        "work exhaustion must not become NoMatch"
    );
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_index_artifact_rpc_checks_domains_bounds_retained_results_and_cancelled_reads() {
    use tinytree_client::cloud_protocol::workspace_cloud_server::WorkspaceCloudServer;
    use tinytree_client::{
        CloudIndexArtifactDescriptor as Descriptor, CloudIndexArtifactId as Id, CloudRawDigest,
    };
    let faults = CloudFaults::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = async_stream::stream! {loop{match listener.accept().await{Ok((stream,_))=>yield Ok::<_,std::io::Error>(stream),Err(error)=>{yield Err(error);break}}}};
    let service = WorkspaceCloudServer::new(faults.clone())
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd);
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap()
    });
    let mut config = Config::default();
    config.endpoint = format!("http://{address}");
    config.limits.max_inflight = 1;
    config.rpc.retry.max_attempts = 1;
    let client = NativeClient::connect(config.clone()).await.unwrap();
    let payload = b"portable index transport payload";
    let root = Id::of_root(payload);
    let node = Id::of_node(payload);
    assert_ne!(root, node);
    for (id, tag) in [
        (root, b"tinytree-index-root-v1\0".as_slice()),
        (node, b"tinytree-index-node-v1\0".as_slice()),
    ] {
        let mut hash = blake3::Hasher::new();
        hash.update(tag);
        hash.update(payload);
        let expected = *hash.finalize().as_bytes();
        assert!(matches!(id,Id::Root(value)|Id::Node(value)if value==expected));
        let descriptor = Descriptor {
            id,
            selected: CloudRawDigest::of(payload),
        };
        assert_eq!(
            client
                .cloud_put_index_artifact(id, payload)
                .await
                .unwrap()
                .descriptor,
            descriptor
        );
        assert_eq!(
            client
                .cloud_lookup_index_artifacts(&[id, Id::Root([0; 32])])
                .await
                .unwrap(),
            vec![Some(descriptor), None]
        );
        let physical = Arc::new(AtomicUsize::new(0));
        let persistent = Arc::new(AtomicUsize::new(0));
        let guarded = client
            .clone()
            .with_lifetime_guard(Arc::new(LifetimeProbe(persistent.clone())))
            .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(physical.clone())));
        let object = guarded.cloud_read_index_artifact(descriptor).await.unwrap();
        assert_eq!(object.payload(), payload);
        assert_eq!(object.descriptor(), descriptor);
        drop(guarded);
        assert_released(&physical).await;
        assert_eq!(persistent.load(Ordering::SeqCst), 0);
        client.cloud_workspace([1; 32]).await.unwrap();
        drop(object);
        assert_released(&persistent).await;
    }
    let descriptor = Descriptor {
        id: root,
        selected: CloudRawDigest::of(payload),
    };
    for mode in [1, 3, 6, 10, 11, 12] {
        faults.mode.store(mode, Ordering::SeqCst);
        let error = client
            .cloud_read_index_artifact(descriptor)
            .await
            .unwrap_err();
        assert!(
            matches!(error.code(), Code::DataLoss | Code::ResourceExhausted),
            "mode{mode}: {error}"
        );
    }
    let Id::Root(raw) = root else { unreachable!() };
    faults.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        client
            .cloud_read_index_artifact(Descriptor {
                id: Id::Node(raw),
                ..descriptor
            })
            .await
            .unwrap_err()
            .code(),
        Code::DataLoss,
        "matching raw hash is insufficient in the wrong semantic domain"
    );
    let before = faults.calls.load(Ordering::SeqCst);
    assert_eq!(
        client
            .cloud_put_index_artifact(Id::Node(raw), payload)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .cloud_put_index_artifact(root, &vec![0; config.cloud.max_object_bytes + 1])
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        client
            .cloud_lookup_index_artifacts(&vec![root; config.cloud.max_lookup_items + 1])
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert_eq!(faults.calls.load(Ordering::SeqCst), before);
    faults.mode.store(7, Ordering::SeqCst);
    assert_eq!(
        client
            .cloud_put_index_artifact(root, payload)
            .await
            .unwrap_err()
            .code(),
        Code::DataLoss,
        "canonical index artifacts cannot select alternate raw bytes for one ID"
    );
    for mode in [4, 10] {
        faults.mode.store(mode, Ordering::SeqCst);
        assert_eq!(
            client
                .cloud_lookup_index_artifacts(&[root])
                .await
                .unwrap_err()
                .code(),
            Code::DataLoss
        );
    }
    faults.mode.store(9, Ordering::SeqCst);
    let dropped = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(dropped.clone())));
    let task = tokio::spawn(async move { guarded.cloud_read_index_artifact(descriptor).await });
    tokio::time::timeout(Duration::from_secs(2), faults.index_entered.notified())
        .await
        .unwrap();
    task.abort();
    let _ = task.await;
    assert_released(&dropped).await;
    faults.mode.store(0, Ordering::SeqCst);
    faults.index_release.notify_one();
    assert_eq!(
        client
            .cloud_read_index_artifact(descriptor)
            .await
            .unwrap()
            .payload(),
        payload,
        "cancelled physical read must release the sole call slot"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_native_reads_bind_fresh_versions_and_stream_files_beyond_cloud_object_limit() {
    use tinytree_client::cloud_protocol::workspace_cloud_server::WorkspaceCloudServer;
    use tinytree_client::{CloudManifestId, CloudRawDigest, RetainedVersionSelector};
    let faults = CloudFaults::default();
    faults.mode.store(30, Ordering::SeqCst);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = async_stream::stream! {loop{match listener.accept().await{Ok((stream,_))=>yield Ok::<_,std::io::Error>(stream),Err(error)=>{yield Err(error);break}}}};
    let service = WorkspaceCloudServer::new(faults.clone())
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd);
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap()
    });
    let mut config = Config::default();
    config.endpoint = format!("http://{address}");
    config.repository_id = "example".into();
    config.limits.max_inflight = 1;
    config.limits.max_metadata_bytes = 4 * 1024 * 1024;
    config.limits.max_batch_bytes = 4 * 1024 * 1024;
    config.limits.max_graph_bytes = 8 * 1024 * 1024;
    config.limits.max_graph_objects = 65_536;
    config.limits.max_file_bytes = 64 * 1024 * 1024;
    config.limits.max_temporary_bytes = 128 * 1024 * 1024;
    config.limits.max_buffered_bytes = 32 * 1024 * 1024;
    config.rpc.retry.max_attempts = 1;
    let client = NativeClient::connect(config.clone()).await.unwrap();
    let manifest = b"raw manifest transport fixture";
    let selector = RetainedVersionSelector {
        workspace_id: [4; 32],
        generation: 1,
        manifest_id: CloudManifestId::of(manifest),
    };
    let selected = CloudRawDigest::of(manifest);
    let drops = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
    let version = guarded
        .cloud_retained_version_selected(selector, selected)
        .await
        .unwrap();
    drop(guarded);
    assert_eq!(version.manifest(), manifest);
    let encoded = tinytree_model::encode(
        &tinytree_model::Object::Symlink("retained target".into()),
        Default::default(),
    )
    .unwrap();
    let key = encoded.descriptor.key;
    let physical_drops = Arc::new(AtomicUsize::new(0));
    let physical = Arc::new(LifetimeProbe(physical_drops.clone()));
    let physical_weak = Arc::downgrade(&physical);
    let calls = client.clone().with_rpc_lifetime_guard(physical);
    let metadata = calls
        .cloud_read_native_metadata(&version, key, &[], key)
        .await
        .unwrap();
    assert_eq!(metadata.canonical_bytes(), encoded.bytes);
    let selected_roots = [[11; 32], [12; 32]];
    let terminal_commit = tinytree_model::CommitId([13; 32]);
    let snapshot_metadata = calls
        .cloud_read_snapshot_metadata(&version, &selected_roots, terminal_commit, &[key], key)
        .await
        .unwrap();
    assert_eq!(snapshot_metadata.canonical_bytes(), encoded.bytes);
    let before_invalid = faults.calls.load(Ordering::SeqCst);
    for roots in [vec![], vec![[11; 32], [11; 32]], vec![[12; 32], [11; 32]]] {
        assert_eq!(
            client
                .cloud_read_snapshot_metadata(&version, &roots, terminal_commit, &[key], key)
                .await
                .err()
                .unwrap()
                .code(),
            Code::InvalidArgument
        );
    }
    assert_eq!(faults.calls.load(Ordering::SeqCst), before_invalid);
    let other = NativeClient::connect(config.clone()).await.unwrap();
    let before = faults.calls.load(Ordering::SeqCst);
    assert_eq!(
        other
            .cloud_read_native_metadata(&version, key, &[], key)
            .await
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        other
            .cloud_read_snapshot_metadata(&version, &selected_roots, terminal_commit, &[key], key)
            .await
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(faults.calls.load(Ordering::SeqCst), before);
    let directory = private_tempdir();
    let filebytes = vec![0x5a; 5 * 1024 * 1024];
    let descriptor = tinytree_model::describe_file(&filebytes);
    assert!(filebytes.len() > config.cloud.max_object_bytes);
    let file = calls
        .cloud_download_native_file(
            &version,
            descriptor.key,
            &[],
            tinytree_model::FileId(descriptor.key.id.0),
            directory.path(),
        )
        .await
        .unwrap();
    assert_eq!(file.descriptor(), descriptor);
    let snapshot_file = calls
        .cloud_download_snapshot_file(
            &version,
            &selected_roots,
            terminal_commit,
            &[descriptor.key],
            tinytree_model::FileId(descriptor.key.id.0),
            directory.path(),
        )
        .await
        .unwrap();
    assert_eq!(snapshot_file.descriptor(), descriptor);
    assert_eq!(faults.native_snapshot_calls.load(Ordering::SeqCst), 2);
    drop(calls);
    assert_released(&physical_drops).await;
    assert!(
        physical_weak.upgrade().is_none(),
        "retained native results must not capture a physical inflight/RPC owner"
    );
    // Both result allocations remain held here, but max_inflight is one.
    let followup = client
        .cloud_retained_version_selected(selector, selected)
        .await
        .unwrap();
    assert_eq!(followup.selector(), selector);
    drop(followup);
    let mut reader = file.open().unwrap();
    drop(file);
    drop(version);
    drop(metadata);
    drop(snapshot_metadata);
    drop(snapshot_file);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "verified file reader retains version/caller owners without an inflight slot"
    );
    let mut copy = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut copy).unwrap();
    assert_eq!(copy, filebytes);
    drop(reader);
    assert_released(&drops).await;
    for mode in [32, 33, 34] {
        faults.mode.store(mode, Ordering::SeqCst);
        let version = client
            .cloud_retained_version_selected(selector, selected)
            .await
            .unwrap();
        assert_eq!(
            client
                .cloud_read_native_metadata(&version, key, &[], key)
                .await
                .err()
                .unwrap()
                .code(),
            Code::DataLoss
        );
        assert_eq!(
            client
                .cloud_read_snapshot_metadata(
                    &version,
                    &selected_roots,
                    terminal_commit,
                    &[key],
                    key
                )
                .await
                .err()
                .unwrap()
                .code(),
            Code::DataLoss
        );
    }
    faults.mode.store(36, Ordering::SeqCst);
    let unavailable = client
        .cloud_retained_version_selected(selector, selected)
        .await
        .unwrap();
    let before = faults.calls.load(Ordering::SeqCst);
    assert_eq!(
        client
            .cloud_read_snapshot_metadata(
                &unavailable,
                &selected_roots,
                terminal_commit,
                &[key],
                key
            )
            .await
            .err()
            .unwrap()
            .code(),
        Code::Unimplemented
    );
    assert_eq!(
        faults.calls.load(Ordering::SeqCst),
        before + 1,
        "no generic or VersionRoot fallback"
    );
    drop(unavailable);
    faults.mode.store(35, Ordering::SeqCst);
    assert_eq!(
        client
            .cloud_retained_version_selected(selector, selected)
            .await
            .err()
            .unwrap()
            .code(),
        Code::Unimplemented
    );
    faults.mode.store(30, Ordering::SeqCst);
    let mut wrong = selected;
    wrong.blake3[0] ^= 1;
    assert_eq!(
        client
            .cloud_retained_version_selected(selector, wrong)
            .await
            .err()
            .unwrap()
            .code(),
        Code::DataLoss
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn cloud_scope_sdk_checks_source_echo_and_forks_without_wire_root_grants() {
    use tinytree_model::CommitId;
    let (_store, _index, service, _, _) = setup_query();
    let running = QueryRunning::start(service, QueryMode::CloudRetry).await;
    let client = running.client().await;
    let version = client.cloud_retained_version([8; 32], 1).await.unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
    let scope = guarded
        .open_cloud_query_scope(
            &version,
            vec![[2; 32], [1; 32]],
            vec![CommitId::ROOT],
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        running.opens.load(Ordering::SeqCst),
        2,
        "lost ACK reuses immutable cloud intent"
    );
    assert_eq!(scope.query_identity_version(), 2);
    assert_eq!(scope.remote_roots(), &[CommitId::ROOT]);
    let fork = guarded.fork_query_scope(&scope, Vec::new()).await.unwrap();
    assert_ne!(scope.handle(), fork.handle());
    assert_eq!(scope.index_epoch(), fork.index_epoch());
    assert_eq!(scope.remote_roots(), fork.remote_roots());
    assert!(fork.expires_at_unix_seconds() <= scope.expires_at_unix_seconds());
    drop(guarded);
    assert_released(&drops).await;
    // A held checked scope/fork cannot retain the physical request guard.
    let foreign = running.client().await;
    let before = running.opens.load(Ordering::SeqCst);
    let counters = || {
        [
            running.counts.forks.load(Ordering::SeqCst),
            running.counts.facts.load(Ordering::SeqCst),
            running.counts.commit_prefixes.load(Ordering::SeqCst),
            running.counts.evaluations.load(Ordering::SeqCst),
            running.counts.closes.load(Ordering::SeqCst),
        ]
    };
    let before_foreign = counters();
    let expression = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
            commits: vec![q::CommitId {
                value: CommitId::ROOT.0.to_vec(),
            }],
        })),
    };
    for retained_scope in [&scope, &fork] {
        assert_eq!(
            foreign
                .fork_query_scope(retained_scope, Vec::new())
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            foreign
                .query_facts(retained_scope, vec![QueryFact::HasId(CommitId::ROOT)])
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            foreign
                .resolve_commit_prefix(retained_scope, "0")
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            foreign
                .begin_query(retained_scope, &expression, 1)
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            foreign
                .close_query_scope(retained_scope)
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
    }
    assert_eq!(
        counters(),
        before_foreign,
        "foreign client rejects captured cloud scopes before transport"
    );
    drop((scope, fork));
    let ordinary = client.open_query_scope(vec![CommitId::ROOT]).await.unwrap();
    assert_eq!(
        foreign
            .query_facts(&ordinary, vec![QueryFact::HasId(CommitId::ROOT)])
            .await
            .unwrap(),
        vec![FactAnswer::HasId(true)],
        "ordinary handle compatibility is unchanged"
    );
    // Ordinary OpenScope above adds exactly one unrelated request.
    let before = before + 1;
    assert_eq!(
        foreign
            .open_cloud_query_scope(&version, vec![[1; 32]], vec![CommitId::ROOT], Vec::new())
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        client
            .open_cloud_query_scope(
                &version,
                vec![[1; 32]; running.config.limits.max_batch_items + 1],
                vec![CommitId::ROOT],
                Vec::new()
            )
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        client
            .open_cloud_query_scope(
                &version,
                vec![[1; 32], [1; 32]],
                vec![CommitId::ROOT],
                Vec::new()
            )
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        running.opens.load(Ordering::SeqCst),
        before,
        "borrowed preflight rejects before any RPC"
    );
    tokio::time::timeout(Duration::from_secs(5), running.stop())
        .await
        .expect("cloud scope fixture did not shut down");
}

#[tokio::test]
async fn cloud_scope_sdk_never_accepts_missing_wrong_or_legacy_source_echoes() {
    use tinytree_model::CommitId;
    for (mode, anchors) in [
        (QueryMode::CloudMissingBinding, vec![CommitId::ROOT]),
        (QueryMode::CloudWrongBinding, vec![CommitId::ROOT]),
        (QueryMode::CloudBindingWidth, vec![CommitId::ROOT]),
        (QueryMode::CloudLegacyVersion, vec![CommitId::ROOT]),
        (QueryMode::Normal, vec![CommitId([1; 32])]),
    ] {
        let (_store, _index, service, _, _) = setup_query();
        let running = QueryRunning::start(service, mode).await;
        let client = running.client().await;
        let version = client.cloud_retained_version([8; 32], 1).await.unwrap();
        let error = client
            .open_cloud_query_scope(&version, vec![[1; 32]], anchors, Vec::new())
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::DataLoss, "{mode:?}");
        assert_eq!(
            running.opens.load(Ordering::SeqCst),
            1,
            "never ordinary-scope fallback"
        );
        tokio::time::timeout(Duration::from_secs(5), running.stop())
            .await
            .expect("cloud scope fixture did not shut down");
    }
}

#[tokio::test]
async fn cancelled_cloud_scope_releases_transport_admission_after_physical_response_drains() {
    let (_store, _index, service, _, _) = setup_query();
    let running = QueryRunning::start(service, QueryMode::CloudPause).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let version = client.cloud_retained_version([8; 32], 1).await.unwrap();
    let first = client.clone();
    let mut task = tokio::spawn(async move {
        first
            .open_cloud_query_scope(
                &version,
                vec![[1; 32]],
                vec![tinytree_model::CommitId::ROOT],
                Vec::new(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = running.counts.fork_entered.notified() => {},
            result = &mut task => panic!("cloud scope completed before reaching the accepted-response pause: {result:?}"),
        }
    }).await.expect("cloud scope did not reach the accepted-response pause");
    task.abort();
    let _ = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("cancelled cloud scope task did not stop");
    running.counts.fork_release.notify_one();
    // Same client and sole slot: completion cannot depend on dropping an
    // unrelated retained scope, increasing a pool, or timeout changes.
    let version = client.cloud_retained_version([8; 32], 1).await.unwrap();
    let second = client.clone();
    let mut task = tokio::spawn(async move {
        second
            .open_cloud_query_scope(
                &version,
                vec![[1; 32]],
                vec![tinytree_model::CommitId::ROOT],
                Vec::new(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            _ = running.counts.fork_entered.notified() => {},
            result = &mut task => panic!("cloud scope completed before reaching the accepted-response pause: {result:?}"),
        }
    }).await.expect("cloud scope did not reach the accepted-response pause");
    running.counts.fork_release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("next cloud scope did not recover the sole transport slot")
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), running.stop())
        .await
        .expect("cloud scope fixture did not shut down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn captured_version_transport_checks_client_footer_stream_and_original_publish_intent() {
    use tinytree_client::cloud_protocol::workspace_cloud_server::WorkspaceCloudServer;
    use tinytree_client::{
        CloudCapturedPublishIntent, CloudManifestId, CloudPublishIntent, CloudRawDigest,
        RetainedVersionSelector,
    };
    let faults = CloudFaults::default();
    faults.mode.store(40, Ordering::SeqCst);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = async_stream::stream! {loop{match listener.accept().await{Ok((stream,_))=>yield Ok::<_,std::io::Error>(stream),Err(error)=>{yield Err(error);break}}}};
    let service = WorkspaceCloudServer::new(faults.clone())
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd);
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .unwrap()
    });
    let mut config = Config::default();
    config.endpoint = format!("http://{address}");
    config.limits.max_inflight = 1;
    config.rpc.retry.max_attempts = 1;
    assert!(!toml::to_string(&config).unwrap().contains("[cloud]"));
    let mut existing_custom = config.clone();
    existing_custom.cloud.max_object_bytes /= 2;
    assert!(
        !toml::to_string(&existing_custom)
            .unwrap()
            .contains("max_capture_bytes"),
        "adding the default capture bound does not rotate existing custom cloud configuration identity"
    );
    let client = NativeClient::connect(config.clone()).await.unwrap();
    let (data, manifest) = capture_transport_fixture(40);
    let selector = RetainedVersionSelector {
        workspace_id: [1; 32],
        generation: 1,
        manifest_id: CloudManifestId::of(&manifest),
    };
    let selected = CloudRawDigest::of(&manifest);
    let retained = client
        .cloud_retained_version_selected(selector, selected)
        .await
        .unwrap();
    let proof = client.cloud_captured_version(retained).unwrap();
    let capture = client.cloud_read_capture(&proof).await.unwrap();
    assert_eq!(capture.payload(), data);
    assert_eq!(capture.descriptor(), proof.descriptor());
    client.cloud_workspace([1; 32]).await.unwrap(); // Retained bytes never pin max_inflight=1.
    drop(capture);
    let foreign = NativeClient::connect(config.clone()).await.unwrap();
    let before = faults.calls.load(Ordering::SeqCst);
    assert_eq!(
        foreign.cloud_read_capture(&proof).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        faults.calls.load(Ordering::SeqCst),
        before,
        "foreign client proof rejected before RPC"
    );
    for (mode, code) in [
        (41, Code::DataLoss),
        (42, Code::DataLoss),
        (43, Code::DataLoss),
        (44, Code::DataLoss),
        (45, Code::DataLoss),
        (46, Code::DataLoss),
        (48, Code::DataLoss),
        (49, Code::DataLoss),
        (50, Code::ResourceExhausted),
        (59, Code::DataLoss),
        (60, Code::DataLoss),
    ] {
        faults.mode.store(mode, Ordering::SeqCst);
        assert_eq!(
            client.cloud_read_capture(&proof).await.unwrap_err().code(),
            code,
            "capture stream mode {mode}"
        );
    }
    faults.mode.store(51, Ordering::SeqCst);
    assert_eq!(
        client.cloud_read_capture(&proof).await.unwrap().payload(),
        data
    );
    drop(proof);
    for (mode, code) in [(53, Code::Unimplemented), (54, Code::DataLoss)] {
        faults.mode.store(mode, Ordering::SeqCst);
        let manifest = capture_transport_fixture(mode).1;
        let selector = RetainedVersionSelector {
            manifest_id: CloudManifestId::of(&manifest),
            ..selector
        };
        let retained = client
            .cloud_retained_version_selected(selector, CloudRawDigest::of(&manifest))
            .await
            .unwrap();
        assert_eq!(
            client.cloud_captured_version(retained).unwrap_err().code(),
            code
        );
    }
    faults.mode.store(58, Ordering::SeqCst);
    let wrong_domain = capture_transport_fixture(58).1;
    let retained = client
        .cloud_retained_version_selected(
            RetainedVersionSelector {
                manifest_id: CloudManifestId::of(&wrong_domain),
                ..selector
            },
            CloudRawDigest::of(&wrong_domain),
        )
        .await
        .unwrap();
    let proof = client.cloud_captured_version(retained).unwrap();
    assert_eq!(
        client.cloud_read_capture(&proof).await.unwrap_err().code(),
        Code::DataLoss,
        "raw digest alone cannot substitute for capture domain ID"
    );
    drop(proof);
    faults.mode.store(40, Ordering::SeqCst);
    let physical = Arc::new(AtomicUsize::new(0));
    let persistent = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(persistent.clone())))
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(physical.clone())));
    let retained = guarded
        .cloud_retained_version_selected(selector, selected)
        .await
        .unwrap();
    let proof = guarded.cloud_captured_version(retained).unwrap();
    let capture = guarded.cloud_read_capture(&proof).await.unwrap();
    drop(guarded);
    assert_released(&physical).await;
    assert_eq!(persistent.load(Ordering::SeqCst), 0);
    client.cloud_workspace([1; 32]).await.unwrap();
    drop(capture);
    drop(proof);
    assert_released(&persistent).await;
    // A canceled partial transfer must release physical admission, while an
    // independent retained-version proof never owns max_inflight's call slot.
    let retained = client
        .cloud_retained_version_selected(selector, selected)
        .await
        .unwrap();
    let proof = Arc::new(client.cloud_captured_version(retained).unwrap());
    faults.mode.store(47, Ordering::SeqCst);
    let canceled_physical = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(canceled_physical.clone())));
    let read = tokio::spawn(async move { guarded.cloud_read_capture(&proof).await });
    tokio::time::timeout(Duration::from_secs(1), faults.index_entered.notified())
        .await
        .unwrap();
    read.abort();
    assert!(read.await.unwrap_err().is_cancelled());
    faults.index_release.notify_one();
    assert_released(&canceled_physical).await;
    client.cloud_workspace([1; 32]).await.unwrap();
    let intent = CloudPublishIntent {
        workspace_id: [1; 32],
        expected_generation: 0,
        idempotency_key: "captured-original".into(),
        manifest: manifest.clone(),
        staging_leases: vec![],
    };
    let before = faults.calls.load(Ordering::SeqCst);
    assert_eq!(
        client
            .cloud_publish_version(&intent)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(faults.calls.load(Ordering::SeqCst), before);
    let intent = CloudCapturedPublishIntent::new(intent, data.clone()).unwrap();
    faults.mode.store(55, Ordering::SeqCst);
    let publication = client
        .cloud_publish_captured_version(&intent)
        .await
        .unwrap();
    assert_eq!(publication.idempotency_key, "captured-original");
    assert_eq!(
        publication.workspace.manifest_id,
        Some(selector.manifest_id)
    );
    assert_eq!(intent.capture_data(), data);
    config.cloud.max_capture_bytes = 1;
    let small = NativeClient::connect(config).await.unwrap();
    let before = faults.calls.load(Ordering::SeqCst);
    assert_eq!(
        small
            .cloud_publish_captured_version(&intent)
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert_eq!(faults.calls.load(Ordering::SeqCst), before);
    server.abort();
    let _ = server.await;
}
