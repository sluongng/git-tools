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
    RetryOpen,
    MalformedScope,
    MissingFact,
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
}
#[derive(Default)]
struct QueryCounts {
    opens: Arc<AtomicUsize>,
    closes: AtomicUsize,
    facts: AtomicUsize,
    commit_prefixes: AtomicUsize,
    change_prefixes: AtomicUsize,
    shortest_prefixes: AtomicUsize,
}
#[derive(Clone)]
struct QueryFaults {
    service: tinytree_server::QueryService,
    mode: QueryMode,
    counts: Arc<QueryCounts>,
    first_scope: Arc<std::sync::Mutex<Option<q::QueryScope>>>,
}
#[tonic::async_trait]
impl RepositoryQuery for QueryFaults {
    async fn open_scope(
        &self,
        request: Request<Streaming<q::OpenScopeFrame>>,
    ) -> Result<Response<q::QueryScope>, Status> {
        let mut reply = self.service.open_scope(request).await?;
        let attempt = self.counts.opens.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::RetryOpen {
            if attempt == 0 {
                *self.first_scope.lock().unwrap() = Some(reply.get_ref().clone());
                return Err(Status::unavailable("scope ACK withheld for replay test"));
            }
            if attempt == 1 && self.first_scope.lock().unwrap().as_ref() != Some(reply.get_ref()) {
                return Err(Status::data_loss("scope retry changed replay identity"));
            }
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
        self.service.fork_scope(request).await
    }
    async fn close_scope(
        &self,
        request: Request<q::CloseScopeRequest>,
    ) -> Result<Response<q::CloseScopeResponse>, Status> {
        self.counts.closes.fetch_add(1, Ordering::SeqCst);
        self.service.close_scope(request).await
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
        request: Request<q::ResolveCommitPrefixRequest>,
    ) -> Result<Response<q::CommitPrefixResult>, Status> {
        self.counts.commit_prefixes.fetch_add(1, Ordering::SeqCst);
        let mut reply = self.service.resolve_commit_prefix(request).await?;
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
        request: Request<q::ResolveChangePrefixRequest>,
    ) -> Result<Response<q::ChangePrefixResult>, Status> {
        self.counts.change_prefixes.fetch_add(1, Ordering::SeqCst);
        let mut reply = self.service.resolve_change_prefix(request).await?;
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
        request: Request<q::ShortestPrefixRequest>,
    ) -> Result<Response<q::ShortestPrefixResult>, Status> {
        self.counts.shortest_prefixes.fetch_add(1, Ordering::SeqCst);
        let mut reply = self.service.shortest_prefix(request).await?;
        if self.mode == QueryMode::WrongShortestKind {
            reply.get_mut().kind = q::PrefixKind::ChangePrefix as i32;
        }
        if self.mode == QueryMode::LongestOverflow {
            reply.get_mut().hex_length = 65;
        }
        Ok(reply)
    }
    async fn evaluate(
        &self,
        request: Request<q::EvaluateRequest>,
    ) -> Result<Response<q::EvaluatePage>, Status> {
        self.service.evaluate(request).await
    }
}
struct QueryRunning {
    config: Config,
    opens: Arc<AtomicUsize>,
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
            counts,
            first_scope: Arc::new(std::sync::Mutex::new(None)),
        };
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    RepositoryQueryServer::new(faults)
                        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                        .send_compressed(tonic::codec::CompressionEncoding::Zstd),
                )
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
fn setup_query() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    RepositoryService,
    tinytree_model::CommitId,
    tinytree_model::ChangeId,
) {
    let (store_dir, store, service) = setup();
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
    (
        store_dir,
        index_dir,
        service.with_query_index(index).unwrap(),
        root,
        change,
    )
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
