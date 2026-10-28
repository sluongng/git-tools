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
    metadata_release: Arc<tokio::sync::Notify>,
    graph: Arc<AtomicUsize>,
    canonical_bytes: Arc<AtomicUsize>,
    files: Arc<AtomicUsize>,
    queries: Arc<QueryCounts>,
    capabilities: Arc<AtomicUsize>,
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
        let metadata_release = Arc::new(tokio::sync::Notify::new());
        let graph = Arc::new(AtomicUsize::new(0));
        let canonical_bytes = Arc::new(AtomicUsize::new(0));
        let files = Arc::new(AtomicUsize::new(0));
        let queries = Arc::new(QueryCounts::default());
        let capabilities = Arc::new(AtomicUsize::new(0));
        let query_service = service.query_grpc_service_with_adapter(QueryFaults {
            service: service.query_service(),
            mode: if mode == Mode::CapabilitiesSlow {
                QueryMode::SlowAfterCapability
            } else {
                QueryMode::Normal
            },
            counts: queries.clone(),
            first_scope: Arc::new(std::sync::Mutex::new(None)),
            first_page: Arc::new(std::sync::Mutex::new(None)),
            first_search: Arc::new(std::sync::Mutex::new(None)),
            first_history_entry: Arc::new(std::sync::Mutex::new(None)),
            first_annotation: Arc::new(std::sync::Mutex::new(None)),
        });
        let faults = Faults {
            capabilities: capabilities.clone(),
            service,
            mode,
            reads: files.clone(),
            writes: Arc::new(AtomicUsize::new(0)),
            metadata: metadata.clone(),
            metadata_release: metadata_release.clone(),
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
            metadata_release,
            graph,
            canonical_bytes,
            files,
            queries,
            capabilities,
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
    CapabilitiesMissing,
    CapabilitiesDuplicate,
    CapabilitiesOrder,
    CapabilitiesUnknown,
    CapabilitiesSchema,
    CapabilitiesRepository,
    CapabilitiesReadiness,
    CapabilitiesAbsent,
    CapabilitiesLegacyFeature,
    CapabilitiesNoFileset,
    CapabilitiesDenied,
    CapabilitiesLimited,
    CapabilitiesTransport,
    CapabilitiesSlow,
    Normal,
    RetryFile,
    CorruptFile,
    TruncateFile,
    DelayFile,
    PartialMetadata,
    PausedMetadata,
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
#[tokio::test]
async fn capabilities_are_exact_checked_configuration_not_implicit_root_readiness_or_fallback() {
    use tinytree_client::{CapabilityFeature as F, RootReadiness};
    for (mode, expected) in [
        (Mode::Normal, None),
        (Mode::CapabilitiesMissing, Some(Code::DataLoss)),
        (Mode::CapabilitiesDuplicate, Some(Code::DataLoss)),
        (Mode::CapabilitiesOrder, Some(Code::DataLoss)),
        (Mode::CapabilitiesUnknown, Some(Code::DataLoss)),
        (Mode::CapabilitiesSchema, Some(Code::DataLoss)),
        (Mode::CapabilitiesRepository, Some(Code::DataLoss)),
        (Mode::CapabilitiesReadiness, Some(Code::DataLoss)),
        (Mode::CapabilitiesAbsent, Some(Code::Unimplemented)),
    ] {
        let (_directory, _store, service) = setup();
        let running = Running::start(service, mode).await;
        let client = running.client().await;
        let result = client.capabilities(&F::ALL).await;
        if let Some(code) = expected {
            assert_eq!(result.unwrap_err().code(), code);
        } else {
            let configured = result.unwrap();
            assert_eq!(configured.root_readiness, RootReadiness::NotChecked);
            assert_eq!(configured.features.len(), 5);
            assert!(configured.features.iter().all(|state| !state.supported));
            assert!(
                configured.features[..3]
                    .iter()
                    .all(|state| state.authorized)
            );
            assert!(
                configured.features[3..]
                    .iter()
                    .all(|state| !state.authorized)
            );
            for requested in [
                vec![],
                vec![F::Blame, F::PathHistory],
                vec![F::Blame, F::Blame],
            ] {
                assert_eq!(
                    client.capabilities(&requested).await.unwrap_err().code(),
                    Code::InvalidArgument
                );
            }
            assert_eq!(
                client.capabilities(&[F::Blame]).await.unwrap().features[0].feature,
                F::Blame
            );
            // Landing recovery is requested only for those specific flows;
            // ordinary creation must retain its original five-feature set.
            assert_eq!(F::ALL.len(), 5);
            let mut every_known_feature = F::ALL.to_vec();
            every_known_feature.extend([
                F::LandingRecoveryCloudRead,
                F::LandingRecoveryCloudWrite,
                F::ExactPathGraphV1,
                F::JjResolvedBlameV1,
            ]);
            for requested in [
                vec![F::LandingRecoveryCloudRead],
                vec![F::LandingRecoveryCloudWrite],
                vec![F::ExactPathGraphV1],
                vec![F::JjResolvedBlameV1],
                vec![F::DurableRetainedOffloadV1],
                every_known_feature,
            ] {
                let configured = client.capabilities(&requested).await.unwrap();
                assert_eq!(configured.root_readiness, RootReadiness::NotChecked);
                assert_eq!(
                    configured
                        .features
                        .iter()
                        .map(|v| v.feature)
                        .collect::<Vec<_>>(),
                    requested
                );
                assert!(configured.features.iter().all(|v| !v.supported));
                assert!(
                    configured
                        .features
                        .iter()
                        .filter(|v| matches!(
                            v.feature,
                            F::LandingRecoveryCloudRead | F::LandingRecoveryCloudWrite
                        ))
                        .all(|v| !v.authorized)
                );
            }
        }
        assert_eq!(running.metadata.load(Ordering::SeqCst), 0);
        assert_eq!(running.graph.load(Ordering::SeqCst), 0);
        assert_eq!(running.files.load(Ordering::SeqCst), 0);
        running.stop().await;
    }
}
#[tokio::test]
async fn additive_singleton_negotiation_distinguishes_legacy_from_denial_limits_transport_and_corruption()
 {
    for (mode, legacy, error) in [
        (Mode::Normal, false, None),
        (Mode::CapabilitiesAbsent, true, None),
        (Mode::CapabilitiesLegacyFeature, true, None),
        (
            Mode::CapabilitiesDenied,
            false,
            Some(Code::PermissionDenied),
        ),
        (
            Mode::CapabilitiesLimited,
            false,
            Some(Code::ResourceExhausted),
        ),
        (Mode::CapabilitiesTransport, false, Some(Code::Unavailable)),
        (Mode::CapabilitiesMissing, false, Some(Code::DataLoss)),
        (Mode::CapabilitiesUnknown, false, Some(Code::DataLoss)),
        (Mode::CapabilitiesSchema, false, Some(Code::DataLoss)),
        (Mode::CapabilitiesReadiness, false, Some(Code::DataLoss)),
    ] {
        let (_directory, _store, service) = setup();
        let running = Running::start(service, mode).await;
        let client = running.client().await;
        for (expected, result) in [
            (
                tinytree_client::CapabilityFeature::JjResolvedBlameV1,
                client.resolved_blame_capability().await,
            ),
            (
                tinytree_client::CapabilityFeature::DurableRetainedOffloadV1,
                client.durable_retained_offload_capability().await,
            ),
        ] {
            if let Some(error) = error {
                assert_eq!(result.unwrap_err().code(), error);
            } else if legacy {
                assert!(result.unwrap().is_none());
            } else {
                let feature = result.unwrap().unwrap();
                assert_eq!(feature.feature, expected);
                assert!(!feature.supported);
                assert_eq!(
                    feature.authorized,
                    expected == tinytree_client::CapabilityFeature::JjResolvedBlameV1
                );
            }
        }
        assert_eq!(running.metadata.load(Ordering::SeqCst), 0);
        assert_eq!(running.graph.load(Ordering::SeqCst), 0);
        assert_eq!(running.files.load(Ordering::SeqCst), 0);
        running.stop().await;
    }
}

#[derive(Clone)]
struct Faults {
    capabilities: Arc<AtomicUsize>,
    service: RepositoryService,
    mode: Mode,
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    metadata: Arc<AtomicUsize>,
    metadata_release: Arc<tokio::sync::Notify>,
    graph: Arc<AtomicUsize>,
    canonical_bytes: Arc<AtomicUsize>,
    publications: Arc<AtomicUsize>,
}
type Stream<T> = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<T, Status>> + Send>>;
#[tonic::async_trait]
impl Repository for Faults {
    async fn get_capabilities(
        &self,
        request: Request<p::GetCapabilitiesRequest>,
    ) -> Result<Response<p::Capabilities>, Status> {
        self.capabilities.fetch_add(1, Ordering::SeqCst);
        if self.mode == Mode::CapabilitiesNoFileset && request.get_ref().features.contains(&18) {
            return Err(Status::invalid_argument("fileset capability is absent"));
        }
        if self.mode == Mode::CapabilitiesSlow {
            tokio::time::sleep(Duration::from_millis(70)).await;
        }
        if self.mode == Mode::CapabilitiesAbsent {
            return Err(Status::unimplemented(
                "capability negotiation is not installed",
            ));
        }
        if self.mode == Mode::CapabilitiesLegacyFeature
            && matches!(request.get_ref().features.as_slice(), [11] | [13] | [15])
        {
            return Err(Status::invalid_argument(
                "legacy capability vocabulary ends at10",
            ));
        }
        if self.mode == Mode::CapabilitiesDenied {
            return Err(Status::permission_denied("capability denied"));
        }
        if self.mode == Mode::CapabilitiesLimited {
            return Err(Status::resource_exhausted("capability busy"));
        }
        if self.mode == Mode::CapabilitiesTransport {
            return Err(Status::unavailable("capability transport unavailable"));
        }
        let mut response = self.service.get_capabilities(request).await?;
        let value = response.get_mut();
        match self.mode {
            Mode::CapabilitiesMissing => {
                value.features.pop();
            }
            Mode::CapabilitiesDuplicate => {
                value.features[1] = value.features[0].clone();
            }
            Mode::CapabilitiesOrder => {
                value.features.swap(0, 1);
            }
            Mode::CapabilitiesUnknown => {
                value.features[0].feature = 99;
            }
            Mode::CapabilitiesSchema => {
                value.schema_version = 2;
            }
            Mode::CapabilitiesRepository => {
                value.repository_id = "other".into();
            }
            Mode::CapabilitiesReadiness => {
                value.root_readiness = 0;
            }
            _ => (),
        }
        Ok(response)
    }

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
        if self.mode == Mode::PausedMetadata {
            self.metadata_release.notified().await;
        }
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
async fn transient_absolute_deadline_survives_phases_guard_clones_and_rebinding() {
    let (_dir, _store, service) = setup();
    let running = Running::start(service, Mode::Normal).await;
    let mut config = running.config();
    config.rpc.operation_timeout_ms = 5000;
    let client = NativeClient::connect(config).await.unwrap();
    let bound = tokio::time::Instant::now() + Duration::from_millis(500);
    let bounded = client.clone().with_operation_deadline(bound);
    let key = tinytree_model::empty_tree().descriptor.key;
    bounded.read_objects(vec![key]).await.unwrap();
    let before = running.metadata.load(Ordering::SeqCst);
    // Simulate another phase consuming the ORIGINAL remaining operation budget.
    tokio::time::sleep_until(bound).await;
    let bounded = bounded
        .with_lifetime_guard(Arc::new(()))
        .with_rpc_lifetime_guard(Arc::new(()))
        .with_operation_deadline(tokio::time::Instant::now() + Duration::from_secs(5));
    assert_eq!(
        bounded
            .read_objects(vec![key])
            .await
            .err()
            .expect("expired clone must fail")
            .code(),
        Code::DeadlineExceeded
    );
    assert_eq!(
        running.metadata.load(Ordering::SeqCst),
        before,
        "expired clone must fail before RPC, without a renewed retry budget"
    );
    // Binding is per-clone, not Inner or config: independent work still runs.
    client.read_objects(vec![key]).await.unwrap();
    assert_eq!(running.metadata.load(Ordering::SeqCst), before + 1);
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
    AnnotationWrongDigest,
    AnnotationMissingFlag,
    AnnotationCyclicParent,
    AnnotationInputHash,
    AnnotationPageCursorEcho,
    AnnotationPageSequence,
    AnnotationPageCounters,
    AnnotationPagePath,
    AnnotationPageClosure,
    AnnotationPageSlow,
    AnnotationPagePendingNode,
    AnnotationPageStalled,
    AnnotationPageDuplicateNode,
    AnnotationPageMissingParent,
    AnnotationPageScannedCount,
    AnnotationPageExpired,
    AnnotationPageShortExpiry,
    AnnotationAttributionHash,
    AnnotationAttributionGraph,
    AnnotationCursorFlood,
    HistoryWrongDigest,
    HistoryWrongRevision,
    HistoryWrongPath,
    HistoryWrongPolicy,
    HistoryInvalidCoverage,
    HistoryFalseComplete,
    HistoryMalformedState,
    HistoryDuplicateEntry,
    HistoryZeroProgress,
    HistoryRepeatedFrontier,
    HistoryPreviousEntry,
    HistorySecondWrongRevision,
    HistoryTerminalReadLimit,
    HistoryCloseAfterPage,
    HistorySlowPages,
    BlameWrongHash,
    BlameGap,
    BlameUnresolvedComplete,
    BlameOriginWidth,
    BlameOriginOverflow,
    BlamePartialReply,
    BlameUnavailableTarget,
    SearchRetry,
    SearchWrongDigest,
    SearchRootWidth,
    SearchMissingManifest,
    SearchWrongPattern,
    SearchWrongPrefix,
    SearchWrongPolicy,
    SearchFalseComplete,
    SearchCorruptSnippet,
    SearchZeroWidth,
    SearchMissingMatchSpan,
    SearchLongMatch,
    SearchDuplicateHit,
    SearchOutsidePrefix,
    SearchInvalidLine,
    SearchOverflow,
    SearchLargePath,
    SearchChangedManifest,
    SearchChangedFile,
    SearchRegressedHit,
    SearchRegressedProgress,
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
    SmallUncompressed,
    FactOversizedReply,
    FactAllocationFlood,
    FactWrongVariantEmptyIds,
    ShortestOversizedReply,
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
    EvaluateAdmissionRefused,
    SlowAfterCapability,
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
    histories: AtomicUsize,
    annotation_pages: AtomicUsize,
    annotation_operations: AtomicUsize,
    annotation_nodes: AtomicUsize,
    blames: AtomicUsize,
    searches: AtomicUsize,
    search_ready: AtomicUsize,
    commit_prefixes: AtomicUsize,
    change_prefixes: AtomicUsize,
    shortest_prefixes: AtomicUsize,
    evaluations: AtomicUsize,
    work_classes: std::sync::Mutex<Vec<(&'static str, Option<String>, bool)>>,
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
    first_search: Arc<std::sync::Mutex<Option<q::SearchResult>>>,
    first_history_entry: Arc<std::sync::Mutex<Option<q::PathHistoryEntry>>>,
    first_annotation: Arc<std::sync::Mutex<Option<q::AnnotationFrontierPage>>>,
}
impl QueryFaults {
    fn record_work_class<T>(&self, method: &'static str, request: &Request<T>, continuation: bool) {
        let class = request
            .metadata()
            .get(tinytree_proto::QUERY_WORK_CLASS_HEADER)
            .map(|value| value.to_str().unwrap().to_owned());
        self.counts
            .work_classes
            .lock()
            .unwrap()
            .push((method, class, continuation));
    }
    fn annotation_page(&self, page: &mut q::AnnotationFrontierPage) {
        self.counts.annotation_nodes.fetch_add(
            page.frontier
                .as_ref()
                .map_or(0, |frontier| frontier.nodes.len()),
            Ordering::SeqCst,
        );
        if self.mode == QueryMode::AnnotationAttributionHash {
            if let Some(blame) = page.blame.as_mut() {
                if let Some(byte) = blame.target_bytes.first_mut() {
                    *byte ^= 1;
                }
            }
        }
        if self.mode == QueryMode::AnnotationAttributionGraph {
            page.frontier
                .as_mut()
                .unwrap()
                .nodes
                .push(q::AnnotationNode {
                    revision: Some(q::CommitId { value: vec![0; 32] }),
                    parents: vec![],
                    source_layer: 0,
                    topological_key: 0,
                    value: Some(q::AnnotationPathValue {
                        state: Some(q::HistoryPathState {
                            kind: 1,
                            native_id: vec![],
                            size_bytes: 0,
                            executable: false,
                        }),
                        copy_id: vec![],
                    }),
                    affects_path: Some(false),
                });
        }
    }
}
#[tonic::async_trait]
impl RepositoryQuery for QueryFaults {
    async fn open_annotation_frontier(
        &self,
        request: Request<q::OpenAnnotationFrontierRequest>,
    ) -> Result<Response<q::AnnotationFrontier>, Status> {
        let mut reply = self.service.open_annotation_frontier(request).await?;
        let result = reply.get_mut();
        match self.mode {
            QueryMode::AnnotationWrongDigest => result.scope_digest[0] ^= 1,
            QueryMode::AnnotationMissingFlag => {
                result.nodes.last_mut().unwrap().affects_path = None
            }
            QueryMode::AnnotationCyclicParent => {
                let node = result.nodes.last_mut().unwrap();
                node.parents = vec![node.revision.clone().unwrap()];
            }
            _ => {}
        }
        Ok(reply)
    }

    async fn open_annotation_frontier_paged(
        &self,
        request: Request<q::OpenAnnotationFrontierPagedRequest>,
    ) -> Result<Response<q::AnnotationFrontierPage>, Status> {
        self.counts.annotation_pages.fetch_add(1, Ordering::SeqCst);
        let mut reply = self.service.open_annotation_frontier_paged(request).await?;
        self.annotation_page(reply.get_mut());
        if self.mode == QueryMode::AnnotationCursorFlood {
            let page = reply.get_mut();
            page.blame = None;
            page.has_more = true;
            page.phase = 1;
            page.sequence = 0;
            page.operations = 1;
            page.emitted_nodes = 0;
            let frontier = page.frontier.as_mut().unwrap();
            frontier.nodes.clear();
            frontier.coverage = 0;
            frontier.scanned_commits = 0;
            page.next_cursor = vec![1; 32];
            *self.first_annotation.lock().unwrap() = Some(page.clone());
        }
        self.counts
            .annotation_operations
            .store(reply.get_ref().operations as usize, Ordering::SeqCst);
        if matches!(
            self.mode,
            QueryMode::AnnotationPageExpired | QueryMode::AnnotationPageShortExpiry
        ) {
            let seconds = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            reply
                .get_mut()
                .frontier
                .as_mut()
                .unwrap()
                .expires_at_unix_seconds = seconds
                + if self.mode == QueryMode::AnnotationPageExpired {
                    -1
                } else {
                    2
                };
        }
        if self.mode == QueryMode::AnnotationPageClosure {
            let page = reply.get_mut();
            page.has_more = false;
            page.next_cursor.clear();
            page.phase = 4;
            page.frontier.as_mut().unwrap().coverage = q::HistoryCoverage::HistoryComplete as i32;
        }
        if self.mode == QueryMode::AnnotationPageSlow {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        Ok(reply)
    }
    async fn continue_annotation_frontier(
        &self,
        request: Request<q::ContinueAnnotationFrontierRequest>,
    ) -> Result<Response<q::AnnotationFrontierPage>, Status> {
        self.counts.annotation_pages.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::AnnotationCursorFlood {
            let mut page = self
                .first_annotation
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .clone();
            page.sequence = (self.counts.annotation_pages.load(Ordering::SeqCst) - 1) as u64;
            page.operations = page.sequence + 1;
            page.next_cursor = vec![0; 32];
            page.next_cursor[..8].copy_from_slice(&(page.sequence + 1).to_le_bytes());
            return Ok(Response::new(page));
        }
        let cursor = request.get_ref().cursor.clone();
        let mut reply = self.service.continue_annotation_frontier(request).await?;
        self.annotation_page(reply.get_mut());
        let page = reply.get_mut();
        let previous_operations = self
            .counts
            .annotation_operations
            .swap(page.operations as usize, Ordering::SeqCst);
        // At a byte boundary the server may have already read/charged the last
        // pending node. Its next page makes output progress without engine work.
        if self.mode == QueryMode::AnnotationPagePendingNode
            && !page.frontier.as_ref().unwrap().nodes.is_empty()
        {
            page.operations = previous_operations as u64;
            self.counts
                .annotation_operations
                .store(previous_operations, Ordering::SeqCst);
        }
        if self.mode == QueryMode::AnnotationPageStalled {
            page.operations = previous_operations as u64;
        }
        if let Some(node) = page
            .frontier
            .as_mut()
            .unwrap()
            .nodes
            .iter_mut()
            .find(|node| node.revision.as_ref().unwrap().value != [0; 32])
        {
            match self.mode {
                QueryMode::AnnotationPageDuplicateNode => {
                    node.revision.as_mut().unwrap().value = vec![0; 32]
                }
                QueryMode::AnnotationPageMissingParent => node.parents[0].value = vec![7; 32],
                _ => {}
            }
        }
        match self.mode {
            QueryMode::AnnotationPageCursorEcho if page.has_more => page.next_cursor = cursor,
            QueryMode::AnnotationPageSequence => page.sequence = 0,
            QueryMode::AnnotationPageScannedCount if !page.has_more => {
                page.frontier.as_mut().unwrap().scanned_commits += 1;
            }
            QueryMode::AnnotationPageCounters => page.usage.as_mut().unwrap().work_nodes = 0,
            QueryMode::AnnotationPagePath => {
                page.frontier.as_mut().unwrap().path.push_str("-changed")
            }
            _ => {}
        }
        if self.mode == QueryMode::AnnotationPageSlow {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        Ok(reply)
    }

    async fn read_annotation_input(
        &self,
        request: Request<q::ReadAnnotationInputRequest>,
    ) -> Result<Response<q::AnnotationInput>, Status> {
        let mut reply = self.service.read_annotation_input(request).await?;
        if self.mode == QueryMode::AnnotationInputHash {
            reply.get_mut().data[0] ^= 1;
        }
        Ok(reply)
    }

    async fn close_annotation_frontier(
        &self,
        request: Request<q::CloseAnnotationFrontierRequest>,
    ) -> Result<Response<q::CloseAnnotationFrontierResponse>, Status> {
        self.service.close_annotation_frontier(request).await
    }

    async fn search(
        &self,
        request: Request<q::SearchRequest>,
    ) -> Result<Response<q::SearchResult>, Status> {
        self.counts.searches.fetch_add(1, Ordering::SeqCst);
        let mut reply = self.service.search(request).await?;
        if reply.get_ref().coverage == q::SearchCoverage::SearchWarming as i32 {
            return Ok(reply);
        }
        let attempt = self.counts.search_ready.fetch_add(1, Ordering::SeqCst);
        let result = reply.get_mut();
        if self.mode == QueryMode::SearchRetry {
            if attempt == 0 {
                *self.first_search.lock().unwrap() = Some(result.clone());
                return Err(Status::unavailable(
                    "Search ACK withheld for exact page retry",
                ));
            }
            if attempt == 1 && self.first_search.lock().unwrap().as_ref() != Some(result) {
                return Err(Status::data_loss("Search retry changed immutable page"));
            }
        }
        match self.mode {
            QueryMode::SearchWrongDigest => result.scope_digest[0] ^= 1,
            QueryMode::SearchRootWidth => {
                result.root_tree.pop();
            }
            QueryMode::SearchMissingManifest => result.manifest_digest.clear(),
            QueryMode::SearchWrongPattern => result.pattern.push(b'!'),
            QueryMode::SearchWrongPrefix => result.path_prefix.push_str("/different"),
            QueryMode::SearchWrongPolicy => result.policy = 0,
            QueryMode::SearchFalseComplete => {
                result.coverage = q::SearchCoverage::SearchComplete as i32;
                result.cursor = vec![1];
            }
            QueryMode::SearchCorruptSnippet => result.hits[0].snippet.fill(b'!'),
            QueryMode::SearchZeroWidth => result.hits[0].match_bytes = 0,
            QueryMode::SearchMissingMatchSpan => result.hits[0].snippet.clear(),
            QueryMode::SearchLongMatch => {
                let hit = &mut result.hits[0];
                hit.file_size = 2048;
                hit.byte_offset = 0;
                hit.match_bytes = 1025;
                hit.line = 1;
                hit.byte_column = 0;
                hit.snippet_offset = 0;
                hit.snippet = vec![b'x'; 1025];
            }
            QueryMode::SearchDuplicateHit => {
                result.hits[1] = result.hits[0].clone();
            }
            QueryMode::SearchOutsidePrefix => result.hits[0].path = "different/file".into(),
            QueryMode::SearchInvalidLine => result.hits[0].line = 0,
            QueryMode::SearchOverflow => result.hits[0].byte_offset = u64::MAX,
            QueryMode::SearchLargePath => result.hits[0].path = "x".repeat(4097),
            QueryMode::SearchChangedManifest if attempt > 0 => result.manifest_digest[0] ^= 1,
            QueryMode::SearchChangedFile if attempt > 0 => result.hits[0].file_id[0] ^= 1,
            QueryMode::SearchRegressedProgress if attempt > 0 => {
                result.entries_examined = 1;
                result.file_bytes_read = 0;
            }
            QueryMode::SearchRegressedHit => {
                if attempt == 0 {
                    *self.first_search.lock().unwrap() = Some(result.clone());
                } else {
                    result.hits[0] =
                        self.first_search.lock().unwrap().as_ref().unwrap().hits[0].clone();
                }
            }
            _ => {}
        }
        Ok(reply)
    }
    async fn path_history(
        &self,
        request: Request<q::PathHistoryRequest>,
    ) -> Result<Response<q::PathHistoryResult>, Status> {
        let page = self.counts.histories.fetch_add(1, Ordering::SeqCst);
        let close_key = request.get_ref().scope.clone();
        let close_metadata = request.metadata().clone();
        if self.mode == QueryMode::HistorySlowPages {
            tokio::time::sleep(Duration::from_millis(80)).await;
        }
        let mut reply = self.service.path_history(request).await?;
        if self.mode == QueryMode::HistoryCloseAfterPage && page == 0 {
            let mut close = Request::new(q::CloseScopeRequest { scope: close_key });
            *close.metadata_mut() = close_metadata;
            self.service.close_scope(close).await?;
        }
        let result = reply.get_mut();
        match self.mode {
            QueryMode::HistoryZeroProgress => {
                result.entries.clear();
                result.scanned_commits = 0;
            }
            QueryMode::HistoryRepeatedFrontier => result.boundary = result.revision.clone(),
            QueryMode::HistoryPreviousEntry => {
                if let Some(entry) = result.entries.first_mut() {
                    let mut first = self.first_history_entry.lock().unwrap();
                    if let Some(previous) = first.as_ref() {
                        *entry = previous.clone();
                    } else {
                        *first = Some(entry.clone());
                    }
                }
            }
            QueryMode::HistorySecondWrongRevision if page > 0 => {
                result.revision.as_mut().unwrap().value[0] ^= 1;
            }
            QueryMode::HistoryTerminalReadLimit => {
                result.coverage = q::HistoryCoverage::HistoryReadLimit as i32;
            }
            QueryMode::HistoryWrongDigest => result.scope_digest[0] ^= 1,
            QueryMode::HistoryWrongRevision => result.revision.as_mut().unwrap().value[0] ^= 1,
            QueryMode::HistoryWrongPath => result.path.push_str(".different"),
            QueryMode::HistoryWrongPolicy => result.policy = 0,
            QueryMode::HistoryInvalidCoverage => result.coverage = 99,
            QueryMode::HistoryFalseComplete => result.boundary = result.revision.clone(),
            QueryMode::HistoryMalformedState => {
                result.entries[0].state.as_mut().unwrap().native_id.clear()
            }
            QueryMode::HistoryDuplicateEntry => {
                result.entries.push(result.entries[0].clone());
                result.scanned_commits += 1;
            }
            _ => {}
        }
        Ok(reply)
    }
    async fn blame(
        &self,
        request: Request<q::BlameRequest>,
    ) -> Result<Response<q::BlameResult>, Status> {
        self.counts.blames.fetch_add(1, Ordering::SeqCst);
        let mut reply = self.service.blame(request).await?;
        let result = reply.get_mut();
        match self.mode {
            QueryMode::BlameWrongHash => result.target_bytes[0] ^= 1,
            QueryMode::BlameGap => result.ranges[0].start_line += 1,
            QueryMode::BlameUnresolvedComplete => result.ranges[0].origin = None,
            QueryMode::BlameOriginWidth => {
                result.ranges[0].origin.as_mut().unwrap().value.pop();
            }
            QueryMode::BlameOriginOverflow => result.ranges[0].original_line = u32::MAX,
            QueryMode::BlamePartialReply => {
                result.coverage = q::HistoryCoverage::HistoryScanLimit as i32;
                result.boundary = result.revision.clone();
                for range in &mut result.ranges {
                    range.origin = None;
                    range.original_line = 0;
                }
            }
            QueryMode::BlameUnavailableTarget => {
                result.coverage = q::HistoryCoverage::HistoryReadLimit as i32;
                result.boundary = result.revision.clone();
                result.target = None;
                result.target_bytes.clear();
                result.ranges.clear();
            }
            _ => {}
        }
        Ok(reply)
    }
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
        self.record_work_class("query_facts", &request, false);
        if self.mode == QueryMode::SlowAfterCapability {
            tokio::time::sleep(Duration::from_millis(70)).await;
        }
        self.counts.facts.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::SmallUncompressed {
            assert!(request.metadata().get("grpc-accept-encoding").is_none());
            assert!(request.metadata().get("grpc-encoding").is_none());
        }
        let mut reply = self.service.query_facts(request).await?;
        let value = reply.get_mut();
        match self.mode {
            QueryMode::FactOversizedReply => value.scope_digest = vec![0; 2048],
            QueryMode::FactAllocationFlood => {
                value.answers = vec![
                    q::GraphFactAnswer {
                        answer: Some(q::graph_fact_answer::Answer::HasId(false)),
                    };
                    200
                ];
            }
            QueryMode::FactWrongVariantEmptyIds => {
                value.answers[0].answer = Some(q::graph_fact_answer::Answer::Heads(q::CommitSet {
                    commits: vec![q::CommitId::default(); 200],
                }));
            }
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
        self.record_work_class("resolve_commit_prefix", &request, false);
        if self.mode == QueryMode::SlowAfterCapability {
            tokio::time::sleep(Duration::from_millis(70)).await;
        }
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
        self.record_work_class("resolve_change_prefix", &request, false);
        if self.mode == QueryMode::SlowAfterCapability {
            tokio::time::sleep(Duration::from_millis(70)).await;
        }
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
        self.record_work_class("shortest_prefix", &request, false);
        if self.mode == QueryMode::SlowAfterCapability {
            tokio::time::sleep(Duration::from_millis(70)).await;
        }
        self.counts.shortest_prefixes.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::SmallUncompressed {
            assert!(request.metadata().get("grpc-accept-encoding").is_none());
            assert!(request.metadata().get("grpc-encoding").is_none());
        }
        if self.mode == QueryMode::PrefixSelectionIgnored {
            request.get_mut().selection = None;
        }
        let mut reply = self.service.shortest_prefix(request).await?;
        if self.mode == QueryMode::PrefixSelectionDigestWrong {
            reply.get_mut().selection_digest = Some(vec![9; 32]);
        }

        if self.mode == QueryMode::ShortestOversizedReply {
            reply.get_mut().selection_digest = Some(vec![0; 2048]);
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
        self.record_work_class("evaluate", &request, !request.get_ref().cursor.is_empty());
        if self.mode == QueryMode::SlowAfterCapability {
            tokio::time::sleep(Duration::from_millis(70)).await;
        }
        let attempt = self.counts.evaluations.fetch_add(1, Ordering::SeqCst);
        if self.mode == QueryMode::EvaluateAdmissionRefused {
            return Err(Status::deadline_exceeded("query admission expired"));
        }
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
        Self::start_with_transport(service, mode, false).await
    }
    /// Synthetic cloud replies test the client's untrusted wire/source-echo
    /// validation, not a configured CloudHost or its authorization/admission.
    /// Keep this explicit raw peer confined to those parser/lifetime tests.
    async fn start_cloud_wire_peer(service: RepositoryService, mode: QueryMode) -> Self {
        Self::start_with_transport(service, mode, true).await
    }
    async fn start_with_transport(
        service: RepositoryService,
        mode: QueryMode,
        raw_cloud_wire_peer: bool,
    ) -> Self {
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
            first_search: Arc::new(std::sync::Mutex::new(None)),
            first_history_entry: Arc::new(std::sync::Mutex::new(None)),
            first_annotation: Arc::new(std::sync::Mutex::new(None)),
        };
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let cloud = CloudFaults::default();
        cloud.mode.store(30, Ordering::SeqCst);
        let task = tokio::spawn(async move {
            let routes = tonic::service::Routes::new(
                RepositoryServer::new(service.clone())
                    .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                    .send_compressed(tonic::codec::CompressionEncoding::Zstd),
            )
            .add_service(
                tinytree_client::cloud_protocol::workspace_cloud_server::WorkspaceCloudServer::new(
                    cloud,
                )
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                .send_compressed(tonic::codec::CompressionEncoding::Zstd),
            );
            let routes = if raw_cloud_wire_peer {
                routes.add_service(
                    RepositoryQueryServer::new(faults)
                        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                        .send_compressed(tonic::codec::CompressionEncoding::Zstd),
                )
            } else {
                routes.add_service(service.query_grpc_service_with_adapter(faults))
            };
            tonic::transport::Server::builder()
                .add_routes(routes)
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
// Alternating file presence gives real changed entries; the newest unchanged
// commit deliberately contributes an empty page with positive scan progress.
fn setup_history_chain() -> QueryFixture {
    use tinytree_model as m;
    let mut config = ServiceConfig::default();
    config.query.max_history_commits = 1;
    let mut fixture = setup_query_configured(config);
    let (_, bytes) = fixture
        .store
        .read_metadata(
            "example",
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(fixture.root.0),
            },
        )
        .unwrap();
    let m::Object::Commit(mut model) = m::decode(&bytes, Default::default()).unwrap() else {
        unreachable!()
    };
    let original_tree = model.root_tree.clone();
    let empty = m::encode(
        &m::Object::Tree(m::Tree { entries: vec![] }),
        Default::default(),
    )
    .unwrap();
    fixture
        .store
        .put("example", empty.descriptor, &empty.bytes, 0)
        .unwrap();
    for number in 0..3 {
        model.parents = vec![fixture.root];
        model.predecessors.clear();
        model.description = format!("bounded history step {number}");
        model.root_tree = if number == 0 {
            m::TreeMerge {
                terms: vec![m::TreeId(empty.descriptor.key.id.0)],
                labels: vec![String::new()],
            }
        } else {
            original_tree.clone()
        };
        let object = m::encode(&m::Object::Commit(model.clone()), Default::default()).unwrap();
        fixture
            .store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
        fixture
            .index
            .ingest("example", object.descriptor, &object.bytes)
            .unwrap();
        fixture.root = m::CommitId(object.descriptor.key.id.0);
    }
    fixture
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
async fn exact_path_graph_requires_negotiated_support_without_eager_fallback() {
    for (policy_version, mode, unsupported) in [
        (1, Mode::Normal, false),
        (1, Mode::CapabilitiesAbsent, true),
        (1, Mode::CapabilitiesLegacyFeature, false),
        (2, Mode::Normal, false),
        (2, Mode::CapabilitiesAbsent, true),
        (2, Mode::CapabilitiesLegacyFeature, true),
        (3, Mode::Normal, false),
        (3, Mode::CapabilitiesAbsent, true),
        (3, Mode::CapabilitiesNoFileset, true),
    ] {
        let (_store, _index, service, root, _) = setup_query();
        let running = Running::start(service, mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![root]).await.unwrap();
        let expression = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::ChangedPath(Box::new(
                q::ChangedPathExpr {
                    matcher: (policy_version == 3).then(|| q::FilesetExpr {
                        expression: Some(q::fileset_expr::Expression::Literal(q::FilesetLiteral {
                            path: "file".into(),
                            exact_file: true,
                        })),
                    }),
                    policy_version,
                    candidates: Some(Box::new(ids_commits(&[root]))),
                    path: if policy_version == 3 {
                        String::new()
                    } else {
                        "file".into()
                    },
                    exact_file: policy_version != 3,
                },
            ))),
        };
        let result = client
            .begin_graph_query(&scope, &expression, 1, false)
            .await;
        if unsupported {
            assert_eq!(result.unwrap_err().code(), Code::Unimplemented);
        } else {
            let mut query = result.unwrap();
            client.close_graph_query(&mut query).await.unwrap();
        }
        assert_eq!(running.queries.evaluations.load(Ordering::SeqCst), 0);
        assert_eq!(running.metadata.load(Ordering::SeqCst), 0);
        assert_eq!(running.files.load(Ordering::SeqCst), 0);
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn changed_path_work_class_follows_policy_across_facts_prefixes_and_query_continuations() {
    for policy_version in [1, 2, 3] {
        let mut config = ServiceConfig::default();
        config.query.max_eval_quantum_steps = 1;
        let fixture = setup_query_configured(config);
        let root = fixture.root;
        let change = fixture.change;
        let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![root]).await.unwrap();
        let expression = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::ChangedPath(Box::new(
                q::ChangedPathExpr {
                    matcher: (policy_version == 3).then(|| q::FilesetExpr {
                        expression: Some(q::fileset_expr::Expression::Glob(q::FilesetGlob {
                            directory: String::new(),
                            pattern: "*.cc".into(),
                            case_insensitive: false,
                            prefix: false,
                        })),
                    }),
                    policy_version,
                    candidates: Some(Box::new(ids_ancestors(root))),
                    path: if policy_version == 3 {
                        String::new()
                    } else {
                        "hello.cc".into()
                    },
                    exact_file: policy_version != 3,
                },
            ))),
        };
        // Nested path predicates must select the same capability and work class
        // on every GraphExpr-bearing route, not only top-level ChangedPath.
        let expression = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Union(q::SetExpr {
                inputs: vec![expression],
            })),
        };
        client
            .query_facts(
                &scope,
                vec![QueryFact::Membership {
                    expression: expression.clone(),
                    candidate: root,
                }],
            )
            .await
            .unwrap();
        client
            .resolve_commit_prefix_selected(&scope, "", &expression)
            .await
            .unwrap();
        client
            .resolve_change_prefix_selected(&scope, "", vec![root], &expression)
            .await
            .unwrap();
        client
            .shortest_prefix_selected(
                &scope,
                PrefixTarget::Change(change),
                vec![root],
                &expression,
            )
            .await
            .unwrap();
        let mut ids = client.begin_query(&scope, &expression, 1).await.unwrap();
        while !ids.is_exhausted() {
            client.evaluate_page(&mut ids).await.unwrap();
        }
        client.close_ids_query(&mut ids).await.unwrap();
        let mut graph = client
            .begin_graph_query(&scope, &expression, 1, false)
            .await
            .unwrap();
        while !graph.is_exhausted() {
            client.evaluate_graph_page(&mut graph).await.unwrap();
        }
        client.close_graph_query(&mut graph).await.unwrap();
        {
            let classes = running.counts.work_classes.lock().unwrap();
            let expected =
                (policy_version >= 2).then_some(tinytree_proto::MERGED_PATH_V2_WORK_CLASS);
            for (method, class, _) in classes.iter() {
                assert_eq!(
                    class.as_deref(),
                    expected,
                    "policy {policy_version}: {method}"
                );
            }
            for method in [
                "query_facts",
                "resolve_commit_prefix",
                "resolve_change_prefix",
                "shortest_prefix",
                "evaluate",
            ] {
                assert!(
                    classes.iter().any(|(observed, _, _)| *observed == method),
                    "{method}"
                );
            }
            assert!(
                classes
                    .iter()
                    .any(|(method, _, continuation)| *method == "evaluate" && *continuation)
            );
        }
        // Ordinary graph expressions do not inherit a prior heavy query's class.
        running.counts.work_classes.lock().unwrap().clear();
        client
            .query_facts(
                &scope,
                vec![QueryFact::Membership {
                    expression: ids_commits(&[root]),
                    candidate: root,
                }],
            )
            .await
            .unwrap();
        assert!(
            running
                .counts
                .work_classes
                .lock()
                .unwrap()
                .iter()
                .all(|(_, class, _)| class.is_none())
        );
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn fileset_graph_validates_complete_fact_batches_before_capability_or_query_rpc() {
    use q::fileset_expr::Expression as F;
    let (_store, _index, service, root, _) = setup_query();
    let running = Running::start(service, Mode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let empty = || q::FilesetExpr {
        expression: Some(F::Empty(q::EmptyFileset {})),
    };
    let predicate = |policy_version, path: &str, exact_file, matcher| q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::ChangedPath(Box::new(
            q::ChangedPathExpr {
                policy_version,
                candidates: Some(Box::new(ids_commits(&[root]))),
                path: path.into(),
                exact_file,
                matcher,
            },
        ))),
    };
    let valid = predicate(3, "", false, Some(empty()));
    for (behavior, expression, expected) in [
        (
            "v1 cannot ignore a new matcher",
            predicate(1, "file", true, Some(empty())),
            Code::InvalidArgument,
        ),
        (
            "v2 cannot ignore a new matcher",
            predicate(2, "file", true, Some(empty())),
            Code::InvalidArgument,
        ),
        (
            "v3 requires matcher",
            predicate(3, "", false, None),
            Code::InvalidArgument,
        ),
        (
            "v3 rejects legacy path",
            predicate(3, "file", false, Some(empty())),
            Code::InvalidArgument,
        ),
        (
            "v3 rejects legacy exact flag",
            predicate(3, "", true, Some(empty())),
            Code::InvalidArgument,
        ),
        (
            "missing matcher node",
            predicate(3, "", false, Some(q::FilesetExpr::default())),
            Code::InvalidArgument,
        ),
        (
            "wide matcher exceeds fixed structural bound",
            predicate(
                3,
                "",
                false,
                Some(q::FilesetExpr {
                    expression: Some(F::Union(q::FilesetUnion {
                        inputs: vec![empty(); 64],
                    })),
                }),
            ),
            Code::ResourceExhausted,
        ),
    ] {
        let before = running.capabilities.load(Ordering::SeqCst);
        assert_eq!(
            client
                .query_facts(
                    &scope,
                    vec![
                        QueryFact::Membership {
                            expression: valid.clone(),
                            candidate: root
                        },
                        QueryFact::Membership {
                            expression,
                            candidate: root
                        },
                    ]
                )
                .await
                .unwrap_err()
                .code(),
            expected,
            "{behavior}"
        );
        assert_eq!(
            running.capabilities.load(Ordering::SeqCst),
            before,
            "{behavior}"
        );
        assert_eq!(
            running.queries.facts.load(Ordering::SeqCst),
            0,
            "{behavior}"
        );
    }
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn merged_path_admission_refusal_does_not_retry_or_downgrade_query() {
    let fixture = setup_query_retained();
    let running =
        QueryRunning::start(fixture.service.clone(), QueryMode::EvaluateAdmissionRefused).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let expression = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::ChangedPath(Box::new(
            q::ChangedPathExpr {
                matcher: None,
                policy_version: 2,
                candidates: Some(Box::new(ids_commits(&[fixture.root]))),
                path: "hello.cc".into(),
                exact_file: true,
            },
        ))),
    };
    let mut query = client.begin_query(&scope, &expression, 1).await.unwrap();
    assert_eq!(
        client.evaluate_page(&mut query).await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    assert_eq!(running.counts.evaluations.load(Ordering::SeqCst), 1);
    let classes = running.counts.work_classes.lock().unwrap().clone();
    assert_eq!(
        classes,
        vec![(
            "evaluate",
            Some(tinytree_proto::MERGED_PATH_V2_WORK_CLASS.to_owned()),
            false
        )]
    );
    client.close_ids_query(&mut query).await.unwrap();
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn merged_path_capability_and_query_share_one_deadline_and_facts_validate_before_rpc() {
    let (_store, _index, service, root, _) = setup_query();
    let running = Running::start(service, Mode::CapabilitiesSlow).await;
    let mut config = running.config();
    config.rpc.operation_timeout_ms = 100;
    config.rpc.rpc_timeout_ms = 1000;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let expression = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::ChangedPath(Box::new(
            q::ChangedPathExpr {
                matcher: None,
                policy_version: 2,
                candidates: Some(Box::new(ids_commits(&[root]))),
                path: "hello.cc".into(),
                exact_file: true,
            },
        ))),
    };
    // Two valid 70ms RPC stages cannot each receive a fresh 100ms budget.
    for (route, result) in [
        (
            "facts",
            client
                .query_facts(
                    &scope,
                    vec![QueryFact::Membership {
                        expression: expression.clone(),
                        candidate: root,
                    }],
                )
                .await
                .map(|_| ()),
        ),
        (
            "commit prefix",
            client
                .resolve_commit_prefix_selected(&scope, "", &expression)
                .await
                .map(|_| ()),
        ),
        (
            "change prefix",
            client
                .resolve_change_prefix_selected(&scope, "", vec![root], &expression)
                .await
                .map(|_| ()),
        ),
        (
            "shortest prefix",
            client
                .shortest_prefix_selected(
                    &scope,
                    PrefixTarget::Commit(root),
                    vec![root],
                    &expression,
                )
                .await
                .map(|_| ()),
        ),
    ] {
        assert_eq!(
            result.unwrap_err().code(),
            Code::DeadlineExceeded,
            "{route}"
        );
    }
    let mut linear = expression.clone();
    if let Some(q::graph_expr::Expression::ChangedPath(path)) = linear.expression.as_mut() {
        path.policy_version = 1;
    }
    let before_combined = running.capabilities.load(Ordering::SeqCst);
    assert_eq!(
        client
            .query_facts(
                &scope,
                vec![
                    QueryFact::Membership {
                        expression: linear,
                        candidate: root
                    },
                    QueryFact::Membership {
                        expression: expression.clone(),
                        candidate: root
                    },
                ]
            )
            .await
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );
    assert_eq!(
        running.capabilities.load(Ordering::SeqCst),
        before_combined + 1,
        "a valid mixed-policy batch negotiates its combined feature set once"
    );
    let before = running.capabilities.load(Ordering::SeqCst);
    let fact_calls = running.queries.facts.load(Ordering::SeqCst);
    let invalid = q::GraphExpr {
        version: 1,
        expression: None,
    };
    assert_eq!(
        client
            .query_facts(
                &scope,
                vec![
                    QueryFact::Membership {
                        expression: expression.clone(),
                        candidate: root
                    },
                    QueryFact::Membership {
                        expression: invalid,
                        candidate: root
                    },
                ]
            )
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(running.capabilities.load(Ordering::SeqCst), before);
    assert_eq!(running.queries.facts.load(Ordering::SeqCst), fact_calls);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
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
    config.limits.max_frame_bytes = 4096;
    config.limits.max_batch_bytes = 32768;
    config.limits.max_metadata_bytes = 16384;
    config.limits.max_request_bytes = 1024;
    config.limits.max_graph_bytes = 16384;
    config.limits.max_buffered_bytes = 262144;
    config.limits.max_batch_items = 16;
    config.limits.max_graph_objects = 800;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![root]).await.unwrap();
    let expression = ids_ancestors(root);
    let mut idle = Vec::new();
    for _ in 0..64 {
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            client.begin_query(&scope, &expression, 1),
        )
        .await
        .expect("persistent admission must never wait on another cursor");
        match result {
            Ok(query) => idle.push(query),
            Err(error) => {
                assert_eq!(error.code(), Code::ResourceExhausted);
                break;
            }
        }
    }
    assert!(
        idle.len() >= 2 && idle.len() < 64,
        "small prepared queries coexist but aggregate persistent state is bounded"
    );
    assert!(
        !client
            .evaluate_page(&mut idle[0])
            .await
            .unwrap()
            .rows
            .is_empty(),
        "full persistent admission must leave page memory available"
    );
    drop(idle.pop());
    let _reused = client.begin_query(&scope, &expression, 1).await.unwrap();
    drop(idle);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn ids_seen_growth_retries_same_cursor_after_releasing_an_idle_owner() {
    let (fixture, _, _, _) = linear_ids_fixture(256);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_frame_bytes = 4096;
    config.limits.max_batch_bytes = 32768;
    config.limits.max_metadata_bytes = 16384;
    config.limits.max_request_bytes = 1024;
    config.limits.max_graph_bytes = 16384;
    config.limits.max_buffered_bytes = 262144;
    config.limits.max_batch_items = 16;
    config.limits.max_graph_objects = 1024;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let expression = ids_ancestors(fixture.root);
    let mut active = client.begin_query(&scope, &expression, 16).await.unwrap();
    let mut idle = Vec::new();
    for _ in 0..64 {
        match client.begin_query(&scope, &expression, 16).await {
            Ok(query) => idle.push(query),
            Err(error) => {
                assert_eq!(error.code(), Code::ResourceExhausted);
                break;
            }
        }
    }
    assert!(!idle.is_empty() && idle.len() < 64);
    let mut sequence = 0;
    loop {
        let calls = running.counts.evaluations.load(Ordering::SeqCst);
        match tokio::time::timeout(Duration::from_secs(2), client.evaluate_page(&mut active))
            .await
            .expect("seen-row growth must fail without waiting")
        {
            Ok(page) => {
                assert_eq!(page.sequence, sequence);
                assert!(!page.exhausted, "fixture history must exceed funded state");
                sequence += 1;
            }
            Err(error) => {
                assert_eq!(error.code(), Code::ResourceExhausted);
                assert_eq!(
                    running.counts.evaluations.load(Ordering::SeqCst),
                    calls,
                    "growth fails before RPC and cannot advance its server cursor"
                );
                break;
            }
        }
    }
    assert!(sequence > 0);
    drop(idle.pop());
    let retry = client.evaluate_page(&mut active).await.unwrap();
    assert_eq!(retry.sequence, sequence);
    assert!(!retry.rows.is_empty());
    client.close_ids_query(&mut active).await.unwrap();
    drop(idle);
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
    config.limits.max_buffered_bytes = 160 * 1024;
    config.limits.max_batch_items = 16;
    config.limits.max_graph_objects = 800;
    let client = NativeClient::connect(config).await.unwrap();
    // Fill persistent headroom with actual application-held canonical pages,
    // not an upfront reservation for hypothetical unseen history.
    let description = "x".repeat(4000);
    let mut parent = fixture.root;
    let mut ids = Vec::new();
    let mut scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    // Install bounded overlays one at a time; scope setup must fit the same
    // small SDK envelope used to exercise persistent page pressure below.
    for seed in 87..95 {
        let draft = private_draft(parent, ChangeId([seed; 16]), &description);
        parent = CommitId(draft.descriptor.key.id.0);
        ids.push(parent);
        let verified = checked_draft(&client, &draft).await;
        let next = client
            .fork_query_scope(&scope, vec![verified])
            .await
            .unwrap();
        client.close_query_scope(&scope).await.unwrap();
        scope = next;
    }
    let expression = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Union(q::SetExpr {
            inputs: vec![ids_commits(&ids)],
        })),
    };
    let mut query = client
        .begin_graph_query(&scope, &expression, 1, true)
        .await
        .unwrap();
    let mut held = Vec::new();
    loop {
        match tokio::time::timeout(
            Duration::from_secs(2),
            client.evaluate_graph_page(&mut query),
        )
        .await
        .expect("retained-page exhaustion must fail without waiting")
        {
            Ok(page) => {
                assert_eq!(page.sequence, held.len() as u64);
                held.push(page);
                assert!(
                    !query.is_exhausted(),
                    "fixture must fill persistent page capacity"
                );
            }
            Err(error) => {
                assert_eq!(error.code(), Code::ResourceExhausted);
                break;
            }
        }
    }
    let next_sequence = held.len() as u64;
    assert!(next_sequence >= 2);
    // An extracted canonical object still owns the first page's reservation.
    let mut rows = held.remove(0).into_rows();
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
    assert_eq!(
        retry.sequence, next_sequence,
        "failed growth/page must not advance the cursor"
    );
    assert_eq!(
        retry.rows()[0].commit,
        ids[ids.len() - 1 - next_sequence as usize]
    );
    drop(retry);
    drop(held);
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
    config.limits.max_frame_bytes = 1024;
    config.limits.max_batch_bytes = 1024;
    config.limits.max_metadata_bytes = 1024;
    config.limits.max_request_bytes = 1024;
    config.limits.max_graph_bytes = 1024;
    config.limits.max_buffered_bytes = 16384;
    config.limits.max_batch_items = 4;
    config.limits.max_graph_objects = 950;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut query = client
        .begin_graph_query(&scope, &ids_ancestors(fixture.root), 1, false)
        .await
        .unwrap();
    let mut held = Vec::new();
    loop {
        match tokio::time::timeout(
            Duration::from_secs(2),
            client.evaluate_graph_page(&mut query),
        )
        .await
        .expect("held progress pages must fail without waiting")
        {
            Ok(page) => {
                assert!(page.is_progress());
                assert_eq!(page.sequence, held.len() as u64);
                held.push(page);
                assert!(held.len() < 16, "fixture must fill persistent headroom");
            }
            Err(error) => {
                assert_eq!(error.code(), Code::ResourceExhausted);
                break;
            }
        }
    }
    let next_sequence = held.len() as u64;
    assert!(next_sequence > 0);
    assert!(!query.is_exhausted());
    drop(held.pop());
    let retry = client.evaluate_graph_page(&mut query).await.unwrap();
    assert_eq!(retry.sequence, next_sequence);
    drop(retry);
    drop(held);
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
            .with_cleanup_deadline(tokio::time::Instant::now() - Duration::from_millis(1))
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
async fn explicit_cleanup_deadline_is_independent_and_not_renewed_by_guard_clones() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::CloseQueryPause).await;
    let mut config = running.config.clone();
    config.query_cleanup_timeout_ms = 2000;
    config.rpc.retry.max_attempts = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let mut query = client
        .begin_query(&scope, &ids_ancestors(fixture.root), 1)
        .await
        .unwrap();
    drop(client.evaluate_page(&mut query).await.unwrap());
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    let cleanup = client
        .clone()
        .with_operation_deadline(tokio::time::Instant::now() - Duration::from_millis(1))
        .with_cleanup_deadline(deadline)
        .with_lifetime_guard(Arc::new(()))
        .with_rpc_lifetime_guard(Arc::new(()))
        .with_cleanup_deadline(deadline + Duration::from_secs(5));
    let error = tokio::time::timeout(
        Duration::from_millis(500),
        cleanup.close_ids_query(&mut query),
    )
    .await
    .expect("explicit cleanup must respect original fresh bound, not configured two-second grace")
    .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert!(
        running.counts.query_closes.load(Ordering::SeqCst) > 0,
        "expired data deadline must not suppress independently admitted cleanup RPC"
    );
    let scope_closes = running.counts.closes.load(Ordering::SeqCst);
    assert_eq!(
        cleanup.close_query_scope(&scope).await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    assert_eq!(
        running.counts.closes.load(Ordering::SeqCst),
        scope_closes,
        "expired cleanup bound must reject Scope close before RPC"
    );
    // Release any server-side request that outlived the timed-out waiter, then
    // permit an independent exact retry. No new grace is hidden in the clone.
    running.counts.close_query_release.notify_waiters();
    running.counts.close_query_release.notify_one();
    client.close_ids_query(&mut query).await.unwrap();
    client
        .clone()
        .with_operation_deadline(tokio::time::Instant::now() - Duration::from_millis(1))
        .close_query_scope(&scope)
        .await
        .unwrap();
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
    config.limits.max_frame_bytes = 1024;
    config.limits.max_batch_bytes = 1024;
    config.limits.max_metadata_bytes = 1024;
    config.limits.max_request_bytes = 1024;
    config.limits.max_graph_bytes = 1024;
    config.limits.max_buffered_bytes = 16384;
    config.limits.max_batch_items = 4;
    config.limits.max_graph_objects = 4;
    config.query_cleanup_timeout_ms = 40;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let expression = ids_ancestors(fixture.root);
    let mut uncertain = client.begin_query(&scope, &expression, 1).await.unwrap();
    client.evaluate_page(&mut uncertain).await.unwrap();
    assert!(client.close_ids_query(&mut uncertain).await.is_err());
    assert!(uncertain.is_closed());
    // The closed handle has released its local query reservation but still owes
    // uncertain cleanup. An explicitly wide requested first page leaves less
    // than a queued item's 1024B in persistent headroom. This is actual page
    // capacity, not a reservation for all hypothetical history rows.
    let mut fresh = client.begin_query(&scope, &expression, 4).await.unwrap();
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

#[tokio::test]
async fn cloud_exact_representation_sdk_routes_large_nonprimary_bytes_with_private_grants() {
    use tinytree_client::{
        CapabilityFeature as F, CloudObjectId, CloudRawDigest, Config, NativeClient,
    };
    let directory = tempfile::tempdir().unwrap();
    let mut profile: tinytree_server::Config =
        toml::from_str(include_str!("../../tinytree-server/examples/cloud.toml")).unwrap();
    profile.service.cloud.max_object_bytes = 1024 * 1024;
    let store = Store::open(
        directory.path(),
        profile
            .store
            .materialize(profile.service.max_metadata_bytes),
    )
    .unwrap();
    store.create_repository("alpha").unwrap();
    store.create_repository("beta").unwrap();
    let source_directory = tempfile::tempdir().unwrap();
    use jj_lib::{
        backend::{MillisSinceEpoch, Timestamp},
        object_id::ObjectId as _,
        op_store::{
            OpStore, Operation, OperationMetadata, RootOperationData, TimestampRange, View,
        },
        simple_op_store::SimpleOpStore,
    };
    let source = SimpleOpStore::init(
        source_directory.path(),
        RootOperationData {
            root_commit_id: jj_lib::backend::CommitId::from_bytes(&[0; 32]),
        },
    )
    .unwrap();
    let mut view = View::make_root(jj_lib::backend::CommitId::from_bytes(&[0; 32]));
    view.wc_commit_ids.insert(
        "task".into(),
        jj_lib::backend::CommitId::from_bytes(&[0; 32]),
    );
    let view_id = source.write_view(&view).await.unwrap();
    let time = Timestamp {
        timestamp: MillisSinceEpoch(0),
        tz_offset: 0,
    };
    let op = Operation {
        view_id: view_id.clone(),
        parents: vec![jj_lib::op_store::OperationId::from_bytes(&[0; 64])],
        metadata: OperationMetadata {
            time: TimestampRange {
                start: time.clone(),
                end: time,
            },
            description: "large exact operation description\n".repeat(12_000),
            hostname: "agent".into(),
            username: "agent".into(),
            is_snapshot: true,
            workspace_name: Some("task".into()),
            attributes: Default::default(),
        },
        commit_predecessors: None,
    };
    let op_id = source.write_operation(&op).await.unwrap();
    let objects = vec![
        tinytree_cloud::export_operation(&source, &op_id, Default::default())
            .await
            .unwrap(),
        tinytree_cloud::export_view(&source, &view_id, Default::default())
            .await
            .unwrap(),
    ];
    let service = RepositoryService::new(
        store.clone(),
        profile.service.clone(),
        vec!["alpha".into(), "beta".into()],
        vec![
            Principal::new(
                "writer",
                "alpha-test-token-not-a-secret",
                vec!["alpha".into()],
                true,
            )
            .unwrap()
            .with_cloud(true),
            Principal::new(
                "reader",
                "reader-test-token-not-a-secret",
                vec!["alpha".into()],
                false,
            )
            .unwrap()
            .with_cloud(true),
            Principal::new(
                "peer",
                "peer-test-token-not-a-secret",
                vec!["alpha".into()],
                true,
            )
            .unwrap()
            .with_cloud(true),
        ],
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(tinytree_server::serve_tcp(listener, service, None, async {
        let _ = stopped.await;
    }));
    // Declared fixture credentials only; no process environment mutation.
    let base = Config {
        endpoint: format!("http://{}", address),
        repository_id: "alpha".into(),
        ..Default::default()
    };
    let sdk = NativeClient::connect(Config {
        token_env: Some("TINYTREE_EXACT_TEST_WRITER".into()),
        ..base.clone()
    })
    .await
    .unwrap();
    let caps = sdk
        .capabilities(&[
            F::ExactCloudRepresentationRead,
            F::ExactCloudRepresentationWrite,
        ])
        .await
        .unwrap();
    assert!(caps.features.iter().all(|f| f.supported && f.authorized));
    assert_eq!(F::ALL.len(), 5);
    let mut changed = [false; 2];
    for object in objects {
        let raw = object.payload().unwrap();
        let id = match object.id() {
            tinytree_cloud::ObjectId::Operation(id) => CloudObjectId::Operation(id),
            tinytree_cloud::ObjectId::View(id) => CloudObjectId::View(id),
        };
        let primary = sdk.cloud_put_object(id, raw).await.unwrap();
        // Unknown protobuf fields are preserved raw but ignored by semantic ID.
        let mut alternate = raw.to_vec();
        alternate.extend_from_slice(&[0xa0, 0x06, 0x01]);
        tinytree_cloud::VerifiedObject::verify(
            object.id(),
            tinytree_cloud::BlobDigest::of(&alternate),
            &alternate,
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
        let exact = sdk
            .cloud_put_object_representation(id, &alternate)
            .await
            .unwrap();
        assert_eq!(exact.descriptor.selected, CloudRawDigest::of(&alternate));
        assert_ne!(exact.descriptor, primary.descriptor);
        assert_eq!(
            sdk.cloud_lookup_objects(&[id]).await.unwrap(),
            vec![Some(primary.descriptor)]
        );
        assert_eq!(
            sdk.cloud_read_object_representation(exact.descriptor)
                .await
                .unwrap()
                .payload(),
            alternate
        );
        assert_eq!(
            sdk.cloud_read_object(primary.descriptor)
                .await
                .unwrap()
                .payload(),
            raw
        );
        let reader = NativeClient::connect(Config {
            token_env: Some("TINYTREE_EXACT_TEST_READER".into()),
            ..base.clone()
        })
        .await
        .unwrap();
        assert_eq!(
            reader
                .cloud_put_object_representation(id, &alternate)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let grants = reader
            .capabilities(&[
                F::ExactCloudRepresentationRead,
                F::ExactCloudRepresentationWrite,
            ])
            .await
            .unwrap();
        assert!(grants.features[0].authorized);
        assert!(!grants.features[1].authorized);
        // A read grant is scoped to the authenticated private owner, not to
        // another owner's catalog even when both can access this repository.
        assert!(
            reader
                .cloud_read_object_representation(exact.descriptor)
                .await
                .is_err()
        );
        let peer = NativeClient::connect(Config {
            token_env: Some("TINYTREE_EXACT_TEST_PEER".into()),
            ..base.clone()
        })
        .await
        .unwrap();
        assert!(
            peer.cloud_read_object_representation(exact.descriptor)
                .await
                .is_err()
        );
        let mut wrong = exact.descriptor;
        wrong.selected.size_bytes += 1;
        assert!(sdk.cloud_read_object_representation(wrong).await.is_err());
        changed[match id {
            CloudObjectId::Operation(_) => 0,
            _ => 1,
        }] = true;
        if matches!(id, CloudObjectId::Operation(_)) {
            assert!(alternate.len() > profile.service.cloud.max_request_bytes);
        }
    }
    assert_eq!(changed, [true, true]);
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
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
fn discovery_fixture_cursor(domain: u8, after: [u8; 32], ceiling: u64) -> Vec<u8> {
    let mut token = vec![1, domain];
    token.extend_from_slice(&[7; 32]);
    token.extend_from_slice(&after);
    token.extend_from_slice(&ceiling.to_be_bytes());
    token
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
        request: Request<tinytree_client::cloud_protocol::ListWorkspacesRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::ListWorkspacesResponse>, Status> {
        use tinytree_client::cloud_protocol as c;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let input = request.into_inner();
        let mode = self.mode.load(Ordering::SeqCst);
        let mut rows = vec![
            c::Workspace {
                workspace_id: vec![2; 32],
                generation: 2,
                manifest_id: Some(vec![4; 32]),
            },
            c::Workspace {
                workspace_id: vec![4; 32],
                generation: 1,
                manifest_id: Some(vec![5; 32]),
            },
        ];
        if !input.continuation.is_empty() {
            rows.remove(0);
        }
        let mut continuation = vec![];
        if rows.len() > input.page_size as usize {
            rows.truncate(input.page_size as usize);
            continuation = discovery_fixture_cursor(1, [2; 32], 0);
        }
        match mode {
            80 => rows.push(rows[0].clone()),
            81 => rows[0].generation = 0,
            82 => rows[0].workspace_id.pop().map(|_| ()).unwrap(),
            83 => continuation = vec![0; 74],
            84 => {
                rows[0].manifest_id = None;
            }
            _ => (),
        }
        Ok(Response::new(c::ListWorkspacesResponse {
            workspaces: rows,
            continuation,
        }))
    }
    async fn list_versions(
        &self,
        request: Request<tinytree_client::cloud_protocol::ListVersionsRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::ListVersionsResponse>, Status> {
        use tinytree_client::cloud_protocol as c;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let input = request.into_inner();
        let mode = self.mode.load(Ordering::SeqCst);
        let mut rows = vec![
            c::VersionSummary {
                workspace_id: input.workspace_id.clone(),
                generation: 1,
                manifest_id: vec![3; 32],
                previous_manifest_id: None,
            },
            c::VersionSummary {
                workspace_id: input.workspace_id.clone(),
                generation: 2,
                manifest_id: vec![4; 32],
                previous_manifest_id: Some(vec![3; 32]),
            },
        ];
        if !input.continuation.is_empty() {
            rows.remove(0);
        }
        let mut continuation = vec![];
        if rows.len() > input.page_size as usize {
            rows.truncate(input.page_size as usize);
            let mut position = [0; 32];
            position[..8].copy_from_slice(&1u64.to_be_bytes());
            continuation = discovery_fixture_cursor(2, position, 2);
        }
        let mut current = c::Workspace {
            workspace_id: input.workspace_id,
            generation: 2,
            manifest_id: Some(vec![4; 32]),
        };
        let mut ceiling = 2;
        match mode {
            90 => rows[0].workspace_id = vec![9; 32],
            91 => rows[0].generation = 2,
            92 => rows[1].previous_manifest_id = Some(vec![9; 32]),
            93 => {
                rows[0].manifest_id.pop();
            }
            94 => rows.clear(),
            95 => ceiling = 1,
            96 => continuation = vec![0; 74],
            97 => current.manifest_id = Some(vec![9; 32]),
            98 => {
                current.generation = u64::MAX;
                ceiling = u64::MAX;
                let mut row = rows[0].clone();
                row.generation = u64::MAX;
                row.previous_manifest_id = Some(vec![3; 32]);
                rows = vec![row.clone(), row];
            }
            _ => (),
        }
        Ok(Response::new(c::ListVersionsResponse {
            workspace: Some(current),
            versions: rows,
            continuation,
            through_generation: ceiling,
        }))
    }

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
    async fn get_latest_captured_version(
        &self,
        request: Request<tinytree_client::cloud_protocol::GetLatestCapturedVersionRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::Version>, Status> {
        if self.mode.load(Ordering::SeqCst) < 40 {
            self.calls.fetch_add(1, Ordering::SeqCst);
            return Err(Status::unimplemented("latest capture unavailable"));
        }
        let input = request.into_inner();
        self.get_version(Request::new(
            tinytree_client::cloud_protocol::GetVersionRequest {
                repository_id: input.repository_id,
                workspace_id: input.workspace_id,
                generation: 1,
            },
        ))
        .await
    }
    async fn get_version(
        &self,
        request: Request<tinytree_client::cloud_protocol::GetVersionRequest>,
    ) -> Result<Response<tinytree_client::cloud_protocol::Version>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        let manifest = if self.mode.load(Ordering::SeqCst) == 100 {
            vec![0; 4 * 1024 * 1024]
        } else if self.mode.load(Ordering::SeqCst) >= 40 {
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
async fn cloud_discovery_checks_pages_scopes_chains_and_malformed_replies() {
    use tinytree_client::cloud_protocol::workspace_cloud_server::WorkspaceCloudServer;
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
    config.rpc.retry.max_attempts = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let first = client.cloud_workspaces(1, &[]).await.unwrap();
    assert_eq!(first.workspaces[0].workspace_id, [2; 32]);
    let last = client
        .cloud_workspaces(1, &first.continuation)
        .await
        .unwrap();
    assert_eq!(last.workspaces[0].workspace_id, [4; 32]);
    assert!(last.continuation.is_empty());
    let first = client.cloud_versions([2; 32], 1, &[]).await.unwrap();
    assert_eq!(first.versions[0].generation, 1);
    assert_eq!(first.through_generation, 2);
    let last = client
        .cloud_versions([2; 32], 1, &first.continuation)
        .await
        .unwrap();
    assert_eq!(last.versions[0].generation, 2);
    assert!(last.continuation.is_empty());
    for (name, mode) in [
        ("duplicate workspace", 80),
        ("unpublished workspace", 81),
        ("malformed workspace ID", 82),
        ("malformed catalog cursor", 83),
        ("missing workspace manifest", 84),
    ] {
        faults.mode.store(mode, Ordering::SeqCst);
        assert_eq!(
            client.cloud_workspaces(2, &[]).await.unwrap_err().code(),
            Code::DataLoss,
            "{name}"
        );
    }
    for (name, mode) in [
        ("wrong workspace", 90),
        ("generation gap", 91),
        ("wrong previous manifest", 92),
        ("malformed manifest ID", 93),
        ("missing versions", 94),
        ("ceiling mismatch", 95),
        ("malformed version cursor", 96),
        ("current pointer mismatch", 97),
    ] {
        faults.mode.store(mode, Ordering::SeqCst);
        assert_eq!(
            client
                .cloud_versions([2; 32], 2, &[])
                .await
                .unwrap_err()
                .code(),
            Code::DataLoss,
            "{name}"
        );
    }
    faults.mode.store(98, Ordering::SeqCst);
    let mut position = [0; 32];
    position[..8].copy_from_slice(&(u64::MAX - 1).to_be_bytes());
    let boundary = discovery_fixture_cursor(2, position, u64::MAX);
    assert_eq!(
        client
            .cloud_versions([2; 32], 2, &boundary)
            .await
            .unwrap_err()
            .code(),
        Code::DataLoss,
        "generation overflow is a corrupt page, not a panic"
    );
    let before = faults.calls.load(Ordering::SeqCst);
    assert_eq!(
        client.cloud_workspaces(0, &[]).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .cloud_versions([2; 32], 129, &[])
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .cloud_workspaces(1, &[0; 74])
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(faults.calls.load(Ordering::SeqCst), before);
    server.abort();
    let _ = server.await;
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
    // The ordinary remote-adapter profile keeps its existing 32 MiB pool.
    // Discovery must accept a small version without reserving the configured
    // 8 MiB maximum, while a larger response still fails within that pool.
    let mut remote = config.clone();
    remote.limits.max_metadata_bytes = 4 * 1024 * 1024;
    remote.limits.max_batch_bytes = 4 * 1024 * 1024;
    remote.limits.max_graph_bytes = 8 * 1024 * 1024;
    remote.limits.max_graph_objects = 65_536;
    remote.limits.max_buffered_bytes = 32 * 1024 * 1024;
    let remote = NativeClient::connect(remote).await.unwrap();
    assert_eq!(
        remote.cloud_version([4; 32], 8).await.unwrap().manifest(),
        intent.manifest
    );
    faults.mode.store(100, Ordering::SeqCst);
    assert!(remote.cloud_version([4; 32], 8).await.is_err());
    faults.mode.store(5, Ordering::SeqCst);
    assert_eq!(
        remote.cloud_version([4; 32], 8).await.unwrap().manifest(),
        intent.manifest,
        "an oversized reply must release its physical and retained owners"
    );
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
    let mut second = tokio::time::timeout(
        Duration::from_millis(100),
        client.begin_graph_query(&scope, &ids_ancestors(fixture.root), 1, true),
    )
    .await
    .expect("another general cursor must not wait for the first")
    .unwrap();
    let second_page = client.evaluate_graph_page(&mut second).await.unwrap();
    assert_eq!(second_page.sequence, 0);
    assert_eq!(second_page.rows()[0].commit, fixture.root);
    let next = client.evaluate_graph_page(&mut wide).await.unwrap();
    assert_eq!(next.sequence, 1);
    assert_eq!(next.rows()[0].commit, tinytree_model::CommitId::ROOT);
    drop(next);
    let second_next = client.evaluate_graph_page(&mut second).await.unwrap();
    assert_eq!(second_next.sequence, 1);
    drop(second_next);
    client.close_graph_query(&mut second).await.unwrap();
    drop(second_page);
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
    let running = QueryRunning::start_cloud_wire_peer(service, QueryMode::CloudRetry).await;
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
        let running = QueryRunning::start_cloud_wire_peer(service, mode).await;
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
    let running = QueryRunning::start_cloud_wire_peer(service, QueryMode::CloudPause).await;
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
    let latest = client.cloud_latest_captured_version([1; 32]).await.unwrap();
    assert_eq!(latest.generation, 1);
    assert_eq!(latest.manifest(), capture_transport_fixture(40).1);
    drop(latest);
    faults.mode.store(0, Ordering::SeqCst);
    let before = faults.calls.load(Ordering::SeqCst);
    assert_eq!(
        client
            .cloud_latest_captured_version([1; 32])
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    assert_eq!(
        faults.calls.load(Ordering::SeqCst),
        before + 1,
        "unsupported latest selection must not query or fall back to the publication head"
    );
    faults.mode.store(40, Ordering::SeqCst);
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
    use tinytree_client::PublicationFailureReason as Reason;
    let mut manifest_config = config.clone();
    manifest_config.cloud.max_manifest_bytes = 1;
    let mut capture_config = config.clone();
    capture_config.cloud.max_capture_bytes = 1;
    for (name, config, expected) in [
        (
            "manifest-bytes",
            manifest_config,
            Reason::ClientManifestBytes,
        ),
        ("capture-bytes", capture_config, Reason::ClientCaptureBytes),
    ] {
        let small = NativeClient::connect(config).await.unwrap();
        let before = faults.calls.load(Ordering::SeqCst);
        let failure = small
            .cloud_publish_captured_version(&intent)
            .await
            .unwrap_err();
        assert_eq!(failure.code(), Code::ResourceExhausted, "{name}");
        assert_eq!(
            Reason::from_message(failure.message()),
            Some(expected),
            "{name}"
        );
        assert_eq!(faults.calls.load(Ordering::SeqCst), before);
    }
    server.abort();
    let _ = server.await;
}

fn setup_annotation_chain(length: usize) -> QueryFixture {
    let mut fixture = setup_query_retained();
    let (_, bytes) = fixture
        .store
        .read_metadata(
            "example",
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(fixture.root.0),
            },
        )
        .unwrap();
    let tinytree_model::Object::Commit(mut model) =
        tinytree_model::decode(&bytes, Default::default()).unwrap()
    else {
        unreachable!()
    };
    for number in 0..length {
        model.parents = vec![fixture.root];
        model.predecessors.clear();
        model.description = format!("annotation continuation step {number}");
        let object = tinytree_model::encode(
            &tinytree_model::Object::Commit(model.clone()),
            Default::default(),
        )
        .unwrap();
        fixture
            .store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
        fixture
            .index
            .ingest("example", object.descriptor, &object.bytes)
            .unwrap();
        fixture.root = tinytree_model::CommitId(object.descriptor.key.id.0);
    }
    fixture
}
fn annotation_reservation() -> q::AnnotationLimits {
    q::AnnotationLimits {
        work_nodes: 100_000,
        work_edges: 1_000_000,
        work_bytes: 32 * 1024 * 1024,
        frontier: 8192,
        read_bytes: 16 * 1024 * 1024,
        diff_input_bytes: 16 * 1024 * 1024,
        diff_lines: 1_000_000,
        diff_work: 8_000_000,
        diff_allocation_bytes: 16 * 1024 * 1024,
        diff_depth: 64,
    }
}
#[tokio::test]
async fn checked_annotation_continuation_collects_deep_history_and_detaches_rpc_owners() {
    let fixture = setup_annotation_chain(300);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    config.rpc.operation_timeout_ms = 30_000;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let result_drops = Arc::new(AtomicUsize::new(0));
    let rpc_drops = Arc::new(AtomicUsize::new(0));
    let operation = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(result_drops.clone())))
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(rpc_drops.clone())));
    let frontier = operation
        .collect_annotation_frontier_with_paging(
            &scope,
            &[fixture.root],
            "BUILD.bazel",
            4096,
            annotation_reservation(),
            7,
            13,
        )
        .await
        .unwrap();
    assert_eq!(frontier.result().version, 3);
    assert_eq!(
        frontier.result().coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert_eq!(frontier.result().scanned_commits, 301);
    assert_eq!(frontier.result().nodes.len(), 302);
    assert!(running.counts.annotation_pages.load(Ordering::SeqCst) > 2);
    let selected = frontier
        .result()
        .nodes
        .last()
        .unwrap()
        .value
        .as_ref()
        .unwrap();
    let body = operation
        .read_annotation_input(&frontier, fixture.root, selected)
        .await
        .unwrap();
    assert!(!body.result().data.is_empty());
    client
        .path_history(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::LinearFixedPathV1,
            1,
        )
        .await
        .unwrap();
    drop(operation);
    assert_released(&rpc_drops).await;
    assert_eq!(result_drops.load(Ordering::SeqCst), 0);
    client.close_annotation_frontier(&frontier).await.unwrap();
    drop(frontier);
    drop(body);
    assert_released(&result_drops).await;
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}
#[tokio::test]
async fn checked_server_annotation_keeps_canonical_graph_remote_and_validates_final_bytes() {
    let fixture = setup_annotation_chain(300);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    config.rpc.operation_timeout_ms = 30_000;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let rpc_drops = Arc::new(AtomicUsize::new(0));
    let operation = client
        .clone()
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(rpc_drops.clone())));
    let blame = operation
        .collect_annotation_blame(
            &scope,
            fixture.root,
            "BUILD.bazel",
            4096,
            annotation_reservation(),
        )
        .await
        .unwrap();
    assert_eq!(
        blame.result().coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert_eq!(blame.result().scanned_commits, 301);
    assert_eq!(
        running.counts.annotation_nodes.load(Ordering::SeqCst),
        0,
        "canonical annotation returns bytes and ranges, not ancestry metadata"
    );
    assert_eq!(
        running.counts.blames.load(Ordering::SeqCst),
        0,
        "no one-shot Blame restart"
    );
    drop(operation);
    assert_released(&rpc_drops).await;
    client
        .path_history(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::LinearFixedPathV1,
            1,
        )
        .await
        .unwrap();
    client.close_annotation_blame(&blame).await.unwrap();
    drop(blame);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
    for mode in [
        QueryMode::AnnotationAttributionHash,
        QueryMode::AnnotationAttributionGraph,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let error = client
            .collect_annotation_blame(
                &scope,
                fixture.root,
                "BUILD.bazel",
                4096,
                annotation_reservation(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::DataLoss, "{mode:?}: {error}");
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}
#[tokio::test]
async fn zero_node_annotation_progress_cannot_exceed_cursor_memory_or_page_admission() {
    for (batch_bytes, max_pages, maximum_calls) in [(8192, 1024, 40), (32 * 1024 * 1024, 2, 3)] {
        let fixture = setup_query_retained();
        let running =
            QueryRunning::start(fixture.service.clone(), QueryMode::AnnotationCursorFlood).await;
        let mut config = running.config.clone();
        config.limits.max_batch_bytes = batch_bytes;
        config.limits.max_metadata_bytes = batch_bytes.min(config.limits.max_metadata_bytes);
        config.limits.max_graph_pages = max_pages;
        let client = NativeClient::connect(config).await.unwrap();
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let error = client
            .collect_annotation_blame(
                &scope,
                fixture.root,
                "BUILD.bazel",
                4096,
                annotation_reservation(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            Code::ResourceExhausted,
            "{batch_bytes}/{max_pages}: {error}"
        );
        assert!(
            running.counts.annotation_pages.load(Ordering::SeqCst) <= maximum_calls,
            "refuse before an unbounded cursor allocation stream"
        );
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn checked_annotation_refuses_actual_graph_capacity_before_aggregate_append() {
    let fixture = setup_annotation_chain(12);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_batch_bytes = 8192;
    config.limits.max_metadata_bytes = 8192;
    config.limits.max_graph_bytes = 8192;
    config.limits.max_request_bytes = 1024;
    config.limits.max_batch_items = 16;
    config.limits.max_frame_bytes = 1024;
    config.limits.max_buffered_bytes = 65536;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let error = client
        .collect_annotation_frontier_with_paging(
            &scope,
            &[fixture.root],
            "BUILD.bazel",
            13,
            annotation_reservation(),
            4,
            128,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(
        error.message(),
        "annotation aggregate allocation exceeded reservation"
    );
    assert!(
        running.counts.annotation_nodes.load(Ordering::SeqCst) > 4,
        "valid small pages fit physically before aggregate retention is refused"
    );
    client.close_query_scope(&scope).await.unwrap();
    drop(scope);
    // Completed public results legitimately retain their own state admission.
    // Two small checked ROOT frontiers leave less than the 16KiB aggregate
    // request, while preserving the independent physical-page window.
    let mut held = Vec::new();
    for _ in 0..2 {
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let frontier = client
            .collect_annotation_frontier_with_paging(
                &scope,
                &[tinytree_model::CommitId::ROOT],
                "BUILD.bazel",
                1,
                annotation_reservation(),
                4,
                128,
            )
            .await
            .unwrap();
        assert_eq!(frontier.result().nodes.len(), 1);
        client.close_annotation_frontier(&frontier).await.unwrap();
        client.close_query_scope(&scope).await.unwrap();
        held.push(frontier);
    }
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let before = running.counts.annotation_nodes.load(Ordering::SeqCst);
    let error = client
        .collect_annotation_frontier_with_paging(
            &scope,
            &[fixture.root],
            "BUILD.bazel",
            13,
            annotation_reservation(),
            4,
            128,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(
        error.message(),
        "annotation aggregate allocation exceeded reservation"
    );
    assert!(
        running.counts.annotation_nodes.load(Ordering::SeqCst) > before,
        "reduced remaining capacity admits real pages but never unadmitted aggregation"
    );
    client.close_query_scope(&scope).await.unwrap();
    drop(scope);
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    // Untrusted opaque anchor inputs need admission even when an honest peer
    // would reject their cardinality before emitting its copied header.
    let anchors = (1..=64)
        .map(|value| tinytree_model::CommitId([value; 32]))
        .collect::<Vec<_>>();
    let before = running.counts.annotation_pages.load(Ordering::SeqCst);
    let error = client
        .collect_annotation_frontier_with_paging(
            &scope,
            &anchors,
            "BUILD.bazel",
            1,
            annotation_reservation(),
            4,
            128,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(
        error.message(),
        "annotation aggregate header exceeds reservation"
    );
    assert_eq!(
        running.counts.annotation_pages.load(Ordering::SeqCst),
        before,
        "refuse all retained anchor-header copies before issuing frontier work"
    );
    client.close_query_scope(&scope).await.unwrap();
    drop(held);
    running.stop().await;
}

#[tokio::test]
async fn checked_server_annotation_accepts_verified_empty_target_without_ancestry_scan() {
    let mut fixture = setup_query_retained();
    let (_, bytes) = fixture
        .store
        .read_metadata(
            "example",
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(fixture.root.0),
            },
        )
        .unwrap();
    let tinytree_model::Object::Commit(mut commit) =
        tinytree_model::decode(&bytes, Default::default()).unwrap()
    else {
        unreachable!()
    };
    let tree_id = commit.root_tree.terms[0];
    let (_, bytes) = fixture
        .store
        .read_metadata(
            "example",
            ObjectKey {
                kind: Kind::Tree,
                id: ObjectId(tree_id.0),
            },
        )
        .unwrap();
    let tinytree_model::Object::Tree(mut tree) =
        tinytree_model::decode(&bytes, Default::default()).unwrap()
    else {
        unreachable!()
    };
    let empty = tinytree_model::describe_file(&[]);
    fixture.store.put("example", empty, &[], 0).unwrap();
    tree.entries
        .iter_mut()
        .find(|entry| entry.name == "BUILD.bazel")
        .unwrap()
        .value = tinytree_model::TreeValue::File {
        id: tinytree_model::FileId(empty.key.id.0),
        executable: false,
        size_bytes: 0,
        copy_id: None,
    };
    let tree =
        tinytree_model::encode(&tinytree_model::Object::Tree(tree), Default::default()).unwrap();
    fixture
        .store
        .put("example", tree.descriptor, &tree.bytes, 0)
        .unwrap();
    commit.parents = vec![fixture.root];
    commit.predecessors.clear();
    commit.description = "empty annotation target".into();
    commit.root_tree.terms = vec![tinytree_model::TreeId(tree.descriptor.key.id.0)];
    let commit =
        tinytree_model::encode(&tinytree_model::Object::Commit(commit), Default::default())
            .unwrap();
    fixture
        .store
        .put("example", commit.descriptor, &commit.bytes, 0)
        .unwrap();
    fixture
        .index
        .ingest("example", commit.descriptor, &commit.bytes)
        .unwrap();
    fixture.root = tinytree_model::CommitId(commit.descriptor.key.id.0);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let blame = client
        .collect_annotation_blame(
            &scope,
            fixture.root,
            "BUILD.bazel",
            1,
            annotation_reservation(),
        )
        .await
        .unwrap();
    assert!(blame.result().target_bytes.is_empty() && blame.result().ranges.is_empty());
    assert!(blame.result().target.is_some());
    assert_eq!(blame.result().scanned_commits, 0);
    assert_eq!(running.counts.annotation_pages.load(Ordering::SeqCst), 1);
    assert_eq!(running.counts.annotation_nodes.load(Ordering::SeqCst), 0);
    client.close_annotation_blame(&blame).await.unwrap();
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn annotation_context_uses_command_deadline_not_first_attempt_timeout() {
    let fixture = setup_annotation_chain(10);
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::AnnotationPageSlow).await;
    let mut config = running.config.clone();
    config.rpc.operation_timeout_ms = 10_000;
    config.rpc.rpc_timeout_ms = 1000;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let started = tokio::time::Instant::now();
    let frontier = client
        .collect_annotation_frontier_with_paging(
            &scope,
            &[fixture.root],
            "BUILD.bazel",
            4096,
            annotation_reservation(),
            1,
            1,
        )
        .await
        .unwrap();
    assert!(
        started.elapsed() > Duration::from_secs(1),
        "multiple pages cross one attempt allowance"
    );
    assert_eq!(frontier.result().scanned_commits, 11);
    client.close_annotation_frontier(&frontier).await.unwrap();
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;

    let fixture = setup_query_retained();
    let running = QueryRunning::start(
        fixture.service.clone(),
        QueryMode::AnnotationPageShortExpiry,
    )
    .await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let started = tokio::time::Instant::now();
    let blame = client
        .collect_annotation_blame(
            &scope,
            fixture.root,
            "BUILD.bazel",
            4096,
            annotation_reservation(),
        )
        .await
        .unwrap();
    assert!(
        blame.data_deadline() <= started + Duration::from_secs(3),
        "a shorter returned context expiry narrows the checked data deadline"
    );
    client.close_annotation_blame(&blame).await.unwrap();
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn checked_annotation_continuation_rejects_unbound_or_nonprogressing_pages() {
    for (mode, expected) in [
        (QueryMode::AnnotationPageCursorEcho, Code::DataLoss),
        (QueryMode::AnnotationPageSequence, Code::DataLoss),
        (QueryMode::AnnotationPageCounters, Code::DataLoss),
        (QueryMode::AnnotationPagePath, Code::DataLoss),
        (QueryMode::AnnotationPageClosure, Code::DataLoss),
        (QueryMode::AnnotationPageStalled, Code::DataLoss),
        (QueryMode::AnnotationPageDuplicateNode, Code::DataLoss),
        (QueryMode::AnnotationPageMissingParent, Code::DataLoss),
        (QueryMode::AnnotationPageScannedCount, Code::DataLoss),
        (QueryMode::AnnotationPageExpired, Code::DeadlineExceeded),
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let error = client
            .collect_annotation_frontier_with_paging(
                &scope,
                &[fixture.root],
                "BUILD.bazel",
                4096,
                annotation_reservation(),
                1,
                1,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), expected, "{mode:?}: {error}");
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}
#[tokio::test]
async fn checked_annotation_continuation_accepts_already_charged_byte_boundary_output() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(
        fixture.service.clone(),
        QueryMode::AnnotationPagePendingNode,
    )
    .await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let frontier = client
        .collect_annotation_frontier_with_paging(
            &scope,
            &[fixture.root],
            "BUILD.bazel",
            4096,
            annotation_reservation(),
            1,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        frontier.result().coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert_eq!(frontier.result().nodes.len(), 2);
    assert_eq!(
        frontier
            .result()
            .nodes
            .last()
            .unwrap()
            .revision
            .as_ref()
            .unwrap()
            .value,
        fixture.root.0
    );
    client.close_annotation_frontier(&frontier).await.unwrap();
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn checked_annotation_continuation_keeps_one_absolute_deadline() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::AnnotationPageSlow).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let started = tokio::time::Instant::now();
    let error = client
        .clone()
        .with_operation_deadline(started + Duration::from_millis(80))
        .collect_annotation_frontier_with_paging(
            &scope,
            &[fixture.root],
            "BUILD.bazel",
            4096,
            annotation_reservation(),
            1,
            1,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert!(started.elapsed() < Duration::from_millis(250));
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn checked_annotation_frontier_and_inputs_keep_source_and_detach_rpc_owners() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    config.limits.max_graph_objects = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let result_drops = Arc::new(AtomicUsize::new(0));
    let rpc_drops = Arc::new(AtomicUsize::new(0));
    let operation = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(result_drops.clone())))
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(rpc_drops.clone())));
    let frontier = operation
        .open_annotation_frontier(&scope, &[fixture.root], "BUILD.bazel", 0)
        .await
        .unwrap();
    assert_eq!(
        frontier.result().coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert_eq!(
        frontier.result().nodes.len(),
        2,
        "one bounded non-root commit PLUS intrinsic root"
    );
    let selected = frontier
        .result()
        .nodes
        .iter()
        .find(|n| n.revision.as_ref().unwrap().value == fixture.root.0)
        .unwrap();
    let expected = selected.value.as_ref().unwrap().clone();
    let body = operation
        .read_annotation_input(&frontier, fixture.root, &expected)
        .await
        .unwrap();
    assert!(!body.result().data.is_empty());
    assert_eq!(body.result().value.as_ref(), Some(&expected));
    // Holding the frontier and input does not retain the sole inflight permit,
    // and other metadata queries on the captured scope remain available.
    operation
        .path_history(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::LinearFixedPathV1,
            0,
        )
        .await
        .unwrap();
    let mut forged = expected.clone();
    forged.copy_id = vec![9; 32];
    assert_eq!(
        operation
            .read_annotation_input(&frontier, fixture.root, &forged)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    drop(operation);
    assert_eq!(rpc_drops.load(Ordering::SeqCst), 1);
    assert_eq!(result_drops.load(Ordering::SeqCst), 0);
    client.close_annotation_frontier(&frontier).await.unwrap();
    assert_eq!(
        client
            .read_annotation_input(&frontier, fixture.root, &expected)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        client
            .open_annotation_frontier(&scope, &[fixture.root], "BUILD.bazel", 0)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert!(
        !body.result().data.is_empty(),
        "logical close does not invalidate independently owned verified bytes"
    );
    drop(frontier);
    drop(body);
    assert_eq!(result_drops.load(Ordering::SeqCst), 1);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}
#[tokio::test]
async fn checked_annotation_rejects_unbound_graphs_and_corrupt_demand_inputs() {
    for mode in [
        QueryMode::AnnotationWrongDigest,
        QueryMode::AnnotationMissingFlag,
        QueryMode::AnnotationCyclicParent,
        QueryMode::AnnotationInputHash,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let result = client
            .open_annotation_frontier(&scope, &[fixture.root], "BUILD.bazel", 0)
            .await;
        let error = if mode == QueryMode::AnnotationInputHash {
            let frontier = result.unwrap();
            let selected = frontier
                .result()
                .nodes
                .iter()
                .find(|n| n.revision.as_ref().unwrap().value == fixture.root.0)
                .unwrap();
            client
                .read_annotation_input(&frontier, fixture.root, selected.value.as_ref().unwrap())
                .await
                .unwrap_err()
        } else {
            result.unwrap_err()
        };
        assert_eq!(error.code(), Code::DataLoss, "{mode:?}: {error}");
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn checked_path_history_and_blame_bind_revision_and_retain_only_result_owners() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let result_drops = Arc::new(AtomicUsize::new(0));
    let rpc_drops = Arc::new(AtomicUsize::new(0));
    let operation = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(result_drops.clone())))
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(rpc_drops.clone())));
    let history = operation
        .path_history(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::LinearFixedPathV1,
            0,
        )
        .await
        .unwrap();
    assert_eq!(
        history.result().coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert_eq!(history.result().entries.len(), 1);
    assert_eq!(
        history.result().entries[0].commit.as_ref().unwrap().value,
        fixture.root.0
    );
    // A held result must not retain the sole inflight slot.
    let blame = operation
        .blame(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::LinearFixedPathV1,
            0,
        )
        .await
        .unwrap();
    assert_eq!(
        blame.result().coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert!(!blame.result().target_bytes.is_empty());
    assert!(
        blame
            .result()
            .ranges
            .iter()
            .all(|range| range.origin.as_ref().unwrap().value == fixture.root.0)
    );
    let dag_blame = operation
        .blame(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::JjResolvedFixedPathV1,
            0,
        )
        .await
        .unwrap();
    assert_eq!(
        dag_blame.result().policy,
        q::HistoryPolicy::JjResolvedFixedPathV1 as i32
    );
    assert_eq!(dag_blame.result().target_bytes, blame.result().target_bytes);
    assert_eq!(dag_blame.result().ranges, blame.result().ranges);
    assert_eq!(
        dag_blame.result().coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    drop(dag_blame);
    let collection = operation
        .collect_path_history(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::LinearFixedPathV1,
            8,
        )
        .await
        .unwrap();
    assert_eq!(collection.result(), history.result());
    drop(operation);
    assert_released(&rpc_drops).await;
    assert_eq!(result_drops.load(Ordering::SeqCst), 0);
    let collection_clone = collection.clone();
    drop(collection);
    let history_clone = history.clone();
    drop(history);
    drop(blame);
    assert_eq!(result_drops.load(Ordering::SeqCst), 0);
    drop(history_clone);
    assert_eq!(result_drops.load(Ordering::SeqCst), 0);
    drop(collection_clone);
    assert_released(&result_drops).await;
    client.close_query_scope(&scope).await.unwrap();
    assert_eq!(
        client
            .path_history(
                &scope,
                fixture.root,
                "BUILD.bazel",
                q::HistoryPolicy::LinearFixedPathV1,
                0
            )
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    running.stop().await;
}

#[tokio::test]
async fn checked_path_history_rejects_unbound_and_malformed_server_replies() {
    for mode in [
        QueryMode::HistoryWrongDigest,
        QueryMode::HistoryWrongRevision,
        QueryMode::HistoryWrongPath,
        QueryMode::HistoryWrongPolicy,
        QueryMode::HistoryInvalidCoverage,
        QueryMode::HistoryFalseComplete,
        QueryMode::HistoryMalformedState,
        QueryMode::HistoryDuplicateEntry,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let error = client
            .path_history(
                &scope,
                fixture.root,
                "BUILD.bazel",
                q::HistoryPolicy::LinearFixedPathV1,
                0,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::DataLoss, "{mode:?}: {error}");
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn checked_blame_rejects_corrupt_bytes_gaps_and_false_attribution() {
    for mode in [
        QueryMode::BlameWrongHash,
        QueryMode::BlameGap,
        QueryMode::BlameUnresolvedComplete,
        QueryMode::BlameOriginWidth,
        QueryMode::BlameOriginOverflow,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let error = client
            .blame(
                &scope,
                fixture.root,
                "BUILD.bazel",
                q::HistoryPolicy::LinearFixedPathV1,
                0,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::DataLoss, "{mode:?}: {error}");
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn fixed_path_history_rejects_invalid_inputs_before_rpc() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    for path in [
        "",
        "/BUILD.bazel",
        "a/../BUILD.bazel",
        "a//file",
        "a\\file",
        "file\0suffix",
    ] {
        assert_eq!(
            client
                .path_history(
                    &scope,
                    fixture.root,
                    path,
                    q::HistoryPolicy::LinearFixedPathV1,
                    0
                )
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            client
                .blame(
                    &scope,
                    fixture.root,
                    path,
                    q::HistoryPolicy::LinearFixedPathV1,
                    0
                )
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    assert_eq!(running.counts.histories.load(Ordering::SeqCst), 0);
    assert_eq!(running.counts.blames.load(Ordering::SeqCst), 0);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn checked_blame_preserves_explicit_partial_coverage_without_invented_origins() {
    // Structural transport fixture: server-side budget and authority behavior
    // are verified by the real service's RPC cases, not by these altered replies.
    for (mode, coverage, has_target) in [
        (
            QueryMode::BlamePartialReply,
            q::HistoryCoverage::HistoryScanLimit,
            true,
        ),
        (
            QueryMode::BlameUnavailableTarget,
            q::HistoryCoverage::HistoryReadLimit,
            false,
        ),
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let blame = client
            .blame(
                &scope,
                fixture.root,
                "BUILD.bazel",
                q::HistoryPolicy::LinearFixedPathV1,
                0,
            )
            .await
            .unwrap();
        assert_eq!(blame.result().coverage, coverage as i32, "{mode:?}");
        assert_eq!(blame.result().target.is_some(), has_target, "{mode:?}");
        assert!(blame.result().boundary.is_some());
        assert!(
            blame
                .result()
                .ranges
                .iter()
                .all(|range| range.origin.is_none())
        );
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

async fn first_ready_search(
    client: &NativeClient,
    scope: &tinytree_client::CheckedScopeHandle,
    revision: tinytree_model::CommitId,
    page_size: u32,
) -> Result<tinytree_client::CheckedSearchPage, Status> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = client
                .search(
                    scope,
                    revision,
                    b"cc_binary",
                    "BUILD.bazel",
                    q::SearchPolicy::LiteralBytesV1,
                    page_size,
                    &[],
                )
                .await?;
            if page.result().coverage != q::SearchCoverage::SearchWarming as i32 {
                return Ok(page);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("bounded search index warming")
}

#[tokio::test]
async fn checked_search_pages_pin_revision_replay_and_release_rpc_owners() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::SearchRetry).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let persistent = Arc::new(AtomicUsize::new(0));
    let physical = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(persistent.clone())))
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(physical.clone())));
    let first = first_ready_search(&guarded, &scope, fixture.root, 1)
        .await
        .unwrap();
    assert_eq!(first.result().hits.len(), 1);
    assert_eq!(
        first.result().coverage,
        q::SearchCoverage::SearchMore as i32
    );
    assert_eq!(
        running.counts.search_ready.load(Ordering::SeqCst),
        2,
        "lost first-page ACK replays the exact immutable page"
    );
    let second = guarded
        .search(
            &scope,
            fixture.root,
            b"cc_binary",
            "BUILD.bazel",
            q::SearchPolicy::LiteralBytesV1,
            1,
            &first.result().cursor,
        )
        .await
        .unwrap();
    let replay = guarded
        .search(
            &scope,
            fixture.root,
            b"cc_binary",
            "BUILD.bazel",
            q::SearchPolicy::LiteralBytesV1,
            1,
            &first.result().cursor,
        )
        .await
        .unwrap();
    assert_eq!(second.result(), replay.result());
    assert!(second.result().hits[0].byte_offset > first.result().hits[0].byte_offset);
    drop(guarded);
    assert_released(&physical).await;
    assert_eq!(persistent.load(Ordering::SeqCst), 0);
    // Retained snippets and session state must leave the sole RPC slot free.
    let mut query = client
        .begin_search(
            &scope,
            fixture.root,
            b"cc_binary",
            "BUILD.bazel",
            q::SearchPolicy::LiteralBytesV1,
            1,
        )
        .await
        .unwrap();
    let mut hits = Vec::new();
    while !query.is_terminal() {
        let page = client.search_page(&mut query).await.unwrap();
        hits.extend(page.result().hits.clone());
        if query.is_warming() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    assert!(query.is_complete());
    let seed = crate::fixture();
    let source = seed
        .objects()
        .iter()
        .find(|object| {
            object.descriptor.key.kind == Kind::File && object.bytes.starts_with(b"load(")
        })
        .unwrap();
    let expected: Vec<_> = source
        .bytes
        .windows(b"cc_binary".len())
        .enumerate()
        .filter_map(|(index, bytes)| (bytes == b"cc_binary").then_some(index as u64))
        .collect();
    assert_eq!(
        hits.iter().map(|hit| hit.byte_offset).collect::<Vec<_>>(),
        expected
    );
    assert!(hits.iter().all(|hit| hit.path == "BUILD.bazel"));
    drop(first);
    drop(second);
    assert_eq!(persistent.load(Ordering::SeqCst), 0);
    drop(replay);
    assert_released(&persistent).await;
    assert_eq!(
        client.search_page(&mut query).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn checked_search_rejects_unbound_or_malformed_snippet_pages() {
    for mode in [
        QueryMode::SearchWrongDigest,
        QueryMode::SearchRootWidth,
        QueryMode::SearchMissingManifest,
        QueryMode::SearchWrongPattern,
        QueryMode::SearchWrongPrefix,
        QueryMode::SearchWrongPolicy,
        QueryMode::SearchFalseComplete,
        QueryMode::SearchCorruptSnippet,
        QueryMode::SearchDuplicateHit,
        QueryMode::SearchOutsidePrefix,
        QueryMode::SearchInvalidLine,
        QueryMode::SearchOverflow,
        QueryMode::SearchLargePath,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let page_size = if mode == QueryMode::SearchDuplicateHit {
            2
        } else {
            1
        };
        let error = first_ready_search(&client, &scope, fixture.root, page_size)
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
async fn checked_search_session_rejects_manifest_progress_and_result_regressions() {
    for mode in [
        QueryMode::SearchChangedManifest,
        QueryMode::SearchChangedFile,
        QueryMode::SearchRegressedHit,
        QueryMode::SearchRegressedProgress,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let mut query = client
            .begin_search(
                &scope,
                fixture.root,
                b"cc_binary",
                "BUILD.bazel",
                q::SearchPolicy::LiteralBytesV1,
                1,
            )
            .await
            .unwrap();
        loop {
            let page = client.search_page(&mut query).await.unwrap();
            if !query.is_warming() {
                assert_eq!(page.result().hits.len(), 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            client.search_page(&mut query).await.unwrap_err().code(),
            Code::DataLoss,
            "{mode:?}"
        );
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn immutable_search_inputs_sessions_and_deadlines_fail_before_remote_work() {
    let fixture = setup_query_retained();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let before = running.counts.searches.load(Ordering::SeqCst);
    for (pattern, prefix, policy, size, cursor) in [
        (&b""[..], "", q::SearchPolicy::LiteralBytesV1, 1, Vec::new()),
        (&b"("[..], "", q::SearchPolicy::RegexBytesV1, 1, Vec::new()),
        (
            &b"\xff"[..],
            "",
            q::SearchPolicy::RegexBytesV1,
            1,
            Vec::new(),
        ),
        (
            &b"a\nb"[..],
            "",
            q::SearchPolicy::LiteralBytesV1,
            1,
            Vec::new(),
        ),
        (
            &b"a"[..],
            "../escape",
            q::SearchPolicy::LiteralBytesV1,
            1,
            Vec::new(),
        ),
        (
            &b"a"[..],
            "",
            q::SearchPolicy::LiteralBytesV1,
            1,
            vec![1; 4097],
        ),
        (
            &b"a"[..],
            "",
            q::SearchPolicy::try_from(0).unwrap(),
            1,
            Vec::new(),
        ),
        (
            &b"a"[..],
            "",
            q::SearchPolicy::LiteralBytesV1,
            4097,
            Vec::new(),
        ),
    ] {
        assert_eq!(
            client
                .search(&scope, fixture.root, pattern, prefix, policy, size, &cursor)
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    assert_eq!(running.counts.searches.load(Ordering::SeqCst), before);
    let foreign = running.client().await;
    let mut query = client
        .begin_search(
            &scope,
            fixture.root,
            b"cc",
            "",
            q::SearchPolicy::LiteralBytesV1,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        foreign.search_page(&mut query).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let expired = client
        .clone()
        .with_operation_deadline(tokio::time::Instant::now() - Duration::from_millis(1));
    assert_eq!(
        expired.search_page(&mut query).await.unwrap_err().code(),
        Code::DeadlineExceeded
    );
    assert_eq!(running.counts.searches.load(Ordering::SeqCst), before);
    client.close_query_scope(&scope).await.unwrap();
    assert_eq!(
        client.search_page(&mut query).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    running.stop().await;
}

// Real server/Store fixture for landing workflows. Deliberately separate from
// malformed-protocol fixtures: success is never supplied by a mock job service.
fn landing_fixture_principal(grant: bool) -> Principal {
    Principal::new("test", TOKEN, vec!["example".into()], grant)
        .unwrap()
        .with_landing_profiles(if grant { vec!["checks".into()] } else { vec![] })
}
fn landing_fixture_owner() -> tinytree_store::LandingOwner {
    // Controlled in-process fixture authority, not SDK-side authentication.
    // Match the real service's authenticated principal namespace for seeding a
    // fixed committer; actual Get still authenticates its normal bearer token.
    let principal = landing_fixture_principal(true);
    let mut retention = blake3::Hasher::new();
    retention.update(b"tinytree-retention-owner-v1\0");
    retention.update(&(principal.id.len() as u64).to_be_bytes());
    retention.update(principal.id.as_bytes());
    let mut owner = blake3::Hasher::new();
    owner.update(b"tinytree-landing-owner-v1\0");
    owner.update(&[1]);
    owner.update(retention.finalize().as_bytes());
    tinytree_store::LandingOwner(*owner.finalize().as_bytes())
}
struct LandingRunning {
    address: std::net::SocketAddr,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl LandingRunning {
    async fn start(
        store: Store,
        config: ServiceConfig,
        grant: bool,
        address: Option<std::net::SocketAddr>,
    ) -> Self {
        let service = RepositoryService::new(
            store,
            config,
            vec!["example".into()],
            vec![
                Principal::new("test", TOKEN, vec!["example".into()], grant)
                    .unwrap()
                    .with_landing_profiles(if grant { vec!["checks".into()] } else { vec![] }),
            ],
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind(
            address.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap()),
        )
        .await
        .unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            tinytree_server::serve_tcp(listener, service, None, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        Self {
            address,
            stop,
            task,
        }
    }
    fn config(&self) -> Config {
        Config {
            endpoint: format!("http://{}", self.address),
            repository_id: "example".into(),
            token_env: Some("TINYTREE_CLIENT_TEST_TOKEN".into()),
            ..Default::default()
        }
    }
    async fn stop(self) {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
fn landing_fixture() -> (
    tempfile::TempDir,
    Store,
    ServiceConfig,
    tinytree_client::LandingStart,
) {
    use tinytree_model::{ChangeId, CommitId, Object};
    let mut config: tinytree_server::Config =
        toml::from_str(include_str!("../../tinytree-server/examples/landing.toml")).unwrap();
    config.service.landing.poll_interval_ms = 10;
    let directory = private_tempdir();
    let store = Store::open(
        directory.path(),
        config.store.materialize(config.service.max_metadata_bytes),
    )
    .unwrap();
    store.create_repository("example").unwrap();
    let fixture = fixture();
    for object in fixture.objects() {
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let base = CommitId(
        *blake3::Hash::from_hex(&fixture.manifest.root_commit)
            .unwrap()
            .as_bytes(),
    );
    let (_, bytes) = store
        .read_metadata(
            "example",
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(base.0),
            },
        )
        .unwrap();
    let Object::Commit(mut draft) = tinytree_model::decode(&bytes, Default::default()).unwrap()
    else {
        unreachable!()
    };
    draft.parents = vec![base];
    draft.change_id = ChangeId([19; 16]);
    draft.description = "landing SDK private draft".into();
    let source = tinytree_model::encode(&Object::Commit(draft), Default::default()).unwrap();
    store
        .put("example", source.descriptor, &source.bytes, 0)
        .unwrap();
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "landing-baseline".into(),
                reference_name: "main".into(),
                expected_generation: 0,
                new_commit: Some(base),
                staging_leases: vec![],
            },
            0,
        )
        .unwrap();
    let request = tinytree_client::LandingStart {
        idempotency_key: "sdk-landing".into(),
        source_base: base,
        source_head: CommitId(source.descriptor.key.id.0),
        destination: "main".into(),
        expected_generation: 1,
        expected_head: base,
        profile_name: "checks".into(),
        expected_profile_version: "v1".into(),
    };
    (directory, store, config.service, request)
}
#[tokio::test]
async fn landing_compressed_preparation_replay_and_retained_results_are_checked() {
    use tinytree_client::{LandingPhase, LandingPreparation};
    let (_directory, store, config, start) = landing_fixture();
    let running = LandingRunning::start(store.clone(), config, true, None).await;
    let mut client_config = running.config();
    client_config.landing.max_response_bytes = 1024 * 1024;
    client_config.limits.max_buffered_bytes = 203 * 1024 * 1024;
    client_config.limits.max_graph_bytes = 32 * 1024 * 1024;
    let client = NativeClient::connect(client_config).await.unwrap();
    let lifetime = Arc::new(AtomicUsize::new(0));
    let rpc_lifetime = Arc::new(AtomicUsize::new(0));
    let selected = client
        .clone()
        .with_lifetime_guard(Arc::new(LifetimeProbe(lifetime.clone())))
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(rpc_lifetime.clone())));
    let job = selected.start_landing(&start).await.unwrap();
    assert!(matches!(
        job.phase(),
        LandingPhase::Intent | LandingPhase::PreparedPendingChecks
    ));
    assert_eq!(job.preparation(), LandingPreparation::ProfileConfigured);
    let handle = job.handle();
    let id = handle.id();
    assert_eq!(
        client.get_landing(&handle).await.unwrap_err().code(),
        Code::ResourceExhausted,
        "retained result must remain charged after RPC"
    );
    drop(selected);
    assert_released(&rpc_lifetime).await;
    assert_eq!(lifetime.load(Ordering::SeqCst), 0);
    drop(job);
    assert_eq!(
        lifetime.load(Ordering::SeqCst),
        0,
        "the handle still owns its intent"
    );
    drop(handle);
    assert_released(&lifetime).await;
    // Preparation shares the server's bounded heavy pool with Start. Status
    // remains light; wait for that physical work before replaying acceptance.
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = client.lookup_landing(&start, Some(id)).await.unwrap();
        let ready = status.phase() != LandingPhase::Intent;
        drop(status);
        if ready {
            break;
        }
        assert!(tokio::time::Instant::now() < ready_deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let replay = client.start_landing(&start).await.unwrap();
    assert_eq!(replay.handle().id(), id);
    drop(replay);
    let status = client.lookup_landing(&start, Some(id)).await.unwrap();
    let handle = status.handle();
    drop(status);
    // The independent next result has headroom with the ordinary default pool.
    let ordinary = NativeClient::connect(running.config()).await.unwrap();
    assert_eq!(
        ordinary.get_landing(&handle).await.unwrap_err().code(),
        Code::InvalidArgument,
        "handles cannot change authenticated client transport"
    );
    drop(handle);
    let mut job = ordinary.lookup_landing(&start, Some(id)).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while job.phase() == LandingPhase::Intent {
        let h = job.handle();
        drop(job);
        job = ordinary.get_landing(&h).await.unwrap();
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(job.phase(), LandingPhase::PreparedPendingChecks);
    assert!(job.roots_retained());
    assert!(job.receipt().is_none());
    assert!(job.candidate().is_some());
    assert_eq!(
        store.references("example", &["main".into()]).unwrap()[0].generation,
        1,
        "preparation/status is not publication or a check verdict"
    );
    drop(job);
    let mut intrinsic = start.clone();
    intrinsic.idempotency_key = "intrinsic-base".into();
    intrinsic.source_base = tinytree_model::CommitId::ROOT;
    client.start_landing(&intrinsic).await.unwrap();
    // Only the trusted embedding Store API supplies a verdict in this fixture;
    // the SDK exposes preparation/status only, including historical receipts.
    let pending = store
        .scan_pending_landings("example", None, Default::default())
        .unwrap()
        .jobs
        .into_iter()
        .find(|job| job.id == id)
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let lease = pending.handle.pin_candidate(&store, now).unwrap();
    pending
        .handle
        .record_check(
            &store,
            lease.identity(),
            tinytree_store::LandingVerdict {
                passed: true,
                evidence: [44; 32],
            },
            now,
        )
        .unwrap();
    let checked = ordinary.lookup_landing(&start, Some(id)).await.unwrap();
    assert_eq!(checked.phase(), LandingPhase::Checked);
    assert!(checked.receipt().is_none());
    drop(lease);
    pending.handle.finalize(&store, now).unwrap();
    let committed = ordinary.get_landing(&checked.handle()).await.unwrap();
    assert_eq!(committed.phase(), LandingPhase::Committed);
    let receipt = committed.receipt().unwrap();
    assert_eq!(receipt.generation, 2);
    assert_eq!(receipt.destination, "main");
    assert_eq!(receipt.evidence, [44; 32]);

    // The original target moved before Start. This is a durable rejection,
    // without a candidate/check/receipt or a promise to retain source objects.
    let mut preparation_stale = start.clone();
    preparation_stale.idempotency_key = "stale-before-start-sdk".into();
    let stale = ordinary.start_landing(&preparation_stale).await.unwrap();
    assert_eq!(stale.phase(), LandingPhase::StaleTarget);
    assert!(stale.candidate().is_none() && stale.receipt().is_none());
    assert!(!stale.roots_retained());
    let stale_id = stale.handle().id();
    let stale_expiry = stale.expires_at_seconds();
    drop(stale);
    let replay = ordinary
        .lookup_landing(&preparation_stale, Some(stale_id))
        .await
        .unwrap();
    assert_eq!(replay.phase(), LandingPhase::StaleTarget);
    assert_eq!(replay.expires_at_seconds(), stale_expiry);
    drop(replay);

    // A trusted non-verdict terminal result is distinct from failed checks and
    // does not change the already committed reference or fabricate evidence.
    let committed_head = tinytree_model::CommitId(receipt.commit.key.id.0);
    let (_, committed_bytes) = store.read_metadata("example", receipt.commit.key).unwrap();
    let tinytree_model::Object::Commit(mut followup) =
        tinytree_model::decode(&committed_bytes, Default::default()).unwrap()
    else {
        unreachable!()
    };
    followup.parents = vec![committed_head];
    followup.change_id = tinytree_model::ChangeId([20; 16]);
    followup.description = "landing SDK subsequent private draft".into();
    let followup = tinytree_model::encode(
        &tinytree_model::Object::Commit(followup),
        Default::default(),
    )
    .unwrap();
    store
        .put("example", followup.descriptor, &followup.bytes, 0)
        .unwrap();
    let mut unavailable_start = start.clone();
    unavailable_start.idempotency_key = "unavailable-sdk".into();
    unavailable_start.source_base = committed_head;
    unavailable_start.source_head = tinytree_model::CommitId(followup.descriptor.key.id.0);
    unavailable_start.expected_head = committed_head;
    unavailable_start.expected_generation = receipt.generation;
    let accepted = ordinary.start_landing(&unavailable_start).await.unwrap();
    let unavailable_id = accepted.handle().id();
    drop(accepted);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let ready = loop {
        let job = ordinary
            .lookup_landing(&unavailable_start, Some(unavailable_id))
            .await
            .unwrap();
        if job.phase() != LandingPhase::Intent {
            break job;
        }
        drop(job);
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(ready.phase(), LandingPhase::PreparedPendingChecks);
    let saved = store
        .recover_landing_handle("example", unavailable_id)
        .unwrap()
        .unwrap();
    let pin = saved.pin_candidate(&store, now).unwrap();
    store.enable_external_checks().unwrap();
    saved
        .record_check_unavailable(&store, pin.identity(), now)
        .unwrap();
    drop(pin);
    let unavailable = ordinary.get_landing(&ready.handle()).await.unwrap();
    assert_eq!(unavailable.phase(), LandingPhase::CheckUnavailable);
    assert!(!unavailable.roots_retained());
    assert!(unavailable.candidate().is_some());
    assert!(unavailable.receipt().is_none());
    let unchanged_reference = store.references("example", &["main".into()]).unwrap();
    assert_eq!(unchanged_reference[0].generation, 2);
    assert_eq!(unchanged_reference[0].commit, Some(receipt.commit));
    drop(unavailable);
    drop(ready);

    // Deterministic identity fast-forward: freeze the original canonical
    // committer directly through the trusted Store fixture. Server wall-clock
    // metadata is deliberately not synchronized or overridden in production.
    let owner = landing_fixture_owner();
    let mut unchanged = start.clone();
    unchanged.idempotency_key = "unchanged-sdk".into();
    unchanged.destination = "unchanged".into();
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "unchanged-baseline".into(),
                reference_name: unchanged.destination.clone(),
                expected_generation: 0,
                new_commit: Some(unchanged.expected_head),
                staging_leases: vec![],
            },
            now,
        )
        .unwrap();
    let (_, bytes) = store
        .read_metadata(
            "example",
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(unchanged.source_head.0),
            },
        )
        .unwrap();
    let tinytree_model::Object::Commit(source) =
        tinytree_model::decode(&bytes, Default::default()).unwrap()
    else {
        unreachable!()
    };
    let begun = store
        .begin_landing(
            "example",
            owner,
            &tinytree_store::LandingRequest {
                idempotency_key: unchanged.idempotency_key.clone(),
                source_base: unchanged.source_base,
                source_head: unchanged.source_head,
                reference_name: unchanged.destination.clone(),
                expected_generation: unchanged.expected_generation,
                expected_head: unchanged.expected_head,
                committer: source.committer,
                check_profile: tinytree_store::LandingCheckProfile {
                    name: unchanged.profile_name.clone(),
                    version: unchanged.expected_profile_version.clone(),
                },
                expires_at: now + 60,
            },
            now,
        )
        .unwrap();
    let prepared = store
        .prepare_landing("example", owner, &unchanged.idempotency_key, now)
        .unwrap();
    let candidate = prepared.candidate.as_ref().unwrap();
    assert_eq!(candidate.head, unchanged.source_head);
    assert_eq!(candidate.rewrites.len(), 1);
    assert_eq!(candidate.rewrites[0].old, candidate.rewrites[0].new);
    let observed = ordinary
        .lookup_landing(&unchanged, Some(begun.id))
        .await
        .unwrap();
    assert_eq!(observed.phase(), LandingPhase::PreparedPendingChecks);
    assert_eq!(observed.candidate().unwrap().head, unchanged.source_head);
    assert!(
        observed.candidate().unwrap().rewrites.is_empty(),
        "identity constructor pairs are not wire evolution relations"
    );
    store
        .record_landing_check(
            "example",
            owner,
            &unchanged.idempotency_key,
            &tinytree_store::LandingCheckIdentity {
                candidate: candidate.head,
                root_tree: candidate.root_tree,
                profile: prepared.profile,
            },
            tinytree_store::LandingVerdict {
                passed: true,
                evidence: [45; 32],
            },
            now,
        )
        .unwrap();
    store
        .finalize_landing("example", owner, &unchanged.idempotency_key, now)
        .unwrap();
    let committed = ordinary.get_landing(&observed.handle()).await.unwrap();
    assert_eq!(committed.phase(), LandingPhase::Committed);
    assert!(committed.candidate().unwrap().rewrites.is_empty());
    assert!(committed.receipt().unwrap().rewrites.is_empty());
    assert_eq!(
        committed.receipt().unwrap().commit.key.id.0,
        unchanged.source_head.0
    );
    running.stop().await;
}
#[tokio::test]
async fn landing_lost_start_response_recovers_read_only_after_launch_grant_and_profile_removal() {
    use tinytree_client::LandingPreparation;
    use tinytree_proto::landing_v1 as l;
    let (_directory, store, mut config, start) = landing_fixture();
    let running = LandingRunning::start(store.clone(), config.clone(), true, None).await;
    // Accept through the real compressed RPC but lose its response at the SDK's
    // crash boundary. Recovery has the persisted original intent, not a handle.
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", running.address))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut raw = l::landing_client::LandingClient::new(channel)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd)
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
    let mut request = Request::new(l::StartLandingRequest {
        repository_id: "example".into(),
        idempotency_key: start.idempotency_key.clone(),
        source_base: start.source_base.0.to_vec(),
        source_head: start.source_head.0.to_vec(),
        destination: start.destination.clone(),
        expected_generation: start.expected_generation,
        expected_head: start.expected_head.0.to_vec(),
        profile_name: start.profile_name.clone(),
        expected_profile_version: start.expected_profile_version.clone(),
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    drop(raw.start_landing(request).await.unwrap());
    drop(raw);
    running.stop().await;
    config.landing.profiles[0].version = "v2".into();
    let running = LandingRunning::start(store, config, false, None).await;
    let client = NativeClient::connect(running.config()).await.unwrap();
    assert_eq!(
        client.start_landing(&start).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    let recovered = client.lookup_landing(&start, None).await.unwrap();
    assert_eq!(
        recovered.preparation(),
        LandingPreparation::ProfileUnavailable
    );
    let id = recovered.handle().id();
    assert_eq!(
        client
            .lookup_landing(&start, Some([99; 32]))
            .await
            .unwrap_err()
            .code(),
        Code::DataLoss
    );
    let mut wrong = start.clone();
    wrong.expected_profile_version = "v2".into();
    assert_eq!(
        client
            .lookup_landing(&wrong, Some(id))
            .await
            .unwrap_err()
            .code(),
        Code::DataLoss
    );
    assert_eq!(
        client
            .get_landing(&recovered.handle())
            .await
            .unwrap()
            .handle()
            .id(),
        id
    );
    running.stop().await;
}

// Explicit negative wire boundary. Successful preparation is tested above using
// the real service and Store, never this fabricated assertion producer.
#[derive(Clone)]
struct LandingProtocolBoundary {
    response: Arc<std::sync::Mutex<tinytree_proto::landing_v1::LandingResponse>>,
    delay: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
}
#[tonic::async_trait]
impl tinytree_proto::landing_v1::landing_server::Landing for LandingProtocolBoundary {
    async fn start_landing(
        &self,
        request: Request<tinytree_proto::landing_v1::StartLandingRequest>,
    ) -> Result<Response<tinytree_proto::landing_v1::LandingResponse>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            format!("Bearer {TOKEN}").as_str()
        );
        assert_eq!(request.get_ref().repository_id, "example");
        self.entered.notify_one();
        tokio::time::sleep(Duration::from_millis(
            self.delay.load(Ordering::SeqCst) as u64
        ))
        .await;
        Ok(Response::new(self.response.lock().unwrap().clone()))
    }
    async fn get_landing(
        &self,
        request: Request<tinytree_proto::landing_v1::GetLandingRequest>,
    ) -> Result<Response<tinytree_proto::landing_v1::LandingResponse>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            format!("Bearer {TOKEN}").as_str()
        );
        assert_eq!(request.get_ref().repository_id, "example");
        self.entered.notify_one();
        tokio::time::sleep(Duration::from_millis(
            self.delay.load(Ordering::SeqCst) as u64
        ))
        .await;
        Ok(Response::new(self.response.lock().unwrap().clone()))
    }
}
#[tokio::test]
async fn landing_rejects_malformed_and_overbudget_assertions_and_cancellation_releases_owners() {
    use tinytree_model::CommitId;
    use tinytree_proto::landing_v1 as l;
    let input = tinytree_client::LandingStart {
        idempotency_key: "boundary".into(),
        source_base: CommitId([5; 32]),
        source_head: CommitId([1; 32]),
        expected_head: CommitId([6; 32]),
        destination: "main".into(),
        expected_generation: 1,
        profile_name: "checks".into(),
        expected_profile_version: "v1".into(),
    };
    let prepared = l::LandingResponse {
        repository_id: "example".into(),
        idempotency_key: "boundary".into(),
        job: Some(l::LandingJob {
            id: vec![7; 32],
            phase: l::Phase::PreparedPendingChecks as i32,
            roots_retained: true,
            expires_at_seconds: 1,
            profile_name: "checks".into(),
            profile_version: "v1".into(),
            preparation: l::PreparationAvailability::ProfileConfigured as i32,
            candidate: Some(l::Candidate {
                head: vec![2; 32],
                root_tree: vec![4; 32],
                rewrites: vec![l::Rewrite {
                    old_commit: vec![1; 32],
                    new_commit: vec![2; 32],
                    change_id: vec![3; 16],
                }],
            }),
            ..Default::default()
        }),
    };
    let reply = Arc::new(std::sync::Mutex::new(prepared.clone()));
    let delay = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let service = LandingProtocolBoundary {
        response: reply.clone(),
        delay: delay.clone(),
        entered: entered.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                l::landing_server::LandingServer::new(service)
                    .send_compressed(tonic::codec::CompressionEncoding::Zstd)
                    .accept_compressed(tonic::codec::CompressionEncoding::Zstd),
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
    let config = Config {
        endpoint: format!("http://{address}"),
        repository_id: "example".into(),
        token_env: Some("TINYTREE_CLIENT_TEST_TOKEN".into()),
        ..Default::default()
    };
    let client = NativeClient::connect(config).await.unwrap();
    let cases: [(&str, Box<dyn Fn(&mut l::LandingResponse)>); 19] = [
        (
            "preparation stale with retained roots",
            Box::new(|r| {
                let job = r.job.as_mut().unwrap();
                job.phase = l::Phase::StaleTarget as i32;
                job.candidate = None;
            }),
        ),
        (
            "preparation stale with conflict",
            Box::new(|r| {
                let job = r.job.as_mut().unwrap();
                job.phase = l::Phase::StaleTarget as i32;
                job.candidate = None;
                job.roots_retained = false;
                job.conflict_path = Some("file.rs".into());
            }),
        ),
        (
            "unknown phase",
            Box::new(|r| r.job.as_mut().unwrap().phase = 999),
        ),
        (
            "unknown readiness",
            Box::new(|r| r.job.as_mut().unwrap().preparation = 999),
        ),
        (
            "wrong job width",
            Box::new(|r| r.job.as_mut().unwrap().id.pop().map(|_| ()).unwrap()),
        ),
        (
            "wrong profile",
            Box::new(|r| r.job.as_mut().unwrap().profile_version = "other".into()),
        ),
        (
            "wrong repository",
            Box::new(|r| r.repository_id = "other".into()),
        ),
        (
            "wrong replay key",
            Box::new(|r| r.idempotency_key = "other".into()),
        ),
        (
            "intent with candidate",
            Box::new(|r| r.job.as_mut().unwrap().phase = l::Phase::Intent as i32),
        ),
        (
            "unavailable with retained roots",
            Box::new(|r| r.job.as_mut().unwrap().phase = l::Phase::CheckUnavailable as i32),
        ),
        (
            "prepared without retained roots",
            Box::new(|r| r.job.as_mut().unwrap().roots_retained = false),
        ),
        (
            "prepared without candidate",
            Box::new(|r| r.job.as_mut().unwrap().candidate = None),
        ),
        (
            "committed without receipt",
            Box::new(|r| r.job.as_mut().unwrap().phase = l::Phase::Committed as i32),
        ),
        (
            "duplicate relation",
            Box::new(|r| {
                let c = r.job.as_mut().unwrap().candidate.as_mut().unwrap();
                c.rewrites.push(c.rewrites[0].clone());
            }),
        ),
        (
            "self relation",
            Box::new(|r| {
                let c = r.job.as_mut().unwrap().candidate.as_mut().unwrap();
                c.rewrites[0].new_commit = c.rewrites[0].old_commit.clone();
            }),
        ),
        (
            "head contradicts relation",
            Box::new(|r| r.job.as_mut().unwrap().candidate.as_mut().unwrap().head = vec![9; 32]),
        ),
        (
            "conflicting source relation",
            Box::new(|r| {
                r.job
                    .as_mut()
                    .unwrap()
                    .candidate
                    .as_mut()
                    .unwrap()
                    .rewrites
                    .push(l::Rewrite {
                        old_commit: vec![1; 32],
                        new_commit: vec![8; 32],
                        change_id: vec![9; 16],
                    })
            }),
        ),
        (
            "conflicting target relation",
            Box::new(|r| {
                r.job
                    .as_mut()
                    .unwrap()
                    .candidate
                    .as_mut()
                    .unwrap()
                    .rewrites
                    .push(l::Rewrite {
                        old_commit: vec![8; 32],
                        new_commit: vec![2; 32],
                        change_id: vec![9; 16],
                    })
            }),
        ),
        (
            "conflicting change relation",
            Box::new(|r| {
                r.job
                    .as_mut()
                    .unwrap()
                    .candidate
                    .as_mut()
                    .unwrap()
                    .rewrites
                    .push(l::Rewrite {
                        old_commit: vec![8; 32],
                        new_commit: vec![9; 32],
                        change_id: vec![3; 16],
                    })
            }),
        ),
    ];
    for (name, change) in cases {
        let mut invalid = prepared.clone();
        change(&mut invalid);
        *reply.lock().unwrap() = invalid;
        assert!(
            matches!(
                client.start_landing(&input).await.unwrap_err().code(),
                Code::DataLoss | Code::ResourceExhausted
            ),
            "{name}"
        );
    }
    let mut oversized = prepared.clone();
    oversized
        .job
        .as_mut()
        .unwrap()
        .candidate
        .as_mut()
        .unwrap()
        .rewrites = vec![
        prepared
            .job
            .as_ref()
            .unwrap()
            .candidate
            .as_ref()
            .unwrap()
            .rewrites[0]
            .clone();
        129
    ];
    *reply.lock().unwrap() = oversized;
    assert_eq!(
        client.start_landing(&input).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    let mut unchanged = prepared.clone();
    unchanged.job.as_mut().unwrap().candidate = Some(l::Candidate {
        head: input.source_head.0.to_vec(),
        root_tree: vec![4; 32],
        rewrites: vec![],
    });
    *reply.lock().unwrap() = unchanged;
    assert!(
        client
            .start_landing(&input)
            .await
            .unwrap()
            .candidate()
            .unwrap()
            .rewrites
            .is_empty(),
        "wire filtering preserves an unchanged candidate without self evolution edges"
    );
    *reply.lock().unwrap() = prepared;
    delay.store(100, Ordering::SeqCst);
    let dropped = Arc::new(AtomicUsize::new(0));
    let transient = client
        .clone()
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(dropped.clone())));
    let _ = futures_util::FutureExt::now_or_never(entered.notified());
    let input_copy = input.clone();
    let task = tokio::spawn(async move { transient.lookup_landing(&input_copy, None).await });
    entered.notified().await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    task.abort();
    let _ = task.await;
    assert_released(&dropped).await;
    let bounded = client
        .clone()
        .with_operation_deadline(tokio::time::Instant::now() + Duration::from_millis(30));
    let began = std::time::Instant::now();
    assert_eq!(
        bounded
            .lookup_landing(&input, None)
            .await
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );
    assert!(began.elapsed() < Duration::from_millis(250));
    delay.store(0, Ordering::SeqCst);
    // A following call proves the cancelled operation did not strand shared
    // physical memory or inflight admission, independently of result ownership.
    assert!(client.lookup_landing(&input, None).await.is_ok());
    let _ = stop.send(());
    server.await.unwrap();
}

// Transport-only response loss wraps the real authenticated service; it does
// not manufacture accepted intents, candidate data or successful job responses.
#[derive(Clone)]
struct LandingLostReplies {
    inner: tinytree_server::LandingGrpcService,
    calls: Arc<AtomicUsize>,
    losses: usize,
}
impl tonic::server::NamedService for LandingLostReplies {
    const NAME: &'static str = "tinytree.landing.v1.Landing";
}
impl tower::Service<http::Request<tonic::body::Body>> for LandingLostReplies {
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
        let start = request.uri().path().ends_with("/StartLanding");
        let attempt = if start {
            self.calls.fetch_add(1, Ordering::SeqCst)
        } else {
            usize::MAX
        };
        let lose = attempt < self.losses;
        let future = self.inner.call(request);
        Box::pin(async move {
            let response = future.await?;
            if lose {
                drop(response);
                Ok(http::Response::builder()
                    .header("content-type", "application/grpc")
                    .header("grpc-status", "14")
                    .header("grpc-message", "accepted%20response%20lost")
                    .body(tonic::body::Body::empty())
                    .unwrap())
            } else {
                Ok(response)
            }
        })
    }
}
#[tokio::test]
async fn landing_start_response_loss_retries_one_stable_intent_under_original_deadline() {
    for (name, losses, deadline_ms) in [
        ("replay accepted intent", 1, 1000),
        ("whole retry deadline", usize::MAX, 70),
    ] {
        let (_directory, store, config, start) = landing_fixture();
        let service = RepositoryService::new(
            store.clone(),
            config,
            vec!["example".into()],
            vec![
                Principal::new("test", TOKEN, vec!["example".into()], true)
                    .unwrap()
                    .with_landing_profiles(vec!["checks".into()]),
            ],
        )
        .unwrap();
        service.start_landing_worker().unwrap();
        let draining = service.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let wrapper = LandingLostReplies {
            inner: service.landing_grpc_service(),
            calls: calls.clone(),
            losses,
        };
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(wrapper)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
            draining.drain_landing().await;
        });
        let mut config = Config {
            endpoint: format!("http://{address}"),
            repository_id: "example".into(),
            token_env: Some("TINYTREE_CLIENT_TEST_TOKEN".into()),
            ..Default::default()
        };
        config.rpc.operation_timeout_ms = deadline_ms;
        config.rpc.rpc_timeout_ms = 20;
        config.rpc.retry.max_attempts = 16;
        config.rpc.retry.initial_backoff_ms = 30;
        config.rpc.retry.max_backoff_ms = 30;
        let client = NativeClient::connect(config).await.unwrap();
        let began = std::time::Instant::now();
        let result = client.start_landing(&start).await;
        if losses == 1 {
            let job = result.unwrap();
            assert!(calls.load(Ordering::SeqCst) >= 2, "{name}");
            let replay = client.start_landing(&start).await.unwrap();
            assert_eq!(replay.handle().id(), job.handle().id());
            let mut conflict = start.clone();
            conflict.expected_profile_version = "different".into();
            assert_eq!(
                client.start_landing(&conflict).await.unwrap_err().code(),
                Code::AlreadyExists
            );
        } else {
            assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
            assert!(began.elapsed() < Duration::from_millis(250), "{name}");
            assert!(
                calls.load(Ordering::SeqCst) < 16,
                "retry must not obtain a fresh operation deadline"
            );
        }
        let jobs = store
            .scan_pending_landings("example", None, Default::default())
            .unwrap();
        assert_eq!(
            jobs.scanned_records, 1,
            "{name}: retries created more than one durable intent"
        );
        let _ = stop.send(());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn landing_independent_pool_reuses_authenticated_transport_and_preserves_ordinary_limits() {
    let (_directory, store, service, start) = landing_fixture();
    let running = LandingRunning::start(store, service, true, None).await;
    let mut config = running.config();
    config.limits.max_buffered_bytes = 32 * 1024 * 1024;
    config.limits.max_batch_bytes = 4 * 1024 * 1024;
    config.limits.max_metadata_bytes = 4 * 1024 * 1024;
    config.limits.max_graph_bytes = 8 * 1024 * 1024;
    let original = NativeClient::connect(config).await.unwrap();
    assert_eq!(
        original.start_landing(&start).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        original
            .clone()
            .with_landing_budget(32 * 1024 * 1024)
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        original
            .clone()
            .with_landing_budget(257 * 1024 * 1024)
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    let budgeted = original
        .clone()
        .with_landing_budget(256 * 1024 * 1024)
        .unwrap();
    let accepted = budgeted.start_landing(&start).await.unwrap();
    let handle = accepted.handle();
    let id = handle.id();
    drop(accepted);
    assert_eq!(
        budgeted.get_landing(&handle).await.unwrap().handle().id(),
        id
    );
    assert_eq!(
        budgeted
            .lookup_landing(&start, Some(id))
            .await
            .unwrap()
            .handle()
            .id(),
        id
    );
    // Same Inner accepts the handle, but the original's small pool remains
    // unchanged. A reconnect has different authenticated handle scope entirely.
    assert_eq!(
        original.get_landing(&handle).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    let foreign = NativeClient::connect(running.config())
        .await
        .unwrap()
        .with_landing_budget(256 * 1024 * 1024)
        .unwrap();
    assert_eq!(
        foreign.get_landing(&handle).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    running.stop().await;
}

#[tokio::test]
async fn checked_regex_search_pages_preserve_variable_width_spans_and_reject_malformed_hits() {
    for mode in [
        QueryMode::Normal,
        QueryMode::SearchZeroWidth,
        QueryMode::SearchMissingMatchSpan,
        QueryMode::SearchLongMatch,
        QueryMode::SearchDuplicateHit,
        QueryMode::SearchOverflow,
        QueryMode::SearchRegressedHit,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let mut query = client
            .begin_search(
                &scope,
                fixture.root,
                b"cc_binary|rules_cc",
                "BUILD.bazel",
                q::SearchPolicy::RegexBytesV1,
                if mode == QueryMode::SearchDuplicateHit {
                    2
                } else {
                    1
                },
            )
            .await
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            let mut hits = Vec::new();
            while !query.is_terminal() {
                let page = client.search_page(&mut query).await?;
                hits.extend(page.result().hits.clone());
                if query.is_warming() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            Ok::<_, Status>(hits)
        })
        .await
        .expect("bounded regex index warming");
        if mode == QueryMode::Normal {
            let hits = result.unwrap();
            assert!(query.is_complete());
            let seed = crate::fixture();
            let source = seed
                .objects()
                .iter()
                .find(|object| {
                    object.descriptor.key.kind == Kind::File && object.bytes.starts_with(b"load(")
                })
                .unwrap();
            let mut expected = Vec::new();
            for literal in [&b"cc_binary"[..], &b"rules_cc"[..]] {
                expected.extend(source.bytes.windows(literal.len()).enumerate().filter_map(
                    |(at, bytes)| (bytes == literal).then_some((at as u64, literal.len() as u32)),
                ));
            }
            expected.sort();
            assert_eq!(
                hits.iter()
                    .map(|hit| (hit.byte_offset, hit.match_bytes))
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(hits.iter().any(|hit| hit.match_bytes == 8));
            assert!(hits.iter().any(|hit| hit.match_bytes == 9));
        } else {
            assert_eq!(result.unwrap_err().code(), Code::DataLoss, "{mode:?}");
        }
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn small_query_replies_share_the_metadata_pool_without_waiting_for_a_page() {
    let fixture = setup_query_retained();
    let running = Running::start(fixture.service.clone(), Mode::PausedMetadata).await;
    let mut config = running.config();
    config.limits.max_metadata_bytes = 4 * 1024 * 1024;
    config.limits.max_batch_bytes = 4 * 1024 * 1024;
    config.limits.max_graph_bytes = 8 * 1024 * 1024;
    config.limits.max_buffered_bytes = 32 * 1024 * 1024;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let guarded = client
        .clone()
        .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
    let key = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(fixture.root.0),
    };
    let background = tokio::spawn(async move { guarded.read_objects(vec![key]).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while running.metadata.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let facts = tokio::time::timeout(
        Duration::from_millis(500),
        client.query_facts(&scope, vec![QueryFact::HasId(fixture.root)]),
    )
    .await
    .expect("scalar facts must coexist with a physically paused metadata page")
    .unwrap();
    assert_eq!(facts, vec![FactAnswer::HasId(true)]);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            client.shortest_prefix(&scope, PrefixTarget::Commit(fixture.root), vec![])
        )
        .await
        .expect("shortest prefix must coexist with a physically paused metadata page")
        .unwrap()
            <= 64
    );
    // The variable-reply path keeps the full page allowance and still waits.
    let general = client.query_facts(&scope, vec![QueryFact::Heads(vec![fixture.root])]);
    tokio::pin!(general);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut general)
            .await
            .is_err()
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    running.metadata_release.notify_one();
    assert_eq!(background.await.unwrap().unwrap().len(), 1);
    assert_eq!(
        general.await.unwrap(),
        vec![FactAnswer::Heads(vec![fixture.root])]
    );
    assert_released(&drops).await;
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn small_query_reply_caps_reject_oversize_and_predecode_collection_expansion() {
    for mode in [
        QueryMode::FactOversizedReply,
        QueryMode::FactAllocationFlood,
        QueryMode::FactWrongVariantEmptyIds,
        QueryMode::ShortestOversizedReply,
        QueryMode::SmallUncompressed,
    ] {
        let fixture = setup_query_retained();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let guarded = client
            .clone()
            .with_rpc_lifetime_guard(Arc::new(LifetimeProbe(drops.clone())));
        if mode == QueryMode::ShortestOversizedReply {
            assert_eq!(
                guarded
                    .shortest_prefix(&scope, PrefixTarget::Commit(fixture.root), vec![])
                    .await
                    .unwrap_err()
                    .code(),
                Code::OutOfRange
            );
        } else {
            let result = guarded
                .query_facts(&scope, vec![QueryFact::HasId(fixture.root)])
                .await;
            match mode {
                QueryMode::FactOversizedReply => {
                    assert_eq!(result.unwrap_err().code(), Code::OutOfRange)
                }
                QueryMode::FactAllocationFlood => {
                    assert_eq!(result.unwrap_err().code(), Code::ResourceExhausted)
                }
                QueryMode::FactWrongVariantEmptyIds => {
                    let status = result.unwrap_err();
                    assert_eq!(status.code(), Code::DataLoss);
                    assert!(status.message().contains("bounded shape"));
                }
                QueryMode::SmallUncompressed => {
                    assert_eq!(result.unwrap(), vec![FactAnswer::HasId(true)]);
                    guarded
                        .shortest_prefix(&scope, PrefixTarget::Commit(fixture.root), vec![])
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }
        }
        drop(guarded);
        assert_released(&drops).await;
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
}

#[tokio::test]
async fn checked_history_collection_uses_same_scope_and_bounded_positive_progress() {
    let fixture = setup_history_chain();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::Normal).await;
    let mut config = running.config.clone();
    config.limits.max_inflight = 1;
    let client = NativeClient::connect(config).await.unwrap();
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let one = client
        .path_history(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::LinearFixedPathV1,
            8,
        )
        .await
        .unwrap();
    assert_eq!(one.result().scanned_commits, 1);
    assert!(one.result().entries.is_empty());
    assert_eq!(running.counts.histories.load(Ordering::SeqCst), 1);
    let limited = client
        .collect_path_history(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::LinearFixedPathV1,
            2,
        )
        .await
        .unwrap();
    assert_eq!(limited.result().revision, one.result().revision);
    assert_eq!(limited.result().scanned_commits, 2);
    assert_eq!(limited.result().entries.len(), 1);
    assert_eq!(
        limited.result().coverage,
        q::HistoryCoverage::HistoryScanLimit as i32
    );
    assert_eq!(running.counts.histories.load(Ordering::SeqCst), 3);
    let all = client
        .collect_path_history(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::LinearFixedPathV1,
            8,
        )
        .await
        .unwrap();
    assert_eq!(all.result().revision, one.result().revision);
    assert_eq!(all.result().scanned_commits, 4);
    assert_eq!(all.result().entries.len(), 3);
    assert_eq!(
        all.result().coverage,
        q::HistoryCoverage::HistoryComplete as i32
    );
    assert!(all.result().boundary.is_none());
    assert_eq!(running.counts.histories.load(Ordering::SeqCst), 7);
    let before = running.counts.histories.load(Ordering::SeqCst);
    for (total, code) in [
        (0, Code::InvalidArgument),
        (u32::MAX, Code::ResourceExhausted),
        (65536, Code::ResourceExhausted),
    ] {
        assert_eq!(
            client
                .collect_path_history(
                    &scope,
                    fixture.root,
                    "BUILD.bazel",
                    q::HistoryPolicy::LinearFixedPathV1,
                    total
                )
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    assert_eq!(running.counts.histories.load(Ordering::SeqCst), before);
    client.close_query_scope(&scope).await.unwrap();
    assert_eq!(
        client
            .collect_path_history(
                &scope,
                fixture.root,
                "BUILD.bazel",
                q::HistoryPolicy::LinearFixedPathV1,
                8
            )
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    running.stop().await;
}

#[tokio::test]
async fn checked_history_collection_rejects_frontier_echo_overlap_and_expired_scope() {
    for (mode, expected, requests) in [
        (QueryMode::HistoryZeroProgress, Code::DataLoss, 1),
        (QueryMode::HistoryRepeatedFrontier, Code::DataLoss, 1),
        (QueryMode::HistoryPreviousEntry, Code::DataLoss, 3),
        (QueryMode::HistorySecondWrongRevision, Code::DataLoss, 2),
        (
            QueryMode::HistoryCloseAfterPage,
            Code::FailedPrecondition,
            2,
        ),
    ] {
        let fixture = setup_history_chain();
        let running = QueryRunning::start(fixture.service.clone(), mode).await;
        let client = running.client().await;
        let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
        let error = client
            .collect_path_history(
                &scope,
                fixture.root,
                "BUILD.bazel",
                q::HistoryPolicy::LinearFixedPathV1,
                8,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), expected, "{mode:?}: {error}");
        assert_eq!(
            running.counts.histories.load(Ordering::SeqCst),
            requests,
            "{mode:?}"
        );
        client.close_query_scope(&scope).await.unwrap();
        running.stop().await;
    }
    let fixture = setup_history_chain();
    let running =
        QueryRunning::start(fixture.service.clone(), QueryMode::HistoryTerminalReadLimit).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let result = client
        .collect_path_history(
            &scope,
            fixture.root,
            "BUILD.bazel",
            q::HistoryPolicy::LinearFixedPathV1,
            8,
        )
        .await
        .unwrap();
    assert_eq!(
        result.result().coverage,
        q::HistoryCoverage::HistoryReadLimit as i32
    );
    assert_eq!(running.counts.histories.load(Ordering::SeqCst), 1);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn checked_history_collection_keeps_one_absolute_deadline() {
    let fixture = setup_history_chain();
    let running = QueryRunning::start(fixture.service.clone(), QueryMode::HistorySlowPages).await;
    let client = running.client().await;
    let scope = client.open_query_scope(vec![fixture.root]).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(130);
    let bounded = client.clone().with_operation_deadline(deadline);
    assert_eq!(
        bounded
            .collect_path_history(
                &scope,
                fixture.root,
                "BUILD.bazel",
                q::HistoryPolicy::LinearFixedPathV1,
                8
            )
            .await
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );
    assert!(running.counts.histories.load(Ordering::SeqCst) <= 2);
    client.close_query_scope(&scope).await.unwrap();
    running.stop().await;
}
