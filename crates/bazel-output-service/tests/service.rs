use bazel_output_service::{
    Config, OutputService, file_locator,
    proto::{
        self, bazel_output_service_client::BazelOutputServiceClient,
        bazel_output_service_server::BazelOutputServiceServer,
    },
    rev2,
};
use prost::Message;
use reapi::{Digest, proto::google::bytestream as bs, reapi as cas_proto};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_stream::{Stream, wrappers::TcpListenerStream};
use tonic::{Request, Response, Status};

fn test_policy(timeout: Duration) -> reapi::policy::Policy {
    let mut policy = reapi::policy::Policy::default();
    policy.rpc_timeout_ms = timeout.as_millis() as u64;
    policy.stream_idle_timeout_ms = timeout.as_millis() as u64;
    policy.operation_timeout_ms = timeout.as_millis() as u64;
    policy
}

#[derive(Clone, Default)]
struct Cas {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    reads: Arc<Mutex<Vec<String>>>,
    stall: Arc<AtomicBool>,
    transient_reads: Arc<AtomicUsize>,
    presence_queries: Arc<Mutex<Vec<cas_proto::FindMissingBlobsRequest>>>,
    zstd: Arc<AtomicBool>,
    capabilities_implemented: Arc<AtomicBool>,
    capabilities_queries: Arc<AtomicUsize>,
    compressed_metadata_requests: Arc<AtomicUsize>,
    metadata_rejection: Arc<AtomicUsize>,
    wire_override: Arc<Mutex<Option<Vec<u8>>>>,
    read_fragment_bytes: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl cas_proto::capabilities_server::Capabilities for Cas {
    async fn get_capabilities(
        &self,
        request: Request<cas_proto::GetCapabilitiesRequest>,
    ) -> Result<Response<cas_proto::ServerCapabilities>, Status> {
        assert_eq!(
            request.metadata().get("x-buildbuddy-api-key").unwrap(),
            "test-key"
        );
        assert_eq!(request.into_inner().instance_name, "fixture");
        self.capabilities_queries.fetch_add(1, Ordering::SeqCst);
        let zstd = self.zstd.load(Ordering::SeqCst);
        if !zstd && !self.capabilities_implemented.load(Ordering::SeqCst) {
            return Err(Status::unimplemented("legacy CAS"));
        }
        let mut response = Response::new(cas_proto::ServerCapabilities {
            cache_capabilities: Some(cas_proto::CacheCapabilities {
                supported_compressors: if zstd { vec![1] } else { vec![] },
                ..Default::default()
            }),
            ..Default::default()
        });
        response
            .metadata_mut()
            .insert("grpc-accept-encoding", "gzip".parse().unwrap());
        Ok(response)
    }
}
#[tonic::async_trait]
impl cas_proto::content_addressable_storage_server::ContentAddressableStorage for Cas {
    type GetTreeStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<cas_proto::GetTreeResponse, Status>> + Send>>;
    type GetChunkMappingStream = std::pin::Pin<
        Box<dyn Stream<Item = Result<cas_proto::GetChunkMappingResponse, Status>> + Send>,
    >;
    async fn find_missing_blobs(
        &self,
        request: Request<cas_proto::FindMissingBlobsRequest>,
    ) -> Result<Response<cas_proto::FindMissingBlobsResponse>, Status> {
        if request
            .metadata()
            .get("x-buildbuddy-api-key")
            .and_then(|v| v.to_str().ok())
            != Some("test-key")
        {
            return Err(Status::unauthenticated("missing credential"));
        }
        if request
            .metadata()
            .get("grpc-encoding")
            .is_some_and(|value| value == "gzip")
        {
            self.compressed_metadata_requests
                .fetch_add(1, Ordering::SeqCst);
            match self.metadata_rejection.load(Ordering::SeqCst) {
                1 => {
                    let mut status = Status::unimplemented("unsupported request codec");
                    status
                        .metadata_mut()
                        .insert("grpc-accept-encoding", "identity".parse().unwrap());
                    return Err(status);
                }
                2 => return Err(Status::unauthenticated("fixture authentication rejection")),
                3 => return Err(Status::unimplemented("fixture method rejection")),
                _ => {}
            }
        }
        let request = request.into_inner();
        self.presence_queries.lock().unwrap().push(request.clone());
        let data = self.data.lock().unwrap();
        let missing_blob_digests = request
            .blob_digests
            .into_iter()
            .filter(|digest| {
                !data.contains_key(&format!(
                    "{}/blobs/{}{}/{}",
                    request.instance_name,
                    if request.digest_function == 9 {
                        "blake3/"
                    } else {
                        ""
                    },
                    digest.hash,
                    digest.size_bytes
                ))
            })
            .collect();
        Ok(Response::new(cas_proto::FindMissingBlobsResponse {
            missing_blob_digests,
        }))
    }
    async fn batch_update_blobs(
        &self,
        _: Request<cas_proto::BatchUpdateBlobsRequest>,
    ) -> Result<Response<cas_proto::BatchUpdateBlobsResponse>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn batch_read_blobs(
        &self,
        _: Request<cas_proto::BatchReadBlobsRequest>,
    ) -> Result<Response<cas_proto::BatchReadBlobsResponse>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn get_tree(
        &self,
        _: Request<cas_proto::GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn split_blob(
        &self,
        _: Request<cas_proto::SplitBlobRequest>,
    ) -> Result<Response<cas_proto::SplitBlobResponse>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn get_chunk_mapping(
        &self,
        _: Request<cas_proto::GetChunkMappingRequest>,
    ) -> Result<Response<Self::GetChunkMappingStream>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn splice_blob(
        &self,
        _: Request<cas_proto::SpliceBlobRequest>,
    ) -> Result<Response<cas_proto::SpliceBlobResponse>, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn register_chunk_mapping(
        &self,
        _: Request<tonic::Streaming<cas_proto::RegisterChunkMappingRequest>>,
    ) -> Result<Response<cas_proto::RegisterChunkMappingResponse>, Status> {
        Err(Status::unimplemented("fixture"))
    }
}
#[tonic::async_trait]
impl bs::byte_stream_server::ByteStream for Cas {
    type ReadStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<bs::ReadResponse, Status>> + Send>>;
    async fn read(
        &self,
        request: Request<bs::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        if request
            .metadata()
            .get("x-buildbuddy-api-key")
            .and_then(|v| v.to_str().ok())
            != Some("test-key")
        {
            return Err(Status::unauthenticated("missing credential"));
        }
        assert!(
            request.metadata().get("grpc-encoding").is_none(),
            "ByteStream must not apply gRPC compression over compressed blobs"
        );
        let name = request.into_inner().resource_name;
        self.reads.lock().unwrap().push(name.clone());
        let compressed = name.contains("/compressed-blobs/zstd/");
        let identity_name = name.replace("/compressed-blobs/zstd/", "/blobs/");
        let bytes = self
            .data
            .lock()
            .unwrap()
            .get(&identity_name)
            .cloned()
            .ok_or_else(|| Status::not_found("missing fixture blob"))?;
        if self
            .transient_reads
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Ok(Response::new(Box::pin(tokio_stream::iter(vec![
                Ok(bs::ReadResponse {
                    data: bytes[..bytes.len().min(2)].to_vec(),
                }),
                Err(Status::unavailable("transient fixture disconnect")),
            ]))));
        }
        if self.stall.load(Ordering::SeqCst) {
            return Ok(Response::new(Box::pin(tokio_stream::pending())));
        }
        let bytes = if compressed {
            self.wire_override
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| reapi::compression::encode_zstd(&bytes).unwrap())
        } else {
            bytes
        };
        let fragment = self.read_fragment_bytes.load(Ordering::SeqCst);
        let chunks: Vec<_> = bytes
            .chunks(if fragment == 0 { 3 } else { fragment })
            .map(|chunk| {
                Ok(bs::ReadResponse {
                    data: chunk.to_vec(),
                })
            })
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
    }
    async fn write(
        &self,
        _: Request<tonic::Streaming<bs::WriteRequest>>,
    ) -> Result<Response<bs::WriteResponse>, Status> {
        Err(Status::unimplemented("readonly fixture"))
    }
    async fn query_write_status(
        &self,
        _: Request<bs::QueryWriteStatusRequest>,
    ) -> Result<Response<bs::QueryWriteStatusResponse>, Status> {
        Err(Status::unimplemented("readonly fixture"))
    }
}
struct Fixture {
    _dir: tempfile::TempDir,
    outside: tempfile::TempDir,
    cas: Cas,
    client: BazelOutputServiceClient<tonic::transport::Channel>,
    endpoint: String,
    service: OutputService,
    servers: Vec<tokio::task::JoinHandle<()>>,
    projection: Option<Arc<HeadlessProjection>>,
}

// This exercises BOS's real CAS adapter without substituting for native mount tests.
struct HeadlessProjection {
    tree: Arc<output_core::Tree>,
    root: std::path::PathBuf,
    _state: tempfile::TempDir,
    refresh_gate: Mutex<Option<(String, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
}
#[async_trait::async_trait]
impl output_core::Projection for HeadlessProjection {
    fn root(&self) -> &std::path::Path {
        &self.root
    }
    async fn ensure_dir(&self, p: &str) -> anyhow::Result<()> {
        self.tree.ensure_dir(p).await?;
        Ok(())
    }
    async fn stage(&self, p: &str, a: output_core::Artifact) -> anyhow::Result<()> {
        self.tree.stage(p, a).await?;
        Ok(())
    }
    async fn stat(&self, p: &str, f: i32) -> anyhow::Result<Option<output_core::TreeStat>> {
        self.tree.stat(p, f).await
    }
    async fn refresh(&self, p: &str) -> anyhow::Result<()> {
        let gate = self.refresh_gate.lock().unwrap().clone();
        if let Some((path, entered, release)) = gate {
            if path == p {
                entered.notify_one();
                release.notified().await;
            }
        }
        self.tree.refresh(p).await?;
        Ok(())
    }
    async fn clean(&self, p: &str) -> anyhow::Result<()> {
        self.tree.clean(p).await
    }
    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}
impl Fixture {
    async fn new() -> Self {
        Self::configured(false, Duration::from_secs(10)).await
    }
    async fn configured(lazy: bool, timeout: Duration) -> Self {
        Self::configured_limit(lazy, timeout, 1024 * 1024).await
    }
    async fn configured_limit(lazy: bool, timeout: Duration, max_blob_bytes: u64) -> Self {
        Self::configured_policy(lazy, test_policy(timeout), max_blob_bytes).await
    }
    async fn configured_policy(
        lazy: bool,
        policy: reapi::policy::Policy,
        max_blob_bytes: u64,
    ) -> Self {
        Self::configured_client_endpoint(lazy, policy, max_blob_bytes, None).await
    }
    async fn configured_client_endpoint(
        lazy: bool,
        policy: reapi::policy::Policy,
        max_blob_bytes: u64,
        client_endpoint: Option<&str>,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let cas = Cas::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let cas_service = cas.clone();
        let cas_presence = cas.clone();
        let cas_capabilities = cas.clone();
        let cas_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(bs::byte_stream_server::ByteStreamServer::new(cas_service).send_compressed(tonic::codec::CompressionEncoding::Gzip))
                .add_service(cas_proto::content_addressable_storage_server::ContentAddressableStorageServer::new(cas_presence).accept_compressed(tonic::codec::CompressionEncoding::Gzip).send_compressed(tonic::codec::CompressionEncoding::Gzip))
                .add_service(cas_proto::capabilities_server::CapabilitiesServer::new(cas_capabilities).accept_compressed(tonic::codec::CompressionEncoding::Gzip).send_compressed(tonic::codec::CompressionEncoding::Gzip))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let service = OutputService::new_with_client_endpoint(
            Config {
                root: dir.path().to_path_buf(),
                cas_endpoint: endpoint.clone(),
                instance_name: "fixture".into(),
                api_key: Some("test-key".into()),
                max_blob_bytes,
                policy,
            },
            client_endpoint.unwrap_or(&endpoint),
        )
        .await
        .unwrap();
        let endpoint = client_endpoint.unwrap_or(&endpoint).to_owned();
        let projection = if lazy {
            let state = tempfile::tempdir().unwrap();
            let tree = output_core::Tree::open(
                state.path(),
                service.backend_identity(),
                service.blob_source(),
                output_core::CacheConfig::default(),
            )
            .unwrap();
            Some(Arc::new(HeadlessProjection {
                tree,
                root: dir.path().canonicalize().unwrap(),
                _state: state,
                refresh_gate: Mutex::new(None),
            }))
        } else {
            None
        };
        let service = if let Some(projection) = &projection {
            service
                .with_projection(projection.clone(), projection.tree.clone())
                .unwrap()
        } else {
            service
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let service_endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service_clone = service.clone();
        let service_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    BazelOutputServiceServer::new(service_clone)
                        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                        .send_compressed(tonic::codec::CompressionEncoding::Gzip),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let client = BazelOutputServiceClient::connect(service_endpoint)
            .await
            .unwrap();
        Self {
            _dir: dir,
            outside: tempfile::tempdir().unwrap(),
            cas,
            client,
            endpoint,
            service,
            servers: vec![cas_task, service_task],
            projection,
        }
    }
    fn request(&self, id: &str, function: i32) -> proto::StartBuildRequest {
        proto::StartBuildRequest {
            version: 1,
            output_base_id: "base".into(),
            build_id: id.into(),
            args: Some(prost_types::Any {
                type_url: "type.googleapis.com/bazel_output_service_rev2.StartBuildArgs".into(),
                value: rev2::StartBuildArgs {
                    remote_cache: self.endpoint.clone(),
                    instance_name: "fixture".into(),
                    digest_function: function,
                }
                .encode_to_vec(),
            }),
            output_path_prefix: String::new(),
            output_path_aliases: HashMap::new(),
        }
    }
    fn blob(&self, data: &[u8], function: i32) -> Digest {
        let hash = if function == 9 {
            blake3::hash(data).to_hex().to_string()
        } else {
            format!("{:x}", Sha256::digest(data))
        };
        let digest = Digest {
            hash,
            size_bytes: data.len() as i64,
        };
        self.cas.data.lock().unwrap().insert(
            format!(
                "fixture/blobs/{}{}/{}",
                if function == 9 { "blake3/" } else { "" },
                digest.hash,
                digest.size_bytes
            ),
            data.to_vec(),
        );
        digest
    }
    async fn stage(&mut self, id: &str, path: &str, digest: Digest) -> i32 {
        self.client
            .stage_artifacts(proto::StageArtifactsRequest {
                build_id: id.into(),
                artifacts: vec![proto::stage_artifacts_request::Artifact {
                    path: path.into(),
                    locator: Some(file_locator(digest)),
                }],
            })
            .await
            .unwrap()
            .into_inner()
            .responses[0]
            .status
            .as_ref()
            .unwrap()
            .code
    }
}

#[tokio::test]
async fn grpc_lifecycle_verifies_sha256_and_blake3_and_preserves_outputs() {
    let mut fixture = Fixture::new().await;
    for function in [1, 9] {
        let id = format!("build-{function}");
        let mut request = fixture.request(&id, function);
        request.output_path_prefix = fixture._dir.path().to_string_lossy().into_owned();
        let response = fixture
            .client
            .start_build(request)
            .await
            .unwrap()
            .into_inner();
        assert!(response.initial_output_path_contents.is_none());
        assert!(std::path::Path::new(&response.output_path_suffix).is_relative());
        let root = fixture._dir.path().join(response.output_path_suffix);
        let digest = fixture.blob(b"remote output payload", function);
        assert_eq!(fixture.stage(&id, "nested/output", digest.clone()).await, 0);
        assert_eq!(
            std::fs::read(root.join("nested/output")).unwrap(),
            b"remote output payload"
        );
        let stats = fixture
            .client
            .batch_stat(proto::BatchStatRequest {
                build_id: id.clone(),
                paths: vec!["nested/output".into(), "nested".into(), "absent".into()],
            })
            .await
            .unwrap()
            .into_inner();
        match stats.responses[0]
            .stat
            .as_ref()
            .unwrap()
            .r#type
            .as_ref()
            .unwrap()
        {
            proto::batch_stat_response::stat::Type::File(file) => assert_eq!(
                rev2::FileArtifactLocator::decode(file.locator.as_ref().unwrap().value.as_slice())
                    .unwrap()
                    .digest
                    .unwrap(),
                digest
            ),
            _ => panic!("not a file"),
        }
        assert!(matches!(
            stats.responses[1].stat.as_ref().unwrap().r#type,
            Some(proto::batch_stat_response::stat::Type::Directory(_))
        ));
        assert!(stats.responses[2].stat.is_none());
        fixture
            .client
            .finalize_artifacts(proto::FinalizeArtifactsRequest {
                build_id: id.clone(),
                artifacts: vec![proto::finalize_artifacts_request::Artifact {
                    path: "nested/output".into(),
                    locator: Some(file_locator(digest)),
                }],
            })
            .await
            .unwrap();
        fixture
            .client
            .finalize_build(proto::FinalizeBuildRequest {
                build_id: id.clone(),
                build_successful: true,
            })
            .await
            .unwrap();
        assert_eq!(
            fixture
                .client
                .batch_stat(proto::BatchStatRequest {
                    build_id: id.clone(),
                    paths: vec![]
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        std::fs::write(root.join("nested/output"), b"local edit").unwrap();
        let response = fixture
            .client
            .start_build(fixture.request("next", function))
            .await
            .unwrap()
            .into_inner();
        assert!(
            response.initial_output_path_contents.is_none(),
            "must not trust unchanged state"
        );
        assert_eq!(
            std::fs::read(root.join("nested/output")).unwrap(),
            b"local edit"
        );
        fixture
            .client
            .clean(proto::CleanRequest {
                output_base_id: "base".into(),
            })
            .await
            .unwrap();
        assert!(!root.exists());
        assert_eq!(
            fixture
                .client
                .batch_stat(proto::BatchStatRequest {
                    build_id: "next".into(),
                    paths: vec![]
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    assert_eq!(fixture.cas.reads.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn rejects_bad_sessions_paths_and_corrupt_cas_without_replacing_output() {
    let mut fixture = Fixture::new().await;
    let mut invalid = fixture.request("first", 1);
    invalid.version = 42;
    assert_eq!(
        fixture
            .client
            .start_build(invalid)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let mut wrong_cas = fixture.request("first", 1);
    wrong_cas.args.as_mut().unwrap().value = rev2::StartBuildArgs {
        remote_cache: "http://127.0.0.1:1".into(),
        instance_name: "fixture".into(),
        digest_function: 1,
    }
    .encode_to_vec();
    assert_eq!(
        fixture
            .client
            .start_build(wrong_cas)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let response = fixture
        .client
        .start_build(fixture.request("first", 1))
        .await
        .unwrap()
        .into_inner();
    let root = std::path::PathBuf::from(response.output_path_suffix);
    let digest = fixture.blob(b"content", 1);
    for path in [
        "../escape",
        "/absolute",
        "nested/../../escape",
        "a\\b",
        "",
        ".",
    ] {
        assert_eq!(
            fixture.stage("first", path, digest.clone()).await,
            tonic::Code::InvalidArgument as i32,
            "{path:?}"
        );
    }
    std::fs::write(root.join("existing"), b"keep me").unwrap();
    fixture.cas.data.lock().unwrap().insert(
        format!("fixture/blobs/{}/{}", digest.hash, digest.size_bytes),
        b"corrupt".to_vec(),
    );
    assert_eq!(
        fixture.stage("first", "existing", digest.clone()).await,
        tonic::Code::DataLoss as i32
    );
    assert_eq!(std::fs::read(root.join("existing")).unwrap(), b"keep me");
    assert!(
        !std::fs::read_dir(&root).unwrap().any(|p| p
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp"))
    );
    fixture.cas.data.lock().unwrap().clear();
    assert_eq!(
        fixture.stage("first", "existing", digest).await,
        tonic::Code::NotFound as i32
    );
    assert_eq!(std::fs::read(root.join("existing")).unwrap(), b"keep me");
    fixture
        .client
        .start_build(fixture.request("second", 1))
        .await
        .unwrap();
    assert_eq!(
        fixture
            .client
            .batch_stat(proto::BatchStatRequest {
                build_id: "first".into(),
                paths: vec![]
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[cfg(unix)]
#[tokio::test]
async fn symlinks_cannot_escape_output_tree_and_final_symlink_is_replaced_not_followed() {
    let mut fixture = Fixture::new().await;
    let response = fixture
        .client
        .start_build(fixture.request("build", 1))
        .await
        .unwrap()
        .into_inner();
    let root = std::path::PathBuf::from(response.output_path_suffix);
    let outside = fixture.outside.path().to_path_buf();
    std::fs::write(outside.join("protected"), b"untouched").unwrap();
    std::os::unix::fs::symlink(&outside, root.join("escape")).unwrap();
    std::os::unix::fs::symlink(outside.join("protected"), root.join("final")).unwrap();
    let digest = fixture.blob(b"content", 1);
    assert_ne!(
        fixture
            .stage("build", "escape/protected", digest.clone())
            .await,
        0
    );
    assert_eq!(
        std::fs::read(outside.join("protected")).unwrap(),
        b"untouched"
    );
    assert_eq!(fixture.stage("build", "final", digest).await, 0);
    assert_eq!(std::fs::read(root.join("final")).unwrap(), b"content");
    assert_eq!(
        std::fs::read(outside.join("protected")).unwrap(),
        b"untouched"
    );
    let stats = fixture
        .client
        .batch_stat(proto::BatchStatRequest {
            build_id: "build".into(),
            paths: vec!["escape/protected".into(), "escape".into()],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(stats.responses[0].stat.as_ref().unwrap().r#type.is_none());
    assert!(matches!(
        stats.responses[1].stat.as_ref().unwrap().r#type,
        Some(proto::batch_stat_response::stat::Type::Symlink(_))
    ));
    fixture
        .client
        .clean(proto::CleanRequest {
            output_base_id: "base".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(outside.join("protected")).unwrap(),
        b"untouched"
    );
}

#[tokio::test]
async fn build_ids_are_unambiguous_and_finalization_is_retryable() {
    let mut fixture = Fixture::new().await;
    fixture
        .client
        .start_build(fixture.request("shared-id", 1))
        .await
        .unwrap();
    let finalize = proto::FinalizeBuildRequest {
        build_id: "shared-id".into(),
        build_successful: true,
    };
    fixture
        .client
        .finalize_build(finalize.clone())
        .await
        .unwrap();
    fixture.client.finalize_build(finalize).await.unwrap();
    let mut other_base = fixture.request("shared-id", 1);
    other_base.output_base_id = "other-base".into();
    assert_eq!(
        fixture
            .client
            .start_build(other_base)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::AlreadyExists
    );
    // The protocol requires StartBuild to supersede any unfinished same-base build.
    fixture
        .client
        .start_build(fixture.request("old", 1))
        .await
        .unwrap();
    fixture
        .client
        .start_build(fixture.request("new", 1))
        .await
        .unwrap();
    assert_eq!(
        fixture
            .client
            .finalize_build(proto::FinalizeBuildRequest {
                build_id: "old".into(),
                build_successful: false
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    fixture
        .client
        .batch_stat(proto::BatchStatRequest {
            build_id: "new".into(),
            paths: vec![],
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn cancelled_staging_drops_temporary_file_and_preserves_destination() {
    use proto::bazel_output_service_server::BazelOutputService;
    let mut fixture = Fixture::new().await;
    let response = fixture
        .client
        .start_build(fixture.request("build", 1))
        .await
        .unwrap()
        .into_inner();
    let root = std::path::PathBuf::from(response.output_path_suffix);
    std::fs::write(root.join("existing"), b"preserve").unwrap();
    let digest = fixture.blob(b"stalled download", 1);
    fixture.cas.stall.store(true, Ordering::SeqCst);
    let service = fixture.service.clone();
    let request = proto::StageArtifactsRequest {
        build_id: "build".into(),
        artifacts: vec![proto::stage_artifacts_request::Artifact {
            path: "existing".into(),
            locator: Some(file_locator(digest)),
        }],
    };
    let stage = tokio::spawn(async move { service.stage_artifacts(Request::new(request)).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.cas.reads.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        std::fs::read_dir(&root).unwrap().any(|p| p
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp"))
    );
    stage.abort();
    assert!(stage.await.unwrap_err().is_cancelled());
    assert!(
        !std::fs::read_dir(&root).unwrap().any(|p| p
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp"))
    );
    assert_eq!(std::fs::read(root.join("existing")).unwrap(), b"preserve");
}

#[cfg(unix)]
#[tokio::test]
async fn concurrent_fifo_and_symlink_replacements_do_not_block_batch_stat() {
    let mut fixture = Fixture::new().await;
    let response = fixture
        .client
        .start_build(fixture.request("build", 1))
        .await
        .unwrap()
        .into_inner();
    let root = std::path::PathBuf::from(response.output_path_suffix);
    let fifo = root.join("fifo");
    let c_path =
        std::ffi::CString::new(std::os::unix::ffi::OsStrExt::as_bytes(fifo.as_os_str())).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
    std::fs::write(root.join("regular"), b"regular output").unwrap();
    std::fs::write(root.join("racy"), b"regular output").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = stop.clone();
    let writer_root = root.clone();
    let writer = std::thread::spawn(move || {
        while !writer_stop.load(Ordering::SeqCst) {
            for source in ["fifo", "regular"] {
                let temp = writer_root.join("next");
                let _ = std::fs::remove_file(&temp);
                std::fs::hard_link(writer_root.join(source), &temp).unwrap();
                std::fs::rename(&temp, writer_root.join("racy")).unwrap();
            }
            let temp = writer_root.join("next");
            let _ = std::fs::remove_file(&temp);
            std::os::unix::fs::symlink("regular", &temp).unwrap();
            std::fs::rename(&temp, writer_root.join("racy")).unwrap();
        }
    });
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..500 {
            fixture
                .client
                .batch_stat(proto::BatchStatRequest {
                    build_id: "build".into(),
                    paths: vec!["racy".into()],
                })
                .await?;
        }
        fixture
            .client
            .start_build(fixture.request("next-build", 1))
            .await?;
        Ok::<(), Status>(())
    })
    .await;
    stop.store(true, Ordering::SeqCst);
    writer.join().unwrap();
    result
        .expect("BatchStat must not block on a concurrently substituted FIFO")
        .unwrap();
}

#[cfg(windows)]
#[tokio::test]
async fn windows_rejects_aliases_and_replaces_existing_outputs() {
    let mut fixture = Fixture::new().await;
    let response = fixture
        .client
        .start_build(fixture.request("windows", 1))
        .await
        .unwrap()
        .into_inner();
    let root = std::path::PathBuf::from(response.output_path_suffix);
    let digest = fixture.blob(b"verified replacement", 1);
    for path in [
        "existing:stream",
        "NUL",
        "dir/COM1.txt",
        "LPT¹",
        "trailing.",
        "dir /output",
        "wild*card",
        "control\u{1f}",
    ] {
        assert_eq!(
            fixture.stage("windows", path, digest.clone()).await,
            tonic::Code::InvalidArgument as i32,
            "{path:?}"
        );
    }
    std::fs::write(root.join("existing"), b"old bytes").unwrap();
    assert_eq!(
        fixture.stage("windows", "existing", digest.clone()).await,
        0
    );
    assert_eq!(
        std::fs::read(root.join("existing")).unwrap(),
        b"verified replacement"
    );
    // Bazel may mark outputs read-only between builds.
    let mut permissions = std::fs::metadata(root.join("existing"))
        .unwrap()
        .permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(root.join("existing"), permissions).unwrap();
    let resource = format!("fixture/blobs/{}/{}", digest.hash, digest.size_bytes);
    fixture
        .cas
        .data
        .lock()
        .unwrap()
        .insert(resource.clone(), b"corrupted replacement".to_vec());
    assert_eq!(
        fixture.stage("windows", "existing", digest.clone()).await,
        tonic::Code::DataLoss as i32
    );
    assert!(
        std::fs::metadata(root.join("existing"))
            .unwrap()
            .permissions()
            .readonly()
    );
    assert_eq!(
        std::fs::read(root.join("existing")).unwrap(),
        b"verified replacement"
    );
    fixture
        .cas
        .data
        .lock()
        .unwrap()
        .insert(resource, b"verified replacement".to_vec());
    // A sharing violation must not leave the original file writable.
    use std::os::windows::fs::OpenOptionsExt;
    let held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(1 | 2)
        .open(root.join("existing"))
        .unwrap();
    assert_ne!(
        fixture.stage("windows", "existing", digest.clone()).await,
        0
    );
    assert!(
        std::fs::metadata(root.join("existing"))
            .unwrap()
            .permissions()
            .readonly()
    );
    assert_eq!(
        std::fs::read(root.join("existing")).unwrap(),
        b"verified replacement"
    );
    drop(held);
    assert_eq!(
        fixture.stage("windows", "existing", digest.clone()).await,
        0
    );
    assert_eq!(
        std::fs::read(root.join("existing")).unwrap(),
        b"verified replacement"
    );
    // Junction creation does not require developer mode or symlink privilege.
    std::fs::write(fixture.outside.path().join("protected"), b"outside").unwrap();
    let junction = root.join("escape");
    let result = std::process::Command::new("cmd.exe")
        .args(["/d", "/c", "mklink", "/J"])
        .arg(&junction)
        .arg(fixture.outside.path())
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "junction fixture failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_ne!(
        fixture.stage("windows", "escape/protected", digest).await,
        0
    );
    fixture
        .client
        .clean(proto::CleanRequest {
            output_base_id: "base".into(),
        })
        .await
        .unwrap();
    assert!(!root.exists());
    assert_eq!(
        std::fs::read(fixture.outside.path().join("protected")).unwrap(),
        b"outside"
    );
}

#[tokio::test]
async fn lazy_cas_adapter_times_out_after_stream_headers() {
    let fixture = Fixture::configured(false, Duration::from_millis(100)).await;
    let digest = fixture.blob(b"stalled stream", 9);
    fixture.cas.stall.store(true, Ordering::SeqCst);
    let artifact = output_core::Artifact {
        backend: fixture.service.backend_identity(),
        instance: "fixture".into(),
        digest_function: 9,
        hash: digest.hash,
        size: digest.size_bytes as u64,
    };
    let source = fixture.service.blob_source();
    let mut file = tempfile::tempfile().unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), source.fetch(&artifact, &mut file))
        .await
        .expect("lazy stream must have a total deadline")
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().code(),
        tonic::Code::DeadlineExceeded
    );
}

#[tokio::test]
async fn lazy_bos_stages_metadata_reads_verified_content_and_prunes_expired_cas() {
    let mut fixture = Fixture::configured(true, Duration::from_secs(10)).await;
    let response = fixture
        .client
        .start_build(fixture.request("lazy", 9))
        .await
        .unwrap()
        .into_inner();
    let prefix = std::path::Path::new(&response.output_path_suffix)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let digest = fixture.blob(b"lazy verified output", 9);
    assert_eq!(
        fixture.stage("lazy", "nested/output", digest.clone()).await,
        0
    );
    let stats = fixture
        .client
        .batch_stat(proto::BatchStatRequest {
            build_id: "lazy".into(),
            paths: vec!["nested/output".into(), "nested".into()],
        })
        .await
        .unwrap()
        .into_inner();
    let proto::batch_stat_response::stat::Type::File(file) = stats.responses[0]
        .stat
        .as_ref()
        .unwrap()
        .r#type
        .as_ref()
        .unwrap()
    else {
        panic!("not a file")
    };
    assert_eq!(
        rev2::FileArtifactLocator::decode(file.locator.as_ref().unwrap().value.as_slice())
            .unwrap()
            .digest
            .unwrap(),
        digest
    );
    assert!(matches!(
        stats.responses[1].stat.as_ref().unwrap().r#type,
        Some(proto::batch_stat_response::stat::Type::Directory(_))
    ));
    assert!(
        fixture.cas.reads.lock().unwrap().is_empty(),
        "stage and stat must not fetch payloads"
    );
    let projection = fixture.projection.as_ref().unwrap().clone();
    let path = format!("{prefix}/nested/output");
    let node = projection.tree.resolve(&path).unwrap();
    assert_eq!(
        projection.tree.read(node.id, 0, 100).await.unwrap(),
        b"lazy verified output"
    );
    assert_eq!(projection.tree.read(node.id, 0, 4).await.unwrap(), b"lazy");
    assert_eq!(fixture.cas.reads.lock().unwrap().len(), 1);
    projection.tree.write(node.id, 0, b"edit").await.unwrap();
    let stats = fixture
        .client
        .batch_stat(proto::BatchStatRequest {
            build_id: "lazy".into(),
            paths: vec!["nested/output".into()],
        })
        .await
        .unwrap()
        .into_inner();
    let proto::batch_stat_response::stat::Type::File(file) = stats.responses[0]
        .stat
        .as_ref()
        .unwrap()
        .r#type
        .as_ref()
        .unwrap()
    else {
        panic!("not a file")
    };
    let dirty = rev2::FileArtifactLocator::decode(file.locator.as_ref().unwrap().value.as_slice())
        .unwrap()
        .digest
        .unwrap();
    assert_ne!(
        dirty.hash, digest.hash,
        "local edits must invalidate remote digest authority"
    );
    assert_eq!(fixture.stage("lazy", "expires", digest).await, 0);
    fixture.cas.data.lock().unwrap().clear();
    fixture
        .client
        .start_build(fixture.request("next-lazy", 9))
        .await
        .unwrap();
    assert!(!fixture.cas.presence_queries.lock().unwrap().is_empty());
    let stats = fixture
        .client
        .batch_stat(proto::BatchStatRequest {
            build_id: "next-lazy".into(),
            paths: vec!["expires".into(), "nested/output".into()],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        stats.responses[0].stat.is_none(),
        "expired remote binding must be removed"
    );
    assert!(
        stats.responses[1].stat.is_some(),
        "authoritative local output must be retained"
    );
    fixture
        .client
        .clean(proto::CleanRequest {
            output_base_id: "base".into(),
        })
        .await
        .unwrap();
    assert!(projection.tree.resolve(&prefix).is_err());
}

#[tokio::test]
async fn compressed_cas_staging_is_atomic_and_negotiated_once() {
    for function in [1, 9] {
        let mut fixture = Fixture::new().await;
        fixture.cas.zstd.store(true, Ordering::SeqCst);
        fixture.client = fixture
            .client
            .clone()
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        let response = fixture
            .client
            .start_build(fixture.request("compressed", function))
            .await
            .unwrap()
            .into_inner();
        let root = std::path::PathBuf::from(response.output_path_suffix);
        let content = vec![b'x'; 128 * 1024];
        let digest = fixture.blob(&content, function);
        assert_eq!(
            fixture.stage("compressed", "output", digest.clone()).await,
            0
        );
        assert_eq!(std::fs::read(root.join("output")).unwrap(), content);
        let encoded = reapi::compression::encode_zstd(&content).unwrap();
        let metrics = fixture.service.metrics();
        assert_eq!(metrics.zstd_read_requests, 1);
        assert_eq!(metrics.cas_read_wire_bytes, encoded.len() as u64);
        assert!(metrics.cas_read_wire_bytes < content.len() as u64);
        let stats = fixture
            .client
            .batch_stat(proto::BatchStatRequest {
                build_id: "compressed".into(),
                paths: vec!["output".into()],
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(stats.responses.len(), 1);
        assert!(stats.responses[0].stat.is_some());

        let cases = [
            ("truncated", encoded[..encoded.len() - 1].to_vec()),
            ("corrupt", vec![0xff; 32]),
            (
                "oversized",
                reapi::compression::encode_zstd(&vec![b'x'; content.len() + 1]).unwrap(),
            ),
            (
                "wrong digest",
                reapi::compression::encode_zstd(&vec![b'y'; content.len()]).unwrap(),
            ),
        ];
        for (name, wire) in cases {
            *fixture.cas.wire_override.lock().unwrap() = Some(wire);
            assert_eq!(
                fixture.stage("compressed", "output", digest.clone()).await,
                tonic::Code::DataLoss as i32,
                "{name}"
            );
            assert_eq!(
                std::fs::read(root.join("output")).unwrap(),
                content,
                "{name}"
            );
            assert!(!std::fs::read_dir(&root).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp")
            }));
        }
        let small = fixture.blob(b"sixteen bytes!!!", function);
        *fixture.cas.wire_override.lock().unwrap() =
            Some(reapi::compression::encode_zstd(&vec![b'x'; 2 * 1024 * 1024]).unwrap());
        let artifact = output_core::Artifact {
            backend: fixture.service.backend_identity(),
            instance: "fixture".into(),
            digest_function: function,
            hash: small.hash,
            size: small.size_bytes as u64,
        };
        let mut partial = tempfile::tempfile().unwrap();
        let error = fixture
            .service
            .blob_source()
            .fetch(&artifact, &mut partial)
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<Status>().unwrap().code(),
            tonic::Code::DataLoss
        );
        assert!(
            partial.metadata().unwrap().len() <= artifact.size,
            "compressed bombs must be rejected before an oversized chunk reaches disk"
        );
        assert_eq!(fixture.cas.capabilities_queries.load(Ordering::SeqCst), 1);
        assert!(
            fixture
                .cas
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|resource| resource.contains("/compressed-blobs/zstd/"))
        );
    }
}

#[tokio::test]
async fn lazy_compressed_cas_verifies_before_exposing_a_range() {
    for function in [1, 9] {
        let mut fixture = Fixture::configured(true, Duration::from_secs(10)).await;
        fixture.cas.zstd.store(true, Ordering::SeqCst);
        let response = fixture
            .client
            .start_build(fixture.request("compressed-lazy", function))
            .await
            .unwrap()
            .into_inner();
        let prefix = std::path::Path::new(&response.output_path_suffix)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let content = vec![b'z'; 128 * 1024];
        let digest = fixture.blob(&content, function);
        assert_eq!(fixture.stage("compressed-lazy", "output", digest).await, 0);
        assert!(fixture.cas.reads.lock().unwrap().is_empty());
        let tree = &fixture.projection.as_ref().unwrap().tree;
        let node = tree.resolve(&format!("{prefix}/output")).unwrap();
        let encoded = reapi::compression::encode_zstd(&content).unwrap();
        *fixture.cas.wire_override.lock().unwrap() = Some(encoded[..encoded.len() - 1].to_vec());
        assert!(
            tree.read(node.id, 0, 1).await.is_err(),
            "even a one-byte read must not expose an unverified compressed blob"
        );
        *fixture.cas.wire_override.lock().unwrap() = None;
        assert_eq!(tree.read(node.id, 0, 4).await.unwrap(), b"zzzz");
        assert_eq!(tree.read(node.id, 4, 4).await.unwrap(), b"zzzz");
        tree.refresh(&prefix).await.unwrap();
        assert_eq!(
            fixture
                .cas
                .compressed_metadata_requests
                .load(Ordering::SeqCst),
            1,
            "FindMissingBlobs should compress requests after explicit transport advertisement"
        );
        let metrics = fixture.service.metrics();
        assert_eq!(metrics.zstd_read_requests, 2);
        assert_eq!(metrics.cas_read_wire_bytes, (encoded.len() * 2 - 1) as u64);
        assert_eq!(fixture.cas.reads.lock().unwrap().len(), 2);
        assert_eq!(fixture.cas.capabilities_queries.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn legacy_cas_uses_identity_and_compressed_stream_deadlines_are_total() {
    for implemented in [false, true] {
        let fixture = Fixture::configured(false, Duration::from_millis(150)).await;
        fixture
            .cas
            .capabilities_implemented
            .store(implemented, Ordering::SeqCst);
        let digest = fixture.blob(b"legacy identity", 1);
        let source = fixture.service.blob_source();
        let artifact = output_core::Artifact {
            backend: fixture.service.backend_identity(),
            instance: "fixture".into(),
            digest_function: 1,
            hash: digest.hash,
            size: digest.size_bytes as u64,
        };
        for _ in 0..2 {
            source
                .fetch(&artifact, &mut tempfile::tempfile().unwrap())
                .await
                .unwrap();
        }
        assert_eq!(fixture.cas.capabilities_queries.load(Ordering::SeqCst), 1);
        assert!(
            fixture
                .cas
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|r| r.contains("/blobs/"))
        );
    }

    let fixture = Fixture::configured(false, Duration::from_millis(150)).await;
    fixture.cas.zstd.store(true, Ordering::SeqCst);
    let digest = fixture.blob(b"stalled compressed stream", 9);
    fixture.cas.stall.store(true, Ordering::SeqCst);
    let artifact = output_core::Artifact {
        backend: fixture.service.backend_identity(),
        instance: "fixture".into(),
        digest_function: 9,
        hash: digest.hash,
        size: digest.size_bytes as u64,
    };
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        fixture
            .service
            .blob_source()
            .fetch(&artifact, &mut tempfile::tempfile().unwrap()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().code(),
        tonic::Code::DeadlineExceeded
    );
    assert!(fixture.cas.reads.lock().unwrap()[0].contains("/compressed-blobs/zstd/"));
}

#[tokio::test]
async fn lazy_presence_deadlines_include_unary_response_bodies() {
    for stalled_method in ["GetCapabilities", "FindMissingBlobs"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(socket).await.unwrap();
            let mut pending_bodies = Vec::new();
            while let Some(request) = connection.accept().await {
                let (request, mut respond) = request.unwrap();
                let mut body = respond
                    .send_response(
                        http::Response::builder()
                            .status(200)
                            .header("content-type", "application/grpc")
                            .body(())
                            .unwrap(),
                        false,
                    )
                    .unwrap();
                if request.uri().path().ends_with(stalled_method) {
                    // Headers are flushed while the message body and trailers never arrive.
                    pending_bodies.push(body);
                } else {
                    assert!(request.uri().path().ends_with("GetCapabilities"));
                    let message = cas_proto::ServerCapabilities::default().encode_to_vec();
                    let mut frame = vec![0];
                    frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
                    frame.extend_from_slice(&message);
                    body.send_data(frame.into(), false).unwrap();
                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("grpc-status", "0".parse().unwrap());
                    body.send_trailers(trailers).unwrap();
                }
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let service = OutputService::new(Config {
            root: dir.path().into(),
            cas_endpoint: endpoint,
            instance_name: "fixture".into(),
            api_key: None,
            max_blob_bytes: 1024,
            policy: test_policy(Duration::from_millis(80)),
        })
        .await
        .unwrap();
        let artifact = output_core::Artifact {
            backend: service.backend_identity(),
            instance: "fixture".into(),
            digest_function: 1,
            hash: format!("{:x}", Sha256::digest(b"one")),
            size: 3,
        };
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            service.blob_source().missing(&[artifact]),
        )
        .await
        .expect("headers alone must not bypass the total unary deadline")
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<Status>().unwrap().code(),
            tonic::Code::DeadlineExceeded,
            "{stalled_method}"
        );
        server.abort();
    }
}

#[tokio::test]
async fn metadata_compression_retries_only_explicit_codec_rejections() {
    for rejection in [1, 2, 3] {
        let fixture = Fixture::new().await;
        fixture.cas.zstd.store(true, Ordering::SeqCst);
        fixture
            .cas
            .metadata_rejection
            .store(rejection, Ordering::SeqCst);
        let digest = fixture.blob(b"presence only", 1);
        let artifact = output_core::Artifact {
            backend: fixture.service.backend_identity(),
            instance: "fixture".into(),
            digest_function: 1,
            hash: digest.hash,
            size: digest.size_bytes as u64,
        };
        let result = fixture.service.blob_source().missing(&[artifact]).await;
        match rejection {
            1 => assert!(result.unwrap().is_empty()),
            2 => assert_eq!(
                result.unwrap_err().downcast_ref::<Status>().unwrap().code(),
                tonic::Code::Unauthenticated
            ),
            3 => assert_eq!(
                result.unwrap_err().downcast_ref::<Status>().unwrap().code(),
                tonic::Code::Unimplemented
            ),
            _ => unreachable!(),
        }
        assert_eq!(
            fixture
                .cas
                .compressed_metadata_requests
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            fixture.cas.presence_queries.lock().unwrap().len(),
            usize::from(rejection == 1)
        );
        assert!(fixture.cas.reads.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn single_message_expansion_yields_to_the_total_read_deadline() {
    let fixture =
        Fixture::configured_limit(false, Duration::from_millis(100), 1024 * 1024 * 1024).await;
    fixture.cas.zstd.store(true, Ordering::SeqCst);
    fixture
        .cas
        .read_fragment_bytes
        .store(usize::MAX, Ordering::SeqCst);
    let source = fixture.service.blob_source();
    let small = fixture.blob(b"prewarm transport and capability discovery", 1);
    let mut artifact = output_core::Artifact {
        backend: fixture.service.backend_identity(),
        instance: "fixture".into(),
        digest_function: 1,
        hash: small.hash,
        size: small.size_bytes as u64,
    };
    source
        .fetch(&artifact, &mut tempfile::tempfile().unwrap())
        .await
        .unwrap();
    let frame = reapi::compression::encode_zstd(&[b'x'; 64 * 1024]).unwrap();
    let wire = frame.repeat(8192); // 512 MiB decoded, no whole-blob allocation.
    artifact.size = 512 * 1024 * 1024;
    fixture.cas.data.lock().unwrap().insert(
        format!("fixture/blobs/{}/{}", artifact.hash, artifact.size),
        vec![],
    );
    *fixture.cas.wire_override.lock().unwrap() = Some(wire);
    let mut partial = tempfile::tempfile().unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        source.fetch(&artifact, &mut partial),
    )
    .await
    .expect("one compressed message must not monopolize the async executor")
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().code(),
        tonic::Code::DeadlineExceeded
    );
    let size = partial.metadata().unwrap().len();
    assert!(
        size > 0 && size < artifact.size,
        "deadline should interrupt actual decompression, not only network setup: {size}"
    );
}

#[tokio::test]
async fn retry_restarts_partial_reads_and_does_not_retry_missing_or_corruption() {
    let fixture = Fixture::new().await;
    let digest = fixture.blob(b"verified complete retry content", 1);
    let artifact = output_core::Artifact {
        backend: fixture.service.backend_identity(),
        instance: "fixture".into(),
        digest_function: 1,
        hash: digest.hash.clone(),
        size: digest.size_bytes as u64,
    };
    fixture.cas.transient_reads.store(1, Ordering::SeqCst);
    let mut file = tempfile::tempfile().unwrap();
    fixture
        .service
        .blob_source()
        .fetch(&artifact, &mut file)
        .await
        .unwrap();
    assert_eq!(fixture.cas.reads.lock().unwrap().len(), 2);
    use std::io::{Read, Seek};
    file.rewind().unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"verified complete retry content");

    fixture.cas.data.lock().unwrap().clear();
    let error = fixture
        .service
        .blob_source()
        .fetch(&artifact, &mut file)
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().code(),
        tonic::Code::NotFound
    );
    assert_eq!(fixture.cas.reads.lock().unwrap().len(), 3);
    fixture.cas.data.lock().unwrap().insert(
        format!("fixture/blobs/{}/{}", digest.hash, digest.size_bytes),
        vec![b'x'; digest.size_bytes as usize],
    );
    let error = fixture
        .service
        .blob_source()
        .fetch(&artifact, &mut file)
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().code(),
        tonic::Code::DataLoss
    );
    assert_eq!(fixture.cas.reads.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn blocked_start_isolated_by_base_and_cancelled_reservation_is_reusable() {
    use proto::bazel_output_service_server::BazelOutputService;
    let fixture = Fixture::configured(true, Duration::from_secs(5)).await;
    let service = fixture.service.clone();
    service
        .start_build(Request::new(fixture.request("old", 1)))
        .await
        .unwrap();
    let projection = fixture.projection.as_ref().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let key = blake3::hash(b"base").to_hex().to_string();
    *projection.refresh_gate.lock().unwrap() = Some((key, entered.clone(), release));
    let request = fixture.request("reserved", 1);
    let task_service = service.clone();
    let blocked =
        tokio::spawn(async move { task_service.start_build(Request::new(request)).await });
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    let mut other = fixture.request("other", 1);
    other.output_base_id = "other-base".into();
    tokio::time::timeout(
        Duration::from_secs(1),
        service.start_build(Request::new(other)),
    )
    .await
    .unwrap()
    .unwrap();
    let mut duplicate = fixture.request("reserved", 1);
    duplicate.output_base_id = "duplicate-base".into();
    assert_eq!(
        service
            .start_build(Request::new(duplicate.clone()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::AlreadyExists
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(40),
            service.batch_stat(Request::new(proto::BatchStatRequest {
                build_id: "old".into(),
                paths: vec![]
            }))
        )
        .await
        .is_err()
    );
    blocked.abort();
    let _ = blocked.await;
    *projection.refresh_gate.lock().unwrap() = None;
    service
        .batch_stat(Request::new(proto::BatchStatRequest {
            build_id: "old".into(),
            paths: vec![],
        }))
        .await
        .unwrap();
    service.start_build(Request::new(duplicate)).await.unwrap();
    service
        .clean(Request::new(proto::CleanRequest {
            output_base_id: "base".into(),
        }))
        .await
        .unwrap();
    service
        .start_build(Request::new(fixture.request("reused", 1)))
        .await
        .unwrap();
    assert_eq!(
        service
            .batch_stat(Request::new(proto::BatchStatRequest {
                build_id: "old".into(),
                paths: vec![]
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn stream_idle_deadline_is_independent_of_the_operation_budget() {
    let mut policy = test_policy(Duration::from_secs(5));
    policy.stream_idle_timeout_ms = 40;
    policy.retry.max_attempts = 1;
    let fixture = Fixture::configured_policy(false, policy, 1024).await;
    let digest = fixture.blob(b"stalled progress", 1);
    fixture.cas.stall.store(true, Ordering::SeqCst);
    let artifact = output_core::Artifact {
        backend: fixture.service.backend_identity(),
        instance: "fixture".into(),
        digest_function: 1,
        hash: digest.hash,
        size: digest.size_bytes as u64,
    };
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        fixture
            .service
            .blob_source()
            .fetch(&artifact, &mut tempfile::tempfile().unwrap()),
    )
    .await
    .expect("idle timeout must precede the five-second operation deadline")
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().code(),
        tonic::Code::DeadlineExceeded
    );
    assert_eq!(fixture.cas.reads.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn canonical_empty_outputs_are_verified_locally_for_eager_and_lazy_reads() {
    use std::io::{Read, Seek, SeekFrom, Write};
    for function in [1, 9] {
        let mut fixture = Fixture::new().await;
        fixture.cas.zstd.store(true, Ordering::SeqCst);
        let response = fixture
            .client
            .start_build(fixture.request("empty", function))
            .await
            .unwrap()
            .into_inner();
        let root = std::path::PathBuf::from(response.output_path_suffix);
        let digest = fixture.blob(b"", function);
        fixture.cas.data.lock().unwrap().clear();
        std::fs::write(root.join("output"), b"old output").unwrap();
        assert_eq!(fixture.stage("empty", "output", digest.clone()).await, 0);
        assert_eq!(std::fs::read(root.join("output")).unwrap(), b"");

        let source = fixture.service.blob_source();
        let artifact = output_core::Artifact {
            backend: fixture.service.backend_identity(),
            instance: "fixture".into(),
            digest_function: function,
            hash: digest.hash,
            size: 0,
        };
        let mut target = tempfile::tempfile().unwrap();
        target.write_all(b"old lazy contents").unwrap();
        source.fetch(&artifact, &mut target).await.unwrap();
        assert_eq!(target.metadata().unwrap().len(), 0);
        assert_eq!(target.stream_position().unwrap(), 0);

        // A zero length alone is not enough: an incorrect hash must neither be
        // accepted nor replace an existing eager output or lazy target.
        let wrong = Digest {
            hash: "0".repeat(64),
            size_bytes: 0,
        };
        std::fs::write(root.join("output"), b"preserve eager").unwrap();
        assert_eq!(
            fixture.stage("empty", "output", wrong.clone()).await,
            tonic::Code::DataLoss as i32
        );
        assert_eq!(
            std::fs::read(root.join("output")).unwrap(),
            b"preserve eager"
        );
        target.write_all(b"preserve lazy").unwrap();
        let wrong_artifact = output_core::Artifact {
            hash: wrong.hash,
            ..artifact
        };
        let error = source
            .fetch(&wrong_artifact, &mut target)
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<Status>().unwrap().code(),
            tonic::Code::DataLoss
        );
        target.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        target.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"preserve lazy");
        assert!(fixture.cas.reads.lock().unwrap().is_empty());
        assert_eq!(fixture.cas.capabilities_queries.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.service.metrics().zstd_read_requests, 0);
    }
}

#[tokio::test]
async fn backend_authority_separates_credentials_and_is_stable_across_state_paths() {
    let dir = tempfile::tempdir().unwrap();
    let mut identities = Vec::new();
    for (name, key) in [
        ("first", Some("scope-a")),
        ("second", Some("scope-b")),
        ("anonymous", None),
        ("same-scope", Some("scope-a")),
    ] {
        let service = OutputService::new(Config {
            root: dir.path().join(name),
            cas_endpoint: "grpc://127.0.0.1:1985".into(),
            instance_name: "fixture".into(),
            api_key: key.map(str::to_owned),
            max_blob_bytes: 1024,
            policy: reapi::policy::Policy::default(),
        })
        .await
        .unwrap();
        identities.push(service.backend_identity());
    }
    assert_ne!(identities[0], identities[1]);
    assert_ne!(identities[0], identities[2]);
    assert_ne!(identities[1], identities[2]);
    assert_eq!(identities[0], identities[3]);
}

#[tokio::test]
async fn local_proxy_alias_retains_upstream_fetch_and_exact_client_scope() {
    for alias in ["http://127.0.0.1:18473", "unix:/tmp/private-cache.sock"] {
        let mut fixture = Fixture::configured_client_endpoint(
            false,
            test_policy(Duration::from_secs(10)),
            1024 * 1024,
            Some(alias),
        )
        .await;
        let mut invalid = fixture.request("wrong", 9);
        invalid.args.as_mut().unwrap().value = rev2::StartBuildArgs {
            remote_cache: "http://127.0.0.1:18474".into(),
            instance_name: "fixture".into(),
            digest_function: 9,
        }
        .encode_to_vec();
        assert_eq!(
            fixture
                .client
                .start_build(invalid)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let response = fixture
            .client
            .start_build(fixture.request("alias", 9))
            .await
            .unwrap()
            .into_inner();
        let root = std::path::PathBuf::from(response.output_path_suffix);
        let digest = fixture.blob(b"upstream contents", 9);
        assert_eq!(fixture.stage("alias", "result", digest).await, 0);
        assert_eq!(
            std::fs::read(root.join("result")).unwrap(),
            b"upstream contents"
        );
        assert_eq!(
            fixture.cas.reads.lock().unwrap().len(),
            1,
            "content must be read from configured upstream, never the client alias"
        );
    }
}
