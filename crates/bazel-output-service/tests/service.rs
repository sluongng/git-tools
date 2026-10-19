use bazel_output_service::{
    Config, OutputService, file_locator,
    proto::{
        self, bazel_output_service_client::BazelOutputServiceClient,
        bazel_output_service_server::BazelOutputServiceServer,
    },
    rev2,
};
use prost::Message;
use reapi::{Digest, proto::google::bytestream as bs};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_stream::{Stream, wrappers::TcpListenerStream};
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
struct Cas {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    reads: Arc<Mutex<Vec<String>>>,
    stall: Arc<AtomicBool>,
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
        let name = request.into_inner().resource_name;
        self.reads.lock().unwrap().push(name.clone());
        let bytes = self
            .data
            .lock()
            .unwrap()
            .get(&name)
            .cloned()
            .ok_or_else(|| Status::not_found("missing fixture blob"))?;
        if self.stall.load(Ordering::SeqCst) {
            return Ok(Response::new(Box::pin(tokio_stream::pending())));
        }
        let chunks: Vec<_> = bytes
            .chunks(3)
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
        let dir = tempfile::tempdir().unwrap();
        let cas = Cas::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let cas_service = cas.clone();
        let cas_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(bs::byte_stream_server::ByteStreamServer::new(cas_service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let service = OutputService::new(Config {
            root: dir.path().to_path_buf(),
            cas_endpoint: endpoint.clone(),
            instance_name: "fixture".into(),
            api_key: Some("test-key".into()),
            max_blob_bytes: 1024 * 1024,
            rpc_timeout: Duration::from_secs(10),
        })
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let service_endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service_clone = service.clone();
        let service_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(BazelOutputServiceServer::new(service_clone))
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
