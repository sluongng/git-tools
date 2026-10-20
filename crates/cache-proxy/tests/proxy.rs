use anyhow::Result;
use cache_proxy::{CacheProxy, ProxyConfig, Upstream};
use futures_core::Stream;
use local_cas::{CacheConfig, LocalCas};
use reapi::{
    proto::google::bytestream::{
        self as bs,
        byte_stream_client::ByteStreamClient,
        byte_stream_server::{ByteStream, ByteStreamServer},
    },
    reapi::*,
};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tonic::{Request, Response, Status, transport::Server};

type S<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
#[derive(Clone, Default)]
struct Backend {
    observed_metadata: Arc<Mutex<Vec<(String, tonic::metadata::MetadataMap)>>>,
    compressed_reads: Arc<AtomicU64>,
    reject_compressed_reads: Arc<AtomicBool>,
    invalid_compressed_response: Arc<AtomicU64>,
    identity_only: Arc<AtomicBool>,
    blobs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    reads: Arc<AtomicU64>,
    ac_reads: Arc<AtomicU64>,
    reject_writes: Arc<Mutex<bool>>,
}
impl Backend {
    fn observe<T>(&self, method: &str, request: &Request<T>) {
        self.observed_metadata
            .lock()
            .unwrap()
            .push((method.to_owned(), request.metadata().clone()));
    }
}
#[tonic::async_trait]
impl ByteStream for Backend {
    type ReadStream = S<bs::ReadResponse>;
    async fn read(
        &self,
        req: Request<bs::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        self.observe("read", &req);
        self.reads.fetch_add(1, Ordering::Relaxed);
        let name = req.into_inner().resource_name;
        let hash = name.split('/').rev().nth(1).unwrap();
        let bytes = self
            .blobs
            .lock()
            .unwrap()
            .get(hash)
            .cloned()
            .ok_or_else(|| Status::not_found("absent"))?;
        let compressed = name.contains("compressed-blobs/zstd/");
        if compressed {
            self.compressed_reads.fetch_add(1, Ordering::Relaxed);
            if self.identity_only.load(Ordering::Relaxed)
                || self.reject_compressed_reads.load(Ordering::Relaxed)
            {
                return Err(Status::unimplemented("zstd resource unsupported"));
            }
        }
        let bytes = if compressed {
            let mut encoded = reapi::compression::encode_zstd(&bytes).unwrap();
            match self.invalid_compressed_response.load(Ordering::Relaxed) {
                1 => encoded = b"not zstd".to_vec(),
                2 => {
                    encoded.truncate(encoded.len().saturating_sub(4));
                }
                _ => {}
            }
            encoded
        } else {
            bytes
        };

        Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
            bs::ReadResponse { data: bytes },
        )]))))
    }
    async fn write(
        &self,
        req: Request<tonic::Streaming<bs::WriteRequest>>,
    ) -> Result<Response<bs::WriteResponse>, Status> {
        self.observe("write", &req);
        if *self.reject_writes.lock().unwrap() {
            return Err(Status::permission_denied("write blocked"));
        }
        let mut stream = req.into_inner();
        let mut bytes = vec![];
        let mut name = String::new();
        while let Some(req) = stream.message().await? {
            name = req.resource_name;
            bytes.extend(req.data);
        }
        let hash = name.split('/').rev().nth(1).unwrap().to_string();
        let compressed = name.contains("compressed-blobs");
        if compressed && self.identity_only.load(Ordering::Relaxed) {
            return Err(Status::unimplemented("zstd unsupported"));
        }
        let size = bytes.len();
        let bytes = if compressed {
            reapi::compression::decode_zstd(&bytes, 1 << 20).unwrap()
        } else {
            bytes
        };
        self.blobs.lock().unwrap().insert(hash, bytes);
        Ok(Response::new(bs::WriteResponse {
            committed_size: if compressed { -1 } else { size as i64 },
        }))
    }
    async fn query_write_status(
        &self,
        req: Request<bs::QueryWriteStatusRequest>,
    ) -> Result<Response<bs::QueryWriteStatusResponse>, Status> {
        self.observe("query_write_status", &req);
        Err(Status::not_found("unknown upload"))
    }
}
#[tonic::async_trait]
impl action_cache_server::ActionCache for Backend {
    async fn get_action_result(
        &self,
        req: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        self.observe("get_action_result", &req);
        self.ac_reads.fetch_add(1, Ordering::Relaxed);
        Ok(Response::new(ActionResult {
            stdout_raw: if req.into_inner().inline_stdout {
                b"inline".to_vec()
            } else {
                vec![]
            },
            ..Default::default()
        }))
    }
    async fn update_action_result(
        &self,
        req: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        self.observe("update_action_result", &req);
        Ok(Response::new(
            req.into_inner().action_result.unwrap_or_default(),
        ))
    }
}
#[tonic::async_trait]
impl capabilities_server::Capabilities for Backend {
    async fn get_capabilities(
        &self,
        _: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        let response = ServerCapabilities {
            cache_capabilities: Some(CacheCapabilities {
                digest_functions: vec![1, 9, 2],
                supported_compressors: if self.identity_only.load(Ordering::Relaxed) {
                    vec![]
                } else {
                    vec![1]
                },
                supported_batch_update_compressors: if self.identity_only.load(Ordering::Relaxed) {
                    vec![]
                } else {
                    vec![1]
                },
                max_batch_total_size_bytes: 16 << 20,
                split_blob_support: true,
                splice_blob_support: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        // Real HTTP proxies may attach a length to the encoded gRPC frame.
        // Filtering capabilities downstream changes that frame's wire length.
        use prost::Message;
        let length = response.encoded_len() + 5;
        let mut response = Response::new(response);
        response
            .metadata_mut()
            .insert("content-length", length.to_string().parse().unwrap());
        Ok(response)
    }
}
#[tonic::async_trait]
impl content_addressable_storage_server::ContentAddressableStorage for Backend {
    async fn find_missing_blobs(
        &self,
        req: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        self.observe("find_missing_blobs", &req);
        Ok(Response::new(FindMissingBlobsResponse {
            missing_blob_digests: req
                .into_inner()
                .blob_digests
                .into_iter()
                .filter(|d| !self.blobs.lock().unwrap().contains_key(&d.hash))
                .collect(),
        }))
    }
    async fn batch_update_blobs(
        &self,
        req: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        self.observe("batch_update_blobs", &req);
        if *self.reject_writes.lock().unwrap() {
            return Err(Status::permission_denied("write blocked"));
        }
        let responses = req
            .into_inner()
            .requests
            .into_iter()
            .map(|r| {
                let d = r.digest.unwrap();
                let bytes = if r.compressor == 1 {
                    reapi::compression::decode_zstd(&r.data, d.size_bytes as u64).unwrap()
                } else {
                    r.data
                };
                self.blobs.lock().unwrap().insert(d.hash.clone(), bytes);
                batch_update_blobs_response::Response {
                    digest: Some(d),
                    status: Some(Default::default()),
                }
            })
            .collect();
        Ok(Response::new(BatchUpdateBlobsResponse { responses }))
    }
    async fn batch_read_blobs(
        &self,
        _: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        Err(Status::unimplemented(
            "test uses verified ByteStream hydration",
        ))
    }
    type GetTreeStream = S<GetTreeResponse>;
    async fn get_tree(
        &self,
        _: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        Err(Status::not_found("tree absent"))
    }
    async fn split_blob(
        &self,
        _: Request<SplitBlobRequest>,
    ) -> Result<Response<SplitBlobResponse>, Status> {
        Ok(Response::new(Default::default()))
    }
    type GetChunkMappingStream = S<GetChunkMappingResponse>;
    async fn get_chunk_mapping(
        &self,
        _: Request<GetChunkMappingRequest>,
    ) -> Result<Response<Self::GetChunkMappingStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
            Default::default(),
        )]))))
    }
    async fn splice_blob(
        &self,
        _: Request<SpliceBlobRequest>,
    ) -> Result<Response<SpliceBlobResponse>, Status> {
        Ok(Response::new(Default::default()))
    }
    async fn register_chunk_mapping(
        &self,
        req: Request<tonic::Streaming<RegisterChunkMappingRequest>>,
    ) -> Result<Response<RegisterChunkMappingResponse>, Status> {
        self.observe("register_chunk_mapping", &req);
        let mut stream = req.into_inner();
        while stream.message().await?.is_some() {}
        Ok(Response::new(Default::default()))
    }
}
struct Harness {
    backend: Backend,
    url: String,
    _dir: tempfile::TempDir,
    cache: Arc<LocalCas>,
    proxy: CacheProxy,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
impl Harness {
    async fn new(ttl: Duration) -> Self {
        let backend = Backend::default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let fake = backend.clone();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(
                    ByteStreamServer::new(fake.clone())
                        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                        .send_compressed(tonic::codec::CompressionEncoding::Gzip),
                )
                .add_service(action_cache_server::ActionCacheServer::new(fake.clone()))
                .add_service(capabilities_server::CapabilitiesServer::new(fake.clone()))
                .add_service(
                    content_addressable_storage_server::ContentAddressableStorageServer::new(fake),
                )
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let config = ProxyConfig {
            upstream: format!("http://{upstream_addr}"),
            backend_identity: "test-security-scope".into(),
            instance_name: "repo".into(),
            action_cache_ttl: ttl,
            upstream_headers: vec![("x-buildbuddy-api-key".into(), "daemon-key".into())],
            local_bearer_token: Some("test-token".into()),
            ..Default::default()
        };
        let upstream = Upstream::connect(config).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalCas::open(dir.path(), upstream.clone(), CacheConfig::default()).unwrap();
        let proxy = CacheProxy::new(upstream, cache.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serving = proxy.clone();
        let proxy_task = tokio::spawn(async move {
            serving
                .serve_listener(listener, std::future::pending())
                .await
                .unwrap();
        });
        Self {
            backend,
            url: format!("http://{addr}"),
            _dir: dir,
            cache,
            proxy,
            tasks: vec![task, proxy_task],
        }
    }
    fn request<T>(&self, body: T) -> Request<T> {
        let mut req = Request::new(body);
        req.metadata_mut()
            .insert("authorization", "Bearer test-token".parse().unwrap());
        req
    }
}
fn digest(bytes: &[u8]) -> Digest {
    Digest {
        hash: blake3::hash(bytes).to_hex().to_string(),
        size_bytes: bytes.len() as i64,
    }
}
#[tokio::test]
async fn verified_shared_reads_do_not_advertise_local_only_blobs_to_rbe() {
    let h = Harness::new(Duration::ZERO).await;
    let bytes = b"shared content";
    let d = digest(bytes);
    h.backend
        .blobs
        .lock()
        .unwrap()
        .insert(d.hash.clone(), bytes.to_vec());
    let mut client =
        content_addressable_storage_client::ContentAddressableStorageClient::connect(h.url.clone())
            .await
            .unwrap();
    let req = BatchReadBlobsRequest {
        instance_name: "repo".into(),
        digests: vec![d.clone()],
        digest_function: 9,
        acceptable_compressors: vec![1],
    };
    for _ in 0..2 {
        let response = client
            .batch_read_blobs(h.request(req.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.responses[0].status.as_ref().unwrap().code, 0);
        assert_eq!(
            reapi::compression::decode_zstd(&response.responses[0].data, d.size_bytes as u64)
                .unwrap(),
            bytes
        );
    }
    assert_eq!(h.backend.reads.load(Ordering::Relaxed), 1);
    assert!(h.cache.metrics().hits >= 1);
    h.backend.blobs.lock().unwrap().clear();
    let response = client
        .find_missing_blobs(h.request(FindMissingBlobsRequest {
            instance_name: "repo".into(),
            blob_digests: vec![d.clone()],
            digest_function: 9,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.missing_blob_digests, vec![d]);
}
#[tokio::test]
async fn writes_are_verified_and_confirmed_upstream_before_success() {
    let h = Harness::new(Duration::ZERO).await;
    let bytes = b"upload";
    let d = digest(bytes);
    let mut client =
        content_addressable_storage_client::ContentAddressableStorageClient::connect(h.url.clone())
            .await
            .unwrap();
    let mut req = BatchUpdateBlobsRequest {
        instance_name: "repo".into(),
        digest_function: 9,
        requests: vec![batch_update_blobs_request::Request {
            digest: Some(d.clone()),
            data: reapi::compression::encode_zstd(bytes).unwrap(),
            compressor: 1,
        }],
    };
    *h.backend.reject_writes.lock().unwrap() = true;
    assert_eq!(
        client
            .batch_update_blobs(h.request(req.clone()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    *h.backend.reject_writes.lock().unwrap() = false;
    client
        .batch_update_blobs(h.request(req.clone()))
        .await
        .unwrap();
    assert_eq!(h.backend.blobs.lock().unwrap()[&d.hash], bytes);
    // An omitted digest function means SHA256, never BLAKE3.
    use sha2::{Digest as _, Sha256};
    let sha_digest = Digest {
        hash: format!("{:x}", Sha256::digest(bytes)),
        size_bytes: bytes.len() as i64,
    };
    client
        .batch_update_blobs(h.request(BatchUpdateBlobsRequest {
            instance_name: "repo".into(),
            digest_function: 0,
            requests: vec![batch_update_blobs_request::Request {
                digest: Some(sha_digest.clone()),
                data: bytes.to_vec(),
                compressor: 0,
            }],
        }))
        .await
        .unwrap();
    assert_eq!(h.backend.blobs.lock().unwrap()[&sha_digest.hash], bytes);
    req.requests[0].data = reapi::compression::encode_zstd(b"wrong!").unwrap();
    assert_eq!(
        client
            .batch_update_blobs(h.request(req))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}
#[tokio::test]
async fn byte_stream_zstd_write_and_partial_read() {
    let h = Harness::new(Duration::ZERO).await;
    let bytes = b"compressed stream contents";
    let d = digest(bytes);
    let mut client = ByteStreamClient::connect(h.url.clone()).await.unwrap();
    let request = bs::WriteRequest {
        resource_name: format!(
            "repo/uploads/test/compressed-blobs/zstd/blake3/{}/{}",
            d.hash, d.size_bytes
        ),
        write_offset: 0,
        finish_write: true,
        data: reapi::compression::encode_zstd(bytes).unwrap(),
    };
    // finish_write must complete even when the client leaves its send stream open.
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    sender.send(request).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        client.write(h.request(tokio_stream::wrappers::ReceiverStream::new(receiver))),
    )
    .await
    .unwrap()
    .unwrap();
    drop(sender);
    assert_eq!(h.backend.blobs.lock().unwrap()[&d.hash], bytes);
    let mut response = client
        .read(h.request(bs::ReadRequest {
            resource_name: format!(
                "repo/compressed-blobs/zstd/blake3/{}/{}",
                d.hash, d.size_bytes
            ),
            read_offset: 3,
            read_limit: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let mut data = vec![];
    while let Some(chunk) = response.message().await.unwrap() {
        data.extend(chunk.data);
    }
    assert_eq!(
        reapi::compression::decode_zstd(&data, bytes.len() as u64).unwrap(),
        &bytes[3..]
    );
}
#[tokio::test]
async fn action_cache_defaults_revalidate_and_inline_variants_are_distinct() {
    for (ttl, expected_calls) in [(Duration::ZERO, 3), (Duration::from_secs(30), 2)] {
        let h = Harness::new(ttl).await;
        let mut client = action_cache_client::ActionCacheClient::connect(h.url.clone())
            .await
            .unwrap();
        let mut req = GetActionResultRequest {
            instance_name: "repo".into(),
            action_digest: Some(digest(b"action")),
            digest_function: 9,
            ..Default::default()
        };
        for _ in 0..2 {
            assert!(
                client
                    .get_action_result(h.request(req.clone()))
                    .await
                    .unwrap()
                    .into_inner()
                    .stdout_raw
                    .is_empty()
            );
        }
        req.inline_stdout = true;
        assert_eq!(
            client
                .get_action_result(h.request(req))
                .await
                .unwrap()
                .into_inner()
                .stdout_raw,
            b"inline"
        );
        assert_eq!(h.backend.ac_reads.load(Ordering::Relaxed), expected_calls);
        assert_eq!(
            h.proxy.metrics().action_cache_hits,
            if ttl.is_zero() { 0 } else { 1 }
        );
    }
}
#[tokio::test]
async fn auth_scope_capabilities_and_corruption_are_enforced() {
    let h = Harness::new(Duration::ZERO).await;
    let mut client = capabilities_client::CapabilitiesClient::connect(h.url.clone())
        .await
        .unwrap();
    assert_eq!(
        client
            .get_capabilities(GetCapabilitiesRequest {
                instance_name: "repo".into()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        client
            .get_capabilities(h.request(GetCapabilitiesRequest {
                instance_name: "other".into()
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let caps = client
        .get_capabilities(h.request(GetCapabilitiesRequest {
            instance_name: "repo".into(),
        }))
        .await
        .unwrap()
        .into_inner()
        .cache_capabilities
        .unwrap();
    assert_eq!(caps.digest_functions, vec![1, 9]);
    assert_eq!(caps.max_batch_total_size_bytes, 4 << 20);
    assert!(caps.split_blob_support && caps.splice_blob_support);
    let mut cas =
        content_addressable_storage_client::ContentAddressableStorageClient::connect(h.url.clone())
            .await
            .unwrap()
            .send_compressed(tonic::codec::CompressionEncoding::Gzip);
    cas.split_blob(h.request(SplitBlobRequest {
        instance_name: "repo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    cas.splice_blob(h.request(SpliceBlobRequest {
        instance_name: "repo".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    cas.register_chunk_mapping(h.request(tokio_stream::iter(vec![
        RegisterChunkMappingRequest {
            instance_name: "repo".into(),
            ..Default::default()
        },
        RegisterChunkMappingRequest::default(),
    ])))
    .await
    .unwrap();

    let d = digest(b"correct");
    h.backend
        .blobs
        .lock()
        .unwrap()
        .insert(d.hash.clone(), b"corrupt".to_vec());
    let mut bytes = ByteStreamClient::connect(h.url.clone()).await.unwrap();
    assert_eq!(
        bytes
            .read(h.request(bs::ReadRequest {
                resource_name: format!("repo/blobs/blake3/{}/{}", d.hash, d.size_bytes),
                ..Default::default()
            }))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::DataLoss
    );
}

#[tokio::test]
async fn upstream_not_found_is_preserved_and_identity_only_caps_do_not_advertise_zstd() {
    let h = Harness::new(Duration::ZERO).await;
    h.backend.identity_only.store(true, Ordering::Relaxed);
    let mut capabilities = capabilities_client::CapabilitiesClient::connect(h.url.clone())
        .await
        .unwrap();
    let caps = capabilities
        .get_capabilities(h.request(GetCapabilitiesRequest {
            instance_name: "repo".into(),
        }))
        .await
        .unwrap()
        .into_inner()
        .cache_capabilities
        .unwrap();
    assert!(caps.supported_compressors.is_empty());
    assert!(caps.supported_batch_update_compressors.is_empty());
    let mut bytes = ByteStreamClient::connect(h.url.clone()).await.unwrap();
    let data = b"identity fallback";
    let d = digest(data);
    let read = bs::ReadRequest {
        resource_name: format!("repo/blobs/blake3/{}/{}", d.hash, d.size_bytes),
        ..Default::default()
    };
    assert_eq!(
        bytes
            .read(h.request(read.clone()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::NotFound
    );
    let mut cas =
        content_addressable_storage_client::ContentAddressableStorageClient::connect(h.url.clone())
            .await
            .unwrap();
    let response = cas
        .batch_read_blobs(h.request(BatchReadBlobsRequest {
            instance_name: "repo".into(),
            digests: vec![d.clone()],
            digest_function: 9,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.responses[0].status.as_ref().unwrap().code,
        tonic::Code::NotFound as i32
    );
    bytes
        .write(h.request(tokio_stream::iter(vec![bs::WriteRequest {
            resource_name: format!(
                "repo/uploads/identity/blobs/blake3/{}/{}",
                d.hash, d.size_bytes
            ),
            data: data.to_vec(),
            write_offset: 0,
            finish_write: true,
        }])))
        .await
        .unwrap();
    let mut result = bytes.read(h.request(read)).await.unwrap().into_inner();
    assert_eq!(result.message().await.unwrap().unwrap().data, data);
}

#[tokio::test]
async fn zstd_hydration_is_negotiated_and_falls_back_without_hiding_corruption() {
    let h = Harness::new(Duration::ZERO).await;
    let data = vec![42; 2 * 1024 * 1024];
    let d = digest(&data);
    h.backend
        .blobs
        .lock()
        .unwrap()
        .insert(d.hash.clone(), data.clone());
    let mut client = ByteStreamClient::connect(h.url.clone()).await.unwrap();
    let mut stream = client
        .read(h.request(bs::ReadRequest {
            resource_name: format!("repo/blobs/blake3/{}/{}", d.hash, d.size_bytes),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut result = vec![];
    while let Some(chunk) = stream.message().await.unwrap() {
        result.extend(chunk.data);
    }
    assert_eq!(result, data);
    assert_eq!(h.backend.compressed_reads.load(Ordering::Relaxed), 1);

    let fallback = Harness::new(Duration::ZERO).await;
    fallback
        .backend
        .reject_compressed_reads
        .store(true, Ordering::Relaxed);
    let mut client = ByteStreamClient::connect(fallback.url.clone())
        .await
        .unwrap();
    for data in [
        b"first fallback blob".as_slice(),
        b"second fallback blob".as_slice(),
    ] {
        let d = digest(data);
        fallback
            .backend
            .blobs
            .lock()
            .unwrap()
            .insert(d.hash.clone(), data.to_vec());
        let mut stream = client
            .read(fallback.request(bs::ReadRequest {
                resource_name: format!("repo/blobs/blake3/{}/{}", d.hash, d.size_bytes),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(stream.message().await.unwrap().unwrap().data, data);
    }
    assert_eq!(fallback.backend.compressed_reads.load(Ordering::Relaxed), 1);
    assert_eq!(fallback.backend.reads.load(Ordering::Relaxed), 3);

    for mode in [1, 2] {
        let h = Harness::new(Duration::ZERO).await;
        h.backend
            .invalid_compressed_response
            .store(mode, Ordering::Relaxed);
        let data = b"must not hide transport corruption";
        let d = digest(data);
        h.backend
            .blobs
            .lock()
            .unwrap()
            .insert(d.hash.clone(), data.to_vec());
        let mut client = ByteStreamClient::connect(h.url.clone()).await.unwrap();
        assert_eq!(
            client
                .read(h.request(bs::ReadRequest {
                    resource_name: format!("repo/blobs/blake3/{}/{}", d.hash, d.size_bytes),
                    ..Default::default()
                }))
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::DataLoss
        );
        assert_eq!(h.backend.reads.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn forwards_only_bounded_request_attribution_and_daemon_credentials() {
    use prost::Message;
    let h = Harness::new(Duration::ZERO).await;
    let attribution = RequestMetadata {
        action_id: "neutral-action".into(),
        tool_invocation_id: "neutral-invocation".into(),
        ..Default::default()
    }
    .encode_to_vec();
    let attributed = |mut request: Request<_>| {
        request.metadata_mut().insert_bin(
            "build.bazel.remote.execution.v2.requestmetadata-bin",
            tonic::metadata::MetadataValue::from_bytes(&attribution),
        );
        request.metadata_mut().insert(
            "x-buildbuddy-api-key",
            "caller-key-must-not-forward".parse().unwrap(),
        );
        request
            .metadata_mut()
            .insert("x-untrusted-header", "do-not-forward".parse().unwrap());
        request
    };
    let mut ac = action_cache_client::ActionCacheClient::connect(h.url.clone())
        .await
        .unwrap();
    ac.get_action_result(attributed(h.request(GetActionResultRequest {
        instance_name: "repo".into(),
        action_digest: Some(digest(b"metadata action")),
        digest_function: 9,
        ..Default::default()
    })))
    .await
    .unwrap();
    {
        let observed = h.backend.observed_metadata.lock().unwrap();
        let metadata = &observed
            .iter()
            .find(|(method, _)| method == "get_action_result")
            .unwrap()
            .1;
        assert_eq!(
            metadata
                .get_bin("build.bazel.remote.execution.v2.requestmetadata-bin")
                .unwrap()
                .to_bytes()
                .unwrap()
                .as_ref(),
            attribution
        );
        assert_eq!(metadata.get("x-buildbuddy-api-key").unwrap(), "daemon-key");
        assert!(metadata.get("authorization").is_none());
        assert!(metadata.get("x-untrusted-header").is_none());
    }

    let mut oversized = h.request(GetActionResultRequest {
        instance_name: "repo".into(),
        ..Default::default()
    });
    oversized.metadata_mut().insert_bin(
        "build.bazel.remote.execution.v2.requestmetadata-bin",
        tonic::metadata::MetadataValue::from_bytes(&vec![0; 8193]),
    );
    assert_eq!(
        ac.get_action_result(oversized).await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );

    // Coalesced hydration has no unique invocation owner. It must not silently
    // attribute a shared fetch to whichever caller wins admission.
    let bytes = b"daemon-owned hydration";
    let d = digest(bytes);
    h.backend
        .blobs
        .lock()
        .unwrap()
        .insert(d.hash.clone(), bytes.to_vec());
    let mut client = ByteStreamClient::connect(h.url.clone()).await.unwrap();
    let mut request = h.request(bs::ReadRequest {
        resource_name: format!("repo/blobs/blake3/{}/{}", d.hash, d.size_bytes),
        ..Default::default()
    });
    request.metadata_mut().insert_bin(
        "build.bazel.remote.execution.v2.requestmetadata-bin",
        tonic::metadata::MetadataValue::from_bytes(&attribution),
    );
    let mut stream = client.read(request).await.unwrap().into_inner();
    assert_eq!(stream.message().await.unwrap().unwrap().data, bytes);
    for (_, metadata) in h
        .backend
        .observed_metadata
        .lock()
        .unwrap()
        .iter()
        .filter(|(method, _)| method == "read")
    {
        assert!(
            metadata
                .get_bin("build.bazel.remote.execution.v2.requestmetadata-bin")
                .is_none()
        );
    }
}

#[tokio::test]
async fn concurrent_streaming_uploads_do_not_starve_admitted_writers_of_connection_credit() {
    let h = Harness::new(Duration::ZERO).await;
    let client = ByteStreamClient::connect(h.url.clone()).await.unwrap();
    let mut base = Vec::with_capacity(512 * 1024);
    for i in 0..16384u32 {
        base.extend_from_slice(blake3::hash(&i.to_le_bytes()).as_bytes());
    }
    let mut uploads = tokio::task::JoinSet::new();
    for index in 0..32u32 {
        let mut bytes = base.clone();
        bytes[..4].copy_from_slice(&index.to_le_bytes());
        let d = digest(&bytes);
        let resource = format!(
            "repo/uploads/concurrent-{index}/blobs/blake3/{}/{}",
            d.hash, d.size_bytes
        );
        let chunks: Vec<_> = bytes
            .chunks(128 * 1024)
            .enumerate()
            .map(|(i, data)| bs::WriteRequest {
                resource_name: resource.clone(),
                write_offset: (i * 128 * 1024) as i64,
                data: data.to_vec(),
                finish_write: (i + 1) * 128 * 1024 == bytes.len(),
            })
            .collect();
        let mut client = client.clone();
        let request = h.request(tokio_stream::iter(chunks));
        uploads.spawn(async move {
            client.write(request).await.unwrap();
        });
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(result) = uploads.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("streaming uploads stalled behind unread queued request bodies");
    assert_eq!(h.backend.blobs.lock().unwrap().len(), 32);
}
