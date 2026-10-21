use anyhow::Result;
use cache_proxy::{CacheProxy, ProxyConfig, Upstream};
use futures_core::Stream;
use local_cas::{CacheConfig, LocalCas};
use reapi::{
    asset,
    proto::google::bytestream::{
        self as bs,
        byte_stream_client::ByteStreamClient,
        byte_stream_server::{ByteStream, ByteStreamServer},
    },
    proto::google::longrunning as lro,
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
    read_body_fault: Arc<AtomicU64>,
    ac_update_entered: Arc<tokio::sync::Notify>,
    ac_update_release: Arc<tokio::sync::Notify>,
    ac_update_block: Arc<AtomicBool>,
    ac_update_fail: Arc<AtomicBool>,
    write_ack_mode: Arc<AtomicU64>,
    ac_reads: Arc<AtomicU64>,
    ac_result: Arc<Mutex<Option<ActionResult>>>,
    ac_error: Arc<AtomicBool>,
    missing_requests: Arc<Mutex<Vec<FindMissingBlobsRequest>>>,
    missing_delay_ms: Arc<AtomicU64>,
    reject_writes: Arc<Mutex<bool>>,
    write_calls: Arc<AtomicU64>,
    write_delay_ms: Arc<AtomicU64>,
    write_transient: Arc<AtomicU64>,
    bad_write_ack: Arc<AtomicBool>,
    write_resources: Arc<Mutex<Vec<String>>>,
    reject_compressed_writes: Arc<AtomicBool>,
    execution_calls: Arc<AtomicU64>,
    grpc_gzip: Arc<AtomicBool>,
    operations_success: Arc<AtomicBool>,
    execution_capabilities: Arc<Mutex<Option<ExecutionCapabilities>>>,
    execution_live: Arc<AtomicU64>,
    execution_delay_ms: Arc<AtomicU64>,
    execution_error: Arc<AtomicBool>,
    operation_names: Arc<Mutex<Vec<String>>>,
    fetch_calls: Arc<AtomicU64>,
    fetch_error: Arc<AtomicU64>,
    fetch_expired: Arc<AtomicBool>,
    fetch_requests: Arc<Mutex<Vec<asset::FetchBlobRequest>>>,
}
impl Backend {
    fn response<T>(&self, value: T) -> Response<T> {
        let mut response = Response::new(value);
        if self.grpc_gzip.load(Ordering::Relaxed) {
            response
                .metadata_mut()
                .insert("grpc-accept-encoding", "gzip".parse().unwrap());
        }
        response
    }
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
        let read_call = self.reads.fetch_add(1, Ordering::Relaxed);
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

        let fault = self.read_body_fault.load(Ordering::Relaxed);
        if fault != 0 && (read_call == 0 || fault == 3) {
            return Ok(Response::new(Box::pin(async_stream::try_stream! {
                yield bs::ReadResponse { data: bytes[..bytes.len() / 2].to_vec() };
                if fault == 1 {
                    Err(Status::unavailable("fixture interrupted body"))?;
                } else {
                    std::future::pending::<()>().await;
                }
            })));
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(vec![Ok(
            bs::ReadResponse { data: bytes },
        )]))))
    }
    async fn write(
        &self,
        req: Request<tonic::Streaming<bs::WriteRequest>>,
    ) -> Result<Response<bs::WriteResponse>, Status> {
        self.observe("write", &req);
        self.write_calls.fetch_add(1, Ordering::Relaxed);
        if self
            .write_transient
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Status::unavailable("fixture transient upload"));
        }
        tokio::time::sleep(Duration::from_millis(
            self.write_delay_ms.load(Ordering::Relaxed),
        ))
        .await;
        if *self.reject_writes.lock().unwrap() {
            return Err(Status::permission_denied("write blocked"));
        }
        let mut stream = req.into_inner();
        let mut bytes = vec![];
        let mut name = String::new();
        let mut finished = false;
        while let Some(req) = stream.message().await? {
            name = req.resource_name;
            finished |= req.finish_write;
            bytes.extend(req.data);
        }
        if !finished {
            return Err(Status::invalid_argument("fixture upload incomplete"));
        }
        self.write_resources.lock().unwrap().push(name.clone());
        let hash = name.split('/').rev().nth(1).unwrap().to_string();
        let compressed = name.contains("compressed-blobs");
        if compressed
            && (self.identity_only.load(Ordering::Relaxed)
                || self.reject_compressed_writes.load(Ordering::Relaxed))
        {
            return Err(Status::unimplemented("zstd unsupported"));
        }
        let size = bytes.len();
        let bytes = if compressed {
            reapi::compression::decode_zstd(&bytes, 1 << 20)
                .map_err(|_| Status::data_loss("fixture invalid compressed upload"))?
        } else {
            bytes
        };
        let logical_size = bytes.len() as i64;
        self.blobs.lock().unwrap().insert(hash, bytes);
        Ok(Response::new(bs::WriteResponse {
            committed_size: if self.bad_write_ack.load(Ordering::Relaxed) {
                -2
            } else if compressed {
                match self.write_ack_mode.load(Ordering::Relaxed) {
                    1 => logical_size,
                    2 => size as i64,
                    3 => logical_size + size as i64 + 1,
                    _ => -1,
                }
            } else {
                size as i64
            },
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
        if self.ac_error.load(Ordering::Relaxed) {
            return Err(Status::unavailable("fixture AC unavailable"));
        }
        if let Some(value) = self.ac_result.lock().unwrap().clone() {
            return Ok(self.response(value));
        }
        Ok(self.response(ActionResult {
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
        if self.ac_update_block.load(Ordering::Relaxed) {
            self.ac_update_entered.notify_one();
            self.ac_update_release.notified().await;
        }
        let value = req.into_inner().action_result.unwrap_or_default();
        *self.ac_result.lock().unwrap() = Some(value.clone());
        if self.ac_update_fail.load(Ordering::Relaxed) {
            return Err(Status::unavailable(
                "fixture committed without acknowledgment",
            ));
        }
        Ok(Response::new(value))
    }
}
#[tonic::async_trait]
impl capabilities_server::Capabilities for Backend {
    async fn get_capabilities(
        &self,
        _: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        let response = ServerCapabilities {
            execution_capabilities: Some(
                self.execution_capabilities
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or(ExecutionCapabilities {
                        digest_function: 9,
                        digest_functions: vec![1, 9, 2],
                        exec_enabled: true,
                        ..Default::default()
                    }),
            ),
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
        if self.grpc_gzip.load(Ordering::Relaxed) {
            response
                .metadata_mut()
                .insert("grpc-accept-encoding", "gzip".parse().unwrap());
        }
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
        self.missing_requests
            .lock()
            .unwrap()
            .push(req.get_ref().clone());
        tokio::time::sleep(Duration::from_millis(
            self.missing_delay_ms.load(Ordering::Relaxed),
        ))
        .await;
        Ok(self.response(FindMissingBlobsResponse {
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
const OPERATION_NAME: &str = "repo:region/uploads/task-1/blobs/opaque/1";
struct ExecutionLease(Arc<AtomicU64>);
impl Drop for ExecutionLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Backend {
    fn execution(&self) -> Response<S<lro::Operation>> {
        self.execution_live.fetch_add(1, Ordering::SeqCst);
        let lease = ExecutionLease(self.execution_live.clone());
        let this = self.clone();
        let stream: S<lro::Operation> = Box::pin(async_stream::try_stream! {
            let _lease=lease;
            yield lro::Operation {name:OPERATION_NAME.into(),..Default::default()};
            tokio::time::sleep(Duration::from_millis(this.execution_delay_ms.load(Ordering::SeqCst))).await;
            if this.execution_error.load(Ordering::SeqCst) {Err(Status::with_details(tonic::Code::Unavailable,"fixture disconnected",vec![8,7].into()))?;}
            yield lro::Operation {name:OPERATION_NAME.into(),done:true,..Default::default()};
        });
        self.response(stream)
    }
}
#[tonic::async_trait]
impl execution_server::Execution for Backend {
    type ExecuteStream = S<lro::Operation>;
    type WaitExecutionStream = S<lro::Operation>;
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        self.observe("execute", &request);
        self.execution_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.execution())
    }
    async fn wait_execution(
        &self,
        request: Request<WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        self.observe("wait_execution", &request);
        self.operation_names
            .lock()
            .unwrap()
            .push(request.into_inner().name);
        Ok(self.execution())
    }
}
#[tonic::async_trait]
impl lro::operations_server::Operations for Backend {
    async fn get_operation(
        &self,
        request: Request<lro::GetOperationRequest>,
    ) -> Result<Response<lro::Operation>, Status> {
        self.observe("get_operation", &request);
        self.operation_names
            .lock()
            .unwrap()
            .push(request.into_inner().name);
        if self.operations_success.load(Ordering::SeqCst) {
            return Ok(self.response(Default::default()));
        }
        Err(Status::unimplemented("fixture operation unsupported"))
    }
    async fn list_operations(
        &self,
        request: Request<lro::ListOperationsRequest>,
    ) -> Result<Response<lro::ListOperationsResponse>, Status> {
        self.observe("list_operations", &request);
        self.operation_names
            .lock()
            .unwrap()
            .push(request.into_inner().name);
        if self.operations_success.load(Ordering::SeqCst) {
            return Ok(self.response(Default::default()));
        }
        Err(Status::unimplemented("fixture operation unsupported"))
    }
    async fn delete_operation(
        &self,
        request: Request<lro::DeleteOperationRequest>,
    ) -> Result<Response<()>, Status> {
        self.observe("delete_operation", &request);
        self.operation_names
            .lock()
            .unwrap()
            .push(request.into_inner().name);
        if self.operations_success.load(Ordering::SeqCst) {
            return Ok(self.response(Default::default()));
        }
        Err(Status::unimplemented("fixture operation unsupported"))
    }
    async fn cancel_operation(
        &self,
        request: Request<lro::CancelOperationRequest>,
    ) -> Result<Response<()>, Status> {
        self.observe("cancel_operation", &request);
        self.operation_names
            .lock()
            .unwrap()
            .push(request.into_inner().name);
        if self.operations_success.load(Ordering::SeqCst) {
            return Ok(self.response(Default::default()));
        }
        Err(Status::unimplemented("fixture operation unsupported"))
    }
    async fn wait_operation(
        &self,
        request: Request<lro::WaitOperationRequest>,
    ) -> Result<Response<lro::Operation>, Status> {
        self.observe("wait_operation", &request);
        self.operation_names
            .lock()
            .unwrap()
            .push(request.into_inner().name);
        if self.operations_success.load(Ordering::SeqCst) {
            return Ok(self.response(Default::default()));
        }
        Err(Status::unimplemented("fixture operation unsupported"))
    }
}
#[tonic::async_trait]
impl asset::fetch_server::Fetch for Backend {
    async fn fetch_blob(
        &self,
        request: Request<asset::FetchBlobRequest>,
    ) -> Result<Response<asset::FetchBlobResponse>, Status> {
        self.observe("fetch_blob", &request);
        self.fetch_calls.fetch_add(1, Ordering::SeqCst);
        let request = request.into_inner();
        self.fetch_requests.lock().unwrap().push(request.clone());
        if self.fetch_error.load(Ordering::SeqCst) == 2 {
            return Err(Status::unavailable("fixture origin unavailable"));
        }
        let uri = request.uris.first().cloned().unwrap_or_default();
        let bytes = format!("asset-body:{uri}").repeat(16).into_bytes();
        let d = digest(&bytes);
        self.blobs.lock().unwrap().insert(d.hash.clone(), bytes);
        let mut response = asset::FetchBlobResponse {
            uri,
            blob_digest: Some(d),
            digest_function: 9,
            qualifiers: request.qualifiers,
            ..Default::default()
        };
        if self.fetch_error.load(Ordering::SeqCst) == 1 {
            response.status = Some(reapi::proto::google::rpc::Status {
                code: 5,
                message: "fixture inline absence".into(),
                ..Default::default()
            });
        }
        if self.fetch_expired.load(Ordering::SeqCst) {
            response.expires_at = Some(Default::default());
        }
        Ok(self.response(response))
    }
    async fn fetch_directory(
        &self,
        request: Request<asset::FetchDirectoryRequest>,
    ) -> Result<Response<asset::FetchDirectoryResponse>, Status> {
        self.observe("fetch_directory", &request);
        Err(Status::unimplemented("fixture directory unsupported"))
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
        Self::with_upstream_stream_limit(ttl, None).await
    }
    async fn with_upstream_stream_limit(ttl: Duration, limit: Option<u32>) -> Self {
        Self::configured(ttl, limit, |_| {}, CacheConfig::default()).await
    }
    async fn configured(
        ttl: Duration,
        limit: Option<u32>,
        configure: impl FnOnce(&mut ProxyConfig),
        cache_config: CacheConfig,
    ) -> Self {
        Self::with_transport(ttl, limit, configure, cache_config, true).await
    }
    async fn with_transport(
        ttl: Duration,
        limit: Option<u32>,
        configure: impl FnOnce(&mut ProxyConfig),
        cache_config: CacheConfig,
        gzip: bool,
    ) -> Self {
        let backend = Backend::default();
        backend.grpc_gzip.store(gzip, Ordering::Relaxed);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let fake = backend.clone();
        let task = tokio::spawn(async move {
            let execution = execution_server::ExecutionServer::new(fake.clone());
            let operations = lro::operations_server::OperationsServer::new(fake.clone());
            let fetch = asset::fetch_server::FetchServer::new(fake.clone());
            let capabilities = capabilities_server::CapabilitiesServer::new(fake.clone());
            let (execution, operations, fetch, capabilities) = if gzip {
                (
                    execution
                        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                        .send_compressed(tonic::codec::CompressionEncoding::Gzip),
                    operations
                        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                        .send_compressed(tonic::codec::CompressionEncoding::Gzip),
                    fetch
                        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                        .send_compressed(tonic::codec::CompressionEncoding::Gzip),
                    capabilities.accept_compressed(tonic::codec::CompressionEncoding::Gzip),
                )
            } else {
                (execution, operations, fetch, capabilities)
            };
            let ac = action_cache_server::ActionCacheServer::new(fake.clone());
            let cas = content_addressable_storage_server::ContentAddressableStorageServer::new(
                fake.clone(),
            );
            let (ac, cas) = if gzip {
                (
                    ac.accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                        .send_compressed(tonic::codec::CompressionEncoding::Gzip),
                    cas.accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                        .send_compressed(tonic::codec::CompressionEncoding::Gzip),
                )
            } else {
                (ac, cas)
            };
            Server::builder()
                .max_concurrent_streams(limit)
                .add_service(
                    ByteStreamServer::new(fake.clone())
                        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                        .send_compressed(tonic::codec::CompressionEncoding::Gzip),
                )
                .add_service(ac)
                .add_service(capabilities)
                .add_service(cas)
                .add_service(execution)
                .add_service(operations)
                .add_service(fetch)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let mut config = ProxyConfig {
            upstream: format!("http://{upstream_addr}"),
            backend_identity: "test-security-scope".into(),
            instance_name: "repo".into(),
            action_cache_ttl: ttl,
            upstream_headers: vec![("x-buildbuddy-api-key".into(), "daemon-key".into())],
            local_bearer_token: Some("test-token".into()),
            ..Default::default()
        };
        configure(&mut config);
        let upstream = Upstream::connect(config).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cache = LocalCas::open(dir.path(), upstream.clone(), cache_config).unwrap();
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
fn retention_policy(config: &mut ProxyConfig) {
    config.upstream_retention = Duration::from_secs(5);
    config.retention_refresh_interval = Duration::from_millis(300);
    config.policy.retry.max_attempts = 1;
}
struct LocalBytes(Vec<u8>);
#[async_trait::async_trait]
impl local_cas::BlobSource for LocalBytes {
    async fn fetch(&self, _: &local_cas::Artifact, file: &mut std::fs::File) -> Result<()> {
        use std::io::Write;
        file.write_all(&self.0)?;
        Ok(())
    }
    async fn missing(&self, artifacts: &[local_cas::Artifact]) -> Result<Vec<local_cas::Artifact>> {
        Ok(artifacts.to_vec())
    }
}
fn action_request() -> GetActionResultRequest {
    GetActionResultRequest {
        instance_name: "repo".into(),
        action_digest: Some(digest(b"retained-action")),
        digest_function: 9,
        ..Default::default()
    }
}
async fn eventually(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("background retention progress");
}
#[tokio::test]
async fn retention_keeps_ac_validity_fixed_and_refreshes_complete_output_references() {
    use prost::Message;
    let h = Harness::configured(
        Duration::from_millis(600),
        None,
        |c| {
            retention_policy(c);
            c.retention_refresh_interval = Duration::from_millis(1500);
        },
        CacheConfig::default(),
    )
    .await;
    let mut expected = vec![];
    let mut store = |bytes: &[u8]| {
        let d = digest(bytes);
        expected.push(d.clone());
        h.backend
            .blobs
            .lock()
            .unwrap()
            .insert(d.hash.clone(), bytes.to_vec());
        d
    };
    let direct = store(b"direct");
    let stdout = store(b"stdout");
    let stderr = store(b"stderr");
    let nested = store(b"nested");
    let file = |d: Digest| FileNode {
        name: "file".into(),
        digest: Some(d),
        ..Default::default()
    };
    let tree_child = Directory {
        files: vec![file(nested)],
        ..Default::default()
    };
    let tree = Tree {
        root: Some(Directory {
            directories: vec![DirectoryNode {
                name: "child".into(),
                digest: Some(digest(&tree_child.encode_to_vec())),
            }],
            ..Default::default()
        }),
        children: vec![tree_child],
    };
    let td = store(&tree.encode_to_vec());
    let child = Directory {
        files: vec![file(store(b"directory file"))],
        ..Default::default()
    };
    let cd = store(&child.encode_to_vec());
    let root = Directory {
        directories: vec![DirectoryNode {
            name: "child".into(),
            digest: Some(cd),
        }],
        ..Default::default()
    };
    let rd = store(&root.encode_to_vec());
    *h.backend.ac_result.lock().unwrap() = Some(ActionResult {
        output_files: vec![OutputFile {
            path: "direct".into(),
            digest: Some(direct),
            ..Default::default()
        }],
        output_directories: vec![
            OutputDirectory {
                path: "tree".into(),
                tree_digest: Some(td),
                ..Default::default()
            },
            OutputDirectory {
                path: "directories".into(),
                root_directory_digest: Some(rd),
                ..Default::default()
            },
        ],
        stdout_digest: Some(stdout),
        stderr_digest: Some(stderr),
        ..Default::default()
    });
    let mut ac = action_cache_client::ActionCacheClient::connect(h.url.clone())
        .await
        .unwrap();
    ac.get_action_result(h.request(action_request()))
        .await
        .unwrap();
    eventually(|| h.proxy.metrics().retention.output_refreshes >= 1).await;
    assert_eq!(
        h.backend.ac_reads.load(Ordering::Relaxed),
        1,
        "first output validation does not duplicate the AC RPC"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    ac.get_action_result(h.request(action_request()))
        .await
        .unwrap();
    assert_eq!(h.backend.ac_reads.load(Ordering::Relaxed), 1);
    tokio::time::sleep(Duration::from_millis(600)).await;
    ac.get_action_result(h.request(action_request()))
        .await
        .unwrap();
    assert_eq!(
        h.backend.ac_reads.load(Ordering::Relaxed),
        2,
        "local hits never slide AC expiry"
    );
    ac.get_action_result(h.request(action_request()))
        .await
        .unwrap();
    eventually(|| h.proxy.metrics().retention.action_refreshes >= 1).await;
    assert_eq!(h.backend.ac_reads.load(Ordering::Relaxed), 3);
    {
        let requests = h.backend.missing_requests.lock().unwrap();
        for d in expected {
            assert!(
                requests.iter().any(|r| r.blob_digests.contains(&d)),
                "unrefreshed digest {d:?}"
            );
        }
    }
    assert_eq!(
        h.backend.reads.load(Ordering::Relaxed),
        6,
        "only Tree/Directory metadata was downloaded during two closure checks"
    );
    assert_eq!(h.cache.metrics().fetches, 0, "no eager output hydration");
    for _ in 0..8 {
        ac.get_action_result(h.request(action_request()))
            .await
            .unwrap();
    }
    assert_eq!(
        h.backend.ac_reads.load(Ordering::Relaxed),
        3,
        "renewed entries deduplicate refreshes"
    );
}
#[tokio::test]
async fn retention_failure_does_not_extend_ac_and_missing_outputs_invalidate_it() {
    for missing in [false, true] {
        let h = Harness::configured(
            Duration::from_millis(700),
            None,
            retention_policy,
            CacheConfig::default(),
        )
        .await;
        *h.backend.ac_result.lock().unwrap() = Some(ActionResult {
            stdout_digest: Some(digest(b"absent log")),
            ..Default::default()
        });
        if !missing {
            let log = digest(b"absent log");
            h.backend
                .blobs
                .lock()
                .unwrap()
                .insert(log.hash, b"absent log".to_vec());
        }
        let mut ac = action_cache_client::ActionCacheClient::connect(h.url.clone())
            .await
            .unwrap();
        ac.get_action_result(h.request(action_request()))
            .await
            .unwrap();
        if !missing {
            eventually(|| h.proxy.metrics().retention.output_refreshes >= 1).await;
            h.backend.ac_error.store(true, Ordering::Relaxed);
            for _ in 0..8 {
                ac.get_action_result(h.request(action_request()))
                    .await
                    .unwrap();
            }
        }
        eventually(|| h.proxy.metrics().retention.failures >= 1).await;
        if missing {
            let before = h.backend.ac_reads.load(Ordering::Relaxed);
            ac.get_action_result(h.request(action_request()))
                .await
                .unwrap();
            assert_eq!(
                h.backend.ac_reads.load(Ordering::Relaxed),
                before + 1,
                "known missing output invalidates still-young AC metadata"
            );
        } else {
            tokio::time::sleep(Duration::from_millis(750)).await;
            let before = h.backend.ac_reads.load(Ordering::Relaxed);
            assert_eq!(
                ac.get_action_result(h.request(action_request()))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::Unavailable
            );
            assert!(
                h.backend.ac_reads.load(Ordering::Relaxed) > before,
                "failed maintenance cannot postpone foreground validation"
            );
        }
    }
}
#[tokio::test]
async fn hourly_ac_revalidation_does_not_postpone_output_maintenance() {
    let h = Harness::configured(
        Duration::ZERO,
        None,
        |c| {
            retention_policy(c);
            c.action_cache_ttl = Duration::from_millis(30);
        },
        CacheConfig::default(),
    )
    .await;
    let log = digest(b"retained log");
    h.backend
        .blobs
        .lock()
        .unwrap()
        .insert(log.hash.clone(), b"retained log".to_vec());
    *h.backend.ac_result.lock().unwrap() = Some(ActionResult {
        stdout_digest: Some(log.clone()),
        ..Default::default()
    });
    let mut ac = action_cache_client::ActionCacheClient::connect(h.url.clone())
        .await
        .unwrap();
    for _ in 0..10 {
        ac.get_action_result(h.request(action_request()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(70)).await;
    }
    let checks = h
        .backend
        .missing_requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.blob_digests.contains(&log))
        .count();
    assert!(
        checks >= 2,
        "a second output check must complete while foreground revalidation is still active; the initial check alone is insufficient"
    );
}
#[tokio::test]
async fn input_presence_does_not_crowd_outputs_out_of_retention_tracking() {
    let h = Harness::configured(
        Duration::ZERO,
        None,
        |c| {
            retention_policy(c);
            c.retention_max_entries = 1;
        },
        CacheConfig::default(),
    )
    .await;
    let input = digest(b"input");
    h.backend
        .blobs
        .lock()
        .unwrap()
        .insert(input.hash.clone(), b"input".to_vec());
    let mut cas =
        content_addressable_storage_client::ContentAddressableStorageClient::connect(h.url.clone())
            .await
            .unwrap();
    // Large, valid mixed-presence boundary: observation must remain linear,
    // and none of these input-only identities may occupy the local-read hot set.
    let mut inputs = (0..50_000u64)
        .map(|n| digest(&n.to_le_bytes()))
        .collect::<Vec<_>>();
    inputs.push(input);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        cas.find_missing_blobs(h.request(FindMissingBlobsRequest {
            instance_name: "repo".into(),
            digest_function: 9,
            blob_digests: inputs,
        })),
    )
    .await
    .expect("large foreground presence response must remain bounded")
    .unwrap();
    assert_eq!(result.into_inner().missing_blob_digests.len(), 50_000);
    h.backend.missing_requests.lock().unwrap().clear();
    let d = digest(b"output");
    h.backend
        .blobs
        .lock()
        .unwrap()
        .insert(d.hash.clone(), b"output".to_vec());
    let a = local_cas::Artifact {
        backend: "test-security-scope".into(),
        instance: "repo".into(),
        digest_function: 9,
        hash: d.hash.clone(),
        size: d.size_bytes as u64,
    };
    drop(h.cache.clone().content(a).await.unwrap());
    eventually(|| {
        h.backend
            .missing_requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.blob_digests.contains(&d))
    })
    .await;
    assert_eq!(h.proxy.metrics().retention.dropped, 0);
}
#[tokio::test]
async fn shared_cas_hits_refresh_in_batches_without_blocking_or_pinning_outputs() {
    let h = Harness::configured(
        Duration::ZERO,
        None,
        |c| {
            retention_policy(c);
            c.retention_refresh_interval = Duration::from_millis(700);
        },
        CacheConfig {
            max_bytes: 8,
            ..Default::default()
        },
    )
    .await;
    let mut artifacts = vec![];
    for bytes in [b"aaaa", b"bbbb"] {
        let d = digest(bytes);
        h.backend
            .blobs
            .lock()
            .unwrap()
            .insert(d.hash.clone(), bytes.to_vec());
        let a = local_cas::Artifact {
            backend: "test-security-scope".into(),
            instance: "repo".into(),
            digest_function: 9,
            hash: d.hash,
            size: 4,
        };
        drop(h.cache.clone().content(a.clone()).await.unwrap());
        artifacts.push(a);
    }
    let mut cas =
        content_addressable_storage_client::ContentAddressableStorageClient::connect(h.url.clone())
            .await
            .unwrap();
    cas.find_missing_blobs(
        h.request(FindMissingBlobsRequest {
            instance_name: "repo".into(),
            digest_function: 9,
            blob_digests: artifacts
                .iter()
                .map(|a| Digest {
                    hash: a.hash.clone(),
                    size_bytes: a.size as i64,
                })
                .collect(),
        }),
    )
    .await
    .unwrap();
    h.backend.missing_requests.lock().unwrap().clear();
    for a in &artifacts {
        drop(h.cache.clone().cached_content(a.clone()).await.unwrap());
    }
    eventually(|| h.proxy.metrics().retention.blob_refreshes >= 2).await;
    assert!(
        h.backend
            .missing_requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.blob_digests.len() == 2),
        "ready outputs share one presence batch"
    );
    let before = h.backend.missing_requests.lock().unwrap().len();
    for _ in 0..10 {
        drop(
            h.cache
                .clone()
                .cached_content(artifacts[0].clone())
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.backend.missing_requests.lock().unwrap().len(), before);
    h.backend.missing_delay_ms.store(500, Ordering::Relaxed);
    eventually(|| h.backend.missing_requests.lock().unwrap().len() > before).await;
    let lease = tokio::time::timeout(
        Duration::from_millis(150),
        h.cache.clone().content(artifacts[0].clone()),
    )
    .await
    .expect("foreground read cannot wait for maintenance")
    .unwrap();
    assert_eq!(std::fs::read(lease.path()).unwrap(), b"aaaa");
    drop(lease);
    h.backend.blobs.lock().unwrap().clear();
    eventually(|| h.proxy.metrics().retention.missing_blobs > 0).await;
    assert!(
        h.cache
            .clone()
            .cached_content(artifacts[0].clone())
            .await
            .unwrap()
            .is_some(),
        "immutable local bytes remain usable even after upstream eviction"
    );
    let bytes = b"cccc";
    let d = digest(bytes);
    h.backend
        .blobs
        .lock()
        .unwrap()
        .insert(d.hash.clone(), bytes.to_vec());
    let a = local_cas::Artifact {
        hash: d.hash,
        ..artifacts[0].clone()
    };
    drop(h.cache.clone().content(a).await.unwrap());
    assert!(
        h.cache.metrics().evictions > 0,
        "maintenance cannot pin output bytes against LRU"
    );
    h.proxy.stop_forwarding();
}
fn local_artifact(bytes: &[u8], function: i32) -> local_cas::Artifact {
    use sha2::{Digest as _, Sha256};
    local_cas::Artifact {
        backend: "test-security-scope".into(),
        instance: "repo".into(),
        digest_function: function,
        hash: if function == 9 {
            digest(bytes).hash
        } else {
            format!("{:x}", Sha256::digest(bytes))
        },
        size: bytes.len() as u64,
    }
}
#[tokio::test]
async fn local_hits_repair_missing_upstream_blobs_without_blocking_readers() {
    for (name, function, identity, reject_zstd, transient, ack) in [
        ("compressed dedup acknowledgment", 9, false, false, 0, 0),
        ("compressed logical acknowledgment", 9, false, false, 0, 1),
        ("compressed wire acknowledgment", 9, false, false, 0, 2),
        ("identity SHA256", 1, true, false, 0, 0),
        ("advertised codec fallback", 9, false, true, 0, 0),
        ("transient retry", 9, false, false, 1, 0),
    ] {
        let h = Harness::configured(
            Duration::ZERO,
            None,
            |c| {
                retention_policy(c);
                c.retention_refresh_interval = Duration::from_secs(2);
                c.policy.retry.max_attempts = 3;
                c.policy.retry.initial_backoff_ms = 1;
            },
            CacheConfig::default(),
        )
        .await;
        h.backend.write_ack_mode.store(ack, Ordering::Relaxed);
        h.backend.identity_only.store(identity, Ordering::Relaxed);
        h.backend
            .reject_compressed_writes
            .store(reject_zstd, Ordering::Relaxed);
        h.backend
            .write_transient
            .store(transient, Ordering::Relaxed);
        h.backend.write_delay_ms.store(300, Ordering::Relaxed);
        let data = vec![b'k'; 200_000];
        let a = local_artifact(&data, function);
        drop(
            h.cache
                .clone()
                .content_from(a.clone(), Arc::new(LocalBytes(data.clone())))
                .await
                .unwrap(),
        );
        eventually(|| h.backend.write_calls.load(Ordering::Relaxed) > transient).await;
        let lease = tokio::time::timeout(
            Duration::from_millis(200),
            h.cache.clone().content(a.clone()),
        )
        .await
        .expect("local readers must not await a repair")
        .unwrap();
        assert_eq!(std::fs::read(lease.path()).unwrap(), data, "{name}");
        drop(lease);
        eventually(|| h.proxy.metrics().retention.blob_repairs == 1).await;
        assert_eq!(h.backend.blobs.lock().unwrap()[&a.hash], data, "{name}");
        assert_eq!(
            h.backend.reads.load(Ordering::Relaxed),
            0,
            "repair must never hydrate from upstream"
        );
        {
            let resources = h.backend.write_resources.lock().unwrap();
            assert!(
                resources
                    .last()
                    .unwrap()
                    .contains(if identity || reject_zstd {
                        "/blobs/"
                    } else {
                        "/compressed-blobs/zstd/"
                    }),
                "{name}"
            );
        }
        let calls = h.backend.write_calls.load(Ordering::Relaxed);
        for _ in 0..8 {
            drop(h.cache.clone().content(a.clone()).await.unwrap());
        }
        assert_eq!(
            h.backend.write_calls.load(Ordering::Relaxed),
            calls,
            "local hits deduplicate repair within the interval"
        );
        assert_eq!(
            h.proxy.metrics().retention.blob_repair_failures,
            0,
            "{name}"
        );
        h.proxy.stop_forwarding();
    }
}
#[tokio::test]
async fn failed_repair_does_not_confirm_retention_or_replace_local_content() {
    for (reject, bad_ack, ack_mode) in [(true, false, 0), (false, true, 0), (false, false, 3)] {
        let h = Harness::configured(
            Duration::ZERO,
            None,
            retention_policy,
            CacheConfig::default(),
        )
        .await;
        *h.backend.reject_writes.lock().unwrap() = reject;
        h.backend.bad_write_ack.store(bad_ack, Ordering::Relaxed);
        h.backend.write_ack_mode.store(ack_mode, Ordering::Relaxed);
        let data = b"retained local bytes".to_vec();
        let a = local_artifact(&data, 9);
        drop(
            h.cache
                .clone()
                .content_from(a.clone(), Arc::new(LocalBytes(data.clone())))
                .await
                .unwrap(),
        );
        eventually(|| h.proxy.metrics().retention.blob_repair_failures == 1).await;
        let metrics = h.proxy.metrics().retention;
        assert_eq!(metrics.blob_repairs, 0);
        assert_eq!(metrics.blob_refreshes, 0);
        let lease = h
            .cache
            .clone()
            .cached_content_for_maintenance(a, tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(std::fs::read(lease.path()).unwrap(), data);
        h.proxy.stop_forwarding();
    }
}
#[tokio::test]
async fn repair_skips_evicted_or_corrupt_local_bytes_without_fetching() {
    for corrupt in [false, true] {
        let h = Harness::configured(
            Duration::ZERO,
            None,
            retention_policy,
            CacheConfig {
                max_bytes: 4,
                ..Default::default()
            },
        )
        .await;
        h.backend.missing_delay_ms.store(300, Ordering::Relaxed);
        let a = local_artifact(b"aaaa", 9);
        let lease = h
            .cache
            .clone()
            .content_from(a.clone(), Arc::new(LocalBytes(b"aaaa".to_vec())))
            .await
            .unwrap();
        let path = lease.path().to_owned();
        drop(lease);
        eventually(|| !h.backend.missing_requests.lock().unwrap().is_empty()).await;
        if corrupt {
            std::fs::write(path, b"oops").unwrap();
        } else {
            drop(
                h.cache
                    .clone()
                    .content_from(
                        local_artifact(b"bbbb", 9),
                        Arc::new(LocalBytes(b"bbbb".to_vec())),
                    )
                    .await
                    .unwrap(),
            );
        }
        eventually(|| h.proxy.metrics().retention.blob_repair_failures > 0).await;
        assert_eq!(h.backend.write_calls.load(Ordering::Relaxed), 0);
        assert_eq!(h.backend.reads.load(Ordering::Relaxed), 0);
        h.proxy.stop_forwarding();
    }
}
#[tokio::test]
async fn shutdown_cancels_repair_and_releases_its_temporary_lease() {
    let h = Harness::configured(
        Duration::ZERO,
        None,
        retention_policy,
        CacheConfig {
            max_bytes: 8,
            ..Default::default()
        },
    )
    .await;
    h.backend.write_delay_ms.store(2000, Ordering::Relaxed);
    let a = local_artifact(b"aaaa", 9);
    drop(
        h.cache
            .clone()
            .content_from(a, Arc::new(LocalBytes(b"aaaa".to_vec())))
            .await
            .unwrap(),
    );
    eventually(|| h.backend.write_calls.load(Ordering::Relaxed) == 1).await;
    h.proxy.stop_forwarding();
    h.proxy.wait_for_maintenance_shutdown().await;
    h.cache.wait_for_idle().await.unwrap();
    let lease = h
        .cache
        .clone()
        .content_from(
            local_artifact(b"bbbbbbbb", 9),
            Arc::new(LocalBytes(b"bbbbbbbb".to_vec())),
        )
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(lease.path()).unwrap(),
        b"bbbbbbbb",
        "an abandoned repair must not retain its CAS lease"
    );
    assert_eq!(h.proxy.metrics().retention.blob_repairs, 0);
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
    for (ack_mode, bad_ack, success) in [
        (1, false, true),
        (2, false, true),
        (3, false, false),
        (0, true, false),
    ] {
        h.backend.write_ack_mode.store(ack_mode, Ordering::Relaxed);
        h.backend.bad_write_ack.store(bad_ack, Ordering::Relaxed);
        let result = client
            .write(h.request(tokio_stream::iter(vec![bs::WriteRequest {
                resource_name: format!(
                    "repo/uploads/ack/compressed-blobs/zstd/blake3/{}/{}",
                    d.hash, d.size_bytes
                ),
                write_offset: 0,
                finish_write: true,
                data: reapi::compression::encode_zstd(bytes).unwrap(),
            }])))
            .await;
        if success {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().code(), tonic::Code::DataLoss);
        }
    }
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
    let h = Harness::configured(
        Duration::ZERO,
        None,
        |c| {
            retention_policy(c);
            c.retention_refresh_interval = Duration::from_secs(3);
        },
        CacheConfig::default(),
    )
    .await;
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
    let a = local_cas::Artifact {
        backend: "test-security-scope".into(),
        instance: "repo".into(),
        digest_function: 9,
        hash: d.hash.clone(),
        size: d.size_bytes as u64,
    };
    drop(
        h.cache
            .clone()
            .content_from(a, Arc::new(LocalBytes(b"correct".to_vec())))
            .await
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        eventually(|| h.backend.missing_requests.lock().unwrap().iter().any(|r| r.blob_digests.contains(&d))).await;
    }).await.expect("corrupt fetch must not defer the first verified-local-read presence check for three seconds");
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
    // The one-stream backend forces h2's pending_open path (Buck2 #1123 /
    // h2 #860): unopened streams must not steal active connection credit.
    for (name, limit) in [
        ("normal backend", None),
        ("queued unopened streams", Some(1)),
    ] {
        let h = Harness::with_upstream_stream_limit(Duration::ZERO, limit).await;
        let mut client = ByteStreamClient::connect(h.url.clone()).await.unwrap();
        // Complete the first exchange before the burst so the client has
        // received the upstream's advertised concurrent-stream limit.
        let resource = format!("repo/uploads/warmup/blobs/blake3/{}/1", digest(b"x").hash);
        assert_eq!(
            client
                .query_write_status(h.request(bs::QueryWriteStatusRequest {
                    resource_name: resource
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
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
        .expect(name);
        assert_eq!(h.backend.blobs.lock().unwrap().len(), 32);
    }
}

#[tokio::test]
async fn scheduler_streams_preserve_policy_metadata_and_leave_cache_headroom() {
    let h = Harness::configured(
        Duration::ZERO,
        None,
        |c| {
            c.policy.rpc_timeout_ms = 30;
            c.policy.operation_timeout_ms = 40;
            c.policy.stream_idle_timeout_ms = 10;
            c.max_execution_streams = 1;
        },
        CacheConfig::default(),
    )
    .await;
    h.backend.execution_delay_ms.store(150, Ordering::SeqCst);
    let channel = tonic::transport::Endpoint::from_shared(h.url.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = execution_client::ExecutionClient::new(channel.clone())
        .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
    let mut request = h.request(ExecuteRequest {
        instance_name: "repo".into(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-buildbuddy-api-key", "caller-key".parse().unwrap());
    request
        .metadata_mut()
        .append("x-buildbuddy-platform.pool", "pool-a".parse().unwrap());
    request
        .metadata_mut()
        .append("x-buildbuddy-platform.pool", "pool-b".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-untrusted-header", "drop-me".parse().unwrap());
    let mut stream = client.execute(request).await.unwrap().into_inner();
    assert_eq!(
        stream.message().await.unwrap().unwrap().name,
        OPERATION_NAME
    );
    let queued_request = h.request(ExecuteRequest {
        instance_name: "repo".into(),
        ..Default::default()
    });
    let mut queued_client = client.clone();
    let queued = tokio::spawn(async move { queued_client.execute(queued_request).await });
    tokio::task::yield_now().await;
    assert!(
        !queued.is_finished(),
        "excess scheduler requests wait instead of exhausting client retries"
    );
    let mut caps = capabilities_client::CapabilitiesClient::new(channel.clone());
    let execution_caps = caps
        .get_capabilities(h.request(GetCapabilitiesRequest {
            instance_name: "repo".into(),
        }))
        .await
        .unwrap()
        .into_inner()
        .execution_capabilities
        .unwrap();
    assert!(execution_caps.exec_enabled);
    assert_eq!(execution_caps.digest_functions, vec![1, 9]);
    assert!(stream.message().await.unwrap().unwrap().done);
    assert!(stream.message().await.unwrap().is_none());
    let mut second = queued.await.unwrap().unwrap().into_inner();
    while second.message().await.unwrap().is_some() {}

    assert_eq!(h.backend.execution_calls.load(Ordering::SeqCst), 2);
    {
        let seen = h.backend.observed_metadata.lock().unwrap();
        let headers = &seen.iter().find(|(name, _)| name == "execute").unwrap().1;
        assert_eq!(headers.get("x-buildbuddy-api-key").unwrap(), "daemon-key");
        assert_eq!(
            headers
                .get_all("x-buildbuddy-platform.pool")
                .iter()
                .map(|v| v.to_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["pool-a", "pool-b"]
        );
        assert!(!headers.contains_key("x-untrusted-header"));
        assert!(
            !headers.contains_key("grpc-timeout"),
            "cache timeout must not become execution deadline"
        );
    }
    h.backend.execution_error.store(true, Ordering::SeqCst);
    let mut resumed = client
        .wait_execution(h.request(WaitExecutionRequest {
            name: OPERATION_NAME.into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resumed.message().await.unwrap().unwrap().name,
        OPERATION_NAME
    );
    let error = resumed.message().await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.details(), &[8, 7]);
    assert_eq!(
        h.backend.execution_calls.load(Ordering::SeqCst),
        2,
        "proxy must not replay Execute"
    );
    let mut operations = lro::operations_client::OperationsClient::new(channel);
    assert_eq!(
        operations
            .cancel_operation(h.request(lro::CancelOperationRequest {
                name: OPERATION_NAME.into()
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    assert!(
        h.backend
            .operation_names
            .lock()
            .unwrap()
            .iter()
            .all(|name| name == OPERATION_NAME)
    );
}

#[tokio::test]
async fn scheduler_caller_deadline_and_disconnect_release_streams() {
    let h = Harness::new(Duration::ZERO).await;
    h.backend.execution_delay_ms.store(5000, Ordering::SeqCst);
    let mut client = execution_client::ExecutionClient::connect(h.url.clone())
        .await
        .unwrap();
    let mut request = h.request(ExecuteRequest {
        instance_name: "repo".into(),
        ..Default::default()
    });
    request.set_timeout(Duration::from_millis(80));
    let mut stream = client.execute(request).await.unwrap().into_inner();
    assert!(!stream.message().await.unwrap().unwrap().done);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.message())
            .await
            .unwrap()
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    drop(stream);
    let mut stream = client
        .wait_execution(h.request(WaitExecutionRequest {
            name: OPERATION_NAME.into(),
        }))
        .await
        .unwrap()
        .into_inner();
    stream.message().await.unwrap().unwrap();
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.backend.execution_live.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.backend.execution_calls.load(Ordering::SeqCst), 1);
}

fn fetch_request(uri: &str) -> asset::FetchBlobRequest {
    asset::FetchBlobRequest {
        instance_name: "repo".into(),
        uris: vec![uri.into()],
        digest_function: 9,
        ..Default::default()
    }
}
async fn read_asset(h: &Harness, d: Digest) {
    let mut client = ByteStreamClient::connect(h.url.clone()).await.unwrap();
    let mut body = client
        .read(h.request(bs::ReadRequest {
            resource_name: format!("repo/blobs/blake3/{}/{}", d.hash, d.size_bytes),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    while body.message().await.unwrap().is_some() {}
}
#[tokio::test]
async fn downloader_mapping_uses_shared_cas_and_eviction_without_hiding_remote_absence() {
    let h = Harness::configured(
        Duration::ZERO,
        None,
        |_| {},
        CacheConfig {
            max_bytes: 600,
            max_inflight_bytes: 600,
            ..Default::default()
        },
    )
    .await;
    let mut client = asset::fetch_client::FetchClient::connect(h.url.clone())
        .await
        .unwrap()
        .send_compressed(tonic::codec::CompressionEncoding::Gzip)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
    let request = fetch_request("https://example.invalid/a");
    let value = client
        .fetch_blob(h.request(request.clone()))
        .await
        .unwrap()
        .into_inner();
    let d = value.blob_digest.unwrap();
    read_asset(&h, d.clone()).await;
    client.fetch_blob(h.request(request.clone())).await.unwrap();
    assert_eq!(h.backend.fetch_calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.proxy.metrics().asset_cache_hits, 1);
    let another = client
        .fetch_blob(h.request(fetch_request("https://example.invalid/b")))
        .await
        .unwrap()
        .into_inner()
        .blob_digest
        .unwrap();
    read_asset(&h, another).await;
    assert!(h.cache.metrics().evictions > 0);
    client.fetch_blob(h.request(request.clone())).await.unwrap();
    assert_eq!(
        h.backend.fetch_calls.load(Ordering::SeqCst),
        3,
        "evicted bytes invalidate metadata reuse"
    );
    read_asset(&h, d.clone()).await;
    h.backend.blobs.lock().unwrap().remove(&d.hash);
    client.fetch_blob(h.request(request)).await.unwrap();
    assert_eq!(
        h.backend.fetch_calls.load(Ordering::SeqCst),
        4,
        "local bytes must not claim upstream availability"
    );
}

#[tokio::test]
async fn downloader_preserves_request_identity_freshness_and_inline_errors() {
    let h = Harness::new(Duration::ZERO).await;
    let mut client = asset::fetch_client::FetchClient::connect(h.url.clone())
        .await
        .unwrap();
    let request = fetch_request("https://example.invalid/asset");
    let d = client
        .fetch_blob(h.request(request.clone()))
        .await
        .unwrap()
        .into_inner()
        .blob_digest
        .unwrap();
    read_asset(&h, d).await;
    let mut variants = Vec::new();
    let mut indexed = request.clone();
    indexed.uris.push("https://mirror.invalid/asset".into());
    indexed.qualifiers.push(asset::Qualifier {
        name: "http_header_url:0:Authorization".into(),
        value: "fixture-value".into(),
    });
    variants.push(indexed.clone());
    indexed.uris.reverse();
    variants.push(indexed);
    let mut fresh = request.clone();
    fresh.oldest_content_accepted = Some(Default::default());
    variants.push(fresh.clone());
    variants.push(fresh);
    for req in &variants {
        client.fetch_blob(h.request(req.clone())).await.unwrap();
    }
    assert_eq!(
        &h.backend.fetch_requests.lock().unwrap()[1..],
        variants.as_slice()
    );
    assert_eq!(h.backend.fetch_calls.load(Ordering::SeqCst), 5);
    h.backend.fetch_error.store(1, Ordering::SeqCst);
    for _ in 0..2 {
        assert_eq!(
            client
                .fetch_blob(h.request(fetch_request("https://example.invalid/missing")))
                .await
                .unwrap()
                .into_inner()
                .status
                .unwrap()
                .code,
            5
        );
    }
    h.backend.fetch_error.store(2, Ordering::SeqCst);
    assert_eq!(
        client
            .fetch_blob(h.request(fetch_request("https://example.invalid/outage")))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        h.backend.fetch_calls.load(Ordering::SeqCst),
        8,
        "transport failure must not trigger proxy retries"
    );
    assert_eq!(
        client
            .fetch_directory(h.request(asset::FetchDirectoryRequest {
                instance_name: "repo".into(),
                uris: request.uris,
                ..Default::default()
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    assert_eq!(
        client
            .fetch_blob(h.request(asset::FetchBlobRequest {
                instance_name: "other-scope".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}

#[tokio::test]
async fn downloader_mapping_expiry_and_metadata_lru_force_upstream_fetch() {
    let h = Harness::configured(
        Duration::ZERO,
        None,
        |c| {
            c.asset_cache_ttl = Duration::from_millis(100);
            c.max_asset_cache_entries = 1;
        },
        CacheConfig::default(),
    )
    .await;
    let mut client = asset::fetch_client::FetchClient::connect(h.url.clone())
        .await
        .unwrap();
    let a = fetch_request("https://example.invalid/a");
    let b = fetch_request("https://example.invalid/b");
    for request in [&a, &b, &a] {
        let d = client
            .fetch_blob(h.request(request.clone()))
            .await
            .unwrap()
            .into_inner()
            .blob_digest
            .unwrap();
        read_asset(&h, d).await;
    }
    assert_eq!(h.backend.fetch_calls.load(Ordering::SeqCst), 3);
    tokio::time::sleep(Duration::from_millis(120)).await;
    client.fetch_blob(h.request(a.clone())).await.unwrap();
    assert_eq!(h.backend.fetch_calls.load(Ordering::SeqCst), 4);
    h.backend.fetch_expired.store(true, Ordering::SeqCst);
    for _ in 0..2 {
        client.fetch_blob(h.request(b.clone())).await.unwrap();
    }
    assert_eq!(h.backend.fetch_calls.load(Ordering::SeqCst), 6);
}

#[tokio::test]
async fn execution_capabilities_prefer_modern_digest_list_and_disable_unsupported_backends() {
    let h = Harness::new(Duration::ZERO).await;
    let mut client = capabilities_client::CapabilitiesClient::connect(h.url.clone())
        .await
        .unwrap();
    for (name, legacy, modern, enabled, selected) in [
        (
            "modern list overrides unsupported legacy",
            2,
            vec![2, 1],
            true,
            1,
        ),
        ("legacy supported without modern list", 9, vec![], true, 9),
        ("modern list has no supported digest", 1, vec![2], false, 1),
        (
            "legacy unsupported without modern list",
            2,
            vec![],
            false,
            2,
        ),
    ] {
        *h.backend.execution_capabilities.lock().unwrap() = Some(ExecutionCapabilities {
            digest_function: legacy,
            digest_functions: modern,
            exec_enabled: true,
            ..Default::default()
        });
        let caps = client
            .get_capabilities(h.request(GetCapabilitiesRequest {
                instance_name: "repo".into(),
            }))
            .await
            .unwrap()
            .into_inner()
            .execution_capabilities
            .unwrap();
        assert_eq!(caps.exec_enabled, enabled, "{name}");
        assert_eq!(caps.digest_function, selected, "{name}");
    }
}

#[tokio::test]
async fn configured_execution_deadline_and_shutdown_close_quiet_streams() {
    for (name, lifetime, shutdown, code) in [
        (
            "configured lifetime",
            Some(Duration::from_millis(80)),
            false,
            tonic::Code::DeadlineExceeded,
        ),
        ("explicit shutdown", None, true, tonic::Code::Unavailable),
    ] {
        let h = Harness::configured(
            Duration::ZERO,
            None,
            |c| c.execution_timeout = lifetime,
            CacheConfig::default(),
        )
        .await;
        h.backend.execution_delay_ms.store(5000, Ordering::SeqCst);
        let mut client = execution_client::ExecutionClient::connect(h.url.clone())
            .await
            .unwrap();
        let mut stream = client
            .execute(h.request(ExecuteRequest {
                instance_name: "repo".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        stream.message().await.unwrap().unwrap();
        if shutdown {
            h.proxy.stop_forwarding();
        }
        let error = tokio::time::timeout(Duration::from_secs(1), stream.message())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code(), code, "{name}");
        assert_eq!(h.backend.execution_calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn scheduler_and_downloader_negotiate_upstream_compression_without_replay() {
    for gzip in [false, true] {
        let h = Harness::with_transport(Duration::ZERO, None, |_| {}, CacheConfig::default(), gzip)
            .await;
        h.backend.operations_success.store(true, Ordering::SeqCst);
        let mut caps = capabilities_client::CapabilitiesClient::connect(h.url.clone())
            .await
            .unwrap();
        caps.get_capabilities(h.request(GetCapabilitiesRequest {
            instance_name: "repo".into(),
        }))
        .await
        .unwrap();

        let mut client = execution_client::ExecutionClient::connect(h.url.clone())
            .await
            .unwrap()
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        let mut fetch = asset::fetch_client::FetchClient::connect(h.url.clone())
            .await
            .unwrap()
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        let mut operations = lro::operations_client::OperationsClient::connect(h.url.clone())
            .await
            .unwrap();
        for _ in 0..2 {
            let mut stream = client
                .execute(h.request(ExecuteRequest {
                    instance_name: "repo".into(),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            while stream.message().await.unwrap().is_some() {}
            fetch
                .fetch_blob(h.request(fetch_request("https://example.invalid/archive")))
                .await
                .unwrap();
            operations
                .get_operation(h.request(lro::GetOperationRequest {
                    name: OPERATION_NAME.into(),
                }))
                .await
                .unwrap();
        }
        assert_eq!(h.backend.execution_calls.load(Ordering::SeqCst), 2);
        let seen = h.backend.observed_metadata.lock().unwrap();
        for method in ["execute", "fetch_blob", "get_operation"] {
            let encodings = seen
                .iter()
                .filter(|(name, _)| name == method)
                .map(|(_, headers)| headers.get("grpc-encoding").and_then(|v| v.to_str().ok()))
                .collect::<Vec<_>>();
            assert_eq!(
                encodings,
                vec![None, if gzip { Some("gzip") } else { None }],
                "{method}, advertised gzip={gzip}"
            );
        }
    }
}

#[tokio::test]
async fn long_scheduler_stream_does_not_block_downloader_or_operation_control() {
    let h = Harness::with_upstream_stream_limit(Duration::ZERO, Some(1)).await;
    h.backend.execution_delay_ms.store(5000, Ordering::SeqCst);
    let mut execution = execution_client::ExecutionClient::connect(h.url.clone())
        .await
        .unwrap();
    let mut stream = execution
        .execute(h.request(ExecuteRequest {
            instance_name: "repo".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!stream.message().await.unwrap().unwrap().done);
    tokio::time::timeout(Duration::from_secs(1), async {
        let mut fetch = asset::fetch_client::FetchClient::connect(h.url.clone())
            .await
            .unwrap();
        fetch
            .fetch_blob(h.request(fetch_request("https://example.invalid/archive")))
            .await
            .unwrap();
        let mut operations = lro::operations_client::OperationsClient::connect(h.url.clone())
            .await
            .unwrap();
        assert_eq!(
            operations
                .cancel_operation(h.request(lro::CancelOperationRequest {
                    name: OPERATION_NAME.into()
                }))
                .await
                .unwrap_err()
                .message(),
            "fixture operation unsupported"
        );
    })
    .await
    .expect("long execution must not occupy the downloader/control connection");
    drop(stream);
}

#[tokio::test]
async fn action_update_completion_invalidates_overlapping_inline_reads_even_on_ambiguous_failure() {
    for fail in [false, true] {
        let h = Harness::configured(
            Duration::from_secs(30),
            None,
            |c| {
                c.policy.retry.max_attempts = 1;
            },
            CacheConfig::default(),
        )
        .await;
        let old = ActionResult {
            exit_code: 1,
            ..Default::default()
        };
        let new = ActionResult {
            exit_code: 2,
            ..Default::default()
        };
        *h.backend.ac_result.lock().unwrap() = Some(old.clone());
        h.backend.ac_update_block.store(true, Ordering::Relaxed);
        h.backend.ac_update_fail.store(fail, Ordering::Relaxed);
        let mut client = action_cache_client::ActionCacheClient::connect(h.url.clone())
            .await
            .unwrap();
        let variants: Vec<_> = (0..8)
            .map(|bits| GetActionResultRequest {
                instance_name: "repo".into(),
                action_digest: Some(digest(b"action-update")),
                digest_function: 9,
                inline_stdout: bits & 1 != 0,
                inline_stderr: bits & 2 != 0,
                inline_output_files: if bits & 4 != 0 {
                    vec!["output".into()]
                } else {
                    vec![]
                },
                ..Default::default()
            })
            .collect();
        let mut updating = client.clone();
        let request = h.request(UpdateActionResultRequest {
            instance_name: "repo".into(),
            action_digest: Some(digest(b"action-update")),
            digest_function: 9,
            action_result: Some(new.clone()),
            ..Default::default()
        });
        let task = tokio::spawn(async move { updating.update_action_result(request).await });
        tokio::time::timeout(
            Duration::from_secs(2),
            h.backend.ac_update_entered.notified(),
        )
        .await
        .unwrap();
        for req in &variants {
            assert_eq!(
                client
                    .get_action_result(h.request(req.clone()))
                    .await
                    .unwrap()
                    .into_inner(),
                old
            );
        }
        h.backend.ac_update_release.notify_one();
        assert_eq!(task.await.unwrap().is_err(), fail);
        let calls = h.backend.ac_reads.load(Ordering::Relaxed);
        for req in variants {
            assert_eq!(
                client
                    .get_action_result(h.request(req))
                    .await
                    .unwrap()
                    .into_inner(),
                new
            );
        }
        assert_eq!(h.backend.ac_reads.load(Ordering::Relaxed), calls + 8);
    }
}

#[tokio::test]
async fn interrupted_download_replays_complete_body_and_decoder_with_bounded_budget() {
    for compressed in [false, true] {
        for fault in [1, 2, 3] {
            let h = Harness::configured(
                Duration::ZERO,
                None,
                |c| {
                    c.policy.retry.max_attempts = if fault == 3 { 16 } else { 2 };
                    c.policy.retry.initial_backoff_ms = 1;
                    c.policy.retry.max_backoff_ms = 1;
                    c.policy.stream_idle_timeout_ms = 80;
                    c.policy.operation_timeout_ms = if fault == 3 { 180 } else { 2000 };
                },
                CacheConfig::default(),
            )
            .await;
            h.backend
                .identity_only
                .store(!compressed, Ordering::Relaxed);
            h.backend.read_body_fault.store(fault, Ordering::Relaxed);
            let data: Vec<_> = (0..200_000).map(|n| (n % 251) as u8).collect();
            let d = digest(&data);
            h.backend
                .blobs
                .lock()
                .unwrap()
                .insert(d.hash.clone(), data.clone());
            let a = local_artifact(&data, 9);
            let start = tokio::time::Instant::now();
            let fetched = h.cache.clone().content(a.clone()).await;
            if fault == 3 {
                assert!(fetched.is_err());
                assert!(start.elapsed() < Duration::from_secs(1));
                assert!(h.backend.reads.load(Ordering::Relaxed) <= 3);
                // A failed partial replay must not become a verified cache hit.
                h.backend.read_body_fault.store(0, Ordering::Relaxed);
                let before = h.backend.reads.load(Ordering::Relaxed);
                let blob = h.cache.clone().content(a).await.unwrap();
                assert_eq!(std::fs::read(blob.path()).unwrap(), data);
                assert_eq!(h.backend.reads.load(Ordering::Relaxed), before + 1);
            } else {
                assert_eq!(std::fs::read(fetched.unwrap().path()).unwrap(), data);
                assert_eq!(h.backend.reads.load(Ordering::Relaxed), 2);
            }
        }
    }
}

#[tokio::test]
async fn chunk_mapping_initial_receive_and_forwarding_share_a_finite_budget() {
    let h = Harness::configured(
        Duration::ZERO,
        Some(1),
        |c| {
            c.policy.stream_idle_timeout_ms = 60;
            c.policy.operation_timeout_ms = 100;
        },
        CacheConfig::default(),
    )
    .await;
    let channel = tonic::transport::Endpoint::from_shared(h.url.clone())
        .unwrap()
        .concurrency_limit(1)
        .connect()
        .await
        .unwrap();
    let mut cas = content_addressable_storage_client::ContentAddressableStorageClient::new(channel);
    for send_first in [false, true] {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        if send_first {
            sender
                .send(RegisterChunkMappingRequest {
                    instance_name: "repo".into(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        let start = tokio::time::Instant::now();
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            cas.register_chunk_mapping(
                h.request(tokio_stream::wrappers::ReceiverStream::new(receiver)),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert!(start.elapsed() < Duration::from_millis(500));
        // Keep the original sender open while verifying useful traffic recovers.
        cas.register_chunk_mapping(h.request(tokio_stream::iter(vec![
            RegisterChunkMappingRequest {
                instance_name: "repo".into(),
                ..Default::default()
            },
        ])))
        .await
        .unwrap();
        drop(sender);
    }

    // A stream that stays active must still exhaust the original operation
    // budget instead of resetting it for every received message.
    let active = async_stream::stream! {
        loop {
            yield RegisterChunkMappingRequest { instance_name: "repo".into(), ..Default::default() };
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        cas.register_chunk_mapping(h.request(active)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);

    // Shutdown also cancels an already-forwarded stream, independently of its
    // receive timeout. Observe upstream entry before initiating shutdown.
    h.backend.observed_metadata.lock().unwrap().clear();
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    sender
        .send(RegisterChunkMappingRequest {
            instance_name: "repo".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let request = h.request(tokio_stream::wrappers::ReceiverStream::new(receiver));
    let task = tokio::spawn(async move { cas.register_chunk_mapping(request).await });
    eventually(|| {
        h.backend
            .observed_metadata
            .lock()
            .unwrap()
            .iter()
            .any(|(method, _)| method == "register_chunk_mapping")
    })
    .await;
    h.proxy.stop_forwarding();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    drop(sender);
}
