//! One-user REAPI cache front end. Local CAS residence never implies RBE-side
//! presence: writes and presence queries always reach the configured backend.
mod download;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use futures_core::Stream;
use local_cas::{Artifact, BlobSource, LocalCas};
use prost::Message;
use reapi::proto::google::bytestream::{
    self as bs,
    byte_stream_client::ByteStreamClient,
    byte_stream_server::{ByteStream, ByteStreamServer},
};
use reapi::{policy::Policy, reapi::*};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    net::TcpListener,
};
use tonic::{
    Request, Response, Status,
    codec::CompressionEncoding,
    metadata::{MetadataKey, MetadataValue},
    transport::{Channel, Endpoint, Server},
};

type ActionEntries = HashMap<Vec<u8>, (Instant, ActionResult)>;

type RpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

const REQUEST_METADATA: &str = "build.bazel.remote.execution.v2.requestmetadata-bin";
/// Explicit attribution only. Authorization and transport metadata are never
/// copied from the caller; the daemon supplies upstream credentials.
#[derive(Clone, Default)]
struct CallerMetadata(Option<MetadataValue<tonic::metadata::Binary>>);
impl CallerMetadata {
    fn capture<T>(request: &Request<T>) -> Result<Self, Status> {
        let Some(value) = request.metadata().get_bin(REQUEST_METADATA) else {
            return Ok(Self::default());
        };
        let bytes = value
            .to_bytes()
            .map_err(|_| Status::invalid_argument("invalid request metadata encoding"))?;
        if bytes.len() > 8192 {
            return Err(Status::resource_exhausted("request metadata exceeds 8 KiB"));
        }
        Ok(Self(Some(value.clone())))
    }
}

#[derive(Clone)]
pub struct ProxyConfig {
    pub upstream: String,
    /// Security identity, not merely hostname. Must match other shared-CAS users.
    pub backend_identity: String,
    pub instance_name: String,
    pub upstream_headers: Vec<(String, String)>,
    pub local_bearer_token: Option<String>,
    pub policy: Policy,
    pub max_blob_bytes: u64,
    pub max_batch_bytes: usize,
    pub max_concurrent_rpcs_per_connection: usize,
    /// Nonzero requires an operator-provided upstream retention guarantee.
    /// Zero is safe with backends whose eviction policy is unknown.
    pub action_cache_ttl: Duration,
    pub max_action_cache_entries: usize,
}
impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            upstream: String::new(),
            backend_identity: String::new(),
            instance_name: String::new(),
            upstream_headers: vec![],
            local_bearer_token: None,
            policy: Policy::default(),
            max_blob_bytes: 512 * 1024 * 1024,
            max_batch_bytes: 4 * 1024 * 1024,
            max_concurrent_rpcs_per_connection: 128,
            action_cache_ttl: Duration::ZERO,
            max_action_cache_entries: 4096,
        }
    }
}
#[derive(Default)]
struct Counters {
    ac_hits: AtomicU64,
    ac_upstream: AtomicU64,
    blob_reads: AtomicU64,
}
#[derive(Clone, Copy, Debug)]
pub struct ProxyMetrics {
    pub action_cache_hits: u64,
    pub action_cache_upstream: u64,
    pub blob_reads: u64,
}
#[derive(Clone)]
pub struct CacheProxy {
    upstream: Arc<Upstream>,
    cache: Arc<LocalCas>,
    ac: Arc<Mutex<ActionEntries>>,
    metrics: Arc<Counters>,
    writes: Arc<tokio::sync::Semaphore>,
}
/// Reusable default source for `LocalCas::open`, sharing the proxy's security scope.
pub struct Upstream {
    config: ProxyConfig,
    channel: Channel,
    zstd_capability: tokio::sync::OnceCell<bool>,
    zstd_disabled: std::sync::atomic::AtomicBool,
}
impl Upstream {
    pub async fn connect(config: ProxyConfig) -> Result<Arc<Self>> {
        config.policy.validate()?;
        ensure!(
            (1..=1024).contains(&config.max_concurrent_rpcs_per_connection),
            "RPC concurrency limit must be 1..1024"
        );
        ensure!(
            !config.backend_identity.is_empty(),
            "cache backend identity is required"
        );
        ensure!(
            config.max_batch_bytes > 0 && config.max_batch_bytes <= 64 * 1024 * 1024,
            "batch limit must be 1..64 MiB"
        );
        ensure!(
            config.max_blob_bytes > 0 && config.max_blob_bytes <= i64::MAX as u64,
            "invalid blob limit"
        );
        ensure!(
            config.action_cache_ttl <= Duration::from_secs(3600),
            "action cache TTL exceeds one hour"
        );
        for (key, value) in &config.upstream_headers {
            let _: MetadataKey<tonic::metadata::Ascii> =
                key.parse().context("invalid upstream header name")?;
            let _: MetadataValue<tonic::metadata::Ascii> = value
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid upstream header value"))?;
        }
        let url = config
            .upstream
            .replace("grpcs://", "https://")
            .replace("grpc://", "http://");
        let use_tls = url.starts_with("https://");
        let endpoint = Endpoint::from_shared(url)
            .map_err(|_| anyhow::anyhow!("invalid upstream endpoint"))?
            .connect_timeout(config.policy.connect_timeout())
            .timeout(config.policy.rpc_timeout());
        let endpoint = if use_tls {
            endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new().with_webpki_roots())
                .context("configuring cache upstream TLS")?
        } else {
            endpoint
        };
        // Shared daemon startup and its durable BES admission must not depend
        // on the remote cache being reachable. RPCs keep their own deadlines;
        // a cold/offline backend fails those callers, not unrelated services.
        let channel = endpoint.connect_lazy();
        Ok(Arc::new(Self {
            config,
            channel,
            zstd_capability: tokio::sync::OnceCell::new(),
            zstd_disabled: std::sync::atomic::AtomicBool::new(false),
        }))
    }
    fn request<T>(&self, value: T) -> Request<T> {
        let mut req = Request::new(value);
        req.set_timeout(self.config.policy.rpc_timeout());
        for (key, value) in &self.config.upstream_headers {
            req.metadata_mut().insert(
                key.parse::<MetadataKey<tonic::metadata::Ascii>>().unwrap(),
                value.parse().unwrap(),
            );
        }
        req
    }
    fn request_with_metadata<T>(&self, value: T, metadata: &CallerMetadata) -> Request<T> {
        let mut request = self.request(value);
        if let Some(value) = &metadata.0 {
            request
                .metadata_mut()
                .insert_bin(REQUEST_METADATA, value.clone());
        }
        request
    }
    fn cas(&self) -> content_addressable_storage_client::ContentAddressableStorageClient<Channel> {
        content_addressable_storage_client::ContentAddressableStorageClient::new(
            self.channel.clone(),
        )
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(self.config.max_batch_bytes + 1024 * 1024)
        .max_encoding_message_size(self.config.max_batch_bytes + 1024 * 1024)
    }
    fn bytes(&self) -> ByteStreamClient<Channel> {
        ByteStreamClient::new(self.channel.clone()).accept_compressed(CompressionEncoding::Gzip)
    }
    fn ac(&self) -> action_cache_client::ActionCacheClient<Channel> {
        action_cache_client::ActionCacheClient::new(self.channel.clone())
            .accept_compressed(CompressionEncoding::Gzip)
    }
    async fn replay<T, F, Fut>(&self, mut operation: F) -> Result<T, Status>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        self.config
            .policy
            .retry(
                tokio::time::Instant::now() + self.config.policy.operation_timeout(),
                self.config.policy.rpc_timeout(),
                |_| operation(),
            )
            .await
    }
    fn bounded_stream<T: Message + Default + Send + 'static>(
        &self,
        mut stream: tonic::Streaming<T>,
    ) -> RpcStream<T> {
        let deadline = tokio::time::Instant::now() + self.config.policy.operation_timeout();
        let idle = self.config.policy.stream_idle_timeout();
        Box::pin(async_stream::try_stream! {
            loop {
                let step = deadline.min(tokio::time::Instant::now() + idle);
                let message = tokio::time::timeout_at(step, stream.message()).await.map_err(|_| Status::deadline_exceeded("upstream stream deadline exceeded"))??;
                match message { Some(message) => yield message, None => break }
            }
        })
    }
    fn scope(&self, instance: &str) -> Result<(), Status> {
        if instance == self.config.instance_name {
            Ok(())
        } else {
            Err(Status::permission_denied(
                "instance is outside daemon cache scope",
            ))
        }
    }
    fn artifact(&self, digest: &Digest, function: i32) -> Result<Artifact, Status> {
        if digest.size_bytes < 0 || digest.size_bytes as u64 > self.config.max_blob_bytes {
            return Err(Status::invalid_argument(
                "blob size outside configured limit",
            ));
        }
        let a = Artifact {
            backend: self.config.backend_identity.clone(),
            instance: self.config.instance_name.clone(),
            digest_function: function_id(function)?,
            hash: digest.hash.clone(),
            size: digest.size_bytes as u64,
        };
        a.validate()
            .map_err(|_| Status::invalid_argument("invalid blob digest"))?;
        Ok(a)
    }
}
fn function_id(value: i32) -> Result<i32, Status> {
    match value {
        0 | 1 => Ok(1),
        9 => Ok(9),
        _ => Err(Status::invalid_argument(
            "only SHA256 and BLAKE3 are supported",
        )),
    }
}
fn resource(a: &Artifact) -> String {
    format!(
        "{}{}blobs/{}{}/{}",
        a.instance,
        if a.instance.is_empty() { "" } else { "/" },
        if a.digest_function == 9 {
            "blake3/"
        } else {
            ""
        },
        a.hash,
        a.size
    )
}
fn clean_response<T>(response: Response<T>) -> Response<T> {
    // This is a new gRPC hop, not an HTTP tunnel. Upstream Content-Length,
    // grpc-encoding, trailers and other transport metadata describe its wire
    // representation, which differs after local capability filtering and codec
    // negotiation. Let tonic construct fresh downstream headers and trailers.
    Response::new(response.into_inner())
}

fn cache_status(error: anyhow::Error) -> Status {
    for cause in error.chain() {
        if let Some(status) = cause.downcast_ref::<Status>() {
            return status.clone();
        }
    }
    if error
        .downcast_ref::<local_cas::CacheTimeoutError>()
        .is_some()
    {
        return Status::deadline_exceeded("local CAS operation timed out");
    }
    if error.downcast_ref::<local_cas::CacheLimitError>().is_some() {
        return Status::resource_exhausted("blob exceeds local CAS budget");
    }
    Status::data_loss("CAS fetch or verification failed")
}
#[async_trait]
impl BlobSource for Upstream {
    async fn fetch(&self, a: &Artifact, target: &mut File) -> Result<()> {
        self.fetch_transport(a, target).await
    }
    async fn missing(&self, artifacts: &[Artifact]) -> Result<Vec<Artifact>> {
        if artifacts.is_empty() {
            return Ok(vec![]);
        }
        let function = artifacts[0].digest_function;
        ensure!(
            artifacts
                .iter()
                .all(|a| a.backend == self.config.backend_identity
                    && a.instance == self.config.instance_name
                    && a.digest_function == function),
            "mixed CAS scope"
        );
        let response = self
            .cas()
            .find_missing_blobs(
                self.request(FindMissingBlobsRequest {
                    instance_name: self.config.instance_name.clone(),
                    blob_digests: artifacts
                        .iter()
                        .map(|a| Digest {
                            hash: a.hash.clone(),
                            size_bytes: a.size as i64,
                        })
                        .collect(),
                    digest_function: function,
                }),
            )
            .await?
            .into_inner();
        Ok(artifacts
            .iter()
            .filter(|a| {
                response
                    .missing_blob_digests
                    .iter()
                    .any(|d| d.hash == a.hash && d.size_bytes == a.size as i64)
            })
            .cloned()
            .collect())
    }
}
impl CacheProxy {
    pub async fn connect(config: ProxyConfig, cache: Arc<LocalCas>) -> Result<Self> {
        Ok(Self::new(Upstream::connect(config).await?, cache))
    }
    pub fn new(upstream: Arc<Upstream>, cache: Arc<LocalCas>) -> Self {
        Self {
            upstream,
            cache,
            ac: Default::default(),
            metrics: Arc::new(Counters::default()),
            writes: Arc::new(tokio::sync::Semaphore::new(4)),
        }
    }
    pub fn metrics(&self) -> ProxyMetrics {
        ProxyMetrics {
            action_cache_hits: self.metrics.ac_hits.load(Ordering::Relaxed),
            action_cache_upstream: self.metrics.ac_upstream.load(Ordering::Relaxed),
            blob_reads: self.metrics.blob_reads.load(Ordering::Relaxed),
        }
    }
    pub async fn serve(
        self,
        addr: SocketAddr,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        self.serve_listener(TcpListener::bind(addr).await?, shutdown)
            .await
    }
    pub async fn serve_listener(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        ensure!(
            listener.local_addr()?.ip().is_loopback(),
            "cache proxy must listen on loopback"
        );
        self.router(true)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                shutdown,
            )
            .await?;
        Ok(())
    }
    /// The caller must bind this socket in a private per-user directory and
    /// enforce socket permissions before exposing it to clients.
    #[cfg(unix)]
    pub async fn serve_unix_listener(
        self,
        listener: tokio::net::UnixListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        self.router(false)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                shutdown,
            )
            .await?;
        Ok(())
    }
    fn router(self, enforce_bearer: bool) -> tonic::transport::server::Router {
        let limit = self.upstream.config.max_batch_bytes + 1024 * 1024;
        let expected = if enforce_bearer {
            self.upstream
                .config
                .local_bearer_token
                .as_ref()
                .map(|token| format!("Bearer {token}"))
        } else {
            None
        };
        let auth = move |req: Request<()>| -> Result<Request<()>, Status> {
            if let Some(expected) = &expected
                && req
                    .metadata()
                    .get("authorization")
                    .and_then(|s| s.to_str().ok())
                    != Some(expected.as_str())
            {
                return Err(Status::unauthenticated(
                    "local cache authentication required",
                ));
            }
            Ok(req)
        };
        use tonic::service::interceptor::InterceptedService;
        // Queued streaming uploads must not consume all connection receive
        // credit while four admitted writers wait for their next chunks. Bound
        // each stream, cap stream count, and reserve twice their aggregate
        // window to leave room for WINDOW_UPDATE release thresholds.
        let rpc_limit = self.upstream.config.max_concurrent_rpcs_per_connection;
        let stream_window = 64 * 1024u32;
        let connection_window = 2 * rpc_limit as u32 * stream_window;
        Server::builder()
            .max_concurrent_streams(Some(rpc_limit as u32))
            .initial_stream_window_size(Some(stream_window))
            .initial_connection_window_size(Some(connection_window))
            .concurrency_limit_per_connection(
                self.upstream.config.max_concurrent_rpcs_per_connection,
            )
            .add_service(InterceptedService::new(
                action_cache_server::ActionCacheServer::new(self.clone())
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip)
                    .max_decoding_message_size(limit)
                    .max_encoding_message_size(limit),
                auth.clone(),
            ))
            .add_service(InterceptedService::new(
                content_addressable_storage_server::ContentAddressableStorageServer::new(
                    self.clone(),
                )
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(limit)
                .max_encoding_message_size(limit),
                auth.clone(),
            ))
            .add_service(InterceptedService::new(
                capabilities_server::CapabilitiesServer::new(self.clone())
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip),
                auth.clone(),
            ))
            .add_service(InterceptedService::new(
                ByteStreamServer::new(self).accept_compressed(CompressionEncoding::Gzip),
                auth,
            ))
    }
    async fn read_response(
        &self,
        digest: Digest,
        function: i32,
        compressed: bool,
    ) -> batch_read_blobs_response::Response {
        let result: Result<(Vec<u8>, i32), Status> = async {
            let blob = self.content(&digest, function).await?;
            let data = tokio::fs::read(blob.path())
                .await
                .map_err(|_| Status::internal("reading verified cache blob"))?;
            if compressed {
                Ok((
                    reapi::compression::encode_zstd(&data)
                        .map_err(|_| Status::internal("encoding blob"))?,
                    1,
                ))
            } else {
                Ok((data, 0))
            }
        }
        .await;
        let (data, compressor, status) = match result {
            Ok((data, compressor)) => (
                data,
                compressor,
                reapi::proto::google::rpc::Status::default(),
            ),
            Err(s) => (
                vec![],
                0,
                reapi::proto::google::rpc::Status {
                    code: s.code() as i32,
                    message: s.message().to_string(),
                    details: vec![],
                },
            ),
        };
        batch_read_blobs_response::Response {
            digest: Some(digest),
            data,
            compressor,
            status: Some(status),
        }
    }
    async fn content(
        &self,
        digest: &Digest,
        function: i32,
    ) -> Result<Arc<local_cas::CachedBlob>, Status> {
        self.metrics.blob_reads.fetch_add(1, Ordering::Relaxed);
        let a = self.upstream.artifact(digest, function)?;
        self.cache
            .clone()
            .content_from(a, self.upstream.clone())
            .await
            .map_err(cache_status)
    }
}
#[tonic::async_trait]
impl action_cache_server::ActionCache for CacheProxy {
    async fn get_action_result(
        &self,
        req: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let req = req.into_inner();
        self.upstream.scope(&req.instance_name)?;
        let key = req.encode_to_vec();
        if let Some((expiry, value)) = self.ac.lock().unwrap().get(&key)
            && *expiry > Instant::now()
        {
            self.metrics.ac_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(value.clone()));
        }
        self.metrics.ac_upstream.fetch_add(1, Ordering::Relaxed);
        let value = self
            .upstream
            .config
            .policy
            .retry(
                tokio::time::Instant::now() + self.upstream.config.policy.operation_timeout(),
                self.upstream.config.policy.rpc_timeout(),
                |_| async {
                    self.upstream
                        .ac()
                        .get_action_result(
                            self.upstream.request_with_metadata(req.clone(), &metadata),
                        )
                        .await
                        .map(Response::into_inner)
                },
            )
            .await?;
        let config = &self.upstream.config;
        if !config.action_cache_ttl.is_zero() && config.max_action_cache_entries > 0 {
            let mut cache = self.ac.lock().unwrap();
            let now = Instant::now();
            cache.retain(|_, (expires, _)| *expires > now);
            if cache.len() >= config.max_action_cache_entries
                && let Some(key) = cache
                    .iter()
                    .min_by_key(|(_, (expiry, _))| *expiry)
                    .map(|(k, _)| k.clone())
            {
                cache.remove(&key);
            }
            let entry_bytes = key.len() + value.encoded_len();
            const MAX_AC_BYTES: usize = 64 * 1024 * 1024;
            while cache
                .values()
                .map(|(_, value)| value.encoded_len())
                .sum::<usize>()
                + cache.keys().map(Vec::len).sum::<usize>()
                + entry_bytes
                > MAX_AC_BYTES
                && !cache.is_empty()
            {
                let key = cache
                    .iter()
                    .min_by_key(|(_, (expiry, _))| *expiry)
                    .map(|(key, _)| key.clone())
                    .unwrap();
                cache.remove(&key);
            }
            if entry_bytes <= MAX_AC_BYTES {
                cache.insert(key, (now + config.action_cache_ttl, value.clone()));
            }
        }
        Ok(Response::new(value))
    }
    async fn update_action_result(
        &self,
        req: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let req = req.into_inner();
        self.upstream.scope(&req.instance_name)?;
        // Invalidate all inline variants. Never acknowledge a local-only update.
        self.ac.lock().unwrap().clear();
        self.upstream
            .replay(|| async {
                self.upstream
                    .ac()
                    .update_action_result(
                        self.upstream.request_with_metadata(req.clone(), &metadata),
                    )
                    .await
            })
            .await
            .map(clean_response)
    }
}
#[tonic::async_trait]
impl content_addressable_storage_server::ContentAddressableStorage for CacheProxy {
    async fn find_missing_blobs(
        &self,
        req: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let req = req.into_inner();
        self.upstream.scope(&req.instance_name)?;
        for d in &req.blob_digests {
            self.upstream.artifact(d, req.digest_function)?;
        }
        self.upstream
            .replay(|| async {
                self.upstream
                    .cas()
                    .find_missing_blobs(self.upstream.request_with_metadata(req.clone(), &metadata))
                    .await
            })
            .await
            .map(clean_response)
    }
    async fn batch_read_blobs(
        &self,
        req: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        let req = req.into_inner();
        self.upstream.scope(&req.instance_name)?;
        let mut total = 0u64;
        for digest in &req.digests {
            total = total
                .checked_add(self.upstream.artifact(digest, req.digest_function)?.size)
                .ok_or_else(|| Status::invalid_argument("batch size overflow"))?;
        }
        if total > self.upstream.config.max_batch_bytes as u64 {
            return Err(Status::invalid_argument("batch exceeds configured limit"));
        }
        let mut responses = Vec::with_capacity(req.digests.len());
        let compressed = req.acceptable_compressors.contains(&1);
        // Keep cold batches from becoming serial network round trips. A bounded
        // window also prevents one large batch from flooding CAS admission.
        for chunk in req.digests.chunks(8) {
            let mut tasks = tokio::task::JoinSet::new();
            for digest in chunk {
                let proxy = self.clone();
                let digest = digest.clone();
                let function = req.digest_function;
                tasks.spawn(async move { proxy.read_response(digest, function, compressed).await });
            }
            while let Some(response) = tasks.join_next().await {
                responses.push(response.map_err(|_| Status::internal("batch read task failed"))?);
            }
        }
        Ok(Response::new(BatchReadBlobsResponse { responses }))
    }
    async fn batch_update_blobs(
        &self,
        req: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let req = req.into_inner();
        self.upstream.scope(&req.instance_name)?;
        let mut total = 0u64;
        for entry in &req.requests {
            let d = entry
                .digest
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing digest"))?;
            let artifact = self.upstream.artifact(d, req.digest_function)?;
            total = total
                .checked_add(artifact.size)
                .ok_or_else(|| Status::invalid_argument("batch size overflow"))?;
            if total > self.upstream.config.max_batch_bytes as u64 {
                return Err(Status::invalid_argument("batch exceeds configured limit"));
            }
            let bytes = match entry.compressor {
                0 => entry.data.clone(),
                1 => reapi::compression::decode_zstd(&entry.data, artifact.size)
                    .map_err(|_| Status::invalid_argument("invalid compressed blob"))?,
                _ => return Err(Status::invalid_argument("unsupported compressor")),
            };
            verify(&bytes, &artifact)?;
        }
        // Upstream receives precisely the validated request, including compressors.
        let result = self
            .upstream
            .replay(|| async {
                self.upstream
                    .cas()
                    .batch_update_blobs(self.upstream.request_with_metadata(req.clone(), &metadata))
                    .await
            })
            .await?;
        // Cache insertion is opportunistic; remote success does not depend on disk quota.
        for entry in req.requests {
            let d = entry.digest.as_ref().unwrap();
            if result.get_ref().responses.iter().any(|r| {
                r.digest.as_ref() == Some(d) && r.status.as_ref().is_some_and(|s| s.code == 0)
            }) {
                let a = self.upstream.artifact(d, req.digest_function)?;
                let data = if entry.compressor == 1 {
                    reapi::compression::decode_zstd(&entry.data, a.size)
                        .map_err(|_| Status::data_loss("invalid compressed blob"))?
                } else {
                    entry.data
                };
                let _ = self
                    .cache
                    .clone()
                    .content_from(a, Arc::new(MemorySource(data)))
                    .await;
            }
        }
        Ok(clean_response(result))
    }
    type GetTreeStream = RpcStream<GetTreeResponse>;
    async fn get_tree(
        &self,
        req: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let req = req.into_inner();
        self.upstream.scope(&req.instance_name)?;
        let stream = self
            .upstream
            .cas()
            .get_tree(self.upstream.request_with_metadata(req, &metadata))
            .await?
            .into_inner();
        Ok(Response::new(self.upstream.bounded_stream(stream)))
    }
    async fn split_blob(
        &self,
        req: Request<SplitBlobRequest>,
    ) -> Result<Response<SplitBlobResponse>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let req = req.into_inner();
        self.upstream.scope(&req.instance_name)?;
        self.upstream
            .cas()
            .split_blob(self.upstream.request_with_metadata(req, &metadata))
            .await
            .map(clean_response)
    }
    type GetChunkMappingStream = RpcStream<GetChunkMappingResponse>;
    async fn get_chunk_mapping(
        &self,
        req: Request<GetChunkMappingRequest>,
    ) -> Result<Response<Self::GetChunkMappingStream>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let req = req.into_inner();
        self.upstream.scope(&req.instance_name)?;
        let stream = self
            .upstream
            .cas()
            .get_chunk_mapping(self.upstream.request_with_metadata(req, &metadata))
            .await?
            .into_inner();
        Ok(Response::new(self.upstream.bounded_stream(stream)))
    }
    async fn splice_blob(
        &self,
        req: Request<SpliceBlobRequest>,
    ) -> Result<Response<SpliceBlobResponse>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let req = req.into_inner();
        self.upstream.scope(&req.instance_name)?;
        self.upstream
            .cas()
            .splice_blob(self.upstream.request_with_metadata(req, &metadata))
            .await
            .map(clean_response)
    }
    async fn register_chunk_mapping(
        &self,
        req: Request<tonic::Streaming<RegisterChunkMappingRequest>>,
    ) -> Result<Response<RegisterChunkMappingResponse>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let mut input = req.into_inner();
        let first = input
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty chunk mapping stream"))?;
        self.upstream.scope(&first.instance_name)?;
        // REAPI specifies that scope and blob identity come from the first
        // message only; later instance fields must be ignored, not reinterpreted.
        let failure = Arc::new(Mutex::new(None));
        let observed = failure.clone();
        let idle = self.upstream.config.policy.stream_idle_timeout();
        let stream = async_stream::stream! {
            yield first;
            loop {
                match tokio::time::timeout(idle, input.message()).await {
                    Ok(Ok(Some(message))) => yield message,
                    Ok(Ok(None)) => break,
                    Ok(Err(status)) => { *observed.lock().unwrap() = Some(status); break; }
                    Err(_) => { *observed.lock().unwrap() = Some(Status::deadline_exceeded("chunk mapping stream idle timeout")); break; }
                }
            }
        };
        let result = self
            .upstream
            .cas()
            .register_chunk_mapping(self.upstream.request_with_metadata(stream, &metadata))
            .await;
        if let Some(status) = failure.lock().unwrap().take() {
            return Err(status);
        }
        result.map(clean_response)
    }
}
fn verify(bytes: &[u8], artifact: &Artifact) -> Result<(), Status> {
    let hash = if artifact.digest_function == 9 {
        blake3::hash(bytes).to_hex().to_string()
    } else {
        format!("{:x}", Sha256::digest(bytes))
    };
    if bytes.len() as u64 != artifact.size || hash != artifact.hash {
        return Err(Status::invalid_argument("blob digest mismatch"));
    }
    Ok(())
}
struct MemorySource(Vec<u8>);
#[async_trait]
impl BlobSource for MemorySource {
    async fn fetch(&self, _: &Artifact, target: &mut File) -> Result<()> {
        target.write_all(&self.0)?;
        Ok(())
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        anyhow::bail!("not a presence authority")
    }
}
#[tonic::async_trait]
impl capabilities_server::Capabilities for CacheProxy {
    async fn get_capabilities(
        &self,
        req: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let req = req.into_inner();
        self.upstream.scope(&req.instance_name)?;
        let mut response = self
            .upstream
            .replay(|| async {
                capabilities_client::CapabilitiesClient::new(self.upstream.channel.clone())
                    .get_capabilities(self.upstream.request_with_metadata(req.clone(), &metadata))
                    .await
            })
            .await?;
        let capabilities = response.get_mut();
        capabilities.execution_capabilities = None;
        if let Some(cache) = capabilities.cache_capabilities.as_mut() {
            cache.digest_functions.retain(|f| matches!(f, 1 | 9));
            cache.max_batch_total_size_bytes = if cache.max_batch_total_size_bytes > 0 {
                cache
                    .max_batch_total_size_bytes
                    .min(self.upstream.config.max_batch_bytes as i64)
            } else {
                self.upstream.config.max_batch_bytes as i64
            };
            cache.max_cas_blob_size_bytes = if cache.max_cas_blob_size_bytes > 0 {
                cache
                    .max_cas_blob_size_bytes
                    .min(self.upstream.config.max_blob_bytes as i64)
            } else {
                self.upstream.config.max_blob_bytes as i64
            };
            cache.supported_compressors.retain(|c| matches!(c, 0 | 1));
            cache
                .supported_batch_update_compressors
                .retain(|c| matches!(c, 0 | 1));
        }
        Ok(clean_response(response))
    }
}
impl Upstream {
    fn parse_resource(&self, name: &str, upload: bool) -> Result<(Artifact, bool), Status> {
        let prefix = if self.config.instance_name.is_empty() {
            String::new()
        } else {
            format!("{}/", self.config.instance_name)
        };
        let mut suffix = name
            .strip_prefix(&prefix)
            .ok_or_else(|| Status::permission_denied("resource outside daemon instance"))?;
        if upload {
            suffix = suffix
                .strip_prefix("uploads/")
                .and_then(|s| s.split_once('/').map(|(_, tail)| tail))
                .ok_or_else(|| Status::invalid_argument("invalid upload resource"))?;
        }
        let (suffix, compressed) = if let Some(s) = suffix.strip_prefix("compressed-blobs/zstd/") {
            (s, true)
        } else if let Some(s) = suffix.strip_prefix("blobs/") {
            (s, false)
        } else {
            return Err(Status::invalid_argument("unsupported blob resource"));
        };
        let (suffix, function) = if let Some(s) = suffix.strip_prefix("blake3/") {
            (s, 9)
        } else if let Some(s) = suffix.strip_prefix("sha256/") {
            (s, 1)
        } else {
            (suffix, 1)
        };
        let (hash, size) = suffix
            .split_once('/')
            .ok_or_else(|| Status::invalid_argument("invalid blob resource"))?;
        let size = if upload {
            size.split('/').next().unwrap()
        } else {
            size
        };
        let size_bytes = size
            .parse::<i64>()
            .map_err(|_| Status::invalid_argument("invalid blob size"))?;
        Ok((
            self.artifact(
                &Digest {
                    hash: hash.to_string(),
                    size_bytes,
                },
                function,
            )?,
            compressed,
        ))
    }
}
struct LimitedFile {
    file: File,
    bytes: u64,
    limit: u64,
}
impl Write for LimitedFile {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() as u64 > self.limit.saturating_sub(self.bytes) {
            return Err(std::io::Error::other(
                "decompressed blob exceeds digest size",
            ));
        }
        self.file.write_all(data)?;
        self.bytes += data.len() as u64;
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
struct FileSource(std::path::PathBuf);
#[async_trait]
impl BlobSource for FileSource {
    async fn fetch(&self, artifact: &Artifact, target: &mut File) -> Result<()> {
        let input = File::open(&self.0)?;
        ensure!(
            std::io::copy(&mut std::io::Read::take(input, artifact.size + 1), target)?
                == artifact.size,
            "invalid staged blob size"
        );
        Ok(())
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        anyhow::bail!("not a presence authority")
    }
}
#[tonic::async_trait]
impl ByteStream for CacheProxy {
    type ReadStream = RpcStream<bs::ReadResponse>;
    async fn read(
        &self,
        req: Request<bs::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let req = req.into_inner();
        let (artifact, compressed) = self.upstream.parse_resource(&req.resource_name, false)?;
        if compressed && req.read_limit != 0 {
            return Err(Status::invalid_argument(
                "compressed reads require read_limit zero",
            ));
        }
        if req.read_offset < 0 || req.read_limit < 0 || req.read_offset as u64 > artifact.size {
            return Err(Status::out_of_range("invalid blob read range"));
        }
        let blob = self
            .content(
                &Digest {
                    hash: artifact.hash,
                    size_bytes: artifact.size as i64,
                },
                artifact.digest_function,
            )
            .await?;
        let mut file = tokio::fs::File::open(blob.path())
            .await
            .map_err(|_| Status::internal("opening cache blob"))?;
        file.seek(std::io::SeekFrom::Start(req.read_offset as u64))
            .await
            .map_err(|_| Status::internal("seeking cache blob"))?;
        let limit = if req.read_limit == 0 {
            artifact.size - req.read_offset as u64
        } else {
            (req.read_limit as u64).min(artifact.size - req.read_offset as u64)
        };
        let reader = file.take(limit);
        let mut reader: Pin<Box<dyn tokio::io::AsyncRead + Send>> = if compressed {
            Box::pin(async_compression::tokio::bufread::ZstdEncoder::new(
                tokio::io::BufReader::new(reader),
            ))
        } else {
            Box::pin(reader)
        };
        Ok(Response::new(Box::pin(
            async_stream::try_stream! { let _lease = blob; let mut buffer = vec![0; 64 * 1024]; loop { let n = reader.read(&mut buffer).await.map_err(|_| Status::internal("reading cache blob"))?; if n == 0 { break; } yield bs::ReadResponse { data: buffer[..n].to_vec() }; } },
        )))
    }
    async fn write(
        &self,
        req: Request<tonic::Streaming<bs::WriteRequest>>,
    ) -> Result<Response<bs::WriteResponse>, Status> {
        let deadline =
            tokio::time::Instant::now() + self.upstream.config.policy.operation_timeout();
        reapi::policy::timeout(deadline, async {
            let _permit = self.writes.acquire().await.map_err(|_| Status::unavailable("proxy shutting down"))?;
            let metadata = CallerMetadata::capture(&req)?;
        let mut input = req.into_inner();
            let mut first = input.message().await?.ok_or_else(|| Status::invalid_argument("empty write stream"))?;
            let name = first.resource_name.clone(); let (artifact, compressed) = self.upstream.parse_resource(&name, true)?;
            if first.write_offset != 0 { return Err(Status::out_of_range("restart an incomplete upload at offset zero")); }
            let encoded = tempfile::NamedTempFile::new().map_err(|_| Status::resource_exhausted("creating upload staging file"))?;
            let decoded = tempfile::NamedTempFile::new().map_err(|_| Status::resource_exhausted("creating upload staging file"))?;
            let mut wire_file = encoded.reopen().map_err(|_| Status::internal("opening upload staging file"))?;
            let mut raw = LimitedFile { file: decoded.reopen().map_err(|_| Status::internal("opening upload staging file"))?, bytes: 0, limit: artifact.size };
            let mut decoder = if compressed { Some(reapi::compression::ZstdDecoder::new().map_err(|_| Status::internal("creating decoder"))?) } else { None };
            let mut offset = 0u64;
            loop {
                if (!first.resource_name.is_empty() && first.resource_name != name) || first.write_offset < 0 || first.write_offset as u64 != offset { return Err(Status::invalid_argument("upload resource or offset changed")); }
                offset = offset.checked_add(first.data.len() as u64).ok_or_else(|| Status::invalid_argument("upload size overflow"))?;
                // Zstd incompressible-frame overhead is bounded without trusting wire sizes.
                if offset > self.upstream.config.max_blob_bytes.saturating_add(1024 * 1024) { return Err(Status::resource_exhausted("encoded upload exceeds configured limit")); }
                wire_file.write_all(&first.data).map_err(|_| Status::resource_exhausted("staging upload"))?;
                if let Some(decoder) = decoder.as_mut() {
                    let mut chunk = first.data.as_slice(); let mut drain = false;
                    while !chunk.is_empty() || drain {
                        let step = decoder.decode_step(chunk, &mut raw).map_err(|_| Status::invalid_argument("invalid compressed blob"))?;
                        chunk = &chunk[step.consumed..]; drain = step.needs_drain;
                        tokio::task::yield_now().await;
                    }
                } else { raw.write_all(&first.data).map_err(|_| Status::invalid_argument("blob exceeds digest size"))?; }
                if first.finish_write { break; }
                first = tokio::time::timeout(self.upstream.config.policy.stream_idle_timeout(), input.message()).await.map_err(|_| Status::deadline_exceeded("upload stream idle timeout"))??.ok_or_else(|| Status::invalid_argument("upload missing finish_write"))?;
            }
            if let Some(decoder) = decoder { decoder.finish().map_err(|_| Status::invalid_argument("incomplete compressed blob"))?; }
            raw.flush().map_err(|_| Status::internal("flushing upload"))?; wire_file.flush().map_err(|_| Status::internal("flushing upload"))?;
            if raw.bytes != artifact.size { return Err(Status::invalid_argument("upload size mismatch")); }
            // Verify streamed bytes before any upstream commit is attempted.
            let mut raw_file = tokio::fs::File::open(decoded.path()).await.map_err(|_| Status::internal("opening staged upload"))?;
            let mut sha = Sha256::new(); let mut blake = blake3::Hasher::new(); let mut buf = vec![0; 128 * 1024];
            loop { let n = raw_file.read(&mut buf).await.map_err(|_| Status::internal("reading staged upload"))?; if n == 0 { break; }
                if artifact.digest_function == 9 { blake.update(&buf[..n]); } else { sha.update(&buf[..n]); } }
            let hash = if artifact.digest_function == 9 { blake.finalize().to_hex().to_string() } else { format!("{:x}", sha.finalize()) };
            if hash != artifact.hash { return Err(Status::invalid_argument("blob digest mismatch")); }
            let mut file = tokio::fs::File::open(encoded.path()).await.map_err(|_| Status::internal("opening staged upload"))?;
            let upload = async_stream::stream! { let mut offset = 0u64; let mut data = vec![0; 64 * 1024]; while let Ok(n) = file.read(&mut data).await { let finish = n == 0; yield bs::WriteRequest { resource_name: name.clone(), write_offset: offset as i64, finish_write: finish, data: data[..n].to_vec() }; offset += n as u64; if finish { break; } } };
            let response = self.upstream.bytes().write(self.upstream.request_with_metadata(upload, &metadata)).await?;
            if !compressed && response.get_ref().committed_size != artifact.size as i64 { return Err(Status::data_loss("upstream did not commit complete blob")); }
            let _ = self.cache.clone().content_from(artifact, Arc::new(FileSource(decoded.path().to_path_buf()))).await;
            Ok(clean_response(response))
        }).await
    }
    async fn query_write_status(
        &self,
        req: Request<bs::QueryWriteStatusRequest>,
    ) -> Result<Response<bs::QueryWriteStatusResponse>, Status> {
        let metadata = CallerMetadata::capture(&req)?;
        let req = req.into_inner();
        self.upstream.parse_resource(&req.resource_name, true)?;
        let mut response = self
            .upstream
            .bytes()
            .query_write_status(self.upstream.request_with_metadata(req, &metadata))
            .await?;
        // Unfinished staging is not retained by this proxy. Tell callers to
        // restart, rather than advertise an offset we cannot locally verify.
        if !response.get_ref().complete {
            response.get_mut().committed_size = 0;
        }
        Ok(clean_response(response))
    }
}
