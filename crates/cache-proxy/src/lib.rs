//! One-user REAPI cache front end. Local CAS residence never implies RBE-side
//! presence: writes and presence queries always reach the configured backend.
mod asset;
mod download;
mod execution;
mod read;
mod repair;
mod retention;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use futures_core::Stream;
use local_cas::{
    Artifact, BlobSource, FailureClass, FailureDiagnostic, FailureDiagnostics, FailureEncoding,
    FailureMetrics, FailureStage, LocalCas,
};
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
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{io::AsyncReadExt, net::TcpListener};
use tonic::{
    Request, Response, Status,
    codec::CompressionEncoding,
    metadata::{MetadataKey, MetadataValue},
    transport::{Channel, Endpoint, Server},
};

use retention::Actions;

type RpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

const REQUEST_METADATA: &str = "build.bazel.remote.execution.v2.requestmetadata-bin";
/// Explicit attribution only. Authorization and transport metadata are never
/// copied from the caller; the daemon supplies upstream credentials.
#[derive(Clone, Default)]
struct CallerMetadata(
    Option<MetadataValue<tonic::metadata::Binary>>,
    Vec<(
        MetadataKey<tonic::metadata::Ascii>,
        MetadataValue<tonic::metadata::Ascii>,
    )>,
);
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
        Ok(Self(Some(value.clone()), Vec::new()))
    }
    fn execution<T>(request: &Request<T>) -> Result<Self, Status> {
        let mut result = Self::capture(request)?;
        let mut bytes = 0;
        for value in request.metadata().iter() {
            if let tonic::metadata::KeyAndValueRef::Ascii(key, value) = value
                && key.as_str().starts_with("x-buildbuddy-platform.")
            {
                bytes += key.as_str().len() + value.as_bytes().len();
                if bytes > 8192 || result.1.len() >= 64 {
                    return Err(Status::resource_exhausted(
                        "execution routing metadata exceeds limit",
                    ));
                }
                result.1.push((key.clone(), value.clone()));
            }
        }
        Ok(result)
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
    /// Shared cached-response descriptor admission across connections and RPCs.
    /// Hydration finishes before consuming a slot; this is not an all-process
    /// descriptor quota.
    pub max_concurrent_reads: usize,
    /// Nonzero requires an operator-provided upstream retention guarantee.
    /// Zero is safe with backends whose eviction policy is unknown.
    pub action_cache_ttl: Duration,
    pub max_action_cache_entries: usize,
    /// Assumed backend retention after a confirmed upstream access; zero disables
    /// best-effort background retention maintenance.
    pub upstream_retention: Duration,
    pub retention_refresh_interval: Duration,
    pub retention_max_entries: usize,
    pub asset_cache_ttl: Duration,
    pub max_asset_cache_entries: usize,
    pub max_asset_cache_bytes: usize,
    pub max_execution_streams: usize,
    pub execution_timeout: Option<Duration>,
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
            max_concurrent_reads: 128,
            action_cache_ttl: Duration::ZERO,
            max_action_cache_entries: 4096,
            upstream_retention: Duration::ZERO,
            retention_refresh_interval: Duration::from_secs(10800),
            retention_max_entries: 16384,
            asset_cache_ttl: Duration::from_secs(60),
            max_asset_cache_entries: 4096,
            max_asset_cache_bytes: 8 * 1024 * 1024,
            max_execution_streams: 256,
            execution_timeout: None,
        }
    }
}
#[derive(Default)]
struct Counters {
    ac_hits: AtomicU64,
    ac_upstream: AtomicU64,
    blob_reads: AtomicU64,
    asset_hits: AtomicU64,
    asset_upstream: AtomicU64,
    execution_requests: AtomicU64,
    execution_waits: AtomicU64,
}
#[derive(Clone, Copy, Debug)]
pub struct ProxyMetrics {
    pub action_cache_hits: u64,
    pub action_cache_upstream: u64,
    pub blob_reads: u64,
    pub asset_cache_hits: u64,
    pub asset_upstream_requests: u64,
    pub execution_requests: u64,
    pub execution_waits: u64,
    pub retention: retention::Metrics,
    pub download_failures: FailureMetrics,
    pub read_slots_in_use: u64,
    pub read_slots_queued: u64,
    pub accept_resource_exhaustions: u64,
    pub serving_read_failures: FailureMetrics,
}
#[derive(Clone)]
pub struct CacheProxy {
    upstream: Arc<Upstream>,
    cache: Arc<LocalCas>,
    ac: Arc<Mutex<Actions>>,
    metrics: Arc<Counters>,
    writes: Arc<tokio::sync::Semaphore>,
    reads: Arc<read::Admission>,
    accept: reapi::server_io::AcceptState,
    executions: Arc<tokio::sync::Semaphore>,
    assets: Arc<Mutex<asset::Mappings>>,
    stop: tokio::sync::watch::Sender<bool>,
}
/// Reusable default source for `LocalCas::open`, sharing the proxy's security scope.
pub struct Upstream {
    config: ProxyConfig,
    channel: Channel,
    stream_channel: Channel,
    asset_channel: Channel,
    operations_channel: Channel,
    maintenance_channel: Channel,
    retention: Arc<retention::Retention>,
    gzip_requests: std::sync::atomic::AtomicU16,
    zstd_capability: tokio::sync::OnceCell<bool>,
    zstd_disabled: std::sync::atomic::AtomicBool,
    download_failures: FailureDiagnostics,
}
#[derive(Clone, Copy)]
enum RpcMethod {
    Execute,
    WaitExecution,
    ListOperations,
    GetOperation,
    DeleteOperation,
    CancelOperation,
    WaitOperation,
    FetchBlob,
    FetchDirectory,
    GetActionResult,
    FindMissingBlobs,
    WriteByteStream,
}
impl Upstream {
    fn send_gzip(&self, method: RpcMethod) -> bool {
        self.gzip_requests.load(Ordering::Relaxed) & (1 << (method as u32)) != 0
    }
    fn observe_gzip<T>(&self, method: RpcMethod, response: &Response<T>) {
        let supported = response
            .metadata()
            .get("grpc-accept-encoding")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.split(',').any(|encoding| encoding.trim() == "gzip"));
        let mask = 1 << (method as u32);
        if supported {
            self.gzip_requests.fetch_or(mask, Ordering::Relaxed);
        } else {
            self.gzip_requests.fetch_and(!mask, Ordering::Relaxed);
        }
    }
    pub async fn connect(config: ProxyConfig) -> Result<Arc<Self>> {
        config.policy.validate()?;
        ensure!(
            (1..=1024).contains(&config.max_concurrent_rpcs_per_connection),
            "RPC concurrency limit must be 1..1024"
        );
        ensure!(
            (1..=1024).contains(&config.max_concurrent_reads),
            "invalid shared cached-response read limit"
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
        ensure!(
            config.upstream_retention <= Duration::from_secs(30 * 86400)
                && config.retention_refresh_interval <= Duration::from_secs(30 * 86400)
                && config.retention_max_entries <= 1_000_000
                && (config.upstream_retention.is_zero()
                    || (!config.retention_refresh_interval.is_zero()
                        && config.retention_refresh_interval < config.upstream_retention
                        && config.action_cache_ttl <= config.upstream_retention)),
            "invalid upstream retention or refresh policy"
        );
        ensure!(
            config.asset_cache_ttl <= Duration::from_secs(3600)
                && config.max_asset_cache_entries <= 1_000_000
                && config.max_asset_cache_bytes <= 64 * 1024 * 1024
                && (1..=1024).contains(&config.max_execution_streams)
                && config
                    .execution_timeout
                    .is_none_or(|d| !d.is_zero() && d <= Duration::from_secs(86400)),
            "invalid asset-cache or execution limits"
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
            .connect_timeout(config.policy.connect_timeout());
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
        let stream_channel = endpoint.clone().connect_lazy();
        let asset_channel = endpoint.clone().connect_lazy();
        let operations_channel = endpoint.clone().connect_lazy();
        let maintenance_channel = endpoint.clone().connect_lazy();
        let channel = endpoint.timeout(config.policy.rpc_timeout()).connect_lazy();
        let upstream = Arc::new_cyclic(|owner| Self {
            retention: retention::Retention::new(owner.clone(), &config),
            config,
            channel,
            stream_channel,
            asset_channel,
            operations_channel,
            maintenance_channel,
            gzip_requests: std::sync::atomic::AtomicU16::new(0),
            zstd_capability: tokio::sync::OnceCell::new(),
            zstd_disabled: std::sync::atomic::AtomicBool::new(false),
            download_failures: FailureDiagnostics::default(),
        });
        upstream.retention.start();
        Ok(upstream)
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
        for (key, value) in &metadata.1 {
            if !self
                .config
                .upstream_headers
                .iter()
                .any(|(configured, _)| configured.eq_ignore_ascii_case(key.as_str()))
            {
                request.metadata_mut().append(key.clone(), value.clone());
            }
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
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(read::descriptor_exhausted)
    }) {
        return Status::resource_exhausted("local file descriptor limit reached");
    }
    Status::data_loss("CAS fetch or verification failed")
}
#[async_trait]
impl BlobSource for Upstream {
    async fn fetch(&self, a: &Artifact, target: &mut File) -> Result<()> {
        self.fetch_transport(a, target).await
    }
    fn verified(&self, artifact: &Artifact, started: Instant) {
        self.retention.confirm(artifact, started, true);
    }
    fn accessed(&self, artifact: &Artifact, cache: std::sync::Weak<LocalCas>) {
        self.retention.accessed(artifact, cache);
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
        let started = Instant::now();
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
        self.retention
            .observe_presence(artifacts, &response.missing_blob_digests, started);
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
        let execution_limit = upstream.config.max_execution_streams;
        let read_limit = upstream.config.max_concurrent_reads;
        let ac = upstream.retention.actions.clone();
        Self {
            upstream,
            cache,
            ac,
            metrics: Arc::new(Counters::default()),
            writes: Arc::new(tokio::sync::Semaphore::new(4)),
            reads: Arc::new(read::Admission::new(read_limit)),
            accept: Default::default(),
            executions: Arc::new(tokio::sync::Semaphore::new(execution_limit)),
            assets: Default::default(),
            stop: tokio::sync::watch::channel(false).0,
        }
    }
    /// Close scheduler forwarding streams before draining the listener. This
    /// disconnects clients; it does not cancel execution at the remote scheduler.
    pub fn stop_forwarding(&self) {
        self.stop.send_replace(true);
        self.upstream.retention.stop();
    }
    /// After `stop_forwarding`, wait for maintenance futures and their upload
    /// leases to drop. Then `LocalCas::wait_for_idle` drains blocking verification.
    pub async fn wait_for_maintenance_shutdown(&self) {
        self.upstream.retention.wait_stopped().await;
    }
    /// Call after stop_forwarding and handler drain/cancellation. Admitted
    /// blocking jobs retain their read slot and cache pin even after their RPC
    /// future is dropped. The semaphore stays open so this barrier can acquire
    /// every permit; the stop watch rejects any late handler admission.
    pub async fn wait_for_reads_idle(&self) -> Result<()> {
        ensure!(
            *self.stop.borrow(),
            "stop forwarding before waiting for cached reads"
        );
        self.reads.wait_for_idle().await
    }
    /// Shared by standalone and composed listeners for accept recovery metrics.
    pub fn accept_state(&self) -> reapi::server_io::AcceptState {
        self.accept.clone()
    }
    pub fn metrics(&self) -> ProxyMetrics {
        ProxyMetrics {
            action_cache_hits: self.metrics.ac_hits.load(Ordering::Relaxed),
            action_cache_upstream: self.metrics.ac_upstream.load(Ordering::Relaxed),
            blob_reads: self.metrics.blob_reads.load(Ordering::Relaxed),
            asset_cache_hits: self.metrics.asset_hits.load(Ordering::Relaxed),
            asset_upstream_requests: self.metrics.asset_upstream.load(Ordering::Relaxed),
            execution_requests: self.metrics.execution_requests.load(Ordering::Relaxed),
            execution_waits: self.metrics.execution_waits.load(Ordering::Relaxed),
            retention: self.upstream.retention.metrics(),
            download_failures: self.upstream.download_failures.metrics(),
            read_slots_in_use: self.reads.in_use(),
            read_slots_queued: self.reads.queued(),
            accept_resource_exhaustions: self.accept.resource_exhaustions(),
            serving_read_failures: self.reads.failure_diagnostics().metrics(),
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
        let stopping = self.clone();
        let accept = self.accept_state();
        self.router(true)
            .serve_with_incoming_shutdown(
                reapi::server_io::tcp_incoming(listener, accept),
                async move {
                    shutdown.await;
                    stopping.stop_forwarding();
                    stopping.wait_for_maintenance_shutdown().await;
                },
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
        let stopping = self.clone();
        let accept = self.accept_state();
        self.router(false)
            .serve_with_incoming_shutdown(
                reapi::server_io::unix_incoming(listener, accept),
                async move {
                    shutdown.await;
                    stopping.stop_forwarding();
                    stopping.wait_for_maintenance_shutdown().await;
                },
            )
            .await?;
        Ok(())
    }
    /// Compose additional daemon services on this private, per-user transport.
    pub fn unix_router(self) -> tonic::transport::server::Router {
        self.router(false)
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
                execution_server::ExecutionServer::new(self.clone())
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip)
                    .max_decoding_message_size(limit)
                    .max_encoding_message_size(limit),
                auth.clone(),
            ))
            .add_service(InterceptedService::new(
                reapi::proto::google::longrunning::operations_server::OperationsServer::new(
                    self.clone(),
                )
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(limit)
                .max_encoding_message_size(limit),
                auth.clone(),
            ))
            .add_service(InterceptedService::new(
                reapi::asset::fetch_server::FetchServer::new(self.clone())
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip)
                    .max_decoding_message_size(limit)
                    .max_encoding_message_size(limit),
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
            let blob = self.read_content(&digest, function).await?;
            let data = self.reads.read_all(blob, self.stop.subscribe()).await?;
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
    async fn read_content(
        &self,
        digest: &Digest,
        function: i32,
    ) -> Result<Arc<local_cas::CachedBlob>, Status> {
        let mut stop = self.stop.subscribe();
        if *stop.borrow() {
            return Err(Status::unavailable("proxy shutting down"));
        }
        tokio::select! {
            biased;
            _ = stop.changed() => Err(Status::unavailable("proxy shutting down")),
            result = self.content(digest, function) => result,
        }
    }
    async fn content(
        &self,
        digest: &Digest,
        function: i32,
    ) -> Result<Arc<local_cas::CachedBlob>, Status> {
        self.metrics.blob_reads.fetch_add(1, Ordering::Relaxed);
        let a = self.upstream.artifact(digest, function)?;
        let expected_size = a.size;
        let digest_function = a.digest_function;
        self.cache
            .clone()
            .content_from(a, self.upstream.clone())
            .await
            .map_err(|error| {
                // Includes failures before staged hydration (for example,
                // reservation/admission). Keep unknown origins unattributed.
                let mut diagnostic =
                    FailureDiagnostic::from_error(FailureStage::Unattributed, &error);
                if let Some(status) = error
                    .chain()
                    .find_map(|cause| cause.downcast_ref::<Status>())
                {
                    diagnostic.class = FailureClass::GrpcStatus;
                    diagnostic.grpc_code = Some(status.code() as i32);
                }
                diagnostic.expected_size = Some(expected_size);
                diagnostic.digest_function = Some(digest_function);
                self.upstream.download_failures.record(diagnostic);
                cache_status(error)
            })
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
        if let Some(entry) = self.ac.lock().unwrap().entries.get_mut(&key)
            && entry.expires > Instant::now()
        {
            self.upstream.retention.action_hit(entry);
            self.metrics.ac_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Response::new(entry.value.clone()));
        }
        let generation = self.ac.lock().unwrap().generation;
        let started = Instant::now();
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
        self.upstream
            .retention
            .store_action(key, value.clone(), started, generation, None);
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
        self.upstream.retention.clear_actions();
        // Reads overlapping the remote attempt can observe its old value. A
        // second generation change excludes them even on ambiguous failures
        // (or cancellation), where the backend may already have committed.
        struct InvalidateOnCompletion<'a>(&'a retention::Retention);
        impl Drop for InvalidateOnCompletion<'_> {
            fn drop(&mut self) {
                self.0.clear_actions();
            }
        }
        let _invalidate = InvalidateOnCompletion(&self.upstream.retention);
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
        let artifacts = req
            .blob_digests
            .iter()
            .map(|d| self.upstream.artifact(d, req.digest_function))
            .collect::<Result<Vec<_>, _>>()?;
        let started = Instant::now();
        let response = self
            .upstream
            .replay(|| async {
                self.upstream
                    .cas()
                    .find_missing_blobs(self.upstream.request_with_metadata(req.clone(), &metadata))
                    .await
            })
            .await?;
        self.upstream.retention.observe_presence(
            &artifacts,
            &response.get_ref().missing_blob_digests,
            started,
        );
        Ok(clean_response(response))
    }
    async fn batch_read_blobs(
        &self,
        req: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        let deadline =
            tokio::time::Instant::now() + self.upstream.config.policy.operation_timeout();
        reapi::policy::timeout(deadline, async {
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
                    tasks.spawn(
                        async move { proxy.read_response(digest, function, compressed).await },
                    );
                }
                while let Some(response) = tasks.join_next().await {
                    responses
                        .push(response.map_err(|_| Status::internal("batch read task failed"))?);
                }
            }
            Ok(Response::new(BatchReadBlobsResponse { responses }))
        })
        .await
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
        let deadline =
            tokio::time::Instant::now() + self.upstream.config.policy.operation_timeout();
        let operation = reapi::policy::timeout(deadline, async {
            let metadata = CallerMetadata::capture(&req)?;
            let mut input = req.into_inner();
            let idle = self.upstream.config.policy.stream_idle_timeout();
            let first = tokio::time::timeout_at(
                deadline.min(tokio::time::Instant::now() + idle),
                input.message(),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("chunk mapping stream idle timeout"))??
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
        });
        let mut stop = self.stop.subscribe();
        tokio::select! {
            biased;
            _ = async { if !*stop.borrow_and_update() { let _ = stop.changed().await; } } => Err(Status::unavailable("proxy shutting down")),
            result = operation => result,
        }
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
                    .accept_compressed(CompressionEncoding::Gzip)
                    .get_capabilities(self.upstream.request_with_metadata(req.clone(), &metadata))
                    .await
            })
            .await?;
        let capabilities = response.get_mut();
        if let Some(execution) = capabilities.execution_capabilities.as_mut() {
            if !execution.digest_functions.is_empty() {
                execution.digest_functions.retain(|f| matches!(f, 1 | 9));
                if let Some(first) = execution.digest_functions.first().copied() {
                    if !execution
                        .digest_functions
                        .contains(&execution.digest_function)
                    {
                        execution.digest_function = first;
                    }
                } else {
                    execution.exec_enabled = false;
                }
            } else if !matches!(execution.digest_function, 1 | 9) {
                execution.exec_enabled = false;
            }
        }
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
        // One setup budget includes hydration, shared read admission and the
        // blocking open/seek. Do not restart it after content becomes resident,
        // or impose it on a long-lived response body.
        let deadline =
            tokio::time::Instant::now() + self.upstream.config.policy.operation_timeout();
        let (reader, compressed) = reapi::policy::timeout(deadline, async {
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
                .read_content(
                    &Digest {
                        hash: artifact.hash,
                        size_bytes: artifact.size as i64,
                    },
                    artifact.digest_function,
                )
                .await?;
            let reader = self
                .reads
                .open(blob, self.stop.subscribe(), req.read_offset as u64)
                .await?;
            let limit = if req.read_limit == 0 {
                artifact.size - req.read_offset as u64
            } else {
                (req.read_limit as u64).min(artifact.size - req.read_offset as u64)
            };
            Ok((reader.take(limit), compressed))
        })
        .await?;
        let reader: Pin<Box<dyn tokio::io::AsyncRead + Send>> = if compressed {
            Box::pin(async_compression::tokio::bufread::ZstdEncoder::new(
                tokio::io::BufReader::new(reader),
            ))
        } else {
            Box::pin(reader)
        };
        Ok(Response::new(read::body(
            reader,
            self.stop.subscribe(),
            self.reads.failure_diagnostics(),
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
            let upload_complete = Arc::new(AtomicBool::new(false));
            let completed = upload_complete.clone();
            let upload = async_stream::stream! { let mut offset = 0u64; let mut data = vec![0; 64 * 1024]; while let Ok(n) = file.read(&mut data).await { let finish = n == 0; if finish { completed.store(true, Ordering::Release); } yield bs::WriteRequest { resource_name: name.clone(), write_offset: offset as i64, finish_write: finish, data: data[..n].to_vec() }; offset += n as u64; if finish { break; } } };
            let response = self.upstream.bytes().write(self.upstream.request_with_metadata(upload, &metadata)).await?;
            let committed = response.get_ref().committed_size;
            let valid = if upload_complete.load(Ordering::Acquire) {
                reapi::upload_ack::valid_completed_write_ack(compressed, committed, artifact.size as i64, offset)
            } else {
                // An upstream may finish early only when the verified blob
                // already exists; it has not consumed the encoded byte count.
                if compressed { committed == -1 } else { committed == artifact.size as i64 }
            };
            if !valid { return Err(Status::data_loss("upstream did not commit complete blob")); }
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
