//! BLAKE3 CAS uploads using the Remote Execution API and ByteStream.
use anyhow::{Context, Result, bail, ensure};
use prost::Message;
use serde::Serialize;
use std::{
    collections::HashSet,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader, ReadBuf};
use tokio_stream::StreamExt;
use tonic::codec::CompressionEncoding;
use tonic::{
    Code, Request,
    metadata::{Ascii, MetadataKey, MetadataValue},
    transport::{Channel, ClientTlsConfig, Endpoint},
};

use ::reapi::policy::Policy;
pub use ::reapi::{Digest, proto, reapi};
use proto::google::bytestream as bs;
use tokio::time::Instant;

const ZSTD: i32 = reapi::compressor::Value::Zstd as i32;
const BLAKE3: i32 = reapi::digest_function::Value::Blake3 as i32;
const FIND_MISSING_LIMIT: usize = 1000;
const BATCH_BYTES: usize = 2 * 1024 * 1024;
const GRPC_BYTES: usize = 4_000_000;
const STREAM_BYTES: usize = 256_000;
const CHUNK_HEADER: &str = "build.bazel.remote.execution.v2.chunked";

/// Headers are explicit configuration; no credential files or environment are read here.
#[derive(Clone)]
pub struct CasConfig {
    pub endpoint: String,
    pub instance_name: String,
    pub headers: Vec<(String, String)>,
    pub policy: Policy,
}
impl std::fmt::Debug for CasConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CasConfig")
            .field("endpoint", &"[configured]")
            .field("instance_name", &self.instance_name)
            .field("headers", &"[redacted]")
            .finish()
    }
}
#[derive(Clone, Debug)]
pub struct UploadBlob {
    pub digest: Digest,
    pub path: PathBuf,
}

#[derive(Default, Debug, Clone, Serialize)]
pub struct UploadStats {
    pub find_missing_calls: u64,
    pub batch_update_calls: u64,
    pub bytestream_write_calls: u64,
    pub splice_calls: u64,
    pub uploaded_bytes: u64,
    /// Application payload bytes emitted into RPC messages, excluding protobuf/HTTP framing.
    pub uploaded_wire_bytes: u64,
    pub zstd_batch_blobs: u64,
    pub zstd_stream_blobs: u64,
    pub uploaded_blobs: u64,
    pub cdc_blobs: u64,
    pub cdc_chunks: u64,
    pub cdc_fallbacks: u64,
}
impl UploadStats {
    fn add(&mut self, other: &Self) {
        self.find_missing_calls += other.find_missing_calls;
        self.batch_update_calls += other.batch_update_calls;
        self.bytestream_write_calls += other.bytestream_write_calls;
        self.splice_calls += other.splice_calls;
        self.uploaded_bytes += other.uploaded_bytes;
        self.uploaded_wire_bytes += other.uploaded_wire_bytes;
        self.zstd_batch_blobs += other.zstd_batch_blobs;
        self.zstd_stream_blobs += other.zstd_stream_blobs;
        self.uploaded_blobs += other.uploaded_blobs;
        self.cdc_blobs += other.cdc_blobs;
        self.cdc_chunks += other.cdc_chunks;
        self.cdc_fallbacks += other.cdc_fallbacks;
    }
}
#[derive(Clone)]
pub struct CasClient {
    channel: Channel,
    instance: String,
    headers: Arc<Vec<(MetadataKey<Ascii>, MetadataValue<Ascii>)>>,
    caps: reapi::CacheCapabilities,
    policy: Policy,
    deadline: Option<Instant>,
    grpc_compression: Arc<::reapi::compression::GrpcCompression>,
    stats: Arc<Mutex<UploadStats>>,
    chunk_workers: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}
#[derive(Clone)]
struct Part {
    digest: Digest,
    path: PathBuf,
    offset: u64,
}
// Cancel the bounded spool reader when its RPC future is dropped (including shutdown).
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct CancelChunkOnDrop(Arc<AtomicBool>);
impl Drop for CancelChunkOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
fn key(d: &Digest) -> (String, i64) {
    (d.hash.clone(), d.size_bytes)
}
fn validate_digest(d: &Digest) -> Result<()> {
    ensure!(
        d.size_bytes >= 0
            && d.hash.len() == 64
            && d.hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid BLAKE3 digest"
    );
    Ok(())
}

// Verify the exact uncompressed part as it is read, before an encoder can emit
// its final frame. A corrupt or truncated spool can never finish the upload.
struct VerifiedPartReader {
    file: tokio::io::Take<tokio::fs::File>,
    remaining: u64,
    hasher: blake3::Hasher,
    hash: String,
    verified: bool,
}
impl AsyncRead for VerifiedPartReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.file).poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                let data = &buf.filled()[before..];
                self.hasher.update(data);
                self.remaining -= data.len() as u64;
                if self.remaining == 0 && !self.verified {
                    if self.hasher.finalize().to_hex().as_str() != self.hash {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "CAS spool content does not match digest",
                        )));
                    }
                    self.verified = true;
                } else if data.is_empty() && self.remaining != 0 && buf.remaining() != 0 {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "truncated CAS spool",
                    )));
                }
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl CasClient {
    pub async fn connect(config: CasConfig) -> Result<Self> {
        config.policy.validate()?;
        let deadline = Instant::now() + config.policy.operation_timeout();
        let uri: axum::http::Uri = config
            .endpoint
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid CAS endpoint"))?;
        ensure!(
            matches!(uri.scheme_str(), Some("http" | "https"))
                && uri.authority().is_some_and(|a| !a.as_str().contains('@'))
                && uri.path() == "/"
                && uri.query().is_none(),
            "CAS endpoint must be an http(s) origin without credentials or query"
        );
        ensure!(
            !config.instance_name.starts_with('/')
                && !config.instance_name.ends_with('/')
                && !config
                    .instance_name
                    .split('/')
                    .any(|s| s == "." || s == ".."),
            "invalid CAS instance name"
        );
        let mut headers = Vec::new();
        for (k, v) in config.headers {
            let k: MetadataKey<Ascii> = k
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid CAS header name"))?;
            ensure!(
                k.as_str() != CHUNK_HEADER,
                "chunking header is managed by the CAS client"
            );
            let v: MetadataValue<Ascii> = v
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid CAS header value"))?;
            headers.push((k, v));
        }
        let https = uri.scheme_str() == Some("https");
        let mut endpoint = Endpoint::from_shared(config.endpoint)
            .map_err(|_| anyhow::anyhow!("invalid CAS endpoint"))?
            .connect_timeout(config.policy.connect_timeout());
        if https {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .context("configure CAS TLS")?;
        }
        let channel = tokio::time::timeout_at(deadline, endpoint.connect())
            .await
            .context("CAS connection timed out")?
            .context("connect CAS")?;
        let mut client = Self {
            channel,
            instance: config.instance_name,
            headers: Arc::new(headers),
            caps: Default::default(),
            policy: config.policy,
            deadline: None,
            grpc_compression: Arc::new(Default::default()),
            stats: Arc::new(Mutex::new(Default::default())),
            chunk_workers: Arc::new(Mutex::new(Vec::new())),
        };
        let response = client
            .policy
            .retry(deadline, client.policy.rpc_timeout(), |budget| {
                let mut request = client.request(
                    reapi::GetCapabilitiesRequest {
                        instance_name: client.instance.clone(),
                    },
                    false,
                );
                request.set_timeout(budget);
                let mut rpc =
                    reapi::capabilities_client::CapabilitiesClient::new(client.channel.clone())
                        .accept_compressed(CompressionEncoding::Gzip);
                async move { rpc.get_capabilities(request).await }
            })
            .await
            .map_err(|s| rpc_error("GetCapabilities", s))?;
        client.grpc_compression.observe(response.metadata());
        client.caps = response
            .into_inner()
            .cache_capabilities
            .context("CAS omitted cache capabilities")?;
        ensure!(
            client.caps.digest_functions.contains(&BLAKE3),
            "CAS does not advertise BLAKE3"
        );
        ensure!(
            client.caps.max_batch_total_size_bytes >= 0 && client.caps.max_cas_blob_size_bytes >= 0,
            "CAS advertises negative size limit"
        );
        Ok(client)
    }
    pub(crate) fn with_deadline(&self, deadline: Instant) -> Self {
        let mut client = self.clone();
        client.deadline = Some(deadline);
        client
    }
    pub(crate) async fn wait_for_chunk_idle(&self) {
        let workers = std::mem::take(&mut *self.chunk_workers.lock().unwrap());
        for worker in workers {
            let _ = worker.await;
        }
    }
    pub fn capabilities(&self) -> &reapi::CacheCapabilities {
        &self.caps
    }
    pub fn stats(&self) -> UploadStats {
        self.stats.lock().expect("CAS stats mutex poisoned").clone()
    }
    fn record(&self, stats: &UploadStats) {
        self.stats
            .lock()
            .expect("CAS stats mutex poisoned")
            .add(stats);
    }
    fn request<T>(&self, value: T, chunk: bool) -> Request<T> {
        let mut req = Request::new(value);
        req.set_timeout(self.policy.rpc_timeout());
        for (k, v) in self.headers.iter() {
            req.metadata_mut().insert(k.clone(), v.clone());
        }
        if chunk {
            req.metadata_mut()
                .insert(CHUNK_HEADER, MetadataValue::from_static("FAST_CDC_2020"));
        }
        req
    }
    fn cas(
        &self,
    ) -> reapi::content_addressable_storage_client::ContentAddressableStorageClient<Channel> {
        reapi::content_addressable_storage_client::ContentAddressableStorageClient::new(
            self.channel.clone(),
        )
        .accept_compressed(CompressionEncoding::Gzip)
        .max_encoding_message_size(GRPC_BYTES)
        .max_decoding_message_size(GRPC_BYTES)
    }
    fn metadata_cas(
        &self,
        encoding: Option<CompressionEncoding>,
    ) -> reapi::content_addressable_storage_client::ContentAddressableStorageClient<Channel> {
        let client = self.cas();
        match encoding {
            Some(encoding) => client.send_compressed(encoding),
            None => client,
        }
    }
    fn deadline(&self) -> Instant {
        self.deadline
            .unwrap_or_else(|| Instant::now() + self.policy.operation_timeout())
    }
    // All retries, including the narrowly classified codec fallback, share one
    // original deadline. A codec fallback never grants a fresh retry budget.
    async fn metadata_rpc<T, F, Fut>(&self, call: F) -> Result<tonic::Response<T>, tonic::Status>
    where
        F: Fn(Option<CompressionEncoding>, Duration) -> Fut,
        Fut: std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        self.metadata_rpc_timeout(self.policy.rpc_timeout(), call)
            .await
    }
    async fn metadata_rpc_timeout<T, F, Fut>(
        &self,
        attempt_timeout: Duration,
        call: F,
    ) -> Result<tonic::Response<T>, tonic::Status>
    where
        F: Fn(Option<CompressionEncoding>, Duration) -> Fut,
        Fut: std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        self.policy
            .retry(self.deadline(), attempt_timeout, |budget| {
                let call = &call;
                async move {
                    let deadline = Instant::now() + budget;
                    let encoding = self.grpc_compression.request_encoding();
                    match call(encoding, budget).await {
                        Err(status) if self.grpc_compression.retry_identity(&status, encoding) => {
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            ::reapi::policy::timeout(deadline, call(None, remaining)).await
                        }
                        response => response,
                    }
                }
            })
            .await
    }
    /// Validate each response against its request. Never infer presence from an incomplete upload response.
    pub async fn find_missing(&self, digests: &[Digest]) -> Result<Vec<Digest>> {
        let mut operation = self.clone();
        operation.deadline = Some(self.deadline());
        let result = tokio::time::timeout_at(
            operation.deadline(),
            operation.find_missing_inner(digests, false),
        )
        .await;
        if Instant::now() >= operation.deadline() {
            return Err(rpc_error(
                "FindMissingBlobs",
                tonic::Status::deadline_exceeded("operation timed out"),
            ));
        }
        result.map_err(|_| {
            rpc_error(
                "FindMissingBlobs",
                tonic::Status::deadline_exceeded("operation timed out"),
            )
        })?
    }
    async fn find_missing_inner(&self, digests: &[Digest], chunk: bool) -> Result<Vec<Digest>> {
        for d in digests {
            validate_digest(d)?;
        }
        let mut missing = Vec::new();
        for batch in digests.chunks(FIND_MISSING_LIMIT) {
            let allowed: HashSet<_> = batch.iter().map(key).collect();
            let request = reapi::FindMissingBlobsRequest {
                instance_name: self.instance.clone(),
                blob_digests: batch.to_vec(),
                digest_function: BLAKE3,
            };
            let response = self
                .metadata_rpc(|encoding, budget| {
                    self.record(&UploadStats {
                        find_missing_calls: 1,
                        ..Default::default()
                    });
                    let mut client = self.metadata_cas(encoding);
                    let mut request = self.request(request.clone(), chunk);
                    request.set_timeout(budget);
                    async move { client.find_missing_blobs(request).await }
                })
                .await
                .map_err(|s| rpc_error("FindMissingBlobs", s))?
                .into_inner();
            let mut seen = HashSet::new();
            for d in response.missing_blob_digests {
                validate_digest(&d)?;
                ensure!(
                    allowed.contains(&key(&d)) && seen.insert(key(&d)),
                    "CAS returned unexpected or duplicate missing digest"
                );
                missing.push(d);
            }
        }
        Ok(missing)
    }
    /// Upload blobs already known to be missing. The caller owns presence TTLs and initial FindMissing.
    pub async fn upload_missing(&self, blobs: &[UploadBlob]) -> Result<UploadStats> {
        self.upload_missing_leased(blobs, &[]).await
    }
    /// Internal retained-payload publication keeps CDC blocking readers pinned
    /// even if the caller's upload future is cancelled.
    pub(crate) async fn upload_missing_leased(
        &self,
        blobs: &[UploadBlob],
        leases: &[Arc<local_cas::CachedBlob>],
    ) -> Result<UploadStats> {
        ensure!(
            leases.is_empty()
                || (leases.len() == blobs.len()
                    && leases
                        .iter()
                        .zip(blobs)
                        .all(|(lease, blob)| lease.path() == blob.path)),
            "CAS upload leases must match their payload paths"
        );
        let mut operation = self.clone();
        operation.deadline = Some(self.deadline());
        let result = tokio::time::timeout_at(
            operation.deadline(),
            operation.upload_missing_inner(blobs, leases),
        )
        .await;
        if Instant::now() >= operation.deadline() {
            return Err(rpc_error(
                "Upload",
                tonic::Status::deadline_exceeded("operation timed out"),
            ));
        }
        result.map_err(|_| {
            rpc_error(
                "Upload",
                tonic::Status::deadline_exceeded("operation timed out"),
            )
        })?
    }

    async fn upload_missing_inner(
        &self,
        blobs: &[UploadBlob],
        leases: &[Arc<local_cas::CachedBlob>],
    ) -> Result<UploadStats> {
        let mut stats = UploadStats::default();
        let mut pending = Vec::new();
        let mut pending_bytes = 0;
        let mut seen = HashSet::new();
        let limit = self.batch_limit();
        for (index, blob) in blobs.iter().enumerate() {
            validate_digest(&blob.digest)?;
            if !seen.insert(key(&blob.digest)) {
                continue;
            }
            ensure!(
                self.caps.max_cas_blob_size_bytes == 0
                    || blob.digest.size_bytes <= self.caps.max_cas_blob_size_bytes,
                "blob exceeds advertised CAS size limit"
            );
            ensure!(
                tokio::fs::metadata(&blob.path)
                    .await
                    .context("stat CAS spool")?
                    .len()
                    == blob.digest.size_bytes as u64,
                "CAS spool size does not match digest"
            );
            if let Some(params) = self.cdc_params(blob.digest.size_bytes) {
                self.flush_batch(&mut pending, false, &mut stats).await?;
                pending_bytes = 0;
                self.upload_cdc(blob, params, &mut stats, leases.get(index).cloned())
                    .await?;
            } else {
                let part = Part {
                    digest: blob.digest.clone(),
                    path: blob.path.clone(),
                    offset: 0,
                };
                self.queue_part(
                    part,
                    false,
                    &mut pending,
                    &mut pending_bytes,
                    limit,
                    &mut stats,
                )
                .await?;
            }
        }
        self.flush_batch(&mut pending, false, &mut stats).await?;
        Ok(stats)
    }
    fn batch_limit(&self) -> usize {
        if self.caps.max_batch_total_size_bytes > 0 {
            BATCH_BYTES.min(self.caps.max_batch_total_size_bytes as usize)
        } else {
            BATCH_BYTES
        }
    }
    fn cdc_params(&self, size: i64) -> Option<reapi::FastCdc2020Params> {
        let p = self.caps.fast_cdc_2020_params.as_ref()?;
        (self.caps.splice_blob_support
            // fastcdc 4 supports average sizes through 4 MiB (REAPI allows 8 MiB).
            && (1024..=fastcdc::v2020::AVERAGE_MAX as u64).contains(&p.avg_chunk_size_bytes)
            && size as u64 > p.avg_chunk_size_bytes * 4)
            .then_some(*p)
    }
    async fn queue_part(
        &self,
        part: Part,
        chunk: bool,
        pending: &mut Vec<reapi::batch_update_blobs_request::Request>,
        bytes: &mut usize,
        limit: usize,
        stats: &mut UploadStats,
    ) -> Result<()> {
        let size = part.digest.size_bytes as usize;
        if size > limit {
            self.flush_batch(pending, chunk, stats).await?;
            *bytes = 0;
            self.stream_part(&part, chunk, stats).await?;
            return Ok(());
        }
        if *bytes + size > limit || pending.len() >= FIND_MISSING_LIMIT {
            self.flush_batch(pending, chunk, stats).await?;
            *bytes = 0;
        }
        let data = read_part(&part).await?;
        let (data, compressor) = if self.caps.supported_batch_update_compressors.contains(&ZSTD) {
            (::reapi::compression::encode_zstd(&data)?, ZSTD)
        } else {
            (data, 0)
        };
        let request = reapi::batch_update_blobs_request::Request {
            digest: Some(part.digest.clone()),
            data,
            compressor,
        };
        let envelope = reapi::BatchUpdateBlobsRequest {
            instance_name: self.instance.clone(),
            requests: Vec::new(),
            digest_function: BLAKE3,
        }
        .encoded_len();
        let request_bytes = request.encoded_len();
        if envelope + 1 + prost::length_delimiter_len(request_bytes) + request_bytes > GRPC_BYTES {
            self.flush_batch(pending, chunk, stats).await?;
            *bytes = 0;
            return self.stream_part(&part, chunk, stats).await;
        }
        pending.push(request);
        let envelope_bytes = reapi::BatchUpdateBlobsRequest {
            instance_name: self.instance.clone(),
            requests: Vec::new(),
            digest_function: BLAKE3,
        }
        .encoded_len()
            + pending
                .iter()
                .map(|r| {
                    let n = r.encoded_len();
                    1 + prost::length_delimiter_len(n) + n
                })
                .sum::<usize>();
        if envelope_bytes > GRPC_BYTES {
            let last = pending.pop().unwrap();
            self.flush_batch(pending, chunk, stats).await?;
            pending.push(last);
            *bytes = size;
        } else {
            *bytes += size;
        }
        Ok(())
    }
    async fn flush_batch(
        &self,
        pending: &mut Vec<reapi::batch_update_blobs_request::Request>,
        chunk: bool,
        stats: &mut UploadStats,
    ) -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let remaining = Arc::new(Mutex::new(std::mem::take(pending)));
        let attempts = AtomicU64::new(0);
        let complete = Arc::new(Mutex::new(UploadStats::default()));
        let result = self
            .policy
            .retry(self.deadline(), self.policy.rpc_timeout(), |budget| {
                attempts.fetch_add(1, Ordering::Relaxed);
                self.record(&UploadStats {
                    batch_update_calls: 1,
                    ..Default::default()
                });
                let requests = remaining.lock().unwrap().clone();
                let emitted = UploadStats {
                    uploaded_wire_bytes: requests.iter().map(|r| r.data.len() as u64).sum(),
                    zstd_batch_blobs: requests.iter().filter(|r| r.compressor == ZSTD).count()
                        as u64,
                    ..Default::default()
                };
                self.record(&emitted);
                complete.lock().unwrap().add(&emitted);
                let encoding = if requests.iter().all(|r| r.compressor == 0) {
                    self.grpc_compression.request_encoding()
                } else {
                    None
                };
                let mut client = self.metadata_cas(encoding);
                let mut request = self.request(
                    reapi::BatchUpdateBlobsRequest {
                        instance_name: self.instance.clone(),
                        requests: requests.clone(),
                        digest_function: BLAKE3,
                    },
                    chunk,
                );
                request.set_timeout(budget);
                let remaining = remaining.clone();
                let complete = complete.clone();
                let codec = &self.grpc_compression;
                // Codec fallback shares this attempt's original deadline and never
                // replays acknowledged per-item successes.
                let attempts = &attempts;
                async move {
                    let deadline = Instant::now() + budget;
                    let response = match client.batch_update_blobs(request).await {
                        Err(status) if codec.retry_identity(&status, encoding) => {
                            attempts.fetch_add(1, Ordering::Relaxed);
                            let emitted = UploadStats {
                                batch_update_calls: 1,
                                uploaded_wire_bytes: requests
                                    .iter()
                                    .map(|r| r.data.len() as u64)
                                    .sum(),
                                zstd_batch_blobs: requests
                                    .iter()
                                    .filter(|r| r.compressor == ZSTD)
                                    .count()
                                    as u64,
                                ..Default::default()
                            };
                            self.record(&emitted);
                            // The call count is accumulated separately below.
                            let mut wire = emitted;
                            wire.batch_update_calls = 0;
                            complete.lock().unwrap().add(&wire);
                            let mut identity = self.request(
                                reapi::BatchUpdateBlobsRequest {
                                    instance_name: self.instance.clone(),
                                    requests: requests.clone(),
                                    digest_function: BLAKE3,
                                },
                                chunk,
                            );
                            identity
                                .set_timeout(deadline.saturating_duration_since(Instant::now()));
                            self.cas().batch_update_blobs(identity).await?
                        }
                        result => result?,
                    }
                    .into_inner();
                    let expected: HashSet<_> = requests
                        .iter()
                        .map(|r| key(r.digest.as_ref().unwrap()))
                        .collect();
                    let mut seen = HashSet::new();
                    let mut statuses = std::collections::HashMap::new();
                    for response in response.responses {
                        let digest = response
                            .digest
                            .ok_or_else(|| tonic::Status::data_loss("missing batch digest"))?;
                        if !expected.contains(&key(&digest)) || !seen.insert(key(&digest)) {
                            return Err(tonic::Status::data_loss(
                                "unexpected or duplicate batch digest",
                            ));
                        }
                        let status = response
                            .status
                            .ok_or_else(|| tonic::Status::data_loss("missing batch status"))?;
                        statuses.insert(key(&digest), status);
                    }
                    if seen != expected {
                        return Err(tonic::Status::data_loss("missing batch responses"));
                    }
                    let mut retry = Vec::new();
                    let mut failure = None;
                    let mut done = UploadStats::default();
                    for request in requests {
                        let digest = request.digest.as_ref().unwrap();
                        let item_status = &statuses[&key(digest)];
                        let code = Code::from_i32(item_status.code);
                        if code == Code::Ok {
                            done.uploaded_blobs += 1;
                            done.uploaded_bytes += digest.size_bytes as u64;
                        } else {
                            let status = tonic::Status::with_details(
                                code,
                                "CAS batch item failed",
                                item_status.encode_to_vec().into(),
                            );
                            if ::reapi::policy::retryable(&status) {
                                retry.push(request);
                                if failure.is_none() {
                                    failure = Some(status);
                                }
                            } else {
                                // Permanent failures take priority over transient entries.
                                failure = Some(status);
                            }
                        }
                    }
                    self.record(&done);
                    complete.lock().unwrap().add(&done);
                    *remaining.lock().unwrap() = retry;
                    match failure {
                        Some(status) => Err(status),
                        None => Ok(()),
                    }
                }
            })
            .await;
        stats.batch_update_calls += attempts.load(Ordering::Relaxed);
        stats.add(&complete.lock().unwrap());
        result.map_err(|s| rpc_error("BatchUpdateBlobs", s))
    }
    async fn stream_part(&self, part: &Part, chunk: bool, stats: &mut UploadStats) -> Result<()> {
        let deadline = self.deadline();
        let attempts = AtomicU64::new(0);
        let local_failure = Mutex::new(None);
        let emitted = Arc::new(AtomicU64::new(0));
        let done = self
            .policy
            .retry(
                deadline,
                deadline.saturating_duration_since(Instant::now()),
                |budget| {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    let local_failure = &local_failure;
                    let emitted = emitted.clone();
                    async move {
                        self.stream_part_attempt(part, chunk, budget, emitted)
                            .await
                            .map_err(|error| match error.downcast_ref::<RpcError>() {
                                Some(rpc) => rpc.status.clone(),
                                None => {
                                    *local_failure.lock().unwrap() = Some(error);
                                    tonic::Status::data_loss(
                                        "CAS upload source or acknowledgment invalid",
                                    )
                                }
                            })
                    }
                },
            )
            .await;
        let attempts = attempts.load(Ordering::Relaxed);
        stats.bytestream_write_calls += attempts;
        stats.zstd_stream_blobs +=
            attempts * u64::from(self.caps.supported_compressors.contains(&ZSTD));
        stats.uploaded_wire_bytes += emitted.load(Ordering::Relaxed);
        let done = done.map_err(|status| {
            local_failure
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| rpc_error("ByteStream.Write", status))
        })?;
        stats.add(&done);
        self.record(&done);
        Ok(())
    }
    async fn stream_part_attempt(
        &self,
        part: &Part,
        chunk: bool,
        budget: Duration,
        emitted: Arc<AtomicU64>,
    ) -> Result<UploadStats> {
        let compressed = self.caps.supported_compressors.contains(&ZSTD);
        let upload_id = uuid::Uuid::new_v4().to_string();
        let prefix = if self.instance.is_empty() {
            String::new()
        } else {
            format!("{}/", self.instance)
        };
        let kind = if compressed {
            "compressed-blobs/zstd"
        } else {
            "blobs"
        };
        let resource = format!(
            "{prefix}uploads/{upload_id}/{kind}/blake3/{}/{}",
            part.digest.hash, part.digest.size_bytes
        );
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let part = part.clone();
        let expected_size = part.digest.size_bytes;
        let mut producer = AbortOnDrop(tokio::spawn(async move {
            let mut file = tokio::fs::File::open(&part.path)
                .await
                .context("open CAS spool")?;
            file.seek(std::io::SeekFrom::Start(part.offset)).await?;
            let verified = VerifiedPartReader {
                file: file.take(part.digest.size_bytes as u64),
                remaining: part.digest.size_bytes as u64,
                hasher: blake3::Hasher::new(),
                hash: part.digest.hash,
                verified: false,
            };
            // Both paths keep memory bounded by the encoder window, one read buffer
            // and the two-frame channel. The digest always describes plaintext.
            let mut reader: Box<dyn AsyncRead + Unpin + Send> = if compressed {
                Box::new(async_compression::tokio::bufread::ZstdEncoder::new(
                    BufReader::new(verified),
                ))
            } else {
                Box::new(verified)
            };
            let mut offset = 0i64;
            loop {
                let mut data = vec![0; STREAM_BYTES];
                let n = reader.read(&mut data).await.context("read CAS upload")?;
                data.truncate(n);
                tx.send(bs::WriteRequest {
                    resource_name: if offset == 0 {
                        resource.clone()
                    } else {
                        String::new()
                    },
                    write_offset: offset,
                    finish_write: n == 0,
                    data,
                })
                .await
                .map_err(|_| anyhow::anyhow!("CAS stopped consuming upload"))?;
                offset += n as i64;
                if n == 0 {
                    return Ok::<_, anyhow::Error>(offset);
                }
            }
        }));
        let started = UploadStats {
            bytestream_write_calls: 1,
            zstd_stream_blobs: u64::from(compressed),
            ..Default::default()
        };
        self.record(&started);
        let shared_stats = self.stats.clone();
        let (progress, mut progress_rx) = tokio::sync::watch::channel(Instant::now());
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(
            move |request: bs::WriteRequest| {
                let bytes = request.data.len() as u64;
                emitted.fetch_add(bytes, Ordering::Relaxed);
                shared_stats
                    .lock()
                    .expect("CAS stats mutex poisoned")
                    .uploaded_wire_bytes += bytes;
                progress.send_replace(Instant::now());
                request
            },
        );
        let mut request = self.request(stream, chunk);
        request.set_timeout(budget);
        let mut client = bs::byte_stream_client::ByteStreamClient::new(self.channel.clone())
            .accept_compressed(CompressionEncoding::Gzip);
        let rpc = client.write(request);
        tokio::pin!(rpc);
        let mut progress_closed = false;
        let response = loop {
            let idle_deadline = *progress_rx.borrow() + self.policy.stream_idle_timeout();
            tokio::select! {
                response = &mut rpc => break response,
                _ = tokio::time::sleep_until(idle_deadline) => {
                    if Instant::now() >= *progress_rx.borrow() + self.policy.stream_idle_timeout() {
                        break Err(tonic::Status::deadline_exceeded("CAS stream made no progress"));
                    }
                }
                changed = progress_rx.changed(), if !progress_closed => {
                    if changed.is_err() {
                        // The stream is fully consumed; still bound the final acknowledgment.
                        progress_closed = true;
                    }
                }
            }
        }
        .map_err(|s| rpc_error("ByteStream.Write", s))?
        .into_inner();
        // REAPI permits early success when another writer already populated CAS.
        // Drop and join the producer immediately rather than waiting on a full queue.
        if (compressed && response.committed_size == -1)
            || (!compressed && response.committed_size == expected_size)
        {
            producer.0.abort();
            let _ = (&mut producer.0).await;
        } else {
            let produced = tokio::time::timeout_at(self.deadline(), &mut producer.0)
                .await
                .context("CAS upload reader timed out")?
                .context("CAS upload reader task failed")??;
            ensure!(
                ::reapi::upload_ack::valid_completed_write_ack(
                    compressed,
                    response.committed_size,
                    expected_size,
                    produced as u64,
                ),
                "CAS ByteStream committed_size mismatch"
            );
        }
        let done = UploadStats {
            uploaded_bytes: expected_size as u64,
            uploaded_blobs: 1,
            ..Default::default()
        };
        Ok(done)
    }
    async fn upload_cdc(
        &self,
        blob: &UploadBlob,
        params: reapi::FastCdc2020Params,
        stats: &mut UploadStats,
        lease: Option<Arc<local_cas::CachedBlob>>,
    ) -> Result<()> {
        match self.upload_cdc_inner(blob, params, stats, lease).await {
            Err(error)
                if error
                    .downcast_ref::<RpcError>()
                    .is_some_and(|e| e.code == Code::Unimplemented)
                    || error.is::<CdcLimit>() =>
            {
                let fallback = UploadStats {
                    cdc_fallbacks: 1,
                    ..Default::default()
                };
                stats.add(&fallback);
                self.record(&fallback);
                self.stream_part(
                    &Part {
                        digest: blob.digest.clone(),
                        path: blob.path.clone(),
                        offset: 0,
                    },
                    false,
                    stats,
                )
                .await
            }
            result => result,
        }
    }
    async fn upload_cdc_inner(
        &self,
        blob: &UploadBlob,
        params: reapi::FastCdc2020Params,
        stats: &mut UploadStats,
        lease: Option<Arc<local_cas::CachedBlob>>,
    ) -> Result<()> {
        let source = blob.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelChunkOnDrop(cancelled.clone());
        let deadline = self.deadline();
        let (sent, result) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _lease = lease;
            let value = chunk_file(source, params, &cancelled, deadline);
            let _ = sent.send(value);
        });
        {
            let mut workers = self.chunk_workers.lock().unwrap();
            workers.retain(|worker| !worker.is_finished());
            workers.push(worker);
        }
        let parts = result.await.context("CAS chunk task failed")??;
        let mut digests = Vec::new();
        let mut unique = HashSet::new();
        for p in &parts {
            if unique.insert(key(&p.digest)) {
                digests.push(p.digest.clone());
            }
        }
        stats.find_missing_calls += digests.len().div_ceil(FIND_MISSING_LIMIT) as u64;
        let missing = self.find_missing_inner(&digests, true).await?;
        self.upload_parts_missing(&parts, &missing, stats).await?;
        let request = reapi::SpliceBlobRequest {
            instance_name: self.instance.clone(),
            blob_digest: Some(blob.digest.clone()),
            chunk_digests: parts.iter().map(|p| p.digest.clone()).collect(),
            digest_function: BLAKE3,
            chunking_function: reapi::chunking_function::Value::FastCdc2020 as i32,
        };
        // A bounded retry handles chunks evicted between FindMissing and splice.
        for attempt in 0..2 {
            let started = UploadStats {
                splice_calls: 1,
                ..Default::default()
            };
            stats.add(&started);
            self.record(&started);
            let response = self
                .metadata_rpc_timeout(
                    Duration::from_millis(
                        (parts.len() as u64)
                            .saturating_mul(250)
                            .clamp(60_000, 600_000),
                    ),
                    |encoding, budget| {
                        let mut client = self.metadata_cas(encoding);
                        let mut request = self.request(request.clone(), false);
                        request.set_timeout(budget);
                        async move { client.splice_blob(request).await }
                    },
                )
                .await;
            match response {
                Ok(r) => {
                    ensure!(
                        r.into_inner().blob_digest.as_ref() == Some(&blob.digest),
                        "CAS splice returned wrong digest"
                    );
                    let done = UploadStats {
                        cdc_blobs: 1,
                        cdc_chunks: parts.len() as u64,
                        ..Default::default()
                    };
                    stats.add(&done);
                    self.record(&done);
                    return Ok(());
                }
                Err(s)
                    if attempt == 0
                        && matches!(s.code(), Code::NotFound | Code::FailedPrecondition) =>
                {
                    stats.find_missing_calls += digests.len().div_ceil(FIND_MISSING_LIMIT) as u64;
                    let missing = self.find_missing_inner(&digests, true).await?;
                    if missing.is_empty() {
                        return Err(rpc_error("SpliceBlob", s));
                    }
                    self.upload_parts_missing(&parts, &missing, stats).await?;
                }
                Err(s) => return Err(rpc_error("SpliceBlob", s)),
            }
        }
        bail!("CAS splice retry exhausted")
    }
    async fn upload_parts_missing(
        &self,
        parts: &[Part],
        missing: &[Digest],
        stats: &mut UploadStats,
    ) -> Result<()> {
        let mut wanted: HashSet<_> = missing.iter().map(key).collect();
        let mut pending = Vec::new();
        let mut bytes = 0;
        for part in parts {
            if wanted.remove(&key(&part.digest)) {
                self.queue_part(
                    part.clone(),
                    true,
                    &mut pending,
                    &mut bytes,
                    self.batch_limit(),
                    stats,
                )
                .await?;
            }
        }
        self.flush_batch(&mut pending, true, stats).await
    }
}
#[derive(Debug)]
struct CdcLimit;
impl std::fmt::Display for CdcLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CAS chunk mapping exceeds unary client limit")
    }
}
impl std::error::Error for CdcLimit {}
struct RpcError {
    method: String,
    code: Code,
    // Retain RetryInfo for the shared policy, but never render server-provided
    // text, metadata or details through either Display or Debug.
    status: tonic::Status,
}
impl std::fmt::Debug for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcError")
            .field("method", &self.method)
            .field("code", &self.code)
            .finish()
    }
}
impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CAS {} failed with {}", self.method, self.code)
    }
}
impl std::error::Error for RpcError {}
fn rpc_error(method: &str, s: tonic::Status) -> anyhow::Error {
    // Server text and details may echo request credentials. Only expose the canonical code.
    RpcError {
        method: method.into(),
        code: s.code(),
        status: s,
    }
    .into()
}
async fn read_part(part: &Part) -> Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(&part.path)
        .await
        .context("open CAS spool")?;
    file.seek(std::io::SeekFrom::Start(part.offset)).await?;
    let mut bytes = vec![0; part.digest.size_bytes as usize];
    file.read_exact(&mut bytes)
        .await
        .context("read CAS spool")?;
    ensure!(
        blake3::hash(&bytes).to_hex().as_str() == part.digest.hash,
        "CAS spool content does not match digest"
    );
    Ok(bytes)
}
fn chunk_file(
    blob: UploadBlob,
    params: reapi::FastCdc2020Params,
    cancelled: &AtomicBool,
    deadline: Instant,
) -> Result<Vec<Part>> {
    let file = std::fs::File::open(&blob.path).context("open CAS chunk spool")?;
    let avg = params.avg_chunk_size_bytes as usize;
    let chunks = fastcdc::v2020::StreamCDC::with_level_and_seed(
        file,
        avg / 4,
        avg,
        avg * 4,
        fastcdc::v2020::Normalization::Level2,
        params.seed as u64,
    );
    let mut parts = Vec::new();
    let mut full = blake3::Hasher::new();
    let mut total = 0;
    for chunk in chunks {
        ensure!(
            !cancelled.load(Ordering::Relaxed) && Instant::now() < deadline,
            "CAS chunking cancelled or timed out"
        );
        let chunk = chunk.context("read CAS chunk spool")?;
        // Bound metadata independently of source-file size and fall back to ByteStream.
        if parts.len() == 32_768 {
            return Err(CdcLimit.into());
        }
        full.update(&chunk.data);
        total += chunk.length as i64;
        parts.push(Part {
            digest: Digest {
                hash: blake3::hash(&chunk.data).to_hex().to_string(),
                size_bytes: chunk.length as i64,
            },
            path: blob.path.clone(),
            offset: chunk.offset,
        });
    }
    ensure!(
        total == blob.digest.size_bytes && full.finalize().to_hex().as_str() == blob.digest.hash,
        "CAS chunk spool content does not match digest"
    );
    Ok(parts)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::reapi::{
        capabilities_server::{Capabilities, CapabilitiesServer},
        content_addressable_storage_server::{
            ContentAddressableStorage, ContentAddressableStorageServer,
        },
    };
    use super::*;
    use bs::byte_stream_server::{ByteStream, ByteStreamServer};
    use std::collections::HashMap;
    use tonic::{Response, Status};

    #[derive(Default)]
    struct State {
        caps: reapi::CacheCapabilities,
        blobs: HashMap<(String, i64), Vec<u8>>,
        missing_sizes: Vec<usize>,
        batch_sizes: Vec<usize>,
        frames: Vec<usize>,
        batch_compressors: Vec<i32>,
        batch_plain_sizes: Vec<usize>,
        resources: Vec<String>,
        chunk_headers: usize,
        splice_calls: usize,
        write_calls: usize,
        capabilities_calls: usize,
        mode: &'static str,
        allow_no_auth: bool,
    }
    #[derive(Clone, Default)]
    struct Fixture(Arc<Mutex<State>>);
    impl Fixture {
        fn note<T>(&self, req: &Request<T>) -> Result<(), Status> {
            if !self.0.lock().unwrap().allow_no_auth
                && req
                    .metadata()
                    .get("x-buildbuddy-api-key")
                    .and_then(|v| v.to_str().ok())
                    != Some("fixture-secret")
            {
                return Err(Status::unauthenticated("fixture-secret must not leak"));
            }
            if req
                .metadata()
                .get(CHUNK_HEADER)
                .and_then(|v| v.to_str().ok())
                == Some("FAST_CDC_2020")
            {
                self.0.lock().unwrap().chunk_headers += 1;
            }
            Ok(())
        }
    }
    #[tonic::async_trait]
    impl Capabilities for Fixture {
        async fn get_capabilities(
            &self,
            req: Request<reapi::GetCapabilitiesRequest>,
        ) -> Result<Response<reapi::ServerCapabilities>, Status> {
            self.note(&req)?;
            assert_eq!(req.get_ref().instance_name, "test/instance");
            {
                let mut state = self.0.lock().unwrap();
                state.capabilities_calls += 1;
                if state.mode == "transient-capabilities" && state.capabilities_calls == 1 {
                    return Err(Status::unavailable("temporary"));
                }
            }
            let mut response = Response::new(reapi::ServerCapabilities {
                cache_capabilities: Some(self.0.lock().unwrap().caps.clone()),
                high_api_version: Some(proto::build::bazel::semver::SemVer {
                    major: 2,
                    minor: 11,
                    ..Default::default()
                }),
                ..Default::default()
            });
            // Tonic does not automatically advertise request codecs on successful
            // replies; model a peer that explicitly advertises this capability.
            response.metadata_mut().insert(
                "grpc-accept-encoding",
                MetadataValue::from_static("gzip,identity"),
            );
            Ok(response)
        }
    }
    #[tonic::async_trait]
    impl ContentAddressableStorage for Fixture {
        async fn find_missing_blobs(
            &self,
            req: Request<reapi::FindMissingBlobsRequest>,
        ) -> Result<Response<reapi::FindMissingBlobsResponse>, Status> {
            self.note(&req)?;
            let req = req.into_inner();
            assert_eq!(req.digest_function, BLAKE3);
            let mut s = self.0.lock().unwrap();
            s.missing_sizes.push(req.blob_digests.len());
            if s.mode == "transient-missing" && s.missing_sizes.len() == 1 {
                return Err(Status::unavailable("temporary"));
            }
            if matches!(
                s.mode,
                "metadata-unimplemented" | "metadata-unauthenticated" | "metadata-denied"
            ) {
                let code = match s.mode {
                    "metadata-unimplemented" => Code::Unimplemented,
                    "metadata-unauthenticated" => Code::Unauthenticated,
                    _ => Code::PermissionDenied,
                };
                let mut metadata = tonic::metadata::MetadataMap::new();
                if code != Code::Unimplemented {
                    metadata.insert(
                        "grpc-accept-encoding",
                        MetadataValue::from_static("identity"),
                    );
                }
                return Err(Status::with_metadata(
                    code,
                    "fixture-secret must not leak",
                    metadata,
                ));
            }
            if s.mode == "chunk-find-unimplemented" {
                return Err(Status::unimplemented("chunk lookup unsupported"));
            }
            let missing = if s.mode == "extra-missing" {
                vec![Digest {
                    hash: "f".repeat(64),
                    size_bytes: 999,
                }]
            } else {
                req.blob_digests
                    .into_iter()
                    .filter(|d| !s.blobs.contains_key(&key(d)))
                    .collect()
            };
            Ok(Response::new(reapi::FindMissingBlobsResponse {
                missing_blob_digests: missing,
            }))
        }
        async fn batch_update_blobs(
            &self,
            req: Request<reapi::BatchUpdateBlobsRequest>,
        ) -> Result<Response<reapi::BatchUpdateBlobsResponse>, Status> {
            self.note(&req)?;
            if self.0.lock().unwrap().mode == "hang-batch" {
                return std::future::pending().await;
            }
            let req = req.into_inner();
            assert_eq!(req.digest_function, BLAKE3);
            let wire_bytes = req.requests.iter().map(|r| r.data.len()).sum();
            let mut decoded = Vec::new();
            for r in req.requests {
                let data = decode_upload(r.data, r.compressor == ZSTD).await;
                decoded.push((r.digest.unwrap(), r.compressor, data));
            }
            let mut s = self.0.lock().unwrap();
            s.batch_sizes.push(wire_bytes);
            s.batch_plain_sizes
                .push(decoded.iter().map(|(_, _, data)| data.len()).sum());
            let mut responses = Vec::new();
            let first_batch = s.batch_sizes.len() == 1;
            for (index, (d, compressor, data)) in decoded.into_iter().enumerate() {
                s.batch_compressors.push(compressor);
                assert!(
                    compressor == 0
                        || s.caps
                            .supported_batch_update_compressors
                            .contains(&compressor)
                );
                assert_eq!(d.hash, blake3::hash(&data).to_hex().to_string());
                assert_eq!(d.size_bytes, data.len() as i64);
                let transient = s.mode == "partial-transient-batch" && first_batch && index == 0;
                if !transient {
                    s.blobs.insert(key(&d), data);
                }
                responses.push(reapi::batch_update_blobs_response::Response {
                    digest: Some(d),
                    status: Some(proto::google::rpc::Status {
                        code: if s.mode == "batch-error" {
                            7
                        } else if transient {
                            14
                        } else {
                            0
                        },
                        ..Default::default()
                    }),
                });
            }
            if s.mode == "omit-batch" {
                responses.clear();
            }
            if s.mode == "duplicate-batch" {
                responses.push(responses[0].clone());
            }
            Ok(Response::new(reapi::BatchUpdateBlobsResponse { responses }))
        }
        async fn splice_blob(
            &self,
            req: Request<reapi::SpliceBlobRequest>,
        ) -> Result<Response<reapi::SpliceBlobResponse>, Status> {
            self.note(&req)?;
            let req = req.into_inner();
            assert_eq!(req.digest_function, BLAKE3);
            assert_eq!(
                req.chunking_function,
                reapi::chunking_function::Value::FastCdc2020 as i32
            );
            let mut s = self.0.lock().unwrap();
            s.splice_calls += 1;
            if s.mode == "splice-unimplemented" {
                return Err(Status::unimplemented("unsupported"));
            }
            if s.mode == "splice-denied" {
                return Err(Status::permission_denied("fixture-secret must not leak"));
            }
            if s.mode == "evict-chunk" && s.splice_calls == 1 {
                s.blobs.remove(&key(&req.chunk_digests[0]));
                return Err(Status::not_found("evicted chunk"));
            }
            let mut data = Vec::new();
            for d in req.chunk_digests {
                data.extend_from_slice(
                    s.blobs
                        .get(&key(&d))
                        .ok_or_else(|| Status::not_found("missing chunk"))?,
                );
            }
            let d = req.blob_digest.unwrap();
            assert_eq!(d.hash, blake3::hash(&data).to_hex().to_string());
            assert_eq!(d.size_bytes, data.len() as i64);
            s.blobs.insert(key(&d), data);
            let response = if s.mode == "wrong-splice" {
                Digest {
                    hash: "f".repeat(64),
                    ..d
                }
            } else {
                d
            };
            Ok(Response::new(reapi::SpliceBlobResponse {
                blob_digest: Some(response),
            }))
        }
        async fn batch_read_blobs(
            &self,
            _: Request<reapi::BatchReadBlobsRequest>,
        ) -> Result<Response<reapi::BatchReadBlobsResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        type GetTreeStream = tokio_stream::Empty<Result<reapi::GetTreeResponse, Status>>;
        async fn get_tree(
            &self,
            _: Request<reapi::GetTreeRequest>,
        ) -> Result<Response<Self::GetTreeStream>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn split_blob(
            &self,
            _: Request<reapi::SplitBlobRequest>,
        ) -> Result<Response<reapi::SplitBlobResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        type GetChunkMappingStream =
            tokio_stream::Empty<Result<reapi::GetChunkMappingResponse, Status>>;
        async fn get_chunk_mapping(
            &self,
            _: Request<reapi::GetChunkMappingRequest>,
        ) -> Result<Response<Self::GetChunkMappingStream>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn register_chunk_mapping(
            &self,
            _: Request<tonic::Streaming<reapi::RegisterChunkMappingRequest>>,
        ) -> Result<Response<reapi::RegisterChunkMappingResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
    }
    #[tonic::async_trait]
    impl ByteStream for Fixture {
        type ReadStream = tokio_stream::Empty<Result<bs::ReadResponse, Status>>;
        async fn read(
            &self,
            _: Request<bs::ReadRequest>,
        ) -> Result<Response<Self::ReadStream>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn query_write_status(
            &self,
            _: Request<bs::QueryWriteStatusRequest>,
        ) -> Result<Response<bs::QueryWriteStatusResponse>, Status> {
            Err(Status::unimplemented("unused"))
        }
        async fn write(
            &self,
            req: Request<tonic::Streaming<bs::WriteRequest>>,
        ) -> Result<Response<bs::WriteResponse>, Status> {
            self.note(&req)?;
            {
                let mut state = self.0.lock().unwrap();
                state.write_calls += 1;
            }
            if self.0.lock().unwrap().mode == "hang-write" {
                return std::future::pending().await;
            }
            let mut stream = req.into_inner();
            let mut data = Vec::new();
            let mut resource = String::new();
            let mut finished = false;
            while let Some(r) = stream.message().await? {
                assert!(!finished);
                assert_eq!(r.write_offset, data.len() as i64);
                assert!(r.data.len() <= STREAM_BYTES);
                if resource.is_empty() {
                    resource = r.resource_name;
                } else {
                    assert!(r.resource_name.is_empty());
                }
                let mode = self.0.lock().unwrap().mode;
                if matches!(
                    mode,
                    "early-complete" | "early-logical-size" | "early-wire-size"
                ) {
                    return Ok(Response::new(bs::WriteResponse {
                        committed_size: if mode == "early-wire-size" {
                            r.data.len() as i64
                        } else if mode == "early-complete"
                            && resource.contains("/compressed-blobs/")
                        {
                            -1
                        } else {
                            resource.rsplit('/').next().unwrap().parse().unwrap()
                        },
                    }));
                }
                self.0.lock().unwrap().frames.push(r.data.len());
                data.extend_from_slice(&r.data);
                let transient_failure = {
                    let state = self.0.lock().unwrap();
                    state.mode == "transient-write" && state.write_calls == 1
                };
                if transient_failure {
                    return Err(Status::unavailable("temporary after partial upload"));
                }
                finished = r.finish_write;
            }
            if !finished {
                return Err(Status::invalid_argument("unfinished write"));
            }
            assert!(resource.starts_with("test/instance/uploads/"));
            let compressed = resource.contains("/compressed-blobs/zstd/blake3/");
            assert!(compressed || resource.contains("/blobs/blake3/"));
            let wire_len = data.len() as i64;
            let data = decode_upload(data, compressed).await;
            let d = Digest {
                hash: blake3::hash(&data).to_hex().to_string(),
                size_bytes: data.len() as i64,
            };
            assert!(resource.ends_with(&format!("/{}/{}", d.hash, d.size_bytes)));
            let delayed = self.0.lock().unwrap().mode == "delayed-write-ack";
            if delayed {
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
            let mut s = self.0.lock().unwrap();
            s.resources.push(resource);
            s.blobs.insert(key(&d), data);
            Ok(Response::new(bs::WriteResponse {
                committed_size: if s.mode == "stream-minus-one" {
                    -1
                } else if s.mode == "logical-compressed-size" {
                    d.size_bytes
                } else if s.mode == "negative-compressed-size" {
                    -2
                } else if s.mode == "wrong-compressed-size" {
                    d.size_bytes + wire_len + 1
                } else {
                    (if compressed { wire_len } else { d.size_bytes })
                        - if s.mode == "short-write" { 1 } else { 0 }
                },
            }))
        }
    }
    pub(crate) struct Running {
        pub(crate) endpoint: String,
        fixture: Fixture,
        client: CasClient,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Running {
        pub(crate) fn contains(&self, digest: &Digest) -> bool {
            self.fixture
                .0
                .lock()
                .unwrap()
                .blobs
                .contains_key(&key(digest))
        }
        pub(crate) fn evict(&self, digest: &Digest) {
            self.fixture.0.lock().unwrap().blobs.remove(&key(digest));
        }
        pub(crate) fn find_missing_count(&self) -> usize {
            self.fixture.0.lock().unwrap().missing_sizes.len()
        }
        pub(crate) fn missing_batch_sizes(&self) -> Vec<usize> {
            self.fixture.0.lock().unwrap().missing_sizes.clone()
        }
        pub(crate) fn upload_count(&self) -> usize {
            let s = self.fixture.0.lock().unwrap();
            s.batch_sizes.len() + s.resources.len()
        }
        pub(crate) fn set_mode(&self, mode: &'static str) {
            self.fixture.0.lock().unwrap().mode = mode;
        }
    }
    impl Drop for Running {
        fn drop(&mut self) {
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
            self.task.abort();
        }
    }
    pub(crate) async fn server(cdc: bool, max_batch: i64, mode: &'static str) -> Running {
        compressed_server(cdc, max_batch, mode, false, false).await
    }
    async fn compressed_server(
        cdc: bool,
        max_batch: i64,
        mode: &'static str,
        batch_zstd: bool,
        stream_zstd: bool,
    ) -> Running {
        let fixture = Fixture::default();
        fixture.0.lock().unwrap().caps = reapi::CacheCapabilities {
            digest_functions: vec![BLAKE3],
            supported_compressors: if stream_zstd { vec![ZSTD] } else { vec![] },
            supported_batch_update_compressors: if batch_zstd { vec![ZSTD] } else { vec![] },
            max_batch_total_size_bytes: max_batch,
            splice_blob_support: cdc,
            fast_cdc_2020_params: cdc.then_some(reapi::FastCdc2020Params {
                avg_chunk_size_bytes: 1024,
                seed: 666,
            }),
            ..Default::default()
        };
        fixture.0.lock().unwrap().mode = mode;
        fixture.0.lock().unwrap().allow_no_auth = mode == "no-auth";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = tokio::sync::oneshot::channel();
        let service = fixture.clone();
        let response_encoding = CompressionEncoding::Gzip;
        let task = tokio::spawn(async move {
            let cas = ContentAddressableStorageServer::new(service.clone())
                .send_compressed(response_encoding);
            let cas = if mode == "identity-cas" {
                cas
            } else {
                cas.accept_compressed(CompressionEncoding::Gzip)
            };
            tonic::transport::Server::builder()
                .add_service(
                    CapabilitiesServer::new(service.clone())
                        .send_compressed(response_encoding)
                        .accept_compressed(CompressionEncoding::Gzip),
                )
                .add_service(cas)
                .add_service(
                    ByteStreamServer::new(service)
                        .send_compressed(response_encoding)
                        .accept_compressed(CompressionEncoding::Gzip),
                )
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = rx.await;
                    },
                )
                .await
                .unwrap();
        });
        let client = CasClient::connect(CasConfig {
            endpoint: endpoint.clone(),
            instance_name: "test/instance".into(),
            policy: Policy::default(),
            headers: vec![("x-buildbuddy-api-key".into(), "fixture-secret".into())],
        })
        .await
        .unwrap();
        Running {
            endpoint,
            fixture,
            client,
            shutdown: Some(tx),
            task,
        }
    }
    async fn decode_upload(data: Vec<u8>, compressed: bool) -> Vec<u8> {
        if !compressed {
            return data;
        }
        let mut decoder = async_compression::tokio::bufread::ZstdDecoder::new(data.as_slice());
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).await.unwrap();
        decoded
    }
    fn blob(dir: &std::path::Path, name: &str, data: &[u8]) -> UploadBlob {
        let path = dir.join(name);
        std::fs::write(&path, data).unwrap();
        UploadBlob {
            digest: Digest {
                hash: blake3::hash(data).to_hex().to_string(),
                size_bytes: data.len() as i64,
            },
            path,
        }
    }
    fn deterministic_data(size: usize) -> Vec<u8> {
        let mut x = 0x12345678u32;
        (0..size)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                x as u8
            })
            .collect()
    }
    #[tokio::test]
    async fn transient_presence_retry_and_partial_batch_do_not_reupload_successes() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = [
            blob(dir.path(), "first", &[1; 11]),
            blob(dir.path(), "second", &[2; 17]),
        ];
        let running = server(false, 0, "transient-missing").await;
        let missing = running
            .client
            .find_missing(&blobs.iter().map(|b| b.digest.clone()).collect::<Vec<_>>())
            .await
            .unwrap();
        assert_eq!(missing.len(), 2);
        assert_eq!(running.fixture.0.lock().unwrap().missing_sizes, [2, 2]);
        let running = server(false, 0, "partial-transient-batch").await;
        let stats = running.client.upload_missing(&blobs).await.unwrap();
        assert_eq!(stats.uploaded_blobs, 2);
        assert_eq!(stats.batch_update_calls, 2);
        assert_eq!(
            running.fixture.0.lock().unwrap().batch_plain_sizes,
            [28, 11]
        );
        assert!(blobs.iter().all(|blob| running.contains(&blob.digest)));
    }
    #[tokio::test]
    async fn capabilities_and_immutable_stream_recover_from_transient_failure() {
        let running = server(false, 0, "transient-capabilities").await;
        assert_eq!(running.fixture.0.lock().unwrap().capabilities_calls, 2);
        let dir = tempfile::tempdir().unwrap();
        let b = blob(dir.path(), "retry-stream", &[3; 100]);
        let running = compressed_server(false, 1, "transient-write", false, true).await;
        let stats = running
            .client
            .upload_missing(std::slice::from_ref(&b))
            .await
            .unwrap();
        assert_eq!(stats.bytestream_write_calls, 2);
        assert_eq!(stats.uploaded_blobs, 1);
        assert_eq!(stats.uploaded_bytes, b.digest.size_bytes as u64);
        assert_eq!(
            stats.zstd_stream_blobs, 2,
            "compression counts request attempts"
        );
        let state = running.fixture.0.lock().unwrap();
        assert_eq!(state.write_calls, 2);
        let first_attempt_wire = state.frames[0] as u64;
        let all_received_wire = state.frames.iter().sum::<usize>() as u64;
        assert!(first_attempt_wire > 0);
        assert_eq!(stats.uploaded_wire_bytes, all_received_wire);
        assert_eq!(stats.uploaded_wire_bytes, first_attempt_wire * 2);
        drop(state);
        assert_eq!(
            running.client.stats().uploaded_wire_bytes,
            stats.uploaded_wire_bytes
        );
        assert_eq!(running.client.stats().zstd_stream_blobs, 2);
        assert!(running.contains(&b.digest));
    }
    #[tokio::test]
    async fn stream_ack_is_not_cut_off_by_unary_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let b = blob(dir.path(), "delayed-stream", &[1; 100]);
        let mut running = server(false, 1, "delayed-write-ack").await;
        running.client.policy.rpc_timeout_ms = 20;
        running.client.policy.stream_idle_timeout_ms = 500;
        running.client.policy.operation_timeout_ms = 1000;
        let stats = running.client.upload_missing(&[b]).await.unwrap();
        assert_eq!(stats.bytestream_write_calls, 1);
        assert_eq!(stats.uploaded_blobs, 1);
    }
    #[tokio::test]
    async fn grpc_batches_missing_and_uploads_by_payload_boundaries() {
        let running = server(false, 0, "").await;
        let digests: Vec<_> = (0..1001)
            .map(|n| Digest {
                hash: format!("{n:064x}"),
                size_bytes: n,
            })
            .collect();
        assert_eq!(
            running.client.find_missing(&digests).await.unwrap().len(),
            1001
        );
        assert_eq!(running.fixture.0.lock().unwrap().missing_sizes, [1000, 1]);
        let dir = tempfile::tempdir().unwrap();
        let blobs = vec![
            blob(dir.path(), "a", &vec![1; BATCH_BYTES / 2]),
            blob(dir.path(), "b", &vec![2; BATCH_BYTES / 2]),
            blob(dir.path(), "c", &[3]),
        ];
        let stats = running.client.upload_missing(&blobs).await.unwrap();
        assert_eq!(stats.uploaded_blobs, 3);
        assert_eq!(stats.batch_update_calls, 2);
        assert_eq!(
            running.fixture.0.lock().unwrap().batch_sizes,
            [BATCH_BYTES, 1]
        );
        assert_eq!(
            running.missing_batch_sizes(),
            [1000, 1],
            "uploads must not repeat the caller's FindMissing"
        );
    }
    #[tokio::test]
    async fn grpc_streams_large_blobs_and_honors_smaller_server_batch_limit() {
        let dir = tempfile::tempdir().unwrap();
        let large = blob(dir.path(), "large", &deterministic_data(BATCH_BYTES + 1));
        let running = server(false, 0, "").await;
        let stats = running
            .client
            .upload_missing(std::slice::from_ref(&large))
            .await
            .unwrap();
        assert_eq!(stats.bytestream_write_calls, 1);
        assert!(
            running
                .fixture
                .0
                .lock()
                .unwrap()
                .frames
                .iter()
                .all(|n| *n <= STREAM_BYTES)
        );
        let small_limit = server(false, 10, "").await;
        let stats = small_limit
            .client
            .upload_missing(&[blob(dir.path(), "eleven", &[7; 11])])
            .await
            .unwrap();
        assert_eq!(stats.bytestream_write_calls, 1);
        let short = server(false, 0, "short-write").await;
        assert!(
            short
                .client
                .upload_missing(&[large])
                .await
                .unwrap_err()
                .to_string()
                .contains("committed_size")
        );
    }
    #[tokio::test]
    async fn grpc_rejects_unexpected_missing_and_incomplete_or_failed_batch_responses() {
        let dir = tempfile::tempdir().unwrap();
        let b = blob(dir.path(), "one", &[1]);
        let extra = server(false, 0, "extra-missing").await;
        assert!(
            extra
                .client
                .find_missing(std::slice::from_ref(&b.digest))
                .await
                .is_err()
        );
        for mode in ["omit-batch", "duplicate-batch", "batch-error"] {
            let running = server(false, 0, mode).await;
            assert!(
                running
                    .client
                    .upload_missing(std::slice::from_ref(&b))
                    .await
                    .is_err(),
                "{mode}"
            );
        }
    }
    #[tokio::test]
    async fn grpc_cdc_negotiates_seed_reuses_chunks_and_confirms_splice() {
        let dir = tempfile::tempdir().unwrap();
        let b = blob(dir.path(), "cdc", &deterministic_data(50_000));
        let running = server(true, 0, "").await;
        let stats = running
            .client
            .upload_missing(std::slice::from_ref(&b))
            .await
            .unwrap();
        assert_eq!(stats.cdc_blobs, 1);
        assert!(stats.cdc_chunks > 1);
        assert_eq!(stats.splice_calls, 1);
        let old_bytes = stats.uploaded_bytes;
        assert_eq!(old_bytes, b.digest.size_bytes as u64);
        let again = running
            .client
            .upload_missing(std::slice::from_ref(&b))
            .await
            .unwrap();
        assert_eq!(again.uploaded_bytes, 0);
        let s = running.fixture.0.lock().unwrap();
        assert!(s.chunk_headers >= 3);
        assert_eq!(s.blobs[&key(&b.digest)].len(), 50_000);
    }
    #[tokio::test]
    async fn grpc_cdc_falls_back_only_when_unsupported_and_bounds_eviction_retry() {
        let dir = tempfile::tempdir().unwrap();
        let b = blob(dir.path(), "cdc", &deterministic_data(50_000));
        let chunk_find = server(true, 0, "chunk-find-unimplemented").await;
        let stats = chunk_find
            .client
            .upload_missing(std::slice::from_ref(&b))
            .await
            .unwrap();
        assert_eq!(stats.cdc_fallbacks, 1);
        assert_eq!(stats.bytestream_write_calls, 1);
        let unsupported = server(true, 0, "splice-unimplemented").await;
        let stats = unsupported
            .client
            .upload_missing(std::slice::from_ref(&b))
            .await
            .unwrap();
        assert_eq!(stats.cdc_fallbacks, 1);
        assert_eq!(stats.bytestream_write_calls, 1);
        let evicted = server(true, 0, "evict-chunk").await;
        let stats = evicted
            .client
            .upload_missing(std::slice::from_ref(&b))
            .await
            .unwrap();
        assert_eq!(stats.splice_calls, 2);
        assert_eq!(evicted.fixture.0.lock().unwrap().missing_sizes.len(), 2);
        for mode in ["splice-denied", "wrong-splice"] {
            let bad = server(true, 0, mode).await;
            let error = bad
                .client
                .upload_missing(std::slice::from_ref(&b))
                .await
                .unwrap_err();
            assert!(!error.to_string().contains("fixture-secret"));
            assert!(bad.fixture.0.lock().unwrap().resources.is_empty());
        }
    }
    #[tokio::test]
    async fn grpc_does_not_upload_changed_spool_or_use_invalid_chunk_parameters() {
        let dir = tempfile::tempdir().unwrap();
        let b = blob(dir.path(), "changed", &vec![1; BATCH_BYTES + 1]);
        std::fs::write(&b.path, vec![2; BATCH_BYTES + 1]).unwrap();
        for compressed in [false, true] {
            let running = compressed_server(false, 0, "", false, compressed).await;
            assert!(
                running
                    .client
                    .upload_missing(std::slice::from_ref(&b))
                    .await
                    .is_err()
            );
            assert!(running.fixture.0.lock().unwrap().blobs.is_empty());
        }
        let good = blob(dir.path(), "good", &deterministic_data(50_000));
        let mut cdc = server(true, 0, "").await;
        cdc.client
            .caps
            .fast_cdc_2020_params
            .as_mut()
            .unwrap()
            .avg_chunk_size_bytes = 8;
        assert!(cdc.client.cdc_params(40 * 1024 * 1024).is_none());
        cdc.client
            .caps
            .fast_cdc_2020_params
            .as_mut()
            .unwrap()
            .avg_chunk_size_bytes = 8 * 1024 * 1024;
        assert!(cdc.client.cdc_params(40 * 1024 * 1024).is_none());
        let stats = cdc.client.upload_missing(&[good]).await.unwrap();
        assert_eq!(stats.cdc_blobs, 0);
        assert_eq!(cdc.fixture.0.lock().unwrap().splice_calls, 0);
    }
    #[tokio::test]
    async fn grpc_deadline_bounds_a_stalled_upload_and_does_not_confirm_presence() {
        let dir = tempfile::tempdir().unwrap();
        let blob = blob(dir.path(), "one", &[1]);
        let mut running = server(false, 0, "hang-batch").await;
        running.client.policy.rpc_timeout_ms = 30;
        running.client.policy.operation_timeout_ms = 30;
        running.client.policy.stream_idle_timeout_ms = 30;
        let started = std::time::Instant::now();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            running.client.upload_missing(std::slice::from_ref(&blob)),
        )
        .await
        .expect("RPC deadline must bound stalled upload")
        .unwrap_err();
        let elapsed = started.elapsed();
        let rpc = error
            .downcast_ref::<RpcError>()
            .expect("structured RPC failure");
        // The server's propagated deadline and our outer deadline may win the
        // race; tonic reports server-side deadline cancellation as Cancelled.
        assert!(
            matches!(rpc.code, Code::DeadlineExceeded | Code::Cancelled),
            "{error}"
        );
        assert!(
            elapsed >= Duration::from_millis(15),
            "upload failed before its deadline: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "upload exceeded the deadline failsafe: {elapsed:?}"
        );
        assert!(!running.contains(&blob.digest));
        assert_eq!(running.client.stats().uploaded_blobs, 0);
    }
    #[tokio::test]
    async fn grpc_metadata_retries_explicit_cross_service_codec_rejection_as_identity() {
        let dir = tempfile::tempdir().unwrap();
        let b = blob(dir.path(), "identity-cas", &deterministic_data(50_000));
        let running = compressed_server(true, 0, "identity-cas", false, true).await;
        assert_eq!(
            running.client.grpc_compression.request_encoding(),
            Some(CompressionEncoding::Gzip)
        );
        assert_eq!(
            running
                .client
                .find_missing(std::slice::from_ref(&b.digest))
                .await
                .unwrap(),
            std::slice::from_ref(&b.digest)
        );
        assert_eq!(running.client.grpc_compression.request_encoding(), None);
        let stats = running
            .client
            .upload_missing(std::slice::from_ref(&b))
            .await
            .unwrap();
        assert_eq!(stats.cdc_blobs, 1);
        assert!(running.contains(&b.digest));
        // Exercise identity BatchUpdate as the first CAS request too.
        let running = compressed_server(false, 0, "identity-cas", false, false).await;
        assert_eq!(
            running
                .client
                .upload_missing(&[b])
                .await
                .unwrap()
                .uploaded_blobs,
            1
        );
        assert_eq!(running.client.grpc_compression.request_encoding(), None);
    }
    #[tokio::test]
    async fn grpc_metadata_does_not_retry_application_unimplemented_or_authentication_failures() {
        let d = Digest {
            hash: "a".repeat(64),
            size_bytes: 1,
        };
        for mode in [
            "metadata-unimplemented",
            "metadata-unauthenticated",
            "metadata-denied",
        ] {
            let running = server(false, 0, mode).await;
            let error = running
                .client
                .find_missing(std::slice::from_ref(&d))
                .await
                .unwrap_err();
            assert!(!error.to_string().contains("fixture-secret"));
            assert_eq!(
                running.fixture.0.lock().unwrap().missing_sizes,
                [1],
                "{mode}"
            );
            assert_eq!(
                running.client.grpc_compression.request_encoding(),
                Some(CompressionEncoding::Gzip)
            );
        }
    }
    #[tokio::test]
    async fn grpc_gzip_unary_responses_work_for_capabilities_presence_upload_and_splice() {
        let dir = tempfile::tempdir().unwrap();
        let b = blob(dir.path(), "unary-zstd", &deterministic_data(50_000));
        let running = compressed_server(true, 0, "grpc-gzip", true, true).await;
        assert_eq!(
            running
                .client
                .find_missing(std::slice::from_ref(&b.digest))
                .await
                .unwrap(),
            std::slice::from_ref(&b.digest)
        );
        let stats = running
            .client
            .upload_missing(std::slice::from_ref(&b))
            .await
            .unwrap();
        assert_eq!(stats.cdc_blobs, 1);
        assert!(running.contains(&b.digest));
    }
    #[tokio::test]
    async fn grpc_zstd_negotiates_batch_and_stream_independently_and_keeps_plain_budget() {
        let dir = tempfile::tempdir().unwrap();
        for (batch_zstd, stream_zstd) in [(true, false), (false, true), (true, true)] {
            let running = compressed_server(false, 128, "", batch_zstd, stream_zstd).await;
            let blobs = vec![
                blob(dir.path(), "a", &[1; 100]),
                blob(dir.path(), "b", &[2; 100]),
                blob(dir.path(), "large", &deterministic_data(STREAM_BYTES * 3)),
            ];
            let stats = running.client.upload_missing(&blobs).await.unwrap();
            assert_eq!(
                stats.batch_update_calls, 2,
                "compression must not relax plaintext budget"
            );
            assert_eq!(stats.bytestream_write_calls, 1);
            assert_eq!(stats.zstd_batch_blobs, if batch_zstd { 2 } else { 0 });
            assert_eq!(stats.zstd_stream_blobs, u64::from(stream_zstd));
            assert_eq!(stats.uploaded_bytes, (200 + STREAM_BYTES * 3) as u64);
            let s = running.fixture.0.lock().unwrap();
            assert_eq!(
                s.batch_compressors,
                vec![if batch_zstd { ZSTD } else { 0 }; 2]
            );
            assert!(s.batch_plain_sizes.iter().all(|n| *n <= 128));
            if batch_zstd {
                assert!(
                    s.batch_sizes
                        .iter()
                        .zip(&s.batch_plain_sizes)
                        .all(|(wire, plain)| wire < plain)
                );
            }
            assert_eq!(
                s.resources[0].contains("/compressed-blobs/zstd/"),
                stream_zstd
            );
            for b in &blobs {
                assert!(s.blobs.contains_key(&key(&b.digest)));
            }
        }
    }
    #[tokio::test]
    async fn grpc_zstd_stream_accepts_compressed_commit_and_early_existing_but_rejects_wrong_size()
    {
        let dir = tempfile::tempdir().unwrap();
        let b = blob(dir.path(), "large", &vec![7; BATCH_BYTES * 2]);
        let early = blob(
            dir.path(),
            "early-large",
            &deterministic_data(BATCH_BYTES * 2),
        );
        for (mode, compressed) in [
            ("", true),
            ("stream-minus-one", true),
            ("logical-compressed-size", true),
            ("early-complete", true),
            ("early-complete", false),
        ] {
            let running = compressed_server(false, 1, mode, false, compressed).await;
            let stats =
                tokio::time::timeout(
                    Duration::from_secs(2),
                    running.client.upload_missing(std::slice::from_ref(
                        if mode == "early-complete" { &early } else { &b },
                    )),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stats.uploaded_blobs, 1);
            assert_eq!(stats.uploaded_bytes, b.digest.size_bytes as u64);
            assert_eq!(stats.zstd_stream_blobs, u64::from(compressed));
            if mode.is_empty() {
                assert!(stats.uploaded_wire_bytes < stats.uploaded_bytes / 100);
            }
        }
        for mode in [
            "wrong-compressed-size",
            "negative-compressed-size",
            "short-write",
        ] {
            let running = compressed_server(false, 1, mode, false, true).await;
            assert!(
                running
                    .client
                    .upload_missing(std::slice::from_ref(&b))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("committed_size"),
                "{mode}"
            );
        }
        // Only the dedup marker may end a compressed upload before the verified
        // producer completes. Neither logical size nor a partial wire count is
        // evidence that the unread portion of the spool was valid.
        for mode in ["early-logical-size", "early-wire-size"] {
            let running = compressed_server(false, 1, mode, false, true).await;
            assert!(
                tokio::time::timeout(
                    Duration::from_secs(2),
                    running.client.upload_missing(std::slice::from_ref(&early)),
                )
                .await
                .expect("early non-dedup response must not hang the producer")
                .is_err(),
                "{mode}"
            );
        }
    }
    #[tokio::test]
    async fn grpc_zstd_cdc_chunks_use_batch_or_stream_and_compress_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let b = blob(dir.path(), "cdc-compressed", &deterministic_data(50_000));
        for limit in [128, BATCH_BYTES as i64] {
            let running = compressed_server(true, limit, "", true, true).await;
            let stats = running
                .client
                .upload_missing(std::slice::from_ref(&b))
                .await
                .unwrap();
            assert_eq!(stats.cdc_blobs, 1);
            assert!(running.contains(&b.digest));
            let s = running.fixture.0.lock().unwrap();
            if limit == 128 {
                assert!(stats.bytestream_write_calls > 0);
                assert!(
                    s.resources
                        .iter()
                        .all(|r| r.contains("/compressed-blobs/zstd/"))
                );
            } else {
                assert!(stats.batch_update_calls > 0);
                assert!(s.batch_compressors.iter().all(|c| *c == ZSTD));
            }
        }
        let running = compressed_server(true, 0, "splice-unimplemented", true, true).await;
        let stats = running.client.upload_missing(&[b]).await.unwrap();
        assert_eq!(stats.cdc_fallbacks, 1);
        assert!(
            running
                .fixture
                .0
                .lock()
                .unwrap()
                .resources
                .iter()
                .all(|r| r.contains("/compressed-blobs/zstd/"))
        );
    }
    #[tokio::test]
    async fn grpc_zstd_stream_deadline_cancels_bounded_producer_without_confirming_presence() {
        let dir = tempfile::tempdir().unwrap();
        let b = blob(
            dir.path(),
            "stalled-stream",
            &deterministic_data(BATCH_BYTES * 2),
        );
        let mut running = compressed_server(false, 1, "hang-write", false, true).await;
        running.client.policy.rpc_timeout_ms = 30;
        running.client.policy.operation_timeout_ms = 1000;
        running.client.policy.retry.max_attempts = 1;
        running.client.policy.stream_idle_timeout_ms = 30;
        let result =
            tokio::time::timeout(Duration::from_secs(2), running.client.upload_missing(&[b]))
                .await
                .unwrap();
        assert!(result.is_err());
        assert_eq!(running.client.stats().uploaded_blobs, 0);
    }
    #[test]
    fn fastcdc_matches_official_zero_and_nonzero_seed_vectors() {
        let image = include_bytes!("../testdata/SekienAkashita.jpg");
        let text = include_str!("../testdata/fastcdc2020_test_vectors.txt");
        let mut seed = 0;
        let mut expected: HashMap<u64, Vec<(usize, usize, u64)>> = HashMap::new();
        for line in text.lines() {
            if let Some(n) = line.strip_prefix("# Seed: ") {
                seed = n.parse().unwrap();
            }
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let fields: Vec<_> = line.split_whitespace().collect();
            expected.entry(seed).or_default().push((
                fields[0].parse().unwrap(),
                fields[1].parse().unwrap(),
                fields[3].parse().unwrap(),
            ));
        }
        for seed in [0, 666] {
            let actual: Vec<_> = fastcdc::v2020::FastCDC::with_level_and_seed(
                image,
                4096,
                16384,
                65535,
                fastcdc::v2020::Normalization::Level2,
                seed,
            )
            .map(|c| (c.offset, c.length, c.hash))
            .collect();
            assert_eq!(actual, expected[&seed], "seed {seed}");
        }
    }
}
