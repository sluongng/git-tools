//! BLAKE3 CAS uploads using the Remote Execution API and ByteStream.
use anyhow::{Context, Result, bail, ensure};
use prost::Message;
use serde::Serialize;
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tonic::{
    Code, Request,
    metadata::{Ascii, MetadataKey, MetadataValue},
    transport::{Channel, ClientTlsConfig, Endpoint},
};

pub use git_reapi::{Digest, proto, reapi};
use proto::google::bytestream as bs;

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
    rpc_timeout: Duration,
    stats: Arc<Mutex<UploadStats>>,
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

impl CasClient {
    pub async fn connect(config: CasConfig) -> Result<Self> {
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
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60));
        if https {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .context("configure CAS TLS")?;
        }
        let channel = endpoint.connect().await.context("connect CAS")?;
        let mut client = Self {
            channel,
            instance: config.instance_name,
            headers: Arc::new(headers),
            caps: Default::default(),
            rpc_timeout: Duration::from_secs(60),
            stats: Arc::new(Mutex::new(Default::default())),
        };
        let response = bounded_rpc(
            client.rpc_timeout,
            reapi::capabilities_client::CapabilitiesClient::new(client.channel.clone())
                .get_capabilities(client.request(
                    reapi::GetCapabilitiesRequest {
                        instance_name: client.instance.clone(),
                    },
                    false,
                )),
        )
        .await
        .map_err(|s| rpc_error("GetCapabilities", s))?
        .into_inner();
        client.caps = response
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
        req.set_timeout(self.rpc_timeout);
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
        .max_encoding_message_size(GRPC_BYTES)
        .max_decoding_message_size(GRPC_BYTES)
    }
    /// Validate each response against its request. Never infer presence from an incomplete upload response.
    pub async fn find_missing(&self, digests: &[Digest]) -> Result<Vec<Digest>> {
        self.find_missing_inner(digests, false).await
    }
    async fn find_missing_inner(&self, digests: &[Digest], chunk: bool) -> Result<Vec<Digest>> {
        for d in digests {
            validate_digest(d)?;
        }
        let mut missing = Vec::new();
        for batch in digests.chunks(FIND_MISSING_LIMIT) {
            let allowed: HashSet<_> = batch.iter().map(key).collect();
            self.record(&UploadStats {
                find_missing_calls: 1,
                ..Default::default()
            });
            let response = bounded_rpc(
                self.rpc_timeout,
                self.cas().find_missing_blobs(self.request(
                    reapi::FindMissingBlobsRequest {
                        instance_name: self.instance.clone(),
                        blob_digests: batch.to_vec(),
                        digest_function: BLAKE3,
                    },
                    chunk,
                )),
            )
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
        let mut stats = UploadStats::default();
        let mut pending = Vec::new();
        let mut pending_bytes = 0;
        let mut seen = HashSet::new();
        let limit = self.batch_limit();
        for blob in blobs {
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
                self.upload_cdc(blob, params, &mut stats).await?;
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
        let request = reapi::batch_update_blobs_request::Request {
            digest: Some(part.digest),
            data,
            compressor: 0,
        };
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
        let requests = std::mem::take(pending);
        let expected: HashSet<_> = requests
            .iter()
            .map(|r| key(r.digest.as_ref().unwrap()))
            .collect();
        let bytes: u64 = requests.iter().map(|r| r.data.len() as u64).sum();
        let count = requests.len() as u64;
        let started = UploadStats {
            batch_update_calls: 1,
            ..Default::default()
        };
        stats.add(&started);
        self.record(&started);
        let response = bounded_rpc(
            self.rpc_timeout,
            self.cas().batch_update_blobs(self.request(
                reapi::BatchUpdateBlobsRequest {
                    instance_name: self.instance.clone(),
                    requests,
                    digest_function: BLAKE3,
                },
                chunk,
            )),
        )
        .await
        .map_err(|s| rpc_error("BatchUpdateBlobs", s))?
        .into_inner();
        let mut seen = HashSet::new();
        for r in response.responses {
            let digest = r.digest.context("CAS omitted batch response digest")?;
            ensure!(
                expected.contains(&key(&digest)) && seen.insert(key(&digest)),
                "CAS returned unexpected or duplicate batch response"
            );
            let status = r.status.context("CAS omitted batch response status")?;
            ensure!(
                status.code == 0,
                "CAS batch upload failed with code {}",
                status.code
            );
        }
        ensure!(seen == expected, "CAS omitted batch upload responses");
        let complete = UploadStats {
            uploaded_bytes: bytes,
            uploaded_blobs: count,
            ..Default::default()
        };
        stats.add(&complete);
        self.record(&complete);
        Ok(())
    }
    async fn stream_part(&self, part: &Part, chunk: bool, stats: &mut UploadStats) -> Result<()> {
        let upload_id = uuid::Uuid::new_v4().to_string();
        let prefix = if self.instance.is_empty() {
            String::new()
        } else {
            format!("{}/", self.instance)
        };
        let resource = format!(
            "{prefix}uploads/{upload_id}/blobs/blake3/{}/{}",
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
            let mut remaining = part.digest.size_bytes as usize;
            let mut offset = 0i64;
            let mut hasher = blake3::Hasher::new();
            loop {
                let mut data = vec![0; remaining.min(STREAM_BYTES)];
                file.read_exact(&mut data).await.context("read CAS spool")?;
                hasher.update(&data);
                remaining -= data.len();
                if remaining == 0 {
                    ensure!(
                        hasher.finalize().to_hex().as_str() == part.digest.hash,
                        "CAS spool content does not match digest"
                    );
                }
                let len = data.len() as i64;
                tx.send(bs::WriteRequest {
                    resource_name: if offset == 0 {
                        resource.clone()
                    } else {
                        String::new()
                    },
                    write_offset: offset,
                    finish_write: remaining == 0,
                    data,
                })
                .await
                .map_err(|_| anyhow::anyhow!("CAS stopped consuming upload"))?;
                offset += len;
                if remaining == 0 {
                    return Ok::<_, anyhow::Error>(());
                }
            }
        }));
        let started = UploadStats {
            bytestream_write_calls: 1,
            ..Default::default()
        };
        stats.add(&started);
        self.record(&started);
        let response = bounded_rpc(
            self.rpc_timeout,
            bs::byte_stream_client::ByteStreamClient::new(self.channel.clone())
                .write(self.request(tokio_stream::wrappers::ReceiverStream::new(rx), chunk)),
        )
        .await;
        let produced = tokio::time::timeout(Duration::from_secs(60), &mut producer.0)
            .await
            .context("CAS upload reader timed out")?
            .context("CAS upload reader task failed")?;
        let response = response
            .map_err(|s| rpc_error("ByteStream.Write", s))?
            .into_inner();
        produced?;
        ensure!(
            response.committed_size == expected_size,
            "CAS ByteStream committed_size mismatch"
        );
        let done = UploadStats {
            uploaded_bytes: expected_size as u64,
            uploaded_blobs: 1,
            ..Default::default()
        };
        stats.add(&done);
        self.record(&done);
        Ok(())
    }
    async fn upload_cdc(
        &self,
        blob: &UploadBlob,
        params: reapi::FastCdc2020Params,
        stats: &mut UploadStats,
    ) -> Result<()> {
        match self.upload_cdc_inner(blob, params, stats).await {
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
    ) -> Result<()> {
        let source = blob.clone();
        let parts = tokio::task::spawn_blocking(move || chunk_file(source, params))
            .await
            .context("CAS chunk task failed")??;
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
            let response = bounded_rpc(
                self.rpc_timeout,
                self.cas().splice_blob(self.request(request.clone(), false)),
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
// Endpoint timeouts need not cover unary response-body decoding. Bound the entire
// generated RPC future as well, and propagate a deadline to the server.
async fn bounded_rpc<T>(
    timeout: Duration,
    future: impl std::future::Future<Output = std::result::Result<tonic::Response<T>, tonic::Status>>,
) -> std::result::Result<tonic::Response<T>, tonic::Status> {
    tokio::time::timeout(timeout, future)
        .await
        .unwrap_or_else(|_| Err(tonic::Status::deadline_exceeded("CAS RPC timed out")))
}
#[derive(Debug)]
struct CdcLimit;
impl std::fmt::Display for CdcLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CAS chunk mapping exceeds unary client limit")
    }
}
impl std::error::Error for CdcLimit {}
#[derive(Debug)]
struct RpcError {
    method: String,
    code: Code,
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
fn chunk_file(blob: UploadBlob, params: reapi::FastCdc2020Params) -> Result<Vec<Part>> {
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
    use super::*;
    use bs::byte_stream_server::{ByteStream, ByteStreamServer};
    use reapi::{
        capabilities_server::{Capabilities, CapabilitiesServer},
        content_addressable_storage_server::{
            ContentAddressableStorage, ContentAddressableStorageServer,
        },
    };
    use std::collections::HashMap;
    use tonic::{Response, Status};

    #[derive(Default)]
    struct State {
        caps: reapi::CacheCapabilities,
        blobs: HashMap<(String, i64), Vec<u8>>,
        missing_sizes: Vec<usize>,
        batch_sizes: Vec<usize>,
        frames: Vec<usize>,
        resources: Vec<String>,
        chunk_headers: usize,
        splice_calls: usize,
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
            Ok(Response::new(reapi::ServerCapabilities {
                cache_capabilities: Some(self.0.lock().unwrap().caps.clone()),
                high_api_version: Some(proto::build::bazel::semver::SemVer {
                    major: 2,
                    minor: 11,
                    ..Default::default()
                }),
                ..Default::default()
            }))
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
            let mut s = self.0.lock().unwrap();
            s.batch_sizes
                .push(req.requests.iter().map(|r| r.data.len()).sum());
            let mut responses = Vec::new();
            for r in req.requests {
                let d = r.digest.unwrap();
                assert_eq!(d.hash, blake3::hash(&r.data).to_hex().to_string());
                assert_eq!(d.size_bytes, r.data.len() as i64);
                assert_eq!(r.compressor, 0);
                s.blobs.insert(key(&d), r.data);
                responses.push(reapi::batch_update_blobs_response::Response {
                    digest: Some(d),
                    status: Some(proto::google::rpc::Status {
                        code: if s.mode == "batch-error" { 7 } else { 0 },
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
                self.0.lock().unwrap().frames.push(r.data.len());
                data.extend_from_slice(&r.data);
                finished = r.finish_write;
            }
            if !finished {
                return Err(Status::invalid_argument("unfinished write"));
            }
            assert!(resource.starts_with("test/instance/uploads/"));
            assert!(resource.contains("/blobs/blake3/"));
            let d = Digest {
                hash: blake3::hash(&data).to_hex().to_string(),
                size_bytes: data.len() as i64,
            };
            assert!(resource.ends_with(&format!("/{}/{}", d.hash, d.size_bytes)));
            let mut s = self.0.lock().unwrap();
            s.resources.push(resource);
            s.blobs.insert(key(&d), data);
            Ok(Response::new(bs::WriteResponse {
                committed_size: d.size_bytes - if s.mode == "short-write" { 1 } else { 0 },
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
        let fixture = Fixture::default();
        fixture.0.lock().unwrap().caps = reapi::CacheCapabilities {
            digest_functions: vec![BLAKE3],
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
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(CapabilitiesServer::new(service.clone()))
                .add_service(ContentAddressableStorageServer::new(service.clone()))
                .add_service(ByteStreamServer::new(service))
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
        let running = server(false, 0, "").await;
        assert!(
            running
                .client
                .upload_missing(std::slice::from_ref(&b))
                .await
                .is_err()
        );
        assert!(running.fixture.0.lock().unwrap().blobs.is_empty());
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
        running.client.rpc_timeout = Duration::from_millis(30);
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
