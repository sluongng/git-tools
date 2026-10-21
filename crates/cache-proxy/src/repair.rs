//! Bounded, replay-safe publication of already verified resident CAS content.
use super::*;
use std::{path::Path, sync::atomic::AtomicBool};

impl Upstream {
    pub(super) async fn repair_blob(
        &self,
        artifact: &Artifact,
        path: &Path,
        deadline: tokio::time::Instant,
    ) -> Result<(), Status> {
        // Do not initialize the foreground capability cell: background discovery
        // must not make foreground reads wait for its connection or RPC.
        let compressed =
            capabilities_client::CapabilitiesClient::new(self.maintenance_channel.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .get_capabilities(self.request(GetCapabilitiesRequest {
                    instance_name: self.config.instance_name.clone(),
                }))
                .await
                .ok()
                .and_then(|r| r.into_inner().cache_capabilities)
                .is_some_and(|c| c.supported_compressors.contains(&1));
        self.config
            .policy
            .retry(
                deadline,
                self.config.policy.rpc_timeout(),
                |budget| async move {
                    let result = self
                        .repair_attempt(artifact, path, compressed, budget)
                        .await;
                    match result {
                        Err(ref s)
                            if compressed
                                && matches!(
                                    s.code(),
                                    tonic::Code::Unimplemented | tonic::Code::InvalidArgument
                                ) =>
                        {
                            // Codec fallback shares this attempt's remaining budget.
                            let remaining = deadline
                                .saturating_duration_since(tokio::time::Instant::now())
                                .min(budget);
                            self.repair_attempt(artifact, path, false, remaining).await
                        }
                        result => result,
                    }
                },
            )
            .await
    }
    async fn repair_attempt(
        &self,
        artifact: &Artifact,
        path: &Path,
        compressed: bool,
        budget: Duration,
    ) -> Result<(), Status> {
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|_| Status::data_loss("local repair bytes unavailable"))?;
        let prefix = if artifact.instance.is_empty() {
            String::new()
        } else {
            format!("{}/", artifact.instance)
        };
        let resource = format!(
            "{prefix}uploads/{}/{}/{}{}/{}",
            uuid::Uuid::new_v4(),
            if compressed {
                "compressed-blobs/zstd"
            } else {
                "blobs"
            },
            if artifact.digest_function == 9 {
                "blake3/"
            } else {
                ""
            },
            artifact.hash,
            artifact.size
        );
        let failure = Arc::new(Mutex::new(None));
        let stream_failure = failure.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let stream_finished = finished.clone();
        let wire = Arc::new(AtomicU64::new(0));
        let stream_wire = wire.clone();
        let (progress, mut progress_rx) = tokio::sync::watch::channel(tokio::time::Instant::now());
        let artifact = artifact.clone();
        let expected_size = artifact.size as i64;
        let upload = async_stream::stream! {
            let mut buffer = vec![0; 64 * 1024];
            let mut size = 0u64;
            let mut sha = Sha256::new();
            let mut blake = blake3::Hasher::new();
            loop {
                let n = match file.read(&mut buffer).await {
                    Ok(n) => n,
                    Err(_) => { *stream_failure.lock().unwrap() = Some(Status::data_loss("reading local repair bytes")); break; }
                };
                if n == 0 {
                    let hash = if artifact.digest_function == 9 { blake.finalize().to_hex().to_string() }
                        else { format!("{:x}", sha.finalize()) };
                    if size != artifact.size || hash != artifact.hash {
                        *stream_failure.lock().unwrap() = Some(Status::data_loss("local repair bytes changed")); break;
                    }
                    // A zero-length compressed resource still needs a valid frame.
                    let data = if compressed && size == 0 { reapi::compression::encode_zstd(&[]).unwrap() } else { vec![] };
                    let offset = stream_wire.fetch_add(data.len() as u64, Ordering::Relaxed);
                    stream_finished.store(true, Ordering::Release);
                    progress.send_replace(tokio::time::Instant::now());
                    yield bs::WriteRequest { resource_name: resource.clone(), write_offset: offset as i64, finish_write: true, data };
                    break;
                }
                size += n as u64;
                if size > artifact.size { *stream_failure.lock().unwrap() = Some(Status::data_loss("local repair bytes grew")); break; }
                sha.update(&buffer[..n]); blake.update(&buffer[..n]);
                let data = if compressed { match reapi::compression::encode_zstd(&buffer[..n]) {
                    Ok(data) => data,
                    Err(_) => { *stream_failure.lock().unwrap() = Some(Status::internal("compressing local repair bytes")); break; }
                }} else { buffer[..n].to_vec() };
                let offset = stream_wire.fetch_add(data.len() as u64, Ordering::Relaxed);
                progress.send_replace(tokio::time::Instant::now());
                yield bs::WriteRequest { resource_name: resource.clone(), write_offset: offset as i64, finish_write: false, data };
                tokio::task::yield_now().await;
            }
        };
        let mut client = ByteStreamClient::new(self.maintenance_channel.clone())
            .accept_compressed(CompressionEncoding::Gzip);
        if !compressed && self.send_gzip(RpcMethod::WriteByteStream) {
            client = client.send_compressed(CompressionEncoding::Gzip);
        }
        let mut request = self.request(upload);
        request.set_timeout(budget);
        let rpc = client.write(request);
        tokio::pin!(rpc);
        let mut progress_closed = false;
        let response = loop {
            let idle = *progress_rx.borrow() + self.config.policy.stream_idle_timeout();
            tokio::select! {
                result = &mut rpc => break result,
                _ = tokio::time::sleep_until(idle) => {
                    if tokio::time::Instant::now() >= *progress_rx.borrow() + self.config.policy.stream_idle_timeout() {
                        return Err(Status::deadline_exceeded("repair upload idle timeout"));
                    }
                },
                changed = progress_rx.changed(), if !progress_closed => { if changed.is_err() { progress_closed = true; } }
            }
        };
        if let Some(error) = failure.lock().unwrap().take() {
            return Err(error);
        }
        let response = response?;
        self.observe_gzip(RpcMethod::WriteByteStream, &response);
        let committed = response.into_inner().committed_size;
        if (!compressed && committed == expected_size)
            || (compressed && committed == -1)
            || (finished.load(Ordering::Acquire)
                && reapi::upload_ack::valid_completed_write_ack(
                    compressed,
                    committed,
                    expected_size,
                    wire.load(Ordering::Relaxed),
                ))
        {
            Ok(())
        } else {
            Err(Status::data_loss("repair upload acknowledgment mismatch"))
        }
    }
}
