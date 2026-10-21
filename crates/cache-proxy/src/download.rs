//! Negotiate CAS payload compression separately from gRPC message compression.
use super::*;
use std::io::{Seek, SeekFrom};

struct BoundedWriter<'a> {
    target: &'a mut File,
    size: u64,
    limit: u64,
}
impl Write for BoundedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() as u64 > self.limit.saturating_sub(self.size) {
            return Err(std::io::Error::other("upstream blob exceeds declared size"));
        }
        self.target.write_all(bytes)?;
        self.size += bytes.len() as u64;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.target.flush()
    }
}
impl Upstream {
    async fn supports_zstd(&self) -> bool {
        if self.zstd_disabled.load(Ordering::Relaxed) {
            return false;
        }
        *self
            .zstd_capability
            .get_or_init(|| async {
                // Capabilities are advisory. A server without this method can still
                // serve the mandatory identity ByteStream resource format.
                self.replay(|| async {
                    capabilities_client::CapabilitiesClient::new(self.channel.clone())
                        .accept_compressed(CompressionEncoding::Gzip)
                        .get_capabilities(self.request(GetCapabilitiesRequest {
                            instance_name: self.config.instance_name.clone(),
                        }))
                        .await
                })
                .await
                .ok()
                .and_then(|response| response.into_inner().cache_capabilities)
                .is_some_and(|capabilities| capabilities.supported_compressors.contains(&1))
            })
            .await
    }
    pub(super) async fn fetch_transport(
        &self,
        artifact: &Artifact,
        target: &mut File,
    ) -> Result<()> {
        ensure!(
            artifact.backend == self.config.backend_identity
                && artifact.instance == self.config.instance_name,
            "CAS scope mismatch"
        );
        let deadline = tokio::time::Instant::now() + self.config.policy.operation_timeout();
        let compressed =
            reapi::policy::timeout(deadline, async { Ok(self.supports_zstd().await) }).await?;
        match self
            .fetch_encoding(artifact, target, compressed, deadline)
            .await
        {
            Err(error)
                if compressed
                    && error.chain().any(|cause| {
                        cause.downcast_ref::<Status>().is_some_and(|status| {
                            matches!(
                                status.code(),
                                tonic::Code::Unimplemented
                                    | tonic::Code::InvalidArgument
                                    | tonic::Code::NotFound
                            )
                        })
                    }) =>
            {
                // Some deployments advertise a codec before every replica can
                // serve its resource names. Identity is mandatory. Do not fall
                // back on auth, transient transport, or integrity failures.
                let not_found = error.chain().any(|cause| {
                    cause
                        .downcast_ref::<Status>()
                        .is_some_and(|status| status.code() == tonic::Code::NotFound)
                });
                target.set_len(0)?;
                target.seek(SeekFrom::Start(0))?;
                let result = self.fetch_encoding(artifact, target, false, deadline).await;
                if result.is_ok() || !not_found {
                    self.zstd_disabled.store(true, Ordering::Relaxed);
                }
                result
            }
            result => result,
        }
    }
    async fn fetch_encoding(
        &self,
        artifact: &Artifact,
        target: &mut File,
        compressed: bool,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        let name = if compressed {
            format!(
                "{}{}compressed-blobs/zstd/{}{}/{}",
                artifact.instance,
                if artifact.instance.is_empty() {
                    ""
                } else {
                    "/"
                },
                if artifact.digest_function == 9 {
                    "blake3/"
                } else {
                    ""
                },
                artifact.hash,
                artifact.size
            )
        } else {
            resource(artifact)
        };
        // Do not negotiate gRPC gzip on already-zstd-compressed resources.
        let client = if compressed {
            ByteStreamClient::new(self.channel.clone())
        } else {
            self.bytes()
        };
        let request = bs::ReadRequest {
            resource_name: name,
            read_offset: 0,
            read_limit: 0,
        };
        self.config
            .policy
            .retry(deadline, self.config.policy.rpc_timeout(), |allowance| {
                let target = target.try_clone();
                let mut client = client.clone();
                let mut request = self.request(request.clone());
                request.set_timeout(allowance);
                async move {
                    let result: Result<()> = async {
                        let mut target = target?;
                        // Each retry is a full replay, including fresh decoder state.
                        target.set_len(0)?;
                        target.seek(SeekFrom::Start(0))?;
                        let mut stream = client.read(request).await?.into_inner();
                        let mut output = BoundedWriter {
                            target: &mut target,
                            size: 0,
                            limit: artifact.size,
                        };
                        let mut decoder = if compressed {
                            Some(reapi::compression::ZstdDecoder::new()?)
                        } else {
                            None
                        };
                        let mut wire_bytes = 0u64;
                        while let Some(chunk) = tokio::time::timeout(
                            self.config.policy.stream_idle_timeout(),
                            stream.message(),
                        )
                        .await
                        .map_err(|_| Status::deadline_exceeded("upstream stream idle timeout"))??
                        {
                            wire_bytes = wire_bytes
                                .checked_add(chunk.data.len() as u64)
                                .context("upstream byte count overflow")?;
                            ensure!(
                                wire_bytes <= artifact.size.saturating_add(1024 * 1024),
                                "upstream encoded blob exceeds declared transfer budget"
                            );
                            if let Some(decoder) = decoder.as_mut() {
                                let mut input = chunk.data.as_slice();
                                let mut drain = false;
                                while !input.is_empty() || drain {
                                    let step = decoder.decode_step(input, &mut output)?;
                                    input = &input[step.consumed..];
                                    drain = step.needs_drain;
                                    // Bound memory and allow operation cancellation between
                                    // fixed-size decoded chunks, including compression bombs.
                                    tokio::task::yield_now().await;
                                }
                            } else {
                                output.write_all(&chunk.data)?;
                            }
                        }
                        if let Some(decoder) = decoder {
                            decoder.finish()?;
                        }
                        ensure!(output.size == artifact.size, "truncated upstream blob");
                        Ok(())
                    }
                    .await;
                    result.map_err(cache_status)
                }
            })
            .await?;
        Ok(())
    }
}
