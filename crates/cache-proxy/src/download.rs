//! Negotiate CAS payload compression separately from gRPC message compression.
use super::*;
use std::io::{Seek, SeekFrom};

struct BoundedWriter<'a> {
    target: &'a mut File,
    size: u64,
    limit: u64,
    diagnostics: &'a FailureDiagnostics,
    digest_function: i32,
    encoding: FailureEncoding,
}
impl Write for BoundedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() as u64 > self.limit.saturating_sub(self.size) {
            let mut diagnostic =
                FailureDiagnostic::new(FailureStage::DownloadSize, FailureClass::SizeMismatch);
            diagnostic.expected_size = Some(self.limit);
            diagnostic.actual_size = Some(self.size.saturating_add(bytes.len() as u64));
            diagnostic.digest_function = Some(self.digest_function);
            diagnostic.encoding = Some(self.encoding);
            self.diagnostics.record(diagnostic);
            return Err(std::io::Error::other("upstream blob exceeds declared size"));
        }
        self.target.write_all(bytes).inspect_err(|error| {
            let mut diagnostic = FailureDiagnostic::from_io(FailureStage::DownloadWrite, error);
            diagnostic.expected_size = Some(self.limit);
            diagnostic.digest_function = Some(self.digest_function);
            diagnostic.encoding = Some(self.encoding);
            self.diagnostics.record(diagnostic);
        })?;
        self.size += bytes.len() as u64;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.target.flush()
    }
}
impl Upstream {
    fn observe_download_failure(
        &self,
        stage: FailureStage,
        error: &anyhow::Error,
        artifact: &Artifact,
        compressed: bool,
        actual_size: Option<u64>,
    ) {
        let mut diagnostic = FailureDiagnostic::from_error(stage, error);
        // Match cache_status's typed-status precedence without retaining its
        // message, details, metadata or source-chain strings.
        if let Some(status) = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<Status>())
        {
            diagnostic.class = FailureClass::GrpcStatus;
            diagnostic.grpc_code = Some(status.code() as i32);
        } else if matches!(stage, FailureStage::DownloadWireSize) {
            diagnostic.class = FailureClass::EncodedSizeLimit;
        } else if matches!(stage, FailureStage::DownloadSize) {
            diagnostic.class = FailureClass::SizeMismatch;
        } else if matches!(diagnostic.class, FailureClass::Unattributed)
            && matches!(
                stage,
                FailureStage::DownloadDecoderStart
                    | FailureStage::DownloadDecode
                    | FailureStage::DownloadDecodeFinish
            )
        {
            diagnostic.class = FailureClass::Decode;
        }
        diagnostic.expected_size = Some(if matches!(stage, FailureStage::DownloadWireSize) {
            artifact.size.saturating_add(1024 * 1024)
        } else {
            artifact.size
        });
        diagnostic.actual_size = actual_size;
        diagnostic.digest_function = Some(artifact.digest_function);
        diagnostic.encoding = Some(if compressed {
            FailureEncoding::Zstd
        } else {
            FailureEncoding::Identity
        });
        self.download_failures.record(diagnostic);
    }
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
                target.set_len(0).inspect_err(|error| {
                    let mut diagnostic =
                        FailureDiagnostic::from_io(FailureStage::DownloadTargetReset, error);
                    diagnostic.digest_function = Some(artifact.digest_function);
                    self.download_failures.record(diagnostic);
                })?;
                target.seek(SeekFrom::Start(0)).inspect_err(|error| {
                    let mut diagnostic =
                        FailureDiagnostic::from_io(FailureStage::DownloadTargetReset, error);
                    diagnostic.digest_function = Some(artifact.digest_function);
                    self.download_failures.record(diagnostic);
                })?;
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
                    let mut stage = FailureStage::DownloadTargetClone;
                    let mut actual_size = None;
                    let result: Result<()> = async {
                        let mut target = target?;
                        // Each retry is a full replay, including fresh decoder state.
                        stage = FailureStage::DownloadTargetReset;
                        target.set_len(0)?;
                        target.seek(SeekFrom::Start(0))?;
                        stage = FailureStage::DownloadHeaders;
                        let mut stream = client.read(request).await?.into_inner();
                        let mut output = BoundedWriter {
                            target: &mut target,
                            size: 0,
                            limit: artifact.size,
                            diagnostics: &self.download_failures,
                            digest_function: artifact.digest_function,
                            encoding: if compressed {
                                FailureEncoding::Zstd
                            } else {
                                FailureEncoding::Identity
                            },
                        };
                        stage = FailureStage::DownloadDecoderStart;
                        let mut decoder = if compressed {
                            Some(reapi::compression::ZstdDecoder::new()?)
                        } else {
                            None
                        };
                        let mut wire_bytes = 0u64;
                        loop {
                            stage = FailureStage::DownloadMessage;
                            let Some(chunk) = tokio::time::timeout(
                                self.config.policy.stream_idle_timeout(),
                                stream.message(),
                            )
                            .await
                            .map_err(|_| {
                                Status::deadline_exceeded("upstream stream idle timeout")
                            })??
                            else {
                                break;
                            };
                            stage = FailureStage::DownloadWireSize;
                            wire_bytes = wire_bytes
                                .checked_add(chunk.data.len() as u64)
                                .context("upstream byte count overflow")?;
                            actual_size = Some(wire_bytes);
                            ensure!(
                                wire_bytes <= artifact.size.saturating_add(1024 * 1024),
                                "upstream encoded blob exceeds declared transfer budget"
                            );
                            if let Some(decoder) = decoder.as_mut() {
                                stage = FailureStage::DownloadDecode;
                                actual_size = None;
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
                                stage = FailureStage::DownloadWrite;
                                actual_size = None;
                                output.write_all(&chunk.data)?;
                            }
                        }
                        if let Some(decoder) = decoder {
                            stage = FailureStage::DownloadDecodeFinish;
                            actual_size = None;
                            decoder.finish()?;
                        }
                        stage = FailureStage::DownloadSize;
                        actual_size = Some(output.size);
                        ensure!(output.size == artifact.size, "truncated upstream blob");
                        Ok(())
                    }
                    .await;
                    if let Err(error) = &result {
                        self.observe_download_failure(
                            stage,
                            error,
                            artifact,
                            compressed,
                            actual_size,
                        );
                    }
                    result.map_err(cache_status)
                }
            })
            .await?;
        Ok(())
    }
}
