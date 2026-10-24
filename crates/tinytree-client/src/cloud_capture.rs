//! Portable capture DATA transport. Framing/hash checks are not canonical
//! manifest coverage, a live frontend attestation, or restore authorization.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CloudCaptureId(pub [u8; 32]);
impl CloudCaptureId {
    pub fn of(bytes: &[u8]) -> Self {
        Self(tinytree_model::cloud_capture_data_id(bytes))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloudCaptureDescriptor {
    pub id: CloudCaptureId,
    pub selected: CloudRawDigest,
}
impl CloudCaptureDescriptor {
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            id: CloudCaptureId::of(bytes),
            selected: CloudRawDigest::of(bytes),
        }
    }
}
pub(super) fn is_v3(bytes: &[u8]) -> bool {
    bytes.get(..8) == Some(b"TTCLOUD\0".as_slice())
        && bytes.get(8..10) == Some(3u16.to_le_bytes().as_slice())
}
fn footer(bytes: &[u8], max_capture_bytes: usize) -> Result<CloudCaptureDescriptor, Status> {
    if !is_v3(bytes) {
        return Err(Status::unimplemented(
            "captured manifest version unsupported",
        ));
    }
    // V3 has the V2 body plus exactly one fixed footer. Native body parsing and
    // exact capture/export coverage stay with the native caller/server.
    if bytes.len() < 82 {
        return Err(corrupt());
    }
    let data = &bytes[bytes.len() - 72..];
    let selected = CloudRawDigest {
        blake3: array(&data[32..64])?,
        size_bytes: u64::from_le_bytes(array(&data[64..72])?),
    };
    if selected.size_bytes == 0 {
        return Err(corrupt());
    }
    if selected.size_bytes > max_capture_bytes as u64 {
        return Err(limit());
    }
    Ok(CloudCaptureDescriptor {
        id: CloudCaptureId(array(&data[..32])?),
        selected,
    })
}
/// An immutable original publish intent, including exact canonical capture DATA.
/// No method changes its expected generation, bytes or idempotency key.
#[derive(Debug)]
pub struct CloudCapturedPublishIntent {
    version: CloudPublishIntent,
    capture: CloudCaptureDescriptor,
    capture_data: Vec<u8>,
}
impl CloudCapturedPublishIntent {
    pub fn new(version: CloudPublishIntent, capture_data: Vec<u8>) -> Result<Self, Status> {
        if version.manifest.len() > 8 * 1024 * 1024 || capture_data.len() > 8 * 1024 * 1024 {
            return Err(limit());
        }
        let capture = footer(&version.manifest, 8 * 1024 * 1024)?;
        if CloudCaptureDescriptor::of(&capture_data) != capture {
            return Err(corrupt());
        }
        Ok(Self {
            version,
            capture,
            capture_data,
        })
    }
    pub fn version(&self) -> &CloudPublishIntent {
        &self.version
    }
    pub fn descriptor(&self) -> CloudCaptureDescriptor {
        self.capture
    }
    pub fn capture_data(&self) -> &[u8] {
        &self.capture_data
    }
}
/// Current-client retained version plus its exact fixed V3 footer. This proves
/// authenticated transport provenance, not full native manifest/capture coverage.
pub struct VerifiedCapturedVersion {
    version: VerifiedRetainedVersion,
    capture: CloudCaptureDescriptor,
}
impl std::fmt::Debug for VerifiedCapturedVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedCapturedVersion")
            .field("selector", &self.version.selector())
            .finish_non_exhaustive()
    }
}
impl VerifiedCapturedVersion {
    pub fn version(&self) -> &VerifiedRetainedVersion {
        &self.version
    }
    pub fn descriptor(&self) -> CloudCaptureDescriptor {
        self.capture
    }
}
/// Owned, raw/domain-hash checked capture DATA. Only borrowed bytes escape the
/// object; the persistent memory owner does not retain a physical RPC call slot.
pub struct CloudCapture {
    descriptor: CloudCaptureDescriptor,
    payload: Vec<u8>,
    _owner: Arc<RetainedCloud>,
}
impl CloudCapture {
    pub fn descriptor(&self) -> CloudCaptureDescriptor {
        self.descriptor
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}
impl std::fmt::Debug for CloudCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudCapture")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}
fn descriptor(value: c::CaptureDescriptor, max: usize) -> Result<CloudCaptureDescriptor, Status> {
    let selected = value.selected.ok_or_else(corrupt)?;
    if selected.size_bytes == 0 {
        return Err(corrupt());
    }
    if selected.size_bytes > max as u64 {
        return Err(limit());
    }
    Ok(CloudCaptureDescriptor {
        id: CloudCaptureId(array(&value.capture_id)?),
        selected: CloudRawDigest {
            blake3: array(&selected.blake3)?,
            size_bytes: selected.size_bytes,
        },
    })
}
impl NativeClient {
    /// A caller-supplied descriptor alone cannot create this client capability.
    pub fn cloud_captured_version(
        &self,
        version: VerifiedRetainedVersion,
    ) -> Result<VerifiedCapturedVersion, Status> {
        version.check_query_client(self)?;
        let capture = footer(
            version.manifest(),
            self.inner.config.cloud.max_capture_bytes,
        )?;
        Ok(VerifiedCapturedVersion { version, capture })
    }
    pub async fn cloud_publish_captured_version(
        &self,
        intent: &CloudCapturedPublishIntent,
    ) -> Result<CloudPublication, Status> {
        if intent.capture.selected.size_bytes > self.inner.config.cloud.max_capture_bytes as u64 {
            return Err(limit());
        }
        self.cloud_publish_at(&intent.version, &intent.capture_data, true)
            .await
    }
    pub async fn cloud_read_capture(
        &self,
        version: &VerifiedCapturedVersion,
    ) -> Result<CloudCapture, Status> {
        // Reject foreign-client capabilities before even constructing an RPC.
        version.version.check_query_client(self)?;
        let expected = version.capture;
        let l = &self.inner.config.cloud;
        if expected.selected.size_bytes > l.max_capture_bytes as u64 {
            return Err(limit());
        }
        let maximum = usize::try_from(expected.selected.size_bytes).map_err(|_| limit())?;
        let selector = version.version.selector();
        let selected_manifest = version.version.manifest_selected();
        let memory = self.cloud_memory(maximum.max(l.max_chunk_bytes))?;
        self.cloud_payload_fits(maximum, memory)?;
        let retained = self.cloud_retained(maximum)?;
        let deadline = self.deadline();
        let (payload, budget) = self
            .retry(deadline, true, |allowance| async move {
                let budget = self.cloud_admit(deadline, memory).await?;
                let mut grpc = self.rpc_client(budget.clone());
                let mut stream = grpc
                    .cloud_capture_read(
                        self.request(
                            c::ReadCaptureRequest {
                                repository_id: self.inner.config.repository_id.clone(),
                                selector: Some(c::RetainedVersionSelector {
                                    workspace_id: selector.workspace_id.to_vec(),
                                    generation: selector.generation,
                                    manifest_id: selector.manifest_id.0.to_vec(),
                                }),
                                expected_capture_id: expected.id.0.to_vec(),
                                manifest_selected: Some(c::RawDigest {
                                    blake3: selected_manifest.blake3.to_vec(),
                                    size_bytes: selected_manifest.size_bytes,
                                }),
                            },
                            allowance,
                        ),
                        l,
                    )
                    .await?
                    .into_inner();
                let mut bytes = Vec::new();
                bytes.try_reserve_exact(maximum).map_err(|_| limit())?;
                let mut eof = false;
                let mut first = true;
                loop {
                    let item = reapi::policy::timeout(
                        deadline.min(Instant::now() + self.inner.config.rpc.stream_idle_timeout()),
                        async { stream.message().await },
                    )
                    .await?;
                    let Some(chunk) = item else { break };
                    if eof
                        || descriptor(chunk.descriptor.ok_or_else(corrupt)?, l.max_capture_bytes)?
                            != expected
                        || chunk.manifest_id.as_slice() != selector.manifest_id.0
                        || chunk.offset != bytes.len() as u64
                        || chunk.data.len() > l.max_chunk_bytes
                        || bytes
                            .len()
                            .checked_add(chunk.data.len())
                            .is_none_or(|n| n > maximum)
                        || (chunk.data.is_empty() && !chunk.eof && !first)
                    {
                        return Err(corrupt());
                    }
                    first = false;
                    bytes.extend_from_slice(&chunk.data);
                    eof = chunk.eof;
                    if eof && bytes.len() != maximum {
                        return Err(corrupt());
                    }
                }
                if !eof || bytes.len() != maximum {
                    return Err(corrupt());
                }
                Ok((bytes, budget))
            })
            .await?;
        blocking_owned(deadline, budget, None, move || {
            let capture = CloudCapture {
                descriptor: expected,
                payload,
                _owner: retained,
            };
            if CloudCaptureDescriptor::of(capture.payload()) != expected {
                return Err(corrupt());
            }
            Ok(capture)
        })
        .await
    }
}
