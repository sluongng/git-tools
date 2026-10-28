//! Portable workspace-cloud transport. Raw digests and manifest IDs are checked;
//! native OP/VIEW semantic verification and manifest graph coverage remain the
//! caller's responsibility. Neither a receipt nor lookup establishes retention.
#[path = "cloud_capture.rs"]
mod capture;
#[path = "cloud_index.rs"]
mod index;
#[path = "cloud_native.rs"]
mod native;
use super::*;
use crate::transport::Shape;
pub use capture::{
    CloudCapture, CloudCaptureDescriptor, CloudCaptureId, CloudCapturedPublishIntent,
    VerifiedCapturedVersion,
};
pub use index::{
    CloudIndexArtifact, CloudIndexArtifactDescriptor, CloudIndexArtifactId, CloudIndexWriteReceipt,
};
pub use native::{RetainedVersionSelector, VerifiedRetainedVersion};
use serde::{Deserialize, Serialize};
use tinytree_proto::cloud_v1 as c;

/// Persist this complete identity before the first request. Its admission fence
/// must not be renewed when retrying an uncertain operation; active pins do not
/// expire when this time passes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudVersionPinIdentity {
    pub pin_id: [u8; 16],
    pub acquire_not_after_unix_seconds: i64,
}
impl CloudVersionPinIdentity {
    fn check(self) -> Result<(), Status> {
        if self.pin_id == [0; 16] || self.acquire_not_after_unix_seconds <= 0 {
            return Err(Status::invalid_argument("invalid cloud pin identity"));
        }
        Ok(())
    }
}
/// Durable job pin. No Drop path releases server retention; release is explicit.
#[derive(Debug)]
pub struct CloudVersionPin {
    pub identity: CloudVersionPinIdentity,
    pub version: VerifiedRetainedVersion,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloudPinSelection {
    LatestCaptured,
    Generation(u64),
    Exact {
        selector: RetainedVersionSelector,
        selected: CloudRawDigest,
    },
}
fn exact_pin_wire(
    selector: RetainedVersionSelector,
    selected: CloudRawDigest,
) -> Result<c::ExactVersionSelection, Status> {
    if selector.generation == 0 || selected.size_bytes == 0 {
        return Err(Status::invalid_argument("invalid cloud pin selector"));
    }
    Ok(c::ExactVersionSelection {
        version: Some(c::RetainedVersionSelector {
            workspace_id: selector.workspace_id.to_vec(),
            generation: selector.generation,
            manifest_id: selector.manifest_id.0.to_vec(),
        }),
        manifest_selected: Some(c::RawDigest {
            blake3: selected.blake3.to_vec(),
            size_bytes: selected.size_bytes,
        }),
    })
}

/// Explicit structured replay identity. Its display token is not authority and
/// never changes a legacy publication's namespace by textual resemblance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloudPublicationIdentity {
    pub workspace_id: [u8; 32],
    pub generation: u64,
    pub nonce: [u8; 16],
}
impl CloudPublicationIdentity {
    pub fn token(self) -> String {
        let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        format!(
            "r2-{}-{:016x}-{}",
            hex(&self.workspace_id),
            self.generation,
            hex(&self.nonce)
        )
    }
    fn wire(self) -> c::PublicationIdentity {
        c::PublicationIdentity {
            workspace_id: self.workspace_id.to_vec(),
            target_generation: self.generation,
            nonce: self.nonce.to_vec(),
        }
    }
    fn check_intent(self, intent: &CloudPublishIntent) -> Result<(), Status> {
        self.check(&intent.idempotency_key)?;
        if self.workspace_id != intent.workspace_id
            || Some(self.generation) != intent.expected_generation.checked_add(1)
        {
            return Err(Status::invalid_argument(
                "invalid structured cloud publication binding",
            ));
        }
        Ok(())
    }
    fn check(self, token: &str) -> Result<(), Status> {
        if self.generation == 0 || token != self.token() {
            return Err(Status::invalid_argument(
                "invalid structured cloud publication binding",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct CloudLimits {
    pub max_object_bytes: usize,
    pub max_manifest_bytes: usize,
    #[serde(skip_serializing_if = "default_capture_limit")]
    pub max_capture_bytes: usize,
    pub max_lookup_items: usize,
    pub max_staging_leases: usize,
    pub max_chunk_bytes: usize,
}
fn default_capture_limit(value: &usize) -> bool {
    *value == 8 * 1024 * 1024
}
impl Default for CloudLimits {
    fn default() -> Self {
        Self {
            max_object_bytes: 4 * 1024 * 1024,
            max_manifest_bytes: 8 * 1024 * 1024,
            max_capture_bytes: 8 * 1024 * 1024,
            max_lookup_items: 256,
            max_staging_leases: 1024,
            max_chunk_bytes: 64 * 1024,
        }
    }
}
impl CloudLimits {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if !(1..=4 * 1024 * 1024).contains(&self.max_object_bytes)
            || !(1..=8 * 1024 * 1024).contains(&self.max_manifest_bytes)
            || !(1..=8 * 1024 * 1024).contains(&self.max_capture_bytes)
            || !(1..=256).contains(&self.max_lookup_items)
            || !(1..=1024).contains(&self.max_staging_leases)
            || !(1024..=64 * 1024).contains(&self.max_chunk_bytes)
        {
            return Err("invalid native cloud limits");
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CloudObjectId {
    Operation([u8; 64]),
    View([u8; 64]),
}
impl CloudObjectId {
    fn wire(self) -> c::ObjectId {
        let (kind, id) = match self {
            Self::Operation(id) => (c::ObjectKind::Operation, id),
            Self::View(id) => (c::ObjectKind::View, id),
        };
        c::ObjectId {
            kind: kind as i32,
            semantic_id: id.to_vec(),
        }
    }
    fn check(self) -> Result<(), Status> {
        let id = match self {
            Self::Operation(id) | Self::View(id) => id,
        };
        if id == [0; 64] {
            Err(Status::invalid_argument(
                "intrinsic cloud objects have no payload",
            ))
        } else {
            Ok(())
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloudRawDigest {
    pub blake3: [u8; 32],
    pub size_bytes: u64,
}
impl CloudRawDigest {
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            blake3: *blake3::hash(bytes).as_bytes(),
            size_bytes: bytes.len() as u64,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloudDescriptor {
    pub id: CloudObjectId,
    pub selected: CloudRawDigest,
}
impl CloudDescriptor {
    fn wire(self) -> c::ObjectDescriptor {
        c::ObjectDescriptor {
            id: Some(self.id.wire()),
            selected: Some(c::RawDigest {
                blake3: self.selected.blake3.to_vec(),
                size_bytes: self.selected.size_bytes,
            }),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloudManifestId(pub [u8; 32]);
impl CloudManifestId {
    pub fn of(bytes: &[u8]) -> Self {
        Self(tinytree_model::cloud_manifest_id(bytes))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudWriteReceipt {
    pub descriptor: CloudDescriptor,
    pub staging_lease: String,
    pub expires_at_unix_seconds: i64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudWorkspace {
    pub workspace_id: [u8; 32],
    pub generation: u64,
    pub manifest_id: Option<CloudManifestId>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudPublication {
    pub idempotency_key: String,
    pub workspace: CloudWorkspace,
}
/// An immutable original publication intent. Retries and reconciliation reuse
/// these exact bytes, expected generation and key; no automatic rebase occurs.
#[derive(Debug)]
pub struct CloudPublishIntent {
    pub workspace_id: [u8; 32],
    pub expected_generation: u64,
    pub idempotency_key: String,
    pub manifest: Vec<u8>,
    pub staging_leases: Vec<String>,
}
struct RetainedCloud {
    _guards: crate::verified::LifetimeGuards,
    _bytes: OwnedSemaphorePermit,
    _state: OwnedSemaphorePermit,
}
/// Bounded selected raw bytes. This proves the selected raw digest, not the
/// native semantic OP/VIEW ID. The payload cannot escape its storage owner.
pub struct CloudObject {
    descriptor: CloudDescriptor,
    payload: Vec<u8>,
    _owner: Arc<RetainedCloud>,
}
impl CloudObject {
    pub fn descriptor(&self) -> CloudDescriptor {
        self.descriptor
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}
impl std::fmt::Debug for CloudObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudObject")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}
/// Exact version envelope and domain-checked raw manifest. Canonical manifest
/// decoding/coverage verification remains the native caller's responsibility.
pub struct CloudVersion {
    pub workspace_id: [u8; 32],
    pub generation: u64,
    pub manifest_id: CloudManifestId,
    pub previous_manifest_id: Option<CloudManifestId>,
    native_authority: Option<c::NativeVersionAuthority>,
    manifest: Vec<u8>,
    _owner: Arc<RetainedCloud>,
}
impl CloudVersion {
    pub fn manifest(&self) -> &[u8] {
        &self.manifest
    }
}
impl std::fmt::Debug for CloudVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudVersion")
            .field("generation", &self.generation)
            .field("manifest_id", &self.manifest_id)
            .finish_non_exhaustive()
    }
}
fn corrupt() -> Status {
    Status::data_loss("invalid workspace-cloud response")
}
fn limit() -> Status {
    Status::resource_exhausted("workspace-cloud resource limit exceeded")
}
fn array<const N: usize>(v: &[u8]) -> Result<[u8; N], Status> {
    v.try_into().map_err(|_| corrupt())
}
fn descriptor(value: c::ObjectDescriptor, max: usize) -> Result<CloudDescriptor, Status> {
    let id = value.id.ok_or_else(corrupt)?;
    let raw = array(&id.semantic_id)?;
    let id = match c::ObjectKind::try_from(id.kind).map_err(|_| corrupt())? {
        c::ObjectKind::Operation => CloudObjectId::Operation(raw),
        c::ObjectKind::View => CloudObjectId::View(raw),
        _ => return Err(corrupt()),
    };
    id.check().map_err(|_| corrupt())?;
    let digest = value.selected.ok_or_else(corrupt)?;
    if digest.size_bytes > max as u64 {
        return Err(limit());
    }
    Ok(CloudDescriptor {
        id,
        selected: CloudRawDigest {
            blake3: array(&digest.blake3)?,
            size_bytes: digest.size_bytes,
        },
    })
}
/// A selector summary; it does not carry verified manifest bytes or authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudVersionSummary {
    pub workspace_id: [u8; 32],
    pub generation: u64,
    pub manifest_id: CloudManifestId,
    pub previous_manifest_id: Option<CloudManifestId>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudWorkspacePage {
    pub workspaces: Vec<CloudWorkspace>,
    pub continuation: Vec<u8>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloudVersionPage {
    pub workspace: CloudWorkspace,
    pub versions: Vec<CloudVersionSummary>,
    pub through_generation: u64,
    pub retired_through: u64,
    pub retention_revision: u64,
    pub continuation: Vec<u8>,
}
fn discovery_cursor(
    cursor: &[u8],
    domain: u8,
    size: usize,
) -> Result<Option<([u8; 32], u64)>, Status> {
    if !(1..=128).contains(&size) {
        return Err(Status::invalid_argument("cloud discovery page size"));
    }
    if cursor.is_empty() {
        return Ok(None);
    }
    let cursor_bytes = if domain == 2 { 82 } else { 74 };
    if cursor.len() != cursor_bytes || cursor[..2] != [1, domain] {
        return Err(Status::invalid_argument("cloud discovery continuation"));
    }
    let id: [u8; 32] = cursor[34..66].try_into().unwrap();
    let ceiling = u64::from_be_bytes(cursor[66..74].try_into().unwrap());
    if (domain == 1 && ceiling != 0)
        || (domain == 2
            && (id[16..] != [0; 16]
                || u64::from_be_bytes(id[8..16].try_into().unwrap())
                    > u64::from_be_bytes(id[..8].try_into().unwrap())
                || ceiling == 0
                || u64::from_be_bytes(id[..8].try_into().unwrap()) == 0
                || u64::from_be_bytes(id[..8].try_into().unwrap()) >= ceiling))
    {
        return Err(Status::invalid_argument("cloud discovery continuation"));
    }
    Ok(Some((id, ceiling)))
}
fn check_discovery_next(
    next: &[u8],
    before: &[u8],
    domain: u8,
    size: usize,
    last: Option<[u8; 32]>,
    ceiling: u64,
    rows: usize,
) -> Result<(), Status> {
    if next.is_empty() {
        return Ok(());
    }
    let (position, c) = discovery_cursor(next, domain, size)
        .map_err(|_| corrupt())?
        .ok_or_else(corrupt)?;
    if rows != size
        || Some(position) != last
        || c != ceiling
        || (!before.is_empty() && next[2..34] != before[2..34])
        || (domain == 2 && !before.is_empty() && next[74..82] != before[74..82])
    {
        return Err(corrupt());
    }
    Ok(())
}
fn workspace(value: c::Workspace, expected: [u8; 32]) -> Result<CloudWorkspace, Status> {
    if array::<32>(&value.workspace_id)? != expected
        || (value.generation == 0) != value.manifest_id.is_none()
    {
        return Err(corrupt());
    }
    Ok(CloudWorkspace {
        workspace_id: expected,
        generation: value.generation,
        manifest_id: value
            .manifest_id
            .map(|v| array(&v).map(CloudManifestId))
            .transpose()?,
    })
}
fn key(value: &str) -> Result<(), Status> {
    if value.is_empty() || value.len() > 256 || !value.bytes().all(|b| b.is_ascii_graphic()) {
        Err(Status::invalid_argument(
            "invalid workspace-cloud idempotency key",
        ))
    } else {
        Ok(())
    }
}
fn lease(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
}
impl NativeClient {
    fn cloud_memory(&self, cap: usize) -> Result<usize, Status> {
        self.cloud_memory_at(cap, None)
    }
    fn cloud_memory_at(
        &self,
        cap: usize,
        reason: Option<tinytree_proto::PublicationFailureReason>,
    ) -> Result<usize, Status> {
        cap.checked_mul(8)
            .and_then(|n| n.checked_add(128 * 1024))
            .filter(|n| {
                *n <= self.inner.config.limits.max_buffered_bytes && *n <= u32::MAX as usize
            })
            .ok_or_else(|| match reason {
                Some(reason) => Status::resource_exhausted(reason.message()),
                None => limit(),
            })
    }
    async fn cloud_admit(&self, deadline: Instant, bytes: usize) -> Result<Arc<Budget>, Status> {
        self.cloud_memory(0)?;
        if bytes > self.inner.config.limits.max_buffered_bytes || bytes > u32::MAX as usize {
            return Err(limit());
        }
        let inflight = reapi::policy::timeout(deadline, async {
            self.inner
                .inflight
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Status::cancelled("native client closed"))
        })
        .await?;
        // Retained objects and earlier cancelled physical bodies share this
        // pool. Do not wait while holding a call slot for a result to be dropped.
        let bytes = self
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| limit())?;
        Ok(Arc::new(Budget {
            _guards: self.rpc_lifetime_guards(),
            _inflight: inflight,
            _bytes: bytes,
        }))
    }
    fn cloud_payload_fits(&self, size: usize, physical: usize) -> Result<(), Status> {
        let retained = size
            .checked_mul(2)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(limit)?;
        if retained
            .checked_add(physical)
            .is_none_or(|n| n > self.inner.config.limits.max_buffered_bytes)
        {
            return Err(limit());
        }
        Ok(())
    }
    fn cloud_retained(&self, size: usize) -> Result<Arc<RetainedCloud>, Status> {
        let n = size
            .checked_mul(2)
            .and_then(|n| n.checked_add(4096))
            .filter(|n| *n <= u32::MAX as usize)
            .ok_or_else(limit)? as u32;
        let state = self
            .inner
            .query_state
            .clone()
            .try_acquire_many_owned(n)
            .map_err(|_| limit())?;
        let bytes = self
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(n)
            .map_err(|_| limit())?;
        Ok(Arc::new(RetainedCloud {
            _guards: self.guards.clone(),
            _bytes: bytes,
            _state: state,
        }))
    }
    async fn cloud_call<T, U, F>(
        &self,
        deadline: Instant,
        path: &'static str,
        shape: Shape,
        cap: usize,
        request_bound: usize,
        make: F,
    ) -> Result<(U, Arc<Budget>), Status>
    where
        T: Message + Send + 'static,
        U: Message + Default + Send + 'static,
        F: Fn() -> T + Sync,
    {
        let memory = self.cloud_memory(cap.max(request_bound))?;
        self.retry(deadline, false, |allowance| {
            let make = &make;
            async move {
                // Per-attempt admission: an earlier timed-out HTTP body may still
                // own its buffers while the retry starts. It keeps its own charge.
                let budget = self.cloud_admit(deadline, memory).await?;
                let request = make();
                if request.encoded_len() > request_bound {
                    return Err(limit());
                }
                let mut grpc = self.rpc_client(budget.clone());
                let response = grpc
                    .cloud_unary(
                        self.request(request, allowance),
                        path,
                        shape,
                        &self.inner.config.cloud,
                        cap,
                        request_bound,
                    )
                    .await?
                    .into_inner();
                Ok((response, budget))
            }
        })
        .await
    }
    pub async fn cloud_put_object(
        &self,
        id: CloudObjectId,
        payload: &[u8],
    ) -> Result<CloudWriteReceipt, Status> {
        self.cloud_put_object_inner(id, payload, false).await
    }
    /// Stage this exact raw encoding without changing primary selection.
    pub async fn cloud_put_object_representation(
        &self,
        id: CloudObjectId,
        payload: &[u8],
    ) -> Result<CloudWriteReceipt, Status> {
        self.cloud_put_object_inner(id, payload, true).await
    }
    async fn cloud_put_object_inner(
        &self,
        id: CloudObjectId,
        payload: &[u8],
        exact: bool,
    ) -> Result<CloudWriteReceipt, Status> {
        id.check()?;
        let limits = &self.inner.config.cloud;
        if payload.len() > limits.max_object_bytes {
            return Err(limit());
        }
        let deadline = self.deadline();
        let maximum = payload.len().checked_add(4096).ok_or_else(limit)?;
        // Hashing and the request copy occur only after memory admission.
        let memory = self.cloud_memory(maximum)?;
        let verify_budget = self.cloud_admit(deadline, memory).await?;
        let bytes = payload.to_vec();
        let selected = blocking_owned(deadline, verify_budget, None, move || {
            Ok(CloudRawDigest::of(&bytes))
        })
        .await?;
        let expected = CloudDescriptor { id, selected };
        let (receipt, _budget) = self
            .cloud_call(
                deadline,
                if exact {
                    "/tinytree.cloud.v1.WorkspaceCloud/PutObjectRepresentation"
                } else {
                    "/tinytree.cloud.v1.WorkspaceCloud/PutObject"
                },
                Shape::CloudReceipt,
                4096,
                maximum,
                || c::PutObjectRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    descriptor: Some(expected.wire()),
                    payload: payload.to_vec(),
                },
            )
            .await?;
        let receipt: c::WriteReceipt = receipt;
        // A semantic ID may already select a different valid raw encoding.
        // Return the server's bounded selection, not the proposed raw digest.
        let actual = descriptor(
            receipt.descriptor.ok_or_else(corrupt)?,
            limits.max_object_bytes,
        )?;
        if actual.id != expected.id
            || (exact && actual != expected)
            || !lease(&receipt.staging_lease)
            || receipt.expires_at_unix_seconds <= 0
        {
            return Err(corrupt());
        }
        Ok(CloudWriteReceipt {
            descriptor: actual,
            staging_lease: receipt.staging_lease,
            expires_at_unix_seconds: receipt.expires_at_unix_seconds,
        })
    }
    pub async fn cloud_lookup_objects(
        &self,
        ids: &[CloudObjectId],
    ) -> Result<Vec<Option<CloudDescriptor>>, Status> {
        let l = &self.inner.config.cloud;
        if ids.len() > l.max_lookup_items {
            return Err(limit());
        }
        for id in ids {
            id.check()?
        }
        let bound = ids
            .len()
            .checked_mul(1024)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(limit)?;
        let (response, _budget) = self
            .cloud_call(
                self.deadline(),
                "/tinytree.cloud.v1.WorkspaceCloud/LookupObjects",
                Shape::CloudSelections,
                bound,
                bound,
                || c::LookupObjectsRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    ids: ids.iter().map(|id| id.wire()).collect(),
                },
            )
            .await?;
        let response: c::LookupObjectsResponse = response;
        if response.selections.len() != ids.len() {
            return Err(corrupt());
        }
        response
            .selections
            .into_iter()
            .zip(ids)
            .map(|(selection, id)| {
                selection
                    .descriptor
                    .map(|v| {
                        let value = descriptor(v, l.max_object_bytes)?;
                        if value.id != *id {
                            return Err(corrupt());
                        }
                        Ok(value)
                    })
                    .transpose()
            })
            .collect()
    }
    pub async fn cloud_read_object(
        &self,
        expected: CloudDescriptor,
    ) -> Result<CloudObject, Status> {
        self.cloud_read_object_inner(expected, false).await
    }
    /// Fetch the exact named raw representation, not semantic primary fallback.
    pub async fn cloud_read_object_representation(
        &self,
        expected: CloudDescriptor,
    ) -> Result<CloudObject, Status> {
        self.cloud_read_object_inner(expected, true).await
    }
    async fn cloud_read_object_inner(
        &self,
        expected: CloudDescriptor,
        exact: bool,
    ) -> Result<CloudObject, Status> {
        expected.id.check()?;
        let l = &self.inner.config.cloud;
        if expected.selected.size_bytes > l.max_object_bytes as u64 {
            return Err(limit());
        }
        let deadline = self.deadline();
        let maximum = usize::try_from(expected.selected.size_bytes).map_err(|_| limit())?;
        let memory = self.cloud_memory(maximum.max(l.max_chunk_bytes))?;
        self.cloud_payload_fits(maximum, memory)?;
        let retained = self.cloud_retained(maximum)?;
        let (payload, budget) = self
            .retry(deadline, true, |allowance| async move {
                let budget = self.cloud_admit(deadline, memory).await?;
                let mut grpc = self.rpc_client(budget.clone());
                let mut stream = if exact {
                    grpc.cloud_representation_read(
                        self.request(
                            c::ReadObjectRepresentationRequest {
                                repository_id: self.inner.config.repository_id.clone(),
                                descriptor: Some(expected.wire()),
                            },
                            allowance,
                        ),
                        l,
                    )
                    .await?
                } else {
                    grpc.cloud_read(
                        self.request(
                            c::ReadObjectRequest {
                                repository_id: self.inner.config.repository_id.clone(),
                                id: Some(expected.id.wire()),
                            },
                            allowance,
                        ),
                        l,
                    )
                    .await?
                }
                .into_inner();
                let mut bytes = Vec::new();
                bytes.try_reserve_exact(maximum).map_err(|_| limit())?;
                let mut eof = false;
                loop {
                    let item = reapi::policy::timeout(
                        deadline.min(Instant::now() + self.inner.config.rpc.stream_idle_timeout()),
                        async { stream.message().await },
                    )
                    .await?;
                    let Some(chunk) = item else { break };
                    if eof
                        || descriptor(chunk.descriptor.ok_or_else(corrupt)?, l.max_object_bytes)?
                            != expected
                        || chunk.offset != bytes.len() as u64
                        || chunk.data.len() > l.max_chunk_bytes
                        || bytes
                            .len()
                            .checked_add(chunk.data.len())
                            .is_none_or(|n| n > maximum)
                        || (chunk.data.is_empty() && !chunk.eof)
                    {
                        return Err(corrupt());
                    }
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
            let object = CloudObject {
                descriptor: expected,
                payload,
                _owner: retained,
            };
            if CloudRawDigest::of(object.payload()) != expected.selected {
                return Err(corrupt());
            }
            Ok(object)
        })
        .await
    }
    pub async fn cloud_workspace(&self, id: [u8; 32]) -> Result<CloudWorkspace, Status> {
        let (value, _budget) = self
            .cloud_call(
                self.deadline(),
                "/tinytree.cloud.v1.WorkspaceCloud/GetWorkspace",
                Shape::CloudWorkspace,
                4096,
                4096,
                || c::GetWorkspaceRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    workspace_id: id.to_vec(),
                },
            )
            .await?;
        workspace(value, id)
    }
    /// A moving owner-scoped catalog page; inserts before a continuation require
    /// a fresh scan. Entries are selectors, not verified retained-version leases.
    pub async fn cloud_workspaces(
        &self,
        page_size: usize,
        continuation: &[u8],
    ) -> Result<CloudWorkspacePage, Status> {
        let after = discovery_cursor(continuation, 1, page_size)?;
        let (value, _budget): (c::ListWorkspacesResponse, _) = self
            .cloud_call(
                self.deadline(),
                "/tinytree.cloud.v1.WorkspaceCloud/ListWorkspaces",
                Shape::CloudWorkspacePage,
                65536,
                1024,
                || c::ListWorkspacesRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    page_size: page_size as u32,
                    continuation: continuation.to_vec(),
                },
            )
            .await?;
        if value.workspaces.len() > page_size {
            return Err(corrupt());
        }
        let mut rows = Vec::new();
        let mut last = after.map(|(id, _)| id);
        for row in value.workspaces {
            let id = array(&row.workspace_id)?;
            if last.is_some_and(|old| id <= old) || row.generation == 0 {
                return Err(corrupt());
            }
            rows.push(workspace(row, id)?);
            last = Some(id);
        }
        check_discovery_next(
            &value.continuation,
            continuation,
            1,
            page_size,
            last,
            0,
            rows.len(),
        )?;
        Ok(CloudWorkspacePage {
            workspaces: rows,
            continuation: value.continuation,
        })
    }
    /// Immutable generation selectors through the first page's ceiling. Fetch
    /// cloud_version for the selected manifest payload/digest before restoring.
    pub async fn cloud_versions(
        &self,
        id: [u8; 32],
        page_size: usize,
        continuation: &[u8],
    ) -> Result<CloudVersionPage, Status> {
        let after = discovery_cursor(continuation, 2, page_size)?;
        let (value, _budget): (c::ListVersionsResponse, _) = self
            .cloud_call(
                self.deadline(),
                "/tinytree.cloud.v1.WorkspaceCloud/ListVersions",
                Shape::CloudVersionPage,
                65536,
                1024,
                || c::ListVersionsRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    workspace_id: id.to_vec(),
                    page_size: page_size as u32,
                    continuation: continuation.to_vec(),
                },
            )
            .await?;
        let current = workspace(value.workspace.ok_or_else(corrupt)?, id)?;
        let (old, ceiling) = after
            .map(|(id, c)| (u64::from_be_bytes(id[..8].try_into().unwrap()), c))
            .unwrap_or((value.retired_through, current.generation));
        if value.through_generation != ceiling
            || ceiling > current.generation
            || value.retired_through > old
            || after.is_some_and(|(position, _)| {
                u64::from_be_bytes(position[8..16].try_into().unwrap()) != value.retired_through
            })
            || value.retired_through > ceiling
            || (ceiling > 0 && value.retired_through == ceiling)
            || (!continuation.is_empty()
                && u64::from_be_bytes(continuation[74..82].try_into().unwrap())
                    != value.retention_revision)
            || (!value.continuation.is_empty()
                && discovery_cursor(&value.continuation, 2, page_size).is_err())
            || (!value.continuation.is_empty()
                && u64::from_be_bytes(value.continuation[74..82].try_into().unwrap())
                    != value.retention_revision)
            || value.versions.len() > page_size
        {
            return Err(corrupt());
        }
        let mut versions = Vec::new();
        for row in value.versions {
            if array::<32>(&row.workspace_id)? != id
                || Some(row.generation)
                    != old
                        .checked_add(versions.len() as u64)
                        .and_then(|n| n.checked_add(1))
                || row.generation > ceiling
            {
                return Err(corrupt());
            }
            let previous = row
                .previous_manifest_id
                .map(|v| array(&v).map(CloudManifestId))
                .transpose()?;
            if (row.generation == 1) != previous.is_none()
                || versions
                    .last()
                    .is_some_and(|v: &CloudVersionSummary| Some(v.manifest_id) != previous)
            {
                return Err(corrupt());
            }
            versions.push(CloudVersionSummary {
                workspace_id: id,
                generation: row.generation,
                manifest_id: CloudManifestId(array(&row.manifest_id)?),
                previous_manifest_id: previous,
            });
        }
        let last = versions.last().map(|v| v.generation).unwrap_or(old);
        if (last < ceiling && versions.len() != page_size)
            || (last == ceiling) != value.continuation.is_empty()
        {
            return Err(corrupt());
        }
        if last == current.generation
            && versions
                .last()
                .is_some_and(|v| Some(v.manifest_id) != current.manifest_id)
        {
            return Err(corrupt());
        }
        let mut position = [0; 32];
        position[..8].copy_from_slice(&last.to_be_bytes());
        position[8..16].copy_from_slice(&value.retired_through.to_be_bytes());
        check_discovery_next(
            &value.continuation,
            continuation,
            2,
            page_size,
            Some(position),
            ceiling,
            versions.len(),
        )?;
        Ok(CloudVersionPage {
            workspace: current,
            versions,
            through_generation: ceiling,
            retired_through: value.retired_through,
            retention_revision: value.retention_revision,
            continuation: value.continuation,
        })
    }
    async fn cloud_publication_at(
        &self,
        key_value: &str,
        deadline: Instant,
        identity: Option<CloudPublicationIdentity>,
    ) -> Result<Option<CloudPublication>, Status> {
        key(key_value)?;
        if let Some(identity) = identity {
            identity.check(key_value)?;
        }
        let (value, _budget) = self
            .cloud_call(
                deadline,
                "/tinytree.cloud.v1.WorkspaceCloud/GetPublication",
                Shape::CloudPublicationResult,
                4096,
                4096,
                || c::GetPublicationRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    idempotency_key: key_value.into(),
                    publication_identity: identity.map(CloudPublicationIdentity::wire),
                },
            )
            .await?;
        let value: c::GetPublicationResponse = value;
        value
            .publication
            .map(|value| {
                if value.idempotency_key != key_value {
                    return Err(corrupt());
                }
                let state = value.workspace.ok_or_else(corrupt)?;
                let id = array(&state.workspace_id)?;
                if identity.is_some_and(|identity| {
                    identity.workspace_id != id || identity.generation != state.generation
                }) {
                    return Err(corrupt());
                }
                Ok(CloudPublication {
                    idempotency_key: value.idempotency_key,
                    workspace: workspace(state, id)?,
                })
            })
            .transpose()
    }
    pub async fn cloud_publication(
        &self,
        key_value: &str,
    ) -> Result<Option<CloudPublication>, Status> {
        self.cloud_publication_at(key_value, self.deadline(), None)
            .await
    }
    async fn require_structured_publication(&self, deadline: Instant) -> Result<(), Status> {
        let capabilities = self
            .clone()
            .with_operation_deadline(deadline)
            .capabilities(&[crate::CapabilityFeature::StructuredCloudPublicationV2])
            .await
            .map_err(|error| {
                if matches!(
                    error.code(),
                    tonic::Code::InvalidArgument | tonic::Code::Unimplemented
                ) {
                    Status::unimplemented("structured cloud publication is not supported")
                } else {
                    error
                }
            })?;
        let feature = capabilities.features[0];
        if !feature.authorized {
            return Err(Status::permission_denied(
                "structured cloud publication is not authorized",
            ));
        }
        if !feature.supported {
            return Err(Status::unimplemented(
                "structured cloud publication is not supported",
            ));
        }
        Ok(())
    }
    async fn require_version_pin(&self, deadline: Instant) -> Result<(), Status> {
        let caps = self
            .clone()
            .with_operation_deadline(deadline)
            .capabilities(&[crate::CapabilityFeature::DurableCloudVersionPinV1])
            .await
            .map_err(|error| {
                if matches!(
                    error.code(),
                    tonic::Code::InvalidArgument | tonic::Code::Unimplemented
                ) {
                    Status::unimplemented("durable cloud version pins are not supported")
                } else if error.code() == tonic::Code::OutOfRange {
                    // A capability error says nothing about this pin identity.
                    Status::failed_precondition("cloud pin capability negotiation unavailable")
                } else {
                    error
                }
            })?;
        if !caps.features[0].authorized {
            return Err(Status::permission_denied(
                "durable cloud version pins are not authorized",
            ));
        }
        if !caps.features[0].supported {
            return Err(Status::unimplemented(
                "durable cloud version pins are not supported",
            ));
        }
        Ok(())
    }
    /// Acquire or replay one persisted admission identity. `OutOfRange` from
    /// this method is reserved for the server's durable terminal identity fence
    /// (released, or admission expired with no active pin). Source unavailability
    /// uses other codes and does not establish that an older worker cannot commit.
    /// A caller may retire an uncertain pending row only when that terminal
    /// result is combined with absence of local authorities and physical claims.
    pub async fn acquire_cloud_version_pin(
        &self,
        workspace_id: [u8; 32],
        identity: CloudVersionPinIdentity,
        selection: CloudPinSelection,
    ) -> Result<CloudVersionPin, Status> {
        identity.check()?;
        let deadline = self.deadline();
        use c::acquire_version_pin_request::Selection;
        let (wire, maximum) = match selection {
            CloudPinSelection::LatestCaptured => {
                (Selection::LatestCaptured(true), self.cloud_version_maximum()?)
            }
            CloudPinSelection::Generation(generation) if generation > 0 => {
                (Selection::Generation(generation), self.cloud_version_maximum()?)
            }
            CloudPinSelection::Exact { selector, selected }
                if selector.workspace_id == workspace_id =>
            {
                let wire = exact_pin_wire(selector, selected)?;
                // Exact selection already binds the response bytes. Reserve that
                // checked bound rather than a discovery-sized arena, so retained
                // capture/query authorities can coexist with reattachment.
                let maximum = usize::try_from(selected.size_bytes).map_err(|_| limit())?;
                if maximum > self.inner.config.cloud.max_manifest_bytes {
                    return Err(limit());
                }
                (Selection::Exact(wire), maximum)
            }
            _ => return Err(Status::invalid_argument("invalid cloud pin selection")),
        };
        self.require_version_pin(deadline).await?;
        self.cloud_payload_fits(maximum, self.cloud_memory(maximum + 4096)?)?;
        let retained = self.cloud_retained(maximum)?;
        let (value, budget): (c::VersionPin, _) = self
            .cloud_call(
                deadline,
                "/tinytree.cloud.v1.WorkspaceCloud/AcquireVersionPin",
                Shape::CloudVersionPin,
                maximum + 4096,
                4096,
                || c::AcquireVersionPinRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    workspace_id: workspace_id.to_vec(),
                    pin_id: identity.pin_id.to_vec(),
                    acquire_not_after_unix_seconds: identity.acquire_not_after_unix_seconds,
                    selection: Some(wire.clone()),
                },
            )
            .await?;
        if value.pin_id.as_slice() != identity.pin_id
            || value.acquire_not_after_unix_seconds != identity.acquire_not_after_unix_seconds
        {
            return Err(corrupt());
        }
        let expected_generation = match selection {
            CloudPinSelection::LatestCaptured => None,
            CloudPinSelection::Generation(generation) => Some(generation),
            CloudPinSelection::Exact { selector, .. } => Some(selector.generation),
        };
        let version = self
            .finish_cloud_version(
                value.version.ok_or_else(corrupt)?,
                budget,
                retained,
                workspace_id,
                expected_generation,
                maximum,
                deadline,
            )
            .await?;
        if let CloudPinSelection::Exact { selector, selected } = selection {
            if version.manifest_id != selector.manifest_id
                || CloudRawDigest::of(version.manifest()) != selected
            {
                return Err(corrupt());
            }
        }
        if !capture::is_v3(version.manifest()) {
            return Err(corrupt());
        }
        Ok(CloudVersionPin {
            identity,
            version: self.checked_retained_version(version)?,
        })
    }
    pub async fn release_cloud_version_pin(
        &self,
        identity: CloudVersionPinIdentity,
        selector: RetainedVersionSelector,
        selected: CloudRawDigest,
    ) -> Result<(), Status> {
        identity.check()?;
        let deadline = self.deadline();
        let exact = exact_pin_wire(selector, selected)?;
        self.require_version_pin(deadline).await?;
        let (value, _budget): (c::VersionPinReleased, _) = self
            .cloud_call(
                deadline,
                "/tinytree.cloud.v1.WorkspaceCloud/ReleaseVersionPin",
                Shape::CloudPinReleased,
                1024,
                4096,
                || c::ReleaseVersionPinRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    pin_id: identity.pin_id.to_vec(),
                    acquire_not_after_unix_seconds: identity.acquire_not_after_unix_seconds,
                    exact: Some(exact.clone()),
                },
            )
            .await?;
        if value.pin_id.as_slice() != identity.pin_id
            || value.acquire_not_after_unix_seconds != identity.acquire_not_after_unix_seconds
        {
            return Err(corrupt());
        }
        Ok(())
    }
    pub async fn cloud_publication_v2(
        &self,
        identity: CloudPublicationIdentity,
    ) -> Result<Option<CloudPublication>, Status> {
        let deadline = self.deadline();
        identity.check(&identity.token())?;
        self.require_structured_publication(deadline).await?;
        self.cloud_publication_at(&identity.token(), deadline, Some(identity))
            .await
    }
    pub async fn cloud_publish_version_v2(
        &self,
        intent: &CloudPublishIntent,
        identity: CloudPublicationIdentity,
    ) -> Result<CloudPublication, Status> {
        let deadline = self.deadline();
        identity.check_intent(intent)?;
        if capture::is_v3(&intent.manifest) {
            return Err(Status::invalid_argument(
                "captured manifest requires capture data",
            ));
        }
        self.require_structured_publication(deadline).await?;
        self.clone()
            .with_operation_deadline(deadline)
            .cloud_publish_at(intent, &[], true, Some(identity))
            .await
    }
    pub async fn cloud_publish_version(
        &self,
        intent: &CloudPublishIntent,
    ) -> Result<CloudPublication, Status> {
        if capture::is_v3(&intent.manifest) {
            return Err(Status::invalid_argument(
                "captured manifest requires capture data",
            ));
        }
        self.cloud_publish_at(intent, &[], false, None).await
    }
    async fn cloud_publish_at(
        &self,
        intent: &CloudPublishIntent,
        capture_data: &[u8],
        same_deadline_reconcile: bool,
        identity: Option<CloudPublicationIdentity>,
    ) -> Result<CloudPublication, Status> {
        key(&intent.idempotency_key)?;
        if let Some(identity) = identity {
            identity.check(&intent.idempotency_key)?;
            if identity.workspace_id != intent.workspace_id
                || Some(identity.generation) != intent.expected_generation.checked_add(1)
            {
                return Err(Status::invalid_argument(
                    "invalid structured cloud publication binding",
                ));
            }
        }
        let l = &self.inner.config.cloud;
        use tinytree_proto::PublicationFailureReason as Reason;
        if intent.manifest.len() > l.max_manifest_bytes {
            return Err(Status::resource_exhausted(
                Reason::ClientManifestBytes.message(),
            ));
        }
        if capture_data.len() > l.max_capture_bytes {
            return Err(Status::resource_exhausted(
                Reason::ClientCaptureBytes.message(),
            ));
        }
        if intent.staging_leases.len() > l.max_staging_leases {
            return Err(Status::resource_exhausted(
                Reason::ClientStagingLeaseCount.message(),
            ));
        }
        if intent.staging_leases.iter().any(|s| !lease(s)) {
            return Err(Status::resource_exhausted(
                Reason::ClientStagingLeaseFormat.message(),
            ));
        }
        let generation = intent
            .expected_generation
            .checked_add(1)
            .ok_or_else(|| Status::invalid_argument("workspace-cloud generation exhausted"))?;
        let bound = intent
            .manifest
            .len()
            .checked_add(capture_data.len())
            .and_then(|n| n.checked_add(intent.staging_leases.len() * 68))
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| Status::resource_exhausted(Reason::ClientRequestSize.message()))?;
        let deadline = self.deadline();
        let verification_memory =
            self.cloud_memory_at(bound, Some(Reason::ClientPublicationMemory))?;
        let verify_budget = self.cloud_admit(deadline, verification_memory).await?;
        let bytes = intent.manifest.clone();
        let expected = blocking_owned(deadline, verify_budget, None, move || {
            Ok(CloudManifestId::of(&bytes))
        })
        .await?;
        let result = self
            .cloud_call(
                deadline,
                "/tinytree.cloud.v1.WorkspaceCloud/PublishVersion",
                Shape::CloudPublication,
                4096,
                bound,
                || c::PublishVersionRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    workspace_id: intent.workspace_id.to_vec(),
                    expected_generation: intent.expected_generation,
                    idempotency_key: intent.idempotency_key.clone(),
                    manifest_id: expected.0.to_vec(),
                    manifest: intent.manifest.clone(),
                    staging_leases: intent.staging_leases.clone(),
                    capture_data: capture_data.to_vec(),
                    publication_identity: identity.map(CloudPublicationIdentity::wire),
                },
            )
            .await;
        let publication = match result {
            Ok((value, _budget)) => {
                let value: c::Publication = value;
                if value.idempotency_key != intent.idempotency_key {
                    return Err(corrupt());
                }
                CloudPublication {
                    idempotency_key: value.idempotency_key,
                    workspace: workspace(
                        value.workspace.ok_or_else(corrupt)?,
                        intent.workspace_id,
                    )?,
                }
            }
            Err(error)
                if reapi::policy::retryable(&error)
                    || error.code() == tonic::Code::DeadlineExceeded =>
            {
                // Reconcile only the original key. Captured publication stays
                // within its original absolute deadline; legacy policy retains
                // its existing fresh, bounded reconciliation deadline.
                match self
                    .cloud_publication_at(
                        &intent.idempotency_key,
                        if same_deadline_reconcile {
                            deadline
                        } else {
                            self.deadline()
                        },
                        identity,
                    )
                    .await?
                {
                    Some(value) => value,
                    None => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        if publication.workspace.workspace_id != intent.workspace_id
            || publication.workspace.generation != generation
            || publication.workspace.manifest_id != Some(expected)
        {
            return Err(corrupt());
        }
        Ok(publication)
    }
    pub async fn cloud_version(
        &self,
        id: [u8; 32],
        generation: u64,
    ) -> Result<CloudVersion, Status> {
        let maximum = self.cloud_version_maximum()?;
        self.cloud_version_bounded(id, generation, maximum).await
    }
    /// Greatest logical capture sequence; an unsupported server fails explicitly.
    /// This never falls back to publication order or the workspace head.
    pub async fn cloud_latest_captured_version(
        &self,
        id: [u8; 32],
    ) -> Result<CloudVersion, Status> {
        let maximum = self.cloud_version_maximum()?;
        self.cloud_selected_version(id, None, maximum).await
    }
    fn cloud_version_maximum(&self) -> Result<usize, Status> {
        // Unknown-size discovery uses a static effective cap within the existing
        // physical and retained pools. Available permits must not change which
        // version sizes a configured client accepts.
        let limits = &self.inner.config.limits;
        let state = limits
            .max_buffered_bytes
            .saturating_sub(query_headroom(limits));
        let fits = |size: usize| {
            size.checked_add(4096)
                .and_then(|cap| self.cloud_memory(cap).ok())
                .is_some_and(|physical| {
                    self.cloud_payload_fits(size, physical).is_ok()
                        && size
                            .checked_mul(2)
                            .and_then(|n| n.checked_add(4096))
                            .is_some_and(|n| n <= state)
                })
        };
        let mut low = 0;
        let mut high = self.inner.config.cloud.max_manifest_bytes;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if fits(middle) {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        if low == 0 {
            return Err(limit());
        }
        Ok(low)
    }
    async fn cloud_version_bounded(
        &self,
        id: [u8; 32],
        generation: u64,
        maximum: usize,
    ) -> Result<CloudVersion, Status> {
        if generation == 0 {
            return Err(Status::invalid_argument(
                "cloud version generation must be positive",
            ));
        }
        self.cloud_selected_version(id, Some(generation), maximum)
            .await
    }
    async fn cloud_selected_version(
        &self,
        id: [u8; 32],
        generation: Option<u64>,
        maximum: usize,
    ) -> Result<CloudVersion, Status> {
        if maximum > self.inner.config.cloud.max_manifest_bytes {
            return Err(limit());
        }
        let deadline = self.deadline();
        self.cloud_payload_fits(maximum, self.cloud_memory(maximum + 4096)?)?;
        let retained = self.cloud_retained(maximum)?;
        let (value, budget) = if let Some(generation) = generation {
            self.cloud_call(
                deadline,
                "/tinytree.cloud.v1.WorkspaceCloud/GetVersion",
                Shape::CloudVersion,
                maximum + 4096,
                4096,
                || c::GetVersionRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    workspace_id: id.to_vec(),
                    generation,
                },
            )
            .await?
        } else {
            self.cloud_call(
                deadline,
                "/tinytree.cloud.v1.WorkspaceCloud/GetLatestCapturedVersion",
                Shape::CloudVersion,
                maximum + 4096,
                4096,
                || c::GetLatestCapturedVersionRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    workspace_id: id.to_vec(),
                },
            )
            .await?
        };
        self.finish_cloud_version(value, budget, retained, id, generation, maximum, deadline)
            .await
    }
    async fn finish_cloud_version(
        &self,
        value: c::Version,
        budget: Arc<Budget>,
        retained: Arc<RetainedCloud>,
        id: [u8; 32],
        generation: Option<u64>,
        maximum: usize,
        deadline: Instant,
    ) -> Result<CloudVersion, Status> {
        let value: c::Version = value;
        if array::<32>(&value.workspace_id)? != id
            || value.generation == 0
            || generation.is_some_and(|generation| value.generation != generation)
            || value.manifest.len() > maximum
        {
            return Err(corrupt());
        }
        let generation = value.generation;
        let expected = CloudManifestId(array(&value.manifest_id)?);
        let previous = value
            .previous_manifest_id
            .map(|v| array(&v).map(CloudManifestId))
            .transpose()?;
        if (generation == 1) != previous.is_none() {
            return Err(corrupt());
        }
        blocking_owned(deadline, budget, None, move || {
            let version = CloudVersion {
                workspace_id: id,
                generation,
                manifest_id: expected,
                previous_manifest_id: previous,
                native_authority: value.native_authority,
                manifest: value.manifest,
                _owner: retained,
            };
            if CloudManifestId::of(version.manifest()) != expected {
                return Err(corrupt());
            }
            Ok(version)
        })
        .await
    }
}
