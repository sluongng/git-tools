//! Portable workspace-cloud transport. Raw digests and manifest IDs are checked;
//! native OP/VIEW semantic verification and manifest graph coverage remain the
//! caller's responsibility. Neither a receipt nor lookup establishes retention.
use super::*;
use crate::transport::Shape;
use serde::{Deserialize, Serialize};
use tinytree_proto::cloud_v1 as c;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct CloudLimits {
    pub max_object_bytes: usize,
    pub max_manifest_bytes: usize,
    pub max_lookup_items: usize,
    pub max_staging_leases: usize,
    pub max_chunk_bytes: usize,
}
impl Default for CloudLimits {
    fn default() -> Self {
        Self {
            max_object_bytes: 4 * 1024 * 1024,
            max_manifest_bytes: 8 * 1024 * 1024,
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
        cap.checked_mul(8)
            .and_then(|n| n.checked_add(128 * 1024))
            .filter(|n| {
                *n <= self.inner.config.limits.max_buffered_bytes && *n <= u32::MAX as usize
            })
            .ok_or_else(limit)
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
                "/tinytree.cloud.v1.WorkspaceCloud/PutObject",
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
                let mut stream = grpc
                    .cloud_read(
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
    async fn cloud_publication_at(
        &self,
        key_value: &str,
        deadline: Instant,
    ) -> Result<Option<CloudPublication>, Status> {
        key(key_value)?;
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
        self.cloud_publication_at(key_value, self.deadline()).await
    }
    pub async fn cloud_publish_version(
        &self,
        intent: &CloudPublishIntent,
    ) -> Result<CloudPublication, Status> {
        key(&intent.idempotency_key)?;
        let l = &self.inner.config.cloud;
        if intent.manifest.len() > l.max_manifest_bytes
            || intent.staging_leases.len() > l.max_staging_leases
            || intent.staging_leases.iter().any(|s| !lease(s))
        {
            return Err(limit());
        }
        let generation = intent
            .expected_generation
            .checked_add(1)
            .ok_or_else(|| Status::invalid_argument("workspace-cloud generation exhausted"))?;
        let bound = intent
            .manifest
            .len()
            .checked_add(intent.staging_leases.len() * 68)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(limit)?;
        let deadline = self.deadline();
        let verify_budget = self
            .cloud_admit(deadline, self.cloud_memory(bound)?)
            .await?;
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
                // Unknown outcome: reconcile the original key under a fresh
                // bounded operation deadline. Never manufacture a new intent.
                match self
                    .cloud_publication_at(&intent.idempotency_key, self.deadline())
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
        if generation == 0 {
            return Err(Status::invalid_argument(
                "cloud version generation must be positive",
            ));
        }
        let deadline = self.deadline();
        let l = &self.inner.config.cloud;
        self.cloud_payload_fits(
            l.max_manifest_bytes,
            self.cloud_memory(l.max_manifest_bytes + 4096)?,
        )?;
        let retained = self.cloud_retained(l.max_manifest_bytes)?;
        let (value, budget) = self
            .cloud_call(
                deadline,
                "/tinytree.cloud.v1.WorkspaceCloud/GetVersion",
                Shape::CloudVersion,
                l.max_manifest_bytes + 4096,
                4096,
                || c::GetVersionRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    workspace_id: id.to_vec(),
                    generation,
                },
            )
            .await?;
        let value: c::Version = value;
        if array::<32>(&value.workspace_id)? != id
            || value.generation != generation
            || value.manifest.len() > l.max_manifest_bytes
        {
            return Err(corrupt());
        }
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
