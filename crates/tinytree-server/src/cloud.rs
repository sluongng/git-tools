//! Private workspace transport. Admission charges declared worst-case work,
//! not measured RSS; canonical payloads and roots stay in the authoritative Store.
use crate::{RepositoryService, auth::Identity, config::ConfigError, service::RpcStream};
use prost::Message;
use std::{
    collections::HashMap,
    io::Read,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tinytree_proto::cloud_v1 as p;
use tinytree_store as s;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tonic::{Request, Response, Status, codec::CompressionEncoding};

#[derive(Clone, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CloudConfig {
    pub enabled: bool,
    pub indexed_versions: bool,
    pub captured_versions: bool,
    /// Advertise full Store-owned retained backing only when the effective
    /// backing implements it. Disabling this policy prevents new offloads;
    /// it never removes previously published dependencies or alters GC roots.
    pub durable_retained_offload: bool,
    pub max_capture_bytes: usize,
    pub max_native_proof_steps: usize,
    pub max_native_proof_bytes: u64,
    pub max_native_proof_edges: usize,
    pub max_object_bytes: usize,
    pub max_manifest_bytes: usize,
    pub max_lookup_items: usize,
    pub max_staging_leases: usize,
    pub max_request_bytes: usize,
    pub max_frame_bytes: usize,
    pub max_inflight: usize,
    pub max_inflight_per_repository: usize,
    pub max_buffered_bytes: u64,
}
impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            indexed_versions: false,
            captured_versions: false,
            durable_retained_offload: true,
            max_capture_bytes: 8 * 1024 * 1024,
            max_native_proof_steps: 256,
            max_native_proof_bytes: 64 * 1024 * 1024,
            max_native_proof_edges: 1_000_000,
            max_object_bytes: 4 * 1024 * 1024,
            max_manifest_bytes: 8 * 1024 * 1024,
            max_lookup_items: 256,
            max_staging_leases: 1024,
            max_request_bytes: 256 * 1024,
            max_frame_bytes: 64 * 1024,
            max_inflight: 2,
            max_inflight_per_repository: 1,
            max_buffered_bytes: 1024 * 1024 * 1024,
        }
    }
}
impl CloudConfig {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if (self.captured_versions && !self.indexed_versions)
            || !(1024..=8 * 1024 * 1024).contains(&self.max_capture_bytes)
            || !(1..=4096).contains(&self.max_native_proof_steps)
            || !(1024..=16 * 1024 * 1024 * 1024).contains(&self.max_native_proof_bytes)
            || !(1..=10_000_000).contains(&self.max_native_proof_edges)
            || !(1024..=64 * 1024 * 1024).contains(&self.max_object_bytes)
            || !(1024..=64 * 1024 * 1024).contains(&self.max_manifest_bytes)
            || !(1..=1024).contains(&self.max_lookup_items)
            || !(1..=65536).contains(&self.max_staging_leases)
            || !(1024..=1024 * 1024).contains(&self.max_request_bytes)
            || !(1024..=1024 * 1024).contains(&self.max_frame_bytes)
            || !(1..=64).contains(&self.max_inflight)
            || !(1..=self.max_inflight).contains(&self.max_inflight_per_repository)
            || !(1024 * 1024..=256 * 1024 * 1024 * 1024).contains(&self.max_buffered_bytes)
        {
            return Err(ConfigError("invalid private workspace service bounds"));
        }
        Ok(())
    }
    /// Pure preflight, also repeated against the effective already-open Store.
    pub(crate) fn validate_store_profile(&self, policy: &s::Config) -> Result<u64, ConfigError> {
        let profile = policy
            .cloud_resource_profile()
            .map_err(|_| ConfigError("private workspace resource bounds overflow"))?;
        let store_work = if self.indexed_versions {
            // Indexed reads can select an existing captured version even when
            // accepting new captured publications is disabled.
            profile
                .version_v1_bytes
                .max(profile.version_v2_bytes)
                .max(profile.version_v3_bytes)
        } else {
            profile.version_v1_bytes
        };
        let ordinary_work = profile.object_bytes;
        let maximum = [
            ("PublishVersion", store_work),
            ("GetVersion", store_work),
            ("GetLatestCapturedVersion", store_work),
            (
                "ReadCapture",
                if self.captured_versions {
                    profile.capture_read_bytes
                } else {
                    0
                },
            ),
            ("PutObject", ordinary_work),
            ("PutObjectRepresentation", ordinary_work),
            ("ReadObjectRepresentation", ordinary_work),
            ("LookupObjects", ordinary_work),
            (
                "ReadNativeClosure",
                if self.indexed_versions {
                    profile.native_read_bytes
                } else {
                    0
                },
            ),
            (
                "PutIndexArtifact",
                if self.indexed_versions {
                    profile.index_artifact_bytes
                } else {
                    0
                },
            ),
            (
                "ReadIndexArtifact",
                if self.indexed_versions {
                    profile.index_artifact_bytes
                } else {
                    0
                },
            ),
        ]
        .into_iter()
        .try_fold(0u64, |maximum, (method, store)| {
            let bytes = (self.wire_limit(method) as u64)
                .checked_mul(128)
                .and_then(|n| n.checked_add(crate::SERVER_CODEC_MEMORY_BYTES as u64))
                .and_then(|n| n.checked_add(store))
                .ok_or(ConfigError("private workspace memory bound"))?;
            Ok::<_, ConfigError>(maximum.max(bytes))
        })?;
        if kib(maximum)? as u64 > self.max_buffered_bytes / 1024 {
            return Err(ConfigError(
                "private workspace Store work exceeds configured service pool; configure an explicit bounded Store profile",
            ));
        }
        Ok(store_work)
    }
    pub(crate) fn wire_limit(&self, method: &str) -> usize {
        match method {
            "PutObject" | "PutObjectRepresentation" | "PutIndexArtifact" => {
                self.max_object_bytes.saturating_add(4096)
            }
            "PublishVersion" => self
                .max_manifest_bytes
                .saturating_add(if self.captured_versions {
                    self.max_capture_bytes
                } else {
                    0
                })
                .saturating_add(self.max_staging_leases.saturating_mul(128))
                .saturating_add(4096),
            _ => self.max_request_bytes,
        }
    }
}
fn admission_error(error: tokio::sync::TryAcquireError, message: &'static str) -> Status {
    match error {
        tokio::sync::TryAcquireError::NoPermits => reapi::policy::resource_exhausted_with_retry(
            message,
            std::time::Duration::from_millis(25),
        ),
        tokio::sync::TryAcquireError::Closed => {
            Status::unavailable("private workspace admission is closed")
        }
    }
}
fn kib(bytes: u64) -> Result<u32, ConfigError> {
    u32::try_from(
        bytes
            .checked_add(1023)
            .ok_or(ConfigError("private workspace memory bound"))?
            / 1024,
    )
    .map_err(|_| ConfigError("private workspace memory bound"))
}
pub(crate) struct CloudHost {
    pub config: CloudConfig,
    pub store: s::Store,
    store_work: u64,
    profile: s::CloudResourceProfile,
    calls: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    repositories: HashMap<String, Arc<Semaphore>>,
    active: AtomicUsize,
    changed: Notify,
    closing: AtomicBool,
    #[cfg(test)]
    pause: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
}
pub(crate) struct Work {
    _memory: OwnedSemaphorePermit,
    _call: OwnedSemaphorePermit,
    _repository: Mutex<Option<OwnedSemaphorePermit>>,
    pub deadline: Instant,
    _ticket: Ticket,
}
struct Ticket(Arc<CloudHost>);
impl Drop for Ticket {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
impl CloudHost {
    pub fn new(
        store: s::Store,
        config: CloudConfig,
        repositories: &[String],
    ) -> Result<Arc<Self>, ConfigError> {
        let profile = store
            .cloud_resource_profile()
            .map_err(|_| ConfigError("private workspace resource bounds overflow"))?;
        let policy = store.resource_config();
        let store_work = config.validate_store_profile(&policy)?;
        Ok(Arc::new(Self {
            calls: Arc::new(Semaphore::new(config.max_inflight)),
            bytes: Arc::new(Semaphore::new((config.max_buffered_bytes / 1024) as usize)),
            repositories: repositories
                .iter()
                .map(|repo| {
                    (
                        repo.clone(),
                        Arc::new(Semaphore::new(config.max_inflight_per_repository)),
                    )
                })
                .collect(),
            config,
            store,
            store_work,
            profile,
            active: AtomicUsize::new(0),
            changed: Notify::new(),
            closing: AtomicBool::new(false),
            #[cfg(test)]
            pause: Mutex::new(None),
        }))
    }
    pub fn admit(self: &Arc<Self>, method: &str, deadline: Instant) -> Result<Arc<Work>, Status> {
        if self.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("private workspace service is draining"));
        }
        let wire = self.config.wire_limit(method) as u64;
        if method == "ReadCapture" && !self.config.captured_versions {
            return Err(Status::unimplemented(
                "captured cloud versions are disabled",
            ));
        }
        if matches!(
            method,
            "PutIndexArtifact"
                | "LookupIndexArtifacts"
                | "ReadIndexArtifact"
                | "ReadNativeClosure"
                | "ReadCapture"
                | "OpenCloudScope"
        ) && !self.config.indexed_versions
        {
            return Err(Status::unimplemented(
                "indexed cloud artifacts are disabled",
            ));
        }
        let store = match method {
            "PublishVersion" | "GetVersion" | "GetLatestCapturedVersion" => self.store_work,
            "PutIndexArtifact" | "ReadIndexArtifact" => self.profile.index_artifact_bytes,
            "ReadNativeClosure" => self.profile.native_read_bytes,
            "ReadCapture" => self.profile.capture_read_bytes,
            "OpenCloudScope" => self
                .profile
                .version_v2_bytes
                .max(self.profile.version_v3_bytes),
            _ => self.profile.object_bytes,
        };
        let bytes = wire
            .checked_mul(128)
            .and_then(|n| n.checked_add(crate::SERVER_CODEC_MEMORY_BYTES as u64))
            .and_then(|n| n.checked_add(store))
            .ok_or_else(|| Status::resource_exhausted("private workspace memory bound"))?;
        let permits =
            kib(bytes).map_err(|_| Status::resource_exhausted("private workspace memory bound"))?;
        // A request that cannot fit even an empty pool is a permanent bound,
        // not transient contention. Never invite retries of the same shape.
        if permits as u64 > self.config.max_buffered_bytes / 1024 {
            return Err(Status::resource_exhausted("private workspace memory bound"));
        }
        let call =
            self.calls.clone().try_acquire_owned().map_err(|error| {
                admission_error(error, "private workspace call budget exhausted")
            })?;
        let memory = self
            .bytes
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|error| admission_error(error, "private workspace memory budget exhausted"))?;
        self.active.fetch_add(1, Ordering::AcqRel);
        // Close/admit linearization: a call which loses the race must not
        // become accepted after drain observes the previous zero count.
        if self.closing.load(Ordering::Acquire) {
            self.active.fetch_sub(1, Ordering::AcqRel);
            self.changed.notify_waiters();
            return Err(Status::unavailable("private workspace service is draining"));
        }
        Ok(Arc::new(Work {
            _memory: memory,
            _call: call,
            _repository: Mutex::new(None),
            deadline,
            _ticket: Ticket(self.clone()),
        }))
    }
    pub fn repository(
        &self,
        work: &Work,
        identity: &Identity,
        repo: &str,
        write: bool,
    ) -> Result<s::CloudOwner, Status> {
        identity.authorize_cloud(repo, write)?;
        let gate = self.repositories.get(repo).ok_or_else(|| {
            Status::permission_denied("private workspace repository access denied")
        })?;
        let permit = gate.clone().try_acquire_owned().map_err(|error| {
            admission_error(error, "private workspace repository budget exhausted")
        })?;
        *work
            ._repository
            .lock()
            .map_err(|_| Status::unavailable("private workspace service unavailable"))? =
            Some(permit);
        Ok(s::CloudOwner {
            principal: crate::retention::owner_hash(identity),
        })
    }
    #[cfg(test)]
    pub(crate) fn pause_job(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    ) {
        *self.pause.lock().unwrap() = Some((entered, resume));
    }
    #[cfg(test)]
    pub(crate) fn active_jobs(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
    pub async fn drain(&self) {
        self.closing.store(true, Ordering::Release);
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}
struct Owned<T> {
    value: T,
    _work: Arc<Work>,
}
pub(crate) async fn execute<T: Send + 'static>(
    work: Arc<Work>,
    f: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    let deadline = work.deadline;
    let job = tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        {
            let pause = work._ticket.0.pause.lock().unwrap().take();
            if let Some((entered, resume)) = pause {
                let _ = entered.send(());
                let _ = resume.recv();
            }
        }
        Owned {
            value: f(),
            _work: work,
        }
    });
    let output = tokio::time::timeout_at(deadline, job)
        .await
        .map_err(|_| Status::deadline_exceeded("private workspace deadline exceeded"))?
        .map_err(|_| Status::internal("private workspace worker failed"))?;
    output.value
}
#[derive(Clone)]
pub struct CloudService {
    repository: RepositoryService,
}
impl CloudService {
    pub(crate) fn new(repository: RepositoryService) -> Self {
        Self { repository }
    }
    pub(crate) fn grpc(self, cap: usize) -> p::workspace_cloud_server::WorkspaceCloudServer<Self> {
        let config = self.repository.service_config().cloud;
        let limit = config
            .max_manifest_bytes
            .saturating_add(4096)
            .max(config.max_frame_bytes.saturating_add(4096))
            .max(1024 * 1024);
        p::workspace_cloud_server::WorkspaceCloudServer::new(self)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(cap)
            .max_encoding_message_size(limit)
    }
    fn context<T: Message>(
        &self,
        request: &Request<T>,
        repo: &str,
        write: bool,
    ) -> Result<(Arc<CloudHost>, Arc<Work>, s::CloudOwner), Status> {
        let identity = self.repository.query_identity(request.metadata())?;
        let host = self
            .repository
            .cloud_host()
            .ok_or_else(|| Status::unavailable("private workspace service is disabled"))?;
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition("private workspace transport admission required")
            })?;
        let owner = host.repository(&work, &identity, repo, write)?;
        Ok((host, work, owner))
    }
}
fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], Status> {
    bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("invalid private workspace identifier length"))
}
fn id(value: Option<p::ObjectId>) -> Result<tinytree_cloud::ObjectId, Status> {
    let value =
        value.ok_or_else(|| Status::invalid_argument("missing private object identifier"))?;
    let raw = array(&value.semantic_id)?;
    match value.kind {
        1 => Ok(tinytree_cloud::ObjectId::Operation(raw)),
        2 => Ok(tinytree_cloud::ObjectId::View(raw)),
        _ => Err(Status::invalid_argument("invalid private object kind")),
    }
}
fn id_wire(id: tinytree_cloud::ObjectId) -> p::ObjectId {
    match id {
        tinytree_cloud::ObjectId::Operation(raw) => p::ObjectId {
            kind: 1,
            semantic_id: raw.to_vec(),
        },
        tinytree_cloud::ObjectId::View(raw) => p::ObjectId {
            kind: 2,
            semantic_id: raw.to_vec(),
        },
    }
}
fn descriptor(value: s::CloudObjectDescriptor) -> p::ObjectDescriptor {
    p::ObjectDescriptor {
        id: Some(id_wire(value.id)),
        selected: Some(p::RawDigest {
            blake3: value.selected.blake3.to_vec(),
            size_bytes: value.selected.size_bytes,
        }),
    }
}
fn index_id(value: Option<p::IndexArtifactId>) -> Result<tinytree_cloud::IndexArtifactId, Status> {
    let value =
        value.ok_or_else(|| Status::invalid_argument("missing index artifact identifier"))?;
    let raw = array(&value.semantic_id)?;
    match value.kind {
        1 => Ok(tinytree_cloud::IndexArtifactId::Root(
            tinytree_cloud::IndexRootId(raw),
        )),
        2 => Ok(tinytree_cloud::IndexArtifactId::Node(
            tinytree_cloud::IndexNodeId(raw),
        )),
        _ => Err(Status::invalid_argument("invalid index artifact kind")),
    }
}
fn index_descriptor(value: tinytree_cloud::IndexArtifactDescriptor) -> p::IndexArtifactDescriptor {
    let (kind, raw) = match value.id {
        tinytree_cloud::IndexArtifactId::Root(id) => (1, id.0),
        tinytree_cloud::IndexArtifactId::Node(id) => (2, id.0),
    };
    p::IndexArtifactDescriptor {
        id: Some(p::IndexArtifactId {
            kind,
            semantic_id: raw.to_vec(),
        }),
        selected: Some(p::RawDigest {
            blake3: value.selected.blake3.to_vec(),
            size_bytes: value.selected.size_bytes,
        }),
    }
}
fn workspace(value: s::CloudWorkspace) -> p::Workspace {
    p::Workspace {
        workspace_id: value.workspace.to_vec(),
        generation: value.generation,
        manifest_id: value.manifest.map(|id| id.0.to_vec()),
    }
}
fn publication(value: s::CloudPublication) -> p::Publication {
    p::Publication {
        idempotency_key: value.idempotency_key,
        workspace: Some(workspace(value.workspace)),
    }
}
fn native_key(
    value: Option<tinytree_proto::v1::ObjectKey>,
) -> Result<tinytree_model::ObjectKey, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("missing native object key"))?;
    let kind = u8::try_from(value.kind)
        .ok()
        .and_then(|kind| tinytree_model::Kind::try_from(kind).ok())
        .ok_or_else(|| Status::invalid_argument("invalid native object kind"))?;
    Ok(tinytree_model::ObjectKey {
        kind,
        id: tinytree_model::ObjectId(array(&value.native_id)?),
    })
}
fn native_descriptor(value: tinytree_model::Descriptor) -> tinytree_proto::v1::ObjectDescriptor {
    tinytree_proto::v1::ObjectDescriptor {
        key: Some(tinytree_proto::v1::ObjectKey {
            kind: value.key.kind as i32,
            native_id: value.key.id.0.to_vec(),
        }),
        content_blake3: value.key.id.0.to_vec(),
        size_bytes: value.size_bytes,
        object_format_version: value.format_version as u32,
    }
}
fn owner_binding(owner: s::CloudOwner, repo: &str) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-cloud-native-owner-v1\0");
    hash.update(&(repo.len() as u64).to_be_bytes());
    hash.update(repo.as_bytes());
    hash.update(&owner.principal);
    *hash.finalize().as_bytes()
}
pub(crate) fn checked_native_source(
    owner: s::CloudOwner,
    repo: &str,
    selector: &p::RetainedVersionSelector,
    authority: &p::NativeVersionAuthority,
) -> Result<(s::CloudVersionSelector, [u8; 32]), Status> {
    if selector.generation == 0 {
        return Err(Status::invalid_argument(
            "invalid retained version generation",
        ));
    }
    let workspace = array(&selector.workspace_id)?;
    let manifest = array(&selector.manifest_id)?;
    let selected = authority
        .manifest_selected
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing retained manifest selection"))?;
    let raw = array(&selected.blake3)?;
    let owner_binding = owner_binding(owner, repo);
    let binding = tinytree_model::cloud_native_binding_digest(
        repo,
        owner_binding,
        workspace,
        selector.generation,
        manifest,
        raw,
        selected.size_bytes,
    );
    if authority.version != 1
        || authority.owner_binding != owner_binding
        || authority.binding_digest != binding
    {
        return Err(Status::permission_denied(
            "retained version authority does not match authenticated source",
        ));
    }
    Ok((
        s::CloudVersionSelector {
            workspace,
            generation: selector.generation,
            manifest: tinytree_cloud::ManifestId(manifest),
            selected: tinytree_cloud::BlobDigest {
                blake3: raw,
                size_bytes: selected.size_bytes,
            },
        },
        binding,
    ))
}
fn native_authority(
    owner: s::CloudOwner,
    repo: &str,
    workspace: [u8; 32],
    generation: u64,
    manifest: [u8; 32],
    bytes: &[u8],
) -> p::NativeVersionAuthority {
    let owner_binding = owner_binding(owner, repo);
    let raw = *blake3::hash(bytes).as_bytes();
    let size = bytes.len() as u64;
    p::NativeVersionAuthority {
        version: 1,
        owner_binding: owner_binding.to_vec(),
        binding_digest: tinytree_model::cloud_native_binding_digest(
            repo,
            owner_binding,
            workspace,
            generation,
            manifest,
            raw,
            size,
        )
        .to_vec(),
        manifest_selected: Some(p::RawDigest {
            blake3: raw.to_vec(),
            size_bytes: size,
        }),
    }
}
fn status(error: s::Error) -> Status {
    if matches!(error, s::Error::Limit("cloud native read deadline")) {
        Status::deadline_exceeded("retained native read deadline exceeded")
    } else if let s::Error::Limit(limit) = error {
        use tinytree_proto::PublicationFailureReason as Reason;
        let reason = match limit {
            "cloud publication request" => Reason::StorePublicationRequest,
            "cloud capture attachment bytes" => Reason::StoreCaptureAttachmentBytes,
            "cloud validation catalog budget" => Reason::StoreValidationCatalogBudget,
            "cloud owner quota" => Reason::StoreOwnerQuota,
            "repository quota" => Reason::StoreRepositoryQuota,
            "cloud version decoded object budget" => Reason::StoreVersionDecodedObjectBudget,
            "cloud capture input memory" => Reason::StoreCaptureInputMemory,
            "cloud capture validation budget" => Reason::StoreCaptureValidationBudget,
            "cloud capture validation metadata" => Reason::StoreCaptureValidationMetadata,
            "cloud index validation budget" => Reason::StoreIndexValidationBudget,
            "cloud index native proof memory" => Reason::StoreIndexNativeProofMemory,
            "cloud index native proof steps" => Reason::StoreIndexNativeProofSteps,
            "cloud index proof work" => Reason::StoreIndexProofWork,
            "cloud validation catalog budget exceeded: records" => Reason::StoreValidationCatalogRecords,
            "cloud validation catalog budget exceeded: metadata" => Reason::StoreValidationCatalogMetadata,
            "cloud validation catalog budget exceeded: records_metadata" => Reason::StoreValidationCatalogRecordsMetadata,
            "cloud validation catalog budget exceeded: payload" => Reason::StoreValidationCatalogPayload,
            "cloud validation catalog budget exceeded: records_payload" => Reason::StoreValidationCatalogRecordsPayload,
            "cloud validation catalog budget exceeded: metadata_payload" => Reason::StoreValidationCatalogMetadataPayload,
            "cloud validation catalog budget exceeded: records_metadata_payload" => Reason::StoreValidationCatalogRecordsMetadataPayload,
            "cloud index proof work exceeded: records" => Reason::StoreIndexProofRecords,
            "cloud index proof work exceeded: metadata" => Reason::StoreIndexProofMetadata,
            "cloud index proof work exceeded: records_metadata" => Reason::StoreIndexProofRecordsMetadata,
            "cloud index proof work exceeded: payload" => Reason::StoreIndexProofPayload,
            "cloud index proof work exceeded: records_payload" => Reason::StoreIndexProofRecordsPayload,
            "cloud index proof work exceeded: metadata_payload" => Reason::StoreIndexProofMetadataPayload,
            "cloud index proof work exceeded: records_metadata_payload" => Reason::StoreIndexProofRecordsMetadataPayload,
            "cloud index artifact allocation" => Reason::StoreIndexArtifactAllocation,
            "cloud index artifact count" => Reason::StoreIndexArtifactCount,
            "cloud index loader memory" => Reason::StoreIndexLoaderMemory,
            "cloud index decoded allocation" => Reason::StoreIndexDecodedAllocation,
            "cloud index graph memory" => Reason::StoreIndexGraphMemory,
            "cloud frozen index memory" => Reason::StoreFrozenIndexMemory,
            "cloud frozen snapshot reconstruction memory" => Reason::StoreFrozenSnapshotMemory,
            "cloud capture graph memory" => Reason::StoreCaptureGraphMemory,
            "cloud capture binding memory" => Reason::StoreCaptureBindingMemory,
            "cloud selected anchors memory" => Reason::StoreSelectedAnchorsMemory,
            "cloud version member allocation" => Reason::StoreVersionMemberAllocation,
            "cloud version raw bytes" => Reason::StoreVersionRawBytes,
            "cloud catalog scan" => Reason::StoreCatalogScan,
            "cloud payload validation" => Reason::StorePayloadValidation,
            "cloud staging closure" => Reason::StoreStagingClosure,
            "cloud catalog references" => Reason::StoreCatalogReferences,
            "cloud retained version quota" => Reason::StoreRetainedVersionQuota,
            "cloud version validation metadata" => Reason::StoreVersionValidationMetadata,
            "cloud version validation bytes" => Reason::StoreVersionValidationBytes,
            "cloud workspace generation" => Reason::StoreWorkspaceGeneration,
            _ => Reason::UnknownStoreLimit,
        };
        Status::resource_exhausted(reason.message())
    } else {
        crate::service::store_status(error)
    }
}
fn now() -> Result<i64, Status> {
    crate::service::now()
}
fn response<T>(value: T, work: Arc<Work>, extra: impl Send + Sync + 'static) -> Response<T> {
    let mut response = Response::new(value);
    response
        .extensions_mut()
        .insert(crate::body_deadline::ResponseBudget::new((extra, work)));
    response
}

#[tonic::async_trait]
impl p::workspace_cloud_server::WorkspaceCloud for CloudService {
    async fn put_object(
        &self,
        request: Request<p::PutObjectRequest>,
    ) -> Result<Response<p::WriteReceipt>, Status> {
        let (host, work, owner) = self.context(&request, &request.get_ref().repository_id, true)?;
        let input = request.into_inner();
        if input.payload.len() > host.config.max_object_bytes {
            return Err(Status::resource_exhausted(
                "private object exceeds payload budget",
            ));
        }
        let d = input
            .descriptor
            .ok_or_else(|| Status::invalid_argument("missing private object descriptor"))?;
        let id = id(d.id)?;
        let digest = d
            .selected
            .ok_or_else(|| Status::invalid_argument("missing private object digest"))?;
        let digest = tinytree_cloud::BlobDigest {
            blake3: array(&digest.blake3)?,
            size_bytes: digest.size_bytes,
        };
        if digest.size_bytes != input.payload.len() as u64 {
            return Err(Status::invalid_argument("private object size mismatch"));
        }
        let store = host.store.clone();
        let receipt = execute(work.clone(), move || {
            store
                .put_cloud_object(
                    &input.repository_id,
                    owner,
                    id,
                    digest,
                    &input.payload,
                    now()?,
                    tinytree_cloud::Limits::default(),
                )
                .map_err(status)
        })
        .await?;
        Ok(response(
            p::WriteReceipt {
                descriptor: Some(descriptor(receipt.descriptor)),
                staging_lease: receipt.staging_lease,
                expires_at_unix_seconds: receipt.expires_at,
            },
            work,
            (),
        ))
    }
    async fn put_object_representation(
        &self,
        request: Request<p::PutObjectRequest>,
    ) -> Result<Response<p::WriteReceipt>, Status> {
        let (host, work, owner) = self.context(&request, &request.get_ref().repository_id, true)?;
        let input = request.into_inner();
        if input.payload.len() > host.config.max_object_bytes {
            return Err(Status::resource_exhausted(
                "private object exceeds payload budget",
            ));
        }
        let d = input
            .descriptor
            .ok_or_else(|| Status::invalid_argument("missing private object descriptor"))?;
        let id = id(d.id)?;
        let digest = d
            .selected
            .ok_or_else(|| Status::invalid_argument("missing private object digest"))?;
        let digest = tinytree_cloud::BlobDigest {
            blake3: array(&digest.blake3)?,
            size_bytes: digest.size_bytes,
        };
        if digest.size_bytes != input.payload.len() as u64 {
            return Err(Status::invalid_argument("private object size mismatch"));
        }
        let store = host.store.clone();
        let receipt = execute(work.clone(), move || {
            store
                .put_cloud_object_representation(
                    &input.repository_id,
                    owner,
                    id,
                    digest,
                    &input.payload,
                    now()?,
                    tinytree_cloud::Limits::default(),
                )
                .map_err(status)
        })
        .await?;
        Ok(response(
            p::WriteReceipt {
                descriptor: Some(descriptor(receipt.descriptor)),
                staging_lease: receipt.staging_lease,
                expires_at_unix_seconds: receipt.expires_at,
            },
            work,
            (),
        ))
    }
    async fn lookup_objects(
        &self,
        request: Request<p::LookupObjectsRequest>,
    ) -> Result<Response<p::LookupObjectsResponse>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        if input.ids.len() > host.config.max_lookup_items {
            return Err(Status::resource_exhausted(
                "private object lookup item budget",
            ));
        }
        let ids = input
            .ids
            .into_iter()
            .map(|value| id(Some(value)))
            .collect::<Result<Vec<_>, _>>()?;
        let store = host.store.clone();
        let values = execute(work.clone(), move || {
            store
                .lookup_cloud_objects(&input.repository_id, owner, &ids)
                .map_err(status)
        })
        .await?;
        Ok(response(
            p::LookupObjectsResponse {
                selections: values
                    .into_iter()
                    .map(|value| p::ObjectSelection {
                        descriptor: value.map(descriptor),
                    })
                    .collect(),
            },
            work,
            (),
        ))
    }
    type ReadObjectStream = RpcStream<p::ObjectChunk>;
    async fn read_object(
        &self,
        request: Request<p::ReadObjectRequest>,
    ) -> Result<Response<Self::ReadObjectStream>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        let id = id(input.id)?;
        let store = host.store.clone();
        let mut limits = tinytree_cloud::Limits::default();
        limits.max_payload_bytes = host.config.max_object_bytes;
        let lease = execute(work.clone(), move || {
            store
                .read_cloud_object(&input.repository_id, owner, id, limits)
                .map_err(status)
        })
        .await?;
        let descriptor = descriptor(lease.descriptor());
        let lease = Arc::new(Mutex::new(lease));
        let held = lease.clone();
        let stream_work = work.clone();
        let frame = host.config.max_frame_bytes;
        let stream = async_stream::try_stream! {
            let mut offset=0u64;
            loop {
                let lease=lease.clone();
                let bytes=execute(stream_work.clone(),move||{
                    let mut bytes=vec![0;frame];
                    let n=lease.lock().map_err(|_|Status::internal("private object read failed"))?.read(&mut bytes).map_err(|_|Status::data_loss("private object payload unavailable"))?;
                    bytes.truncate(n);Ok(bytes)
                }).await?;
                let n=bytes.len();let eof=offset+n as u64==descriptor.selected.as_ref().unwrap().size_bytes;
                if n==0 && !eof {Err(Status::data_loss("private object payload truncated"))?;}
                yield p::ObjectChunk{descriptor:Some(descriptor.clone()),offset,data:bytes,eof};
                offset+=n as u64;
                if eof{break;}
            }
        };
        Ok(response(
            Box::pin(stream) as Self::ReadObjectStream,
            work,
            held,
        ))
    }
    type ReadObjectRepresentationStream = RpcStream<p::ObjectChunk>;
    async fn read_object_representation(
        &self,
        request: Request<p::ReadObjectRepresentationRequest>,
    ) -> Result<Response<Self::ReadObjectRepresentationStream>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        let d = input
            .descriptor
            .ok_or_else(|| Status::invalid_argument("missing private representation descriptor"))?;
        let id = id(d.id)?;
        let raw = d
            .selected
            .ok_or_else(|| Status::invalid_argument("missing private representation digest"))?;
        let exact = s::CloudObjectDescriptor {
            id,
            selected: tinytree_cloud::BlobDigest {
                blake3: array(&raw.blake3)?,
                size_bytes: raw.size_bytes,
            },
        };
        let store = host.store.clone();
        let mut limits = tinytree_cloud::Limits::default();
        limits.max_payload_bytes = host.config.max_object_bytes;
        let lease = execute(work.clone(), move || {
            store
                .read_cloud_object_representation(&input.repository_id, owner, exact, limits)
                .map_err(status)
        })
        .await?;
        let descriptor = descriptor(lease.descriptor());
        let lease = Arc::new(Mutex::new(lease));
        let held = lease.clone();
        let stream_work = work.clone();
        let frame = host.config.max_frame_bytes;
        let stream = async_stream::try_stream! {
            let mut offset=0u64;
            loop {
                let lease=lease.clone();
                let bytes=execute(stream_work.clone(),move||{
                    let mut bytes=vec![0;frame];
                    let n=lease.lock().map_err(|_|Status::internal("private object read failed"))?.read(&mut bytes).map_err(|_|Status::data_loss("private object payload unavailable"))?;
                    bytes.truncate(n);Ok(bytes)
                }).await?;
                let n=bytes.len();let eof=offset+n as u64==descriptor.selected.as_ref().unwrap().size_bytes;
                if n==0 && !eof {Err(Status::data_loss("private object payload truncated"))?;}
                yield p::ObjectChunk{descriptor:Some(descriptor.clone()),offset,data:bytes,eof};
                offset+=n as u64;
                if eof{break;}
            }
        };
        Ok(response(
            Box::pin(stream) as Self::ReadObjectRepresentationStream,
            work,
            held,
        ))
    }
    async fn put_index_artifact(
        &self,
        request: Request<p::PutIndexArtifactRequest>,
    ) -> Result<Response<p::IndexArtifactWriteReceipt>, Status> {
        let (host, work, owner) = self.context(&request, &request.get_ref().repository_id, true)?;
        let input = request.into_inner();
        if input.payload.len() > host.config.max_object_bytes.min(4 * 1024 * 1024) {
            return Err(Status::resource_exhausted(
                "private object exceeds payload budget",
            ));
        }
        let d = input
            .descriptor
            .ok_or_else(|| Status::invalid_argument("missing private object descriptor"))?;
        let id = index_id(d.id)?;
        let digest = d
            .selected
            .ok_or_else(|| Status::invalid_argument("missing private object digest"))?;
        let digest = tinytree_cloud::BlobDigest {
            blake3: array(&digest.blake3)?,
            size_bytes: digest.size_bytes,
        };
        if digest.size_bytes != input.payload.len() as u64 {
            return Err(Status::invalid_argument("private object size mismatch"));
        }
        let store = host.store.clone();
        let receipt = execute(work.clone(), move || {
            store
                .put_cloud_index_artifact(
                    &input.repository_id,
                    owner,
                    tinytree_cloud::IndexArtifactDescriptor {
                        id,
                        selected: digest,
                    },
                    &input.payload,
                    now()?,
                    tinytree_cloud::IndexArtifactLimits::default(),
                )
                .map_err(status)
        })
        .await?;
        Ok(response(
            p::IndexArtifactWriteReceipt {
                descriptor: Some(index_descriptor(receipt.descriptor)),
                staging_lease: receipt.staging_lease,
                expires_at_unix_seconds: receipt.expires_at,
            },
            work,
            (),
        ))
    }
    async fn lookup_index_artifacts(
        &self,
        request: Request<p::LookupIndexArtifactsRequest>,
    ) -> Result<Response<p::LookupIndexArtifactsResponse>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        if input.ids.len() > host.config.max_lookup_items {
            return Err(Status::resource_exhausted(
                "private object lookup item budget",
            ));
        }
        let ids = input
            .ids
            .into_iter()
            .map(|value| index_id(Some(value)))
            .collect::<Result<Vec<_>, _>>()?;
        let store = host.store.clone();
        let values = execute(work.clone(), move || {
            store
                .lookup_cloud_index_artifacts(&input.repository_id, owner, &ids)
                .map_err(status)
        })
        .await?;
        Ok(response(
            p::LookupIndexArtifactsResponse {
                selections: values
                    .into_iter()
                    .map(|value| p::IndexArtifactSelection {
                        descriptor: value.map(index_descriptor),
                    })
                    .collect(),
            },
            work,
            (),
        ))
    }
    type ReadIndexArtifactStream = RpcStream<p::IndexArtifactChunk>;
    async fn read_index_artifact(
        &self,
        request: Request<p::ReadIndexArtifactRequest>,
    ) -> Result<Response<Self::ReadIndexArtifactStream>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        let id = index_id(input.id)?;
        let store = host.store.clone();
        let mut limits = tinytree_cloud::IndexArtifactLimits::default();
        limits.max_artifact_bytes = host.config.max_object_bytes.min(4 * 1024 * 1024);
        let lease = execute(work.clone(), move || {
            store
                .read_cloud_index_artifact(&input.repository_id, owner, id, limits)
                .map_err(status)
        })
        .await?;
        let descriptor = index_descriptor(lease.descriptor());
        let lease = Arc::new(Mutex::new(lease));
        let held = lease.clone();
        let stream_work = work.clone();
        let frame = host.config.max_frame_bytes;
        let stream = async_stream::try_stream! {
            let mut offset=0u64;
            loop {
                let lease=lease.clone();
                let bytes=execute(stream_work.clone(),move||{
                    let mut bytes=vec![0;frame];
                    let n=lease.lock().map_err(|_|Status::internal("private object read failed"))?.read(&mut bytes).map_err(|_|Status::data_loss("private object payload unavailable"))?;
                    bytes.truncate(n);Ok(bytes)
                }).await?;
                let n=bytes.len();let eof=offset+n as u64==descriptor.selected.as_ref().unwrap().size_bytes;
                if n==0 && !eof {Err(Status::data_loss("private object payload truncated"))?;}
                yield p::IndexArtifactChunk{descriptor:Some(descriptor.clone()),offset,data:bytes,eof};
                offset+=n as u64;
                if eof{break;}
            }
        };
        Ok(response(
            Box::pin(stream) as Self::ReadIndexArtifactStream,
            work,
            held,
        ))
    }
    type ReadCaptureStream = RpcStream<p::CaptureChunk>;
    async fn read_capture(
        &self,
        request: Request<p::ReadCaptureRequest>,
    ) -> Result<Response<Self::ReadCaptureStream>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        let selector = input
            .selector
            .ok_or_else(|| Status::invalid_argument("missing retained version selector"))?;
        let selected = input
            .manifest_selected
            .ok_or_else(|| Status::invalid_argument("missing retained manifest selection"))?;
        if selector.generation == 0 || selected.size_bytes == 0 {
            return Err(Status::invalid_argument(
                "invalid retained version selection",
            ));
        }
        if selected.size_bytes > host.config.max_manifest_bytes as u64 {
            return Err(Status::resource_exhausted(
                "retained manifest exceeds budget",
            ));
        }
        let expected = tinytree_cloud::CaptureDataId(array(&input.expected_capture_id)?);
        let selection = s::CloudVersionSelector {
            workspace: array(&selector.workspace_id)?,
            generation: selector.generation,
            manifest: tinytree_cloud::ManifestId(array(&selector.manifest_id)?),
            selected: tinytree_cloud::BlobDigest {
                blake3: array(&selected.blake3)?,
                size_bytes: selected.size_bytes,
            },
        };
        let manifest_id = selection.manifest.0;
        let store = host.store.clone();
        let lease = execute(work.clone(), move || {
            store
                .read_cloud_capture(&input.repository_id, owner, selection, expected)
                .map_err(status)
        })
        .await?;
        let capture = lease.descriptor();
        if capture.selected.size_bytes > host.config.max_capture_bytes as u64 {
            return Err(Status::resource_exhausted(
                "capture attachment exceeds budget",
            ));
        }
        let descriptor = p::CaptureDescriptor {
            capture_id: capture.id.0.to_vec(),
            selected: Some(p::RawDigest {
                blake3: capture.selected.blake3.to_vec(),
                size_bytes: capture.selected.size_bytes,
            }),
        };
        let lease = Arc::new(Mutex::new(lease));
        let held = lease.clone();
        let stream_work = work.clone();
        let frame = host.config.max_frame_bytes;
        let stream = async_stream::try_stream! {
            let mut offset = 0u64;
            loop {
                let lease = lease.clone();
                let bytes = execute(stream_work.clone(), move || {
                    let mut bytes = vec![0; frame];
                    let n = lease.lock()
                        .map_err(|_| Status::internal("capture read unavailable"))?
                        .read(&mut bytes)
                        .map_err(|_| Status::data_loss("capture payload unavailable"))?;
                    bytes.truncate(n);
                    Ok(bytes)
                }).await?;
                let end = offset.checked_add(bytes.len() as u64)
                    .ok_or_else(|| Status::data_loss("capture stream offset overflow"))?;
                let size = capture.selected.size_bytes;
                let eof = end == size;
                if end > size || (bytes.is_empty() && !eof) {
                    Err(Status::data_loss("capture payload length mismatch"))?;
                }
                yield p::CaptureChunk {
                    descriptor: Some(descriptor.clone()),
                    offset,
                    data: bytes,
                    eof,
                    manifest_id: manifest_id.to_vec(),
                };
                offset = end;
                if eof { break; }
            }
        };
        Ok(response(
            Box::pin(stream) as Self::ReadCaptureStream,
            work,
            held,
        ))
    }
    async fn publish_version(
        &self,
        request: Request<p::PublishVersionRequest>,
    ) -> Result<Response<p::Publication>, Status> {
        let (host, work, owner) = self.context(&request, &request.get_ref().repository_id, true)?;
        let input = request.into_inner();
        use tinytree_proto::PublicationFailureReason as Reason;
        if input.manifest.len() > host.config.max_manifest_bytes {
            return Err(Status::resource_exhausted(Reason::ManifestBytes.message()));
        }
        if input.capture_data.len() > host.config.max_capture_bytes {
            return Err(Status::resource_exhausted(Reason::CaptureBytes.message()));
        }
        if input.staging_leases.len() > host.config.max_staging_leases {
            return Err(Status::resource_exhausted(Reason::StagingLeaseCount.message()));
        }
        // The opt-in reserves the complete indexed Store profile before decode.
        let indexed_format = input.manifest.get(..8) == Some(b"TTCLOUD\0".as_slice())
            && matches!(input.manifest.get(8..10), Some([2, 0] | [3, 0]));
        let captured_format = input.manifest.get(..8) == Some(b"TTCLOUD\0".as_slice())
            && input.manifest.get(8..10) == Some([3u8, 0].as_slice());
        if !host.config.indexed_versions && indexed_format {
            return Err(Status::unimplemented(
                "indexed cloud manifests are not enabled by this RPC adapter",
            ));
        }
        let captured = captured_format || !input.capture_data.is_empty();
        if captured && !host.config.captured_versions {
            return Err(Status::unimplemented(
                "captured cloud versions are disabled",
            ));
        }
        let publish = s::PublishCloudRequest {
            workspace: array(&input.workspace_id)?,
            expected_generation: input.expected_generation,
            idempotency_key: input.idempotency_key,
            manifest: tinytree_cloud::ManifestId(array(&input.manifest_id)?),
            manifest_bytes: input.manifest,
            staging_leases: input.staging_leases,
        };
        let store = host.store.clone();
        let retention = self.repository.service_config().retention;
        let limits = s::CloudPublicationLimits {
            retention,
            ..Default::default()
        };
        let value = execute(work.clone(), move || {
            if captured {
                store.publish_captured_cloud_version(
                    &input.repository_id,
                    owner,
                    &s::PublishCapturedCloudRequest {
                        publication: publish,
                        capture_data: input.capture_data,
                    },
                    now()?,
                    &limits,
                )
            } else {
                store.publish_cloud_version(&input.repository_id, owner, &publish, now()?, &limits)
            }
            .map_err(status)
        })
        .await?;
        Ok(response(publication(value), work, ()))
    }
    async fn get_workspace(
        &self,
        request: Request<p::GetWorkspaceRequest>,
    ) -> Result<Response<p::Workspace>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        let workspace_id = array(&input.workspace_id)?;
        let store = host.store.clone();
        let value = execute(work.clone(), move || {
            store
                .get_cloud_workspace(&input.repository_id, owner, workspace_id)
                .map_err(status)
        })
        .await?;
        Ok(response(workspace(value), work, ()))
    }
    async fn list_workspaces(
        &self,
        request: Request<p::ListWorkspacesRequest>,
    ) -> Result<Response<p::ListWorkspacesResponse>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        let store = host.store.clone();
        let page = execute(work.clone(), move || {
            store
                .list_cloud_workspaces(
                    &input.repository_id,
                    owner,
                    input.page_size as usize,
                    &input.continuation,
                )
                .map_err(status)
        })
        .await?;
        Ok(response(
            p::ListWorkspacesResponse {
                workspaces: page.workspaces.into_iter().map(workspace).collect(),
                continuation: page.continuation,
            },
            work,
            (),
        ))
    }
    async fn list_versions(
        &self,
        request: Request<p::ListVersionsRequest>,
    ) -> Result<Response<p::ListVersionsResponse>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        let id = array(&input.workspace_id)?;
        let store = host.store.clone();
        let page = execute(work.clone(), move || {
            store
                .list_cloud_versions(
                    &input.repository_id,
                    owner,
                    id,
                    input.page_size as usize,
                    &input.continuation,
                )
                .map_err(status)
        })
        .await?;
        Ok(response(
            p::ListVersionsResponse {
                workspace: Some(workspace(page.workspace)),
                versions: page
                    .versions
                    .into_iter()
                    .map(|v| p::VersionSummary {
                        workspace_id: v.workspace.to_vec(),
                        generation: v.generation,
                        manifest_id: v.manifest.0.to_vec(),
                        previous_manifest_id: v.previous.map(|id| id.0.to_vec()),
                    })
                    .collect(),
                continuation: page.continuation,
                through_generation: page.through_generation,
            },
            work,
            (),
        ))
    }
    async fn get_version(
        &self,
        request: Request<p::GetVersionRequest>,
    ) -> Result<Response<p::Version>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        let workspace_id = array(&input.workspace_id)?;
        let store = host.store.clone();
        let mut limits = tinytree_cloud::ManifestLimits::default();
        limits.max_manifest_bytes = host.config.max_manifest_bytes;
        let indexed_versions = host.config.indexed_versions;
        let authority_repo = input.repository_id.clone();
        let value = execute(work.clone(), move || {
            if indexed_versions {
                store
                    .read_cloud_version(
                        &input.repository_id,
                        owner,
                        workspace_id,
                        input.generation,
                        limits,
                    )
                    .map(Some)
                    .map_err(status)
            } else {
                store
                    .read_cloud_version_v1(
                        &input.repository_id,
                        owner,
                        workspace_id,
                        input.generation,
                        limits,
                    )
                    .map_err(status)
            }
        })
        .await?
        .ok_or_else(|| {
            Status::unimplemented("indexed cloud manifests are not enabled by this RPC adapter")
        })?;
        let version = value.version();
        let output = p::Version {
            workspace_id: version.workspace.to_vec(),
            generation: version.generation,
            manifest_id: version.manifest.0.to_vec(),
            previous_manifest_id: version.previous.map(|id| id.0.to_vec()),
            manifest: value.manifest_bytes().to_vec(),
            native_authority: indexed_versions.then(|| {
                native_authority(
                    owner,
                    &authority_repo,
                    version.workspace,
                    version.generation,
                    version.manifest.0,
                    value.manifest_bytes(),
                )
            }),
        };
        Ok(response(output, work, value))
    }
    async fn get_latest_captured_version(
        &self,
        request: Request<p::GetLatestCapturedVersionRequest>,
    ) -> Result<Response<p::Version>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        if !host.config.indexed_versions || !host.config.captured_versions {
            return Err(Status::unimplemented(
                "captured cloud versions are not enabled",
            ));
        }
        let input = request.into_inner();
        let workspace_id = array(&input.workspace_id)?;
        let store = host.store.clone();
        let mut limits = tinytree_cloud::ManifestLimits::default();
        limits.max_manifest_bytes = host.config.max_manifest_bytes;
        let authority_repo = input.repository_id.clone();
        let deadline = work.deadline.into_std();
        let value = execute(work.clone(), move || {
            store
                .read_latest_captured_cloud_version(
                    &input.repository_id,
                    owner,
                    workspace_id,
                    limits,
                    deadline,
                )
                .map_err(|error| match error {
                    s::Error::Invalid(
                        "ambiguous latest capture exporter" | "ambiguous latest capture sequence",
                    ) => Status::failed_precondition(
                        "latest capture is ambiguous; select an explicit generation",
                    ),
                    s::Error::Limit("latest capture deadline") => {
                        Status::deadline_exceeded("latest capture deadline")
                    }
                    error => status(error),
                })
        })
        .await?
        .ok_or_else(|| Status::not_found("no captured cloud version"))?;
        let version = value.version();
        let output = p::Version {
            workspace_id: version.workspace.to_vec(),
            generation: version.generation,
            manifest_id: version.manifest.0.to_vec(),
            previous_manifest_id: version.previous.map(|id| id.0.to_vec()),
            manifest: value.manifest_bytes().to_vec(),
            native_authority: Some(native_authority(
                owner,
                &authority_repo,
                version.workspace,
                version.generation,
                version.manifest.0,
                value.manifest_bytes(),
            )),
        };
        Ok(response(output, work, value))
    }
    type ReadNativeClosureStream = RpcStream<p::NativeClosureChunk>;
    async fn read_native_closure(
        &self,
        request: Request<p::ReadNativeClosureRequest>,
    ) -> Result<Response<Self::ReadNativeClosureStream>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        let selector = input
            .selector
            .ok_or_else(|| Status::invalid_argument("missing retained version selector"))?;
        let workspace = array(&selector.workspace_id)?;
        let manifest = array(&selector.manifest_id)?;
        if selector.generation == 0 {
            return Err(Status::invalid_argument(
                "invalid retained version generation",
            ));
        }
        let authority = input
            .authority
            .ok_or_else(|| Status::failed_precondition("retained native authority is required"))?;
        let selected = authority
            .manifest_selected
            .ok_or_else(|| Status::invalid_argument("missing retained manifest selection"))?;
        let raw = array(&selected.blake3)?;
        let owner_binding = owner_binding(owner, &input.repository_id);
        let binding = tinytree_model::cloud_native_binding_digest(
            &input.repository_id,
            owner_binding,
            workspace,
            selector.generation,
            manifest,
            raw,
            selected.size_bytes,
        );
        if authority.version != 1
            || authority.owner_binding != owner_binding
            || authority.binding_digest != binding
        {
            return Err(Status::permission_denied(
                "retained version authority does not match authenticated source",
            ));
        }
        let requested = native_key(input.requested)?;
        let proof = input
            .proof
            .ok_or_else(|| Status::invalid_argument("missing native closure proof"))?;
        if proof.edge_path.len() > host.config.max_native_proof_steps {
            return Err(Status::resource_exhausted(
                "native proof path exceeds budget",
            ));
        }
        enum NativeTerminal {
            VersionRoot(tinytree_model::ObjectKey),
            SnapshotCommit(Vec<tinytree_cloud::IndexRootId>, tinytree_model::CommitId),
        }
        let terminal = match proof.terminal {
            Some(p::native_closure_proof::Terminal::VersionRoot(value)) => {
                NativeTerminal::VersionRoot(native_key(Some(value))?)
            }
            Some(p::native_closure_proof::Terminal::SnapshotCommit(value)) => {
                if value.index_root_ids.len() > host.config.max_native_proof_steps {
                    return Err(Status::resource_exhausted(
                        "snapshot terminal exceeds budget",
                    ));
                }
                if value.index_root_ids.is_empty()
                    || value
                        .index_root_ids
                        .windows(2)
                        .any(|pair| pair[0] >= pair[1])
                {
                    return Err(Status::invalid_argument(
                        "snapshot roots must be sorted unique",
                    ));
                }
                let commit = tinytree_model::CommitId(array(&value.commit_id)?);
                for id in &value.index_root_ids {
                    let _: [u8; 32] = array(id)?;
                }
                NativeTerminal::SnapshotCommit(
                    value
                        .index_root_ids
                        .iter()
                        .map(|id| array(id).map(tinytree_cloud::IndexRootId))
                        .collect::<Result<Vec<_>, _>>()?,
                    commit,
                )
            }
            None => return Err(Status::invalid_argument("missing native proof terminal")),
        };
        let path = proof
            .edge_path
            .into_iter()
            .map(|key| native_key(Some(key)))
            .collect::<Result<Vec<_>, _>>()?;
        let store = host.store.clone();
        let limits = s::CloudNativeReadLimits {
            max_path_steps: host.config.max_native_proof_steps,
            max_metadata_bytes: host.config.max_native_proof_bytes,
            max_edges: host.config.max_native_proof_edges,
            deadline: Some(work.deadline.into_std()),
        };
        let lease = execute(work.clone(), move || {
            let selector = s::CloudVersionSelector {
                workspace,
                generation: selector.generation,
                manifest: tinytree_cloud::ManifestId(manifest),
                selected: tinytree_cloud::BlobDigest {
                    blake3: raw,
                    size_bytes: selected.size_bytes,
                },
            };
            match terminal {
                NativeTerminal::VersionRoot(root) => store.read_cloud_native(
                    &input.repository_id,
                    owner,
                    selector,
                    requested,
                    root,
                    &path,
                    limits,
                ),
                NativeTerminal::SnapshotCommit(roots, commit) => store.read_cloud_snapshot_native(
                    &input.repository_id,
                    owner,
                    selector,
                    requested,
                    &roots,
                    commit,
                    &path,
                    limits,
                ),
            }
            .map_err(status)
        })
        .await?;
        let descriptor = native_descriptor(lease.descriptor());
        let lease = Arc::new(Mutex::new(lease));
        let held = lease.clone();
        let stream_work = work.clone();
        let frame = host.config.max_frame_bytes;
        let stream = async_stream::try_stream! {
            let mut offset = 0u64;
            loop {
                let lease = lease.clone();
                let bytes = execute(stream_work.clone(), move || {
                    let mut bytes = vec![0; frame];
                    let n = lease.lock().map_err(|_| Status::internal("retained native read failed"))?
                        .read(&mut bytes).map_err(|_| Status::data_loss("retained native payload unavailable"))?;
                    bytes.truncate(n); Ok(bytes)
                }).await?;
                let n = bytes.len();
                let eof = offset + n as u64 == descriptor.size_bytes;
                if n == 0 && !eof { Err(Status::data_loss("retained native payload truncated"))?; }
                yield p::NativeClosureChunk { descriptor: Some(descriptor.clone()), offset, data: bytes,
                    eof, source_binding_digest: binding.to_vec() };
                offset += n as u64;
                if eof { break; }
            }
        };
        Ok(response(
            Box::pin(stream) as Self::ReadNativeClosureStream,
            work,
            held,
        ))
    }
    async fn get_publication(
        &self,
        request: Request<p::GetPublicationRequest>,
    ) -> Result<Response<p::GetPublicationResponse>, Status> {
        let (host, work, owner) =
            self.context(&request, &request.get_ref().repository_id, false)?;
        let input = request.into_inner();
        let store = host.store.clone();
        let value = execute(work.clone(), move || {
            store
                .cloud_publication(&input.repository_id, owner, &input.idempotency_key)
                .map_err(status)
        })
        .await?;
        Ok(response(
            p::GetPublicationResponse {
                publication: value.map(publication),
            },
            work,
            (),
        ))
    }
}

#[cfg(test)]
mod indexed_adapter_tests {
    use super::*;
    use p::workspace_cloud_server::WorkspaceCloud;
    #[tokio::test]
    async fn cloud_memory_and_repository_pressure_are_retryable_but_store_limits_are_not() {
        let directory = tempfile::tempdir().unwrap();
        let mut profile: crate::Config =
            toml::from_str(include_str!("../examples/cloud.toml")).unwrap();
        profile.service.cloud.max_inflight = 8;
        let store = s::Store::open(
            directory.path(),
            profile
                .store
                .materialize(profile.service.max_metadata_bytes),
        )
        .unwrap();
        store.create_repository("example").unwrap();
        let repository =
            RepositoryService::new(store, profile.service, vec!["example".into()], Vec::new())
                .unwrap();
        let host = repository.cloud_host().unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        let mut admitted = Vec::new();
        loop {
            match host.admit("GetVersion", deadline) {
                Ok(work) => admitted.push(work),
                Err(error) => {
                    assert_eq!(error.message(), "private workspace memory budget exhausted");
                    assert!(reapi::policy::retryable(&error));
                    assert!(!error.details().is_empty());
                    break;
                }
            }
        }
        assert!(!admitted.is_empty());
        assert_eq!(host.active_jobs(), admitted.len());
        drop(admitted);
        assert_eq!(host.active_jobs(), 0);
        let identity = repository.query_identity(&Default::default()).unwrap();
        let first = host.admit("GetWorkspace", deadline).unwrap();
        host.repository(&first, &identity, "example", false)
            .unwrap();
        let second = host.admit("GetWorkspace", deadline).unwrap();
        let error = host
            .repository(&second, &identity, "example", false)
            .unwrap_err();
        assert_eq!(
            error.message(),
            "private workspace repository budget exhausted"
        );
        assert!(reapi::policy::retryable(&error));
        drop(second);
        drop(first);
        assert_eq!(host.active_jobs(), 0);
        let next = host.admit("GetWorkspace", deadline).unwrap();
        host.repository(&next, &identity, "example", false).unwrap();
        drop(next);
        assert_eq!(host.active_jobs(), 0);
        let permanent = status(s::Error::Limit("cloud staging quota"));
        assert_eq!(permanent.code(), tonic::Code::ResourceExhausted);
        assert!(permanent.details().is_empty());
        assert!(!reapi::policy::retryable(&permanent));
        use tinytree_proto::PublicationFailureReason as Reason;
        for (limit, expected) in [
            ("cloud validation catalog budget", Reason::StoreValidationCatalogBudget),
            ("cloud version decoded object budget", Reason::StoreVersionDecodedObjectBudget),
            ("cloud validation catalog budget exceeded: records", Reason::StoreValidationCatalogRecords),
            ("cloud validation catalog budget exceeded: metadata", Reason::StoreValidationCatalogMetadata),
            ("cloud validation catalog budget exceeded: records_metadata", Reason::StoreValidationCatalogRecordsMetadata),
            ("cloud validation catalog budget exceeded: payload", Reason::StoreValidationCatalogPayload),
            ("cloud validation catalog budget exceeded: records_payload", Reason::StoreValidationCatalogRecordsPayload),
            ("cloud validation catalog budget exceeded: metadata_payload", Reason::StoreValidationCatalogMetadataPayload),
            ("cloud validation catalog budget exceeded: records_metadata_payload", Reason::StoreValidationCatalogRecordsMetadataPayload),
            ("cloud index proof work exceeded: records", Reason::StoreIndexProofRecords),
            ("cloud index proof work exceeded: metadata", Reason::StoreIndexProofMetadata),
            ("cloud index proof work exceeded: records_metadata", Reason::StoreIndexProofRecordsMetadata),
            ("cloud index proof work exceeded: payload", Reason::StoreIndexProofPayload),
            ("cloud index proof work exceeded: records_payload", Reason::StoreIndexProofRecordsPayload),
            ("cloud index proof work exceeded: metadata_payload", Reason::StoreIndexProofMetadataPayload),
            ("cloud index proof work exceeded: records_metadata_payload", Reason::StoreIndexProofRecordsMetadataPayload),
            ("synthetic-sensitive-marker /private/fixture token=not-real", Reason::UnknownStoreLimit),
        ] {
            let failure = status(s::Error::Limit(limit));
            assert_eq!(failure.code(), tonic::Code::ResourceExhausted);
            assert_eq!(Reason::from_message(failure.message()), Some(expected));
            assert!(failure.details().is_empty());
            assert!(!reapi::policy::retryable(&failure));
            assert!(!format!("{failure:?}").contains("synthetic-sensitive-marker"));
            assert!(!failure.message().contains("/private/"));
        }
    }
    #[tokio::test]
    async fn legacy_rpc_rejects_indexed_publish_and_read_before_artifact_work() {
        let directory = tempfile::tempdir().unwrap();
        let profile: crate::Config =
            toml::from_str(include_str!("../examples/cloud.toml")).unwrap();
        let store = s::Store::open(
            directory.path(),
            profile
                .store
                .materialize(profile.service.max_metadata_bytes),
        )
        .unwrap();
        store.create_repository("example").unwrap();
        let repository = RepositoryService::new(
            store.clone(),
            profile.service,
            vec!["example".into()],
            Vec::new(),
        )
        .unwrap();
        let identity = repository.query_identity(&Default::default()).unwrap();
        let owner = s::CloudOwner {
            principal: crate::retention::owner_hash(&identity),
        };
        let indexed = tinytree_cloud::build_index_artifacts(
            &tinytree_cloud::PortableIndexSnapshot {
                roots: vec![tinytree_model::CommitId::ROOT],
                drafts: vec![],
            },
            Default::default(),
        )
        .unwrap();
        for artifact in &indexed.artifacts {
            store
                .put_cloud_index_artifact(
                    "example",
                    owner,
                    artifact.descriptor,
                    &artifact.bytes,
                    0,
                    Default::default(),
                )
                .unwrap();
        }
        let value = tinytree_cloud::ManifestV2 {
            recovery: tinytree_cloud::Manifest {
                workspace: [41; 32],
                exporter: [42; 16],
                sequence: 1,
                operation_heads: vec![[0; 64]],
                undo_operations: vec![],
                intent_operations: vec![],
                working_copy: tinytree_cloud::WorkingCopy {
                    workspace_name: "task".into(),
                    wc_operation: [0; 64],
                    snapshot_operation: [0; 64],
                    snapshot_commit: [0; 32],
                    tree: tinytree_model::synthetic_root().root_tree,
                },
                parents: vec![],
                members: vec![],
                intent_roots: vec![],
                native_roots: vec![tinytree_model::empty_tree().descriptor.key],
                coverage: tinytree_cloud::Coverage::FullRequired,
                detached: vec![],
            },
            operation_indexes: vec![tinytree_cloud::OperationIndex {
                operation: [0; 64],
                root: indexed.root.id,
            }],
            index_roots: vec![indexed.root],
        };
        let manifest = tinytree_cloud::encode_manifest_v2(&value, Default::default()).unwrap();
        let id = tinytree_cloud::ManifestId::of(&manifest);
        let host = repository.cloud_host().unwrap();
        let service = CloudService::new(repository);
        let mut request = Request::new(p::PublishVersionRequest {
            repository_id: "example".into(),
            workspace_id: vec![41; 32],
            expected_generation: 0,
            idempotency_key: "indexed-adapter-gate".into(),
            manifest_id: id.0.to_vec(),
            manifest: manifest.clone(),
            staging_leases: vec![],
            capture_data: vec![],
        });
        request.extensions_mut().insert(
            host.admit(
                "PublishVersion",
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .unwrap(),
        );
        assert_eq!(
            service.publish_version(request).await.unwrap_err().code(),
            tonic::Code::Unimplemented
        );
        assert_eq!(
            store
                .get_cloud_workspace("example", owner, [41; 32])
                .unwrap()
                .generation,
            0
        );
        assert_eq!(
            store
                .cloud_publication("example", owner, "indexed-adapter-gate")
                .unwrap(),
            None
        );
        store
            .publish_cloud_version(
                "example",
                owner,
                &s::PublishCloudRequest {
                    workspace: [41; 32],
                    expected_generation: 0,
                    idempotency_key: "indexed-adapter-gate".into(),
                    manifest: id,
                    manifest_bytes: manifest,
                    staging_leases: vec![],
                },
                1,
                &Default::default(),
            )
            .unwrap();
        // Removing actual payloads makes a heavy/general read fail. The legacy
        // adapter must still report its unsupported schema without loading them.
        let namespace = std::fs::read_dir(directory.path().join("cloud"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        for artifact in &indexed.artifacts {
            let digest = artifact
                .descriptor
                .selected
                .blake3
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            std::fs::remove_file(namespace.join(digest)).unwrap();
        }
        let mut metadata = Request::new(p::GetWorkspaceRequest {
            repository_id: "example".into(),
            workspace_id: vec![41; 32],
        });
        metadata.extensions_mut().insert(
            host.admit(
                "GetWorkspace",
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .unwrap(),
        );
        assert_eq!(
            service
                .get_workspace(metadata)
                .await
                .unwrap()
                .into_inner()
                .generation,
            1
        );
        let mut metadata = Request::new(p::GetPublicationRequest {
            repository_id: "example".into(),
            idempotency_key: "indexed-adapter-gate".into(),
        });
        metadata.extensions_mut().insert(
            host.admit(
                "GetPublication",
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .unwrap(),
        );
        assert!(
            service
                .get_publication(metadata)
                .await
                .unwrap()
                .into_inner()
                .publication
                .is_some()
        );
        let mut request = Request::new(p::GetVersionRequest {
            repository_id: "example".into(),
            workspace_id: vec![41; 32],
            generation: 1,
        });
        request.extensions_mut().insert(
            host.admit(
                "GetVersion",
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .unwrap(),
        );
        assert_eq!(
            service.get_version(request).await.unwrap_err().code(),
            tonic::Code::Unimplemented
        );
        assert_eq!(host.active_jobs(), 0);
    }
}
