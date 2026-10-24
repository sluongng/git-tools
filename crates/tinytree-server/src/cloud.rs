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
        if !(1024..=64 * 1024 * 1024).contains(&self.max_object_bytes)
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
            profile.version_v1_bytes.max(profile.version_v2_bytes)
        } else {
            profile.version_v1_bytes
        };
        let ordinary_work = profile.object_bytes;
        let maximum = [
            ("PublishVersion", store_work),
            ("GetVersion", store_work),
            ("PutObject", ordinary_work),
            ("LookupObjects", ordinary_work),
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
            "PutObject" | "PutIndexArtifact" => self.max_object_bytes.saturating_add(4096),
            "PublishVersion" => self
                .max_manifest_bytes
                .saturating_add(self.max_staging_leases.saturating_mul(128))
                .saturating_add(4096),
            _ => self.max_request_bytes,
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
        let call =
            self.calls.clone().try_acquire_owned().map_err(|_| {
                Status::resource_exhausted("private workspace call budget exhausted")
            })?;
        let wire = self.config.wire_limit(method) as u64;
        if matches!(
            method,
            "PutIndexArtifact" | "LookupIndexArtifacts" | "ReadIndexArtifact"
        ) && !self.config.indexed_versions
        {
            return Err(Status::unimplemented(
                "indexed cloud artifacts are disabled",
            ));
        }
        let store = match method {
            "PublishVersion" | "GetVersion" => self.store_work,
            "PutIndexArtifact" | "ReadIndexArtifact" => self.profile.index_artifact_bytes,
            _ => self.profile.object_bytes,
        };
        let bytes = wire
            .checked_mul(128)
            .and_then(|n| n.checked_add(store))
            .ok_or_else(|| Status::resource_exhausted("private workspace memory bound"))?;
        let permits =
            kib(bytes).map_err(|_| Status::resource_exhausted("private workspace memory bound"))?;
        let memory = self
            .bytes
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| Status::resource_exhausted("private workspace memory budget exhausted"))?;
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
        let permit = gate.clone().try_acquire_owned().map_err(|_| {
            Status::resource_exhausted("private workspace repository budget exhausted")
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
fn status(error: s::Error) -> Status {
    crate::service::store_status(error)
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
    async fn publish_version(
        &self,
        request: Request<p::PublishVersionRequest>,
    ) -> Result<Response<p::Publication>, Status> {
        let (host, work, owner) = self.context(&request, &request.get_ref().repository_id, true)?;
        let input = request.into_inner();
        if input.manifest.len() > host.config.max_manifest_bytes
            || input.staging_leases.len() > host.config.max_staging_leases
        {
            return Err(Status::resource_exhausted(
                "private workspace publication exceeds budget",
            ));
        }
        // The opt-in reserves the complete indexed Store profile before decode.
        if !host.config.indexed_versions
            && input.manifest.get(..8) == Some(b"TTCLOUD\0".as_slice())
            && input.manifest.get(8..10) == Some([2u8, 0].as_slice())
        {
            return Err(Status::unimplemented(
                "indexed cloud manifests are not enabled by this RPC adapter",
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
            store
                .publish_cloud_version(&input.repository_id, owner, &publish, now()?, &limits)
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
        };
        Ok(response(output, work, value))
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
