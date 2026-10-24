use crate::{
    Principal,
    auth::{Authorization, Identity},
    config::{ConfigError, ServiceConfig},
    retention::{ReferenceScope, wire_name},
};
use async_stream::try_stream;
use futures_core::Stream;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::{self, Read},
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tinytree_model::{CommitId, Descriptor, Kind, Object, ObjectId, ObjectKey};
use tinytree_proto::v1::{
    self as p,
    repository_server::{Repository, RepositoryServer},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    time::Instant,
};
use tonic::{Request, Response, Status, Streaming, codec::CompressionEncoding};

#[path = "web.rs"]
pub(crate) mod web;

pub type RpcStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct RepositoryService {
    inner: Arc<Inner>,
    query: Option<Arc<crate::query_index::QueryHost>>,
    cloud: Option<Arc<crate::cloud::CloudHost>>,
    landing: Arc<crate::landing::LandingHost>,
}
struct Inner {
    store: tinytree_store::Store,
    config: ServiceConfig,
    auth: Authorization,
    global: Arc<Semaphore>,
    repositories: HashMap<String, Arc<Semaphore>>,
    bytes: Arc<Semaphore>,
    continuation_key: [u8; 32],
    #[cfg(test)]
    graph_read_pause: std::sync::Mutex<Option<GraphReadPause>>,
}
#[cfg(test)]
struct GraphReadPause {
    entered: std::sync::mpsc::SyncSender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}
struct Admission {
    _global: OwnedSemaphorePermit,
    _repository: OwnedSemaphorePermit,
}

impl RepositoryService {
    pub fn new(
        store: tinytree_store::Store,
        config: ServiceConfig,
        repositories: Vec<String>,
        principals: Vec<Principal>,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        let unique: HashSet<_> = repositories.iter().collect();
        if repositories.is_empty()
            || repositories.len() > 1024
            || unique.len() != repositories.len()
            || repositories
                .iter()
                .any(|r| tinytree_store::validate_repository_id(r).is_err())
        {
            return Err(ConfigError("invalid native repository identifiers"));
        }
        config.landing.validate_repositories(&repositories)?;
        for principal in &principals {
            config
                .landing
                .validate_grants(&principal.landing_profiles)?;
        }
        let auth = Authorization::new(principals, &repositories)?;
        let landing =
            crate::landing::LandingHost::new(store.clone(), config.landing.clone(), &repositories)?;
        let cloud = if config.cloud.enabled {
            Some(crate::cloud::CloudHost::new(
                store.clone(),
                config.cloud.clone(),
                &repositories,
            )?)
        } else {
            None
        };
        let repository_limits = repositories
            .into_iter()
            .map(|repo| {
                (
                    repo,
                    Arc::new(Semaphore::new(config.max_inflight_per_repository)),
                )
            })
            .collect();
        let mut continuation_key = [0; 32];
        continuation_key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        continuation_key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        Ok(Self {
            query: None,
            cloud,
            landing,
            inner: Arc::new(Inner {
                store,
                global: Arc::new(Semaphore::new(config.max_inflight)),
                repositories: repository_limits,
                bytes: Arc::new(Semaphore::new(config.max_buffered_bytes)),
                config,
                auth,
                continuation_key,
                #[cfg(test)]
                graph_read_pause: std::sync::Mutex::new(None),
            }),
        })
    }
    /// Install the rebuildable derived index. Scopes/warms use dedicated pools,
    /// independent from canonical object admission and ACKs.
    pub fn with_query_index(
        mut self,
        index: Arc<tinytree_index::Index>,
    ) -> Result<Self, ConfigError> {
        if self.query.is_some() {
            return Err(ConfigError("query host already installed"));
        }
        self.query = Some(crate::query_index::QueryHost::new(
            index,
            self.inner.store.clone(),
            &self.inner.config,
        )?);
        Ok(self)
    }
    pub fn landing_grpc_service(&self) -> crate::LandingGrpcService {
        crate::LandingGrpcService::new(self.clone())
    }
    pub(crate) fn landing_host(&self) -> Arc<crate::landing::LandingHost> {
        self.landing.clone()
    }
    /// Start bounded preparation/expiry discovery when assembling a custom
    /// transport. The supported TCP and Unix listener helpers call this once.
    pub fn start_landing_worker(&self) -> Result<(), Status> {
        self.landing.start()
    }
    pub async fn drain_landing(&self) {
        self.landing.drain().await;
    }
    pub fn cloud_grpc_service(&self) -> crate::CloudGrpcService {
        crate::CloudGrpcService::new(self.clone())
    }
    pub(crate) fn cloud_host(&self) -> Option<Arc<crate::cloud::CloudHost>> {
        self.cloud.clone()
    }
    /// Process-local diagnostics for derived content warming. This does not
    /// expose a network endpoint or prove that canonical content is retained.
    pub fn search_stats(&self) -> Option<crate::SearchStats> {
        self.query_host().map(|host| host.search.stats())
    }
    /// Globally drains shared query hosting across all cloned service listeners.
    pub async fn drain_query(&self) -> Result<(), Status> {
        if let Some(host) = &self.query {
            host.drain().await?;
        }
        Ok(())
    }
    pub async fn drain_cloud(&self) {
        if let Some(host) = &self.cloud {
            host.drain().await;
        }
    }
    pub fn query_service(&self) -> crate::QueryService {
        crate::QueryService::new(self.clone())
    }
    pub fn query_grpc_service(&self) -> crate::QueryGrpcService {
        crate::QueryGrpcService::new(self.clone())
    }
    pub(crate) fn service_config(&self) -> ServiceConfig {
        self.inner.config.clone()
    }
    pub(crate) fn query_identity(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Identity, Status> {
        self.inner.auth.identify(metadata)
    }
    pub(crate) fn query_deadline(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        streaming: bool,
    ) -> Result<Instant, Status> {
        self.deadline(metadata, streaming)
    }
    pub(crate) fn query_host(&self) -> Option<Arc<crate::query_index::QueryHost>> {
        self.query.clone()
    }
    pub fn grpc_service(&self) -> RepositoryServer<Self> {
        RepositoryServer::new(self.clone())
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(self.inner.config.decode_limit())
            .max_encoding_message_size(self.inner.config.encode_limit())
    }
    pub fn authentication_enabled(&self) -> bool {
        self.inner.auth.enabled()
    }
    pub fn maximum_inflight(&self) -> usize {
        self.inner.config.max_inflight
    }
    pub fn deadline_layer(&self) -> crate::DeadlineLayer {
        crate::DeadlineLayer::new(
            self.inner.config.rpc.operation_timeout_ms,
            self.inner.config.rpc.stream_idle_timeout_ms,
            self.inner.config.max_frame_bytes,
        )
    }
    async fn response<T: Message>(
        &self,
        value: T,
        admission: Arc<Admission>,
        memory: Option<Arc<OwnedSemaphorePermit>>,
        deadline: Instant,
    ) -> Result<Response<T>, Status> {
        let memory = match memory {
            Some(memory) => memory,
            None => {
                self.memory(value.encoded_len().saturating_mul(4).max(1024), deadline)
                    .await?
            }
        };
        let mut response = Response::new(value);
        response
            .extensions_mut()
            .insert(crate::body_deadline::ResponseBudget::new((
                admission, memory,
            )));
        Ok(response)
    }
    fn deadline(
        &self,
        metadata: &tonic::metadata::MetadataMap,
        streaming: bool,
    ) -> Result<Instant, Status> {
        let policy = &self.inner.config.rpc;
        let mut budget = Duration::from_millis(if streaming {
            policy.operation_timeout_ms
        } else {
            policy.rpc_timeout_ms.min(policy.operation_timeout_ms)
        });
        if let Some(value) = metadata.get("grpc-timeout") {
            let text = value
                .to_str()
                .map_err(|_| Status::invalid_argument("invalid RPC timeout"))?;
            if text.len() < 2 || text.len() > 9 {
                return Err(Status::invalid_argument("invalid RPC timeout"));
            }
            let (digits, unit) = text.split_at(text.len() - 1);
            if !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(Status::invalid_argument("invalid RPC timeout"));
            }
            let n: u64 = digits
                .parse()
                .map_err(|_| Status::invalid_argument("invalid RPC timeout"))?;
            let duration = match unit {
                "H" => Duration::from_secs(n.saturating_mul(3600)),
                "M" => Duration::from_secs(n.saturating_mul(60)),
                "S" => Duration::from_secs(n),
                "m" => Duration::from_millis(n),
                "u" => Duration::from_micros(n),
                "n" => Duration::from_nanos(n),
                _ => return Err(Status::invalid_argument("invalid RPC timeout")),
            };
            budget = budget.min(duration);
        }
        Ok(Instant::now() + budget)
    }
    fn bounded<T: Message>(&self, message: &T) -> Result<(), Status> {
        if message.encoded_len() > self.inner.config.max_request_bytes {
            return Err(Status::resource_exhausted(
                "native request exceeds byte budget",
            ));
        }
        Ok(())
    }
    fn batch(&self, n: usize) -> Result<(), Status> {
        if n > self.inner.config.max_batch_items {
            return Err(Status::resource_exhausted(
                "native batch exceeds item budget",
            ));
        }
        Ok(())
    }
    async fn global(&self, deadline: Instant) -> Result<OwnedSemaphorePermit, Status> {
        acquire(self.inner.global.clone(), deadline).await
    }
    async fn repository(
        &self,
        repo: &str,
        deadline: Instant,
    ) -> Result<OwnedSemaphorePermit, Status> {
        let gate = self
            .inner
            .repositories
            .get(repo)
            .ok_or_else(|| Status::not_found("native repository unavailable"))?;
        acquire(gate.clone(), deadline).await
    }
    async fn admit(
        &self,
        identity: &Identity,
        repo: &str,
        write: bool,
        deadline: Instant,
    ) -> Result<Arc<Admission>, Status> {
        identity.authorize(repo, write)?;
        let global = self.global(deadline).await?;
        let repository = self.repository(repo, deadline).await?;
        Ok(Arc::new(Admission {
            _global: global,
            _repository: repository,
        }))
    }
    async fn memory(
        &self,
        n: usize,
        deadline: Instant,
    ) -> Result<Arc<OwnedSemaphorePermit>, Status> {
        if n > self.inner.config.max_buffered_bytes {
            return Err(Status::resource_exhausted(
                "native operation exceeds memory budget",
            ));
        }
        let n = u32::try_from(n.max(1))
            .map_err(|_| Status::resource_exhausted("native operation exceeds memory budget"))?;
        tokio::time::timeout_at(deadline, self.inner.bytes.clone().acquire_many_owned(n))
            .await
            .map_err(|_| timeout())?
            .map(Arc::new)
            .map_err(|_| Status::unavailable("native service is draining"))
    }
    async fn validate_repository(
        &self,
        repo: String,
        admission: Arc<Admission>,
        deadline: Instant,
    ) -> Result<Arc<Admission>, Status> {
        let store = self.inner.store.clone();
        blocking(deadline, move || {
            store.references(&repo, &[]).map_err(store_status)?;
            Ok(admission)
        })
        .await
    }
    fn graph_fingerprint(
        &self,
        identity: &Identity,
        request: &p::GetCommitGraphRequest,
    ) -> [u8; 32] {
        let mut request = request.clone();
        request.continuation.clear();
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-graph-v1\0");
        hash.update(&(identity.id.len() as u64).to_be_bytes());
        hash.update(identity.id.as_bytes());
        hash.update(&request.encode_to_vec());
        *hash.finalize().as_bytes()
    }
    fn graph_offset(&self, token: &str, fingerprint: [u8; 32]) -> Result<usize, Status> {
        if token.is_empty() {
            return Ok(0);
        }
        if token.len() > 1024 {
            return Err(Status::invalid_argument("invalid graph continuation"));
        }
        let (body, mac) = token
            .split_once('.')
            .ok_or_else(|| Status::invalid_argument("invalid graph continuation"))?;
        let body =
            unhex(body).ok_or_else(|| Status::invalid_argument("invalid graph continuation"))?;
        let mac =
            unhex(mac).ok_or_else(|| Status::invalid_argument("invalid graph continuation"))?;
        let expected = blake3::keyed_hash(&self.inner.continuation_key, &body);
        if mac.len() != 32
            || mac
                .iter()
                .zip(expected.as_bytes())
                .fold(0u8, |a, (x, y)| a | (x ^ y))
                != 0
        {
            return Err(Status::failed_precondition(
                "graph continuation expired or changed",
            ));
        }
        let cursor: Cursor = serde_json::from_slice(&body)
            .map_err(|_| Status::invalid_argument("invalid graph continuation"))?;
        if cursor.version != 1 || cursor.fingerprint != fingerprint {
            return Err(Status::failed_precondition(
                "graph continuation inputs changed",
            ));
        }
        usize::try_from(cursor.offset)
            .map_err(|_| Status::invalid_argument("invalid graph continuation"))
    }
    fn graph_cursor(&self, offset: usize, fingerprint: [u8; 32]) -> Result<String, Status> {
        let body = serde_json::to_vec(&Cursor {
            version: 1,
            fingerprint,
            offset: offset as u64,
        })
        .map_err(|_| Status::internal("cannot encode graph continuation"))?;
        let hash = blake3::keyed_hash(&self.inner.continuation_key, &body);
        Ok(format!("{}.{}", hex(&body), hash.to_hex()))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    fingerprint: [u8; 32],
    offset: u64,
}

#[tonic::async_trait]
impl Repository for RepositoryService {
    async fn open_repository(
        &self,
        request: Request<p::OpenRepositoryRequest>,
    ) -> Result<Response<p::RepositoryView>, Status> {
        let identity = self.inner.auth.identify(request.metadata())?;
        let deadline = self.deadline(request.metadata(), false)?;
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        self.batch(input.reference_names.len())?;
        let admission = self
            .admit(&identity, &input.repository_id, false, deadline)
            .await?;
        let store = self.inner.store.clone();
        let repository_id = input.repository_id;
        let response_repo = repository_id.clone();
        let names = input
            .reference_names
            .iter()
            .map(|name| ReferenceScope::parse(name, &identity)?.stored_name())
            .collect::<Result<Vec<_>, Status>>()?;
        let response_admission = admission.clone();
        let refs = blocking(deadline, move || {
            let _admission = admission;
            store
                .references(&repository_id, &names)
                .map_err(store_status)
        })
        .await?;
        self.response(
            p::RepositoryView {
                repository_id: response_repo,
                protocol_major: 1,
                object_format_version: tinytree_model::FORMAT_VERSION as u32,
                references: refs
                    .into_iter()
                    .map(|value| reference(value, &identity))
                    .collect::<Result<_, _>>()?,
                max_batch_items: self.inner.config.max_batch_items as u32,
                max_request_bytes: self.inner.config.max_request_bytes as u64,
                max_frame_bytes: self.inner.config.max_frame_bytes as u32,
            },
            response_admission,
            None,
            deadline,
        )
        .await
    }

    async fn get_commit_graph(
        &self,
        request: Request<p::GetCommitGraphRequest>,
    ) -> Result<Response<p::CommitGraphPage>, Status> {
        let identity = self.inner.auth.identify(request.metadata())?;
        let deadline = self.deadline(request.metadata(), false)?;
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        self.batch(input.heads.len().saturating_add(input.common.len()))?;
        let admission = self
            .admit(&identity, &input.repository_id, false, deadline)
            .await?;
        let page_size = if input.page_size == 0 {
            self.inner.config.max_batch_items.min(64)
        } else {
            input.page_size as usize
        };
        self.batch(page_size)?;
        let fingerprint = self.graph_fingerprint(&identity, &input);
        let offset = self.graph_offset(&input.continuation, fingerprint)?;
        let heads = input
            .heads
            .iter()
            .map(commit_key)
            .collect::<Result<Vec<_>, _>>()?;
        let common = input
            .common
            .iter()
            .map(commit_key)
            .collect::<Result<Vec<_>, _>>()?;
        let memory = self
            .memory(
                self.inner.config.max_graph_bytes.saturating_mul(4),
                deadline,
            )
            .await?;
        let store = self.inner.store.clone();
        let cfg = self.inner.config.clone();
        let repo = input.repository_id;
        let response_admission = admission.clone();
        let response_memory = memory.clone();
        #[cfg(test)]
        let pause = self.inner.graph_read_pause.lock().unwrap().take();
        let entries = blocking(deadline, move || {
            let (_admission, _memory) = (admission, memory);
            store.references(&repo, &[]).map_err(store_status)?;
            // Reader roots are acquired under the store's GC gate and protect
            // their present closure until this graph traversal physically ends.
            // They are per-call leases, never persistent reference mutations.
            let _roots = graph_root_leases(&store, &repo, &heads, &common, &cfg)?;
            #[cfg(test)]
            if let Some(pause) = pause {
                pause
                    .entered
                    .send(())
                    .map_err(|_| Status::internal("graph test observer stopped"))?;
                pause
                    .resume
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|_| Status::deadline_exceeded("graph test interleaving expired"))?;
            }
            graph_difference(&store, &repo, heads, common, &cfg)
        })
        .await?;
        if offset > entries.len() {
            return Err(Status::failed_precondition(
                "graph continuation is no longer available",
            ));
        }
        let mut page = Vec::new();
        let mut bytes = 0usize;
        for entry in entries.iter().skip(offset).take(page_size) {
            let next = bytes.saturating_add(entry.encoded_len() + 16);
            if next > self.inner.config.encode_limit().saturating_sub(1024) {
                break;
            }
            bytes = next;
            page.push(entry.clone());
        }
        if page.is_empty() && offset < entries.len() {
            return Err(Status::resource_exhausted(
                "graph entry exceeds response budget",
            ));
        }
        let end = offset + page.len();
        let continuation = if end < entries.len() {
            self.graph_cursor(end, fingerprint)?
        } else {
            String::new()
        };
        self.response(
            p::CommitGraphPage {
                entries: page,
                continuation,
            },
            response_admission,
            Some(response_memory),
            deadline,
        )
        .await
    }

    type BatchReadObjectsStream = RpcStream<p::ObjectResult>;
    async fn batch_read_objects(
        &self,
        request: Request<p::BatchReadObjectsRequest>,
    ) -> Result<Response<Self::BatchReadObjectsStream>, Status> {
        let identity = self.inner.auth.identify(request.metadata())?;
        let deadline = self.deadline(request.metadata(), true)?;
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        self.batch(input.keys.len())?;
        let admission = self
            .admit(&identity, &input.repository_id, false, deadline)
            .await?;
        let admission = self
            .validate_repository(input.repository_id.clone(), admission, deadline)
            .await?;
        let this = self.clone();
        let stream = try_stream! {
            let _admission=admission;
            for (index,wire_key) in input.keys.into_iter().enumerate(){
                if Instant::now()>=deadline {Err(timeout())?;}
                let object=match key(&wire_key){Ok(k) if k.kind!=Kind::File&&!intrinsic(k)=>k,_=>{
                    yield object_error(index,wire_key,Status::invalid_argument("key has no native metadata payload"))?;continue;
                }};
                let store=this.inner.store.clone();let repo=input.repository_id.clone();
                let held_admission=_admission.clone();
                let desc=blocking(deadline,move||{let _held_admission=held_admission;store.descriptor(&repo,object).map_err(read_status)}).await;
                let desc=match desc {Ok(Some(d))=>d,Ok(None)=>{yield object_error(index,wire_key,Status::not_found("native object unavailable"))?;continue;},Err(e)=>{yield object_error(index,wire_key,e)?;continue;}};
                if desc.size_bytes>this.inner.config.max_metadata_bytes as u64 {yield object_error(index,wire_key,Status::resource_exhausted("native metadata exceeds budget"))?;continue;}
                let memory=this.memory((desc.size_bytes as usize).saturating_mul(4).max(1024),deadline).await?;
                let store=this.inner.store.clone();let repo=input.repository_id.clone();
                let held_admission=_admission.clone();let held_memory=memory.clone();
                let data=blocking(deadline,move||{let (_held_admission,_held_memory)=(held_admission,held_memory);store.read_metadata(&repo,object).map_err(read_status)}).await;
                match data {Ok((descriptor,bytes))=>yield p::ObjectResult{request_index:index as u32,key:Some(wire_key),result:Some(p::object_result::Result::Object(p::ObjectData{descriptor:Some(descriptor_wire(descriptor)),canonical_envelope:bytes}))},Err(e)=>yield object_error(index,wire_key,e)?,}
                drop(memory);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn find_missing_objects(
        &self,
        request: Request<p::FindMissingObjectsRequest>,
    ) -> Result<Response<p::MissingObjects>, Status> {
        let identity = self.inner.auth.identify(request.metadata())?;
        let deadline = self.deadline(request.metadata(), false)?;
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        self.batch(input.keys.len())?;
        let admission = self
            .admit(&identity, &input.repository_id, false, deadline)
            .await?;
        let keys = input
            .keys
            .into_iter()
            .map(|wire| key(&wire).map(|key| (key, wire)))
            .collect::<Result<Vec<_>, _>>()?;
        let store = self.inner.store.clone();
        let repo = input.repository_id;
        let response_admission = admission.clone();
        let keys = blocking(deadline, move || {
            let _admission = admission;
            store.references(&repo, &[]).map_err(store_status)?;
            let mut missing = Vec::new();
            for (key, wire) in keys {
                if !intrinsic(key)
                    && store
                        .descriptor(&repo, key)
                        .map_err(store_status)?
                        .is_none()
                {
                    missing.push(wire);
                }
            }
            Ok(missing)
        })
        .await?;
        self.response(
            p::MissingObjects { keys },
            response_admission,
            None,
            deadline,
        )
        .await
    }

    type ReadFileStream = RpcStream<p::FileFrame>;
    async fn read_file(
        &self,
        request: Request<p::ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let identity = self.inner.auth.identify(request.metadata())?;
        let deadline = self.deadline(request.metadata(), true)?;
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let admission = self
            .admit(&identity, &input.repository_id, false, deadline)
            .await?;
        let object = ObjectKey {
            kind: Kind::File,
            id: ObjectId::from_slice(&input.native_file_id)
                .map_err(|_| Status::invalid_argument("invalid native file ID"))?,
        };
        let memory = self
            .memory(
                self.inner.config.max_frame_bytes.saturating_mul(4) + 1024,
                deadline,
            )
            .await?;
        let store = self.inner.store.clone();
        let repo = input.repository_id;
        let (mut lease, admission) = blocking(deadline, move || {
            store
                .read(&repo, object)
                .map(|lease| (lease, admission))
                .map_err(store_status)
        })
        .await?;
        let descriptor = lease.descriptor();
        let frame_bytes = self.inner.config.max_frame_bytes;
        let (sender, mut receiver) = mpsc::channel(2);
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let (_admission, _memory) = (admission, memory);
            let send_frame = |value| {
                runtime.block_on(async {
                    tokio::time::timeout_at(deadline, sender.send(value))
                        .await
                        .map_err(|_| ())
                        .and_then(|result| result.map_err(|_| ()))
                })
            };
            if send_frame(Ok(p::FileFrame {
                frame: Some(p::file_frame::Frame::Header(descriptor_wire(descriptor))),
            }))
            .is_err()
            {
                return;
            }
            let mut offset = 0u64;
            let mut hash = blake3::Hasher::new();
            loop {
                let mut data = vec![0; frame_bytes];
                let n = match lease.read(&mut data) {
                    Ok(n) => n,
                    Err(_) => {
                        let _ = send_frame(Err(Status::data_loss("native file read failed")));
                        return;
                    }
                };
                if n == 0 {
                    break;
                }
                data.truncate(n);
                hash.update(&data);
                if offset.saturating_add(n as u64) > descriptor.size_bytes {
                    let _ = send_frame(Err(Status::data_loss("native file length changed")));
                    return;
                }
                let chunk = p::DataChunk { offset, data };
                offset += n as u64;
                if send_frame(Ok(p::FileFrame {
                    frame: Some(p::file_frame::Frame::Chunk(chunk)),
                }))
                .is_err()
                {
                    return;
                }
            }
            if offset != descriptor.size_bytes || hash.finalize().as_bytes() != &descriptor.key.id.0
            {
                let _ = send_frame(Err(Status::data_loss("native file integrity changed")));
            }
        });
        let idle = Duration::from_millis(self.inner.config.rpc.stream_idle_timeout_ms);
        let stream = try_stream! {while let Some(frame)=receive(&mut receiver,deadline,idle).await? {yield frame?;}};
        Ok(Response::new(Box::pin(stream)))
    }

    async fn write_object(
        &self,
        request: Request<Streaming<p::WriteFrame>>,
    ) -> Result<Response<p::WriteReceipt>, Status> {
        let identity = self.inner.auth.identify(request.metadata())?;
        let deadline = self.deadline(request.metadata(), true)?;
        let global = self.global(deadline).await?;
        let mut body = request.into_inner();
        let idle = Duration::from_millis(self.inner.config.rpc.stream_idle_timeout_ms);
        let first = message(&mut body, deadline, idle)
            .await?
            .ok_or_else(|| Status::invalid_argument("upload header required"))?;
        let header = match first.frame {
            Some(p::write_frame::Frame::Header(h)) => h,
            _ => return Err(Status::invalid_argument("upload header must be first")),
        };
        identity.authorize(&header.repository_id, true)?;
        let repo_admission = self.repository(&header.repository_id, deadline).await?;
        let admission = Arc::new(Admission {
            _global: global,
            _repository: repo_admission,
        });
        let admission = self
            .validate_repository(header.repository_id.clone(), admission, deadline)
            .await?;
        let descriptor = descriptor(
            header
                .descriptor
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("object descriptor required"))?,
        )?;
        if intrinsic(descriptor.key) {
            return Err(Status::invalid_argument(
                "intrinsic root cannot be uploaded",
            ));
        }
        let memory_bytes = if descriptor.key.kind == Kind::File {
            self.inner.config.max_frame_bytes * 4 + 1024
        } else {
            if descriptor.size_bytes > self.inner.config.max_metadata_bytes as u64 {
                return Err(Status::resource_exhausted("native metadata exceeds budget"));
            }
            (descriptor.size_bytes as usize).saturating_mul(4).max(1024)
        };
        let memory = self.memory(memory_bytes, deadline).await?;
        let (sender, receiver) = mpsc::channel(2);
        let store = self.inner.store.clone();
        let repo = header.repository_id;
        let now = now()?;
        let response_admission = admission.clone();
        let response_memory = memory.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let (_admission, _memory) = (admission, memory);
            let mut reader = UploadReader {
                receiver,
                current: io::Cursor::new(Vec::new()),
                finished: false,
            };
            store
                .put_stream(&repo, descriptor, &mut reader, now)
                .map_err(store_status)
        });
        let ingestion: Result<(), Status> = async {
            let mut offset = 0u64;
            let mut frames = 0usize;
            while let Some(frame) = message(&mut body, deadline, idle).await? {
                frames += 1;
                if frames > 1_000_000 {
                    return Err(Status::resource_exhausted("upload frame count exceeded"));
                }
                let chunk = match frame.frame {
                    Some(p::write_frame::Frame::Chunk(c)) => c,
                    _ => {
                        return Err(Status::invalid_argument(
                            "upload contains repeated or empty header",
                        ));
                    }
                };
                if chunk.data.is_empty()
                    || chunk.data.len() > self.inner.config.max_frame_bytes
                    || chunk.offset != offset
                {
                    return Err(Status::invalid_argument(
                        "invalid upload chunk size or offset",
                    ));
                }
                offset = offset
                    .checked_add(chunk.data.len() as u64)
                    .ok_or_else(|| Status::invalid_argument("upload length overflow"))?;
                if offset > descriptor.size_bytes {
                    return Err(Status::invalid_argument("upload exceeds declared length"));
                }
                send(&sender, Upload::Data(chunk.data), deadline, idle).await?;
            }
            if offset != descriptor.size_bytes {
                return Err(Status::invalid_argument(
                    "upload ended before declared length",
                ));
            }
            send(&sender, Upload::Finish, deadline, idle).await
        }
        .await;
        drop(sender);
        let outcome = tokio::time::timeout_at(deadline, worker)
            .await
            .map_err(|_| timeout())
            .and_then(|result| result.map_err(|_| Status::internal("native upload worker failed")))
            .and_then(|result| result);
        if let Err(error) = ingestion {
            // A closed bridge can mean reservation/quota rejection before any
            // body was stored. Preserve that permanent status rather than
            // converting it into a retryable UNAVAILABLE.
            if error.code() == tonic::Code::Unavailable {
                if let Err(store_error) = outcome {
                    return Err(store_error);
                }
            }
            return Err(error);
        }
        let receipt = outcome?;
        self.response(
            p::WriteReceipt {
                descriptor: Some(descriptor_wire(receipt.descriptor)),
                staging_lease: receipt.staging_lease,
                staging_lease_expiry_unix_seconds: receipt.expires_at,
            },
            response_admission,
            Some(response_memory),
            deadline,
        )
        .await
    }

    async fn publish_reference(
        &self,
        request: Request<p::PublishReferenceRequest>,
    ) -> Result<Response<p::Publication>, Status> {
        let identity = self.inner.auth.identify(request.metadata())?;
        let deadline = self.deadline(request.metadata(), false)?;
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        self.batch(input.staging_leases.len())?;
        let scope = ReferenceScope::parse(&input.reference_name, &identity)?;
        let public = matches!(&scope, ReferenceScope::Public(_));
        if !public {
            identity.authorize_retention(&input.repository_id)?;
            if !input.staging_leases.is_empty() {
                return Err(Status::invalid_argument(
                    "native retention does not accept staging tokens",
                ));
            }
        }
        let admission = self
            .admit(&identity, &input.repository_id, public, deadline)
            .await?;
        let commit = input.new_commit.as_ref().map(commit_key).transpose()?;
        if commit.is_some_and(|k| intrinsic(k)) {
            return Err(Status::invalid_argument(
                "intrinsic root cannot be published",
            ));
        }
        if let ReferenceScope::Hold { commit: target, .. } = &scope {
            if commit.is_some_and(|key| key.id.0 != target.0) {
                return Err(Status::invalid_argument(
                    "native retention target cannot change",
                ));
            }
        }
        let original_id = input.idempotency_key;
        let scoped_id = publication_key(&identity, &original_id)?;
        let req = tinytree_store::PublishRequest {
            idempotency_key: scoped_id,
            reference_name: scope.stored_name()?,
            expected_generation: input.expected_generation,
            new_commit: commit.map(|k| CommitId(k.id.0)),
            staging_leases: input.staging_leases,
        };
        let store = self.inner.store.clone();
        let repo = input.repository_id;
        let now = now()?;
        let limits = self.inner.config.retention.clone();
        let response_admission = admission.clone();
        let publication = blocking(deadline, move || {
            let _admission = admission;
            match scope {
                ReferenceScope::Public(_) => store.publish(&repo, &req, now),
                ReferenceScope::Hold { owner, .. } => {
                    store.publish_retention(&repo, owner, &req, &limits, now)
                }
            }
            .map_err(store_status)
        })
        .await?;
        let mut publication = publication_wire(publication, &identity)?;
        publication.idempotency_key = original_id;
        self.response(publication, response_admission, None, deadline)
            .await
    }

    async fn get_publication(
        &self,
        request: Request<p::GetPublicationRequest>,
    ) -> Result<Response<p::Publication>, Status> {
        let identity = self.inner.auth.identify(request.metadata())?;
        let deadline = self.deadline(request.metadata(), false)?;
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let admission = self
            .admit(&identity, &input.repository_id, false, deadline)
            .await?;
        let store = self.inner.store.clone();
        let repo = input.repository_id;
        let original_id = input.idempotency_key;
        let id = publication_key(&identity, &original_id)?;
        let response_admission = admission.clone();
        let publication = blocking(deadline, move || {
            let _admission = admission;
            store
                .publication(&repo, &id)
                .map_err(store_status)?
                .ok_or_else(|| Status::not_found("native publication unavailable"))
        })
        .await?;
        let mut publication = publication_wire(publication, &identity)?;
        publication.idempotency_key = original_id;
        self.response(publication, response_admission, None, deadline)
            .await
    }
}

fn timeout() -> Status {
    Status::deadline_exceeded("native operation deadline exceeded")
}
fn publication_key(identity: &Identity, id: &str) -> Result<String, Status> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Status::invalid_argument(
            "invalid publication idempotency key",
        ));
    }
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-publication-principal-v1\0");
    h.update(&(identity.id.len() as u64).to_be_bytes());
    h.update(identity.id.as_bytes());
    h.update(id.as_bytes());
    Ok(format!("rpc-{}", h.finalize().to_hex()))
}
async fn acquire(gate: Arc<Semaphore>, deadline: Instant) -> Result<OwnedSemaphorePermit, Status> {
    tokio::time::timeout_at(deadline, gate.acquire_owned())
        .await
        .map_err(|_| timeout())?
        .map_err(|_| Status::unavailable("native service is draining"))
}
async fn blocking<T: Send + 'static>(
    deadline: Instant,
    f: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::time::timeout_at(deadline, tokio::task::spawn_blocking(f))
        .await
        .map_err(|_| timeout())?
        .map_err(|_| Status::internal("native store worker failed"))?
}
async fn receive<T>(
    receiver: &mut mpsc::Receiver<T>,
    deadline: Instant,
    idle: Duration,
) -> Result<Option<T>, Status> {
    tokio::time::timeout_at(deadline.min(Instant::now() + idle), receiver.recv())
        .await
        .map_err(|_| timeout())
}
async fn message(
    body: &mut Streaming<p::WriteFrame>,
    deadline: Instant,
    idle: Duration,
) -> Result<Option<p::WriteFrame>, Status> {
    tokio::time::timeout_at(deadline.min(Instant::now() + idle), body.message())
        .await
        .map_err(|_| timeout())?
}
async fn send(
    sender: &mpsc::Sender<Upload>,
    value: Upload,
    deadline: Instant,
    idle: Duration,
) -> Result<(), Status> {
    tokio::time::timeout_at(deadline.min(Instant::now() + idle), sender.send(value))
        .await
        .map_err(|_| timeout())?
        .map_err(|_| Status::unavailable("native upload stopped"))
}
enum Upload {
    Data(Vec<u8>),
    Finish,
}
struct UploadReader {
    receiver: mpsc::Receiver<Upload>,
    current: io::Cursor<Vec<u8>>,
    finished: bool,
}
impl Read for UploadReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.current.read(buffer)?;
            if n != 0 {
                return Ok(n);
            }
            if self.finished {
                return Ok(0);
            }
            match self.receiver.blocking_recv() {
                Some(Upload::Data(data)) => self.current = io::Cursor::new(data),
                Some(Upload::Finish) => self.finished = true,
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "upload cancelled before clean EOF",
                    ));
                }
            }
        }
    }
}
pub(crate) fn now() -> Result<i64, Status> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("native server clock unavailable"))?
        .as_secs();
    i64::try_from(value).map_err(|_| Status::internal("native server clock unavailable"))
}
fn key(value: &p::ObjectKey) -> Result<ObjectKey, Status> {
    let kind = u8::try_from(value.kind)
        .ok()
        .and_then(|k| Kind::try_from(k).ok())
        .ok_or_else(|| Status::invalid_argument("invalid native object kind"))?;
    let id = ObjectId::from_slice(&value.native_id)
        .map_err(|_| Status::invalid_argument("invalid native object ID"))?;
    Ok(ObjectKey { kind, id })
}
fn commit_key(value: &p::ObjectKey) -> Result<ObjectKey, Status> {
    let key = key(value)?;
    if key.kind != Kind::Commit {
        return Err(Status::invalid_argument("commit object required"));
    }
    Ok(key)
}
fn intrinsic(key: ObjectKey) -> bool {
    key.kind == Kind::Commit && key.id.0 == CommitId::ROOT.0
}
fn key_wire(key: ObjectKey) -> p::ObjectKey {
    p::ObjectKey {
        kind: key.kind as i32,
        native_id: key.id.0.to_vec(),
    }
}
fn descriptor(value: &p::ObjectDescriptor) -> Result<Descriptor, Status> {
    let key = key(value
        .key
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("object key required"))?)?;
    if value.content_blake3 != key.id.0
        || value.object_format_version != tinytree_model::FORMAT_VERSION as u32
    {
        return Err(Status::invalid_argument(
            "native descriptor digest or format mismatch",
        ));
    }
    Ok(Descriptor {
        key,
        size_bytes: value.size_bytes,
        format_version: tinytree_model::FORMAT_VERSION,
    })
}
pub(crate) fn descriptor_wire(value: Descriptor) -> p::ObjectDescriptor {
    p::ObjectDescriptor {
        key: Some(key_wire(value.key)),
        content_blake3: value.key.id.0.to_vec(),
        size_bytes: value.size_bytes,
        object_format_version: value.format_version as u32,
    }
}
fn reference(
    value: tinytree_store::Reference,
    identity: &Identity,
) -> Result<p::Reference, Status> {
    Ok(p::Reference {
        name: wire_name(&value.name, identity)?,
        generation: value.generation,
        commit: value.commit.map(descriptor_wire),
    })
}
fn publication_wire(
    value: tinytree_store::Publication,
    identity: &Identity,
) -> Result<p::Publication, Status> {
    Ok(p::Publication {
        idempotency_key: value.idempotency_key,
        reference: Some(reference(value.reference, identity)?),
    })
}
pub(crate) fn store_status(error: tinytree_store::Error) -> Status {
    use tinytree_store::Error as E;
    match error {
        E::UnknownRepository | E::MissingObject(_) => {
            Status::not_found("native repository or object unavailable")
        }
        E::Invalid(_) | E::Model(_) => Status::invalid_argument("invalid native object or request"),
        E::Corrupt(_) => Status::data_loss("native store integrity check failed"),
        E::StaleReference(_) => Status::aborted("native reference generation changed"),
        E::StaleCloudWorkspace(_) => Status::aborted("cloud workspace generation changed"),
        E::IdempotencyConflict => {
            Status::already_exists("native idempotency key has another request")
        }
        E::Limit(_) => Status::resource_exhausted("native store limit exceeded"),
        E::Poisoned => Status::unavailable("native store requires recovery"),
        E::Io(_) | E::Database(_) => Status::internal("native storage operation failed"),
    }
}
fn read_status(error: tinytree_store::Error) -> Status {
    match error {
        tinytree_store::Error::Model(_) | tinytree_store::Error::Corrupt(_) => {
            Status::data_loss("native store integrity check failed")
        }
        tinytree_store::Error::Io(ref e)
            if matches!(
                e.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::UnexpectedEof
            ) =>
        {
            Status::data_loss("native payload is unavailable or incomplete")
        }
        other => store_status(other),
    }
}
fn object_error(
    index: usize,
    key: p::ObjectKey,
    status: Status,
) -> Result<p::ObjectResult, Status> {
    let code = match status.code() {
        tonic::Code::NotFound => p::item_error::Code::NotFound,
        tonic::Code::ResourceExhausted => p::item_error::Code::ResourceExhausted,
        tonic::Code::Unavailable => p::item_error::Code::Unavailable,
        tonic::Code::InvalidArgument | tonic::Code::DataLoss => p::item_error::Code::InvalidObject,
        _ => return Err(status),
    };
    Ok(p::ObjectResult {
        request_index: index as u32,
        key: Some(key),
        result: Some(p::object_result::Result::Error(p::ItemError {
            code: code as i32,
            message: status.message().into(),
        })),
    })
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn unhex(text: &str) -> Option<Vec<u8>> {
    if text.len() % 2 != 0 {
        return None;
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|b| {
            let h = (b[0] as char).to_digit(16)?;
            let l = (b[1] as char).to_digit(16)?;
            Some(((h << 4) | l) as u8)
        })
        .collect()
}

fn graph_root_leases(
    store: &tinytree_store::Store,
    repo: &str,
    heads: &[ObjectKey],
    common: &[ObjectKey],
    config: &ServiceConfig,
) -> Result<Vec<tinytree_store::RootReadGuard>, Status> {
    let mut seen = HashSet::new();
    let mut leases = Vec::new();
    for key in heads.iter().chain(common).copied() {
        if intrinsic(key) || !seen.insert(key) {
            continue;
        }
        let handles = seen.len().saturating_mul(
            4 * std::mem::size_of::<tinytree_store::RootReadGuard>()
                + 4 * std::mem::size_of::<ObjectKey>()
                + 128,
        );
        if handles > config.max_graph_bytes {
            return Err(Status::resource_exhausted(
                "native graph root handles exceed allocation budget",
            ));
        }
        let descriptor = store
            .descriptor(repo, key)
            .map_err(read_status)?
            .ok_or_else(|| Status::not_found("native graph object unavailable"))?;
        if descriptor.size_bytes > config.max_metadata_bytes.min(config.max_graph_bytes) as u64 {
            return Err(Status::resource_exhausted(
                "native graph metadata exceeds budget",
            ));
        }
        leases.push(store.pin_root(repo, key).map_err(read_status)?);
    }
    Ok(leases)
}

fn graph_difference(
    store: &tinytree_store::Store,
    repo: &str,
    heads: Vec<ObjectKey>,
    common: Vec<ObjectKey>,
    config: &ServiceConfig,
) -> Result<Vec<p::CommitGraphEntry>, Status> {
    fn walk(
        store: &tinytree_store::Store,
        repo: &str,
        roots: Vec<ObjectKey>,
        config: &ServiceConfig,
        bytes: &mut usize,
        objects: &mut usize,
        excluded: Option<&BTreeMap<[u8; 32], p::CommitGraphEntry>>,
    ) -> Result<BTreeMap<[u8; 32], p::CommitGraphEntry>, Status> {
        let mut result = BTreeMap::new();
        let mut seen = HashSet::new();
        let mut stack = roots;
        while let Some(key) = stack.pop() {
            if intrinsic(key)
                || excluded.is_some_and(|skip| skip.contains_key(&key.id.0))
                || !seen.insert(key.id.0)
            {
                continue;
            }
            *objects = objects
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("native graph exceeds object budget"))?;
            if *objects > config.max_graph_objects {
                return Err(Status::resource_exhausted(
                    "native graph exceeds object budget",
                ));
            }
            let descriptor = store
                .descriptor(repo, key)
                .map_err(read_status)?
                .ok_or_else(|| Status::not_found("native graph object unavailable"))?;
            if descriptor.size_bytes > config.max_metadata_bytes as u64 {
                return Err(Status::resource_exhausted(
                    "native graph metadata exceeds budget",
                ));
            }
            if descriptor.size_bytes > config.max_graph_bytes.saturating_sub(*bytes) as u64 {
                return Err(Status::resource_exhausted(
                    "native graph exceeds byte budget",
                ));
            }
            let (descriptor, data) = store.read_metadata(repo, key).map_err(read_status)?;
            *bytes = bytes
                .checked_add(data.capacity())
                .ok_or_else(|| Status::resource_exhausted("native graph exceeds byte budget"))?;
            if *bytes > config.max_graph_bytes {
                return Err(Status::resource_exhausted(
                    "native graph exceeds byte budget",
                ));
            }
            let commit = match tinytree_model::decode(
                &data,
                tinytree_model::Limits {
                    max_object_bytes: config.max_metadata_bytes,
                    ..Default::default()
                },
            )
            .map_err(|_| Status::data_loss("invalid native graph commit"))?
            {
                Object::Commit(c) => c,
                _ => return Err(Status::data_loss("native graph contains a non-commit")),
            };
            let parents: Vec<_> = commit
                .parents
                .into_iter()
                .map(|id| ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(id.0),
                })
                .collect();
            let entry_heap = std::mem::size_of::<p::CommitGraphEntry>()
                .saturating_add(128)
                .saturating_add(
                    parents
                        .len()
                        .saturating_mul(4 * std::mem::size_of::<p::ObjectKey>() + 64),
                );
            *bytes = bytes.checked_add(entry_heap).ok_or_else(|| {
                Status::resource_exhausted("native graph exceeds allocation budget")
            })?;
            if *bytes > config.max_graph_bytes {
                return Err(Status::resource_exhausted(
                    "native graph exceeds allocation budget",
                ));
            }
            stack.extend(parents.iter().copied());
            result.insert(
                key.id.0,
                p::CommitGraphEntry {
                    commit: Some(descriptor_wire(descriptor)),
                    parents: parents.into_iter().map(key_wire).collect(),
                    canonical_envelope: data,
                },
            );
        }
        Ok(result)
    }
    let mut bytes = 0;
    let mut objects = 0;
    let excluded = walk(store, repo, common, config, &mut bytes, &mut objects, None)?;
    let included = walk(
        store,
        repo,
        heads,
        config,
        &mut bytes,
        &mut objects,
        Some(&excluded),
    )?;
    Ok(included.into_values().collect())
}

#[cfg(test)]
mod graph_tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn graph_handler_roots_survive_gc_only_until_physical_traversal_finishes() {
        let temporary = tempfile::tempdir().unwrap();
        let store = tinytree_store::Store::open(temporary.path(), Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let mut commit = tinytree_model::synthetic_root();
        commit.parents = vec![CommitId::ROOT];
        commit.change_id = tinytree_model::ChangeId([1; 16]);
        commit.description = "parent".into();
        let parent =
            tinytree_model::encode(&Object::Commit(commit.clone()), Default::default()).unwrap();
        store
            .put("example", parent.descriptor, &parent.bytes, 0)
            .unwrap();
        commit.parents = vec![CommitId(parent.descriptor.key.id.0)];
        commit.description = "head".into();
        let head =
            tinytree_model::encode(&Object::Commit(commit.clone()), Default::default()).unwrap();
        store
            .put("example", head.descriptor, &head.bytes, 0)
            .unwrap();
        commit.parents = vec![CommitId::ROOT];
        commit.description = "independent common".into();
        let common = tinytree_model::encode(&Object::Commit(commit), Default::default()).unwrap();
        store
            .put("example", common.descriptor, &common.bytes, 0)
            .unwrap();
        let publication = tinytree_store::PublishRequest {
            idempotency_key: "seed-reference".into(),
            reference_name: "main".into(),
            expected_generation: 0,
            new_commit: Some(CommitId(head.descriptor.key.id.0)),
            staging_leases: Vec::new(),
        };
        store.publish("example", &publication, 1).unwrap();
        let service = RepositoryService::new(
            store.clone(),
            ServiceConfig::default(),
            vec!["example".into()],
            Vec::new(),
        )
        .unwrap();
        let (entered, started) = std::sync::mpsc::sync_channel(1);
        let (resume, wait) = std::sync::mpsc::sync_channel(1);
        *service.inner.graph_read_pause.lock().unwrap() = Some(GraphReadPause {
            entered,
            resume: wait,
        });
        let task = tokio::spawn(async move {
            service
                .get_commit_graph(Request::new(p::GetCommitGraphRequest {
                    repository_id: "example".into(),
                    heads: vec![
                        key_wire(head.descriptor.key),
                        key_wire(head.descriptor.key),
                        key_wire(ObjectKey {
                            kind: Kind::Commit,
                            id: ObjectId(CommitId::ROOT.0),
                        }),
                    ],
                    common: vec![
                        key_wire(common.descriptor.key),
                        key_wire(common.descriptor.key),
                    ],
                    page_size: 64,
                    continuation: String::new(),
                }))
                .await
        });
        tokio::task::spawn_blocking(move || started.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        let mut remove = publication;
        remove.idempotency_key = "remove-reference".into();
        remove.expected_generation = 1;
        remove.new_commit = None;
        store.publish("example", &remove, 2).unwrap();
        store.gc("example", i64::MAX - 1).unwrap();
        for key in [
            head.descriptor.key,
            parent.descriptor.key,
            common.descriptor.key,
        ] {
            assert!(
                store.descriptor("example", key).unwrap().is_some(),
                "per-call root lease must retain its ancestry"
            );
        }
        resume.send(()).unwrap();
        let response = task.await.unwrap().unwrap().into_inner();
        assert_eq!(response.entries.len(), 2);
        assert!(response.entries.iter().all(|entry| {
            entry
                .commit
                .as_ref()
                .unwrap()
                .key
                .as_ref()
                .unwrap()
                .native_id
                != common.descriptor.key.id.0
        }));
        drop(response);
        store.gc("example", i64::MAX - 1).unwrap();
        for key in [
            head.descriptor.key,
            parent.descriptor.key,
            common.descriptor.key,
        ] {
            assert!(
                store.descriptor("example", key).unwrap().is_none(),
                "graph call must not create permanent roots"
            );
        }
    }
}
