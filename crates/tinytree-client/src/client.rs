#[path = "capabilities.rs"]
pub(crate) mod capabilities;
#[path = "cloud.rs"]
pub(crate) mod cloud;
#[path = "history.rs"]
pub(crate) mod history;
#[path = "landing.rs"]
pub(crate) mod landing;
#[path = "forge.rs"]
pub(crate) mod forge;
#[path = "forge_resolve.rs"]
mod forge_resolve;
#[path = "forge_review.rs"]
mod forge_review;
#[path = "forge_transform.rs"]
mod forge_transform;
#[path = "query.rs"]
pub(crate) mod query;
#[path = "search.rs"]
pub(crate) mod search;
use crate::transport::Client as RepositoryClient;
use crate::{
    CanonicalCommitSummaries, Config, MetadataOutcome, UploadSource, VerifiedFile,
    VerifiedMetadata,
    verified::{OwnedTemporary, Source, Temporary},
};
use prost::Message;
use std::{
    collections::HashSet,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tinytree_model::{Descriptor, Kind, Object, ObjectId, ObjectKey};
use tinytree_proto::v1 as p;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tonic::{
    Request, Status,
    codec::CompressionEncoding,
    transport::{Certificate, ClientTlsConfig, Endpoint},
};

#[derive(Clone)]
pub struct NativeClient {
    inner: Arc<Inner>,
    landing_budget: Option<Arc<landing::LandingBudget>>,
    guards: crate::verified::LifetimeGuards,
    rpc_guards: crate::verified::LifetimeGuards,
    operation_deadline: Option<Instant>,
    cleanup_deadline: Option<Instant>,
}
struct Inner {
    config: Config,
    transport: RepositoryClient,
    token: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    inflight: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    // Idle query state also consumes `bytes`, but cannot occupy the allowance
    // needed to fetch its next page. Acquire this sub-pool before `bytes`.
    query_state: Arc<Semaphore>,
    disk: Arc<Semaphore>,
    query_cleanup: tokio::sync::mpsc::Sender<query::QueryCleanup>,
}
struct Budget {
    _guards: crate::verified::LifetimeGuards,
    _inflight: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
fn unary_memory(limits: &crate::Limits) -> usize {
    limits.max_batch_bytes.max(limits.max_request_bytes) * 4
}
fn query_headroom(limits: &crate::Limits) -> usize {
    unary_memory(limits).max(query::CLEANUP_MEMORY)
}
impl NativeClient {
    /// Bound successive operations on this clone by one absolute deadline.
    /// Rebinding can only shorten it. The base client, configuration and
    /// authenticated proof identity are unchanged. Query execution retains its
    /// existing Begin-captured whole-query deadline; independent cleanup retains
    /// its own bounded grace period rather than inheriting this transient bound.
    pub fn with_operation_deadline(mut self, deadline: Instant) -> Self {
        self.operation_deadline = Some(
            self.operation_deadline
                .map_or(deadline, |old| old.min(deadline)),
        );
        self
    }
    /// Bound explicit query/scope release attempts independently of the data
    /// operation deadline. Rebinding only shortens this clone's cleanup budget;
    /// queued Drop cleanup keeps its separate configured grace period.
    pub fn with_cleanup_deadline(mut self, deadline: Instant) -> Self {
        self.cleanup_deadline = Some(
            self.cleanup_deadline
                .map_or(deadline, |old| old.min(deadline)),
        );
        self
    }
    fn cleanup_deadline(&self, timeout: Duration) -> Instant {
        let configured = Instant::now() + timeout;
        self.cleanup_deadline
            .map_or(configured, |bound| bound.min(configured))
    }
    /// Pin external resource owners through temporary creation, readers,
    /// snapshots and cancelled blocking work produced by this client. Existing
    /// client clones are unchanged; the guard does not authorize remote access.
    pub fn with_lifetime_guard<T: std::any::Any + Send + Sync>(self, guard: Arc<T>) -> Self {
        let mut guards = self.guards.as_ref().clone();
        guards.push(guard);
        Self {
            inner: self.inner,
            landing_budget: self.landing_budget,
            guards: Arc::new(guards),
            rpc_guards: self.rpc_guards,
            operation_deadline: self.operation_deadline,
            cleanup_deadline: self.cleanup_deadline,
        }
    }
    /// Pin a physical request/job owner through actual codec and blocking work.
    /// Unlike `with_lifetime_guard`, this owner is never captured by query state,
    /// returned pages/metadata/files, or deferred Drop cleanup. Use a transient
    /// clone per operation; existing clones and access authorization are unchanged.
    pub fn with_rpc_lifetime_guard<T: std::any::Any + Send + Sync>(self, guard: Arc<T>) -> Self {
        let mut guards = self.rpc_guards.as_ref().clone();
        guards.push(guard);
        Self {
            inner: self.inner,
            landing_budget: self.landing_budget,
            guards: self.guards,
            rpc_guards: Arc::new(guards),
            operation_deadline: self.operation_deadline,
            cleanup_deadline: self.cleanup_deadline,
        }
    }
    fn without_rpc_guards(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            landing_budget: self.landing_budget.clone(),
            guards: self.guards.clone(),
            rpc_guards: Default::default(),
            // Stored query clients / deferred Drop cleanup must not inherit a
            // caller's transient request bound. The query's explicit execution
            // deadline remains captured separately by Begin.
            operation_deadline: None,
            cleanup_deadline: None,
        }
    }
    fn rpc_lifetime_guards(&self) -> crate::verified::LifetimeGuards {
        if self.rpc_guards.is_empty() {
            return self.guards.clone();
        }
        let mut guards = self.guards.as_ref().clone();
        guards.extend(self.rpc_guards.iter().cloned());
        Arc::new(guards)
    }
    fn rpc_client(&self, budget: Arc<Budget>) -> RepositoryClient {
        self.inner.transport.clone().with_owner(budget)
    }
    pub async fn connect(config: Config) -> Result<Self, Status> {
        Self::connect_with_trust(config, None).await
    }
    /// Connect using captured explicit trust roots instead of rereading a path.
    /// Requires TLS configuration and a nonempty, bounded PEM snapshot. The
    /// configured server name and all ordinary transport checks still apply.
    pub async fn connect_with_ca_certificate(
        config: Config,
        ca: Arc<[u8]>,
    ) -> Result<Self, Status> {
        if config.tls.is_none() || ca.is_empty() || ca.len() > 1024 * 1024 {
            return Err(Status::invalid_argument("invalid native trust snapshot"));
        }
        Self::connect_with_trust(config, Some(ca)).await
    }
    async fn connect_with_trust(
        config: Config,
        ca_snapshot: Option<Arc<[u8]>>,
    ) -> Result<Self, Status> {
        config
            .validate()
            .map_err(|_| Status::invalid_argument("invalid native client configuration"))?;
        let deadline = Instant::now() + config.rpc.operation_timeout();
        let token = match &config.token_env {
            None => None,
            Some(name) => {
                let value = std::env::var(name).map_err(|_| {
                    Status::failed_precondition("cannot resolve native client token")
                })?;
                if !(16..=4096).contains(&value.len())
                    || !value.bytes().all(|b| b.is_ascii_graphic())
                {
                    return Err(Status::failed_precondition("invalid native client token"));
                }
                Some(
                    format!("Bearer {value}")
                        .parse()
                        .map_err(|_| Status::failed_precondition("invalid native client token"))?,
                )
            }
        };
        let transport = if config.transport == crate::Transport::ForgeHttpsV1 {
            let ca = match (&config.tls, ca_snapshot) {
                (_, Some(ca)) => Some(ca),
                (Some(tls), None) => {
                    let path = tls.ca_certificate.clone();
                    Some(Arc::from(
                        blocking(deadline, move || {
                            use std::io::Read;
                            let mut bytes = Vec::new();
                            std::fs::File::open(path)
                                .and_then(|file| file.take(1024 * 1024 + 1).read_to_end(&mut bytes))
                                .map_err(|_| {
                                    Status::failed_precondition("cannot read native trust roots")
                                })?;
                            if bytes.is_empty() || bytes.len() > 1024 * 1024 {
                                return Err(Status::resource_exhausted(
                                    "native trust roots exceed limit",
                                ));
                            }
                            Ok(bytes)
                        })
                        .await?,
                    ))
                }
                (None, None) => None,
            };
            RepositoryClient::https(
                crate::forge_http::Client::new(&config, ca)?,
                config.limits.clone(),
            )
        } else {
            let channel = if let Some(path) = config.endpoint.strip_prefix("unix://") {
                #[cfg(unix)]
                {
                    let checked = PathBuf::from(path);
                    blocking(deadline, move || secure_unix(&checked)).await?;
                    let path = PathBuf::from(path);
                    let endpoint = Endpoint::from_static("http://[::]:50051")
                        .connect_timeout(config.rpc.connect_timeout());
                    reapi::policy::timeout(deadline, async {
                        endpoint
                            .connect_with_connector(tower::service_fn(move |_| {
                                let path = path.clone();
                                async move {
                                    tokio::net::UnixStream::connect(path)
                                        .await
                                        .map(hyper_util::rt::TokioIo::new)
                                }
                            }))
                            .await
                            .map_err(|_| {
                                Status::unavailable("cannot connect to native Unix endpoint")
                            })
                    })
                    .await?
                }
                #[cfg(not(unix))]
                {
                    let _ = path;
                    return Err(Status::failed_precondition("Unix transport unavailable"));
                }
            } else {
                let mut endpoint = Endpoint::from_shared(config.endpoint.clone())
                    .map_err(|_| Status::invalid_argument("invalid native endpoint"))?
                    .connect_timeout(config.rpc.connect_timeout());
                if let Some(tls) = &config.tls {
                    let path = tls.ca_certificate.clone();
                    let ca = if let Some(ca) = ca_snapshot {
                        ca.as_ref().to_vec()
                    } else {
                        blocking(deadline, move || {
                            use std::io::Read;
                            let mut bytes = Vec::new();
                            std::fs::File::open(path)
                                .map_err(|_| {
                                    Status::failed_precondition("cannot read native trust roots")
                                })?
                                .take(1024 * 1024 + 1)
                                .read_to_end(&mut bytes)
                                .map_err(|_| {
                                    Status::failed_precondition("cannot read native trust roots")
                                })?;
                            if bytes.is_empty() || bytes.len() > 1024 * 1024 {
                                return Err(Status::resource_exhausted(
                                    "native trust roots exceed limit",
                                ));
                            }
                            Ok(bytes)
                        })
                        .await?
                    };
                    let mut settings =
                        ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca));
                    if let Some(domain) = &tls.domain_name {
                        settings = settings.domain_name(domain);
                    }
                    endpoint = endpoint.tls_config(settings).map_err(|_| {
                        Status::failed_precondition("invalid native TLS configuration")
                    })?;
                }
                config
                    .rpc
                    .retry(deadline, config.rpc.connect_timeout(), |_| {
                        let endpoint = endpoint.clone();
                        async move {
                            endpoint.connect().await.map_err(|_| {
                                Status::unavailable("cannot connect to native endpoint")
                            })
                        }
                    })
                    .await?
            };
            let mut grpc = RepositoryClient::new(channel, config.limits.clone())
                .max_decoding_message_size(config.limits.max_batch_bytes)
                .max_encoding_message_size(config.limits.max_request_bytes);
            if config.compression {
                grpc = grpc
                    .send_compressed(CompressionEncoding::Zstd)
                    .accept_compressed(CompressionEncoding::Zstd);
            }
            grpc
        };
        let (query_cleanup, cleanup_receiver) =
            tokio::sync::mpsc::channel(config.limits.query_cleanup_capacity);
        let client = Self {
            operation_deadline: None,
            cleanup_deadline: None,
            landing_budget: None,
            guards: Default::default(),
            rpc_guards: Default::default(),
            inner: Arc::new(Inner {
                inflight: Arc::new(Semaphore::new(config.limits.max_inflight)),
                bytes: Arc::new(Semaphore::new(config.limits.max_buffered_bytes)),
                query_state: Arc::new(Semaphore::new(
                    config
                        .limits
                        .max_buffered_bytes
                        .saturating_sub(query_headroom(&config.limits)),
                )),
                disk: Arc::new(Semaphore::new(config.limits.max_temporary_bytes as usize)),
                config,
                transport,
                token,
                query_cleanup,
            }),
        };
        let worker = query::CleanupWorker::new(&client);
        tokio::spawn(worker.run(cleanup_receiver));
        Ok(client)
    }
    fn deadline(&self) -> Instant {
        let configured = Instant::now() + self.inner.config.rpc.operation_timeout();
        self.operation_deadline
            .map_or(configured, |bound| bound.min(configured))
    }
    async fn admit(&self, deadline: Instant, bytes: usize) -> Result<Arc<Budget>, Status> {
        if bytes > self.inner.config.limits.max_buffered_bytes {
            return Err(Status::resource_exhausted(
                "native client memory budget exceeded",
            ));
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
        let bytes = reapi::policy::timeout(deadline, async {
            self.inner
                .bytes
                .clone()
                .acquire_many_owned(bytes as u32)
                .await
                .map_err(|_| Status::cancelled("native client closed"))
        })
        .await?;
        Ok(Arc::new(Budget {
            _inflight: inflight,
            _bytes: bytes,
            _guards: self.rpc_lifetime_guards(),
        }))
    }
    fn request<T>(&self, value: T, allowance: Duration) -> Request<T> {
        let mut request = Request::new(value);
        request.set_timeout(allowance);
        if let Some(token) = &self.inner.token {
            request
                .metadata_mut()
                .insert("authorization", token.clone());
        }
        request
    }
    fn unary_memory(&self) -> usize {
        unary_memory(&self.inner.config.limits)
    }
    /// Bound small, uncompressed query replies without charging a metadata page.
    /// Query validation bounds each aggregate of facts, AST nodes and IDs by
    /// max_batch_items. Include retained inputs, request clones/encoding, vector
    /// growth and output conversion. The codec separately scans before Prost
    /// with a collection allocation budget of twice the response byte cap.
    fn small_query_memory(&self, response_cap: usize) -> Result<Option<usize>, Status> {
        use tinytree_proto::query_v1 as q;
        let containers = std::mem::size_of::<q::GraphExpr>()
            + std::mem::size_of::<q::GraphFact>()
            + std::mem::size_of::<q::CommitId>()
            + std::mem::size_of::<q::GraphFactAnswer>();
        let limits = &self.inner.config.limits;
        let memory = limits
            .max_request_bytes
            .checked_mul(4)
            .and_then(|n| {
                limits
                    .max_batch_items
                    .checked_mul(containers)
                    .and_then(|v| v.checked_mul(16))
                    .and_then(|v| n.checked_add(v))
            })
            .and_then(|n| response_cap.checked_mul(8).and_then(|v| n.checked_add(v)))
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| Status::resource_exhausted("small query memory budget overflow"))?;
        // Preserve tighter configurations' existing admission and transport.
        Ok((memory < self.unary_memory()).then_some(memory))
    }
    fn bounded<T: Message>(&self, value: &T) -> Result<(), Status> {
        if value.encoded_len() > self.inner.config.limits.max_request_bytes {
            Err(Status::resource_exhausted(
                "native request exceeds byte budget",
            ))
        } else {
            Ok(())
        }
    }
    fn batch(&self, count: usize) -> Result<(), Status> {
        if count > self.inner.config.limits.max_batch_items {
            Err(Status::resource_exhausted(
                "native batch exceeds item budget",
            ))
        } else {
            Ok(())
        }
    }
    async fn retry<T, F, Fut>(&self, deadline: Instant, stream: bool, f: F) -> Result<T, Status>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        self.inner
            .config
            .rpc
            .retry(
                deadline,
                if stream {
                    self.inner.config.rpc.operation_timeout()
                } else {
                    self.inner.config.rpc.rpc_timeout()
                },
                f,
            )
            .await
    }
    pub async fn open(&self, names: Vec<String>) -> Result<p::RepositoryView, Status> {
        let deadline = self.deadline();
        self.batch(names.len())?;
        let budget = self.admit(deadline, self.unary_memory()).await?;
        if names
            .iter()
            .any(|name| name.is_empty() || name.len() > 4096)
            || names.iter().collect::<HashSet<_>>().len() != names.len()
        {
            return Err(Status::invalid_argument(
                "invalid or duplicate native reference names",
            ));
        }
        if names.iter().map(String::len).sum::<usize>() > self.inner.config.limits.max_request_bytes
        {
            return Err(Status::resource_exhausted(
                "native reference names exceed request budget",
            ));
        }
        self.bounded(&p::OpenRepositoryRequest {
            repository_id: self.inner.config.repository_id.clone(),
            reference_names: names.clone(),
        })?;

        self.retry(deadline, false, |allowance| {
            let mut grpc = self
                .rpc_client(budget.clone())
                .max_decoding_message_size(self.inner.config.limits.max_batch_bytes);
            let names = names.clone();
            async move {
                let view = grpc
                    .open_repository(self.request(
                        p::OpenRepositoryRequest {
                            repository_id: self.inner.config.repository_id.clone(),
                            reference_names: names.clone(),
                        },
                        allowance,
                    ))
                    .await?
                    .into_inner();
                if view.encoded_len() > self.inner.config.limits.max_batch_bytes
                    || view.repository_id != self.inner.config.repository_id
                    || view.protocol_major != 1
                    || view.object_format_version != u32::from(tinytree_model::FORMAT_VERSION)
                    || view.references.len() != names.len()
                    || view.max_batch_items == 0
                    || view.max_frame_bytes == 0
                    || view.max_request_bytes == 0
                {
                    return Err(integrity());
                }
                let mut expected: HashSet<_> = names.into_iter().collect();
                for reference in &view.references {
                    validate_reference(reference)?;
                    if !expected.remove(&reference.name) {
                        return Err(integrity());
                    }
                }
                if !expected.is_empty() {
                    return Err(integrity());
                }
                Ok(view)
            }
        })
        .await
    }
    pub async fn find_missing(&self, mut keys: Vec<ObjectKey>) -> Result<Vec<ObjectKey>, Status> {
        let deadline = self.deadline();
        self.batch(keys.len())?;
        let budget = self.admit(deadline, self.unary_memory()).await?;
        let mut unique = HashSet::new();
        keys.retain(|key| unique.insert(*key));
        self.bounded(&p::FindMissingObjectsRequest {
            repository_id: self.inner.config.repository_id.clone(),
            keys: keys.iter().copied().map(wire_key).collect(),
        })?;

        self.retry(deadline, false, |allowance| {
            let mut grpc = self
                .rpc_client(budget.clone())
                .max_decoding_message_size(self.inner.config.limits.max_batch_bytes);
            let keys = keys.clone();
            async move {
                let response = grpc
                    .find_missing_objects(self.request(
                        p::FindMissingObjectsRequest {
                            repository_id: self.inner.config.repository_id.clone(),
                            keys: keys.iter().copied().map(wire_key).collect(),
                        },
                        allowance,
                    ))
                    .await?
                    .into_inner();
                let requested: HashSet<_> = keys.into_iter().collect();
                let mut seen = HashSet::new();
                let mut missing = Vec::new();
                for key in response.keys {
                    let key = parse_key(&key)?;
                    if intrinsic(key) || !requested.contains(&key) || !seen.insert(key) {
                        return Err(integrity());
                    }
                    missing.push(key);
                }
                Ok(missing)
            }
        })
        .await
    }
    pub async fn read_objects(&self, keys: Vec<ObjectKey>) -> Result<Vec<MetadataOutcome>, Status> {
        self.batch(keys.len())?;
        let deadline = self.deadline();
        let budget = self
            .admit(deadline, self.inner.config.limits.metadata_memory())
            .await?;
        self.metadata(keys, deadline, budget).await
    }
    async fn metadata(
        &self,
        keys: Vec<ObjectKey>,
        deadline: Instant,
        budget: Arc<Budget>,
    ) -> Result<Vec<MetadataOutcome>, Status> {
        self.batch(keys.len())?;
        self.bounded(&p::BatchReadObjectsRequest {
            repository_id: self.inner.config.repository_id.clone(),
            keys: keys.iter().copied().map(wire_key).collect(),
        })?;
        if keys
            .iter()
            .any(|key| key.kind == Kind::File || intrinsic(*key))
        {
            return Err(Status::invalid_argument(
                "metadata RPC requires stored metadata keys",
            ));
        }
        let state = Arc::new(tokio::sync::Mutex::new((
            keys.iter()
                .map(|_| None)
                .collect::<Vec<Option<MetadataOutcome>>>(),
            0usize,
        )));
        self.retry(deadline, true, |allowance| {
            let mut grpc = self.rpc_client(budget.clone()).max_decoding_message_size(
                self.inner
                    .config
                    .limits
                    .max_metadata_bytes
                    .saturating_add(1024),
            );
            let state = state.clone();
            let keys = keys.clone();
            let budget = budget.clone();
            async move {
                let pending: Vec<_> = {
                    let state = state.lock().await;
                    keys.iter()
                        .copied()
                        .enumerate()
                        .filter(|(i, _)| state.0[*i].is_none())
                        .collect()
                };
                if pending.is_empty() {
                    return Ok(());
                }
                let mut body = grpc
                    .batch_read_objects(self.request(
                        p::BatchReadObjectsRequest {
                            repository_id: self.inner.config.repository_id.clone(),
                            keys: pending.iter().map(|(_, key)| wire_key(*key)).collect(),
                        },
                        allowance,
                    ))
                    .await?
                    .into_inner();
                let mut received = HashSet::new();
                let mut unavailable = false;
                while let Some(item) = self.message(&mut body, deadline).await? {
                    let index = item.request_index as usize;
                    if index >= pending.len()
                        || !received.insert(index)
                        || item.key.as_ref().map(parse_key).transpose()? != Some(pending[index].1)
                    {
                        return Err(integrity());
                    }
                    let original = pending[index].0;
                    let outcome = match item.result {
                        Some(p::object_result::Result::Object(data)) => {
                            let descriptor =
                                parse_descriptor(data.descriptor.as_ref().ok_or_else(integrity)?)?;
                            if descriptor.key != pending[index].1
                                || descriptor.size_bytes
                                    > self.inner.config.limits.max_metadata_bytes as u64
                            {
                                return Err(integrity());
                            }
                            {
                                let mut state = state.lock().await;
                                state.1 = state
                                    .1
                                    .checked_add(data.canonical_envelope.len())
                                    .ok_or_else(integrity)?;
                                if state.1 > self.inner.config.limits.max_batch_bytes {
                                    return Err(Status::resource_exhausted(
                                        "native metadata batch exceeds byte budget",
                                    ));
                                }
                            }
                            Some(MetadataOutcome::Object(
                                verify_metadata(
                                    descriptor,
                                    data.canonical_envelope,
                                    self.inner.config.limits.max_metadata_bytes,
                                    None,
                                    deadline,
                                    budget.clone(),
                                )
                                .await?,
                            ))
                        }
                        Some(p::object_result::Result::Error(error)) => {
                            let code = p::item_error::Code::try_from(error.code)
                                .map_err(|_| integrity())?;
                            match code {
                                p::item_error::Code::Unavailable => {
                                    unavailable = true;
                                    None
                                }
                                p::item_error::Code::NotFound
                                | p::item_error::Code::ResourceExhausted
                                | p::item_error::Code::InvalidObject => {
                                    Some(MetadataOutcome::Error { code })
                                }
                                _ => return Err(integrity()),
                            }
                        }
                        None => return Err(integrity()),
                    };
                    if let Some(outcome) = outcome {
                        state.lock().await.0[original] = Some(outcome);
                    }
                }
                if received.len() != pending.len() {
                    return Err(integrity());
                }
                if unavailable {
                    Err(Status::unavailable(
                        "native metadata items temporarily unavailable",
                    ))
                } else {
                    Ok(())
                }
            }
        })
        .await?;
        let state = Arc::try_unwrap(state)
            .map_err(|_| Status::internal("native metadata operation retained state"))?
            .into_inner();
        state
            .0
            .into_iter()
            .map(|item| item.ok_or_else(integrity))
            .collect()
    }
    /// Fetch bounded server-selected summaries and cross-check each against its
    /// canonical commit. The set's completeness/topology remains untrusted.
    pub async fn canonical_commit_summaries(
        &self,
        heads: Vec<ObjectKey>,
        common: Vec<ObjectKey>,
    ) -> Result<CanonicalCommitSummaries, Status> {
        self.batch(heads.len().saturating_add(common.len()))?;
        if heads
            .iter()
            .chain(&common)
            .any(|key| key.kind != Kind::Commit)
        {
            return Err(Status::invalid_argument(
                "native graph requires commit keys",
            ));
        }
        let deadline = self.deadline();
        let budget = self
            .admit(deadline, self.inner.config.limits.graph_memory())
            .await?;
        let mut continuation = String::new();
        let mut tokens = HashSet::new();
        let mut entries = Vec::new();
        let mut seen = HashSet::new();
        let mut bytes = 0usize;
        let mut retained_heap = 0usize;
        for _ in 0..self.inner.config.limits.max_graph_pages {
            let request = p::GetCommitGraphRequest {
                repository_id: self.inner.config.repository_id.clone(),
                heads: heads.iter().copied().map(wire_key).collect(),
                common: common.iter().copied().map(wire_key).collect(),
                page_size: self.inner.config.limits.max_batch_items as u32,
                continuation: continuation.clone(),
            };
            self.bounded(&request)?;
            let mut page = self
                .retry(deadline, false, |allowance| {
                    let mut grpc = self
                        .rpc_client(budget.clone())
                        .max_decoding_message_size(self.inner.config.limits.max_graph_bytes);
                    let request = request.clone();
                    async move {
                        Ok(grpc
                            .get_commit_graph(self.request(request, allowance))
                            .await?
                            .into_inner())
                    }
                })
                .await?;
            bytes = bytes
                .checked_add(page.encoded_len())
                .ok_or_else(integrity)?;
            if bytes > self.inner.config.limits.max_graph_bytes
                || page.entries.len() > self.inner.config.limits.max_batch_items
                || entries.len().saturating_add(page.entries.len())
                    > self.inner.config.limits.max_graph_objects
                || page.continuation.len() > 4096
            {
                return Err(Status::resource_exhausted(
                    "native graph exceeds client budget",
                ));
            }
            let mut keys = Vec::new();
            for entry in &page.entries {
                let allocation = entry.canonical_envelope.capacity().saturating_add(
                    std::mem::size_of::<p::CommitGraphEntry>()
                        .saturating_mul(4)
                        .saturating_add(
                            entry.parents.len().saturating_mul(
                                std::mem::size_of::<p::ObjectKey>()
                                    .saturating_mul(4)
                                    .saturating_add(64),
                            ),
                        )
                        .saturating_add(128)
                        .saturating_add(
                            std::mem::size_of::<ObjectKey>()
                                .saturating_mul(4)
                                .saturating_add(16),
                        ),
                );
                retained_heap = retained_heap
                    .checked_add(allocation)
                    .ok_or_else(integrity)?;
                if retained_heap > self.inner.config.limits.max_graph_bytes {
                    return Err(Status::resource_exhausted(
                        "native retained graph summaries exceed allocation budget",
                    ));
                }
            }
            for entry in &page.entries {
                let descriptor = parse_descriptor(entry.commit.as_ref().ok_or_else(integrity)?)?;
                if descriptor.key.kind != Kind::Commit
                    || intrinsic(descriptor.key)
                    || !seen.insert(descriptor.key)
                {
                    return Err(integrity());
                }
                for parent in &entry.parents {
                    if parse_key(parent)?.kind != Kind::Commit {
                        return Err(integrity());
                    }
                }
                keys.push(descriptor.key);
            }
            // An absent payload is the additive old-server compatibility path.
            // A nonempty invalid payload is never replaced by a metadata RPC.
            for (entry, key) in page.entries.iter_mut().zip(keys) {
                let expected = parse_descriptor(entry.commit.as_ref().unwrap())?;
                let direct = !entry.canonical_envelope.is_empty();
                let metadata = if !direct {
                    let mut canonical = self.metadata(vec![key], deadline, budget.clone()).await?;
                    match canonical.pop().ok_or_else(integrity)? {
                        MetadataOutcome::Object(object) => object,
                        MetadataOutcome::Error { code } => return Err(item_status(code)),
                    }
                } else {
                    if entry.canonical_envelope.len() > self.inner.config.limits.max_metadata_bytes
                        || expected.size_bytes != entry.canonical_envelope.len() as u64
                    {
                        return Err(integrity());
                    }
                    // Input bytes already count toward retained_heap. Additional
                    // decoded strings plus encoder capacity use at most 3x bytes;
                    // four independent commit vectors use at most 128 bytes/item.
                    // Bound each vector BEFORE decode, including empty labels.
                    let fixed = std::mem::size_of::<tinytree_model::Commit>();
                    let available = self
                        .inner
                        .config
                        .limits
                        .max_graph_bytes
                        .checked_sub(retained_heap)
                        .and_then(|n| {
                            n.checked_sub(entry.canonical_envelope.len().saturating_mul(3))
                        })
                        .and_then(|n| n.checked_sub(fixed))
                        .ok_or_else(|| {
                            Status::resource_exhausted(
                                "native graph verification exceeds allocation budget",
                            )
                        })?;
                    let items = (available / 128).min(tinytree_model::Limits::default().max_items);
                    if items == 0 {
                        return Err(Status::resource_exhausted(
                            "native graph verification exceeds allocation budget",
                        ));
                    }
                    verify_metadata(
                        expected,
                        std::mem::take(&mut entry.canonical_envelope),
                        self.inner.config.limits.max_metadata_bytes,
                        Some(items),
                        deadline,
                        budget.clone(),
                    )
                    .await?
                };
                let commit = match &metadata.object {
                    Object::Commit(commit) => commit,
                    _ => return Err(integrity()),
                };
                if retained_heap.saturating_add(commit_heap(commit))
                    > self.inner.config.limits.max_graph_bytes
                {
                    return Err(Status::resource_exhausted(
                        "native graph canonical object exceeds allocation budget",
                    ));
                }
                if expected != metadata.descriptor
                    || entry.parents.len() != commit.parents.len()
                    || !entry
                        .parents
                        .iter()
                        .zip(&commit.parents)
                        .all(|(wire, id)| parse_key(wire).is_ok_and(|key| key.id.0 == id.0))
                {
                    return Err(integrity());
                }
                // Preserve canonical payloads supplied by the server. Legacy
                // summaries remain payload-free rather than retaining extra RPC
                // allocations not charged in their original response budget.
                if direct {
                    entry.canonical_envelope = metadata.bytes;
                }
            }
            continuation = page.continuation;
            entries.extend(page.entries);
            if continuation.is_empty() {
                return Ok(CanonicalCommitSummaries { entries });
            }
            retained_heap = retained_heap
                .checked_add(
                    continuation.len().saturating_mul(2).saturating_add(
                        std::mem::size_of::<String>()
                            .saturating_mul(4)
                            .saturating_add(32),
                    ),
                )
                .ok_or_else(integrity)?;
            if retained_heap > self.inner.config.limits.max_graph_bytes {
                return Err(Status::resource_exhausted(
                    "native graph continuations exceed allocation budget",
                ));
            }
            if !tokens.insert(continuation.clone()) {
                return Err(integrity());
            }
        }
        Err(Status::resource_exhausted(
            "native graph exceeds page budget",
        ))
    }
    async fn message<T: Message + Default>(
        &self,
        body: &mut tonic::Streaming<T>,
        deadline: Instant,
    ) -> Result<Option<T>, Status> {
        reapi::policy::timeout(
            deadline.min(Instant::now() + self.inner.config.rpc.stream_idle_timeout()),
            async { body.message().await },
        )
        .await
    }
    async fn temporary(
        &self,
        parent: &Path,
        deadline: Instant,
        budget: Arc<Budget>,
    ) -> Result<Temporary, Status> {
        let n = self.inner.config.limits.max_file_bytes as u32;
        let disk = Arc::new(
            reapi::policy::timeout(deadline, async {
                self.inner
                    .disk
                    .clone()
                    .acquire_many_owned(n)
                    .await
                    .map_err(|_| Status::cancelled("native client closed"))
            })
            .await?,
        );
        let parent = parent.to_path_buf();
        let guards = self.guards.clone();
        blocking_owned(deadline, budget, None, move || {
            let parent = Arc::new(secure_directory(&parent)?);
            let file = tempfile::Builder::new()
                .prefix("tinytree-")
                .disable_cleanup(true)
                .tempfile_in(parent.path())
                .map_err(|_| Status::internal("cannot create native temporary file"))?;
            Ok(Temporary {
                file: Arc::new(OwnedTemporary::new(file)?),
                _disk: disk,
                _guards: {
                    let mut guards = guards.as_ref().clone();
                    guards.push(parent);
                    Arc::new(guards)
                },
            })
        })
        .await
    }
    pub async fn download_file(&self, id: ObjectId, parent: &Path) -> Result<VerifiedFile, Status> {
        if parent.as_os_str().len() > 4096 {
            return Err(Status::invalid_argument(
                "native temporary directory path too long",
            ));
        }
        let deadline = self.deadline();
        let budget = self
            .admit(
                deadline,
                self.inner.config.limits.max_frame_bytes * 4 + 1024,
            )
            .await?;
        self.retry(deadline, true, |allowance| {
            let mut grpc = self.rpc_client(budget.clone()).max_decoding_message_size(
                self.inner
                    .config
                    .limits
                    .max_frame_bytes
                    .saturating_add(1024),
            );
            let budget = budget.clone();
            async move {
                let temporary = self.temporary(parent, deadline, budget.clone()).await?;
                let held = temporary.clone();
                let memory = budget.clone();
                let mut file = blocking_owned(
                    deadline,
                    budget.clone(),
                    Some(temporary.clone()),
                    move || {
                        let _memory = memory;
                        held.file
                            .reopen()
                            .map_err(|_| Status::internal("cannot open native temporary file"))
                    },
                )
                .await?;
                let mut body = grpc
                    .read_file(self.request(
                        p::ReadFileRequest {
                            repository_id: self.inner.config.repository_id.clone(),
                            native_file_id: id.0.to_vec(),
                        },
                        allowance,
                    ))
                    .await?
                    .into_inner();
                let header = self
                    .message(&mut body, deadline)
                    .await?
                    .ok_or_else(integrity)?;
                let descriptor = match header.frame {
                    Some(p::file_frame::Frame::Header(d)) => parse_descriptor(&d)?,
                    _ => return Err(integrity()),
                };
                if descriptor.key
                    != (ObjectKey {
                        kind: Kind::File,
                        id,
                    })
                    || descriptor.size_bytes > self.inner.config.limits.max_file_bytes
                {
                    return Err(integrity());
                }
                let mut offset = 0u64;
                let mut hash = blake3::Hasher::new();
                while let Some(frame) = self.message(&mut body, deadline).await? {
                    let chunk = match frame.frame {
                        Some(p::file_frame::Frame::Chunk(chunk)) => chunk,
                        _ => return Err(integrity()),
                    };
                    if chunk.offset != offset
                        || chunk.data.is_empty()
                        || chunk.data.len() > self.inner.config.limits.max_frame_bytes
                        || offset.saturating_add(chunk.data.len() as u64) > descriptor.size_bytes
                    {
                        return Err(integrity());
                    }
                    hash.update(&chunk.data);
                    offset += chunk.data.len() as u64;
                    let held = temporary.clone();
                    let memory = budget.clone();
                    file = blocking_owned(
                        deadline,
                        budget.clone(),
                        Some(temporary.clone()),
                        move || {
                            use std::io::Write;
                            let (_held, _memory) = (held, memory);
                            file.write_all(&chunk.data).map_err(|_| {
                                Status::internal("cannot write native temporary file")
                            })?;
                            Ok(file)
                        },
                    )
                    .await?;
                }
                if offset != descriptor.size_bytes || hash.finalize().as_bytes() != &id.0 {
                    return Err(integrity());
                }
                drop(file);
                Ok(VerifiedFile {
                    temporary,
                    descriptor,
                })
            }
        })
        .await
    }
    pub async fn prepare_upload_file(
        &self,
        path: &Path,
        expected: Descriptor,
        parent: &Path,
    ) -> Result<UploadSource, Status> {
        if path.as_os_str().len() > 4096 || parent.as_os_str().len() > 4096 {
            return Err(Status::invalid_argument("native snapshot path too long"));
        }
        if expected.key.kind != Kind::File
            || expected.format_version != tinytree_model::FORMAT_VERSION
            || expected.size_bytes > self.inner.config.limits.max_file_bytes
        {
            return Err(Status::invalid_argument("invalid native upload descriptor"));
        }
        let deadline = self.deadline();
        let budget = self
            .admit(
                deadline,
                self.inner.config.limits.max_frame_bytes * 4 + 1024,
            )
            .await?;
        let temporary = self.temporary(parent, deadline, budget.clone()).await?;
        let held = temporary.clone();
        let path = path.to_path_buf();
        let frame_bytes = self.inner.config.limits.max_frame_bytes;
        blocking_owned(deadline, budget, Some(temporary.clone()), move || {
            use std::io::{Read, Write};
            let mut options = std::fs::OpenOptions::new();
            options.read(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
            }
            let mut source = options
                .open(path)
                .map_err(|_| Status::invalid_argument("cannot open native upload source"))?;
            let metadata = source
                .metadata()
                .map_err(|_| Status::invalid_argument("cannot inspect native upload source"))?;
            if !metadata.is_file() || metadata.len() != expected.size_bytes {
                return Err(Status::invalid_argument(
                    "native upload source must be a regular file of declared size",
                ));
            }
            let mut output = held
                .file
                .reopen()
                .map_err(|_| Status::internal("cannot open native upload snapshot"))?;
            let mut hash = blake3::Hasher::new();
            let mut length = 0u64;
            let mut buffer = vec![0; frame_bytes];
            loop {
                if Instant::now() >= deadline {
                    return Err(Status::deadline_exceeded(
                        "native snapshot deadline exceeded",
                    ));
                }
                let n = source
                    .read(&mut buffer)
                    .map_err(|_| Status::invalid_argument("cannot read native upload source"))?;
                if n == 0 {
                    break;
                }
                length = length.saturating_add(n as u64);
                if length > expected.size_bytes {
                    return Err(integrity());
                }
                hash.update(&buffer[..n]);
                output
                    .write_all(&buffer[..n])
                    .map_err(|_| Status::internal("cannot write native upload snapshot"))?;
            }
            if length != expected.size_bytes || hash.finalize().as_bytes() != &expected.key.id.0 {
                return Err(integrity());
            }
            Ok(())
        })
        .await?;
        Ok(UploadSource {
            inner: Arc::new(Source {
                temporary,
                descriptor: expected,
            }),
        })
    }
    pub async fn verify_metadata(
        &self,
        descriptor: Descriptor,
        bytes: Vec<u8>,
    ) -> Result<VerifiedMetadata, Status> {
        if bytes.len() > self.inner.config.limits.max_metadata_bytes
            || descriptor.size_bytes != bytes.len() as u64
        {
            return Err(Status::invalid_argument(
                "native metadata input exceeds declared client bounds",
            ));
        }
        let deadline = self.deadline();
        let budget = self
            .admit(deadline, self.inner.config.limits.max_metadata_bytes * 4)
            .await?;
        verify_metadata(
            descriptor,
            bytes,
            self.inner.config.limits.max_metadata_bytes,
            None,
            deadline,
            budget,
        )
        .await
    }
    pub async fn upload_metadata(
        &self,
        object: VerifiedMetadata,
    ) -> Result<p::WriteReceipt, Status> {
        if object.descriptor.size_bytes > self.inner.config.limits.max_metadata_bytes as u64 {
            return Err(Status::resource_exhausted(
                "native metadata upload exceeds client budget",
            ));
        }
        let deadline = self.deadline();
        let budget = self
            .admit(deadline, self.inner.config.limits.max_metadata_bytes * 4)
            .await?;
        let object = Arc::new(object);
        let descriptor = object.descriptor;
        self.retry(deadline,true,|allowance| {let mut grpc=self.rpc_client(budget.clone()).max_decoding_message_size(1024).max_encoding_message_size(self.inner.config.limits.max_frame_bytes.saturating_add(1024));let object=object.clone();let budget=budget.clone();async move {
            let frame_bytes=self.inner.config.limits.max_frame_bytes;let repo=self.inner.config.repository_id.clone();
            let (done,finished)=tokio::sync::oneshot::channel();
            let stream=async_stream::stream!{let _budget=budget;yield header(&repo,descriptor);for (i,bytes) in object.bytes.chunks(frame_bytes).enumerate(){yield p::WriteFrame{frame:Some(p::write_frame::Frame::Chunk(p::DataChunk{offset:(i*frame_bytes) as u64,data:bytes.to_vec()}))};}let _=done.send(());};
            let receipt=grpc.write_object(self.request(stream,allowance)).await?.into_inner();
            reapi::policy::timeout(deadline,async{finished.await.map_err(|_|integrity())}).await?;
            validate_write_receipt(&receipt,descriptor)?;Ok(receipt)
        }}).await
    }
    pub async fn upload_file(&self, source: UploadSource) -> Result<p::WriteReceipt, Status> {
        if source.descriptor().size_bytes > self.inner.config.limits.max_file_bytes {
            return Err(Status::resource_exhausted(
                "native upload exceeds file budget",
            ));
        }
        let deadline = self.deadline();
        let budget = self
            .admit(
                deadline,
                self.inner.config.limits.max_frame_bytes * 4 + 1024,
            )
            .await?;
        self.retry(deadline,true,|allowance| {let mut grpc=self.rpc_client(budget.clone()).max_decoding_message_size(1024).max_encoding_message_size(self.inner.config.limits.max_frame_bytes.saturating_add(1024));let source=source.clone();let budget=budget.clone();async move {
            let descriptor=source.descriptor();let frame_bytes=self.inner.config.limits.max_frame_bytes;let repo=self.inner.config.repository_id.clone();
            let held=source.clone();let memory=budget.clone();let file=blocking_owned(deadline,budget.clone(),Some(source.inner.temporary.clone()),move||{let _memory=memory;held.inner.temporary.file.reopen().map_err(|_|Status::internal("cannot reopen native upload snapshot"))}).await?;
            let (done,finished)=tokio::sync::oneshot::channel();
            let stream=async_stream::stream! {
                let mut file=file;let mut offset=0u64;let mut hash=blake3::Hasher::new();let mut outcome=Ok(());
                yield header(&repo,descriptor);
                loop {
                    let held=source.clone();let memory=budget.clone();
                    let read=blocking_owned(deadline,budget.clone(),Some(source.inner.temporary.clone()),move||{use std::io::Read;let (_held,_memory)=(held,memory);let mut bytes=vec![0;frame_bytes];let n=file.read(&mut bytes).map_err(|_|Status::internal("cannot read native upload snapshot"))?;bytes.truncate(n);Ok((file,bytes))}).await;
                    let (next,bytes)=match read{Ok(value)=>value,Err(error)=>{outcome=Err(error);break;}};file=next;
                    if bytes.is_empty(){break;}
                    if offset.saturating_add(bytes.len() as u64)>descriptor.size_bytes{outcome=Err(integrity());break;}
                    hash.update(&bytes);let chunk=p::DataChunk{offset,data:bytes};offset+=chunk.data.len() as u64;
                    yield p::WriteFrame{frame:Some(p::write_frame::Frame::Chunk(chunk))};
                }
                if outcome.is_ok()&&(offset!=descriptor.size_bytes||hash.finalize().as_bytes()!=&descriptor.key.id.0){outcome=Err(integrity());}
                let _=done.send(outcome);
            };
            let receipt=grpc.write_object(self.request(stream,allowance)).await?.into_inner();
            let outcome=reapi::policy::timeout(deadline,async{finished.await.map_err(|_|integrity())?}).await?;
            let _=outcome;validate_write_receipt(&receipt,descriptor)?;Ok(receipt)
        }}).await
    }
    pub async fn publication(&self, idempotency_key: &str) -> Result<p::Publication, Status> {
        validate_idempotency(idempotency_key)?;
        let deadline = self.deadline();
        let budget = self.admit(deadline, self.unary_memory()).await?;
        self.publication_at(idempotency_key, deadline, budget).await
    }
    async fn publication_at(
        &self,
        id: &str,
        deadline: Instant,
        budget: Arc<Budget>,
    ) -> Result<p::Publication, Status> {
        self.bounded(&p::GetPublicationRequest {
            repository_id: self.inner.config.repository_id.clone(),
            idempotency_key: id.into(),
        })?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self
                .rpc_client(budget.clone())
                .max_decoding_message_size(self.inner.config.limits.max_batch_bytes);
            async move {
                let publication = grpc
                    .get_publication(self.request(
                        p::GetPublicationRequest {
                            repository_id: self.inner.config.repository_id.clone(),
                            idempotency_key: id.into(),
                        },
                        allowance,
                    ))
                    .await?
                    .into_inner();
                validate_publication(&publication, id)?;
                Ok(publication)
            }
        })
        .await
    }
    pub async fn publish(
        &self,
        request: p::PublishReferenceRequest,
    ) -> Result<p::Publication, Status> {
        if request.repository_id != self.inner.config.repository_id
            || request.idempotency_key.is_empty()
        {
            return Err(Status::invalid_argument(
                "invalid native publication request",
            ));
        }
        if let Some(commit) = &request.new_commit {
            let key = parse_key(commit)?;
            if key.kind != Kind::Commit || intrinsic(key) {
                return Err(Status::invalid_argument(
                    "native reference must target a stored commit",
                ));
            }
        }
        validate_idempotency(&request.idempotency_key)?;
        self.batch(request.staging_leases.len())?;
        self.bounded(&request)?;
        let deadline = self.deadline();
        let budget = self.admit(deadline, self.unary_memory()).await?;
        let result = self
            .retry(deadline, false, |allowance| {
                let mut grpc = self
                    .rpc_client(budget.clone())
                    .max_decoding_message_size(self.inner.config.limits.max_batch_bytes);
                let request = request.clone();
                async move {
                    match grpc
                        .publish_reference(self.request(request.clone(), allowance))
                        .await
                    {
                        Ok(response) => {
                            let response = response.into_inner();
                            validate_publication_request(&response, &request)?;
                            Ok(Ok(response))
                        }
                        // A stale generation is definitive. It needs a caller
                        // decision, not transparent CAS retry or receipt lookup.
                        Err(status) if status.code() == tonic::Code::Aborted => Ok(Err(status)),
                        Err(status) => Err(status),
                    }
                }
            })
            .await
            .and_then(|result| result);
        match result {
            Ok(receipt) => Ok(receipt),
            Err(error)
                if error.code() != tonic::Code::Aborted
                    && reapi::policy::retryable(&error)
                    && Instant::now() < deadline =>
            {
                match self
                    .publication_at(&request.idempotency_key, deadline, budget.clone())
                    .await
                {
                    Ok(receipt) => {
                        validate_publication_request(&receipt, &request)?;
                        // Query receipts omit original lease inputs. Replaying
                        // the exact request checks the complete binding too.
                        self.retry(deadline, false, |allowance| {
                            let mut grpc =
                                self.rpc_client(budget.clone()).max_decoding_message_size(
                                    self.inner.config.limits.max_batch_bytes,
                                );
                            let request = request.clone();
                            async move {
                                let confirmed = grpc
                                    .publish_reference(self.request(request.clone(), allowance))
                                    .await?
                                    .into_inner();
                                validate_publication_request(&confirmed, &request)?;
                                Ok(confirmed)
                            }
                        })
                        .await
                    }
                    Err(status) if status.code() == tonic::Code::NotFound => Err(error),
                    Err(status) => Err(status),
                }
            }
            Err(error) => Err(error),
        }
    }
}
fn integrity() -> Status {
    Status::data_loss("native repository response failed verification")
}
fn intrinsic(key: ObjectKey) -> bool {
    key.kind == Kind::Commit && key.id.0 == [0; 32]
}
fn wire_key(key: ObjectKey) -> p::ObjectKey {
    p::ObjectKey {
        kind: key.kind as i32,
        native_id: key.id.0.to_vec(),
    }
}
fn parse_key(key: &p::ObjectKey) -> Result<ObjectKey, Status> {
    let kind = u8::try_from(key.kind)
        .ok()
        .and_then(|value| Kind::try_from(value).ok())
        .ok_or_else(integrity)?;
    let id = ObjectId::from_slice(&key.native_id).map_err(|_| integrity())?;
    Ok(ObjectKey { kind, id })
}
fn wire_descriptor(descriptor: Descriptor) -> p::ObjectDescriptor {
    p::ObjectDescriptor {
        key: Some(wire_key(descriptor.key)),
        content_blake3: descriptor.key.id.0.to_vec(),
        size_bytes: descriptor.size_bytes,
        object_format_version: u32::from(descriptor.format_version),
    }
}
fn parse_descriptor(wire: &p::ObjectDescriptor) -> Result<Descriptor, Status> {
    let key = parse_key(wire.key.as_ref().ok_or_else(integrity)?)?;
    if intrinsic(key)
        || wire.content_blake3 != key.id.0
        || wire.object_format_version != u32::from(tinytree_model::FORMAT_VERSION)
    {
        return Err(integrity());
    }
    Ok(Descriptor {
        key,
        size_bytes: wire.size_bytes,
        format_version: tinytree_model::FORMAT_VERSION,
    })
}
fn validate_reference(reference: &p::Reference) -> Result<(), Status> {
    if reference.name.is_empty() || reference.name.len() > 4096 {
        return Err(integrity());
    }
    if let Some(descriptor) = &reference.commit {
        if parse_descriptor(descriptor)?.key.kind != Kind::Commit {
            return Err(integrity());
        }
    }
    Ok(())
}
fn validate_publication(publication: &p::Publication, id: &str) -> Result<(), Status> {
    if publication.idempotency_key != id {
        return Err(integrity());
    }
    validate_reference(publication.reference.as_ref().ok_or_else(integrity)?)
}
fn validate_publication_request(
    publication: &p::Publication,
    request: &p::PublishReferenceRequest,
) -> Result<(), Status> {
    validate_publication(publication, &request.idempotency_key)?;
    let reference = publication.reference.as_ref().unwrap();
    let expected_generation = request
        .expected_generation
        .checked_add(1)
        .ok_or_else(integrity)?;
    if reference.name != request.reference_name
        || reference.generation != expected_generation
        || reference
            .commit
            .as_ref()
            .map(parse_descriptor)
            .transpose()?
            .map(|d| d.key)
            != request.new_commit.as_ref().map(parse_key).transpose()?
    {
        return Err(integrity());
    }
    Ok(())
}
fn validate_write_receipt(receipt: &p::WriteReceipt, expected: Descriptor) -> Result<(), Status> {
    if parse_descriptor(receipt.descriptor.as_ref().ok_or_else(integrity)?)? != expected
        || receipt.staging_lease.is_empty()
        || receipt.staging_lease.len() > 4096
        || receipt.staging_lease_expiry_unix_seconds <= 0
    {
        return Err(integrity());
    }
    Ok(())
}
fn header(repo: &str, descriptor: Descriptor) -> p::WriteFrame {
    p::WriteFrame {
        frame: Some(p::write_frame::Frame::Header(p::WriteHeader {
            repository_id: repo.into(),
            descriptor: Some(wire_descriptor(descriptor)),
        })),
    }
}
async fn blocking<T: Send + 'static>(
    deadline: Instant,
    operation: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    reapi::policy::timeout(deadline, async {
        tokio::task::spawn_blocking(operation)
            .await
            .map_err(|_| Status::internal("native client worker failed"))?
    })
    .await
}
// Keep returned allocations/handles covered even in a completed blocking task
// whose caller was cancelled before collecting its result.
struct OwnedWork<T> {
    value: T,
    _temporary: Option<Temporary>,
    _budget: Arc<Budget>,
}
async fn blocking_owned<T: Send + 'static>(
    deadline: Instant,
    budget: Arc<Budget>,
    temporary: Option<Temporary>,
    operation: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    let work = blocking(deadline, move || {
        // Establish the ordered owner before invoking the operation, including
        // errors or unwinding. Unreceived results and temporary cleanup must
        // finish before the last physical request budget can be released.
        let mut work = OwnedWork {
            value: None,
            _temporary: temporary,
            _budget: budget,
        };
        work.value = Some(operation());
        Ok(work)
    })
    .await?;
    work.value
        .ok_or_else(|| Status::internal("native client worker returned no result"))?
}

fn commit_heap(commit: &tinytree_model::Commit) -> usize {
    let mut bytes = std::mem::size_of::<tinytree_model::Commit>()
        .saturating_add(
            commit
                .parents
                .capacity()
                .saturating_mul(std::mem::size_of::<tinytree_model::CommitId>()),
        )
        .saturating_add(
            commit
                .predecessors
                .capacity()
                .saturating_mul(std::mem::size_of::<tinytree_model::CommitId>()),
        )
        .saturating_add(
            commit
                .root_tree
                .terms
                .capacity()
                .saturating_mul(std::mem::size_of::<tinytree_model::TreeId>()),
        )
        .saturating_add(
            commit
                .root_tree
                .labels
                .capacity()
                .saturating_mul(std::mem::size_of::<String>()),
        )
        .saturating_add(commit.description.capacity())
        .saturating_add(commit.author.name.capacity())
        .saturating_add(commit.author.email.capacity())
        .saturating_add(commit.committer.name.capacity())
        .saturating_add(commit.committer.email.capacity());
    for label in &commit.root_tree.labels {
        bytes = bytes.saturating_add(label.capacity());
    }
    if let Some(signature) = &commit.secure_signature {
        bytes = bytes
            .saturating_add(signature.data.capacity())
            .saturating_add(signature.signature.capacity());
    }
    bytes
}

async fn verify_metadata(
    descriptor: Descriptor,
    bytes: Vec<u8>,
    limit: usize,
    graph_items: Option<usize>,
    deadline: Instant,
    budget: Arc<Budget>,
) -> Result<VerifiedMetadata, Status> {
    blocking_owned(deadline, budget.clone(), None, move || {
        let _budget = budget;
        if descriptor.key.kind == Kind::File || intrinsic(descriptor.key) {
            return Err(integrity());
        }
        let limits = tinytree_model::Limits {
            max_object_bytes: limit,
            max_items: graph_items.unwrap_or(tinytree_model::Limits::default().max_items),
            ..Default::default()
        };
        let invalid = |error| {
            if graph_items.is_some() && matches!(error, tinytree_model::Error::Limit) {
                Status::resource_exhausted("native graph canonical shape exceeds allocation budget")
            } else {
                integrity()
            }
        };
        tinytree_model::verify(&descriptor, &bytes, limits).map_err(invalid)?;
        let object = tinytree_model::decode(&bytes, limits).map_err(invalid)?;
        let canonical = tinytree_model::encode(&object, limits).map_err(invalid)?;
        if canonical.bytes != bytes || canonical.descriptor != descriptor {
            return Err(integrity());
        }
        Ok(VerifiedMetadata {
            _query_owner: None,
            descriptor,
            bytes,
            object,
        })
    })
    .await
}
fn secure_directory(path: &Path) -> Result<private_fs::PrivateDir, Status> {
    // Preserve Unix's established canonical temporary-root behavior (for example
    // the OS-managed temporary-root alias). Windows checks the original path so
    // no junction/reparse traversal is hidden by canonicalization.
    #[cfg(unix)]
    let path = path
        .canonicalize()
        .map_err(|_| Status::failed_precondition("cannot resolve native temporary directory"))?;
    private_fs::PrivateDir::open_existing(&path).map_err(|_| {
        Status::failed_precondition("native temporary directory must be trusted and owner-private")
    })
}

#[cfg(unix)]
fn secure_unix(path: &Path) -> Result<(), Status> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let uid = unsafe { libc::geteuid() };
    let parent = path
        .parent()
        .ok_or_else(|| Status::invalid_argument("invalid native Unix path"))?;
    let mut ancestor = PathBuf::new();
    for component in parent.components() {
        if !matches!(
            component,
            std::path::Component::RootDir | std::path::Component::Normal(_)
        ) {
            return Err(Status::invalid_argument(
                "native Unix path must be normalized",
            ));
        }
        ancestor.push(component.as_os_str());
        let metadata = std::fs::symlink_metadata(&ancestor)
            .map_err(|_| Status::failed_precondition("cannot inspect native Unix parent"))?;
        let mode = metadata.permissions().mode();
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || !(metadata.uid() == 0 || metadata.uid() == uid)
            || (mode & 0o022 != 0 && !(metadata.uid() == 0 && mode & 0o1000 != 0))
        {
            return Err(Status::failed_precondition("untrusted native Unix parent"));
        }
    }
    let metadata = std::fs::symlink_metadata(parent)
        .map_err(|_| Status::failed_precondition("cannot inspect native Unix parent"))?;
    if metadata.uid() != uid || metadata.permissions().mode() & 0o077 != 0 {
        return Err(Status::failed_precondition(
            "native Unix parent must be private",
        ));
    }
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| Status::failed_precondition("cannot inspect native Unix socket"))?;
    use std::os::unix::fs::FileTypeExt;
    if !metadata.file_type().is_socket()
        || metadata.uid() != uid
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(Status::failed_precondition(
            "native Unix socket must be private",
        ));
    }
    Ok(())
}

fn item_status(code: p::item_error::Code) -> Status {
    match code {
        p::item_error::Code::NotFound => Status::not_found("native canonical metadata unavailable"),
        p::item_error::Code::ResourceExhausted => {
            Status::resource_exhausted("native canonical metadata budget exhausted")
        }
        p::item_error::Code::Unavailable => {
            Status::unavailable("native canonical metadata temporarily unavailable")
        }
        p::item_error::Code::InvalidObject => {
            Status::data_loss("native canonical metadata rejected")
        }
        _ => integrity(),
    }
}

fn validate_idempotency(id: &str) -> Result<(), Status> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        Err(Status::invalid_argument(
            "invalid native publication idempotency key",
        ))
    } else {
        Ok(())
    }
}
