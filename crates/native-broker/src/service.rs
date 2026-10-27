use crate::{Config, proto as p, wire};
use futures::AsyncReadExt;
use http::{Request as HttpRequest, Response as HttpResponse};
use jj_native_backend::{CopyCatalogLimits, NativeObjectIo, ObjectIoResources};
use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    time::Instant,
};
use tonic::{
    Request, Response, Status, Streaming, body::Body, codec::CompressionEncoding,
    server::NamedService,
};
use tower::{Layer, Service};
type Stream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;
#[derive(Clone)]
pub struct BrokerService {
    inner: Arc<Inner>,
}
struct Inner {
    stores: HashMap<String, Arc<dyn NativeObjectIo>>,
    command_resolver: Option<Arc<dyn crate::CommandResolver>>,
    host_access_resolver: Option<Arc<dyn crate::HostAccessResolver>>,
    controls: HashMap<String, Arc<dyn crate::NativeRepositoryControl>>,
    queries: HashMap<String, Arc<dyn crate::NativeQueryIo>>,
    descriptions: HashMap<String, crate::ProviderDescription>,
    query_work: ObjectIoResources,
    config: Config,
    metadata_work: ObjectIoResources,
    calls: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    staging: Arc<Semaphore>,
}
impl Inner {
    fn host_access(&self) -> Result<Option<crate::CommandGuard>, Status> {
        self.host_access_resolver
            .as_ref()
            .map(|resolver| {
                resolver
                    .acquire_host_access()
                    .map_err(|error| Status::new(error.code(), "native host access unavailable"))
            })
            .transpose()
    }
    async fn host_access_until(
        &self,
        deadline: Instant,
    ) -> Result<Option<crate::CommandGuard>, Status> {
        loop {
            if Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(
                    "native host admission deadline exceeded",
                ));
            }
            match self.host_access() {
                Err(error) if error.code() == tonic::Code::Aborted => {
                    // No guard, provider job or pool permits exist while
                    // collection owns the fence. The caller can cancel this
                    // timer; never detach waiting admission or refresh budget.
                    tokio::time::sleep_until(
                        deadline.min(Instant::now() + std::time::Duration::from_millis(5)),
                    )
                    .await;
                }
                result => return result,
            }
        }
    }
    async fn admit(&self, path: &str, deadline: Instant) -> Result<Arc<Work>, Status> {
        let is_control = path.starts_with("/native.broker.v1.RepositoryControl/");
        let is_query =
            path.starts_with("/native.broker.v1.QueryIo/") && !path.ends_with("/DescribeProvider");
        if is_query && self.queries.is_empty() && !path.ends_with("/PreflightCreation") {
            return Err(Status::failed_precondition(
                "native repository query is not configured",
            ));
        }
        // The alias is inside the protobuf body, which has not been decoded at
        // this boundary. Reserve the largest registered provider workset.
        let is_metadata = [
            "/Descriptor",
            "/ReadMetadata",
            "/CopyCatalog",
            "/ReadFile",
            "/ReadLocalFile",
            "/WriteMetadata",
            "/WriteFile",
        ]
        .iter()
        .any(|suffix| path.ends_with(suffix));
        let landing = path.ends_with("/RepositoryControl/Landing")
            || path == "/native.broker.v1.RepositoryControl/Landing";
        let landing_work = self
            .controls
            .values()
            .filter_map(|c| c.landing_resources())
            .fold(
                crate::ControlResources {
                    memory_bytes: 0,
                    temporary_bytes: 0,
                },
                |a, b| crate::ControlResources {
                    memory_bytes: a.memory_bytes.max(b.memory_bytes),
                    temporary_bytes: a.temporary_bytes.max(b.temporary_bytes),
                },
            );
        if landing && landing_work.memory_bytes == 0 {
            return Err(Status::unimplemented("native landing is not configured"));
        }
        let memory = if landing {
            landing_work.memory_bytes as usize
        } else if is_query {
            self.config
                .query_memory()
                .saturating_add(self.query_work.memory_bytes as usize)
        } else if is_control {
            self.config.max_memory_bytes
        } else if path.ends_with("/CopyCatalog") {
            self.config
                .catalog_memory()
                .max(self.config.metadata_memory())
                .max(self.config.file_memory())
        } else {
            self.config.metadata_memory().max(self.config.file_memory())
        };
        let memory = if is_metadata {
            memory.max(self.metadata_work.memory_bytes as usize)
        } else {
            memory
        };
        let temporary = if landing {
            landing_work.temporary_bytes
        } else if is_query {
            self.query_work.temporary_bytes
        } else if is_control {
            self.config.max_temporary_bytes
        } else if is_metadata {
            self.metadata_work.temporary_bytes
        } else {
            0
        };
        let calls = self
            .calls
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("broker draining"))?;
        let bytes = self
            .memory
            .clone()
            .acquire_many_owned(memory as u32)
            .await
            .map_err(|_| Status::unavailable("broker draining"))?;
        let staging = if temporary > 0 {
            Some(
                self.staging
                    .clone()
                    .acquire_many_owned(temporary as u32)
                    .await
                    .map_err(|_| Status::unavailable("broker staging admission closed"))?,
            )
        } else {
            None
        };
        Ok(Arc::new(Work {
            _calls: calls,
            _memory: bytes,
            _staging: staging,
            deadline,
            rpc: false,
            command_token: None,
            bootstrap: false,
            command: Mutex::new(None),
            _host: None,
        }))
    }
}
impl BrokerService {
    /// Enable command attribution for all configured RPC aliases. Register
    /// before exposing/cloning this service. Trusted in-process source reads
    /// remain a distinct host capability, not a tokenless RPC fallback.
    pub fn with_command_resolver(
        mut self,
        resolver: Arc<dyn crate::CommandResolver>,
    ) -> Result<Self, Status> {
        Arc::get_mut(&mut self.inner)
            .ok_or_else(|| Status::failed_precondition("broker registry already shared"))?
            .command_resolver = Some(resolver);
        Ok(self)
    }
    /// Fence trusted in-process source/query jobs and tokenless bootstrap RPCs.
    /// Register before sharing the broker. Standalone untracked brokers may omit
    /// this hook; a managed collector host must provide it for the entire cohort.
    pub fn with_host_access_resolver(
        mut self,
        resolver: Arc<dyn crate::HostAccessResolver>,
    ) -> Result<Self, Status> {
        Arc::get_mut(&mut self.inner)
            .ok_or_else(|| Status::failed_precondition("broker registry already shared"))?
            .host_access_resolver = Some(resolver);
        Ok(self)
    }
    pub(crate) fn authorize_command(&self, alias: &str, work: &Arc<Work>) -> Result<(), Status> {
        self.store(alias)?;
        if !work.rpc {
            return Ok(());
        }
        let Some(resolver) = &self.inner.command_resolver else {
            return if work.command_token.is_some() {
                Err(Status::failed_precondition(
                    "native command attribution unavailable",
                ))
            } else {
                Ok(())
            };
        };
        let Some(token) = work.command_token else {
            return if work.bootstrap {
                Ok(())
            } else {
                Err(Status::unauthenticated("native command token required"))
            };
        };
        let mut binding = work
            .command
            .lock()
            .map_err(|_| Status::internal("native command scope unavailable"))?;
        if let Some((bound, _)) = &*binding {
            return if bound == alias {
                Ok(())
            } else {
                Err(Status::permission_denied("native command alias mismatch"))
            };
        }
        let guard = resolver
            .resolve(alias, token)
            .map_err(|error| Status::new(error.code(), "native command access denied"))?;
        *binding = Some((alias.to_owned(), guard));
        Ok(())
    }
    /// Call after stopping incoming requests. Waits for transport-held guards
    /// AND detached draining provider jobs; it never force-closes their I/O.
    pub async fn drain(&self) -> Result<(), Status> {
        let permits = self
            .inner
            .calls
            .clone()
            .acquire_many_owned(self.inner.config.max_inflight as u32)
            .await
            .map_err(|_| Status::unavailable("broker admission closed"))?;
        drop(permits);
        Ok(())
    }
    pub fn new(
        stores: HashMap<String, Arc<dyn NativeObjectIo>>,
        config: Config,
    ) -> Result<Self, Status> {
        config.validate()?;
        if stores.is_empty() || stores.len() > 1024 {
            return Err(Status::invalid_argument("invalid broker store registry"));
        }
        let mut metadata_work = ObjectIoResources::default();
        for (id, provider) in &stores {
            wire::id(id)?;
            if let Some(required) = provider.metadata_work_budget() {
                if required.memory_bytes == 0
                    || required.memory_bytes > config.max_memory_bytes as u64
                    || required.temporary_bytes > config.max_temporary_bytes
                {
                    return Err(Status::invalid_argument(
                        "native metadata resource bounds exceed broker pools",
                    ));
                }
                metadata_work.memory_bytes = metadata_work.memory_bytes.max(required.memory_bytes);
                metadata_work.temporary_bytes =
                    metadata_work.temporary_bytes.max(required.temporary_bytes);
            }
        }
        Ok(Self {
            inner: Arc::new(Inner {
                stores,
                command_resolver: None,
                host_access_resolver: None,
                metadata_work,
                controls: HashMap::new(),
                queries: HashMap::new(),
                descriptions: HashMap::new(),
                query_work: ObjectIoResources::default(),
                calls: Arc::new(Semaphore::new(config.max_inflight)),
                memory: Arc::new(Semaphore::new(config.max_memory_bytes)),
                staging: Arc::new(Semaphore::new(config.max_temporary_bytes as usize)),
                config,
            }),
        })
    }
    /// Attach host-supplied control only to existing aliases, before cloning or
    /// exposing the service. No incoming RPC chooses an implementation/budget.
    pub fn with_controls(
        mut self,
        controls: HashMap<String, Arc<dyn crate::NativeRepositoryControl>>,
    ) -> Result<Self, Status> {
        for (id, control) in &controls {
            wire::id(id)?;
            if !self.inner.stores.contains_key(id) {
                return Err(Status::invalid_argument(
                    "control alias has no native object provider",
                ));
            }
            let required = control.resources();
            if control.landing_resources().is_some_and(|r| {
                r.memory_bytes == 0
                    || r.memory_bytes > self.inner.config.max_memory_bytes as u64
                    || r.temporary_bytes > self.inner.config.max_temporary_bytes
            }) {
                return Err(Status::invalid_argument(
                    "native landing resource bounds exceed broker pools",
                ));
            }
            if required.memory_bytes == 0
                || required.memory_bytes > self.inner.config.max_memory_bytes as u64
                || required.temporary_bytes > self.inner.config.max_temporary_bytes
            {
                return Err(Status::invalid_argument(
                    "native control resource bounds exceed broker pools",
                ));
            }
        }
        Arc::get_mut(&mut self.inner)
            .ok_or_else(|| {
                Status::failed_precondition("attach native controls before sharing the service")
            })?
            .controls = controls;
        Ok(self)
    }
    /// Register independently computed provider identities before sharing the
    /// service. Query capability must agree with actual configured adapters.
    pub fn with_provider_descriptions(
        mut self,
        descriptions: HashMap<String, crate::ProviderDescription>,
    ) -> Result<Self, Status> {
        if descriptions.len() != self.inner.stores.len() {
            return Err(Status::invalid_argument(
                "native provider identity registry is incomplete",
            ));
        }
        for (id, value) in &descriptions {
            wire::id(id)?;
            if !self.inner.stores.contains_key(id)
                || value.identity == [0; 32]
                || value.query_capable != self.inner.queries.contains_key(id)
            {
                return Err(Status::invalid_argument(
                    "invalid native provider identity registration",
                ));
            }
        }
        Arc::get_mut(&mut self.inner)
            .ok_or_else(|| {
                Status::failed_precondition("register provider identity before sharing service")
            })?
            .descriptions = descriptions;
        Ok(self)
    }
    pub fn describe_provider(&self, id: &str) -> Result<crate::ProviderDescription, Status> {
        self.store(id)?;
        self.inner.descriptions.get(id).copied().ok_or_else(|| {
            Status::failed_precondition("native provider identity is not configured")
        })
    }
    /// Attach immutable-query adapters only to existing configured aliases.
    pub fn with_queries(
        mut self,
        queries: HashMap<String, Arc<dyn crate::NativeQueryIo>>,
    ) -> Result<Self, Status> {
        if !self.inner.descriptions.is_empty() {
            return Err(Status::failed_precondition(
                "query adapters must be registered before provider descriptions",
            ));
        }
        let mut query_work = ObjectIoResources::default();
        let mut distinct = std::collections::HashSet::new();
        let mut persistent = 0u64;
        for (id, query) in &queries {
            wire::id(id)?;
            if !self.inner.stores.contains_key(id) {
                return Err(Status::invalid_argument(
                    "query alias has no native object provider",
                ));
            }
            let required = query.resources();
            if distinct.insert(Arc::as_ptr(query) as *const () as usize) {
                persistent = persistent
                    .checked_add(required.persistent_memory_bytes)
                    .ok_or_else(|| {
                        Status::invalid_argument("native query persistent budget overflow")
                    })?;
                if persistent > self.inner.config.max_query_persistent_bytes {
                    return Err(Status::invalid_argument(
                        "aggregate native query persistent budgets exceed max_query_persistent_bytes; reduce per-repository query limits or raise the configured aggregate cap",
                    ));
                }
            }
            if required.memory_bytes == 0
                || required
                    .memory_bytes
                    .checked_add(self.inner.config.query_memory() as u64)
                    .is_none_or(|bytes| bytes > self.inner.config.max_memory_bytes as u64)
                || required.temporary_bytes > self.inner.config.max_temporary_bytes
            {
                return Err(Status::invalid_argument(
                    "native query resource bounds exceed broker pools",
                ));
            }
            query_work.memory_bytes = query_work.memory_bytes.max(required.memory_bytes);
            query_work.temporary_bytes = query_work.temporary_bytes.max(required.temporary_bytes);
        }
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            Status::failed_precondition("attach native queries before sharing the service")
        })?;
        inner.queries = queries;
        inner.query_work = query_work;
        Ok(self)
    }
    pub(crate) fn query_provider(&self, id: &str) -> Result<Arc<dyn crate::NativeQueryIo>, Status> {
        self.store(id)?;
        self.inner.queries.get(id).cloned().ok_or_else(|| {
            Status::failed_precondition("native repository query is not configured for this alias")
        })
    }
    pub(crate) fn query_limit(&self) -> usize {
        self.inner.config.max_query_bytes
    }
    pub(crate) fn query_config(&self) -> Config {
        self.inner.config.clone()
    }
    pub fn query_rpc_service(&self) -> crate::query_service::QueryRpcService {
        let cap = self.inner.config.max_query_bytes;
        let raw = p::query_io_server::QueryIoServer::new(self.clone())
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(cap)
            .max_encoding_message_size(cap);
        let describe = raw
            .clone()
            .max_decoding_message_size(1024)
            .max_encoding_message_size(1024);
        crate::query_service::QueryRpcService {
            inner: self.wrap(raw),
            describe: self.wrap(describe),
        }
    }
    /// Same admission and adapter as local RPC, without a recursive RPC call.
    pub fn query_io(&self, id: &str) -> Result<Arc<dyn crate::NativeQueryIo>, Status> {
        Ok(Arc::new(crate::query_service::GuardedQuery::new(
            self.clone(),
            self.query_provider(id)?,
            id.to_owned(),
        )))
    }
    fn control(&self, id: &str) -> Result<Arc<dyn crate::NativeRepositoryControl>, Status> {
        self.store(id)?;
        self.inner.controls.get(id).cloned().ok_or_else(|| {
            Status::failed_precondition(
                "native repository control is not configured for this alias",
            )
        })
    }
    /// In-process object I/O facade sharing RPC admission and drain. Reads are
    /// guarded; writes preserve the provider's existing staging/ACK contract.
    pub fn source_io(&self, id: &str) -> Result<Arc<dyn NativeObjectIo>, Status> {
        Ok(Arc::new(crate::local::GuardedIo::new(
            self.clone(),
            self.store(id)?,
            self.inner.config.clone(),
        )))
    }
    pub(crate) fn operation_deadline(&self) -> Instant {
        Instant::now() + self.inner.config.policy.operation_timeout()
    }
    pub(crate) async fn admit_local(
        &self,
        method: &str,
        deadline: Instant,
    ) -> Result<Arc<Work>, Status> {
        // Acquire the independent host fence BEFORE reserving any physical
        // provider work. No permanent guard is attached to the source facade.
        let host = self.inner.host_access_until(deadline).await?;
        let mut work = tokio::time::timeout_at(deadline, self.inner.admit(method, deadline))
            .await
            .map_err(|_| {
                Status::deadline_exceeded("broker source admission deadline exceeded")
            })??;
        Arc::get_mut(&mut work)
            .ok_or_else(|| Status::internal("broker source scope already shared"))?
            ._host = host;
        Ok(work)
    }
    fn store(&self, id: &str) -> Result<Arc<dyn NativeObjectIo>, Status> {
        wire::id(id)?;
        self.inner
            .stores
            .get(id)
            .cloned()
            .ok_or_else(|| Status::not_found("native broker store unavailable"))
    }
    async fn reserve_staging(
        &self,
        bytes: u64,
        deadline: Instant,
    ) -> Result<OwnedSemaphorePermit, Status> {
        if bytes > self.inner.config.max_temporary_bytes {
            return Err(Status::resource_exhausted(
                "broker write staging exceeds global limit",
            ));
        }
        tokio::time::timeout_at(
            deadline,
            self.inner.staging.clone().acquire_many_owned(bytes as u32),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("broker write staging admission deadline exceeded"))?
        .map_err(|_| Status::unavailable("broker staging admission closed"))
    }
    /// Includes pre-decode admission and response-lifetime budgets. Embed this
    /// service directly; callers cannot omit the independent body deadline.
    pub fn rpc_service(&self) -> RpcService {
        let raw = p::object_io_server::ObjectIoServer::new(self.clone())
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(
                self.inner
                    .config
                    .max_metadata_bytes
                    .max(self.inner.config.max_frame_bytes)
                    + 4096,
            )
            .max_encoding_message_size(
                self.inner
                    .config
                    .max_metadata_bytes
                    .max(self.inner.config.max_frame_bytes)
                    + 4096,
            );
        RpcService {
            inner: self.wrap(raw),
        }
    }
    /// This second service shares the exact ObjectIo pools, deadlines and drain.
    pub fn control_rpc_service(&self) -> ControlRpcService {
        let raw = p::repository_control_server::RepositoryControlServer::new(self.clone())
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(16 * 1024)
            .max_encoding_message_size(128 * 1024 + 4096);
        ControlRpcService {
            inner: self.wrap(raw),
        }
    }
    fn wrap<S>(
        &self,
        raw: S,
    ) -> tower::util::BoxCloneSyncService<HttpRequest<Body>, HttpResponse<Body>, Infallible>
    where
        S: Service<HttpRequest<Body>, Response = HttpResponse<Body>, Error = Infallible>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        let gate = Gate {
            inner: raw,
            shared: self.inner.clone(),
        };
        let timed = grpc_transport::DeadlineLayer::new(
            self.inner.config.policy.operation_timeout_ms,
            self.inner.config.policy.stream_idle_timeout_ms,
            self.inner.config.max_frame_bytes,
        )
        .layer(gate);
        let mapped = tower::ServiceBuilder::new()
            .map_response(|response: HttpResponse<_>| response.map(Body::new))
            .service(timed);
        tower::util::BoxCloneSyncService::new(mapped)
    }
}
#[derive(Clone)]
pub struct RpcService {
    inner: tower::util::BoxCloneSyncService<HttpRequest<Body>, HttpResponse<Body>, Infallible>,
}
#[derive(Clone)]
pub struct ControlRpcService {
    inner: tower::util::BoxCloneSyncService<HttpRequest<Body>, HttpResponse<Body>, Infallible>,
}
impl NamedService for ControlRpcService {
    const NAME: &'static str = "native.broker.v1.RepositoryControl";
}
impl Service<HttpRequest<Body>> for ControlRpcService {
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: HttpRequest<Body>) -> Self::Future {
        self.inner.call(request)
    }
}
impl NamedService for RpcService {
    const NAME: &'static str = "native.broker.v1.ObjectIo";
}
impl Service<HttpRequest<Body>> for RpcService {
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, r: HttpRequest<Body>) -> Self::Future {
        self.inner.call(r)
    }
}
#[derive(Clone)]
struct Gate<S> {
    inner: S,
    shared: Arc<Inner>,
}
impl<S> Service<HttpRequest<Body>> for Gate<S>
where
    S: Service<HttpRequest<Body>, Response = HttpResponse<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: HttpRequest<Body>) -> Self::Future {
        let shared = self.shared.clone();
        let path = request.uri().path().to_owned();
        let command_token = match crate::command::token(request.headers()) {
            Ok(token) => token,
            Err(status) => return Box::pin(async move { Ok(status.into_http()) }),
        };
        let budget = match grpc_budget(request.headers(), shared.config.policy.operation_timeout())
        {
            Ok(value) => value,
            Err(status) => return Box::pin(async move { Ok(status.into_http()) }),
        };
        let deadline = Instant::now() + budget;
        let ready = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, ready);
        Box::pin(async move {
            let result = tokio::time::timeout_at(deadline, async {
                // Bootstrap has no command job fence and its alias is still in
                // the undecoded body. Fence the whole host cohort before pools.
                let bootstrap = matches!(
                    path.as_str(),
                    "/native.broker.v1.QueryIo/DescribeProvider"
                        | "/native.broker.v1.QueryIo/PreflightCreation"
                        | "/native.broker.v1.RepositoryControl/ObserveReference"
                        | "/native.broker.v1.RepositoryControl/AdmitCommit"
                );
                let host = if bootstrap && command_token.is_none() {
                    shared.host_access_until(deadline).await?
                } else {
                    None
                };
                let mut work = shared.admit(&path, deadline).await?;
                let context = Arc::get_mut(&mut work)
                    .ok_or_else(|| Status::internal("broker request scope already shared"))?;
                context.rpc = true;
                context.command_token = command_token;
                context.bootstrap = bootstrap;
                context._host = host;
                let mut request = request.map(|body| {
                    Body::new(InboundBody::new(
                        body,
                        deadline,
                        shared.config.policy.stream_idle_timeout(),
                    ))
                });
                request.extensions_mut().insert(work.clone());
                // Do not cancel a provider future while it awaits an owned
                // blocking job. A timed-out caller detaches this draining task;
                // its admission remains charged until actual work completes.
                tokio::spawn(async move {
                    let mut response = inner.call(request).await.unwrap();
                    // Keep an adapter's returned page owner independently of
                    // registry advancement, explicit close and handler teardown.
                    let returned = response
                        .extensions_mut()
                        .remove::<grpc_transport::ResponseBudget>();
                    response
                        .extensions_mut()
                        .insert(grpc_transport::ResponseBudget::new((returned, work)));
                    response
                })
                .await
                .map_err(|_| Status::internal("native broker request worker failed"))
            })
            .await;
            match result {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(status)) => Ok(status.into_http()),
                Err(_) => Ok(Status::deadline_exceeded(
                    "native broker operation deadline exceeded",
                )
                .into_http()),
            }
        })
    }
}

#[tonic::async_trait]
impl p::object_io_server::ObjectIo for BrokerService {
    async fn descriptor(
        &self,
        request: Request<p::ObjectRequest>,
    ) -> Result<Response<p::DescriptorReply>, Status> {
        let owner = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let input = request.into_inner();
        self.authorize_command(&input.store_id, &owner)?;
        let provider = self.store(&input.store_id)?;
        let value = provider
            .descriptor_with_owner(wire::key(input.kind, &input.id)?, owner)
            .await
            .map_err(wire::backend)?;
        Ok(Response::new(p::DescriptorReply {
            descriptor: value.map(wire::describe),
        }))
    }
    async fn read_metadata(
        &self,
        request: Request<p::ObjectRequest>,
    ) -> Result<Response<p::Metadata>, Status> {
        let owner = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let input = request.into_inner();
        self.authorize_command(&input.store_id, &owner)?;
        let provider = self.store(&input.store_id)?;
        let key = wire::key(input.kind, &input.id)?;
        if key.kind == tinytree_model::Kind::File {
            return Err(Status::invalid_argument("file requires broker stream"));
        }
        let metadata = provider
            .read_metadata_with_owner(key, owner)
            .await
            .map_err(wire::backend)?;
        if metadata.descriptor().key != key
            || metadata.bytes().len() > self.inner.config.max_metadata_bytes
        {
            return Err(Status::resource_exhausted("broker metadata exceeds limit"));
        }
        Ok(Response::new(p::Metadata {
            descriptor: Some(wire::describe(metadata.descriptor())),
            canonical: metadata.bytes().to_vec(),
        }))
    }
    async fn write_metadata(
        &self,
        request: Request<p::MetadataWrite>,
    ) -> Result<Response<p::ObjectDescriptor>, Status> {
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let deadline = work.deadline;
        let input = request.into_inner();
        self.authorize_command(&input.store_id, &work)?;
        let provider = self.store(&input.store_id)?;
        if input.canonical.len() > self.inner.config.max_metadata_bytes {
            return Err(Status::resource_exhausted("broker metadata exceeds limit"));
        }
        let _staging = self
            .reserve_staging(input.canonical.len() as u64, deadline)
            .await?;
        let object = tinytree_model::decode(&input.canonical, self.inner.config.model_limits())
            .map_err(|_| Status::invalid_argument("invalid broker canonical metadata"))?;
        let encoded = tinytree_model::encode(&object, self.inner.config.model_limits())
            .map_err(|_| Status::invalid_argument("invalid broker canonical metadata"))?;
        if encoded.bytes != input.canonical {
            return Err(Status::invalid_argument("noncanonical broker metadata"));
        }
        let descriptor = if work.requires_owned_io() {
            provider
                .write_metadata_with_owner(object, Arc::new((work.clone(), _staging)))
                .await
        } else {
            provider.write_metadata(object).await
        }
        .map_err(wire::backend)?;
        if descriptor != encoded.descriptor {
            return Err(Status::data_loss("broker provider metadata mismatch"));
        }
        Ok(Response::new(wire::describe(descriptor)))
    }
    type ReadFileStream = Stream<p::ReadFrame>;
    async fn read_file(
        &self,
        request: Request<p::ObjectRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let input = request.into_inner();
        self.authorize_command(&input.store_id, &work)?;
        let provider = self.store(&input.store_id)?;
        let key = wire::key(input.kind, &input.id)?;
        if key.kind != tinytree_model::Kind::File {
            return Err(Status::invalid_argument("broker stream requires file"));
        }
        let source = provider
            .read_file_for_cache(tinytree_model::FileId(key.id.0), work.clone())
            .await
            .map_err(wire::backend)?;
        self.file_response(source,work,key).await
    }
    type ReadLocalFileStream = Stream<p::ReadFrame>;
    async fn read_local_file(&self,request:Request<p::LocalFileRequest>)->Result<Response<Self::ReadLocalFileStream>,Status>{
        let work=request.extensions().get::<Arc<Work>>().cloned().ok_or_else(||Status::internal("broker request missing resource scope"))?;
        let input=request.into_inner();
        self.authorize_command(&input.store_id,&work)?;
        let expected=wire::descriptor(input.expected.ok_or_else(||Status::invalid_argument("bounded local FILE descriptor required"))?)?;
        if expected.key.kind!=tinytree_model::Kind::File || expected.size_bytes>input.max_bytes || input.max_bytes>self.inner.config.max_file_bytes {
            return Err(Status::invalid_argument("bounded local FILE request limit invalid"));
        }
        let provider=self.store(&input.store_id)?;
        let read=provider.read_local_file_bounded(expected,input.max_bytes,work.clone()).await.map_err(wire::backend)?;
        if read.descriptor()!=expected {return Err(Status::data_loss("bounded local FILE provider descriptor changed"));}
        self.file_response(read,work,expected.key).await
    }
    async fn write_file(
        &self,
        request: Request<Streaming<p::WriteFrame>>,
    ) -> Result<Response<p::ObjectDescriptor>, Status> {
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let deadline = work.deadline;
        let mut body = request.into_inner();
        let first = body
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("missing broker upload header"))?;
        let Some(p::write_frame::Frame::Header(header)) = first.frame else {
            return Err(Status::invalid_argument("missing broker upload header"));
        };
        self.authorize_command(&header.store_id, &work)?;
        let provider = self.store(&header.store_id)?;
        if header.max_bytes > self.inner.config.max_file_bytes {
            return Err(Status::resource_exhausted("broker upload exceeds limit"));
        }
        let reservation = header
            .max_bytes
            .checked_mul(2)
            .ok_or_else(|| Status::resource_exhausted("broker upload staging exceeds limit"))?;
        let _staging = self.reserve_staging(reservation, deadline).await?;
        let (sender, receiver) = mpsc::channel(2);
        let mut reader = UploadReader {
            receiver,
            current: std::io::Cursor::new(Vec::new()),
            done: false,
        };
        let provider_copy = provider.clone();
        let write_owner = Arc::new((work.clone(), _staging));
        let managed = work.requires_owned_io();
        let write = async move {
            if managed {
                provider_copy
                    .write_file_with_owner(&mut reader, header.max_bytes, write_owner)
                    .await
            } else {
                provider_copy
                    .write_file(&mut reader, header.max_bytes)
                    .await
            }
            .map_err(wire::backend)
        };
        let frame = self.inner.config.max_frame_bytes;
        let ingestion = async move {
            let result = async {
                let mut offset = 0u64;
                let mut complete = false;
                while let Some(next) = body.message().await? {
                    match next.frame {
                        Some(p::write_frame::Frame::Chunk(chunk)) if !complete => {
                            if chunk.offset != offset
                                || chunk.data.is_empty()
                                || chunk.data.len() > frame
                                || chunk.data.len() as u64 > header.max_bytes.saturating_sub(offset)
                            {
                                return Err(Status::invalid_argument(
                                    "invalid broker upload chunk",
                                ));
                            }
                            offset += chunk.data.len() as u64;
                            sender.send(Upload::Data(chunk.data)).await.map_err(|_| {
                                Status::failed_precondition("broker upload stopped")
                            })?;
                        }
                        Some(p::write_frame::Frame::Complete(true)) if !complete => complete = true,
                        _ => {
                            return Err(Status::invalid_argument("invalid broker upload sequence"));
                        }
                    }
                }
                if !complete {
                    return Err(Status::cancelled("broker upload did not complete"));
                }
                sender
                    .send(Upload::Complete)
                    .await
                    .map_err(|_| Status::failed_precondition("broker upload stopped"))?;
                Ok::<_, Status>(())
            }
            .await;
            drop(sender);
            result
        };
        let (written, ingested) = tokio::join!(write, ingestion);
        ingested?;
        let id = written?;
        let key = tinytree_model::ObjectKey {
            kind: tinytree_model::Kind::File,
            id: tinytree_model::ObjectId(id.0),
        };
        let descriptor = if managed {
            provider.descriptor_with_owner(key, work.clone()).await
        } else {
            provider.descriptor(key).await
        }
        .map_err(wire::backend)?
        .ok_or_else(|| Status::internal("durable broker upload descriptor missing"))?;
        Ok(Response::new(wire::describe(descriptor)))
    }
    type CopyCatalogStream = Stream<p::CatalogFrame>;
    async fn copy_catalog(
        &self,
        request: Request<p::CatalogRequest>,
    ) -> Result<Response<Self::CopyCatalogStream>, Status> {
        let owner = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let input = request.into_inner();
        self.authorize_command(&input.store_id, &owner)?;
        let provider = self.store(&input.store_id)?;
        let cfg = &self.inner.config;
        if input.max_objects == 0
            || input.max_objects > cfg.max_catalog_objects as u64
            || input.max_bytes == 0
            || input.max_bytes > cfg.max_catalog_bytes as u64
            || input.max_edges == 0
            || input.max_edges > cfg.max_catalog_edges as u64
        {
            return Err(Status::resource_exhausted(
                "broker catalog request exceeds limit",
            ));
        }
        let snapshot = provider
            .copy_catalog_snapshot_with_owner(
                CopyCatalogLimits {
                    max_objects: input.max_objects as usize,
                    max_metadata_bytes: input.max_bytes as usize,
                    max_edges: input.max_edges as usize,
                },
                owner,
            )
            .await
            .map_err(wire::backend)?;
        let entries = snapshot.into_entries();
        let output = async_stream::try_stream! {
            let mut hash=blake3::Hasher::new();hash.update(b"native-broker-copy-catalog-v1\0");
            let mut count=0u64;let mut size=0u64;
            for entry in entries {
                wire::catalog_hash(&mut hash,&entry);count+=1;size+=entry.descriptor().size_bytes;
                yield p::CatalogFrame {frame:Some(p::catalog_frame::Frame::Entry(p::Metadata {descriptor:Some(wire::describe(entry.descriptor())),canonical:entry.bytes().to_vec()}))};
            }
            yield p::CatalogFrame {frame:Some(p::catalog_frame::Frame::Complete(p::Complete {count,size_bytes:size,digest:hash.finalize().as_bytes().to_vec()}))};
        };
        Ok(Response::new(Box::pin(output)))
    }
}
enum Upload {
    Data(Vec<u8>),
    Complete,
}
struct UploadReader {
    receiver: mpsc::Receiver<Upload>,
    current: std::io::Cursor<Vec<u8>>,
    done: bool,
}
impl futures::AsyncRead for UploadReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        use std::io::Read;
        loop {
            let n = self.current.read(output)?;
            if n != 0 || output.is_empty() {
                return Poll::Ready(Ok(n));
            }
            if self.done {
                return Poll::Ready(Ok(0));
            }
            match std::task::ready!(self.receiver.poll_recv(cx)) {
                Some(Upload::Data(data)) => self.current = std::io::Cursor::new(data),
                Some(Upload::Complete) => self.done = true,
                None => return Poll::Ready(Err(std::io::Error::other("broker upload cancelled"))),
            }
        }
    }
}

fn grpc_budget(
    headers: &http::HeaderMap,
    maximum: std::time::Duration,
) -> Result<std::time::Duration, Status> {
    let Some(value) = headers.get("grpc-timeout") else {
        return Ok(maximum);
    };
    let value = value
        .to_str()
        .map_err(|_| Status::invalid_argument("invalid broker timeout"))?;
    if !(2..=9).contains(&value.len()) {
        return Err(Status::invalid_argument("invalid broker timeout"));
    }
    let (number, unit) = value.split_at(value.len() - 1);
    if !number.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Status::invalid_argument("invalid broker timeout"));
    }
    let number: u64 = number
        .parse()
        .map_err(|_| Status::invalid_argument("invalid broker timeout"))?;
    let value = match unit {
        "H" => std::time::Duration::from_secs(number.saturating_mul(3600)),
        "M" => std::time::Duration::from_secs(number.saturating_mul(60)),
        "S" => std::time::Duration::from_secs(number),
        "m" => std::time::Duration::from_millis(number),
        "u" => std::time::Duration::from_micros(number),
        "n" => std::time::Duration::from_nanos(number),
        _ => return Err(Status::invalid_argument("invalid broker timeout")),
    };
    Ok(value.min(maximum))
}

pub(crate) struct Work {
    _calls: OwnedSemaphorePermit,
    _memory: OwnedSemaphorePermit,
    _staging: Option<OwnedSemaphorePermit>,
    pub(crate) deadline: Instant,
    rpc: bool,
    command_token: Option<[u8; 32]>,
    bootstrap: bool,
    // Resolved accepted-job guard outlives all handler/body/provider owners.
    command: Mutex<Option<(String, crate::CommandGuard)>>,
    // Last owner: bootstrap/source physical jobs and returned bodies retain it.
    _host: Option<crate::CommandGuard>,
}
impl Work {
    fn requires_owned_io(&self) -> bool {
        self._host.is_some()
            || self
                .command
                .lock()
                .map(|binding| binding.is_some())
                .unwrap_or(true)
    }
}
struct InboundBody {
    body: Body,
    timer: Pin<Box<tokio::time::Sleep>>,
    deadline: Instant,
    idle: std::time::Duration,
    done: bool,
}
impl InboundBody {
    fn new(body: Body, deadline: Instant, idle: std::time::Duration) -> Self {
        Self {
            body,
            timer: Box::pin(tokio::time::sleep_until(
                deadline.min(Instant::now() + idle),
            )),
            deadline,
            idle,
            done: false,
        }
    }
}
impl http_body::Body for InboundBody {
    type Data = bytes::Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<bytes::Bytes>, Status>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        if this.timer.as_mut().poll(cx).is_ready() {
            this.done = true;
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "broker upload body deadline exceeded",
            ))));
        }
        let result = Pin::new(&mut this.body).poll_frame(cx);
        if result.is_ready() {
            this.timer
                .as_mut()
                .reset(this.deadline.min(Instant::now() + this.idle));
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.done || self.body.is_end_stream()
    }
}
async fn send_read(
    sender: &mpsc::Sender<Result<p::ReadFrame, Status>>,
    frame: Result<p::ReadFrame, Status>,
    deadline: Instant,
) -> Result<(), Status> {
    tokio::time::timeout_at(deadline, sender.send(frame))
        .await
        .map_err(|_| Status::deadline_exceeded("broker file stream deadline exceeded"))?
        .map_err(|_| Status::cancelled("broker file consumer stopped"))
}

#[tonic::async_trait]
impl p::repository_control_server::RepositoryControl for BrokerService {
    async fn landing(
        &self,
        request: Request<p::LandingRequest>,
    ) -> Result<Response<p::LandingReply>, Status> {
        use prost::Message;
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let input = request.into_inner();
        self.authorize_command(&input.store_id, &work)?;
        let intent = crate::LandingIntent::decode(&input)?;
        let control = self.control(&input.store_id)?;
        if control.landing_resources().is_none() {
            return Err(Status::unimplemented("native landing is not configured"));
        }
        let value = control
            .landing_with_owner(intent, input.submit, work.clone())
            .await
            .map_err(crate::control::status)?;
        let mut response = Response::new(p::LandingReply {
            job: value.job().encode_to_vec(),
        });
        response
            .extensions_mut()
            .insert(grpc_transport::ResponseBudget::new(value));
        Ok(response)
    }
    async fn observe_reference(
        &self,
        request: Request<p::ObserveReferenceRequest>,
    ) -> Result<Response<p::ReferenceSnapshot>, Status> {
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let input = request.into_inner();
        self.authorize_command(&input.store_id, &work)?;
        crate::control::reference_name(&input.name)?;
        let control = self.control(&input.store_id)?;
        let value = if work.requires_owned_io() {
            control
                .observe_reference_with_owner(&input.name, work.clone())
                .await
        } else {
            control.observe_reference(&input.name).await
        }
        .map_err(crate::control::status)?;
        if value.name != input.name || value.generation == 0 && value.head.is_some() {
            return Err(Status::data_loss(
                "native control returned an invalid reference observation",
            ));
        }
        let head = value
            .head
            .map(|head| {
                crate::control::commit(head.head).map_err(|_| {
                    Status::data_loss("native control returned an invalid admitted head")
                })?;
                Ok::<_, Status>(p::AdmittedCommit {
                    head: head.head.0.to_vec(),
                    metadata_objects: head.metadata_objects,
                })
            })
            .transpose()?;
        Ok(Response::new(p::ReferenceSnapshot {
            name: value.name,
            generation: value.generation,
            head,
        }))
    }
    async fn admit_commit(
        &self,
        request: Request<p::AdmitCommitRequest>,
    ) -> Result<Response<p::AdmittedCommit>, Status> {
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let input = request.into_inner();
        self.authorize_command(&input.store_id, &work)?;
        let head = request_commit(&input.head)?;
        let control = self.control(&input.store_id)?;
        let value = if work.requires_owned_io() {
            control.admit_commit_with_owner(head, work.clone()).await
        } else {
            control.admit_commit(head).await
        }
        .map_err(crate::control::status)?;
        if value.head != head {
            return Err(Status::data_loss("native control admitted another head"));
        }
        Ok(Response::new(p::AdmittedCommit {
            head: head.0.to_vec(),
            metadata_objects: value.metadata_objects,
        }))
    }
    async fn publish(
        &self,
        request: Request<p::PublishRequest>,
    ) -> Result<Response<p::ConfirmedPublication>, Status> {
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let input = request.into_inner();
        self.authorize_command(&input.store_id, &work)?;
        crate::control::reference_name(&input.name)?;
        crate::control::key(&input.idempotency_key)?;
        let head = request_commit(&input.head)?;
        let expected = input.expected_generation.ok_or_else(|| {
            Status::invalid_argument("native publication requires an explicit expected generation")
        })?;
        let generation = expected
            .checked_add(1)
            .ok_or_else(|| Status::invalid_argument("native reference generation exhausted"))?;
        let wanted = crate::PublishRequest {
            name: input.name,
            head,
            expected_generation: expected,
            idempotency_key: input.idempotency_key,
        };
        let control = self.control(&input.store_id)?;
        let value = if work.requires_owned_io() {
            control
                .publish_with_owner(wanted.clone(), work.clone())
                .await
        } else {
            control.publish(wanted.clone()).await
        }
        .map_err(crate::control::status)?;
        if value.idempotency_key != wanted.idempotency_key
            || value.name != wanted.name
            || value.head != wanted.head
            || value.generation != generation
        {
            return Err(Status::data_loss(
                "native control publication does not match original intent",
            ));
        }
        Ok(Response::new(control_publication(value)?))
    }
    async fn publication(
        &self,
        request: Request<p::PublicationRequest>,
    ) -> Result<Response<p::PublicationReply>, Status> {
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let input = request.into_inner();
        self.authorize_command(&input.store_id, &work)?;
        crate::control::key(&input.idempotency_key)?;
        let control = self.control(&input.store_id)?;
        let value = if work.requires_owned_io() {
            control
                .publication_with_owner(&input.idempotency_key, work.clone())
                .await
        } else {
            control.publication(&input.idempotency_key).await
        }
        .map_err(crate::control::status)?;
        if value
            .as_ref()
            .is_some_and(|v| v.idempotency_key != input.idempotency_key)
        {
            return Err(Status::data_loss("native control publication key mismatch"));
        }
        Ok(Response::new(p::PublicationReply {
            publication: value.map(control_publication).transpose()?,
        }))
    }
}
fn request_commit(bytes: &[u8]) -> Result<crate::CommitId, Status> {
    let head = crate::CommitId(
        bytes
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid native control commit ID"))?,
    );
    crate::control::commit(head)?;
    Ok(head)
}
fn control_publication(
    value: crate::ConfirmedPublication,
) -> Result<p::ConfirmedPublication, Status> {
    crate::control::reference_name(&value.name)
        .map_err(|_| Status::data_loss("native control returned an invalid publication name"))?;
    crate::control::key(&value.idempotency_key)
        .map_err(|_| Status::data_loss("native control returned an invalid publication key"))?;
    crate::control::commit(value.head)
        .map_err(|_| Status::data_loss("native control returned an invalid publication head"))?;
    if value.generation == 0 {
        return Err(Status::data_loss(
            "native control returned an invalid publication generation",
        ));
    }
    Ok(p::ConfirmedPublication {
        idempotency_key: value.idempotency_key,
        name: value.name,
        generation: value.generation,
        head: value.head.0.to_vec(),
    })
}

impl BrokerService {
    async fn file_response(&self,source:jj_native_backend::FileRead,work:Arc<Work>,key:tinytree_model::ObjectKey)->Result<Response<Stream<p::ReadFrame>>,Status>{
        let (descriptor, mut source) = source.into_parts();
        if descriptor.key != key || descriptor.size_bytes > self.inner.config.max_file_bytes {
            return Err(Status::resource_exhausted("broker file exceeds limit"));
        }
        let frame = self.inner.config.max_frame_bytes;
        let (sender, receiver) = mpsc::channel(2);
        tokio::spawn(async move {
            let deadline = work.deadline;
            let _work = work;
            let result = async {
                send_read(
                    &sender,
                    Ok(p::ReadFrame {
                        frame: Some(p::read_frame::Frame::Descriptor(wire::describe(descriptor))),
                    }),
                    deadline,
                )
                .await?;
                let mut offset = 0u64;
                let mut count = 0u64;
                loop {
                    if sender.is_closed() || Instant::now() >= deadline {
                        return Err(Status::cancelled("broker file consumer stopped"));
                    }
                    let mut data = vec![0; frame];
                    // The owned task retains its budget until this read really
                    // finishes, even when the transport body has expired.
                    let n = source
                        .read(&mut data)
                        .await
                        .map_err(|_| Status::data_loss("broker source read failed"))?;
                    if n == 0 {
                        break;
                    }
                    data.truncate(n);
                    if n as u64 > descriptor.size_bytes.saturating_sub(offset) {
                        return Err(Status::data_loss("broker source size changed"));
                    }
                    send_read(
                        &sender,
                        Ok(p::ReadFrame {
                            frame: Some(p::read_frame::Frame::Chunk(p::Chunk { offset, data })),
                        }),
                        deadline,
                    )
                    .await?;
                    offset += n as u64;
                    count += 1;
                }
                if offset != descriptor.size_bytes {
                    return Err(Status::data_loss("broker source truncated"));
                }
                send_read(
                    &sender,
                    Ok(p::ReadFrame {
                        frame: Some(p::read_frame::Frame::Complete(p::Complete {
                            count,
                            size_bytes: offset,
                            digest: descriptor.key.id.0.to_vec(),
                        })),
                    }),
                    deadline,
                )
                .await
            }
            .await;
            if let Err(status) = result {
                let _ = send_read(&sender, Err(status), deadline).await;
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }
}
