use crate::command::CommandChannel;
use crate::{
    Config, NativeQueryIo, QueryOwner, QueryReply, proto as p, query::*, query_protocol as q,
    query_service::handle, wire,
};
use http_body::Body as _;
use prost::Message;
use std::{
    future::Future,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tinytree_model::CommitId;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tonic::{Request, Status, codec::CompressionEncoding};
fn integrity() -> Status {
    Status::data_loss("invalid native query response")
}
fn checked_id(value: Option<&q::CommitId>) -> Result<[u8; 32], Status> {
    value
        .ok_or_else(integrity)?
        .value
        .as_slice()
        .try_into()
        .map_err(|_| integrity())
}
fn checked_rows(rows: &[q::GraphRow], limits: tinytree_model::Limits) -> Result<(), Status> {
    let mut ids = std::collections::HashSet::new();
    for row in rows {
        let id = checked_id(row.commit.as_ref())?;
        let change: &[u8] = &row.change.as_ref().ok_or_else(integrity)?.value;
        if change.len() != 16 || !ids.insert(id) {
            return Err(integrity());
        }
        for edge in &row.edges {
            match edge.edge.as_ref().ok_or_else(integrity)? {
                q::selected_edge::Edge::Direct(v) => {
                    checked_id(v.target.as_ref())?;
                }
                q::selected_edge::Edge::Indirect(v) => {
                    checked_id(v.target.as_ref())?;
                }
                q::selected_edge::Edge::Missing(v) => {
                    checked_id(v.boundary.as_ref())?;
                }
            }
        }
        if let Some(data) = &row.canonical {
            if id == [0; 32] {
                return Err(integrity());
            }
            let d = data.descriptor.as_ref().ok_or_else(integrity)?;
            let key = d.key.as_ref().ok_or_else(integrity)?;
            if key.kind != tinytree_model::Kind::Commit as i32
                || key.native_id.as_slice() != id
                || d.content_blake3.as_slice() != id
                || d.object_format_version != tinytree_model::FORMAT_VERSION as u32
            {
                return Err(integrity());
            }
            let descriptor = tinytree_model::Descriptor {
                key: tinytree_model::ObjectKey {
                    kind: tinytree_model::Kind::Commit,
                    id: tinytree_model::ObjectId(id),
                },
                size_bytes: d.size_bytes,
                format_version: tinytree_model::FORMAT_VERSION,
            };
            tinytree_model::verify(&descriptor, &data.canonical_envelope, limits)
                .map_err(|_| integrity())?;
            match tinytree_model::decode(&data.canonical_envelope, limits)
                .map_err(|_| integrity())?
            {
                tinytree_model::Object::Commit(commit)
                    if commit.change_id.0.as_slice() == change => {}
                _ => return Err(integrity()),
            }
        }
    }
    Ok(())
}
// The request future can be cancelled before Hyper has finished consuming its
// encoded body. Both directions retain this physical budget through teardown.
struct QueryRpcBudget {
    _external: QueryOwner,
    _call: OwnedSemaphorePermit,
    bytes: Mutex<OwnedSemaphorePermit>,
}
struct OwnedQueryBody {
    body: Pin<Box<tonic::body::Body>>,
    _owner: Arc<QueryRpcBudget>,
}
impl http_body::Body for OwnedQueryBody {
    type Data = bytes::Bytes;
    type Error = tonic::Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.body.as_mut().poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
#[derive(Clone)]
struct OwnedQueryChannel {
    channel: CommandChannel,
    owner: Arc<QueryRpcBudget>,
}
impl tower::Service<http::Request<tonic::body::Body>> for OwnedQueryChannel {
    type Response = http::Response<tonic::body::Body>;
    type Error = tonic::transport::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.channel, cx)
    }
    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let owner = self.owner.clone();
        let request = request.map(|body| {
            tonic::body::Body::new(OwnedQueryBody {
                body: Box::pin(body),
                _owner: owner.clone(),
            })
        });
        let response = tower::Service::call(&mut self.channel, request);
        Box::pin(async move {
            Ok(response.await?.map(|body| {
                tonic::body::Body::new(OwnedQueryBody {
                    body: Box::pin(body),
                    _owner: owner,
                })
            }))
        })
    }
}
#[derive(Clone)]
pub struct BrokerQueryClient {
    inner: Arc<Inner>,
    operation_deadline: Option<Instant>,
}
#[derive(Clone)]
struct Inner {
    channel: CommandChannel,
    store: String,
    config: Config,
    calls: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    persistent: Arc<Semaphore>,
}
impl std::fmt::Debug for BrokerQueryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerQueryClient").finish_non_exhaustive()
    }
}
impl BrokerQueryClient {
    /// Limit this clone's entire operation, including local queueing and RPC
    /// retries. Repeated calls can only shorten the absolute deadline; sibling
    /// clones retain their own allowance and share only transport/resource pools.
    pub fn with_operation_deadline(mut self, deadline: Instant) -> Self {
        self.operation_deadline = Some(
            self.operation_deadline
                .map_or(deadline, |old| old.min(deadline)),
        );
        self
    }
    /// Bind subsequent local RPCs to a live host command. Runtime only: shared
    /// channel/pools and configured provider identity remain unchanged.
    pub fn with_command_token(mut self, token: [u8; 32]) -> Self {
        let mut inner = (*self.inner).clone();
        inner.channel = inner.channel.with_command_token(token);
        self.inner = Arc::new(inner);
        self
    }

    pub async fn connect(
        socket: impl AsRef<Path>,
        store: impl Into<String>,
        config: Config,
    ) -> Result<Self, Status> {
        Self::connect_local(
            crate::LocalEndpoint::UnixSocket(socket.as_ref().to_owned()),
            store,
            config,
        )
        .await
    }
    /// All channel I/O runs on the independent broker runtime, including when
    /// a synchronous jj adapter blocks its caller's current-thread runtime.
    pub async fn connect_local(
        endpoint: crate::LocalEndpoint,
        store: impl Into<String>,
        config: Config,
    ) -> Result<Self, Status> {
        let store = store.into();
        crate::client::run_control(async move {
            config.validate()?;
            wire::id(&store)?;
            if config.query_memory().saturating_add(16 * 1024) > config.max_memory_bytes {
                return Err(Status::invalid_argument(
                    "query client memory is below decode reservation",
                ));
            }
            let channel = crate::transport::connect_local_channel(endpoint, &config).await?;
            let channel = CommandChannel::new(channel);
            Ok(Self {
                operation_deadline: None,
                inner: Arc::new(Inner {
                    channel,
                    store,
                    calls: Arc::new(Semaphore::new(config.max_inflight)),
                    bytes: Arc::new(Semaphore::new(config.max_memory_bytes)),
                    persistent: Arc::new(Semaphore::new(
                        config.max_memory_bytes - config.query_memory(),
                    )),
                    config,
                }),
            })
        })
        .await
    }
    /// Check a live daemon identity; persisted local manifests are never their
    /// own expected identity. This performs no upstream repository discovery.
    pub async fn describe_provider(&self) -> Result<crate::ProviderDescription, Status> {
        let expected = self.inner.store.clone();
        let reply = self
            .execute(Arc::new(()), |mut client, deadline, store, _| async move {
                client = client
                    .max_decoding_message_size(1024)
                    .max_encoding_message_size(1024);
                Ok(client
                    .describe_provider(request(
                        p::DescribeProviderRequest { store_id: store },
                        deadline,
                        1024,
                    )?)
                    .await?
                    .into_inner())
            })
            .await?
            .value;
        let identity: [u8; 32] = reply.identity.try_into().map_err(|_| integrity())?;
        if reply.store_id != expected || identity == [0; 32] {
            return Err(integrity());
        }
        let index_mode =
            match p::ProviderIndexMode::try_from(reply.index_mode).map_err(|_| integrity())? {
                p::ProviderIndexMode::Legacy => crate::ProviderIndexMode::Legacy,
                p::ProviderIndexMode::Federated => crate::ProviderIndexMode::Federated,
                _ => return Err(integrity()),
            };
        Ok(crate::ProviderDescription {
            identity,
            index_mode,
            query_capable: reply.query_capable.ok_or_else(integrity)?,
        })
    }
    fn preflight(&self, payload_bytes: usize) -> Result<(), Status> {
        crate::query_service::bounded_input(
            payload_bytes.saturating_add(crate::query_service::field_bytes(self.inner.store.len())),
            self.inner.config.max_query_bytes,
        )
    }
    async fn execute<T, F, Fut>(
        &self,
        owner: QueryOwner,
        mut operation: F,
    ) -> Result<QueryReply<T>, Status>
    where
        T: Message + Send + 'static,
        F: FnMut(
                p::query_io_client::QueryIoClient<OwnedQueryChannel>,
                Instant,
                String,
                usize,
            ) -> Fut
            + Send
            + 'static,
        Fut: Future<Output = Result<T, Status>> + Send + 'static,
    {
        let inner = self.inner.clone();
        // Derive the deadline before dispatch to the independent runtime. Time
        // waiting to be polled there or for an admission slot is not renewed.
        let configured = Instant::now() + inner.config.policy.operation_timeout();
        let deadline = self
            .operation_deadline
            .map_or(configured, |bound| bound.min(configured));
        crate::client::run_control(async move {
            reapi::policy::timeout(deadline, async move {
                let call = inner
                    .calls
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Status::unavailable("query client closed"))?;
                let bytes = inner
                    .bytes
                    .clone()
                    .acquire_many_owned(inner.config.query_memory() as u32)
                    .await
                    .map_err(|_| Status::unavailable("query client closed"))?;
                let budget = Arc::new(QueryRpcBudget {
                    _external: owner.clone(),
                    _call: call,
                    bytes: Mutex::new(bytes),
                });
                let client = p::query_io_client::QueryIoClient::new(OwnedQueryChannel {
                    channel: inner.channel.clone(),
                    owner: budget.clone(),
                })
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
                .max_decoding_message_size(inner.config.max_query_bytes)
                .max_encoding_message_size(inner.config.max_query_bytes);
                let value = inner
                    .config
                    .policy
                    .retry(deadline, inner.config.policy.rpc_timeout(), |allowance| {
                        operation(
                            client.clone(),
                            Instant::now() + allowance,
                            inner.store.clone(),
                            inner.config.max_query_bytes,
                        )
                    })
                    .await?;
                let len = value.encoded_len();
                if len > inner.config.max_query_bytes {
                    return Err(Status::resource_exhausted(
                        "query client reply exceeds limit",
                    ));
                }
                let retained = len.saturating_mul(128).saturating_add(4096);
                // Persistent replies cannot consume the headroom needed for
                // another full decode. Fail instead of waiting on held pages.
                let state = inner
                    .persistent
                    .clone()
                    .try_acquire_many_owned(retained as u32)
                    .map_err(|_| {
                        Status::resource_exhausted("native query retained reply budget exhausted")
                    })?;
                let retained = budget
                    .bytes
                    .lock()
                    .map_err(|_| Status::internal("query client memory requires recovery"))?
                    .split(retained)
                    .ok_or_else(|| Status::internal("query client reply reservation missing"))?;
                Ok(QueryReply {
                    value,
                    owner: Arc::new((state, retained)),
                })
            })
            .await
        })
        .await
        .map_err(crate::query::status)
    }
}
fn request<T: Message>(value: T, deadline: Instant, cap: usize) -> Result<Request<T>, Status> {
    if value.encoded_len() > cap {
        return Err(Status::resource_exhausted(
            "native query request exceeds limit",
        ));
    }
    let left = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| Status::deadline_exceeded("native query deadline exceeded"))?;
    let mut request = Request::new(value);
    request.set_timeout(left);
    Ok(request)
}
fn ids(v: Vec<CommitId>) -> Vec<Vec<u8>> {
    v.into_iter().map(|id| id.0.to_vec()).collect()
}
fn scope(v: p::QueryScopeReply) -> Result<QueryScope, Status> {
    if v.expires_at_unix_seconds <= 0 {
        return Err(integrity());
    }
    Ok(QueryScope {
        handle: handle(&v.handle)?,
        expires_at_unix_seconds: v.expires_at_unix_seconds,
        query_identity_version: crate::normalize_query_identity_version(v.query_identity_version)?,
        cloud_source_digest: v
            .cloud_source_digest
            .as_deref()
            .map(handle)
            .transpose()
            .map_err(|_| integrity())?,
    })
}
#[async_trait::async_trait]
impl NativeQueryIo for BrokerQueryClient {
    fn resources(&self) -> QueryResources {
        QueryResources {
            memory_bytes: self.inner.config.query_memory() as u64,
            temporary_bytes: 0,
            persistent_memory_bytes: self.inner.config.max_memory_bytes as u64,
        }
    }
    async fn open_cloud_scope(
        &self,
        i: OpenCloudQueryScope,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, Status> {
        let cap = self.inner.config.max_query_bytes;
        self.preflight(
            i.source
                .validate(cap)?
                .saturating_add(crate::query_service::overlay_bytes(&i.overlay, cap)?)
                .saturating_add(128),
        )?;
        let expected = i.source.digest();
        let reply = self
            .execute(o, move |mut client, deadline, store, cap| {
                let i = i.clone();
                async move {
                    let source = i.source;
                    Ok(client
                        .open_cloud_scope(request(
                            p::QueryOpenCloudScopeRequest {
                                store_id: store,
                                source: Some(p::NativeCloudQuerySource {
                                    provider_identity: source.provider_identity.to_vec(),
                                    workspace_id: source.workspace_id.to_vec(),
                                    generation: source.generation,
                                    manifest_id: source.manifest_id.to_vec(),
                                    manifest_blake3: source.manifest_blake3.to_vec(),
                                    manifest_size_bytes: source.manifest_size_bytes,
                                    index_root_ids: source
                                        .index_root_ids
                                        .into_iter()
                                        .map(|id| id.to_vec())
                                        .collect(),
                                    expected_anchors: ids(source.expected_anchors),
                                }),
                                overlay: i.overlay,
                                replay_key: i.replay_key.to_vec(),
                            },
                            deadline,
                            cap,
                        )?)
                        .await?
                        .into_inner())
                }
            })
            .await?;
        let value = scope(reply.value)?;
        crate::query_service::validate_scope_source(&value, Some(expected))?;
        Ok(QueryReply {
            value,
            owner: reply.owner,
        })
    }
    async fn open_scope(
        &self,
        i: OpenQueryScope,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, Status> {
        let expected = crate::normalize_query_identity_version(i.query_identity_version)?;
        let cap = self.inner.config.max_query_bytes;
        self.preflight(
            crate::query_service::visible_bytes(&i.roots, cap)?
                .saturating_add(crate::query_service::overlay_bytes(&i.overlay, cap)?)
                .saturating_add(40),
        )?;
        let r = self
            .execute(o, move |mut c, d, s, cap| {
                let i = i.clone();
                async move {
                    Ok(c.open_scope(request(
                        p::QueryOpenScopeRequest {
                            store_id: s,
                            roots: ids(i.roots),
                            query_identity_version: i.query_identity_version,
                            overlay: i.overlay,
                            replay_key: i.replay_key.to_vec(),
                        },
                        d,
                        cap,
                    )?)
                    .await?
                    .into_inner())
                }
            })
            .await?;
        let value = scope(r.value)?;
        crate::query_service::validate_scope_source(&value, None)?;
        if value.query_identity_version != expected {
            return Err(Status::unimplemented(
                "native query identity negotiation failed",
            ));
        }
        Ok(QueryReply {
            value,
            owner: r.owner,
        })
    }
    async fn fork_scope(
        &self,
        i: ForkQueryScope,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, Status> {
        let expected = i.expected_cloud_source_digest;
        let cap = self.inner.config.max_query_bytes;
        self.preflight(
            crate::query_service::overlay_bytes(&i.overlay, cap)?
                .saturating_add(68)
                .saturating_add(if expected.is_some() { 34 } else { 0 }),
        )?;
        let r = self
            .execute(o, move |mut c, d, s, cap| {
                let i = i.clone();
                async move {
                    Ok(c.fork_scope(request(
                        p::QueryForkScopeRequest {
                            store_id: s,
                            parent: i.parent.to_vec(),
                            expected_cloud_source_digest: i
                                .expected_cloud_source_digest
                                .map(|digest| digest.to_vec()),
                            overlay: i.overlay,
                            replay_key: i.replay_key.to_vec(),
                        },
                        d,
                        cap,
                    )?)
                    .await?
                    .into_inner())
                }
            })
            .await?;
        let value = scope(r.value)?;
        crate::query_service::validate_scope_source(&value, expected)?;
        Ok(QueryReply {
            value,
            owner: r.owner,
        })
    }
    async fn facts(
        &self,
        s: [u8; 32],
        f: Vec<q::GraphFact>,
        o: QueryOwner,
    ) -> Result<QueryReply<q::GraphFacts>, Status> {
        let expected = crate::query_service::fact_shapes(&f, self.inner.config.max_query_bytes)?;
        let n = f.len();
        self.preflight(f.iter().fold(34usize, |n, f| {
            n.saturating_add(crate::query_service::field_bytes(f.encoded_len()))
        }))?;
        let r = self
            .execute(o, move |mut c, d, store_id, cap| {
                let f = f.clone();
                async move {
                    Ok(c.facts(request(
                        p::QueryFactsRequest {
                            store_id,
                            scope: s.to_vec(),
                            facts: f,
                        },
                        d,
                        cap,
                    )?)
                    .await?
                    .into_inner())
                }
            })
            .await?;
        if r.value.scope_digest.len() != 32 || r.value.index_epoch.len() != 16 {
            return Err(integrity());
        }
        for (answer, expected) in r.value.answers.iter().zip(expected) {
            let actual = match answer.answer.as_ref().ok_or_else(integrity)? {
                q::graph_fact_answer::Answer::HasId(_) => 1,
                q::graph_fact_answer::Answer::IsAncestor(_) => 2,
                q::graph_fact_answer::Answer::Heads(_) => 3,
                q::graph_fact_answer::Answer::CommonAncestors(_) => 4,
                q::graph_fact_answer::Answer::Membership(_) => 5,
            };
            if actual != expected {
                return Err(integrity());
            }
            match answer.answer.as_ref().ok_or_else(integrity)? {
                q::graph_fact_answer::Answer::HasId(_)
                | q::graph_fact_answer::Answer::IsAncestor(_)
                | q::graph_fact_answer::Answer::Membership(_) => {}
                q::graph_fact_answer::Answer::Heads(v)
                | q::graph_fact_answer::Answer::CommonAncestors(v) => {
                    for id in &v.commits {
                        checked_id(Some(id))?;
                    }
                }
            }
        }
        if r.value.answers.len() != n {
            return Err(Status::data_loss("native query fact count mismatch"));
        }
        Ok(r)
    }
    async fn resolve_commit_prefix(
        &self,
        s: [u8; 32],
        f: String,
        o: QueryOwner,
    ) -> Result<QueryReply<q::CommitPrefixResult>, Status> {
        crate::query_service::prefix(&f, 64)?;
        self.preflight(34usize.saturating_add(crate::query_service::field_bytes(f.len())))?;
        let r = self
            .execute(o, move |mut c, d, store_id, cap| {
                let f = f.clone();
                async move {
                    Ok(c.resolve_commit_prefix(request(
                        p::QueryCommitPrefixRequest {
                            store_id,
                            scope: s.to_vec(),
                            hex_prefix: f,
                            selection: None,
                        },
                        d,
                        cap,
                    )?)
                    .await?
                    .into_inner())
                }
            })
            .await?;
        if r.value.result.is_none() {
            return Err(Status::data_loss("missing native commit prefix result"));
        }
        Ok(r)
    }
    async fn resolve_change_prefix(
        &self,
        s: [u8; 32],
        f: String,
        h: Vec<CommitId>,
        o: QueryOwner,
    ) -> Result<QueryReply<q::ChangePrefixResult>, Status> {
        crate::query_service::prefix(&f, 32)?;
        self.preflight(
            crate::query_service::visible_bytes(&h, self.inner.config.max_query_bytes)?
                .saturating_add(34)
                .saturating_add(crate::query_service::field_bytes(f.len())),
        )?;
        let r = self
            .execute(o, move |mut c, d, store_id, cap| {
                let f = f.clone();
                let h = h.clone();
                async move {
                    Ok(c.resolve_change_prefix(request(
                        p::QueryChangePrefixRequest {
                            store_id,
                            scope: s.to_vec(),
                            hex_prefix: f,
                            visible_heads: ids(h),
                            selection: None,
                        },
                        d,
                        cap,
                    )?)
                    .await?
                    .into_inner())
                }
            })
            .await?;
        if r.value.result.is_none() {
            return Err(Status::data_loss("missing native change prefix result"));
        }
        Ok(r)
    }
    async fn shortest_prefix(
        &self,
        s: [u8; 32],
        t: q::shortest_prefix_request::Target,
        h: Vec<CommitId>,
        o: QueryOwner,
    ) -> Result<QueryReply<q::ShortestPrefixResult>, Status> {
        self.preflight(
            crate::query_service::visible_bytes(&h, self.inner.config.max_query_bytes)?
                .saturating_add(34)
                .saturating_add(crate::query_service::target_bytes(&t)?),
        )?;
        let target = match t {
            q::shortest_prefix_request::Target::Commit(v) => {
                p::query_shortest_prefix_request::Target::Commit(v.value)
            }
            q::shortest_prefix_request::Target::Change(v) => {
                p::query_shortest_prefix_request::Target::Change(v.value)
            }
        };
        self.execute(o, move |mut c, d, store_id, cap| {
            let target = target.clone();
            let h = h.clone();
            async move {
                Ok(c.shortest_prefix(request(
                    p::QueryShortestPrefixRequest {
                        store_id,
                        scope: s.to_vec(),
                        target: Some(target),
                        visible_heads: ids(h),
                        selection: None,
                    },
                    d,
                    cap,
                )?)
                .await?
                .into_inner())
            }
        })
        .await
    }
    async fn resolve_commit_prefix_selected(
        &self,
        s: [u8; 32],
        f: String,
        selection: q::GraphExpr,
        o: QueryOwner,
    ) -> Result<QueryReply<q::CommitPrefixResult>, Status> {
        crate::validate_prefix_selection(&selection, self.inner.config.max_query_bytes)?;
        crate::query_service::prefix(&f, 64)?;
        self.preflight(
            34usize
                .saturating_add(crate::query_service::field_bytes(selection.encoded_len()))
                .saturating_add(crate::query_service::field_bytes(f.len())),
        )?;
        self.preflight(
            crate::query_service::field_bytes(selection.encoded_len()).saturating_add(128),
        )?;
        let r = self
            .execute(o, move |mut c, d, store_id, cap| {
                let selection = selection.clone();
                let f = f.clone();
                async move {
                    let digest = crate::prefix_selection_digest(&selection, cap)?;
                    let value = c
                        .resolve_commit_prefix(request(
                            p::QueryCommitPrefixRequest {
                                store_id,
                                scope: s.to_vec(),
                                hex_prefix: f,
                                selection: Some(selection),
                            },
                            d,
                            cap,
                        )?)
                        .await?
                        .into_inner();
                    if value.selection_digest.as_deref() != Some(digest.as_slice()) {
                        return Err(Status::data_loss("selected prefix digest mismatch"));
                    }
                    Ok(value)
                }
            })
            .await?;
        if r.value.result.is_none() {
            return Err(Status::data_loss("missing native commit prefix result"));
        }
        Ok(r)
    }
    async fn resolve_change_prefix_selected(
        &self,
        s: [u8; 32],
        f: String,
        h: Vec<CommitId>,
        selection: q::GraphExpr,
        o: QueryOwner,
    ) -> Result<QueryReply<q::ChangePrefixResult>, Status> {
        crate::validate_prefix_selection(&selection, self.inner.config.max_query_bytes)?;
        crate::query_service::prefix(&f, 32)?;
        self.preflight(
            crate::query_service::visible_bytes(&h, self.inner.config.max_query_bytes)?
                .saturating_add(34)
                .saturating_add(crate::query_service::field_bytes(selection.encoded_len()))
                .saturating_add(crate::query_service::field_bytes(f.len())),
        )?;
        self.preflight(
            crate::query_service::field_bytes(selection.encoded_len()).saturating_add(128),
        )?;
        let r = self
            .execute(o, move |mut c, d, store_id, cap| {
                let selection = selection.clone();
                let f = f.clone();
                let h = h.clone();
                async move {
                    let digest = crate::prefix_selection_digest(&selection, cap)?;
                    let value = c
                        .resolve_change_prefix(request(
                            p::QueryChangePrefixRequest {
                                store_id,
                                scope: s.to_vec(),
                                hex_prefix: f,
                                visible_heads: ids(h),
                                selection: Some(selection),
                            },
                            d,
                            cap,
                        )?)
                        .await?
                        .into_inner();
                    if value.selection_digest.as_deref() != Some(digest.as_slice()) {
                        return Err(Status::data_loss("selected prefix digest mismatch"));
                    }
                    Ok(value)
                }
            })
            .await?;
        if r.value.result.is_none() {
            return Err(Status::data_loss("missing native change prefix result"));
        }
        Ok(r)
    }
    async fn shortest_prefix_selected(
        &self,
        s: [u8; 32],
        t: q::shortest_prefix_request::Target,
        h: Vec<CommitId>,
        selection: q::GraphExpr,
        o: QueryOwner,
    ) -> Result<QueryReply<q::ShortestPrefixResult>, Status> {
        crate::validate_prefix_selection(&selection, self.inner.config.max_query_bytes)?;
        self.preflight(
            crate::query_service::visible_bytes(&h, self.inner.config.max_query_bytes)?
                .saturating_add(34)
                .saturating_add(crate::query_service::field_bytes(selection.encoded_len()))
                .saturating_add(crate::query_service::target_bytes(&t)?),
        )?;
        let target = match t {
            q::shortest_prefix_request::Target::Commit(v) => {
                p::query_shortest_prefix_request::Target::Commit(v.value)
            }
            q::shortest_prefix_request::Target::Change(v) => {
                p::query_shortest_prefix_request::Target::Change(v.value)
            }
        };
        let r = self
            .execute(o, move |mut c, d, store_id, cap| {
                let selection = selection.clone();
                let target = target.clone();
                let h = h.clone();
                async move {
                    let digest = crate::prefix_selection_digest(&selection, cap)?;
                    let value = c
                        .shortest_prefix(request(
                            p::QueryShortestPrefixRequest {
                                store_id,
                                scope: s.to_vec(),
                                target: Some(target),
                                visible_heads: ids(h),
                                selection: Some(selection),
                            },
                            d,
                            cap,
                        )?)
                        .await?
                        .into_inner();
                    if value.selection_digest.as_deref() != Some(digest.as_slice()) {
                        return Err(Status::data_loss("selected prefix digest mismatch"));
                    }
                    Ok(value)
                }
            })
            .await?;
        if r.value.in_selection.is_none() {
            return Err(Status::data_loss(
                "selected shortest prefix binding missing",
            ));
        }
        Ok(r)
    }
    async fn begin(&self, i: BeginQuery, o: QueryOwner) -> Result<QueryReply<QueryHandle>, Status> {
        crate::query_service::expression_shape(&i.expression, self.inner.config.max_query_bytes)?;
        self.preflight(
            crate::query_service::field_bytes(i.expression.encoded_len()).saturating_add(76),
        )?;
        let r = self
            .execute(o, move |mut c, d, store_id, cap| {
                let i = i.clone();
                async move {
                    Ok(c.begin(request(
                        p::QueryBeginRequest {
                            store_id,
                            scope: i.scope.to_vec(),
                            expression: Some(i.expression),
                            projection: i.projection as i32,
                            page_size: i.page_size,
                            replay_key: i.replay_key.to_vec(),
                        },
                        d,
                        cap,
                    )?)
                    .await?
                    .into_inner())
                }
            })
            .await?;
        Ok(QueryReply {
            value: QueryHandle {
                handle: handle(&r.value.handle)?,
                expires_at_unix_seconds: r.value.expires_at_unix_seconds,
            },
            owner: r.owner,
        })
    }
    async fn page(
        &self,
        q: [u8; 32],
        s: u64,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryPage>, Status> {
        let r = self
            .execute(o, move |mut c, d, store_id, cap| async move {
                Ok(c.page(request(
                    p::QueryPageRequest {
                        store_id,
                        query: q.to_vec(),
                        expected_sequence: s,
                    },
                    d,
                    cap,
                )?)
                .await?
                .into_inner())
            })
            .await?;
        checked_rows(&r.value.rows, self.inner.config.model_limits())?;
        if r.value.sequence != s {
            return Err(Status::data_loss("native query page sequence mismatch"));
        }
        let exhausted = r
            .value
            .exhausted
            .ok_or_else(|| Status::data_loss("missing native query exhaustion"))?;
        Ok(QueryReply {
            value: QueryPage {
                sequence: s,
                rows: r.value.rows,
                exhausted,
            },
            owner: r.owner,
        })
    }
    async fn read_commit(
        &self,
        s: [u8; 32],
        id: CommitId,
        o: QueryOwner,
    ) -> Result<QueryReply<jj_native_backend::VerifiedMetadata>, Status> {
        let limits = self.inner.config.model_limits();
        let r = self
            .execute(o, move |mut c, d, store_id, cap| async move {
                Ok(c.read_commit(request(
                    p::QueryReadCommitRequest {
                        store_id,
                        scope: s.to_vec(),
                        commit: id.0.to_vec(),
                    },
                    d,
                    cap,
                )?)
                .await?
                .into_inner())
            })
            .await?;
        let descriptor = wire::descriptor(
            r.value
                .descriptor
                .ok_or_else(|| Status::data_loss("missing native query commit descriptor"))?,
        )?;
        if descriptor.key.kind != tinytree_model::Kind::Commit || descriptor.key.id.0 != id.0 {
            return Err(Status::data_loss("native query commit identity mismatch"));
        }
        let value =
            jj_native_backend::VerifiedMetadata::verify(descriptor, r.value.canonical, limits)
                .map_err(|_| Status::data_loss("invalid native query canonical commit"))?;
        Ok(QueryReply {
            value,
            owner: r.owner,
        })
    }
    async fn path_history(
        &self,
        input: ScopedPathQuery,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::PathHistoryResult>, Status> {
        input.validate(self.inner.config.max_query_bytes)?;
        self.preflight(input.path.len().saturating_add(128))?;
        let expected = input.clone();
        let result = self
            .execute(owner, move |mut client, deadline, store_id, cap| {
                let payload = p::QueryPathRequest {
                    store_id,
                    scope: input.scope.to_vec(),
                    revision: input.revision.0.to_vec(),
                    path: input.path.clone(),
                    policy: input.policy as i32,
                    max_commits: input.max_commits,
                };
                async move {
                    Ok(client
                        .path_history(request(payload, deadline, cap)?)
                        .await?
                        .into_inner())
                }
            })
            .await?;
        expected.check_history(&result.value)?;
        Ok(result)
    }
    async fn blame(
        &self,
        input: ScopedPathQuery,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::BlameResult>, Status> {
        input.validate(self.inner.config.max_query_bytes)?;
        self.preflight(input.path.len().saturating_add(128))?;
        let expected = input.clone();
        let result = self
            .execute(owner, move |mut client, deadline, store_id, cap| {
                let payload = p::QueryPathRequest {
                    store_id,
                    scope: input.scope.to_vec(),
                    revision: input.revision.0.to_vec(),
                    path: input.path.clone(),
                    policy: input.policy as i32,
                    max_commits: input.max_commits,
                };
                async move {
                    Ok(client
                        .blame(request(payload, deadline, cap)?)
                        .await?
                        .into_inner())
                }
            })
            .await?;
        expected.check_blame(&result.value, self.inner.config.model_limits())?;
        Ok(result)
    }
    async fn search(
        &self,
        input: ScopedSearchQuery,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::SearchResult>, Status> {
        input.validate(self.inner.config.max_query_bytes)?;
        self.preflight(
            input
                .pattern
                .len()
                .saturating_add(input.path_prefix.len())
                .saturating_add(input.cursor.len())
                .saturating_add(256),
        )?;
        let expected = input.clone();
        let result = self
            .execute(owner, move |mut client, deadline, store_id, cap| {
                let payload = p::QuerySearchRequest {
                    store_id,
                    scope: input.scope.to_vec(),
                    revision: input.revision.0.to_vec(),
                    pattern: input.pattern.clone(),
                    path_prefix: input.path_prefix.clone(),
                    policy: input.policy as i32,
                    page_size: input.page_size,
                    cursor: input.cursor.clone(),
                };
                async move {
                    Ok(client
                        .search(request(payload, deadline, cap)?)
                        .await?
                        .into_inner())
                }
            })
            .await?;
        expected.check_result(&result.value)?;
        Ok(result)
    }
    async fn close_query(&self, h: [u8; 32], o: QueryOwner) -> Result<(), Status> {
        self.execute(o, move |mut c, d, store_id, cap| async move {
            Ok(c.close_query(request(
                p::QueryCloseRequest {
                    store_id,
                    handle: h.to_vec(),
                },
                d,
                cap,
            )?)
            .await?
            .into_inner())
        })
        .await
        .map(|_| ())
    }
    async fn close_scope(&self, h: [u8; 32], o: QueryOwner) -> Result<(), Status> {
        self.execute(o, move |mut c, d, store_id, cap| async move {
            Ok(c.close_scope(request(
                p::QueryCloseRequest {
                    store_id,
                    handle: h.to_vec(),
                },
                d,
                cap,
            )?)
            .await?
            .into_inner())
        })
        .await
        .map(|_| ())
    }
}
