use crate::{
    BrokerService, NativeQueryIo, QueryOwner, QueryReply, proto as p, query::*,
    query_protocol as q, service::Work, wire,
};
use http::{Request as HttpRequest, Response as HttpResponse};
use prost::Message;
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{Request, Response, Status, body::Body, server::NamedService};
use tower::Service;

pub(crate) fn handle(bytes: &[u8]) -> Result<[u8; 32], Status> {
    bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("native query handle must be 32 bytes"))
}
fn commits(values: Vec<Vec<u8>>) -> Result<Vec<tinytree_model::CommitId>, Status> {
    values
        .into_iter()
        .map(|v| handle(&v).map(tinytree_model::CommitId))
        .collect()
}
fn input<T: Message>(request: Request<T>, cap: usize) -> Result<(T, Arc<Work>), Status> {
    let work = request
        .extensions()
        .get::<Arc<Work>>()
        .cloned()
        .ok_or_else(|| Status::internal("broker query missing resource scope"))?;
    if request.get_ref().encoded_len() > cap {
        return Err(Status::resource_exhausted(
            "native query request exceeds limit",
        ));
    }
    Ok((request.into_inner(), work))
}
fn reply<T: Message>(
    value: QueryReply<T>,
    work: Arc<Work>,
    cap: usize,
) -> Result<Response<T>, Status> {
    if value.value.encoded_len() > cap {
        return Err(Status::resource_exhausted(
            "native query reply exceeds limit",
        ));
    }
    let mut response = Response::new(value.value);
    response
        .extensions_mut()
        .insert(grpc_transport::ResponseBudget::new((value.owner, work)));
    Ok(response)
}
pub(crate) fn prefix(value: &str, width: usize) -> Result<(), Status> {
    if value.len() > width
        || !value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(Status::invalid_argument("invalid native query prefix"));
    }
    Ok(())
}
pub(crate) fn field_bytes(len: usize) -> usize {
    let mut n = len;
    let mut width = 1usize;
    while n >= 128 {
        n >>= 7;
        width += 1;
    }
    len.saturating_add(width).saturating_add(1)
}
pub(crate) fn visible_bytes(
    heads: &[tinytree_model::CommitId],
    cap: usize,
) -> Result<usize, Status> {
    let bytes = heads.len().saturating_mul(34);
    bounded_input(bytes, cap)?;
    Ok(bytes)
}
pub(crate) fn overlay_bytes(overlay: &[Vec<u8>], cap: usize) -> Result<usize, Status> {
    if overlay.len() > cap / 2 {
        return Err(Status::resource_exhausted(
            "native query overlay exceeds request bound",
        ));
    }
    let mut bytes = 0usize;
    for value in overlay {
        bytes = bytes.saturating_add(field_bytes(value.len()));
        bounded_input(bytes, cap)?;
    }
    Ok(bytes)
}
pub(crate) fn target_bytes(target: &q::shortest_prefix_request::Target) -> Result<usize, Status> {
    let (bytes, width) = match target {
        q::shortest_prefix_request::Target::Commit(id) => (&id.value, 32),
        q::shortest_prefix_request::Target::Change(id) => (&id.value, 16),
    };
    if bytes.len() != width {
        return Err(Status::invalid_argument(
            "invalid native query prefix target ID",
        ));
    }
    Ok(field_bytes(width))
}
pub(crate) fn fact_shapes(facts: &[q::GraphFact], cap: usize) -> Result<Vec<u8>, Status> {
    if facts.len() > cap / 2 {
        return Err(Status::resource_exhausted(
            "native query facts exceed request bound",
        ));
    }
    let mut literals = 0usize;
    let mut id = |id: Option<&q::CommitId>| -> Result<(), Status> {
        if id.is_none_or(|id| id.value.len() != 32) {
            return Err(Status::invalid_argument(
                "invalid native query fact commit ID",
            ));
        }
        literals = literals.saturating_add(1);
        bounded_input(literals.saturating_mul(34), cap)
    };
    let mut shapes = Vec::with_capacity(facts.len());
    let mut bytes = 0usize;
    for fact in facts {
        let shape = match fact
            .fact
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing native query fact"))?
        {
            q::graph_fact::Fact::HasId(v) => {
                id(v.commit.as_ref())?;
                1
            }
            q::graph_fact::Fact::IsAncestor(v) => {
                id(v.ancestor.as_ref())?;
                id(v.descendant.as_ref())?;
                2
            }
            q::graph_fact::Fact::Heads(v) => {
                bounded_input(v.commits.len().saturating_mul(34), cap)?;
                for v in &v.commits {
                    id(Some(v))?;
                }
                3
            }
            q::graph_fact::Fact::CommonAncestors(v) => {
                bounded_input(
                    v.left
                        .len()
                        .saturating_add(v.right.len())
                        .saturating_mul(34),
                    cap,
                )?;
                for v in v.left.iter().chain(&v.right) {
                    id(Some(v))?;
                }
                4
            }
        };
        bytes = bytes.saturating_add(field_bytes(fact.encoded_len()));
        bounded_input(bytes, cap)?;
        shapes.push(shape);
    }
    Ok(shapes)
}
pub(crate) fn expression_shape(expression: &q::GraphExpr, cap: usize) -> Result<(), Status> {
    let mut pending = vec![(expression, 1usize)];
    let mut count = 0usize;
    let mut literals = 0usize;
    while let Some((node, depth)) = pending.pop() {
        count += 1;
        if count > 1024 || depth > 64 {
            return Err(Status::resource_exhausted(
                "native query AST exceeds structural bounds",
            ));
        }
        if node.version != 1 {
            return Err(Status::invalid_argument(
                "unsupported native query AST version",
            ));
        }
        macro_rules! child {
            ($value:expr) => {{
                let value = $value
                    .ok_or_else(|| Status::invalid_argument("missing native query AST input"))?;
                pending.push((value, depth + 1));
            }};
        }
        use q::graph_expr::Expression as E;
        match node
            .expression
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing native query AST operation"))?
        {
            E::Commits(v) => {
                literals = literals.saturating_add(v.commits.len());
                bounded_input(literals.saturating_mul(34), cap)?;
                if v.commits.iter().any(|id| id.value.len() != 32) {
                    return Err(Status::invalid_argument(
                        "invalid native query literal commit ID",
                    ));
                }
            }
            E::Ancestors(v) => child!(v.heads.as_deref()),
            E::Range(v) => {
                child!(v.roots.as_deref());
                child!(v.heads.as_deref());
            }
            E::DagRange(v) => {
                child!(v.roots.as_deref());
                child!(v.heads.as_deref());
            }
            E::Heads(v) | E::Roots(v) => child!(v.input.as_deref()),
            E::Union(v) | E::Intersection(v) => {
                if v.inputs
                    .len()
                    .saturating_add(pending.len())
                    .saturating_add(count)
                    > 1024
                {
                    return Err(Status::resource_exhausted(
                        "native query AST exceeds structural bounds",
                    ));
                }
                pending.extend(v.inputs.iter().map(|node| (node, depth + 1)));
            }
            E::Difference(v) => {
                child!(v.left.as_deref());
                child!(v.right.as_deref());
            }
            E::Latest(v) => child!(v.candidates.as_deref()),
        }
    }
    Ok(())
}
pub(crate) fn bounded_input(bytes: usize, cap: usize) -> Result<(), Status> {
    if bytes > cap {
        return Err(Status::resource_exhausted(
            "native query request exceeds limit",
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub struct QueryRpcService {
    pub(crate) inner:
        tower::util::BoxCloneSyncService<HttpRequest<Body>, HttpResponse<Body>, Infallible>,
    pub(crate) describe:
        tower::util::BoxCloneSyncService<HttpRequest<Body>, HttpResponse<Body>, Infallible>,
}
impl NamedService for QueryRpcService {
    const NAME: &'static str = "native.broker.v1.QueryIo";
}
impl Service<HttpRequest<Body>> for QueryRpcService {
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        std::task::ready!(self.inner.poll_ready(cx))?;
        self.describe.poll_ready(cx)
    }
    fn call(&mut self, request: HttpRequest<Body>) -> Self::Future {
        if request.uri().path() == "/native.broker.v1.QueryIo/DescribeProvider" {
            self.describe.call(request)
        } else {
            self.inner.call(request)
        }
    }
}
#[tonic::async_trait]
impl p::query_io_server::QueryIo for BrokerService {
    async fn describe_provider(
        &self,
        request: Request<p::DescribeProviderRequest>,
    ) -> Result<Response<p::ProviderDescription>, Status> {
        let (input, work) = input(request, 1024)?;
        let description = self.describe_provider(&input.store_id)?;
        reply(
            QueryReply {
                value: p::ProviderDescription {
                    store_id: input.store_id,
                    identity: description.identity.to_vec(),
                    query_capable: Some(description.query_capable),
                    index_mode: match description.index_mode {
                        crate::ProviderIndexMode::Legacy => p::ProviderIndexMode::Legacy,
                        crate::ProviderIndexMode::Federated => p::ProviderIndexMode::Federated,
                    } as i32,
                },
                owner: Arc::new(()),
            },
            work,
            1024,
        )
    }
    async fn open_scope(
        &self,
        request: Request<p::QueryOpenScopeRequest>,
    ) -> Result<Response<p::QueryScopeReply>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        let v = self
            .query_provider(&i.store_id)?
            .open_scope(
                OpenQueryScope {
                    roots: commits(i.roots)?,
                    overlay: i.overlay,
                    replay_key: handle(&i.replay_key)?,
                },
                w.clone(),
            )
            .await
            .map_err(crate::query::status)?;
        reply(
            QueryReply {
                value: p::QueryScopeReply {
                    handle: v.value.handle.to_vec(),
                    expires_at_unix_seconds: v.value.expires_at_unix_seconds,
                },
                owner: v.owner,
            },
            w,
            self.query_limit(),
        )
    }
    async fn fork_scope(
        &self,
        request: Request<p::QueryForkScopeRequest>,
    ) -> Result<Response<p::QueryScopeReply>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        let v = self
            .query_provider(&i.store_id)?
            .fork_scope(
                ForkQueryScope {
                    parent: handle(&i.parent)?,
                    overlay: i.overlay,
                    replay_key: handle(&i.replay_key)?,
                },
                w.clone(),
            )
            .await
            .map_err(crate::query::status)?;
        reply(
            QueryReply {
                value: p::QueryScopeReply {
                    handle: v.value.handle.to_vec(),
                    expires_at_unix_seconds: v.value.expires_at_unix_seconds,
                },
                owner: v.owner,
            },
            w,
            self.query_limit(),
        )
    }
    async fn facts(
        &self,
        request: Request<p::QueryFactsRequest>,
    ) -> Result<Response<q::GraphFacts>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        fact_shapes(&i.facts, self.query_limit())?;
        let v = self
            .query_provider(&i.store_id)?
            .facts(handle(&i.scope)?, i.facts, w.clone())
            .await
            .map_err(crate::query::status)?;
        reply(v, w, self.query_limit())
    }
    async fn resolve_commit_prefix(
        &self,
        request: Request<p::QueryCommitPrefixRequest>,
    ) -> Result<Response<q::CommitPrefixResult>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        prefix(&i.hex_prefix, 64)?;
        let v = self
            .query_provider(&i.store_id)?
            .resolve_commit_prefix(handle(&i.scope)?, i.hex_prefix, w.clone())
            .await
            .map_err(crate::query::status)?;
        reply(v, w, self.query_limit())
    }
    async fn resolve_change_prefix(
        &self,
        request: Request<p::QueryChangePrefixRequest>,
    ) -> Result<Response<q::ChangePrefixResult>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        prefix(&i.hex_prefix, 32)?;
        let v = self
            .query_provider(&i.store_id)?
            .resolve_change_prefix(
                handle(&i.scope)?,
                i.hex_prefix,
                commits(i.visible_heads)?,
                w.clone(),
            )
            .await
            .map_err(crate::query::status)?;
        reply(v, w, self.query_limit())
    }
    async fn shortest_prefix(
        &self,
        request: Request<p::QueryShortestPrefixRequest>,
    ) -> Result<Response<q::ShortestPrefixResult>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        let target = match i
            .target
            .ok_or_else(|| Status::invalid_argument("missing native query prefix target"))?
        {
            p::query_shortest_prefix_request::Target::Commit(v) => {
                q::shortest_prefix_request::Target::Commit(q::CommitId {
                    value: handle(&v)?.to_vec(),
                })
            }
            p::query_shortest_prefix_request::Target::Change(v) => {
                if v.len() != 16 {
                    return Err(Status::invalid_argument("change ID must be 16 bytes"));
                }
                q::shortest_prefix_request::Target::Change(q::ChangeId { value: v })
            }
        };
        let v = self
            .query_provider(&i.store_id)?
            .shortest_prefix(
                handle(&i.scope)?,
                target,
                commits(i.visible_heads)?,
                w.clone(),
            )
            .await
            .map_err(crate::query::status)?;
        reply(v, w, self.query_limit())
    }
    async fn begin(
        &self,
        request: Request<p::QueryBeginRequest>,
    ) -> Result<Response<p::QueryHandleReply>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        expression_shape(
            i.expression
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing native query expression"))?,
            self.query_limit(),
        )?;
        let projection = q::Projection::try_from(i.projection)
            .map_err(|_| Status::invalid_argument("invalid native query projection"))?;
        if projection == q::Projection::Unspecified || i.page_size == 0 {
            return Err(Status::invalid_argument(
                "invalid native query projection or page size",
            ));
        }
        let v = self
            .query_provider(&i.store_id)?
            .begin(
                BeginQuery {
                    scope: handle(&i.scope)?,
                    expression: i.expression.ok_or_else(|| {
                        Status::invalid_argument("missing native query expression")
                    })?,
                    projection,
                    page_size: i.page_size,
                    replay_key: handle(&i.replay_key)?,
                },
                w.clone(),
            )
            .await
            .map_err(crate::query::status)?;
        reply(
            QueryReply {
                value: p::QueryHandleReply {
                    handle: v.value.handle.to_vec(),
                    expires_at_unix_seconds: v.value.expires_at_unix_seconds,
                },
                owner: v.owner,
            },
            w,
            self.query_limit(),
        )
    }
    async fn page(
        &self,
        request: Request<p::QueryPageRequest>,
    ) -> Result<Response<p::QueryPageReply>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        let v = self
            .query_provider(&i.store_id)?
            .page(handle(&i.query)?, i.expected_sequence, w.clone())
            .await
            .map_err(crate::query::status)?;
        if v.value.sequence != i.expected_sequence {
            return Err(Status::data_loss("native query reply sequence mismatch"));
        }
        reply(
            QueryReply {
                value: p::QueryPageReply {
                    sequence: v.value.sequence,
                    rows: v.value.rows,
                    exhausted: Some(v.value.exhausted),
                },
                owner: v.owner,
            },
            w,
            self.query_limit(),
        )
    }
    async fn read_commit(
        &self,
        request: Request<p::QueryReadCommitRequest>,
    ) -> Result<Response<p::Metadata>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        let id = handle(&i.commit)?;
        let v = self
            .query_provider(&i.store_id)?
            .read_commit(handle(&i.scope)?, tinytree_model::CommitId(id), w.clone())
            .await
            .map_err(crate::query::status)?;
        if v.value.descriptor().key.kind != tinytree_model::Kind::Commit
            || v.value.descriptor().key.id.0 != id
        {
            return Err(Status::data_loss("native query commit mismatch"));
        }
        if v.value.bytes().len().saturating_add(128) > self.query_limit() {
            return Err(Status::resource_exhausted(
                "native query canonical commit exceeds reply limit",
            ));
        }
        reply(
            QueryReply {
                value: p::Metadata {
                    descriptor: Some(wire::describe(v.value.descriptor())),
                    canonical: v.value.bytes().to_vec(),
                },
                owner: v.owner,
            },
            w,
            self.query_limit(),
        )
    }
    async fn close_query(
        &self,
        request: Request<p::QueryCloseRequest>,
    ) -> Result<Response<p::QueryCloseReply>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        self.query_provider(&i.store_id)?
            .close_query(handle(&i.handle)?, w.clone())
            .await
            .map_err(crate::query::status)?;
        reply(
            QueryReply {
                value: p::QueryCloseReply {},
                owner: Arc::new(()),
            },
            w,
            self.query_limit(),
        )
    }
    async fn close_scope(
        &self,
        request: Request<p::QueryCloseRequest>,
    ) -> Result<Response<p::QueryCloseReply>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        self.query_provider(&i.store_id)?
            .close_scope(handle(&i.handle)?, w.clone())
            .await
            .map_err(crate::query::status)?;
        reply(
            QueryReply {
                value: p::QueryCloseReply {},
                owner: Arc::new(()),
            },
            w,
            self.query_limit(),
        )
    }
}

/// In-process facade; persistent returned page owners never capture the call
/// slot. The detached accepted job owns the slot until provider completion.
pub(crate) struct GuardedQuery {
    service: BrokerService,
    provider: Arc<dyn NativeQueryIo>,
}
impl std::fmt::Debug for GuardedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuardedQuery").finish_non_exhaustive()
    }
}
impl GuardedQuery {
    pub(crate) fn new(service: BrokerService, provider: Arc<dyn NativeQueryIo>) -> Self {
        Self { service, provider }
    }
    async fn run<T: Send + 'static, F, Fut>(
        &self,
        owner: QueryOwner,
        operation: F,
    ) -> Result<QueryReply<T>, Status>
    where
        F: FnOnce(Arc<dyn NativeQueryIo>, QueryOwner) -> Fut + Send + 'static,
        Fut: Future<Output = Result<QueryReply<T>, Status>> + Send + 'static,
        T: WireSize,
    {
        let service = self.service.clone();
        let provider = self.provider.clone();
        crate::client::run_control(async move {
            let deadline = service.operation_deadline();
            let work = service
                .admit_local("/native.broker.v1.QueryIo/Local", deadline)
                .await
                .map_err(crate::query::status)?;
            let cap = service.query_limit();
            // Only spawn after admission: cancelled waiters cannot leave an
            // unbounded queue of detached jobs waiting for the shared pool.
            let accepted = tokio::spawn(async move {
                let _work = work.clone();
                let physical: QueryOwner = Arc::new((owner, work));
                let reply = operation(provider, physical)
                    .await
                    .map_err(crate::query::status)?;
                if reply.value.wire_size() > cap {
                    return Err(Status::resource_exhausted(
                        "native query reply exceeds limit",
                    ));
                }
                Ok(reply)
            });
            tokio::time::timeout_at(deadline, accepted)
                .await
                .map_err(|_| Status::deadline_exceeded("native query operation deadline exceeded"))?
                .map_err(|_| Status::internal("native query provider job failed"))?
        })
        .await
    }
}
trait WireSize {
    fn wire_size(&self) -> usize;
}
macro_rules! message_size {($($t:ty),*)=>{$(impl WireSize for $t { fn wire_size(&self)->usize {self.encoded_len()} })*};}
message_size!(
    q::GraphFacts,
    q::CommitPrefixResult,
    q::ChangePrefixResult,
    q::ShortestPrefixResult
);
impl WireSize for QueryScope {
    fn wire_size(&self) -> usize {
        64
    }
}
impl WireSize for QueryHandle {
    fn wire_size(&self) -> usize {
        64
    }
}
impl WireSize for QueryPage {
    fn wire_size(&self) -> usize {
        self.rows.iter().fold(32usize, |n, r| {
            n.saturating_add(r.encoded_len()).saturating_add(8)
        })
    }
}
impl WireSize for jj_native_backend::VerifiedMetadata {
    fn wire_size(&self) -> usize {
        self.bytes().len().saturating_add(128)
    }
}
impl WireSize for () {
    fn wire_size(&self) -> usize {
        0
    }
}
#[async_trait::async_trait]
impl NativeQueryIo for GuardedQuery {
    fn resources(&self) -> QueryResources {
        self.provider.resources()
    }
    async fn open_scope(
        &self,
        i: OpenQueryScope,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, Status> {
        bounded_input(
            overlay_bytes(&i.overlay, self.service.query_limit())?
                .saturating_add(visible_bytes(&i.roots, self.service.query_limit())?)
                .saturating_add(128),
            self.service.query_limit(),
        )?;
        self.run(o, move |p, o| async move { p.open_scope(i, o).await })
            .await
    }
    async fn fork_scope(
        &self,
        i: ForkQueryScope,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, Status> {
        bounded_input(
            overlay_bytes(&i.overlay, self.service.query_limit())?.saturating_add(128),
            self.service.query_limit(),
        )?;
        self.run(o, move |p, o| async move { p.fork_scope(i, o).await })
            .await
    }
    async fn facts(
        &self,
        s: [u8; 32],
        f: Vec<q::GraphFact>,
        o: QueryOwner,
    ) -> Result<QueryReply<q::GraphFacts>, Status> {
        fact_shapes(&f, self.service.query_limit())?;
        bounded_input(
            f.iter().fold(128usize, |n, v| {
                n.saturating_add(v.encoded_len()).saturating_add(8)
            }),
            self.service.query_limit(),
        )?;
        self.run(o, move |p, o| async move { p.facts(s, f, o).await })
            .await
    }
    async fn resolve_commit_prefix(
        &self,
        s: [u8; 32],
        f: String,
        o: QueryOwner,
    ) -> Result<QueryReply<q::CommitPrefixResult>, Status> {
        prefix(&f, 64)?;
        self.run(o, move |p, o| async move {
            p.resolve_commit_prefix(s, f, o).await
        })
        .await
    }
    async fn resolve_change_prefix(
        &self,
        s: [u8; 32],
        f: String,
        h: Vec<tinytree_model::CommitId>,
        o: QueryOwner,
    ) -> Result<QueryReply<q::ChangePrefixResult>, Status> {
        prefix(&f, 32)?;
        bounded_input(
            visible_bytes(&h, self.service.query_limit())?
                .saturating_add(f.len())
                .saturating_add(128),
            self.service.query_limit(),
        )?;
        self.run(o, move |p, o| async move {
            p.resolve_change_prefix(s, f, h, o).await
        })
        .await
    }
    async fn shortest_prefix(
        &self,
        s: [u8; 32],
        t: q::shortest_prefix_request::Target,
        h: Vec<tinytree_model::CommitId>,
        o: QueryOwner,
    ) -> Result<QueryReply<q::ShortestPrefixResult>, Status> {
        bounded_input(
            visible_bytes(&h, self.service.query_limit())?
                .saturating_add(target_bytes(&t)?)
                .saturating_add(128),
            self.service.query_limit(),
        )?;
        self.run(
            o,
            move |p, o| async move { p.shortest_prefix(s, t, h, o).await },
        )
        .await
    }
    async fn begin(&self, i: BeginQuery, o: QueryOwner) -> Result<QueryReply<QueryHandle>, Status> {
        expression_shape(&i.expression, self.service.query_limit())?;
        bounded_input(
            i.expression.encoded_len().saturating_add(128),
            self.service.query_limit(),
        )?;
        if i.projection == q::Projection::Unspecified || i.page_size == 0 {
            return Err(Status::invalid_argument(
                "invalid native query projection or page size",
            ));
        }
        self.run(o, move |p, o| async move { p.begin(i, o).await })
            .await
    }
    async fn page(
        &self,
        q: [u8; 32],
        s: u64,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryPage>, Status> {
        self.run(o, move |p, o| async move {
            let r = p.page(q, s, o).await.map_err(crate::query::status)?;
            if r.value.sequence != s {
                return Err(Status::data_loss("native query reply sequence mismatch"));
            }
            Ok(r)
        })
        .await
    }
    async fn read_commit(
        &self,
        s: [u8; 32],
        c: tinytree_model::CommitId,
        o: QueryOwner,
    ) -> Result<QueryReply<jj_native_backend::VerifiedMetadata>, Status> {
        self.run(o, move |p, o| async move {
            let r = p.read_commit(s, c, o).await.map_err(crate::query::status)?;
            if r.value.descriptor().key.kind != tinytree_model::Kind::Commit
                || r.value.descriptor().key.id.0 != c.0
            {
                return Err(Status::data_loss("native query commit mismatch"));
            }
            Ok(r)
        })
        .await
    }
    async fn close_query(&self, q: [u8; 32], o: QueryOwner) -> Result<(), Status> {
        self.run(o, move |p, o| async move {
            p.close_query(q, o).await.map_err(crate::query::status)?;
            Ok(QueryReply {
                value: (),
                owner: Arc::new(()),
            })
        })
        .await
        .map(|_| ())
    }
    async fn close_scope(&self, s: [u8; 32], o: QueryOwner) -> Result<(), Status> {
        self.run(o, move |p, o| async move {
            p.close_scope(s, o).await.map_err(crate::query::status)?;
            Ok(QueryReply {
                value: (),
                owner: Arc::new(()),
            })
        })
        .await
        .map(|_| ())
    }
}
