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
pub(crate) fn validate_scope_source(
    scope: &QueryScope,
    expected: Option<[u8; 32]>,
) -> Result<(), Status> {
    if scope.cloud_source_digest != expected
        || (expected.is_some() && scope.query_identity_version != 2)
    {
        return Err(Status::data_loss(
            "native cloud query source negotiation failed",
        ));
    }
    Ok(())
}
fn cloud_source(value: p::NativeCloudQuerySource, cap: usize) -> Result<CloudQuerySource, Status> {
    if value.index_root_ids.len() > 4096 || value.expected_anchors.len() > 4096 {
        return Err(Status::resource_exhausted(
            "native cloud query source count exceeded",
        ));
    }
    let source = CloudQuerySource {
        provider_identity: handle(&value.provider_identity)?,
        workspace_id: handle(&value.workspace_id)?,
        generation: value.generation,
        manifest_id: handle(&value.manifest_id)?,
        manifest_blake3: handle(&value.manifest_blake3)?,
        manifest_size_bytes: value.manifest_size_bytes,
        index_root_ids: value
            .index_root_ids
            .into_iter()
            .map(|id| handle(&id))
            .collect::<Result<_, _>>()?,
        expected_anchors: commits(value.expected_anchors)?,
    };
    source.validate(cap)?;
    Ok(source)
}
fn commits(values: Vec<Vec<u8>>) -> Result<Vec<tinytree_model::CommitId>, Status> {
    values
        .into_iter()
        .map(|v| handle(&v).map(tinytree_model::CommitId))
        .collect()
}
fn path_query(value: p::QueryPathRequest, cap: usize) -> Result<ScopedPathQuery, Status> {
    if value.annotation_limits.is_some() {
        return Err(Status::invalid_argument(
            "annotation limits are only supported for blame",
        ));
    }
    let input = ScopedPathQuery {
        scope: handle(&value.scope)?,
        revision: tinytree_model::CommitId(handle(&value.revision)?),
        path: value.path,
        policy: q::HistoryPolicy::try_from(value.policy)
            .map_err(|_| Status::unimplemented("native history policy unsupported"))?,
        max_commits: value.max_commits,
    };
    input.validate(cap)?;
    Ok(input)
}
fn search_query(value: p::QuerySearchRequest, cap: usize) -> Result<ScopedSearchQuery, Status> {
    let input = ScopedSearchQuery {
        scope: handle(&value.scope)?,
        revision: tinytree_model::CommitId(handle(&value.revision)?),
        pattern: value.pattern,
        path_prefix: value.path_prefix,
        policy: q::SearchPolicy::try_from(value.policy)
            .map_err(|_| Status::unimplemented("native search policy unsupported"))?,
        page_size: value.page_size,
        cursor: value.cursor,
    };
    input.validate(cap)?;
    Ok(input)
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
            q::graph_fact::Fact::Membership(v) => {
                id(v.candidate.as_ref())?;
                let expression = v.expression.as_ref().ok_or_else(|| {
                    Status::invalid_argument("missing native query membership expression")
                })?;
                expression_shape(expression, cap)?;
                5
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
            E::ChangedPath(v) => {
                if v.policy_version != 1
                    || v.path.is_empty()
                    || v.path.len() > 4096
                    || v.path.contains(['\\', '\0'])
                    || v.path.split('/').any(|c| matches!(c, "" | "." | ".."))
                {
                    return Err(Status::invalid_argument("invalid exact-path graph filter"));
                }
                child!(v.candidates.as_deref());
            }
            E::Heads(v) | E::Roots(v) | E::HasConflict(v) => child!(v.input.as_deref()),
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
        if matches!(
            request.uri().path(),
            "/native.broker.v1.QueryIo/DescribeProvider"
                | "/native.broker.v1.QueryIo/PreflightCreation"
        ) {
            self.describe.call(request)
        } else {
            self.inner.call(request)
        }
    }
}
#[tonic::async_trait]
impl p::query_io_server::QueryIo for BrokerService {
    async fn blame_capabilities(&self, request: Request<p::DescribeProviderRequest>)
        -> Result<Response<p::BlameCapabilitiesReply>, Status> {
        let (input, work) = input(request, self.query_limit())?;
        self.authorize_command(&input.store_id, &work)?;
        let result = self.query_provider(&input.store_id)?.blame_capabilities(work.clone()).await
            .map_err(crate::query::status)?;
        reply(QueryReply { value: result.value.wire(), owner: result.owner }, work, self.query_limit())
    }
    async fn preflight_creation(
        &self,
        request: Request<p::CreationRequest>,
    ) -> Result<Response<p::CreationPlan>, Status> {
        let (input, work) = input(request, 1024)?;
        self.authorize_command(&input.store_id, &work)?;
        let root = handle(&input.root)?;
        let plan = self
            .describe_provider(&input.store_id)?
            .creation_plan(root)?;
        let query = plan.index_mode == crate::ProviderIndexMode::Federated;
        if query || plan.policy.upload_enabled {
            let checked = self
                .query_provider(&input.store_id)?
                .preflight_creation(
                    tinytree_model::CommitId(root),
                    query,
                    plan.policy.upload_enabled,
                    plan.policy.auto_upload,
                    work.deadline,
                    work.clone(),
                )
                .await?;
            drop(checked);
        }
        reply(
            QueryReply {
                value: plan.wire(),
                owner: Arc::new(()),
            },
            work,
            1024,
        )
    }

    async fn describe_provider(
        &self,
        request: Request<p::DescribeProviderRequest>,
    ) -> Result<Response<p::ProviderDescription>, Status> {
        let (input, work) = input(request, 1024)?;
        self.authorize_command(&input.store_id, &work)?;
        let description = self.describe_provider(&input.store_id)?;
        reply(
            QueryReply {
                value: p::ProviderDescription {
                    store_id: input.store_id,
                    identity: description.identity.to_vec(),
                    query_capable: Some(description.query_capable),
                    creation_policy: description.creation_policy.map(CreationPolicy::wire),
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
    async fn open_cloud_scope(
        &self,
        request: Request<p::QueryOpenCloudScopeRequest>,
    ) -> Result<Response<p::QueryScopeReply>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        self.authorize_command(&i.store_id, &w)?;
        let source = cloud_source(
            i.source
                .ok_or_else(|| Status::invalid_argument("missing native cloud query source"))?,
            self.query_limit(),
        )?;
        if self.describe_provider(&i.store_id)?.identity != source.provider_identity {
            return Err(Status::failed_precondition(
                "native cloud query provider identity changed",
            ));
        }
        overlay_bytes(&i.overlay, self.query_limit())?;
        let expected = source.digest();
        let value = self
            .query_provider(&i.store_id)?
            .open_cloud_scope(
                OpenCloudQueryScope {
                    source,
                    overlay: i.overlay,
                    replay_key: handle(&i.replay_key)?,
                },
                w.clone(),
            )
            .await
            .map_err(crate::query::status)?;
        validate_scope_source(&value.value, Some(expected))?;
        reply(
            QueryReply {
                value: p::QueryScopeReply {
                    handle: value.value.handle.to_vec(),
                    expires_at_unix_seconds: value.value.expires_at_unix_seconds,
                    query_identity_version: value.value.query_identity_version,
                    cloud_source_digest: value
                        .value
                        .cloud_source_digest
                        .map(|digest| digest.to_vec()),
                },
                owner: value.owner,
            },
            w,
            self.query_limit(),
        )
    }
    async fn open_scope(
        &self,
        request: Request<p::QueryOpenScopeRequest>,
    ) -> Result<Response<p::QueryScopeReply>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        self.authorize_command(&i.store_id, &w)?;
        let v = self
            .query_provider(&i.store_id)?
            .open_scope(
                OpenQueryScope {
                    roots: commits(i.roots)?,
                    query_identity_version: crate::normalize_query_identity_version(
                        i.query_identity_version,
                    )?,
                    overlay: i.overlay,
                    replay_key: handle(&i.replay_key)?,
                },
                w.clone(),
            )
            .await
            .map_err(crate::query::status)?;
        validate_scope_source(&v.value, None)?;
        reply(
            QueryReply {
                value: p::QueryScopeReply {
                    handle: v.value.handle.to_vec(),
                    expires_at_unix_seconds: v.value.expires_at_unix_seconds,
                    query_identity_version: v.value.query_identity_version,
                    cloud_source_digest: v.value.cloud_source_digest.map(|digest| digest.to_vec()),
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
        self.authorize_command(&i.store_id, &w)?;
        let expected = i
            .expected_cloud_source_digest
            .as_deref()
            .map(handle)
            .transpose()?;
        let v = self
            .query_provider(&i.store_id)?
            .fork_scope(
                ForkQueryScope {
                    parent: handle(&i.parent)?,
                    expected_cloud_source_digest: expected,
                    overlay: i.overlay,
                    replay_key: handle(&i.replay_key)?,
                },
                w.clone(),
            )
            .await
            .map_err(crate::query::status)?;
        validate_scope_source(&v.value, expected)?;
        reply(
            QueryReply {
                value: p::QueryScopeReply {
                    handle: v.value.handle.to_vec(),
                    expires_at_unix_seconds: v.value.expires_at_unix_seconds,
                    query_identity_version: v.value.query_identity_version,
                    cloud_source_digest: v.value.cloud_source_digest.map(|digest| digest.to_vec()),
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
        self.authorize_command(&i.store_id, &w)?;
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
        self.authorize_command(&i.store_id, &w)?;
        prefix(&i.hex_prefix, 64)?;
        let provider = self.query_provider(&i.store_id)?;
        let v = match i.selection {
            Some(selection) => {
                let digest = crate::prefix_selection_digest(&selection, self.query_limit())?;
                let v = provider
                    .resolve_commit_prefix_selected(
                        handle(&i.scope)?,
                        i.hex_prefix,
                        selection,
                        w.clone(),
                    )
                    .await
                    .map_err(crate::query::status)?;
                if v.value.selection_digest.as_deref() != Some(digest.as_slice()) {
                    return Err(Status::data_loss("selected prefix digest mismatch"));
                }
                v
            }
            None => provider
                .resolve_commit_prefix(handle(&i.scope)?, i.hex_prefix, w.clone())
                .await
                .map_err(crate::query::status)?,
        };
        reply(v, w, self.query_limit())
    }
    async fn resolve_change_prefix(
        &self,
        request: Request<p::QueryChangePrefixRequest>,
    ) -> Result<Response<q::ChangePrefixResult>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        self.authorize_command(&i.store_id, &w)?;
        prefix(&i.hex_prefix, 32)?;
        let provider = self.query_provider(&i.store_id)?;
        let v = match i.selection {
            Some(selection) => {
                let digest = crate::prefix_selection_digest(&selection, self.query_limit())?;
                let v = provider
                    .resolve_change_prefix_selected(
                        handle(&i.scope)?,
                        i.hex_prefix,
                        commits(i.visible_heads)?,
                        selection,
                        w.clone(),
                    )
                    .await
                    .map_err(crate::query::status)?;
                if v.value.selection_digest.as_deref() != Some(digest.as_slice()) {
                    return Err(Status::data_loss("selected prefix digest mismatch"));
                }
                v
            }
            None => provider
                .resolve_change_prefix(
                    handle(&i.scope)?,
                    i.hex_prefix,
                    commits(i.visible_heads)?,
                    w.clone(),
                )
                .await
                .map_err(crate::query::status)?,
        };
        reply(v, w, self.query_limit())
    }
    async fn shortest_prefix(
        &self,
        request: Request<p::QueryShortestPrefixRequest>,
    ) -> Result<Response<q::ShortestPrefixResult>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        self.authorize_command(&i.store_id, &w)?;
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
        let provider = self.query_provider(&i.store_id)?;
        let v = match i.selection {
            Some(selection) => {
                let digest = crate::prefix_selection_digest(&selection, self.query_limit())?;
                let v = provider
                    .shortest_prefix_selected(
                        handle(&i.scope)?,
                        target,
                        commits(i.visible_heads)?,
                        selection,
                        w.clone(),
                    )
                    .await
                    .map_err(crate::query::status)?;
                if v.value.selection_digest.as_deref() != Some(digest.as_slice())
                    || v.value.in_selection.is_none()
                {
                    return Err(Status::data_loss(
                        "selected shortest prefix binding missing",
                    ));
                }
                v
            }
            None => provider
                .shortest_prefix(
                    handle(&i.scope)?,
                    target,
                    commits(i.visible_heads)?,
                    w.clone(),
                )
                .await
                .map_err(crate::query::status)?,
        };
        reply(v, w, self.query_limit())
    }
    async fn begin(
        &self,
        request: Request<p::QueryBeginRequest>,
    ) -> Result<Response<p::QueryHandleReply>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        self.authorize_command(&i.store_id, &w)?;
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
        self.authorize_command(&i.store_id, &w)?;
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
        self.authorize_command(&i.store_id, &w)?;
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
    async fn annotation_capabilities(
        &self,
        request: Request<p::DescribeProviderRequest>,
    ) -> Result<Response<p::BlameCapabilitiesReply>, Status> {
        let (input, work) = input(request, self.query_limit())?;
        self.authorize_command(&input.store_id, &work)?;
        let result = self
            .query_provider(&input.store_id)?
            .annotation_capabilities(work.clone())
            .await
            .map_err(crate::query::status)?;
        reply(
            QueryReply {
                value: result.value.annotation_wire(),
                owner: result.owner,
            },
            work,
            self.query_limit(),
        )
    }
    async fn open_annotation_frontier(
        &self,
        request: Request<p::QueryOpenAnnotationRequest>,
    ) -> Result<Response<q::AnnotationFrontier>, Status> {
        let (input, work) = input(request, self.query_limit())?;
        self.authorize_command(&input.store_id, &work)?;
        let provider = self.query_provider(&input.store_id)?;
        let input = OpenAnnotationFrontier {
            scope: handle(&input.scope)?,
            anchors: commits(input.anchors)?,
            path: input.path,
            max_commits: input.max_commits,
                        limits: input.limits,
        };
        if input.limits.is_some(){return Err(Status::invalid_argument("lowered caps require limited frontier RPC"));}
        input.validate(self.query_limit())?;
        let result = provider
            .open_annotation_frontier(input.clone(), work.clone())
            .await
            .map_err(crate::query::status)?;
        input.check(&result.value, self.query_limit())?;
        reply(result, work, self.query_limit())
    }
    async fn open_limited_annotation_frontier(
        &self,
        request: Request<p::QueryOpenAnnotationRequest>,
    ) -> Result<Response<q::AnnotationFrontier>, Status> {
        let (input, work) = input(request, self.query_limit())?;
        self.authorize_command(&input.store_id, &work)?;
        let provider = self.query_provider(&input.store_id)?;
        let input = OpenAnnotationFrontier {
            scope: handle(&input.scope)?,
            anchors: commits(input.anchors)?,
            path: input.path,
            max_commits: input.max_commits,
                        limits: input.limits,
        };
        if input.limits.is_none(){return Err(Status::invalid_argument("limited frontier requires caps"));}
        input.validate(self.query_limit())?;
        let result = provider
            .open_limited_annotation_frontier(input.clone(), work.clone())
            .await
            .map_err(crate::query::status)?;
        input.check(&result.value, self.query_limit())?;
        reply(result, work, self.query_limit())
    }
    async fn read_annotation_input(
        &self,
        request: Request<p::QueryReadAnnotationRequest>,
    ) -> Result<Response<q::AnnotationInput>, Status> {
        let (input, work) = input(request, self.query_limit())?;
        self.authorize_command(&input.store_id, &work)?;
        let provider = self.query_provider(&input.store_id)?;
        let input = ReadAnnotationInput {
            scope: handle(&input.scope)?,
            context: handle(&input.context)?,
            revision: tinytree_model::CommitId(handle(&input.revision)?),
            expected: input
                .expected
                .ok_or_else(|| Status::invalid_argument("annotation expected state missing"))?,
        };
        input.validate(self.query_limit())?;
        let result = provider
            .read_annotation_input(input.clone(), work.clone())
            .await
            .map_err(crate::query::status)?;
        input.check(&result.value, self.query_config().model_limits())?;
        reply(result, work, self.query_limit())
    }
    async fn close_annotation_frontier(
        &self,
        request: Request<p::QueryCloseRequest>,
    ) -> Result<Response<p::QueryCloseReply>, Status> {
        let (input, work) = input(request, self.query_limit())?;
        self.authorize_command(&input.store_id, &work)?;
        let result = self
            .query_provider(&input.store_id)?
            .close_annotation_frontier(handle(&input.handle)?, work.clone())
            .await
            .map_err(crate::query::status)?;
        reply(
            QueryReply {
                value: p::QueryCloseReply {},
                owner: result.owner,
            },
            work,
            self.query_limit(),
        )
    }
    async fn path_history(
        &self,
        request: Request<p::QueryPathRequest>,
    ) -> Result<Response<q::PathHistoryResult>, Status> {
        let (input, work) = input(request, self.query_limit())?;
        self.authorize_command(&input.store_id, &work)?;
        let provider = self.query_provider(&input.store_id)?;
        let input = path_query(input, self.query_limit())?;
        let result = provider
            .path_history(input.clone(), work.clone())
            .await
            .map_err(crate::query::status)?;
        input.check_history(&result.value)?;
        reply(result, work, self.query_limit())
    }
    async fn blame(
        &self,
        request: Request<p::QueryPathRequest>,
    ) -> Result<Response<q::BlameResult>, Status> {
        let (raw, work) = input(request, self.query_limit())?;
        self.authorize_command(&raw.store_id, &work)?;
        let provider = self.query_provider(&raw.store_id)?;
        let input = path_query(raw, self.query_limit())?;
        let result = provider
            .blame(input.clone(), work.clone())
            .await
            .map_err(crate::query::status)?;
        input.check_blame(&result.value, self.query_config().model_limits())?;
        reply(result, work, self.query_limit())
    }
    async fn blame_with_limits(
        &self,
        request: Request<p::QueryPathRequest>,
    ) -> Result<Response<q::BlameResult>, Status> {
        let (mut raw, work) = input(request, self.query_limit())?;
        self.authorize_command(&raw.store_id, &work)?;
        let provider = self.query_provider(&raw.store_id)?;
        let limits = raw.annotation_limits.take().ok_or_else(|| {
            Status::invalid_argument("bounded blame requires explicit annotation limits")
        })?;
        if !crate::query::annotation_limits_valid(&limits) {
            return Err(Status::invalid_argument(
                "positive annotation limits required",
            ));
        }
        let input = path_query(raw, self.query_limit())?;
        let result = provider
            .blame_with_limits(input.clone(), limits, work.clone())
            .await
            .map_err(crate::query::status)?;
        input.check_blame(&result.value, self.query_config().model_limits())?;
        reply(result, work, self.query_limit())
    }
    async fn search(
        &self,
        request: Request<p::QuerySearchRequest>,
    ) -> Result<Response<q::SearchResult>, Status> {
        let (input, work) = input(request, self.query_limit())?;
        self.authorize_command(&input.store_id, &work)?;
        let provider = self.query_provider(&input.store_id)?;
        let input = search_query(input, self.query_limit())?;
        let result = provider
            .search(input.clone(), work.clone())
            .await
            .map_err(crate::query::status)?;
        input.check_result(&result.value)?;
        reply(result, work, self.query_limit())
    }
    async fn close_query(
        &self,
        request: Request<p::QueryCloseRequest>,
    ) -> Result<Response<p::QueryCloseReply>, Status> {
        let (i, w) = input(request, self.query_limit())?;
        self.authorize_command(&i.store_id, &w)?;
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
        self.authorize_command(&i.store_id, &w)?;
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
// A cancelled waiter can discard a completed detached output. Destroy its page
// payload/retention owners before releasing physical host and pool ownership.
// Never attach these transport owners to the persistent QueryReply.owner.
struct OwnedQueryResult<T> {
    result: Result<QueryReply<T>, Status>,
    _work: Arc<Work>,
}
pub(crate) struct GuardedQuery {
    service: BrokerService,
    provider: Arc<dyn NativeQueryIo>,
    alias: String,
}
impl std::fmt::Debug for GuardedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuardedQuery").finish_non_exhaustive()
    }
}
impl GuardedQuery {
    pub(crate) fn new(
        service: BrokerService,
        provider: Arc<dyn NativeQueryIo>,
        alias: String,
    ) -> Self {
        Self {
            service,
            provider,
            alias,
        }
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
        let deadline = service.operation_deadline();
        crate::client::run_control(async move {
            let work = service
                .admit_local("/native.broker.v1.QueryIo/Local", deadline)
                .await
                .map_err(crate::query::status)?;
            let cap = service.query_limit();
            // Only spawn after admission: cancelled waiters cannot leave an
            // unbounded queue of detached jobs waiting for the shared pool.
            let accepted = tokio::spawn(async move {
                let physical: QueryOwner = Arc::new((owner, work.clone()));
                let result = async move {
                    let reply = operation(provider, physical)
                        .await
                        .map_err(crate::query::status)?;
                    if reply.value.wire_size() > cap {
                        return Err(Status::resource_exhausted(
                            "native query reply exceeds limit",
                        ));
                    }
                    Ok(reply)
                }
                .await;
                OwnedQueryResult {
                    result,
                    _work: work,
                }
            });
            let result = tokio::time::timeout_at(deadline, accepted)
                .await
                .map_err(|_| Status::deadline_exceeded("native query operation deadline exceeded"))?
                .map_err(|_| Status::internal("native query provider job failed"))?;
            result.result
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
    q::ShortestPrefixResult,
    q::PathHistoryResult,
    q::BlameResult,
    q::SearchResult,
    q::AnnotationFrontier,
    q::AnnotationInput
);
impl WireSize for BlameCapabilities {
    fn wire_size(&self) -> usize { self.wire().encoded_len() }
}
impl WireSize for QueryScope {
    fn wire_size(&self) -> usize {
        128
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
    async fn blame_capabilities(&self, owner: QueryOwner) -> Result<QueryReply<BlameCapabilities>, Status> {
        self.run(owner, move |provider, owner| async move { provider.blame_capabilities(owner).await }).await
    }
    fn resources(&self) -> QueryResources {
        self.provider.resources()
    }
    async fn open_cloud_scope(
        &self,
        i: OpenCloudQueryScope,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, Status> {
        let cap = self.service.query_limit();
        bounded_input(
            i.source
                .validate(cap)?
                .saturating_add(overlay_bytes(&i.overlay, cap)?)
                .saturating_add(128),
            cap,
        )?;
        if self.service.describe_provider(&self.alias)?.identity != i.source.provider_identity {
            return Err(Status::failed_precondition(
                "native cloud query provider identity changed",
            ));
        }
        let expected = i.source.digest();
        let reply = self
            .run(o, move |provider, owner| async move {
                provider.open_cloud_scope(i, owner).await
            })
            .await?;
        validate_scope_source(&reply.value, Some(expected))?;
        Ok(reply)
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
        let reply = self
            .run(o, move |p, o| async move { p.open_scope(i, o).await })
            .await?;
        validate_scope_source(&reply.value, None)?;
        Ok(reply)
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
        let expected = i.expected_cloud_source_digest;
        let reply = self
            .run(o, move |p, o| async move { p.fork_scope(i, o).await })
            .await?;
        validate_scope_source(&reply.value, expected)?;
        Ok(reply)
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
    async fn resolve_commit_prefix_selected(
        &self,
        s: [u8; 32],
        f: String,
        selection: q::GraphExpr,
        o: QueryOwner,
    ) -> Result<QueryReply<q::CommitPrefixResult>, Status> {
        crate::validate_prefix_selection(&selection, self.service.query_limit())?;
        bounded_input(
            selection
                .encoded_len()
                .saturating_add(f.len())
                .saturating_add(128)
                .saturating_add(field_bytes(selection.encoded_len())),
            self.service.query_limit(),
        )?;
        prefix(&f, 64)?;
        self.run(o, move |p, o| async move {
            p.resolve_commit_prefix_selected(s, f, selection, o).await
        })
        .await
    }
    async fn resolve_change_prefix_selected(
        &self,
        s: [u8; 32],
        f: String,
        h: Vec<tinytree_model::CommitId>,
        selection: q::GraphExpr,
        o: QueryOwner,
    ) -> Result<QueryReply<q::ChangePrefixResult>, Status> {
        crate::validate_prefix_selection(&selection, self.service.query_limit())?;
        bounded_input(
            selection
                .encoded_len()
                .saturating_add(f.len())
                .saturating_add(128)
                .saturating_add(field_bytes(selection.encoded_len())),
            self.service.query_limit(),
        )?;
        prefix(&f, 32)?;
        bounded_input(
            visible_bytes(&h, self.service.query_limit())?
                .saturating_add(f.len())
                .saturating_add(128)
                .saturating_add(field_bytes(selection.encoded_len())),
            self.service.query_limit(),
        )?;
        self.run(o, move |p, o| async move {
            p.resolve_change_prefix_selected(s, f, h, selection, o)
                .await
        })
        .await
    }
    async fn shortest_prefix_selected(
        &self,
        s: [u8; 32],
        t: q::shortest_prefix_request::Target,
        h: Vec<tinytree_model::CommitId>,
        selection: q::GraphExpr,
        o: QueryOwner,
    ) -> Result<QueryReply<q::ShortestPrefixResult>, Status> {
        crate::validate_prefix_selection(&selection, self.service.query_limit())?;
        bounded_input(
            visible_bytes(&h, self.service.query_limit())?
                .saturating_add(target_bytes(&t)?)
                .saturating_add(128)
                .saturating_add(field_bytes(selection.encoded_len())),
            self.service.query_limit(),
        )?;
        self.run(o, move |p, o| async move {
            p.shortest_prefix_selected(s, t, h, selection, o).await
        })
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
    async fn annotation_capabilities(
        &self,
        owner: QueryOwner,
    ) -> Result<QueryReply<BlameCapabilities>, Status> {
        self.run(owner, move |provider, owner| async move {
            provider.annotation_capabilities(owner).await
        })
        .await
    }
    async fn open_annotation_frontier(
        &self,
        input: OpenAnnotationFrontier,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::AnnotationFrontier>, Status> {
        if input.limits.is_some(){return self.open_limited_annotation_frontier(input,owner).await;}
        input.validate(self.service.query_limit())?;
        let cap = self.service.query_limit();
        self.run(owner, move |provider, owner| async move {
            let result = provider
                .open_annotation_frontier(input.clone(), owner)
                .await?;
            input.check(&result.value, cap)?;
            Ok(result)
        })
        .await
    }
    async fn open_limited_annotation_frontier(
        &self,
        input: OpenAnnotationFrontier,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::AnnotationFrontier>, Status> {
        if input.limits.is_none(){return Err(Status::invalid_argument("limited frontier requires caps"));}
        input.validate(self.service.query_limit())?;
        let cap = self.service.query_limit();
        self.run(owner, move |provider, owner| async move {
            let result = provider
                .open_limited_annotation_frontier(input.clone(), owner)
                .await?;
            input.check(&result.value, cap)?;
            Ok(result)
        })
        .await
    }
    async fn read_annotation_input(
        &self,
        input: ReadAnnotationInput,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::AnnotationInput>, Status> {
        input.validate(self.service.query_limit())?;
        let limits = self.service.query_config().model_limits();
        self.run(owner, move |provider, owner| async move {
            let result = provider.read_annotation_input(input.clone(), owner).await?;
            input.check(&result.value, limits)?;
            Ok(result)
        })
        .await
    }
    async fn close_annotation_frontier(
        &self,
        scope: [u8; 32],
        owner: QueryOwner,
    ) -> Result<QueryReply<()>, Status> {
        self.run(owner, move |provider, owner| async move {
            provider.close_annotation_frontier(scope, owner).await
        })
        .await
    }
    async fn path_history(
        &self,
        input: ScopedPathQuery,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::PathHistoryResult>, Status> {
        input.validate(self.service.query_limit())?;
        self.run(owner, move |provider, owner| async move {
            let result = provider.path_history(input.clone(), owner).await?;
            input.check_history(&result.value)?;
            Ok(result)
        })
        .await
    }
    async fn blame(
        &self,
        input: ScopedPathQuery,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::BlameResult>, Status> {
        input.validate(self.service.query_limit())?;
        let limits = self.service.query_config().model_limits();
        self.run(owner, move |provider, owner| async move {
            let result = provider.blame(input.clone(), owner).await?;
            input.check_blame(&result.value, limits)?;
            Ok(result)
        })
        .await
    }
    async fn blame_with_limits(
        &self,
        input: ScopedPathQuery,
        annotation_limits: q::AnnotationLimits,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::BlameResult>, Status> {
        input.validate(self.service.query_limit())?;
        if !crate::query::annotation_limits_valid(&annotation_limits) {
            return Err(Status::invalid_argument(
                "positive annotation limits required",
            ));
        }
        let limits = self.service.query_config().model_limits();
        self.run(owner, move |provider, owner| async move {
            let result = provider
                .blame_with_limits(input.clone(), annotation_limits, owner)
                .await?;
            input.check_blame(&result.value, limits)?;
            Ok(result)
        })
        .await
    }
    async fn search(
        &self,
        input: ScopedSearchQuery,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::SearchResult>, Status> {
        input.validate(self.service.query_limit())?;
        self.run(owner, move |provider, owner| async move {
            let result = provider.search(input.clone(), owner).await?;
            input.check_result(&result.value)?;
            Ok(result)
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
