//! Authenticated immutable remote and private-draft query scopes.
//! Evaluation remains explicit unsupported work.
use crate::{
    RepositoryService,
    auth::Identity,
    query_index::{QueryConfig, QueryHost, Scoped, Work, index_error},
};
use prost::Message;
use q::repository_query_server::{RepositoryQuery, RepositoryQueryServer};
use std::sync::Arc;
use tinytree_index::{PrefixResolution, WorkBudget};
use tinytree_model as m;
use tinytree_proto::query_v1 as q;
use tokio::time::Instant;
use tonic::{Request, Response, Status, Streaming, codec::CompressionEncoding};
#[derive(Clone)]
pub struct QueryService {
    repository: RepositoryService,
}
impl QueryService {
    pub(crate) fn new(repository: RepositoryService) -> Self {
        Self { repository }
    }
    pub(crate) fn grpc(self) -> RepositoryQueryServer<Self> {
        let config = self.repository.service_config();
        RepositoryQueryServer::new(self)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(config.max_request_bytes.min(config.query.max_request_bytes))
            .max_encoding_message_size(config.query.max_response_bytes)
    }
    fn context<T>(
        &self,
        request: &Request<T>,
        streaming: bool,
    ) -> Result<(Identity, Instant, Arc<QueryHost>), Status> {
        let identity = self.repository.query_identity(request.metadata())?;
        let mut deadline = self
            .repository
            .query_deadline(request.metadata(), streaming)?;
        if let Some(work) = request.extensions().get::<Arc<Work>>() {
            deadline = deadline.min(work.deadline);
        }
        let host = self
            .repository
            .query_host()
            .ok_or_else(|| Status::unavailable("derived queries are not configured"))?;
        Ok((identity, deadline, host))
    }
    fn bounded<T: Message>(&self, input: &T) -> Result<(), Status> {
        if input.encoded_len()
            > self
                .repository
                .service_config()
                .max_request_bytes
                .min(self.repository.service_config().query.max_request_bytes)
        {
            return Err(Status::resource_exhausted(
                "query request exceeds byte budget",
            ));
        }
        Ok(())
    }
    async fn upload<T: Message + Send + 'static>(
        &self,
        identity: Identity,
        deadline: Instant,
        host: Arc<QueryHost>,
        work: Arc<Work>,
        header: q::ScopeHeader,
        parent: Option<q::ScopeKey>,
        mut stream: Streaming<T>,
        frame: impl Fn(T) -> UploadFrame,
    ) -> Result<Response<q::QueryScope>, Status> {
        if header.version != 1 {
            return Err(Status::invalid_argument("unsupported query scope version"));
        }
        let replay: [u8; 32] = header
            .replay_key
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("scope replay key must be 32 bytes"))?;
        if header.overlay_object_count > host.config.max_scope_overlay_nodes as u64
            || header.overlay_bytes > host.config.max_scope_overlay_bytes as u64
        {
            return Err(Status::resource_exhausted(
                "scope overlay declaration exceeds budget",
            ));
        }
        if (header.overlay_object_count == 0) != (header.overlay_bytes == 0) {
            return Err(Status::invalid_argument(
                "inconsistent scope overlay declaration",
            ));
        }
        let roots = ids(&header.roots, host.config.max_roots)?;
        if roots.is_empty() || roots.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Status::invalid_argument(
                "scope roots must be nonempty sorted unique IDs",
            ));
        }
        let prepare_header = header.clone();
        let owner = identity.id.clone();
        let (prepared, work) = execute(host.clone(), work, deadline, move |host, work| {
            let prepared = host.prepare(&owner, &prepare_header, roots, parent)?;
            work.scopes.lock().unwrap().push(prepared.base.clone());
            Ok(prepared)
        })
        .await?;
        let mut digest = prepared.digest();
        let mut commits = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut current: Option<(m::Descriptor, Vec<u8>)> = None;
        let mut bytes = 0u64;
        let mut wire_bytes = 0usize;
        let max_wire = (header.overlay_bytes as usize)
            .saturating_mul(2)
            .saturating_add((header.overlay_object_count as usize).saturating_mul(512))
            .saturating_add(1024);
        let idle = std::time::Duration::from_millis(
            self.repository.service_config().rpc.stream_idle_timeout_ms,
        );
        loop {
            let value = next(&mut stream, deadline, idle)
                .await?
                .ok_or_else(|| Status::invalid_argument("scope stream truncated"))?;
            let encoded = value.encoded_len();
            if encoded
                > self
                    .repository
                    .service_config()
                    .max_frame_bytes
                    .saturating_add(1024)
            {
                return Err(Status::resource_exhausted("overlay frame exceeds budget"));
            }
            wire_bytes = wire_bytes.saturating_add(encoded);
            if wire_bytes > max_wire {
                return Err(Status::resource_exhausted("overlay framing exceeds budget"));
            }
            match frame(value) {
                UploadFrame::Commit(value) if current.is_none() => {
                    if commits.len() as u64 >= header.overlay_object_count {
                        return Err(Status::invalid_argument("overlay object count exceeded"));
                    }
                    let value = value
                        .descriptor
                        .ok_or_else(|| Status::invalid_argument("overlay descriptor missing"))?;
                    let key = value
                        .key
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("overlay key missing"))?;
                    let id: [u8; 32] = key
                        .native_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::invalid_argument("invalid overlay ID"))?;
                    if key.kind != 3
                        || id == [0; 32]
                        || value.content_blake3 != id
                        || value.object_format_version != m::FORMAT_VERSION as u32
                        || value.size_bytes == 0
                    {
                        return Err(Status::invalid_argument(
                            "invalid overlay commit descriptor",
                        ));
                    }
                    if value.size_bytes > host.config.max_commit_bytes as u64
                        || value.size_bytes > header.overlay_bytes.saturating_sub(bytes)
                    {
                        return Err(Status::resource_exhausted(
                            "overlay object exceeds declaration or budget",
                        ));
                    }
                    if !seen.insert(id) {
                        return Err(Status::invalid_argument("duplicate overlay commit"));
                    }
                    digest.update(&id);
                    digest.update(&value.size_bytes.to_be_bytes());
                    digest.update(&value.object_format_version.to_be_bytes());
                    current = Some((
                        m::Descriptor {
                            key: m::ObjectKey {
                                kind: m::Kind::Commit,
                                id: m::ObjectId(id),
                            },
                            size_bytes: value.size_bytes,
                            format_version: m::FORMAT_VERSION,
                        },
                        Vec::with_capacity(value.size_bytes as usize),
                    ));
                }
                UploadFrame::Chunk(value) => {
                    let (descriptor, buffer) = current
                        .as_mut()
                        .ok_or_else(|| Status::invalid_argument("overlay chunk without commit"))?;
                    if value.data.is_empty()
                        || value.offset != buffer.len() as u64
                        || value.data.len() > self.repository.service_config().max_frame_bytes
                        || value.data.len() as u64
                            > descriptor.size_bytes.saturating_sub(buffer.len() as u64)
                    {
                        return Err(Status::invalid_argument(
                            "noncontiguous or oversized overlay chunk",
                        ));
                    }
                    digest.update(&value.data);
                    buffer.extend_from_slice(&value.data);
                }
                UploadFrame::CommitEnd(end) => {
                    let (descriptor, buffer) = current
                        .take()
                        .ok_or_else(|| Status::invalid_argument("overlay end without commit"))?;
                    if end.size_bytes != descriptor.size_bytes
                        || buffer.len() as u64 != descriptor.size_bytes
                        || end.content_blake3 != descriptor.key.id.0
                        || blake3::hash(&buffer).as_bytes() != &descriptor.key.id.0
                    {
                        return Err(Status::data_loss(
                            "overlay content does not match descriptor",
                        ));
                    }
                    bytes += descriptor.size_bytes;
                    commits.push((descriptor, Arc::<[u8]>::from(buffer)));
                }
                UploadFrame::End(end) if current.is_none() => {
                    if end.overlay_object_count != header.overlay_object_count
                        || end.overlay_bytes != header.overlay_bytes
                        || commits.len() as u64 != header.overlay_object_count
                        || bytes != header.overlay_bytes
                    {
                        return Err(Status::invalid_argument("scope overlay totals differ"));
                    }
                    if next(&mut stream, deadline, idle).await?.is_some() {
                        return Err(Status::invalid_argument("scope has trailing frames"));
                    }
                    break;
                }
                _ => return Err(Status::invalid_argument("invalid overlay frame order")),
            }
        }
        let digest = *digest.finalize().as_bytes();
        let (value, work) = execute(host, work, deadline, move |host, work| {
            let scope = host.install(&identity.id, &header, replay, prepared, commits, digest)?;
            work.scopes.lock().unwrap().push(scope.clone());
            Ok(scope.wire.clone())
        })
        .await?;
        response(
            value,
            work,
            self.repository.service_config().query.max_response_bytes,
        )
    }
    async fn scoped<T: Message + Send + 'static>(
        &self,
        identity: Identity,
        deadline: Instant,
        host: Arc<QueryHost>,
        admission: Option<Arc<Work>>,
        key: q::ScopeKey,
        f: impl FnOnce(&Scoped, &mut WorkBudget, &QueryConfig) -> Result<T, Status> + Send + 'static,
    ) -> Result<Response<T>, Status> {
        identity.authorize(&key.repository_id, false)?;
        let work = admitted(&host, deadline, admission).await?;
        let (value, work) = execute(host, work, deadline, move |host, work| {
            let scope = host.lookup(&identity.id, &key.repository_id, &key.handle)?;
            work.scopes.lock().unwrap().push(scope.clone());
            #[cfg(test)]
            host.pause_query()?;
            let mut budget = host.config.budget();
            f(&scope, &mut budget, &host.config)
        })
        .await?;
        response(
            value,
            work,
            self.repository.service_config().query.max_response_bytes,
        )
    }
}
async fn admitted(
    host: &Arc<QueryHost>,
    deadline: Instant,
    existing: Option<Arc<Work>>,
) -> Result<Arc<Work>, Status> {
    if let Some(work) = existing {
        if !work.belongs_to(host) {
            return Err(Status::internal("query ingress owner mismatch"));
        }
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded("derived query deadline exceeded"));
        }
        Ok(work)
    } else {
        host.admit(deadline).await
    }
}
async fn execute<T: Send + 'static>(
    host: Arc<QueryHost>,
    work: Arc<Work>,
    deadline: Instant,
    f: impl FnOnce(&QueryHost, &Arc<Work>) -> Result<T, Status> + Send + 'static,
) -> Result<(T, Arc<Work>), Status> {
    // The result also retains the owner: timeout/cancellation cannot release
    // physical work or a completed-but-not-yet-collected allocation early.
    let (value, owner) = tokio::time::timeout_at(
        deadline,
        tokio::task::spawn_blocking(move || {
            let value = f(&host, &work);
            (value, work)
        }),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("derived query deadline exceeded"))?
    .map_err(|_| Status::internal("derived query worker failed"))?;
    value.map(|value| (value, owner))
}
fn response<T: Message>(value: T, work: Arc<Work>, maximum: usize) -> Result<Response<T>, Status> {
    if value.encoded_len() > maximum {
        return Err(Status::resource_exhausted(
            "query response exceeds byte budget",
        ));
    }
    let mut response = Response::new(value);
    response
        .extensions_mut()
        .insert(crate::body_deadline::ResponseBudget::new(work));
    Ok(response)
}
fn commit(value: &q::CommitId) -> Result<m::CommitId, Status> {
    Ok(m::CommitId(value.value.as_slice().try_into().map_err(
        |_| Status::invalid_argument("invalid native commit ID"),
    )?))
}
fn change(value: &q::ChangeId) -> Result<m::ChangeId, Status> {
    Ok(m::ChangeId(value.value.as_slice().try_into().map_err(
        |_| Status::invalid_argument("invalid native change ID"),
    )?))
}
fn ids(values: &[q::CommitId], maximum: usize) -> Result<Vec<m::CommitId>, Status> {
    if values.len() > maximum {
        return Err(Status::resource_exhausted("query input count exceeded"));
    }
    values.iter().map(commit).collect()
}
fn wire(id: m::CommitId) -> q::CommitId {
    q::CommitId {
        value: id.0.to_vec(),
    }
}
fn set(values: Vec<m::CommitId>, maximum: usize) -> Result<q::CommitSet, Status> {
    if values.len() > maximum {
        return Err(Status::resource_exhausted("query result count exceeded"));
    }
    Ok(q::CommitSet {
        commits: values.into_iter().map(wire).collect(),
    })
}
fn scope_key(key: Option<q::ScopeKey>) -> Result<q::ScopeKey, Status> {
    key.ok_or_else(|| Status::invalid_argument("query scope is required"))
}
fn prefix(text: &str, width: usize) -> Result<(), Status> {
    if text.len() > width
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Status::invalid_argument(
            "invalid lowercase hexadecimal prefix",
        ));
    }
    Ok(())
}
fn visible(scope: &Scoped, heads: &[m::CommitId], budget: &mut WorkBudget) -> Result<(), Status> {
    for id in heads {
        if !scope.graph().has_id(*id, budget).map_err(index_error)? {
            return Err(Status::invalid_argument(
                "visible head is outside query scope",
            ));
        }
    }
    Ok(())
}
#[tonic::async_trait]
impl RepositoryQuery for QueryService {
    async fn open_scope(
        &self,
        request: Request<Streaming<q::OpenScopeFrame>>,
    ) -> Result<Response<q::QueryScope>, Status> {
        let (identity, deadline, host) = self.context(&request, true)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        let work = admitted(&host, deadline, admission).await?;
        let idle = std::time::Duration::from_millis(
            self.repository.service_config().rpc.stream_idle_timeout_ms,
        );
        let mut stream = request.into_inner();
        let first = next(&mut stream, deadline, idle)
            .await?
            .ok_or_else(|| Status::invalid_argument("scope header missing"))?;
        self.bounded(&first)?;
        let Some(q::open_scope_frame::Frame::Header(header)) = first.frame else {
            return Err(Status::invalid_argument("scope header must be first"));
        };
        identity.authorize(&header.repository_id, false)?;
        self.upload(
            identity,
            deadline,
            host,
            work,
            header,
            None,
            stream,
            |frame| match frame.frame {
                Some(q::open_scope_frame::Frame::Commit(v)) => UploadFrame::Commit(v),
                Some(q::open_scope_frame::Frame::Chunk(v)) => UploadFrame::Chunk(v),
                Some(q::open_scope_frame::Frame::CommitEnd(v)) => UploadFrame::CommitEnd(v),
                Some(q::open_scope_frame::Frame::ScopeEnd(v)) => UploadFrame::End(v),
                _ => UploadFrame::Invalid,
            },
        )
        .await
    }
    async fn fork_scope(
        &self,
        request: Request<Streaming<q::ForkScopeFrame>>,
    ) -> Result<Response<q::QueryScope>, Status> {
        let (identity, deadline, host) = self.context(&request, true)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        let work = admitted(&host, deadline, admission).await?;
        let idle = std::time::Duration::from_millis(
            self.repository.service_config().rpc.stream_idle_timeout_ms,
        );
        let mut stream = request.into_inner();
        let first = next(&mut stream, deadline, idle)
            .await?
            .ok_or_else(|| Status::invalid_argument("fork header missing"))?;
        self.bounded(&first)?;
        let Some(q::fork_scope_frame::Frame::Header(header)) = first.frame else {
            return Err(Status::invalid_argument("fork header must be first"));
        };
        let header_scope = header
            .scope
            .ok_or_else(|| Status::invalid_argument("fork scope header missing"))?;
        identity.authorize(&header_scope.repository_id, false)?;
        let parent = scope_key(header.parent_scope)?;
        identity.authorize(&parent.repository_id, false)?;
        self.upload(
            identity,
            deadline,
            host,
            work,
            header_scope,
            Some(parent),
            stream,
            |frame| match frame.frame {
                Some(q::fork_scope_frame::Frame::Commit(v)) => UploadFrame::Commit(v),
                Some(q::fork_scope_frame::Frame::Chunk(v)) => UploadFrame::Chunk(v),
                Some(q::fork_scope_frame::Frame::CommitEnd(v)) => UploadFrame::CommitEnd(v),
                Some(q::fork_scope_frame::Frame::ScopeEnd(v)) => UploadFrame::End(v),
                _ => UploadFrame::Invalid,
            },
        )
        .await
    }
    async fn close_scope(
        &self,
        request: Request<q::CloseScopeRequest>,
    ) -> Result<Response<q::CloseScopeResponse>, Status> {
        let (identity, deadline, host) = self.context(&request, false)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        self.bounded(request.get_ref())?;
        let key = scope_key(request.into_inner().scope)?;
        identity.authorize(&key.repository_id, false)?;
        let work = admitted(&host, deadline, admission).await?;
        let (value, work) = execute(host, work, deadline, move |host, _| {
            host.close(&identity.id, &key.repository_id, &key.handle)?;
            Ok(q::CloseScopeResponse {})
        })
        .await?;
        response(
            value,
            work,
            self.repository.service_config().query.max_response_bytes,
        )
    }
    async fn query_facts(
        &self,
        request: Request<q::QueryFactsRequest>,
    ) -> Result<Response<q::GraphFacts>, Status> {
        let (identity, deadline, host) = self.context(&request, false)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let key = scope_key(input.scope)?;
        if input.facts.len() > host.config.max_facts {
            return Err(Status::resource_exhausted("query fact count exceeded"));
        }
        self.scoped(
            identity,
            deadline,
            host,
            admission,
            key,
            move |scope, budget, config| {
                let mut answers = Vec::new();
                let mut inputs = 0usize;
                let mut outputs = 0usize;
                for fact in input.facts {
                    let answer = match fact
                        .fact
                        .ok_or_else(|| Status::invalid_argument("query fact variant missing"))?
                    {
                        q::graph_fact::Fact::HasId(value) => {
                            inputs += 1;
                            q::graph_fact_answer::Answer::HasId(
                                scope
                                    .graph()
                                    .has_id(
                                        commit(value.commit.as_ref().ok_or_else(|| {
                                            Status::invalid_argument("commit ID missing")
                                        })?)?,
                                        budget,
                                    )
                                    .map_err(index_error)?,
                            )
                        }
                        q::graph_fact::Fact::IsAncestor(value) => {
                            inputs += 2;
                            q::graph_fact_answer::Answer::IsAncestor(
                                scope
                                    .graph()
                                    .is_ancestor(
                                        commit(value.ancestor.as_ref().ok_or_else(|| {
                                            Status::invalid_argument("ancestor missing")
                                        })?)?,
                                        commit(value.descendant.as_ref().ok_or_else(|| {
                                            Status::invalid_argument("descendant missing")
                                        })?)?,
                                        budget,
                                    )
                                    .map_err(index_error)?,
                            )
                        }
                        q::graph_fact::Fact::Heads(value) => {
                            inputs = inputs.saturating_add(value.commits.len());
                            let values = scope
                                .graph()
                                .heads(&ids(&value.commits, config.max_input_ids)?, budget)
                                .map_err(index_error)?;
                            outputs = outputs.saturating_add(values.len());
                            q::graph_fact_answer::Answer::Heads(set(values, config.max_result_ids)?)
                        }
                        q::graph_fact::Fact::CommonAncestors(value) => {
                            inputs = inputs
                                .saturating_add(value.left.len())
                                .saturating_add(value.right.len());
                            let values = scope
                                .graph()
                                .common_ancestors(
                                    &ids(&value.left, config.max_input_ids)?,
                                    &ids(&value.right, config.max_input_ids)?,
                                    budget,
                                )
                                .map_err(index_error)?;
                            outputs = outputs.saturating_add(values.len());
                            q::graph_fact_answer::Answer::CommonAncestors(set(
                                values,
                                config.max_result_ids,
                            )?)
                        }
                    };
                    if inputs > config.max_input_ids || outputs > config.max_result_ids {
                        return Err(Status::resource_exhausted(
                            "query aggregate ID count exceeded",
                        ));
                    }
                    answers.push(q::GraphFactAnswer {
                        answer: Some(answer),
                    });
                }
                Ok(q::GraphFacts {
                    answers,
                    scope_digest: scope.wire.content_digest.clone(),
                    index_epoch: scope.wire.index_epoch.clone(),
                })
            },
        )
        .await
    }
    async fn resolve_commit_prefix(
        &self,
        request: Request<q::ResolveCommitPrefixRequest>,
    ) -> Result<Response<q::CommitPrefixResult>, Status> {
        let (identity, deadline, host) = self.context(&request, false)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let key = scope_key(input.scope)?;
        prefix(&input.hex_prefix, 64)?;
        self.scoped(
            identity,
            deadline,
            host,
            admission,
            key,
            move |scope, budget, _| {
                let result = match scope
                    .graph()
                    .resolve_commit_prefix(&input.hex_prefix, budget)
                    .map_err(index_error)?
                {
                    PrefixResolution::NoMatch => {
                        q::commit_prefix_result::Result::NoMatch(q::NoMatch {})
                    }
                    PrefixResolution::Ambiguous => {
                        q::commit_prefix_result::Result::Ambiguous(q::AmbiguousCommits {})
                    }
                    PrefixResolution::Single(id) => {
                        q::commit_prefix_result::Result::Single(q::CommitMatch {
                            commit: Some(wire(id)),
                        })
                    }
                };
                Ok(q::CommitPrefixResult {
                    result: Some(result),
                })
            },
        )
        .await
    }
    async fn resolve_change_prefix(
        &self,
        request: Request<q::ResolveChangePrefixRequest>,
    ) -> Result<Response<q::ChangePrefixResult>, Status> {
        let (identity, deadline, host) = self.context(&request, false)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let key = scope_key(input.scope)?;
        prefix(&input.hex_prefix, 32)?;
        self.scoped(
            identity,
            deadline,
            host,
            admission,
            key,
            move |scope, budget, config| {
                let heads = ids(&input.visible_heads, config.max_input_ids)?;
                visible(scope, &heads, budget)?;
                let result = match scope
                    .graph()
                    .resolve_change_prefix(&input.hex_prefix, budget)
                    .map_err(index_error)?
                {
                    PrefixResolution::NoMatch => {
                        q::change_prefix_result::Result::NoMatch(q::NoMatch {})
                    }
                    PrefixResolution::Ambiguous => {
                        q::change_prefix_result::Result::Ambiguous(q::AmbiguousChanges {})
                    }
                    PrefixResolution::Single(targets) => {
                        if targets.commits.len() > config.max_result_ids {
                            return Err(Status::resource_exhausted(
                                "query change target count exceeded",
                            ));
                        }
                        let mut values = Vec::new();
                        for target in targets.commits {
                            let mut found = false;
                            for head in &heads {
                                if scope
                                    .graph()
                                    .is_ancestor(target, *head, budget)
                                    .map_err(index_error)?
                                {
                                    found = true;
                                    break;
                                }
                            }
                            values.push(q::ChangeTarget {
                                commit: Some(wire(target)),
                                visible: Some(found),
                            });
                        }
                        q::change_prefix_result::Result::Single(q::ChangeMatch {
                            change: Some(q::ChangeId {
                                value: targets.change_id.0.to_vec(),
                            }),
                            targets: values,
                        })
                    }
                };
                Ok(q::ChangePrefixResult {
                    result: Some(result),
                })
            },
        )
        .await
    }
    async fn shortest_prefix(
        &self,
        request: Request<q::ShortestPrefixRequest>,
    ) -> Result<Response<q::ShortestPrefixResult>, Status> {
        let (identity, deadline, host) = self.context(&request, false)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let key = scope_key(input.scope)?;
        self.scoped(
            identity,
            deadline,
            host,
            admission,
            key,
            move |scope, budget, config| {
                let heads = ids(&input.visible_heads, config.max_input_ids)?;
                visible(scope, &heads, budget)?;
                let (kind, length) = match input
                    .target
                    .ok_or_else(|| Status::invalid_argument("prefix target missing"))?
                {
                    q::shortest_prefix_request::Target::Commit(id) => (
                        q::PrefixKind::CommitPrefix,
                        scope
                            .graph()
                            .shortest_commit_prefix(commit(&id)?, budget)
                            .map_err(index_error)?,
                    ),
                    q::shortest_prefix_request::Target::Change(id) => (
                        q::PrefixKind::ChangePrefix,
                        scope
                            .graph()
                            .shortest_change_prefix(change(&id)?, budget)
                            .map_err(index_error)?,
                    ),
                };
                Ok(q::ShortestPrefixResult {
                    kind: kind as i32,
                    hex_length: length as u32,
                })
            },
        )
        .await
    }
    async fn evaluate(
        &self,
        request: Request<q::EvaluateRequest>,
    ) -> Result<Response<q::EvaluatePage>, Status> {
        let (identity, _, _) = self.context(&request, false)?;
        self.bounded(request.get_ref())?;
        let key = scope_key(request.into_inner().scope)?;
        identity.authorize(&key.repository_id, false)?;
        Err(Status::unimplemented(
            "query evaluation and cursors are not implemented",
        ))
    }
}
enum UploadFrame {
    Commit(q::OverlayCommitHeader),
    Chunk(tinytree_proto::v1::DataChunk),
    CommitEnd(q::OverlayCommitEnd),
    End(q::ScopeEnd),
    Invalid,
}
async fn next<T>(
    stream: &mut Streaming<T>,
    deadline: Instant,
    idle: std::time::Duration,
) -> Result<Option<T>, Status> {
    tokio::time::timeout_at(deadline.min(Instant::now() + idle), stream.message())
        .await
        .map_err(|_| Status::deadline_exceeded("query scope stream expired"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_physical_query_retains_scope_roots_and_admission_until_completion() {
        let temporary = tempfile::tempdir().unwrap();
        let store =
            tinytree_store::Store::open(temporary.path().join("objects"), Default::default())
                .unwrap();
        store.create_repository("example").unwrap();
        let mut commit = m::synthetic_root();
        commit.parents = vec![m::CommitId::ROOT];
        commit.change_id = m::ChangeId([1; 16]);
        let canonical = m::encode(&m::Object::Commit(commit), Default::default()).unwrap();
        store
            .put("example", canonical.descriptor, &canonical.bytes, 0)
            .unwrap();
        let index =
            tinytree_index::Index::open(temporary.path().join("derived"), &Default::default())
                .unwrap();
        index
            .ingest("example", canonical.descriptor, &canonical.bytes)
            .unwrap();
        let mut config = crate::ServiceConfig::default();
        config.query.max_inflight = 1;
        let repository =
            RepositoryService::new(store.clone(), config, vec!["example".into()], Vec::new())
                .unwrap()
                .with_query_index(index)
                .unwrap();
        let host = repository.query_host().unwrap();
        let owned = host.clone();
        let scope = tokio::task::spawn_blocking(move || {
            owned.open(
                "local-owner",
                "example",
                [1; 32],
                vec![m::CommitId(canonical.descriptor.key.id.0)],
            )
        })
        .await
        .unwrap()
        .unwrap();
        let (entered, started) = std::sync::mpsc::sync_channel(1);
        let (resume, wait) = std::sync::mpsc::sync_channel(1);
        *host.pause.lock().unwrap() = Some(crate::query_index::QueryPause {
            entered,
            resume: wait,
        });
        let service = repository.query_service();
        let key = scope.key.clone().unwrap();
        let input = q::QueryFactsRequest {
            scope: Some(key.clone()),
            facts: vec![q::GraphFact {
                fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                    commit: Some(wire(m::CommitId(canonical.descriptor.key.id.0))),
                })),
            }],
        };
        let worker = service.clone();
        let call = tokio::spawn(async move { worker.query_facts(Request::new(input)).await });
        tokio::task::spawn_blocking(move || {
            started.recv_timeout(std::time::Duration::from_secs(5))
        })
        .await
        .unwrap()
        .unwrap();
        call.abort();
        let _ = call.await;
        let owned = host.clone();
        let closed = key.clone();
        tokio::task::spawn_blocking(move || owned.close("local-owner", "example", &closed.handle))
            .await
            .unwrap()
            .unwrap();
        let owned = store.clone();
        tokio::task::spawn_blocking(move || {
            owned.gc("example", i64::MAX - 1).unwrap();
            assert!(
                owned
                    .descriptor("example", canonical.descriptor.key)
                    .unwrap()
                    .is_some()
            );
        })
        .await
        .unwrap();
        let mut request = Request::new(q::QueryFactsRequest {
            scope: Some(key.clone()),
            facts: Vec::new(),
        });
        request.set_timeout(std::time::Duration::from_millis(30));
        assert_eq!(
            service.query_facts(request).await.unwrap_err().code(),
            tonic::Code::DeadlineExceeded,
            "cancelled waiter must not free accepted physical query admission"
        );
        resume.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let owned = store.clone();
                let retained = tokio::task::spawn_blocking(move || {
                    owned.gc("example", i64::MAX - 1).unwrap();
                    owned
                        .descriptor("example", canonical.descriptor.key)
                        .unwrap()
                        .is_some()
                })
                .await
                .unwrap();
                if !retained {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            service
                .query_facts(Request::new(q::QueryFactsRequest {
                    scope: Some(key),
                    facts: Vec::new()
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
}
