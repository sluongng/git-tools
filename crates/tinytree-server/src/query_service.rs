//! Authenticated immutable remote and private-draft query scopes.
//! Evaluation remains explicit unsupported work.
use crate::{
    RepositoryService,
    auth::Identity,
    query_index::{QueryConfig, QueryHost, Scoped, Work, index_error},
};
use prost::Message;
use q::repository_query_server::RepositoryQuery;
use std::sync::Arc;
use tinytree_index::{PrefixResolution, WorkBudget};
use tinytree_model as m;
use tinytree_proto::query_v1 as q;
use tokio::time::Instant;
use tonic::{Request, Response, Status, Streaming};
#[derive(Clone)]
pub struct QueryService {
    repository: RepositoryService,
}
impl QueryService {
    pub(crate) fn new(repository: RepositoryService) -> Self {
        Self { repository }
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
        cloud_proof: Option<Arc<crate::cloud::Work>>,
        header: q::ScopeHeader,
        parent: Option<q::ScopeKey>,
        mut stream: Streaming<T>,
        frame: impl Fn(T) -> UploadFrame,
    ) -> Result<Response<q::QueryScope>, Status> {
        if !matches!(header.version, 1 | 2) {
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
        if (roots.is_empty() && cloud_proof.is_none() && parent.is_none())
            || roots.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Status::invalid_argument(
                "scope roots must be nonempty sorted unique IDs",
            ));
        }
        if cloud_proof.is_some()
            && (header.version != 2
                || !roots.is_empty()
                || header.cloud_source.is_none()
                || parent.is_some())
        {
            return Err(Status::invalid_argument(
                "cloud scope requires identity2, empty roots and exact source",
            ));
        }
        if cloud_proof.is_none() && header.cloud_source.is_some() {
            return Err(Status::invalid_argument(
                "cloud source requires OpenCloudScope",
            ));
        }
        let cloud_owner = if let Some(proof) = &cloud_proof {
            Some(
                self.repository
                    .cloud_host()
                    .ok_or_else(|| Status::unimplemented("cloud query scopes are disabled"))?
                    .repository(proof, &identity, &header.repository_id, false)?,
            )
        } else {
            None
        };
        let cloud_capability = identity.cloud_capability().is_ok();
        let prepare_header = header.clone();
        let owner = identity.id.clone();
        let (prepared, work) = execute(host.clone(), work, deadline, move |host, work| {
            // The proof owner is physical-only. Prepared/Scoped retain an
            // independently charged compact Store guard, never this Work.
            let _proof = cloud_proof;
            let prepared = if let Some(cloud_owner) = cloud_owner {
                host.prepare_cloud(&owner, &prepare_header, cloud_owner)?
            } else {
                host.prepare(&owner, &prepare_header, roots, parent, cloud_capability)?
            };
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
            if scope.is_cloud() {
                identity.cloud_capability()?;
            }
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
async fn admitted_history(
    host: &Arc<QueryHost>,
    deadline: Instant,
    existing: Option<Arc<Work>>,
) -> Result<Arc<Work>, Status> {
    if let Some(work) = existing {
        if !work.belongs_to(host) || !work.has_history_memory() {
            return Err(Status::internal("history ingress owner mismatch"));
        }
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded("history admission expired"));
        }
        Ok(work)
    } else {
        host.admit_history(deadline).await
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
fn selection_digest(expression: &q::GraphExpr) -> Vec<u8> {
    tinytree_model::query_prefix_selection_digest(&expression.encode_to_vec()).to_vec()
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
            None,
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
    async fn open_cloud_scope(
        &self,
        request: Request<Streaming<q::OpenScopeFrame>>,
    ) -> Result<Response<q::QueryScope>, Status> {
        let (identity, deadline, host) = self.context(&request, true)?;
        let proof = request
            .extensions()
            .get::<Arc<crate::query_ingress::CloudProofAdmission>>()
            .ok_or_else(|| Status::failed_precondition("cloud scope transport admission required"))?
            .0
            .lock()
            .map_err(|_| Status::internal("cloud proof admission poisoned"))?
            .take()
            .ok_or_else(|| Status::failed_precondition("cloud proof admission already consumed"))?;
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
            Some(proof),
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
            None,
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
        let facts_host = host.clone();
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
                let mut remaining_steps = config.max_eval_steps as u64;
                for fact in input.facts {
                    let answer = match fact
                        .fact
                        .ok_or_else(|| Status::invalid_argument("query fact variant missing"))?
                    {
                        q::graph_fact::Fact::Membership(value) => {
                            let candidate = commit(value.candidate.as_ref().ok_or_else(|| {
                                Status::invalid_argument("membership candidate missing")
                            })?)?;
                            let expression = value.expression.ok_or_else(|| {
                                Status::invalid_argument("membership expression missing")
                            })?;
                            q::graph_fact_answer::Answer::Membership(
                                facts_host.cursors.membership(
                                    scope,
                                    expression,
                                    candidate,
                                    budget,
                                    config,
                                    deadline,
                                    &mut remaining_steps,
                                    &mut inputs,
                                )?,
                            )
                        }
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
        let prefix_host = host.clone();
        self.scoped(
            identity,
            deadline,
            host,
            admission,
            key,
            move |scope, budget, config| {
                let (resolved, selection_digest) = if let Some(expression) = &input.selection {
                    let mut selected = crate::query_prefix::PrefixSelection::new(
                        scope,
                        expression,
                        config,
                        &prefix_host.cursors,
                        deadline,
                    )?;
                    let digest = selection_digest(expression);
                    (
                        selected.resolve_commit(&input.hex_prefix, budget)?,
                        Some(digest),
                    )
                } else {
                    (
                        scope
                            .graph()
                            .resolve_commit_prefix(&input.hex_prefix, budget)
                            .map_err(index_error)?,
                        None,
                    )
                };
                let result = match resolved {
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
                    selection_digest,
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
        let prefix_host = host.clone();
        self.scoped(
            identity,
            deadline,
            host,
            admission,
            key,
            move |scope, budget, config| {
                let heads = ids(&input.visible_heads, config.max_input_ids)?;
                visible(scope, &heads, budget)?;
                let (resolved, selection_digest) = if let Some(expression) = &input.selection {
                    let mut selected = crate::query_prefix::PrefixSelection::new(
                        scope,
                        expression,
                        config,
                        &prefix_host.cursors,
                        deadline,
                    )?;
                    let digest = selection_digest(expression);
                    (
                        selected.resolve_change(&input.hex_prefix, budget)?,
                        Some(digest),
                    )
                } else {
                    (
                        scope
                            .graph()
                            .resolve_change_prefix(&input.hex_prefix, budget)
                            .map_err(index_error)?,
                        None,
                    )
                };
                let result = match resolved {
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
                    selection_digest,
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
        let prefix_host = host.clone();
        self.scoped(
            identity,
            deadline,
            host,
            admission,
            key,
            move |scope, budget, config| {
                let heads = ids(&input.visible_heads, config.max_input_ids)?;
                visible(scope, &heads, budget)?;
                if let Some(expression) = &input.selection {
                    let mut selected = crate::query_prefix::PrefixSelection::new(
                        scope,
                        expression,
                        config,
                        &prefix_host.cursors,
                        deadline,
                    )?;
                    let (kind, length) = match input
                        .target
                        .ok_or_else(|| Status::invalid_argument("prefix target missing"))?
                    {
                        q::shortest_prefix_request::Target::Commit(id) => (
                            q::PrefixKind::CommitPrefix,
                            selected.shortest_commit(commit(&id)?, budget)?,
                        ),
                        q::shortest_prefix_request::Target::Change(id) => (
                            q::PrefixKind::ChangePrefix,
                            selected.shortest_change(change(&id)?, budget)?,
                        ),
                    };
                    return Ok(q::ShortestPrefixResult {
                        kind: kind as i32,
                        hex_length: length.unwrap_or(0) as u32,
                        in_selection: Some(length.is_some()),
                        selection_digest: Some(selection_digest(expression)),
                    });
                }
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
                    in_selection: None,
                    selection_digest: None,
                })
            },
        )
        .await
    }
    async fn search(
        &self,
        request: Request<q::SearchRequest>,
    ) -> Result<Response<q::SearchResult>, Status> {
        let (identity, deadline, host) = self.context(&request, false)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let key = scope_key(input.scope.clone())?;
        identity.authorize(&key.repository_id, false)?;
        let work = if let Some(work) = admission {
            if !work.belongs_to(&host) || !work.has_search_memory() {
                return Err(Status::internal("search ingress memory owner mismatch"));
            }
            if Instant::now() >= deadline {
                return Err(Status::deadline_exceeded("search deadline exceeded"));
            }
            work
        } else {
            host.admit_search(deadline).await?
        };
        let (value, work) = execute(host, work, deadline, move |host, work| {
            let scope = host.lookup(&identity.id, &key.repository_id, &key.handle)?;
            if scope.is_cloud() {
                identity.cloud_capability()?;
            }
            work.scopes.lock().unwrap().push(scope.clone());
            let mut budget = host.config.budget();
            host.search.query(
                host,
                scope,
                &key.repository_id,
                &identity.id,
                input,
                &mut budget,
                deadline,
            )
        })
        .await?;
        response(
            value,
            work,
            self.repository.service_config().query.max_response_bytes,
        )
    }
    async fn path_history(
        &self,
        request: Request<q::PathHistoryRequest>,
    ) -> Result<Response<q::PathHistoryResult>, Status> {
        let (identity, deadline, host) = self.context(&request, false)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let key = scope_key(input.scope.clone())?;
        identity.authorize(&key.repository_id, false)?;
        let work = admitted_history(&host, deadline, admission).await?;
        let (value, work) = execute(host, work, deadline, move |host, work| {
            let scope = host.lookup(&identity.id, &key.repository_id, &key.handle)?;
            if scope.is_cloud() {
                identity.cloud_capability()?;
            }
            work.scopes.lock().unwrap().push(scope.clone());
            let mut budget = host.config.budget();
            crate::query_history::path_history(
                host,
                &scope,
                &key.repository_id,
                input,
                &mut budget,
                deadline,
            )
        })
        .await?;
        response(
            value,
            work,
            self.repository.service_config().query.max_response_bytes,
        )
    }
    async fn blame(
        &self,
        request: Request<q::BlameRequest>,
    ) -> Result<Response<q::BlameResult>, Status> {
        let (identity, deadline, host) = self.context(&request, false)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let key = scope_key(input.scope.clone())?;
        identity.authorize(&key.repository_id, false)?;
        let work = admitted_history(&host, deadline, admission).await?;
        let (value, work) = execute(host, work, deadline, move |host, work| {
            let scope = host.lookup(&identity.id, &key.repository_id, &key.handle)?;
            if scope.is_cloud() {
                identity.cloud_capability()?;
            }
            work.scopes.lock().unwrap().push(scope.clone());
            let mut budget = host.config.budget();
            crate::query_history::blame(
                host,
                &scope,
                &key.repository_id,
                input,
                &mut budget,
                deadline,
            )
        })
        .await?;
        response(
            value,
            work,
            self.repository.service_config().query.max_response_bytes,
        )
    }
    async fn close_query(
        &self,
        request: Request<q::CloseQueryRequest>,
    ) -> Result<Response<q::CloseQueryResponse>, Status> {
        let (identity, deadline, host) = self.context(&request, false)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let key = scope_key(input.scope)?;
        identity.authorize(&key.repository_id, false)?;
        let work = admitted(&host, deadline, admission).await?;
        let (value, work) = execute(host, work, deadline, move |host, _| {
            host.cursors
                .close(host, &identity.id, &key, &input.query_id)?;
            Ok(q::CloseQueryResponse {})
        })
        .await?;
        response(
            value,
            work,
            self.repository.service_config().query.max_response_bytes,
        )
    }
    async fn evaluate(
        &self,
        request: Request<q::EvaluateRequest>,
    ) -> Result<Response<q::EvaluatePage>, Status> {
        let (identity, deadline, host) = self.context(&request, false)?;
        let admission = request.extensions().get::<Arc<Work>>().cloned();
        self.bounded(request.get_ref())?;
        let input = request.into_inner();
        let key = scope_key(input.scope.clone())?;
        identity.authorize(&key.repository_id, false)?;
        let work = admitted(&host, deadline, admission).await?;
        let (value, work) = execute(host, work, deadline, move |host, work| {
            if host.requires_cloud(&identity.id, &key.repository_id, &key.handle)? {
                identity.cloud_capability()?;
            }
            let result = host.cursors.evaluate(host, &identity.id, input)?;
            work.query_owners.lock().unwrap().push(result.owner);
            Ok(result.page)
        })
        .await?;
        response(
            value,
            work,
            self.repository.service_config().query.max_response_bytes,
        )
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
        for projection in [
            None,
            Some(q::Projection::Ids),
            Some(q::Projection::Graph),
            Some(q::Projection::Canonical),
        ] {
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
            let call = tokio::spawn(async move {
                if let Some(projection) = projection {
                    worker
                        .evaluate(Request::new(q::EvaluateRequest {
                            scope: input.scope,
                            query_id: vec![19; 32],
                            expression: Some(q::GraphExpr {
                                version: 1,
                                expression: Some(q::graph_expr::Expression::Commits(
                                    q::CommitSet {
                                        commits: vec![wire(m::CommitId(
                                            canonical.descriptor.key.id.0,
                                        ))],
                                    },
                                )),
                            }),
                            projection: projection as i32,
                            page_size: 128,
                            cursor: Vec::new(),
                        }))
                        .await
                        .map(|_| ())
                } else {
                    worker.query_facts(Request::new(input)).await.map(|_| ())
                }
            });
            tokio::task::spawn_blocking(move || {
                started.recv_timeout(std::time::Duration::from_secs(5))
            })
            .await
            .unwrap()
            .unwrap();
            call.abort();
            let _ = call.await;
            let close_call = if projection.is_some() {
                let service = service.clone();
                let close_key = key.clone();
                Some(tokio::spawn(async move {
                    service
                        .close_query(Request::new(q::CloseQueryRequest {
                            scope: Some(close_key),
                            query_id: vec![19; 32],
                        }))
                        .await
                }))
            } else {
                None
            };

            let owned = host.clone();
            let closed = key.clone();
            tokio::task::spawn_blocking(move || {
                owned.close("local-owner", "example", &closed.handle)
            })
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
            if let Some(close) = close_call {
                close.await.unwrap().unwrap();
            }

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
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_query_preserves_the_previously_returned_response_owner_until_drop() {
        let temporary = tempfile::tempdir().unwrap();
        let store =
            tinytree_store::Store::open(temporary.path().join("objects"), Default::default())
                .unwrap();
        store.create_repository("example").unwrap();
        let mut commit = m::synthetic_root();
        commit.parents = vec![m::CommitId::ROOT];
        commit.change_id = m::ChangeId([22; 16]);
        let object = m::encode(&m::Object::Commit(commit), Default::default()).unwrap();
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
        let index =
            tinytree_index::Index::open(temporary.path().join("derived"), &Default::default())
                .unwrap();
        index
            .ingest("example", object.descriptor, &object.bytes)
            .unwrap();
        let mut config = crate::ServiceConfig::default();
        config.query.max_eval_quantum_steps = 1;
        let repository =
            RepositoryService::new(store.clone(), config, vec!["example".into()], Vec::new())
                .unwrap()
                .with_query_index(index)
                .unwrap();
        let host = repository.query_host().unwrap();
        let opened = host.clone();
        let scope = tokio::task::spawn_blocking(move || {
            opened.open(
                "local-owner",
                "example",
                [21; 32],
                vec![m::CommitId(object.descriptor.key.id.0)],
            )
        })
        .await
        .unwrap()
        .unwrap();
        let service = repository.query_service();
        let input = q::EvaluateRequest {
            scope: scope.key.clone(),
            query_id: vec![23; 32],
            expression: Some(q::GraphExpr {
                version: 1,
                expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                    commits: vec![wire(m::CommitId(object.descriptor.key.id.0))],
                })),
            }),
            projection: q::Projection::Canonical as i32,
            page_size: 128,
            cursor: Vec::new(),
        };
        // This is a direct-handler response-budget lifetime regression, not a
        // claim that the unit fixture exercised HTTP/2 socket backpressure.
        let response = service.evaluate(Request::new(input.clone())).await.unwrap();
        assert_eq!(response.get_ref().exhausted, Some(false));
        service
            .close_query(Request::new(q::CloseQueryRequest {
                scope: scope.key.clone(),
                query_id: input.query_id.clone(),
            }))
            .await
            .unwrap();
        service
            .close_scope(Request::new(q::CloseScopeRequest {
                scope: scope.key.clone(),
            }))
            .await
            .unwrap();
        let held = store.clone();
        tokio::task::spawn_blocking(move || {
            held.gc("example", i64::MAX - 1).unwrap();
            assert!(
                held.descriptor("example", object.descriptor.key)
                    .unwrap()
                    .is_some()
            );
        })
        .await
        .unwrap();
        assert_eq!(
            service
                .evaluate(Request::new(input))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        drop(response);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let held = store.clone();
                let gone = tokio::task::spawn_blocking(move || {
                    held.gc("example", i64::MAX - 1).unwrap();
                    held.descriptor("example", object.descriptor.key)
                        .unwrap()
                        .is_none()
                })
                .await
                .unwrap();
                if gone {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn membership_facts_preserve_ast_limits_strict_errors_and_shared_step_budget() {
        let temporary = tempfile::tempdir().unwrap();
        let store =
            tinytree_store::Store::open(temporary.path().join("objects"), Default::default())
                .unwrap();
        store.create_repository("example").unwrap();
        let mut commit = m::synthetic_root();
        commit.parents = vec![m::CommitId::ROOT];
        commit.change_id = m::ChangeId([31; 16]);
        let object = m::encode(&m::Object::Commit(commit), Default::default()).unwrap();
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
        let index =
            tinytree_index::Index::open(temporary.path().join("derived"), &Default::default())
                .unwrap();
        index
            .ingest("example", object.descriptor, &object.bytes)
            .unwrap();
        let mut config = crate::ServiceConfig::default();
        config.query.max_eval_ast_nodes = 1;
        config.query.max_eval_ast_depth = 1;
        config.query.max_eval_literals = 1;
        config.query.max_eval_steps = 8;
        let repository = RepositoryService::new(store, config, vec!["example".into()], Vec::new())
            .unwrap()
            .with_query_index(index)
            .unwrap();
        let host = repository.query_host().unwrap();
        let owner = host.clone();
        let known = m::CommitId(object.descriptor.key.id.0);
        let scope = tokio::task::spawn_blocking(move || {
            owner.open("local-owner", "example", [32; 32], vec![known])
        })
        .await
        .unwrap()
        .unwrap();
        let key = scope.key.clone();
        let service = repository.query_service();
        let expression = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: vec![wire(known)],
            })),
        };
        let fact = |expression: q::GraphExpr, candidate: m::CommitId| q::GraphFact {
            fact: Some(q::graph_fact::Fact::Membership(q::Membership {
                expression: Some(expression),
                candidate: Some(wire(candidate)),
            })),
        };
        // The internal intersection must not reduce the existing user AST/depth
        // or literal allowance. Repeated calls retain no cursor memory/history.
        for _ in 0..20 {
            let answer = service
                .query_facts(Request::new(q::QueryFactsRequest {
                    scope: key.clone(),
                    facts: vec![fact(expression.clone(), known)],
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                answer.answers[0].answer,
                Some(q::graph_fact_answer::Answer::Membership(true))
            );
        }
        let error = service
            .query_facts(Request::new(q::QueryFactsRequest {
                scope: key.clone(),
                facts: vec![
                    fact(expression.clone(), known),
                    fact(expression.clone(), known),
                ],
            }))
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::ResourceExhausted,
            "fact batches share one evaluation step allowance"
        );
        let absent = m::CommitId([33; 32]);
        let mut invalid = expression.clone();
        invalid.version = 2;
        assert_eq!(
            service
                .query_facts(Request::new(q::QueryFactsRequest {
                    scope: key.clone(),
                    facts: vec![fact(invalid, absent)]
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument,
            "an absent candidate must not bypass full AST validation"
        );
        let empty_intersection = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Intersection(q::SetExpr {
                inputs: vec![],
            })),
        };
        assert_eq!(
            service
                .query_facts(Request::new(q::QueryFactsRequest {
                    scope: key.clone(),
                    facts: vec![fact(empty_intersection, absent)]
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let answer = service
            .query_facts(Request::new(q::QueryFactsRequest {
                scope: key.clone(),
                facts: vec![fact(expression.clone(), absent)],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            answer.answers[0].answer,
            Some(q::graph_fact_answer::Answer::Membership(false))
        );
        let unknown_literal = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: vec![wire(absent)],
            })),
        };
        assert_eq!(
            service
                .query_facts(Request::new(q::QueryFactsRequest {
                    scope: key,
                    facts: vec![fact(unknown_literal, known)]
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument,
            "out-of-scope literals preserve Evaluate errors"
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_query_shutdown_survives_cancelled_waiter_and_releases_store_after_physical_work()
     {
        use std::future::Future;
        let temporary = tempfile::tempdir().unwrap();
        let objects = temporary.path().join("objects");
        let store = tinytree_store::Store::open(&objects, Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let index =
            tinytree_index::Index::open(temporary.path().join("index"), &Default::default())
                .unwrap();
        let mut config = crate::ServiceConfig::default();
        config.query.max_inflight = 1;
        config.query.expiry_interval_ms = 60_000;
        let repository =
            RepositoryService::new(store.clone(), config, vec!["example".into()], vec![])
                .unwrap()
                .with_query_index(index)
                .unwrap();
        let host = repository.query_host().unwrap();
        let captured = host.clone();
        let scope = tokio::task::spawn_blocking(move || {
            captured.open("local-owner", "example", [8; 32], vec![m::CommitId::ROOT])
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
        let worker = service.clone();
        let key = scope.key.clone();
        let call = tokio::spawn(async move {
            worker
                .query_facts(Request::new(q::QueryFactsRequest {
                    scope: key,
                    facts: vec![],
                }))
                .await
        });
        tokio::task::spawn_blocking(move || started.recv().unwrap())
            .await
            .unwrap();
        call.abort();
        let _ = call.await;
        let owned = repository.clone();
        let (pending, observed) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            let future = owned.drain_query();
            tokio::pin!(future);
            let mut pending = Some(pending);
            std::future::poll_fn(|cx| {
                let result = future.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(pending) = pending.take() {
                        let _ = pending.send(());
                    }
                }
                result
            })
            .await
        });
        observed.await.unwrap();
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(
            service
                .query_facts(Request::new(q::QueryFactsRequest {
                    scope: scope.key,
                    facts: vec![]
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
        let owned = repository.clone();
        let second = tokio::spawn(async move { owned.drain_query().await });
        assert!(
            !second.is_finished(),
            "accepted paused work still owns its physical admission"
        );
        resume.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        repository.drain_query().await.unwrap();
        drop(host);
        drop(service);
        drop(repository);
        drop(store);
        tinytree_store::Store::open_existing(objects, Default::default()).unwrap();
    }
}
