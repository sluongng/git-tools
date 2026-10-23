//! Checked immutable query scopes with private canonical draft overlays. Exact negative/completeness answers trust the
//! authenticated server; canonical IDs cannot prove that results were not omitted.
use super::*;
use tinytree_model::{ChangeId, CommitId};
use tinytree_proto::query_v1 as q;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckedScopeHandle {
    repository: String,
    handle: [u8; 32],
    digest: [u8; 32],
    epoch: [u8; 16],
    high_water: u64,
    expires_at: i64,
    remote_roots: Arc<[CommitId]>,
}
impl CheckedScopeHandle {
    /// Authoritative remote checkpoint roots, not private draft heads.
    pub fn remote_roots(&self) -> &[CommitId] {
        &self.remote_roots
    }
    pub fn repository_id(&self) -> &str {
        &self.repository
    }
    pub fn handle(&self) -> &[u8; 32] {
        &self.handle
    }
    pub fn content_digest(&self) -> &[u8; 32] {
        &self.digest
    }
    pub fn index_epoch(&self) -> &[u8; 16] {
        &self.epoch
    }
    pub fn high_water(&self) -> u64 {
        self.high_water
    }
    pub fn expires_at_unix_seconds(&self) -> i64 {
        self.expires_at
    }
    fn key(&self) -> q::ScopeKey {
        q::ScopeKey {
            repository_id: self.repository.clone(),
            handle: self.handle.to_vec(),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryFact {
    HasId(CommitId),
    IsAncestor {
        ancestor: CommitId,
        descendant: CommitId,
    },
    Heads(Vec<CommitId>),
    CommonAncestors {
        left: Vec<CommitId>,
        right: Vec<CommitId>,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FactAnswer {
    HasId(bool),
    IsAncestor(bool),
    Heads(Vec<CommitId>),
    CommonAncestors(Vec<CommitId>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitPrefix {
    NoMatch,
    Single(CommitId),
    Ambiguous,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeTarget {
    pub commit: CommitId,
    pub visible: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeMatch {
    pub change: ChangeId,
    pub targets: Vec<ChangeTarget>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChangePrefix {
    NoMatch,
    Single(ChangeMatch),
    Ambiguous,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrefixTarget {
    Commit(CommitId),
    Change(ChangeId),
}

impl NativeClient {
    fn checked_scope(&self, scope: &CheckedScopeHandle, allow_expired: bool) -> Result<(), Status> {
        if scope.repository != self.inner.config.repository_id {
            return Err(Status::invalid_argument(
                "query scope belongs to another repository",
            ));
        }
        if !allow_expired && scope.expires_at <= unix_seconds()? {
            return Err(Status::failed_precondition("native query scope expired"));
        }
        Ok(())
    }
    /// Opens an immutable remote-root scope without private drafts.
    pub async fn open_query_scope(
        &self,
        roots: Vec<CommitId>,
    ) -> Result<CheckedScopeHandle, Status> {
        self.open_query_scope_with_overlay(roots, Vec::new()).await
    }
    /// Admits private, parent-first canonical drafts into a query scope only.
    /// This neither uploads authoritative objects nor publishes/backups changes.
    /// Membership of external/inherited parents is checked by the server.
    pub async fn open_query_scope_with_overlay(
        &self,
        mut roots: Vec<CommitId>,
        overlay: Vec<VerifiedMetadata>,
    ) -> Result<CheckedScopeHandle, Status> {
        self.batch(roots.len())?;
        roots.sort_unstable();
        roots.dedup();
        if roots.is_empty() {
            return Err(Status::invalid_argument("query scope requires roots"));
        }
        self.install_query_scope(roots.into(), overlay, None).await
    }
    /// Shares the parent's immutable remote checkpoint and private graph, adding
    /// only these parent-first drafts. Forks cannot extend the parent's expiry.
    pub async fn fork_query_scope(
        &self,
        parent: &CheckedScopeHandle,
        overlay: Vec<VerifiedMetadata>,
    ) -> Result<CheckedScopeHandle, Status> {
        self.checked_scope(parent, false)?;
        self.install_query_scope(parent.remote_roots.clone(), overlay, Some(parent))
            .await
    }
    async fn install_query_scope(
        &self,
        roots: Arc<[CommitId]>,
        overlay: Vec<VerifiedMetadata>,
        parent: Option<&CheckedScopeHandle>,
    ) -> Result<CheckedScopeHandle, Status> {
        let deadline = self.deadline();
        self.batch(overlay.len())?;
        let limits = &self.inner.config.limits;
        let mut total = 0usize;
        for object in &overlay {
            if object.descriptor.key.kind != Kind::Commit
                || object.descriptor.key.id.0 == [0; 32]
                || !matches!(&object.object, Object::Commit(_))
                || object.bytes.len() > limits.max_metadata_bytes
                || object.descriptor.size_bytes != object.bytes.len() as u64
            {
                return Err(Status::invalid_argument(
                    "query overlay requires verified non-root commits",
                ));
            }
            total = total
                .checked_add(object.bytes.len())
                .ok_or_else(|| Status::resource_exhausted("query overlay bytes exceed budget"))?;
        }
        if total > limits.max_batch_bytes {
            return Err(Status::resource_exhausted(
                "query overlay bytes exceed budget",
            ));
        }
        let memory = total
            .checked_mul(4)
            .and_then(|n| {
                overlay
                    .len()
                    .checked_mul(512)
                    .and_then(|nodes| n.checked_add(nodes))
            })
            .and_then(|n| n.checked_add(self.unary_memory()))
            .ok_or_else(|| Status::resource_exhausted("query overlay memory exceeds budget"))?;
        let budget = self.admit(deadline, memory).await?;
        let all: HashSet<_> = overlay
            .iter()
            .map(|object| object.descriptor.key.id)
            .collect();
        if all.len() != overlay.len() {
            return Err(Status::invalid_argument("duplicate query overlay commit"));
        }
        let mut earlier = HashSet::with_capacity(overlay.len());
        for object in &overlay {
            let Object::Commit(commit) = &object.object else {
                unreachable!()
            };
            for parent in &commit.parents {
                let key = ObjectId(parent.0);
                if all.contains(&key) && !earlier.contains(&key) {
                    return Err(Status::invalid_argument(
                        "query overlay commits must be parent-first",
                    ));
                }
            }
            earlier.insert(object.descriptor.key.id);
        }
        // Move canonical byte vectors once; retries share them without retaining
        // a second decoded object graph or cloning complete payloads per attempt.
        let overlay = Arc::new(Overlay {
            total: total as u64,
            commits: overlay
                .into_iter()
                .map(|object| OverlayCommit {
                    descriptor: object.descriptor,
                    bytes: object.bytes,
                })
                .collect(),
        });
        let mut replay_key = [0u8; 32];
        getrandom::fill(&mut replay_key)
            .map_err(|_| Status::internal("cannot generate query replay identity"))?;
        let header = q::ScopeHeader {
            version: 1,
            repository_id: self.inner.config.repository_id.clone(),
            replay_key: replay_key.to_vec(),
            roots: roots.iter().copied().map(wire_commit).collect(),
            overlay_object_count: overlay.commits.len() as u64,
            overlay_bytes: overlay.total,
        };
        let frame_bytes = limits
            .max_frame_bytes
            .min(limits.max_request_bytes.saturating_sub(128));
        if frame_bytes == 0 {
            return Err(Status::resource_exhausted("query frame budget too small"));
        }
        let fork_header = parent.map(|parent| q::ForkScopeHeader {
            scope: Some(header.clone()),
            parent_scope: Some(parent.key()),
        });
        if let Some(header) = &fork_header {
            self.bounded(&q::ForkScopeFrame {
                frame: Some(q::fork_scope_frame::Frame::Header(header.clone())),
            })?;
        } else {
            self.bounded(&q::OpenScopeFrame {
                frame: Some(q::open_scope_frame::Frame::Header(header.clone())),
            })?;
        }
        self.retry(deadline, true, |allowance| {
            let mut grpc = self.inner.grpc.clone();
            let (header, fork_header, overlay, budget, roots) = (
                header.clone(),
                fork_header.clone(),
                overlay.clone(),
                budget.clone(),
                roots.clone(),
            );
            async move {
                let (done, finished) = tokio::sync::oneshot::channel();
                let physical_owner = Arc::new((overlay.clone(), budget.clone()));
                let scope = if let Some(header) = fork_header {
                    grpc.fork_scope(
                        self.request(
                            fork_scope_frames(header, overlay, budget, frame_bytes, done),
                            allowance,
                        ),
                        physical_owner,
                    )
                    .await?
                    .into_inner()
                } else {
                    grpc.open_scope(
                        self.request(
                            scope_frames(header, overlay, budget, frame_bytes, done),
                            allowance,
                        ),
                        physical_owner,
                    )
                    .await?
                    .into_inner()
                };
                reapi::policy::timeout(deadline, async { finished.await.map_err(|_| integrity()) })
                    .await?;
                let key = scope.key.ok_or_else(integrity)?;
                if key.repository_id != self.inner.config.repository_id
                    || scope.expires_at_unix_seconds <= unix_seconds()?
                    || parent.is_some_and(|parent| {
                        key.handle.as_slice() == parent.handle
                            || scope.expires_at_unix_seconds > parent.expires_at
                            || scope.high_water != parent.high_water
                            || scope.index_epoch.as_slice() != parent.epoch
                    })
                {
                    return Err(integrity());
                }
                Ok(CheckedScopeHandle {
                    repository: key.repository_id,
                    handle: key.handle.try_into().map_err(|_| integrity())?,
                    digest: scope.content_digest.try_into().map_err(|_| integrity())?,
                    epoch: scope.index_epoch.try_into().map_err(|_| integrity())?,
                    high_water: scope.high_water,
                    expires_at: scope.expires_at_unix_seconds,
                    remote_roots: roots,
                })
            }
        })
        .await
    }
    pub async fn close_query_scope(&self, scope: &CheckedScopeHandle) -> Result<(), Status> {
        self.checked_scope(scope, true)?;
        let deadline = self.deadline();
        let _budget = self.admit(deadline, self.unary_memory()).await?;
        let request = q::CloseScopeRequest {
            scope: Some(scope.key()),
        };
        self.bounded(&request)?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.inner.grpc.clone();
            let request = request.clone();
            async move {
                grpc.close_scope(self.request(request, allowance)).await?;
                Ok(())
            }
        })
        .await
    }
    pub async fn query_facts(
        &self,
        scope: &CheckedScopeHandle,
        facts: Vec<QueryFact>,
    ) -> Result<Vec<FactAnswer>, Status> {
        self.checked_scope(scope, false)?;
        self.batch(facts.len())?;
        let deadline = self.deadline();
        let _budget = self.admit(deadline, self.unary_memory()).await?;
        let mut wire = Vec::with_capacity(facts.len());
        let mut input_ids = 0usize;
        for fact in &facts {
            let count = match fact {
                QueryFact::HasId(_) => 1,
                QueryFact::IsAncestor { .. } => 2,
                QueryFact::Heads(ids) => ids.len(),
                QueryFact::CommonAncestors { left, right } => {
                    left.len().checked_add(right.len()).ok_or_else(|| {
                        Status::resource_exhausted("query input IDs exceed budget")
                    })?
                }
            };
            input_ids = input_ids
                .checked_add(count)
                .ok_or_else(|| Status::resource_exhausted("query input IDs exceed budget"))?;
            self.batch(input_ids)?;
            use q::graph_fact::Fact;
            let fact = match fact {
                QueryFact::HasId(id) => Fact::HasId(q::HasId {
                    commit: Some(wire_commit(*id)),
                }),
                QueryFact::IsAncestor {
                    ancestor,
                    descendant,
                } => Fact::IsAncestor(q::IsAncestor {
                    ancestor: Some(wire_commit(*ancestor)),
                    descendant: Some(wire_commit(*descendant)),
                }),
                QueryFact::Heads(ids) => {
                    self.batch(ids.len())?;
                    Fact::Heads(q::Heads {
                        commits: ids.iter().copied().map(wire_commit).collect(),
                    })
                }
                QueryFact::CommonAncestors { left, right } => {
                    self.batch(left.len())?;
                    self.batch(right.len())?;
                    Fact::CommonAncestors(q::CommonAncestors {
                        left: left.iter().copied().map(wire_commit).collect(),
                        right: right.iter().copied().map(wire_commit).collect(),
                    })
                }
            };
            wire.push(q::GraphFact { fact: Some(fact) });
        }
        let request = q::QueryFactsRequest {
            scope: Some(scope.key()),
            facts: wire,
        };
        self.bounded(&request)?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.inner.grpc.clone();
            let request = request.clone();
            let expected = &facts;
            async move {
                let reply = grpc
                    .query_facts(self.request(request, allowance))
                    .await?
                    .into_inner();
                if reply.scope_digest.as_slice() != scope.digest
                    || reply.index_epoch.as_slice() != scope.epoch
                    || reply.answers.len() != expected.len()
                {
                    return Err(integrity());
                }
                reply
                    .answers
                    .into_iter()
                    .zip(expected)
                    .map(|(answer, expected)| {
                        use q::graph_fact_answer::Answer;
                        match (expected, answer.answer) {
                            (QueryFact::HasId(_), Some(Answer::HasId(value))) => {
                                Ok(FactAnswer::HasId(value))
                            }
                            (QueryFact::IsAncestor { .. }, Some(Answer::IsAncestor(value))) => {
                                Ok(FactAnswer::IsAncestor(value))
                            }
                            (QueryFact::Heads(_), Some(Answer::Heads(value))) => {
                                Ok(FactAnswer::Heads(commit_set(value)?))
                            }
                            (
                                QueryFact::CommonAncestors { .. },
                                Some(Answer::CommonAncestors(value)),
                            ) => Ok(FactAnswer::CommonAncestors(commit_set(value)?)),
                            _ => Err(integrity()),
                        }
                    })
                    .collect()
            }
        })
        .await
    }
    pub async fn resolve_commit_prefix(
        &self,
        scope: &CheckedScopeHandle,
        prefix: &str,
    ) -> Result<CommitPrefix, Status> {
        self.checked_scope(scope, false)?;
        check_prefix(prefix, 64)?;
        let deadline = self.deadline();
        let _budget = self.admit(deadline, self.unary_memory()).await?;
        let request = q::ResolveCommitPrefixRequest {
            scope: Some(scope.key()),
            hex_prefix: prefix.into(),
        };
        self.bounded(&request)?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.inner.grpc.clone();
            let request = request.clone();
            async move {
                use q::commit_prefix_result::Result as R;
                match grpc
                    .resolve_commit_prefix(self.request(request, allowance))
                    .await?
                    .into_inner()
                    .result
                {
                    Some(R::NoMatch(_)) => Ok(CommitPrefix::NoMatch),
                    Some(R::Single(value)) => {
                        let id = checked_commit(value.commit)?;
                        if !hex_matches(&id.0, prefix) {
                            return Err(integrity());
                        }
                        Ok(CommitPrefix::Single(id))
                    }
                    Some(R::Ambiguous(_)) => Ok(CommitPrefix::Ambiguous),
                    None => Err(integrity()),
                }
            }
        })
        .await
    }
    pub async fn resolve_change_prefix(
        &self,
        scope: &CheckedScopeHandle,
        prefix: &str,
        mut visible_heads: Vec<CommitId>,
    ) -> Result<ChangePrefix, Status> {
        self.checked_scope(scope, false)?;
        check_prefix(prefix, 32)?;
        self.batch(visible_heads.len())?;
        visible_heads.sort_unstable();
        visible_heads.dedup();
        let deadline = self.deadline();
        let _budget = self.admit(deadline, self.unary_memory()).await?;
        let request = q::ResolveChangePrefixRequest {
            scope: Some(scope.key()),
            hex_prefix: prefix.into(),
            visible_heads: visible_heads.into_iter().map(wire_commit).collect(),
        };
        self.bounded(&request)?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.inner.grpc.clone();
            let request = request.clone();
            async move {
                use q::change_prefix_result::Result as R;
                match grpc
                    .resolve_change_prefix(self.request(request, allowance))
                    .await?
                    .into_inner()
                    .result
                {
                    Some(R::NoMatch(_)) => Ok(ChangePrefix::NoMatch),
                    Some(R::Ambiguous(_)) => Ok(ChangePrefix::Ambiguous),
                    Some(R::Single(value)) => {
                        let change = ChangeId(
                            value
                                .change
                                .ok_or_else(integrity)?
                                .value
                                .try_into()
                                .map_err(|_| integrity())?,
                        );
                        if !hex_matches(&change.0, prefix) || value.targets.is_empty() {
                            return Err(integrity());
                        }
                        let mut unique = HashSet::new();
                        let targets = value
                            .targets
                            .into_iter()
                            .map(|target| {
                                let commit = checked_commit(target.commit)?;
                                if !unique.insert(commit) {
                                    return Err(integrity());
                                }
                                Ok(ChangeTarget {
                                    commit,
                                    visible: target.visible.ok_or_else(integrity)?,
                                })
                            })
                            .collect::<Result<Vec<_>, Status>>()?;
                        Ok(ChangePrefix::Single(ChangeMatch { change, targets }))
                    }
                    None => Err(integrity()),
                }
            }
        })
        .await
    }
    pub async fn shortest_prefix(
        &self,
        scope: &CheckedScopeHandle,
        target: PrefixTarget,
        mut visible_heads: Vec<CommitId>,
    ) -> Result<u32, Status> {
        self.checked_scope(scope, false)?;
        self.batch(visible_heads.len())?;
        visible_heads.sort_unstable();
        visible_heads.dedup();
        let (target, kind, maximum) = match target {
            PrefixTarget::Commit(id) => (
                q::shortest_prefix_request::Target::Commit(wire_commit(id)),
                q::PrefixKind::CommitPrefix,
                64,
            ),
            PrefixTarget::Change(id) => (
                q::shortest_prefix_request::Target::Change(q::ChangeId {
                    value: id.0.to_vec(),
                }),
                q::PrefixKind::ChangePrefix,
                32,
            ),
        };
        let deadline = self.deadline();
        let _budget = self.admit(deadline, self.unary_memory()).await?;
        let request = q::ShortestPrefixRequest {
            scope: Some(scope.key()),
            target: Some(target),
            visible_heads: visible_heads.into_iter().map(wire_commit).collect(),
        };
        self.bounded(&request)?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.inner.grpc.clone();
            let request = request.clone();
            async move {
                let reply = grpc
                    .shortest_prefix(self.request(request, allowance))
                    .await?
                    .into_inner();
                if reply.kind != kind as i32 || reply.hex_length > maximum {
                    return Err(integrity());
                }
                Ok(reply.hex_length)
            }
        })
        .await
    }
}
struct OverlayCommit {
    descriptor: Descriptor,
    bytes: Vec<u8>,
}
struct Overlay {
    commits: Vec<OverlayCommit>,
    total: u64,
}
enum OverlayFrame {
    Commit(q::OverlayCommitHeader),
    Chunk(p::DataChunk),
    CommitEnd(q::OverlayCommitEnd),
    ScopeEnd(q::ScopeEnd),
}
fn overlay_frames(
    overlay: Arc<Overlay>,
    budget: Arc<Budget>,
    frame_bytes: usize,
) -> impl tonic::codegen::tokio_stream::Stream<Item = OverlayFrame> + Send + 'static {
    async_stream::stream! {
        let _budget = budget;
        for commit in &overlay.commits {
            yield OverlayFrame::Commit(q::OverlayCommitHeader { descriptor: Some(wire_descriptor(commit.descriptor)) });
            for (index, bytes) in commit.bytes.chunks(frame_bytes).enumerate() {
                yield OverlayFrame::Chunk(p::DataChunk { offset: (index * frame_bytes) as u64, data: bytes.to_vec() });
            }
            yield OverlayFrame::CommitEnd(q::OverlayCommitEnd { size_bytes: commit.descriptor.size_bytes, content_blake3: commit.descriptor.key.id.0.to_vec() });
        }
        yield OverlayFrame::ScopeEnd(q::ScopeEnd { overlay_object_count: overlay.commits.len() as u64, overlay_bytes: overlay.total });
    }
}
fn scope_frames(
    header: q::ScopeHeader,
    overlay: Arc<Overlay>,
    budget: Arc<Budget>,
    frame_bytes: usize,
    done: tokio::sync::oneshot::Sender<()>,
) -> impl tonic::codegen::tokio_stream::Stream<Item = q::OpenScopeFrame> + Send + 'static {
    async_stream::stream! {
        use tonic::codegen::tokio_stream::StreamExt;
        let body = overlay_frames(overlay,budget,frame_bytes); tokio::pin!(body);
        yield q::OpenScopeFrame { frame: Some(q::open_scope_frame::Frame::Header(header)) };
        while let Some(frame) = body.next().await {
            use q::open_scope_frame::Frame;
            let frame = match frame { OverlayFrame::Commit(v)=>Frame::Commit(v),OverlayFrame::Chunk(v)=>Frame::Chunk(v),OverlayFrame::CommitEnd(v)=>Frame::CommitEnd(v),OverlayFrame::ScopeEnd(v)=>Frame::ScopeEnd(v) };
            yield q::OpenScopeFrame { frame: Some(frame) };
        }
        let _ = done.send(());
    }
}
fn fork_scope_frames(
    header: q::ForkScopeHeader,
    overlay: Arc<Overlay>,
    budget: Arc<Budget>,
    frame_bytes: usize,
    done: tokio::sync::oneshot::Sender<()>,
) -> impl tonic::codegen::tokio_stream::Stream<Item = q::ForkScopeFrame> + Send + 'static {
    async_stream::stream! {
        use tonic::codegen::tokio_stream::StreamExt;
        let body = overlay_frames(overlay,budget,frame_bytes); tokio::pin!(body);
        yield q::ForkScopeFrame { frame: Some(q::fork_scope_frame::Frame::Header(header)) };
        while let Some(frame) = body.next().await {
            use q::fork_scope_frame::Frame;
            let frame = match frame { OverlayFrame::Commit(v)=>Frame::Commit(v),OverlayFrame::Chunk(v)=>Frame::Chunk(v),OverlayFrame::CommitEnd(v)=>Frame::CommitEnd(v),OverlayFrame::ScopeEnd(v)=>Frame::ScopeEnd(v) };
            yield q::ForkScopeFrame { frame: Some(frame) };
        }
        let _ = done.send(());
    }
}

fn wire_commit(id: CommitId) -> q::CommitId {
    q::CommitId {
        value: id.0.to_vec(),
    }
}
fn checked_commit(id: Option<q::CommitId>) -> Result<CommitId, Status> {
    Ok(CommitId(
        id.ok_or_else(integrity)?
            .value
            .try_into()
            .map_err(|_| integrity())?,
    ))
}
fn commit_set(set: q::CommitSet) -> Result<Vec<CommitId>, Status> {
    let mut unique = HashSet::new();
    set.commits
        .into_iter()
        .map(|id| {
            let id = checked_commit(Some(id))?;
            if !unique.insert(id) {
                return Err(integrity());
            }
            Ok(id)
        })
        .collect()
}
fn check_prefix(prefix: &str, maximum: usize) -> Result<(), Status> {
    if prefix.len() > maximum
        || !prefix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(Status::invalid_argument(
            "invalid native hexadecimal prefix",
        ));
    }
    Ok(())
}
fn hex_matches(bytes: &[u8], prefix: &str) -> bool {
    prefix.bytes().enumerate().all(|(index, expected)| {
        let byte = bytes[index / 2];
        let nibble = if index % 2 == 0 { byte >> 4 } else { byte & 15 };
        expected == b"0123456789abcdef"[nibble as usize]
    })
}
fn unix_seconds() -> Result<i64, Status> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|value| i64::try_from(value.as_secs()).ok())
        .ok_or_else(|| Status::failed_precondition("native query clock unavailable"))
}
