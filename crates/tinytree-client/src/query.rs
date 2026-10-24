//! Checked immutable query scopes with private canonical draft overlays. Exact negative/completeness answers trust the
//! authenticated server; canonical IDs cannot prove that results were not omitted.
use super::*;
use crate::VerifiedRetainedVersion;
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
    query_identity_version: u32,
    cloud_source: Option<Arc<CloudScopeSource>>,
}
#[derive(Clone)]
struct CloudScopeSource {
    wire: q::CloudSnapshotSource,
    native_binding: [u8; 32],
    selected: Vec<[u8; 32]>,
    client_identity: std::sync::Weak<Inner>,
}
impl std::fmt::Debug for CloudScopeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudScopeSource")
            .field("selected", &self.selected)
            .finish_non_exhaustive()
    }
}
impl PartialEq for CloudScopeSource {
    fn eq(&self, other: &Self) -> bool {
        self.wire == other.wire
            && self.native_binding == other.native_binding
            && self.selected == other.selected
            && self.client_identity.ptr_eq(&other.client_identity)
    }
}
impl Eq for CloudScopeSource {}
impl CheckedScopeHandle {
    pub fn query_identity_version(&self) -> u32 {
        self.query_identity_version
    }
    /// Derive an exact v2 identity from caller-provided randomness. The binding
    /// is public replay provenance, not a remote authorization credential.
    pub fn bound_query_id(&self, nonce: [u8; 16]) -> Result<[u8; 32], Status> {
        if self.query_identity_version != 2 {
            return Err(Status::invalid_argument(
                "scope does not use bound query identities",
            ));
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-query-scope-id-v2\0");
        hash.update(&(self.repository.len() as u64).to_be_bytes());
        hash.update(self.repository.as_bytes());
        hash.update(&self.handle);
        hash.update(&nonce);
        let mut id = [0; 32];
        id[..16].copy_from_slice(&nonce);
        id[16..].copy_from_slice(&hash.finalize().as_bytes()[..16]);
        Ok(id)
    }
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
#[derive(Clone, Debug, PartialEq)]
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
    /// Membership in the original bounded expression, not a fetched history.
    Membership {
        expression: q::GraphExpr,
        candidate: CommitId,
    },
}
// The protobuf graph AST contains only discrete (non-floating-point) values.
impl Eq for QueryFact {}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FactAnswer {
    HasId(bool),
    IsAncestor(bool),
    Heads(Vec<CommitId>),
    CommonAncestors(Vec<CommitId>),
    Membership(bool),
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
        if scope
            .cloud_source
            .as_ref()
            .is_some_and(|source| !source.client_identity.ptr_eq(&Arc::downgrade(&self.inner)))
        {
            return Err(Status::failed_precondition(
                "cloud query scope belongs to another client",
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
        self.install_query_scope(roots.into(), overlay, None, 1, None)
            .await
    }
    /// Explicitly negotiate scope-bound query identities; never downgrade.
    pub async fn open_query_scope_v2(
        &self,
        roots: Vec<CommitId>,
    ) -> Result<CheckedScopeHandle, Status> {
        self.open_query_scope_with_overlay_v2(roots, Vec::new())
            .await
    }
    pub async fn open_query_scope_with_overlay_v2(
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
        self.install_query_scope(roots.into(), overlay, None, 2, None)
            .await
    }
    /// Open an explicitly retained portable-index snapshot. Expected anchors are
    /// local verification inputs, not roots granted on the wire. This requires
    /// server support and never falls back to an ordinary root-based scope.
    pub async fn open_cloud_query_scope(
        &self,
        version: &VerifiedRetainedVersion,
        mut selected_index_roots: Vec<[u8; 32]>,
        mut expected_anchors: Vec<CommitId>,
        overlay: Vec<VerifiedMetadata>,
    ) -> Result<CheckedScopeHandle, Status> {
        version.check_query_client(self)?;
        self.batch(selected_index_roots.len())?;
        self.batch(expected_anchors.len())?;
        if selected_index_roots.is_empty() || expected_anchors.is_empty() {
            return Err(Status::invalid_argument(
                "cloud query scope requires selected roots and expected anchors",
            ));
        }
        selected_index_roots.sort_unstable();
        if selected_index_roots.windows(2).any(|ids| ids[0] == ids[1]) {
            return Err(Status::invalid_argument("duplicate cloud query index root"));
        }
        expected_anchors.sort_unstable();
        expected_anchors.dedup();
        self.install_query_scope(
            expected_anchors.into(),
            overlay,
            None,
            2,
            Some((version, selected_index_roots)),
        )
        .await
    }
    /// Shares the parent's immutable remote checkpoint and private graph, adding
    /// only these parent-first drafts. Forks cannot extend the parent's expiry.
    pub async fn fork_query_scope(
        &self,
        parent: &CheckedScopeHandle,
        overlay: Vec<VerifiedMetadata>,
    ) -> Result<CheckedScopeHandle, Status> {
        self.checked_scope(parent, false)?;
        self.install_query_scope(
            parent.remote_roots.clone(),
            overlay,
            Some(parent),
            parent.query_identity_version,
            None,
        )
        .await
    }
    async fn install_query_scope(
        &self,
        roots: Arc<[CommitId]>,
        overlay: Vec<VerifiedMetadata>,
        parent: Option<&CheckedScopeHandle>,
        identity_version: u32,
        cloud: Option<(&VerifiedRetainedVersion, Vec<[u8; 32]>)>,
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
        let source_bytes = cloud
            .as_ref()
            .map_or(0, |(_, ids)| ids.len().saturating_mul(128))
            .checked_add(roots.len().saturating_mul(128))
            .ok_or_else(|| {
                Status::resource_exhausted("query scope source memory exceeds budget")
            })?;
        let memory = memory.checked_add(source_bytes).ok_or_else(|| {
            Status::resource_exhausted("query scope source memory exceeds budget")
        })?;
        let budget = self.admit(deadline, memory).await?;
        let cloud_source = if let Some((version, selected)) = cloud {
            let (selector, authority) = version.query_source_parts(self)?;
            Some(Arc::new(CloudScopeSource {
                wire: q::CloudSnapshotSource {
                    selector: Some(selector),
                    authority: Some(authority),
                    index_root_ids: selected.iter().map(|id| id.to_vec()).collect(),
                },
                native_binding: version.binding_digest(),
                selected,
                client_identity: Arc::downgrade(&self.inner),
            }))
        } else {
            parent.and_then(|parent| parent.cloud_source.clone())
        };
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
            version: identity_version,
            repository_id: self.inner.config.repository_id.clone(),
            replay_key: replay_key.to_vec(),
            roots: if cloud_source.is_some() {
                Vec::new()
            } else {
                roots.iter().copied().map(wire_commit).collect()
            },
            cloud_source: if parent.is_none() {
                cloud_source.as_ref().map(|source| source.wire.clone())
            } else {
                None
            },
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
            let mut grpc = self.rpc_client(budget.clone());
            let cloud_source = cloud_source.clone();
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
                } else if cloud_source.is_some() {
                    grpc.open_cloud_scope(
                        self.request(
                            scope_frames(header, overlay, budget, frame_bytes, done),
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
                if (identity_version == 2 && scope.query_identity_version != 2)
                    || (identity_version == 1 && !matches!(scope.query_identity_version, 0 | 1))
                    || key.repository_id != self.inner.config.repository_id
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
                let epoch: [u8; 16] = scope
                    .index_epoch
                    .as_slice()
                    .try_into()
                    .map_err(|_| integrity())?;
                if let Some(source) = &cloud_source {
                    let expected = tinytree_model::cloud_query_source_binding(
                        source.native_binding,
                        &source.selected,
                        &roots,
                        epoch,
                    );
                    if scope.cloud_source_binding.as_slice() != expected {
                        return Err(integrity());
                    }
                } else if !scope.cloud_source_binding.is_empty() {
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
                    query_identity_version: identity_version,
                    cloud_source,
                })
            }
        })
        .await
    }
    pub async fn close_query_scope(&self, scope: &CheckedScopeHandle) -> Result<(), Status> {
        self.checked_scope(scope, true)?;
        let deadline = self.deadline();
        let budget = self.admit(deadline, self.unary_memory()).await?;
        let request = q::CloseScopeRequest {
            scope: Some(scope.key()),
        };
        self.bounded(&request)?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.rpc_client(budget.clone());
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
        let mut request_bytes = 256usize;
        let mut input_ids = 0usize;
        let mut expression_nodes = 0usize;
        for fact in &facts {
            let (count, bytes) = match fact {
                QueryFact::Membership { expression, .. } => {
                    let (nodes, bytes, literals) =
                        validate_expression(expression, &self.inner.config.limits)?;
                    expression_nodes = expression_nodes.checked_add(nodes).ok_or_else(|| {
                        Status::resource_exhausted("query expression nodes exceed budget")
                    })?;
                    self.batch(expression_nodes)?;
                    (
                        literals.checked_add(1).ok_or_else(|| {
                            Status::resource_exhausted("query input IDs exceed budget")
                        })?,
                        bytes.checked_add(96).ok_or_else(|| {
                            Status::resource_exhausted("query request exceeds budget")
                        })?,
                    )
                }
                QueryFact::HasId(_) => (1, 72),
                QueryFact::IsAncestor { .. } => (2, 112),
                QueryFact::Heads(ids) => (
                    ids.len(),
                    ids.len()
                        .checked_mul(40)
                        .and_then(|n| n.checked_add(32))
                        .ok_or_else(|| {
                            Status::resource_exhausted("query request exceeds budget")
                        })?,
                ),
                QueryFact::CommonAncestors { left, right } => {
                    let count = left.len().checked_add(right.len()).ok_or_else(|| {
                        Status::resource_exhausted("query input IDs exceed budget")
                    })?;
                    (
                        count,
                        count
                            .checked_mul(40)
                            .and_then(|n| n.checked_add(32))
                            .ok_or_else(|| {
                                Status::resource_exhausted("query request exceeds budget")
                            })?,
                    )
                }
            };
            input_ids = input_ids
                .checked_add(count)
                .ok_or_else(|| Status::resource_exhausted("query input IDs exceed budget"))?;
            self.batch(input_ids)?;
            request_bytes = request_bytes
                .checked_add(bytes)
                .ok_or_else(|| Status::resource_exhausted("query request exceeds budget"))?;
            if request_bytes > self.inner.config.limits.max_request_bytes {
                return Err(Status::resource_exhausted("query request exceeds budget"));
            }
        }
        let deadline = self.deadline();
        let budget = self.admit(deadline, self.unary_memory()).await?;
        let mut wire = Vec::with_capacity(facts.len());
        for fact in &facts {
            use q::graph_fact::Fact;
            let fact = match fact {
                QueryFact::Membership {
                    expression,
                    candidate,
                } => Fact::Membership(q::Membership {
                    expression: Some(expression.clone()),
                    candidate: Some(wire_commit(*candidate)),
                }),
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
            let mut grpc = self.rpc_client(budget.clone());
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
                            (QueryFact::Membership { .. }, Some(Answer::Membership(value))) => {
                                Ok(FactAnswer::Membership(value))
                            }
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
    fn prefix_selection_digest(expression: &q::GraphExpr) -> [u8; 32] {
        tinytree_model::query_prefix_selection_digest(&expression.encode_to_vec())
    }
    fn checked_selection_digest(
        actual: Option<&[u8]>,
        expected: Option<[u8; 32]>,
    ) -> Result<(), Status> {
        if actual != expected.as_ref().map(|v| v.as_slice()) {
            return Err(integrity());
        }
        Ok(())
    }
    pub async fn resolve_commit_prefix(
        &self,
        scope: &CheckedScopeHandle,
        prefix: &str,
    ) -> Result<CommitPrefix, Status> {
        self.resolve_commit_prefix_inner(scope, prefix, None).await
    }
    /// Resolve only within the complete supplied structural selection.
    pub async fn resolve_commit_prefix_selected(
        &self,
        scope: &CheckedScopeHandle,
        prefix: &str,
        selection: &q::GraphExpr,
    ) -> Result<CommitPrefix, Status> {
        self.resolve_commit_prefix_inner(scope, prefix, Some(selection))
            .await
    }
    async fn resolve_commit_prefix_inner(
        &self,
        scope: &CheckedScopeHandle,
        prefix: &str,
        selection: Option<&q::GraphExpr>,
    ) -> Result<CommitPrefix, Status> {
        self.checked_selection(selection, 256)?;
        self.checked_scope(scope, false)?;
        check_prefix(prefix, 64)?;
        let deadline = self.deadline();
        let budget = self.admit(deadline, self.unary_memory()).await?;
        let expected_selection = selection.map(Self::prefix_selection_digest);
        let request = q::ResolveCommitPrefixRequest {
            scope: Some(scope.key()),
            hex_prefix: prefix.into(),
            selection: selection.cloned(),
        };
        self.bounded(&request)?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.rpc_client(budget.clone());
            let request = request.clone();
            async move {
                use q::commit_prefix_result::Result as R;
                let reply = grpc
                    .resolve_commit_prefix(self.request(request, allowance))
                    .await?
                    .into_inner();
                Self::checked_selection_digest(
                    reply.selection_digest.as_deref(),
                    expected_selection,
                )?;
                match reply.result {
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
        visible_heads: Vec<CommitId>,
    ) -> Result<ChangePrefix, Status> {
        self.resolve_change_prefix_inner(scope, prefix, visible_heads, None)
            .await
    }
    /// Return the selected targets of one distinct change; targets outside the
    /// selection are omitted, while explicit visible-head flags are preserved.
    pub async fn resolve_change_prefix_selected(
        &self,
        scope: &CheckedScopeHandle,
        prefix: &str,
        visible_heads: Vec<CommitId>,
        selection: &q::GraphExpr,
    ) -> Result<ChangePrefix, Status> {
        self.resolve_change_prefix_inner(scope, prefix, visible_heads, Some(selection))
            .await
    }
    async fn resolve_change_prefix_inner(
        &self,
        scope: &CheckedScopeHandle,
        prefix: &str,
        mut visible_heads: Vec<CommitId>,
        selection: Option<&q::GraphExpr>,
    ) -> Result<ChangePrefix, Status> {
        self.checked_selection(
            selection,
            visible_heads
                .len()
                .checked_mul(40)
                .and_then(|n| n.checked_add(256))
                .ok_or_else(|| {
                    Status::resource_exhausted("selected prefix request exceeds budget")
                })?,
        )?;
        self.checked_scope(scope, false)?;
        check_prefix(prefix, 32)?;
        self.batch(visible_heads.len())?;
        visible_heads.sort_unstable();
        visible_heads.dedup();
        let deadline = self.deadline();
        let budget = self.admit(deadline, self.unary_memory()).await?;
        let expected_selection = selection.map(Self::prefix_selection_digest);
        let request = q::ResolveChangePrefixRequest {
            scope: Some(scope.key()),
            hex_prefix: prefix.into(),
            visible_heads: visible_heads.into_iter().map(wire_commit).collect(),
            selection: selection.cloned(),
        };
        self.bounded(&request)?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.rpc_client(budget.clone());
            let request = request.clone();
            async move {
                use q::change_prefix_result::Result as R;
                let reply = grpc
                    .resolve_change_prefix(self.request(request, allowance))
                    .await?
                    .into_inner();
                Self::checked_selection_digest(
                    reply.selection_digest.as_deref(),
                    expected_selection,
                )?;
                match reply.result {
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
        visible_heads: Vec<CommitId>,
    ) -> Result<u32, Status> {
        self.shortest_prefix_inner(scope, target, visible_heads, None)
            .await?
            .ok_or_else(integrity)
    }
    /// Selected absence is explicit and lets the native caller decide whether
    /// to fall back to its complete repository, never a fabricated full length.
    pub async fn shortest_prefix_selected(
        &self,
        scope: &CheckedScopeHandle,
        target: PrefixTarget,
        visible_heads: Vec<CommitId>,
        selection: &q::GraphExpr,
    ) -> Result<Option<u32>, Status> {
        self.shortest_prefix_inner(scope, target, visible_heads, Some(selection))
            .await
    }
    fn checked_selection(
        &self,
        selection: Option<&q::GraphExpr>,
        other_bytes: usize,
    ) -> Result<(), Status> {
        if let Some(expression) = selection {
            let (_, bytes, _) = validate_expression(expression, &self.inner.config.limits)?;
            if bytes
                .checked_add(other_bytes)
                .is_none_or(|n| n > self.inner.config.limits.max_request_bytes)
            {
                return Err(Status::resource_exhausted(
                    "selected prefix request exceeds budget",
                ));
            }
        }
        Ok(())
    }
    async fn shortest_prefix_inner(
        &self,
        scope: &CheckedScopeHandle,
        target: PrefixTarget,
        mut visible_heads: Vec<CommitId>,
        selection: Option<&q::GraphExpr>,
    ) -> Result<Option<u32>, Status> {
        self.checked_selection(
            selection,
            visible_heads
                .len()
                .checked_mul(40)
                .and_then(|n| n.checked_add(256))
                .ok_or_else(|| {
                    Status::resource_exhausted("selected prefix request exceeds budget")
                })?,
        )?;
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
        let budget = self.admit(deadline, self.unary_memory()).await?;
        let expected_selection = selection.map(Self::prefix_selection_digest);
        let request = q::ShortestPrefixRequest {
            scope: Some(scope.key()),
            target: Some(target),
            visible_heads: visible_heads.into_iter().map(wire_commit).collect(),
            selection: selection.cloned(),
        };
        self.bounded(&request)?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self.rpc_client(budget.clone());
            let request = request.clone();
            async move {
                let reply = grpc
                    .shortest_prefix(self.request(request, allowance))
                    .await?
                    .into_inner();
                Self::checked_selection_digest(
                    reply.selection_digest.as_deref(),
                    expected_selection,
                )?;
                if reply.kind != kind as i32 || reply.hex_length > maximum {
                    return Err(integrity());
                }
                if selection.is_some() {
                    match reply.in_selection {
                        Some(true) => Ok(Some(reply.hex_length)),
                        Some(false) if reply.hex_length == 0 => Ok(None),
                        _ => Err(integrity()),
                    }
                } else if reply.in_selection.is_none() {
                    Ok(Some(reply.hex_length))
                } else {
                    Err(integrity())
                }
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

/// A scoped IDS-only evaluation. The original operation deadline starts in
/// `begin_query`, including time held by the caller between pages. No inflight
/// permit is held between calls. Drop releases reserved cumulative-state memory.
pub struct CheckedIdsQuery {
    client: NativeClient,
    scope: CheckedScopeHandle,
    expression: Arc<q::GraphExpr>,
    query_id: [u8; 32],
    page_size: u32,
    deadline: Instant,
    cursor: Vec<u8>,
    sequence: u64,
    pages: usize,
    row_capacity: usize,
    seen: HashSet<CommitId>,
    exhausted: bool,
    begin_guards: crate::verified::LifetimeGuards,
    cleanup_guards: crate::verified::LifetimeGuards,
    close_guards: crate::verified::LifetimeGuards,
    cleanup_armed: bool,
    closed: bool,
    memory: Option<Arc<QueryMemory>>,
}
impl std::fmt::Debug for CheckedIdsQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedIdsQuery")
            .field("pages", &self.pages)
            .field("rows", &self.seen.len())
            .field("exhausted", &self.exhausted)
            .finish_non_exhaustive()
    }
}
impl CheckedIdsQuery {
    pub fn query_id(&self) -> &[u8; 32] {
        &self.query_id
    }
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }
    /// Closed means cancelled/released locally, not proven EOF.
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    fn release_cleanup_guards(&mut self) {
        self.begin_guards = Default::default();
        self.cleanup_guards = Default::default();
        self.close_guards = Default::default();
        self.client.guards = Default::default();
    }
    fn release_closed_state(&mut self) {
        self.seen = HashSet::new();
        self.cursor = Vec::new();
        self.expression = Arc::new(q::GraphExpr::default());
        self.memory.take();
    }
}
impl Drop for CheckedIdsQuery {
    fn drop(&mut self) {
        if self.cleanup_armed {
            self.release_closed_state();
            // Cleanup admission is nonblocking and precedes request allocation.
            // A full queue or byte pool falls back to server-side expiry.
            let Ok(queued_state) = self
                .client
                .inner
                .query_state
                .clone()
                .try_acquire_many_owned(1024)
            else {
                return;
            };
            let Ok(queued_bytes) = self.client.inner.bytes.clone().try_acquire_many_owned(1024)
            else {
                return;
            };
            let _ = self.client.inner.query_cleanup.try_send(QueryCleanup {
                scope: self.scope.key(),
                query_id: self.query_id,
                deadline: Instant::now()
                    + Duration::from_millis(self.client.inner.config.query_cleanup_timeout_ms),
                guards: CleanupGuards {
                    _begin: self.begin_guards.clone(),
                    _attempt: self.cleanup_guards.clone(),
                    _current: self.close_guards.clone(),
                },
                queued_state: Some(queued_state),
                queued_bytes: Some(queued_bytes),
            });
        }
    }
}
struct QueryMemory {
    _guards: crate::verified::LifetimeGuards,
    _bytes: OwnedSemaphorePermit,
    _state: OwnedSemaphorePermit,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IdRow {
    pub commit: CommitId,
    pub change: ChangeId,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckedIdsPage {
    pub sequence: u64,
    pub rows: Vec<IdRow>,
    pub exhausted: bool,
}
impl CheckedIdsPage {
    pub fn is_progress(&self) -> bool {
        self.rows.is_empty() && !self.exhausted
    }
}
impl NativeClient {
    /// Prepare a versioned structural query without fetching history. The input
    /// AST is borrowed, checked iteratively, then compactly cloned once. GRAPH
    /// and CANONICAL projections are deliberately absent from this API.
    /// Preparation is local: before any Evaluate is accepted, closing the scope
    /// may make the first page unavailable. Only server-established queries and
    /// their unclosed final-page replay survive logical scope closure.
    pub async fn begin_query(
        &self,
        scope: &CheckedScopeHandle,
        expression: &q::GraphExpr,
        page_size: u32,
    ) -> Result<CheckedIdsQuery, Status> {
        let mut id = [0; 32];
        getrandom::fill(&mut id)
            .map_err(|_| Status::internal("cannot generate evaluation identity"))?;
        if scope.query_identity_version == 2 {
            id = scope.bound_query_id(id[..16].try_into().unwrap())?;
        }
        self.begin_query_with_id(scope, expression, page_size, id)
            .await
    }
    /// Prepare with the exact provided identity. V2 identities must already be
    /// bound to this scope; this method never transforms caller bytes.
    pub async fn begin_query_with_id(
        &self,
        scope: &CheckedScopeHandle,
        expression: &q::GraphExpr,
        page_size: u32,
        query_id: [u8; 32],
    ) -> Result<CheckedIdsQuery, Status> {
        if scope.query_identity_version == 2
            && scope.bound_query_id(query_id[..16].try_into().unwrap())? != query_id
        {
            return Err(Status::invalid_argument(
                "query identity belongs to another scope",
            ));
        }
        let deadline = self.deadline();
        self.checked_scope(scope, false)?;
        if page_size == 0
            || page_size as usize > self.inner.config.limits.max_batch_items
            || page_size > 4096
        {
            return Err(Status::invalid_argument("invalid IDS query page size"));
        }
        let limits = &self.inner.config.limits;
        let (nodes, encoded, _) = validate_expression(expression, limits)?;
        // Membership intersections inherit their literal bound. This lets a
        // selected canonical read or template membership check coexist with its
        // still-live outer query without changing graph traversal reservations.
        let row_capacity = expression_row_capacity(expression, limits.max_graph_objects);
        let bytes = row_capacity
            .checked_mul(128)
            .and_then(|n| encoded.checked_mul(4).and_then(|e| n.checked_add(e)))
            .and_then(|n| nodes.checked_mul(256).and_then(|e| n.checked_add(e)))
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| Status::resource_exhausted("IDS query state exceeds memory budget"))?;
        if bytes
            .checked_add(query_headroom(limits))
            .is_none_or(|n| n > limits.max_buffered_bytes)
        {
            return Err(Status::resource_exhausted(
                "IDS query state leaves no page admission budget",
            ));
        }
        let state = reapi::policy::timeout(deadline, async {
            self.inner
                .query_state
                .clone()
                .acquire_many_owned(bytes as u32)
                .await
                .map_err(|_| Status::cancelled("native client closed"))
        })
        .await?;
        let reservation = reapi::policy::timeout(deadline, async {
            self.inner
                .bytes
                .clone()
                .acquire_many_owned(bytes as u32)
                .await
                .map_err(|_| Status::cancelled("native client closed"))
        })
        .await?;
        let memory = Arc::new(QueryMemory {
            _state: state,
            _bytes: reservation,
            _guards: self.guards.clone(),
        });
        Ok(CheckedIdsQuery {
            client: self.without_rpc_guards(),
            scope: scope.clone(),
            expression: Arc::new(expression.clone()),
            query_id,
            page_size,
            deadline,
            cursor: Vec::new(),
            sequence: 0,
            pages: 0,
            row_capacity,
            seen: HashSet::new(),
            exhausted: false,
            begin_guards: self.guards.clone(),
            cleanup_guards: self.guards.clone(),
            close_guards: self.guards.clone(),
            cleanup_armed: false,
            closed: false,
            memory: Some(memory),
        })
    }
    /// Fetch exactly one page/owned work quantum. Empty progress pages are
    /// visible to callers; transport retries replay identical inputs. On errors
    /// no cursor, sequence or duplicate-tracking state advances. Ordering and
    /// completeness are authenticated server guarantees, not provable from IDs.
    pub async fn evaluate_page(
        &self,
        query: &mut CheckedIdsQuery,
    ) -> Result<CheckedIdsPage, Status> {
        if !Arc::ptr_eq(&self.inner, &query.client.inner) {
            return Err(Status::invalid_argument(
                "IDS query belongs to another client",
            ));
        }
        if query.closed {
            return Err(Status::failed_precondition("IDS query is closed"));
        }
        if query.exhausted {
            return Err(Status::failed_precondition("IDS query already exhausted"));
        }
        // An accepted server query retains its immutable scope after CloseScope;
        // only its original deadline (and server query expiry) govern continuations.
        if query.pages == 0 {
            self.checked_scope(&query.scope, false)?;
        }
        if query.pages >= self.inner.config.limits.max_graph_pages {
            return Err(Status::resource_exhausted(
                "IDS query page budget exhausted",
            ));
        }
        let budget = self.admit(query.deadline, self.unary_memory()).await?;
        let request = q::EvaluateRequest {
            scope: Some(query.scope.key()),
            query_id: query.query_id.to_vec(),
            expression: Some((*query.expression).clone()),
            projection: q::Projection::Ids as i32,
            page_size: query.page_size,
            cursor: query.cursor.clone(),
        };
        self.bounded(&request)?;
        let owner = Arc::new((
            query.expression.clone(),
            query.memory.as_ref().ok_or_else(integrity)?.clone(),
            budget,
        ));
        let reply = self
            .retry(query.deadline, false, |allowance| {
                query.cleanup_armed = true;
                query.cleanup_guards = self.guards.clone();
                let mut grpc = self.inner.grpc.clone();
                let request = request.clone();
                let owner = owner.clone();
                async move {
                    grpc.evaluate(self.request(request, allowance), owner)
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await?;
        if reply.scope_digest.as_slice() != query.scope.digest
            || reply.index_epoch.as_slice() != query.scope.epoch
            || reply.sequence != query.sequence
            || reply.rows.len() > query.page_size as usize
            || reply.cursor.len() > 4096
        {
            return Err(integrity());
        }
        let exhausted = reply.exhausted.ok_or_else(integrity)?;
        if exhausted != reply.cursor.is_empty() || (!exhausted && reply.cursor == query.cursor) {
            return Err(integrity());
        }
        if query
            .seen
            .len()
            .checked_add(reply.rows.len())
            .is_none_or(|n| n > query.row_capacity)
        {
            return Err(Status::resource_exhausted("IDS query row budget exhausted"));
        }
        let mut page_ids = HashSet::with_capacity(reply.rows.len());
        let mut rows = Vec::with_capacity(reply.rows.len());
        for row in reply.rows {
            if !row.edges.is_empty() || row.canonical.is_some() {
                return Err(integrity());
            }
            let commit = checked_commit(row.commit)?;
            let change = ChangeId(
                row.change
                    .ok_or_else(integrity)?
                    .value
                    .try_into()
                    .map_err(|_| integrity())?,
            );
            if query.seen.contains(&commit) || !page_ids.insert(commit) {
                return Err(integrity());
            }
            rows.push(IdRow { commit, change });
        }
        if query
            .seen
            .len()
            .checked_add(rows.len())
            .is_none_or(|n| n > query.row_capacity)
        {
            return Err(Status::resource_exhausted("IDS query row budget exhausted"));
        }
        let next_sequence = query.sequence.checked_add(1).ok_or_else(integrity)?;
        query.seen.extend(page_ids);
        query.cursor = reply.cursor;
        query.sequence = next_sequence;
        query.pages += 1;
        query.exhausted = exhausted;
        Ok(CheckedIdsPage {
            sequence: reply.sequence,
            rows,
            exhausted,
        })
    }
}

/// A checked selected-graph query. Its original deadline includes caller pauses.
pub struct CheckedGraphQuery {
    core: CheckedIdsQuery,
    canonical: bool,
    edges: usize,
}
impl std::fmt::Debug for CheckedGraphQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedGraphQuery")
            .field("core", &self.core)
            .field("canonical", &self.canonical)
            .finish_non_exhaustive()
    }
}
impl CheckedGraphQuery {
    pub fn query_id(&self) -> &[u8; 32] {
        self.core.query_id()
    }
    pub fn is_exhausted(&self) -> bool {
        self.core.is_exhausted()
    }
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
    }
}
/// Ordered selected-set edges; repeated targets are intentional and preserved.
/// Indirect and missing edges are not canonical parent summaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphEdge {
    Direct(CommitId),
    Indirect(CommitId),
    Missing(CommitId),
}
pub struct GraphRow {
    pub commit: CommitId,
    pub change: ChangeId,
    edges: Vec<GraphEdge>,
    pub canonical: Option<VerifiedMetadata>,
    // Field order keeps quota and guards alive through payload destruction.
    _memory: Option<Arc<GraphPageMemory>>,
}
impl GraphRow {
    pub fn edges(&self) -> &[GraphEdge] {
        &self.edges
    }
}
impl std::fmt::Debug for GraphRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphRow")
            .field("commit", &self.commit)
            .field("change", &self.change)
            .field("edges", &self.edges)
            .field("canonical", &self.canonical.is_some())
            .finish()
    }
}
struct GraphPageMemory {
    _guards: crate::verified::LifetimeGuards,
    _state: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
pub struct CheckedGraphPage {
    pub sequence: u64,
    rows: Vec<GraphRow>,
    pub exhausted: bool,
    _memory: Arc<GraphPageMemory>,
}
/// Owned rows keep the original allocation charged until its iterator buffer
/// is destroyed, including after the last row has been yielded.
pub struct CheckedGraphRows {
    rows: std::vec::IntoIter<GraphRow>,
    _memory: Arc<GraphPageMemory>,
}
impl Iterator for CheckedGraphRows {
    type Item = GraphRow;
    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}
impl ExactSizeIterator for CheckedGraphRows {}
impl std::iter::FusedIterator for CheckedGraphRows {}
impl std::fmt::Debug for CheckedGraphPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedGraphPage")
            .field("sequence", &self.sequence)
            .field("rows", &self.rows)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}
impl CheckedGraphPage {
    pub fn rows(&self) -> &[GraphRow] {
        &self.rows
    }
    pub fn into_rows(self) -> CheckedGraphRows {
        CheckedGraphRows {
            rows: self.rows.into_iter(),
            _memory: self._memory,
        }
    }
    pub fn is_progress(&self) -> bool {
        self.rows.is_empty() && !self.exhausted
    }
}
impl NativeClient {
    /// Begin GRAPH, or CANONICAL when requested, under the same bounded replay
    /// and deadline rules as IDS. No history is fetched during preparation.
    pub async fn begin_graph_query(
        &self,
        scope: &CheckedScopeHandle,
        expression: &q::GraphExpr,
        page_size: u32,
        canonical: bool,
    ) -> Result<CheckedGraphQuery, Status> {
        Ok(CheckedGraphQuery {
            core: self.begin_query(scope, expression, page_size).await?,
            canonical,
            edges: 0,
        })
    }
    pub async fn begin_graph_query_with_id(
        &self,
        scope: &CheckedScopeHandle,
        expression: &q::GraphExpr,
        page_size: u32,
        canonical: bool,
        query_id: [u8; 32],
    ) -> Result<CheckedGraphQuery, Status> {
        Ok(CheckedGraphQuery {
            core: self
                .begin_query_with_id(scope, expression, page_size, query_id)
                .await?,
            canonical,
            edges: 0,
        })
    }
    /// Verify one complete selected-graph page before advancing any query state.
    /// Canonical hashes authenticate rows, not server completeness or indirect
    /// ancestry. This never downloads FILE objects or hydrates full history.
    pub async fn evaluate_graph_page(
        &self,
        query: &mut CheckedGraphQuery,
    ) -> Result<CheckedGraphPage, Status> {
        let core = &mut query.core;
        if !Arc::ptr_eq(&self.inner, &core.client.inner) {
            return Err(Status::invalid_argument(
                "graph query belongs to another client",
            ));
        }
        if core.closed {
            return Err(Status::failed_precondition("graph query is closed"));
        }
        if core.exhausted {
            return Err(Status::failed_precondition("graph query already exhausted"));
        }
        if core.pages == 0 {
            self.checked_scope(&core.scope, false)?;
        }
        let limits = &self.inner.config.limits;
        // Bound owner-vector duplication before allocating any page guards.
        // This fixed allowance covers page/iterator/Arc control scaffolding,
        // including empty progress pages that otherwise contain no row bytes.
        const PAGE_OWNER_BYTES: usize = 512;
        let guard_count = core
            .memory
            .as_ref()
            .ok_or_else(integrity)?
            ._guards
            .len()
            .checked_add(self.guards.len())
            .ok_or_else(|| Status::resource_exhausted("graph page guard budget exhausted"))?;
        if guard_count
            .checked_mul(std::mem::size_of::<Arc<dyn std::any::Any + Send + Sync>>())
            .and_then(|n| n.checked_add(PAGE_OWNER_BYTES))
            .is_none_or(|n| n > limits.max_batch_bytes)
        {
            return Err(Status::resource_exhausted(
                "graph page guard budget exhausted",
            ));
        }
        if core.pages >= limits.max_graph_pages {
            return Err(Status::resource_exhausted(
                "graph query page budget exhausted",
            ));
        }
        let mut budget = self.admit(core.deadline, self.unary_memory()).await?;
        // Detach retained-page bytes, not its inflight slot. Physical transport
        // and canonical workers retain this reservation through Budget guards;
        // completed pages retain it independently until dropped.
        let admitted = Arc::get_mut(&mut budget).ok_or_else(integrity)?;
        let window = Arc::new(std::sync::Mutex::new(
            admitted
                ._bytes
                .split(limits.max_batch_bytes)
                .ok_or_else(integrity)?,
        ));
        let mut guards = admitted._guards.as_ref().clone();
        guards.push(window.clone());
        admitted._guards = Arc::new(guards);
        let request = q::EvaluateRequest {
            scope: Some(core.scope.key()),
            query_id: core.query_id.to_vec(),
            expression: Some((*core.expression).clone()),
            projection: if query.canonical {
                q::Projection::Canonical
            } else {
                q::Projection::Graph
            } as i32,
            page_size: core.page_size,
            cursor: core.cursor.clone(),
        };
        self.bounded(&request)?;
        let owner = Arc::new((
            core.expression.clone(),
            core.memory.as_ref().ok_or_else(integrity)?.clone(),
            budget.clone(),
        ));
        let reply = self
            .retry(core.deadline, false, |allowance| {
                core.cleanup_armed = true;
                core.cleanup_guards = self.guards.clone();
                let mut grpc = self.inner.grpc.clone();
                let request = request.clone();
                let owner = owner.clone();
                async move {
                    grpc.evaluate(self.request(request, allowance), owner)
                        .await
                        .map(|r| r.into_inner())
                }
            })
            .await?;
        if reply.scope_digest.as_slice() != core.scope.digest
            || reply.index_epoch.as_slice() != core.scope.epoch
            || reply.sequence != core.sequence
            || reply.rows.len() > core.page_size as usize
            || reply.cursor.len() > 4096
        {
            return Err(integrity());
        }
        let exhausted = reply.exhausted.ok_or_else(integrity)?;
        if exhausted != reply.cursor.is_empty() || (!exhausted && reply.cursor == core.cursor) {
            return Err(integrity());
        }
        if core
            .seen
            .len()
            .checked_add(reply.rows.len())
            .is_none_or(|n| n > core.row_capacity)
        {
            return Err(Status::resource_exhausted(
                "graph query row budget exhausted",
            ));
        }
        let mut page_ids = HashSet::with_capacity(reply.rows.len());
        let mut rows = Vec::with_capacity(reply.rows.len());
        let mut edge_count = query.edges;
        let mut retained = reply
            .rows
            .len()
            .checked_mul(std::mem::size_of::<GraphRow>())
            .and_then(|n| n.checked_add(PAGE_OWNER_BYTES))
            .ok_or_else(integrity)?;
        for row in reply.rows {
            let commit = checked_commit(row.commit)?;
            let change = ChangeId(
                row.change
                    .ok_or_else(integrity)?
                    .value
                    .try_into()
                    .map_err(|_| integrity())?,
            );
            if core.seen.contains(&commit) || !page_ids.insert(commit) {
                return Err(integrity());
            }
            edge_count = edge_count
                .checked_add(row.edges.len())
                .ok_or_else(integrity)?;
            if edge_count > limits.max_query_edges {
                return Err(Status::resource_exhausted(
                    "graph query edge budget exhausted",
                ));
            }
            retained = retained
                .checked_add(
                    row.edges
                        .len()
                        .saturating_mul(std::mem::size_of::<GraphEdge>()),
                )
                .ok_or_else(integrity)?;
            let mut edges = Vec::with_capacity(row.edges.len());
            for edge in row.edges {
                use q::selected_edge::Edge;
                edges.push(match edge.edge.ok_or_else(integrity)? {
                    Edge::Direct(e) => GraphEdge::Direct(checked_commit(e.target)?),
                    Edge::Indirect(e) => GraphEdge::Indirect(checked_commit(e.target)?),
                    Edge::Missing(e) => GraphEdge::Missing(checked_commit(e.boundary)?),
                });
            }
            let canonical = match row.canonical {
                None if !query.canonical || commit == CommitId::ROOT => None,
                Some(data) if query.canonical && commit != CommitId::ROOT => {
                    let descriptor =
                        parse_descriptor(data.descriptor.as_ref().ok_or_else(integrity)?)?;
                    if descriptor.key.kind != Kind::Commit
                        || descriptor.key.id.0 != commit.0
                        || descriptor.size_bytes != data.canonical_envelope.len() as u64
                        || data.canonical_envelope.len() > limits.max_metadata_bytes
                    {
                        return Err(integrity());
                    }
                    let available = limits
                        .max_batch_bytes
                        .checked_sub(retained)
                        .and_then(|n| {
                            n.checked_sub(data.canonical_envelope.len().saturating_mul(3))
                        })
                        .and_then(|n| n.checked_sub(std::mem::size_of::<tinytree_model::Commit>()))
                        .ok_or_else(|| {
                            Status::resource_exhausted(
                                "graph canonical allocation budget exhausted",
                            )
                        })?;
                    let items = (available / 128).min(tinytree_model::Limits::default().max_items);
                    if items == 0 {
                        return Err(Status::resource_exhausted(
                            "graph canonical allocation budget exhausted",
                        ));
                    }
                    // Reuse the page's admission: public verify_metadata would
                    // deadlock with max_inflight=1 and renew the operation deadline.
                    let metadata = verify_metadata(
                        descriptor,
                        data.canonical_envelope,
                        limits.max_metadata_bytes,
                        Some(items),
                        core.deadline,
                        budget.clone(),
                    )
                    .await?;
                    let Object::Commit(decoded) = &metadata.object else {
                        return Err(integrity());
                    };
                    let parent_memory = decoded
                        .parents
                        .len()
                        .checked_mul(64)
                        .ok_or_else(integrity)?;
                    if retained
                        .checked_add(metadata.bytes.capacity())
                        .and_then(|n| n.checked_add(commit_heap(decoded)))
                        .and_then(|n| n.checked_add(parent_memory))
                        .is_none_or(|n| n > limits.max_batch_bytes)
                    {
                        return Err(Status::resource_exhausted(
                            "graph canonical parent allocation budget exhausted",
                        ));
                    }
                    let parents: HashSet<_> = decoded.parents.iter().copied().collect();
                    if decoded.change_id != change
                        || edges.iter().any(
                            |edge| matches!(edge, GraphEdge::Direct(id) if !parents.contains(id)),
                        )
                    {
                        return Err(integrity());
                    }
                    retained = retained
                        .checked_add(metadata.bytes.capacity())
                        .and_then(|n| n.checked_add(commit_heap(decoded)))
                        .ok_or_else(integrity)?;
                    Some(metadata)
                }
                _ => return Err(integrity()),
            };
            if retained > limits.max_batch_bytes {
                return Err(Status::resource_exhausted(
                    "graph page allocation budget exhausted",
                ));
            }
            if commit == CommitId::ROOT && (change != ChangeId::ROOT || !edges.is_empty()) {
                return Err(integrity());
            }
            rows.push(GraphRow {
                _memory: None,
                commit,
                change,
                edges,
                canonical,
            });
        }
        if core
            .seen
            .len()
            .checked_add(rows.len())
            .is_none_or(|n| n > core.row_capacity)
        {
            return Err(Status::resource_exhausted(
                "graph query row budget exhausted",
            ));
        }
        // Application-held pages share the idle-query headroom pool. Never
        // wait while holding page admission: a held earlier page may itself be
        // the owner of those bytes. Failure leaves the replay cursor unchanged.
        let mut result_guards = Vec::with_capacity(guard_count);
        result_guards.extend(
            core.memory
                .as_ref()
                .ok_or_else(integrity)?
                ._guards
                .iter()
                .cloned(),
        );
        result_guards.extend(self.guards.iter().cloned());
        retained = retained
            .checked_add(
                result_guards
                    .capacity()
                    .checked_mul(std::mem::size_of::<Arc<dyn std::any::Any + Send + Sync>>())
                    .ok_or_else(integrity)?,
            )
            .filter(|n| *n <= limits.max_batch_bytes)
            .ok_or_else(|| {
                Status::resource_exhausted("graph page owner allocation budget exhausted")
            })?;
        let state = self
            .inner
            .query_state
            .clone()
            .try_acquire_many_owned(retained as u32)
            .map_err(|_| {
                Status::resource_exhausted("retained graph pages exhaust query headroom")
            })?;
        let memory = Arc::new(GraphPageMemory {
            _bytes: window
                .lock()
                .map_err(|_| integrity())?
                .split(retained)
                .ok_or_else(integrity)?,
            _state: state,
            _guards: Arc::new(result_guards),
        });
        for row in &mut rows {
            row._memory = Some(memory.clone());
            if let Some(metadata) = &mut row.canonical {
                metadata._query_owner = Some(memory.clone());
            }
        }
        let sequence = core.sequence.checked_add(1).ok_or_else(integrity)?;
        core.seen.extend(page_ids);
        core.cursor = reply.cursor;
        core.sequence = sequence;
        core.pages += 1;
        core.exhausted = exhausted;
        query.edges = edge_count;
        Ok(CheckedGraphPage {
            _memory: memory,
            sequence: reply.sequence,
            rows,
            exhausted,
        })
    }
}

pub(super) const CLEANUP_MEMORY: usize = 4 * crate::transport::QUERY_CLOSE_CAP + 2048;

// Queue items retain resource owners, never the AST, QueryMemory or inflight
// slot. Deadline includes queue residence. A single worker owns only transport
// and semaphore clones, so dropping the last Inner closes/drains the channel.
struct CleanupGuards {
    _begin: crate::verified::LifetimeGuards,
    _attempt: crate::verified::LifetimeGuards,
    _current: crate::verified::LifetimeGuards,
}
struct CleanupOwner {
    _guards: CleanupGuards,
    _queued_state: Option<OwnedSemaphorePermit>,
    _queued_bytes: Option<OwnedSemaphorePermit>,
}
pub(super) struct QueryCleanup {
    scope: q::ScopeKey,
    query_id: [u8; 32],
    deadline: Instant,
    guards: CleanupGuards,
    queued_state: Option<OwnedSemaphorePermit>,
    queued_bytes: Option<OwnedSemaphorePermit>,
}
pub(super) struct CleanupWorker {
    grpc: RepositoryClient,
    token: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    policy: reapi::policy::Policy,
    inflight: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    rpc_guards: crate::verified::LifetimeGuards,
}
impl CleanupWorker {
    pub(super) fn new(client: &NativeClient) -> Self {
        Self {
            grpc: client.inner.grpc.clone(),
            token: client.inner.token.clone(),
            policy: client.inner.config.rpc.clone(),
            inflight: client.inner.inflight.clone(),
            bytes: client.inner.bytes.clone(),
            rpc_guards: client.rpc_guards.clone(),
        }
    }
    pub(super) async fn run(self, mut receiver: tokio::sync::mpsc::Receiver<QueryCleanup>) {
        while let Some(item) = receiver.recv().await {
            let _ = self.close(item).await;
        }
    }
    async fn close(&self, item: QueryCleanup) -> Result<(), Status> {
        let deadline = item.deadline;
        // Close-specific raw/decompressed cap + encoded/decoded overhead and
        // fixed owner/request scaffolding. Never ordinary page-sized admission.
        let mut guards: Vec<Arc<dyn std::any::Any + Send + Sync>> = vec![Arc::new(CleanupOwner {
            _guards: item.guards,
            _queued_state: item.queued_state,
            _queued_bytes: item.queued_bytes,
        })];
        // Only this explicit physical close attempt sees request owners. The
        // queued item/uncertain-query guards remain persistent-only.
        guards.extend(self.rpc_guards.iter().cloned());
        let guards = Arc::new(guards);
        let inflight = reapi::policy::timeout(deadline, async {
            self.inflight
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Status::cancelled("native client closed"))
        })
        .await?;
        let bytes = reapi::policy::timeout(deadline, async {
            self.bytes
                .clone()
                .acquire_many_owned(CLEANUP_MEMORY as u32)
                .await
                .map_err(|_| Status::cancelled("native client closed"))
        })
        .await?;
        let owner = Arc::new(Budget {
            _inflight: inflight,
            _bytes: bytes,
            _guards: guards,
        });
        let request = q::CloseQueryRequest {
            scope: Some(item.scope),
            query_id: item.query_id.to_vec(),
        };
        self.policy
            .retry(deadline, self.policy.rpc_timeout(), |allowance| {
                let mut grpc = self.grpc.clone();
                let mut request = Request::new(request.clone());
                request.set_timeout(allowance);
                if let Some(token) = &self.token {
                    request
                        .metadata_mut()
                        .insert("authorization", token.clone());
                }
                let owner = owner.clone();
                async move { grpc.close_query(request, owner).await.map(|_| ()) }
            })
            .await
    }
}
impl NativeClient {
    /// Close/release a query without claiming EOF. Cleanup has its own bounded
    /// deadline, never extends query execution, and can be retried after an
    /// uncertain ACK. Drop is bounded best effort; expiry is the fallback.
    pub async fn close_ids_query(&self, query: &mut CheckedIdsQuery) -> Result<(), Status> {
        if !Arc::ptr_eq(&self.inner, &query.client.inner) {
            return Err(Status::invalid_argument("query belongs to another client"));
        }
        query.closed = true;
        query.release_closed_state();
        if !query.cleanup_armed {
            query.release_cleanup_guards();
            return Ok(());
        }
        let item = QueryCleanup {
            scope: query.scope.key(),
            query_id: query.query_id,
            deadline: Instant::now()
                + Duration::from_millis(self.inner.config.query_cleanup_timeout_ms),
            guards: CleanupGuards {
                _begin: query.begin_guards.clone(),
                _attempt: query.cleanup_guards.clone(),
                _current: self.guards.clone(),
            },
            queued_state: None,
            queued_bytes: None,
        };
        // Preserve current owners for an uncertain close's Drop retry too.
        query.close_guards = self.guards.clone();
        CleanupWorker::new(self).close(item).await?;
        query.cleanup_armed = false;
        query.release_cleanup_guards();
        Ok(())
    }
    pub async fn close_graph_query(&self, query: &mut CheckedGraphQuery) -> Result<(), Status> {
        self.close_ids_query(&mut query.core).await
    }
}

/// Called only after full expression validation bounds depth to 64 and the
/// aggregate node count. This borrowed walk allocates nothing and never rewrites
/// evaluation, so even zero-capacity results preserve strict literal errors.
fn expression_row_capacity(expression: &q::GraphExpr, maximum: usize) -> usize {
    match expression.expression.as_ref() {
        Some(q::graph_expr::Expression::Commits(set)) => set.commits.len().min(maximum),
        Some(q::graph_expr::Expression::Intersection(set)) => set
            .inputs
            .iter()
            .map(|child| expression_row_capacity(child, maximum))
            .min()
            .unwrap_or(maximum),
        _ => maximum,
    }
}

fn validate_expression(
    expression: &q::GraphExpr,
    limits: &crate::Limits,
) -> Result<(usize, usize, usize), Status> {
    let invalid = || Status::invalid_argument("invalid versioned graph expression");
    let exceeded = || Status::resource_exhausted("graph expression exceeds client bounds");
    let mut pending = vec![(expression, 1usize)];
    let mut nodes = 0usize;
    let mut literals = 0usize;
    while let Some((node, depth)) = pending.pop() {
        nodes += 1;
        if depth > 64 || nodes > limits.max_batch_items {
            return Err(exceeded());
        }
        if node.version != 1 {
            return Err(invalid());
        }
        use q::graph_expr::Expression as E;
        let mut children = Vec::new();
        match node.expression.as_ref().ok_or_else(invalid)? {
            E::Commits(set) => {
                literals = literals
                    .checked_add(set.commits.len())
                    .ok_or_else(exceeded)?;
                if literals > limits.max_batch_items
                    || set.commits.iter().any(|id| id.value.len() != 32)
                {
                    return Err(invalid());
                }
            }
            E::Ancestors(value) => {
                generation_range(value.generations.as_ref())?;
                parent_range(value.parents.as_ref())?;
                children.push(value.heads.as_deref().ok_or_else(invalid)?);
            }
            E::Range(value) => {
                generation_range(value.generations.as_ref())?;
                parent_range(value.parents.as_ref())?;
                children.push(value.roots.as_deref().ok_or_else(invalid)?);
                children.push(value.heads.as_deref().ok_or_else(invalid)?);
            }
            E::DagRange(value) => {
                generation_range(value.generations_from_roots.as_ref())?;
                children.push(value.roots.as_deref().ok_or_else(invalid)?);
                children.push(value.heads.as_deref().ok_or_else(invalid)?);
            }
            E::Heads(value) | E::Roots(value) | E::HasConflict(value) => {
                children.push(value.input.as_deref().ok_or_else(invalid)?)
            }
            E::Union(value) | E::Intersection(value) => {
                if value.inputs.len() > limits.max_batch_items {
                    return Err(exceeded());
                }
                children.extend(value.inputs.iter());
            }
            E::Difference(value) => {
                children.push(value.left.as_deref().ok_or_else(invalid)?);
                children.push(value.right.as_deref().ok_or_else(invalid)?);
            }
            E::Latest(value) => {
                if value.count > limits.max_graph_objects as u64 {
                    return Err(exceeded());
                }
                children.push(value.candidates.as_deref().ok_or_else(invalid)?);
            }
        }
        if nodes
            .checked_add(pending.len())
            .and_then(|n| n.checked_add(children.len()))
            .is_none_or(|n| n > limits.max_batch_items)
        {
            return Err(exceeded());
        }
        pending.extend(children.into_iter().map(|child| (child, depth + 1)));
    }
    let bytes = expression.encoded_len();
    if bytes > limits.max_request_bytes {
        return Err(exceeded());
    }
    Ok((nodes, bytes, literals))
}
fn generation_range(range: Option<&q::GenerationRange>) -> Result<(), Status> {
    if range.is_some_and(|range| range.end_exclusive.is_some_and(|end| end < range.start)) {
        return Err(Status::invalid_argument("invalid generation interval"));
    }
    Ok(())
}
fn parent_range(range: Option<&q::ParentRange>) -> Result<(), Status> {
    if range.is_some_and(|range| range.end_exclusive.is_some_and(|end| end < range.start)) {
        return Err(Status::invalid_argument("invalid parent interval"));
    }
    Ok(())
}
