//! Checked root-only query scopes. Exact negative/completeness answers trust the
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
}
impl CheckedScopeHandle {
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
    /// Opens an immutable root-only scope. Overlays, forks and evaluation are not
    /// implemented by this client API; this is not an offload/backup receipt.
    pub async fn open_query_scope(
        &self,
        mut roots: Vec<CommitId>,
    ) -> Result<CheckedScopeHandle, Status> {
        let deadline = self.deadline();
        self.batch(roots.len())?;
        roots.sort_unstable();
        roots.dedup();
        if roots.is_empty() {
            return Err(Status::invalid_argument("query scope requires roots"));
        }
        let _budget = self.admit(deadline, self.unary_memory()).await?;
        let mut replay_key = [0u8; 32];
        getrandom::fill(&mut replay_key)
            .map_err(|_| Status::internal("cannot generate query replay identity"))?;
        let header = q::ScopeHeader {
            version: 1,
            repository_id: self.inner.config.repository_id.clone(),
            replay_key: replay_key.to_vec(),
            roots: roots.into_iter().map(wire_commit).collect(),
            overlay_object_count: 0,
            overlay_bytes: 0,
        };
        self.bounded(&header)?;
        self.retry(deadline, true, |allowance| {
            let mut grpc = self.inner.grpc.clone();
            let header = header.clone();
            let budget = _budget.clone();
            async move {
                let frames = scope_frames(header, budget);
                let scope = grpc
                    .open_scope(self.request(frames, allowance))
                    .await?
                    .into_inner();
                let key = scope.key.ok_or_else(integrity)?;
                if key.repository_id != self.inner.config.repository_id
                    || scope.expires_at_unix_seconds <= unix_seconds()?
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
fn scope_frames(
    header: q::ScopeHeader,
    budget: Arc<Budget>,
) -> impl tonic::codegen::tokio_stream::Stream<Item = q::OpenScopeFrame> + Send + 'static {
    async_stream::stream! {
        // Tonic may retain the request body after its response waiter cancels.
        let _budget = budget;
        yield q::OpenScopeFrame { frame: Some(q::open_scope_frame::Frame::Header(header)) };
        yield q::OpenScopeFrame { frame: Some(q::open_scope_frame::Frame::ScopeEnd(q::ScopeEnd { overlay_object_count: 0, overlay_bytes: 0 })) };
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
