//! Checked fixed-path history. The authenticated server supplies path association
//! and completeness; canonical FILE verification is not an ancestry proof.
use super::*;
use crate::CheckedScopeHandle;
use std::sync::Mutex;
use tinytree_model::CommitId;
use tinytree_proto::query_v1 as q;

struct HistoryMemory {
    _guards: crate::verified::LifetimeGuards,
    _state: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
struct OwnedHistory<T> {
    // Release payload before its independent retained-allocation owner.
    value: T,
    _memory: HistoryMemory,
}
#[derive(Clone)]
pub struct CheckedPathHistory(Arc<OwnedHistory<q::PathHistoryResult>>);
#[derive(Clone)]
pub struct CheckedBlame(Arc<OwnedHistory<q::BlameResult>>);
impl CheckedPathHistory {
    pub fn result(&self) -> &q::PathHistoryResult {
        &self.0.value
    }
}
impl CheckedBlame {
    pub fn result(&self) -> &q::BlameResult {
        &self.0.value
    }
}
impl std::fmt::Debug for CheckedPathHistory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedPathHistory")
            .field("entries", &self.result().entries.len())
            .field("coverage", &self.result().coverage)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for CheckedBlame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedBlame")
            .field("ranges", &self.result().ranges.len())
            .field("coverage", &self.result().coverage)
            .finish_non_exhaustive()
    }
}
fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && !path.bytes().any(|b| b == 0 || b == b'\\')
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}
fn id(value: Option<&q::CommitId>) -> Result<CommitId, Status> {
    Ok(CommitId(
        value
            .ok_or_else(integrity)?
            .value
            .as_slice()
            .try_into()
            .map_err(|_| integrity())?,
    ))
}
fn source(
    scope: &CheckedScopeHandle,
    revision: CommitId,
    path: &str,
    policy: q::HistoryPolicy,
    digest: &[u8],
    epoch: &[u8],
    actual_revision: Option<&q::CommitId>,
    actual_path: &str,
    actual_policy: i32,
    coverage: i32,
    boundary: Option<&q::CommitId>,
    scanned: u32,
    maximum: u32,
) -> Result<bool, Status> {
    if digest != scope.content_digest()
        || epoch != scope.index_epoch()
        || id(actual_revision)? != revision
        || actual_path != path
        || actual_policy != policy as i32
        || (maximum != 0 && scanned > maximum)
        || q::HistoryCoverage::try_from(coverage).is_err()
        || coverage == 0
    {
        return Err(integrity());
    }
    let complete = coverage == q::HistoryCoverage::HistoryComplete as i32;
    if complete != boundary.is_none() {
        return Err(integrity());
    }
    if boundary.is_some() {
        id(boundary)?;
    }
    Ok(complete)
}
fn state(value: Option<&q::HistoryPathState>) -> Result<usize, Status> {
    let value = value.ok_or_else(integrity)?;
    match q::HistoryPathKind::try_from(value.kind).map_err(|_| integrity())? {
        q::HistoryPathKind::HistoryPathAbsent
            if value.native_id.is_empty() && value.size_bytes == 0 && !value.executable => {}
        q::HistoryPathKind::HistoryPathFile if value.native_id.len() == 32 => {}
        q::HistoryPathKind::HistoryPathSymlink
            if value.native_id.len() == 32 && !value.executable => {}
        q::HistoryPathKind::HistoryPathTree
            if value.native_id.len() == 32 && value.size_bytes == 0 && !value.executable => {}
        _ => return Err(integrity()),
    }
    Ok(value.native_id.capacity())
}
fn footprint_header(
    digest: &Vec<u8>,
    epoch: &Vec<u8>,
    revision: Option<&q::CommitId>,
    path: &String,
    boundary: Option<&q::CommitId>,
) -> usize {
    digest.capacity()
        + epoch.capacity()
        + path.capacity()
        + revision.map_or(0, |v| v.value.capacity())
        + boundary.map_or(0, |v| v.value.capacity())
        + 1024
}
impl NativeClient {
    fn history_input(
        &self,
        scope: &CheckedScopeHandle,
        path: &str,
        policy: q::HistoryPolicy,
        maximum: u32,
    ) -> Result<(), Status> {
        self.checked_scope(scope, false)?;
        if policy != q::HistoryPolicy::LinearFixedPathV1 || !valid_path(path) {
            return Err(Status::invalid_argument(
                "invalid fixed-path history request",
            ));
        }
        if maximum as usize > self.inner.config.limits.max_graph_objects {
            return Err(Status::resource_exhausted(
                "history scan exceeds client budget",
            ));
        }
        Ok(())
    }
    async fn history_budget(
        &self,
        deadline: Instant,
    ) -> Result<(Arc<Budget>, Arc<Mutex<OwnedSemaphorePermit>>), Status> {
        let mut budget = self.admit(deadline, self.unary_memory()).await?;
        let admitted = Arc::get_mut(&mut budget).ok_or_else(integrity)?;
        // Codec preflight allows at most twice the encoded cap for collection
        // allocation. Keep that and the encoded message charged through physical
        // decoder teardown; returned payloads detach only their measured share.
        let bytes = self
            .inner
            .config
            .limits
            .max_batch_bytes
            .checked_mul(3)
            .ok_or_else(integrity)?;
        let window = Arc::new(Mutex::new(
            admitted._bytes.split(bytes).ok_or_else(integrity)?,
        ));
        let mut guards = admitted._guards.as_ref().clone();
        guards.push(window.clone());
        admitted._guards = Arc::new(guards);
        Ok((budget, window))
    }
    fn retain_history<T>(
        &self,
        value: T,
        retained: usize,
        window: &Mutex<OwnedSemaphorePermit>,
    ) -> Result<Arc<OwnedHistory<T>>, Status> {
        if retained > self.inner.config.limits.max_batch_bytes.saturating_mul(2) {
            return Err(Status::resource_exhausted(
                "history result allocation exceeds budget",
            ));
        }
        // Do not wait while holding RPC admission: earlier application-held
        // results may own the same headroom. No inflight/RPC guard is retained.
        let state = self
            .inner
            .query_state
            .clone()
            .try_acquire_many_owned(retained as u32)
            .map_err(|_| {
                Status::resource_exhausted("retained history results exhaust query headroom")
            })?;
        let bytes = window
            .lock()
            .map_err(|_| integrity())?
            .split(retained)
            .ok_or_else(integrity)?;
        Ok(Arc::new(OwnedHistory {
            value,
            _memory: HistoryMemory {
                _guards: self.guards.clone(),
                _state: state,
                _bytes: bytes,
            },
        }))
    }
    /// Query one immutable fixed path without downloading historical FILEs.
    /// An incomplete result is not resumable pagination or a complete history.
    pub async fn path_history(
        &self,
        scope: &CheckedScopeHandle,
        revision: CommitId,
        path: &str,
        policy: q::HistoryPolicy,
        max_commits: u32,
    ) -> Result<CheckedPathHistory, Status> {
        self.history_input(scope, path, policy, max_commits)?;
        let deadline = self.deadline();
        let (budget, window) = self.history_budget(deadline).await?;
        let request = q::PathHistoryRequest {
            scope: Some(scope.key()),
            revision: Some(q::CommitId {
                value: revision.0.to_vec(),
            }),
            path: path.into(),
            policy: policy as i32,
            max_commits,
        };
        self.bounded(&request)?;
        let reply = self
            .retry(deadline, false, |allowance| {
                let mut grpc = self.rpc_client(budget.clone());
                let request = request.clone();
                async move {
                    grpc.path_history(self.request(request, allowance))
                        .await
                        .map(|v| v.into_inner())
                }
            })
            .await?;
        let captured = scope.clone();
        let path = path.to_owned();
        let limits = self.inner.config.limits.clone();
        let worker_owner = budget.clone();
        let (reply, retained, _owner) = tokio::time::timeout_at(
            deadline,
            tokio::task::spawn_blocking(move || {
                let retained = (|| {
                    source(
                        &captured,
                        revision,
                        &path,
                        policy,
                        &reply.scope_digest,
                        &reply.index_epoch,
                        reply.revision.as_ref(),
                        &reply.path,
                        reply.policy,
                        reply.coverage,
                        reply.boundary.as_ref(),
                        reply.scanned_commits,
                        if max_commits == 0 {
                            limits.max_graph_objects as u32
                        } else {
                            max_commits
                        },
                    )?;
                    if reply.entries.len() > reply.scanned_commits as usize
                        || reply.entries.len() > limits.max_graph_objects
                    {
                        return Err(integrity());
                    }
                    let mut retained = footprint_header(
                        &reply.scope_digest,
                        &reply.index_epoch,
                        reply.revision.as_ref(),
                        &reply.path,
                        reply.boundary.as_ref(),
                    ) + reply.entries.capacity()
                        * std::mem::size_of::<q::PathHistoryEntry>();
                    let mut seen = HashSet::with_capacity(reply.entries.len());
                    for entry in &reply.entries {
                        let commit = id(entry.commit.as_ref())?;
                        if commit == CommitId::ROOT
                            || !seen.insert(commit)
                            || entry.state == entry.parent_state
                        {
                            return Err(integrity());
                        }
                        retained += entry.commit.as_ref().unwrap().value.capacity()
                            + state(entry.state.as_ref())?
                            + state(entry.parent_state.as_ref())?;
                    }
                    Ok(retained)
                })();
                (reply, retained, worker_owner)
            }),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("history validation deadline exceeded"))?
        .map_err(|_| Status::internal("history validation worker failed"))?;
        Ok(CheckedPathHistory(
            self.retain_history(reply, retained?, &window)?,
        ))
    }
    /// Verify target bytes and complete/partial line partition. Origins and
    /// completeness are authenticated assertions, not proofs from FILE hashes.
    pub async fn blame(
        &self,
        scope: &CheckedScopeHandle,
        revision: CommitId,
        path: &str,
        policy: q::HistoryPolicy,
        max_commits: u32,
    ) -> Result<CheckedBlame, Status> {
        self.history_input(scope, path, policy, max_commits)?;
        let deadline = self.deadline();
        let (budget, window) = self.history_budget(deadline).await?;
        let request = q::BlameRequest {
            scope: Some(scope.key()),
            revision: Some(q::CommitId {
                value: revision.0.to_vec(),
            }),
            path: path.into(),
            policy: policy as i32,
            max_commits,
        };
        self.bounded(&request)?;
        let reply = self
            .retry(deadline, false, |allowance| {
                let mut grpc = self.rpc_client(budget.clone());
                let request = request.clone();
                async move {
                    grpc.blame(self.request(request, allowance))
                        .await
                        .map(|v| v.into_inner())
                }
            })
            .await?;
        let captured = scope.clone();
        let path = path.to_owned();
        let limits = self.inner.config.limits.clone();
        let worker_owner = budget.clone();
        let (reply, retained, _owner) = tokio::time::timeout_at(
            deadline,
            tokio::task::spawn_blocking(move || {
                let retained = (|| {
                    let complete = source(
                        &captured,
                        revision,
                        &path,
                        policy,
                        &reply.scope_digest,
                        &reply.index_epoch,
                        reply.revision.as_ref(),
                        &reply.path,
                        reply.policy,
                        reply.coverage,
                        reply.boundary.as_ref(),
                        reply.scanned_commits,
                        if max_commits == 0 {
                            limits.max_graph_objects as u32
                        } else {
                            max_commits
                        },
                    )?;
                    let mut retained = footprint_header(
                        &reply.scope_digest,
                        &reply.index_epoch,
                        reply.revision.as_ref(),
                        &reply.path,
                        reply.boundary.as_ref(),
                    ) + reply.target_bytes.capacity()
                        + reply.ranges.capacity() * std::mem::size_of::<q::BlameRange>();
                    let Some(target) = &reply.target else {
                        if complete || !reply.target_bytes.is_empty() || !reply.ranges.is_empty() {
                            return Err(integrity());
                        }
                        return Ok(retained);
                    };
                    let target = parse_descriptor(target)?;
                    if target.key.kind != Kind::File
                        || target.size_bytes != reply.target_bytes.len() as u64
                        || target.size_bytes > limits.max_file_bytes
                        || tinytree_model::describe_file(&reply.target_bytes) != target
                    {
                        return Err(integrity());
                    }
                    let descriptor = reply.target.as_ref().unwrap();
                    retained += descriptor.content_blake3.capacity()
                        + descriptor.key.as_ref().unwrap().native_id.capacity();
                    let lines = reply.target_bytes.split_inclusive(|b| *b == b'\n').count();
                    if reply.ranges.len() > limits.max_query_edges || lines > u32::MAX as usize {
                        return Err(integrity());
                    }
                    let mut next = 0;
                    let mut unresolved = false;
                    for range in &reply.ranges {
                        if range.start_line != next
                            || range.end_line <= range.start_line
                            || range.end_line as usize > lines
                        {
                            return Err(integrity());
                        }
                        next = range.end_line;
                        if let Some(origin) = &range.origin {
                            if id(Some(origin))? == CommitId::ROOT
                                || range
                                    .original_line
                                    .checked_add(range.end_line - range.start_line)
                                    .is_none()
                            {
                                return Err(integrity());
                            }
                            retained += origin.value.capacity();
                        } else {
                            if range.original_line != 0 {
                                return Err(integrity());
                            }
                            unresolved = true;
                        }
                    }
                    if next as usize != lines || (complete && unresolved) {
                        return Err(integrity());
                    }
                    Ok(retained)
                })();
                (reply, retained, worker_owner)
            }),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("blame validation deadline exceeded"))?
        .map_err(|_| Status::internal("blame validation worker failed"))?;
        Ok(CheckedBlame(
            self.retain_history(reply, retained?, &window)?,
        ))
    }
}
