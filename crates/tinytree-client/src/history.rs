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
/// A bounded composition of checked exact-path queries, not a single server
/// reply or a portable continuation token. `result().revision` is the original
/// selection; each contributing RPC was checked against its own start revision.
#[derive(Clone)]
pub struct CheckedPathHistoryCollection(Arc<OwnedHistory<HistoryCollection>>);
struct HistoryCollection {
    value: q::PathHistoryResult,
    _scope: CheckedScopeHandle,
}
impl CheckedPathHistoryCollection {
    pub fn result(&self) -> &q::PathHistoryResult {
        &self.0.value.value
    }
}
impl std::fmt::Debug for CheckedPathHistoryCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedPathHistoryCollection")
            .field("entries", &self.result().entries.len())
            .field("coverage", &self.result().coverage)
            .finish_non_exhaustive()
    }
}
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
        if !matches!(
            policy,
            q::HistoryPolicy::LinearFixedPathV1 | q::HistoryPolicy::JjResolvedFixedPathV1
        ) || !valid_path(path)
        {
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
    /// Collect exact fixed-path history within one checked live/cloud scope.
    /// Only a positive-progress scan boundary can start another query. This
    /// does not resume after scope expiry, relocate paths, or continue blame.
    /// Server read/work caps remain per RPC, not aggregate byte-budget claims.
    pub async fn collect_path_history(
        &self,
        scope: &CheckedScopeHandle,
        revision: CommitId,
        path: &str,
        policy: q::HistoryPolicy,
        max_commits: u32,
    ) -> Result<CheckedPathHistoryCollection, Status> {
        self.history_input(scope, path, policy, max_commits)?;
        // Keep this explicit even if the one-reply API later gains a policy
        // that follows renames: a boundary alone cannot prove relocated paths.
        if max_commits == 0 || policy != q::HistoryPolicy::LinearFixedPathV1 {
            return Err(Status::invalid_argument(
                "collection requires a positive exact-path scan bound",
            ));
        }
        let deadline = self.deadline();
        let client = self.clone().with_operation_deadline(deadline);
        // Fresh clones below contain at most three 32-byte IDs per entry.
        // Charge vector capacity and both progress/entry hash tables before
        // allocating; decoded pages keep their separate measured owners until
        // copying finishes. Idle aggregate storage cannot consume RPC headroom.
        let retained = (max_commits as usize)
            .checked_mul(1024)
            .and_then(|n| path.len().checked_mul(2).and_then(|p| n.checked_add(p)))
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(integrity)?;
        if retained > self.inner.config.limits.max_batch_bytes.saturating_mul(2) {
            return Err(Status::resource_exhausted(
                "history collection allocation exceeds budget",
            ));
        }
        let amount = u32::try_from(retained).map_err(|_| integrity())?;
        let state = self
            .inner
            .query_state
            .clone()
            .try_acquire_many_owned(amount)
            .map_err(|_| {
                Status::resource_exhausted("history collection exhausts query headroom")
            })?;
        let bytes = self
            .inner
            .bytes
            .clone()
            .try_acquire_many_owned(amount)
            .map_err(|_| Status::resource_exhausted("history collection exhausts byte headroom"))?;
        let memory = HistoryMemory {
            _guards: self.guards.clone(),
            _state: state,
            _bytes: bytes,
        };
        let mut result = q::PathHistoryResult {
            scope_digest: scope.content_digest().to_vec(),
            index_epoch: scope.index_epoch().to_vec(),
            revision: Some(q::CommitId {
                value: revision.0.to_vec(),
            }),
            path: path.to_owned(),
            policy: policy as i32,
            coverage: q::HistoryCoverage::HistoryScanLimit as i32,
            scanned_commits: 0,
            entries: Vec::with_capacity(max_commits as usize),
            boundary: Some(q::CommitId {
                value: revision.0.to_vec(),
            }),
        };
        let mut starts = HashSet::with_capacity(max_commits as usize);
        let mut entries = HashSet::with_capacity(max_commits as usize);
        let mut start = revision;
        starts.insert(start);
        loop {
            if Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(
                    "history collection deadline exceeded",
                ));
            }
            let remaining = max_commits
                .checked_sub(result.scanned_commits)
                .filter(|n| *n != 0)
                .ok_or_else(integrity)?;
            let page = client
                .path_history(scope, start, path, policy, remaining)
                .await?;
            let value = page.result();
            let scanned = result
                .scanned_commits
                .checked_add(value.scanned_commits)
                .filter(|n| *n <= max_commits)
                .ok_or_else(integrity)?;
            let boundary = value
                .boundary
                .as_ref()
                .map(|value| id(Some(value)))
                .transpose()?;
            for entry in &value.entries {
                let entry_id = id(entry.commit.as_ref())?;
                if boundary == Some(entry_id)
                    || (entry_id != start && starts.contains(&entry_id))
                    || !entries.insert(entry_id)
                {
                    return Err(integrity());
                }
            }
            let more = value.coverage == q::HistoryCoverage::HistoryScanLimit as i32;
            if more {
                let next = boundary.ok_or_else(integrity)?;
                if value.scanned_commits == 0
                    || next == CommitId::ROOT
                    || entries.contains(&next)
                    || !starts.insert(next)
                {
                    return Err(integrity());
                }
            }
            // Clone bounded-length fields, not retained capacities from decoded
            // overwritten protobuf fields. The aggregate reservation predates it.
            result.entries.extend(value.entries.iter().cloned());
            result.scanned_commits = scanned;
            result.coverage = value.coverage;
            result.boundary = value.boundary.clone();
            drop(page);
            if !more || scanned == max_commits {
                break;
            }
            start = boundary.ok_or_else(integrity)?;
        }
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded(
                "history collection deadline exceeded",
            ));
        }
        Ok(CheckedPathHistoryCollection(Arc::new(OwnedHistory {
            value: HistoryCollection {
                value: result,
                _scope: scope.clone(),
            },
            _memory: memory,
        })))
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
        if policy == q::HistoryPolicy::JjResolvedFixedPathV1 {
            let capability = self
                .clone()
                .with_operation_deadline(deadline)
                .resolved_blame_capability()
                .await?
                .ok_or_else(|| {
                    Status::unimplemented("server resolved-blame capability unavailable")
                })?;
            if !capability.authorized {
                return Err(Status::permission_denied(
                    "resolved blame is not authorized",
                ));
            }
            if !capability.supported {
                return Err(Status::unimplemented(
                    "server resolved-blame capability unavailable",
                ));
            }
        }
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

#[path = "history/annotation.rs"]
mod annotation;
pub use annotation::{CheckedAnnotationFrontier, CheckedAnnotationInput};
