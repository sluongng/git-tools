//! Authenticated preparation/status assertions, not check execution or proof of
//! canonical candidate contents. Import canonical objects before trusting those
//! contents. There is deliberately no caller verdict or finalize operation.
use super::*;
use crate::transport::Shape;
use serde::{Deserialize, Serialize};
use std::sync::Weak;
use tinytree_model::{ChangeId, CommitId, TreeId};
use tinytree_proto::landing_v1 as l;

// Pinned Tonic uses zstd's streaming default window limit (log27). Reserve
// 128MiB window plus 64MiB context/block/encoder headroom; payloads are charged
// separately. This is a stack-specific conservative allowance, not an arbitrary
// decoder/RSS bound. Revisit with the advertised-window tests on stack upgrades.
pub(crate) const ZSTD_WORKSPACE_BYTES: usize = 192 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LandingLimits {
    pub max_response_bytes: usize,
    pub max_rewrites: usize,
    pub max_conflict_path_bytes: usize,
}
impl Default for LandingLimits {
    fn default() -> Self {
        Self {
            max_response_bytes: 128 * 1024,
            max_rewrites: 128,
            max_conflict_path_bytes: 4096,
        }
    }
}
impl LandingLimits {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if !(4096..=1024 * 1024).contains(&self.max_response_bytes)
            || !(1..=128).contains(&self.max_rewrites)
            || !(1..=64 * 1024).contains(&self.max_conflict_path_bytes)
        {
            return Err("invalid landing client limits");
        }
        Ok(())
    }
}
/// Persist this original intent before Start. The server selects committer,
/// expiry and check execution; retries must reuse these exact input fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LandingStart {
    pub idempotency_key: String,
    /// The intrinsic root is allowed only here, not as either head.
    pub source_base: CommitId,
    pub source_head: CommitId,
    pub destination: String,
    pub expected_generation: u64,
    pub expected_head: CommitId,
    pub profile_name: String,
    pub expected_profile_version: String,
}
impl LandingStart {
    fn validate(&self) -> Result<(), Status> {
        if self.idempotency_key.is_empty()
            || self.idempotency_key.len() > 128
            || !self
                .idempotency_key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
            || self.source_head == CommitId::ROOT
            || self.expected_head == CommitId::ROOT
            || self.destination.is_empty()
            || self.destination.len() > 512
            || self.destination.contains('\0')
            || self.destination == "_tinytree"
            || self.destination.starts_with("_tinytree/")
            || self.profile_name.is_empty()
            || self.profile_name.len() > 64
            || !self
                .profile_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
            || self.expected_profile_version.is_empty()
            || self.expected_profile_version.len() > 128
            || self.expected_profile_version.contains('\0')
        {
            return Err(Status::invalid_argument("invalid landing intent"));
        }
        Ok(())
    }
    fn wire(&self, repository: &str) -> l::StartLandingRequest {
        l::StartLandingRequest {
            repository_id: repository.into(),
            idempotency_key: self.idempotency_key.clone(),
            source_base: self.source_base.0.to_vec(),
            source_head: self.source_head.0.to_vec(),
            destination: self.destination.clone(),
            expected_generation: self.expected_generation,
            expected_head: self.expected_head.0.to_vec(),
            profile_name: self.profile_name.clone(),
            expected_profile_version: self.expected_profile_version.clone(),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LandingPhase {
    Intent,
    PreparedPendingChecks,
    Checked,
    Committed,
    Conflict,
    Unsupported,
    CheckFailed,
    StaleTarget,
    Expired,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LandingPreparation {
    ProfileConfigured,
    ProfileUnavailable,
    PreparationDisabled,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LandingRewrite {
    pub old_commit: CommitId,
    pub new_commit: CommitId,
    pub change_id: ChangeId,
}
#[derive(Debug)]
pub struct LandingCandidate {
    pub head: CommitId,
    pub root_tree: TreeId,
    pub rewrites: Vec<LandingRewrite>,
}
#[derive(Debug)]
pub struct LandingReceipt {
    pub destination: String,
    pub generation: u64,
    pub commit: Descriptor,
    pub rewrites: Vec<LandingRewrite>,
    /// Authenticated server assertion, not independently verified build evidence.
    pub evidence: [u8; 32],
}
struct RetainedLanding {
    _guards: crate::verified::LifetimeGuards,
    _bytes: OwnedSemaphorePermit,
    _state: OwnedSemaphorePermit,
}
struct IntentBinding {
    start: LandingStart,
    repository: String,
    client: Weak<Inner>,
    _memory: Arc<RetainedLanding>,
}
/// Exact observed job bound to the original intent and authenticated connection.
/// A newly connected client should use lookup_landing with its persisted intent.
#[derive(Clone)]
pub struct LandingHandle {
    id: [u8; 32],
    binding: Arc<IntentBinding>,
}
impl LandingHandle {
    pub fn id(&self) -> [u8; 32] {
        self.id
    }
    pub fn intent(&self) -> &LandingStart {
        &self.binding.start
    }
    pub fn repository_id(&self) -> &str {
        &self.binding.repository
    }
}
impl std::fmt::Debug for LandingHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LandingHandle")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
struct Job {
    handle: LandingHandle,
    phase: LandingPhase,
    preparation: LandingPreparation,
    roots_retained: bool,
    expires_at_seconds: i64,
    candidate: Option<LandingCandidate>,
    receipt: Option<LandingReceipt>,
    conflict_path: Option<String>,
    // Payloads precede the reservation so they disappear before permits return.
    _memory: Arc<RetainedLanding>,
}
#[derive(Clone)]
pub struct CheckedLandingJob(Arc<Job>);
impl std::fmt::Debug for CheckedLandingJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckedLandingJob")
            .field("handle", &self.0.handle)
            .field("phase", &self.0.phase)
            .finish_non_exhaustive()
    }
}
impl CheckedLandingJob {
    pub fn handle(&self) -> LandingHandle {
        self.0.handle.clone()
    }
    pub fn phase(&self) -> LandingPhase {
        self.0.phase
    }
    pub fn preparation(&self) -> LandingPreparation {
        self.0.preparation
    }
    /// Current durable recovery hold, never a permanent residency promise.
    pub fn roots_retained(&self) -> bool {
        self.0.roots_retained
    }
    pub fn expires_at_seconds(&self) -> i64 {
        self.0.expires_at_seconds
    }
    pub fn candidate(&self) -> Option<&LandingCandidate> {
        self.0.candidate.as_ref()
    }
    pub fn receipt(&self) -> Option<&LandingReceipt> {
        self.0.receipt.as_ref()
    }
    pub fn conflict_path(&self) -> Option<&str> {
        self.0.conflict_path.as_deref()
    }
}
fn bad() -> Status {
    Status::data_loss("landing response has inconsistent bounded assertions")
}
fn exhausted() -> Status {
    Status::resource_exhausted("landing client allocation budget exhausted")
}
fn id32(bytes: &[u8]) -> Result<[u8; 32], Status> {
    let id: [u8; 32] = bytes.try_into().map_err(|_| bad())?;
    if id == [0; 32] {
        return Err(bad());
    }
    Ok(id)
}
fn rewrites(wire: Vec<l::Rewrite>, maximum: usize) -> Result<Vec<LandingRewrite>, Status> {
    if wire.len() > maximum {
        return Err(exhausted());
    }
    let mut old = HashSet::new();
    let mut new = HashSet::new();
    let mut changes = HashSet::new();
    let mut output = Vec::with_capacity(wire.len());
    for r in wire {
        let old_commit = CommitId(id32(&r.old_commit)?);
        let new_commit = CommitId(id32(&r.new_commit)?);
        let change_id = ChangeId::from_slice(&r.change_id).map_err(|_| bad())?;
        if old_commit == new_commit
            || !old.insert(old_commit)
            || !new.insert(new_commit)
            || !changes.insert(change_id)
        {
            return Err(bad());
        }
        output.push(LandingRewrite {
            old_commit,
            new_commit,
            change_id,
        });
    }
    // A wire rewrite is one simultaneous relation, not an evolution chain.
    if old.iter().any(|id| new.contains(id)) {
        return Err(bad());
    }
    Ok(output)
}
fn checked(
    wire: l::LandingResponse,
    handle: Option<LandingHandle>,
    binding: Arc<IntentBinding>,
    memory: Arc<RetainedLanding>,
    limits: LandingLimits,
    expected_id: Option<[u8; 32]>,
) -> Result<CheckedLandingJob, Status> {
    if wire.repository_id != binding.repository
        || wire.idempotency_key != binding.start.idempotency_key
    {
        return Err(bad());
    }
    let job = wire.job.ok_or_else(bad)?;
    let id = id32(&job.id)?;
    if expected_id.is_some_and(|expected| expected != id)
        || job.profile_name != binding.start.profile_name
        || job.profile_version != binding.start.expected_profile_version
        || job.expires_at_seconds <= 0
    {
        return Err(bad());
    }
    let phase = match l::Phase::try_from(job.phase).map_err(|_| bad())? {
        l::Phase::Intent => LandingPhase::Intent,
        l::Phase::PreparedPendingChecks => LandingPhase::PreparedPendingChecks,
        l::Phase::Checked => LandingPhase::Checked,
        l::Phase::Committed => LandingPhase::Committed,
        l::Phase::Conflict => LandingPhase::Conflict,
        l::Phase::Unsupported => LandingPhase::Unsupported,
        l::Phase::CheckFailed => LandingPhase::CheckFailed,
        l::Phase::StaleTarget => LandingPhase::StaleTarget,
        l::Phase::Expired => LandingPhase::Expired,
        _ => return Err(bad()),
    };
    let preparation = match l::PreparationAvailability::try_from(job.preparation)
        .map_err(|_| bad())?
    {
        l::PreparationAvailability::ProfileConfigured => LandingPreparation::ProfileConfigured,
        l::PreparationAvailability::ProfileUnavailable => LandingPreparation::ProfileUnavailable,
        l::PreparationAvailability::PreparationDisabled => LandingPreparation::PreparationDisabled,
        _ => return Err(bad()),
    };
    let candidate = job
        .candidate
        .map(|c| -> Result<_, Status> {
            let head = CommitId(id32(&c.head)?);
            let rewrites = rewrites(c.rewrites, limits.max_rewrites)?;
            if (head == binding.start.source_head && !rewrites.is_empty())
                || (head != binding.start.source_head
                    && rewrites.last().is_none_or(|r| {
                        r.old_commit != binding.start.source_head || r.new_commit != head
                    }))
            {
                return Err(bad());
            }
            Ok(LandingCandidate {
                head,
                root_tree: TreeId(id32(&c.root_tree)?),
                rewrites,
            })
        })
        .transpose()?;
    let receipt = job
        .receipt
        .map(|r| -> Result<_, Status> {
            let reference = r.reference.ok_or_else(bad)?;
            let descriptor =
                parse_descriptor(reference.commit.as_ref().ok_or_else(bad)?).map_err(|_| bad())?;
            let rewrites = rewrites(r.rewrites, limits.max_rewrites)?;
            let c = candidate.as_ref().ok_or_else(bad)?;
            if reference.name != binding.start.destination
                || Some(reference.generation) != binding.start.expected_generation.checked_add(1)
                || descriptor.key.kind != Kind::Commit
                || descriptor.key.id.0 != c.head.0
                || descriptor.size_bytes == 0
                || rewrites != c.rewrites
            {
                return Err(bad());
            }
            Ok(LandingReceipt {
                destination: reference.name,
                generation: reference.generation,
                commit: descriptor,
                rewrites,
                evidence: r.evidence.as_slice().try_into().map_err(|_| bad())?,
            })
        })
        .transpose()?;
    if job
        .conflict_path
        .as_ref()
        .is_some_and(|p| p.len() > limits.max_conflict_path_bytes || p.contains('\0'))
    {
        return Err(bad());
    }
    let has = candidate.is_some();
    let received = receipt.is_some();
    let conflict = job.conflict_path.is_some();
    let valid = match phase {
        LandingPhase::Intent => !has && !received && !conflict && job.roots_retained,
        LandingPhase::PreparedPendingChecks | LandingPhase::Checked => {
            has && !received && !conflict && job.roots_retained
        }
        LandingPhase::Committed => has && received && !conflict,
        LandingPhase::Conflict => !has && !received && conflict && !job.roots_retained,
        LandingPhase::Unsupported => !has && !received && !conflict && !job.roots_retained,
        LandingPhase::CheckFailed | LandingPhase::StaleTarget => {
            has && !received && !conflict && !job.roots_retained
        }
        LandingPhase::Expired => !received && !job.roots_retained && !(has && conflict),
    };
    if !valid {
        return Err(bad());
    }
    Ok(CheckedLandingJob(Arc::new(Job {
        handle: handle.unwrap_or(LandingHandle { id, binding }),
        phase,
        preparation,
        roots_retained: job.roots_retained,
        expires_at_seconds: job.expires_at_seconds,
        candidate,
        receipt,
        conflict_path: job.conflict_path,
        _memory: memory,
    })))
}
pub(crate) struct LandingBudget {
    bytes: Arc<Semaphore>,
    state: Arc<Semaphore>,
}
impl NativeClient {
    /// Give landing RPCs an independent, bounded pool without reconnecting or
    /// changing the authenticated channel, token, repository, or ordinary pools.
    /// Cache this clone per provider: creating pools per request would bypass the
    /// bound. Hosts must reserve aggregate physical codec/result work externally
    /// before using it; this method does not provide process-wide admission.
    pub fn with_landing_budget(mut self, memory_bytes: usize) -> Result<Self, Status> {
        let cap = self.inner.config.landing.max_response_bytes;
        let call = cap
            .checked_mul(8)
            .and_then(|n| n.checked_add(256 * 1024))
            .and_then(|n| {
                n.checked_add(if self.inner.config.compression {
                    ZSTD_WORKSPACE_BYTES
                } else {
                    0
                })
            })
            .ok_or_else(exhausted)?;
        let retained = cap
            .checked_mul(2)
            .and_then(|n| n.checked_add(16384))
            .ok_or_else(exhausted)?;
        let minimum = call
            .checked_add(retained.checked_mul(2).ok_or_else(exhausted)?)
            .ok_or_else(exhausted)?;
        if memory_bytes < minimum || memory_bytes > 256 * 1024 * 1024 {
            return Err(Status::invalid_argument(
                "landing pool is outside bounded codec/result allowance",
            ));
        }
        self.landing_budget = Some(Arc::new(LandingBudget {
            bytes: Arc::new(Semaphore::new(memory_bytes)),
            state: Arc::new(Semaphore::new(memory_bytes - call)),
        }));
        Ok(self)
    }
    fn landing_retained(&self) -> Result<Arc<RetainedLanding>, Status> {
        let n = self
            .inner
            .config
            .landing
            .max_response_bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(16384))
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(exhausted)?;
        let state = self
            .landing_budget
            .as_ref()
            .map(|b| b.state.clone())
            .unwrap_or_else(|| self.inner.query_state.clone())
            .try_acquire_many_owned(n)
            .map_err(|_| exhausted())?;
        let bytes = self
            .landing_budget
            .as_ref()
            .map(|b| b.bytes.clone())
            .unwrap_or_else(|| self.inner.bytes.clone())
            .try_acquire_many_owned(n)
            .map_err(|_| exhausted())?;
        Ok(Arc::new(RetainedLanding {
            _guards: self.guards.clone(),
            _bytes: bytes,
            _state: state,
        }))
    }
    async fn landing_call<T: Message + Send + 'static>(
        &self,
        deadline: Instant,
        path: &'static str,
        make: impl Fn() -> T + Sync,
    ) -> Result<(l::LandingResponse, Arc<Budget>), Status> {
        let cap = self.inner.config.landing.max_response_bytes;
        let memory = cap
            .checked_mul(8)
            .and_then(|n| n.checked_add(256 * 1024))
            .and_then(|n| {
                n.checked_add(if self.inner.config.compression {
                    ZSTD_WORKSPACE_BYTES
                } else {
                    0
                })
            })
            .ok_or_else(exhausted)?;
        self.retry(deadline, false, |allowance| {
            let make = &make;
            async move {
                let inflight = reapi::policy::timeout(deadline, async {
                    self.inner
                        .inflight
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| Status::cancelled("native client closed"))
                })
                .await?;
                let bytes = self
                    .landing_budget
                    .as_ref()
                    .map(|b| b.bytes.clone())
                    .unwrap_or_else(|| self.inner.bytes.clone())
                    .try_acquire_many_owned(u32::try_from(memory).map_err(|_| exhausted())?)
                    .map_err(|_| exhausted())?;
                let budget = Arc::new(Budget {
                    _guards: self.rpc_lifetime_guards(),
                    _inflight: inflight,
                    _bytes: bytes,
                });
                let request = make();
                if request.encoded_len() > 16 * 1024 {
                    return Err(exhausted());
                }
                let response = self
                    .rpc_client(budget.clone())
                    .landing_unary(
                        self.request(request, allowance),
                        path,
                        &self.inner.config.landing,
                    )
                    .await?
                    .into_inner();
                Ok((response, budget))
            }
        })
        .await
    }
    pub async fn start_landing(&self, start: &LandingStart) -> Result<CheckedLandingJob, Status> {
        start.validate()?;
        let deadline = self.deadline();
        let memory = self.landing_retained()?;
        let binding = Arc::new(IntentBinding {
            start: start.clone(),
            repository: self.inner.config.repository_id.clone(),
            client: Arc::downgrade(&self.inner),
            _memory: memory.clone(),
        });
        let (wire, budget) = self
            .landing_call(
                deadline,
                "/tinytree.landing.v1.Landing/StartLanding",
                || start.wire(&self.inner.config.repository_id),
            )
            .await?;
        let limits = self.inner.config.landing.clone();
        blocking_owned(deadline, budget, None, move || {
            checked(wire, None, binding, memory, limits, None)
        })
        .await
    }
    /// Recover by authenticated key after a lost Start response, even if launch
    /// authorization was removed. Original intent bindings are structural
    /// expectations, not a cryptographic proof of the server's frozen intent.
    /// Supply the prior ID whenever one was durably observed.
    pub async fn lookup_landing(
        &self,
        start: &LandingStart,
        expected_id: Option<[u8; 32]>,
    ) -> Result<CheckedLandingJob, Status> {
        start.validate()?;
        if expected_id == Some([0; 32]) {
            return Err(Status::invalid_argument("invalid landing job ID"));
        }
        let deadline = self.deadline();
        let memory = self.landing_retained()?;
        let binding = Arc::new(IntentBinding {
            start: start.clone(),
            repository: self.inner.config.repository_id.clone(),
            client: Arc::downgrade(&self.inner),
            _memory: memory.clone(),
        });
        let (wire, budget) = self
            .landing_call(deadline, "/tinytree.landing.v1.Landing/GetLanding", || {
                l::GetLandingRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    idempotency_key: start.idempotency_key.clone(),
                }
            })
            .await?;
        let limits = self.inner.config.landing.clone();
        blocking_owned(deadline, budget, None, move || {
            checked(wire, None, binding, memory, limits, expected_id)
        })
        .await
    }
    pub async fn get_landing(&self, handle: &LandingHandle) -> Result<CheckedLandingJob, Status> {
        if !handle.binding.client.ptr_eq(&Arc::downgrade(&self.inner))
            || handle.binding.repository != self.inner.config.repository_id
        {
            return Err(Status::invalid_argument(
                "landing handle belongs to another authenticated client",
            ));
        }
        let deadline = self.deadline();
        let memory = self.landing_retained()?;
        let (wire, budget) = self
            .landing_call(deadline, "/tinytree.landing.v1.Landing/GetLanding", || {
                l::GetLandingRequest {
                    repository_id: self.inner.config.repository_id.clone(),
                    idempotency_key: handle.binding.start.idempotency_key.clone(),
                }
            })
            .await?;
        let binding = handle.binding.clone();
        let handle = handle.clone();
        let expected = handle.id;
        let limits = self.inner.config.landing.clone();
        blocking_owned(deadline, budget, None, move || {
            checked(wire, Some(handle), binding, memory, limits, Some(expected))
        })
        .await
    }
}
