//! Original-intent local landing facade. No check verdict or finalize API.
use crate::{CommandGuard, CommitId, landing_protocol as l};
use prost::Message;
use std::sync::Arc;
use tonic::Status;
const CAP: usize = 128 * 1024;
#[derive(Clone, Debug)]
pub struct LandingIntent {
    pub request: l::StartLandingRequest,
    pub source_stack: Vec<CommitId>,
    pub observed_job: Option<[u8; 32]>,
}
impl LandingIntent {
    pub fn validate(&self) -> Result<(), Status> {
        let r = &self.request;
        crate::control::key(&r.idempotency_key)?;
        crate::control::reference_name(&r.destination)?;
        let id = |v: &[u8]| v.len() == 32 && v.iter().any(|b| *b != 0);
        if !r.repository_id.is_empty()
            || r.source_base.len() != 32
            || !id(&r.source_head)
            || !id(&r.expected_head)
            || r.expected_generation == 0
            || r.expected_generation == u64::MAX
            || r.profile_name.is_empty()
            || r.profile_name.len() > 64
            || !r
                .profile_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
            || r.expected_profile_version.is_empty()
            || r.expected_profile_version.len() > 128
            || r.expected_profile_version.contains('\0')
            || self.source_stack.is_empty()
            || self.source_stack.len() > 128
            || self.source_stack.last().map(|id| id.0.as_slice()) != Some(r.source_head.as_slice())
            || self
                .source_stack
                .iter()
                .any(|v| v.0 == [0; 32] || v.0.as_slice() == r.source_base)
            || self
                .source_stack
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.source_stack.len()
            || self.observed_job == Some([0; 32])
            || r.encoded_len() > 4096
        {
            return Err(Status::invalid_argument(
                "invalid native landing original intent",
            ));
        }
        Ok(())
    }
    pub(crate) fn wire(&self, store: String, submit: bool) -> crate::proto::LandingRequest {
        crate::proto::LandingRequest {
            store_id: store,
            original_intent: self.request.encode_to_vec(),
            source_stack: self.source_stack.iter().flat_map(|id| id.0).collect(),
            observed_job: self.observed_job.map(|id| id.to_vec()),
            submit,
        }
    }
    pub(crate) fn decode(value: &crate::proto::LandingRequest) -> Result<Self, Status> {
        if value.original_intent.len() > 4096
            || value.source_stack.len() > 128 * 32
            || value.source_stack.len() % 32 != 0
        {
            return Err(Status::resource_exhausted(
                "native landing request limit exceeded",
            ));
        }
        let intent = Self {
            request: l::StartLandingRequest::decode(value.original_intent.as_slice())
                .map_err(|_| Status::invalid_argument("invalid native landing intent encoding"))?,
            source_stack: value
                .source_stack
                .chunks_exact(32)
                .map(|v| CommitId(v.try_into().unwrap()))
                .collect(),
            observed_job: value
                .observed_job
                .as_deref()
                .map(|id| {
                    id.try_into().map_err(|_| {
                        Status::invalid_argument("invalid native landing observed job")
                    })
                })
                .transpose()?,
        };
        intent.validate()?;
        Ok(intent)
    }
}
#[derive(Clone)]
pub struct LandingReport(Arc<Report>);
struct Report {
    job: l::LandingJob,
    _owner: CommandGuard,
}
impl std::fmt::Debug for LandingReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LandingReport")
            .field("phase", &self.0.job.phase)
            .finish_non_exhaustive()
    }
}
impl LandingReport {
    pub fn job(&self) -> &l::LandingJob {
        &self.0.job
    }
    /// Host adapters supply a structurally checked SDK result plus its physical
    /// result owner. The local client independently validates the bounded shape.
    pub fn checked(
        job: l::LandingJob,
        intent: &LandingIntent,
        owner: CommandGuard,
    ) -> Result<Self, Status> {
        checked(&job, intent)?;
        Ok(Self(Arc::new(Report { job, _owner: owner })))
    }
    pub(crate) fn decode(
        bytes: &[u8],
        intent: &LandingIntent,
        owner: CommandGuard,
    ) -> Result<Self, Status> {
        if bytes.len() > CAP {
            return Err(Status::resource_exhausted(
                "native landing result limit exceeded",
            ));
        }
        shape(bytes, 0)?;
        let job = l::LandingJob::decode(bytes).map_err(|_| bad())?;
        Self::checked(job, intent, owner)
    }
}
fn bad() -> Status {
    Status::data_loss("native landing result does not match original intent")
}
fn checked(job: &l::LandingJob, intent: &LandingIntent) -> Result<(), Status> {
    use l::Phase as P;
    let r = &intent.request;
    let phase = P::try_from(job.phase).map_err(|_| bad())?;
    if phase == P::Unspecified
        || !(1..=3).contains(&job.preparation)
        || job.id.len() != 32
        || job.id.iter().all(|b| *b == 0)
        || intent
            .observed_job
            .is_some_and(|id| id.as_slice() != job.id)
        || job.profile_name != r.profile_name
        || job.profile_version != r.expected_profile_version
        || job.expires_at_seconds <= 0
        || job.encoded_len() > CAP
        || job
            .conflict_path
            .as_ref()
            .is_some_and(|s| s.len() > 4096 || s.contains('\0'))
        || job.receipt.is_some() != (phase == P::Committed)
        || matches!(
            phase,
            P::PreparedPendingChecks
                | P::Checked
                | P::Committed
                | P::CheckFailed
                | P::CheckUnavailable
                | P::StaleTarget
        ) && job.candidate.is_none()
        || matches!(phase, P::Intent | P::Conflict | P::Unsupported) && job.candidate.is_some()
    {
        return Err(bad());
    }
    let has = job.candidate.is_some();
    let conflict = job.conflict_path.is_some();
    let valid = match phase {
        P::Intent => !has && !conflict && job.roots_retained,
        P::PreparedPendingChecks | P::Checked => has && !conflict && job.roots_retained,
        P::Committed => has && !conflict,
        P::Conflict => !has && conflict && !job.roots_retained,
        P::Unsupported => !has && !conflict && !job.roots_retained,
        P::CheckFailed | P::CheckUnavailable | P::StaleTarget => {
            has && !conflict && !job.roots_retained
        }
        P::Expired => !job.roots_retained && !(has && conflict),
        P::Unspecified => false,
    };
    if !valid {
        return Err(bad());
    }
    if let Some(c) = &job.candidate {
        if c.head.len() != 32
            || c.head.iter().all(|b| *b == 0)
            || c.root_tree.len() != 32
            || c.root_tree.iter().all(|b| *b == 0)
            || c.rewrites.len() > 128
        {
            return Err(bad());
        }
        let (mut old, mut new, mut changes) = (
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
            std::collections::HashSet::new(),
        );
        for row in &c.rewrites {
            if row.old_commit.len() != 32
                || row.new_commit.len() != 32
                || row.change_id.len() != 16
                || row.old_commit == row.new_commit
                || row.old_commit.iter().all(|b| *b == 0)
                || row.new_commit.iter().all(|b| *b == 0)
                || !old.insert(&row.old_commit)
                || !new.insert(&row.new_commit)
                || !changes.insert(&row.change_id)
                || !intent
                    .source_stack
                    .iter()
                    .any(|id| id.0.as_slice() == row.old_commit)
            {
                return Err(bad());
            }
        }
        if old.iter().any(|id| new.contains(id)) {
            return Err(bad());
        }
        if c.head == r.source_head {
            if !c.rewrites.is_empty() {
                return Err(bad());
            }
        } else if c
            .rewrites
            .last()
            .is_none_or(|v| v.old_commit != r.source_head || v.new_commit != c.head)
        {
            return Err(bad());
        }
    }
    if let Some(receipt) = &job.receipt {
        let candidate = job.candidate.as_ref().ok_or_else(bad)?;
        let reference = receipt.reference.as_ref().ok_or_else(bad)?;
        let descriptor = reference.commit.as_ref().ok_or_else(bad)?;
        let key = descriptor.key.as_ref().ok_or_else(bad)?;
        if reference.name != r.destination
            || reference.generation != r.expected_generation.checked_add(1).ok_or_else(bad)?
            || key.kind != 3
            || key.native_id != candidate.head
            || descriptor.content_blake3 != candidate.head
            || descriptor.object_format_version != 1
            || descriptor.size_bytes == 0
            || receipt.evidence.len() != 32
            || receipt.rewrites != candidate.rewrites
        {
            return Err(bad());
        }
    }
    Ok(())
}
// Predecode count/string/width checks before Prost can allocate nested Vecs.
// The small fixed depth mirrors LandingJob -> candidate/receipt -> rewrite/ref.
fn shape(mut bytes: &[u8], kind: u8) -> Result<(), Status> {
    let mut counts = [0u16; 16];
    while !bytes.is_empty() {
        let tag = varint(&mut bytes)?;
        let field = usize::try_from(tag >> 3).map_err(|_| bad())?;
        if field == 0 || field >= counts.len() {
            return Err(bad());
        }
        counts[field] = counts[field].checked_add(1).ok_or_else(bad)?;
        let repeated = matches!((kind, field), (1, 3) | (2, 2));
        if counts[field] > if repeated { 128 } else { 1 } {
            return Err(bad());
        }
        match tag & 7 {
            0 => {
                varint(&mut bytes)?;
            }
            2 => {
                let n = usize::try_from(varint(&mut bytes)?).map_err(|_| bad())?;
                let value = bytes.get(..n).ok_or_else(bad)?;
                bytes = &bytes[n..];
                let nested = match (kind, field) {
                    (0, 7) => Some(1),
                    (0, 8) => Some(2),
                    (1, 3) | (2, 2) => Some(3),
                    _ => None,
                };
                if let Some(next) = nested {
                    shape(value, next)?;
                } else {
                    let max = match (kind, field) {
                        (0, 5) => 64,
                        (0, 6) => 128,
                        (0, 9) => 4096,
                        (2, 1) => 4096,
                        _ => 32,
                    };
                    if n > max {
                        return Err(bad());
                    }
                }
            }
            _ => return Err(bad()),
        }
    }
    Ok(())
}
fn varint(bytes: &mut &[u8]) -> Result<u64, Status> {
    let mut result = 0u64;
    for shift in (0..70).step_by(7) {
        let (&b, rest) = bytes.split_first().ok_or_else(bad)?;
        *bytes = rest;
        if shift == 63 && b > 1 {
            return Err(bad());
        }
        result |= u64::from(b & 127) << shift;
        if b & 128 == 0 {
            return Ok(result);
        }
    }
    Err(bad())
}
