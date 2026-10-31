//! Portable original landing intent and historical assertions. None of these
//! decoded values authorize dispatch, retention, reconciliation, or checkout.
use serde::{Deserialize, Serialize};
use std::io;
fn is_false(value: &bool) -> bool {
    !*value
}
fn valid_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn validate_names(reference: &str, bookmark: &str) -> io::Result<()> {
    if reference.is_empty()
        || reference.len() > 512
        || reference.contains('\0')
        || reference == "_tinytree"
        || reference.starts_with("_tinytree/")
        || bookmark.is_empty()
        || bookmark.len() > 512
        || bookmark.contains('\0')
    {
        return Err(invalid());
    }
    Ok(())
}
fn invalid() -> io::Error {
    io::Error::other("invalid portable landing recovery data")
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalLandingPhase {
    Intent,
    PreparedPendingChecks,
    Checked,
    Committed,
    Conflict,
    Unsupported,
    CheckFailed,
    CheckUnavailable,
    StaleTarget,
    Expired,
    PublicationAbandoned,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalLandingPreparation {
    ProfileConfigured,
    ProfileUnavailable,
    PreparationDisabled,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HistoricalLandingRewrite {
    pub old_commit: [u8; 32],
    pub new_commit: [u8; 32],
    pub change_id: [u8; 16],
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HistoricalLandingCandidate {
    pub head: [u8; 32],
    pub root_tree: [u8; 32],
    pub rewrites: Vec<HistoricalLandingRewrite>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HistoricalLandingReceipt {
    pub reference: String,
    pub generation: u64,
    pub commit: [u8; 32],
    pub commit_size_bytes: u64,
    pub object_format_version: u16,
    pub rewrites: Vec<HistoricalLandingRewrite>,
    pub evidence: [u8; 32],
}
/// Structurally checked history, not an authenticated CheckedLandingJob or live hold.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HistoricalLandingJob {
    pub job_id: [u8; 32],
    pub phase: HistoricalLandingPhase,
    pub preparation: HistoricalLandingPreparation,
    pub roots_retained: bool,
    pub expires_at_seconds: i64,
    pub profile_name: String,
    pub profile_version: String,
    pub candidate: Option<HistoricalLandingCandidate>,
    pub receipt: Option<HistoricalLandingReceipt>,
    pub conflict_path: Option<String>,
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
impl HistoricalLandingJob {
    pub fn validate(&self, intent: &PortableLanding) -> io::Result<()> {
        if self.job_id == [0; 32]
            || intent.job_id.as_deref() != Some(hex(&self.job_id).as_str())
            || self.expires_at_seconds <= 0
            || self.profile_name != intent.profile_name
            || self.profile_version != intent.profile_version
            || self
                .conflict_path
                .as_ref()
                .is_some_and(|p| p.len() > 4096 || p.contains('\0'))
        {
            return Err(invalid());
        }
        if let Some(c) = &self.candidate {
            let mut old = std::collections::HashSet::new();
            let mut new = std::collections::HashSet::new();
            let mut changes = std::collections::HashSet::new();
            if c.head == [0; 32] || c.root_tree == [0; 32] || c.rewrites.len() > 128 {
                return Err(invalid());
            }
            for r in &c.rewrites {
                if r.old_commit == [0; 32]
                    || r.new_commit == [0; 32]
                    || r.old_commit == r.new_commit
                    || !old.insert(r.old_commit)
                    || !new.insert(r.new_commit)
                    || !changes.insert(r.change_id)
                    || !intent.source_stack.iter().any(|s| s == &hex(&r.old_commit))
                {
                    return Err(invalid());
                }
            }
            if old.iter().any(|o| new.contains(o))
                || (hex(&c.head) == intent.source_head && !c.rewrites.is_empty())
                || (hex(&c.head) != intent.source_head
                    && c.rewrites.last().is_none_or(|r| {
                        hex(&r.old_commit) != intent.source_head || r.new_commit != c.head
                    }))
            {
                return Err(invalid());
            }
            if let Some(head) = &intent.candidate_head {
                let mut roots = std::iter::once(hex(&c.head))
                    .chain(c.rewrites.iter().map(|r| hex(&r.new_commit)))
                    .collect::<Vec<_>>();
                roots.sort();
                roots.dedup();
                let mut persisted = intent.result_roots.clone();
                persisted.sort();
                persisted.dedup();
                if head != &hex(&c.head) || persisted != roots {
                    return Err(invalid());
                }
            }
        } else if intent.candidate_head.is_some() {
            return Err(invalid());
        }
        if let Some(r) = &self.receipt {
            let c = self.candidate.as_ref().ok_or_else(invalid)?;
            if r.reference != intent.reference
                || Some(r.generation) != intent.generation.checked_add(1)
                || r.commit != c.head
                || r.commit_size_bytes == 0
                || r.object_format_version != tinytree_model::FORMAT_VERSION
                || r.rewrites != c.rewrites
            {
                return Err(invalid());
            }
        }
        use HistoricalLandingPhase as P;
        let expected_terminal = match self.phase {
            P::Conflict => Some("conflict"),
            P::Unsupported => Some("unsupported"),
            P::CheckFailed => Some("check_failed"),
            P::CheckUnavailable => Some("check_unavailable"),
            P::StaleTarget => Some("stale_target"),
            P::Expired => Some("expired"),
            P::PublicationAbandoned => Some("publication_abandoned"),
            _ => None,
        };
        if intent.terminal.as_deref() != expected_terminal
            || (intent.acknowledged && self.phase != P::Committed)
            || (intent.apply_operation.is_some() && self.phase != P::Committed)
            || (matches!(
                self.phase,
                P::PreparedPendingChecks | P::Checked | P::Committed | P::PublicationAbandoned
            ) && intent.candidate_head.is_none()
                && !intent.acknowledged)
            || (intent.restored_lookup_only
                && intent.acknowledged
                && intent.apply_operation.is_none())
        {
            return Err(invalid());
        }
        let has = self.candidate.is_some();
        let receipt = self.receipt.is_some();
        let conflict = self.conflict_path.is_some();
        use HistoricalLandingPhase::*;
        let valid = match self.phase {
            Intent => !has && !receipt && !conflict && self.roots_retained,
            PreparedPendingChecks | Checked | PublicationAbandoned => {
                has && !receipt && !conflict && self.roots_retained
            }
            Committed => has && receipt && !conflict,
            Conflict => !has && !receipt && conflict && !self.roots_retained,
            Unsupported => !has && !receipt && !conflict && !self.roots_retained,
            CheckFailed | CheckUnavailable => has && !receipt && !conflict && !self.roots_retained,
            StaleTarget => !receipt && !conflict && !self.roots_retained,
            Expired => !receipt && !self.roots_retained && !(has && conflict),
        };
        if !valid {
            return Err(invalid());
        }
        Ok(())
    }
}
/// Durable original intent. This is a local recovery journal, never a client
/// verdict or permission to finalize the server job. IDs remain protected until
/// exact operation publication and native checkout acknowledgement are durable.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PortableLanding {
    pub request_id: String,
    pub source_base: String,
    pub source_head: String,
    pub reference: String,
    pub bookmark: String,
    pub generation: u64,
    pub expected_head: String,
    pub profile_name: String,
    pub profile_version: String,
    pub source_operation: String,
    /// Oldest first; source_head is last. Never reconstructed from a moving @.
    pub source_stack: Vec<String>,
    pub fork_operation: Option<String>,
    pub fork_acknowledged: bool,
    pub job_id: Option<String>,
    /// Canonical admitted candidate/evolution roots, populated after Get.
    #[serde(default)]
    pub result_roots: Vec<String>,
    pub candidate_head: Option<String>,
    pub apply_operation: Option<String>,
    pub operation_writing: Option<String>,
    pub acknowledged: bool,
    pub terminal: Option<String>,
    /// Historical data only. A decoded capsule never recreates authenticated job authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed: Option<HistoricalLandingJob>,
    /// A recovered intent must be looked up before any operation reconciliation or checkout.
    #[serde(default, skip_serializing_if = "is_false")]
    pub restored_lookup_only: bool,
}
impl PortableLanding {
    /// Conservative owned DTO allowance: bounded strings/128 stack entries,
    /// 256 result IDs and two 128-entry rewrite vectors, including parser/vector
    /// growth and the 4KiB conflict path. No raw object bodies are retained here.
    pub const MAX_WORKING_BYTES: usize = 4 * 1024 * 1024;
    pub fn validate(&self) -> io::Result<()> {
        validate_names(&self.reference, &self.bookmark)?;
        let nonroot = |id: &str| valid_hex(id, 64) && id.bytes().any(|b| b != b'0');
        let operation = |id: &str| valid_hex(id, 128);
        let mut stack = std::collections::HashSet::new();
        if !valid_hex(&self.request_id, 32)
            || !valid_hex(&self.source_base, 64)
            || !nonroot(&self.source_head)
            || !nonroot(&self.expected_head)
            || self.generation == 0
            || self.generation == u64::MAX
            || self.profile_name.is_empty()
            || self.profile_name.len() > 64
            || !self
                .profile_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
            || self.profile_version.is_empty()
            || self.profile_version.len() > 128
            || self.profile_version.contains('\0')
            || !operation(&self.source_operation)
            || self.source_stack.is_empty()
            || self.source_stack.len() > 128
            || self.source_stack.last() != Some(&self.source_head)
            || self
                .source_stack
                .iter()
                .any(|id| !nonroot(id) || !stack.insert(id))
            || stack.contains(&self.source_base)
            || (self.fork_acknowledged && self.fork_operation.is_none())
            || self
                .fork_operation
                .as_deref()
                .is_some_and(|id| !operation(id))
            || self
                .apply_operation
                .as_deref()
                .is_some_and(|id| !operation(id))
            || self.job_id.as_deref().is_some_and(|id| !nonroot(id))
            || self
                .candidate_head
                .as_deref()
                .is_some_and(|id| !nonroot(id))
            || self.result_roots.len() > 256
            || self.result_roots.iter().any(|id| !nonroot(id))
            || (self.job_id.is_some() && !self.fork_acknowledged)
            || (self.candidate_head.is_some()
                && (self.job_id.is_none()
                    || !self
                        .result_roots
                        .iter()
                        .any(|id| Some(id) == self.candidate_head.as_ref())))
            || (!self.result_roots.is_empty() && self.candidate_head.is_none())
            || (self.apply_operation.is_some()
                && (self.job_id.is_none()
                    || (self.candidate_head.is_none()
                        && !(self.acknowledged
                            && self.observed.as_ref().is_some_and(|j| {
                                j.phase == HistoricalLandingPhase::Committed
                                    && j.candidate.is_some()
                                    && j.receipt.is_some()
                            })))))
            || (self.terminal.is_some() && (self.job_id.is_none() || self.acknowledged))
            || self
                .operation_writing
                .as_deref()
                .is_some_and(|phase| match phase {
                    "freeze" => self.fork_operation.is_some(),
                    "apply" => {
                        self.apply_operation.is_some()
                            || self.job_id.is_none()
                            || self.candidate_head.is_none()
                    }
                    _ => true,
                })
            || (self.acknowledged && self.apply_operation.is_none())
            || self.terminal.as_deref().is_some_and(|s| {
                !matches!(
                    s,
                    "conflict"
                        | "unsupported"
                        | "check_failed"
                        | "check_unavailable"
                        | "stale_target"
                        | "expired"
                        | "publication_abandoned"
                )
            })
        {
            return Err(io::Error::other("invalid native landing recovery intent"));
        }
        if let Some(observed) = &self.observed {
            observed.validate(self)?;
        }
        Ok(())
    }
    /// An acknowledged apply's historical head is a retention binding only,
    /// never permission to apply or replay a decoded operation.
    pub fn retention_candidate_head(&self) -> Option<String> {
        self.candidate_head.clone().or_else(|| {
            self.acknowledged
                .then(|| {
                    self.observed
                        .as_ref()
                        .filter(|j| j.phase == HistoricalLandingPhase::Committed)
                        .and_then(|j| j.candidate.as_ref())
                        .map(|c| hex(&c.head))
                })
                .flatten()
        })
    }
    pub fn retained_operations(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.source_operation.as_str())
            .chain(self.fork_operation.as_deref())
            .chain(self.apply_operation.as_deref())
    }
    pub fn retained_commit_heads(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.source_head.as_str())
            .chain(std::iter::once(self.expected_head.as_str()))
            .chain(
                (self.source_base.bytes().any(|b| b != b'0')).then_some(self.source_base.as_str()),
            )
            .chain(self.source_stack.iter().map(String::as_str))
            .chain(self.result_roots.iter().map(String::as_str))
    }
}

/// Retained abandoned intents are history, never authority to apply or release.
pub fn validate_archived_landings(landings: &[PortableLanding]) -> io::Result<()> {
    if landings.len() > 128 {
        return Err(invalid());
    }
    let mut jobs = std::collections::HashSet::new();
    for landing in landings {
        landing.validate()?;
        if landing.terminal.as_deref() != Some("publication_abandoned")
            || landing.acknowledged
            || landing.apply_operation.is_some()
            || landing.observed.as_ref().is_none_or(|job| {
                job.phase != HistoricalLandingPhase::PublicationAbandoned
                    || !jobs.insert(job.job_id)
            })
        {
            return Err(invalid());
        }
    }
    Ok(())
}
