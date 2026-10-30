//! Canonical review DTOs shared by trusted repository embeddings.
use serde::{Deserialize, Serialize};
use tinytree_model::CommitId;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingCheckProfile {
    pub name: String,
    pub version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeReviewDependency {
    pub change_id: [u8; 16],
    pub patchset_id: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgeReviewLineageOperation {
    Rename,
    Copy,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeReviewLineageHint {
    pub operation: ForgeReviewLineageOperation,
    pub source_path: String,
    pub target_path: String,
}

#[derive(Clone, Debug)]
pub struct ForgeReviewPushRequest {
    pub request_id: String,
    pub source_base: CommitId,
    pub source_head: CommitId,
    pub view_version: [u8; 32],
    pub expected_patchset: Option<[u8; 32]>,
    pub predecessor: Option<ForgeReviewDependency>,
    pub provenance: Option<[u8; 32]>,
    pub lineage: Vec<ForgeReviewLineageHint>,
    /// Selected by trusted server configuration, not an arbitrary client command.
    pub check_profile: LandingCheckProfile,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgeReviewState {
    RebasePending,
    Prepared,
    Conflict,
    Stale,
    ChecksPending,
    ReadyToLand,
    Landed,
    Unsupported,
    CheckFailed,
    Expired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeReviewCandidate {
    pub id: [u8; 32],
    pub head: [u8; 32],
    pub root_tree: [u8; 32],
    pub target: [u8; 32],
    pub target_generation: u64,
    pub rewrites: Vec<ForgeReviewRewrite>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeReviewRewrite {
    pub old: [u8; 32],
    pub new: [u8; 32],
    pub change_id: [u8; 16],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeReview {
    pub change_id: [u8; 16],
    pub owner: String,
    pub patchset_id: [u8; 32],
    pub patchset_number: u64,
    pub is_current: bool,
    pub source_base: [u8; 32],
    pub source_head: [u8; 32],
    pub view_version: [u8; 32],
    pub predecessor: Option<ForgeReviewDependency>,
    pub provenance: Option<[u8; 32]>,
    pub lineage: Vec<ForgeReviewLineageHint>,
    pub check_profile: LandingCheckProfile,
    pub state: ForgeReviewState,
    pub candidate: Option<ForgeReviewCandidate>,
    pub conflict_path: Option<String>,
    pub approvals: usize,
    pub comments: Vec<ForgeReviewComment>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeReviewComment {
    pub author: String,
    pub patchset_id: [u8; 32],
    pub body: String,
    pub path: Option<String>,
    pub created_at: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgeReviewDecision {
    Approve,
    RequestChanges,
}
