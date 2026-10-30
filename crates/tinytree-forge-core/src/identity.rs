//! Exact persisted identity algorithms. JSON tuple order and domain bytes are
//! protocol data; embeddings must not substitute host-language serialization.
use crate::{
    Error, Result,
    accounts::{AccountId, PolicyEpoch},
    review::{ForgeReviewDecision, ForgeReviewPushRequest},
};
use serde::Serialize;
use tinytree_model::ChangeId;

fn fingerprint(value: &impl Serialize, maximum: usize) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Corrupt("metadata encoding"))?;
    if bytes.len() > maximum {
        return Err(Error::Limit("review record bytes"));
    }
    Ok(*blake3::hash(&bytes).as_bytes())
}
pub fn push_fingerprint(r: &ForgeReviewPushRequest) -> Result<[u8; 32]> {
    fingerprint(
        &(
            r.source_base.0,
            r.source_head.0,
            r.view_version,
            r.expected_patchset,
            &r.predecessor,
            r.provenance,
            &r.lineage,
            &r.check_profile,
        ),
        16 * 1024,
    )
}
pub fn decision_fingerprint(
    change: ChangeId,
    patchset: [u8; 32],
    candidate: [u8; 32],
    decision: ForgeReviewDecision,
) -> Result<[u8; 32]> {
    fingerprint(&("decision", change.0, patchset, candidate, decision), 1024)
}
pub fn land_fingerprint(
    change: ChangeId,
    patchset: [u8; 32],
    candidate: [u8; 32],
) -> Result<[u8; 32]> {
    fingerprint(&("land", change.0, patchset, candidate), 1024)
}
pub fn comment_fingerprint(
    change: ChangeId,
    patchset: [u8; 32],
    body: &str,
    path: Option<&str>,
) -> Result<[u8; 32]> {
    fingerprint(&("comment", change.0, patchset, body, path), 12 * 1024)
}
pub fn patchset_id(
    account: &AccountId,
    change: ChangeId,
    fingerprint: [u8; 32],
    epoch: PolicyEpoch,
) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-forge-patchset-v1\0");
    h.update(account.0.as_bytes());
    h.update(&change.0);
    h.update(&fingerprint);
    h.update(&epoch.0.to_be_bytes());
    *h.finalize().as_bytes()
}
pub fn review_owner(account: &AccountId, change: ChangeId) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-forge-review-owner-v1\0");
    h.update(account.0.as_bytes());
    h.update(&change.0);
    *h.finalize().as_bytes()
}
pub fn landing_job_id(repo: &str, owner: [u8; 32], token: &str) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-landing-job-v1\0");
    h.update(&(repo.len() as u64).to_be_bytes());
    h.update(repo.as_bytes());
    h.update(&owner);
    h.update(token.as_bytes());
    *h.finalize().as_bytes()
}
pub fn operation_id(principal: &str, request: &str) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(principal.as_bytes());
    h.update(&[0]);
    h.update(request.as_bytes());
    *h.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::review::*;
    use tinytree_model::CommitId;
    fn hex(id: [u8; 32]) -> String {
        id.iter().map(|b| format!("{b:02x}")).collect()
    }
    #[test]
    fn persisted_local_identity_vectors_survive_shared_core_extraction() {
        // Frozen from the original local algorithms, including ordered serde
        // tuples, enum spellings, account scope and epoch's big-endian bytes.
        let account = AccountId("team".into());
        let change = ChangeId([7; 16]);
        let mut request = ForgeReviewPushRequest {
            request_id: "push-one".into(),
            source_base: CommitId([1; 32]),
            source_head: CommitId([2; 32]),
            view_version: [3; 32],
            expected_patchset: None,
            predecessor: Some(ForgeReviewDependency {
                change_id: [4; 16],
                patchset_id: [5; 32],
            }),
            provenance: None,
            lineage: vec![ForgeReviewLineageHint {
                operation: ForgeReviewLineageOperation::Rename,
                source_path: "old.txt".into(),
                target_path: "new.txt".into(),
            }],
            check_profile: LandingCheckProfile {
                name: "checks".into(),
                version: "v1".into(),
            },
        };
        let fp = push_fingerprint(&request).unwrap();
        assert_eq!(
            hex(fp),
            "b1890fef9f0c35d8a632990a0e44e8f7fac68863c63099028863e40e20e8f79e"
        );
        assert_eq!(
            hex(patchset_id(&account, change, fp, PolicyEpoch(9))),
            "045e656f102feaacbe44b536cae6c04cc0739b081d342d5fa4e58b63ca22bb27"
        );
        let owner = review_owner(&account, change);
        assert_eq!(
            hex(owner),
            "ba686d7f0b0489e87c7247115e3251212297aaccdc1bfe7427671d0b4d78e948"
        );
        assert_eq!(
            hex(landing_job_id("forge-team", owner, "forge-token")),
            "5277b3e31b65eaecacdc2c595fe3823a53ef9a42f90bf7c00fdea52bb846610b"
        );
        assert_eq!(
            hex(operation_id("writer", "push-one")),
            "6d2dbeafaea1bd64dcd5d38b01d33d9586b7586a9a36cf2636f66d7ea3be6920"
        );
        assert_eq!(
            hex(
                decision_fingerprint(change, [8; 32], [9; 32], ForgeReviewDecision::Approve)
                    .unwrap()
            ),
            "a092fef5551610f66476d18c3808c17182be720983f41714699a39c3205cc144"
        );
        assert_eq!(
            hex(land_fingerprint(change, [8; 32], [9; 32]).unwrap()),
            "397998c89219ad10895445c93606c4d7882bb9728bafec6dd4e4422123354d3f"
        );
        assert_eq!(
            hex(comment_fingerprint(change, [8; 32], "Looks good", Some("new.txt")).unwrap()),
            "4ca541dfa353ec14d43e3b0ea086c97d86ab093f8854708b1acec45853383e50"
        );
        request.request_id = "retry-key".into();
        assert_eq!(
            push_fingerprint(&request).unwrap(),
            fp,
            "request identity is separate from immutable patchset content"
        );
        request.check_profile.version = "v2".into();
        assert_ne!(push_fingerprint(&request).unwrap(), fp);
        assert_ne!(
            patchset_id(&account, change, fp, PolicyEpoch(9)),
            patchset_id(&account, change, fp, PolicyEpoch(10))
        );
        assert_ne!(
            decision_fingerprint(change, [8; 32], [9; 32], ForgeReviewDecision::Approve).unwrap(),
            decision_fingerprint(change, [8; 32], [10; 32], ForgeReviewDecision::Approve).unwrap()
        );
        assert_ne!(
            operation_id("writer", "push-one"),
            operation_id("reviewer", "push-one")
        );
    }
}
