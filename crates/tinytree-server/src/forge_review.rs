//! Authenticated review adapter. Identities and check profiles are service inputs.
use serde::Deserialize;
use serde_json::Value;
use tinytree_model::{ChangeId, CommitId, Signature};
use tinytree_proto::forge_v1::ForgeOperation;
use tinytree_store::{
    AccountId, ForgeActor, ForgeReview, ForgeReviewDecision, ForgeReviewDependency,
    ForgeReviewPushRequest, LandingCheckProfile, Store,
};
use tonic::Status;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dependency {
    change_id: String,
    patchset_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Push {
    source_base: String,
    source_head: String,
    view_version: String,
    expected_patchset: Option<String>,
    predecessor: Option<Dependency>,
    provenance: Option<String>,
    #[serde(default)]
    lineage: Vec<tinytree_store::ForgeReviewLineageHint>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Get {
    change_id: String,
    patchset_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Decide {
    change_id: String,
    patchset_id: String,
    decision: String,
    candidate_id: Option<String>,
    body: Option<String>,
    path: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Land {
    change_id: String,
    patchset_id: String,
    candidate_id: String,
}
fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Status> {
    if bytes.len() > 16384 {
        return Err(Status::resource_exhausted("review payload limit"));
    }
    serde_json::from_slice(bytes).map_err(|_| Status::invalid_argument("invalid review payload"))
}
fn id<const N: usize>(text: &str) -> Result<[u8; N], Status> {
    if text.len() != N * 2 {
        return Err(Status::invalid_argument("invalid lowercase hex identifier"));
    }
    let mut output = [0; N];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let digit = |b: u8| match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            _ => None,
        };
        output[index] = digit(pair[0])
            .zip(digit(pair[1]))
            .map(|(a, b)| a * 16 + b)
            .ok_or_else(|| Status::invalid_argument("invalid lowercase hex identifier"))?;
    }
    Ok(output)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn error(error: tinytree_store::Error) -> Status {
    use tinytree_store::Error;
    match error {
        Error::ForgeAccessDenied => Status::permission_denied("review access denied"),
        Error::StaleForgePolicy | Error::StaleForgeReview => {
            Status::failed_precondition("review or policy is stale")
        }
        Error::IdempotencyConflict => {
            Status::already_exists("request key was used for different inputs")
        }
        Error::Invalid(_) => Status::failed_precondition("invalid review state or input"),
        Error::Limit(_) => Status::resource_exhausted("review resource limit"),
        _ => Status::internal("review operation failed"),
    }
}
fn result(
    store: &Store,
    account: &AccountId,
    actor: &ForgeActor,
    view: &str,
    review: ForgeReview,
    now: i64,
) -> Result<(String, Value), Status> {
    store
        .forge_resolve_view(
            account,
            Some(actor),
            view,
            Some(tinytree_store::ViewVersion(review.view_version)),
        )
        .map_err(error)?;
    let state = serde_json::to_value(review.state)
        .map_err(|_| Status::internal("review encoding failed"))?
        .as_str()
        .unwrap_or("unknown")
        .to_owned();
    let mut value =
        serde_json::to_value(&review).map_err(|_| Status::internal("review encoding failed"))?;
    // The Store's typed arrays are durable DTOs; the public wire uses hex IDs.
    fn convert(value: &mut Value) {
        match value {
            Value::Object(fields) => {
                for (name, value) in fields {
                    if matches!(
                        name.as_str(),
                        "change_id"
                            | "patchset_id"
                            | "source_base"
                            | "source_head"
                            | "view_version"
                            | "provenance"
                            | "id"
                            | "head"
                            | "root_tree"
                            | "target"
                            | "old"
                            | "new"
                    ) {
                        if let Value::Array(bytes) = value {
                            if bytes.iter().all(|v| v.as_u64().is_some_and(|n| n < 256)) {
                                *value = Value::String(hex(&bytes
                                    .iter()
                                    .map(|v| v.as_u64().unwrap() as u8)
                                    .collect::<Vec<_>>()));
                                continue;
                            }
                        }
                    }
                    convert(value);
                }
            }
            Value::Array(items) => {
                for value in items {
                    convert(value);
                }
            }
            _ => {}
        }
    }
    convert(&mut value);
    if let Some(projected) = store
        .forge_project_review_result(
            account,
            actor,
            ChangeId(review.change_id),
            review.patchset_id,
            now,
        )
        .map_err(error)?
    {
        value["policy_epoch"] = serde_json::json!(projected.policy_epoch.0);
        value["projected_head"] = Value::String(hex(&projected.projected_commit));
        value["projected_rewrites"] = serde_json::json!([{"old":hex(&review.source_head),"new":hex(&projected.projected_commit),"change_id":hex(&review.change_id)}]);
    }
    Ok((state, value))
}

pub(crate) fn review_call(
    store: &Store,
    actor: &ForgeActor,
    operation: ForgeOperation,
    account: &AccountId,
    view: &str,
    request_id: &str,
    payload_json: &[u8],
    committer: &Signature,
    profile: Option<&LandingCheckProfile>,
    now: i64,
) -> Result<Option<(String, Value)>, Status> {
    let review = match operation {
        ForgeOperation::PushReview => {
            let p: Push = parse(payload_json)?;
            let selected = store
                .forge_resolve_view(
                    account,
                    Some(actor),
                    view,
                    Some(tinytree_store::ViewVersion(id(&p.view_version)?)),
                )
                .map_err(error)?;
            let request = ForgeReviewPushRequest {
                request_id: request_id.into(),
                source_base: CommitId(id(&p.source_base)?),
                source_head: CommitId(id(&p.source_head)?),
                view_version: selected.version().0,
                expected_patchset: p.expected_patchset.as_deref().map(id).transpose()?,
                predecessor: p
                    .predecessor
                    .map(|d| {
                        Ok::<_, Status>(ForgeReviewDependency {
                            change_id: id(&d.change_id)?,
                            patchset_id: id(&d.patchset_id)?,
                        })
                    })
                    .transpose()?,
                provenance: p.provenance.as_deref().map(id).transpose()?,
                lineage: p.lineage,
                check_profile: profile.cloned().ok_or_else(|| {
                    Status::failed_precondition("review checks are not configured")
                })?,
            };
            store
                .forge_push_review(account, actor, &request, committer, now)
                .map_err(error)?
        }
        ForgeOperation::GetReview => {
            let p: Get = parse(payload_json)?;
            match p.patchset_id {
                Some(version) => store.forge_review_patchset(
                    account,
                    actor,
                    ChangeId(id(&p.change_id)?),
                    id(&version)?,
                ),
                None => store.forge_review(account, actor, ChangeId(id(&p.change_id)?)),
            }
            .map_err(error)?
        }
        ForgeOperation::GetReviewOperation => {
            let _: Empty = parse(payload_json)?;
            return store
                .forge_review_operation(account, actor, request_id)
                .map_err(error)?
                .map(|r| result(store, account, actor, view, r, now))
                .transpose()
                .map(|r| Some(r.unwrap_or(("not_found".into(), Value::Null))));
        }
        ForgeOperation::DecideReview => {
            let p: Decide = parse(payload_json)?;
            let change = ChangeId(id(&p.change_id)?);
            let patchset = id(&p.patchset_id)?;
            let selected = store
                .forge_review_patchset(account, actor, change, patchset)
                .map_err(error)?;
            store
                .forge_resolve_view(
                    account,
                    Some(actor),
                    view,
                    Some(tinytree_store::ViewVersion(selected.view_version)),
                )
                .map_err(error)?;
            if p.decision == "comment" {
                if p.candidate_id.is_some() {
                    return Err(Status::invalid_argument(
                        "candidate_id only applies to review decisions",
                    ));
                }
                store
                    .forge_comment_review(
                        account,
                        actor,
                        change,
                        patchset,
                        request_id,
                        p.body
                            .as_deref()
                            .ok_or_else(|| Status::invalid_argument("comment body required"))?,
                        p.path.as_deref(),
                        now,
                    )
                    .map_err(error)?
            } else {
                if p.body.is_some() || p.path.is_some() {
                    return Err(Status::invalid_argument("body and path require comment"));
                }
                let decision = match p.decision.as_str() {
                    "approve" => ForgeReviewDecision::Approve,
                    "request_changes" => ForgeReviewDecision::RequestChanges,
                    _ => return Err(Status::invalid_argument("unknown review decision")),
                };
                store
                    .forge_decide_review(
                        account,
                        actor,
                        change,
                        patchset,
                        id(p.candidate_id.as_deref().ok_or_else(|| {
                            Status::invalid_argument("candidate_id required for review decision")
                        })?)?,
                        request_id,
                        decision,
                        now,
                    )
                    .map_err(error)?
            }
        }
        ForgeOperation::LandReview => {
            let p: Land = parse(payload_json)?;
            let change = ChangeId(id(&p.change_id)?);
            let patchset = id(&p.patchset_id)?;
            let review = store
                .forge_review_patchset(account, actor, change, patchset)
                .map_err(error)?;
            store
                .forge_resolve_view(
                    account,
                    Some(actor),
                    view,
                    Some(tinytree_store::ViewVersion(review.view_version)),
                )
                .map_err(error)?;
            if profile != Some(&review.check_profile) {
                return Err(Status::failed_precondition(
                    "review check profile is no longer selected",
                ));
            }
            store
                .forge_land_review_request(
                    account,
                    actor,
                    change,
                    patchset,
                    id(&p.candidate_id)?,
                    request_id,
                    now,
                )
                .map_err(error)?
        }
        _ => return Ok(None),
    };
    result(store, account, actor, view, review, now).map(Some)
}
