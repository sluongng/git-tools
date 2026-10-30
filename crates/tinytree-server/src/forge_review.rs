//! Authenticated review adapter. Identities and check profiles are service inputs.
use tinytree_forge_core::review_commands::{self, ReviewCommand, DecisionIntent, format_review, hex};
use serde_json::Value;
use tinytree_model::{ChangeId, Signature};
use tinytree_proto::forge_v1::ForgeOperation;
use tinytree_store::{
    AccountId, ForgeActor, ForgeReview, LandingCheckProfile, Store,
};
use tonic::Status;

fn command_error(error: tinytree_forge_core::Error) -> Status {
    match error {
        tinytree_forge_core::Error::Limit(message) => Status::resource_exhausted(message),
        tinytree_forge_core::Error::Invalid(message) => Status::invalid_argument(message),
        tinytree_forge_core::Error::Corrupt(_) => Status::internal("review encoding failed"),
    }
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
    let (state, mut value) = format_review(&review).map_err(command_error)?;
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
    let Some(command) = review_commands::parse(operation as i32, payload_json).map_err(command_error)? else { return Ok(None); };
    let review = match command {
        ReviewCommand::Push(intent) => {
            store.forge_resolve_view(account, Some(actor), view, Some(tinytree_store::ViewVersion(intent.view_version))).map_err(error)?;
            let request = intent.into_request(request_id, profile.cloned().ok_or_else(|| Status::failed_precondition("review checks are not configured"))?);
            store.forge_push_review(account, actor, &request, committer, now).map_err(error)?
        }
        ReviewCommand::Get { change_id, patchset_id } => {
            match patchset_id {
                Some(version) => store.forge_review_patchset(account, actor, ChangeId(change_id), version),
                None => store.forge_review(account, actor, ChangeId(change_id)),
            }.map_err(error)?
        }
        ReviewCommand::Operation => {
            return store.forge_review_operation(account, actor, request_id).map_err(error)?
                .map(|r| result(store, account, actor, view, r, now)).transpose()
                .map(|r| Some(r.unwrap_or(("not_found".into(), Value::Null))));
        }
        ReviewCommand::Decide { change_id, patchset_id, intent } => {
            let change = ChangeId(change_id);
            let selected = store.forge_review_patchset(account, actor, change, patchset_id).map_err(error)?;
            store.forge_resolve_view(account, Some(actor), view, Some(tinytree_store::ViewVersion(selected.view_version))).map_err(error)?;
            match intent {
                DecisionIntent::Comment { body, path } => store.forge_comment_review(account, actor, change, patchset_id, request_id, &body, path.as_deref(), now),
                DecisionIntent::Vote { decision, candidate_id } => store.forge_decide_review(account, actor, change, patchset_id, candidate_id, request_id, decision, now),
            }.map_err(error)?
        }
        ReviewCommand::Land { change_id, patchset_id, candidate_id } => {
            let change = ChangeId(change_id);
            let review = store.forge_review_patchset(account, actor, change, patchset_id).map_err(error)?;
            store.forge_resolve_view(account, Some(actor), view, Some(tinytree_store::ViewVersion(review.view_version))).map_err(error)?;
            if profile != Some(&review.check_profile) { return Err(Status::failed_precondition("review check profile is no longer selected")); }
            store.forge_land_review_request(account, actor, change, patchset_id, candidate_id, request_id, now).map_err(error)?
        }
    };
    result(store, account, actor, view, review, now).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_review_parser_retains_native_dispatch_fallback_and_strict_payload_errors() {
        let (_directory,store,account,actor,_view)=crate::forge_accounts::tests::fixture();
        let committer=Signature{name:"Fixture".into(),email:"fixture@example.invalid".into(),timestamp:tinytree_model::Timestamp{millis_since_epoch:0,tz_offset_minutes:0}};
        let call=|operation,payload:&[u8]|review_call(&store,&actor,operation,&account,"public","request",payload,&committer,None,0);
        assert!(call(ForgeOperation::ReadFile,b"not owned by review parser").unwrap().is_none());
        assert_eq!(call(ForgeOperation::GetReviewOperation,br#"{"request_id":"override"}"#).unwrap_err().code(),tonic::Code::InvalidArgument);
        assert_eq!(call(ForgeOperation::GetReviewOperation,&vec![b' ';16385]).unwrap_err().code(),tonic::Code::ResourceExhausted);
        assert_eq!(call(ForgeOperation::GetReviewOperation,b"{}").unwrap().unwrap(),("not_found".into(),Value::Null));
    }
}
