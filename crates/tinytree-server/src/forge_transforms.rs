//! Account-scoped transform and retained-dependency wire adapters.
use serde::Deserialize;
use serde_json::{Value, json};
use tinytree_model::{ChangeId, Signature};
use tinytree_proto::forge_v1::ForgeOperation;
use tinytree_store::forge_transforms::{
    ForgeLineageHint, ForgeTransformRequest, ForgeTransformSpec,
};
use tinytree_store::{
    AccountId, ForgeActor, ForgeReviewPushRequest, LandingCheckProfile, Store, ViewVersion,
};
use tonic::Status;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    source_account: AccountId,
    source_view: String,
    source_before_view: String,
    source_after_view: String,
    destination_view: Option<String>,
    spec: ForgeTransformSpec,
    change_id: String,
    #[serde(default)]
    lineage: Vec<ForgeLineageHint>,
    expected_patchset: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Run {
    run_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolvePin {
    run_id: String,
    path: Option<String>,
    offset: Option<u64>,
    max_bytes: Option<usize>,
    after_path: Option<String>,
    limit: Option<usize>,
}

pub(crate) fn transform_call(
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
    use ForgeOperation as Op;
    match operation {
        Op::StartTransform | Op::SetPin => {
            let payload: Start = parse(payload_json)?;
            let profile = profile.ok_or_else(|| {
                Status::failed_precondition("transform review requires a configured check profile")
            })?;
            let before = store
                .forge_resolve_view(
                    &payload.source_account,
                    Some(actor),
                    &payload.source_view,
                    Some(ViewVersion(id(&payload.source_before_view)?)),
                )
                .map_err(status)?;
            let after = store
                .forge_resolve_view(
                    &payload.source_account,
                    Some(actor),
                    &payload.source_view,
                    Some(ViewVersion(id(&payload.source_after_view)?)),
                )
                .map_err(status)?;
            let destination = store
                .forge_resolve_view(
                    account,
                    Some(actor),
                    view,
                    payload
                        .destination_view
                        .as_deref()
                        .map(id)
                        .transpose()?
                        .map(ViewVersion),
                )
                .map_err(status)?;
            let run = store
                .forge_prepare_transform(
                    actor,
                    &ForgeTransformRequest {
                        request_id: request_id.into(),
                        before,
                        after,
                        destination,
                        spec: payload.spec,
                        change_id: ChangeId(id(&payload.change_id)?),
                        lineage: payload.lineage,
                    },
                    committer,
                    now,
                )
                .map_err(status)?;
            if run.pin.retired {
                return Err(Status::failed_precondition("dependency pin retired"));
            }
            let review = store
                .forge_push_review(
                    account,
                    actor,
                    &ForgeReviewPushRequest {
                        request_id: format!("transform-{}", hex(&run.id)),
                        source_base: tinytree_model::CommitId(run.destination_base),
                        source_head: tinytree_model::CommitId(run.output_head),
                        view_version: run.destination_view,
                        expected_patchset: payload
                            .expected_patchset
                            .as_deref()
                            .map(id)
                            .transpose()?,
                        predecessor: None,
                        provenance: Some(run.id),
                        lineage: vec![],
                        check_profile: profile.clone(),
                    },
                    committer,
                    now,
                )
                .map_err(status)?;
            // The prepared transform root protects bytes before and throughout
            // proposal creation. Review retirement integration may later replace
            // this conservative owner with its exact retained patchset roots.
            let transform = store
                .forge_transform_run(account, actor, run.id)
                .map_err(status)?;
            let review = json_value(review)?;
            let state = review
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("rebase_pending")
                .to_owned();
            Ok(Some((
                state,
                json!({"transform": transform, "review": review}),
            )))
        }
        Op::GetTransform => {
            let payload: Run = parse(payload_json)?;
            let result = store
                .forge_transform_run(account, actor, id(&payload.run_id)?)
                .map_err(status)?;
            Ok(Some((
                if result.pin.retired {
                    "retired"
                } else {
                    "prepared"
                }
                .into(),
                json_value(result)?,
            )))
        }
        Op::ReadLineage => {
            let payload: Run = parse(payload_json)?;
            let result = store
                .forge_transform_run(account, actor, id(&payload.run_id)?)
                .map_err(status)?;
            Ok(Some((
                if result.origin.is_some() {
                    "complete"
                } else {
                    "origin_unavailable"
                }
                .into(),
                json!({"run_id": result.id, "origin": result.origin}),
            )))
        }
        Op::ResolvePin => {
            let payload: ResolvePin = parse(payload_json)?;
            let run = id(&payload.run_id)?;
            let result = if let Some(path) = payload.path {
                if payload.after_path.is_some() || payload.limit.is_some() {
                    return Err(Status::invalid_argument(
                        "pin chunk and page selectors cannot be combined",
                    ));
                }
                json_value(
                    store
                        .forge_read_pin_file(
                            account,
                            actor,
                            run,
                            &path,
                            payload.offset.unwrap_or(0),
                            payload.max_bytes.unwrap_or(64 * 1024),
                        )
                        .map_err(status)?,
                )?
            } else {
                if payload.offset.is_some() || payload.max_bytes.is_some() {
                    return Err(Status::invalid_argument(
                        "pin byte range requires a file path",
                    ));
                }
                json_value(
                    store
                        .forge_pin_files(
                            account,
                            actor,
                            run,
                            payload.after_path.as_deref(),
                            payload.limit.unwrap_or(128),
                        )
                        .map_err(status)?,
                )?
            };
            Ok(Some(("retained".into(), result)))
        }
        Op::RetirePin => {
            let payload: Run = parse(payload_json)?;
            let result = store
                .forge_retire_pin(account, actor, id(&payload.run_id)?)
                .map_err(status)?;
            Ok(Some(("retired".into(), json_value(result)?)))
        }
        _ => Ok(None),
    }
}

fn parse<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, Status> {
    if bytes.len() > 1024 * 1024 {
        return Err(Status::resource_exhausted("transform payload limit"));
    }
    serde_json::from_slice(bytes)
        .map_err(|_| Status::invalid_argument("invalid transform operation payload"))
}
fn json_value(value: impl serde::Serialize) -> Result<Value, Status> {
    serde_json::to_value(value).map_err(|_| Status::internal("transform response encoding"))
}
fn id<const N: usize>(text: &str) -> Result<[u8; N], Status> {
    if text.len() != N * 2
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Status::invalid_argument(
            "invalid transform object identity",
        ));
    }
    let mut result = [0; N];
    for (out, pair) in result.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        let digit = |v: u8| if v <= b'9' { v - b'0' } else { v - b'a' + 10 };
        *out = digit(pair[0]) * 16 + digit(pair[1]);
    }
    Ok(result)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn status(error: tinytree_store::Error) -> Status {
    use tinytree_store::Error as E;
    match error {
        E::ForgeAccessDenied => Status::permission_denied("forge access denied"),
        E::StaleForgePolicy => Status::aborted("forge access policy changed"),
        E::StaleReference(_) => Status::aborted("transform target changed"),
        E::IdempotencyConflict => Status::already_exists("transform request identity conflict"),
        E::Limit(_) => Status::resource_exhausted("transform resource limit"),
        E::Invalid("dependency pin retired") => {
            Status::failed_precondition("dependency pin retired")
        }
        E::Invalid("dependency pin still referenced") => {
            Status::failed_precondition("dependency pin still referenced")
        }
        E::Invalid("transform destination conflict") => {
            Status::aborted("transform destination conflict")
        }
        E::Invalid(_) | E::Model(_) => Status::invalid_argument("invalid transform operation"),
        E::Corrupt(_) => Status::data_loss("transform integrity check failed"),
        _ => Status::unavailable("transform storage operation unavailable"),
    }
}
