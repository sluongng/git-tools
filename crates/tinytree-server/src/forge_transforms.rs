//! Account-scoped storage adapter for the shared strict transform protocol.
use serde_json::Value;
use tinytree_forge_core::transform_commands::{self as wire, Command};
use tinytree_model::{ChangeId, Signature};
use tinytree_proto::forge_v1::ForgeOperation;
use tinytree_store::forge_transforms::ForgeTransformRequest;
use tinytree_store::{
    AccountId, ForgeActor, ForgeReviewPushRequest, LandingCheckProfile, Store, ViewVersion,
};
use tonic::Status;

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
    let Some(command) = wire::parse(operation as i32, payload_json).map_err(protocol)? else {
        return Ok(None);
    };
    Ok(Some(match command {
        Command::Start(payload) | Command::SetPin(payload) => {
            let profile = profile.ok_or_else(|| {
                Status::failed_precondition("transform review requires a configured check profile")
            })?;
            let source = AccountId(payload.source_account);
            let before = store
                .forge_resolve_view(
                    &source,
                    Some(actor),
                    &payload.source_view,
                    Some(ViewVersion(
                        wire::id(&payload.source_before_view).map_err(protocol)?,
                    )),
                )
                .map_err(status)?;
            let after = store
                .forge_resolve_view(
                    &source,
                    Some(actor),
                    &payload.source_view,
                    Some(ViewVersion(
                        wire::id(&payload.source_after_view).map_err(protocol)?,
                    )),
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
                        .map(wire::id)
                        .transpose()
                        .map_err(protocol)?
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
                        change_id: ChangeId(wire::id(&payload.change_id).map_err(protocol)?),
                        lineage: payload
                            .lineage
                            .into_iter()
                            .map(|hint| hint.into_hint())
                            .collect(),
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
                        request_id: format!("transform-{}", wire::hex(&run.id)),
                        source_base: tinytree_model::CommitId(run.destination_base),
                        source_head: tinytree_model::CommitId(run.output_head),
                        view_version: run.destination_view,
                        expected_patchset: payload
                            .expected_patchset
                            .as_deref()
                            .map(wire::id)
                            .transpose()
                            .map_err(protocol)?,
                        predecessor: None,
                        provenance: Some(run.id),
                        lineage: vec![],
                        check_profile: profile.clone(),
                    },
                    committer,
                    now,
                )
                .map_err(status)?;
            let transform = store
                .forge_transform_run(account, actor, run.id)
                .map_err(status)?;
            wire::format_started(&transform, &review)
                .map_err(|_| Status::internal("transform response encoding"))?
        }
        Command::Get { run_id } => {
            let result = store
                .forge_transform_run(account, actor, wire::id(&run_id).map_err(protocol)?)
                .map_err(status)?;
            (
                if result.pin.retired {
                    "retired"
                } else {
                    "prepared"
                }
                .into(),
                wire::format_view(&result),
            )
        }
        Command::ReadLineage { run_id } => {
            let result = store
                .forge_transform_run(account, actor, wire::id(&run_id).map_err(protocol)?)
                .map_err(status)?;
            (
                if result.origin.is_some() {
                    "complete"
                } else {
                    "origin_unavailable"
                }
                .into(),
                wire::format_lineage(&result),
            )
        }
        Command::ResolvePin(payload) => {
            let run = wire::id(&payload.run_id).map_err(protocol)?;
            let result = if let Some(path) = payload.path {
                wire::format_chunk(
                    &store
                        .forge_read_pin_file(
                            account,
                            actor,
                            run,
                            &path,
                            payload.offset.unwrap_or(0),
                            payload.max_bytes.unwrap_or(65536),
                        )
                        .map_err(status)?,
                )
            } else {
                wire::format_page(
                    &store
                        .forge_pin_files(
                            account,
                            actor,
                            run,
                            payload.after_path.as_deref(),
                            payload.limit.unwrap_or(128),
                        )
                        .map_err(status)?,
                )
            };
            ("retained".into(), result)
        }
        Command::RetirePin { run_id } => {
            let result = store
                .forge_retire_pin(
                    account,
                    actor,
                    view,
                    wire::id(&run_id).map_err(protocol)?,
                    request_id,
                )
                .map_err(status)?;
            ("retired".into(), wire::format_pin(&result))
        }
    }))
}
fn protocol(error: tinytree_forge_core::Error) -> Status {
    match error {
        tinytree_forge_core::Error::Limit(_) => {
            Status::resource_exhausted("transform resource limit")
        }
        _ => Status::invalid_argument("invalid transform operation payload"),
    }
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
