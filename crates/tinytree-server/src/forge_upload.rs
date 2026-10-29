//! Bounded resumable WriteObject sessions. Legacy unary uploads remain separate.
use serde::Deserialize;
use serde_json::Value;
use tinytree_proto::forge_v1::ForgeOperation;
use tinytree_store::{AccountId, ForgeActor, Store, ViewVersion};
use tonic::Status;

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Upload {
    Begin {
        version: [u8; 32],
        path: String,
        kind: u8,
        id: [u8; 32],
        size_bytes: u64,
    },
    Append {
        session_id: [u8; 32],
        offset: u64,
        data: Vec<u8>,
    },
    Status {
        session_id: [u8; 32],
    },
    Finalize {
        session_id: [u8; 32],
    },
}

pub(crate) fn upload_call(
    store: &Store,
    actor: Option<&ForgeActor>,
    operation: ForgeOperation,
    account: &AccountId,
    view: &str,
    request_id: &str,
    payload: &[u8],
    now: i64,
) -> Result<Option<(String, Value)>, Status> {
    if operation != ForgeOperation::WriteObject {
        return Ok(None);
    }
    if payload.len() > 2 * 1024 * 1024 {
        return Err(Status::resource_exhausted("upload request limit"));
    }
    // Return only legacy bodies to the existing strict unary adapter. A present
    // but invalid action cannot silently fall through to another operation.
    let value: Value = serde_json::from_slice(payload)
        .map_err(|_| Status::invalid_argument("invalid upload request"))?;
    if value.get("action").is_none() {
        return Ok(None);
    }
    let request: Upload = serde_json::from_slice(payload)
        .map_err(|_| Status::invalid_argument("invalid upload session request"))?;
    let actor = actor.ok_or_else(|| Status::permission_denied("upload requires authentication"))?;
    let result = match request {
        Upload::Begin {
            version,
            path,
            kind,
            id,
            size_bytes,
        } => {
            let kind = tinytree_model::Kind::try_from(kind)
                .map_err(|_| Status::invalid_argument("invalid upload object kind"))?;
            let resolved = store
                .forge_resolve_view(account, Some(actor), view, Some(ViewVersion(version)))
                .map_err(status)?;
            store
                .forge_begin_upload(
                    &resolved,
                    actor,
                    request_id,
                    &path,
                    tinytree_model::Descriptor {
                        key: tinytree_model::ObjectKey {
                            kind,
                            id: tinytree_model::ObjectId(id),
                        },
                        size_bytes,
                        format_version: tinytree_model::FORMAT_VERSION,
                    },
                    now,
                )
                .map_err(status)?
        }
        Upload::Append {
            session_id,
            offset,
            data,
        } => store
            .forge_append_upload(account, actor, view, session_id, offset, &data, now)
            .map_err(status)?,
        Upload::Status { session_id } => store
            .forge_upload_status(account, actor, view, session_id, now)
            .map_err(status)?,
        Upload::Finalize { session_id } => store
            .forge_finalize_upload(account, actor, view, session_id, now)
            .map_err(status)?,
    };
    let state = if result.receipt.is_some() {
        "committed"
    } else {
        "pending"
    };
    let value =
        serde_json::to_value(result).map_err(|_| Status::internal("upload response encoding"))?;
    Ok(Some((state.into(), value)))
}
fn status(error: tinytree_store::Error) -> Status {
    use tinytree_store::Error as E;
    match error {
        E::ForgeAccessDenied => Status::permission_denied("upload access denied"),
        E::StaleForgePolicy => Status::aborted("upload policy changed"),
        E::IdempotencyConflict => {
            Status::already_exists("upload retry conflicts with committed bytes or binding")
        }
        E::Limit(_) => Status::resource_exhausted("upload resource limit"),
        E::Invalid("upload session expired") => {
            Status::failed_precondition("upload session expired")
        }
        E::Invalid("upload incomplete") => Status::failed_precondition("upload incomplete"),
        E::Invalid("unknown upload session") => Status::not_found("unknown upload session"),
        E::Invalid(_) | E::Model(_) => Status::invalid_argument("invalid upload object or chunk"),
        E::Corrupt(_) => Status::data_loss("upload integrity check failed"),
        _ => Status::unavailable("upload storage unavailable"),
    }
}
