//! Account/view scoped forge calls. No actor, verdict or raw route override.
use super::{context::Context, output, pending};
use jj_cli::{
    command_error::{CommandError, user_error},
    ui::Ui,
};
use serde_json::Value;
use std::io::Write as _;
use tinytree_client::{NativeClient, forge_protocol as f};

#[derive(Debug)]
pub(super) struct CallError {
    pub status: tonic::Status,
    pub request_id: Option<String>,
}
impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.status.message())?;
        if let Some(id) = &self.request_id {
            write!(
                f,
                "; saved request {id} can be reconciled with tt request resume"
            )?;
        }
        Ok(())
    }
}
impl std::error::Error for CallError {}

pub(super) async fn call(
    context: &Context,
    operation: f::ForgeOperation,
    payload: Value,
    request_id: Option<String>,
    mutation: bool,
    source: Option<pending::Source>,
) -> Result<f::ForgeCallResponse, CommandError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return call_inner(context, operation, payload, request_id, mutation, source).await;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(user_error)?;
    runtime.block_on(call_inner(
        context, operation, payload, request_id, mutation, source,
    ))
}

async fn call_inner(
    context: &Context,
    operation: f::ForgeOperation,
    payload: Value,
    request_id: Option<String>,
    mutation: bool,
    source: Option<pending::Source>,
) -> Result<f::ForgeCallResponse, CommandError> {
    let config = context.client_config().map_err(user_error)?;
    let bytes = serde_json::to_vec(&payload).map_err(user_error)?;
    if bytes.len() > 1024 * 1024 {
        return Err(user_error("Tinytree request payload exceeds 1 MiB"));
    }
    let pending = if mutation {
        Some(
            pending::Pending::prepare(
                context,
                operation as i32,
                &payload,
                request_id.clone(),
                source,
            )
            .map_err(user_error)?,
        )
    } else {
        None
    };
    let input = f::ForgeCallRequest {
        operation: operation as i32,
        account_id: context.account_id.clone(),
        view: context.view.clone(),
        request_id: pending
            .as_ref()
            .map(|p| p.request_id.clone())
            .or(request_id)
            .unwrap_or_default(),
        payload_json: bytes,
    };
    let saved_key = pending.as_ref().map(|value| value.request_id.clone());
    let client = NativeClient::connect(config).await.map_err(|status| {
        user_error(CallError {
            status,
            request_id: saved_key.clone(),
        })
    })?;
    // SDK verifies the exact operation/account/view/replay binding and caps
    // decoding before returning an authenticated control-plane assertion.
    let result = if operation == f::ForgeOperation::ResolveView {
        client.forge_resolve_view(input).await
    } else if operation == f::ForgeOperation::PushReview {
        client.forge_push_review(input).await
    } else {
        client.forge_call(input).await
    };
    result.map_err(|status| {
        user_error(CallError {
            status,
            request_id: saved_key,
        })
    })
}

pub(super) fn print(
    ui: &Ui,
    machine: bool,
    reply: f::ForgeCallResponse,
) -> Result<(), CommandError> {
    let mut result: Value = serde_json::from_slice(&reply.result_json)
        .map_err(|_| user_error("Tinytree returned invalid operation JSON"))?;
    normalize_ids(&mut result);
    let operation = f::ForgeOperation::try_from(reply.operation)
        .map_err(|_| user_error("Tinytree returned an unknown operation"))?
        .as_str_name()
        .trim_start_matches("FORGE_OPERATION_")
        .to_ascii_lowercase();
    let body = serde_json::json!({"account_id":reply.account_id,"view":reply.view,"request_id":reply.request_id,"value":result});
    if machine {
        output::write(ui, &operation, &reply.state, body)
    } else {
        writeln!(ui.stdout(), "{}: {}", operation, reply.state)?;
        serde_json::to_writer_pretty(ui.stdout(), &body).map_err(user_error)?;
        writeln!(ui.stdout())?;
        Ok(())
    }
}

// CLI identities have one spelling even when internal serde DTOs use byte arrays.
fn normalize_ids(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            for (name, value) in fields {
                let identity = matches!(
                    name.as_str(),
                    "id" | "revision"
                        | "main_revision"
                        | "projected_commit"
                        | "projected_tree"
                        | "view_version"
                        | "public_view"
                        | "source_base"
                        | "source_head"
                        | "candidate_id"
                        | "change_id"
                        | "patchset_id"
                        | "tree_id"
                        | "commit_id"
                        | "run_id"
                        | "session_id"
                        | "provenance"
                        | "copy_id"
                        | "file_id"
                        | "pin_id"
                        | "artifact"
                        | "retired_version"
                );
                if identity
                    && let Value::Array(bytes) = value
                    && matches!(bytes.len(), 16 | 32)
                    && bytes.iter().all(|b| b.as_u64().is_some_and(|v| v <= 255))
                {
                    *value = Value::String(
                        bytes
                            .iter()
                            .map(|b| format!("{:02x}", b.as_u64().unwrap()))
                            .collect(),
                    );
                } else {
                    normalize_ids(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                normalize_ids(value);
            }
        }
        _ => {}
    }
}
