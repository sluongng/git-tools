//! Finite command output. Never write diagnostics or credentials to stdout.
use jj_cli::{command_error::CommandError, ui::Ui};
use serde::Serialize;
use std::io::Write as _;

#[derive(Debug, Serialize)]
pub(super) struct Error<'a> {
    pub code: &'a str,
    pub message: &'a str,
}

#[derive(Debug, Serialize)]
pub(super) struct Envelope<T> {
    pub schema_version: u32,
    pub operation: String,
    pub state: String,
    pub result: Option<T>,
    pub error: Option<Error<'static>>,
}

pub(super) fn write<T: Serialize>(
    ui: &Ui,
    operation: &str,
    state: &str,
    value: T,
) -> Result<(), CommandError> {
    let report = Envelope {
        schema_version: 1,
        operation: operation.to_owned(),
        state: state.to_owned(),
        result: Some(value),
        error: None,
    };
    serde_json::to_writer(ui.stdout(), &report)
        .map_err(|_| jj_cli::command_error::user_error("cannot encode command result"))?;
    writeln!(ui.stdout())?;
    Ok(())
}

pub(super) fn error(ui: &Ui, operation: &str, error: &CommandError) -> std::io::Result<()> {
    // Detailed human diagnostics remain on stderr. Do not serialize error
    // sources, configuration, credential environment values or request bodies.
    let message = error.error.to_string();
    let call = error.error.downcast_ref::<super::rpc::CallError>();
    let request_id = call.and_then(|e| e.request_id.as_deref());
    let grpc_code = call.map(|e| e.status.code() as i32);
    let uncertain = call.is_some_and(|e| {
        e.request_id.is_some()
            && matches!(
                e.status.code(),
                tonic::Code::Unavailable
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Cancelled
                    | tonic::Code::Unknown
            )
    });
    let report = serde_json::json!({
        "schema_version": 1,
        "operation": operation,
        "state": if uncertain {"uncertain"} else {"failed"},
        "request_id":request_id,
        "result": null,
        "error": {"code": if uncertain {"outcome_uncertain"} else {"command_failed"}, "message": message,"grpc_code":grpc_code},
    });
    serde_json::to_writer(ui.stdout(), &report)?;
    writeln!(ui.stdout())
}
