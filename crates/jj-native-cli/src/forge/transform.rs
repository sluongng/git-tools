//! Declarative cross-account transformations and retained, tagless source pins.
use super::{rpc, service};
use jj_cli::{
    command_error::{CommandError, user_error},
    ui::Ui,
};
use serde_json::json;
use std::{io::Read as _, path::PathBuf};
use tinytree_client::forge_protocol::ForgeOperation as Op;
use tinytree_forge_core::transform_commands::{self as wire, Command as TransformIntent};
#[derive(clap::Subcommand, Clone, Debug)]
pub(super) enum TransformCommand {
    Start {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        request_id: Option<String>,
    },
    Show {
        run_id: String,
    },
}
#[derive(clap::Subcommand, Clone, Debug)]
pub(super) enum PinCommand {
    Create {
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        request_id: Option<String>,
    },
    Resolve {
        run_id: String,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, requires = "path")]
        offset: Option<u64>,
        #[arg(long, requires = "path")]
        max_bytes: Option<usize>,
        #[arg(long, conflicts_with = "path")]
        after_path: Option<String>,
        #[arg(long, conflicts_with = "path")]
        limit: Option<usize>,
    },
    Retire {
        run_id: String,
        #[arg(long)]
        request_id: Option<String>,
    },
}
#[derive(clap::Args, Clone, Debug)]
pub(super) struct LineageArgs {
    run_id: String,
}
fn checked<const N: usize>(value: String) -> Result<String, CommandError> {
    service::id::<N>(&value)?;
    Ok(value)
}
fn plan(path: PathBuf) -> Result<serde_json::Value, CommandError> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(user_error)?
        .take(wire::MAX_PAYLOAD_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(user_error)?;
    let Some(TransformIntent::Start(intent)) = wire::parse(30, &bytes).map_err(user_error)? else {
        return Err(user_error("invalid typed transform plan"));
    };
    serde_json::to_value(intent).map_err(user_error)
}
async fn call(
    ui: &Ui,
    machine: bool,
    op: Op,
    body: serde_json::Value,
    key: Option<String>,
    mutation: bool,
) -> Result<(), CommandError> {
    wire::parse(op as i32, &serde_json::to_vec(&body).map_err(user_error)?).map_err(user_error)?;
    rpc::print(
        ui,
        machine,
        rpc::call(&service::context()?, op, body, key, mutation, None).await?,
    )
}
pub(super) async fn transform(
    ui: &Ui,
    machine: bool,
    args: TransformCommand,
) -> Result<(), CommandError> {
    match args {
        TransformCommand::Start {
            plan: path,
            request_id,
        } => {
            call(
                ui,
                machine,
                Op::StartTransform,
                plan(path)?,
                request_id,
                true,
            )
            .await
        }
        TransformCommand::Show { run_id } => {
            call(
                ui,
                machine,
                Op::GetTransform,
                json!({"run_id":checked::<32>(run_id)?}),
                None,
                false,
            )
            .await
        }
    }
}
pub(super) async fn pin(ui: &Ui, machine: bool, args: PinCommand) -> Result<(), CommandError> {
    match args {
        PinCommand::Create {
            plan: path,
            request_id,
        } => call(ui, machine, Op::SetPin, plan(path)?, request_id, true).await,
        PinCommand::Retire { run_id, request_id } => {
            call(
                ui,
                machine,
                Op::RetirePin,
                json!({"run_id":checked::<32>(run_id)?}),
                request_id,
                true,
            )
            .await
        }
        PinCommand::Resolve {
            run_id,
            path,
            offset,
            max_bytes,
            after_path,
            limit,
        } => {
            call(ui,machine,Op::ResolvePin,json!({"run_id":checked::<32>(run_id)?,"path":path,"offset":offset,"max_bytes":max_bytes,"after_path":after_path,"limit":limit}),None,false).await
        }
    }
}
pub(super) async fn lineage(ui: &Ui, machine: bool, args: LineageArgs) -> Result<(), CommandError> {
    call(
        ui,
        machine,
        Op::ReadLineage,
        json!({"run_id":checked::<32>(args.run_id)?}),
        None,
        false,
    )
    .await
}
