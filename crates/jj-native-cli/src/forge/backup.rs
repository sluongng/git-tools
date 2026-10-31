//! Explicit private WIP lifecycle. Handler selection is not backup success.
use super::{client, output, service, workspace};
use jj_cli::{
    cli_util::CommandHelper,
    command_error::{CommandError, user_error},
    ui::Ui,
};
use jj_lib::object_id::ObjectId as _;
use serde_json::json;
use std::{io::Write as _, path::PathBuf};
use tinytree_client::{
    PrivateCapsuleClient, Transport,
    private_capsule::{CapsuleAction, CapsuleCall, CapsuleState},
};

#[derive(clap::Args, Clone, Debug)]
pub(super) struct BackupArgs {
    #[command(subcommand)]
    command: Option<BackupCommand>,
}
#[derive(clap::Subcommand, Clone, Debug)]
enum BackupCommand {
    /// Finish only the exact saved immutable pending backup, without capturing newer WIP.
    Resume,
    /// Retire only saved older heads proven covered by the acknowledged latest head.
    Prune {
        #[arg(long, required = true)]
        superseded: bool,
    },
}
pub(super) async fn run(
    ui: &mut Ui,
    command: &CommandHelper,
    machine: bool,
    args: BackupArgs,
) -> Result<(), CommandError> {
    match args.command {
        None => backup(ui, command, machine).await,
        Some(BackupCommand::Resume) => client::with_runtime(resume(ui, command, machine)).await,
        Some(BackupCommand::Prune { superseded: true }) => {
            client::with_runtime(prune(ui, command, machine)).await
        }
        Some(BackupCommand::Prune { superseded: false }) => {
            Err(user_error("private backup pruning requires --superseded"))
        }
    }
}
async fn maintenance_target(
    command: &CommandHelper,
) -> Result<(workspace_daemon::control::Client, PathBuf), CommandError> {
    if command
        .matches()
        .try_contains_id("repository")
        .unwrap_or(false)
    {
        return Err(user_error(
            "backup maintenance must run inside its intended workspace; omit --repository",
        ));
    }
    // Do not load a Workspace/ReadonlyRepo: the managed loader admits a writer.
    // Resolve only the supported marker and exact Ready registry binding.
    let cwd = command.cwd().canonicalize().map_err(user_error)?;
    let mut root = None;
    for ancestor in cwd.ancestors().take(256) {
        match std::fs::symlink_metadata(ancestor.join(".jj/native-workspace")) {
            Ok(_) => {
                root = Some(ancestor.to_owned());
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(user_error(error)),
        }
    }
    let root = root.ok_or_else(|| {
        user_error("backup maintenance requires an existing derived Forge workspace")
    })?;
    let descriptor = client::maintenance_descriptor(&root).await?;
    Ok((
        workspace_daemon::control::Client {
            socket: descriptor.state_dir().join("control.sock"),
        },
        root,
    ))
}
async fn resume(ui: &Ui, command: &CommandHelper, machine: bool) -> Result<(), CommandError> {
    let (client, root) = maintenance_target(command).await?;
    let value = client
        .call(workspace_daemon::control::Request::NativeResumePrivateBackup { root })
        .map_err(user_error)?;
    let result: workspace_daemon::control::NativePrivateBackupResumed =
        serde_json::from_value(value).map_err(|_| {
            user_error("invalid saved backup acknowledgement; pending state retained")
        })?;
    result.receipt.validate().map_err(user_error)?;
    if machine {
        output::write(
            ui,
            "backup_resume",
            "complete",
            json!({"receipt":result.receipt,"replayed":result.replayed,"newer_working_copy_captured":false}),
        )?;
    } else {
        writeln!(
            ui.stdout(),
            "Saved private backup sealed: {} generation {}, commit {}, operation {}. Newer working-copy changes were not captured.",
            result.receipt.capsule_id,
            result.receipt.generation,
            result.receipt.snapshot_commit,
            result.receipt.snapshot_operation
        )?;
    }
    Ok(())
}
async fn prune(ui: &Ui, command: &CommandHelper, machine: bool) -> Result<(), CommandError> {
    let (client, root) = maintenance_target(command).await?;
    let value = client
        .call(workspace_daemon::control::Request::NativePrunePrivateCapsules { root })
        .map_err(user_error)?;
    let result: workspace_daemon::control::NativeCapsulesPruned = serde_json::from_value(value)
        .map_err(|_| {
            user_error("invalid private cleanup acknowledgement; pending state retained")
        })?;
    service::id::<32>(&result.current_capsule)?;
    if machine {
        output::write(
            ui,
            "backup_prune",
            if result.pending == 0 {
                "complete"
            } else {
                "pending"
            },
            result,
        )?;
    } else {
        writeln!(
            ui.stdout(),
            "Private backup cleanup: {} retired, {} local files removed, {} pending. Current working copy was not backed up.",
            result.retired,
            result.local_files,
            result.pending
        )?;
    }
    Ok(())
}

#[derive(clap::Args, Clone, Debug)]
pub(super) struct RestoreArgs {
    pub destination: PathBuf,
    #[arg(long)]
    pub capsule: String,
}
pub(super) async fn backup(
    ui: &mut Ui,
    command: &CommandHelper,
    machine: bool,
) -> Result<(), CommandError> {
    let global = command.global_args();
    if global.ignore_working_copy || global.no_integrate_operation || global.at_operation.is_some()
    {
        return Err(user_error(
            "private backup requires the current working copy with ordinary snapshot/integration enabled",
        ));
    }
    let loader = command.workspace_loader()?;
    // Ordinary workspace_helper performs the supported jj snapshot boundary.
    // Do not substitute a read-only load/status approximation for edited files.
    let helper = command.workspace_helper(ui).await?;
    let commit = helper
        .get_wc_commit()
        .await?
        .ok_or_else(|| user_error("private backup requires a working-copy revision"))?;
    let snapshot_commit = commit.id().hex();
    let snapshot_operation = helper.repo().op_id().hex();
    crate::commands::request_private_backup(
        loader.repo_path(),
        machine,
        snapshot_commit,
        snapshot_operation,
    )
    .map_err(user_error)?;
    // No output here: helpers and writer lease still exist. Main emits only
    // after runner teardown and the durable sealed-capsule acknowledgement.
    Ok(())
}
pub(crate) fn finish(outcome: crate::commands::FinishOutcome) -> bool {
    let crate::commands::FinishOutcome::PrivateBackup { machine, result } = outcome else {
        return true;
    };
    let mut stdout = std::io::stdout().lock();
    match result {
        Ok(receipt) => {
            if machine {
                serde_json::to_writer(&mut stdout, &json!({"schema_version":1,"operation":"backup","state":"complete","result":receipt,"error":null})).and_then(|_| { writeln!(stdout).map_err(serde_json::Error::io) }).is_ok()
            } else {
                writeln!(
                    stdout,
                    "Private WIP backup sealed: {} generation {}",
                    receipt.capsule_id, receipt.generation
                )
                .is_ok()
            }
        }
        Err(message) => {
            if machine {
                let _ = serde_json::to_writer(
                    &mut stdout,
                    &json!({"schema_version":1,"operation":"backup","state":"failed","result":null,"error":{"code":"backup_unacknowledged","message":message}}),
                );
                let _ = writeln!(stdout);
            } else {
                eprintln!("Private WIP backup failed: {message}");
            }
            false
        }
    }
}

/// Fresh restore uses authenticated capsule selection only as input. The daemon
/// collector independently downloads and verifies every byte before Ready.
pub(super) async fn restore(
    ui: &mut Ui,
    command: &CommandHelper,
    machine: bool,
    args: RestoreArgs,
) -> Result<(), CommandError> {
    client::with_runtime(restore_inner(ui, command, machine, args)).await
}
async fn restore_inner(
    ui: &mut Ui,
    command: &CommandHelper,
    machine: bool,
    args: RestoreArgs,
) -> Result<(), CommandError> {
    let capsule_id = service::id::<32>(&args.capsule)?;
    let target = command.cwd().join(&args.destination);
    let mut context = service::context()?;
    match std::fs::symlink_metadata(&target) {
        Ok(metadata) => {
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || (std::fs::read_dir(&target)
                    .map_err(user_error)?
                    .next()
                    .transpose()
                    .map_err(user_error)?
                    .is_some()
                    && !client::private_retry(&target, &args.capsule, &context)?)
            {
                return Err(user_error(
                    "private restore requires an absent or empty nonsymlink destination; resume an existing restored workspace with tt work or tt tinytree resume",
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(user_error(error)),
    }
    if context.transport != Transport::ForgeHttpsV1 {
        return Err(user_error(
            "private restore requires an explicit forge-https-v1 context",
        ));
    }
    let trust = client::trust(&context)?;
    let capsule_client =
        PrivateCapsuleClient::connect(context.client_config().map_err(user_error)?, trust)
            .await
            .map_err(user_error)?;
    let reply = capsule_client
        .call(
            CapsuleCall::new(&context.account_id, &args.capsule, CapsuleAction::Status),
            None,
        )
        .await
        .map_err(user_error)?;
    if reply.state != CapsuleState::Sealed {
        return Err(user_error(
            "private capsule is not sealed and cannot be restored",
        ));
    }
    let capsule = reply.binding();
    capsule.validate().map_err(user_error)?;
    if !capsule.selection.view.is_empty() {
        context.view = capsule.selection.view.clone();
    }
    let version = service::id::<32>(&capsule.selection.view_version)?;
    let enrolled = client::enroll(&context, &target, Some(version)).await?;
    if enrolled.projection.view_version != version
        || enrolled.projection.policy_epoch != capsule.selection.policy_epoch
        || enrolled.projection.projected_commit
            != service::id::<32>(&capsule.selection.projected_commit)?
        || enrolled.projection.projected_tree
            != service::id::<32>(&capsule.selection.projected_tree)?
    {
        return Err(user_error(
            "capsule selection differs from the independently verified Forge projection; original state retained",
        ));
    }
    client::mark_private_restore(&enrolled, &capsule)?;
    let descriptor = workspace_daemon::forge_client::ClientDescriptor::load(&enrolled.descriptor)
        .map_err(user_error)?;
    descriptor.validate().map_err(user_error)?;
    let value = workspace_daemon::control::Client {
        socket: descriptor.state_dir().join("control.sock"),
    }
    .call(workspace_daemon::control::Request::NativeRestoreCapsule {
        root: descriptor.destination().to_owned(),
        capsule_id,
    })
    .map_err(user_error)?;
    let restored: workspace_daemon::control::NativeCapsuleRestored = serde_json::from_value(value)
        .map_err(|_| {
            user_error("invalid private restore acknowledgement; original state retained")
        })?;
    restored.receipt.validate().map_err(user_error)?;
    if restored.receipt.capsule_id != capsule.capsule_id
        || restored.receipt.workspace_id != capsule.workspace_id
        || restored.receipt.generation != capsule.generation
        || restored.receipt.bytes != capsule.descriptor.total_bytes
        || restored.receipt.chain_root != capsule.descriptor.chain_root
        || restored.mounted.binding.root != descriptor.destination()
        || restored.mounted.registry_path != descriptor.registry_path()
        || restored.mounted.binding.repository != enrolled.store
        || restored.mounted.binding.endpoint.local()
            != native_broker::LocalEndpoint::UnixSocket(enrolled.broker.clone())
    {
        return Err(user_error(
            "private restore receipt differs from the selected capsule or provider; existing state retained",
        ));
    }
    // The manager ACK follows shared staging, OP/WC/closure readback and Ready.
    // Re-read typed Ready eligibility rather than creating an operation here.
    client::finish_resume(descriptor.destination())?;
    let scope = workspace::Scope::load(descriptor.destination())?;
    if scope.base != service::id::<32>(&restored.base)?
        || scope.tree != service::id::<32>(&restored.tree)?
    {
        return Err(user_error(
            "verified private review base differs from restore acknowledgement",
        ));
    }
    if machine {
        output::write(
            ui,
            "work",
            "ready",
            json!({"destination":descriptor.destination(),"account_id":context.account_id,
        "view":context.view,"view_version":capsule.selection.view_version,"base":restored.base,"tree":restored.tree,
        "client_descriptor":enrolled.descriptor,"provider_identity":workspace::hex(&enrolled.provider_identity),"receipt":restored.receipt}),
        )?;
    }
    Ok(())
}
