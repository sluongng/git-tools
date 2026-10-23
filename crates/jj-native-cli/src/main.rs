//! Local native-store entry point. Remote commands and mounts are separate work.
mod broker;
use std::path::{Path, PathBuf};

use jj_cli::cli_util::{CliRunner, CommandHelper};
use jj_cli::command_error::{CommandError, cli_error, user_error_with_message};
use jj_cli::ui::Ui;
use jj_lib::backend::{Backend, BackendInitError};
use jj_lib::repo::StoreFactories;
use jj_lib::settings::UserSettings;
use jj_lib::signing_factory::signer_from_settings;
use jj_lib::workspace::{Workspace, WorkspaceInitError};
use jj_native_backend::{Config, NativeBackend};

#[derive(clap::Parser, Clone, Debug)]
enum NativeCommand {
    /// Commands for native Tinytree repositories (no Git conversion).
    Tinytree {
        #[command(subcommand)]
        command: TinytreeCommand,
    },
}

#[derive(clap::Subcommand, Clone, Debug)]
enum TinytreeCommand {
    /// Initialize a local native repository and ordinary materialized workspace.
    Init {
        #[arg(default_value = ".", value_hint = clap::ValueHint::DirPath)]
        destination: PathBuf,
        /// Use a private host broker instead of opening storage in this process.
        #[arg(long, requires = "store")]
        broker: Option<PathBuf>,
        /// Opaque repository ID configured by the broker owner.
        #[arg(long, requires = "broker")]
        store: Option<String>,
    },
}

async fn run_native_command(
    ui: &mut Ui,
    command: &CommandHelper,
    args: NativeCommand,
) -> Result<(), CommandError> {
    match args {
        NativeCommand::Tinytree {
            command:
                TinytreeCommand::Init {
                    destination,
                    broker: socket,
                    store,
                },
        } => {
            if command.global_args().no_integrate_operation
                || command.global_args().ignore_working_copy
                || command.global_args().at_operation.is_some()
            {
                return Err(cli_error(
                    "native initialization does not support operation or working-copy overrides",
                ));
            }
            let locator = match (socket, store) {
                (Some(socket), Some(store)) => Some(
                    broker::Locator::new(command.cwd().join(socket), store).map_err(|error| {
                        user_error_with_message("Invalid native broker configuration", error)
                    })?,
                ),
                (None, None) => None,
                _ => {
                    return Err(cli_error(
                        "native broker and store must be supplied together",
                    ));
                }
            };
            let destination = command.cwd().join(destination);
            std::fs::create_dir_all(&destination)
                .map_err(|error| user_error_with_message("Failed to create workspace", error))?;
            let destination = destination
                .canonicalize()
                .map_err(|error| user_error_with_message("Failed to resolve workspace", error))?;
            let (settings, _) = command.settings_for_new_workspace(ui, &destination)?;
            let initializer =
                |_: &UserSettings, path: &Path| -> Result<Box<dyn Backend>, BackendInitError> {
                    match &locator {
                        Some(locator) => broker::init(path, locator.clone()),
                        None => Ok(Box::new(NativeBackend::init(path, Config::default())?)),
                    }
                };
            // Default local working-copy, operation, index and workspace stores
            // remain owned by this workspace; only immutable object I/O changes.
            Workspace::init_with_backend(
                &settings,
                &destination,
                &initializer,
                signer_from_settings(&settings).map_err(WorkspaceInitError::SignInit)?,
            )
            .await?;
            Ok(())
        }
    }
}

fn main() -> std::process::ExitCode {
    let mut factories = StoreFactories::empty();
    broker::register(&mut factories);
    CliRunner::init()
        .version(env!("CARGO_PKG_VERSION"))
        .add_store_factories(factories)
        .add_subcommand(run_native_command)
        .run()
        .into()
}
