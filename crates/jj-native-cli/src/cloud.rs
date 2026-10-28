//! Recovery discovery goes through the matching daemon, never a caller URL.
use crate::vfs;
use jj_cli::{
    cli_util::CommandHelper,
    command_error::{CommandError, user_error},
    ui::Ui,
};
use std::{io::Write as _, path::PathBuf};

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct CloudArgs {
    #[arg(long)]
    daemon_config: PathBuf,
    #[arg(long)]
    store: String,
    #[command(subcommand)]
    command: CloudCommand,
}
#[derive(clap::Subcommand, Clone, Debug)]
enum CloudCommand {
    /// List one moving catalog page. A listed generation is not a retention lease.
    Workspaces {
        #[arg(long, default_value_t = 32, value_parser = clap::value_parser!(u16).range(1..=128))]
        limit: u16,
        #[arg(long, default_value = "")]
        cursor: String,
        #[arg(long)]
        json: bool,
    },
    /// List generation selectors through the first page's fixed upper bound.
    Versions {
        workspace: String,
        #[arg(long, default_value_t = 32, value_parser = clap::value_parser!(u16).range(1..=128))]
        limit: u16,
        #[arg(long, default_value = "")]
        cursor: String,
        #[arg(long)]
        json: bool,
    },
    /// Restore a generation, or resolve latest once. Retries reuse the saved pin.
    Restore {
        workspace: String,
        destination: PathBuf,
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        generation: Option<u64>,
        #[arg(long)]
        json: bool,
    },
}
fn failure(error: impl std::fmt::Display) -> CommandError {
    user_error(format!("Native cloud recovery: {error}"))
}

pub(crate) fn run(ui: &Ui, command: &CommandHelper, args: CloudArgs) -> Result<(), CommandError> {
    vfs::reject_recovery_overrides(command)?;
    #[cfg(any(not(unix), feature = "protocol-fixture"))]
    {
        let _ = (ui, args);
        return Err(failure(
            "cloud recovery requires the production native daemon",
        ));
    }
    #[cfg(all(unix, not(feature = "protocol-fixture")))]
    {
        use workspace_daemon::control::{self as c, Request};
        let config_path = command.cwd().join(args.daemon_config);
        let config = workspace_daemon::config::Config::load(&config_path).map_err(failure)?;
        let client = c::ScopedClient::new(&config);
        match args.command {
            CloudCommand::Workspaces {
                limit,
                cursor,
                json,
            } => {
                let page: c::NativeCloudWorkspaces = serde_json::from_value(
                    client
                        .call(Request::NativeCloudWorkspaces {
                            store: args.store,
                            page_size: limit,
                            continuation: cursor,
                        })
                        .map_err(failure)?,
                )
                .map_err(failure)?;
                if json {
                    writeln!(
                        ui.stdout(),
                        "{}",
                        serde_json::to_string(&page).map_err(failure)?
                    )?;
                } else {
                    writeln!(
                        ui.stdout(),
                        "WORKSPACE                                                         GENERATION  LOCAL DESTINATION"
                    )?;
                    for row in page.workspaces {
                        writeln!(
                            ui.stdout(),
                            "{}  {:>10}  {}",
                            row.workspace_id,
                            row.generation,
                            row.local_destination
                                .map(|path| format!("{path:?}"))
                                .unwrap_or_else(|| "-".into())
                        )?;
                    }
                    writeln!(
                        ui.stdout(),
                        "Moving catalog; listed versions are selectors, not retention leases."
                    )?;
                    if !page.continuation.is_empty() {
                        writeln!(ui.stdout(), "Next page: --cursor {}", page.continuation)?;
                    }
                }
            }
            CloudCommand::Versions {
                workspace,
                limit,
                cursor,
                json,
            } => {
                let page: c::NativeCloudVersions = serde_json::from_value(
                    client
                        .call(Request::NativeCloudVersions {
                            store: args.store,
                            workspace_id: workspace,
                            page_size: limit,
                            continuation: cursor,
                        })
                        .map_err(failure)?,
                )
                .map_err(failure)?;
                if json {
                    writeln!(
                        ui.stdout(),
                        "{}",
                        serde_json::to_string(&page).map_err(failure)?
                    )?;
                } else {
                    writeln!(ui.stdout(), "GENERATION  MANIFEST")?;
                    for version in page.versions {
                        writeln!(
                            ui.stdout(),
                            "{:>10}  {}",
                            version.generation,
                            version.manifest_id
                        )?;
                    }
                    if page.through_generation > page.retired_through {
                        writeln!(
                            ui.stdout(),
                            "Retained generations {} through {}; restore verifies the chosen capsule.",
                            page.retired_through + 1,
                            page.through_generation
                        )?;
                    } else {
                        writeln!(ui.stdout(), "No retained cloud versions.")?;
                    }
                    if !page.continuation.is_empty() {
                        writeln!(ui.stdout(), "Next page: --cursor {}", page.continuation)?;
                    }
                }
            }
            CloudCommand::Restore {
                workspace,
                destination,
                generation,
                json,
            } => {
                let expected_source = vfs::restore_identity(&workspace)?;
                let destination = command.cwd().join(destination);
                let client = vfs::restore_control(&config_path)?;
                let report: c::NativeCloudRestored = serde_json::from_value(
                    client
                        .call(Request::NativeCloudRestore {
                            root: destination.clone(),
                            store: args.store.clone(),
                            source_workspace: workspace,
                            generation,
                        })
                        .map_err(failure)?,
                )
                .map_err(failure)?;
                if report.source.workspace_id != expected_source
                    || generation.is_some_and(|value| value != report.source.generation)
                {
                    return Err(failure(
                        "daemon restored a different cloud selection; preserve state",
                    ));
                }
                let binding = vfs::restored_response(
                    serde_json::to_value(&report.mounted).map_err(failure)?,
                    &destination,
                    Some(&args.store),
                    Some(report.mounted.binding.workspace_id),
                )?;
                if json {
                    writeln!(
                        ui.stdout(),
                        "{}",
                        serde_json::to_string(&report).map_err(failure)?
                    )?;
                } else {
                    writeln!(
                        ui.stdout(),
                        "Restored cloud generation {} into {}{}",
                        report.source.generation,
                        binding.root.display(),
                        if report.reused_destination {
                            " (saved source pin reused)"
                        } else {
                            ""
                        }
                    )?;
                }
            }
        }
        Ok(())
    }
}
