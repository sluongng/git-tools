//! Native jj entry point. User workspaces require daemon-owned lazy jj-vfs.
mod broker;
mod commands;
mod history;
mod landing;
mod query_source;
mod remote;
mod remote_state;
mod roots;
mod search;
mod vfs;
use std::path::PathBuf;

use jj_cli::cli_util::{CliRunner, CommandHelper};
use jj_cli::command_error::{CommandError, cli_error, user_error_with_message};
use jj_cli::ui::Ui;
use jj_lib::repo::StoreFactories;
use jj_lib::workspace::Workspace;

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
    /// Submit a frozen native stack to server-owned checks, or reconcile its result.
    Land(landing::LandArgs),
    /// Search an immutable revision, including local draft and dirty-file overlays.
    Search(search::SearchArgs),
    /// Inspect exact-path history without downloading historical file bodies.
    History(history::PathArgs),
    /// Attribute a file using remote history and local draft/working-copy edits.
    Blame(history::PathArgs),
    /// Resume a stopped or interrupted lazy workspace using its original identity.
    Resume {
        destination: PathBuf,
        /// Matching daemon configuration is required for restored origins.
        #[arg(long)]
        daemon_config: Option<PathBuf>,
    },
    /// Restore an exact retained V3 capture into a mandatory lazy workspace.
    Restore {
        destination: PathBuf,
        #[arg(long)]
        daemon_config: PathBuf,
        #[arg(long)]
        store: String,
        /// New destination identity, retained for exact retries.
        #[arg(long)]
        workspace_id: String,
        #[arg(long)]
        source_workspace: String,
        #[arg(long)]
        generation: u64,
        #[arg(long)]
        manifest: String,
        #[arg(long)]
        manifest_blake3: String,
        #[arg(long)]
        manifest_bytes: u64,
    },
    /// Stop an explicit lazy mount; run from outside its directory.
    Unmount { destination: PathBuf },
    /// Reconcile pending native mutations against authoritative controller receipts.
    Recover {
        destination: PathBuf,
        /// Full retained jj operation ID proving the applied workspace tree.
        #[arg(long)]
        operation: Option<String>,
    },
    /// Inspect retained native roots without snapshotting or collecting data.
    Roots {
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 10_000)]
        max_operations: usize,
        #[arg(long, default_value_t = 100_000)]
        max_roots: usize,
        #[arg(long, default_value_t = 8 * 1024 * 1024)]
        max_file_bytes: usize,
    },
    /// Clone a selected native reference into a daemon-owned lazy workspace.
    Clone {
        destination: PathBuf,
        #[arg(long)]
        broker: PathBuf,
        #[arg(long)]
        store: String,
        #[arg(long)]
        reference: String,
        #[arg(long)]
        bookmark: String,
    },
    /// Fetch one native reference, merging it into a selected local bookmark.
    Fetch {
        #[arg(long)]
        reference: String,
        #[arg(long)]
        bookmark: String,
        /// Archive a pending local push intent before fetching; does not cancel upstream work.
        #[arg(long)]
        abandon_pending_push: bool,
    },
    /// Publish a selected local bookmark using this workspace's observed generation.
    Push {
        #[arg(long)]
        reference: String,
        #[arg(long)]
        bookmark: String,
    },
    /// Initialize a native repository through a daemon-owned lazy workspace.
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
            command: TinytreeCommand::Land(args),
        } => landing::run(ui, command, args).await,
        NativeCommand::Tinytree {
            command: TinytreeCommand::Search(args),
        } => search::run(ui, command, args).await,
        NativeCommand::Tinytree {
            command: TinytreeCommand::History(args),
        } => history::run(ui, command, args, false).await,
        NativeCommand::Tinytree {
            command: TinytreeCommand::Blame(args),
        } => history::run(ui, command, args, true).await,
        NativeCommand::Tinytree {
            command:
                TinytreeCommand::Resume {
                    destination,
                    daemon_config,
                },
        } => vfs::resume(ui, command, &command.cwd().join(destination), daemon_config).await,
        NativeCommand::Tinytree {
            command:
                TinytreeCommand::Restore {
                    destination,
                    daemon_config,
                    store,
                    workspace_id,
                    source_workspace,
                    generation,
                    manifest,
                    manifest_blake3,
                    manifest_bytes,
                },
        } => {
            vfs::restore(
                ui,
                command,
                &command.cwd().join(destination),
                daemon_config,
                store,
                workspace_id,
                source_workspace,
                generation,
                manifest,
                manifest_blake3,
                manifest_bytes,
            )
            .await
        }
        NativeCommand::Tinytree {
            command: TinytreeCommand::Unmount { destination },
        } => vfs::unmount(command, &command.cwd().join(destination)),
        NativeCommand::Tinytree {
            command:
                TinytreeCommand::Recover {
                    destination,
                    operation,
                },
        } => vfs::recover(command, &command.cwd().join(destination), operation).await,
        NativeCommand::Tinytree {
            command:
                TinytreeCommand::Roots {
                    json,
                    max_operations,
                    max_roots,
                    max_file_bytes,
                },
        } => {
            use std::io::Write as _;
            if command.global_args().at_operation.is_some()
                || command.global_args().ignore_working_copy
                || command.global_args().no_integrate_operation
            {
                return Err(cli_error(
                    "root inventory requires the actual current workspace",
                ));
            }
            let limits = roots::CollectorLimits {
                max_operations,
                max_object_roots: max_roots,
                max_file_bytes,
                ..Default::default()
            };
            let workspace = load_audit_workspace(command, &limits)?;
            let checkpoint = roots::collect(&workspace, &limits).await.map_err(|error| {
                user_error_with_message("Cannot inventory native retention roots", error)
            })?;
            if json {
                let bytes = serde_json::to_vec(&checkpoint.audit())
                    .map_err(|_| cli_error("cannot encode native root inventory"))?;
                ui.stdout().write_all(&bytes)?;
                writeln!(ui.stdout())?;
            } else {
                writeln!(
                    ui.stdout(),
                    "{} retained operations, {} native object roots",
                    checkpoint.retained_operations.len(),
                    checkpoint.object_roots.len()
                )?;
                writeln!(
                    ui.stdout(),
                    "Read-only inventory; this does not authorize garbage collection."
                )?;
            }
            Ok(())
        }
        NativeCommand::Tinytree {
            command:
                TinytreeCommand::Clone {
                    destination,
                    broker,
                    store,
                    reference,
                    bookmark,
                },
        } => remote::clone(ui, command, destination, broker, store, reference, bookmark).await,
        NativeCommand::Tinytree {
            command:
                TinytreeCommand::Fetch {
                    reference,
                    bookmark,
                    abandon_pending_push,
                },
        } => remote::fetch(ui, command, reference, bookmark, abandon_pending_push).await,
        NativeCommand::Tinytree {
            command:
                TinytreeCommand::Push {
                    reference,
                    bookmark,
                },
        } => remote::push(ui, command, reference, bookmark).await,
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
            #[cfg(not(feature = "protocol-fixture"))]
            if locator.is_none() {
                return Err(cli_error(
                    "native initialization requires --broker and --store for a lazy mount",
                ));
            }
            #[cfg(not(feature = "protocol-fixture"))]
            broker::creation_policy(locator.as_ref().expect("validated broker locator")).map_err(
                |error| {
                    user_error_with_message("Native provider cannot create this workspace", error)
                },
            )?;
            let destination = command.cwd().join(destination);
            vfs::prepare_destination(&destination)
                .map_err(|error| user_error_with_message("Failed to create workspace", error))?;
            let destination = destination
                .canonicalize()
                .map_err(|error| user_error_with_message("Failed to resolve workspace", error))?;
            vfs::initialize(
                ui,
                command,
                &destination,
                locator,
                &jj_lib::backend::CommitId::new(vec![0; 32]),
            )
            .await?;
            Ok(())
        }
    }
}

/// Construct the audit workspace through bounded persisted WC reads. Loading
/// ordinary LocalWorkingCopy first would decode checkout before audit limits.
#[cfg(feature = "protocol-fixture")]
fn load_audit_workspace(
    command: &CommandHelper,
    limits: &roots::CollectorLimits,
) -> Result<Workspace, CommandError> {
    use jj_lib::local_working_copy::LocalWorkingCopy;
    use jj_lib::repo::RepoLoader;
    let loader = command.workspace_loader()?;
    if loader.get_working_copy_type().map_err(|error| {
        user_error_with_message("Cannot identify working-copy implementation", error)
    })? != LocalWorkingCopy::NAME
    {
        return Err(cli_error(
            "root inventory does not support this working-copy implementation",
        ));
    }
    let mut factories = jj_lib::default_backend_factories::default_backend_factories();
    broker::register(&mut factories);
    let repo_loader =
        RepoLoader::init_from_file_system(command.settings(), loader.repo_path(), &factories)
            .map_err(|error| {
                user_error_with_message("Cannot load native audit repository", error)
            })?;
    let root = loader.workspace_root().to_path_buf();
    let working_copy = LocalWorkingCopy::load_readonly(
        repo_loader.store().clone(),
        root.clone(),
        root.join(".jj/working_copy"),
        command.settings(),
        limits.max_file_bytes,
        limits.max_decode_bytes,
    )
    .map_err(|error| user_error_with_message("Cannot read bounded working-copy state", error))?;
    Workspace::new(
        &root,
        loader.repo_path().to_path_buf(),
        Box::new(working_copy),
        repo_loader,
    )
    .map_err(|error| user_error_with_message("Cannot construct native audit workspace", error))
}

#[cfg(not(feature = "protocol-fixture"))]
fn load_audit_workspace(
    command: &CommandHelper,
    limits: &roots::CollectorLimits,
) -> Result<Workspace, CommandError> {
    roots::load_native_workspace(command, limits).map_err(|error| {
        user_error_with_message("Cannot load bounded native root inventory", error)
    })
}

fn main() -> std::process::ExitCode {
    let mut factories = StoreFactories::empty();
    broker::register(&mut factories);
    let runner = CliRunner::init();
    #[cfg(not(feature = "protocol-fixture"))]
    let runner = {
        let mut working_copies = jj_lib::workspace::WorkingCopyFactories::new();
        working_copies.insert(
            jj_vfs::native::NAME.to_owned(),
            Box::new(vfs::WorkingCopyFactory) as _,
        );
        runner
            .add_dispatch_hook(async |ui, command, dispatch| {
                if let Ok(loader) = command.workspace_loader() {
                    let registry = native_workspace_admin::Registry::open_existing(
                        native_workspace_admin::Registry::default_path().map_err(|error| {
                            user_error_with_message("Cannot select native command registry", error)
                        })?,
                    )
                    .map_err(|error| {
                        user_error_with_message("Cannot open native command registry", error)
                    })?;
                    let binding = registry.lookup(loader.workspace_root()).map_err(|error| {
                        user_error_with_message("Cannot bind native command", error)
                    })?;
                    commands::ensure_binding(&binding).map_err(|error| {
                        user_error_with_message("Cannot admit native command", error)
                    })?;
                }
                dispatch.call(ui, command).await
            })
            .set_workspace_loader_factory(Box::new(vfs::Loader))
            .add_working_copy_factories(working_copies)
    };
    let status = runner
        .version(env!("CARGO_PKG_VERSION"))
        .add_store_factories(factories)
        .add_subcommand(run_native_command)
        .run();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    broker::drain_cleanup(deadline);
    commands::finish(deadline);
    status.into()
}

#[cfg(test)]
mod restore_arguments_tests {
    use super::*;
    use clap::Parser as _;
    #[test]
    fn retained_restore_requires_exact_source_and_configuration_without_clone_fallback() {
        let args = vec![
            "fixture".into(),
            "tinytree".into(),
            "restore".into(),
            "workspace".into(),
            "--daemon-config".into(),
            "daemon.toml".into(),
            "--store".into(),
            "example".into(),
            "--workspace-id".into(),
            "01".repeat(32),
            "--source-workspace".into(),
            "02".repeat(32),
            "--generation".into(),
            "3".into(),
            "--manifest".into(),
            "04".repeat(32),
            "--manifest-blake3".into(),
            "05".repeat(32),
            "--manifest-bytes".into(),
            "1024".into(),
        ];
        let parsed = NativeCommand::try_parse_from(args.clone()).unwrap();
        assert!(matches!(
            parsed,
            NativeCommand::Tinytree {
                command: TinytreeCommand::Restore {
                    generation: 3,
                    manifest_bytes: 1024,
                    ..
                }
            }
        ));
        let mut missing = args.clone();
        let config = missing.iter().position(|s| s == "--daemon-config").unwrap();
        missing.drain(config..config + 2);
        assert!(NativeCommand::try_parse_from(missing).is_err());
        let mut broker = args;
        broker.extend(["--broker".into(), "unconfigured.sock".into()]);
        assert!(
            NativeCommand::try_parse_from(broker).is_err(),
            "restore cannot select an arbitrary connector instead of configured daemon identity"
        );
        assert!(matches!(
            NativeCommand::try_parse_from(["fixture", "tinytree", "resume", "workspace"]).unwrap(),
            NativeCommand::Tinytree {
                command: TinytreeCommand::Resume {
                    daemon_config: None,
                    ..
                }
            }
        ));
        assert!(matches!(
            NativeCommand::try_parse_from([
                "fixture",
                "tinytree",
                "resume",
                "workspace",
                "--daemon-config",
                "daemon.toml"
            ])
            .unwrap(),
            NativeCommand::Tinytree {
                command: TinytreeCommand::Resume {
                    daemon_config: Some(_),
                    ..
                }
            }
        ));
    }
}
