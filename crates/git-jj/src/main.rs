use std::path::PathBuf;

use git_jj::{NAME, VfsWorkingCopyFactory};
use jj_cli::cli_util::{CliRunner, CommandHelper};
use jj_cli::command_error::{CommandError, user_error};
use jj_cli::ui::Ui;
use jj_lib::backend::Backend;
use jj_lib::git;
use jj_lib::git_backend::GitBackend;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::ReadonlyRepo;
use jj_lib::settings::UserSettings;
use jj_lib::signing_factory::signer_from_settings;
use jj_lib::workspace::{WorkingCopyFactories, Workspace, WorkspaceInitError};

#[derive(clap::Parser, Clone, Debug)]
enum VfsCommand {
    /// Adopt an existing writable git-vfs mount without reading its file blobs.
    InitVfs {
        /// Persistent overlay directory used by the mount (not the bare clone).
        #[arg(long)]
        overlay: PathBuf,
    },
}

async fn run_vfs_command(
    ui: &mut Ui,
    command: &CommandHelper,
    args: VfsCommand,
) -> Result<(), CommandError> {
    match args {
        VfsCommand::InitVfs { overlay } => {
            let root = command.cwd();
            let overlay = overlay
                .canonicalize()
                .map_err(|e| user_error(e.to_string()))?;
            let git_executable =
                git::GitSettings::from_settings(command.settings())?.executable_path;
            let output = |arguments: &[&str]| -> Result<String, CommandError> {
                let out = std::process::Command::new(&git_executable)
                    .current_dir(root)
                    .args(arguments)
                    .output()
                    .map_err(|e| user_error(e.to_string()))?;
                if !out.status.success() {
                    return Err(user_error(
                        "init-vfs requires an active writable git-vfs mount with native Git metadata",
                    ));
                }
                String::from_utf8(out.stdout)
                    .map(|s| s.trim().to_owned())
                    .map_err(|e| user_error(e.to_string()))
            };
            let gitdir = PathBuf::from(output(&["rev-parse", "--absolute-git-dir"])?);
            let top = PathBuf::from(output(&["rev-parse", "--show-toplevel"])?);
            if top.canonicalize().map_err(|e| user_error(e.to_string()))?
                != root.canonicalize().map_err(|e| user_error(e.to_string()))?
            {
                return Err(user_error(
                    "run init-vfs at the mount root, not in a subdirectory",
                ));
            }
            let expected_gitdir = overlay
                .join("git")
                .canonicalize()
                .map_err(|e| user_error(e.to_string()))?;
            if gitdir
                .canonicalize()
                .map_err(|e| user_error(e.to_string()))?
                != expected_gitdir
            {
                return Err(user_error(
                    "overlay does not belong to the mounted Git working tree",
                ));
            }
            let base = output(&["rev-parse", "--verify", "HEAD^{commit}"])?;
            if output(&["ls-tree", "-rz", "--format=%(objectmode)", "HEAD"])?
                .split('\0')
                .any(|mode| mode == "160000")
            {
                return Err(user_error(
                    "Git submodules are not supported by the VFS working-copy backend",
                ));
            }
            let (settings, _) = command.settings_for_new_workspace(ui, root)?;
            let initializer = |settings: &UserSettings, store: &std::path::Path| {
                let backend: Box<dyn Backend> =
                    Box::new(GitBackend::init_external(settings, store, &gitdir)?);
                Ok(backend)
            };
            let (workspace, repo) = Workspace::init_with_factories(
                &settings,
                root,
                &initializer,
                signer_from_settings(&settings).map_err(WorkspaceInitError::SignInit)?,
                &ReadonlyRepo::default_workspace_store_initializer(),
                &ReadonlyRepo::default_op_store_initializer(),
                &ReadonlyRepo::default_op_heads_store_initializer(),
                &ReadonlyRepo::default_index_store_initializer(),
                &ReadonlyRepo::default_submodule_store_initializer(),
                &VfsWorkingCopyFactory {
                    initial_overlay: Some(overlay),
                    initial_base: Some(base),
                },
                WorkspaceName::DEFAULT.to_owned(),
            )
            .await?;
            let mut workspace = command.for_workable_repo(ui, workspace, repo)?;
            let mut tx = workspace.start_transaction();
            let head = git::import_head_commit(tx.repo_mut())
                .await?
                .ok_or_else(|| user_error("mounted Git repository has no HEAD"))?;
            tx.check_out(&head)?;
            tx.finish(ui, "adopt Git VFS projected tree").await?;
            Ok(())
        }
    }
}

fn main() -> std::process::ExitCode {
    let mut factories = WorkingCopyFactories::new();
    factories.insert(
        NAME.to_owned(),
        Box::new(VfsWorkingCopyFactory {
            initial_overlay: None,
            initial_base: None,
        }) as _,
    );
    CliRunner::init()
        .version(env!("CARGO_PKG_VERSION"))
        .add_working_copy_factories(factories)
        .add_subcommand(run_vfs_command)
        .run()
        .into()
}
