//! Single-process product façade over the native VCS and hosted forge service.
mod context;
mod output;
mod pending;
mod reconcile;
mod review;
mod rpc;
mod service;
pub(crate) use crate::forge_source as source;
mod transform;
mod upload;
mod workspace;

use futures::StreamExt as _;
use jj_cli::{
    cli_util::{CliRunner, CommandHelper},
    command_error::{CommandError, user_error},
    ui::Ui,
};
use jj_lib::{backend::TreeValue, matchers::EverythingMatcher, object_id::ObjectId as _};
use serde_json::{Value, json};
use std::io::Write as _;

#[derive(clap::Args, Clone, Debug)]
#[command(
    long_about = "Tinytree VCS and code review. Work in an account's monorepo through focused views; use one CLI for source changes and review."
)]
struct Global {
    /// Emit one versioned JSON result for machine-supported commands.
    #[arg(long, global = true)]
    json: bool,
}

#[derive(clap::Subcommand, Clone, Debug)]
enum Command {
    /// Copy a tracked file with explicit native lineage.
    Copy(crate::forge_paths::PathArgs),
    /// Move a tracked file with explicit native lineage.
    Move(crate::forge_paths::PathArgs),
    /// Create a lazy native workspace from an authorized account/view snapshot.
    Work(workspace::WorkArgs),
    /// Submit an immutable patchset for server-side rebase and review, not main publication.
    Push(review::PushArgs),
    /// Inspect, discuss, approve or land exact review patchsets.
    Review {
        #[command(subcommand)]
        command: review::ReviewCommand,
    },
    /// Submit or inspect a declarative cross-account transformation.
    Transform {
        #[command(subcommand)]
        command: transform::TransformCommand,
    },
    /// Create, read or retire retained source identities without release tags.
    Pin {
        #[command(subcommand)]
        command: transform::PinCommand,
    },
    /// Inspect explicit copy/rename origins for a retained transform.
    Lineage(transform::LineageArgs),
    /// Authenticate a configured account and save its routing context.
    Login(service::LoginArgs),
    /// Provision or inspect the account's single canonical monorepo.
    Account {
        #[command(subcommand)]
        command: service::AccountCommand,
    },
    /// Inspect or publish authorized monorepo views.
    View {
        #[command(subcommand)]
        command: service::ViewCommand,
    },
    /// Search code in the selected authorized account view.
    Search(service::SearchArgs),
    /// Inspect or replay an exact saved outbound intent after interruption.
    Request {
        #[command(subcommand)]
        command: service::RequestCommand,
    },
    /// Inspect or select the local account/view context; never stores token values.
    Context {
        #[command(subcommand)]
        command: ContextCommand,
    },
}

#[derive(clap::Subcommand, Clone, Debug)]
enum ContextCommand {
    /// Display configured routing context, not a verified login session.
    Show,
    /// Persist routing context. The service still authenticates every request.
    Set {
        #[arg(long)]
        server: String,
        #[arg(long)]
        account: String,
        #[arg(long, default_value = "public")]
        view: String,
        #[arg(long)]
        token_env: Option<String>,
        #[arg(long)]
        ca_certificate: Option<std::path::PathBuf>,
        #[arg(long)]
        tls_domain: Option<String>,
    },
}

pub(crate) fn configure<'a>(runner: CliRunner<'a>) -> CliRunner<'a> {
    runner
        .name("tt")
        .about("Tinytree VCS and code review")
        .add_global_args::<Global, _>(|_ui, _args| Ok(()))
        .add_subcommand(run)
        .add_dispatch_hook(async |ui, command, dispatch| {
            let machine = machine(command);
            let operation = command.matches().subcommand_name().unwrap_or("unknown");
            let result = if machine && matches!(operation, "status" | "diff") {
                workspace_summary(ui, command, operation).await
            } else if machine && !matches!(operation, "context" | "login" | "account" | "view" | "search" | "request" | "push" | "review" | "transform" | "pin" | "lineage" | "work" | "copy" | "move") {
                Err(user_error("this command has no stable Tinytree JSON schema; omit --json for native human output"))
            } else {
                dispatch.call(ui, command).await
            };
            if let Err(error) = &result
                && machine
            {
                output::error(ui, operation, error)?;
            }
            result
        })
}

fn machine(command: &CommandHelper) -> bool {
    command
        .matches()
        .try_get_one::<bool>("json")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(false)
}

async fn run(ui: &mut Ui, command: &CommandHelper, args: Command) -> Result<(), CommandError> {
    match args {
        Command::Copy(args) => {
            let result = crate::forge_paths::run(ui, command, args, false).await?;
            if machine(command) {
                output::write(ui, "copy", "copied", result)
            } else {
                Ok(())
            }
        }
        Command::Move(args) => {
            let result = crate::forge_paths::run(ui, command, args, true).await?;
            if machine(command) {
                output::write(ui, "move", "moved", result)
            } else {
                Ok(())
            }
        }
        Command::Work(args) => workspace::work(ui, command, machine(command), args).await,
        Command::Transform { command: args } => {
            transform::transform(ui, machine(command), args).await
        }
        Command::Pin { command: args } => transform::pin(ui, machine(command), args).await,
        Command::Lineage(args) => transform::lineage(ui, machine(command), args).await,
        Command::Push(args) => review::push(ui, command, machine(command), args).await,
        Command::Review { command: args } => review::run(ui, machine(command), args).await,
        Command::Login(args) => service::login(ui, machine(command), args).await,
        Command::Account { command: args } => service::account(ui, machine(command), args).await,
        Command::View { command: args } => service::view(ui, machine(command), args).await,
        Command::Search(args) => service::search(ui, machine(command), args).await,
        Command::Request { command: args } => {
            service::request(ui, command, machine(command), args).await
        }
        Command::Context { command: args } => {
            let value = match args {
                ContextCommand::Show => {
                    context::Context::load(&context::open().map_err(user_error)?)
                        .map_err(user_error)?
                }
                ContextCommand::Set {
                    server,
                    account,
                    view,
                    token_env,
                    ca_certificate,
                    tls_domain,
                } => {
                    let value = context::Context {
                        schema_version: 1,
                        server,
                        account_id: account,
                        view,
                        token_env,
                        ca_certificate,
                        tls_domain,
                    };
                    value
                        .save(&context::create().map_err(user_error)?)
                        .map_err(user_error)?;
                    value
                }
            };
            if machine(command) {
                output::write(ui, "context", "configured", value)
            } else {
                writeln!(
                    ui.stdout(),
                    "Account: {}\nView: {}\nServer: {}",
                    value.account_id,
                    value.view,
                    value.server
                )?;
                Ok(())
            }
        }
    }
}

fn value(value: &jj_lib::backend::MergedTreeValue) -> Value {
    match value.as_resolved() {
        Some(None) => Value::Null,
        Some(Some(TreeValue::File {
            id,
            executable,
            copy_id,
        })) => json!({"kind":"file","id":id.hex(),"executable":executable,"copy_id":copy_id.hex()}),
        Some(Some(TreeValue::Symlink(id))) => json!({"kind":"symlink","id":id.hex()}),
        Some(Some(TreeValue::Tree(id))) => json!({"kind":"tree","id":id.hex()}),
        Some(Some(TreeValue::GitSubmodule(id))) => json!({"kind":"git_submodule","id":id.hex()}),
        None => json!({"kind":"conflict"}),
    }
}

async fn workspace_summary(
    ui: &Ui,
    command: &CommandHelper,
    operation: &str,
) -> Result<(), CommandError> {
    let matches = command.matches().subcommand().unwrap().1;
    // Do not silently ignore filesets/revisions/options accepted by jj's richer
    // human commands. The machine summary has one explicit current-WC meaning.
    let allowed = [
        "json",
        "repository",
        "color",
        "quiet",
        "no_pager",
        "config",
        "config_file",
        "debug",
    ];
    if matches.ids().any(|id| {
        matches.value_source(id.as_str()) == Some(clap::parser::ValueSource::CommandLine)
            && !allowed.contains(&id.as_str())
    }) {
        return Err(user_error(
            "machine status/diff supports only the full current working-copy tree summary; remove revision, fileset and formatting options",
        ));
    }
    let (helper, stats, _) = command.workspace_helper_with_stats(ui).await?;
    let commit = helper
        .get_wc_commit()
        .await?
        .ok_or_else(|| user_error("no working-copy revision"))?;
    let parent_tree = commit.parent_tree(helper.repo().as_ref()).await?;
    let tree = commit.tree();
    let matcher = EverythingMatcher;
    let mut stream = parent_tree.diff_stream(&tree, &matcher);
    let mut changed = Vec::new();
    let mut path_bytes = 0usize;
    if commit.parent_ids().len() > 128 {
        return Err(user_error("machine summary parent limit exceeded"));
    }
    while let Some(entry) = stream.next().await {
        if changed.len() >= 16_384 {
            return Err(user_error(
                "machine working-copy summary path limit exceeded",
            ));
        }
        path_bytes += entry.path.as_internal_file_string().len();
        if path_bytes > 1024 * 1024 || entry.path.as_internal_file_string().len() > 4096 {
            return Err(user_error("machine summary path byte limit exceeded"));
        }
        let values = entry.values?;
        changed.push(json!({"path":entry.path.as_internal_file_string(),"before":value(&values.before),"after":value(&values.after)}));
    }
    let untracked = stats
        .untracked_paths
        .keys()
        .map(|p| p.as_internal_file_string())
        .collect::<Vec<_>>();
    if untracked.len() > 16384
        || untracked.iter().any(|p| p.len() > 4096)
        || untracked.iter().map(|p| p.len()).sum::<usize>() > 1024 * 1024
    {
        return Err(user_error("machine summary untracked path limit exceeded"));
    }
    let scope=workspace::Scope::load(command.workspace_loader()?.workspace_root()).ok().map(|s|json!({"account_id":s.context.account_id,"view":s.context.view,"view_version":workspace::hex(&s.view_version),"base":workspace::hex(&s.base)}));
    output::write(
        ui,
        operation,
        "complete",
        json!({
            "format":"tree_summary", "revision_id":commit.id().hex(), "change_id":commit.change_id().hex(),
            "operation_id":helper.repo().op_id().hex(), "parents":commit.parent_ids().iter().map(|id|id.hex()).collect::<Vec<_>>(),
            "changes":changed,"untracked":untracked,"workspace":scope,
        }),
    )
}
