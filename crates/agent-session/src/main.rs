use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    version,
    about = "Private Linux Git VFS + Bazel sessions (JSON results)"
)]
struct Cli {
    #[command(subcommand)]
    command: Operation,
}

#[derive(Subcommand)]
enum Operation {
    /// Create a new private session; the directory must not exist.
    Create {
        session: PathBuf,
        #[arg(long)]
        proxy_url: String,
        #[arg(long)]
        proxy_repo: String,
        #[arg(long)]
        reference: String,
        /// Full immutable SHA-1 commit, not a moving branch.
        #[arg(long)]
        revision: String,
        /// Provisioned RBE/r2c2 configuration outside the mount; no secrets copied.
        #[arg(long)]
        bazelrc: PathBuf,
        #[arg(long, default_value = "git-vfs")]
        git_vfs: PathBuf,
        #[arg(long, default_value = "git")]
        git: PathBuf,
        #[arg(long, default_value = "bazel")]
        bazel: PathBuf,
        #[arg(long, default_value_t = 60)]
        mount_timeout_seconds: u64,
        /// Existing absolute host directory shared by cooperating agents.
        #[arg(long)]
        repo_contents_cache: Option<PathBuf>,
        /// Enable remote repository contents caching independently of local sharing.
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true", conflicts_with = "no_remote_repo_contents_cache")]
        remote_repo_contents_cache: bool,
        /// Disable remote repository caching without changing local sharing.
        #[arg(long, conflicts_with = "remote_repo_contents_cache")]
        no_remote_repo_contents_cache: bool,
    },
    /// Reuse the pinned repository and durable overlay without recloning.
    Resume { session: PathBuf },
    /// Inspect mount/process readiness without changing the session.
    Status { session: PathBuf },
    /// Run Bazel build; extra arguments follow --. Never shares output bases.
    Build {
        session: PathBuf,
        #[arg(last = true, required = true)]
        args: Vec<String>,
    },
    /// Native git commit, using the existing index, hooks and signing settings.
    /// Stage changes yourself first. Extra commit arguments follow --.
    Save {
        session: PathBuf,
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// Shut down only this session's Bazel and verified VFS; retain all state.
    Stop { session: PathBuf },
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let session = match &cli.command {
        Operation::Create { session, .. }
        | Operation::Resume { session }
        | Operation::Status { session }
        | Operation::Build { session, .. }
        | Operation::Save { session, .. }
        | Operation::Stop { session } => session.clone(),
    };
    let result = match cli.command {
        Operation::Create {
            session,
            proxy_url,
            proxy_repo,
            reference,
            revision,
            bazelrc,
            git_vfs,
            git,
            bazel,
            mount_timeout_seconds,
            repo_contents_cache,
            remote_repo_contents_cache,
            no_remote_repo_contents_cache,
        } => agent_session::create(
            &session,
            agent_session::Settings {
                proxy_url,
                proxy_repo,
                reference,
                revision,
                bazelrc,
                git_vfs,
                git,
                bazel,
                mount_timeout_seconds,
                repo_contents_cache,
                remote_repo_contents_cache: remote_repo_contents_cache
                    && !no_remote_repo_contents_cache,
            },
        ),
        Operation::Resume { session } => agent_session::resume(&session),
        Operation::Status { session } => agent_session::status(&session),
        Operation::Build { session, args } => agent_session::build(&session, &args),
        Operation::Save { session, args } => agent_session::save(&session, &args),
        Operation::Stop { session } => agent_session::stop(&session),
    };
    let (value, exit) = match result {
        Ok(report) => (
            serde_json::json!({ "ok": true, "report": report }),
            std::process::ExitCode::SUCCESS,
        ),
        Err(error) => (
            serde_json::json!({ "ok": false, "error": error.to_string(), "report": agent_session::status(&session).ok() }),
            std::process::ExitCode::FAILURE,
        ),
    };
    if let Err(error) = serde_json::to_writer_pretty(std::io::stdout().lock(), &value) {
        eprintln!("writing session result: {error}");
        return std::process::ExitCode::FAILURE;
    }
    println!();
    exit
}
