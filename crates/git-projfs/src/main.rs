use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Writable Windows ProjFS projection from a bare Git partial clone")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Start or resume a fixed snapshot. Ctrl-C stops the provider, preserving files.
    Mount {
        #[arg(long)]
        repo: PathBuf,
        #[arg(long, default_value = "HEAD")]
        revision: String,
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        state: PathBuf,
        #[arg(long, default_value = "git")]
        git: PathBuf,
        /// Environment variable holding a GitHub API token (never a token value).
        #[arg(long)]
        github_token_env: Option<String>,
    },
    /// Compute Git's blob digest of the actual mounted bytes (hydrates the file).
    Digest { path: PathBuf },
}
fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Digest { path } => println!("{}", git_projfs::gitsha1(&path)?),
        Command::Mount {
            repo,
            revision,
            root,
            state,
            git,
            github_token_env,
        } => {
            #[cfg(all(windows, feature = "native"))]
            {
                let _projection = git_projfs::windows::start_with_size_options(
                    &repo,
                    &revision,
                    &root,
                    &state,
                    &git,
                    &git_workspace::backend::SizeWarmOptions {
                        github_token_env,
                        ..Default::default()
                    },
                )?;
                let (sender, receiver) = std::sync::mpsc::channel();
                ctrlc::set_handler(move || {
                    let _ = sender.send(());
                })?;
                eprintln!(
                    "ProjFS serving {} at {revision}; local writes persist after shutdown",
                    root.display()
                );
                receiver.recv()?;
            }
            #[cfg(not(all(windows, feature = "native")))]
            {
                let _ = (repo, revision, root, state, git, github_token_env);
                anyhow::bail!(
                    "mount requires the native Cargo feature, Windows 10 2004+, Client-ProjFS enabled, and an NTFS volume"
                );
            }
        }
    }
    Ok(())
}
