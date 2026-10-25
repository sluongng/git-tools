use clap::{Parser, Subcommand};
use std::path::PathBuf;
#[derive(Parser)]
#[command(about = "Export native public parent ancestry to a local bare Git destination")]
struct Args {
    #[command(subcommand)]
    command: Action,
}
#[derive(Subcommand)]
enum Action {
    Export {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        destination_git: PathBuf,
        #[arg(long, default_value = "main")]
        source_reference: String,
        #[arg(long, default_value = "refs/heads/main")]
        destination_reference: String,
        #[arg(long, default_value = "git")]
        git: PathBuf,
        #[arg(long, default_value_t = 600)]
        timeout_seconds: u64,
    },
    Status {
        #[arg(long)]
        state: PathBuf,
    },
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Args::parse().command {
        Action::Export {
            config,
            state,
            destination_git,
            source_reference,
            destination_reference,
            git,
            timeout_seconds,
        } => {
            let mut options = tinytree_git::ExportOptions::new(state, destination_git, git);
            options.source_reference = source_reference;
            options.destination_reference = destination_reference;
            options.limits.timeout = std::time::Duration::from_secs(timeout_seconds);
            let result =
                tinytree_git::export(tinytree_client::Config::load(&config)?, options).await?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Action::Status { state } => {
            println!("{}", serde_json::to_string(&tinytree_git::status(&state)?)?)
        }
    }
    Ok(())
}
