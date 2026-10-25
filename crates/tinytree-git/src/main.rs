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
    Watch {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        watch_state: PathBuf,
        #[arg(long)]
        destination_git: PathBuf,
        #[arg(long, default_value = "main")]
        source_reference: String,
        #[arg(long, default_value = "refs/heads/main")]
        destination_reference: String,
        #[arg(long, default_value = "git")]
        git: PathBuf,
        #[arg(long, default_value_t = 5000)]
        poll_interval_ms: u64,
        #[arg(long, default_value_t = 5000)]
        probe_timeout_ms: u64,
        #[arg(long, default_value_t = 300000)]
        max_backoff_ms: u64,
        #[arg(long, default_value_t = 600)]
        timeout_seconds: u64,
    },
    WatchStatus {
        #[arg(long)]
        watch_state: PathBuf,
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
        Action::Watch {
            config,
            state,
            watch_state,
            destination_git,
            source_reference,
            destination_reference,
            git,
            poll_interval_ms,
            probe_timeout_ms,
            max_backoff_ms,
            timeout_seconds,
        } => {
            #[cfg(unix)]
            {
                use std::time::Duration;
                use tinytree_git::watch::{WatchOptions, start_watch};
                let mut export = tinytree_git::ExportOptions::new(state, destination_git, git);
                export.source_reference = source_reference;
                export.destination_reference = destination_reference;
                export.limits.timeout = Duration::from_secs(timeout_seconds);
                let mut options = WatchOptions::new(export, watch_state);
                options.poll_interval = Duration::from_millis(poll_interval_ms);
                options.probe_timeout = Duration::from_millis(probe_timeout_ms);
                options.max_backoff = Duration::from_millis(max_backoff_ms);
                let mut handle = start_watch(tinytree_client::Config::load(&config)?, options)?;
                let status = tokio::select! {result=handle.wait()=>result?,signal=tokio::signal::ctrl_c()=>{signal?;handle.shutdown().await?}};
                println!("{}", serde_json::to_string(&status)?);
            }
            #[cfg(not(unix))]
            anyhow::bail!("local watcher currently requires Unix");
        }
        Action::WatchStatus { watch_state } => {
            #[cfg(unix)]
            println!(
                "{}",
                serde_json::to_string(&tinytree_git::watch::watch_status(&watch_state)?)?
            );
            #[cfg(not(unix))]
            anyhow::bail!("local watcher currently requires Unix");
        }
        Action::Status { state } => {
            println!("{}", serde_json::to_string(&tinytree_git::status(&state)?)?)
        }
    }
    Ok(())
}
