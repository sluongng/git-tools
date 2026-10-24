use clap::Parser;
use std::{path::PathBuf, time::Duration};
use tinytree_import::{Limits, Options, import_snapshot_at};

#[derive(Parser)]
#[command(about = "Seed one immutable Git snapshot into a native Tinytree reference")]
struct Args {
    #[arg(long)]
    source_git: PathBuf,
    #[arg(long)]
    revision: String,
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    reference: String,
    #[arg(long, default_value_t = 0)]
    expected_generation: u64,
    #[arg(long, default_value = "git")]
    git: PathBuf,
    #[arg(long, default_value_t = 100_000)]
    max_entries: usize,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_file_bytes: usize,
    #[arg(long, default_value_t = 2 * 1024 * 1024 * 1024)]
    max_total_bytes: u64,
    #[arg(long, default_value_t = 600)]
    timeout_seconds: u64,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let limits = Limits {
        max_entries: args.max_entries,
        max_file_bytes: args.max_file_bytes,
        max_total_bytes: args.max_total_bytes,
        timeout: Duration::from_secs(args.timeout_seconds),
        ..Default::default()
    };
    limits.validate()?;
    let deadline = tokio::time::Instant::now() + limits.timeout;
    let config = tinytree_client::Config::load(&args.config)?;
    let repository_id = config.repository_id.clone();
    let client = tokio::time::timeout_at(deadline, tinytree_client::NativeClient::connect(config))
        .await
        .map_err(|_| anyhow::anyhow!("native client connection deadline exceeded"))??;
    let report = import_snapshot_at(
        client,
        Options {
            source_git: args.source_git,
            git_program: args.git,
            revision: args.revision,
            repository_id,
            reference: args.reference,
            expected_generation: args.expected_generation,
            limits,
        },
        deadline,
    )
    .await?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
