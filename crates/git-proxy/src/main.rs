use anyhow::{Context, Result};
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
#[derive(Parser)]
#[command(about = "Read-only configured-origin Git pull-through cache")]
struct Args {
    #[arg(long, required = true, value_name = "ALIAS=UPSTREAM")]
    repo: Vec<String>,
    #[arg(long)]
    cache_dir: PathBuf,
    #[arg(long, default_value = "git")]
    git: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8082")]
    listen: SocketAddr,
    #[arg(long, default_value_t = 10)]
    refresh_ttl_seconds: u64,
    #[arg(long)]
    enable_blake3: bool,
    /// Maximum completed and reserved generated-response bytes; 0 disables caching.
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    pack_cache_max_bytes: u64,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    pack_cache_max_entry_bytes: u64,
    #[arg(long, default_value_t = 1024)]
    pack_cache_max_entries: usize,
    #[arg(long, default_value_t = 300)]
    pack_cache_ttl_seconds: u64,
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let repositories = args
        .repo
        .into_iter()
        .map(|s| {
            s.split_once('=')
                .map(|(a, u)| (a.to_owned(), u.to_owned()))
                .context("--repo must be ALIAS=UPSTREAM")
        })
        .collect::<Result<Vec<_>>>()?;
    let router = git_proxy::router(git_proxy::Config {
        cache_dir: args.cache_dir,
        git: args.git,
        repositories,
        refresh_ttl: Duration::from_secs(args.refresh_ttl_seconds),
        enable_blake3: args.enable_blake3,
        pack_cache: git_proxy::PackCacheConfig {
            max_bytes: args.pack_cache_max_bytes,
            max_entry_bytes: args.pack_cache_max_entry_bytes,
            max_entries: args.pack_cache_max_entries,
            ttl: Duration::from_secs(args.pack_cache_ttl_seconds),
        },
    })?;
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!("git-proxy listening on {}", listener.local_addr()?);
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
