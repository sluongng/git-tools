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
    /// Opt-in BLAKE3 CAS publication; http(s) origin, not a Git endpoint.
    #[arg(long)]
    cas_endpoint: Option<String>,
    #[arg(long, default_value = "")]
    cas_instance_name: String,
    /// HEADER=ENV_VAR; values never appear in arguments or configuration logs.
    #[arg(long, value_name = "HEADER=ENV_VAR")]
    cas_header_env: Vec<String>,
    /// Match Bazel --experimental_remote_cache_ttl; not negotiated by REAPI.
    #[arg(long, default_value_t = 10800)]
    cas_remote_ttl_seconds: u64,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    cas_staging_limit_bytes: u64,
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
    let cas_sync = args
        .cas_endpoint
        .map(|endpoint| {
            let header_env = args
                .cas_header_env
                .iter()
                .map(|entry| {
                    entry
                        .split_once('=')
                        .map(|(header, env)| (header.to_owned(), env.to_owned()))
                        .filter(|(header, env)| !header.is_empty() && !env.is_empty())
                        .context("--cas-header-env requires HEADER=ENV_VAR")
                })
                .collect::<Result<Vec<_>>>()?;
            Ok::<_, anyhow::Error>(git_proxy::CasSyncConfig {
                endpoint,
                instance_name: args.cas_instance_name,
                header_env,
                remote_ttl: Duration::from_secs(args.cas_remote_ttl_seconds),
                staging_limit_bytes: args.cas_staging_limit_bytes,
            })
        })
        .transpose()?;
    let (router, cas_shutdown) = git_proxy::router_with_cas_shutdown(git_proxy::Config {
        cache_dir: args.cache_dir,
        git: args.git,
        repositories,
        refresh_ttl: Duration::from_secs(args.refresh_ttl_seconds),
        enable_blake3: args.enable_blake3,
        cas_sync,
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
    if let Some(cas) = cas_shutdown {
        cas.shutdown().await;
    }
    Ok(())
}
