use bazel_output_service::{
    Config, OutputService, proto::bazel_output_service_server::BazelOutputServiceServer,
};
use clap::{Parser, ValueEnum};
use output_core::{Projection, Tree};
use reapi::policy::Policy;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Storage {
    /// Select the native lazy backend for this operating system.
    Lazy,
    Fuse,
    Eager,
    Nfs,
    Projfs,
}

/// Local Bazel Remote Output Service with verified, demand-loaded outputs.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8377")]
    listen: SocketAddr,
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    cas_endpoint: String,
    #[arg(long, default_value = "")]
    instance_name: String,
    /// Name of the environment variable holding a BuildBuddy API key (not its value).
    #[arg(long, default_value = "BUILDBUDDY_API_KEY")]
    api_key_env: String,
    #[arg(long, default_value_t = 17_179_869_184)]
    max_blob_bytes: u64,
    /// Explicit TOML policy file. No configuration is loaded automatically.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override the per-attempt unary RPC timeout (not the total operation budget).
    #[arg(long)]
    rpc_timeout_secs: Option<u64>,
    #[arg(long, value_enum, default_value_t = Storage::Lazy)]
    storage: Storage,
    /// Durable namespace, writable payloads and verified cache; outside --root.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    /// Optional verified CAS directory, separate from authoritative namespace state.
    /// One owner holds this directory; library consumers can share its cache handle.
    #[arg(long)]
    cas_cache_dir: Option<PathBuf>,
    /// Maximum verified CAS cache bytes. A single blob must fit this budget.
    #[arg(long, default_value_t = 4_294_967_296)]
    max_cache_bytes: u64,
    #[arg(long, default_value_t = 8)]
    max_concurrent_fetches: usize,
    #[arg(long, default_value_t = 268_435_456)]
    max_inflight_bytes: u64,
    /// Optional atomic JSON counters for local diagnostics (never credentials).
    #[arg(long)]
    metrics_file: Option<PathBuf>,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = Args::parse();
    // Reject invalid policy before inspecting or mounting native storage.
    let policy = args.policy()?;
    args.storage = args.storage.resolve()?;
    anyhow::ensure!(
        args.listen.ip().is_loopback(),
        "unauthenticated service must bind loopback"
    );
    #[cfg(target_os = "macos")]
    if args.storage == Storage::Nfs {
        // Never canonicalize/stat a stale NFS mount before inspecting the mount
        // table: those calls can wait indefinitely for its departed server.
        output_nfs::ensure_unmounted(&args.root)?;
    }
    let api_key = match std::env::var(&args.api_key_env) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => anyhow::bail!("credential environment variable is not valid Unicode"),
    };
    let service = OutputService::new(Config {
        root: args.root.clone(),
        cas_endpoint: args.cas_endpoint.clone(),
        instance_name: args.instance_name.clone(),
        api_key,
        max_blob_bytes: args.max_blob_bytes,
        policy,
    })
    .await?;
    let (projection, tree) = if args.storage == Storage::Eager {
        (None, None)
    } else {
        let (projection, tree) = mount_projection(&args, &service).await?;
        (Some(projection), Some(tree))
    };
    // Once a native mount exists, every subsequent error must pass through
    // shutdown. Dropping the runtime under a busy NFS mount would strand it.
    let result = async {
        let service = if let Some(projection) = &projection {
            service.with_projection(projection.clone(), tree.as_ref().unwrap().clone())?
        } else {
            service
        };
        eprintln!(
            "Bazel Remote Output Service listening on {} ({:?})",
            args.listen, args.storage
        );
        let metrics_task = if let Some(path) = args.metrics_file.clone() {
            write_metrics(&path, &service)?;
            let service = service.clone();
            Some(tokio::spawn(async move {
                let mut ticks = tokio::time::interval(Duration::from_millis(50));
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticks.tick().await;
                    // Readers on Windows can transiently deny rename sharing.
                    let _ = write_metrics(&path, &service);
                }
            }))
        } else {
            None
        };
        let result = tonic::transport::Server::builder()
            .add_service(
                BazelOutputServiceServer::new(service.clone())
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .send_compressed(tonic::codec::CompressionEncoding::Gzip),
            )
            .serve_with_shutdown(args.listen, async {
                let _ = wait_for_shutdown().await;
            })
            .await;
        eprintln!("output_service_stats {:?}", service.metrics());
        if let Some(task) = metrics_task {
            task.abort();
            let _ = task.await;
        }
        result?;
        if let Some(path) = &args.metrics_file {
            write_metrics(path, &service)?;
        }
        anyhow::Ok(())
    }
    .await;
    if let Some(tree) = &tree {
        eprintln!("output_cache_stats {:?}", tree.cache_metrics());
    }
    if let Some(projection) = &projection {
        while let Err(error) = projection.shutdown().await {
            eprintln!(
                "output mount shutdown failed: {error:#}; keeping the filesystem server alive. \
                 Release open files or unmount it, then interrupt this process again to retry."
            );
            wait_for_shutdown().await?;
        }
    }
    result
}

impl Storage {
    fn resolve(self) -> anyhow::Result<Self> {
        if self != Self::Lazy {
            return Ok(self);
        }
        #[cfg(target_os = "linux")]
        return Ok(Self::Fuse);
        #[cfg(target_os = "macos")]
        return Ok(Self::Nfs);
        #[cfg(windows)]
        return Ok(Self::Projfs);
        #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
        anyhow::bail!("no native lazy output backend for this platform")
    }
}

impl Args {
    fn policy(&self) -> anyhow::Result<Policy> {
        let mut policy = match &self.config {
            Some(path) => Policy::load(path)?,
            None => Policy::default(),
        };
        if let Some(seconds) = self.rpc_timeout_secs {
            policy.rpc_timeout_ms = seconds
                .checked_mul(1_000)
                .ok_or_else(|| anyhow::anyhow!("--rpc-timeout-secs is too large"))?;
        }
        policy.validate()?;
        Ok(policy)
    }
}

async fn wait_for_shutdown() -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        // Supervisors send CTRL_BREAK to the owned process group.
        let mut breaks = tokio::signal::windows::ctrl_break()?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = breaks.recv() => {},
        }
    }
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    Ok(())
}

fn write_metrics(path: &std::path::Path, service: &OutputService) -> anyhow::Result<()> {
    let metrics = service.metrics();
    let data = serde_json::json!({
        "sampled_at_ns": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos(),
        "stage_requests": metrics.stage_requests, "staged_files": metrics.staged_files,
        "verified_bytes": metrics.verified_bytes, "lazy_fetches": metrics.lazy_fetches,
        "lazy_verified_bytes": metrics.lazy_verified_bytes, "lazy_cache_hits": metrics.lazy_cache_hits,
        "zstd_read_requests": metrics.zstd_read_requests, "cas_read_wire_bytes": metrics.cas_read_wire_bytes,
    });
    let temp = path.with_extension(format!("{}.tmp", std::process::id()));
    std::fs::write(&temp, serde_json::to_vec(&data)?)?;
    std::fs::rename(temp, path)?;
    Ok(())
}

async fn mount_projection(
    args: &Args,
    service: &OutputService,
) -> anyhow::Result<(Arc<dyn Projection>, Arc<Tree>)> {
    let state = args
        .state_dir
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("--state-dir is required for lazy storage"))?;
    std::fs::create_dir_all(state)?;
    let state = std::fs::canonicalize(state)?;
    let root = std::fs::canonicalize(&args.root)?;
    anyhow::ensure!(
        !state.starts_with(&root) && !root.starts_with(&state),
        "state directory and output mount must not overlap"
    );
    let cache = args
        .cas_cache_dir
        .clone()
        .unwrap_or_else(|| state.join("cache"));
    std::fs::create_dir_all(&cache)?;
    let cache = std::fs::canonicalize(cache)?;
    anyhow::ensure!(
        !cache.starts_with(&root) && !root.starts_with(&cache),
        "CAS cache directory and output mount must not overlap"
    );
    anyhow::ensure!(
        cache != state && !state.starts_with(&cache),
        "CAS cache directory must not contain authoritative namespace state"
    );
    let cache = local_cas::LocalCas::open(
        &cache,
        service.blob_source(),
        output_core::CacheConfig {
            max_bytes: args.max_cache_bytes,
            max_concurrent_fetches: args.max_concurrent_fetches,
            max_inflight_bytes: args.max_inflight_bytes,
            operation_timeout: service.policy().operation_timeout(),
            fetch_timeout: service.policy().operation_timeout(),
        },
    )?;
    let namespace = Tree::open_with_cache(&state, service.backend_identity(), cache)?;
    let projection: Arc<dyn Projection> = match args.storage {
        Storage::Fuse => {
            #[cfg(target_os = "linux")]
            {
                output_fuse::start(namespace.clone(), &root).await?
            }
            #[cfg(not(target_os = "linux"))]
            {
                anyhow::bail!("FUSE lazy output storage requires Linux")
            }
        }
        Storage::Nfs => {
            #[cfg(target_os = "macos")]
            {
                Arc::new(output_nfs::NfsMount::start(namespace.clone(), &root).await?)
            }
            #[cfg(not(target_os = "macos"))]
            {
                anyhow::bail!("NFS lazy storage requires the native macOS client")
            }
        }
        Storage::Projfs => {
            #[cfg(windows)]
            {
                Arc::new(
                    output_projfs::start(namespace.clone(), &root, &state.join("provider")).await?,
                )
            }
            #[cfg(not(windows))]
            {
                anyhow::bail!("ProjFS lazy storage requires native Windows with ProjFS enabled")
            }
        }
        Storage::Eager | Storage::Lazy => unreachable!(),
    };
    Ok((projection, namespace))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(extra: &[&str]) -> Args {
        Args::try_parse_from(
            [
                "git-output-service",
                "--root",
                "outputs",
                "--cas-endpoint",
                "grpc://127.0.0.1:1985",
            ]
            .into_iter()
            .chain(extra.iter().copied()),
        )
        .unwrap()
    }

    #[test]
    fn explicit_config_and_cli_override_preserve_other_budgets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("policy.toml");
        std::fs::write(
            &path,
            "[reapi]\nrpc_timeout_ms = 23000\noperation_timeout_ms = 91000\n",
        )
        .unwrap();
        let path = path.to_str().unwrap();
        let from_file = args(&["--config", path]).policy().unwrap();
        assert_eq!(from_file.rpc_timeout(), Duration::from_secs(23));
        let overridden = args(&["--config", path, "--rpc-timeout-secs", "7"])
            .policy()
            .unwrap();
        assert_eq!(overridden.rpc_timeout(), Duration::from_secs(7));
        assert_eq!(overridden.operation_timeout(), Duration::from_secs(91));
        assert_eq!(args(&[]).policy().unwrap(), Policy::default());
    }

    #[test]
    fn native_lazy_storage_is_default_and_eager_requires_selection() {
        let defaults = args(&[]);
        assert_eq!(defaults.storage, Storage::Lazy);
        let native = defaults.storage.resolve().unwrap();
        #[cfg(target_os = "linux")]
        assert_eq!(native, Storage::Fuse);
        #[cfg(target_os = "macos")]
        assert_eq!(native, Storage::Nfs);
        #[cfg(windows)]
        assert_eq!(native, Storage::Projfs);
        assert_eq!(
            args(&["--storage", "eager"]).storage.resolve().unwrap(),
            Storage::Eager
        );
    }

    #[test]
    fn invalid_override_is_rejected_before_startup() {
        assert!(args(&["--rpc-timeout-secs", "0"]).policy().is_err());
        assert!(
            args(&["--rpc-timeout-secs", "18446744073709551615"])
                .policy()
                .is_err()
        );
    }
}
