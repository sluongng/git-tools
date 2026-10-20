use bazel_output_service::{
    Config, OutputService, proto::bazel_output_service_server::BazelOutputServiceServer,
};
use clap::{Parser, ValueEnum};
use output_core::{Projection, Tree};
use reapi::policy::Policy;
use serde::Deserialize;
use std::io::Read;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Storage {
    /// Select the native lazy backend for this operating system.
    Lazy,
    Fuse,
    #[value(skip)]
    #[serde(skip)]
    Eager,
    Nfs,
    Projfs,
}

/// Local Bazel Remote Output Service with verified, demand-loaded outputs.
#[derive(Parser)]
struct Args {
    #[arg(long)]
    listen: Option<SocketAddr>,
    #[arg(long)]
    root: Option<PathBuf>,
    #[arg(long)]
    cas_endpoint: Option<String>,
    #[arg(long)]
    instance_name: Option<String>,
    /// Name of the environment variable holding a BuildBuddy API key (not its value).
    #[arg(long)]
    api_key_env: Option<String>,
    #[arg(long)]
    max_blob_bytes: Option<u64>,
    /// Explicit TOML configuration. No configuration is loaded automatically.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override the per-attempt unary RPC timeout (not the total operation budget).
    #[arg(long)]
    rpc_timeout_secs: Option<u64>,
    /// Lazy is the default. Native backend selection never falls back to eager.
    #[arg(long, value_enum, conflicts_with = "debug_eager")]
    storage: Option<Storage>,
    /// DEBUG ONLY: download every staged blob to an ordinary directory.
    #[arg(long, conflicts_with = "storage")]
    debug_eager: bool,
    /// Durable namespace and writable payloads, outside --root. Required for lazy storage.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    /// Optional verified CAS directory. One process owns the directory.
    #[arg(long)]
    cas_cache_dir: Option<PathBuf>,
    #[arg(long)]
    max_cache_bytes: Option<u64>,
    #[arg(long)]
    max_concurrent_fetches: Option<usize>,
    #[arg(long)]
    max_inflight_bytes: Option<u64>,
    /// Additional streamed-to-disk admission budget for larger verified blobs.
    #[arg(long)]
    max_oversized_inflight_bytes: Option<u64>,
    /// Optional atomic JSON counters for local diagnostics (never credentials).
    #[arg(long)]
    metrics_file: Option<PathBuf>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ConfigFile {
    reapi: Policy,
    output_service: FileOptions,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileOptions {
    listen: Option<SocketAddr>,
    root: Option<PathBuf>,
    cas_endpoint: Option<String>,
    instance_name: Option<String>,
    api_key_env: Option<String>,
    storage: Option<Storage>,
    state_dir: Option<PathBuf>,
    cas_cache_dir: Option<PathBuf>,
    max_blob_bytes: Option<u64>,
    max_cache_bytes: Option<u64>,
    max_concurrent_fetches: Option<usize>,
    max_inflight_bytes: Option<u64>,
    max_oversized_inflight_bytes: Option<u64>,
    metrics_file: Option<PathBuf>,
}

struct ResolvedArgs {
    listen: SocketAddr,
    root: PathBuf,
    cas_endpoint: String,
    instance_name: String,
    api_key_env: String,
    max_blob_bytes: u64,
    storage: Storage,
    state_dir: Option<PathBuf>,
    cas_cache_dir: Option<PathBuf>,
    max_cache_bytes: u64,
    max_concurrent_fetches: usize,
    max_inflight_bytes: u64,
    max_oversized_inflight_bytes: u64,
    metrics_file: Option<PathBuf>,
    policy: Policy,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Resolve and validate configuration before credentials, network or mounts.
    let args = Args::parse().resolve()?;
    if args.storage == Storage::Eager {
        eprintln!("WARNING: --debug-eager enables debug-only eager output downloads");
    }
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
        policy: args.policy.clone(),
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
    fn resolve(self) -> anyhow::Result<ResolvedArgs> {
        let mut file = if let Some(path) = &self.config {
            let input = std::fs::File::open(path)
                .map_err(|_| anyhow::anyhow!("cannot open output service configuration file"))?;
            let mut bytes = Vec::new();
            input
                .take(65_537)
                .read_to_end(&mut bytes)
                .map_err(|_| anyhow::anyhow!("cannot read output service configuration file"))?;
            anyhow::ensure!(
                bytes.len() <= 65_536,
                "output service configuration exceeds 64 KiB"
            );
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| anyhow::anyhow!("output service configuration is not UTF-8"))?;
            let mut file: ConfigFile = toml::from_str(text).map_err(|_| {
                anyhow::anyhow!(
                    "invalid output service TOML configuration; check schema and value types"
                )
            })?;
            let base = path.parent().unwrap_or_else(|| std::path::Path::new("."));
            for path in [
                &mut file.output_service.root,
                &mut file.output_service.state_dir,
                &mut file.output_service.cas_cache_dir,
                &mut file.output_service.metrics_file,
            ]
            .into_iter()
            .flatten()
            {
                if path.is_relative() {
                    *path = base.join(&*path);
                }
            }
            file
        } else {
            ConfigFile::default()
        };
        if let Some(seconds) = self.rpc_timeout_secs {
            file.reapi.rpc_timeout_ms = seconds
                .checked_mul(1_000)
                .ok_or_else(|| anyhow::anyhow!("--rpc-timeout-secs is too large"))?;
        }
        file.reapi.validate()?;
        let options = file.output_service;
        let storage = if self.debug_eager {
            anyhow::ensure!(
                self.storage.is_none(),
                "--debug-eager conflicts with --storage"
            );
            Storage::Eager
        } else {
            self.storage
                .or(options.storage)
                .unwrap_or(Storage::Lazy)
                .resolve()?
        };
        let args = ResolvedArgs {
            listen: self
                .listen
                .or(options.listen)
                .unwrap_or_else(|| "127.0.0.1:8377".parse().unwrap()),
            root: self.root.or(options.root).ok_or_else(|| {
                anyhow::anyhow!("root is required via --root or [output_service].root")
            })?,
            cas_endpoint: self.cas_endpoint.or(options.cas_endpoint).ok_or_else(|| {
                anyhow::anyhow!(
                    "CAS endpoint is required via --cas-endpoint or [output_service].cas_endpoint"
                )
            })?,
            instance_name: self
                .instance_name
                .or(options.instance_name)
                .unwrap_or_default(),
            api_key_env: self
                .api_key_env
                .or(options.api_key_env)
                .unwrap_or_else(|| "BUILDBUDDY_API_KEY".into()),
            max_blob_bytes: self
                .max_blob_bytes
                .or(options.max_blob_bytes)
                .unwrap_or(17_179_869_184),
            storage,
            state_dir: self.state_dir.or(options.state_dir),
            cas_cache_dir: self.cas_cache_dir.or(options.cas_cache_dir),
            max_cache_bytes: self
                .max_cache_bytes
                .or(options.max_cache_bytes)
                .unwrap_or(4_294_967_296),
            max_concurrent_fetches: self
                .max_concurrent_fetches
                .or(options.max_concurrent_fetches)
                .unwrap_or(8),
            max_inflight_bytes: self
                .max_inflight_bytes
                .or(options.max_inflight_bytes)
                .unwrap_or(268_435_456),
            max_oversized_inflight_bytes: self
                .max_oversized_inflight_bytes
                .or(options.max_oversized_inflight_bytes)
                .unwrap_or(1_073_741_824),
            metrics_file: self.metrics_file.or(options.metrics_file),
            policy: file.reapi,
        };
        anyhow::ensure!(
            args.listen.ip().is_loopback(),
            "unauthenticated service must bind loopback"
        );
        anyhow::ensure!(
            args.storage == Storage::Eager || args.state_dir.is_some(),
            "state directory is required for lazy storage"
        );
        anyhow::ensure!(
            !args.cas_endpoint.is_empty(),
            "CAS endpoint must not be empty"
        );
        anyhow::ensure!(
            !args.api_key_env.is_empty() && !args.api_key_env.contains(['=', '\0']),
            "credential environment variable name is invalid"
        );
        anyhow::ensure!(
            args.max_blob_bytes > 0
                && args.max_cache_bytes > 0
                && args.max_inflight_bytes > 0
                && args.max_concurrent_fetches > 0,
            "output service resource limits must be nonzero"
        );
        Ok(args)
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
    args: &ResolvedArgs,
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
            max_oversized_inflight_bytes: args.max_oversized_inflight_bytes,
            operation_timeout: service.policy().operation_timeout(),
            fetch_timeout: service.policy().operation_timeout(),
        },
    )?;
    eprintln!(
        "lazy output limits (bytes): metadata={} readable={} cache={} normal_inflight={} oversized_inflight={}; concurrent_fetches={} (oversized=0 disables the extra lane)",
        args.max_blob_bytes,
        args.max_blob_bytes.min(cache.config().readable_limit()),
        args.max_cache_bytes,
        args.max_inflight_bytes,
        args.max_oversized_inflight_bytes,
        args.max_concurrent_fetches,
    );
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
                "--state-dir",
                "state",
                "--cas-endpoint",
                "grpc://127.0.0.1:1985",
            ]
            .into_iter()
            .chain(extra.iter().copied()),
        )
        .unwrap()
    }

    #[test]
    fn explicit_policy_file_and_cli_override_preserve_other_budgets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("policy.toml");
        std::fs::write(
            &path,
            "[reapi]\nrpc_timeout_ms = 23000\noperation_timeout_ms = 91000\n",
        )
        .unwrap();
        let path = path.to_str().unwrap();
        let from_file = args(&["--config", path]).resolve().unwrap();
        assert_eq!(from_file.policy.rpc_timeout(), Duration::from_secs(23));
        let overridden = args(&["--config", path, "--rpc-timeout-secs", "7"])
            .resolve()
            .unwrap();
        assert_eq!(overridden.policy.rpc_timeout(), Duration::from_secs(7));
        assert_eq!(
            overridden.policy.operation_timeout(),
            Duration::from_secs(91)
        );
        assert_eq!(args(&[]).resolve().unwrap().policy, Policy::default());
    }

    #[test]
    fn complete_config_resolves_paths_and_cli_takes_precedence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.toml");
        std::fs::write(
            &path,
            r#"
[output_service]
root = "mount"
state_dir = "state"
cas_cache_dir = "cas"
metrics_file = "metrics.json"
cas_endpoint = "grpcs://cache.example.com"
instance_name = "builds"
listen = "127.0.0.1:8001"
api_key_env = "EXAMPLE_CAS_KEY"
storage = "lazy"
max_blob_bytes = 12345
max_cache_bytes = 23456
max_inflight_bytes = 34567
max_oversized_inflight_bytes = 45678
max_concurrent_fetches = 3
[reapi]
rpc_timeout_ms = 1234
"#,
        )
        .unwrap();
        let parse = |extra: &[&str]| {
            Args::try_parse_from(
                ["git-output-service", "--config", path.to_str().unwrap()]
                    .into_iter()
                    .chain(extra.iter().copied()),
            )
            .unwrap()
            .resolve()
            .unwrap()
        };
        let configured = parse(&[]);
        assert_eq!(configured.root, dir.path().join("mount"));
        assert_eq!(configured.state_dir, Some(dir.path().join("state")));
        assert_eq!(configured.cas_cache_dir, Some(dir.path().join("cas")));
        assert_eq!(
            configured.metrics_file,
            Some(dir.path().join("metrics.json"))
        );
        assert_eq!(configured.cas_endpoint, "grpcs://cache.example.com");
        assert_eq!(configured.instance_name, "builds");
        assert_eq!(configured.listen.port(), 8001);
        assert_eq!(configured.api_key_env, "EXAMPLE_CAS_KEY");
        assert_eq!(configured.max_blob_bytes, 12345);
        assert_eq!(configured.max_cache_bytes, 23456);
        assert_eq!(configured.max_inflight_bytes, 34567);
        assert_eq!(configured.max_oversized_inflight_bytes, 45678);
        assert_eq!(configured.max_concurrent_fetches, 3);
        assert_eq!(configured.policy.rpc_timeout_ms, 1234);
        let overridden = parse(&[
            "--root",
            "cli-mount",
            "--state-dir",
            "cli-state",
            "--cas-cache-dir",
            "cli-cas",
            "--metrics-file",
            "cli.json",
            "--cas-endpoint",
            "grpc://127.0.0.1:9999",
            "--instance-name",
            "",
            "--listen",
            "127.0.0.1:8002",
            "--api-key-env",
            "CLI_KEY",
            "--max-blob-bytes",
            "100",
            "--max-cache-bytes",
            "200",
            "--max-inflight-bytes",
            "300",
            "--max-concurrent-fetches",
            "2",
            "--max-oversized-inflight-bytes",
            "0",
        ]);
        assert_eq!(overridden.root, PathBuf::from("cli-mount"));
        assert_eq!(overridden.state_dir, Some(PathBuf::from("cli-state")));
        assert_eq!(overridden.cas_cache_dir, Some(PathBuf::from("cli-cas")));
        assert_eq!(overridden.metrics_file, Some(PathBuf::from("cli.json")));
        assert_eq!(overridden.cas_endpoint, "grpc://127.0.0.1:9999");
        assert_eq!(overridden.instance_name, "");
        assert_eq!(overridden.listen.port(), 8002);
        assert_eq!(overridden.api_key_env, "CLI_KEY");
        assert_eq!(overridden.max_blob_bytes, 100);
        assert_eq!(overridden.max_cache_bytes, 200);
        assert_eq!(overridden.max_inflight_bytes, 300);
        assert_eq!(overridden.max_oversized_inflight_bytes, 0);
        assert_eq!(overridden.max_concurrent_fetches, 2);
        assert_eq!(overridden.policy.rpc_timeout_ms, 1234);
    }

    #[test]
    fn native_lazy_default_and_debug_only_eager() {
        let defaults = args(&[]).resolve().unwrap();
        assert_eq!(defaults.storage, Storage::Lazy.resolve().unwrap());
        assert_eq!(defaults.listen, "127.0.0.1:8377".parse().unwrap());
        assert_eq!(defaults.max_cache_bytes, 4_294_967_296);
        assert_eq!(defaults.max_inflight_bytes, 268_435_456);
        assert_eq!(defaults.max_oversized_inflight_bytes, 1_073_741_824);
        assert_eq!(defaults.max_concurrent_fetches, 8);
        assert_eq!(defaults.max_blob_bytes, 17_179_869_184);
        assert_eq!(defaults.api_key_env, "BUILDBUDDY_API_KEY");
        assert_eq!(
            args(&["--debug-eager"]).resolve().unwrap().storage,
            Storage::Eager
        );
        assert!(Args::try_parse_from(["bos", "--storage", "eager"]).is_err());
        assert!(Args::try_parse_from(["bos", "--storage", "lazy", "--debug-eager"]).is_err());
        assert!(
            Args::try_parse_from(["bos", "--root", "out", "--cas-endpoint", "grpc://127.0.0.1"])
                .unwrap()
                .resolve()
                .is_err()
        );
        assert!(
            Args::try_parse_from([
                "bos",
                "--root",
                "out",
                "--cas-endpoint",
                "grpc://127.0.0.1",
                "--debug-eager"
            ])
            .unwrap()
            .resolve()
            .is_ok()
        );
    }

    #[test]
    fn invalid_or_sensitive_config_is_rejected_without_echoing_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        for input in [
            "[output_service]\napi_key = 'SECRET_VALUE'",
            "[output_service]\nstorage = 'eager' # SECRET_VALUE",
            "[output_service]\ndebug_eager = true # SECRET_VALUE",
            "[unknown]\nkey = 'SECRET_VALUE'",
            "[reapi]\nrpc_timeout_ms = 'SECRET_VALUE'",
            "[output_service]\nroot = 'SECRET_VALUE",
        ] {
            std::fs::write(&path, input).unwrap();
            let error = args(&["--config", path.to_str().unwrap()])
                .resolve()
                .err()
                .unwrap();
            assert!(!format!("{error:#}").contains("SECRET_VALUE"));
        }
        std::fs::write(&path, vec![b' '; 65_537]).unwrap();
        assert!(
            args(&["--config", path.to_str().unwrap()])
                .resolve()
                .is_err()
        );
        std::fs::write(&path, [0xff]).unwrap();
        assert!(
            args(&["--config", path.to_str().unwrap()])
                .resolve()
                .is_err()
        );
        assert!(args(&["--rpc-timeout-secs", "0"]).resolve().is_err());
        assert!(
            args(&["--rpc-timeout-secs", "18446744073709551615"])
                .resolve()
                .is_err()
        );
        assert!(args(&["--max-concurrent-fetches", "0"]).resolve().is_err());
        assert!(args(&["--listen", "0.0.0.0:8000"]).resolve().is_err());
    }

    #[test]
    fn shipped_example_is_a_complete_valid_config() {
        let file: ConfigFile =
            toml::from_str(include_str!("../../../examples/bazel-output-service.toml")).unwrap();
        file.reapi.validate().unwrap();
        assert!(file.output_service.root.is_some());
        assert!(file.output_service.state_dir.is_some());
        assert!(file.output_service.cas_endpoint.is_some());
        assert_eq!(file.output_service.storage, Some(Storage::Lazy));
        assert_eq!(
            file.output_service.max_oversized_inflight_bytes,
            Some(1_073_741_824)
        );
    }
}
