use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use workspace_fuse::backend::{GitRepo, SizeFallback, SizeSource, SizeWarmOptions};
use workspace_fuse::filesystem::Snapshot;
use workspace_fuse::overlay::changes_since;
use workspace_fuse::prefetch::BazelPrefetchPolicy;

use workspace_fuse::cas_digest;

#[derive(Parser)]
#[command(
    version,
    about = "Mount a Git commit without checking out or eagerly fetching its blobs"
)]
struct Cli {
    /// Git executable. It must support cat-file remote-object-info for lazy metadata.
    #[arg(long, global = true, default_value = "git")]
    git: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Fallback {
    /// Fail metadata queries that cannot be answered without fetching content.
    Error,
    /// Explicitly permit fetching blobs to determine their sizes.
    Fetch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum SizeSourceArg {
    /// Use GitHub's tree API for github.com remotes; native Git elsewhere.
    Auto,
    /// Use the size cache and native Git's protocol-v2 object-info command.
    Git,
    /// Require GitHub's tree API for the configured github.com remote.
    Github,
}

#[derive(Args)]
struct RepoArgs {
    /// Bare SHA-1 Git repository (normally created by git-vfs clone).
    #[arg(long)]
    repo: PathBuf,
    #[arg(long, default_value = "HEAD")]
    revision: String,
    #[arg(long, default_value = "origin")]
    remote: String,
    #[arg(long, value_enum, default_value = "error")]
    size_fallback: Fallback,
    /// Provider for warm-sizes. Explicit providers batch metadata before mount;
    /// auto warms GitHub metadata but keeps other origins demand-driven.
    #[arg(long, value_enum, default_value = "auto")]
    size_source: SizeSourceArg,
    /// Read an API token from this environment variable (never a token value).
    #[arg(long, value_name = "NAME")]
    github_token_env: Option<String>,
}

impl RepoArgs {
    fn size_options(&self) -> SizeWarmOptions {
        SizeWarmOptions {
            source: match self.size_source {
                SizeSourceArg::Auto => SizeSource::Auto,
                SizeSourceArg::Git => SizeSource::Git,
                SizeSourceArg::Github => SizeSource::GitHub,
            },
            github_token_env: self.github_token_env.clone(),
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Create a bare, blobless partial clone. No working tree is created.
    Clone {
        url: String,
        repository: PathBuf,
        /// Branch or tag to clone; otherwise use the remote's default branch.
        #[arg(long)]
        branch: Option<String>,
        /// Limit history like git clone --depth. Omit to retain branch history.
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
        depth: Option<u32>,
    },
    /// Mount a pinned commit with a persistent writable overlay until Ctrl-C.
    Mount {
        #[command(flatten)]
        repository: RepoArgs,
        mountpoint: PathBuf,
        /// Keep the original immutable, read-only behavior (no Git working-tree view).
        #[arg(long)]
        read_only: bool,
        /// Persistent writable state directory. Defaults inside the bare repository.
        #[arg(long)]
        overlay: Option<PathBuf>,
        /// Trust this proxy's cached BLAKE3 mappings and expose user.blake3.
        #[arg(long, requires = "blake3_proxy_repo")]
        blake3_proxy_url: Option<String>,
        /// Repository alias on the BLAKE3 mapping proxy.
        #[arg(long, requires = "blake3_proxy_url")]
        blake3_proxy_repo: Option<String>,
        /// Batch-prefetch Bazel inputs in the background after mounting.
        #[arg(long)]
        prefetch_bazel: bool,
        /// Restrict background metadata/symlink selection to a relative subtree.
        #[arg(long, requires = "prefetch_bazel")]
        prefetch_path: Vec<PathBuf>,
        /// Include additional relative files/subtrees in the initial background batch.
        #[arg(long, requires = "prefetch_bazel")]
        prefetch_include: Vec<PathBuf>,
        /// Skip literal MODULE/rc discovery in background prefetch.
        #[arg(long, requires = "prefetch_bazel")]
        prefetch_no_discover: bool,
        /// Maximum logical bytes in the small-blob content cache (0 also disables the default large-blob spool).
        #[arg(long, default_value_t = 67108864)]
        content_cache_bytes: u64,
        /// Separate anonymous large-blob spool limit; defaults to 512 MiB when the content cache is enabled, otherwise 0. An explicit value enables this pool independently; 0 disables it.
        #[arg(long)]
        content_large_blob_spool_bytes: Option<u64>,
        /// Maximum bytes reserved by outstanding immutable read replies (at least 16 MiB).
        #[arg(long, default_value_t = 67108864, value_parser = clap::value_parser!(u64).range(16777216..))]
        content_reply_bytes: u64,
    },
    /// List the projected namespace without asking for blob sizes or contents.
    List {
        #[command(flatten)]
        repository: RepoArgs,
    },
    /// Read metadata for a relative path (may issue a remote size query).
    Stat {
        #[command(flatten)]
        repository: RepoArgs,
        path: PathBuf,
    },
    /// Print raw blob bytes, fetching the blob through Git if needed.
    Cat {
        #[command(flatten)]
        repository: RepoArgs,
        path: PathBuf,
    },
    /// Print a file's Git SHA-1 from its tree entry without fetching it.
    Digest {
        #[command(flatten)]
        repository: RepoArgs,
        path: PathBuf,
        /// Output the 20 raw bytes, matching user.gitsha1 on the mount.
        #[arg(long)]
        raw: bool,
    },
    /// Look up a base blob's BLAKE3 content digest through a trusted Git proxy.
    CasDigest {
        #[command(flatten)]
        repository: RepoArgs,
        /// Explicit HTTP(S) origin, without credentials, query, or path prefix.
        #[arg(long)]
        proxy_url: String,
        /// Repository alias configured on the proxy.
        #[arg(long)]
        proxy_repo: String,
        path: PathBuf,
    },
    /// Batch-cache trusted proxy BLAKE3 mappings and sizes without reading blobs.
    WarmBlake3 {
        #[command(flatten)]
        repository: RepoArgs,
        #[arg(long)]
        proxy_url: String,
        #[arg(long)]
        proxy_repo: String,
    },
    /// Prefill the persistent size cache for a commit without reading blobs.
    WarmSizes {
        #[command(flatten)]
        repository: RepoArgs,
    },
    /// Batch-fetch Bazel/Starlark metadata and symlink blobs in the base snapshot.
    PrefetchBazel {
        #[command(flatten)]
        repository: RepoArgs,
        /// Repository-relative file/subtree root (repeatable; default: whole repository).
        #[arg(long = "path")]
        paths: Vec<PathBuf>,
        /// Additional exact file/subtree contents, unioned into the initial fetch.
        #[arg(long = "include")]
        includes: Vec<PathBuf>,
        /// Skip best-effort local MODULE.bazel and rc input discovery.
        #[arg(long)]
        no_discover: bool,
    },
    /// Report the persistent local size-cache entry count.
    Cache {
        #[command(flatten)]
        repository: RepoArgs,
    },
    /// Git fsmonitor v2 hook. Output is binary and intended for Git, not a terminal.
    #[command(hide = true)]
    Fsmonitor {
        #[arg(long)]
        overlay: PathBuf,
        version: u8,
        token: String,
    },
}

fn open(args: &RepoArgs, git: &Path) -> Result<GitRepo> {
    let fallback = match args.size_fallback {
        Fallback::Error => SizeFallback::Error,
        Fallback::Fetch => SizeFallback::Fetch,
    };
    GitRepo::open(&args.repo, git, &args.remote, fallback)
}

fn print_json(value: &impl serde::Serialize) -> Result<()> {
    serde_json::to_writer_pretty(io::stdout().lock(), value)?;
    println!();
    Ok(())
}

fn clone_repository(
    git: &Path,
    url: &str,
    repository: &Path,
    branch: Option<&str>,
    depth: Option<u32>,
) -> Result<()> {
    if repository.exists() {
        bail!("clone destination already exists: {}", repository.display());
    }
    let mut command = Command::new(git);
    for name in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_INDEX_FILE",
        "GIT_PREFIX",
        "GIT_NAMESPACE",
        "GIT_SHALLOW_FILE",
    ] {
        command.env_remove(name);
    }
    command.arg("--no-replace-objects");
    command.args([
        "-c",
        "protocol.version=2",
        "clone",
        "--bare",
        "--filter=blob:none",
        "--single-branch",
        "--no-local",
    ]);
    if let Some(branch) = branch {
        command.arg("--branch").arg(branch);
    }
    if let Some(depth) = depth {
        command.arg("--depth").arg(depth.to_string());
    }
    let output = command
        .arg("--")
        .arg(url)
        .arg(repository)
        .output()
        .context("starting Git partial clone")?;
    if !output.status.success() {
        bail!(
            "Git partial clone failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }
    let repo = GitRepo::open(repository, git, "origin", SizeFallback::Error)?;
    let promisor = repo.git_output(&["config", "--get", "remote.origin.promisor"], false)?;
    if promisor != b"true\n" {
        bail!("clone is not configured as a promisor repository");
    }
    let snapshot = Snapshot::load(&repo, "HEAD")?;
    print_json(&serde_json::json!({
        "repository": repo.repository_path(),
        "commit": snapshot.commit,
        "bare": true,
        "filter": "blob:none",
        "requested_depth": depth,
        "note": "Server support determines whether clone actually omits blobs; inspect Git warnings above."
    }))
}

struct MountRuntime {
    blake3: bool,
    content: workspace_fuse::backend::ContentConfig,
}

fn mount(
    repo: GitRepo,
    snapshot: Snapshot,
    mountpoint: &Path,
    read_only: bool,
    overlay_path: Option<&Path>,
    prefetch: Option<BazelPrefetchPolicy>,
    runtime: MountRuntime,
) -> Result<()> {
    let (sender, receiver) = std::sync::mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;
    let mut host = workspace_fuse::host::SourceMount::start(
        repo,
        snapshot,
        workspace_fuse::host::MountConfig {
            mountpoint: mountpoint.to_owned(),
            read_only,
            overlay: overlay_path.map(Path::to_owned),
            prefetch,
            blake3: runtime.blake3,
            content: runtime.content,
            git_vfs_executable: std::env::current_exe()?,
        },
    )?;
    eprintln!("Mounted at {}. Ctrl-C unmounts.", mountpoint.display());
    loop {
        match receiver.recv_timeout(std::time::Duration::from_millis(200)) {
            Ok(()) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) if host.is_finished() => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(error) => return Err(error).context("waiting for shutdown signal"),
        }
    }
    host.shutdown()?;
    if runtime.blake3 {
        eprintln!(
            "BLAKE3 xattrs: {}",
            serde_json::to_string(&host.blake3_report())?
        );
    }
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Clone {
            url,
            repository,
            branch,
            depth,
        } => clone_repository(&cli.git, &url, &repository, branch.as_deref(), depth),
        Commands::Mount {
            repository,
            mountpoint,
            read_only,
            overlay,
            prefetch_bazel,
            blake3_proxy_url,
            blake3_proxy_repo,
            prefetch_path,
            prefetch_include,
            prefetch_no_discover,
            content_cache_bytes,
            content_large_blob_spool_bytes,
            content_reply_bytes,
        } => {
            let mut repo = open(&repository, &cli.git)?;
            let snapshot = Snapshot::load(&repo, &repository.revision)?;
            let blake3 = blake3_proxy_url.is_some();
            if let Some(origin) = blake3_proxy_url.as_deref() {
                let report = repo.warm_blake3_snapshot(
                    &snapshot,
                    origin,
                    blake3_proxy_repo.as_deref().expect("paired arguments"),
                )?;
                eprintln!("BLAKE3 metadata: {}", serde_json::to_string(&report)?);
            }
            if !blake3
                && (matches!(
                    repository.size_source,
                    SizeSourceArg::Github | SizeSourceArg::Git
                ) || (repository.size_source == SizeSourceArg::Auto
                    && repo.is_github_remote()?))
            {
                let report = repo.warm_snapshot_sizes(&snapshot, &repository.size_options())?;
                eprintln!("Size metadata: {}", serde_json::to_string(&report)?);
            }
            let policy = prefetch_bazel
                .then(|| {
                    BazelPrefetchPolicy::new(&prefetch_path)
                        .and_then(|policy| policy.with_includes(&prefetch_include))
                        .map(|policy| policy.with_discovery(!prefetch_no_discover))
                })
                .transpose()?;
            mount(
                repo,
                snapshot,
                &mountpoint,
                read_only,
                overlay.as_deref(),
                policy,
                MountRuntime {
                    blake3,
                    content: workspace_fuse::backend::ContentConfig {
                        cache_bytes: content_cache_bytes,
                        large_blob_spool_bytes: content_large_blob_spool_bytes,
                        reply_bytes: content_reply_bytes,
                        ..workspace_fuse::backend::ContentConfig::default()
                    },
                },
            )
        }
        Commands::List { repository } => {
            let repo = open(&repository, &cli.git)?;
            let snapshot = Snapshot::load(&repo, &repository.revision)?;
            print_json(
                &serde_json::json!({ "commit": snapshot.commit, "entries": snapshot.entries() }),
            )
        }
        Commands::Stat { repository, path } => {
            let mut repo = open(&repository, &cli.git)?;
            let snapshot = Snapshot::load(&repo, &repository.revision)?;
            print_json(&snapshot.stat_path(&mut repo, &path)?)
        }
        Commands::Cat { repository, path } => {
            let mut repo = open(&repository, &cli.git)?;
            let snapshot = Snapshot::load(&repo, &repository.revision)?;
            io::stdout()
                .lock()
                .write_all(&snapshot.read_path(&mut repo, &path)?)?;
            Ok(())
        }
        Commands::Digest {
            repository,
            path,
            raw,
        } => {
            let repo = open(&repository, &cli.git)?;
            let snapshot = Snapshot::load(&repo, &repository.revision)?;
            let digest = snapshot.digest_path(&path)?;
            if raw {
                io::stdout().lock().write_all(&digest)?;
            } else {
                println!("{}", hex::encode(digest));
            }
            Ok(())
        }
        Commands::CasDigest {
            repository,
            proxy_url,
            proxy_repo,
            path,
        } => {
            let repo = open(&repository, &cli.git)?;
            let snapshot = Snapshot::load(&repo, &repository.revision)?;
            let oid = hex::encode(snapshot.digest_path(&path)?);
            print_json(&cas_digest::lookup(&proxy_url, &proxy_repo, &oid)?)
        }
        Commands::WarmBlake3 {
            repository,
            proxy_url,
            proxy_repo,
        } => {
            let mut repo = open(&repository, &cli.git)?;
            let snapshot = Snapshot::load(&repo, &repository.revision)?;
            print_json(&repo.warm_blake3_snapshot(&snapshot, &proxy_url, &proxy_repo)?)
        }
        Commands::WarmSizes { repository } => {
            let mut repo = open(&repository, &cli.git)?;
            let snapshot = Snapshot::load(&repo, &repository.revision)?;
            let report = repo.warm_snapshot_sizes(&snapshot, &repository.size_options())?;
            let mut stats = serde_json::to_value(repo.cache_stats()?)?;
            stats["warm"] = serde_json::to_value(report)?;
            print_json(&stats)
        }
        Commands::PrefetchBazel {
            repository,
            paths,
            includes,
            no_discover,
        } => {
            let policy = workspace_fuse::prefetch::BazelPrefetchPolicy::new(&paths)?
                .with_includes(&includes)?
                .with_discovery(!no_discover);
            let mut repo = open(&repository, &cli.git)?;
            let snapshot = Snapshot::load(&repo, &repository.revision)?;
            print_json(&workspace_fuse::prefetch::prefetch_bazel_with_policy(
                &mut repo, &snapshot, &policy,
            )?)
        }
        Commands::Cache { repository } => print_json(&open(&repository, &cli.git)?.cache_stats()?),
        Commands::Fsmonitor {
            overlay,
            version,
            token,
        } => {
            if version != 2 {
                bail!("git-vfs fsmonitor requires hook protocol version 2");
            }
            let (token, paths) = changes_since(&overlay, &token)?;
            let mut output = io::stdout().lock();
            output.write_all(token.as_bytes())?;
            output.write_all(&[0])?;
            for path in paths {
                output.write_all(&path)?;
                output.write_all(&[0])?;
            }
            Ok(())
        }
    }
}
