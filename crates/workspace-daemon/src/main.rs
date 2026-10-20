use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use workspace_daemon::config::{Config, EXAMPLE};
#[cfg(unix)]
use workspace_daemon::control::{Client, Request};
#[derive(Parser)]
#[command(
    version,
    about = "Private coding workspaces with one shared host cache and mount daemon"
)]
struct Cli {
    #[arg(long,global=true,default_value_os_t=default_config())]
    config: PathBuf,
    #[command(subcommand)]
    command: Operation,
}
fn default_config() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config/workspace/config.toml")
}
#[derive(Subcommand)]
enum Operation {
    /// Write a starter TOML without overwriting an existing file.
    Init,
    #[command(hide = true)]
    Fsmonitor {
        #[arg(long)]
        overlay: PathBuf,
        version: u32,
        token: String,
    },
    /// Run the host service in the foreground (normally started automatically).
    Serve,
    /// Safely detach verified disconnected mounts after a daemon crash.
    Recover {
        session: Option<PathBuf>,
    },
    #[command(hide = true)]
    MountProbe {
        path: PathBuf,
    },
    /// Inspect shared cache and mount ownership, or one private session.
    Status {
        session: Option<PathBuf>,
    },
    /// Create a private writable source workspace; pin a full commit SHA.
    Create {
        session: PathBuf,
        #[arg(long)]
        proxy_url: Option<String>,
        #[arg(long = "repo")]
        proxy_repo: String,
        #[arg(long, default_value = "master")]
        reference: String,
        #[arg(long)]
        revision: String,
        #[arg(long)]
        bazelrc: Option<PathBuf>,
    },
    Build {
        session: PathBuf,
        #[arg(last = true, required = true)]
        args: Vec<String>,
    },
    Test {
        session: PathBuf,
        #[arg(last = true, required = true)]
        args: Vec<String>,
    },
    Save {
        session: PathBuf,
        #[arg(last = true)]
        args: Vec<String>,
    },
    Stop {
        session: PathBuf,
    },
    Resume {
        session: PathBuf,
    },
    /// Stop the daemon only when all source sessions have been stopped.
    Shutdown,
}
#[cfg(unix)]
fn client(config: &Config) -> Client {
    Client {
        socket: config.socket(),
    }
}
#[cfg(unix)]
fn ensure_daemon(path: &Path, config: &Config) -> Result<()> {
    if client(config).call(Request::Status).is_ok() {
        return Ok(());
    }
    // The child holds a state-directory lock; concurrent startups are harmless.
    use std::os::unix::fs::OpenOptionsExt;
    workspace_daemon::daemon::private_dir(&config.state_dir)?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(config.state_dir.join("daemon.log"))?;
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("--config")
        .arg(path)
        .arg("serve")
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    use std::os::unix::process::CommandExt;
    command.process_group(0);
    let mut child = command.spawn().context("starting workspace daemon")?;
    let start = Instant::now();
    loop {
        if client(config).call(Request::Status).is_ok() {
            return Ok(());
        }
        if child.try_wait()?.is_some() {
            anyhow::bail!("daemon startup failed; inspect private daemon.log")
        }
        if start.elapsed() > Duration::from_secs(30) {
            anyhow::bail!("daemon startup timed out; inspect private daemon.log")
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
#[cfg(unix)]
async fn run(cli: Cli) -> Result<serde_json::Value> {
    if matches!(cli.command, Operation::Init) {
        let parent = cli.config.parent().context("config parent")?;
        std::fs::create_dir_all(parent)?;
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&cli.config)
            .context("config already exists or cannot be created")?;
        file.write_all(EXAMPLE.as_bytes())?;
        return Ok(
            serde_json::json!({"config":cli.config,"next":"Set cache.upstream and your credential environment variable, then run workspace create."}),
        );
    }
    let config = Config::load(&cli.config)?;
    if let Operation::Recover { session } = &cli.command {
        let session = session
            .as_ref()
            .map(|path| path.canonicalize().context("recovery session must exist"))
            .transpose()?;
        let controller = client(&config);
        if std::os::unix::net::UnixStream::connect(config.socket()).is_ok() {
            return controller.call(Request::Recover { session });
        }
        return workspace_daemon::daemon::recover_offline(config, session.as_deref());
    }
    if matches!(cli.command, Operation::Serve) {
        workspace_daemon::daemon::serve(config).await?;
        return Ok(serde_json::json!({"stopped":true}));
    }
    if !matches!(cli.command, Operation::Status { .. } | Operation::Shutdown) {
        ensure_daemon(&cli.config, &config)?;
    }
    let controller = client(&config);
    // Session orchestration is synchronous and must not block the async runtime
    // used by an in-process service. The CLI does not host any mount itself.
    let report = match cli.command {
        Operation::Status { session: None } => return controller.call(Request::Status),
        Operation::Shutdown => return controller.call(Request::Shutdown),
        Operation::Status {
            session: Some(session),
        } => agent_session::status_with_controller(&session, &controller)?,
        Operation::Create {
            session,
            proxy_url,
            proxy_repo,
            reference,
            revision,
            bazelrc,
        } => {
            let proxy_url = match proxy_url {
                Some(url) => url,
                None => controller.call(Request::Endpoints)?["git_proxy_url"]
                    .as_str()
                    .context("configure git_proxy.repositories or provide --proxy-url")?
                    .to_owned(),
            };
            let bazelrc = bazelrc
                .or(config.session.bazelrc)
                .context("provide --bazelrc or session.bazelrc")?;
            agent_session::create_with_controller(
                &session,
                agent_session::Settings {
                    proxy_url,
                    proxy_repo,
                    reference,
                    revision,
                    bazelrc,
                    git_vfs: config.session.git_vfs,
                    git: config.session.git,
                    bazel: config.session.bazel,
                    mount_timeout_seconds: 60,
                    repo_contents_cache: config.session.repo_contents_cache,
                    remote_repo_contents_cache: config.session.remote_repo_contents_cache,
                },
                &controller,
            )?
        }
        Operation::Build { session, args } => {
            agent_session::build_with_controller(&session, &args, &controller)?
        }
        Operation::Test { session, args } => {
            agent_session::test_with_controller(&session, &args, &controller)?
        }
        Operation::Save { session, args } => {
            agent_session::save_with_controller(&session, &args, &controller)?
        }
        Operation::Stop { session } => agent_session::stop_with_controller(&session, &controller)?,
        Operation::Resume { session } => {
            agent_session::resume_with_controller(&session, &controller)?
        }
        Operation::Init
        | Operation::Serve
        | Operation::Fsmonitor { .. }
        | Operation::Recover { .. }
        | Operation::MountProbe { .. } => unreachable!(),
    };
    Ok(serde_json::to_value(report)?)
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    #[cfg(target_os = "linux")]
    if let Operation::MountProbe { path } = &cli.command {
        return std::process::ExitCode::from(
            match std::fs::symlink_metadata(path.join(format!(
                ".workspace-recovery-probe-{}",
                uuid::Uuid::new_v4().simple()
            ))) {
                Err(error) if error.raw_os_error() == Some(libc::ENOTCONN) => 72,
                _ => 73,
            },
        );
    }
    #[cfg(target_os = "linux")]
    if let Operation::Fsmonitor {
        overlay,
        version,
        token,
    } = &cli.command
    {
        let result = (|| -> Result<()> {
            anyhow::ensure!(*version == 2, "fsmonitor requires hook protocol version 2");
            let (token, paths) = workspace_fuse::overlay::changes_since(overlay, token)?;
            use std::io::Write;
            let mut out = std::io::stdout().lock();
            out.write_all(token.as_bytes())?;
            out.write_all(&[0])?;
            for path in paths {
                out.write_all(&path)?;
                out.write_all(&[0])?;
            }
            Ok(())
        })();
        return match result {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("fsmonitor: {error}");
                std::process::ExitCode::FAILURE
            }
        };
    }

    #[cfg(unix)]
    let result = run(cli).await;
    #[cfg(not(unix))]
    let result: Result<serde_json::Value> = {
        let _ = cli;
        Err(anyhow::anyhow!(
            "private daemon control is currently supported on Unix only"
        ))
    };
    match result {
        Ok(report) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({"ok":true,"report":report}))
                    .unwrap()
            );
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::json!({"ok":false,"error":error.to_string()})
            );
            std::process::ExitCode::FAILURE
        }
    }
}
