use clap::{Parser, Subcommand};
use std::{
    io::{Read, Write},
    path::PathBuf,
};
use tinytree_check_runner::{Config, MAX_REQUEST_BYTES, Request};

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    config: PathBuf,
    #[command(subcommand)]
    command: Operation,
}
#[derive(Subcommand)]
enum Operation {
    Run,
    Recover,
    ProfileVersion {
        #[arg(long)]
        profile: String,
    },
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let result = async {
        let config = Config::load(&cli.config)?;
        #[cfg(target_os = "linux")]
        let executable = PathBuf::from("/proc/self/exe");
        #[cfg(not(target_os = "linux"))]
        let executable = std::env::current_exe()?;
        tinytree_check_runner::Binary {
            path: executable,
            sha256: config.runner_sha256.clone(),
        }
        .verify()?;
        if let Operation::ProfileVersion { profile } = cli.command {
            println!("{}", config.profile_version(&profile)?);
            return Ok::<_, anyhow::Error>(());
        }
        let bytes = read_request()?;
        let request = Request::decode(&bytes)?;
        let (sender, receiver) = tokio::sync::watch::channel(false);
        #[cfg(unix)]
        let _signal = tokio::spawn(async move {
            let mut interrupt =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
            let _ = sender.send(true);
            Ok::<_, std::io::Error>(())
        });
        #[cfg(not(unix))]
        let _sender = sender;
        let receipt = match cli.command {
            Operation::Run => tinytree_check_runner::run(&config, request, receiver).await?,
            Operation::Recover => tinytree_check_runner::recover(&config, request).await?,
            Operation::ProfileVersion { .. } => unreachable!(),
        };
        let mut output = std::io::stdout().lock();
        output.write_all(&receipt.encode()?)?;
        output.write_all(b"\n")?;
        Ok(())
    }
    .await;
    if result.is_err() {
        // Errors can contain operator paths or child diagnostics. The service
        // receives only a stable transport failure, never those raw details.
        eprintln!("check runner rejected the request or requires operator recovery");
        std::process::ExitCode::FAILURE
    } else {
        std::process::ExitCode::SUCCESS
    }
}

#[cfg(target_os = "linux")]
fn read_request() -> anyhow::Result<Vec<u8>> {
    use std::{
        os::fd::{AsRawFd, FromRawFd},
        time::{Duration, Instant},
    };
    let descriptor = unsafe { libc::dup(libc::STDIN_FILENO) };
    anyhow::ensure!(descriptor >= 0, "cannot open request input");
    let mut input = unsafe { std::fs::File::from_raw_fd(descriptor) };
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    anyhow::ensure!(
        flags >= 0
            && unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0,
        "cannot bound request input"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut bytes = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        anyhow::ensure!(!remaining.is_zero(), "request input deadline exceeded");
        let mut poll = libc::pollfd {
            fd: input.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut poll, 1, remaining.as_millis().min(10000) as i32) };
        if result < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            anyhow::bail!("request input poll failed")
        }
        if result == 0 {
            continue;
        }
        let mut chunk = [0; 4096];
        match input.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                bytes.extend_from_slice(&chunk[..n]);
                anyhow::ensure!(
                    bytes.len() <= MAX_REQUEST_BYTES,
                    "request input exceeds budget"
                );
            }
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(bytes)
}
#[cfg(not(target_os = "linux"))]
fn read_request() -> anyhow::Result<Vec<u8>> {
    anyhow::bail!("native checker execution requires Linux")
}
