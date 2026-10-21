#![cfg(unix)]
use anyhow::Result;
use reapi::reapi::{
    GetCapabilitiesRequest, ServerCapabilities,
    capabilities_server::{Capabilities, CapabilitiesServer},
};
use std::{path::PathBuf, time::Duration};
use tonic::{Request, Response, Status};
use workspace_daemon::{
    config::Config,
    control::{Client, Request as ControlRequest},
};
#[derive(Default)]
struct Backend;
#[tonic::async_trait]
impl Capabilities for Backend {
    async fn get_capabilities(
        &self,
        _: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        Ok(Response::new(ServerCapabilities::default()))
    }
}
fn config(dir: PathBuf, port: u16, upstream: u16) -> Config {
    let mut config = Config::default();
    config.state_dir = dir;
    config.cache.listen = format!("127.0.0.1:{port}").parse().unwrap();
    config.cache.upstream = format!("grpc://127.0.0.1:{upstream}");
    config.bos.enabled = false;
    config
}
async fn call(socket: PathBuf, request: ControlRequest) -> Result<serde_json::Value> {
    tokio::task::spawn_blocking(move || Client { socket }.call(request)).await?
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_control_retains_registry_and_excludes_second_owner() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let upstream_port = upstream.local_addr()?.port();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(CapabilitiesServer::new(Backend))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(upstream)),
    );
    let available = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = available.local_addr()?.port();
    drop(available);
    let mut config = config(dir.path().join("host"), port, upstream_port);
    let git_port = std::net::TcpListener::bind("127.0.0.1:0")?;
    config.git_proxy.listen = git_port.local_addr()?;
    drop(git_port);
    config
        .git_proxy
        .repositories
        .insert("fixture".into(), "https://example.invalid/repo.git".into());
    let socket = config.socket();
    let task = tokio::spawn(workspace_daemon::daemon::serve(config.clone()));
    for _ in 0..100 {
        if call(socket.clone(), ControlRequest::Status).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let status = call(socket.clone(), ControlRequest::Status).await?;
    assert_eq!(status["cache"]["resident_bytes"], 0);
    assert!(status["bos_endpoint"].is_null());
    assert_eq!(status["bes_endpoint"], config.bes_endpoint());
    assert!(status["bes"].is_object());
    use agent_session::MountController;
    let cli = Client {
        socket: socket.clone(),
    };
    let args = tokio::task::spawn_blocking(move || cli.bazel_args()).await??;
    assert!(args.contains(&format!("--bes_backend={}", config.bes_endpoint())));
    assert_eq!(
        call(socket.clone(), ControlRequest::RetryBes).await?["streams_retried"],
        0
    );
    // Native Git's plain endpoint is never public; metadata clients may keep
    // using the capability prefix without placing it in any subprocess argv.
    let token = std::fs::read_to_string(config.state_dir.join("cache.token"))?;
    async fn http(
        addr: std::net::SocketAddr,
        path: &str,
        header: Option<String>,
    ) -> Result<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        stream
            .write_all(
                format!(
                    "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{}\r\n",
                    header
                        .map(|h| format!("Authorization: {h}\r\n"))
                        .unwrap_or_default()
                )
                .as_bytes(),
            )
            .await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        Ok(response)
    }
    assert!(
        http(config.git_proxy.listen, "/metrics", None)
            .await?
            .starts_with("HTTP/1.1 401")
    );
    assert!(
        http(
            config.git_proxy.listen,
            "/metrics",
            Some("Bearer wrong".into())
        )
        .await?
        .starts_with("HTTP/1.1 401")
    );
    assert!(
        http(
            config.git_proxy.listen,
            "/metrics",
            Some(format!("Bearer {token}"))
        )
        .await?
        .starts_with("HTTP/1.1 200")
    );
    assert!(
        http(config.git_proxy.listen, &format!("/{token}/metrics"), None)
            .await?
            .starts_with("HTTP/1.1 200")
    );
    assert!(!status.to_string().contains(&std::fs::read_to_string(
        config.state_dir.join("cache.token")
    )?));
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&socket)?.permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(config.state_dir.join("bes.sock"))?
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(
        workspace_daemon::daemon::serve(config.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("another workspace daemon")
    );
    let bogus = dir.path().join("not-owned");
    assert_eq!(
        call(
            socket.clone(),
            ControlRequest::Mounted {
                session: bogus.clone()
            }
        )
        .await?,
        false
    );
    // Unmount of an unowned path is harmless and never invokes a native unmount.
    assert_eq!(
        call(socket.clone(), ControlRequest::Unmount { session: bogus }).await?,
        true
    );
    call(socket, ControlRequest::Shutdown).await?;
    tokio::time::timeout(Duration::from_secs(5), task).await???;
    assert!(config.state_dir.join("mounts.json").is_file());
    assert!(!config.socket().exists());
    assert!(!config.state_dir.join("bes.sock").exists());
    server.abort();
    Ok(())
}
#[tokio::test]
async fn public_state_directory_is_rejected_before_binding() -> Result<()> {
    let dir = tempfile::tempdir()?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))?;
    let config = config(dir.path().to_path_buf(), 8378, 1);
    let error = workspace_daemon::daemon::serve(config).await.unwrap_err();
    assert!(error.to_string().contains("mode 0700"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_upstream_does_not_prevent_durable_bes_admission_startup() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let unavailable = std::net::TcpListener::bind("127.0.0.1:0")?;
    let upstream_port = unavailable.local_addr()?.port();
    drop(unavailable);
    let config = config(dir.path().join("host"), 8378, upstream_port);
    let socket = config.socket();
    let task = tokio::spawn(workspace_daemon::daemon::serve(config.clone()));
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(status) = call(socket.clone(), ControlRequest::Status).await {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    assert_eq!(status["bes_endpoint"], config.bes_endpoint());
    call(socket, ControlRequest::Shutdown).await?;
    tokio::time::timeout(Duration::from_secs(5), task).await???;
    Ok(())
}

// Requires a native unprivileged FUSE mount, not a hermetic RBE sandbox.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "native FUSE acceptance; run explicitly on a host with /dev/fuse"]
async fn busy_output_mount_refuses_shutdown_without_losing_service() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let upstream_port = upstream.local_addr()?.port();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(CapabilitiesServer::new(Backend))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(upstream)),
    );
    let mut config = config(dir.path().join("host"), 8378, upstream_port);
    config.bos.enabled = true;
    let socket = config.socket();
    let task = tokio::spawn(workspace_daemon::daemon::serve(config.clone()));
    for _ in 0..100 {
        if call(socket.clone(), ControlRequest::Status).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    call(socket.clone(), ControlRequest::Status).await?;
    let handle = std::fs::File::open(config.state_dir.join("outputs"))?;
    let error = call(socket.clone(), ControlRequest::Shutdown)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("daemon remains running"));
    assert!(call(socket.clone(), ControlRequest::Status).await?["bos_endpoint"].is_string());
    assert!(std::fs::read_dir(config.state_dir.join("outputs")).is_ok());
    drop(handle);
    call(socket, ControlRequest::Shutdown).await?;
    tokio::time::timeout(Duration::from_secs(5), task).await???;
    server.abort();
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "native subprocess crash/FUSE acceptance; set WORKSPACE_BIN to the built workspace binary"]
async fn crashed_daemon_recovers_source_and_outputs_without_losing_staged_edits() -> Result<()> {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let executable = PathBuf::from(
        std::env::var_os("WORKSPACE_BIN")
            .expect("set WORKSPACE_BIN to the built workspace executable"),
    );
    let dir = tempfile::tempdir()?;
    let origin = dir.path().join("origin");
    std::fs::create_dir(&origin)?;
    let git = |args: &[&str]| -> Result<Vec<u8>> {
        let output = Command::new("git")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .arg("-C")
            .arg(&origin)
            .args(args)
            .output()?;
        anyhow::ensure!(
            output.status.success(),
            "fixture git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(output.stdout)
    };
    git(&["init", "-b", "master"])?;
    std::fs::write(origin.join("tracked.txt"), "base\n")?;
    git(&["add", "tracked.txt"])?;
    git(&[
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "-m",
        "fixture",
    ])?;
    let revision = String::from_utf8(git(&["rev-parse", "HEAD"])?)?
        .trim()
        .to_owned();
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let upstream_port = upstream.local_addr()?.port();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(CapabilitiesServer::new(Backend))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(upstream)),
    );
    let available = std::net::TcpListener::bind("127.0.0.1:0")?;
    let git_port = available.local_addr()?.port();
    drop(available);
    let state = dir.path().join("host");
    let session = dir.path().join("agent");
    let rc = dir.path().join("provisioned.bazelrc");
    std::fs::write(&rc, "")?;
    let configuration = dir.path().join("config.toml");
    std::fs::write(
        &configuration,
        format!(
            "state_dir = {:?}\n[cache]\nupstream = {:?}\n[bos]\nenabled = true\n[git_proxy]\nlisten = {:?}\n[git_proxy.repositories]\nfixture = {:?}\n[session]\nbazel = '/bin/true'\nbazelrc = {:?}\n",
            state.display().to_string(),
            format!("grpc://127.0.0.1:{upstream_port}"),
            format!("127.0.0.1:{git_port}"),
            origin.display().to_string(),
            rc.display().to_string()
        ),
    )?;
    struct Fixture {
        child: Option<std::process::Child>,
        executable: PathBuf,
        config: PathBuf,
        session: PathBuf,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            if let Some(child) = self.child.as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
            let _ = Command::new(&self.executable)
                .arg("--config")
                .arg(&self.config)
                .arg("recover")
                .arg(&self.session)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
    let mut fixture = Fixture {
        child: None,
        executable: executable.clone(),
        config: configuration.clone(),
        session: session.clone(),
    };
    let start = || -> Result<std::process::Child> {
        Ok(Command::new(&executable)
            .arg("--config")
            .arg(&configuration)
            .arg("serve")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?)
    };
    let invoke = |args: Vec<std::ffi::OsString>| -> Result<Vec<u8>> {
        let output = Command::new(&executable)
            .arg("--config")
            .arg(&configuration)
            .args(args)
            .output()?;
        anyhow::ensure!(
            output.status.success(),
            "workspace fixture command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(output.stdout)
    };
    fixture.child = Some(start()?);
    let socket = state.join("control.sock");
    for _ in 0..200 {
        if call(socket.clone(), ControlRequest::Status).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // A default daemon may crash before any source session exists. Its output
    // mount must still be recoverable without inventing a session directory.
    fixture.child.as_mut().unwrap().kill()?;
    fixture.child.as_mut().unwrap().wait()?;
    fixture.child = None;
    let bos_only: serde_json::Value = serde_json::from_slice(&invoke(vec!["recover".into()])?)?;
    assert_eq!(bos_only["report"]["bos_detached"], true);
    assert!(bos_only["report"]["session"].is_null());
    fixture.child = Some(start()?);
    for _ in 0..200 {
        if call(socket.clone(), ControlRequest::Status).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    invoke(vec![
        "create".into(),
        session.as_os_str().into(),
        "--repo".into(),
        "fixture".into(),
        "--revision".into(),
        revision.into(),
    ])?;
    let workspace = session.join("workspace");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(workspace.join("tracked.txt"))?;
    file.write_all(b"durable edit\n")?;
    file.sync_all()?;
    drop(file);
    let stage = Command::new("git")
        .arg("-C")
        .arg(&workspace)
        .args(["add", "tracked.txt"])
        .output()?;
    anyhow::ensure!(stage.status.success(), "stage failed");
    let index = Command::new("git")
        .arg("-C")
        .arg(&workspace)
        .args(["ls-files", "--stage"])
        .output()?
        .stdout;
    assert!(
        call(
            socket.clone(),
            ControlRequest::Recover {
                session: Some(session.clone())
            }
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("live owned mount")
    );
    // Only this fixture's unreaped child is killed. No PID loaded from state.
    fixture.child.as_mut().unwrap().kill()?;
    fixture.child.as_mut().unwrap().wait()?;
    fixture.child = None;
    let recovered: serde_json::Value =
        serde_json::from_slice(&invoke(vec!["recover".into(), session.as_os_str().into()])?)?;
    assert_eq!(recovered["report"]["detached"], true);
    assert_eq!(recovered["report"]["bos_detached"], true);
    // Explicit recovery is idempotent and never deletes the durable overlay.
    invoke(vec!["recover".into(), session.as_os_str().into()])?;
    fixture.child = Some(start()?);
    for _ in 0..200 {
        if call(socket.clone(), ControlRequest::Status).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    invoke(vec!["resume".into(), session.as_os_str().into()])?;
    assert_eq!(
        std::fs::read(workspace.join("tracked.txt"))?,
        b"durable edit\n"
    );
    assert_eq!(
        Command::new("git")
            .arg("-C")
            .arg(&workspace)
            .args(["ls-files", "--stage"])
            .output()?
            .stdout,
        index
    );
    let staged = Command::new("git")
        .arg("-C")
        .arg(&workspace)
        .args(["show", ":tracked.txt"])
        .output()?;
    assert!(staged.status.success());
    assert_eq!(staged.stdout, b"durable edit\n");
    invoke(vec!["stop".into(), session.as_os_str().into()])?;
    call(socket, ControlRequest::Shutdown).await?;
    fixture.child.as_mut().unwrap().wait()?;
    fixture.child = None;
    server.abort();
    Ok(())
}
