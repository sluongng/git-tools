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
#[tonic::async_trait]
impl reapi::reapi::execution_server::Execution for Backend {
    type ExecuteStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<reapi::proto::google::longrunning::Operation, Status>,
                > + Send,
        >,
    >;
    type WaitExecutionStream = Self::ExecuteStream;
    async fn execute(
        &self,
        request: Request<reapi::reapi::ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        if request.into_inner().skip_cache_lookup {
            let first = tokio_stream::iter([Ok(reapi::proto::google::longrunning::Operation {
                name: "opaque-quiet-operation".into(),
                ..Default::default()
            })]);
            return Ok(Response::new(Box::pin(tokio_stream::StreamExt::chain(
                first,
                tokio_stream::pending(),
            ))));
        }
        Err(Status::unavailable("fixture scheduler reached"))
    }
    async fn wait_execution(
        &self,
        _: Request<reapi::reapi::WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        Err(Status::unavailable("fixture scheduler resume reached"))
    }
}
#[tonic::async_trait]
impl reapi::asset::fetch_server::Fetch for Backend {
    async fn fetch_blob(
        &self,
        _: Request<reapi::asset::FetchBlobRequest>,
    ) -> Result<Response<reapi::asset::FetchBlobResponse>, Status> {
        Ok(Response::new(reapi::asset::FetchBlobResponse {
            status: Some(reapi::proto::google::rpc::Status {
                code: 5,
                message: "fixture downloader reached".into(),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
    async fn fetch_directory(
        &self,
        _: Request<reapi::asset::FetchDirectoryRequest>,
    ) -> Result<Response<reapi::asset::FetchDirectoryResponse>, Status> {
        Err(Status::unimplemented("fixture directory reached"))
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
            .add_service(
                reapi::reapi::execution_server::ExecutionServer::new(Backend)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip),
            )
            .add_service(
                reapi::asset::fetch_server::FetchServer::new(Backend)
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip),
            )
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
    // A proof from another daemon/version must fail before a destructive
    // operation dispatch; the original daemon remains available afterward.
    let mismatch = call(
        socket.clone(),
        ControlRequest::Scoped {
            version: 0,
            digest: "invalid-proof".into(),
            request: Box::new(ControlRequest::Shutdown),
        },
    )
    .await
    .unwrap_err();
    assert!(mismatch.to_string().contains("credential scope mismatch"));
    assert_eq!(
        call(socket.clone(), ControlRequest::Status).await?["pid"],
        status["pid"]
    );
    // Repositories are configured here: session.git is captured by the
    // long-lived Git proxy, unlike the genuinely per-session tools.
    let mut changed_git = config.clone();
    changed_git.session.git = "different-git".into();
    let mismatch = tokio::task::spawn_blocking(move || {
        workspace_daemon::control::ScopedClient::new(&changed_git).validate_identity()
    })
    .await?
    .unwrap_err();
    assert!(mismatch.to_string().contains("credential scope mismatch"));
    let mut changed_session = config.clone();
    changed_session.session.bazel = "different-bazel".into();
    changed_session.session.git_vfs = "different-vfs".into();
    changed_session.session.bazelrc = Some("/unused/fixture.bazelrc".into());
    tokio::task::spawn_blocking(move || {
        workspace_daemon::control::ScopedClient::new(&changed_session).validate_identity()
    })
    .await??;
    assert_eq!(status["cache"]["resident_bytes"], 0);
    assert_eq!(status["proxy"]["read_slots_in_use"], 0);
    assert_eq!(status["proxy"]["read_slots_queued"], 0);
    assert!(status["bos_endpoint"].is_null());
    assert_eq!(status["bes_endpoint"], config.bes_endpoint());
    assert!(status["bes"].is_object());
    let path = config.state_dir.join("rbe.sock");
    let channel = tonic::transport::Endpoint::from_static("http://localhost")
        .connect_with_connector(tower::service_fn(move |_| {
            let path = path.clone();
            async move {
                tokio::net::UnixStream::connect(path)
                    .await
                    .map(hyper_util::rt::TokioIo::new)
            }
        }))
        .await?;
    let mut caps = reapi::reapi::capabilities_client::CapabilitiesClient::new(channel.clone());
    caps.get_capabilities(GetCapabilitiesRequest::default())
        .await?;
    let mut execution = reapi::reapi::execution_client::ExecutionClient::new(channel.clone());
    let error = execution
        .execute(reapi::reapi::ExecuteRequest::default())
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "fixture scheduler reached");
    let mut downloader = reapi::asset::fetch_client::FetchClient::new(channel.clone());
    assert_eq!(
        downloader
            .fetch_blob(reapi::asset::FetchBlobRequest {
                uris: vec!["https://example.invalid/archive".into()],
                ..Default::default()
            })
            .await?
            .into_inner()
            .status
            .unwrap()
            .message,
        "fixture downloader reached"
    );
    let mut events = reapi::bes::publish_build_event_client::PublishBuildEventClient::new(channel);
    let event = reapi::bes::PublishBuildToolEventStreamRequest {
        ordered_build_event: Some(reapi::bes::OrderedBuildEvent {
            stream_id: Some(reapi::bes::StreamId {
                build_id: "neutral-build".into(),
                invocation_id: "neutral-invocation".into(),
                component: 3,
            }),
            sequence_number: 1,
            event: Some(reapi::bes::BuildEvent {
                event: Some(reapi::bes::build_event::Event::ComponentStreamFinished(
                    reapi::bes::build_event::BuildComponentStreamFinished { r#type: 1 },
                )),
                ..Default::default()
            }),
        }),
        ..Default::default()
    };
    let mut acks = events
        .publish_build_tool_event_stream(tokio_stream::iter([event]))
        .await?
        .into_inner();
    assert_eq!(acks.message().await?.unwrap().sequence_number, 1);
    assert!(acks.message().await?.is_none());
    for old in ["cache.sock", "bes.sock", "bos.sock"] {
        assert!(!config.state_dir.join(old).exists());
    }

    use agent_session::MountController;
    let cli = Client {
        socket: socket.clone(),
    };
    let args = tokio::task::spawn_blocking(move || cli.bazel_args()).await??;
    assert!(args.contains(&format!("--bes_backend={}", config.bes_endpoint())));
    assert_eq!(status["rbe_endpoint"], config.rbe_endpoint());
    for flag in [
        "remote_cache",
        "remote_executor",
        "experimental_remote_downloader",
    ] {
        assert!(args.contains(&format!("--{flag}={}", config.rbe_endpoint())));
    }
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
        std::fs::metadata(config.state_dir.join("rbe.sock"))?
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
    let mut quiet = execution
        .execute(reapi::reapi::ExecuteRequest {
            skip_cache_lookup: true,
            ..Default::default()
        })
        .await?
        .into_inner();
    assert_eq!(
        quiet.message().await?.unwrap().name,
        "opaque-quiet-operation"
    );
    call(socket, ControlRequest::Shutdown).await?;
    assert_eq!(
        quiet.message().await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
    tokio::time::timeout(Duration::from_secs(5), task).await???;
    assert!(config.state_dir.join("mounts.json").is_file());
    assert!(!config.socket().exists());
    assert!(!config.state_dir.join("rbe.sock").exists());
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

#[test]
fn cli_reuse_binds_effective_config_and_credentials_but_not_session_settings() -> Result<()> {
    use std::process::Command;
    let executable = PathBuf::from(env!("CARGO_BIN_EXE_workspace"));
    let dir = tempfile::tempdir()?;
    let unavailable = std::net::TcpListener::bind("127.0.0.1:0")?;
    let upstream = format!("grpc://{}", unavailable.local_addr()?);
    // Keep this listener alive without serving: no real backend or credentials.
    let state = dir.path().join("host");
    let other_state = dir.path().join("other-host");
    struct Cleanup(Vec<PathBuf>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            for root in &self.0 {
                let _ = Client {
                    socket: root.join("control.sock"),
                }
                .call(ControlRequest::Shutdown);
            }
        }
    }
    let _cleanup = Cleanup(vec![state.clone(), other_state.clone()]);
    let write_config = |name: &str,
                        root: &std::path::Path,
                        endpoint: &str,
                        instance: &str,
                        selector: &str,
                        bes: bool,
                        extra: &str|
     -> Result<PathBuf> {
        let path = dir.path().join(format!("{name}.toml"));
        std::fs::write(
            &path,
            format!(
                "state_dir = {:?}\n[cache]\nupstream = {endpoint:?}\ninstance_name = {instance:?}\napi_key_env = {selector:?}\n[bes]\nenabled = {bes}\n{extra}\n",
                root.to_str().unwrap(),
            ),
        )?;
        Ok(path)
    };
    let run = |path: &std::path::Path,
               operation: &str,
               credential: &str|
     -> Result<std::process::Output> {
        Ok(Command::new(&executable)
            .arg("--config")
            .arg(path)
            .arg(operation)
            .env("WORKSPACE_FIXTURE_KEY", credential)
            .env("WORKSPACE_FIXTURE_OTHER_KEY", credential)
            .output()?)
    };
    let a = write_config(
        "a",
        &state,
        &upstream,
        "scope-a",
        "WORKSPACE_FIXTURE_KEY",
        true,
        "",
    )?;
    let first = run(&a, "retry-bes", "synthetic-credential-a")?;
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let initial: serde_json::Value =
        serde_json::from_slice(&run(&a, "status", "synthetic-credential-a")?.stdout)?;
    let pid = initial["report"]["pid"].clone();
    assert!(pid.is_u64());
    assert!(initial["report"].get("identity").is_none());
    assert!(!initial.to_string().contains("synthetic-credential"));
    struct Case<'a> {
        name: &'a str,
        endpoint: &'a str,
        instance: &'a str,
        selector: &'a str,
        bes: bool,
        credential: &'a str,
        extra: &'a str,
        compatible: bool,
    }
    for case in [
        Case {
            name: "instance scope",
            instance: "scope-b",
            ..Case::default(&upstream)
        },
        Case {
            name: "upstream",
            endpoint: "grpc://127.0.0.1:1",
            ..Case::default(&upstream)
        },
        Case {
            name: "BES enablement",
            bes: false,
            ..Case::default(&upstream)
        },
        Case {
            name: "credential selector",
            selector: "WORKSPACE_FIXTURE_OTHER_KEY",
            ..Case::default(&upstream)
        },
        Case {
            name: "credential rotation",
            credential: "synthetic-credential-b",
            ..Case::default(&upstream)
        },
        Case {
            name: "BES credential selector",
            extra: "api_key_env = 'WORKSPACE_FIXTURE_OTHER_KEY'",
            ..Case::default(&upstream)
        },
        Case {
            name: "BES credential inheritance",
            extra: "api_key_env = 'WORKSPACE_FIXTURE_KEY'",
            compatible: true,
            ..Case::default(&upstream)
        },
        Case {
            name: "session executable choices",
            extra: "[session]\ngit = 'different-git'\nbazel = 'different-bazel'\ngit_vfs = 'different-vfs'\nbazelrc = '/unused/fixture.bazelrc'",
            compatible: true,
            ..Case::default(&upstream)
        },
    ] {
        let candidate = write_config(
            "candidate",
            &state,
            case.endpoint,
            case.instance,
            case.selector,
            case.bes,
            case.extra,
        )?;
        let output = run(&candidate, "retry-bes", case.credential)?;
        assert_eq!(
            output.status.success(),
            case.compatible,
            "{}: {}",
            case.name,
            String::from_utf8_lossy(&output.stderr)
        );
        if !case.compatible {
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("daemon configuration or credential scope mismatch"),
                "{}",
                case.name
            );
            // Read-only and destructive commands must not silently select A either.
            for operation in ["status", "shutdown"] {
                let output = run(&candidate, operation, case.credential)?;
                assert!(!output.status.success(), "{}: {operation}", case.name);
            }
        }
        assert!(!String::from_utf8_lossy(&output.stdout).contains("synthetic-credential"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("synthetic-credential"));
    }
    impl<'a> Case<'a> {
        fn default(endpoint: &'a str) -> Self {
            Self {
                name: "same scope",
                endpoint,
                instance: "scope-a",
                selector: "WORKSPACE_FIXTURE_KEY",
                bes: true,
                credential: "synthetic-credential-a",
                extra: "",
                compatible: false,
            }
        }
    }
    let b = write_config(
        "b",
        &other_state,
        &upstream,
        "scope-b",
        "WORKSPACE_FIXTURE_OTHER_KEY",
        true,
        "",
    )?;
    assert!(
        run(&b, "retry-bes", "synthetic-credential-b")?
            .status
            .success()
    );
    let status_b: serde_json::Value =
        serde_json::from_slice(&run(&b, "status", "synthetic-credential-b")?.stdout)?;
    assert_eq!(status_b["report"]["instance_name"], "scope-b");
    assert_ne!(status_b["report"]["pid"], pid);
    let final_a: serde_json::Value =
        serde_json::from_slice(&run(&a, "status", "synthetic-credential-a")?.stdout)?;
    assert_eq!(final_a["report"]["pid"], pid);
    assert_eq!(final_a["report"]["instance_name"], "scope-a");
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

#[derive(Default)]
struct SyncState {
    capabilities: std::sync::atomic::AtomicUsize,
    find_missing: std::sync::atomic::AtomicUsize,
    uploads: std::sync::atomic::AtomicUsize,
    blob: std::sync::Mutex<Option<Vec<u8>>>,
}
#[derive(Clone, Default)]
struct SyncBackend(std::sync::Arc<SyncState>);
#[tonic::async_trait]
impl Capabilities for SyncBackend {
    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        assert_eq!(request.into_inner().instance_name, "fixture/cas");
        self.0
            .capabilities
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Response::new(ServerCapabilities {
            cache_capabilities: Some(reapi::reapi::CacheCapabilities {
                digest_functions: vec![reapi::reapi::digest_function::Value::Blake3 as i32],
                max_batch_total_size_bytes: 1024 * 1024,
                max_cas_blob_size_bytes: 1024 * 1024,
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}
#[tonic::async_trait]
impl reapi::reapi::content_addressable_storage_server::ContentAddressableStorage for SyncBackend {
    async fn find_missing_blobs(
        &self,
        request: Request<reapi::reapi::FindMissingBlobsRequest>,
    ) -> Result<Response<reapi::reapi::FindMissingBlobsResponse>, Status> {
        let request = request.into_inner();
        assert_eq!(request.instance_name, "fixture/cas");
        assert_eq!(
            request.digest_function,
            reapi::reapi::digest_function::Value::Blake3 as i32
        );
        self.0
            .find_missing
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Response::new(reapi::reapi::FindMissingBlobsResponse {
            missing_blob_digests: if self.0.blob.lock().unwrap().is_none() {
                request.blob_digests
            } else {
                vec![]
            },
        }))
    }
    async fn batch_update_blobs(
        &self,
        request: Request<reapi::reapi::BatchUpdateBlobsRequest>,
    ) -> Result<Response<reapi::reapi::BatchUpdateBlobsResponse>, Status> {
        let request = request.into_inner();
        assert_eq!(request.instance_name, "fixture/cas");
        assert_eq!(
            request.digest_function,
            reapi::reapi::digest_function::Value::Blake3 as i32
        );
        assert_eq!(request.requests.len(), 1);
        let mut responses = Vec::new();
        for upload in request.requests {
            assert_eq!(upload.compressor, 0);
            let digest = upload.digest.unwrap();
            assert_eq!(digest.hash, blake3::hash(&upload.data).to_hex().to_string());
            assert_eq!(digest.size_bytes, upload.data.len() as i64);
            *self.0.blob.lock().unwrap() = Some(upload.data);
            self.0
                .uploads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            responses.push(reapi::reapi::batch_update_blobs_response::Response {
                digest: Some(digest),
                status: Some(reapi::proto::google::rpc::Status::default()),
            });
        }
        Ok(Response::new(reapi::reapi::BatchUpdateBlobsResponse {
            responses,
        }))
    }
    async fn batch_read_blobs(
        &self,
        _: Request<reapi::reapi::BatchReadBlobsRequest>,
    ) -> Result<Response<reapi::reapi::BatchReadBlobsResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    type GetTreeStream = tokio_stream::Empty<Result<reapi::reapi::GetTreeResponse, Status>>;
    async fn get_tree(
        &self,
        _: Request<reapi::reapi::GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn split_blob(
        &self,
        _: Request<reapi::reapi::SplitBlobRequest>,
    ) -> Result<Response<reapi::reapi::SplitBlobResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn splice_blob(
        &self,
        _: Request<reapi::reapi::SpliceBlobRequest>,
    ) -> Result<Response<reapi::reapi::SpliceBlobResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    type GetChunkMappingStream =
        tokio_stream::Empty<Result<reapi::reapi::GetChunkMappingResponse, Status>>;
    async fn get_chunk_mapping(
        &self,
        _: Request<reapi::reapi::GetChunkMappingRequest>,
    ) -> Result<Response<Self::GetChunkMappingStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn register_chunk_mapping(
        &self,
        _: Request<tonic::Streaming<reapi::reapi::RegisterChunkMappingRequest>>,
    ) -> Result<Response<reapi::reapi::RegisterChunkMappingResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_alias_config_publishes_git_mapping_to_cas() -> Result<()> {
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = tempfile::tempdir()?;
    let git = PathBuf::from(std::env::var_os("GIT_PROXY_TEST_GIT").expect("declared Git fixture"));
    async fn git_run(git: &std::path::Path, args: &[&str]) -> Result<String> {
        let output = tokio::process::Command::new(git)
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .await?;
        anyhow::ensure!(output.status.success(), "fixture Git failed");
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }
    let repository = dir.path().join("origin");
    let repository = repository.to_str().unwrap();
    git_run(&git, &["init", "-b", "main", repository]).await?;
    git_run(&git, &["-C", repository, "config", "user.name", "Test"]).await?;
    git_run(
        &git,
        &[
            "-C",
            repository,
            "config",
            "user.email",
            "test@example.invalid",
        ],
    )
    .await?;
    let content = b"mapping upload fixture\n";
    std::fs::write(std::path::Path::new(repository).join("hello"), content)?;
    git_run(&git, &["-C", repository, "add", "."]).await?;
    git_run(&git, &["-C", repository, "commit", "-m", "fixture"]).await?;
    let oid = git_run(&git, &["-C", repository, "rev-parse", "HEAD:hello"]).await?;
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let upstream_port = upstream.local_addr()?.port();
    let backend = SyncBackend::default();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(CapabilitiesServer::new(backend.clone()))
            .add_service(reapi::reapi::content_addressable_storage_server::ContentAddressableStorageServer::new(backend.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(upstream)),
    );
    let cache_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let cache_port = cache_listener.local_addr()?.port();
    let git_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let git_port = git_listener.local_addr()?.port();
    drop((cache_listener, git_listener));
    let state = dir.path().join("host");
    let text = format!(
        "state_dir = {:?}\n[cache]\nlisten = '127.0.0.1:{cache_port}'\nupstream = 'grpc://127.0.0.1:{upstream_port}'\ninstance_name = 'fixture/cas'\napi_key_env = 'UNSET_FIXTURE_CAS_KEY'\n[bos]\nenabled = false\n[bes]\nenabled = false\n[git_proxy]\nlisten = '127.0.0.1:{git_port}'\nsync_to_cas = true\n[git_proxy.repositories]\nfixture = {repository:?}\n[session]\ngit = {:?}\n",
        state, git,
    );
    let config_file = dir.path().join("config.toml");
    std::fs::write(&config_file, &text)?;
    let config = Config::load(&config_file)?;
    let mut unsupported = config.clone();
    unsupported.state_dir = dir.path().join("unsupported");
    unsupported.cache.upstream = "grpc://example.invalid/path".into();
    unsupported.validate()?;
    let error = workspace_daemon::daemon::serve(unsupported.clone())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Git CAS upstream must be an origin without credentials, path or query"
    );
    assert!(
        !unsupported.state_dir.exists(),
        "reject before producer admission"
    );
    assert_eq!(backend.0.capabilities.load(Ordering::SeqCst), 0);
    let socket = config.socket();
    let task = tokio::spawn(workspace_daemon::daemon::serve(config.clone()));
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if call(socket.clone(), ControlRequest::Status).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let token = std::fs::read_to_string(config.state_dir.join("cache.token"))?;
    async fn request(
        addr: std::net::SocketAddr,
        token: &str,
        body: Option<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let (method, path, body) = match body {
            Some(body) => (
                "POST",
                "/api/v1/repos/fixture/objects",
                serde_json::to_string(&body)?,
            ),
            None => ("GET", "/api/v1/cache/stats", String::new()),
        };
        let mut stream = tokio::net::TcpStream::connect(addr).await?;
        stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await?;
        let mut response = String::new();
        stream.read_to_string(&mut response).await?;
        anyhow::ensure!(
            response.starts_with("HTTP/1.1 200"),
            "fixture HTTP request failed"
        );
        let (_, body) = response.split_once("\r\n\r\n").unwrap();
        Ok(serde_json::from_str(body)?)
    }
    let mapping = request(
        config.git_proxy.listen,
        &token,
        Some(serde_json::json!({"oids":[oid],"blake3":true})),
    )
    .await?;
    assert_eq!(
        mapping["objects"][0]["blake3"]["hash"],
        blake3::hash(content).to_hex().to_string()
    );
    let stats = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let stats = request(config.git_proxy.listen, &token, None)
                .await
                .unwrap();
            if stats["cas_sync"]["pending_objects"] == 0 {
                break stats;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(stats["cas_sync"]["failed_jobs"], 0);
    assert_eq!(stats["cas_sync"]["confirmed_objects"], 1);
    assert_eq!(stats["cas_sync"]["uploaded_objects"], 1);
    assert!(backend.0.capabilities.load(Ordering::SeqCst) >= 1);
    assert!(backend.0.find_missing.load(Ordering::SeqCst) >= 1);
    assert_eq!(backend.0.uploads.load(Ordering::SeqCst), 1);
    assert_eq!(
        backend.0.blob.lock().unwrap().as_deref(),
        Some(content.as_slice())
    );
    call(socket, ControlRequest::Shutdown).await?;
    tokio::time::timeout(Duration::from_secs(10), task).await???;
    server.abort();
    Ok(())
}
