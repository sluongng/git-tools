//! Shipped CLI acceptance against an actual Linux FUSE mount. Protocol fixtures
//! must not count as acceptance of lazy, externally administered workspaces.
#![cfg(target_os = "linux")]

use futures::StreamExt;
use p::repository_server::{Repository, RepositoryServer};
use q::repository_query_server::{RepositoryQuery, RepositoryQueryServer};
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    os::unix::{net::UnixStream, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tinytree_client::{protocol as p, query_protocol as q};
use tinytree_model as m;
use tonic::{
    Request as RpcRequest, Response as RpcResponse, Status, Streaming, codec::CompressionEncoding,
};
use workspace_daemon::control::{Request, Response};

struct Fixture {
    directory: Option<tempfile::TempDir>,
    cli: PathBuf,
    daemon_binary: PathBuf,
    daemon: Option<Child>,
    home: PathBuf,
    registry: PathBuf,
    jj_config: PathBuf,
    daemon_config: PathBuf,
    empty_path: PathBuf,
    socket: PathBuf,
    broker: PathBuf,
    command_number: usize,
    child_env: Vec<(String, String)>,
}

impl Fixture {
    fn new() -> Self {
        // A short private scratch path is necessary for sockaddr_un. This is a
        // native-provider fixture, not a remotely executable protocol suite.
        let directory = tempfile::Builder::new()
            .prefix("tt-native-")
            .tempdir_in("/tmp")
            .unwrap();
        let base = directory.path();
        let home = base.join("home");
        let registry = base.join("registry");
        let empty_path = base.join("no-programs");
        fs::create_dir(&home).unwrap();
        fs::create_dir(&empty_path).unwrap();
        let jj_config = base.join("jj.toml");
        fs::write(
            &jj_config,
            "[user]\nname='Native Fixture'\nemail='fixture@example.invalid'\n\
             [operation]\nhostname='test'\nusername='test'\n\
             [ui]\npaginate='never'\n",
        )
        .unwrap();
        let cache_port = TcpListener::bind("127.0.0.1:0").unwrap();
        let git_port = TcpListener::bind("127.0.0.1:0").unwrap();
        let daemon_config = base.join("daemon.toml");
        let state = base.join("daemon");
        let quoted = |path: &Path| toml::Value::String(path.to_str().unwrap().to_owned());
        fs::write(
            &daemon_config,
            format!(
                "state_dir={}\n\
                 [cache]\nlisten='{}'\nupstream='grpc://127.0.0.1:9'\n\
                 api_key_env='TINYTREE_UNUSED_TEST_KEY'\n\
                 [git_proxy]\nlisten='{}'\n\
                 [bes]\nenabled=false\n[bos]\nenabled=false\n\
                 [native.workspaces]\nregistry_path={}\n\
                 [native.repositories.example]\n",
                quoted(&state),
                cache_port.local_addr().unwrap(),
                git_port.local_addr().unwrap(),
                quoted(&registry),
            ),
        )
        .unwrap();
        workspace_daemon::config::Config::load(&daemon_config).unwrap();
        drop((cache_port, git_port));
        Self {
            cli: Path::new(env!("CARGO_BIN_EXE_tinytree-jj"))
                .canonicalize()
                .unwrap(),
            daemon_binary: Path::new(env!("CARGO_BIN_EXE_workspace"))
                .canonicalize()
                .unwrap(),
            daemon: None,
            home,
            registry,
            jj_config,
            daemon_config,
            empty_path,
            socket: state.join("control.sock"),
            broker: state.join("rbe.sock"),
            directory: Some(directory),
            command_number: 0,
            child_env: vec![],
        }
    }

    fn base(&self) -> &Path {
        self.directory.as_ref().unwrap().path()
    }

    fn environment(&self, command: &mut Command) {
        command
            .env_clear()
            .env("HOME", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join("config"))
            .env("TINYTREE_REGISTRY", &self.registry)
            .env("JJ_CONFIG", &self.jj_config)
            .env("TERM", "dumb")
            .envs(self.child_env.iter().map(|(name, value)| (name, value)))
            .stdin(Stdio::null());
        unsafe extern "C" {
            fn umask(mask: u32) -> u32;
        }
        // Exercise ordinary shell defaults in the CLI, daemon, and recovery
        // helper rather than inheriting a restrictive test-runner umask.
        // SAFETY: umask is async-signal-safe and affects only the forked child.
        unsafe {
            command.pre_exec(|| {
                umask(0o022);
                Ok(())
            });
        }
    }

    fn configure_remote(&mut self, endpoint: &str) {
        let mut config: toml::Value =
            toml::from_str(&fs::read_to_string(&self.daemon_config).unwrap()).unwrap();
        let mut remote = jj_native_remote::Config::default();
        remote.remote.endpoint = endpoint.to_owned();
        remote.remote.repository_id = "example".into();
        remote.remote.token_env = Some("TINYTREE_MOUNT_TEST_TOKEN".into());
        remote.authorization_profile = "native-mount-fixture".into();
        config["native"]["repositories"]["example"]
            .as_table_mut()
            .unwrap()
            .insert("remote".into(), toml::Value::try_from(remote).unwrap());
        fs::write(&self.daemon_config, toml::to_string(&config).unwrap()).unwrap();
        workspace_daemon::config::Config::load(&self.daemon_config).unwrap();
        self.child_env
            .push(("TINYTREE_MOUNT_TEST_TOKEN".into(), REMOTE_TOKEN.into()));
    }

    fn index_mode(&mut self, mode: &str) {
        let mut config: toml::Value =
            toml::from_str(&fs::read_to_string(&self.daemon_config).unwrap()).unwrap();
        config
            .as_table_mut()
            .unwrap()
            .entry("native")
            .or_insert_with(|| toml::Value::Table(Default::default()))
            .as_table_mut()
            .unwrap()
            .entry("workspaces")
            .or_insert_with(|| toml::Value::Table(Default::default()))
            .as_table_mut()
            .unwrap()
            .insert("index_mode".into(), mode.into());
        fs::write(&self.daemon_config, toml::to_string(&config).unwrap()).unwrap();
    }

    fn start(&mut self) {
        assert!(self.daemon.is_none());
        let log = fs::File::create(self.base().join("daemon.log")).unwrap();
        let mut command = Command::new(&self.daemon_binary);
        self.environment(&mut command);
        // Only the daemon's kernel mount helper needs a native utility PATH.
        // The shipped jj commands below have no Git or other executable on PATH.
        command
            .args(["--config"])
            .arg(&self.daemon_config)
            .arg("serve")
            .current_dir(self.base())
            .env("PATH", "/usr/bin:/bin")
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        self.daemon = Some(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if self.control(Request::Status).is_ok() {
                return;
            }
            if let Some(status) = self.daemon.as_mut().unwrap().try_wait().unwrap() {
                panic!(
                    "daemon exited {status}: {}",
                    fs::read_to_string(self.base().join("daemon.log")).unwrap()
                );
            }
            assert!(Instant::now() < deadline, "daemon startup timed out");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn control(&self, request: Request) -> anyhow::Result<serde_json::Value> {
        let mut stream = UnixStream::connect(&self.socket)?;
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        stream.set_write_timeout(Some(Duration::from_secs(3)))?;
        let mut bytes = serde_json::to_vec(&request)?;
        bytes.push(b'\n');
        stream.write_all(&bytes)?;
        let mut bytes = Vec::new();
        BufReader::new(stream)
            .take(1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes)?;
        anyhow::ensure!(bytes.len() <= 1024 * 1024, "oversized control response");
        let response: Response = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(response.ok, "{:?}", response.error);
        Ok(response.result)
    }

    fn run(&mut self, cwd: &Path, args: &[&str]) -> Output {
        let binary = self.cli.clone();
        let args: Vec<_> = ["--no-pager", "--color=never"]
            .into_iter()
            .chain(args.iter().copied())
            .collect();
        self.run_binary(cwd, &binary, &args, false)
    }

    fn workspace(&mut self, args: &[&str]) -> Output {
        let binary = self.daemon_binary.clone();
        let config = self.daemon_config.to_str().unwrap().to_owned();
        let args: Vec<_> = ["--config", config.as_str()]
            .into_iter()
            .chain(args.iter().copied())
            .collect();
        let base = self.base().to_owned();
        self.run_binary(&base, &binary, &args, true)
    }

    fn run_binary(
        &mut self,
        cwd: &Path,
        binary: &Path,
        args: &[&str],
        native_path: bool,
    ) -> Output {
        self.command_number += 1;
        let stdout_path = self.base().join(format!("{}.out", self.command_number));
        let stderr_path = self.base().join(format!("{}.err", self.command_number));
        let mut command = Command::new(binary);
        self.environment(&mut command);
        command
            .args(args)
            .current_dir(cwd)
            .env(
                "PATH",
                if native_path {
                    Path::new("/usr/bin:/bin")
                } else {
                    &self.empty_path
                },
            )
            .stdout(fs::File::create(&stdout_path).unwrap())
            .stderr(fs::File::create(&stderr_path).unwrap());
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "CLI timed out {args:?}: {}",
                    fs::read_to_string(&stderr_path).unwrap()
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        Output {
            status,
            stdout: fs::read(stdout_path).unwrap(),
            stderr: fs::read(stderr_path).unwrap(),
        }
    }

    fn success(&mut self, cwd: &Path, args: &[&str]) -> String {
        let output = self.run(cwd, args);
        assert!(
            output.status.success(),
            "{args:?}: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if args.contains(&"log") {
            assert!(
                !String::from_utf8_lossy(&output.stdout).contains("<Error"),
                "successful CLI log rendered a template failure {args:?}: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        String::from_utf8(output.stdout).unwrap()
    }

    fn initialize(&mut self, root: &Path) {
        let base = self.base().to_owned();
        let broker = self.broker.clone();
        self.success(
            &base,
            &[
                "tinytree",
                "init",
                root.to_str().unwrap(),
                "--broker",
                broker.to_str().unwrap(),
                "--store",
                "example",
            ],
        );
        assert_fuse_mounted(root);
    }

    fn clone_remote(&mut self, root: &Path) {
        let base = self.base().to_owned();
        let broker = self.broker.clone();
        self.success(
            &base,
            &[
                "tinytree",
                "clone",
                root.to_str().unwrap(),
                "--broker",
                broker.to_str().unwrap(),
                "--store",
                "example",
                "--reference",
                "main",
                "--bookmark",
                "main",
            ],
        );
        assert_fuse_mounted(root);
    }

    fn revision(&mut self, root: &Path, revision: &str) -> String {
        let id = self.success(
            root,
            &["log", "--no-graph", "-r", revision, "-T", "commit_id"],
        );
        let id = id.trim().to_owned();
        assert_eq!(id.len(), 64);
        assert!(id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        id
    }

    fn binding(&self, root: &Path) -> native_workspace_admin::Binding {
        let registry = native_workspace_admin::Registry::open_existing(&self.registry).unwrap();
        let binding = registry.lookup(root).unwrap();
        let marker = fs::read_to_string(root.join(".jj/native-workspace")).unwrap();
        assert_eq!(marker, binding.id());
        assert_eq!(marker.len(), 64);
        let control_entries: Vec<_> = fs::read_dir(root.join(".jj"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(
            control_entries,
            [std::ffi::OsString::from("native-workspace")]
        );
        assert!(!binding.admin.starts_with(root));
        assert!(!binding.repo.starts_with(root));
        assert!(!binding.working_copy.starts_with(root));
        assert_eq!(
            fs::read_to_string(binding.repo.join("store/type")).unwrap(),
            "tinytree-native-v1"
        );
        for directory in ["op_store", "op_heads", "index"] {
            assert!(binding.repo.join(directory).is_dir());
        }
        assert!(!root.join(".git").exists());
        binding
    }

    fn stop_daemon(&mut self) {
        self.control(Request::Shutdown).unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            // Keep the owned child in the fixture on errors/timeouts so Drop
            // cannot mistake a still-running writer for a completed shutdown.
            if let Some(status) = self.daemon.as_mut().unwrap().try_wait().unwrap() {
                assert!(status.success());
                self.daemon.take();
                return;
            }
            assert!(Instant::now() < deadline, "daemon shutdown timed out");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn kill_daemon(&mut self) {
        let mut child = self.daemon.take().unwrap();
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(
            !status.success(),
            "the fixture owner must actually be killed"
        );
    }

    fn retain_scratch_if_mounted(&mut self) {
        let mounts = fs::read_to_string("/proc/self/mountinfo");
        let safe = mounts.is_ok_and(|mounts| {
            !mounts.lines().any(|line| {
                line.split_whitespace()
                    .nth(4)
                    .is_some_and(|path| Path::new(path).starts_with(self.base()))
            })
        });
        if !safe {
            let directory = self.directory.take().unwrap().keep();
            eprintln!(
                "native fixture mount remains; scratch retained at {}",
                directory.display()
            );
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if self.daemon.is_none() {
            self.retain_scratch_if_mounted();
            return;
        }
        // Never delete a live projection or kill its owner. On failure retain
        // the fixture's private paths and daemon for explicit recovery.
        let cleanup = (|| -> anyhow::Result<()> {
            let status = self.control(Request::Status)?;
            let mounts = status["native"]["workspaces"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("native status inventory unavailable"))?;
            for mount in mounts {
                if mount["active"].as_bool() == Some(true)
                    || mount["provider_recovery_owned"].as_bool() == Some(true)
                {
                    let id = mount["workspace_id"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("native mount identity unavailable"))?;
                    self.control(Request::NativeUnmount {
                        workspace_id: id.to_owned(),
                    })?;
                }
            }
            self.control(Request::Shutdown)?;
            Ok(())
        })();
        if let Err(error) = cleanup {
            let directory = self.directory.take().unwrap().keep();
            eprintln!(
                "native fixture cleanup requires recovery at {}: {error:#}",
                directory.display()
            );
            return;
        }
        let mut child = self.daemon.take().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => {
                    self.retain_scratch_if_mounted();
                    return;
                }
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                _ => {
                    let directory = self.directory.take().unwrap().keep();
                    eprintln!("native fixture owner retained at {}", directory.display());
                    return;
                }
            }
        }
    }
}

fn assert_fuse_mounted(root: &Path) {
    assert!(
        fuse_mounted(root),
        "expected an actual FUSE mount at {}",
        root.display()
    );
}

fn fuse_mounted(root: &Path) -> bool {
    let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap();
    mounts.lines().any(|line| {
        let Some((mount, filesystem)) = line.split_once(" - ") else {
            return false;
        };
        mount.split_whitespace().nth(4) == root.to_str() && filesystem.starts_with("fuse")
    })
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn shipped_cli_uses_lazy_mounts_and_external_per_workspace_state() {
    let mut fixture = Fixture::new();
    let base_dir = fixture.base().to_owned();
    let rejected = base_dir.join("no-broker");
    let output = fixture.run(&base_dir, &["tinytree", "init", rejected.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(
        !rejected.exists(),
        "a missing broker must not create a checkout"
    );
    fixture.start();
    let root = base_dir.join("workspace");
    fixture.initialize(&root);
    let binding = fixture.binding(&root);
    assert!(fs::write(root.join(".jj/native-workspace"), "tampered").is_err());

    fs::write(root.join("hello.txt"), "first native version\n").unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["describe", "-m", "native base"]);
    let base = fixture.revision(&root, "@");
    fixture.success(&root, &["new", "-m", "native child"]);
    fs::write(root.join("hello.txt"), "second native version\n").unwrap();
    fixture.success(&root, &["status"]);
    let child = fixture.revision(&root, "@");
    assert_ne!(base, child);
    assert_eq!(fixture.revision(&root, "@-"), base);
    fixture.success(&root, &["restore", "--from", "@-", "hello.txt"]);
    assert_eq!(
        fs::read(root.join("hello.txt")).unwrap(),
        b"first native version\n"
    );
    fixture.success(&root, &["undo"]);
    assert_eq!(
        fs::read(root.join("hello.txt")).unwrap(),
        b"second native version\n"
    );
    assert_eq!(fixture.revision(&root, "@"), child);
    fixture.success(&root, &["edit", &base]);
    assert_eq!(
        fs::read(root.join("hello.txt")).unwrap(),
        b"first native version\n"
    );
    fixture.success(&root, &["edit", &child]);
    assert_eq!(
        fixture.success(&root, &["file", "show", "hello.txt"]),
        "second native version\n"
    );

    // A second agent gets separate operation/index/WC state, not a worktree
    // sharing another agent's mutable administration.
    let second = base_dir.join("second");
    fixture.initialize(&second);
    let second_binding = fixture.binding(&second);
    assert_ne!(binding.workspace_id, second_binding.workspace_id);
    assert_ne!(binding.repo, second_binding.repo);
    assert_ne!(binding.working_copy, second_binding.working_copy);
    fs::write(second.join("independent.txt"), "second agent\n").unwrap();
    fixture.success(&second, &["status"]);
    assert!(!root.join("independent.txt").exists());
    assert!(!second.join("hello.txt").exists());

    // Do not snapshot this last edit. Stopping and resuming must preserve
    // unsnapshotted bytes as well as the previous canonical operation state.
    fs::write(root.join("hello.txt"), "dirty across restart\n").unwrap();
    fixture.success(
        &base_dir,
        &["tinytree", "unmount", second.to_str().unwrap()],
    );
    fixture.success(&base_dir, &["tinytree", "unmount", root.to_str().unwrap()]);
    assert!(
        !root.join("hello.txt").exists(),
        "no materialized fallback remains"
    );
    fixture.stop_daemon();
    fixture.start();
    fixture.success(&base_dir, &["tinytree", "resume", root.to_str().unwrap()]);
    assert_fuse_mounted(&root);
    assert_eq!(fixture.binding(&root), binding);
    assert_eq!(
        fs::read(root.join("hello.txt")).unwrap(),
        b"dirty across restart\n"
    );
    fixture.success(&root, &["status"]);
    assert_eq!(
        fixture.success(&root, &["file", "show", "hello.txt"]),
        "dirty across restart\n"
    );
    fixture.success(&base_dir, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
}

const REMOTE_TOKEN: &str = "public-mounted-native-token-not-a-secret";

/// Count real upstream method admission without replacing the repository
/// implementation, its compression, or its production deadline layer.
#[derive(Clone)]
struct CountFileReads<S> {
    inner: S,
    reads: Arc<AtomicUsize>,
    metadata: Arc<MetadataReads>,
}
impl<S, B> tower::Service<http::Request<B>> for CountFileReads<S>
where
    S: tower::Service<http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path() == "/tinytree.v1.Repository/ReadFile" {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        if request
            .uri()
            .path()
            .starts_with("/tinytree.query.v1.RepositoryQuery/")
        {
            self.metadata.query_calls.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.call(request)
    }
}

#[derive(Default)]
struct MetadataReads {
    keys: Mutex<HashSet<(i32, [u8; 32])>>,
    calls: AtomicUsize,
    canonical_bytes: AtomicUsize,
    commit_bytes: AtomicUsize,
    tree_bytes: AtomicUsize,
    other_bytes: AtomicUsize,
    graph_calls: AtomicUsize,
    graph_commit_bytes: AtomicUsize,
    query_calls: AtomicUsize,
    query_commit_bytes: AtomicUsize,
}

/// Observe decoded metadata without replacing any repository implementation.
#[derive(Clone)]
struct CountMetadata {
    inner: tinytree_server::RepositoryService,
    reads: Arc<MetadataReads>,
}

#[tonic::async_trait]
impl Repository for CountMetadata {
    async fn open_repository(
        &self,
        request: RpcRequest<p::OpenRepositoryRequest>,
    ) -> Result<RpcResponse<p::RepositoryView>, Status> {
        self.inner.open_repository(request).await
    }

    async fn get_commit_graph(
        &self,
        request: RpcRequest<p::GetCommitGraphRequest>,
    ) -> Result<RpcResponse<p::CommitGraphPage>, Status> {
        self.reads.graph_calls.fetch_add(1, Ordering::SeqCst);
        let response = self.inner.get_commit_graph(request).await?;
        self.reads.graph_commit_bytes.fetch_add(
            response
                .get_ref()
                .entries
                .iter()
                .map(|entry| entry.canonical_envelope.len())
                .sum::<usize>(),
            Ordering::SeqCst,
        );
        Ok(response)
    }

    type BatchReadObjectsStream =
        <tinytree_server::RepositoryService as Repository>::BatchReadObjectsStream;

    async fn batch_read_objects(
        &self,
        request: RpcRequest<p::BatchReadObjectsRequest>,
    ) -> Result<RpcResponse<Self::BatchReadObjectsStream>, Status> {
        self.reads.calls.fetch_add(1, Ordering::SeqCst);
        {
            let mut keys = self.reads.keys.lock().unwrap();
            for key in &request.get_ref().keys {
                if let Ok(id) = key.native_id.as_slice().try_into() {
                    if keys.len() >= 8192 && !keys.contains(&(key.kind, id)) {
                        return Err(Status::resource_exhausted("fixture metadata counter limit"));
                    }
                    keys.insert((key.kind, id));
                }
            }
        }
        let response = self.inner.batch_read_objects(request).await?;
        let reads = self.reads.clone();
        let body = response.into_inner().map(move |result| {
            if let Ok(p::ObjectResult {
                result: Some(p::object_result::Result::Object(object)),
                ..
            }) = &result
            {
                reads
                    .canonical_bytes
                    .fetch_add(object.canonical_envelope.len(), Ordering::SeqCst);
                match object
                    .descriptor
                    .as_ref()
                    .and_then(|descriptor| descriptor.key.as_ref())
                    .map(|key| key.kind)
                {
                    Some(kind) if kind == p::ObjectKind::Commit as i32 => {
                        reads
                            .commit_bytes
                            .fetch_add(object.canonical_envelope.len(), Ordering::SeqCst);
                    }
                    Some(kind) if kind == p::ObjectKind::Tree as i32 => {
                        reads
                            .tree_bytes
                            .fetch_add(object.canonical_envelope.len(), Ordering::SeqCst);
                    }
                    _ => {
                        reads
                            .other_bytes
                            .fetch_add(object.canonical_envelope.len(), Ordering::SeqCst);
                    }
                }
            }
            result
        });
        Ok(RpcResponse::new(Box::pin(body)))
    }

    async fn find_missing_objects(
        &self,
        request: RpcRequest<p::FindMissingObjectsRequest>,
    ) -> Result<RpcResponse<p::MissingObjects>, Status> {
        self.inner.find_missing_objects(request).await
    }

    type ReadFileStream = <tinytree_server::RepositoryService as Repository>::ReadFileStream;

    async fn read_file(
        &self,
        request: RpcRequest<p::ReadFileRequest>,
    ) -> Result<RpcResponse<Self::ReadFileStream>, Status> {
        self.inner.read_file(request).await
    }

    async fn write_object(
        &self,
        request: RpcRequest<Streaming<p::WriteFrame>>,
    ) -> Result<RpcResponse<p::WriteReceipt>, Status> {
        self.inner.write_object(request).await
    }

    async fn publish_reference(
        &self,
        request: RpcRequest<p::PublishReferenceRequest>,
    ) -> Result<RpcResponse<p::Publication>, Status> {
        self.inner.publish_reference(request).await
    }

    async fn get_publication(
        &self,
        request: RpcRequest<p::GetPublicationRequest>,
    ) -> Result<RpcResponse<p::Publication>, Status> {
        self.inner.get_publication(request).await
    }
}

/// Observe query payloads after the actual authenticated query implementation;
/// preserve its response extensions, admission and transport policy.
#[derive(Clone)]
struct CountQueryMetadata {
    inner: tinytree_server::QueryService,
    reads: Arc<MetadataReads>,
}
#[tonic::async_trait]
impl RepositoryQuery for CountQueryMetadata {
    async fn open_scope(
        &self,
        r: RpcRequest<Streaming<q::OpenScopeFrame>>,
    ) -> Result<RpcResponse<q::QueryScope>, Status> {
        self.inner.open_scope(r).await
    }
    async fn fork_scope(
        &self,
        r: RpcRequest<Streaming<q::ForkScopeFrame>>,
    ) -> Result<RpcResponse<q::QueryScope>, Status> {
        self.inner.fork_scope(r).await
    }
    async fn close_scope(
        &self,
        r: RpcRequest<q::CloseScopeRequest>,
    ) -> Result<RpcResponse<q::CloseScopeResponse>, Status> {
        self.inner.close_scope(r).await
    }
    async fn query_facts(
        &self,
        r: RpcRequest<q::QueryFactsRequest>,
    ) -> Result<RpcResponse<q::GraphFacts>, Status> {
        self.inner.query_facts(r).await
    }
    async fn resolve_commit_prefix(
        &self,
        r: RpcRequest<q::ResolveCommitPrefixRequest>,
    ) -> Result<RpcResponse<q::CommitPrefixResult>, Status> {
        self.inner.resolve_commit_prefix(r).await
    }
    async fn resolve_change_prefix(
        &self,
        r: RpcRequest<q::ResolveChangePrefixRequest>,
    ) -> Result<RpcResponse<q::ChangePrefixResult>, Status> {
        self.inner.resolve_change_prefix(r).await
    }
    async fn shortest_prefix(
        &self,
        r: RpcRequest<q::ShortestPrefixRequest>,
    ) -> Result<RpcResponse<q::ShortestPrefixResult>, Status> {
        self.inner.shortest_prefix(r).await
    }
    async fn close_query(
        &self,
        r: RpcRequest<q::CloseQueryRequest>,
    ) -> Result<RpcResponse<q::CloseQueryResponse>, Status> {
        self.inner.close_query(r).await
    }
    async fn evaluate(
        &self,
        r: RpcRequest<q::EvaluateRequest>,
    ) -> Result<RpcResponse<q::EvaluatePage>, Status> {
        let response = self.inner.evaluate(r).await?;
        let bytes = response
            .get_ref()
            .rows
            .iter()
            .filter_map(|row| row.canonical.as_ref())
            .map(|object| object.canonical_envelope.len())
            .sum::<usize>();
        self.reads
            .query_commit_bytes
            .fetch_add(bytes, Ordering::SeqCst);
        Ok(response)
    }
}

struct RemoteServer {
    _directory: tempfile::TempDir,
    runtime: Option<tokio::runtime::Runtime>,
    task: Option<tokio::task::JoinHandle<()>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    store: tinytree_store::Store,
    endpoint: String,
    head: String,
    historical_head: String,
    historical_trees: Vec<tinytree_model::ObjectKey>,
    source_file: tinytree_model::ObjectKey,
    source_bytes: Vec<u8>,
    reads: Arc<AtomicUsize>,
    metadata: Arc<MetadataReads>,
    setup_ns: u128,
    index_ready_ns: u128,
}

impl RemoteServer {
    fn start() -> Self {
        Self::start_with_history(8)
    }
    fn start_with_history(history: usize) -> Self {
        assert!((1..=1024).contains(&history));
        let setup_started = Instant::now();
        let directory = tempfile::tempdir().unwrap();
        let store = tinytree_store::Store::open(directory.path().join("store"), Default::default())
            .unwrap();
        store.create_repository("example").unwrap();
        let seed = tinytree_fixture::build_fixture(tinytree_fixture::FixtureConfig {
            unused_files: 2,
            bytes_per_file: 64,
        })
        .unwrap();
        for object in seed.objects() {
            store
                .put("example", object.descriptor, &object.bytes, 0)
                .unwrap();
        }
        let root_commit = seed
            .objects()
            .iter()
            .find(|object| object.descriptor.key.kind == m::Kind::Commit)
            .unwrap();
        let m::Object::Commit(template) =
            m::decode(&root_commit.bytes, m::Limits::default()).unwrap()
        else {
            unreachable!()
        };
        let root = seed
            .objects()
            .iter()
            .find(|object| object.descriptor.key.id.0 == template.root_tree.terms[0].0)
            .unwrap();
        let m::Object::Tree(current_tree) = m::decode(&root.bytes, m::Limits::default()).unwrap()
        else {
            unreachable!()
        };
        let source_value = current_tree
            .entries
            .iter()
            .find(|entry| entry.name == "hello.cc")
            .unwrap()
            .value
            .clone();
        let mut index_commits = vec![(root_commit.descriptor, root_commit.bytes.clone())];
        let mut parent = m::CommitId::ROOT;
        let mut historical_head = None;
        let mut historical_trees = Vec::new();
        for number in 0..history {
            let mut tree = current_tree.clone();
            tree.entries.push(m::TreeEntry {
                name: format!("history-{number:02}.cc"),
                value: source_value.clone(),
            });
            tree.entries
                .sort_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
            let tree = m::encode(&m::Object::Tree(tree), m::Limits::default()).unwrap();
            store
                .put("example", tree.descriptor, &tree.bytes, 0)
                .unwrap();
            historical_trees.push(tree.descriptor.key);
            let mut commit = template.clone();
            commit.parents = vec![parent];
            commit.predecessors.clear();
            commit.root_tree.terms = vec![m::TreeId(tree.descriptor.key.id.0)];
            commit.change_id = m::ChangeId(
                blake3::hash(format!("mounted-history-{number}").as_bytes()).as_bytes()[..16]
                    .try_into()
                    .unwrap(),
            );
            commit.description = format!("native history {number}");
            let commit = m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap();
            store
                .put("example", commit.descriptor, &commit.bytes, 0)
                .unwrap();
            index_commits.push((commit.descriptor, commit.bytes.clone()));
            parent = m::CommitId(commit.descriptor.key.id.0);
            historical_head
                .get_or_insert_with(|| blake3::Hash::from(parent.0).to_hex().to_string());
        }
        let mut current = template;
        current.parents = vec![parent];
        let current = m::encode(&m::Object::Commit(current), m::Limits::default()).unwrap();
        store
            .put("example", current.descriptor, &current.bytes, 0)
            .unwrap();
        index_commits.push((current.descriptor, current.bytes.clone()));
        let head_id = m::CommitId(current.descriptor.key.id.0);
        let head = blake3::Hash::from(head_id.0).to_hex().to_string();
        store
            .publish(
                "example",
                &tinytree_store::PublishRequest {
                    idempotency_key: "mounted-seed".into(),
                    reference_name: "main".into(),
                    expected_generation: 0,
                    new_commit: Some(head_id),
                    staging_leases: vec![],
                },
                0,
            )
            .unwrap();
        let source = seed
            .manifest
            .files
            .iter()
            .find(|file| file.path == "hello.cc")
            .unwrap();
        let source_file = tinytree_model::ObjectKey {
            kind: tinytree_model::Kind::File,
            id: tinytree_model::ObjectId(*blake3::Hash::from_hex(&source.id).unwrap().as_bytes()),
        };
        let source_bytes = seed
            .objects()
            .iter()
            .find(|object| object.descriptor.key == source_file)
            .unwrap()
            .bytes
            .clone();
        let setup_ns = setup_started.elapsed().as_nanos();
        let indexing_started = Instant::now();
        let index =
            tinytree_index::Index::open(directory.path().join("index"), &Default::default())
                .unwrap();
        for (descriptor, bytes) in index_commits {
            index.ingest("example", descriptor, &bytes).unwrap();
        }
        let index_ready_ns = indexing_started.elapsed().as_nanos();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let configuration = tinytree_server::ServiceConfig::default();
        let request_bytes = configuration
            .max_request_bytes
            .max(configuration.max_frame_bytes + 1024);
        let response_bytes = configuration.max_metadata_bytes + 4096;
        let query_response_bytes = configuration.query.max_response_bytes;
        let service = runtime.block_on(async {
            tinytree_server::RepositoryService::new(
                store.clone(),
                configuration,
                vec!["example".into()],
                vec![
                    tinytree_server::Principal::new(
                        "mounted-writer",
                        REMOTE_TOKEN,
                        vec!["example".into()],
                        true,
                    )
                    .unwrap()
                    .with_retention(true),
                ],
            )
            .unwrap()
            .with_query_index(index)
            .unwrap()
        });
        let reads = Arc::new(AtomicUsize::new(0));
        let counter = reads.clone();
        let metadata = Arc::new(MetadataReads::default());
        let observed = CountMetadata {
            inner: service.clone(),
            reads: metadata.clone(),
        };
        let query = CountQueryMetadata {
            inner: service.query_service(),
            reads: metadata.clone(),
        };
        let query_counter = metadata.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let (endpoint, task) = runtime.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .layer(service.deadline_layer())
                    .layer(tower::layer::layer_fn(move |inner| CountFileReads {
                        inner,
                        reads: counter.clone(),
                        metadata: query_counter.clone(),
                    }))
                    .concurrency_limit_per_connection(service.maximum_inflight())
                    .add_service(
                        RepositoryServer::new(observed)
                            .accept_compressed(CompressionEncoding::Zstd)
                            .send_compressed(CompressionEncoding::Zstd)
                            .max_decoding_message_size(request_bytes)
                            .max_encoding_message_size(response_bytes),
                    )
                    .add_service(
                        RepositoryQueryServer::new(query)
                            .accept_compressed(CompressionEncoding::Zstd)
                            .send_compressed(CompressionEncoding::Zstd)
                            .max_decoding_message_size(request_bytes)
                            .max_encoding_message_size(query_response_bytes),
                    )
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener).map(|accepted| {
                            accepted.and_then(|stream| {
                                stream.set_nodelay(true)?;
                                Ok(stream)
                            })
                        }),
                        async {
                            let _ = stopped.await;
                        },
                    )
                    .await
                    .unwrap();
            });
            (endpoint, task)
        });
        Self {
            _directory: directory,
            runtime: Some(runtime),
            task: Some(task),
            stop: Some(stop),
            store,
            endpoint,
            head,
            historical_head: historical_head.unwrap(),
            historical_trees,
            source_file,
            source_bytes,
            reads,
            metadata,
            setup_ns,
            index_ready_ns,
        }
    }

    fn file_reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    fn historical_tree_reads(&self) -> usize {
        let requested = self.metadata.keys.lock().unwrap();
        self.historical_trees
            .iter()
            .filter(|key| requested.contains(&(key.kind as i32, key.id.0)))
            .count()
    }

    fn public(&self) -> tinytree_store::Reference {
        self.store
            .references("example", &["main".into()])
            .unwrap()
            .remove(0)
    }
}

impl Drop for RemoteServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let runtime = self.runtime.as_ref().unwrap();
            runtime.block_on(async move {
                let abort = task.abort_handle();
                if tokio::time::timeout(Duration::from_secs(5), task)
                    .await
                    .is_err()
                {
                    abort.abort();
                }
            });
        }
        self.runtime
            .take()
            .unwrap()
            .shutdown_timeout(Duration::from_secs(5));
    }
}

fn remote_observation(binding: &native_workspace_admin::Binding) -> (Vec<u8>, toml::Value) {
    let bytes = fs::read(binding.working_copy.join("native-remote.toml")).unwrap();
    let value = toml::from_str(std::str::from_utf8(&bytes).unwrap()).unwrap();
    (bytes, value)
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn shipped_remote_cli_clones_lazily_and_publishes_between_isolated_mounts() {
    // Declaration order makes the daemon/mount owner clean up before upstream.
    // Its runtime keeps serving while this test waits for real CLI subprocesses.
    let upstream = RemoteServer::start();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.start();
    let base = fixture.base().to_owned();
    let first = base.join("writer");
    let second = base.join("reader");
    let cloning = Instant::now();
    fixture.clone_remote(&first);
    fixture.clone_remote(&second);
    let clone_pair_ns = cloning.elapsed().as_nanos();
    let first_binding = fixture.binding(&first);
    let second_binding = fixture.binding(&second);
    assert_ne!(first_binding.repo, second_binding.repo);
    assert_ne!(first_binding.working_copy, second_binding.working_copy);
    assert_eq!(fixture.revision(&first, "main"), upstream.head);
    assert_eq!(fixture.revision(&second, "main"), upstream.head);
    assert_eq!(
        fs::metadata(first.join("hello.cc")).unwrap().len(),
        upstream.source_bytes.len() as u64
    );
    assert_eq!(
        fs::metadata(second.join("assets/unused-00000000.bin"))
            .unwrap()
            .len(),
        64
    );
    assert_eq!(
        upstream.file_reads(),
        0,
        "clone/stat must load metadata, not FILE payloads"
    );
    let clone_metadata_bytes = upstream.metadata.canonical_bytes.load(Ordering::SeqCst);
    eprintln!(
        "{{\"schema\":1,\"benchmark\":\"native_history_admission\",\"history_commits\":8,\"clone_pair_ns\":{clone_pair_ns},\"metadata_rpcs\":{},\"canonical_metadata_bytes\":{clone_metadata_bytes},\"historical_trees_requested\":{},\"file_rpcs\":{}}}",
        upstream.metadata.calls.load(Ordering::SeqCst),
        upstream.historical_tree_reads(),
        upstream.file_reads(),
    );
    assert!(clone_metadata_bytes > 0);
    assert_eq!(
        upstream.historical_tree_reads(),
        0,
        "clone/index/stat must not fetch unused ancestor trees"
    );
    let historical_listing =
        fixture.success(&first, &["file", "list", "-r", &upstream.historical_head]);
    assert!(
        historical_listing
            .lines()
            .any(|path| path == "history-00.cc")
    );
    assert!(!historical_listing.contains("history-01.cc"));
    assert_eq!(
        upstream.historical_tree_reads(),
        1,
        "listing one old revision fetches its tree, not every ancestor tree"
    );
    assert!(upstream.metadata.canonical_bytes.load(Ordering::SeqCst) > clone_metadata_bytes);
    assert_eq!(
        upstream.file_reads(),
        0,
        "historical file listing needs metadata, not historical file contents"
    );
    assert_eq!(
        fs::read(first.join("hello.cc")).unwrap(),
        upstream.source_bytes
    );
    assert_eq!(
        upstream.file_reads(),
        1,
        "one cold source read must issue one real ReadFile RPC"
    );
    assert_eq!(
        fs::read(first.join("hello.cc")).unwrap(),
        upstream.source_bytes
    );
    assert_eq!(
        fs::read(second.join("hello.cc")).unwrap(),
        upstream.source_bytes
    );
    assert_eq!(
        upstream.file_reads(),
        1,
        "warm reads share the daemon CAS across mounts"
    );

    let (second_observation, second_state) = remote_observation(&second_binding);
    assert_eq!(
        second_state["observations"][0]["generation"].as_integer(),
        Some(1)
    );
    fs::write(
        first.join("agent.txt"),
        "published from lazy source mount\n",
    )
    .unwrap();
    fixture.success(&first, &["status"]);
    fixture.success(&first, &["describe", "-m", "native mounted publication"]);
    fixture.success(&first, &["bookmark", "set", "main", "-r", "@"]);
    let published = fixture.revision(&first, "@");
    fixture.success(
        &first,
        &[
            "tinytree",
            "push",
            "--reference",
            "main",
            "--bookmark",
            "main",
        ],
    );
    let public = upstream.public();
    assert_eq!(public.generation, 2);
    assert_eq!(
        blake3::Hash::from(public.commit.unwrap().key.id.0)
            .to_hex()
            .as_str(),
        published
    );
    assert_eq!(
        remote_observation(&second_binding).0,
        second_observation,
        "another workspace's publication must not advance this workspace's observation"
    );
    assert!(!second.join("agent.txt").exists());
    fixture.success(
        &second,
        &[
            "tinytree",
            "fetch",
            "--reference",
            "main",
            "--bookmark",
            "main",
        ],
    );
    assert_eq!(fixture.revision(&second, "main"), published);
    assert_eq!(
        upstream.file_reads(),
        1,
        "metadata fetch must not hydrate unrelated source files"
    );
    let (observed, state) = remote_observation(&second_binding);
    assert_eq!(state["observations"][0]["generation"].as_integer(), Some(2));
    fixture.success(&second, &["edit", "main"]);
    assert_eq!(
        fs::read(second.join("agent.txt")).unwrap(),
        b"published from lazy source mount\n"
    );
    fixture.success(&second, &["undo"]);
    assert_eq!(
        remote_observation(&second_binding).0,
        observed,
        "jj undo must not rewind the external remote observation"
    );
    fixture.success(&second, &["edit", &published]);

    fixture.success(&base, &["tinytree", "unmount", first.to_str().unwrap()]);
    fixture.success(&base, &["tinytree", "unmount", second.to_str().unwrap()]);
    fixture.stop_daemon();
    // Cache fills are disposable CAS data, not authoritative repository FILEs.
    let local = tinytree_store::Store::open_existing(
        base.join("daemon/native/example/native-objects"),
        jj_native_backend::Config::default().store,
    )
    .unwrap();
    assert!(
        local
            .descriptor("local", upstream.source_file)
            .unwrap()
            .is_none(),
        "reading a remote source file must not install authoritative residency"
    );
    drop(local);
    fixture.start();
    fixture.success(&base, &["tinytree", "resume", second.to_str().unwrap()]);
    assert_fuse_mounted(&second);
    assert_eq!(fixture.binding(&second), second_binding);
    assert_eq!(remote_observation(&second_binding).0, observed);
    assert_eq!(fixture.revision(&second, "main"), published);
    let reads = upstream.file_reads();
    assert_eq!(
        fs::read(second.join("hello.cc")).unwrap(),
        upstream.source_bytes
    );
    assert_eq!(
        upstream.file_reads(),
        reads,
        "warm CAS survives daemon restart"
    );
    assert_eq!(
        fs::read(second.join("agent.txt")).unwrap(),
        b"published from lazy source mount\n"
    );
    fixture.success(&base, &["tinytree", "unmount", second.to_str().unwrap()]);
    fixture.stop_daemon();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn federated_index_production_factory_preserves_operations_and_drains_cli_queries() {
    let upstream = RemoteServer::start();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("federated");
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("federated");
    fixture.clone_remote(&root);
    let binding = fixture.binding(&root);
    let index = binding.repo.join("index");
    assert_eq!(
        fs::read_to_string(index.join("type")).unwrap(),
        jj_native_index::NAME
    );
    assert_eq!(fixture.revision(&root, "main"), upstream.head);
    assert!(!fixture.success(&root, &["log", "-n", "40"]).is_empty());
    // More than the server's active-scope allowance: each real process must
    // drain logical cleanup, not defer every close until its absolute TTL.
    for command in 0..40 {
        let args: &[&str] = if command % 2 == 0 {
            &["status"]
        } else {
            &["log", "-n", "40"]
        };
        let output = fixture.run(&root, args);
        assert!(
            output.status.success(),
            "CLI iteration {command} {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !String::from_utf8_lossy(&output.stdout).contains("<Error"),
            "CLI template iteration {command} {args:?}: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains("cleanup remains pending"),
            "CLI cleanup iteration {command} {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !String::from_utf8_lossy(&output.stderr)
                .contains("native command drain remains pending"),
            "command fence did not drain after iteration {command}"
        );
        let status = fixture.control(Request::Status).unwrap();
        assert_eq!(
            status["native"]["commands"]["active_commands"], 0,
            "command fence leaked after iteration {command}"
        );
        assert_eq!(status["native"]["commands"]["collection_enabled"], false);
    }
    let second = base.join("federated-reader");
    fixture.clone_remote(&second);
    let previous = fixture.revision(&root, "@");
    fixture.success(&root, &["new", "-m", "first private draft"]);
    let draft = fixture.revision(&root, "@");
    assert_ne!(draft, previous);
    fixture.success(&root, &["describe", "-m", "amended private draft"]);
    let amended = fixture.revision(&root, "@");
    assert_ne!(amended, draft);
    fixture.success(&root, &["undo"]);
    assert_eq!(fixture.revision(&root, "@"), draft);
    let operation = fixture.success(&root, &["op", "log", "--no-graph", "-n", "1", "-T", "id"]);
    let operation = operation.trim().to_owned();
    assert_eq!(operation.len(), 128);
    fixture.success(&root, &["--at-op", &operation, "log", "-n", "40"]);
    fixture.success(&root, &["bookmark", "set", "main", "-r", "@"]);
    fixture.success(
        &root,
        &[
            "tinytree",
            "push",
            "--reference",
            "main",
            "--bookmark",
            "main",
        ],
    );
    fixture.success(
        &second,
        &[
            "tinytree",
            "fetch",
            "--reference",
            "main",
            "--bookmark",
            "main",
        ],
    );
    assert_eq!(fixture.revision(&second, "main"), draft);
    // The writer now observes its own formerly private draft as a public root;
    // the new checkpoint must not resend it as an overlapping private overlay.
    fixture.success(
        &root,
        &[
            "tinytree",
            "fetch",
            "--reference",
            "main",
            "--bookmark",
            "main",
        ],
    );
    assert_eq!(fixture.revision(&root, "main"), draft);
    fixture.success(&base, &["tinytree", "unmount", second.to_str().unwrap()]);
    assert_eq!(
        upstream.file_reads(),
        0,
        "clone/log/draft metadata must not fetch source FILE bodies"
    );
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();

    // Creation policy does not rotate the live security identity or replace an
    // existing repository's recorded native index with the legacy index.
    fixture.index_mode("legacy");
    fixture.start();
    fixture.success(&base, &["tinytree", "resume", root.to_str().unwrap()]);
    fixture.success(&root, &["status"]);
    assert_eq!(fixture.revision(&root, "@"), draft);
    assert_eq!(
        fs::read_to_string(index.join("type")).unwrap(),
        jj_native_index::NAME
    );

    // Missing operation associations fail closed and are never rebuilt from
    // fetched ancestry or silently replaced with a default index.
    let association = index.join("operations").join(&operation);
    let saved = base.join("saved-operation-association");
    fs::rename(&association, &saved).unwrap();
    let output = fixture.run(&root, &["--at-op", &operation, "log", "-n", "40"]);
    assert!(!output.status.success());
    assert!(!association.exists());
    assert_eq!(
        fs::read_to_string(index.join("type")).unwrap(),
        jj_native_index::NAME
    );
    fs::rename(saved, &association).unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();

    let mut config: toml::Value =
        toml::from_str(&fs::read_to_string(&fixture.daemon_config).unwrap()).unwrap();
    // A changed provider configuration changes the live fingerprint. Keep the
    // remote authorization binding intact so the daemon itself can reopen.
    config["native"]["repositories"]["example"]
        .as_table_mut()
        .unwrap()
        .insert(
            "max_staging_uploads".into(),
            toml::Value::Integer(
                jj_native_backend::Config::default()
                    .store
                    .max_staging_uploads as i64
                    + 1,
            ),
        );
    fs::write(&fixture.daemon_config, toml::to_string(&config).unwrap()).unwrap();
    fixture.start();
    let output = fixture.run(&base, &["tinytree", "resume", root.to_str().unwrap()]);
    assert!(
        !output.status.success(),
        "changed live provider identity must not load the old index"
    );
    assert_eq!(
        fs::read_to_string(index.join("type")).unwrap(),
        jj_native_index::NAME
    );
    assert!(index.join("manifest").is_file());
}

fn write_synced(path: &Path, bytes: &[u8]) {
    let mut file = fs::File::create(path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn files_in(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            assert!(path.is_file());
            (path.clone(), fs::read(path).unwrap())
        })
        .collect()
}

fn recovery_report(output: Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "native recovery failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["ok"], true);
    result["report"].clone()
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn killed_daemon_mounts_require_exact_private_recovery_before_resume() {
    let mut fixture = Fixture::new();
    fixture.start();
    let base = fixture.base().to_owned();
    let first = base.join("first");
    let second = base.join("second");
    fixture.initialize(&first);
    fixture.initialize(&second);
    let bindings = [fixture.binding(&first), fixture.binding(&second)];
    let mut revisions = Vec::new();
    for (index, root) in [&first, &second].into_iter().enumerate() {
        let bootstrap = fixture.revision(root, "@");
        write_synced(
            &root.join("canonical.txt"),
            format!("canonical {index}\n").as_bytes(),
        );
        fixture.success(root, &["describe", "-m", "canonical before crash"]);
        let revision = fixture.revision(root, "@");
        assert_ne!(revision, bootstrap);
        assert_ne!(revision, "0".repeat(64));
        revisions.push(revision);
        // Close and fsync the handle, but deliberately issue no jj snapshot.
        write_synced(
            &root.join("dirty.txt"),
            format!("unsnapshotted {index}\n").as_bytes(),
        );
    }
    let first_id = bindings[0].id();
    let second_id = bindings[1].id();
    let first_record = base
        .join("daemon/native-workspaces")
        .join(&first_id)
        .join("workspace.json");
    let original_record = fs::read(&first_record).unwrap();
    let original: serde_json::Value = serde_json::from_slice(&original_record).unwrap();
    assert!(original["native_mount"]["kernel"].is_object());
    assert!(original["native_mount"]["context"].is_object());
    assert!(original["native_mount"]["namespace_identity"].is_string());
    let mut admin = files_in(&fixture.registry.join("client-intents"));
    for binding in &bindings {
        for path in [
            binding.admin.join("initialization.json"),
            binding.working_copy.join("native-vfs.json"),
        ] {
            admin.insert(path.clone(), fs::read(path).unwrap());
        }
        admin.extend(files_in(&binding.repo.join("op_heads/heads")));
    }
    let live = fixture.workspace(&["recover", "--native-workspace", &first_id]);
    assert!(
        !live.status.success(),
        "a live owned provider must not be recovered"
    );
    assert_fuse_mounted(&first);
    assert_fuse_mounted(&second);
    fixture.kill_daemon();
    for root in [&first, &second] {
        assert_fuse_mounted(root);
        assert_eq!(
            fs::metadata(root.join("dirty.txt"))
                .unwrap_err()
                .raw_os_error(),
            Some(107),
            "the kernel must report ENOTCONN after its exact owner is killed"
        );
    }

    let mut missing = original.clone();
    missing.as_object_mut().unwrap().remove("native_mount");
    write_synced(&first_record, &serde_json::to_vec(&missing).unwrap());
    let refused = fixture.workspace(&["recover", "--native-workspace", &first_id]);
    assert!(
        !refused.status.success(),
        "a missing proof must not authorize detach"
    );
    assert_fuse_mounted(&first);
    assert_fuse_mounted(&second);
    let mut wrong = original;
    let id = wrong["native_mount"]["kernel"]["id"].as_u64().unwrap();
    wrong["native_mount"]["kernel"]["id"] = serde_json::Value::from(id.checked_add(1).unwrap());
    write_synced(&first_record, &serde_json::to_vec(&wrong).unwrap());
    let refused = fixture.workspace(&["recover", "--native-workspace", &first_id]);
    assert!(
        !refused.status.success(),
        "a mismatched proof must not authorize detach"
    );
    assert_fuse_mounted(&first);
    assert_fuse_mounted(&second);
    write_synced(&first_record, &original_record);

    let recovered =
        recovery_report(fixture.workspace(&["recover", "--native-workspace", &first_id]));
    assert_eq!(recovered["workspace_id"], first_id);
    assert_eq!(recovered["detached"], true);
    assert!(!fuse_mounted(&first));
    assert_fuse_mounted(&second);
    assert_eq!(
        fs::metadata(second.join("dirty.txt"))
            .unwrap_err()
            .raw_os_error(),
        Some(107),
        "recovering one identity must leave the other disconnected mount untouched"
    );
    let repeated =
        recovery_report(fixture.workspace(&["recover", "--native-workspace", &first_id]));
    assert_eq!(repeated["detached"], false);

    // Keep the second disconnected mount while starting its replacement owner.
    // Ordinary unmount must not misclassify an unowned kernel mount as stopped.
    fixture.start();
    assert!(!fuse_mounted(&first));
    assert_fuse_mounted(&second);
    assert!(
        fixture
            .control(Request::NativeUnmount {
                workspace_id: second_id.clone(),
            })
            .is_err(),
        "an unowned disconnected mount requires explicit recovery, not unmount"
    );
    assert_fuse_mounted(&second);
    let status = fixture.control(Request::Status).unwrap();
    let record = status["native"]["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["workspace_id"] == second_id)
        .unwrap();
    assert_eq!(record["state"], "recovery_required");

    // This invocation now routes through the running daemon. Its failed startup
    // restoration must have retained the exact proof needed for recovery.
    let recovered =
        recovery_report(fixture.workspace(&["recover", "--native-workspace", &second_id]));
    assert_eq!(recovered["workspace_id"], second_id);
    assert_eq!(recovered["detached"], true);
    assert!(!fuse_mounted(&second));
    for (path, bytes) in &admin {
        assert_eq!(&fs::read(path).unwrap(), bytes);
    }

    // Explicit recovery leaves both stopped until the user's resume command.
    assert!(!fuse_mounted(&first));
    assert!(!fuse_mounted(&second));
    for (index, root) in [&first, &second].into_iter().enumerate() {
        fixture.success(&base, &["tinytree", "resume", root.to_str().unwrap()]);
        assert_fuse_mounted(root);
        assert_eq!(fixture.binding(root), bindings[index]);
        assert_eq!(
            fixture
                .success(
                    root,
                    &[
                        "--ignore-working-copy",
                        "log",
                        "--no-graph",
                        "-r",
                        "@",
                        "-T",
                        "commit_id"
                    ]
                )
                .trim(),
            revisions[index]
        );
        assert_eq!(
            fs::read(root.join("canonical.txt")).unwrap(),
            format!("canonical {index}\n").as_bytes()
        );
        assert_eq!(
            fs::read(root.join("dirty.txt")).unwrap(),
            format!("unsnapshotted {index}\n").as_bytes()
        );
    }
    for (path, bytes) in &admin {
        assert_eq!(&fs::read(path).unwrap(), bytes);
    }
    for root in [&first, &second] {
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    }
    fixture.stop_daemon();
}

#[derive(Clone, Copy, Default, serde::Serialize)]
struct StartupCounters {
    metadata_batch_rpcs: usize,
    legacy_graph_rpcs: usize,
    query_rpcs: usize,
    canonical_metadata_bytes: usize,
    canonical_commit_bytes: usize,
    canonical_tree_bytes: usize,
    canonical_other_metadata_bytes: usize,
    distinct_batch_commit_keys_requested: usize,
    distinct_batch_tree_keys_requested: usize,
    historical_trees_requested: usize,
    file_rpcs: usize,
}
impl StartupCounters {
    fn subtract(self, before: Self) -> Self {
        Self {
            metadata_batch_rpcs: self.metadata_batch_rpcs - before.metadata_batch_rpcs,
            legacy_graph_rpcs: self.legacy_graph_rpcs - before.legacy_graph_rpcs,
            query_rpcs: self.query_rpcs - before.query_rpcs,
            canonical_metadata_bytes: self.canonical_metadata_bytes
                - before.canonical_metadata_bytes,
            canonical_commit_bytes: self.canonical_commit_bytes - before.canonical_commit_bytes,
            canonical_tree_bytes: self.canonical_tree_bytes - before.canonical_tree_bytes,
            canonical_other_metadata_bytes: self.canonical_other_metadata_bytes
                - before.canonical_other_metadata_bytes,
            distinct_batch_commit_keys_requested: self.distinct_batch_commit_keys_requested
                - before.distinct_batch_commit_keys_requested,
            distinct_batch_tree_keys_requested: self.distinct_batch_tree_keys_requested
                - before.distinct_batch_tree_keys_requested,
            historical_trees_requested: self.historical_trees_requested
                - before.historical_trees_requested,
            file_rpcs: self.file_rpcs - before.file_rpcs,
        }
    }
}
impl RemoteServer {
    fn startup_counters(&self) -> StartupCounters {
        let (commits, trees) = {
            let keys = self.metadata.keys.lock().unwrap();
            (
                keys.iter()
                    .filter(|(kind, _)| *kind == p::ObjectKind::Commit as i32)
                    .count(),
                keys.iter()
                    .filter(|(kind, _)| *kind == p::ObjectKind::Tree as i32)
                    .count(),
            )
        };
        let commit_bytes = self.metadata.commit_bytes.load(Ordering::SeqCst)
            + self.metadata.graph_commit_bytes.load(Ordering::SeqCst)
            + self.metadata.query_commit_bytes.load(Ordering::SeqCst);
        let tree_bytes = self.metadata.tree_bytes.load(Ordering::SeqCst);
        let other_bytes = self.metadata.other_bytes.load(Ordering::SeqCst);
        StartupCounters {
            metadata_batch_rpcs: self.metadata.calls.load(Ordering::SeqCst),
            legacy_graph_rpcs: self.metadata.graph_calls.load(Ordering::SeqCst),
            query_rpcs: self.metadata.query_calls.load(Ordering::SeqCst),
            canonical_metadata_bytes: commit_bytes + tree_bytes + other_bytes,
            canonical_commit_bytes: commit_bytes,
            canonical_tree_bytes: tree_bytes,
            canonical_other_metadata_bytes: other_bytes,
            distinct_batch_commit_keys_requested: commits,
            distinct_batch_tree_keys_requested: trees,
            historical_trees_requested: self.historical_tree_reads(),
            file_rpcs: self.file_reads(),
        }
    }
}
struct StartupTrialFailure<'a> {
    upstream: &'a RemoteServer,
    history: usize,
    trial: usize,
    index_mode: &'static str,
    phase: &'static str,
    started: Instant,
    before: StartupCounters,
    complete: bool,
}
impl<'a> StartupTrialFailure<'a> {
    fn new(
        upstream: &'a RemoteServer,
        history: usize,
        trial: usize,
        index_mode: &'static str,
    ) -> Self {
        Self {
            upstream,
            history,
            trial,
            index_mode,
            phase: "fixture_configuration",
            started: Instant::now(),
            before: upstream.startup_counters(),
            complete: false,
        }
    }
    fn begin(&mut self, phase: &'static str) {
        self.phase = phase;
        self.before = self.upstream.startup_counters();
        self.started = Instant::now();
    }
}
impl Drop for StartupTrialFailure<'_> {
    fn drop(&mut self) {
        if !self.complete && std::thread::panicking() {
            // No panic text, child stderr, arguments, paths or endpoints enter
            // this record. The original panic and fixture recovery still run.
            eprintln!(
                "{}",
                serde_json::json!({
                    "schema":1,"benchmark":"native_mounted_startup","measurement":false,
                    "status":"failed_or_timeout","historical_commits":self.history,"trial":self.trial,"index_mode":self.index_mode,
                    "phase":self.phase,"phase_elapsed_ns":self.started.elapsed().as_nanos(),
                    "partial_phase_counters":self.upstream.startup_counters().subtract(self.before),
                    "cli_timeout_seconds":30,"daemon_readiness_timeout_seconds":20,
                    "failure_bound_not_success_latency":true
                })
            );
        }
    }
}
fn mounted_command_retention_cumulative(fixture: &Fixture) -> serde_json::Value {
    let status = fixture
        .control(Request::Status)
        .expect("command retention diagnostics unavailable");
    let stats = &status["native"]["commands"];
    assert_eq!(
        stats["active_commands"], 0,
        "timed commands returned with command fences still pending"
    );
    assert_eq!(
        stats["collection_enabled"], false,
        "checkpoint A must not enable collection"
    );
    let mut output = serde_json::Map::new();
    for field in [
        "active_commands",
        "registered_workspaces",
        "dirty_transitions",
        "dirty_transition_ns",
        "physical_drains",
        "physical_drain_ns",
        "retention_transactions",
        "retention_encoded_write_bytes",
    ] {
        output.insert(
            field.into(),
            stats[field]
                .as_u64()
                .expect("missing numeric command retention metric")
                .into(),
        );
    }
    output.insert("collection_enabled".into(), false.into());
    output.insert(
        "counter_window".into(),
        "daemon_start_through_four_timed_commands_before_cleanup".into(),
    );
    output.insert(
        "timing_attribution".into(),
        "cumulative_only_not_per_phase".into(),
    );
    output.insert(
        "control_snapshot_outside_measured_phases".into(),
        true.into(),
    );
    output.insert(
        "dirty_transition_includes_registry_queueing".into(),
        true.into(),
    );
    output.insert(
        "physical_drain_includes_accepted_job_wait".into(),
        true.into(),
    );
    serde_json::Value::Object(output)
}
fn mounted_startup_phase(
    tracker: &mut StartupTrialFailure<'_>,
    phase: &'static str,
    action: impl FnOnce(),
) -> serde_json::Value {
    tracker.begin(phase);
    action();
    let elapsed_ns = tracker.started.elapsed().as_nanos();
    let counters = tracker.upstream.startup_counters().subtract(tracker.before);
    serde_json::json!({"elapsed_ns":elapsed_ns,"counters":counters,"file_payload_free":counters.file_rpcs==0})
}

#[test]
#[ignore = "manual actual FUSE startup benchmark; requires TINYTREE_MOUNTED_STARTUP_BENCH=1"]
fn native_mounted_startup_benchmark_ladder() {
    // mounted_tests runs all ignored native cases. An explicit gate prevents an
    // ordinary acceptance run from unknowingly executing nine expensive trials.
    if std::env::var("TINYTREE_MOUNTED_STARTUP_BENCH").as_deref() != Ok("1") {
        eprintln!(
            "{}",
            serde_json::json!({"schema":1,"benchmark":"native_mounted_startup","measurement":false,"status":"skipped","reason":"requires explicit TINYTREE_MOUNTED_STARTUP_BENCH=1 and usable Linux FUSE"})
        );
        return;
    }
    let index_mode = match std::env::var("TINYTREE_MOUNTED_STARTUP_INDEX_MODE") {
        Ok(value) if value == "legacy" => "legacy",
        Ok(value) if value == "federated" => "federated",
        Err(std::env::VarError::NotPresent) => "legacy",
        _ => panic!("TINYTREE_MOUNTED_STARTUP_INDEX_MODE must be legacy or federated"),
    };
    let expected_index = match index_mode {
        "legacy" => jj_lib::default_index::DefaultIndexStore::NAME,
        "federated" => jj_native_index::NAME,
        _ => unreachable!(),
    };
    for history in [8usize, 128, 1024] {
        for trial in 1..=3 {
            // Fresh upstream/index/daemon/cache/mount state in every trial.
            // Declare the kernel/daemon fixture after upstream so it tears down
            // all real mounted workspaces before the serving runtime exits.
            let upstream_started = Instant::now();
            let upstream = match std::panic::catch_unwind(|| {
                RemoteServer::start_with_history(history)
            }) {
                Ok(upstream) => upstream,
                Err(panic) => {
                    eprintln!(
                        "{}",
                        serde_json::json!({"schema":1,"benchmark":"native_mounted_startup","measurement":false,"status":"failed_or_timeout","historical_commits":history,"trial":trial,"index_mode":index_mode,"phase":"upstream_setup","phase_elapsed_ns":upstream_started.elapsed().as_nanos(),"partial_phase_counters":null,"counters_available":false,"failure_bound_not_success_latency":true})
                    );
                    std::panic::resume_unwind(panic);
                }
            };
            let upstream_total_ns = upstream_started.elapsed().as_nanos();
            // Declare the owner before the failure tracker, so its actual
            // FUSE/daemon cleanup runs AFTER the sanitized failure record.
            let mut fixture_owner = std::cell::OnceCell::new();
            let mut failure = StartupTrialFailure::new(&upstream, history, trial, index_mode);
            let fixture_started = Instant::now();
            assert!(fixture_owner.set(Fixture::new()).is_ok());
            let fixture = fixture_owner.get_mut().unwrap();
            fixture.configure_remote(&upstream.endpoint);
            fixture.index_mode(index_mode);
            let fixture_setup_ns = fixture_started.elapsed().as_nanos();
            let daemon = mounted_startup_phase(&mut failure, "daemon_start", || fixture.start());
            let base = fixture.base().to_owned();
            let first = base.join("agent-one");
            let second = base.join("agent-two");
            let cold_clone = mounted_startup_phase(&mut failure, "first_cold_clone", || {
                fixture.clone_remote(&first)
            });
            failure.begin("first_mount_validation");
            assert_fuse_mounted(&first);
            let first_binding = fixture.binding(&first);
            assert_eq!(
                fs::read_to_string(first_binding.repo.join("index/type")).unwrap(),
                expected_index,
                "requested benchmark index mode must actually be recorded"
            );
            let cold_log = mounted_startup_phase(&mut failure, "first_default_log_n40", || {
                let output = fixture.success(&first, &["log", "-n", "40"]);
                assert!(
                    !output.trim().is_empty(),
                    "default log must render a real native workspace"
                );
            });
            let warm_log = mounted_startup_phase(&mut failure, "repeated_warm_log_n40", || {
                let output = fixture.success(&first, &["log", "-n", "40"]);
                assert!(!output.trim().is_empty());
            });
            let shared_daemon_second_clone = mounted_startup_phase(
                &mut failure,
                "second_agent_clone_shared_daemon_cache",
                || fixture.clone_remote(&second),
            );
            failure.begin("second_mount_validation");
            assert_fuse_mounted(&second);
            let second_binding = fixture.binding(&second);
            assert_eq!(
                fs::read_to_string(second_binding.repo.join("index/type")).unwrap(),
                expected_index
            );
            assert_ne!(first_binding.repo, second_binding.repo);
            assert_ne!(first_binding.working_copy, second_binding.working_copy);
            let total = upstream.startup_counters();
            // Do not add status RPCs between measured phases: their untimed
            // delay could warm asynchronous prefetch before the next command.
            // One final snapshot observes counts, never waits for pending work.
            failure.begin("command_retention_diagnostics");
            let command_retention = mounted_command_retention_cumulative(fixture);
            failure.begin("cleanup_second_unmount");
            fixture.success(&base, &["tinytree", "unmount", second.to_str().unwrap()]);
            failure.begin("cleanup_first_unmount");
            fixture.success(&base, &["tinytree", "unmount", first.to_str().unwrap()]);
            failure.begin("cleanup_daemon_shutdown");
            fixture.stop_daemon();
            failure.complete = true;
            eprintln!(
                "{}",
                serde_json::json!({
                    "schema":1,"benchmark":"native_mounted_startup","measurement":true,"status":"completed",
                    "historical_commits":history,"reachable_stored_commits":history+1,
                    "index_ingested_commits":history+2,"trial":trial,"trials":3,"index_mode":index_mode,
                    "workspace_provider":"actual_linux_fuse","checkout":"lazy_no_full_checkout_substitute",
                    "source_setup_ns":upstream.setup_ns,"index_ready_ns":upstream.index_ready_ns,
                    "index_ready_before_daemon":true,"upstream_total_setup_ns":upstream_total_ns,
                    "fixture_configuration_ns":fixture_setup_ns,
                    "daemon_start":daemon,"first_cold_clone":cold_clone,
                    "first_default_log_n40":cold_log,"repeated_warm_log_n40":warm_log,
                    "second_agent_clone_shared_daemon_cache":shared_daemon_second_clone,
                    "total_command_boundary_counters":total,"file_payload_free":total.file_rpcs==0,
                    "command_retention_cumulative":command_retention,
                    "retention_component_timings_not_phase_attribution":true,
                    "byte_measure":"uncompressed_canonical_envelopes_not_network_wire_bytes",
                    "counters_are_command_boundary_snapshots":true,
                    "background_work_may_cross_phase_boundaries":true,
                    "cli_exit_poll_ms":10,"daemon_readiness_poll_ms":20,
                    "authenticated":true,"zstd":true,"tcp_nodelay":true,
                    "default_log_template_unchanged":true,"native_index_store_default_not_assumed":true
                })
            );
        }
    }
}
