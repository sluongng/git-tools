//! Shipped CLI acceptance against an actual Linux FUSE mount. Protocol fixtures
//! must not count as acceptance of lazy, externally administered workspaces.
#![cfg(target_os = "linux")]
#![cfg_attr(feature = "native-gc-benchmark", recursion_limit = "256")]

use futures::StreamExt;
use jj_lib::object_id::ObjectId as _;
use p::repository_server::{Repository, RepositoryServer};
use q::repository_query_server::RepositoryQuery;
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
        atomic::{AtomicBool, AtomicUsize, Ordering},
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

struct RunningNativeCommand {
    child: Child,
    stdout: PathBuf,
    stderr: PathBuf,
    deadline: Instant,
}
impl RunningNativeCommand {
    fn finish(mut self) -> Output {
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < self.deadline,
                "parallel native command timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        Output {
            status,
            stdout: fs::read(&self.stdout).unwrap(),
            stderr: fs::read(&self.stderr).unwrap(),
        }
    }
}
impl Drop for RunningNativeCommand {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
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
                 [native.workspaces]\nregistry_path={}\nindex_mode='legacy'\n\
                 [native.workspaces.capture]\npolicy='disabled'\n\
                 [native.workspaces.upload]\npolicy='disabled'\n\
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

    fn automatic_policies(&mut self) {
        let mut config: toml::Value =
            toml::from_str(&fs::read_to_string(&self.daemon_config).unwrap()).unwrap();
        let policies = config["native"]["workspaces"].as_table_mut().unwrap();
        policies.remove("index_mode");
        policies.remove("capture");
        policies.remove("upload");
        fs::write(&self.daemon_config, toml::to_string(&config).unwrap()).unwrap();
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

    fn query_replay_capacity(&mut self, limit: usize) {
        let mut config: toml::Value =
            toml::from_str(&fs::read_to_string(&self.daemon_config).unwrap()).unwrap();
        config["native"]["repositories"]["example"]["remote"]["query"]["max_replays"] =
            i64::try_from(limit).unwrap().into();
        fs::write(&self.daemon_config, toml::to_string(&config).unwrap()).unwrap();
        workspace_daemon::config::Config::load(&self.daemon_config).unwrap();
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

    fn capture_policy(&mut self, policy: &str) {
        let mut config: toml::Value =
            toml::from_str(&fs::read_to_string(&self.daemon_config).unwrap()).unwrap();
        config["native"]["workspaces"]
            .as_table_mut()
            .unwrap()
            .insert(
                "capture".into(),
                toml::Value::Table(toml::map::Map::from_iter([(
                    "policy".into(),
                    policy.into(),
                )])),
            );
        fs::write(&self.daemon_config, toml::to_string(&config).unwrap()).unwrap();
        workspace_daemon::config::Config::load(&self.daemon_config).unwrap();
    }

    fn enable_local_gc(&mut self) {
        let mut config: toml::Value =
            toml::from_str(&fs::read_to_string(&self.daemon_config).unwrap()).unwrap();
        config["native"]["workspaces"]
            .as_table_mut()
            .unwrap()
            .insert(
                "gc".into(),
                toml::Value::Table(toml::map::Map::from_iter([
                    ("policy".into(), "local_complete".into()),
                    // Fixture-only: production retains the conservative grace.
                    ("grace_seconds".into(), 0.into()),
                ])),
            );
        fs::write(&self.daemon_config, toml::to_string(&config).unwrap()).unwrap();
        workspace_daemon::config::Config::load(&self.daemon_config).unwrap();
    }

    fn enable_capture_upload(&mut self) {
        let mut config: toml::Value =
            toml::from_str(&fs::read_to_string(&self.daemon_config).unwrap()).unwrap();
        config["native"]["workspaces"]
            .as_table_mut()
            .unwrap()
            .insert(
                "upload".into(),
                toml::Value::Table(toml::map::Map::from_iter([
                    ("policy".into(), "publish_retaining_roots".into()),
                    ("poll_ms".into(), 250.into()),
                ])),
            );
        fs::write(&self.daemon_config, toml::to_string(&config).unwrap()).unwrap();
        workspace_daemon::config::Config::load(&self.daemon_config).unwrap();
    }

    fn enable_capture_retirement(&mut self) {
        self.enable_capture_retirement_with_limit(2);
    }

    fn enable_capture_retirement_with_limit(&mut self, limit: usize) {
        self.capture_policy("after_command_best_effort");
        self.enable_capture_upload();
        let mut config: toml::Value =
            toml::from_str(&fs::read_to_string(&self.daemon_config).unwrap()).unwrap();
        config["native"]["workspaces"]["capture"]
            .as_table_mut()
            .unwrap()
            .insert("max_pending".into(), i64::try_from(limit).unwrap().into());
        config["native"]["workspaces"]["upload"]["policy"] = "publish_retiring_redundant".into();
        fs::write(&self.daemon_config, toml::to_string(&config).unwrap()).unwrap();
        workspace_daemon::config::Config::load(&self.daemon_config).unwrap();
    }

    fn spawn_daemon(&mut self) {
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
    }

    fn start(&mut self) {
        self.spawn_daemon();
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

    fn expect_provider_startup_refusal(&mut self) {
        self.spawn_daemon();
        let deadline = Instant::now() + Duration::from_secs(20);
        let status = loop {
            if self.daemon.as_mut().unwrap().try_wait().unwrap().is_some() {
                let status = self.daemon.as_mut().unwrap().wait().unwrap();
                // Clear ownership only after this exact original child is reaped.
                self.daemon.take();
                break status;
            }
            // A still-live child stays owned for the existing bounded Drop
            // cleanup and scratch retention; never kill a possible provider.
            assert!(Instant::now() < deadline, "provider startup refusal timed out");
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(!status.success(), "changed provider must be refused at startup");
        let log = fs::read_to_string(self.base().join("daemon.log")).unwrap();
        assert!(log.contains("native provider enrollment does not match the effective configuration"),
            "startup refusal must be the fixed provider-enrollment mismatch");
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
        self.run_with_watchdog(cwd, args, Duration::from_secs(30))
    }

    fn run_with_watchdog(&mut self, cwd: &Path, args: &[&str], watchdog: Duration) -> Output {
        let binary = self.cli.clone();
        let args: Vec<_> = ["--no-pager", "--color=never"]
            .into_iter()
            .chain(args.iter().copied())
            .collect();
        self.run_binary_with_watchdog(cwd, &binary, &args, false, watchdog)
    }

    fn spawn_cli(&mut self, cwd: &Path, args: &[&str]) -> RunningNativeCommand {
        self.command_number += 1;
        let stdout = self.base().join(format!("{}.out", self.command_number));
        let stderr = self.base().join(format!("{}.err", self.command_number));
        let mut command = Command::new(&self.cli);
        self.environment(&mut command);
        let child = command
            .args(["--no-pager", "--color=never"])
            .args(args)
            .current_dir(cwd)
            .env("PATH", &self.empty_path)
            .stdout(fs::File::create(&stdout).unwrap())
            .stderr(fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap();
        RunningNativeCommand {
            child,
            stdout,
            stderr,
            deadline: Instant::now() + Duration::from_secs(30),
        }
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
        self.run_binary_with_watchdog(cwd, binary, args, native_path, Duration::from_secs(30))
    }

    fn run_binary_with_watchdog(
        &mut self,
        cwd: &Path,
        binary: &Path,
        args: &[&str],
        native_path: bool,
        watchdog: Duration,
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
        let deadline = Instant::now() + watchdog;
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
        self.success_with_watchdog(cwd, args, Duration::from_secs(30))
    }

    fn success_with_watchdog(&mut self, cwd: &Path, args: &[&str], watchdog: Duration) -> String {
        let output = self.run_with_watchdog(cwd, args, watchdog);
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

    fn cloud(&mut self, cwd: &Path, args: &[&str]) -> String {
        let configuration = self.daemon_config.to_str().unwrap().to_owned();
        let mut command = vec![
            "tinytree",
            "cloud",
            "--daemon-config",
            &configuration,
            "--store",
            "example",
        ];
        command.extend_from_slice(args);
        self.success(cwd, &command)
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
        if self.directory.is_none() {
            return;
        }
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
    fixture.enable_local_gc();
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
    let status = fixture.control(Request::Status).unwrap();
    assert_eq!(status["native"]["commands"]["collection_enabled"], true);
    assert!(
        status["native"]["commands"]["local_gc"]["clean_checkpoints"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        status["native"]["commands"]["local_gc"]["automatic_collection"],
        false
    );

    // The old child is no longer an ancestor after describe. Its undo/index
    // history must nevertheless remain a complete root across explicit GC.
    fixture.success(&root, &["describe", "-m", "amended child retained by GC"]);
    let amended_child = fixture.revision(&root, "@");
    assert_ne!(amended_child, child);
    assert_eq!(native_gc_report(&mut fixture, false)["status"], "planned");
    assert_eq!(native_gc_report(&mut fixture, true)["status"], "collected");
    fixture.success(&root, &["undo"]);
    assert_eq!(fixture.revision(&root, "@"), child);
    fixture.success(&root, &["edit", &amended_child]);
    fixture.success(&root, &["edit", &child]);
    assert_eq!(
        fs::read(root.join("hello.txt")).unwrap(),
        b"second native version\n"
    );

    // Do not snapshot this last edit. Stopping and resuming must preserve
    // unsnapshotted bytes as well as the previous canonical operation state.
    fs::write(root.join("hello.txt"), "dirty across restart\n").unwrap();
    fs::write(
        second.join("independent.txt"),
        "second dirty across restart\n",
    )
    .unwrap();
    // Core's dirty bytes are outside the authoritative native Store sweep.
    // They need not block collection, but neither workspace may lose them.
    assert_eq!(native_gc_report(&mut fixture, false)["status"], "planned");
    assert_eq!(native_gc_report(&mut fixture, true)["status"], "collected");
    assert_eq!(
        fs::read(root.join("hello.txt")).unwrap(),
        b"dirty across restart\n"
    );
    assert_eq!(
        fs::read(second.join("independent.txt")).unwrap(),
        b"second dirty across restart\n"
    );
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
    let store_path = fixture
        .socket
        .parent()
        .unwrap()
        .join("native/example/native-objects");
    let orphan_bytes = b"unreachable offline GC fixture file\n";
    let orphan = m::describe_file(orphan_bytes);
    // Deliberate fault injection, not a supported editing path: the daemon is
    // stopped and this Store owns the exclusive lock throughout the append.
    {
        let store = tinytree_store::Store::open_existing(&store_path, Default::default()).unwrap();
        store.put("local", orphan, orphan_bytes, 0).unwrap();
    }
    let orphan_path = store_path
        .join("repos/local")
        .join((m::Kind::File as u8).to_string())
        .join(capture_identity(&orphan.key.id.0));
    assert_eq!(fs::read(&orphan_path).unwrap(), orphan_bytes);
    fixture.start();
    let unscoped = workspace_daemon::control::Client {
        socket: fixture.socket.clone(),
    }
    .call(Request::NativeGc {
        store: "example".into(),
        apply: true,
    });
    assert!(unscoped
        .unwrap_err()
        .to_string()
        .contains("configuration-scoped"));
    assert_eq!(fs::read(&orphan_path).unwrap(), orphan_bytes);
    let preview = native_gc_report(&mut fixture, false);
    assert_eq!(preview["status"], "planned");
    assert!(preview["would_delete_objects"].as_u64().unwrap() >= 1);
    assert!(preview["would_delete_logical_bytes"].as_u64().unwrap() >= orphan.size_bytes);
    assert_eq!(
        fs::read(&orphan_path).unwrap(),
        orphan_bytes,
        "preview cannot unlink a candidate"
    );
    let applied = native_gc_report(&mut fixture, true);
    assert_eq!(applied["status"], "collected");
    assert!(applied["deleted_objects"].as_u64().unwrap() >= 1);
    assert!(applied["unlinked_payloads"].as_u64().unwrap() >= 1);
    assert!(!orphan_path.exists());
    fixture.success(&base_dir, &["tinytree", "resume", root.to_str().unwrap()]);
    fixture.success(&base_dir, &["tinytree", "resume", second.to_str().unwrap()]);
    assert_eq!(fixture.binding(&second), second_binding);
    assert_eq!(
        fs::read(second.join("independent.txt")).unwrap(),
        b"second dirty across restart\n"
    );
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
    fixture.success(&second, &["status"]);
    assert_eq!(
        fixture.success(&second, &["file", "show", "independent.txt"]),
        "second dirty across restart\n"
    );
    assert_eq!(native_gc_report(&mut fixture, true)["status"], "collected");
    fixture.success(
        &base_dir,
        &["tinytree", "unmount", second.to_str().unwrap()],
    );
    fixture.success(&base_dir, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
    // Final offline inspection has no live daemon or native provider owner.
    {
        let store = tinytree_store::Store::open_existing(&store_path, Default::default()).unwrap();
        assert_eq!(store.descriptor("local", orphan.key).unwrap(), None);
        for bytes in [
            b"first native version\n".as_slice(),
            b"second native version\n".as_slice(),
            b"second agent\n".as_slice(),
            b"dirty across restart\n".as_slice(),
            b"second dirty across restart\n".as_slice(),
        ] {
            let descriptor = m::describe_file(bytes);
            assert_eq!(
                store.descriptor("local", descriptor.key).unwrap(),
                Some(descriptor)
            );
            let mut actual = Vec::new();
            store
                .read("local", descriptor.key)
                .unwrap()
                .read_to_end(&mut actual)
                .unwrap();
            assert_eq!(actual, bytes);
        }
        for id in [&base, &child, &amended_child] {
            let key = m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(*blake3::Hash::from_hex(id).unwrap().as_bytes()),
            };
            assert!(store.descriptor("local", key).unwrap().is_some());
        }
    }
    assert_gc_rejects_unenrolled_state();
}

fn native_gc_report(fixture: &mut Fixture, apply: bool) -> serde_json::Value {
    let args = if apply {
        vec!["native-gc", "--store", "example", "--apply"]
    } else {
        vec!["native-gc", "--store", "example"]
    };
    let output = fixture.workspace(&args);
    assert!(
        output.status.success(),
        "native GC {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let envelope: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(envelope["ok"], true);
    envelope
        .get("report")
        .filter(|report| report.is_object())
        .expect("native GC CLI must return its report object")
        .clone()
}

fn assert_gc_rejects_unenrolled_state() {
    let mut fixture = Fixture::new();
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("unenrolled");
    fixture.initialize(&root);
    let binding = fixture.binding(&root);
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
    let retained = native_admin_state(&binding);
    fixture.enable_local_gc();
    let rejected = fixture.workspace(&["serve"]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("cohort"));
    assert_eq!(native_admin_state(&binding), retained);
    assert!(!fuse_mounted(&root));
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
impl<S, B, R> tower::Service<http::Request<B>> for CountFileReads<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<R>>,
    R: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        futures::future::Either<std::future::Ready<Result<Self::Response, Self::Error>>, S::Future>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if request.uri().path() == "/tinytree.cloud.v1.WorkspaceCloud/GetVersion"
            && self.metadata.unavailable_versions.load(Ordering::Acquire)
        {
            return futures::future::Either::Left(std::future::ready(Ok(Status::not_found(
                "fixture retained version temporarily unavailable",
            )
            .into_http())));
        }
        self.metadata.transport_calls.fetch_add(1, Ordering::SeqCst);
        if request
            .uri()
            .path()
            .starts_with("/tinytree.cloud.v1.WorkspaceCloud/")
        {
            *self
                .metadata
                .cloud_calls
                .lock()
                .unwrap()
                .entry(request.uri().path().to_owned())
                .or_default() += 1;
        }
        if request.uri().path() == "/tinytree.v1.Repository/ReadFile" {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        if request.uri().path() == "/tinytree.v1.Repository/WriteObject" {
            self.metadata.writes.fetch_add(1, Ordering::SeqCst);
        }
        if request
            .uri()
            .path()
            .starts_with("/tinytree.query.v1.RepositoryQuery/")
        {
            self.metadata.query_calls.fetch_add(1, Ordering::SeqCst);
        }
        if request
            .uri()
            .path()
            .starts_with("/tinytree.landing.v1.Landing/")
        {
            self.metadata.landing_calls.fetch_add(1, Ordering::SeqCst);
            match request.uri().path() {
                "/tinytree.landing.v1.Landing/StartLanding" => {
                    self.metadata.landing_starts.fetch_add(1, Ordering::SeqCst);
                }
                "/tinytree.landing.v1.Landing/GetLanding" => {
                    self.metadata.landing_lookups.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
        }
        futures::future::Either::Right(self.inner.call(request))
    }
}

#[derive(Default)]
struct MetadataReads {
    ordinary_scope_opens: AtomicUsize,
    shortest_prefix_replies: AtomicUsize,
    hold_publications: AtomicUsize,
    capability_requests: Mutex<Vec<Vec<i32>>>,
    blame_policies: Mutex<Vec<i32>>,
    annotation_anchors: Mutex<Vec<Vec<Vec<u8>>>>,
    annotation_nodes: AtomicUsize,
    annotation_continues: AtomicUsize,
    retained_scope_opens: AtomicUsize,
    creation_fault: AtomicUsize,
    unavailable_versions: AtomicBool,
    deny_metadata: AtomicBool,
    transport_calls: AtomicUsize,
    cloud_calls: Mutex<BTreeMap<String, usize>>,
    file_keys: Mutex<HashSet<[u8; 32]>>,
    writes: AtomicUsize,
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
    history_calls: AtomicUsize,
    landing_calls: AtomicUsize,
    landing_starts: AtomicUsize,
    landing_lookups: AtomicUsize,
    public_paused: AtomicBool,
    public_started: AtomicBool,
    public_release: tokio::sync::Notify,
}

/// Observe decoded metadata without replacing any repository implementation.
#[derive(Clone)]
struct CountMetadata {
    inner: tinytree_server::RepositoryService,
    reads: Arc<MetadataReads>,
}

#[tonic::async_trait]
impl Repository for CountMetadata {
    async fn get_capabilities(
        &self,
        request: RpcRequest<p::GetCapabilitiesRequest>,
    ) -> Result<RpcResponse<p::Capabilities>, Status> {
        self.reads
            .capability_requests
            .lock()
            .unwrap()
            .push(request.get_ref().features.clone());
        match self.reads.creation_fault.load(Ordering::SeqCst) {
            8 if matches!(request.get_ref().features.as_slice(), [11] | [14]) => {
                Err(Status::invalid_argument("legacy feature vocabulary"))
            }
            9 if matches!(request.get_ref().features.as_slice(), [11] | [14]) => {
                let mut response = self.inner.get_capabilities(request).await?;
                response.get_mut().features[0].authorized = false;
                Ok(response)
            }
            10 if matches!(request.get_ref().features.as_slice(), [11] | [14]) => {
                Err(Status::resource_exhausted("capability pressure"))
            }
            11 if matches!(request.get_ref().features.as_slice(), [11] | [14]) => {
                Err(Status::data_loss("invalid capability reply"))
            }
            1 => Err(Status::unimplemented(
                "legacy server has no feature negotiation",
            )),
            2 => Err(Status::permission_denied("creation features denied")),
            6 => {
                let mut response = self.inner.get_capabilities(request).await?;
                for feature in &mut response.get_mut().features {
                    if feature.feature == 5 {
                        feature.authorized = false;
                    }
                }
                Ok(response)
            }
            _ => self.inner.get_capabilities(request).await,
        }
    }

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
        if self.reads.deny_metadata.load(Ordering::SeqCst) {
            return Err(Status::unavailable(
                "fixture canonical metadata is unavailable",
            ));
        }
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
        if let Ok(id) = request.get_ref().native_file_id.as_slice().try_into() {
            self.reads.file_keys.lock().unwrap().insert(id);
        }
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
        if request
            .get_ref()
            .reference_name
            .starts_with("_tinytree/hold/")
        {
            self.reads.hold_publications.fetch_add(1, Ordering::SeqCst);
            if self.reads.creation_fault.load(Ordering::SeqCst) == 7 {
                return Err(Status::permission_denied("selected root retention denied"));
            }
        }
        if request.get_ref().reference_name == "main"
            && self.reads.public_paused.load(Ordering::SeqCst)
        {
            self.reads.public_started.store(true, Ordering::SeqCst);
            loop {
                let released = self.reads.public_release.notified();
                if !self.reads.public_paused.load(Ordering::SeqCst) {
                    break;
                }
                released.await;
            }
        }
        self.inner.publish_reference(request).await
    }

    async fn get_publication(
        &self,
        request: RpcRequest<p::GetPublicationRequest>,
    ) -> Result<RpcResponse<p::Publication>, Status> {
        self.inner.get_publication(request).await
    }
}

/// Observe logical query payloads while forwarding all extensions. Registration
/// uses the repository's guarded transport to preserve predecode admission.
#[derive(Clone)]
struct CountQueryMetadata {
    inner: tinytree_server::QueryService,
    reads: Arc<MetadataReads>,
}
#[tonic::async_trait]
impl RepositoryQuery for CountQueryMetadata {
    async fn open_annotation_frontier(&self, request: RpcRequest<q::OpenAnnotationFrontierRequest>) -> Result<RpcResponse<q::AnnotationFrontier>, Status> {
        self.reads.annotation_anchors.lock().unwrap().push(request.get_ref().anchors.iter().map(|id|id.value.clone()).collect());
        self.inner.open_annotation_frontier(request).await
    }

    async fn open_annotation_frontier_paged(
        &self,
        request: RpcRequest<q::OpenAnnotationFrontierPagedRequest>,
    ) -> Result<RpcResponse<q::AnnotationFrontierPage>, Status> {
        self.reads.annotation_anchors.lock().unwrap().push(
            request
                .get_ref()
                .input
                .as_ref()
                .unwrap()
                .anchors
                .iter()
                .map(|id| id.value.clone())
                .collect(),
        );
        let reply = self.inner.open_annotation_frontier_paged(request).await?;
        self.reads.annotation_nodes.fetch_add(
            reply.get_ref().frontier.as_ref().unwrap().nodes.len(),
            Ordering::SeqCst,
        );
        Ok(reply)
    }
    async fn continue_annotation_frontier(
        &self,
        request: RpcRequest<q::ContinueAnnotationFrontierRequest>,
    ) -> Result<RpcResponse<q::AnnotationFrontierPage>, Status> {
        self.reads
            .annotation_continues
            .fetch_add(1, Ordering::SeqCst);
        let reply = self.inner.continue_annotation_frontier(request).await?;
        self.reads.annotation_nodes.fetch_add(
            reply.get_ref().frontier.as_ref().unwrap().nodes.len(),
            Ordering::SeqCst,
        );
        Ok(reply)
    }

    async fn read_annotation_input(
        &self,
        request: RpcRequest<q::ReadAnnotationInputRequest>,
    ) -> Result<RpcResponse<q::AnnotationInput>, Status> {
        self.inner.read_annotation_input(request).await
    }

    async fn close_annotation_frontier(
        &self,
        request: RpcRequest<q::CloseAnnotationFrontierRequest>,
    ) -> Result<RpcResponse<q::CloseAnnotationFrontierResponse>, Status> {
        self.inner.close_annotation_frontier(request).await
    }

    async fn search(
        &self,
        r: RpcRequest<q::SearchRequest>,
    ) -> Result<RpcResponse<q::SearchResult>, Status> {
        self.inner.search(r).await
    }
    async fn path_history(
        &self,
        r: RpcRequest<q::PathHistoryRequest>,
    ) -> Result<RpcResponse<q::PathHistoryResult>, Status> {
        self.reads.history_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.path_history(r).await
    }
    async fn blame(
        &self,
        r: RpcRequest<q::BlameRequest>,
    ) -> Result<RpcResponse<q::BlameResult>, Status> {
        self.reads.blame_policies.lock().unwrap().push(r.get_ref().policy);
        self.inner.blame(r).await
    }
    async fn open_scope(
        &self,
        r: RpcRequest<Streaming<q::OpenScopeFrame>>,
    ) -> Result<RpcResponse<q::QueryScope>, Status> {
        self.reads
            .ordinary_scope_opens
            .fetch_add(1, Ordering::SeqCst);
        if self.reads.creation_fault.load(Ordering::SeqCst) == 3 {
            return Err(Status::failed_precondition(
                "selected immutable root is unavailable",
            ));
        }
        self.inner.open_scope(r).await
    }
    async fn open_cloud_scope(
        &self,
        r: RpcRequest<Streaming<q::OpenScopeFrame>>,
    ) -> Result<RpcResponse<q::QueryScope>, Status> {
        self.reads
            .retained_scope_opens
            .fetch_add(1, Ordering::SeqCst);
        if self.reads.creation_fault.load(Ordering::SeqCst) == 4 {
            return Err(Status::failed_precondition(
                "selected retained source is unavailable",
            ));
        }
        self.inner.open_cloud_scope(r).await
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
        let reply = self.inner.shortest_prefix(r).await;
        if reply.is_ok() {
            self.reads
                .shortest_prefix_replies
                .fetch_add(1, Ordering::SeqCst);
        }
        reply
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
    historical_commits: Vec<tinytree_model::ObjectKey>,
    summary_parent_commits: Vec<tinytree_model::ObjectKey>,
    source_file: tinytree_model::ObjectKey,
    source_bytes: Vec<u8>,
    reads: Arc<AtomicUsize>,
    metadata: Arc<MetadataReads>,
    setup_ns: u128,
    index_ready_ns: u128,
}

#[derive(Clone, Copy)]
struct GrowingCloudProofProfile {
    validation_records: usize,
    validation_metadata_bytes: u64,
    admission_bytes: u64,
}

impl RemoteServer {
    fn start() -> Self {
        Self::start_with_history(8)
    }
    fn start_with_history(history: usize) -> Self {
        Self::start_with_history_and_cloud(history, false)
    }
    fn start_with_cloud() -> Self {
        Self::start_with_history_and_cloud(8, true)
    }
    const GROWING_PRIVATE_PROOF_PROFILE: GrowingCloudProofProfile = GrowingCloudProofProfile {
        validation_records: 524_288,
        validation_metadata_bytes: 64 * 1024 * 1024,
        admission_bytes: 12 * 1024 * 1024 * 1024,
    };
    fn start_with_growing_private_history() -> Self {
        // This expanded workload retains every queued capture and charges each
        // retained version's future revalidation work. These finite record,
        // metadata, and admission bounds size this sample, not every command.
        // Admission accounting is not a measured process-memory reservation.
        Self::start_profile_configured(
            8, true, None, false, false, None, false, None,
            Some(Self::GROWING_PRIVATE_PROOF_PROFILE),
        )
    }
    fn cloud_profile(proof_profile: Option<GrowingCloudProofProfile>) -> tinytree_server::Config {
        let mut profile: tinytree_server::Config = toml::from_str(
                r#"
[service]
max_metadata_bytes=4194304
[service.cloud]
enabled=true
indexed_versions=true
captured_versions=true
max_capture_bytes=262144
max_manifest_bytes=524288
max_staging_leases=256
max_buffered_bytes=2147483648
[store]
max_retention_validation_records=4096
max_retention_validation_bytes=8388608
max_closure_objects=8192
max_closure_edges=32768
max_path_contexts=8192
max_path_context_bytes=8388608
[store.cloud]
max_object_bytes=4194304
max_decode_bytes=16777216
max_version_decode_bytes=268435456
max_manifest_bytes=524288
max_manifest_decode_bytes=16777216
max_manifest_members=1024
"#,
        )
        .unwrap();
        if let Some(proof) = proof_profile {
            profile.store.max_retention_validation_records = proof.validation_records;
            profile.store.max_retention_validation_bytes = proof.validation_metadata_bytes;
            profile.service.cloud.max_buffered_bytes = proof.admission_bytes;
        }
        profile
    }
    fn start_with_cloud_capture_capacity() -> Self {
        Self::start_with_cloud_capture_capacity_and_history(8)
    }
    fn start_with_cloud_capture_capacity_and_history(history: usize) -> Self {
        // This many-capture workload retains historical operation/index proofs
        // for both workspaces. Give its catalog an explicit bounded profile;
        // keep the small fixtures and production defaults unchanged. The cloud
        // pool accounts configured worst-case work, not eager allocation/RSS.
        Self::start_profile_configured(
            history,
            true,
            None,
            false,
            false,
            Some((262_144, 64 * 1024 * 1024, 12 * 1024 * 1024 * 1024)),
            false,
            // Explicit legacy history/blame checks use a caller-supplied bound
            // sufficient for this seeded fixture; production defaults stay fixed.
            Some((history + 1).next_power_of_two().max(256)),
            None,
        )
    }
    fn start_with_cloud_landing_recovery() -> Self {
        // Recovery retains ordinary cloud versions before its pending landing
        // capsule. Use the ordinary cloud catalog budget for this combined
        // workflow, without changing the small landing/performance profiles.
        Self::start_profile_configured(8, true, None, false, true, None, true, None, None)
    }
    fn start_with_history_and_cloud(history: usize, cloud_enabled: bool) -> Self {
        Self::start_configured(history, cloud_enabled, None)
    }
    fn start_with_blame_limit(bytes: usize) -> Self {
        Self::start_configured(8, false, Some(bytes))
    }
    fn start_configured(history: usize, cloud_enabled: bool, blame_bytes: Option<usize>) -> Self {
        Self::start_search_configured(history, cloud_enabled, blame_bytes, false)
    }
    fn start_with_search() -> Self {
        Self::start_search_configured(8, false, None, true)
    }
    fn start_search_configured(
        history: usize,
        cloud_enabled: bool,
        blame_bytes: Option<usize>,
        search_seed: bool,
    ) -> Self {
        Self::start_landing_configured(history, cloud_enabled, blame_bytes, search_seed, false)
    }
    fn start_with_landing() -> Self {
        Self::start_landing_configured(8, false, None, false, true)
    }
    fn start_landing_configured(
        history: usize,
        cloud_enabled: bool,
        blame_bytes: Option<usize>,
        search_seed: bool,
        landing_enabled: bool,
    ) -> Self {
        Self::start_profile_configured(
            history,
            cloud_enabled,
            blame_bytes,
            search_seed,
            landing_enabled,
            None,
            false,
            None,
            None,
        )
    }
    fn start_with_history_scan_limit(limit: usize) -> Self {
        Self::start_profile_configured(8, false, None, false, false, None, false, Some(limit), None)
    }
    fn start_profile_configured(
        history: usize,
        cloud_enabled: bool,
        blame_bytes: Option<usize>,
        search_seed: bool,
        landing_enabled: bool,
        cloud_catalog: Option<(usize, u64, u64)>,
        cloud_landing_recovery: bool,
        history_scan_limit: Option<usize>,
        cloud_proof_profile: Option<GrowingCloudProofProfile>,
    ) -> Self {
        Self::start_retention_profile_configured(
            history,
            cloud_enabled,
            blame_bytes,
            search_seed,
            landing_enabled,
            cloud_catalog,
            cloud_landing_recovery,
            history_scan_limit,
            cloud_proof_profile,
            None,
        )
    }
    fn start_with_rolling_cloud_window(window: u64) -> Self {
        Self::start_retention_profile_configured(
            8,
            true,
            None,
            false,
            false,
            None,
            false,
            None,
            None,
            Some(window),
        )
    }
    fn start_retention_profile_configured(
        history: usize,
        cloud_enabled: bool,
        blame_bytes: Option<usize>,
        search_seed: bool,
        landing_enabled: bool,
        cloud_catalog: Option<(usize, u64, u64)>,
        cloud_landing_recovery: bool,
        history_scan_limit: Option<usize>,
        cloud_proof_profile: Option<GrowingCloudProofProfile>,
        rolling_window: Option<u64>,
    ) -> Self {
        assert!((1..=1024).contains(&history));
        assert!(
            !cloud_landing_recovery
                || (cloud_enabled && landing_enabled && cloud_catalog.is_none())
        );
        assert!(cloud_proof_profile.is_none()
            || (cloud_enabled && !landing_enabled && cloud_catalog.is_none()));
        let setup_started = Instant::now();
        let directory = tempfile::tempdir().unwrap();
        let (mut configuration, store_configuration) = if cloud_enabled || landing_enabled {
            let mut profile = Self::cloud_profile(cloud_proof_profile);
            if landing_enabled {
                if !cloud_landing_recovery {
                    profile.store.max_retention_validation_records = 1024;
                    profile.store.max_retention_validation_bytes = 2 * 1024 * 1024;
                }
                profile.store.max_closure_objects = 4096;
                profile.store.max_closure_edges = 16384;
                profile.store.max_path_contexts = 4096;
                profile.store.max_path_context_bytes = 4 * 1024 * 1024;
                profile.store.cloud.max_version_decode_bytes = 128 * 1024 * 1024;
            }
            if let Some((records, bytes, admission)) = cloud_catalog {
                profile.store.max_retention_validation_records = records;
                profile.store.max_retention_validation_bytes = bytes;
                profile.service.cloud.max_buffered_bytes = admission;
                // One operator/repository drives this burst workload. Reuse the
                // existing global replay capacity without raising active limits
                // or shortening the closed-scope idempotency window.
                profile.service.query.max_scope_replays_per_principal =
                    profile.service.query.max_scope_replays;
                profile.service.query.max_scope_replays_per_repository =
                    profile.service.query.max_scope_replays;
            }
            if let Some(window) = rolling_window {
                profile.store.cloud.retirement_enabled = true;
                profile.store.cloud.rolling_retention_enabled = true;
                profile.store.cloud.max_versions_per_workspace = window;
            }
            profile.validate().unwrap();
            let store = profile
                .store
                .materialize(profile.service.max_metadata_bytes);
            (profile.service, store)
        } else {
            (
                tinytree_server::ServiceConfig::default(),
                tinytree_store::Config::default(),
            )
        };
        if let Some(limit) = history_scan_limit {
            configuration.query.max_history_commits = limit;
        }
        if landing_enabled {
            configuration.landing = toml::from_str(
                r#"
enabled=true
max_buffered_bytes=3221225472
poll_interval_ms=10
[[profiles]]
name="checks"
version="v1"
repositories=["example"]
"#,
            )
            .unwrap();
            if cloud_landing_recovery {
                // The combined catalog also needs funded Store-validation work
                // in landing admission; keep the small profiles unchanged.
                configuration.landing.max_buffered_bytes = 4 * 1024 * 1024 * 1024;
            }
        }
        if let Some(bytes) = blame_bytes {
            configuration.query.max_blame_file_bytes = bytes;
        }
        let store =
            tinytree_store::Store::open(directory.path().join("store"), store_configuration)
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
        let m::Object::Commit(mut template) =
            m::decode(&root_commit.bytes, m::Limits::default()).unwrap()
        else {
            unreachable!()
        };
        let root = seed
            .objects()
            .iter()
            .find(|object| object.descriptor.key.id.0 == template.root_tree.terms[0].0)
            .unwrap();
        let m::Object::Tree(mut current_tree) =
            m::decode(&root.bytes, m::Limits::default()).unwrap()
        else {
            unreachable!()
        };
        if search_seed {
            let file = |bytes: &[u8]| {
                let descriptor = m::describe_file(bytes);
                store.put("example", descriptor, bytes, 0).unwrap();
                m::TreeValue::File {
                    id: m::FileId(descriptor.key.id.0),
                    executable: false,
                    size_bytes: descriptor.size_bytes,
                    copy_id: None,
                }
            };
            for (name, bytes) in [
                ("a-many.txt", b"needle\n".repeat(300)),
                ("deleted.txt", b"needle deleted baseline\n".to_vec()),
                ("keep.txt", b"needle unchanged baseline\n".to_vec()),
                ("modified.txt", b"needle modified baseline\n".to_vec()),
                ("rename.txt", b"needle rename baseline\n".to_vec()),
                ("shape.txt", b"needle file before directory\n".to_vec()),
            ] {
                current_tree.entries.push(m::TreeEntry {
                    name: name.into(),
                    value: file(&bytes),
                });
            }
            let swap = m::encode(
                &m::Object::Tree(m::Tree {
                    entries: vec![m::TreeEntry {
                        name: "old.txt".into(),
                        value: file(b"needle child before replacement\n"),
                    }],
                }),
                m::Limits::default(),
            )
            .unwrap();
            store
                .put("example", swap.descriptor, &swap.bytes, 0)
                .unwrap();
            current_tree.entries.push(m::TreeEntry {
                name: "swap".into(),
                value: m::TreeValue::Tree(m::TreeId(swap.descriptor.key.id.0)),
            });
            current_tree
                .entries
                .sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
            let tree =
                m::encode(&m::Object::Tree(current_tree.clone()), m::Limits::default()).unwrap();
            store
                .put("example", tree.descriptor, &tree.bytes, 0)
                .unwrap();
            template.root_tree.terms = vec![m::TreeId(tree.descriptor.key.id.0)];
        }
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
        let mut historical_commits = Vec::new();
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
            historical_commits.push(commit.descriptor.key);
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
        let request_bytes = configuration
            .max_request_bytes
            .max(configuration.max_frame_bytes + 1024);
        let response_bytes = configuration.max_metadata_bytes + 4096;
        let service = runtime.block_on(async {
            tinytree_server::RepositoryService::new(
                store.clone(),
                configuration,
                vec!["example".into()],
                vec![tinytree_server::Principal::new(
                    "mounted-writer",
                    REMOTE_TOKEN,
                    vec!["example".into()],
                    true,
                )
                .unwrap()
                .with_retention(true)
                .with_cloud(cloud_enabled)
                .with_landing_profiles(if landing_enabled {
                    vec!["checks".into()]
                } else {
                    vec![]
                })],
            )
            .unwrap()
            .with_query_index(index)
            .unwrap()
        });
        runtime.block_on(async {
            service.start_landing_worker().unwrap();
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
        let cloud = service.cloud_grpc_service();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let (endpoint, task) = runtime.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                let drain = service.clone();
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
                    .add_service(service.query_grpc_service_with_adapter(query))
                    .add_service(cloud)
                    .add_service(service.landing_grpc_service())
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
                drain.drain_landing().await;
            });
            (endpoint, task)
        });
        let published = store
            .references("example", &["main".into()])
            .unwrap()
            .remove(0)
            .commit
            .unwrap();
        let (published_descriptor, published_bytes) =
            store.read_metadata("example", published.key).unwrap();
        assert_eq!(published_descriptor, published);
        let m::Object::Commit(published_commit) =
            m::decode(&published_bytes, m::Limits::default()).unwrap()
        else {
            panic!("published head must be a commit");
        };
        let summary_parent_commits = published_commit
            .parents
            .into_iter()
            .map(|id| m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(id.0),
            })
            .collect();
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
            historical_commits,
            summary_parent_commits,
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

    fn historical_commit_reads(&self) -> usize {
        let requested = self.metadata.keys.lock().unwrap();
        self.historical_commits
            .iter()
            .filter(|key| requested.contains(&(key.kind as i32, key.id.0)))
            .count()
    }

    fn unrelated_historical_commit_reads(&self) -> usize {
        // The default public-tip summary asks whether that commit is empty,
        // which needs its exact parent COMMITs, but not their ancestry or TREEs.
        // Derive that allowance from the published canonical head, not a count.
        let requested = self.metadata.keys.lock().unwrap();
        self.historical_commits
            .iter()
            .filter(|key| !self.summary_parent_commits.contains(key))
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

fn assert_builtin_blame_parity(fixture: &mut Fixture, root: &Path, result: &serde_json::Value) {
    let oracle = fixture.success(
        root,
        &[
            "file",
            "annotate",
            "hello.cc",
            "--revision",
            "@",
            "--template",
            "commit.commit_id() ++ \":\" ++ original_line_number ++ \"\\n\"",
        ],
    );
    let expected = oracle.lines().map(str::to_owned).collect::<Vec<_>>();
    let actual = result["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|line| {
            let origin = &line["origin"];
            format!(
                "{}:{}",
                origin["commit"].as_str().unwrap(),
                origin["original_line"].as_u64().unwrap()
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "remote/local composition must match pinned jj FileAnnotator origins, not rendered fallback labels"
    );
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn shipped_remote_history_collects_only_when_explicitly_requested() {
    let upstream = RemoteServer::start_with_history_scan_limit(2);
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.start();
    let root = fixture.base().join("history-collector");
    fixture.clone_remote(&root);
    let binding = fixture.binding(&root);
    let sidecar = fs::read(binding.working_copy.join("native-remote.toml")).unwrap();
    let public = upstream.public();
    let writes = upstream.metadata.writes.load(Ordering::SeqCst);
    let files = upstream.file_reads();
    let holds = upstream.metadata.hold_publications.load(Ordering::SeqCst);
    let calls = upstream.metadata.history_calls.load(Ordering::SeqCst);
    let bounded: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree",
            "history",
            "hello.cc",
            "--revision",
            "main",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(bounded["revision"], upstream.head);
    assert_eq!(bounded["coverage"], "scan_limit");
    assert_eq!(bounded["remote_scanned_commits"], 2);
    assert_eq!(bounded["local_commits"], 0);
    assert!(
        bounded["entries"].as_array().unwrap().is_empty(),
        "hello.cc was introduced at the oldest seeded commit, beyond this bounded page"
    );
    assert_eq!(
        upstream.metadata.history_calls.load(Ordering::SeqCst) - calls,
        1,
        "the default request remains exactly one history RPC"
    );
    let calls = upstream.metadata.history_calls.load(Ordering::SeqCst);
    let complete: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree",
            "history",
            "hello.cc",
            "--revision",
            "main",
            "--max-commits",
            "512",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(
        complete["revision"], upstream.head,
        "collection must preserve the original exact revision"
    );
    assert_eq!(complete["coverage"], "complete");
    assert_eq!(complete["local_commits"], 0);
    assert!(complete["remote_scanned_commits"].as_u64().unwrap() > 2);
    assert!(
        upstream.metadata.history_calls.load(Ordering::SeqCst) - calls > 1,
        "the explicit bound must continue through multiple limited RPCs"
    );
    let entries = complete["entries"].as_array().unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry["commit"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![upstream.historical_head.as_str()],
        "the fixed seed introduces hello.cc exactly once at its oldest commit"
    );
    assert_eq!(entries[0]["state"]["kind"], "file");
    assert_eq!(
        entries[0]["state"]["id"],
        blake3::Hash::from(upstream.source_file.id.0)
            .to_hex()
            .to_string()
    );
    assert_eq!(entries[0]["parent_state"]["kind"], "absent");
    assert_eq!(entries[0]["local"], false);
    assert_eq!(
        fs::read(binding.working_copy.join("native-remote.toml")).unwrap(),
        sidecar
    );
    assert_eq!(upstream.public(), public);
    assert_eq!(upstream.metadata.writes.load(Ordering::SeqCst), writes);
    assert_eq!(
        upstream.file_reads(),
        files,
        "history metadata must not hydrate FILE bodies into the client"
    );
    assert_eq!(
        upstream.metadata.hold_publications.load(Ordering::SeqCst),
        holds,
        "read-only history must not publish retention holds"
    );
    fixture
        .control(Request::NativeUnmount {
            workspace_id: binding.id(),
        })
        .unwrap();
    fixture.stop_daemon();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn shipped_remote_blame_composes_private_and_dirty_paths_without_uploading() {
    let upstream = RemoteServer::start();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("legacy");
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("path-query");
    fixture.clone_remote(&root);
    let binding = fixture.binding(&root);
    assert_eq!(
        fs::read_to_string(binding.repo.join("index/type")).unwrap(),
        jj_lib::default_index::DefaultIndexStore::NAME
    );
    let observation = remote_observation(&binding).0;
    let public = upstream.public();
    let writes = upstream.metadata.writes.load(Ordering::SeqCst);
    let reads = upstream.file_reads();
    let public_result: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree",
            "blame",
            "hello.cc",
            "--revision",
            "main",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(public_result["coverage"], "complete");
    assert_eq!(public_result["revision"], upstream.head);
    assert_eq!(public_result["local_commits"], 0);
    let public_history: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree",
            "history",
            "hello.cc",
            "--revision",
            "main",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(public_history["coverage"], "complete");
    assert_eq!(public_history["entries"].as_array().unwrap().len(), 1);
    assert_eq!(
        upstream.file_reads(),
        reads,
        "remote history/blame transfers only selected target bytes inside Blame, never historical FILE RPCs to the client"
    );
    // Provisioning checks out the public commit by creating jj's ordinary
    // private working-copy child. It remains part of the ancestry we scan,
    // even though this path's bytes are unchanged in that child.
    assert_ne!(fixture.revision(&root, "@"), upstream.head);
    assert_eq!(fixture.revision(&root, "@-"), upstream.head);

    fixture.success(&root, &["new", "-m", "private path base"]);
    let mut first = b"repeat\nrepeat\nlocal base\n".to_vec();
    first.extend_from_slice(&upstream.source_bytes);
    fs::write(root.join("hello.cc"), &first).unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["new", "-m", "private path child"]);
    let mut dirty = b"repeat\nlocal child\nrepeat\n".to_vec();
    dirty.extend_from_slice(&upstream.source_bytes);
    dirty.extend_from_slice(b"dirty\0\xff without newline");
    fs::write(root.join("hello.cc"), &dirty).unwrap();
    // No status/snapshot command here: the query must capture the actual dirty @.
    let result: serde_json::Value =
        serde_json::from_str(&fixture.success(&root, &["tinytree", "blame", "hello.cc", "--json"]))
            .unwrap();
    assert_eq!(result["coverage"], "complete");
    assert_eq!(result["local_commits"], 3);
    assert_eq!(fixture.revision(&root, "@---"), upstream.head);
    assert_eq!(result["revision"], fixture.revision(&root, "@"));
    let decoded = result["lines"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|line| {
            line["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|byte| byte.as_u64().unwrap() as u8)
        })
        .collect::<Vec<_>>();
    assert_eq!(decoded, dirty);
    assert_eq!(
        upstream.file_reads(),
        reads,
        "local overlays must not hydrate remote historical files or unrelated assets"
    );
    assert_eq!(
        upstream.metadata.writes.load(Ordering::SeqCst),
        writes,
        "querying private/dirty content must not publish its objects"
    );
    let private_id = m::ObjectId(
        *blake3::Hash::from_hex(result["revision"].as_str().unwrap())
            .unwrap()
            .as_bytes(),
    );
    assert!(upstream
        .store
        .descriptor(
            "example",
            m::ObjectKey {
                kind: m::Kind::Commit,
                id: private_id
            }
        )
        .unwrap()
        .is_none());
    assert_eq!(remote_observation(&binding).0, observation);
    assert_eq!(upstream.public(), public);
    // The oracle may read historical bodies. Counter assertions above precede it.
    assert_builtin_blame_parity(&mut fixture, &root, &result);

    // A type transition ends the old fixed-path lifetime even if exactly the
    // same bytes are re-added later. Do not blame the earlier incarnation.
    fixture.success(&root, &["new", "-m", "path becomes directory"]);
    fs::remove_file(root.join("hello.cc")).unwrap();
    fs::create_dir(root.join("hello.cc")).unwrap();
    fs::write(root.join("hello.cc/nested"), b"directory incarnation\n").unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["new", "-m", "path returns as file"]);
    fs::remove_dir_all(root.join("hello.cc")).unwrap();
    fs::write(root.join("hello.cc"), &dirty).unwrap();
    let returned: serde_json::Value =
        serde_json::from_str(&fixture.success(&root, &["tinytree", "blame", "hello.cc", "--json"]))
            .unwrap();
    assert_eq!(returned["coverage"], "complete");
    assert!(returned["lines"]
        .as_array()
        .unwrap()
        .iter()
        .all(|line| line["origin"]["commit"] == returned["revision"]));
    assert_builtin_blame_parity(&mut fixture, &root, &returned);
    assert_eq!(upstream.metadata.writes.load(Ordering::SeqCst), writes);
    assert_eq!(remote_observation(&binding).0, observation);
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();

    // The server may return a typed incomplete result without target bytes when
    // its configured file cap is too small. It must not become complete-empty.
    let limited_upstream = RemoteServer::start_with_blame_limit(1);
    let mut limited = Fixture::new();
    limited.configure_remote(&limited_upstream.endpoint);
    limited.start();
    let limited_base = limited.base().to_owned();
    let limited_root = limited_base.join("limited-path-query");
    limited.clone_remote(&limited_root);
    let rejected = limited.run(
        &limited_root,
        &[
            "tinytree",
            "blame",
            "hello.cc",
            "--revision",
            "main",
            "--json",
        ],
    );
    assert!(
        !rejected.status.success(),
        "missing over-budget baseline cannot be a complete empty file"
    );
    assert!(rejected.stdout.is_empty());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("baseline content unavailable"));
    assert_eq!(limited_upstream.file_reads(), 0);
    limited.success(
        &limited_base,
        &["tinytree", "unmount", limited_root.to_str().unwrap()],
    );
    limited.stop_daemon();

    // Exercise the actual thin-index factory as well. Ordinary jj annotate's
    // file predicate is unsupported there; the Legacy oracle above is distinct.
    let mut thin = Fixture::new();
    thin.configure_remote(&upstream.endpoint);
    thin.index_mode("federated");
    thin.start();
    let thin_base = thin.base().to_owned();
    let thin_root = thin_base.join("thin-path-query");
    thin.clone_remote(&thin_root);
    let thin_binding = thin.binding(&thin_root);
    assert_eq!(
        fs::read_to_string(thin_binding.repo.join("index/type")).unwrap(),
        jj_native_index::NAME
    );
    let thin_observation = remote_observation(&thin_binding).0;
    let thin_reads = upstream.file_reads();
    let thin_writes = upstream.metadata.writes.load(Ordering::SeqCst);
    let thin_public: serde_json::Value = serde_json::from_str(&thin.success(
        &thin_root,
        &[
            "tinytree",
            "blame",
            "hello.cc",
            "--revision",
            "main",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(thin_public["coverage"], "complete");
    assert_eq!(thin_public["revision"], upstream.head);
    assert_eq!(thin_public["local_commits"], 0);
    assert_eq!(thin.revision(&thin_root, "@-"), upstream.head);
    thin.success(&thin_root, &["new", "-m", "thin private path"]);
    let mut thin_dirty = b"thin dirty line\n".to_vec();
    thin_dirty.extend_from_slice(&upstream.source_bytes);
    fs::write(thin_root.join("hello.cc"), &thin_dirty).unwrap();
    let thin_result: serde_json::Value = serde_json::from_str(
        &thin.success(&thin_root, &["tinytree", "blame", "hello.cc", "--json"]),
    )
    .unwrap();
    assert_eq!(thin_result["coverage"], "complete");
    assert_eq!(thin_result["local_commits"], 2);
    assert_eq!(thin.revision(&thin_root, "@--"), upstream.head);
    assert_eq!(thin_result["revision"], thin.revision(&thin_root, "@"));
    let thin_lines = thin_result["lines"].as_array().unwrap();
    assert_eq!(
        thin_lines.len(),
        thin_public["lines"].as_array().unwrap().len() + 1
    );
    assert_eq!(
        thin_lines[0]["bytes"],
        serde_json::json!(b"thin dirty line\n".as_slice())
    );
    assert_eq!(thin_lines[0]["origin"]["commit"], thin_result["revision"]);
    assert_eq!(thin_lines[0]["origin"]["original_line"], 1);
    for (actual, expected) in thin_lines[1..]
        .iter()
        .zip(thin_public["lines"].as_array().unwrap())
    {
        assert_eq!(actual["origin"], expected["origin"]);
        assert_eq!(actual["bytes"], expected["bytes"]);
    }
    let thin_history: serde_json::Value = serde_json::from_str(
        &thin.success(&thin_root, &["tinytree", "history", "hello.cc", "--json"]),
    )
    .unwrap();
    assert_eq!(thin_history["coverage"], "complete");
    assert_eq!(thin_history["local_commits"], 2);
    assert_eq!(
        upstream.file_reads(),
        thin_reads,
        "thin source queries must not hydrate historical or unrelated FILEs"
    );
    assert_eq!(
        upstream.metadata.writes.load(Ordering::SeqCst),
        thin_writes,
        "thin private queries must not publish authoritative objects"
    );
    assert_eq!(remote_observation(&thin_binding).0, thin_observation);
    thin.success(
        &thin_base,
        &["tinytree", "unmount", thin_root.to_str().unwrap()],
    );
    thin.stop_daemon();
}

// Explicit compatibility oracle only: keep the native objects/operation/IDs,
// but let stock FileAnnotator build its own eager Legacy index. Native-path
// hydration counters are checked before this separate oracle is invoked.
fn legacy_annotation_oracle(fixture: &mut Fixture, root: &Path, args: &[&str]) -> Vec<u8> {
    struct RestoreIndex {
        index: PathBuf,
        saved: PathBuf,
    }
    impl Drop for RestoreIndex {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.index).unwrap();
            fs::rename(&self.saved, &self.index).unwrap();
        }
    }
    let binding = fixture.binding(root);
    let index = binding.repo.join("index");
    let saved = binding.repo.join("annotate-oracle-native-index");
    fs::rename(&index, &saved).unwrap();
    fs::create_dir(&index).unwrap();
    let _restore = RestoreIndex {
        index: index.clone(),
        saved,
    };
    jj_lib::default_index::DefaultIndexStore::init(&index).unwrap();
    fs::write(
        index.join("type"),
        jj_lib::default_index::DefaultIndexStore::NAME,
    )
    .unwrap();
    let output = fixture.run(root, args);
    assert!(
        output.status.success(),
        "stock annotate oracle: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn ordinary_exact_path_log_filters_graphs_over_public_private_and_dirty_entries() {
    use std::os::unix::fs::PermissionsExt as _;
    let upstream = RemoteServer::start();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("federated");
    fixture
        .child_env
        .push(("JJ_TIMESTAMP".into(), "2001-02-03T04:05:06Z".into()));
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("ordinary-path-log");
    fixture.clone_remote(&root);
    fs::write(root.join("hello.cc"), b"private modification\n").unwrap();
    fixture.success(&root, &["status"]);
    let modified = fixture.revision(&root, "@");
    fixture.success(&root, &["new", "-m", "unchanged selected path"]);
    let unchanged = fixture.revision(&root, "@");
    fixture.success(&root, &["new", "-m", "mode-only selected path"]);
    fs::set_permissions(root.join("hello.cc"), fs::Permissions::from_mode(0o755)).unwrap();
    fixture.success(&root, &["status"]);
    let mode = fixture.revision(&root, "@");
    fixture.success(&root, &["new", "-m", "delete selected path"]);
    fs::remove_file(root.join("hello.cc")).unwrap();
    fixture.success(&root, &["status"]);
    let deleted = fixture.revision(&root, "@");
    fixture.success(&root, &["new", "-m", "readd selected path"]);
    fs::write(root.join("hello.cc"), b"readded selected file\n").unwrap();
    fixture.success(&root, &["status"]);
    let readded = fixture.revision(&root, "@");
    fixture.success(&root, &["new", "-m", "prefix-selected file"]);
    fs::write(root.join("shape"), b"prefix-selected file\n").unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["new", "-m", "selected file becomes directory"]);
    fs::remove_file(root.join("shape")).unwrap();
    fs::create_dir(root.join("shape")).unwrap();
    fs::write(root.join("shape/child"), b"directory child\n").unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["new", "-m", "selected directory child changes"]);
    fs::write(root.join("shape/child"), b"changed directory child\n").unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["new", "-m", "selected directory becomes file"]);
    fs::remove_dir_all(root.join("shape")).unwrap();
    fs::write(root.join("shape"), b"restored file\n").unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["new", "-m", "unrelated entry"]);
    fs::create_dir(root.join("nested")).unwrap();
    fs::write(root.join("nested/new.txt"), b"unrelated entry\n").unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["new", "-m", "distinct fileset paths in one change"]);
    fs::write(root.join("hello.cc"), b"selected mixed change\n").unwrap();
    fs::write(root.join("nested/new.txt"), b"excluded mixed change\n").unwrap();
    fs::write(root.join("nested/UPPER.RS"), b"case-sensitive fileset\n").unwrap();
    fixture.success(&root, &["status"]);
    let mixed_paths = fixture.revision(&root, "@");
    fixture.success(&root, &["new", "-m", "dirty selected path"]);
    fs::write(
        root.join("hello.cc"),
        b"dirty query snapshots this change\n",
    )
    .unwrap();
    let reads = upstream.file_reads();
    let first = fixture.run(
        &root,
        &[
            "log",
            "--no-graph",
            "-r",
            "ancestors(@)",
            "-T",
            "commit_id ++ \"\\n\"",
            "hello.cc",
        ],
    );
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let dirty = fixture.revision(&root, "@");
    let lines = String::from_utf8(first.stdout).unwrap();
    for id in [&dirty, &readded, &deleted, &mode, &modified] {
        assert!(lines.lines().any(|line| line == id));
    }
    assert!(!lines.lines().any(|line| line == unchanged));
    let disconnected = format!("{dirty} | {modified} | main");
    let mut observed = Vec::new();
    for (selection, path, graph) in [
        ("ancestors(main)", "hello.cc", true),
        ("ancestors(@)", "hello.cc", false),
        ("ancestors(@)", "file:hello.cc", true),
        (disconnected.as_str(), "hello.cc", true),
        ("ancestors(@)", "nested", true),
        ("ancestors(@)", "shape", true),
        ("ancestors(@)", "absent.txt", false),
    ] {
        let mut args = vec![
            "--ignore-working-copy",
            "log",
            "-r",
            selection,
            "-T",
            "commit_id ++ \"\\n\"",
            path,
        ];
        if !graph {
            args.push("--no-graph");
        }
        let output = fixture.run(&root, &args);
        assert!(
            output.status.success(),
            "{selection}/{path}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        observed.push((
            args.into_iter().map(str::to_owned).collect::<Vec<_>>(),
            output.stdout,
        ));
    }
    // Distinct exact/prefix selectors must share the same candidate set;
    // nesting and duplicates must not change the existential path semantics.
    for (selectors, graph) in [
        (vec!["file:hello.cc", "nested"], true),
        (vec!["file:hello.cc | (shape | file:hello.cc)"], false),
        (
            vec!["file:hello.cc", "file:hello.cc", "nested/new.txt"],
            false,
        ),
    ] {
        let mut args = vec![
            "--ignore-working-copy",
            "log",
            "-r",
            "ancestors(@)",
            "-T",
            "commit_id ++ \"\\n\"",
        ];
        if !graph {
            args.push("--no-graph");
        }
        args.extend(selectors);
        let output = fixture.run(&root, &args);
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        observed.push((args.into_iter().map(str::to_owned).collect(), output.stdout));
    }
    // Exact-file history through a directory is an existing explicit boundary,
    // including when that selector is one branch of an otherwise supported union.
    let directory_boundary = fixture.run(
        &root,
        &[
            "--ignore-working-copy",
            "log",
            "-r",
            "ancestors(@)",
            "file:hello.cc | file:shape",
        ],
    );
    assert!(!directory_boundary.status.success());
    assert!(directory_boundary.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&directory_boundary.stderr).contains("path_exact_file_directory")
    );
    let many_paths = (0..65).map(|n| format!("absent-{n}")).collect::<Vec<_>>();
    let mut limited_args = vec!["--ignore-working-copy", "log", "-r", "ancestors(@)"];
    limited_args.extend(many_paths.iter().map(String::as_str));
    let limited = fixture.run(&root, &limited_args);
    assert!(
        !limited.status.success(),
        "fileset union must retain AST limits"
    );
    assert!(limited.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&limited.stderr).contains("native index resource limit exceeded")
    );
    // A matcher applies to each changed path, not independently to the commit
    // sets selected by its operands. The mixed commit above changes both paths.
    for (fileset, expected_mixed) in [
        ("hello.cc & nested", false),
        ("hello.cc ~ nested", true),
        ("glob:**/*.rs", false),
        ("glob-i:**/*.rs", true),
    ] {
        let args = [
            "--ignore-working-copy",
            "log",
            "--no-graph",
            "-r",
            "ancestors(@)",
            "-T",
            "commit_id ++ \"\\n\"",
            fileset,
        ];
        let output = fixture.run(&root, &args);
        assert!(
            output.status.success(),
            "{fileset}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let ids = String::from_utf8(output.stdout.clone()).unwrap();
        assert_eq!(
            ids.lines().any(|id| id == mixed_paths),
            expected_mixed,
            "{fileset}: match paths within one commit, including case policy"
        );
        if fileset == "hello.cc & nested" {
            assert!(ids.is_empty(), "disjoint path matchers cannot intersect");
        }
        observed.push((args.into_iter().map(str::to_owned).collect(), output.stdout));
    }
    for (selection, fileset) in [
        ("ancestors(main)", "glob:*.cc"),
        ("ancestors(@)", "glob:**/*.cc"),
        ("ancestors(@)", "glob-i:**/*.CC"),
        ("ancestors(@)", "prefix-glob:shape"),
        ("ancestors(@)", "prefix-glob-i:SHAPE"),
        ("ancestors(@)", "hello.cc | (glob:**/*.txt ~ nested)"),
        ("ancestors(@)", "~hello.cc"),
        ("ancestors(@)", "all()"),
        ("ancestors(@)", "none()"),
    ] {
        let args = [
            "--ignore-working-copy",
            "log",
            "-r",
            selection,
            "-T",
            "commit_id ++ \"\\n\"",
            fileset,
        ];
        let output = fixture.run(
            &root,
            &args,
        );
        assert!(
            output.status.success(),
            "{selection}/{fileset}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        observed.push((args.into_iter().map(str::to_owned).collect(), output.stdout));
    }
    let default_args = [
        "--ignore-working-copy",
        "log",
        "-r",
        "ancestors(@)",
        "hello.cc",
    ];
    let default_output = fixture.run(&root, &default_args);
    assert!(default_output.status.success());
    observed.push((
        default_args.into_iter().map(str::to_owned).collect(),
        default_output.stdout,
    ));
    let default_fileset_args = [
        "--ignore-working-copy",
        "log",
        "-n",
        "3",
        "-r",
        "ancestors(@)",
        "glob:**/*.cc ~ nested",
    ];
    let default_fileset = fixture.run(&root, &default_fileset_args);
    assert!(
        default_fileset.status.success(),
        "default fileset display: {}",
        String::from_utf8_lossy(&default_fileset.stderr)
    );
    observed.push((
        default_fileset_args.into_iter().map(str::to_owned).collect(),
        default_fileset.stdout,
    ));
    assert_eq!(
        upstream.file_reads(),
        reads,
        "ordinary path selection must not hydrate historical regular FILEs"
    );
    for (args, expected) in observed {
        let args = args.iter().map(String::as_str).collect::<Vec<_>>();
        assert_eq!(
            expected,
            legacy_annotation_oracle(&mut fixture, &root, &args),
            "{args:?}"
        );
    }
    // An unselected private merge must not force a local ancestor metadata walk.
    fixture.success(&root, &["new", "-r", "@", "-r", "@-"]);
    let public_args = [
        "--ignore-working-copy",
        "log",
        "-r",
        "ancestors(main)",
        "-T",
        "commit_id ++ \"\\n\"",
        "hello.cc",
    ];
    let public = fixture.run(&root, &public_args);
    assert!(
        public.status.success(),
        "{}",
        String::from_utf8_lossy(&public.stderr)
    );
    assert_eq!(
        public.stdout,
        legacy_annotation_oracle(&mut fixture, &root, &public_args)
    );
    let private_merge_args = ["--ignore-working-copy", "log", "-r", "@", "hello.cc"];
    let private_merge = fixture.run(&root, &private_merge_args);
    assert!(
        private_merge.status.success(),
        "{}",
        String::from_utf8_lossy(&private_merge.stderr)
    );
    assert_eq!(
        private_merge.stdout,
        legacy_annotation_oracle(&mut fixture, &root, &private_merge_args)
    );

    // Two independent line edits merge cleanly at the selected paths while an
    // unrelated file remains unresolved. Neither exact nor prefix selection may
    // hydrate that unrelated FILE or turn the clean merge into a path change.
    fixture.success(&root, &["new", "-r", "main", "-m", "path merge base"]);
    fs::write(root.join("hello.cc"), b"base\nanchor\nlast\n").unwrap();
    fs::create_dir(root.join("selected")).unwrap();
    fs::write(root.join("selected/child"), b"base\nanchor\nlast\n").unwrap();
    fs::write(root.join("unrelated.txt"), b"unrelated base\n").unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["bookmark", "set", "path-merge-base", "-r", "@"]);
    fixture.success(
        &root,
        &["new", "-r", "path-merge-base", "-m", "path merge left"],
    );
    for path in ["hello.cc", "selected/child"] {
        fs::write(root.join(path), b"left\nanchor\nlast\n").unwrap();
    }
    fs::write(root.join("unrelated.txt"), b"unrelated left\n").unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["bookmark", "set", "path-merge-left", "-r", "@"]);
    fixture.success(
        &root,
        &["new", "-r", "path-merge-base", "-m", "path merge right"],
    );
    for path in ["hello.cc", "selected/child"] {
        fs::write(root.join(path), b"base\nanchor\nright\n").unwrap();
    }
    fs::write(root.join("unrelated.txt"), b"unrelated right\n").unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["bookmark", "set", "path-merge-right", "-r", "@"]);
    // Historical unresolved candidates are supported without projecting their
    // conflicted root into the working copy. Keep the mounted WC resolved.
    fixture.success(
        &root,
        &[
            "new",
            "--no-edit",
            "-r",
            "path-merge-left",
            "-r",
            "path-merge-right",
            "-m",
            "clean selected path merge",
        ],
    );
    fixture.success(
        &root,
        &[
            "bookmark",
            "set",
            "path-merge-clean",
            "-r",
            "heads(path-merge-left:: & path-merge-right::)",
        ],
    );
    let clean = fixture.revision(&root, "path-merge-clean");
    assert_eq!(
        fixture
            .success(
                &root,
                &["file", "show", "-r", "path-merge-clean", "hello.cc"]
            )
            .as_bytes(),
        b"left\nanchor\nright\n"
    );
    fixture.success(
        &root,
        &[
            "new",
            "-r",
            "path-merge-base",
            "-m",
            "resolved selected edit source",
        ],
    );
    for path in ["hello.cc", "selected/child"] {
        fs::write(root.join(path), b"edited\nanchor\nright\n").unwrap();
    }
    fixture.success(&root, &["status"]);
    fixture.success(
        &root,
        &["bookmark", "set", "path-merge-edit-source", "-r", "@"],
    );
    fixture.success(
        &root,
        &[
            "new",
            "--no-edit",
            "-r",
            "path-merge-left",
            "-r",
            "path-merge-right",
            "-m",
            "explicit selected path merge edit",
        ],
    );
    fixture.success(
        &root,
        &[
            "bookmark",
            "set",
            "path-merge-edit",
            "-r",
            "heads(path-merge-left:: & path-merge-right::) ~ path-merge-clean",
        ],
    );
    // Rewrite only the selected paths into the unresolved historical commit;
    // the unrelated conflict remains and the resolved source stays checked out.
    fixture.success(
        &root,
        &[
            "restore",
            "--from",
            "path-merge-edit-source",
            "--into",
            "path-merge-edit",
            "hello.cc",
            "selected/child",
        ],
    );
    let edited = fixture.revision(&root, "path-merge-edit");
    let sibling_selection = format!("{clean} | {edited}");
    assert_eq!(
        fixture.revision(&root, "@"),
        fixture.revision(&root, "path-merge-edit-source"),
        "historical conflict construction must leave the resolved source checked out"
    );
    assert_eq!(
        fixture.success(
            &root,
            &[
                "--ignore-working-copy",
                "log",
                "--no-graph",
                "-r",
                &sibling_selection,
                "-T",
                "if(conflict, \"conflict\\n\", \"resolved\\n\")"
            ]
        ),
        "conflict\nconflict\n",
        "both historical candidates must retain the unrelated unresolved conflict"
    );
    // Merging the sibling merges requires recursively merging their TWO common
    // bases (left/right), rather than choosing one base or OR-ing parent diffs.
    fixture.success(
        &root,
        &[
            "new",
            "--no-edit",
            "-r",
            "path-merge-clean",
            "-r",
            "path-merge-edit",
            "-m",
            "recursive common-base path merge",
        ],
    );
    fixture.success(
        &root,
        &[
            "bookmark",
            "set",
            "path-merge-cc",
            "-r",
            "heads(path-merge-clean:: & path-merge-edit::)",
        ],
    );
    let joined = fixture.revision(&root, "path-merge-cc");
    fixture.success(
        &root,
        &[
            "new",
            "--no-edit",
            "-r",
            "path-merge-clean",
            "-r",
            "path-merge-edit",
            "-m",
            "resolve only previously unrelated path",
        ],
    );
    fixture.success(
        &root,
        &[
            "bookmark",
            "set",
            "path-merge-resolution",
            "-r",
            "heads(path-merge-clean:: & path-merge-edit::) ~ path-merge-cc",
        ],
    );
    fixture.success(
        &root,
        &[
            "restore",
            "--from",
            "path-merge-edit-source",
            "--into",
            "path-merge-resolution",
            "unrelated.txt",
        ],
    );
    let resolved = fixture.revision(&root, "path-merge-resolution");
    let selection = format!("{clean} | {edited} | {joined} | {resolved}");
    let selected_files: HashSet<_> = [
        b"base\nanchor\nlast\n".as_slice(),
        b"left\nanchor\nlast\n",
        b"base\nanchor\nright\n",
        b"left\nanchor\nright\n",
        b"edited\nanchor\nright\n",
    ]
    .into_iter()
    .map(|bytes| m::file_id(bytes).0)
    .collect();
    let unrelated_files: HashSet<_> = [
        b"unrelated base\n".as_slice(),
        b"unrelated left\n",
        b"unrelated right\n",
    ]
    .into_iter()
    .map(|bytes| m::file_id(bytes).0)
    .collect();
    let mut merge_observed = Vec::new();
    for published in [false, true] {
        if published {
            // Retain the sibling merges, recursive-base join, and explicit
            // resolution in one public closure without changing selected IDs.
            fixture.success(
                &root,
                &[
                    "new",
                    "--no-edit",
                    "-r",
                    "path-merge-cc",
                    "-r",
                    "path-merge-resolution",
                    "-m",
                    "publish path merge fixture",
                ],
            );
            fixture.success(
                &root,
                &[
                    "bookmark",
                    "set",
                    "main",
                    "-r",
                    "heads(path-merge-cc:: & path-merge-resolution::)",
                ],
            );
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
        }
        let stage_public = upstream.public();
        let stage_writes = upstream.metadata.writes.load(Ordering::SeqCst);
        upstream.metadata.file_keys.lock().unwrap().clear();
        let absent_reads = upstream.file_reads();
        let absent_args = [
            "--ignore-working-copy",
            "log",
            "--no-graph",
            "-r",
            &selection,
            "-T",
            "commit_id ++ \"\\n\"",
            "absent-selected-path",
        ];
        let absent = fixture.run(&root, &absent_args);
        assert!(
            absent.status.success(),
            "{}",
            String::from_utf8_lossy(&absent.stderr)
        );
        assert!(absent.stdout.is_empty());
        assert_eq!(
            upstream.file_reads(),
            absent_reads,
            "unmatched private/public merges must not read FILE bodies"
        );
        for selector in [
            "file:hello.cc",
            "selected",
            "glob:*.cc",
            "prefix-glob:selected",
            "glob:*.cc & ~glob:unrelated.*",
            "all() ~ unrelated.txt",
        ] {
            let args = [
                "--ignore-working-copy",
                "log",
                "--no-graph",
                "-r",
                &selection,
                "-T",
                "commit_id ++ \"\\n\"",
                selector,
            ];
            let output = fixture.run(&root, &args);
            assert!(
                output.status.success(),
                "published={published}/{selector}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                output.stdout,
                format!("{edited}\n").as_bytes(),
                "clean automatic merge must be omitted, explicit merge edit included"
            );
            merge_observed.push((
                args.into_iter().map(str::to_owned).collect::<Vec<_>>(),
                output.stdout,
            ));
        }
        let requested = upstream.metadata.file_keys.lock().unwrap();
        let new_requests = requested.clone();
        assert!(
            new_requests.is_subset(&selected_files),
            "only matched FILE values may hydrate: {new_requests:?}"
        );
        assert!(
            new_requests.is_disjoint(&unrelated_files),
            "unrelated unresolved conflict must stay unhydrated"
        );
        drop(requested);
        assert_eq!(
            upstream.metadata.writes.load(Ordering::SeqCst),
            stage_writes,
            "ordinary path queries must not publish authoritative merge scratch"
        );
        assert_eq!(upstream.public(), stage_public);
        // Select the unresolved path itself. Clean/edit preserve equal terms;
        // the recursive join normalizes their raw term layout, which stock
        // counts as a full-term change alongside the explicit resolution.
        let unrelated_args = [
            "--ignore-working-copy",
            "log",
            "--no-graph",
            "-r",
            &selection,
            "-T",
            "commit_id ++ \"\\n\"",
            "file:unrelated.txt",
        ];
        let unrelated = fixture.run(&root, &unrelated_args);
        assert!(
            unrelated.status.success(),
            "published={published}/unresolved: {}",
            String::from_utf8_lossy(&unrelated.stderr)
        );
        merge_observed.push((
            unrelated_args.into_iter().map(str::to_owned).collect(),
            unrelated.stdout,
        ));
        let allowed: HashSet<_> = selected_files.union(&unrelated_files).copied().collect();
        assert!(
            upstream
                .metadata
                .file_keys
                .lock()
                .unwrap()
                .is_subset(&allowed),
            "the selected unresolved query may hydrate only its own FILE terms"
        );
        assert_eq!(
            upstream.metadata.writes.load(Ordering::SeqCst),
            stage_writes
        );
        assert_eq!(upstream.public(), stage_public);
        // Counters above belong solely to native commands, not this eager
        // compatibility oracle which retains identical native commit IDs.
        for (args, expected) in merge_observed.drain(..) {
            let args = args.iter().map(String::as_str).collect::<Vec<_>>();
            let stock = legacy_annotation_oracle(&mut fixture, &root, &args);
            if args.last() == Some(&"file:unrelated.txt") {
                // These two changes are siblings, so their topological tie
                // ordering is unspecified. Compare membership, not tie order.
                let mut actual = String::from_utf8(expected)
                    .unwrap()
                    .lines()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                let mut oracle = String::from_utf8(stock)
                    .unwrap()
                    .lines()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                actual.sort_unstable();
                oracle.sort_unstable();
                assert_eq!(
                    actual, oracle,
                    "unresolved native/stock membership published={published}"
                );
                let mut wanted = vec![joined.clone(), resolved.clone()];
                wanted.sort_unstable();
                assert_eq!(
                    actual, wanted,
                    "stock counts join normalization and explicit resolution"
                );
                assert!(
                    !actual.contains(&clean) && !actual.contains(&edited),
                    "equal unresolved terms in the original sibling merges must be excluded"
                );
            } else {
                assert_eq!(
                    expected, stock,
                    "native/stock mismatch published={published}/{args:?}"
                );
            }
        }
    }
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn ordinary_annotation_unifies_private_and_canonical_merges() {
    let upstream = RemoteServer::start();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("federated");
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("ordinary-merge-annotation");
    fixture.clone_remote(&root);
    fs::write(root.join("hello.cc"), b"base\nanchor\nlast\n").unwrap();
    fixture.success(&root, &["describe", "-m", "annotation base"]);
    fixture.success(&root, &["bookmark", "set", "annotation-base", "-r", "@"]);
    fixture.success(&root, &["new", "-r", "annotation-base"]);
    fs::write(root.join("hello.cc"), b"left\nanchor\nlast\n").unwrap();
    fixture.success(&root, &["describe", "-m", "annotation left"]);
    fixture.success(&root, &["bookmark", "set", "annotation-left", "-r", "@"]);
    fixture.success(&root, &["new", "-r", "annotation-base"]);
    fs::write(root.join("hello.cc"), b"base\nanchor\nright\n").unwrap();
    fixture.success(&root, &["describe", "-m", "annotation right"]);
    fixture.success(&root, &["bookmark", "set", "annotation-right", "-r", "@"]);
    fixture.success(&root, &["new", "-r", "annotation-left", "-r", "annotation-right"]);
    fs::write(root.join("hello.cc"), b"left\nanchor\nright\n").unwrap();
    fixture.success(&root, &["describe", "-m", "annotation public merge"]);
    fixture.success(&root, &["bookmark", "set", "main", "-r", "@"]);
    fixture.success(&root, &["tinytree", "push", "--reference", "main", "--bookmark", "main"]);
    let binding = fixture.binding(&root);
    let observation = remote_observation(&binding).0;
    let public = upstream.public();
    let reads = upstream.file_reads();
    let writes = upstream.metadata.writes.load(Ordering::SeqCst);
    // Every native command below has already snapshotted its target. Keep the
    // stock comparison read-only so its temporary default index cannot append
    // an operation absent from the restored native index association ledger.
    let stock = |fixture: &mut Fixture, args: &[&str]| {
        let args = std::iter::once("--ignore-working-copy").chain(args.iter().copied()).collect::<Vec<_>>();
        legacy_annotation_oracle(fixture, &root, &args)
    };
    let detail = "commit.commit_id() ++ \":\" ++ original_line_number ++ \":\" ++ line_number ++ \":\" ++ first_line_in_hunk ++ \":\" ++ content";
    let public_args = ["file", "annotate", "hello.cc", "-r", "main", "-T", detail];
    let public_output = fixture.run(&root, &public_args);
    assert!(public_output.status.success(), "{}", String::from_utf8_lossy(&public_output.stderr));
    let json: serde_json::Value = serde_json::from_str(&fixture.success(&root, &["tinytree", "blame", "hello.cc", "-r", "main", "--json"])).unwrap();
    assert_eq!(json["policy"], "jj_resolved_fixed_path_v1");
    assert_eq!(json["coverage"], "complete");
    fixture.success(&root, &["new", "-r", "main", "-m", "annotation private child"]);
    fs::write(root.join("hello.cc"), b"private\nleft\nanchor\nright\n").unwrap();
    let private_args = ["file", "annotate", "hello.cc", "-T", detail];
    let private_output = fixture.run(&root, &private_args);
    assert!(private_output.status.success(), "{}", String::from_utf8_lossy(&private_output.stderr));
    fixture.success(&root, &["new", "-m", "annotation dirty child"]);
    fs::write(root.join("hello.cc"), b"private\nleft\nanchor\nright\ndirty\n").unwrap();
    let dirty_output = fixture.run(&root, &private_args);
    assert!(
        dirty_output.status.success(),
        "{}",
        String::from_utf8_lossy(&dirty_output.stderr)
    );
    assert!(
        upstream.metadata.blame_policies.lock().unwrap().is_empty(),
        "canonical and mixed commands share continuation, never segment Blame"
    );

    // Ordinary annotation requires actual continuation support; unsupported
    // vocabulary never selects a different attribution policy or graph.
    upstream.metadata.creation_fault.store(8, Ordering::SeqCst);
    let legacy = fixture.run(
        &root,
        &[
            "file",
            "annotate",
            "hello.cc",
            "-r",
            "annotation-base",
            "-T",
            detail,
        ],
    );
    assert!(!legacy.status.success() && legacy.stdout.is_empty());
    let legacy_merge = fixture.run(&root, &public_args);
    assert!(!legacy_merge.status.success() && legacy_merge.stdout.is_empty());
    assert!(upstream.metadata.blame_policies.lock().unwrap().is_empty());
    for fault in [9, 10, 11] {
        upstream.metadata.creation_fault.store(fault, Ordering::SeqCst);
        let calls = upstream.metadata.blame_policies.lock().unwrap().len();
        let denied = fixture.run(&root, &public_args);
        assert!(!denied.status.success() && denied.stdout.is_empty(), "fault {fault}");
        assert_eq!(upstream.metadata.blame_policies.lock().unwrap().len(), calls, "fault {fault} must not fall back to Blame");
    }
    upstream.metadata.creation_fault.store(0, Ordering::SeqCst);
    assert_eq!(upstream.file_reads(), reads, "no historical/unrelated FILE RPC fallback");
    assert_eq!(upstream.metadata.writes.load(Ordering::SeqCst), writes);
    assert_eq!(upstream.public(), public);
    assert_eq!(remote_observation(&binding).0, observation);
    assert_eq!(public_output.stdout, stock(&mut fixture, &public_args));

    // Repeated lines around an unchanged auto-merge challenge pair-diff
    // composition when stock omits the merge beneath a private child.
    fixture.success(&root, &["new", "-r", "main", "-m", "repeated annotation base"]);
    fs::write(root.join("hello.cc"), b"x\n").unwrap();
    fixture.success(&root, &["describe", "-m", "repeated annotation base"]);
    fixture.success(&root, &["bookmark", "set", "repeated-base", "-r", "@"]);
    fixture.success(&root, &["new"]);
    fs::write(root.join("hello.cc"), b"a\nx\n").unwrap();
    fixture.success(&root, &["describe", "-m", "repeated annotation left"]);
    fixture.success(&root, &["bookmark", "set", "repeated-left", "-r", "@"]);
    let repeated_left = fixture.revision(&root, "@");
    fixture.success(&root, &["new", "-r", "repeated-base"]);
    fs::write(root.join("hello.cc"), b"x\na\n").unwrap();
    fixture.success(&root, &["describe", "-m", "repeated annotation right"]);
    fixture.success(&root, &["bookmark", "set", "repeated-right", "-r", "@"]);
    fixture.success(&root, &["new", "-r", "repeated-left", "-r", "repeated-right", "-m", "unchanged repeated merge"]);
    assert_eq!(fs::read(root.join("hello.cc")).unwrap(), b"a\nx\na\n");
    fixture.success(&root, &["bookmark", "set", "main", "-r", "@"]);
    fixture.success(&root, &["tinytree", "push", "--reference", "main", "--bookmark", "main"]);
    fixture.success(&root, &["new", "-r", "main", "-m", "repeated private child"]);
    fs::write(root.join("hello.cc"), b"x\na\n").unwrap();
    let composed = fixture.run(&root, &private_args);
    assert!(composed.status.success(), "{}", String::from_utf8_lossy(&composed.stderr));
    let oracle = stock(&mut fixture, &private_args);
    assert_eq!(composed.stdout, oracle);
    assert!(String::from_utf8(oracle).unwrap().ends_with(&format!("{repeated_left}:1:2:true:a\n")), "stock maps the repeated line to the first parent's first line, not the merge's trailing line");

    // An unchanged canonical linear node can hide the merge; merely checking
    // baseline.parents.len() must not restore the disproven pair-diff shortcut.
    fixture.success(&root, &["new", "-r", "main", "-m", "unchanged canonical extension"]);
    fixture.success(&root, &["bookmark", "set", "main", "-r", "@"]);
    fixture.success(&root, &["tinytree", "push", "--reference", "main", "--bookmark", "main"]);
    fixture.success(&root, &["new", "-r", "main", "-m", "private after unchanged extension"]);
    fs::write(root.join("hello.cc"), b"x\na\n").unwrap();
    let before_hidden = upstream.file_reads();
    let hidden_merge = fixture.run(&root, &private_args);
    assert!(hidden_merge.status.success(), "{}", String::from_utf8_lossy(&hidden_merge.stderr));
    assert_eq!(upstream.file_reads(), before_hidden, "canonical comparisons never use generic FILE fallback");
    assert_eq!(hidden_merge.stdout, stock(&mut fixture, &private_args));
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn ordinary_annotation_empty_pair_diff_still_follows_global_canonical_ancestry() {
    let upstream = RemoteServer::start();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("federated");
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("private-merge-boundary");
    fixture.clone_remote(&root);
    fs::write(root.join("hello.cc"), b"x\nkeep\nend\n").unwrap();
    fixture.success(&root, &["describe", "-m", "boundary base"]);
    let origin = fixture.revision(&root, "@");
    fixture.success(&root, &["bookmark", "set", "boundary-base", "-r", "@"]);
    fixture.success(&root, &["new"]);
    fs::write(root.join("hello.cc"), b"x\nkeep\nP\nend\n").unwrap();
    fixture.success(&root, &["describe", "-m", "boundary left"]);
    fixture.success(&root, &["bookmark", "set", "boundary-left", "-r", "@"]);
    fixture.success(&root, &["new", "-r", "boundary-base"]);
    fs::write(root.join("hello.cc"), b"keep\nend\n").unwrap();
    fixture.success(&root, &["describe", "-m", "boundary right"]);
    fixture.success(&root, &["bookmark", "set", "boundary-right", "-r", "@"]);
    fixture.success(&root, &["new", "-r", "boundary-left", "-r", "boundary-right", "-m", "unchanged boundary merge"]);
    assert_eq!(fs::read(root.join("hello.cc")).unwrap(), b"keep\nP\nend\n");
    fixture.success(&root, &["bookmark", "set", "main", "-r", "@"]);
    fixture.success(&root, &["tinytree", "push", "--reference", "main", "--bookmark", "main"]);
    fixture.success(&root, &["new", "-r", "main", "-m", "private boundary child"]);
    fs::write(root.join("hello.cc"), b"x\n").unwrap();
    let reads = upstream.file_reads();
    let args = ["file", "annotate", "hello.cc", "-T", r#"commit.commit_id() ++ ":" ++ original_line_number ++ ":" ++ content"#];
    let attributed = fixture.run(&root, &args);
    assert!(attributed.status.success(), "{}", String::from_utf8_lossy(&attributed.stderr));
    assert_eq!(upstream.file_reads(), reads, "no eager FILE fallback");
    let dedicated = fixture.run(&root, &["tinytree", "blame", "hello.cc", "--json"]);
    assert!(dedicated.status.success(), "{}", String::from_utf8_lossy(&dedicated.stderr));
    let json: serde_json::Value = serde_json::from_slice(&dedicated.stdout).unwrap();
    assert_eq!(json["coverage"], "complete");
    assert_eq!(json["lines"][0]["origin"]["commit"], origin);
    let expected = legacy_annotation_oracle(&mut fixture, &root, &args);
    assert_eq!(expected, format!("{origin}:1:x\n").as_bytes());
    assert_eq!(attributed.stdout, expected);
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn ordinary_annotation_private_merge_uses_multiple_canonical_anchors() {
    let upstream = RemoteServer::start();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("federated");
    fixture.start();
    let base=fixture.base().to_owned();
    let root=base.join("private-multiple-anchors");
    fixture.clone_remote(&root);
    fs::write(root.join("hello.cc"),b"x\n").unwrap();
    fixture.success(&root,&["describe","-m","public base"]);
    fixture.success(&root,&["bookmark","set","public-base","-r","@"]);
    fixture.success(&root,&["new","-r","public-base"]);
    fs::write(root.join("hello.cc"),b"a\nx\n").unwrap();
    fixture.success(&root,&["describe","-m","public left"]);
    fixture.success(&root,&["bookmark","set","public-left","-r","@"]);
    let left=fixture.revision(&root,"@");
    fixture.success(&root,&["new","-r","public-base"]);
    fs::write(root.join("hello.cc"),b"x\na\n").unwrap();
    fixture.success(&root,&["describe","-m","public right"]);
    fixture.success(&root,&["bookmark","set","public-right","-r","@"]);
    let right=fixture.revision(&root,"@");
    fixture.success(&root,&["new","-r","public-left","-r","public-right","-m","public merge"]);
    fixture.success(&root,&["bookmark","set","main","-r","@"]);
    fixture.success(&root,&["tinytree","push","--reference","main","--bookmark","main"]);
    fixture.success(&root,&["new","-r","public-left","-m","private left"]);
    fs::write(root.join("hello.cc"),b"private\na\nx\n").unwrap();
    fixture.success(&root,&["describe","-m","private left"]);
    fixture.success(&root,&["bookmark","set","private-left","-r","@"]);
    let private_left=fixture.revision(&root,"@");
    fixture.success(&root,&["new","-r","public-right","-m","private right"]);
    fs::write(root.join("hello.cc"),b"x\na\nright\n").unwrap();
    fixture.success(&root,&["describe","-m","private right"]);
    fixture.success(&root,&["bookmark","set","private-right","-r","@"]);
    let private_right=fixture.revision(&root,"@");
    fixture.success(&root,&["new","-r","private-left","-r","private-right","-m","private resolved merge"]);
    fs::write(root.join("hello.cc"),b"private\nx\na\nright\n").unwrap();
    fixture.success(&root,&["describe","-m","private resolved merge"]);
    let reads=upstream.file_reads();
    let writes=upstream.metadata.writes.load(Ordering::SeqCst);
    let public=upstream.public();
    let args=["file","annotate","hello.cc","-T",r#"commit.commit_id() ++ ":" ++ original_line_number ++ ":" ++ content"#];
    let output=fixture.run(&root,&args);
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    assert_eq!(upstream.file_reads(),reads,"canonical inputs use the exact-path context, not generic FILE");
    assert_eq!(upstream.metadata.writes.load(Ordering::SeqCst),writes,"annotation does not publish private objects");
    assert_eq!(upstream.public(),public);
    let anchors=upstream.metadata.annotation_anchors.lock().unwrap().last().unwrap().clone();
    let to_hex=|bytes:&Vec<u8>|bytes.iter().map(|b|format!("{b:02x}")).collect::<String>();
    let anchors=anchors.iter().map(to_hex).collect::<std::collections::BTreeSet<_>>();
    assert_eq!(anchors,[left,right].into_iter().collect());
    assert!(!anchors.contains(&private_left) && !anchors.contains(&private_right));
    let oracle_args=std::iter::once("--ignore-working-copy").chain(args.iter().copied()).collect::<Vec<_>>();
    assert_eq!(output.stdout,legacy_annotation_oracle(&mut fixture,&root,&oracle_args));
    fixture.success(&base,&["tinytree","unmount",root.to_str().unwrap()]);
    fixture.stop_daemon();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn ordinary_annotate_matches_stock_templates_private_revisions_and_dirty_bytes() {
    // Reuse the 300-line oracle while also crossing the former 256-commit
    // frontier in canonical and private-tail annotation.
    let upstream = RemoteServer::start_search_configured(300, false, None, true);
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("federated");
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("ordinary-annotate");
    fixture.clone_remote(&root);
    let binding = fixture.binding(&root);
    let observation = remote_observation(&binding).0;
    let public = upstream.public();
    let writes = upstream.metadata.writes.load(Ordering::SeqCst);
    let detail = "commit.commit_id() ++ \":\" ++ original_line_number ++ \":\" ++ line_number ++ \":\" ++ first_line_in_hunk ++ \":\" ++ content";
    let baseline_args = ["file", "annotate", "hello.cc", "-r", "main", "-T", detail];
    let nodes_before = upstream.metadata.annotation_nodes.load(Ordering::SeqCst);
    let pages_before = upstream
        .metadata
        .annotation_continues
        .load(Ordering::SeqCst);
    let baseline = fixture.run(&root, &baseline_args);
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stderr)
    );
    assert_eq!(
        upstream.metadata.annotation_nodes.load(Ordering::SeqCst),
        nodes_before,
        "canonical annotation keeps ancestry metadata on the server"
    );
    assert!(
        upstream
            .metadata
            .annotation_continues
            .load(Ordering::SeqCst)
            > pages_before,
        "deep canonical annotation actually continues beyond one page"
    );
    let limited = fixture.run(
        &root,
        &[
            "--config",
            "tinytree.annotation.max-commits=256",
            "file",
            "annotate",
            "hello.cc",
            "-r",
            "main",
            "-T",
            detail,
        ],
    );
    assert!(
        !limited.status.success() && limited.stdout.is_empty(),
        "aggregate exhaustion never emits partial stdout"
    );
    for setting in ["merge.hunk-level=\"word\"", "merge.same-change=\"keep\""] {
        let legacy_calls = upstream.metadata.blame_policies.lock().unwrap().len();
        let frontier_calls = upstream.metadata.annotation_anchors.lock().unwrap().len();
        let rejected = fixture.run(
            &root,
            &[
                "--config", setting, "file", "annotate", "hello.cc", "-r", "main",
            ],
        );
        assert!(!rejected.status.success() && rejected.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&rejected.stderr)
                .contains("requires line/accept merge semantics")
        );
        assert_eq!(
            upstream.metadata.blame_policies.lock().unwrap().len(),
            legacy_calls
        );
        assert_eq!(
            upstream.metadata.annotation_anchors.lock().unwrap().len(),
            frontier_calls
        );
    }
    let default_args = ["file", "annotate", "hello.cc", "-r", "main"];
    let baseline_default = fixture.success(&root, &default_args);
    assert!(!baseline_default.contains("<Error"));
    // Primary oracle is the supported explicit Legacy creation workflow.
    let mut legacy = Fixture::new();
    legacy.configure_remote(&upstream.endpoint);
    legacy.index_mode("legacy");
    legacy.start();
    let legacy_base = legacy.base().to_owned();
    let legacy_root = legacy_base.join("ordinary-annotate-legacy");
    legacy.clone_remote(&legacy_root);
    let legacy_baseline = legacy.run(&legacy_root, &baseline_args);
    assert!(legacy_baseline.status.success());
    assert_eq!(baseline.stdout, legacy_baseline.stdout);
    assert_eq!(
        baseline_default,
        legacy.success(&legacy_root, &default_args)
    );
    let long_args = ["file", "annotate", "a-many.txt", "-r", "main"];
    let before_prefixes = upstream
        .metadata
        .shortest_prefix_replies
        .load(Ordering::SeqCst);
    let long_default = fixture.run(&root, &long_args);
    assert!(
        long_default.status.success(),
        "{}",
        String::from_utf8_lossy(&long_default.stderr)
    );
    assert_eq!(
        long_default.stdout.split_inclusive(|b| *b == b'\n').count(),
        300
    );
    assert!(
        upstream
            .metadata
            .shortest_prefix_replies
            .load(Ordering::SeqCst)
            - before_prefixes
            > 128,
        "default shortest-change formatting must complete beyond one cooperative budget"
    );
    let legacy_long_default = legacy.run(&legacy_root, &long_args);
    assert!(
        legacy_long_default.status.success(),
        "{}",
        String::from_utf8_lossy(&legacy_long_default.stderr)
    );
    assert_eq!(long_default.stdout, legacy_long_default.stdout);
    let long_detail_args = ["file", "annotate", "a-many.txt", "-r", "main", "-T", detail];
    let long_detail = fixture.run(&root, &long_detail_args);
    let legacy_long_detail = legacy.run(&legacy_root, &long_detail_args);
    assert!(long_detail.status.success() && legacy_long_detail.status.success());
    assert_eq!(
        long_detail.stdout, legacy_long_detail.stdout,
        "every canonical origin, line number, hunk flag and content byte matches Legacy"
    );
    assert_eq!(
        fixture
            .run(
                &root,
                &[
                    "file",
                    "annotate",
                    "a-many.txt",
                    "-r",
                    "main",
                    "-T",
                    "content"
                ]
            )
            .stdout,
        b"needle\n".repeat(300)
    );
    let mut initial_dirty = b"ordinary dirty line\n".to_vec();
    initial_dirty.extend_from_slice(&upstream.source_bytes);
    initial_dirty.extend_from_slice(b"dirty\0\xff without newline");
    fs::write(root.join("hello.cc"), &initial_dirty).unwrap();
    fs::write(legacy_root.join("hello.cc"), &initial_dirty).unwrap();
    let primary_args = ["file", "annotate", "hello.cc", "-T", detail];
    let before_primary = upstream.file_reads();
    let native_dirty = fixture.run(&root, &primary_args);
    assert!(
        native_dirty.status.success(),
        "{}",
        String::from_utf8_lossy(&native_dirty.stderr)
    );
    assert_eq!(upstream.file_reads(), before_primary);
    let native_wc = fixture.revision(&root, "@");
    let legacy_dirty = legacy.run(&legacy_root, &primary_args);
    assert!(legacy_dirty.status.success());
    let legacy_wc = legacy.revision(&legacy_root, "@");
    // Only the independently allocated dirty working-copy identity differs.
    // Preserve every canonical committed origin, line index, hunk and byte.
    let normalize = |bytes: &[u8], wc: &str| {
        bytes
            .split_inclusive(|byte| *byte == b'\n')
            .map(|line| {
                if line.starts_with(wc.as_bytes()) {
                    [b"<working-copy>".as_slice(), &line[wc.len()..]].concat()
                } else {
                    line.to_vec()
                }
            })
            .flatten()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        normalize(&native_dirty.stdout, &native_wc),
        normalize(&legacy_dirty.stdout, &legacy_wc)
    );
    legacy.success(
        &legacy_base,
        &["tinytree", "unmount", legacy_root.to_str().unwrap()],
    );
    legacy.stop_daemon();
    fs::write(root.join("hello.cc"), &upstream.source_bytes).unwrap();
    fixture.success(&root, &["status"]);
    let reads = upstream.file_reads();
    fixture.success(&root, &["new", "-m", "ordinary annotate private base"]);
    let mut first = b"repeat\nrepeat\nprivate base\n".to_vec();
    first.extend_from_slice(&upstream.source_bytes);
    fs::write(root.join("hello.cc"), &first).unwrap();
    fixture.success(&root, &["status"]);
    let private_revision = fixture.revision(&root, "@");
    fixture.success(&root, &["new", "-m", "ordinary annotate private child"]);
    let mut dirty = b"repeat\nprivate child\nrepeat\n".to_vec();
    dirty.extend_from_slice(&upstream.source_bytes);
    dirty.extend_from_slice(b"dirty\0\xff without newline");
    fs::write(root.join("hello.cc"), &dirty).unwrap();
    // This ordinary command, not a preceding status/dedicated query, snapshots @.
    let dirty_args = ["file", "annotate", "hello.cc", "-T", detail];
    let dirty_output = fixture.run(&root, &dirty_args);
    assert!(
        dirty_output.status.success(),
        "{}",
        String::from_utf8_lossy(&dirty_output.stderr)
    );
    let dirty_revision = fixture.revision(&root, "@");
    let private_args = [
        "file",
        "annotate",
        "hello.cc",
        "-r",
        &private_revision,
        "-T",
        detail,
    ];
    let private_output = fixture.run(&root, &private_args);
    assert!(
        private_output.status.success(),
        "{}",
        String::from_utf8_lossy(&private_output.stderr)
    );
    let configured = "templates.file_annotate='line_number ++ \":\" ++ content'";
    let configured_args = [
        "--config",
        configured,
        "file",
        "annotate",
        "hello.cc",
        "-r",
        &dirty_revision,
    ];
    let configured_output = fixture.run(&root, &configured_args);
    assert!(configured_output.status.success());
    let precedence_args = [
        "--config",
        configured,
        "file",
        "annotate",
        "hello.cc",
        "-r",
        &dirty_revision,
        "-T",
        detail,
    ];
    let precedence = fixture.run(&root, &precedence_args);
    assert!(precedence.status.success());
    assert_eq!(precedence.stdout, dirty_output.stdout);
    fs::create_dir(root.join("subdir")).unwrap();
    let relative = fixture.run(
        &root.join("subdir"),
        &[
            "file",
            "annotate",
            "../hello.cc",
            "-r",
            &dirty_revision,
            "-T",
            detail,
        ],
    );
    assert!(relative.status.success());
    assert_eq!(relative.stdout, dirty_output.stdout);
    writeln!(
        fs::OpenOptions::new()
            .append(true)
            .open(&fixture.jj_config)
            .unwrap(),
        "\n[aliases]\nattribute=['file','annotate']"
    )
    .unwrap();
    let alias = fixture.run(
        &root,
        &["attribute", "hello.cc", "-r", &dirty_revision, "-T", detail],
    );
    assert!(
        alias.status.success(),
        "{}",
        String::from_utf8_lossy(&alias.stderr)
    );
    assert_eq!(alias.stdout, dirty_output.stdout);
    let output_limited = fixture.run(
        &root,
        &[
            "file",
            "annotate",
            "hello.cc",
            "-r",
            "main",
            "-T",
            "pad_end(33554433, content)",
        ],
    );
    assert!(!output_limited.status.success());
    assert!(
        output_limited.stdout.is_empty(),
        "template output budget must refuse before ordinary stdout"
    );
    assert!(String::from_utf8_lossy(&output_limited.stderr).contains("rendered output"));
    // One raw writer emits many one-byte writes. Its operation/Vec overhead,
    // not just the much smaller payload, must consume the output allowance.
    let raw_limited = fixture.run(
        &root,
        &[
            "file",
            "annotate",
            "hello.cc",
            "-r",
            "main",
            "-T",
            "raw_escape_sequence(pad_end(1048576, \"\"))",
        ],
    );
    assert!(!raw_limited.status.success());
    assert!(raw_limited.stdout.is_empty());
    assert!(String::from_utf8_lossy(&raw_limited.stderr).contains("rendered output"));

    assert_eq!(
        upstream.file_reads(),
        reads,
        "ordinary annotate cannot hydrate historical/unrelated FILEs"
    );
    assert_eq!(upstream.metadata.writes.load(Ordering::SeqCst), writes);
    assert_eq!(upstream.public(), public);
    assert_eq!(remote_observation(&binding).0, observation);
    assert_eq!(
        baseline.stdout,
        legacy_annotation_oracle(&mut fixture, &root, &baseline_args)
    );
    assert_eq!(
        baseline_default.as_bytes(),
        legacy_annotation_oracle(&mut fixture, &root, &default_args)
    );
    assert_eq!(
        private_output.stdout,
        legacy_annotation_oracle(&mut fixture, &root, &private_args)
    );
    let pinned_dirty_args = [
        "--ignore-working-copy",
        "file",
        "annotate",
        "hello.cc",
        "-r",
        &dirty_revision,
        "-T",
        detail,
    ];
    assert_eq!(
        dirty_output.stdout,
        legacy_annotation_oracle(&mut fixture, &root, &pinned_dirty_args)
    );
    assert_eq!(
        configured_output.stdout,
        legacy_annotation_oracle(&mut fixture, &root, &configured_args)
    );
    fs::remove_dir(root.join("subdir")).unwrap();
    fixture.success(&root, &["status"]);
    fixture.success(&root, &["new", "-m", "annotation bounds child"]);
    fs::write(root.join("hello.cc"), b"line\n".repeat(16_385)).unwrap();
    let oversized = fixture.run(&root, &["file", "annotate", "hello.cc"]);
    assert!(!oversized.status.success());
    assert!(oversized.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&oversized.stderr).contains("content limit"),
        "{}",
        String::from_utf8_lossy(&oversized.stderr)
    );
    fs::write(root.join("hello.cc"), &dirty).unwrap();
    fixture.success(&root, &["status"]);
    // A resolved merge with redundant ancestry keeps stock parent semantics;
    // unified annotation must not simplify it to first-parent blame.
    fixture.success(
        &root,
        &[
            "new",
            "-r",
            "@",
            "-r",
            "@-",
            "-m",
            "annotation merge boundary",
        ],
    );
    let before_merge_reads = upstream.file_reads();
    let merge_args = ["file", "annotate", "hello.cc"];
    let merge = fixture.run(&root, &merge_args);
    assert!(
        merge.status.success(),
        "{}",
        String::from_utf8_lossy(&merge.stderr)
    );
    assert_eq!(upstream.file_reads(), before_merge_reads);
    assert_eq!(
        merge.stdout,
        legacy_annotation_oracle(&mut fixture, &root, &merge_args)
    );
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();

    let limited_upstream = RemoteServer::start_with_blame_limit(1);
    let mut limited = Fixture::new();
    limited.configure_remote(&limited_upstream.endpoint);
    limited.index_mode("federated");
    limited.start();
    let limited_base = limited.base().to_owned();
    let limited_root = limited_base.join("ordinary-annotate-limit");
    limited.clone_remote(&limited_root);
    let rejected = limited.run(
        &limited_root,
        &["file", "annotate", "hello.cc", "-r", "main"],
    );
    assert!(!rejected.status.success());
    assert!(rejected.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("limit exceeded"),
        "{}",
        String::from_utf8_lossy(&rejected.stderr)
    );
    assert!(
        limited_upstream
            .metadata
            .blame_policies
            .lock()
            .unwrap()
            .is_empty(),
        "a bounded attribution failure never falls back to one-shot Blame"
    );
    assert_eq!(limited_upstream.file_reads(), 0);
    limited.success(
        &limited_base,
        &["tinytree", "unmount", limited_root.to_str().unwrap()],
    );
    limited.stop_daemon();
}

fn materialized_search_files(
    store: &tinytree_store::Store,
    head: &str,
) -> BTreeMap<String, Vec<u8>> {
    fn walk(
        store: &tinytree_store::Store,
        tree: m::TreeId,
        prefix: &str,
        files: &mut BTreeMap<String, Vec<u8>>,
    ) {
        let (_, bytes) = store
            .read_metadata(
                "example",
                m::ObjectKey {
                    kind: m::Kind::Tree,
                    id: m::ObjectId(tree.0),
                },
            )
            .unwrap();
        let m::Object::Tree(tree) = m::decode(&bytes, m::Limits::default()).unwrap() else {
            unreachable!()
        };
        for entry in tree.entries {
            let path = if prefix.is_empty() {
                entry.name
            } else {
                format!("{prefix}/{}", entry.name)
            };
            match entry.value {
                m::TreeValue::Tree(id) => walk(store, id, &path, files),
                m::TreeValue::File { id, .. } => {
                    let mut lease = store
                        .read(
                            "example",
                            m::ObjectKey {
                                kind: m::Kind::File,
                                id: m::ObjectId(id.0),
                            },
                        )
                        .unwrap();
                    let mut bytes = Vec::new();
                    lease.read_to_end(&mut bytes).unwrap();
                    files.insert(path, bytes);
                }
                _ => {}
            }
        }
    }
    let id = m::ObjectId(*blake3::Hash::from_hex(head).unwrap().as_bytes());
    let (_, bytes) = store
        .read_metadata(
            "example",
            m::ObjectKey {
                kind: m::Kind::Commit,
                id,
            },
        )
        .unwrap();
    let m::Object::Commit(commit) = m::decode(&bytes, m::Limits::default()).unwrap() else {
        unreachable!()
    };
    let mut files = BTreeMap::new();
    walk(store, commit.root_tree.terms[0], "", &mut files);
    files
}
fn naive_search(
    files: &BTreeMap<String, Vec<u8>>,
    pattern: &[u8],
) -> Vec<(String, String, u64, u64, u64)> {
    let mut hits = Vec::new();
    for (path, bytes) in files {
        let mut offset = 0;
        while offset + pattern.len() <= bytes.len() {
            if bytes[offset..].starts_with(pattern) {
                let line = bytes[..offset].iter().filter(|&&b| b == b'\n').count() + 1;
                let column = bytes[..offset]
                    .iter()
                    .rposition(|&b| b == b'\n')
                    .map_or(offset, |at| offset - at - 1);
                hits.push((
                    path.clone(),
                    blake3::Hash::from(m::file_id(bytes).0).to_hex().to_string(),
                    offset as u64,
                    line as u64,
                    column as u64,
                ));
                offset += pattern.len();
            } else {
                offset += 1;
            }
        }
    }
    hits
}
fn search_rows(result: &serde_json::Value) -> Vec<(String, String, u64, u64, u64)> {
    result["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| {
            (
                hit["path"].as_str().unwrap().to_owned(),
                hit["file_id"].as_str().unwrap().to_owned(),
                hit["byte_offset"].as_u64().unwrap(),
                hit["line"].as_u64().unwrap(),
                hit["byte_column"].as_u64().unwrap(),
            )
        })
        .collect()
}
#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn shipped_indexed_search_reconciles_selected_private_and_dirty_trees() {
    let upstream = RemoteServer::start_with_search();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("federated");
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("indexed-search");
    fixture.clone_remote(&root);
    let binding = fixture.binding(&root);
    assert_eq!(
        fs::read_to_string(binding.repo.join("index/type")).unwrap(),
        jj_native_index::NAME
    );
    let observed = remote_observation(&binding).0;
    let public = upstream.public();
    let writes = upstream.metadata.writes.load(Ordering::SeqCst);
    let public_files = materialized_search_files(&upstream.store, &upstream.head);
    let initial: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree", "search", "(need)le", "-r", "main", "--limit", "10000", "--json",
        ],
    ))
    .unwrap();
    assert_eq!(initial["policy"], "regex_bytes_v1");
    assert_eq!(initial["coverage"], "complete");
    assert_eq!(initial["revision"], upstream.head);
    assert_eq!(
        search_rows(&initial),
        naive_search(&public_files, b"needle")
    );
    assert_eq!(
        upstream.file_reads(),
        0,
        "remote search must not download matching or unrelated FILEs into the source client"
    );
    fixture.success(&root, &["new", "-m", "private search changes"]);
    fs::write(root.join("modified.txt"), b"old match removed\n").unwrap();
    fs::remove_file(root.join("deleted.txt")).unwrap();
    fs::rename(root.join("rename.txt"), root.join("renamed.txt")).unwrap();
    fs::remove_file(root.join("shape.txt")).unwrap();
    fs::create_dir(root.join("shape.txt")).unwrap();
    fs::write(
        root.join("shape.txt/nested.txt"),
        b"needle new nested file\n",
    )
    .unwrap();
    fs::remove_dir_all(root.join("swap")).unwrap();
    fs::write(root.join("swap"), b"needle replacement file\n").unwrap();
    fs::write(root.join("new.txt"), b"needle private addition\n").unwrap();
    fs::write(root.join("z-local.txt"), b"needle local late\n".repeat(240)).unwrap();
    fixture.success(&root, &["status"]);
    let older = fixture.revision(&root, "@");
    let mut older_files = public_files.clone();
    older_files.insert("modified.txt".into(), b"old match removed\n".to_vec());
    older_files.remove("deleted.txt");
    let renamed = older_files.remove("rename.txt").unwrap();
    older_files.insert("renamed.txt".into(), renamed.clone());
    older_files.remove("shape.txt");
    older_files.insert(
        "shape.txt/nested.txt".into(),
        b"needle new nested file\n".to_vec(),
    );
    older_files.remove("swap/old.txt");
    older_files.insert("swap".into(), b"needle replacement file\n".to_vec());
    older_files.insert("new.txt".into(), b"needle private addition\n".to_vec());
    older_files.insert("z-local.txt".into(), b"needle local late\n".repeat(240));
    fixture.success(&root, &["new", "-m", "dirty search child"]);
    fs::write(root.join("modified.txt"), b"needle dirty replacement\n").unwrap();
    fs::remove_file(root.join("new.txt")).unwrap();
    fs::remove_file(root.join("keep.txt")).unwrap();
    std::os::unix::fs::symlink("modified.txt", root.join("keep.txt")).unwrap();
    fs::write(
        root.join("binary.bin"),
        b"\0needle\xff binary selected content",
    )
    .unwrap();
    let mut dirty_files = older_files.clone();
    dirty_files.insert(
        "modified.txt".into(),
        b"needle dirty replacement\n".to_vec(),
    );
    dirty_files.remove("new.txt");
    dirty_files.remove("keep.txt"); // Regular-file policy must not follow the symlink.
    dirty_files.insert(
        "binary.bin".into(),
        b"\0needle\xff binary selected content".to_vec(),
    );
    let dirty: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree", "search", "nee[d]le", "--limit", "10000", "--json",
        ],
    ))
    .unwrap();
    assert_eq!(dirty["coverage"], "complete");
    assert_eq!(dirty["revision"], fixture.revision(&root, "@"));
    assert_eq!(search_rows(&dirty), naive_search(&dirty_files, b"needle"));
    let literal: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree", "search", "-F", "needle", "--limit", "10000", "--json",
        ],
    ))
    .unwrap();
    assert_eq!(literal["policy"], "literal_bytes_v1");
    assert_eq!(literal["coverage"], "complete");
    assert_eq!(search_rows(&literal), naive_search(&dirty_files, b"needle"));
    let selected: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree", "search", "needle", "-r", &older, "--limit", "10000", "--json",
        ],
    ))
    .unwrap();
    assert_eq!(selected["coverage"], "complete");
    assert_eq!(selected["revision"], older);
    assert_eq!(
        search_rows(&selected),
        naive_search(&older_files, b"needle")
    );
    let limited: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &["tinytree", "search", "needle", "--limit", "200", "--json"],
    ))
    .unwrap();
    assert_eq!(limited["coverage"], "result_limit");
    assert!(limited["remote_pages"].as_u64().unwrap() >= 2);
    assert_eq!(
        search_rows(&limited),
        naive_search(&dirty_files, b"needle")[..200],
        "late local matches cannot hide earlier remote matches on later pages"
    );
    let prefix: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree",
            "search",
            "needle",
            "--path-prefix",
            "shape.txt",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(prefix["coverage"], "complete");
    assert_eq!(prefix["hits"].as_array().unwrap().len(), 1);
    assert_eq!(prefix["hits"][0]["path"], "shape.txt/nested.txt");
    let empty: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &["tinytree", "search", "absent-search-literal", "--json"],
    ))
    .unwrap();
    assert_eq!(empty["coverage"], "complete");
    assert!(empty["hits"].as_array().unwrap().is_empty());
    let allowed = m::file_id(&renamed).0;
    assert!(
        upstream
            .metadata
            .file_keys
            .lock()
            .unwrap()
            .iter()
            .all(|id| *id == allowed),
        "only renamed selected content may hydrate; untouched public files/assets stay remote"
    );
    assert_eq!(upstream.metadata.writes.load(Ordering::SeqCst), writes);
    assert_eq!(remote_observation(&binding).0, observed);
    assert_eq!(upstream.public(), public);
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
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
    assert!(historical_listing
        .lines()
        .any(|path| path == "history-00.cc"));
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
fn growing_private_history_cloud_profile_is_admissible() {
    let ordinary = RemoteServer::cloud_profile(None);
    ordinary.validate().unwrap();
    let growing = RemoteServer::cloud_profile(Some(RemoteServer::GROWING_PRIVATE_PROOF_PROFILE));
    // Public validation includes Store work plus wire/admission accounting, not
    // merely syntactic bounds or a hand-computed memory estimate.
    growing.validate().unwrap();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn federated_index_production_factory_preserves_operations_and_drains_cli_queries() {
    let upstream = RemoteServer::start_with_growing_private_history();
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
    // Grow actual private overlays, not only read-only scopes. Each subprocess
    // must release active work without waiting for the server's absolute TTL.
    // Keep the default scope and replay capacities: tuning them would hide the
    // burst behavior this workflow is intended to exercise.
    let mut generated_changes = HashSet::new();
    for draft_number in 0..40 {
        let description = format!("private replay draft {draft_number}");
        let template = "commit_id ++ \"\\n\" ++ change_id ++ \"\\n\" ++ description";
        let mut generated = None;
        for (phase, args) in [
            ("new", vec!["new", "-m", "private replay draft"]),
            ("generated", vec!["--ignore-working-copy", "log", "--no-graph", "-r", "@", "-T", template]),
            ("describe", vec!["describe", "-m", description.as_str()]),
            ("author", vec!["metaedit", "--force-rewrite", "--author", "Replay Fixture <replay@example.invalid>",
                "--author-timestamp", "2001-02-03T04:05:06Z"]),
            ("verified", vec!["--ignore-working-copy", "log", "--no-graph", "-r", "@", "-T", template]),
        ] {
            let output = fixture.run(&root, &args);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "private draft {draft_number}/{phase}: {stderr}");
            assert!(!stdout.contains("<Error"), "private draft {draft_number}/{phase}: {stdout}");
            assert!(!stderr.contains("cleanup remains pending"), "private draft {draft_number}/{phase}: {stderr}");
            assert!(!stderr.contains("native command drain remains pending"),
                "private draft {draft_number}/{phase}: {stderr}");
            let status = fixture.control(Request::Status).unwrap();
            assert_eq!(status["native"]["commands"]["active_commands"], 0,
                "command fence leaked after private draft {draft_number}/{phase}");
            assert_eq!(status["native"]["commands"]["collection_enabled"], false);
            if phase == "generated" || phase == "verified" {
                let mut metadata = stdout.splitn(3, '\n');
                let commit = metadata.next().unwrap();
                let change = metadata.next().unwrap();
                let observed_description = metadata.next().unwrap();
                assert_eq!(commit.len(), 64);
                assert_eq!(change.len(), 32);
                if phase == "generated" {
                    assert!(generated_changes.insert(change.to_owned()), "new must generate a fresh ChangeID");
                    generated = Some(change.to_owned());
                } else {
                    assert_eq!(Some(change), generated.as_deref(), "metadata rewrites must preserve ChangeID");
                    assert_eq!(observed_description.trim_end(), description);
                }
            }
        }
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
    fixture.capture_policy("after_command_best_effort");
    fixture.start();
    fixture.success(&base, &["tinytree", "resume", root.to_str().unwrap()]);
    fixture.success(&root, &["status"]);
    assert_eq!(fixture.revision(&root, "@"), draft);
    assert_eq!(
        fs::read_to_string(index.join("type")).unwrap(),
        jj_native_index::NAME
    );

    // The queued capture includes newly authored native objects which the
    // target does not already possess, not merely an already-published root.
    fixture.success(&root, &["new", "-m", "capture-only private draft"]);
    let authored_bytes = b"private captured workspace file\n";
    fs::write(root.join("capture-only.txt"), authored_bytes).unwrap();
    fixture.success(&root, &["status"]);
    let authored_commit = m::ObjectKey {
        kind: m::Kind::Commit,
        id: m::ObjectId(
            *blake3::Hash::from_hex(&fixture.revision(&root, "@"))
                .unwrap()
                .as_bytes(),
        ),
    };
    let authored_file = m::describe_file(authored_bytes);
    assert!(upstream
        .store
        .descriptor("example", authored_commit)
        .unwrap()
        .is_none());
    assert!(upstream
        .store
        .descriptor("example", authored_file.key)
        .unwrap()
        .is_none());
    let public_before_upload = upstream.public();
    let captured = local_capture_status(&fixture);
    assert_eq!(captured["policy"], "after_command_best_effort");
    assert!(
        captured["queued"].as_u64().unwrap() > 0,
        "a successful pristine federated command must queue an actual host-sealed capture: {captured}"
    );
    let pending = captured["pending_count"].as_u64().unwrap();
    let pending_bytes = captured["pending_bytes"].as_u64().unwrap();
    assert!(pending > 0 && pending_bytes > 0);
    assert_eq!(captured["inventory_available"], true);
    assert_eq!(captured["pending_dirty"].as_u64().unwrap(), pending);
    assert_eq!(
        captured["pending_clean"], 0,
        "local staging does not acknowledge remote backup"
    );
    assert_eq!(captured["pending_unavailable"], 0);
    assert_eq!(
        upstream.file_reads(),
        0,
        "local capture must not hydrate source FILE bodies"
    );
    let bundles = local_capture_bundles(&fixture);
    assert!(!bundles.is_empty());
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
    fixture.start();
    let reopened = local_capture_status(&fixture);
    assert_eq!(reopened["pending_count"].as_u64().unwrap(), pending);
    assert_eq!(reopened["pending_bytes"].as_u64().unwrap(), pending_bytes);
    assert_eq!(reopened["pending_dirty"].as_u64().unwrap(), pending);
    assert_eq!(reopened["pending_clean"], 0);
    assert_eq!(reopened["inventory_available"], true);
    assert_eq!(
        local_capture_bundles(&fixture),
        bundles,
        "pending immutable local bundles survive daemon restart before another command"
    );
    fixture.success(&base, &["tinytree", "resume", root.to_str().unwrap()]);

    let before_fallback = local_capture_status(&fixture);
    let dirty = root.join("capture-untracked.txt");
    fs::write(&dirty, b"unsnapshotted source bytes\n").unwrap();
    let output = fixture.run(&root, &["--ignore-working-copy", "log", "-n", "1"]);
    assert!(
        output.status.success(),
        "best-effort capture must preserve successful jj command: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("<Error"));
    let fallback = local_capture_status(&fixture);
    assert!(
        fallback["attempted"].as_u64().unwrap() > before_fallback["attempted"].as_u64().unwrap()
    );
    assert_eq!(fallback["queued"], before_fallback["queued"]);
    assert_eq!(fallback["last_disposition"], "candidate_unavailable");
    assert_eq!(fallback["pending_count"], before_fallback["pending_count"]);
    assert_eq!(fallback["pending_clean"], 0);
    assert_eq!(upstream.file_reads(), 0);
    fs::remove_file(dirty).unwrap();

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
    // Keep a genuine concurrent operation head without letting another normal
    // source command reconcile it before capture. This changes metadata only;
    // the active working copy still contains the newly authored file above.
    fixture.success(
        &root,
        &[
            "--at-op",
            &operation,
            "describe",
            "-m",
            "parallel retained operation",
        ],
    );
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    assert!(native_operation_heads(&binding).len() >= 2);
    let queued_before_upload = local_capture_status(&fixture);
    let packs_before_upload = local_capture_bundles(&fixture);
    fixture.stop_daemon();
    fixture.enable_capture_upload();
    let upload_benchmark = (std::env::var("TINYTREE_CAPTURE_UPLOAD_BENCH").as_deref() == Ok("1"))
        .then(|| (upstream.startup_counters(), Instant::now()));
    fixture.start();
    let daemon_ready = Instant::now();
    let expected_publications = queued_before_upload["pending_count"].as_u64().unwrap();
    let published = wait_for_capture_publications(&fixture, expected_publications);
    // Snapshot before verification adds any traffic. Reporting remains optional
    // and uses the existing readiness/publication observations, not extra RPCs.
    let upload_measurement = upload_benchmark.map(|(before, started)| {
        let observed = Instant::now();
        (
            started,
            daemon_ready,
            observed,
            upstream.startup_counters().subtract(before),
        )
    });
    assert_eq!(published["local_roots_retained"], true);
    assert_eq!(published["current_backup_availability_verified"], false);
    assert_eq!(published["last_disposition"], "published_roots_retained");
    assert_eq!(
        upstream.public(),
        public_before_upload,
        "saved capture upload must not move a public native reference"
    );
    assert!(upstream
        .store
        .descriptor("example", authored_commit)
        .unwrap()
        .is_some());
    assert_eq!(
        upstream
            .store
            .descriptor("example", authored_file.key)
            .unwrap(),
        Some(authored_file)
    );
    let mut uploaded_file = Vec::new();
    upstream
        .store
        .read("example", authored_file.key)
        .unwrap()
        .read_to_end(&mut uploaded_file)
        .unwrap();
    assert_eq!(uploaded_file, authored_bytes);
    let restore_capture = verify_uploaded_capture(
        &upstream,
        &fixture,
        &binding,
        &packs_before_upload,
        expected_publications,
    );
    let retained = local_capture_status(&fixture);
    for field in [
        "pending_count",
        "pending_bytes",
        "pending_dirty",
        "pending_clean",
        "pending_root_claims",
        "charged_spool_bytes",
    ] {
        assert_eq!(
            retained[field], queued_before_upload[field],
            "publication must retain local {field}"
        );
    }
    assert_eq!(
        local_capture_bundles(&fixture),
        packs_before_upload,
        "upload cannot remove or rewrite the private local pack"
    );
    let upload_directory = fixture
        .socket
        .parent()
        .unwrap()
        .join("native-commands/upload");
    let immutable_jobs = files_in(&upload_directory);
    assert_eq!(
        immutable_jobs
            .keys()
            .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("published"))
            .count() as u64,
        expected_publications
    );
    fixture.stop_daemon();
    fixture.start();
    // Daemon readiness does not wait for the independently owned background
    // catalog scan. Observe that durable inventory before comparing its count.
    let resumed = wait_for_capture_publications(&fixture, expected_publications);
    assert_eq!(
        resumed["historical_published_count"],
        published["historical_published_count"]
    );
    assert_eq!(
        resumed["attempted"], 0,
        "published jobs must not be uploaded again"
    );
    assert_eq!(
        files_in(&upload_directory),
        immutable_jobs,
        "restart must preserve original durable publication intents and receipts"
    );
    assert_eq!(local_capture_bundles(&fixture), packs_before_upload);
    assert_eq!(
        local_capture_status(&fixture)["pending_count"],
        queued_before_upload["pending_count"]
    );
    fixture.stop_daemon();

    // A separate daemon has no source pack, native objects or shared FILE
    // cache. Restore must collect the selected retained state from the service.
    // The expanded retained-history restore is allowed its existing product
    // deadline plus bounded completion grace. This is not a latency assertion;
    // ordinary CLI calls and the smaller restore fixture retain thirty seconds.
    restore_captured_workspace_with_watchdog(
        &upstream,
        &restore_capture,
        authored_bytes,
        Duration::from_millis(native_workspace_admin::restore::RestoreLimits::default().timeout_ms)
            + Duration::from_secs(15),
    );
    assert_eq!(upstream.public(), public_before_upload);
    assert_eq!(local_capture_bundles(&fixture), packs_before_upload);

    let admin_before_refusal = native_admin_state(&binding);
    let index_before_refusal = native_metadata_snapshot(&index, |_| true);
    let workspace_metadata = fixture.base().join("daemon/native-workspaces");
    let workspace_before_refusal = native_metadata_snapshot(&workspace_metadata, |relative| {
        relative.components().count() == 2
            && relative.file_name() == Some(std::ffi::OsStr::new("workspace.json"))
    });
    let mapping_metadata = fixture.base().join("daemon/native-commands/records");
    let mappings_before_refusal = native_metadata_snapshot(&mapping_metadata, |_| true);
    let mut config: toml::Value =
        toml::from_str(&fs::read_to_string(&fixture.daemon_config).unwrap()).unwrap();
    // A changed provider configuration changes the live fingerprint. Refuse
    // before provider opening, rather than waiting for a later resume request.
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
    fixture.expect_provider_startup_refusal();
    assert!(native_admin_state(&binding) == admin_before_refusal,
        "startup refusal must preserve native administration bytes");
    assert!(native_metadata_snapshot(&index, |_| true) == index_before_refusal,
        "startup refusal must preserve the complete index and namespace");
    assert!(native_metadata_snapshot(&workspace_metadata, |relative| {
        relative.components().count() == 2
            && relative.file_name() == Some(std::ffi::OsStr::new("workspace.json"))
    }) == workspace_before_refusal,
        "startup refusal must preserve workspace records and namespace");
    assert!(native_metadata_snapshot(&mapping_metadata, |_| true) == mappings_before_refusal,
        "startup refusal must preserve command mappings and namespace");
    assert_eq!(
        fs::read_to_string(index.join("type")).unwrap(),
        jj_native_index::NAME
    );
    assert!(index.join("manifest").is_file());
    if let Some((started, ready, observed, counters)) = upload_measurement {
        // Emit only after the full semantic, retention and restart proofs pass.
        // These are local-loopback backlog measurements, not wire byte counts
        // or isolated upload latency; source caches reflect preceding commands.
        eprintln!(
            "{}",
            serde_json::json!({
                "schema":1,"benchmark":"native_capture_upload_backlog",
                "measurement":true,"status":"completed",
                "workspace_provider":"actual_linux_fuse","transport":"authenticated_zstd_loopback",
                "source_metadata_cache":"warm_after_commands",
                "daemon_process":"restarted_with_existing_private_state",
                "target":"base_history_present_authored_draft_file_missing",
                "queued_capture_count":expected_publications,
                "pending_capture_accounted_bytes":queued_before_upload["pending_bytes"],
                "pending_root_claims":queued_before_upload["pending_root_claims"],
                "local_pack_encoded_bytes":packs_before_upload.values().map(Vec::len).sum::<usize>(),
                "known_new_authored_file_bytes":authored_bytes.len(),
                "daemon_start_observed_ns":ready.duration_since(started).as_nanos(),
                "daemon_ready_to_all_published_observed_ns":observed.duration_since(ready).as_nanos(),
                "startup_to_all_published_observed_ns":observed.duration_since(started).as_nanos(),
                "source_read_counters":counters,
                "source_read_counters_exclude_upload_and_cloud_rpcs":true,
                "daemon_readiness_poll_ms":20,"publication_observation_poll_ms":50,"uploader_poll_ms":250,
                "every_retained_capture_verified":true,"restart_verified":true,
                "public_refs_unchanged":true,"local_roots_and_packs_retained":true,
                "not_wan_wire_bytes_rss_or_bazel_startup":true
            })
        );
    }
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn parallel_native_workspaces_keep_backing_up_beyond_capture_capacity() {
    let upstream = RemoteServer::start_with_cloud_capture_capacity_and_history(64);
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    // Match the upstream replay share for this rapid single-operator workload.
    // Active scope/query limits and registry memory remain unchanged.
    fixture.query_replay_capacity(1024);
    fixture.automatic_policies();
    // Keep the policy keys omitted. Only this stress fixture's queue bounds and
    // polling interval differ from the ordinary automatic profile.
    let mut config: toml::Value =
        toml::from_str(&fs::read_to_string(&fixture.daemon_config).unwrap()).unwrap();
    config["native"]["workspaces"]
        .as_table_mut()
        .unwrap()
        .insert(
            "capture".into(),
            toml::Value::Table(toml::map::Map::from_iter([(
                "max_pending".into(),
                4.into(),
            )])),
        );
    config["native"]["workspaces"]
        .as_table_mut()
        .unwrap()
        .insert(
            "upload".into(),
            toml::Value::Table(toml::map::Map::from_iter([("poll_ms".into(), 250.into())])),
        );
    fs::write(&fixture.daemon_config, toml::to_string(&config).unwrap()).unwrap();
    fixture.start();
    let base = fixture.base().to_owned();
    let roots = [base.join("agent-a"), base.join("agent-b")];
    for root in &roots {
        fixture.clone_remote(root);
    }
    let bindings = roots.each_ref().map(|root| fixture.binding(root));
    assert_ne!(bindings[0].workspace_id, bindings[1].workspace_id);
    assert_ne!(bindings[0].repo, bindings[1].repo);
    for binding in &bindings {
        assert_eq!(
            fs::read_to_string(binding.repo.join("index/type")).unwrap(),
            jj_native_index::NAME
        );
        let record: serde_json::Value = serde_json::from_slice(
            &fs::read(
                fixture
                    .base()
                    .join("daemon/native-workspaces")
                    .join(binding.id())
                    .join("workspace.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let bytes: Vec<u8> = serde_json::from_value(record["creation_plan"].clone()).unwrap();
        let plan = native_broker::CreationPlan::decode(&bytes).unwrap();
        assert!(plan.policy.auto_capture && plan.policy.auto_upload);
        assert_eq!(plan.index_mode, native_broker::ProviderIndexMode::Federated);
    }
    fn wait_latest(fixture: &Fixture, queued: u64) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let status = fixture.control(Request::Status).unwrap();
            let local = &status["native"]["commands"]["local_capture"];
            let upload = &status["native"]["commands"]["capture_upload"];
            if local["pending_count"] == 2
                && upload["historical_published_count"] == 2
                && upload["cleanup_pending"] == false
                && upload["retired_local_capture_count"].as_u64().unwrap()
                    >= queued.saturating_sub(2)
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "two-workspace backup stalled: {status}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    let first_captures = local_capture_status(&fixture)["queued"].as_u64().unwrap();
    wait_latest(&fixture, first_captures);
    assert_eq!(cloud_generation(&upstream, bindings[0].workspace_id), 1);
    assert_eq!(cloud_generation(&upstream, bindings[1].workspace_id), 1);
    assert_eq!(upstream.historical_commit_reads(), 0);
    assert_eq!(upstream.historical_tree_reads(), 0);
    let retained_holds = upstream.metadata.hold_publications.load(Ordering::SeqCst);
    assert_eq!(
        retained_holds, 1,
        "both clones share their exact retained public anchor"
    );
    for root in &roots {
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    }
    fixture.stop_daemon();
    // A creation-policy change must not reinterpret either saved index or
    // revoke the immutable eligibility of these already enrolled workspaces.
    fixture.index_mode("legacy");
    fixture.start();
    for root in &roots {
        fixture.success(&base, &["tinytree", "resume", root.to_str().unwrap()]);
    }
    for root in &roots {
        fixture.success(root, &["status"]);
    }
    let public = upstream.public();
    let initial = local_capture_status(&fixture)["queued"].as_u64().unwrap();
    wait_latest(&fixture, initial);
    let first: workspace_daemon::control::NativeCloudWorkspaces =
        serde_json::from_str(&fixture.cloud(&base, &["workspaces", "--limit", "1", "--json"]))
            .unwrap();
    assert_eq!(first.workspaces.len(), 1);
    assert!(!first.continuation.is_empty());
    let second: workspace_daemon::control::NativeCloudWorkspaces =
        serde_json::from_str(&fixture.cloud(
            &base,
            &[
                "workspaces",
                "--limit",
                "1",
                "--cursor",
                &first.continuation,
                "--json",
            ],
        ))
        .unwrap();
    assert_eq!(second.workspaces.len(), 1);
    assert!(second.continuation.is_empty());
    let listed: std::collections::BTreeSet<_> = first
        .workspaces
        .into_iter()
        .chain(second.workspaces)
        .map(|row| {
            let expected = bindings
                .iter()
                .find(|binding| capture_identity(&binding.workspace_id) == row.workspace_id)
                .unwrap();
            assert_eq!(
                row.local_destination.as_deref(),
                Some(expected.root.as_path())
            );
            row.workspace_id
        })
        .collect();
    assert_eq!(listed.len(), 2);
    assert_eq!(
        local_capture_status(&fixture)["queued"],
        initial,
        "cloud discovery must not snapshot a source workspace"
    );
    assert!(fixture
        .control(Request::NativeCloudWorkspaces {
            store: "example".into(),
            page_size: 1,
            continuation: String::new(),
        })
        .unwrap_err()
        .to_string()
        .contains("scoped"));
    let mut expected = initial;
    // 70 distinct edits across TWO independent agents exceed both the tiny
    // four-ticket fixture limit and the ordinary 64-ticket default.
    for round in 0..35 {
        for (agent, root) in roots.iter().enumerate() {
            fs::create_dir_all(root.join("private/nested")).unwrap();
            fs::write(
                root.join("private/nested/agent.txt"),
                format!("private agent {agent} iteration {round}\n"),
            )
            .unwrap();
        }
        let first = fixture.spawn_cli(&roots[0], &["status"]);
        let second = fixture.spawn_cli(&roots[1], &["status"]);
        for output in [first.finish(), second.finish()] {
            assert!(
                output.status.success(),
                "parallel native command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(!String::from_utf8_lossy(&output.stderr)
                .contains("native command drain remains pending"));
        }
        expected += 2;
        let local = local_capture_status(&fixture);
        assert_eq!(
            local["queued"].as_u64().unwrap(),
            expected,
            "each successful independent command must retain its distinct edited snapshot: {local}"
        );
        assert!(local["pending_count"].as_u64().unwrap() <= 4);
        assert_eq!(local["pending_clean"], 0);
        wait_latest(&fixture, expected);
        assert_eq!(local_capture_bundles(&fixture).len(), 2);
        for (agent, root) in roots.iter().enumerate() {
            assert_eq!(
                fs::read_to_string(root.join("private/nested/agent.txt")).unwrap(),
                format!("private agent {agent} iteration {round}\n")
            );
        }
        assert_eq!(upstream.public(), public);
        assert_eq!(upstream.file_reads(), 0);
        assert_eq!(
            upstream.unrelated_historical_commit_reads(),
            0,
            "status and automatic backup must not walk unrelated public history"
        );
        assert_eq!(upstream.historical_tree_reads(), 0);
        assert_eq!(
            upstream.metadata.hold_publications.load(Ordering::SeqCst),
            retained_holds,
            "capture publication must not create per-command retention holds"
        );
    }
    assert!(expected >= 70);
    assert!(cloud_generation(&upstream, bindings[0].workspace_id) >= 35);
    assert!(cloud_generation(&upstream, bindings[1].workspace_id) >= 35);
    for root in &roots {
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    }
    fixture.stop_daemon();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn automatic_remote_creation_requires_live_features_before_destination_or_intent() {
    let upstream = RemoteServer::start(); // query available, captured cloud disabled
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.automatic_policies();
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("unsupported-auto");
    let broker = fixture.broker.clone();
    let args = [
        "tinytree",
        "clone",
        root.to_str().unwrap(),
        "--broker",
        broker.to_str().unwrap(),
        "--store",
        "example",
        "--reference",
        "main",
    ];
    let output = fixture.run(&base, &args);
    assert!(!output.status.success());
    assert!(
        !root.exists(),
        "failed live preflight must precede destination creation"
    );
    assert!(
        !fixture.registry.exists(),
        "failed live preflight must precede client intent creation"
    );
    assert_eq!(upstream.file_reads(), 0);
    // A caller-visible plan is selection data, not a bearer proof. Bypassing
    // the CLI still requires the host's own authenticated feature/root check.
    let query = upstream.runtime.as_ref().unwrap().block_on(async {
        native_broker::BrokerQueryClient::connect(
            fixture.broker.clone(),
            "example",
            Default::default(),
        )
        .await
        .unwrap()
    });
    let description = upstream
        .runtime
        .as_ref()
        .unwrap()
        .block_on(query.describe_provider())
        .unwrap();
    let root_id: [u8; 32] = (0..32)
        .map(|i| u8::from_str_radix(&upstream.head[i * 2..i * 2 + 2], 16).unwrap())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    let plan = description.creation_plan(root_id).unwrap();
    assert!(fixture
        .control(Request::NativeMountPlanned {
            root: root.clone(),
            store: "example".into(),
            commit: upstream.head.clone(),
            workspace_id: "6a".repeat(32),
            creation_plan: plan.encode(),
        })
        .is_err());
    assert!(!root.exists());
    assert!(!fixture
        .base()
        .join("daemon/native-workspaces")
        .join("6a".repeat(32))
        .exists());
    fixture.stop_daemon();
    // An explicit no-backup policy needs only the query features and remains
    // usable against this server; it is not silently converted into backup.
    fixture.capture_policy("disabled");
    fixture.start();
    fixture.clone_remote(&root);
    let binding = fixture.binding(&root);
    assert_eq!(
        fs::read_to_string(binding.repo.join("index/type")).unwrap(),
        jj_native_index::NAME
    );
    fixture.success(&root, &["status"]);
    assert_eq!(local_capture_status(&fixture)["queued"], 0);
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
    let capable = RemoteServer::start_with_cloud();
    for (name, fault) in [
        ("legacy server", 1),
        ("denied features", 2),
        ("unreadable selected root", 3),
        ("selected root retention denied", 7),
    ] {
        let mut denied = Fixture::new();
        denied.configure_remote(&capable.endpoint);
        denied.automatic_policies();
        denied.start();
        capable
            .metadata
            .creation_fault
            .store(fault, Ordering::SeqCst);
        let denied_root = denied.base().join("denied-destination");
        let broker = denied.broker.clone();
        let base = denied.base().to_owned();
        let result = denied.run(
            &base,
            &[
                "tinytree",
                "clone",
                denied_root.to_str().unwrap(),
                "--broker",
                broker.to_str().unwrap(),
                "--store",
                "example",
                "--reference",
                "main",
            ],
        );
        assert!(!result.status.success(), "{name}");
        assert!(
            !denied_root.exists(),
            "{name}: destination must not be created"
        );
        assert!(
            !denied.registry.exists(),
            "{name}: client intent must not be created"
        );
        denied.stop_daemon();
    }
    capable.metadata.creation_fault.store(0, Ordering::SeqCst);
    // Negotiated features and a caller-visible plan do not create retention
    // authority for a raw nonempty host mount. Normal clone admits its exact
    // public head first; this bypass has deliberately never observed it.
    let mut raw = Fixture::new();
    raw.configure_remote(&capable.endpoint);
    raw.automatic_policies();
    raw.start();
    let base = raw.base().to_owned();
    let destination = base.join("unadmitted-auto");
    let query = capable.runtime.as_ref().unwrap().block_on(async {
        native_broker::BrokerQueryClient::connect(raw.broker.clone(), "example", Default::default())
            .await
            .unwrap()
    });
    let description = capable
        .runtime
        .as_ref()
        .unwrap()
        .block_on(query.describe_provider())
        .unwrap();
    let commit = *blake3::Hash::from_hex(&capable.head).unwrap().as_bytes();
    let plan = description.creation_plan(commit).unwrap();
    assert!(plan.policy.auto_upload);
    let holds = capable.metadata.hold_publications.load(Ordering::SeqCst);
    let preflight = capable
        .runtime
        .as_ref()
        .unwrap()
        .block_on(query.preflight_creation(m::CommitId(commit)));
    assert!(
        preflight.is_err(),
        "missing ready anchor cannot pass automatic preflight"
    );
    assert!(raw
        .control(Request::NativeMountPlanned {
            root: destination.clone(),
            store: "example".into(),
            commit: capable.head.clone(),
            workspace_id: "6b".repeat(32),
            creation_plan: plan.encode(),
        })
        .is_err());
    assert!(!destination.exists());
    assert!(!raw.registry.exists());
    assert!(!raw
        .base()
        .join("daemon/native-workspaces")
        .join("6b".repeat(32))
        .exists());
    assert_eq!(
        capable.metadata.hold_publications.load(Ordering::SeqCst),
        holds,
        "read-only preflight must never create a missing anchor"
    );
    // Intrinsic-root init needs no public anchor. Its entirely local first
    // capture still reaches the configured retained cloud automatically.
    let empty = base.join("intrinsic-auto");
    raw.initialize(&empty);
    let binding = raw.binding(&empty);
    wait_for_capture_publications(&raw, 1);
    assert!(cloud_generation(&capable, binding.workspace_id) >= 1);
    assert_eq!(
        capable.metadata.hold_publications.load(Ordering::SeqCst),
        holds
    );
    raw.success(&base, &["tinytree", "unmount", empty.to_str().unwrap()]);
    raw.stop_daemon();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn pending_landing_cloud_restore_preserves_original_intent_and_applies_over_frozen_index() {
    pending_landing_cold_recovery(None, Some("resolution_recorded"), false, false);
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn pending_landing_cloud_restore_discards_captured_apply_orphan_and_recovers_owned_checkout() {
    pending_landing_cold_recovery(
        Some("apply_written"),
        Some("checkout_applied"),
        false,
        false,
    );
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn pending_landing_cloud_restore_acknowledges_current_new_without_replay() {
    pending_landing_cold_recovery(
        Some("apply_finished"),
        Some("resolution_published"),
        false,
        false,
    );
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn pending_landing_cloud_restore_preserves_acknowledged_operation_without_new_apply() {
    pending_landing_cold_recovery(Some("acknowledged"), None, false, false);
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn pending_landing_cloud_restore_preserves_amended_source_and_dirty_work() {
    pending_landing_cold_recovery(Some("apply_written"), None, true, false);
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn pending_landing_cloud_restore_retries_before_owned_record_without_replaying_orphan() {
    pending_landing_cold_recovery(None, Some("apply_written"), false, false);
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn pending_landing_cloud_restore_retires_terminal_without_source_metadata_or_checkout() {
    pending_landing_cold_recovery(None, None, true, true);
}

fn landing_resolution_records(
    fixture: &Fixture,
    binding: &native_workspace_admin::Binding,
    request: &str,
) -> BTreeMap<PathBuf, Vec<u8>> {
    let directory = fixture
        .registry
        .join("landing-resolutions")
        .join(binding.id());
    if !directory.exists() {
        return BTreeMap::new();
    }
    files_in(&directory)
        .into_iter()
        .filter(|(path, _)| {
            path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(&format!("{request}-"))
        })
        .collect()
}

fn pending_landing_cold_recovery(
    producer_fault: Option<&str>,
    recovery_fault: Option<&str>,
    amend_source: bool,
    terminal: bool,
) {
    let upstream = RemoteServer::start_with_cloud_landing_recovery();
    let mut producer = Fixture::new();
    producer.configure_remote(&upstream.endpoint);
    producer.automatic_policies();
    producer.start();
    let producer_base = producer.base().to_owned();
    let (source_root, source, key, candidate) =
        prepare_native_landing(&mut producer, &upstream, "pending-cloud-source");
    let binding = producer.binding(&source_root);
    let job = upstream
        .store
        .landing_job("example", RemoteServer::landing_owner(), &key)
        .unwrap()
        .unwrap();
    assert_eq!(job.state, tinytree_store::LandingState::Prepared);
    assert_eq!(
        job.candidate.as_ref().unwrap().head.0,
        *blake3::Hash::from_hex(&candidate).unwrap().as_bytes()
    );
    let captured_operation = producer_fault.map(|point| {
        upstream.finish_fixture_landing(&key, true);
        let receipt = if point == "acknowledged" {
            producer.success(&source_root, &["tinytree", "land", "--resume"]);
            None
        } else {
            Some(kill_landing_at(
                &producer,
                &source_root,
                &["tinytree", "land", "--resume"],
                point,
            ))
        };
        let (_, saved) = remote_observation(&binding);
        if point == "apply_written" {
            receipt.unwrap().lines().nth(1).unwrap().to_owned()
        } else {
            saved["landing"]["apply_operation"]
                .as_str()
                .unwrap()
                .to_owned()
        }
    });
    producer.success(&source_root, &["status"]);
    let deadline = Instant::now() + Duration::from_secs(90);
    // A local Published record is ONLY a selector for the exact sealed job,
    // never proof of current cloud availability. Quiesce the producer before
    // independently authenticating the selected immutable cloud version below.
    let upload_directory = producer
        .socket
        .parent()
        .unwrap()
        .join("native-commands/upload");
    let (selected_generation, selected_manifest, capsule) = loop {
        let selected = fs::read_dir(&upload_directory).unwrap().find_map(|entry| {
            let path = entry.unwrap().path();
            if path.extension().and_then(|v| v.to_str()) != Some("published") {
                return None;
            }
            // Retirement may unlink older selectors during this scan.
            let read = |path: &Path| match fs::read(path) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("publication selector read failed: {error}"),
            };
            let bytes = read(&path)?;
            let header: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let job_bytes = read(&path.with_extension("job"))?;
            assert_eq!(&job_bytes[..8], b"TTUPJOB1");
            let size = u32::from_le_bytes(job_bytes[8..12].try_into().unwrap()) as usize;
            assert!(size <= 4096);
            let job_header: serde_json::Value =
                serde_json::from_slice(&job_bytes[12..12 + size]).unwrap();
            assert_eq!(header, job_header);
            assert_eq!(header["workspace"], serde_json::json!(binding.workspace_id));
            let capture_offset = 12 + size + header["manifest_size"].as_u64().unwrap() as usize;
            let capsule = job_bytes[capture_offset..].to_vec();
            assert_eq!(
                capsule.len() as u64,
                header["capture_size"].as_u64().unwrap()
            );
            let data = tinytree_cloud::decode_data(
                &capsule,
                tinytree_cloud::CaptureDataId::of(&capsule).0,
                &Default::default(),
            )
            .unwrap();
            let pending = data.version == 2
                && data
                    .remote
                    .as_ref()
                    .and_then(|r| r.landing.as_ref())
                    .is_some_and(|p| {
                        p.request_id == key && p.observed.as_ref().is_some_and(|j| {
                            j.phase
                                == if producer_fault.is_some() {
                                    tinytree_cloud::HistoricalLandingPhase::Committed
                                } else {
                                    tinytree_cloud::HistoricalLandingPhase::PreparedPendingChecks
                                }
                        }) && match producer_fault {
                            Some("apply_written") => {
                                p.operation_writing.as_deref() == Some("apply")
                            }
                            Some("apply_finished") => {
                                p.apply_operation.is_some() && !p.acknowledged
                            }
                            Some("acknowledged") => p.acknowledged && p.apply_operation.is_some(),
                            _ => true,
                        }
                    });
            pending.then(|| {
                (
                    header["expected_generation"].as_u64().unwrap() + 1,
                    serde_json::from_value::<[u8; 32]>(header["manifest_id"].clone()).unwrap(),
                    capsule,
                )
            })
        });
        if let Some(selected) = selected {
            break selected;
        }
        if Instant::now() >= deadline {
            let jobs = fs::read_dir(&upload_directory)
                .unwrap()
                .filter_map(|entry| {
                    let path = entry.unwrap().path();
                    if path.extension().and_then(|v| v.to_str()) != Some("job") {
                        return None;
                    }
                    let bytes = fs::read(path).ok()?;
                    let size = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
                    Some(
                        serde_json::from_slice::<serde_json::Value>(&bytes[12..12 + size]).unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            let cloud_calls = upstream.metadata.cloud_calls.lock().unwrap().clone();
            panic!(
                "pending DATA-v2 landing capture was not published: local={};upload={};jobs={jobs:?};cloud_calls={cloud_calls:?};daemon={}",
                local_capture_status(&producer),
                producer.control(Request::Status).unwrap()["native"]["commands"]["capture_upload"],
                fs::read_to_string(producer_base.join("daemon.log")).unwrap()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let selected_data = tinytree_cloud::decode_data(
        &capsule,
        tinytree_cloud::CaptureDataId::of(&capsule).0,
        &Default::default(),
    )
    .unwrap();
    eprintln!(
        "cloud-landing-recovery selected_generation={} operations={} indexes={} blobs={} native_roots={} capture_bytes={}",
        selected_generation,
        selected_data.operations.len(),
        selected_data.indexes.len(),
        selected_data.blobs.len(),
        selected_data.roots.len(),
        capsule.len()
    );
    assert_eq!(
        native_operation_heads(&binding),
        selected_data.operation_heads
    );
    let (source_wc, _) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
            .unwrap();
    assert!(!source_wc.pending);
    assert_eq!(
        source_wc.operation.hex(),
        selected_data.working_copy.wc_operation
    );
    assert_eq!(
        source_wc.workspace.as_str(),
        selected_data.working_copy.workspace
    );
    assert_eq!(
        source_wc.trees[0].terms,
        selected_data.working_copy.tree.terms
    );
    producer.success(
        &producer_base,
        &["tinytree", "unmount", source_root.to_str().unwrap()],
    );
    producer.stop_daemon();
    // No source uploader owns the repository admission now. Require actual
    // authenticated GetVersion/ReadCapture and all manifest/root bindings while
    // the original evidence still exists; a receipt alone cannot pass recovery.
    let (generation, manifest_id, raw, _manifest, authenticated_capsule) =
        retained_capture_versions_observed(
            &upstream,
            binding.workspace_id,
            None,
            Instant::now() + Duration::from_secs(30),
        )
        .into_iter()
        .find(|(generation, id, _, _, _)| {
            *generation == selected_generation && id.0 == selected_manifest
        })
        .expect("published pending landing immutable version missing");
    assert_eq!(authenticated_capsule, capsule);
    let data = tinytree_cloud::decode_data(
        &capsule,
        tinytree_cloud::CaptureDataId::of(&capsule).0,
        &Default::default(),
    )
    .unwrap();
    let original = data
        .remote
        .as_ref()
        .unwrap()
        .landing
        .as_ref()
        .unwrap()
        .clone();
    assert_eq!(original.source_head, source);
    assert_eq!(
        original.job_id.as_deref(),
        Some(capture_identity(&job.id).as_str())
    );
    assert_eq!(
        original.observed.as_ref().unwrap().expires_at_seconds,
        job.expires_at
    );
    let selection = workspace_daemon::control::NativeRestoreSelection {
        workspace_id: binding.workspace_id,
        generation,
        manifest_id: manifest_id.0,
        manifest_blake3: raw.blake3,
        manifest_size_bytes: raw.size_bytes,
    };
    let original_public = upstream.public();
    let starts = upstream.metadata.landing_starts.load(Ordering::SeqCst);
    // The source workspace, registry, native object store, packs and provider
    // cache disappear. Recovery below owns a NEW empty daemon/cache namespace.
    drop(producer);
    assert!(!producer_base.exists());
    let mut restored = Fixture::new();
    let restored_base = restored.base().to_owned();
    assert_ne!(restored_base, producer_base);
    assert!(!restored_base.join("daemon").exists());
    restored.configure_remote(&upstream.endpoint);
    restored.automatic_policies();
    restored.start();
    let root = restored_base.join("restored-pending");
    assert!(!root.exists());
    let workspace = capture_identity(&selection.workspace_id);
    let generation = selection.generation.to_string();
    let file_reads = upstream.file_reads();
    let result: workspace_daemon::control::NativeCloudRestored =
        serde_json::from_str(&restored.cloud(
            &restored_base,
            &[
                "restore",
                &workspace,
                root.to_str().unwrap(),
                "--generation",
                &generation,
                "--json",
            ],
        ))
        .unwrap();
    assert_eq!(result.source, selection);
    assert_fuse_mounted(&root);
    assert_eq!(
        upstream.file_reads(),
        file_reads,
        "cloud restore admission reads metadata, not FILE bodies"
    );
    assert_eq!(
        upstream.metadata.landing_starts.load(Ordering::SeqCst),
        starts,
        "restore never resubmits the original job"
    );
    assert_eq!(upstream.public(), original_public);
    let restored_binding = restored.binding(&root);
    assert_eq!(
        native_operation_heads(&restored_binding),
        data.operation_heads
    );
    let (wc, _) = jj_vfs::native::read_retained_state_for_capture(
        &restored_binding.projection(),
        1024 * 1024,
    )
    .unwrap();
    assert!(!wc.pending);
    assert_eq!(wc.operation.hex(), data.working_copy.wc_operation);
    assert_eq!(wc.workspace.as_str(), data.working_copy.workspace);
    assert_eq!(wc.trees[0].terms, data.working_copy.tree.terms);
    // The selected authenticated representation must survive restore as well
    // as subsequent hint replacement; semantic identity alone is insufficient.
    let captured_payloads = captured_operation.as_ref().map(|captured| {
        let simple = jj_lib::simple_op_store::SimpleOpStore::load(
            &restored_binding.repo.join("op_store"),
            jj_lib::op_store::RootOperationData {
                root_commit_id: jj_lib::backend::CommitId::from_bytes(&[0; 32]),
            },
        );
        let op = jj_lib::op_store::OperationId::try_from_hex(captured).unwrap();
        let view = &data
            .operations
            .iter()
            .find(|op| &op.operation == captured)
            .unwrap()
            .view;
        let view = jj_lib::op_store::ViewId::try_from_hex(view).unwrap();
        let operation_bytes = simple
            .read_cloud_operation_payload(&op, Default::default())
            .unwrap();
        let view_bytes = simple
            .read_cloud_view_payload(&view, Default::default())
            .unwrap();
        for (kind, id, bytes) in [
            (1, captured.as_str(), &operation_bytes),
            (
                2,
                data.operations
                    .iter()
                    .find(|op| &op.operation == captured)
                    .unwrap()
                    .view
                    .as_str(),
                &view_bytes,
            ),
        ] {
            let selected = data
                .blobs
                .iter()
                .find(|blob| blob.kind == kind && blob.id == id)
                .unwrap();
            assert_eq!(
                selected.blake3,
                *blake3::hash(bytes).as_bytes(),
                "restore preserves the selected OP/VIEW representation"
            );
            assert_eq!(selected.size, bytes.len() as u64);
        }
        (operation_bytes, view_bytes)
    });
    let scope = native_workspace_admin::remote_state::RemoteScope::new(
        restored.broker.clone(),
        "example".into(),
    )
    .unwrap();
    let (sidecar_before, _) = remote_observation(&restored_binding);
    let saved = native_workspace_admin::remote_state::decode(&sidecar_before, &scope).unwrap();
    let mut expected = original.clone();
    expected.restored_lookup_only = true;
    assert_eq!(saved.landing.as_ref(), Some(&expected));
    let heads = native_operation_heads(&restored_binding);
    let state_before = jj_vfs::native::read_retained_state(
        &restored_binding.root,
        &restored_binding.working_copy,
        &restored_binding.repository,
    )
    .unwrap()
    .persisted_bytes;
    let lookups = upstream.metadata.landing_lookups.load(Ordering::SeqCst);
    let status = landing_report(&mut restored, &root, &["tinytree", "land", "--status"]);
    assert_eq!(
        status["phase"],
        if producer_fault.is_some() {
            "committed"
        } else {
            "prepared_pending_checks"
        }
    );
    assert_eq!(status["request_id"], key);
    assert_eq!(status["job_id"], original.job_id.as_deref().unwrap());
    assert_eq!(
        upstream.metadata.landing_starts.load(Ordering::SeqCst),
        starts
    );
    assert!(upstream.metadata.landing_lookups.load(Ordering::SeqCst) > lookups);
    assert_eq!(remote_observation(&restored_binding).0, sidecar_before);
    assert_eq!(native_operation_heads(&restored_binding), heads);
    assert_eq!(
        jj_vfs::native::read_retained_state(
            &restored_binding.root,
            &restored_binding.working_copy,
            &restored_binding.repository
        )
        .unwrap()
        .persisted_bytes,
        state_before
    );
    assert_eq!(upstream.file_reads(), file_reads);
    // Existing trusted fixture checks provide functional protocol evidence only,
    // not evidence that a compiler or remote Bazel check actually ran.
    let committed = if producer_fault.is_none() {
        upstream.finish_fixture_landing(&key, !terminal)
    } else {
        upstream
            .store
            .landing_job("example", RemoteServer::landing_owner(), &key)
            .unwrap()
            .unwrap()
    };
    assert_eq!(committed.id, job.id);
    assert_eq!(committed.expires_at, job.expires_at);
    if amend_source {
        restored.success(
            &root,
            &["describe", "-r", &source, "-m", "amended after restore"],
        );
        fs::write(root.join("dirty.txt"), b"preserve unrecorded edits\n").unwrap();
        let before_heads = native_operation_heads(&restored_binding);
        let before_wc = jj_vfs::native::read_retained_state_for_capture(
            &restored_binding.projection(),
            1024 * 1024,
        )
        .unwrap()
        .0
        .persisted_bytes;
        let before_sidecar = remote_observation(&restored_binding).0;
        let before_admin = native_admin_state(&restored_binding);
        let before_public = upstream.public();
        if terminal {
            upstream
                .metadata
                .deny_metadata
                .store(true, Ordering::SeqCst);
        }
        let refused = restored.run(&root, &["tinytree", "land", "--resume"]);
        upstream
            .metadata
            .deny_metadata
            .store(false, Ordering::SeqCst);
        if terminal {
            assert!(
                refused.status.success(),
                "{}",
                String::from_utf8_lossy(&refused.stderr)
            );
            let report: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
            assert_eq!(report["phase"], "check_failed");
            let saved = native_workspace_admin::remote_state::decode(
                &remote_observation(&restored_binding).0,
                &scope,
            )
            .unwrap()
            .landing
            .unwrap();
            assert_eq!(saved.source_operation, original.source_operation);
            assert_eq!(saved.source_stack, original.source_stack);
            assert_eq!(saved.fork_operation.as_ref(), Some(&saved.source_operation));
            assert!(saved.fork_acknowledged);
            assert!(saved.apply_operation.is_none());
            assert!(saved.operation_writing.is_none());
            assert!(!saved.restored_lookup_only);
            assert_eq!(saved.terminal.as_deref(), Some("check_failed"));
            assert!(saved.candidate_head.is_none() && saved.result_roots.is_empty());
        } else {
            assert!(!refused.status.success());
            assert!(
                String::from_utf8_lossy(&refused.stderr).contains("independently amended"),
                "{}",
                String::from_utf8_lossy(&refused.stderr)
            );
        }
        assert_eq!(native_operation_heads(&restored_binding), before_heads);
        assert_eq!(
            jj_vfs::native::read_retained_state_for_capture(
                &restored_binding.projection(),
                1024 * 1024
            )
            .unwrap()
            .0
            .persisted_bytes,
            before_wc
        );
        if !terminal {
            assert_eq!(remote_observation(&restored_binding).0, before_sidecar);
        }
        assert_eq!(
            fs::read(root.join("dirty.txt")).unwrap(),
            b"preserve unrecorded edits\n"
        );
        assert_eq!(
            upstream.metadata.landing_starts.load(Ordering::SeqCst),
            starts
        );
        assert_eq!(upstream.public(), before_public);
        assert_eq!(native_admin_state(&restored_binding), before_admin);
        assert!(landing_resolution_records(&restored, &restored_binding, &key).is_empty());
        restored.success(
            &restored_base,
            &["tinytree", "unmount", root.to_str().unwrap()],
        );
        restored.stop_daemon();
        return;
    }
    if producer_fault.is_none() {
        fs::write(
            root.join("dirty.txt"),
            b"preserve edits through fresh apply\n",
        )
        .unwrap();
    }
    let already_acknowledged = producer_fault == Some("acknowledged");
    let current_new = matches!(producer_fault, Some("apply_finished" | "acknowledged"));
    let before_refs = current_new.then(|| {
        restored.success(
            &root,
            &[
                "--ignore-working-copy",
                "log",
                "--no-graph",
                "-r",
                "all()",
                "-T",
                "commit_id ++ \"\\n\"",
            ],
        )
    });
    if current_new {
        fs::write(
            root.join("dirty-new.txt"),
            b"do not snapshot or checkout this edit\n",
        )
        .unwrap();
    }
    let before_content = fs::read(root.join("pending-cloud-source.txt")).unwrap();
    let mut unrecorded = None;
    let owned = recovery_fault
        .map(|point| {
            let receipt =
                kill_landing_at(&restored, &root, &["tinytree", "land", "--resume"], point);
            if point == "resolution_recorded" {
                Some(receipt.lines().nth(1).unwrap().to_owned())
            } else {
                if point == "apply_written" {
                    unrecorded = Some(receipt.lines().nth(1).unwrap().to_owned());
                }
                None
            }
        })
        .flatten();
    let closed_admin = if producer_fault.is_none() && recovery_fault == Some("resolution_recorded")
    {
        kill_landing_at(
            &restored,
            &root,
            &["tinytree", "land", "--resume"],
            "closed_before_cleanup",
        );
        let saved = remote_observation(&restored_binding).1;
        assert_eq!(saved["landing"]["acknowledged"].as_bool(), Some(true));
        assert!(!landing_resolution_records(&restored, &restored_binding, &key).is_empty());
        Some(native_admin_state(&restored_binding))
    } else {
        None
    };
    let result = restored.run(&root, &["tinytree", "land", "--resume"]);
    assert!(
        result.status.success(),
        "resume failed: {}; fixture_host_base={}; actual_wc={}; heads={:?}",
        String::from_utf8_lossy(&result.stderr),
        fs::read_to_string(
            restored
                .registry
                .join("landing-resolutions")
                .join(restored_binding.id())
                .join(format!("{key}-apply.json"))
        )
        .unwrap_or_default(),
        String::from_utf8_lossy(
            &jj_vfs::native::read_retained_state_for_capture(
                &restored_binding.projection(),
                1024 * 1024
            )
            .unwrap()
            .0
            .persisted_bytes
        ),
        native_operation_heads(&restored_binding)
    );
    let applied: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert!(landing_resolution_records(&restored, &restored_binding, &key).is_empty());
    if let Some(before) = closed_admin {
        assert_eq!(
            native_admin_state(&restored_binding),
            before,
            "retrying durable ACK cleanup changes no OP, VIEW, index, head or WC bytes"
        );
    }
    if let Some(unrecorded) = unrecorded {
        assert_ne!(
            applied["applied_operation"], unrecorded,
            "an unrecorded nonce-stamped orphan is not host-owned replay authority"
        );
    }
    if current_new {
        assert_eq!(
            fs::read(root.join("dirty-new.txt")).unwrap(),
            b"do not snapshot or checkout this edit\n"
        );
    }
    if let Some(owned) = owned {
        assert_eq!(applied["applied_operation"], owned);
    }
    if let Some(captured) = &captured_operation {
        if already_acknowledged {
            assert_eq!(applied["applied_operation"], captured.as_str());
            assert_eq!(
                native_operation_heads(&restored_binding),
                heads,
                "ACK bookkeeping creates no operation"
            );
            assert_eq!(
                jj_vfs::native::read_retained_state_for_capture(
                    &restored_binding.projection(),
                    1024 * 1024
                )
                .unwrap()
                .0
                .persisted_bytes,
                state_before,
                "ACK bookkeeping does not mutate WC state"
            );
        } else {
            assert_ne!(
                applied["applied_operation"],
                captured.as_str(),
                "captured OP must never become replay authority"
            );
        }
    }
    if let Some(before) = before_refs {
        assert_eq!(
            restored.success(
                &root,
                &[
                    "--ignore-working-copy",
                    "log",
                    "--no-graph",
                    "-r",
                    "all()",
                    "-T",
                    "commit_id ++ \"\\n\""
                ]
            ),
            before,
            "metadata ACK does not alter evolution or visible commits"
        );
    }
    assert_eq!(
        fs::read(root.join("pending-cloud-source.txt")).unwrap(),
        before_content
    );
    assert_eq!(applied["phase"], "applied");
    assert_eq!(applied["acknowledged"], true);
    if producer_fault.is_none() {
        assert_eq!(
            fs::read(root.join("dirty.txt")).unwrap(),
            b"preserve edits through fresh apply\n"
        );
    }
    assert_eq!(restored.revision(&root, "@-"), candidate);
    assert_eq!(
        upstream.metadata.landing_starts.load(Ordering::SeqCst),
        starts
    );
    let (bytes, _) = remote_observation(&restored_binding);
    let final_state = native_workspace_admin::remote_state::decode(&bytes, &scope).unwrap();
    let final_landing = final_state.landing.unwrap();
    assert_eq!(final_landing.request_id, original.request_id);
    assert_eq!(final_landing.job_id, original.job_id);
    assert_eq!(final_landing.source_operation, original.source_operation);
    assert_eq!(final_landing.source_stack, original.source_stack);
    assert_eq!(
        final_landing.observed.as_ref().unwrap().expires_at_seconds,
        job.expires_at
    );
    assert!(final_landing.acknowledged);
    assert!(!final_landing.restored_lookup_only);
    assert!(final_landing.result_roots.is_empty());
    assert!(final_landing.observed.as_ref().unwrap().receipt.is_some());
    assert_eq!(
        fs::read(root.join("pending-cloud-source.txt")).unwrap(),
        b"source pending-cloud-source\n"
    );
    let mut published_after_ack = None;
    if let Some(captured) = &captured_operation {
        // Observe a real post-replacement portable capture, not merely that old
        // files still exist. The daemon inventories retained orphan OPs and
        // their VIEW/index association even after the sidecar selects a new OP.
        restored.success(&root, &["status"]);
        let directory = restored
            .socket
            .parent()
            .unwrap()
            .join("native-commands/upload");
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let selected = fs::read_dir(&directory)
                .unwrap()
                .filter_map(|entry| {
                    let path = entry.unwrap().path();
                    if path.extension().and_then(|x| x.to_str()) != Some("job") {
                        return None;
                    }
                    let bytes = fs::read(&path).ok()?;
                    let size = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
                    let header: serde_json::Value =
                        serde_json::from_slice(&bytes[12..12 + size]).unwrap();
                    let published = fs::read(path.with_extension("published")).ok()?;
                    let published: serde_json::Value = serde_json::from_slice(&published).unwrap();
                    assert_eq!(
                        published, header,
                        "Published is only an exact sealed-ticket selector"
                    );
                    let manifest_size = header["manifest_size"].as_u64().unwrap() as usize;
                    let capsule = &bytes[12 + size + manifest_size..];
                    let data = tinytree_cloud::decode_data(
                        capsule,
                        tinytree_cloud::CaptureDataId::of(capsule).0,
                        &Default::default(),
                    )
                    .unwrap();
                    let p = data.remote.as_ref()?.landing.as_ref()?;
                    if !p.acknowledged
                        || p.restored_lookup_only
                        || p.apply_operation != final_landing.apply_operation
                    {
                        return None;
                    }
                    let manifest = tinytree_cloud::decode_manifest_v3(
                        &bytes[12 + size..12 + size + manifest_size],
                        tinytree_cloud::ManifestId(
                            serde_json::from_value(header["manifest_id"].clone()).unwrap(),
                        ),
                        Default::default(),
                    )
                    .unwrap();
                    Some((
                        data,
                        manifest,
                        header["expected_generation"].as_u64().unwrap() + 1,
                        serde_json::from_value::<[u8; 32]>(header["manifest_id"].clone()).unwrap(),
                        capsule.to_vec(),
                    ))
                })
                .next();
            if let Some((after, manifest, generation, manifest_id, capsule)) = selected {
                published_after_ack = Some((generation, manifest_id, capsule));
                if current_new {
                    let previous = data
                        .operations
                        .iter()
                        .find(|op| op.operation == data.working_copy.wc_operation)
                        .unwrap();
                    let ack = after
                        .operations
                        .iter()
                        .find(|op| Some(&op.operation) == final_landing.apply_operation.as_ref())
                        .unwrap();
                    assert_eq!(
                        ack.view, previous.view,
                        "metadata ACK preserves exact CURRENT bookmarks, WC commit, visible heads and evolution view"
                    );
                }
                let old = data
                    .operations
                    .iter()
                    .find(|op| &op.operation == captured)
                    .unwrap();
                assert!(
                    after.operations.contains(old),
                    "original raw orphan OP and VIEW remain portable"
                );
                let index = data
                    .indexes
                    .iter()
                    .find(|index| &index.operation == captured)
                    .unwrap();
                assert!(
                    after.indexes.contains(index),
                    "original exact OP/index association remains portable"
                );
                let (op_bytes, view_bytes) = captured_payloads.as_ref().unwrap();
                for (kind, id, bytes) in [(1, captured, op_bytes), (2, &old.view, view_bytes)] {
                    let blob = after
                        .blobs
                        .iter()
                        .find(|b| b.kind == kind && &b.id == id)
                        .unwrap();
                    assert_eq!(blob.blake3, *blake3::hash(bytes).as_bytes());
                    assert_eq!(blob.size, bytes.len() as u64);
                }
                let op = jj_lib::op_store::OperationId::try_from_hex(captured).unwrap();
                let recovery = &manifest.history.recovery;
                let is_undo = recovery
                    .undo_operations
                    .iter()
                    .any(|id| id.as_slice() == op.as_bytes());
                if already_acknowledged {
                    assert!(
                        is_undo
                            || recovery
                                .operation_heads
                                .iter()
                                .chain(&recovery.intent_operations)
                                .any(|id| id.as_slice() == op.as_bytes()),
                        "the unchanged acknowledged OP retains its exact manifest root role"
                    );
                } else {
                    assert!(is_undo, "replaced original orphan remains an undo root");
                }
                // A normal WC snapshot can replace its old direct TREE root.
                // Prove EVERY removed direct root is still a canonical
                // dependency of a retained COMMIT, not merely present on disk.
                let missing = data
                    .roots
                    .iter()
                    .filter(|root| !after.roots.contains(root))
                    .collect::<Vec<_>>();
                let retained_commits = after
                    .roots
                    .iter()
                    .filter(|root| root.kind == m::Kind::Commit as u8)
                    .collect::<Vec<_>>();
                assert!(retained_commits.len() <= 256);
                let mut dependencies = std::collections::BTreeSet::new();
                let mut canonical_bytes = 0usize;
                if !missing.is_empty() {
                    for retained in &retained_commits {
                        let key = m::ObjectKey {
                            kind: m::Kind::Commit,
                            id: m::ObjectId(retained.id),
                        };
                        let (descriptor, bytes) =
                            upstream.store.read_metadata("example", key).unwrap();
                        assert_eq!(descriptor.key, key);
                        assert!(bytes.len() <= 4 * 1024 * 1024);
                        canonical_bytes += bytes.len();
                        assert!(
                            canonical_bytes <= 32 * 1024 * 1024,
                            "retained canonical dependency proof byte bound"
                        );
                        m::verify(&descriptor, &bytes, Default::default()).unwrap();
                        let m::Object::Commit(commit) =
                            m::decode(&bytes, Default::default()).unwrap()
                        else {
                            unreachable!()
                        };
                        for tree in commit.root_tree.terms {
                            dependencies.insert((m::Kind::Tree as u8, tree.0));
                        }
                        for parent in commit.parents {
                            dependencies.insert((m::Kind::Commit as u8, parent.0));
                        }
                    }
                }
                for root in missing {
                    assert!(
                        dependencies.contains(&(root.kind, root.id)),
                        "original root kind={} id={} is neither direct nor a verified canonical dependency of retained COMMIT roots",
                        root.kind,
                        capture_identity(&root.id)
                    );
                }
                break;
            }
            assert!(
                Instant::now() < deadline,
                "no portable post-replacement ACK capture: local={}; upload={}; daemon={}",
                local_capture_status(&restored),
                restored.control(Request::Status).unwrap()["native"]["commands"]["capture_upload"],
                fs::read_to_string(restored_base.join("daemon.log")).unwrap()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    restored.success(
        &restored_base,
        &["tinytree", "unmount", root.to_str().unwrap()],
    );
    restored.stop_daemon();
    if let Some((generation, manifest_id, selected_capsule)) = published_after_ack {
        // A receipt was only a selector above. After quiescing its producer,
        // authenticate the exact immutable version and all capture/root bindings.
        let selected = retained_capture_versions_observed(
            &upstream,
            restored_binding.workspace_id,
            None,
            Instant::now() + Duration::from_secs(30),
        )
        .into_iter()
        .find(|(g, id, _, _, _)| *g == generation && id.0 == manifest_id)
        .expect("post-ACK Published version is not actually retained");
        assert_eq!(selected.4, selected_capsule);
    }
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3"]
fn exact_nonprimary_capture_upload_and_empty_cache_restore_preserve_all_raw_encodings() {
    use jj_lib::object_id::ObjectId as _;
    let upstream = RemoteServer::start_with_cloud_capture_capacity_and_history(260);
    let mut producer = Fixture::new();
    producer.configure_remote(&upstream.endpoint);
    producer.index_mode("federated");
    producer.capture_policy("after_command_best_effort");
    // The focused merge ancestry adds authoring operations before the desired
    // multi-head capture. Keep this fixture's pending catalog explicitly bounded
    // without starving that final capture behind earlier best-effort tickets.
    let mut config: toml::Value =
        toml::from_str(&fs::read_to_string(&producer.daemon_config).unwrap()).unwrap();
    config["native"]["workspaces"]["capture"]
        .as_table_mut()
        .unwrap()
        .insert("max_pending".into(), 128.into());
    fs::write(&producer.daemon_config, toml::to_string(&config).unwrap()).unwrap();
    workspace_daemon::config::Config::load(&producer.daemon_config).unwrap();
    producer.start();
    let base = producer.base().to_owned();
    let root = base.join("exact-variants");
    producer.clone_remote(&root);
    let binding = producer.binding(&root);
    producer.success(&root, &["new", "-m", "frozen path merge base"]);
    fs::write(root.join("merge-log.txt"), b"base\nanchor\nlast\n").unwrap();
    producer.success(&root, &["status"]);
    producer.success(&root, &["bookmark", "set", "frozen-merge-base", "-r", "@"]);
    producer.success(
        &root,
        &[
            "new",
            "-r",
            "frozen-merge-base",
            "-m",
            "frozen path merge left",
        ],
    );
    fs::write(root.join("merge-log.txt"), b"left\nanchor\nlast\n").unwrap();
    producer.success(&root, &["status"]);
    producer.success(&root, &["bookmark", "set", "frozen-merge-left", "-r", "@"]);
    producer.success(
        &root,
        &[
            "new",
            "-r",
            "frozen-merge-base",
            "-m",
            "frozen path merge right",
        ],
    );
    fs::write(root.join("merge-log.txt"), b"base\nanchor\nright\n").unwrap();
    producer.success(&root, &["status"]);
    producer.success(&root, &["bookmark", "set", "frozen-merge-right", "-r", "@"]);
    producer.success(
        &root,
        &[
            "new",
            "-r",
            "frozen-merge-left",
            "-r",
            "frozen-merge-right",
            "-m",
            "frozen clean path merge",
        ],
    );
    producer.success(&root, &["bookmark", "set", "frozen-merge-clean", "-r", "@"]);
    let clean = producer.revision(&root, "@");
    producer.success(
        &root,
        &[
            "new",
            "-r",
            "frozen-merge-left",
            "-r",
            "frozen-merge-right",
            "-m",
            "frozen explicit merge edit",
        ],
    );
    fs::write(root.join("merge-log.txt"), b"edited\nanchor\nright\n").unwrap();
    producer.success(&root, &["status"]);
    producer.success(&root, &["bookmark", "set", "frozen-merge-edit", "-r", "@"]);
    let edited = producer.revision(&root, "@");
    producer.success(
        &root,
        &[
            "new",
            "-r",
            "frozen-merge-clean",
            "-r",
            "frozen-merge-edit",
            "-m",
            "retain both path merge siblings",
        ],
    );
    let joined = producer.revision(&root, "@");
    let merge_candidates = format!("{clean} | {edited} | {joined}");
    let merge_args = [
        "--ignore-working-copy",
        "log",
        "--no-graph",
        "-r",
        &merge_candidates,
        "-T",
        "commit_id ++ \"\\n\"",
        "file:merge-log.txt",
    ];
    let query_writes = upstream.metadata.writes.load(Ordering::SeqCst);
    let native = producer.run(&root, &merge_args);
    assert!(
        native.status.success(),
        "{}",
        String::from_utf8_lossy(&native.stderr)
    );
    assert_eq!(native.stdout, format!("{edited}\n").as_bytes());
    assert_eq!(
        upstream.metadata.writes.load(Ordering::SeqCst),
        query_writes
    );
    let merge_expected = legacy_annotation_oracle(&mut producer, &root, &merge_args);
    assert_eq!(native.stdout, merge_expected);
    for selector in ["glob:merge-*.txt", "glob:*.txt ~ capture-only.txt"] {
        let mut args = merge_args;
        *args.last_mut().unwrap() = selector;
        let output = producer.run(&root, &args);
        assert!(
            output.status.success(),
            "pre-capture {selector}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, merge_expected);
        assert_eq!(
            output.stdout,
            legacy_annotation_oracle(&mut producer, &root, &args),
            "pre-capture matcher preserves the same stock merge selection"
        );
    }
    producer.success(&root, &["new", "-m", "exact raw capture"]);
    let authored = b"private captured workspace exact representation\n";
    fs::write(root.join("capture-only.txt"), authored).unwrap();
    producer.success(&root, &["status"]);
    let operation = producer.success(&root, &["op", "log", "--no-graph", "-n", "1", "-T", "id"]);
    producer.success(&root, &["describe", "-m", "current exact branch"]);
    producer.success(
        &root,
        &[
            "--at-op",
            operation.trim(),
            "describe",
            "-m",
            "parallel exact raw capture",
        ],
    );
    producer.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    assert!(native_operation_heads(&binding).len() >= 2);
    let count = local_capture_status(&producer)["pending_count"]
        .as_u64()
        .unwrap();
    let packs = local_capture_bundles(&producer);
    assert!(count > 0);
    producer.stop_daemon();
    let pack_directory = Arc::new(
        private_fs::PrivateDir::open_existing(
            &producer
                .socket
                .parent()
                .unwrap()
                .join("native-commands/captures"),
        )
        .unwrap(),
    );
    let mut payloads = BTreeMap::new();
    for bytes in packs.values() {
        let pack = native_capture_pack::VerifiedPack::open(
            pack_directory.clone(),
            *blake3::hash(bytes).as_bytes(),
            bytes.len() as u64,
            Default::default(),
            Arc::new(()),
        )
        .unwrap();
        for entry in pack
            .portable_entries()
            .filter(|e| matches!(e.descriptor().kind, 1 | 2))
        {
            let key = (entry.descriptor().kind, entry.descriptor().id.clone());
            let raw = entry.read_verified().unwrap().bytes().to_vec();
            if let Some(old) = payloads.insert(key, raw.clone()) {
                assert_eq!(old, raw);
            }
        }
    }
    let mut principal = blake3::Hasher::new();
    principal.update(b"tinytree-retention-owner-v1\0");
    let name = b"mounted-writer";
    principal.update(&(name.len() as u64).to_be_bytes());
    principal.update(name);
    let owner = tinytree_store::CloudOwner {
        principal: *principal.finalize().as_bytes(),
    };
    let mut kinds = [0, 0];
    for ((kind, text), bytes) in &payloads {
        let id = if *kind == 1 {
            tinytree_cloud::ObjectId::Operation(
                jj_lib::op_store::OperationId::try_from_hex(text)
                    .unwrap()
                    .as_bytes()
                    .try_into()
                    .unwrap(),
            )
        } else {
            tinytree_cloud::ObjectId::View(
                jj_lib::op_store::ViewId::try_from_hex(text)
                    .unwrap()
                    .as_bytes()
                    .try_into()
                    .unwrap(),
            )
        };
        // Compatibility fixture: a legal unknown field changes raw encoding,
        // not OP/VIEW semantic identity. The immutable saved pack stays exact.
        let mut alternate = bytes.clone();
        alternate.extend_from_slice(&[0xa0, 0x06, 1]);
        let digest = tinytree_cloud::BlobDigest::of(&alternate);
        tinytree_cloud::VerifiedObject::verify(id, digest, &alternate, Default::default()).unwrap();
        upstream
            .store
            .put_cloud_object(
                "example",
                owner,
                id,
                digest,
                &alternate,
                0,
                Default::default(),
            )
            .unwrap();
        kinds[(*kind - 1) as usize] += 1;
    }
    assert!(kinds.iter().all(|n| *n > 0));
    producer.enable_capture_upload();
    producer.start();
    wait_for_capture_publications(&producer, count);
    assert_eq!(
        local_capture_bundles(&producer),
        packs,
        "exact upload cannot rewrite immutable capsules"
    );
    producer.stop_daemon();
    let mut capture = verify_uploaded_capture(&upstream, &producer, &binding, &packs, count);
    capture.merged_path_log = Some(FrozenMergedPathLog {
        candidates: merge_candidates,
        expected: merge_expected,
    });
    upstream.store.gc("example", 1_000_000_000_000).unwrap();
    let public = upstream.public();
    drop(pack_directory);
    drop(producer);
    assert!(!base.exists());
    restore_captured_workspace(&upstream, &capture, authored);
    assert_eq!(upstream.public(), public);
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn published_capture_retirement_continues_distinct_edits_and_restores_old_versions() {
    let upstream = RemoteServer::start_with_cloud();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("federated");
    fixture.enable_capture_retirement();
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("retiring");
    fixture.clone_remote(&root);
    let binding = fixture.binding(&root);
    let public = upstream.public();
    let mut captures = Vec::new();
    let mut first_pack = None;
    let mut high_water_bytes = 0;
    let mut largest_pack_bytes = 0;
    let mut expected_publications = 0;
    // More distinct edits than the configured TWO-ticket capacity. Do not use
    // revision/log helpers here: those would create additional captures.
    for edit in 0..5 {
        if edit == 2 {
            // New publications are still permitted while exact retained-version
            // reads fail. An old proof failure must not starve the new upload.
            upstream
                .metadata
                .unavailable_versions
                .store(true, Ordering::Release);
        }
        let bytes = format!("private captured workspace file revision {edit}\n").into_bytes();
        fs::write(root.join("capture-only.txt"), &bytes).unwrap();
        fixture.success(&root, &["status"]);
        let captured = local_capture_status(&fixture);
        let queued = captured["queued"].as_u64().unwrap();
        assert!(
            queued > expected_publications,
            "unique edit was not captured: {captured}"
        );
        expected_publications = queued;
        assert_eq!(captured["pending_clean"], 0);
        assert!(captured["pending_count"].as_u64().unwrap() <= 2);
        let (wc, _) =
            jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
                .unwrap();
        assert!(!wc.pending);
        captures.push((wc.operation.hex(), bytes));
        if edit == 2 {
            let deadline = Instant::now() + Duration::from_secs(90);
            while cloud_generation(&upstream, binding.workspace_id) < queued {
                assert!(
                    Instant::now() < deadline,
                    "old proof failure starved newer backup upload"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
            let uncertain = local_capture_status(&fixture);
            assert_eq!(
                uncertain["pending_count"], 2,
                "uncertain old and new capsules must remain local"
            );
            assert_eq!(uncertain["pending_clean"], 0);
            assert_eq!(local_capture_bundles(&fixture).len(), 2);
            upstream
                .metadata
                .unavailable_versions
                .store(false, Ordering::Release);
        }
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let status = fixture.control(Request::Status).unwrap();
            let local = &status["native"]["commands"]["local_capture"];
            let upload = &status["native"]["commands"]["capture_upload"];
            high_water_bytes = high_water_bytes.max(local["charged_spool_bytes"].as_u64().unwrap());
            if local["pending_count"] == 1
                && upload["historical_published_count"] == 1
                && upload["cleanup_pending"] == false
                && upload["retired_local_capture_count"].as_u64().unwrap() >= queued - 1
            {
                assert_eq!(upload["policy"], "publish_retiring_redundant");
                assert_eq!(upload["policy_retires_redundant_capsules"], true);
                assert_eq!(local["pending_dirty"], 1);
                assert_eq!(local["pending_clean"], 0);
                assert_eq!(local["pending_unavailable"], 0);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "capture retirement did not progress: local={local};upload={upload};daemon={}",
                fs::read_to_string(fixture.base().join("daemon.log")).unwrap()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let packs = local_capture_bundles(&fixture);
        assert_eq!(packs.len(), 1, "old unique packs must actually be removed");
        let actual_bytes: u64 = packs.values().map(|bytes| bytes.len() as u64).sum();
        largest_pack_bytes = largest_pack_bytes.max(actual_bytes);
        assert_eq!(
            local_capture_status(&fixture)["charged_spool_bytes"],
            actual_bytes
        );
        let upload = files_in(
            &fixture
                .socket
                .parent()
                .unwrap()
                .join("native-commands/upload"),
        );
        assert_eq!(
            upload.len(),
            2,
            "only the newest job and publication record remain"
        );
        if let Some(first) = &first_pack {
            assert!(
                !packs.contains_key(first),
                "the old exact capsule remained local"
            );
        } else {
            first_pack = Some(packs.keys().next().unwrap().clone());
        }
        assert_eq!(
            upstream.public(),
            public,
            "private backup must not land edits"
        );
        assert_eq!(
            upstream.file_reads(),
            0,
            "backup must not hydrate historical source FILEs"
        );
    }
    assert!(expected_publications >= 5);
    assert!(high_water_bytes > 0 && high_water_bytes <= 2 * largest_pack_bytes);
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
    fixture.start();
    assert_eq!(local_capture_status(&fixture)["pending_count"], 1);
    assert_eq!(local_capture_bundles(&fixture).len(), 1);
    let versions =
        retained_capture_versions(&upstream, binding.workspace_id, expected_publications);
    // Local capsule retirement can be justified by the old remote version alone.
    // Independently require the real newest export to carry the former snapshot
    // and its exact undo/index/raw associations before server retirement is added.
    let oldest = versions
        .iter()
        .find(|(_, _, _, manifest, _)| {
            capture_identity(&manifest.history.recovery.working_copy.wc_operation)
                == captures.first().unwrap().0
        })
        .unwrap();
    let newest = versions
        .iter()
        .find(|(_, _, _, manifest, _)| {
            capture_identity(&manifest.history.recovery.working_copy.wc_operation)
                == captures.last().unwrap().0
        })
        .unwrap();
    let decode = |version: &RetainedCaptureVersion| {
        tinytree_cloud::decode_data(
            &version.4,
            tinytree_cloud::CaptureDataId::of(&version.4).0,
            &Default::default(),
        )
        .unwrap()
    };
    let old_data = decode(oldest);
    let new_data = decode(newest);
    assert_eq!(new_data.working_copy.wc_operation, captures.last().unwrap().0);
    for operation in &old_data.operations {
        assert!(
            new_data.operations.contains(operation),
            "newest real export must preserve the older exact operation and view"
        );
    }
    for index in &old_data.indexes {
        assert!(
            new_data.indexes.contains(index),
            "newest real export must preserve every older operation/index association"
        );
    }
    for blob in &old_data.blobs {
        assert!(
            new_data.blobs.contains(blob),
            "newest real export must preserve older selected raw OP/VIEW representations"
        );
    }
    for member in &oldest.3.history.recovery.members {
        assert!(
            newest.3.history.recovery.members.contains(member),
            "newest remote manifest must retain the older exact raw selection"
        );
    }
    assert!(
        new_data.operations.iter().any(|operation| {
            operation.operation == old_data.working_copy.snapshot_operation
        }),
        "the original snapshot operation must remain usable from newest capture alone"
    );
    fixture.success(&base, &["tinytree", "resume", root.to_str().unwrap()]);
    let oldest_snapshot = versions
        .iter()
        .find_map(|(_, _, _, manifest, _)| {
            let wc = &manifest.history.recovery.working_copy;
            (capture_identity(&wc.wc_operation) == captures[0].0)
                .then(|| capture_identity(&wc.snapshot_commit))
        })
        .expect("oldest captured operation missing remotely");
    assert_eq!(
        fixture
            .success(
                &root,
                &[
                    "--ignore-working-copy",
                    "--at-op",
                    &captures[0].0,
                    "log",
                    "--no-graph",
                    "-r",
                    "@",
                    "-T",
                    "commit_id",
                ]
            )
            .trim(),
        oldest_snapshot,
        "old operation/index associations must resolve the exact former snapshot"
    );
    assert_eq!(
        fs::read(root.join("capture-only.txt")).unwrap(),
        captures.last().unwrap().1
    );
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();

    // Read only the actual remote retained versions. Old private packs have
    // already gone; neither restore may depend on a saved local capsule.
    for (selected, use_latest) in [
        (captures.first().unwrap(), false),
        (captures.last().unwrap(), true),
    ] {
        let (generation, manifest_id, raw, _manifest, capsule) = versions
            .iter()
            .find(|(_, _, _, manifest, _)| {
                capture_identity(&manifest.history.recovery.working_copy.wc_operation) == selected.0
            })
            .expect("distinct edited snapshot missing from retained cloud history");
        let data = tinytree_cloud::decode_data(
            capsule,
            tinytree_cloud::CaptureDataId::of(capsule).0,
            &Default::default(),
        )
        .unwrap();
        let selection = workspace_daemon::control::NativeRestoreSelection {
            workspace_id: binding.workspace_id,
            generation: *generation,
            manifest_id: manifest_id.0,
            manifest_blake3: raw.blake3,
            manifest_size_bytes: raw.size_bytes,
        };
        restore_retired_capture(&upstream, &selection, &data, &selected.1, use_latest);
    }
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn rolling_cloud_window_preserves_a_cold_host_pinned_restore_after_restart() {
    fn default_workspace(fixture: &mut Fixture, endpoint: &str) {
        fixture.configure_remote(endpoint);
        fixture.automatic_policies();
        let mut config: toml::Value =
            toml::from_str(&fs::read_to_string(&fixture.daemon_config).unwrap()).unwrap();
        // Only shorten the fixture scheduler tick; capture/upload policy stays Auto.
        config["native"]["workspaces"]
            .as_table_mut()
            .unwrap()
            .insert(
                "upload".into(),
                toml::Value::Table(toml::map::Map::from_iter([("poll_ms".into(), 250.into())])),
            );
        fs::write(&fixture.daemon_config, toml::to_string(&config).unwrap()).unwrap();
    }
    let upstream = RemoteServer::start_with_rolling_cloud_window(2);
    let mut producer = Fixture::new();
    default_workspace(&mut producer, &upstream.endpoint);
    producer.start();
    let producer_base = producer.base().to_owned();
    let source = producer_base.join("rolling-source");
    producer.clone_remote(&source);
    let binding = producer.binding(&source);
    let public = upstream.public();
    let first_bytes = b"first pinned private snapshot\n";
    fs::write(source.join("capture-only.txt"), first_bytes).unwrap();
    producer.success(&source, &["status"]);
    let first_count = local_capture_status(&producer)["queued"].as_u64().unwrap();
    assert!((1..=2).contains(&first_count));
    // GetWorkspace is legitimately absent before the first background publication.
    wait_for_capture_publications(&producer, 1);
    let deadline = Instant::now() + Duration::from_secs(90);
    while cloud_generation(&upstream, binding.workspace_id) < first_count {
        assert!(
            Instant::now() < deadline,
            "initial default backup did not publish"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let initial = retained_capture_versions(&upstream, binding.workspace_id, first_count);
    // Fresh-first scheduling can publish the edited capture before the clone
    // capture. Select logical capture order, not publication generation order.
    let selected = initial
        .iter()
        .max_by_key(|version| version.3.history.recovery.sequence)
        .unwrap();
    let selection = workspace_daemon::control::NativeRestoreSelection {
        workspace_id: binding.workspace_id,
        generation: selected.0,
        manifest_id: selected.1.0,
        manifest_blake3: selected.2.blake3,
        manifest_size_bytes: selected.2.size_bytes,
    };
    let workspace = capture_identity(&binding.workspace_id);
    let generation = selection.generation.to_string();
    let mut restored = Fixture::new();
    default_workspace(&mut restored, &upstream.endpoint);
    restored.start();
    let restored_base = restored.base().to_owned();
    let destination = restored_base.join("pinned-restore");
    let file_reads = upstream.file_reads();
    let report: workspace_daemon::control::NativeCloudRestored =
        serde_json::from_str(&restored.cloud(
            &restored_base,
            &[
                "restore",
                &workspace,
                destination.to_str().unwrap(),
                "--generation",
                &generation,
                "--json",
            ],
        ))
        .unwrap();
    assert_eq!(report.source, selection);
    assert_eq!(
        upstream.file_reads(),
        file_reads,
        "restore must remain metadata-only"
    );
    // No restored FILE or query is read before shutdown: the durable host pin,
    // not a prewarmed cache or a surviving reader, must protect this source.
    restored.success(
        &restored_base,
        &["tinytree", "unmount", destination.to_str().unwrap()],
    );
    restored.stop_daemon();

    let mut latest = first_count;
    for edit in 1..=6 {
        fs::write(
            source.join("capture-only.txt"),
            format!("later snapshot {edit}\n"),
        )
        .unwrap();
        producer.success(&source, &["status"]);
        let queued = local_capture_status(&producer)["queued"].as_u64().unwrap();
        assert!(queued > latest, "distinct default capture did not advance");
        let deadline = Instant::now() + Duration::from_secs(90);
        while cloud_generation(&upstream, binding.workspace_id) < queued {
            assert!(
                Instant::now() < deadline,
                "default backup stalled beyond rolling window"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        latest = queued;
    }
    assert!(latest >= selection.generation + 6);
    assert_eq!(
        upstream.public(),
        public,
        "backup cannot publish private roots"
    );
    restored.start();
    let page: workspace_daemon::control::NativeCloudVersions =
        serde_json::from_str(&restored.cloud(
            &restored_base,
            &["versions", &workspace, "--limit", "10", "--json"],
        ))
        .unwrap();
    assert_eq!(
        page.versions
            .iter()
            .map(|v| v.generation)
            .collect::<Vec<_>>(),
        vec![latest - 1, latest]
    );
    assert!(page.continuation.is_empty());
    let expired_destination = restored_base.join("expired-restore");
    let configuration = restored.daemon_config.to_str().unwrap().to_owned();
    let expired_generation = (selection.generation + 1).to_string();
    let expired = restored.run(
        &restored_base,
        &[
            "tinytree",
            "cloud",
            "--daemon-config",
            &configuration,
            "--store",
            "example",
            "restore",
            &workspace,
            expired_destination.to_str().unwrap(),
            "--generation",
            &expired_generation,
            "--json",
        ],
    );
    assert!(
        !expired.status.success(),
        "expired source must not fall back to latest"
    );
    assert!(!expired_destination.exists());
    let resumed: workspace_daemon::control::NativeCloudRestored =
        serde_json::from_str(&restored.cloud(
            &restored_base,
            &[
                "restore",
                &workspace,
                destination.to_str().unwrap(),
                "--json",
            ],
        ))
        .unwrap();
    assert!(resumed.reused_destination);
    assert_eq!(resumed.source, selection, "retry must not reselect latest");
    assert_eq!(
        upstream.file_reads(),
        file_reads,
        "resume cannot eagerly hydrate source FILEs"
    );
    assert_eq!(
        fs::read(destination.join("capture-only.txt")).unwrap(),
        first_bytes
    );
    let log = restored.success(
        &destination,
        &[
            "--ignore-working-copy",
            "log",
            "--no-graph",
            "-r",
            "@",
            "-T",
            "commit_id",
        ],
    );
    assert_eq!(
        log.trim(),
        capture_identity(&selected.3.history.recovery.working_copy.snapshot_commit)
    );
    let blame = restored.success(
        &destination,
        &[
            "--ignore-working-copy",
            "file",
            "annotate",
            "capture-only.txt",
        ],
    );
    assert!(blame.contains("first pinned private snapshot"));
    assert_eq!(upstream.public(), public);
    restored.success(
        &restored_base,
        &["tinytree", "unmount", destination.to_str().unwrap()],
    );
    restored.stop_daemon();
    producer.success(
        &producer_base,
        &["tinytree", "unmount", source.to_str().unwrap()],
    );
    producer.stop_daemon();
}

fn cloud_generation(upstream: &RemoteServer, workspace: [u8; 32]) -> u64 {
    use tinytree_client::cloud_protocol as c;
    upstream.runtime.as_ref().unwrap().block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let client = tokio::time::timeout_at(
            deadline,
            c::workspace_cloud_client::WorkspaceCloudClient::connect(upstream.endpoint.clone()),
        )
        .await
        .unwrap()
        .unwrap()
        .accept_compressed(CompressionEncoding::Zstd);
        // Use the shipped SDK policy: temporary admission needs a server hint;
        // permanent resource quotas must not become an observer retry loop.
        let policy = tinytree_client::Config::default().rpc;
        let value = policy
            .retry(deadline, Duration::from_secs(10), |allowance| {
                let mut client = client.clone();
                async move {
                    let mut request = RpcRequest::new(c::GetWorkspaceRequest {
                        repository_id: "example".into(),
                        workspace_id: workspace.to_vec(),
                    });
                    request.metadata_mut().insert(
                        "authorization",
                        format!("Bearer {REMOTE_TOKEN}").parse().unwrap(),
                    );
                    request.set_timeout(allowance);
                    client
                        .get_workspace(request)
                        .await
                        .map(|response| response.into_inner())
                }
            })
            .await
            .unwrap();
        assert_eq!(value.workspace_id, workspace);
        value.generation
    })
}

fn restore_retired_capture(
    upstream: &RemoteServer,
    selection: &workspace_daemon::control::NativeRestoreSelection,
    data: &tinytree_cloud::CaptureData,
    expected_bytes: &[u8],
    use_latest: bool,
) {
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.automatic_policies();
    let mut config: toml::Value =
        toml::from_str(&fs::read_to_string(&fixture.daemon_config).unwrap()).unwrap();
    config["native"]["workspaces"]
        .as_table_mut()
        .unwrap()
        .insert(
            "upload".into(),
            toml::Value::Table(toml::map::Map::from_iter([("poll_ms".into(), 250.into())])),
        );
    fs::write(&fixture.daemon_config, toml::to_string(&config).unwrap()).unwrap();
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("retired-restore");
    let workspace = capture_identity(&selection.workspace_id);
    let file_reads = upstream.file_reads();
    let mut cursor = String::new();
    let mut generations = Vec::new();
    loop {
        let page: workspace_daemon::control::NativeCloudVersions =
            serde_json::from_str(&fixture.cloud(
                &base,
                &[
                    "versions", &workspace, "--limit", "2", "--cursor", &cursor, "--json",
                ],
            ))
            .unwrap();
        assert_eq!(page.workspace.workspace_id, workspace);
        assert!(page.through_generation >= selection.generation);
        generations.extend(page.versions.into_iter().map(|version| version.generation));
        cursor = page.continuation;
        if cursor.is_empty() {
            break;
        }
    }
    assert_eq!(
        generations,
        (1..=cloud_generation(upstream, selection.workspace_id)).collect::<Vec<_>>()
    );
    assert_eq!(
        upstream.file_reads(),
        file_reads,
        "discovery reads no native FILE bodies"
    );
    let generation = selection.generation.to_string();
    let mut restore = vec!["restore", &workspace, root.to_str().unwrap(), "--json"];
    if !use_latest {
        restore.extend(["--generation", &generation]);
    }
    let ordinary_scopes = upstream
        .metadata
        .ordinary_scope_opens
        .load(Ordering::SeqCst);
    let retained_scopes = upstream
        .metadata
        .retained_scope_opens
        .load(Ordering::SeqCst);
    let public = upstream.public();
    // Save the host's exact request, then fail either feature admission or its
    // private retained scope. Neither failure may allocate per-ID/destination state.
    upstream
        .metadata
        .creation_fault
        .store(if use_latest { 4 } else { 2 }, Ordering::SeqCst);
    let configuration = fixture.daemon_config.to_str().unwrap().to_owned();
    let mut failed_args = vec![
        "tinytree",
        "cloud",
        "--daemon-config",
        &configuration,
        "--store",
        "example",
    ];
    failed_args.extend(restore.iter().copied());
    let failed = fixture.run(&base, &failed_args);
    assert!(!failed.status.success());
    assert!(!root.exists());
    assert_eq!(upstream.file_reads(), file_reads);
    let intents = files_in(&fixture.base().join("daemon/native-cloud-restores"));
    assert_eq!(intents.len(), 1);
    let (intent_name, intent_bytes) = intents.iter().next().unwrap();
    let saved: serde_json::Value = serde_json::from_slice(intent_bytes).unwrap();
    assert_eq!(saved["source"], serde_json::to_value(selection).unwrap());
    assert_eq!(saved["requested_policy"]["require_lazy_queries"], true);
    assert_eq!(saved["requested_policy"]["auto_capture"], true);
    assert_eq!(saved["requested_policy"]["auto_upload"], true);
    let saved_identity: [u8; 32] = serde_json::from_value(saved["workspace_id"].clone()).unwrap();
    assert!(!fixture
        .base()
        .join("daemon/native-workspaces")
        .join(capture_identity(&saved_identity))
        .exists());
    upstream.metadata.creation_fault.store(0, Ordering::SeqCst);
    fixture.stop_daemon();
    // The host-owned intent already froze Auto. Changing new-workspace policy
    // must not reselect its source or eligibility on restart.
    fixture.index_mode("legacy");
    fixture.start();
    let report: workspace_daemon::control::NativeCloudRestored =
        serde_json::from_str(&fixture.cloud(&base, &restore)).unwrap();
    assert_eq!(report.source, *selection);
    assert!(report.reused_destination);
    assert_eq!(report.mounted.binding.workspace_id, saved_identity);
    assert_eq!(
        fs::read(
            fixture
                .base()
                .join("daemon/native-cloud-restores")
                .join(intent_name)
        )
        .unwrap(),
        *intent_bytes
    );
    assert_eq!(
        upstream
            .metadata
            .ordinary_scope_opens
            .load(Ordering::SeqCst),
        ordinary_scopes,
        "restore readiness must not reinterpret private roots as ordinary public scopes"
    );
    assert!(
        upstream
            .metadata
            .retained_scope_opens
            .load(Ordering::SeqCst)
            > retained_scopes
    );
    assert_eq!(
        upstream.file_reads(),
        file_reads,
        "restore admission is metadata-only"
    );
    let binding = fixture.binding(&root);
    assert_eq!(native_operation_heads(&binding), data.operation_heads);
    let (wc, _) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
            .unwrap();
    assert!(!wc.pending);
    assert_eq!(wc.operation.hex(), data.working_copy.wc_operation);
    assert_eq!(
        fs::read(root.join("capture-only.txt")).unwrap(),
        expected_bytes
    );
    let record: serde_json::Value = serde_json::from_slice(
        &fs::read(
            fixture
                .base()
                .join("daemon/native-workspaces")
                .join(binding.id())
                .join("workspace.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(
        record.get("creation_plan").is_none(),
        "retained authority is not a public-root creation plan"
    );
    assert_eq!(
        record["restore_enrollment"]["requested"]["auto_capture"],
        true
    );
    assert_eq!(
        record["restore_enrollment"]["requested"]["auto_upload"],
        true
    );
    assert_eq!(
        fs::read_to_string(binding.repo.join("index/type")).unwrap(),
        jj_native_index::NAME
    );
    let restored_identity = binding.workspace_id;
    let holds_before_backup = upstream.metadata.hold_publications.load(Ordering::SeqCst);
    let public_scopes_before_backup = upstream
        .metadata
        .ordinary_scope_opens
        .load(Ordering::SeqCst);
    let later = b"later local edit must survive restore retry\n";
    fs::write(root.join("capture-only.txt"), later).unwrap();
    let retry: workspace_daemon::control::NativeCloudRestored =
        serde_json::from_str(&fixture.cloud(
            &base,
            &["restore", &workspace, root.to_str().unwrap(), "--json"],
        ))
        .unwrap();
    assert!(retry.reused_destination);
    assert_eq!(
        retry.source, *selection,
        "latest retry must reuse the old exact pin even when newer cloud versions exist"
    );
    assert_eq!(retry.mounted.binding.workspace_id, restored_identity);
    assert_eq!(fs::read(root.join("capture-only.txt")).unwrap(), later);
    let configuration = fixture.daemon_config.to_str().unwrap().to_owned();
    let rejected = fixture.run(
        &base,
        &[
            "tinytree",
            "cloud",
            "--daemon-config",
            &configuration,
            "--store",
            "example",
            "restore",
            &workspace,
            root.to_str().unwrap(),
            "--generation",
            &(selection.generation + 1).to_string(),
        ],
    );
    assert!(!rejected.status.success());
    assert_eq!(fs::read(root.join("capture-only.txt")).unwrap(), later);
    fixture.success(&root, &["status"]);
    let uploaded = wait_for_capture_publications(&fixture, 1);
    assert!(uploaded["historical_published_count"].as_u64().unwrap() >= 1);
    assert!(cloud_generation(upstream, restored_identity) >= 1);
    let native_reads_after_selected_file = upstream.file_reads();
    assert_eq!(upstream.public(), public);
    assert_eq!(
        upstream.metadata.hold_publications.load(Ordering::SeqCst),
        holds_before_backup,
        "restored private roots reuse retained cloud authority, never new public holds"
    );
    assert_eq!(
        upstream
            .metadata
            .ordinary_scope_opens
            .load(Ordering::SeqCst),
        public_scopes_before_backup,
        "restored automatic backup never opens public query scopes for private roots"
    );
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
    fixture.start();
    let resumed: workspace_daemon::control::NativeCloudRestored =
        serde_json::from_str(&fixture.cloud(
            &base,
            &["restore", &workspace, root.to_str().unwrap(), "--json"],
        ))
        .unwrap();
    assert!(resumed.reused_destination);
    assert_eq!(resumed.source, *selection);
    assert_eq!(resumed.mounted.binding.workspace_id, restored_identity);
    assert_eq!(fs::read(root.join("capture-only.txt")).unwrap(), later);
    let after_restart = b"restored automatic backup after daemon restart\n";
    fs::write(root.join("capture-only.txt"), after_restart).unwrap();
    fixture.success(&root, &["status"]);
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let generation = cloud_generation(upstream, restored_identity);
        if generation >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restored workspace backup did not advance: {}",
            fixture.control(Request::Status).unwrap()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let retained = retained_capture_versions(
        upstream,
        restored_identity,
        cloud_generation(upstream, restored_identity),
    );
    let latest = retained.last().unwrap();
    let latest_data = tinytree_cloud::decode_data(
        &latest.4,
        tinytree_cloud::CaptureDataId::of(&latest.4).0,
        &Default::default(),
    )
    .unwrap();
    let (current_wc, _) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
            .unwrap();
    assert_eq!(
        latest_data.operation_heads,
        native_operation_heads(&binding)
    );
    assert_eq!(
        latest_data.working_copy.wc_operation,
        current_wc.operation.hex()
    );
    assert_eq!(
        latest_data.working_copy.tree.terms,
        current_wc.trees[0].terms
    );
    assert_eq!(
        upstream.file_reads(),
        native_reads_after_selected_file,
        "continued backup must not hydrate unrelated native FILE bodies"
    );
    assert_eq!(upstream.public(), public);
    assert_eq!(
        upstream.metadata.hold_publications.load(Ordering::SeqCst),
        holds_before_backup,
        "restored private roots reuse retained cloud authority, never new public holds"
    );
    assert_eq!(
        upstream
            .metadata
            .ordinary_scope_opens
            .load(Ordering::SeqCst),
        public_scopes_before_backup,
        "restored automatic backup never opens public query scopes for private roots"
    );
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
    // Explicit Legacy opts out of enrollment negotiation. Its retained-source
    // collector still negotiates exact raw representations before scope access.
    upstream.metadata.creation_fault.store(4, Ordering::SeqCst);
    fixture.start();
    let compatibility_root = base.join("explicit-legacy-restore");
    let capability_count = upstream.metadata.capability_requests.lock().unwrap().len();
    let configuration = fixture.daemon_config.to_str().unwrap().to_owned();
    let failure = fixture.run(
        &base,
        &[
            "tinytree",
            "cloud",
            "--daemon-config",
            &configuration,
            "--store",
            "example",
            "restore",
            &workspace,
            compatibility_root.to_str().unwrap(),
            "--generation",
            &generation,
        ],
    );
    assert!(!failure.status.success());
    assert!(!compatibility_root.exists());
    let requests = upstream.metadata.capability_requests.lock().unwrap();
    assert_eq!(&requests[capability_count..], &[vec![9]]);
    drop(requests);
    fixture.stop_daemon();
    // Model the canonical pre-enrollment journal schema in a controlled private
    // fixture. The next startup uses Auto, but this old intent must stay None.
    #[derive(serde::Serialize, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct OldCloudIntent {
        version: u32,
        root: PathBuf,
        store: String,
        workspace_id: [u8; 32],
        source: workspace_daemon::control::NativeRestoreSelection,
        provider_identity: [u8; 32],
    }
    let directory =
        private_fs::PrivateDir::open_existing(fixture.base().join("daemon/native-cloud-restores"))
            .unwrap();
    let intents = files_in(directory.path());
    let (name, mut old) = intents
        .iter()
        .find_map(|(name, bytes)| {
            let value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            (value["root"] == compatibility_root.to_str().unwrap()).then_some((name, value))
        })
        .unwrap();
    old.as_object_mut().unwrap().remove("requested_policy");
    let old: OldCloudIntent = serde_json::from_value(old).unwrap();
    directory
        .replace(
            Path::new(name).file_name().unwrap(),
            &serde_json::to_vec(&old).unwrap(),
        )
        .unwrap();
    fixture.automatic_policies();
    upstream.metadata.creation_fault.store(1, Ordering::SeqCst);
    fixture.start();
    let compatibility: workspace_daemon::control::NativeCloudRestored =
        serde_json::from_str(&fixture.cloud(
            &base,
            &[
                "restore",
                &workspace,
                compatibility_root.to_str().unwrap(),
                "--generation",
                &generation,
                "--json",
            ],
        ))
        .unwrap();
    assert_eq!(compatibility.source, *selection);
    let compatibility_binding = fixture.binding(&compatibility_root);
    assert_eq!(
        fs::read_to_string(compatibility_binding.repo.join("index/type")).unwrap(),
        jj_native_index::NAME
    );
    let old_record: serde_json::Value = serde_json::from_slice(
        &fs::read(
            fixture
                .base()
                .join("daemon/native-workspaces")
                .join(compatibility_binding.id())
                .join("workspace.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(old_record.get("restore_enrollment").is_none());
    let queued = local_capture_status(&fixture)["queued"].as_u64().unwrap();
    fixture.success(&compatibility_root, &["status"]);
    assert_eq!(local_capture_status(&fixture)["queued"], queued);
    assert_eq!(
        cloud_generation(upstream, compatibility_binding.workspace_id),
        0
    );
    fixture.success(
        &base,
        &["tinytree", "unmount", compatibility_root.to_str().unwrap()],
    );
    fixture.stop_daemon();
    // Explicit capture disable gates Auto upload and needs only the selected
    // lazy-query features, even when the advertised cloud-write grant is absent.
    fixture.capture_policy("disabled");
    upstream.metadata.creation_fault.store(6, Ordering::SeqCst);
    fixture.start();
    let disabled_root = base.join("disabled-automatic-backup");
    let capability_count = upstream.metadata.capability_requests.lock().unwrap().len();
    let disabled: workspace_daemon::control::NativeCloudRestored =
        serde_json::from_str(&fixture.cloud(
            &base,
            &[
                "restore",
                &workspace,
                disabled_root.to_str().unwrap(),
                "--generation",
                &generation,
                "--json",
            ],
        ))
        .unwrap();
    assert_eq!(disabled.source, *selection);
    let requests = upstream.metadata.capability_requests.lock().unwrap();
    assert_eq!(&requests[capability_count..], &[vec![1, 2, 3], vec![9]]);
    drop(requests);
    let queued = local_capture_status(&fixture)["queued"].as_u64().unwrap();
    fixture.success(&disabled_root, &["status"]);
    assert_eq!(local_capture_status(&fixture)["queued"], queued);
    assert_eq!(
        cloud_generation(upstream, disabled.mounted.binding.workspace_id),
        0
    );
    fixture.success(
        &base,
        &["tinytree", "unmount", disabled_root.to_str().unwrap()],
    );
    fixture.stop_daemon();
    upstream.metadata.creation_fault.store(0, Ordering::SeqCst);
}

fn wait_for_capture_publications(fixture: &Fixture, expected: u64) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let status = fixture.control(Request::Status).unwrap();
        let upload = &status["native"]["commands"]["capture_upload"];
        if upload["historical_published_count"].as_u64().unwrap() >= expected {
            return upload.clone();
        }
        assert!(
            Instant::now() < deadline,
            "capture publication inventory timed out: {upload}; daemon={}",
            fs::read_to_string(fixture.base().join("daemon.log")).unwrap()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn local_capture_status(fixture: &Fixture) -> serde_json::Value {
    let status = fixture.control(Request::Status).unwrap();
    assert_eq!(status["native"]["commands"]["active_commands"], 0);
    assert_eq!(status["native"]["commands"]["collection_enabled"], false);
    status["native"]["commands"]["local_capture"].clone()
}

fn local_capture_bundles(fixture: &Fixture) -> BTreeMap<String, Vec<u8>> {
    let directory = fixture
        .socket
        .parent()
        .unwrap()
        .join("native-commands/captures");
    let private = Arc::new(private_fs::PrivateDir::open_existing(&directory).unwrap());
    fs::read_dir(&directory)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("pack") {
                return None;
            }
            let bytes = fs::read(&path).unwrap();
            assert!(!bytes.is_empty());
            assert_eq!(
                path.file_stem().unwrap().to_str().unwrap(),
                blake3::hash(&bytes).to_hex().as_str()
            );
            // Exercise the shared structural reader against bytes emitted by
            // the actual daemon, including after restart. Neither the private
            // WC nor the optional private sidecar receives a portable lease.
            let pack = native_capture_pack::VerifiedPack::open(
                private.clone(),
                *blake3::hash(&bytes).as_bytes(),
                bytes.len() as u64,
                native_capture_pack::Limits::default(),
                Arc::new(()), // This bounded native fixture has no parallel readers.
            )
            .unwrap();
            let mut kinds = Vec::new();
            for lease in pack.portable_entries() {
                let descriptor = lease.descriptor();
                assert!((1..=5).contains(&descriptor.kind));
                kinds.push(descriptor.kind);
                let payload = lease.read_verified().unwrap();
                assert_eq!(payload.bytes().len() as u64, descriptor.size);
                assert_eq!(blake3::hash(payload.bytes()).as_bytes(), &descriptor.blake3);
            }
            assert!(kinds.contains(&1) && kinds.contains(&2));
            assert_eq!(kinds.iter().filter(|kind| **kind == 5).count(), 1);
            Some((entry.file_name().to_str().unwrap().to_owned(), bytes))
        })
        .collect()
}

/// Read every immutable retained generation from the actual authenticated
/// service, then independently match its full semantic proof to a saved pack.
struct VerifiedRestoreCapture {
    selection: workspace_daemon::control::NativeRestoreSelection,
    data: tinytree_cloud::CaptureData,
    objects: Vec<tinytree_cloud::VerifiedObject>,
    merged_path_log: Option<FrozenMergedPathLog>,
}
struct FrozenMergedPathLog {
    candidates: String,
    expected: Vec<u8>,
}

type RetainedCaptureVersion = (
    u64,
    tinytree_cloud::ManifestId,
    tinytree_cloud::BlobDigest,
    tinytree_cloud::ManifestV3,
    Vec<u8>,
);

fn retained_capture_versions(
    upstream: &RemoteServer,
    workspace_id: [u8; 32],
    expected_publications: u64,
) -> Vec<RetainedCaptureVersion> {
    retained_capture_versions_observed(
        upstream,
        workspace_id,
        Some(expected_publications),
        Instant::now() + Duration::from_secs(30),
    )
}

fn retained_capture_versions_observed(
    upstream: &RemoteServer,
    workspace_id: [u8; 32],
    expected_publications: Option<u64>,
    deadline: Instant,
) -> Vec<RetainedCaptureVersion> {
    use tinytree_client::cloud_protocol as c;
    fn authenticated<T>(value: T, allowance: Duration) -> RpcRequest<T> {
        let mut request = RpcRequest::new(value);
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {REMOTE_TOKEN}").parse().unwrap(),
        );
        request.set_timeout(allowance);
        request
    }
    let selections = upstream.runtime.as_ref().unwrap().block_on(async {
        let deadline =
            tokio::time::Instant::now() + deadline.saturating_duration_since(Instant::now());
        tokio::time::timeout_at(deadline, async {
            let policy = tinytree_client::Config::default().rpc;
            let client =
                c::workspace_cloud_client::WorkspaceCloudClient::connect(upstream.endpoint.clone())
                    .await
                    .unwrap()
                    .accept_compressed(CompressionEncoding::Zstd)
                    .send_compressed(CompressionEncoding::Zstd);
            // Share one absolute operation deadline; retry only the shipped
            // policy's transient statuses and server RetryInfo admission hints.
            let workspace = policy
                .retry(deadline, Duration::from_secs(30), |allowance| {
                    let mut client = client.clone();
                    async move {
                        client
                            .get_workspace(authenticated(
                                c::GetWorkspaceRequest {
                                    repository_id: "example".into(),
                                    workspace_id: workspace_id.to_vec(),
                                },
                                allowance,
                            ))
                            .await
                            .map(|value| value.into_inner())
                    }
                })
                .await
                .unwrap();
            assert_eq!(workspace.workspace_id, workspace_id);
            assert!((1..=256).contains(&workspace.generation));
            if let Some(expected_publications) = expected_publications {
                assert_eq!(
                    workspace.generation, expected_publications,
                    "every durable Published record must correspond to a retained generation"
                );
            }
            let mut selections = Vec::new();
            let mut previous = None;
            for generation in 1..=workspace.generation {
                let version = policy
                    .retry(deadline, Duration::from_secs(30), |allowance| {
                        let mut client = client.clone();
                        async move {
                            client
                                .get_version(authenticated(
                                    c::GetVersionRequest {
                                        repository_id: "example".into(),
                                        workspace_id: workspace_id.to_vec(),
                                        generation,
                                    },
                                    allowance,
                                ))
                                .await
                                .map(|value| value.into_inner())
                        }
                    })
                    .await
                    .unwrap();
                assert_eq!(version.workspace_id, workspace_id);
                assert_eq!(version.generation, generation);
                let manifest_id = tinytree_cloud::ManifestId::of(&version.manifest);
                assert_eq!(version.manifest_id, manifest_id.0);
                assert_eq!(version.previous_manifest_id, previous);
                previous = Some(manifest_id.0.to_vec());
                let manifest = tinytree_cloud::decode_manifest_v3(
                    &version.manifest,
                    manifest_id,
                    Default::default(),
                )
                .unwrap();
                assert_eq!(
                    tinytree_cloud::encode_manifest_v3(&manifest, Default::default()).unwrap(),
                    version.manifest
                );
                assert_eq!(manifest.history.recovery.workspace, workspace_id);
                let raw = tinytree_cloud::BlobDigest::of(&version.manifest);
                let authority = version.native_authority.as_ref().unwrap();
                assert_eq!(authority.version, 1);
                let selected = authority.manifest_selected.as_ref().unwrap();
                assert_eq!(selected.blake3, raw.blake3);
                assert_eq!(selected.size_bytes, raw.size_bytes);
                let owner: [u8; 32] = authority.owner_binding.as_slice().try_into().unwrap();
                assert_eq!(
                    authority.binding_digest,
                    m::cloud_native_binding_digest(
                        "example",
                        owner,
                        workspace_id,
                        generation,
                        manifest_id.0,
                        raw.blake3,
                        raw.size_bytes
                    )
                );

                let request = c::ReadCaptureRequest {
                    repository_id: "example".into(),
                    selector: Some(c::RetainedVersionSelector {
                        workspace_id: workspace_id.to_vec(),
                        generation,
                        manifest_id: manifest_id.0.to_vec(),
                    }),
                    expected_capture_id: manifest.capture.id.0.to_vec(),
                    manifest_selected: Some(c::RawDigest {
                        blake3: raw.blake3.to_vec(),
                        size_bytes: raw.size_bytes,
                    }),
                };
                let mut stream = policy
                    .retry(deadline, Duration::from_secs(30), |allowance| {
                        let mut client = client.clone();
                        let request = request.clone();
                        async move {
                            client
                                .read_capture(authenticated(request, allowance))
                                .await
                                .map(|value| value.into_inner())
                        }
                    })
                    .await
                    .unwrap();
                let mut capsule = Vec::new();
                let mut eof = false;
                while let Some(chunk) = stream.message().await.unwrap() {
                    assert!(!eof, "capture frames after EOF");
                    assert_eq!(chunk.manifest_id, manifest_id.0);
                    assert_eq!(chunk.offset, capsule.len() as u64);
                    let descriptor = chunk.descriptor.unwrap();
                    assert_eq!(descriptor.capture_id, manifest.capture.id.0);
                    let selected = descriptor.selected.unwrap();
                    assert_eq!(selected.blake3, manifest.capture.selected.blake3);
                    assert_eq!(selected.size_bytes, manifest.capture.selected.size_bytes);
                    assert!(capsule.len() + chunk.data.len() <= selected.size_bytes as usize);
                    assert!(!chunk.data.is_empty() || chunk.eof);
                    capsule.extend_from_slice(&chunk.data);
                    eof = chunk.eof;
                }
                assert!(eof);
                assert_eq!(
                    tinytree_cloud::CaptureDescriptor::of(&capsule),
                    manifest.capture
                );
                selections.push((generation, manifest_id, raw, manifest, capsule));
            }
            assert_eq!(workspace.manifest_id, previous);
            selections
        })
        .await
        .expect("retained capture verification timed out")
    });
    selections
}

fn verify_uploaded_capture(
    upstream: &RemoteServer,
    fixture: &Fixture,
    binding: &native_workspace_admin::Binding,
    packs: &BTreeMap<String, Vec<u8>>,
    expected_publications: u64,
) -> VerifiedRestoreCapture {
    let selections =
        retained_capture_versions(upstream, binding.workspace_id, expected_publications);
    fn identity<const N: usize>(text: &str) -> [u8; N] {
        assert_eq!(text.len(), N * 2);
        let mut id = [0; N];
        for (i, byte) in id.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).unwrap();
        }
        id
    }
    let directory = Arc::new(
        private_fs::PrivateDir::open_existing(
            fixture
                .socket
                .parent()
                .unwrap()
                .join("native-commands/captures"),
        )
        .unwrap(),
    );
    let source_heads = native_operation_heads(binding);
    let (source_wc, _) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
            .unwrap();
    assert!(!source_wc.pending);
    let mut restore = None;
    for (generation, manifest_id, raw, manifest, capsule) in selections {
        let mut matched = false;
        for bytes in packs.values() {
            let pack = native_capture_pack::VerifiedPack::open(
                directory.clone(),
                *blake3::hash(bytes).as_bytes(),
                bytes.len() as u64,
                Default::default(),
                Arc::new(()),
            )
            .unwrap();
            let capsule_entry = pack
                .portable_entries()
                .find(|e| e.descriptor().kind == 5)
                .unwrap();
            if capsule_entry.descriptor().blake3 != manifest.capture.selected.blake3 {
                continue;
            }
            assert_eq!(capsule_entry.read_verified().unwrap().bytes(), capsule);
            let mut objects = Vec::new();
            let mut artifacts = Vec::new();
            for entry in pack.portable_entries() {
                let descriptor = entry.descriptor();
                let payload = entry.read_verified().unwrap();
                let selected = tinytree_cloud::BlobDigest {
                    blake3: descriptor.blake3,
                    size_bytes: descriptor.size,
                };
                match descriptor.kind {
                    1 | 2 => {
                        let id = identity(&descriptor.id);
                        let id = if descriptor.kind == 1 {
                            tinytree_cloud::ObjectId::Operation(id)
                        } else {
                            tinytree_cloud::ObjectId::View(id)
                        };
                        objects.push(
                            tinytree_cloud::VerifiedObject::verify(
                                id,
                                selected,
                                payload.bytes(),
                                Default::default(),
                            )
                            .unwrap(),
                        );
                    }
                    3 | 4 => {
                        let id = identity(&descriptor.id);
                        let id = if descriptor.kind == 3 {
                            tinytree_cloud::IndexArtifactId::Root(tinytree_cloud::IndexRootId(id))
                        } else {
                            tinytree_cloud::IndexArtifactId::Node(tinytree_cloud::IndexNodeId(id))
                        };
                        artifacts.push(
                            tinytree_cloud::decode_index_artifact(
                                payload.bytes(),
                                tinytree_cloud::IndexArtifactDescriptor { id, selected },
                                Default::default(),
                            )
                            .unwrap(),
                        );
                    }
                    5 => {}
                    _ => {
                        panic!("private WC/remote sidecars must not become portable cloud objects")
                    }
                }
            }
            let graph = tinytree_cloud::verify_index_artifact_graph(
                &manifest.history.index_roots,
                &artifacts,
                Default::default(),
            )
            .unwrap();
            tinytree_cloud::verify_capture_export_binding(
                &manifest,
                &capsule,
                &objects,
                &artifacts,
                &graph,
                Default::default(),
            )
            .unwrap();
            assert_eq!(manifest.history.recovery.members.len(), objects.len());
            // Portable manifests describe only semantic OP/VIEW64 and radix
            // root/node32 artifacts. Private local kinds128/129 remain pack-only.
            let data =
                tinytree_cloud::decode_data(&capsule, manifest.capture.id.0, &Default::default())
                    .unwrap();
            assert!(data.blobs.iter().all(|blob| (1..=4).contains(&blob.kind)));
            if data.operation_heads == source_heads
                && data.working_copy.wc_operation == source_wc.operation.hex()
                && data.working_copy.workspace == source_wc.workspace.as_str()
                && source_wc.trees.len() == 1
                && data.working_copy.tree.terms == source_wc.trees[0].terms
                && data.working_copy.tree.labels == source_wc.trees[0].labels
            {
                restore = Some(VerifiedRestoreCapture {
                    selection: workspace_daemon::control::NativeRestoreSelection {
                        workspace_id: binding.workspace_id,
                        generation,
                        manifest_id: manifest_id.0,
                        manifest_blake3: raw.blake3,
                        manifest_size_bytes: raw.size_bytes,
                    },
                    data,
                    objects,
                    merged_path_log: None,
                });
            }
            matched = true;
            break;
        }
        assert!(
            matched,
            "authenticated retained capture has no matching immutable local pack"
        );
    }
    let restore = restore.expect("retained capture must match the actual stopped source heads/WC");
    assert!(restore.data.operation_heads.len() >= 2);
    restore
}

fn native_operation_heads(binding: &native_workspace_admin::Binding) -> Vec<String> {
    jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(&binding.repo.join("op_heads"))
        .get_op_heads_bounded(1024)
        .unwrap()
        .iter()
        .map(|id| id.hex())
        .collect()
}

fn capture_identity(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn assert_restored_metadata(
    binding: &native_workspace_admin::Binding,
    capture: &VerifiedRestoreCapture,
) {
    use tinytree_cloud as c;
    assert_eq!(
        native_operation_heads(binding),
        capture.data.operation_heads
    );
    assert!(capture.data.operation_heads.len() >= 2);
    assert_eq!(
        fs::read_to_string(binding.repo.join("index/type")).unwrap(),
        jj_native_index::NAME
    );
    let mut actual_objects = Vec::new();
    for expected in &capture.objects {
        if expected.payload().is_none() {
            continue;
        }
        let (directory, name) = match expected.id() {
            c::ObjectId::Operation(id) => ("operations", capture_identity(&id)),
            c::ObjectId::View(id) => ("views", capture_identity(&id)),
        };
        let bytes = fs::read(binding.repo.join("op_store").join(directory).join(name)).unwrap();
        let actual = c::VerifiedObject::verify(
            expected.id(),
            c::BlobDigest::of(&bytes),
            &bytes,
            Default::default(),
        )
        .unwrap();
        // jj may normalize a legal protobuf representation during import, but
        // every semantic operation/view and its identity must remain exact.
        assert!(actual.operation() == expected.operation());
        assert!(actual.view() == expected.view());
        actual_objects.push(actual);
    }
    let wc = &capture.data.working_copy;
    let snapshot_operation = actual_objects
        .iter()
        .find(|object| {
            matches!(object.id(), c::ObjectId::Operation(id) if capture_identity(&id) == wc.snapshot_operation)
        })
        .unwrap()
        .operation()
        .unwrap();
    let snapshot_view = actual_objects
        .iter()
        .find(|object| {
            matches!(object.id(), c::ObjectId::View(id) if id.as_slice() == snapshot_operation.view_id.as_bytes())
        })
        .unwrap()
        .view()
        .unwrap();
    assert_eq!(
        snapshot_view
            .wc_commit_ids
            .iter()
            .find(|(name, _)| name.as_str() == wc.workspace)
            .unwrap()
            .1
            .as_bytes(),
        wc.snapshot_commit.as_slice()
    );
    let (actual_wc, _) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
            .unwrap();
    assert!(!actual_wc.pending);
    assert_eq!(actual_wc.operation.hex(), wc.wc_operation);
    assert_eq!(actual_wc.workspace.as_str(), wc.workspace);
    assert_eq!(actual_wc.trees.len(), 1);
    assert_eq!(actual_wc.trees[0].terms, wc.tree.terms);
    assert_eq!(actual_wc.trees[0].labels, wc.tree.labels);
}

#[derive(Debug, PartialEq, Eq)]
struct NativeAdminState {
    heads: Vec<String>,
    operations: BTreeMap<PathBuf, Vec<u8>>,
    views: BTreeMap<PathBuf, Vec<u8>>,
    index_associations: BTreeMap<PathBuf, Vec<u8>>,
    working_copy: Vec<u8>,
}
fn native_admin_state(binding: &native_workspace_admin::Binding) -> NativeAdminState {
    let index_type = fs::read_to_string(binding.repo.join("index/type")).unwrap();
    let associations = if index_type == jj_native_index::NAME {
        "operations"
    } else if index_type == jj_lib::default_index::DefaultIndexStore::NAME {
        "op_links"
    } else {
        panic!("unsupported recorded index type for native admin state");
    };
    let (wc, _) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
            .unwrap();
    assert!(!wc.pending);
    NativeAdminState {
        heads: native_operation_heads(binding),
        operations: files_in(&binding.repo.join("op_store/operations")),
        views: files_in(&binding.repo.join("op_store/views")),
        index_associations: files_in(&binding.repo.join("index").join(associations)),
        working_copy: wc.persisted_bytes.to_vec(),
    }
}

fn restore_captured_workspace(
    upstream: &RemoteServer,
    capture: &VerifiedRestoreCapture,
    authored_bytes: &[u8],
) {
    restore_captured_workspace_with_watchdog(upstream, capture, authored_bytes, Duration::from_secs(30));
}

fn restore_captured_workspace_with_watchdog(
    upstream: &RemoteServer,
    capture: &VerifiedRestoreCapture,
    authored_bytes: &[u8],
    restore_watchdog: Duration,
) {
    // Build the eager stock oracle in a separate live view. A frozen restore
    // deliberately cannot authorize ambient ancestry reads for a Legacy index.
    let inherited_oracle = {
        let mut oracle = Fixture::new();
        oracle.configure_remote(&upstream.endpoint);
        oracle.index_mode("legacy");
        oracle.start();
        let oracle_base = oracle.base().to_owned();
        let oracle_root = oracle_base.join("retained-inherited-oracle");
        oracle.clone_remote(&oracle_root);
        let output = oracle.run(&oracle_root, &[
            "file", "annotate", "hello.cc", "-r", &upstream.head,
        ]);
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        oracle.success(&oracle_base, &["tinytree", "unmount", oracle_root.to_str().unwrap()]);
        oracle.stop_daemon();
        output.stdout
    };
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("restored");
    let configuration = fixture.daemon_config.to_str().unwrap().to_owned();
    let destination_id = [0x6d; 32];
    assert_ne!(destination_id, capture.selection.workspace_id);
    let selection = &capture.selection;
    let cache_before = fixture.control(Request::Status).unwrap()["cache"].clone();
    assert_eq!(cache_before["fetches"], 0);
    assert_eq!(cache_before["verified_bytes"], 0);
    let file_reads_before = upstream.file_reads();
    fixture.success_with_watchdog(
        &base,
        &[
            "tinytree",
            "restore",
            root.to_str().unwrap(),
            "--daemon-config",
            &configuration,
            "--store",
            "example",
            "--workspace-id",
            &capture_identity(&destination_id),
            "--source-workspace",
            &capture_identity(&selection.workspace_id),
            "--generation",
            &selection.generation.to_string(),
            "--manifest",
            &capture_identity(&selection.manifest_id),
            "--manifest-blake3",
            &capture_identity(&selection.manifest_blake3),
            "--manifest-bytes",
            &selection.manifest_size_bytes.to_string(),
        ],
        restore_watchdog,
    );
    assert_fuse_mounted(&root);
    let binding = fixture.binding(&root);
    assert_eq!(binding.workspace_id, destination_id);
    assert_ne!(binding.workspace_id, selection.workspace_id);
    // No jj workspace command may run before these exact saved metadata checks:
    // default loading could otherwise merge the captured concurrent heads.
    assert_restored_metadata(&binding, capture);
    let cache_after_restore = fixture.control(Request::Status).unwrap()["cache"].clone();
    assert_eq!(cache_after_restore["fetches"], cache_before["fetches"]);
    assert_eq!(
        cache_after_restore["verified_bytes"],
        cache_before["verified_bytes"]
    );
    assert_eq!(upstream.file_reads(), file_reads_before);
    let public_before_queries = upstream.public();
    let writes_before_queries = upstream.metadata.writes.load(Ordering::SeqCst);
    let remote_state_path = binding.working_copy.join("native-remote.toml");
    let remote_state_before = fs::read(&remote_state_path).ok();
    let captured_revision = capture_identity(&capture.data.working_copy.snapshot_commit);
    let history: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree",
            "history",
            "capture-only.txt",
            "-r",
            &captured_revision,
            "--max-commits",
            "512",
            "--json",
        ],
    ))
    .unwrap();
    // The legacy PathHistory RPC remains explicitly linear. Only this focused
    // fixture adds merge ancestry; ordinary V2 log below must still compare the
    // complete selected merge set against its saved stock result.
    assert_eq!(
        history["coverage"],
        if capture.merged_path_log.is_some() {
            "merge_boundary"
        } else {
            "complete"
        }
    );
    assert_eq!(history["revision"], captured_revision);
    assert!(history["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| { entry["commit"] == captured_revision && entry["local"] == false }));
    let blame: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree",
            "blame",
            "capture-only.txt",
            "-r",
            &captured_revision,
            "--max-commits",
            "512",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(blame["coverage"], "complete");
    assert_eq!(blame["policy"], "jj_resolved_fixed_path_v1");
    assert_eq!(blame["revision"], captured_revision);
    assert_eq!(blame["local_commits"], 0);
    assert_eq!(blame["lines"][0]["origin"]["commit"], captured_revision);
    assert_eq!(blame["lines"][0]["origin"]["original_line"], 1);
    assert_eq!(
        blame["lines"][0]["bytes"],
        serde_json::json!(authored_bytes)
    );
    let ordinary_scopes_before = upstream
        .metadata
        .ordinary_scope_opens
        .load(Ordering::SeqCst);
    let canonical_before = upstream.metadata.query_commit_bytes.load(Ordering::SeqCst);
    let nodes_before = upstream.metadata.annotation_nodes.load(Ordering::SeqCst);
    let annotation = fixture.success(&root, &[
        "file", "annotate", "capture-only.txt", "-r", &captured_revision,
        "-T", "commit.commit_id() ++ \":\" ++ original_line_number ++ \":\" ++ line_number ++ \":\" ++ first_line_in_hunk ++ \":\" ++ content",
    ]);
    assert_eq!(
        annotation.as_bytes(),
        [
            format!("{captured_revision}:1:1:true:").as_bytes(),
            authored_bytes
        ]
        .concat()
    );
    // Default prefix/author/timestamp rendering must use captured-only COMMIT
    // metadata, not IDs returned by blame as an ambient read capability.
    let retained_before_default = upstream.metadata.retained_scope_opens.load(Ordering::SeqCst);
    let default_annotation = fixture.success(
        &root,
        &[
            "file",
            "annotate",
            "capture-only.txt",
            "-r",
            &captured_revision,
        ],
    );
    assert_eq!(upstream.metadata.retained_scope_opens.load(Ordering::SeqCst) - retained_before_default, 1,
        "ordinary retained annotation shares its canonical index root instead of reopening the frozen source");
    assert!(!default_annotation.contains("<Error"));
    assert!(default_annotation.contains(std::str::from_utf8(authored_bytes).unwrap().trim_end()));
    let inherited_annotation = fixture.success(
        &root,
        &["file", "annotate", "hello.cc", "-r", &captured_revision],
    );
    assert!(!inherited_annotation.contains("<Error"));
    assert_eq!(
        upstream.metadata.annotation_nodes.load(Ordering::SeqCst),
        nodes_before,
        "retained annotation keeps canonical ancestry metadata remote"
    );
    assert!(
        upstream.metadata.blame_policies.lock().unwrap().is_empty(),
        "ordinary retained annotation uses the same checked continuation graph"
    );
    assert!(upstream.metadata.query_commit_bytes.load(Ordering::SeqCst) > canonical_before);
    assert_eq!(
        upstream
            .metadata
            .ordinary_scope_opens
            .load(Ordering::SeqCst),
        ordinary_scopes_before,
        "frozen ordinary annotate must not open a live scope for origin metadata or prefixes"
    );
    if let Some(merge) = &capture.merged_path_log {
        let retained_before = upstream
            .metadata
            .retained_scope_opens
            .load(Ordering::SeqCst);
        for selector in [
            "file:merge-log.txt",
            "merge-log.txt",
            "glob:merge-*.txt",
            "glob:*.txt ~ capture-only.txt",
        ] {
            let args = [
                "--ignore-working-copy",
                "log",
                "--no-graph",
                "-r",
                &merge.candidates,
                "-T",
                "commit_id ++ \"\\n\"",
                selector,
            ];
            let output = fixture.run(&root, &args);
            assert!(
                output.status.success(),
                "frozen {selector}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                output.stdout, merge.expected,
                "frozen ordinary log preserves the pre-capture stock native-ID result"
            );
        }
        assert!(
            upstream
                .metadata
                .retained_scope_opens
                .load(Ordering::SeqCst)
                > retained_before,
            "ordinary frozen path log must use retained scope admission"
        );
        assert_eq!(
            upstream
                .metadata
                .ordinary_scope_opens
                .load(Ordering::SeqCst),
            ordinary_scopes_before,
            "frozen merge selection must not open a live source"
        );
        assert_eq!(
            upstream.file_reads(),
            file_reads_before,
            "frozen merge work stays remote; no client historical FILE fallback"
        );
        assert_eq!(
            upstream.metadata.writes.load(Ordering::SeqCst),
            writes_before_queries,
            "frozen merged-parent scratch is never authoritative"
        );
        assert_eq!(upstream.public(), public_before_queries);
    }
    let search: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree",
            "search",
            "-F",
            "private captured workspace",
            "--path-prefix",
            "capture-only.txt",
            "-r",
            &captured_revision,
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(search["coverage"], "complete");
    assert_eq!(search["revision"], captured_revision);
    assert_eq!(search["hits"].as_array().unwrap().len(), 1);
    assert_eq!(search["hits"][0]["path"], "capture-only.txt");
    assert_eq!(
        upstream.file_reads(),
        file_reads_before,
        "retained queries must not download historical FILE bodies to the client"
    );
    assert_eq!(upstream.public(), public_before_queries);
    assert_eq!(
        upstream.metadata.writes.load(Ordering::SeqCst),
        writes_before_queries
    );
    assert_eq!(fs::read(&remote_state_path).ok(), remote_state_before);
    assert_eq!(
        fs::read(root.join("capture-only.txt")).unwrap(),
        authored_bytes
    );
    let cache_after_read = fixture.control(Request::Status).unwrap()["cache"].clone();
    assert!(
        cache_after_read["fetches"].as_u64().unwrap() > cache_before["fetches"].as_u64().unwrap()
    );
    assert!(cache_after_read["verified_bytes"].as_u64().unwrap() >= authored_bytes.len() as u64);

    // A new private child is not part of the frozen cloud selection. Its FILE
    // bytes must be composed locally, not mistaken for remotely readable data.
    fixture.success(&root, &["new", "-m", "post-restore private edit"]);
    let mut private_bytes = authored_bytes.to_vec();
    private_bytes.extend_from_slice(b"restored and snapshotted edit\n");
    fs::write(root.join("capture-only.txt"), &private_bytes).unwrap();
    fixture.success(&root, &["status"]);
    let current = native_admin_state(&binding);
    let (wc, _) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
            .unwrap();
    assert_ne!(wc.operation.hex(), capture.data.working_copy.wc_operation);
    let mut dirty = private_bytes;
    dirty.extend_from_slice(b"unsnapshotted restored edit across restart\n");
    fs::write(root.join("capture-only.txt"), &dirty).unwrap();
    assert_eq!(inherited_annotation.as_bytes(), inherited_oracle);
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    assert!(
        !root.join("capture-only.txt").exists(),
        "no materialized checkout remains"
    );
    fixture.stop_daemon();
    fixture.start();
    fixture.success(
        &base,
        &[
            "tinytree",
            "resume",
            root.to_str().unwrap(),
            "--daemon-config",
            &configuration,
        ],
    );
    assert_fuse_mounted(&root);
    assert_eq!(fixture.binding(&root), binding);
    assert_eq!(
        native_admin_state(&binding),
        current,
        "committed resume must preserve CURRENT metadata, not reapply the capture"
    );
    assert_eq!(fs::read(root.join("capture-only.txt")).unwrap(), dirty);
    let reads_before_dirty_queries = upstream.file_reads();
    let dirty_annotation = fixture.run(
        &root,
        &["file", "annotate", "capture-only.txt", "-T", "content"],
    );
    assert!(
        dirty_annotation.status.success(),
        "{}",
        String::from_utf8_lossy(&dirty_annotation.stderr)
    );
    assert_eq!(dirty_annotation.stdout, dirty);
    assert_eq!(
        upstream
            .metadata
            .ordinary_scope_opens
            .load(Ordering::SeqCst),
        ordinary_scopes_before
    );
    let dirty_blame: serde_json::Value = serde_json::from_str(
        &fixture.success(&root, &["tinytree", "blame", "capture-only.txt", "--max-commits", "512", "--json"]),
    )
    .unwrap();
    assert_eq!(dirty_blame["coverage"], "complete");
    assert!(dirty_blame["local_commits"].as_u64().unwrap() >= 1);
    assert_eq!(
        dirty_blame["lines"][0]["origin"],
        blame["lines"][0]["origin"]
    );
    let dirty_revision = fixture.revision(&root, "@");
    assert_eq!(dirty_blame["revision"], dirty_revision);
    assert_eq!(dirty_blame["lines"][2]["origin"]["commit"], dirty_revision);
    let reconstructed: Vec<u8> = dirty_blame["lines"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|line| {
            line["bytes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|byte| byte.as_u64().unwrap() as u8)
        })
        .collect();
    assert_eq!(reconstructed, dirty);
    let dirty_search: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree",
            "search",
            "-F",
            "restored edit",
            "--path-prefix",
            "capture-only.txt",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(dirty_search["coverage"], "complete");
    assert_eq!(dirty_search["revision"], dirty_revision);
    assert_eq!(dirty_search["hits"].as_array().unwrap().len(), 1);
    assert_eq!(dirty_search["hits"][0]["line"], 3);
    // Removing the captured line must suppress its frozen remote search hit,
    // not merge an obsolete result back into the current local file.
    dirty.drain(..authored_bytes.len());
    fs::write(root.join("capture-only.txt"), &dirty).unwrap();
    let removed: serde_json::Value = serde_json::from_str(&fixture.success(
        &root,
        &[
            "tinytree",
            "search",
            "-F",
            "private captured workspace",
            "--path-prefix",
            "capture-only.txt",
            "--json",
        ],
    ))
    .unwrap();
    assert_eq!(removed["coverage"], "complete");
    assert!(removed["hits"].as_array().unwrap().is_empty());
    assert_eq!(upstream.file_reads(), reads_before_dirty_queries);
    assert_eq!(upstream.public(), public_before_queries);
    assert_eq!(
        upstream.metadata.writes.load(Ordering::SeqCst),
        writes_before_queries
    );
    assert_eq!(fs::read(&remote_state_path).ok(), remote_state_before);
    fixture.success(&root, &["status"]);
    assert_eq!(
        fixture
            .success(&root, &["file", "show", "capture-only.txt"])
            .as_bytes(),
        dirty
    );
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
}

fn write_synced(path: &Path, bytes: &[u8]) {
    let mut file = fs::File::create(path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

// Read only selected external metadata bodies. Other entries contribute their
// names and kinds, never credentials, cache tokens, or source FILE payloads.
fn native_metadata_snapshot(
    root: &Path,
    include_bytes: fn(&Path) -> bool,
) -> BTreeMap<PathBuf, (u8, Option<Vec<u8>>)> {
    assert!(fs::symlink_metadata(root).unwrap().file_type().is_dir(),
        "fixture metadata snapshot requires a real directory root");
    let mut result = BTreeMap::new();
    let mut pending = vec![root.to_owned()];
    let mut remaining = 256 * 1024 * 1024u64;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_owned();
            assert!(result.len() < 65_536 && relative.components().count() <= 16,
                "fixture metadata namespace exceeds snapshot bounds");
            let metadata = fs::symlink_metadata(&path).unwrap();
            assert!(!metadata.file_type().is_symlink(),
                "fixture metadata snapshot refuses symbolic links");
            let kind = if metadata.is_dir() { 0 } else if metadata.is_file() { 1 } else { 3 };
            let bytes = if metadata.is_file() && include_bytes(&relative) {
                remaining = remaining.checked_sub(metadata.len())
                    .expect("fixture metadata bytes exceed snapshot bound");
                Some(fs::read(&path).unwrap())
            } else { None };
            if metadata.is_dir() { pending.push(path); }
            result.insert(relative, (kind, bytes));
        }
    }
    result
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
    capture_policy: &'static str,
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
        capture_policy: &'static str,
    ) -> Self {
        Self {
            upstream,
            history,
            trial,
            index_mode,
            capture_policy,
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
                    "status":"failed_or_timeout","historical_commits":self.history,"trial":self.trial,"index_mode":self.index_mode,"local_capture_policy":self.capture_policy,
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
    if let Some(capture) = stats.get("local_capture") {
        let mut counters = serde_json::Map::new();
        for field in [
            "policy",
            "pending_count",
            "pending_bytes",
            "pending_dirty",
            "pending_clean",
            "pending_unavailable",
            "inventory_available",
            "pending_root_claims",
            "attempted",
            "queued",
            "skipped",
            "failed",
            "last_disposition",
        ] {
            if let Some(value) = capture.get(field) {
                counters.insert(field.into(), value.clone());
            }
        }
        output.insert("local_capture".into(), counters.into());
    }
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
    let capture_policy = match std::env::var("TINYTREE_MOUNTED_STARTUP_CAPTURE_POLICY") {
        Ok(value) if value == "disabled" => "disabled",
        Ok(value) if value == "after_command_best_effort" => "after_command_best_effort",
        Err(std::env::VarError::NotPresent) => "disabled",
        _ => panic!(
            "TINYTREE_MOUNTED_STARTUP_CAPTURE_POLICY must be disabled or after_command_best_effort"
        ),
    };
    assert!(
        capture_policy == "disabled" || index_mode == "federated",
        "capture benchmark requires the supported federated index"
    );
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
            let mut failure =
                StartupTrialFailure::new(&upstream, history, trial, index_mode, capture_policy);
            let fixture_started = Instant::now();
            assert!(fixture_owner.set(Fixture::new()).is_ok());
            let fixture = fixture_owner.get_mut().unwrap();
            fixture.configure_remote(&upstream.endpoint);
            fixture.index_mode(index_mode);
            fixture.capture_policy(capture_policy);
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
                    "local_capture_policy":capture_policy,"best_effort_capture_cost_included_in_command_latency":capture_policy!="disabled",
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

// The dedicated manual Bazel fixture enables this feature; ordinary mounted
// acceptance does not compile or run the iterative measurement workload.
#[cfg(feature = "native-gc-benchmark")]
mod native_gc_measurements {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    const FILE_BYTES: usize = 4096;
    const TRIALS: usize = 3;

    fn nanos(started: Instant) -> u64 {
        u64::try_from(started.elapsed().as_nanos()).unwrap()
    }
    fn file_version(iteration: usize) -> Vec<u8> {
        let mut bytes = vec![b'x'; FILE_BYTES];
        let prefix = format!("native local snapshot {iteration:04}\n");
        bytes[..prefix.len()].copy_from_slice(prefix.as_bytes());
        bytes
    }
    fn number(value: &serde_json::Value, name: &str) -> u64 {
        value[name].as_u64().unwrap()
    }
    fn command_statistics(samples: &[u64]) -> serde_json::Value {
        assert!(!samples.is_empty());
        let mut ordered = samples.to_vec();
        ordered.sort_unstable();
        let middle = ordered.len() / 2;
        let median = (ordered[(ordered.len() - 1) / 2] as u128 + ordered[middle] as u128) / 2;
        let p95 = (ordered.len() * 95).div_ceil(100) - 1;
        serde_json::json!({"median_ns":median,"p95_ns":ordered[p95],
            "first_ns":samples[0],"last_ns":samples[samples.len()-1],"samples_ns":samples})
    }
    fn stopped_store_disk(root: &Path) -> serde_json::Value {
        let mut pending = vec![root.to_owned()];
        let mut inodes = HashSet::new();
        let mut apparent = 0u64;
        let mut allocated = 0u64;
        let mut files = 0u64;
        while let Some(path) = pending.pop() {
            let metadata = fs::symlink_metadata(&path).unwrap();
            assert!(!metadata.file_type().is_symlink());
            if !inodes.insert((metadata.dev(), metadata.ino())) {
                continue;
            }
            allocated += metadata.blocks().checked_mul(512).unwrap();
            if metadata.is_dir() {
                for entry in fs::read_dir(path).unwrap() {
                    pending.push(entry.unwrap().path());
                }
            } else {
                assert!(metadata.is_file());
                apparent += metadata.len();
                files += 1;
            }
        }
        serde_json::json!({"apparent_file_bytes":apparent,"allocated_bytes":allocated,
            "regular_files":files,"owner_stopped":true})
    }
    fn revision_at(fixture: &mut Fixture, root: &Path, operation: Option<&str>) -> String {
        let mut args = vec!["--ignore-working-copy"];
        if let Some(operation) = operation {
            args.extend(["--at-op", operation]);
        }
        args.extend(["log", "--no-graph", "-r", "@", "-T", "commit_id"]);
        let revision = fixture.success(root, &args).trim().to_owned();
        assert_eq!(revision.len(), 64);
        assert!(revision.bytes().all(|byte| byte.is_ascii_hexdigit()));
        revision
    }
    fn old_file(fixture: &mut Fixture, root: &Path, operation: &str) -> Vec<u8> {
        fixture
            .success(
                root,
                &[
                    "--ignore-working-copy",
                    "--at-op",
                    operation,
                    "file",
                    "show",
                    "working.txt",
                ],
            )
            .into_bytes()
    }
    fn run_trial(iterations: usize, trial: usize, complete: bool) -> serde_json::Value {
        let mut fixture = Fixture::new();
        if complete {
            // Policy is selected before ANY native storage exists and never
            // changed for this cohort. Grace0 belongs only to the fixture.
            fixture.enable_local_gc();
        }
        let started = Instant::now();
        fixture.start();
        let daemon_start_ns = nanos(started);
        let base = fixture.base().to_owned();
        let root = base.join("workspace");
        let started = Instant::now();
        fixture.initialize(&root);
        let initialize_mount_ns = nanos(started);
        let binding = fixture.binding(&root);
        assert_eq!(
            fs::read_to_string(binding.repo.join("index/type")).unwrap(),
            jj_lib::default_index::DefaultIndexStore::NAME
        );
        let corpus: Vec<_> = (1..=iterations).map(file_version).collect();
        let mut digest = blake3::Hasher::new();
        for bytes in &corpus {
            digest.update(bytes);
        }
        let before = fixture.control(Request::Status).unwrap()["native"]["commands"].clone();
        assert_eq!(before["collection_enabled"], complete);
        let mut samples = Vec::with_capacity(iterations);
        let loop_started = Instant::now();
        for bytes in &corpus {
            fs::write(root.join("working.txt"), bytes).unwrap();
            let command_started = Instant::now();
            fixture.success(&root, &["status"]);
            samples.push(nanos(command_started));
        }
        let iteration_wall_ns = nanos(loop_started);
        let after = fixture.control(Request::Status).unwrap()["native"]["commands"].clone();
        assert_eq!(after["active_commands"], 0);
        let clean = number(&after["local_gc"], "clean_checkpoints")
            - number(&before["local_gc"], "clean_checkpoints");
        let blocked = number(&after["local_gc"], "blocked_checkpoints")
            - number(&before["local_gc"], "blocked_checkpoints");
        assert_eq!(clean, if complete { iterations as u64 } else { 0 });
        assert_eq!(
            blocked, 0,
            "never measure a failed checkpoint as LocalComplete cost"
        );

        // Identical proof commands in both modes, entirely outside the timed
        // snapshot loop. The oldest of N successive snapshot operations binds
        // the first version; its expected contents are verified, not assumed.
        let operations = fixture.success(
            &root,
            &[
                "--ignore-working-copy",
                "op",
                "log",
                "--no-graph",
                "-n",
                &iterations.to_string(),
                "-T",
                "id ++ \"\\n\"",
            ],
        );
        let operations: Vec<_> = operations.lines().filter(|line| !line.is_empty()).collect();
        assert_eq!(operations.len(), iterations);
        assert!(operations
            .iter()
            .all(|id| id.len() == 128 && id.bytes().all(|b| b.is_ascii_hexdigit())));
        let oldest = operations[operations.len() - 1].to_owned();
        let first_revision = revision_at(&mut fixture, &root, Some(&oldest));
        let last_revision = revision_at(&mut fixture, &root, None);
        assert_ne!(first_revision, last_revision);
        let (preview_ns, preview, apply_ns, applied) = if complete {
            let started = Instant::now();
            let preview = native_gc_report(&mut fixture, false);
            let preview_ns = nanos(started);
            assert_eq!(preview["status"], "planned");
            let started = Instant::now();
            let applied = native_gc_report(&mut fixture, true);
            let apply_ns = nanos(started);
            assert_eq!(applied["status"], "collected");
            (Some(preview_ns), preview, Some(apply_ns), applied)
        } else {
            (None, serde_json::Value::Null, None, serde_json::Value::Null)
        };
        assert_eq!(old_file(&mut fixture, &root, &oldest), corpus[0]);
        assert_eq!(
            fs::read(root.join("working.txt")).unwrap(),
            corpus[iterations - 1]
        );
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        assert!(
            !root.join("working.txt").exists(),
            "no materialized fallback remains"
        );
        fixture.stop_daemon();
        let store = fixture
            .socket
            .parent()
            .unwrap()
            .join("native/example/native-objects");
        let stopped_disk = stopped_store_disk(&store);

        let started = Instant::now();
        fixture.start();
        let restart_daemon_ns = nanos(started);
        let started = Instant::now();
        fixture.success(&base, &["tinytree", "resume", root.to_str().unwrap()]);
        let resume_mount_ns = nanos(started);
        assert_fuse_mounted(&root);
        assert_eq!(fixture.binding(&root), binding);
        assert_eq!(
            fs::read(root.join("working.txt")).unwrap(),
            corpus[iterations - 1]
        );
        assert_eq!(revision_at(&mut fixture, &root, None), last_revision);
        assert_eq!(
            revision_at(&mut fixture, &root, Some(&oldest)),
            first_revision
        );
        assert_eq!(old_file(&mut fixture, &root, &oldest), corpus[0]);
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        assert!(!root.join("working.txt").exists());
        fixture.stop_daemon();
        let restarted_disk = stopped_store_disk(&store);
        serde_json::json!({
            "schema":1,"benchmark":"native_mounted_local_gc_iterations","measurement":true,"status":"completed",
            "policy":if complete {"local_complete"} else {"disabled"},"iterations":iterations,"trial":trial,"trials":TRIALS,
            "file_bytes":FILE_BYTES,"payload_corpus_digest":digest.finalize().to_hex().to_string(),
            "workspace_provider":"actual_linux_fuse","index_mode":"legacy","external_network":false,
            "fresh_cohort_each_mode":true,"os_cache_uncontrolled":true,"fixture_grace_seconds":if complete {Some(0)} else {None},
            "daemon_start_ns":daemon_start_ns,"initialize_mount_ns":initialize_mount_ns,
            "iteration_wall_ns":iteration_wall_ns,"status_commands":command_statistics(&samples),
            "clean_checkpoints_in_loop":clean,"blocked_checkpoints_in_loop":blocked,
            "command_physical_drain_ns_in_loop":number(&after,"physical_drain_ns")-number(&before,"physical_drain_ns"),
            "retention_encoded_write_bytes_in_loop":number(&after,"retention_encoded_write_bytes")-number(&before,"retention_encoded_write_bytes"),
            "preview_cli_wall_ns":preview_ns,"preview":preview,"apply_cli_wall_ns":apply_ns,"apply":applied,
            "preview_primes_apply_cache":complete,"restart_daemon_ns":restart_daemon_ns,"resume_mount_ns":resume_mount_ns,
            "native_store_after_stop":stopped_disk,"native_store_after_restart_and_stop":restarted_disk,
            "disk_scope":"authoritative_native_store_only_excludes_jj_admin_core_cas_and_registry",
            "allocated_method":"namespace_st_blocks_times512_unique_inode_including_directories_not_df_or_rss",
            "first_and_last_history_verified_after_restart":true,"full_history_retained":true,"synthetic_orphans_injected":false,
            "cli_exit_poll_ms":10,"daemon_readiness_poll_ms":20,"no_disk_plateau_or_remote_bazel_claim":true
        })
    }

    #[test]
    #[ignore = "opt-in native GC iteration measurement"]
    fn native_gc_iteration_benchmark() {
        let profiles = match std::env::var("TINYTREE_MOUNTED_GC_BENCH_ITERATIONS") {
            Err(std::env::VarError::NotPresent) => vec![16, 128],
            Ok(value) if value == "16" => vec![16],
            Ok(value) if value == "128" => vec![128],
            Ok(value) if value == "1024" => vec![1024],
            _ => panic!("native GC benchmark iterations must be 16, 128 or 1024"),
        };
        for iterations in profiles {
            for trial in 1..=TRIALS {
                for complete in [false, true] {
                    // A success row is emitted only after this complete trial's
                    // history/GC/restart proofs have all passed.
                    eprintln!("{}", run_trial(iterations, trial, complete));
                }
            }
        }
    }
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn immutable_unpublished_candidate_mount_survives_restart_without_editable_downgrade() {
    let upstream = RemoteServer::start();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.start();
    let original = upstream.public();
    let descriptor = original.commit.unwrap();
    let (_, bytes) = upstream
        .store
        .read_metadata("example", descriptor.key)
        .unwrap();
    let m::Object::Commit(mut candidate) = m::decode(&bytes, m::Limits::default()).unwrap() else {
        unreachable!()
    };
    candidate.description = "unpublished immutable check candidate".into();
    candidate.change_id = m::ChangeId([0xc1; 16]);
    let candidate = m::encode(&m::Object::Commit(candidate), m::Limits::default()).unwrap();
    upstream
        .store
        .put("example", candidate.descriptor, &candidate.bytes, 0)
        .unwrap();
    let commit = blake3::Hash::from(candidate.descriptor.key.id.0)
        .to_hex()
        .to_string();
    assert_ne!(commit, upstream.head);
    let root = fixture.base().join("candidate");
    let id = "c1".repeat(32);
    let args = [
        "native-candidate-mount",
        root.to_str().unwrap(),
        "--store",
        "example",
        "--commit",
        &commit,
        "--workspace-id",
        &id,
    ];
    let creating = fixture.workspace(&args);
    assert!(
        creating.status.success(),
        "{}",
        String::from_utf8_lossy(&creating.stderr)
    );
    assert_fuse_mounted(&root);
    assert_eq!(upstream.file_reads(), 0);
    let record_path = fixture
        .base()
        .join("daemon/native-workspaces")
        .join(&id)
        .join("workspace.json");
    let recorded = fs::read(&record_path).unwrap();
    let record: serde_json::Value = serde_json::from_slice(&recorded).unwrap();
    assert_eq!(record["version"], 3);
    assert_eq!(record["origin"]["kind"], "candidate");
    assert!(fixture
        .control(Request::NativeMount {
            root: root.clone(),
            store: "example".into(),
            commit: commit.clone(),
            workspace_id: id.clone()
        })
        .is_err());
    let changed = fixture.workspace(&[
        "native-candidate-mount",
        root.to_str().unwrap(),
        "--store",
        "example",
        "--commit",
        &upstream.head,
        "--workspace-id",
        &id,
    ]);
    assert!(!changed.status.success());
    assert_eq!(fs::read(&record_path).unwrap(), recorded);
    let file = root.join("hello.cc");
    assert_eq!(
        fs::OpenOptions::new()
            .write(true)
            .open(&file)
            .unwrap_err()
            .raw_os_error(),
        Some(30)
    );
    assert_eq!(
        fs::write(root.join("new"), b"blocked")
            .unwrap_err()
            .raw_os_error(),
        Some(30)
    );
    assert_eq!(
        fs::rename(&file, root.join("renamed"))
            .unwrap_err()
            .raw_os_error(),
        Some(30)
    );
    assert_eq!(upstream.file_reads(), 0);
    assert_eq!(fs::read(&file).unwrap(), upstream.source_bytes);
    assert_eq!(
        upstream.public(),
        original,
        "candidate admission must not move the public reference"
    );
    fixture
        .control(Request::NativeUnmount {
            workspace_id: id.clone(),
        })
        .unwrap();
    assert!(!fuse_mounted(&root));
    fixture.stop_daemon();
    fixture.start();
    let resumed = fixture.workspace(&["native-resume", root.to_str().unwrap()]);
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let replay = fixture.workspace(&args);
    assert!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert_fuse_mounted(&root);
    assert_eq!(
        fs::write(root.join("after-restart"), b"blocked")
            .unwrap_err()
            .raw_os_error(),
        Some(30)
    );
    assert_eq!(fs::read(&file).unwrap(), upstream.source_bytes);
    fixture
        .control(Request::NativeUnmount { workspace_id: id })
        .unwrap();
    fixture.stop_daemon();
    if std::env::var_os("TINYTREE_TEST_RETAIN_CANDIDATE").as_deref()
        == Some(std::ffi::OsStr::new("1"))
    {
        let kept = fixture.directory.take().unwrap().keep();
        eprintln!("TINYTREE_IMMUTABLE_FIXTURE={}", kept.display());
    }
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; run mounted_tests locally"]
fn stopped_immutable_candidates_retire_locally_and_release_workspace_quota() {
    let upstream = RemoteServer::start();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    let config = fs::read_to_string(&fixture.daemon_config).unwrap().replace(
        "[native.workspaces]\n",
        "[native.workspaces]\nmax_workspaces=1\n",
    );
    fs::write(&fixture.daemon_config, config).unwrap();
    fixture.start();
    let original = upstream.public();
    let id = "c2".repeat(32);
    let root = fixture.base().join("retiring-candidate");
    let mount = |fixture: &mut Fixture, id: &str, root: &Path| {
        fixture.workspace(&[
            "native-candidate-mount",
            root.to_str().unwrap(),
            "--store",
            "example",
            "--commit",
            &upstream.head,
            "--workspace-id",
            id,
        ])
    };
    let retire = |fixture: &mut Fixture, id: &str, commit: &str| {
        fixture.workspace(&[
            "native-candidate-retire",
            "--workspace-id",
            id,
            "--expected-commit",
            commit,
        ])
    };
    assert!(mount(&mut fixture, &id, &root).status.success());
    let (_, commit_bytes) = upstream
        .store
        .read_metadata("example", original.commit.unwrap().key)
        .unwrap();
    let m::Object::Commit(commit_object) = m::decode(&commit_bytes, m::Limits::default()).unwrap()
    else {
        unreachable!()
    };
    let inspected = fixture.workspace(&[
        "native-candidate-inspect",
        "--workspace-id",
        &id,
        "--commit",
        &upstream.head,
    ]);
    assert!(
        inspected.status.success(),
        "{}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let proof: serde_json::Value = serde_json::from_slice(&inspected.stdout).unwrap();
    assert_eq!(
        proof["report"],
        serde_json::json!({"version":1,"workspace_id":id,"commit":upstream.head,
        "root_tree":blake3::Hash::from(commit_object.root_tree.terms[0].0).to_hex().to_string(),
        "access":"immutable_candidate","root":root,"store":"example","active":true})
    );
    assert!(!fixture
        .workspace(&[
            "native-candidate-inspect",
            "--workspace-id",
            &id,
            "--commit",
            &"11".repeat(32)
        ])
        .status
        .success());
    assert!(fixture
        .control(Request::NativeCandidateInspect {
            workspace_id: id.clone(),
            commit: upstream.head.clone()
        })
        .is_err());

    assert!(
        !retire(&mut fixture, &id, &upstream.head).status.success(),
        "active candidate must be preserved"
    );
    assert!(
        fixture
            .control(Request::NativeCandidateRetire {
                workspace_id: id.clone(),
                expected_commit: upstream.head.clone()
            })
            .is_err(),
        "unscoped retirement must be rejected"
    );
    let second_id = "c3".repeat(32);
    let second_root = fixture.base().join("next-candidate");
    assert!(
        !mount(&mut fixture, &second_id, &second_root)
            .status
            .success(),
        "the one-workspace quota must be real"
    );
    assert!(fixture
        .workspace(&["native-unmount", "--workspace-id", &id])
        .status
        .success());
    assert!(!fixture
        .workspace(&[
            "native-candidate-inspect",
            "--workspace-id",
            &id,
            "--commit",
            &upstream.head
        ])
        .status
        .success());
    let child = fixture.base().join("daemon/native-workspaces").join(&id);
    let record_bytes = fs::read(child.join("workspace.json")).unwrap();
    assert!(!retire(&mut fixture, &id, &"11".repeat(32)).status.success());
    assert_eq!(
        fs::read(child.join("workspace.json")).unwrap(),
        record_bytes
    );
    let header_path = child.join("mount/native-vfs.json");
    let header_bytes = fs::read(&header_path).unwrap();
    let mut header: serde_json::Value = serde_json::from_slice(&header_bytes).unwrap();
    header["ready"] = false.into();
    fs::write(&header_path, serde_json::to_vec(&header).unwrap()).unwrap();
    assert!(
        !retire(&mut fixture, &id, &upstream.head).status.success(),
        "unready namespaces cannot be discarded"
    );
    fs::write(&header_path, &header_bytes).unwrap();
    fs::write(root.join("foreign"), b"preserve user data").unwrap();
    // Crash before initial receipt rename: partial atomic-write bytes must not
    // become disposal authority, nor prevent reopening this stopped candidate.
    fixture.stop_daemon();
    let temporary = fixture
        .base()
        .join("daemon/native-candidate-retirements/.private-1-1.tmp");
    use std::os::unix::fs::OpenOptionsExt;
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .unwrap()
        .write_all(b"{incomplete")
        .unwrap();
    let calls_before_temp_recovery = upstream.metadata.transport_calls.load(Ordering::SeqCst);
    fixture.start();
    assert_eq!(
        upstream.metadata.transport_calls.load(Ordering::SeqCst),
        calls_before_temp_recovery
    );
    assert!(!temporary.exists());
    assert!(
        child.join("mount/native-vfs.json").exists(),
        "an uncommitted temporary cannot retire its candidate"
    );
    let calls_before_retire = upstream.metadata.transport_calls.load(Ordering::SeqCst);
    let result = retire(&mut fixture, &id, &upstream.head);
    assert!(
        result.status.success(),
        "{}; candidate children={:?}; mount children={:?}",
        String::from_utf8_lossy(&result.stderr),
        fs::read_dir(&child)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<Vec<_>>(),
        fs::read_dir(child.join("mount"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        upstream.metadata.transport_calls.load(Ordering::SeqCst),
        calls_before_retire,
        "retirement is local, not a provider operation"
    );
    let response: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(response["report"]["retired"], true);
    assert_eq!(response["report"]["root_removed"], false);
    assert_eq!(response["report"]["retention_gc"], false);
    assert!(!child.exists());
    assert!(!fixture.registry.join("admin").join(&id).exists());
    assert_eq!(
        fs::read(root.join("foreign")).unwrap(),
        b"preserve user data"
    );
    assert!(retire(&mut fixture, &id, &upstream.head).status.success());
    assert!(!retire(&mut fixture, &id, &"11".repeat(32)).status.success());
    assert!(
        !mount(&mut fixture, &id, &root).status.success(),
        "retired IDs cannot be reused"
    );
    assert!(fixture
        .control(Request::NativeMount {
            root: root.clone(),
            store: "example".into(),
            commit: upstream.head.clone(),
            workspace_id: id.clone()
        })
        .is_err());
    assert!(
        mount(&mut fixture, &second_id, &second_root)
            .status
            .success(),
        "retirement must release max_workspaces quota"
    );
    assert_eq!(
        fs::read(second_root.join("hello.cc")).unwrap(),
        upstream.source_bytes
    );
    assert_eq!(
        upstream.public(),
        original,
        "retirement must not alter canonical references"
    );
    fixture
        .control(Request::NativeUnmount {
            workspace_id: second_id.clone(),
        })
        .unwrap();
    assert!(retire(&mut fixture, &second_id, &upstream.head)
        .status
        .success());
    assert!(
        !second_root.exists(),
        "only empty mount directories should be removed"
    );
    fixture.stop_daemon();

    // Model a crash prefix: durable intent survived, namespace/admin cleanup
    // finished, but workspace.json and completion publication did not. Startup
    // must finish this locally rather than initialize or contact a provider.
    let receipt_path = fixture
        .base()
        .join("daemon/native-candidate-retirements")
        .join(format!("{id}.json"));
    let mut receipt: serde_json::Value =
        serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    receipt["complete"] = false.into();
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)
        .unwrap()
        .write_all(b"{incomplete completion")
        .unwrap();

    fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
    fs::create_dir(&child).unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&child, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(child.join("workspace.json"), &record_bytes).unwrap();
    let calls_before_replay = upstream.metadata.transport_calls.load(Ordering::SeqCst);
    fixture.start();
    assert_eq!(
        upstream.metadata.transport_calls.load(Ordering::SeqCst),
        calls_before_replay,
        "pending retirement recovery must not contact a provider"
    );
    assert!(!temporary.exists());
    assert!(!child.exists());
    assert_eq!(
        fs::read(root.join("foreign")).unwrap(),
        b"preserve user data"
    );
    assert!(retire(&mut fixture, &id, &upstream.head).status.success());
    assert!(
        !mount(&mut fixture, &id, &root).status.success(),
        "restart cannot forget retired identities"
    );
    assert!(!fixture
        .workspace(&["native-resume", root.to_str().unwrap()])
        .status
        .success());
    let rw_id = "c4".repeat(32);
    let rw_root = fixture.base().join("editable");
    fixture
        .control(Request::NativeMount {
            root: rw_root.clone(),
            store: "example".into(),
            commit: upstream.head.clone(),
            workspace_id: rw_id.clone(),
        })
        .unwrap();
    assert!(
        !fixture
            .workspace(&[
                "native-candidate-inspect",
                "--workspace-id",
                &rw_id,
                "--commit",
                &upstream.head
            ])
            .status
            .success(),
        "editable mounts cannot produce immutable proof"
    );
    let reads_before = upstream.file_reads();
    assert_eq!(
        fs::read(rw_root.join("hello.cc")).unwrap(),
        upstream.source_bytes
    );
    assert_eq!(
        upstream.file_reads(),
        reads_before,
        "shared clean CAS survives candidate retirement"
    );
    fixture
        .control(Request::NativeUnmount {
            workspace_id: rw_id.clone(),
        })
        .unwrap();
    assert!(
        !retire(&mut fixture, &rw_id, &upstream.head)
            .status
            .success(),
        "ordinary writable workspaces are not disposable candidates"
    );
    assert!(fixture
        .base()
        .join("daemon/native-workspaces")
        .join(rw_id)
        .exists());
    fixture.stop_daemon();
}

impl RemoteServer {
    fn landing_owner() -> tinytree_store::LandingOwner {
        // Trusted fixture authority only. Actual Start/Get authenticate through
        // the real service; no client verdict/finalize RPC exists.
        let name = b"mounted-writer";
        let mut retention = blake3::Hasher::new();
        retention.update(b"tinytree-retention-owner-v1\0");
        retention.update(&(name.len() as u64).to_be_bytes());
        retention.update(name);
        let mut owner = blake3::Hasher::new();
        owner.update(b"tinytree-landing-owner-v1\0");
        owner.update(&[1]);
        owner.update(retention.finalize().as_bytes());
        tinytree_store::LandingOwner(*owner.finalize().as_bytes())
    }
    fn finish_fixture_landing(&self, key: &str, passed: bool) -> tinytree_store::LandingJob {
        let owner = Self::landing_owner();
        let job = self
            .store
            .landing_job("example", owner, key)
            .unwrap()
            .unwrap();
        let candidate = job.candidate.unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        self.store
            .record_landing_check(
                "example",
                owner,
                key,
                &tinytree_store::LandingCheckIdentity {
                    candidate: candidate.head,
                    root_tree: candidate.root_tree,
                    profile: job.profile,
                },
                tinytree_store::LandingVerdict {
                    passed,
                    evidence: [61; 32],
                },
                now,
            )
            .unwrap();
        self.store
            .finalize_landing("example", owner, key, now)
            .unwrap()
    }
}
fn landing_report(fixture: &mut Fixture, root: &Path, args: &[&str]) -> serde_json::Value {
    serde_json::from_str(&fixture.success(root, args)).unwrap()
}
fn kill_landing_at(fixture: &Fixture, root: &Path, args: &[&str], point: &str) -> String {
    let marker = fixture.base().join(format!(
        "landing-{}-{point}.marker",
        root.file_name().unwrap().to_str().unwrap()
    ));
    let stdout = fixture.base().join(format!("landing-{point}.out"));
    let stderr = fixture.base().join(format!("landing-{point}.err"));
    let binary = Path::new(env!("CARGO_BIN_EXE_landing_fault_driver"))
        .canonicalize()
        .unwrap();
    let mut command = Command::new(binary);
    fixture.environment(&mut command);
    command
        .current_dir(root)
        .args(args)
        .env("PATH", &fixture.empty_path)
        .env("TINYTREE_LANDING_TEST_POINT", point)
        .env("TINYTREE_LANDING_TEST_MARKER", &marker)
        .stdout(fs::File::create(&stdout).unwrap())
        .stderr(fs::File::create(&stderr).unwrap());
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !marker.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "fault driver exited before {point}: {}",
            fs::read_to_string(&stderr).unwrap()
        );
        assert!(
            Instant::now() < deadline,
            "fault driver did not reach {point}: {}",
            fs::read_to_string(&stderr).unwrap()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    // Kernel lease release is not physical completion. Wait for the daemon's
    // existing lease poller to drain the killed command before starting another.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = fixture.control(Request::Status).unwrap();
        if status["native"]["commands"]["active_commands"] == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "killed native command did not physically drain: {status}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    fs::read_to_string(marker).unwrap()
}
#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn native_landing_preserves_iteration_and_replays_exact_unpublished_evolution_after_client_death() {
    let upstream = RemoteServer::start_with_landing();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("federated");
    fixture.start();
    let base = fixture.base().to_owned();
    let root = base.join("landing");
    fixture.clone_remote(&root);
    let binding = fixture.binding(&root);
    fs::write(root.join("authored.txt"), b"submitted source\n").unwrap();
    fixture.success(&root, &["describe", "-m", "submitted source"]);
    let submitted = fixture.revision(&root, "@");
    let submit = [
        "tinytree",
        "land",
        "--base",
        upstream.head.as_str(),
        "--reference",
        "main",
        "--bookmark",
        "main",
        "--profile",
        "checks",
        "--profile-version",
        "v1",
    ];
    // Actual lost response: Start was accepted, but the CLI is killed before
    // persisting its returned job ID. The original intent/key stays durable.
    kill_landing_at(&fixture, &root, &submit, "response_received");
    let (_, state) = remote_observation(&binding);
    assert!(state["landing"].get("job_id").is_none());
    let key = state["landing"]["request_id"].as_str().unwrap().to_owned();
    assert_ne!(fixture.revision(&root, "@"), submitted);
    assert_eq!(
        fixture.revision(&root, "@-"),
        submitted,
        "submission leaves a fresh working-copy child"
    );
    fs::write(
        root.join("after.txt"),
        b"edits made while server checks run\n",
    )
    .unwrap();
    fixture.success(&root, &["status"]);
    assert_eq!(
        upstream.public().commit.unwrap().key.id.0,
        *blake3::Hash::from_hex(&upstream.head).unwrap().as_bytes()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let prepared = loop {
        let report = landing_report(&mut fixture, &root, &["tinytree", "land", "--status"]);
        if report["phase"] == "prepared_pending_checks" {
            break report;
        }
        assert!(
            Instant::now() < deadline,
            "candidate was not prepared: {report}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    let candidate = prepared["candidate"].as_str().unwrap().to_owned();
    assert_ne!(candidate, submitted);
    assert_eq!(
        upstream.file_reads(),
        0,
        "submit/status must not read any unchanged source FILE"
    );
    assert_eq!(
        upstream.historical_tree_reads(),
        0,
        "thin staging must not walk unrelated historical trees"
    );
    assert_eq!(
        upstream
            .store
            .scan_pending_landings("example", None, Default::default())
            .unwrap()
            .jobs
            .len(),
        1,
        "lost response must not allocate another job"
    );
    let committed = upstream.finish_fixture_landing(&key, true);
    assert_eq!(committed.state, tinytree_store::LandingState::Committed);
    assert_eq!(
        committed
            .receipt
            .as_ref()
            .unwrap()
            .reference
            .commit
            .unwrap()
            .key
            .id
            .0,
        *blake3::Hash::from_hex(&candidate).unwrap().as_bytes()
    );
    kill_landing_at(
        &fixture,
        &root,
        &["tinytree", "land", "--resume"],
        "apply_saved",
    );
    let (_, saved) = remote_observation(&binding);
    let exact_op = saved["landing"]["apply_operation"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(!saved["landing"]["acknowledged"].as_bool().unwrap());
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
    fixture.start();
    fixture.success(&base, &["tinytree", "resume", root.to_str().unwrap()]);
    let applied = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
    assert_eq!(applied["phase"], "applied");
    assert_eq!(applied["applied_operation"], exact_op);
    assert_eq!(applied["acknowledged"], true);
    assert_eq!(fixture.revision(&root, "@-"), candidate);
    assert_eq!(
        fs::read(root.join("after.txt")).unwrap(),
        b"edits made while server checks run\n"
    );
    assert_eq!(
        fs::read(root.join("authored.txt")).unwrap(),
        b"submitted source\n"
    );
    let evolution = fixture.success(
        &root,
        &[
            "evolog",
            "--no-graph",
            "-r",
            &candidate,
            "-T",
            "commit.commit_id() ++ \"\\n\"",
        ],
    );
    assert_eq!(evolution.lines().filter(|id| *id == candidate).count(), 1);
    assert_eq!(evolution.lines().filter(|id| *id == submitted).count(), 1);
    let replay = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
    assert_eq!(replay["applied_operation"], exact_op);
    let replay_evolution = fixture.success(
        &root,
        &[
            "evolog",
            "--no-graph",
            "-r",
            &candidate,
            "-T",
            "commit.commit_id() ++ \"\\n\"",
        ],
    );
    assert_eq!(
        replay_evolution, evolution,
        "receipt replay cannot duplicate evolution"
    );
    assert_eq!(upstream.file_reads(), 0);
    if let Some(binary) = std::env::var_os("TINYTREE_PRE_LANDING_CLI") {
        // Native-only optional compatibility proof against an independently
        // pinned historical executable, never the current mutable bazel-bin.
        let expected = std::env::var("TINYTREE_PRE_LANDING_CLI_BLAKE3").unwrap();
        let binary = PathBuf::from(binary);
        let mut file = fs::File::open(&binary).unwrap();
        assert!(file.metadata().unwrap().len() <= 512 * 1024 * 1024);
        let mut hash = blake3::Hasher::new();
        hash.update_reader(&mut file).unwrap();
        assert_eq!(hash.finalize().to_hex().as_str(), expected);
        let before_heads = native_operation_heads(&binding);
        let before_wc = jj_vfs::native::read_retained_state(
            &binding.root,
            &binding.working_copy,
            &binding.repository,
        )
        .unwrap()
        .persisted_bytes;
        let rejected = fixture.run_binary(&root, &binary, &["status"], false);
        assert!(!rejected.status.success());
        assert!(String::from_utf8_lossy(&rejected.stderr).contains("native broker locator"));
        assert_eq!(native_operation_heads(&binding), before_heads);
        let after_wc = jj_vfs::native::read_retained_state(
            &binding.root,
            &binding.working_copy,
            &binding.repository,
        )
        .unwrap()
        .persisted_bytes;
        assert_eq!(
            after_wc, before_wc,
            "legacy reader must reject before working-copy mutation"
        );
        assert_eq!(
            fs::read(root.join("after.txt")).unwrap(),
            b"edits made while server checks run\n"
        );
        eprintln!(
            "native landing historical reader rejected before repository/working-copy mutation"
        );
    }
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
}

fn prepare_native_landing(
    fixture: &mut Fixture,
    upstream: &RemoteServer,
    name: &str,
) -> (PathBuf, String, String, String) {
    prepare_native_landing_with_description(fixture, upstream, name, name)
}
fn prepare_native_landing_with_description(
    fixture: &mut Fixture,
    upstream: &RemoteServer,
    name: &str,
    description: &str,
) -> (PathBuf, String, String, String) {
    let root = fixture.base().join(name);
    fixture.clone_remote(&root);
    let base = fixture.revision(&root, "main");
    fs::write(root.join(format!("{name}.txt")), format!("source {name}\n")).unwrap();
    fixture.success(&root, &["describe", "-m", description]);
    let source = fixture.revision(&root, "@");
    landing_report(
        fixture,
        &root,
        &[
            "tinytree",
            "land",
            "--base",
            &base,
            "--reference",
            "main",
            "--bookmark",
            "main",
            "--profile",
            "checks",
            "--profile-version",
            "v1",
        ],
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let report = landing_report(fixture, &root, &["tinytree", "land", "--status"]);
        if report["phase"] == "prepared_pending_checks" {
            let key = report["request_id"].as_str().unwrap().to_owned();
            let candidate = report["candidate"].as_str().unwrap().to_owned();
            assert_eq!(fixture.revision(&root, "@-"), source);
            assert_eq!(
                upstream.file_reads(),
                0,
                "private edits must not hydrate unrelated FILEs"
            );
            return (root, source, key, candidate);
        }
        assert!(Instant::now() < deadline, "not prepared: {report}");
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn new_landing_fixture() -> (RemoteServer, Fixture, PathBuf) {
    let upstream = RemoteServer::start_with_landing();
    let mut fixture = Fixture::new();
    fixture.configure_remote(&upstream.endpoint);
    fixture.index_mode("federated");
    fixture.start();
    let base = fixture.base().to_owned();
    (upstream, fixture, base)
}
fn seed_landing_conflict_target(upstream: &RemoteServer) {
    let original = m::CommitId(*blake3::Hash::from_hex(&upstream.head).unwrap().as_bytes());
    let (_, bytes) = upstream
        .store
        .read_metadata(
            "example",
            m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(original.0),
            },
        )
        .unwrap();
    let m::Object::Commit(mut commit) = m::decode(&bytes, Default::default()).unwrap() else {
        unreachable!()
    };
    let (_, bytes) = upstream
        .store
        .read_metadata(
            "example",
            m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(commit.root_tree.terms[0].0),
            },
        )
        .unwrap();
    let m::Object::Tree(mut tree) = m::decode(&bytes, Default::default()).unwrap() else {
        unreachable!()
    };
    let content = b"destination addition\n";
    let file = m::describe_file(content);
    upstream.store.put("example", file, content, 0).unwrap();
    tree.entries.push(m::TreeEntry {
        name: "authored.txt".into(),
        value: m::TreeValue::File {
            id: m::FileId(file.key.id.0),
            executable: false,
            size_bytes: file.size_bytes,
            copy_id: None,
        },
    });
    tree.entries.sort_by(|a, b| a.name.cmp(&b.name));
    let tree = m::encode(&m::Object::Tree(tree), Default::default()).unwrap();
    upstream
        .store
        .put("example", tree.descriptor, &tree.bytes, 0)
        .unwrap();
    commit.parents = vec![original];
    commit.predecessors.clear();
    commit.change_id = m::ChangeId([77; 16]);
    commit.root_tree.terms = vec![m::TreeId(tree.descriptor.key.id.0)];
    let commit = m::encode(&m::Object::Commit(commit), Default::default()).unwrap();
    upstream
        .store
        .put("example", commit.descriptor, &commit.bytes, 0)
        .unwrap();
    upstream
        .store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "conflict-destination".into(),
                reference_name: "main".into(),
                expected_generation: 1,
                new_commit: Some(m::CommitId(commit.descriptor.key.id.0)),
                staging_leases: vec![],
            },
            0,
        )
        .unwrap();
}
#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn native_restored_landing_aggregate_metadata_budget_preserves_current_state() {
    for identity in [false, true] {
        let (upstream, mut fixture, base) = new_landing_fixture();
        let description = format!("configured metadata boundary\n{}", "x".repeat(1024));
        let (root, source, first_key, _) = prepare_native_landing_with_description(
            &mut fixture,
            &upstream,
            "canonical-configured-boundary",
            &description,
        );
        let binding = fixture.binding(&root);
        let key = if identity {
            // Explicit historical-committer compatibility: trusted Store API,
            // not a forged wire verdict, creates a legitimate identity job.
            upstream.finish_fixture_landing(&first_key, false);
            landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
            fixture.success(&root, &["edit", &source]);
            let destination = fixture.revision(&root, "main");
            kill_landing_at(
                &fixture,
                &root,
                &[
                    "tinytree",
                    "land",
                    "--base",
                    &destination,
                    "--reference",
                    "main",
                    "--bookmark",
                    "main",
                    "--profile",
                    "checks",
                    "--profile-version",
                    "v1",
                ],
                "freeze_written",
            );
            let (_, pending) = remote_observation(&binding);
            let pending = &pending["landing"];
            let decode =
                |value: &str| m::CommitId(*blake3::Hash::from_hex(value).unwrap().as_bytes());
            let source_id = decode(&source);
            let (_, bytes) = upstream
                .store
                .read_metadata(
                    "example",
                    m::ObjectKey {
                        kind: m::Kind::Commit,
                        id: m::ObjectId(source_id.0),
                    },
                )
                .unwrap();
            let m::Object::Commit(original) = m::decode(&bytes, Default::default()).unwrap() else {
                unreachable!()
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let request = tinytree_store::LandingRequest {
                idempotency_key: pending["request_id"].as_str().unwrap().into(),
                source_base: decode(pending["source_base"].as_str().unwrap()),
                source_head: source_id,
                reference_name: "main".into(),
                expected_generation: pending["generation"].as_integer().unwrap() as u64,
                expected_head: decode(pending["expected_head"].as_str().unwrap()),
                committer: original.committer,
                check_profile: tinytree_store::LandingCheckProfile {
                    name: "checks".into(),
                    version: "v1".into(),
                },
                expires_at: now + 3600,
            };
            upstream
                .store
                .begin_landing("example", RemoteServer::landing_owner(), &request, now)
                .unwrap();
            request.idempotency_key
        } else {
            first_key
        };
        let (_, mut state) = remote_observation(&binding);
        state
            .get_mut("landing")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .insert("restored_lookup_only".into(), toml::Value::Boolean(true));
        let sidecar = binding.working_copy.join("native-remote.toml");
        fs::write(&sidecar, toml::to_string(&state).unwrap()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let candidate = loop {
            let report = landing_report(&mut fixture, &root, &["tinytree", "land", "--status"]);
            if report["phase"] == "prepared_pending_checks" {
                break report["candidate"].as_str().unwrap().to_owned();
            }
            assert!(
                Instant::now() < deadline,
                "configured boundary job was not prepared: {report}"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(candidate == source, identity);
        upstream.finish_fixture_landing(&key, true);
        let before_sidecar = fs::read(&sidecar).unwrap();
        let before_heads = native_operation_heads(&binding);
        let before_wc =
            jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
                .unwrap()
                .0
                .persisted_bytes;
        let starts = upstream.metadata.landing_starts.load(Ordering::SeqCst);
        let public = upstream.public();
        let evolution = fixture.success(
            &root,
            &[
                "--ignore-working-copy",
                "evolog",
                "--no-graph",
                "-r",
                &source,
                "-T",
                "commit.commit_id() ++ \"\\n\"",
            ],
        );
        let binary = Path::new(env!("CARGO_BIN_EXE_landing_fault_driver"))
            .canonicalize()
            .unwrap();
        fixture.child_env.push((
            "TINYTREE_LANDING_TEST_CANONICAL_WORK".into(),
            (128 * 1024).to_string(),
        ));
        let output = fixture.run_binary(
            &root,
            &binary,
            &[
                "--no-pager",
                "--color=never",
                "tinytree",
                "land",
                "--resume",
            ],
            false,
        );
        fixture.child_env.pop();
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("native canonical test reservations released"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if identity {
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(report["acknowledged"], true);
            assert_eq!(
                fixture.success(
                    &root,
                    &[
                        "--ignore-working-copy",
                        "evolog",
                        "--no-graph",
                        "-r",
                        &source,
                        "-T",
                        "commit.commit_id() ++ \"\\n\""
                    ]
                ),
                evolution,
                "identity admission does not invent evolution"
            );
        } else {
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("canonical source/result aggregate work budget exceeded"),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(native_operation_heads(&binding), before_heads);
            assert_eq!(
                jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
                    .unwrap()
                    .0
                    .persisted_bytes,
                before_wc
            );
            assert_eq!(fs::read(&sidecar).unwrap(), before_sidecar);
            assert!(!fixture
                .registry
                .join("landing-resolutions")
                .join(binding.id())
                .join(format!("{key}-apply.json"))
                .exists());
        }
        assert_eq!(
            upstream.metadata.landing_starts.load(Ordering::SeqCst),
            starts
        );
        assert_eq!(upstream.public(), public);
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn native_restored_freeze_compatibility_recovers_current_not_captured_operations() {
    struct Case {
        name: &'static str,
        outside_stack: bool,
        passed: bool,
    }
    for case in [
        Case {
            name: "fresh-freeze-owned-crash",
            outside_stack: false,
            passed: true,
        },
        Case {
            name: "outside-stack-dirty-no-fork",
            outside_stack: true,
            passed: true,
        },
        Case {
            name: "prestart-none-terminal-lineage",
            outside_stack: false,
            passed: false,
        },
    ] {
        let upstream = RemoteServer::start_with_cloud_landing_recovery();
        let mut fixture = Fixture::new();
        fixture.configure_remote(&upstream.endpoint);
        fixture.automatic_policies();
        fixture.start();
        let base = fixture.base().to_owned();
        let root = base.join(case.name);
        fixture.clone_remote(&root);
        let destination = fixture.revision(&root, "main");
        fs::write(
            root.join("freeze-source.txt"),
            b"original submitted source\n",
        )
        .unwrap();
        fixture.success(&root, &["describe", "-m", "freeze source"]);
        let source = fixture.revision(&root, "@");
        let captured = kill_landing_at(
            &fixture,
            &root,
            &[
                "tinytree",
                "land",
                "--base",
                &destination,
                "--reference",
                "main",
                "--bookmark",
                "main",
                "--profile",
                "checks",
                "--profile-version",
                "v1",
            ],
            "freeze_written",
        );
        let captured = captured.lines().nth(1).unwrap().to_owned();
        let binding = fixture.binding(&root);
        let (_, mut saved) = remote_observation(&binding);
        let key = saved["landing"]["request_id"].as_str().unwrap().to_owned();
        let original_source_op = saved["landing"]["source_operation"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(saved["landing"].get("job_id").is_none());
        // A real retained pre-Start capsule publishes the private canonical
        // source closure before any fresh remote source admission can succeed.
        fixture.success(&root, &["status"]);
        wait_for_capture_publications(&fixture, 1);
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let published =
                retained_capture_versions_observed(&upstream, binding.workspace_id, None, deadline)
                    .into_iter()
                    .any(|(_, _, _, manifest, capsule)| {
                        let data = tinytree_cloud::decode_data(
                            &capsule,
                            tinytree_cloud::CaptureDataId::of(&capsule).0,
                            &Default::default(),
                        )
                        .unwrap();
                        data.remote
                            .as_ref()
                            .and_then(|remote| remote.landing.as_ref())
                            .is_some_and(|p| p.request_id == key && p.job_id.is_none())
                            && data.operations.iter().any(|op| op.operation == captured)
                            && manifest.history.recovery.native_roots.iter().any(|root| {
                                root.kind == m::Kind::Commit
                                    && capture_identity(&root.id.0) == source
                            })
                    });
            if published {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "original pre-Start capsule was not published"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        if case.outside_stack {
            fixture.success(&root, &["new", &source]);
            fs::write(root.join("later.txt"), b"keep raw private edits\n").unwrap();
        }
        // Explicit compatibility injection models only the restored trust flag.
        // OPs, WC state, original intent and crash prefixes are real CLI output;
        // no host nonce record or server verdict is manufactured here.
        saved
            .get_mut("landing")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .insert("restored_lookup_only".into(), toml::Value::Boolean(true));
        let path = binding.working_copy.join("native-remote.toml");
        fs::write(&path, toml::to_string(&saved).unwrap()).unwrap();
        let before_sidecar = fs::read(&path).unwrap();
        let before_wc =
            jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
                .unwrap()
                .0
                .persisted_bytes;
        let before_heads = native_operation_heads(&binding);
        let starts = upstream.metadata.landing_starts.load(Ordering::SeqCst);
        let refused = fixture.run(&root, &["tinytree", "land", "--resume"]);
        assert!(!refused.status.success());
        assert!(String::from_utf8_lossy(&refused.stderr).contains("original landing lookup failed"));
        assert_eq!(
            upstream.metadata.landing_starts.load(Ordering::SeqCst),
            starts
        );
        assert_eq!(fs::read(&path).unwrap(), before_sidecar);
        assert_eq!(native_operation_heads(&binding), before_heads);
        assert_eq!(
            jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
                .unwrap()
                .0
                .persisted_bytes,
            before_wc
        );
        if !case.outside_stack {
            let owned = kill_landing_at(
                &fixture,
                &root,
                &["tinytree", "land", "--resume", "--submit-original"],
                "freeze_resolution_recorded",
            );
            assert_ne!(owned.lines().nth(1).unwrap(), captured);
        }
        landing_report(
            &mut fixture,
            &root,
            &["tinytree", "land", "--resume", "--submit-original"],
        );
        assert_eq!(
            upstream.metadata.landing_starts.load(Ordering::SeqCst),
            starts + 1
        );
        if case.outside_stack {
            assert_eq!(
                native_operation_heads(&binding),
                before_heads,
                "no-fork bookkeeping creates no operation"
            );
            assert_eq!(
                jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
                    .unwrap()
                    .0
                    .persisted_bytes,
                before_wc
            );
            assert_eq!(
                fs::read(root.join("later.txt")).unwrap(),
                b"keep raw private edits\n"
            );
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let report = landing_report(&mut fixture, &root, &["tinytree", "land", "--status"]);
            if report["phase"] == "prepared_pending_checks" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "original job was not prepared: {report}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let (_, pending) = remote_observation(&binding);
        assert_eq!(
            pending["landing"]["request_id"].as_str(),
            Some(key.as_str())
        );
        assert_eq!(
            pending["landing"]["source_operation"].as_str(),
            Some(original_source_op.as_str())
        );
        assert_ne!(
            pending["landing"]["fork_operation"].as_str(),
            Some(captured.as_str())
        );
        if case.outside_stack {
            assert_eq!(
                pending["landing"]["fork_operation"].as_str(),
                Some(original_source_op.as_str())
            );
        }
        upstream.finish_fixture_landing(&key, case.passed);
        let closed_admin = if !case.passed {
            kill_landing_at(
                &fixture,
                &root,
                &["tinytree", "land", "--resume"],
                "closed_before_cleanup",
            );
            assert!(!landing_resolution_records(&fixture, &binding, &key).is_empty());
            fixture.success(
                &root,
                &[
                    "describe",
                    "-r",
                    "@",
                    "-m",
                    "ordinary work after terminal closure",
                ],
            );
            fs::write(
                root.join("after-terminal.txt"),
                b"preserve later raw work\n",
            )
            .unwrap();
            Some(native_admin_state(&binding))
        } else {
            None
        };
        let result = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
        assert!(landing_resolution_records(&fixture, &binding, &key).is_empty());
        if let Some(before) = closed_admin {
            assert_eq!(
                native_admin_state(&binding),
                before,
                "terminal cleanup retry preserves exact OP, VIEW, index, heads and WC"
            );
            assert_eq!(
                fs::read(root.join("after-terminal.txt")).unwrap(),
                b"preserve later raw work\n"
            );
            // Explicit legacy-sidecar compatibility: older closed terminal
            // records may lack historical observations. The fresh real lookup
            // must enrich them before cleanup, not mistake them for a retry.
            let (_, mut legacy) = remote_observation(&binding);
            assert!(legacy
                .get_mut("landing")
                .unwrap()
                .as_table_mut()
                .unwrap()
                .remove("observed")
                .is_some());
            fs::write(
                binding.working_copy.join("native-remote.toml"),
                toml::to_string(&legacy).unwrap(),
            )
            .unwrap();
            let enriched = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
            assert_eq!(enriched["phase"], "check_failed");
            assert!(remote_observation(&binding).1["landing"]
                .get("observed")
                .is_some());
            assert_eq!(native_admin_state(&binding), before);
            assert!(landing_resolution_records(&fixture, &binding, &key).is_empty());
        }
        assert_eq!(
            result["phase"],
            if case.passed {
                "applied"
            } else {
                "check_failed"
            }
        );
        assert_eq!(
            upstream.metadata.landing_starts.load(Ordering::SeqCst),
            starts + 1
        );
        assert_eq!(
            fs::read(root.join("freeze-source.txt")).unwrap(),
            b"original submitted source\n"
        );
        if case.outside_stack {
            assert_eq!(
                fs::read(root.join("later.txt")).unwrap(),
                b"keep raw private edits\n"
            );
        }
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn native_landing_recovery_edges_preserve_divergence_and_terminal_states() {
    for point in ["apply_written", "checkout_applied"] {
        let (upstream, mut owner, base) = new_landing_fixture();
        let fixture = &mut owner;
        let upstream = &upstream;

        let (root, source, key, candidate) = prepare_native_landing(fixture, upstream, point);
        fs::write(
            root.join("later.txt"),
            b"working-copy changes after submission\n",
        )
        .unwrap();
        fixture.success(&root, &["status"]);
        upstream.finish_fixture_landing(&key, true);
        let marker = kill_landing_at(fixture, &root, &["tinytree", "land", "--resume"], point);
        let binding = fixture.binding(&root);
        let (_, saved) = remote_observation(&binding);
        let exact = if point == "apply_written" {
            assert_eq!(
                saved["landing"]["operation_writing"].as_str(),
                Some("apply")
            );
            assert!(saved["landing"].get("apply_operation").is_none());
            marker.lines().nth(1).unwrap().to_owned()
        } else {
            saved["landing"]["apply_operation"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        let applied = landing_report(fixture, &root, &["tinytree", "land", "--resume"]);
        assert_eq!(applied["applied_operation"], exact);
        assert_eq!(applied["acknowledged"], true);
        assert_eq!(fixture.revision(&root, "@-"), candidate);
        assert_eq!(
            fs::read(root.join("later.txt")).unwrap(),
            b"working-copy changes after submission\n"
        );
        let evolution = fixture.success(
            &root,
            &[
                "evolog",
                "--no-graph",
                "-r",
                &candidate,
                "-T",
                "commit.commit_id() ++ \"\\n\"",
            ],
        );
        assert_eq!(evolution.lines().filter(|id| *id == candidate).count(), 1);
        assert_eq!(evolution.lines().filter(|id| *id == source).count(), 1);
        let (_, saved) = remote_observation(&binding);
        assert!(saved["landing"].get("candidate_head").is_none());
        assert!(saved["landing"]["result_roots"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(
            saved["landing"]["observed"]["receipt"].is_table(),
            "historical committed receipt survives explicit claim classification"
        );
        let committed = upstream
            .store
            .landing_job("example", RemoteServer::landing_owner(), &key)
            .unwrap()
            .unwrap();
        upstream
            .store
            .expire_landing(
                "example",
                RemoteServer::landing_owner(),
                &key,
                committed.expires_at,
            )
            .unwrap();
        let replay = landing_report(fixture, &root, &["tinytree", "land", "--resume"]);
        assert_eq!(replay["applied_operation"], exact);
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
    {
        let (upstream, mut owner, base) = new_landing_fixture();
        let fixture = &mut owner;
        let upstream = &upstream;

        // Explicit amendment of a submitted source is not the same tested commit.
        let (root, source, key, _candidate) = prepare_native_landing(fixture, upstream, "amended");
        fixture.success(
            &root,
            &["describe", "-r", &source, "-m", "amended while checks run"],
        );
        let amended = fixture.revision(&root, "@-");
        assert_ne!(amended, source);
        fs::write(root.join("keep.txt"), b"keep private divergence\n").unwrap();
        fixture.success(&root, &["status"]);
        upstream.finish_fixture_landing(&key, true);
        let failed = fixture.run(&root, &["tinytree", "land", "--resume"]);
        assert!(!failed.status.success());
        assert!(String::from_utf8_lossy(&failed.stderr).contains("reconciliation_required"));
        assert_eq!(fixture.revision(&root, "@-"), amended);
        assert_eq!(
            fs::read(root.join("keep.txt")).unwrap(),
            b"keep private divergence\n"
        );
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
    {
        let (upstream, mut owner, base) = new_landing_fixture();
        let fixture = &mut owner;
        let upstream = &upstream;

        // A detached saved operation cannot overwrite edits made after client death.
        let (root, _source, key, _candidate) =
            prepare_native_landing(fixture, upstream, "advanced");
        upstream.finish_fixture_landing(&key, true);
        kill_landing_at(
            fixture,
            &root,
            &["tinytree", "land", "--resume"],
            "apply_saved",
        );
        fs::write(root.join("keep.txt"), b"new operation after saved plan\n").unwrap();
        fixture.success(&root, &["status"]);
        let current = fixture.revision(&root, "@");
        let failed = fixture.run(&root, &["tinytree", "land", "--resume"]);
        assert!(!failed.status.success());
        assert!(String::from_utf8_lossy(&failed.stderr).contains("reconciliation_required"));
        assert_eq!(fixture.revision(&root, "@"), current);
        assert_eq!(
            fs::read(root.join("keep.txt")).unwrap(),
            b"new operation after saved plan\n"
        );
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
    {
        let (upstream, mut owner, base) = new_landing_fixture();
        let fixture = &mut owner;
        let upstream = &upstream;

        // A conflicting rebase remains a server conflict, not a local reset.
        seed_landing_conflict_target(upstream);
        let conflict = fixture.base().join("conflict");
        fixture.clone_remote(&conflict);
        fixture.success(&conflict, &["new", &upstream.head]);
        fs::write(
            conflict.join("authored.txt"),
            b"conflicting source addition\n",
        )
        .unwrap();
        fixture.success(&conflict, &["describe", "-m", "conflicting draft"]);
        let before = upstream.public();
        landing_report(
            fixture,
            &conflict,
            &[
                "tinytree",
                "land",
                "--base",
                &upstream.head,
                "--reference",
                "main",
                "--bookmark",
                "main",
                "--profile",
                "checks",
                "--profile-version",
                "v1",
            ],
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let report = landing_report(fixture, &conflict, &["tinytree", "land", "--resume"]);
            if report["phase"] == "conflict" {
                assert!(report["applied_operation"].is_null());
                break;
            }
            assert!(
                Instant::now() < deadline,
                "conflict was not terminal: {report}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(upstream.public(), before);
        assert_eq!(
            fs::read(conflict.join("authored.txt")).unwrap(),
            b"conflicting source addition\n"
        );
        fixture.success(&base, &["tinytree", "unmount", conflict.to_str().unwrap()]);
        fixture.stop_daemon();
    }
    // Restored helper security boundary: jj helper loading normally resolves
    // plural heads, and optional stale-WC auto-update can check out a new tree.
    // Both actual retained shapes must be refused BEFORE either behavior.
    for (name, plural) in [
        ("restored-plural-heads", true),
        ("restored-stale-wc", false),
    ] {
        use jj_lib::op_heads_store::OpHeadsStore as _;
        let (upstream, mut fixture, base) = new_landing_fixture();
        let (root, _, key, _) = prepare_native_landing(&mut fixture, &upstream, name);
        upstream.finish_fixture_landing(&key, true);
        let binding = fixture.binding(&root);
        let (_, mut saved) = remote_observation(&binding);
        let source_op = jj_lib::op_store::OperationId::try_from_hex(
            saved["landing"]["source_operation"].as_str().unwrap(),
        )
        .unwrap();
        saved
            .get_mut("landing")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .insert("restored_lookup_only".into(), toml::Value::Boolean(true));
        let path = binding.working_copy.join("native-remote.toml");
        fs::write(&path, toml::to_string(&saved).unwrap()).unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&fixture.jj_config)
            .unwrap()
            .write_all(b"\n[snapshot]\nauto-update-stale=true\n")
            .unwrap();
        let store =
            jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(&binding.repo.join("op_heads"));
        let original_heads = store.get_op_heads_bounded(2).unwrap();
        assert_eq!(original_heads.len(), 1);
        assert_ne!(original_heads[0], source_op);
        upstream.runtime.as_ref().unwrap().block_on(async {
            store
                .update_op_heads(if plural { &[] } else { &original_heads }, &source_op)
                .await
                .unwrap();
        });
        let heads = native_operation_heads(&binding);
        assert_eq!(heads.len(), if plural { 2 } else { 1 });
        let wc_before =
            jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
                .unwrap()
                .0
                .persisted_bytes;
        let sidecar_before = fs::read(&path).unwrap();
        let public = upstream.public();
        let starts = upstream.metadata.landing_starts.load(Ordering::SeqCst);
        for action in ["--status", "--resume"] {
            let rejected = fixture.run(&root, &["tinytree", "land", action]);
            assert!(
                !rejected.status.success(),
                "{name} {action} unexpectedly accepted"
            );
            assert!(String::from_utf8_lossy(&rejected.stderr).contains("recovery_required"));
            assert_eq!(native_operation_heads(&binding), heads);
            assert_eq!(fs::read(&path).unwrap(), sidecar_before);
            assert_eq!(jj_vfs::native::read_retained_state_for_capture(
                &binding.projection(), 1024 * 1024,
            ).unwrap().0.persisted_bytes, wc_before);
            assert_eq!(upstream.public(), public);
            assert_eq!(
                upstream.metadata.landing_starts.load(Ordering::SeqCst),
                starts
            );
        }
        upstream.runtime.as_ref().unwrap().block_on(async {
            let selected = store.get_op_heads_bounded(2).unwrap();
            store
                .update_op_heads(&selected, &original_heads[0])
                .await
                .unwrap();
        });
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
    // Security boundary: an untrusted restored journal may claim a raw apply
    // write while the authenticated original job is only prepared. Status must
    // remain pure lookup; resume may observe Prepared but never replay raw OPs.
    {
        let (upstream, mut fixture, base) = new_landing_fixture();
        let (root, _source, _key, _candidate) =
            prepare_native_landing(&mut fixture, &upstream, "restored-apply-uncertainty");
        let binding = fixture.binding(&root);
        let (_, mut saved) = remote_observation(&binding);
        let path = binding.working_copy.join("native-remote.toml");
        saved
            .get_mut("landing")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .insert("restored_lookup_only".into(), toml::Value::Boolean(true));
        saved
            .get_mut("landing")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .insert(
                "operation_writing".into(),
                toml::Value::String("apply".into()),
            );
        fs::write(&path, toml::to_string(&saved).unwrap()).unwrap();
        let before = fs::read(&path).unwrap();
        let head = fixture.revision(&root, "@");
        let public = upstream.public();
        let report = landing_report(&mut fixture, &root, &["tinytree", "land", "--status"]);
        assert_eq!(report["phase"], "prepared_pending_checks");
        assert_eq!(
            fs::read(&path).unwrap(),
            before,
            "lookup must not release the restored fence or reconcile an untrusted raw operation"
        );
        assert_eq!(fixture.revision(&root, "@"), head);
        let heads = native_operation_heads(&binding);
        let wc =
            jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
                .unwrap()
                .0
                .persisted_bytes;
        let starts = upstream.metadata.landing_starts.load(Ordering::SeqCst);
        let scope = native_workspace_admin::remote_state::RemoteScope::new(
            fixture.broker.clone(),
            "example".into(),
        )
        .unwrap();
        let before_state = native_workspace_admin::remote_state::decode(&before, &scope).unwrap();
        let observed = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
        assert_eq!(observed["phase"], "prepared_pending_checks");
        assert_eq!(
            native_workspace_admin::remote_state::decode(&fs::read(&path).unwrap(), &scope)
                .unwrap(),
            before_state,
            "resume may normalize encoding but preserves every validated state field"
        );
        assert_eq!(native_operation_heads(&binding), heads);
        assert_eq!(
            jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
                .unwrap()
                .0
                .persisted_bytes,
            wc
        );
        assert_eq!(
            upstream.metadata.landing_starts.load(Ordering::SeqCst),
            starts
        );
        assert_eq!(fixture.revision(&root, "@"), head);
        assert_eq!(upstream.public(), public);
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
    // A fresh canonical apply may crash after its raw OP write. It must not
    // launder any captured orphan inventory into ordinary retry authority.
    {
        let (upstream, mut fixture, base) = new_landing_fixture();
        let (root, _source, key, _candidate) =
            prepare_native_landing(&mut fixture, &upstream, "restored-mid-apply");
        let binding = fixture.binding(&root);
        let (_, mut saved) = remote_observation(&binding);
        let path = binding.working_copy.join("native-remote.toml");
        saved
            .get_mut("landing")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .insert("restored_lookup_only".into(), toml::Value::Boolean(true));
        fs::write(&path, toml::to_string(&saved).unwrap()).unwrap();
        upstream.finish_fixture_landing(&key, true);
        let orphan_receipt = kill_landing_at(
            &mut fixture,
            &root,
            &["tinytree", "land", "--resume"],
            "apply_written",
        );
        let (before, saved) = remote_observation(&binding);
        assert_eq!(
            saved["landing"]["restored_lookup_only"].as_bool(),
            Some(true)
        );
        assert!(saved["landing"].get("operation_writing").is_none());
        assert!(saved["landing"].get("apply_operation").is_none());
        let head = fixture.revision(&root, "@");
        let public = upstream.public();
        let report = landing_report(&mut fixture, &root, &["tinytree", "land", "--status"]);
        assert_eq!(report["phase"], "committed");
        assert_eq!(
            fs::read(&path).unwrap(),
            before,
            "status cannot recover a captured/fresh uncertain raw operation"
        );
        assert_eq!(fixture.revision(&root, "@"), head);
        assert_eq!(upstream.public(), public);
        let starts = upstream.metadata.landing_starts.load(Ordering::SeqCst);
        let recovered = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
        assert_eq!(recovered["phase"], "applied");
        assert_eq!(recovered["acknowledged"], true);
        assert_ne!(
            recovered["applied_operation"],
            orphan_receipt.lines().nth(1).unwrap(),
            "an unrecorded nonce orphan never becomes retry authority"
        );
        assert_eq!(
            upstream.metadata.landing_starts.load(Ordering::SeqCst),
            starts
        );
        assert_eq!(upstream.public(), public);
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
    // A different agent can advance main before submission and again after
    // this candidate commits. Applying the historical receipt must preserve the
    // newer fetched bookmark while retaining rewritten results through the exact
    // apply operation, not demand that its head remain a direct VIEW reference.
    for rewritten in [false, true] {
        let (upstream, mut fixture, base) = new_landing_fixture();
        let root = base.join("advanced-reference-apply");
        fixture.clone_remote(&root);
        let source_base = fixture.revision(&root, "main");
        fs::write(
            root.join("advanced-source.txt"),
            b"private source with a newer target\n",
        )
        .unwrap();
        fixture.success(&root, &["describe", "-m", "advanced target source"]);
        let source = fixture.revision(&root, "@");
        let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let advance = |label: &str| {
            let current = upstream.public();
            let descriptor = current.commit.unwrap();
            let (_, bytes) = upstream
                .store
                .read_metadata("example", descriptor.key)
                .unwrap();
            let m::Object::Commit(mut commit) = m::decode(&bytes, Default::default()).unwrap()
            else {
                unreachable!()
            };
            commit.parents = vec![m::CommitId(descriptor.key.id.0)];
            commit.predecessors.clear();
            commit.change_id = m::ChangeId([if label == "before" { 88 } else { 89 }; 16]);
            commit.description = format!("independent public advance {label}");
            let encoded = m::encode(&m::Object::Commit(commit), Default::default()).unwrap();
            upstream
                .store
                .put("example", encoded.descriptor, &encoded.bytes, 0)
                .unwrap();
            upstream
                .store
                .publish(
                    "example",
                    &tinytree_store::PublishRequest {
                        idempotency_key: format!("independent-{label}"),
                        reference_name: "main".into(),
                        expected_generation: current.generation,
                        new_commit: Some(m::CommitId(encoded.descriptor.key.id.0)),
                        staging_leases: vec![],
                    },
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                )
                .unwrap();
            hex(&encoded.descriptor.key.id.0)
        };
        if rewritten {
            advance("before");
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
        }
        let land_args = [
            "tinytree",
            "land",
            "--base",
            &source_base,
            "--revision",
            &source,
            "--reference",
            "main",
            "--bookmark",
            "main",
            "--profile",
            "checks",
            "--profile-version",
            "v1",
        ];
        landing_report(&mut fixture, &root, &land_args);
        if !rewritten {
            // Explicit historical-committer compatibility fixture. First use the
            // normal CLI staging workflow, terminalize that attempt, then stop a
            // NEW native intent at its real durable freeze-write boundary. The
            // trusted embedding accepts that exact original request with the
            // canonical source's committer, deterministically preserving its ID.
            // This is functional fixture check evidence, not a real Bazel run.
            let deadline = Instant::now() + Duration::from_secs(10);
            let initial = loop {
                let report = landing_report(&mut fixture, &root, &["tinytree", "land", "--status"]);
                if report["phase"] == "prepared_pending_checks" {
                    break report;
                }
                assert!(Instant::now() < deadline, "{report}");
                std::thread::sleep(Duration::from_millis(10));
            };
            upstream.finish_fixture_landing(initial["request_id"].as_str().unwrap(), false);
            landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
            fixture.success(&root, &["edit", &source]);
            kill_landing_at(&fixture, &root, &land_args, "freeze_written");
            let (_, saved) = remote_observation(&fixture.binding(&root));
            let pending = &saved["landing"];
            let decode =
                |text: &str| m::CommitId(*blake3::Hash::from_hex(text).unwrap().as_bytes());
            let source_id = decode(pending["source_head"].as_str().unwrap());
            let (_, bytes) = upstream
                .store
                .read_metadata(
                    "example",
                    m::ObjectKey {
                        kind: m::Kind::Commit,
                        id: m::ObjectId(source_id.0),
                    },
                )
                .unwrap();
            let m::Object::Commit(source_commit) = m::decode(&bytes, Default::default()).unwrap()
            else {
                unreachable!()
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let request = tinytree_store::LandingRequest {
                idempotency_key: pending["request_id"].as_str().unwrap().into(),
                source_base: decode(pending["source_base"].as_str().unwrap()),
                source_head: source_id,
                reference_name: pending["reference"].as_str().unwrap().into(),
                expected_generation: pending["generation"].as_integer().unwrap() as u64,
                expected_head: decode(pending["expected_head"].as_str().unwrap()),
                committer: source_commit.committer,
                check_profile: tinytree_store::LandingCheckProfile {
                    name: pending["profile_name"].as_str().unwrap().into(),
                    version: pending["profile_version"].as_str().unwrap().into(),
                },
                expires_at: now + 3600,
            };
            upstream
                .store
                .begin_landing("example", RemoteServer::landing_owner(), &request, now)
                .unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        let prepared = loop {
            let report = landing_report(&mut fixture, &root, &["tinytree", "land", "--status"]);
            if report["phase"] == "prepared_pending_checks" {
                break report;
            }
            assert!(Instant::now() < deadline, "{report}");
            std::thread::sleep(Duration::from_millis(10));
        };
        let key = prepared["request_id"].as_str().unwrap();
        let candidate = prepared["candidate"].as_str().unwrap().to_owned();
        assert_eq!(candidate != source, rewritten);
        upstream.finish_fixture_landing(key, true);
        let newest = advance("after");
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
        if rewritten {
            // Existing canonical divergence protection treats the newly fetched
            // tested rewrite as another visible incarnation. This fixture keeps
            // that unsupported path explicit; retention bookkeeping must never
            // weaken it or overwrite work to make the case succeed.
            let status = landing_report(&mut fixture, &root, &["tinytree", "land", "--status"]);
            assert_eq!(status["phase"], "committed");
            let current = fixture.revision(&root, "@");
            let binding = fixture.binding(&root);
            let (saved_before, claims_before) = remote_observation(&binding);
            let heads = native_operation_heads(&binding);
            let rejected = fixture.run(&root, &["tinytree", "land", "--resume"]);
            assert!(!rejected.status.success());
            assert!(String::from_utf8_lossy(&rejected.stderr)
                .contains("submitted source was amended or diverged"));
            assert_eq!(fixture.revision(&root, "@"), current);
            assert_eq!(fixture.revision(&root, "main"), newest);
            assert_eq!(remote_observation(&binding).0, saved_before);
            assert_eq!(native_operation_heads(&binding), heads);
            assert!(!claims_before["landing"]["result_roots"]
                .as_array()
                .unwrap()
                .is_empty());
            assert_eq!(
                fs::read(root.join("advanced-source.txt")).unwrap(),
                b"private source with a newer target\n"
            );
        } else {
            let applied = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
            assert_eq!(applied["acknowledged"], true);
            assert_eq!(fixture.revision(&root, "main"), newest);
            assert_eq!(fixture.revision(&root, "@-"), candidate);
            let (_, saved) = remote_observation(&fixture.binding(&root));
            assert!(saved["landing"]["result_roots"]
                .as_array()
                .unwrap()
                .is_empty());
            assert!(saved["landing"]["observed"]["receipt"].is_table());
        }
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
    // Failed and stale server checks never create a local apply operation.
    for stale in [false, true] {
        let (upstream, mut owner, base) = new_landing_fixture();
        let fixture = &mut owner;
        let upstream = &upstream;

        let name = if stale { "stale" } else { "failed" };
        let (root, _source, key, _candidate) = prepare_native_landing(fixture, upstream, name);
        let before = upstream.public();
        if stale {
            upstream
                .store
                .publish(
                    "example",
                    &tinytree_store::PublishRequest {
                        idempotency_key: "fixture-stale-target".into(),
                        reference_name: "main".into(),
                        expected_generation: before.generation,
                        new_commit: before.commit.map(|d| m::CommitId(d.key.id.0)),
                        staging_leases: vec![],
                    },
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                )
                .unwrap();
        }
        let job = upstream.finish_fixture_landing(&key, stale);
        assert_eq!(
            job.state,
            if stale {
                tinytree_store::LandingState::StaleTarget
            } else {
                tinytree_store::LandingState::CheckFailed
            }
        );
        let head = fixture.revision(&root, "@");
        let report = landing_report(fixture, &root, &["tinytree", "land", "--resume"]);
        assert_eq!(
            report["phase"],
            if stale {
                "stale_target"
            } else {
                "check_failed"
            }
        );
        assert!(report["applied_operation"].is_null());
        assert_eq!(fixture.revision(&root, "@"), head);
        assert_eq!(upstream.public().commit, before.commit);
        let (_, saved) = remote_observation(&fixture.binding(&root));
        assert!(
            saved["landing"].get("candidate_head").is_none(),
            "fresh nonapplicable terminal result retires only its current candidate claim"
        );
        assert!(saved["landing"]["result_roots"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(
            saved["landing"]["observed"]["phase"].as_str(),
            Some(if stale {
                "stale_target"
            } else {
                "check_failed"
            })
        );
        assert!(
            saved["landing"]["observed"]["candidate"].is_table(),
            "historical candidate remains portable"
        );
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
    {
        let (upstream, mut fixture, base) = new_landing_fixture();
        let (root, source, key, _) = prepare_native_landing(&mut fixture, &upstream, "unavailable");
        let before = upstream.public();
        let head = fixture.revision(&root, "@");
        let owner = RemoteServer::landing_owner();
        let prepared = upstream
            .store
            .landing_job("example", owner, &key)
            .unwrap()
            .unwrap();
        let candidate = prepared.candidate.unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        upstream.store.enable_external_checks().unwrap();
        let unavailable = upstream
            .store
            .record_landing_check_unavailable(
                "example",
                owner,
                &key,
                &tinytree_store::LandingCheckIdentity {
                    candidate: candidate.head,
                    root_tree: candidate.root_tree,
                    profile: prepared.profile,
                },
                now,
            )
            .unwrap();
        assert_eq!(
            unavailable.state,
            tinytree_store::LandingState::CheckUnavailable
        );
        let report = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
        assert_eq!(report["phase"], "check_unavailable");
        assert!(report["applied_operation"].is_null());
        assert_eq!(fixture.revision(&root, "@"), head);
        assert_eq!(fixture.revision(&root, "@-"), source);
        assert_eq!(upstream.public(), before);
        let (_, saved) = remote_observation(&fixture.binding(&root));
        assert_eq!(
            saved["landing"]["terminal"].as_str(),
            Some("check_unavailable")
        );
        assert!(saved["landing"].get("candidate_head").is_none());
        assert!(saved["landing"]["result_roots"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(
            saved["landing"]["source_head"].as_str(),
            Some(source.as_str())
        );
        assert_eq!(
            fs::read(root.join("unavailable.txt")).unwrap(),
            b"source unavailable\n"
        );

        // A non-verdict terminal result permits an ordinary new intent without
        // replacing the private working copy or claiming the checks failed.
        let next = landing_report(
            &mut fixture,
            &root,
            &[
                "tinytree",
                "land",
                "--base",
                &upstream.head,
                "--revision",
                &source,
                "--reference",
                "main",
                "--bookmark",
                "main",
                "--profile",
                "checks",
                "--profile-version",
                "v1",
            ],
        );
        assert_ne!(next["request_id"].as_str(), Some(key.as_str()));
        assert!(next["applied_operation"].is_null());
        assert_eq!(fixture.revision(&root, "@"), head);
        assert_eq!(upstream.public(), before);
        fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
        fixture.stop_daemon();
    }
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn native_landing_successful_iteration_reuses_default_quota_without_crashes() {
    let (upstream, mut fixture, base) = new_landing_fixture();
    let root = base.join("clean-iteration");
    fixture.clone_remote(&root);
    let mut canonical = HashSet::new();
    for iteration in 0..4 {
        fs::write(
            root.join(format!("iteration-{iteration}.txt")),
            format!("draft {iteration}\n"),
        )
        .unwrap();
        fixture.success(
            &root,
            &["describe", "-m", &format!("iteration {iteration}")],
        );
        let observed = fixture.revision(&root, "main");
        let source = fixture.revision(&root, "@");
        landing_report(
            &mut fixture,
            &root,
            &[
                "tinytree",
                "land",
                "--base",
                &observed,
                "--reference",
                "main",
                "--bookmark",
                "main",
                "--profile",
                "checks",
                "--profile-version",
                "v1",
            ],
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        let prepared = loop {
            let report = landing_report(&mut fixture, &root, &["tinytree", "land", "--status"]);
            if report["phase"] == "prepared_pending_checks" {
                break report;
            }
            assert!(
                Instant::now() < deadline,
                "iteration {iteration} did not prepare: {report}"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        let candidate = prepared["candidate"].as_str().unwrap().to_owned();
        assert!(canonical.insert(candidate.clone()));
        upstream.finish_fixture_landing(prepared["request_id"].as_str().unwrap(), true);
        let applied = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
        assert_eq!(applied["acknowledged"], true);
        assert_eq!(fixture.revision(&root, "main"), candidate);
        assert_eq!(fixture.revision(&root, "@-"), candidate);
        let evolution = fixture.success(
            &root,
            &[
                "evolog",
                "--no-graph",
                "-r",
                &candidate,
                "-T",
                "commit.commit_id() ++ \"\\n\"",
            ],
        );
        assert_eq!(evolution.lines().filter(|id| *id == source).count(), 1);
        assert_eq!(upstream.file_reads(), 0);
        assert_eq!(upstream.historical_tree_reads(), 0);
    }
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn native_landing_selected_ancestor_working_copy_forks_and_rebases_at_that_ancestor() {
    let (upstream, mut fixture, base) = new_landing_fixture();
    let root = base.join("ancestor");
    fixture.clone_remote(&root);
    let observed = fixture.revision(&root, "main");
    fs::write(root.join("first.txt"), b"first stack commit\n").unwrap();
    fixture.success(&root, &["describe", "-m", "first stack commit"]);
    let first = fixture.revision(&root, "@");
    fixture.success(&root, &["new"]);
    fs::write(root.join("second.txt"), b"second stack commit\n").unwrap();
    fixture.success(&root, &["describe", "-m", "second stack commit"]);
    let tip = fixture.revision(&root, "@");
    fixture.success(&root, &["bookmark", "set", "draft-tip", "-r", &tip]);
    fixture.success(&root, &["edit", &first]);
    landing_report(
        &mut fixture,
        &root,
        &[
            "tinytree",
            "land",
            "--base",
            &observed,
            "--revision",
            &tip,
            "--reference",
            "main",
            "--bookmark",
            "main",
            "--profile",
            "checks",
            "--profile-version",
            "v1",
        ],
    );
    assert_ne!(fixture.revision(&root, "@"), first);
    assert_eq!(
        fixture.revision(&root, "@-"),
        first,
        "freeze the actual selected working copy, not just the stack tip"
    );
    fs::write(
        root.join("continued.txt"),
        b"continued from the selected ancestor\n",
    )
    .unwrap();
    fixture.success(&root, &["status"]);
    let deadline = Instant::now() + Duration::from_secs(10);
    let prepared = loop {
        let report = landing_report(&mut fixture, &root, &["tinytree", "land", "--status"]);
        if report["phase"] == "prepared_pending_checks" {
            break report;
        }
        assert!(Instant::now() < deadline, "not prepared: {report}");
        std::thread::sleep(Duration::from_millis(10));
    };
    let committed = upstream.finish_fixture_landing(prepared["request_id"].as_str().unwrap(), true);
    let candidate = committed.candidate.unwrap();
    let rewritten_first = candidate
        .rewrites
        .iter()
        .find(|r| r.old.0 == *blake3::Hash::from_hex(&first).unwrap().as_bytes())
        .unwrap()
        .new;
    let applied = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
    assert_eq!(applied["acknowledged"], true);
    assert_eq!(
        fixture.revision(&root, "@-"),
        rewritten_first
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    assert_ne!(
        fixture.revision(&root, "@-"),
        prepared["candidate"].as_str().unwrap()
    );
    assert_eq!(
        fixture.revision(&root, "draft-tip"),
        prepared["candidate"].as_str().unwrap()
    );
    assert_eq!(
        fs::read(root.join("continued.txt")).unwrap(),
        b"continued from the selected ancestor\n"
    );
    assert!(
        !root.join("second.txt").exists(),
        "do not jump the workspace to the selected stack tip"
    );
    assert_eq!(upstream.file_reads(), 0);
    assert_eq!(upstream.historical_tree_reads(), 0);
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
}

#[test]
#[ignore = "requires usable /dev/fuse and fusermount3; trusted fixture verdict is not RBE evidence"]
fn native_landing_waits_for_an_interrupted_publication_without_mutating_either_intent() {
    let (upstream, mut fixture, base) = new_landing_fixture();
    let (root, _source, key, candidate) =
        prepare_native_landing(&mut fixture, &upstream, "interrupted-push");
    upstream.finish_fixture_landing(&key, true);
    // A real ordinary push has durably recorded its old-generation intent, but
    // its upstream CAS has not yet answered when that CLI process is killed.
    struct Release(Arc<MetadataReads>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.public_paused.store(false, Ordering::SeqCst);
            self.0.public_release.notify_waiters();
        }
    }
    upstream
        .metadata
        .public_paused
        .store(true, Ordering::SeqCst);
    let release = Release(upstream.metadata.clone());
    let mut command = Command::new(&fixture.cli);
    fixture.environment(&mut command);
    command
        .current_dir(&root)
        .args([
            "tinytree",
            "push",
            "--reference",
            "main",
            "--bookmark",
            "main",
        ])
        .env("PATH", &fixture.empty_path)
        .stdout(fs::File::create(base.join("interrupted-push.out")).unwrap())
        .stderr(fs::File::create(base.join("interrupted-push.err")).unwrap());
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !upstream.metadata.public_started.load(Ordering::SeqCst) {
        assert!(
            child.try_wait().unwrap().is_none(),
            "ordinary push exited before upstream CAS pause: {}",
            fs::read_to_string(base.join("interrupted-push.err")).unwrap()
        );
        assert!(
            Instant::now() < deadline,
            "ordinary push did not reach upstream CAS"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    drop(release);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = fixture.control(Request::Status).unwrap();
        if status["native"]["commands"]["active_commands"] == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "interrupted publication did not physically drain: {status}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let binding = fixture.binding(&root);
    let (before, state) = remote_observation(&binding);
    assert_eq!(state["pending"]["kind"].as_str(), Some("push"));
    assert_eq!(state["pending"]["generation"].as_integer(), Some(1));
    assert_eq!(state["landing"]["request_id"].as_str(), Some(key.as_str()));
    let before_heads = native_operation_heads(&binding);
    let before_wc = jj_vfs::native::read_retained_state(
        &binding.root,
        &binding.working_copy,
        &binding.repository,
    )
    .unwrap()
    .persisted_bytes;
    let landing_calls = upstream.metadata.landing_calls.load(Ordering::SeqCst);
    for flag in ["--status", "--resume"] {
        let output = fixture.run(&root, &["tinytree", "land", flag]);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr)
            .contains("resolve the original pending native fetch/publication"));
        assert_eq!(
            remote_observation(&binding).0,
            before,
            "both exact original journals must remain byte-identical"
        );
        assert_eq!(native_operation_heads(&binding), before_heads);
        assert_eq!(
            jj_vfs::native::read_retained_state(
                &binding.root,
                &binding.working_copy,
                &binding.repository
            )
            .unwrap()
            .persisted_bytes,
            before_wc
        );
        assert_eq!(
            upstream.metadata.landing_calls.load(Ordering::SeqCst),
            landing_calls,
            "guard must run before status/control dispatch"
        );
    }
    // Retry the ordinary original intent to obtain its authoritative stale-CAS
    // result. Only that workflow clears it; landing never replaces the journal.
    let stale = fixture.run(
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
    assert!(!stale.status.success());
    assert!(String::from_utf8_lossy(&stale.stderr).contains("native reference changed"));
    assert!(remote_observation(&binding).1.get("pending").is_none());
    let applied = landing_report(&mut fixture, &root, &["tinytree", "land", "--resume"]);
    assert_eq!(applied["acknowledged"], true);
    assert_eq!(fixture.revision(&root, "@-"), candidate);
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
}

#[cfg(feature = "forge-cli")]
fn forge_mount_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
#[cfg(feature = "forge-cli")]
struct ForgeMountedServer {
    _directory: tempfile::TempDir,
    runtime: Option<tokio::runtime::Runtime>,
    task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    store: tinytree_store::Store,
    endpoint: String,
    seed_base: String,
    seed_head: String,
    seed_view: String,
    scratch: PathBuf,
}
#[cfg(feature = "forge-cli")]
impl ForgeMountedServer {
    const TOKEN: &'static str = "public-forge-mounted-owner-fixture-token";
    fn start() -> Self {
        // The selected native wrapper runs this one case serially. Establish
        // only its public fixture credential before starting any runtime or
        // child; this never reads or mutates the user's parent environment.
        unsafe {
            std::env::set_var(
                "FORGE_MOUNT_CHECKER_TOKEN",
                "public-forge-mounted-checker-fixture-token",
            );
        }
        use sha2::{Digest as _, Sha256};
        use tinytree_store::{AccountId, ForgeActor, ForgeViewVisibility, ViewSpec};
        let directory = tempfile::Builder::new()
            .prefix("tt-forge-server-")
            .tempdir_in("/tmp")
            .unwrap();
        let base = directory.path();
        let operator = private_fs::PrivateDir::create(base.join("operator")).unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let executable = Path::new(env!("TINYTREE_CHECKER_FIXTURE"))
            .canonicalize()
            .unwrap();
        let pin = format!("{:x}", Sha256::digest(fs::read(&executable).unwrap()));
        let quoted = |p: &Path| serde_json::to_string(p.to_str().unwrap()).unwrap();
        let daemon = operator.path().join("daemon.toml");
        let cache_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let git_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        // Valid, explicit trusted raw-read template. The testonly runner below
        // does not mount this template or establish real build/check evidence.
        fs::write(
            &daemon,
            format!(
                r#"state_dir={}
[cache]
listen="{}"
upstream="grpc://127.0.0.1:9"
api_key_env="UNUSED_FORGE_MOUNT_FIXTURE_KEY"
[git_proxy]
listen="{}"
[bes]
enabled=false
[bos]
enabled=false
[native.workspaces.capture]
policy="disabled"
[native.workspaces.upload]
policy="disabled"
[native.repositories.checker-template]
checker_forge_accounts=true
[native.repositories.checker-template.remote]
authorization_profile="trusted-forge-checker-fixture"
[native.repositories.checker-template.remote.remote]
endpoint="{}"
repository_id="template"
token_env="FORGE_MOUNT_CHECKER_TOKEN"
"#,
                quoted(&operator.path().join("daemon-state")),
                cache_listener.local_addr().unwrap(),
                git_listener.local_addr().unwrap(),
                endpoint
            ),
        )
        .unwrap();
        workspace_daemon::config::Config::load(&daemon).unwrap();
        drop((cache_listener, git_listener));
        let scratch = operator.path().join("checker-state");
        let policy = operator.path().join("checker.toml");
        fs::write(
            &policy,
            format!(
                r#"scratch={}
daemon_config={}
rbe_socket={}
runner_sha256="{}"
workspace_environment=["FORGE_MOUNT_CHECKER_TOKEN"]
[workspace]
path={}
sha256="{}"
[bazel]
path={}
sha256="{}"
[[profiles]]
name="checks"
repositories=[]
forge_accounts=true
store="checker-template"
mode="build"
targets=["//fixture:checks"]
flags=["--define=fixture_mode=pass"]
timeout_seconds=60
"#,
                quoted(&scratch),
                quoted(&daemon),
                quoted(&operator.path().join("rbe.sock")),
                pin,
                quoted(&executable),
                pin,
                quoted(&executable),
                pin
            ),
        )
        .unwrap();
        let version = tinytree_check_runner::Config::load(&policy)
            .unwrap()
            .profile_version("checks")
            .unwrap();
        let mut store_config = tinytree_store::Config::default();
        store_config.max_retention_validation_records = 4096;
        store_config.max_retention_validation_bytes = 8 * 1024 * 1024;
        store_config.max_closure_objects = 8192;
        store_config.max_closure_edges = 32768;
        store_config.max_path_contexts = 8192;
        store_config.max_path_context_bytes = 8 * 1024 * 1024;
        let store = tinytree_store::Store::open(base.join("store"), store_config).unwrap();
        let account = AccountId("mounted".into());
        let actor = ForgeActor {
            principal: "owner".into(),
        };
        let state = store.forge_provision_account(&account, &actor).unwrap();
        let canonical = store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .commit
            .unwrap()
            .key
            .id
            .0;
        let view = store
            .forge_publish_view(
                &account,
                &actor,
                ViewSpec {
                    name: "workspace".into(),
                    root_prefix: "".into(),
                    include_prefixes: vec!["".into()],
                    exclude_prefixes: vec![],
                    visibility: ForgeViewVisibility::Members,
                    revision: canonical,
                    export_allowed: true,
                },
                state.epoch,
                false,
            )
            .unwrap();
        let projected = store.forge_project_view(&view, 0).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let mut entries = Vec::new();
        for (path, bytes) in [
            ("copy.txt", b"copy baseline\n".as_slice()),
            ("rename.txt", b"rename baseline\n".as_slice()),
        ] {
            let d = m::describe_file(bytes);
            store
                .forge_upload_object(&view, &actor, path, d, bytes, now)
                .unwrap();
            entries.push(m::TreeEntry {
                name: path.into(),
                value: m::TreeValue::File {
                    id: m::FileId(d.key.id.0),
                    executable: false,
                    size_bytes: d.size_bytes,
                    copy_id: None,
                },
            });
        }
        let tree = m::encode(&m::Object::Tree(m::Tree { entries }), Default::default()).unwrap();
        store
            .forge_upload_object(&view, &actor, "", tree.descriptor, &tree.bytes, now)
            .unwrap();
        let signature = m::Signature {
            name: "Fixture".into(),
            email: "fixture@example.invalid".into(),
            timestamp: m::Timestamp {
                millis_since_epoch: now * 1000,
                tz_offset_minutes: 0,
            },
        };
        let head = m::encode(
            &m::Object::Commit(m::Commit {
                parents: vec![m::CommitId(projected.projected_commit)],
                predecessors: vec![],
                root_tree: m::TreeMerge {
                    terms: vec![m::TreeId(tree.descriptor.key.id.0)],
                    labels: vec![String::new()],
                },
                change_id: m::ChangeId([191; 16]),
                description: "Seed mounted fixture".into(),
                author: signature.clone(),
                committer: signature,
                secure_signature: None,
            }),
            Default::default(),
        )
        .unwrap();
        store
            .forge_upload_object(&view, &actor, "", head.descriptor, &head.bytes, now)
            .unwrap();
        let mut config = tinytree_server::ServiceConfig::default();
        config.max_buffered_bytes = 256 * 1024 * 1024;
        config.landing.enabled = true;
        config.landing.poll_interval_ms = 10;
        config.landing.job_ttl_seconds = 120;
        config.landing.max_buffered_bytes = 6 * 1024 * 1024 * 1024;
        config.landing.forge_profile = Some("checks".into());
        config.landing.profiles = vec![tinytree_server::LandingProfile {
            name: "checks".into(),
            version,
            repositories: vec![],
        }];
        config.landing.checker = Some(tinytree_server::LandingCheckerConfig {
            enabled: true,
            executable,
            sha256: pin,
            config: policy,
            max_checks: 1,
            max_checks_per_repository: 1,
            max_jobs: 16,
            max_attempts: 2,
            max_recovery_attempts: 8,
            retry_delay_seconds: 1,
            execution_timeout_seconds: 30,
            cleanup_grace_seconds: 10,
        });
        let service = tinytree_server::RepositoryService::new(
            store.clone(),
            config,
            vec![],
            vec![
                tinytree_server::Principal::new_forge("owner", Self::TOKEN).unwrap(),
                tinytree_server::Principal::new_forge_checker(
                    "checker",
                    "public-forge-mounted-checker-fixture-token",
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = runtime.spawn(tinytree_server::serve_tcp(listener, service, None, async {
            let _ = stopped.await;
        }));
        Self {
            _directory: directory,
            runtime: Some(runtime),
            task: Some(task),
            stop: Some(stop),
            store,
            endpoint,
            seed_base: forge_mount_hex(&projected.projected_commit),
            seed_head: forge_mount_hex(&head.descriptor.key.id.0),
            seed_view: forge_mount_hex(&view.version().0),
            scratch,
        }
    }
    fn current_view(&self) -> [u8; 32] {
        use tinytree_store::{AccountId, ForgeActor, ForgeViewVisibility, ViewSpec};
        let account = AccountId("mounted".into());
        let actor = ForgeActor {
            principal: "owner".into(),
        };
        let revision = self
            .store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .commit
            .unwrap()
            .key
            .id
            .0;
        let epoch = self
            .store
            .forge_get_account(&account, &actor)
            .unwrap()
            .epoch;
        self.store
            .forge_publish_view(
                &account,
                &actor,
                ViewSpec {
                    name: "workspace".into(),
                    root_prefix: "".into(),
                    include_prefixes: vec!["".into()],
                    exclude_prefixes: vec![],
                    visibility: ForgeViewVisibility::Members,
                    revision,
                    export_allowed: true,
                },
                epoch,
                false,
            )
            .unwrap()
            .version()
            .0
    }
}
#[cfg(feature = "forge-cli")]
impl Drop for ForgeMountedServer {
    fn drop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        let task = self.task.take().unwrap();
        self.runtime.as_ref().unwrap().block_on(async {
            tokio::time::timeout(Duration::from_secs(15), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        });
        self.runtime
            .take()
            .unwrap()
            .shutdown_timeout(Duration::from_secs(5));
    }
}
#[cfg(feature = "forge-cli")]
fn forge_mount_json(fixture: &mut Fixture, root: &Path, args: &[&str]) -> serde_json::Value {
    let mut args = args.to_vec();
    args.push("--json");
    serde_json::from_str(&fixture.success_with_watchdog(root, &args, Duration::from_secs(90)))
        .unwrap()
}
#[cfg(feature = "forge-cli")]
fn forge_mount_checked(fixture: &mut Fixture, root: &Path, change: &str) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let value = forge_mount_json(fixture, root, &["review", "show", change]);
        if value["state"] == "prepared" || value["state"] == "ready_to_land" {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "trusted fixture checker did not pass: {value}"
        );
        assert!(
            !matches!(
                value["state"].as_str(),
                Some("check_failed" | "unsupported" | "expired" | "conflict")
            ),
            "{value}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
#[cfg(feature = "forge-cli")]
fn forge_mount_approve_land(
    fixture: &mut Fixture,
    root: &Path,
    checked: &serde_json::Value,
    request: &str,
) -> serde_json::Value {
    let review = &checked["result"]["value"];
    let change = review["change_id"].as_str().unwrap();
    let patchset = review["patchset_id"].as_str().unwrap();
    let candidate = review["candidate"]["id"].as_str().unwrap();
    forge_mount_json(
        fixture,
        root,
        &[
            "review",
            "approve",
            change,
            "--patchset",
            patchset,
            "--candidate",
            candidate,
            "--request-id",
            &format!("{request}-approve"),
        ],
    );
    let landed = forge_mount_json(
        fixture,
        root,
        &[
            "review",
            "land",
            change,
            "--patchset",
            patchset,
            "--candidate",
            candidate,
            "--request-id",
            &format!("{request}-land"),
        ],
    );
    assert_eq!(landed["state"], "landed");
    landed
}

#[cfg(feature = "forge-cli")]
#[test]
#[ignore = "requires Linux FUSE; pinned checker subprocess is testonly orchestration evidence, not a real Bazel/RBE check"]
fn forge_product_mounted_push_check_land_reconcile_preserves_iteration() {
    let server = ForgeMountedServer::start();
    let mut fixture = Fixture::new();
    // This acceptance case deliberately exercises a file beyond the default
    // new-file snapshot guard, using the supported explicit bounded setting.
    fs::OpenOptions::new()
        .append(true)
        .open(&fixture.jj_config)
        .unwrap()
        .write_all(b"\n[snapshot]\nmax-new-file-size=2097152\n")
        .unwrap();
    let base = fixture.base().to_owned();
    fixture.child_env.push((
        "FORGE_MOUNT_OWNER_TOKEN".into(),
        ForgeMountedServer::TOKEN.into(),
    ));
    forge_mount_json(
        &mut fixture,
        &base,
        &[
            "context",
            "set",
            "--server",
            &server.endpoint,
            "--account",
            "mounted",
            "--view",
            "workspace",
            "--token-env",
            "FORGE_MOUNT_OWNER_TOKEN",
        ],
    );
    // Bootstrap visible source through an owned upload and the same real server
    // checker/approval/landing path, without forging a caller-side check verdict.
    let seed = forge_mount_json(
        &mut fixture,
        &base,
        &[
            "push",
            "--source-base",
            &server.seed_base,
            "--source-head",
            &server.seed_head,
            "--view-version",
            &server.seed_view,
            "--request-id",
            "mounted-seed",
        ],
    );
    let seed_change = seed["result"]["value"]["change_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let checked = forge_mount_checked(&mut fixture, &base, &seed_change);
    forge_mount_approve_land(&mut fixture, &base, &checked, "mounted-seed");
    let version = server.current_view();
    let mut daemon: toml::Value =
        toml::from_str(&fs::read_to_string(&fixture.daemon_config).unwrap()).unwrap();
    let mut client = tinytree_client::Config::default();
    client.endpoint = server.endpoint.clone();
    client.token_env = Some("FORGE_MOUNT_OWNER_TOKEN".into());
    let forge = native_broker::ForgeRemoteConfig {
        account_id: "mounted".into(),
        view: "workspace".into(),
        view_version: Some(version),
        client,
    };
    daemon["native"]["repositories"]["example"]
        .as_table_mut()
        .unwrap()
        .insert("forge".into(), toml::Value::try_from(forge).unwrap());
    fs::write(&fixture.daemon_config, toml::to_string(&daemon).unwrap()).unwrap();
    fixture.start();
    let root = base.join("forge-work");
    let broker = fixture.broker.clone();
    forge_mount_json(
        &mut fixture,
        &base,
        &[
            "work",
            root.to_str().unwrap(),
            "--broker",
            broker.to_str().unwrap(),
            "--store",
            "example",
            "--version",
            &forge_mount_hex(&version),
        ],
    );
    assert_fuse_mounted(&root);
    assert_eq!(
        fs::read(root.join("rename.txt")).unwrap(),
        b"rename baseline\n"
    );
    forge_mount_json(&mut fixture, &root, &["move", "rename.txt", "renamed.txt"]);
    forge_mount_json(&mut fixture, &root, &["copy", "copy.txt", "copied.txt"]);
    // Force >1MiB FILE transfer through sequential authenticated upload sessions.
    let mut large = vec![b'x'; 1024 * 1024 + 17];
    fs::write(root.join("large.bin"), &large).unwrap();
    fixture.success(
        &root,
        &["describe", "-m", "rename, copy and large authored file"],
    );
    let source = fixture.revision(&root, "@");
    let pushed = forge_mount_json(
        &mut fixture,
        &root,
        &["push", "--request-id", "mounted-source"],
    );
    let change = pushed["result"]["value"]["change_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(pushed["result"]["value"]["source_head"], source);
    assert_ne!(
        fixture.revision(&root, "@"),
        source,
        "push starts a fresh WIP child"
    );
    fs::write(root.join("continued.txt"), b"iteration before amendment\n").unwrap();
    fixture.success(&root, &["describe", "-m", "continued local iteration"]);
    fixture.success(&root, &["bookmark", "set", "continued-wip"]);
    let first_checked = forge_mount_checked(&mut fixture, &root, &change);
    let first_patchset = first_checked["result"]["value"]["patchset_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let first_candidate = first_checked["result"]["value"]["candidate"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    forge_mount_json(
        &mut fixture,
        &root,
        &[
            "review",
            "approve",
            &change,
            "--patchset",
            &first_patchset,
            "--candidate",
            &first_candidate,
            "--request-id",
            "mounted-first-approval",
        ],
    );
    // A replacement is the same logical source change, never a second active
    // unrelated review. The already-snapshotted continuation stays a descendant.
    fixture.success(&root, &["edit", &source]);
    large[0] = b'y';
    fs::write(root.join("large.bin"), &large).unwrap();
    fixture.success(
        &root,
        &["describe", "-m", "amended source retains explicit lineage"],
    );
    let replacement = forge_mount_json(
        &mut fixture,
        &root,
        &[
            "push",
            "--expected-patchset",
            &first_patchset,
            "--request-id",
            "mounted-replacement",
        ],
    );
    assert_eq!(replacement["result"]["value"]["change_id"], change);
    assert_ne!(
        replacement["result"]["value"]["patchset_id"],
        first_patchset
    );
    assert_eq!(
        replacement["result"]["value"]["approvals"], 0,
        "replacement cannot inherit first candidate approval"
    );
    let historical = forge_mount_json(
        &mut fixture,
        &root,
        &["review", "show", &change, "--patchset", &first_patchset],
    );
    assert_eq!(historical["result"]["value"]["patchset_id"], first_patchset);
    assert_eq!(historical["result"]["value"]["source_head"], source);
    assert_eq!(historical["result"]["value"]["is_current"], false);
    fixture.success(&root, &["edit", "continued-wip"]);
    assert_eq!(
        fs::read(root.join("continued.txt")).unwrap(),
        b"iteration before amendment\n"
    );
    fs::write(root.join("descendant.txt"), b"newer descendant\n").unwrap();
    fixture.success(&root, &["describe", "-m", "newer local descendant"]);
    let descendant = fixture.revision(&root, "@");
    fixture.success(&root, &["bookmark", "set", "draft-tip", "-r", &descendant]);
    fixture.success(&root, &["new", "-m", "dirty WIP above descendant"]);
    fs::write(root.join("dirty.txt"), b"dirty iteration must survive\n").unwrap();
    let checked = forge_mount_checked(&mut fixture, &root, &change);
    let lineage = checked["result"]["value"]["lineage"].as_array().unwrap();
    assert!(lineage.iter().any(|edge| edge["operation"] == "rename"
        && edge["source_path"] == "rename.txt"
        && edge["target_path"] == "renamed.txt"));
    assert!(lineage.iter().any(|edge| edge["operation"] == "copy"
        && edge["source_path"] == "copy.txt"
        && edge["target_path"] == "copied.txt"));
    let landed = forge_mount_approve_land(&mut fixture, &root, &checked, "mounted-replacement");
    assert_eq!(
        fixture.revision(&root, "@-"),
        descendant,
        "land does not silently reconcile local state"
    );
    let reconciled = forge_mount_json(
        &mut fixture,
        &root,
        &["request", "reconcile", "mounted-replacement"],
    );
    assert_eq!(
        reconciled["result"]["value"]["local_reconciliation"],
        "applied"
    );
    assert_eq!(
        fs::read(root.join("dirty.txt")).unwrap(),
        b"dirty iteration must survive\n"
    );
    assert_eq!(
        fs::read(root.join("descendant.txt")).unwrap(),
        b"newer descendant\n"
    );
    assert_eq!(
        fs::read(root.join("copied.txt")).unwrap(),
        b"copy baseline\n"
    );
    assert_eq!(
        fs::read(root.join("renamed.txt")).unwrap(),
        b"rename baseline\n"
    );
    assert!(!root.join("rename.txt").exists());
    assert_eq!(fs::read(root.join("large.bin")).unwrap(), large);
    let projected = landed["result"]["value"]["projected_head"]
        .as_str()
        .unwrap();
    assert_eq!(fixture.revision(&root, "draft-tip-"), projected);
    assert_eq!(
        fixture.revision(&root, "@-"),
        fixture.revision(&root, "draft-tip")
    );
    let applied_working_copy = fixture.revision(&root, "@");
    let applied_descendant = fixture.revision(&root, "draft-tip");
    let replay = forge_mount_json(
        &mut fixture,
        &root,
        &["request", "reconcile", "mounted-replacement"],
    );
    assert_eq!(replay["state"], "landed");
    assert_eq!(
        fixture.revision(&root, "@"),
        applied_working_copy,
        "reconciliation replay cannot rewrite newer WIP again"
    );
    assert_eq!(fixture.revision(&root, "draft-tip"), applied_descendant);
    assert_eq!(
        fs::read(root.join("continued.txt")).unwrap(),
        b"iteration before amendment\n"
    );
    assert_eq!(
        fs::read(root.join("dirty.txt")).unwrap(),
        b"dirty iteration must survive\n"
    );
    let executions = fs::read_to_string(server.scratch.join("executions")).unwrap();
    assert_eq!(
        executions.lines().filter(|line| *line == "run").count(),
        3,
        "one pinned physical fixture process per seed/first/replacement candidate"
    );
    fixture.success(&base, &["tinytree", "unmount", root.to_str().unwrap()]);
    fixture.stop_daemon();
}
