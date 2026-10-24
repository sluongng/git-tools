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
    assert!(
        unscoped
            .unwrap_err()
            .to_string()
            .contains("configuration-scoped")
    );
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
        self.inner.call(request)
    }
}

#[derive(Default)]
struct MetadataReads {
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
        self.inner.path_history(r).await
    }
    async fn blame(
        &self,
        r: RpcRequest<q::BlameRequest>,
    ) -> Result<RpcResponse<q::BlameResult>, Status> {
        self.inner.blame(r).await
    }
    async fn open_scope(
        &self,
        r: RpcRequest<Streaming<q::OpenScopeFrame>>,
    ) -> Result<RpcResponse<q::QueryScope>, Status> {
        self.inner.open_scope(r).await
    }
    async fn open_cloud_scope(
        &self,
        r: RpcRequest<Streaming<q::OpenScopeFrame>>,
    ) -> Result<RpcResponse<q::QueryScope>, Status> {
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
        Self::start_with_history_and_cloud(history, false)
    }
    fn start_with_cloud() -> Self {
        Self::start_with_history_and_cloud(8, true)
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
        assert!((1..=1024).contains(&history));
        let setup_started = Instant::now();
        let directory = tempfile::tempdir().unwrap();
        let (mut configuration, store_configuration) = if cloud_enabled {
            let profile: tinytree_server::Config = toml::from_str(
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
        let request_bytes = configuration
            .max_request_bytes
            .max(configuration.max_frame_bytes + 1024);
        let response_bytes = configuration.max_metadata_bytes + 4096;
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
                    .with_retention(true)
                    .with_cloud(cloud_enabled),
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
        let cloud = service.cloud_grpc_service();
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
                    .add_service(service.query_grpc_service_with_adapter(query))
                    .add_service(cloud)
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
    assert!(
        upstream
            .store
            .descriptor(
                "example",
                m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: private_id
                }
            )
            .unwrap()
            .is_none()
    );
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
    assert!(
        returned["lines"]
            .as_array()
            .unwrap()
            .iter()
            .all(|line| line["origin"]["commit"] == returned["revision"])
    );
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
            "tinytree", "search", "needle", "-r", "main", "--limit", "10000", "--json",
        ],
    ))
    .unwrap();
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
        &["tinytree", "search", "needle", "--limit", "10000", "--json"],
    ))
    .unwrap();
    assert_eq!(dirty["coverage"], "complete");
    assert_eq!(dirty["revision"], fixture.revision(&root, "@"));
    assert_eq!(search_rows(&dirty), naive_search(&dirty_files, b"needle"));
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
    let upstream = RemoteServer::start_with_cloud();
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
    assert!(
        upstream
            .store
            .descriptor("example", authored_commit)
            .unwrap()
            .is_none()
    );
    assert!(
        upstream
            .store
            .descriptor("example", authored_file.key)
            .unwrap()
            .is_none()
    );
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
    assert!(
        upstream
            .store
            .descriptor("example", authored_commit)
            .unwrap()
            .is_some()
    );
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
    restore_captured_workspace(&upstream, &restore_capture, authored_bytes);
    assert_eq!(upstream.public(), public_before_upload);
    assert_eq!(local_capture_bundles(&fixture), packs_before_upload);

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
}

fn verify_uploaded_capture(
    upstream: &RemoteServer,
    fixture: &Fixture,
    binding: &native_workspace_admin::Binding,
    packs: &BTreeMap<String, Vec<u8>>,
    expected_publications: u64,
) -> VerifiedRestoreCapture {
    use tinytree_client::cloud_protocol as c;
    fn authenticated<T>(value: T) -> RpcRequest<T> {
        let mut request = RpcRequest::new(value);
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {REMOTE_TOKEN}").parse().unwrap(),
        );
        request.set_timeout(Duration::from_secs(10));
        request
    }
    let selections = upstream.runtime.as_ref().unwrap().block_on(async {
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut client =
                c::workspace_cloud_client::WorkspaceCloudClient::connect(upstream.endpoint.clone())
                    .await
                    .unwrap()
                    .accept_compressed(CompressionEncoding::Zstd)
                    .send_compressed(CompressionEncoding::Zstd);
            let workspace = client
                .get_workspace(authenticated(c::GetWorkspaceRequest {
                    repository_id: "example".into(),
                    workspace_id: binding.workspace_id.to_vec(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(workspace.workspace_id, binding.workspace_id);
            assert!((1..=256).contains(&workspace.generation));
            assert_eq!(
                workspace.generation, expected_publications,
                "every durable Published record must correspond to a retained generation"
            );
            let mut selections = Vec::new();
            let mut previous = None;
            for generation in 1..=workspace.generation {
                let version = client
                    .get_version(authenticated(c::GetVersionRequest {
                        repository_id: "example".into(),
                        workspace_id: binding.workspace_id.to_vec(),
                        generation,
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(version.workspace_id, binding.workspace_id);
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
                assert_eq!(manifest.history.recovery.workspace, binding.workspace_id);
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
                        binding.workspace_id,
                        generation,
                        manifest_id.0,
                        raw.blake3,
                        raw.size_bytes
                    )
                );

                let mut stream = client
                    .read_capture(authenticated(c::ReadCaptureRequest {
                        repository_id: "example".into(),
                        selector: Some(c::RetainedVersionSelector {
                            workspace_id: binding.workspace_id.to_vec(),
                            generation,
                            manifest_id: manifest_id.0.to_vec(),
                        }),
                        expected_capture_id: manifest.capture.id.0.to_vec(),
                        manifest_selected: Some(c::RawDigest {
                            blake3: raw.blake3.to_vec(),
                            size_bytes: raw.size_bytes,
                        }),
                    }))
                    .await
                    .unwrap()
                    .into_inner();
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
    fixture.success(
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
    assert_eq!(
        fs::read(root.join("capture-only.txt")).unwrap(),
        authored_bytes
    );
    let cache_after_read = fixture.control(Request::Status).unwrap()["cache"].clone();
    assert!(
        cache_after_read["fetches"].as_u64().unwrap() > cache_before["fetches"].as_u64().unwrap()
    );
    assert!(cache_after_read["verified_bytes"].as_u64().unwrap() >= authored_bytes.len() as u64);

    fs::write(
        root.join("capture-only.txt"),
        b"restored and snapshotted edit\n",
    )
    .unwrap();
    fixture.success(&root, &["status"]);
    let current = native_admin_state(&binding);
    let (wc, _) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
            .unwrap();
    assert_ne!(wc.operation.hex(), capture.data.working_copy.wc_operation);
    let dirty = b"unsnapshotted restored edit across restart\n";
    fs::write(root.join("capture-only.txt"), dirty).unwrap();
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
        assert!(
            operations
                .iter()
                .all(|id| id.len() == 128 && id.bytes().all(|b| b.is_ascii_hexdigit()))
        );
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
