//! Real commands in separate processes, with no Git executable on PATH.
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

struct OwnedChild(Option<Child>);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct Client {
    _directory: tempfile::TempDir,
    binary: PathBuf,
    root: PathBuf,
    home: PathBuf,
    config: PathBuf,
    empty_path: PathBuf,
}
impl Client {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("home");
        let root = directory.path().join("workspace");
        let empty_path = directory.path().join("no-programs");
        fs::create_dir(&home).unwrap();
        fs::create_dir(&root).unwrap();
        fs::create_dir(&empty_path).unwrap();
        let config = directory.path().join("jj.toml");
        fs::write(
            &config,
            "[user]\nname = 'Native Fixture'\nemail = 'fixture@example.invalid'\n\
             [operation]\nhostname = 'test'\nusername = 'test'\n\
             [ui]\npaginate = 'never'\n",
        )
        .unwrap();
        Self {
            _directory: directory,
            binary: Path::new(env!("CARGO_BIN_EXE_tinytree-jj"))
                .canonicalize()
                .unwrap(),
            root,
            home,
            config,
            empty_path,
        }
    }
    fn run(&self, args: &[&str]) -> Output {
        let child = Command::new(&self.binary)
            .current_dir(&self.root)
            .args(["--no-pager", "--color=never"])
            .args(args)
            .env_clear()
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("XDG_CONFIG_HOME", self.home.join("config"))
            .env("JJ_CONFIG", &self.config)
            .env("PATH", &self.empty_path)
            .env("TERM", "dumb")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut child = OwnedChild(Some(child));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if child.0.as_mut().unwrap().try_wait().unwrap().is_some() {
                return child.0.take().unwrap().wait_with_output().unwrap();
            }
            assert!(Instant::now() < deadline, "command timed out: {args:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn success(&self, args: &[&str]) -> String {
        let output = self.run(args);
        assert!(
            output.status.success(),
            "{args:?} failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        String::from_utf8(output.stdout).unwrap()
    }
    fn revision(&self, revision: &str) -> String {
        let id = self.success(&["log", "--no-graph", "-r", revision, "-T", "commit_id"]);
        let id = id.trim().to_owned();
        assert_eq!(id.len(), 64, "native commit ID: {id}");
        assert!(id.bytes().all(|c| c.is_ascii_hexdigit()));
        id
    }
    fn assert_native(&self) {
        assert_eq!(
            fs::read_to_string(self.root.join(".jj/repo/store/type")).unwrap(),
            "tinytree-native-v1",
        );
        assert!(!self.root.join(".git").exists());
        assert!(!self.root.join(".jj/repo/store/git").exists());
        assert!(self.root.join(".jj/repo/op_store").is_dir());
        assert!(self.root.join(".jj/repo/op_heads").is_dir());
        assert!(self.root.join(".jj/repo/index").is_dir());
    }
}

#[test]
fn local_native_commands_snapshot_edit_restore_and_undo_without_git() {
    let client = Client::new();
    client.success(&["tinytree", "init"]);
    client.assert_native();
    let initial_change = client.success(&["log", "--no-graph", "-r", "@", "-T", "change_id"]);
    fs::write(client.root.join("hello.txt"), "first native version\n").unwrap();
    client.success(&["status"]);
    assert_eq!(
        client.success(&["file", "show", "hello.txt"]),
        "first native version\n",
    );
    client.success(&["describe", "-m", "native base"]);
    assert_eq!(
        client.success(&["log", "--no-graph", "-r", "@", "-T", "change_id"]),
        initial_change,
    );
    let base = client.revision("@");
    client.success(&["new", "-m", "native child"]);
    fs::write(client.root.join("hello.txt"), "second native version\n").unwrap();
    client.success(&["status"]);
    let child = client.revision("@");
    assert_ne!(base, child);
    assert_eq!(client.revision("@-"), base);

    client.success(&["restore", "--from", "@-", "hello.txt"]);
    assert_eq!(
        fs::read_to_string(client.root.join("hello.txt")).unwrap(),
        "first native version\n",
    );
    client.success(&["undo"]);
    assert_eq!(
        fs::read_to_string(client.root.join("hello.txt")).unwrap(),
        "second native version\n",
    );
    assert_eq!(client.revision("@"), child);
    client.success(&["edit", &base]);
    assert_eq!(
        fs::read_to_string(client.root.join("hello.txt")).unwrap(),
        "first native version\n",
    );
    client.success(&["edit", &child]);
    assert_eq!(
        client.success(&["file", "show", "hello.txt"]),
        "second native version\n",
    );
    assert_eq!(
        client
            .success(&["log", "--no-graph", "-r", "@", "-T", "description"])
            .trim(),
        "native child",
    );
    client.assert_native();
}

#[test]
fn native_init_rejects_unsupported_overrides_and_existing_workspace() {
    let client = Client::new();
    let rejected = client.run(&["--ignore-working-copy", "tinytree", "init", "rejected"]);
    assert!(!rejected.status.success());
    assert!(!client.root.join("rejected").exists());
    client.success(&["tinytree", "init"]);
    fs::write(client.root.join("keep.txt"), "keep existing workspace\n").unwrap();
    client.success(&["status"]);
    let original = client.revision("@");
    let rejected = client.run(&["tinytree", "init"]);
    assert!(!rejected.status.success());
    assert_eq!(client.revision("@"), original);
    assert_eq!(
        fs::read_to_string(client.root.join("keep.txt")).unwrap(),
        "keep existing workspace\n",
    );
    client.assert_native();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_jj_processes_share_broker_objects_not_workspace_state() {
    use jj_native_backend::{DirectObjectIo, NativeObjectIo};
    use std::{collections::HashMap, os::unix::fs::PermissionsExt, sync::Arc};
    let host = tempfile::tempdir().unwrap();
    fs::set_permissions(host.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let host_path = host.path().canonicalize().unwrap();
    let repository = host_path.join("repository");
    fs::create_dir(&repository).unwrap();
    fs::set_permissions(&repository, fs::Permissions::from_mode(0o700)).unwrap();
    let provider: Arc<dyn NativeObjectIo> =
        Arc::new(DirectObjectIo::init(&repository, jj_native_backend::Config::default()).unwrap());
    let service = native_broker::BrokerService::new(
        HashMap::from([("example".into(), provider)]),
        native_broker::Config::default(),
    )
    .unwrap();
    let socket = host_path.join("broker.sock");
    let serve_path = socket.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    // Keep the owned server task even if a child assertion fails.
    struct ServerGuard {
        stop: Option<tokio::sync::oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<()>>,
    }
    impl Drop for ServerGuard {
        fn drop(&mut self) {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
            if let Some(task) = self.task.take() {
                task.abort();
            }
        }
    }
    let task = tokio::spawn(async move {
        native_broker::serve_unix(&serve_path, service, async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    let mut server = ServerGuard {
        stop: Some(stop),
        task: Some(task),
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(
                !server.task.as_ref().unwrap().is_finished(),
                "broker exited before ready"
            );
            if fs::metadata(&socket).is_ok_and(|m| m.permissions().mode() & 0o777 == 0o600) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let first = Client::new();
    let second = Client::new();
    let socket_argument = socket.to_str().unwrap();
    for client in [&first, &second] {
        client.success(&[
            "tinytree",
            "init",
            "--broker",
            socket_argument,
            "--store",
            "example",
        ]);
        client.assert_native();
        assert!(
            client
                .root
                .join(".jj/repo/store/native-broker.toml")
                .is_file()
        );
        assert!(!client.root.join(".jj/repo/store/native-objects").exists());
    }
    fs::write(first.root.join("shared.txt"), "first workspace\n").unwrap();
    fs::write(second.root.join("shared.txt"), "second workspace\n").unwrap();
    std::thread::scope(|scope| {
        let a = scope.spawn(|| first.success(&["describe", "-m", "first private operation"]));
        let b = scope.spawn(|| second.success(&["describe", "-m", "second private operation"]));
        a.join().unwrap();
        b.join().unwrap();
    });
    assert_ne!(first.revision("@"), second.revision("@"));
    assert_eq!(
        first.success(&["file", "show", "shared.txt"]),
        "first workspace\n"
    );
    assert_eq!(
        second.success(&["file", "show", "shared.txt"]),
        "second workspace\n"
    );
    // Every command loads the durable locator in a new process; no process opens
    // the authoritative store and no Git program is available on the empty PATH.
    first.success(&["new", "-m", "first private child"]);
    second.success(&["status"]);
    first.success(&["undo"]);
    assert_eq!(
        second.success(&["file", "show", "shared.txt"]),
        "second workspace\n"
    );
    let _ = server.stop.take().unwrap().send(());
    tokio::time::timeout(Duration::from_secs(5), server.task.take().unwrap())
        .await
        .unwrap()
        .unwrap();
}
