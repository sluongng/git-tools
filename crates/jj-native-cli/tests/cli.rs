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
