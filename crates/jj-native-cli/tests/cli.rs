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
    fn spawn(&self, args: &[&str]) -> OwnedChild {
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
        OwnedChild(Some(child))
    }
    fn run(&self, args: &[&str]) -> Output {
        self.wait(self.spawn(args), args)
    }
    fn wait(&self, mut child: OwnedChild, args: &[&str]) -> Output {
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
    fn roots(&self) -> serde_json::Value {
        let encoded = self.success(&["tinytree", "roots", "--json"]);
        let audit: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(audit["version"], 1);
        assert!(encoded.len() <= audit["encoded_bytes_upper_bound"].as_u64().unwrap() as usize);
        let roots = audit["object_roots"].as_array().unwrap();
        let mut previous = None;
        for root in roots {
            let kind = root["kind"].as_str().unwrap();
            let rank = match kind {
                "file" => 1,
                "tree" => 2,
                "commit" => 3,
                "symlink" => 4,
                "copy_history" => 5,
                _ => panic!("unexpected kind"),
            };
            let id = root["id"].as_str().unwrap();
            assert_eq!(id.len(), 64);
            assert!(
                id.bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            );
            let key = (rank, id);
            assert!(
                previous.is_none_or(|old| old < key),
                "roots must be unique and sorted"
            );
            previous = Some(key);
        }
        audit
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
fn stock_annotation_filters_unchanged_merge_parents_before_attribution() {
    let client = Client::new();
    client.success(&["tinytree", "init"]);
    let path = client.root.join("story.txt");
    fs::write(&path, "x\n").unwrap();
    client.success(&["describe", "-m", "A"]);
    client.success(&["bookmark", "set", "base", "-r", "@"]);

    client.success(&["new", "-r", "base"]);
    client.success(&["describe", "-m", "U"]);
    client.success(&["bookmark", "set", "unchanged", "-r", "@"]);

    client.success(&["new", "-r", "base"]);
    fs::remove_file(&path).unwrap();
    client.success(&["describe", "-m", "D"]);
    client.success(&["new"]);
    fs::write(&path, "x\n").unwrap();
    client.success(&["describe", "-m", "B"]);
    client.success(&["bookmark", "set", "readded", "-r", "@"]);

    client.success(&["new", "-r", "unchanged", "-r", "readded"]);
    client.success(&["describe", "-m", "M"]);
    // This is stock jj's local annotation engine, not the remote policy. The
    // unchanged U is filtered out, then the indirect M->A edge is removed as
    // transitive through B. Raw first-parent propagation would wrongly pick A.
    assert_eq!(
        client.success(&[
            "--ignore-working-copy", "file", "annotate", "story.txt", "-r", "@",
            "-T", "commit.description().first_line() ++ \":\" ++ content",
        ]),
        "B:x\n",
    );
}

#[test]
fn stock_annotation_resolves_recursive_bases_and_ordered_octopus_parents() {
    struct Case {
        commits: Vec<(&'static str, Vec<usize>, &'static str)>,
        annotation: &'static str,
    }
    let cases = [
        Case {
            commits: vec![
                ("base", vec![], "a\nstable\nb\n"),
                ("left", vec![0], "A\nstable\nb\n"),
                ("right", vec![0], "a\nstable\nB\n"),
                ("merge-left", vec![1, 2], "A\nstable\nB\n"),
                ("merge-right", vec![2, 1], "A\nstable\nB\n"),
                ("recursive", vec![3, 4], "A\nstable\nB\n"),
                ("unchanged-head", vec![5], "A\nstable\nB\n"),
            ],
            annotation: "left:A\nbase:stable\nright:B\n",
        },
        Case {
            commits: vec![
                ("base", vec![], "a\nanchor-a\nb\nanchor-b\nc\n"),
                ("first", vec![0], "A\nanchor-a\nb\nanchor-b\nc\n"),
                ("second", vec![0], "a\nanchor-a\nB\nanchor-b\nc\n"),
                ("third", vec![0], "a\nanchor-a\nb\nanchor-b\nC\n"),
                ("octopus", vec![1, 2, 3], "A\nanchor-a\nB\nanchor-b\nC\n"),
                ("unchanged-head", vec![4], "A\nanchor-a\nB\nanchor-b\nC\n"),
            ],
            annotation: "first:A\nbase:anchor-a\nsecond:B\nbase:anchor-b\nthird:C\n",
        },
    ];
    for case in cases {
        let client = Client::new();
        client.success(&["tinytree", "init"]);
        for (index, (description, parents, content)) in case.commits.iter().enumerate() {
            if index != 0 {
                let mut args = vec!["new".to_owned()];
                for parent in parents {
                    args.extend(["-r".to_owned(), format!("node{parent}")]);
                }
                client.success(&args.iter().map(String::as_str).collect::<Vec<_>>());
            }
            fs::write(client.root.join("story.txt"), content).unwrap();
            client.success(&["describe", "-m", description]);
            client.success(&["bookmark", "set", &format!("node{index}"), "-r", "@"]);
        }
        assert_eq!(client.success(&[
            "--ignore-working-copy", "file", "annotate", "story.txt", "-r", "@",
            "-T", "commit.description().first_line() ++ \":\" ++ content",
        ]), case.annotation);
    }
}

#[test]
fn search_validates_regex_before_repository_access_and_accepts_fixed_strings() {
    let client = Client::new(); // Deliberately not a repository.
    let malformed = client.run(&["tinytree", "search", "["]);
    assert!(!malformed.status.success());
    assert!(
        String::from_utf8_lossy(&malformed.stderr).contains("Native search failed:"),
        "malformed regex must be rejected before workspace discovery: {}",
        String::from_utf8_lossy(&malformed.stderr)
    );
    for flag in ["-F", "--fixed-strings"] {
        let literal = client.run(&["tinytree", "search", flag, "["]);
        assert!(!literal.status.success());
        let stderr = String::from_utf8_lossy(&literal.stderr);
        assert!(
            !stderr.contains("Native search failed:") && stderr.contains("repo"),
            "fixed strings must accept regex metacharacters and reach workspace discovery: {stderr}"
        );
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
    // An abandoned operation remains physically retained until jj's operation
    // GC. Its old view/native commit must remain in a read-only root inventory.
    let old_operation = client.success(&["op", "log", "--no-graph", "-n", "1", "-T", "id"]);
    client.success(&["describe", "-m", "rewritten child"]);
    client.success(&["op", "abandon", old_operation.trim()]);
    let audit = client.roots();
    assert!(
        audit["retained_operations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id == old_operation.trim())
    );
    assert!(
        audit["object_roots"]
            .as_array()
            .unwrap()
            .iter()
            .any(|root| root["kind"] == "commit" && root["id"] == child)
    );

    // Audit must not snapshot unsaved filesystem edits or create operations.
    let before = audit["operation_heads"].clone();
    fs::write(client.root.join("hello.txt"), "not snapshotted by audit\n").unwrap();
    let after = client.roots();
    assert_eq!(after["operation_heads"], before);
    assert_eq!(
        after["working_copy_operation"],
        audit["working_copy_operation"]
    );
    assert_eq!(after["object_roots"], audit["object_roots"]);
    let rejected = client.run(&["tinytree", "roots", "--json", "--max-operations", "1"]);
    assert!(!rejected.status.success());
    assert!(
        rejected.stdout.is_empty(),
        "no partial audit after a failed budget"
    );
    assert_eq!(client.roots()["operation_heads"], before);
    let oversized = client.run(&["tinytree", "roots", "--json", "--max-file-bytes", "16"]);
    assert!(!oversized.status.success());
    assert!(
        oversized.stdout.is_empty(),
        "no partial audit after a bounded read failure"
    );
    assert_eq!(client.roots()["operation_heads"], before);
    assert_eq!(
        fs::read_to_string(client.root.join("hello.txt")).unwrap(),
        "not snapshotted by audit\n"
    );
    client.assert_native();
}

#[test]
fn native_root_audit_keeps_negative_working_copy_terms_and_fails_closed() {
    use jj_native_backend::{DirectObjectIo, NativeObjectIo};
    use tinytree_model::{Kind, Object, ObjectId, ObjectKey};
    let client = Client::new();
    client.success(&["tinytree", "init"]);
    fs::write(client.root.join("conflict.txt"), "base\n").unwrap();
    client.success(&["describe", "-m", "base"]);
    let base = client.revision("@");
    client.success(&["new", &base, "-m", "left"]);
    fs::write(client.root.join("conflict.txt"), "left\n").unwrap();
    client.success(&["status"]);
    let left = client.revision("@");
    client.success(&["new", &base, "-m", "right"]);
    fs::write(client.root.join("conflict.txt"), "right\n").unwrap();
    client.success(&["status"]);
    let right = client.revision("@");
    client.success(&["new", &left, &right]);
    let merged = client.revision("@");
    let bytes: Vec<u8> = (0..merged.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&merged[offset..offset + 2], 16).unwrap())
        .collect();
    let provider = DirectObjectIo::load(
        &client.root.join(".jj/repo/store"),
        jj_native_backend::Config::default(),
    )
    .unwrap();
    let metadata = futures::executor::block_on(provider.read_metadata(ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(bytes.try_into().unwrap()),
    }))
    .unwrap();
    let Object::Commit(commit) = metadata.into_object() else {
        panic!("expected commit")
    };
    assert!(
        commit.root_tree.terms.len() >= 3,
        "fixture must have a negative tree term"
    );
    let negative = commit.root_tree.terms[1];
    drop(provider);
    let audit = client.roots();
    for term in &commit.root_tree.terms {
        let id: String = term.0.iter().map(|byte| format!("{byte:02x}")).collect();
        assert!(
            audit["object_roots"]
                .as_array()
                .unwrap()
                .iter()
                .any(|root| root["kind"] == "tree" && root["id"] == id)
        );
    }
    assert_ne!(negative, commit.root_tree.terms[0]);
    // Deliberately corrupt the retained operation fixture, including operations
    // outside current ancestry. No root subset may escape after a missing view.
    let views = client.root.join(".jj/repo/op_store/views");
    let victims: Vec<_> = fs::read_dir(views)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .collect();
    assert!(!victims.is_empty());
    let originals: Vec<_> = victims.iter().map(|path| fs::read(path).unwrap()).collect();
    // Malformed protobuf targets used to panic in jj's assumption-based view
    // conversion before the audit could verify the semantic object identity.
    for malformed in [&[0x4a, 0x00][..], &[0x4a, 0x02, 0x1a, 0x00][..]] {
        for path in &victims {
            fs::write(path, malformed).unwrap();
        }
        let rejected = client.run(&["tinytree", "roots", "--json"]);
        assert_eq!(
            rejected.status.code(),
            Some(1),
            "{}",
            String::from_utf8_lossy(&rejected.stderr)
        );
        assert!(rejected.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&rejected.stderr).contains("panicked"));
        for path in &victims {
            assert_eq!(fs::read(path).unwrap(), malformed);
        }
        for (path, bytes) in victims.iter().zip(&originals) {
            fs::write(path, bytes).unwrap();
        }
    }
    let checkout = client.root.join(".jj/working_copy/checkout");
    let original_checkout = fs::read(&checkout).unwrap();
    let oversized = vec![0u8; 64 * 1024];
    fs::write(&checkout, &oversized).unwrap();
    let rejected = client.run(&["tinytree", "roots", "--json", "--max-file-bytes", "16"]);
    assert_eq!(rejected.status.code(), Some(1));
    assert!(rejected.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&rejected.stderr).contains("panicked"));
    assert_eq!(fs::read(&checkout).unwrap(), oversized);
    fs::write(&checkout, original_checkout).unwrap();
    for victim in victims {
        fs::remove_file(victim).unwrap();
    }
    let rejected = client.run(&["tinytree", "roots", "--json"]);
    assert!(!rejected.status.success());
    assert!(rejected.stdout.is_empty());
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

#[cfg(unix)]
#[derive(Default)]
struct PublicationPause {
    enabled: std::sync::atomic::AtomicBool,
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}
#[cfg(unix)]
struct PausingControl {
    inner: jj_native_remote::RemoteRepositoryControl,
    pause: std::sync::Arc<PublicationPause>,
}
#[cfg(unix)]
impl std::fmt::Debug for PausingControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PausingControl")
            .finish_non_exhaustive()
    }
}
#[cfg(unix)]
#[async_trait::async_trait]
impl native_broker::NativeRepositoryControl for PausingControl {
    fn resources(&self) -> native_broker::ControlResources {
        native_broker::NativeRepositoryControl::resources(&self.inner)
    }
    async fn observe_reference(
        &self,
        name: &str,
    ) -> Result<native_broker::ReferenceSnapshot, native_broker::ControlStatus> {
        native_broker::NativeRepositoryControl::observe_reference(&self.inner, name).await
    }
    async fn admit_commit(
        &self,
        head: native_broker::CommitId,
    ) -> Result<native_broker::AdmittedCommit, native_broker::ControlStatus> {
        native_broker::NativeRepositoryControl::admit_commit(&self.inner, head).await
    }
    async fn publish(
        &self,
        request: native_broker::PublishRequest,
    ) -> Result<native_broker::ConfirmedPublication, native_broker::ControlStatus> {
        let confirmed =
            native_broker::NativeRepositoryControl::publish(&self.inner, request).await?;
        // The real remote provider/server have committed and acknowledged; only
        // this broker response is paused for actual child failure injection.
        if self
            .pause
            .enabled
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.pause.started.notify_one();
            self.pause.resume.notified().await;
        }
        Ok(confirmed)
    }
    async fn publication(
        &self,
        key: &str,
    ) -> Result<Option<native_broker::ConfirmedPublication>, native_broker::ControlStatus> {
        native_broker::NativeRepositoryControl::publication(&self.inner, key).await
    }
}

#[cfg(unix)]
struct NativeRemoteFixture {
    _root: tempfile::TempDir,
    socket: PathBuf,
    store: tinytree_store::Store,
    head: String,
    pause: std::sync::Arc<PublicationPause>,
    broker_stop: Option<tokio::sync::oneshot::Sender<()>>,
    server_stop: Option<tokio::sync::oneshot::Sender<()>>,
    broker_task: Option<tokio::task::JoinHandle<()>>,
    server_task: Option<tokio::task::JoinHandle<()>>,
}
#[cfg(unix)]
impl Drop for NativeRemoteFixture {
    fn drop(&mut self) {
        if let Some(stop) = self.broker_stop.take() {
            let _ = stop.send(());
        }
        if let Some(stop) = self.server_stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.broker_task.take() {
            task.abort();
        }
        if let Some(task) = self.server_task.take() {
            task.abort();
        }
    }
}
#[cfg(unix)]
impl NativeRemoteFixture {
    async fn start() -> Self {
        use std::{collections::HashMap, os::unix::fs::PermissionsExt, sync::Arc};
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.path().canonicalize().unwrap();
        let server_path = path.join("server");
        fs::create_dir(&server_path).unwrap();
        let store = tinytree_store::Store::open(&server_path, Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let fixture = tinytree_fixture::build_fixture(tinytree_fixture::FixtureConfig {
            unused_files: 2,
            bytes_per_file: 64,
        })
        .unwrap();
        let mut head = None;
        for object in fixture.objects() {
            store
                .put("example", object.descriptor, &object.bytes, 0)
                .unwrap();
            if object.descriptor.key.kind == tinytree_model::Kind::Commit {
                head = Some(tinytree_model::CommitId(object.descriptor.key.id.0));
            }
        }
        let head = head.unwrap();
        store
            .publish(
                "example",
                &tinytree_store::PublishRequest {
                    idempotency_key: "seed".into(),
                    reference_name: "main".into(),
                    expected_generation: 0,
                    new_commit: Some(head),
                    staging_leases: vec![],
                },
                0,
            )
            .unwrap();
        let service = tinytree_server::RepositoryService::new(
            store.clone(),
            Default::default(),
            vec!["example".into()],
            vec![
                tinytree_server::Principal::new(
                    "writer",
                    "public-cli-writer-token-not-a-secret",
                    vec!["example".into()],
                    true,
                )
                .unwrap()
                .with_retention(true),
                tinytree_server::Principal::new(
                    "reader",
                    "public-cli-reader-token-not-a-secret",
                    vec!["example".into()],
                    false,
                )
                .unwrap()
                .with_retention(true),
            ],
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (server_stop, stopped) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            tinytree_server::serve_tcp(listener, service, None, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        struct NoDefaultSource;
        #[async_trait::async_trait]
        impl local_cas::BlobSource for NoDefaultSource {
            async fn fetch(
                &self,
                _: &local_cas::Artifact,
                _: &mut std::fs::File,
            ) -> anyhow::Result<()> {
                anyhow::bail!("native test must use its scoped source")
            }
            async fn missing(
                &self,
                artifacts: &[local_cas::Artifact],
            ) -> anyhow::Result<Vec<local_cas::Artifact>> {
                Ok(artifacts.to_vec())
            }
        }
        let cache = local_cas::LocalCas::open(
            path.join("cache"),
            Arc::new(NoDefaultSource),
            Default::default(),
        )
        .unwrap();
        let local_path = path.join("local");
        fs::create_dir(&local_path).unwrap();
        fs::set_permissions(&local_path, fs::Permissions::from_mode(0o700)).unwrap();
        let direct = Arc::new(
            jj_native_backend::DirectObjectIo::init(&local_path, Default::default()).unwrap(),
        );
        let mut config = jj_native_remote::Config::default();
        config.remote.endpoint = format!("http://{address}");
        config.remote.repository_id = "example".into();
        config.remote.token_env = Some("TINYTREE_CLI_WRITER_TOKEN".into());
        config.authorization_profile = "cli-writer".into();
        let reader_config = {
            let mut config = config.clone();
            config.remote.token_env = Some("TINYTREE_CLI_READER_TOKEN".into());
            config.authorization_profile = "cli-reader".into();
            config
        };
        let remote = jj_native_remote::RemoteRepository::open(
            direct,
            cache.clone(),
            &path.join("ledger"),
            &path.join("staging"),
            config,
        )
        .unwrap();
        let reader_path = path.join("reader-local");
        fs::create_dir(&reader_path).unwrap();
        fs::set_permissions(&reader_path, fs::Permissions::from_mode(0o700)).unwrap();
        let reader = jj_native_remote::RemoteRepository::open(
            Arc::new(
                jj_native_backend::DirectObjectIo::init(&reader_path, Default::default()).unwrap(),
            ),
            cache,
            &path.join("reader-ledger"),
            &path.join("reader-staging"),
            reader_config,
        )
        .unwrap();
        let reader_provider: Arc<dyn jj_native_backend::NativeObjectIo> = reader.io;
        let reader_control: Arc<dyn native_broker::NativeRepositoryControl> =
            Arc::new(reader.control);
        let provider: Arc<dyn jj_native_backend::NativeObjectIo> = remote.io;
        let pause = Arc::new(PublicationPause::default());
        let control: Arc<dyn native_broker::NativeRepositoryControl> = Arc::new(PausingControl {
            inner: remote.control,
            pause: pause.clone(),
        });
        let broker = native_broker::BrokerService::new(
            HashMap::from([
                ("example".into(), provider),
                ("reader".into(), reader_provider),
            ]),
            Default::default(),
        )
        .unwrap()
        .with_controls(HashMap::from([
            ("example".into(), control),
            ("reader".into(), reader_control),
        ]))
        .unwrap();
        let socket = path.join("broker.sock");
        let serve_path = socket.clone();
        let (broker_stop, stopped) = tokio::sync::oneshot::channel();
        let broker_task = tokio::spawn(async move {
            native_broker::serve_unix(&serve_path, broker, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        let fixture = Self {
            _root: root,
            socket,
            store,
            head: head.0.iter().map(|b| format!("{b:02x}")).collect(),
            pause,
            broker_stop: Some(broker_stop),
            server_stop: Some(server_stop),
            broker_task: Some(broker_task),
            server_task: Some(server_task),
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    !fixture.broker_task.as_ref().unwrap().is_finished(),
                    "native broker startup failed"
                );
                if fs::metadata(&fixture.socket)
                    .is_ok_and(|metadata| metadata.permissions().mode() & 0o777 == 0o600)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        fixture
    }
    fn public(&self) -> tinytree_store::Reference {
        self.store
            .references("example", &["main".into()])
            .unwrap()
            .remove(0)
    }
    fn clone(&self, client: &Client) {
        self.clone_store(client, "example");
    }
    fn clone_store(&self, client: &Client, store: &str) {
        client.success(&[
            "tinytree",
            "clone",
            ".",
            "--broker",
            self.socket.to_str().unwrap(),
            "--store",
            store,
            "--reference",
            "main",
            "--bookmark",
            "main",
        ]);
    }
    async fn stop(mut self) {
        let _ = self.broker_stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.broker_task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        let _ = self.server_stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.server_task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
}
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_native_remote_cli_preserves_workspace_generations_conflicts_and_undo() {
    let fixture = NativeRemoteFixture::start().await;
    let first = Client::new();
    let second = Client::new();
    fixture.clone(&first);
    fixture.clone(&second);
    for client in [&first, &second] {
        client.assert_native();
        assert_eq!(client.revision("main"), fixture.head);
        assert_eq!(client.revision("@-"), fixture.head);
    }
    first.success(&["bookmark", "set", "created", "-r", "@"]);
    first.success(&[
        "tinytree",
        "fetch",
        "--reference",
        "created",
        "--bookmark",
        "created",
    ]);
    first.success(&[
        "tinytree",
        "push",
        "--reference",
        "created",
        "--bookmark",
        "created",
    ]);
    assert_eq!(
        fixture
            .store
            .references("example", &["created".into()])
            .unwrap()[0]
            .generation,
        1,
        "an explicitly observed absent reference carries generation zero into its first publication"
    );
    fs::write(second.root.join("agent.txt"), "client B data\n").unwrap();
    second.success(&["describe", "-m", "client B change"]);
    second.success(&["bookmark", "set", "main", "-r", "@"]);
    let bhead = second.revision("@");
    second.success(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert_eq!(fixture.public().generation, 2);
    assert_eq!(
        fixture
            .public()
            .commit
            .unwrap()
            .key
            .id
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        bhead
    );
    let sidecar = second.root.join(".jj/working_copy/native-remote.toml");
    let observed = fs::read(&sidecar).unwrap();
    second.success(&["undo"]);
    assert_eq!(
        fs::read(&sidecar).unwrap(),
        observed,
        "jj undo cannot roll back an external observed generation"
    );
    let rejected = second.run(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert!(!rejected.status.success());
    assert_eq!(fixture.public().generation, 2);
    second.success(&[
        "tinytree",
        "fetch",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    second.success(&[
        "bookmark",
        "set",
        "main",
        "--allow-backwards",
        "-r",
        &fixture.head,
    ]);
    second.success(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert_eq!(fixture.public().generation, 3);
    assert_eq!(first.revision("main"), fixture.head);
    let old = fs::read(first.root.join(".jj/working_copy/native-remote.toml")).unwrap();
    let stale = first.run(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert!(
        !stale.status.success(),
        "same head after ABA still requires the workspace's old generation"
    );
    assert_eq!(fixture.public().generation, 3);
    assert_eq!(
        fs::read(first.root.join(".jj/working_copy/native-remote.toml")).unwrap(),
        old
    );
    first.success(&[
        "tinytree",
        "fetch",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    fs::write(first.root.join("agent.txt"), "client A data\n").unwrap();
    first.success(&["describe", "-m", "client A change"]);
    first.success(&["bookmark", "set", "main", "-r", "@"]);
    let ahead = first.revision("@");
    first.success(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert_eq!(fixture.public().generation, 4);
    second.success(&["bookmark", "set", "main", "--allow-backwards", "-r", &bhead]);
    second.success(&[
        "tinytree",
        "fetch",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert_eq!(
        second.success(&["file", "show", "agent.txt"]),
        "client B data\n",
        "fetch must preserve the independent working copy"
    );
    let conflict = second.run(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert!(
        !conflict.status.success(),
        "divergent bookmarks must merge, not silently overwrite local work"
    );
    assert_eq!(fixture.public().generation, 4);
    second.success(&["rebase", "-r", "@", "-d", &ahead]);
    assert!(second.success(&["status"]).contains("conflict"));
    let rebased = second.revision("@");
    second.success(&["bookmark", "set", "main", "--allow-backwards", "-r", "@"]);
    second.success(&[
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ]);
    assert_eq!(fixture.public().generation, 5);
    let third = Client::new();
    fixture.clone(&third);
    assert_eq!(
        third.revision("main"),
        rebased,
        "native tree conflicts survive publication and another materialized clone"
    );
    assert!(
        third
            .success(&["file", "show", "agent.txt"])
            .contains("<<<<<<<")
    );
    assert_eq!(
        first.success(&["file", "show", "agent.txt"]),
        "client A data\n"
    );
    fixture.stop().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_publication_recovery_reuses_one_intent_after_client_death_or_local_state_failure() {
    enum Failure {
        ClientDies,
        StateCommitCollision,
    }
    for failure in [Failure::ClientDies, Failure::StateCommitCollision] {
        let fixture = NativeRemoteFixture::start().await;
        let client = Client::new();
        let quoted_bookmark = "\"feature name\"";
        client.success(&[
            "tinytree",
            "clone",
            ".",
            "--broker",
            fixture.socket.to_str().unwrap(),
            "--store",
            "example",
            "--reference",
            "main",
            "--bookmark",
            quoted_bookmark,
        ]);
        fs::write(
            client.root.join("recovery.txt"),
            "durable publication data\n",
        )
        .unwrap();
        client.success(&["describe", "-m", "recover exact native intent"]);
        client.success(&["bookmark", "set", quoted_bookmark, "-r", "@"]);
        let head = client.revision("@");
        fixture
            .pause
            .enabled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let args = [
            "tinytree",
            "push",
            "--reference",
            "main",
            "--bookmark",
            quoted_bookmark,
        ];
        let mut child = client.spawn(&args);
        tokio::time::timeout(Duration::from_secs(10), fixture.pause.started.notified())
            .await
            .unwrap();
        assert_eq!(fixture.public().generation, 2);
        let sidecar = client.root.join(".jj/working_copy/native-remote.toml");
        assert!(fs::read_to_string(&sidecar).unwrap().contains("request_id"));
        match failure {
            Failure::ClientDies => {
                assert!(child.0.as_mut().unwrap().try_wait().unwrap().is_none());
                child.0.as_mut().unwrap().kill().unwrap();
                assert!(!child.0.take().unwrap().wait().unwrap().success());
                fixture.pause.resume.notify_one();
            }
            Failure::StateCommitCollision => {
                let saved = sidecar.with_extension("saved");
                fs::rename(&sidecar, &saved).unwrap();
                fs::create_dir(&sidecar).unwrap();
                fixture.pause.resume.notify_one();
                let result = client.wait(child, &args);
                assert!(
                    !result.status.success(),
                    "local durable-state failure must not report completion"
                );
                fs::remove_dir(&sidecar).unwrap();
                fs::rename(saved, &sidecar).unwrap();
            }
        }
        client.success(&args);
        assert_eq!(
            fixture.public().generation,
            2,
            "recovery must not create a second publication generation"
        );
        assert_eq!(client.revision("\"feature name\"@tinytree"), head);
        assert!(!fs::read_to_string(&sidecar).unwrap().contains("request_id"));
        fixture.stop().await;
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readonly_failed_push_requires_explicit_bounded_local_replay_abandonment() {
    let fixture = NativeRemoteFixture::start().await;
    let client = Client::new();
    fixture.clone_store(&client, "reader");
    fs::write(
        client.root.join("local-only.txt"),
        "readonly clients can edit locally\n",
    )
    .unwrap();
    client.success(&["describe", "-m", "local readonly work"]);
    client.success(&["bookmark", "set", "main", "-r", "@"]);
    let push = [
        "tinytree",
        "push",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ];
    assert!(!client.run(&push).status.success());
    assert_eq!(fixture.public().generation, 1);
    let fetch = [
        "tinytree",
        "fetch",
        "--reference",
        "main",
        "--bookmark",
        "main",
    ];
    assert!(
        !client.run(&fetch).status.success(),
        "default recovery must not assume a failed publication never committed"
    );
    let path = client.root.join(".jj/working_copy/native-remote.toml");
    let original = fs::read_to_string(&path).unwrap();
    let mut boundary: toml::Value = toml::from_str(&original).unwrap();
    let intent = boundary["pending"].clone();
    let active = client.roots();
    assert!(
        active["intent_operations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id.as_str() == intent["base_operation"].as_str())
    );
    assert!(
        active["object_roots"]
            .as_array()
            .unwrap()
            .iter()
            .any(|root| root["kind"] == "commit" && root["id"].as_str() == intent["head"].as_str())
    );
    let mut missing_active = boundary.clone();
    missing_active["pending"]["base_operation"] = toml::Value::String("ff".repeat(64));
    fs::write(&path, toml::to_string(&missing_active).unwrap()).unwrap();
    let rejected = client.run(&["tinytree", "roots", "--json"]);
    assert!(
        !rejected.status.success(),
        "active recovery requires its base operation"
    );
    assert!(rejected.stdout.is_empty());
    fs::write(&path, &original).unwrap();
    let archived = (0..128)
        .map(|index| {
            let mut intent = intent.clone();
            intent["request_id"] = toml::Value::String(format!("{index:032x}"));
            intent
        })
        .collect();
    boundary["archived_pushes"] = toml::Value::Array(archived);
    fs::write(&path, toml::to_string(&boundary).unwrap()).unwrap();
    let full = fs::read(&path).unwrap();
    let abandon = [
        "tinytree",
        "fetch",
        "--reference",
        "main",
        "--bookmark",
        "main",
        "--abandon-pending-push",
    ];
    assert!(!client.run(&abandon).status.success());
    assert_eq!(
        fs::read(&path).unwrap(),
        full,
        "a full archive must preserve the active replay intent without mutation"
    );
    fs::write(&path, &original).unwrap();
    client.success(&abandon);
    let state: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    assert!(state.get("pending").is_none());
    assert_eq!(state["archived_pushes"].as_array().unwrap().len(), 1);
    // Archival retains the publication head, not an indefinitely required jj
    // operation DAG. Operation GC may already have expired an archived base.
    let mut expired_archive = state.clone();
    expired_archive["archived_pushes"][0]["base_operation"] = toml::Value::String("ff".repeat(64));
    fs::write(&path, toml::to_string(&expired_archive).unwrap()).unwrap();
    let archived_audit = client.roots();
    assert!(
        archived_audit["intent_operations"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        archived_audit["object_roots"]
            .as_array()
            .unwrap()
            .iter()
            .any(|root| root["kind"] == "commit" && root["id"].as_str() == intent["head"].as_str())
    );
    assert_eq!(
        client.success(&["file", "show", "local-only.txt"]),
        "readonly clients can edit locally\n"
    );
    assert_eq!(
        fixture.public().generation,
        1,
        "abandoning local replay is not an upstream write or rollback"
    );
    fixture.stop().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_fetch_preflights_observation_capacity_before_snapshot_or_pending_state() {
    let fixture = NativeRemoteFixture::start().await;
    let client = Client::new();
    fixture.clone(&client);
    let path = client.root.join(".jj/working_copy/native-remote.toml");
    let mut state: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    let observations = state["observations"].as_array_mut().unwrap();
    for index in 0..127 {
        let mut entry = toml::map::Map::new();
        entry.insert(
            "reference".into(),
            toml::Value::String(format!("boundary-{index}")),
        );
        entry.insert(
            "bookmark".into(),
            toml::Value::String(format!("boundary-{index}")),
        );
        entry.insert("generation".into(), toml::Value::Integer(0));
        observations.push(toml::Value::Table(entry));
    }
    fs::write(&path, toml::to_string(&state).unwrap()).unwrap();
    let saved = fs::read(&path).unwrap();
    let before = client.revision("@");
    fs::write(
        client.root.join("must-not-snapshot.txt"),
        "pending ordinary edit\n",
    )
    .unwrap();
    let rejected = client.run(&[
        "tinytree",
        "fetch",
        "--reference",
        "one-too-many",
        "--bookmark",
        "extra",
    ]);
    assert!(!rejected.status.success());
    assert_eq!(
        fs::read(&path).unwrap(),
        saved,
        "admission failure must not leave a pending fetch"
    );
    assert_eq!(
        client
            .success(&[
                "--ignore-working-copy",
                "log",
                "--no-graph",
                "-r",
                "@",
                "-T",
                "commit_id"
            ])
            .trim(),
        before,
        "capacity must be checked before jj snapshots or commits local work"
    );
    assert_eq!(fixture.public().generation, 1);
    fixture.stop().await;
}
