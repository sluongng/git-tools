use std::{
    collections::BTreeMap,
    io::Read as _,
    path::{Path, PathBuf},
    process::Command,
};
use tinytree_client::{Config, NativeClient};
use tinytree_import::{Limits, Options, Report, import_snapshot};
use tinytree_model as m;
use tinytree_server::{Principal, RepositoryService, ServiceConfig};
use tinytree_store::Store;

const TOKEN: &str = "public-import-loopback-token-not-secret";
fn git_path() -> PathBuf {
    std::env::var_os("GIT_PROXY_TEST_GIT")
        .expect("declared Git fixture tool")
        .into()
}
fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new(git_path())
        .arg("-C")
        .arg(dir)
        .args([
            "-c",
            "user.name=Snapshot Fixture",
            "-c",
            "user.email=fixture@example.invalid",
        ])
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture Git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .trim_end()
        .to_owned()
}
struct Input {
    _temporary: tempfile::TempDir,
    work: PathBuf,
    bare: PathBuf,
    revision: String,
}
impl Input {
    fn new(format: &str, many: bool) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let work = temporary.path().join("source");
        std::fs::create_dir(&work).unwrap();
        git(&work, &["init", &format!("--object-format={format}")]);
        std::fs::write(work.join("tracked.txt"), b"committed source\n").unwrap();
        std::fs::write(work.join("empty"), []).unwrap();
        std::fs::create_dir(work.join("bin")).unwrap();
        std::fs::write(work.join("bin/tool"), b"#!/bin/sh\nprintf fixture\n").unwrap();
        std::fs::write(work.join(".gitignore"), b"user.bazelrc\n").unwrap();
        if many {
            for index in 0..300 {
                std::fs::write(
                    work.join(format!("file-{index:03}")),
                    format!("unique fixture {index}\n"),
                )
                .unwrap();
            }
        }
        git(&work, &["add", "."]);
        git(&work, &["update-index", "--chmod=+x", "bin/tool"]);
        let mut child = Command::new(git_path())
            .arg("-C")
            .arg(&work)
            .args(["hash-object", "-w", "--stdin"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        std::io::Write::write_all(child.stdin.as_mut().unwrap(), b"tracked.txt").unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        let oid = String::from_utf8(output.stdout).unwrap();
        git(
            &work,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("120000,{},link", oid.trim()),
            ],
        );
        git(&work, &["commit", "-m", "public fixture snapshot"]);
        let revision = git(&work, &["rev-parse", "HEAD"]);
        std::fs::write(work.join("tracked.txt"), b"dirty worktree sentinel").unwrap();
        std::fs::write(work.join("untracked-secret"), b"untracked sentinel").unwrap();
        std::fs::write(work.join("user.bazelrc"), b"ignored credential sentinel").unwrap();
        let bare = temporary.path().join("bare.git");
        git(
            temporary.path(),
            &[
                "clone",
                "--bare",
                work.to_str().unwrap(),
                bare.to_str().unwrap(),
            ],
        );
        Self {
            _temporary: temporary,
            work,
            bare,
            revision,
        }
    }
    fn options(&self, source: &Path, reference: &str) -> Options {
        Options {
            source_git: source.to_owned(),
            git_program: git_path(),
            revision: self.revision.clone(),
            repository_id: "example".into(),
            reference: reference.into(),
            expected_generation: 0,
            limits: Limits::default(),
        }
    }
}
struct Running {
    _temporary: tempfile::TempDir,
    store: Store,
    config: Config,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Running {
    async fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path().join("store"), Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let service = RepositoryService::new(
            store.clone(),
            ServiceConfig::default(),
            vec!["example".into()],
            vec![
                Principal::new("fixture", TOKEN, vec!["example".into()], true)
                    .unwrap()
                    .with_retention(true),
            ],
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Config {
            endpoint: format!("http://{}", listener.local_addr().unwrap()),
            repository_id: "example".into(),
            token_env: Some("TINYTREE_IMPORT_TEST_TOKEN".into()),
            ..Default::default()
        };
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            tinytree_server::serve_tcp(listener, service, None, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        Self {
            _temporary: temporary,
            store,
            config,
            stop,
            task,
        }
    }
    async fn client(&self) -> NativeClient {
        NativeClient::connect(self.config.clone()).await.unwrap()
    }
    async fn stop(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap();
    }
}
fn object(store: &Store, kind: m::Kind, id: [u8; 32]) -> m::Object {
    let (_, bytes) = store
        .read_metadata(
            "example",
            m::ObjectKey {
                kind,
                id: m::ObjectId(id),
            },
        )
        .unwrap();
    m::decode(&bytes, Default::default()).unwrap()
}
fn entries(
    store: &Store,
    tree: m::TreeId,
    prefix: &str,
    output: &mut BTreeMap<String, m::TreeValue>,
) {
    let m::Object::Tree(tree) = object(store, m::Kind::Tree, tree.0) else {
        panic!("tree");
    };
    for entry in tree.entries {
        let path = format!("{prefix}{}", entry.name);
        match entry.value {
            m::TreeValue::Tree(child) => entries(store, child, &format!("{path}/"), output),
            value => {
                output.insert(path, value);
            }
        }
    }
}
fn native_id(report: &Report) -> [u8; 32] {
    *blake3::Hash::from_hex(&report.native_commit)
        .unwrap()
        .as_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_snapshot_ignores_dirty_and_untracked_files_and_publishes_large_closure() {
    let input = Input::new("sha1", true);
    let running = Running::new().await;
    let config_file = running._temporary.path().join("client.toml");
    std::fs::write(&config_file, toml::to_string(&running.config).unwrap()).unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_tinytree-import"))
        .args(["--source-git"])
        .arg(&input.work)
        .args(["--revision", &input.revision, "--config"])
        .arg(&config_file)
        .args(["--reference", "snapshots/example", "--git"])
        .arg(git_path())
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["files"], 304);
    assert_eq!(report["symlinks"], 1);
    assert_eq!(report["generation"], 1);
    let report = import_snapshot(
        running.client().await,
        input.options(&input.bare, "snapshots/example"),
    )
    .await
    .unwrap();
    assert!(report.reused_current_reference);
    assert_eq!(report.uploaded_objects, 0);
    assert_eq!(report.generation, 1);
    let m::Object::Commit(commit) = object(&running.store, m::Kind::Commit, native_id(&report))
    else {
        panic!("commit");
    };
    let mut files = BTreeMap::new();
    entries(&running.store, commit.root_tree.terms[0], "", &mut files);
    assert_eq!(files.len(), 305);
    assert!(!files.contains_key("untracked-secret"));
    assert!(!files.contains_key("user.bazelrc"));
    let m::TreeValue::File {
        id,
        executable,
        size_bytes,
        ..
    } = files["tracked.txt"]
    else {
        panic!("file");
    };
    assert!(!executable);
    assert_eq!(size_bytes, 17);
    assert!(matches!(
        files["bin/tool"],
        m::TreeValue::File {
            executable: true,
            ..
        }
    ));
    let m::TreeValue::Symlink { id: link, .. } = files["link"] else {
        panic!("symlink");
    };
    assert_eq!(
        object(&running.store, m::Kind::Symlink, link.0),
        m::Object::Symlink("tracked.txt".into())
    );
    // Model a listing/ODB race deterministically: a Git wrapper reports a
    // foreign file for ls-tree while cat-file serves the pinned original ODB.
    // Import must derive every path/mode/OID from authenticated TREE bytes,
    // never combine a listing with a later, independently valid TREE audit.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let foreign_oid = git(&input.work, &["hash-object", "-w", "untracked-secret"]);
        let wrapper = input._temporary.path().join("inconsistent-git");
        let declared_git = git_path().to_str().unwrap().replace('\'', "'\\''");
        std::fs::write(&wrapper, format!(
            "#!/bin/sh\nfor arg do\nif [ \"$arg\" = ls-tree ]; then\nprintf '100644 blob {foreign_oid} 18\\tforeign-secret\\0'\nexit 0\nfi\ndone\nexec '{declared_git}' \"$@\"\n"
        )).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut inconsistent = input.options(&input.work, "inconsistent-listing");
        inconsistent.git_program = wrapper;
        let imported = import_snapshot(running.client().await, inconsistent)
            .await
            .unwrap();
        assert_eq!(imported.native_tree, report.native_tree);
        assert_eq!(imported.native_commit, report.native_commit);
        let foreign = m::describe_file(b"untracked sentinel");
        assert!(
            running.store.read("example", foreign.key).is_err(),
            "foreign listing content must never be uploaded"
        );
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    running.store.gc("example", now + 7200).unwrap();
    // A consumer fetches exactly the selected FILE after publication/GC; no checkout exists in the importer.
    let mut parent_builder = tempfile::Builder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        parent_builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    let parent = parent_builder.tempdir().unwrap();
    let downloaded = running
        .client()
        .await
        .download_file(m::ObjectId(id.0), parent.path())
        .await
        .unwrap();
    let mut bytes = Vec::new();
    downloaded.open().unwrap().read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"committed source\n");
    assert_eq!(
        std::fs::read(input.work.join("tracked.txt")).unwrap(),
        b"dirty worktree sentinel"
    );
    assert_eq!(
        std::fs::read(input.work.join("user.bazelrc")).unwrap(),
        b"ignored credential sentinel"
    );
    running.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sha256_snapshots_are_deterministic_and_limits_or_stale_generation_do_not_publish() {
    let input = Input::new("sha256", false);
    let running = Running::new().await;
    let client = running.client().await;
    let first = import_snapshot(client.clone(), input.options(&input.bare, "sha256"))
        .await
        .unwrap();
    let second = import_snapshot(client.clone(), input.options(&input.work, "sha256"))
        .await
        .unwrap();
    assert_eq!(first.native_commit, second.native_commit);
    assert!(second.reused_current_reference);
    for (name, mut options) in [
        ("entry-limit", input.options(&input.bare, "entry-limit")),
        ("file-limit", input.options(&input.bare, "file-limit")),
        ("revision", input.options(&input.bare, "revision")),
    ] {
        match name {
            "entry-limit" => options.limits.max_entries = 1,
            "file-limit" => options.limits.max_file_bytes = 1,
            _ => options.revision = "HEAD".into(),
        }
        assert!(import_snapshot(client.clone(), options).await.is_err());
        assert!(
            client.open(vec![name.into()]).await.unwrap().references[0]
                .commit
                .is_none()
        );
    }
    // A newer immutable snapshot cannot overwrite an occupied reference at the default generation zero.
    std::fs::write(
        input.work.join("tracked.txt"),
        b"another committed snapshot\n",
    )
    .unwrap();
    git(&input.work, &["add", "tracked.txt"]);
    git(&input.work, &["commit", "-m", "second snapshot"]);
    let mut newer = input.options(&input.work, "sha256");
    newer.revision = git(&input.work, &["rev-parse", "HEAD"]);
    assert!(
        import_snapshot(client.clone(), newer.clone())
            .await
            .is_err()
    );
    assert_eq!(
        client.open(vec!["sha256".into()]).await.unwrap().references[0].generation,
        1
    );
    newer.expected_generation = 1;
    let third = import_snapshot(client.clone(), newer).await.unwrap();
    assert_eq!(third.generation, 2);
    assert_ne!(third.native_commit, first.native_commit);
    // A well-formed commit stored under another immutable ID must not be
    // accepted merely because Git can parse it. This touches fixture ODB only.
    let tree = git(
        &input.work,
        &["rev-parse", &format!("{}^{{tree}}", input.revision)],
    );
    let altered = git(
        &input.work,
        &["commit-tree", &tree, "-m", "different object bytes"],
    );
    let original_path = input
        .bare
        .join("objects")
        .join(&input.revision[..2])
        .join(&input.revision[2..]);
    let altered_path = input
        .work
        .join(".git/objects")
        .join(&altered[..2])
        .join(&altered[2..]);
    std::fs::remove_file(&original_path).unwrap();
    std::fs::copy(altered_path, original_path).unwrap();
    let corrupt = input.options(&input.bare, "corrupt-commit");
    let error = import_snapshot(client.clone(), corrupt).await.unwrap_err();
    assert!(
        error.to_string().contains("Git commit checksum mismatch")
            || error.to_string().contains("Git object reader failed"),
        "{error:#}"
    );
    assert!(
        client
            .open(vec!["corrupt-commit".into()])
            .await
            .unwrap()
            .references[0]
            .commit
            .is_none()
    );
    // Gitlinks cannot be silently replaced by empty directories or synthetic text.
    git(
        &input.work,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{},submodule", input.revision),
        ],
    );
    git(&input.work, &["commit", "-m", "gitlink boundary"]);
    let mut unsupported = input.options(&input.work, "unsupported");
    unsupported.revision = git(&input.work, &["rev-parse", "HEAD"]);
    assert!(import_snapshot(client.clone(), unsupported).await.is_err());
    assert!(
        client
            .open(vec!["unsupported".into()])
            .await
            .unwrap()
            .references[0]
            .commit
            .is_none()
    );
    running.stop().await;
}

#[cfg(unix)]
fn history_options(
    input: &Input,
    state: PathBuf,
    head: String,
) -> tinytree_import::history::HistoryOptions {
    tinytree_import::history::HistoryOptions {
        source_git: input.work.clone(),
        revision: head,
        state_dir: state,
        reference: "history/main".into(),
        expected_generation: 0,
        git_program: git_path(),
        limits: Default::default(),
    }
}
#[cfg(unix)]
fn git_bytes(dir: &Path, args: &[&str]) -> Vec<u8> {
    let output = Command::new(git_path())
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
#[cfg(unix)]
fn git_objects(dir: &Path, head: &str) -> BTreeMap<String, (String, Vec<u8>)> {
    let ids = git(dir, &["rev-list", "--objects", "--no-object-names", head]);
    ids.lines()
        .map(|id| {
            let kind = git(dir, &["cat-file", "-t", id]);
            let bytes = git_bytes(dir, &["cat-file", &kind, id]);
            (id.to_string(), (kind, bytes))
        })
        .collect()
}
#[cfg(unix)]
fn history_fixture() -> (Input, String) {
    let input = Input::new("sha1", false);
    std::fs::write(input.work.join("binary.dat"), b"binary\0fixture\xff\n").unwrap();
    git(&input.work, &["add", "binary.dat"]);
    git(&input.work, &["commit", "-m", "second public commit"]);
    let second = git(&input.work, &["rev-parse", "HEAD"]);
    let tree = git(&input.work, &["rev-parse", "HEAD^{tree}"]);
    let left = git(
        &input.work,
        &[
            "commit-tree",
            &tree,
            "-p",
            &second,
            "-m",
            "left public parent",
        ],
    );
    let right = git(
        &input.work,
        &[
            "commit-tree",
            &tree,
            "-p",
            &second,
            "-m",
            "right public parent",
        ],
    );
    let merge = git(
        &input.work,
        &[
            "commit-tree",
            &tree,
            "-p",
            &right,
            "-p",
            &left,
            "-m",
            "ordered public merge",
        ],
    );
    git(&input.work, &["update-ref", "refs/heads/history", &merge]);
    (input, merge)
}
#[cfg(unix)]
#[tokio::test]
async fn history_cli_roundtrip_preserves_all_original_git_ids_and_recovers_provenance_without_local_state()
 {
    use tinytree_import::history::{HistoryProof, HistoryReport, import_history, verify_history};
    let (input, head) = history_fixture();
    let running = Running::new().await;
    let options = history_options(
        &input,
        input._temporary.path().join("history-state"),
        head.clone(),
    );
    let config_file = input._temporary.path().join("history-client.toml");
    std::fs::write(&config_file, toml::to_string(&running.config).unwrap()).unwrap();
    let result = tokio::process::Command::new(env!("CARGO_BIN_EXE_tinytree-import"))
        .arg("--history")
        .arg("--state")
        .arg(&options.state_dir)
        .arg("--source-git")
        .arg(&input.work)
        .arg("--revision")
        .arg(&head)
        .arg("--config")
        .arg(&config_file)
        .args(["--reference", "history/main", "--git"])
        .arg(git_path())
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: HistoryReport = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report.commits, 5);
    assert_eq!(report.git_head, head);
    assert_eq!(report.publication_generation, Some(1));
    assert!(report.is_current);
    let proof = HistoryProof {
        retention_hold: report.retention_hold.clone(),
        provenance_checkpoint: report.provenance_checkpoint.clone(),
    };
    let verified = verify_history(running.config.clone(), proof.clone(), Default::default())
        .await
        .unwrap();
    assert_eq!(verified.native_head, report.native_head);
    let receiver = input._temporary.path().join("history-receiver.git");
    let mut export_options = tinytree_git::ExportOptions::new(
        input._temporary.path().join("export-state"),
        receiver.clone(),
        git_path(),
    );
    export_options.source_reference = "history/main".into();
    let exported = tinytree_git::export(running.config.clone(), export_options.clone())
        .await
        .unwrap();
    assert_eq!(exported.git_head, head);
    let originals = git_objects(&input.work, &head);
    assert_eq!(git_objects(&receiver, &head), originals);
    let all = git(
        &receiver,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname)",
        ],
    );
    assert_eq!(
        all.lines().collect::<std::collections::BTreeSet<_>>(),
        originals.keys().map(String::as_str).collect()
    );
    for id in all.lines() {
        let kind = git(&receiver, &["cat-file", "-t", id]);
        let bytes = git_bytes(&receiver, &["cat-file", &kind, id]);
        for sentinel in [
            b"dirty worktree sentinel".as_slice(),
            b"untracked sentinel",
            b"ignored credential sentinel",
        ] {
            assert!(!bytes.windows(sentinel.len()).any(|w| w == sentinel));
        }
    }
    git(&receiver, &["fsck", "--full", "--strict"]);
    let clone = input._temporary.path().join("fresh-history-clone");
    git(
        input._temporary.path(),
        &[
            "clone",
            "--branch",
            "main",
            receiver.to_str().unwrap(),
            clone.to_str().unwrap(),
        ],
    );
    assert_eq!(
        std::fs::read(clone.join("binary.dat")).unwrap(),
        b"binary\0fixture\xff\n"
    );
    assert_eq!(std::fs::read(clone.join("empty")).unwrap(), b"");
    assert_eq!(
        std::fs::read_link(clone.join("link")).unwrap(),
        PathBuf::from("tracked.txt")
    );
    use std::os::unix::fs::PermissionsExt;
    assert_ne!(
        std::fs::metadata(clone.join("bin/tool"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111,
        0
    );
    let replay = import_history(running.config.clone(), options.clone())
        .await
        .unwrap();
    assert_eq!(replay.native_head, report.native_head);
    assert_eq!(replay.retention_hold, report.retention_hold);
    // A native-authored descendant keeps the exact original Git tip as parent.
    let native = blake3::Hash::from_hex(&report.native_head).unwrap();
    let m::Object::Commit(mut child) = object(&running.store, m::Kind::Commit, *native.as_bytes())
    else {
        panic!()
    };
    child.parents = vec![m::CommitId(*native.as_bytes())];
    child.description = "native child after original Git history\n".into();
    child.change_id = m::ChangeId([77; 16]);
    let encoded = m::encode(&m::Object::Commit(child), Default::default()).unwrap();
    running
        .store
        .put("example", encoded.descriptor, &encoded.bytes, 0)
        .unwrap();
    running
        .store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "native-history-child".into(),
                reference_name: "history/main".into(),
                expected_generation: 1,
                new_commit: Some(m::CommitId(encoded.descriptor.key.id.0)),
                staging_leases: vec![],
            },
            0,
        )
        .unwrap();
    let child_export = tinytree_git::export(running.config.clone(), export_options.clone())
        .await
        .unwrap();
    assert_eq!(
        git(
            &receiver,
            &["rev-parse", &format!("{}^", child_export.git_head)]
        ),
        head
    );
    // Stable receipt proves generation1, not that the target still equals it.
    let historical = import_history(running.config.clone(), options.clone())
        .await
        .unwrap();
    assert_eq!(historical.publication_generation, Some(1));
    assert_eq!(historical.current_generation, 2);
    assert!(!historical.is_current);
    assert_eq!(
        historical.current_native_head,
        Some(
            blake3::Hash::from(encoded.descriptor.key.id.0)
                .to_hex()
                .to_string()
        )
    );
    // Empty local source, adapter and receiver: explicit retained proof remains
    // verifiable; native public history regenerates original IDs and its child.
    std::fs::remove_dir_all(&options.state_dir).unwrap();
    std::fs::remove_dir_all(&input.work).unwrap();
    std::fs::remove_dir_all(&input.bare).unwrap();
    std::fs::remove_dir_all(&receiver).unwrap();
    std::fs::remove_dir_all(&export_options.state_dir).unwrap();
    let recovered = verify_history(running.config.clone(), proof, Default::default())
        .await
        .unwrap();
    assert_eq!(recovered.native_head, report.native_head);
    assert!(!recovered.is_current);
    let regenerated = tinytree_git::export(running.config.clone(), export_options)
        .await
        .unwrap();
    assert_eq!(regenerated.git_head, child_export.git_head);
    for (id, (kind, bytes)) in originals {
        assert_eq!(git_bytes(&receiver, &["cat-file", &kind, &id]), bytes);
    }
    running.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn history_native_cas_conflict_keeps_durable_provenance_and_never_overwrites_competing_writer()
 {
    use tinytree_import::history::{HistoryProof, import_history, verify_history};
    let (input, head) = history_fixture();
    let running = Running::new().await;
    let snapshot = import_snapshot(
        running.client().await,
        input.options(&input.work, "history/main"),
    )
    .await
    .unwrap();
    let options = history_options(&input, input._temporary.path().join("conflict-state"), head);
    assert!(
        import_history(running.config.clone(), options.clone())
            .await
            .is_err()
    );
    let envelope: serde_json::Value =
        serde_json::from_slice(&std::fs::read(options.state_dir.join("journal.json")).unwrap())
            .unwrap();
    let proof = HistoryProof {
        retention_hold: envelope["journal"]["hold"].as_str().unwrap().into(),
        provenance_checkpoint: envelope["journal"]["checkpoint"].as_str().unwrap().into(),
    };
    let verified = verify_history(running.config.clone(), proof, Default::default())
        .await
        .unwrap();
    assert_eq!(verified.publication_generation, None);
    assert!(!verified.is_current);
    assert_eq!(verified.current_native_head, Some(snapshot.native_commit));
    assert!(
        import_history(running.config.clone(), options.clone())
            .await
            .is_err()
    );
    // Even an exact current generation is not permission to replace a nonempty target.
    let mut replacement = options;
    replacement.expected_generation = 1;
    replacement.state_dir = input._temporary.path().join("nonempty-current-generation");
    assert!(
        import_history(running.config.clone(), replacement)
            .await
            .is_err()
    );
    assert_eq!(
        running
            .client()
            .await
            .open(vec!["history/main".into()])
            .await
            .unwrap()
            .references[0]
            .generation,
        1
    );
    running.stop().await;
}

#[cfg(unix)]
fn git_write_object(dir: &Path, kind: &str, bytes: &[u8]) -> String {
    use std::io::Write;
    let mut child = Command::new(git_path())
        .arg("-C")
        .arg(dir)
        .args(["hash-object", "-w", "--stdin", "-t", kind])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(bytes).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout).unwrap().trim().into()
}
#[cfg(unix)]
async fn forged_proof(
    running: &Running,
    original: &tinytree_import::history::HistoryReport,
    manifest: &serde_json::Value,
) -> tinytree_import::history::HistoryProof {
    let bytes = serde_json::to_vec(manifest).unwrap();
    let descriptor = m::describe_file(&bytes);
    running.store.put("example", descriptor, &bytes, 0).unwrap();
    let checkpoint_id = *blake3::Hash::from_hex(&original.provenance_checkpoint)
        .unwrap()
        .as_bytes();
    let m::Object::Commit(mut checkpoint) = object(&running.store, m::Kind::Commit, checkpoint_id)
    else {
        panic!()
    };
    let m::Object::Tree(mut tree) = object(
        &running.store,
        m::Kind::Tree,
        checkpoint.root_tree.terms[0].0,
    ) else {
        panic!()
    };
    tree.entries[0].value = m::TreeValue::File {
        id: m::FileId(descriptor.key.id.0),
        executable: false,
        size_bytes: descriptor.size_bytes,
        copy_id: None,
    };
    let tree = m::encode(&m::Object::Tree(tree), Default::default()).unwrap();
    running
        .store
        .put("example", tree.descriptor, &tree.bytes, 0)
        .unwrap();
    checkpoint.root_tree.terms[0] = m::TreeId(tree.descriptor.key.id.0);
    checkpoint.parents = vec![m::CommitId(
        *blake3::Hash::from_hex(manifest["native_head"].as_str().unwrap())
            .unwrap()
            .as_bytes(),
    )];
    let mut change = blake3::Hasher::new();
    change.update(b"tinytree-history-provenance-v1\0");
    change.update(&descriptor.key.id.0);
    checkpoint.change_id = m::ChangeId(change.finalize().as_bytes()[..16].try_into().unwrap());
    let encoded = m::encode(&m::Object::Commit(checkpoint), Default::default()).unwrap();
    running
        .store
        .put("example", encoded.descriptor, &encoded.bytes, 0)
        .unwrap();
    let native = blake3::Hash::from(encoded.descriptor.key.id.0)
        .to_hex()
        .to_string();
    let hold = format!(
        "{}/{}",
        original.retention_hold.rsplit_once('/').unwrap().0,
        native
    );
    running
        .client()
        .await
        .publish(tinytree_client::protocol::PublishReferenceRequest {
            repository_id: "example".into(),
            reference_name: hold.clone(),
            expected_generation: 0,
            new_commit: Some(tinytree_client::protocol::ObjectKey {
                kind: tinytree_client::protocol::ObjectKind::Commit as i32,
                native_id: encoded.descriptor.key.id.0.to_vec(),
            }),
            idempotency_key: format!("forged-{}", &native[..32]),
            staging_leases: vec![],
        })
        .await
        .unwrap();
    tinytree_import::history::HistoryProof {
        retention_hold: hold,
        provenance_checkpoint: native,
    }
}
#[cfg(unix)]
#[tokio::test]
async fn history_provenance_verifier_rejects_forged_commit_tree_and_ordered_parent_associations() {
    use tinytree_import::history::{import_history, verify_history};
    let (input, head) = history_fixture();
    let running = Running::new().await;
    let report = import_history(
        running.config.clone(),
        history_options(
            &input,
            input._temporary.path().join("proof-state"),
            head.clone(),
        ),
    )
    .await
    .unwrap();
    let client = running.client().await;
    let private_stage = input._temporary.path().join("proof-download");
    std::fs::create_dir(&private_stage).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&private_stage, std::fs::Permissions::from_mode(0o700)).unwrap();
    let file = client
        .download_file(
            m::ObjectId(
                *blake3::Hash::from_hex(&report.provenance_manifest)
                    .unwrap()
                    .as_bytes(),
            ),
            &private_stage,
        )
        .await
        .unwrap();
    let mut bytes = vec![];
    file.open().unwrap().read_to_end(&mut bytes).unwrap();
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    enum Forgery {
        Commit,
        Tree,
        OrderedParents,
    }
    for forgery in [Forgery::Commit, Forgery::Tree, Forgery::OrderedParents] {
        let mut manifest = original.clone();
        match forgery {
            Forgery::Commit => {
                let replacement = manifest["commits"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|r| r["git"] != head)
                    .unwrap()["native"]
                    .clone();
                manifest["native_head"] = replacement.clone();
                let row = manifest["commits"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|r| r["git"] == head)
                    .unwrap();
                row["native"] = replacement;
            }
            Forgery::Tree => {
                let rows = manifest["trees"].as_array_mut().unwrap();
                assert!(rows.len() > 1);
                rows[0]["native"] = rows[1]["native"].clone();
            }
            Forgery::OrderedParents => {
                let row = manifest["commits"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|r| r["git"] == head)
                    .unwrap();
                let raw: Vec<u8> = serde_json::from_value(row["raw"].clone()).unwrap();
                let raw = String::from_utf8(raw).unwrap();
                let mut lines = raw.split('\n').map(str::to_string).collect::<Vec<_>>();
                assert!(lines[1].starts_with("parent ") && lines[2].starts_with("parent "));
                lines.swap(1, 2);
                let raw = lines.join("\n").into_bytes();
                let changed = git_write_object(&input.work, "commit", &raw);
                row["raw"] = serde_json::to_value(raw).unwrap();
                row["git"] = changed.clone().into();
                manifest["git_head"] = changed.into();
                // Manifest rows are canonical Git-OID ordered, as real migration output is.
                manifest["commits"]
                    .as_array_mut()
                    .unwrap()
                    .sort_by(|a, b| a["git"].as_str().cmp(&b["git"].as_str()));
            }
        }
        let proof = forged_proof(&running, &report, &manifest).await;
        assert!(
            verify_history(running.config.clone(), proof, Default::default())
                .await
                .is_err()
        );
    }
    running.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn history_unsupported_headers_corrupt_git_and_changed_identity_fail_without_publication() {
    use tinytree_import::history::import_history;
    let input = Input::new("sha1", false);
    let running = Running::new().await;
    let raw = git_bytes(&input.work, &["cat-file", "commit", &input.revision]);
    let split = raw.windows(2).position(|b| b == b"\n\n").unwrap();
    for (name, header) in [
        (
            "signature",
            b"gpgsig opaque-signature\n continuation".as_slice(),
        ),
        ("encoding", b"encoding ISO-8859-1"),
        ("extra", b"extra unsupported"),
    ] {
        let mut changed = raw[..split].to_vec();
        changed.push(b'\n');
        changed.extend_from_slice(header);
        changed.extend_from_slice(&raw[split..]);
        let head = git_write_object(&input.work, "commit", &changed);
        let options = history_options(&input, input._temporary.path().join(name), head);
        assert!(
            import_history(running.config.clone(), options.clone())
                .await
                .is_err()
        );
        let journal: serde_json::Value =
            serde_json::from_slice(&std::fs::read(options.state_dir.join("journal.json")).unwrap())
                .unwrap();
        assert!(journal["journal"]["hold"].is_null());
        assert!(
            running
                .client()
                .await
                .open(vec!["history/main".into()])
                .await
                .unwrap()
                .references[0]
                .commit
                .is_none()
        );
    }
    let mut duplicate = raw.clone();
    let first = duplicate.iter().position(|b| *b == b'\n').unwrap() + 1;
    let duplicate_headers = format!("parent {}\nparent {}\n", input.revision, input.revision);
    duplicate.splice(first..first, duplicate_headers.bytes());
    let duplicate = git_write_object(&input.work, "commit", &duplicate);
    assert!(
        import_history(
            running.config.clone(),
            history_options(
                &input,
                input._temporary.path().join("duplicate-parents"),
                duplicate
            )
        )
        .await
        .is_err()
    );
    let tree = git(&input.work, &["rev-parse", "HEAD^{tree}"]);
    let altered = git(
        &input.work,
        &["commit-tree", &tree, "-m", "different hash preimage"],
    );
    std::fs::copy(
        input
            .work
            .join(".git/objects")
            .join(&altered[..2])
            .join(&altered[2..]),
        input
            .work
            .join(".git/objects")
            .join(&input.revision[..2])
            .join(&input.revision[2..]),
    )
    .unwrap();
    let options = history_options(
        &input,
        input._temporary.path().join("corrupt-source"),
        input.revision.clone(),
    );
    assert!(
        import_history(running.config.clone(), options)
            .await
            .is_err()
    );
    let (fresh, head) = history_fixture();
    let options = history_options(&fresh, fresh._temporary.path().join("identity-state"), head);
    let valid = import_history(running.config.clone(), options.clone())
        .await
        .unwrap();
    let checkpoint = std::fs::read(options.state_dir.join("journal.json")).unwrap();
    let mut changed = running.config.clone();
    changed.token_env = None;
    let error = import_history(changed, options.clone()).await.unwrap_err();
    assert!(error.to_string().contains("identity changed"));
    assert_eq!(
        std::fs::read(options.state_dir.join("journal.json")).unwrap(),
        checkpoint
    );
    assert_eq!(
        import_history(running.config.clone(), options)
            .await
            .unwrap()
            .retention_hold,
        valid.retention_hold
    );
    running.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn history_cancelled_caller_keeps_journal_fence_until_git_child_is_killed_and_reaped() {
    use std::os::unix::fs::PermissionsExt;
    use tinytree_import::history::import_history;
    let (input, head) = history_fixture();
    let running = Running::new().await;
    let marker = input._temporary.path().join("accepted-reader");
    let wrapper = input._temporary.path().join("paused-git");
    fn quote(s: &str) -> String {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
    std::fs::write(&wrapper,format!("#!/bin/sh\n[ -z \"${{TINYTREE_IMPORT_TEST_TOKEN+x}}\" ] || exit 81\nfor argument do\n if [ \"$argument\" = cat-file ]; then\n  printf '%s' \"$$\" > {}\n  kill -STOP \"$$\"\n fi\ndone\nexec {} \"$@\"\n",quote(marker.to_str().unwrap()),quote(git_path().to_str().unwrap()))).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut options = history_options(&input, input._temporary.path().join("cancel-state"), head);
    options.git_program = wrapper;
    options.limits.timeout = std::time::Duration::from_secs(3);
    let config = running.config.clone();
    let accepted = options.clone();
    let caller = tokio::spawn(async move { import_history(config, accepted).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pid: i32 = std::fs::read_to_string(marker).unwrap().parse().unwrap();
    caller.abort();
    let _ = caller.await;
    options.git_program = git_path();
    options.limits.timeout = std::time::Duration::from_secs(30);
    assert!(
        import_history(running.config.clone(), options.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("busy")
    );
    let report = tokio::time::timeout(std::time::Duration::from_secs(6), async {
        loop {
            match import_history(running.config.clone(), options.clone()).await {
                Ok(r) => break r,
                Err(e) if e.to_string().contains("busy") => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await
                }
                Err(e) => panic!("{e:#}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(report.is_current);
    assert!(unsafe { libc::kill(pid, 0) } < 0);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    running.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn history_bounds_include_shared_subtree_depth_and_refuse_partial_or_alternate_sources() {
    use tinytree_import::history::import_history;
    enum Boundary {
        SharedDepth,
        Promisor,
        Alternates,
        Shallow,
        Objects,
        Entries,
        Bytes,
        Memory,
    }
    for boundary in [
        Boundary::SharedDepth,
        Boundary::Promisor,
        Boundary::Alternates,
        Boundary::Shallow,
        Boundary::Objects,
        Boundary::Entries,
        Boundary::Bytes,
        Boundary::Memory,
    ] {
        let input = Input::new("sha1", false);
        let running = Running::new().await;
        let mut options = history_options(
            &input,
            input._temporary.path().join("bounded-history"),
            input.revision.clone(),
        );
        match boundary {
            Boundary::SharedDepth => {
                for relative in ["a/child/grandchild", "b/wrapper/child/grandchild"] {
                    std::fs::create_dir_all(input.work.join(relative)).unwrap();
                    std::fs::write(
                        input.work.join(relative).join("leaf"),
                        b"identical subtree bytes",
                    )
                    .unwrap();
                }
                git(&input.work, &["add", "a", "b"]);
                git(
                    &input.work,
                    &["commit", "-m", "shared subtree at unequal depths"],
                );
                options.revision = git(&input.work, &["rev-parse", "HEAD"]);
                options.limits.max_depth = 4;
            }
            Boundary::Promisor => {
                git(
                    &input.work,
                    &["config", "--local", "remote.origin.promisor", "true"],
                );
            }
            Boundary::Alternates => {
                std::fs::write(
                    input.work.join(".git/objects/info/alternates"),
                    b"/nonexistent/object-store\n",
                )
                .unwrap();
            }
            Boundary::Shallow => {
                std::fs::write(
                    input.work.join(".git/shallow"),
                    format!("{}\n", input.revision),
                )
                .unwrap();
            }
            Boundary::Objects => options.limits.max_objects = 1,
            Boundary::Entries => options.limits.max_entries = 1,
            Boundary::Bytes => options.limits.max_total_bytes = 64,
            Boundary::Memory => options.limits.max_memory_bytes = 1024 * 1024,
        }
        assert!(
            import_history(running.config.clone(), options.clone())
                .await
                .is_err()
        );
        let journal: serde_json::Value =
            serde_json::from_slice(&std::fs::read(options.state_dir.join("journal.json")).unwrap())
                .unwrap();
        assert!(journal["journal"]["hold"].is_null());
        assert!(
            running
                .client()
                .await
                .open(vec!["history/main".into()])
                .await
                .unwrap()
                .references[0]
                .commit
                .is_none()
        );
        running.stop().await;
    }
}

#[cfg(unix)]
mod history_ack_fault {
    use super::*;
    use p::repository_server::Repository;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tinytree_client::protocol as p;
    #[derive(Default)]
    pub struct Faults {
        pub lose_hold_once: AtomicBool,
        pub lose_public_acks: AtomicBool,
        pub public_applied: AtomicBool,
        pub opens: AtomicUsize,
    }
    #[derive(Clone)]
    struct FaultService {
        inner: RepositoryService,
        faults: Arc<Faults>,
    }
    #[tonic::async_trait]
    impl Repository for FaultService {
        async fn get_capabilities(
            &self,
            request: tonic::Request<p::GetCapabilitiesRequest>,
        ) -> Result<tonic::Response<p::Capabilities>, tonic::Status> {
            self.inner.get_capabilities(request).await
        }
        type BatchReadObjectsStream = <RepositoryService as Repository>::BatchReadObjectsStream;
        type ReadFileStream = <RepositoryService as Repository>::ReadFileStream;
        async fn open_repository(
            &self,
            r: tonic::Request<p::OpenRepositoryRequest>,
        ) -> Result<tonic::Response<p::RepositoryView>, tonic::Status> {
            self.faults.opens.fetch_add(1, Ordering::SeqCst);
            self.inner.open_repository(r).await
        }
        async fn get_commit_graph(
            &self,
            r: tonic::Request<p::GetCommitGraphRequest>,
        ) -> Result<tonic::Response<p::CommitGraphPage>, tonic::Status> {
            self.inner.get_commit_graph(r).await
        }
        async fn batch_read_objects(
            &self,
            r: tonic::Request<p::BatchReadObjectsRequest>,
        ) -> Result<tonic::Response<Self::BatchReadObjectsStream>, tonic::Status> {
            self.inner.batch_read_objects(r).await
        }
        async fn find_missing_objects(
            &self,
            r: tonic::Request<p::FindMissingObjectsRequest>,
        ) -> Result<tonic::Response<p::MissingObjects>, tonic::Status> {
            self.inner.find_missing_objects(r).await
        }
        async fn read_file(
            &self,
            r: tonic::Request<p::ReadFileRequest>,
        ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
            self.inner.read_file(r).await
        }
        async fn write_object(
            &self,
            r: tonic::Request<tonic::Streaming<p::WriteFrame>>,
        ) -> Result<tonic::Response<p::WriteReceipt>, tonic::Status> {
            self.inner.write_object(r).await
        }
        async fn publish_reference(
            &self,
            r: tonic::Request<p::PublishReferenceRequest>,
        ) -> Result<tonic::Response<p::Publication>, tonic::Status> {
            let hold = r.get_ref().reference_name.starts_with("_tinytree/hold/");
            let result = self.inner.publish_reference(r).await?;
            if hold && self.faults.lose_hold_once.swap(false, Ordering::SeqCst) {
                return Err(tonic::Status::unavailable("injected lost hold ACK"));
            }
            if !hold && self.faults.lose_public_acks.load(Ordering::SeqCst) {
                self.faults.public_applied.store(true, Ordering::SeqCst);
                return Err(tonic::Status::unavailable("injected lost public ACK"));
            }
            Ok(result)
        }
        async fn get_publication(
            &self,
            r: tonic::Request<p::GetPublicationRequest>,
        ) -> Result<tonic::Response<p::Publication>, tonic::Status> {
            if r.get_ref().idempotency_key.ends_with("_public")
                && self.faults.public_applied.load(Ordering::SeqCst)
                && self.faults.lose_public_acks.load(Ordering::SeqCst)
            {
                return Err(tonic::Status::unavailable("injected receipt outage"));
            }
            self.inner.get_publication(r).await
        }
    }
    pub async fn running(faults: Arc<Faults>) -> Running {
        use tokio_stream::StreamExt;
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path().join("store"), Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let service = RepositoryService::new(
            store.clone(),
            ServiceConfig::default(),
            vec!["example".into()],
            vec![
                Principal::new("fixture", TOKEN, vec!["example".into()], true)
                    .unwrap()
                    .with_retention(true),
            ],
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Config {
            endpoint: format!("http://{}", listener.local_addr().unwrap()),
            repository_id: "example".into(),
            token_env: Some("TINYTREE_IMPORT_TEST_TOKEN".into()),
            ..Default::default()
        };
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    p::repository_server::RepositoryServer::new(FaultService {
                        inner: service,
                        faults,
                    })
                    .accept_compressed(tonic::codec::CompressionEncoding::Zstd),
                )
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener).map(|s| {
                        s.map(|stream| {
                            stream.set_nodelay(true).unwrap();
                            stream
                        })
                    }),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        });
        Running {
            _temporary: temporary,
            store,
            config,
            stop,
            task,
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn history_lost_hold_and_public_ack_recovery_preserves_original_receipt_after_another_writer_advances()
 {
    use std::sync::{Arc, atomic::Ordering};
    use tinytree_import::history::{HistoryProof, import_history, verify_history};
    let faults = Arc::new(history_ack_fault::Faults::default());
    faults.lose_hold_once.store(true, Ordering::SeqCst);
    faults.lose_public_acks.store(true, Ordering::SeqCst);
    let running = history_ack_fault::running(faults.clone()).await;
    let (input, head) = history_fixture();
    let mut options = history_options(&input, input._temporary.path().join("lost-ack-state"), head);
    options.limits.timeout = std::time::Duration::from_secs(3);
    assert!(
        import_history(running.config.clone(), options.clone())
            .await
            .is_err()
    );
    assert!(faults.public_applied.load(Ordering::SeqCst));
    let client = running.client().await;
    let current = client.open(vec!["history/main".into()]).await.unwrap();
    let r = &current.references[0];
    assert_eq!(r.generation, 1);
    let native = r
        .commit
        .as_ref()
        .unwrap()
        .key
        .as_ref()
        .unwrap()
        .native_id
        .clone();
    let native: [u8; 32] = native.try_into().unwrap();
    let m::Object::Commit(mut child) = object(&running.store, m::Kind::Commit, native) else {
        panic!()
    };
    child.parents = vec![m::CommitId(native)];
    child.change_id = m::ChangeId([99; 16]);
    child.description = "writer after lost acknowledgement\n".into();
    let encoded = m::encode(&m::Object::Commit(child), Default::default()).unwrap();
    running
        .store
        .put("example", encoded.descriptor, &encoded.bytes, 0)
        .unwrap();
    running
        .store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "competing-after-lost-ack".into(),
                reference_name: "history/main".into(),
                expected_generation: 1,
                new_commit: Some(m::CommitId(encoded.descriptor.key.id.0)),
                staging_leases: vec![],
            },
            0,
        )
        .unwrap();
    faults.lose_public_acks.store(false, Ordering::SeqCst);
    options.limits.timeout = std::time::Duration::from_secs(30);
    // Resume solely from the already retained and verified provenance, not Git.
    std::fs::remove_dir_all(&input.work).unwrap();
    // Wait for accepted physical work to release its journal fence before
    // constructing a crash-prefix candidate in that private namespace.
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(options.state_dir.join("writer.lock"))
                .unwrap();
            if fs2::FileExt::try_lock_exclusive(&lock).is_ok() {
                drop(lock);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // A torn, unACKed fixed candidate cannot hide or replace the durable intent.
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut partial = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(options.state_dir.join("journal.next"))
        .unwrap();
    partial.write_all(b"{partial candidate").unwrap();
    partial.sync_all().unwrap();
    drop(partial);
    let recovered = tokio::time::timeout(std::time::Duration::from_secs(8), async {
        loop {
            match import_history(running.config.clone(), options.clone()).await {
                Ok(report) => break report,
                Err(e) if e.to_string().contains("busy") => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await
                }
                Err(e) => panic!("{e:#}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(recovered.publication_generation, Some(1));
    assert_eq!(recovered.current_generation, 2);
    assert!(!recovered.is_current);
    assert_eq!(
        recovered.native_head,
        blake3::Hash::from(native).to_hex().to_string()
    );
    assert!(!options.state_dir.join("journal.next").exists());
    let proof = HistoryProof {
        retention_hold: recovered.retention_hold,
        provenance_checkpoint: recovered.provenance_checkpoint,
    };
    assert_eq!(
        verify_history(running.config.clone(), proof, Default::default())
            .await
            .unwrap()
            .publication_generation,
        Some(1)
    );
    let before = faults.opens.load(Ordering::SeqCst);
    for invalid in [
        HistoryProof {
            retention_hold: "x".repeat(1024 * 1024),
            provenance_checkpoint: "0".repeat(64),
        },
        HistoryProof {
            retention_hold: format!("_tinytree/hold/v1/{}/{}", "A".repeat(32), "0".repeat(64)),
            provenance_checkpoint: "0".repeat(64),
        },
    ] {
        assert!(
            verify_history(running.config.clone(), invalid, Default::default())
                .await
                .is_err()
        );
    }
    assert_eq!(
        faults.opens.load(Ordering::SeqCst),
        before,
        "malformed proof reached native RPC"
    );
    running.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn history_abrupt_cli_death_preserves_fence_until_surviving_git_reader_terminates() {
    use std::os::unix::fs::PermissionsExt;
    use tinytree_import::history::import_history;
    let (input, head) = history_fixture();
    let running = Running::new().await;
    let options = history_options(
        &input,
        input._temporary.path().join("process-crash-state"),
        head.clone(),
    );
    let config_file = input._temporary.path().join("process-client.toml");
    std::fs::write(&config_file, toml::to_string(&running.config).unwrap()).unwrap();
    let marker = input._temporary.path().join("surviving-reader");
    let wrapper = input._temporary.path().join("process-git-wrapper");
    fn quote(s: &str) -> String {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
    std::fs::write(&wrapper,format!("#!/bin/sh\nfor argument do\n if [ \"$argument\" = cat-file ]; then\n  printf '%s' \"$$\" > {}\n  kill -STOP \"$$\"\n fi\ndone\nexec {} \"$@\"\n",quote(marker.to_str().unwrap()),quote(git_path().to_str().unwrap()))).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut adapter = tokio::process::Command::new(env!("CARGO_BIN_EXE_tinytree-import"))
        .arg("--history")
        .arg("--state")
        .arg(&options.state_dir)
        .arg("--source-git")
        .arg(&input.work)
        .arg("--revision")
        .arg(&head)
        .arg("--config")
        .arg(&config_file)
        .args([
            "--reference",
            "history/main",
            "--timeout-seconds",
            "30",
            "--git",
        ])
        .arg(&wrapper)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !marker.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pid: i32 = std::fs::read_to_string(marker).unwrap().parse().unwrap();
    struct Terminate(i32);
    impl Drop for Terminate {
        fn drop(&mut self) {
            unsafe {
                libc::kill(self.0, libc::SIGKILL);
            }
        }
    }
    let terminate = Terminate(pid);
    adapter.start_kill().unwrap();
    assert!(!adapter.wait().await.unwrap().success());
    assert!(
        import_history(running.config.clone(), options.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("busy")
    );
    drop(terminate); // The platform reaper owns waiting for the orphan.
    let recovered = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match import_history(running.config.clone(), options.clone()).await {
                Ok(r) => break r,
                Err(e) if e.to_string().contains("busy") => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await
                }
                Err(e) => panic!("{e:#}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(recovered.git_head, head);
    assert!(recovered.is_current);
    running.stop().await;
}
