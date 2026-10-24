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
            vec![Principal::new("fixture", TOKEN, vec!["example".into()], true).unwrap()],
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
