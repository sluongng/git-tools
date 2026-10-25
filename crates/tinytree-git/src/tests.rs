use super::*;
use std::{
    path::{Path, PathBuf},
    process::Command,
};
use tinytree_model as m;
use tinytree_server::{Principal, RepositoryService, ServiceConfig};
use tinytree_store::{PublishRequest, Store};

fn git_program() -> PathBuf {
    std::env::var_os("GIT_PROXY_TEST_GIT")
        .expect("declared Git tool")
        .into()
}
fn git(dir: &Path, args: &[&str]) -> Vec<u8> {
    let output = Command::new(git_program())
        .arg("-C")
        .arg(dir)
        .args([
            "-c",
            "user.name=Fixture",
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
        "Git fixture: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
fn text(dir: &Path, args: &[&str]) -> String {
    String::from_utf8(git(dir, args)).unwrap().trim_end().into()
}
struct Running {
    temp: tempfile::TempDir,
    store: Store,
    config: tinytree_client::Config,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Running {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("store"), Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let service = RepositoryService::new(
            store.clone(),
            ServiceConfig::default(),
            vec!["example".into()],
            vec![
                Principal::new(
                    "fixture",
                    "public-git-export-loopback-token",
                    vec!["example".into()],
                    true,
                )
                .unwrap()
                .with_retention(true),
            ],
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = tinytree_client::Config {
            endpoint: format!("http://{}", listener.local_addr().unwrap()),
            repository_id: "example".into(),
            token_env: Some("TINYTREE_GIT_TEST_TOKEN".into()),
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
            temp,
            store,
            config,
            stop,
            task,
        }
    }
    fn object(&self, value: m::Object) -> m::ObjectId {
        let encoded = m::encode(&value, Default::default()).unwrap();
        self.store
            .put("example", encoded.descriptor, &encoded.bytes, 0)
            .unwrap();
        encoded.descriptor.key.id
    }
    fn file(&self, bytes: &[u8]) -> m::FileId {
        let descriptor = m::describe_file(bytes);
        self.store.put("example", descriptor, bytes, 0).unwrap();
        m::FileId(descriptor.key.id.0)
    }
    fn tree(&self, bytes: &[u8]) -> m::TreeId {
        let file = self.file(bytes);
        let link = self.object(m::Object::Symlink("binary".into()));
        let tree = m::Tree {
            entries: vec![
                m::TreeEntry {
                    name: "binary".into(),
                    value: m::TreeValue::File {
                        id: file,
                        executable: false,
                        size_bytes: bytes.len() as u64,
                        copy_id: None,
                    },
                },
                m::TreeEntry {
                    name: "executable".into(),
                    value: m::TreeValue::File {
                        id: file,
                        executable: true,
                        size_bytes: bytes.len() as u64,
                        copy_id: None,
                    },
                },
                m::TreeEntry {
                    name: "link".into(),
                    value: m::TreeValue::Symlink {
                        id: m::SymlinkId(link.0),
                        target_bytes: 6,
                    },
                },
            ],
        };
        m::TreeId(self.object(m::Object::Tree(tree)).0)
    }
    fn commit(
        &self,
        tree: m::TreeId,
        parents: Vec<m::CommitId>,
        predecessors: Vec<m::CommitId>,
        message: &str,
    ) -> m::CommitId {
        let signature = m::Signature {
            name: "Export Fixture".into(),
            email: "fixture@example.invalid".into(),
            timestamp: m::Timestamp {
                millis_since_epoch: 1_700_000_000_999,
                tz_offset_minutes: 90,
            },
        };
        let commit = m::Commit {
            parents,
            predecessors,
            root_tree: m::TreeMerge {
                terms: vec![tree],
                labels: vec![String::new()],
            },
            change_id: m::ChangeId([11; 16]),
            description: message.into(),
            author: signature.clone(),
            committer: signature,
            secure_signature: None,
        };
        m::CommitId(self.object(m::Object::Commit(commit)).0)
    }
    fn publish(&self, head: m::CommitId, generation: u64) {
        self.store
            .publish(
                "example",
                &PublishRequest {
                    idempotency_key: format!("fixture-{generation}"),
                    reference_name: "main".into(),
                    expected_generation: generation,
                    new_commit: Some(head),
                    staging_leases: vec![],
                },
                0,
            )
            .unwrap();
    }
    fn options(&self) -> ExportOptions {
        ExportOptions::new(
            self.temp.path().join("adapter"),
            self.temp.path().join("receiver.git"),
            git_program(),
        )
    }
    async fn stop(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap();
    }
}

#[tokio::test]
async fn public_parent_graph_export_is_durable_replayable_and_private_predecessors_never_enter_git()
{
    let running = Running::new().await;
    let private_tree = running.tree(b"PRIVATE PREDECESSOR FILE SENTINEL");
    let private = running.commit(
        private_tree,
        vec![m::CommitId::ROOT],
        vec![],
        "PRIVATE PREDECESSOR MESSAGE SENTINEL",
    );
    let bytes = b"binary\0bytes\xff\n";
    let tree = running.tree(bytes);
    let base = running.commit(
        tree,
        vec![m::CommitId::ROOT],
        vec![private],
        "public base\n",
    );
    let left = running.commit(tree, vec![base], vec![], "public left\n");
    let right = running.commit(tree, vec![base], vec![], "public right\n");
    let merge = running.commit(tree, vec![right, left], vec![private], "public merge\n");
    running.publish(merge, 0);
    let options = running.options();
    let config_file = running.temp.path().join("client.toml");
    std::fs::write(&config_file, format!("endpoint = {:?}\nrepository_id = \"example\"\ntoken_env = \"TINYTREE_GIT_TEST_TOKEN\"\n", running.config.endpoint)).unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_tinytree-git"))
        .arg("export")
        .arg("--config")
        .arg(&config_file)
        .arg("--state")
        .arg(&options.state_dir)
        .arg("--destination-git")
        .arg(&options.destination_git)
        .arg("--git")
        .arg(git_program())
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Report = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report.exported_commits, 4);
    assert!(report.hold_released);
    assert_eq!(report.native_head, state::hex(&merge.0));
    git(&options.destination_git, &["fsck", "--strict"]);
    let content = text(
        &options.destination_git,
        &["cat-file", "commit", &report.git_head],
    );
    assert!(content.contains("author Export Fixture <fixture@example.invalid> 1700000000 +0130\n"));
    assert!(content.ends_with("\n\npublic merge"));
    let state_status = status(&options.state_dir).unwrap();
    assert_eq!(state_status.mapped_commits, 4);
    assert!(state_status.pending_phase.is_none());
    let envelope: serde_json::Value =
        serde_json::from_slice(&std::fs::read(options.state_dir.join("state.json")).unwrap())
            .unwrap();
    // Parent order comes from native parents, not predecessor order or ID sorting.
    let mappings = &envelope["state"]["mappings"];
    let right_git = mappings[state::hex(&right.0)]["git"].as_str().unwrap();
    let left_git = mappings[state::hex(&left.0)]["git"].as_str().unwrap();
    assert!(content.contains(&format!("parent {right_git}\nparent {left_git}\n")));
    let all = text(
        &options.destination_git,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname)",
        ],
    );
    assert_eq!(all.lines().count(), 7); // four commits, one tree, two blobs + private-free content dedup
    for id in all.lines() {
        let kind = text(&options.destination_git, &["cat-file", "-t", id]);
        let payload = git(&options.destination_git, &["cat-file", &kind, id]);
        assert!(!payload.windows(7).any(|w| w == b"PRIVATE"));
    }
    let clone = running.temp.path().join("clone");
    git(
        running.temp.path(),
        &[
            "clone",
            "--branch",
            "main",
            options.destination_git.to_str().unwrap(),
            clone.to_str().unwrap(),
        ],
    );
    assert_eq!(std::fs::read(clone.join("binary")).unwrap(), bytes);
    assert_eq!(
        std::fs::read_link(clone.join("link")).unwrap(),
        PathBuf::from("binary")
    );
    use std::os::unix::fs::PermissionsExt;
    assert_ne!(
        std::fs::metadata(clone.join("executable"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111,
        0
    );
    let replay = export(running.config.clone(), options.clone())
        .await
        .unwrap();
    assert_eq!(replay.git_head, report.git_head);
    assert_eq!(replay.exported_commits, 0);
    git(
        &options.destination_git,
        &["update-ref", "refs/heads/unrelated", &report.git_head],
    );
    let next = running.commit(tree, vec![merge], vec![], "next public\n");
    running.publish(next, 1);
    let appended = export(running.config.clone(), options.clone())
        .await
        .unwrap();
    assert_eq!(appended.exported_commits, 1);
    assert_eq!(
        text(
            &options.destination_git,
            &["rev-parse", "refs/heads/unrelated"]
        ),
        report.git_head
    );
    assert_eq!(
        text(
            &options.destination_git,
            &["rev-parse", &format!("{}^", appended.git_head)]
        ),
        report.git_head
    );
    // Native rewrites cannot overwrite a delivered public-parent branch.
    let rewritten = running.commit(tree, vec![base], vec![next], "rewrite\n");
    running.publish(rewritten, 2);
    assert!(
        export(running.config.clone(), options.clone())
            .await
            .is_err()
    );
    assert_eq!(
        text(&options.destination_git, &["rev-parse", "refs/heads/main"]),
        appended.git_head
    );
    assert_eq!(
        status(&options.state_dir).unwrap().pending_phase.as_deref(),
        Some("held")
    );
    running.stop().await;
}

#[tokio::test]
async fn interruptions_reconcile_exact_objects_ref_and_native_hold_without_duplicate_commits() {
    for fault in [
        TestFault::BeforeObjects,
        TestFault::BeforeRef,
        TestFault::AfterRef,
        TestFault::BeforeRelease,
    ] {
        let running = Running::new().await;
        let tree = running.tree(b"recovery bytes\n");
        let head = running.commit(tree, vec![m::CommitId::ROOT], vec![], "recoverable\n");
        running.publish(head, 0);
        let mut options = running.options();
        options.fault = Some(fault);
        assert!(
            export(running.config.clone(), options.clone())
                .await
                .is_err()
        );
        let pending = status(&options.state_dir).unwrap();
        assert!(pending.pending_phase.is_some());
        let envelope: serde_json::Value =
            serde_json::from_slice(&std::fs::read(options.state_dir.join("state.json")).unwrap())
                .unwrap();
        let hold = envelope["state"]["pending"]["hold"]
            .as_str()
            .unwrap()
            .to_string();
        let client = tinytree_client::NativeClient::connect(running.config.clone())
            .await
            .unwrap();
        assert!(
            client.open(vec![hold.clone()]).await.unwrap().references[0]
                .commit
                .is_some()
        );
        let prepared = envelope["state"]["pending"]["git"]
            .as_str()
            .map(str::to_string);
        // Crash after writing a complete candidate but before its rename ACK.
        // The fixed candidate can be reconciled without growing orphan records.
        std::fs::copy(
            options.state_dir.join("state.json"),
            options.state_dir.join("state.next"),
        )
        .unwrap();
        options.fault = None;
        let delivered = export(running.config.clone(), options.clone())
            .await
            .unwrap();
        if let Some(prepared) = prepared {
            assert_eq!(delivered.git_head, prepared);
        }
        assert!(delivered.hold_released);
        assert!(status(&options.state_dir).unwrap().pending_phase.is_none());
        assert!(
            client.open(vec![hold]).await.unwrap().references[0]
                .commit
                .is_none()
        );
        assert_eq!(
            text(
                &options.destination_git,
                &["rev-list", "--count", "refs/heads/main"]
            ),
            "1"
        );
        assert_eq!(
            export(running.config.clone(), options)
                .await
                .unwrap()
                .git_head,
            delivered.git_head
        );
        running.stop().await;
    }
}

#[tokio::test]
async fn receiver_divergence_and_resource_limits_preserve_pending_native_retention() {
    let running = Running::new().await;
    let tree = running.tree(b"bounded\n");
    let head = running.commit(tree, vec![m::CommitId::ROOT], vec![], "first\n");
    running.publish(head, 0);
    let mut options = running.options();
    options.fault = Some(TestFault::BeforeRef);
    assert!(
        export(running.config.clone(), options.clone())
            .await
            .is_err()
    );
    let envelope: serde_json::Value =
        serde_json::from_slice(&std::fs::read(options.state_dir.join("state.json")).unwrap())
            .unwrap();
    let prepared = envelope["state"]["pending"]["git"].as_str().unwrap();
    // A genuine independently authored receiver commit is not an anchor grant.
    let tree = text(
        &options.destination_git,
        &["rev-parse", &format!("{prepared}^{{tree}}")],
    );
    let divergent = text(
        &options.destination_git,
        &["commit-tree", &tree, "-m", "independent receiver"],
    );
    git(
        &options.destination_git,
        &["update-ref", "refs/heads/main", &divergent],
    );
    options.fault = None;
    assert!(
        export(running.config.clone(), options.clone())
            .await
            .is_err()
    );
    assert!(status(&options.state_dir).unwrap().pending_phase.is_some());
    assert_eq!(
        text(&options.destination_git, &["rev-parse", "refs/heads/main"]),
        divergent
    );
    git(
        &options.destination_git,
        &["symbolic-ref", "refs/heads/main", "refs/heads/other"],
    );
    assert!(
        export(running.config.clone(), options.clone())
            .await
            .is_err()
    );
    let mut bounded = ExportOptions::new(
        running.temp.path().join("bounded-state"),
        running.temp.path().join("bounded.git"),
        git_program(),
    );
    bounded.limits.max_file_bytes = 2;
    assert!(
        export(running.config.clone(), bounded.clone())
            .await
            .is_err()
    );
    assert!(status(&bounded.state_dir).unwrap().pending_phase.is_some());
    running.stop().await;
}

#[tokio::test]
async fn dropping_caller_keeps_physical_writer_locked_until_deadline_kills_and_reaps_git_child() {
    use std::os::unix::fs::PermissionsExt;
    let running = Running::new().await;
    let tree = running.tree(b"accepted physical writer\n");
    let head = running.commit(tree, vec![m::CommitId::ROOT], vec![], "deadline\n");
    running.publish(head, 0);
    let marker = running.temp.path().join("physical-child");
    let wrapper = running.temp.path().join("git-wrapper");
    fn quote(s: &str) -> String {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
    let script = format!(
        "#!/bin/sh\n[ -z \"${{TINYTREE_GIT_TEST_TOKEN+x}}\" ] || exit 81\nfor argument do\n if [ \"$argument\" = hash-object ]; then\n  printf '%s' \"$$\" > {}\n  kill -STOP \"$$\"\n fi\ndone\nexec {} \"$@\"\n",
        quote(marker.to_str().unwrap()),
        quote(git_program().to_str().unwrap())
    );
    std::fs::write(&wrapper, script).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut options = running.options();
    options.git_program = wrapper;
    options.limits.timeout = std::time::Duration::from_secs(3);
    let config = running.config.clone();
    let accepted = options.clone();
    let caller = tokio::spawn(async move { export(config, accepted).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pid: i32 = std::fs::read_to_string(&marker).unwrap().parse().unwrap();
    caller.abort();
    let _ = caller.await;
    assert!(
        state::Resources::open(options.clone()).is_err(),
        "caller cancellation released physical lock"
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let gone = unsafe { libc::kill(pid, 0) } < 0
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
            if gone && state::Resources::open(options.clone()).is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        status(&options.state_dir).unwrap().pending_phase.as_deref(),
        Some("held")
    );
    options.git_program = git_program();
    options.limits.timeout = std::time::Duration::from_secs(30);
    assert!(
        export(running.config.clone(), options)
            .await
            .unwrap()
            .hold_released
    );
    running.stop().await;
}

#[tokio::test]
async fn unsupported_native_signatures_and_conflicted_trees_fail_without_publication() {
    for signed in [true, false] {
        let running = Running::new().await;
        let tree = running.tree(b"unrepresentable metadata\n");
        let original = running.commit(tree, vec![m::CommitId::ROOT], vec![], "unsupported\n");
        let (_, bytes) = running
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
            panic!()
        };
        if signed {
            commit.secure_signature = Some(m::SecureSignature {
                data: m::unsigned_commit_bytes(&commit, Default::default()).unwrap(),
                signature: b"signature".to_vec(),
            });
        } else {
            commit.root_tree = m::TreeMerge {
                terms: vec![tree, tree, tree],
                labels: vec!["left".into(), "base".into(), "right".into()],
            };
        }
        let head = m::CommitId(running.object(m::Object::Commit(commit)).0);
        running.publish(head, 0);
        let options = running.options();
        assert!(
            export(running.config.clone(), options.clone())
                .await
                .is_err()
        );
        assert!(status(&options.state_dir).unwrap().pending_phase.is_some());
        assert_eq!(
            text(
                &options.destination_git,
                &["for-each-ref", "refs/heads/main"]
            ),
            ""
        );
        running.stop().await;
    }
}

#[tokio::test]
async fn unchanged_or_release_resume_cannot_ack_a_missing_or_divergent_receiver() {
    struct Case {
        name: &'static str,
        release: bool,
        diverge: bool,
    }
    for case in [
        Case {
            name: "unchanged missing blob",
            release: false,
            diverge: false,
        },
        Case {
            name: "release missing blob",
            release: true,
            diverge: false,
        },
        Case {
            name: "release diverged",
            release: true,
            diverge: true,
        },
    ] {
        let running = Running::new().await;
        let tree = running.tree(b"reachable receiver bytes\n");
        let head = running.commit(tree, vec![m::CommitId::ROOT], vec![], "connectivity\n");
        running.publish(head, 0);
        let mut options = running.options();
        if case.release {
            options.fault = Some(TestFault::BeforeRelease);
        }
        let result = export(running.config.clone(), options.clone()).await;
        assert_eq!(result.is_err(), case.release, "{}", case.name);
        let delivered = text(&options.destination_git, &["rev-parse", "refs/heads/main"]);
        let checkpoint = std::fs::read(options.state_dir.join("state.json")).unwrap();
        let mut expected = delivered.clone();
        if case.diverge {
            let tree = text(
                &options.destination_git,
                &["rev-parse", &format!("{delivered}^{{tree}}")],
            );
            expected = text(
                &options.destination_git,
                &["commit-tree", &tree, "-m", "independent receiver"],
            );
            git(
                &options.destination_git,
                &["update-ref", "refs/heads/main", &expected],
            );
        } else {
            let blob = text(
                &options.destination_git,
                &["rev-parse", &format!("{delivered}:binary")],
            );
            std::fs::remove_file(
                options
                    .destination_git
                    .join("objects")
                    .join(&blob[..2])
                    .join(&blob[2..]),
            )
            .unwrap();
        }
        options.fault = None;
        assert!(
            export(running.config.clone(), options.clone())
                .await
                .is_err(),
            "{}",
            case.name
        );
        assert_eq!(
            std::fs::read(options.state_dir.join("state.json")).unwrap(),
            checkpoint,
            "{}",
            case.name
        );
        assert_eq!(
            text(&options.destination_git, &["rev-parse", "refs/heads/main"]),
            expected,
            "{}",
            case.name
        );
        if case.release {
            let envelope: serde_json::Value = serde_json::from_slice(&checkpoint).unwrap();
            let hold = envelope["state"]["pending"]["hold"].as_str().unwrap();
            let client = tinytree_client::NativeClient::connect(running.config.clone())
                .await
                .unwrap();
            assert!(
                client.open(vec![hold.into()]).await.unwrap().references[0]
                    .commit
                    .is_some(),
                "{}",
                case.name
            );
        }
        running.stop().await;
    }
}

#[tokio::test]
async fn abrupt_adapter_process_death_keeps_inherited_writer_fence_until_child_termination() {
    use std::os::unix::fs::PermissionsExt;
    let running = Running::new().await;
    let tree = running.tree(b"abrupt-process recovery\n");
    let head = running.commit(tree, vec![m::CommitId::ROOT], vec![], "process crash\n");
    running.publish(head, 0);
    let options = running.options();
    let config_file = running.temp.path().join("client.toml");
    std::fs::write(&config_file,format!("endpoint = {:?}\nrepository_id = \"example\"\ntoken_env = \"TINYTREE_GIT_TEST_TOKEN\"\n",running.config.endpoint)).unwrap();
    let marker = running.temp.path().join("crash-child");
    let wrapper = running.temp.path().join("crash-git-wrapper");
    fn quote(s: &str) -> String {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
    std::fs::write(&wrapper,format!("#!/bin/sh\nfor argument do\n if [ \"$argument\" = hash-object ]; then\n  printf '%s' \"$$\" > {}\n  kill -STOP \"$$\"\n fi\ndone\nexec {} \"$@\"\n",quote(marker.to_str().unwrap()),quote(git_program().to_str().unwrap()))).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut adapter = tokio::process::Command::new(env!("CARGO_BIN_EXE_tinytree-git"))
        .arg("export")
        .arg("--config")
        .arg(&config_file)
        .arg("--state")
        .arg(&options.state_dir)
        .arg("--destination-git")
        .arg(&options.destination_git)
        .arg("--git")
        .arg(&wrapper)
        .arg("--timeout-seconds")
        .arg("30")
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
    let pid: i32 = std::fs::read_to_string(&marker).unwrap().parse().unwrap();
    struct ChildTermination(i32);
    impl Drop for ChildTermination {
        fn drop(&mut self) {
            unsafe {
                libc::kill(self.0, libc::SIGKILL);
            }
        }
    }
    let termination = ChildTermination(pid);
    adapter.start_kill().unwrap();
    assert!(!adapter.wait().await.unwrap().success());
    assert!(
        state::Resources::open(options.clone()).is_err(),
        "process death freed a surviving physical child's fence"
    );
    drop(termination); // The orphan is killed; the platform reaper owns its wait.
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while state::Resources::open(options.clone()).is_err() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        status(&options.state_dir).unwrap().pending_phase.as_deref(),
        Some("held")
    );
    assert!(
        export(running.config.clone(), options)
            .await
            .unwrap()
            .hold_released
    );
    running.stop().await;
}
