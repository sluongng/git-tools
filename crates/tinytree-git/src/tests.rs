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
    counts: std::sync::Arc<RpcCounts>,
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
        let counts = std::sync::Arc::new(RpcCounts::default());
        let observed = counts.clone();
        let task = tokio::spawn(async move {
            serve_counted(listener, service, observed, stopped).await;
        });
        Self {
            temp,
            store,
            config,
            stop,
            task,
            counts,
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
    // The same subtree first appears shallow, then under an extra directory.
    // A cache hit must account for descendants, not only its current depth.
    let leaf = running.tree(b"bounded\n");
    let subtree = |name: &str, child| {
        m::TreeId(
            running
                .object(m::Object::Tree(m::Tree {
                    entries: vec![m::TreeEntry {
                        name: name.into(),
                        value: m::TreeValue::Tree(child),
                    }],
                }))
                .0,
        )
    };
    let shared = subtree("d", leaf);
    let nested = subtree("c", shared);
    let root = m::TreeId(
        running
            .object(m::Object::Tree(m::Tree {
                entries: vec![
                    m::TreeEntry {
                        name: "a".into(),
                        value: m::TreeValue::Tree(shared),
                    },
                    m::TreeEntry {
                        name: "b".into(),
                        value: m::TreeValue::Tree(nested),
                    },
                ],
            }))
            .0,
    );
    let deeper = running.commit(root, vec![head], vec![], "shared subtree at two depths\n");
    running.publish(deeper, 1);
    let config_file = running.temp.path().join("depth-client.toml");
    std::fs::write(
        &config_file,
        format!(
            "endpoint = {:?}\nrepository_id = \"example\"\ntoken_env = \"TINYTREE_GIT_TEST_TOKEN\"\n",
            running.config.endpoint
        ),
    )
    .unwrap();
    for (name, action, max_depth, allowed) in [
        ("export cached subtree too deep", "export", 3, false),
        ("export cached subtree at boundary", "export", 4, true),
        ("watch cached subtree too deep", "watch", 3, false),
        ("watch cached subtree at boundary", "watch", 4, true),
    ] {
        let depth_options = ExportOptions::new(
            running
                .temp
                .path()
                .join(format!("{action}-depth-{max_depth}-state")),
            running
                .temp
                .path()
                .join(format!("{action}-depth-{max_depth}.git")),
            git_program(),
        );
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_tinytree-git"));
        command
            .arg(action)
            .arg("--config")
            .arg(&config_file)
            .arg("--state")
            .arg(&depth_options.state_dir)
            .arg("--destination-git")
            .arg(&depth_options.destination_git)
            .arg("--git")
            .arg(git_program())
            .arg("--max-depth")
            .arg(max_depth.to_string())
            .args(["--timeout-seconds", "30"]);
        if action == "watch" {
            let watch_dir = running.temp.path().join(format!("depth-{max_depth}-watch"));
            let child = command
                .arg("--watch-state")
                .arg(&watch_dir)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let observed = wait_watch(&watch_dir, |s| {
                if allowed {
                    s.delivered.as_ref().is_some_and(|p| p.generation == 2)
                } else {
                    s.last_error == Some(watch::WatchError::ExportFailed)
                }
            })
            .await;
            if !allowed {
                assert!(observed.delivered.is_none(), "{name}");
            }
            assert_eq!(
                unsafe { libc::kill(child.id().unwrap() as i32, libc::SIGINT) },
                0
            );
            let output =
                tokio::time::timeout(std::time::Duration::from_secs(5), child.wait_with_output())
                    .await
                    .unwrap()
                    .unwrap();
            assert!(
                output.status.success(),
                "{name}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        } else {
            let output = command.output().await.unwrap();
            assert_eq!(
                output.status.success(),
                allowed,
                "{name}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            if !allowed {
                assert!(
                    String::from_utf8_lossy(&output.stderr)
                        .contains("native tree depth limit exceeded"),
                    "{name}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
        if allowed {
            assert_eq!(
                git(
                    &depth_options.destination_git,
                    &["show", "refs/heads/main:a/d/binary"]
                ),
                b"bounded\n"
            );
            assert_eq!(
                git(
                    &depth_options.destination_git,
                    &["show", "refs/heads/main:b/c/d/binary"]
                ),
                b"bounded\n"
            );
        } else {
            assert!(
                status(&depth_options.state_dir)
                    .unwrap()
                    .pending_phase
                    .is_some(),
                "{name}"
            );
            assert!(
                text(
                    &depth_options.destination_git,
                    &["for-each-ref", "refs/heads/main"]
                )
                .is_empty(),
                "{name}: rejected depth must not publish Git ref"
            );
            let envelope: serde_json::Value = serde_json::from_slice(
                &std::fs::read(depth_options.state_dir.join("state.json")).unwrap(),
            )
            .unwrap();
            let hold = envelope["state"]["pending"]["hold"].as_str().unwrap();
            let client = tinytree_client::NativeClient::connect(running.config.clone())
                .await
                .unwrap();
            let retained = client.open(vec![hold.into()]).await.unwrap();
            assert_eq!(
                retained.references[0]
                    .commit
                    .as_ref()
                    .unwrap()
                    .key
                    .as_ref()
                    .unwrap()
                    .native_id,
                deeper.0,
                "{name}: failed export keeps exact native head retained"
            );
        }
    }
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

#[cfg(unix)]
fn watcher_options(running: &Running) -> watch::WatchOptions {
    let mut options =
        watch::WatchOptions::new(running.options(), running.temp.path().join("watch-state"));
    options.poll_interval = std::time::Duration::from_millis(100);
    options.probe_timeout = std::time::Duration::from_millis(500);
    options.max_backoff = std::time::Duration::from_millis(800);
    options.export.limits.timeout = std::time::Duration::from_secs(30);
    options
}
#[cfg(unix)]
async fn wait_watch(
    path: &Path,
    mut predicate: impl FnMut(&watch::WatchStatus) -> bool,
) -> watch::WatchStatus {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Ok(status) = watch::watch_status(path) {
                if predicate(&status) {
                    break status;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "watch state did not converge: {:?}",
            watch::watch_status(path)
        )
    })
}
#[cfg(unix)]
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}
#[cfg(unix)]
#[tokio::test]
async fn watcher_cli_catches_up_without_reconverting_or_fscanning_unchanged_polls_and_stops_cleanly()
 {
    use std::os::unix::fs::PermissionsExt;
    let running = Running::new().await;
    let tree = running.tree(b"watch public bytes\n");
    let first = running.commit(tree, vec![m::CommitId::ROOT], vec![], "watch first\n");
    running.publish(first, 0);
    let options = watcher_options(&running);
    let calls = running.temp.path().join("git-write-calls");
    let wrapper = running.temp.path().join("counting-git");
    std::fs::write(&wrapper,format!("#!/bin/sh\n[ -z \"${{TINYTREE_GIT_TEST_TOKEN+x}}\" ] || exit 81\nfor argument do\n case \"$argument\" in fsck|hash-object) printf '%s\\n' \"$argument\" >> {} ;; esac\ndone\nexec {} \"$@\"\n",shell_quote(calls.to_str().unwrap()),shell_quote(git_program().to_str().unwrap()))).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = running.temp.path().join("watch-client.toml");
    std::fs::write(&config,format!("endpoint = {:?}\nrepository_id = \"example\"\ntoken_env = \"TINYTREE_GIT_TEST_TOKEN\"\n",running.config.endpoint)).unwrap();
    let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_tinytree-git"))
        .arg("watch")
        .arg("--config")
        .arg(&config)
        .arg("--state")
        .arg(&options.export.state_dir)
        .arg("--watch-state")
        .arg(&options.watch_dir)
        .arg("--destination-git")
        .arg(&options.export.destination_git)
        .arg("--git")
        .arg(&wrapper)
        .args([
            "--poll-interval-ms",
            "100",
            "--probe-timeout-ms",
            "500",
            "--max-backoff-ms",
            "800",
            "--timeout-seconds",
            "30",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let initial = wait_watch(&options.watch_dir, |s| {
        s.delivered.as_ref().is_some_and(|p| p.generation == 1)
            && s.lag_at_last_observation == Some(0)
    })
    .await;
    assert_eq!(initial.writer_fence_held, Some(true));
    let writes = std::fs::read(&calls).unwrap();
    let file_reads = running
        .counts
        .files
        .load(std::sync::atomic::Ordering::SeqCst);
    let metadata_reads = running
        .counts
        .metadata
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(!writes.is_empty());
    #[cfg(target_os = "linux")]
    let mut mapping_opens = {
        use std::os::fd::FromRawFd;
        let descriptor = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        assert!(descriptor >= 0);
        let path = std::ffi::CString::new(
            options
                .export
                .state_dir
                .join("state.json")
                .as_os_str()
                .as_encoded_bytes(),
        )
        .unwrap();
        assert!(unsafe { libc::inotify_add_watch(descriptor, path.as_ptr(), libc::IN_OPEN) } >= 0);
        unsafe { std::fs::File::from_raw_fd(descriptor) }
    };
    let idle = wait_watch(&options.watch_dir, |s| {
        s.probes >= initial.probes + 3 && s.lag_at_last_observation == Some(0)
    })
    .await;
    assert_eq!(
        std::fs::read(&calls).unwrap(),
        writes,
        "unchanged polls ran fsck or object conversion"
    );
    assert_eq!(idle.export_attempts, initial.export_attempts);
    #[cfg(target_os = "linux")]
    {
        let mut events = [0u8; 4096];
        let error = std::io::Read::read(&mut mapping_opens, &mut events)
            .expect_err("unchanged polls reopened the full export mapping");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    }

    assert_eq!(
        running
            .counts
            .files
            .load(std::sync::atomic::Ordering::SeqCst),
        file_reads
    );
    assert_eq!(
        running
            .counts
            .metadata
            .load(std::sync::atomic::Ordering::SeqCst),
        metadata_reads
    );
    let second = running.commit(tree, vec![first], vec![], "watch second\n");
    let third = running.commit(tree, vec![second], vec![], "watch third\n");
    running.publish(second, 1);
    running.publish(third, 2);
    let caught = wait_watch(&options.watch_dir, |s| {
        s.delivered.as_ref().is_some_and(|p| p.generation == 3)
            && s.lag_at_last_observation == Some(0)
    })
    .await;
    assert_eq!(
        caught.observed_desired.as_ref().unwrap().native_head,
        Some(state::hex(&third.0))
    );
    assert_eq!(
        text(
            &options.export.destination_git,
            &["rev-list", "--count", "refs/heads/main"]
        ),
        "3"
    );
    git(&options.export.destination_git, &["fsck", "--strict"]);
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_tinytree-git"))
        .arg("watch-status")
        .arg("--watch-state")
        .arg(&options.watch_dir)
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let cli_status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(cli_status["delivered"]["generation"], 3);
    assert_eq!(
        unsafe { libc::kill(child.id().unwrap() as i32, libc::SIGINT) },
        0
    );
    let output = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stopped = watch::watch_status(&options.watch_dir).unwrap();
    assert_eq!(stopped.phase, watch::WatchPhase::Stopped);
    assert_eq!(stopped.writer_fence_held, Some(false));
    assert!(stopped.lag_at_last_observation.is_none());
    running.stop().await;
}

#[cfg(unix)]
async fn restart_running(
    temp: tempfile::TempDir,
    store: Store,
    config: tinytree_client::Config,
) -> Running {
    let address = config.endpoint.strip_prefix("http://").unwrap();
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
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
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let counts = std::sync::Arc::new(RpcCounts::default());
    let observed = counts.clone();
    let task = tokio::spawn(async move {
        serve_counted(listener, service, observed, stopped).await;
    });
    Running {
        temp,
        store,
        config,
        stop,
        task,
        counts,
    }
}
#[cfg(unix)]
#[tokio::test]
async fn watcher_replays_ready_offline_then_release_after_restart_and_never_overwrites_divergent_receiver()
 {
    let running = Running::new().await;
    let tree = running.tree(b"durable pending watch\n");
    let head = running.commit(tree, vec![m::CommitId::ROOT], vec![], "pending watch\n");
    running.publish(head, 0);
    let mut options = watcher_options(&running);
    options.poll_interval = std::time::Duration::from_millis(250);
    options.max_backoff = std::time::Duration::from_secs(1);
    options.probe_timeout = std::time::Duration::from_millis(100);
    options.export.limits.timeout = std::time::Duration::from_secs(1);
    options.export.fault = Some(TestFault::BeforeRef);
    assert!(
        export(running.config.clone(), options.export.clone())
            .await
            .is_err()
    );
    options.export.fault = None;
    let saved: serde_json::Value = serde_json::from_slice(
        &std::fs::read(options.export.state_dir.join("state.json")).unwrap(),
    )
    .unwrap();
    let prepared = saved["state"]["pending"]["git"]
        .as_str()
        .unwrap()
        .to_string();
    let Running {
        temp,
        store,
        config,
        stop,
        task,
        counts: _,
    } = running;
    stop.send(()).unwrap();
    task.await.unwrap();
    let mut handle = watch::start_watch(config.clone(), options.clone()).unwrap();
    let outage = wait_watch(&options.watch_dir, |s| {
        s.phase == watch::WatchPhase::Backoff
            && s.pending_export_phase.as_deref() == Some("release")
    })
    .await;
    assert!(outage.observed_desired.is_none());
    assert!(outage.lag_at_last_observation.is_none());
    assert_eq!(outage.delivered.as_ref().unwrap().git_head, prepared);
    assert_eq!(
        text(
            &options.export.destination_git,
            &["rev-parse", "refs/heads/main"]
        ),
        prepared
    );
    let attempt = outage.export_attempts;
    handle.shutdown().await.unwrap();
    let persisted = watch::watch_status(&options.watch_dir).unwrap();
    assert!(persisted.next_retry_unix_ms.is_some());
    let mut handle = watch::start_watch(config.clone(), options.clone()).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(
        watch::watch_status(&options.watch_dir)
            .unwrap()
            .export_attempts,
        attempt,
        "restart discarded durable backoff"
    );
    let running = restart_running(temp, store, config).await;
    wait_watch(&options.watch_dir, |s| {
        s.pending_export_phase.is_none() && s.lag_at_last_observation == Some(0)
    })
    .await;
    assert!(
        export(running.config.clone(), options.export.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("busy")
    );
    let second = running.commit(tree, vec![head], vec![], "second after outage\n");
    let third = running.commit(tree, vec![second], vec![], "third after outage\n");
    running.publish(second, 1);
    running.publish(third, 2);
    let caught = wait_watch(&options.watch_dir, |s| {
        s.delivered.as_ref().is_some_and(|p| p.generation == 3)
            && s.lag_at_last_observation == Some(0)
    })
    .await;
    git(
        &options.export.destination_git,
        &["update-ref", "refs/heads/main", &prepared],
    );
    let conflict = wait_watch(&options.watch_dir, |s| {
        s.last_error == Some(watch::WatchError::DestinationDiverged)
    })
    .await;
    assert!(conflict.lag_at_last_observation.is_none());
    assert_eq!(
        text(
            &options.export.destination_git,
            &["rev-parse", "refs/heads/main"]
        ),
        prepared
    );
    assert_eq!(conflict.export_attempts, caught.export_attempts);
    let permanent = wait_watch(&options.watch_dir, |s| {
        s.consecutive_failures >= 3 && s.last_error == Some(watch::WatchError::DestinationDiverged)
    })
    .await;
    assert_eq!(permanent.export_attempts, caught.export_attempts);
    assert!(
        permanent
            .next_retry_unix_ms
            .unwrap()
            .saturating_sub(permanent.updated_at_unix_ms)
            <= permanent.max_backoff_ms
    );
    handle.shutdown().await.unwrap();
    // A clock rollback can leave future past-event and retry timestamps. The
    // persisted retry is clamped, never treated as age zero/healthy freshness.
    let path = options.watch_dir.join("watch.json");
    let envelope: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let identity: [u8; 32] =
        serde_json::from_value(envelope["record"]["identity"].clone()).unwrap();
    let mut snapshot: watch::WatchStatus =
        serde_json::from_value(envelope["record"]["status"].clone()).unwrap();
    let future = snapshot.updated_at_unix_ms + 86_400_000;
    snapshot.next_retry_unix_ms = Some(future);
    snapshot.last_full_verification_unix_ms = Some(future);
    snapshot.last_receiver_ref_check_unix_ms = Some(future);
    #[derive(serde::Serialize)]
    struct Record {
        identity: [u8; 32],
        status: watch::WatchStatus,
    }
    let record = Record {
        identity,
        status: snapshot,
    };
    let bytes = serde_json::to_vec(&record).unwrap();
    let rewritten = serde_json::json!({"digest":*blake3::hash(&bytes).as_bytes(),"record":record});
    std::fs::write(&path, serde_json::to_vec(&rewritten).unwrap()).unwrap();
    let skewed = watch::watch_status(&options.watch_dir).unwrap();
    assert!(skewed.clock_skewed);
    assert!(skewed.lag_at_last_observation.is_none());
    let opens = running
        .counts
        .opens
        .load(std::sync::atomic::Ordering::SeqCst);
    let mut restarted = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    let clamped = wait_watch(&options.watch_dir, |s| {
        s.writer_fence_held == Some(true)
            && s.phase == watch::WatchPhase::Backoff
            && s.next_retry_unix_ms
                .is_some_and(|n| n.saturating_sub(s.updated_at_unix_ms) <= s.max_backoff_ms)
    })
    .await;
    assert!(clamped.clock_skewed);
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(
        running
            .counts
            .opens
            .load(std::sync::atomic::Ordering::SeqCst),
        opens,
        "future retry was reset to immediate"
    );
    restarted.shutdown().await.unwrap();
    running.stop().await;
}

#[derive(Default)]
struct RpcCounts {
    force_generation: std::sync::atomic::AtomicU64,
    opens: std::sync::atomic::AtomicU64,
    files: std::sync::atomic::AtomicU64,
    metadata: std::sync::atomic::AtomicU64,
}
#[derive(Clone)]
struct CountedService {
    inner: RepositoryService,
    counts: std::sync::Arc<RpcCounts>,
}
use tinytree_client::protocol as wire;
use wire::repository_server::Repository;
#[tonic::async_trait]
impl Repository for CountedService {
    type BatchReadObjectsStream = <RepositoryService as Repository>::BatchReadObjectsStream;
    type ReadFileStream = <RepositoryService as Repository>::ReadFileStream;
    async fn get_capabilities(
        &self,
        r: tonic::Request<wire::GetCapabilitiesRequest>,
    ) -> Result<tonic::Response<wire::Capabilities>, tonic::Status> {
        self.inner.get_capabilities(r).await
    }
    async fn open_repository(
        &self,
        r: tonic::Request<wire::OpenRepositoryRequest>,
    ) -> Result<tonic::Response<wire::RepositoryView>, tonic::Status> {
        self.counts
            .opens
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut response = self.inner.open_repository(r).await?;
        let forced = self
            .counts
            .force_generation
            .load(std::sync::atomic::Ordering::SeqCst);
        if forced > 0 {
            for reference in &mut response.get_mut().references {
                if reference.name == "main" {
                    reference.generation = forced;
                }
            }
        }
        Ok(response)
    }
    async fn get_commit_graph(
        &self,
        r: tonic::Request<wire::GetCommitGraphRequest>,
    ) -> Result<tonic::Response<wire::CommitGraphPage>, tonic::Status> {
        self.inner.get_commit_graph(r).await
    }
    async fn batch_read_objects(
        &self,
        r: tonic::Request<wire::BatchReadObjectsRequest>,
    ) -> Result<tonic::Response<Self::BatchReadObjectsStream>, tonic::Status> {
        self.counts
            .metadata
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.batch_read_objects(r).await
    }
    async fn find_missing_objects(
        &self,
        r: tonic::Request<wire::FindMissingObjectsRequest>,
    ) -> Result<tonic::Response<wire::MissingObjects>, tonic::Status> {
        self.inner.find_missing_objects(r).await
    }
    async fn read_file(
        &self,
        r: tonic::Request<wire::ReadFileRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        self.counts
            .files
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.read_file(r).await
    }
    async fn write_object(
        &self,
        r: tonic::Request<tonic::Streaming<wire::WriteFrame>>,
    ) -> Result<tonic::Response<wire::WriteReceipt>, tonic::Status> {
        self.inner.write_object(r).await
    }
    async fn publish_reference(
        &self,
        r: tonic::Request<wire::PublishReferenceRequest>,
    ) -> Result<tonic::Response<wire::Publication>, tonic::Status> {
        self.inner.publish_reference(r).await
    }
    async fn get_publication(
        &self,
        r: tonic::Request<wire::GetPublicationRequest>,
    ) -> Result<tonic::Response<wire::Publication>, tonic::Status> {
        self.inner.get_publication(r).await
    }
}
async fn serve_counted(
    listener: tokio::net::TcpListener,
    service: RepositoryService,
    counts: std::sync::Arc<RpcCounts>,
    stopped: tokio::sync::oneshot::Receiver<()>,
) {
    use tokio_stream::StreamExt;
    tonic::transport::Server::builder()
        .layer(service.deadline_layer())
        .add_service(
            wire::repository_server::RepositoryServer::new(CountedService {
                inner: service,
                counts,
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
}

struct Unpause(std::sync::Arc<watch::TestPause>);
impl Drop for Unpause {
    fn drop(&mut self) {
        self.0.unblock();
    }
}
async fn wait_pause(pause: &watch::TestPause) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !pause.entered.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn watcher_startup_drop_and_cancelled_wait_keep_both_fences_until_status_writer_drains() {
    let running = Running::new().await;
    let tree = running.tree(b"startup ownership\n");
    let head = running.commit(tree, vec![m::CommitId::ROOT], vec![], "startup\n");
    running.publish(head, 0);
    let mut options = watcher_options(&running);
    options.poll_interval = std::time::Duration::from_millis(500);
    options.max_backoff = std::time::Duration::from_secs(1);
    let mut too_small = options.clone();
    too_small.export.limits.max_memory_bytes = 1024 * 1024;
    assert!(watch::start_watch(running.config.clone(), too_small).is_err());
    assert_eq!(
        running
            .counts
            .opens
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    let mut overlap = options.clone();
    overlap.watch_dir = options.export.state_dir.clone();
    assert!(watch::start_watch(running.config.clone(), overlap).is_err());
    assert!(!options.watch_dir.exists());
    let pause = watch::TestPause::new(Some(watch::WatchPhase::Starting), true);
    let release = Unpause(pause.clone());
    options.test_pause = Some(pause.clone());
    let mut handle = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    wait_pause(&pause).await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), handle.wait())
            .await
            .is_err()
    );
    assert!(
        export(running.config.clone(), options.export.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("busy")
    );
    let mut contender = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    assert!(
        contender
            .wait()
            .await
            .unwrap_err()
            .to_string()
            .contains("busy")
    );
    drop(handle);
    assert_eq!(
        running
            .counts
            .opens
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    drop(release);
    let stopped = wait_watch(&options.watch_dir, |s| {
        s.phase == watch::WatchPhase::Stopped && s.writer_fence_held == Some(false)
    })
    .await;
    assert_eq!(stopped.export_attempts, 0);
    let before = std::fs::read(options.watch_dir.join("watch.json")).unwrap();
    let mut wrong = running.config.clone();
    wrong.token_env = None;
    let mut changed = watch::start_watch(wrong, options.clone()).unwrap();
    assert!(
        changed
            .wait()
            .await
            .unwrap_err()
            .to_string()
            .contains("identity changed")
    );
    assert_eq!(
        std::fs::read(options.watch_dir.join("watch.json")).unwrap(),
        before
    );
    options.test_pause = None;
    let mut handle = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    wait_watch(&options.watch_dir, |s| s.lag_at_last_observation == Some(0)).await;
    let opens = running
        .counts
        .opens
        .load(std::sync::atomic::Ordering::SeqCst);
    std::fs::create_dir(options.watch_dir.join("watch.next")).unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), handle.wait())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(
        running
            .counts
            .opens
            .load(std::sync::atomic::Ordering::SeqCst),
        opens,
        "status write failure did not stop scheduling"
    );
    assert!(
        watch::watch_status(&options.watch_dir)
            .unwrap()
            .lag_at_last_observation
            .is_none()
    );
    assert!(options.watch_dir.join("watch.next").is_dir());
    running.stop().await;
}

#[tokio::test]
async fn watcher_cheap_ref_timeout_cannot_overlap_or_ack_shutdown_before_accepted_reap_tail_drains()
{
    let running = Running::new().await;
    let tree = running.tree(b"physical tail\n");
    let head = running.commit(tree, vec![m::CommitId::ROOT], vec![], "tail\n");
    running.publish(head, 0);
    let mut options = watcher_options(&running);
    options.probe_timeout = std::time::Duration::from_millis(100);
    let pause = watch::TestPause::new(None, false);
    let release = Unpause(pause.clone());
    options.test_pause = Some(pause.clone());
    let mut handle = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    wait_watch(&options.watch_dir, |s| s.lag_at_last_observation == Some(0)).await;
    pause.arm();
    wait_pause(&pause).await;
    let probes = watch::watch_status(&options.watch_dir).unwrap().probes;
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    assert_eq!(
        watch::watch_status(&options.watch_dir).unwrap().probes,
        probes,
        "new operation overlapped accepted physical tail"
    );
    handle.request_shutdown();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), handle.wait())
            .await
            .is_err()
    );
    assert!(
        export(running.config.clone(), options.export.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("busy")
    );
    drop(release);
    let stopped = tokio::time::timeout(std::time::Duration::from_secs(3), handle.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stopped.probes, probes);
    assert_eq!(stopped.phase, watch::WatchPhase::Stopped);
    running.stop().await;
}

#[tokio::test]
async fn watcher_observation_is_not_selected_job_and_torn_checkpoints_recover_from_exporter_truth()
{
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let running = Running::new().await;
    let tree = running.tree(b"latest-selection bytes\n");
    let first = running.commit(tree, vec![m::CommitId::ROOT], vec![], "selection first\n");
    running.publish(first, 0);
    let mut options = watcher_options(&running);
    options.poll_interval = std::time::Duration::from_secs(1);
    options.max_backoff = std::time::Duration::from_secs(2);
    let marker = running.temp.path().join("selected-version");
    let arm = running.temp.path().join("pause-next-export");
    let wrapper = running.temp.path().join("selection-git");
    std::fs::write(&wrapper,format!("#!/bin/sh\nfor argument do\n if [ \"$argument\" = version ] && [ -f {} ]; then\n  printf '%s' \"$$\" > {}\n  kill -STOP \"$$\"\n fi\ndone\nexec {} \"$@\"\n",shell_quote(arm.to_str().unwrap()),shell_quote(marker.to_str().unwrap()),shell_quote(git_program().to_str().unwrap()))).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    options.export.git_program = wrapper;
    let mut handle = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    wait_watch(&options.watch_dir, |s| s.lag_at_last_observation == Some(0)).await;
    std::fs::write(&arm, b"pause").unwrap();
    let second = running.commit(tree, vec![first], vec![], "observed second\n");
    let third = running.commit(tree, vec![second], vec![], "selected third\n");
    running.publish(second, 1);
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        while !marker.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pid: i32 = std::fs::read_to_string(&marker).unwrap().parse().unwrap();
    struct Kill(i32);
    impl Drop for Kill {
        fn drop(&mut self) {
            if self.0 > 0 {
                unsafe {
                    libc::kill(self.0, libc::SIGKILL);
                }
            }
        }
    }
    let mut kill = Kill(pid);
    let inflight = watch::watch_status(&options.watch_dir).unwrap();
    assert_eq!(inflight.phase, watch::WatchPhase::Exporting);
    assert_eq!(inflight.observed_desired.as_ref().unwrap().generation, 2);
    assert_eq!(inflight.lag_at_last_observation, Some(1));
    running.publish(third, 2);
    std::fs::remove_file(&arm).unwrap();
    assert_eq!(unsafe { libc::kill(pid, libc::SIGCONT) }, 0);
    let selected = wait_watch(&options.watch_dir, |s| {
        s.delivered.as_ref().is_some_and(|d| d.generation == 3) && !s.last_probe_succeeded
    })
    .await;
    kill.0 = 0;
    assert_eq!(selected.observed_desired.as_ref().unwrap().generation, 2);
    assert!(selected.lag_at_last_observation.is_none());
    wait_watch(&options.watch_dir, |s| {
        s.lag_at_last_observation == Some(0)
            && s.observed_desired
                .as_ref()
                .is_some_and(|p| p.generation == 3)
    })
    .await;
    handle.shutdown().await.unwrap();
    let fourth = running.commit(tree, vec![third], vec![], "ACK before watcher save\n");
    running.publish(fourth, 3);
    let delivered = export(running.config.clone(), options.export.clone())
        .await
        .unwrap();
    assert_eq!(delivered.source_generation, 4);
    for path in [
        options.watch_dir.join("watch.next"),
        options.export.state_dir.join("state.next"),
    ] {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        f.write_all(b"{torn unACKed candidate").unwrap();
        f.sync_all().unwrap();
    }
    let mut restarted = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    let recovered = wait_watch(&options.watch_dir, |s| {
        s.delivered.as_ref().is_some_and(|d| d.generation == 4)
            && s.lag_at_last_observation == Some(0)
    })
    .await;
    assert_eq!(
        recovered.delivered.as_ref().unwrap().git_head,
        delivered.git_head
    );
    assert!(!options.watch_dir.join("watch.next").exists());
    assert!(!options.export.state_dir.join("state.next").exists());
    restarted.shutdown().await.unwrap();
    running.stop().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn watcher_high_descriptor_cli_death_preserves_both_original_fences_until_child_termination()
{
    use std::os::unix::fs::PermissionsExt;
    let running = Running::new().await;
    let tree = running.tree(b"high-descriptor ownership\n");
    let head = running.commit(tree, vec![m::CommitId::ROOT], vec![], "high descriptors\n");
    running.publish(head, 0);
    let options = watcher_options(&running);
    let marker = running.temp.path().join("high-fd-child");
    let wrapper = running.temp.path().join("high-fd-git");
    std::fs::write(&wrapper,format!("#!/bin/sh\nfor argument do\n if [ \"$argument\" = hash-object ]; then\n  printf '%s' \"$$\" > {}\n  kill -STOP \"$$\"\n fi\ndone\nexec {} \"$@\"\n",shell_quote(marker.to_str().unwrap()),shell_quote(git_program().to_str().unwrap()))).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = running.temp.path().join("pressure-client.toml");
    std::fs::write(&config,format!("endpoint = {:?}\nrepository_id = \"example\"\ntoken_env = \"TINYTREE_GIT_TEST_TOKEN\"\n",running.config.endpoint)).unwrap();
    let mut process = tokio::process::Command::new(env!("TINYTREE_WATCH_FD_FIXTURE"))
        .arg(env!("CARGO_BIN_EXE_tinytree-git"))
        .arg("watch")
        .arg("--config")
        .arg(&config)
        .arg("--state")
        .arg(&options.export.state_dir)
        .arg("--watch-state")
        .arg(&options.watch_dir)
        .arg("--destination-git")
        .arg(&options.export.destination_git)
        .arg("--git")
        .arg(&wrapper)
        .args([
            "--poll-interval-ms",
            "100",
            "--probe-timeout-ms",
            "500",
            "--max-backoff-ms",
            "800",
            "--timeout-seconds",
            "30",
        ])
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
    struct Kill(i32);
    impl Drop for Kill {
        fn drop(&mut self) {
            unsafe {
                libc::kill(self.0, libc::SIGKILL);
            }
        }
    }
    let kill = Kill(pid);
    let locks = [
        options.watch_dir.join("watch.lock"),
        options.export.state_dir.join("writer.lock"),
    ];
    let mut inherited = vec![];
    for entry in std::fs::read_dir(format!("/proc/{pid}/fd")).unwrap() {
        let entry = entry.unwrap();
        if std::fs::read_link(entry.path())
            .ok()
            .is_some_and(|p| locks.contains(&p))
        {
            inherited.push(
                entry
                    .file_name()
                    .to_string_lossy()
                    .parse::<usize>()
                    .unwrap(),
            );
        }
    }
    assert_eq!(inherited.len(), 2);
    assert!(
        inherited.iter().all(|fd| *fd > 256),
        "pressure did not cover original high lock descriptors"
    );
    process.start_kill().unwrap();
    assert!(!process.wait().await.unwrap().success());
    assert_eq!(
        watch::watch_status(&options.watch_dir)
            .unwrap()
            .writer_fence_held,
        Some(true)
    );
    let mut contender = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    assert!(
        contender
            .wait()
            .await
            .unwrap_err()
            .to_string()
            .contains("busy")
    );
    assert!(
        export(running.config.clone(), options.export.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("busy")
    );
    drop(kill);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        use std::os::unix::fs::OpenOptionsExt;
        loop {
            let lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(options.export.state_dir.join("writer.lock"))
                .unwrap();
            let free = fs2::FileExt::try_lock_exclusive(&lock).is_ok();
            if free
                && watch::watch_status(&options.watch_dir)
                    .unwrap()
                    .writer_fence_held
                    == Some(false)
            {
                drop(lock);
                break;
            }
            drop(lock);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut recovered = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    wait_watch(&options.watch_dir, |s| s.lag_at_last_observation == Some(0)).await;
    recovered.shutdown().await.unwrap();
    running.stop().await;
}

#[tokio::test]
async fn watcher_preserves_git_on_source_deletion_or_same_head_regression_and_paces_unsupported_pending_work()
 {
    use std::sync::atomic::Ordering;
    let running = Running::new().await;
    let tree = running.tree(b"deletion is not Git permission\n");
    let head = running.commit(tree, vec![m::CommitId::ROOT], vec![], "deletion source\n");
    running.publish(head, 0);
    let options = watcher_options(&running);
    let mut handle = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    let first = wait_watch(&options.watch_dir, |s| s.lag_at_last_observation == Some(0)).await;
    let git_head = first.delivered.as_ref().unwrap().git_head.clone();
    running
        .store
        .publish(
            "example",
            &PublishRequest {
                idempotency_key: "delete-source-ref".into(),
                reference_name: "main".into(),
                expected_generation: 1,
                new_commit: None,
                staging_leases: vec![],
            },
            0,
        )
        .unwrap();
    let absent = wait_watch(&options.watch_dir, |s| {
        s.last_error == Some(watch::WatchError::SourceAbsent)
    })
    .await;
    assert_eq!(absent.observed_desired.as_ref().unwrap().generation, 2);
    assert!(
        absent
            .observed_desired
            .as_ref()
            .unwrap()
            .native_head
            .is_none()
    );
    assert!(absent.lag_at_last_observation.is_none());
    assert_eq!(absent.export_attempts, first.export_attempts);
    assert_eq!(
        text(
            &options.export.destination_git,
            &["rev-parse", "refs/heads/main"]
        ),
        git_head
    );
    running.publish(head, 2);
    let recreated = wait_watch(&options.watch_dir, |s| {
        s.delivered.as_ref().is_some_and(|d| d.generation == 3)
            && s.lag_at_last_observation == Some(0)
    })
    .await;
    assert_eq!(recreated.delivered.as_ref().unwrap().git_head, git_head);
    running.counts.force_generation.store(1, Ordering::SeqCst);
    let regressed = wait_watch(&options.watch_dir, |s| {
        s.last_error == Some(watch::WatchError::SourceRegressed)
    })
    .await;
    assert!(regressed.lag_at_last_observation.is_none());
    assert_eq!(regressed.delivered.as_ref().unwrap().generation, 3);
    assert_eq!(regressed.observed_desired.as_ref().unwrap().generation, 3);
    assert_eq!(regressed.export_attempts, recreated.export_attempts);
    handle.shutdown().await.unwrap();
    running.stop().await;

    let running = Running::new().await;
    let tree = running.tree(b"unsupported signature remains retained\n");
    let unsigned = running.commit(
        tree,
        vec![m::CommitId::ROOT],
        vec![],
        "unsupported source\n",
    );
    let (_, raw) = running
        .store
        .read_metadata(
            "example",
            m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(unsigned.0),
            },
        )
        .unwrap();
    let m::Object::Commit(mut signed) = m::decode(&raw, Default::default()).unwrap() else {
        panic!()
    };
    signed.secure_signature = Some(m::SecureSignature {
        data: m::unsigned_commit_bytes(&signed, Default::default()).unwrap(),
        signature: b"opaque unsupported signature".to_vec(),
    });
    let signed = m::CommitId(running.object(m::Object::Commit(signed)).0);
    running.publish(signed, 0);
    let mut options = watcher_options(&running);
    options.max_backoff = std::time::Duration::from_millis(400);
    let mut handle = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    let failed = wait_watch(&options.watch_dir, |s| {
        s.consecutive_failures >= 3 && s.last_error == Some(watch::WatchError::ExportFailed)
    })
    .await;
    assert_eq!(failed.pending_export_phase.as_deref(), Some("held"));
    assert!(failed.delivered.is_none());
    assert_eq!(failed.probes, 1);
    assert!(
        failed
            .next_retry_unix_ms
            .unwrap()
            .saturating_sub(failed.updated_at_unix_ms)
            <= 400
    );
    assert_eq!(running.counts.files.load(Ordering::SeqCst), 0);
    let envelope: serde_json::Value = serde_json::from_slice(
        &std::fs::read(options.export.state_dir.join("state.json")).unwrap(),
    )
    .unwrap();
    let hold = envelope["state"]["pending"]["hold"]
        .as_str()
        .unwrap()
        .to_string();
    let client = tinytree_client::NativeClient::connect(running.config.clone())
        .await
        .unwrap();
    assert!(
        client.open(vec![hold]).await.unwrap().references[0]
            .commit
            .is_some()
    );
    handle.shutdown().await.unwrap();
    let opens = running.counts.opens.load(Ordering::SeqCst);
    let mut restarted = watch::start_watch(running.config.clone(), options.clone()).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(running.counts.opens.load(Ordering::SeqCst), opens);
    restarted.shutdown().await.unwrap();
    running.stop().await;
}

#[cfg(target_os = "linux")]
fn push_options(r: &Running) -> push::PushOptions {
    let mut o = push::PushOptions::new(
        r.options(),
        r.temp.path().join("push-state"),
        r.temp.path().join("remote.git").to_str().unwrap().into(),
    );
    o.poll_interval = std::time::Duration::from_millis(100);
    o.operation_timeout = std::time::Duration::from_secs(3);
    o.max_backoff = std::time::Duration::from_millis(200);
    o
}
#[cfg(target_os = "linux")]
async fn wait_push(
    path: &Path,
    mut predicate: impl FnMut(&push::PushStatus) -> bool,
) -> push::PushStatus {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Ok(s) = push::push_status(path) {
                if predicate(&s) {
                    break s;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("push did not converge: {:?}", push::push_status(path)))
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn push_outbox_replays_exact_pending_before_latest_delivery_and_only_publishes_one_public_branch()
 {
    use std::os::unix::fs::PermissionsExt;
    let r = Running::new().await;
    let tree = r.tree(b"one-way public backup\n");
    let a = r.commit(tree, vec![m::CommitId::ROOT], vec![], "first backup\n");
    r.publish(a, 0);
    let o = push_options(&r);
    let first = export(r.config.clone(), o.export.clone()).await.unwrap();
    // No remote yet: select and pin A before the outage, independently of export.
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    let pending = wait_push(&o.push_dir, |s| {
        s.pending.is_some() && s.last_error == Some(push::PushError::RemoteUnavailable)
    })
    .await;
    assert_eq!(pending.pending.unwrap().position.git_head, first.git_head);
    let b = r.commit(tree, vec![a], vec![], "second backup\n");
    r.publish(b, 1);
    let second = export(r.config.clone(), o.export.clone()).await.unwrap();
    let coalesced = wait_push(&o.push_dir, |s| {
        s.desired
            .as_ref()
            .is_some_and(|p| p.git_head == second.git_head)
    })
    .await;
    assert_eq!(coalesced.pending.unwrap().position.git_head, first.git_head);
    assert_ne!(coalesced.lag_at_last_observation, Some(0));
    h.shutdown().await.unwrap();
    let remote = r.temp.path().join("remote.git");
    std::fs::create_dir(&remote).unwrap();
    git(&remote, &["init", "--bare", "--template="]);
    git(
        &o.export.destination_git,
        &[
            "push",
            remote.to_str().unwrap(),
            &format!("{}:refs/heads/unrelated", first.git_head),
        ],
    );
    let receipts = r.temp.path().join("received");
    let hook = remote.join("hooks/post-receive");
    std::fs::create_dir_all(hook.parent().unwrap()).unwrap();
    std::fs::write(&hook,format!("#!/bin/sh\nwhile read old new reference; do printf '%s %s\\n' \"$new\" \"$reference\" >> {}; done\n",shell_quote(receipts.to_str().unwrap()))).unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    let delivered = wait_push(&o.push_dir, |s| {
        s.delivered
            .as_ref()
            .is_some_and(|p| p.git_head == second.git_head)
            && s.pending.is_none()
    })
    .await;
    assert_eq!(delivered.lag_at_last_observation, Some(0));
    assert_eq!(
        std::fs::read_to_string(&receipts).unwrap(),
        format!(
            "{} refs/heads/main\n{} refs/heads/main\n",
            first.git_head, second.git_head
        )
    );
    assert_eq!(
        text(&remote, &["for-each-ref", "--format=%(refname)"]),
        "refs/heads/main\nrefs/heads/unrelated"
    );
    assert_eq!(
        text(&remote, &["rev-parse", "refs/heads/unrelated"]),
        first.git_head
    );
    assert_eq!(
        text(&remote, &["rev-parse", "refs/heads/main"]),
        second.git_head
    );
    git(&remote, &["fsck", "--strict"]);
    let clone = r.temp.path().join("fresh-clone");
    git(
        r.temp.path(),
        &[
            "clone",
            "--bare",
            remote.to_str().unwrap(),
            clone.to_str().unwrap(),
        ],
    );
    git(&clone, &["fsck", "--strict"]);
    // A divergent/deleted receiver is observable and never repaired by force.
    git(&remote, &["update-ref", "-d", "refs/heads/main"]);
    let divergence = wait_push(&o.push_dir, |s| {
        s.last_error == Some(push::PushError::RemoteDiverged)
    })
    .await;
    assert_eq!(divergence.delivered.unwrap().git_head, second.git_head);
    assert_eq!(
        text(&remote, &["for-each-ref", "--format=%(refname)"]),
        "refs/heads/unrelated"
    );
    h.shutdown().await.unwrap();
    r.stop().await;
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn push_reconciles_ambiguous_remote_success_without_reselection_and_rejects_changed_binding()
{
    let r = Running::new().await;
    let tree = r.tree(b"ambiguous push\n");
    let a = r.commit(tree, vec![m::CommitId::ROOT], vec![], "ambiguous\n");
    r.publish(a, 0);
    let mut o = push_options(&r);
    let report = export(r.config.clone(), o.export.clone()).await.unwrap();
    let remote = r.temp.path().join("remote.git");
    std::fs::create_dir(&remote).unwrap();
    git(&remote, &["init", "--bare", "--template="]);
    o.after_push = true;
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    let unknown = wait_push(&o.push_dir, |s| {
        s.last_error == Some(push::PushError::PushUnconfirmed)
    })
    .await;
    assert!(unknown.delivered.is_none());
    assert!(unknown.pending.is_some());
    assert_eq!(
        text(&remote, &["rev-parse", "refs/heads/main"]),
        report.git_head
    );
    h.shutdown().await.unwrap();
    o.after_push = false;
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    let ack = wait_push(&o.push_dir, |s| {
        s.delivered.is_some() && s.pending.is_none()
    })
    .await;
    assert_eq!(ack.attempts, 1);
    assert_eq!(ack.delivered.unwrap().git_head, report.git_head);
    h.shutdown().await.unwrap();
    // Crash-equivalent durable ACK: receiver deletion must retain the pin on restart.
    let b = r.commit(tree, vec![a], vec![], "ack cleanup\n");
    r.publish(b, 1);
    let second = export(r.config.clone(), o.export.clone()).await.unwrap();
    o.after_ack = true;
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    wait_push(&o.push_dir, |s| {
        s.pending.as_ref().is_some_and(|p| p.acknowledged)
    })
    .await;
    h.shutdown().await.unwrap();
    let pin = text(
        &o.export.destination_git,
        &[
            "for-each-ref",
            "--format=%(refname)",
            "refs/tinytree-backup/",
        ],
    );
    assert!(!pin.is_empty());
    assert_eq!(
        text(&o.export.destination_git, &["rev-parse", &pin]),
        second.git_head
    );
    git(&remote, &["update-ref", "-d", "refs/heads/main"]);
    o.after_ack = false;
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    wait_push(&o.push_dir, |s| {
        s.last_error == Some(push::PushError::RemoteDiverged)
    })
    .await;
    h.shutdown().await.unwrap();
    assert_eq!(
        text(&o.export.destination_git, &["rev-parse", &pin]),
        second.git_head
    );
    git(
        &remote,
        &["update-ref", "refs/heads/main", &second.git_head],
    );
    // A symbolic alias to the advancing public branch is never accepted as a pin.
    git(
        &o.export.destination_git,
        &["symbolic-ref", &pin, "refs/heads/main"],
    );
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    wait_push(&o.push_dir, |s| {
        s.last_error == Some(push::PushError::PushUnconfirmed)
    })
    .await;
    h.shutdown().await.unwrap();
    assert_eq!(
        text(&o.export.destination_git, &["symbolic-ref", &pin]),
        "refs/heads/main"
    );
    git(
        &o.export.destination_git,
        &["symbolic-ref", "--delete", &pin],
    );
    git(
        &o.export.destination_git,
        &["update-ref", &pin, &second.git_head],
    );
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    wait_push(&o.push_dir, |s| {
        s.delivered
            .as_ref()
            .is_some_and(|p| p.git_head == second.git_head)
            && s.pending.is_none()
    })
    .await;
    h.shutdown().await.unwrap();
    // A valid journal losing DELIVERED is not a fresh successful observation.
    let export_path = o.export.state_dir.join("state.json");
    let exported = std::fs::read(&export_path).unwrap();
    let mut e: serde_json::Value = serde_json::from_slice(&exported).unwrap();
    e["state"]["delivered"] = serde_json::Value::Null;
    // Re-serialize the typed state in canonical field order for its checksum.
    let state: crate::state::State = serde_json::from_value(e["state"].clone()).unwrap();
    e["digest"] =
        serde_json::to_value(*blake3::hash(&serde_json::to_vec(&state).unwrap()).as_bytes())
            .unwrap();
    std::fs::write(&export_path, serde_json::to_vec(&e).unwrap()).unwrap();
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    let absent = wait_push(&o.push_dir, |s| {
        s.last_error == Some(push::PushError::CheckpointUnavailable)
    })
    .await;
    assert!(absent.lag_at_last_observation.is_none());
    h.shutdown().await.unwrap();
    std::fs::write(export_path, exported).unwrap();
    let saved = std::fs::read(o.push_dir.join("push.json")).unwrap();
    o.remote_reference = "refs/heads/other".into();
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    assert!(h.wait().await.is_err());
    assert_eq!(std::fs::read(o.push_dir.join("push.json")).unwrap(), saved);
    o.remote_reference = "refs/heads/main".into();
    #[derive(serde::Serialize)]
    struct SavedPushRecord {
        identity: [u8; 32],
        status: push::PushStatus,
    }
    let mut envelope: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    let mut status: push::PushStatus =
        serde_json::from_value(envelope["record"]["status"].clone()).unwrap();
    status.updated_at_unix_ms = u64::MAX;
    status.lag_at_last_observation = Some(0);
    let record = SavedPushRecord {
        identity: serde_json::from_value(envelope["record"]["identity"].clone()).unwrap(),
        status,
    };
    envelope["digest"] =
        serde_json::to_value(*blake3::hash(&serde_json::to_vec(&record).unwrap()).as_bytes())
            .unwrap();
    envelope["record"] = serde_json::to_value(record).unwrap();
    std::fs::write(
        o.push_dir.join("push.json"),
        serde_json::to_vec(&envelope).unwrap(),
    )
    .unwrap();
    let future = push::push_status(&o.push_dir).unwrap();
    assert!(future.clock_skewed);
    assert!(future.lag_at_last_observation.is_none());
    // Corruption and oversized or linked journals are preserved, never repaired.
    for bytes in [b"invalid checkpoint".to_vec(), vec![b'x'; 65537]] {
        std::fs::write(o.push_dir.join("push.json"), &bytes).unwrap();
        let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
        assert!(h.wait().await.is_err());
        assert_eq!(std::fs::read(o.push_dir.join("push.json")).unwrap(), bytes);
    }
    std::fs::write(o.push_dir.join("push.json"), &saved).unwrap();
    let alias = o.push_dir.join("preserved.json");
    std::fs::rename(o.push_dir.join("push.json"), &alias).unwrap();
    std::os::unix::fs::symlink(&alias, o.push_dir.join("push.json")).unwrap();
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    assert!(h.wait().await.is_err());
    assert_eq!(std::fs::read(&alias).unwrap(), saved);
    assert!(
        std::fs::symlink_metadata(o.push_dir.join("push.json"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    std::fs::remove_file(&alias).unwrap(); // Deliberately broken committed-state link.
    let next = o.push_dir.join("push.next");
    std::fs::write(&next, b"interrupted candidate").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&next, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    assert!(h.wait().await.is_err());
    assert_eq!(std::fs::read(next).unwrap(), b"interrupted candidate");
    r.stop().await;
}

#[cfg(target_os = "linux")]
async fn https_helper(remote: &str) -> i32 {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            for entry in std::fs::read_dir("/proc").unwrap().flatten() {
                let Some(pid) = entry
                    .file_name()
                    .to_str()
                    .and_then(|s| s.parse::<i32>().ok())
                else {
                    continue;
                };
                if let Ok(cmd) = std::fs::read(entry.path().join("cmdline")) {
                    let args = String::from_utf8_lossy(&cmd);
                    if args.contains("remote-https") && args.contains(remote) {
                        return pid;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual declared HTTPS helper did not launch")
}
#[cfg(target_os = "linux")]
fn process_live(pid: i32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .is_some_and(|s| {
            s.rsplit_once(')')
                .is_some_and(|(_, r)| !matches!(r.split_whitespace().next(), Some("Z" | "X")))
        })
}
#[cfg(target_os = "linux")]
fn verify_helper_ownership(pid: i32, lock: &Path) {
    use std::os::unix::fs::MetadataExt;
    let lock_inode = std::fs::metadata(lock).unwrap().ino();
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    let session = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(3)
        .unwrap();
    let links = |pid: &str| {
        std::fs::read_dir(format!("/proc/{pid}/fd"))
            .unwrap()
            .flatten()
            .filter_map(|f| std::fs::read_link(f.path()).ok())
            .collect::<Vec<_>>()
    };
    let helper = links(&pid.to_string());
    let leader = links(session);
    assert!(
        std::fs::read_dir(format!("/proc/{pid}/fd"))
            .unwrap()
            .flatten()
            .any(|f| std::fs::metadata(f.path()).is_ok_and(|m| m.ino() == lock_inode)),
        "actual HTTPS helper dropped inherited fence"
    );
    assert!(
        helper.iter().any(
            |link| link.to_str().is_some_and(|s| s.starts_with("socket:["))
                && leader.contains(link)
        ),
        "actual HTTPS helper dropped inherited ownership endpoint"
    );
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn push_actual_https_helper_keeps_fence_and_endpoint_through_cancellation_and_actor_death_at_high_fds()
 {
    let r = Running::new().await;
    let t = r.tree(b"helper ownership\n");
    let a = r.commit(t, vec![m::CommitId::ROOT], vec![], "ownership\n");
    r.publish(a, 0);
    let mut o = push_options(&r);
    export(r.config.clone(), o.export.clone()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    o.remote = format!("https://{}/backup.git", listener.local_addr().unwrap());
    o.operation_timeout = std::time::Duration::from_secs(10);
    let mut h = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    // TCP is accepted but the loopback fixture deliberately never finishes TLS.
    let (connection, _) =
        tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
    let helper = https_helper(&o.remote).await;
    verify_helper_ownership(helper, &o.push_dir.join("push.lock"));
    let mut competing = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    assert!(competing.wait().await.is_err());
    let stopped = tokio::time::timeout(std::time::Duration::from_secs(5), h.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stopped.phase, push::PushPhase::Stopped);
    assert!(!process_live(helper));
    assert_eq!(
        push::push_status(&o.push_dir).unwrap().writer_fence_held,
        Some(false)
    );
    drop(connection);
    // Restart the real CLI through the existing declared high-descriptor launcher.
    let config = r.temp.path().join("push-client.toml");
    std::fs::write(&config,format!("endpoint = {:?}\nrepository_id = \"example\"\ntoken_env = \"TINYTREE_GIT_TEST_TOKEN\"\n",r.config.endpoint)).unwrap();
    let mut child = tokio::process::Command::new(env!("TINYTREE_WATCH_FD_FIXTURE"))
        .arg(env!("CARGO_BIN_EXE_tinytree-git"))
        .args(["push-watch", "--config"])
        .arg(&config)
        .arg("--state")
        .arg(&o.export.state_dir)
        .arg("--push-state")
        .arg(&o.push_dir)
        .arg("--destination-git")
        .arg(&o.export.destination_git)
        .arg("--remote")
        .arg(&o.remote)
        .arg("--git")
        .arg(git_program())
        .args([
            "--poll-interval-ms",
            "100",
            "--operation-timeout-ms",
            "10000",
            "--max-backoff-ms",
            "200",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let (connection, _) =
        tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
    let helper = https_helper(&o.remote).await;
    verify_helper_ownership(helper, &o.push_dir.join("push.lock"));
    use std::os::fd::{AsRawFd, FromRawFd};
    let descriptor = unsafe { libc::syscall(libc::SYS_pidfd_open, helper, 0) } as i32;
    assert!(descriptor >= 0);
    let helper_identity = unsafe { std::fs::File::from_raw_fd(descriptor) };
    child.start_kill().unwrap();
    child.wait().await.unwrap();
    assert!(process_live(helper));
    assert_eq!(
        push::push_status(&o.push_dir).unwrap().writer_fence_held,
        Some(true)
    );
    let mut competing = push::start_push_watch(r.config.clone(), o.clone()).unwrap();
    assert!(competing.wait().await.is_err());
    // Explicit fixture cleanup targets the exact verified helper, never a guessed group.
    assert_eq!(
        unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                helper_identity.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        },
        0
    );
    drop(connection);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if push::push_status(&o.push_dir).unwrap().writer_fence_held == Some(false) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    r.stop().await;
}
