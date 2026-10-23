use super::*;
use std::sync::mpsc;
use std::time::Duration;
use tinytree_model::{
    ChangeId, Commit, CopyHistory, CopyId, FileId, Object, Signature, Timestamp, Tree, TreeEntry,
    TreeId, TreeMerge, TreeValue,
};

fn config() -> Config {
    Config {
        staging_ttl_seconds: 10,
        ..Config::default()
    }
}
fn open() -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), config()).unwrap();
    store.create_repository("demo").unwrap();
    (directory, store)
}
fn put_object(store: &Store, object: Object, now: i64) -> Descriptor {
    let encoded = tinytree_model::encode(&object, Limits::default()).unwrap();
    store
        .put("demo", encoded.descriptor, &encoded.bytes, now)
        .unwrap();
    encoded.descriptor
}
fn tree(file: Descriptor, size: u64) -> Tree {
    Tree {
        entries: vec![TreeEntry {
            name: "source.txt".to_owned(),
            value: TreeValue::File {
                id: FileId(file.key.id.0),
                executable: false,
                size_bytes: size,
                copy_id: None,
            },
        }],
    }
}
fn commit(root: TreeId, description: &str) -> Commit {
    let person = Signature {
        name: "Developer".to_owned(),
        email: "author@example.invalid".to_owned(),
        timestamp: Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    Commit {
        parents: vec![CommitId::ROOT],
        predecessors: vec![],
        root_tree: TreeMerge {
            terms: vec![root],
            labels: vec![String::new()],
        },
        change_id: ChangeId([1; 16]),
        description: description.to_owned(),
        author: person.clone(),
        committer: person,
        secure_signature: None,
    }
}
fn fixture(store: &Store, description: &str) -> (Descriptor, Descriptor, Descriptor) {
    let bytes = b"native source\n";
    let file = tinytree_model::describe_file(bytes);
    store.put("demo", file, bytes, 0).unwrap();
    let t = put_object(store, Object::Tree(tree(file, bytes.len() as u64)), 0);
    let c = put_object(
        store,
        Object::Commit(commit(TreeId(t.key.id.0), description)),
        0,
    );
    (file, t, c)
}
fn request(id: &str, generation: u64, target: Option<Descriptor>) -> PublishRequest {
    PublishRequest {
        idempotency_key: id.to_owned(),
        reference_name: "Main/feature".to_owned(),
        expected_generation: generation,
        new_commit: target.map(|d| CommitId(d.key.id.0)),
        staging_leases: vec![],
    }
}

struct BlockedRead {
    bytes: io::Cursor<&'static [u8]>,
    entered: Option<mpsc::Sender<()>>,
    release: mpsc::Receiver<()>,
}
impl Read for BlockedRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(tx) = self.entered.take() {
            tx.send(()).unwrap();
            self.release.recv().unwrap();
        }
        self.bytes.read(buf)
    }
}

#[test]
fn restart_preserves_objects_generations_and_immutable_publication_receipts() {
    let (directory, store) = open();
    let (file, _, c) = fixture(&store, "first");
    let publish = request("publish-1", 0, Some(c));
    let first = store.publish("demo", &publish, 0).unwrap();
    assert_eq!(first.reference.generation, 1);
    drop(store);
    let store = Store::open(directory.path(), config()).unwrap();
    let mut read = store.read("demo", file.key).unwrap();
    let mut bytes = Vec::new();
    read.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"native source\n");
    drop(read);
    assert_eq!(store.publish("demo", &publish, 100).unwrap(), first);
    let deletion = store
        .publish("demo", &request("delete-1", 1, None), 100)
        .unwrap();
    assert_eq!(deletion.reference.generation, 2);
    assert!(deletion.reference.commit.is_none());
    assert_eq!(store.publish("demo", &publish, 100).unwrap(), first);
    assert_eq!(store.publication("demo", "publish-1").unwrap(), Some(first));
    assert!(matches!(
        store.publish("demo", &request("publish-1", 2, None), 100),
        Err(Error::IdempotencyConflict)
    ));
    let recreated = store
        .publish("demo", &request("recreate-1", 2, Some(c)), 100)
        .unwrap();
    assert_eq!(recreated.reference.generation, 3);
    assert!(matches!(
        store.publish("demo", &request("stale-1", 1, Some(c)), 100),
        Err(Error::StaleReference(_))
    ));
}

#[test]
fn incomplete_negative_merge_closure_and_wrong_file_lengths_do_not_publish() {
    let (_directory, store) = open();
    let (file, t, _) = fixture(&store, "base");
    let mut removed = tree(file, file.size_bytes);
    removed.entries[0].name = "removed.txt".to_owned();
    let negative = tinytree_model::encode(&Object::Tree(removed), Limits::default()).unwrap();
    let mut merged = commit(TreeId(t.key.id.0), "conflict");
    merged.root_tree = TreeMerge {
        terms: vec![
            TreeId(t.key.id.0),
            TreeId(negative.descriptor.key.id.0),
            TreeId(t.key.id.0),
        ],
        labels: vec!["left".into(), "base".into(), "right".into()],
    };
    let c = put_object(&store, Object::Commit(merged), 0);
    assert!(matches!(
        store.publish("demo", &request("merge-1", 0, Some(c)), 0),
        Err(Error::MissingObject(_))
    ));
    assert_eq!(
        store.references("demo", &["Main/feature".into()]).unwrap()[0].generation,
        0
    );
    store
        .put("demo", negative.descriptor, &negative.bytes, 0)
        .unwrap();
    store
        .publish("demo", &request("merge-1", 0, Some(c)), 0)
        .unwrap();
    store.gc("demo", 100).unwrap();
    assert!(
        store
            .descriptor("demo", negative.descriptor.key)
            .unwrap()
            .is_some()
    );

    let wrong_tree = put_object(&store, Object::Tree(tree(file, file.size_bytes + 1)), 100);
    let wrong_commit = put_object(
        &store,
        Object::Commit(commit(TreeId(wrong_tree.key.id.0), "bad stat")),
        100,
    );
    assert!(matches!(
        store.publish("demo", &request("bad-length", 1, Some(wrong_commit)), 100),
        Err(Error::Invalid("tree child byte length"))
    ));
    assert_eq!(
        store.references("demo", &["Main/feature".into()]).unwrap()[0].commit,
        Some(c)
    );
}

#[test]
fn leases_protect_streams_and_owner_until_drop_while_expired_unrooted_objects_collect() {
    let (directory, store) = open();
    let d = tinytree_model::describe_file(b"retained");
    let original = store.put("demo", d, b"retained", 100).unwrap();
    let older = store.put("demo", d, b"retained", 0).unwrap();
    assert_eq!(older.expires_at, original.expires_at);
    let mut lease = store.read("demo", d.key).unwrap();
    let root = store.pin_root("demo", d.key).unwrap();
    assert_eq!(root.descriptor(), d);
    assert_eq!(store.gc("demo", 200).unwrap().deleted_objects, 0);
    drop(store);
    assert!(Store::open(directory.path(), config()).is_err());
    let mut bytes = Vec::new();
    lease.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"retained");
    drop(lease);
    assert!(
        Store::open(directory.path(), config()).is_err(),
        "a lightweight root retains the store after the payload reader is gone"
    );
    drop(root);
    let store = Store::open(directory.path(), config()).unwrap();
    let gc = store.gc("demo", 200).unwrap();
    assert_eq!(gc.deleted_objects, 1);
    assert_eq!(gc.deleted_bytes, d.size_bytes);
    assert!(store.descriptor("demo", d.key).unwrap().is_none());
    assert!(
        store
            .descriptor("demo", tinytree_model::empty_tree().descriptor.key)
            .unwrap()
            .is_some()
    );
}

#[test]
fn corrupt_root_metadata_aborts_gc_without_deleting_other_objects() {
    let (_directory, store) = open();
    let (_, t, c) = fixture(&store, "root");
    store
        .publish("demo", &request("root-1", 0, Some(c)), 0)
        .unwrap();
    let victim = tinytree_model::describe_file(b"unrooted");
    store.put("demo", victim, b"unrooted", 0).unwrap();
    // Fault injection is below the public API; the asserted failure and data
    // retention are observed through the same public methods used by a server.
    fs::remove_file(store.inner.path("demo", t.key)).unwrap();
    assert!(matches!(
        store.gc("demo", 100),
        Err(Error::Corrupt("indexed payload missing"))
    ));
    assert!(store.descriptor("demo", victim.key).unwrap().is_some());
}

#[test]
fn uploads_reject_hash_truncation_and_interruption_without_publishing_metadata() {
    let (_directory, store) = open();
    let d = tinytree_model::describe_file(b"expected");
    assert!(matches!(
        store.put("demo", d, b"different", 0),
        Err(Error::Invalid(_)) | Err(Error::Model(_))
    ));
    assert!(matches!(
        store.put("demo", d, b"short", 0),
        Err(Error::Model(tinytree_model::Error::SizeMismatch))
    ));
    struct Broken;
    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("injected read failure"))
        }
    }
    assert!(matches!(
        store.put_stream("demo", d, &mut Broken, 0),
        Err(Error::Io(_))
    ));
    assert!(store.descriptor("demo", d.key).unwrap().is_none());
    assert_eq!(
        fs::read_dir(store.inner.root.join("staging"))
            .unwrap()
            .count(),
        0
    );
    assert!(matches!(
        store.put("missing", d, b"expected", 0),
        Err(Error::UnknownRepository)
    ));
    store.create_repository("other").unwrap();
    store.put("demo", d, b"expected", 0).unwrap();
    assert!(store.descriptor("other", d.key).unwrap().is_none());
    assert!(matches!(
        store.read("other", d.key),
        Err(Error::MissingObject(_))
    ));
    for invalid in [
        "../escape",
        "Mixed",
        "a.b",
        "",
        "a\\b",
        "con",
        "prn",
        "aux",
        "nul",
        "com1",
        "lpt9",
    ] {
        assert!(store.create_repository(invalid).is_err());
    }
}

#[test]
fn streaming_upload_does_not_hold_the_publication_gate() {
    let (_directory, store) = open();
    let d = tinytree_model::describe_file(b"streamed");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer = store.clone();
    let upload = std::thread::spawn(move || {
        writer.put_stream(
            "demo",
            d,
            &mut BlockedRead {
                bytes: io::Cursor::new(b"streamed"),
                entered: Some(entered_tx),
                release: release_rx,
            },
            0,
        )
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let probe = store.clone();
    let (probe_tx, probe_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        probe_tx
            .send(probe.references("demo", &["main".into()]))
            .unwrap()
    });
    let available = probe_rx.recv_timeout(Duration::from_secs(2));
    release_tx.send(()).unwrap();
    upload.join().unwrap().unwrap();
    reader.join().unwrap();
    assert!(available.unwrap().unwrap()[0].commit.is_none());
}

#[test]
fn concurrent_reference_updates_have_one_winner_and_no_overwrite() {
    let (_directory, store) = open();
    let (_, _, first) = fixture(&store, "first");
    let (_, _, second) = fixture(&store, "second");
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for (id, target) in [("first", first), ("second", second)] {
        let s = store.clone();
        let b = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            b.wait();
            s.publish("demo", &request(id, 0, Some(target)), 0)
        }));
    }
    barrier.wait();
    let results: Vec<_> = workers.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::StaleReference(_))))
            .count(),
        1
    );
    assert_eq!(
        store.references("demo", &["Main/feature".into()]).unwrap()[0].generation,
        1
    );
}

#[test]
fn repository_quota_rejects_new_objects_but_accepts_verified_duplicates() {
    let directory = tempfile::tempdir().unwrap();
    let mut c = config();
    c.max_objects_per_repository = 2;
    let store = Store::open(directory.path(), c).unwrap();
    store.create_repository("demo").unwrap();
    let first = tinytree_model::describe_file(b"one");
    store.put("demo", first, b"one", 0).unwrap();
    store.put("demo", first, b"one", 1).unwrap();
    let second = tinytree_model::describe_file(b"two");
    assert!(matches!(
        store.put("demo", second, b"two", 0),
        Err(Error::Limit("repository quota"))
    ));
    assert!(store.descriptor("demo", second.key).unwrap().is_none());
}

fn directory_snapshot(root: &Path) -> Vec<(PathBuf, u32, &'static str, Vec<u8>)> {
    fn walk(root: &Path, path: &Path, result: &mut Vec<(PathBuf, u32, &'static str, Vec<u8>)>) {
        let m = fs::symlink_metadata(path).unwrap();
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            m.permissions().mode() & 0o7777
        };
        #[cfg(not(unix))]
        let mode = 0;
        let (kind, bytes) = if m.is_symlink() {
            (
                "link",
                fs::read_link(path)
                    .unwrap()
                    .as_os_str()
                    .as_encoded_bytes()
                    .to_vec(),
            )
        } else if m.is_dir() {
            ("directory", Vec::new())
        } else {
            ("file", fs::read(path).unwrap())
        };
        result.push((
            path.strip_prefix(root).unwrap().to_owned(),
            mode,
            kind,
            bytes,
        ));
        if m.is_dir() {
            let mut entries: Vec<_> = fs::read_dir(path)
                .unwrap()
                .map(|e| e.unwrap().path())
                .collect();
            entries.sort();
            for entry in entries {
                walk(root, &entry, result);
            }
        }
    }
    let mut result = Vec::new();
    walk(root, root, &mut result);
    result
}

#[test]
fn opening_unrelated_or_unrecognized_roots_preserves_contents_and_modes() {
    let directory = tempfile::tempdir().unwrap();
    let before = directory_snapshot(directory.path());
    assert!(Store::open_existing(directory.path(), config()).is_err());
    assert_eq!(directory_snapshot(directory.path()), before);
    assert!(Store::open_existing(directory.path().join("missing/nested"), config()).is_err());
    assert_eq!(directory_snapshot(directory.path()), before);
    fs::write(directory.path().join("sentinel"), b"unrelated data").unwrap();
    let before = directory_snapshot(directory.path());
    assert!(Store::open(directory.path(), config()).is_err());
    assert!(Store::open_existing(directory.path(), config()).is_err());
    assert_eq!(directory_snapshot(directory.path()), before);

    struct Case {
        name: &'static str,
        path: &'static str,
        replacement: Option<&'static [u8]>,
    }
    for case in [
        Case {
            name: "corrupt_marker",
            path: STORE_MARKER,
            replacement: Some(b"not a store"),
        },
        Case {
            name: "missing_marker",
            path: STORE_MARKER,
            replacement: None,
        },
        Case {
            name: "missing_database_version",
            path: "metadata/version",
            replacement: None,
        },
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path(), config()).unwrap();
        store.create_repository("demo").unwrap();
        drop(store);
        match case.replacement {
            Some(bytes) => fs::write(directory.path().join(case.path), bytes).unwrap(),
            None => fs::remove_file(directory.path().join(case.path)).unwrap(),
        }
        let before = directory_snapshot(directory.path());
        assert!(
            Store::open(directory.path(), config()).is_err(),
            "{}",
            case.name
        );
        assert!(Store::open_existing(directory.path(), config()).is_err());
        assert_eq!(
            directory_snapshot(directory.path()),
            before,
            "{}",
            case.name
        );
    }
}

#[test]
fn opening_live_owner_does_not_change_store_contents_or_mode() {
    let (directory, _store) = open();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o750)).unwrap();
    }
    let before = directory_snapshot(directory.path());
    assert!(Store::open(directory.path(), config()).is_err());
    assert_eq!(directory_snapshot(directory.path()), before);
}

#[test]
#[cfg(unix)]
fn opening_a_symlink_root_does_not_follow_or_modify_its_target() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("target");
    fs::create_dir(&target).unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&target, fs::Permissions::from_mode(0o750)).unwrap();
    let link = directory.path().join("store-link");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let before = directory_snapshot(directory.path());
    for spelling in ["store-link", "store-link/", "store-link/."] {
        assert!(
            Store::open(directory.path().join(spelling), config()).is_err(),
            "{spelling}"
        );
        assert_eq!(directory_snapshot(directory.path()), before, "{spelling}");
    }
}

#[test]
fn fresh_nested_roots_reopen_with_owned_marker_and_native_data() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("new/parents/native/store");
    let store = Store::open(&root, config()).unwrap();
    store.create_repository("demo").unwrap();
    let (_, _, commit) = fixture(&store, "durable nested root");
    store
        .publish("demo", &request("nested", 0, Some(commit)), 0)
        .unwrap();
    drop(store);
    let store = Store::open_existing(&root, config()).unwrap();
    assert_eq!(
        store.references("demo", &["Main/feature".into()]).unwrap()[0].commit,
        Some(commit)
    );
    assert_eq!(
        fs::read(root.join(STORE_MARKER)).unwrap(),
        STORE_MARKER_BYTES
    );
}

#[test]
fn security_duplicate_edge_chain_exceeds_work_budget_without_publishing() {
    let directory = tempfile::tempdir().unwrap();
    let mut c = config();
    c.max_closure_objects = 16;
    c.max_closure_edges = 100;
    let store = Store::open(directory.path(), c).unwrap();
    store.create_repository("demo").unwrap();
    let empty = tinytree_model::empty_tree().descriptor;
    let mut previous = put_object(
        &store,
        Object::Commit(commit(TreeId(empty.key.id.0), "base")),
        0,
    );
    for depth in 0..4 {
        let mut node = commit(TreeId(empty.key.id.0), &format!("repeated parents {depth}"));
        node.parents = vec![CommitId(previous.key.id.0); 512];
        previous = put_object(&store, Object::Commit(node), 0);
    }
    assert!(matches!(
        store.publish("demo", &request("work-budget", 0, Some(previous)), 0),
        Err(Error::Limit("closure edges"))
    ));
    assert_eq!(
        store.references("demo", &["Main/feature".into()]).unwrap()[0].generation,
        0
    );
    assert!(store.publication("demo", "work-budget").unwrap().is_none());
}

#[test]
fn copy_provenance_is_checked_for_reused_subtrees_and_negative_merge_terms() {
    let (_directory, store) = open();
    let payload = b"copied";
    let file = tinytree_model::describe_file(payload);
    store.put("demo", file, payload, 0).unwrap();
    let history = put_object(
        &store,
        Object::CopyHistory(CopyHistory {
            current_path: "left/source.txt".into(),
            parents: vec![],
            salt: vec![1],
        }),
        0,
    );
    let mut copied = tree(file, file.size_bytes);
    if let TreeValue::File { copy_id, .. } = &mut copied.entries[0].value {
        *copy_id = Some(CopyId(history.key.id.0));
    }
    let subtree = put_object(&store, Object::Tree(copied), 0);
    let child = |name: &str| TreeEntry {
        name: name.into(),
        value: TreeValue::Tree(TreeId(subtree.key.id.0)),
    };
    let left = put_object(
        &store,
        Object::Tree(Tree {
            entries: vec![child("left")],
        }),
        0,
    );
    let right = put_object(
        &store,
        Object::Tree(Tree {
            entries: vec![child("right")],
        }),
        0,
    );
    let reused = put_object(
        &store,
        Object::Tree(Tree {
            entries: vec![child("left"), child("right")],
        }),
        0,
    );
    let valid = put_object(
        &store,
        Object::Commit(commit(TreeId(left.key.id.0), "valid provenance")),
        0,
    );
    store
        .publish("demo", &request("valid-copy", 0, Some(valid)), 0)
        .unwrap();
    let cases = [
        (
            "reused-subtree",
            TreeMerge {
                terms: vec![TreeId(reused.key.id.0)],
                labels: vec![String::new()],
            },
        ),
        (
            "negative-term",
            TreeMerge {
                terms: vec![
                    TreeId(left.key.id.0),
                    TreeId(right.key.id.0),
                    TreeId(left.key.id.0),
                ],
                labels: vec!["left".into(), "base".into(), "right".into()],
            },
        ),
    ];
    for (id, roots) in cases {
        let mut invalid = commit(TreeId(left.key.id.0), id);
        invalid.root_tree = roots;
        let target = put_object(&store, Object::Commit(invalid), 0);
        assert!(
            matches!(
                store.publish("demo", &request(id, 1, Some(target)), 0),
                Err(Error::Invalid("copy history current path"))
            ),
            "{id}"
        );
        assert_eq!(
            store.references("demo", &["Main/feature".into()]).unwrap()[0].commit,
            Some(valid)
        );
    }
    store.gc("demo", 100).unwrap();
    assert!(store.descriptor("demo", history.key).unwrap().is_some());
}

#[test]
fn tree_context_count_and_total_path_bytes_are_bounded() {
    struct Case {
        contexts: usize,
        bytes: usize,
        expected: &'static str,
    }
    for case in [
        Case {
            contexts: 2,
            bytes: 1024,
            expected: "tree path contexts",
        },
        Case {
            contexts: 100,
            bytes: 3,
            expected: "tree path context bytes",
        },
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut c = config();
        c.max_path_contexts = case.contexts;
        c.max_path_context_bytes = case.bytes;
        let store = Store::open(directory.path(), c).unwrap();
        store.create_repository("demo").unwrap();
        let (file, _, _) = fixture(&store, "base");
        let child = put_object(&store, Object::Tree(tree(file, file.size_bytes)), 0);
        let root = put_object(
            &store,
            Object::Tree(Tree {
                entries: ["left", "right"]
                    .into_iter()
                    .map(|name| TreeEntry {
                        name: name.into(),
                        value: TreeValue::Tree(TreeId(child.key.id.0)),
                    })
                    .collect(),
            }),
            0,
        );
        let target = put_object(
            &store,
            Object::Commit(commit(TreeId(root.key.id.0), "bounded contexts")),
            0,
        );
        assert!(
            matches!(store.publish("demo", &request("contexts", 0, Some(target)), 0), Err(Error::Limit(reason)) if reason == case.expected)
        );
    }
}

#[test]
fn duplicate_removed_by_gc_rechecks_quota_before_reinstalling_payload() {
    let directory = tempfile::tempdir().unwrap();
    let mut c = config();
    c.max_objects_per_repository = 2;
    let store = Store::open(directory.path(), c).unwrap();
    store.create_repository("demo").unwrap();
    let original = tinytree_model::describe_file(b"one");
    store.put("demo", original, b"one", 0).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer = store.clone();
    let upload = std::thread::spawn(move || {
        writer.put_stream(
            "demo",
            original,
            &mut BlockedRead {
                bytes: io::Cursor::new(b"one"),
                entered: Some(entered_tx),
                release: release_rx,
            },
            0,
        )
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let replacement = tinytree_model::describe_file(b"two");
    let prepared = (|| -> Result<()> {
        store.gc("demo", 100)?;
        store.put("demo", replacement, b"two", 100)?;
        Ok(())
    })();
    release_tx.send(()).unwrap();
    let result = upload.join().unwrap();
    prepared.unwrap();
    assert!(matches!(result, Err(Error::Limit("repository quota"))));
    assert!(store.descriptor("demo", original.key).unwrap().is_none());
    assert!(!store.inner.path("demo", original.key).exists());
    assert!(store.descriptor("demo", replacement.key).unwrap().is_some());
    assert_eq!(
        fs::read_dir(store.inner.root.join("staging"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn object_pages_are_sorted_exclusive_kind_and_repository_scoped() {
    let (_directory, store) = open();
    let mut expected = Vec::new();
    for bytes in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
        let d = tinytree_model::describe_file(bytes);
        store.put("demo", d, bytes, 0).unwrap();
        expected.push(d);
    }
    expected.sort_by_key(|d| d.key.id);
    let first = store.list_objects("demo", Kind::File, None, 2).unwrap();
    assert_eq!(first, expected[..2]);
    let second = store
        .list_objects("demo", Kind::File, Some(first[1].key.id), 2)
        .unwrap();
    assert_eq!(second, expected[2..]);
    assert!(
        store
            .list_objects("demo", Kind::File, Some(second[0].key.id), 2)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.list_objects("demo", Kind::Tree, None, 2).unwrap(),
        vec![tinytree_model::empty_tree().descriptor]
    );
    store.create_repository("other").unwrap();
    assert!(
        store
            .list_objects("other", Kind::File, None, 2)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store.list_objects("missing", Kind::File, None, 0),
        Err(Error::UnknownRepository)
    ));
    for limit in [0, 1025] {
        assert!(matches!(
            store.list_objects("demo", Kind::File, None, limit),
            Err(Error::Invalid("object page limit"))
        ));
    }
}

fn hold_owner(principal: u8, client: u8) -> RetentionOwner {
    RetentionOwner {
        principal: [principal; 32],
        client: [client; 16],
    }
}
fn hold_request(
    owner: RetentionOwner,
    head: Descriptor,
    id: &str,
    generation: u64,
    acquire: bool,
) -> PublishRequest {
    PublishRequest {
        reference_name: retention_reference(owner, CommitId(head.key.id.0)).unwrap(),
        idempotency_key: id.to_owned(),
        expected_generation: generation,
        new_commit: acquire.then_some(CommitId(head.key.id.0)),
        staging_leases: vec![],
    }
}

#[test]
fn private_hold_survives_public_rewrite_restart_and_gc_until_explicit_release() {
    let (directory, store) = open();
    let (file, _, old_head) = fixture(&store, "private retained history");
    let owner = hold_owner(0xab, 0xcd);
    let limits = RetentionLimits::default();
    let acquire = hold_request(owner, old_head, "retain-old", 0, true);
    assert_eq!(
        parse_retention_reference(&acquire.reference_name),
        Some((owner, CommitId(old_head.key.id.0)))
    );
    assert!(parse_retention_reference(&acquire.reference_name.to_uppercase()).is_none());
    assert!(parse_retention_reference(&(acquire.reference_name.clone() + "/extra")).is_none());
    assert!(retention_reference(owner, CommitId::ROOT).is_err());
    assert!(store.publish("demo", &acquire, 0).is_err());
    assert!(
        store
            .publish_retention("demo", hold_owner(1, 2), &acquire, &limits, 0)
            .is_err()
    );
    let mut wrong_target = acquire.clone();
    wrong_target.new_commit = Some(CommitId([7; 32]));
    assert!(
        store
            .publish_retention("demo", owner, &wrong_target, &limits, 0)
            .is_err()
    );
    store
        .publish("demo", &request("public-old", 0, Some(old_head)), 0)
        .unwrap();
    let receipt = store
        .publish_retention("demo", owner, &acquire, &limits, 0)
        .unwrap();
    let noop = hold_request(owner, old_head, "unneeded-new-key", 1, true);
    assert!(
        store
            .publish_retention("demo", owner, &noop, &limits, 0)
            .is_err()
    );
    assert!(
        store
            .publication("demo", "unneeded-new-key")
            .unwrap()
            .is_none()
    );
    let replacement = put_object(
        &store,
        Object::Commit(commit(
            TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
            "public replacement without old bytes",
        )),
        1,
    );
    store
        .publish("demo", &request("public-new", 1, Some(replacement)), 1)
        .unwrap();
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    store.gc("demo", 100).unwrap();
    assert_eq!(
        store.descriptor("demo", old_head.key).unwrap(),
        Some(old_head)
    );
    assert_eq!(store.descriptor("demo", file.key).unwrap(), Some(file));
    let lowered = RetentionLimits {
        max_known_clients_per_repository: 1,
        max_known_clients_per_principal: 1,
        max_root_records_per_repository: 1,
        max_root_records_per_principal: 1,
        max_root_records_per_client: 1,
        max_receipts_per_repository: 1,
        max_receipts_per_principal: 1,
        max_receipts_per_client: 1,
        max_live_roots_per_client: 1,
        max_unique_bytes_per_client: 1,
        max_unique_bytes_per_principal: 1,
        max_quota_scan_roots: 1,
    };
    assert_eq!(
        store
            .publish_retention("demo", owner, &acquire, &lowered, 100)
            .unwrap(),
        receipt
    );
    let release = hold_request(owner, old_head, "release-old", 1, false);
    let released = store
        .publish_retention("demo", owner, &release, &lowered, 101)
        .unwrap();
    assert_eq!(released.reference.generation, 2);
    assert_eq!(released.reference.commit, None);
    store.gc("demo", 200).unwrap();
    assert_eq!(store.descriptor("demo", old_head.key).unwrap(), None);
    assert_eq!(store.descriptor("demo", file.key).unwrap(), None);
    assert_eq!(
        store
            .publish_retention("demo", owner, &acquire, &lowered, 200)
            .unwrap(),
        receipt,
        "a historical receipt is replayable, not a new retention promise"
    );
    assert_eq!(
        store
            .publish_retention("demo", owner, &release, &lowered, 200)
            .unwrap(),
        released
    );
    let stale = hold_request(owner, old_head, "stale-reacquire", 0, true);
    assert!(matches!(
        store.publish_retention("demo", owner, &stale, &limits, 200),
        Err(Error::StaleReference(Reference { generation: 2, .. }))
    ));
}

#[test]
fn retention_quotas_reject_atomically_without_losing_reserved_release_capacity() {
    #[derive(Clone, Copy, Debug)]
    enum Quota {
        RepositoryClients,
        PrincipalClients,
        RepositoryRoots,
        PrincipalRoots,
        ClientRoots,
        LiveClientRoots,
        RepositoryReceipts,
        PrincipalReceipts,
        ClientReceipts,
        ClientBytes,
        PrincipalBytes,
        ScanWork,
    }
    for quota in [
        Quota::RepositoryClients,
        Quota::PrincipalClients,
        Quota::RepositoryRoots,
        Quota::PrincipalRoots,
        Quota::ClientRoots,
        Quota::LiveClientRoots,
        Quota::RepositoryReceipts,
        Quota::PrincipalReceipts,
        Quota::ClientReceipts,
        Quota::ClientBytes,
        Quota::PrincipalBytes,
        Quota::ScanWork,
    ] {
        let (_directory, store) = open();
        let (_, _, first) = fixture(&store, "first");
        let (_, _, next) = fixture(&store, "next");
        let baseline_owner = hold_owner(1, 1);
        let mut candidate_owner = baseline_owner;
        let defaults = RetentionLimits::default();
        store
            .publish_retention(
                "demo",
                baseline_owner,
                &hold_request(baseline_owner, first, "baseline", 0, true),
                &defaults,
                0,
            )
            .unwrap();
        let baseline_bytes: u64 = store
            .inner
            .closure("demo", [first.key], true, false)
            .unwrap()
            .into_iter()
            .map(|key| store.descriptor("demo", key).unwrap().unwrap().size_bytes)
            .sum();
        let mut limited = defaults.clone();
        match quota {
            Quota::RepositoryClients => {
                limited.max_known_clients_per_repository = 1;
                candidate_owner = hold_owner(2, 1);
            }
            Quota::PrincipalClients => {
                limited.max_known_clients_per_principal = 1;
                candidate_owner = hold_owner(1, 2);
            }
            Quota::RepositoryRoots => limited.max_root_records_per_repository = 1,
            Quota::PrincipalRoots => limited.max_root_records_per_principal = 1,
            Quota::ClientRoots => limited.max_root_records_per_client = 1,
            Quota::LiveClientRoots => limited.max_live_roots_per_client = 1,
            Quota::RepositoryReceipts => limited.max_receipts_per_repository = 3,
            Quota::PrincipalReceipts => limited.max_receipts_per_principal = 3,
            Quota::ClientReceipts => limited.max_receipts_per_client = 3,
            Quota::ClientBytes => limited.max_unique_bytes_per_client = baseline_bytes,
            Quota::PrincipalBytes => limited.max_unique_bytes_per_principal = baseline_bytes,
            Quota::ScanWork => {
                store
                    .publish_retention(
                        "demo",
                        baseline_owner,
                        &hold_request(baseline_owner, next, "second", 0, true),
                        &defaults,
                        0,
                    )
                    .unwrap();
                limited.max_quota_scan_roots = 1;
            }
        }
        let candidate = if matches!(quota, Quota::ScanWork) {
            let (_, _, third) = fixture(&store, "third");
            third
        } else {
            next
        };
        let request = hold_request(candidate_owner, candidate, "candidate", 0, true);
        assert!(
            matches!(
                store.publish_retention("demo", candidate_owner, &request, &limited, 0),
                Err(Error::Limit(_))
            ),
            "{quota:?}"
        );
        assert!(
            store.publication("demo", "candidate").unwrap().is_none(),
            "{quota:?}"
        );
        let current = store
            .references("demo", &[request.reference_name.clone()])
            .unwrap()
            .remove(0);
        assert_eq!(current.generation, 0, "{quota:?}");
        store
            .publish_retention("demo", candidate_owner, &request, &defaults, 0)
            .unwrap();
        store
            .publish_retention(
                "demo",
                candidate_owner,
                &hold_request(candidate_owner, candidate, "release-candidate", 1, false),
                &limited,
                0,
            )
            .unwrap();
    }
}

#[test]
fn retention_deduplicates_shared_history_and_persists_receipt_and_tombstone_caps() {
    let (directory, store) = open();
    let (file, tree, first) = fixture(&store, "shared ancestor");
    let mut child = commit(TreeId(tree.key.id.0), "shared descendant");
    child.parents = vec![CommitId(first.key.id.0)];
    let second = put_object(&store, Object::Commit(child), 0);
    let unique_bytes = file.size_bytes + tree.size_bytes + first.size_bytes + second.size_bytes;
    let owner = hold_owner(1, 1);
    let peer = hold_owner(1, 2);
    let limits = RetentionLimits {
        max_unique_bytes_per_client: unique_bytes,
        max_unique_bytes_per_principal: unique_bytes,
        ..RetentionLimits::default()
    };
    for (owner, head, id) in [
        (owner, first, "a"),
        (owner, second, "b"),
        (peer, second, "peer"),
    ] {
        store
            .publish_retention(
                "demo",
                owner,
                &hold_request(owner, head, id, 0, true),
                &limits,
                0,
            )
            .unwrap();
    }
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    let lowered = RetentionLimits {
        max_receipts_per_repository: 1,
        max_receipts_per_principal: 1,
        max_receipts_per_client: 1,
        max_unique_bytes_per_client: 1,
        max_unique_bytes_per_principal: 1,
        ..limits
    };
    store
        .publish_retention(
            "demo",
            peer,
            &hold_request(peer, second, "peer-release", 1, false),
            &lowered,
            0,
        )
        .unwrap();
    drop(store);

    let (_directory, store) = open();
    let (_, _, head) = fixture(&store, "receipt churn");
    let limits = RetentionLimits {
        max_root_records_per_repository: 1,
        max_root_records_per_principal: 1,
        max_root_records_per_client: 1,
        max_known_clients_per_repository: 1,
        max_known_clients_per_principal: 1,
        max_receipts_per_repository: 6,
        max_receipts_per_principal: 6,
        max_receipts_per_client: 6,
        ..RetentionLimits::default()
    };
    for (generation, acquire, key) in [
        (0, true, "acquire-1"),
        (1, false, "release-1"),
        (2, true, "acquire-2"),
        (3, false, "release-2"),
        (4, true, "acquire-3"),
        (5, false, "release-3"),
    ] {
        store
            .publish_retention(
                "demo",
                owner,
                &hold_request(owner, head, key, generation, acquire),
                &limits,
                0,
            )
            .unwrap();
    }
    let blocked = hold_request(owner, head, "cap-exhausted", 6, true);
    assert!(matches!(
        store.publish_retention("demo", owner, &blocked, &limits, 0),
        Err(Error::Limit("retention receipt quota"))
    ));
    assert!(
        store
            .publication("demo", "cap-exhausted")
            .unwrap()
            .is_none()
    );
    let new_client = hold_owner(1, 3);
    assert!(matches!(
        store.publish_retention(
            "demo",
            new_client,
            &hold_request(new_client, head, "new-client-after-release", 0, true),
            &limits,
            0
        ),
        Err(Error::Limit(_))
    ));
    let (_, _, other) = fixture(&store, "different root record");
    assert!(matches!(
        store.publish_retention(
            "demo",
            owner,
            &hold_request(owner, other, "new-root-after-release", 0, true),
            &limits,
            0
        ),
        Err(Error::Limit(_))
    ));
}

#[test]
fn retention_reopen_rejects_exact_accounting_and_history_mismatches_without_repair() {
    enum Corruption {
        Counter {
            scope: &'static str,
            field: &'static str,
            value: u64,
        },
        MissingReceipt,
        ReceiptOwner,
        ExtraCounter,
    }
    for corruption in [
        Corruption::Counter {
            scope: "repository",
            field: "receipts",
            value: 2,
        },
        Corruption::Counter {
            scope: "principal",
            field: "clients",
            value: 0,
        },
        Corruption::Counter {
            scope: "client",
            field: "roots",
            value: 1,
        },
        Corruption::MissingReceipt,
        Corruption::ReceiptOwner,
        Corruption::ExtraCounter,
    ] {
        let (directory, store) = open();
        let owner = hold_owner(1, 2);
        let (_, _, first) = fixture(&store, "first held revision");
        let (_, _, second) = fixture(&store, "second held revision");
        let limits = RetentionLimits::default();
        for request in [
            hold_request(owner, first, "hold-first", 0, true),
            hold_request(owner, first, "release-first", 1, false),
            hold_request(owner, second, "hold-second", 0, true),
        ] {
            store
                .publish_retention("demo", owner, &request, &limits, 0)
                .unwrap();
        }
        let mut tx = store
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        let (changed_key, changed_value) = match corruption {
            Corruption::Counter {
                scope,
                field,
                value,
            } => {
                let suffix = match scope {
                    "repository" => "retention/v1/repository".to_owned(),
                    "principal" => format!("retention/v1/principal/{}", hex(&owner.principal)),
                    "client" => format!(
                        "retention/v1/client/{}/{}",
                        hex(&owner.principal),
                        hex(&owner.client)
                    ),
                    _ => unreachable!(),
                };
                let physical = key("demo", suffix.as_bytes());
                let bytes = tx
                    .get(&store.inner.spaces.meta, &physical)
                    .unwrap()
                    .unwrap();
                let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                json[field] = value.into();
                let changed = serde_json::to_vec(&json).unwrap();
                tx.insert(&store.inner.spaces.meta, &physical, &changed);
                (Some(physical), Some(changed))
            }
            Corruption::MissingReceipt => {
                tx.remove(&store.inner.spaces.receipts, key("demo", b"hold-first"));
                (None, None)
            }
            Corruption::ReceiptOwner => {
                let physical = key("demo", b"hold-first");
                let bytes = tx
                    .get(&store.inner.spaces.receipts, &physical)
                    .unwrap()
                    .unwrap();
                let mut record: ReceiptRecord = decode(&bytes).unwrap();
                record.reference_name =
                    retention_reference(hold_owner(3, 4), CommitId(first.key.id.0)).unwrap();
                tx.insert(
                    &store.inner.spaces.receipts,
                    &physical,
                    encode(&record).unwrap(),
                );
                (None, None)
            }
            Corruption::ExtraCounter => {
                let physical = key("demo", b"retention/v1/principal/unknown");
                let value = br#"{"clients":0,"roots":0,"receipts":0,"live":0}"#.to_vec();
                tx.insert(&store.inner.spaces.meta, &physical, &value);
                (Some(physical), Some(value))
            }
        };
        store.inner.commit(tx).unwrap();
        drop(store);
        assert!(matches!(
            Store::open_existing(directory.path(), config()),
            Err(Error::Corrupt(_))
        ));
        // Inspect the raw engine only to prove failed validation did not reset
        // a plausible-looking counter to its inferred value or delete it.
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .manual_journal_persist(true)
            .open()
            .unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        if let (Some(key), Some(value)) = (changed_key, changed_value) {
            assert_eq!(
                db.read_tx().get(&meta, key).unwrap().unwrap().as_ref(),
                value.as_slice()
            );
        }
    }
}

#[test]
fn retention_reopen_validation_budgets_include_public_records_and_fail_closed() {
    let (directory, store) = open();
    let (_, _, head) = fixture(&store, "public receipts also consume validation work");
    store
        .publish("demo", &request("public", 0, Some(head)), 0)
        .unwrap();
    drop(store);
    let limits = [
        Config {
            max_retention_validation_records: 1,
            ..config()
        },
        Config {
            max_retention_validation_bytes: 1,
            ..config()
        },
    ];
    for config in limits {
        assert!(matches!(
            Store::open_existing(directory.path(), config),
            Err(Error::Limit("retention reopen validation"))
        ));
    }
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store
            .publication("demo", "public")
            .unwrap()
            .unwrap()
            .reference
            .commit,
        Some(head)
    );
}

fn client_collection(store: &Store, now: i64) -> ClientGcRequest {
    ClientGcRequest {
        epoch: store.client_gc_epoch("demo").unwrap(),
        complete_roots: vec![],
        transient_roots: vec![],
        promised_files: vec![],
        now,
        grace_seconds: 0,
        limits: ClientGcLimits::default(),
    }
}

#[test]
fn client_gc_keeps_lazy_canonical_closure_without_hydrating_promised_file() {
    let (_directory, store) = open();
    let file = tinytree_model::describe_file(b"remote bytes not downloaded");
    let t = put_object(&store, Object::Tree(tree(file, file.size_bytes)), 0);
    let c = put_object(
        &store,
        Object::Commit(commit(TreeId(t.key.id.0), "lazy")),
        0,
    );
    let spare = tinytree_model::describe_file(b"unreachable bytes");
    store.put("demo", spare, b"unreachable bytes", 0).unwrap();
    let mut request = client_collection(&store, 100);
    request.complete_roots = vec![
        c.key,
        ObjectKey {
            kind: Kind::Commit,
            id: ObjectId(CommitId::ROOT.0),
        },
    ];
    request.promised_files = vec![file];
    let report = store.client_gc("demo", &request).unwrap();
    assert_eq!(report.deleted_objects, 1);
    assert_eq!(report.deleted_bytes, spare.size_bytes);
    assert_eq!(report.unlinked_payloads, 1);
    assert_eq!(store.descriptor("demo", file.key).unwrap(), None);
    assert!(!store.inner.path("demo", file.key).exists());
    assert_eq!(store.descriptor("demo", t.key).unwrap(), Some(t));
    assert_eq!(store.descriptor("demo", c.key).unwrap(), Some(c));
    assert_eq!(store.descriptor("demo", spare.key).unwrap(), None);
    assert!(!store.inner.path("demo", spare.key).exists());
    // Even an explicit promise is NOT permission to offload reachable residents.
    store
        .put("demo", file, b"remote bytes not downloaded", 100)
        .unwrap();
    request.epoch = store.client_gc_epoch("demo").unwrap();
    request.now = 200;
    assert_eq!(
        store.client_gc("demo", &request).unwrap().deleted_objects,
        0
    );
    assert_eq!(store.descriptor("demo", file.key).unwrap(), Some(file));
}

#[test]
fn client_gc_invalid_complete_graphs_fail_before_any_deletion() {
    enum Failure {
        Missing,
        WrongLength,
        IndexedMissing,
        CorruptMetadata,
        NegativeTerm,
        Predecessor,
    }
    for failure in [
        Failure::Missing,
        Failure::WrongLength,
        Failure::IndexedMissing,
        Failure::CorruptMetadata,
        Failure::NegativeTerm,
        Failure::Predecessor,
    ] {
        let (_directory, store) = open();
        let (file, t, c) = fixture(&store, "invalid");
        let spare = tinytree_model::describe_file(b"must survive invalid mark");
        store
            .put("demo", spare, b"must survive invalid mark", 0)
            .unwrap();
        let mut request = client_collection(&store, 100);
        request.complete_roots.push(c.key);
        match failure {
            Failure::Missing => {
                let absent = tinytree_model::describe_file(b"not resident");
                let t = put_object(&store, Object::Tree(tree(absent, absent.size_bytes)), 0);
                request.complete_roots = vec![t.key];
            }
            Failure::WrongLength => {
                let t = put_object(&store, Object::Tree(tree(file, file.size_bytes + 1)), 0);
                request.complete_roots = vec![t.key];
            }
            Failure::IndexedMissing => {
                fs::remove_file(store.inner.path("demo", file.key)).unwrap();
                request.promised_files = vec![file];
            }
            Failure::CorruptMetadata => {
                fs::write(
                    store.inner.path("demo", t.key),
                    vec![0; t.size_bytes as usize],
                )
                .unwrap();
            }
            Failure::NegativeTerm => {
                let mut broken = commit(TreeId(t.key.id.0), "negative");
                broken.root_tree = TreeMerge {
                    terms: vec![TreeId(t.key.id.0), TreeId([8; 32]), TreeId(t.key.id.0)],
                    labels: vec!["left".into(), "base".into(), "right".into()],
                };
                request.complete_roots = vec![put_object(&store, Object::Commit(broken), 0).key];
            }
            Failure::Predecessor => {
                let mut broken = commit(TreeId(t.key.id.0), "predecessor");
                broken.predecessors = vec![CommitId([9; 32])];
                request.complete_roots = vec![put_object(&store, Object::Commit(broken), 0).key];
            }
        }
        request.epoch = store.client_gc_epoch("demo").unwrap();
        assert!(store.client_gc("demo", &request).is_err());
        assert_eq!(store.descriptor("demo", spare.key).unwrap(), Some(spare));
        assert!(store.inner.path("demo", spare.key).exists());
        assert!(
            store
                .inner
                .db
                .read_tx()
                .prefix(&store.inner.spaces.client_deletions, key("demo", b""))
                .next()
                .is_none()
        );
    }
}

#[test]
fn client_gc_epoch_is_instance_repository_and_duplicate_write_sensitive() {
    let (directory, store) = open();
    let bytes = b"duplicate refreshes age";
    let file = tinytree_model::describe_file(bytes);
    store.put("demo", file, bytes, 0).unwrap();
    let request = client_collection(&store, 100);
    store.put("demo", file, bytes, 90).unwrap();
    assert!(matches!(
        store.client_gc("demo", &request),
        Err(Error::Invalid("stale client GC epoch"))
    ));
    let mut fresh = client_collection(&store, 100);
    fresh.grace_seconds = 20;
    assert_eq!(store.client_gc("demo", &fresh).unwrap().deleted_objects, 0);
    let old_instance = client_collection(&store, 200);
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert!(matches!(
        store.client_gc("demo", &old_instance),
        Err(Error::Invalid("stale client GC epoch"))
    ));
    store.create_repository("other").unwrap();
    assert!(matches!(
        store.client_gc("other", &client_collection(&store, 200)),
        Err(Error::Invalid("stale client GC epoch"))
    ));
    assert_eq!(
        store
            .client_gc("demo", &client_collection(&store, 200))
            .unwrap()
            .deleted_objects,
        1
    );
}

#[test]
fn client_gc_recent_incomplete_graph_and_active_reader_retain_old_descendants() {
    let (_directory, store) = open();
    let (file, t, c) = fixture(&store, "old base");
    let mut unfinished = commit(TreeId([7; 32]), "unfinished write");
    unfinished.parents = vec![CommitId(c.key.id.0)];
    let unfinished = put_object(&store, Object::Commit(unfinished), 95);
    let mut request = client_collection(&store, 100);
    request.grace_seconds = 20;
    assert_eq!(
        store.client_gc("demo", &request).unwrap().deleted_objects,
        0
    );
    for d in [file, t, c, unfinished] {
        assert_eq!(store.descriptor("demo", d.key).unwrap(), Some(d));
    }
    let reader = store.read("demo", c.key).unwrap();
    assert_eq!(
        store
            .client_gc("demo", &client_collection(&store, 200))
            .unwrap()
            .deleted_objects,
        1
    );
    assert_eq!(store.descriptor("demo", file.key).unwrap(), Some(file));
    drop(reader);
    assert_eq!(
        store
            .client_gc("demo", &client_collection(&store, 300))
            .unwrap()
            .deleted_objects,
        3
    );
}

#[test]
fn client_gc_legacy_unknown_ages_stamp_and_retain_before_reclamation() {
    let (directory, store) = open();
    let (_, _, c) = fixture(&store, "legacy");
    // A pre-age-schema store has no age rows. Simulate only that historical
    // metadata state; collection itself uses the public API after reopen.
    let mut tx = store
        .inner
        .db
        .write_tx()
        .durability(Some(PersistMode::SyncAll));
    for item in store
        .inner
        .db
        .read_tx()
        .prefix(&store.inner.spaces.client_ages, key("demo", b""))
    {
        tx.remove(
            &store.inner.spaces.client_ages,
            item.into_inner().unwrap().0,
        );
    }
    store.inner.commit(tx).unwrap();
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    let first = store
        .client_gc("demo", &client_collection(&store, 100))
        .unwrap();
    assert_eq!(first.deleted_objects, 0);
    assert_eq!(first.stamped_unknown_objects, 4);
    assert_eq!(store.descriptor("demo", c.key).unwrap(), Some(c));
    let mut rollback = client_collection(&store, 90);
    rollback.grace_seconds = 0;
    assert_eq!(
        store.client_gc("demo", &rollback).unwrap().deleted_objects,
        0
    );
    assert_eq!(
        store
            .client_gc("demo", &client_collection(&store, 101))
            .unwrap()
            .deleted_objects,
        3
    );
}

#[test]
fn client_gc_direct_tree_root_checks_contextual_copy_provenance() {
    let (_directory, store) = open();
    let file = tinytree_model::describe_file(b"promised copy source");
    let history = put_object(
        &store,
        Object::CopyHistory(CopyHistory {
            current_path: "wrong.txt".into(),
            parents: vec![],
            salt: vec![1],
        }),
        0,
    );
    let mut tree = tree(file, file.size_bytes);
    let TreeValue::File { copy_id, .. } = &mut tree.entries[0].value else {
        unreachable!()
    };
    *copy_id = Some(CopyId(history.key.id.0));
    let root = put_object(&store, Object::Tree(tree), 0);
    let spare = tinytree_model::describe_file(b"copy failure must preserve");
    store
        .put("demo", spare, b"copy failure must preserve", 0)
        .unwrap();
    let mut request = client_collection(&store, 100);
    request.complete_roots = vec![root.key];
    request.promised_files = vec![file];
    assert!(matches!(
        store.client_gc("demo", &request),
        Err(Error::Invalid("copy history current path"))
    ));
    assert_eq!(store.descriptor("demo", spare.key).unwrap(), Some(spare));
}

#[test]
fn client_gc_budget_failure_has_zero_deletions() {
    enum Bound {
        Catalog,
        Marked,
        Edges,
        Metadata,
        Deletions,
        DeleteBytes,
    }
    for bound in [
        Bound::Catalog,
        Bound::Marked,
        Bound::Edges,
        Bound::Metadata,
        Bound::Deletions,
        Bound::DeleteBytes,
    ] {
        let (_directory, store) = open();
        let (file, _, c) = fixture(&store, "budget");
        let mut request = client_collection(&store, 100);
        match bound {
            Bound::Catalog => request.limits.max_catalog_records = 1,
            Bound::Marked => {
                request.complete_roots = vec![c.key];
                request.limits.max_marked_objects = 1;
            }
            Bound::Edges => {
                request.complete_roots = vec![c.key];
                request.limits.max_edges = 1;
            }
            Bound::Metadata => {
                request.complete_roots = vec![c.key];
                request.limits.max_metadata_bytes = 1;
            }
            Bound::Deletions => request.limits.max_deleted_objects = 1,
            Bound::DeleteBytes => request.limits.max_deleted_bytes = 1,
        }
        assert!(matches!(
            store.client_gc("demo", &request),
            Err(Error::Limit(_))
        ));
        assert_eq!(store.descriptor("demo", file.key).unwrap(), Some(file));
        assert_eq!(store.descriptor("demo", c.key).unwrap(), Some(c));
    }
}

// Materialize the exact durable boundary after index removal and before unlink.
// This models that restart state; the process-crash suite separately exercises
// actual journal/process behavior, not machine power-loss semantics.
fn client_gc_pending_unlink(store: &Store, d: Descriptor, remove_index: bool) {
    #[derive(Serialize)]
    struct Pending {
        object: StoredDescriptor,
        revision: u64,
    }
    let _gate = store.inner.lock().unwrap();
    let mut tx = store
        .inner
        .db
        .write_tx()
        .durability(Some(PersistMode::SyncAll));
    let revision = store
        .inner
        .advance_client_revision(&mut tx, "demo")
        .unwrap();
    if remove_index {
        tx.remove(&store.inner.spaces.objects, object_key("demo", d.key));
        tx.remove(&store.inner.spaces.client_ages, object_key("demo", d.key));
        let mut repo = store.inner.repository("demo").unwrap();
        repo.bytes -= d.size_bytes;
        repo.objects -= 1;
        tx.insert(&store.inner.spaces.repos, "demo", encode(&repo).unwrap());
    }
    tx.insert(
        &store.inner.spaces.client_deletions,
        object_key("demo", d.key),
        encode(&Pending {
            object: d.into(),
            revision,
        })
        .unwrap(),
    );
    store.inner.commit(tx).unwrap();
}

#[test]
fn client_gc_restart_recovers_tombstone_but_never_unlinks_reinstalled_index() {
    for reinstall in [false, true] {
        let (directory, store) = open();
        let bytes = b"pending physical unlink";
        let file = tinytree_model::describe_file(bytes);
        store.put("demo", file, bytes, 0).unwrap();
        client_gc_pending_unlink(&store, file, true);
        assert_eq!(store.descriptor("demo", file.key).unwrap(), None);
        drop(store);
        let store = Store::open_existing(directory.path(), config()).unwrap();
        if reinstall {
            store.put("demo", file, bytes, 99).unwrap();
            // A crash may also leave a stale deletion record alongside an
            // installed index. Recovery must cancel it, not unlink the bytes.
            client_gc_pending_unlink(&store, file, false);
        }
        let mut request = client_collection(&store, 100);
        if reinstall {
            request.complete_roots.push(file.key);
        }
        let report = store.client_gc("demo", &request).unwrap();
        assert_eq!(report.deleted_objects, 0);
        assert_eq!(report.unlinked_payloads, u64::from(!reinstall));
        assert_eq!(
            store.descriptor("demo", file.key).unwrap(),
            reinstall.then_some(file)
        );
        assert_eq!(store.inner.path("demo", file.key).exists(), reinstall);
        assert!(
            store
                .inner
                .db
                .read_tx()
                .prefix(&store.inner.spaces.client_deletions, key("demo", b""))
                .next()
                .is_none()
        );
    }
}

#[test]
fn client_gc_recovery_defers_unindexed_leased_candidate_until_reader_drops() {
    let (_directory, store) = open();
    let bytes = b"writer candidate still leased";
    let file = tinytree_model::describe_file(bytes);
    store.put("demo", file, bytes, 0).unwrap();
    let reader = store.read("demo", file.key).unwrap();
    client_gc_pending_unlink(&store, file, true);
    let report = store
        .client_gc("demo", &client_collection(&store, 100))
        .unwrap();
    assert_eq!(report.deferred_deletions, 1);
    assert_eq!(report.unlinked_payloads, 0);
    assert!(store.inner.path("demo", file.key).exists());
    drop(reader);
    let report = store
        .client_gc("demo", &client_collection(&store, 100))
        .unwrap();
    assert_eq!(report.deferred_deletions, 0);
    assert_eq!(report.unlinked_payloads, 1);
    assert!(!store.inner.path("demo", file.key).exists());
}

#[test]
fn client_gc_repeated_edits_prune_expired_staging_with_a_small_scan_budget() {
    let (_directory, store) = open();
    for edit in 0..12 {
        let now = edit * 20;
        let bytes = format!("unique edit {edit}");
        let file = tinytree_model::describe_file(bytes.as_bytes());
        store.put("demo", file, bytes.as_bytes(), now).unwrap();
        let mut request = client_collection(&store, now + 11);
        // Exactly the permanent empty tree, this edit's file and its lease.
        // Without pruning, a second edit already exceeds this scan budget.
        request.limits.max_catalog_records = 3;
        let report = store.client_gc("demo", &request).unwrap();
        assert_eq!(report.expired_leases, 1);
        assert_eq!(report.deleted_objects, 1);
        assert_eq!(store.descriptor("demo", file.key).unwrap(), None);
        assert!(
            store
                .inner
                .db
                .read_tx()
                .prefix(&store.inner.spaces.staging, key("demo", b""))
                .next()
                .is_none()
        );
    }
    let mut request = client_collection(&store, 300);
    request.limits.max_catalog_records = 1;
    let report = store.client_gc("demo", &request).unwrap();
    assert_eq!(report.expired_leases, 0);
    assert_eq!(report.deleted_objects, 0);
}

#[test]
fn packed_repository_reopens_without_changing_publication_or_object_identity() {
    let (directory, store) = open();
    let (file, tree, head) = fixture(&store, "packed head");
    let large_bytes = vec![19; 65 * 1024];
    let large = tinytree_model::describe_file(&large_bytes);
    store.put("demo", large, &large_bytes, 0).unwrap();
    let lease = store.read("demo", file.key).unwrap();
    assert!(matches!(
        store.pack("demo", PackOptions::default()),
        Err(Error::Limit("pack maintenance requires quiescence"))
    ));
    drop(lease);
    let report = store.pack("demo", PackOptions::default()).unwrap();
    assert_eq!(report.packed_objects, 3);
    assert_eq!(report.removed_loose_files, 3);
    assert_eq!(
        report.logical_bytes,
        file.size_bytes + tree.size_bytes + head.size_bytes
    );
    assert_eq!(
        store
            .pack("demo", PackOptions::default())
            .unwrap()
            .packed_objects,
        0
    );
    assert!(store.inner.path("demo", large.key).exists());
    assert!(!store.inner.path("demo", file.key).exists());
    store.put("demo", file, b"native source\n", 2).unwrap();
    assert!(
        !store.inner.path("demo", file.key).exists(),
        "duplicate upload must reuse packed backing"
    );
    let publication = store
        .publish("demo", &request("packed-publication", 0, Some(head)), 3)
        .unwrap();
    drop(store);
    let mut limits = config();
    limits.max_packed_read_memory_bytes = pack::PACK_READ_CHARGE;
    let store = Store::open_existing(directory.path(), limits).unwrap();
    assert_eq!(store.descriptor("demo", head.key).unwrap(), Some(head));
    assert_eq!(
        store.publication("demo", "packed-publication").unwrap(),
        Some(publication)
    );
    let _file_root = store.pin_root("demo", file.key).unwrap();
    let _tree_root = store.pin_root("demo", tree.key).unwrap();
    let mut first = store.read("demo", file.key).unwrap();
    assert!(matches!(
        store.read("demo", tree.key),
        Err(Error::Limit("packed read memory"))
    ));
    let mut contents = Vec::new();
    first.read_to_end(&mut contents).unwrap();
    assert_eq!(contents, b"native source\n");
    drop(first);
    assert!(store.read("demo", tree.key).is_ok());
    let mut actual = Vec::new();
    store
        .read("demo", large.key)
        .unwrap()
        .read_to_end(&mut actual)
        .unwrap();
    assert_eq!(actual, large_bytes);
}

fn related_packed_files(store: &Store) -> (Descriptor, Vec<u8>, Descriptor, PackReport) {
    let mut seed = 0x13579bdfu32;
    let bytes: Vec<u8> = (0..4096)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as u8
        })
        .collect();
    let mut anchor = bytes.clone();
    anchor.push(3);
    let target = tinytree_model::describe_file(&bytes);
    let base = tinytree_model::describe_file(&anchor);
    store.put("demo", target, &bytes, 0).unwrap();
    store.put("demo", base, &anchor, 0).unwrap();
    let report = store.pack("demo", PackOptions::default()).unwrap();
    assert_eq!(report.packed_objects, 2);
    assert_eq!(
        report.delta_entries, 1,
        "related revisions should exercise an actual dictionary dependency"
    );
    (target, bytes, base, report)
}

#[test]
fn packed_gc_preserves_a_live_delta_after_its_logical_anchor_is_deleted() {
    let (_directory, store) = open();
    let (target, bytes, base, report) = related_packed_files(&store);
    let pack_path = store
        .inner
        .root
        .join("repos/demo/packs")
        .join(format!("{}.pack", hex(&report.pack_id.unwrap())));
    let mut lease = store.read("demo", target.key).unwrap();
    let collected = store.gc("demo", 100).unwrap();
    assert_eq!(collected.deleted_objects, 1);
    assert_eq!(store.descriptor("demo", base.key).unwrap(), None);
    assert!(
        pack_path.exists(),
        "physical anchor remains while another member needs it"
    );
    let mut contents = Vec::new();
    lease.read_to_end(&mut contents).unwrap();
    assert_eq!(contents, bytes);
    drop(lease);
    let mut reread = Vec::new();
    store
        .read("demo", target.key)
        .unwrap()
        .read_to_end(&mut reread)
        .unwrap();
    assert_eq!(reread, bytes);
    assert_eq!(store.gc("demo", 100).unwrap().deleted_objects, 1);
    assert!(!pack_path.exists());
    assert!(matches!(
        store.read("demo", target.key),
        Err(Error::MissingObject(_))
    ));
}

#[test]
fn packed_client_gc_reclaims_only_after_the_last_retained_member() {
    let (_directory, store) = open();
    let (target, bytes, base, report) = related_packed_files(&store);
    let pack_path = store
        .inner
        .root
        .join("repos/demo/packs")
        .join(format!("{}.pack", hex(&report.pack_id.unwrap())));
    let mut request = client_collection(&store, 100);
    request.complete_roots = vec![target.key];
    assert_eq!(
        store.client_gc("demo", &request).unwrap().deleted_objects,
        1
    );
    assert_eq!(store.descriptor("demo", base.key).unwrap(), None);
    let mut contents = Vec::new();
    store
        .read("demo", target.key)
        .unwrap()
        .read_to_end(&mut contents)
        .unwrap();
    assert_eq!(contents, bytes);
    assert!(pack_path.exists());
    let request = client_collection(&store, 100);
    assert_eq!(
        store.client_gc("demo", &request).unwrap().deleted_objects,
        1
    );
    assert!(!pack_path.exists());
}

#[test]
fn corrupt_packs_and_inconsistent_feature_metadata_fail_closed() {
    let (directory, store) = open();
    let (target, _, _, report) = related_packed_files(&store);
    let path = store
        .inner
        .root
        .join("repos/demo/packs")
        .join(format!("{}.pack", hex(&report.pack_id.unwrap())));
    let original = fs::read(&path).unwrap();
    let mut corrupt = original.clone();
    corrupt[8] ^= 1;
    fs::write(&path, &corrupt).unwrap();
    assert!(store.read("demo", target.key).is_err());
    fs::write(&path, &original[..original.len() - 1]).unwrap();
    assert!(store.descriptor("demo", target.key).is_err());
    fs::write(&path, &original).unwrap();
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.meta, b"format")
            .unwrap()
            .as_deref(),
        Some(b"tinytree-store-2".as_slice())
    );
    let mut tx = store
        .inner
        .db
        .write_tx()
        .durability(Some(PersistMode::SyncAll));
    tx.insert(&store.inner.spaces.meta, b"format", b"tinytree-store-1");
    store.inner.commit(tx).unwrap();
    drop(store);
    assert!(
        Store::open_existing(directory.path(), config()).is_err(),
        "packed locations cannot masquerade as an old loose-only store"
    );
}

#[test]
fn identical_cross_kind_packs_preserve_all_logical_members_across_gc_and_reopen() {
    let (directory, store) = open();
    let encoded: Vec<_> = ["first-target", "other-target"]
        .into_iter()
        .map(|target| {
            tinytree_model::encode(&Object::Symlink(target.into()), Limits::default()).unwrap()
        })
        .collect();
    let files: Vec<_> = encoded
        .iter()
        .map(|object| {
            let descriptor = tinytree_model::describe_file(&object.bytes);
            store.put("demo", descriptor, &object.bytes, 0).unwrap();
            descriptor
        })
        .collect();
    let first = store.pack("demo", PackOptions::default()).unwrap();
    assert_eq!(first.packed_objects, 2);
    for object in &encoded {
        store
            .put("demo", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let second = store.pack("demo", PackOptions::default()).unwrap();
    assert_eq!(second.packed_objects, 2);
    assert_eq!(
        first.pack_id, second.pack_id,
        "identical physical bytes deduplicate"
    );
    let pack_path = store
        .inner
        .root
        .join("repos/demo/packs")
        .join(format!("{}.pack", hex(&first.pack_id.unwrap())));
    let mut request = client_collection(&store, 100);
    request.complete_roots = encoded.iter().map(|object| object.descriptor.key).collect();
    assert_eq!(
        store.client_gc("demo", &request).unwrap().deleted_objects,
        2
    );
    for file in files {
        assert_eq!(store.descriptor("demo", file.key).unwrap(), None);
    }
    assert!(pack_path.exists());
    drop(store);
    let store = Store::open(directory.path(), config()).unwrap();
    for object in &encoded {
        let mut bytes = Vec::new();
        store
            .read("demo", object.descriptor.key)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert_eq!(bytes, object.bytes);
    }
    assert_eq!(
        store
            .client_gc("demo", &client_collection(&store, 100))
            .unwrap()
            .deleted_objects,
        2
    );
    assert!(!pack_path.exists());
}

#[test]
fn bounded_pack_scan_advances_across_empty_pages_and_survives_reopen() {
    let (directory, mut store) = open();
    let large_bytes = vec![b'x'; 65 * 1024];
    let large = tinytree_model::describe_file(&large_bytes);
    store.put("demo", large, &large_bytes, 0).unwrap();
    let objects: Vec<_> = ["target-a", "target-b"]
        .into_iter()
        .map(|target| put_object(&store, Object::Symlink(target.into()), 0))
        .collect();
    let options = PackOptions {
        max_scan_objects: 1,
        ..PackOptions::default()
    };
    let first = store.pack("demo", options.clone()).unwrap();
    assert_eq!(first.scanned_objects, 1);
    assert_eq!(
        first.packed_objects, 0,
        "oversized file is an empty eligibility page"
    );
    assert!(!first.scan_complete);
    drop(store);
    store = Store::open(directory.path(), config()).unwrap();
    let mut packed = 0;
    let mut complete = false;
    for _ in 0..8 {
        let report = store.pack("demo", options.clone()).unwrap();
        assert!(report.scanned_objects <= 1);
        packed += report.packed_objects;
        if report.scan_complete {
            complete = true;
            break;
        }
    }
    assert!(complete, "bounded maintenance eventually completes a sweep");
    assert_eq!(packed, 2, "singleton eligible pages cannot starve");
    for object in objects {
        let mut bytes = Vec::new();
        store
            .read("demo", object.key)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert!(!bytes.is_empty());
    }
    let mut complete = false;
    for _ in 0..8 {
        let report = store.pack("demo", options.clone()).unwrap();
        assert_eq!(report.packed_objects, 0);
        if report.scan_complete {
            complete = true;
            break;
        }
    }
    assert!(complete, "already-packed pages also advance to the end");
}

#[test]
fn pack_capacity_continues_without_skipping_eligible_members() {
    let (_directory, store) = open();
    let objects: Vec<_> = [b'a', b'b', b'c']
        .into_iter()
        .map(|byte| {
            let bytes = vec![byte; 600];
            let descriptor = tinytree_model::describe_file(&bytes);
            store.put("demo", descriptor, &bytes, 0).unwrap();
            descriptor
        })
        .collect();
    let options = PackOptions {
        max_pack_bytes: 1024,
        ..PackOptions::default()
    };
    let mut packed = 0;
    let mut complete = false;
    for _ in 0..8 {
        let report = store.pack("demo", options.clone()).unwrap();
        assert!(
            report.packed_objects <= 1,
            "raw budget fits only one member"
        );
        packed += report.packed_objects;
        if report.scan_complete {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert_eq!(packed, objects.len() as u64);
    for object in objects {
        assert!(!store.inner.path("demo", object.key).exists());
        let mut contents = Vec::new();
        store
            .read("demo", object.key)
            .unwrap()
            .read_to_end(&mut contents)
            .unwrap();
        assert_eq!(contents.len(), 600);
    }
}
