//! Generate canonical objects and observable results from the unchanged local
//! Store for the alternate backend's runtime conformance checks.
use serde_json::{Value, json};
use tinytree_model::{
    ChangeId, Commit, CommitId, CopyHistory, CopyId, Descriptor, FileId, Kind, Limits, Object,
    Signature, SymlinkId, Timestamp, Tree, TreeEntry, TreeId, TreeMerge, TreeValue,
};
use tinytree_store::{AccountId, Config, Error, ForgeAccountMutation, ForgeActor, ForgeMember, ForgeViewVisibility, PolicyEpoch, PublishRequest, Reference, Store, ViewSpec, ViewVersion};

const REPOSITORY: &str = "forge-example";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn reference(value: &Reference) -> Value {
    json!({"name":value.name,"generation":value.generation,
        "head":value.commit.map(|d|hex(&d.key.id.0))})
}

fn publish(token: &str, generation: u64, head: Descriptor) -> PublishRequest {
    PublishRequest {
        idempotency_key: token.into(),
        reference_name: "main".into(),
        expected_generation: generation,
        new_commit: Some(CommitId(head.key.id.0)),
        staging_leases: vec![],
    }
}

fn failure(result: tinytree_store::Result<tinytree_store::Publication>) -> &'static str {
    match result {
        Err(Error::StaleReference(_)) => "stale_reference",
        Err(Error::IdempotencyConflict) => "idempotency_conflict",
        Err(Error::MissingObject(_)) => "missing_closure",
        Err(Error::Invalid(_)) => "invalid_closure",
        _ => panic!("fixture expected a specific refused publication"),
    }
}

// Separate wide projection oracle; existing raw/closure fixtures stay unchanged.
fn projection_fixture(store: &Store) -> Value {
    let account = AccountId("forge-wide".into());
    let owner = ForgeActor { principal: "forge-owner".into() };
    store.forge_mutate_account(&account, &owner, "wide-provision", ForgeAccountMutation::Provision).unwrap();
    let repository = account.repository_id();
    let mut objects = Vec::new();
    let mut put = |object: Option<Object>, body: &[u8]| {
        let (descriptor, bytes) = if let Some(object) = object {
            let encoded = tinytree_model::encode(&object, Limits::default()).unwrap();
            (encoded.descriptor, encoded.bytes)
        } else { (tinytree_model::describe_file(body), body.to_vec()) };
        store.put(&repository, descriptor, &bytes, 0).unwrap();
        objects.push(json!({"kind":descriptor.key.kind as u8,"id":hex(&descriptor.key.id.0),"size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version,"bytes":bytes}));
        descriptor
    };
    let readme_bytes = "\u{feff}# Wide tree\nNative BOM README\n".as_bytes();
    let readme = put(None, readme_bytes);
    let secret = put(None, b"private fixture bytes never projected\n");
    let private_tree = put(Some(Object::Tree(Tree { entries: vec![TreeEntry { name: "secret.txt".into(), value: TreeValue::File { id: FileId(secret.key.id.0), executable: false, size_bytes: secret.size_bytes, copy_id: None } }] })), &[]);
    let mut entries = vec![TreeEntry { name: "README.md".into(), value: TreeValue::File { id: FileId(readme.key.id.0), executable: false, size_bytes: readme.size_bytes, copy_id: None } }];
    for ordinal in 0..700 {
        let file = put(None, format!("Public fixture file {ordinal}\n").as_bytes());
        entries.push(TreeEntry { name: format!("file-{ordinal:04}.txt"), value: TreeValue::File { id: FileId(file.key.id.0), executable: false, size_bytes: file.size_bytes, copy_id: None } });
    }
    entries.push(TreeEntry { name: "private".into(), value: TreeValue::Tree(TreeId(private_tree.key.id.0)) });
    let tree = put(Some(Object::Tree(Tree { entries })), &[]);
    let person = Signature { name: "Fixture".into(), email: "fixture@example.invalid".into(), timestamp: Timestamp { millis_since_epoch: 0, tz_offset_minutes: 0 } };
    let root = put(Some(Object::Commit(Commit { parents: vec![CommitId::ROOT], predecessors: vec![], root_tree: TreeMerge { terms: vec![TreeId(tree.key.id.0)], labels: vec![String::new()] }, change_id: ChangeId([91;16]), description: "Wide projection fixture".into(), author: person.clone(), committer: person, secure_signature: None })), &[]);
    let spec = ViewSpec { name: "public".into(), root_prefix: String::new(), include_prefixes: vec![String::new()], exclude_prefixes: vec!["private".into()], visibility: ForgeViewVisibility::Public, revision: root.key.id.0, export_allowed: false };
    let published = store.forge_mutate_account(&account, &owner, "wide-view", ForgeAccountMutation::PublishView { spec: spec.clone(), expected_epoch: PolicyEpoch(1), make_default: true, follow_main: false }).unwrap();
    let view = store.forge_resolve_view(&account, Some(&owner), "public", None).unwrap();
    let files = store.forge_view_files(&view, 1000).unwrap();
    let projection = store.forge_project_view(&view, 0).unwrap();
    let readme_content = String::from_utf8(store.forge_read_view_file(&view, "README.md", 65536).unwrap()).unwrap();
    let snapshot = json!({"account_id":account.0,"view":view.name(),"view_version":view.version().0,"revision":view.revision().0,"policy_epoch":view.epoch().0,"root_prefix":view.root_prefix(),"projected_commit":projection.projected_commit,"projected_tree":projection.projected_tree,"paths":files.into_iter().map(|f|f.path).collect::<Vec<_>>(),"readme":{"path":"README.md","content":readme_content},"readme_error":null,"complete":true});
    json!({"account_id":account.0,"owner":owner.principal,"objects":objects,"root":{"kind":3,"id":hex(&root.key.id.0)},"spec":spec,"published":published,"expected_snapshot":snapshot,"readme":{"kind":1,"id":hex(&readme.key.id.0),"bytes":readme_bytes},"private_file":{"kind":1,"id":hex(&secret.key.id.0)}})
}

// Independent production-ingest boundary: one real 1MiB FILE and its minimal
// canonical TREE/COMMIT closure. Never reuse fabricated or invalid object IDs.
fn bootstrap_fixture(store: &Store) -> Value {
    let repository = "bootstrap-boundary";
    store.create_repository(repository).unwrap();
    let mut objects = Vec::new();
    let mut put = |object: Option<Object>, body: &[u8]| {
        let (descriptor, bytes) = match object {
            Some(object) => {
                let encoded = tinytree_model::encode(&object, Limits::default()).unwrap();
                (encoded.descriptor, encoded.bytes)
            }
            None => (tinytree_model::describe_file(body), body.to_vec()),
        };
        tinytree_model::verify(&descriptor, &bytes, Limits::default()).unwrap();
        store.put(repository, descriptor, &bytes, 0).unwrap();
        objects.push(json!({"kind":descriptor.key.kind as u8,"id":hex(&descriptor.key.id.0),
            "size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version,"bytes":bytes}));
        descriptor
    };
    let file = put(None, &vec![b'x'; 1024 * 1024]);
    assert_eq!(file.size_bytes, 1024 * 1024);
    let tree = put(
        Some(Object::Tree(Tree {
            entries: vec![TreeEntry {
                name: "boundary.bin".into(),
                value: TreeValue::File {
                    id: FileId(file.key.id.0),
                    executable: false,
                    size_bytes: file.size_bytes,
                    copy_id: None,
                },
            }],
        })),
        &[],
    );
    let person = Signature {
        name: "Fixture".into(),
        email: "fixture@example.invalid".into(),
        timestamp: Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    let root = put(
        Some(Object::Commit(Commit {
            parents: vec![CommitId::ROOT],
            predecessors: vec![],
            root_tree: TreeMerge {
                terms: vec![TreeId(tree.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: ChangeId([92; 16]),
            description: "Bootstrap boundary fixture".into(),
            author: person.clone(),
            committer: person,
            secure_signature: None,
        })),
        &[],
    );
    drop(put);
    // Native publication independently checks the complete typed closure.
    let publication = store
        .publish(repository, &publish("bootstrap-boundary", 0, root), 0)
        .unwrap();
    assert_eq!(publication.reference.commit.unwrap(), root);
    assert_eq!(objects.len(), 3);
    json!({"root":{"kind":3,"id":hex(&root.key.id.0)},"largest_file":objects[0],"objects":objects})
}

// Independent paged-search oracle. Large FILE content is stored once even
// though eight visible paths reference it; no preexisting fixtures are changed.
fn search_fixture(store: &Store) -> Value {
    use tinytree_search::{MatchBudget, MatchLimits, Pattern, SearchMode};
    let account = AccountId("forge-search".into());
    let owner = ForgeActor {
        principal: "forge-owner".into(),
    };
    store
        .forge_mutate_account(
            &account,
            &owner,
            "search-provision",
            ForgeAccountMutation::Provision,
        )
        .unwrap();
    let repository = account.repository_id();
    let mut objects = Vec::new();
    let mut identities = std::collections::HashSet::new();
    let mut put = |object: Option<Object>, body: &[u8]| {
        let (descriptor, bytes) = match object {
            Some(object) => {
                let encoded = tinytree_model::encode(&object, Limits::default()).unwrap();
                (encoded.descriptor, encoded.bytes)
            }
            None => (tinytree_model::describe_file(body), body.to_vec()),
        };
        store.put(&repository, descriptor, &bytes, 0).unwrap();
        if identities.insert(descriptor.key) {
            objects.push(json!({"kind":descriptor.key.kind as u8,"id":hex(&descriptor.key.id.0),"size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version,"bytes":bytes}));
        }
        descriptor
    };
    let mut first_bytes = String::new();
    for ordinal in 0..128 {
        first_bytes.push_str(&format!("line {ordinal:03} needle\n"));
    }
    first_bytes.push_str("α needle\n");
    first_bytes.push_str("nul\0needle\n");
    let first = put(None, first_bytes.as_bytes());
    let binary = put(None, b"\xffneedle\n");
    let link = put(Some(Object::Symlink("00-hits.txt".into())), &[]);
    let filler = put(None, &vec![b'f'; 1024 * 1024]);
    let readme_bytes = b"# Paged search fixture\nPublic code search\n";
    let readme = put(None, readme_bytes);
    let late = put(None, b"after prefix needle\n");
    let secret = put(None, b"private secret needle\n");
    let file_entry = |name: &str, descriptor: Descriptor| TreeEntry {
        name: name.into(),
        value: TreeValue::File {
            id: FileId(descriptor.key.id.0),
            executable: false,
            size_bytes: descriptor.size_bytes,
            copy_id: None,
        },
    };
    let private_tree = put(
        Some(Object::Tree(Tree {
            entries: vec![file_entry("secret.txt", secret)],
        })),
        &[],
    );
    let mut entries = vec![
        file_entry("00-hits.txt", first),
        file_entry("01-binary.bin", binary),
        TreeEntry {
            name: "02-link".into(),
            value: TreeValue::Symlink {
                id: SymlinkId(link.key.id.0),
                target_bytes: 11,
            },
        },
    ];
    for ordinal in 0..8 {
        entries.push(file_entry(
            &format!("{}-fill-{ordinal}.txt", ordinal + 10),
            filler,
        ));
    }
    entries.push(file_entry("README.md", readme));
    entries.push(TreeEntry {
        name: "private".into(),
        value: TreeValue::Tree(TreeId(private_tree.key.id.0)),
    });
    entries.push(file_entry("zzmatch.txt", late));
    let tree = put(Some(Object::Tree(Tree { entries })), &[]);
    let person = Signature {
        name: "Fixture".into(),
        email: "fixture@example.invalid".into(),
        timestamp: Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    let root = put(
        Some(Object::Commit(Commit {
            parents: vec![CommitId::ROOT],
            predecessors: vec![],
            root_tree: TreeMerge {
                terms: vec![TreeId(tree.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: ChangeId([93; 16]),
            description: "Paged search fixture".into(),
            author: person.clone(),
            committer: person,
            secure_signature: None,
        })),
        &[],
    );
    drop(put);
    let spec = ViewSpec {
        name: "public".into(),
        root_prefix: String::new(),
        include_prefixes: vec![String::new()],
        exclude_prefixes: vec!["private".into()],
        visibility: ForgeViewVisibility::Public,
        revision: root.key.id.0,
        export_allowed: false,
    };
    let published = store
        .forge_mutate_account(
            &account,
            &owner,
            "search-view",
            ForgeAccountMutation::PublishView {
                spec: spec.clone(),
                expected_epoch: PolicyEpoch(1),
                make_default: true,
                follow_main: false,
            },
        )
        .unwrap();
    let view = store.forge_resolve_view(&account, None, "", None).unwrap();
    let files = store.forge_view_files(&view, 1000).unwrap();
    let projection = store.forge_project_view(&view, 0).unwrap();
    let pattern = Pattern::new(b"needle", SearchMode::Literal).unwrap();
    let mut expected_hits = Vec::new();
    let mut skipped_binary = 0;
    for file in &files {
        if file.descriptor.key.kind != Kind::File {
            continue;
        }
        let bytes = store
            .forge_read_view_file(&view, &file.path, 1024 * 1024)
            .unwrap();
        if std::str::from_utf8(&bytes).is_err() {
            skipped_binary += 1;
            continue;
        }
        let mut budget = MatchBudget::new(MatchLimits {
            max_input_bytes: 1024 * 1024,
            max_work: 64 * 1024 * 1024,
            max_output_bytes: 256 * 1024,
        });
        let page = pattern.scan(&bytes, 0, 1000, &mut budget, || true).unwrap();
        assert!(page.next_offset.is_none());
        for hit in page.hits {
            expected_hits.push(json!({"path":file.path,"line":hit.line,"column":hit.column,"preview":String::from_utf8_lossy(&hit.snippet)}));
        }
    }
    assert_eq!(expected_hits.len(), 131);
    assert_eq!(skipped_binary, 1);
    assert_eq!(expected_hits.last().unwrap()["path"], "zzmatch.txt");
    assert_eq!(
        files
            .iter()
            .filter(|file| file.descriptor == filler)
            .count(),
        8
    );
    assert_eq!(
        objects
            .iter()
            .filter(|object| object["kind"] == 1 && object["id"] == hex(&filler.key.id.0))
            .count(),
        1
    );
    let readme_content = String::from_utf8(
        store
            .forge_read_view_file(&view, "README.md", 65536)
            .unwrap(),
    )
    .unwrap();
    let snapshot = json!({"account_id":account.0,"view":view.name(),"view_version":view.version().0,"revision":view.revision().0,"policy_epoch":view.epoch().0,"root_prefix":view.root_prefix(),"projected_commit":projection.projected_commit,"projected_tree":projection.projected_tree,"paths":files.into_iter().map(|f|f.path).collect::<Vec<_>>(),"readme":{"path":"README.md","content":readme_content},"readme_error":null,"complete":true});
    json!({"account_id":account.0,"owner":owner.principal,"objects":objects,"root":{"kind":3,"id":hex(&root.key.id.0)},"spec":spec,"published":published,"expected_snapshot":snapshot,"expected_hits":expected_hits,"expected_skipped_files":{"binary":skipped_binary,"oversized":0},"late_match_path":"zzmatch.txt","readme":{"kind":1,"id":hex(&readme.key.id.0),"bytes":readme_bytes.as_slice()},"private_file":{"kind":1,"id":hex(&secret.key.id.0)},"filler_file":{"kind":1,"id":hex(&filler.key.id.0),"size_bytes":filler.size_bytes,"format_version":filler.format_version}})
}

fn main() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), Config::default()).unwrap();
    store.create_repository(REPOSITORY).unwrap();
    let mut objects = Vec::new();
    let mut put = |name: &str, object: Option<Object>, body: &[u8]| {
        let (descriptor, bytes) = match object {
            Some(object) => {
                let encoded = tinytree_model::encode(&object, Limits::default()).unwrap();
                (encoded.descriptor, encoded.bytes)
            }
            None => (tinytree_model::describe_file(body), body.to_vec()),
        };
        store.put(REPOSITORY, descriptor, &bytes, 0).unwrap();
        objects.push(json!({"name":name,"kind":descriptor.key.kind as u8,
            "id":hex(&descriptor.key.id.0),"size_bytes":descriptor.size_bytes,"bytes":bytes}));
        descriptor
    };
    let original = put("original_file", None, b"# Tiny tree\nOriginal source\n");
    let updated = put("updated_file", None, b"# Tiny tree\nUpdated source\n");
    let seed = put(
        "copy_seed",
        Some(Object::CopyHistory(CopyHistory {
            current_path: "README.md".into(),
            parents: vec![],
            salt: b"fixture-seed".to_vec(),
        })),
        &[],
    );
    let copied = put(
        "nested_copy",
        Some(Object::CopyHistory(CopyHistory {
            current_path: "src/source.rs".into(),
            parents: vec![CopyId(seed.key.id.0)],
            salt: b"fixture-copy".to_vec(),
        })),
        &[],
    );
    let symlink = put("symlink", Some(Object::Symlink("README.md".into())), &[]);
    let subtree = put(
        "subtree",
        Some(Object::Tree(Tree {
            entries: vec![TreeEntry {
                name: "source.rs".into(),
                value: TreeValue::File {
                    id: FileId(updated.key.id.0),
                    executable: false,
                    size_bytes: updated.size_bytes,
                    copy_id: Some(CopyId(copied.key.id.0)),
                },
            }],
        })),
        &[],
    );
    let make_tree = |file: Descriptor, size: u64, name: &str| {
        Object::Tree(Tree {
            entries: vec![
                TreeEntry {
                    name: name.into(),
                    value: TreeValue::File {
                        id: FileId(file.key.id.0),
                        executable: false,
                        size_bytes: size,
                        copy_id: Some(CopyId(seed.key.id.0)),
                    },
                },
                TreeEntry {
                    name: "link".into(),
                    value: TreeValue::Symlink {
                        id: SymlinkId(symlink.key.id.0),
                        target_bytes: 9,
                    },
                },
                TreeEntry {
                    name: "src".into(),
                    value: TreeValue::Tree(TreeId(subtree.key.id.0)),
                },
            ],
        })
    };
    let first_tree = put(
        "first_tree",
        Some(make_tree(original, original.size_bytes, "README.md")),
        &[],
    );
    let next_tree = put(
        "next_tree",
        Some(make_tree(updated, updated.size_bytes, "README.md")),
        &[],
    );
    let wrong_size = put(
        "wrong_size_tree",
        Some(make_tree(original, original.size_bytes + 1, "README.md")),
        &[],
    );
    // The entries remain canonically sorted, but CopyHistory belongs to README.md.
    let wrong_copy = put(
        "wrong_copy_tree",
        Some(make_tree(original, original.size_bytes, "README.other")),
        &[],
    );
    let missing_tree = put(
        "missing_tree",
        Some(Object::Tree(Tree {
            entries: vec![TreeEntry {
                name: "missing".into(),
                value: TreeValue::File {
                    id: FileId([99; 32]),
                    executable: false,
                    size_bytes: 5,
                    copy_id: None,
                },
            }],
        })),
        &[],
    );
    let person = Signature {
        name: "Fixture".into(),
        email: "fixture@example.invalid".into(),
        timestamp: Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    let commit = |tree: Descriptor, parent: CommitId, change: u8| {
        Object::Commit(Commit {
            parents: vec![parent],
            predecessors: vec![],
            root_tree: TreeMerge {
                terms: vec![TreeId(tree.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: ChangeId([change; 16]),
            description: format!("Fixture change {change}"),
            author: person.clone(),
            committer: person.clone(),
            secure_signature: None,
        })
    };
    let first = put(
        "first_commit",
        Some(commit(first_tree, CommitId::ROOT, 1)),
        &[],
    );
    let next = put(
        "next_commit",
        Some(commit(next_tree, CommitId(first.key.id.0), 2)),
        &[],
    );
    let bad_size = put(
        "wrong_size_commit",
        Some(commit(wrong_size, CommitId::ROOT, 3)),
        &[],
    );
    let bad_copy = put(
        "wrong_copy_commit",
        Some(commit(wrong_copy, CommitId::ROOT, 4)),
        &[],
    );
    let missing = put(
        "missing_commit",
        Some(commit(missing_tree, CommitId::ROOT, 5)),
        &[],
    );
    drop(put);
    let initial = reference(&store.references(REPOSITORY, &["main".into()]).unwrap()[0]);
    let first_request = publish("first", 0, first);
    let first_result = store.publish(REPOSITORY, &first_request, 1).unwrap();
    let replay = store.publish(REPOSITORY, &first_request, 1).unwrap();
    assert_eq!(first_result, replay);
    let next_result = store
        .publish(REPOSITORY, &publish("next", 1, next), 2)
        .unwrap();
    let historical_replay = store.publish(REPOSITORY, &first_request, 2).unwrap();
    assert_eq!(first_result, historical_replay);
    let refused = json!({
        "stale":failure(store.publish(REPOSITORY, &publish("stale", 0, next), 2)),
        "rebound":failure(store.publish(REPOSITORY, &publish("first", 0, next), 2)),
        "wrong_size":failure(store.publish(REPOSITORY, &publish("wrong-size", 2, bad_size), 2)),
        "wrong_copy":failure(store.publish(REPOSITORY, &publish("wrong-copy", 2, bad_copy), 2)),
        "missing":failure(store.publish(REPOSITORY, &publish("missing", 2, missing), 2)),
    });
    let after = reference(&store.references(REPOSITORY, &["main".into()]).unwrap()[0]);
    assert_eq!(after, reference(&next_result.reference));
    // Reopen proves receipts and refs are native durable observations, not a mock.
    drop(store);
    let store = Store::open_existing(directory.path(), Config::default()).unwrap();
    assert_eq!(
        store.publish(REPOSITORY, &first_request, 3).unwrap(),
        first_result
    );
    assert_eq!(
        reference(&store.references(REPOSITORY, &["main".into()]).unwrap()[0]),
        after
    );
    let account = AccountId("forge-demo".into());
    let owner = ForgeActor { principal: "forge-owner".into() };
    let provisioned = store.forge_mutate_account(&account, &owner, "provision-owner", ForgeAccountMutation::Provision).unwrap();
    let member = ForgeMember { principal: "forge-member".into(), can_write: true, can_review: true, is_admin: true, roles: vec!["reviewer".into()] };
    let added = store.forge_mutate_account(&account, &owner, "add-member", ForgeAccountMutation::SetMember { member: member.clone(), expected_epoch: PolicyEpoch(1) }).unwrap();
    let unchanged = store.forge_mutate_account(&account, &owner, "same-member", ForgeAccountMutation::SetMember { member: member.clone(), expected_epoch: PolicyEpoch(2) }).unwrap();
    let revoked = store.forge_mutate_account(&account, &owner, "revoke-admin", ForgeAccountMutation::SetMember { member: ForgeMember { is_admin: false, ..member }, expected_epoch: PolicyEpoch(2) }).unwrap();
    let public_spec = ViewSpec { name: "public".into(), root_prefix: String::new(), include_prefixes: vec![String::new()], exclude_prefixes: vec![], visibility: ForgeViewVisibility::Public, revision: serde_json::from_value(provisioned["main_revision"].clone()).unwrap(), export_allowed: false };
    let public = store.forge_mutate_account(&account, &owner, "public-view", ForgeAccountMutation::PublishView { spec: public_spec.clone(), expected_epoch: PolicyEpoch(3), make_default: true, follow_main: true }).unwrap();
    let locked_spec = ViewSpec { visibility: ForgeViewVisibility::Members, ..public_spec.clone() };
    let locked = store.forge_mutate_account(&account, &owner, "locked-view", ForgeAccountMutation::PublishView { spec: locked_spec.clone(), expected_epoch: PolicyEpoch(4), make_default: false, follow_main: true }).unwrap();
    let retired = store.forge_mutate_account(&account, &owner, "retire-old-view", ForgeAccountMutation::RetireView { version: ViewVersion(serde_json::from_value(public["view_version"].clone()).unwrap()), expected_epoch: PolicyEpoch(5), view: "public".into() }).unwrap();
    // A real canonical closure larger than the old all-in-one 256-object cap.
    // Keep these objects separate so raw-backend fixtures retain their scope.
    let mut history = Vec::new();
    let mut history_head = next;
    for ordinal in 0..300u128 {
        let encoded = tinytree_model::encode(&Object::Commit(Commit {
            parents: vec![CommitId(history_head.key.id.0)], predecessors: vec![],
            root_tree: TreeMerge { terms: vec![TreeId(next_tree.key.id.0)], labels: vec![String::new()] },
            change_id: ChangeId((ordinal + 1000).to_le_bytes()),
            description: format!("Closure fixture {ordinal}"),
            author: person.clone(), committer: person.clone(), secure_signature: None,
        }), Limits::default()).unwrap();
        store.put(REPOSITORY, encoded.descriptor, &encoded.bytes, 0).unwrap();
        history_head = encoded.descriptor;
        history.push(json!({"kind":encoded.descriptor.key.kind as u8,"id":hex(&encoded.descriptor.key.id.0),"size_bytes":encoded.descriptor.size_bytes,"format_version":encoded.descriptor.format_version,"bytes":encoded.bytes}));
    }
    let history_publication = store.publish(REPOSITORY, &publish("closure-history", 2, history_head), 4).unwrap();
    let projection_fixture = projection_fixture(&store);
    let bootstrap_fixture = bootstrap_fixture(&store);
    let search_fixture = search_fixture(&store);
    let value = json!({"schema_version":1,"account":"example","repository":REPOSITORY,
        "projection":projection_fixture,
        "bootstrap":bootstrap_fixture,
        "search":search_fixture,
        "forge":{"account_id":account.0,"provisioned":provisioned,"added":added,"unchanged":unchanged,"revoked":revoked,"views":{"public_spec":public_spec,"public":public,"locked_spec":locked_spec,"locked":locked,"retired":retired}},
        "closure":{"root":{"kind":3,"id":hex(&history_head.key.id.0)},"objects":history,"native_publication":reference(&history_publication.reference)},
        "objects":objects,"native":{"initial":initial,"first":reference(&first_result.reference),
        "next":reference(&next_result.reference),"historical_replay":reference(&historical_replay.reference),
        "after_refusals":after,"refusals":refused,"reopen":true}});
    let output = serde_json::to_string(&value).unwrap();
    assert!(output.len() < 16 * 1024 * 1024, "contract fixture exceeds runtime runner output bound");
    println!("{output}");
    assert_eq!(first.key.kind, Kind::Commit);
}
