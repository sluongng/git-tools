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
    let value = json!({"schema_version":1,"account":"example","repository":REPOSITORY,
        "forge":{"account_id":account.0,"provisioned":provisioned,"added":added,"unchanged":unchanged,"revoked":revoked,"views":{"public_spec":public_spec,"public":public,"locked_spec":locked_spec,"locked":locked,"retired":retired}},
        "closure":{"root":{"kind":3,"id":hex(&history_head.key.id.0)},"objects":history,"native_publication":reference(&history_publication.reference)},
        "objects":objects,"native":{"initial":initial,"first":reference(&first_result.reference),
        "next":reference(&next_result.reference),"historical_replay":reference(&historical_replay.reference),
        "after_refusals":after,"refusals":refused,"reopen":true}});
    println!("{value}");
    assert_eq!(first.key.kind, Kind::Commit);
}
