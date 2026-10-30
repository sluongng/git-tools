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
    drop(put);
    let uploads = projection_uploads(store, &account, &owner, &view, &projection, secret);
    let reviews = projection_reviews(store, &account, &owner, &view, &projection);
    let readme_content = String::from_utf8(store.forge_read_view_file(&view, "README.md", 65536).unwrap()).unwrap();
    let snapshot = json!({"account_id":account.0,"view":view.name(),"view_version":view.version().0,"revision":view.revision().0,"policy_epoch":view.epoch().0,"root_prefix":view.root_prefix(),"projected_commit":projection.projected_commit,"projected_tree":projection.projected_tree,"paths":files.into_iter().map(|f|f.path).collect::<Vec<_>>(),"readme":{"path":"README.md","content":readme_content},"readme_error":null,"complete":true});
    json!({"account_id":account.0,"owner":owner.principal,"uploads":uploads,"reviews":reviews,"objects":objects,"root":{"kind":3,"id":hex(&root.key.id.0)},"spec":spec,"published":published,"expected_snapshot":snapshot,"readme":{"kind":1,"id":hex(&readme.key.id.0),"bytes":readme_bytes},"private_file":{"kind":1,"id":hex(&secret.key.id.0)}})
}

// Native admission oracle has genesis main, while the pure candidate oracle
// explicitly targets the canonical view root. These are different target states;
// neither is a passing checker receipt or a Cloudflare candidate-state oracle.
fn projection_reviews(
    store: &Store,
    account: &AccountId,
    actor: &ForgeActor,
    view: &tinytree_store::ResolvedView,
    projection: &tinytree_store::ForgeProjection,
) -> Value {
    fn encoded(object: Object) -> (Descriptor, Vec<u8>) {
        let encoded = tinytree_model::encode(&object, Limits::default()).unwrap();
        (encoded.descriptor, encoded.bytes)
    }
    let repository = account.repository_id();
    let before = store
        .references(&repository, &["main".into()])
        .unwrap()
        .remove(0);
    let (_, bytes) = store
        .read_metadata(
            &repository,
            tinytree_model::ObjectKey {
                kind: Kind::Tree,
                id: tinytree_model::ObjectId(projection.projected_tree),
            },
        )
        .unwrap();
    let Object::Tree(mut root) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else {
        panic!("review projected tree")
    };
    assert_eq!(root.entries.len(), 701);
    let (_, bytes) = store
        .read_metadata(
            &repository,
            tinytree_model::ObjectKey {
                kind: Kind::Commit,
                id: tinytree_model::ObjectId(projection.projected_commit),
            },
        )
        .unwrap();
    let Object::Commit(mut source) = tinytree_model::decode(&bytes, Limits::default()).unwrap()
    else {
        panic!("review projected commit")
    };
    let file_bytes = b"Plain authored source for review admission\n".to_vec();
    let file = tinytree_model::describe_file(&file_bytes);
    root.entries.push(TreeEntry {
        name: "source.txt".into(),
        value: TreeValue::File {
            id: FileId(file.key.id.0),
            executable: false,
            size_bytes: file.size_bytes,
            copy_id: None,
        },
    });
    root.entries.sort_by(|a, b| a.name.cmp(&b.name));
    let tree = encoded(Object::Tree(root));
    source.parents = vec![CommitId(projection.projected_commit)];
    source.predecessors.clear();
    source.root_tree = TreeMerge {
        terms: vec![TreeId(tree.0.key.id.0)],
        labels: vec![String::new()],
    };
    source.change_id = ChangeId([94; 16]);
    source.description = "Plain authored review fixture".into();
    let committer = source.committer.clone();
    let head = encoded(Object::Commit(source));
    let mut objects = Vec::new();
    for (name, path, (descriptor, bytes)) in [
        ("plain-file", "source.txt", (file, file_bytes)),
        ("plain-root-tree", "", tree),
        ("plain-source-commit", "", head.clone()),
    ] {
        let request_id = format!("native-review-upload-{name}");
        let begun = store
            .forge_begin_upload(view, actor, &request_id, path, descriptor, 0)
            .unwrap();
        let mut offset = 0;
        for chunk in bytes.chunks(64 * 1024) {
            let status = store
                .forge_append_upload(
                    account,
                    actor,
                    view.name(),
                    begun.session_id,
                    offset,
                    chunk,
                    0,
                )
                .unwrap();
            offset += chunk.len() as u64;
            assert_eq!(status.offset, offset);
        }
        let status = store
            .forge_finalize_upload(account, actor, view.name(), begun.session_id, 0)
            .unwrap();
        assert_eq!(
            status.state,
            tinytree_store::forge_upload::ForgeUploadState::Committed
        );
        let receipt = status.receipt.as_ref().expect("native scoped byte receipt");
        assert_eq!(receipt.id, descriptor.key.id.0);
        assert_eq!(receipt.kind, descriptor.key.kind as u8);
        assert_eq!(receipt.size_bytes, descriptor.size_bytes);
        assert_eq!(receipt.format_version, descriptor.format_version);
        objects.push(json!({"name":name,"path":path,"request_id":request_id,"kind":descriptor.key.kind as u8,"id":hex(&descriptor.key.id.0),"size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version,"bytes":bytes,"session_id":begun.session_id,
            "native_receipt":serde_json::to_value(receipt).unwrap()}));
    }
    let profile = tinytree_store::LandingCheckProfile {
        name: "checks".into(),
        version: hex(&tinytree_model::file_id(b"fixture-check-profile-v1").0),
    };
    let request = tinytree_store::ForgeReviewPushRequest {
        request_id: "native-review-plain".into(),
        source_base: CommitId(projection.projected_commit),
        source_head: CommitId(head.0.key.id.0),
        view_version: view.version().0,
        expected_patchset: None,
        predecessor: None,
        provenance: None,
        lineage: vec![],
        check_profile: profile.clone(),
    };
    let review = store
        .forge_push_review(account, actor, &request, &committer, 1)
        .unwrap();
    assert_eq!(review.change_id, [94; 16]);
    assert_eq!(review.source_head, head.0.key.id.0);
    assert_eq!(review.source_base, projection.projected_commit);
    assert_eq!(review.view_version, view.version().0);
    assert_eq!(review.patchset_number, 1);
    assert!(review.is_current);
    assert_eq!(review.check_profile, profile);
    let replay = store
        .forge_push_review(account, actor, &request, &committer, 1)
        .unwrap();
    assert_eq!(replay.patchset_id, review.patchset_id);
    assert_eq!(replay.patchset_number, 1);
    assert_eq!(
        store
            .forge_review_operation(account, actor, &request.request_id)
            .unwrap()
            .unwrap()
            .patchset_id,
        review.patchset_id
    );
    let after = store
        .references(&repository, &["main".into()])
        .unwrap()
        .remove(0);
    assert_eq!(before.generation, after.generation);
    assert_eq!(before.commit, after.commit);

    struct Reader<'a> {
        store: &'a Store,
        repository: &'a str,
        reads: usize,
    }
    impl tinytree_landing::MetadataReader for Reader<'_> {
        fn read(
            &mut self,
            key: tinytree_model::ObjectKey,
            maximum: usize,
        ) -> Result<Vec<u8>, tinytree_landing::ReadError> {
            assert_ne!(
                key.kind,
                Kind::File,
                "pure builder must never read FILE bodies"
            );
            let descriptor = self
                .store
                .descriptor(self.repository, key)
                .map_err(|_| tinytree_landing::ReadError::Unavailable)?
                .ok_or(tinytree_landing::ReadError::Missing)?;
            if descriptor.size_bytes > maximum as u64 {
                return Err(tinytree_landing::ReadError::Limit);
            }
            self.reads += 1;
            self.store
                .read_metadata(self.repository, key)
                .map(|(_, bytes)| bytes)
                .map_err(|_| tinytree_landing::ReadError::Unavailable)
        }
    }
    let mut reader = Reader {
        store,
        repository: &repository,
        reads: 0,
    };
    // This mapping is established by the real native view/projection above,
    // never a client-submitted guessed canonical/source relationship.
    let candidate = tinytree_landing::build_on_projected_base(
        &mut reader,
        tinytree_landing::Request {
            source_base: request.source_base,
            source_head: request.source_head,
            destination_head: view.revision(),
            committer: committer.clone(),
        },
        view.revision(),
        tinytree_landing::Limits::default(),
    )
    .unwrap();
    assert_eq!(candidate.rewrites.len(), 1);
    assert_eq!(candidate.rewrites[0].old, request.source_head);
    assert_eq!(candidate.rewrites[0].change_id, ChangeId([94; 16]));
    let (_, canonical_commit) = store
        .read_metadata(
            &repository,
            tinytree_model::ObjectKey {
                kind: Kind::Commit,
                id: tinytree_model::ObjectId(view.revision().0),
            },
        )
        .unwrap();
    let Object::Commit(canonical_commit) =
        tinytree_model::decode(&canonical_commit, Limits::default()).unwrap()
    else {
        panic!("canonical source commit")
    };
    let (_, canonical_tree) = store
        .read_metadata(
            &repository,
            tinytree_model::ObjectKey {
                kind: Kind::Tree,
                id: tinytree_model::ObjectId(canonical_commit.root_tree.terms[0].0),
            },
        )
        .unwrap();
    let Object::Tree(mut canonical_tree) =
        tinytree_model::decode(&canonical_tree, Limits::default()).unwrap()
    else {
        panic!("canonical source tree")
    };
    assert!(
        canonical_tree
            .entries
            .iter()
            .any(|entry| entry.name == "private")
    );
    canonical_tree.entries.push(TreeEntry {
        name: "source.txt".into(),
        value: TreeValue::File {
            id: FileId(file.key.id.0),
            executable: false,
            size_bytes: file.size_bytes,
            copy_id: None,
        },
    });
    canonical_tree.entries.sort_by(|a, b| a.name.cmp(&b.name));
    let expected_tree = encoded(Object::Tree(canonical_tree));
    assert_eq!(
        candidate.root_tree.0, expected_tree.0.key.id.0,
        "builder preserves excluded canonical subtree and exact unchanged baseline entries"
    );
    let (state, native_review) =
        tinytree_forge_core::review_commands::format_review(&review).unwrap();
    json!({"case":"plain-addition","objects":objects,"request_id":request.request_id,
        "push":{"source_base":hex(&projection.projected_commit),"source_head":hex(&head.0.key.id.0),"view_version":hex(&view.version().0),"expected_patchset":null,"predecessor":null,"provenance":null,"lineage":[]},
        "profile":profile,"change_id":hex(&[94;16]),"patchset_id":hex(&review.patchset_id),
        "admission_oracle":{"actual_main":reference(&before),"state":state,"review":native_review,"check_execution":"not_run","main_unchanged":true,"replay_same_patchset":true,"candidate_state_parity":false},
        "builder_oracle":{"destination_head":hex(&view.revision().0),"root_tree":hex(&candidate.root_tree.0),"change_id":hex(&candidate.rewrites[0].change_id.0),"source_head":hex(&request.source_head.0),"metadata_reads":reader.reads,"file_body_reads":0,"committer_clock":"frozen_native_fixture_not_runtime_head_oracle"}})
}

// Ordered public session uploads run against the actual native policy/scope
// adapter. Canonical metadata comes only from model encode or stored projection
// bytes, never from a JavaScript codec or a fabricated descriptor.
fn projection_uploads(
    store: &Store,
    account: &AccountId,
    actor: &ForgeActor,
    view: &tinytree_store::ResolvedView,
    projection: &tinytree_store::ForgeProjection,
    private_file: Descriptor,
) -> Value {
    fn encoded(object: Object) -> (Descriptor, Vec<u8>) {
        let object = tinytree_model::encode(&object, Limits::default()).unwrap();
        (object.descriptor, object.bytes)
    }
    let repository = account.repository_id();
    let before = store.references(&repository, &["main".into()]).unwrap().remove(0);
    let (_, bytes) = store.read_metadata(&repository, tinytree_model::ObjectKey {
        kind: Kind::Tree, id: tinytree_model::ObjectId(projection.projected_tree),
    }).unwrap();
    let Object::Tree(mut root) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else { panic!("projection tree") };
    assert_eq!(root.entries.len(), 701);
    let (_, bytes) = store.read_metadata(&repository, tinytree_model::ObjectKey {
        kind: Kind::Commit, id: tinytree_model::ObjectId(projection.projected_commit),
    }).unwrap();
    let Object::Commit(mut source) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else { panic!("projection commit") };
    let file_bytes = b"New scoped copy content\n".to_vec();
    let file = tinytree_model::describe_file(&file_bytes);
    let copy = encoded(Object::CopyHistory(CopyHistory { current_path: "vendor/copied.txt".into(), parents: vec![], salt: b"upload-copy-fixture".to_vec() }));
    let subtree = encoded(Object::Tree(Tree { entries: vec![TreeEntry {
        name: "copied.txt".into(), value: TreeValue::File { id: FileId(file.key.id.0), executable: false,
            size_bytes: file.size_bytes, copy_id: Some(CopyId(copy.0.key.id.0)) },
    }] }));
    root.entries.push(TreeEntry { name: "vendor".into(), value: TreeValue::Tree(TreeId(subtree.0.key.id.0)) });
    root.entries.sort_by(|a,b|a.name.cmp(&b.name));
    assert!(root.entries.len() > 128);
    let root = encoded(Object::Tree(root));
    source.parents = vec![CommitId(projection.projected_commit)];
    source.predecessors.clear();
    source.root_tree = TreeMerge { terms: vec![TreeId(root.0.key.id.0)], labels: vec![String::new()] };
    source.change_id = ChangeId([93;16]);
    source.description = "Authored scoped fixture source".into();
    let authored = encoded(Object::Commit(source.clone()));
    let leaked = encoded(Object::Tree(Tree { entries: vec![TreeEntry {
        name: "leak.txt".into(), value: TreeValue::File { id: FileId(private_file.key.id.0), executable: false,
            size_bytes: private_file.size_bytes, copy_id: None },
    }] }));
    let mut raw_parent = source.clone(); raw_parent.parents = vec![view.revision()];
    let raw_parent = encoded(Object::Commit(raw_parent));
    let mut root_predecessor = source; root_predecessor.predecessors = vec![CommitId::ROOT];
    let root_predecessor = encoded(Object::Commit(root_predecessor));
    let wrong_copy = encoded(Object::CopyHistory(CopyHistory { current_path: "elsewhere.txt".into(), parents: vec![], salt: b"upload-wrong-copy-fixture".to_vec() }));
    let inputs = vec![
        ("new-file", "vendor/copied.txt", (file,file_bytes), true),
        ("new-copy", "vendor/copied.txt", copy, true),
        ("nested-tree", "vendor", subtree.clone(), true),
        ("wide-root-tree", "", root, true),
        ("source-commit", "", authored, true),
        ("nested-tree-wrong-empty-context", "", subtree, false),
        ("known-private-file", "", leaked, false),
        ("raw-canonical-parent", "", raw_parent, false),
        ("intrinsic-root-predecessor", "", root_predecessor, false),
        ("wrong-copy-current-path", "vendor/copied.txt", wrong_copy, false),
    ];
    let mut cases = Vec::new();
    for (name,path,(descriptor,bytes),success) in inputs {
        let request_id = format!("native-upload-{name}");
        let begin = store.forge_begin_upload(view,actor,&request_id,path,descriptor,0).unwrap();
        let mut offset = 0;
        for chunk in bytes.chunks(64*1024) {
            let status = store.forge_append_upload(account,actor,"",begin.session_id,offset,chunk,0).unwrap();
            offset += chunk.len() as u64; assert_eq!(status.offset,offset);
        }
        let result = store.forge_finalize_upload(account,actor,"",begin.session_id,0);
        assert_eq!(result.is_ok(),success,"native upload case {name}: {result:?}");
        let expected = match result {
            Ok(status) => {
                let receipt = status.receipt.as_ref().expect("native committed receipt");
                assert_eq!(status.offset,descriptor.size_bytes);
                assert_eq!(receipt.id,descriptor.key.id.0);
                assert_eq!(receipt.kind,descriptor.key.kind as u8);
                assert_eq!(receipt.size_bytes,descriptor.size_bytes);
                assert_eq!(receipt.format_version,descriptor.format_version);
                let replay = store.forge_finalize_upload(account,actor,"",begin.session_id,0).unwrap();
                assert_eq!(serde_json::to_value(replay).unwrap(),serde_json::to_value(&status).unwrap());
                json!({"state":"committed","offset":status.offset,"receipt":{"kind":receipt.kind,"id":receipt.id,
                    "size_bytes":receipt.size_bytes,"format_version":receipt.format_version}})
            }
            Err(error) => {
                let code = match error {
                    Error::ForgeAccessDenied => "permission_denied",
                    Error::Invalid(_) | Error::Model(_) => "invalid_argument",
                    Error::Limit(_) => "resource_exhausted",
                    other => panic!("unexpected native upload error {name}: {other:?}"),
                };
                json!({"state":"error","code":code,"stage":"finalize"})
            }
        };
        cases.push(json!({"name":name,"path":path,"request_id":request_id,"kind":descriptor.key.kind as u8,
            "id":hex(&descriptor.key.id.0),"size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version,
            "bytes":bytes,"session_id":begin.session_id,"expected":expected}));
    }
    let after = store.references(&repository, &["main".into()]).unwrap().remove(0);
    assert_eq!(before.generation,after.generation); assert_eq!(before.commit,after.commit);
    assert_eq!(store.forge_view_files(view,1000).unwrap().len(),701);
    json!({"view_version":view.version().0,"policy_epoch":view.epoch().0,"projected_base":projection.projected_commit,
        "expected_wide_file_count":701,"cases":cases})
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


// Disposable, genuinely executable Bazel repository for the trusted checker
// process acceptance. This fixture supplies bytes/identities, never a verdict.
fn checker_fixture(store: &Store, profile_version: &str) -> Value {
    let account = AccountId("forge-check".into());
    let actor = ForgeActor { principal: "check-owner".into() };
    store.forge_mutate_account(&account, &actor, "checker-provision", ForgeAccountMutation::Provision).unwrap();
    let repository = account.repository_id();
    let mut objects = Vec::new();
    let mut put = |value: Option<Object>, body: &[u8]| {
        let (descriptor, bytes) = match value {
            Some(value) => { let encoded = tinytree_model::encode(&value, Limits::default()).unwrap(); (encoded.descriptor, encoded.bytes) }
            None => (tinytree_model::describe_file(body), body.to_vec()),
        };
        store.put(&repository, descriptor, &bytes, 0).unwrap();
        objects.push(json!({"kind":descriptor.key.kind as u8,"id":hex(&descriptor.key.id.0),"size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version,"bytes":bytes}));
        descriptor
    };
    let rule = br##"def _smoke(ctx):
    executable = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(executable, """#!/bin/sh
set -eu
[ -z "${TT_CHECKER_TOKEN+x}" ] || exit 97
[ -z "${TT_FORGE_TOKEN+x}" ] || exit 98
IFS= read -r code < "$TEST_SRCDIR/$TEST_WORKSPACE/status.txt"
exit "$code"
""", is_executable = True)
    return [DefaultInfo(executable = executable, runfiles = ctx.runfiles(files = [ctx.file.src]))]

smoke_test = rule(implementation = _smoke, attrs = {"src": attr.label(allow_single_file = True)}, test = True)
"##;
    let files: Vec<(&str, &[u8])> = vec![
        ("BUILD.bazel", b"load(\":smoke.bzl\", \"smoke_test\")\nsmoke_test(name = \"smoke\", src = \"status.txt\")\n"),
        ("MODULE.bazel", b"module(name = \"checker_fixture\", version = \"0.0.0\")\n"),
        ("README.md", b"# Disposable checked source\nActual Bazel test execution, not a synthetic verdict.\n"),
        ("smoke.bzl", rule), ("status.txt", b"0\n"),
    ];
    let mut canonical = Tree { entries: files.into_iter().map(|(name, body)| {
        let descriptor = put(None, body);
        TreeEntry { name: name.into(), value: TreeValue::File { id: FileId(descriptor.key.id.0), size_bytes: descriptor.size_bytes, executable: false, copy_id: None } }
    }).collect() };
    let secret = put(None, b"excluded canonical checker fixture bytes\n");
    let private = put(Some(Object::Tree(Tree { entries: vec![TreeEntry { name: "secret.txt".into(), value: TreeValue::File { id: FileId(secret.key.id.0), size_bytes: secret.size_bytes, executable: false, copy_id: None } }] })), &[]);
    canonical.entries.push(TreeEntry { name: "private".into(), value: TreeValue::Tree(TreeId(private.key.id.0)) });
    canonical.entries.sort_by(|a,b| a.name.cmp(&b.name));
    let tree = put(Some(Object::Tree(canonical.clone())), &[]);
    let signature = Signature { name: "Checker Fixture".into(), email: "fixture@example.invalid".into(), timestamp: Timestamp { millis_since_epoch: 0, tz_offset_minutes: 0 } };
    let source = Commit { parents: vec![CommitId::ROOT], predecessors: vec![], root_tree: TreeMerge { terms: vec![TreeId(tree.key.id.0)], labels: vec![String::new()] }, change_id: ChangeId([150;16]), description: "Disposable checker source".into(), author: signature.clone(), committer: signature, secure_signature: None };
    let root = put(Some(Object::Commit(source)), &[]);
    drop(put);
    let spec = ViewSpec { name: "public".into(), root_prefix: String::new(), include_prefixes: vec![String::new()], exclude_prefixes: vec!["private".into()], visibility: ForgeViewVisibility::Public, revision: root.key.id.0, export_allowed: false };
    store.forge_mutate_account(&account, &actor, "checker-view", ForgeAccountMutation::PublishView { spec: spec.clone(), expected_epoch: PolicyEpoch(1), make_default: true, follow_main: false }).unwrap();
    let view = store.forge_resolve_view(&account, Some(&actor), "public", None).unwrap();
    let projection = store.forge_project_view(&view, 0).unwrap();
    let (_, bytes) = store.read_metadata(&repository, tinytree_model::ObjectKey { kind: Kind::Tree, id: tinytree_model::ObjectId(projection.projected_tree) }).unwrap();
    let Object::Tree(projected_tree) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else { panic!("checker projected tree") };
    let (_, bytes) = store.read_metadata(&repository, tinytree_model::ObjectKey { kind: Kind::Commit, id: tinytree_model::ObjectId(projection.projected_commit) }).unwrap();
    let Object::Commit(projected_commit) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else { panic!("checker projected commit") };
    let profile = tinytree_store::LandingCheckProfile { name: "checks".into(), version: profile_version.into() };
    let mut reviews = Vec::new();
    let mut cancellation = None;
    let mut expected = canonical;
    for (label, path, body, change) in [("positive", "receipt.txt", b"real checked source receipt\n".as_slice(), 151), ("negative", "status.txt", b"1\n".as_slice(), 152)] {
        let file = tinytree_model::describe_file(body);
        let mut selected = projected_tree.clone();
        if let Some(entry) = selected.entries.iter_mut().find(|entry| entry.name == path) {
            let TreeValue::File { id, size_bytes, .. } = &mut entry.value else { panic!("checker edit is a regular FILE") };
            *id = FileId(file.key.id.0); *size_bytes = file.size_bytes;
        } else { selected.entries.push(TreeEntry { name: path.into(), value: TreeValue::File { id: FileId(file.key.id.0), size_bytes: file.size_bytes, executable: false, copy_id: None } }); }
        selected.entries.sort_by(|a,b| a.name.cmp(&b.name));
        let tree = tinytree_model::encode(&Object::Tree(selected), Limits::default()).unwrap();
        let mut commit = projected_commit.clone();
        commit.parents = vec![CommitId(projection.projected_commit)]; commit.predecessors.clear();
        commit.root_tree.terms = vec![TreeId(tree.descriptor.key.id.0)]; commit.change_id = ChangeId([change;16]); commit.description = format!("Actual {label} Bazel check");
        let mut cancellation_commit = commit.clone();
        cancellation_commit.description = "Metadata-only cancellation acceptance amendment".into();
        let commit = tinytree_model::encode(&Object::Commit(commit), Limits::default()).unwrap();
        let mut uploads = Vec::new();
        for (path, descriptor, bytes) in [(path, file, body.to_vec()), ("", tree.descriptor, tree.bytes), ("", commit.descriptor, commit.bytes)] {
            let request_id = format!("checker-{label}-upload-{}", descriptor.key.kind as u8);
            let begun = store.forge_begin_upload(&view, &actor, &request_id, path, descriptor, 0).unwrap();
            let mut offset = 0;
            for chunk in bytes.chunks(65536) { store.forge_append_upload(&account, &actor, "public", begun.session_id, offset, chunk, 0).unwrap(); offset += chunk.len() as u64; }
            let finished = store.forge_finalize_upload(&account, &actor, "public", begun.session_id, 0).unwrap();
            assert!(finished.receipt.is_some());
            uploads.push(json!({"path":path,"kind":descriptor.key.kind as u8,"id":hex(&descriptor.key.id.0),"size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version,"bytes":bytes,"request_id":request_id}));
        }
        let request = tinytree_store::ForgeReviewPushRequest { request_id: format!("checker-{label}-push"), source_base: CommitId(projection.projected_commit), source_head: CommitId(commit.descriptor.key.id.0), view_version: view.version().0, expected_patchset: None, predecessor: None, provenance: None, lineage: vec![], check_profile: profile.clone() };
        let review = store.forge_push_review(&account, &actor, &request, &projected_commit.committer, 1).unwrap();
        if let Some(entry) = expected.entries.iter_mut().find(|entry| entry.name == path) {
            entry.value = TreeValue::File { id: FileId(file.key.id.0), size_bytes: file.size_bytes, executable: false, copy_id: None };
        } else { expected.entries.push(TreeEntry { name: path.into(), value: TreeValue::File { id: FileId(file.key.id.0), size_bytes: file.size_bytes, executable: false, copy_id: None } }); }
        expected.entries.sort_by(|a,b| a.name.cmp(&b.name));
        let expected = tinytree_model::encode(&Object::Tree(expected.clone()), Limits::default()).unwrap();
        reviews.push(json!({"label":label,"objects":uploads,"request_id":request.request_id,"change_id":hex(&[change;16]),"patchset_id":hex(&review.patchset_id),"expected_canonical_tree":hex(&expected.descriptor.key.id.0),"push":{"source_base":hex(&projection.projected_commit),"source_head":hex(&request.source_head.0),"view_version":hex(&view.version().0),"expected_patchset":null,"predecessor":null,"provenance":null,"lineage":[]},"native_admission_only":true}));
        if label == "negative" {
            // Same logical change/tree: only immutable commit metadata changes.
            // This is an explicit failed-patchset amendment, not a lease retry
            // on an ineligible already-Failed candidate or a synthetic busy test.
            let amended = tinytree_model::encode(&Object::Commit(cancellation_commit), Limits::default()).unwrap();
            let request_id = "checker-cancellation-upload-3";
            let begun = store.forge_begin_upload(&view, &actor, request_id, "", amended.descriptor, 0).unwrap();
            let mut offset = 0;
            for chunk in amended.bytes.chunks(65536) { store.forge_append_upload(&account, &actor, "public", begun.session_id, offset, chunk, 0).unwrap(); offset += chunk.len() as u64; }
            assert!(store.forge_finalize_upload(&account, &actor, "public", begun.session_id, 0).unwrap().receipt.is_some());
            let amendment = tinytree_store::ForgeReviewPushRequest { request_id: "checker-cancellation-push".into(), source_base: request.source_base,
                source_head: CommitId(amended.descriptor.key.id.0), view_version: request.view_version, expected_patchset: Some(review.patchset_id),
                predecessor: None, provenance: None, lineage: vec![], check_profile: profile.clone() };
            let admitted = store.forge_push_review(&account, &actor, &amendment, &projected_commit.committer, 1).unwrap();
            assert_eq!(admitted.change_id, review.change_id); assert_ne!(admitted.patchset_id, review.patchset_id);
            cancellation = Some(json!({"label":"cancellation","metadata_only":true,"objects":[{"path":"","kind":3,"id":hex(&amended.descriptor.key.id.0),"size_bytes":amended.descriptor.size_bytes,"format_version":amended.descriptor.format_version,"bytes":amended.bytes,"request_id":request_id}],
                "request_id":amendment.request_id,"change_id":hex(&[change;16]),"patchset_id":hex(&admitted.patchset_id),"expected_canonical_tree":hex(&expected.descriptor.key.id.0),
                "push":{"source_base":hex(&amendment.source_base.0),"source_head":hex(&amendment.source_head.0),"view_version":hex(&amendment.view_version),"expected_patchset":hex(&review.patchset_id),"predecessor":null,"provenance":null,"lineage":[]}}));
        }
    }
    json!({"account_id":account.0,"owner":actor.principal,"objects":objects,"root":{"kind":3,"id":hex(&root.key.id.0)},"spec":spec,"view_version":view.version().0,"projected_base":hex(&projection.projected_commit),"profile":profile,"private_file":{"kind":1,"id":hex(&secret.key.id.0)},"reviews":reviews,"cancellation":cancellation.unwrap()})
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let checker_profile = match arguments.next() {
        None => hex(&tinytree_model::file_id(b"checker-acceptance-default-profile").0),
        Some(argument) => {
            assert_eq!(argument, "--checker-profile-version", "unknown fixture argument");
            let value = arguments.next().expect("checker profile version required");
            assert!(value.len() == 64 && value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)), "invalid checker profile version");
            assert!(arguments.next().is_none(), "unexpected fixture arguments"); value
        }
    };
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
    let checker_fixture = checker_fixture(&store, &checker_profile);
    let value = json!({"schema_version":1,"account":"example","repository":REPOSITORY,
        "projection":projection_fixture,
        "bootstrap":bootstrap_fixture,
        "search":search_fixture,
        "checker":checker_fixture,
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
