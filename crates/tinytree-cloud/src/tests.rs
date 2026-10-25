use super::*;
use jj_lib::{
    backend::{MillisSinceEpoch, Timestamp},
    content_hash::blake2b_hash,
    op_store::{
        OperationMetadata, RemoteRef, RemoteRefState, RemoteView, RootOperationData, TimestampRange,
    },
};
use std::{collections::BTreeMap, path::Path};
use tinytree_model as m;

fn commit(byte: u8) -> CommitId {
    CommitId::from_bytes(&[byte; 32])
}
fn init(root: &Path) -> SimpleOpStore {
    std::fs::create_dir(root).unwrap();
    SimpleOpStore::init(
        root,
        RootOperationData {
            root_commit_id: commit(0),
        },
    )
    .unwrap()
}
fn rich_view() -> View {
    let mut view = View::make_root(commit(1));
    view.head_ids.insert(commit(2));
    view.local_bookmarks.insert(
        "feature".into(),
        RefTarget::from_removes_adds([Some(commit(3))], [Some(commit(4)), Some(commit(5))]),
    );
    view.local_tags
        .insert("release".into(), RefTarget::normal(commit(6)));
    view.git_refs
        .insert("refs/notes/review".into(), RefTarget::normal(commit(7)));
    view.git_heads
        .insert("task".into(), RefTarget::normal(commit(8)));
    view.wc_commit_ids.insert("task".into(), commit(9));
    view.remote_views.insert(
        "origin".into(),
        RemoteView {
            bookmarks: BTreeMap::from([
                (
                    "feature".into(),
                    RemoteRef {
                        target: RefTarget::normal(commit(10)),
                        state: RemoteRefState::Tracked,
                    },
                ),
                ("deleted".into(), RemoteRef::absent()),
            ]),
            tags: BTreeMap::from([(
                "release".into(),
                RemoteRef {
                    target: RefTarget::normal(commit(11)),
                    state: RemoteRefState::New,
                },
            )]),
        },
    );
    view
}
fn operation(
    view_id: ViewId,
    predecessors: Option<BTreeMap<CommitId, Vec<CommitId>>>,
) -> Operation {
    Operation {
        view_id,
        parents: vec![OperationId::from_bytes(&[0; 64])],
        metadata: OperationMetadata {
            time: TimestampRange {
                start: Timestamp {
                    timestamp: MillisSinceEpoch(1_700_000_000_000),
                    tz_offset: 60,
                },
                end: Timestamp {
                    timestamp: MillisSinceEpoch(1_700_000_000_123),
                    tz_offset: -60,
                },
            },
            description: "snapshot\nretain exact operation metadata".into(),
            hostname: "agent-host".into(),
            username: "agent".into(),
            is_snapshot: true,
            workspace_name: Some("task".into()),
            attributes: BTreeMap::from([("command".into(), "snapshot".into())]),
        },
        commit_predecessors: predecessors,
    }
}

#[tokio::test]
async fn real_stores_preserve_views_operations_and_all_predecessor_states() {
    let dir = tempfile::tempdir().unwrap();
    let source = init(&dir.path().join("source"));
    let target_path = dir.path().join("target");
    let target = init(&target_path);
    let expected_view = rich_view();
    let view_id = source.write_view(&expected_view).await.unwrap();
    let view = export_view(&source, &view_id, Limits::default())
        .await
        .unwrap();
    assert_eq!(view.view(), Some(&expected_view));
    assert_eq!(
        view.id(),
        ObjectId::View(array(view_id.as_bytes()).unwrap())
    );
    assert_eq!(
        view.dependencies().native_commits,
        (1..=11).map(|n| [n; 32]).collect::<Vec<_>>()
    );
    assert_eq!(view.import(&target).await.unwrap(), view.id());
    let mut identities = Vec::new();
    for (name, predecessors, expected_native) in [
        ("predecessors not recorded", None, vec![]),
        (
            "recorded empty predecessor map",
            Some(BTreeMap::new()),
            vec![],
        ),
        (
            "rewrites and a new commit",
            Some(BTreeMap::from([
                (commit(12), vec![commit(13), commit(14)]),
                (commit(15), vec![]),
            ])),
            vec![[12; 32], [13; 32], [14; 32], [15; 32]],
        ),
    ] {
        let expected = operation(view_id.clone(), predecessors);
        let id = source.write_operation(&expected).await.unwrap();
        let exported = export_operation(&source, &id, Limits::default())
            .await
            .unwrap();
        assert_eq!(exported.operation(), Some(&expected), "{name}");
        assert_eq!(
            exported.dependencies().native_commits,
            expected_native,
            "{name}"
        );
        assert_eq!(exported.dependencies().operations, vec![[0; 64]], "{name}");
        assert_eq!(
            exported.dependencies().views,
            vec![array(view_id.as_bytes()).unwrap()],
            "{name}"
        );
        let decoded = VerifiedObject::verify(
            exported.id(),
            exported.digest().unwrap(),
            exported.payload().unwrap(),
            Limits::default(),
        )
        .unwrap();
        assert_eq!(decoded.operation(), Some(&expected), "{name}");
        assert_eq!(
            decoded.import(&target).await.unwrap(),
            exported.id(),
            "{name}"
        );
        identities.push((id, expected));
    }
    assert!(identities[0].0 != identities[1].0 && identities[1].0 != identities[2].0);
    assert_eq!(view.dependencies().native_commits.len(), 11);
    drop(target);
    let reopened = SimpleOpStore::load(
        &target_path,
        RootOperationData {
            root_commit_id: commit(0),
        },
    );
    assert_eq!(reopened.read_view(&view_id).await.unwrap(), expected_view);
    for (id, expected) in identities {
        assert_eq!(reopened.read_operation(&id).await.unwrap(), expected);
    }
}

#[tokio::test]
async fn retained_admission_charge_covers_rich_decoded_objects_and_intrinsics() {
    let dir = tempfile::tempdir().unwrap();
    let store = init(&dir.path().join("source"));
    let small_id = store.write_view(&rich_view()).await.unwrap();
    let small = export_view(&store, &small_id, Limits::default())
        .await
        .unwrap();
    let mut expanded_view = rich_view();
    for index in 0..100 {
        expanded_view.local_bookmarks.insert(
            format!("bookmark-{index}-{}", "x".repeat(1024)).into(),
            RefTarget::from_removes_adds([Some(commit(1))], [Some(commit(2)), Some(commit(3))]),
        );
    }
    let expanded_id = store.write_view(&expanded_view).await.unwrap();
    let expanded = export_view(&store, &expanded_id, Limits::default())
        .await
        .unwrap();
    let view_charge = expanded.retained_size_upper_bound().unwrap();
    assert!(view_charge > small.retained_size_upper_bound().unwrap());
    assert!(
        view_charge > std::mem::size_of::<VerifiedObject>() + expanded.payload().unwrap().len()
    );
    let mut operation = operation(
        expanded_id,
        Some(BTreeMap::from([
            (commit(12), vec![commit(13), commit(14)]),
            (commit(15), vec![]),
        ])),
    );
    operation.metadata.description = "snapshot ".repeat(8192);
    operation
        .metadata
        .attributes
        .insert("detail".into(), "x".repeat(4096));
    let id = store.write_operation(&operation).await.unwrap();
    let object = export_operation(&store, &id, Limits::default())
        .await
        .unwrap();
    let charge = object.retained_size_upper_bound().unwrap();
    assert!(
        charge
            > std::mem::size_of::<VerifiedObject>()
                + object.payload().unwrap().len()
                + operation.metadata.description.len()
                + operation.metadata.attributes["detail"].len()
    );
    let decoded = VerifiedObject::verify(
        object.id(),
        object.digest().unwrap(),
        object.payload().unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(decoded.retained_size_upper_bound().unwrap(), charge);
    for id in [ObjectId::Operation([0; 64]), ObjectId::View([0; 64])] {
        let intrinsic = VerifiedObject::intrinsic(id).unwrap();
        assert!(
            intrinsic.retained_size_upper_bound().unwrap() >= std::mem::size_of::<VerifiedObject>()
        );
    }
}

#[tokio::test]
async fn raw_and_semantic_identity_checks_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    let store = init(&dir.path().join("source"));
    let view_id = store.write_view(&rich_view()).await.unwrap();
    let id = store
        .write_operation(&operation(
            view_id,
            Some(BTreeMap::from([(commit(1), vec![commit(2)])])),
        ))
        .await
        .unwrap();
    let exported = export_operation(&store, &id, Limits::default())
        .await
        .unwrap();
    let bytes = exported.payload().unwrap();
    let digest = exported.digest().unwrap();
    // Legal protobuf alternate encoding: a large singular view_id is replaced
    // by the canonical final value. Prost retains the first field's capacity
    // even though the verified semantic identity and final ID width are exact.
    let overwritten_bytes = 128 * 1024;
    let mut alternate = vec![0x0a]; // Operation.view_id, length-delimited.
    let mut length = overwritten_bytes;
    while length >= 128 {
        alternate.push((length as u8 & 0x7f) | 0x80);
        length >>= 7;
    }
    alternate.push(length as u8);
    alternate.resize(alternate.len() + overwritten_bytes, 1);
    alternate.extend_from_slice(bytes);
    let alternate_object = VerifiedObject::verify(
        exported.id(),
        BlobDigest::of(&alternate),
        &alternate,
        Limits::default(),
    )
    .unwrap();
    assert_eq!(alternate_object.operation(), exported.operation());
    assert!(
        alternate_object.retained_size_upper_bound().unwrap()
            >= std::mem::size_of::<VerifiedObject>() + alternate.len() + overwritten_bytes
    );
    assert_ne!(&digest.blake3[..], &exported.id().bytes()[..]);
    let mut wrong_digest = digest;
    wrong_digest.size_bytes += 1;
    assert!(VerifiedObject::verify(exported.id(), wrong_digest, bytes, Limits::default()).is_err());
    assert!(
        VerifiedObject::verify(
            ObjectId::Operation([9; 64]),
            digest,
            bytes,
            Limits::default()
        )
        .is_err()
    );
    assert!(
        VerifiedObject::verify(
            ObjectId::View(*exported.id().bytes()),
            digest,
            bytes,
            Limits::default()
        )
        .is_err()
    );
    assert!(
        VerifiedObject::verify(
            ObjectId::Operation([0; 64]),
            digest,
            bytes,
            Limits::default()
        )
        .is_err()
    );
    for corrupted in [
        bytes[..bytes.len() - 1].to_vec(),
        [bytes, &[0]].concat(),
        [bytes, &[0x28, 0]].concat(),
    ] {
        assert!(
            VerifiedObject::verify(
                exported.id(),
                BlobDigest::of(&corrupted),
                &corrupted,
                Limits::default()
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn malformed_shapes_and_limits_fail_before_import() {
    let dir = tempfile::tempdir().unwrap();
    let store = init(&dir.path().join("source"));
    let view_id = store.write_view(&rich_view()).await.unwrap();
    let id = store
        .write_operation(&operation(view_id, None))
        .await
        .unwrap();
    let object = export_operation(&store, &id, Limits::default())
        .await
        .unwrap();
    let bytes = object.payload().unwrap();
    for (name, limits) in [
        (
            "payload bytes",
            Limits {
                max_payload_bytes: bytes.len() - 1,
                max_field_bytes: 1,
                ..Limits::default()
            },
        ),
        (
            "predecode fields",
            Limits {
                max_fields: 1,
                ..Limits::default()
            },
        ),
        (
            "field bytes",
            Limits {
                max_field_bytes: 8,
                ..Limits::default()
            },
        ),
        (
            "decode allocation work",
            Limits {
                max_decode_bytes: 32,
                ..Limits::default()
            },
        ),
        (
            "foreign backend width",
            Limits {
                commit_id_bytes: 20,
                ..Limits::default()
            },
        ),
    ] {
        assert!(
            VerifiedObject::verify(object.id(), object.digest().unwrap(), bytes, limits).is_err(),
            "{name}"
        );
    }
    // A deleted local bookmark must be absent from the map. The upstream View
    // setter removes it; constructing one by hand cannot round-trip its hash.
    let mut noncanonical = rich_view();
    noncanonical
        .local_bookmarks
        .insert("deleted".into(), RefTarget::absent());
    let noncanonical_id = store.write_view(&noncanonical).await.unwrap();
    assert!(
        export_view(&store, &noncanonical_id, Limits::default())
            .await
            .is_err()
    );
    // A present but empty RefTarget once reached an infallible conversion path.
    let mut malformed = vec![0x0a, 32];
    malformed.extend([1; 32]);
    malformed.extend([0x2a, 7, 0x0a, 3, b'b', b'a', b'd', 0x12, 0]);
    assert!(
        VerifiedObject::verify(
            ObjectId::View([8; 64]),
            BlobDigest::of(&malformed),
            &malformed,
            Limits::default()
        )
        .is_err()
    );
}

#[tokio::test]
async fn intrinsic_roots_are_explicit_and_non_native_stores_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = init(&dir.path().join("native"));
    for id in [ObjectId::Operation([0; 64]), ObjectId::View([0; 64])] {
        let intrinsic = VerifiedObject::intrinsic(id).unwrap();
        assert_eq!(intrinsic.payload(), None);
        assert_eq!(intrinsic.digest(), None);
        assert_eq!(intrinsic.import(&store).await.unwrap(), id);
    }
    assert!(VerifiedObject::intrinsic(ObjectId::Operation([1; 64])).is_err());
    assert!(
        export_operation(
            &store,
            &OperationId::from_bytes(&[0; 64]),
            Limits {
                max_fields: 0,
                ..Limits::default()
            }
        )
        .await
        .is_err()
    );
    let foreign_path = dir.path().join("foreign");
    std::fs::create_dir(&foreign_path).unwrap();
    let foreign = SimpleOpStore::init(
        &foreign_path,
        RootOperationData {
            root_commit_id: CommitId::from_bytes(&[0; 20]),
        },
    )
    .unwrap();
    assert!(
        export_view(&foreign, &ViewId::from_bytes(&[0; 64]), Limits::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn corrupt_stored_identity_and_unroundtrippable_legacy_operation_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source");
    let store = init(&path);
    let view = store.write_view(&rich_view()).await.unwrap();
    let original = operation(view.clone(), None);
    let id = store.write_operation(&original).await.unwrap();
    let exported = export_operation(&store, &id, Limits::default())
        .await
        .unwrap();
    let target_path = dir.path().join("target");
    let target_store = init(&target_path);
    exported.import(&target_store).await.unwrap();
    std::fs::write(
        target_path.join("operations").join(id.hex()),
        b"corrupt existing object",
    )
    .unwrap();
    assert!(
        exported.import(&target_store).await.is_err(),
        "existing corrupt import must not be acknowledged"
    );
    assert_eq!(
        std::fs::read(target_path.join("operations").join(id.hex())).unwrap(),
        b"corrupt existing object"
    );

    let other = operation(view, Some(BTreeMap::new()));
    let other_id = store.write_operation(&other).await.unwrap();
    let source = path.join("operations").join(other_id.hex());
    let target = path.join("operations").join(id.hex());
    std::fs::copy(source, target).unwrap();
    assert!(
        export_operation(&store, &id, Limits::default())
            .await
            .is_err()
    );
    let mut legacy = Operation::make_root(ViewId::from_bytes(&[7; 64]));
    legacy.commit_predecessors = None;
    let legacy_id = ObjectId::Operation(array(&blake2b_hash(&legacy)).unwrap());
    let mut bytes = vec![0x0a, 64];
    bytes.extend([7; 64]);
    assert!(
        VerifiedObject::verify(legacy_id, BlobDigest::of(&bytes), &bytes, Limits::default())
            .is_err()
    );
}

fn native_key(kind: tinytree_model::Kind, byte: u8) -> tinytree_model::ObjectKey {
    tinytree_model::ObjectKey {
        kind,
        id: tinytree_model::ObjectId([byte; 32]),
    }
}
fn manifest_roots(
    objects: &[VerifiedObject],
    wc: &WorkingCopy,
    intents: &[IntentRoots],
) -> Vec<tinytree_model::ObjectKey> {
    use tinytree_model as m;
    let mut keys = BTreeSet::new();
    for object in objects {
        for id in &object.dependencies().native_commits {
            if *id != [0; 32] {
                keys.insert((m::Kind::Commit as u8, *id));
            }
        }
    }
    for tree in &wc.tree.terms {
        keys.insert((m::Kind::Tree as u8, tree.0));
    }
    for intent in intents {
        for root in &intent.roots {
            keys.insert((root.kind as u8, root.id.0));
        }
    }
    keys.into_iter()
        .map(|(kind, id)| m::ObjectKey {
            kind: m::Kind::try_from(kind).unwrap(),
            id: m::ObjectId(id),
        })
        .collect()
}
async fn manifest_fixture() -> (Manifest, Vec<VerifiedObject>) {
    use tinytree_model as m;
    let dir = tempfile::tempdir().unwrap();
    let store = init(&dir.path().join("manifest-source"));
    let view_a = store.write_view(&rich_view()).await.unwrap();
    let id_a = store
        .write_operation(&operation(
            view_a.clone(),
            Some(BTreeMap::from([(commit(12), vec![commit(13), commit(0)])])),
        ))
        .await
        .unwrap();
    let mut changed = rich_view();
    changed.head_ids.insert(commit(16));
    let view_b = store.write_view(&changed).await.unwrap();
    let mut op_b = operation(view_b.clone(), Some(BTreeMap::new()));
    op_b.parents = vec![id_a.clone()];
    let id_b = store.write_operation(&op_b).await.unwrap();
    let mut op_c = operation(view_b.clone(), None);
    op_c.parents = vec![id_b.clone(), id_a.clone()];
    op_c.metadata.description = "merge private task histories".into();
    let id_c = store.write_operation(&op_c).await.unwrap();
    let mut objects = vec![
        export_view(&store, &view_a, Limits::default())
            .await
            .unwrap(),
        export_view(&store, &view_b, Limits::default())
            .await
            .unwrap(),
    ];
    for id in [&id_a, &id_b, &id_c] {
        objects.push(
            export_operation(&store, id, Limits::default())
                .await
                .unwrap(),
        );
    }
    let members = objects
        .iter()
        .map(|object| Member {
            id: object.id(),
            selected: object.digest().unwrap(),
        })
        .collect::<Vec<_>>();
    let wc = WorkingCopy {
        workspace_name: "task".into(),
        wc_operation: array(id_a.as_bytes()).unwrap(),
        snapshot_operation: array(id_c.as_bytes()).unwrap(),
        snapshot_commit: [9; 32],
        tree: m::TreeMerge {
            terms: vec![
                m::TreeId([21; 32]),
                m::TreeId([22; 32]),
                m::TreeId([23; 32]),
            ],
            labels: vec!["our side".into(), "base".into(), "their side".into()],
        },
    };
    let intent_roots = vec![
        IntentRoots {
            kind: IntentKind::Publication,
            id: [1; 32],
            roots: vec![native_key(m::Kind::File, 17), native_key(m::Kind::Tree, 18)],
        },
        IntentRoots {
            kind: IntentKind::Recovery,
            id: [2; 32],
            roots: vec![
                native_key(m::Kind::Symlink, 19),
                native_key(m::Kind::CopyHistory, 20),
            ],
        },
    ];
    let native_roots = manifest_roots(&objects, &wc, &intent_roots);
    let mut manifest = Manifest {
        workspace: [1; 32],
        exporter: [2; 16],
        sequence: 7,
        operation_heads: vec![array(id_c.as_bytes()).unwrap()],
        undo_operations: vec![array(id_a.as_bytes()).unwrap()],
        intent_operations: vec![array(id_b.as_bytes()).unwrap()],
        working_copy: wc,
        parents: vec![ManifestId([1; 32]), ManifestId([2; 32])],
        members,
        intent_roots,
        native_roots,
        coverage: Coverage::FullRequired,
        detached: vec![],
    };
    manifest.members.sort_by_key(|m| m.id);
    // Verification does not require the caller's decoded-object order to match.
    objects.reverse();
    (manifest, objects)
}
#[tokio::test]
async fn manifest_roundtrip_preserves_full_undo_graph_native_roots_and_tree_merge_order() {
    use tinytree_model as m;
    let (manifest, objects) = manifest_fixture().await;
    let limits = ManifestLimits::default();
    let bytes = encode_manifest(&manifest, limits).unwrap();
    let id = ManifestId::of(&bytes);
    assert_ne!(
        id.0,
        BlobDigest::of(&bytes).blake3,
        "manifest and raw blob hash domains remain distinct"
    );
    let decoded = decode_manifest(&bytes, id, limits).unwrap();
    assert_eq!(decoded, manifest);
    assert_eq!(encode_manifest(&decoded, limits).unwrap(), bytes);
    let checked = verify_coverage(&decoded, &objects, limits).unwrap();
    assert_eq!(checked.native_roots(), manifest.native_roots);
    assert_ne!(
        checked.working_copy().wc_operation,
        checked.working_copy().snapshot_operation
    );
    for byte in [3, 4, 5, 12, 13, 16] {
        assert!(
            checked
                .native_roots()
                .contains(&native_key(m::Kind::Commit, byte)),
            "negative conflict and predecessor terms must survive"
        );
    }
    for key in [
        native_key(m::Kind::File, 17),
        native_key(m::Kind::Tree, 22),
        native_key(m::Kind::Symlink, 19),
        native_key(m::Kind::CopyHistory, 20),
    ] {
        assert!(checked.native_roots().contains(&key));
    }
    assert!(
        !checked
            .native_roots()
            .contains(&native_key(m::Kind::Commit, 0))
    );
    let mut reordered = manifest.clone();
    reordered.working_copy.tree.terms.swap(0, 2);
    reordered.working_copy.tree.labels.swap(0, 2);
    let other = encode_manifest(&reordered, limits).unwrap();
    assert_ne!(ManifestId::of(&other), id);
    assert_eq!(
        decode_manifest(&other, ManifestId::of(&other), limits)
            .unwrap()
            .working_copy
            .tree,
        reordered.working_copy.tree
    );
    verify_coverage(&reordered, &objects, limits).unwrap();
    let mut repeated = manifest.clone();
    repeated.working_copy.tree.terms[2] = repeated.working_copy.tree.terms[0];
    repeated.native_roots =
        manifest_roots(&objects, &repeated.working_copy, &repeated.intent_roots);
    let encoded = encode_manifest(&repeated, limits).unwrap();
    assert_eq!(
        decode_manifest(&encoded, ManifestId::of(&encoded), limits)
            .unwrap()
            .working_copy
            .tree,
        repeated.working_copy.tree
    );
    verify_coverage(&repeated, &objects, limits).unwrap();
}
#[tokio::test]
async fn manifest_coverage_rejects_omitted_dependencies_wrong_representations_and_unreachable_members()
 {
    let (manifest, objects) = manifest_fixture().await;
    let limits = ManifestLimits::default();
    for omitted in [
        ObjectId::Operation(manifest.working_copy.wc_operation),
        objects
            .iter()
            .find(|o| matches!(o.id(), ObjectId::View(_)))
            .unwrap()
            .id(),
    ] {
        let mut incomplete = manifest.clone();
        incomplete.members.retain(|m| m.id != omitted);
        let borrowed = objects
            .iter()
            .filter(|o| o.id() != omitted)
            .collect::<Vec<_>>();
        // Reverify exported immutable bytes to form an independently owned list.
        let remaining = borrowed
            .into_iter()
            .map(|o| {
                VerifiedObject::verify(
                    o.id(),
                    o.digest().unwrap(),
                    o.payload().unwrap(),
                    Limits::default(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            verify_coverage(&incomplete, &remaining, limits),
            Err(Error::Invalid("required cloud object missing"))
        ));
    }
    let mut wrong = manifest.clone();
    wrong.members[0].selected.blake3[0] ^= 1;
    assert!(matches!(
        verify_coverage(&wrong, &objects, limits),
        Err(Error::Invalid("selected cloud representation mismatch"))
    ));
    let mut wrong = manifest.clone();
    wrong.working_copy.snapshot_commit = [88; 32];
    assert!(matches!(
        verify_coverage(&wrong, &objects, limits),
        Err(Error::Invalid("selected workspace snapshot mismatch"))
    ));
    let mut wrong = manifest.clone();
    wrong.intent_operations = vec![[99; 64]];
    assert!(verify_coverage(&wrong, &objects, limits).is_err());
    let dir = tempfile::tempdir().unwrap();
    let store = init(&dir.path().join("unreachable"));
    let mut unused = rich_view();
    unused.head_ids.insert(commit(99));
    let id = store.write_view(&unused).await.unwrap();
    let extra = export_view(&store, &id, Limits::default()).await.unwrap();
    let mut extra_objects = objects;
    extra_objects.push(extra);
    let mut wrong = manifest;
    wrong.members.push(Member {
        id: extra_objects.last().unwrap().id(),
        selected: extra_objects.last().unwrap().digest().unwrap(),
    });
    wrong.members.sort_by_key(|m| m.id);
    assert!(matches!(
        verify_coverage(&wrong, &extra_objects, limits),
        Err(Error::Invalid("unreachable cloud member"))
    ));
}
#[tokio::test]
async fn manifest_native_root_union_is_exact_for_conflicts_predecessors_tree_and_intent_roots() {
    use tinytree_model as m;
    let (manifest, objects) = manifest_fixture().await;
    let limits = ManifestLimits::default();
    for (name, key) in [
        ("negative conflict", native_key(m::Kind::Commit, 3)),
        ("rewrite predecessor", native_key(m::Kind::Commit, 13)),
        ("working-copy remove term", native_key(m::Kind::Tree, 22)),
        ("publication file", native_key(m::Kind::File, 17)),
        ("recovery copy", native_key(m::Kind::CopyHistory, 20)),
    ] {
        let mut wrong = manifest.clone();
        wrong.native_roots.retain(|r| *r != key);
        assert!(
            matches!(
                verify_coverage(&wrong, &objects, limits),
                Err(Error::Invalid("native root union mismatch"))
            ),
            "{name}"
        );
    }
    let mut wrong = manifest;
    wrong
        .native_roots
        .push(native_key(m::Kind::CopyHistory, 99));
    assert!(matches!(
        verify_coverage(&wrong, &objects, limits),
        Err(Error::Invalid("native root union mismatch"))
    ));
}
#[tokio::test]
async fn manifest_decoder_rejects_malformed_noncanonical_and_over_budget_inputs() {
    let (manifest, objects) = manifest_fixture().await;
    let limits = ManifestLimits::default();
    let bytes = encode_manifest(&manifest, limits).unwrap();
    assert!(decode_manifest(&bytes, ManifestId([0; 32]), limits).is_err());
    for end in 0..bytes.len() {
        let cut = &bytes[..end];
        assert!(
            decode_manifest(cut, ManifestId::of(cut), limits).is_err(),
            "truncated length {end}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(matches!(
        decode_manifest(&trailing, ManifestId::of(&trailing), limits),
        Err(Error::Invalid("trailing cloud manifest data"))
    ));
    let mut unknown = bytes.clone();
    unknown[8..10].copy_from_slice(&2u16.to_le_bytes());
    assert!(matches!(
        decode_manifest(&unknown, ManifestId::of(&unknown), limits),
        Err(Error::Unsupported(_))
    ));
    let mut giant = bytes.clone();
    let heads_count = 73 + CODEC.len();
    giant[heads_count..heads_count + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        decode_manifest(&giant, ManifestId::of(&giant), limits),
        Err(Error::Limit(_))
    ));
    let mut invalid_utf8 = bytes.clone();
    let name = invalid_utf8.windows(4).position(|v| v == b"task").unwrap();
    invalid_utf8[name] = 255;
    assert!(decode_manifest(&invalid_utf8, ManifestId::of(&invalid_utf8), limits).is_err());
    assert!(matches!(
        decode_manifest(
            &bytes,
            ManifestId::of(&bytes),
            ManifestLimits {
                max_decode_bytes: 1,
                ..limits
            }
        ),
        Err(Error::Limit(_))
    ));
    assert!(matches!(
        verify_coverage(
            &manifest,
            &objects,
            ManifestLimits {
                max_total_payload_bytes: 0,
                ..limits
            }
        ),
        Err(Error::Limit(_))
    ));
    assert!(matches!(
        verify_coverage(
            &manifest,
            &objects,
            ManifestLimits {
                max_members: 1,
                ..limits
            }
        ),
        Err(Error::Limit(_))
    ));
    assert!(matches!(
        verify_coverage(
            &manifest,
            &objects,
            ManifestLimits {
                max_edges: 1,
                ..limits
            }
        ),
        Err(Error::Limit(_))
    ));
    let mut unsorted = manifest.clone();
    unsorted.members.reverse();
    assert!(encode_manifest(&unsorted, limits).is_err());
    let mut duplicate = manifest.clone();
    duplicate.operation_heads.push(duplicate.operation_heads[0]);
    assert!(encode_manifest(&duplicate, limits).is_err());
    let mut unsorted = manifest.clone();
    unsorted.native_roots.reverse();
    assert!(encode_manifest(&unsorted, limits).is_err());
    let mut invalid = manifest.clone();
    invalid.working_copy.tree.terms.pop();
    assert!(encode_manifest(&invalid, limits).is_err());
    let mut invalid = manifest.clone();
    invalid.working_copy.workspace_name = "/not/a/portable/name".into();
    let error = encode_manifest(&invalid, limits).unwrap_err().to_string();
    assert!(!error.contains("/not/a"));
    let mut detached = manifest;
    detached.detached.push(DetachedComponent {
        roots: vec![[7; 64]],
        missing_parent_edges: vec![MissingParentEdge {
            from_operation: [7; 64],
            to_parent: [8; 64],
        }],
    });
    assert!(matches!(
        encode_manifest(&detached, limits),
        Err(Error::Unsupported("detached cloud components"))
    ));
    let mut detached_wire = bytes;
    let end = detached_wire.len();
    detached_wire[end - 4..].copy_from_slice(&1u32.to_le_bytes());
    assert!(matches!(
        decode_manifest(&detached_wire, ManifestId::of(&detached_wire), limits),
        Err(Error::Unsupported("detached cloud components"))
    ));
}
#[test]
fn intrinsic_cloud_checkpoint_has_no_stored_op_view_payload_but_retains_the_real_empty_tree() {
    use tinytree_model as m;
    let wc = WorkingCopy {
        workspace_name: "task".into(),
        wc_operation: [0; 64],
        snapshot_operation: [0; 64],
        snapshot_commit: [0; 32],
        tree: m::synthetic_root().root_tree,
    };
    let native_roots = manifest_roots(&[], &wc, &[]);
    let manifest = Manifest {
        workspace: [1; 32],
        exporter: [2; 16],
        sequence: 0,
        operation_heads: vec![[0; 64]],
        undo_operations: vec![],
        intent_operations: vec![],
        working_copy: wc,
        parents: vec![],
        members: vec![],
        intent_roots: vec![],
        native_roots,
        coverage: Coverage::FullRequired,
        detached: vec![],
    };
    let limits = ManifestLimits::default();
    let bytes = encode_manifest(&manifest, limits).unwrap();
    let decoded = decode_manifest(&bytes, ManifestId::of(&bytes), limits).unwrap();
    let checked = verify_coverage(&decoded, &[], limits).unwrap();
    assert_eq!(checked.native_roots(), &[m::empty_tree().descriptor.key]);
    let mut invalid = manifest;
    invalid.working_copy.snapshot_commit = [1; 32];
    assert!(verify_coverage(&invalid, &[], limits).is_err());
}

fn portable_index_fixture() -> PortableIndexSnapshot {
    let mut drafts = Vec::new();
    for description in ["hidden divergent draft", "promoted draft remains local"] {
        let mut commit = m::synthetic_root();
        commit.parents = vec![m::CommitId::ROOT];
        commit.change_id = m::ChangeId([9; 16]);
        commit.description = description.into();
        let encoded = m::encode(&m::Object::Commit(commit.clone()), Default::default()).unwrap();
        drafts.push(IndexDraft {
            descriptor: encoded.descriptor,
            change: commit.change_id,
            parents: commit.parents,
            order: 7,
        });
    }
    drafts.sort_by_key(IndexDraft::commit);
    PortableIndexSnapshot {
        roots: vec![m::CommitId::ROOT, drafts[1].commit()],
        drafts,
    }
}
#[test]
fn portable_index_roundtrip_preserves_exact_hidden_promoted_membership_and_shared_ordinals() {
    let original = portable_index_fixture();
    let bytes = encode_index_snapshot(&original, IndexSnapshotLimits::default()).unwrap();
    let id = IndexSnapshotId::of(&bytes);
    let verified = decode_index_snapshot(&bytes, id, IndexSnapshotLimits::default()).unwrap();
    assert_eq!(verified.snapshot(), &original);
    assert_eq!(verified.descriptor().selected, BlobDigest::of(&bytes));
    assert_ne!(
        id.0,
        BlobDigest::of(&bytes).blake3,
        "snapshot semantic domain differs from raw transfer digest"
    );
    assert_eq!(
        encode_index_snapshot(verified.snapshot(), IndexSnapshotLimits::default()).unwrap(),
        bytes
    );
    let mut reordered = original.clone();
    reordered.drafts[0].order = 8;
    assert_ne!(
        IndexSnapshotId::of(
            &encode_index_snapshot(&reordered, IndexSnapshotLimits::default()).unwrap()
        ),
        id
    );
    let mut removed = original;
    removed.drafts.remove(0);
    assert_ne!(
        IndexSnapshotId::of(
            &encode_index_snapshot(&removed, IndexSnapshotLimits::default()).unwrap()
        ),
        id
    );
}
#[test]
fn portable_index_rejects_tampering_noncanonical_graphs_and_declared_allocation_bounds() {
    let original = portable_index_fixture();
    let bytes = encode_index_snapshot(&original, IndexSnapshotLimits::default()).unwrap();
    let id = IndexSnapshotId::of(&bytes);
    let mut damaged = bytes.clone();
    *damaged.last_mut().unwrap() ^= 1;
    assert!(decode_index_snapshot(&damaged, id, IndexSnapshotLimits::default()).is_err());
    let mut count = bytes.clone();
    count[14..18].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(
        decode_index_snapshot(
            &count,
            IndexSnapshotId::of(&count),
            IndexSnapshotLimits::default()
        )
        .is_err()
    );
    let mut version = bytes.clone();
    version[8..10].copy_from_slice(&2u16.to_le_bytes());
    assert!(matches!(
        decode_index_snapshot(
            &version,
            IndexSnapshotId::of(&version),
            IndexSnapshotLimits::default()
        ),
        Err(Error::Unsupported(_))
    ));
    assert!(
        decode_index_snapshot(
            &bytes,
            id,
            IndexSnapshotLimits {
                max_decode_bytes: 1,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(
        encode_index_snapshot(
            &original,
            IndexSnapshotLimits {
                max_encoded_bytes: bytes.len() - 1,
                ..Default::default()
            }
        )
        .is_err()
    );
    let mut zero = original.clone();
    zero.drafts[0].order = 0;
    let mut duplicate = original.clone();
    duplicate.drafts.push(duplicate.drafts[0].clone());
    let mut cycle = original.clone();
    cycle.drafts[0].parents = vec![cycle.drafts[0].commit()];
    let mut unsorted = original.clone();
    unsorted.roots.reverse();
    let mut kind = original.clone();
    kind.drafts[0].descriptor.key.kind = m::Kind::Tree;
    let mut empty = original.clone();
    empty.roots.clear();
    for (name, value) in [
        ("zero ordinal", zero),
        ("duplicate draft", duplicate),
        ("local cycle", cycle),
        ("unsorted roots", unsorted),
        ("noncommit descriptor", kind),
        ("implicit intrinsic anchor", empty),
    ] {
        assert!(
            encode_index_snapshot(&value, IndexSnapshotLimits::default()).is_err(),
            "{name}"
        );
    }
}
fn index_artifact_fixture(
    models: &[&PortableIndexSnapshot],
) -> (Vec<IndexRootDescriptor>, Vec<VerifiedIndexArtifact>) {
    let limits = IndexArtifactLimits::default();
    let mut all = BTreeMap::new();
    let mut roots = Vec::new();
    for model in models {
        let encoded = build_index_artifacts(model, limits).unwrap();
        roots.push(encoded.root);
        for artifact in encoded.artifacts {
            if let Some(old) = all.insert(artifact.descriptor.id, artifact.bytes.clone()) {
                assert_eq!(old, artifact.bytes);
            }
        }
    }
    roots.sort_by_key(|d| d.id);
    roots.dedup();
    let artifacts = all
        .into_iter()
        .map(|(id, bytes)| {
            decode_index_artifact(
                &bytes,
                IndexArtifactDescriptor {
                    id,
                    selected: BlobDigest::of(&bytes),
                },
                limits,
            )
            .unwrap()
        })
        .collect();
    (roots, artifacts)
}
#[tokio::test]
async fn manifest_v2_binds_every_actual_operation_to_exact_index_and_retains_hidden_records() {
    let (recovery, objects) = manifest_fixture().await;
    let empty = PortableIndexSnapshot {
        roots: vec![m::CommitId::ROOT],
        drafts: vec![],
    };
    let full = portable_index_fixture();
    let limits = ManifestV2Limits::default();
    let empty_root = build_index_artifacts(&empty, limits.artifacts)
        .unwrap()
        .root;
    let full_root = build_index_artifacts(&full, limits.artifacts).unwrap().root;
    let (index_roots, artifacts) = index_artifact_fixture(&[&empty, &full]);
    let graph = verify_index_artifact_graph(&index_roots, &artifacts, limits.artifacts).unwrap();
    let mut bindings = vec![OperationIndex {
        operation: [0; 64],
        root: empty_root.id,
    }];
    bindings.extend(
        recovery
            .members
            .iter()
            .filter_map(|member| match member.id {
                ObjectId::Operation(id) => Some(OperationIndex {
                    operation: id,
                    root: full_root.id,
                }),
                _ => None,
            }),
    );
    bindings.sort_by_key(|b| b.operation);
    let mut value = ManifestV2 {
        recovery,
        operation_indexes: bindings,
        index_roots: index_roots.clone(),
    };
    value.recovery.native_roots.extend(
        full.roots
            .iter()
            .filter(|id| **id != m::CommitId::ROOT)
            .map(|id| m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(id.0),
            }),
    );
    value
        .recovery
        .native_roots
        .extend(full.drafts.iter().map(|d| d.descriptor.key));
    value
        .recovery
        .native_roots
        .sort_by_key(|r| (r.kind as u8, r.id.0));
    value.recovery.native_roots.dedup();
    let bytes = encode_manifest_v2(&value, limits).unwrap();
    let digest = ManifestId::of(&bytes);
    assert!(matches!(
        decode_manifest(&bytes, digest, limits.recovery),
        Err(Error::Unsupported(_))
    ));
    let decoded = decode_manifest_v2(&bytes, digest, limits).unwrap();
    assert_eq!(decoded, value);
    assert_eq!(
        verify_coverage_v2(&decoded, &objects, &graph, limits)
            .unwrap()
            .native_roots(),
        value.recovery.native_roots
    );
    let old = encode_manifest(&value.recovery, limits.recovery).unwrap();
    assert!(matches!(
        decode_manifest_v2(&old, ManifestId::of(&old), limits),
        Err(Error::Unsupported(_))
    ));
    assert!(verify_coverage(&value.recovery, &objects, limits.recovery).is_err());
    let mut changed_full = full.clone();
    changed_full.drafts[0].order = 11;
    let changed_root = build_index_artifacts(&changed_full, limits.artifacts)
        .unwrap()
        .root;
    let (changed_roots, changed_artifacts) = index_artifact_fixture(&[&empty, &changed_full]);
    let changed_graph =
        verify_index_artifact_graph(&changed_roots, &changed_artifacts, limits.artifacts).unwrap();
    let mut changed = value.clone();
    for binding in &mut changed.operation_indexes {
        if binding.root == full_root.id {
            binding.root = changed_root.id;
        }
    }
    changed.index_roots = changed_roots.clone();
    assert_eq!(changed.recovery, value.recovery);
    assert_ne!(
        ManifestId::of(&encode_manifest_v2(&changed, limits).unwrap()),
        digest
    );
    verify_coverage_v2(&changed, &objects, &changed_graph, limits).unwrap();
    let mut missing = value.clone();
    missing.operation_indexes.remove(0);
    assert!(encode_manifest_v2(&missing, limits).is_err());
    let mut omitted = value.clone();
    omitted
        .recovery
        .native_roots
        .retain(|root| *root != full.drafts[0].descriptor.key);
    assert!(verify_coverage_v2(&omitted, &objects, &graph, limits).is_err());
    let mut selected = value.clone();
    selected.index_roots[0].selected.blake3[0] ^= 1;
    assert!(verify_coverage_v2(&selected, &objects, &graph, limits).is_err());
    let mut tighter = limits;
    tighter.artifacts.snapshot.max_drafts = 1;
    assert!(
        verify_coverage_v2(&value, &objects, &graph, tighter).is_err(),
        "already verified graph must obey tighter current draft policy"
    );
    let mut tighter = limits;
    tighter.artifacts.snapshot.max_roots = 1;
    assert!(verify_coverage_v2(&value, &objects, &graph, tighter).is_err());
    let mut tighter = limits;
    tighter.artifacts.snapshot.max_edges = 1;
    assert!(verify_coverage_v2(&value, &objects, &graph, tighter).is_err());
    let mut tighter = limits;
    tighter.artifacts.snapshot.max_metadata_bytes = 1;
    assert!(verify_coverage_v2(&value, &objects, &graph, tighter).is_err());
    assert!(
        decode_manifest_v2(
            &bytes,
            digest,
            ManifestV2Limits {
                max_operation_indexes: 1,
                ..limits
            }
        )
        .is_err()
    );
    assert!(decode_manifest_v2(&bytes, ManifestId([0; 32]), limits).is_err());
}

fn artifact_history_model(count: usize) -> PortableIndexSnapshot {
    let mut model = PortableIndexSnapshot {
        roots: vec![m::CommitId::ROOT],
        drafts: vec![],
    };
    let mut parent = m::CommitId::ROOT;
    for number in 1..=count {
        let mut commit = m::synthetic_root();
        commit.parents = vec![parent];
        commit.change_id = m::ChangeId([9; 16]);
        commit.description = format!("portable artifact draft {number}");
        let encoded = m::encode(&m::Object::Commit(commit.clone()), Default::default()).unwrap();
        parent = m::CommitId(encoded.descriptor.key.id.0);
        model.drafts.push(IndexDraft {
            descriptor: encoded.descriptor,
            change: commit.change_id,
            parents: commit.parents,
            order: number as u64,
        });
    }
    model.drafts.sort_by_key(IndexDraft::commit);
    model
}
#[test]
fn radix_artifacts_roundtrip_and_reject_noncanonical_or_incomplete_dependency_graphs() {
    let model = artifact_history_model(70);
    let limits = IndexArtifactLimits::default();
    let encoded = build_index_artifacts(&model, limits).unwrap();
    let decoded = encoded
        .artifacts
        .iter()
        .map(|a| decode_index_artifact(&a.bytes, a.descriptor, limits).unwrap())
        .collect::<Vec<_>>();
    let roots = [encoded.root];
    let graph = verify_index_artifact_graph(&roots, &decoded, limits).unwrap();
    assert_eq!(graph.snapshot(encoded.root.id).unwrap(), model);
    let replay = build_index_artifacts(&model, limits).unwrap();
    assert_eq!(encoded.root, replay.root);
    assert!(
        encoded
            .artifacts
            .iter()
            .zip(&replay.artifacts)
            .all(|(a, b)| a.descriptor == b.descriptor && a.bytes == b.bytes)
    );
    assert!(verify_index_artifact_graph(&roots, &decoded[1..], limits).is_err());
    let mut duplicate = encoded
        .artifacts
        .iter()
        .map(|a| decode_index_artifact(&a.bytes, a.descriptor, limits).unwrap())
        .collect::<Vec<_>>();
    duplicate.push(
        decode_index_artifact(
            &encoded.artifacts[0].bytes,
            encoded.artifacts[0].descriptor,
            limits,
        )
        .unwrap(),
    );
    assert!(verify_index_artifact_graph(&roots, &duplicate, limits).is_err());
    let other = portable_index_fixture();
    let (all_roots, all) = index_artifact_fixture(&[&model, &other]);
    assert!(
        verify_index_artifact_graph(&roots, &all, limits).is_err(),
        "unrelated artifacts are not silently retained as members"
    );
    verify_index_artifact_graph(&all_roots, &all, limits).unwrap();
    let root = decoded
        .iter()
        .find_map(|a| match a.artifact() {
            IndexArtifact::Root(r) => Some(r.clone()),
            _ => None,
        })
        .unwrap();
    let branch_id = root.drafts.unwrap();
    let branch = decoded
        .iter()
        .find(|a| a.descriptor().id == IndexArtifactId::Node(branch_id))
        .unwrap();
    let IndexArtifact::Node(mut wrong_summary) = branch.artifact().clone() else {
        panic!()
    };
    wrong_summary.records += 1;
    let replacement = encode_index_artifact(&IndexArtifact::Node(wrong_summary), limits).unwrap();
    let IndexArtifactId::Node(replacement_id) = replacement.descriptor.id else {
        panic!()
    };
    let mut wrong_root = root.clone();
    wrong_root.drafts = Some(replacement_id);
    let replacement_root = encode_index_artifact(&IndexArtifact::Root(wrong_root), limits).unwrap();
    let IndexArtifactId::Root(replacement_root_id) = replacement_root.descriptor.id else {
        panic!()
    };
    let replacement_roots = [IndexRootDescriptor {
        id: replacement_root_id,
        selected: replacement_root.descriptor.selected,
    }];
    let mut replaced = encoded
        .artifacts
        .iter()
        .filter(|a| {
            a.descriptor.id != IndexArtifactId::Root(encoded.root.id)
                && a.descriptor.id != IndexArtifactId::Node(branch_id)
        })
        .map(|a| decode_index_artifact(&a.bytes, a.descriptor, limits).unwrap())
        .collect::<Vec<_>>();
    replaced
        .push(decode_index_artifact(&replacement.bytes, replacement.descriptor, limits).unwrap());
    replaced.push(
        decode_index_artifact(&replacement_root.bytes, replacement_root.descriptor, limits)
            .unwrap(),
    );
    assert!(
        verify_index_artifact_graph(&replacement_roots, &replaced, limits).is_err(),
        "branch counts must equal actual dependency sums"
    );
    let IndexArtifact::Node(mut misplaced) = decoded
        .iter()
        .find(|a| a.descriptor().id == IndexArtifactId::Node(root.anchors))
        .unwrap()
        .artifact()
        .clone()
    else {
        panic!()
    };
    misplaced.depth = 1;
    let misplaced = encode_index_artifact(&IndexArtifact::Node(misplaced), limits).unwrap();
    let IndexArtifactId::Node(misplaced_id) = misplaced.descriptor.id else {
        panic!()
    };
    let mut wrong_root = root;
    wrong_root.anchors = misplaced_id;
    let wrong_root = encode_index_artifact(&IndexArtifact::Root(wrong_root), limits).unwrap();
    let IndexArtifactId::Root(wrong_id) = wrong_root.descriptor.id else {
        panic!()
    };
    let wrong_roots = [IndexRootDescriptor {
        id: wrong_id,
        selected: wrong_root.descriptor.selected,
    }];
    let mut replaced = encoded
        .artifacts
        .iter()
        .filter(|a| {
            a.descriptor.id != IndexArtifactId::Root(encoded.root.id)
                && a.descriptor.id != IndexArtifactId::Node(root_anchors(&decoded))
        })
        .map(|a| decode_index_artifact(&a.bytes, a.descriptor, limits).unwrap())
        .collect::<Vec<_>>();
    replaced.push(decode_index_artifact(&misplaced.bytes, misplaced.descriptor, limits).unwrap());
    replaced.push(decode_index_artifact(&wrong_root.bytes, wrong_root.descriptor, limits).unwrap());
    assert!(
        verify_index_artifact_graph(&wrong_roots, &replaced, limits).is_err(),
        "a valid node cannot be placed at a different radix depth"
    );
    let source = &encoded
        .artifacts
        .iter()
        .find(|a| matches!(a.descriptor.id, IndexArtifactId::Node(_)))
        .unwrap();
    let mut huge = source.bytes.clone();
    huge[44..48].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(
        decode_index_artifact(
            &huge,
            IndexArtifactDescriptor {
                id: IndexArtifactId::of_node(&huge),
                selected: BlobDigest::of(&huge)
            },
            limits
        )
        .is_err()
    );
    assert!(
        decode_index_artifact(
            &source.bytes,
            source.descriptor,
            IndexArtifactLimits {
                max_decode_bytes: 1,
                ..limits
            }
        )
        .is_err()
    );
}
fn root_anchors(artifacts: &[VerifiedIndexArtifact]) -> IndexNodeId {
    artifacts
        .iter()
        .find_map(|a| match a.artifact() {
            IndexArtifact::Root(root) => Some(root.anchors),
            _ => None,
        })
        .unwrap()
}
#[test]
fn radix_single_large_record_is_indivisible_and_obeys_common_wire_cap() {
    let mut commit = m::synthetic_root();
    commit.parents = vec![m::CommitId::ROOT; 9000];
    commit.description = "one bounded large parent vector".into();
    let encoded = m::encode(&m::Object::Commit(commit.clone()), Default::default()).unwrap();
    let model = PortableIndexSnapshot {
        roots: vec![m::CommitId::ROOT],
        drafts: vec![IndexDraft {
            descriptor: encoded.descriptor,
            change: commit.change_id,
            parents: commit.parents,
            order: 1,
        }],
    };
    let mut limits = IndexArtifactLimits::default();
    limits.snapshot.max_parents = 9000;
    let built = build_index_artifacts(&model, limits).unwrap();
    assert!(built.artifacts.iter().any(|a| a.bytes.len() > 256 * 1024));
    assert!(
        built
            .artifacts
            .iter()
            .all(|a| a.bytes.len() <= 4 * 1024 * 1024)
    );
    let artifacts = built
        .artifacts
        .iter()
        .map(|a| decode_index_artifact(&a.bytes, a.descriptor, limits).unwrap())
        .collect::<Vec<_>>();
    let roots = [built.root];
    assert_eq!(
        verify_index_artifact_graph(&roots, &artifacts, limits)
            .unwrap()
            .snapshot(built.root.id)
            .unwrap(),
        model
    );
    limits.max_artifact_bytes = 256 * 1024;
    assert!(build_index_artifacts(&model, limits).is_err());
}
#[test]
fn radix_history_reuses_artifacts_across_one_thousand_draft_operations() {
    let full = artifact_history_model(1000);
    let limits = IndexArtifactLimits::default();
    let mut chronological = full.drafts.clone();
    chronological.sort_by_key(|d| d.order);
    let mut current = PortableIndexSnapshot {
        roots: full.roots.clone(),
        drafts: vec![],
    };
    let mut unique = BTreeMap::<IndexArtifactId, (IndexArtifactDescriptor, Vec<u8>)>::new();
    let mut roots = Vec::new();
    let mut full_bytes = 0u64;
    let mut all_bytes = 0u64;
    for draft in chronological {
        current.drafts.push(draft);
        current.drafts.sort_by_key(IndexDraft::commit);
        full_bytes += encode_index_snapshot(&current, limits.snapshot)
            .unwrap()
            .len() as u64;
        let built = build_index_artifacts(&current, limits).unwrap();
        roots.push(built.root);
        for artifact in built.artifacts {
            all_bytes += artifact.bytes.len() as u64;
            if let Some((descriptor, bytes)) = unique.get(&artifact.descriptor.id) {
                assert_eq!(*descriptor, artifact.descriptor);
                assert_eq!(*bytes, artifact.bytes);
            } else {
                unique.insert(
                    artifact.descriptor.id,
                    (artifact.descriptor, artifact.bytes),
                );
            }
        }
    }
    let unique_bytes = unique
        .values()
        .map(|(_, bytes)| bytes.len() as u64)
        .sum::<u64>();
    assert!(
        unique_bytes < full_bytes / 2,
        "logical artifact reuse must reduce storage/transfer, not merely rename full snapshots"
    );
    roots.sort_by_key(|r| r.id);
    let artifacts = unique
        .values()
        .map(|(d, b)| decode_index_artifact(b, *d, limits).unwrap())
        .collect::<Vec<_>>();
    let graph = verify_index_artifact_graph(&roots, &artifacts, limits).unwrap();
    let final_root = build_index_artifacts(&full, limits).unwrap().root;
    assert_eq!(graph.snapshot(final_root.id).unwrap(), full);
    let replay = build_index_artifacts(&full, limits).unwrap();
    assert!(
        replay
            .artifacts
            .iter()
            .all(|a| unique.contains_key(&a.descriptor.id))
    );
    eprintln!(
        "{{\"schema\":1,\"benchmark\":\"portable_index_artifact_history_bytes\",\"draft_operations\":1000,\"full_snapshot_logical_bytes\":{full_bytes},\"all_artifact_logical_bytes_before_dedup\":{all_bytes},\"unique_artifact_logical_bytes\":{unique_bytes},\"unique_artifacts\":{},\"snapshot_roots\":{},\"max_artifact_bytes\":{},\"cpu_incremental_claim\":false,\"wire_compressed_bytes\":false}}",
        unique.len(),
        roots.len(),
        unique.values().map(|(_, b)| b.len()).max().unwrap()
    );
}

#[test]
fn capture_data_is_bounded_portable_data_not_frontend_authority() {
    let zero = "00".repeat(64);
    let root = [7; 32];
    let mut data = CaptureData {
        version: 1,
        policy: CapturePolicy::HostQuiescentV1,
        codec: CODEC.into(),
        operation_heads: vec![zero.clone()],
        working_copy: PortableWorkingCopy {
            workspace: "default".into(),
            wc_operation: zero.clone(),
            snapshot_operation: zero.clone(),
            snapshot_commit: [0; 32],
            tree: CaptureTree {
                terms: vec![m::empty_tree().descriptor.key.id.0],
                labels: vec![],
            },
        },
        operations: vec![OperationRecord {
            operation: zero.clone(),
            view: zero.clone(),
            parents: vec![],
        }],
        indexes: vec![IndexRecord {
            operation: zero.clone(),
            root,
        }],
        blobs: vec![BlobRecord {
            kind: 3,
            id: "07".repeat(32),
            blake3: [8; 32],
            size: 128,
        }],
        roots: vec![],
        remote: Some(PortableRemoteState {
            landing: None,
            observations: vec![],
            archived_pushes: vec![ArchivedIntent {
                reference: "refs/heads/main".into(),
                bookmark: "main".into(),
                generation: 1,
                head: Some("09".repeat(32)),
                request_id: "0a".repeat(16),
                base_operation: zero.clone(),
                confirmed_generation: Some(2),
            }],
        }),
    };
    let limits = CaptureDataLimits::default();
    let bytes = encode_capture_data(&data, limits).unwrap();
    let decoded = decode_data(&bytes, data_digest(&bytes), &limits).unwrap();
    assert_eq!(
        decoded, data,
        "a policy label is portable data, not a live host proof"
    );
    let text = std::str::from_utf8(&bytes).unwrap();
    for forbidden in [
        "socket",
        "repository_path",
        "projection_generation",
        "acknowledgement",
        "receipt",
    ] {
        assert!(!text.contains(forbidden));
    }
    assert!(decode_data(&bytes, [0; 32], &limits).is_err());
    assert!(
        decode_data(
            &bytes,
            data_digest(&bytes),
            &CaptureDataLimits {
                max_bytes: bytes.len() - 1,
                ..limits
            }
        )
        .is_err()
    );
    let mut unknown: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    unknown["host_path"] = "/private/source".into();
    let unknown = serde_json::to_vec(&unknown).unwrap();
    assert!(decode_data(&unknown, data_digest(&unknown), &limits).is_err());
    assert!(
        !text.contains("landing"),
        "existing v1 bytes have no new optional field"
    );
    let original = data.clone();
    let mut landing = PortableLanding {
        request_id: "12".repeat(16),
        source_base: "00".repeat(32),
        source_head: "13".repeat(32),
        reference: "main".into(),
        bookmark: "main".into(),
        generation: 1,
        expected_head: "14".repeat(32),
        profile_name: "checks".into(),
        profile_version: "v1".into(),
        source_operation: zero.clone(),
        source_stack: vec!["13".repeat(32)],
        fork_operation: None,
        fork_acknowledged: false,
        job_id: None,
        result_roots: vec![],
        candidate_head: None,
        apply_operation: None,
        operation_writing: Some("freeze".into()),
        acknowledged: false,
        terminal: None,
        observed: None,
        restored_lookup_only: false,
    };
    data.version = 2;
    data.remote.as_mut().unwrap().landing = Some(landing.clone());
    let pending_bytes = encode_capture_data(&data, limits).unwrap();
    assert_eq!(
        decode_data(&pending_bytes, data_digest(&pending_bytes), &limits).unwrap(),
        data
    );
    data.version = 1;
    assert!(
        encode_capture_data(&data, limits).is_err(),
        "v1 cannot silently carry a landing"
    );
    data.version = 2;
    data.remote.as_mut().unwrap().landing = None;
    assert!(
        encode_capture_data(&data, limits).is_err(),
        "v2 requires landing"
    );
    landing.operation_writing = None;
    landing.fork_operation = Some(zero.clone());
    landing.fork_acknowledged = true;
    landing.job_id = Some("17".repeat(32));
    let candidate = HistoricalLandingCandidate {
        head: [24; 32],
        root_tree: [25; 32],
        rewrites: vec![HistoricalLandingRewrite {
            old_commit: [19; 32],
            new_commit: [24; 32],
            change_id: [26; 16],
        }],
    };
    for (name, phase, retained, with_receipt, terminal) in [
        (
            "prepared",
            HistoricalLandingPhase::PreparedPendingChecks,
            true,
            false,
            None,
        ),
        (
            "committed_not_applied",
            HistoricalLandingPhase::Committed,
            false,
            true,
            None,
        ),
        (
            "acknowledged_committed",
            HistoricalLandingPhase::Committed,
            false,
            true,
            None,
        ),
        (
            "stale_before_start",
            HistoricalLandingPhase::StaleTarget,
            false,
            false,
            Some("stale_target"),
        ),
        (
            "terminal_unavailable",
            HistoricalLandingPhase::CheckUnavailable,
            false,
            false,
            Some("check_unavailable"),
        ),
    ] {
        let mut p = landing.clone();
        p.candidate_head = Some("18".repeat(32));
        p.result_roots = vec!["18".repeat(32)];
        if terminal.is_some() {
            p.candidate_head = None;
            p.result_roots.clear();
        }
        p.terminal = terminal.map(str::to_owned);
        p.observed = Some(HistoricalLandingJob {
            job_id: [23; 32],
            phase,
            preparation: HistoricalLandingPreparation::ProfileConfigured,
            roots_retained: retained,
            expires_at_seconds: 12345,
            profile_name: "checks".into(),
            profile_version: "v1".into(),
            candidate: (name != "stale_before_start").then(|| candidate.clone()),
            receipt: with_receipt.then(|| HistoricalLandingReceipt {
                reference: "main".into(),
                generation: 2,
                commit: [24; 32],
                commit_size_bytes: 128,
                object_format_version: m::FORMAT_VERSION,
                rewrites: candidate.rewrites.clone(),
                evidence: [27; 32],
            }),
            conflict_path: None,
        });
        if name == "acknowledged_committed" {
            p.acknowledged = true;
            p.apply_operation = Some(zero.clone());
            p.candidate_head = None;
            p.result_roots.clear();
        }
        data.remote.as_mut().unwrap().landing = Some(p);
        if name == "stale_before_start" {
            let mut invalid = data.clone();
            invalid
                .remote
                .as_mut()
                .unwrap()
                .landing
                .as_mut()
                .unwrap()
                .observed
                .as_mut()
                .unwrap()
                .roots_retained = true;
            assert!(encode_capture_data(&invalid, limits).is_err());
            let mut invalid = data.clone();
            invalid
                .remote
                .as_mut()
                .unwrap()
                .landing
                .as_mut()
                .unwrap()
                .observed
                .as_mut()
                .unwrap()
                .conflict_path = Some("file.rs".into());
            assert!(encode_capture_data(&invalid, limits).is_err());
        }
        let encoded = encode_capture_data(&data, limits).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(
            decode_data(&encoded, data_digest(&encoded), &limits).unwrap(),
            data,
            "{name}"
        );
    }
    let mut forged = data.clone();
    forged
        .remote
        .as_mut()
        .unwrap()
        .landing
        .as_mut()
        .unwrap()
        .source_operation = "01".repeat(64);
    assert!(
        encode_capture_data(&forged, limits).is_err(),
        "landing operations belong to captured inventory"
    );
    let mut forged = data.clone();
    forged
        .remote
        .as_mut()
        .unwrap()
        .landing
        .as_mut()
        .unwrap()
        .observed
        .as_mut()
        .unwrap()
        .expires_at_seconds = 0;
    assert!(
        encode_capture_data(&forged, limits).is_err(),
        "unknown legacy expiry stays None, never fabricated zero"
    );
    data = original;
    assert_eq!(
        encode_capture_data(&data, limits).unwrap(),
        bytes,
        "v1 canonical bytes unchanged"
    );
    data.remote.as_mut().unwrap().archived_pushes[0].base_operation = "01".repeat(64);
    assert!(
        encode_capture_data(&data, limits).is_err(),
        "archived intents retain exact OP ancestry"
    );
    data.remote = None;
    data.operations.push(OperationRecord {
        operation: "01".repeat(64),
        view: zero,
        parents: vec!["02".repeat(64)],
    });
    data.indexes.push(IndexRecord {
        operation: "01".repeat(64),
        root,
    });
    assert!(
        encode_capture_data(&data, limits).is_err(),
        "missing retained orphan parents are not ignored"
    );
}
