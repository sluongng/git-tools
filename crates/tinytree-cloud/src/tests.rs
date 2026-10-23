use super::*;
use jj_lib::{
    backend::{MillisSinceEpoch, Timestamp},
    content_hash::blake2b_hash,
    op_store::{
        OperationMetadata, RemoteRef, RemoteRefState, RemoteView, RootOperationData, TimestampRange,
    },
};
use std::{collections::BTreeMap, path::Path};

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
