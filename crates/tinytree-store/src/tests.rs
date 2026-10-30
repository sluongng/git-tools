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

fn forge_fixture(store: &Store, account: &AccountId) -> CommitId {
    let repo = account.repository_id();
    let put = |object: Object| {
        let encoded = tinytree_model::encode(&object, Limits::default()).unwrap();
        store.put(&repo, encoded.descriptor, &encoded.bytes, 0).unwrap();
        encoded.descriptor
    };
    let mut entries = Vec::new();
    for (name, bytes) in [("README.md", b"# Account\npublic needle\n".as_slice()), ("secret.txt", b"private needle\n".as_slice()), ("source.rs", b"fn needle() {}\n".as_slice())] {
        let d = tinytree_model::describe_file(bytes);
        store.put(&repo, d, bytes, 0).unwrap();
        entries.push(TreeEntry { name: name.into(), value: TreeValue::File { id: FileId(d.key.id.0), executable: false, size_bytes: d.size_bytes, copy_id: None } });
    }
    let tree = put(Object::Tree(Tree { entries }));
    let mut commit = commit(TreeId(tree.key.id.0), "forge fixture");
    commit.parents = vec![CommitId(store.references(&repo, &["main".into()]).unwrap()[0].commit.unwrap().key.id.0)];
    CommitId(put(Object::Commit(commit)).key.id.0)
}
fn forge_public_view(revision: CommitId) -> ViewSpec {
    ViewSpec { name: "public".into(), root_prefix: String::new(), include_prefixes: vec![String::new()], exclude_prefixes: vec![], visibility: ForgeViewVisibility::Public, revision: revision.0, export_allowed: true }
}

#[test]
fn forge_account_provision_is_single_owned_durable_monorepo() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), config()).unwrap();
    let id = AccountId("sample".into());
    let owner = ForgeActor { principal: "owner".into() };
    let first = store.forge_provision_account(&id, &owner).unwrap();
    let second = store.forge_provision_account(&id, &owner).unwrap();
    assert_eq!(first.repository, second.repository);
    assert_eq!(first.repository, "forge-sample");
    assert!(matches!(store.forge_provision_account(&id, &ForgeActor { principal: "other".into() }), Err(Error::ForgeAccessDenied)));
    assert!(store.forge_provision_account(&AccountId("../other".into()), &owner).is_err());
    let main = store.references(&first.repository, &["main".into()]).unwrap().remove(0);
    assert_eq!(main.generation, 1);
    assert!(main.commit.is_some());
    assert_eq!(store.list_objects(&first.repository, Kind::Commit, None, 10).unwrap().len(), 1);
    assert_eq!(store.list_objects(&first.repository, Kind::Tree, None, 10).unwrap().len(), 1);
    let format = store.inner.db.read_tx().get(&store.inner.spaces.meta,b"format").unwrap().unwrap();
    assert!(forge_feature(&format)); assert_eq!(store_feature(Some(&format)).unwrap(),1);
    // Packing advances the independent base feature without implying cloud/check
    // activation or erasing the forge authority marker.
    store.pack(&first.repository,PackOptions::default()).unwrap();
    let packed_format = store.inner.db.read_tx().get(&store.inner.spaces.meta,b"format").unwrap().unwrap();
    assert!(forge_feature(&packed_format)); assert!(store_feature(Some(&packed_format)).unwrap()>1);
    drop(store);
    let reopened = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(reopened.forge_provision_account(&id, &owner).unwrap().repository, first.repository);
    assert_eq!(reopened.references(&first.repository, &["main".into()]).unwrap()[0].commit, main.commit);
}

#[test]
fn forge_public_and_locked_views_recheck_policy_and_confine_direct_reads() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), config()).unwrap();
    let account = AccountId("sample".into());
    let owner = ForgeActor { principal: "owner".into() };
    store.forge_provision_account(&account, &owner).unwrap();
    let revision = forge_fixture(&store, &account);
    let public = store.forge_publish_view(&account, &owner, forge_public_view(revision), PolicyEpoch(1), true).unwrap();
    let old_version = public.version();
    let anonymous = store.forge_resolve_view(&account, None, "", None).unwrap();
    assert_eq!(store.forge_read_view_file(&anonymous, "secret.txt", 1024).unwrap(), b"private needle\n");
    let mut restricted = forge_public_view(revision);
    restricted.exclude_prefixes = vec!["secret.txt".into()];
    store.forge_publish_view(&account, &owner, restricted, public.epoch(), true).unwrap();
    assert!(matches!(store.forge_read_view_file(&anonymous, "README.md", 1024), Err(Error::StaleForgePolicy)));
    let old_projection = store.forge_resolve_view(&account, None, "public", Some(old_version)).unwrap();
    assert!(matches!(store.forge_read_view_file(&old_projection, "secret.txt", 1024), Err(Error::ForgeAccessDenied)));
    assert!(matches!(store.forge_read_view_file(&old_projection, "../secret.txt", 1024), Err(Error::Invalid(_))));
    let files = store.forge_view_files(&old_projection, 10).unwrap();
    assert_eq!(files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(), ["README.md", "source.rs"]);
    let mut locked = forge_public_view(revision);
    locked.name = "locked".into(); locked.visibility = ForgeViewVisibility::Roles(vec!["security".into()]);
    let private = store.forge_publish_view(&account, &owner, locked, old_projection.epoch(), false).unwrap();
    assert!(matches!(store.forge_resolve_view(&account, None, "locked", None), Err(Error::ForgeAccessDenied)));
    let member = ForgeActor { principal: "reader".into() };
    let policy = store.forge_set_member(&account, &owner, ForgeMember { principal: member.principal.clone(), can_write: false, can_review: true, is_admin: false, roles: vec!["security".into()] }, private.epoch()).unwrap();
    let readable = store.forge_resolve_view(&account, Some(&member), "locked", None).unwrap();
    assert_eq!(store.forge_read_view_file(&readable, "secret.txt", 1024).unwrap(), b"private needle\n");
    store.forge_remove_member(&account, &owner, &member.principal, policy.epoch).unwrap();
    assert!(matches!(store.forge_read_view_file(&readable, "secret.txt", 1024), Err(Error::ForgeAccessDenied)));
    let other = AccountId("other".into());
    store.forge_provision_account(&other, &owner).unwrap();
    assert!(matches!(store.forge_resolve_view(&other, None, "public", Some(old_version)), Err(Error::ForgeAccessDenied)));
}

#[test]
fn forge_published_view_revision_survives_source_reference_movement_and_gc() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), config()).unwrap();
    let account = AccountId("sample".into());
    let owner = ForgeActor { principal: "owner".into() };
    let info = store.forge_provision_account(&account, &owner).unwrap();
    let revision = forge_fixture(&store, &account);
    store.forge_publish_view(&account, &owner, forge_public_view(revision), info.epoch, true).unwrap();
    store.gc(&info.repository, 100).unwrap();
    let view = store.forge_resolve_view(&account, None, "", None).unwrap();
    assert_eq!(view.revision(), revision);
    assert_eq!(store.forge_read_view_file(&view, "README.md", 1024).unwrap(), b"# Account\npublic needle\n");
    drop(store);
    let reopened = Store::open_existing(directory.path(), config()).unwrap();
    let view = reopened.forge_resolve_view(&account, None, "", None).unwrap();
    assert_eq!(reopened.forge_read_view_file(&view, "source.rs", 1024).unwrap(), b"fn needle() {}\n");
}

#[test]
fn forge_retired_historical_view_releases_only_its_source_gc_root() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), config()).unwrap();
    let account = AccountId("retirement".into());
    let actor = ForgeActor { principal: "owner".into() };
    store.forge_provision_account(&account, &actor).unwrap();
    let revision = forge_fixture(&store, &account);
    let old = store.forge_publish_view(&account, &actor, forge_public_view(revision), PolicyEpoch(1), true).unwrap();
    assert!(store.forge_retire_view(&account, &actor, old.version(), old.epoch()).is_err());
    let projection = store.forge_project_view(&old, 0).unwrap();
    let genesis = store.references(old.repository(), &["main".into()]).unwrap()[0].commit.unwrap();
    let current = store.forge_publish_view(&account, &actor, forge_public_view(CommitId(genesis.key.id.0)), old.epoch(), true).unwrap();
    let file = tinytree_model::describe_file(b"fn needle() {}\n");
    let mutation = ForgeAccountMutation::RetireView { version: old.version(), expected_epoch: current.epoch(), view: "public".into() };
    let receipt = store.forge_mutate_account(&account, &actor, "retire-historical", mutation.clone()).unwrap();
    assert_eq!(receipt["retired_version"], serde_json::json!(old.version().0));
    assert_eq!(store.forge_mutate_account(&account, &actor, "retire-historical", mutation.clone()).unwrap(), receipt);
    let mut mismatch = mutation.clone();
    if let ForgeAccountMutation::RetireView { view, .. } = &mut mismatch { *view = "other".into(); }
    assert!(store.forge_mutate_account(&account, &actor, "retire-historical", mismatch).is_err());
    assert!(matches!(store.forge_mutate_account(&account, &actor, "stale-retirement", mutation.clone()), Err(Error::StaleForgePolicy)));
    let info = store.forge_get_account(&account, &actor).unwrap();
    assert!(matches!(store.forge_resolve_view(&account, None, "public", Some(old.version())), Err(Error::ForgeAccessDenied)));
    assert!(store.forge_publish_view(&account, &actor, forge_public_view(revision), info.epoch, true).is_err());
    store.gc(&info.repository, 100).unwrap();
    assert!(store.descriptor(&info.repository, ObjectKey { kind: Kind::Commit, id: ObjectId(revision.0) }).unwrap().is_none());
    assert!(store.descriptor(&info.repository, file.key).unwrap().is_none());
    assert!(store.descriptor(&info.repository, ObjectKey { kind: Kind::Commit, id: ObjectId(projection.projected_commit) }).unwrap().is_none());
    {
        let _gate = store.inner.lock().unwrap();
        assert_eq!(store.inner.forge_projection_relation_in(&store.inner.db.read_tx(), &account, old.version(), old.epoch(), CommitId(projection.projected_commit)).unwrap(), revision);
    }
    drop(store);
    let reopened = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(reopened.forge_mutate_account(&account, &actor, "retire-historical", mutation).unwrap(), receipt);
    assert_eq!(reopened.forge_resolve_view(&account, None, "", None).unwrap().revision().0, genesis.key.id.0);
}

#[test]
fn forge_projection_excludes_private_objects_and_uploads_are_actor_path_epoch_bound() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), config()).unwrap();
    let account = AccountId("projection".into());
    let actor = ForgeActor { principal: "owner".into() };
    store.forge_provision_account(&account, &actor).unwrap();
    let revision = forge_fixture(&store, &account);
    let mut spec = forge_public_view(revision); spec.exclude_prefixes = vec!["secret.txt".into()];
    let view = store.forge_publish_view(&account, &actor, spec.clone(), PolicyEpoch(1), true).unwrap();
    let projected = store.forge_project_view(&view, 0).unwrap();
    assert_eq!(projected.projected_commit, store.forge_project_view(&view, 0).unwrap().projected_commit);
    assert_ne!(projected.projected_commit, revision.0);
    let bytes = store.forge_read_projected_object(&view, ObjectKey { kind:Kind::Commit, id:ObjectId(projected.projected_commit) }, 0, 4096).unwrap();
    let Object::Commit(projected_payload) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else { panic!("not a commit") };
    assert_eq!(projected_payload.parents, vec![CommitId::ROOT]); assert!(projected_payload.predecessors.is_empty());
    assert_eq!(projected_payload.description, "Projected workspace");
    let private = tinytree_model::describe_file(b"private needle\n");
    assert!(matches!(store.forge_read_projected_object(&view, private.key, 0, 4096), Err(Error::ForgeAccessDenied)));
    assert!(matches!(store.forge_read_projected_object(&view, ObjectKey { kind:Kind::Commit,id:ObjectId(revision.0) }, 0, 4096), Err(Error::ForgeAccessDenied)));
    let reviewed=store.forge_project_review_head(&view,revision,ChangeId([8;16]),CommitId(projected.projected_commit),0).unwrap();
    let (descriptor,empty,eof)=store.forge_read_object_chunk(&view,None,ObjectKey{kind:Kind::Commit,id:ObjectId(reviewed.projected_commit)},0,0,0).unwrap();
    assert!(empty.is_empty());assert!(!eof);assert!(descriptor.size_bytes>0);
    let (_,chunk,eof)=store.forge_read_object_chunk(&view,None,descriptor.key,0,0,4096).unwrap();assert!(eof);
    let Object::Commit(reviewed_payload)=tinytree_model::decode(&chunk,Limits::default()).unwrap() else{panic!("not a commit")};
    assert_eq!(reviewed_payload.change_id,ChangeId([8;16]));assert_eq!(reviewed_payload.parents,vec![CommitId(projected.projected_commit)]);
    let (_,tree_bytes)=store.read_metadata(view.repository(),ObjectKey{kind:Kind::Tree,id:ObjectId(projected.projected_tree)}).unwrap();
    let Object::Tree(tree_payload)=tinytree_model::decode(&tree_bytes,Limits::default()).unwrap()else{panic!("not a tree")};
    let (source_file,source_size,source_copy)=tree_payload.entries.iter().find_map(|e|if e.name=="source.rs"{if let TreeValue::File{id,size_bytes,copy_id:Some(copy),..}=e.value{Some((id,size_bytes,copy))}else{None}}else{None}).unwrap();
    // Captured from the native Store builder before the shared-core extraction.
    assert_eq!(projected.projected_commit, [193, 128, 130, 77, 26, 39, 133, 114, 29, 38, 247, 228, 226, 62, 32, 30, 55, 159, 185, 107, 79, 169, 173, 110, 100, 68, 197, 75, 89, 251, 112, 231]);
    assert_eq!(projected.projected_tree, [55, 82, 200, 186, 190, 207, 9, 210, 91, 172, 23, 106, 76, 248, 231, 158, 211, 242, 178, 72, 81, 67, 138, 77, 196, 124, 10, 246, 43, 10, 204, 93]);
    assert_eq!(reviewed.projected_commit, [188, 155, 231, 129, 127, 191, 198, 171, 192, 78, 67, 94, 146, 132, 209, 109, 26, 142, 194, 32, 144, 198, 63, 23, 59, 68, 63, 188, 141, 212, 94, 196]);
    assert_eq!(reviewed.projected_tree, [196, 185, 133, 161, 202, 57, 165, 153, 209, 89, 66, 210, 145, 93, 64, 19, 105, 201, 22, 50, 125, 227, 141, 120, 20, 147, 18, 243, 2, 69, 147, 166]);
    assert_eq!(source_copy.0, [73, 132, 231, 136, 25, 24, 205, 214, 190, 186, 148, 209, 131, 22, 120, 233, 228, 226, 84, 24, 27, 170, 180, 152, 183, 176, 2, 30, 134, 249, 255, 29]);
    let origin={let _gate=store.inner.lock().unwrap();store.inner.forge_projected_copy_origin_in(&store.inner.db.read_tx(),&view,source_copy).unwrap()};
    assert_eq!(origin.canonical_revision,revision.0);assert_eq!(origin.canonical_path,"source.rs");assert_eq!(origin.canonical_copy,None);
    let copy=tinytree_model::encode(&Object::CopyHistory(CopyHistory{current_path:"renamed.rs".into(),parents:vec![source_copy],salt:vec![1]}),Limits::default()).unwrap();
    let copy_receipt = store.forge_upload_object(&view,&actor,"renamed.rs",copy.descriptor,&copy.bytes,0).unwrap();
    let renamed=tinytree_model::encode(&Object::Tree(Tree{entries:vec![TreeEntry{name:"renamed.rs".into(),value:TreeValue::File{id:source_file,size_bytes:source_size,executable:false,copy_id:Some(CopyId(copy.descriptor.key.id.0))}}]}),Limits::default()).unwrap();
    store.forge_upload_object(&view,&actor,"",renamed.descriptor,&renamed.bytes,0).unwrap();
    let child_copy=tinytree_model::encode(&Object::CopyHistory(CopyHistory{current_path:"final.rs".into(),parents:vec![CopyId(copy.descriptor.key.id.0)],salt:vec![4]}),Limits::default()).unwrap();
    store.forge_upload_object(&view,&actor,"final.rs",child_copy.descriptor,&child_copy.bytes,1).unwrap();
    let final_tree=tinytree_model::encode(&Object::Tree(Tree{entries:vec![TreeEntry{name:"final.rs".into(),value:TreeValue::File{id:source_file,size_bytes:source_size,executable:false,copy_id:Some(CopyId(child_copy.descriptor.key.id.0))}}]}),Limits::default()).unwrap();
    store.forge_upload_object(&view,&actor,"",final_tree.descriptor,&final_tree.bytes,copy_receipt.expires_at).unwrap();
    assert!(matches!(store.forge_upload_object(&view,&actor,"final.rs",child_copy.descriptor,&child_copy.bytes,copy_receipt.expires_at),Err(Error::ForgeAccessDenied)),"TREE copy reference uses admitted child authority; supplying the child again must recheck its expired direct parent receipt");

    let secret_copy=tinytree_model::encode(&Object::CopyHistory(CopyHistory{current_path:"secret.txt".into(),parents:vec![],salt:vec![2]}),Limits::default()).unwrap();store.put(view.repository(),secret_copy.descriptor,&secret_copy.bytes,0).unwrap();
    let forged=tinytree_model::encode(&Object::CopyHistory(CopyHistory{current_path:"forged.rs".into(),parents:vec![CopyId(secret_copy.descriptor.key.id.0)],salt:vec![3]}),Limits::default()).unwrap();
    assert!(matches!(store.forge_upload_object(&view,&actor,"forged.rs",forged.descriptor,&forged.bytes,0),Err(Error::ForgeAccessDenied)));
    let uploaded = tinytree_model::describe_file(b"new public bytes");
    store.forge_upload_file(&view, &actor, "new.rs", b"new public bytes", 0).unwrap();
    {
        let _gate = store.inner.lock().unwrap(); let tx = store.inner.db.read_tx();
        assert_eq!(store.inner.forge_projection_base_in(&tx,&view,CommitId(projected.projected_commit)).unwrap(),revision);
        assert!(store.inner.forge_validate_file_in(&tx,&view,&actor,"new.rs",private,0).is_err());
        store.inner.forge_validate_file_in(&tx,&view,&actor,"new.rs",uploaded,0).unwrap();
        assert!(store.inner.forge_validate_file_in(&tx,&view,&actor,"other.rs",uploaded,0).is_err());
        assert!(store.inner.forge_validate_file_in(&tx,&view,&ForgeActor{principal:"other".into()},"new.rs",uploaded,0).is_err());
    }
    let make_tree = |descriptor:Descriptor| tinytree_model::encode(&Object::Tree(Tree { entries:vec![TreeEntry { name:"new.rs".into(),value:TreeValue::File { id:FileId(descriptor.key.id.0),executable:false,size_bytes:descriptor.size_bytes,copy_id:None } }] }),Limits::default()).unwrap();
    let poisoned=make_tree(private);
    assert!(matches!(store.forge_upload_object(&view,&actor,"",poisoned.descriptor,&poisoned.bytes,0),Err(Error::ForgeAccessDenied)));
    let allowed=make_tree(uploaded);
    store.forge_upload_object(&view,&actor,"",allowed.descriptor,&allowed.bytes,0).unwrap();
    assert_eq!(store.forge_read_authorized_object(&view,Some(&actor),allowed.descriptor.key,0,4096).unwrap(),allowed.bytes);
    assert!(matches!(store.forge_read_authorized_object(&view,None,allowed.descriptor.key,0,4096),Err(Error::ForgeAccessDenied)));
    let mut draft=commit(TreeId(allowed.descriptor.key.id.0),"author supplied draft");
    draft.parents=vec![CommitId(projected.projected_commit)];
    let draft=tinytree_model::encode(&Object::Commit(draft),Limits::default()).unwrap();
    store.forge_upload_object(&view,&actor,"",draft.descriptor,&draft.bytes,0).unwrap();
    spec.exclude_prefixes.push("source.rs".into());
    store.forge_publish_view(&account,&actor,spec,view.epoch(),true).unwrap();
    assert!(matches!(store.forge_read_projected_object(&view,uploaded.key,0,4096),Err(Error::StaleForgePolicy)));
    store.gc(view.repository(),100).unwrap();
    drop(store);
    Store::open_existing(directory.path(),config()).unwrap();
}

#[test]
fn forge_account_mutations_replay_the_original_durable_acknowledgment() {
    let directory=tempfile::tempdir().unwrap();let store=Store::open(directory.path(),config()).unwrap();
    let account=AccountId("replay".into());let actor=ForgeActor{principal:"owner".into()};
    let initial=store.forge_mutate_account(&account,&actor,"provision",ForgeAccountMutation::Provision).unwrap();
    assert_eq!(initial["main_generation"],serde_json::json!(1));
    assert_eq!(initial["main_revision"],store.forge_get_account_snapshot(&account,&actor).unwrap()["main_revision"]);
    assert_ne!(initial["main_revision"],serde_json::json!(CommitId::ROOT.0));
    let revision=forge_fixture(&store,&account);
    let mutation=ForgeAccountMutation::PublishView{spec:forge_public_view(revision),expected_epoch:PolicyEpoch(1),make_default:true,follow_main:false};
    let acknowledgment=store.forge_mutate_account(&account,&actor,"publish",mutation.clone()).unwrap();
    assert_eq!(store.forge_mutate_account(&account,&actor,"publish",mutation.clone()).unwrap(),acknowledgment);
    store.forge_set_member(&account,&actor,ForgeMember{principal:"reader".into(),can_write:false,can_review:false,is_admin:false,roles:vec![]},PolicyEpoch(2)).unwrap();
    assert_eq!(store.forge_mutate_account(&account,&actor,"provision",ForgeAccountMutation::Provision).unwrap(),initial);
    assert!(matches!(store.forge_mutate_account(&account,&actor,"publish",ForgeAccountMutation::Provision),Err(Error::Invalid("forge request replay mismatch"))));
    drop(store);
    let store=Store::open_existing(directory.path(),config()).unwrap();
    assert_eq!(store.forge_mutate_account(&account,&actor,"publish",mutation).unwrap(),acknowledgment);
    assert_eq!(store.forge_get_account(&account,&actor).unwrap().epoch,PolicyEpoch(3));
}

#[test]
fn forge_projection_bounds_cumulative_paths_even_with_shared_long_prefixes() {
    let (_directory,store)=open();let account=AccountId("bounded-paths".into());let actor=ForgeActor{principal:"owner".into()};store.forge_provision_account(&account,&actor).unwrap();let repo=account.repository_id();
    let file=tinytree_model::describe_file(b"x");store.put(&repo,file,b"x",0).unwrap();
    let put=|object:Object|{let encoded=tinytree_model::encode(&object,Limits::default()).unwrap();store.put(&repo,encoded.descriptor,&encoded.bytes,0).unwrap();encoded.descriptor};
    let mut tree=put(Object::Tree(Tree{entries:(0..600).map(|i|TreeEntry{name:format!("file-{i:04}"),value:TreeValue::File{id:FileId(file.key.id.0),size_bytes:1,executable:false,copy_id:None}}).collect()}));
    for i in 0..60 {tree=put(Object::Tree(Tree{entries:vec![TreeEntry{name:format!("{i:02}{}","p".repeat(60)),value:TreeValue::Tree(TreeId(tree.key.id.0))}]}));}
    let revision=put(Object::Commit(commit(TreeId(tree.key.id.0),"shared prefix boundary")));
    let view=store.forge_publish_view(&account,&actor,forge_public_view(CommitId(revision.key.id.0)),PolicyEpoch(1),true).unwrap();
    assert!(matches!(store.forge_view_files(&view,1000),Err(Error::Limit("forge projected path bytes"))));
    assert!(matches!(store.forge_project_view(&view,0),Err(Error::Limit("forge projected path bytes"))));
}

#[test]
fn forge_following_named_views_advance_atomically_without_moving_historical_versions() {
    let directory=tempfile::tempdir().unwrap();let store=Store::open(directory.path(),config()).unwrap();let account=AccountId("following".into());let actor=ForgeActor{principal:"owner".into()};
    let provision=store.forge_mutate_account(&account,&actor,"provision",ForgeAccountMutation::Provision).unwrap();let repo=account.repository_id();let genesis=store.references(&repo,&["main".into()]).unwrap()[0].commit.unwrap();
    store.forge_mutate_account(&account,&actor,"publish",ForgeAccountMutation::PublishView{spec:forge_public_view(CommitId(genesis.key.id.0)),expected_epoch:PolicyEpoch(1),make_default:true,follow_main:true}).unwrap();
    let mut frozen=forge_public_view(CommitId(genesis.key.id.0));frozen.name="frozen".into();store.forge_publish_view(&account,&actor,frozen,PolicyEpoch(2),false).unwrap();
    let historical=store.forge_resolve_view(&account,None,"",None).unwrap();let candidate=forge_fixture(&store,&account);
    {
        let _gate=store.inner.lock().unwrap();let mut tx=store.inner.db.write_tx().durability(Some(PersistMode::SyncAll));
        store.inner.forge_check_following_view_capacity_in(&tx,&account,candidate).unwrap();
        let d=store.inner.descriptor(&repo,ObjectKey{kind:Kind::Commit,id:ObjectId(candidate.0)}).unwrap().unwrap();
        tx.insert(&store.inner.spaces.refs,key(&repo,b"main"),encode(&StoredReference{generation:2,commit:Some(d.into())}).unwrap());
        store.inner.forge_advance_main_views_in(&mut tx,&repo,candidate).unwrap();store.inner.commit(tx).unwrap();
    }
    let current=store.forge_resolve_view(&account,None,"",None).unwrap();assert_eq!(current.revision(),candidate);assert_ne!(current.version(),historical.version());assert_eq!(current.epoch(),historical.epoch());
    assert_eq!(store.forge_read_view_file(&current,"README.md",1024).unwrap(),b"# Account\npublic needle\n");
    assert!(store.forge_view_files(&historical,10).unwrap().is_empty());assert_eq!(store.forge_resolve_view(&account,None,"public",Some(historical.version())).unwrap().revision().0,genesis.key.id.0);
    assert_eq!(store.forge_resolve_view(&account,None,"frozen",None).unwrap().revision().0,genesis.key.id.0);
    let snapshot=store.forge_get_account_snapshot(&account,&actor).unwrap();assert_eq!(snapshot["main_revision"],serde_json::json!(candidate.0));assert_eq!(snapshot["main_generation"],2);
    assert_eq!(store.forge_mutate_account(&account,&actor,"provision",ForgeAccountMutation::Provision).unwrap(),provision);
    // An uncommitted canonical transaction must not move the named pointer.
    {let _gate=store.inner.lock().unwrap();let mut tx=store.inner.db.write_tx();store.inner.forge_advance_main_views_in(&mut tx,&repo,CommitId(genesis.key.id.0)).unwrap();}
    assert_eq!(store.forge_resolve_view(&account,None,"",None).unwrap().revision(),candidate);
    drop(store);let store=Store::open_existing(directory.path(),config()).unwrap();assert_eq!(store.forge_resolve_view(&account,None,"",None).unwrap().revision(),candidate);
}

#[test]
fn forge_large_owned_catalogs_support_public_reads_reopen_and_exact_replay() {
    let directory=tempfile::tempdir().unwrap();let store=Store::open(directory.path(),config()).unwrap();let account=AccountId("catalogs".into());let actor=ForgeActor{principal:"owner".into()};store.forge_provision_account(&account,&actor).unwrap();let repo=account.repository_id();
    let file=tinytree_model::describe_file(b"catalog fixture\n");store.put(&repo,file,b"catalog fixture\n",0).unwrap();
    let tree=tinytree_model::encode(&Object::Tree(Tree{entries:(0..120).map(|i|TreeEntry{name:format!("file-{i:04}"),value:TreeValue::File{id:FileId(file.key.id.0),size_bytes:file.size_bytes,executable:false,copy_id:None}}).collect()}),Limits::default()).unwrap();store.put(&repo,tree.descriptor,&tree.bytes,0).unwrap();
    let source=tinytree_model::encode(&Object::Commit(commit(TreeId(tree.descriptor.key.id.0),"large owned catalogs")),Limits::default()).unwrap();store.put(&repo,source.descriptor,&source.bytes,0).unwrap();
    let mut epoch=PolicyEpoch(1);
    for i in 0..32 {epoch=store.forge_set_member(&account,&actor,ForgeMember{principal:format!("member-{i:02}"),can_write:false,can_review:false,is_admin:false,roles:(0..8).map(|r|format!("role-{r}-{}","x".repeat(64))).collect()},epoch).unwrap().epoch;}
    let mut spec=forge_public_view(CommitId(source.descriptor.key.id.0));spec.include_prefixes=(0..63).map(|i|format!("path-{i:02}/{}","p".repeat(512))).chain(std::iter::once(String::new())).collect();
    let mutation=ForgeAccountMutation::PublishView{spec:spec.clone(),expected_epoch:epoch,make_default:true,follow_main:false};
    let acknowledgment=store.forge_mutate_account(&account,&actor,"publish-catalog",mutation.clone()).unwrap();
    let view=store.forge_resolve_view(&account,None,"",None).unwrap();let projection=store.forge_project_view(&view,0).unwrap();
    assert_eq!(store.forge_view_files(&view,200).unwrap().len(),120);assert_eq!(store.forge_read_view_file(&view,"file-0119",1024).unwrap(),b"catalog fixture\n");
    assert!(store.forge_authorized_object_descriptor(&view,None,ObjectKey{kind:Kind::Commit,id:ObjectId(projection.projected_commit)},0).is_ok());
    {
        let tx=store.inner.db.read_tx();
        for prefix in [b"forge/accounts/v1/".as_slice(),b"forge/views/v1/".as_slice(),b"forge/projections/v1/".as_slice()] {
            assert!(tx.prefix(&store.inner.spaces.meta,prefix).any(|entry|entry.into_inner().unwrap().1.len()>16*1024),"representative catalog must exceed generic record limit");
        }
    }
    let mut oversized=spec.clone();oversized.include_prefixes.push("extra".into());
    assert!(store.forge_publish_view(&account,&actor,oversized,view.epoch(),true).is_err());
    let mut oversized=spec;oversized.include_prefixes=vec!["\"".repeat(4096);64];oversized.exclude_prefixes=oversized.include_prefixes.clone();
    assert!(matches!(store.forge_publish_view(&account,&actor,oversized,view.epoch(),true),Err(Error::Limit("forge view record"))));
    assert_eq!(store.forge_mutate_account(&account,&actor,"publish-catalog",mutation.clone()).unwrap(),acknowledgment);
    let genesis=store.references(&repo,&["main".into()]).unwrap()[0].commit.unwrap();
    let current=store.forge_publish_view(&account,&actor,forge_public_view(CommitId(genesis.key.id.0)),view.epoch(),true).unwrap();
    store.forge_retire_view(&account,&actor,view.version(),current.epoch()).unwrap();store.gc(&repo,100).unwrap();
    assert!(store.descriptor(&repo,file.key).unwrap().is_none());
    {
        let _gate=store.inner.lock().unwrap();assert_eq!(store.inner.forge_projection_relation_in(&store.inner.db.read_tx(),&account,view.version(),view.epoch(),CommitId(projection.projected_commit)).unwrap().0,source.descriptor.key.id.0);
    }
    drop(store);let store=Store::open_existing(directory.path(),config()).unwrap();
    assert_eq!(store.forge_mutate_account(&account,&actor,"publish-catalog",mutation).unwrap(),acknowledgment);
    assert_eq!(store.forge_resolve_view(&account,None,"",None).unwrap().revision().0,genesis.key.id.0);
}

#[test]
fn forge_rewritten_commit_predecessors_require_scoped_metadata_ownership() {
    let directory=tempfile::tempdir().unwrap();let store=Store::open(directory.path(),config()).unwrap();let account=AccountId("evolution".into());let actor=ForgeActor{principal:"writer".into()};let peer=ForgeActor{principal:"peer".into()};store.forge_provision_account(&account,&actor).unwrap();
    store.forge_set_member(&account,&actor,ForgeMember{principal:peer.principal.clone(),can_write:true,can_review:false,is_admin:false,roles:vec![]},PolicyEpoch(1)).unwrap();
    let canonical=forge_fixture(&store,&account);let view=store.forge_publish_view(&account,&actor,forge_public_view(canonical),PolicyEpoch(2),true).unwrap();let projection=store.forge_project_view(&view,0).unwrap();
    let peer_view=store.forge_resolve_view(&account,Some(&peer),"public",Some(view.version())).unwrap();
    let mut original=commit(TreeId(projection.projected_tree),"original authored source");original.parents=vec![CommitId(projection.projected_commit)];original.predecessors.clear();
    let original=tinytree_model::encode(&Object::Commit(original),Limits::default()).unwrap();store.forge_upload_object(&view,&actor,"",original.descriptor,&original.bytes,0).unwrap();
    let Object::Commit(mut rewritten)=tinytree_model::decode(&original.bytes,Limits::default()).unwrap()else{panic!("not a commit")};rewritten.description="legitimate describe rewrite".into();rewritten.predecessors=vec![CommitId(original.descriptor.key.id.0)];
    let rewritten=tinytree_model::encode(&Object::Commit(rewritten),Limits::default()).unwrap();store.forge_upload_object(&view,&actor,"",rewritten.descriptor,&rewritten.bytes,0).unwrap();
    assert_eq!(store.forge_read_authorized_object(&view,Some(&actor),rewritten.descriptor.key,0,4096).unwrap(),rewritten.bytes,"source identity/bytes must not be sanitized or dropped");
    let Object::Commit(mut victim)=tinytree_model::decode(&original.bytes,Limits::default()).unwrap()else{panic!("not a commit")};victim.description="peer-owned metadata not submitted by writer".into();let victim=tinytree_model::encode(&Object::Commit(victim),Limits::default()).unwrap();store.forge_upload_object(&peer_view,&peer,"",victim.descriptor,&victim.bytes,0).unwrap();
    for predecessor in [CommitId(victim.descriptor.key.id.0),canonical,CommitId::ROOT] {
        let Object::Commit(mut forged)=tinytree_model::decode(&rewritten.bytes,Limits::default()).unwrap()else{panic!("not a commit")};forged.predecessors=vec![predecessor];let forged=tinytree_model::encode(&Object::Commit(forged),Limits::default()).unwrap();
        assert!(matches!(store.forge_upload_object(&view,&actor,"",forged.descriptor,&forged.bytes,0),Err(Error::ForgeAccessDenied)),"known victim/private canonical predecessor digest is not an upload receipt");
    }
    let Object::Commit(mut excessive)=tinytree_model::decode(&rewritten.bytes,Limits::default()).unwrap()else{panic!("not a commit")};excessive.predecessors=vec![CommitId(original.descriptor.key.id.0);129];let excessive=tinytree_model::encode(&Object::Commit(excessive),Limits::default()).unwrap();assert!(matches!(store.forge_upload_object(&view,&actor,"",excessive.descriptor,&excessive.bytes,0),Err(Error::Invalid("forge uploaded commit shape"))));
    drop(store);let store=Store::open_existing(directory.path(),config()).unwrap();let view=store.forge_resolve_view(&account,Some(&actor),"public",None).unwrap();assert_eq!(store.forge_read_authorized_object(&view,Some(&actor),rewritten.descriptor.key,0,4096).unwrap(),rewritten.bytes);
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
fn commit_batches_preserve_mixed_receipts_exact_quota_gc_epochs_and_reopen() {
    let objects: Vec<_> = ["first", "second", "third"]
        .into_iter()
        .map(|description| {
            tinytree_model::encode(
                &Object::Commit(commit(
                    TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
                    description,
                )),
                Limits::default(),
            )
            .unwrap()
        })
        .collect();
    let directory = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.max_objects_per_repository = 4; // Intrinsic empty tree plus three commits.
    limits.max_repository_bytes = tinytree_model::empty_tree().descriptor.size_bytes
        + objects.iter().map(|o| o.descriptor.size_bytes).sum::<u64>();
    let store = Store::open(directory.path(), limits.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let first = store
        .put("demo", objects[0].descriptor, &objects[0].bytes, 0)
        .unwrap();
    let old_epoch = client_collection(&store, 100);
    let receipts = store.put_commit_batch("demo", &objects, 90).unwrap();
    assert_eq!(receipts.len(), 3);
    assert_eq!(receipts[0].staging_lease, first.staging_lease);
    for (receipt, object) in receipts.iter().zip(&objects) {
        assert_eq!(receipt.descriptor, object.descriptor);
        assert_eq!(receipt.expires_at, 100);
        assert_eq!(
            store
                .read_metadata("demo", object.descriptor.key)
                .unwrap()
                .1,
            object.bytes
        );
    }
    assert!(matches!(
        store.plan_client_gc("demo", &old_epoch),
        Err(Error::Invalid("stale client GC epoch"))
    ));
    let extra = tinytree_model::encode(
        &Object::Commit(commit(
            TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
            "extra",
        )),
        Limits::default(),
    )
    .unwrap();
    assert!(matches!(
        store.put_commit_batch("demo", &[extra.clone()], 91),
        Err(Error::Limit("repository quota"))
    ));
    assert_eq!(
        store.descriptor("demo", extra.descriptor.key).unwrap(),
        None
    );
    assert_eq!(
        store.put_commit_batch("demo", &objects, 5).unwrap(),
        receipts
    );
    drop(store);
    let store = Store::open_existing(directory.path(), limits).unwrap();
    assert_eq!(
        store
            .list_objects("demo", Kind::Commit, None, 8)
            .unwrap()
            .len(),
        3
    );
    for object in &objects {
        assert_eq!(
            store
                .read_metadata("demo", object.descriptor.key)
                .unwrap()
                .1,
            object.bytes
        );
    }
    assert_eq!(store.gc("demo", 99).unwrap().deleted_objects, 0);
    assert_eq!(store.gc("demo", 200).unwrap().deleted_objects, 3);
}

#[test]
fn commit_batches_reject_invalid_members_before_publication_and_repair_missing_payload() {
    let (_directory, store) = open();
    let objects: Vec<_> = ["first", "second"]
        .into_iter()
        .map(|description| {
            tinytree_model::encode(
                &Object::Commit(commit(
                    TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
                    description,
                )),
                Limits::default(),
            )
            .unwrap()
        })
        .collect();
    let mut corrupt = objects.clone();
    *corrupt[1].bytes.last_mut().unwrap() ^= 1;
    assert!(matches!(
        store.put_commit_batch("demo", &corrupt, 0),
        Err(Error::Model(_))
    ));
    assert!(matches!(
        store.put_commit_batch("demo", &[objects[0].clone(), objects[0].clone()], 0),
        Err(Error::Invalid("duplicate commit batch member"))
    ));
    assert!(matches!(
        store.put_commit_batch("demo", &[tinytree_model::empty_tree()], 0),
        Err(Error::Invalid("commit batch kind"))
    ));
    for object in &objects {
        assert_eq!(
            store.descriptor("demo", object.descriptor.key).unwrap(),
            None
        );
    }
    store
        .put("demo", objects[1].descriptor, &objects[1].bytes, 0)
        .unwrap();
    let payload = store.inner.path("demo", objects[1].descriptor.key);
    fs::write(&payload, &corrupt[1].bytes).unwrap();
    assert!(matches!(
        store.put_commit_batch("demo", &objects, 1),
        Err(Error::Model(tinytree_model::Error::HashMismatch))
    ));
    assert_eq!(
        store.descriptor("demo", objects[0].descriptor.key).unwrap(),
        None
    );
    assert!(!store.inner.path("demo", objects[0].descriptor.key).exists());
    fs::remove_file(&payload).unwrap();
    let receipts = store.put_commit_batch("demo", &objects, 2).unwrap();
    assert_eq!(receipts.len(), 2);
    for object in &objects {
        assert_eq!(
            store
                .read_metadata("demo", object.descriptor.key)
                .unwrap()
                .1,
            object.bytes
        );
    }
}

#[test]
fn commit_batch_late_receipt_error_has_no_partial_index_publication() {
    let (directory, store) = open();
    let objects: Vec<_> = ["first", "second"]
        .into_iter()
        .map(|description| {
            tinytree_model::encode(
                &Object::Commit(commit(
                    TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
                    description,
                )),
                Limits::default(),
            )
            .unwrap()
        })
        .collect();
    // Fault injection below the public API makes the SECOND receipt fail after
    // the first member's transaction writes. Neither index entry may escape.
    let stage_key = key("demo", lease_id(objects[1].descriptor.key).as_bytes());
    let mut tx = store
        .inner
        .db
        .write_tx()
        .durability(Some(PersistMode::SyncAll));
    tx.insert(
        &store.inner.spaces.staging,
        &stage_key,
        encode(&StageRecord {
            object: objects[0].descriptor.into(),
            expires_at: 100,
        })
        .unwrap(),
    );
    store.inner.commit(tx).unwrap();
    assert!(matches!(
        store.put_commit_batch("demo", &objects, 0),
        Err(Error::Corrupt("staging descriptor changed"))
    ));
    for object in &objects {
        assert_eq!(
            store.descriptor("demo", object.descriptor.key).unwrap(),
            None
        );
        assert!(matches!(
            store.read_metadata("demo", object.descriptor.key),
            Err(Error::MissingObject(_))
        ));
    }
    let mut tx = store
        .inner
        .db
        .write_tx()
        .durability(Some(PersistMode::SyncAll));
    tx.remove(&store.inner.spaces.staging, stage_key);
    store.inner.commit(tx).unwrap();
    assert_eq!(
        store.put_commit_batch("demo", &objects, 1).unwrap().len(),
        2
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store
            .list_objects("demo", Kind::Commit, None, 8)
            .unwrap()
            .len(),
        2
    );
    for object in &objects {
        assert_eq!(
            store
                .read_metadata("demo", object.descriptor.key)
                .unwrap()
                .1,
            object.bytes
        );
    }
}

#[test]
fn concurrent_commit_batches_reuse_immutable_payloads_without_double_counting() {
    let (_directory, store) = open();
    let objects: Vec<_> = ["first", "second", "third"]
        .into_iter()
        .map(|description| {
            tinytree_model::encode(
                &Object::Commit(commit(
                    TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
                    description,
                )),
                Limits::default(),
            )
            .unwrap()
        })
        .collect();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let other = store.clone();
    let other_objects = objects.clone();
    let other_barrier = barrier.clone();
    let worker = std::thread::spawn(move || {
        other_barrier.wait();
        other.put_commit_batch("demo", &other_objects, 1)
    });
    barrier.wait();
    let first = store.put_commit_batch("demo", &objects, 1).unwrap();
    let second = worker.join().unwrap().unwrap();
    assert_eq!(first, second);
    assert_eq!(
        store
            .list_objects("demo", Kind::Commit, None, 8)
            .unwrap()
            .len(),
        3
    );
    for object in &objects {
        assert_eq!(
            store
                .read_metadata("demo", object.descriptor.key)
                .unwrap()
                .1,
            object.bytes
        );
    }
}

#[test]
fn packed_commit_batch_releases_reconstruction_memory_without_a_gc_pin_gap() {
    let directory = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.max_packed_read_memory_bytes = pack::PACK_READ_CHARGE;
    let store = Store::open(directory.path(), limits.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let objects: Vec<_> = ["first", "second", "third"]
        .into_iter()
        .map(|description| {
            tinytree_model::encode(
                &Object::Commit(commit(
                    TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
                    description,
                )),
                Limits::default(),
            )
            .unwrap()
        })
        .collect();
    for object in &objects {
        store
            .put("demo", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let spare = tinytree_model::encode(
        &Object::Commit(commit(
            TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
            "unrooted spare",
        )),
        Limits::default(),
    )
    .unwrap();
    store
        .put("demo", spare.descriptor, &spare.bytes, 0)
        .unwrap();
    assert!(
        store
            .pack("demo", PackOptions::default())
            .unwrap()
            .packed_objects
            >= 3
    );
    for object in &objects {
        assert!(
            store
                .inner
                .is_packed("demo", object.descriptor.key)
                .unwrap()
        );
    }
    // Only one packed reconstruction fits. The complete chunk must still work,
    // retaining lightweight GC roots after each verified reader is consumed.
    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let resume = std::sync::Arc::new(std::sync::Barrier::new(2));
    *store.inner.commit_batch_pause.lock().unwrap() = Some((entered.clone(), resume.clone()));
    let writer = store.clone();
    let submitted = objects.clone();
    let worker = std::thread::spawn(move || writer.put_commit_batch("demo", &submitted, 200));
    entered.wait(); // All old ReadLease buffers dropped; lightweight roots remain.
    assert_eq!(
        store.gc("demo", 200).unwrap().deleted_objects,
        1,
        "GC must run and delete the spare, not any batch member"
    );
    assert_eq!(
        store.descriptor("demo", spare.descriptor.key).unwrap(),
        None
    );
    resume.wait();
    let receipts = worker.join().unwrap().unwrap();
    assert_eq!(receipts.len(), 3);
    assert!(receipts.iter().all(|receipt| receipt.expires_at == 210));
    drop(store);
    let store = Store::open_existing(directory.path(), limits).unwrap();
    for object in &objects {
        assert_eq!(
            store
                .read_metadata("demo", object.descriptor.key)
                .unwrap()
                .1,
            object.bytes
        );
    }
}

#[test]
fn commit_batch_bounds_follow_small_operator_staging_capacity() {
    let object = tinytree_model::encode(
        &Object::Commit(commit(
            TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
            "small",
        )),
        Limits::default(),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.max_staging_uploads = 1;
    limits.max_staging_bytes = object.descriptor.size_bytes;
    let store = Store::open(directory.path(), limits).unwrap();
    store.create_repository("demo").unwrap();
    assert_eq!(
        store.commit_batch_limits(),
        CommitBatchLimits {
            max_objects: 1,
            max_bytes: object.descriptor.size_bytes
        }
    );
    assert!(matches!(
        store.put_commit_batch("demo", &[object.clone(), object.clone()], 0),
        Err(Error::Limit("commit batch object count"))
    ));
    assert_eq!(
        store
            .put_commit_batch("demo", &[object.clone()], 0)
            .unwrap()[0]
            .descriptor,
        object.descriptor
    );
    let larger = tinytree_model::encode(
        &Object::Commit(commit(
            TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
            "larger than configured capacity",
        )),
        Limits::default(),
    )
    .unwrap();
    assert!(matches!(
        store.put_commit_batch("demo", &[larger.clone()], 1),
        Err(Error::Limit("commit batch bytes"))
    ));
    assert_eq!(
        store.descriptor("demo", larger.descriptor.key).unwrap(),
        None
    );
    assert!(store.put_commit_batch("demo", &[object], 2).is_ok());
}

#[test]
fn resident_metadata_reuses_durable_backing_but_refreshes_receipt_and_gc_epoch() {
    let (directory, store) = open();
    let object = tinytree_model::encode(
        &Object::Commit(commit(
            TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
            "resident metadata",
        )),
        Limits::default(),
    )
    .unwrap();
    let first = store
        .put("demo", object.descriptor, &object.bytes, 0)
        .unwrap();
    let old_epoch = client_collection(&store, 100);
    let payload = store.inner.path("demo", object.descriptor.key);
    let staging = directory.path().join("staging");
    let aside = directory.path().join("staging-aside");
    fs::rename(&staging, &aside).unwrap();
    fs::write(&staging, b"staging deliberately unavailable").unwrap();
    // A verified resident hit needs no payload staging even though its durable
    // staging receipt and age/revision still have to be refreshed.
    let refreshed = store
        .put("demo", object.descriptor, &object.bytes, 90)
        .unwrap();
    assert_eq!(refreshed.descriptor, first.descriptor);
    assert_eq!(refreshed.staging_lease, first.staging_lease);
    assert_eq!(refreshed.expires_at, 100);
    let older = store
        .put("demo", object.descriptor, &object.bytes, 5)
        .unwrap();
    assert_eq!(older.expires_at, refreshed.expires_at);
    assert!(matches!(
        store.plan_client_gc("demo", &old_epoch),
        Err(Error::Invalid("stale client GC epoch"))
    ));
    assert_eq!(fs::read(&payload).unwrap(), object.bytes);
    let (descriptor, bytes) = store.read_metadata("demo", object.descriptor.key).unwrap();
    assert_eq!(descriptor, object.descriptor);
    assert_eq!(bytes, object.bytes);
    // Arbitrary streams retain their existing consume/verify/install path.
    assert!(matches!(
        store.put_stream(
            "demo",
            object.descriptor,
            &mut io::Cursor::new(&object.bytes),
            91,
        ),
        Err(Error::Io(_))
    ));
    fs::remove_file(&staging).unwrap();
    fs::rename(&aside, &staging).unwrap();
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(store.gc("demo", 99).unwrap().deleted_objects, 0);
    assert_eq!(store.gc("demo", 200).unwrap().deleted_objects, 1);
    assert_eq!(
        store.descriptor("demo", object.descriptor.key).unwrap(),
        None
    );
}

#[test]
fn metadata_duplicates_verify_submission_and_backing_and_repair_missing_loose_payload() {
    let (_directory, store) = open();
    let object = tinytree_model::encode(
        &Object::Commit(commit(
            TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
            "verified metadata",
        )),
        Limits::default(),
    )
    .unwrap();
    store
        .put("demo", object.descriptor, &object.bytes, 0)
        .unwrap();
    let mut corrupt = object.bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(matches!(
        store.put("demo", object.descriptor, &corrupt, 1),
        Err(Error::Model(_))
    ));
    let payload = store.inner.path("demo", object.descriptor.key);
    fs::write(&payload, &corrupt).unwrap();
    assert!(matches!(
        store.put("demo", object.descriptor, &object.bytes, 2),
        Err(Error::Model(tinytree_model::Error::HashMismatch))
    ));
    assert_eq!(fs::read(&payload).unwrap(), corrupt);
    // Preserve the existing install path's ability to restore a missing loose
    // payload; a descriptor row without its bytes is not a resident hit.
    fs::remove_file(&payload).unwrap();
    store
        .put("demo", object.descriptor, &object.bytes, 3)
        .unwrap();
    assert_eq!(
        store
            .read_metadata("demo", object.descriptor.key)
            .unwrap()
            .1,
        object.bytes
    );
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
        assert!(store.plan_client_gc("demo", &request).is_err());
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
        store.plan_client_gc("demo", &request),
        Err(Error::Invalid("stale client GC epoch"))
    ));
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
        store.plan_client_gc("demo", &old_instance),
        Err(Error::Invalid("stale client GC epoch"))
    ));
    assert!(matches!(
        store.client_gc("demo", &old_instance),
        Err(Error::Invalid("stale client GC epoch"))
    ));
    store.create_repository("other").unwrap();
    assert!(matches!(
        store.plan_client_gc("other", &client_collection(&store, 200)),
        Err(Error::Invalid("stale client GC epoch"))
    ));
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
            store.plan_client_gc("demo", &request),
            Err(Error::Limit(_))
        ));
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
fn client_gc_plan_preserves_payloads_catalog_and_recovery_work() {
    let (_directory, store) = open();
    let (file, tree, head) = fixture(&store, "retained history");
    let spare_bytes = b"unreachable resident";
    let spare = tinytree_model::describe_file(spare_bytes);
    store.put("demo", spare, spare_bytes, 0).unwrap();
    let legacy_bytes = b"unknown age must not be stamped by a plan";
    let legacy = tinytree_model::describe_file(legacy_bytes);
    store.put("demo", legacy, legacy_bytes, 0).unwrap();
    let pending_bytes = b"pending physical deletion";
    let pending = tinytree_model::describe_file(pending_bytes);
    store.put("demo", pending, pending_bytes, 0).unwrap();
    client_gc_pending_unlink(&store, pending, true);
    let mut tx = store
        .inner
        .db
        .write_tx()
        .durability(Some(PersistMode::SyncAll));
    tx.remove(
        &store.inner.spaces.client_ages,
        object_key("demo", legacy.key),
    );
    store.inner.commit(tx).unwrap();

    // Observe every durable namespace that selection or application can touch.
    // Raw rows also catch a stamp/lease update that leaves descriptors unchanged.
    let catalog = || {
        let view = store.inner.db.read_tx();
        [
            &store.inner.spaces.repos,
            &store.inner.spaces.objects,
            &store.inner.spaces.refs,
            &store.inner.spaces.staging,
            &store.inner.spaces.client_ages,
            &store.inner.spaces.client_revisions,
            &store.inner.spaces.client_deletions,
            &store.inner.spaces.locations,
            &store.inner.spaces.packs,
        ]
        .map(|space| {
            view.prefix(space, b"")
                .map(|row| {
                    let (key, value) = row.into_inner().unwrap();
                    (key.to_vec(), value.to_vec())
                })
                .collect::<Vec<_>>()
        })
    };
    let before = catalog();
    let mut request = client_collection(&store, 100);
    request.complete_roots.push(head.key);
    let plan = store.plan_client_gc("demo", &request).unwrap();
    assert_eq!(plan.would_delete_objects, 1);
    assert_eq!(plan.would_delete_bytes, spare.size_bytes);
    assert_eq!(plan.pending_deletion_objects, 1);
    assert_eq!(plan.pending_deletion_bytes, pending.size_bytes);
    // Repository creation predates age rows for the intrinsic empty tree.
    assert_eq!(plan.would_stamp_unknown_objects, 2);
    assert_eq!(plan.would_expire_leases, before[3].len() as u64);
    assert!(plan.would_expire_leases > 0);
    assert_eq!(catalog(), before);
    assert_eq!(store.plan_client_gc("demo", &request).unwrap(), plan);
    assert_eq!(catalog(), before);
    for (descriptor, bytes) in [
        (spare, spare_bytes.as_slice()),
        (legacy, legacy_bytes.as_slice()),
        (pending, pending_bytes.as_slice()),
    ] {
        assert_eq!(
            fs::read(store.inner.path("demo", descriptor.key)).unwrap(),
            bytes
        );
    }
    for descriptor in [file, tree, head, spare, legacy] {
        assert_eq!(
            store.descriptor("demo", descriptor.key).unwrap(),
            Some(descriptor)
        );
    }
    assert_eq!(store.descriptor("demo", pending.key).unwrap(), None);

    // Reusing the same epoch proves planning neither advances the catalog nor
    // pre-applies recovery work. Application performs a new gated selection.
    let applied = store.client_gc("demo", &request).unwrap();
    assert_eq!(applied.deleted_objects, plan.would_delete_objects);
    assert_eq!(applied.deleted_bytes, plan.would_delete_bytes);
    assert_eq!(
        applied.stamped_unknown_objects,
        plan.would_stamp_unknown_objects
    );
    assert_eq!(applied.expired_leases, plan.would_expire_leases);
    assert_eq!(applied.unlinked_payloads, 2);
    assert!(!store.inner.path("demo", spare.key).exists());
    assert!(!store.inner.path("demo", pending.key).exists());
    assert_eq!(store.descriptor("demo", legacy.key).unwrap(), Some(legacy));

    // A plan must reject prospective accounting underflow just as apply does,
    // without consuming its epoch or making partial stamps/recovery changes.
    let repository = store.inner.repository("demo").unwrap();
    for (corrupt, expected) in [
        (
            RepositoryRecord {
                bytes: 0,
                ..repository.clone()
            },
            "repository byte count",
        ),
        (
            RepositoryRecord {
                objects: 0,
                ..repository.clone()
            },
            "repository object count",
        ),
    ] {
        let mut tx = store
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        tx.insert(&store.inner.spaces.repos, "demo", encode(&corrupt).unwrap());
        store.inner.commit(tx).unwrap();
        let mut request = client_collection(&store, 200);
        request.complete_roots.push(head.key);
        let before = catalog();
        assert!(matches!(
            store.plan_client_gc("demo", &request),
            Err(Error::Corrupt(actual)) if actual == expected
        ));
        assert_eq!(catalog(), before);
        assert!(matches!(
            store.client_gc("demo", &request),
            Err(Error::Corrupt(actual)) if actual == expected
        ));
        assert_eq!(catalog(), before);
        assert_eq!(store.descriptor("demo", legacy.key).unwrap(), Some(legacy));
        assert_eq!(
            fs::read(store.inner.path("demo", legacy.key)).unwrap(),
            legacy_bytes
        );
    }
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
    let (_, commit_bytes) = store.read_metadata("demo", head.key).unwrap();
    store
        .put_commit_batch(
            "demo",
            &[tinytree_model::EncodedObject {
                descriptor: head,
                bytes: commit_bytes.clone(),
            }],
            2,
        )
        .unwrap();
    store.put("demo", head, &commit_bytes, 2).unwrap();
    assert!(
        !store.inner.path("demo", head.key).exists(),
        "verified resident metadata must reuse its packed backing"
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
    let pack_before = fs::read(&pack_path).unwrap();
    let plan = store.plan_client_gc("demo", &request).unwrap();
    assert_eq!(plan.would_delete_objects, 1);
    assert_eq!(plan.would_delete_bytes, target.size_bytes);
    assert_eq!(fs::read(&pack_path).unwrap(), pack_before);
    assert_eq!(store.descriptor("demo", target.key).unwrap(), Some(target));
    assert_eq!(
        store.client_gc("demo", &request).unwrap().deleted_objects,
        plan.would_delete_objects
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

fn cloud_view(native: CommitId) -> tinytree_cloud::VerifiedObject {
    cloud_view_with_heads(native, 0)
}
fn cloud_view_with_heads(native: CommitId, extra: usize) -> tinytree_cloud::VerifiedObject {
    use jj_lib::{
        object_id::ObjectId as _,
        op_store::{OpStore, RootOperationData, View},
        simple_op_store::SimpleOpStore,
    };
    futures::executor::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let source = SimpleOpStore::init(
            directory.path(),
            RootOperationData {
                root_commit_id: jj_lib::backend::CommitId::from_bytes(&[0; 32]),
            },
        )
        .unwrap();
        let id = jj_lib::backend::CommitId::from_bytes(&native.0);
        let mut view = View::make_root(id.clone());
        for number in 0..extra {
            let raw = blake3::hash(format!("cloud-summary-head-{number}").as_bytes());
            view.head_ids
                .insert(jj_lib::backend::CommitId::from_bytes(raw.as_bytes()));
        }
        view.wc_commit_ids.insert("task".into(), id);
        let id = source.write_view(&view).await.unwrap();
        tinytree_cloud::export_view(&source, &id, tinytree_cloud::Limits::default())
            .await
            .unwrap()
    })
}
fn reverse_cloud_fields(bytes: &[u8]) -> Vec<u8> {
    fn varint(bytes: &[u8], cursor: &mut usize) -> u64 {
        let mut value = 0u64;
        for shift in (0..70).step_by(7) {
            let byte = bytes[*cursor];
            *cursor += 1;
            value |= ((byte & 127) as u64) << shift;
            if byte & 128 == 0 {
                return value;
            }
        }
        panic!("fixture varint");
    }
    let mut cursor = 0usize;
    let mut fields = Vec::new();
    while cursor < bytes.len() {
        let start = cursor;
        let tag = varint(bytes, &mut cursor);
        match tag & 7 {
            0 => {
                varint(bytes, &mut cursor);
            }
            1 => cursor += 8,
            2 => {
                let length = varint(bytes, &mut cursor) as usize;
                cursor += length;
            }
            5 => cursor += 4,
            _ => panic!("fixture wire type"),
        };
        fields.push(&bytes[start..cursor]);
    }
    fields.reverse();
    fields.concat()
}
#[test]
fn cloud_catalog_selects_verified_representation_and_isolates_owner_and_repository() {
    let (directory, store) = open();
    store.create_repository("other").unwrap();
    let owner = CloudOwner {
        principal: [31; 32],
    };
    let other = CloudOwner {
        principal: [32; 32],
    };
    let object = cloud_view(CommitId([9; 32]));
    let bytes = object.payload().unwrap();
    let receipt = store
        .put_cloud_object(
            "demo",
            owner,
            object.id(),
            object.digest().unwrap(),
            bytes,
            0,
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    let reordered = reverse_cloud_fields(bytes);
    assert_ne!(reordered, bytes);
    let alternate = tinytree_cloud::BlobDigest::of(&reordered);
    tinytree_cloud::VerifiedObject::verify(
        object.id(),
        alternate,
        &reordered,
        tinytree_cloud::Limits::default(),
    )
    .unwrap();
    let again = store
        .put_cloud_object(
            "demo",
            owner,
            object.id(),
            alternate,
            &reordered,
            1,
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    assert_eq!(receipt.descriptor, again.descriptor);
    assert_eq!(receipt.staging_lease, again.staging_lease);
    assert_eq!(
        store
            .lookup_cloud_objects("demo", other, &[object.id()])
            .unwrap(),
        vec![None]
    );
    assert_eq!(
        store
            .lookup_cloud_objects("other", owner, &[object.id()])
            .unwrap(),
        vec![None]
    );
    assert!(
        store
            .read_cloud_object(
                "demo",
                other,
                object.id(),
                tinytree_cloud::Limits::default()
            )
            .is_err()
    );
    let mut lease = store
        .read_cloud_object(
            "demo",
            owner,
            object.id(),
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    let mut actual = Vec::new();
    lease.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, bytes);
    drop(lease);
    assert!(
        store
            .client_gc("demo", &client_collection(&store, 2))
            .is_err()
    );
    drop(store);
    let reopened = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        reopened
            .lookup_cloud_objects("demo", owner, &[object.id()])
            .unwrap(),
        vec![Some(receipt.descriptor)]
    );
}
#[test]
fn exact_cloud_representations_preserve_primary_history_and_raw_bytes_through_gc_and_reopen() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [73; 32],
    };
    let (_, root, head) = fixture(&store, "exact cloud representations");
    let (manifest, leases) = cloud_capture(&store, owner, head, root);
    let first = cloud_request(&manifest, 0, "primary-representations", &leases);
    let limits = CloudPublicationLimits::default();
    store
        .publish_cloud_version("demo", owner, &first, 1, &limits)
        .unwrap();
    let mut next = manifest.clone();
    next.sequence = 2;
    next.parents = vec![first.manifest];
    let mut exact_leases = Vec::new();
    let mut raw = Vec::new();
    let mut changed = [false; 2];
    for member in &mut next.members {
        let mut primary = store
            .read_cloud_object("demo", owner, member.id, tinytree_cloud::Limits::default())
            .unwrap();
        let mut bytes = Vec::new();
        primary.read_to_end(&mut bytes).unwrap();
        drop(primary);
        let alternate = reverse_cloud_fields(&bytes);
        let digest = tinytree_cloud::BlobDigest::of(&alternate);
        let exact = store
            .put_cloud_object_representation(
                "demo",
                owner,
                member.id,
                digest,
                &alternate,
                2,
                tinytree_cloud::Limits::default(),
            )
            .unwrap();
        assert_eq!(exact.descriptor.selected, digest);
        assert_eq!(
            store
                .lookup_cloud_objects("demo", owner, &[member.id])
                .unwrap()[0]
                .unwrap()
                .selected,
            member.selected
        );
        if alternate != bytes {
            changed[match member.id {
                tinytree_cloud::ObjectId::Operation(_) => 0,
                _ => 1,
            }] = true;
            assert_ne!(
                exact.staging_lease,
                leases
                    .iter()
                    .find(|r| r.descriptor.id == member.id)
                    .unwrap()
                    .staging_lease
            );
        }
        let wrong = CloudOwner {
            principal: [74; 32],
        };
        assert!(
            store
                .read_cloud_object_representation(
                    "demo",
                    wrong,
                    exact.descriptor,
                    tinytree_cloud::Limits::default()
                )
                .is_err()
        );
        raw.push((exact.descriptor, alternate));
        member.selected = digest;
        exact_leases.push(exact);
    }
    assert_eq!(changed, [true, true]);
    let second = cloud_request(&next, 1, "exact-representations", &exact_leases);
    store
        .publish_cloud_version("demo", owner, &second, 3, &limits)
        .unwrap();
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.meta, b"format")
            .unwrap()
            .unwrap()
            .as_ref(),
        b"tinytree-store-11"
    );
    store.gc("demo", 100_000).unwrap();
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    for (d, expected) in raw {
        let mut lease = store
            .read_cloud_object_representation("demo", owner, d, tinytree_cloud::Limits::default())
            .unwrap();
        let mut actual = Vec::new();
        lease.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, expected);
    }
    for (generation, expected) in [(1, &first), (2, &second)] {
        assert_eq!(
            store
                .read_cloud_version(
                    "demo",
                    owner,
                    manifest.workspace,
                    generation,
                    limits.manifest
                )
                .unwrap()
                .manifest_bytes(),
            expected.manifest_bytes
        );
    }
}

#[test]
fn exact_cloud_put_pins_expired_primary_until_variant_commit_and_readers_pin_all_encodings() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [76; 32],
    };
    let object = cloud_view(CommitId([0; 32]));
    let original = object.payload().unwrap();
    let alternate = reverse_cloud_fields(original);
    assert_ne!(original, alternate);
    store
        .put_cloud_object(
            "demo",
            owner,
            object.id(),
            object.digest().unwrap(),
            original,
            0,
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    *store.inner.variant_put_pause.lock().unwrap() = Some((entered.clone(), release.clone()));
    let child = store.clone();
    let id = object.id();
    let bytes = alternate.clone();
    let handle = std::thread::spawn(move || {
        child.put_cloud_object_representation(
            "demo",
            owner,
            id,
            tinytree_cloud::BlobDigest::of(&bytes),
            &bytes,
            0,
            tinytree_cloud::Limits::default(),
        )
    });
    entered.wait();
    assert_eq!(store.gc("demo", 100_000).unwrap().deleted_cloud_objects, 0);
    assert_eq!(
        store.lookup_cloud_objects("demo", owner, &[id]).unwrap()[0]
            .unwrap()
            .selected,
        object.digest().unwrap()
    );
    release.wait();
    let receipt = handle.join().unwrap().unwrap();
    *store.inner.variant_put_pause.lock().unwrap() = None;
    let mut lease = store
        .read_cloud_object_representation(
            "demo",
            owner,
            receipt.descriptor,
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    assert_eq!(store.gc("demo", 100_000).unwrap().deleted_cloud_objects, 0);
    let mut actual = Vec::new();
    lease.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, alternate);
    drop(lease);
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(store.gc("demo", 100_000).unwrap().deleted_cloud_objects, 2);
    assert!(store.lookup_cloud_objects("demo", owner, &[id]).unwrap()[0].is_none());
    drop(store);
    Store::open_existing(directory.path(), config()).unwrap();
}

#[test]
fn exact_cloud_variant_quota_refusal_does_not_install_raw_or_advance_format() {
    let directory = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.cloud.max_objects_per_owner = 1;
    let store = Store::open(directory.path(), cfg.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [77; 32],
    };
    let object = cloud_view(CommitId([0; 32]));
    let bytes = object.payload().unwrap();
    let alternate = reverse_cloud_fields(bytes);
    let digest = tinytree_cloud::BlobDigest::of(&alternate);
    assert_ne!(digest, object.digest().unwrap());
    let primary = store
        .put_cloud_object(
            "demo",
            owner,
            object.id(),
            object.digest().unwrap(),
            bytes,
            0,
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    assert!(matches!(
        store.put_cloud_object_representation(
            "demo",
            owner,
            object.id(),
            digest,
            &alternate,
            0,
            tinytree_cloud::Limits::default()
        ),
        Err(Error::Limit(_))
    ));
    let namespace = fs::read_dir(directory.path().join("cloud"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert!(!namespace.join(hex(&digest.blake3)).exists());
    assert!(
        store
            .inner
            .db
            .read_tx()
            .iter(
                &store
                    .inner
                    .db
                    .keyspace("cloud-opview-variants-v1", KeyspaceCreateOptions::default)
                    .unwrap()
            )
            .next()
            .is_none()
    );
    assert_ne!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.meta, b"format")
            .unwrap()
            .unwrap()
            .as_ref(),
        b"tinytree-store-11"
    );
    assert_eq!(
        store
            .lookup_cloud_objects("demo", owner, &[object.id()])
            .unwrap(),
        vec![Some(primary.descriptor)]
    );
    drop(store);
    Store::open_existing(directory.path(), cfg).unwrap();
}

#[test]
fn exact_cloud_variant_only_promise_charges_primary_anchor_and_reopen_rejects_format_faults() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [78; 32],
    };
    let (file, tree, head) = fixture(&store, "variant-only retained promise");
    let (mut manifest, _) = cloud_capture(&store, owner, head, tree);
    let primary = manifest.members.clone();
    let mut raws = std::collections::BTreeMap::new();
    let mut leases = Vec::new();
    for member in &mut manifest.members {
        let mut lease = store
            .read_cloud_object("demo", owner, member.id, Default::default())
            .unwrap();
        let mut bytes = Vec::new();
        lease.read_to_end(&mut bytes).unwrap();
        drop(lease);
        raws.insert(member.selected.blake3, member.selected.size_bytes);
        let mut alternate = bytes.clone();
        alternate.extend_from_slice(&[0xa0, 0x06, 1]);
        let digest = tinytree_cloud::BlobDigest::of(&alternate);
        let receipt = store
            .put_cloud_object_representation(
                "demo",
                owner,
                member.id,
                digest,
                &alternate,
                0,
                Default::default(),
            )
            .unwrap();
        member.selected = digest;
        raws.insert(digest.blake3, digest.size_bytes);
        leases.push(receipt);
    }
    let raw_bytes = raws.values().sum::<u64>();
    let exact = raw_bytes + file.size_bytes + tree.size_bytes + head.size_bytes;
    let request = cloud_request(&manifest, 0, "variant-only-promise", &leases);
    let mut limits = CloudPublicationLimits::default();
    limits.retention.max_unique_bytes_per_principal = exact - 1;
    assert!(matches!(
        store.publish_cloud_version("demo", owner, &request, 1, &limits),
        Err(Error::Limit(_))
    ));
    assert!(
        store
            .cloud_publication("demo", owner, "variant-only-promise")
            .unwrap()
            .is_none()
    );
    limits.retention.max_unique_bytes_per_principal = exact;
    store
        .publish_cloud_version("demo", owner, &request, 1, &limits)
        .unwrap();
    store.gc("demo", 100_000).unwrap();
    for anchor in primary {
        assert_eq!(
            store
                .lookup_cloud_objects("demo", owner, &[anchor.id])
                .unwrap()[0]
                .unwrap()
                .selected,
            anchor.selected
        );
    }
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store
            .read_cloud_version("demo", owner, manifest.workspace, 1, limits.manifest)
            .unwrap()
            .manifest_bytes(),
        request.manifest_bytes
    );
    // Security boundary: an oversized corrupt catalog row is refused through
    // the public exact reader before dependency decoding or a raw-row copy.
    {
        let space = store
            .inner
            .db
            .keyspace("cloud-opview-variants-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let (key, old) = store
            .inner
            .db
            .read_tx()
            .iter(&space)
            .next()
            .unwrap()
            .into_inner()
            .unwrap();
        let mut tx = store.inner.db.write_tx();
        tx.insert(&space, key.clone(), vec![0u8; 1024 * 1024 + 1]);
        tx.commit().unwrap();
        let member = &manifest.members[0];
        assert!(matches!(
            store.read_cloud_object_representation(
                "demo",
                owner,
                CloudObjectDescriptor {
                    id: member.id,
                    selected: member.selected
                },
                Default::default()
            ),
            Err(Error::Corrupt("cloud metadata row size"))
        ));
        let mut tx = store.inner.db.write_tx();
        tx.insert(&space, key, old);
        tx.commit().unwrap();
    }
    drop(store);
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        let mut tx = db.write_tx();
        tx.insert(&meta, b"format", b"tinytree-store-10");
        tx.commit().unwrap();
    }
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("cloud variants precede feature"))
    ));
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        let mut tx = db.write_tx();
        tx.insert(&meta, b"format", b"tinytree-store-11");
        tx.commit().unwrap();
        let space = db
            .keyspace("cloud-opview-variants-v1", KeyspaceCreateOptions::default)
            .unwrap();
        db.inner().delete_keyspace(space.inner().clone()).unwrap();
    }
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("cloud variant keyspace missing"))
    ));
}

#[test]
fn staged_cloud_readers_protect_present_native_descendants_until_actual_drop() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = config();
    policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 2 * 1024 * 1024 * 1024,
        max_entries: 65_536,
    });
    let store = Store::open(directory.path(), policy.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let (file, _tree, head) = fixture(&store, "cloud source");
    let owner = CloudOwner {
        principal: [33; 32],
    };
    let object = cloud_view(CommitId(head.key.id.0));
    let payload = object.payload().unwrap();
    store
        .put_cloud_object(
            "demo",
            owner,
            object.id(),
            object.digest().unwrap(),
            payload,
            5,
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    let report = store.gc("demo", 11).unwrap();
    assert_eq!(report.deleted_cloud_objects, 0);
    assert_eq!(store.descriptor("demo", file.key).unwrap(), Some(file));
    let lease = store
        .read_cloud_object(
            "demo",
            owner,
            object.id(),
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    let report = store.gc("demo", 16).unwrap();
    assert_eq!(report.expired_cloud_leases, 1);
    assert_eq!(report.deleted_cloud_objects, 0);
    assert_eq!(store.descriptor("demo", head.key).unwrap(), Some(head));
    drop(lease);
    let report = store.gc("demo", 16).unwrap();
    assert_eq!(report.deleted_cloud_objects, 1);
    assert_eq!(report.deleted_cloud_bytes, payload.len() as u64);
    assert_eq!(store.descriptor("demo", file.key).unwrap(), None);
    assert_eq!(
        store
            .lookup_cloud_objects("demo", owner, &[object.id()])
            .unwrap(),
        vec![None]
    );
    let empty_owner = store.cloud_catalog_budget_summary().unwrap();
    assert_eq!(empty_owner.catalog_records, 0);
    assert_eq!(
        empty_owner.namespaces, 1,
        "GC retains the emptied accounting namespace"
    );
    drop(store);
    let reopened = Store::open_existing(directory.path(), policy).unwrap();
    assert_eq!(
        reopened.cloud_catalog_budget_summary().unwrap(),
        empty_owner
    );
}
#[test]
fn cloud_feature_upgrade_survives_packing_and_missing_required_keyspace_fails_closed() {
    let (directory, store) = open();
    fixture(&store, "cloud pack compatibility");
    store.pack("demo", PackOptions::default()).unwrap();
    let owner = CloudOwner {
        principal: [34; 32],
    };
    let object = cloud_view(CommitId([9; 32]));
    store
        .put_cloud_object(
            "demo",
            owner,
            object.id(),
            object.digest().unwrap(),
            object.payload().unwrap(),
            0,
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    store.pack("demo", PackOptions::default()).unwrap();
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.meta, b"format")
            .unwrap()
            .unwrap()
            .as_ref(),
        b"tinytree-store-3"
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert!(
        store
            .read_cloud_object(
                "demo",
                owner,
                object.id(),
                tinytree_cloud::Limits::default()
            )
            .is_ok()
    );
    drop(store);
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let space = db
            .keyspace("cloud-native-roots-v1", KeyspaceCreateOptions::default)
            .unwrap();
        db.inner().delete_keyspace(space.inner().clone()).unwrap();
    }
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("cloud keyspace missing"))
    ));
}
#[test]
fn native_and_cloud_objects_share_repository_physical_quota() {
    let directory = tempfile::tempdir().unwrap();
    let object = cloud_view(CommitId([9; 32]));
    let mut cfg = config();
    cfg.max_repository_bytes =
        tinytree_model::empty_tree().descriptor.size_bytes + object.digest().unwrap().size_bytes;
    let store = Store::open(directory.path(), cfg).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [35; 32],
    };
    store
        .put_cloud_object(
            "demo",
            owner,
            object.id(),
            object.digest().unwrap(),
            object.payload().unwrap(),
            0,
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    let spare = tinytree_model::describe_file(b"new");
    assert!(matches!(
        store.put("demo", spare, b"new", 0),
        Err(Error::Limit(_))
    ));
    assert_eq!(
        store
            .lookup_cloud_objects("demo", owner, &[object.id()])
            .unwrap()[0]
            .unwrap()
            .selected,
        object.digest().unwrap()
    );
}

#[test]
fn concurrent_first_cloud_uploads_share_one_durable_owner_namespace() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [36; 32],
    };
    let objects = (1..=8)
        .map(|id| cloud_view(CommitId([id; 32])))
        .collect::<Vec<_>>();
    let barrier = Arc::new(std::sync::Barrier::new(objects.len()));
    let threads = objects
        .into_iter()
        .map(|object| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .put_cloud_object(
                        "demo",
                        owner,
                        object.id(),
                        object.digest().unwrap(),
                        object.payload().unwrap(),
                        0,
                        tinytree_cloud::Limits::default(),
                    )
                    .unwrap()
                    .descriptor
            })
        })
        .collect::<Vec<_>>();
    let selected = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    for descriptor in selected {
        assert_eq!(
            store
                .lookup_cloud_objects("demo", owner, &[descriptor.id])
                .unwrap(),
            vec![Some(descriptor)]
        );
    }
}

#[test]
fn cloud_dependency_rows_above_native_metadata_bound_remain_readable_and_reopenable() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [37; 32],
    };
    let object = cloud_view_with_heads(CommitId([9; 32]), 1000);
    let receipt = store
        .put_cloud_object(
            "demo",
            owner,
            object.id(),
            object.digest().unwrap(),
            object.payload().unwrap(),
            0,
            tinytree_cloud::Limits::default(),
        )
        .unwrap();
    let mut actual = Vec::new();
    store
        .read_cloud_object(
            "demo",
            owner,
            object.id(),
            tinytree_cloud::Limits::default(),
        )
        .unwrap()
        .read_to_end(&mut actual)
        .unwrap();
    assert_eq!(actual, object.payload().unwrap());
    // The summary contains 1001 explicit native identities and cannot fit the
    // ordinary native catalog's 16 KiB JSON bound.
    assert!(object.dependencies().native_commits.len() > 1000);
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store
            .lookup_cloud_objects("demo", owner, &[object.id()])
            .unwrap(),
        vec![Some(receipt.descriptor)]
    );
    store.gc("demo", 1).unwrap();
}
#[test]
fn oversized_cloud_dependency_row_is_rejected_before_durable_catalog_ack() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [38; 32],
    };
    let object = cloud_view_with_heads(CommitId([9; 32]), 12000);
    let result = store.put_cloud_object(
        "demo",
        owner,
        object.id(),
        object.digest().unwrap(),
        object.payload().unwrap(),
        0,
        tinytree_cloud::Limits::default(),
    );
    assert!(matches!(
        result,
        Err(Error::Limit("cloud metadata row bytes"))
    ));
    assert_eq!(
        store
            .lookup_cloud_objects("demo", owner, &[object.id()])
            .unwrap(),
        vec![None]
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store
            .lookup_cloud_objects("demo", owner, &[object.id()])
            .unwrap(),
        vec![None]
    );
}

fn cloud_capture(
    store: &Store,
    owner: CloudOwner,
    head: Descriptor,
    root: Descriptor,
) -> (tinytree_cloud::Manifest, Vec<CloudWriteReceipt>) {
    cloud_capture_with_refs(store, owner, head, root, 0)
}
fn cloud_capture_with_refs(
    store: &Store,
    owner: CloudOwner,
    head: Descriptor,
    root: Descriptor,
    bookmarks: usize,
) -> (tinytree_cloud::Manifest, Vec<CloudWriteReceipt>) {
    cloud_capture_with_parent_edges(store, owner, head, root, bookmarks, 1)
}
fn cloud_capture_with_parent_edges(
    store: &Store,
    owner: CloudOwner,
    head: Descriptor,
    root: Descriptor,
    bookmarks: usize,
    parent_edges: usize,
) -> (tinytree_cloud::Manifest, Vec<CloudWriteReceipt>) {
    cloud_capture_with_attributes(
        store,
        owner,
        head,
        root,
        bookmarks,
        parent_edges,
        Default::default(),
    )
}
fn cloud_capture_with_attributes(
    store: &Store,
    owner: CloudOwner,
    head: Descriptor,
    root: Descriptor,
    bookmarks: usize,
    parent_edges: usize,
    attributes: std::collections::BTreeMap<String, String>,
) -> (tinytree_cloud::Manifest, Vec<CloudWriteReceipt>) {
    use jj_lib::{
        backend::{MillisSinceEpoch, Timestamp as JjTimestamp},
        object_id::ObjectId as _,
        op_store::{
            OpStore, Operation, OperationMetadata, RootOperationData, TimestampRange, View,
        },
        simple_op_store::SimpleOpStore,
    };
    futures::executor::block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let source = SimpleOpStore::init(
            directory.path(),
            RootOperationData {
                root_commit_id: jj_lib::backend::CommitId::from_bytes(&[0; 32]),
            },
        )
        .unwrap();
        let commit = jj_lib::backend::CommitId::from_bytes(&head.key.id.0);
        let mut view = View::make_root(commit.clone());
        view.wc_commit_ids.insert("task".into(), commit.clone());
        for index in 0..bookmarks {
            view.local_bookmarks.insert(
                format!("task-{index}").into(),
                jj_lib::op_store::RefTarget::normal(commit.clone()),
            );
        }
        let view_id = source.write_view(&view).await.unwrap();
        let timestamp = JjTimestamp {
            timestamp: MillisSinceEpoch(0),
            tz_offset: 0,
        };
        let op = Operation {
            view_id: view_id.clone(),
            parents: vec![jj_lib::op_store::OperationId::from_bytes(&[0; 64]); parent_edges],
            metadata: OperationMetadata {
                time: TimestampRange {
                    start: timestamp.clone(),
                    end: timestamp,
                },
                description: "capture task".into(),
                hostname: "agent".into(),
                username: "agent".into(),
                is_snapshot: true,
                workspace_name: Some("task".into()),
                attributes,
            },
            commit_predecessors: None,
        };
        let operation_id = source.write_operation(&op).await.unwrap();
        let objects = vec![
            tinytree_cloud::export_view(&source, &view_id, tinytree_cloud::Limits::default())
                .await
                .unwrap(),
            tinytree_cloud::export_operation(
                &source,
                &operation_id,
                tinytree_cloud::Limits::default(),
            )
            .await
            .unwrap(),
        ];
        let mut members = Vec::new();
        let mut receipts = Vec::new();
        for object in objects {
            let receipt = store
                .put_cloud_object(
                    "demo",
                    owner,
                    object.id(),
                    object.digest().unwrap(),
                    object.payload().unwrap(),
                    0,
                    tinytree_cloud::Limits::default(),
                )
                .unwrap();
            members.push(tinytree_cloud::Member {
                id: object.id(),
                selected: receipt.descriptor.selected,
            });
            receipts.push(receipt);
        }
        members.sort_by_key(|member| member.id);
        let operation = operation_id.as_bytes().try_into().unwrap();
        let mut native_roots = vec![head.key, root.key];
        native_roots.sort_by_key(|key| (key.kind as u8, key.id.0));
        (
            tinytree_cloud::Manifest {
                workspace: [41; 32],
                exporter: [42; 16],
                sequence: 1,
                operation_heads: vec![operation],
                undo_operations: vec![],
                intent_operations: vec![],
                working_copy: tinytree_cloud::WorkingCopy {
                    workspace_name: "task".into(),
                    wc_operation: operation,
                    snapshot_operation: operation,
                    snapshot_commit: head.key.id.0,
                    tree: TreeMerge {
                        terms: vec![TreeId(root.key.id.0)],
                        labels: vec![String::new()],
                    },
                },
                parents: vec![],
                members,
                intent_roots: vec![],
                native_roots,
                coverage: tinytree_cloud::Coverage::FullRequired,
                detached: vec![],
            },
            receipts,
        )
    })
}
fn cloud_request(
    manifest: &tinytree_cloud::Manifest,
    generation: u64,
    token: &str,
    leases: &[CloudWriteReceipt],
) -> PublishCloudRequest {
    let bytes =
        tinytree_cloud::encode_manifest(manifest, tinytree_cloud::ManifestLimits::default())
            .unwrap();
    PublishCloudRequest {
        workspace: manifest.workspace,
        expected_generation: generation,
        idempotency_key: token.into(),
        manifest: tinytree_cloud::ManifestId::of(&bytes),
        manifest_bytes: bytes,
        staging_leases: leases
            .iter()
            .map(|receipt| receipt.staging_lease.clone())
            .collect(),
    }
}
#[test]
fn cloud_publication_is_atomic_replayable_retains_history_and_survives_gc_and_reopen() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [40; 32],
    };
    let (file, root, head) = fixture(&store, "cloud task");
    let (manifest, leases) = cloud_capture(&store, owner, head, root);
    let first = cloud_request(&manifest, 0, "capture-first", &leases);
    let limits = CloudPublicationLimits::default();
    let published = store
        .publish_cloud_version("demo", owner, &first, 1, &limits)
        .unwrap();
    assert_eq!(published.workspace.generation, 1);
    assert_eq!(
        store
            .publish_cloud_version("demo", owner, &first, 100, &limits)
            .unwrap(),
        published
    );
    assert_eq!(
        store
            .get_cloud_workspace(
                "demo",
                CloudOwner {
                    principal: [99; 32]
                },
                manifest.workspace
            )
            .unwrap()
            .generation,
        0
    );
    let (_, next_root, next_head) = fixture(&store, "next cloud task");
    let (mut next, next_leases) = cloud_capture(&store, owner, next_head, next_root);
    next.sequence = 2;
    next.parents = vec![first.manifest];
    let second = cloud_request(&next, 1, "capture-second", &next_leases);
    store
        .publish_cloud_version("demo", owner, &second, 2, &limits)
        .unwrap();
    assert_eq!(
        store
            .publish_cloud_version("demo", owner, &first, 100, &limits)
            .unwrap(),
        published
    );
    let mut conflicting = first.clone();
    conflicting.manifest_bytes.push(0);
    assert!(matches!(
        store.publish_cloud_version("demo", owner, &conflicting, 3, &limits),
        Err(Error::IdempotencyConflict)
    ));
    let mut stale = second.clone();
    stale.idempotency_key = "stale-capture".into();
    assert!(matches!(
        store.publish_cloud_version("demo", owner, &stale, 3, &limits),
        Err(Error::StaleCloudWorkspace(_))
    ));
    let guard = store
        .read_cloud_version("demo", owner, manifest.workspace, 1, limits.manifest)
        .unwrap();
    assert_eq!(guard.manifest_bytes(), first.manifest_bytes);
    store.gc("demo", 100).unwrap();
    assert!(store.descriptor("demo", file.key).unwrap().is_some());
    assert!(matches!(
        store.client_gc("demo", &client_collection(&store, 100)),
        Err(Error::Invalid(_))
    ));
    drop(guard);
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, manifest.workspace)
            .unwrap()
            .generation,
        2
    );
    assert_eq!(
        store
            .read_cloud_version("demo", owner, manifest.workspace, 1, limits.manifest)
            .unwrap()
            .manifest_bytes(),
        first.manifest_bytes
    );
    assert_eq!(
        store
            .cloud_publication("demo", owner, "capture-first")
            .unwrap(),
        Some(published)
    );
    assert!(store.descriptor("demo", file.key).unwrap().is_some());
}

#[test]
fn cloud_discovery_is_owner_scoped_bounded_and_versions_pin_the_initial_ceiling() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [40; 32],
    };
    let peer = CloudOwner {
        principal: [41; 32],
    };
    let (_, root, head) = fixture(&store, "discover cloud history");
    let (mut manifest, leases) = cloud_capture(&store, owner, head, root);
    let limits = CloudPublicationLimits::default();
    assert!(store.list_cloud_workspaces("demo", owner, 0, &[]).is_err());
    assert!(
        store
            .list_cloud_workspaces("demo", owner, 129, &[])
            .is_err()
    );
    assert!(
        store
            .list_cloud_workspaces("demo", owner, 2, &[])
            .unwrap()
            .workspaces
            .is_empty()
    );
    let absent = store
        .list_cloud_versions("demo", owner, [9; 32], 2, &[])
        .unwrap();
    assert_eq!(absent.workspace.generation, 0);
    assert_eq!(absent.through_generation, 0);
    assert!(absent.versions.is_empty() && absent.continuation.is_empty());
    manifest.workspace = [2; 32];
    let first = cloud_request(&manifest, 0, "discovery-first", &leases);
    store
        .publish_cloud_version("demo", owner, &first, 1, &limits)
        .unwrap();
    manifest.sequence = 2;
    manifest.parents = vec![first.manifest];
    let second = cloud_request(&manifest, 1, "discovery-second", &[]);
    store
        .publish_cloud_version("demo", owner, &second, 2, &limits)
        .unwrap();
    let versions = store
        .list_cloud_versions("demo", owner, [2; 32], 1, &[])
        .unwrap();
    assert_eq!(versions.versions[0].manifest, first.manifest);
    assert_eq!(versions.through_generation, 2);
    assert!(!versions.continuation.is_empty());
    for (who, id, size) in [(peer, [2; 32], 1), (owner, [3; 32], 1), (owner, [2; 32], 2)] {
        assert!(
            store
                .list_cloud_versions("demo", who, id, size, &versions.continuation)
                .is_err()
        );
    }
    manifest.sequence = 3;
    manifest.parents = vec![second.manifest];
    let third = cloud_request(&manifest, 2, "discovery-third", &[]);
    store
        .publish_cloud_version("demo", owner, &third, 3, &limits)
        .unwrap();
    let page = store
        .list_cloud_versions("demo", owner, [2; 32], 1, &versions.continuation)
        .unwrap();
    assert_eq!(page.workspace.generation, 3);
    assert_eq!(page.through_generation, 2);
    assert_eq!(page.versions.len(), 1);
    assert_eq!(page.versions[0].manifest, second.manifest);
    assert!(page.continuation.is_empty());
    // Other workspace insertion precedes a moving catalog cursor: fresh scan
    // finds it; continuation does not pretend to be a catalog snapshot.
    manifest.workspace = [4; 32];
    manifest.sequence = 1;
    manifest.parents.clear();
    let other = cloud_request(&manifest, 0, "discovery-other", &[]);
    store
        .publish_cloud_version("demo", owner, &other, 4, &limits)
        .unwrap();
    let catalog = store.list_cloud_workspaces("demo", owner, 1, &[]).unwrap();
    assert_eq!(catalog.workspaces[0].workspace, [2; 32]);
    assert!(!catalog.continuation.is_empty());
    assert!(
        store
            .list_cloud_workspaces("demo", peer, 1, &catalog.continuation)
            .is_err()
    );
    assert!(
        store
            .list_cloud_workspaces("demo", owner, 2, &catalog.continuation)
            .is_err()
    );
    assert!(
        store
            .list_cloud_versions("demo", owner, [2; 32], 1, &catalog.continuation)
            .is_err()
    );
    assert!(
        store
            .list_cloud_workspaces("demo", peer, 1, &[])
            .unwrap()
            .workspaces
            .is_empty()
    );
    assert!(
        store
            .list_cloud_workspaces("other", owner, 1, &catalog.continuation)
            .is_err()
    );
    manifest.workspace = [1; 32];
    let inserted = cloud_request(&manifest, 0, "discovery-before-cursor", &[]);
    store
        .publish_cloud_version("demo", owner, &inserted, 5, &limits)
        .unwrap();
    let mut malformed = catalog.continuation.clone();
    malformed.push(0);
    assert!(
        store
            .list_cloud_workspaces("demo", owner, 1, &malformed)
            .is_err()
    );
    store.gc("demo", 100).unwrap();
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    let tail = store
        .list_cloud_workspaces("demo", owner, 1, &catalog.continuation)
        .unwrap();
    assert_eq!(tail.workspaces[0].workspace, [4; 32]);
    assert!(tail.continuation.is_empty());
    assert_eq!(
        store
            .list_cloud_workspaces("demo", owner, 128, &[])
            .unwrap()
            .workspaces
            .iter()
            .map(|w| w.workspace)
            .collect::<Vec<_>>(),
        vec![[1; 32], [2; 32], [4; 32]]
    );

    assert_eq!(
        store
            .list_cloud_versions("demo", owner, [2; 32], 128, &[])
            .unwrap()
            .versions
            .len(),
        3
    );
}

#[test]
fn invalid_cloud_checkpoint_and_incomplete_coverage_leave_no_publication() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [43; 32],
    };
    let (_, root, head) = fixture(&store, "validate checkpoint");
    let (manifest, leases) = cloud_capture(&store, owner, head, root);
    let mut invalid = manifest.clone();
    let empty = tinytree_model::empty_tree().descriptor.key;
    invalid.working_copy.tree.terms = vec![TreeId(empty.id.0)];
    invalid.native_roots = vec![head.key, empty];
    invalid
        .native_roots
        .sort_by_key(|key| (key.kind as u8, key.id.0));
    let request = cloud_request(&invalid, 0, "wrong-checkpoint", &leases);
    assert!(matches!(
        store.publish_cloud_version(
            "demo",
            owner,
            &request,
            1,
            &CloudPublicationLimits::default()
        ),
        Err(Error::Invalid(
            "cloud working-copy checkpoint tree mismatch"
        ))
    ));
    let mut incomplete = manifest.clone();
    incomplete.members.pop();
    let request = cloud_request(&incomplete, 0, "incomplete-capture", &[]);
    assert!(matches!(
        store.publish_cloud_version(
            "demo",
            owner,
            &request,
            1,
            &CloudPublicationLimits::default()
        ),
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, manifest.workspace)
            .unwrap()
            .generation,
        0
    );
    assert_eq!(
        store
            .cloud_publication("demo", owner, "wrong-checkpoint")
            .unwrap(),
        None
    );
    // Failed validation must not consume staging or reserve a receipt slot.
    let valid = cloud_request(&manifest, 0, "wrong-checkpoint", &leases);
    store
        .publish_cloud_version("demo", owner, &valid, 1, &CloudPublicationLimits::default())
        .unwrap();
    drop(store);
    Store::open_existing(directory.path(), config()).unwrap();
}

#[test]
fn cloud_and_legacy_holds_share_one_deduplicated_principal_promise_quota() {
    let (_directory, store) = open();
    let owner = CloudOwner {
        principal: [44; 32],
    };
    let (file, root, head) = fixture(&store, "shared promised history");
    let (manifest, leases) = cloud_capture(&store, owner, head, root);
    let cloud_bytes = manifest
        .members
        .iter()
        .map(|member| member.selected.size_bytes)
        .sum::<u64>();
    let exact_bytes = cloud_bytes + file.size_bytes + root.size_bytes + head.size_bytes;
    // The repository union also includes another owner's history and public
    // roots. Neither its canonical bytes nor opaque recovery blobs belong to
    // this principal's byte-exact promise charge.
    let (_, unrelated_root, unrelated_head) = fixture(&store, "another owner's history");
    let unrelated_owner = CloudOwner {
        principal: [43; 32],
    };
    let (unrelated_manifest, unrelated_leases) =
        cloud_capture(&store, unrelated_owner, unrelated_head, unrelated_root);
    store
        .publish_cloud_version(
            "demo",
            unrelated_owner,
            &cloud_request(&unrelated_manifest, 0, "other-owner", &unrelated_leases),
            0,
            &CloudPublicationLimits::default(),
        )
        .unwrap();
    let public_head = put_object(
        &store,
        Object::Commit(commit(TreeId(root.key.id.0), "public-only history")),
        0,
    );
    store
        .publish("demo", &request("public-only", 0, Some(public_head)), 0)
        .unwrap();
    let retention = RetentionLimits {
        max_unique_bytes_per_principal: exact_bytes,
        ..RetentionLimits::default()
    };
    let hold = hold_owner(44, 1);
    store
        .publish_retention(
            "demo",
            hold,
            &hold_request(hold, head, "legacy-first", 0, true),
            &retention,
            0,
        )
        .unwrap();
    let mut limits = CloudPublicationLimits {
        retention: retention.clone(),
        ..CloudPublicationLimits::default()
    };
    let request = cloud_request(&manifest, 0, "cloud-promise", &leases);
    limits.retention.max_unique_bytes_per_principal = exact_bytes - 1;
    assert!(matches!(
        store.publish_cloud_version("demo", owner, &request, 1, &limits),
        Err(Error::Limit("principal promised closure byte quota"))
    ));
    limits.retention.max_unique_bytes_per_principal = exact_bytes;
    store
        .publish_cloud_version("demo", owner, &request, 1, &limits)
        .unwrap();
    // A second client holding the same canonical history adds no promise bytes.
    let peer = hold_owner(44, 2);
    store
        .publish_retention(
            "demo",
            peer,
            &hold_request(peer, head, "same-history", 0, true),
            &retention,
            2,
        )
        .unwrap();
    let other = put_object(
        &store,
        Object::Commit(commit(TreeId(root.key.id.0), "additional promise")),
        2,
    );
    assert!(matches!(
        store.publish_retention(
            "demo",
            peer,
            &hold_request(peer, other, "additional-history", 0, true),
            &retention,
            2
        ),
        Err(Error::Limit(_))
    ));
}

#[test]
fn prospective_cloud_validation_budget_rejects_before_ack_and_reopens_cleanly() {
    for (name, records, metadata, payload, reason) in [
        (
            "records",
            true,
            false,
            false,
            "cloud validation catalog budget exceeded: records",
        ),
        (
            "metadata",
            false,
            true,
            false,
            "cloud validation catalog budget exceeded: metadata",
        ),
        (
            "records_metadata",
            true,
            true,
            false,
            "cloud validation catalog budget exceeded: records_metadata",
        ),
        (
            "payload",
            false,
            false,
            true,
            "cloud validation catalog budget exceeded: payload",
        ),
        (
            "records_payload",
            true,
            false,
            true,
            "cloud validation catalog budget exceeded: records_payload",
        ),
        (
            "metadata_payload",
            false,
            true,
            true,
            "cloud validation catalog budget exceeded: metadata_payload",
        ),
        (
            "records_metadata_payload",
            true,
            true,
            true,
            "cloud validation catalog budget exceeded: records_metadata_payload",
        ),
    ] {
        let (directory, store) = open();
        let owner = CloudOwner {
            principal: [45; 32],
        };
        let (_, root, head) = fixture(&store, "bounded validation");
        let (manifest, leases) = cloud_capture(&store, owner, head, root);
        let raw = manifest
            .members
            .iter()
            .map(|member| member.selected.size_bytes)
            .sum::<u64>();
        drop(store);
        let mut cfg = config();
        // Unique catalog fits, but retaining one version would require validating
        // both the catalog and its selected DAG on every fail-closed reopen.
        if payload {
            cfg.cloud.max_validation_payload_bytes = raw * 2 - 1;
        }
        if records {
            cfg.max_retention_validation_records = 15;
        }
        if metadata {
            cfg.max_retention_validation_bytes = 8192;
        }
        let store = Store::open_existing(directory.path(), cfg.clone()).unwrap();
        let request = cloud_request(&manifest, 0, "budgeted-capture", &leases);
        assert!(matches!(
            store.publish_cloud_version(
                "demo",
                owner,
                &request,
                1,
                &CloudPublicationLimits::default()
            ),
            Err(Error::Limit(actual)) if actual == reason
        ));
        assert_eq!(
            store
                .get_cloud_workspace("demo", owner, manifest.workspace)
                .unwrap()
                .generation,
            0
        );
        assert_eq!(
            store
                .cloud_publication("demo", owner, "budgeted-capture")
                .unwrap(),
            None
        );
        drop(store);
        let store = Store::open_existing(directory.path(), cfg).unwrap();
        drop(store);
        let store = Store::open_existing(directory.path(), config()).unwrap();
        let receipt = store
            .publish_cloud_version(
                "demo",
                owner,
                &request,
                1,
                &CloudPublicationLimits::default(),
            )
            .unwrap();
        assert_eq!(receipt.workspace.generation, 1, "{name}");
        assert_eq!(
            store
                .cloud_publication("demo", owner, "budgeted-capture")
                .unwrap(),
            Some(receipt),
            "{name}"
        );
        drop(store);
        Store::open_existing(directory.path(), config()).unwrap();
    }
}

#[test]
fn concurrent_cloud_captures_use_generation_cas_without_partial_receipts() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [46; 32],
    };
    let (_, root, head) = fixture(&store, "concurrent capture");
    let (manifest, _) = cloud_capture(&store, owner, head, root);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let jobs = (0..2)
        .map(|index| {
            let store = store.clone();
            let barrier = barrier.clone();
            let mut manifest = manifest.clone();
            manifest.sequence += index;
            let request = cloud_request(&manifest, 0, &format!("concurrent-{index}"), &[]);
            std::thread::spawn(move || {
                barrier.wait();
                store.publish_cloud_version(
                    "demo",
                    owner,
                    &request,
                    1,
                    &CloudPublicationLimits::default(),
                )
            })
        })
        .collect::<Vec<_>>();
    let results = jobs
        .into_iter()
        .map(|job| job.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(Error::StaleCloudWorkspace(_))))
            .count(),
        1
    );
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, manifest.workspace)
            .unwrap()
            .generation,
        1
    );
    assert_eq!(
        (0..2)
            .filter(|index| store
                .cloud_publication("demo", owner, &format!("concurrent-{index}"))
                .unwrap()
                .is_some())
            .count(),
        1
    );
    drop(store);
    Store::open_existing(directory.path(), config()).unwrap();
}

#[test]
fn decoded_cloud_version_budget_bounds_retained_objects_before_publication() {
    let directory = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.cloud.max_version_decode_bytes = 65536;
    let store = Store::open(directory.path(), cfg.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [47; 32],
    };
    let (_, root, head) = fixture(&store, "bounded decoded objects");
    let (manifest, leases) = cloud_capture_with_refs(&store, owner, head, root, 1000);
    let request = cloud_request(&manifest, 0, "decoded-budget", &leases);
    assert!(matches!(
        store.publish_cloud_version(
            "demo",
            owner,
            &request,
            1,
            &CloudPublicationLimits::default()
        ),
        Err(Error::Limit("cloud version decoded object budget"))
    ));
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, manifest.workspace)
            .unwrap()
            .generation,
        0
    );
    assert_eq!(
        store
            .cloud_publication("demo", owner, "decoded-budget")
            .unwrap(),
        None
    );
    drop(store);
    Store::open_existing(directory.path(), cfg).unwrap();
}

#[test]
fn cloud_publication_verifies_native_file_bytes_before_promising_retention() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [48; 32],
    };
    let (file, root, head) = fixture(&store, "verify cloud native closure");
    let (manifest, leases) = cloud_capture(&store, owner, head, root);
    let path = store.inner.path("demo", file.key);
    let original = fs::read(&path).unwrap();
    fs::write(&path, vec![0; original.len()]).unwrap();
    let request = cloud_request(&manifest, 0, "verify-native-files", &leases);
    assert!(
        store
            .publish_cloud_version(
                "demo",
                owner,
                &request,
                1,
                &CloudPublicationLimits::default()
            )
            .is_err()
    );
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, manifest.workspace)
            .unwrap()
            .generation,
        0
    );
    assert_eq!(
        store
            .cloud_publication("demo", owner, "verify-native-files")
            .unwrap(),
        None
    );
    fs::write(path, original).unwrap();
    store
        .publish_cloud_version(
            "demo",
            owner,
            &request,
            1,
            &CloudPublicationLimits::default(),
        )
        .unwrap();
    drop(store);
    Store::open_existing(directory.path(), config()).unwrap();
}

#[test]
fn cloud_reopen_rejects_missing_retained_version_root_record() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [49; 32],
    };
    let (_, root, head) = fixture(&store, "exact retained root index");
    let (manifest, leases) = cloud_capture(&store, owner, head, root);
    let request = cloud_request(&manifest, 0, "root-index", &leases);
    store
        .publish_cloud_version(
            "demo",
            owner,
            &request,
            1,
            &CloudPublicationLimits::default(),
        )
        .unwrap();
    drop(store);
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let space = db
            .keyspace("cloud-native-roots-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let keys = db
            .read_tx()
            .iter(&space)
            .map(|entry| entry.into_inner().unwrap().0)
            .collect::<Vec<_>>();
        assert_eq!(keys.len(), 1);
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.remove(&space, keys[0].clone());
        tx.commit().unwrap();
    }
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("cloud version roots missing"))
    ));
}

#[test]
fn prospective_cloud_repeated_metadata_work_is_bounded_before_ack() {
    for (name, record_limit, expects_prior_versions, minimum_generations) in [
        ("first_version_exceeds_budget", 15, false, 0),
        ("retained_history_fills_catalog", 1024, true, 1),
        (
            "long_shared_history_uses_resident_not_cumulative_memory",
            4096,
            true,
            64,
        ),
    ] {
        let (directory, store) = open();
        let empty = store.cloud_catalog_budget_summary().unwrap();
        assert_eq!(empty.catalog_records, 0);
        assert_eq!(empty.catalog_metadata_bytes, 0);
        assert_eq!(empty.required_total_records, 1);
        assert_eq!(empty.required_total_metadata_bytes, 4096);
        assert_eq!(empty.required_total_payload_bytes, 0);
        let owner = CloudOwner {
            principal: [50; 32],
        };
        let (_, root, head) = fixture(&store, "bounded metadata validation");
        let (mut manifest, leases) = cloud_capture(&store, owner, head, root);
        drop(store);
        let mut cfg = config();
        cfg.max_retention_validation_records = record_limit;
        cfg.cloud.resident = Some(CloudResidentLimits {
            max_bytes: 2 * 1024 * 1024 * 1024,
            max_entries: 65_536,
        });
        let store = Store::open_existing(directory.path(), cfg.clone()).unwrap();
        let mut generation = 0;
        let mut latest = None;
        let mut rejected = None;
        for attempt in 0..512 {
            manifest.sequence = attempt + 1;
            let key = format!("metadata-budget-{attempt}");
            let request = cloud_request(
                &manifest,
                generation,
                &key,
                if generation == 0 { &leases } else { &[] },
            );
            let before = store
                .get_cloud_workspace("demo", owner, manifest.workspace)
                .unwrap();
            let before_budget = store.cloud_catalog_budget_summary().unwrap();
            match store.publish_cloud_version(
                "demo",
                owner,
                &request,
                1,
                &CloudPublicationLimits::default(),
            ) {
                Ok(receipt) => {
                    generation += 1;
                    assert_eq!(receipt.workspace.generation, generation, "{name}");
                    let after_budget = store.cloud_catalog_budget_summary().unwrap();
                    assert_eq!(after_budget.versions, generation, "{name}");
                    assert_eq!(after_budget.publication_receipts, generation, "{name}");
                    assert!(
                        after_budget.required_total_records > before_budget.required_total_records,
                        "{name}"
                    );
                    assert!(
                        after_budget.retained_version_metadata_work
                            > before_budget.retained_version_metadata_work,
                        "{name}"
                    );
                    latest = Some((key, receipt));
                }
                Err(Error::Limit("cloud validation catalog budget exceeded: records")) => {
                    assert_eq!(
                        store.cloud_catalog_budget_summary().unwrap(),
                        before_budget,
                        "{name}: refused growth changed accounting"
                    );
                    assert_eq!(
                        store
                            .get_cloud_workspace("demo", owner, manifest.workspace)
                            .unwrap(),
                        before,
                        "{name}"
                    );
                    assert_eq!(
                        store.cloud_publication("demo", owner, &key).unwrap(),
                        None,
                        "{name}"
                    );
                    rejected = Some(key);
                    break;
                }
                other => panic!("{name}: unexpected publication result {other:?}"),
            }
        }
        assert!(
            rejected.is_some(),
            "{name}: bounded catalog must reject growth"
        );
        assert_eq!(generation > 0, expects_prior_versions, "{name}");
        assert!(
            generation >= minimum_generations,
            "retained history must reach its declared public boundary"
        );
        assert_eq!(store.descriptor("demo", head.key).unwrap(), Some(head));
        assert_eq!(store.descriptor("demo", root.key).unwrap(), Some(root));
        let final_budget = store.cloud_catalog_budget_summary().unwrap();
        assert!(
            final_budget.required_total_records <= record_limit as u64,
            "{name}"
        );
        assert_eq!(final_budget.versions, generation, "{name}");
        drop(store);
        let reopened = Store::open_existing(directory.path(), cfg).unwrap();
        assert_eq!(
            reopened.cloud_catalog_budget_summary().unwrap(),
            final_budget,
            "{name}: reopened accounting differs"
        );
        assert_eq!(
            reopened
                .get_cloud_workspace("demo", owner, manifest.workspace)
                .unwrap()
                .generation,
            generation,
            "{name}"
        );
        assert_eq!(
            reopened
                .cloud_publication("demo", owner, &rejected.unwrap())
                .unwrap(),
            None,
            "{name}"
        );
        if let Some((key, receipt)) = latest {
            assert_eq!(
                reopened.cloud_publication("demo", owner, &key).unwrap(),
                Some(receipt),
                "{name}"
            );
        }
        assert_eq!(reopened.descriptor("demo", head.key).unwrap(), Some(head));
        assert_eq!(reopened.descriptor("demo", root.key).unwrap(), Some(root));
    }
}

#[test]
fn resident_catalog_refusal_preserves_ack_state_and_strict_reopen_namespace() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [53; 32],
    };
    let (_, root, head) = fixture(&store, "resident catalog boundary");
    let (mut manifest, leases) = cloud_capture(&store, owner, head, root);
    drop(store);
    let mut policy = config();
    policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 2 * 1024 * 1024 * 1024,
        max_entries: 512,
    });
    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    let mut generation = 0;
    let mut refused = None;
    for sequence in 1..=32 {
        manifest.sequence = sequence;
        let request = cloud_request(
            &manifest,
            generation,
            &format!("resident-boundary-{sequence}"),
            if generation == 0 { &leases } else { &[] },
        );
        let before = store.cloud_catalog_budget_summary().unwrap();
        match store.publish_cloud_version("demo", owner, &request, 1, &Default::default()) {
            Ok(receipt) => {
                generation += 1;
                assert_eq!(receipt.workspace.generation, generation);
            }
            Err(Error::Limit("cloud resident memory")) => {
                assert_eq!(store.cloud_catalog_budget_summary().unwrap(), before);
                assert_eq!(
                    store
                        .get_cloud_workspace("demo", owner, manifest.workspace)
                        .unwrap()
                        .generation,
                    generation
                );
                assert!(
                    store
                        .cloud_publication("demo", owner, &request.idempotency_key)
                        .unwrap()
                        .is_none()
                );
                refused = Some(request);
                break;
            }
            _ => panic!("unexpected resident publication outcome"),
        }
    }
    assert!(
        generation > 0,
        "resident policy must allow ordinary publication before exhaustion"
    );
    let refused = refused.expect("resident capacity must refuse bounded growth");
    drop(store);
    // A valid engine may contain preallocated WAL padding. Resource refusal
    // must neither trim that journal nor create/migrate a keyspace.
    let mut journals: Vec<_> = fs::read_dir(directory.path().join("metadata"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "jnl"))
        .collect();
    journals.sort();
    assert!(!journals.is_empty());
    let journal = OpenOptions::new().append(true).open(&journals[0]).unwrap();
    journal
        .set_len(journal.metadata().unwrap().len() + 4096)
        .unwrap();
    journal.sync_all().unwrap();
    drop(journal);
    let before = directory_snapshot(directory.path());
    let times: Vec<_> = before
        .iter()
        .map(|(path, _, _, _)| {
            fs::symlink_metadata(directory.path().join(path))
                .unwrap()
                .modified()
                .unwrap()
        })
        .collect();
    let mut insufficient = policy.clone();
    insufficient.cloud.resident.as_mut().unwrap().max_bytes = 65536;
    assert!(matches!(
        Store::open_existing(directory.path(), insufficient),
        Err(Error::Limit("cloud resident memory"))
    ));
    assert_eq!(directory_snapshot(directory.path()), before);
    let after_times: Vec<_> = before
        .iter()
        .map(|(path, _, _, _)| {
            fs::symlink_metadata(directory.path().join(path))
                .unwrap()
                .modified()
                .unwrap()
        })
        .collect();
    assert_eq!(after_times, times);
    policy.cloud.resident.as_mut().unwrap().max_entries = 65_536;
    let store = Store::open_existing(directory.path(), policy).unwrap();
    let receipt = store
        .publish_cloud_version("demo", owner, &refused, 1, &Default::default())
        .unwrap();
    assert_eq!(receipt.workspace.generation, generation + 1);
    drop(store);
    // Legacy admission remains available without the opt-in policy.
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store
            .cloud_publication("demo", owner, &refused.idempotency_key)
            .unwrap(),
        Some(receipt)
    );
}

#[test]
fn prospective_resident_manifest_growth_refuses_before_ack_and_retries_exactly() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = config();
    policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 4 * 1024 * 1024,
        max_entries: 65_536,
    });
    let store = Store::open(directory.path(), policy.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [55; 32],
    };
    let (_, root, _) = fixture(&store, "prospective resident manifest");
    // A real canonical conflicted checkpoint can have substantial labels while
    // the selected cloud catalog remains small. Its native payload is admitted
    // by the independent native codec, not by the future manifest's arena.
    let mut snapshot = commit(TreeId(root.key.id.0), "resident conflict checkpoint");
    snapshot.root_tree = TreeMerge {
        terms: vec![TreeId(root.key.id.0); 3],
        labels: vec!["left".into(), "base".repeat(15 * 1024), "right".into()],
    };
    let checkpoint = snapshot.root_tree.clone();
    let head = put_object(&store, Object::Commit(snapshot), 0);
    let (mut manifest, leases) = cloud_capture(&store, owner, head, root);
    manifest.working_copy.tree = checkpoint;
    let request = cloud_request(&manifest, 0, "resident-incoming-manifest", &leases);
    let before = store.cloud_catalog_budget_summary().unwrap();
    assert!(matches!(
        store.publish_cloud_version("demo", owner, &request, 1, &Default::default()),
        Err(Error::Limit("cloud resident memory"))
    ));
    assert_eq!(store.cloud_catalog_budget_summary().unwrap(), before);
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, manifest.workspace)
            .unwrap()
            .generation,
        0
    );
    assert!(
        store
            .cloud_publication("demo", owner, &request.idempotency_key)
            .unwrap()
            .is_none()
    );
    drop(store);
    policy.cloud.resident.as_mut().unwrap().max_bytes = 64 * 1024 * 1024;
    let store = Store::open_existing(directory.path(), policy).unwrap();
    let receipt = store
        .publish_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
    assert_eq!(receipt.workspace.generation, 1);
    assert_eq!(
        store
            .read_cloud_version("demo", owner, manifest.workspace, 1, Default::default())
            .unwrap()
            .manifest_bytes(),
        request.manifest_bytes
    );
}

#[test]
fn retained_version_arenas_follow_leases_and_refuse_gc_without_deleting() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = config();
    policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 2 * 1024 * 1024 * 1024,
        max_entries: 512,
    });
    let store = Store::open(directory.path(), policy.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [54; 32],
    };
    let (file, root, head) = fixture(&store, "leased resident arena");
    let (manifest, leases) = cloud_capture(&store, owner, head, root);
    let mut missing = manifest.clone();
    // Classify missing metadata before admission even when precharging all
    // requested root slots would exceed the permanent pin budget.
    for byte in 100..228 {
        missing.native_roots.push(ObjectKey {
            kind: Kind::Commit,
            id: ObjectId([byte; 32]),
        });
    }
    missing
        .native_roots
        .sort_by_key(|key| (key.kind as u8, key.id.0));
    for _ in 0..3 {
        let invalid = cloud_request(&missing, 0, "resident-missing-native", &leases);
        assert!(matches!(
            store.publish_cloud_version("demo", owner, &invalid, 1, &Default::default()),
            Err(Error::MissingObject(_))
        ));
    }
    let request = cloud_request(&manifest, 0, "resident-lease", &leases);
    store
        .publish_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
    drop(store);
    // Size only the fixture from its closed catalog. Public reads below must
    // fit one complete arena plus pin growth, but never two operation arenas.
    let arena = cloud::resident::preflight(directory.path(), &policy)
        .unwrap()
        .charge()
        .unwrap();
    assert!(arena.bytes > 65536);
    // Calibrate one prospective arena from this closed fixture, not a second
    // simultaneous validation arena. A same-shaped next version grows by the
    // same catalog charge; no production policy is changed by this fixture.
    let calibration = Store::open_existing(directory.path(), config()).unwrap();
    let mut next_manifest = manifest.clone();
    next_manifest.sequence = 2;
    let calibration_request = cloud_request(&next_manifest, 1, "resident-calibration", &[]);
    calibration
        .publish_cloud_version("demo", owner, &calibration_request, 1, &Default::default())
        .unwrap();
    drop(calibration);
    let current_arena = cloud::resident::preflight(directory.path(), &policy)
        .unwrap()
        .charge()
        .unwrap();
    let mut single_reader_policy = policy.clone();
    let max_bytes = current_arena.bytes + (current_arena.bytes - arena.bytes) + 65536;
    assert!(
        max_bytes < 2 * current_arena.bytes,
        "fixture cannot admit a second arena"
    );
    single_reader_policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes,
        max_entries: 65_536,
    });
    let store = Store::open_existing(directory.path(), single_reader_policy.clone()).unwrap();
    next_manifest.sequence = 3;
    let bounded_request = cloud_request(&next_manifest, 2, "resident-bounded-publish", &[]);
    let published = store
        .publish_cloud_version("demo", owner, &bounded_request, 1, &Default::default())
        .unwrap();
    assert_eq!(published.workspace.generation, 3);
    assert_eq!(
        store
            .publish_cloud_version("demo", owner, &bounded_request, 1, &Default::default())
            .unwrap(),
        published
    );
    drop(store);
    let store = Store::open_existing(directory.path(), single_reader_policy).unwrap();
    assert_eq!(
        store
            .cloud_publication("demo", owner, &bounded_request.idempotency_key)
            .unwrap(),
        Some(published)
    );
    let first = store
        .read_cloud_version("demo", owner, manifest.workspace, 1, Default::default())
        .unwrap();
    assert_eq!(first.manifest_bytes(), request.manifest_bytes);
    // Native operations need only bounded reader/pin admission, not another
    // complete cloud catalog arena while this immutable version is retained.
    assert!(store.references("demo", &[]).unwrap().is_empty());
    assert_eq!(store.descriptor("demo", head.key).unwrap(), Some(head));
    let pin = store.pin_root("demo", head.key).unwrap();
    assert_eq!(pin.descriptor(), head);
    let (actual, metadata) = store.read_metadata("demo", head.key).unwrap();
    assert_eq!(actual, head);
    tinytree_model::verify(&head, &metadata, Limits::default()).unwrap();
    drop(metadata);
    let mut reader = store.read("demo", file.key).unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"native source\n");
    drop(reader);
    drop(pin);
    let mut native_readers = Vec::new();
    for _ in 0..32 {
        match store.read("demo", file.key) {
            Ok(reader) => native_readers.push(reader),
            Err(Error::Limit("cloud resident memory")) => break,
            _ => panic!("unexpected native reader outcome"),
        }
    }
    assert!(!native_readers.is_empty() && native_readers.len() < 32);
    drop(native_readers);
    drop(store.read("demo", file.key).unwrap());
    assert!(matches!(
        store.read_cloud_version("demo", owner, manifest.workspace, 1, Default::default()),
        Err(Error::Limit("cloud resident memory"))
    ));
    drop(first);
    let retry = store
        .read_cloud_version("demo", owner, manifest.workspace, 1, Default::default())
        .unwrap();
    assert_eq!(retry.manifest_bytes(), request.manifest_bytes);
    drop(retry);
    drop(store);
    let store = Store::open_existing(directory.path(), policy).unwrap();
    let mut readers = Vec::new();
    for _ in 0..8 {
        match store.read_cloud_version("demo", owner, manifest.workspace, 1, Default::default()) {
            Ok(reader) => readers.push(reader),
            Err(Error::Limit("cloud resident memory")) => break,
            _ => panic!("unexpected resident reader outcome"),
        }
    }
    assert!(!readers.is_empty());
    assert!(
        readers.len() < 8,
        "returned version owners must retain their arena charges"
    );
    // Release one admitted reader so the detached physical read can take its
    // place. It must retain that arena until completion, not charge it twice.
    drop(readers.pop().unwrap());
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let release = Arc::new(std::sync::Barrier::new(2));
    *store.inner.cloud_version_read_pause.lock().unwrap() = Some((entered_tx, release.clone()));
    let physical = store.clone();
    let workspace = manifest.workspace;
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let caller = std::thread::spawn(move || {
        let result = physical.read_cloud_version("demo", owner, workspace, 1, Default::default());
        let completed = result.is_ok();
        drop(result);
        done_tx.send(completed).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    // Losing the caller's join handle does not cancel the physical read or its
    // native/cloud pins and catalog arena. No timeout-owner shortcut is used.
    drop(caller);
    let before = directory_snapshot(directory.path());
    assert!(matches!(
        store.gc("demo", 100),
        Err(Error::Limit("cloud resident memory"))
    ));
    assert_eq!(
        directory_snapshot(directory.path()),
        before,
        "refused GC must not commit or unlink anything"
    );
    for reader in &readers {
        assert_eq!(reader.manifest_bytes(), request.manifest_bytes);
    }
    *store.inner.cloud_version_read_pause.lock().unwrap() = None;
    release.wait();
    assert!(done_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    drop(readers);
    store.gc("demo", 100).unwrap();
    let reader = store
        .read_cloud_version("demo", owner, manifest.workspace, 1, Default::default())
        .unwrap();
    assert_eq!(reader.manifest_bytes(), request.manifest_bytes);
    drop(reader);
    assert_eq!(store.descriptor("demo", head.key).unwrap(), Some(head));
}

#[test]
fn runtime_pin_capacity_remains_charged_after_readers_are_removed() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = config();
    policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 128 * 1024,
        max_entries: 1024,
    });
    let store = Store::open(directory.path(), policy).unwrap();
    store.create_repository("demo").unwrap();
    let mut objects = Vec::new();
    for ordinal in 0..24 {
        let bytes = format!("bounded native reader {ordinal}");
        let descriptor = tinytree_model::describe_file(bytes.as_bytes());
        store.put("demo", descriptor, bytes.as_bytes(), 0).unwrap();
        objects.push(descriptor);
    }
    let mut readers = Vec::new();
    for object in &objects {
        match store.pin_root("demo", object.key) {
            Ok(reader) => readers.push(reader),
            Err(Error::Limit("cloud resident memory")) => break,
            _ => panic!("unexpected pin capacity outcome"),
        }
    }
    let highwater = readers.len();
    assert!(highwater > 1 && highwater < objects.len());
    drop(readers);
    let mut same_root_readers = Vec::new();
    for _ in 0..24 {
        match store.pin_root("demo", objects[0].key) {
            Ok(reader) => same_root_readers.push(reader),
            Err(Error::Limit("cloud resident memory")) => break,
            _ => panic!("unexpected repeated pin outcome"),
        }
    }
    assert_eq!(
        same_root_readers.len(),
        highwater,
        "empty tables must not refund their retained bucket capacity"
    );
    drop(same_root_readers);
    assert!(
        store.pin_root("demo", objects[0].key).is_ok(),
        "dropping owners releases lease memory without leaking the physical admission"
    );
}

#[test]
fn resident_recovery_limits_do_not_reuse_logical_validation_work_limits() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = config();
    policy.max_retention_validation_records = 64;
    policy.max_retention_validation_bytes = 1024 * 1024;
    policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 2 * 1024 * 1024 * 1024,
        max_entries: 65_536,
    });
    let store = Store::open(directory.path(), policy.clone()).unwrap();
    store.create_repository("demo").unwrap();
    for ordinal in 0..96 {
        let bytes = format!("recovery allocation boundary {ordinal}");
        store
            .put(
                "demo",
                tinytree_model::describe_file(bytes.as_bytes()),
                bytes.as_bytes(),
                0,
            )
            .unwrap();
    }
    // More native deletions than the logical cloud scan-record limit are one
    // ordinary transaction. That limit is neither a WAL item nor padding cap.
    assert_eq!(store.gc("demo", 100).unwrap().deleted_objects, 96);
    drop(store);
    let store = Store::open_existing(directory.path(), policy).unwrap();
    assert_eq!(
        store
            .cloud_catalog_budget_summary()
            .unwrap()
            .catalog_records,
        0
    );
    assert_eq!(store.gc("demo", 100).unwrap().deleted_objects, 0);
}

#[test]
fn strict_resident_accounting_layout_refuses_before_recovery_mutates_namespace() {
    struct Case {
        name: &'static str,
        rows: usize,
        repo_bytes: usize,
        suffix_bytes: usize,
        value_padding: usize,
        expected: &'static str,
    }
    for case in [
        Case {
            name: "orphan repository totals",
            rows: 128,
            repo_bytes: 0,
            suffix_bytes: 0,
            value_padding: 0,
            expected: "cloud resident accounting namespaces",
        },
        Case {
            name: "oversized repository key",
            rows: 1,
            repo_bytes: 256,
            suffix_bytes: 0,
            value_padding: 0,
            expected: "cloud resident accounting key",
        },
        Case {
            name: "malformed owner suffix",
            rows: 1,
            repo_bytes: 0,
            suffix_bytes: 31,
            value_padding: 0,
            expected: "cloud resident accounting key",
        },
        Case {
            name: "oversized scalar encoding",
            rows: 1,
            repo_bytes: 0,
            suffix_bytes: 0,
            value_padding: 4096,
            expected: "cloud resident accounting row",
        },
    ] {
        let (directory, store) = open();
        let (_, root, head) = fixture(&store, "strict accounting resource boundary");
        let _ = cloud_capture(
            &store,
            CloudOwner {
                principal: [56; 32],
            },
            head,
            root,
        );
        let accounting = store
            .inner
            .db
            .keyspace("cloud-accounting-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let mut tx = store
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        for ordinal in 0..case.rows {
            let repo = if case.repo_bytes == 0 {
                format!("orphan-{ordinal:04}")
            } else {
                "r".repeat(case.repo_bytes)
            };
            let mut key = (repo.len() as u16).to_be_bytes().to_vec();
            key.extend_from_slice(repo.as_bytes());
            key.resize(key.len() + case.suffix_bytes, 0);
            let mut value = br#"{"objects":0,"blob_bytes":0,"selected_bytes":0,"metadata_bytes":0,"records":0,"namespaces":0,"stages":0,"workspaces":0,"versions":0,"receipts":0}"#.to_vec();
            value.resize(value.len() + case.value_padding, b' ');
            tx.insert(&accounting, key, value);
        }
        tx.commit().unwrap();
        drop(accounting);
        drop(store);
        let mut journals: Vec<_> = fs::read_dir(directory.path().join("metadata"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "jnl"))
            .collect();
        journals.sort();
        let journal = OpenOptions::new().append(true).open(&journals[0]).unwrap();
        journal
            .set_len(journal.metadata().unwrap().len() + 4096)
            .unwrap();
        journal.sync_all().unwrap();
        drop(journal);
        let before = directory_snapshot(directory.path());
        let times: Vec<_> = before
            .iter()
            .map(|(path, _, _, _)| {
                fs::symlink_metadata(directory.path().join(path))
                    .unwrap()
                    .modified()
                    .unwrap()
            })
            .collect();
        let mut policy = config();
        policy.cloud.resident = Some(CloudResidentLimits {
            max_bytes: 2 * 1024 * 1024 * 1024,
            max_entries: 65_536,
        });
        assert!(
            matches!(Store::open_existing(directory.path(), policy), Err(Error::Corrupt(reason)) if reason == case.expected),
            "{}: strict accounting refusal category",
            case.name
        );
        assert_eq!(
            directory_snapshot(directory.path()),
            before,
            "{}: resource preflight must not mutate rejected storage",
            case.name
        );
        let after_times: Vec<_> = before
            .iter()
            .map(|(path, _, _, _)| {
                fs::symlink_metadata(directory.path().join(path))
                    .unwrap()
                    .modified()
                    .unwrap()
            })
            .collect();
        assert_eq!(
            after_times, times,
            "{}: resource preflight changed modification times",
            case.name
        );
    }
}

#[test]
fn legacy_only_quota_does_not_introduce_public_reference_union_validation() {
    let directory = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.max_closure_objects = 3;
    let store = Store::open(directory.path(), cfg).unwrap();
    store.create_repository("demo").unwrap();
    let (_, _, public) = fixture(&store, "public history");
    let (file, root, held) = fixture(&store, "separate legacy history");
    store
        .publish("demo", &request("public", 0, Some(public)), 0)
        .unwrap();
    let owner = hold_owner(65, 1);
    let limits = RetentionLimits {
        max_unique_bytes_per_principal: file.size_bytes + root.size_bytes + held.size_bytes,
        ..RetentionLimits::default()
    };
    // Each history fits three objects; their union does not. With no cloud or
    // landing roots, the existing legacy-only policy must remain owner-local.
    store
        .publish_retention(
            "demo",
            owner,
            &hold_request(owner, held, "legacy-only", 0, true),
            &limits,
            1,
        )
        .unwrap();
}

#[test]
fn cloud_gc_extension_charges_descendant_roots_without_recharging_owner_roots() {
    for (edges, accepted) in [(8, false), (9, true)] {
        let directory = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.max_closure_edges = edges;
        let store = Store::open(directory.path(), cfg).unwrap();
        store.create_repository("demo").unwrap();
        let (_, root, parent) = fixture(&store, "retained ancestor");
        let previous_owner = CloudOwner {
            principal: [61; 32],
        };
        let (previous, previous_leases) = cloud_capture(&store, previous_owner, parent, root);
        store
            .publish_cloud_version(
                "demo",
                previous_owner,
                &cloud_request(&previous, 0, "ancestor", &previous_leases),
                0,
                &CloudPublicationLimits::default(),
            )
            .unwrap();
        let mut child = commit(TreeId(root.key.id.0), "owner descendant");
        child.parents = vec![CommitId(parent.key.id.0)];
        let child = put_object(&store, Object::Commit(child), 0);
        let owner = CloudOwner {
            principal: [62; 32],
        };
        let (manifest, leases) = cloud_capture(&store, owner, child, root);
        let publication = cloud_request(&manifest, 0, "descendant", &leases);
        // Owner graph/root edges cost six and its file context costs one.
        // The GC union adds the intrinsic empty TREE and the already-seen
        // ancestor as explicit roots: both must still pay their root edge.
        let result = store.publish_cloud_version(
            "demo",
            owner,
            &publication,
            1,
            &CloudPublicationLimits::default(),
        );
        if accepted {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(Error::Limit("closure edges"))));
            assert_eq!(
                store
                    .cloud_publication("demo", owner, "descendant")
                    .unwrap(),
                None
            );
        }
    }
}

#[test]
fn cloud_gc_extension_preserves_tree_context_and_path_byte_budgets() {
    struct Case {
        previous_is_child: bool,
        contexts: usize,
        bytes: usize,
        owner_roots: usize,
        expected: &'static str,
    }
    for case in [
        Case {
            previous_is_child: true,
            contexts: 3,
            bytes: 1024,
            owner_roots: 100,
            expected: "tree path contexts",
        },
        Case {
            previous_is_child: false,
            contexts: 100,
            bytes: 8,
            owner_roots: 100,
            expected: "tree path context bytes",
        },
        Case {
            previous_is_child: true,
            contexts: 3,
            bytes: 1024,
            owner_roots: 1,
            expected: "tree path contexts",
        },
        Case {
            previous_is_child: true,
            contexts: 4,
            bytes: 1024,
            owner_roots: 1,
            expected: "cloud promised root quota scan",
        },
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.max_path_contexts = case.contexts;
        cfg.max_path_context_bytes = case.bytes;
        let store = Store::open(directory.path(), cfg).unwrap();
        store.create_repository("demo").unwrap();
        let (file, _, _) = fixture(&store, "context base");
        let shared = put_object(&store, Object::Tree(tree(file, file.size_bytes)), 0);
        let nested = |name: &str| Tree {
            entries: vec![TreeEntry {
                name: name.into(),
                value: TreeValue::Tree(TreeId(shared.key.id.0)),
            }],
        };
        let previous_root = if case.previous_is_child {
            shared
        } else {
            put_object(&store, Object::Tree(nested("right")), 0)
        };
        let previous_head = put_object(
            &store,
            Object::Commit(commit(TreeId(previous_root.key.id.0), "previous contexts")),
            0,
        );
        let previous_owner = CloudOwner {
            principal: [63; 32],
        };
        let (previous, previous_leases) =
            cloud_capture(&store, previous_owner, previous_head, previous_root);
        store
            .publish_cloud_version(
                "demo",
                previous_owner,
                &cloud_request(&previous, 0, "previous-contexts", &previous_leases),
                0,
                &CloudPublicationLimits::default(),
            )
            .unwrap();
        let root = put_object(&store, Object::Tree(nested("left")), 0);
        let head = put_object(
            &store,
            Object::Commit(commit(TreeId(root.key.id.0), "new contexts")),
            0,
        );
        let owner = CloudOwner {
            principal: [64; 32],
        };
        let (manifest, leases) = cloud_capture(&store, owner, head, root);
        let mut limits = CloudPublicationLimits::default();
        limits.retention.max_unique_bytes_per_principal = 1;
        limits.retention.max_quota_scan_roots = case.owner_roots;
        // The owner's left subtree is valid by itself. Extending to an older
        // explicit TREE root must add its empty-prefix context even when its
        // graph object was visited below left. Different prefixes also share
        // one cumulative path-byte budget, not separate per-phase budgets.
        // Required GC validation also precedes known owner-byte excess and,
        // in the final two cases, the owner's root-count limit (two native
        // roots). With sufficient GC contexts, the root-count verdict wins.
        let result = store.publish_cloud_version(
            "demo",
            owner,
            &cloud_request(&manifest, 0, "new-contexts", &leases),
            1,
            &limits,
        );
        assert!(
            matches!(&result, Err(Error::Limit(reason)) if *reason == case.expected),
            "contexts={}, path bytes={}, owner roots={}: expected {}, got {result:?}",
            case.contexts,
            case.bytes,
            case.owner_roots,
            case.expected
        );
        assert_eq!(
            store
                .cloud_publication("demo", owner, "new-contexts")
                .unwrap(),
            None
        );
    }
}

#[test]
fn cloud_gc_root_budget_is_shared_across_owners_and_all_publication_routes() {
    for cloud_first in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.max_closure_objects = 4;
        let store = Store::open(directory.path(), cfg.clone()).unwrap();
        store.create_repository("demo").unwrap();
        let (_, root, legacy_head) = fixture(&store, "legacy owner history");
        let (_, _, cloud_head) = fixture(&store, "different cloud owner history");
        let legacy_owner = hold_owner(51, 1);
        let cloud_owner = CloudOwner {
            principal: [52; 32],
        };
        let (manifest, leases) = cloud_capture(&store, cloud_owner, cloud_head, root);
        let cloud = cloud_request(&manifest, 0, "bounded-cloud-union", &leases);
        let hold = hold_request(legacy_owner, legacy_head, "bounded-legacy-union", 0, true);
        if cloud_first {
            store
                .publish_cloud_version(
                    "demo",
                    cloud_owner,
                    &cloud,
                    1,
                    &CloudPublicationLimits::default(),
                )
                .unwrap();
            assert!(matches!(
                store.publish_retention(
                    "demo",
                    legacy_owner,
                    &hold,
                    &RetentionLimits::default(),
                    2
                ),
                Err(Error::Limit("object closure"))
            ));
            assert!(matches!(
                store.publish(
                    "demo",
                    &request("bounded-public-union", 0, Some(legacy_head)),
                    2
                ),
                Err(Error::Limit("object closure"))
            ));
            assert_eq!(
                store.publication("demo", "bounded-legacy-union").unwrap(),
                None
            );
            assert_eq!(
                store.publication("demo", "bounded-public-union").unwrap(),
                None
            );
            assert_eq!(
                store
                    .get_cloud_workspace("demo", cloud_owner, manifest.workspace)
                    .unwrap()
                    .generation,
                1
            );
        } else {
            store
                .publish_retention("demo", legacy_owner, &hold, &RetentionLimits::default(), 1)
                .unwrap();
            assert!(matches!(
                store.publish_cloud_version(
                    "demo",
                    cloud_owner,
                    &cloud,
                    2,
                    &CloudPublicationLimits::default()
                ),
                Err(Error::Limit("object closure"))
            ));
            assert_eq!(
                store
                    .cloud_publication("demo", cloud_owner, "bounded-cloud-union")
                    .unwrap(),
                None
            );
            assert_eq!(
                store
                    .get_cloud_workspace("demo", cloud_owner, manifest.workspace)
                    .unwrap()
                    .generation,
                0
            );
            // Removing the old root is allowed and immediately makes room for
            // the same rejected cloud request; its staging was not consumed.
            store
                .publish_retention(
                    "demo",
                    legacy_owner,
                    &hold_request(legacy_owner, legacy_head, "release-old-union", 1, false),
                    &RetentionLimits::default(),
                    3,
                )
                .unwrap();
            store
                .publish_cloud_version(
                    "demo",
                    cloud_owner,
                    &cloud,
                    3,
                    &CloudPublicationLimits::default(),
                )
                .unwrap();
        }
        store.gc("demo", 100).unwrap();
        drop(store);
        Store::open_existing(directory.path(), cfg).unwrap();
    }
}

#[test]
fn native_reference_replacement_checks_poststate_not_old_plus_new_cloud_union() {
    let directory = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.max_closure_objects = 5;
    let store = Store::open(directory.path(), cfg.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let (_, root, cloud_head) = fixture(&store, "cloud history");
    let (_, _, first) = fixture(&store, "first public history");
    let (_, _, replacement) = fixture(&store, "replacement public history");
    let owner = CloudOwner {
        principal: [53; 32],
    };
    let (manifest, leases) = cloud_capture(&store, owner, cloud_head, root);
    store
        .publish_cloud_version(
            "demo",
            owner,
            &cloud_request(&manifest, 0, "cloud-anchor", &leases),
            1,
            &CloudPublicationLimits::default(),
        )
        .unwrap();
    store
        .publish("demo", &request("first-public", 0, Some(first)), 1)
        .unwrap();
    store
        .publish("demo", &request("replace-public", 1, Some(replacement)), 2)
        .unwrap();
    store.gc("demo", 100).unwrap();
    assert_eq!(store.descriptor("demo", first.key).unwrap(), None);
    assert_eq!(
        store.descriptor("demo", replacement.key).unwrap(),
        Some(replacement)
    );
    drop(store);
    cfg.max_closure_objects = 4;
    let store = Store::open_existing(directory.path(), cfg).unwrap();
    // Lowering an operator work limit must not prevent a growth-free release.
    store
        .publish("demo", &request("release-public", 2, None), 101)
        .unwrap();
    store.gc("demo", 101).unwrap();
    assert_eq!(store.descriptor("demo", replacement.key).unwrap(), None);
}

fn cloud_index_artifacts(head: CommitId) -> Vec<tinytree_cloud::EncodedIndexArtifact> {
    use tinytree_cloud::{
        IndexArtifact, IndexArtifactId, IndexCollection, IndexNode, IndexNodePayload, IndexRoot,
    };
    let leaf = tinytree_cloud::encode_index_artifact(
        &IndexArtifact::Node(IndexNode {
            collection: IndexCollection::Anchors,
            depth: 0,
            prefix: [0; 32],
            records: 1,
            record_bytes: 32,
            payload: IndexNodePayload::Anchors(vec![head]),
        }),
        Default::default(),
    )
    .unwrap();
    let IndexArtifactId::Node(id) = leaf.descriptor.id else {
        panic!("node fixture");
    };
    let root = tinytree_cloud::encode_index_artifact(
        &IndexArtifact::Root(IndexRoot {
            anchors: id,
            drafts: None,
            anchor_count: 1,
            draft_count: 0,
        }),
        Default::default(),
    )
    .unwrap();
    vec![leaf, root]
}
#[test]
fn typed_index_artifacts_are_owner_scoped_readable_and_reopenable_without_op_ids() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [61; 32],
    };
    let other = CloudOwner {
        principal: [62; 32],
    };
    store.create_repository("other").unwrap();
    let artifacts = cloud_index_artifacts(CommitId([7; 32]));
    for artifact in &artifacts {
        let receipt = store
            .put_cloud_index_artifact(
                "demo",
                owner,
                artifact.descriptor,
                &artifact.bytes,
                0,
                Default::default(),
            )
            .unwrap();
        assert_eq!(receipt.descriptor, artifact.descriptor);
        assert_eq!(receipt.staging_lease.len(), 64);
        let retry = store
            .put_cloud_index_artifact(
                "demo",
                owner,
                artifact.descriptor,
                &artifact.bytes,
                1,
                Default::default(),
            )
            .unwrap();
        assert_eq!(receipt.staging_lease, retry.staging_lease);
        assert_eq!(
            store
                .lookup_cloud_index_artifacts("demo", other, &[artifact.descriptor.id])
                .unwrap(),
            vec![None]
        );
        assert_eq!(
            store
                .lookup_cloud_index_artifacts("other", owner, &[artifact.descriptor.id])
                .unwrap(),
            vec![None]
        );
        let mut bytes = Vec::new();
        store
            .read_cloud_index_artifact("demo", owner, artifact.descriptor.id, Default::default())
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert_eq!(bytes, artifact.bytes);
    }
    assert!(
        store
            .client_gc("demo", &client_collection(&store, 2))
            .is_err()
    );
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.meta, b"format")
            .unwrap()
            .unwrap()
            .as_ref(),
        b"tinytree-store-4"
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    for artifact in artifacts {
        assert_eq!(
            store
                .lookup_cloud_index_artifacts("demo", owner, &[artifact.descriptor.id])
                .unwrap(),
            vec![Some(artifact.descriptor)]
        );
    }
}
#[test]
fn actual_index_root_reader_keeps_present_artifact_and_native_descendants_through_gc() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [63; 32],
    };
    let (file, _, head) = fixture(&store, "index read lease");
    let artifacts = cloud_index_artifacts(CommitId(head.key.id.0));
    for artifact in &artifacts {
        store
            .put_cloud_index_artifact(
                "demo",
                owner,
                artifact.descriptor,
                &artifact.bytes,
                0,
                Default::default(),
            )
            .unwrap();
    }
    let reader = store
        .read_cloud_index_artifact(
            "demo",
            owner,
            artifacts[1].descriptor.id,
            Default::default(),
        )
        .unwrap();
    store.gc("demo", 100).unwrap();
    assert_eq!(store.descriptor("demo", file.key).unwrap(), Some(file));
    assert!(
        store
            .lookup_cloud_index_artifacts("demo", owner, &[artifacts[0].descriptor.id])
            .unwrap()[0]
            .is_some()
    );
    drop(reader);
    let report = store.gc("demo", 101).unwrap();
    assert_eq!(report.deleted_cloud_objects, 2);
    assert_eq!(
        store
            .lookup_cloud_index_artifacts("demo", owner, &[artifacts[1].descriptor.id])
            .unwrap(),
        vec![None]
    );
    assert_eq!(store.descriptor("demo", file.key).unwrap(), None);
    drop(store);
    Store::open_existing(directory.path(), config()).unwrap();
}
#[test]
fn index_artifact_upload_rejects_bad_typed_identity_and_raw_bytes_before_ack() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [64; 32],
    };
    let artifacts = cloud_index_artifacts(CommitId([8; 32]));
    let artifact = &artifacts[0];
    let mut bad = artifact.descriptor;
    bad.id = tinytree_cloud::IndexArtifactId::Root(tinytree_cloud::IndexRootId([9; 32]));
    assert!(matches!(
        store.put_cloud_index_artifact("demo", owner, bad, &artifact.bytes, 0, Default::default()),
        Err(Error::Invalid(_))
    ));
    let mut corrupt = artifact.bytes.clone();
    corrupt[0] ^= 1;
    assert!(matches!(
        store.put_cloud_index_artifact(
            "demo",
            owner,
            artifact.descriptor,
            &corrupt,
            0,
            Default::default()
        ),
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        store
            .lookup_cloud_index_artifacts("demo", owner, &[artifact.descriptor.id])
            .unwrap(),
        vec![None]
    );
    drop(store);
    Store::open_existing(directory.path(), config()).unwrap();
}
#[test]
fn index_and_op_uploads_share_owner_staging_and_repository_physical_quotas() {
    let directory = tempfile::tempdir().unwrap();
    let artifact = cloud_index_artifacts(CommitId([10; 32])).remove(0);
    let object = cloud_view(CommitId([11; 32]));
    let mut cfg = config();
    cfg.max_repository_bytes = tinytree_model::empty_tree().descriptor.size_bytes
        + artifact.descriptor.selected.size_bytes;
    let store = Store::open(directory.path(), cfg).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [65; 32],
    };
    store
        .put_cloud_index_artifact(
            "demo",
            owner,
            artifact.descriptor,
            &artifact.bytes,
            0,
            Default::default(),
        )
        .unwrap();
    assert!(matches!(
        store.put_cloud_object(
            "demo",
            owner,
            object.id(),
            object.digest().unwrap(),
            object.payload().unwrap(),
            0,
            Default::default()
        ),
        Err(Error::Limit(_))
    ));
    let extra = tinytree_model::describe_file(b"extra");
    assert!(matches!(
        store.put("demo", extra, b"extra", 0),
        Err(Error::Limit(_))
    ));
}
#[test]
fn index_feature4_survives_pack_and_missing_required_index_space_fails_closed() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [66; 32],
    };
    let (_, _, head) = fixture(&store, "packed index root");
    let artifact = cloud_index_artifacts(CommitId(head.key.id.0)).remove(0);
    store
        .put_cloud_index_artifact(
            "demo",
            owner,
            artifact.descriptor,
            &artifact.bytes,
            0,
            Default::default(),
        )
        .unwrap();
    store.pack("demo", PackOptions::default()).unwrap();
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.meta, b"format")
            .unwrap()
            .unwrap()
            .as_ref(),
        b"tinytree-store-4"
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    drop(store);
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&meta, b"format", b"tinytree-store-3");
        tx.commit().unwrap();
    }
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("cloud index feature marker missing"))
    ));
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&meta, b"format", b"tinytree-store-4");
        tx.commit().unwrap();
    }
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let space = db
            .keyspace("cloud-index-staging-v1", KeyspaceCreateOptions::default)
            .unwrap();
        db.inner().delete_keyspace(space.inner().clone()).unwrap();
    }
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("cloud index keyspace missing"))
    ));
}

fn cloud_payload_bytes(path: &std::path::Path) -> u64 {
    let root = path.join("cloud");
    if !root.exists() {
        return 0;
    }
    let mut directories = vec![root];
    let mut bytes = 0;
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let ty = entry.file_type().unwrap();
            if ty.is_dir() {
                directories.push(entry.path());
            } else if ty.is_file() {
                bytes += entry.metadata().unwrap().len();
            }
        }
    }
    bytes
}
#[test]
fn rejected_cloud_metadata_quota_installs_no_raw_payload_and_retry_can_succeed() {
    for index in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut cfg = config();
        cfg.cloud.max_metadata_bytes_per_owner = 1;
        let store = Store::open(directory.path(), cfg).unwrap();
        store.create_repository("demo").unwrap();
        let owner = CloudOwner {
            principal: [67; 32],
        };
        let object = cloud_view(CommitId([12; 32]));
        let artifact = cloud_index_artifacts(CommitId([13; 32])).remove(0);
        for _ in 0..3 {
            let result = if index {
                store
                    .put_cloud_index_artifact(
                        "demo",
                        owner,
                        artifact.descriptor,
                        &artifact.bytes,
                        0,
                        Default::default(),
                    )
                    .map(|_| ())
            } else {
                store
                    .put_cloud_object(
                        "demo",
                        owner,
                        object.id(),
                        object.digest().unwrap(),
                        object.payload().unwrap(),
                        0,
                        Default::default(),
                    )
                    .map(|_| ())
            };
            assert!(matches!(result, Err(Error::Limit("cloud owner quota"))));
            assert_eq!(
                cloud_payload_bytes(directory.path()),
                0,
                "a rejected metadata promise must not leak raw disk usage"
            );
        }
        drop(store);
        let store = Store::open_existing(directory.path(), config()).unwrap();
        let expected = if index {
            store
                .put_cloud_index_artifact(
                    "demo",
                    owner,
                    artifact.descriptor,
                    &artifact.bytes,
                    1,
                    Default::default(),
                )
                .unwrap();
            artifact.bytes.len() as u64
        } else {
            store
                .put_cloud_object(
                    "demo",
                    owner,
                    object.id(),
                    object.digest().unwrap(),
                    object.payload().unwrap(),
                    1,
                    Default::default(),
                )
                .unwrap();
            object.payload().unwrap().len() as u64
        };
        assert_eq!(cloud_payload_bytes(directory.path()), expected);
        drop(store);
        Store::open_existing(directory.path(), config()).unwrap();
    }
}
#[test]
fn preexisting_verified_index_raw_payload_is_reused_without_overwrite() {
    let directory = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.cloud.max_metadata_bytes_per_owner = 1;
    let store = Store::open(directory.path(), cfg).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [68; 32],
    };
    let artifact = cloud_index_artifacts(CommitId([14; 32])).remove(0);
    assert!(
        store
            .put_cloud_index_artifact(
                "demo",
                owner,
                artifact.descriptor,
                &artifact.bytes,
                0,
                Default::default()
            )
            .is_err()
    );
    drop(store);
    // Model a previous interrupted publication: complete canonical raw bytes
    // exist without a catalog row. Upload must verify and sync the collision,
    // never adopt a different representation or overwrite unknown bytes.
    let namespace = fs::read_dir(directory.path().join("cloud"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let path = namespace.join(hex(&artifact.descriptor.selected.blake3));
    fs::write(&path, &artifact.bytes).unwrap();
    let store = Store::open_existing(directory.path(), config()).unwrap();
    let receipt = store
        .put_cloud_index_artifact(
            "demo",
            owner,
            artifact.descriptor,
            &artifact.bytes,
            1,
            Default::default(),
        )
        .unwrap();
    let again = store
        .put_cloud_index_artifact(
            "demo",
            owner,
            artifact.descriptor,
            &artifact.bytes,
            2,
            Default::default(),
        )
        .unwrap();
    assert_eq!(receipt.descriptor, again.descriptor);
    assert_eq!(receipt.staging_lease, again.staging_lease);
    assert_eq!(fs::read(path).unwrap(), artifact.bytes);
    assert_eq!(
        cloud_payload_bytes(directory.path()),
        artifact.bytes.len() as u64
    );
    drop(store);
    Store::open_existing(directory.path(), config()).unwrap();
}

fn cloud_index_draft(
    store: &Store,
    descriptor: Descriptor,
    order: u64,
) -> tinytree_cloud::IndexDraft {
    let (_, bytes) = store.read_metadata("demo", descriptor.key).unwrap();
    let Object::Commit(commit) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else {
        panic!("commit fixture");
    };
    tinytree_cloud::IndexDraft {
        descriptor,
        change: commit.change_id,
        parents: commit.parents,
        order,
    }
}
fn cloud_v2_capture(
    store: &Store,
    owner: CloudOwner,
    head: Descriptor,
    root: Descriptor,
    logical: tinytree_cloud::PortableIndexSnapshot,
) -> (
    tinytree_cloud::ManifestV2,
    Vec<String>,
    Vec<tinytree_cloud::IndexArtifactDescriptor>,
) {
    cloud_v2_capture_from_recovery(
        store,
        owner,
        logical,
        cloud_capture(store, owner, head, root),
    )
}
fn cloud_v2_capture_from_recovery(
    store: &Store,
    owner: CloudOwner,
    logical: tinytree_cloud::PortableIndexSnapshot,
    recovery: (tinytree_cloud::Manifest, Vec<CloudWriteReceipt>),
) -> (
    tinytree_cloud::ManifestV2,
    Vec<String>,
    Vec<tinytree_cloud::IndexArtifactDescriptor>,
) {
    let (mut recovery, ordinary) = recovery;
    let intrinsic = tinytree_cloud::build_index_artifacts(
        &tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId::ROOT],
            drafts: vec![],
        },
        Default::default(),
    )
    .unwrap();
    let selected = tinytree_cloud::build_index_artifacts(&logical, Default::default()).unwrap();
    let operation = recovery.operation_heads[0];
    for id in &logical.roots {
        if *id != CommitId::ROOT {
            recovery.native_roots.push(ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(id.0),
            });
        }
    }
    recovery
        .native_roots
        .extend(logical.drafts.iter().map(|draft| draft.descriptor.key));
    recovery
        .native_roots
        .sort_by_key(|key| (key.kind as u8, key.id.0));
    recovery.native_roots.dedup();
    let mut roots = vec![intrinsic.root, selected.root];
    roots.sort_by_key(|root| root.id);
    roots.dedup_by_key(|root| root.id);
    let mut artifacts = std::collections::BTreeMap::new();
    for artifact in intrinsic.artifacts.into_iter().chain(selected.artifacts) {
        artifacts.entry(artifact.descriptor.id).or_insert(artifact);
    }
    let mut leases = ordinary
        .into_iter()
        .map(|receipt| receipt.staging_lease)
        .collect::<Vec<_>>();
    let mut descriptors = Vec::new();
    for artifact in artifacts.into_values() {
        let receipt = store
            .put_cloud_index_artifact(
                "demo",
                owner,
                artifact.descriptor,
                &artifact.bytes,
                0,
                Default::default(),
            )
            .unwrap();
        leases.push(receipt.staging_lease);
        descriptors.push(receipt.descriptor);
    }
    let value = tinytree_cloud::ManifestV2 {
        recovery,
        operation_indexes: vec![
            tinytree_cloud::OperationIndex {
                operation: [0; 64],
                root: intrinsic.root.id,
            },
            tinytree_cloud::OperationIndex {
                operation,
                root: selected.root.id,
            },
        ],
        index_roots: roots,
    };
    (value, leases, descriptors)
}
fn cloud_v2_add_snapshot(
    store: &Store,
    owner: CloudOwner,
    head: Descriptor,
    root: Descriptor,
    value: &mut tinytree_cloud::ManifestV2,
    leases: &mut Vec<String>,
    logical: tinytree_cloud::PortableIndexSnapshot,
    bookmarks: usize,
) {
    let (other, receipts) = cloud_capture_with_refs(store, owner, head, root, bookmarks);
    let operation = other.operation_heads[0];
    value.recovery.undo_operations.push(operation);
    value.recovery.undo_operations.sort();
    value.recovery.undo_operations.dedup();
    value.recovery.members.extend(other.members);
    value.recovery.members.sort_by_key(|member| member.id);
    value.recovery.members.dedup_by_key(|member| member.id);
    leases.extend(receipts.into_iter().map(|receipt| receipt.staging_lease));
    value.recovery.native_roots.extend(
        logical
            .roots
            .iter()
            .filter(|id| **id != CommitId::ROOT)
            .map(|id| ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(id.0),
            }),
    );
    value
        .recovery
        .native_roots
        .extend(logical.drafts.iter().map(|draft| draft.descriptor.key));
    value
        .recovery
        .native_roots
        .sort_by_key(|key| (key.kind as u8, key.id.0));
    value.recovery.native_roots.dedup();
    let selected = tinytree_cloud::build_index_artifacts(&logical, Default::default()).unwrap();
    for artifact in selected.artifacts {
        leases.push(
            store
                .put_cloud_index_artifact(
                    "demo",
                    owner,
                    artifact.descriptor,
                    &artifact.bytes,
                    0,
                    Default::default(),
                )
                .unwrap()
                .staging_lease,
        );
    }
    value.index_roots.push(selected.root);
    value.index_roots.sort_by_key(|root| root.id);
    value.index_roots.dedup_by_key(|root| root.id);
    value
        .operation_indexes
        .push(tinytree_cloud::OperationIndex {
            operation,
            root: selected.root.id,
        });
    value.operation_indexes.sort_by_key(|entry| entry.operation);
    leases.sort();
    leases.dedup();
}
fn cloud_v2_request(
    value: &tinytree_cloud::ManifestV2,
    generation: u64,
    token: &str,
    leases: Vec<String>,
) -> PublishCloudRequest {
    let bytes = tinytree_cloud::encode_manifest_v2(value, Default::default()).unwrap();
    PublishCloudRequest {
        workspace: value.recovery.workspace,
        expected_generation: generation,
        idempotency_key: token.into(),
        manifest: tinytree_cloud::ManifestId::of(&bytes),
        manifest_bytes: bytes,
        staging_leases: leases,
    }
}
#[test]
fn indexed_cloud_versions_preserve_hidden_drafts_artifacts_and_mixed_schema_history_after_gc_restart()
 {
    let directory = tempfile::tempdir().unwrap();
    let mut resident_policy = config();
    resident_policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 2 * 1024 * 1024 * 1024,
        max_entries: 65_536,
    });
    let store = Store::open(directory.path(), resident_policy.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [71; 32],
    };
    let (_, root, head) = fixture(&store, "visible index anchor");
    let ancestor = head;
    let mut visible = commit(TreeId(root.key.id.0), "visible descendant anchor");
    visible.parents = vec![CommitId(ancestor.key.id.0)];
    let head = put_object(&store, Object::Commit(visible), 0);
    let bytes = b"private hidden source";
    let hidden_file = tinytree_model::describe_file(bytes);
    store.put("demo", hidden_file, bytes, 0).unwrap();
    let copy_parent = put_object(
        &store,
        Object::CopyHistory(CopyHistory {
            current_path: "source.txt".into(),
            parents: vec![],
            salt: vec![1],
        }),
        0,
    );
    let copy = put_object(
        &store,
        Object::CopyHistory(CopyHistory {
            current_path: "source.txt".into(),
            parents: vec![CopyId(copy_parent.key.id.0)],
            salt: vec![2],
        }),
        0,
    );
    let symlink = put_object(&store, Object::Symlink("source.txt".into()), 0);
    let mut hidden_entries = tree(hidden_file, bytes.len() as u64);
    if let TreeValue::File { copy_id, .. } = &mut hidden_entries.entries[0].value {
        *copy_id = Some(CopyId(copy.key.id.0));
    }
    hidden_entries.entries.insert(
        0,
        TreeEntry {
            name: "link".into(),
            value: TreeValue::Symlink {
                id: tinytree_model::SymlinkId(symlink.key.id.0),
                target_bytes: 10,
            },
        },
    );
    let hidden_tree = put_object(&store, Object::Tree(hidden_entries), 0);
    let mut hidden = commit(TreeId(hidden_tree.key.id.0), "hidden draft");
    hidden.committer.timestamp.millis_since_epoch = -7000;
    hidden.root_tree = TreeMerge {
        terms: vec![
            TreeId(hidden_tree.key.id.0),
            TreeId(root.key.id.0),
            TreeId(hidden_tree.key.id.0),
        ],
        labels: vec!["left".into(), "base".into(), "right".into()],
    };
    hidden.parents = vec![CommitId(head.key.id.0)];
    let hidden = put_object(&store, Object::Commit(hidden), 0);
    let logical = tinytree_cloud::PortableIndexSnapshot {
        roots: vec![CommitId(head.key.id.0)],
        drafts: vec![cloud_index_draft(&store, hidden, 7)],
    };
    let (value, leases, artifacts) = cloud_v2_capture(&store, owner, head, root, logical);
    let first = cloud_v2_request(&value, 0, "indexed-first", leases);
    let publication = store
        .publish_cloud_version("demo", owner, &first, 1, &Default::default())
        .unwrap();
    assert_eq!(publication.workspace.generation, 1);
    let selector = CloudVersionSelector {
        workspace: value.recovery.workspace,
        generation: 1,
        manifest: first.manifest,
        selected: tinytree_cloud::BlobDigest::of(&first.manifest_bytes),
    };
    let selected_index = value
        .operation_indexes
        .iter()
        .find(|entry| entry.operation != [0; 64])
        .unwrap()
        .root;
    let intrinsic_index = value
        .operation_indexes
        .iter()
        .find(|entry| entry.operation == [0; 64])
        .unwrap()
        .root;
    let snapshot = store
        .open_cloud_snapshot("demo", owner, selector, &[selected_index], 4)
        .unwrap();
    assert_eq!(snapshot.anchors(), &[CommitId(head.key.id.0)]);
    assert!(
        !snapshot.anchors().contains(&CommitId(hidden.key.id.0)),
        "retained private drafts are not remote query roots"
    );
    let frozen = snapshot.frozen_commits();
    assert_eq!(
        frozen.len(),
        1,
        "hidden drafts belong to the frozen baseline"
    );
    assert_eq!(frozen[0].descriptor, hidden);
    assert_eq!(frozen[0].parents, vec![CommitId(head.key.id.0)]);
    assert_eq!(
        frozen[0].change,
        cloud_index_draft(&store, hidden, 7).change
    );
    assert_eq!(frozen[0].committer_millis, -7000);
    assert!(frozen[0].has_conflict);
    assert_eq!(
        frozen[0].order, 7,
        "preserve original ordinal, not commit-ID order"
    );
    assert_eq!(snapshot.selected_roots(), &[selected_index]);
    assert_eq!(snapshot.selector(), selector);
    assert!(snapshot.retained_size_upper_bound().unwrap() < 1024 * 1024);
    let intrinsic = store
        .open_cloud_snapshot("demo", owner, selector, &[intrinsic_index], 4)
        .unwrap();
    assert_eq!(
        intrinsic.anchors(),
        &[CommitId::ROOT],
        "intrinsic selection is not a missing-source fallback"
    );
    assert!(
        intrinsic.frozen_commits().is_empty(),
        "unselected drafts must not leak into intrinsic snapshot"
    );
    let mut both = vec![selected_index, intrinsic_index];
    both.sort();
    both.dedup();
    assert_eq!(
        store
            .open_cloud_snapshot("demo", owner, selector, &both, 4)
            .unwrap()
            .anchors(),
        &[CommitId::ROOT, CommitId(head.key.id.0)]
    );
    assert_eq!(
        store
            .open_cloud_snapshot("demo", owner, selector, &both, 4)
            .unwrap()
            .frozen_commits(),
        snapshot.frozen_commits()
    );
    assert!(matches!(
        store.open_cloud_snapshot("demo", owner, selector, &both, 1),
        Err(Error::Limit("cloud selected anchor count"))
    ));
    assert!(
        store
            .open_cloud_snapshot(
                "demo",
                owner,
                selector,
                &[selected_index, selected_index],
                4
            )
            .is_err()
    );
    assert!(
        store
            .open_cloud_snapshot(
                "demo",
                owner,
                selector,
                &[tinytree_cloud::IndexRootId([99; 32])],
                4
            )
            .is_err()
    );
    assert!(
        store
            .open_cloud_snapshot(
                "demo",
                CloudOwner {
                    principal: [99; 32]
                },
                selector,
                &[selected_index],
                4
            )
            .is_err()
    );
    drop(snapshot);
    drop(intrinsic);
    // Snapshot membership is the selected anchor ancestry PLUS its frozen
    // drafts, not every object retained elsewhere by this manifest.
    for terminal in [hidden, ancestor] {
        let mut lease = store
            .read_cloud_snapshot_native(
                "demo",
                owner,
                selector,
                terminal.key,
                &[selected_index],
                CommitId(terminal.key.id.0),
                &[],
                Default::default(),
            )
            .unwrap();
        let mut bytes = Vec::new();
        lease.read_to_end(&mut bytes).unwrap();
        assert_eq!(blake3::hash(&bytes).as_bytes(), &terminal.key.id.0);
    }
    assert!(matches!(
        store.read_cloud_snapshot_native(
            "demo",
            owner,
            selector,
            hidden.key,
            &[intrinsic_index],
            CommitId(hidden.key.id.0),
            &[],
            Default::default()
        ),
        Err(Error::Invalid(
            "native commit is outside selected cloud snapshots"
        ))
    ));
    assert!(
        store
            .read_cloud_snapshot_native(
                "demo",
                owner,
                selector,
                hidden.key,
                &[selected_index],
                CommitId::ROOT,
                &[],
                Default::default()
            )
            .is_err()
    );
    assert!(
        store
            .read_cloud_snapshot_native(
                "demo",
                owner,
                selector,
                hidden.key,
                &[selected_index, selected_index],
                CommitId(hidden.key.id.0),
                &[],
                Default::default()
            )
            .is_err()
    );
    let mut expired = CloudNativeReadLimits::default();
    expired.deadline = Some(std::time::Instant::now());
    assert!(matches!(
        store.read_cloud_snapshot_native(
            "demo",
            owner,
            selector,
            hidden.key,
            &[selected_index],
            CommitId(hidden.key.id.0),
            &[],
            expired
        ),
        Err(Error::Limit(_))
    ));
    let path = [hidden_tree.key, hidden_file.key];
    let mut snapshot_file = store
        .read_cloud_snapshot_native(
            "demo",
            owner,
            selector,
            hidden_file.key,
            &[selected_index],
            CommitId(hidden.key.id.0),
            &path,
            Default::default(),
        )
        .unwrap();
    let mut snapshot_bytes = Vec::new();
    snapshot_file.read_to_end(&mut snapshot_bytes).unwrap();
    assert_eq!(snapshot_bytes, bytes);
    drop(snapshot_file);
    assert!(
        store
            .read_cloud_snapshot_native(
                "demo",
                owner,
                selector,
                hidden_file.key,
                &[selected_index],
                CommitId(hidden.key.id.0),
                &[hidden_file.key],
                Default::default()
            )
            .is_err()
    );
    let mut read = store
        .read_cloud_native(
            "demo",
            owner,
            selector,
            hidden_file.key,
            hidden.key,
            &path,
            Default::default(),
        )
        .unwrap();
    assert_eq!(read.descriptor(), hidden_file);
    // Active read retains the complete exact version while GC runs.
    store.gc("demo", 100).unwrap();
    let mut restored = Vec::new();
    read.read_to_end(&mut restored).unwrap();
    assert_eq!(restored, bytes);
    drop(read);
    for (requested, path) in [
        (hidden.key, vec![]),
        (hidden_tree.key, vec![hidden_tree.key]),
        (symlink.key, vec![hidden_tree.key, symlink.key]),
        (
            copy_parent.key,
            vec![hidden_tree.key, copy.key, copy_parent.key],
        ),
    ] {
        let mut lease = store
            .read_cloud_native(
                "demo",
                owner,
                selector,
                requested,
                hidden.key,
                &path,
                Default::default(),
            )
            .unwrap();
        let mut actual = Vec::new();
        lease.read_to_end(&mut actual).unwrap();
        assert_eq!(blake3::hash(&actual).as_bytes(), &requested.id.0);
    }
    let mut final_bound = CloudNativeReadLimits::default();
    final_bound.max_metadata_bytes = hidden.size_bytes - 1;
    assert!(matches!(
        store.read_cloud_native(
            "demo",
            owner,
            selector,
            hidden.key,
            hidden.key,
            &[],
            final_bound
        ),
        Err(Error::Limit("cloud native proof metadata bytes"))
    ));
    assert!(
        store
            .read_cloud_native(
                "demo",
                owner,
                selector,
                hidden_file.key,
                head.key,
                &path,
                Default::default()
            )
            .is_err(),
        "arbitrary CAS reachability is not a canonical proof"
    );
    assert!(
        store
            .read_cloud_native(
                "demo",
                owner,
                selector,
                hidden_file.key,
                hidden.key,
                &[hidden_file.key],
                Default::default()
            )
            .is_err(),
        "proof cannot skip the tree"
    );
    assert!(
        store
            .read_cloud_native(
                "demo",
                CloudOwner {
                    principal: [99; 32]
                },
                selector,
                hidden_file.key,
                hidden.key,
                &path,
                Default::default()
            )
            .is_err()
    );
    let mut wrong = selector;
    wrong.manifest = tinytree_cloud::ManifestId([31; 32]);
    assert!(
        store
            .read_cloud_native(
                "demo",
                owner,
                wrong,
                hidden_file.key,
                hidden.key,
                &path,
                Default::default()
            )
            .is_err()
    );
    let mut small = CloudNativeReadLimits::default();
    small.max_metadata_bytes = hidden.size_bytes - 1;
    assert!(matches!(
        store.read_cloud_native(
            "demo",
            owner,
            selector,
            hidden_file.key,
            hidden.key,
            &path,
            small
        ),
        Err(Error::Limit("cloud native proof metadata bytes"))
    ));
    small = Default::default();
    small.max_edges = 1;
    assert!(matches!(
        store.read_cloud_native(
            "demo",
            owner,
            selector,
            hidden_file.key,
            hidden.key,
            &path,
            small
        ),
        Err(Error::Limit("cloud native proof edges"))
    ));
    small = Default::default();
    small.max_path_steps = 1;
    assert!(matches!(
        store.read_cloud_native(
            "demo",
            owner,
            selector,
            hidden_file.key,
            hidden.key,
            &path,
            small
        ),
        Err(Error::Limit("cloud native proof path"))
    ));

    assert!(
        store
            .read_cloud_version_v1(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .manifest_bytes(),
        first.manifest_bytes
    );
    let (_, next_root, next_head) = fixture(&store, "later visible history");
    let (mut next, leases) = cloud_capture(&store, owner, next_head, next_root);
    next.parents = vec![first.manifest];
    let second = cloud_request(&next, 1, "legacy-second", &leases);
    store
        .publish_cloud_version("demo", owner, &second, 2, &Default::default())
        .unwrap();
    assert!(
        store
            .read_cloud_version_v1(
                "demo",
                owner,
                value.recovery.workspace,
                2,
                Default::default()
            )
            .unwrap()
            .is_some()
    );
    store.gc("demo", 100).unwrap();
    assert_eq!(
        store.descriptor("demo", hidden_file.key).unwrap(),
        Some(hidden_file)
    );
    for artifact in &artifacts {
        assert_eq!(
            store
                .lookup_cloud_index_artifacts("demo", owner, &[artifact.id])
                .unwrap(),
            vec![Some(*artifact)]
        );
    }
    assert_eq!(
        store
            .publish_cloud_version("demo", owner, &first, 100, &Default::default())
            .unwrap(),
        publication
    );
    drop(store);
    let store = Store::open_existing(directory.path(), resident_policy.clone()).unwrap();
    assert_eq!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .manifest_bytes(),
        first.manifest_bytes
    );
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, value.recovery.workspace)
            .unwrap()
            .generation,
        2
    );
    assert_eq!(store.descriptor("demo", hidden.key).unwrap(), Some(hidden));
    assert_eq!(
        store
            .open_cloud_snapshot("demo", owner, selector, &[selected_index], 4)
            .unwrap()
            .anchors(),
        &[CommitId(head.key.id.0)]
    );

    let mut restored = Vec::new();
    store
        .read_cloud_native(
            "demo",
            owner,
            selector,
            hidden_file.key,
            hidden.key,
            &path,
            Default::default(),
        )
        .unwrap()
        .read_to_end(&mut restored)
        .unwrap();
    assert_eq!(
        restored, bytes,
        "restart and later workspace versions preserve exact old native authority"
    );
}

#[test]
fn indexed_cloud_publication_rejects_canonical_draft_mismatch_and_non_anchor_parents_atomically() {
    enum Failure {
        Change,
        ParentOrder,
        UnrelatedAnchor,
        PredecessorOnly,
        CachedCommitOutsideOtherSnapshot,
    }
    for failure in [
        Failure::Change,
        Failure::ParentOrder,
        Failure::UnrelatedAnchor,
        Failure::PredecessorOnly,
        Failure::CachedCommitOutsideOtherSnapshot,
    ] {
        let (directory, store) = open();
        let owner = CloudOwner {
            principal: [72; 32],
        };
        let (_, root, base) = fixture(&store, "index proof base");
        let side = put_object(
            &store,
            Object::Commit(commit(TreeId(root.key.id.0), "index side")),
            0,
        );
        let head = if matches!(failure, Failure::PredecessorOnly) {
            let mut c = commit(TreeId(root.key.id.0), "predecessor-only anchor");
            c.predecessors = vec![CommitId(side.key.id.0)];
            put_object(&store, Object::Commit(c), 0)
        } else {
            base
        };
        let mut hidden = commit(TreeId(root.key.id.0), "index draft proof");
        hidden.parents = if matches!(failure, Failure::ParentOrder) {
            vec![CommitId(head.key.id.0), CommitId(side.key.id.0)]
        } else if matches!(failure, Failure::PredecessorOnly) {
            vec![CommitId(side.key.id.0)]
        } else {
            vec![CommitId(head.key.id.0)]
        };
        let hidden = put_object(&store, Object::Commit(hidden), 0);
        let mut record = cloud_index_draft(&store, hidden, 9);
        if matches!(failure, Failure::Change) {
            record.change = ChangeId([19; 16]);
        }
        if matches!(failure, Failure::ParentOrder) {
            record.parents.reverse();
        }
        let roots = match failure {
            Failure::UnrelatedAnchor => vec![CommitId::ROOT],
            Failure::ParentOrder => {
                let mut ids = vec![CommitId(head.key.id.0), CommitId(side.key.id.0)];
                ids.sort();
                ids
            }
            _ => vec![CommitId(head.key.id.0)],
        };
        let (mut value, mut leases, _) = cloud_v2_capture(
            &store,
            owner,
            head,
            root,
            tinytree_cloud::PortableIndexSnapshot {
                roots,
                drafts: vec![record],
            },
        );
        if matches!(failure, Failure::CachedCommitOutsideOtherSnapshot) {
            let valid = value
                .operation_indexes
                .iter()
                .find(|entry| entry.operation != [0; 64])
                .unwrap()
                .root;
            // Make the valid snapshot run first, so its canonical COMMIT is
            // cached before a different snapshot tries to borrow its authority.
            let logical = (10..1000)
                .map(|order| tinytree_cloud::PortableIndexSnapshot {
                    roots: vec![CommitId::ROOT],
                    drafts: vec![cloud_index_draft(&store, hidden, order)],
                })
                .find(|logical| {
                    tinytree_cloud::build_index_artifacts(logical, Default::default())
                        .unwrap()
                        .root
                        .id
                        > valid
                })
                .unwrap();
            cloud_v2_add_snapshot(
                &store,
                owner,
                head,
                root,
                &mut value,
                &mut leases,
                logical,
                1,
            );
        }
        let request = cloud_v2_request(&value, 0, "invalid-index-proof", leases);
        assert!(matches!(
            store.publish_cloud_version("demo", owner, &request, 1, &Default::default()),
            Err(Error::Invalid(_))
        ));
        assert_eq!(
            store
                .get_cloud_workspace("demo", owner, value.recovery.workspace)
                .unwrap()
                .generation,
            0
        );
        assert_eq!(
            store
                .cloud_publication("demo", owner, "invalid-index-proof")
                .unwrap(),
            None
        );
        drop(store);
        Store::open_existing(directory.path(), config()).unwrap();
    }
}

#[test]
fn indexed_version_read_and_gc_fail_closed_on_missing_artifact_but_v1_gate_does_no_proof_work() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [73; 32],
    };
    let (_, root, head) = fixture(&store, "closed indexed proof");
    let (value, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![],
        },
    );
    let request = cloud_v2_request(&value, 0, "closed-indexed-proof", leases);
    store
        .publish_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
    let spare = tinytree_model::describe_file(b"must survive invalid mark");
    store
        .put("demo", spare, b"must survive invalid mark", 0)
        .unwrap();
    drop(store);
    let mut policy = config();
    policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 2 * 1024 * 1024 * 1024,
        max_entries: 65_536,
    });
    let arena = cloud::resident::preflight(directory.path(), &policy)
        .unwrap()
        .charge()
        .unwrap();
    policy.cloud.resident.as_mut().unwrap().max_bytes = arena.bytes + 65536;
    let store = Store::open_existing(directory.path(), policy).unwrap();
    let artifact = artifacts
        .iter()
        .find(|d| matches!(d.id, tinytree_cloud::IndexArtifactId::Node(_)))
        .unwrap();
    let namespace = fs::read_dir(directory.path().join("cloud"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let path = namespace.join(hex(&artifact.selected.blake3));
    let bytes = fs::read(&path).unwrap();
    fs::remove_file(&path).unwrap();
    assert!(
        store
            .read_cloud_version_v1(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .is_err()
    );
    assert!(store.gc("demo", 100).is_err());
    assert_eq!(store.descriptor("demo", spare.key).unwrap(), Some(spare));
    fs::write(path, bytes).unwrap();
    assert_eq!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .manifest_bytes(),
        request.manifest_bytes
    );
    drop(store);
    Store::open_existing(directory.path(), config()).unwrap();
}
#[test]
fn prospective_cloud_shared_index_ancestry_rebuilds_only_verified_work_across_owners() {
    let directory = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.max_retention_validation_records = 2048;
    let store = Store::open(directory.path(), cfg.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let (_, root, mut head) = fixture(&store, "shared proof base");
    for index in 0..48 {
        let mut node = commit(
            TreeId(root.key.id.0),
            &format!("shared proof ancestor {index}"),
        );
        node.parents = vec![CommitId(head.key.id.0)];
        head = put_object(&store, Object::Commit(node), 0);
    }
    let mut draft = commit(TreeId(root.key.id.0), "shared proof draft");
    draft.parents = vec![CommitId(head.key.id.0)];
    let draft = put_object(&store, Object::Commit(draft), 0);
    let mut published = Vec::new();
    for principal in [82, 83] {
        let owner = CloudOwner {
            principal: [principal; 32],
        };
        let snapshot = |order| tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![cloud_index_draft(&store, draft, order)],
        };
        let (mut value, mut leases, _) = cloud_v2_capture(&store, owner, head, root, snapshot(1));
        for order in 2..=12 {
            cloud_v2_add_snapshot(
                &store,
                owner,
                head,
                root,
                &mut value,
                &mut leases,
                snapshot(order),
                order as usize,
            );
        }
        let before = store.cloud_catalog_budget_summary().unwrap();
        let request = cloud_v2_request(&value, 0, "shared-index-proof", leases);
        let receipt = store
            .publish_cloud_version("demo", owner, &request, 1, &Default::default())
            .expect("shared canonical ancestry fits the finite catalog budget for both owners");
        let after = store.cloud_catalog_budget_summary().unwrap();
        assert!(
            after.retained_version_record_work - before.retained_version_record_work < 512,
            "physical proof work must not multiply ancestry by snapshot count"
        );
        assert_eq!(after.versions, published.len() as u64 + 1);
        published.push((owner, request, receipt));
    }
    let expected = store.cloud_catalog_budget_summary().unwrap();
    drop(store);
    // Boundary fixture: model prior derived overcounts, then invalidate a
    // receipt. Failed full validation must install NONE of the recalculation.
    let mut old_roots = Vec::new();
    let original_receipt;
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let roots = db
            .keyspace("cloud-native-roots-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let accounting = db
            .keyspace("cloud-accounting-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let receipts = db
            .keyspace("cloud-receipts-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let view = db.read_tx();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        for entry in view.iter(&roots) {
            let (key, bytes) = entry.into_inner().unwrap();
            let mut bytes = bytes.to_vec();
            let count = u32::from_le_bytes(bytes[40..44].try_into().unwrap()) as usize;
            let start = 44 + count * 33 + 2;
            for offset in [start, start + 8, start + 16] {
                let old = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                bytes[offset..offset + 8].copy_from_slice(&(old + 500).to_le_bytes());
            }
            tx.insert(&roots, key.clone(), bytes.clone());
            old_roots.push((key.to_vec(), bytes));
        }
        for entry in view.iter(&accounting) {
            let (key, bytes) = entry.into_inner().unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let added = if key.len() <= 6 { 1000 } else { 500 };
            for field in [
                "version_records",
                "version_metadata_bytes",
                "version_payload_bytes",
            ] {
                value[field] = serde_json::json!(value[field].as_u64().unwrap() + added);
            }
            tx.insert(&accounting, key, serde_json::to_vec(&value).unwrap());
        }
        let (key, bytes) = view.iter(&receipts).next().unwrap().into_inner().unwrap();
        original_receipt = (key.to_vec(), bytes.to_vec());
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["version"]["manifest"] = serde_json::to_value([91u8; 32]).unwrap();
        tx.insert(&receipts, key, serde_json::to_vec(&value).unwrap());
        tx.commit().unwrap();
    }
    assert!(matches!(
        Store::open_existing(directory.path(), cfg.clone()),
        Err(Error::Corrupt("cloud receipt replay history"))
    ));
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let roots = db
            .keyspace("cloud-native-roots-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let receipts = db
            .keyspace("cloud-receipts-v1", KeyspaceCreateOptions::default)
            .unwrap();
        for (key, bytes) in &old_roots {
            assert_eq!(
                db.read_tx().get(&roots, key).unwrap().unwrap().as_ref(),
                bytes
            );
        }
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&receipts, original_receipt.0, original_receipt.1);
        tx.commit().unwrap();
    }
    let store = Store::open_existing(directory.path(), cfg.clone()).unwrap();
    assert_eq!(
        store.cloud_catalog_budget_summary().unwrap(),
        expected,
        "verified reopen rederives root work and owner/repository/global totals together"
    );
    store.gc("demo", 100).unwrap();
    let after_gc = store.cloud_catalog_budget_summary().unwrap();
    assert_eq!(
        after_gc.retained_version_record_work,
        expected.retained_version_record_work
    );
    drop(store);
    let store = Store::open_existing(directory.path(), cfg).unwrap();
    assert_eq!(store.cloud_catalog_budget_summary().unwrap(), after_gc);
    for (owner, request, receipt) in published {
        assert_eq!(
            store
                .cloud_publication("demo", owner, "shared-index-proof")
                .unwrap(),
            Some(receipt)
        );
        assert_eq!(
            store
                .read_cloud_version("demo", owner, request.workspace, 1, Default::default())
                .unwrap()
                .manifest_bytes(),
            request.manifest_bytes
        );
    }
}

#[test]
fn indexed_version_prospective_payload_budget_includes_artifact_and_canonical_native_rereads() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [74; 32],
    };
    let (_, root, head) = fixture(&store, "proof budget anchor");
    let mut hidden = commit(TreeId(root.key.id.0), "proof budget draft");
    hidden.parents = vec![CommitId(head.key.id.0)];
    let hidden = put_object(&store, Object::Commit(hidden), 0);
    let (value, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![cloud_index_draft(&store, hidden, 1)],
        },
    );
    let selected = value
        .recovery
        .members
        .iter()
        .map(|member| member.selected.size_bytes)
        .sum::<u64>()
        + artifacts.iter().map(|d| d.selected.size_bytes).sum::<u64>();
    let request = cloud_v2_request(&value, 0, "indexed-proof-budget", leases);
    drop(store);
    let mut cfg = config();
    cfg.cloud.max_validation_payload_bytes = 2 * selected + head.size_bytes + hidden.size_bytes - 1;
    let store = Store::open_existing(directory.path(), cfg.clone()).unwrap();
    assert!(matches!(
        store.publish_cloud_version("demo", owner, &request, 1, &Default::default()),
        Err(Error::Limit(
            "cloud validation catalog budget exceeded: payload"
        ))
    ));
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, value.recovery.workspace)
            .unwrap()
            .generation,
        0
    );
    assert_eq!(
        store
            .cloud_publication("demo", owner, "indexed-proof-budget")
            .unwrap(),
        None
    );
    drop(store);
    let store = Store::open_existing(directory.path(), cfg).unwrap();
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    store
        .publish_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
}

#[test]
fn indexed_artifacts_share_exact_principal_promise_quota_with_legacy_holds() {
    let (_directory, store) = open();
    let owner = CloudOwner {
        principal: [75; 32],
    };
    let (file, root, head) = fixture(&store, "joint indexed quota");
    let mut hidden = commit(TreeId(root.key.id.0), "joint indexed hidden");
    hidden.parents = vec![CommitId(head.key.id.0)];
    let hidden = put_object(&store, Object::Commit(hidden), 0);
    let (value, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![cloud_index_draft(&store, hidden, 2)],
        },
    );
    let raw = value
        .recovery
        .members
        .iter()
        .map(|member| member.selected.size_bytes)
        .sum::<u64>()
        + artifacts.iter().map(|d| d.selected.size_bytes).sum::<u64>();
    let exact = file.size_bytes + root.size_bytes + head.size_bytes + hidden.size_bytes + raw;
    let retention = RetentionLimits {
        max_unique_bytes_per_principal: exact,
        ..Default::default()
    };
    let legacy = hold_owner(75, 1);
    store
        .publish_retention(
            "demo",
            legacy,
            &hold_request(legacy, head, "indexed-old-hold", 0, true),
            &retention,
            0,
        )
        .unwrap();
    let request = cloud_v2_request(&value, 0, "joint-indexed-quota", leases);
    let mut limits = CloudPublicationLimits {
        retention: retention.clone(),
        ..Default::default()
    };
    limits.retention.max_unique_bytes_per_principal = exact - 1;
    assert!(matches!(
        store.publish_cloud_version("demo", owner, &request, 1, &limits),
        Err(Error::Limit("principal promised closure byte quota"))
    ));
    limits.retention.max_unique_bytes_per_principal = exact;
    store
        .publish_cloud_version("demo", owner, &request, 1, &limits)
        .unwrap();
    let peer = hold_owner(75, 2);
    store
        .publish_retention(
            "demo",
            peer,
            &hold_request(peer, hidden, "same-indexed-history", 0, true),
            &retention,
            2,
        )
        .unwrap();
    let extra = put_object(
        &store,
        Object::Commit(commit(
            TreeId(root.key.id.0),
            "excess joint indexed history",
        )),
        2,
    );
    assert!(matches!(
        store.publish_retention(
            "demo",
            peer,
            &hold_request(peer, extra, "excess-indexed-history", 0, true),
            &retention,
            2
        ),
        Err(Error::Limit(_))
    ));
}
#[test]
fn unrelated_index_stage_rejects_atomic_publication_without_consuming_required_leases() {
    let (_directory, store) = open();
    let owner = CloudOwner {
        principal: [76; 32],
    };
    let (_, root, head) = fixture(&store, "typed stage selection");
    let (value, leases, _) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![],
        },
    );
    let extra = cloud_index_artifacts(CommitId([77; 32])).remove(0);
    let extra_receipt = store
        .put_cloud_index_artifact(
            "demo",
            owner,
            extra.descriptor,
            &extra.bytes,
            0,
            Default::default(),
        )
        .unwrap();
    let mut unrelated = leases.clone();
    unrelated.push(extra_receipt.staging_lease);
    let invalid = cloud_v2_request(&value, 0, "typed-stage-selection", unrelated);
    assert!(matches!(
        store.publish_cloud_version("demo", owner, &invalid, 1, &Default::default()),
        Err(Error::Invalid("unrelated cloud index staging lease"))
    ));
    assert_eq!(
        store
            .cloud_publication("demo", owner, "typed-stage-selection")
            .unwrap(),
        None
    );
    let valid = cloud_v2_request(&value, 0, "typed-stage-selection", leases);
    store
        .publish_cloud_version("demo", owner, &valid, 1, &Default::default())
        .unwrap();
    store.gc("demo", 100).unwrap();
    assert_eq!(
        store
            .lookup_cloud_index_artifacts("demo", owner, &[extra.descriptor.id])
            .unwrap(),
        vec![None]
    );
}

fn small_index_cloud_config() -> Config {
    let mut small = config();
    small.cloud.max_version_decode_bytes = 65536;
    small.cloud.max_object_bytes = 1024;
    small.cloud.max_decode_bytes = 32768;
    small.cloud.max_field_bytes = 1024;
    small.cloud.index_artifacts.max_artifacts = 4;
    small.cloud.index_artifacts.max_snapshots = 2;
    small.cloud.index_artifacts.max_artifact_bytes = 4096;
    small.cloud.index_artifacts.max_total_artifact_bytes = 65536;
    small.cloud.index_artifacts.max_decode_bytes = 262144;
    small.cloud.index_artifacts.max_graph_steps = 100000;
    small.cloud.index_artifacts.snapshot.max_decode_bytes = 8192;
    small.cloud.index_artifacts.snapshot.max_roots = 2;
    small.cloud.index_artifacts.snapshot.max_drafts = 1;
    small.cloud.index_artifacts.snapshot.max_parents = 2;
    small.cloud.index_artifacts.snapshot.max_edges = 4;
    small.cloud.index_artifacts.snapshot.max_encoded_bytes = 4096;
    small
}
#[test]
fn selected_cloud_snapshot_overlap_preserves_minimum_original_order() {
    for (name, other_order, expected_order) in [
        ("same original ordinal deduplicates", 7, 7),
        (
            "larger overlapping ordinal preserves original minimum",
            8,
            7,
        ),
        ("smaller overlapping ordinal becomes minimum", 6, 6),
    ] {
        let (_directory, store) = open();
        let owner = CloudOwner {
            principal: [81; 32],
        };
        let (_, root, head) = fixture(&store, "overlapping checkpoint anchor");
        let mut draft = commit(TreeId(root.key.id.0), "retained hidden draft");
        draft.parents = vec![CommitId(head.key.id.0)];
        let draft = put_object(&store, Object::Commit(draft), 0);
        let logical = tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![cloud_index_draft(&store, draft, 7)],
        };
        let (mut value, mut leases, _) = cloud_v2_capture(&store, owner, head, root, logical);
        let original = value
            .operation_indexes
            .iter()
            .find(|entry| entry.operation != [0; 64])
            .unwrap()
            .root;
        // A real second operation/view has a different reference set, while
        // sharing exactly the same native history and immutable draft bytes.
        let (other_operation, receipts) = cloud_capture_with_refs(&store, owner, head, root, 1);
        let operation = other_operation.operation_heads[0];
        assert!(
            !value
                .operation_indexes
                .iter()
                .any(|entry| entry.operation == operation)
        );
        value.recovery.undo_operations.push(operation);
        value.recovery.members.extend(other_operation.members);
        value.recovery.members.sort_by_key(|member| member.id);
        value.recovery.members.dedup_by_key(|member| member.id);
        leases.extend(receipts.into_iter().map(|receipt| receipt.staging_lease));
        let alternate = tinytree_cloud::build_index_artifacts(
            &tinytree_cloud::PortableIndexSnapshot {
                roots: vec![CommitId::ROOT, CommitId(head.key.id.0)],
                drafts: vec![cloud_index_draft(&store, draft, other_order)],
            },
            Default::default(),
        )
        .unwrap();
        for artifact in &alternate.artifacts {
            leases.push(
                store
                    .put_cloud_index_artifact(
                        "demo",
                        owner,
                        artifact.descriptor,
                        &artifact.bytes,
                        0,
                        Default::default(),
                    )
                    .unwrap()
                    .staging_lease,
            );
        }
        value.index_roots.push(alternate.root);
        value.index_roots.sort_by_key(|root| root.id);
        value
            .operation_indexes
            .push(tinytree_cloud::OperationIndex {
                operation,
                root: alternate.root.id,
            });
        value.operation_indexes.sort_by_key(|entry| entry.operation);
        leases.sort();
        leases.dedup();
        let request = cloud_v2_request(&value, 0, "overlapping-checkpoints", leases);
        store
            .publish_cloud_version("demo", owner, &request, 1, &Default::default())
            .unwrap();
        let selector = CloudVersionSelector {
            workspace: value.recovery.workspace,
            generation: 1,
            manifest: request.manifest,
            selected: tinytree_cloud::BlobDigest::of(&request.manifest_bytes),
        };
        assert_eq!(
            store
                .open_cloud_snapshot("demo", owner, selector, &[original], 4)
                .unwrap()
                .frozen_commits()[0]
                .order,
            7,
            "{name}"
        );
        assert_eq!(
            store
                .open_cloud_snapshot("demo", owner, selector, &[alternate.root.id], 4)
                .unwrap()
                .frozen_commits()[0]
                .order,
            other_order,
            "{name}"
        );
        let mut selected = vec![original, alternate.root.id];
        selected.sort();
        let result = store.open_cloud_snapshot("demo", owner, selector, &selected, 4);
        let guard = result.unwrap();
        assert_eq!(guard.frozen_commits().len(), 1, "{name}");
        assert_eq!(guard.frozen_commits()[0].descriptor, draft, "{name}");
        assert_eq!(guard.frozen_commits()[0].order, expected_order, "{name}");
    }
}

#[test]
fn indexed_native_ancestry_proof_admits_frontier_and_decode_within_remaining_version_memory() {
    let directory = tempfile::tempdir().unwrap();
    let small = small_index_cloud_config();
    let store = Store::open(directory.path(), small.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let tree = tinytree_model::empty_tree().descriptor;
    let mut parent = CommitId::ROOT;
    let mut head = None;
    for index in 0..1000 {
        let mut object = commit(TreeId(tree.key.id.0), &format!("bounded anchor {index}"));
        object.parents = vec![parent];
        let descriptor = put_object(&store, Object::Commit(object), 0);
        parent = CommitId(descriptor.key.id.0);
        head = Some(descriptor);
    }
    let head = head.unwrap();
    let owner = CloudOwner {
        principal: [78; 32],
    };
    let (value, leases, _) = cloud_v2_capture(
        &store,
        owner,
        head,
        tree,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![parent],
            drafts: vec![],
        },
    );
    let request = cloud_v2_request(&value, 0, "bounded-index-native-memory", leases);
    assert!(matches!(
        store.publish_cloud_version("demo", owner, &request, 1, &Default::default()),
        Err(Error::Limit("cloud index native proof memory"))
    ));
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, value.recovery.workspace)
            .unwrap()
            .generation,
        0
    );
    assert_eq!(
        store
            .cloud_publication("demo", owner, "bounded-index-native-memory")
            .unwrap(),
        None
    );
    drop(store);
    let store = Store::open_existing(directory.path(), small.clone()).unwrap();
    drop(store);
    let mut adequate = small;
    adequate.cloud.max_version_decode_bytes = 2 * 1024 * 1024;
    // The same valid indexed history also exceeds a separately selected finite
    // record budget. Reject before publication, then reuse the original request.
    let mut record_bounded = adequate.clone();
    record_bounded.max_retention_validation_records = 64;
    let bounded = Store::open_existing(directory.path(), record_bounded).unwrap();
    assert!(matches!(
        bounded.publish_cloud_version("demo", owner, &request, 1, &Default::default()),
        Err(Error::Limit("cloud index proof work exceeded: records"))
    ));
    assert_eq!(
        bounded
            .get_cloud_workspace("demo", owner, value.recovery.workspace)
            .unwrap()
            .generation,
        0
    );
    assert_eq!(
        bounded
            .cloud_publication("demo", owner, "bounded-index-native-memory")
            .unwrap(),
        None
    );
    drop(bounded);
    let store = Store::open_existing(directory.path(), adequate.clone()).unwrap();
    store
        .publish_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
    assert_eq!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .manifest_bytes(),
        request.manifest_bytes
    );
    drop(store);
    let store = Store::open_existing(directory.path(), adequate).unwrap();
    assert_eq!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .manifest_bytes(),
        request.manifest_bytes
    );
}

#[test]
fn root_only_index_cannot_bypass_working_copy_commit_decode_memory_admission() {
    let directory = tempfile::tempdir().unwrap();
    let small = small_index_cloud_config();
    let store = Store::open(directory.path(), small.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let tree = tinytree_model::empty_tree().descriptor;
    let head = put_object(
        &store,
        Object::Commit(commit(
            TreeId(tree.key.id.0),
            &"large working-copy metadata".repeat(400),
        )),
        0,
    );
    let owner = CloudOwner {
        principal: [79; 32],
    };
    // The index has no nonzero anchors or drafts, but OP/VIEW records retain a
    // real WC commit. Its canonical-byte check must own its own memory budget.
    let (value, leases, _) = cloud_v2_capture(
        &store,
        owner,
        head,
        tree,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId::ROOT],
            drafts: vec![],
        },
    );
    let request = cloud_v2_request(&value, 0, "bounded-index-wc-memory", leases);
    assert!(matches!(
        store.publish_cloud_version("demo", owner, &request, 1, &Default::default()),
        Err(Error::Limit("cloud index native proof memory"))
    ));
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, value.recovery.workspace)
            .unwrap()
            .generation,
        0
    );
    assert_eq!(
        store
            .cloud_publication("demo", owner, "bounded-index-wc-memory")
            .unwrap(),
        None
    );
    drop(store);
    let store = Store::open_existing(directory.path(), small.clone()).unwrap();
    drop(store);
    let mut adequate = small;
    adequate.cloud.max_version_decode_bytes = 512 * 1024;
    let store = Store::open_existing(directory.path(), adequate.clone()).unwrap();
    store
        .publish_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
    assert_eq!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .manifest_bytes(),
        request.manifest_bytes
    );
    drop(store);
    let store = Store::open_existing(directory.path(), adequate).unwrap();
    assert_eq!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .manifest_bytes(),
        request.manifest_bytes
    );
}
#[test]
fn packing_native_metadata_preserves_retained_indexed_version_memory_contract() {
    let directory = tempfile::tempdir().unwrap();
    let mut tight = small_index_cloud_config();
    tight.cloud.max_version_decode_bytes = 512 * 1024;
    let store = Store::open(directory.path(), tight.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let tree = tinytree_model::empty_tree().descriptor;
    let head = put_object(
        &store,
        Object::Commit(commit(
            TreeId(tree.key.id.0),
            &"working-copy packing proof".repeat(400),
        )),
        0,
    );
    let unrelated = put_object(
        &store,
        Object::Commit(commit(TreeId(tree.key.id.0), "unrelated packing candidate")),
        0,
    );
    let owner = CloudOwner {
        principal: [80; 32],
    };
    let (value, leases, _) = cloud_v2_capture(
        &store,
        owner,
        head,
        tree,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId::ROOT],
            drafts: vec![],
        },
    );
    let request = cloud_v2_request(&value, 0, "indexed-pack-poststate", leases);
    store
        .publish_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
    // Only small unrelated commit metadata is selected. The large WC commit
    // stays loose and must not pay another object's reconstruction reservation.
    store
        .pack(
            "demo",
            PackOptions {
                max_object_bytes: 1024,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(store.inner.is_packed("demo", unrelated.key).unwrap());
    assert!(!store.inner.is_packed("demo", head.key).unwrap());
    let packs = store
        .inner
        .db
        .read_tx()
        .iter(&store.inner.spaces.packs)
        .count();
    assert!(matches!(
        store.pack("demo", PackOptions::default()),
        Err(Error::Limit("cloud index native proof memory"))
    ));
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .iter(&store.inner.spaces.packs)
            .count(),
        packs
    );
    assert!(!store.inner.is_packed("demo", head.key).unwrap());
    assert!(store.inner.path("demo", head.key).exists());
    assert_eq!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .manifest_bytes(),
        request.manifest_bytes
    );
    drop(store);
    let store = Store::open_existing(directory.path(), tight.clone()).unwrap();
    drop(store);
    let mut adequate = tight;
    adequate.cloud.max_version_decode_bytes = 4 * 1024 * 1024;
    let store = Store::open_existing(directory.path(), adequate.clone()).unwrap();
    store.pack("demo", PackOptions::default()).unwrap();
    assert!(store.inner.is_packed("demo", head.key).unwrap());
    assert_eq!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .manifest_bytes(),
        request.manifest_bytes
    );
    drop(store);
    let store = Store::open_existing(directory.path(), adequate).unwrap();
    assert_eq!(
        store
            .read_cloud_version(
                "demo",
                owner,
                value.recovery.workspace,
                1,
                Default::default()
            )
            .unwrap()
            .manifest_bytes(),
        request.manifest_bytes
    );
}

#[test]
fn cloud_resource_profile_matches_open_store_and_rejects_overflow() {
    let directory = tempfile::tempdir().unwrap();
    let policy = config();
    let declared = policy.cloud_resource_profile().unwrap();
    assert!(declared.version_v2_bytes >= declared.version_v1_bytes);
    let store = Store::open(directory.path(), policy.clone()).unwrap();
    assert_eq!(declared, store.cloud_resource_profile().unwrap());
    let mut bounded = policy.clone();
    bounded.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 2 * 1024 * 1024 * 1024,
        max_entries: 65_536,
    });
    let bounded_profile = bounded.cloud_resource_profile().unwrap();
    assert!(bounded_profile.version_v3_bytes < declared.version_v3_bytes);
    bounded.max_retention_validation_bytes *= 2;
    bounded.max_retention_validation_records *= 2;
    assert_eq!(bounded.cloud_resource_profile().unwrap(), bounded_profile);
    bounded.cloud.max_version_decode_bytes *= 2;
    assert!(
        bounded.cloud_resource_profile().unwrap().version_v1_bytes
            > bounded_profile.version_v1_bytes
    );
    let untouched = directory.path().join("invalid-resident");
    bounded.cloud.resident.as_mut().unwrap().max_entries = 0;
    assert!(matches!(
        Store::open(&untouched, bounded),
        Err(Error::Invalid(_))
    ));
    assert!(!untouched.exists());
    let mut impossible = policy;
    impossible.max_retention_validation_bytes = u64::MAX;
    assert!(matches!(
        impossible.cloud_resource_profile(),
        Err(Error::Limit(_))
    ));
}

fn captured_cloud_request(
    store: &Store,
    owner: CloudOwner,
    history: tinytree_cloud::ManifestV2,
    leases: Vec<String>,
    artifacts: &[tinytree_cloud::IndexArtifactDescriptor],
    expected_generation: u64,
    token: &str,
) -> PublishCapturedCloudRequest {
    use jj_lib::object_id::ObjectId as _;
    use tinytree_cloud as c;
    let mut operations = vec![c::OperationRecord {
        operation: hex(&[0; 64]),
        view: hex(&[0; 64]),
        parents: vec![],
    }];
    let mut blobs = Vec::new();
    for member in &history.recovery.members {
        let mut lease = store
            .read_cloud_object("demo", owner, member.id, Default::default())
            .unwrap();
        let mut bytes = Vec::new();
        lease.read_to_end(&mut bytes).unwrap();
        let object =
            c::VerifiedObject::verify(member.id, member.selected, &bytes, Default::default())
                .unwrap();
        if let Some(operation) = object.operation() {
            operations.push(c::OperationRecord {
                operation: hex(member.id.bytes()),
                view: operation.view_id.hex(),
                parents: operation
                    .parents
                    .iter()
                    .map(|parent| parent.hex())
                    .collect(),
            });
        }
        blobs.push(c::BlobRecord {
            kind: match member.id {
                c::ObjectId::Operation(_) => 1,
                c::ObjectId::View(_) => 2,
            },
            id: hex(member.id.bytes()),
            blake3: member.selected.blake3,
            size: member.selected.size_bytes,
        });
    }
    for artifact in artifacts {
        let (kind, id) = match artifact.id {
            c::IndexArtifactId::Root(id) => (3, id.0),
            c::IndexArtifactId::Node(id) => (4, id.0),
        };
        blobs.push(c::BlobRecord {
            kind,
            id: hex(&id),
            blake3: artifact.selected.blake3,
            size: artifact.selected.size_bytes,
        });
    }
    operations.sort_by(|left, right| left.operation.cmp(&right.operation));
    blobs.sort_by(|left, right| (left.kind, &left.id).cmp(&(right.kind, &right.id)));
    let recovery = &history.recovery;
    let data = c::CaptureData {
        version: 1,
        policy: c::CapturePolicy::HostQuiescentV1,
        codec: c::CODEC.into(),
        operation_heads: recovery.operation_heads.iter().map(|id| hex(id)).collect(),
        working_copy: c::PortableWorkingCopy {
            workspace: recovery.working_copy.workspace_name.clone(),
            wc_operation: hex(&recovery.working_copy.wc_operation),
            snapshot_operation: hex(&recovery.working_copy.snapshot_operation),
            snapshot_commit: recovery.working_copy.snapshot_commit,
            tree: c::CaptureTree {
                terms: recovery
                    .working_copy
                    .tree
                    .terms
                    .iter()
                    .map(|id| id.0)
                    .collect(),
                labels: recovery.working_copy.tree.labels.clone(),
            },
        },
        operations,
        indexes: history
            .operation_indexes
            .iter()
            .map(|index| c::IndexRecord {
                operation: hex(&index.operation),
                root: index.root.0,
            })
            .collect(),
        blobs,
        roots: recovery
            .native_roots
            .iter()
            .map(|root| c::CapturedRoot {
                kind: root.kind as u8,
                id: root.id.0,
            })
            .collect(),
        remote: None,
    };
    let capture_data = c::encode_capture_data(&data, Default::default()).unwrap();
    let manifest = c::ManifestV3 {
        history,
        capture: c::CaptureDescriptor::of(&capture_data),
    };
    let bytes = c::encode_manifest_v3(&manifest, Default::default()).unwrap();
    PublishCapturedCloudRequest {
        publication: PublishCloudRequest {
            workspace: manifest.history.recovery.workspace,
            expected_generation,
            idempotency_key: token.into(),
            manifest: c::ManifestId::of(&bytes),
            manifest_bytes: bytes,
            staging_leases: leases,
        },
        capture_data,
    }
}

fn retirement_clone(request: &PublishCapturedCloudRequest) -> PublishCapturedCloudRequest {
    PublishCapturedCloudRequest {
        publication: request.publication.clone(),
        capture_data: request.capture_data.clone(),
    }
}
fn retirement_selector(request: &PublishCapturedCloudRequest) -> CloudVersionSelector {
    CloudVersionSelector {
        workspace: request.publication.workspace,
        generation: request.publication.expected_generation + 1,
        manifest: request.publication.manifest,
        selected: tinytree_cloud::BlobDigest::of(&request.publication.manifest_bytes),
    }
}
fn retirement_request(
    template: &PublishCapturedCloudRequest,
    generation: u64,
    previous: Option<tinytree_cloud::ManifestId>,
) -> (CloudPublicationIdentity, PublishCapturedCloudRequest) {
    let mut manifest = tinytree_cloud::decode_manifest_v3(
        &template.publication.manifest_bytes,
        template.publication.manifest,
        Default::default(),
    )
    .unwrap();
    manifest.history.recovery.sequence = generation;
    manifest.history.recovery.parents = previous.into_iter().collect();
    let identity = CloudPublicationIdentity {
        workspace: template.publication.workspace,
        generation,
        nonce: [generation as u8; 16],
    };
    let bytes = tinytree_cloud::encode_manifest_v3(&manifest, Default::default()).unwrap();
    let mut request = retirement_clone(template);
    request.publication.expected_generation = generation - 1;
    request.publication.idempotency_key = identity.token();
    request.publication.manifest = tinytree_cloud::ManifestId::of(&bytes);
    request.publication.manifest_bytes = bytes;
    if generation != 1 {
        request.publication.staging_leases.clear();
    }
    (identity, request)
}
fn retirement_alternate_raw(
    store: &Store,
    owner: CloudOwner,
    request: &mut PublishCapturedCloudRequest,
) -> (CloudObjectDescriptor, Vec<u8>) {
    let mut manifest = tinytree_cloud::decode_manifest_v3(
        &request.publication.manifest_bytes,
        request.publication.manifest,
        Default::default(),
    )
    .unwrap();
    let member = manifest
        .history
        .recovery
        .members
        .iter_mut()
        .find(|member| matches!(member.id, tinytree_cloud::ObjectId::Operation(_)))
        .unwrap();
    let mut lease = store
        .read_cloud_object("demo", owner, member.id, Default::default())
        .unwrap();
    let mut original = Vec::new();
    lease.read_to_end(&mut original).unwrap();
    drop(lease);
    let raw = reverse_cloud_fields(&original);
    assert_ne!(raw, original);
    let descriptor = CloudObjectDescriptor {
        id: member.id,
        selected: tinytree_cloud::BlobDigest::of(&raw),
    };
    let primary = store
        .put_cloud_object(
            "demo",
            owner,
            member.id,
            member.selected,
            &original,
            1,
            Default::default(),
        )
        .unwrap();
    request
        .publication
        .staging_leases
        .retain(|lease| lease != &primary.staging_lease);
    let receipt = store
        .put_cloud_object_representation(
            "demo",
            owner,
            descriptor.id,
            descriptor.selected,
            &raw,
            1,
            Default::default(),
        )
        .unwrap();
    member.selected = descriptor.selected;
    request
        .publication
        .staging_leases
        .push(receipt.staging_lease);
    let mut data = tinytree_cloud::decode_data(
        &request.capture_data,
        tinytree_cloud::data_digest(&request.capture_data),
        &Default::default(),
    )
    .unwrap();
    let blob = data
        .blobs
        .iter_mut()
        .find(|blob| blob.kind == 1 && blob.id == hex(descriptor.id.bytes()))
        .unwrap();
    blob.blake3 = descriptor.selected.blake3;
    blob.size = descriptor.selected.size_bytes;
    request.capture_data = tinytree_cloud::encode_capture_data(&data, Default::default()).unwrap();
    manifest.capture = tinytree_cloud::CaptureDescriptor::of(&request.capture_data);
    request.publication.manifest_bytes =
        tinytree_cloud::encode_manifest_v3(&manifest, Default::default()).unwrap();
    request.publication.manifest =
        tinytree_cloud::ManifestId::of(&request.publication.manifest_bytes);
    (descriptor, raw)
}

#[test]
fn cloud_pin_release_fences_are_bounded_and_survive_clock_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = config();
    policy.cloud.max_versions_per_workspace = 2;
    policy.cloud.max_pin_lifecycle_records_per_workspace = 2;
    let store = Store::open(directory.path(), policy.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [126; 32],
    };
    let (_, root, head) = fixture(&store, "pin lifecycle source");
    let (history, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![],
        },
    );
    let template = captured_cloud_request(
        &store,
        owner,
        history,
        leases,
        &artifacts,
        0,
        "pin-lifecycle-source",
    );
    let (publication, request) = retirement_request(&template, 1, None);
    store
        .publish_captured_cloud_version_v2(
            "demo",
            owner,
            publication,
            &request,
            1,
            &Default::default(),
        )
        .unwrap();
    let selected = retirement_selector(&request);
    let identity = |byte, deadline| CloudVersionPinIdentity {
        pin_id: [byte; 16],
        acquire_not_after_unix_seconds: deadline,
    };
    let active = identity(1, 200);
    let absent_release = identity(2, 200);
    let missing = identity(3, 200);
    store
        .pin_cloud_version("demo", owner, active, selected, || Ok(100))
        .unwrap();
    assert_eq!(
        store
            .cloud_version_pin("demo", owner, selected.workspace, active, || Ok(300))
            .unwrap(),
        Some(selected),
        "admission expiry never expires an active source"
    );
    store
        .pin_cloud_version("demo", owner, active, selected, || {
            Err(Error::Invalid("unused active-replay clock"))
        })
        .unwrap();
    store
        .release_cloud_version_pin("demo", owner, absent_release, selected, || Ok(110))
        .unwrap();
    assert!(
        matches!(
            store.pin_cloud_version("demo", owner, absent_release, selected, || Ok(111)),
            Err(Error::CloudReplayExpired)
        ),
        "Release fences even an accepted worker with no previous row"
    );
    assert!(matches!(
        store.pin_cloud_version("demo", owner, missing, selected, || Ok(111)),
        Err(Error::Limit("cloud pin lifecycle records"))
    ));
    assert!(
        matches!(
            store.release_cloud_version_pin("demo", owner, missing, selected, || Ok(111)),
            Err(Error::Limit("cloud pin lifecycle records"))
        ),
        "a full fence pool cannot acknowledge an absent release without fencing it"
    );
    assert_eq!(
        store
            .cloud_version_pin("demo", owner, selected.workspace, active, || Ok(111))
            .unwrap(),
        Some(selected),
        "capacity refusal preserves the active pin"
    );
    let held = store
        .read_cloud_version(
            "demo",
            owner,
            selected.workspace,
            selected.generation,
            Default::default(),
        )
        .unwrap();
    store
        .release_cloud_version_pin("demo", owner, active, selected, || Ok(112))
        .unwrap();
    assert!(
        matches!(
            store.pin_cloud_version_from_read_guard("demo", owner, active, &held, || Ok(113)),
            Err(Error::CloudReplayExpired)
        ),
        "a delayed accepted read cannot recreate its released durable pin"
    );
    let mut wrong = selected;
    wrong.selected.size_bytes += 1;
    assert!(matches!(
        store.release_cloud_version_pin("demo", owner, active, wrong, || Ok(113)),
        Err(Error::IdempotencyConflict)
    ));
    drop(held);
    drop(store);
    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    // This terminal missing result also reclaims both expired release fences.
    assert!(matches!(
        store.cloud_version_pin("demo", owner, selected.workspace, missing, || Ok(201)),
        Err(Error::CloudReplayExpired)
    ));
    drop(store);
    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    assert!(
        matches!(
            store.pin_cloud_version("demo", owner, active, selected, || Ok(100)),
            Err(Error::CloudReplayExpired)
        ),
        "reopen plus wall-clock rollback cannot undo a reclaimed release fence"
    );
    let terminal_missing = identity(4, 300);
    assert!(matches!(
        store.cloud_version_pin("demo", owner, selected.workspace, terminal_missing, || Ok(
            301
        )),
        Err(Error::CloudReplayExpired)
    ));
    drop(store);
    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    assert!(
        matches!(
            store.pin_cloud_version("demo", owner, terminal_missing, selected, || Ok(100)),
            Err(Error::CloudReplayExpired)
        ),
        "ExpiredMissing persists its floor even without any fence to reclaim"
    );
    // A changed expiry denotes a different complete identity, never a renewal.
    let replacement = identity(3, 400);
    store
        .pin_cloud_version("demo", owner, replacement, selected, || Ok(301))
        .unwrap();
    assert!(matches!(
        store.cloud_version_pin("demo", owner, selected.workspace, missing, || Ok(301)),
        Err(Error::CloudReplayExpired)
    ));
    assert_eq!(
        store
            .cloud_version_pin("demo", owner, selected.workspace, replacement, || Ok(900))
            .unwrap(),
        Some(selected)
    );
    assert!(
        store
            .cloud_version_pin(
                "demo",
                CloudOwner {
                    principal: [127; 32]
                },
                selected.workspace,
                replacement,
                || Ok(301)
            )
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store.pin_cloud_version("demo", owner, replacement, wrong, || Ok(301)),
        Err(Error::IdempotencyConflict)
    ));
    drop(store);
    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    let cold = store
        .read_cloud_version(
            "demo",
            owner,
            selected.workspace,
            selected.generation,
            Default::default(),
        )
        .unwrap();
    assert_eq!(
        tinytree_cloud::BlobDigest::of(cold.manifest_bytes()),
        selected.selected,
        "active sources reopen cold after their acquisition window"
    );
    drop(cold);
    store
        .release_cloud_version_pin("demo", owner, replacement, selected, || Ok(402))
        .unwrap();
    let fresh = identity(5, 450);
    store
        .pin_cloud_version("demo", owner, fresh, selected, || Ok(402))
        .unwrap();
    assert_eq!(
        store
            .cloud_version_pin("demo", owner, selected.workspace, fresh, || Ok(1000))
            .unwrap(),
        Some(selected),
        "expired fences do not create a lifetime row ceiling"
    );
    assert!(matches!(
        store.pin_cloud_version("demo", owner, identity(6, 1000), selected, || Ok(402)),
        Err(Error::Invalid("cloud pin admission horizon"))
    ));
    assert!(matches!(
        store.cloud_version_pin("demo", owner, selected.workspace, identity(7, 450), || Err(
            Error::Invalid("trusted clock failed")
        )),
        Err(Error::Invalid("trusted clock failed"))
    ));
}

#[test]
fn cloud_rolling_policy_shrink_waits_for_successful_atomic_publication() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = config();
    policy.cloud.max_versions_per_workspace = 8;
    let store = Store::open(directory.path(), policy.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [124; 32],
    };
    let (_, root, head) = fixture(&store, "rolling policy adoption");
    let (history, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![],
        },
    );
    let template = captured_cloud_request(
        &store,
        owner,
        history,
        leases,
        &artifacts,
        0,
        "policy-adoption",
    );
    let mut previous = None;
    for generation in 1..=4 {
        let (identity, request) = retirement_request(&template, generation, previous);
        store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                identity,
                &request,
                1,
                &Default::default(),
            )
            .unwrap();
        previous = Some(request.publication.manifest);
    }
    let before = store.cloud_catalog_budget_summary().unwrap();
    let workspace = template.publication.workspace;
    let state = store
        .cloud_retention_state("demo", owner, workspace)
        .unwrap();
    assert_eq!(state.retained_versions, 4);
    drop(store);
    policy.cloud.max_versions_per_workspace = 2;
    assert!(
        matches!(
            Store::open_existing(directory.path(), policy.clone()),
            Err(Error::Limit("cloud retained version quota"))
        ),
        "nonrolling hard bounds remain unchanged"
    );
    policy.cloud.retirement_enabled = true;
    policy.cloud.rolling_retention_enabled = true;
    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    assert_eq!(
        store.cloud_catalog_budget_summary().unwrap(),
        before,
        "reopen does not retire the existing normal suffix"
    );
    assert_eq!(
        store
            .cloud_retention_state("demo", owner, workspace)
            .unwrap(),
        state
    );
    assert_eq!(
        store
            .list_cloud_versions("demo", owner, workspace, 128, &[])
            .unwrap()
            .versions
            .len(),
        4
    );
    let old = store
        .read_cloud_version("demo", owner, workspace, 1, Default::default())
        .unwrap();
    assert_eq!(old.version().generation, 1);
    drop(old);
    let latest = store
        .read_latest_captured_cloud_version(
            "demo",
            owner,
            workspace,
            Default::default(),
            std::time::Instant::now() + Duration::from_secs(10),
        )
        .unwrap()
        .unwrap();
    assert_eq!(latest.version().generation, 4);
    drop(latest);
    let (identity, next) = retirement_request(&template, 5, previous);
    let mut invalid = retirement_clone(&next);
    invalid.capture_data.clear();
    assert!(
        store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                identity,
                &invalid,
                1,
                &Default::default()
            )
            .is_err()
    );
    assert_eq!(store.cloud_catalog_budget_summary().unwrap(), before);
    assert_eq!(
        store
            .cloud_retention_state("demo", owner, workspace)
            .unwrap(),
        state
    );
    store
        .publish_captured_cloud_version_v2("demo", owner, identity, &next, 1, &Default::default())
        .unwrap();
    let state = store
        .cloud_retention_state("demo", owner, workspace)
        .unwrap();
    assert_eq!((state.retired_through, state.retained_versions), (3, 2));
    assert_eq!(store.cloud_catalog_budget_summary().unwrap().versions, 2);
    assert_eq!(
        store
            .list_cloud_versions("demo", owner, workspace, 128, &[])
            .unwrap()
            .versions
            .iter()
            .map(|version| version.generation)
            .collect::<Vec<_>>(),
        vec![4, 5]
    );
    assert!(matches!(
        store.read_cloud_version("demo", owner, workspace, 1, Default::default()),
        Err(Error::CloudVersionExpired)
    ));
    let after = store.cloud_catalog_budget_summary().unwrap();
    drop(store);
    let store = Store::open_existing(directory.path(), policy).unwrap();
    assert_eq!(store.cloud_catalog_budget_summary().unwrap(), after);
    assert_eq!(
        store
            .cloud_retention_state("demo", owner, workspace)
            .unwrap(),
        state
    );
}

#[test]
fn cloud_rolling_window_keeps_only_live_pin_and_reader_exceptions() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = config();
    policy.cloud.max_versions_per_workspace = 2;
    policy.cloud.retirement_enabled = true;
    policy.cloud.rolling_retention_enabled = true;
    let store = Store::open(directory.path(), policy.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [123; 32],
    };
    let (_, root, old_head) = fixture(&store, "pinned old snapshot");
    let (history, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        old_head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(old_head.key.id.0)],
            drafts: vec![],
        },
    );
    let template = captured_cloud_request(
        &store,
        owner,
        history,
        leases,
        &artifacts,
        0,
        "rolling-source",
    );
    let (first_identity, first) = retirement_request(&template, 1, None);
    store
        .publish_captured_cloud_version("demo", owner, &first, 1, &Default::default())
        .unwrap();
    let old = retirement_selector(&first);
    let pin = CloudVersionPinIdentity {
        pin_id: [1; 16],
        acquire_not_after_unix_seconds: 200,
    };
    store
        .pin_cloud_version("demo", owner, pin, old, || Ok(100))
        .unwrap();
    let (second_identity, second) =
        retirement_request(&template, 2, Some(first.publication.manifest));
    store
        .publish_captured_cloud_version_v2(
            "demo",
            owner,
            second_identity,
            &second,
            1,
            &Default::default(),
        )
        .unwrap();
    let reader = store
        .read_cloud_version("demo", owner, old.workspace, 2, Default::default())
        .unwrap();

    // A genuinely different snapshot need not contain all old DATA or undo.
    let (_, new_root, new_head) = fixture(&store, "independent current snapshot");
    let (history, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        new_head,
        new_root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(new_head.key.id.0)],
            drafts: vec![],
        },
    );
    let replacement = captured_cloud_request(
        &store,
        owner,
        history,
        leases,
        &artifacts,
        2,
        "rolling-replacement",
    );
    let second_replay = retirement_clone(&second);
    let mut current = second;
    for generation in 3..=6 {
        let (identity, mut next) =
            retirement_request(&replacement, generation, Some(current.publication.manifest));
        if generation == 3 {
            next.publication.staging_leases = replacement.publication.staging_leases.clone();
        }
        store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                identity,
                &next,
                1,
                &Default::default(),
            )
            .unwrap();
        current = next;
    }
    let state = store
        .cloud_retention_state("demo", owner, old.workspace)
        .unwrap();
    assert_eq!((state.retired_through, state.retained_versions), (4, 2));
    let listed = store
        .list_cloud_versions("demo", owner, old.workspace, 128, &[])
        .unwrap();
    assert_eq!(
        listed
            .versions
            .iter()
            .map(|version| version.generation)
            .collect::<Vec<_>>(),
        vec![5, 6]
    );
    assert_eq!(store.cloud_catalog_budget_summary().unwrap().versions, 4);
    let cold = store
        .read_cloud_version("demo", owner, old.workspace, 1, Default::default())
        .unwrap();
    assert_eq!(cold.manifest_bytes(), first.publication.manifest_bytes);
    assert!(matches!(
        store.cloud_publication("demo", owner, &first.publication.idempotency_key),
        Err(Error::CloudReplayExpired)
    ));
    assert!(matches!(
        store.publish_captured_cloud_version("demo", owner, &first, 1, &Default::default()),
        Err(Error::CloudReplayExpired)
    ));
    assert!(matches!(
        store.cloud_publication_v2("demo", owner, second_identity),
        Err(Error::CloudReplayExpired)
    ));
    assert!(matches!(
        store.publish_captured_cloud_version_v2(
            "demo",
            owner,
            second_identity,
            &second_replay,
            1,
            &Default::default()
        ),
        Err(Error::CloudReplayExpired)
    ));
    assert!(matches!(
        store.cloud_publication_v2("demo", owner, first_identity),
        Err(Error::CloudReplayExpired)
    ));
    assert!(matches!(
        store.publish_captured_cloud_version_v2(
            "demo",
            owner,
            first_identity,
            &first,
            1,
            &Default::default()
        ),
        Err(Error::CloudReplayExpired)
    ));
    let latest = store
        .read_latest_captured_cloud_version(
            "demo",
            owner,
            old.workspace,
            Default::default(),
            std::time::Instant::now() + Duration::from_secs(10),
        )
        .unwrap()
        .unwrap();
    assert_eq!(latest.version().generation, 6);
    drop(latest);
    let (next_identity, next) =
        retirement_request(&replacement, 7, Some(current.publication.manifest));
    let mut invalid = retirement_clone(&next);
    invalid.capture_data.clear();
    assert!(
        store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                next_identity,
                &invalid,
                1,
                &Default::default()
            )
            .is_err()
    );
    let (stale_identity, stale) =
        retirement_request(&replacement, 8, Some(current.publication.manifest));
    assert!(matches!(
        store.publish_captured_cloud_version_v2(
            "demo",
            owner,
            stale_identity,
            &stale,
            1,
            &Default::default()
        ),
        Err(Error::StaleCloudWorkspace(_))
    ));
    assert_eq!(
        store
            .cloud_retention_state("demo", owner, old.workspace)
            .unwrap(),
        state
    );

    drop(reader);
    assert!(
        matches!(
            store.read_cloud_version("demo", owner, old.workspace, 2, Default::default()),
            Err(Error::CloudVersionExpired)
        ),
        "stale metadata cannot grant an exact read"
    );
    assert_eq!(
        store.cloud_catalog_budget_summary().unwrap().versions,
        4,
        "guard drop performs no database write"
    );
    // A malformed stale receipt must abort publication/cleanup without erasing
    // either the evidence or the valid recent window.
    let receipts = store
        .inner
        .db
        .keyspace("cloud-receipts-v1", KeyspaceCreateOptions::default)
        .unwrap();
    let (receipt_key, original) = store
        .inner
        .db
        .read_tx()
        .iter(&receipts)
        .map(|item| item.into_inner().unwrap())
        .find(|(_, bytes)| {
            serde_json::from_slice::<serde_json::Value>(bytes).unwrap()["version"]["generation"]
                == 2
        })
        .unwrap();
    let mut bad: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let byte = bad["version"]["manifest"][0].as_u64().unwrap();
    bad["version"]["manifest"][0] = serde_json::Value::from(byte ^ 1);
    let bad = serde_json::to_vec(&bad).unwrap();
    let mut tx = store
        .inner
        .db
        .write_tx()
        .durability(Some(PersistMode::SyncAll));
    tx.insert(&receipts, &*receipt_key, &bad);
    tx.commit().unwrap();
    assert!(matches!(
        store.publish_captured_cloud_version_v2(
            "demo",
            owner,
            next_identity,
            &next,
            1,
            &Default::default()
        ),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(store.gc("demo", 100), Err(Error::Corrupt(_))));
    assert_eq!(
        store
            .cloud_retention_state("demo", owner, old.workspace)
            .unwrap(),
        state
    );
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .get(&receipts, &receipt_key)
            .unwrap()
            .unwrap()
            .as_ref(),
        bad.as_slice()
    );
    let mut tx = store
        .inner
        .db
        .write_tx()
        .durability(Some(PersistMode::SyncAll));
    tx.insert(&receipts, &*receipt_key, &*original);
    tx.commit().unwrap();
    store.gc("demo", 100).unwrap();
    assert_eq!(store.cloud_catalog_budget_summary().unwrap().versions, 3);
    assert_eq!(
        store.descriptor("demo", old_head.key).unwrap(),
        Some(old_head)
    );
    assert_eq!(
        store.descriptor("demo", new_head.key).unwrap(),
        Some(new_head)
    );
    drop(cold);
    drop(receipts);
    drop(store);

    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    let cold = store
        .read_cloud_version("demo", owner, old.workspace, 1, Default::default())
        .unwrap();
    assert_eq!(cold.manifest_bytes(), first.publication.manifest_bytes);
    store
        .release_cloud_version_pin("demo", owner, pin, old, || Ok(100))
        .unwrap();
    let still_held = store
        .read_cloud_version("demo", owner, old.workspace, 1, Default::default())
        .unwrap();
    drop(still_held);
    drop(cold);
    assert!(matches!(
        store.read_cloud_version("demo", owner, old.workspace, 1, Default::default()),
        Err(Error::CloudVersionExpired)
    ));
    assert_eq!(store.cloud_catalog_budget_summary().unwrap().versions, 3);
    drop(store);
    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    assert_eq!(
        store.cloud_catalog_budget_summary().unwrap().versions,
        2,
        "reopen reaps well-formed unprotected exceptions"
    );
    assert!(matches!(
        store.read_cloud_version("demo", owner, old.workspace, 1, Default::default()),
        Err(Error::CloudVersionExpired)
    ));
    store.gc("demo", 100).unwrap();
    assert_eq!(store.descriptor("demo", old_head.key).unwrap(), None);
    assert_eq!(
        store.descriptor("demo", new_head.key).unwrap(),
        Some(new_head)
    );
    // Delayed compatible captures must not move the logical head backwards
    // when its publication generation leaves the dense recent window.
    let with_sequence = |mut request: PublishCapturedCloudRequest, sequence| {
        let mut manifest = tinytree_cloud::decode_manifest_v3(
            &request.publication.manifest_bytes,
            request.publication.manifest,
            Default::default(),
        )
        .unwrap();
        manifest.history.recovery.sequence = sequence;
        request.publication.manifest_bytes =
            tinytree_cloud::encode_manifest_v3(&manifest, Default::default()).unwrap();
        request.publication.manifest =
            tinytree_cloud::ManifestId::of(&request.publication.manifest_bytes);
        request
    };
    for (generation, sequence, expected) in [(7, 100, 7), (8, 99, 7), (9, 98, 7)] {
        let (identity, next) =
            retirement_request(&replacement, generation, Some(current.publication.manifest));
        let next = with_sequence(next, sequence);
        store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                identity,
                &next,
                1,
                &Default::default(),
            )
            .unwrap();
        let latest = store
            .read_latest_captured_cloud_version(
                "demo",
                owner,
                old.workspace,
                Default::default(),
                std::time::Instant::now() + Duration::from_secs(10),
            )
            .unwrap()
            .unwrap();
        assert_eq!(latest.version().generation, expected);
        drop(latest);
        current = next;
    }
    assert_eq!(
        store.cloud_catalog_budget_summary().unwrap().versions,
        3,
        "only one unpinned logical-head exception supplements recent-N"
    );
    assert!(
        matches!(
            store.read_cloud_version("demo", owner, old.workspace, 7, Default::default()),
            Err(Error::CloudVersionExpired)
        ),
        "the logical singleton is not direct old-source authority"
    );
    store.gc("demo", 100).unwrap();
    drop(store);
    let store = Store::open_existing(directory.path(), policy).unwrap();
    let latest = store
        .read_latest_captured_cloud_version(
            "demo",
            owner,
            old.workspace,
            Default::default(),
            std::time::Instant::now() + Duration::from_secs(10),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        latest.version().generation,
        7,
        "the singleton survives cold reopen and GC"
    );
    drop(latest);
    let (identity, next) = retirement_request(&replacement, 10, Some(current.publication.manifest));
    let next = with_sequence(next, 101);
    store
        .publish_captured_cloud_version_v2("demo", owner, identity, &next, 1, &Default::default())
        .unwrap();
    assert_eq!(
        store.cloud_catalog_budget_summary().unwrap().versions,
        2,
        "a newer logical head reaps the old singleton"
    );
    let latest = store
        .read_latest_captured_cloud_version(
            "demo",
            owner,
            old.workspace,
            Default::default(),
            std::time::Instant::now() + Duration::from_secs(10),
        )
        .unwrap()
        .unwrap();
    assert_eq!(latest.version().generation, 10);
    drop(latest);
    let state = store
        .cloud_retention_state("demo", owner, old.workspace)
        .unwrap();
    let before = store.cloud_catalog_budget_summary().unwrap();
    let (identity, conflicting) =
        retirement_request(&replacement, 11, Some(next.publication.manifest));
    let mut conflicting = with_sequence(conflicting, 101);
    let mut data = tinytree_cloud::decode_data(
        &conflicting.capture_data,
        tinytree_cloud::CaptureDataId::of(&conflicting.capture_data).0,
        &Default::default(),
    )
    .unwrap();
    data.remote = Some(tinytree_cloud::PortableRemoteState {
        observations: vec![],
        archived_pushes: vec![],
        landing: None,
    });
    conflicting.capture_data =
        tinytree_cloud::encode_capture_data(&data, Default::default()).unwrap();
    let mut manifest = tinytree_cloud::decode_manifest_v3(
        &conflicting.publication.manifest_bytes,
        conflicting.publication.manifest,
        Default::default(),
    )
    .unwrap();
    manifest.capture = tinytree_cloud::CaptureDescriptor::of(&conflicting.capture_data);
    conflicting.publication.manifest_bytes =
        tinytree_cloud::encode_manifest_v3(&manifest, Default::default()).unwrap();
    conflicting.publication.manifest =
        tinytree_cloud::ManifestId::of(&conflicting.publication.manifest_bytes);
    assert!(matches!(
        store.publish_captured_cloud_version_v2(
            "demo",
            owner,
            identity,
            &conflicting,
            1,
            &Default::default()
        ),
        Err(Error::Invalid("ambiguous latest capture sequence"))
    ));
    manifest.history.recovery.sequence = 102;
    manifest.history.recovery.exporter = [43; 16];
    conflicting.publication.manifest_bytes =
        tinytree_cloud::encode_manifest_v3(&manifest, Default::default()).unwrap();
    conflicting.publication.manifest =
        tinytree_cloud::ManifestId::of(&conflicting.publication.manifest_bytes);
    assert!(matches!(
        store.publish_captured_cloud_version_v2(
            "demo",
            owner,
            identity,
            &conflicting,
            1,
            &Default::default()
        ),
        Err(Error::Invalid("ambiguous latest capture exporter"))
    ));
    assert_eq!(
        store
            .cloud_retention_state("demo", owner, old.workspace)
            .unwrap(),
        state
    );
    assert_eq!(store.cloud_catalog_budget_summary().unwrap(), before);
}

#[test]
fn cloud_retirement_keeps_monotonic_generations_exact_pins_undo_and_raw_selections() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = config();
    policy.cloud.max_versions_per_workspace = 2;
    policy.cloud.retirement_enabled = true;
    policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 256 * 1024 * 1024,
        max_entries: 65_536,
    });
    let store = Store::open(directory.path(), policy.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [121; 32],
    };
    let (_, root, head) = fixture(&store, "retained private undo source");
    let (history, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![],
        },
    );
    let mut template = captured_cloud_request(
        &store,
        owner,
        history,
        leases,
        &artifacts,
        0,
        "retirement-source",
    );
    let (raw_descriptor, raw) = retirement_alternate_raw(&store, owner, &mut template);
    let (legacy_identity, first) = retirement_request(&template, 1, None);
    store
        .publish_captured_cloud_version("demo", owner, &first, 1, &Default::default())
        .unwrap();
    // A legacy text token looking exactly like v2 cannot occupy v2's binary domain.
    assert!(
        store
            .cloud_publication_v2("demo", owner, legacy_identity)
            .unwrap()
            .is_none()
    );
    let (second_identity, second) =
        retirement_request(&template, 2, Some(first.publication.manifest));
    let second_receipt = store
        .publish_captured_cloud_version_v2(
            "demo",
            owner,
            second_identity,
            &second,
            1,
            &Default::default(),
        )
        .unwrap();
    let old = retirement_selector(&first);
    let pin_id = CloudVersionPinIdentity {
        pin_id: [7; 16],
        acquire_not_after_unix_seconds: 200,
    };
    store
        .pin_cloud_version("demo", owner, pin_id, old, || Ok(100))
        .unwrap();
    assert_eq!(
        store
            .cloud_version_pin("demo", owner, old.workspace, pin_id, || Ok(100))
            .unwrap(),
        Some(old)
    );
    store
        .pin_cloud_version("demo", owner, pin_id, old, || Ok(100))
        .unwrap();
    let request = CloudRetirementRequest {
        expected_current: retirement_selector(&second),
        expected_floor: 0,
        expected_revision: 0,
        retire_through: 1,
    };
    let reader = store
        .read_cloud_version("demo", owner, old.workspace, 1, Default::default())
        .unwrap();
    assert!(matches!(
        store.retire_cloud_versions("demo", owner, request),
        Err(Error::Invalid("cloud version has active readers"))
    ));
    drop(reader);
    assert!(matches!(
        store.retire_cloud_versions("demo", owner, request),
        Err(Error::Invalid("cloud version has durable pins"))
    ));
    drop(store);
    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    assert!(matches!(
        store.retire_cloud_versions("demo", owner, request),
        Err(Error::Invalid("cloud version has durable pins"))
    ));
    let mut wrong = old;
    wrong.selected.size_bytes += 1;
    assert!(matches!(
        store.release_cloud_version_pin("demo", owner, pin_id, wrong, || Ok(100)),
        Err(Error::IdempotencyConflict)
    ));
    store
        .release_cloud_version_pin("demo", owner, pin_id, old, || Ok(100))
        .unwrap();
    assert!(matches!(
        store.cloud_version_pin("demo", owner, old.workspace, pin_id, || Ok(100)),
        Err(Error::CloudReplayExpired)
    ));
    store
        .release_cloud_version_pin("demo", owner, pin_id, old, || Ok(100))
        .unwrap();
    let cursor = store
        .list_cloud_versions("demo", owner, old.workspace, 1, &[])
        .unwrap()
        .continuation;
    assert!(!cursor.is_empty());
    // Corruption boundary: retirement must not erase a malformed receipt and
    // make a catalog pass reopen by deleting the evidence of a wrong binding.
    {
        let receipts = store
            .inner
            .db
            .keyspace("cloud-receipts-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let (receipt_key, original_receipt) = store
            .inner
            .db
            .read_tx()
            .iter(&receipts)
            .map(|item| item.into_inner().unwrap())
            .find(|(_, bytes)| {
                serde_json::from_slice::<serde_json::Value>(bytes).unwrap()["format"] == 1
            })
            .unwrap();
        let mut corrupted: serde_json::Value = serde_json::from_slice(&original_receipt).unwrap();
        let byte = corrupted["version"]["manifest"][0].as_u64().unwrap();
        corrupted["version"]["manifest"][0] = serde_json::Value::from(byte ^ 1);
        let corrupted = serde_json::to_vec(&corrupted).unwrap();
        let mut tx = store
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        tx.insert(&receipts, &*receipt_key, &corrupted);
        tx.commit().unwrap();
        assert!(matches!(
            store.retire_cloud_versions("demo", owner, request),
            Err(Error::Corrupt("cloud receipt replay history"))
        ));
        assert_eq!(
            store
                .cloud_retention_state("demo", owner, old.workspace)
                .unwrap()
                .retired_through,
            0
        );
        assert_eq!(
            store
                .inner
                .db
                .read_tx()
                .get(&receipts, &receipt_key)
                .unwrap()
                .unwrap()
                .as_ref(),
            corrupted.as_slice()
        );
        let mut tx = store
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        tx.insert(&receipts, &*receipt_key, &*original_receipt);
        tx.commit().unwrap();
    }
    let retired = store.retire_cloud_versions("demo", owner, request).unwrap();
    assert_eq!(retired.retired_through, 1);
    assert_eq!(
        store.retire_cloud_versions("demo", owner, request).unwrap(),
        retired
    );
    assert!(matches!(
        store.list_cloud_versions("demo", owner, old.workspace, 1, &cursor),
        Err(Error::CloudRetentionChanged)
    ));
    assert!(matches!(
        store.read_cloud_version("demo", owner, old.workspace, 1, Default::default()),
        Err(Error::CloudVersionExpired)
    ));
    assert!(matches!(
        store.publish_captured_cloud_version("demo", owner, &first, 1, &Default::default()),
        Err(Error::CloudReplayExpired)
    ));
    assert!(matches!(
        store.cloud_publication("demo", owner, &legacy_identity.token()),
        Err(Error::CloudReplayExpired)
    ));
    assert!(matches!(
        store.cloud_publication_v2("demo", owner, legacy_identity),
        Err(Error::CloudReplayExpired)
    ));
    assert_eq!(
        store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                second_identity,
                &second,
                1,
                &Default::default()
            )
            .unwrap(),
        second_receipt
    );
    let mut conflict = retirement_clone(&second);
    conflict.publication.manifest_bytes.push(0);
    assert!(matches!(
        store.publish_captured_cloud_version_v2(
            "demo",
            owner,
            second_identity,
            &conflict,
            1,
            &Default::default()
        ),
        Err(Error::IdempotencyConflict)
    ));
    let mut foreign_workspace = retirement_clone(&first);
    foreign_workspace.publication.workspace = [177; 32];
    foreign_workspace.publication.expected_generation = 0;
    assert!(matches!(
        store.publish_captured_cloud_version(
            "demo",
            owner,
            &foreign_workspace,
            1,
            &Default::default()
        ),
        Err(Error::CloudReplayExpired)
    ));
    assert!(
        store
            .cloud_publication(
                "demo",
                CloudOwner {
                    principal: [122; 32]
                },
                &legacy_identity.token()
            )
            .unwrap()
            .is_none()
    );
    assert!(
        !store.supports_durable_retained_offload(),
        "old eternal-exact capability is not a pin-aware lifecycle contract"
    );
    let mut current = second;
    let mut current_identity = second_identity;
    for generation in 3..=6 {
        let state = store
            .cloud_retention_state("demo", owner, old.workspace)
            .unwrap();
        if state.retained_versions == 2 {
            store
                .retire_cloud_versions(
                    "demo",
                    owner,
                    CloudRetirementRequest {
                        expected_current: retirement_selector(&current),
                        expected_floor: state.retired_through,
                        expected_revision: state.retention_revision,
                        retire_through: state.retired_through + 1,
                    },
                )
                .unwrap();
        }
        let (identity, next) =
            retirement_request(&template, generation, Some(current.publication.manifest));
        let receipt = store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                identity,
                &next,
                1,
                &Default::default(),
            )
            .unwrap();
        assert_eq!(receipt.workspace.generation, generation);
        current = next;
        current_identity = identity;
        store.gc("demo", 100).unwrap();
    }
    let state = store
        .cloud_retention_state("demo", owner, old.workspace)
        .unwrap();
    assert_eq!(state.retired_through, 4);
    assert_eq!(state.retained_versions, 2);
    let listed = store
        .list_cloud_versions("demo", owner, old.workspace, 128, &[])
        .unwrap();
    assert_eq!(
        listed
            .versions
            .iter()
            .map(|version| version.generation)
            .collect::<Vec<_>>(),
        vec![5, 6]
    );
    let latest = store
        .read_latest_captured_cloud_version(
            "demo",
            owner,
            old.workspace,
            Default::default(),
            std::time::Instant::now() + Duration::from_secs(10),
        )
        .unwrap()
        .unwrap();
    assert_eq!(latest.version().generation, 6);
    let mut selected = store
        .read_cloud_object_representation("demo", owner, raw_descriptor, Default::default())
        .unwrap();
    let mut actual = Vec::new();
    selected.read_to_end(&mut actual).unwrap();
    assert_eq!(
        actual, raw,
        "successor keeps the exact non-primary OP representation"
    );
    drop(selected);
    drop(latest);
    let before = store.cloud_catalog_budget_summary().unwrap();
    assert_eq!(before.versions, 2);
    assert_eq!(before.publication_receipts, 2);
    drop(store);
    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    assert_eq!(store.cloud_catalog_budget_summary().unwrap(), before);
    assert!(
        store
            .cloud_publication_v2("demo", owner, current_identity)
            .unwrap()
            .is_some()
    );
    assert_eq!(store.descriptor("demo", head.key).unwrap(), Some(head));
    let read = store
        .read_cloud_version("demo", owner, old.workspace, 5, Default::default())
        .unwrap();
    let fifth = CloudVersionSelector {
        workspace: old.workspace,
        generation: 5,
        manifest: read.version().manifest,
        selected: tinytree_cloud::BlobDigest::of(read.manifest_bytes()),
    };
    drop(read);
    // Calibrate the exact prospective catalog: release fences are now durable
    // rows, so a replacement Active hold coexists with those rows. The finite
    // policy still fits one such arena plus slack, never two physical arenas.
    let calibration_pin = CloudVersionPinIdentity {
        pin_id: [98; 16],
        acquire_not_after_unix_seconds: 200,
    };
    store
        .pin_cloud_version("demo", owner, calibration_pin, fifth, || Ok(100))
        .unwrap();
    store
        .release_cloud_version_pin("demo", owner, calibration_pin, fifth, || Ok(100))
        .unwrap();
    let prospective_probe = CloudVersionPinIdentity {
        pin_id: [97; 16],
        acquire_not_after_unix_seconds: 120,
    };
    store
        .pin_cloud_version("demo", owner, prospective_probe, fifth, || Ok(100))
        .unwrap();
    drop(store);
    let prospective = cloud::resident::preflight(directory.path(), &policy)
        .unwrap()
        .charge()
        .unwrap();
    let store = Store::open_existing(directory.path(), policy.clone()).unwrap();
    // Reclaim this probe only after its admission window; the durable clock
    // floor prevents a delayed probe Acquire from returning after deletion.
    store
        .release_cloud_version_pin("demo", owner, prospective_probe, fifth, || Ok(120))
        .unwrap();
    drop(store);
    let arena = cloud::resident::preflight(directory.path(), &policy)
        .unwrap()
        .charge()
        .unwrap();
    let max_bytes = prospective.bytes + 65536;
    assert!(max_bytes < 2 * arena.bytes);
    policy.cloud.resident.as_mut().unwrap().max_bytes = max_bytes;
    let store = Store::open_existing(directory.path(), policy).unwrap();
    let request = CloudRetirementRequest {
        expected_current: retirement_selector(&current),
        expected_floor: state.retired_through,
        expected_revision: state.retention_revision,
        retire_through: 5,
    };
    let read = store
        .read_cloud_version("demo", owner, old.workspace, 5, Default::default())
        .unwrap();
    let replay = store
        .cloud_publication_v2("demo", owner, current_identity)
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                current_identity,
                &current,
                1,
                &Default::default()
            )
            .unwrap(),
        replay,
        "scalar receipt lookup and exact replay share exclusion without a second catalog arena"
    );
    let transfer_pin = CloudVersionPinIdentity {
        pin_id: [99; 16],
        acquire_not_after_unix_seconds: 200,
    };
    assert_eq!(
        store
            .pin_cloud_version_from_read_guard("demo", owner, transfer_pin, &read, || Ok(100))
            .unwrap(),
        fifth
    );
    assert_eq!(
        store
            .pin_cloud_version_from_read_guard("demo", owner, transfer_pin, &read, || Ok(100))
            .unwrap(),
        fifth
    );
    assert_eq!(
        store
            .cloud_version_pin("demo", owner, old.workspace, transfer_pin, || Ok(100))
            .unwrap(),
        Some(fifth)
    );
    let (_foreign_directory, foreign) = open();
    for result in [
        foreign.pin_cloud_version_from_read_guard("demo", owner, transfer_pin, &read, || Ok(100)),
        store.pin_cloud_version_from_read_guard("other", owner, transfer_pin, &read, || Ok(100)),
        store.pin_cloud_version_from_read_guard(
            "demo",
            CloudOwner {
                principal: [122; 32],
            },
            transfer_pin,
            &read,
            || Ok(100),
        ),
    ] {
        assert!(matches!(
            result,
            Err(Error::Invalid("cloud version pin read guard scope"))
        ));
    }
    drop(read);
    let independent = store
        .read_cloud_version("demo", owner, old.workspace, 6, Default::default())
        .unwrap();
    assert!(matches!(
        store.retire_cloud_versions("demo", owner, request),
        Err(Error::Limit("cloud resident memory"))
    ));
    assert!(matches!(
        store.pin_cloud_version_from_read_guard("demo", owner, transfer_pin, &independent, || Ok(
            100
        )),
        Err(Error::IdempotencyConflict)
    ));
    assert_eq!(
        store
            .cloud_version_pin("demo", owner, old.workspace, transfer_pin, || Ok(100))
            .unwrap(),
        Some(fifth)
    );
    drop(independent);
    assert!(
        matches!(
            store.retire_cloud_versions("demo", owner, request),
            Err(Error::Invalid("cloud version has durable pins"))
        ),
        "durable transfer survives the last ephemeral reader"
    );
    store
        .release_cloud_version_pin("demo", owner, transfer_pin, fifth, || Ok(100))
        .unwrap();
    assert_eq!(
        store
            .cloud_retention_state("demo", owner, old.workspace)
            .unwrap(),
        state
    );
    assert_eq!(
        store
            .retire_cloud_versions("demo", owner, request)
            .unwrap()
            .retired_through,
        5
    );
    assert!(matches!(
        store.retire_cloud_versions(
            "demo",
            owner,
            CloudRetirementRequest {
                expected_revision: 0,
                ..request
            }
        ),
        Err(Error::CloudRetentionChanged)
    ));
}

#[test]
fn cloud_retirement_rejects_valid_successors_missing_undo_or_exact_raw_coverage() {
    for omit_undo in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut policy = config();
        policy.cloud.retirement_enabled = true;
        let store = Store::open(directory.path(), policy.clone()).unwrap();
        store.create_repository("demo").unwrap();
        let owner = CloudOwner {
            principal: [123; 32],
        };
        let (_, root, head) = fixture(&store, "old private undo");
        let (history, leases, artifacts) = cloud_v2_capture(
            &store,
            owner,
            head,
            root,
            tinytree_cloud::PortableIndexSnapshot {
                roots: vec![CommitId(head.key.id.0)],
                drafts: vec![],
            },
        );
        let original = captured_cloud_request(
            &store,
            owner,
            history,
            leases,
            &artifacts,
            0,
            "coverage-first",
        );
        let mut first = retirement_clone(&original);
        retirement_alternate_raw(&store, owner, &mut first);
        store
            .publish_captured_cloud_version("demo", owner, &first, 1, &Default::default())
            .unwrap();
        let template = if omit_undo {
            let mut snapshot = commit(TreeId(root.key.id.0), "independent newer working copy");
            snapshot.change_id = ChangeId([231; 16]);
            let other = put_object(&store, Object::Commit(snapshot), 0);
            let (history, leases, artifacts) = cloud_v2_capture(
                &store,
                owner,
                other,
                root,
                tinytree_cloud::PortableIndexSnapshot {
                    roots: vec![CommitId(other.key.id.0)],
                    drafts: vec![],
                },
            );
            captured_cloud_request(
                &store,
                owner,
                history,
                leases,
                &artifacts,
                1,
                "coverage-second",
            )
        } else {
            original
        };
        let (identity, mut second) =
            retirement_request(&template, 2, Some(first.publication.manifest));
        // Newly uploaded independent selections still need their initial stages.
        if omit_undo {
            second.publication.staging_leases = template.publication.staging_leases.clone();
        }
        store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                identity,
                &second,
                1,
                &Default::default(),
            )
            .unwrap();
        let before = store.cloud_catalog_budget_summary().unwrap();
        let request = CloudRetirementRequest {
            expected_current: retirement_selector(&second),
            expected_floor: 0,
            expected_revision: 0,
            retire_through: 1,
        };
        assert!(
            matches!(
                store.retire_cloud_versions("demo", owner, request),
                Err(Error::Invalid(
                    "cloud retirement successor coverage incomplete"
                ))
            ),
            "omit_undo={omit_undo}"
        );
        assert_eq!(store.cloud_catalog_budget_summary().unwrap(), before);
        assert!(
            store
                .cloud_publication("demo", owner, "unknown-legacy-key")
                .unwrap()
                .is_none(),
            "refusal cannot close replay namespace"
        );
        assert_eq!(
            store
                .read_cloud_version(
                    "demo",
                    owner,
                    first.publication.workspace,
                    1,
                    Default::default()
                )
                .unwrap()
                .manifest_bytes(),
            first.publication.manifest_bytes
        );
        drop(store);
        let default_store = Store::open_existing(directory.path(), config()).unwrap();
        assert!(
            default_store.supports_durable_retained_offload(),
            "v2 receipts alone do not enable retirement or weaken the old promise"
        );
        assert!(matches!(
            default_store.retire_cloud_versions("demo", owner, request),
            Err(Error::Invalid("cloud retirement is disabled"))
        ));
    }
}

fn retirement_remote_state(
    request: &mut PublishCapturedCloudRequest,
    remote: tinytree_cloud::PortableRemoteState,
) {
    use tinytree_cloud as c;
    let mut data = c::decode_data(
        &request.capture_data,
        c::data_digest(&request.capture_data),
        &Default::default(),
    )
    .unwrap();
    data.version = if remote.landing.is_some() { 2 } else { 1 };
    data.remote = Some(remote);
    let mut manifest = c::decode_manifest_v3(
        &request.publication.manifest_bytes,
        request.publication.manifest,
        Default::default(),
    )
    .unwrap();
    // Wrapper IDs depend on DATA; only their native root union is needed to
    // construct the exact final DATA/root binding below.
    let intents =
        c::capture_recovery_intents(&data, c::CaptureDataId([0; 32]), Default::default()).unwrap();
    manifest.history.recovery.native_roots.extend(
        intents
            .iter()
            .flat_map(|intent| intent.roots.iter().copied()),
    );
    manifest
        .history
        .recovery
        .native_roots
        .sort_by_key(|key| (key.kind as u8, key.id.0));
    manifest.history.recovery.native_roots.dedup();
    data.roots = manifest
        .history
        .recovery
        .native_roots
        .iter()
        .map(|key| c::CapturedRoot {
            kind: key.kind as u8,
            id: key.id.0,
        })
        .collect();
    request.capture_data = c::encode_capture_data(&data, Default::default()).unwrap();
    manifest.capture = c::CaptureDescriptor::of(&request.capture_data);
    manifest.history.recovery.intent_roots =
        c::capture_recovery_intents(&data, manifest.capture.id, Default::default()).unwrap();
    request.publication.manifest_bytes =
        c::encode_manifest_v3(&manifest, Default::default()).unwrap();
    request.publication.manifest = c::ManifestId::of(&request.publication.manifest_bytes);
}

#[test]
fn cloud_retirement_compares_verified_observations_without_expiring_uncertain_intents() {
    use tinytree_cloud as c;
    enum Head {
        Old,
        Advanced,
    }
    struct Case {
        name: &'static str,
        observation: Option<(u64, Head)>,
        confirmed_push: Option<u64>,
        landing_profile: &'static str,
        allowed: bool,
    }
    let cases = [
        Case {
            name: "advanced observed main",
            observation: Some((3, Head::Advanced)),
            confirmed_push: None,
            landing_profile: "v1",
            allowed: true,
        },
        Case {
            name: "missing observation",
            observation: None,
            confirmed_push: None,
            landing_profile: "v1",
            allowed: false,
        },
        Case {
            name: "regressing observed generation",
            observation: Some((1, Head::Old)),
            confirmed_push: None,
            landing_profile: "v1",
            allowed: false,
        },
        Case {
            name: "equal generation changed head",
            observation: Some((2, Head::Advanced)),
            confirmed_push: None,
            landing_profile: "v1",
            allowed: false,
        },
        Case {
            name: "changed pending push",
            observation: Some((3, Head::Advanced)),
            confirmed_push: Some(3),
            landing_profile: "v1",
            allowed: false,
        },
        Case {
            name: "changed pending landing",
            observation: Some((3, Head::Advanced)),
            confirmed_push: None,
            landing_profile: "v2",
            allowed: false,
        },
    ];
    for case in cases {
        let directory = tempfile::tempdir().unwrap();
        let mut policy = config();
        policy.cloud.retirement_enabled = true;
        policy.cloud.max_versions_per_workspace = 2;
        let store = Store::open(directory.path(), policy).unwrap();
        store.create_repository("demo").unwrap();
        let owner = CloudOwner {
            principal: [124; 32],
        };
        let (_, root, head) = fixture(&store, "private source with pending intents");
        let mut advanced = commit(TreeId(root.key.id.0), "observed main advanced");
        advanced.parents = vec![CommitId(head.key.id.0)];
        let advanced = put_object(&store, Object::Commit(advanced), 0);
        let (history, leases, artifacts) = cloud_v2_capture(
            &store,
            owner,
            head,
            root,
            c::PortableIndexSnapshot {
                roots: vec![CommitId(head.key.id.0)],
                drafts: vec![],
            },
        );
        let template = captured_cloud_request(
            &store,
            owner,
            history,
            leases,
            &artifacts,
            0,
            "intent-source",
        );
        let data = c::decode_data(
            &template.capture_data,
            c::data_digest(&template.capture_data),
            &Default::default(),
        )
        .unwrap();
        let old_remote = c::PortableRemoteState {
            observations: vec![c::RemoteObservation {
                reference: "main".into(),
                bookmark: "main".into(),
                generation: 2,
                head: Some(hex(&head.key.id.0)),
            }],
            archived_pushes: vec![c::ArchivedIntent {
                reference: "main".into(),
                bookmark: "main".into(),
                generation: 2,
                head: Some(hex(&head.key.id.0)),
                request_id: "13".repeat(16),
                base_operation: data.working_copy.wc_operation.clone(),
                confirmed_generation: None,
            }],
            landing: Some(c::PortableLanding {
                request_id: "12".repeat(16),
                source_base: "00".repeat(32),
                source_head: hex(&head.key.id.0),
                reference: "main".into(),
                bookmark: "main".into(),
                generation: 2,
                expected_head: hex(&head.key.id.0),
                profile_name: "checks".into(),
                profile_version: "v1".into(),
                source_operation: data.working_copy.wc_operation.clone(),
                source_stack: vec![hex(&head.key.id.0)],
                fork_operation: None,
                fork_acknowledged: false,
                job_id: None,
                result_roots: vec![],
                candidate_head: None,
                apply_operation: None,
                operation_writing: None,
                acknowledged: false,
                terminal: None,
                observed: None,
                restored_lookup_only: false,
            }),
        };
        let (first_identity, mut first) = retirement_request(&template, 1, None);
        retirement_remote_state(&mut first, old_remote.clone());
        store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                first_identity,
                &first,
                1,
                &Default::default(),
            )
            .unwrap();
        let mut new_remote = old_remote;
        new_remote.observations = case
            .observation
            .map(|(generation, selected)| c::RemoteObservation {
                reference: "main".into(),
                bookmark: "main".into(),
                generation,
                head: Some(hex(&match selected {
                    Head::Old => head.key.id.0,
                    Head::Advanced => advanced.key.id.0,
                })),
            })
            .into_iter()
            .collect();
        new_remote.archived_pushes[0].confirmed_generation = case.confirmed_push;
        new_remote.landing.as_mut().unwrap().profile_version = case.landing_profile.into();
        let (identity, mut second) =
            retirement_request(&template, 2, Some(first.publication.manifest));
        retirement_remote_state(&mut second, new_remote);
        // Primary publication verifies each manifest against its own DATA and
        // captured intent roots. Retirement never bypasses that verifier.
        store
            .publish_captured_cloud_version_v2(
                "demo",
                owner,
                identity,
                &second,
                1,
                &Default::default(),
            )
            .unwrap();
        let before = store.cloud_catalog_budget_summary().unwrap();
        let result = store.retire_cloud_versions(
            "demo",
            owner,
            CloudRetirementRequest {
                expected_current: retirement_selector(&second),
                expected_floor: 0,
                expected_revision: 0,
                retire_through: 1,
            },
        );
        if case.allowed {
            assert_eq!(result.unwrap().retired_through, 1, "{}", case.name);
            store.gc("demo", 100).unwrap();
            assert_eq!(
                store.descriptor("demo", head.key).unwrap(),
                Some(head),
                "old intent/source roots stay covered"
            );
        } else {
            assert!(
                matches!(
                    result,
                    Err(Error::Invalid(
                        "cloud retirement successor coverage incomplete"
                    ))
                ),
                "{}",
                case.name
            );
            assert_eq!(
                store.cloud_catalog_budget_summary().unwrap(),
                before,
                "{}",
                case.name
            );
        }
    }
}

#[test]
fn captured_versions_publish_atomically_replay_and_retain_the_exact_capsule_through_gc_pack_restart()
 {
    let directory = tempfile::tempdir().unwrap();
    let mut resident_policy = config();
    resident_policy.cloud.resident = Some(CloudResidentLimits {
        max_bytes: 2 * 1024 * 1024 * 1024,
        max_entries: 65_536,
    });
    let store = Store::open(directory.path(), resident_policy.clone()).unwrap();
    store.create_repository("demo").unwrap();
    assert!(store.supports_durable_retained_offload());
    let owner = CloudOwner {
        principal: [91; 32],
    };
    let (_, root, head) = fixture(&store, "captured anchor");
    let mut hidden = commit(TreeId(root.key.id.0), "captured hidden draft");
    hidden.parents = vec![CommitId(head.key.id.0)];
    let hidden = put_object(&store, Object::Commit(hidden), 0);
    let (history, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![cloud_index_draft(&store, hidden, 9)],
        },
    );
    let request = captured_cloud_request(
        &store,
        owner,
        history,
        leases,
        &artifacts,
        0,
        "captured-first",
    );
    assert!(
        store
            .publish_cloud_version("demo", owner, &request.publication, 1, &Default::default())
            .is_err(),
        "legacy API cannot acknowledge a history-only V3 publication"
    );
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, request.publication.workspace)
            .unwrap()
            .generation,
        0
    );
    assert!(
        store
            .cloud_publication("demo", owner, "captured-first")
            .unwrap()
            .is_none()
    );
    let mut malformed = PublishCapturedCloudRequest {
        publication: request.publication.clone(),
        capture_data: request.capture_data.clone(),
    };
    malformed.publication.idempotency_key = "captured-malformed".into();
    malformed.capture_data.push(b' ');
    assert!(
        store
            .publish_captured_cloud_version("demo", owner, &malformed, 1, &Default::default())
            .is_err()
    );
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, request.publication.workspace)
            .unwrap()
            .generation,
        0
    );
    for (name, policy, workspace) in [
        (
            "capture-unfenced-policy",
            tinytree_cloud::CapturePolicy::TreeFreezeCandidateV1,
            "task",
        ),
        (
            "capture-wc-mismatch",
            tinytree_cloud::CapturePolicy::HostQuiescentV1,
            "other",
        ),
    ] {
        let mut data = tinytree_cloud::decode_data(
            &request.capture_data,
            tinytree_cloud::CaptureDataId::of(&request.capture_data).0,
            &Default::default(),
        )
        .unwrap();
        data.policy = policy;
        data.working_copy.workspace = workspace.into();
        let capture_data = tinytree_cloud::encode_capture_data(&data, Default::default()).unwrap();
        let mut manifest = tinytree_cloud::decode_manifest_v3(
            &request.publication.manifest_bytes,
            request.publication.manifest,
            Default::default(),
        )
        .unwrap();
        manifest.capture = tinytree_cloud::CaptureDescriptor::of(&capture_data);
        let bytes = tinytree_cloud::encode_manifest_v3(&manifest, Default::default()).unwrap();
        let mut publication = request.publication.clone();
        publication.idempotency_key = name.into();
        publication.manifest = tinytree_cloud::ManifestId::of(&bytes);
        publication.manifest_bytes = bytes;
        let invalid = PublishCapturedCloudRequest {
            publication,
            capture_data,
        };
        assert!(
            store
                .publish_captured_cloud_version("demo", owner, &invalid, 1, &Default::default())
                .is_err(),
            "{name}"
        );
        assert!(
            store
                .cloud_publication("demo", owner, name)
                .unwrap()
                .is_none(),
            "{name}"
        );
        assert_eq!(
            store
                .get_cloud_workspace("demo", owner, request.publication.workspace)
                .unwrap()
                .generation,
            0,
            "{name}"
        );
    }
    let published = store
        .publish_captured_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
    assert_eq!(published.workspace.generation, 1);
    assert_eq!(
        store
            .publish_captured_cloud_version("demo", owner, &request, 2, &Default::default())
            .unwrap(),
        published
    );
    malformed.publication.idempotency_key = "captured-first".into();
    assert!(matches!(
        store.publish_captured_cloud_version("demo", owner, &malformed, 2, &Default::default()),
        Err(Error::IdempotencyConflict)
    ));
    malformed.capture_data.clear();
    assert!(matches!(
        store.publish_captured_cloud_version("demo", owner, &malformed, 2, &Default::default()),
        Err(Error::IdempotencyConflict)
    ));
    let descriptor = tinytree_cloud::CaptureDescriptor::of(&request.capture_data);
    let selector = CloudVersionSelector {
        workspace: request.publication.workspace,
        generation: 1,
        manifest: request.publication.manifest,
        selected: tinytree_cloud::BlobDigest::of(&request.publication.manifest_bytes),
    };
    assert!(
        store
            .read_cloud_version_v1("demo", owner, selector.workspace, 1, Default::default())
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .read_cloud_capture(
                "demo",
                CloudOwner {
                    principal: [92; 32]
                },
                selector,
                descriptor.id
            )
            .is_err()
    );
    assert!(
        store
            .read_cloud_capture(
                "demo",
                owner,
                selector,
                tinytree_cloud::CaptureDataId([0; 32])
            )
            .is_err()
    );
    let mut lease = store
        .read_cloud_capture("demo", owner, selector, descriptor.id)
        .unwrap();
    assert_eq!(lease.descriptor(), descriptor);
    let mut bytes = vec![0; 16];
    lease.read_exact(&mut bytes).unwrap();
    store.gc("demo", 100).unwrap();
    lease.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, request.capture_data);
    drop(lease);
    store.pack("demo", PackOptions::default()).unwrap();
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.meta, b"format")
            .unwrap()
            .unwrap()
            .as_ref(),
        b"tinytree-store-5"
    );
    let indexed = tinytree_cloud::decode_manifest_v3(
        &request.publication.manifest_bytes,
        request.publication.manifest,
        Default::default(),
    )
    .unwrap();
    let selected = indexed
        .history
        .operation_indexes
        .iter()
        .find(|entry| entry.operation != [0; 64])
        .unwrap()
        .root;
    let guard = store
        .open_cloud_snapshot("demo", owner, selector, &[selected], 4)
        .unwrap();
    assert_eq!(guard.frozen_commits()[0].descriptor, hidden);
    drop(guard);
    drop(store);
    let store = Store::open_existing(directory.path(), resident_policy.clone()).unwrap();
    assert!(
        store.supports_durable_retained_offload(),
        "owned backing contract survives reopen and GC/pack"
    );
    let mut lease = store
        .read_cloud_capture("demo", owner, selector, descriptor.id)
        .unwrap();
    let mut recovered = Vec::new();
    lease.read_to_end(&mut recovered).unwrap();
    assert_eq!(recovered, request.capture_data);
    drop(lease);
    assert_eq!(
        store
            .cloud_publication("demo", owner, "captured-first")
            .unwrap()
            .unwrap(),
        published
    );
}

#[test]
fn capture_size_boundary_accepts_a_large_canonical_operation_inventory_as_a_binary_row() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [95; 32],
    };
    let (_, root, head) = fixture(&store, "large capture inventory");
    // Ordered parent edges are preserved by the actual operation codec, including
    // repeated intrinsic parents. Their hex representation is legitimate capture
    // data, not padding in an otherwise small attachment.
    let recovery = cloud_capture_with_parent_edges(&store, owner, head, root, 0, 9_000);
    let (history, leases, artifacts) = cloud_v2_capture_from_recovery(
        &store,
        owner,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![],
        },
        recovery,
    );
    let request = captured_cloud_request(
        &store,
        owner,
        history,
        leases,
        &artifacts,
        0,
        "large-capture",
    );
    assert!(request.capture_data.len() > 1024 * 1024);
    let id = tinytree_cloud::CaptureDataId::of(&request.capture_data);
    let publication = store
        .publish_captured_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
    let selector = CloudVersionSelector {
        workspace: request.publication.workspace,
        generation: publication.workspace.generation,
        manifest: request.publication.manifest,
        selected: tinytree_cloud::BlobDigest::of(&request.publication.manifest_bytes),
    };
    let mut lease = store
        .read_cloud_capture("demo", owner, selector, id)
        .unwrap();
    let mut bytes = Vec::new();
    lease.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, request.capture_data);
    drop(lease);
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    let mut lease = store
        .read_cloud_capture("demo", owner, selector, id)
        .unwrap();
    bytes.clear();
    lease.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, request.capture_data);
}

#[test]
fn acknowledged_capture_selected_index_feature_is_atomic_and_enforced_on_reopen() {
    use tinytree_cloud as c;
    for (legacy_view, required_feature) in [(true, 9), (false, 10)] {
        let (directory, store) = open();
        let owner = CloudOwner {
            principal: [96; 32],
        };
        let (_, tree, source) = fixture(&store, "landing source");
        let mut candidate_commit = commit(TreeId(tree.key.id.0), "landed candidate");
        candidate_commit.parents = vec![CommitId(source.key.id.0)];
        let candidate = put_object(&store, Object::Commit(candidate_commit), 0);
        let attributes = [
            ("request", hex(&[12; 16])),
            ("phase", "apply".into()),
            ("source", hex(&source.key.id.0)),
            ("source_operation", hex(&[0; 64])),
            ("job", hex(&[23; 32])),
            ("candidate", hex(&candidate.key.id.0)),
        ]
        .into_iter()
        .map(|(key, value)| (format!("tinytree.landing.{key}"), value))
        .collect();
        let recovery = cloud_capture_with_attributes(
            &store,
            owner,
            if legacy_view { candidate } else { source },
            tree,
            0,
            1,
            attributes,
        );
        let (mut history, leases, artifacts) = cloud_v2_capture_from_recovery(
            &store,
            owner,
            c::PortableIndexSnapshot {
                roots: vec![CommitId(candidate.key.id.0)],
                drafts: vec![],
            },
            recovery,
        );
        history.recovery.native_roots.push(source.key);
        history
            .recovery
            .native_roots
            .sort_by_key(|key| (key.kind as u8, key.id.0));
        history.recovery.native_roots.dedup();
        let mut request = captured_cloud_request(
            &store,
            owner,
            history,
            leases,
            &artifacts,
            0,
            "acknowledged-capture",
        );
        let mut data = c::decode_data(
            &request.capture_data,
            c::data_digest(&request.capture_data),
            &Default::default(),
        )
        .unwrap();
        let rewrites = vec![c::HistoricalLandingRewrite {
            old_commit: source.key.id.0,
            new_commit: candidate.key.id.0,
            change_id: [26; 16],
        }];
        data.version = 2;
        data.remote = Some(c::PortableRemoteState {
            observations: vec![],
            archived_pushes: vec![],
            landing: Some(c::PortableLanding {
                request_id: hex(&[12; 16]),
                source_base: hex(&[0; 32]),
                source_head: hex(&source.key.id.0),
                reference: "main".into(),
                bookmark: "main".into(),
                generation: 1,
                expected_head: hex(&source.key.id.0),
                profile_name: "checks".into(),
                profile_version: "v1".into(),
                source_operation: hex(&[0; 64]),
                source_stack: vec![hex(&source.key.id.0)],
                fork_operation: Some(hex(&[0; 64])),
                fork_acknowledged: true,
                job_id: Some(hex(&[23; 32])),
                result_roots: vec![],
                candidate_head: None,
                apply_operation: Some(data.operation_heads[0].clone()),
                operation_writing: None,
                acknowledged: true,
                terminal: None,
                restored_lookup_only: false,
                observed: Some(c::HistoricalLandingJob {
                    job_id: [23; 32],
                    phase: c::HistoricalLandingPhase::Committed,
                    preparation: c::HistoricalLandingPreparation::ProfileConfigured,
                    roots_retained: false,
                    expires_at_seconds: 12345,
                    profile_name: "checks".into(),
                    profile_version: "v1".into(),
                    candidate: Some(c::HistoricalLandingCandidate {
                        head: candidate.key.id.0,
                        root_tree: tree.key.id.0,
                        rewrites: rewrites.clone(),
                    }),
                    receipt: Some(c::HistoricalLandingReceipt {
                        reference: "main".into(),
                        generation: 2,
                        commit: candidate.key.id.0,
                        commit_size_bytes: candidate.size_bytes,
                        object_format_version: tinytree_model::FORMAT_VERSION,
                        rewrites,
                        evidence: [27; 32],
                    }),
                    conflict_path: None,
                }),
            }),
        });
        request.capture_data = c::encode_capture_data(&data, Default::default()).unwrap();
        let mut manifest = c::decode_manifest_v3(
            &request.publication.manifest_bytes,
            request.publication.manifest,
            Default::default(),
        )
        .unwrap();
        manifest.capture = c::CaptureDescriptor::of(&request.capture_data);
        manifest.history.recovery.intent_roots =
            c::capture_recovery_intents(&data, manifest.capture.id, Default::default()).unwrap();
        request.publication.manifest_bytes =
            c::encode_manifest_v3(&manifest, Default::default()).unwrap();
        request.publication.manifest = c::ManifestId::of(&request.publication.manifest_bytes);
        let mut quota = CloudPublicationLimits::default();
        quota.retention.max_unique_bytes_per_principal = 1;
        assert!(
            store
                .publish_captured_cloud_version("demo", owner, &request, 1, &quota)
                .is_err()
        );
        assert!(
            store_feature(
                store
                    .inner
                    .db
                    .read_tx()
                    .get(&store.inner.spaces.meta, b"format")
                    .unwrap()
                    .as_deref()
            )
            .unwrap()
                < 9
        );
        let receipt = store
            .publish_captured_cloud_version("demo", owner, &request, 1, &Default::default())
            .unwrap();
        assert_eq!(
            store_feature(
                store
                    .inner
                    .db
                    .read_tx()
                    .get(&store.inner.spaces.meta, b"format")
                    .unwrap()
                    .as_deref()
            )
            .unwrap(),
            required_feature
        );
        drop(store);
        let reopened = Store::open_existing(directory.path(), config()).unwrap();
        assert_eq!(
            reopened
                .cloud_publication("demo", owner, "acknowledged-capture")
                .unwrap(),
            Some(receipt)
        );
        drop(reopened);
        {
            let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
                .open()
                .unwrap();
            let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
            let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
            tx.insert(
                &meta,
                b"format",
                if legacy_view {
                    b"tinytree-store-8".as_slice()
                } else {
                    b"tinytree-store-9".as_slice()
                },
            );
            tx.commit().unwrap();
        }
        assert!(matches!(
            Store::open_existing(directory.path(), config()),
            Err(Error::Corrupt("landing capture without store feature"))
        ));
    }
}

#[test]
fn landing_capture_activates_its_atomic_feature_and_rejects_downgraded_reopen() {
    use tinytree_cloud as c;
    for (stale_before_acceptance, required_format) in [
        (false, b"tinytree-store-9".as_slice()),
        (true, b"tinytree-store-10".as_slice()),
    ] {
        let (directory, store) = open();
        let owner = CloudOwner {
            principal: [95; 32],
        };
        let (_, tree, head) = fixture(&store, "portable landing source");
        let (history, leases, artifacts) = cloud_v2_capture(
            &store,
            owner,
            head,
            tree,
            c::PortableIndexSnapshot {
                roots: vec![CommitId(head.key.id.0)],
                drafts: vec![],
            },
        );
        let mut request = captured_cloud_request(
            &store,
            owner,
            history,
            leases,
            &artifacts,
            0,
            "landing-capture",
        );
        let mut data = c::decode_data(
            &request.capture_data,
            c::data_digest(&request.capture_data),
            &Default::default(),
        )
        .unwrap();
        data.version = 2;
        data.remote = Some(c::PortableRemoteState {
            observations: vec![],
            archived_pushes: vec![],
            landing: Some(c::PortableLanding {
                request_id: "12".repeat(16),
                source_base: "00".repeat(32),
                source_head: hex(&head.key.id.0),
                reference: "main".into(),
                bookmark: "main".into(),
                generation: 1,
                expected_head: hex(&head.key.id.0),
                profile_name: "checks".into(),
                profile_version: "v1".into(),
                source_operation: data.working_copy.wc_operation.clone(),
                source_stack: vec![hex(&head.key.id.0)],
                fork_operation: None,
                fork_acknowledged: false,
                job_id: None,
                result_roots: vec![],
                candidate_head: None,
                apply_operation: None,
                operation_writing: None,
                acknowledged: false,
                terminal: None,
                observed: None,
                restored_lookup_only: false,
            }),
        });
        request.capture_data = c::encode_capture_data(&data, Default::default()).unwrap();
        let mut manifest = c::decode_manifest_v3(
            &request.publication.manifest_bytes,
            request.publication.manifest,
            Default::default(),
        )
        .unwrap();
        manifest.capture = c::CaptureDescriptor::of(&request.capture_data);
        manifest.history.recovery.intent_roots =
            c::capture_recovery_intents(&data, manifest.capture.id, Default::default()).unwrap();
        request.publication.manifest_bytes =
            c::encode_manifest_v3(&manifest, Default::default()).unwrap();
        request.publication.manifest = c::ManifestId::of(&request.publication.manifest_bytes);
        if stale_before_acceptance {
            let landing = data.remote.as_mut().unwrap().landing.as_mut().unwrap();
            landing.fork_operation = Some(landing.source_operation.clone());
            landing.fork_acknowledged = true;
            landing.job_id = Some("17".repeat(32));
            landing.terminal = Some("stale_target".into());
            landing.observed = Some(c::HistoricalLandingJob {
                job_id: [23; 32],
                phase: c::HistoricalLandingPhase::StaleTarget,
                preparation: c::HistoricalLandingPreparation::ProfileConfigured,
                roots_retained: false,
                expires_at_seconds: 12345,
                profile_name: "checks".into(),
                profile_version: "v1".into(),
                candidate: None,
                receipt: None,
                conflict_path: None,
            });
            request.capture_data = c::encode_capture_data(&data, Default::default()).unwrap();
            manifest.capture = c::CaptureDescriptor::of(&request.capture_data);
            manifest.history.recovery.intent_roots =
                c::capture_recovery_intents(&data, manifest.capture.id, Default::default())
                    .unwrap();
            request.publication.manifest_bytes =
                c::encode_manifest_v3(&manifest, Default::default()).unwrap();
            request.publication.manifest = c::ManifestId::of(&request.publication.manifest_bytes);
        }
        let mut failed = request.publication.clone();
        failed.expected_generation = 1;
        assert!(
            store
                .publish_captured_cloud_version(
                    "demo",
                    owner,
                    &PublishCapturedCloudRequest {
                        publication: failed,
                        capture_data: request.capture_data.clone()
                    },
                    1,
                    &Default::default()
                )
                .is_err()
        );
        assert!(
            store_feature(
                store
                    .inner
                    .db
                    .read_tx()
                    .get(&store.inner.spaces.meta, b"format")
                    .unwrap()
                    .as_deref()
            )
            .unwrap()
                < 9
        );
        assert!(
            store
                .inner
                .db
                .read_tx()
                .iter(&store.inner.spaces.landing)
                .next()
                .is_none()
        );
        // Feature-nine validation retains its arena across the layout fence.
        // A quota refusal needs only the current catalog plus pin slack, not a
        // second independently admitted arena for external-check activation.
        drop(store);
        let mut resident_policy = config();
        resident_policy.cloud.resident = Some(CloudResidentLimits {
            max_bytes: 2 * 1024 * 1024 * 1024,
            max_entries: 65_536,
        });
        let arena = cloud::resident::preflight(directory.path(), &resident_policy)
            .unwrap()
            .charge()
            .unwrap();
        assert!(arena.bytes > 65536);
        resident_policy.cloud.resident.as_mut().unwrap().max_bytes = arena.bytes + 65536;
        let store = Store::open_existing(directory.path(), resident_policy).unwrap();
        let mut quota = CloudPublicationLimits::default();
        quota.retention.max_unique_bytes_per_principal = 1;
        assert!(matches!(
            store.publish_captured_cloud_version("demo", owner, &request, 1, &quota),
            Err(Error::Limit("principal promised closure byte quota"))
        ));
        assert!(
            store_feature(
                store
                    .inner
                    .db
                    .read_tx()
                    .get(&store.inner.spaces.meta, b"format")
                    .unwrap()
                    .as_deref()
            )
            .unwrap()
                < 9,
            "failed quota after layout setup cannot advance the capsule feature"
        );
        assert!(
            store
                .cloud_publication("demo", owner, "landing-capture")
                .unwrap()
                .is_none()
        );
        drop(store);
        let store = Store::open_existing(directory.path(), config()).unwrap();
        let publication = store
            .publish_captured_cloud_version("demo", owner, &request, 1, &Default::default())
            .unwrap();
        assert_eq!(publication.workspace.generation, 1);
        assert!(
            store
                .inner
                .db
                .read_tx()
                .iter(&store.inner.spaces.landing)
                .next()
                .is_none(),
            "historical capsule observations never create local landing jobs"
        );
        assert_eq!(
            store.references("demo", &["main".into()]).unwrap()[0].generation,
            0
        );
        assert_eq!(
            store
                .inner
                .db
                .read_tx()
                .get(&store.inner.spaces.meta, b"format")
                .unwrap()
                .unwrap()
                .as_ref(),
            required_format
        );
        assert!(
            store.external_checks_directory().unwrap().is_some(),
            "higher feature preserves prior durable layout fence without dispatching checks"
        );
        drop(store);
        drop(Store::open_existing(directory.path(), config()).unwrap());
        let set_format = |value: &[u8]| {
            let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
                .open()
                .unwrap();
            let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
            let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
            tx.insert(&meta, b"format", value);
            tx.commit().unwrap();
        };
        set_format(if stale_before_acceptance {
            b"tinytree-store-9"
        } else {
            b"tinytree-store-8"
        });
        assert!(matches!(
            Store::open_existing(directory.path(), config()),
            Err(Error::Corrupt("landing capture without store feature"))
        ));
        set_format(required_format);
        let reopened = Store::open_existing(directory.path(), config()).unwrap();
        assert_eq!(
            reopened
                .cloud_publication("demo", owner, "landing-capture")
                .unwrap()
                .unwrap(),
            publication
        );
    }
}

#[test]
fn capture_binding_uses_remaining_version_budget_without_changing_history_only_admission() {
    let directory = tempfile::tempdir().unwrap();
    let small = small_index_cloud_config();
    let store = Store::open(directory.path(), small).unwrap();
    store.create_repository("demo").unwrap();
    let owner = CloudOwner {
        principal: [93; 32],
    };
    let (_, root, head) = fixture(&store, "small capture budget");
    let (mut history, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![],
        },
    );
    let ordinary = cloud_v2_request(&history, 0, "history-without-capsule", leases);
    store
        .publish_cloud_version("demo", owner, &ordinary, 1, &Default::default())
        .unwrap();
    history.recovery.sequence = 2;
    let captured = captured_cloud_request(
        &store,
        owner,
        history,
        vec![],
        &artifacts,
        1,
        "remaining-capture-budget",
    );
    assert!(matches!(
        store.publish_captured_cloud_version("demo", owner, &captured, 2, &Default::default()),
        Err(Error::Limit(_))
    ));
    assert_eq!(
        store
            .get_cloud_workspace("demo", owner, captured.publication.workspace)
            .unwrap()
            .generation,
        1
    );
    assert!(
        store
            .cloud_publication("demo", owner, "remaining-capture-budget")
            .unwrap()
            .is_none()
    );
    let captures = store
        .inner
        .db
        .keyspace("cloud-captures-v1", KeyspaceCreateOptions::default)
        .unwrap();
    assert!(store.inner.db.read_tx().iter(&captures).next().is_none());
}

#[test]
fn captured_version_missing_orphan_corrupt_and_downgraded_rows_fail_closed_on_reopen() {
    let (directory, store) = open();
    let owner = CloudOwner {
        principal: [94; 32],
    };
    let (_, root, head) = fixture(&store, "capture integrity");
    let (history, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![],
        },
    );
    let request = captured_cloud_request(
        &store,
        owner,
        history,
        leases,
        &artifacts,
        0,
        "capture-integrity",
    );
    store
        .publish_captured_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
    let space = store
        .inner
        .db
        .keyspace("cloud-captures-v1", KeyspaceCreateOptions::default)
        .unwrap();
    let (key, row) = store
        .inner
        .db
        .read_tx()
        .iter(&space)
        .next()
        .unwrap()
        .into_inner()
        .unwrap();
    let key = key.to_vec();
    let row = row.to_vec();
    let mut tx = store
        .inner
        .db
        .write_tx()
        .durability(Some(PersistMode::SyncAll));
    tx.remove(&space, &key);
    tx.commit().unwrap();
    let selector = CloudVersionSelector {
        workspace: request.publication.workspace,
        generation: 1,
        manifest: request.publication.manifest,
        selected: tinytree_cloud::BlobDigest::of(&request.publication.manifest_bytes),
    };
    assert!(matches!(
        store.read_cloud_capture(
            "demo",
            owner,
            selector,
            tinytree_cloud::CaptureDataId::of(&request.capture_data)
        ),
        Err(Error::Corrupt("cloud version capture missing"))
    ));
    drop(space);
    drop(store);
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("cloud version capture missing"))
    ));
    fn repair(path: &Path, key: &[u8], value: Option<&[u8]>, feature: Option<&[u8]>) {
        let db = SingleWriterTxDatabase::builder(path.join("metadata"))
            .open()
            .unwrap();
        let captures = db
            .keyspace("cloud-captures-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        match value {
            Some(value) => tx.insert(&captures, key, value),
            None => tx.remove(&captures, key),
        }
        if let Some(feature) = feature {
            tx.insert(&meta, b"format", feature);
        }
        tx.commit().unwrap();
    }
    repair(directory.path(), &key, Some(&row), None);
    drop(Store::open_existing(directory.path(), config()).unwrap());
    let mut orphan = key.clone();
    *orphan.last_mut().unwrap() ^= 0x80;
    repair(directory.path(), &orphan, Some(&row), None);
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt(
            "unreferenced cloud capture or missing feature"
        ))
    ));
    repair(directory.path(), &orphan, None, None);
    let mut corrupt = row.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    repair(directory.path(), &key, Some(&corrupt), None);
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("cloud capture record identity"))
    ));
    repair(
        directory.path(),
        &key,
        Some(&row),
        Some(b"tinytree-store-4"),
    );
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt(
            "unreferenced cloud capture or missing feature"
        ))
    ));
    repair(
        directory.path(),
        &key,
        Some(&row),
        Some(b"tinytree-store-5"),
    );
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let captures = db
            .keyspace("cloud-captures-v1", KeyspaceCreateOptions::default)
            .unwrap();
        db.inner()
            .delete_keyspace(captures.inner().clone())
            .unwrap();
    }
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("cloud capture keyspace missing"))
    ));
}

// Landing fixtures use the real Store API and strict canonical closures. They do
// not stand in for a trusted check runner or server/CLI landing acceptance.
fn landing_commit(
    store: &Store,
    repo: &str,
    parent: CommitId,
    root: TreeId,
    change: u8,
    description: &str,
) -> Descriptor {
    let mut object = commit(root, description);
    object.parents = vec![parent];
    object.change_id = ChangeId([change; 16]);
    let encoded = tinytree_model::encode(&Object::Commit(object), Limits::default()).unwrap();
    store
        .put(repo, encoded.descriptor, &encoded.bytes, 0)
        .unwrap();
    encoded.descriptor
}
fn landing_fixture(store: &Store, repo: &str) -> (Descriptor, Descriptor, Descriptor) {
    let empty = tinytree_model::empty_tree();
    let base = landing_commit(
        store,
        repo,
        CommitId::ROOT,
        TreeId(empty.descriptor.key.id.0),
        1,
        "base",
    );
    let make_tree = |name: &str, bytes: &[u8]| {
        let file = tinytree_model::describe_file(bytes);
        store.put(repo, file, bytes, 0).unwrap();
        let encoded = tinytree_model::encode(
            &Object::Tree(Tree {
                entries: vec![TreeEntry {
                    name: name.into(),
                    value: TreeValue::File {
                        id: FileId(file.key.id.0),
                        executable: false,
                        size_bytes: file.size_bytes,
                        copy_id: None,
                    },
                }],
            }),
            Limits::default(),
        )
        .unwrap();
        store
            .put(repo, encoded.descriptor, &encoded.bytes, 0)
            .unwrap();
        TreeId(encoded.descriptor.key.id.0)
    };
    let source = landing_commit(
        store,
        repo,
        CommitId(base.key.id.0),
        make_tree("source.txt", b"source"),
        2,
        "source change",
    );
    let dest = landing_commit(
        store,
        repo,
        CommitId(base.key.id.0),
        make_tree("destination.txt", b"destination"),
        3,
        "destination change",
    );
    store
        .publish(repo, &request("landing-initial", 0, Some(dest)), 0)
        .unwrap();
    (base, source, dest)
}
fn landing_request(
    base: Descriptor,
    source: Descriptor,
    dest: Descriptor,
    token: &str,
) -> LandingRequest {
    LandingRequest {
        idempotency_key: token.into(),
        source_base: CommitId(base.key.id.0),
        source_head: CommitId(source.key.id.0),
        reference_name: "Main/feature".into(),
        expected_generation: 1,
        expected_head: CommitId(dest.key.id.0),
        committer: Signature {
            name: "Landing service".into(),
            email: "landing@example.invalid".into(),
            timestamp: Timestamp {
                millis_since_epoch: 42,
                tz_offset_minutes: 0,
            },
        },
        check_profile: LandingCheckProfile {
            name: "checks".into(),
            version: "v1".into(),
        },
        expires_at: 100,
    }
}
fn landing_identity(job: &LandingJob) -> LandingCheckIdentity {
    let candidate = job.candidate.as_ref().unwrap();
    LandingCheckIdentity {
        candidate: candidate.head,
        root_tree: candidate.root_tree,
        profile: job.profile.clone(),
    }
}

fn forge_review_fixture() -> (
    tempfile::TempDir,
    Store,
    AccountId,
    ForgeActor,
    ForgeActor,
    ViewVersion,
    CommitId,
) {
    let (directory, store) = open();
    let account = AccountId("review-team".into());
    let owner = ForgeActor {
        principal: "writer".into(),
    };
    let reviewer = ForgeActor {
        principal: "reviewer".into(),
    };
    store.forge_provision_account(&account, &owner).unwrap();
    store
        .forge_set_member(
            &account,
            &owner,
            ForgeMember {
                principal: reviewer.principal.clone(),
                can_write: false,
                can_review: true,
                is_admin: false,
                roles: vec![],
            },
            PolicyEpoch(1),
        )
        .unwrap();
    let base = CommitId(
        store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .commit
            .unwrap()
            .key
            .id
            .0,
    );
    let view = store
        .forge_publish_view(
            &account,
            &owner,
            ViewSpec {
                name: "all".into(),
                root_prefix: "".into(),
                include_prefixes: vec!["".into()],
                exclude_prefixes: vec![],
                visibility: ForgeViewVisibility::Members,
                revision: base.0,
                export_allowed: true,
            },
            PolicyEpoch(2),
            false,
        )
        .unwrap();
    let projected = store.forge_project_view(&view, 0).unwrap();
    (
        directory,
        store,
        account,
        owner,
        reviewer,
        view.version(),
        CommitId(projected.projected_commit),
    )
}
fn forge_review_source(
    store: &Store,
    account: &AccountId,
    parent: CommitId,
    paths: &[(&str, &[u8])],
    change: u8,
) -> CommitId {
    let repo = account.repository_id();
    let mut entries = Vec::new();
    for (path, content) in paths {
        let file = tinytree_model::describe_file(content);
        let actor = ForgeActor {
            principal: "writer".into(),
        };
        let view = store
            .forge_resolve_view(account, Some(&actor), "all", None)
            .unwrap();
        store
            .forge_upload_file(&view, &actor, path, content, 0)
            .unwrap();
        entries.push(TreeEntry {
            name: (*path).into(),
            value: TreeValue::File {
                id: FileId(file.key.id.0),
                executable: false,
                size_bytes: file.size_bytes,
                copy_id: None,
            },
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let encoded =
        tinytree_model::encode(&Object::Tree(Tree { entries }), Limits::default()).unwrap();
    let actor = ForgeActor {
        principal: "writer".into(),
    };
    let view = store
        .forge_resolve_view(account, Some(&actor), "all", None)
        .unwrap();
    store
        .forge_upload_object(&view, &actor, "", encoded.descriptor, &encoded.bytes, 0)
        .unwrap();
    let descriptor = landing_commit(
        store,
        &repo,
        parent,
        TreeId(encoded.descriptor.key.id.0),
        change,
        "review change",
    );
    let (_, bytes) = store.read_metadata(&repo, descriptor.key).unwrap();
    store
        .forge_upload_object(&view, &actor, "", descriptor, &bytes, 0)
        .unwrap();
    CommitId(descriptor.key.id.0)
}
fn forge_review_request(
    base: CommitId,
    head: CommitId,
    view: ViewVersion,
    token: &str,
) -> ForgeReviewPushRequest {
    ForgeReviewPushRequest {
        request_id: token.into(),
        source_base: base,
        source_head: head,
        view_version: view.0,
        expected_patchset: None,
        predecessor: None,
        provenance: None,
        lineage: vec![],
        check_profile: LandingCheckProfile {
            name: "checks".into(),
            version: "v1".into(),
        },
    }
}
fn forge_review_signature() -> Signature {
    Signature {
        name: "Review service".into(),
        email: "review@example.invalid".into(),
        timestamp: Timestamp {
            millis_since_epoch: 42,
            tz_offset_minutes: 0,
        },
    }
}
// This helper is the trusted Store embedding, not a client-supplied verdict RPC.
fn forge_review_check(store: &Store, account: &AccountId, review: &ForgeReview) -> LandingHandle {
    let candidate = review.candidate.as_ref().unwrap();
    let handle = store
        .recover_landing_handle(&account.repository_id(), candidate.id)
        .unwrap()
        .unwrap();
    let job = handle.status(store).unwrap();
    handle
        .record_check(
            store,
            &landing_identity(&job),
            LandingVerdict {
                passed: true,
                evidence: [7; 32],
            },
            2,
        )
        .unwrap();
    handle
}

#[test]
fn forge_review_push_is_immutable_replayable_and_checks_never_bypass_explicit_review_land() {
    let (_directory, store, account, writer, reviewer, view, _base) = forge_review_fixture();
    let public_base = store
        .references(&account.repository_id(), &["main".into()])
        .unwrap()[0]
        .commit
        .unwrap()
        .key
        .id
        .0;
    store
        .forge_mutate_account(
            &account,
            &writer,
            "follow-public",
            ForgeAccountMutation::PublishView {
                spec: ViewSpec {
                    name: "public".into(),
                    root_prefix: "".into(),
                    include_prefixes: vec!["".into()],
                    exclude_prefixes: vec![],
                    visibility: ForgeViewVisibility::Public,
                    revision: public_base,
                    export_allowed: true,
                },
                expected_epoch: PolicyEpoch(3),
                make_default: true,
                follow_main: true,
            },
        )
        .unwrap();
    let old_public = store.forge_resolve_view(&account, None, "", None).unwrap();
    let selected = store
        .forge_resolve_view(&account, Some(&writer), "all", Some(view))
        .unwrap();
    let base = CommitId(
        store
            .forge_project_view(&selected, 0)
            .unwrap()
            .projected_commit,
    );

    let head = forge_review_source(&store, &account, base, &[("source.txt", b"source")], 2);
    let request = forge_review_request(base, head, view, "push-one");
    let pushed = store
        .forge_push_review(&account, &writer, &request, &forge_review_signature(), 1)
        .unwrap();
    let candidate = pushed.candidate.as_ref().unwrap();
    assert_eq!(pushed.source_head, head.0);
    assert_eq!(
        store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .generation,
        1
    );
    assert_eq!(
        store
            .forge_push_review(&account, &writer, &request, &forge_review_signature(), 2)
            .unwrap()
            .patchset_id,
        pushed.patchset_id
    );
    let mut changed = request.clone();
    changed.provenance = Some([9; 32]);
    assert!(matches!(
        store.forge_push_review(&account, &writer, &changed, &forge_review_signature(), 2),
        Err(Error::IdempotencyConflict)
    ));

    let comment = store
        .forge_comment_review(
            &account,
            &reviewer,
            ChangeId(pushed.change_id),
            pushed.patchset_id,
            "comment-one",
            "Review note",
            Some("source.txt"),
            2,
        )
        .unwrap();
    assert_eq!(comment.comments.len(), 1);
    assert_eq!(
        store
            .forge_comment_review(
                &account,
                &reviewer,
                ChangeId(pushed.change_id),
                pushed.patchset_id,
                "comment-one",
                "Review note",
                Some("source.txt"),
                2
            )
            .unwrap()
            .comments
            .len(),
        1
    );
    assert!(matches!(
        store.forge_comment_review(
            &account,
            &reviewer,
            ChangeId(pushed.change_id),
            pushed.patchset_id,
            "comment-one",
            "Changed note",
            Some("source.txt"),
            2
        ),
        Err(Error::IdempotencyConflict)
    ));
    let handle = forge_review_check(&store, &account, &pushed);
    assert!(
        store
            .forge_review_manages_landing(&account.repository_id(), candidate.id)
            .unwrap()
    );
    assert!(
        handle.finalize(&store, 3).is_err(),
        "passing checks alone cannot publish"
    );
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(pushed.change_id),
            pushed.patchset_id,
            pushed.candidate.as_ref().unwrap().id,
            "approve-one",
            3,
        )
        .unwrap();
    assert!(
        handle.finalize(&store, 3).is_err(),
        "approval alone is not an explicit land request"
    );
    assert!(matches!(
        store.publish(
            &account.repository_id(),
            &PublishRequest {
                idempotency_key: "bypass".into(),
                reference_name: "main".into(),
                expected_generation: 1,
                new_commit: Some(head),
                staging_leases: vec![]
            },
            3
        ),
        Err(Error::ForgeAccessDenied)
    ));
    let job = handle.status(&store).unwrap();
    let raw = LandingRequest {
        idempotency_key: "raw-bypass".into(),
        source_base: base,
        source_head: head,
        reference_name: "main".into(),
        expected_generation: 1,
        expected_head: base,
        committer: forge_review_signature(),
        check_profile: job.profile,
        expires_at: 100,
    };
    assert!(matches!(
        store.begin_landing(&account.repository_id(), LandingOwner([1; 32]), &raw, 3),
        Err(Error::ForgeAccessDenied)
    ));
    let landed = store
        .forge_land_review(
            &account,
            &writer,
            ChangeId(pushed.change_id),
            pushed.patchset_id,
            candidate.id,
            4,
        )
        .unwrap();
    assert_eq!(landed.state, ForgeReviewState::Landed);
    assert_eq!(
        store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .commit
            .unwrap()
            .key
            .id
            .0,
        candidate.head
    );
    let current = store.forge_resolve_view(&account, None, "", None).unwrap();
    assert_eq!(current.revision().0, candidate.head);
    assert_ne!(current.version(), old_public.version());
    assert_eq!(
        current.epoch(),
        old_public.epoch(),
        "following main is not an ACL change"
    );
    let historical = store
        .forge_resolve_view(&account, None, "public", Some(old_public.version()))
        .unwrap();
    assert_eq!(historical.revision().0, public_base);
    assert!(store.forge_view_files(&old_public, 100).unwrap().is_empty());
}

#[test]
fn forge_review_refresh_rebases_on_advanced_main_and_conflicts_preserve_original_patchsets() {
    let (_directory, store, account, writer, reviewer, view, base) = forge_review_fixture();
    let original = forge_review_source(&store, &account, base, &[("same.txt", b"original")], 2);
    let first = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, original, view, "original"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    let other = forge_review_source(&store, &account, base, &[("same.txt", b"other")], 3);
    let second = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, other, view, "other"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    forge_review_check(&store, &account, &second);
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(second.change_id),
            second.patchset_id,
            second.candidate.as_ref().unwrap().id,
            "approve-other",
            2,
        )
        .unwrap();
    store
        .forge_land_review(
            &account,
            &writer,
            ChangeId(second.change_id),
            second.patchset_id,
            second.candidate.unwrap().id,
            3,
        )
        .unwrap();
    let conflict = store
        .forge_prepare_review(
            &account,
            &writer,
            ChangeId(first.change_id),
            first.patchset_id,
            4,
        )
        .unwrap();
    assert_eq!(conflict.state, ForgeReviewState::Conflict);
    assert_eq!(conflict.source_head, original.0);
    assert!(conflict.candidate.is_none());
    assert_eq!(conflict.conflict_path.as_deref(), Some("same.txt"));
    assert_eq!(
        store
            .forge_review_operation(&account, &writer, "original")
            .unwrap()
            .unwrap()
            .patchset_id,
        first.patchset_id
    );
}

#[test]
fn forge_review_pinned_predecessor_uses_exact_server_candidate_and_lands_approved_stack() {
    let (_directory, store, account, writer, reviewer, view, base) = forge_review_fixture();
    let parent_head = forge_review_source(&store, &account, base, &[("a", b"parent")], 2);
    let parent = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, parent_head, view, "parent"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    let child_head = forge_review_source(
        &store,
        &account,
        parent_head,
        &[("a", b"parent"), ("b", b"child")],
        3,
    );
    let mut request = forge_review_request(parent_head, child_head, view, "child");
    request.predecessor = Some(ForgeReviewDependency {
        change_id: parent.change_id,
        patchset_id: parent.patchset_id,
    });
    let child = store
        .forge_push_review(&account, &writer, &request, &forge_review_signature(), 1)
        .unwrap();
    let child_candidate = child.candidate.as_ref().unwrap();
    let (_, bytes) = store
        .read_metadata(
            &account.repository_id(),
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(child_candidate.head),
            },
        )
        .unwrap();
    let Object::Commit(commit) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else {
        panic!()
    };
    assert_eq!(
        commit.parents,
        [CommitId(parent.candidate.as_ref().unwrap().head)]
    );
    forge_review_check(&store, &account, &parent);
    forge_review_check(&store, &account, &child);
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(child.change_id),
            child.patchset_id,
            child.candidate.as_ref().unwrap().id,
            "child-approval",
            2,
        )
        .unwrap();
    assert!(
        store
            .forge_land_review(
                &account,
                &writer,
                ChangeId(child.change_id),
                child.patchset_id,
                child_candidate.id,
                3
            )
            .is_err(),
        "unapproved predecessor cannot land as a side effect"
    );
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(parent.change_id),
            parent.patchset_id,
            parent.candidate.as_ref().unwrap().id,
            "parent-approval",
            3,
        )
        .unwrap();
    let landed = store
        .forge_land_review(
            &account,
            &writer,
            ChangeId(child.change_id),
            child.patchset_id,
            child_candidate.id,
            4,
        )
        .unwrap();
    assert_eq!(landed.state, ForgeReviewState::Landed);
    assert_eq!(
        store
            .forge_review(&account, &writer, ChangeId(parent.change_id))
            .unwrap()
            .state,
        ForgeReviewState::Landed
    );
}

#[test]
fn forge_review_restart_retains_original_and_candidate_after_attempt_expiry_and_gc() {
    let (directory, store, account, writer, _reviewer, view, base) = forge_review_fixture();
    let head = forge_review_source(&store, &account, base, &[("durable.txt", b"durable")], 2);
    let pushed = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, head, view, "durable"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    let candidate = pushed.candidate.as_ref().unwrap();
    let handle = store
        .recover_landing_handle(&account.repository_id(), candidate.id)
        .unwrap()
        .unwrap();

    for index in 0..6 {
        store
            .forge_comment_review(
                &account,
                &writer,
                ChangeId(pushed.change_id),
                pushed.patchset_id,
                &format!("durable-comment-{index}"),
                &"review detail".repeat(300),
                Some("durable.txt"),
                2,
            )
            .unwrap();
    }
    assert!(
        store
            .forge_review(&account, &writer, ChangeId(pushed.change_id))
            .unwrap()
            .comments
            .iter()
            .map(|c| c.body.len())
            .sum::<usize>()
            > 16 * 1024
    );
    store
        .forge_set_member(
            &account,
            &writer,
            ForgeMember {
                principal: "observer".into(),
                can_write: false,
                can_review: false,
                is_admin: false,
                roles: vec![],
            },
            PolicyEpoch(3),
        )
        .unwrap();
    handle.expire(&store, 100_000).unwrap();
    store.gc(&account.repository_id(), 100_001).unwrap();
    assert!(
        store
            .descriptor(
                &account.repository_id(),
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(head.0)
                }
            )
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .descriptor(
                &account.repository_id(),
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(candidate.head)
                }
            )
            .unwrap()
            .is_some()
    );
    drop(handle);
    drop(store);
    let reopened = Store::open_existing(directory.path(), config()).unwrap();
    let readback = reopened
        .forge_review_operation(&account, &writer, "durable")
        .unwrap()
        .unwrap();
    assert_eq!(readback.patchset_id, pushed.patchset_id);
    assert_eq!(readback.source_head, head.0);
    assert_eq!(readback.candidate.unwrap().head, candidate.head);
}

#[test]
fn forge_review_projected_base_and_owned_upload_rebase_without_exposing_canonical_history() {
    let (_directory, store, account, writer, _reviewer, view, _base) = forge_review_fixture();
    let selected = store
        .forge_resolve_view(&account, Some(&writer), "all", Some(view))
        .unwrap();
    let projection = store.forge_project_view(&selected, 0).unwrap();
    let projected = CommitId(projection.projected_commit);
    let head = forge_review_source(
        &store,
        &account,
        projected,
        &[("visible.txt", b"visible")],
        5,
    );
    let review = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(projected, head, view, "projected"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    assert!(review.candidate.is_some());
    assert_eq!(review.source_base, projection.projected_commit);
    assert_ne!(
        review.candidate.unwrap().target,
        projection.projected_commit
    );
}

#[test]
fn forge_review_digest_knowledge_without_owned_upload_is_not_write_authority() {
    let (_directory, store, account, writer, _reviewer, view, base) = forge_review_fixture();
    let repo = account.repository_id();
    let file = tinytree_model::describe_file(b"not-supplied-by-view-writer");
    store
        .put(&repo, file, b"not-supplied-by-view-writer", 0)
        .unwrap();
    let tree = tinytree_model::encode(
        &Object::Tree(Tree {
            entries: vec![TreeEntry {
                name: "guessed.txt".into(),
                value: TreeValue::File {
                    id: FileId(file.key.id.0),
                    executable: false,
                    size_bytes: file.size_bytes,
                    copy_id: None,
                },
            }],
        }),
        Limits::default(),
    )
    .unwrap();
    store.put(&repo, tree.descriptor, &tree.bytes, 0).unwrap();
    let head = CommitId(
        landing_commit(
            &store,
            &repo,
            base,
            TreeId(tree.descriptor.key.id.0),
            5,
            "guessed reference",
        )
        .key
        .id
        .0,
    );
    assert!(matches!(
        store.forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, head, view, "guessed"),
            &forge_review_signature(),
            1
        ),
        Err(Error::ForgeAccessDenied)
    ));
    assert!(
        store
            .forge_review_operation(&account, &writer, "guessed")
            .unwrap()
            .is_none()
    );
}

#[test]
fn forge_review_revoked_policy_epoch_cannot_land_previously_checked_and_approved_candidate() {
    let (_directory, store, account, writer, reviewer, view, base) = forge_review_fixture();
    let head = forge_review_source(&store, &account, base, &[("visible.txt", b"visible")], 5);
    let review = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, head, view, "epoch"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    forge_review_check(&store, &account, &review);
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(review.change_id),
            review.patchset_id,
            review.candidate.as_ref().unwrap().id,
            "epoch-approve",
            2,
        )
        .unwrap();
    store
        .forge_set_member(
            &account,
            &writer,
            ForgeMember {
                principal: reviewer.principal.clone(),
                can_write: false,
                can_review: false,
                is_admin: false,
                roles: vec![],
            },
            PolicyEpoch(3),
        )
        .unwrap();
    assert!(matches!(
        store.forge_land_review(
            &account,
            &writer,
            ChangeId(review.change_id),
            review.patchset_id,
            review.candidate.unwrap().id,
            3
        ),
        Err(Error::StaleForgePolicy)
    ));
    assert_eq!(
        store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .generation,
        1
    );
}

#[test]
fn forge_review_explicit_native_rename_and_copy_keep_scoped_provenance_through_rebase() {
    for rename in [true, false] {
        let (_directory, store, account, writer, reviewer, view, base) = forge_review_fixture();
        let source =
            forge_review_source(&store, &account, base, &[("original.txt", b"content")], 4);
        let first = store
            .forge_push_review(
                &account,
                &writer,
                &forge_review_request(base, source, view, "seed"),
                &forge_review_signature(),
                1,
            )
            .unwrap();
        forge_review_check(&store, &account, &first);
        store
            .forge_approve_review(
                &account,
                &reviewer,
                ChangeId(first.change_id),
                first.patchset_id,
                first.candidate.as_ref().unwrap().id,
                "seed-approve",
                2,
            )
            .unwrap();
        let landed = store
            .forge_land_review(
                &account,
                &writer,
                ChangeId(first.change_id),
                first.patchset_id,
                first.candidate.unwrap().id,
                3,
            )
            .unwrap();
        let canonical = landed.candidate.unwrap().head;
        let selected = store
            .forge_publish_view(
                &account,
                &writer,
                ViewSpec {
                    name: "all".into(),
                    root_prefix: "".into(),
                    include_prefixes: vec!["".into()],
                    exclude_prefixes: vec![],
                    visibility: ForgeViewVisibility::Members,
                    revision: canonical,
                    export_allowed: true,
                },
                PolicyEpoch(3),
                false,
            )
            .unwrap();
        let projection = store.forge_project_view(&selected, 4).unwrap();
        let repo = account.repository_id();
        let (_, bytes) = store
            .read_metadata(
                &repo,
                ObjectKey {
                    kind: Kind::Tree,
                    id: ObjectId(projection.projected_tree),
                },
            )
            .unwrap();
        let Object::Tree(tree) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else {
            panic!()
        };
        let original = tree.entries[0].value.clone();
        let TreeValue::File {
            copy_id: Some(parent_copy),
            ..
        } = original.clone()
        else {
            panic!("sanitized copy seed missing")
        };
        let copy = tinytree_model::encode(
            &Object::CopyHistory(tinytree_model::CopyHistory {
                current_path: "renamed.txt".into(),
                parents: vec![parent_copy],
                salt: vec![9],
            }),
            Limits::default(),
        )
        .unwrap();
        store
            .forge_upload_object(
                &selected,
                &writer,
                "renamed.txt",
                copy.descriptor,
                &copy.bytes,
                4,
            )
            .unwrap();
        let edited = tinytree_model::describe_file(b"edited content");
        store
            .forge_upload_file(&selected, &writer, "renamed.txt", b"edited content", 4)
            .unwrap();
        let mut entries = if rename {
            vec![]
        } else {
            vec![TreeEntry {
                name: "original.txt".into(),
                value: original,
            }]
        };
        entries.push(TreeEntry {
            name: "renamed.txt".into(),
            value: TreeValue::File {
                id: FileId(edited.key.id.0),
                executable: false,
                size_bytes: edited.size_bytes,
                copy_id: Some(tinytree_model::CopyId(copy.descriptor.key.id.0)),
            },
        });
        let tree =
            tinytree_model::encode(&Object::Tree(Tree { entries }), Limits::default()).unwrap();
        store
            .forge_upload_object(&selected, &writer, "", tree.descriptor, &tree.bytes, 4)
            .unwrap();
        let head = CommitId(
            landing_commit(
                &store,
                &repo,
                CommitId(projection.projected_commit),
                TreeId(tree.descriptor.key.id.0),
                5,
                "explicit lineage",
            )
            .key
            .id
            .0,
        );
        let (descriptor, bytes) = store
            .read_metadata(
                &repo,
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(head.0),
                },
            )
            .unwrap();
        store
            .forge_upload_object(&selected, &writer, "", descriptor, &bytes, 4)
            .unwrap();
        let mut request = forge_review_request(
            CommitId(projection.projected_commit),
            head,
            selected.version(),
            "lineage",
        );
        request.lineage = vec![ForgeReviewLineageHint {
            operation: if rename {
                ForgeReviewLineageOperation::Rename
            } else {
                ForgeReviewLineageOperation::Copy
            },
            source_path: "original.txt".into(),
            target_path: "renamed.txt".into(),
        }];

        let review = store
            .forge_push_review(&account, &writer, &request, &forge_review_signature(), 5)
            .unwrap();
        assert_eq!(review.lineage, request.lineage);
        assert!(review.candidate.is_some());
        forge_review_check(&store, &account, &review);
        store
            .forge_approve_review(
                &account,
                &reviewer,
                ChangeId(review.change_id),
                review.patchset_id,
                review.candidate.as_ref().unwrap().id,
                "lineage-approve",
                6,
            )
            .unwrap();
        let landed = store
            .forge_land_review(
                &account,
                &writer,
                ChangeId(review.change_id),
                review.patchset_id,
                review.candidate.unwrap().id,
                7,
            )
            .unwrap();
        assert_eq!(landed.state, ForgeReviewState::Landed);
        let safe = store
            .forge_project_review_result(
                &account,
                &writer,
                ChangeId(landed.change_id),
                landed.patchset_id,
                7,
            )
            .unwrap()
            .unwrap();
        let (_, bytes, _) = store
            .forge_read_object_chunk(
                &selected,
                None,
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(safe.projected_commit),
                },
                7,
                0,
                65536,
            )
            .unwrap();
        let Object::Commit(commit) = tinytree_model::decode(&bytes, Limits::default()).unwrap()
        else {
            panic!()
        };
        assert_eq!(commit.change_id, ChangeId(landed.change_id));
        assert_eq!(commit.parents, vec![CommitId(projection.projected_commit)]);
    }
}

#[test]
fn forge_review_new_patchset_cannot_reuse_old_candidate_checks_or_approval() {
    let (_directory, store, account, writer, reviewer, view, base) = forge_review_fixture();
    let head = forge_review_source(&store, &account, base, &[("source.txt", b"first")], 5);
    let request = forge_review_request(base, head, view, "version-one");
    let first = store
        .forge_push_review(&account, &writer, &request, &forge_review_signature(), 1)
        .unwrap();
    forge_review_check(&store, &account, &first);
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(first.change_id),
            first.patchset_id,
            first.candidate.as_ref().unwrap().id,
            "old-approval",
            2,
        )
        .unwrap();
    let head = forge_review_source(&store, &account, base, &[("source.txt", b"updated")], 5);
    let mut next = forge_review_request(base, head, view, "version-two");
    next.expected_patchset = Some(first.patchset_id);
    let second = store
        .forge_push_review(&account, &writer, &next, &forge_review_signature(), 3)
        .unwrap();
    assert_eq!(first.change_id, second.change_id);
    assert_ne!(first.patchset_id, second.patchset_id);
    assert_eq!(second.approvals, 0);
    assert!(matches!(
        store.forge_land_review(
            &account,
            &writer,
            ChangeId(first.change_id),
            first.patchset_id,
            first.candidate.unwrap().id,
            4
        ),
        Err(Error::StaleForgeReview)
    ));
    assert!(matches!(
        store.forge_approve_review(
            &account,
            &reviewer,
            ChangeId(second.change_id),
            second.patchset_id,
            second.candidate.as_ref().unwrap().id,
            "old-approval",
            4
        ),
        Err(Error::IdempotencyConflict)
    ));
    let replay = store
        .forge_push_review(&account, &writer, &request, &forge_review_signature(), 4)
        .unwrap();
    assert_eq!(replay.patchset_id, first.patchset_id);
    assert!(!replay.is_current);
    assert_eq!(
        store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .generation,
        1
    );
}

#[test]
fn forge_review_accepts_owned_native_symlink_metadata_without_digest_only_authority() {
    let (_directory, store, account, writer, _reviewer, view, base) = forge_review_fixture();
    let selected = store
        .forge_resolve_view(&account, Some(&writer), "all", Some(view))
        .unwrap();
    let link =
        tinytree_model::encode(&Object::Symlink("source.txt".into()), Limits::default()).unwrap();
    store
        .forge_upload_object(&selected, &writer, "link", link.descriptor, &link.bytes, 0)
        .unwrap();
    let tree = tinytree_model::encode(
        &Object::Tree(Tree {
            entries: vec![TreeEntry {
                name: "link".into(),
                value: TreeValue::Symlink {
                    id: tinytree_model::SymlinkId(link.descriptor.key.id.0),
                    target_bytes: 10,
                },
            }],
        }),
        Limits::default(),
    )
    .unwrap();
    store
        .forge_upload_object(&selected, &writer, "", tree.descriptor, &tree.bytes, 0)
        .unwrap();
    let head = CommitId(
        landing_commit(
            &store,
            &account.repository_id(),
            base,
            TreeId(tree.descriptor.key.id.0),
            5,
            "owned symlink",
        )
        .key
        .id
        .0,
    );
    let (descriptor, bytes) = store
        .read_metadata(
            &account.repository_id(),
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(head.0),
            },
        )
        .unwrap();
    store
        .forge_upload_object(&selected, &writer, "", descriptor, &bytes, 0)
        .unwrap();
    let review = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, head, view, "symlink"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    assert!(review.candidate.is_some());
}

#[test]
fn forge_review_rejects_another_writers_hash_only_source_commit_without_reserving_change() {
    let (_directory, store, account, writer, reviewer, view, base) = forge_review_fixture();
    let initial = forge_review_source(&store, &account, base, &[("remove.txt", b"visible")], 4);
    let initial = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, initial, view, "initial"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    forge_review_check(&store, &account, &initial);
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(initial.change_id),
            initial.patchset_id,
            initial.candidate.as_ref().unwrap().id,
            "initial-approve",
            2,
        )
        .unwrap();
    let initial = store
        .forge_land_review(
            &account,
            &writer,
            ChangeId(initial.change_id),
            initial.patchset_id,
            initial.candidate.unwrap().id,
            3,
        )
        .unwrap();
    let victim = ForgeActor {
        principal: "second-writer".into(),
    };
    store
        .forge_set_member(
            &account,
            &writer,
            ForgeMember {
                principal: victim.principal.clone(),
                can_write: true,
                can_review: false,
                is_admin: false,
                roles: vec![],
            },
            PolicyEpoch(3),
        )
        .unwrap();
    let selected = store
        .forge_publish_view(
            &account,
            &writer,
            ViewSpec {
                name: "all".into(),
                root_prefix: "".into(),
                include_prefixes: vec!["".into()],
                exclude_prefixes: vec![],
                visibility: ForgeViewVisibility::Members,
                revision: initial.candidate.unwrap().head,
                export_allowed: true,
            },
            PolicyEpoch(4),
            false,
        )
        .unwrap();
    let base = CommitId(
        store
            .forge_project_view(&selected, 0)
            .unwrap()
            .projected_commit,
    );
    let empty =
        tinytree_model::encode(&Object::Tree(Tree { entries: vec![] }), Limits::default()).unwrap();
    store
        .forge_upload_object(&selected, &victim, "", empty.descriptor, &empty.bytes, 0)
        .unwrap();
    let head = landing_commit(
        &store,
        &account.repository_id(),
        base,
        TreeId(empty.descriptor.key.id.0),
        9,
        "second writer draft",
    );
    let (_, bytes) = store
        .read_metadata(&account.repository_id(), head.key)
        .unwrap();
    store
        .forge_upload_object(&selected, &victim, "", head, &bytes, 0)
        .unwrap();
    let request = forge_review_request(
        base,
        CommitId(head.key.id.0),
        selected.version(),
        "hash-only",
    );
    assert!(matches!(
        store.forge_push_review(&account, &writer, &request, &forge_review_signature(), 1),
        Err(Error::ForgeAccessDenied)
    ));
    assert!(
        store
            .forge_review_operation(&account, &writer, "hash-only")
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .forge_review(&account, &writer, ChangeId([9; 16]))
            .is_err()
    );
    assert_eq!(
        store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .generation,
        2
    );

    let accepted = store
        .forge_push_review(&account, &victim, &request, &forge_review_signature(), 1)
        .unwrap();
    assert_eq!(accepted.owner, victim.principal);
    assert!(accepted.candidate.is_some());
}

#[test]
fn forge_review_delayed_decision_cannot_approve_an_unseen_rebased_candidate() {
    let (_directory, store, account, writer, reviewer, view, base) = forge_review_fixture();
    let head = forge_review_source(&store, &account, base, &[("a", b"first")], 4);
    let first = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, head, view, "first"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    let inspected = first.candidate.unwrap().id;
    let head = forge_review_source(&store, &account, base, &[("b", b"other")], 5);
    let other = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, head, view, "other"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    forge_review_check(&store, &account, &other);
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(other.change_id),
            other.patchset_id,
            other.candidate.as_ref().unwrap().id,
            "other-approve",
            2,
        )
        .unwrap();
    store
        .forge_land_review(
            &account,
            &writer,
            ChangeId(other.change_id),
            other.patchset_id,
            other.candidate.unwrap().id,
            3,
        )
        .unwrap();
    let refreshed = store
        .forge_prepare_review(
            &account,
            &writer,
            ChangeId(first.change_id),
            first.patchset_id,
            4,
        )
        .unwrap();
    let current = refreshed.candidate.unwrap().id;
    assert_ne!(current, inspected);
    assert!(matches!(
        store.forge_approve_review(
            &account,
            &reviewer,
            ChangeId(first.change_id),
            first.patchset_id,
            inspected,
            "delayed-approve",
            4
        ),
        Err(Error::StaleForgeReview)
    ));
    assert_eq!(
        store
            .forge_review(&account, &reviewer, ChangeId(first.change_id))
            .unwrap()
            .approvals,
        0
    );
    let approved = store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(first.change_id),
            first.patchset_id,
            current,
            "delayed-approve",
            4,
        )
        .unwrap();
    assert_eq!(approved.approvals, 1);
    assert_eq!(
        store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .generation,
        2
    );
}

#[test]
fn forge_review_expired_checked_predecessor_cannot_be_landed_by_a_newer_child() {
    let (_directory, store, account, writer, reviewer, view, base) = forge_review_fixture();
    let head = forge_review_source(&store, &account, base, &[("a", b"parent")], 4);
    let parent = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, head, view, "parent"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    let head = forge_review_source(
        &store,
        &account,
        head,
        &[("a", b"parent"), ("b", b"child")],
        5,
    );
    let mut request = forge_review_request(CommitId(parent.source_head), head, view, "child");
    request.predecessor = Some(ForgeReviewDependency {
        change_id: parent.change_id,
        patchset_id: parent.patchset_id,
    });
    let child = store
        .forge_push_review(&account, &writer, &request, &forge_review_signature(), 2)
        .unwrap();
    let parent_handle = forge_review_check(&store, &account, &parent);
    let child_handle = forge_review_check(&store, &account, &child);
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(parent.change_id),
            parent.patchset_id,
            parent.candidate.as_ref().unwrap().id,
            "parent-approve",
            3,
        )
        .unwrap();
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(child.change_id),
            child.patchset_id,
            child.candidate.as_ref().unwrap().id,
            "child-approve",
            3,
        )
        .unwrap();
    let expired_at = parent_handle.status(&store).unwrap().expires_at;
    assert!(child_handle.status(&store).unwrap().expires_at > expired_at);
    assert_eq!(
        parent_handle.status(&store).unwrap().state,
        LandingState::Checked
    );
    assert!(
        store
            .forge_land_review(
                &account,
                &writer,
                ChangeId(child.change_id),
                child.patchset_id,
                child.candidate.unwrap().id,
                expired_at
            )
            .is_err()
    );
    assert_eq!(
        parent_handle.status(&store).unwrap().state,
        LandingState::Checked,
        "test does not rely on expiry worker transition"
    );
    assert_eq!(
        store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .generation,
        1
    );
}

#[test]
fn forge_review_child_current_profile_does_not_replace_predecessors_profile_obligation() {
    let (_directory, store, account, writer, reviewer, view, base) = forge_review_fixture();
    let head = forge_review_source(&store, &account, base, &[("a", b"parent")], 4);
    let parent = store
        .forge_push_review(
            &account,
            &writer,
            &forge_review_request(base, head, view, "old-profile-parent"),
            &forge_review_signature(),
            1,
        )
        .unwrap();
    let head = forge_review_source(
        &store,
        &account,
        head,
        &[("a", b"parent"), ("b", b"child")],
        5,
    );
    let mut request = forge_review_request(
        CommitId(parent.source_head),
        head,
        view,
        "new-profile-child",
    );
    request.predecessor = Some(ForgeReviewDependency {
        change_id: parent.change_id,
        patchset_id: parent.patchset_id,
    });
    request.check_profile.version = "v2".into();
    let child = store
        .forge_push_review(&account, &writer, &request, &forge_review_signature(), 2)
        .unwrap();
    forge_review_check(&store, &account, &parent);
    forge_review_check(&store, &account, &child);
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(parent.change_id),
            parent.patchset_id,
            parent.candidate.as_ref().unwrap().id,
            "old-profile-approve",
            3,
        )
        .unwrap();
    store
        .forge_approve_review(
            &account,
            &reviewer,
            ChangeId(child.change_id),
            child.patchset_id,
            child.candidate.as_ref().unwrap().id,
            "new-profile-approve",
            3,
        )
        .unwrap();
    assert!(
        store
            .forge_land_review(
                &account,
                &writer,
                ChangeId(child.change_id),
                child.patchset_id,
                child.candidate.unwrap().id,
                4
            )
            .is_err()
    );
    assert_eq!(
        store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .generation,
        1
    );
}

#[test]
fn landing_restart_check_finalize_and_historical_replay_are_exact_and_atomic() {
    let (directory, store) = open();
    let owner = LandingOwner([1; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "land");
    let accepted = store.begin_landing("demo", owner, &input, 0).unwrap();
    assert_eq!(accepted.state, LandingState::Intent);
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    let prepared = store.prepare_landing("demo", owner, "land", 1).unwrap();
    assert_eq!(prepared.state, LandingState::Prepared);
    let candidate = prepared.candidate.as_ref().unwrap();
    let (_, bytes) = store
        .read_metadata(
            "demo",
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(candidate.head.0),
            },
        )
        .unwrap();
    let Object::Commit(new) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else {
        panic!()
    };
    assert_eq!(new.parents, [CommitId(dest.key.id.0)]);
    assert_eq!(new.change_id, ChangeId([2; 16]));
    assert_eq!(new.committer, input.committer);
    let (_, bytes) = store
        .read_metadata(
            "demo",
            ObjectKey {
                kind: Kind::Tree,
                id: ObjectId(candidate.root_tree.0),
            },
        )
        .unwrap();
    let Object::Tree(tree) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else {
        panic!()
    };
    assert_eq!(
        tree.entries
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        ["destination.txt", "source.txt"]
    );
    let identity = landing_identity(&prepared);
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    let checked = store
        .record_landing_check(
            "demo",
            owner,
            "land",
            &identity,
            LandingVerdict {
                passed: true,
                evidence: [9; 32],
            },
            2,
        )
        .unwrap();
    assert_eq!(checked.state, LandingState::Checked);
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    let done = store.finalize_landing("demo", owner, "land", 3).unwrap();
    assert_eq!(done.state, LandingState::Committed);
    let receipt = done.receipt.as_ref().unwrap();
    assert_eq!(receipt.reference.generation, 2);
    assert_eq!(
        receipt.reference.commit.unwrap().key.id.0,
        identity.candidate.0
    );
    assert_eq!(receipt.rewrites.len(), 1);
    assert_eq!(receipt.rewrites[0].old, input.source_head);
    assert_eq!(receipt.evidence, [9; 32]);
    assert_eq!(
        store
            .references("demo", &[input.reference_name.clone()])
            .unwrap()[0],
        receipt.reference
    );
    store
        .publish("demo", &request("move-after-land", 2, Some(base)), 4)
        .unwrap();
    let done = store.expire_landing("demo", owner, "land", 100).unwrap();
    assert!(!done.roots_retained);
    store.gc("demo", 1000).unwrap();
    assert!(matches!(
        store.read("demo", source.key),
        Err(Error::MissingObject(_))
    ));
    let mut retry = input.clone();
    retry.committer.timestamp.millis_since_epoch = 5000;
    retry.expires_at = 5001;
    assert_eq!(
        store.begin_landing("demo", owner, &retry, 5000).unwrap(),
        done
    );
    assert_eq!(
        store.finalize_landing("demo", owner, "land", 5000).unwrap(),
        done
    );
    assert_eq!(
        store.references("demo", &[input.reference_name]).unwrap()[0].generation,
        3
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store.landing_job("demo", owner, "land").unwrap().unwrap(),
        done
    );
}

#[test]
fn landing_checks_bind_candidate_root_profile_and_cannot_turn_failure_into_publication() {
    let (_directory, store) = open();
    let owner = LandingOwner([2; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "reject");
    store.begin_landing("demo", owner, &input, 0).unwrap();
    let prepared = store.prepare_landing("demo", owner, "reject", 1).unwrap();
    let identity = landing_identity(&prepared);
    let mut wrong = identity.clone();
    wrong.candidate = CommitId([7; 32]);
    assert!(matches!(
        store.record_landing_check(
            "demo",
            owner,
            "reject",
            &wrong,
            LandingVerdict {
                passed: true,
                evidence: [1; 32]
            },
            2
        ),
        Err(Error::Invalid("landing check identity"))
    ));
    let mut wrong = identity.clone();
    wrong.root_tree = TreeId([7; 32]);
    assert!(
        store
            .record_landing_check(
                "demo",
                owner,
                "reject",
                &wrong,
                LandingVerdict {
                    passed: true,
                    evidence: [1; 32]
                },
                2
            )
            .is_err()
    );
    let mut wrong = identity.clone();
    wrong.profile.version = "other".into();
    assert!(
        store
            .record_landing_check(
                "demo",
                owner,
                "reject",
                &wrong,
                LandingVerdict {
                    passed: true,
                    evidence: [1; 32]
                },
                2
            )
            .is_err()
    );
    assert!(store.finalize_landing("demo", owner, "reject", 2).is_err());
    let failed = store
        .record_landing_check(
            "demo",
            owner,
            "reject",
            &identity,
            LandingVerdict {
                passed: false,
                evidence: [1; 32],
            },
            2,
        )
        .unwrap();
    assert_eq!(failed.state, LandingState::CheckFailed);
    assert!(matches!(
        store.record_landing_check(
            "demo",
            owner,
            "reject",
            &identity,
            LandingVerdict {
                passed: true,
                evidence: [1; 32]
            },
            3
        ),
        Err(Error::IdempotencyConflict)
    ));
    assert_eq!(
        store.finalize_landing("demo", owner, "reject", 3).unwrap(),
        failed
    );
    assert_eq!(
        store.references("demo", &[input.reference_name]).unwrap()[0].commit,
        Some(dest)
    );
}

#[test]
fn landing_target_moved_before_acceptance_is_durable_without_candidate_or_roots() {
    let (directory, store) = open();
    let owner = LandingOwner([31; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "stale-before-acceptance");
    let next = landing_commit(
        &store,
        "demo",
        CommitId(dest.key.id.0),
        TreeId(tinytree_model::empty_tree().descriptor.key.id.0),
        32,
        "another writer",
    );
    store
        .publish("demo", &request("other-writer", 1, Some(next)), 1)
        .unwrap();
    let before = store
        .references("demo", std::slice::from_ref(&input.reference_name))
        .unwrap();
    let stale = store.begin_landing("demo", owner, &input, 2).unwrap();
    assert_eq!(stale.state, LandingState::StaleTarget);
    assert!(!stale.roots_retained);
    assert!(stale.candidate.is_none() && stale.receipt.is_none() && stale.conflict_path.is_none());
    assert_eq!(stale.expires_at, input.expires_at);
    assert_eq!(
        store
            .prepare_landing("demo", owner, &input.idempotency_key, 3)
            .unwrap(),
        stale
    );
    assert_eq!(
        store
            .finalize_landing("demo", owner, &input.idempotency_key, 3)
            .unwrap(),
        stale
    );
    assert!(
        store
            .scan_pending_landings("demo", None, Default::default())
            .unwrap()
            .jobs
            .is_empty()
    );
    assert!(store.external_checks_directory().unwrap().is_some());
    let mut replay = input.clone();
    replay.committer.timestamp.millis_since_epoch += 1;
    replay.expires_at += 1;
    assert_eq!(
        store.begin_landing("demo", owner, &replay, 3).unwrap(),
        stale
    );
    let mut changed = input.clone();
    changed.expected_generation = 2;
    changed.expected_head = CommitId(next.key.id.0);
    assert!(matches!(
        store.begin_landing("demo", owner, &changed, 3),
        Err(Error::IdempotencyConflict)
    ));
    changed.idempotency_key = "fresh-after-stale".into();
    let fresh = store.begin_landing("demo", owner, &changed, 3).unwrap();
    assert_eq!(fresh.state, LandingState::Intent);
    store
        .expire_landing("demo", owner, &changed.idempotency_key, changed.expires_at)
        .unwrap();
    assert_eq!(
        store
            .references("demo", std::slice::from_ref(&input.reference_name))
            .unwrap(),
        before
    );
    store.gc("demo", 1000).unwrap();
    assert!(
        store.descriptor("demo", source.key).unwrap().is_none(),
        "terminal rejection makes no source residency promise"
    );
    assert_eq!(
        store.begin_landing("demo", owner, &input, 1001).unwrap(),
        stale
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store
            .landing_job("demo", owner, &input.idempotency_key)
            .unwrap(),
        Some(stale.clone())
    );
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.meta, b"format")
            .unwrap()
            .unwrap()
            .as_ref(),
        b"tinytree-store-10"
    );
    drop(store);
    let set_format = |format: &[u8]| {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&meta, b"format", format);
        tx.commit().unwrap();
    };
    set_format(b"tinytree-store-9");
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt(
            "preparation-stale landing without store feature"
        ))
    ));
    set_format(b"tinytree-store-10");
    drop(Store::open_existing(directory.path(), config()).unwrap());
}

#[test]
fn landing_competing_jobs_have_one_atomic_winner_and_a_durable_stale_result() {
    let (directory, store) = open();
    let owner = LandingOwner([3; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let mut jobs = Vec::new();
    for token in ["first", "second"] {
        let input = landing_request(base, source, dest, token);
        store.begin_landing("demo", owner, &input, 0).unwrap();
        let job = store.prepare_landing("demo", owner, token, 1).unwrap();
        store
            .record_landing_check(
                "demo",
                owner,
                token,
                &landing_identity(&job),
                LandingVerdict {
                    passed: true,
                    evidence: [2; 32],
                },
                2,
            )
            .unwrap();
    }
    let barrier = Arc::new(std::sync::Barrier::new(3));
    for token in ["first", "second"] {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        jobs.push(std::thread::spawn(move || {
            barrier.wait();
            store.finalize_landing("demo", owner, token, 3).unwrap()
        }));
    }
    barrier.wait();
    let results = jobs
        .into_iter()
        .map(|j| j.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .filter(|j| j.state == LandingState::Committed)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|j| j.state == LandingState::StaleTarget)
            .count(),
        1
    );
    let winner = results
        .iter()
        .find(|j| j.state == LandingState::Committed)
        .unwrap();
    assert_eq!(
        store.references("demo", &["Main/feature".into()]).unwrap()[0],
        winner.receipt.as_ref().unwrap().reference
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    for (token, expected) in ["first", "second"].into_iter().zip(results) {
        assert_eq!(
            store.finalize_landing("demo", owner, token, 4).unwrap(),
            expected
        );
    }
}

#[test]
fn landing_expiry_is_durable_and_gc_does_not_infer_expiry_from_clock() {
    let (directory, store) = open();
    let owner = LandingOwner([4; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "expire");
    store.begin_landing("demo", owner, &input, 0).unwrap();
    store.gc("demo", 1000).unwrap();
    assert!(store.read("demo", source.key).is_ok());
    assert!(
        store
            .prepare_landing("demo", owner, "expire", 1000)
            .is_err()
    );
    assert!(store.expire_landing("demo", owner, "expire", 99).is_err());
    let expired = store.expire_landing("demo", owner, "expire", 100).unwrap();
    assert_eq!(expired.state, LandingState::Expired);
    store.gc("demo", 1000).unwrap();
    assert!(matches!(
        store.read("demo", source.key),
        Err(Error::MissingObject(_))
    ));
    assert_eq!(
        store.prepare_landing("demo", owner, "expire", 0).unwrap(),
        expired
    );
    assert_eq!(
        store.begin_landing("demo", owner, &input, 0).unwrap(),
        expired
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store.prepare_landing("demo", owner, "expire", 0).unwrap(),
        expired
    );
}

#[test]
fn landing_runtime_owners_protect_inputs_and_installed_outputs_during_expiry() {
    for phase in ["inputs", "outputs"] {
        let (_directory, store) = open();
        let owner = LandingOwner([5; 32]);
        let (base, source, dest) = landing_fixture(&store, "demo");
        let input = landing_request(base, source, dest, "inflight");
        store.begin_landing("demo", owner, &input, 0).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let worker_store = store.clone();
        let worker = std::thread::spawn(move || {
            landing::test_checkpoint(Some(Box::new(move |current| {
                if current == phase {
                    entered_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }
            })));
            let result = worker_store.prepare_landing("demo", owner, "inflight", 1);
            landing::test_checkpoint(None);
            result
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        store
            .expire_landing("demo", owner, "inflight", 100)
            .unwrap();
        store.gc("demo", 1000).unwrap();
        assert!(store.read("demo", source.key).is_ok());
        resume_tx.send(()).unwrap();
        assert_eq!(worker.join().unwrap().unwrap().state, LandingState::Expired);
        store.gc("demo", 1000).unwrap();
        assert!(matches!(
            store.read("demo", source.key),
            Err(Error::MissingObject(_))
        ));
    }
}

#[test]
fn landing_scoped_replay_and_quota_fail_closed_without_forgetting_accepted_results() {
    let directory = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.landing.max_jobs_per_repository = 1;
    let store = Store::open(directory.path(), limits.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = LandingOwner([6; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "same");
    let accepted = store.begin_landing("demo", owner, &input, 0).unwrap();
    assert!(
        store
            .landing_job("demo", LandingOwner([7; 32]), "same")
            .unwrap()
            .is_none()
    );
    let mut different = input.clone();
    different.source_head = CommitId([99; 32]);
    assert!(matches!(
        store.begin_landing("demo", owner, &different, 0),
        Err(Error::IdempotencyConflict)
    ));
    different = input.clone();
    different.check_profile.version = "v2".into();
    assert!(matches!(
        store.begin_landing("demo", owner, &different, 0),
        Err(Error::IdempotencyConflict)
    ));
    let expired = store.expire_landing("demo", owner, "same", 100).unwrap();
    different = input.clone();
    different.idempotency_key = "new".into();
    different.expires_at = 200;
    assert!(matches!(
        store.begin_landing("demo", owner, &different, 100),
        Err(Error::Limit("landing accepted job quota"))
    ));
    store
        .publish("demo", &request("quota-other-writer", 1, Some(source)), 100)
        .unwrap();
    different.idempotency_key = "stale-at-quota".into();
    assert!(matches!(
        store.begin_landing("demo", owner, &different, 100),
        Err(Error::Limit("landing accepted job quota"))
    ));
    assert!(
        store
            .landing_job("demo", owner, &different.idempotency_key)
            .unwrap()
            .is_none()
    );
    assert!(
        store_feature(
            store
                .inner
                .db
                .read_tx()
                .get(&store.inner.spaces.meta, b"format")
                .unwrap()
                .as_deref()
        )
        .unwrap()
            < 10,
        "failed rejection admission cannot advance its atomic feature"
    );
    assert_eq!(
        store.landing_job("demo", owner, "same").unwrap().unwrap(),
        expired
    );
    assert_eq!(accepted.id, expired.id);
    drop(store);
    assert!(Store::open_existing(directory.path(), limits).is_ok());
}

#[test]
fn landing_feature_layout_and_downgrade_checks_fail_closed_on_reopen() {
    let (directory, store) = open();
    let owner = LandingOwner([8; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    store
        .begin_landing(
            "demo",
            owner,
            &landing_request(base, source, dest, "feature"),
            0,
        )
        .unwrap();
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.meta, b"format")
            .unwrap()
            .unwrap()
            .as_ref(),
        b"tinytree-store-6"
    );
    assert!(directory.path().join("cloud").is_dir());
    assert!(matches!(
        store.plan_client_gc("demo", &client_collection(&store, 100)),
        Err(Error::Invalid(
            "authoritative service state requires server GC"
        ))
    ));
    drop(store);
    drop(Store::open_existing(directory.path(), config()).unwrap());
    let set_format = |format: &[u8]| {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&meta, b"format", format);
        tx.commit().unwrap();
    };
    set_format(b"tinytree-store-5");
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("landing state without store feature"))
    ));
    set_format(b"tinytree-store-6");
    {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let jobs = db
            .keyspace("landing-jobs-v1", KeyspaceCreateOptions::default)
            .unwrap();
        db.inner().delete_keyspace(jobs.inner().clone()).unwrap();
    }
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("landing keyspace missing"))
    ));
}

#[test]
fn landing_checked_candidate_and_check_binding_corruption_are_rejected() {
    let (directory, store) = open();
    let owner = LandingOwner([9; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "corruption");
    let id = store.begin_landing("demo", owner, &input, 0).unwrap().id;
    let prepared = store
        .prepare_landing("demo", owner, "corruption", 1)
        .unwrap();
    store
        .record_landing_check(
            "demo",
            owner,
            "corruption",
            &landing_identity(&prepared),
            LandingVerdict {
                passed: true,
                evidence: [4; 32],
            },
            2,
        )
        .unwrap();
    let physical = key("demo", &id);
    let original = store
        .inner
        .db
        .read_tx()
        .get(&store.inner.spaces.landing, &physical)
        .unwrap()
        .unwrap()
        .to_vec();
    drop(store);
    let replace = |bytes: &[u8]| {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let jobs = db
            .keyspace("landing-jobs-v1", KeyspaceCreateOptions::default)
            .unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&jobs, &physical, bytes);
        tx.commit().unwrap();
    };
    let mut row: serde_json::Value = serde_json::from_slice(&original).unwrap();
    row["candidate"]["tree"][0] = serde_json::json!(255);
    replace(&serde_json::to_vec(&row).unwrap());
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("landing candidate digest"))
    ));
    let mut row: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let value = row["check"]["binding"][0].as_u64().unwrap();
    row["check"]["binding"][0] = serde_json::json!(value ^ 1);
    replace(&serde_json::to_vec(&row).unwrap());
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("landing check binding"))
    ));
    replace(&original);
    assert!(Store::open_existing(directory.path(), config()).is_ok());
}

#[test]
fn landing_global_record_and_candidate_budgets_reject_before_acknowledgement() {
    let directory = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.max_retention_validation_bytes = 6000;
    let store = Store::open(directory.path(), limits.clone()).unwrap();
    let owner = LandingOwner([10; 32]);
    let mut requests = Vec::new();
    for repo in ["demo", "other"] {
        store.create_repository(repo).unwrap();
        let (base, source, dest) = landing_fixture(&store, repo);
        let mut input = landing_request(base, source, dest, "budget");
        input.committer.name = "n".repeat(1024);
        input.committer.email = "e".repeat(1024);
        requests.push((repo, input));
    }
    store
        .begin_landing(requests[0].0, owner, &requests[0].1, 0)
        .unwrap();
    assert!(matches!(
        store.begin_landing(requests[1].0, owner, &requests[1].1, 0),
        Err(Error::Limit("landing record scan budget"))
    ));
    assert!(
        store
            .landing_job(requests[1].0, owner, "budget")
            .unwrap()
            .is_none()
    );
    drop(store);
    drop(Store::open_existing(directory.path(), limits).unwrap());
    let directory = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.landing.max_validation_metadata_bytes = 4096;
    let store = Store::open(directory.path(), limits.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let (base, source, dest) = landing_fixture(&store, "demo");
    let mut prepared = 0;
    let mut rejected = false;
    for index in 0..16 {
        let token = format!("candidate-{index}");
        store
            .begin_landing(
                "demo",
                owner,
                &landing_request(base, source, dest, &token),
                0,
            )
            .unwrap();
        match store.prepare_landing("demo", owner, &token, 1) {
            Ok(job) => {
                assert_eq!(job.state, LandingState::Prepared);
                prepared += 1
            }
            Err(Error::Limit("landing validation bytes")) => {
                assert_eq!(
                    store
                        .landing_job("demo", owner, &token)
                        .unwrap()
                        .unwrap()
                        .state,
                    LandingState::Intent
                );
                rejected = true;
                break;
            }
            other => panic!("unexpected prepare result: {other:?}"),
        }
    }
    assert!(prepared > 0 && rejected);
    drop(store);
    assert!(Store::open_existing(directory.path(), limits).is_ok());
}

#[test]
fn landing_expiry_uses_reserved_space_when_accepted_replay_ledger_is_full() {
    let (directory, store) = open();
    let owner = LandingOwner([11; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "last-byte");
    let accepted = store.begin_landing("demo", owner, &input, 0).unwrap();
    let physical = key("demo", &accepted.id);
    let bytes = store
        .inner
        .db
        .read_tx()
        .get(&store.inner.spaces.landing, &physical)
        .unwrap()
        .unwrap();
    let budget = physical.len() + bytes.len() + 1;
    drop(bytes);
    drop(store);
    for (maximum, accepted) in [(budget - 1, false), (budget, true)] {
        let fresh = tempfile::tempdir().unwrap();
        let mut limits = config();
        limits.max_retention_validation_bytes = maximum as u64;
        let fresh_store = Store::open(fresh.path(), limits.clone()).unwrap();
        fresh_store.create_repository("demo").unwrap();
        let (base, source, dest) = landing_fixture(&fresh_store, "demo");
        let input = landing_request(base, source, dest, "last-byte");
        let result = fresh_store.begin_landing("demo", owner, &input, 0);
        if accepted {
            assert_eq!(result.unwrap().state, LandingState::Intent);
            assert_eq!(
                fresh_store
                    .expire_landing("demo", owner, "last-byte", 100)
                    .unwrap()
                    .state,
                LandingState::Expired
            );
        } else {
            assert!(matches!(
                result,
                Err(Error::Limit("landing record scan budget"))
            ));
            assert!(
                fresh_store
                    .landing_job("demo", owner, "last-byte")
                    .unwrap()
                    .is_none()
            );
        }
        drop(fresh_store);
        drop(Store::open_existing(fresh.path(), limits).unwrap());
    }
    let mut limits = config();
    limits.max_retention_validation_bytes = budget as u64;
    let store = Store::open_existing(directory.path(), limits.clone()).unwrap();
    assert_eq!(
        store
            .expire_landing("demo", owner, "last-byte", 100)
            .unwrap()
            .state,
        LandingState::Expired
    );
    store.gc("demo", 1000).unwrap();
    drop(store);
    assert!(Store::open_existing(directory.path(), limits).is_ok());
}

#[test]
fn landing_roots_join_ordinary_publication_and_candidate_gc_admission_budgets() {
    let directory = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.max_closure_objects = 8;
    let store = Store::open(directory.path(), limits.clone()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = LandingOwner([13; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "union");
    store.begin_landing("demo", owner, &input, 0).unwrap();
    assert!(matches!(
        store.prepare_landing("demo", owner, "union", 1),
        Err(Error::Limit("object closure"))
    ));
    assert_eq!(
        store
            .landing_job("demo", owner, "union")
            .unwrap()
            .unwrap()
            .state,
        LandingState::Intent
    );
    let empty = tinytree_model::empty_tree();
    let other = landing_commit(
        &store,
        "demo",
        CommitId::ROOT,
        TreeId(empty.descriptor.key.id.0),
        9,
        "new unrelated target",
    );
    assert!(matches!(
        store.publish("demo", &request("over-union", 1, Some(other)), 2),
        Err(Error::Limit("object closure"))
    ));
    assert_eq!(
        store
            .references("demo", &[input.reference_name.clone()])
            .unwrap()[0]
            .commit,
        Some(dest)
    );
    store.gc("demo", 1000).unwrap();
    assert!(store.read("demo", source.key).is_ok());
    drop(store);
    let store = Store::open_existing(directory.path(), limits).unwrap();
    store.expire_landing("demo", owner, "union", 100).unwrap();
    let other = landing_commit(
        &store,
        "demo",
        CommitId::ROOT,
        TreeId(empty.descriptor.key.id.0),
        9,
        "new unrelated target",
    );
    store
        .publish("demo", &request("after-release", 1, Some(other)), 100)
        .unwrap();
}

#[test]
fn landing_unavailable_releases_roots_without_verdict_or_publication_and_replays_after_restart() {
    let directory = external_layout_directory();
    let store = Store::open(directory.path(), config()).unwrap();
    store.create_repository("demo").unwrap();
    let owner = LandingOwner([20; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "unavailable");
    let accepted = store.begin_landing("demo", owner, &input, 0).unwrap();
    let prepared = store
        .prepare_landing("demo", owner, "unavailable", 1)
        .unwrap();
    let identity = landing_identity(&prepared);
    let before = store
        .references("demo", std::slice::from_ref(&input.reference_name))
        .unwrap();
    assert_eq!(before[0].generation, 1);
    assert_eq!(before[0].commit, Some(dest));
    for changed in [
        LandingCheckIdentity {
            candidate: CommitId([1; 32]),
            ..identity.clone()
        },
        LandingCheckIdentity {
            root_tree: TreeId([1; 32]),
            ..identity.clone()
        },
        LandingCheckIdentity {
            profile: LandingCheckProfile {
                version: "other".into(),
                ..identity.profile.clone()
            },
            ..identity.clone()
        },
    ] {
        assert!(matches!(
            store.record_landing_check_unavailable("demo", owner, "unavailable", &changed, 2),
            Err(Error::Invalid("landing check identity"))
        ));
        assert_eq!(
            store
                .landing_job("demo", owner, "unavailable")
                .unwrap()
                .unwrap(),
            prepared
        );
    }
    assert!(
        store
            .recover_landing_handle("demo", [99; 32])
            .unwrap()
            .is_none()
    );
    store.create_repository("other").unwrap();
    assert!(
        store
            .recover_landing_handle("other", accepted.id)
            .unwrap()
            .is_none()
    );
    let handle = store
        .recover_landing_handle("demo", accepted.id)
        .unwrap()
        .unwrap();
    let (other_directory, other_store) = open();
    assert!(handle.status(&other_store).is_err());
    drop(other_store);
    drop(other_directory);
    assert!(matches!(
        handle.record_check_unavailable(&store, &identity, 2),
        Err(Error::Invalid("external checks are not activated"))
    ));
    assert_eq!(handle.status(&store).unwrap(), prepared);
    assert_eq!(store.external_checks_directory().unwrap(), None);
    store.enable_external_checks().unwrap();
    let unavailable = handle
        .record_check_unavailable(&store, &identity, 2)
        .unwrap();
    assert_eq!(unavailable.state, LandingState::CheckUnavailable);
    assert_eq!(unavailable.expires_at, input.expires_at);
    assert_eq!(unavailable.candidate, prepared.candidate);
    assert!(!unavailable.roots_retained);
    assert!(unavailable.receipt.is_none());
    assert!(unavailable.conflict_path.is_none());
    assert_eq!(
        store
            .references("demo", std::slice::from_ref(&input.reference_name))
            .unwrap(),
        before
    );
    assert!(
        store
            .scan_pending_landings("demo", None, Default::default())
            .unwrap()
            .jobs
            .is_empty()
    );
    assert_eq!(
        handle
            .record_check_unavailable(&store, &identity, 3)
            .unwrap(),
        unavailable
    );
    assert!(
        handle
            .record_check(
                &store,
                &identity,
                LandingVerdict {
                    passed: true,
                    evidence: [1; 32]
                },
                3
            )
            .is_err()
    );
    assert_eq!(handle.finalize(&store, 3).unwrap(), unavailable);
    assert_eq!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.meta, b"format")
            .unwrap()
            .unwrap()
            .as_ref(),
        b"tinytree-store-8"
    );
    let candidate_key = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(identity.candidate.0),
    };
    store.gc("demo", 1000).unwrap();
    assert!(store.descriptor("demo", candidate_key).unwrap().is_none());
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert!(
        handle.status(&store).is_err(),
        "a prior instance's handle is not reused after reopen"
    );
    let recovered = store
        .recover_landing_handle("demo", accepted.id)
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status(&store).unwrap(), unavailable);
    assert_eq!(
        recovered
            .record_check_unavailable(&store, &identity, 1001)
            .unwrap(),
        unavailable
    );
    assert_eq!(
        store
            .references("demo", std::slice::from_ref(&input.reference_name))
            .unwrap(),
        before
    );
    drop(recovered);
    drop(store);
    let set_format = |format: &[u8]| {
        let db = SingleWriterTxDatabase::builder(directory.path().join("metadata"))
            .open()
            .unwrap();
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
        let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&meta, b"format", format);
        tx.commit().unwrap();
    };
    set_format(b"tinytree-store-7");
    assert!(matches!(
        Store::open_existing(directory.path(), config()),
        Err(Error::Corrupt("unavailable landing without store feature"))
    ));
    set_format(b"tinytree-store-8");
    drop(Store::open_existing(directory.path(), config()).unwrap());
}

#[test]
fn landing_unavailable_cannot_replace_a_verdict_and_due_jobs_expire_normally() {
    let directory = external_layout_directory();
    let store = Store::open(directory.path(), config()).unwrap();
    store.create_repository("demo").unwrap();
    store.enable_external_checks().unwrap();
    let owner = LandingOwner([21; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    for (token, passed) in [("passing-check", true), ("failing-check", false)] {
        let input = landing_request(base, source, dest, token);
        store.begin_landing("demo", owner, &input, 0).unwrap();
        let prepared = store.prepare_landing("demo", owner, token, 1).unwrap();
        let identity = landing_identity(&prepared);
        let checked = store
            .record_landing_check(
                "demo",
                owner,
                token,
                &identity,
                LandingVerdict {
                    passed,
                    evidence: [2; 32],
                },
                2,
            )
            .unwrap();
        assert!(
            store
                .record_landing_check_unavailable("demo", owner, token, &identity, 3)
                .is_err()
        );
        assert_eq!(
            store.landing_job("demo", owner, token).unwrap().unwrap(),
            checked
        );
    }
    let input = landing_request(base, source, dest, "due");
    store.begin_landing("demo", owner, &input, 0).unwrap();
    let prepared = store.prepare_landing("demo", owner, "due", 1).unwrap();
    let expired = store
        .record_landing_check_unavailable(
            "demo",
            owner,
            "due",
            &landing_identity(&prepared),
            input.expires_at,
        )
        .unwrap();
    assert_eq!(expired.state, LandingState::Expired);
    assert_eq!(expired.expires_at, input.expires_at);
    assert!(!expired.roots_retained);
    assert!(expired.receipt.is_none());
    assert_eq!(
        store
            .references("demo", std::slice::from_ref(&input.reference_name))
            .unwrap()[0]
            .commit,
        Some(dest)
    );
}

#[test]
fn landing_unavailable_fits_legacy_prepared_record_and_exact_catalog_limits() {
    let directory = external_layout_directory();
    let store = Store::open(directory.path(), config()).unwrap();
    store.create_repository("demo").unwrap();
    store.enable_external_checks().unwrap();
    let owner = LandingOwner([22; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let token = "u".repeat(128);
    let mut input = landing_request(base, source, dest, &token);
    input.committer.name = "n".repeat(1024);
    input.committer.email = "e".repeat(1024);
    input.check_profile.name = "p".repeat(128);
    input.check_profile.version = "v".repeat(128);
    let accepted = store.begin_landing("demo", owner, &input, 0).unwrap();
    let prepared = store.prepare_landing("demo", owner, &token, 1).unwrap();
    let identity = landing_identity(&prepared);
    let physical = key("demo", &accepted.id);
    let bytes = store
        .inner
        .db
        .read_tx()
        .get(&store.inner.spaces.landing, &physical)
        .unwrap()
        .unwrap();
    let record_bytes = bytes.len();
    let catalog_bytes = physical.len() + record_bytes;
    assert!(
        record_bytes >= 4096,
        "fixture reaches the supported exact record boundary"
    );
    drop(bytes);
    drop(store);
    let mut limits = config();
    limits.landing.max_record_bytes = record_bytes;
    limits.max_retention_validation_bytes = catalog_bytes as u64;
    let store = Store::open_existing(directory.path(), limits.clone()).unwrap();
    let unavailable = store
        .record_landing_check_unavailable("demo", owner, &token, &identity, 2)
        .unwrap();
    assert_eq!(unavailable.state, LandingState::CheckUnavailable);
    assert!(!unavailable.roots_retained);
    assert!(unavailable.receipt.is_none());
    assert!(
        store
            .inner
            .db
            .read_tx()
            .get(&store.inner.spaces.landing, &physical)
            .unwrap()
            .unwrap()
            .len()
            <= record_bytes
    );
    drop(store);
    assert!(Store::open_existing(directory.path(), limits).is_ok());
}

#[test]
fn landing_conflicts_and_unsupported_stacks_are_durable_non_publications() {
    let (directory, store) = open();
    let owner = LandingOwner([14; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let bytes = b"different addition";
    let file = tinytree_model::describe_file(bytes);
    store.put("demo", file, bytes, 0).unwrap();
    let tree = put_object(&store, Object::Tree(tree(file, file.size_bytes)), 0);
    let conflict = landing_commit(
        &store,
        "demo",
        CommitId(base.key.id.0),
        TreeId(tree.key.id.0),
        4,
        "conflicting target",
    );
    store
        .publish("demo", &request("conflicting-target", 1, Some(conflict)), 0)
        .unwrap();
    let mut input = landing_request(base, source, conflict, "conflict");
    input.expected_generation = 2;
    store.begin_landing("demo", owner, &input, 0).unwrap();
    let result = store.prepare_landing("demo", owner, "conflict", 1).unwrap();
    assert_eq!(result.state, LandingState::Conflict);
    assert_eq!(result.conflict_path.as_deref(), Some("source.txt"));
    assert!(result.receipt.is_none());
    assert_eq!(
        store
            .finalize_landing("demo", owner, "conflict", 2)
            .unwrap(),
        result
    );
    let mut merge = commit(TreeId(tree.key.id.0), "unsupported merge");
    merge.parents = vec![CommitId(base.key.id.0), CommitId(dest.key.id.0)];
    merge.change_id = ChangeId([7; 16]);
    let merge = put_object(&store, Object::Commit(merge), 0);
    let mut input = landing_request(base, merge, conflict, "merge");
    input.expected_generation = 2;
    store.begin_landing("demo", owner, &input, 0).unwrap();
    let unsupported = store.prepare_landing("demo", owner, "merge", 1).unwrap();
    assert_eq!(unsupported.state, LandingState::Unsupported);
    assert!(unsupported.receipt.is_none());
    assert_eq!(
        store.references("demo", &[input.reference_name]).unwrap()[0].commit,
        Some(conflict)
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store
            .landing_job("demo", owner, "conflict")
            .unwrap()
            .unwrap(),
        result
    );
    assert_eq!(
        store.landing_job("demo", owner, "merge").unwrap().unwrap(),
        unsupported
    );
}

#[test]
fn landing_pending_discovery_resumes_after_restart_and_progresses_over_terminal_rows() {
    let (directory, store) = open();
    let owner = LandingOwner([15; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let mut expected = HashSet::new();
    for index in 0..12 {
        let token = format!("discovery-{index}");
        let job = store
            .begin_landing(
                "demo",
                owner,
                &landing_request(base, source, dest, &token),
                0,
            )
            .unwrap();
        if index < 3 {
            expected.insert(job.id);
            if index > 0 {
                let prepared = store.prepare_landing("demo", owner, &token, 1).unwrap();
                if index == 2 {
                    store
                        .record_landing_check(
                            "demo",
                            owner,
                            &token,
                            &landing_identity(&prepared),
                            LandingVerdict {
                                passed: true,
                                evidence: [3; 32],
                            },
                            2,
                        )
                        .unwrap();
                }
            }
        } else {
            store.expire_landing("demo", owner, &token, 100).unwrap();
        }
    }
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    store.create_repository("other").unwrap();
    let mut cursor = None;
    let mut found = HashSet::new();
    let mut terminal_page = false;
    let mut handles = Vec::new();
    let limits = LandingScanLimits {
        max_records: 1,
        max_jobs: 1,
        ..Default::default()
    };
    loop {
        let page = store
            .scan_pending_landings("demo", cursor.as_ref(), limits)
            .unwrap();
        assert!(page.scanned_records <= 1);
        assert!(page.scanned_bytes <= limits.max_scan_bytes);
        if page.jobs.is_empty() && page.scanned_records == 1 {
            terminal_page = true;
        }
        for pending in page.jobs {
            assert!(found.insert(pending.id));
            let job = pending.handle.status(&store).unwrap();
            assert_eq!(pending.id, job.id);
            assert_eq!(pending.state, job.state);
            assert!(!format!("{:?}", pending.handle).contains("discovery-"));
            handles.push(pending.handle);
        }
        if page.scan_complete {
            break;
        }
        let next = page.next.expect("bounded scan must progress");
        assert!(
            store
                .scan_pending_landings("other", Some(&next), limits)
                .is_err()
        );
        cursor = Some(next);
    }
    assert_eq!(found, expected);
    assert!(terminal_page);
    assert_eq!(handles.len(), 3);
    let first = store.scan_pending_landings("demo", None, limits).unwrap();
    let old_cursor = first.next.unwrap();
    let handle = handles.remove(0);
    let other_directory = tempfile::tempdir().unwrap();
    let other = Store::open(other_directory.path(), config()).unwrap();
    other.create_repository("demo").unwrap();
    assert!(handle.status(&other).is_err());
    assert!(
        other
            .scan_pending_landings("demo", Some(&old_cursor), limits)
            .is_err()
    );
    drop(other);
    for handle in &handles {
        let job = handle.status(&store).unwrap();
        if job.state == LandingState::Intent {
            assert_eq!(
                handle.prepare(&store, 1).unwrap().state,
                LandingState::Prepared
            );
        }
    }
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert!(handle.status(&store).is_err());
    assert!(
        store
            .scan_pending_landings("demo", Some(&old_cursor), limits)
            .is_err()
    );
    assert!(
        store
            .scan_pending_landings("other", None, limits)
            .unwrap()
            .scan_complete
    );
}

#[test]
fn landing_pending_pages_do_not_skip_unreturned_live_jobs_at_output_boundaries() {
    let (_directory, store) = open();
    let owner = LandingOwner([16; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let mut expected = HashSet::new();
    for index in 0..3 {
        let job = store
            .begin_landing(
                "demo",
                owner,
                &landing_request(base, source, dest, &format!("output-{index}")),
                0,
            )
            .unwrap();
        expected.insert(job.id);
    }
    let limits = LandingScanLimits {
        max_output_bytes: 512,
        ..Default::default()
    };
    let mut cursor = None;
    let mut found = HashSet::new();
    loop {
        let page = store
            .scan_pending_landings("demo", cursor.as_ref(), limits)
            .unwrap();
        assert!(page.jobs.len() <= 1);
        for pending in page.jobs {
            assert!(found.insert(pending.id));
        }
        if page.scan_complete {
            break;
        }
        cursor = page.next;
        assert!(matches!(
            store.scan_pending_landings(
                "demo",
                cursor.as_ref(),
                LandingScanLimits {
                    max_output_bytes: 1,
                    ..limits
                }
            ),
            Err(Error::Limit("landing scan output bytes"))
        ));
    }
    assert_eq!(found, expected);
    assert!(matches!(
        store.scan_pending_landings(
            "demo",
            None,
            LandingScanLimits {
                max_scan_bytes: 1,
                ..limits
            }
        ),
        Err(Error::Limit("landing scan row bytes"))
    ));
    assert!(
        store
            .scan_pending_landings(
                "demo",
                None,
                LandingScanLimits {
                    max_records: 0,
                    ..limits
                }
            )
            .is_err()
    );
}

#[test]
fn landing_check_owner_survives_expiry_without_authorizing_a_late_verdict() {
    let (_directory, store) = open();
    let owner = LandingOwner([17; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "check-owner");
    store.begin_landing("demo", owner, &input, 0).unwrap();
    let prepared = store
        .prepare_landing("demo", owner, "check-owner", 1)
        .unwrap();
    let lease = store
        .pin_landing_candidate("demo", owner, "check-owner", 2)
        .unwrap();
    assert_eq!(lease.identity(), &landing_identity(&prepared));
    let key = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(lease.identity().candidate.0),
    };
    store
        .expire_landing("demo", owner, "check-owner", 100)
        .unwrap();
    store.gc("demo", 1000).unwrap();
    assert!(store.read("demo", key).is_ok());
    assert!(
        store
            .record_landing_check(
                "demo",
                owner,
                "check-owner",
                lease.identity(),
                LandingVerdict {
                    passed: true,
                    evidence: [3; 32]
                },
                101
            )
            .is_err()
    );
    assert_eq!(
        store
            .finalize_landing("demo", owner, "check-owner", 101)
            .unwrap()
            .state,
        LandingState::Expired
    );
    drop(lease);
    store.gc("demo", 1000).unwrap();
    assert!(matches!(
        store.read("demo", key),
        Err(Error::MissingObject(_))
    ));
    assert!(
        store
            .pin_landing_candidate("demo", owner, "check-owner", 0)
            .is_err()
    );
}

#[test]
fn landing_committed_recovery_roots_survive_lost_ack_reference_move_gc_and_restart() {
    let (directory, store) = open();
    let owner = LandingOwner([18; 32]);
    let (base, source, dest) = landing_fixture(&store, "demo");
    let input = landing_request(base, source, dest, "lost-ack");
    store.begin_landing("demo", owner, &input, 0).unwrap();
    let prepared = store.prepare_landing("demo", owner, "lost-ack", 1).unwrap();
    let identity = landing_identity(&prepared);
    store
        .record_landing_check(
            "demo",
            owner,
            "lost-ack",
            &identity,
            LandingVerdict {
                passed: true,
                evidence: [9; 32],
            },
            2,
        )
        .unwrap();
    // Discard the final result, as if the durable ACK were lost in transit.
    store
        .finalize_landing("demo", owner, "lost-ack", 3)
        .unwrap();
    store
        .publish("demo", &request("disjoint-move", 2, Some(base)), 4)
        .unwrap();
    assert!(store.expire_landing("demo", owner, "lost-ack", 99).is_err());
    store.gc("demo", 1000).unwrap();
    assert!(store.read("demo", source.key).is_ok());
    let candidate = ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(identity.candidate.0),
    };
    assert!(store.read("demo", candidate).is_ok());
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    let replay = store.begin_landing("demo", owner, &input, 5000).unwrap();
    assert_eq!(replay.state, LandingState::Committed);
    assert!(replay.roots_retained);
    assert_eq!(replay.receipt.as_ref().unwrap().reference.generation, 2);
    let admission = store.pin_root("demo", candidate).unwrap();
    let (_, bytes) = store.read_metadata("demo", candidate).unwrap();
    let Object::Commit(commit) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else {
        panic!()
    };
    assert_eq!(commit.root_tree.terms, [identity.root_tree]);
    assert_eq!(
        store
            .scan_pending_landings("demo", None, LandingScanLimits::default())
            .unwrap()
            .jobs[0]
            .state,
        LandingState::Committed
    );
    drop(admission);
    let released = store
        .expire_landing("demo", owner, "lost-ack", 5000)
        .unwrap();
    assert_eq!(released.state, LandingState::Committed);
    assert!(!released.roots_retained);
    assert_eq!(released.receipt, replay.receipt);
    store.gc("demo", 5000).unwrap();
    assert!(matches!(
        store.read("demo", candidate),
        Err(Error::MissingObject(_))
    ));
    assert!(matches!(
        store.read("demo", source.key),
        Err(Error::MissingObject(_))
    ));
    assert_eq!(
        store
            .finalize_landing("demo", owner, "lost-ack", 5000)
            .unwrap(),
        released
    );
    assert_eq!(
        store.references("demo", &[input.reference_name]).unwrap()[0].generation,
        3
    );
    assert!(
        store
            .scan_pending_landings("demo", None, LandingScanLimits::default())
            .unwrap()
            .jobs
            .is_empty()
    );
}

#[test]
#[ignore = "manual authoritative Store phase benchmark"]
fn benchmark_landing_store_phases_with_verified_file_closures() {
    use std::time::Instant;
    fn inventory(store: &Store) -> Vec<(u8, usize)> {
        [
            Kind::File,
            Kind::Tree,
            Kind::Commit,
            Kind::Symlink,
            Kind::CopyHistory,
        ]
        .into_iter()
        .map(|kind| {
            let mut count = 0;
            let mut after = None;
            loop {
                let page = store.list_objects("demo", kind, after, 1024).unwrap();
                count += page.len();
                if page.len() < 1024 {
                    break;
                }
                after = page.last().map(|d| d.key.id);
            }
            (kind as u8, count)
        })
        .collect()
    }
    for files in [64, 1024] {
        for trial in 0..3 {
            let (_directory, store) = open();
            let mut values = Vec::new();
            for index in 0..files {
                let mut bytes = vec![0u8; 4096];
                bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
                let d = tinytree_model::describe_file(&bytes);
                store.put("demo", d, &bytes, 0).unwrap();
                values.push(TreeEntry {
                    name: format!("file-{index:04}"),
                    value: TreeValue::File {
                        id: FileId(d.key.id.0),
                        executable: false,
                        size_bytes: 4096,
                        copy_id: None,
                    },
                });
            }
            let base_tree = put_object(
                &store,
                Object::Tree(Tree {
                    entries: values.clone(),
                }),
                0,
            );
            let base = landing_commit(
                &store,
                "demo",
                CommitId::ROOT,
                TreeId(base_tree.key.id.0),
                1,
                "benchmark base",
            );
            let changed = |index: usize, marker: u8| {
                let mut bytes = vec![0u8; 4096];
                bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
                bytes[4095] = marker;
                let d = tinytree_model::describe_file(&bytes);
                store.put("demo", d, &bytes, 0).unwrap();
                TreeValue::File {
                    id: FileId(d.key.id.0),
                    executable: false,
                    size_bytes: 4096,
                    copy_id: None,
                }
            };
            let mut first_entries = values.clone();
            first_entries[0].value = changed(0, 1);
            let first_tree = put_object(
                &store,
                Object::Tree(Tree {
                    entries: first_entries.clone(),
                }),
                0,
            );
            let first = landing_commit(
                &store,
                "demo",
                CommitId(base.key.id.0),
                TreeId(first_tree.key.id.0),
                2,
                "first source",
            );
            let mut second_entries = first_entries.clone();
            second_entries[1].value = changed(1, 2);
            let second_tree = put_object(
                &store,
                Object::Tree(Tree {
                    entries: second_entries.clone(),
                }),
                0,
            );
            let second = landing_commit(
                &store,
                "demo",
                CommitId(first.key.id.0),
                TreeId(second_tree.key.id.0),
                3,
                "second source",
            );
            let destination_value = changed(2, 3);
            let mut destination_entries = values.clone();
            destination_entries[2].value = destination_value.clone();
            let destination_tree = put_object(
                &store,
                Object::Tree(Tree {
                    entries: destination_entries,
                }),
                0,
            );
            let dest = landing_commit(
                &store,
                "demo",
                CommitId(base.key.id.0),
                TreeId(destination_tree.key.id.0),
                4,
                "independent destination",
            );
            store
                .publish("demo", &request("benchmark-initial", 0, Some(dest)), 0)
                .unwrap();
            let owner = LandingOwner([19; 32]);
            let input = landing_request(base, second, dest, "benchmark");
            first_entries[2].value = destination_value.clone();
            second_entries[2].value = destination_value;
            let expected_first = tinytree_model::encode(
                &Object::Tree(Tree {
                    entries: first_entries,
                }),
                Limits::default(),
            )
            .unwrap();
            let expected_second = tinytree_model::encode(
                &Object::Tree(Tree {
                    entries: second_entries,
                }),
                Limits::default(),
            )
            .unwrap();
            let expected_commit = |source: Descriptor, parent: CommitId, tree: TreeId| {
                let (_, bytes) = store.read_metadata("demo", source.key).unwrap();
                let Object::Commit(mut c) =
                    tinytree_model::decode(&bytes, Limits::default()).unwrap()
                else {
                    panic!()
                };
                c.parents = vec![parent];
                c.root_tree.terms = vec![tree];
                c.committer = input.committer.clone();
                tinytree_model::encode(&Object::Commit(c), Limits::default()).unwrap()
            };
            let expected_first_commit = expected_commit(
                first,
                CommitId(dest.key.id.0),
                TreeId(expected_first.descriptor.key.id.0),
            );
            let expected_second_commit = expected_commit(
                second,
                CommitId(expected_first_commit.descriptor.key.id.0),
                TreeId(expected_second.descriptor.key.id.0),
            );
            let before = inventory(&store);
            let start = Instant::now();
            let accepted = store.begin_landing("demo", owner, &input, 0).unwrap();
            let begin_ns = start.elapsed().as_nanos();
            assert_eq!(accepted.state, LandingState::Intent);
            let start = Instant::now();
            let prepared = store
                .prepare_landing("demo", owner, "benchmark", 1)
                .unwrap();
            let prepare_ns = start.elapsed().as_nanos();
            let candidate = prepared.candidate.as_ref().unwrap();
            assert_eq!(candidate.head.0, expected_second_commit.descriptor.key.id.0);
            assert_eq!(candidate.root_tree.0, expected_second.descriptor.key.id.0);
            assert_eq!(
                candidate
                    .rewrites
                    .iter()
                    .map(|r| r.change_id)
                    .collect::<Vec<_>>(),
                [ChangeId([2; 16]), ChangeId([3; 16])]
            );
            let identity = landing_identity(&prepared);
            let start = Instant::now();
            store
                .record_landing_check(
                    "demo",
                    owner,
                    "benchmark",
                    &identity,
                    LandingVerdict {
                        passed: true,
                        evidence: [1; 32],
                    },
                    2,
                )
                .unwrap();
            let check_ns = start.elapsed().as_nanos();
            let start = Instant::now();
            let done = store
                .finalize_landing("demo", owner, "benchmark", 3)
                .unwrap();
            let finalize_ns = start.elapsed().as_nanos();
            let receipt = done.receipt.as_ref().unwrap();
            assert_eq!(receipt.rewrites.len(), 2);
            assert_eq!(
                receipt.reference.commit.unwrap().key.id.0,
                identity.candidate.0
            );
            assert_eq!(
                store.references("demo", &[input.reference_name]).unwrap()[0],
                receipt.reference
            );
            let after = inventory(&store);
            assert_eq!(before[0], after[0]);
            assert_eq!(after[1].1 - before[1].1, 2);
            assert_eq!(after[2].1 - before[2].1, 2);
            println!(
                "TINYTREE_LANDING_STORE_PHASE_V1 {}",
                serde_json::json!({"files":files,"file_bytes":4096,"trial":trial,"begin_ns":begin_ns,"prepare_ns":prepare_ns,"record_check_ns":check_ns,"finalize_ns":finalize_ns,"logical_strict_file_closure_bytes":(files+3)*4096,"builder":{"metadata_reads":candidate.stats.metadata_reads,"metadata_bytes":candidate.stats.metadata_bytes,"work":candidate.stats.work,"encoded_output_bytes":candidate.stats.output_bytes},"inventory_before":before,"inventory_after":after,"scope":"authoritative Store component; setup excluded; strict FILE verification enabled; check verdict fixture only; not RPC, runner, WAN, or IO-byte measurement"})
            );
        }
    }
}

// Private layout acceptance uses a trusted ancestor, not a test-runner scratch
// directory whose permissions are deliberately broader than this capability.
fn external_layout_directory() -> tempfile::TempDir {
    #[cfg(unix)]
    {
        tempfile::Builder::new()
            .prefix("tinytree-layout-")
            .tempdir_in("/tmp")
            .unwrap()
    }
    #[cfg(not(unix))]
    {
        tempfile::tempdir().unwrap()
    }
}
fn external_layout_format(root: &Path, format: &[u8]) {
    let db = SingleWriterTxDatabase::builder(root.join("metadata"))
        .open()
        .unwrap();
    let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
    let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
    tx.insert(&meta, b"format", format);
    tx.commit().unwrap();
}
#[test]
fn external_layout_activation_is_private_durable_idempotent_and_server_gc_only() {
    let directory = external_layout_directory();
    let store = Store::open(directory.path(), config()).unwrap();
    store.create_repository("demo").unwrap();
    assert_eq!(store.external_checks_directory().unwrap(), None);
    let handles = [store.clone(), store.clone()];
    let workers =
        handles.map(|store| std::thread::spawn(move || store.enable_external_checks().unwrap()));
    let [a, b] = workers.map(|worker| worker.join().unwrap());
    assert_eq!(a, b);
    assert_eq!(a, directory.path().join("external-checks"));
    assert_eq!(
        fs::read(a.join(".header.json")).unwrap(),
        b"{\"format\":\"tinytree-external-checks\",\"version\":1}\n"
    );
    assert_eq!(fs::metadata(a.join("journal.lock")).unwrap().len(), 0);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for name in ["", "jobs", "binaries", ".header.json", "journal.lock"] {
            assert_eq!(
                fs::metadata(a.join(name)).unwrap().permissions().mode() & 0o077,
                0
            );
        }
    }
    assert!(matches!(
        store.plan_client_gc("demo", &client_collection(&store, 100)),
        Err(Error::Invalid(
            "authoritative service state requires server GC"
        ))
    ));
    store.gc("demo", 100).unwrap();
    drop(store);
    let reopened = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(reopened.external_checks_directory().unwrap(), Some(a));
}
#[test]
fn external_layout_activation_preserves_feature_six_landing_receipts() {
    let directory = external_layout_directory();
    let store = Store::open(directory.path(), config()).unwrap();
    store.create_repository("demo").unwrap();
    let (base, source, dest) = landing_fixture(&store, "demo");
    let owner = LandingOwner([1; 32]);
    store
        .begin_landing(
            "demo",
            owner,
            &landing_request(base, source, dest, "layout"),
            0,
        )
        .unwrap();
    let prepared = store.prepare_landing("demo", owner, "layout", 1).unwrap();
    let identity = landing_identity(&prepared);
    store
        .record_landing_check(
            "demo",
            owner,
            "layout",
            &identity,
            LandingVerdict {
                passed: true,
                evidence: [9; 32],
            },
            2,
        )
        .unwrap();
    let before = store.finalize_landing("demo", owner, "layout", 3).unwrap();
    assert_eq!(before.state, LandingState::Committed);
    let receipt = before.receipt.as_ref().unwrap();
    assert_eq!(receipt.evidence, [9; 32]);
    assert_eq!(receipt.rewrites.len(), 1);
    assert_eq!(receipt.rewrites[0].old, CommitId(source.key.id.0));
    assert_eq!(receipt.rewrites[0].new, identity.candidate);
    store.enable_external_checks().unwrap();
    drop(store);
    let reopened = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        reopened
            .landing_job("demo", owner, "layout")
            .unwrap()
            .unwrap(),
        before
    );
    assert!(reopened.external_checks_directory().unwrap().is_some());
}
#[test]
fn external_layout_reopen_refuses_corruption_without_repair() {
    struct Case {
        name: &'static str,
        damage: fn(&Path),
    }
    for case in [
        Case {
            name: "missing header",
            damage: |p| fs::remove_file(p.join(".header.json")).unwrap(),
        },
        Case {
            name: "unknown version",
            damage: |p| {
                fs::write(
                    p.join(".header.json"),
                    b"{\"format\":\"tinytree-external-checks\",\"version\":2}\n",
                )
                .unwrap()
            },
        },
        Case {
            name: "oversized header",
            damage: |p| fs::write(p.join(".header.json"), vec![b'x'; 129]).unwrap(),
        },
        Case {
            name: "missing child",
            damage: |p| fs::remove_dir(p.join("jobs")).unwrap(),
        },
        Case {
            name: "nonempty lock",
            damage: |p| fs::write(p.join("journal.lock"), b"x").unwrap(),
        },
        Case {
            name: "unknown entry",
            damage: |p| fs::write(p.join("unexpected"), b"").unwrap(),
        },
        Case {
            name: "missing layout",
            damage: |p| fs::remove_dir_all(p).unwrap(),
        },
    ] {
        let directory = external_layout_directory();
        let store = Store::open(directory.path(), config()).unwrap();
        let path = store.enable_external_checks().unwrap();
        drop(store);
        (case.damage)(&path);
        assert!(
            Store::open_existing(directory.path(), config()).is_err(),
            "{}",
            case.name
        );
    }
}
#[test]
fn external_layout_complete_empty_prefix_recovers_but_unmarked_journals_do_not() {
    for populated in [false, true] {
        let directory = external_layout_directory();
        let store = Store::open(directory.path(), config()).unwrap();
        let path = store.enable_external_checks().unwrap();
        drop(store);
        external_layout_format(directory.path(), b"tinytree-store-6");
        if populated {
            fs::write(path.join("jobs/attempt"), b"not dispatched legally").unwrap();
        }
        let reopened = Store::open_existing(directory.path(), config());
        if populated {
            assert!(matches!(
                reopened,
                Err(Error::Corrupt("external check data precedes Store feature"))
            ));
        } else {
            let store = reopened.unwrap();
            assert_eq!(store.external_checks_directory().unwrap(), None);
            assert_eq!(store.enable_external_checks().unwrap(), path);
            drop(store);
            assert!(
                Store::open_existing(directory.path(), config())
                    .unwrap()
                    .external_checks_directory()
                    .unwrap()
                    .is_some()
            );
        }
    }
    let directory = external_layout_directory();
    let store = Store::open(directory.path(), config()).unwrap();
    fs::create_dir(directory.path().join("external-checks")).unwrap();
    assert!(store.enable_external_checks().is_err());
    assert!(
        !directory
            .path()
            .join("external-checks/.header.json")
            .exists()
    );
    drop(store);
    assert!(Store::open_existing(directory.path(), config()).is_err());
}
#[cfg(unix)]
#[test]
fn external_layout_rejects_symlinks_broad_permissions_and_foreign_ownership() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for target in ["", "jobs", ".header.json", "journal.lock"] {
        let directory = external_layout_directory();
        let store = Store::open(directory.path(), config()).unwrap();
        let path = store.enable_external_checks().unwrap();
        drop(store);
        let target = if target.is_empty() {
            path
        } else {
            path.join(target)
        };
        let saved = directory.path().join("saved");
        fs::rename(&target, &saved).unwrap();
        symlink(&saved, &target).unwrap();
        assert!(Store::open_existing(directory.path(), config()).is_err());
        assert!(
            fs::symlink_metadata(&target)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }
    for target in ["", "jobs", ".header.json"] {
        let directory = external_layout_directory();
        let store = Store::open(directory.path(), config()).unwrap();
        let path = store.enable_external_checks().unwrap().join(target);
        drop(store);
        let mode = fs::metadata(&path).unwrap().permissions().mode() | 0o040;
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        assert!(Store::open_existing(directory.path(), config()).is_err());
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            mode & 0o777
        );
    }
    if unsafe { libc::geteuid() } == 0 {
        use std::os::unix::ffi::OsStrExt;
        let directory = external_layout_directory();
        let store = Store::open(directory.path(), config()).unwrap();
        let header = store.enable_external_checks().unwrap().join(".header.json");
        drop(store);
        let name = std::ffi::CString::new(header.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::chown(name.as_ptr(), 1, u32::MAX) }, 0);
        assert!(Store::open_existing(directory.path(), config()).is_err());
    }
}

#[test]
fn latest_capture_uses_logical_sequence_without_changing_publication_order() {
    let (directory, mut store) = open();
    let owner = CloudOwner {
        principal: [91; 32],
    };
    let (_, root, head) = fixture(&store, "logical capture selection");
    let (mut history, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![],
        },
    );
    let workspace = history.recovery.workspace;
    let deadline = || std::time::Instant::now() + Duration::from_secs(30);
    assert!(
        store
            .read_latest_captured_cloud_version(
                "demo",
                owner,
                workspace,
                Default::default(),
                deadline()
            )
            .unwrap()
            .is_none()
    );
    let mut previous = None;
    for (generation, sequence, expected) in [(1, 10, 1), (2, 8, 1), (3, 11, 3), (4, 11, 4)] {
        history.recovery.sequence = sequence;
        // Repeated logical captures may differ only in publication parents.
        history.recovery.parents = previous.into_iter().collect();
        let request = captured_cloud_request(
            &store,
            owner,
            history.clone(),
            if generation == 1 {
                leases.clone()
            } else {
                vec![]
            },
            &artifacts,
            generation - 1,
            &format!("logical-{generation}"),
        );
        let receipt = store
            .publish_captured_cloud_version("demo", owner, &request, 1, &Default::default())
            .unwrap();
        assert_eq!(receipt.workspace.generation, generation);
        assert_eq!(
            store
                .get_cloud_workspace("demo", owner, workspace)
                .unwrap()
                .generation,
            generation
        );
        let selected = store
            .read_latest_captured_cloud_version(
                "demo",
                owner,
                workspace,
                Default::default(),
                deadline(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(selected.version().generation, expected);
        drop(selected);
        assert_eq!(
            store
                .cloud_publication("demo", owner, &format!("logical-{generation}"))
                .unwrap(),
            Some(receipt)
        );
        previous = Some(request.publication.manifest);
    }
    let foreign = CloudOwner {
        principal: [92; 32],
    };
    assert!(
        store
            .read_latest_captured_cloud_version(
                "demo",
                foreign,
                workspace,
                Default::default(),
                deadline()
            )
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store.read_latest_captured_cloud_version(
            "demo",
            owner,
            workspace,
            Default::default(),
            std::time::Instant::now()
        ),
        Err(Error::Limit("latest capture deadline"))
    ));
    let mut small = tinytree_cloud::ManifestLimits::default();
    small.max_manifest_bytes = 1;
    assert!(
        store
            .read_latest_captured_cloud_version("demo", owner, workspace, small, deadline())
            .is_err()
    );
    // Tighten only this isolated fixture's scan policy after valid publication:
    // production reopening validates its catalog before serving queries.
    let original = store.inner.config.clone();
    Arc::get_mut(&mut store.inner)
        .unwrap()
        .config
        .max_retention_validation_records = 2;
    assert!(matches!(
        store.read_latest_captured_cloud_version(
            "demo",
            owner,
            workspace,
            Default::default(),
            deadline()
        ),
        Err(Error::Limit("cloud catalog scan"))
    ));
    Arc::get_mut(&mut store.inner).unwrap().config = original.clone();
    Arc::get_mut(&mut store.inner)
        .unwrap()
        .config
        .max_retention_validation_bytes = 1;
    assert!(matches!(
        store.read_latest_captured_cloud_version(
            "demo",
            owner,
            workspace,
            Default::default(),
            deadline()
        ),
        Err(Error::Limit("cloud catalog scan"))
    ));
    Arc::get_mut(&mut store.inner).unwrap().config = original.clone();
    Arc::get_mut(&mut store.inner)
        .unwrap()
        .config
        .cloud
        .max_versions_per_workspace = 3;
    assert!(matches!(
        store.read_latest_captured_cloud_version(
            "demo",
            owner,
            workspace,
            Default::default(),
            deadline()
        ),
        Err(Error::Limit("latest capture version count"))
    ));
    Arc::get_mut(&mut store.inner).unwrap().config = original;
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    assert_eq!(
        store
            .read_latest_captured_cloud_version(
                "demo",
                owner,
                workspace,
                Default::default(),
                deadline()
            )
            .unwrap()
            .unwrap()
            .version()
            .generation,
        4
    );
}

#[test]
fn latest_capture_refuses_ambiguous_bindings_but_explicit_generations_remain_readable() {
    let (_directory, store) = open();
    let owner = CloudOwner {
        principal: [91; 32],
    };
    let (_, root, head) = fixture(&store, "ambiguous capture selection");
    let (mut history, leases, artifacts) = cloud_v2_capture(
        &store,
        owner,
        head,
        root,
        tinytree_cloud::PortableIndexSnapshot {
            roots: vec![CommitId(head.key.id.0)],
            drafts: vec![],
        },
    );
    let workspace = history.recovery.workspace;
    let deadline = || std::time::Instant::now() + Duration::from_secs(30);
    for generation in 1..=4 {
        history.recovery.sequence = if generation <= 2 { 10 } else { 11 };
        if generation == 4 {
            history.recovery.exporter = [43; 16];
        }
        let mut request = captured_cloud_request(
            &store,
            owner,
            history.clone(),
            if generation == 1 {
                leases.clone()
            } else {
                vec![]
            },
            &artifacts,
            generation - 1,
            &format!("ambiguity-{generation}"),
        );
        if generation == 2 {
            let mut data = tinytree_cloud::decode_data(
                &request.capture_data,
                tinytree_cloud::CaptureDataId::of(&request.capture_data).0,
                &Default::default(),
            )
            .unwrap();
            data.remote = Some(tinytree_cloud::PortableRemoteState {
                observations: vec![],
                archived_pushes: vec![],
                landing: None,
            });
            request.capture_data =
                tinytree_cloud::encode_capture_data(&data, Default::default()).unwrap();
            let mut manifest = tinytree_cloud::decode_manifest_v3(
                &request.publication.manifest_bytes,
                request.publication.manifest,
                Default::default(),
            )
            .unwrap();
            manifest.capture = tinytree_cloud::CaptureDescriptor::of(&request.capture_data);
            request.publication.manifest_bytes =
                tinytree_cloud::encode_manifest_v3(&manifest, Default::default()).unwrap();
            request.publication.manifest =
                tinytree_cloud::ManifestId::of(&request.publication.manifest_bytes);
        }
        store
            .publish_captured_cloud_version("demo", owner, &request, 1, &Default::default())
            .unwrap();
        let selected = store.read_latest_captured_cloud_version(
            "demo",
            owner,
            workspace,
            Default::default(),
            deadline(),
        );
        match generation {
            1 | 3 => assert_eq!(selected.unwrap().unwrap().version().generation, generation),
            2 => assert!(matches!(
                selected,
                Err(Error::Invalid("ambiguous latest capture sequence"))
            )),
            4 => assert!(matches!(
                selected,
                Err(Error::Invalid("ambiguous latest capture exporter"))
            )),
            _ => unreachable!(),
        }
        assert_eq!(
            store
                .read_cloud_version("demo", owner, workspace, generation, Default::default())
                .unwrap()
                .version()
                .generation,
            generation
        );
    }
}

#[test]
fn latest_capture_does_not_select_history_only_versions() {
    let (_directory, store) = open();
    let owner = CloudOwner {
        principal: [91; 32],
    };
    let (_, root, head) = fixture(&store, "history only selection");
    let (manifest, leases) = cloud_capture(&store, owner, head, root);
    let request = cloud_request(&manifest, 0, "history-only", &leases);
    store
        .publish_cloud_version("demo", owner, &request, 1, &Default::default())
        .unwrap();
    assert!(
        store
            .read_latest_captured_cloud_version(
                "demo",
                owner,
                manifest.workspace,
                Default::default(),
                std::time::Instant::now() + Duration::from_secs(30)
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .read_cloud_version("demo", owner, manifest.workspace, 1, Default::default())
            .unwrap()
            .version()
            .generation,
        1
    );
}

#[test]
fn forge_review_concurrent_product_lands_have_one_winner_and_durable_stale_replay() {
    let (directory, store, account, writer, reviewer, view, base) = forge_review_fixture();
    let repo = account.repository_id();
    let observed = store.references(&repo, &["main".into()]).unwrap().remove(0);
    let mut reviews = Vec::new();
    for (token, path, content, change) in [
        ("race-first", "first.txt", b"first".as_slice(), 41),
        ("race-second", "second.txt", b"second".as_slice(), 42),
    ] {
        let head = forge_review_source(&store, &account, base, &[(path, content)], change);
        let review = store
            .forge_push_review(
                &account,
                &writer,
                &forge_review_request(base, head, view, token),
                &forge_review_signature(),
                1,
            )
            .unwrap();
        let candidate = review.candidate.as_ref().unwrap();
        assert_eq!(candidate.target, observed.commit.unwrap().key.id.0);
        assert_eq!(candidate.target_generation, observed.generation);
        forge_review_check(&store, &account, &review);
        let approved = store
            .forge_approve_review(
                &account,
                &reviewer,
                ChangeId(review.change_id),
                review.patchset_id,
                candidate.id,
                &format!("approve-{token}"),
                2,
            )
            .unwrap();
        assert_eq!(approved.state, ForgeReviewState::ReadyToLand);
        reviews.push((token.to_owned(), review));
    }
    assert_ne!(
        reviews[0].1.candidate.as_ref().unwrap().head,
        reviews[1].1.candidate.as_ref().unwrap().head
    );
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let threads = reviews
        .iter()
        .map(|(token, review)| {
            let store = store.clone();
            let account = account.clone();
            let writer = writer.clone();
            let barrier = barrier.clone();
            let review = review.clone();
            let token = format!("land-{token}");
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .forge_land_review_request(
                        &account,
                        &writer,
                        ChangeId(review.change_id),
                        review.patchset_id,
                        review.candidate.unwrap().id,
                        &token,
                        3,
                    )
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let results = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .filter(|review| review.state == ForgeReviewState::Landed)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|review| review.state == ForgeReviewState::Stale)
            .count(),
        1
    );
    let winner = results
        .iter()
        .find(|review| review.state == ForgeReviewState::Landed)
        .unwrap();
    let main = store.references(&repo, &["main".into()]).unwrap().remove(0);
    assert_eq!(
        main.generation,
        observed.generation + 1,
        "only one main publication"
    );
    assert_eq!(
        main.commit.unwrap().key.id.0,
        winner.candidate.as_ref().unwrap().head
    );
    let (_, bytes) = store
        .read_metadata(&repo, main.commit.unwrap().key)
        .unwrap();
    let Object::Commit(commit) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else {
        panic!("main is not a commit")
    };
    assert_eq!(
        commit.parents,
        vec![CommitId(observed.commit.unwrap().key.id.0)],
        "winner remains single-parent"
    );
    drop(store);
    let store = Store::open_existing(directory.path(), config()).unwrap();
    for ((token, original), expected) in reviews.iter().zip(results.iter()) {
        assert_eq!(
            store
                .forge_review(&account, &writer, ChangeId(original.change_id))
                .unwrap(),
            *expected
        );
        assert_eq!(
            store
                .forge_review_operation(&account, &writer, &format!("land-{token}"))
                .unwrap()
                .unwrap(),
            *expected
        );
        assert_eq!(
            store
                .forge_land_review_request(
                    &account,
                    &writer,
                    ChangeId(original.change_id),
                    original.patchset_id,
                    original.candidate.as_ref().unwrap().id,
                    &format!("land-{token}"),
                    4,
                )
                .unwrap(),
            *expected,
            "exact land retry preserves winner/stale disposition"
        );
    }
    assert_eq!(
        store.references(&repo, &["main".into()]).unwrap().remove(0),
        main,
        "restart and replays never publish again"
    );
}

#[test]
fn forge_scoped_subtree_upload_preserves_nested_projected_copy_context() {
    let directory = tempfile::tempdir().unwrap(); let store = Store::open(directory.path(), config()).unwrap();
    let account = AccountId("nested-upload".into()); let actor = ForgeActor { principal: "owner".into() };
    store.forge_provision_account(&account, &actor).unwrap(); let repo = account.repository_id();
    let old = forge_fixture(&store, &account);
    let (_, bytes) = store.read_metadata(&repo, ObjectKey { kind:Kind::Commit,id:ObjectId(old.0) }).unwrap();
    let Object::Commit(mut canonical) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else { panic!("commit") };
    let wrapped = tinytree_model::encode(&Object::Tree(Tree { entries:vec![TreeEntry { name:"vendor".into(),value:TreeValue::Tree(canonical.root_tree.terms[0]) }] }), Limits::default()).unwrap();
    store.put(&repo, wrapped.descriptor, &wrapped.bytes, 0).unwrap(); canonical.root_tree.terms = vec![TreeId(wrapped.descriptor.key.id.0)];
    let source = tinytree_model::encode(&Object::Commit(canonical), Limits::default()).unwrap(); store.put(&repo, source.descriptor, &source.bytes, 0).unwrap();
    let view = store.forge_publish_view(&account, &actor, forge_public_view(CommitId(source.descriptor.key.id.0)), PolicyEpoch(1), true).unwrap();
    let projection = store.forge_project_view(&view, 0).unwrap();
    let (_, bytes) = store.read_metadata(&repo, ObjectKey { kind:Kind::Tree,id:ObjectId(projection.projected_tree) }).unwrap();
    let Object::Tree(root) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else { panic!("tree") };
    let TreeValue::Tree(vendor) = root.entries[0].value else { panic!("vendor subtree") };
    let (_, bytes) = store.read_metadata(&repo, ObjectKey { kind:Kind::Tree,id:ObjectId(vendor.0) }).unwrap();
    let Object::Tree(mut subtree) = tinytree_model::decode(&bytes, Limits::default()).unwrap() else { panic!("tree") };
    assert!(subtree.entries.iter().any(|entry|matches!(entry.value,TreeValue::File { copy_id:Some(_),.. })));
    let edited = tinytree_model::describe_file(b"edited nested source\n"); store.forge_upload_file(&view, &actor, "vendor/edited.txt", b"edited nested source\n", 0).unwrap();
    subtree.entries.push(TreeEntry { name:"edited.txt".into(),value:TreeValue::File { id:FileId(edited.key.id.0),size_bytes:edited.size_bytes,executable:false,copy_id:None } });subtree.entries.sort_by(|a,b|a.name.cmp(&b.name));
    let subtree = tinytree_model::encode(&Object::Tree(subtree), Limits::default()).unwrap();
    assert!(matches!(store.forge_upload_object(&view,&actor,"wrong",subtree.descriptor,&subtree.bytes,0),Err(Error::ForgeAccessDenied)));
    // Availability of an intermediate TREE is enough: its descendant leaves
    // and copy identities, rather than a TREE receipt, carry scope authority.
    let root = tinytree_model::encode(&Object::Tree(Tree { entries:vec![TreeEntry { name:"vendor".into(),value:TreeValue::Tree(TreeId(subtree.descriptor.key.id.0)) }] }),Limits::default()).unwrap();
    store.forge_upload_object(&view,&actor,"",root.descriptor,&root.bytes,0).unwrap();
    store.forge_upload_object(&view,&actor,"vendor",subtree.descriptor,&subtree.bytes,0).unwrap();
    assert!(matches!(store.inner.closure(&repo,[subtree.descriptor.key],true,false),Err(Error::Invalid("copy history current path"))),"ordinary repository-root validation remains strict");
    let root = tinytree_model::encode(&Object::Tree(Tree { entries:vec![TreeEntry { name:"vendor".into(),value:TreeValue::Tree(TreeId(subtree.descriptor.key.id.0)) }] }),Limits::default()).unwrap();
    store.forge_upload_object(&view,&actor,"",root.descriptor,&root.bytes,0).unwrap();
    let Object::Tree(decoded) = tinytree_model::decode(&subtree.bytes, Limits::default()).unwrap() else { panic!("tree") };
    let visible = decoded.entries.iter().find_map(|entry| match entry.value {
        TreeValue::File { id, size_bytes, .. } if entry.name != "edited.txt" => Some((id,size_bytes)),
        _ => None,
    }).unwrap();
    let paged = tinytree_model::encode(&Object::Tree(Tree { entries:(0..257).map(|i| TreeEntry {
        name:format!("page-{i:03}"), value:TreeValue::File {id:visible.0,size_bytes:visible.1,executable:false,copy_id:None}
    }).collect() }), Limits::default()).unwrap();
    store.forge_upload_object(&view,&actor,"vendor",paged.descriptor,&paged.bytes,0).unwrap();
    let Object::Tree(mut wrong_size) = tinytree_model::decode(&paged.bytes, Limits::default()).unwrap() else { panic!("tree") };
    if let TreeValue::File { size_bytes, .. } = &mut wrong_size.entries[256].value { *size_bytes += 1; }
    let wrong_size = tinytree_model::encode(&Object::Tree(wrong_size), Limits::default()).unwrap();
    assert!(matches!(store.forge_upload_object(&view,&actor,"vendor",wrong_size.descriptor,&wrong_size.bytes,0),Err(Error::Invalid("forge uploaded file size"))),"later pages must enforce the same exact descriptor requirements");
    let mut authored = commit(TreeId(root.descriptor.key.id.0),"edit existing nested directory");authored.parents=vec![CommitId(projection.projected_commit)];
    let authored = tinytree_model::encode(&Object::Commit(authored),Limits::default()).unwrap();store.forge_upload_object(&view,&actor,"",authored.descriptor,&authored.bytes,0).unwrap();
    assert_eq!(store.forge_read_authorized_object(&view,Some(&actor),authored.descriptor.key,0,4096).unwrap(),authored.bytes);
}

