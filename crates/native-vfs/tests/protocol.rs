mod common;
use native_vfs::{prepare, prepare_existing};
use std::{fs, sync::atomic::Ordering};

#[tokio::test]
async fn prepare_is_metadata_only_and_restart_preserves_dirty_files_and_control_identity() {
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    let mut missing = config.clone();
    missing.state = dir.path().join("missing-parent/state");
    assert!(
        prepare_existing(missing.clone(), provider.clone(), cache.clone())
            .await
            .is_err()
    );
    assert!(!missing.state.parent().unwrap().exists());
    assert!(!config.root.exists());
    assert!(!config.state.exists());
    assert_eq!(provider.metadata_reads.load(Ordering::SeqCst), 0);
    let workspace = prepare(config.clone(), provider.clone(), cache.clone())
        .await
        .unwrap();
    let tree = workspace.tree();
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 0);
    assert_eq!(provider.metadata_reads.load(Ordering::SeqCst), 0);
    assert!(
        tree.resolve("tool")
            .unwrap_err()
            .is::<output_core::SourceNotLoaded>()
    );
    assert_eq!(
        tree.resolve_source("tool").await.unwrap().mode & 0o777,
        0o755
    );
    assert_eq!(
        tree.readlink_source(tree.resolve_source("link").await.unwrap().id)
            .await
            .unwrap(),
        "tool"
    );
    let control = tree.resolve_source(".jj/native-workspace").await.unwrap();
    assert_eq!(
        tree.read(control.id, 0, 100).await.unwrap(),
        "03".repeat(32).as_bytes()
    );
    assert!(tree.write(control.id, 0, b"other identity").await.is_err());
    assert!(
        tree.rename(tree.root_id(), ".jj", tree.root_id(), "renamed")
            .await
            .is_err()
    );
    let local = tree.create(tree.root_id(), "new", 0o644).await.unwrap();
    tree.write(local.id, 0, b"new local bytes").await.unwrap();
    let tool = tree.resolve_source("tool").await.unwrap();
    tree.write(tool.id, 0, b"DIRTY").await.unwrap();
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 1);
    let snapshot = workspace.capture_lazy_tree_only().unwrap();
    assert!(
        snapshot
            .entries()
            .iter()
            .all(|e| !e.path.starts_with(".jj"))
    );
    drop(snapshot);
    cache.wait_for_idle().await.unwrap();
    drop(tree);
    drop(workspace);
    let workspace = prepare(config.clone(), provider.clone(), cache.clone())
        .await
        .unwrap();
    let tree = workspace.tree();
    assert_eq!(provider.metadata_reads.load(Ordering::SeqCst), 2);
    assert_eq!(
        tree.read(tree.resolve_source("unused").await.unwrap().id, 0, 100)
            .await
            .unwrap(),
        b"untouched source\n"
    );
    assert_eq!(
        tree.read(tree.resolve_source("new").await.unwrap().id, 0, 100)
            .await
            .unwrap(),
        b"new local bytes"
    );
    assert!(
        tree.read(tree.resolve_source("tool").await.unwrap().id, 0, 100)
            .await
            .unwrap()
            .starts_with(b"DIRTY")
    );
    assert_eq!(
        tree.resolve_source(".jj/native-workspace")
            .await
            .unwrap()
            .id,
        control.id
    );
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 2);
    assert!(
        prepare(config, provider, cache).await.is_err(),
        "a second owner must not open this namespace"
    );
}

#[tokio::test]
async fn pending_exact_bootstrap_replay_does_not_reset_dirty_state() {
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    let workspace = prepare(config.clone(), provider.clone(), cache.clone())
        .await
        .unwrap();
    let tree = workspace.tree();
    let local = tree
        .create(tree.root_id(), "survivor", 0o644)
        .await
        .unwrap();
    tree.write(local.id, 0, b"must survive receipt replay")
        .await
        .unwrap();
    drop(tree);
    drop(workspace);
    // Model the durable tree-install/header-ready crash boundary. This is not
    // a process-kill or machine-power-loss durability claim.
    let state = private_fs::PrivateDir::open_existing(&config.state).unwrap();
    let mut header: serde_json::Value =
        serde_json::from_slice(&state.read("native-vfs.json", 8192).unwrap()).unwrap();
    header["ready"] = false.into();
    state
        .replace("native-vfs.json", &serde_json::to_vec(&header).unwrap())
        .unwrap();
    drop(state);
    let incomplete = fs::read(config.state.join("native-vfs.json")).unwrap();
    assert!(
        prepare_existing(config.clone(), provider.clone(), cache.clone())
            .await
            .is_err()
    );
    assert_eq!(
        fs::read(config.state.join("native-vfs.json")).unwrap(),
        incomplete,
        "strict reopen cannot complete an interrupted bootstrap"
    );
    let workspace = prepare(config, provider.clone(), cache).await.unwrap();
    let tree = workspace.tree();
    assert_eq!(
        tree.read(tree.resolve_source("survivor").await.unwrap().id, 0, 100)
            .await
            .unwrap(),
        b"must survive receipt replay"
    );
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn changed_identity_seed_or_unknown_state_is_rejected_without_adoption() {
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    let workspace = prepare(config.clone(), provider.clone(), cache.clone())
        .await
        .unwrap();
    let marker = workspace
        .tree()
        .existing_source_control("03".repeat(32).as_bytes())
        .unwrap();
    let marker_path = workspace
        .tree()
        .state_dir()
        .join("local")
        .join(marker.id.to_string());
    drop(workspace);
    let before = fs::read(config.state.join("native-vfs.json")).unwrap();
    let mut changed = config.clone();
    changed.workspace_identity = [4; 32];
    assert!(
        prepare(changed, provider.clone(), cache.clone())
            .await
            .is_err()
    );
    let mut changed = config.clone();
    changed.root_merge.terms[0] = tinytree_model::TreeId([5; 32]);
    assert!(
        prepare(changed, provider.clone(), cache.clone())
            .await
            .is_err()
    );
    assert_eq!(
        fs::read(config.state.join("native-vfs.json")).unwrap(),
        before
    );
    // Exact-size corrupt marker bytes pass local-size checks, but must be
    // rejected before application maintenance or marker/header repair.
    let original_marker = fs::read(&marker_path).unwrap();
    let bad_marker = vec![b'x'; original_marker.len()];
    fs::write(&marker_path, &bad_marker).unwrap();
    assert!(
        prepare_existing(config.clone(), provider.clone(), cache.clone())
            .await
            .is_err()
    );
    assert_eq!(fs::read(&marker_path).unwrap(), bad_marker);
    assert_eq!(
        fs::read(config.state.join("native-vfs.json")).unwrap(),
        before
    );
    fs::write(&marker_path, original_marker).unwrap();
    // Corruption fixture: removing an immutable control payload must not be
    // repaired by strict reopen or by rewriting the ready header.
    fs::remove_file(&marker_path).unwrap();
    assert!(
        prepare_existing(config.clone(), provider.clone(), cache.clone())
            .await
            .is_err()
    );
    assert!(!marker_path.exists());
    assert_eq!(
        fs::read(config.state.join("native-vfs.json")).unwrap(),
        before
    );
    // Pinned KV-layout corruption: the ordinary opener could create a fresh
    // active journal before discovering missing application data. Strict prepare
    // must reject without that repair or deletion of incomplete keyspaces.
    let database = config.state.join("namespace/namespace.fjall");
    let saved_journals = dir.path().join("saved-journals");
    fs::create_dir(&saved_journals).unwrap();
    for entry in fs::read_dir(&database).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|ext| ext == "jnl") {
            fs::rename(entry.path(), saved_journals.join(entry.file_name())).unwrap();
        }
    }
    let before_layout: std::collections::BTreeSet<_> = fs::read_dir(&database)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(
        prepare_existing(config.clone(), provider.clone(), cache.clone())
            .await
            .is_err()
    );
    assert_eq!(
        fs::read_dir(&database)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<std::collections::BTreeSet<_>>(),
        before_layout
    );
    for entry in fs::read_dir(&saved_journals).unwrap() {
        let entry = entry.unwrap();
        fs::rename(entry.path(), database.join(entry.file_name())).unwrap();
    }
    let current = database.join("keyspaces/1/current");
    let saved_current = fs::read(&current).unwrap();
    fs::remove_file(&current).unwrap();
    assert!(
        prepare_existing(config.clone(), provider.clone(), cache.clone())
            .await
            .is_err()
    );
    assert!(!current.exists());
    assert!(
        database.join("keyspaces/1/tables").is_dir(),
        "incomplete keyspace must not be deleted"
    );
    assert_eq!(
        fs::read(config.state.join("native-vfs.json")).unwrap(),
        before
    );
    // Restore only this deliberate fixture corruption for clean teardown.
    fs::write(current, saved_current).unwrap();
    let unknown = dir.path().join("unknown");
    let guard = private_fs::PrivateDir::create(&unknown).unwrap();
    guard.replace("sentinel", b"unrelated").unwrap();
    drop(guard);
    let mut changed = config;
    changed.state = unknown.clone();
    assert!(
        prepare_existing(changed.clone(), provider.clone(), cache.clone())
            .await
            .is_err()
    );
    assert!(prepare(changed, provider, cache).await.is_err());
    assert_eq!(fs::read(unknown.join("sentinel")).unwrap(), b"unrelated");
    assert_eq!(fs::read_dir(unknown).unwrap().count(), 1);
}

#[tokio::test]
async fn overlapping_paths_fail_before_storage_initialization() {
    let dir = tempfile::tempdir().unwrap();
    let (mut config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    config.state = config.root.join("state");
    assert!(prepare(config.clone(), provider, cache).await.is_err());
    assert!(!config.root.exists());
    assert!(!config.state.exists());
}

#[tokio::test]
async fn restart_authorizes_durable_current_base_without_replacing_it_with_bootstrap() {
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    let workspace = prepare(config.clone(), provider.clone(), cache.clone())
        .await
        .unwrap();
    let new_file = provider.file(b"new durable base source");
    let root = provider.object(tinytree_model::Object::Tree(tinytree_model::Tree {
        entries: vec![tinytree_model::TreeEntry {
            name: "next".into(),
            value: tinytree_model::TreeValue::File {
                id: tinytree_model::FileId(new_file.key.id.0),
                size_bytes: new_file.size_bytes,
                executable: false,
                copy_id: None,
            },
        }],
    }));
    let lazy = workspace
        .compiler()
        .lazy_tree(tinytree_model::TreeId(root.key.id.0), 2)
        .unwrap();
    let tree = workspace.tree();
    let snapshot = workspace.capture_lazy_tree_only().unwrap();
    snapshot
        .install(
            lazy.source_root(tree.identity()),
            "checked-switch",
            &config.source_limits,
        )
        .unwrap();
    let local = tree.create(tree.root_id(), "dirty", 0o644).await.unwrap();
    tree.write(local.id, 0, b"dirty on new base").await.unwrap();
    let current = workspace.capture_lazy_tree_only().unwrap();
    let expected = current.version().clone();
    drop(current);
    let pin = tree.lazy_source_pin();
    let header = fs::read(config.state.join("native-vfs.json")).unwrap();
    let reads = provider.metadata_reads.load(Ordering::SeqCst);
    drop(lazy);
    drop(tree);
    drop(workspace);
    let workspace = prepare_existing(config.clone(), provider.clone(), cache)
        .await
        .unwrap();
    assert_eq!(
        provider.metadata_reads.load(Ordering::SeqCst),
        reads,
        "strict reopen attaches current pin without fetching original bootstrap or current ancestry"
    );
    assert_eq!(
        fs::read(config.state.join("native-vfs.json")).unwrap(),
        header
    );
    let current = workspace.capture_lazy_tree_only().unwrap();
    assert_eq!(current.version(), &expected);
    drop(current);
    let tree = workspace.tree();
    assert_eq!(tree.lazy_source_pin(), pin);
    assert!(tree.resolve_source("tool").await.is_err());
    assert_eq!(
        tree.read(tree.resolve_source("next").await.unwrap().id, 0, 100)
            .await
            .unwrap(),
        b"new durable base source"
    );
    assert_eq!(
        tree.read(tree.resolve_source("dirty").await.unwrap().id, 0, 100)
            .await
            .unwrap(),
        b"dirty on new base"
    );
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shared_cache_overlap_is_rejected_before_state_or_mount_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    let mut inside_cache = config.clone();
    inside_cache.state = cache.root_path().join("authoritative-state");
    assert!(
        prepare(inside_cache.clone(), provider.clone(), cache.clone())
            .await
            .is_err()
    );
    assert!(!inside_cache.state.exists());
    assert!(!config.root.exists());
    let mut covers_cache = config.clone();
    covers_cache.root = dir.path().to_path_buf();
    assert!(prepare(covers_cache, provider, cache).await.is_err());
    assert!(!config.state.exists());
}

#[tokio::test]
async fn persistent_nonempty_root_without_native_binding_is_not_adopted() {
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    let workspace = prepare(config.clone(), provider.clone(), cache.clone())
        .await
        .unwrap();
    drop(workspace);
    fs::write(config.root.join("foreign"), b"not a projection binding").unwrap();
    let header = fs::read(config.state.join("native-vfs.json")).unwrap();
    assert!(prepare(config.clone(), provider, cache).await.is_err());
    assert_eq!(
        fs::read(config.root.join("foreign")).unwrap(),
        b"not a projection binding"
    );
    assert_eq!(
        fs::read(config.state.join("native-vfs.json")).unwrap(),
        header
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn recovery_guard_pins_existing_state_without_looking_up_or_repairing_the_mount() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    let workspace = prepare(config.clone(), provider, cache).await.unwrap();
    let identity = workspace.tree().identity().to_owned();
    let header = fs::read(config.state.join("native-vfs.json")).unwrap();
    assert!(
        native_vfs::lock_recovery(
            &config.state,
            &config.root,
            config.workspace_identity,
            config.scope.backend(),
            &identity,
        )
        .is_err(),
        "a live provider still owns its private state"
    );
    drop(workspace);
    let database = config.state.join("namespace/namespace.fjall");
    fs::set_permissions(&database, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        fs::metadata(&config.state).unwrap().permissions().mode() & 0o777,
        0o700
    );
    fs::remove_dir(&config.root).unwrap();
    assert!(
        native_vfs::lock_recovery(
            &config.state,
            &config.root,
            config.workspace_identity,
            config.scope.backend(),
            "different-namespace",
        )
        .is_err()
    );
    let guard = native_vfs::lock_recovery(
        &config.state,
        &config.root,
        config.workspace_identity,
        config.scope.backend(),
        &identity,
    )
    .unwrap();
    assert!(
        !config.root.exists(),
        "recovery must not inspect or create the projection root"
    );
    assert!(
        native_vfs::lock_recovery(
            &config.state,
            &config.root,
            config.workspace_identity,
            config.scope.backend(),
            &identity,
        )
        .is_err(),
        "the guard retains exclusive source ownership"
    );
    assert_eq!(
        fs::read(config.state.join("native-vfs.json")).unwrap(),
        header
    );
    drop(guard);
    assert_eq!(
        fs::metadata(&database).unwrap().permissions().mode() & 0o777,
        0o755,
        "readonly recovery must not repair inherited child permissions"
    );
    let original = database.with_file_name("saved-database");
    fs::rename(&database, &original).unwrap();
    symlink(&original, &database).unwrap();
    assert!(
        native_vfs::lock_recovery(
            &config.state,
            &config.root,
            config.workspace_identity,
            config.scope.backend(),
            &identity,
        )
        .is_err(),
        "a symlink is not a recognized namespace child"
    );
    fs::remove_file(&database).unwrap();
    fs::rename(original, &database).unwrap();
    fs::remove_file(config.state.join("owner.lock")).unwrap();
    assert!(
        native_vfs::lock_recovery(
            &config.state,
            &config.root,
            config.workspace_identity,
            config.scope.backend(),
            &identity,
        )
        .is_err()
    );
    assert!(
        !config.state.join("owner.lock").exists(),
        "missing ownership evidence is not repaired"
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn immutable_candidate_is_lazy_sealed_and_recovers_only_with_its_access_policy() {
    use output_core::{
        Artifact, CoreError, SetAttr, SourceAccess, SourceContent, SourceEntry, SourceLimits,
        SourceManifest,
    };
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    let candidate =
        native_vfs::prepare_immutable_candidate(config.clone(), provider.clone(), cache.clone())
            .await
            .unwrap();
    let tree = candidate.tree();
    let root_tree = config.root_merge.terms[0].0;
    assert_eq!(
        tree.source_access(),
        SourceAccess::ImmutableCandidate { root_tree }
    );
    let assert_readonly = |e: anyhow::Error| {
        assert_eq!(
            e.downcast_ref::<CoreError>(),
            Some(&CoreError::ReadOnly),
            "{e:#}"
        )
    };
    let artifact = Artifact {
        backend: config.scope.backend().into(),
        instance: String::new(),
        digest_function: 9,
        hash: "a".repeat(64),
        size: 5,
    };
    assert_readonly(tree.create(1, "new", 0o644).await.unwrap_err());
    assert_readonly(
        tree.create_exclusive(1, "new", 0o644, [1; 8])
            .await
            .unwrap_err(),
    );
    assert_readonly(tree.mkdir(1, "new", 0o755).await.unwrap_err());
    assert_readonly(tree.symlink(1, "new", "tool").await.unwrap_err());
    assert_readonly(tree.ensure_dir("new/child").await.unwrap_err());
    assert_readonly(tree.unlink(1, "tool").await.unwrap_err());
    assert_readonly(tree.unlink_file(1, "tool").await.unwrap_err());
    assert_readonly(tree.unlink_directory(1, "unvisited").await.unwrap_err());
    assert_readonly(tree.rename(1, "tool", 1, "moved").await.unwrap_err());
    assert_readonly(tree.stage("new", artifact.clone()).await.unwrap_err());
    assert_readonly(tree.prepare_stage("new", artifact).await.err().unwrap());
    assert_readonly(tree.prepare_refresh("").await.err().unwrap());
    assert_readonly(tree.clean("").await.unwrap_err());
    assert_readonly(tree.write(1, 0, b"blocked").await.unwrap_err());
    assert_readonly(
        tree.setattr(
            1,
            SetAttr {
                mode: Some(0o777),
                ..Default::default()
            },
        )
        .await
        .unwrap_err(),
    );
    assert_eq!(
        provider.metadata_reads.load(Ordering::SeqCst),
        0,
        "rejected mutations cannot hydrate directories"
    );
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 0);
    let tool = tree.resolve_source("tool").await.unwrap();
    assert_readonly(tree.write(tool.id, 0, b"blocked").await.unwrap_err());
    assert_readonly(
        tree.setattr(
            tool.id,
            SetAttr {
                size: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap_err(),
    );
    assert_eq!(
        provider.file_reads.load(Ordering::SeqCst),
        0,
        "rejected write/truncate cannot copy up a payload"
    );
    let baseline_snapshot = candidate.capture_lazy_tree_only().unwrap();
    let baseline = baseline_snapshot.version().clone();
    drop(baseline_snapshot);
    let install = candidate
        .compiler()
        .lazy_tree_for_source(config.root_merge.terms[0], 1, config.source_limits.clone())
        .unwrap();
    tree.install_immutable_lazy_source(
        install.source_root(tree.identity()),
        "native-candidate-bootstrap-v1",
        &config.source_limits,
    )
    .unwrap();
    drop(install);
    let replacement = candidate
        .compiler()
        .lazy_tree_for_source(config.root_merge.terms[0], 2, config.source_limits.clone())
        .unwrap();
    assert_readonly(
        tree.install_lazy_source(
            replacement.source_root(tree.identity()),
            Some(baseline.clone()),
            "replace",
            &config.source_limits,
        )
        .unwrap_err(),
    );
    drop(replacement);
    assert_readonly(
        tree.install_source(
            SourceManifest {
                identity: tree.identity().into(),
                root: [7; 32],
                entries: vec![SourceEntry {
                    path: "other".into(),
                    mode: 0o755,
                    content: SourceContent::Directory,
                }],
            },
            Some(baseline),
            "replace-full",
            &SourceLimits::default(),
        )
        .unwrap_err(),
    );
    let snapshot = candidate.capture_lazy_tree_only().unwrap();
    let expected = snapshot.version().clone();
    let replacement = candidate
        .compiler()
        .lazy_tree_for_source(config.root_merge.terms[0], 2, config.source_limits.clone())
        .unwrap();
    assert_readonly(
        snapshot
            .acknowledge_root(
                replacement.source_root(tree.identity()),
                Vec::new(),
                expected,
                "advance",
                &config.source_limits,
            )
            .unwrap_err(),
    );
    drop(replacement);
    assert_eq!(
        tree.read(tool.id, 0, 100).await.unwrap(),
        b"native source\n"
    );
    let identity = tree.identity().to_owned();
    drop(tree);
    drop(candidate);
    cache.wait_for_idle().await.unwrap();
    let state = private_fs::PrivateDir::open_existing(&config.state).unwrap();
    let before = state.read("native-vfs.json", 8192).unwrap();
    let mut header: serde_json::Value = serde_json::from_slice(&before).unwrap();
    assert_eq!(header["format"], 2);
    assert!(
        prepare(config.clone(), provider.clone(), cache.clone())
            .await
            .is_err(),
        "editable open must not downgrade candidate state"
    );
    assert_eq!(state.read("native-vfs.json", 8192).unwrap(), before);
    // Model a crash after the namespace/receipt commit but before ready marker.
    header["ready"] = false.into();
    state
        .replace("native-vfs.json", &serde_json::to_vec(&header).unwrap())
        .unwrap();
    drop(state);
    let candidate =
        native_vfs::prepare_immutable_candidate(config.clone(), provider.clone(), cache.clone())
            .await
            .unwrap();
    assert_eq!(candidate.tree().identity(), identity);
    assert_eq!(
        candidate.tree().source_access(),
        SourceAccess::ImmutableCandidate { root_tree }
    );
    assert_eq!(
        candidate.tree().read(tool.id, 0, 100).await.unwrap(),
        b"native source\n"
    );
    drop(candidate);
    cache.wait_for_idle().await.unwrap();
    let candidate = native_vfs::prepare_existing_immutable_candidate(config, provider, cache)
        .await
        .unwrap();
    assert_eq!(candidate.tree().identity(), identity);
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[tokio::test]
async fn unsupported_immutable_provider_has_no_provisioning_side_effects() {
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    assert!(!config.state.exists() && !config.root.exists());
    let error = native_vfs::prepare_immutable_candidate(config.clone(), provider.clone(), cache)
        .await
        .unwrap_err();
    assert!(error.is::<native_vfs::ImmutableCandidateUnsupported>());
    assert!(!config.state.exists() && !config.root.exists());
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 0);
    assert_eq!(provider.metadata_reads.load(Ordering::SeqCst), 0);
}
