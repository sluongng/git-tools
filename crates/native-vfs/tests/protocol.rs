mod common;
use native_vfs::prepare;
use std::{fs, sync::atomic::Ordering};

#[tokio::test]
async fn prepare_is_metadata_only_and_restart_preserves_dirty_files_and_control_identity() {
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
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
    let unknown = dir.path().join("unknown");
    let guard = private_fs::PrivateDir::create(&unknown).unwrap();
    guard.replace("sentinel", b"unrelated").unwrap();
    drop(guard);
    let mut changed = config;
    changed.state = unknown.clone();
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
    drop(lazy);
    drop(tree);
    drop(workspace);
    let workspace = prepare(config, provider.clone(), cache).await.unwrap();
    let tree = workspace.tree();
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
