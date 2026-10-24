#![cfg(target_os = "linux")]
mod common;
use std::{fs, os::unix::fs::PermissionsExt, sync::atomic::Ordering};

/// Native kernel acceptance only. The deterministic provider and file counters
/// are compiled into the declared Rust fixture; no external repository is used.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires usable /dev/fuse and fusermount3"]
async fn native_mount_is_metadata_only_lazy_writable_and_restartable() {
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let cache = common::cache(dir.path());
    let prepared = native_vfs::prepare(config.clone(), provider.clone(), cache.clone())
        .await
        .unwrap();
    let startup = native_vfs::mount_prepared_for_restore_with_metadata_ttl(
        prepared,
        std::time::Duration::from_millis(1000),
    )
    .await
    .unwrap();
    let retained = startup.mount().clone();
    let marker = config.root.join(".jj/native-workspace");
    let identity: String = config
        .workspace_identity
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(fs::read(&marker).unwrap(), identity.as_bytes());
    assert!(fs::metadata(&config.root).unwrap().is_dir());
    let file = config.root.join("tool");
    assert!(
        fs::read(&file).is_err(),
        "regular reads are closed from first visible mount"
    );
    assert!(fs::OpenOptions::new().write(true).open(&file).is_err());
    assert!(fs::write(config.root.join("not-created"), b"blocked").is_err());
    assert!(
        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&file)
            .is_err()
    );
    assert!(fs::rename(&file, config.root.join("not-renamed")).is_err());
    assert!(fs::create_dir(config.root.join("not-a-directory")).is_err());
    assert!(
        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&marker)
            .is_err()
    );
    assert!(
        retained
            .capture_for_backup(
                &config.source_limits,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2)
            )
            .await
            .is_err()
    );
    assert!(
        fs::read(&file).is_err(),
        "failed capture cannot clear startup admission"
    );
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        provider.metadata_reads.load(Ordering::SeqCst),
        0,
        "marker readiness cannot hydrate source directories"
    );
    drop(startup);
    assert!(
        fs::read(&file).is_err(),
        "dropping startup fence must leave mount closed"
    );
    assert_eq!(fs::read(&marker).unwrap(), identity.as_bytes());
    retained.shutdown().await.unwrap();
    drop(retained);
    let prepared = native_vfs::prepare(config.clone(), provider.clone(), cache.clone())
        .await
        .unwrap();
    let mut startup = native_vfs::mount_prepared_for_restore_with_metadata_ttl(
        prepared,
        std::time::Duration::from_millis(1000),
    )
    .await
    .unwrap();
    assert!(fs::read(&file).is_err());
    startup.activate().await.unwrap();
    let mount = startup.mount().clone();
    drop(startup);

    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 0);
    assert_eq!(provider.metadata_reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::metadata(config.root.join("tool"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert_eq!(
        fs::read_link(config.root.join("link")).unwrap(),
        std::path::Path::new("tool")
    );
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs::read(config.root.join("tool")).unwrap(),
        b"native source\n"
    );
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 1);
    let snapshot = mount
        .tree()
        .freeze_lazy_source_wait(
            output_core::SourceFrontendFence::TreeOnly,
            &config.source_limits,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    let expected = snapshot.version().clone();
    drop(snapshot);
    let receipt = mount
        .checkout_resolved(
            &config.root_merge,
            expected.clone(),
            "native-checked-replay".into(),
        )
        .await
        .unwrap();
    let replay = mount
        .checkout_resolved(&config.root_merge, expected, "native-checked-replay".into())
        .await
        .unwrap();
    assert_eq!(
        receipt, replay,
        "exact retry must retain the original generation"
    );
    assert_eq!(
        mount.tree().source_baseline().unwrap().1,
        receipt.version.generation
    );
    fs::write(config.root.join("tool"), b"dirty native file\n").unwrap();
    fs::write(config.root.join("new"), b"new native file\n").unwrap();
    // Kernel-cached positive entries/attributes must track mutations immediately.
    let transient = config.root.join("transient");
    fs::write(&transient, b"long value").unwrap();
    assert_eq!(fs::metadata(&transient).unwrap().len(), 10);
    fs::set_permissions(&transient, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        fs::metadata(&transient).unwrap().permissions().mode() & 0o777,
        0o600
    );
    fs::OpenOptions::new()
        .write(true)
        .open(&transient)
        .unwrap()
        .set_len(2)
        .unwrap();
    assert_eq!(fs::metadata(&transient).unwrap().len(), 2);
    let moved = config.root.join("moved-transient");
    fs::rename(&transient, &moved).unwrap();
    assert!(!transient.exists());
    fs::remove_file(&moved).unwrap();
    assert!(!moved.exists());
    fs::write(&moved, b"recreated").unwrap();
    assert_eq!(fs::read(&moved).unwrap(), b"recreated");
    fs::remove_file(&moved).unwrap();
    assert!(fs::write(config.root.join(".jj/native-workspace"), b"tampered").is_err());
    assert!(fs::rename(config.root.join(".jj"), config.root.join("moved")).is_err());
    let snapshot = mount
        .capture_for_backup(
            &config.source_limits,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(
        snapshot
            .snapshot()
            .entries()
            .iter()
            .any(|e| e.path == "new")
    );
    drop(snapshot);
    mount.shutdown().await.unwrap();
    drop(mount);
    cache.wait_for_idle().await.unwrap();
    let prepared = native_vfs::prepare(config.clone(), provider.clone(), cache)
        .await
        .unwrap();
    let mount = native_vfs::mount_prepared_with_metadata_ttl(
        prepared,
        std::time::Duration::from_millis(1000),
    )
    .await
    .unwrap();
    assert_eq!(
        fs::read(config.root.join("tool")).unwrap(),
        b"dirty native file\n"
    );
    assert_eq!(
        fs::read(config.root.join("new")).unwrap(),
        b"new native file\n"
    );
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 1);
    mount.shutdown().await.unwrap();
}

/// Positive cached metadata must not survive a completed control-plane switch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires usable /dev/fuse and fusermount3"]
async fn cached_native_metadata_is_invalidated_on_checked_root_switch() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tinytree_model as m;
    let dir = tempfile::tempdir().unwrap();
    let (config, provider) = common::fixture(dir.path());
    let prepared = native_vfs::prepare(config.clone(), provider.clone(), common::cache(dir.path()))
        .await
        .unwrap();
    let mount = Arc::new(
        native_vfs::mount_prepared_with_metadata_ttl(
            prepared,
            std::time::Duration::from_millis(1000),
        )
        .await
        .unwrap(),
    );
    let tool = config.root.join("tool");
    assert_eq!(fs::metadata(&tool).unwrap().len(), 14);
    let before = mount.metadata_diagnostics().unwrap();
    for _ in 0..20 {
        assert_eq!(fs::metadata(&tool).unwrap().len(), 14);
    }
    assert_eq!(
        mount.metadata_diagnostics().unwrap().getattrs,
        before.getattrs,
        "kernel should reuse positive metadata within TTL"
    );
    assert_eq!(
        mount.metadata_diagnostics().unwrap().lookups,
        before.lookups,
        "kernel should reuse positive entries within TTL"
    );
    let new_file = provider.file(b"replacement content\n");
    let root = provider.object(m::Object::Tree(m::Tree {
        entries: vec![m::TreeEntry {
            name: "tool".into(),
            value: m::TreeValue::File {
                id: m::FileId(new_file.key.id.0),
                size_bytes: new_file.size_bytes,
                executable: false,
                copy_id: None,
            },
        }],
    }));
    let merge = m::TreeMerge {
        terms: vec![m::TreeId(root.key.id.0)],
        labels: vec![String::new()],
    };
    let capture = mount
        .capture_for_backup(
            &config.source_limits,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();
    let version = capture.snapshot().version().clone();
    drop(capture);
    let running = Arc::new(AtomicBool::new(true));
    let active = running.clone();
    let observed = tool.clone();
    let reader = std::thread::spawn(move || {
        while active.load(Ordering::Acquire) {
            let _ = fs::metadata(&observed);
        }
    });
    let result = mount
        .checkout_resolved(&merge, version, "cached-root-switch".into())
        .await;
    running.store(false, Ordering::Release);
    reader.join().unwrap();
    result.unwrap();
    // No TTL sleep: the completed publication must have evicted the old entry.
    assert_eq!(fs::metadata(&tool).unwrap().len(), new_file.size_bytes);
    assert_eq!(
        fs::metadata(&tool).unwrap().permissions().mode() & 0o777,
        0o644
    );
    assert_eq!(fs::read(&tool).unwrap(), b"replacement content\n");
    assert!(!config.root.join("unused").exists());
    assert!(!config.root.join("link").exists());
    mount.shutdown().await.unwrap();
}
