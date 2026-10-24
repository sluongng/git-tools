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
    let startup = native_vfs::mount_prepared_for_restore(prepared)
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
    let mut startup = native_vfs::mount_prepared_for_restore(prepared)
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
    assert!(fs::write(config.root.join(".jj/native-workspace"), b"tampered").is_err());
    assert!(fs::rename(config.root.join(".jj"), config.root.join("moved")).is_err());
    let snapshot = mount.capture_lazy_quiescent().unwrap();
    assert!(snapshot.entries().iter().any(|e| e.path == "new"));
    drop(snapshot);
    mount.shutdown().await.unwrap();
    drop(mount);
    cache.wait_for_idle().await.unwrap();
    let mount = native_vfs::start(config.clone(), provider.clone(), cache)
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
