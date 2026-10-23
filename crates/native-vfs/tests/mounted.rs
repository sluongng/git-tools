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
    let mount = native_vfs::start(config.clone(), provider.clone(), cache.clone())
        .await
        .unwrap();
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 0);
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
    fs::write(config.root.join("tool"), b"dirty native file\n").unwrap();
    fs::write(config.root.join("new"), b"new native file\n").unwrap();
    assert!(fs::write(config.root.join(".jj/native-workspace"), b"tampered").is_err());
    assert!(fs::rename(config.root.join(".jj"), config.root.join("moved")).is_err());
    let snapshot = mount.capture_quiescent().unwrap();
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
