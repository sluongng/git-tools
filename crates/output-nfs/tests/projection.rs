use async_trait::async_trait;
use nfsserve::{nfs::*, vfs::NFSFileSystem};
use output_core::{Artifact, BlobSource, CacheConfig, Tree};
use output_nfs::OutputNfs;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
struct Source {
    blobs: HashMap<String, Vec<u8>>,
    fetches: AtomicUsize,
    delay: Mutex<Option<Arc<Delay>>>,
    absent: Mutex<Vec<Artifact>>,
}
struct Delay {
    hash: String,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl BlobSource for Source {
    async fn fetch(&self, a: &Artifact, target: &mut std::fs::File) -> anyhow::Result<()> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let delay = self.delay.lock().unwrap().clone();
        if let Some(delay) = delay
            && delay.hash == a.hash
        {
            eprintln!("fixture delayed fetch start {}", a.hash);
            delay.started.notify_one();
            delay.release.notified().await;
            eprintln!("fixture delayed fetch released {}", a.hash);
        }
        target.write_all(self.blobs.get(&a.hash).unwrap())?;
        Ok(())
    }
    async fn missing(&self, _: &[Artifact]) -> anyhow::Result<Vec<Artifact>> {
        Ok(self.absent.lock().unwrap().clone())
    }
}
fn artifact(bytes: &[u8]) -> Artifact {
    Artifact {
        backend: "fixture".into(),
        instance: "".into(),
        digest_function: 1,
        hash: format!("{:x}", Sha256::digest(bytes)),
        size: bytes.len() as u64,
    }
}
fn fixture_with_config(config: CacheConfig) -> (tempfile::TempDir, Arc<Tree>, Arc<Source>) {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Source {
        blobs: [
            (artifact(b"first").hash, b"first".to_vec()),
            (artifact(b"other").hash, b"other".to_vec()),
            (artifact(b"slow!").hash, b"slow!".to_vec()),
            (artifact(b"burst").hash, b"burst".to_vec()),
        ]
        .into(),
        fetches: AtomicUsize::new(0),
        delay: Mutex::new(None),
        absent: Mutex::new(Vec::new()),
    });
    let tree = Tree::open(dir.path().join("state"), "fixture", source.clone(), config).unwrap();
    (dir, tree, source)
}
fn fixture() -> (tempfile::TempDir, Arc<Tree>, Arc<Source>) {
    fixture_with_config(CacheConfig::default())
}
#[tokio::test]
async fn oversized_metadata_is_visible_and_read_limit_has_specific_nfs_error() {
    let (_dir, tree, source) = fixture_with_config(CacheConfig {
        max_inflight_bytes: 4,
        max_oversized_inflight_bytes: 0,
        ..Default::default()
    });
    let node = tree.stage("file", artifact(b"first")).await.unwrap();
    let fs = OutputNfs::new(tree);
    assert_eq!(fs.getattr(node.id).await.unwrap().size, 5);
    assert!(matches!(
        fs.read(node.id, 0, 1).await,
        Err(nfsstat3::NFS3ERR_FBIG)
    ));
    assert_eq!(source.fetches.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn metadata_is_lazy_and_old_handles_keep_content() {
    let (_dir, tree, source) = fixture();
    tree.stage("file", artifact(b"first")).await.unwrap();
    let fs = OutputNfs::new(tree.clone());
    let old = fs.lookup(1, &b"file"[..].into()).await.unwrap();
    assert_eq!(fs.getattr(old).await.unwrap().size, 5);
    assert_eq!(fs.readdir(1, 0, 10).await.unwrap().entries.len(), 1);
    assert_eq!(source.fetches.load(Ordering::SeqCst), 0);
    assert_eq!(fs.read(old, 1, 3).await.unwrap().0, b"irs");
    tree.stage("file", artifact(b"other")).await.unwrap();
    let new = fs.lookup(1, &b"file"[..].into()).await.unwrap();
    assert_ne!(old, new);
    assert_eq!(fs.read(old, 0, 99).await.unwrap().0, b"first");
    assert_eq!(fs.read(new, 0, 99).await.unwrap().0, b"other");
    assert_eq!(source.fetches.load(Ordering::SeqCst), 2);
    let metadata = fs.getattr(new).await.unwrap();
    let wrong_guard = nfstime3 {
        seconds: metadata.ctime.seconds.saturating_sub(1),
        nseconds: metadata.ctime.nseconds,
    };
    assert!(matches!(
        fs.setattr_guarded(
            new,
            sattr3 {
                mode: set_mode3::mode(0o600),
                ..Default::default()
            },
            Some(wrong_guard)
        )
        .await,
        Err(nfsstat3::NFS3ERR_NOT_SYNC)
    ));
    assert_eq!(fs.getattr(new).await.unwrap().mode, metadata.mode);
    fs.write(new, 0, b"local").await.unwrap();
    assert_eq!(fs.read(new, 0, 99).await.unwrap().0, b"local");
    let Some(output_core::TreeStat::File {
        artifact: Some(local),
    }) = tree.stat("file", 1).await.unwrap()
    else {
        panic!("local digest missing")
    };
    assert_eq!(local.hash, artifact(b"local").hash);
}
#[tokio::test]
async fn guarded_creation_and_deleted_directory_cursor() {
    let (_dir, tree, _source) = fixture();
    let fs = OutputNfs::new(tree);
    let name = |s: &str| filename3::from(s.as_bytes());
    let (a, _) = fs.mkdir(1, &name("a")).await.unwrap();
    let (b, _) = fs.mkdir(1, &name("b")).await.unwrap();
    fs.rmdir(1, &name("a")).await.unwrap();
    let list = fs.readdir(1, a, 100).await.unwrap();
    assert_eq!(list.entries[0].fileid, b);
    let id = fs.create_exclusive(1, &name("file"), [1; 8]).await.unwrap();
    assert_eq!(
        fs.create_exclusive(1, &name("file"), [1; 8]).await.unwrap(),
        id
    );
    assert!(matches!(
        fs.create_exclusive(1, &name("file"), [2; 8]).await,
        Err(nfsstat3::NFS3ERR_EXIST)
    ));
    assert!(matches!(
        fs.remove(1, &name("b")).await,
        Err(nfsstat3::NFS3ERR_ISDIR)
    ));
    assert!(matches!(
        fs.rmdir(1, &name("file")).await,
        Err(nfsstat3::NFS3ERR_NOTDIR)
    ));
}
#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native macOS NFS mount; no sudo"]
async fn native_publication_and_mutation() {
    use output_core::Projection;
    use output_nfs::NfsMount;
    use std::io::{Read, Seek, SeekFrom};
    let (dir, tree, source) = fixture();
    let mount = NfsMount::start(tree.clone(), dir.path().join("mount"))
        .await
        .unwrap();
    assert!(
        NfsMount::start(tree.clone(), dir.path().join("mount"))
            .await
            .is_err(),
        "must reject even empty existing mount"
    );
    let result: anyhow::Result<()> = async {
        let root = mount.root();
        anyhow::ensure!(
            !root.join("missing").exists(),
            "negative cache fixture unexpectedly present"
        );
        anyhow::ensure!(
            !root.join("new/deep/leaf").exists(),
            "negative nested parent fixture"
        );
        mount.stage("new/deep/leaf", artifact(b"other")).await?;
        anyhow::ensure!(
            std::fs::metadata(root.join("new/deep/leaf"))?.len() == 5,
            "negative nested parent remained cached"
        );
        mount.stage("file", artifact(b"first")).await?;
        anyhow::ensure!(
            source.fetches.load(Ordering::SeqCst) == 0,
            "metadata staging fetched bytes"
        );
        let held_id = tree.resolve("file")?.id;
        let mut held = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(root.join("file"))?;
        let mut old = String::new();
        held.read_to_string(&mut old)?;
        anyhow::ensure!(old == "first", "held handle contents: {old}");
        held.seek(SeekFrom::Start(0))?;
        std::io::Write::write_all(&mut held, b"edit!")?;
        held.sync_all()?;
        mount.stage("file", artifact(b"other")).await?;
        anyhow::ensure!(
            std::fs::read(root.join("file"))? == b"other",
            "visible contents of file differ from other"
        );
        held.seek(SeekFrom::Start(0))?;
        old.clear();
        held.read_to_string(&mut old)?;
        anyhow::ensure!(old == "edit!", "held local edit changed: {old}");
        anyhow::ensure!(
            matches!(
                mount.maintain_quiescent(2).await?,
                output_core::MaintenanceOutcome::BusyRetained
            ),
            "ordinary unmount must retain the busy old generation"
        );
        anyhow::ensure!(
            mount.state() == output_nfs::NfsMountState::Mounted,
            "busy maintenance stopped serving"
        );
        held.seek(SeekFrom::Start(0))?;
        old.clear();
        held.read_to_string(&mut old)?;
        anyhow::ensure!(old == "edit!", "busy maintenance lost held edit");
        anyhow::ensure!(
            std::fs::read(root.join("file"))? == b"other",
            "busy maintenance lost replacement"
        );
        drop(held);
        let collected = match mount.maintain_quiescent(2).await? {
            output_core::MaintenanceOutcome::Rotated { collection } => collection,
            other => anyhow::bail!("released generation did not rotate: {other:?}"),
        };
        anyhow::ensure!(
            collected.reclaimed_local_bytes >= 5 && tree.get(held_id).is_err(),
            "old payload/metadata not reclaimed"
        );
        anyhow::ensure!(
            std::fs::read(root.join("file"))? == b"other",
            "fresh mount lost replacement"
        );
        // Collection failure must still restore native access to linked edits.
        std::fs::write(root.join("kept-local"), b"unuploaded native edit")?;
        let failed = tree.create(1, "retirement-failure", 0o644).await?;
        tree.write(failed.id, 0, b"retired payload").await?;
        tree.unlink(1, "retirement-failure").await?;
        let payload = tree.state_dir().join("local").join(failed.id.to_string());
        std::fs::remove_file(&payload)?;
        std::fs::create_dir(&payload)?;
        anyhow::ensure!(
            mount.maintain_quiescent(2).await.is_err(),
            "injected deletion failure was hidden"
        );
        anyhow::ensure!(
            mount.state() == output_nfs::NfsMountState::Mounted,
            "GC error left client unmounted"
        );
        anyhow::ensure!(
            std::fs::read(root.join("kept-local"))? == b"unuploaded native edit",
            "GC failure lost live authority"
        );
        std::fs::remove_dir(&payload)?;
        std::fs::write(&payload, b"known retired payload")?;
        anyhow::ensure!(
            matches!(
                mount.maintain_quiescent(2).await?,
                output_core::MaintenanceOutcome::Rotated { .. }
            ),
            "repaired maintenance did not recover"
        );
        anyhow::ensure!(!payload.exists(), "repaired ledger was not drained");
        std::fs::remove_file(root.join("kept-local"))?;
        mount.maintain_quiescent(64).await?;
        let collect_iterations = std::env::var("OUTPUT_NFS_ITERATION_GC").as_deref() != Ok("0");
        let bytes = vec![0x5a; 32 * 1024];
        std::fs::write(root.join("iteration-live"), &bytes)?;
        let iterations_started = std::time::Instant::now();
        let mut rotation_us = 0;
        let mut peak_retired_bytes = 0;
        for iteration in 0..16 {
            std::fs::write(root.join("iteration-retired"), &bytes)?;
            std::fs::remove_file(root.join("iteration-retired"))?;
            peak_retired_bytes = peak_retired_bytes.max(tree.retirement_metrics().retained_local_bytes);
            if collect_iterations && iteration % 4 == 3 {
                let started = std::time::Instant::now();
                anyhow::ensure!(matches!(mount.maintain_quiescent(64).await?, output_core::MaintenanceOutcome::Rotated { .. }), "iteration boundary did not rotate");
                rotation_us += started.elapsed().as_micros();
            }
            anyhow::ensure!(std::fs::read(root.join("iteration-live"))? == bytes, "iteration GC evicted linked edits");
        }
        let wall_us = iterations_started.elapsed().as_micros();
        let retained = tree.retirement_metrics();
        let local_bytes: u64 = std::fs::read_dir(tree.state_dir().join("local"))?
            .map(|entry| Ok::<_, anyhow::Error>(entry?.metadata()?.len())).collect::<anyhow::Result<Vec<_>>>()?.into_iter().sum();
        eprintln!("NFS_ITERATIONS collect={} count=16 payload_bytes=32768 wall_us={} rotation_us={} peak_retired_bytes={} retained_local_bytes={} local_payload_bytes={} linked_edit_bytes=32768",
            collect_iterations, wall_us, rotation_us, peak_retired_bytes, retained.retained_local_bytes, local_bytes);
        anyhow::ensure!(retained.retained_local_bytes == if collect_iterations { 0 } else { 16 * 32768 }, "unexpected iteration retirement occupancy");
        std::fs::remove_file(root.join("iteration-live"))?;
        mount.maintain_quiescent(64).await?;
        // Poll once to admit the owned operation, then cancel its caller. A
        // subsequent operation waits for complete drain/remount, not a lost
        // guard or an unmounted root with an apparently successful response.
        let mut cancelled = Box::pin(mount.maintain_quiescent(64));
        let first = std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(cancelled.as_mut(), cx))).await;
        drop(cancelled);
        eprintln!("NFS_MAINTENANCE_CANCEL admitted_pending={}", first.is_pending());
        if let std::task::Poll::Ready(result) = first { result?; }
        anyhow::ensure!(matches!(mount.maintain_quiescent(64).await?, output_core::MaintenanceOutcome::Rotated { .. }), "cancelled admitted maintenance lost session ownership");
        anyhow::ensure!(mount.state() == output_nfs::NfsMountState::Mounted, "cancelled maintenance left recovery state");
        mount.stage("missing", artifact(b"first")).await?;
        anyhow::ensure!(
            std::fs::read(root.join("missing"))? == b"first",
            "visible contents of missing differ from first"
        );
        use std::os::unix::fs::PermissionsExt;
        let sentinel = dir.path().join("external-sentinel");
        std::fs::write(&sentinel, b"do not change")?;
        std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o600))?;
        std::os::unix::fs::symlink(&sentinel, root.join("link"))?;
        anyhow::ensure!(
            std::fs::read(root.join("link"))? == b"do not change",
            "symlink fixture"
        );
        mount.stage("link", artifact(b"other")).await?;
        anyhow::ensure!(
            std::fs::read(root.join("link"))? == b"other",
            "cached symlink not replaced"
        );
        anyhow::ensure!(
            std::fs::metadata(&sentinel)?.permissions().mode() & 0o777 == 0o600,
            "invalidation changed external symlink target mode"
        );
        let delay = Arc::new(Delay {
            hash: artifact(b"slow!").hash,
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *source.delay.lock().unwrap() = Some(delay.clone());
        mount.stage("delayed", artifact(b"slow!")).await?;
        let delayed_path = root.join("delayed");
        let reader = tokio::task::spawn_blocking(move || std::fs::read(delayed_path));
        tokio::time::timeout(std::time::Duration::from_secs(5), delay.started.notified()).await?;
        let replacement = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            mount.stage("delayed", artifact(b"other")),
        )
        .await;
        delay.release.notify_one();
        let delayed_bytes = reader.await??;
        replacement??;
        anyhow::ensure!(
            delayed_bytes == b"slow!",
            "old in-flight read changed generation"
        );
        anyhow::ensure!(
            std::fs::read(root.join("delayed"))? == b"other",
            "late read overwrote new generation"
        );
        mount.stage("nested/missing", artifact(b"first")).await?;
        anyhow::ensure!(
            std::fs::read(root.join("nested/missing"))? == b"first",
            "nested stage failed"
        );
        *source.absent.lock().unwrap() = vec![artifact(b"first")];
        mount.refresh("").await?;
        anyhow::ensure!(
            !root.join("nested/missing").exists(),
            "refresh retained cached nested name"
        );
        source.absent.lock().unwrap().clear();
        std::fs::write(root.join("file"), b"local")?;
        anyhow::ensure!(
            std::fs::read(root.join("file"))? == b"local",
            "visible contents of file differ from local"
        );
        std::fs::create_dir(root.join("dir"))?;
        std::fs::write(root.join("dir/a"), b"dirty")?;
        std::fs::rename(root.join("dir/a"), root.join("dir/b"))?;
        std::fs::remove_file(root.join("dir/b"))?;
        std::fs::remove_dir(root.join("dir"))?;
        let burst = Arc::new(Delay {
            hash: artifact(b"burst").hash,
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        *source.delay.lock().unwrap() = Some(burst.clone());
        eprintln!("native burst staging");
        for i in 0..32 {
            mount
                .stage(&format!("burst-{i}"), artifact(b"burst"))
                .await?;
        }
        eprintln!("native burst launching reads");
        let mut readers = Vec::new();
        for i in 0..32 {
            let path = root.join(format!("burst-{i}"));
            readers.push(tokio::task::spawn_blocking(move || std::fs::read(path)));
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), burst.started.notified()).await?;
        let metadata_path = root.to_owned();
        let metadata = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio::task::spawn_blocking(move || {
                std::fs::read_dir(metadata_path).map(|entries| entries.count())
            }),
        )
        .await;
        eprintln!("native burst metadata completed: {}", metadata.is_ok());
        burst.release.notify_one();
        for reader in readers {
            let bytes =
                tokio::time::timeout(std::time::Duration::from_secs(20), reader).await???;
            anyhow::ensure!(bytes == b"burst", "native JUKEBOX retry lost contents");
        }
        eprintln!("native burst all readers complete");
        anyhow::ensure!(metadata??? > 0, "metadata listing empty under load");
        mount.clean("").await?;
        anyhow::ensure!(!root.join("file").exists(), "clean left cached file");
        anyhow::ensure!(
            !root.join("missing").exists(),
            "negative cache fixture unexpectedly present"
        );
        Ok(())
    }
    .await;
    let shutdown = if result.is_ok() {
        mount.shutdown().await
    } else {
        // Only this test-owned mount is force-unmounted on failure, so pending
        // kernel read retries cannot strand the test host after a timeout.
        let cleanup = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio::process::Command::new("/sbin/umount")
                .arg("-f")
                .arg(mount.root())
                .kill_on_drop(true)
                .output(),
        )
        .await;
        eprintln!("native failure force-unmount: {cleanup:?}");
        Ok(())
    };
    result.unwrap();
    shutdown.unwrap();
    let remount = NfsMount::start(tree.clone(), dir.path().join("mount"))
        .await
        .unwrap();
    let entries = std::fs::read_dir(remount.root()).unwrap().count();
    // Manual unmount followed by a replacement export: shutting down the old
    // owner must neither hang on its dead root nor unmount the replacement.
    let unmount = tokio::process::Command::new("/sbin/umount")
        .arg(remount.root())
        .output()
        .await
        .unwrap();
    assert!(unmount.status.success());
    let replacement = NfsMount::start(tree.clone(), dir.path().join("mount"))
        .await
        .unwrap();
    remount.shutdown().await.unwrap();
    assert!(
        output_nfs::ensure_unmounted(replacement.root()).is_err(),
        "old shutdown removed replacement mount"
    );
    replacement.shutdown().await.unwrap();
    assert_eq!(entries, 0);
}

// Exercise the real transport, not only the filesystem trait. NFSv3 procedure 6
// returns post-op attributes followed by count, EOF and the XDR opaque payload.
#[tokio::test]
async fn real_tcp_read_fetches_verified_content() {
    use nfsserve::tcp::{NFSTcp, NFSTcpListener};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (_dir, tree, source) = fixture();
    tree.stage("file", artifact(b"first")).await.unwrap();
    let fs = OutputNfs::new(tree);
    let id = fs.lookup(1, &b"file"[..].into()).await.unwrap();
    let handle = fs.id_to_fh(id).data;
    let server = NFSTcpListener::bind("127.0.0.1:0", fs).await.unwrap();
    let port = server.get_listen_port();
    let task = tokio::spawn(async move { server.handle_forever().await });
    let mut call = Vec::new();
    for n in [42u32, 0, 2, 100003, 3, 6, 0, 0, 0, 0, handle.len() as u32] {
        call.extend_from_slice(&n.to_be_bytes());
    }
    call.extend_from_slice(&handle);
    for n in [0u32, 0, 99] {
        call.extend_from_slice(&n.to_be_bytes());
    }
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    socket
        .write_u32(0x8000_0000 | call.len() as u32)
        .await
        .unwrap();
    socket.write_all(&call).await.unwrap();
    let marker = socket.read_u32().await.unwrap();
    assert_ne!(marker & 0x8000_0000, 0);
    let mut reply = vec![0; (marker & 0x7fff_ffff) as usize];
    socket.read_exact(&mut reply).await.unwrap();
    assert_eq!(
        &reply[..24],
        [42u32, 1, 0, 0, 0, 0].map(u32::to_be_bytes).as_flattened()
    );
    assert_eq!(&reply[24..32], &[0, 0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(&reply[128..133], b"first");
    assert_eq!(source.fetches.load(Ordering::SeqCst), 1);
    task.abort();
}

async fn send_call(stream: &mut tokio::net::TcpStream, xid: u32, procedure: u32, handle: &[u8]) {
    use tokio::io::AsyncWriteExt;
    let mut call = Vec::new();
    for n in [
        xid,
        0,
        2,
        100003,
        3,
        procedure,
        0,
        0,
        0,
        0,
        handle.len() as u32,
    ] {
        call.extend_from_slice(&n.to_be_bytes());
    }
    call.extend_from_slice(handle);
    if procedure == 6 {
        for n in [0u32, 0, 99] {
            call.extend_from_slice(&n.to_be_bytes());
        }
    }
    stream
        .write_u32(0x8000_0000 | call.len() as u32)
        .await
        .unwrap();
    stream.write_all(&call).await.unwrap();
}
async fn receive_call(stream: &mut tokio::net::TcpStream) -> (u32, u32) {
    use tokio::io::AsyncReadExt;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let marker = stream.read_u32().await.unwrap();
        assert_ne!(marker & 0x8000_0000, 0);
        let mut reply = vec![0; (marker & 0x7fff_ffff) as usize];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(
            &reply[4..24],
            [1u32, 0, 0, 0, 0].map(u32::to_be_bytes).as_flattened()
        );
        (
            u32::from_be_bytes(reply[..4].try_into().unwrap()),
            u32::from_be_bytes(reply[24..28].try_into().unwrap()),
        )
    })
    .await
    .expect("RPC was starved by cold hydration")
}
#[tokio::test]
async fn cold_read_overload_preserves_metadata_and_same_xid_retry() {
    use nfsserve::tcp::{NFSTcp, NFSTcpListener};
    let (_dir, tree, source) = fixture();
    let delay = Arc::new(Delay {
        hash: artifact(b"slow!").hash,
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    *source.delay.lock().unwrap() = Some(delay.clone());
    tree.stage("file", artifact(b"slow!")).await.unwrap();
    let fs = OutputNfs::with_read_limit(tree, 4);
    let handle = fs
        .id_to_fh(fs.lookup(1, &b"file"[..].into()).await.unwrap())
        .data;
    let server = NFSTcpListener::bind("127.0.0.1:0", fs).await.unwrap();
    let port = server.get_listen_port();
    let task = tokio::spawn(async move { server.handle_forever().await });
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    for xid in 1..=20 {
        send_call(&mut stream, xid, 6, &handle).await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), delay.started.notified())
        .await
        .unwrap();
    send_call(&mut stream, 99, 1, &handle).await;
    let mut rejected = Vec::new();
    let mut metadata = false;
    while rejected.len() < 16 || !metadata {
        let (xid, status) = receive_call(&mut stream).await;
        if xid == 99 {
            assert_eq!(status, 0);
            metadata = true;
        } else {
            assert_eq!(status, nfsstat3::NFS3ERR_JUKEBOX as u32);
            rejected.push(xid);
        }
    }
    delay.release.notify_one();
    for _ in 0..4 {
        let (xid, status) = receive_call(&mut stream).await;
        assert!((1..=20).contains(&xid));
        assert_eq!(status, 0);
    }
    // The same stream and XID must now succeed, not replay sixty seconds of
    // stale overload. Mutation success replies retain ordinary replay behavior.
    send_call(&mut stream, rejected[0], 6, &handle).await;
    assert_eq!(receive_call(&mut stream).await, (rejected[0], 0));
    assert_eq!(source.fetches.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test]
async fn stateless_handles_pin_server_generation_until_all_server_clones_end() {
    let (_dir, tree, _source) = fixture();
    let node = tree.create(1, "output", 0o644).await.unwrap();
    tree.write(node.id, 0, b"local edit").await.unwrap();
    let fs = OutputNfs::new(tree.clone());
    let connection = fs.clone();
    let old = fs.lookup(1, &b"output"[..].into()).await.unwrap();
    tree.unlink(1, "output").await.unwrap();
    let replacement = tree.stage("output", artifact(b"first")).await.unwrap();
    assert_eq!(tree.reclaim_retired(64).unwrap().reclaimed_nodes, 0);
    assert_eq!(fs.read(old, 0, 99).await.unwrap().0, b"local edit");
    drop(fs);
    assert_eq!(tree.reclaim_retired(64).unwrap().reclaimed_nodes, 0);
    assert_eq!(connection.read(old, 0, 99).await.unwrap().0, b"local edit");
    drop(connection);
    // The transport owner has ended the whole server generation. There is no
    // inference here from zero executing RPCs, an idle timeout, or fake CLOSE.
    assert_eq!(tree.reclaim_retired(64).unwrap().reclaimed_local_bytes, 10);
    assert!(tree.get(old).is_err());
    assert_eq!(tree.read(replacement.id, 0, 99).await.unwrap(), b"first");
}

#[tokio::test]
async fn owned_transport_shutdown_drains_nested_work_and_expires_file_handles() {
    use nfsserve::tcp::{NFSTcp, NFSTcpListener};
    use tokio::io::AsyncReadExt;
    let (_dir, tree, source) = fixture();
    let delay = Arc::new(Delay {
        hash: artifact(b"slow!").hash,
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    *source.delay.lock().unwrap() = Some(delay.clone());
    let old = tree.stage("file", artifact(b"slow!")).await.unwrap();
    let fs = OutputNfs::new(tree.clone());
    let old_handle = fs.id_to_fh(old.id);
    let server = NFSTcpListener::bind("127.0.0.1:0", fs).await.unwrap();
    let port = server.get_listen_port();
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let serving = tokio::spawn(async move { server.handle_until_shutdown(receiver).await });
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    send_call(&mut stream, 1, 6, &old_handle.data).await;
    delay.started.notified().await;
    let replacement = tree.stage("file", artifact(b"first")).await.unwrap();
    assert_eq!(tree.reclaim_retired(64).unwrap().reclaimed_nodes, 0);
    stop.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), serving)
        .await
        .expect("shutdown must not wait for abandoned hydration")
        .unwrap()
        .unwrap();
    let mut byte = [0];
    assert!(
        matches!(stream.read(&mut byte).await, Ok(0) | Err(_)),
        "owned connection remained live"
    );
    assert_eq!(
        tree.reclaim_retired_at_boundary(4).unwrap().reclaimed_nodes,
        1,
        "nested RPC/context clones must release the old generation fence"
    );
    delay.release.notify_one();
    let fresh = OutputNfs::new(tree.clone());
    assert!(matches!(
        fresh.fh_to_id(&old_handle),
        Err(nfsstat3::NFS3ERR_STALE)
    ));
    assert_eq!(
        fresh.fh_to_id(&fresh.id_to_fh(replacement.id)).unwrap(),
        replacement.id
    );
    assert_eq!(fresh.read(replacement.id, 0, 99).await.unwrap().0, b"first");
}

#[tokio::test]
async fn native_source_metadata_switch_preserves_nfs_generations_without_hydration() {
    use output_core::{
        SourceContent, SourceEntry, SourceFrontendFence, SourceLimits, SourceManifest,
    };
    let (_dir, tree, source) = fixture();
    let limits = SourceLimits::default();
    let remote = Artifact {
        backend: "fixture".into(),
        instance: "".into(),
        digest_function: 9,
        hash: "a".repeat(64),
        size: 5,
    };
    let manifest = SourceManifest {
        identity: tree.identity().into(),
        root: [1; 32],
        entries: vec![
            SourceEntry {
                path: "file".into(),
                mode: 0o644,
                content: SourceContent::File(remote),
            },
            SourceEntry {
                path: "link".into(),
                mode: 0o777,
                content: SourceContent::Symlink("file".into()),
            },
        ],
    };
    tree.install_source(manifest.clone(), None, "initial", &limits)
        .unwrap();
    let fs = OutputNfs::new(tree.clone());
    let old = fs.lookup(1, &b"file"[..].into()).await.unwrap();
    let old_handle = fs.id_to_fh(old);
    assert_eq!(fs.getattr(old).await.unwrap().size, 5);
    let symlink = fs.lookup(1, &b"link"[..].into()).await.unwrap();
    assert_eq!(fs.readlink(symlink).await.unwrap().0, b"file");
    assert_eq!(source.fetches.load(Ordering::SeqCst), 0);
    // Truncate locally without downloading the old blob, then retain this dirty
    // generation through a full namespace switch (not per-file staging).
    fs.setattr(
        old,
        sattr3 {
            size: set_size3::size(0),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    fs.write(old, 0, b"local").await.unwrap();
    let expected = tree
        .freeze_source(SourceFrontendFence::TreeOnly, &limits)
        .unwrap()
        .version()
        .clone();
    let mut next = manifest;
    next.root = [2; 32];
    if let SourceContent::File(artifact) = &mut next.entries[0].content {
        artifact.hash = "b".repeat(64);
        artifact.size = 7;
    }
    tree.install_source(next, Some(expected), "next", &limits)
        .unwrap();
    let new = fs.lookup(1, &b"file"[..].into()).await.unwrap();
    assert_ne!(new, old);
    assert_eq!(fs.getattr(new).await.unwrap().size, 7);
    assert_eq!(fs.fh_to_id(&old_handle).unwrap(), old);
    assert_eq!(fs.read(old, 0, 5).await.unwrap().0, b"local");
    assert_eq!(
        source.fetches.load(Ordering::SeqCst),
        0,
        "source metadata publication must never read a blob"
    );
}

#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires actual macOS NFS client; source capture remains unsupported"]
async fn native_source_dirty_page_flush_preparation_is_not_a_capture_proof() {
    use output_core::{Projection, SourceContent, SourceEntry, SourceLimits, SourceManifest};
    use output_nfs::{NfsMount, NfsSourceUnsupported};
    let (dir, tree, source) = fixture();
    let limits = SourceLimits::default();
    tree.install_source(
        SourceManifest {
            identity: tree.identity().into(),
            root: [1; 32],
            entries: vec![SourceEntry {
                path: "file".into(),
                mode: 0o644,
                content: SourceContent::File(Artifact {
                    backend: "fixture".into(),
                    instance: "".into(),
                    digest_function: 9,
                    hash: "a".repeat(64),
                    size: 5,
                }),
            }],
        },
        None,
        "initial",
        &limits,
    )
    .unwrap();
    let mount = NfsMount::start(tree.clone(), dir.path().join("source-mount"))
        .await
        .unwrap();
    let result: anyhow::Result<()> = async {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(mount.root().join("file"))?;
        file.write_all(b"native dirty source bytes")?;
        // This establishes this single descriptor's flush behavior only. It
        // cannot prove a bounded whole-client/page-cache COMMIT barrier.
        file.sync_all()?;
        drop(file);
        let node = tree.resolve("file")?;
        anyhow::ensure!(tree.read(node.id, 0, 64).await? == b"native dirty source bytes");
        let error = mount
            .capture_source_quiescent(limits.clone())
            .await
            .err()
            .unwrap();
        anyhow::ensure!(error.downcast_ref::<NfsSourceUnsupported>().is_some());
        anyhow::ensure!(tree.source_baseline()?.0 == [1; 32]);
        anyhow::ensure!(
            source.fetches.load(Ordering::SeqCst) == 0,
            "metadata-only source path hydrated content"
        );
        Ok(())
    }
    .await;
    let stopped = mount.shutdown().await;
    result.unwrap();
    stopped.unwrap();
}
