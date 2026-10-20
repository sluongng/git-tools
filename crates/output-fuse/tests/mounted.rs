#![cfg(target_os = "linux")]

use anyhow::{Result, bail};
use async_trait::async_trait;
use output_core::{Artifact, BlobSource, CacheConfig, Projection, Tree, TreeStat};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, PermissionsExt, symlink},
    },
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};
use tokio::sync::{Notify, Semaphore};

struct Source {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
    fetches: AtomicU64,
    blocked: Mutex<Option<String>>,
    entered: Notify,
    release: Semaphore,
}
impl Source {
    fn new() -> Self {
        Self {
            blobs: Mutex::new(HashMap::new()),
            fetches: AtomicU64::new(0),
            blocked: Mutex::new(None),
            entered: Notify::new(),
            release: Semaphore::new(0),
        }
    }
    fn artifact(&self, bytes: &[u8]) -> Artifact {
        let hash = blake3::hash(bytes).to_hex().to_string();
        self.blobs
            .lock()
            .unwrap()
            .insert(hash.clone(), bytes.to_vec());
        Artifact {
            backend: "test".into(),
            instance: "".into(),
            digest_function: 9,
            hash,
            size: bytes.len() as u64,
        }
    }
}
#[async_trait]
impl BlobSource for Source {
    async fn fetch(&self, artifact: &Artifact, file: &mut File) -> Result<()> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let blocked = self.blocked.lock().unwrap().as_ref() == Some(&artifact.hash);
        if blocked {
            self.entered.notify_one();
            self.release.acquire().await?.forget();
        }
        let bytes = self.blobs.lock().unwrap().get(&artifact.hash).cloned();
        let Some(bytes) = bytes else {
            bail!("unknown test blob")
        };
        file.write_all(&bytes)?;
        Ok(())
    }
    async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
        Ok(vec![])
    }
}
async fn io<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work).await.unwrap()
}
fn mapped_bytes(file: &File, len: usize) -> Vec<u8> {
    let address = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            file.as_raw_fd(),
            0,
        )
    };
    assert_ne!(
        address,
        libc::MAP_FAILED,
        "{}",
        std::io::Error::last_os_error()
    );
    let bytes = unsafe { std::slice::from_raw_parts(address.cast::<u8>(), len) }.to_vec();
    assert_eq!(unsafe { libc::munmap(address, len) }, 0);
    bytes
}

/// Run explicitly on Linux with unprivileged /dev/fuse and fusermount3. This is
/// a kernel acceptance test, not a substitute for the portable Tree tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires usable /dev/fuse and fusermount3"]
async fn mounted_outputs_are_lazy_writable_and_generation_safe() {
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().join("outputs");
    let source = Arc::new(Source::new());
    let tree = Tree::open(
        fixture.path().join("state"),
        "test",
        source.clone(),
        CacheConfig::default(),
    )
    .unwrap();
    let mount = output_fuse::start(tree.clone(), &root).await.unwrap();
    assert!(
        output_fuse::start(tree.clone(), &root).await.is_err(),
        "must not stack mounts"
    );
    let mut oversized = source.artifact(b"metadata only");
    oversized.size = CacheConfig::default().readable_limit() + 1;
    mount.stage("oversized", oversized.clone()).await.unwrap();
    let oversized_path = root.join("oversized");
    io(move || {
        assert_eq!(fs::metadata(&oversized_path).unwrap().len(), oversized.size);
        let error = File::open(&oversized_path).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EFBIG));
        // Discarding the oversized generation needs no hydration or quota.
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&oversized_path)
            .unwrap();
        assert_eq!(fs::metadata(&oversized_path).unwrap().len(), 0);
    })
    .await;
    assert_eq!(source.fetches.load(Ordering::SeqCst), 0);
    let old = source.artifact(b"old bytes");
    let new = source.artifact(b"new bytes");
    mount.ensure_dir("base").await.unwrap();
    let negative = root.join("base/nested");
    io(move || assert!(!negative.exists())).await;
    mount.stage("base/nested/file", old.clone()).await.unwrap();
    assert!(
        matches!(mount.stat("base/nested/file", 9).await.unwrap(), Some(TreeStat::File {artifact: Some(a)}) if a == old)
    );
    let path = root.join("base/nested/file");
    let local_path = path.clone();
    let old_handle = io(move || {
        assert_eq!(fs::metadata(&local_path).unwrap().len(), 9);
        assert_eq!(
            fs::read_dir(local_path.parent().unwrap()).unwrap().count(),
            1
        );
        let link = local_path.parent().unwrap().join("link");
        symlink("file", &link).unwrap();
        assert_eq!(fs::read_link(&link).unwrap(), std::path::Path::new("file"));
        // Metadata and opening a remote generation must not hydrate.
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(local_path)
            .unwrap()
    })
    .await;
    assert_eq!(source.fetches.load(Ordering::SeqCst), 0);
    let old_handle = io(move || {
        assert_eq!(mapped_bytes(&old_handle, 9), b"old bytes");
        old_handle
    })
    .await;
    assert_eq!(source.fetches.load(Ordering::SeqCst), 1);
    mount.stage("base/nested/file", new.clone()).await.unwrap();
    let replacement = path.clone();
    let old_handle = io(move || {
        let mut old_handle = old_handle;
        let fresh = File::open(&replacement).unwrap();
        assert_eq!(
            mapped_bytes(&fresh, 9),
            b"new bytes",
            "page/dentry caches must not retain old generation"
        );
        old_handle.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = vec![];
        old_handle.read_to_end(&mut bytes).unwrap();
        assert_eq!(
            bytes, b"old bytes",
            "open descriptor must keep retired generation"
        );
        old_handle.seek(SeekFrom::Start(0)).unwrap();
        old_handle.write_all(b"OLD").unwrap();
        old_handle.sync_all().unwrap();
        assert_eq!(
            fs::read(&replacement).unwrap(),
            b"new bytes",
            "retired COW write must not change replacement"
        );
        old_handle
    })
    .await;
    drop(old_handle);
    let metadata_only = source.artifact(b"do not fetch me");
    mount.stage("base/truncate", metadata_only).await.unwrap();
    let fetches = source.fetches.load(Ordering::SeqCst);
    let truncate = root.join("base/truncate");
    io(move || {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&truncate)
            .unwrap();
        file.write_all(b"local").unwrap();
        file.set_permissions(fs::Permissions::from_mode(0o640))
            .unwrap();
        file.set_times(fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(100)))
            .unwrap();
        file.sync_all().unwrap();
        let metadata = fs::metadata(&truncate).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o640);
        assert_eq!(metadata.mtime(), 100);
        assert_eq!(fs::read(&truncate).unwrap(), b"local");
        let renamed = truncate.with_file_name("renamed");
        fs::rename(&truncate, &renamed).unwrap();
        let mut unlinked = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&renamed)
            .unwrap();
        fs::remove_file(&renamed).unwrap();
        fs::write(&renamed, b"replacement").unwrap();
        unlinked.write_all(b"OLD").unwrap();
        assert_eq!(fs::read(&renamed).unwrap(), b"replacement");
        fs::remove_file(renamed).unwrap();
        let dir = truncate.with_file_name("empty");
        fs::create_dir(&dir).unwrap();
        fs::remove_dir(&dir).unwrap();
    })
    .await;
    assert_eq!(
        source.fetches.load(Ordering::SeqCst),
        fetches,
        "truncate-to-zero must not download remote bytes"
    );

    // A real ELF exercises executable mappings and loader reads, not only a
    // shell script whose contents could be read through a different path.
    let executable = io(|| fs::read("/bin/true").unwrap()).await;
    mount
        .stage("base/true", source.artifact(&executable))
        .await
        .unwrap();
    let executable_path = root.join("base/true");
    io(move || {
        assert!(
            std::process::Command::new(executable_path)
                .status()
                .unwrap()
                .success()
        )
    })
    .await;

    let slow = source.artifact(b"slow content");
    *source.blocked.lock().unwrap() = Some(slow.hash.clone());
    // Distinct inodes force separate FUSE requests, while identical CAS bytes
    // still exercise shared-fetch coalescing. More readers than the64 active
    // slots must wait, not expose EAGAIN to ordinary blocking filesystem users.
    const READERS: usize = 96;
    let ready = Arc::new(std::sync::Barrier::new(READERS + 1));
    let mut reads = vec![];
    for index in 0..READERS {
        let relative = format!("base/slow-{index}");
        mount.stage(&relative, slow.clone()).await.unwrap();
        let slow_path = root.join(relative);
        let ready = ready.clone();
        reads.push(tokio::task::spawn_blocking(move || {
            ready.wait();
            fs::read(slow_path)
        }));
    }
    io(move || ready.wait()).await;
    tokio::time::timeout(Duration::from_secs(5), source.entered.notified())
        .await
        .unwrap();
    // Give the already-started native callers time to reach the kernel's
    // background queue while the shared CAS fetch remains deliberately gated.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let metadata_path = path.clone();
    let metadata = tokio::task::spawn_blocking(move || {
        assert_eq!(fs::metadata(&metadata_path).unwrap().len(), 9);
        assert!(
            fs::read_dir(metadata_path.parent().unwrap())
                .unwrap()
                .count()
                >= 2
        );
    });
    let responsive = tokio::time::timeout(Duration::from_secs(2), metadata).await;
    source.release.add_permits(1);
    assert!(responsive.is_ok(), "metadata blocked behind a CAS read");
    for read in reads {
        assert_eq!(read.await.unwrap().unwrap(), b"slow content");
    }

    // Clean after a previously successful kernel lookup, then restage through
    // a previously negative parent to exercise both directions of invalidation.
    mount.clean("base").await.unwrap();
    let cleaned = root.join("base");
    io(move || assert!(!cleaned.exists())).await;
    mount.stage("base/nested/file", old).await.unwrap();
    io(move || assert_eq!(fs::read(path).unwrap(), b"old bytes")).await;
    mount.shutdown().await.unwrap();
    mount.shutdown().await.unwrap();
    assert!(
        fs::read_dir(&root).unwrap().next().is_none(),
        "shutdown must remove the mount"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_overlapping_state_and_nonempty_mountpoints_before_mounting() {
    let fixture = tempfile::tempdir().unwrap();
    let source = Arc::new(Source::new());
    let tree = Tree::open(
        fixture.path().join("state"),
        "test",
        source.clone(),
        CacheConfig::default(),
    )
    .unwrap();
    assert!(
        output_fuse::start(tree.clone(), &fixture.path().join("state/view"))
            .await
            .is_err()
    );
    let occupied = fixture.path().join("occupied");
    fs::create_dir(&occupied).unwrap();
    fs::write(occupied.join("preserve"), b"local").unwrap();
    assert!(output_fuse::start(tree, &occupied).await.is_err());
    assert_eq!(fs::read(occupied.join("preserve")).unwrap(), b"local");
    assert_eq!(source.fetches.load(Ordering::SeqCst), 0);
}
