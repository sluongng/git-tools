#![cfg(windows)]
use anyhow::{Context, Result};
use async_trait::async_trait;
use output_core::{Artifact, BlobSource, CacheConfig, Projection, Tree, TreeStat};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct Source {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
    fetches: AtomicUsize,
    delay: AtomicUsize,
}
impl Source {
    fn artifact(&self, bytes: &[u8]) -> Artifact {
        let hash = format!("{:x}", Sha256::digest(bytes));
        self.blobs
            .lock()
            .unwrap()
            .insert(hash.clone(), bytes.to_vec());
        Artifact {
            backend: "fixture".into(),
            instance: "test".into(),
            digest_function: 1,
            hash,
            size: bytes.len() as u64,
        }
    }
}
#[async_trait]
impl BlobSource for Source {
    async fn fetch(&self, a: &Artifact, target: &mut std::fs::File) -> Result<()> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(
            self.delay.load(Ordering::SeqCst) as u64
        ))
        .await;
        target.write_all(self.blobs.lock().unwrap().get(&a.hash).unwrap())?;
        Ok(())
    }
    async fn missing(&self, artifacts: &[Artifact]) -> Result<Vec<Artifact>> {
        Ok(artifacts
            .iter()
            .filter(|a| !self.blobs.lock().unwrap().contains_key(&a.hash))
            .cloned()
            .collect())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires enabled Windows ProjFS"]
async fn lazy_native_generations_edits_restart_and_clean() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let source = Arc::new(Source::default());
    let a = source.artifact(b"generation A\n");
    let b = source.artifact(b"generation B\n");
    let c = source.artifact(b"renamed without hydration\n");
    let tree = Tree::open(
        temp.path().join("core"),
        "fixture",
        source.clone(),
        CacheConfig::default(),
    )?;
    let root = temp.path().join("mount");
    let state = temp.path().join("provider");
    let mount = output_projfs::start(tree.clone(), &root, &state).await?;
    assert!(mount.stage("newdir/NUL.txt", a.clone()).await.is_err());
    assert!(mount.stage("newdir/a:", a.clone()).await.is_err());
    assert!(mount.ensure_dir("newdir/NUL").await.is_err());
    assert!(tree.resolve("newdir").is_err());
    std::fs::create_dir_all(root.join("native-parent/deep"))?;
    mount.stage("native-parent/deep/output", a.clone()).await?;
    assert_eq!(
        std::fs::metadata(root.join("native-parent/deep/output"))?.len(),
        a.size,
        "staged files must be visible beneath native action directories"
    );
    mount.stage("metadata-only", a.clone()).await?;
    let metadata_path = root.join("metadata-only");
    let mut permissions = std::fs::metadata(&metadata_path)?.permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&metadata_path, permissions)?;
    {
        use std::os::windows::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .access_mode(windows_sys::Win32::Storage::FileSystem::FILE_WRITE_ATTRIBUTES)
            .open(&metadata_path)?;
        file.set_modified(std::time::SystemTime::now())?;
    }
    assert!(
        matches!(mount.stat("metadata-only",1).await?,Some(TreeStat::File{artifact:Some(ref got)})if got==&a),
        "metadata-only attributes/times preserve payload digest"
    );
    assert_eq!(
        source.fetches.load(Ordering::SeqCst),
        0,
        "metadata-only dirtiness must not hydrate"
    );
    {
        use std::os::windows::ffi::OsStrExt;
        let path: Vec<_> = metadata_path
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect();
        assert_ne!(
            unsafe {
                windows_sys::Win32::Storage::FileSystem::SetFileAttributesW(
                    path.as_ptr(),
                    windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_ARCHIVE,
                )
            },
            0
        );
    }
    eprintln!("phase ensure dir");
    mount.ensure_dir("out").await?;
    mount.stage("out/file", a.clone()).await?;
    eprintln!("phase metadata");
    assert_eq!(std::fs::read_dir(root.join("out"))?.count(), 1);
    assert_eq!(std::fs::metadata(root.join("out/file"))?.len(), a.size);
    assert_eq!(
        source.fetches.load(Ordering::SeqCst),
        0,
        "metadata must not fetch CAS"
    );
    assert!(
        matches!(mount.stat("out/file",1).await?,Some(TreeStat::File{artifact:Some(ref got)})if got==&a)
    );
    assert_eq!(std::fs::read(root.join("out/file"))?, b"generation A\n");
    assert_eq!(source.fetches.load(Ordering::SeqCst), 1);
    std::fs::write(root.join("metadata-only"), vec![b'x'; a.size as usize])?;
    assert!(
        matches!(
            mount.stat("metadata-only", 1).await?,
            Some(TreeStat::File { artifact: None })
        ),
        "same-sized native writes must not retain remote digest"
    );
    eprintln!("phase generation B");
    mount.stage("out/file", b.clone()).await?;
    assert_eq!(std::fs::read(root.join("out/file"))?, b"generation B\n");
    assert_eq!(source.fetches.load(Ordering::SeqCst), 2);
    eprintln!("phase rename");
    mount.stage("out/before", c.clone()).await?;
    let retired_renamed = tree.resolve("out/before")?.id;
    std::fs::metadata(root.join("out/before"))?;
    rename_external(&root.join("out/before"), &root.join("out/after"))?;
    // The original path now publishes another generation, while the native
    // renamed placeholder must keep its own old locator across GC and restart.
    mount.stage("out/before", a.clone()).await?;
    assert!(tree.get(retired_renamed).is_err());
    assert_eq!(
        source.fetches.load(Ordering::SeqCst),
        2,
        "collection is metadata-only"
    );
    assert!(
        matches!(
            mount.stat("out/after", 1).await?,
            Some(TreeStat::File { artifact: None })
        ),
        "renamed native file must be visible without trusting another path generation"
    );
    mount.stage("out/rename-a", a.clone()).await?;
    mount.stage("out/rename-b", b.clone()).await?;
    std::fs::metadata(root.join("out/rename-a"))?;
    std::fs::metadata(root.join("out/rename-b"))?;
    rename_external(&root.join("out/rename-a"), &root.join("out/rename-b"))?;
    assert_eq!(std::fs::read(root.join("out/rename-b"))?, b"generation A\n");
    assert!(
        matches!(
            mount.stat("out/rename-b", 1).await?,
            Some(TreeStat::File { artifact: None })
        ),
        "rename-overwrite must never report stale B locator"
    );
    mount.stage("out/internal-a", a.clone()).await?;
    mount.stage("out/internal-b", b.clone()).await?;
    std::fs::metadata(root.join("out/internal-a"))?;
    std::fs::metadata(root.join("out/internal-b"))?;
    std::fs::rename(root.join("out/internal-a"), root.join("out/internal-b"))?;
    assert!(
        matches!(
            mount.stat("out/internal-b", 1).await?,
            Some(TreeStat::File { artifact: None })
        ),
        "native file identity must detect provider-process rename without notifications"
    );
    assert!(!root.join("out/negative").exists());
    mount.stage("out/negative", a.clone()).await?;
    assert_eq!(std::fs::read(root.join("out/negative"))?, b"generation A\n");
    std::fs::create_dir(root.join("local-temp"))?;
    std::fs::write(root.join("local-temp/child"), b"local action")?;
    rename_external(&root.join("local-temp"), &root.join("local-output"))?;
    assert_eq!(
        std::fs::read(root.join("local-output/child"))?,
        b"local action"
    );
    assert!(matches!(
        mount.stat("local-output/child", 1).await?,
        Some(TreeStat::File { artifact: None })
    ));
    eprintln!("phase native edit");
    std::fs::write(root.join("out/file"), b"native edit")?;
    assert!(matches!(
        mount.stat("out/file", 1).await?,
        Some(TreeStat::File { artifact: None })
    ));
    assert!(mount.stage("out/file", a.clone()).await.is_err());
    assert_eq!(std::fs::read(root.join("out/file"))?, b"native edit");
    std::fs::remove_file(root.join("out/file"))?;
    mount.stage("out/file", a.clone()).await?;
    assert_eq!(std::fs::read(root.join("out/file"))?, b"generation A\n");
    std::fs::write(root.join("out/file"), b"converted local file")?;
    mount.stage("out/offline-a", a.clone()).await?;
    mount.stage("out/offline-b", b.clone()).await?;
    std::fs::metadata(root.join("out/offline-a"))?;
    std::fs::metadata(root.join("out/offline-b"))?;
    for _ in 0..128 {
        mount.stage("out/compaction", a.clone()).await?;
    }
    assert_eq!(tree.retirement_metrics().retained_nodes, 0);
    assert!(
        std::fs::read(state.join("native-identities.log"))?
            .split(|byte| *byte == b'\n')
            .count()
            < 128,
        "retired fingerprints must not accumulate one log entry per generation"
    );
    eprintln!("phase stop and offline edit");
    mount.shutdown().await?;
    drop(mount);
    std::fs::write(root.join("out/file"), b"offline edit")?;
    let offline_rename = std::fs::rename(root.join("out/offline-a"), root.join("out/offline-b"));
    eprintln!("offline clean-placeholder rename result: {offline_rename:?}");
    let other = Tree::open(
        temp.path().join("other-core"),
        "fixture",
        source.clone(),
        CacheConfig::default(),
    )?;
    assert!(
        output_projfs::start(other, &root, &state).await.is_err(),
        "GUID state must reject a different durable core identity"
    );
    {
        use std::fs::OpenOptions;
        let mut log = OpenOptions::new()
            .append(true)
            .open(state.join("native-identities.log"))?;
        log.write_all(b"{partial-tail")?;
    }
    let mount = output_projfs::start(tree.clone(), &root, &state).await?;
    assert!(tree.get(retired_renamed).is_err());
    assert_eq!(
        std::fs::read(root.join("out/after"))?,
        b"renamed without hydration\n",
        "old persisted placeholder hydrates without retired inode metadata"
    );
    assert!(
        matches!(mount.stat("out/compaction",1).await?, Some(TreeStat::File { artifact: Some(ref got) }) if got == &a),
        "the compacted identity journal retains live digest authority across restart"
    );
    assert!(
        matches!(mount.stat("out/negative",1).await?,Some(TreeStat::File{artifact:Some(ref artifact)})if artifact==&a),
        "clean fingerprint survives restart and incomplete log tail"
    );
    assert!(matches!(
        mount.stat("out/file", 1).await?,
        Some(TreeStat::File { artifact: None })
    ));
    if offline_rename.is_ok() {
        assert!(
            matches!(
                mount.stat("out/offline-b", 1).await?,
                Some(TreeStat::File { artifact: None })
            ),
            "offline rename must not retain wrong locator"
        );
    }
    assert!(
        matches!(
            mount.stat("out/rename-b", 1).await?,
            Some(TreeStat::File { artifact: None })
        ),
        "rename distrust must survive restart"
    );
    assert!(mount.stage("out/file", b.clone()).await.is_err());
    assert_eq!(std::fs::read(root.join("out/file"))?, b"offline edit");
    eprintln!("phase clean");
    mount.clean("").await.context("clean root")?;
    eprintln!("phase cleaned root");
    assert_eq!(std::fs::read_dir(&root)?.count(), 0);
    mount.ensure_dir("out").await?;
    mount
        .stage("out/file", b)
        .await
        .context("restage after clean")?;
    eprintln!("phase restaged after clean");
    assert_eq!(std::fs::read(root.join("out/file"))?, b"generation B\n");
    mount.shutdown().await?;
    drop(mount);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires enabled Windows ProjFS"]
async fn native_publication_recovery_sharing_and_cancellation() -> Result<()> {
    use std::{fs::OpenOptions, os::windows::fs::OpenOptionsExt};
    use windows_sys::Win32::Storage::FileSystem::*;
    let temp = tempfile::tempdir()?;
    let source = Arc::new(Source::default());
    let a = source.artifact(b"A");
    let b = source.artifact(b"B");
    let tree = Tree::open(
        temp.path().join("core"),
        "fixture",
        source.clone(),
        CacheConfig::default(),
    )?;
    let root = temp.path().join("mount");
    let state = temp.path().join("provider");
    let mount = output_projfs::start(tree.clone(), &root, &state).await?;
    mount.stage("shared", a.clone()).await?;
    let held = OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ)
        .open(root.join("shared"))?;
    assert!(
        mount.stage("shared", b.clone()).await.is_err(),
        "sharing conflict must be reported"
    );
    assert_eq!(std::fs::read(root.join("shared"))?, b"A");
    drop(held);
    mount.stage("shared", b.clone()).await?;
    assert_eq!(std::fs::read(root.join("shared"))?, b"B");
    // Interrupted-before-native publication plus incomplete nonauthoritative tmp.
    let prepared = tree.prepare_stage("recovered", a.clone()).await?;
    std::fs::write(
        state.join("publication.json"),
        serde_json::to_vec(&serde_json::json!({"prepared":prepared,"rename_epoch":0}))?,
    )?;
    std::fs::write(state.join("publication.tmp"), b"{incomplete")?;
    mount.shutdown().await?;
    drop(mount);
    let mount = output_projfs::start(tree.clone(), &root, &state).await?;
    assert_eq!(std::fs::read(root.join("recovered"))?, b"A");
    assert!(!state.join("publication.json").exists());
    assert!(!state.join("publication.tmp").exists());
    // Crash-reappearing journal from a durably aborted preparation is ignored.
    let stale = tree.prepare_stage("aborted", a.clone()).await?;
    tree.abort_stage(stale.clone()).await?;
    std::fs::write(
        state.join("publication.json"),
        serde_json::to_vec(&serde_json::json!({"prepared":stale,"rename_epoch":0}))?,
    )?;
    mount.shutdown().await?;
    drop(mount);
    let mount = output_projfs::start(tree.clone(), &root, &state).await?;
    assert!(!root.join("aborted").exists());
    assert!(!state.join("publication.json").exists());
    // Optional identity-cache loss must disable digest reuse, never turn two
    // missing fingerprints into a match or resurrect a native tombstone.
    mount.stage("lost-identity", a.clone()).await?;
    mount.shutdown().await?;
    drop(mount);
    std::fs::write(state.join("native-identities.log"), b"")?;
    let mount = output_projfs::start(tree.clone(), &root, &state).await?;
    assert!(
        matches!(
            mount.stat("lost-identity", 1).await?,
            Some(TreeStat::File { artifact: None })
        ),
        "missing native identity must disable digest reuse"
    );
    let output = std::process::Command::new(python()?)
        .args(["-c", "import os,sys;os.unlink(sys.argv[1])"])
        .arg(root.join("lost-identity"))
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "native client deletion: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        mount.stat("lost-identity", 1).await?.is_none(),
        "native tombstone must beat remote locator after identity-cache loss"
    );
    mount.shutdown().await?;
    drop(mount);
    let mount = output_projfs::start(tree.clone(), &root, &state).await?;
    assert!(
        mount.stat("lost-identity", 1).await?.is_none(),
        "native tombstone remains absent after restart"
    );
    // A clean operation must durably supersede a pending stage, not resurrect it.
    let prepared = tree.prepare_stage("interrupted", a.clone()).await?;
    std::fs::write(
        state.join("publication.json"),
        serde_json::to_vec(&serde_json::json!({"prepared":prepared,"rename_epoch":0}))?,
    )?;
    mount.clean("").await?;
    mount.shutdown().await?;
    drop(mount);
    let mount = output_projfs::start(tree.clone(), &root, &state).await?;
    assert!(!root.join("interrupted").exists());
    source.delay.store(2000, Ordering::SeqCst);
    let mut paths = Vec::new();
    for index in 0..16 {
        let bytes = vec![index as u8 + 1; 65536];
        let artifact = source.artifact(&bytes);
        let name = format!("cancel-{index}");
        mount.stage(&name, artifact).await?;
        paths.push(root.join(name));
    }
    let output = std::process::Command::new(python()?)
        .arg("-c")
        .arg(include_str!("support/cancel_reads.py"))
        .args(&paths)
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "native cancel client: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    for _ in 0..100 {
        if mount.metrics().pending == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(mount.metrics().pending, 0);
    eprintln!("native cancellation metrics: {:?}", mount.metrics());
    source.delay.store(0, Ordering::SeqCst);
    assert_eq!(std::fs::read(root.join("cancel-0"))?, vec![1; 65536]);
    // Stop with 32 native clients queued in verified CAS hydration. No worker
    // may write through a context after StopVirtualizing returns.
    source.delay.store(1000, Ordering::SeqCst);
    let mut paths = Vec::new();
    for index in 0..32 {
        let bytes = vec![index as u8 + 32; 65536];
        let artifact = source.artifact(&bytes);
        let name = format!("stop-{index}");
        mount.stage(&name, artifact).await?;
        paths.push(root.join(name));
    }
    let baseline = source.fetches.load(Ordering::SeqCst);
    let mut child = std::process::Command::new(python()?)
        .arg("-c")
        .arg(include_str!("support/cancel_reads.py"))
        .arg("--shutdown")
        .args(&paths)
        .spawn()?;
    for _ in 0..200 {
        if source.fetches.load(Ordering::SeqCst) > baseline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(source.fetches.load(Ordering::SeqCst) > baseline);
    mount.shutdown().await?;
    let mut status = None;
    for _ in 0..300 {
        if let Some(s) = child.try_wait()? {
            status = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if status.is_none() {
        child.kill()?;
        child.wait()?;
        anyhow::bail!("native readers did not stop")
    }
    assert!(status.unwrap().success());
    for _ in 0..100 {
        if mount.metrics().pending == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(mount.metrics().pending, 0);
    drop(mount);
    let mount = output_projfs::start(tree.clone(), &root, &state).await?;
    source.delay.store(0, Ordering::SeqCst);
    assert_eq!(std::fs::read(root.join("stop-0"))?, vec![32; 65536]);
    // Clean removes directory junctions without following them outside the root.
    let outside = temp.path().join("outside");
    std::fs::create_dir(&outside)?;
    std::fs::write(outside.join("sentinel"), b"keep")?;
    let status = std::process::Command::new("cmd")
        .args(["/c", "mklink", "/J"])
        .arg(root.join("junction"))
        .arg(&outside)
        .status()?;
    assert!(status.success());
    mount.clean("").await?;
    assert_eq!(std::fs::read(outside.join("sentinel"))?, b"keep");
    mount.shutdown().await?;
    drop(mount);
    Ok(())
}

fn python() -> Result<std::ffi::OsString> {
    std::env::var_os("OUTPUT_PROJFS_TEST_PYTHON")
        .context("set OUTPUT_PROJFS_TEST_PYTHON to native Python 3 for external client tests")
}
fn rename_external(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    let output = std::process::Command::new(python()?)
        .args(["-c", "import os,sys;os.replace(sys.argv[1],sys.argv[2])"])
        .arg(from)
        .arg(to)
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "external rename: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires enabled Windows ProjFS"]
async fn late_old_hydration_cannot_replace_new_generation() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let source = Arc::new(Source::default());
    let a = source.artifact(b"old generation A payload");
    let b = source.artifact(b"new generation B payload");
    let tree = Tree::open(
        temp.path().join("core"),
        "fixture",
        source.clone(),
        CacheConfig::default(),
    )?;
    let root = temp.path().join("mount");
    let state = temp.path().join("provider");
    let mount = output_projfs::start(tree.clone(), &root, &state).await?;
    mount.stage("race", a).await?;
    let old_node = tree.resolve("race")?.id;
    source.delay.store(1000, Ordering::SeqCst);
    let before = source.fetches.load(Ordering::SeqCst);
    let mut child = std::process::Command::new(python()?)
        .arg("-c")
        .arg(include_str!("support/read_once.py"))
        .arg(root.join("race"))
        .arg("old generation A payload")
        .spawn()?;
    for _ in 0..200 {
        if source.fetches.load(Ordering::SeqCst) > before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        source.fetches.load(Ordering::SeqCst) > before,
        "old generation must be fetching"
    );
    let publication = mount.stage("race", b.clone()).await;
    if publication.is_ok() {
        assert!(
            tree.get(old_node).is_err(),
            "pending read owns its locator, not inode metadata"
        );
    }
    eprintln!("B publication during A fetch: {publication:?}");
    let mut status = None;
    for _ in 0..200 {
        if let Some(s) = child.try_wait()? {
            status = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if status.is_none() {
        child.kill()?;
        child.wait()?;
        anyhow::bail!("old generation read did not drain")
    }
    assert!(status.unwrap().success());
    source.delay.store(0, Ordering::SeqCst);
    if publication.is_err() {
        mount.stage("race", b.clone()).await?;
    }
    assert_eq!(
        std::fs::read(root.join("race"))?,
        b"new generation B payload",
        "late A data must not overwrite visible B"
    );
    match mount.stat("race", 1).await? {
        Some(TreeStat::File {
            artifact: Some(locator),
        }) => assert_eq!(locator, b),
        Some(TreeStat::File { artifact: None }) => {}
        other => panic!("new generation metadata missing: {other:?}"),
    }
    mount.shutdown().await?;
    drop(mount);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires enabled Windows ProjFS and NTFS directory/file oplocks"]
async fn native_source_capture_uses_ntfs_edits_without_reading_untouched_payloads() -> Result<()> {
    use output_core::{SourceContent, SourceEntry, SourceLimits, SourceManifest};
    use output_projfs::{NativeCaptureError, NativeCaptureLimits, NativeSourceContent};
    use private_fs::PrivateDir;
    let temp = tempfile::tempdir()?;
    let source = Arc::new(Source::default());
    let artifact = |bytes: &[u8]| {
        let hash = blake3::hash(bytes).to_hex().to_string();
        source
            .blobs
            .lock()
            .unwrap()
            .insert(hash.clone(), bytes.to_vec());
        Artifact {
            backend: "fixture".into(),
            instance: "native-source".into(),
            digest_function: 9,
            hash,
            size: bytes.len() as u64,
        }
    };
    let original = artifact(b"original native contents\n");
    let untouched = Artifact {
        backend: "fixture".into(),
        instance: "native-source".into(),
        digest_function: 9,
        hash: "ab".repeat(32),
        size: 8 * 1024 * 1024,
    };
    // Untouched content is deliberately unavailable: a metadata-only capture
    // must not attempt hydration at all.
    let tree = Tree::open(
        temp.path().join("core"),
        "fixture",
        source.clone(),
        CacheConfig::default(),
    )?;
    let limits = SourceLimits::default();
    tree.install_source(
        SourceManifest {
            identity: tree.identity().into(),
            root: [7; 32],
            entries: vec![
                SourceEntry {
                    path: "delete-me".into(),
                    mode: 0o644,
                    content: SourceContent::File(original.clone()),
                },
                SourceEntry {
                    path: "edit-me".into(),
                    mode: 0o644,
                    content: SourceContent::File(original.clone()),
                },
                SourceEntry {
                    path: "rename-before".into(),
                    mode: 0o644,
                    content: SourceContent::File(original.clone()),
                },
                SourceEntry {
                    path: "untouched".into(),
                    mode: 0o755,
                    content: SourceContent::File(untouched.clone()),
                },
            ],
        },
        None,
        "initial-source".into(),
        &limits,
    )?;
    tree.install_source_control(b"fixture-native-source-marker", &limits)?;
    let root = PrivateDir::create(temp.path().join("mount"))?;
    let state = PrivateDir::create(temp.path().join("provider"))?;
    let captures = Arc::new(PrivateDir::create(temp.path().join("captures"))?);
    let mount = Arc::new(output_projfs::start(tree.clone(), root.path(), state.path()).await?);
    std::fs::metadata(root.path().join("delete-me"))?;
    std::fs::metadata(root.path().join("rename-before"))?;
    std::fs::remove_file(root.path().join("delete-me"))?;
    std::fs::rename(
        root.path().join("rename-before"),
        root.path().join("rename-after"),
    )?;
    std::fs::write(root.path().join("edit-me"), b"changed! native contents\n")?;
    std::fs::create_dir(root.path().join("untracked"))?;
    std::fs::write(root.path().join("untracked/new"), b"new native file")?;
    assert_eq!(
        std::fs::read(root.path().join(".jj/native-workspace"))?,
        b"fixture-native-source-marker",
        "fresh control marker is real NTFS bytes"
    );

    let capture = mount
        .capture_source(captures.clone(), NativeCaptureLimits::default())
        .await?;
    assert!(
        capture
            .entries()
            .iter()
            .all(|entry| entry.path != "delete-me"
                && entry.path != "rename-before"
                && !entry.path.starts_with(".jj"))
    );
    let unchanged = capture
        .entries()
        .iter()
        .find(|entry| entry.path == "untouched")
        .unwrap();
    assert_eq!(unchanged.mode, 0o755);
    assert!(matches!(&unchanged.content, NativeSourceContent::Remote(got) if got == &untouched));
    let edited = capture
        .entries()
        .iter()
        .find(|entry| entry.path == "edit-me")
        .unwrap();
    let held = match &edited.content {
        NativeSourceContent::Local(file) => file.clone(),
        _ => panic!("NTFS edit was omitted"),
    };
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut held.open()?, &mut bytes)?;
    assert_eq!(bytes, b"changed! native contents\n");
    assert_eq!(held.blake3(), *blake3::hash(&bytes).as_bytes());
    assert!(
        capture
            .entries()
            .iter()
            .any(|entry| entry.path == "untracked/new"
                && matches!(entry.content, NativeSourceContent::Local(_)))
    );
    assert!(
        capture
            .entries()
            .iter()
            .any(|entry| entry.path == "rename-after")
    );
    assert_eq!(
        source.fetches.load(Ordering::SeqCst),
        1,
        "only renamed original contents hydrate"
    );
    assert!(
        std::fs::write(root.path().join("edit-me"), b"racing writer").is_err(),
        "held capture must exclude writes to captured native files"
    );
    capture.validate_still_current()?;
    // Directory R oplocks are advisory; a native namespace write is permitted
    // but invalidates the whole capture, including provider-process writes.
    std::fs::write(root.path().join("late-new-file"), b"late change")?;
    let error = capture.validate_still_current().unwrap_err();
    assert_eq!(
        error.downcast_ref::<NativeCaptureError>(),
        Some(&NativeCaptureError::Busy)
    );
    let expected = capture.version().clone();
    let mut reader = held.open()?;
    assert!(
        capture
            .acknowledge_root([8; 32], expected, "stale-capture".into(), &limits)
            .is_err()
    );
    std::fs::write(root.path().join("edit-me"), b"after capture")?;
    let mut retained = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut retained)?;
    assert_eq!(
        retained, b"changed! native contents\n",
        "private capture survives native mutation and guard drop"
    );
    drop(reader);
    drop(held);
    mount.shutdown().await?;
    drop(mount);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires enabled Windows ProjFS and NTFS directory/file oplocks"]
async fn native_source_capture_rejects_active_writers_and_foreign_root_binding() -> Result<()> {
    use output_core::{SourceContent, SourceEntry, SourceLimits, SourceManifest};
    use output_projfs::{NativeCaptureLimits, SourceRootGuard};
    use private_fs::PrivateDir;
    let temp = tempfile::tempdir()?;
    let source = Arc::new(Source::default());
    let artifact = source.artifact(b"native source");
    let tree = Tree::open(
        temp.path().join("core"),
        "fixture",
        source,
        CacheConfig::default(),
    )?;
    tree.install_source(
        SourceManifest {
            identity: tree.identity().into(),
            root: [9; 32],
            entries: vec![SourceEntry {
                path: "file".into(),
                mode: 0o644,
                content: SourceContent::File(artifact),
            }],
        },
        None,
        "initial-source".into(),
        &SourceLimits::default(),
    )?;
    tree.install_source_control(b"fixture-native-source-marker", &SourceLimits::default())?;
    let root_path = temp.path().join("mount");
    let root = PrivateDir::create(&root_path)?;
    let state = PrivateDir::create(temp.path().join("provider"))?;
    let captures = Arc::new(PrivateDir::create(temp.path().join("captures"))?);
    let mount = Arc::new(output_projfs::start(tree.clone(), &root_path, state.path()).await?);
    std::fs::write(root_path.join("file"), b"dirty")?;
    let writer = std::fs::OpenOptions::new()
        .write(true)
        .open(root_path.join("file"))?;
    assert!(
        mount
            .capture_source(captures, NativeCaptureLimits::default())
            .await
            .is_err()
    );
    drop(writer);
    let binding = SourceRootGuard::open(&root_path, state.path(), tree.identity())?;
    assert!(SourceRootGuard::open(&root_path, state.path(), "another-tree").is_err());
    assert!(
        std::fs::rename(&root_path, temp.path().join("moved")).is_err(),
        "root identity is pinned"
    );
    drop(binding);
    mount.shutdown().await?;
    drop(mount);
    drop(root);
    let moved = temp.path().join("moved");
    std::fs::rename(&root_path, &moved)?;
    let replacement = PrivateDir::create(&root_path)?;
    let id = windows_sys::core::GUID {
        data1: 1,
        data2: 2,
        data3: 3,
        data4: [4; 8],
    };
    use std::os::windows::ffi::OsStrExt;
    let name: Vec<_> = root_path.as_os_str().encode_wide().chain(Some(0)).collect();
    let result = unsafe {
        windows_sys::Win32::Storage::ProjectedFileSystem::PrjMarkDirectoryAsPlaceholder(
            name.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            &id,
        )
    };
    assert!(result >= 0);
    assert!(
        SourceRootGuard::open(&root_path, state.path(), tree.identity()).is_err(),
        "matching paths/manifest cannot adopt a replacement NTFS directory"
    );
    drop(replacement);
    std::fs::remove_dir(&root_path)?;
    std::fs::rename(&moved, &root_path)?;
    let _restored = SourceRootGuard::open(&root_path, state.path(), tree.identity())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires enabled Windows ProjFS and NTFS control-file oplocks"]
async fn native_source_control_is_real_readable_and_tampering_never_repairs_silently() -> Result<()>
{
    use output_core::{SourceLimits, SourceManifest};
    use private_fs::PrivateDir;
    let temp = tempfile::tempdir()?;
    let source = Arc::new(Source::default());
    let tree = Tree::open(
        temp.path().join("core"),
        "fixture",
        source.clone(),
        CacheConfig::default(),
    )?;
    let limits = SourceLimits::default();
    tree.install_source(
        SourceManifest {
            identity: tree.identity().into(),
            root: [4; 32],
            entries: vec![],
        },
        None,
        "initial-source",
        &limits,
    )?;
    let expected = b"opaque-native-source-control";
    tree.install_source_control(expected, &limits)?;
    let root = PrivateDir::create(temp.path().join("mount"))?;
    let state = PrivateDir::create(temp.path().join("provider"))?;
    let mount = Arc::new(output_projfs::start(tree.clone(), root.path(), state.path()).await?);
    let marker = root.path().join(".jj/native-workspace");
    assert_eq!(std::fs::read(&marker)?, expected);
    assert_eq!(
        source.fetches.load(Ordering::SeqCst),
        0,
        "marker is not a sparse CAS placeholder"
    );
    let captures = Arc::new(PrivateDir::create(temp.path().join("captures"))?);
    let clean = mount
        .capture_source(
            captures.clone(),
            output_projfs::NativeCaptureLimits::default(),
        )
        .await?;
    assert!(
        clean.entries().is_empty(),
        "administration is not repository content"
    );
    drop(clean);
    // These calls deliberately originate in the provider process too: kernel
    // read/identity leases must enforce them without notification callbacks.
    assert!(std::fs::write(&marker, b"truncated").is_err());
    assert!(std::fs::remove_file(&marker).is_err());
    assert!(std::fs::rename(&marker, root.path().join("moved-marker")).is_err());
    assert!(
        std::fs::rename(
            root.path().join(".jj"),
            root.path().join("moved-administration")
        )
        .is_err()
    );
    assert_eq!(std::fs::read(&marker)?, expected);
    let extra = root.path().join(".jj/unexpected");
    let _ = std::fs::write(&extra, b"unpreventable native administration change");
    // Post-create denial semantics vary; if NTFS retains an entry despite a
    // callback failure (or no provider-process callback), it must block capture.
    if extra.try_exists()? {
        assert!(
            mount
                .capture_source(captures, output_projfs::NativeCaptureLimits::default())
                .await
                .is_err()
        );
        assert_eq!(
            tree.source_baseline()?.0,
            [4; 32],
            "no acknowledgment after administration change"
        );
    }
    mount.shutdown().await?;
    drop(mount);
    if extra.try_exists()? {
        std::fs::remove_file(extra)?;
    }
    // Once the provider is stopped its kernel leases are gone. A recognized
    // restart must reject changed bytes, not overwrite them from the core copy.
    std::fs::write(&marker, b"changed while stopped")?;
    assert!(
        output_projfs::start(tree.clone(), root.path(), state.path())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&marker)?, b"changed while stopped");
    Ok(())
}
