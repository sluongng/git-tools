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
    std::fs::metadata(root.join("out/before"))?;
    rename_external(&root.join("out/before"), &root.join("out/after"))?;
    assert_eq!(
        std::fs::read(root.join("out/after"))?,
        b"renamed without hydration\n"
    );
    assert_eq!(source.fetches.load(Ordering::SeqCst), 3);
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
    let mount = output_projfs::start(tree, &root, &state).await?;
    mount.stage("race", a).await?;
    source.delay.store(1000, Ordering::SeqCst);
    let before = source.fetches.load(Ordering::SeqCst);
    let mut child = std::process::Command::new(python()?)
        .arg("-c")
        .arg(include_str!("support/read_once.py"))
        .arg(root.join("race"))
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
