//! Owned in-process source mounts. No process-global signal handler is installed.
use crate::backend::{ContentConfig, ContentHandle, GitRepo};
use crate::filesystem::{GitFs, Snapshot};
use crate::overlay::Overlay;
use crate::prefetch::{BazelPrefetchPolicy, prefetch_bazel_with_policy};
use crate::worktree::WorkingTree;
use crate::writable::WritableGitFs;
use anyhow::{Context, Result, bail};
use fuser::MountOption;
use std::path::{Path, PathBuf};

pub struct MountConfig {
    pub mountpoint: PathBuf,
    pub read_only: bool,
    pub overlay: Option<PathBuf>,
    pub prefetch: Option<BazelPrefetchPolicy>,
    pub blake3: bool,
    pub content: ContentConfig,
    /// git-vfs executable used by native Git's fsmonitor bridge, not this daemon.
    pub git_vfs_executable: PathBuf,
}

pub struct SourceMount {
    session: MountedSession,
    working: Option<WorkingTree>,
    background: Option<BackgroundPrefetch>,
    content: ContentHandle,
    stats: std::sync::Arc<crate::blake3_xattr::Blake3XattrStats>,
}
impl SourceMount {
    pub fn start(mut repo: GitRepo, snapshot: Snapshot, config: MountConfig) -> Result<Self> {
        let mountpoint = &config.mountpoint;
        let mountpoint = mountpoint
            .canonicalize()
            .context("mountpoint must already exist")?;
        if !mountpoint.is_dir() || mountpoint.read_dir()?.next().is_some() {
            bail!("mountpoint must be an empty directory");
        }
        let repository = repo.repository_path().canonicalize()?;
        if repository.starts_with(&mountpoint) || mountpoint.starts_with(&repository) {
            bail!("mountpoint and bare repository must not overlap");
        }
        let commit = snapshot.commit.clone();
        // Demand-only mounts need the same native-process cancellation scope as
        // prefetch mounts before we can safely join their dispatch thread.
        let content_owner = crate::backend::ContentScheduler::new(&mut repo, config.content)?;
        let content = content_owner.handle();
        repo.set_content_handle(content.clone());
        let (background_repo, mount_cancellation) = repo.background_handle()?;
        let background = config
            .prefetch
            .map(|policy| {
                policy.validate(&snapshot)?;
                Ok::<_, anyhow::Error>((
                    background_repo,
                    snapshot.clone(),
                    policy,
                    mount_cancellation.clone(),
                ))
            })
            .transpose()?;
        let mut options = vec![
            MountOption::NoDev,
            MountOption::NoSuid,
            MountOption::DefaultPermissions,
            MountOption::FSName("git-vfs".into()),
        ];
        let stats = std::sync::Arc::new(crate::blake3_xattr::Blake3XattrStats::default());
        let working;
        let session = if config.read_only {
            options.push(MountOption::RO);
            working = None;
            {
                let fs = GitFs::new(snapshot, repo).with_content_scheduler(content_owner);
                let fs = if config.blake3 {
                    fs.with_blake3(stats.clone())
                } else {
                    fs
                };
                spawn_session(fs, &mountpoint, &options, mount_cancellation.clone())
            }
        } else {
            options.push(MountOption::RW);
            let directory = config
                .overlay
                .as_deref()
                .map(Path::to_owned)
                .unwrap_or_else(|| repository.join("git-vfs-overlays").join(&commit));
            let state = WorkingTree::lock(&directory)?;
            if state.directory.starts_with(&mountpoint) || mountpoint.starts_with(&state.directory)
            {
                bail!("mountpoint and writable overlay must not overlap");
            }
            let overlay = Overlay::open(&state.directory, &snapshot, &state.gitdir)?;
            state.prepare(&repo, &snapshot, &mountpoint, &config.git_vfs_executable)?;
            eprintln!("Writable overlay: {}", state.directory.display());
            working = Some(state);
            drop(snapshot);
            {
                let fs = WritableGitFs::new(overlay, repo).with_content_scheduler(content_owner);
                let fs = if config.blake3 {
                    fs.with_blake3(stats.clone())
                } else {
                    fs
                };
                spawn_session(fs, &mountpoint, &options, mount_cancellation.clone())
            }
        }
        .context("mounting git-vfs (check /dev/fuse and fusermount3)")?;
        // The mount is live before speculative content transfer starts. This uses a
        // second in-process backend: a separate CLI would conflict with Fjall's lock.
        let background = background.map(|(mut repo, snapshot, policy, cancellation)| {
        let worker_cancellation = cancellation.clone();
        let worker = std::thread::spawn(move || {
            let started = std::time::Instant::now();
            match prefetch_bazel_with_policy(&mut repo, &snapshot, &policy) {
                Ok(report) => eprintln!(
                    "Background prefetch: {}",
                    serde_json::json!({"seconds": started.elapsed().as_secs_f64(), "report": report})
                ),
                Err(_) => eprintln!(
                    "Background prefetch failed: {}",
                    serde_json::json!({"seconds": started.elapsed().as_secs_f64(), "cancelled": worker_cancellation.is_cancelled()})
                ),
            }
        });
        BackgroundPrefetch { cancellation, worker: Some(worker) }
    });

        Ok(Self {
            session,
            working,
            background,
            content,
            stats,
        })
    }
    pub fn is_finished(&self) -> bool {
        self.session
            .worker
            .as_ref()
            .is_none_or(|worker| worker.is_finished())
    }
    pub fn blake3_report(&self) -> crate::blake3_xattr::Blake3XattrReport {
        self.stats.report()
    }
    /// Cancel prefetch and pending native Git operations, stop dispatch, then
    /// release overlay ownership. Does not delete authoritative workspace data.
    pub fn shutdown(&mut self) -> Result<()> {
        self.content.cancel();
        self.session.cancellation.cancel();
        self.background.take();
        self.session.shutdown()?;
        let state = self.content.stats();
        anyhow::ensure!(state.worker_failures == 0, "content worker failed");
        anyhow::ensure!(
            state.outstanding_requests == 0 && state.inflight_oids == 0,
            "content requests remain after dispatch shutdown"
        );
        self.working.take();
        Ok(())
    }
}
impl Drop for SourceMount {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

// Own dispatch separately from mount detachment: client descriptors can outlive
// a lazy unmount, but must not prevent our filesystem/database teardown.
struct MountedSession {
    unmount: fuser::SessionUnmounter,
    worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    cancellation: crate::backend::BackgroundCancellation,
}

impl MountedSession {
    fn shutdown(&mut self) -> Result<()> {
        if self.worker.is_none() {
            return Ok(());
        }
        self.cancellation.cancel();
        let unmount = self.unmount.unmount();
        // Let dispatch service any final unmount requests before stopping it.
        // A lazy detach may leave client handles alive, so it is not itself a
        // termination signal for the receive loop.
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        let dispatch = self.worker.take().map(|worker| worker.join());
        unmount.context("unmount git-vfs")?;
        if let Some(dispatch) = dispatch {
            dispatch
                .map_err(|_| anyhow::anyhow!("git-vfs dispatch thread panicked"))?
                .context("stop git-vfs dispatch")?;
        }
        Ok(())
    }
}

impl Drop for MountedSession {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn spawn_session<F: fuser::Filesystem + Send + 'static>(
    fs: F,
    mountpoint: &Path,
    options: &[MountOption],
    cancellation: crate::backend::BackgroundCancellation,
) -> std::io::Result<MountedSession> {
    let mut session = fuser::Session::new(fs, mountpoint, options)?;
    let unmount = session.unmount_callable();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker_stop = stop.clone();
    let worker = std::thread::Builder::new()
        .name("git-vfs-dispatch".into())
        .spawn(move || session.run_until(&worker_stop))?;
    Ok(MountedSession {
        unmount,
        worker: Some(worker),
        stop,
        cancellation,
    })
}

struct BackgroundPrefetch {
    cancellation: crate::backend::BackgroundCancellation,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Drop for BackgroundPrefetch {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
