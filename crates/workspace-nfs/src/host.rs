//! Reusable loopback export hosting. Native mounting stays explicit: callers
//! must unmount the client before stopping an export, never strand a live mount.
use crate::GitNfs;
use anyhow::{Result, ensure};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
};
use workspace_core::{backend::GitRepo, filesystem::Snapshot};
use workspace_overlay::{overlay::Overlay, worktree::WorkingTree};

pub struct ExportConfig {
    pub mountpoint: PathBuf,
    pub overlay: PathBuf,
    pub listen: SocketAddrV4,
    pub git_vfs_executable: PathBuf,
}

pub struct ExportHost {
    fs: GitNfs,
    port: u16,
    worker: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}
impl ExportHost {
    /// The supplied repository may already hold a shared source CAS.
    pub async fn start(repo: GitRepo, snapshot: Snapshot, config: ExportConfig) -> Result<Self> {
        ensure!(
            *config.listen.ip() == Ipv4Addr::LOCALHOST,
            "only loopback exports are supported"
        );
        std::fs::create_dir_all(&config.mountpoint)?;
        let root = config.mountpoint.canonicalize()?;
        let working = WorkingTree::lock(&config.overlay)?;
        ensure!(
            !working.directory.starts_with(&root) && !root.starts_with(&working.directory),
            "mountpoint and overlay must not overlap"
        );
        ensure!(
            !repo.repository_path().starts_with(&root) && !root.starts_with(repo.repository_path()),
            "mountpoint and repository must not overlap"
        );
        let overlay = Overlay::open(&working.directory, &snapshot, &working.gitdir)?;
        working.prepare(&repo, &snapshot, &root, &config.git_vfs_executable)?;
        let fs = GitNfs::new(repo, overlay, working)?;
        let listener = NFSTcpListener::bind(&config.listen.to_string(), fs.clone()).await?;
        let port = listener.get_listen_port();
        let worker = tokio::spawn(async move { listener.handle_forever().await });
        Ok(Self {
            fs,
            port,
            worker: Some(worker),
        })
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn filesystem(&self) -> GitNfs {
        self.fs.clone()
    }
    pub fn is_finished(&self) -> bool {
        self.worker
            .as_ref()
            .is_none_or(|worker| worker.is_finished())
    }
    /// The native client must be unmounted first. Existing TCP clients receive
    /// STALE; their references retain the overlay lock until they disconnect.
    pub async fn shutdown(&mut self) -> Result<()> {
        self.fs.close().await?;
        if let Some(worker) = self.worker.take() {
            worker.abort();
            match worker.await {
                Ok(result) => result?,
                Err(error) if error.is_cancelled() => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}
impl Drop for ExportHost {
    fn drop(&mut self) {
        self.fs.close_admission();
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}
