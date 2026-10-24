use super::*;
#[cfg(not(target_os = "macos"))]
use anyhow::ensure;
use output_core::{
    MaintenanceOutcome, SourceFrontendFence, SourceLimits, SourceManifest, SourceReceipt,
    SourceSnapshot, SourceVersion,
};

/// Native source capture is deliberately disabled until the macOS client
/// dirty-page flush/COMMIT barrier has passed real native acceptance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NfsSourceUnsupported;
impl std::fmt::Display for NfsSourceUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NFS source capture/checkout is unsupported until the native client flush barrier is validated")
    }
}
impl std::error::Error for NfsSourceUnsupported {}

/// Caller-visible native availability. RecoveryRequired is never a healthy
/// mount claim: explicit maintenance can retry after its reported cause clears.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NfsMountState {
    Mounted,
    RecoveryRequired,
    Stopped,
}

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
struct NativeSession {
    fs: OutputNfs,
    #[cfg(target_os = "macos")]
    identity: Option<MountIdentity>,
    #[cfg(target_os = "macos")]
    export: String,
    #[cfg(target_os = "macos")]
    port: u16,
    #[cfg(target_os = "macos")]
    stop: tokio::sync::watch::Sender<bool>,
    #[cfg(target_os = "macos")]
    server: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    #[cfg(target_os = "macos")]
    mount_child: Option<tokio::process::Child>,
    #[cfg(target_os = "macos")]
    stop_failure: Option<String>,
    #[cfg(target_os = "macos")]
    unmount_pending: Option<tokio::task::JoinHandle<Result<Option<std::io::Error>>>>,
}
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
enum SessionState {
    Mounted(NativeSession),
    Recovery {
        session: Option<NativeSession>,
        reason: String,
    },
    Transitioning,
    Stopped,
}

/// Owns the native client and every server task. Quiesce native users and RPC
/// admission before explicit maintenance; rotation briefly unmounts the root.
/// Busy unmount retains the exact serving session and all authoritative bytes.
pub struct NfsMount {
    tree: Arc<Tree>,
    root: PathBuf,
    control: Arc<AsyncMutex<()>>,
    session: Arc<Mutex<SessionState>>,
}
/// A failed start can retain a possibly-mounted server. Keep this error and use
/// its recovery handle to retry maintenance/shutdown; do not replace its root.
pub struct NfsStartError {
    mount: NfsMount,
    cause: anyhow::Error,
}
impl NfsStartError {
    pub fn recovery(&self) -> &NfsMount {
        &self.mount
    }
}
impl std::fmt::Debug for NfsStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NfsStartError")
            .field("cause", &self.cause)
            .field("state", &self.mount.state())
            .finish()
    }
}
impl std::fmt::Display for NfsStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "native NFS start needs recovery: {}", self.cause)
    }
}
impl std::error::Error for NfsStartError {}

impl NfsMount {
    pub async fn start(tree: Arc<Tree>, root: impl AsRef<Path>) -> Result<Self> {
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (tree, root);
            bail!("output-nfs native mounting is supported only on macOS")
        }
        #[cfg(target_os = "macos")]
        {
            ensure_unmounted(root.as_ref())?;
            std::fs::create_dir_all(root.as_ref())?;
            let root = root.as_ref().canonicalize()?;
            ensure!(
                std::fs::read_dir(&root)?.next().is_none(),
                "output mountpoint must be empty"
            );
            let mount = Self {
                tree,
                root,
                control: Arc::new(AsyncMutex::new(())),
                session: Arc::new(Mutex::new(SessionState::Recovery {
                    session: None,
                    reason: "not mounted yet".into(),
                })),
            };
            match mount.maintain_quiescent(64).await {
                Ok(_) => Ok(mount),
                Err(cause) => Err(NfsStartError { mount, cause }.into()),
            }
        }
    }
    pub fn state(&self) -> NfsMountState {
        match &*self.session.lock().unwrap() {
            SessionState::Mounted(_) => NfsMountState::Mounted,
            SessionState::Stopped => NfsMountState::Stopped,
            _ => NfsMountState::RecoveryRequired,
        }
    }
    // Cancellation before admission changes nothing. Once admitted, this task
    // owns the session through unmount, complete task drain and remount/recovery.
    pub async fn maintain_quiescent(
        &self,
        transaction_budget: usize,
    ) -> Result<MaintenanceOutcome> {
        #[cfg(not(target_os = "macos"))]
        {
            let _ = transaction_budget;
            bail!("native NFS maintenance requires macOS")
        }
        #[cfg(target_os = "macos")]
        {
            let guard = self.control.clone().lock_owned().await;
            let state = self.session.clone();
            let tree = self.tree.clone();
            let root = self.root.clone();
            tokio::spawn(async move {
                let _guard = guard;
                maintain(&state, &tree, &root, transaction_budget).await
            })
            .await
            .context("native maintenance task failed")?
        }
    }
    /// The caller must stop all native users before requesting capture. Tree's
    /// mutation barrier cannot flush dirty pages retained by the NFS client.
    /// This currently fails without freezing/changing the tree or rotating the
    /// mount; metadata-only open+fsync/COMMIT needs native validation first.
    pub async fn capture_source_quiescent(&self, _limits: SourceLimits) -> Result<SourceSnapshot> {
        Err(NfsSourceUnsupported.into())
    }
    async fn complete<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(NfsPublisher) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let guard = self.control.clone().lock_owned().await;
        let fs = match &*self.session.lock().unwrap() {
            SessionState::Mounted(session) => session.fs.clone(),
            SessionState::Recovery { reason, .. } => {
                bail!("native output unavailable; retry quiescent maintenance: {reason}")
            }
            _ => bail!("native output is not mounted"),
        };
        let publisher = NfsPublisher {
            fs,
            root: self.root.clone(),
        };
        tokio::spawn(async move {
            let _guard = guard;
            operation(publisher).await
        })
        .await
        .context("native publication task failed")?
    }
    async fn shutdown_owned(&self) -> Result<()> {
        #[cfg(not(target_os = "macos"))]
        {
            bail!("native NFS shutdown requires macOS")
        }
        #[cfg(target_os = "macos")]
        {
            let guard = self.control.clone().lock_owned().await;
            let state = self.session.clone();
            let root = self.root.clone();
            tokio::spawn(async move {
                let _guard = guard;
                let previous =
                    std::mem::replace(&mut *state.lock().unwrap(), SessionState::Transitioning);
                let old = match previous {
                    SessionState::Mounted(old)
                    | SessionState::Recovery {
                        session: Some(old), ..
                    } => Some(old),
                    SessionState::Recovery { session: None, .. } | SessionState::Stopped => None,
                    SessionState::Transitioning => {
                        *state.lock().unwrap() = SessionState::Recovery {
                            session: None,
                            reason: "interrupted transition".into(),
                        };
                        bail!("native transition needs recovery")
                    }
                };
                if let Some(mut old) = old {
                    match old.unmount_owned(&root).await {
                        Ok(false) => {
                            *state.lock().unwrap() = SessionState::Mounted(old);
                            bail!("native output busy; mounted server retained")
                        }
                        Err(error) => {
                            // If a replacement owns this path, our detached
                            // server may stop, but the replacement must survive.
                            if old.replaced_at(&root).unwrap_or(false) {
                                if let Err(error) = old.stop().await {
                                    *state.lock().unwrap() = SessionState::Recovery {
                                        session: Some(old),
                                        reason: error.to_string(),
                                    };
                                    return Err(error);
                                }
                            } else {
                                *state.lock().unwrap() = SessionState::Recovery {
                                    session: Some(old),
                                    reason: error.to_string(),
                                };
                                return Err(error);
                            }
                        }
                        Ok(true) => {
                            if let Err(error) = old.stop().await {
                                *state.lock().unwrap() = SessionState::Recovery {
                                    session: Some(old),
                                    reason: error.to_string(),
                                };
                                return Err(error);
                            }
                        }
                    }
                }
                *state.lock().unwrap() = SessionState::Stopped;
                Ok(())
            })
            .await
            .context("native shutdown task failed")?
        }
    }
}

#[cfg(target_os = "macos")]
impl NativeSession {
    async fn launch(tree: Arc<Tree>) -> Result<Self> {
        let fs = OutputNfs::new(tree);
        let export = format!(
            "output-{:016x}{:016x}",
            fs.handle_generation[0], fs.handle_generation[1]
        );
        let mut listener = NFSTcpListener::bind("127.0.0.1:0", fs.clone()).await?;
        listener.with_export_name(&export);
        let port = listener.get_listen_port();
        let (stop, receiver) = tokio::sync::watch::channel(false);
        // Retain explicit stop ownership even if a start error is discarded:
        // dropping an error must not strand a possibly-mounted native client.
        let keep_stop = stop.clone();
        let server = tokio::spawn(async move {
            let _keep_stop = keep_stop;
            listener.handle_until_shutdown(receiver).await
        });
        Ok(Self {
            fs,
            identity: None,
            export: format!("127.0.0.1:/{export}"),
            port,
            stop,
            server: Some(server),
            mount_child: None,
            stop_failure: None,
            unmount_pending: None,
        })
    }
    async fn mount(&mut self, root: &Path) -> Result<()> {
        ensure_unmounted(root)?;
        ensure!(
            std::fs::read_dir(root)?.next().is_none(),
            "unmounted output root contains files; refusing to hide them"
        );
        let access = if self.fs.tree.source_access().is_read_write() {
            "rw"
        } else {
            "ro"
        };
        let options = format!(
            "{access},vers=3,tcp,locallocks,noresvport,port={},mountport={},retrycnt=0,nobrowse,nosuid,nodev",
            self.port, self.port
        );
        self.mount_child = Some(
            tokio::process::Command::new("/sbin/mount_nfs")
                .args(["-o", &options, &self.export])
                .arg(root)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::inherit())
                .kill_on_drop(true)
                .spawn()?,
        );
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.mount_child.as_mut().unwrap().wait(),
        )
        .await;
        let status = match result {
            Ok(Ok(status)) => {
                self.mount_child.take();
                status
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                // Killing on Drop is not a join. A retry must not see no mount,
                // free the old server, then race a late mount helper completion.
                self.finish_mount_process().await?;
                bail!("mount_nfs timed out; child reaped and server retained for recovery");
            }
        };
        let matches: Vec<_> = mounted_exports()?
            .into_iter()
            .filter(|(path, _)| path == root)
            .collect();
        ensure!(
            matches.len() == 1 && matches[0].1.source == self.export.as_bytes(),
            "native mount ownership could not be verified; server retained for recovery"
        );
        self.identity = Some(matches[0].1.clone());
        ensure!(
            status.success(),
            "mount_nfs exited with {status}; server retained for recovery"
        );
        Ok(())
    }
    fn replaced_at(&self, root: &Path) -> Result<bool> {
        Ok(mounted_exports()?
            .iter()
            .any(|(path, identity)| path == root && identity.source != self.export.as_bytes()))
    }
    /// True means no owned mount remains; false means an ordinary busy refusal.
    async fn unmount_owned(&mut self, root: &Path) -> Result<bool> {
        self.finish_mount_process().await?;
        if self.unmount_pending.is_none() {
            let matches: Vec<_> = mounted_exports()?
                .into_iter()
                .filter(|(path, _)| path == root)
                .collect();
            if matches.is_empty() {
                return Ok(true);
            }
            ensure!(
                matches.len() == 1
                    && matches[0].1.source == self.export.as_bytes()
                    && self
                        .identity
                        .as_ref()
                        .is_none_or(|identity| identity == &matches[0].1),
                "output root belongs to another mount; refusing to unmount it"
            );
            let root = root.to_owned();
            self.unmount_pending = Some(tokio::task::spawn_blocking(move || {
                use std::os::unix::ffi::OsStrExt;
                let path = std::ffi::CString::new(root.as_os_str().as_bytes())?;
                let rc = unsafe { libc::unmount(path.as_ptr(), 0) };
                Ok(if rc == 0 {
                    None
                } else {
                    Some(std::io::Error::last_os_error())
                })
            }));
        }
        // A blocking kernel operation cannot safely be killed. Retain its exact
        // task and the server if it exceeds the bound; a retry joins it before
        // mount-table absence can authorize collection or another native mount.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.unmount_pending.as_mut().unwrap(),
        )
        .await
        .context("native unmount still pending; operation and server retained")?;
        self.unmount_pending.take();
        match result.context("native unmount worker failed")?? {
            None => Ok(true),
            Some(error) if error.raw_os_error() == Some(libc::EBUSY) => Ok(false),
            Some(error) => Err(error.into()),
        }
    }
    async fn finish_mount_process(&mut self) -> Result<()> {
        terminate_mount_child(&mut self.mount_child, std::time::Duration::from_secs(2)).await
    }
    async fn stop(&mut self) -> Result<()> {
        ensure!(
            self.mount_child.is_none() && self.unmount_pending.is_none(),
            "native mount operation remains pending; server retained"
        );
        if let Some(error) = &self.stop_failure {
            bail!("server task failed; generation retained: {error}");
        }
        let _ = self.stop.send(true);
        if let Some(server) = self.server.as_mut() {
            let result = tokio::time::timeout(std::time::Duration::from_secs(10), server)
                .await
                .context("NFS task drain timed out; pending server and generation retained")?;
            self.server.take();
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    // Explicit serving shutdown joins nested work before
                    // returning I/O errors too. A fresh session may recover.
                    eprintln!("NFS serving ended before shutdown: {error}");
                }
                Err(error) => {
                    self.stop_failure = Some(error.to_string());
                    return Err(error.into());
                }
            }
        }
        // The caller drops fs only after every nested task has joined.
        Ok(())
    }
}

#[cfg(any(target_os = "macos", test))]
async fn terminate_mount_child(
    child: &mut Option<tokio::process::Child>,
    budget: std::time::Duration,
) -> Result<()> {
    if let Some(pending) = child.as_mut() {
        // Drop/kill are not joins. Cancellation or timeout retains the exact
        // Child so a later retry must reap it before observing mount absence.
        let _ = pending.start_kill();
        tokio::time::timeout(budget, pending.wait())
            .await
            .context("mount helper reap timed out; pending child and server retained")?
            .context("mount helper could not be reaped; server retained")?;
        child.take();
    }
    Ok(())
}

#[cfg(target_os = "macos")]
async fn maintain(
    state: &Mutex<SessionState>,
    tree: &Arc<Tree>,
    root: &Path,
    budget: usize,
) -> Result<MaintenanceOutcome> {
    let previous = std::mem::replace(&mut *state.lock().unwrap(), SessionState::Transitioning);
    let old = match previous {
        SessionState::Mounted(old)
        | SessionState::Recovery {
            session: Some(old), ..
        } => Some(old),
        SessionState::Recovery { session: None, .. } => None,
        SessionState::Stopped => {
            *state.lock().unwrap() = SessionState::Stopped;
            bail!("native output is stopped")
        }
        SessionState::Transitioning => {
            *state.lock().unwrap() = SessionState::Recovery {
                session: None,
                reason: "interrupted native transition".into(),
            };
            bail!("native transition needs recovery")
        }
    };
    if let Some(mut old) = old {
        match old.unmount_owned(root).await {
            Ok(false) => {
                *state.lock().unwrap() = SessionState::Mounted(old);
                return Ok(MaintenanceOutcome::BusyRetained);
            }
            Err(error) => {
                *state.lock().unwrap() = SessionState::Recovery {
                    session: Some(old),
                    reason: error.to_string(),
                };
                return Err(error);
            }
            Ok(true) => {}
        }
        if let Err(error) = old.stop().await {
            *state.lock().unwrap() = SessionState::Recovery {
                session: Some(old),
                reason: error.to_string(),
            };
            return Err(error);
        }
    }
    // Collection failure is not permission to leave authoritative live files
    // inaccessible: still restore a fresh native mount before returning error.
    let collection = tree.reclaim_retired_at_boundary(budget);
    let mut fresh = match NativeSession::launch(tree.clone()).await {
        Ok(fresh) => fresh,
        Err(error) => {
            *state.lock().unwrap() = SessionState::Recovery {
                session: None,
                reason: error.to_string(),
            };
            return Err(error);
        }
    };
    if let Err(error) = fresh.mount(root).await {
        *state.lock().unwrap() = SessionState::Recovery {
            session: Some(fresh),
            reason: error.to_string(),
        };
        return Err(error);
    }
    *state.lock().unwrap() = SessionState::Mounted(fresh);
    match collection {
        Ok(collection) => Ok(MaintenanceOutcome::Rotated { collection }),
        Err(error) => Err(error.context("collection failed; native mount restored")),
    }
}

#[derive(Clone)]
pub(super) struct NfsPublisher {
    pub(super) fs: OutputNfs,
    pub(super) root: PathBuf,
}
impl NfsPublisher {
    // Foundation only: no public caller can enter this until native capture
    // proves page-cache flush/COMMIT ordering. The embedding owner must have
    // quiesced native users; this never claims Tree freezing flushes the client.
    #[allow(dead_code)]
    async fn publish_source_owned(
        &self,
        manifest: SourceManifest,
        expected: SourceVersion,
        request_id: String,
        limits: SourceLimits,
    ) -> Result<SourceReceipt> {
        let old = self
            .fs
            .tree
            .freeze_source(SourceFrontendFence::TreeOnly, &limits)?;
        let mut old_ids = old
            .entries()
            .iter()
            .map(|entry| entry.node.id)
            .collect::<Vec<_>>();
        old_ids.push(1);
        drop(old);
        let _old_marker = self.fs.mark(old_ids.clone())?;
        let receipt =
            self.fs
                .tree
                .install_source(manifest, Some(expected), &request_id, &limits)?;
        let current = self
            .fs
            .tree
            .freeze_source(SourceFrontendFence::TreeOnly, &limits)?;
        let old_ids = old_ids.into_iter().collect::<HashSet<_>>();
        let _new_marker = self.fs.mark(
            current
                .entries()
                .iter()
                .map(|entry| entry.node.id)
                .filter(|id| !old_ids.contains(id))
                .collect(),
        )?;
        let root_node = self.fs.tree.get(1)?;
        let mut paths = vec![(String::new(), root_node.clone())];
        let mut directories = current
            .entries()
            .iter()
            .filter(|entry| entry.node.kind == NodeKind::Directory)
            .map(|entry| (entry.path.clone(), entry.node.clone()))
            .collect::<Vec<_>>();
        directories.sort_by_key(|(path, _)| Path::new(path).components().count());
        paths.extend(directories.iter().cloned());
        paths.extend(
            current
                .entries()
                .iter()
                .filter(|entry| entry.node.kind == NodeKind::File)
                .map(|entry| (entry.path.clone(), entry.node.clone())),
        );
        // Symlinks are invalidated through their containing directories; never
        // chmod or open a symlink target while updating the mounted namespace.
        directories.reverse();
        paths.extend(directories);
        paths.push((String::new(), root_node));
        let mut failure = None;
        for (relative, node) in paths {
            let root = self.root.clone();
            let result = tokio::task::spawn_blocking(move || {
                invalidate_no_follow(&root, &relative, node.mode)
            })
            .await
            .context("native source invalidation task failed")
            .and_then(|result| result);
            if let Err(error) = result {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(receipt)
    }
    // chmod re-enters the server. Never hold Tree namespace locks here.
    pub(super) async fn invalidate(&self, paths: Vec<(String, Node)>) -> Result<()> {
        let mut failure = None;
        for (relative, node) in paths {
            let _marker = self.fs.mark(vec![node.id])?;
            let root = self.root.clone();
            let mode = node.mode;
            let result =
                tokio::task::spawn_blocking(move || invalidate_no_follow(&root, &relative, mode))
                    .await;
            if let Err(error) = result
                .context("native invalidation task failed")
                .and_then(|result| result)
            {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    async fn publish(&self, path: &str, node: Option<Node>) -> Result<()> {
        let ancestors = self.ancestor_nodes(path)?;
        // A newly created ancestor may itself have a cached ENOENT. Refresh
        // from the nearest visible ancestor before traversing deeper paths.
        let mut before = ancestors.clone();
        before.reverse();
        if let Some(node) = node {
            before.push((path.to_owned(), node));
        }
        before.extend(ancestors);
        self.invalidate(before).await
    }
    fn ancestor_nodes(&self, path: &str) -> Result<Vec<(String, Node)>> {
        let mut current = Path::new(path).parent();
        let mut nodes = Vec::new();
        while let Some(parent) = current {
            let relative = parent.to_str().context("non-UTF8 path")?;
            nodes.push((relative.to_owned(), self.fs.tree.resolve(relative)?));
            current = parent.parent();
        }
        Ok(nodes)
    }
}
#[async_trait]
impl Projection for NfsMount {
    fn root(&self) -> &Path {
        &self.root
    }
    async fn ensure_dir(&self, path: &str) -> Result<()> {
        let path = path.to_owned();
        self.complete(move |publisher| async move {
            publisher.fs.tree.ensure_dir(&path).await?;
            publisher.publish(&path, None).await
        })
        .await
    }
    async fn stage(&self, path: &str, artifact: Artifact) -> Result<()> {
        let path = path.to_owned();
        self.complete(move |publisher| async move {
            let previous = publisher.fs.tree.resolve(&path).ok();
            publisher.fs.tree.stage(&path, artifact).await?;
            let node = publisher.fs.tree.resolve(&path)?;
            // Keep the retired generation marked through every native cache barrier.
            let _old_marker = publisher
                .fs
                .mark(previous.iter().map(|node| node.id).collect())?;
            publisher.publish(&path, Some(node)).await
        })
        .await
    }
    async fn publish_source(
        &self,
        _manifest: SourceManifest,
        _expected: SourceVersion,
        _request_id: String,
        _limits: SourceLimits,
    ) -> Result<SourceReceipt> {
        // No metadata commit may precede a proven native dirty-page fence.
        Err(NfsSourceUnsupported.into())
    }
    async fn stat(&self, path: &str, digest_function: i32) -> Result<Option<TreeStat>> {
        ensure!(
            self.state() == NfsMountState::Mounted,
            "native output unavailable; retry quiescent maintenance"
        );
        self.tree.stat(path, digest_function).await
    }
    async fn refresh(&self, prefix: &str) -> Result<()> {
        // Remote presence checks must not block publication for other bases.
        let plan = self.tree.prepare_refresh(prefix).await?;
        self.complete(move |publisher| async move {
            let removed = publisher.fs.tree.apply_refresh(plan).await?;
            let mut nodes = Vec::new();
            let mut seen = HashSet::new();
            for path in removed {
                for item in publisher.ancestor_nodes(&path)? {
                    if seen.insert(item.1.id) {
                        nodes.push(item);
                    }
                }
            }
            nodes.sort_by_key(|(path, _)| std::cmp::Reverse(Path::new(path).components().count()));
            publisher.invalidate(nodes).await
        })
        .await
    }
    async fn clean(&self, prefix: &str) -> Result<()> {
        let prefix = prefix.to_owned();
        self.complete(move |publisher| async move {
            publisher.fs.tree.clean(&prefix).await?;
            let mut nodes = publisher.ancestor_nodes(&prefix)?;
            if nodes.is_empty() {
                nodes.push(("".to_owned(), publisher.fs.tree.get(1)?));
            }
            publisher.invalidate(nodes).await
        })
        .await
    }
    async fn maintain_quiescent(&self, budget: usize) -> Result<output_core::MaintenanceOutcome> {
        NfsMount::maintain_quiescent(self, budget).await
    }
    async fn shutdown(&self) -> Result<()> {
        self.shutdown_owned().await
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    use std::{
        io::Write,
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };

    struct Source;
    #[async_trait]
    impl output_core::BlobSource for Source {
        async fn fetch(&self, _: &Artifact, _: &mut std::fs::File) -> Result<()> {
            bail!("publication must not fetch content")
        }
        async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
            Ok(vec![])
        }
    }
    fn artifact(hash: &str) -> Artifact {
        Artifact {
            backend: "fixture".into(),
            instance: "".into(),
            digest_function: 1,
            hash: hash.repeat(64),
            size: 1,
        }
    }
    fn mounted_fixture(tree: Arc<Tree>, root: PathBuf) -> Arc<NfsMount> {
        let fs = OutputNfs::new(tree.clone());
        let session = NativeSession {
            fs: fs.clone(),
            #[cfg(target_os = "macos")]
            identity: None,
            #[cfg(target_os = "macos")]
            export: String::new(),
            #[cfg(target_os = "macos")]
            port: 0,
            #[cfg(target_os = "macos")]
            stop: tokio::sync::watch::channel(false).0,
            #[cfg(target_os = "macos")]
            server: None,
            #[cfg(target_os = "macos")]
            mount_child: None,
            #[cfg(target_os = "macos")]
            stop_failure: None,
            #[cfg(target_os = "macos")]
            unmount_pending: None,
        };
        Arc::new(NfsMount {
            tree: tree.clone(),
            root,
            control: Arc::new(AsyncMutex::new(())),
            session: Arc::new(Mutex::new(SessionState::Mounted(session))),
        })
    }
    // Use a local directory for the native barrier, without needing an NFS mount.
    // The delayed blocking callback has the same non-abortable lifetime as chmod.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_publication_retains_markers_and_excludes_following_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("view");
        std::fs::create_dir(&root).unwrap();
        std::fs::File::create(root.join("file"))
            .unwrap()
            .write_all(b"x")
            .unwrap();
        let tree = Tree::open(
            dir.path().join("state"),
            "fixture",
            Arc::new(Source),
            output_core::CacheConfig::default(),
        )
        .unwrap();
        tree.stage("file", artifact("1")).await.unwrap();
        let old = tree.resolve("file").unwrap();
        let mount = mounted_fixture(tree.clone(), root);
        let fs = match &*mount.session.lock().unwrap() {
            SessionState::Mounted(session) => session.fs.clone(),
            _ => unreachable!(),
        };
        let entered = Arc::new(tokio::sync::Notify::new());
        let (release, blocked) = std::sync::mpsc::channel();
        let task_mount = mount.clone();
        let task_entered = entered.clone();
        let caller = tokio::spawn(async move {
            task_mount
                .complete(move |publisher| async move {
                    let _marker = publisher.fs.mark(vec![old.id])?;
                    publisher.fs.tree.stage("file", artifact("2")).await?;
                    task_entered.notify_one();
                    tokio::task::spawn_blocking(move || blocked.recv()).await??;
                    let node = publisher.fs.tree.resolve("file")?;
                    publisher.publish("file", Some(node)).await
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        caller.abort();
        let _ = caller.await;
        assert!(fs.invalidating.lock().unwrap().contains(&old.id));
        let finished = Arc::new(AtomicBool::new(false));
        let next_mount = mount.clone();
        let next_finished = finished.clone();
        let mut next = tokio::spawn(async move {
            next_mount.stage("file", artifact("3")).await.unwrap();
            next_finished.store(true, Ordering::SeqCst);
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(40), &mut next)
                .await
                .is_err()
        );
        assert!(!finished.load(Ordering::SeqCst));
        assert_eq!(
            tree.resolve("file").unwrap().remote.unwrap().hash,
            artifact("2").hash
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), next)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            tree.resolve("file").unwrap().remote.unwrap().hash,
            artifact("3").hash
        );
        assert!(!fs.invalidating.lock().unwrap().contains(&old.id));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_checkout_requires_native_capture_before_any_namespace_change() {
        use output_core::{SourceContent, SourceEntry};
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("view");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("file"), b"native cache fixture").unwrap();
        let tree = Tree::open(
            dir.path().join("state"),
            "fixture",
            Arc::new(Source),
            output_core::CacheConfig::default(),
        )
        .unwrap();
        let mut file = artifact("1");
        file.digest_function = 9;
        let manifest = SourceManifest {
            identity: tree.identity().into(),
            root: [1; 32],
            entries: vec![SourceEntry {
                path: "file".into(),
                mode: 0o644,
                content: SourceContent::File(file),
            }],
        };
        let limits = SourceLimits::default();
        let installed = tree
            .install_source(manifest.clone(), None, "initial", &limits)
            .unwrap();
        let old = tree.resolve("file").unwrap();
        let mount = mounted_fixture(tree.clone(), root);
        let capture = mount.capture_source_quiescent(limits.clone()).await;
        assert!(
            capture
                .err()
                .unwrap()
                .downcast_ref::<NfsSourceUnsupported>()
                .is_some()
        );
        let mut next = manifest;
        next.root = [2; 32];
        let rejected = mount
            .publish_source(
                next.clone(),
                installed.version.clone(),
                "unsupported".into(),
                limits.clone(),
            )
            .await;
        assert!(
            rejected
                .err()
                .unwrap()
                .downcast_ref::<NfsSourceUnsupported>()
                .is_some()
        );
        assert_eq!(tree.source_baseline().unwrap().0, [1; 32]);
        assert!(tree.source_receipt("unsupported").unwrap().is_none());
        assert_eq!(tree.resolve("file").unwrap().id, old.id);
        // Exercise the future owned primitive with a local cache-barrier
        // fixture only. This does not prove native NFS page-cache flushing.
        tree.setattr(
            old.id,
            SetAttr {
                size: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        tree.write(old.id, 0, b"retained").await.unwrap();
        let retained = tree.get(old.id).unwrap();
        let fs = match &*mount.session.lock().unwrap() {
            SessionState::Mounted(session) => session.fs.clone(),
            _ => unreachable!(),
        };
        let old_handle = fs.id_to_fh(old.id);
        let expected = tree
            .freeze_source(SourceFrontendFence::TreeOnly, &limits)
            .unwrap()
            .version()
            .clone();
        let receipt = mount
            .complete(move |publisher| async move {
                publisher
                    .publish_source_owned(next, expected, "prepared-primitive".into(), limits)
                    .await
            })
            .await
            .unwrap();
        assert_eq!(receipt.root, [2; 32]);
        assert_ne!(tree.resolve("file").unwrap().id, old.id);
        let mut retired = retained;
        retired.linked = false;
        assert_eq!(tree.get(old.id).unwrap(), retired);
        assert_eq!(fs.fh_to_id(&old_handle).unwrap(), old.id);
        let attributes = fs.getattr(old.id).await.unwrap();
        assert_eq!(attributes.size, retired.size);
        assert_eq!(attributes.mode, retired.mode);
        assert_eq!(fs.read(old.id, 0, 32).await.unwrap().0, b"retained");
        assert_eq!(mount.state(), NfsMountState::Mounted);
    }
    #[test]
    #[ignore = "subprocess fixture invoked by owned_mount_child_is_reaped_after_cancel_and_retry"]
    fn blocked_mount_helper() {
        if std::env::var("OUTPUT_NFS_BLOCKED_HELPER").as_deref() != Ok("1") {
            return;
        }
        use std::io::Write;
        println!("mount-helper-ready");
        std::io::stdout().flush().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(600));
    }
    #[tokio::test]
    async fn owned_mount_child_is_reaped_after_cancel_and_retry() {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "mount::publication_tests::blocked_mount_helper",
                "--ignored",
                "--nocapture",
            ])
            .env("OUTPUT_NFS_BLOCKED_HELPER", "1")
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut line = String::new();
                assert!(output.read_line(&mut line).await.unwrap() != 0);
                if line.contains("mount-helper-ready") {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let mut owner = Some(child);
        let mut reaping = Box::pin(terminate_mount_child(&mut owner, Duration::from_secs(2)));
        let first =
            std::future::poll_fn(|cx| std::task::Poll::Ready(reaping.as_mut().poll(cx))).await;
        drop(reaping);
        if let std::task::Poll::Ready(result) = first {
            result.unwrap();
        }
        terminate_mount_child(&mut owner, Duration::from_secs(2))
            .await
            .unwrap();
        assert!(owner.is_none());
        assert_eq!(
            unsafe { libc::kill(pid as i32, 0) },
            -1,
            "helper or unreaped zombie remains"
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }
}
