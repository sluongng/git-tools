//! Lazy, writable CAS outputs over single-user loopback NFSv3.
//! Server restart requires unmount/remount: process-generation handles expire.
use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
#[cfg(target_os = "macos")]
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use nfsserve::{nfs::*, vfs::*};
use output_core::{Artifact, CoreError, Node, NodeKind, Projection, SetAttr, Tree, TreeStat};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct OutputNfs {
    tree: Arc<Tree>,
    invalidating: Arc<Mutex<HashSet<u64>>>,
    hydration: Arc<Semaphore>,
}
// Scope markers to the re-entrant chmod, including error/cancellation paths.
struct InvalidationMark {
    active: Arc<Mutex<HashSet<u64>>>,
    ids: Vec<u64>,
}
impl Drop for InvalidationMark {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            for id in &self.ids {
                active.remove(id);
            }
        }
    }
}
fn status(error: anyhow::Error) -> nfsstat3 {
    status_ref(&error)
}
fn status_ref(error: &anyhow::Error) -> nfsstat3 {
    if let Some(error) = error.downcast_ref::<CoreError>() {
        return match error {
            CoreError::NotFound => nfsstat3::NFS3ERR_NOENT,
            CoreError::AlreadyExists => nfsstat3::NFS3ERR_EXIST,
            CoreError::NotDirectory => nfsstat3::NFS3ERR_NOTDIR,
            CoreError::IsDirectory => nfsstat3::NFS3ERR_ISDIR,
            CoreError::NotEmpty => nfsstat3::NFS3ERR_NOTEMPTY,
            CoreError::InvalidInput => nfsstat3::NFS3ERR_INVAL,
            CoreError::NotSync => nfsstat3::NFS3ERR_NOT_SYNC,
            CoreError::Stale => nfsstat3::NFS3ERR_STALE,
        };
    }

    let errno = error.chain().find_map(|e| {
        e.downcast_ref::<std::io::Error>()
            .and_then(|e| e.raw_os_error())
    });
    match errno {
        Some(libc::ENOENT) => nfsstat3::NFS3ERR_NOENT,
        Some(libc::ENOTDIR) => nfsstat3::NFS3ERR_NOTDIR,
        Some(libc::EISDIR) => nfsstat3::NFS3ERR_ISDIR,
        Some(libc::EEXIST) => nfsstat3::NFS3ERR_EXIST,
        Some(libc::ENOTEMPTY) => nfsstat3::NFS3ERR_NOTEMPTY,
        Some(libc::EINVAL) => nfsstat3::NFS3ERR_INVAL,
        Some(libc::EACCES) | Some(libc::EPERM) => nfsstat3::NFS3ERR_ACCES,
        Some(libc::EFBIG) => nfsstat3::NFS3ERR_FBIG,
        Some(libc::ENOSPC) => nfsstat3::NFS3ERR_NOSPC,
        Some(libc::ESTALE) => nfsstat3::NFS3ERR_NOT_SYNC,
        _ => nfsstat3::NFS3ERR_IO,
    }
}
fn timestamp(ns: u64) -> nfstime3 {
    nfstime3 {
        seconds: (ns / 1_000_000_000).min(u32::MAX as u64) as u32,
        nseconds: (ns % 1_000_000_000) as u32,
    }
}
fn attrs(node: Node) -> fattr3 {
    fattr3 {
        ftype: match node.kind {
            NodeKind::File => ftype3::NF3REG,
            NodeKind::Directory => ftype3::NF3DIR,
            NodeKind::Symlink => ftype3::NF3LNK,
        },
        mode: node.mode,
        nlink: if node.kind == NodeKind::Directory {
            2
        } else {
            1
        },
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        size: node.size,
        used: node.size,
        rdev: specdata3::default(),
        fsid: 1,
        fileid: node.id,
        atime: timestamp(node.mtime),
        mtime: timestamp(node.mtime),
        ctime: timestamp(node.ctime),
    }
}
fn name(value: &filename3) -> std::result::Result<&str, nfsstat3> {
    if value.0.len() > 255 {
        return Err(nfsstat3::NFS3ERR_NAMETOOLONG);
    }
    let s = std::str::from_utf8(&value.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
    if s.is_empty() || s == "." || s == ".." || s.contains('/') || s.contains('\0') {
        return Err(nfsstat3::NFS3ERR_INVAL);
    }
    Ok(s)
}
fn convert_attr(attr: sattr3) -> std::result::Result<SetAttr, nfsstat3> {
    if matches!(attr.uid,set_uid3::uid(v) if v!=unsafe{libc::getuid()})
        || matches!(attr.gid,set_gid3::gid(v) if v!=unsafe{libc::getgid()})
    {
        return Err(nfsstat3::NFS3ERR_PERM);
    }
    let size = match attr.size {
        set_size3::size(v) if v > i64::MAX as u64 => return Err(nfsstat3::NFS3ERR_FBIG),
        set_size3::size(v) => Some(v),
        _ => None,
    };
    let mtime = match attr.mtime {
        set_mtime::DONT_CHANGE => None,
        set_mtime::SET_TO_SERVER_TIME => Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        ),
        set_mtime::SET_TO_CLIENT_TIME(t) if t.nseconds < 1_000_000_000 => {
            Some(t.seconds as u64 * 1_000_000_000 + t.nseconds as u64)
        }
        _ => return Err(nfsstat3::NFS3ERR_INVAL),
    };
    Ok(SetAttr {
        mode: match attr.mode {
            set_mode3::mode(v) => Some(v & 0o777),
            _ => None,
        },
        size,
        mtime,
    })
}
impl OutputNfs {
    pub fn new(tree: Arc<Tree>) -> Self {
        Self::with_read_limit(tree, 64)
    }
    /// Limit concurrent operations that may wait on payload/inode I/O.
    pub fn with_read_limit(tree: Arc<Tree>, limit: usize) -> Self {
        assert!(
            (1..=64).contains(&limit),
            "payload admission must be 1..=64"
        );
        Self {
            tree,
            invalidating: Arc::new(Mutex::new(HashSet::new())),
            hydration: Arc::new(Semaphore::new(limit)),
        }
    }
    // Reserve half of the 128 transport slots for metadata/publication work.
    // Never queue here: NFS clients retry JUKEBOX after capacity is available.
    fn admit_hydration(&self) -> std::result::Result<OwnedSemaphorePermit, nfsstat3> {
        self.hydration
            .clone()
            .try_acquire_owned()
            .map_err(|_| nfsstat3::NFS3ERR_JUKEBOX)
    }
    fn mark(&self, ids: Vec<u64>) -> Result<InvalidationMark> {
        self.invalidating
            .lock()
            .map_err(|_| anyhow::anyhow!("invalidation lock poisoned"))?
            .extend(ids.iter().copied());
        Ok(InvalidationMark {
            active: self.invalidating.clone(),
            ids,
        })
    }
    async fn create_inner(
        &self,
        parent: u64,
        filename: &filename3,
        attr: sattr3,
        guarded: bool,
    ) -> std::result::Result<(u64, fattr3), nfsstat3> {
        let attr = convert_attr(attr)?;
        let _admission = self.admit_hydration()?;
        let filename = name(filename)?;
        let node = match self
            .tree
            .create(parent, filename, attr.mode.unwrap_or(0o644))
            .await
        {
            Ok(node) => node,
            Err(e) if !guarded && matches!(status_ref(&e), nfsstat3::NFS3ERR_EXIST) => {
                self.tree.lookup(parent, filename).map_err(status)?
            }
            Err(e) => return Err(status(e)),
        };
        if node.kind != NodeKind::File {
            return Err(nfsstat3::NFS3ERR_EXIST);
        }
        let node = self.tree.setattr(node.id, attr).await.map_err(status)?;
        Ok((node.id, attrs(node)))
    }
}
#[async_trait]
impl NFSFileSystem for OutputNfs {
    fn max_inflight_requests(&self) -> usize {
        128
    }
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }
    fn root_dir(&self) -> u64 {
        self.tree.root_id()
    }
    async fn lookup(
        &self,
        parent: u64,
        filename: &filename3,
    ) -> std::result::Result<u64, nfsstat3> {
        let node = self.tree.get(parent).map_err(status)?;
        if node.kind != NodeKind::Directory {
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        }
        if filename.0 == b"." {
            return Ok(parent);
        }
        if filename.0 == b".." {
            return Ok(node.parent.max(1));
        }
        Ok(self
            .tree
            .lookup(parent, name(filename)?)
            .map_err(status)?
            .id)
    }
    async fn getattr(&self, id: u64) -> std::result::Result<fattr3, nfsstat3> {
        Ok(attrs(self.tree.get(id).map_err(status)?))
    }
    async fn setattr(&self, id: u64, attr: sattr3) -> std::result::Result<fattr3, nfsstat3> {
        self.setattr_guarded(id, attr, None).await
    }
    async fn setattr_guarded(
        &self,
        id: u64,
        attr: sattr3,
        guard: Option<nfstime3>,
    ) -> std::result::Result<fattr3, nfsstat3> {
        let change = convert_attr(attr)?;
        let node = self.tree.get(id).map_err(status)?;
        // A same-mode synthetic chmod exists only to deliver fresh post-op attrs.
        if guard.is_none()
            && change.mode == Some(node.mode)
            && change.size.is_none()
            && change.mtime.is_none()
            && self
                .invalidating
                .lock()
                .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
                .contains(&id)
        {
            return Ok(attrs(node));
        }
        let _admission = self.admit_hydration()?;
        let expected = guard.map(|t| t.seconds as u64 * 1_000_000_000 + t.nseconds as u64);
        Ok(attrs(
            self.tree
                .setattr_guarded(id, change, expected)
                .await
                .map_err(status)?,
        ))
    }
    async fn read(
        &self,
        id: u64,
        offset: u64,
        count: u32,
    ) -> std::result::Result<(Vec<u8>, bool), nfsstat3> {
        if count > 1024 * 1024 {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        let _admission = self.admit_hydration()?;
        let node = self.tree.get(id).map_err(status)?;
        if node.kind != NodeKind::File {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        let bytes = self
            .tree
            .read(id, offset, count as usize)
            .await
            .map_err(status)?;
        let eof = offset.saturating_add(bytes.len() as u64) >= node.size;
        Ok((bytes, eof))
    }
    async fn write(
        &self,
        id: u64,
        offset: u64,
        data: &[u8],
    ) -> std::result::Result<fattr3, nfsstat3> {
        if data.len() > 1024 * 1024 {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        let _admission = self.admit_hydration()?;
        self.tree.write(id, offset, data).await.map_err(status)?;
        self.getattr(id).await
    }
    async fn create(
        &self,
        p: u64,
        n: &filename3,
        a: sattr3,
    ) -> std::result::Result<(u64, fattr3), nfsstat3> {
        self.create_inner(p, n, a, false).await
    }
    async fn create_guarded(
        &self,
        p: u64,
        n: &filename3,
        a: sattr3,
    ) -> std::result::Result<(u64, fattr3), nfsstat3> {
        self.create_inner(p, n, a, true).await
    }
    async fn create_exclusive(
        &self,
        p: u64,
        n: &filename3,
        v: [u8; 8],
    ) -> std::result::Result<u64, nfsstat3> {
        Ok(self
            .tree
            .create_exclusive(p, name(n)?, 0o644, v)
            .await
            .map_err(status)?
            .id)
    }
    async fn mkdir(&self, p: u64, n: &filename3) -> std::result::Result<(u64, fattr3), nfsstat3> {
        let node = self.tree.mkdir(p, name(n)?, 0o755).await.map_err(status)?;
        Ok((node.id, attrs(node)))
    }
    async fn remove(&self, p: u64, n: &filename3) -> std::result::Result<(), nfsstat3> {
        self.tree.unlink_file(p, name(n)?).await.map_err(status)
    }
    async fn rmdir(&self, p: u64, n: &filename3) -> std::result::Result<(), nfsstat3> {
        self.tree
            .unlink_directory(p, name(n)?)
            .await
            .map_err(status)
    }
    async fn rename(
        &self,
        p: u64,
        n: &filename3,
        d: u64,
        t: &filename3,
    ) -> std::result::Result<(), nfsstat3> {
        let _admission = self.admit_hydration()?;
        self.tree
            .rename(p, name(n)?, d, name(t)?)
            .await
            .map_err(status)
    }
    async fn readdir(
        &self,
        p: u64,
        after: u64,
        max: usize,
    ) -> std::result::Result<ReadDirResult, nfsstat3> {
        let nodes = self
            .tree
            .list(p, after, max.min(4096).saturating_add(1))
            .map_err(status)?;
        let end = nodes.len() <= max.min(4096);
        Ok(ReadDirResult {
            entries: nodes
                .into_iter()
                .take(max.min(4096))
                .map(|n| DirEntry {
                    fileid: n.id,
                    name: n.name.as_bytes().into(),
                    attr: attrs(n),
                })
                .collect(),
            end,
        })
    }
    async fn symlink(
        &self,
        p: u64,
        n: &filename3,
        t: &nfspath3,
        _a: &sattr3,
    ) -> std::result::Result<(u64, fattr3), nfsstat3> {
        let target = std::str::from_utf8(&t.0).map_err(|_| nfsstat3::NFS3ERR_INVAL)?;
        if target.contains('\0') {
            return Err(nfsstat3::NFS3ERR_INVAL);
        }
        let node = self
            .tree
            .symlink(p, name(n)?, target)
            .await
            .map_err(status)?;
        Ok((node.id, attrs(node)))
    }
    async fn readlink(&self, id: u64) -> std::result::Result<nfspath3, nfsstat3> {
        Ok(self.tree.readlink(id).map_err(status)?.as_bytes().into())
    }
}

// Never follow cached symlinks while forcing a view update. Directory-only
// descriptor traversal also prevents an ancestor symlink escaping the export.
fn invalidate_no_follow(root: &Path, relative: &str, mode: u32) -> Result<()> {
    use std::{
        ffi::CString,
        os::{
            fd::{AsRawFd, FromRawFd, OwnedFd},
            unix::fs::OpenOptionsExt,
        },
    };
    let mut parent: OwnedFd = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(root)?
        .into();
    let components: Vec<_> = Path::new(relative).components().collect();
    if components.is_empty() {
        let result = unsafe { libc::fchmod(parent.as_raw_fd(), mode as libc::mode_t) };
        if result != 0 {
            return Err(std::io::Error::last_os_error()).context("invalidate export root");
        }
        return Ok(());
    }
    for component in &components[..components.len() - 1] {
        let std::path::Component::Normal(name) = component else {
            bail!("invalid invalidation path")
        };
        use std::os::unix::ffi::OsStrExt;
        let name = CString::new(name.as_bytes())?;
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("open invalidation parent for {relative}"));
        }
        parent = unsafe { OwnedFd::from_raw_fd(fd) };
    }
    let std::path::Component::Normal(name) = components.last().unwrap() else {
        bail!("invalid invalidation leaf")
    };
    use std::os::unix::ffi::OsStrExt;
    let name = CString::new(name.as_bytes())?;
    let result = unsafe {
        libc::fchmodat(
            parent.as_raw_fd(),
            name.as_ptr(),
            mode as libc::mode_t,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("invalidate {relative}"));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct MountIdentity {
    fsid: [i32; 2],
    source: Vec<u8>,
}
#[cfg(target_os = "macos")]
fn mounted_exports() -> Result<Vec<(PathBuf, MountIdentity)>> {
    use std::{ffi::CStr, os::unix::ffi::OsStrExt};
    for _ in 0..4 {
        let count = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
        ensure!(
            count >= 0,
            "read mount table: {}",
            std::io::Error::last_os_error()
        );
        let capacity = count as usize + 16;
        let mut entries: Vec<libc::statfs> = (0..capacity)
            .map(|_| unsafe { std::mem::zeroed() })
            .collect();
        let count = unsafe {
            libc::getfsstat(
                entries.as_mut_ptr(),
                std::mem::size_of_val(entries.as_slice()) as libc::c_int,
                libc::MNT_NOWAIT,
            )
        };
        ensure!(
            count >= 0,
            "read mount table: {}",
            std::io::Error::last_os_error()
        );
        if count as usize >= capacity {
            continue;
        }
        return Ok(entries
            .into_iter()
            .take(count as usize)
            .map(|entry| {
                let path = unsafe { CStr::from_ptr(entry.f_mntonname.as_ptr()) }.to_bytes();
                let source = unsafe { CStr::from_ptr(entry.f_mntfromname.as_ptr()) }
                    .to_bytes()
                    .to_vec();
                let fsid = unsafe { std::mem::transmute::<libc::fsid_t, [i32; 2]>(entry.f_fsid) };
                (
                    PathBuf::from(std::ffi::OsStr::from_bytes(path)),
                    MountIdentity { fsid, source },
                )
            })
            .collect());
    }
    bail!("mount table kept changing")
}
#[cfg(target_os = "macos")]
fn mount_path(root: &Path) -> Result<PathBuf> {
    let absolute = std::path::absolute(root)?;
    // Resolve aliases in the local parent, never touch a potentially stale
    // mounted root itself. macOS /var and /tmp aliases resolve here too.
    let mut parent = absolute
        .parent()
        .context("output mountpoint must not be filesystem root")?;
    let mut suffix = vec![
        absolute
            .file_name()
            .context("missing mountpoint name")?
            .to_owned(),
    ];
    loop {
        match parent.canonicalize() {
            Ok(mut resolved) => {
                for name in suffix.iter().rev() {
                    resolved.push(name)
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                suffix.push(
                    parent
                        .file_name()
                        .context("missing mountpoint parent")?
                        .to_owned(),
                );
                parent = parent
                    .parent()
                    .context("missing existing mountpoint ancestor")?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}
/// Reject an existing mount before canonicalizing or enumerating its root.
/// MNT_NOWAIT reads the kernel mount table without contacting an NFS server.
pub fn ensure_unmounted(root: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let absolute = std::path::absolute(root)?;
        let mounts = mounted_exports()?;
        ensure!(
            !mounts.iter().any(|(path, _)| path == &absolute),
            "output root already mounted: {}",
            root.display()
        );
        let resolved = mount_path(root)?;
        ensure!(
            !mounts.iter().any(|(path, _)| path == &resolved),
            "output root already mounted: {}",
            root.display()
        );
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = root;
    }
    Ok(())
}

/// Owns both the server and native mount. Call shutdown before dropping it.
/// Failed unmount leaves the server alive rather than stranding a mounted client.
pub struct NfsMount {
    fs: OutputNfs,
    root: PathBuf,
    #[cfg(target_os = "macos")]
    identity: MountIdentity,
    control: Arc<AsyncMutex<()>>,
    server: Mutex<Option<tokio::task::JoinHandle<std::io::Result<()>>>>,
}
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
            let fs = OutputNfs::new(tree);
            let listener = NFSTcpListener::bind("127.0.0.1:0", fs.clone()).await?;
            let port = listener.get_listen_port();
            let server = tokio::spawn(async move { listener.handle_forever().await });
            let options = format!(
                "vers=3,tcp,locallocks,noresvport,port={port},mountport={port},retrycnt=0,nobrowse,nosuid,nodev"
            );
            let result = tokio::process::Command::new("/sbin/mount_nfs")
                .args(["-o", &options, "127.0.0.1:/"])
                .arg(&root)
                .output()
                .await;
            match result {
                Ok(result) if result.status.success() => {}
                Ok(result) => {
                    server.abort();
                    bail!("mount_nfs: {}", String::from_utf8_lossy(&result.stderr))
                }
                Err(error) => {
                    server.abort();
                    return Err(error.into());
                }
            }
            let mut matches = mounted_exports()?
                .into_iter()
                .filter(|(path, _)| path == &root);
            let identity = matches
                .next()
                .context("mounted NFS export absent from mount table")?
                .1;
            ensure!(
                matches.next().is_none(),
                "multiple mounts appeared at output root"
            );
            Ok(Self {
                identity,
                fs,
                root,
                control: Arc::new(AsyncMutex::new(())),
                server: Mutex::new(Some(server)),
            })
        }
    }
    // Admission is cancellable. Once admitted, namespace publication and the
    // complete native invalidation barrier retain their guard independently of
    // the caller: dropping a JoinHandle does not abort its task.
    async fn complete<F, Fut>(&self, operation: F) -> Result<()>
    where
        F: FnOnce(NfsPublisher) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let guard = self.control.clone().lock_owned().await;
        let publisher = NfsPublisher {
            fs: self.fs.clone(),
            root: self.root.clone(),
        };
        tokio::spawn(async move {
            let _guard = guard;
            operation(publisher).await
        })
        .await
        .context("native publication task failed")?
    }
}
#[derive(Clone)]
struct NfsPublisher {
    fs: OutputNfs,
    root: PathBuf,
}
impl NfsPublisher {
    // chmod re-enters the server. Never hold Tree namespace locks here.
    async fn invalidate(&self, paths: Vec<(String, Node)>) -> Result<()> {
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
    async fn stat(&self, path: &str, digest_function: i32) -> Result<Option<TreeStat>> {
        self.fs.tree.stat(path, digest_function).await
    }
    async fn refresh(&self, prefix: &str) -> Result<()> {
        // Remote presence checks must not block publication for other bases.
        let plan = self.fs.tree.prepare_refresh(prefix).await?;
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
    async fn shutdown(&self) -> Result<()> {
        let _guard = self.control.lock().await;
        if self
            .server
            .lock()
            .map_err(|_| anyhow::anyhow!("server lock poisoned"))?
            .is_none()
        {
            return Ok(());
        }
        #[cfg(target_os = "macos")]
        {
            let matches: Vec<_> = mounted_exports()?
                .into_iter()
                .filter(|(path, _)| path == &self.root)
                .collect();
            if !matches
                .iter()
                .any(|(_, identity)| identity == &self.identity)
            {
                // A manual unmount already removed our export. A replacement
                // mount belongs to somebody else and must not be unmounted.
                if let Some(server) = self
                    .server
                    .lock()
                    .map_err(|_| anyhow::anyhow!("server lock poisoned"))?
                    .take()
                {
                    server.abort()
                }
                return Ok(());
            }
            ensure!(
                matches.len() == 1,
                "output root is covered by another mount; refusing to unmount it"
            );
        }
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio::process::Command::new("/sbin/umount")
                .arg(&self.root)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .context("unmount timed out; server retained for the mounted client")??;
        ensure!(
            result.status.success(),
            "unmount {}: {}",
            self.root.display(),
            String::from_utf8_lossy(&result.stderr)
        );
        if let Some(server) = self
            .server
            .lock()
            .map_err(|_| anyhow::anyhow!("server lock poisoned"))?
            .take()
        {
            server.abort()
        }
        Ok(())
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
        let mount = Arc::new(NfsMount {
            fs: OutputNfs::new(tree.clone()),
            root,
            #[cfg(target_os = "macos")]
            identity: MountIdentity {
                fsid: [0, 0],
                source: vec![],
            },
            control: Arc::new(AsyncMutex::new(())),
            server: Mutex::new(None),
        });
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
        assert!(mount.fs.invalidating.lock().unwrap().contains(&old.id));
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
        assert!(!mount.fs.invalidating.lock().unwrap().contains(&old.id));
    }
}
