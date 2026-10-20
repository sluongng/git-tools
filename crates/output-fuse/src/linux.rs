use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use fuser::*;
use output_core::{Artifact, CoreError, Node, NodeKind, Projection, SetAttr, Tree, TreeStat};
use std::{
    ffi::OsStr,
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    runtime::Handle,
    sync::{Mutex as AsyncMutex, Notify, Semaphore},
};

const TTL: Duration = Duration::ZERO;
const PAYLOAD_SLOTS: usize = 64;

fn errno(error: anyhow::Error) -> i32 {
    if let Some(e) = error.downcast_ref::<CoreError>() {
        return match e {
            CoreError::NotFound => libc::ENOENT,
            CoreError::AlreadyExists => libc::EEXIST,
            CoreError::NotDirectory => libc::ENOTDIR,
            CoreError::IsDirectory => libc::EISDIR,
            CoreError::NotEmpty => libc::ENOTEMPTY,
            CoreError::InvalidInput => libc::EINVAL,
            CoreError::NotSync | CoreError::Stale => libc::ESTALE,
        };
    }
    error
        .chain()
        .find_map(|e| {
            e.downcast_ref::<std::io::Error>()
                .and_then(|e| e.raw_os_error())
        })
        .unwrap_or(libc::EIO)
}
fn kind(node: &Node) -> FileType {
    match node.kind {
        NodeKind::File => FileType::RegularFile,
        NodeKind::Directory => FileType::Directory,
        NodeKind::Symlink => FileType::Symlink,
    }
}
fn attrs(node: &Node) -> FileAttr {
    FileAttr {
        ino: node.id,
        size: node.size,
        blocks: node.size.div_ceil(512),
        atime: UNIX_EPOCH + Duration::from_nanos(node.mtime),
        mtime: UNIX_EPOCH + Duration::from_nanos(node.mtime),
        ctime: UNIX_EPOCH + Duration::from_nanos(node.ctime),
        crtime: UNIX_EPOCH,
        kind: kind(node),
        perm: (node.mode & 0o7777) as u16,
        nlink: if !node.linked {
            0
        } else if node.kind == NodeKind::Directory {
            2
        } else {
            1
        },
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}
fn name(value: &OsStr) -> Result<String> {
    let value = value.to_str().context("non-UTF8 output name")?;
    ensure!(value.len() <= 255, "output name too long");
    Ok(value.to_owned())
}
fn ns(time: TimeOrNow) -> u64 {
    let time = match time {
        TimeOrNow::SpecificTime(t) => t,
        TimeOrNow::Now => SystemTime::now(),
    };
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

/// Counts deferred replies as well as active workers. Shutdown cannot race an
/// accepted request whose asynchronous admission task has not yet been polled.
#[derive(Default)]
struct Pending {
    count: AtomicUsize,
    changed: Notify,
}
impl Pending {
    async fn drain(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.count.load(Ordering::SeqCst) == 0 {
                return;
            }
            changed.await;
        }
    }
}
struct RequestTicket(Arc<Pending>);
impl Drop for RequestTicket {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}

/// The FUSE receiver never waits for CAS. Queued requests await asynchronous
/// admission without occupying blocking workers; only 64 active operations may
/// perform blocking filesystem/database work. The kernel limits background
/// requests to128 and individual payload buffers to1MiB. Synchronous requests
/// are backpressured by their calling processes, not rejected with EAGAIN.
#[derive(Clone)]
struct Adapter {
    tree: Arc<Tree>,
    runtime: Handle,
    payload: Arc<Semaphore>,
    pending: Arc<Pending>,
    stopped: Arc<AtomicBool>,
}
impl Adapter {
    fn dispatch<F: Future<Output = ()> + Send + 'static>(&self, work: F) {
        let runtime = self.runtime.clone();
        let payload = self.payload.clone();
        self.runtime.spawn(async move {
            let _admission = payload
                .acquire_owned()
                .await
                .expect("FUSE admission remains open");
            tokio::task::spawn_blocking(move || runtime.block_on(work))
                .await
                .ok();
        });
    }
    fn enter(&self) -> Result<RequestTicket, i32> {
        // Count before testing stopped. Sequential consistency across these
        // two atomics excludes store-buffering: shutdown cannot observe zero
        // while this callback observes the pre-shutdown admission flag.
        self.pending.count.fetch_add(1, Ordering::SeqCst);
        let ticket = RequestTicket(self.pending.clone());
        if self.stopped.load(Ordering::SeqCst) {
            return Err(libc::EIO);
        }
        Ok(ticket)
    }
}
impl Filesystem for Adapter {
    fn init(&mut self, _: &Request<'_>, config: &mut KernelConfig) -> Result<(), i32> {
        let _ = config.set_max_background(128);
        let _ = config.set_max_write(1024 * 1024);
        Ok(())
    }
    fn lookup(&mut self, _: &Request<'_>, parent: u64, name_: &OsStr, reply: ReplyEntry) {
        let result = name(name_).and_then(|n| self.tree.lookup(parent, &n));
        match result {
            Ok(n) => reply.entry(&TTL, &attrs(&n), n.generation),
            Err(e) => reply.error(errno(e)),
        }
    }
    fn getattr(&mut self, _: &Request<'_>, ino: u64, _: Option<u64>, reply: ReplyAttr) {
        match self.tree.get(ino) {
            Ok(n) => reply.attr(&TTL, &attrs(&n)),
            Err(e) => reply.error(errno(e)),
        }
    }
    fn readlink(&mut self, _: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.tree.readlink(ino) {
            Ok(target) => reply.data(target.as_bytes()),
            Err(e) => reply.error(errno(e)),
        }
    }
    fn readdir(
        &mut self,
        _: &Request<'_>,
        ino: u64,
        _: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let result = (|| -> Result<()> {
            let directory = self.tree.get(ino)?;
            if offset == 0 && reply.add(ino, 1, FileType::Directory, ".") {
                return Ok(());
            }
            if offset <= 1 && reply.add(directory.parent, 2, FileType::Directory, "..") {
                return Ok(());
            }
            let mut after = (offset as u64).saturating_sub(2);
            loop {
                let nodes = self.tree.list(ino, after, 256)?;
                if nodes.is_empty() {
                    break;
                }
                for node in nodes {
                    if reply.add(node.id, (node.id + 2) as i64, kind(&node), &node.name) {
                        return Ok(());
                    }
                    after = node.id;
                }
            }
            Ok(())
        })();
        match result {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(e)),
        }
    }
    fn open(&mut self, _: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let node = match self.tree.get(ino) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(e));
                return;
            }
        };
        if node.kind != NodeKind::File {
            reply.error(libc::EISDIR);
            return;
        }
        if flags & libc::O_TRUNC == 0 {
            reply.opened(ino, 0);
            return;
        }
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree
                .setattr(
                    ino,
                    SetAttr {
                        size: Some(0),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => reply.opened(ino, 0),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    fn read(
        &mut self,
        _: &Request<'_>,
        ino: u64,
        _: u64,
        offset: i64,
        size: u32,
        _: i32,
        _: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 || size > 1024 * 1024 {
            reply.error(libc::EINVAL);
            return;
        }
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.read(ino, offset as u64, size as usize).await {
                Ok(data) => reply.data(&data),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    fn write(
        &mut self,
        _: &Request<'_>,
        ino: u64,
        _: u64,
        offset: i64,
        data: &[u8],
        _: u32,
        _: i32,
        _: Option<u64>,
        reply: ReplyWrite,
    ) {
        if offset < 0 || data.len() > 1024 * 1024 {
            reply.error(libc::EINVAL);
            return;
        }
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        let data = data.to_vec();
        self.dispatch(async move {
            let _permit = permit;
            match tree.write(ino, offset as u64, &data).await {
                Ok(n) => reply.written(n as u32),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    fn setattr(
        &mut self,
        _: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _: Option<SystemTime>,
        _: Option<u64>,
        _: Option<SystemTime>,
        _: Option<SystemTime>,
        _: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if uid.is_some_and(|v| v != unsafe { libc::getuid() })
            || gid.is_some_and(|v| v != unsafe { libc::getgid() })
            || flags.is_some_and(|v| v != 0)
        {
            reply.error(libc::EPERM);
            return;
        }
        if size.is_some_and(|s| s > i64::MAX as u64) {
            reply.error(libc::EFBIG);
            return;
        }
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree
                .setattr(
                    ino,
                    SetAttr {
                        mode,
                        size,
                        mtime: mtime.map(ns),
                    },
                )
                .await
            {
                Ok(n) => reply.attr(&TTL, &attrs(&n)),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    fn create(
        &mut self,
        _: &Request<'_>,
        parent: u64,
        filename: &OsStr,
        mode: u32,
        umask: u32,
        _: i32,
        reply: ReplyCreate,
    ) {
        let filename = match name(filename) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(e));
                return;
            }
        };
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.create(parent, &filename, mode & !umask & 0o7777).await {
                Ok(n) => reply.created(&TTL, &attrs(&n), n.generation, n.id, 0),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    fn mknod(
        &mut self,
        _: &Request<'_>,
        parent: u64,
        filename: &OsStr,
        mode: u32,
        umask: u32,
        _: u32,
        reply: ReplyEntry,
    ) {
        if mode & libc::S_IFMT != libc::S_IFREG {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        let filename = match name(filename) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(e));
                return;
            }
        };
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.create(parent, &filename, mode & !umask & 0o7777).await {
                Ok(n) => reply.entry(&TTL, &attrs(&n), n.generation),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    fn mkdir(
        &mut self,
        _: &Request<'_>,
        parent: u64,
        filename: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let filename = match name(filename) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(e));
                return;
            }
        };
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.mkdir(parent, &filename, mode & !umask & 0o7777).await {
                Ok(n) => reply.entry(&TTL, &attrs(&n), n.generation),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    fn symlink(
        &mut self,
        _: &Request<'_>,
        parent: u64,
        filename: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let filename = match name(filename) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(e));
                return;
            }
        };
        let target = match target.to_str() {
            Some(t) => t.to_owned(),
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.symlink(parent, &filename, &target).await {
                Ok(n) => reply.entry(&TTL, &attrs(&n), n.generation),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    fn unlink(&mut self, _: &Request<'_>, parent: u64, filename: &OsStr, reply: ReplyEmpty) {
        let filename = match name(filename) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(e));
                return;
            }
        };
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.unlink_file(parent, &filename).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    fn rmdir(&mut self, _: &Request<'_>, parent: u64, filename: &OsStr, reply: ReplyEmpty) {
        let filename = match name(filename) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(e));
                return;
            }
        };
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.unlink_directory(parent, &filename).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    fn rename(
        &mut self,
        _: &Request<'_>,
        parent: u64,
        filename: &OsStr,
        dest: u64,
        target: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        if flags != 0 {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        let (filename, target) = match name(filename).and_then(|n| Ok((n, name(target)?))) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(e));
                return;
            }
        };
        let permit = match self.enter() {
            Ok(p) => p,
            Err(e) => {
                reply.error(e);
                return;
            }
        };
        let tree = self.tree.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.rename(parent, &filename, dest, &target).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(e)),
            }
        });
    }
    // Each Tree write/truncate is fsynced before its reply. No writeback-cache
    // capability is advertised, so flush/fsync have no outstanding dirty data.
    fn flush(&mut self, _: &Request<'_>, _: u64, _: u64, _: u64, reply: ReplyEmpty) {
        reply.ok();
    }
    fn fsync(&mut self, _: &Request<'_>, _: u64, _: u64, _: bool, reply: ReplyEmpty) {
        reply.ok();
    }
    fn statfs(&mut self, _: &Request<'_>, _: u64, reply: ReplyStatfs) {
        use std::os::unix::ffi::OsStrExt;
        let path = match std::ffi::CString::new(self.tree.state_dir().as_os_str().as_bytes()) {
            Ok(path) => path,
            Err(_) => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } != 0 {
            reply.error(
                std::io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO),
            );
            return;
        }
        reply.statfs(
            stat.f_blocks,
            stat.f_bfree,
            stat.f_bavail,
            stat.f_files,
            stat.f_ffree,
            stat.f_frsize as u32,
            stat.f_namemax as u32,
            stat.f_bsize as u32,
        );
    }
    fn getxattr(&mut self, _: &Request<'_>, ino: u64, attr: &OsStr, size: u32, reply: ReplyXattr) {
        let function = match attr.to_str() {
            Some("user.blake3") => 9,
            Some("user.sha256") => 1,
            _ => {
                reply.error(libc::ENODATA);
                return;
            }
        };
        let artifact = self
            .tree
            .get(ino)
            .ok()
            .and_then(|n| n.remote)
            .filter(|a| a.digest_function == function);
        let Some(artifact) = artifact else {
            reply.error(libc::ENODATA);
            return;
        };
        let mut bytes = Vec::with_capacity(32);
        for index in (0..64).step_by(2) {
            bytes.push(u8::from_str_radix(&artifact.hash[index..index + 2], 16).unwrap());
        }
        if size == 0 {
            reply.size(32);
        } else if size < 32 {
            reply.error(libc::ERANGE);
        } else {
            reply.data(&bytes);
        }
    }
}

fn mounted_ids(root: &Path) -> Result<Vec<u64>> {
    let encoded = root
        .as_os_str()
        .as_encoded_bytes()
        .iter()
        .flat_map(|byte| match byte {
            b' ' => b"\\040".to_vec(),
            b'\t' => b"\\011".to_vec(),
            b'\n' => b"\\012".to_vec(),
            b'\\' => b"\\134".to_vec(),
            other => vec![*other],
        })
        .collect::<Vec<_>>();
    let mounts = std::fs::read("/proc/self/mountinfo")?;
    mounts
        .split(|b| *b == b'\n')
        .filter(|line| line.split(|b| *b == b' ').nth(4) == Some(encoded.as_slice()))
        .map(|line| {
            Ok(std::str::from_utf8(
                line.split(|b| *b == b' ')
                    .next()
                    .context("missing mount ID")?,
            )?
            .parse()?)
        })
        .collect()
}
struct NativeSession {
    unmounter: SessionUnmounter,
    worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    stop: Arc<AtomicBool>,
    root: PathBuf,
    mount_id: u64,
}
impl NativeSession {
    fn unmount(&mut self) -> Result<()> {
        let ids = mounted_ids(&self.root)?;
        if !ids.contains(&self.mount_id) {
            return Ok(());
        }
        ensure!(
            ids == [self.mount_id],
            "another mount covers the output root; server retained"
        );
        // Use an ordinary (not forced/lazy) unmount with a finite helper
        // lifetime. Busy clients retain a fully working server and can close
        // their handles before retrying shutdown. Never require sudo.
        let mut child = match std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(&self.root)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
        {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::process::Command::new("fusermount")
                    .arg("-u")
                    .arg(&self.root)
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::piped())
                    .spawn()?
            }
            other => other?,
        };
        let until = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if child.try_wait()?.is_some() {
                break;
            }
            if std::time::Instant::now() >= until {
                let _ = child.kill();
                let _ = child.wait();
                anyhow::bail!("output unmount timed out; server retained");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let result = child.wait_with_output()?;
        ensure!(
            result.status.success(),
            "output unmount failed; server retained: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        ensure!(
            !mounted_ids(&self.root)?.contains(&self.mount_id),
            "output unmount failed; server retained"
        );
        self.unmounter.unmount()?;
        Ok(())
    }
}
impl Drop for NativeSession {
    fn drop(&mut self) {
        // Receiver termination drops Session on its own native thread, where
        // fuser performs its automatic unmount. Explicit shutdown joins it.
        self.stop.store(true, Ordering::Release);
    }
}

/// Mounted projection. Dropping the final owner unmounts the FUSE session;
/// explicit shutdown additionally joins its receiver outside Tokio workers.
pub struct FuseMount {
    root: PathBuf,
    adapter: Adapter,
    notifier: Notifier,
    session: Arc<Mutex<Option<NativeSession>>>,
    control: Arc<AsyncMutex<()>>,
    stat_work: Arc<Semaphore>,
}

/// Mount a single-user writable view. The caller must keep the Tokio runtime
/// alive through shutdown. State/cache must not be inside the projected root.
pub async fn start(tree: Arc<Tree>, root: &Path) -> Result<Arc<FuseMount>> {
    let root = root.to_owned();
    let runtime = Handle::current();
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&root)?;
        let root = root.canonicalize()?;
        ensure!(
            mounted_ids(&root)?.is_empty(),
            "output root is already mounted"
        );
        ensure!(
            !root.starts_with(tree.state_dir()) && !tree.state_dir().starts_with(&root),
            "output mount and state overlap"
        );
        ensure!(
            std::fs::read_dir(&root)?.next().is_none(),
            "output mountpoint must be empty"
        );
        let adapter = Adapter {
            tree,
            runtime,
            payload: Arc::new(Semaphore::new(PAYLOAD_SLOTS)),
            pending: Arc::new(Pending::default()),
            stopped: Arc::new(AtomicBool::new(false)),
        };
        let mut session = Session::new(
            adapter.clone(),
            &root,
            &[
                MountOption::FSName("git-tools-outputs".into()),
                MountOption::DefaultPermissions,
                MountOption::NoAtime,
                MountOption::NoSuid,
                MountOption::NoDev,
            ],
        )?;
        let notifier = session.notifier();
        let unmounter = session.unmount_callable();
        let stop = Arc::new(AtomicBool::new(false));
        let receiver_stop = stop.clone();
        let worker = std::thread::spawn(move || session.run_until(&receiver_stop));
        let mut native = NativeSession {
            unmounter,
            worker: Some(worker),
            stop,
            root: root.clone(),
            mount_id: 0,
        };
        // This native request cannot finish until INIT and a kernel GETATTR
        // have completed. Publication notifications are now safe to send.
        std::fs::metadata(&root).context("FUSE mount readiness")?;
        let ids = mounted_ids(&root)?;
        ensure!(ids.len() == 1, "ambiguous output mount identity");
        native.mount_id = ids[0];
        Ok(Arc::new(FuseMount {
            root,
            adapter,
            notifier,
            session: Arc::new(Mutex::new(Some(native))),
            control: Arc::new(AsyncMutex::new(())),
            stat_work: Arc::new(Semaphore::new(PAYLOAD_SLOTS)),
        }))
    })
    .await
    .context("mount worker failed")?
}
impl FuseMount {
    async fn complete<T: Send + 'static>(
        &self,
        operation: impl FnOnce(
            Arc<Tree>,
            Notifier,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<T>> + Send>>
        + Send
        + 'static,
    ) -> Result<T> {
        let tree = self.adapter.tree.clone();
        let notifier = self.notifier.clone();
        // Await serialization without occupying a blocking worker. Once
        // spawned, the committed worker owns the guard through cancellation.
        let guard = self.control.clone().lock_owned().await;
        let runtime = self.adapter.runtime.clone();
        let stopped = self.adapter.stopped.clone();
        tokio::task::spawn_blocking(move || {
            runtime.block_on(async move {
                let _guard = guard;
                ensure!(
                    !stopped.load(Ordering::Acquire),
                    "output projection stopped"
                );
                operation(tree, notifier).await
            })
        })
        .await
        .context("output publication worker failed")?
    }
}
fn notify_result(result: std::io::Result<()>) -> Result<()> {
    match result {
        Err(e) if e.raw_os_error() == Some(libc::ENOENT) => Ok(()),
        other => other.context("invalidate output kernel cache"),
    }
}
fn invalidate(notifier: &Notifier, node: &Node) -> Result<()> {
    notify_result(notifier.inval_inode(node.id, 0, 0))?;
    if node.id != 1 {
        notify_result(notifier.inval_entry(node.parent, OsStr::new(&node.name)))?;
    }
    Ok(())
}
fn ancestors(tree: &Tree, notifier: &Notifier, path: &str) -> Result<()> {
    let mut current = String::new();
    invalidate(notifier, &tree.get(1)?)?;
    for part in path.split('/').filter(|p| !p.is_empty()) {
        if !current.is_empty() {
            current.push('/');
        }
        current.push_str(part);
        if let Ok(node) = tree.resolve(&current) {
            invalidate(notifier, &node)?;
        }
    }
    Ok(())
}
fn descendants(tree: &Tree, id: u64, out: &mut Vec<Node>) -> Result<()> {
    let node = tree.get(id)?;
    if node.kind == NodeKind::Directory {
        let mut after = 0;
        loop {
            let children = tree.list(id, after, 256)?;
            if children.is_empty() {
                break;
            }
            for child in children {
                after = child.id;
                descendants(tree, child.id, out)?;
            }
        }
    }
    out.push(node);
    Ok(())
}
#[async_trait]
impl Projection for FuseMount {
    fn root(&self) -> &Path {
        &self.root
    }
    async fn ensure_dir(&self, path: &str) -> Result<()> {
        let path = path.to_owned();
        self.complete(move |tree, notifier| {
            Box::pin(async move {
                tree.ensure_dir(&path).await?;
                ancestors(&tree, &notifier, &path)
            })
        })
        .await
    }
    async fn stage(&self, path: &str, artifact: Artifact) -> Result<()> {
        let path = path.to_owned();
        self.complete(move |tree, notifier| {
            Box::pin(async move {
                let previous = tree.resolve(&path).ok();
                tree.stage(&path, artifact).await?;
                if let Some(old) = previous {
                    invalidate(&notifier, &old)?;
                }
                ancestors(&tree, &notifier, &path)
            })
        })
        .await
    }
    async fn stat(&self, path: &str, function: i32) -> Result<Option<TreeStat>> {
        let tree = self.adapter.tree.clone();
        let runtime = self.adapter.runtime.clone();
        let path = path.to_owned();
        // Local-file stat may hash via nested blocking work. Reserve its own
        // bounded lane, independently of payload admission, and queue before
        // spawning so neither CAS verification nor metadata can be starved by
        // thousands of callers occupying the runtime blocking pool.
        let permit = self.stat_work.clone().acquire_owned().await?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            runtime.block_on(tree.stat(&path, function))
        })
        .await
        .context("output stat worker failed")?
    }
    async fn refresh(&self, prefix: &str) -> Result<()> {
        let plan = self.adapter.tree.prepare_refresh(prefix).await?;
        self.complete(move |tree, notifier| {
            Box::pin(async move {
                let paths = tree.apply_refresh(plan).await?;
                for path in paths {
                    if let Some(parent) = Path::new(&path).parent() {
                        if let (Some(parent), Some(name)) =
                            (parent.to_str(), Path::new(&path).file_name())
                        {
                            if let Ok(node) = tree.resolve(parent) {
                                notify_result(notifier.inval_entry(node.id, name))?;
                                invalidate(&notifier, &node)?;
                            }
                        }
                    }
                }
                Ok(())
            })
        })
        .await
    }
    async fn clean(&self, prefix: &str) -> Result<()> {
        let prefix = prefix.to_owned();
        self.complete(move |tree, notifier| {
            Box::pin(async move {
                let mut nodes = vec![];
                if let Ok(root) = tree.resolve(&prefix) {
                    descendants(&tree, root.id, &mut nodes)?;
                }
                tree.clean(&prefix).await?;
                for node in nodes {
                    invalidate(&notifier, &node)?;
                }
                invalidate(&notifier, &tree.get(1)?)
            })
        })
        .await
    }
    async fn shutdown(&self) -> Result<()> {
        let sessions = self.session.clone();
        // Await serialization without occupying a blocking worker. Once
        // spawned, the committed worker owns the guard through cancellation.
        let guard = self.control.clone().lock_owned().await;
        let runtime = self.adapter.runtime.clone();
        let stopped = self.adapter.stopped.clone();
        let payload = self.adapter.payload.clone();
        let pending = self.adapter.pending.clone();
        tokio::task::spawn_blocking(move || {
            runtime.block_on(async move {
                let _guard = guard;
                stopped.store(true, Ordering::SeqCst);
                pending.drain().await;
                let _drain = payload.acquire_many(PAYLOAD_SLOTS as u32).await?;
                let mut slot = sessions
                    .lock()
                    .map_err(|_| anyhow::anyhow!("session lock poisoned"))?;
                if let Some(session) = slot.as_mut() {
                    if let Err(error) = session.unmount() {
                        // Failed unmount must leave a working server, including
                        // future payload/mutation admission, for the live mount.
                        stopped.store(false, Ordering::SeqCst);
                        return Err(error);
                    }
                    session.stop.store(true, Ordering::Release);
                    if let Some(worker) = session.worker.take() {
                        worker
                            .join()
                            .map_err(|_| anyhow::anyhow!("FUSE receiver panicked"))??;
                    }
                    slot.take();
                }
                Ok::<_, anyhow::Error>(())
            })
        })
        .await
        .context("output unmount worker failed")?
    }
}
