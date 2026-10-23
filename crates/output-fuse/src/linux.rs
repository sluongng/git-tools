use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use fuser::*;
use output_core::{
    Artifact, CoreError, GenerationLease, Node, NodeKind, NodeLease, Projection, SetAttr, Tree,
    TreeStat,
};
use std::{
    collections::HashMap,
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
    if error.is::<output_core::SourceLoadBusy>()
        || error.is::<output_core::SourceNotLoaded>()
        || error.is::<output_core::CacheTimeoutError>()
    {
        return libc::EAGAIN;
    }
    if error
        .downcast_ref::<output_core::CacheLimitError>()
        .is_some()
    {
        eprintln!("output-fuse hydration: {error:#}");
        return libc::EFBIG;
    }
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
struct RequestTicket {
    pending: Arc<Pending>,
    _generation: GenerationLease,
}
impl Drop for RequestTicket {
    fn drop(&mut self) {
        self.pending.count.fetch_sub(1, Ordering::SeqCst);
        self.pending.changed.notify_waiters();
    }
}

#[derive(Default)]
struct Maintenance {
    running: bool,
    requested: bool,
}

#[derive(Default)]
struct References {
    lookups: HashMap<u64, u64>,
    opens: HashMap<u64, Vec<NodeLease>>,
}
struct KernelReferences {
    tree: Arc<Tree>,
    references: Mutex<References>,
}
impl KernelReferences {
    fn entry(&self, node: &Node, already_retained: bool) -> Result<()> {
        let mut refs = self.references.lock().unwrap();
        if !already_retained {
            self.tree.retain_lookup(node.id)?;
        }
        *refs.lookups.entry(node.id).or_insert(0) += 1;
        Ok(())
    }
    fn open(&self, id: u64) -> Result<()> {
        let pin = self.tree.pin(id)?;
        self.references
            .lock()
            .unwrap()
            .opens
            .entry(id)
            .or_default()
            .push(pin);
        Ok(())
    }
    fn forget(&self, id: u64, count: u64) {
        let mut refs = self.references.lock().unwrap();
        if let Some(current) = refs.lookups.get_mut(&id) {
            let forgotten = count.min(*current);
            *current -= forgotten;
            self.tree.forget(id, forgotten);
            if *current == 0 {
                refs.lookups.remove(&id);
            }
        }
    }
    fn release(&self, id: u64) {
        let mut refs = self.references.lock().unwrap();
        if let Some(pins) = refs.opens.get_mut(&id) {
            pins.pop();
            if pins.is_empty() {
                refs.opens.remove(&id);
            }
        }
    }
    fn clear(&self) {
        let mut refs = self.references.lock().unwrap();
        for (id, count) in refs.lookups.drain() {
            self.tree.forget(id, count);
        }
        refs.opens.clear();
    }
}
impl Drop for KernelReferences {
    fn drop(&mut self) {
        self.clear();
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
    atomic_truncate: bool,
    runtime: Handle,
    payload: Arc<Semaphore>,
    pending: Arc<Pending>,
    stopped: Arc<AtomicBool>,
    references: Arc<KernelReferences>,
    maintenance: Arc<Mutex<Maintenance>>,
    retirement_budget: usize,
}
impl Adapter {
    fn dispatch<F: Future<Output = ()> + Send + 'static>(&self, work: F) {
        let runtime = self.runtime.clone();
        let payload = self.payload.clone();
        let adapter = self.clone();
        self.runtime.spawn(async move {
            let _admission = payload
                .acquire_owned()
                .await
                .expect("FUSE admission remains open");
            tokio::task::spawn_blocking(move || runtime.block_on(work))
                .await
                .ok();
            adapter.collect();
        });
    }
    fn collect(&self) {
        if self.retirement_budget == 0 {
            return;
        }
        {
            let mut maintenance = self.maintenance.lock().unwrap();
            maintenance.requested = true;
            if maintenance.running {
                return;
            }
            maintenance.running = true;
        }
        let adapter = self.clone();
        self.runtime.spawn_blocking(move || {
            loop {
                adapter.maintenance.lock().unwrap().requested = false;
                // Full rounds prevent pinned early IDs or parent dependencies from
                // starving reclaimable leaves/ancestors when no later RPC arrives.
                if let Err(error) = adapter
                    .tree
                    .reclaim_retired_at_boundary(adapter.retirement_budget)
                {
                    eprintln!("output retirement deferred: {error:#}");
                }
                let mut maintenance = adapter.maintenance.lock().unwrap();
                if !maintenance.requested {
                    maintenance.running = false;
                    break;
                }
            }
        });
    }
    fn enter(&self) -> Result<RequestTicket, i32> {
        // Count before testing stopped. Sequential consistency across these
        // two atomics excludes store-buffering: shutdown cannot observe zero
        // while this callback observes the pre-shutdown admission flag.
        self.pending.count.fetch_add(1, Ordering::SeqCst);
        let ticket = RequestTicket {
            pending: self.pending.clone(),
            _generation: self.tree.pin_generation(),
        };
        if self.stopped.load(Ordering::SeqCst) {
            return Err(libc::EIO);
        }
        Ok(ticket)
    }
}
fn reply_directory(tree: &Tree, ino: u64, offset: i64, mut reply: ReplyDirectory) {
    let result = (|| -> Result<()> {
        let directory = tree.get(ino)?;
        if offset == 0 && reply.add(ino, 1, FileType::Directory, ".") {
            return Ok(());
        }
        if offset <= 1 && reply.add(directory.parent, 2, FileType::Directory, "..") {
            return Ok(());
        }
        let mut after = (offset as u64).saturating_sub(2);
        loop {
            let nodes = tree.list(ino, after, 256)?;
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
impl Filesystem for Adapter {
    fn init(&mut self, _: &Request<'_>, config: &mut KernelConfig) -> Result<(), i32> {
        self.atomic_truncate = config.add_capabilities(consts::FUSE_ATOMIC_O_TRUNC).is_ok();
        let _ = config.set_max_background(128);
        let _ = config.set_max_write(1024 * 1024);
        Ok(())
    }
    fn lookup(&mut self, _: &Request<'_>, parent: u64, name_: &OsStr, reply: ReplyEntry) {
        let owned_name = match name(name_) {
            Ok(name) => name,
            Err(error) => return reply.error(errno(error)),
        };
        let result = self.tree.lookup_referenced(parent, &owned_name);
        if result
            .as_ref()
            .is_err_and(|e| e.is::<output_core::SourceNotLoaded>())
        {
            let ticket = match self.enter() {
                Ok(ticket) => ticket,
                Err(error) => return reply.error(error),
            };
            let tree = self.tree.clone();
            let references = self.references.clone();
            self.dispatch(async move {
                let _ticket = ticket;
                let result = async {
                    tree.ensure_directory_loaded(parent).await?;
                    let node = tree.lookup_referenced(parent, &owned_name)?;
                    references.entry(&node, true)?;
                    Ok::<_, anyhow::Error>(node)
                }
                .await;
                match result {
                    Ok(node) => reply.entry(&TTL, &attrs(&node), node.generation),
                    Err(error) => reply.error(errno(error)),
                }
            });
            return;
        }
        match result {
            Ok(n) => match self.references.entry(&n, true) {
                Ok(()) => reply.entry(&TTL, &attrs(&n), n.generation),
                Err(e) => reply.error(errno(e)),
            },
            Err(e) => reply.error(errno(e)),
        }
    }
    fn forget(&mut self, _: &Request<'_>, ino: u64, count: u64) {
        self.references.forget(ino, count);
        self.collect();
    }
    fn opendir(&mut self, _: &Request<'_>, ino: u64, _: i32, reply: ReplyOpen) {
        match self.references.open(ino) {
            Ok(()) => reply.opened(ino, 0),
            Err(e) => reply.error(errno(e)),
        }
    }
    fn release(
        &mut self,
        _: &Request<'_>,
        ino: u64,
        _: u64,
        _: i32,
        _: Option<u64>,
        _: bool,
        reply: ReplyEmpty,
    ) {
        self.references.release(ino);
        self.collect();
        reply.ok();
    }
    fn releasedir(&mut self, _: &Request<'_>, ino: u64, _: u64, _: i32, reply: ReplyEmpty) {
        self.references.release(ino);
        self.collect();
        reply.ok();
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
            Err(e) if e.is::<output_core::SourceNotLoaded>() => {
                let ticket = match self.enter() {
                    Ok(ticket) => ticket,
                    Err(error) => return reply.error(error),
                };
                let tree = self.tree.clone();
                self.dispatch(async move {
                    let _ticket = ticket;
                    match tree.readlink_source(ino).await {
                        Ok(target) => reply.data(target.as_bytes()),
                        Err(error) => reply.error(errno(error)),
                    }
                });
            }
            Err(e) => reply.error(errno(e)),
        }
    }
    fn readdir(&mut self, _: &Request<'_>, ino: u64, _: u64, offset: i64, reply: ReplyDirectory) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        match self.tree.list(ino, 0, 0) {
            Ok(_) => reply_directory(&self.tree, ino, offset, reply),
            Err(error) if error.is::<output_core::SourceNotLoaded>() => {
                let ticket = match self.enter() {
                    Ok(ticket) => ticket,
                    Err(error) => return reply.error(error),
                };
                let tree = self.tree.clone();
                self.dispatch(async move {
                    let _ticket = ticket;
                    match tree.ensure_directory_loaded(ino).await {
                        Ok(()) => reply_directory(&tree, ino, offset, reply),
                        Err(error) => reply.error(errno(error)),
                    }
                });
            }
            Err(error) => reply.error(errno(error)),
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
            // Linux buffered reads can normalize a FUSE page-cache failure to
            // EIO. Reject known impossible hydration at open instead, without
            // fetching, so users get the actionable file-too-large error.
            // O_TRUNC still discards a remote generation without hydrating it.
            // On old kernels without atomic truncation, writable opens must
            // remain possible for the kernel's separate truncate operation.
            let limit = self.tree.readable_limit();
            if node.remote.is_some()
                && node.size > limit
                && (self.atomic_truncate || flags & libc::O_ACCMODE == libc::O_RDONLY)
            {
                reply.error(errno(
                    output_core::CacheLimitError {
                        size: node.size,
                        limit,
                    }
                    .into(),
                ));
                return;
            }
            match self.references.open(ino) {
                Ok(()) => reply.opened(ino, 0),
                Err(e) => reply.error(errno(e)),
            }
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
        let references = self.references.clone();
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
                Ok(_) => match references.open(ino) {
                    Ok(()) => reply.opened(ino, 0),
                    Err(e) => reply.error(errno(e)),
                },
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
        let references = self.references.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.create(parent, &filename, mode & !umask & 0o7777).await {
                Ok(n) => match references
                    .entry(&n, false)
                    .and_then(|_| references.open(n.id))
                {
                    Ok(()) => reply.created(&TTL, &attrs(&n), n.generation, n.id, 0),
                    Err(e) => reply.error(errno(e)),
                },
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
        let references = self.references.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.create(parent, &filename, mode & !umask & 0o7777).await {
                Ok(n) => match references.entry(&n, false) {
                    Ok(()) => reply.entry(&TTL, &attrs(&n), n.generation),
                    Err(e) => reply.error(errno(e)),
                },
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
        let references = self.references.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.mkdir(parent, &filename, mode & !umask & 0o7777).await {
                Ok(n) => match references.entry(&n, false) {
                    Ok(()) => reply.entry(&TTL, &attrs(&n), n.generation),
                    Err(e) => reply.error(errno(e)),
                },
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
        let references = self.references.clone();
        self.dispatch(async move {
            let _permit = permit;
            match tree.symlink(parent, &filename, &target).await {
                Ok(n) => match references.entry(&n, false) {
                    Ok(()) => reply.entry(&TTL, &attrs(&n), n.generation),
                    Err(e) => reply.error(errno(e)),
                },
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
    start_with_retirement_budget(tree, root, 64).await
}
/// Configure the per-transaction retirement budget. Zero disables automatic
/// collection, retaining the conservative raw-ID behavior for diagnostics.
pub async fn start_with_retirement_budget(
    tree: Arc<Tree>,
    root: &Path,
    retirement_budget: usize,
) -> Result<Arc<FuseMount>> {
    start_named(tree, root, retirement_budget, "git-tools-outputs".into()).await
}
/// Native source mounts have a workspace-specific kernel source name, so
/// disconnected recovery cannot mistake an output mount for a source mount.
pub async fn start_native_source(
    tree: Arc<Tree>,
    root: &Path,
    workspace_identity: [u8; 32],
) -> Result<Arc<FuseMount>> {
    ensure!(
        workspace_identity != [0; 32],
        "native source identity must be nonzero"
    );
    let id: String = workspace_identity
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    start_named(tree, root, 64, format!("git-tools-native-{id}")).await
}
async fn start_named(
    tree: Arc<Tree>,
    root: &Path,
    retirement_budget: usize,
    source_name: String,
) -> Result<Arc<FuseMount>> {
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
        // Starting a new empty kernel mount ends the old handle generation.
        // Explicit external Tree users must pin any IDs they retain themselves.
        tree.reclaim_retired_at_boundary(retirement_budget)?;
        let references = Arc::new(KernelReferences {
            tree: tree.clone(),
            references: Mutex::new(References::default()),
        });
        let adapter = Adapter {
            tree,
            references,
            maintenance: Arc::new(Mutex::new(Maintenance::default())),
            retirement_budget,
            atomic_truncate: false,
            runtime,
            payload: Arc::new(Semaphore::new(PAYLOAD_SLOTS)),
            pending: Arc::new(Pending::default()),
            stopped: Arc::new(AtomicBool::new(false)),
        };
        let mut session = Session::new(
            adapter.clone(),
            &root,
            &[
                MountOption::FSName(source_name),
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
        let adapter = self.adapter.clone();
        tokio::task::spawn_blocking(move || {
            runtime.block_on(async move {
                let _guard = guard;
                ensure!(
                    !stopped.load(Ordering::Acquire),
                    "output projection stopped"
                );
                let generation = tree.pin_generation();
                let result = operation(tree, notifier).await;
                drop(generation);
                adapter.collect();
                result
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
    async fn wait_source_idle(&self) -> Result<()> {
        // Publication workers retain this mutex through physical completion and
        // kernel notifications, even if their initiating future was dropped.
        let _guard = self.control.lock().await;
        ensure!(
            !self.adapter.stopped.load(Ordering::Acquire),
            "native source projection is stopped"
        );
        Ok(())
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
    async fn publish_source(
        &self,
        manifest: output_core::SourceManifest,
        expected: output_core::SourceVersion,
        request_id: String,
        limits: output_core::SourceLimits,
    ) -> Result<output_core::SourceReceipt> {
        self.complete(move |tree, notifier| {
            Box::pin(async move {
                // Capture only metadata/local dirty state, never clean contents.
                // The install rechecks the complete version under its own fence;
                // a kernel writer in between makes the request stale, not lost.
                let snapshot =
                    tree.freeze_source(output_core::SourceFrontendFence::TreeOnly, &limits)?;
                let old: Vec<_> = snapshot
                    .entries()
                    .iter()
                    .map(|entry| entry.node.clone())
                    .collect();
                drop(snapshot);
                let receipt =
                    tree.install_source(manifest, Some(expected), &request_id, &limits)?;
                // complete() owns a generation lease across publication and all
                // notifications, including when the initiating RPC is cancelled.
                for node in old {
                    invalidate(&notifier, &node)?;
                }
                let current =
                    tree.freeze_source(output_core::SourceFrontendFence::TreeOnly, &limits)?;
                for entry in current.entries() {
                    invalidate(&notifier, &entry.node)?;
                }
                invalidate(&notifier, &tree.get(tree.root_id())?)?;
                Ok(receipt)
            })
        })
        .await
    }
    async fn publish_lazy_source(
        &self,
        root: output_core::LazySourceRoot,
        expected: output_core::SourceVersion,
        request_id: String,
        limits: output_core::SourceLimits,
    ) -> Result<output_core::SourceReceipt> {
        self.complete(move |tree, notifier| {
            Box::pin(async move {
                // Capture only the explicit lazy delta. Unopened directories
                // remain immutable base links rather than inferred deletions.
                let deadline = tokio::time::Instant::now()
                    + Duration::from_millis(root.load_limits.timeout_ms);
                let snapshot = tree
                    .freeze_lazy_source_wait(
                        output_core::SourceFrontendFence::TreeOnly,
                        &limits,
                        deadline,
                    )
                    .await?;
                let old = tree.known_source_nodes(&limits)?;
                // Retain the same source fence through replacement: dropping it
                // and reacquiring lets a kernel metadata job race publication.
                // The explicit original version also preserves exact replay.
                let receipt =
                    snapshot.install_with_expected(root, expected, &request_id, &limits)?;
                // complete() retains the generation/control guard through every
                // notification, even if its initiating RPC was cancelled.
                for node in old {
                    invalidate(&notifier, &node)?;
                }
                for node in tree.known_source_nodes(&limits)? {
                    invalidate(&notifier, &node)?;
                }
                invalidate(&notifier, &tree.get(tree.root_id())?)?;
                Ok(receipt)
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
        let references = self.adapter.references.clone();
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
                    references.clear();
                }
                Ok::<_, anyhow::Error>(())
            })
        })
        .await
        .context("output unmount worker failed")?
    }
}
