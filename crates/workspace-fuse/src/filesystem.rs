//! Read-only FUSE adapter for an immutable Git tree.
use crate::backend::GitRepo;
use anyhow::{Result, anyhow};
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyXattr, Request,
};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::Mutex;
use std::time::Duration;
use workspace_core::filesystem::Kind;
pub use workspace_core::filesystem::{FileMetadata, PublicEntry, Snapshot};
const TTL: Duration = Duration::from_secs(3600);
const XATTRS: &[u8] = b"user.gitsha1\0user.gitsha1.hex\0user.gitsha1.base\0";
fn errno_error(code: i32) -> anyhow::Error {
    std::io::Error::from_raw_os_error(code).into()
}
fn file_type(kind: Kind) -> FileType {
    match kind {
        Kind::File => FileType::RegularFile,
        Kind::Symlink => FileType::Symlink,
        _ => FileType::Directory,
    }
}
fn errno(error: &anyhow::Error) -> i32 {
    error
        .downcast_ref::<std::io::Error>()
        .and_then(|e| e.raw_os_error())
        .unwrap_or(libc::EIO)
}

fn snapshot_attr(
    snapshot: &Snapshot,
    repo: &mut GitRepo,
    inode: u64,
    uid: u32,
    gid: u32,
) -> Result<FileAttr> {
    let n = snapshot.node(inode)?;
    let size = snapshot.size(repo, n)?;
    let perm = match n.kind {
        Kind::Directory | Kind::Gitlink => 0o555,
        Kind::Symlink => 0o777,
        Kind::File => (n.mode & 0o777 & !0o222) as u16,
    };
    Ok(FileAttr {
        ino: inode,
        size,
        blocks: size.div_ceil(512),
        atime: snapshot.timestamp(),
        mtime: snapshot.timestamp(),
        ctime: snapshot.timestamp(),
        crtime: snapshot.timestamp(),
        kind: file_type(n.kind),
        perm,
        nlink: if n.kind.is_dir() {
            2 + n
                .children
                .iter()
                .filter(|&&i| snapshot.nodes()[(i - 1) as usize].kind.is_dir())
                .count() as u32
        } else {
            1
        },
        uid,
        gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    })
}
pub struct GitFs {
    content_owner: Option<crate::backend::ContentScheduler>,
    blake3: Option<std::sync::Arc<crate::blake3_xattr::Blake3XattrStats>>,
    snapshot: Snapshot,
    repo: Mutex<GitRepo>,
    uid: u32,
    gid: u32,
}
impl GitFs {
    pub fn new(snapshot: Snapshot, repo: GitRepo) -> Self {
        Self {
            blake3: None,
            content_owner: None,
            snapshot,
            repo: Mutex::new(repo),
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        }
    }
    pub fn with_content_scheduler(mut self, scheduler: crate::backend::ContentScheduler) -> Self {
        self.repo
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .set_content_handle(scheduler.handle());
        self.content_owner = Some(scheduler);
        self
    }
    fn content_handle(&self) -> Option<crate::backend::ContentHandle> {
        self.repo
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .content_handle()
    }
    pub fn with_blake3(
        mut self,
        stats: std::sync::Arc<crate::blake3_xattr::Blake3XattrStats>,
    ) -> Self {
        self.blake3 = Some(stats);
        self
    }
    fn attr(&self, inode: u64) -> Result<FileAttr> {
        snapshot_attr(
            &self.snapshot,
            &mut *self
                .repo
                .lock()
                .map_err(|_| anyhow!("Git backend lock poisoned"))?,
            inode,
            self.uid,
            self.gid,
        )
        .inspect_err(|error| log_backend_error("metadata", inode, error))
    }
    fn data(&self, inode: u64) -> Result<Vec<u8>> {
        self.snapshot
            .read_node(
                &mut *self
                    .repo
                    .lock()
                    .map_err(|_| anyhow!("Git backend lock poisoned"))?,
                self.snapshot.node(inode)?,
            )
            .inspect_err(|error| log_backend_error("content", inode, error))
    }
}

fn log_backend_error(operation: &str, inode: u64, error: &anyhow::Error) {
    if errno(error) != libc::EIO {
        return;
    }
    // Backend errors exclude Git stderr, which can contain URLs and credentials.
    // Keep diagnostics independent of user paths and prevent terminal-control output.
    let message: String = format!("{error:#}")
        .chars()
        .take(2048)
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect();
    eprintln!("git-vfs: {operation} failed for inode {inode}: {message}");
}

fn reply_xattr(reply: ReplyXattr, size: u32, data: &[u8]) {
    if size == 0 {
        reply.size(data.len() as u32);
    } else if (size as usize) < data.len() {
        reply.error(libc::ERANGE);
    } else {
        reply.data(data);
    }
}

impl Drop for GitFs {
    fn drop(&mut self) {
        drop(self.content_owner.take());
    }
}

impl Filesystem for GitFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let result = (|| {
            let p = self.snapshot.node(parent)?;
            if !p.kind.is_dir() {
                return Err(errno_error(libc::ENOTDIR));
            }
            let inode = if name == OsStr::new(".") {
                parent
            } else if name == OsStr::new("..") {
                p.parent
            } else {
                *p.children
                    .iter()
                    .find(|&&i| self.snapshot.nodes()[(i - 1) as usize].name == name)
                    .ok_or_else(|| errno_error(libc::ENOENT))?
            };
            self.attr(inode)
        })();
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(errno(&e)),
        }
    }
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.attr(ino) {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(e) => reply.error(errno(&e)),
        }
    }
    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        match self.snapshot.node(ino) {
            Err(e) => reply.error(errno(&e)),
            Ok(n) if n.kind.is_dir() => reply.error(libc::EISDIR),
            Ok(n) if n.kind == Kind::Symlink => reply.error(libc::ELOOP),
            Ok(_)
                if flags & libc::O_ACCMODE != libc::O_RDONLY
                    || flags & (libc::O_TRUNC | libc::O_APPEND) != 0 =>
            {
                reply.error(libc::EROFS)
            }
            Ok(_) => reply.opened(ino, 0),
        }
    }
    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        if let Some(content) = self.content_handle() {
            match self.snapshot.node(ino) {
                Ok(node) if !node.kind.is_dir() => {
                    let Some(oid) = node.oid.clone() else {
                        reply.error(libc::EIO);
                        return;
                    };
                    let _ = content.read(&oid, offset as u64, size, move |result| match result {
                        Ok(bytes) => reply.data(&bytes),
                        Err(error) => reply.error(error.errno()),
                    });
                }
                Ok(_) => reply.error(libc::EISDIR),
                Err(error) => reply.error(errno(&error)),
            }
            return;
        }
        // No userspace content cache: Git persists fetched objects, and the kernel caches reads.
        match self.data(ino) {
            Ok(data) => {
                let start = usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(data.len());
                let end = start.saturating_add(size as usize).min(data.len());
                reply.data(&data[start..end]);
            }
            Err(e) => reply.error(errno(&e)),
        }
    }
    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        if let Some(content) = self.content_handle() {
            let plan = (|| -> Result<(String, u32)> {
                let node = self.snapshot.node(ino)?;
                if node.kind != Kind::Symlink {
                    return Err(errno_error(libc::EINVAL));
                }
                let oid = node.oid.clone().ok_or_else(|| errno_error(libc::EIO))?;
                let length =
                    u32::try_from(self.attr(ino)?.size).map_err(|_| errno_error(libc::EFBIG))?;
                Ok((oid, length))
            })();
            match plan {
                Ok((oid, length)) => {
                    let _ = content.read(&oid, 0, length, move |result| match result {
                        Ok(bytes) => reply.data(&bytes),
                        Err(error) => reply.error(error.errno()),
                    });
                }
                Err(error) => reply.error(errno(&error)),
            }
            return;
        }
        match self.snapshot.node(ino) {
            Err(e) => reply.error(errno(&e)),
            Ok(n) if n.kind != Kind::Symlink => reply.error(libc::EINVAL),
            Ok(_) => match self.data(ino) {
                Ok(data) => reply.data(&data),
                Err(e) => reply.error(errno(&e)),
            },
        }
    }
    fn opendir(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        match self.snapshot.node(ino) {
            Err(e) => reply.error(errno(&e)),
            Ok(n) if !n.kind.is_dir() => reply.error(libc::ENOTDIR),
            Ok(_) if flags & libc::O_ACCMODE != libc::O_RDONLY => reply.error(libc::EROFS),
            Ok(_) => reply.opened(ino, 0),
        }
    }
    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let n = match self.snapshot.node(ino) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(&e));
                return;
            }
        };
        if !n.kind.is_dir() {
            reply.error(libc::ENOTDIR);
            return;
        }
        let entries = std::iter::once((ino, FileType::Directory, OsStr::new(".")))
            .chain(std::iter::once((
                n.parent,
                FileType::Directory,
                OsStr::new(".."),
            )))
            .chain(n.children.iter().map(|&i| {
                let child = &self.snapshot.nodes()[(i - 1) as usize];
                (i, file_type(child.kind), child.name.as_os_str())
            }));
        for (index, (inode, kind, name)) in entries
            .enumerate()
            .skip(usize::try_from(offset).unwrap_or(usize::MAX))
        {
            if reply.add(inode, (index + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok();
    }
    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let n = match self.snapshot.node(ino) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(&e));
                return;
            }
        };
        if n.kind.is_dir() {
            reply.error(libc::ENODATA);
            return;
        }
        let oid = n.oid.as_ref().expect("validated blob ID");
        if size == 0 {
            let length = match name.as_bytes() {
                b"user.gitsha1" | b"user.gitsha1.base" => 20,
                b"user.gitsha1.hex" => 40,
                b"user.blake3" | b"user.blake3.hex" => {
                    if self.blake3.is_none() {
                        reply.error(libc::ENODATA);
                        return;
                    }
                    let available = self
                        .repo
                        .lock()
                        .map_err(|_| anyhow::anyhow!("backend lock poisoned"))
                        .and_then(|repo| repo.cached_blake3(oid));
                    match available {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            reply.error(libc::ENODATA);
                            return;
                        }
                        Err(error) => {
                            reply.error(errno(&error));
                            return;
                        }
                    }
                    if name.as_bytes() == b"user.blake3.hex" {
                        64
                    } else {
                        32
                    }
                }
                _ => {
                    reply.error(libc::ENODATA);
                    return;
                }
            };
            reply.size(length);
            return;
        }
        if matches!(name.as_bytes(), b"user.blake3" | b"user.blake3.hex") {
            let Some(stats) = &self.blake3 else {
                reply.error(libc::ENODATA);
                return;
            };
            match self
                .repo
                .lock()
                .map_err(|_| anyhow::anyhow!("backend lock poisoned"))
                .and_then(|repo| stats.base(&repo, oid))
            {
                Ok(data) => reply_xattr(
                    reply,
                    size,
                    &if name.as_bytes() == b"user.blake3.hex" {
                        hex::encode(data).into_bytes()
                    } else {
                        data
                    },
                ),
                Err(error) => reply.error(errno(&error)),
            }
            return;
        }
        match name.as_bytes() {
            b"user.gitsha1" | b"user.gitsha1.base" => match hex::decode(oid) {
                Ok(data) => reply_xattr(reply, size, &data),
                Err(_) => reply.error(libc::EIO),
            },
            b"user.gitsha1.hex" => reply_xattr(reply, size, oid.as_bytes()),
            _ => reply.error(libc::ENODATA),
        }
    }
    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        match self.snapshot.node(ino) {
            Ok(n) => reply_xattr(
                reply,
                size,
                if n.kind.is_dir() {
                    b""
                } else if self.blake3.is_some() {
                    crate::blake3_xattr::BASE_XATTRS
                } else {
                    XATTRS
                },
            ),
            Err(e) => reply.error(errno(&e)),
        }
    }
    fn access(&mut self, req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        let n = match self.snapshot.node(ino) {
            Ok(n) => n,
            Err(e) => {
                reply.error(errno(&e));
                return;
            }
        };
        if mask & libc::W_OK != 0 {
            reply.error(libc::EROFS);
        } else if mask & libc::X_OK != 0
            && !n.kind.is_dir()
            && n.kind != Kind::Symlink
            && n.mode & 0o111 == 0
        {
            reply.error(libc::EACCES);
        } else {
            let _ = req;
            reply.ok();
        }
    }
}
