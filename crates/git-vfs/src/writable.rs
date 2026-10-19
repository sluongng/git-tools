//! Mutable FUSE view backed by a persistent copy-on-write overlay.
use crate::backend::GitRepo;
use crate::overlay::{Kind, Overlay};
use anyhow::Result;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr, Request, TimeOrNow,
};
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::{Duration, SystemTime};

// All supported overlay mutations go through this mount. Linux FUSE invalidates
// positive entry/attribute caches for those in-band operations, including direct
// writes. Keep DIRECT_IO and uncached ENOENT replies; out-of-band writers would
// require explicit invalidation before this cache policy could support them.
const TTL: Duration = Duration::from_secs(1);
const DIRECT_IO: u32 = 1;
const XATTRS: &[u8] = b"user.gitsha1\0user.gitsha1.hex\0";

fn error(code: i32) -> anyhow::Error {
    std::io::Error::from_raw_os_error(code).into()
}
fn errno(err: &anyhow::Error) -> i32 {
    err.downcast_ref::<std::io::Error>()
        .and_then(|e| e.raw_os_error())
        .unwrap_or(libc::EIO)
}
fn fail(err: anyhow::Error) -> i32 {
    let code = errno(&err);
    // Do not log backend errors: they can contain paths, URLs, or credentials.
    if code == libc::EIO {
        eprintln!("git-vfs: writable filesystem backend operation failed (EIO)");
    }
    code
}
fn empty(reply: ReplyEmpty, result: Result<()>) {
    match result {
        Ok(()) => reply.ok(),
        Err(e) => reply.error(fail(e)),
    }
}
fn xattr(reply: ReplyXattr, size: u32, data: &[u8]) {
    if size == 0 {
        reply.size(data.len() as u32);
    } else if (size as usize) < data.len() {
        reply.error(libc::ERANGE);
    } else {
        reply.data(data);
    }
}
#[derive(Clone, Copy)]
struct FileHandle {
    inode: u64,
    flags: i32,
}
struct DirHandle {
    inode: u64,
    entries: Vec<(u64, FileType, OsString)>,
}

pub struct WritableGitFs {
    blake3: Option<std::sync::Arc<crate::blake3_xattr::Blake3XattrStats>>,
    overlay: Overlay,
    repo: GitRepo,
    uid: u32,
    gid: u32,
    next_handle: u64,
    files: HashMap<u64, FileHandle>,
    dirs: HashMap<u64, DirHandle>,
}
impl WritableGitFs {
    pub fn new(overlay: Overlay, repo: GitRepo) -> Self {
        Self {
            blake3: None,
            overlay,
            repo,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            next_handle: 1,
            files: HashMap::new(),
            dirs: HashMap::new(),
        }
    }
    pub fn with_blake3(
        mut self,
        stats: std::sync::Arc<crate::blake3_xattr::Blake3XattrStats>,
    ) -> Self {
        self.blake3 = Some(stats);
        self
    }
    fn handle_id(&mut self) -> Result<u64> {
        let id = self.next_handle;
        self.next_handle = id.checked_add(1).ok_or_else(|| error(libc::EMFILE))?;
        Ok(id)
    }
    fn file_handle(&self, inode: u64, fh: u64) -> Result<FileHandle> {
        self.files
            .get(&fh)
            .copied()
            .filter(|h| h.inode == inode)
            .ok_or_else(|| error(libc::EBADF))
    }
    fn dir_handle(&self, inode: u64, fh: u64) -> Result<&DirHandle> {
        self.dirs
            .get(&fh)
            .filter(|h| h.inode == inode)
            .ok_or_else(|| error(libc::EBADF))
    }
    fn attr(&mut self, inode: u64) -> Result<FileAttr> {
        let size = self.overlay.size(inode, &mut self.repo)?;
        let n = self.overlay.entry(inode)?;
        let nlink = if !n.linked {
            0
        } else if n.kind.is_dir() {
            2 + self.overlay.directory_children_count(inode)? as u32
        } else {
            1
        };
        Ok(FileAttr {
            ino: inode,
            size,
            blocks: size.div_ceil(512),
            atime: n.mtime,
            mtime: n.mtime,
            ctime: n.ctime,
            crtime: n.ctime,
            kind: n.kind.file_type(),
            perm: (n.mode & 0o7777) as u16,
            nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        })
    }
    fn check_access(&self, req: &Request<'_>, inode: u64, mask: i32) -> Result<()> {
        let n = self.overlay.entry(inode)?;
        if mask & libc::W_OK != 0 && n.control {
            return Err(error(libc::EROFS));
        }
        if req.uid() == 0 {
            if mask & libc::X_OK != 0 && !n.kind.is_dir() && n.mode & 0o111 == 0 {
                return Err(error(libc::EACCES));
            }
            return Ok(());
        }
        let shift = if req.uid() == self.uid {
            6
        } else if req.gid() == self.gid {
            3
        } else {
            0
        };
        let allowed = (n.mode >> shift) & 7;
        if (mask as u32) & !allowed & 7 != 0 {
            Err(error(libc::EACCES))
        } else {
            Ok(())
        }
    }
    fn open_file(&mut self, req: &Request<'_>, inode: u64, flags: i32) -> Result<u64> {
        let n = self.overlay.entry(inode)?;
        if n.kind.is_dir() {
            return Err(error(libc::EISDIR));
        }
        if n.kind == Kind::Symlink {
            return Err(error(libc::ELOOP));
        }
        let access = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => libc::R_OK,
            libc::O_WRONLY => libc::W_OK,
            libc::O_RDWR => libc::R_OK | libc::W_OK,
            _ => return Err(error(libc::EINVAL)),
        };
        self.check_access(req, inode, access)?;
        if flags & libc::O_TRUNC != 0 {
            if flags & libc::O_ACCMODE == libc::O_RDONLY {
                return Err(error(libc::EACCES));
            }
            self.overlay
                .setattr(inode, None, Some(0), None, &mut self.repo)?;
        }
        let fh = self.handle_id()?;
        self.files.insert(fh, FileHandle { inode, flags });
        Ok(fh)
    }
    fn sync_file(&mut self, inode: u64, fh: u64) -> Result<()> {
        self.file_handle(inode, fh)?;
        self.overlay.sync()
    }
}
impl Filesystem for WritableGitFs {
    fn destroy(&mut self) {
        if let Err(e) = self.overlay.sync() {
            let _ = fail(e);
        }
    }
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let result = self
            .overlay
            .lookup(parent, name)
            .and_then(|ino| self.attr(ino));
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, fh: Option<u64>, reply: ReplyAttr) {
        let result = (|| {
            if let Some(fh) = fh
                && self.file_handle(ino, fh).is_err()
            {
                self.dir_handle(ino, fh)?;
            }
            self.attr(ino)
        })();
        match result {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn setattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let result = (|| {
            if uid.is_some_and(|u| u != self.uid) || gid.is_some_and(|g| g != self.gid) {
                return Err(error(libc::EPERM));
            }
            if flags.is_some_and(|f| f != 0) {
                return Err(error(libc::EOPNOTSUPP));
            }
            if req.uid() != 0 && req.uid() != self.uid {
                return Err(error(libc::EPERM));
            }
            if let Some(fh) = fh {
                match self.file_handle(ino, fh) {
                    Ok(h) if size.is_some() && h.flags & libc::O_ACCMODE == libc::O_RDONLY => {
                        return Err(error(libc::EBADF));
                    }
                    Ok(_) => {}
                    Err(_) => {
                        self.dir_handle(ino, fh)?;
                    }
                }
            } else if size.is_some() {
                self.check_access(req, ino, libc::W_OK)?;
            }
            let mtime = mtime.map(|t| match t {
                TimeOrNow::SpecificTime(t) => t,
                TimeOrNow::Now => SystemTime::now(),
            });
            self.overlay
                .setattr(ino, mode, size, mtime, &mut self.repo)?;
            self.attr(ino)
        })();
        match result {
            Ok(attr) => reply.attr(&TTL, &attr),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn open(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        // Direct I/O avoids stale page-cache/mmap content after a second handle writes.
        // Writable mmap and writeback caching are intentionally unsupported.
        match self.open_file(req, ino, flags) {
            Ok(fh) => reply.opened(fh, DIRECT_IO),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let result = (|| {
            let h = self.file_handle(ino, fh)?;
            if h.flags & libc::O_ACCMODE == libc::O_WRONLY {
                return Err(error(libc::EBADF));
            }
            if offset < 0 {
                return Err(error(libc::EINVAL));
            }
            if size == 0 {
                return Ok(Vec::new());
            }
            let data = self.overlay.read(ino, &mut self.repo)?;
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(data.len());
            let end = start.saturating_add(size as usize).min(data.len());
            Ok(data[start..end].to_vec())
        })();
        match result {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let result = (|| {
            let h = self.file_handle(ino, fh)?;
            if h.flags & libc::O_ACCMODE == libc::O_RDONLY {
                return Err(error(libc::EBADF));
            }
            if offset < 0 {
                return Err(error(libc::EINVAL));
            }
            if data.is_empty() {
                return Ok(0);
            }
            // fuser serializes callbacks; size lookup and append form one operation.
            let offset = if h.flags & libc::O_APPEND != 0 {
                self.overlay.size(ino, &mut self.repo)?
            } else {
                offset as u64
            };
            let count = self.overlay.write(ino, offset, data, &mut self.repo)?;
            if h.flags & (libc::O_SYNC | libc::O_DSYNC) != 0 {
                self.overlay.sync()?;
            }
            u32::try_from(count).map_err(|_| error(libc::EOVERFLOW))
        })();
        match result {
            Ok(count) => reply.written(count),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let result = (|| {
            if !matches!(
                flags & libc::O_ACCMODE,
                libc::O_RDONLY | libc::O_WRONLY | libc::O_RDWR
            ) {
                return Err(error(libc::EINVAL));
            }
            self.check_access(req, parent, libc::W_OK | libc::X_OK)?;
            let fh = self.handle_id()?;
            let ino = self.overlay.create(
                parent,
                name,
                Kind::File,
                (mode & 0o7777) & !umask,
                Some(Vec::new()),
            )?;
            // Creation grants the requested descriptor access even with mode 000.
            self.files.insert(fh, FileHandle { inode: ino, flags });
            Ok((self.attr(ino)?, fh))
        })();
        match result {
            Ok((attr, fh)) => reply.created(&TTL, &attr, 0, fh, DIRECT_IO),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        if mode & libc::S_IFMT != libc::S_IFREG {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        let result = self
            .overlay
            .create(
                parent,
                name,
                Kind::File,
                (mode & 0o7777) & !umask,
                Some(Vec::new()),
            )
            .and_then(|ino| self.attr(ino));
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let result = self
            .overlay
            .create(
                parent,
                name,
                Kind::Directory,
                (mode & 0o7777) & !umask,
                None,
            )
            .and_then(|ino| self.attr(ino));
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let result = self
            .overlay
            .create(
                parent,
                name,
                Kind::Symlink,
                0o777,
                Some(target.as_os_str().as_bytes().to_vec()),
            )
            .and_then(|ino| self.attr(ino));
        match result {
            Ok(attr) => reply.entry(&TTL, &attr, 0),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let result = (|| {
            if self.overlay.entry(ino)?.kind != Kind::Symlink {
                return Err(error(libc::EINVAL));
            }
            self.overlay.read(ino, &mut self.repo)
        })();
        match result {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        empty(reply, self.overlay.unlink(parent, name, false));
    }
    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        empty(reply, self.overlay.unlink(parent, name, true));
    }
    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        empty(
            reply,
            self.overlay.rename(parent, name, newparent, newname, flags),
        );
    }
    fn flush(&mut self, _req: &Request<'_>, ino: u64, fh: u64, _owner: u64, reply: ReplyEmpty) {
        let result = self.file_handle(ino, fh).and_then(|handle| {
            // Reading cannot leave dirty state. Do not force a database-wide
            // disk sync for each ordinary read/close; explicit fsync still does.
            if handle.flags & libc::O_ACCMODE == libc::O_RDONLY {
                Ok(())
            } else {
                self.overlay.sync()
            }
        });
        empty(reply, result);
    }
    fn fsync(&mut self, _req: &Request<'_>, ino: u64, fh: u64, _datasync: bool, reply: ReplyEmpty) {
        let result = self.sync_file(ino, fh);
        empty(reply, result);
    }
    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let result = self.file_handle(ino, fh).and_then(|handle| {
            self.files.remove(&fh);
            if handle.flags & libc::O_ACCMODE == libc::O_RDONLY {
                Ok(())
            } else {
                self.overlay.sync()
            }
        });
        empty(reply, result);
    }
    fn opendir(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let result = (|| {
            let n = self.overlay.entry(ino)?;
            if !n.kind.is_dir() {
                return Err(error(libc::ENOTDIR));
            }
            if flags & libc::O_ACCMODE != libc::O_RDONLY {
                return Err(error(libc::EISDIR));
            }
            let mut entries = vec![
                (ino, FileType::Directory, OsString::from(".")),
                (n.parent, FileType::Directory, OsString::from("..")),
            ];
            for child in self.overlay.children(ino)? {
                let n = self.overlay.entry(child)?;
                entries.push((child, n.kind.file_type(), n.name.clone()));
            }
            let fh = self.handle_id()?;
            self.dirs.insert(
                fh,
                DirHandle {
                    inode: ino,
                    entries,
                },
            );
            Ok(fh)
        })();
        match result {
            Ok(fh) => reply.opened(fh, 0),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        match self.dir_handle(ino, fh) {
            Ok(h) => {
                for (index, (ino, kind, name)) in h
                    .entries
                    .iter()
                    .enumerate()
                    .skip(usize::try_from(offset).unwrap_or(usize::MAX))
                {
                    if reply.add(*ino, (index + 1) as i64, *kind, name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(fail(e)),
        }
    }
    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        let result = self.dir_handle(ino, fh).map(|_| ()).map(|_| {
            self.dirs.remove(&fh);
        });
        empty(reply, result);
    }
    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let result = self
            .dir_handle(ino, fh)
            .map(|_| ())
            .and_then(|_| self.overlay.sync());
        empty(reply, result);
    }
    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let result = (|| {
            if name == OsStr::new("user.blake3") || name == OsStr::new("user.blake3.hex") {
                let stats = self.blake3.as_ref().ok_or_else(|| error(libc::ENODATA))?;
                let entry = self.overlay.entry(ino)?;
                if entry.control || entry.kind.is_dir() {
                    stats.miss();
                    return Err(error(libc::ENODATA));
                }
                // data is authoritative after every write, including same-size edits;
                // base_oid remains the original Git identity for journal purposes.
                let digest = if let Some(data) = &entry.data {
                    stats.local(data).to_vec()
                } else if let Some(oid) = &entry.base_oid {
                    stats.base(&self.repo, oid)?
                } else {
                    stats.miss();
                    return Err(error(libc::ENODATA));
                };
                return Ok(if name == OsStr::new("user.blake3.hex") {
                    hex::encode(digest).into_bytes()
                } else {
                    digest
                });
            }
            if name != OsStr::new("user.gitsha1") && name != OsStr::new("user.gitsha1.hex") {
                return Err(error(libc::ENODATA));
            }
            let digest = self.overlay.digest(ino)?;
            Ok(if name == OsStr::new("user.gitsha1.hex") {
                hex::encode(digest).into_bytes()
            } else {
                digest
            })
        })();
        match result {
            Ok(data) => xattr(reply, size, &data),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        match self.overlay.entry(ino) {
            Ok(n) => xattr(
                reply,
                size,
                if n.kind.is_dir() || n.control {
                    b""
                } else if self.blake3.is_some() {
                    crate::blake3_xattr::XATTRS
                } else {
                    XATTRS
                },
            ),
            Err(e) => reply.error(fail(e)),
        }
    }
    fn access(&mut self, req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        empty(reply, self.check_access(req, ino, mask));
    }
}
