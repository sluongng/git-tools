//! Immutable Git tree view. Blob contents are read through Git's promisor mechanism.
use crate::backend::GitRepo;
use anyhow::{Context, Result, anyhow, ensure};
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyXattr, Request,
};
use serde::Serialize;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TTL: Duration = Duration::from_secs(3600);
const XATTRS: &[u8] = b"user.gitsha1\0user.gitsha1.hex\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    File,
    Directory,
    Symlink,
    Gitlink,
}
impl Kind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
            Self::Symlink => "symlink",
            Self::Gitlink => "gitlink",
        }
    }
    fn file_type(self) -> FileType {
        match self {
            Self::File => FileType::RegularFile,
            Self::Symlink => FileType::Symlink,
            _ => FileType::Directory,
        }
    }
    fn is_dir(self) -> bool {
        matches!(self, Self::Directory | Self::Gitlink)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Node {
    pub(crate) inode: u64,
    pub(crate) parent: u64,
    pub(crate) path: PathBuf,
    pub(crate) name: OsString,
    pub(crate) mode: u32,
    pub(crate) kind: Kind,
    pub(crate) oid: Option<String>,
    pub(crate) children: Vec<u64>,
}

#[derive(Debug, Serialize)]
pub struct PublicEntry {
    pub path: String,
    pub inode: u64,
    pub mode: u32,
    pub kind: String,
    pub oid: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct FileMetadata {
    pub path: String,
    pub inode: u64,
    pub mode: u32,
    pub kind: String,
    pub oid: Option<String>,
    pub size: u64,
    pub digest_hex: Option<String>,
}

#[derive(Clone)]
pub struct Snapshot {
    pub commit: String,
    pub(crate) nodes: Vec<Node>,
    paths: HashMap<PathBuf, u64>,
    pub(crate) timestamp: SystemTime,
}

fn errno_error(code: i32) -> anyhow::Error {
    std::io::Error::from_raw_os_error(code).into()
}
fn errno(error: &anyhow::Error) -> i32 {
    error
        .downcast_ref::<std::io::Error>()
        .and_then(|e| e.raw_os_error())
        .unwrap_or(libc::EIO)
}

impl Snapshot {
    pub fn load(repo: &GitRepo, revision: &str) -> Result<Self> {
        let spec = format!("{revision}^{{commit}}");
        let commit = String::from_utf8(
            repo.git_output(&["rev-parse", "--verify", "--end-of-options", &spec], false)?,
        )?
        .trim()
        .to_owned();
        ensure!(
            commit.len() == 40 && commit.bytes().all(|b| b.is_ascii_hexdigit()),
            "GITSHA1 requires a SHA-1 Git repository"
        );
        let seconds: i64 = String::from_utf8(repo.git_output(
            &[
                "show",
                "-s",
                "--no-show-signature",
                "--no-notes",
                "--format=%ct",
                &commit,
            ],
            false,
        )?)?
        .trim()
        .parse()?;
        let timestamp = if seconds >= 0 {
            UNIX_EPOCH.checked_add(Duration::from_secs(seconds as u64))
        } else {
            UNIX_EPOCH.checked_sub(Duration::from_secs(seconds.unsigned_abs()))
        }
        .context("commit timestamp is out of range")?;
        let mut snapshot = Self {
            commit,
            nodes: vec![Node {
                inode: 1,
                parent: 1,
                path: PathBuf::new(),
                name: OsString::new(),
                mode: 0o040000,
                kind: Kind::Directory,
                oid: None,
                children: Vec::new(),
            }],
            paths: HashMap::from([(PathBuf::new(), 1)]),
            timestamp,
        };
        let tree = repo.git_output(
            &["ls-tree", "-r", "-t", "-z", "--full-tree", &snapshot.commit],
            false,
        )?;
        for record in tree.split(|&b| b == 0).filter(|r| !r.is_empty()) {
            let tab = record
                .iter()
                .position(|&b| b == b'\t')
                .context("invalid ls-tree entry")?;
            let header = std::str::from_utf8(&record[..tab])?;
            let fields: Vec<_> = header.split(' ').collect();
            ensure!(fields.len() == 3, "invalid ls-tree header");
            let mode = u32::from_str_radix(fields[0], 8)?;
            let kind = match (mode, fields[1]) {
                (0o040000, "tree") => Kind::Directory,
                (0o100644 | 0o100755, "blob") => Kind::File,
                (0o120000, "blob") => Kind::Symlink,
                (0o160000, "commit") => Kind::Gitlink,
                _ => {
                    return Err(anyhow!(
                        "unsupported tree entry mode {} type {}",
                        fields[0],
                        fields[1]
                    ));
                }
            };
            ensure!(
                fields[2].len() == 40 && fields[2].bytes().all(|b| b.is_ascii_hexdigit()),
                "invalid SHA-1 object ID"
            );
            let raw_path = &record[tab + 1..];
            ensure!(
                raw_path
                    .split(|&byte| byte == b'/')
                    .all(|part| !part.is_empty() && part != b"." && part != b".."),
                "unsafe Git tree path"
            );
            let path = PathBuf::from(OsString::from_vec(raw_path.to_vec()));
            ensure!(
                !path.as_os_str().is_empty()
                    && path.components().all(|c| matches!(c, Component::Normal(_))),
                "unsafe Git tree path"
            );
            let parent_path = path.parent().unwrap_or(Path::new(""));
            let parent = *snapshot
                .paths
                .get(parent_path)
                .context("tree entry missing parent")?;
            ensure!(
                snapshot.node(parent)?.kind == Kind::Directory,
                "tree entry below non-directory"
            );
            ensure!(!snapshot.paths.contains_key(&path), "duplicate tree path");
            let inode = snapshot.nodes.len() as u64 + 1;
            let name = path
                .file_name()
                .context("tree path missing name")?
                .to_owned();
            snapshot.paths.insert(path.clone(), inode);
            snapshot.nodes.push(Node {
                inode,
                parent,
                path,
                name,
                mode,
                kind,
                oid: Some(fields[2].to_owned()),
                children: Vec::new(),
            });
            snapshot.nodes[(parent - 1) as usize].children.push(inode);
        }
        Ok(snapshot)
    }

    fn node(&self, inode: u64) -> Result<&Node> {
        inode
            .checked_sub(1)
            .and_then(|i| self.nodes.get(i as usize))
            .ok_or_else(|| errno_error(libc::ENOENT))
    }
    fn path_node(&self, path: &Path) -> Result<&Node> {
        let mut clean = PathBuf::new();
        for component in path.components() {
            match component {
                Component::RootDir | Component::CurDir => {}
                Component::Normal(name) => clean.push(name),
                _ => return Err(errno_error(libc::EINVAL)),
            }
        }
        let inode = self
            .paths
            .get(&clean)
            .ok_or_else(|| errno_error(libc::ENOENT))?;
        self.node(*inode)
    }
    pub fn entries(&self) -> Vec<PublicEntry> {
        self.nodes
            .iter()
            .skip(1)
            .map(|n| PublicEntry {
                path: n.path.to_string_lossy().into_owned(),
                inode: n.inode,
                mode: n.mode,
                kind: n.kind.name().into(),
                oid: n.oid.clone(),
            })
            .collect()
    }
    fn size(&self, repo: &mut GitRepo, node: &Node) -> Result<u64> {
        if node.kind.is_dir() {
            return Ok(0);
        }
        let oid = node.oid.as_ref().context("blob missing object ID")?;
        let sizes = repo.blob_sizes(std::slice::from_ref(oid))?;
        sizes
            .get(oid)
            .copied()
            .context("size query omitted requested blob")
    }
    pub fn stat_path(&self, repo: &mut GitRepo, path: &Path) -> Result<FileMetadata> {
        let n = self.path_node(path)?;
        Ok(FileMetadata {
            path: n.path.to_string_lossy().into_owned(),
            inode: n.inode,
            mode: n.mode,
            kind: n.kind.name().into(),
            oid: n.oid.clone(),
            size: self.size(repo, n)?,
            digest_hex: if n.kind.is_dir() { None } else { n.oid.clone() },
        })
    }
    fn read_node(&self, repo: &mut GitRepo, n: &Node) -> Result<Vec<u8>> {
        if n.kind.is_dir() {
            return Err(errno_error(libc::EISDIR));
        }
        repo.read_blob(n.oid.as_deref().context("blob missing object ID")?)
    }
    /// Reads blob bytes without checkout filters. Symlinks return their target bytes.
    pub fn read_path(&self, repo: &mut GitRepo, path: &Path) -> Result<Vec<u8>> {
        self.read_node(repo, self.path_node(path)?)
    }
    pub fn digest_path(&self, path: &Path) -> Result<Vec<u8>> {
        let n = self.path_node(path)?;
        if n.kind.is_dir() {
            return Err(errno_error(libc::ENODATA));
        }
        Ok(hex::decode(
            n.oid.as_deref().context("blob missing object ID")?,
        )?)
    }
    fn attr(&self, repo: &mut GitRepo, inode: u64, uid: u32, gid: u32) -> Result<FileAttr> {
        let n = self.node(inode)?;
        let size = self.size(repo, n)?;
        let perm = match n.kind {
            Kind::Directory | Kind::Gitlink => 0o555,
            Kind::Symlink => 0o777,
            Kind::File => (n.mode & 0o777 & !0o222) as u16,
        };
        Ok(FileAttr {
            ino: inode,
            size,
            blocks: size.div_ceil(512),
            atime: self.timestamp,
            mtime: self.timestamp,
            ctime: self.timestamp,
            crtime: self.timestamp,
            kind: n.kind.file_type(),
            perm,
            nlink: if n.kind.is_dir() {
                2 + n
                    .children
                    .iter()
                    .filter(|&&i| self.nodes[(i - 1) as usize].kind.is_dir())
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
}

pub struct GitFs {
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
            snapshot,
            repo: Mutex::new(repo),
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        }
    }
    pub fn with_blake3(
        mut self,
        stats: std::sync::Arc<crate::blake3_xattr::Blake3XattrStats>,
    ) -> Self {
        self.blake3 = Some(stats);
        self
    }
    fn attr(&self, inode: u64) -> Result<FileAttr> {
        self.snapshot
            .attr(
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
                    .find(|&&i| self.snapshot.nodes[(i - 1) as usize].name == name)
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
                let child = &self.snapshot.nodes[(i - 1) as usize];
                (i, child.kind.file_type(), child.name.as_os_str())
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
                b"user.gitsha1" => 20,
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
            b"user.gitsha1" => match hex::decode(oid) {
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
                    crate::blake3_xattr::XATTRS
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
