//! Immutable Git tree view. Blob contents are read through Git's promisor mechanism.
use crate::backend::GitRepo;
use anyhow::{Context, Result, anyhow, ensure};
use serde::Serialize;
use std::collections::HashMap;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    File,
    Directory,
    Symlink,
    Gitlink,
}
impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
            Self::Symlink => "symlink",
            Self::Gitlink => "gitlink",
        }
    }
    pub fn is_dir(self) -> bool {
        matches!(self, Self::Directory | Self::Gitlink)
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    pub inode: u64,
    pub parent: u64,
    pub path: PathBuf,
    pub name: OsString,
    pub mode: u32,
    pub kind: Kind,
    pub oid: Option<String>,
    pub children: Vec<u64>,
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

/// Clones share immutable tree records and path lookup storage.
#[derive(Clone)]
pub struct Snapshot {
    pub commit: String,
    pub(crate) nodes: Arc<Vec<Node>>,
    paths: Arc<HashMap<PathBuf, u64>>,
    pub(crate) timestamp: SystemTime,
}

fn errno_error(code: i32) -> anyhow::Error {
    std::io::Error::from_raw_os_error(code).into()
}

impl Snapshot {
    /// Immutable tree records in inode order.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

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
        let mut nodes = vec![Node {
            inode: 1,
            parent: 1,
            path: PathBuf::new(),
            name: OsString::new(),
            mode: 0o040000,
            kind: Kind::Directory,
            oid: None,
            children: Vec::new(),
        }];
        let mut paths = HashMap::from([(PathBuf::new(), 1)]);
        let tree = repo.git_output(
            &["ls-tree", "-r", "-t", "-z", "--full-tree", &commit],
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
            let parent = *paths
                .get(parent_path)
                .context("tree entry missing parent")?;
            ensure!(
                nodes[(parent - 1) as usize].kind == Kind::Directory,
                "tree entry below non-directory"
            );
            ensure!(!paths.contains_key(&path), "duplicate tree path");
            let inode = nodes.len() as u64 + 1;
            let name = path
                .file_name()
                .context("tree path missing name")?
                .to_owned();
            paths.insert(path.clone(), inode);
            nodes.push(Node {
                inode,
                parent,
                path,
                name,
                mode,
                kind,
                oid: Some(fields[2].to_owned()),
                children: Vec::new(),
            });
            nodes[(parent - 1) as usize].children.push(inode);
        }
        Ok(Self {
            commit,
            nodes: Arc::new(nodes),
            paths: Arc::new(paths),
            timestamp,
        })
    }

    pub fn node(&self, inode: u64) -> Result<&Node> {
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
    pub fn size(&self, repo: &mut GitRepo, node: &Node) -> Result<u64> {
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
    pub fn read_node(&self, repo: &mut GitRepo, n: &Node) -> Result<Vec<u8>> {
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
}
