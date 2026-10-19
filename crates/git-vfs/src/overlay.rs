//! Persistent mutable namespace over a pinned Git tree.
use crate::backend::GitRepo;
#[path = "overlay_codec.rs"]
mod codec;
#[path = "overlay_fjall.rs"]
mod fjall;
#[path = "overlay_payload.rs"]
mod payload;
use crate::filesystem::{Kind as BaseKind, Snapshot};
use anyhow::{Context, Result, ensure};
use fuser::FileType;
pub use payload::Payload;
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    File,
    Directory,
    Symlink,
    Gitlink,
}
impl Kind {
    pub fn is_dir(self) -> bool {
        matches!(self, Self::Directory | Self::Gitlink)
    }
    pub fn file_type(self) -> FileType {
        match self {
            Self::File => FileType::RegularFile,
            Self::Symlink => FileType::Symlink,
            _ => FileType::Directory,
        }
    }
    fn code(self) -> i64 {
        match self {
            Self::File => 0,
            Self::Directory => 1,
            Self::Symlink => 2,
            Self::Gitlink => 3,
        }
    }
    fn decode(code: i64) -> Result<Self> {
        Ok(match code {
            0 => Self::File,
            1 => Self::Directory,
            2 => Self::Symlink,
            3 => Self::Gitlink,
            _ => anyhow::bail!("invalid overlay kind"),
        })
    }
}
#[derive(Clone, Debug)]
pub struct Entry {
    pub inode: u64,
    pub parent: u64,
    pub name: OsString,
    pub kind: Kind,
    pub mode: u32,
    pub base_oid: Option<String>,
    pub payload: Payload,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub linked: bool,
    pub control: bool,
}
pub struct Overlay {
    database: fjall::Store,
    entries: BTreeMap<u64, Entry>,
    namespace: HashMap<u64, DirectoryIndex>,
    // Only inodes already unlinked at exclusive open are reclaimed. Inodes
    // unlinked during this session retain their open-descriptor semantics.
    reclaimed: HashSet<u64>,
}

/// Logical overlay state; protected `.git` pointer bytes are not user payload.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct OverlayStats {
    pub entry_count: u64,
    pub linked_entries: u64,
    pub tombstone_entries: u64,
    pub live_payload_bytes: u64,
    pub dead_payload_bytes: u64,
    /// Payload bytes removed during this exclusive open, not lifetime total.
    pub reclaimed_payload_bytes: u64,
    pub payload_read_bytes: u64,
    pub payload_write_bytes: u64,
    pub payload_copy_bytes: u64,
    pub payload_hash_bytes: u64,
    pub payload_syncs: u64,
    pub payload_directory_syncs: u64,
    pub metadata_syncs: u64,
    pub recovered_operations: u64,
}

#[derive(Default)]
struct DirectoryIndex {
    by_name: HashMap<OsString, u64>,
    children: BTreeSet<u64>,
    directory_children: usize,
}

// Only namespace changes copy a name; file contents never enter the index.
struct Binding {
    inode: u64,
    parent: u64,
    name: OsString,
    directory: bool,
}
impl Binding {
    fn live(entry: &Entry) -> bool {
        entry.linked && entry.inode != 1
    }
    fn from_entry(entry: &Entry) -> Option<Self> {
        Self::live(entry).then(|| Self {
            inode: entry.inode,
            parent: entry.parent,
            name: entry.name.clone(),
            directory: entry.kind.is_dir(),
        })
    }
    fn unchanged(old: Option<&Entry>, new: &Entry) -> bool {
        match old {
            Some(old) if Self::live(old) && Self::live(new) => {
                old.parent == new.parent
                    && old.name == new.name
                    && old.kind.is_dir() == new.kind.is_dir()
            }
            _ => !old.is_some_and(Self::live) && !Self::live(new),
        }
    }
}
impl DirectoryIndex {
    fn insert(&mut self, binding: Binding) {
        let previous = self.by_name.insert(binding.name, binding.inode);
        debug_assert!(previous.is_none());
        let inserted = self.children.insert(binding.inode);
        debug_assert!(inserted);
        self.directory_children += usize::from(binding.directory);
    }
    fn remove(&mut self, binding: &Binding) {
        let previous = self.by_name.remove(&binding.name);
        debug_assert_eq!(previous, Some(binding.inode));
        let removed = self.children.remove(&binding.inode);
        debug_assert!(removed);
        self.directory_children -= usize::from(binding.directory);
    }
}
fn reject_legacy_overlay(directory: &Path) -> Result<()> {
    for (name, engine) in [("overlay.sqlite", "SQLite"), ("overlay.lmdb", "LMDB")] {
        match std::fs::symlink_metadata(directory.join(name)) {
            Ok(_) => anyhow::bail!(
                "legacy {engine} overlay ({name}) is not supported; automatic migration is not supported; preserve it and use a separate directory for a new Fjall overlay"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("check legacy overlay storage"),
        }
    }
    Ok(())
}
fn require_existing_overlay(directory: &Path) -> Result<()> {
    reject_legacy_overlay(directory)?;
    ensure!(
        directory.join("overlay.fjall").is_dir(),
        "overlay storage does not exist"
    );
    Ok(())
}

fn error(code: i32) -> anyhow::Error {
    std::io::Error::from_raw_os_error(code).into()
}
fn timestamp(t: SystemTime) -> String {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos().to_string(),
        Err(e) => format!("-{}", e.duration().as_nanos()),
    }
}
fn parse_time(s: String) -> Result<SystemTime> {
    let n: i128 = s.parse()?;
    let a = n.unsigned_abs();
    let d = Duration::new(
        u64::try_from(a / 1_000_000_000)?,
        (a % 1_000_000_000) as u32,
    );
    if n < 0 {
        UNIX_EPOCH.checked_sub(d)
    } else {
        UNIX_EPOCH.checked_add(d)
    }
    .context("overlay timestamp out of range")
}
impl Overlay {
    pub fn open(directory: &Path, snapshot: &Snapshot, gitdir: &Path) -> Result<Self> {
        reject_legacy_overlay(directory)?;
        std::fs::create_dir_all(directory)?;
        let (database, entries) = fjall::Store::open(directory, snapshot, gitdir)?;
        let mut namespace: HashMap<u64, DirectoryIndex> = HashMap::new();
        for entry in entries.values() {
            if let Some(binding) = Binding::from_entry(entry) {
                let index = namespace.entry(binding.parent).or_default();
                ensure!(
                    !index.by_name.contains_key(&binding.name),
                    "duplicate overlay namespace binding"
                );
                index.insert(binding);
            }
        }
        let reclaimed = entries
            .values()
            .filter(|e| !e.linked && !e.control)
            .map(|e| e.inode)
            .collect();
        Ok(Self {
            database,
            entries,
            namespace,
            reclaimed,
        })
    }
    pub fn stats(&self) -> OverlayStats {
        let mut stats = OverlayStats {
            entry_count: self.entries.len() as u64,
            reclaimed_payload_bytes: self.database.reclaimed_payload_bytes,
            payload_read_bytes: self.database.files.counters.read.get(),
            payload_write_bytes: self.database.files.counters.written.get(),
            payload_copy_bytes: self.database.files.counters.copied.get(),
            payload_hash_bytes: self.database.files.counters.hashed.get(),
            payload_syncs: self.database.files.counters.syncs.get(),
            payload_directory_syncs: self.database.files.counters.directory_syncs.get(),
            metadata_syncs: self.database.metadata_syncs.get(),
            recovered_operations: self.database.files.counters.recovered.get(),
            ..OverlayStats::default()
        };
        for entry in self.entries.values() {
            if entry.linked {
                stats.linked_entries += 1;
            } else {
                stats.tombstone_entries += 1;
            }
            if !entry.control {
                let bytes = entry.payload.size().unwrap_or(0);
                if entry.linked {
                    stats.live_payload_bytes += bytes;
                } else {
                    stats.dead_payload_bytes += bytes;
                }
            }
        }
        stats
    }
    /// Reject reclaimed inode content rather than resurrecting its base blob.
    pub fn content_entry(&self, ino: u64) -> Result<&Entry> {
        self.database.check_healthy()?;
        if self.reclaimed.contains(&ino) {
            return Err(error(libc::ENOENT));
        }
        self.entry(ino)
    }
    pub fn entry(&self, ino: u64) -> Result<&Entry> {
        self.entries.get(&ino).ok_or_else(|| error(libc::ENOENT))
    }
    pub fn lookup(&self, parent: u64, name: &OsStr) -> Result<u64> {
        let p = self.entry(parent)?;
        if !p.kind.is_dir() {
            return Err(error(libc::ENOTDIR));
        }
        if !p.linked {
            return Err(error(libc::ENOENT));
        }
        if name == OsStr::new(".") {
            return Ok(parent);
        }
        if name == OsStr::new("..") {
            return Ok(p.parent);
        }
        self.namespace
            .get(&parent)
            .and_then(|index| index.by_name.get(name))
            .copied()
            .ok_or_else(|| error(libc::ENOENT))
    }
    pub fn children(&self, ino: u64) -> Result<Vec<u64>> {
        if !self.entry(ino)?.kind.is_dir() {
            return Err(error(libc::ENOTDIR));
        }
        Ok(self
            .namespace
            .get(&ino)
            .map_or_else(Vec::new, |index| index.children.iter().copied().collect()))
    }
    pub fn directory_children_count(&self, ino: u64) -> Result<usize> {
        if !self.entry(ino)?.kind.is_dir() {
            return Err(error(libc::ENOTDIR));
        }
        Ok(self
            .namespace
            .get(&ino)
            .map_or(0, |index| index.directory_children))
    }
    pub fn size(&mut self, ino: u64, repo: &mut GitRepo) -> Result<u64> {
        let e = self.content_entry(ino)?;
        if e.kind.is_dir() {
            return Ok(0);
        }
        if let Some(size) = e.payload.size() {
            return Ok(size);
        }
        ensure!(matches!(e.payload, Payload::Base), "file has no content");
        let oid = e.base_oid.as_ref().context("base file has no identity")?;
        repo.blob_sizes(std::slice::from_ref(oid))?
            .get(oid)
            .copied()
            .context("missing blob size")
    }
    pub fn read_range(
        &mut self,
        ino: u64,
        offset: u64,
        length: usize,
        repo: &mut GitRepo,
    ) -> Result<Vec<u8>> {
        let e = self.content_entry(ino)?;
        if e.kind.is_dir() {
            return Err(error(libc::EISDIR));
        }
        match &e.payload {
            Payload::File { size, .. } => self.database.files.read(ino, *size, offset, length),
            Payload::Inline(data) => {
                let start = usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(data.len());
                Ok(data[start..start.saturating_add(length).min(data.len())].to_vec())
            }
            Payload::Base => {
                let data =
                    repo.read_blob(e.base_oid.as_deref().context("base file has no identity")?)?;
                let start = usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(data.len());
                Ok(data[start..start.saturating_add(length).min(data.len())].to_vec())
            }
            Payload::None => Err(error(libc::ENOENT)),
        }
    }
    pub fn read(&mut self, ino: u64, repo: &mut GitRepo) -> Result<Vec<u8>> {
        let size = usize::try_from(self.size(ino, repo)?).map_err(|_| error(libc::EFBIG))?;
        self.read_range(ino, 0, size, repo)
    }
    pub fn digest(&self, ino: u64) -> Result<Vec<u8>> {
        let e = self.content_entry(ino)?;
        if e.control || e.kind.is_dir() {
            return Err(error(libc::ENODATA));
        }
        if matches!(e.payload, Payload::Base) {
            return Ok(hex::decode(
                e.base_oid.as_deref().context("base file has no digest")?,
            )?);
        }
        let size = e.payload.size().context("file has no digest")?;
        let mut hash = Sha1::new();
        hash.update(format!("blob {size}\0").as_bytes());
        match &e.payload {
            Payload::Inline(data) => hash.update(data),
            Payload::File { .. } => self
                .database
                .files
                .hash(ino, size, |data| hash.update(data))?,
            _ => return Err(error(libc::ENODATA)),
        }
        Ok(hash.finalize().to_vec())
    }
    pub fn blake3_digest(&self, ino: u64) -> Result<[u8; 32]> {
        let entry = self.content_entry(ino)?;
        let mut hash = blake3::Hasher::new();
        match &entry.payload {
            Payload::Inline(data) => {
                hash.update(data);
            }
            Payload::File { size, .. } => self.database.files.hash(ino, *size, |data| {
                hash.update(data);
            })?,
            _ => return Err(error(libc::ENODATA)),
        }
        Ok(*hash.finalize().as_bytes())
    }
    fn validate_name(&self, parent: u64, name: &OsStr) -> Result<()> {
        let b = name.as_bytes();
        if b.is_empty() || b == b"." || b == b".." || b.contains(&b'/') || b.contains(&0) {
            return Err(error(libc::EINVAL));
        }
        if parent == 1 && b.eq_ignore_ascii_case(b".git") {
            return Err(error(libc::EPERM));
        }
        let p = self.entry(parent)?;
        if !p.kind.is_dir() {
            return Err(error(libc::ENOTDIR));
        }
        if !p.linked {
            return Err(error(libc::ENOENT));
        }
        Ok(())
    }
    fn path(&self, ino: u64) -> Result<Vec<u8>> {
        let mut names = Vec::new();
        let mut i = ino;
        let mut seen = BTreeSet::new();
        while i != 1 {
            ensure!(seen.insert(i), "overlay parent cycle");
            let e = self.entry(i)?;
            names.push(e.name.as_bytes());
            i = e.parent;
        }
        let mut path = Vec::new();
        for name in names.into_iter().rev() {
            if !path.is_empty() {
                path.push(b'/');
            }
            path.extend_from_slice(name);
        }
        Ok(path)
    }
    fn journal_path(&self, ino: u64) -> Result<Vec<u8>> {
        let mut p = self.path(ino)?;
        if self.entry(ino)?.kind.is_dir() {
            p.push(b'/');
        }
        Ok(p)
    }
    fn commit(&mut self, changed: Vec<Entry>, paths: Vec<Vec<u8>>) -> Result<()> {
        self.database.check_healthy()?;
        let mut post_entries = HashMap::with_capacity(changed.len());
        let mut deltas = Vec::new();
        for entry in &changed {
            ensure!(
                post_entries.insert(entry.inode, entry).is_none(),
                "duplicate changed overlay inode"
            );
            let old = self.entries.get(&entry.inode);
            if !Binding::unchanged(old, entry) {
                deltas.push((
                    old.and_then(Binding::from_entry),
                    Binding::from_entry(entry),
                ));
            }
        }

        // Validate the final namespace before its durable transaction. A rename
        // may replace another inode, so additions can reuse any removed binding.
        let mut removed = HashSet::with_capacity(deltas.len());
        let mut directory_changes: HashMap<u64, (usize, usize)> = HashMap::new();
        for (old, _) in &deltas {
            if let Some(old) = old {
                let index = self
                    .namespace
                    .get(&old.parent)
                    .context("missing overlay namespace parent")?;
                ensure!(
                    index.by_name.get(&old.name) == Some(&old.inode)
                        && index.children.contains(&old.inode),
                    "inconsistent overlay namespace binding"
                );
                removed.insert(old.inode);
                directory_changes.entry(old.parent).or_default().0 += usize::from(old.directory);
            }
        }
        let mut added = HashSet::with_capacity(deltas.len());
        for (_, new) in &deltas {
            if let Some(new) = new {
                let parent = post_entries
                    .get(&new.parent)
                    .copied()
                    .or_else(|| self.entries.get(&new.parent))
                    .context("missing overlay namespace parent")?;
                ensure!(
                    parent.linked && parent.kind.is_dir(),
                    "invalid overlay namespace parent"
                );
                ensure!(
                    added.insert((new.parent, new.name.as_os_str())),
                    "duplicate overlay namespace addition"
                );
                if let Some(existing) = self
                    .namespace
                    .get(&new.parent)
                    .and_then(|index| index.by_name.get(&new.name))
                {
                    ensure!(
                        removed.contains(existing),
                        "occupied overlay namespace binding"
                    );
                }
                directory_changes.entry(new.parent).or_default().1 += usize::from(new.directory);
            }
        }
        for (parent, (removed, added)) in directory_changes {
            self.namespace
                .get(&parent)
                .map_or(0, |index| index.directory_children)
                .checked_sub(removed)
                .and_then(|count| count.checked_add(added))
                .context("overlay directory child count overflow")?;
        }

        self.database.commit(&changed, &paths)?;
        // Remove every old binding before inserting any new one: otherwise an
        // overwritten rename destination could remove the source's new binding.
        for (old, _) in &deltas {
            if let Some(old) = old {
                let index = self.namespace.get_mut(&old.parent).unwrap();
                index.remove(old);
                if index.children.is_empty() {
                    self.namespace.remove(&old.parent);
                }
            }
        }
        for (_, new) in deltas {
            if let Some(new) = new {
                self.namespace.entry(new.parent).or_default().insert(new);
            }
        }
        for entry in changed {
            self.entries.insert(entry.inode, entry);
        }
        Ok(())
    }
    pub fn create(
        &mut self,
        parent: u64,
        name: &OsStr,
        kind: Kind,
        mode: u32,
        data: Option<Vec<u8>>,
    ) -> Result<u64> {
        self.database.check_healthy()?;
        self.validate_name(parent, name)?;
        match self.lookup(parent, name) {
            Ok(_) => return Err(error(libc::EEXIST)),
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .and_then(|e| e.raw_os_error())
                    == Some(libc::ENOENT) => {}
            Err(e) => return Err(e),
        };
        let ino = self
            .entries
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| error(libc::EFBIG))?;
        let now = SystemTime::now();
        let e = Entry {
            inode: ino,
            parent,
            name: name.to_owned(),
            kind,
            mode: mode & 0o7777,
            base_oid: None,
            payload: if kind.is_dir() {
                Payload::None
            } else if kind == Kind::File {
                let data = data.unwrap_or_default();
                if data.is_empty() {
                    Payload::Inline(data)
                } else {
                    self.database.files.install(ino, &data)?;
                    Payload::File {
                        size: data.len() as u64,
                        generation: 0,
                    }
                }
            } else {
                Payload::Inline(data.unwrap_or_default())
            },
            mtime: now,
            ctime: now,
            linked: true,
            control: false,
        };
        let mut path = self.path(parent)?;
        if !path.is_empty() {
            path.push(b'/');
        }
        path.extend_from_slice(name.as_bytes());
        if kind.is_dir() {
            path.push(b'/');
        }
        let mut p = self.entry(parent)?.clone();
        p.mtime = now;
        p.ctime = now;
        self.commit(vec![e, p], vec![path])?;
        Ok(ino)
    }
    fn change_payload(
        &mut self,
        ino: u64,
        after: &Entry,
        paths: &[Vec<u8>],
        write: Option<(u64, &[u8])>,
        repo: &mut GitRepo,
    ) -> Result<()> {
        if matches!(self.entry(ino)?.payload, Payload::File { .. }) {
            return self.database.mutate(self.entry(ino)?, after, paths, write);
        }
        let Payload::File { size, .. } = after.payload else {
            anyhow::bail!("copy-up target missing")
        };
        // Only first copy-up uses the old authoritative Base/Inline contents.
        // Its unpublished backing can be rebuilt on retry without a redo intent.
        let old = if size == 0 {
            Vec::new()
        } else {
            self.read(ino, repo)?
        };
        self.database
            .copy_up(self.entry(ino)?, after, paths, &old, write)
    }
    pub fn write(
        &mut self,
        ino: u64,
        offset: u64,
        data: &[u8],
        repo: &mut GitRepo,
    ) -> Result<usize> {
        let mut e = self.content_entry(ino)?.clone();
        if e.control {
            return Err(error(libc::EPERM));
        }
        if e.kind.is_dir() {
            return Err(error(libc::EISDIR));
        }
        if data.is_empty() {
            return Ok(0);
        }
        let end = offset
            .checked_add(data.len() as u64)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| error(libc::EFBIG))?;
        let size = self.size(ino, repo)?.max(end);
        let previous_generation = match e.payload {
            Payload::File { generation, .. } => generation,
            _ => 0,
        };
        let generation = previous_generation
            .checked_add(1)
            .context("content generation overflow")?;
        e.payload = Payload::File { size, generation };
        e.mtime = SystemTime::now();
        e.ctime = e.mtime;
        let paths = if e.linked {
            vec![self.journal_path(ino)?]
        } else {
            vec![]
        };
        self.change_payload(ino, &e, &paths, Some((offset, data)), repo)?;
        self.entries.insert(ino, e);
        Ok(data.len())
    }
    pub fn setattr(
        &mut self,
        ino: u64,
        mode: Option<u32>,
        size: Option<u64>,
        mtime: Option<SystemTime>,
        repo: &mut GitRepo,
    ) -> Result<()> {
        let mut e = self.content_entry(ino)?.clone();
        if e.control {
            return Err(error(libc::EPERM));
        }
        if let Some(size) = size {
            if e.kind.is_dir() {
                return Err(error(libc::EISDIR));
            }
            if size > i64::MAX as u64 {
                return Err(error(libc::EFBIG));
            }
            let previous_generation = match e.payload {
                Payload::File { generation, .. } => generation,
                _ => 0,
            };
            let generation = previous_generation
                .checked_add(1)
                .context("content generation overflow")?;
            e.payload = Payload::File { size, generation };
            e.mtime = SystemTime::now();
        }
        if let Some(mode) = mode {
            e.mode = mode & 0o7777;
        }
        if let Some(mtime) = mtime {
            e.mtime = mtime;
        }
        e.ctime = SystemTime::now();
        let paths = if e.linked {
            vec![self.journal_path(ino)?]
        } else {
            vec![]
        };
        if size.is_some() {
            self.change_payload(ino, &e, &paths, None, repo)?;
            self.entries.insert(ino, e);
            Ok(())
        } else {
            self.commit(vec![e], paths)
        }
    }
    pub fn unlink(&mut self, parent: u64, name: &OsStr, directory: bool) -> Result<()> {
        self.validate_name(parent, name)?;
        let ino = self.lookup(parent, name)?;
        let mut e = self.entry(ino)?.clone();
        if e.kind.is_dir() != directory {
            return Err(error(if directory {
                libc::ENOTDIR
            } else {
                libc::EISDIR
            }));
        }
        if directory && !self.children(ino)?.is_empty() {
            return Err(error(libc::ENOTEMPTY));
        }
        let path = self.journal_path(ino)?;
        e.linked = false;
        e.ctime = SystemTime::now();
        let mut p = self.entry(parent)?.clone();
        p.mtime = e.ctime;
        p.ctime = e.ctime;
        self.commit(vec![e, p], vec![path])
    }
    pub fn rename(
        &mut self,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
    ) -> Result<()> {
        if flags & !libc::RENAME_NOREPLACE != 0 {
            return Err(error(libc::EINVAL));
        }
        self.validate_name(parent, name)?;
        self.validate_name(newparent, newname)?;
        let ino = self.lookup(parent, name)?;
        let mut source = self.entry(ino)?.clone();
        let destination = self.lookup(newparent, newname);
        let dest = match destination {
            Ok(i) => Some(i),
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .and_then(|e| e.raw_os_error())
                    == Some(libc::ENOENT) =>
            {
                None
            }
            Err(e) => return Err(e),
        };
        if dest.is_some() && flags != 0 {
            return Err(error(libc::EEXIST));
        }
        if dest == Some(ino) {
            return Ok(());
        }
        if source.kind.is_dir() {
            let mut i = newparent;
            while i != 1 {
                if i == ino {
                    return Err(error(libc::EINVAL));
                }
                i = self.entry(i)?.parent;
            }
        }
        let mut changed = Vec::new();
        if let Some(d) = dest {
            let mut e = self.entry(d)?.clone();
            if e.kind.is_dir() != source.kind.is_dir() {
                return Err(error(if source.kind.is_dir() {
                    libc::ENOTDIR
                } else {
                    libc::EISDIR
                }));
            }
            if e.kind.is_dir() && !self.children(d)?.is_empty() {
                return Err(error(libc::ENOTEMPTY));
            }
            e.linked = false;
            e.ctime = SystemTime::now();
            changed.push(e);
        }
        let old = self.path(ino)?;
        let mut new = self.path(newparent)?;
        if !new.is_empty() {
            new.push(b'/');
        }
        new.extend_from_slice(newname.as_bytes());
        let mut paths = Vec::new();
        for e in self
            .entries
            .values()
            .filter(|e| e.linked && e.inode != 1 && !e.control)
        {
            let path = self.path(e.inode)?;
            if path == old
                || (source.kind.is_dir() && path.starts_with(&[old.as_slice(), b"/"].concat()))
            {
                let mut replacement = new.clone();
                replacement.extend_from_slice(&path[old.len()..]);
                let mut path = path;
                if e.kind.is_dir() {
                    path.push(b'/');
                    replacement.push(b'/');
                }
                paths.push(path);
                paths.push(replacement);
            }
        }
        let now = SystemTime::now();
        source.parent = newparent;
        source.name = newname.to_owned();
        source.ctime = now;
        changed.push(source);
        for p in [parent, newparent].into_iter().collect::<BTreeSet<_>>() {
            let mut e = self.entry(p)?.clone();
            e.mtime = now;
            e.ctime = now;
            changed.push(e);
        }
        self.commit(changed, paths)
    }
    pub fn sync(&mut self) -> Result<()> {
        self.database.sync()
    }
}
/// Return one consistent journal snapshot; an unknown token invalidates the whole tree.
pub fn changes_since(directory: &Path, token: &str) -> Result<(String, Vec<Vec<u8>>)> {
    require_existing_overlay(directory)?;
    fjall::changes_since(directory, token)
}
pub fn dirty_paths(directory: &Path) -> Result<Vec<Vec<u8>>> {
    require_existing_overlay(directory)?;
    fjall::dirty_paths(directory)
}
