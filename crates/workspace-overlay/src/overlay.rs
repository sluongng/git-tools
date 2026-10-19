//! Persistent mutable namespace over a pinned Git tree.
use crate::backend::GitRepo;
#[path = "overlay_codec.rs"]
mod codec;
#[path = "overlay_fjall.rs"]
mod fjall;
#[path = "overlay_lifetime.rs"]
mod lifetime;
#[path = "overlay_payload.rs"]
mod payload;
use crate::filesystem::{Kind as BaseKind, Snapshot};
use anyhow::{Context, Result, ensure};
pub use lifetime::{Lease, PinKind};
pub use payload::Payload;
use sha1::{Digest, Sha1};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::ops::Deref;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
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
    pub fn is_dir(self) -> bool {
        matches!(self, Self::Directory | Self::Gitlink)
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
/// Owned view of decoded metadata. Eviction never invalidates a caller's view.
#[derive(Debug)]
pub struct EntryGuard(Arc<Entry>);
impl Deref for EntryGuard {
    type Target = Entry;
    fn deref(&self) -> &Entry {
        &self.0
    }
}

#[derive(Clone, Copy)]
struct EntrySummary {
    linked: bool,
    control: bool,
    payload_bytes: u64,
}
impl From<&Entry> for EntrySummary {
    fn from(e: &Entry) -> Self {
        Self {
            linked: e.linked,
            control: e.control,
            payload_bytes: e.payload.size().unwrap_or(0),
        }
    }
}

/// Limits apply to cached decoded metadata, not namespace indexes, the Git tree,
/// Fjall pages, or temporary caller-held guards. Zero disables residency.
#[derive(Clone, Copy, Debug)]
pub struct MetadataCacheConfig {
    pub entries: usize,
    pub bytes: usize,
}
impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            entries: 4096,
            bytes: 4 * 1024 * 1024,
        }
    }
}
struct MetadataCache {
    config: MetadataCacheConfig,
    entries: HashMap<u64, (Arc<Entry>, u64, usize)>,
    recency: BTreeMap<u64, u64>,
    clock: u64,
    bytes: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
}
impl MetadataCache {
    fn new(config: MetadataCacheConfig) -> Self {
        Self {
            config,
            entries: HashMap::new(),
            recency: BTreeMap::new(),
            clock: 0,
            bytes: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }
    fn tick(&mut self) -> u64 {
        // Renormalization also keeps pathological long-lived sessions correct.
        if self.clock == u64::MAX {
            let ordered: Vec<_> = self.recency.values().copied().collect();
            self.recency.clear();
            for (index, inode) in ordered.into_iter().enumerate() {
                let tick = index as u64;
                self.entries.get_mut(&inode).unwrap().1 = tick;
                self.recency.insert(tick, inode);
            }
            self.clock = self.entries.len() as u64;
        }
        self.clock += 1;
        self.clock
    }
    fn get(&mut self, inode: u64) -> Option<Arc<Entry>> {
        if !self.entries.contains_key(&inode) {
            self.misses += 1;
            return None;
        }
        self.hits += 1;
        let tick = self.tick();
        let (entry, previous, _) = self.entries.get_mut(&inode).unwrap();
        self.recency.remove(previous);
        *previous = tick;
        self.recency.insert(tick, inode);
        Some(entry.clone())
    }
    fn remove(&mut self, inode: u64) {
        if let Some((_, tick, bytes)) = self.entries.remove(&inode) {
            self.recency.remove(&tick);
            self.bytes -= bytes;
        }
    }
    fn insert(&mut self, entry: Arc<Entry>) {
        self.remove(entry.inode);
        let bytes = std::mem::size_of::<Entry>()
            + entry.name.as_bytes().len()
            + entry.base_oid.as_ref().map_or(0, String::len)
            + match &entry.payload {
                Payload::Inline(bytes) => bytes.len(),
                _ => 0,
            };
        if self.config.entries == 0 || bytes > self.config.bytes {
            return;
        }
        while self.entries.len() >= self.config.entries || self.bytes > self.config.bytes - bytes {
            let (_, inode) = self.recency.first_key_value().unwrap();
            let inode = *inode;
            self.remove(inode);
            self.evictions += 1;
        }
        let tick = self.tick();
        self.bytes += bytes;
        self.recency.insert(tick, entry.inode);
        self.entries.insert(entry.inode, (entry, tick, bytes));
    }
}
pub struct Overlay {
    database: fjall::Store,
    entries: BTreeMap<u64, EntrySummary>,
    metadata: RefCell<MetadataCache>,
    namespace: HashMap<u64, DirectoryIndex>,
    // Only inodes already unlinked at exclusive open are reclaimed. Inodes
    // unlinked during this session retain their open-descriptor semantics.
    reclaimed: HashSet<u64>,
    base_inode_highwater: u64,
    dependencies: HashMap<u64, usize>,
    lifetime: lifetime::Lifetime,
    online_reclaimed_payload_bytes: u64,
    reclaimed_entries: u64,
    unlinked_inodes: BTreeSet<u64>,
    gc_ready: BTreeSet<u64>,
    gc_visited_entries: u64,
    rename_visited_entries: u64,
    digests: RefCell<HashMap<u64, CachedDigests>>,
    digest_hash_operations: Cell<u64>,
    digest_hash_bytes: Cell<u64>,
    digest_cache_hits: Cell<u64>,
}

#[derive(Clone, Copy)]
struct CachedDigests {
    generation: u64,
    size: u64,
    sha1: [u8; 20],
    blake3: [u8; 32],
}

/// Logical overlay state; protected `.git` pointer bytes are not user payload.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct OverlayStats {
    pub entry_count: u64,
    pub metadata_resident_entries: u64,
    pub metadata_cache_max_entries: u64,
    pub metadata_cache_max_estimated_bytes: u64,
    pub metadata_resident_estimated_bytes: u64,
    pub metadata_cache_hits: u64,
    pub metadata_cache_misses: u64,
    pub metadata_cache_evictions: u64,
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
    pub online_reclaimed_payload_bytes: u64,
    pub reclaimed_entries: u64,
    pub gc_visited_entries: u64,
    pub rename_visited_entries: u64,
    pub digest_hash_operations: u64,
    pub digest_hash_bytes: u64,
    pub digest_cache_hits: u64,
    pub digest_cache_entries: u64,
    pub retirement_pending_items: u64,
    pub retirement_pending_bytes: u64,
    pub retirement_failures: u64,
    pub retirement_sync_fallbacks: u64,
    pub lookup_references: u64,
    pub open_references: u64,
    pub pinned_references: u64,
    pub lookup_accounting_errors: u64,
    #[serde(flatten)]
    pub journal: Option<serde_json::Value>,
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
        let config = MetadataCacheConfig {
            entries: std::env::var("GIT_VFS_METADATA_CACHE_ENTRIES")
                .ok()
                .map(|v| v.parse())
                .transpose()
                .context("invalid metadata cache entry limit")?
                .unwrap_or(MetadataCacheConfig::default().entries),
            bytes: std::env::var("GIT_VFS_METADATA_CACHE_BYTES")
                .ok()
                .map(|v| v.parse())
                .transpose()
                .context("invalid metadata cache byte limit")?
                .unwrap_or(MetadataCacheConfig::default().bytes),
        };
        Self::open_with_metadata_cache(directory, snapshot, gitdir, config)
    }
    pub fn open_with_metadata_cache(
        directory: &Path,
        snapshot: &Snapshot,
        gitdir: &Path,
        config: MetadataCacheConfig,
    ) -> Result<Self> {
        reject_legacy_overlay(directory)?;
        std::fs::create_dir_all(directory)?;
        let base_inode_highwater = snapshot.nodes().len() as u64;
        ensure!(
            snapshot
                .nodes()
                .iter()
                .enumerate()
                .all(|(i, n)| n.inode == i as u64 + 1),
            "base snapshot inode allocation must be contiguous"
        );
        let mut entries = BTreeMap::new();
        let mut namespace: HashMap<u64, DirectoryIndex> = HashMap::new();
        let lifetime = lifetime::Lifetime::default();
        let mut dependencies = HashMap::new();
        let mut unlinked_inodes = BTreeSet::new();
        let database = fjall::Store::open(directory, snapshot, gitdir, |entry| {
            if let Some(binding) = Binding::from_entry(entry) {
                let index = namespace.entry(binding.parent).or_default();
                ensure!(
                    !index.by_name.contains_key(&binding.name),
                    "duplicate overlay namespace binding"
                );
                index.insert(binding);
            }
            if entry.inode != 1 {
                *dependencies.entry(entry.parent).or_insert(0) += 1;
            }
            if entry.control || entry.inode == 1 {
                lifetime.protect(entry.inode);
            }
            if entry.control {
                ensure!(
                    entry.inode > base_inode_highwater,
                    "control overlaps base inode range"
                );
            }
            if !entry.linked && !entry.control {
                unlinked_inodes.insert(entry.inode);
            }
            entries.insert(entry.inode, EntrySummary::from(entry));
            Ok(())
        })?;
        let reclaimed = unlinked_inodes.iter().copied().collect();
        let gc_ready = unlinked_inodes.clone();
        let mut overlay = Self {
            database,
            entries,
            namespace,
            reclaimed,
            base_inode_highwater,
            metadata: RefCell::new(MetadataCache::new(config)),
            dependencies,
            lifetime,
            online_reclaimed_payload_bytes: 0,
            reclaimed_entries: 0,
            unlinked_inodes,
            gc_ready,
            gc_visited_entries: 0,
            rename_visited_entries: 0,
            digests: RefCell::default(),
            digest_hash_operations: Cell::new(0),
            digest_hash_bytes: Cell::new(0),
            digest_cache_hits: Cell::new(0),
        };
        overlay.reclaim_unreferenced()?;
        overlay.drain_maintenance()?;
        // Startup reclamation belongs to the existing remount counter, not the
        // online work attributed to this newly served session.
        overlay.online_reclaimed_payload_bytes = 0;
        overlay.reclaimed_entries = 0;
        Ok(overlay)
    }
    pub fn record_lookup(&self, inode: u64) -> Result<()> {
        self.entry(inode)?;
        self.lifetime.lookup(inode)
    }
    pub fn forget(&mut self, inode: u64, nlookup: u64) {
        self.lifetime.forget(inode, nlookup);
    }
    pub fn pin(&self, inodes: Vec<u64>, kind: PinKind) -> Result<Lease> {
        for inode in &inodes {
            self.entry(*inode)?;
        }
        self.lifetime.pin(inodes, kind)
    }
    pub fn release_kernel_references(&self) {
        self.lifetime.release_kernel();
    }
    pub fn drain_maintenance(&mut self) -> Result<()> {
        self.database.drain_maintenance()
    }
    /// Explicit safe collection. Direct callers retain unlinked content until
    /// requesting this maintenance operation; mounted callers supply real pins.
    pub fn reclaim_unreferenced(&mut self) -> Result<()> {
        self.database.check_healthy()?;
        for inode in self.lifetime.take_notifications() {
            if self.unlinked_inodes.contains(&inode) {
                self.gc_ready.insert(inode);
            }
        }
        while let Some(inode) = self.gc_ready.pop_first() {
            self.gc_visited_entries += 1;
            if !self.unlinked_inodes.contains(&inode) || !self.lifetime.eligible(inode) {
                continue;
            }
            let entry = self.entry(inode)?.clone();
            let remove = inode > self.base_inode_highwater
                && self.dependencies.get(&inode).copied().unwrap_or(0) == 0;
            if !remove && matches!(entry.payload, Payload::None) {
                continue;
            }
            let bytes = entry.payload.size().unwrap_or(0);
            self.database.retire(&entry, remove)?;
            self.online_reclaimed_payload_bytes += bytes;
            self.digests.borrow_mut().remove(&inode);
            if remove {
                self.reclaimed.remove(&inode);
                self.unlinked_inodes.remove(&inode);
                self.entries.remove(&inode);
                self.metadata.borrow_mut().remove(inode);
                self.reclaimed_entries += 1;
                let count = self
                    .dependencies
                    .get_mut(&entry.parent)
                    .context("missing retained-parent dependency")?;
                *count = count
                    .checked_sub(1)
                    .context("retained-parent dependency underflow")?;
                if *count == 0 {
                    self.dependencies.remove(&entry.parent);
                }
                if self.unlinked_inodes.contains(&entry.parent) {
                    self.gc_ready.insert(entry.parent);
                }
            } else {
                self.reclaimed.insert(inode);
                let mut entry = entry;
                entry.payload = Payload::None;
                self.publish_entry(entry);
            }
        }
        Ok(())
    }

    pub fn stats(&self) -> OverlayStats {
        let (pending_items, pending_bytes, _, fallbacks, failed) = self.database.retirement_stats();
        let (lookups, opens, pins, errors) = self.lifetime.stats();
        let metadata = self.metadata.borrow();
        let mut stats = OverlayStats {
            metadata_resident_entries: metadata.entries.len() as u64,
            metadata_cache_max_entries: metadata.config.entries as u64,
            metadata_cache_max_estimated_bytes: metadata.config.bytes as u64,
            metadata_resident_estimated_bytes: metadata.bytes as u64,
            metadata_cache_hits: metadata.hits,
            metadata_cache_misses: metadata.misses,
            metadata_cache_evictions: metadata.evictions,
            online_reclaimed_payload_bytes: self.online_reclaimed_payload_bytes,
            reclaimed_entries: self.reclaimed_entries,
            gc_visited_entries: self.gc_visited_entries,
            rename_visited_entries: self.rename_visited_entries,
            digest_hash_operations: self.digest_hash_operations.get(),
            digest_hash_bytes: self.digest_hash_bytes.get(),
            digest_cache_hits: self.digest_cache_hits.get(),
            digest_cache_entries: self.digests.borrow().len() as u64,
            retirement_pending_items: pending_items,
            retirement_pending_bytes: pending_bytes,
            retirement_failures: failed,
            retirement_sync_fallbacks: fallbacks,
            lookup_references: lookups,
            open_references: opens,
            pinned_references: pins,
            lookup_accounting_errors: errors,
            journal: self
                .database
                .journal_stats()
                .ok()
                .and_then(|stats| serde_json::to_value(stats).ok()),
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
                let bytes = entry.payload_bytes;
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
    pub fn content_entry(&self, ino: u64) -> Result<EntryGuard> {
        self.database.check_healthy()?;
        if self.reclaimed.contains(&ino) {
            return Err(error(libc::ENOENT));
        }
        self.entry(ino)
    }
    pub fn entry(&self, ino: u64) -> Result<EntryGuard> {
        self.database.check_healthy()?;
        if !self.entries.contains_key(&ino) {
            return Err(error(libc::ENOENT));
        }
        if let Some(entry) = self.metadata.borrow_mut().get(ino) {
            return Ok(EntryGuard(entry));
        }
        let entry = Arc::new(
            self.database
                .read_entry(ino)?
                .context("overlay metadata record missing")?,
        );
        ensure!(entry.inode == ino, "overlay entry key mismatch");
        self.metadata.borrow_mut().insert(entry.clone());
        Ok(EntryGuard(entry))
    }
    fn publish_entry(&mut self, entry: Entry) {
        self.entries.insert(entry.inode, EntrySummary::from(&entry));
        self.metadata.borrow_mut().insert(Arc::new(entry));
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
    /// Validate digest availability without reading or hashing mutable contents.
    pub fn digest_available(&self, ino: u64) -> Result<()> {
        let entry = self.content_entry(ino)?;
        if entry.control || entry.kind.is_dir() {
            return Err(error(libc::ENODATA));
        }
        match &entry.payload {
            Payload::Base => {
                let oid = entry
                    .base_oid
                    .as_ref()
                    .ok_or_else(|| error(libc::ENODATA))?;
                ensure!(
                    hex::decode(oid)?.len() == 20,
                    "invalid base digest identity"
                );
                Ok(())
            }
            Payload::Inline(_) | Payload::File { .. } => Ok(()),
            Payload::None => Err(error(libc::ENODATA)),
        }
    }
    fn local_digests(&self, ino: u64) -> Result<(CachedDigests, bool)> {
        self.digest_available(ino)?;
        let entry = self.content_entry(ino)?;
        let (generation, size) = match &entry.payload {
            Payload::Inline(data) => (0, data.len() as u64),
            Payload::File { generation, size } => (*generation, *size),
            _ => return Err(error(libc::ENODATA)),
        };
        if let Some(cached) = self
            .digests
            .borrow()
            .get(&ino)
            .copied()
            .filter(|cached| cached.generation == generation && cached.size == size)
        {
            self.digest_cache_hits.set(self.digest_cache_hits.get() + 1);
            return Ok((cached, false));
        }
        let mut sha1 = Sha1::new();
        sha1.update(format!("blob {size}\0").as_bytes());
        let mut blake3 = blake3::Hasher::new();
        let mut consume = |bytes: &[u8]| {
            sha1.update(bytes);
            blake3.update(bytes);
        };
        match &entry.payload {
            Payload::Inline(data) => consume(data),
            Payload::File { .. } => self.database.files.hash(ino, size, consume)?,
            _ => return Err(error(libc::ENODATA)),
        }
        let cached = CachedDigests {
            generation,
            size,
            sha1: sha1.finalize().into(),
            blake3: *blake3.finalize().as_bytes(),
        };
        self.digests.borrow_mut().insert(ino, cached);
        self.digest_hash_operations
            .set(self.digest_hash_operations.get() + 1);
        self.digest_hash_bytes
            .set(self.digest_hash_bytes.get() + size);
        Ok((cached, true))
    }
    pub fn digest(&self, ino: u64) -> Result<Vec<u8>> {
        self.digest_available(ino)?;
        let entry = self.content_entry(ino)?;
        if matches!(entry.payload, Payload::Base) {
            return Ok(hex::decode(
                entry
                    .base_oid
                    .as_deref()
                    .context("base file has no digest")?,
            )?);
        }
        Ok(self.local_digests(ino)?.0.sha1.to_vec())
    }
    pub fn blake3_digest(&self, ino: u64) -> Result<[u8; 32]> {
        Ok(self.blake3_digest_with_status(ino)?.0)
    }
    /// Returns the local digest and whether this request computed the pair.
    pub fn blake3_digest_with_status(&self, ino: u64) -> Result<([u8; 32], bool)> {
        let (cached, computed) = self.local_digests(ino)?;
        Ok((cached.blake3, computed))
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
            names.push(e.name.as_bytes().to_vec());
            i = e.parent;
        }
        let mut path = Vec::new();
        for name in names.into_iter().rev() {
            if !path.is_empty() {
                path.push(b'/');
            }
            path.extend_from_slice(&name);
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
        let mut previous_entries = HashMap::new();
        for entry in &changed {
            ensure!(
                post_entries.insert(entry.inode, entry).is_none(),
                "duplicate changed overlay inode"
            );
            let old = self.database.read_entry(entry.inode)?;
            ensure!(
                old.is_some() == self.entries.contains_key(&entry.inode),
                "overlay identity summary mismatch"
            );
            if !Binding::unchanged(old.as_ref(), entry) {
                deltas.push((
                    old.as_ref().and_then(Binding::from_entry),
                    Binding::from_entry(entry),
                ));
            }
            previous_entries.insert(entry.inode, old);
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
                let parent_guard;
                let parent = if let Some(parent) = post_entries.get(&new.parent) {
                    *parent
                } else {
                    parent_guard = self.entry(new.parent)?;
                    &parent_guard
                };
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
            let previous = previous_entries.remove(&entry.inode).flatten();
            if !entry.linked && !entry.control && previous.as_ref().is_none_or(|old| old.linked) {
                self.unlinked_inodes.insert(entry.inode);
                self.gc_ready.insert(entry.inode);
            }
            if previous.is_none() && entry.inode != 1 {
                *self.dependencies.entry(entry.parent).or_insert(0) += 1;
            } else if let Some(previous) = previous.as_ref()
                && previous.parent != entry.parent
            {
                let count = self.dependencies.get_mut(&previous.parent).unwrap();
                *count -= 1;
                if *count == 0 {
                    self.dependencies.remove(&previous.parent);
                }
                *self.dependencies.entry(entry.parent).or_insert(0) += 1;
            }
            self.publish_entry(entry);
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
            .database
            .highwater
            .get()
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
            return self
                .database
                .mutate(&*self.entry(ino)?, after, paths, write);
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
            .copy_up(&*self.entry(ino)?, after, paths, &old, write)
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
        self.digests.borrow_mut().remove(&ino);
        self.publish_entry(e);
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
            self.digests.borrow_mut().remove(&ino);
            self.publish_entry(e);
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
    fn rename_paths(
        &self,
        inode: u64,
        newparent: u64,
        newname: &OsStr,
    ) -> Result<(Vec<Vec<u8>>, u64)> {
        fn add(
            paths: &mut Vec<Vec<u8>>,
            bytes: &mut usize,
            old: &[u8],
            new: &[u8],
            directory: bool,
        ) -> bool {
            let suffix = usize::from(directory);
            let added = old
                .len()
                .saturating_add(new.len())
                .saturating_add(2 * suffix);
            if paths.len() + 2 > 4096 || bytes.saturating_add(added) > 1024 * 1024 {
                paths.clear();
                paths.push(b"/".to_vec());
                return false;
            }
            let mut old = old.to_vec();
            let mut new = new.to_vec();
            if directory {
                old.push(b'/');
                new.push(b'/');
            }
            *bytes += added;
            paths.push(old);
            paths.push(new);
            true
        }
        let mut old = self.path(inode)?;
        let mut new = self.path(newparent)?;
        if !new.is_empty() {
            new.push(b'/');
        }
        new.extend_from_slice(newname.as_bytes());
        let mut paths = Vec::new();
        let mut bytes = 0;
        let mut visited = 1;
        let directory = self.entry(inode)?.kind.is_dir();
        if !add(&mut paths, &mut bytes, &old, &new, directory) || !directory {
            return Ok((paths, visited));
        }
        let empty = BTreeSet::new();
        // Iterator frames keep traversal memory proportional to depth. Do not
        // queue every child/path before the bounded full-invalidation fallback.
        let children = self
            .namespace
            .get(&inode)
            .map_or(&empty, |index| &index.children);
        let mut stack = vec![(children.iter(), old.len(), new.len())];
        while let Some((children, old_len, new_len)) = stack.last_mut() {
            let Some(child) = children.next().copied() else {
                stack.pop();
                continue;
            };
            old.truncate(*old_len);
            new.truncate(*new_len);
            let entry = self.entry(child)?;
            old.push(b'/');
            old.extend_from_slice(entry.name.as_bytes());
            new.push(b'/');
            new.extend_from_slice(entry.name.as_bytes());
            visited += 1;
            let directory = entry.kind.is_dir();
            if !add(&mut paths, &mut bytes, &old, &new, directory) {
                break;
            }
            if directory {
                let children = self
                    .namespace
                    .get(&child)
                    .map_or(&empty, |index| &index.children);
                stack.push((children.iter(), old.len(), new.len()));
            }
        }
        Ok((paths, visited))
    }
    pub fn rename(
        &mut self,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
    ) -> Result<()> {
        // The overlay API uses Linux/FUSE rename flag values on every Unix host.
        if flags & !1_u32 != 0 {
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
        let (paths, visited) = self.rename_paths(ino, newparent, newname)?;
        self.rename_visited_entries += visited;
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
