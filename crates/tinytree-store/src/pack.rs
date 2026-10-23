//! Explicit, quiescent packing. Logical descriptors never depend on this layout.
use super::*;
use std::io::Cursor;
use std::ops::Bound;

pub(super) const PACK_READ_CHARGE: u64 = 2 * 1024 * 1024;
const MAX_OBJECT_BYTES: u64 = 64 * 1024;
const MAX_PACK_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct PackOptions {
    pub max_objects: usize,
    pub max_object_bytes: u64,
    pub max_pack_bytes: u64,
    pub max_scan_objects: usize,
    pub compression_level: i32,
    pub use_dictionaries: bool,
}
impl Default for PackOptions {
    fn default() -> Self {
        Self {
            max_objects: 4096,
            max_object_bytes: MAX_OBJECT_BYTES,
            max_pack_bytes: 8 * 1024 * 1024,
            max_scan_objects: 16_384,
            compression_level: 3,
            use_dictionaries: true,
        }
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PackReport {
    pub pack_id: Option<[u8; 32]>,
    pub scanned_objects: u64,
    /// True when this call reached the end of the current repository sweep.
    pub scan_complete: bool,
    pub packed_objects: u64,
    pub logical_bytes: u64,
    pub pack_bytes: u64,
    pub removed_loose_files: u64,
    pub raw_entries: u64,
    pub zstd_entries: u64,
    pub delta_entries: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Location {
    pack: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackRecord {
    version: u32,
    bytes: u64,
    live_members: u64,
}

fn codec_error(error: impl std::fmt::Display) -> Error {
    Error::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("packed object: {error}"),
    ))
}
fn digest(descriptor: Descriptor) -> packblob::Digest {
    packblob::Digest {
        hash: descriptor.key.id.0,
        size_bytes: descriptor.size_bytes,
    }
}
fn limits() -> packblob::Limits {
    packblob::Limits {
        max_entries: 4096,
        max_blob_bytes: MAX_OBJECT_BYTES,
        max_encoded_bytes: MAX_OBJECT_BYTES * 2,
        max_anchor_bytes: MAX_OBJECT_BYTES,
        max_pack_bytes: MAX_PACK_BYTES,
        window_log_max: 16,
    }
}
pub(super) struct Memory {
    used: Arc<AtomicU64>,
}
impl Memory {
    fn reserve(inner: &Inner) -> Result<Self> {
        inner
            .packed_read_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(PACK_READ_CHARGE)
                    .filter(|n| *n <= inner.config.max_packed_read_memory_bytes)
            })
            .map_err(|_| Error::Limit("packed read memory"))?;
        Ok(Self {
            used: inner.packed_read_bytes.clone(),
        })
    }
}
impl Drop for Memory {
    fn drop(&mut self) {
        self.used.fetch_sub(PACK_READ_CHARGE, Ordering::AcqRel);
    }
}
pub(super) enum Backing {
    Loose(File),
    Packed {
        bytes: Cursor<Vec<u8>>,
        _memory: Memory,
    },
}
impl Backing {
    pub(super) fn sync_all(&self) -> io::Result<()> {
        match self {
            Self::Loose(file) => file.sync_all(),
            Self::Packed { .. } => Ok(()),
        }
    }
}
impl Read for Backing {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Loose(file) => file.read(out),
            Self::Packed { bytes, .. } => bytes.read(out),
        }
    }
}
impl Seek for Backing {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Loose(file) => file.seek(to),
            Self::Packed { bytes, .. } => bytes.seek(to),
        }
    }
}
impl Store {
    /// Pack a bounded set of small loose objects during explicit maintenance.
    /// All normal readers/uploads must be quiescent. No automatic traffic pause
    /// or background compaction is performed by this API.
    pub fn pack(&self, repo: &str, options: PackOptions) -> Result<PackReport> {
        if options.max_objects < 2
            || options.max_objects > 4096
            || options.max_object_bytes == 0
            || options.max_object_bytes > MAX_OBJECT_BYTES
            || options.max_pack_bytes < 1024
            || options.max_pack_bytes > MAX_PACK_BYTES
            || options.max_scan_objects == 0
            || options.max_scan_objects > 1_000_000
            || !(1..=9).contains(&options.compression_level)
        {
            return Err(Error::Invalid("pack options"));
        }
        let gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        if !gate.readers.is_empty()
            || gate.staging_uploads != 0
            || !gate.repository_reservations.is_empty()
        {
            return Err(Error::Limit("pack maintenance requires quiescence"));
        }
        self.inner.reclaim_empty_packs(repo)?;
        let mut report = PackReport::default();
        let mut selected = Vec::new();
        let prefix = key(repo, b"");
        // Persist progress even when a page contains only packed/oversized
        // objects. The cursor is exclusive and may name an object since GC'd.
        let cursor_key = key(repo, b"pack-scan-cursor-v1");
        let view = self.inner.db.read_tx();
        let saved_cursor = view.get(&self.inner.spaces.meta, &cursor_key)?;
        if saved_cursor
            .as_ref()
            .is_some_and(|cursor| !cursor.starts_with(&prefix) || cursor.len() != prefix.len() + 33)
        {
            return Err(Error::Corrupt("pack scan cursor"));
        }
        let lower = match saved_cursor {
            Some(cursor) => Bound::Excluded(cursor.to_vec()),
            None => Bound::Included(prefix.clone()),
        };
        let mut end = prefix.clone();
        *end.last_mut().expect("repository key separator") = 1;
        let mut objects = view.range(&self.inner.spaces.objects, (lower, Bound::Excluded(end)));
        let mut next_cursor = None;
        let mut worst_bytes = 40u64;
        while report.scanned_objects < options.max_scan_objects as u64
            && selected.len() < options.max_objects
        {
            let Some(item) = objects.next() else {
                next_cursor = None;
                report.scan_complete = true;
                break;
            };
            report.scanned_objects += 1;
            let (key_bytes, bytes) = item.into_inner()?;
            let previous_cursor = next_cursor.clone();
            next_cursor = Some(key_bytes.to_vec());
            let descriptor = decode::<StoredDescriptor>(&bytes)?.descriptor()?;
            if &*key_bytes != object_key(repo, descriptor.key).as_slice() {
                return Err(Error::Corrupt("pack object index"));
            }
            if descriptor.key == tinytree_model::empty_tree().descriptor.key
                || descriptor.size_bytes > options.max_object_bytes
                || self.inner.location(repo, descriptor.key)?.is_some()
            {
                continue;
            }
            let charge = descriptor
                .size_bytes
                .checked_add(72)
                .ok_or(Error::Limit("pack bytes"))?;
            if worst_bytes
                .checked_add(charge)
                .is_none_or(|n| n > options.max_pack_bytes)
            {
                if !selected.is_empty() {
                    // Retry this otherwise eligible object in the next pack;
                    // a completed sweep must not hide capacity-only skips.
                    next_cursor = previous_cursor;
                    break;
                }
                // A single object exceeding the configured pack budget cannot
                // become eligible in a later page of this sweep.
                continue;
            }
            self.inner.check_backing(repo, descriptor)?;
            let mut backing = self.inner.open_backing(repo, descriptor)?;
            verify_stream(&mut backing, descriptor, self.inner.config.model_limits)?;
            backing.seek(SeekFrom::Start(0))?;
            let mut contents = Vec::with_capacity(descriptor.size_bytes as usize);
            backing.read_to_end(&mut contents)?;
            worst_bytes += charge;
            report.logical_bytes += descriptor.size_bytes;
            selected.push((descriptor, contents));
        }
        if selected.is_empty() {
            let mut tx = self
                .inner
                .db
                .write_tx()
                .durability(Some(PersistMode::SyncAll));
            match next_cursor {
                Some(cursor) => tx.insert(&self.inner.spaces.meta, &cursor_key, cursor),
                None => tx.remove(&self.inner.spaces.meta, &cursor_key),
            }
            self.inner.commit(tx)?;
            report.logical_bytes = 0;
            return Ok(report);
        }
        // Same-kind largest anchors provide a bounded, simple grouping policy.
        // Every member independently falls back when its anchor is unhelpful.
        selected.sort_by(|(a, _), (b, _)| {
            (a.key.kind as u8)
                .cmp(&(b.key.kind as u8))
                .then_with(|| b.size_bytes.cmp(&a.size_bytes))
                .then_with(|| a.key.id.0.cmp(&b.key.id.0))
        });
        let temporary = NamedTempFile::new_in(self.inner.root.join("staging"))?;
        let mut writer = packblob::Writer::new(
            temporary,
            packblob::WriteConfig {
                limits: packblob::Limits {
                    max_pack_bytes: options.max_pack_bytes,
                    ..limits()
                },
                compression_level: options.compression_level,
            },
        )
        .map_err(codec_error)?;
        let mut anchor: Option<(Kind, packblob::Digest, Vec<u8>)> = None;
        let mut written_digests = HashSet::new();
        for (descriptor, contents) in &selected {
            let base = anchor
                .as_ref()
                .filter(|(kind, _, _)| options.use_dictionaries && kind == &descriptor.key.kind)
                .map(|(_, id, bytes)| (*id, bytes.as_slice()));
            let written = writer.add(contents, base).map_err(codec_error)?;
            if written != digest(*descriptor) {
                return Err(Error::Corrupt("packed writer digest"));
            }
            let fresh_digest = written_digests.insert(written);
            if fresh_digest
                && anchor
                    .as_ref()
                    .is_none_or(|(kind, _, _)| *kind != descriptor.key.kind)
            {
                anchor = Some((descriptor.key.kind, written, contents.clone()));
            }
        }
        let (mut temporary, summary) = writer.finish().map_err(codec_error)?;
        temporary.as_file_mut().seek(SeekFrom::Start(0))?;
        let mut hash = blake3::Hasher::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let n = temporary.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        let pack_id = *hash.finalize().as_bytes();
        let pack_bytes = temporary.as_file().metadata()?.len();
        temporary.as_file().sync_all()?;
        let directory = self.inner.pack_directory(repo);
        create_durable_directories(&directory)?;
        let path = self.inner.pack_path(repo, pack_id);
        match temporary.persist_noclobber(&path) {
            Ok(file) => drop(file),
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                // Existing immutable candidate must be byte-for-byte authentic.
                let mut file = File::open(&path)?;
                let mut actual = blake3::Hasher::new();
                loop {
                    let n = file.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    actual.update(&buffer[..n]);
                }
                if actual.finalize().as_bytes() != &pack_id || file.metadata()?.len() != pack_bytes
                {
                    return Err(Error::Corrupt("existing pack collision"));
                }
            }
            Err(error) => return Err(error.error.into()),
        }
        sync_dir(&directory)?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        tx.insert(&self.inner.spaces.meta, b"format", b"tinytree-store-2");
        match next_cursor {
            Some(cursor) => tx.insert(&self.inner.spaces.meta, &cursor_key, cursor),
            None => tx.remove(&self.inner.spaces.meta, &cursor_key),
        }
        // Physical packs deduplicate across logical kinds and maintenance
        // calls. Never overwrite membership belonging to an earlier caller.
        let catalog_key = key(repo, &pack_id);
        let previous_members = match tx.get(&self.inner.spaces.packs, &catalog_key)? {
            Some(bytes) => {
                let record = decode::<PackRecord>(&bytes)?;
                if record.version != 1 || record.bytes != pack_bytes {
                    return Err(Error::Corrupt("existing pack catalog"));
                }
                record.live_members
            }
            None => 0,
        };
        let live_members = previous_members
            .checked_add(selected.len() as u64)
            .ok_or(Error::Corrupt("pack membership overflow"))?;
        for (descriptor, _) in &selected {
            if tx
                .get(
                    &self.inner.spaces.locations,
                    object_key(repo, descriptor.key),
                )?
                .is_some()
            {
                return Err(Error::Corrupt("pack selected existing location"));
            }
        }
        tx.insert(
            &self.inner.spaces.packs,
            catalog_key,
            encode(&PackRecord {
                version: 1,
                bytes: pack_bytes,
                live_members,
            })?,
        );
        for (descriptor, _) in &selected {
            tx.insert(
                &self.inner.spaces.locations,
                object_key(repo, descriptor.key),
                encode(&Location { pack: pack_id })?,
            );
        }
        self.inner.commit(tx)?;
        // Logical locations are durable before removing any old payload.
        for (descriptor, _) in &selected {
            match fs::remove_file(self.inner.path(repo, descriptor.key)) {
                Ok(()) => report.removed_loose_files += 1,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        for kind in [
            Kind::File,
            Kind::Tree,
            Kind::Commit,
            Kind::Symlink,
            Kind::CopyHistory,
        ] {
            sync_dir(
                &self
                    .inner
                    .root
                    .join("repos")
                    .join(repo)
                    .join((kind as u8).to_string()),
            )?;
        }
        report.pack_id = Some(pack_id);
        report.packed_objects = selected.len() as u64;
        report.pack_bytes = pack_bytes;
        report.raw_entries = summary.raw_entries;
        report.zstd_entries = summary.zstd_entries;
        report.delta_entries = summary.delta_entries;
        Ok(report)
    }
}
impl Inner {
    pub(super) fn validate_packs_on_reopen(&self) -> Result<()> {
        let view = self.db.read_tx();
        let mut records = 0usize;
        let mut bytes_seen = 0u64;
        let mut counts: HashMap<Vec<u8>, u64> = HashMap::new();
        for item in view.prefix(&self.spaces.locations, b"") {
            let (physical, bytes) = item.into_inner()?;
            records = records
                .checked_add(1)
                .ok_or(Error::Limit("pack reopen records"))?;
            bytes_seen = bytes_seen
                .checked_add(bytes.len() as u64)
                .ok_or(Error::Limit("pack reopen bytes"))?;
            if records > self.config.max_retention_validation_records
                || bytes_seen > self.config.max_retention_validation_bytes
            {
                return Err(Error::Limit("pack reopen validation"));
            }
            let separator = physical
                .iter()
                .position(|byte| *byte == 0)
                .ok_or(Error::Corrupt("pack location key"))?;
            let repo = std::str::from_utf8(&physical[..separator])
                .map_err(|_| Error::Corrupt("pack repository key"))?;
            validate_repository_id(repo)?;
            self.repository(repo)?;
            let descriptor = decode::<StoredDescriptor>(
                &view
                    .get(&self.spaces.objects, physical.as_ref())?
                    .ok_or(Error::Corrupt("orphan packed location"))?,
            )?
            .descriptor()?;
            if physical.as_ref() != object_key(repo, descriptor.key).as_slice()
                || descriptor.size_bytes > MAX_OBJECT_BYTES
            {
                return Err(Error::Corrupt("packed descriptor key"));
            }
            let location: Location = decode(&bytes)?;
            *counts.entry(key(repo, &location.pack)).or_default() += 1;
        }
        let has_locations = !counts.is_empty();
        for item in view.prefix(&self.spaces.packs, b"") {
            let (physical, bytes) = item.into_inner()?;
            records = records
                .checked_add(1)
                .ok_or(Error::Limit("pack reopen records"))?;
            bytes_seen = bytes_seen
                .checked_add(bytes.len() as u64)
                .ok_or(Error::Limit("pack reopen bytes"))?;
            if records > self.config.max_retention_validation_records
                || bytes_seen > self.config.max_retention_validation_bytes
            {
                return Err(Error::Limit("pack reopen validation"));
            }
            let separator = physical
                .iter()
                .position(|byte| *byte == 0)
                .ok_or(Error::Corrupt("pack catalog key"))?;
            let repo = std::str::from_utf8(&physical[..separator])
                .map_err(|_| Error::Corrupt("pack repository key"))?;
            validate_repository_id(repo)?;
            self.repository(repo)?;
            let id: [u8; 32] = physical[separator + 1..]
                .try_into()
                .map_err(|_| Error::Corrupt("pack catalog identity"))?;
            let record: PackRecord = decode(&bytes)?;
            if record.version != 1
                || record.bytes > MAX_PACK_BYTES
                || record.live_members != counts.remove(physical.as_ref()).unwrap_or(0)
            {
                return Err(Error::Corrupt("pack membership count"));
            }
            if record.live_members != 0 {
                let metadata = fs::symlink_metadata(self.pack_path(repo, id))?;
                if !metadata.is_file() || metadata.len() != record.bytes {
                    return Err(Error::Corrupt("packed payload type or size"));
                }
            }
        }
        if !counts.is_empty()
            || (has_locations
                && view.get(&self.spaces.meta, b"format")?.as_deref()
                    != Some(b"tinytree-store-2".as_slice()))
        {
            return Err(Error::Corrupt("packed catalog or feature marker missing"));
        }
        Ok(())
    }
    fn pack_directory(&self, repo: &str) -> PathBuf {
        self.root.join("repos").join(repo).join("packs")
    }
    fn pack_path(&self, repo: &str, pack: [u8; 32]) -> PathBuf {
        self.pack_directory(repo)
            .join(format!("{}.pack", hex(&pack)))
    }
    pub(super) fn is_packed(&self, repo: &str, object: ObjectKey) -> Result<bool> {
        Ok(self.location(repo, object)?.is_some())
    }
    fn location(&self, repo: &str, object: ObjectKey) -> Result<Option<Location>> {
        self.db
            .read_tx()
            .get(&self.spaces.locations, object_key(repo, object))?
            .map(|bytes| decode(&bytes))
            .transpose()
    }
    fn packed_file(&self, repo: &str, descriptor: Descriptor, location: &Location) -> Result<File> {
        if descriptor.size_bytes > MAX_OBJECT_BYTES {
            return Err(Error::Corrupt("packed object exceeds format limit"));
        }
        let record: PackRecord = decode(
            &self
                .db
                .read_tx()
                .get(&self.spaces.packs, key(repo, &location.pack))?
                .ok_or(Error::Corrupt("packed catalog missing"))?,
        )?;
        if record.version != 1 || record.live_members == 0 || record.bytes > MAX_PACK_BYTES {
            return Err(Error::Corrupt("packed catalog"));
        }
        let path = self.pack_path(repo, location.pack);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file() || metadata.len() != record.bytes {
            return Err(Error::Corrupt("packed payload type or size"));
        }
        Ok(File::open(path)?)
    }
    pub(super) fn check_backing(&self, repo: &str, descriptor: Descriptor) -> Result<()> {
        match self.location(repo, descriptor.key)? {
            Some(location) => {
                let file = self.packed_file(repo, descriptor, &location)?;
                let mut reader = packblob::Reader::open(file, limits()).map_err(codec_error)?;
                if reader
                    .lookup(digest(descriptor))
                    .map_err(codec_error)?
                    .is_none()
                {
                    return Err(Error::Corrupt("pack object index missing"));
                }
            }
            None => match fs::symlink_metadata(self.path(repo, descriptor.key)) {
                Ok(metadata) if metadata.is_file() && metadata.len() == descriptor.size_bytes => {}
                Ok(_) => return Err(Error::Corrupt("object payload type or size")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Err(Error::Corrupt("indexed payload missing"));
                }
                Err(error) => return Err(error.into()),
            },
        }
        Ok(())
    }
    pub(super) fn open_backing(&self, repo: &str, descriptor: Descriptor) -> Result<Backing> {
        match self.location(repo, descriptor.key)? {
            Some(location) => {
                let memory = Memory::reserve(self)?;
                let mut reader = packblob::Reader::open(
                    self.packed_file(repo, descriptor, &location)?,
                    limits(),
                )
                .map_err(codec_error)?;
                let bytes = reader.read_blob(digest(descriptor)).map_err(codec_error)?;
                Ok(Backing::Packed {
                    bytes: Cursor::new(bytes),
                    _memory: memory,
                })
            }
            None => Ok(Backing::Loose(File::open(self.path(repo, descriptor.key))?)),
        }
    }
    pub(super) fn remove_location(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        repo: &str,
        object: ObjectKey,
    ) -> Result<()> {
        let location_key = object_key(repo, object);
        let Some(bytes) = tx.get(&self.spaces.locations, &location_key)? else {
            return Ok(());
        };
        let location: Location = decode(&bytes)?;
        let pack_key = key(repo, &location.pack);
        let mut record: PackRecord = decode(
            &tx.get(&self.spaces.packs, &pack_key)?
                .ok_or(Error::Corrupt("pack member catalog missing"))?,
        )?;
        record.live_members = record
            .live_members
            .checked_sub(1)
            .ok_or(Error::Corrupt("pack member count"))?;
        tx.insert(&self.spaces.packs, pack_key, encode(&record)?);
        tx.remove(&self.spaces.locations, location_key);
        Ok(())
    }
    /// Caller owns the publication gate, which also orders every reader lease.
    pub(super) fn reclaim_empty_packs(&self, repo: &str) -> Result<()> {
        let mut empty = Vec::new();
        for item in self.db.read_tx().prefix(&self.spaces.packs, key(repo, b"")) {
            let (catalog_key, bytes) = item.into_inner()?;
            let record: PackRecord = decode(&bytes)?;
            if record.version != 1 {
                return Err(Error::Corrupt("pack catalog version"));
            }
            if record.live_members == 0 {
                let suffix = catalog_key
                    .get(repo.len() + 1..)
                    .ok_or(Error::Corrupt("pack catalog key"))?;
                let id: [u8; 32] = suffix
                    .try_into()
                    .map_err(|_| Error::Corrupt("pack catalog key"))?;
                empty.push((catalog_key, id));
            }
        }
        for (catalog_key, id) in empty {
            match fs::remove_file(self.pack_path(repo, id)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            sync_dir(&self.pack_directory(repo))?;
            let mut tx = self.db.write_tx().durability(Some(PersistMode::SyncAll));
            tx.remove(&self.spaces.packs, catalog_key);
            self.commit(tx)?;
        }
        Ok(())
    }
}
