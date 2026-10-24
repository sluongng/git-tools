//! Independently versioned, byte-bounded rebuildable content records. These
//! records neither authorize canonical reads nor retain canonical objects.
//! Missing evicted records require warming; they are not negative search results.
use super::*;
use std::collections::HashMap;
use tinytree_search::{ALGORITHM_VERSION, FILTER_BYTES, FileFilter};

const KEYSPACE: &str = "content-search-v1";
const MAGIC: &[u8; 4] = b"TCS1";
const DOMAIN: &[u8] = b"tinytree-content-record-v1\0";
const OVERHEAD: usize = 4 + 1 + 8 + 4 + 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentLimits {
    pub max_bytes: u64,
    pub max_records: usize,
    pub max_manifest_entries: usize,
    pub max_manifest_bytes: usize,
    pub max_path_bytes: usize,
}
impl Default for ContentLimits {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024,
            max_records: 65_536,
            max_manifest_entries: 65_536,
            max_manifest_bytes: 8 * 1024 * 1024,
            max_path_bytes: 4096,
        }
    }
}
impl ContentLimits {
    fn validate(&self) -> Result<()> {
        if !(1024..=1024 * 1024 * 1024).contains(&self.max_bytes)
            || !(1..=1_000_000).contains(&self.max_records)
            || !(1..=1_000_000).contains(&self.max_manifest_entries)
            || !(128..=64 * 1024 * 1024).contains(&self.max_manifest_bytes)
            || !(1..=4096).contains(&self.max_path_bytes)
        {
            return Err(Error::Invalid("content cache limits"));
        }
        Ok(())
    }

    /// Check whether this immutable manifest and every distinct required FILE
    /// filter can coexist. A permanent quota failure must not be retried as a
    /// cache miss. This performs no database mutation or canonical reads.
    pub fn check_manifest(&self, root: m::TreeId, entries: &[ContentEntry]) -> Result<()> {
        checked_manifest(root, entries, self).map(|_| ())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentEntry {
    pub path: String,
    pub file: m::FileId,
    pub size: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentManifest {
    pub root: m::TreeId,
    pub digest: [u8; 32],
    pub entries: Vec<ContentEntry>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContentFile {
    pub file: m::FileId,
    pub size: u64,
    pub filter: FileFilter,
}

pub(super) struct ContentState {
    table: SingleWriterTxKeyspace,
    limits: ContentLimits,
    bytes: u64,
    next: u64,
    // Conservative persistent metadata bound: at most max_records entries in
    // each map. FIFO, not access LRU; lookups never write recency to disk.
    order: BTreeMap<u64, (Vec<u8>, u64)>,
    sequence: HashMap<Vec<u8>, u64>,
    failed: bool,
}
fn record_key(tag: u8, repository: &str, id: &[u8; 32]) -> Result<Vec<u8>> {
    let repo = repo_hash(repository)?;
    Ok(key(tag, &repo, id))
}
fn checksum(key: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(DOMAIN);
    hash.update(&(key.len() as u64).to_be_bytes());
    hash.update(key);
    hash.update(bytes);
    *hash.finalize().as_bytes()
}
fn encode_record(key: &[u8], sequence: u64, payload: &[u8]) -> Result<Vec<u8>> {
    let length = u32::try_from(payload.len()).map_err(|_| Error::BudgetExceeded)?;
    let mut bytes = Vec::with_capacity(
        payload
            .len()
            .checked_add(OVERHEAD)
            .ok_or(Error::BudgetExceeded)?,
    );
    bytes.extend(MAGIC);
    bytes.push(key[0]);
    bytes.extend(sequence.to_be_bytes());
    bytes.extend(length.to_be_bytes());
    bytes.extend(payload);
    bytes.extend(checksum(key, &bytes));
    Ok(bytes)
}
fn decode_record<'a>(key: &[u8], bytes: &'a [u8], maximum: usize) -> Result<(u64, &'a [u8])> {
    if key.is_empty()
        || bytes.len() < OVERHEAD
        || bytes.len() > maximum
        || &bytes[..4] != MAGIC
        || bytes[4] != key[0]
    {
        return Err(Error::Corrupt);
    }
    let sequence = u64::from_be_bytes(bytes[5..13].try_into().map_err(|_| Error::Corrupt)?);
    let len = u32::from_be_bytes(bytes[13..17].try_into().map_err(|_| Error::Corrupt)?) as usize;
    let end = 17usize.checked_add(len).ok_or(Error::Corrupt)?;
    if end.checked_add(32) != Some(bytes.len())
        || checksum(key, &bytes[..end]).as_slice() != &bytes[end..]
    {
        return Err(Error::Corrupt);
    }
    Ok((sequence, &bytes[17..end]))
}
fn header_payload(bytes: u64, next: u64, rows: usize) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(32);
    result.extend(1u32.to_be_bytes());
    result.extend(ALGORITHM_VERSION.to_be_bytes());
    result.extend(bytes.to_be_bytes());
    result.extend(next.to_be_bytes());
    result.extend(
        u64::try_from(rows)
            .map_err(|_| Error::Corrupt)?
            .to_be_bytes(),
    );
    Ok(result)
}
fn u64_at(bytes: &[u8], start: usize) -> Result<u64> {
    Ok(u64::from_be_bytes(
        bytes
            .get(start..start + 8)
            .ok_or(Error::Corrupt)?
            .try_into()
            .map_err(|_| Error::Corrupt)?,
    ))
}
fn validate_path(path: &str, limits: &ContentLimits) -> Result<()> {
    if path.is_empty()
        || path.len() > limits.max_path_bytes
        || path.contains(['\0', '\\'])
        || path.split('/').any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(Error::Invalid("content manifest path"));
    }
    Ok(())
}
fn manifest_payload(
    root: m::TreeId,
    entries: &[ContentEntry],
    limits: &ContentLimits,
) -> Result<([u8; 32], Vec<u8>)> {
    if entries.len() > limits.max_manifest_entries {
        return Err(Error::BudgetExceeded);
    }
    let mut size = 36usize;
    let mut previous: Option<&str> = None;
    for entry in entries {
        validate_path(&entry.path, limits)?;
        if previous.is_some_and(|p| p >= entry.path.as_str()) {
            return Err(Error::Invalid("content manifest order"));
        }
        size = size
            .checked_add(2 + entry.path.len() + 32 + 8)
            .ok_or(Error::BudgetExceeded)?;
        if size
            .checked_add(OVERHEAD)
            .is_none_or(|n| n > limits.max_manifest_bytes)
        {
            return Err(Error::BudgetExceeded);
        }
        previous = Some(&entry.path);
    }
    let mut payload = Vec::with_capacity(size);
    payload.extend([0; 32]);
    payload.extend((entries.len() as u32).to_be_bytes());
    for entry in entries {
        payload.extend((entry.path.len() as u16).to_be_bytes());
        payload.extend(entry.path.as_bytes());
        payload.extend(entry.file.0);
        payload.extend(entry.size.to_be_bytes());
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-content-manifest-v1\0");
    hash.update(&ALGORITHM_VERSION.to_be_bytes());
    hash.update(&root.0);
    hash.update(&payload[32..]);
    let digest = *hash.finalize().as_bytes();
    payload[..32].copy_from_slice(&digest);
    Ok((digest, payload))
}

fn checked_manifest(
    root: m::TreeId,
    entries: &[ContentEntry],
    limits: &ContentLimits,
) -> Result<([u8; 32], Vec<u8>)> {
    limits.validate()?;
    let (digest, payload) = manifest_payload(root, entries, limits)?;
    let mut files = BTreeMap::new();
    for entry in entries {
        if files
            .insert(entry.file, entry.size)
            .is_some_and(|size| size != entry.size)
        {
            return Err(Error::Invalid("conflicting content FILE sizes"));
        }
    }
    // Every data key is tag + repository hash + native identity. Envelope size
    // is independent of insertion sequence; the fixed cache header is excluded
    // from max_bytes consistently with ContentState's logical accounting.
    let rows = files.len().checked_add(1).ok_or(Error::BudgetExceeded)?;
    let filter_bytes = 65usize + OVERHEAD + 8 + FILTER_BYTES;
    let required = files
        .len()
        .checked_mul(filter_bytes)
        .and_then(|bytes| bytes.checked_add(65 + OVERHEAD))
        .and_then(|bytes| bytes.checked_add(payload.len()))
        .ok_or(Error::BudgetExceeded)?;
    if rows > limits.max_records
        || u64::try_from(required).map_err(|_| Error::BudgetExceeded)? > limits.max_bytes
    {
        return Err(Error::BudgetExceeded);
    }
    Ok((digest, payload))
}
fn decode_manifest(
    root: m::TreeId,
    payload: &[u8],
    limits: &ContentLimits,
) -> Result<ContentManifest> {
    if payload.len() < 36
        || payload
            .len()
            .checked_add(OVERHEAD)
            .is_none_or(|n| n > limits.max_manifest_bytes)
    {
        return Err(Error::Corrupt);
    }
    let digest: [u8; 32] = payload[..32].try_into().map_err(|_| Error::Corrupt)?;
    let count =
        u32::from_be_bytes(payload[32..36].try_into().map_err(|_| Error::Corrupt)?) as usize;
    if count > limits.max_manifest_entries || count > (payload.len() - 36) / 43 {
        return Err(Error::Corrupt);
    }
    let mut entries = Vec::with_capacity(count);
    let mut offset = 36;
    for _ in 0..count {
        let n = u16::from_be_bytes(
            payload
                .get(offset..offset + 2)
                .ok_or(Error::Corrupt)?
                .try_into()
                .map_err(|_| Error::Corrupt)?,
        ) as usize;
        offset += 2;
        let path = std::str::from_utf8(payload.get(offset..offset + n).ok_or(Error::Corrupt)?)
            .map_err(|_| Error::Corrupt)?
            .to_owned();
        offset += n;
        let file = m::FileId(
            payload
                .get(offset..offset + 32)
                .ok_or(Error::Corrupt)?
                .try_into()
                .map_err(|_| Error::Corrupt)?,
        );
        offset += 32;
        let size = u64_at(payload, offset)?;
        offset += 8;
        entries.push(ContentEntry { path, file, size });
    }
    if offset != payload.len() {
        return Err(Error::Corrupt);
    }
    let (actual, canonical) =
        manifest_payload(root, &entries, limits).map_err(|_| Error::Corrupt)?;
    if actual != digest || canonical != payload {
        return Err(Error::Corrupt);
    }
    Ok(ContentManifest {
        root,
        digest,
        entries,
    })
}

impl ContentState {
    fn open(db: &SingleWriterTxDatabase, limits: &ContentLimits) -> Result<Self> {
        limits.validate()?;
        let existed = db.keyspace_exists(KEYSPACE);
        let table = db
            .keyspace(KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(storage)?;
        let raw = table.get(b"header").map_err(storage)?;
        let (bytes, next, rows) = if let Some(raw) = raw {
            let (sequence, payload) = decode_record(b"header", &raw, 128)?;
            if sequence != 0
                || payload.len() != 32
                || &payload[..4] != 1u32.to_be_bytes()
                || &payload[4..8] != ALGORITHM_VERSION.to_be_bytes()
            {
                return Err(Error::UnsupportedSchema);
            }
            (
                u64_at(payload, 8)?,
                u64_at(payload, 16)?,
                usize::try_from(u64_at(payload, 24)?).map_err(|_| Error::Corrupt)?,
            )
        } else {
            if existed || db.read_tx().prefix(&table, b"").next().is_some() {
                return Err(Error::Corrupt);
            }
            let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
            tx.insert(
                &table,
                b"header",
                encode_record(b"header", 0, &header_payload(0, 0, 0)?)?,
            );
            tx.commit().map_err(storage)?;
            (0, 0, 0)
        };
        if bytes > limits.max_bytes || rows > limits.max_records {
            return Err(Error::BudgetExceeded);
        }
        let mut state = Self {
            table,
            limits: limits.clone(),
            bytes,
            next,
            order: BTreeMap::new(),
            sequence: HashMap::new(),
            failed: false,
        };
        let mut actual = 0u64;
        for item in db.read_tx().prefix(&state.table, b"") {
            let (key, raw) = item.into_inner().map_err(storage)?;
            if key.as_ref() == b"header" {
                continue;
            }
            if key.len() != 65
                || !matches!(key[0], b'f' | b'm')
                || state.order.len() >= limits.max_records
            {
                return Err(Error::Corrupt);
            }
            let maximum = if key[0] == b'f' {
                OVERHEAD + 8 + FILTER_BYTES
            } else {
                limits.max_manifest_bytes
            };
            let (sequence, payload) = decode_record(&key, &raw, maximum)?;
            if sequence == 0 || sequence > next || state.order.contains_key(&sequence) {
                return Err(Error::Corrupt);
            }
            if key[0] == b'f' {
                if payload.len() != 8 + FILTER_BYTES {
                    return Err(Error::Corrupt);
                }
            } else {
                let root = m::TreeId(key[33..].try_into().map_err(|_| Error::Corrupt)?);
                decode_manifest(root, payload, limits)?;
            }
            let charge = (key.len() + raw.len()) as u64;
            actual = actual.checked_add(charge).ok_or(Error::Corrupt)?;
            if actual > limits.max_bytes {
                return Err(Error::Corrupt);
            }
            state.order.insert(sequence, (key.to_vec(), charge));
            state.sequence.insert(key.to_vec(), sequence);
        }
        if actual != bytes || state.order.len() != rows {
            return Err(Error::Corrupt);
        }
        Ok(state)
    }
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.failed {
            return Err(Error::Unavailable);
        }
        let raw = self.table.get(key).map_err(storage)?;
        let Some(raw) = raw else {
            if self.sequence.contains_key(key) {
                return Err(Error::Corrupt);
            }
            return Ok(None);
        };
        let maximum = if key[0] == b'f' {
            OVERHEAD + 8 + FILTER_BYTES
        } else {
            self.limits.max_manifest_bytes
        };
        let (sequence, payload) = decode_record(key, &raw, maximum)?;
        if self.sequence.get(key) != Some(&sequence)
            || self.order.get(&sequence).is_none_or(|(stored, size)| {
                stored.as_slice() != key || *size != (key.len() + raw.len()) as u64
            })
        {
            return Err(Error::Corrupt);
        }
        Ok(Some(payload.to_vec()))
    }
    fn put(
        &mut self,
        db: &SingleWriterTxDatabase,
        key: Vec<u8>,
        payload: &[u8],
        protected: &HashSet<Vec<u8>>,
        durability: Option<PersistMode>,
    ) -> Result<()> {
        if self.failed {
            return Err(Error::Unavailable);
        }
        if let Some(old) = self.read(&key)? {
            if old != payload {
                return Err(Error::Corrupt);
            }
            // Completing an unchanged manifest is still a durability barrier:
            // filters may have been rebuilt since its original publication.
            // Fjall skips empty write transactions, so persist explicitly.
            if let Some(mode) = durability {
                if let Err(error) = db.persist(mode) {
                    self.failed = true;
                    return Err(storage(error));
                }
            }
            return Ok(());
        }
        let next = self.next.checked_add(1).ok_or(Error::BudgetExceeded)?;
        let raw = encode_record(&key, next, payload)?;
        let charge = (key.len() + raw.len()) as u64;
        if charge > self.limits.max_bytes {
            return Err(Error::BudgetExceeded);
        }
        let mut bytes = self
            .bytes
            .checked_add(charge)
            .ok_or(Error::BudgetExceeded)?;
        let mut rows = self.order.len() + 1;
        let mut removed = Vec::new();
        for (sequence, (old, size)) in &self.order {
            if bytes <= self.limits.max_bytes && rows <= self.limits.max_records {
                break;
            }
            if protected.contains(old) {
                continue;
            }
            bytes = bytes.checked_sub(*size).ok_or(Error::Corrupt)?;
            rows -= 1;
            removed.push((*sequence, old.clone()));
        }
        if bytes > self.limits.max_bytes || rows > self.limits.max_records {
            return Err(Error::BudgetExceeded);
        }
        let mut tx = db.write_tx().durability(durability);
        for (_, old) in &removed {
            tx.remove(&self.table, old);
        }
        tx.insert(&self.table, &key, raw);
        tx.insert(
            &self.table,
            b"header",
            encode_record(b"header", 0, &header_payload(bytes, next, rows)?)?,
        );
        if let Err(error) = tx.commit() {
            self.failed = true;
            return Err(storage(error));
        }
        for (sequence, old) in removed {
            self.order.remove(&sequence);
            self.sequence.remove(&old);
        }
        self.order.insert(next, (key.clone(), charge));
        self.sequence.insert(key, next);
        self.bytes = bytes;
        self.next = next;
        Ok(())
    }
}
fn ready(state: &mut State, limits: &ContentLimits) -> Result<()> {
    if state.failed {
        return Err(Error::Unavailable);
    }
    if let Some(content) = &state.content {
        if &content.limits != limits {
            return Err(Error::Invalid("content cache policy changed while open"));
        }
    } else {
        state.content = Some(ContentState::open(&state.db, limits)?);
    }
    Ok(())
}
impl Index {
    pub fn content_file(
        &self,
        repository: &str,
        file: m::FileId,
        size: u64,
        limits: &ContentLimits,
    ) -> Result<Option<ContentFile>> {
        let key = record_key(b'f', repository, &file.0)?;
        let mut state = self.state.lock().unwrap();
        ready(&mut state, limits)?;
        let Some(payload) = state.content.as_ref().unwrap().read(&key)? else {
            return Ok(None);
        };
        if payload.len() != 8 + FILTER_BYTES || u64_at(&payload, 0)? != size {
            return Err(Error::Corrupt);
        }
        Ok(Some(ContentFile {
            file,
            size,
            filter: FileFilter::from_bytes(payload[8..].try_into().map_err(|_| Error::Corrupt)?),
        }))
    }
    /// Caller must derive the filter from verified canonical FILE bytes. These
    /// rebuildable rows are journaled without a per-file sync. Publication of a
    /// complete manifest synchronizes all preceding filter transactions.
    pub fn put_content_file(
        &self,
        repository: &str,
        file: m::FileId,
        size: u64,
        filter: FileFilter,
        limits: &ContentLimits,
    ) -> Result<()> {
        self.put_content_file_with_durability(repository, file, size, filter, limits, None)
    }
    #[cfg(test)]
    pub(super) fn put_content_file_synced_for_benchmark(
        &self,
        repository: &str,
        file: m::FileId,
        size: u64,
        filter: FileFilter,
        limits: &ContentLimits,
    ) -> Result<()> {
        self.put_content_file_with_durability(
            repository,
            file,
            size,
            filter,
            limits,
            Some(PersistMode::SyncAll),
        )
    }
    fn put_content_file_with_durability(
        &self,
        repository: &str,
        file: m::FileId,
        size: u64,
        filter: FileFilter,
        limits: &ContentLimits,
        durability: Option<PersistMode>,
    ) -> Result<()> {
        let key = record_key(b'f', repository, &file.0)?;
        let mut payload = Vec::with_capacity(8 + FILTER_BYTES);
        payload.extend(size.to_be_bytes());
        payload.extend(filter.as_bytes());
        let mut state = self.state.lock().unwrap();
        ready(&mut state, limits)?;
        let State { db, content, .. } = &mut *state;
        content
            .as_mut()
            .unwrap()
            .put(db, key, &payload, &HashSet::new(), durability)
    }
    pub fn content_manifest(
        &self,
        repository: &str,
        root: m::TreeId,
        limits: &ContentLimits,
    ) -> Result<Option<ContentManifest>> {
        let key = record_key(b'm', repository, &root.0)?;
        let mut state = self.state.lock().unwrap();
        ready(&mut state, limits)?;
        state
            .content
            .as_ref()
            .unwrap()
            .read(&key)?
            .map(|payload| decode_manifest(root, &payload, limits))
            .transpose()
    }
    /// Publish the complete, sorted canonical regular-file manifest atomically.
    /// Existing required filters are protected during this publication's FIFO
    /// eviction. Later eviction can remove them; readers must check every filter.
    pub fn put_content_manifest(
        &self,
        repository: &str,
        root: m::TreeId,
        entries: &[ContentEntry],
        limits: &ContentLimits,
    ) -> Result<[u8; 32]> {
        let key = record_key(b'm', repository, &root.0)?;
        let (digest, payload) = checked_manifest(root, entries, limits)?;
        let mut protected = HashSet::new();
        let mut state = self.state.lock().unwrap();
        ready(&mut state, limits)?;
        for entry in entries {
            let file_key = record_key(b'f', repository, &entry.file.0)?;
            let Some(raw) = state.content.as_ref().unwrap().read(&file_key)? else {
                return Err(Error::NotReady);
            };
            if raw.len() != 8 + FILTER_BYTES || u64_at(&raw, 0)? != entry.size {
                return Err(Error::Corrupt);
            }
            protected.insert(file_key);
        }
        let State { db, content, .. } = &mut *state;
        content
            .as_mut()
            .unwrap()
            .put(db, key, &payload, &protected, Some(PersistMode::SyncAll))?;
        Ok(digest)
    }
}
