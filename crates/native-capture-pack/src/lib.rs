//! Structural local capture-pack verification, not semantic object verification,
//! quiescent frontend proof, cloud attachment authority, or a backup receipt.
//!
//! Blocking APIs: run on an appropriately owned blocking worker. `admission` must
//! reserve `Limits::max_memory_bytes` and the configured IO work externally, and
//! remains held by every entry lease and returned buffer. Private sections and
//! the enclosing pack never have portable read capabilities.
use private_fs::PrivateDir;
use serde::Serialize;
use std::{
    any::Any,
    collections::BTreeSet,
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

pub const MAGIC: &[u8; 8] = b"TTLOCP01";
pub const TABLE_MAGIC: &[u8; 8] = b"TTLTABLE";
/// Field names and serialization order are the published V1 on-disk format.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Entry {
    pub kind: u8,
    pub id: String,
    pub blake3: [u8; 32],
    pub size: u64,
    /// Absolute payload offset, not the frame header offset.
    pub offset: u64,
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn require(ok: bool, message: &'static str) -> io::Result<()> {
    if ok { Ok(()) } else { Err(invalid(message)) }
}
fn valid_id(kind: u8, id: &str) -> bool {
    let width = match kind {
        1 | 2 => 128,
        3 | 4 | 5 | 128 | 129 => 64,
        _ => return false,
    };
    id.len() == width
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
/// Encode only a frame header. The caller supplies the exact payload separately.
/// This pure encoder is not proof that a live capture was quiescent.
pub fn frame_header(kind: u8, id: &str, size: u64, hash: [u8; 32]) -> io::Result<Vec<u8>> {
    require(valid_id(kind, id), "invalid capture entry identity")?;
    let mut out = Vec::with_capacity(42 + id.len());
    out.extend_from_slice(&[kind, id.len() as u8]);
    out.extend_from_slice(id.as_bytes());
    out.extend_from_slice(&size.to_le_bytes());
    out.extend_from_slice(&hash);
    Ok(out)
}
/// Canonical published Entry[] JSON. Callers bound entry and encoded capacities
/// before invoking this function; the verifier does so under its memory pool.
pub fn encode_table(entries: &[Entry]) -> io::Result<Vec<u8>> {
    serde_json::to_vec(entries).map_err(|_| invalid("capture table encoding failed"))
}
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_pack_bytes: u64,
    pub max_entries: usize,
    pub max_table_bytes: usize,
    /// Covers entries, sort/dedup scratch, table bytes and simultaneous read buffers.
    pub max_memory_bytes: u64,
    pub max_entry_bytes: u64,
    /// Cumulative bytes returned/attempted by portable leases (including retries).
    pub max_read_bytes: u64,
    /// Whole-pack verification IO limit. Each physical byte is read once.
    pub max_scan_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_pack_bytes: 256 * 1024 * 1024,
            max_entries: 102_051,
            max_table_bytes: 31 * 1024 * 1024,
            max_memory_bytes: 128 * 1024 * 1024,
            max_entry_bytes: 16 * 1024 * 1024,
            max_read_bytes: 1024 * 1024 * 1024,
            max_scan_bytes: 256 * 1024 * 1024,
        }
    }
}
struct Memory {
    used: AtomicU64,
    maximum: u64,
}
struct Reservation {
    pool: Arc<Memory>,
    bytes: u64,
}
impl Memory {
    fn reserve(self: &Arc<Self>, bytes: u64) -> io::Result<Reservation> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(bytes).filter(|n| *n <= self.maximum)
            })
            .map_err(|_| invalid("capture memory limit exceeded"))?;
        Ok(Reservation {
            pool: self.clone(),
            bytes,
        })
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.pool.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
struct Inner {
    file: Mutex<File>,
    entries: Vec<Entry>,
    limits: Limits,
    read_bytes: AtomicU64,
    memory: Arc<Memory>,
    _directory: Arc<PrivateDir>,
    _inventory: Reservation,
    _admission: Arc<dyn Any + Send + Sync>,
}
/// An exact raw-digest-verified pack. It deliberately exposes no raw File,
/// filename, private payload, or enclosing-blob read API.
pub struct VerifiedPack {
    inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct PortableEntryLease {
    inner: Arc<Inner>,
    index: usize,
}
/// Immutable, rehashed payload. Its memory and physical admission are held until
/// the bytes are deallocated, even after the originating pack/lease is dropped.
pub struct OwnedPortableBytes {
    bytes: Vec<u8>,
    _memory: Reservation,
    _inner: Arc<Inner>,
}
impl OwnedPortableBytes {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
impl PortableEntryLease {
    pub fn descriptor(&self) -> &Entry {
        &self.inner.entries[self.index]
    }
    /// Rehash the retained FD's payload before exposing immutable bytes. A
    /// same-user mutation after initial pack verification cannot feed an upload.
    pub fn read_verified(&self) -> io::Result<OwnedPortableBytes> {
        let entry = self.descriptor();
        require(
            entry.size <= self.inner.limits.max_entry_bytes,
            "capture entry read limit exceeded",
        )?;
        let size = usize::try_from(entry.size).map_err(|_| invalid("capture entry too large"))?;
        let memory = self.inner.memory.reserve(entry.size)?;
        self.inner
            .read_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(entry.size)
                    .filter(|n| *n <= self.inner.limits.max_read_bytes)
            })
            .map_err(|_| invalid("capture read work exhausted"))?;
        let mut bytes = vec![0; size];
        let mut file = self
            .inner
            .file
            .lock()
            .map_err(|_| invalid("capture reader poisoned"))?;
        file.seek(SeekFrom::Start(entry.offset))?;
        file.read_exact(&mut bytes)?;
        require(
            blake3::hash(&bytes).as_bytes() == &entry.blake3,
            "capture entry changed",
        )?;
        drop(file);
        Ok(OwnedPortableBytes {
            bytes,
            _memory: memory,
            _inner: self.inner.clone(),
        })
    }
}
impl VerifiedPack {
    pub fn portable_entries(&self) -> impl Iterator<Item = PortableEntryLease> + '_ {
        self.inner
            .entries
            .iter()
            .enumerate()
            .filter(|(_, e)| (1..=5).contains(&e.kind))
            .map(|(index, _)| PortableEntryLease {
                inner: self.inner.clone(),
                index,
            })
    }
    /// The filename is derived only from the expected raw digest. The caller
    /// retains its durable ticket separately; no claimed policy in bytes grants
    /// frontend, repository or cloud authority.
    pub fn open(
        directory: Arc<PrivateDir>,
        expected_hash: [u8; 32],
        expected_size: u64,
        limits: Limits,
        admission: Arc<dyn Any + Send + Sync>,
    ) -> io::Result<Self> {
        require(
            expected_size >= 24
                && expected_size <= limits.max_pack_bytes
                && expected_size <= limits.max_scan_bytes,
            "capture pack size limit exceeded",
        )?;
        require(
            limits.max_entries > 0 && limits.max_table_bytes > 0,
            "invalid capture limits",
        )?;
        let memory = Arc::new(Memory {
            used: AtomicU64::new(0),
            maximum: limits.max_memory_bytes,
        });
        // Reserve all allowed inventory slots BEFORE scanning. 512 bytes covers
        // Entry/String + BTree node/key + Vec growth; table has a separate charge.
        let inventory_bytes = (limits.max_entries as u64)
            .checked_mul(512)
            .ok_or_else(|| invalid("capture inventory overflow"))?;
        let inventory = memory.reserve(inventory_bytes)?;
        let scratch = memory.reserve(128 * 1024)?;
        let name = format!("{}.pack", hex(&expected_hash));
        let file = directory.open_existing_file(&name, expected_size)?;
        require(
            file.metadata()?.len() == expected_size,
            "capture pack size differs",
        )?;
        let mut scan = Scanner {
            file,
            hash: blake3::Hasher::new(),
            position: 0,
            size: expected_size,
        };
        require(&scan.array::<8>()? == MAGIC, "invalid capture pack magic")?;
        let mut entries = Vec::with_capacity(limits.max_entries);
        let mut seen = BTreeSet::new();
        let mut capsule = 0;
        let mut private_wc = 0;
        let mut private_remote = 0;
        loop {
            let start = scan.position;
            let first = scan.array::<1>()?[0];
            if first == TABLE_MAGIC[0] {
                let rest = scan.array::<7>()?;
                require(rest == TABLE_MAGIC[1..], "invalid capture table marker")?;
                break;
            }
            require(
                entries.len() < limits.max_entries,
                "capture entry count exceeded",
            )?;
            let width = scan.array::<1>()?[0] as usize;
            require(
                width == 64 || width == 128,
                "invalid capture identity width",
            )?;
            let mut id = [0; 128];
            scan.read(&mut id[..width])?;
            let id = std::str::from_utf8(&id[..width])
                .map_err(|_| invalid("invalid capture identity"))?
                .to_owned();
            require(valid_id(first, &id), "invalid capture entry identity")?;
            require(seen.insert((first, id.clone())), "duplicate capture entry")?;
            let size = u64::from_le_bytes(scan.array::<8>()?);
            let hash = scan.array::<32>()?;
            require(
                size <= limits.max_pack_bytes
                    && scan
                        .position
                        .checked_add(size)
                        .is_some_and(|end| end <= expected_size),
                "capture section exceeds pack",
            )?;
            let offset = scan.position;
            let mut section = blake3::Hasher::new();
            let mut left = size;
            let mut buffer = [0u8; 64 * 1024];
            while left > 0 {
                let take = left.min(buffer.len() as u64) as usize;
                scan.read(&mut buffer[..take])?;
                section.update(&buffer[..take]);
                left -= take as u64;
            }
            require(
                section.finalize().as_bytes() == &hash,
                "capture section hash differs",
            )?;
            if first >= 128 {
                require(id == hex(&hash), "private capture identity differs")?;
            }
            if first == 5 {
                capsule += 1;
            }
            if first == 128 {
                private_wc += 1;
            }
            if first == 129 {
                private_remote += 1;
            }
            entries.push(Entry {
                kind: first,
                id,
                blake3: hash,
                size,
                offset,
            });
            require(scan.position > start, "capture scan made no progress")?;
        }
        require(
            capsule == 1 && private_wc == 1 && private_remote <= 1,
            "capture completion sections missing",
        )?;
        let table_size = u64::from_le_bytes(scan.array::<8>()?);
        require(
            table_size <= limits.max_table_bytes as u64
                && scan.position.checked_add(table_size) == Some(expected_size),
            "capture table size or EOF differs",
        )?;
        // No attacker-controlled JSON decoding: encode scanned frame descriptors
        // and compare canonical bytes exactly. Unknown keys/whitespace/escapes,
        // arrays and numeric expansion cannot allocate through a JSON decoder.
        entries.sort_unstable_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
        drop(seen);
        // Every validated descriptor is under 512 JSON bytes (fixed ASCII ID,
        // 32 decimal u8s, and fixed-width scalar fields). This also matches the
        // existing writer's preallocation bound. Charge the encoder's possible
        // geometric Vec capacity before allocating, not after observing output.
        let table_bound = entries
            .len()
            .checked_mul(512)
            .ok_or_else(|| invalid("capture table bound overflow"))?;
        require(
            table_bound <= limits.max_table_bytes,
            "capture table decode budget exceeded",
        )?;
        let _table_memory = memory.reserve(
            (table_bound as u64)
                .checked_mul(2)
                .ok_or_else(|| invalid("table budget overflow"))?,
        )?;
        let canonical = encode_table(&entries)?;
        require(
            canonical.len() as u64 == table_size,
            "capture table length differs",
        )?;
        let mut offset = 0;
        let mut buffer = [0u8; 64 * 1024];
        while offset < canonical.len() {
            let take = (canonical.len() - offset).min(buffer.len());
            scan.read(&mut buffer[..take])?;
            require(
                buffer[..take] == canonical[offset..offset + take],
                "capture table differs",
            )?;
            offset += take;
        }
        require(
            scan.position == expected_size && scan.hash.finalize().as_bytes() == &expected_hash,
            "capture pack digest differs",
        )?;
        require(
            scan.file.metadata()?.len() == expected_size,
            "capture pack changed during verification",
        )?;
        drop(canonical);
        drop(_table_memory);
        drop(scratch);
        Ok(Self {
            inner: Arc::new(Inner {
                file: Mutex::new(scan.file),
                entries,
                limits,
                read_bytes: AtomicU64::new(0),
                memory,
                _directory: directory,
                _inventory: inventory,
                _admission: admission,
            }),
        })
    }
}
struct Scanner {
    file: File,
    hash: blake3::Hasher,
    position: u64,
    size: u64,
}
impl Scanner {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<()> {
        let end = self
            .position
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("capture offset overflow"))?;
        require(end <= self.size, "truncated capture pack")?;
        self.file.read_exact(bytes)?;
        self.hash.update(bytes);
        self.position = end;
        Ok(())
    }
    fn array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut bytes = [0; N];
        self.read(&mut bytes)?;
        Ok(bytes)
    }
}
fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 15) as usize] as char);
    }
    out
}
#[cfg(test)]
mod tests;
