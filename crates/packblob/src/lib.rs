//! Immutable, selectively readable BLAKE3 blob packs. This indexed format is
//! independent of logical repository objects and is not a Mononoke envelope.
use std::{
    collections::BTreeMap,
    fmt,
    io::{self, Cursor, Read, Seek, SeekFrom, Write},
};

const HEADER: &[u8; 8] = b"PBLBB3I1";
const FOOTER: &[u8; 8] = b"PBLBB3E1";
const RECORD_SIZE: u64 = 72;
const FOOTER_SIZE: u64 = 32;
const NO_BASE: u64 = u64::MAX;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Invalid(&'static str),
    Limit(&'static str),
    Integrity,
    NotFound,
    Poisoned,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "pack I/O: {e}"),
            Self::Invalid(s) => write!(f, "invalid pack: {s}"),
            Self::Limit(s) => write!(f, "pack limit: {s}"),
            Self::Integrity => f.write_str("pack blob digest or size mismatch"),
            Self::NotFound => f.write_str("blob not present in pack"),
            Self::Poisoned => f.write_str("pack writer failed"),
        }
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest {
    pub hash: [u8; 32],
    pub size_bytes: u64,
}
impl Digest {
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            hash: *blake3::hash(bytes).as_bytes(),
            size_bytes: bytes.len() as u64,
        }
    }
    pub fn verify(&self, bytes: &[u8]) -> Result<()> {
        if *self == Self::of(bytes) {
            Ok(())
        } else {
            Err(Error::Integrity)
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum Codec {
    Raw = 0,
    Zstd = 1,
    Delta = 2,
}
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_entries: u64,
    pub max_blob_bytes: u64,
    pub max_encoded_bytes: u64,
    pub max_anchor_bytes: u64,
    pub max_pack_bytes: u64,
    pub window_log_max: u32,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_entries: 16_384,
            max_blob_bytes: 64 * 1024,
            max_encoded_bytes: 64 * 1024,
            max_anchor_bytes: 64 * 1024,
            max_pack_bytes: 8 * 1024 * 1024,
            window_log_max: 16,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        if self.max_entries == 0
            || self.max_entries > 1_000_000
            || self.max_blob_bytes > 64 * 1024 * 1024
            || self.max_anchor_bytes > 64 * 1024 * 1024
            || self.max_encoded_bytes == 0
            || self.max_encoded_bytes > 128 * 1024 * 1024
            || self.max_pack_bytes < 40
            || self.max_pack_bytes > (1u64 << 40)
            || !(10..=27).contains(&self.window_log_max)
        {
            return Err(Error::Limit("invalid configuration"));
        }
        Ok(())
    }
    /// Conservative per-extraction payload/window reserve, excluding caller and
    /// backing-reader buffers. Applications must separately bound concurrency.
    pub fn decode_memory_bound(&self) -> Result<u64> {
        self.validate()?;
        self.max_encoded_bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(self.max_blob_bytes))
            .and_then(|n| n.checked_add(self.max_anchor_bytes))
            .and_then(|n| n.checked_add(1u64 << self.window_log_max))
            .and_then(|n| n.checked_add(512 * 1024))
            .ok_or(Error::Limit("memory accounting overflow"))
    }
}
#[derive(Clone, Debug)]
pub struct WriteConfig {
    pub limits: Limits,
    pub compression_level: i32,
}
impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            compression_level: 3,
        }
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub entries: u64,
    pub raw_bytes: u64,
    pub encoded_bytes: u64,
    pub raw_entries: u64,
    pub zstd_entries: u64,
    pub delta_entries: u64,
}
#[derive(Clone, Debug)]
pub struct Entry {
    pub digest: Digest,
    pub offset: u64,
    pub encoded_size: u64,
    pub codec: Codec,
    pub base_ordinal: Option<u64>,
}
#[derive(Clone, Debug)]
pub struct EncodedBlob {
    pub digest: Digest,
    pub codec: Codec,
    pub base: Option<Digest>,
    pub bytes: Vec<u8>,
}
struct Written {
    entry: Entry,
    base: Option<Digest>,
}
/// Streams payloads and retains bounded index metadata until `finish`. Neither
/// finish nor Drop flushes/syncs/publishes the backing store; its owner does so.
pub struct Writer<W: Write> {
    output: W,
    config: WriteConfig,
    offset: u64,
    records: BTreeMap<Digest, Written>,
    failed: bool,
}
impl<W: Write> Writer<W> {
    pub fn new(mut output: W, config: WriteConfig) -> Result<Self> {
        config.limits.validate()?;
        if !(-7..=19).contains(&config.compression_level) {
            return Err(Error::Limit("compression level"));
        }
        output.write_all(HEADER)?;
        Ok(Self {
            output,
            config,
            offset: 8,
            records: BTreeMap::new(),
            failed: false,
        })
    }
    /// An optional anchor must already have an independent representation in
    /// this pack. Anchor bytes are checked, not trusted from the supplied ID.
    pub fn add(&mut self, bytes: &[u8], base: Option<(Digest, &[u8])>) -> Result<Digest> {
        if self.failed {
            return Err(Error::Poisoned);
        }
        if bytes.len() as u64 > self.config.limits.max_blob_bytes {
            return Err(Error::Limit("blob bytes"));
        }
        let digest = Digest::of(bytes);
        if self.records.contains_key(&digest) {
            return Ok(digest);
        }
        if self.records.len() as u64 >= self.config.limits.max_entries {
            return Err(Error::Limit("entry count"));
        }
        let independent = encode(bytes, None, &self.config)?;
        let (mut codec, mut encoded) = if independent.len() < bytes.len() {
            (Codec::Zstd, independent)
        } else {
            (Codec::Raw, bytes.to_vec())
        };
        let mut chosen_base = None;
        if let Some((id, content)) = base {
            id.verify(content)?;
            let prior = self
                .records
                .get(&id)
                .ok_or(Error::Invalid("anchor not already in pack"))?;
            if prior.entry.codec == Codec::Delta {
                return Err(Error::Invalid("delta anchor must be independent"));
            }
            if id.size_bytes <= self.config.limits.max_anchor_bytes && !content.is_empty() {
                let delta = encode(bytes, Some(content), &self.config)?;
                if delta.len() < encoded.len() {
                    codec = Codec::Delta;
                    encoded = delta;
                    chosen_base = Some(id);
                }
            }
        }
        if encoded.len() as u64 > self.config.limits.max_encoded_bytes {
            return Err(Error::Limit("encoded bytes"));
        }
        let end = self
            .offset
            .checked_add(encoded.len() as u64)
            .ok_or(Error::Limit("offset overflow"))?;
        let final_size = (self.records.len() as u64 + 1)
            .checked_mul(RECORD_SIZE)
            .and_then(|n| n.checked_add(FOOTER_SIZE))
            .and_then(|n| n.checked_add(end))
            .ok_or(Error::Limit("pack size overflow"))?;
        if final_size > self.config.limits.max_pack_bytes {
            return Err(Error::Limit("pack bytes"));
        }
        if let Err(error) = self.output.write_all(&encoded) {
            self.failed = true;
            return Err(error.into());
        }
        self.records.insert(
            digest,
            Written {
                entry: Entry {
                    digest,
                    offset: self.offset,
                    encoded_size: encoded.len() as u64,
                    codec,
                    base_ordinal: None,
                },
                base: chosen_base,
            },
        );
        self.offset = end;
        Ok(digest)
    }
    pub fn finish(mut self) -> Result<(W, Summary)> {
        if self.failed {
            return Err(Error::Poisoned);
        }
        let ordinals: BTreeMap<Digest, u64> = self
            .records
            .keys()
            .enumerate()
            .map(|(n, d)| (*d, n as u64))
            .collect();
        let index_offset = self.offset;
        let mut summary = Summary {
            entries: self.records.len() as u64,
            ..Summary::default()
        };
        for written in self.records.values() {
            let mut entry = written.entry.clone();
            entry.base_ordinal = written.base.map(|d| ordinals[&d]);
            self.output.write_all(&record_bytes(&entry))?;
            summary.raw_bytes = summary
                .raw_bytes
                .checked_add(entry.digest.size_bytes)
                .ok_or(Error::Limit("logical byte count"))?;
            match entry.codec {
                Codec::Raw => summary.raw_entries += 1,
                Codec::Zstd => summary.zstd_entries += 1,
                Codec::Delta => summary.delta_entries += 1,
            }
        }
        self.output.write_all(FOOTER)?;
        self.output.write_all(&index_offset.to_le_bytes())?;
        self.output.write_all(&summary.entries.to_le_bytes())?;
        self.output.write_all(&RECORD_SIZE.to_le_bytes())?;
        summary.encoded_bytes = index_offset + summary.entries * RECORD_SIZE + FOOTER_SIZE;
        Ok((self.output, summary))
    }
}
fn encode(bytes: &[u8], base: Option<&[u8]>, config: &WriteConfig) -> Result<Vec<u8>> {
    // Raw prefix APIs never auto-detect a formatted dictionary in file bytes.
    let mut encoder = match base {
        Some(base) => zstd::stream::write::Encoder::with_ref_prefix(
            Vec::new(),
            config.compression_level,
            base,
        )?,
        None => zstd::stream::write::Encoder::new(Vec::new(), config.compression_level)?,
    };
    encoder.window_log(config.limits.window_log_max)?;
    encoder.set_pledged_src_size(Some(bytes.len() as u64))?;
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}
fn record_bytes(entry: &Entry) -> [u8; 72] {
    let mut bytes = [0; 72];
    bytes[..32].copy_from_slice(&entry.digest.hash);
    for (index, value) in [
        entry.digest.size_bytes,
        entry.offset,
        entry.encoded_size,
        entry.base_ordinal.unwrap_or(NO_BASE),
        entry.codec as u64,
    ]
    .into_iter()
    .enumerate()
    {
        bytes[32 + index * 8..40 + index * 8].copy_from_slice(&value.to_le_bytes());
    }
    bytes
}
fn integer(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

/// Opening reads only the fixed header/footer. Lookups and extractions validate
/// accessed records, not the entire sorted index. The backing file must remain
/// immutable; blob identity is always checked against the caller's digest.
pub struct Reader<R: Read + Seek> {
    input: R,
    limits: Limits,
    index_offset: u64,
    entries: u64,
}
impl<R: Read + Seek> Reader<R> {
    pub fn open(mut input: R, limits: Limits) -> Result<Self> {
        limits.validate()?;
        let length = input.seek(SeekFrom::End(0))?;
        if length < 40 {
            return Err(Error::Invalid("truncated pack"));
        }
        if length > limits.max_pack_bytes {
            return Err(Error::Limit("pack bytes"));
        }
        let mut header = [0; 8];
        input.seek(SeekFrom::Start(0))?;
        input.read_exact(&mut header)?;
        if &header != HEADER {
            return Err(Error::Invalid("header/version"));
        }
        let mut footer = [0; 32];
        input.seek(SeekFrom::Start(length - 32))?;
        input.read_exact(&mut footer)?;
        if &footer[..8] != FOOTER {
            return Err(Error::Invalid("footer/version"));
        }
        let index_offset = integer(&footer, 8);
        let entries = integer(&footer, 16);
        if entries > limits.max_entries {
            return Err(Error::Limit("entry count"));
        }
        if integer(&footer, 24) != RECORD_SIZE
            || index_offset < 8
            || entries
                .checked_mul(RECORD_SIZE)
                .and_then(|n| n.checked_add(index_offset))
                .and_then(|n| n.checked_add(FOOTER_SIZE))
                != Some(length)
        {
            return Err(Error::Invalid("index extent"));
        }
        Ok(Self {
            input,
            limits,
            index_offset,
            entries,
        })
    }
    pub fn entry_count(&self) -> u64 {
        self.entries
    }
    pub fn into_inner(self) -> R {
        self.input
    }
    pub fn entry_at(&mut self, ordinal: u64) -> Result<Entry> {
        if ordinal >= self.entries {
            return Err(Error::Invalid("entry ordinal"));
        }
        let offset = ordinal
            .checked_mul(RECORD_SIZE)
            .and_then(|n| n.checked_add(self.index_offset))
            .ok_or(Error::Invalid("index overflow"))?;
        self.input.seek(SeekFrom::Start(offset))?;
        let mut record = [0; 72];
        self.input.read_exact(&mut record)?;
        let codec = match integer(&record, 64) {
            0 => Codec::Raw,
            1 => Codec::Zstd,
            2 => Codec::Delta,
            _ => return Err(Error::Invalid("codec")),
        };
        let base = integer(&record, 56);
        let entry = Entry {
            digest: Digest {
                hash: record[..32].try_into().unwrap(),
                size_bytes: integer(&record, 32),
            },
            offset: integer(&record, 40),
            encoded_size: integer(&record, 48),
            codec,
            base_ordinal: (base != NO_BASE).then_some(base),
        };
        if entry.offset < 8
            || entry
                .offset
                .checked_add(entry.encoded_size)
                .is_none_or(|n| n > self.index_offset)
            || (entry.codec == Codec::Raw && entry.encoded_size != entry.digest.size_bytes)
            || (entry.codec == Codec::Delta) != entry.base_ordinal.is_some()
            || entry
                .base_ordinal
                .is_some_and(|n| n >= self.entries || n == ordinal)
        {
            return Err(Error::Invalid("entry bounds or anchor"));
        }
        Ok(entry)
    }
    pub fn lookup(&mut self, digest: Digest) -> Result<Option<Entry>> {
        let (mut low, mut high) = (0, self.entries);
        while low < high {
            let middle = low + (high - low) / 2;
            let entry = self.entry_at(middle)?;
            match entry.digest.cmp(&digest) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Some(entry)),
            }
        }
        Ok(None)
    }
    fn target(&mut self, digest: Digest) -> Result<Entry> {
        self.lookup(digest)?.ok_or(Error::NotFound)
    }
    fn base(&mut self, entry: &Entry) -> Result<Option<Entry>> {
        let Some(ordinal) = entry.base_ordinal else {
            return Ok(None);
        };
        let base = self.entry_at(ordinal)?;
        if base.codec == Codec::Delta || base.base_ordinal.is_some() {
            return Err(Error::Invalid("delta chain"));
        }
        Ok(Some(base))
    }
    fn payload(&mut self, entry: &Entry) -> Result<Vec<u8>> {
        if entry.encoded_size > self.limits.max_encoded_bytes {
            return Err(Error::Limit("encoded bytes"));
        }
        let size = usize::try_from(entry.encoded_size)
            .map_err(|_| Error::Limit("encoded address space"))?;
        self.input.seek(SeekFrom::Start(entry.offset))?;
        let mut bytes = vec![0; size];
        self.input.read_exact(&mut bytes)?;
        Ok(bytes)
    }
    /// Extract just the target frame plus its independently identified anchor.
    /// This is not an authenticated or negotiated network protocol.
    pub fn read_encoded(&mut self, digest: Digest) -> Result<EncodedBlob> {
        let entry = self.target(digest)?;
        if digest.size_bytes > self.limits.max_blob_bytes {
            return Err(Error::Limit("blob bytes"));
        }
        let base = self.base(&entry)?.map(|entry| entry.digest);
        let bytes = self.payload(&entry)?;
        Ok(EncodedBlob {
            digest,
            codec: entry.codec,
            base,
            bytes,
        })
    }
    pub fn read_blob(&mut self, digest: Digest) -> Result<Vec<u8>> {
        let entry = self.target(digest)?;
        if digest.size_bytes > self.limits.max_blob_bytes {
            return Err(Error::Limit("blob bytes"));
        }
        let base = self.base(&entry)?;
        let base_bytes = if let Some(base) = base {
            if base.digest.size_bytes > self.limits.max_anchor_bytes {
                return Err(Error::Limit("anchor bytes"));
            }
            let encoded = self.payload(&base)?;
            let decoded = decode(
                base.codec,
                &encoded,
                None,
                base.digest.size_bytes,
                &self.limits,
            )?;
            base.digest.verify(&decoded)?;
            Some(decoded)
        } else {
            None
        };
        let encoded = self.payload(&entry)?;
        let bytes = decode(
            entry.codec,
            &encoded,
            base_bytes.as_deref(),
            digest.size_bytes,
            &self.limits,
        )?;
        digest.verify(&bytes)?;
        Ok(bytes)
    }
}
fn decode(
    codec: Codec,
    encoded: &[u8],
    base: Option<&[u8]>,
    expected: u64,
    limits: &Limits,
) -> Result<Vec<u8>> {
    if codec == Codec::Raw {
        if encoded.len() as u64 != expected {
            return Err(Error::Integrity);
        }
        return Ok(encoded.to_vec());
    }
    if !encoded.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        return Err(Error::Invalid("Zstandard frame"));
    }
    let cursor = Cursor::new(encoded);
    let mut decoder = match base {
        Some(base) => zstd::stream::read::Decoder::with_ref_prefix(cursor, base)?,
        None => zstd::stream::read::Decoder::with_buffer(cursor)?,
    }
    .single_frame();
    decoder.window_log_max(limits.window_log_max)?;
    let mut output = Vec::with_capacity(
        usize::try_from(expected).map_err(|_| Error::Limit("decoded address space"))?,
    );
    let mut buffer = [0; 8192];
    loop {
        let count = decoder.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        if (output.len() as u64)
            .checked_add(count as u64)
            .is_none_or(|n| n > expected)
        {
            return Err(Error::Integrity);
        }
        output.extend_from_slice(&buffer[..count]);
    }
    if decoder.finish().position() != encoded.len() as u64 || output.len() as u64 != expected {
        return Err(Error::Invalid("frame size or trailing bytes"));
    }
    Ok(output)
}

#[cfg(test)]
mod tests;
