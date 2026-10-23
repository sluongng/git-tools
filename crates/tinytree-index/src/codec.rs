//! Typed, key-bound derived records. Canonical Tinytree encodings are unchanged.
use super::*;
use serde::{Deserialize, Serialize};
const MAGIC: &[u8; 4] = b"TTI2";
const DOMAIN: &[u8] = b"tinytree-derived-index-record-v2\0";
const HEADER: usize = 12;
const CHECKSUM: usize = 32;
pub(super) trait RecordCodec: Sized {
    const KIND: u8;
    fn payload(&self, key: &[u8]) -> Result<Vec<u8>>;
    fn from_payload(key: &[u8], payload: &[u8], limits: &Limits) -> Result<Self>;
}
fn valid_key(key: &[u8], kind: u8) -> Result<()> {
    let valid = match kind {
        b'h' => key == b"header",
        b'n' => key.len() == 65 && key[0] == b'n',
        b'p' => key.len() == 41 && key[0] == b'p',
        b'c' => key.len() == 81 && key[0] == b'c',
        b'r' => key.len() == 33 && key[0] == b'r',
        _ => false,
    };
    if valid { Ok(()) } else { Err(Error::Corrupt) }
}
fn checksum(key: &[u8], header_payload: &[u8]) -> Result<[u8; 32]> {
    let length = u32::try_from(key.len()).map_err(|_| Error::Corrupt)?;
    let mut hash = blake3::Hasher::new();
    hash.update(DOMAIN);
    hash.update(&length.to_le_bytes());
    hash.update(key);
    hash.update(header_payload);
    Ok(*hash.finalize().as_bytes())
}
pub(super) fn encode<T: RecordCodec>(key: &[u8], value: &T) -> Result<Vec<u8>> {
    valid_key(key, T::KIND)?;
    let payload = value.payload(key)?;
    let length =
        u32::try_from(payload.len()).map_err(|_| Error::Invalid("derived payload length"))?;
    let mut bytes = Vec::with_capacity(
        HEADER
            .checked_add(payload.len())
            .and_then(|n| n.checked_add(CHECKSUM))
            .ok_or(Error::Corrupt)?,
    );
    bytes.extend(MAGIC);
    bytes.push(T::KIND);
    bytes.extend([0; 3]);
    bytes.extend(length.to_le_bytes());
    bytes.extend(payload);
    let digest = checksum(key, &bytes)?;
    bytes.extend(digest);
    Ok(bytes)
}
pub(super) fn decode<T: RecordCodec>(key: &[u8], bytes: &[u8], limits: &Limits) -> Result<T> {
    if bytes.len() > limits.max_record_bytes {
        return Err(Error::Corrupt);
    }
    if key == b"header" && !bytes.starts_with(MAGIC) {
        return legacy_header(bytes);
    }
    valid_key(key, T::KIND)?;
    if bytes.len() < HEADER + CHECKSUM
        || &bytes[..4] != MAGIC
        || bytes[4] != T::KIND
        || bytes[5..8] != [0; 3]
    {
        return Err(Error::Corrupt);
    }
    let length = u32::from_le_bytes(bytes[8..12].try_into().map_err(|_| Error::Corrupt)?) as usize;
    let end = HEADER.checked_add(length).ok_or(Error::Corrupt)?;
    if end.checked_add(CHECKSUM) != Some(bytes.len())
        || checksum(key, &bytes[..end])?.as_slice() != &bytes[end..]
    {
        return Err(Error::Corrupt);
    }
    T::from_payload(key, &bytes[HEADER..end], limits)
}
fn take<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N]> {
    bytes
        .get(offset..offset.checked_add(N).ok_or(Error::Corrupt)?)
        .ok_or(Error::Corrupt)?
        .try_into()
        .map_err(|_| Error::Corrupt)
}
fn u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(take(bytes, offset)?))
}
impl RecordCodec for Header {
    const KIND: u8 = b'h';
    fn payload(&self, _key: &[u8]) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(48);
        bytes.extend(self.schema.to_le_bytes());
        bytes.extend(self.native_format.to_le_bytes());
        bytes.extend([0; 2]);
        bytes.extend(self.epoch);
        bytes.extend(self.repositories.to_le_bytes());
        bytes.extend(self.nodes.to_le_bytes());
        bytes.extend(self.bytes.to_le_bytes());
        Ok(bytes)
    }
    fn from_payload(_key: &[u8], bytes: &[u8], _limits: &Limits) -> Result<Self> {
        if bytes.len() != 48 || bytes[6..8] != [0; 2] {
            return Err(Error::Corrupt);
        }
        let schema = u32::from_le_bytes(take(bytes, 0)?);
        let native_format = u16::from_le_bytes(take(bytes, 4)?);
        if schema != STORAGE_SCHEMA || native_format != m::FORMAT_VERSION {
            return Err(Error::UnsupportedSchema);
        }
        Ok(Self {
            schema,
            native_format,
            epoch: take(bytes, 8)?,
            repositories: u64_at(bytes, 24)?,
            nodes: u64_at(bytes, 32)?,
            bytes: u64_at(bytes, 40)?,
        })
    }
}
impl RecordCodec for Repository {
    const KIND: u8 = b'r';
    fn payload(&self, key: &[u8]) -> Result<Vec<u8>> {
        if repo_hash(&self.name)?.as_slice() != &key[1..] {
            return Err(Error::Corrupt);
        }
        let mut bytes = Vec::with_capacity(20 + self.name.len());
        bytes.extend(
            u32::try_from(self.name.len())
                .map_err(|_| Error::Corrupt)?
                .to_le_bytes(),
        );
        bytes.extend(self.name.as_bytes());
        bytes.extend(self.high_water.to_le_bytes());
        bytes.extend(self.generation.to_le_bytes());
        Ok(bytes)
    }
    fn from_payload(key: &[u8], bytes: &[u8], _limits: &Limits) -> Result<Self> {
        let length = u32::from_le_bytes(take(bytes, 0)?) as usize;
        if length == 0 || length > 512 || length.checked_add(20) != Some(bytes.len()) {
            return Err(Error::Corrupt);
        }
        let name = std::str::from_utf8(&bytes[4..4 + length]).map_err(|_| Error::Corrupt)?;
        if repo_hash(name).map_err(|_| Error::Corrupt)?.as_slice() != &key[1..] {
            return Err(Error::Corrupt);
        }
        Ok(Self {
            name: name.into(),
            high_water: u64_at(bytes, 4 + length)?,
            generation: u64_at(bytes, 12 + length)?,
        })
    }
}
impl RecordCodec for StoredNode {
    const KIND: u8 = b'n';
    fn payload(&self, key: &[u8]) -> Result<Vec<u8>> {
        if key[33..] != self.commit || self.skips.len() > 64 {
            return Err(Error::Corrupt);
        }
        let count = u32::try_from(self.parents.len())
            .map_err(|_| Error::Invalid("derived parent count"))?;
        let mut bytes = Vec::new();
        bytes.extend(self.change);
        bytes.extend(self.position.to_le_bytes());
        bytes.extend(self.generation.to_le_bytes());
        bytes.extend(self.size.to_le_bytes());
        bytes.extend(self.committer_millis.to_le_bytes());
        bytes.extend(count.to_le_bytes());
        bytes.push(self.skips.len() as u8);
        // Explicit feature presence prevents an old reserved-zero row from
        // being mistaken for a verified clean commit, even in a mixed catalog.
        bytes.extend([u8::from(self.has_conflict), 1, 0]);
        for parent in &self.parents {
            bytes.extend(parent)
        }
        for position in &self.skips {
            bytes.extend(position.to_le_bytes())
        }
        Ok(bytes)
    }
    fn from_payload(key: &[u8], bytes: &[u8], limits: &Limits) -> Result<Self> {
        if bytes.len() < 56 || bytes[53] > 1 || bytes[54..56] != [1, 0] {
            return Err(Error::Corrupt);
        }
        let parents = u32::from_le_bytes(take(bytes, 48)?) as usize;
        let skips = bytes[52] as usize;
        let expected = parents
            .checked_mul(32)
            .and_then(|n| skips.checked_mul(8).and_then(|s| n.checked_add(s)))
            .and_then(|n| n.checked_add(56))
            .ok_or(Error::Corrupt)?;
        if parents == 0
            || parents > limits.max_parents
            || skips > 64
            || expected != bytes.len()
            || u64_at(bytes, 16)? == 0
            || u64_at(bytes, 24)? == 0
            || u64_at(bytes, 32)? == 0
            || key[33..] == [0; 32]
        {
            return Err(Error::Corrupt);
        }
        // Counts and exact payload size have been checked before vector allocation.
        let mut parent_ids = Vec::with_capacity(parents);
        for index in 0..parents {
            parent_ids.push(take(bytes, 56 + index * 32)?)
        }
        let mut positions = Vec::with_capacity(skips);
        for index in 0..skips {
            positions.push(u64_at(bytes, 56 + parents * 32 + index * 8)?)
        }
        Ok(Self {
            commit: key[33..].try_into().map_err(|_| Error::Corrupt)?,
            change: take(bytes, 0)?,
            parents: parent_ids,
            position: u64_at(bytes, 16)?,
            generation: u64_at(bytes, 24)?,
            size: u64_at(bytes, 32)?,
            committer_millis: i64::from_le_bytes(take(bytes, 40)?),
            has_conflict: bytes[53] == 1,
            skips: positions,
        })
    }
}
impl RecordCodec for [u8; 32] {
    const KIND: u8 = b'p';
    fn payload(&self, _key: &[u8]) -> Result<Vec<u8>> {
        Ok(self.to_vec())
    }
    fn from_payload(key: &[u8], bytes: &[u8], _limits: &Limits) -> Result<Self> {
        let id = take(bytes, 0)?;
        if bytes.len() != 32 || id == [0; 32] || u64::from_be_bytes(take(key, 33)?) == 0 {
            return Err(Error::Corrupt);
        }
        Ok(id)
    }
}
impl RecordCodec for u64 {
    const KIND: u8 = b'c';
    fn payload(&self, _key: &[u8]) -> Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }
    fn from_payload(_key: &[u8], bytes: &[u8], _limits: &Limits) -> Result<Self> {
        let value = u64_at(bytes, 0)?;
        if bytes.len() != 8 || value == 0 {
            return Err(Error::Corrupt);
        }
        Ok(value)
    }
}
#[derive(Serialize, Deserialize)]
struct LegacyHeader {
    schema: u32,
    epoch: [u8; 16],
    repositories: u64,
    nodes: u64,
    bytes: u64,
}
#[derive(Deserialize)]
struct LegacyEnvelope {
    value: LegacyHeader,
    checksum: [u8; 32],
}
fn legacy_header<T>(bytes: &[u8]) -> Result<T> {
    let legacy: LegacyEnvelope = serde_json::from_slice(bytes).map_err(|_| Error::Corrupt)?;
    let canonical = serde_json::to_vec(&legacy.value).map_err(|_| Error::Corrupt)?;
    if legacy.value.schema == 1 && *blake3::hash(&canonical).as_bytes() == legacy.checksum {
        Err(Error::UnsupportedSchema)
    } else {
        Err(Error::Corrupt)
    }
}
