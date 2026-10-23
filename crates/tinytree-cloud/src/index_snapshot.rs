//! Portable logical index records, not local segment/configuration backups.
//! Verification here proves bounded canonical syntax and the local draft DAG.
//! Importers must separately verify canonical draft bytes and admitted roots.
use crate::{BlobDigest, Error, Result};
use std::collections::BTreeMap;
use tinytree_model as m;

const MAGIC: &[u8; 8] = b"TTIDXSN\0";
const VERSION: u16 = 1;
const ORDER_POLICY: u16 = 1;
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IndexSnapshotId(pub [u8; 32]);
impl IndexSnapshotId {
    pub fn of(bytes: &[u8]) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-portable-index-snapshot-v1\0");
        hash.update(bytes);
        Self(*hash.finalize().as_bytes())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexSnapshotDescriptor {
    pub id: IndexSnapshotId,
    pub selected: BlobDigest,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDraft {
    pub descriptor: m::Descriptor,
    pub change: m::ChangeId,
    /// Canonical parent order; not a set.
    pub parents: Vec<m::CommitId>,
    /// Original nonzero ordinal. Different commits may share this value.
    pub order: u64,
}
impl IndexDraft {
    pub fn commit(&self) -> m::CommitId {
        m::CommitId(self.descriptor.key.id.0)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableIndexSnapshot {
    /// Sorted unique, nonempty exact anchors; intrinsic ROOT is an explicit marker.
    pub roots: Vec<m::CommitId>,
    /// Sorted by commit ID for canonical encoding, not reordered numeric ordinals.
    pub drafts: Vec<IndexDraft>,
}
#[derive(Clone, Copy, Debug)]
pub struct IndexSnapshotLimits {
    pub max_encoded_bytes: usize,
    pub max_decode_bytes: usize,
    pub max_roots: usize,
    pub max_drafts: usize,
    pub max_parents: usize,
    pub max_edges: usize,
    pub max_metadata_bytes: u64,
}
impl Default for IndexSnapshotLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 16 * 1024 * 1024,
            max_decode_bytes: 64 * 1024 * 1024,
            max_roots: 4096,
            max_drafts: 4096,
            max_parents: 4096,
            max_edges: 1_000_000,
            max_metadata_bytes: 4 * 1024 * 1024,
        }
    }
}
impl IndexSnapshotLimits {
    pub fn validate(self) -> Result<()> {
        if !(22..=256 * 1024 * 1024).contains(&self.max_encoded_bytes)
            || self.max_decode_bytes == 0
            || self.max_decode_bytes > 1024 * 1024 * 1024
            || !(1..=100_000).contains(&self.max_roots)
            || !(1..=100_000).contains(&self.max_drafts)
            || !(1..=100_000).contains(&self.max_parents)
            || self.max_edges > 10_000_000
            || self.max_metadata_bytes == 0
            || self.max_metadata_bytes > 64 * 1024 * 1024
        {
            return Err(Error::Invalid("portable index limits"));
        }
        Ok(())
    }
}
fn invalid() -> Error {
    Error::Invalid("portable index snapshot")
}
fn limit() -> Error {
    Error::Limit("portable index snapshot")
}
fn charge(used: &mut usize, n: usize, maximum: usize) -> Result<()> {
    *used = used
        .checked_add(n)
        .filter(|n| *n <= maximum)
        .ok_or_else(limit)?;
    Ok(())
}
fn product(n: usize, width: usize) -> Result<usize> {
    n.checked_mul(width).ok_or_else(limit)
}
pub(crate) fn shape(value: &PortableIndexSnapshot, limits: IndexSnapshotLimits) -> Result<usize> {
    limits.validate()?;
    if value.roots.is_empty()
        || value.roots.len() > limits.max_roots
        || value.drafts.len() > limits.max_drafts
    {
        return Err(limit());
    }
    if !value.roots.windows(2).all(|p| p[0] < p[1])
        || !value
            .drafts
            .windows(2)
            .all(|p| p[0].commit() < p[1].commit())
    {
        return Err(invalid());
    }
    let mut bytes = 22usize
        .checked_add(product(value.roots.len(), 32)?)
        .ok_or_else(limit)?;
    let mut memory = 4096usize + std::mem::size_of::<PortableIndexSnapshot>();
    charge(
        &mut memory,
        product(value.roots.len(), 32)?,
        limits.max_decode_bytes,
    )?;
    charge(
        &mut memory,
        product(value.drafts.len(), std::mem::size_of::<IndexDraft>() + 512)?,
        limits.max_decode_bytes,
    )?;
    let mut edges = 0usize;
    for draft in &value.drafts {
        if draft.descriptor.key.kind != m::Kind::Commit
            || draft.commit() == m::CommitId::ROOT
            || draft.order == 0
            || draft.descriptor.format_version != m::FORMAT_VERSION
            || draft.descriptor.size_bytes == 0
            || draft.descriptor.size_bytes > limits.max_metadata_bytes
            || draft.parents.is_empty()
        {
            return Err(invalid());
        }
        if draft.parents.len() > limits.max_parents {
            return Err(limit());
        }
        edges = edges
            .checked_add(draft.parents.len())
            .filter(|n| *n <= limits.max_edges)
            .ok_or_else(limit)?;
        charge(
            &mut memory,
            product(draft.parents.len(), 32)?,
            limits.max_decode_bytes,
        )?;
        bytes = bytes
            .checked_add(70)
            .and_then(|n| n.checked_add(draft.parents.len().checked_mul(32)?))
            .filter(|n| *n <= limits.max_encoded_bytes)
            .ok_or_else(limit)?;
    }
    if bytes > limits.max_encoded_bytes {
        return Err(limit());
    }
    // Scratch accounting above covers this map, colors and one explicit frame
    // per node. External parents are witnesses for the importing provider, not
    // fabricated missing/local nodes. Every local edge is visited at most once.
    let nodes: BTreeMap<_, _> = value
        .drafts
        .iter()
        .enumerate()
        .map(|(i, d)| (d.commit(), i))
        .collect();
    let mut colors = vec![0u8; value.drafts.len()];
    for start in 0..value.drafts.len() {
        if colors[start] != 0 {
            continue;
        }
        colors[start] = 1;
        let mut stack = vec![(start, 0usize)];
        while let Some((node, next)) = stack.last_mut() {
            if *next == value.drafts[*node].parents.len() {
                colors[*node] = 2;
                stack.pop();
                continue;
            }
            let parent = value.drafts[*node].parents[*next];
            *next += 1;
            if let Some(&parent) = nodes.get(&parent) {
                match colors[parent] {
                    1 => return Err(invalid()),
                    0 => {
                        colors[parent] = 1;
                        stack.push((parent, 0));
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(bytes)
}
/// Does not normalize caller sets, merge parent order or rewrite draft ordinals.
pub fn encode_index_snapshot(
    value: &PortableIndexSnapshot,
    limits: IndexSnapshotLimits,
) -> Result<Vec<u8>> {
    let size = shape(value, limits)?;
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&m::FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&ORDER_POLICY.to_le_bytes());
    bytes.extend_from_slice(&(value.roots.len() as u32).to_le_bytes());
    for root in &value.roots {
        bytes.extend_from_slice(&root.0);
    }
    bytes.extend_from_slice(&(value.drafts.len() as u32).to_le_bytes());
    for draft in &value.drafts {
        bytes.extend_from_slice(&draft.descriptor.key.id.0);
        bytes.extend_from_slice(&draft.change.0);
        bytes.extend_from_slice(&draft.descriptor.size_bytes.to_le_bytes());
        bytes.extend_from_slice(&draft.descriptor.format_version.to_le_bytes());
        bytes.extend_from_slice(&draft.order.to_le_bytes());
        bytes.extend_from_slice(&(draft.parents.len() as u32).to_le_bytes());
        for parent in &draft.parents {
            bytes.extend_from_slice(&parent.0);
        }
    }
    debug_assert_eq!(bytes.len(), size);
    Ok(bytes)
}
pub struct VerifiedIndexSnapshot {
    value: PortableIndexSnapshot,
    descriptor: IndexSnapshotDescriptor,
}
impl VerifiedIndexSnapshot {
    pub fn snapshot(&self) -> &PortableIndexSnapshot {
        &self.value
    }
    pub fn descriptor(&self) -> IndexSnapshotDescriptor {
        self.descriptor
    }
    /// Owned record buffers only; caller separately admits codec scratch work.
    pub fn retained_size_upper_bound(&self) -> Result<usize> {
        let mut bytes = std::mem::size_of::<Self>();
        charge(
            &mut bytes,
            product(
                self.value.roots.capacity(),
                std::mem::size_of::<m::CommitId>(),
            )?,
            usize::MAX,
        )?;
        charge(
            &mut bytes,
            product(
                self.value.drafts.capacity(),
                std::mem::size_of::<IndexDraft>(),
            )?,
            usize::MAX,
        )?;
        for draft in &self.value.drafts {
            charge(
                &mut bytes,
                product(draft.parents.capacity(), std::mem::size_of::<m::CommitId>())?,
                usize::MAX,
            )?;
        }
        Ok(bytes)
    }
}
impl std::fmt::Debug for VerifiedIndexSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedIndexSnapshot")
            .field("roots", &self.value.roots.len())
            .field("drafts", &self.value.drafts.len())
            .finish_non_exhaustive()
    }
}
struct Reader<'a> {
    bytes: &'a [u8],
    memory: usize,
    maximum: usize,
}
impl Reader<'_> {
    fn raw<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (head, tail) = self.bytes.split_at_checked(N).ok_or_else(invalid)?;
        self.bytes = tail;
        Ok(head.try_into().expect("fixed width"))
    }
    fn count<T>(&mut self, max: usize, min: usize) -> Result<usize> {
        let n = u32::from_le_bytes(self.raw()?) as usize;
        if n > max || product(n, min)? > self.bytes.len() {
            return Err(limit());
        }
        charge(
            &mut self.memory,
            product(n, std::mem::size_of::<T>())?,
            self.maximum,
        )?;
        Ok(n)
    }
}
/// The independently selected domain identity is checked before allocation.
/// This does not attest external root membership or verify draft canonical bytes.
pub fn decode_index_snapshot(
    bytes: &[u8],
    expected: IndexSnapshotId,
    limits: IndexSnapshotLimits,
) -> Result<VerifiedIndexSnapshot> {
    limits.validate()?;
    if bytes.len() > limits.max_encoded_bytes {
        return Err(limit());
    }
    if IndexSnapshotId::of(bytes) != expected {
        return Err(invalid());
    }
    let mut r = Reader {
        bytes,
        memory: std::mem::size_of::<PortableIndexSnapshot>(),
        maximum: limits.max_decode_bytes,
    };
    if &r.raw::<8>()? != MAGIC {
        return Err(invalid());
    }
    if u16::from_le_bytes(r.raw()?) != VERSION
        || u16::from_le_bytes(r.raw()?) != m::FORMAT_VERSION
        || u16::from_le_bytes(r.raw()?) != ORDER_POLICY
    {
        return Err(Error::Unsupported("portable index snapshot version"));
    }
    let n = r.count::<m::CommitId>(limits.max_roots, 32)?;
    let mut roots = Vec::with_capacity(n);
    for _ in 0..n {
        roots.push(m::CommitId(r.raw()?));
    }
    let n = r.count::<IndexDraft>(limits.max_drafts, 102)?;
    let mut drafts = Vec::with_capacity(n);
    let mut edges = 0usize;
    for _ in 0..n {
        let commit = m::CommitId(r.raw()?);
        let change = m::ChangeId(r.raw()?);
        let size = u64::from_le_bytes(r.raw()?);
        let version = u16::from_le_bytes(r.raw()?);
        let order = u64::from_le_bytes(r.raw()?);
        let n = r.count::<m::CommitId>(limits.max_parents, 32)?;
        edges = edges
            .checked_add(n)
            .filter(|n| *n <= limits.max_edges)
            .ok_or_else(limit)?;
        let mut parents = Vec::with_capacity(n);
        for _ in 0..n {
            parents.push(m::CommitId(r.raw()?));
        }
        drafts.push(IndexDraft {
            descriptor: m::Descriptor {
                key: m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(commit.0),
                },
                size_bytes: size,
                format_version: version,
            },
            change,
            parents,
            order,
        });
    }
    if !r.bytes.is_empty() {
        return Err(invalid());
    }
    let value = PortableIndexSnapshot { roots, drafts };
    shape(&value, limits)?;
    Ok(VerifiedIndexSnapshot {
        value,
        descriptor: IndexSnapshotDescriptor {
            id: expected,
            selected: BlobDigest::of(bytes),
        },
    })
}
