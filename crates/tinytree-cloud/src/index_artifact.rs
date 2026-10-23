//! Canonical persistent logical-index radix artifacts. Format does not depend on
//! insertion history, filesystem layout or transport configuration. Builders
//! currently rebuild from a bounded model; only storage/transfer reuse is claimed.
use crate::{BlobDigest, Error, IndexDraft, IndexSnapshotLimits, PortableIndexSnapshot, Result};
use std::collections::{BTreeMap, BTreeSet};
use tinytree_model as m;
const MAGIC: &[u8; 8] = b"TTIDXAR\0";
const VERSION: u16 = 1;
const HEADER: usize = 56;
const LEAF_RECORDS: usize = 64;
const LEAF_BYTES: usize = 256 * 1024;
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IndexRootId(pub [u8; 32]);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IndexNodeId(pub [u8; 32]);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum IndexArtifactId {
    Root(IndexRootId),
    Node(IndexNodeId),
}
impl IndexArtifactId {
    pub fn of_root(bytes: &[u8]) -> Self {
        Self::Root(IndexRootId(domain(b"tinytree-index-root-v1\0", bytes)))
    }
    pub fn of_node(bytes: &[u8]) -> Self {
        Self::Node(IndexNodeId(domain(b"tinytree-index-node-v1\0", bytes)))
    }
}
fn domain(tag: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(tag);
    h.update(bytes);
    *h.finalize().as_bytes()
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexRootDescriptor {
    pub id: IndexRootId,
    pub selected: BlobDigest,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexArtifactDescriptor {
    pub id: IndexArtifactId,
    pub selected: BlobDigest,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexCollection {
    Anchors,
    Drafts,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexRoot {
    pub anchors: IndexNodeId,
    pub drafts: Option<IndexNodeId>,
    pub anchor_count: u32,
    pub draft_count: u32,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexChild {
    pub slot: u8,
    pub node: IndexNodeId,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexNodePayload {
    Anchors(Vec<m::CommitId>),
    Drafts(Vec<IndexDraft>),
    Branch(Vec<IndexChild>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexNode {
    pub collection: IndexCollection,
    pub depth: u8,
    /// High depth nibbles are meaningful; remaining bits must be zero.
    pub prefix: [u8; 32],
    pub records: u32,
    /// Encoded leaf record bytes, excluding the fixed node header.
    pub record_bytes: u64,
    pub payload: IndexNodePayload,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexArtifact {
    Root(IndexRoot),
    Node(IndexNode),
}
#[derive(Clone, Copy, Debug)]
pub struct IndexArtifactLimits {
    pub snapshot: IndexSnapshotLimits,
    pub max_artifact_bytes: usize,
    pub max_artifacts: usize,
    pub max_snapshots: usize,
    pub max_total_artifact_bytes: u64,
    /// Aggregate retained artifact records, lookup scratch and one snapshot decode.
    pub max_decode_bytes: usize,
    pub max_graph_steps: usize,
}
impl Default for IndexArtifactLimits {
    fn default() -> Self {
        Self {
            snapshot: Default::default(),
            max_artifact_bytes: 4 * 1024 * 1024,
            max_artifacts: 100_000,
            max_snapshots: 4096,
            max_total_artifact_bytes: 64 * 1024 * 1024,
            max_decode_bytes: 256 * 1024 * 1024,
            max_graph_steps: 10_000_000,
        }
    }
}
impl IndexArtifactLimits {
    pub fn validate(self) -> Result<()> {
        self.snapshot.validate()?;
        if !(88..=4 * 1024 * 1024).contains(&self.max_artifact_bytes)
            || !(1..=1_000_000).contains(&self.max_artifacts)
            || !(1..=100_000).contains(&self.max_snapshots)
            || self.max_total_artifact_bytes == 0
            || self.max_total_artifact_bytes > 1024 * 1024 * 1024
            || self.max_decode_bytes == 0
            || self.max_decode_bytes > 1024 * 1024 * 1024
            || self.max_graph_steps == 0
            || self.max_graph_steps > 100_000_000
        {
            return Err(bad());
        }
        Ok(())
    }
}
fn bad() -> Error {
    Error::Invalid("portable index artifact")
}
fn limit() -> Error {
    Error::Limit("portable index artifacts")
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(limit)
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or_else(limit)
}
fn nibble(key: &[u8; 32], depth: u8) -> u8 {
    let byte = key[(depth / 2) as usize];
    if depth % 2 == 0 { byte >> 4 } else { byte & 15 }
}
fn prefix(mut key: [u8; 32], depth: u8) -> [u8; 32] {
    let bytes = (depth / 2) as usize;
    if depth % 2 != 0 {
        key[bytes] &= 0xf0;
        key[bytes + 1..].fill(0);
    } else {
        key[bytes..].fill(0);
    }
    key
}
fn record_bytes(d: &IndexDraft) -> Result<usize> {
    add(70, mul(d.parents.len(), 32)?)
}
fn node_shape(node: &IndexNode, l: IndexArtifactLimits) -> Result<usize> {
    if node.depth > 64 || prefix(node.prefix, node.depth) != node.prefix || node.records == 0 {
        return Err(bad());
    }
    let maximum = match node.collection {
        IndexCollection::Anchors => l.snapshot.max_roots,
        IndexCollection::Drafts => l.snapshot.max_drafts,
    };
    if node.records as usize > maximum || node.record_bytes > l.snapshot.max_encoded_bytes as u64 {
        return Err(limit());
    }
    let payload = match &node.payload {
        IndexNodePayload::Anchors(records) => {
            if node.collection != IndexCollection::Anchors
                || records.len() != node.records as usize
                || !records.windows(2).all(|p| p[0] < p[1])
                || records
                    .iter()
                    .any(|id| prefix(id.0, node.depth) != node.prefix)
            {
                return Err(bad());
            }
            mul(records.len(), 32)?
        }
        IndexNodePayload::Drafts(records) => {
            if node.collection != IndexCollection::Drafts
                || records.len() != node.records as usize
                || !records.windows(2).all(|p| p[0].commit() < p[1].commit())
            {
                return Err(bad());
            }
            let mut bytes = 0;
            let mut edges = 0usize;
            for d in records {
                if prefix(d.commit().0, node.depth) != node.prefix
                    || d.commit() == m::CommitId::ROOT
                    || d.descriptor.key.kind != m::Kind::Commit
                    || d.descriptor.format_version != m::FORMAT_VERSION
                    || d.descriptor.size_bytes == 0
                    || d.descriptor.size_bytes > l.snapshot.max_metadata_bytes
                    || d.order == 0
                    || d.parents.is_empty()
                {
                    return Err(bad());
                }
                if d.parents.len() > l.snapshot.max_parents {
                    return Err(limit());
                }
                edges = add(edges, d.parents.len())?;
                if edges > l.snapshot.max_edges {
                    return Err(limit());
                }
                bytes = add(bytes, record_bytes(d)?)?;
            }
            bytes
        }
        IndexNodePayload::Branch(children) => {
            if node.depth == 64
                || node.records < 2
                || children.is_empty()
                || children.len() > 16
                || !children.windows(2).all(|p| p[0].slot < p[1].slot)
                || children.iter().any(|c| c.slot >= 16)
            {
                return Err(bad());
            }
            if node.records as usize <= LEAF_RECORDS
                && add(
                    HEADER,
                    usize::try_from(node.record_bytes).map_err(|_| limit())?,
                )? <= LEAF_BYTES
            {
                return Err(bad());
            }
            return Ok(add(HEADER, add(1, mul(children.len(), 33)?)?)?);
        }
    };
    if node.record_bytes != payload as u64 {
        return Err(bad());
    }
    let bytes = add(HEADER, payload)?;
    if node.records as usize > LEAF_RECORDS || (bytes > LEAF_BYTES && node.records != 1) {
        return Err(bad());
    }
    Ok(bytes)
}
fn artifact_size(a: &IndexArtifact, l: IndexArtifactLimits) -> Result<usize> {
    l.validate()?;
    let n = match a {
        IndexArtifact::Root(root) => {
            if root.anchor_count == 0
                || root.anchor_count as usize > l.snapshot.max_roots
                || root.draft_count as usize > l.snapshot.max_drafts
                || root.drafts.is_some() != (root.draft_count != 0)
            {
                return Err(bad());
            }
            if root.drafts.is_some() { 88 } else { 56 }
        }
        IndexArtifact::Node(node) => node_shape(node, l)?,
    };
    if n > l.max_artifact_bytes {
        return Err(limit());
    }
    Ok(n)
}
fn draft_write(out: &mut Vec<u8>, d: &IndexDraft) {
    out.extend_from_slice(&d.descriptor.key.id.0);
    out.extend_from_slice(&d.change.0);
    out.extend_from_slice(&d.descriptor.size_bytes.to_le_bytes());
    out.extend_from_slice(&d.descriptor.format_version.to_le_bytes());
    out.extend_from_slice(&d.order.to_le_bytes());
    out.extend_from_slice(&(d.parents.len() as u32).to_le_bytes());
    for p in &d.parents {
        out.extend_from_slice(&p.0);
    }
}
#[derive(Debug)]
pub struct EncodedIndexArtifact {
    pub descriptor: IndexArtifactDescriptor,
    pub bytes: Vec<u8>,
}
pub fn encode_index_artifact(
    value: &IndexArtifact,
    l: IndexArtifactLimits,
) -> Result<EncodedIndexArtifact> {
    let size = artifact_size(value, l)?;
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    match value {
        IndexArtifact::Root(root) => {
            bytes.push(0);
            bytes.extend_from_slice(&m::FORMAT_VERSION.to_le_bytes());
            bytes.extend_from_slice(&1u16.to_le_bytes());
            bytes.extend_from_slice(&root.anchor_count.to_le_bytes());
            bytes.extend_from_slice(&root.draft_count.to_le_bytes());
            bytes.extend_from_slice(&root.anchors.0);
            bytes.push(u8::from(root.drafts.is_some()));
            if let Some(id) = root.drafts {
                bytes.extend_from_slice(&id.0);
            }
        }
        IndexArtifact::Node(node) => {
            bytes.push(match (&node.collection, &node.payload) {
                (IndexCollection::Anchors, IndexNodePayload::Anchors(_)) => 1,
                (IndexCollection::Drafts, IndexNodePayload::Drafts(_)) => 2,
                (IndexCollection::Anchors, IndexNodePayload::Branch(_)) => 3,
                (IndexCollection::Drafts, IndexNodePayload::Branch(_)) => 4,
                _ => return Err(bad()),
            });
            bytes.push(node.depth);
            bytes.extend_from_slice(&node.prefix);
            bytes.extend_from_slice(&node.records.to_le_bytes());
            bytes.extend_from_slice(&node.record_bytes.to_le_bytes());
            match &node.payload {
                IndexNodePayload::Anchors(v) => {
                    for id in v {
                        bytes.extend_from_slice(&id.0)
                    }
                }
                IndexNodePayload::Drafts(v) => {
                    for d in v {
                        draft_write(&mut bytes, d)
                    }
                }
                IndexNodePayload::Branch(v) => {
                    bytes.push(v.len() as u8);
                    for c in v {
                        bytes.push(c.slot);
                        bytes.extend_from_slice(&c.node.0);
                    }
                }
            }
        }
    }
    debug_assert_eq!(bytes.len(), size);
    let id = match value {
        IndexArtifact::Root(_) => IndexArtifactId::of_root(&bytes),
        _ => IndexArtifactId::of_node(&bytes),
    };
    Ok(EncodedIndexArtifact {
        descriptor: IndexArtifactDescriptor {
            id,
            selected: BlobDigest::of(&bytes),
        },
        bytes,
    })
}
pub struct VerifiedIndexArtifact {
    value: IndexArtifact,
    descriptor: IndexArtifactDescriptor,
}
impl VerifiedIndexArtifact {
    pub fn artifact(&self) -> &IndexArtifact {
        &self.value
    }
    pub fn descriptor(&self) -> IndexArtifactDescriptor {
        self.descriptor
    }
    pub fn retained_size_upper_bound(&self) -> Result<usize> {
        let mut n = std::mem::size_of::<Self>();
        if let IndexArtifact::Node(node) = &self.value {
            match &node.payload {
                IndexNodePayload::Anchors(v) => n = add(n, mul(v.capacity(), 32)?)?,
                IndexNodePayload::Branch(v) => {
                    n = add(n, mul(v.capacity(), std::mem::size_of::<IndexChild>())?)?
                }
                IndexNodePayload::Drafts(v) => {
                    n = add(n, mul(v.capacity(), std::mem::size_of::<IndexDraft>())?)?;
                    for d in v {
                        n = add(n, mul(d.parents.capacity(), 32)?)?;
                    }
                }
            }
        }
        Ok(n)
    }
}
impl std::fmt::Debug for VerifiedIndexArtifact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedIndexArtifact")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}
struct Reader<'a> {
    bytes: &'a [u8],
    used: usize,
    maximum: usize,
}
impl Reader<'_> {
    fn raw<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (h, t) = self.bytes.split_at_checked(N).ok_or_else(bad)?;
        self.bytes = t;
        Ok(h.try_into().expect("fixed width"))
    }
    fn reserve<T>(&mut self, n: usize, min: usize, max: usize) -> Result<()> {
        if n > max || mul(n, min)? > self.bytes.len() {
            return Err(limit());
        }
        self.used = add(self.used, mul(n, std::mem::size_of::<T>())?)?;
        if self.used > self.maximum {
            return Err(limit());
        }
        Ok(())
    }
}
pub fn decode_index_artifact(
    bytes: &[u8],
    expected: IndexArtifactDescriptor,
    l: IndexArtifactLimits,
) -> Result<VerifiedIndexArtifact> {
    l.validate()?;
    if bytes.len() > l.max_artifact_bytes {
        return Err(limit());
    }
    let id = match expected.id {
        IndexArtifactId::Root(_) => IndexArtifactId::of_root(bytes),
        _ => IndexArtifactId::of_node(bytes),
    };
    if id != expected.id || expected.selected != BlobDigest::of(bytes) {
        return Err(bad());
    }
    if l.max_decode_bytes < 4096 {
        return Err(limit());
    }
    let mut r = Reader {
        bytes,
        used: 4096,
        maximum: l.max_decode_bytes,
    };
    if &r.raw::<8>()? != MAGIC {
        return Err(bad());
    }
    if u16::from_le_bytes(r.raw()?) != VERSION {
        return Err(Error::Unsupported("index artifact version"));
    }
    let kind = r.raw::<1>()?[0];
    let value = if kind == 0 {
        if !matches!(expected.id, IndexArtifactId::Root(_)) {
            return Err(bad());
        }
        if u16::from_le_bytes(r.raw()?) != m::FORMAT_VERSION || u16::from_le_bytes(r.raw()?) != 1 {
            return Err(Error::Unsupported("index artifact native/order policy"));
        }
        let anchor_count = u32::from_le_bytes(r.raw()?);
        let draft_count = u32::from_le_bytes(r.raw()?);
        let anchors = IndexNodeId(r.raw()?);
        let drafts = match r.raw::<1>()?[0] {
            0 => None,
            1 => Some(IndexNodeId(r.raw()?)),
            _ => return Err(bad()),
        };
        IndexArtifact::Root(IndexRoot {
            anchors,
            drafts,
            anchor_count,
            draft_count,
        })
    } else {
        if !matches!(expected.id, IndexArtifactId::Node(_)) {
            return Err(bad());
        }
        let collection = match kind {
            1 | 3 => IndexCollection::Anchors,
            2 | 4 => IndexCollection::Drafts,
            _ => return Err(bad()),
        };
        let depth = r.raw::<1>()?[0];
        let prefix = r.raw()?;
        let records = u32::from_le_bytes(r.raw()?);
        let record_bytes = u64::from_le_bytes(r.raw()?);
        let payload = match kind {
            1 => {
                r.reserve::<m::CommitId>(records as usize, 32, l.snapshot.max_roots)?;
                let mut v = Vec::with_capacity(records as usize);
                for _ in 0..records {
                    v.push(m::CommitId(r.raw()?));
                }
                IndexNodePayload::Anchors(v)
            }
            2 => {
                r.reserve::<IndexDraft>(records as usize, 102, l.snapshot.max_drafts)?;
                let mut v = Vec::with_capacity(records as usize);
                let mut edges = 0usize;
                for _ in 0..records {
                    let id = m::ObjectId(r.raw()?);
                    let change = m::ChangeId(r.raw()?);
                    let size_bytes = u64::from_le_bytes(r.raw()?);
                    let format_version = u16::from_le_bytes(r.raw()?);
                    let order = u64::from_le_bytes(r.raw()?);
                    let n = u32::from_le_bytes(r.raw()?) as usize;
                    edges = add(edges, n)?;
                    if edges > l.snapshot.max_edges {
                        return Err(limit());
                    }
                    r.reserve::<m::CommitId>(n, 32, l.snapshot.max_parents)?;
                    let mut parents = Vec::with_capacity(n);
                    for _ in 0..n {
                        parents.push(m::CommitId(r.raw()?));
                    }
                    v.push(IndexDraft {
                        descriptor: m::Descriptor {
                            key: m::ObjectKey {
                                kind: m::Kind::Commit,
                                id,
                            },
                            size_bytes,
                            format_version,
                        },
                        change,
                        parents,
                        order,
                    });
                }
                IndexNodePayload::Drafts(v)
            }
            _ => {
                let n = r.raw::<1>()?[0] as usize;
                r.reserve::<IndexChild>(n, 33, 16)?;
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(IndexChild {
                        slot: r.raw::<1>()?[0],
                        node: IndexNodeId(r.raw()?),
                    });
                }
                IndexNodePayload::Branch(v)
            }
        };
        IndexArtifact::Node(IndexNode {
            collection,
            depth,
            prefix,
            records,
            record_bytes,
            payload,
        })
    };
    if !r.bytes.is_empty() {
        return Err(bad());
    }
    artifact_size(&value, l)?;
    Ok(VerifiedIndexArtifact {
        value,
        descriptor: expected,
    })
}
#[derive(Debug)]
pub struct IndexArtifactSnapshot {
    pub root: IndexRootDescriptor,
    pub artifacts: Vec<EncodedIndexArtifact>,
}
struct Builder {
    artifacts: BTreeMap<IndexArtifactId, EncodedIndexArtifact>,
    bytes: u64,
    limits: IndexArtifactLimits,
}
impl Builder {
    fn emit(&mut self, value: IndexArtifact) -> Result<IndexArtifactId> {
        let size = artifact_size(&value, self.limits)?;
        if self.artifacts.len() >= self.limits.max_artifacts
            || self
                .bytes
                .checked_add(size as u64)
                .is_none_or(|n| n > self.limits.max_total_artifact_bytes)
        {
            return Err(limit());
        }
        let encoded = encode_index_artifact(&value, self.limits)?;
        let id = encoded.descriptor.id;
        if let Some(old) = self.artifacts.get(&id) {
            if old.bytes != encoded.bytes {
                return Err(bad());
            }
        } else {
            self.bytes += size as u64;
            self.artifacts.insert(id, encoded);
        }
        Ok(id)
    }
    fn node(
        &mut self,
        collection: IndexCollection,
        keys: &[[u8; 32]],
        drafts: Option<&[IndexDraft]>,
        depth: u8,
    ) -> Result<IndexNodeId> {
        if keys.is_empty() {
            return Err(bad());
        }
        let record_bytes = if let Some(drafts) = drafts {
            drafts
                .iter()
                .try_fold(0usize, |n, d| add(n, record_bytes(d)?))?
        } else {
            mul(keys.len(), 32)?
        };
        let prefix = prefix(keys[0], depth);
        let leaf = keys.len() <= LEAF_RECORDS
            && (add(HEADER, record_bytes)? <= LEAF_BYTES || keys.len() == 1);
        let payload = if leaf {
            match drafts {
                Some(v) => IndexNodePayload::Drafts(v.to_vec()),
                None => IndexNodePayload::Anchors(keys.iter().copied().map(m::CommitId).collect()),
            }
        } else {
            if depth == 64 {
                return Err(bad());
            }
            let mut children = Vec::new();
            let mut first = 0;
            while first < keys.len() {
                let slot = nibble(&keys[first], depth);
                let mut end = first + 1;
                while end < keys.len() && nibble(&keys[end], depth) == slot {
                    end += 1;
                }
                let node = self.node(
                    collection,
                    &keys[first..end],
                    drafts.map(|v| &v[first..end]),
                    depth + 1,
                )?;
                children.push(IndexChild { slot, node });
                first = end;
            }
            IndexNodePayload::Branch(children)
        };
        let id = self.emit(IndexArtifact::Node(IndexNode {
            collection,
            depth,
            prefix,
            records: keys.len() as u32,
            record_bytes: record_bytes as u64,
            payload,
        }))?;
        let IndexArtifactId::Node(id) = id else {
            return Err(bad());
        };
        Ok(id)
    }
}
/// O(N) model rebuilding is intentional initially. Stable nodes provide byte
/// reuse across operation roots without claiming incremental CPU performance.
pub fn build_index_artifacts(
    value: &PortableIndexSnapshot,
    l: IndexArtifactLimits,
) -> Result<IndexArtifactSnapshot> {
    l.validate()?;
    crate::index_snapshot::shape(value, l.snapshot)?;
    let required = add(
        l.snapshot.max_decode_bytes,
        add(
            usize::try_from(l.max_total_artifact_bytes).map_err(|_| limit())?,
            add(mul(l.max_artifacts, 512)?, mul(l.max_artifact_bytes, 2)?)?,
        )?,
    )?;
    if required > l.max_decode_bytes {
        return Err(limit());
    }
    let mut builder = Builder {
        artifacts: BTreeMap::new(),
        bytes: 0,
        limits: l,
    };
    let keys: Vec<_> = value.roots.iter().map(|id| id.0).collect();
    let anchors = builder.node(IndexCollection::Anchors, &keys, None, 0)?;
    let drafts = if value.drafts.is_empty() {
        None
    } else {
        let keys: Vec<_> = value.drafts.iter().map(|d| d.commit().0).collect();
        Some(builder.node(IndexCollection::Drafts, &keys, Some(&value.drafts), 0)?)
    };
    let id = builder.emit(IndexArtifact::Root(IndexRoot {
        anchors,
        drafts,
        anchor_count: value.roots.len() as u32,
        draft_count: value.drafts.len() as u32,
    }))?;
    let IndexArtifactId::Root(id) = id else {
        return Err(bad());
    };
    let selected = builder.artifacts[&IndexArtifactId::Root(id)]
        .descriptor
        .selected;
    Ok(IndexArtifactSnapshot {
        root: IndexRootDescriptor { id, selected },
        artifacts: builder.artifacts.into_values().collect(),
    })
}
struct Walk<'a> {
    table: &'a BTreeMap<IndexArtifactId, &'a VerifiedIndexArtifact>,
    visited: &'a mut BTreeSet<IndexArtifactId>,
    steps: &'a mut usize,
    limits: IndexArtifactLimits,
    expected_anchors: usize,
    expected_drafts: usize,
    model_bytes: usize,
    model_memory: usize,
}
impl Walk<'_> {
    fn tick(&mut self, n: usize) -> Result<()> {
        *self.steps = add(*self.steps, n)?;
        if *self.steps > self.limits.max_graph_steps {
            return Err(limit());
        }
        Ok(())
    }
    fn node(
        &mut self,
        id: IndexNodeId,
        collection: IndexCollection,
        depth: u8,
        wanted: [u8; 32],
        out: &mut PortableIndexSnapshot,
    ) -> Result<(u32, u64)> {
        self.tick(1)?;
        let key = IndexArtifactId::Node(id);
        let artifact = *self.table.get(&key).ok_or_else(bad)?;
        self.visited.insert(key);
        let IndexArtifact::Node(node) = artifact.artifact() else {
            return Err(bad());
        };
        if node.collection != collection || node.depth != depth || node.prefix != wanted {
            return Err(bad());
        }
        match &node.payload {
            IndexNodePayload::Anchors(v) => {
                self.tick(v.len())?;
                if add(out.roots.len(), v.len())? > self.expected_anchors {
                    return Err(limit());
                }
                self.model_bytes = add(self.model_bytes, mul(v.len(), 32)?)?;
                if self.model_bytes > self.limits.snapshot.max_encoded_bytes {
                    return Err(limit());
                }
                out.roots.extend(v);
            }
            IndexNodePayload::Drafts(v) => {
                self.tick(v.len())?;
                if add(out.drafts.len(), v.len())? > self.expected_drafts {
                    return Err(limit());
                }
                for d in v {
                    self.tick(d.parents.len())?;
                    self.model_bytes = add(self.model_bytes, record_bytes(d)?)?;
                    self.model_memory = add(self.model_memory, mul(d.parents.len(), 32)?)?;
                    if self.model_bytes > self.limits.snapshot.max_encoded_bytes
                        || self.model_memory > self.limits.snapshot.max_decode_bytes
                    {
                        return Err(limit());
                    }
                }
                out.drafts.extend(v.iter().cloned());
            }
            IndexNodePayload::Branch(v) => {
                let (mut count, mut bytes) = (0u32, 0u64);
                for child in v {
                    let mut wanted = wanted;
                    let offset = (depth / 2) as usize;
                    if depth % 2 == 0 {
                        wanted[offset] = child.slot << 4;
                    } else {
                        wanted[offset] |= child.slot;
                    }
                    let (c, b) = self.node(child.node, collection, depth + 1, wanted, out)?;
                    count = count.checked_add(c).ok_or_else(limit)?;
                    bytes = bytes.checked_add(b).ok_or_else(limit)?;
                }
                if count != node.records || bytes != node.record_bytes {
                    return Err(bad());
                }
            }
        };
        Ok((node.records, node.record_bytes))
    }
    fn snapshot(&mut self, descriptor: IndexRootDescriptor) -> Result<PortableIndexSnapshot> {
        self.tick(1)?;
        let key = IndexArtifactId::Root(descriptor.id);
        let artifact = *self.table.get(&key).ok_or_else(bad)?;
        if artifact.descriptor().selected != descriptor.selected {
            return Err(bad());
        }
        self.visited.insert(key);
        let IndexArtifact::Root(root) = artifact.artifact() else {
            return Err(bad());
        };
        self.expected_anchors = root.anchor_count as usize;
        self.expected_drafts = root.draft_count as usize;
        self.model_bytes = 22;
        self.model_memory = add(
            4096,
            add(
                mul(self.expected_anchors, 32)?,
                mul(
                    self.expected_drafts,
                    std::mem::size_of::<IndexDraft>() + 512,
                )?,
            )?,
        )?;
        if self.model_memory > self.limits.snapshot.max_decode_bytes {
            return Err(limit());
        }
        let mut model = PortableIndexSnapshot {
            roots: Vec::with_capacity(self.expected_anchors),
            drafts: Vec::with_capacity(self.expected_drafts),
        };
        let (count, _) = self.node(
            root.anchors,
            IndexCollection::Anchors,
            0,
            [0; 32],
            &mut model,
        )?;
        if count != root.anchor_count {
            return Err(bad());
        }
        if let Some(id) = root.drafts {
            let (count, _) = self.node(id, IndexCollection::Drafts, 0, [0; 32], &mut model)?;
            if count != root.draft_count {
                return Err(bad());
            }
        }
        crate::index_snapshot::shape(&model, self.limits.snapshot)?;
        Ok(model)
    }
}
/// Borrowed immutable artifact proof; it is not native canonical-byte,
/// authorization, remote-retention or client-fencing proof.
pub struct VerifiedIndexGraph<'a> {
    roots: &'a [IndexRootDescriptor],
    table: BTreeMap<IndexArtifactId, &'a VerifiedIndexArtifact>,
    artifacts: &'a [VerifiedIndexArtifact],
    limits: IndexArtifactLimits,
}
impl VerifiedIndexGraph<'_> {
    pub fn roots(&self) -> &[IndexRootDescriptor] {
        self.roots
    }
    pub fn validate_for_limits(&self, limits: IndexArtifactLimits) -> Result<()> {
        verify_index_artifact_graph(self.roots, self.artifacts, limits).map(|_| ())
    }
    pub fn snapshot(&self, root: IndexRootId) -> Result<PortableIndexSnapshot> {
        let descriptor = *self.roots.iter().find(|d| d.id == root).ok_or_else(bad)?;
        let mut visited = BTreeSet::new();
        let mut steps = 0;
        Walk {
            table: &self.table,
            visited: &mut visited,
            steps: &mut steps,
            limits: self.limits,
            expected_anchors: 0,
            expected_drafts: 0,
            model_bytes: 0,
            model_memory: 0,
        }
        .snapshot(descriptor)
    }
}
/// The supplied artifact set must equal the dependency closure of these exact
/// roots: missing, duplicated, incorrectly placed and unrelated artifacts fail.
pub fn verify_index_artifact_graph<'a>(
    roots: &'a [IndexRootDescriptor],
    artifacts: &'a [VerifiedIndexArtifact],
    l: IndexArtifactLimits,
) -> Result<VerifiedIndexGraph<'a>> {
    l.validate()?;
    if roots.is_empty()
        || roots.len() > l.max_snapshots
        || artifacts.len() > l.max_artifacts
        || !roots.windows(2).all(|p| p[0].id < p[1].id)
    {
        return Err(bad());
    }
    let mut total = 0u64;
    let mut memory = add(
        4096,
        add(mul(artifacts.len(), 512)?, l.snapshot.max_decode_bytes)?,
    )?;
    for a in artifacts {
        artifact_size(a.artifact(), l)?;
        total = total
            .checked_add(a.descriptor().selected.size_bytes)
            .filter(|n| *n <= l.max_total_artifact_bytes)
            .ok_or_else(limit)?;
        memory = add(memory, a.retained_size_upper_bound()?)?;
        if memory > l.max_decode_bytes {
            return Err(limit());
        }
    }
    let mut table = BTreeMap::new();
    for a in artifacts {
        if table.insert(a.descriptor().id, a).is_some() {
            return Err(bad());
        }
    }
    let mut visited = BTreeSet::new();
    let mut steps = 0;
    for root in roots {
        Walk {
            table: &table,
            visited: &mut visited,
            steps: &mut steps,
            limits: l,
            expected_anchors: 0,
            expected_drafts: 0,
            model_bytes: 0,
            model_memory: 0,
        }
        .snapshot(*root)?;
    }
    if visited.len() != table.len() {
        return Err(bad());
    }
    Ok(VerifiedIndexGraph {
        roots,
        table,
        artifacts,
        limits: l,
    })
}
