//! Deterministic tree-entry rebasing, not a landing/publication service.
//!
//! Callers must authorize, retain and strictly validate complete closures before
//! use. Equal subtrees are deliberately not opened; this builder is not proof of
//! their availability, FILE sizes or copy-history ancestry. No FILE body is read.
use std::{
    collections::{HashMap, HashSet},
    fmt,
};
use tinytree_model as m;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadError {
    Missing,
    Unavailable,
    Limit,
}

/// Return canonical metadata only. Implementations must enforce `max_bytes`
/// before allocating/reading a payload, and honor their caller's deadline.
/// Returned bytes are independently authenticated and decoded by this crate.
pub trait MetadataReader {
    fn read(&mut self, key: m::ObjectKey, max_bytes: usize) -> Result<Vec<u8>, ReadError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Budget {
    Configuration,
    Stack,
    Ancestry,
    Reads,
    MetadataBytes,
    Work,
    Depth,
    Path,
    Entries,
    OutputObjects,
    OutputBytes,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Limit(Budget),
    UnsupportedHistory(&'static str),
    Invalid(&'static str),
    Conflict { path: String },
    Read { key: m::ObjectKey, error: ReadError },
    Object(m::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Limit(b) => write!(f, "landing candidate limit: {b:?}"),
            Self::UnsupportedHistory(s) => write!(f, "unsupported landing history: {s}"),
            Self::Invalid(s) => write!(f, "invalid landing candidate: {s}"),
            Self::Conflict { path } => write!(f, "landing tree conflict at {path}"),
            Self::Read { error, .. } => write!(f, "landing metadata read failed: {error:?}"),
            Self::Object(e) => write!(f, "invalid landing metadata: {e}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<m::Error> for Error {
    fn from(value: m::Error) -> Self {
        Self::Object(value)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_stack: usize,
    pub max_ancestry: usize,
    pub max_reads: usize,
    pub max_metadata_bytes: usize,
    pub max_work: usize,
    pub max_depth: usize,
    pub max_path_bytes: usize,
    pub max_entries_per_tree: usize,
    pub max_object_bytes: usize,
    pub max_field_bytes: usize,
    pub max_output_objects: usize,
    pub max_output_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_stack: 64,
            max_ancestry: 4096,
            max_reads: 20_000,
            max_metadata_bytes: 64 * 1024 * 1024,
            max_work: 1_000_000,
            max_depth: 128,
            max_path_bytes: 4096,
            max_entries_per_tree: 100_000,
            max_object_bytes: 16 * 1024 * 1024,
            max_field_bytes: 1024 * 1024,
            max_output_objects: 4096,
            max_output_bytes: 32 * 1024 * 1024,
        }
    }
}
impl Limits {
    pub fn validate(self) -> Result<(), Error> {
        if self.max_stack == 0
            || self.max_stack > 4096
            || self.max_ancestry == 0
            || self.max_ancestry > 1_000_000
            || self.max_reads == 0
            || self.max_reads > 1_000_000
            || self.max_metadata_bytes == 0
            || self.max_metadata_bytes > 1024 * 1024 * 1024
            || self.max_work == 0
            || self.max_work > 100_000_000
            || self.max_depth == 0
            || self.max_depth > 128
            || self.max_path_bytes == 0
            || self.max_path_bytes > 64 * 1024
            || self.max_entries_per_tree == 0
            || self.max_entries_per_tree > 1_000_000
            || self.max_object_bytes == 0
            || self.max_object_bytes > 64 * 1024 * 1024
            || self.max_field_bytes == 0
            || self.max_field_bytes > self.max_object_bytes
            || self.max_output_objects == 0
            || self.max_output_objects > 1_000_000
            || self.max_output_bytes == 0
            || self.max_output_bytes > 1024 * 1024 * 1024
        {
            return Err(Error::Limit(Budget::Configuration));
        }
        Ok(())
    }
    fn model(self, bytes: usize) -> m::Limits {
        m::Limits {
            max_object_bytes: self.max_object_bytes.min(bytes),
            max_items: self.max_entries_per_tree,
            max_field_bytes: self.max_field_bytes.min(bytes),
            max_path_bytes: self.max_path_bytes,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    pub source_base: m::CommitId,
    pub source_head: m::CommitId,
    pub destination_head: m::CommitId,
    /// Frozen by a durable caller before construction; no clock is read here.
    pub committer: m::Signature,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// An unchanged identity is a valid fast-forward result, not an evolution edge.
/// Callers must skip `old == new` when importing rewrite/evolution relations.
pub struct Rewrite {
    pub old: m::CommitId,
    pub new: m::CommitId,
    pub change_id: m::ChangeId,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub metadata_reads: usize,
    pub metadata_bytes: usize,
    pub work: usize,
    pub output_bytes: usize,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub head: m::CommitId,
    pub root_tree: m::TreeId,
    /// Parent-before-child stack order, including empty changes.
    pub rewrites: Vec<Rewrite>,
    /// Constructed objects, deduplicated by ID. Equal subtrees are shared, not copied.
    pub objects: Vec<m::EncodedObject>,
    pub stats: Stats,
}

pub fn build(
    reader: &mut impl MetadataReader,
    request: Request,
    limits: Limits,
) -> Result<Candidate, Error> {
    limits.validate()?;
    if request.committer.name.len() > limits.max_field_bytes
        || request.committer.email.len() > limits.max_field_bytes
    {
        return Err(Error::Limit(Budget::MetadataBytes));
    }
    let mut builder = Builder {
        reader,
        limits,
        stats: Stats::default(),
        objects: Vec::new(),
        generated: HashMap::new(),
        assembling_bytes: 0,
    };
    builder.build(request, None, false)
}

/// Rebase a patch whose source base has an explicitly verified rewritten parent.
/// The caller must establish the exact source-base -> mapped-base relation from
/// retained authoritative provenance. A matching ChangeId alone is not authority.
/// Ordinary build keeps its original common-ancestor requirement unchanged.
pub fn build_on_mapped_base(
    reader: &mut impl MetadataReader,
    request: Request,
    mapped_base: m::CommitId,
    limits: Limits,
) -> Result<Candidate, Error> {
    limits.validate()?;
    if request.committer.name.len() > limits.max_field_bytes
        || request.committer.email.len() > limits.max_field_bytes
    {
        return Err(Error::Limit(Budget::MetadataBytes));
    }
    let mut builder = Builder {
        reader,
        limits,
        stats: Stats::default(),
        objects: Vec::new(),
        generated: HashMap::new(),
        assembling_bytes: 0,
    };
    builder.build(request, Some(mapped_base), false)
}

/// Rebase from a sanitized snapshot whose canonical baseline was verified by
/// the caller's trusted projection ledger. Never accept this mapping from clients.
pub fn build_on_projected_base(
    reader: &mut impl MetadataReader,
    request: Request,
    canonical_base: m::CommitId,
    limits: Limits,
) -> Result<Candidate, Error> {
    limits.validate()?;
    if request.committer.name.len() > limits.max_field_bytes
        || request.committer.email.len() > limits.max_field_bytes
    {
        return Err(Error::Limit(Budget::MetadataBytes));
    }
    let mut builder = Builder {
        reader,
        limits,
        stats: Stats::default(),
        objects: Vec::new(),
        generated: HashMap::new(),
        assembling_bytes: 0,
    };
    builder.build(request, Some(canonical_base), true)
}

#[derive(Clone, Copy)]
enum BaselineMapping {
    Projected(m::TreeId),
    Rewritten(m::TreeId),
}
impl BaselineMapping {
    fn tree(self) -> m::TreeId {
        match self {
            Self::Projected(tree) | Self::Rewritten(tree) => tree,
        }
    }
    fn at(self, tree: m::TreeId) -> Self {
        match self {
            Self::Projected(_) => Self::Projected(tree),
            Self::Rewritten(_) => Self::Rewritten(tree),
        }
    }
    fn is_projected(self) -> bool {
        matches!(self, Self::Projected(_))
    }
}

struct Builder<'a, R> {
    reader: &'a mut R,
    limits: Limits,
    stats: Stats,
    objects: Vec<m::EncodedObject>,
    generated: HashMap<m::ObjectKey, usize>,
    assembling_bytes: usize,
}
fn key(kind: m::Kind, id: [u8; 32]) -> m::ObjectKey {
    m::ObjectKey {
        kind,
        id: m::ObjectId(id),
    }
}
fn charge(value: &mut usize, amount: usize, max: usize, budget: Budget) -> Result<(), Error> {
    *value = value
        .checked_add(amount)
        .filter(|n| *n <= max)
        .ok_or(Error::Limit(budget))?;
    Ok(())
}
impl<R: MetadataReader> Builder<'_, R> {
    fn work(&mut self, count: usize) -> Result<(), Error> {
        charge(
            &mut self.stats.work,
            count,
            self.limits.max_work,
            Budget::Work,
        )
    }
    fn read(&mut self, key: m::ObjectKey) -> Result<m::Object, Error> {
        self.work(1)?;
        charge(
            &mut self.stats.metadata_reads,
            1,
            self.limits.max_reads,
            Budget::Reads,
        )?;
        let remaining = self.limits.max_metadata_bytes - self.stats.metadata_bytes;
        let max = remaining.min(self.limits.max_object_bytes);
        if max == 0 {
            return Err(Error::Limit(Budget::MetadataBytes));
        }
        let bytes = if let Some(index) = self.generated.get(&key) {
            let bytes = &self.objects[*index].bytes;
            if bytes.len() > max {
                return Err(Error::Limit(Budget::MetadataBytes));
            }
            bytes.clone()
        } else {
            self.reader
                .read(key, max)
                .map_err(|error| Error::Read { key, error })?
        };
        if bytes.len() > max {
            return Err(Error::Limit(Budget::MetadataBytes));
        }
        charge(
            &mut self.stats.metadata_bytes,
            bytes.len(),
            self.limits.max_metadata_bytes,
            Budget::MetadataBytes,
        )?;
        if *blake3::hash(&bytes).as_bytes() != key.id.0 {
            return Err(Error::Object(m::Error::HashMismatch));
        }
        let mut model_limits = self.limits.model(max);
        if self.stats.work == self.limits.max_work {
            return Err(Error::Limit(Budget::Work));
        }
        model_limits.max_items = model_limits
            .max_items
            .min(self.limits.max_work - self.stats.work);
        let object = m::decode(&bytes, model_limits)?;
        let items = match &object {
            m::Object::Tree(t) => t.entries.len(),
            m::Object::Commit(c) => {
                c.parents.len()
                    + c.predecessors.len()
                    + c.root_tree.terms.len()
                    + c.root_tree.labels.len()
            }
            m::Object::CopyHistory(c) => c.parents.len(),
            m::Object::Symlink(_) => 0,
        };
        self.work(items)?;
        if object.kind() != key.kind {
            return Err(Error::Invalid("metadata kind"));
        }
        Ok(object)
    }
    fn commit(&mut self, id: m::CommitId) -> Result<m::Commit, Error> {
        if id == m::CommitId::ROOT {
            return Ok(m::synthetic_root());
        }
        match self.read(key(m::Kind::Commit, id.0))? {
            m::Object::Commit(c) => Ok(c),
            _ => Err(Error::Invalid("commit kind")),
        }
    }
    fn tree(&mut self, id: m::TreeId) -> Result<m::Tree, Error> {
        match self.read(key(m::Kind::Tree, id.0))? {
            m::Object::Tree(t) => Ok(t),
            _ => Err(Error::Invalid("tree kind")),
        }
    }
    fn root(c: &m::Commit) -> Result<m::TreeId, Error> {
        if c.root_tree.terms.len() != 1 {
            return Err(Error::UnsupportedHistory("unresolved tree"));
        }
        Ok(c.root_tree.terms[0])
    }
    fn emit(&mut self, object: m::Object) -> Result<m::ObjectKey, Error> {
        if self.objects.len() >= self.limits.max_output_objects {
            return Err(Error::Limit(Budget::OutputObjects));
        }
        let remaining =
            self.limits.max_output_bytes - self.stats.output_bytes - self.assembling_bytes;
        let encoded = m::encode(&object, self.limits.model(remaining)).map_err(|e| {
            if e == m::Error::Limit && remaining < self.limits.max_object_bytes {
                Error::Limit(Budget::OutputBytes)
            } else {
                Error::Object(e)
            }
        })?;
        let key = encoded.descriptor.key;
        if !self.generated.contains_key(&key) {
            charge(
                &mut self.stats.output_bytes,
                encoded.bytes.len(),
                self.limits.max_output_bytes,
                Budget::OutputBytes,
            )?;
            self.generated.insert(key, self.objects.len());
            self.objects.push(encoded);
        }
        Ok(key)
    }
    fn build(
        &mut self,
        request: Request,
        mapped_base: Option<m::CommitId>,
        projected: bool,
    ) -> Result<Candidate, Error> {
        let mut chain = Vec::new();
        let mut seen = HashSet::new();
        let mut changes = HashSet::new();
        let mut cursor = request.source_head;
        while cursor != request.source_base {
            if chain.len() >= self.limits.max_stack {
                return Err(Error::Limit(Budget::Stack));
            }
            if cursor == m::CommitId::ROOT || !seen.insert(cursor) {
                return Err(Error::Invalid("source base is not an ancestor"));
            }
            let commit = self.commit(cursor)?;
            if commit.parents.len() != 1 {
                return Err(Error::UnsupportedHistory("source merge"));
            }
            if commit.secure_signature.is_some() {
                return Err(Error::UnsupportedHistory("signed source commit"));
            }
            Self::root(&commit)?;
            if !changes.insert(commit.change_id) {
                return Err(Error::Invalid("duplicate source ChangeId"));
            }
            let old = cursor;
            cursor = commit.parents[0];
            chain.push((old, commit));
        }
        if chain.is_empty() {
            return Err(Error::Invalid("empty source stack"));
        }
        chain.reverse();
        let mut visits = 0;
        let mut destination = request.destination_head;
        let mut destination_seen = HashSet::new();
        while destination != mapped_base.unwrap_or(request.source_base) {
            charge(&mut visits, 1, self.limits.max_ancestry, Budget::Ancestry)?;
            if destination == m::CommitId::ROOT || !destination_seen.insert(destination) {
                return Err(Error::Invalid("base is not on destination ancestry"));
            }
            let c = self.commit(destination)?;
            if seen.contains(&destination) || changes.contains(&c.change_id) {
                return Err(Error::UnsupportedHistory(
                    "source change already on destination",
                ));
            }
            Self::root(&c)?;
            if c.parents.len() != 1 {
                return Err(Error::UnsupportedHistory("destination merge"));
            }
            destination = c.parents[0];
        }
        let base = self.commit(request.source_base)?;
        let mapped_tree = if let Some(mapped) = mapped_base {
            if mapped == m::CommitId::ROOT {
                return Err(Error::Invalid("mapped source base identity"));
            }
            let rewritten_base = self.commit(mapped)?;
            if !projected && rewritten_base.change_id != base.change_id {
                return Err(Error::Invalid("mapped source base identity"));
            }
            Some(Self::root(&rewritten_base)?)
        } else {
            None
        };
        if changes.contains(&base.change_id) {
            return Err(Error::Invalid("source ChangeId equals base ChangeId"));
        }
        let mut base_tree = Self::root(&base)?;
        let destination = self.commit(request.destination_head)?;
        let mut destination_tree = Self::root(&destination)?;
        let mut parent = request.destination_head;
        let mut rewrites = Vec::with_capacity(chain.len());
        let mut mapping = mapped_tree.map(|tree| {
            if projected {
                BaselineMapping::Projected(tree)
            } else {
                BaselineMapping::Rewritten(tree)
            }
        });
        for (old, source) in chain {
            let source_tree = Self::root(&source)?;
            let merged =
                self.merge_inner(base_tree, source_tree, destination_tree, "", 0, mapping)?;
            // Subsequent source commits map to their exact rewritten predecessor,
            // not back to the initial sanitized projection's baseline.
            mapping = mapping.map(|_| BaselineMapping::Rewritten(merged));
            let rewritten = m::Commit {
                parents: vec![parent],
                root_tree: m::TreeMerge {
                    terms: vec![merged],
                    labels: vec![String::new()],
                },
                committer: request.committer.clone(),
                ..source
            };
            let change_id = rewritten.change_id;
            let new = m::CommitId(self.emit(m::Object::Commit(rewritten))?.id.0);
            rewrites.push(Rewrite {
                old,
                new,
                change_id,
            });
            base_tree = source_tree;
            destination_tree = merged;
            parent = new;
        }
        Ok(Candidate {
            head: parent,
            root_tree: destination_tree,
            rewrites,
            objects: std::mem::take(&mut self.objects),
            stats: self.stats,
        })
    }
    fn path(&self, parent: &str, name: &str) -> Result<String, Error> {
        let len = parent
            .len()
            .checked_add(name.len())
            .and_then(|n| n.checked_add(usize::from(!parent.is_empty())))
            .filter(|n| *n <= self.limits.max_path_bytes)
            .ok_or(Error::Limit(Budget::Path))?;
        let mut path = String::with_capacity(len);
        if !parent.is_empty() {
            path.push_str(parent);
            path.push('/');
        }
        path.push_str(name);
        Ok(path)
    }
    fn copy(&mut self, value: &m::TreeValue, path: &str) -> Result<(), Error> {
        if let m::TreeValue::File {
            copy_id: Some(id), ..
        } = value
        {
            match self.read(key(m::Kind::CopyHistory, id.0))? {
                m::Object::CopyHistory(c) if c.current_path == path => {}
                _ => return Err(Error::Invalid("copy history current path")),
            }
        }
        Ok(())
    }
    fn reserve_assembly(&mut self, amount: usize) -> Result<(), Error> {
        charge(
            &mut self.assembling_bytes,
            amount,
            self.limits.max_output_bytes - self.stats.output_bytes,
            Budget::OutputBytes,
        )
    }
    fn merge_inner(
        &mut self,
        base: m::TreeId,
        source: m::TreeId,
        dest: m::TreeId,
        path: &str,
        depth: usize,
        mapping: Option<BaselineMapping>,
    ) -> Result<m::TreeId, Error> {
        self.work(1)?;
        if source == base {
            return Ok(dest);
        }
        if dest == base || source == dest {
            return Ok(source);
        }
        if depth >= self.limits.max_depth {
            return Err(Error::Limit(Budget::Depth));
        }
        let trees = [self.tree(base)?, self.tree(source)?, self.tree(dest)?];
        let canonical_tree = mapping.map(|m| self.tree(m.tree())).transpose()?;
        let mut positions = [0usize; 3];
        let mut output = Vec::new();
        // Conservative v1 envelope/entry allowance bounds unfinished parent
        // trees as well as retained encoded children before cloning names.
        let mut assembly = 128;
        self.reserve_assembly(assembly)?;
        loop {
            let entries = std::array::from_fn::<_, 3, _>(|i| trees[i].entries.get(positions[i]));
            let Some(name) = entries.iter().flatten().map(|e| e.name.as_str()).min() else {
                break;
            };
            self.work(1)?;
            let originals = std::array::from_fn::<_, 3, _>(|i| {
                entries[i]
                    .filter(|e| e.name == name)
                    .map(|e| e.value.clone())
            });
            let mapped = canonical_tree
                .as_ref()
                .and_then(|t| {
                    t.entries
                        .binary_search_by(|e| e.name.as_str().cmp(name))
                        .ok()
                        .map(|i| &t.entries[i])
                })
                .map(|e| &e.value);
            let mut normalized = originals.clone();
            if let (
                Some(m::TreeValue::File {
                    id: a,
                    executable: ae,
                    size_bytes: az,
                    copy_id: projected_copy,
                }),
                Some(m::TreeValue::File {
                    id: b,
                    executable: be,
                    size_bytes: bz,
                    copy_id: canonical_copy,
                }),
            ) = (&originals[0], mapped)
            {
                if a == b && ae == be && az == bz {
                    normalized[0] = mapped.cloned();
                    if originals[1] == originals[0] {
                        normalized[1] = mapped.cloned();
                    } else if let Some(m::TreeValue::File { copy_id, .. }) = &mut normalized[1] {
                        // A same-path edit retains the baseline identity. Translate
                        // only that identity, preserving submitted bytes and mode.
                        if *copy_id == *projected_copy {
                            *copy_id = *canonical_copy;
                        }
                    }
                } else if mapping.is_some_and(BaselineMapping::is_projected) {
                    return Err(Error::Invalid("projected baseline file mapping"));
                }
                // A rewritten predecessor may contain concurrent content/mode
                // changes. Non-equivalent FILEs retain ordinary three-way rules.
            }
            let values = std::array::from_fn::<_, 3, _>(|i| normalized[i].as_ref());
            let child_path = self.path(path, name)?;
            let chosen = if values[1] == values[0] {
                values[2].cloned()
            } else if values[2] == values[0] || values[1] == values[2] {
                values[1].cloned()
            } else if let [
                Some(m::TreeValue::Tree(b)),
                Some(m::TreeValue::Tree(s)),
                Some(m::TreeValue::Tree(d)),
            ] = values
            {
                Some(m::TreeValue::Tree(self.merge_inner(
                    *b,
                    *s,
                    *d,
                    &child_path,
                    depth + 1,
                    match mapped {
                        Some(m::TreeValue::Tree(id)) => mapping.map(|m| m.at(*id)),
                        _ => None,
                    },
                )?))
            } else if mapping.is_some_and(BaselineMapping::is_projected)
                && matches!(values[0], Some(m::TreeValue::Tree(_)) | None)
                && matches!(values[1], Some(m::TreeValue::Tree(_)) | None)
                && matches!(values[2], Some(m::TreeValue::Tree(_)))
            {
                let empty = m::TreeId(
                    self.emit(m::Object::Tree(m::Tree {
                        entries: Vec::new(),
                    }))?
                    .id
                    .0,
                );
                let tree_id = |value: Option<&m::TreeValue>| match value {
                    Some(m::TreeValue::Tree(id)) => *id,
                    _ => empty,
                };
                let merged = self.merge_inner(
                    tree_id(values[0]),
                    tree_id(values[1]),
                    tree_id(values[2]),
                    &child_path,
                    depth + 1,
                    match mapped {
                        Some(m::TreeValue::Tree(id)) => mapping.map(|m| m.at(*id)),
                        _ => mapping.map(|m| m.at(empty)),
                    },
                )?;
                if self.tree(merged)?.entries.is_empty() {
                    None
                } else {
                    Some(m::TreeValue::Tree(merged))
                }
            } else {
                return Err(Error::Conflict { path: child_path });
            };
            if let Some(value) = chosen {
                if output.len() >= self.limits.max_entries_per_tree {
                    return Err(Error::Limit(Budget::Entries));
                }
                self.copy(&value, &child_path)?;
                let entry_bytes = name
                    .len()
                    .checked_add(128)
                    .ok_or(Error::Limit(Budget::OutputBytes))?;
                self.reserve_assembly(entry_bytes)?;
                assembly += entry_bytes;
                output.push(m::TreeEntry {
                    name: name.to_owned(),
                    value,
                });
            }
            for i in 0..3 {
                if entries[i].is_some_and(|e| e.name == name) {
                    positions[i] += 1;
                }
            }
        }
        self.assembling_bytes -= assembly;
        Ok(m::TreeId(
            self.emit(m::Object::Tree(m::Tree { entries: output }))?
                .id
                .0,
        ))
    }
}

#[cfg(test)]
mod tests;
