//! Native-only canonical objects. This is not protobuf or upstream Commit serde.
//! No storage, transport, signing-key access, Git conversion or GC is performed.
use std::fmt;

pub const FORMAT_VERSION: u16 = 1;
const MAGIC: &[u8; 8] = b"TTREE\0\0\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid(&'static str),
    Limit,
    UnsupportedVersion,
    UnsupportedKind,
    UnsupportedGitSubmodule,
    SizeMismatch,
    HashMismatch,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid native object: {reason}"),
            Self::Limit => f.write_str("native object limit exceeded"),
            Self::UnsupportedVersion => f.write_str("unsupported native object version"),
            Self::UnsupportedKind => f.write_str("unsupported native object kind"),
            Self::UnsupportedGitSubmodule => {
                f.write_str("GitSubmodule is unsupported by native format v1")
            }
            Self::SizeMismatch => f.write_str("native object size mismatch"),
            Self::HashMismatch => f.write_str("native object hash mismatch"),
        }
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

macro_rules! id {
    ($name:ident, $size:literal) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub [u8; $size]);
        impl $name {
            pub fn from_slice(bytes: &[u8]) -> Result<Self> {
                Ok(Self(
                    bytes.try_into().map_err(|_| Error::Invalid("ID width"))?,
                ))
            }
            pub fn as_bytes(&self) -> &[u8; $size] {
                &self.0
            }
        }
    };
}
id!(ObjectId, 32);
id!(FileId, 32);
id!(TreeId, 32);
id!(CommitId, 32);
id!(SymlinkId, 32);
id!(CopyId, 32);
id!(ChangeId, 16);
impl CommitId {
    pub const ROOT: Self = Self([0; 32]);
}
impl ChangeId {
    pub const ROOT: Self = Self([0; 16]);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Kind {
    File = 1,
    Tree = 2,
    Commit = 3,
    Symlink = 4,
    CopyHistory = 5,
}
impl TryFrom<u8> for Kind {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::File),
            2 => Ok(Self::Tree),
            3 => Ok(Self::Commit),
            4 => Ok(Self::Symlink),
            5 => Ok(Self::CopyHistory),
            _ => Err(Error::UnsupportedKind),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ObjectKey {
    pub kind: Kind,
    pub id: ObjectId,
}
/// Exact stored byte size, not decoded file length or a logical tree size.
/// The digest algorithm is fixed to raw, unkeyed BLAKE3 in format v1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Descriptor {
    pub key: ObjectKey,
    pub size_bytes: u64,
    pub format_version: u16,
}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_object_bytes: usize,
    pub max_items: usize,
    pub max_field_bytes: usize,
    pub max_path_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_object_bytes: 16 * 1024 * 1024,
            max_items: 100_000,
            max_field_bytes: 1024 * 1024,
            max_path_bytes: 64 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeEntry {
    pub name: String,
    pub value: TreeValue,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TreeValue {
    File {
        id: FileId,
        executable: bool,
        size_bytes: u64,
        copy_id: Option<CopyId>,
    },
    Symlink {
        id: SymlinkId,
        target_bytes: u64,
    },
    Tree(TreeId),
}
/// Entries must already be strictly ordered by UTF-8 bytes. Encoding never sorts
/// or normalizes input silently. GitSubmodule has no native v1 representation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tree {
    pub entries: Vec<TreeEntry>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Timestamp {
    pub millis_since_epoch: i64,
    pub tz_offset_minutes: i32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signature {
    pub name: String,
    pub email: String,
    pub timestamp: Timestamp,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecureSignature {
    pub data: Vec<u8>,
    pub signature: Vec<u8>,
}
/// Exact jj Merge order: add, remove, add, ... . Terms are never simplified,
/// deduplicated or reordered. A resolved merge has one term and one empty label.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeMerge {
    pub terms: Vec<TreeId>,
    pub labels: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    pub parents: Vec<CommitId>,
    pub predecessors: Vec<CommitId>,
    pub root_tree: TreeMerge,
    pub change_id: ChangeId,
    pub description: String,
    pub author: Signature,
    pub committer: Signature,
    pub secure_signature: Option<SecureSignature>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyHistory {
    pub current_path: String,
    pub parents: Vec<CopyId>,
    pub salt: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Object {
    Tree(Tree),
    Commit(Commit),
    Symlink(String),
    CopyHistory(CopyHistory),
}
impl Object {
    pub fn kind(&self) -> Kind {
        match self {
            Self::Tree(_) => Kind::Tree,
            Self::Commit(_) => Kind::Commit,
            Self::Symlink(_) => Kind::Symlink,
            Self::CopyHistory(_) => Kind::CopyHistory,
        }
    }
    /// Every direct object reference, including negative merge terms and copy
    /// ancestry. Synthetic root commits are intrinsic, not missing CAS objects.
    pub fn references(&self) -> Vec<ObjectKey> {
        let mut result = Vec::new();
        let mut push = |kind, id| {
            result.push(ObjectKey {
                kind,
                id: ObjectId(id),
            })
        };
        match self {
            Self::Tree(tree) => {
                for entry in &tree.entries {
                    match entry.value {
                        TreeValue::File { id, copy_id, .. } => {
                            push(Kind::File, id.0);
                            if let Some(id) = copy_id {
                                push(Kind::CopyHistory, id.0);
                            }
                        }
                        TreeValue::Symlink { id, .. } => push(Kind::Symlink, id.0),
                        TreeValue::Tree(id) => push(Kind::Tree, id.0),
                    }
                }
            }
            Self::Commit(commit) => {
                for id in commit.parents.iter().chain(&commit.predecessors) {
                    if *id != CommitId::ROOT {
                        push(Kind::Commit, id.0);
                    }
                }
                for id in &commit.root_tree.terms {
                    push(Kind::Tree, id.0);
                }
            }
            Self::CopyHistory(copy) => {
                for id in &copy.parents {
                    push(Kind::CopyHistory, id.0);
                }
            }
            Self::Symlink(_) => {}
        }
        result
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedObject {
    pub descriptor: Descriptor,
    pub bytes: Vec<u8>,
}
fn descriptor(kind: Kind, bytes: &[u8]) -> Descriptor {
    Descriptor {
        key: ObjectKey {
            kind,
            id: ObjectId(*blake3::hash(bytes).as_bytes()),
        },
        size_bytes: bytes.len() as u64,
        format_version: FORMAT_VERSION,
    }
}
pub fn describe_file(bytes: &[u8]) -> Descriptor {
    descriptor(Kind::File, bytes)
}
pub fn file_id(bytes: &[u8]) -> FileId {
    FileId(*blake3::hash(bytes).as_bytes())
}
/// The real empty tree is stored/hashed normally; only the synthetic commit is
/// reserved. Its root-tree reference still needs the empty-tree object available.
pub fn empty_tree() -> EncodedObject {
    encode(&Object::Tree(Tree::default()), Limits::default()).expect("fixed empty tree fits")
}
pub fn synthetic_root() -> Commit {
    let signature = Signature {
        name: String::new(),
        email: String::new(),
        timestamp: Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    Commit {
        parents: vec![],
        predecessors: vec![],
        root_tree: TreeMerge {
            terms: vec![TreeId(empty_tree().descriptor.key.id.0)],
            labels: vec![String::new()],
        },
        change_id: ChangeId::ROOT,
        description: String::new(),
        author: signature.clone(),
        committer: signature,
        secure_signature: None,
    }
}
/// Signature preimage is the entire canonical unsigned commit envelope. This
/// checks representation binding only, not signer identity or cryptography.
pub fn unsigned_commit_bytes(commit: &Commit, limits: Limits) -> Result<Vec<u8>> {
    let mut writer = Writer {
        bytes: Vec::new(),
        limits,
    };
    writer.raw(MAGIC)?;
    writer.raw(&FORMAT_VERSION.to_be_bytes())?;
    writer.u8(Kind::Commit as u8)?;
    encode_commit(&mut writer, commit, false)?;
    Ok(writer.bytes)
}
pub fn encode(object: &Object, limits: Limits) -> Result<EncodedObject> {
    encode_inner(object, limits)
}
fn encode_inner(object: &Object, limits: Limits) -> Result<EncodedObject> {
    let mut w = Writer {
        bytes: Vec::new(),
        limits,
    };
    w.raw(MAGIC)?;
    w.raw(&FORMAT_VERSION.to_be_bytes())?;
    w.u8(object.kind() as u8)?;
    match object {
        Object::Tree(tree) => {
            w.count(tree.entries.len())?;
            let mut previous: Option<&str> = None;
            for entry in &tree.entries {
                component(&entry.name)?;
                if entry.name.len() > limits.max_path_bytes {
                    return Err(Error::Limit);
                }
                if previous.is_some_and(|name| name.as_bytes() >= entry.name.as_bytes()) {
                    return Err(Error::Invalid("tree order or duplicate name"));
                }
                previous = Some(&entry.name);
                w.string(&entry.name)?;
                match entry.value {
                    TreeValue::File {
                        id,
                        executable,
                        size_bytes,
                        copy_id,
                    } => {
                        w.u8(1)?;
                        w.raw(&id.0)?;
                        w.u8(u8::from(executable))?;
                        w.u64(size_bytes)?;
                        w.u8(u8::from(copy_id.is_some()))?;
                        if let Some(id) = copy_id {
                            w.raw(&id.0)?;
                        }
                    }
                    TreeValue::Symlink { id, target_bytes } => {
                        w.u8(2)?;
                        w.raw(&id.0)?;
                        w.u64(target_bytes)?;
                    }
                    TreeValue::Tree(id) => {
                        w.u8(3)?;
                        w.raw(&id.0)?;
                    }
                }
            }
        }
        Object::Commit(commit) => encode_commit(&mut w, commit, true)?,
        Object::Symlink(target) => {
            if target.contains('\0') {
                return Err(Error::Invalid("symlink NUL"));
            }
            w.string(target)?;
        }
        Object::CopyHistory(copy) => {
            relative_path(&copy.current_path, limits)?;
            w.string(&copy.current_path)?;
            w.ids(copy.parents.iter().map(|id| id.0))?;
            w.field(&copy.salt)?;
        }
    }
    let descriptor = descriptor(object.kind(), &w.bytes);
    if object.kind() == Kind::Commit && descriptor.key.id.0 == CommitId::ROOT.0 {
        return Err(Error::Invalid("reserved root identity"));
    }
    Ok(EncodedObject {
        descriptor,
        bytes: w.bytes,
    })
}
fn encode_commit(w: &mut Writer, commit: &Commit, include_signature: bool) -> Result<()> {
    if commit.parents.is_empty() {
        return Err(Error::Invalid("synthetic root is intrinsic, not stored"));
    }
    w.ids(commit.parents.iter().map(|id| id.0))?;
    w.ids(commit.predecessors.iter().map(|id| id.0))?;
    let merge = &commit.root_tree;
    if merge.terms.is_empty()
        || merge.terms.len() % 2 != 1
        || merge.labels.len() != merge.terms.len()
        || (merge.terms.len() == 1 && !merge.labels[0].is_empty())
    {
        return Err(Error::Invalid("merge terms or labels"));
    }
    w.ids(merge.terms.iter().map(|id| id.0))?;
    w.count(merge.labels.len())?;
    for label in &merge.labels {
        w.string(label)?;
    }
    w.raw(&commit.change_id.0)?;
    w.string(&commit.description)?;
    w.person(&commit.author)?;
    w.person(&commit.committer)?;
    w.u8(u8::from(
        include_signature && commit.secure_signature.is_some(),
    ))?;
    if let Some(signature) = commit
        .secure_signature
        .as_ref()
        .filter(|_| include_signature)
    {
        if signature.data != unsigned_commit_bytes(commit, w.limits)? {
            return Err(Error::Invalid("signature preimage"));
        }
        w.field(&signature.data)?;
        w.field(&signature.signature)?;
    }
    Ok(())
}
/// Decode one complete metadata envelope, validating canonical representation.
/// Files are raw bytes and deliberately do not enter this decoder.
pub fn decode(bytes: &[u8], limits: Limits) -> Result<Object> {
    if bytes.len() > limits.max_object_bytes {
        return Err(Error::Limit);
    }
    let mut r = Reader {
        bytes,
        position: 0,
        limits,
    };
    if r.take(8)? != MAGIC {
        return Err(Error::Invalid("magic"));
    }
    if u16::from_be_bytes(r.array()?) != FORMAT_VERSION {
        return Err(Error::UnsupportedVersion);
    }
    let object = match Kind::try_from(r.u8()?)? {
        Kind::File => return Err(Error::UnsupportedKind),
        Kind::Tree => {
            let count = r.count(5)?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let name = r.string()?;
                let value = match r.u8()? {
                    1 => {
                        let id = FileId(r.array()?);
                        let executable = r.boolean()?;
                        let size_bytes = r.u64()?;
                        let copy_id = if r.boolean()? {
                            Some(CopyId(r.array()?))
                        } else {
                            None
                        };
                        TreeValue::File {
                            id,
                            executable,
                            size_bytes,
                            copy_id,
                        }
                    }
                    2 => TreeValue::Symlink {
                        id: SymlinkId(r.array()?),
                        target_bytes: r.u64()?,
                    },
                    3 => TreeValue::Tree(TreeId(r.array()?)),
                    _ => return Err(Error::UnsupportedKind),
                };
                entries.push(TreeEntry { name, value });
            }
            Object::Tree(Tree { entries })
        }
        Kind::Commit => {
            let parents = r.ids()?.into_iter().map(CommitId).collect();
            let predecessors = r.ids()?.into_iter().map(CommitId).collect();
            let terms = r.ids()?.into_iter().map(TreeId).collect();
            let count = r.count(4)?;
            let mut labels = Vec::with_capacity(count);
            for _ in 0..count {
                labels.push(r.string()?);
            }
            let change_id = ChangeId(r.array()?);
            let description = r.string()?;
            let author = r.person()?;
            let committer = r.person()?;
            let secure_signature = if r.boolean()? {
                Some(SecureSignature {
                    data: r.field()?.to_vec(),
                    signature: r.field()?.to_vec(),
                })
            } else {
                None
            };
            Object::Commit(Commit {
                parents,
                predecessors,
                root_tree: TreeMerge { terms, labels },
                change_id,
                description,
                author,
                committer,
                secure_signature,
            })
        }
        Kind::Symlink => Object::Symlink(r.string()?),
        Kind::CopyHistory => Object::CopyHistory(CopyHistory {
            current_path: r.string()?,
            parents: r.ids()?.into_iter().map(CopyId).collect(),
            salt: r.field()?.to_vec(),
        }),
    };
    if r.position != bytes.len() {
        return Err(Error::Invalid("trailing bytes"));
    }
    // Reuse validation, including tree order, merge and signature invariants.
    // The codec has no alternate integer/string/optional encodings to normalize.
    encode(&object, limits)?;
    Ok(object)
}
pub fn verify(descriptor: &Descriptor, bytes: &[u8], limits: Limits) -> Result<()> {
    if bytes.len() > limits.max_object_bytes {
        return Err(Error::Limit);
    }
    if descriptor.format_version != FORMAT_VERSION {
        return Err(Error::UnsupportedVersion);
    }
    if descriptor.size_bytes != bytes.len() as u64 {
        return Err(Error::SizeMismatch);
    }
    if *blake3::hash(bytes).as_bytes() != descriptor.key.id.0 {
        return Err(Error::HashMismatch);
    }
    if descriptor.key.kind != Kind::File && decode(bytes, limits)?.kind() != descriptor.key.kind {
        return Err(Error::Invalid("descriptor kind"));
    }
    Ok(())
}
fn component(name: &str) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\0') {
        return Err(Error::Invalid("path component"));
    }
    Ok(())
}
fn relative_path(path: &str, limits: Limits) -> Result<()> {
    if path.len() > limits.max_path_bytes {
        return Err(Error::Limit);
    }
    for name in path.split('/') {
        component(name)?;
    }
    Ok(())
}
struct Writer {
    bytes: Vec<u8>,
    limits: Limits,
}
impl Writer {
    fn raw(&mut self, value: &[u8]) -> Result<()> {
        if self
            .bytes
            .len()
            .checked_add(value.len())
            .is_none_or(|n| n > self.limits.max_object_bytes)
        {
            return Err(Error::Limit);
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }
    fn u8(&mut self, value: u8) -> Result<()> {
        self.raw(&[value])
    }
    fn u64(&mut self, value: u64) -> Result<()> {
        self.raw(&value.to_be_bytes())
    }
    fn count(&mut self, count: usize) -> Result<()> {
        if count > self.limits.max_items {
            return Err(Error::Limit);
        }
        self.raw(
            &u32::try_from(count)
                .map_err(|_| Error::Limit)?
                .to_be_bytes(),
        )
    }
    fn field(&mut self, value: &[u8]) -> Result<()> {
        if value.len() > self.limits.max_field_bytes {
            return Err(Error::Limit);
        }
        self.raw(
            &u32::try_from(value.len())
                .map_err(|_| Error::Limit)?
                .to_be_bytes(),
        )?;
        self.raw(value)
    }
    fn string(&mut self, value: &str) -> Result<()> {
        self.field(value.as_bytes())
    }
    fn ids(&mut self, ids: impl ExactSizeIterator<Item = [u8; 32]>) -> Result<()> {
        self.count(ids.len())?;
        for id in ids {
            self.raw(&id)?;
        }
        Ok(())
    }
    fn person(&mut self, value: &Signature) -> Result<()> {
        self.string(&value.name)?;
        self.string(&value.email)?;
        self.raw(&value.timestamp.millis_since_epoch.to_be_bytes())?;
        self.raw(&value.timestamp.tz_offset_minutes.to_be_bytes())
    }
}
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
    limits: Limits,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.position.checked_add(n).ok_or(Error::Limit)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(Error::Invalid("truncated object"))?;
        self.position = end;
        Ok(value)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("checked array length"))
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }
    fn boolean(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::Invalid("boolean")),
        }
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn count(&mut self, minimum_width: usize) -> Result<usize> {
        let n = u32::from_be_bytes(self.array()?) as usize;
        if n > self.limits.max_items || n > (self.bytes.len() - self.position) / minimum_width {
            return Err(Error::Limit);
        }
        Ok(n)
    }
    fn field(&mut self) -> Result<&'a [u8]> {
        let n = u32::from_be_bytes(self.array()?) as usize;
        if n > self.limits.max_field_bytes {
            return Err(Error::Limit);
        }
        self.take(n)
    }
    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.field()?.to_vec()).map_err(|_| Error::Invalid("UTF-8"))
    }
    fn ids(&mut self) -> Result<Vec<[u8; 32]>> {
        let n = self.count(32)?;
        let mut ids = Vec::with_capacity(n);
        for _ in 0..n {
            ids.push(self.array()?);
        }
        Ok(ids)
    }
    fn person(&mut self) -> Result<Signature> {
        Ok(Signature {
            name: self.string()?,
            email: self.string()?,
            timestamp: Timestamp {
                millis_since_epoch: i64::from_be_bytes(self.array()?),
                tz_offset_minutes: i32::from_be_bytes(self.array()?),
            },
        })
    }
}

#[cfg(test)]
mod tests;
