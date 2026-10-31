//! Canonical transform identities and finite native artifact construction.
//! Selection, byte ownership, authorization and publication belong to embeddings.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tinytree_model::{
    self as model, ChangeId, Commit, CommitId, Descriptor, EncodedObject, FileId, Object,
    Signature, Timestamp, Tree, TreeEntry, TreeId, TreeMerge, TreeValue,
};

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid(&'static str),
    Limit(&'static str),
    Corrupt(&'static str),
    Model(model::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<model::Error> for Error {
    fn from(value: model::Error) -> Self {
        Self::Model(value)
    }
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| Error::Corrupt("metadata encoding"))
}
pub const MAX_FILES: usize = 4096;
pub const MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_FILE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_RECORD_BYTES: usize = 1024 * 1024;
// Bound path storage independently from payload bytes. A shared deep prefix
// must not multiply suffix allocations at every tree construction level.
pub const MAX_TREE_PATH_BYTES: usize = 1024 * 1024;
// Canonical file paths must fit the same depth as Forge views/uploads.
pub const MAX_TREE_DEPTH: usize = 64;
pub const MAX_TREE_DIRECTORIES: usize = MAX_FILES;
pub const MAX_TREE_NODES: usize = MAX_FILES * 2;

#[derive(Default)]
pub struct TransformPathBudget {
    bytes: usize,
}
impl TransformPathBudget {
    pub fn observe(&mut self, value: &str) -> Result<()> {
        path(value, false)?;
        if value.split('/').count() > MAX_TREE_DEPTH {
            return Err(Error::Limit("transform tree depth"));
        }
        self.bytes = self
            .bytes
            .checked_add(value.len())
            .ok_or(Error::Limit("transform tree path bytes"))?;
        if self.bytes > MAX_TREE_PATH_BYTES {
            return Err(Error::Limit("transform tree path bytes"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgePathMapping {
    pub source_prefix: String,
    pub destination_prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeTransformSpec {
    pub mappings: Vec<ForgePathMapping>,
}

impl ForgeTransformSpec {
    pub fn validate(&self) -> Result<()> {
        if self.mappings.is_empty() || self.mappings.len() > 64 {
            return Err(Error::Limit("transform mappings"));
        }
        for (index, mapping) in self.mappings.iter().enumerate() {
            path(&mapping.source_prefix, true)?;
            path(&mapping.destination_prefix, true)?;
            for other in &self.mappings[..index] {
                if overlap(&mapping.source_prefix, &other.source_prefix)
                    || overlap(&mapping.destination_prefix, &other.destination_prefix)
                {
                    return Err(Error::Invalid("ambiguous transform mapping"));
                }
            }
        }
        Ok(())
    }

    pub fn id(&self) -> Result<[u8; 32]> {
        self.validate()?;
        Ok(digest(
            b"tinytree-forge-transform-spec-v1\0",
            &encode(self)?,
        ))
    }

    pub fn map_path(&self, source: &str) -> Result<Option<String>> {
        self.validate()?;
        path(source, false)?;
        for mapping in &self.mappings {
            if let Some(suffix) = relative(&mapping.source_prefix, source) {
                let mapped = join(&mapping.destination_prefix, suffix);
                path(&mapped, false)?;
                return Ok(Some(mapped));
            }
        }
        Ok(None)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgeLineageOperation {
    Rename,
    Copy,
    ImportedOrigin,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeLineageHint {
    pub operation: ForgeLineageOperation,
    pub source_path: String,
    pub target_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeLineageEdge {
    pub operation: ForgeLineageOperation,
    pub source_account: String,
    pub source_revision: [u8; 32],
    pub source_path: String,
    pub destination_path: String,
    pub destination_revision: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeDependencyPin {
    pub id: [u8; 32],
    pub artifact: [u8; 32],
    pub reference: String,
    pub generation: u64,
    pub retired: bool,
    /// Authoritative service-owned references, not arbitrary caller labels.
    pub protected_roots: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeTransformRun {
    pub id: [u8; 32],
    pub spec: [u8; 32],
    pub source_account: String,
    pub source_before: [u8; 32],
    pub source_after: [u8; 32],
    pub source_before_view: [u8; 32],
    pub source_after_view: [u8; 32],
    pub destination_account: String,
    pub destination_view: [u8; 32],
    pub destination_base: [u8; 32],
    pub output_head: [u8; 32],
    pub lineage: Vec<ForgeLineageEdge>,
    pub destination_paths: Vec<String>,
    pub pin: ForgeDependencyPin,
}

/// Caller-visible provenance. An unavailable origin does not reveal account,
/// revision or path identifiers from the private source record.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeTransformView {
    pub id: [u8; 32],
    pub destination_view: [u8; 32],
    pub destination_account: String,
    pub destination_base: [u8; 32],
    pub output_head: [u8; 32],
    pub pin: ForgeDependencyPin,
    pub origin: Option<ForgeTransformOrigin>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeTransformOrigin {
    pub account: String,
    pub before: [u8; 32],
    pub after: [u8; 32],
    pub lineage: Vec<ForgeLineageEdge>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgePinFile {
    pub path: String,
    pub file_id: [u8; 32],
    pub size_bytes: u64,
    pub executable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgePinPage {
    pub pin_id: [u8; 32],
    pub artifact: [u8; 32],
    pub files: Vec<ForgePinFile>,
    pub next_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgePinChunk {
    pub pin_id: [u8; 32],
    pub file_id: [u8; 32],
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub next_offset: u64,
    pub eof: bool,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformIdentityInput {
    pub spec: [u8; 32],
    pub source_account: String,
    pub before: [u8; 32],
    pub after: [u8; 32],
    pub before_view: [u8; 32],
    pub after_view: [u8; 32],
    pub before_view_name: String,
    pub after_view_name: String,
    pub destination_account: String,
    pub destination_view: [u8; 32],
    pub destination_view_name: String,
    pub destination_base: [u8; 32],
    pub change_id: [u8; 16],
    pub lineage: Vec<ForgeLineageHint>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenCommitter {
    pub name: String,
    pub email: String,
    pub millis: i64,
    pub offset: i32,
}
impl FrozenCommitter {
    pub fn capture(value: &Signature) -> Self {
        Self {
            name: value.name.clone(),
            email: value.email.clone(),
            millis: value.timestamp.millis_since_epoch,
            offset: value.timestamp.tz_offset_minutes,
        }
    }
    pub fn signature(&self) -> Signature {
        Signature {
            name: self.name.clone(),
            email: self.email.clone(),
            timestamp: Timestamp {
                millis_since_epoch: self.millis,
                tz_offset_minutes: self.offset,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectedFile {
    pub id: FileId,
    pub size: u64,
    pub executable: bool,
}

pub fn projected_value(file: &ProjectedFile) -> TreeValue {
    TreeValue::File {
        id: file.id,
        executable: file.executable,
        size_bytes: file.size,
        copy_id: None,
    }
}
pub fn strip_copy(value: &TreeValue) -> TreeValue {
    match value {
        TreeValue::File {
            id,
            executable,
            size_bytes,
            ..
        } => TreeValue::File {
            id: *id,
            executable: *executable,
            size_bytes: *size_bytes,
            copy_id: None,
        },
        value => value.clone(),
    }
}
fn relative<'a>(prefix: &str, value: &'a str) -> Option<&'a str> {
    if prefix.is_empty() {
        Some(value)
    } else if value == prefix {
        Some("")
    } else {
        value
            .strip_prefix(prefix)
            .and_then(|suffix| suffix.strip_prefix('/'))
    }
}
fn overlap(left: &str, right: &str) -> bool {
    relative(left, right).is_some() || relative(right, left).is_some()
}
pub fn join_path(prefix: &str, suffix: &str) -> String {
    if prefix.is_empty() {
        suffix.into()
    } else if suffix.is_empty() {
        prefix.into()
    } else {
        format!("{prefix}/{suffix}")
    }
}
pub fn validate_path(value: &str, root: bool) -> Result<()> {
    if (value.is_empty() && !root)
        || value.len() > 4096
        || value.contains(['\\', '\0'])
        || value.chars().any(char::is_control)
        || (!value.is_empty()
            && value
                .split('/')
                .any(|part| part.is_empty() || matches!(part, "." | ".." | ".jj" | ".git")))
    {
        return Err(Error::Invalid("transform path"));
    }
    Ok(())
}
fn digest(domain: &[u8], value: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(value);
    *hash.finalize().as_bytes()
}
use self::{join_path as join, validate_path as path};

pub fn run_id(input: &TransformIdentityInput) -> Result<[u8; 32]> {
    Ok(digest(
        b"tinytree-forge-transform-run-v1\0",
        &encode(input)?,
    ))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Clone, Debug)]
pub struct BuiltTree {
    pub root: TreeId,
    pub objects: Vec<EncodedObject>,
}
pub fn build_tree(files: &BTreeMap<String, TreeValue>, limits: model::Limits) -> Result<BuiltTree> {
    // Validate the complete shape before allocating a trie or writing any
    // directory objects. Each component then borrows its one input path;
    // recursion never clones remaining suffixes or keeps ancestor maps.
    if files.len() > MAX_FILES {
        return Err(Error::Limit("transform tree files"));
    }
    let mut paths = TransformPathBudget::default();
    for name in files.keys() {
        paths.observe(name)?;
    }
    #[derive(Default)]
    struct Directory<'a> {
        entries: BTreeMap<&'a str, Entry<'a>>,
    }
    enum Entry<'a> {
        Leaf(&'a TreeValue),
        Directory(Directory<'a>),
    }
    struct Shape {
        nodes: usize,
        directories: usize,
    }
    impl Shape {
        fn reserve(&mut self, directory: bool) -> Result<()> {
            if self.nodes >= MAX_TREE_NODES
                || (directory && self.directories >= MAX_TREE_DIRECTORIES)
            {
                return Err(Error::Limit("transform tree nodes"));
            }
            self.nodes += 1;
            if directory {
                self.directories += 1;
            }
            Ok(())
        }
    }
    impl<'a> Directory<'a> {
        fn insert(&mut self, name: &'a str, value: &'a TreeValue, shape: &mut Shape) -> Result<()> {
            use std::collections::btree_map::Entry as Slot;
            match name.split_once('/') {
                Some((component, rest)) => {
                    let entry = match self.entries.entry(component) {
                        Slot::Vacant(slot) => {
                            shape.reserve(true)?;
                            slot.insert(Entry::Directory(Directory::default()))
                        }
                        Slot::Occupied(slot) => slot.into_mut(),
                    };
                    match entry {
                        Entry::Directory(directory) => directory.insert(rest, value, shape),
                        Entry::Leaf(_) => Err(Error::Invalid("transform file-directory collision")),
                    }
                }
                None => match self.entries.entry(name) {
                    Slot::Vacant(slot) => {
                        shape.reserve(false)?;
                        slot.insert(Entry::Leaf(value));
                        Ok(())
                    }
                    Slot::Occupied(_) => Err(Error::Invalid("transform file-directory collision")),
                },
            }
        }
        fn write(self, limits: model::Limits, objects: &mut Vec<EncodedObject>) -> Result<TreeId> {
            let mut entries = Vec::with_capacity(self.entries.len());
            for (name, entry) in self.entries {
                let value = match entry {
                    Entry::Leaf(value) => value.clone(),
                    Entry::Directory(directory) => {
                        TreeValue::Tree(directory.write(limits, objects)?)
                    }
                };
                entries.push(TreeEntry {
                    name: name.to_owned(),
                    value,
                });
            }
            let tree = model::encode(&Object::Tree(Tree { entries }), limits)?;
            let id = TreeId(tree.descriptor.key.id.0);
            objects.push(tree);
            Ok(id)
        }
    }
    let mut directory = Directory::default();
    let mut shape = Shape {
        nodes: 1,
        directories: 1,
    };
    for (name, value) in files {
        directory.insert(name, value, &mut shape)?;
    }
    let mut objects = Vec::new();
    let root = directory.write(limits, &mut objects)?;
    Ok(BuiltTree { root, objects })
}

#[derive(Clone, Debug)]
pub struct SelectedSourceFile {
    pub relative_path: String,
    pub canonical_path: String,
    pub descriptor: Descriptor,
    pub executable: bool,
    pub copy_id: Option<[u8; 32]>,
}
#[derive(Clone, Debug)]
pub struct MappedFile {
    pub source_relative_path: String,
    pub source_canonical_path: String,
    pub mapped_path: String,
    pub destination_path: String,
    pub descriptor: Descriptor,
    pub executable: bool,
    pub copy_id: Option<[u8; 32]>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionState {
    pub selected: usize,
    pub bytes: u64,
    pub mapped_path_bytes: usize,
    pub canonical_path_bytes: usize,
    pub last_path: Option<String>,
}
#[derive(Clone, Debug)]
pub struct MappedSelectionPage {
    pub files: Vec<MappedFile>,
    pub state: SelectionState,
}
/// Inputs are complete current-policy selected view facts supplied by the host.
/// The returned descriptors require actual byte transfer; they confer no grant.
pub fn map_selection_page(
    spec: &ForgeTransformSpec,
    destination_root: &str,
    files: &[SelectedSourceFile],
    mut state: SelectionState,
) -> Result<MappedSelectionPage> {
    spec.validate()?;
    path(destination_root, true)?;
    if files.len() > 128
        || state.selected > MAX_FILES
        || state.bytes > MAX_BYTES
        || state.mapped_path_bytes > MAX_TREE_PATH_BYTES
        || state.canonical_path_bytes > MAX_TREE_PATH_BYTES
    {
        return Err(Error::Limit("transform selection page"));
    }
    let mut out = Vec::new();
    for file in files {
        if state
            .last_path
            .as_ref()
            .is_some_and(|last| last >= &file.relative_path)
        {
            return Err(Error::Invalid("transform selection order"));
        }
        state.selected += 1;
        if state.selected > MAX_FILES {
            return Err(Error::Limit("transform tree files"));
        }
        state.last_path = Some(file.relative_path.clone());
        let Some(mapped) = spec.map_path(&file.relative_path)? else {
            continue;
        };
        let canonical = join(destination_root, &mapped);
        let mut paths = TransformPathBudget {
            bytes: state.mapped_path_bytes,
        };
        paths.observe(&mapped)?;
        state.mapped_path_bytes = paths.bytes;
        let mut paths = TransformPathBudget {
            bytes: state.canonical_path_bytes,
        };
        paths.observe(&canonical)?;
        state.canonical_path_bytes = paths.bytes;
        if file.descriptor.key.kind != model::Kind::File {
            return Err(Error::Invalid("transform supports regular files only"));
        }
        state.bytes = state
            .bytes
            .checked_add(file.descriptor.size_bytes)
            .ok_or(Error::Limit("transform input bytes"))?;
        if state.bytes > MAX_BYTES || file.descriptor.size_bytes > MAX_FILE_BYTES as u64 {
            return Err(Error::Limit("transform input bytes"));
        }
        out.push(MappedFile {
            source_relative_path: file.relative_path.clone(),
            source_canonical_path: file.canonical_path.clone(),
            mapped_path: mapped,
            destination_path: canonical,
            descriptor: file.descriptor,
            executable: file.executable,
            copy_id: file.copy_id,
        });
    }
    Ok(MappedSelectionPage { files: out, state })
}

#[derive(Clone, Debug)]
pub struct TransformBuild {
    pub run: ForgeTransformRun,
    pub objects: Vec<EncodedObject>,
}
/// Build exact canonical proposals. No FILE bodies, current-policy decisions or
/// storage/publication side effects are part of this deterministic operation.
pub fn build_transform(
    input: &TransformIdentityInput,
    committer: &FrozenCommitter,
    destination_root: &str,
    before: &BTreeMap<String, ProjectedFile>,
    after: &BTreeMap<String, ProjectedFile>,
    destination: &BTreeMap<String, TreeValue>,
    mut lineage: Vec<ForgeLineageEdge>,
    limits: model::Limits,
) -> Result<TransformBuild> {
    path(destination_root, true)?;
    if destination.len() > MAX_FILES
        || input.lineage.len() > MAX_FILES
        || lineage.len() > MAX_FILES * 2
    {
        return Err(Error::Limit("transform tree files"));
    }
    for selected in [before, after] {
        let mut bytes = 0u64;
        for file in selected.values() {
            bytes = bytes
                .checked_add(file.size)
                .ok_or(Error::Limit("transform input bytes"))?;
            if bytes > MAX_BYTES || file.size > MAX_FILE_BYTES as u64 {
                return Err(Error::Limit("transform input bytes"));
            }
        }
    }
    let id = run_id(input)?;
    let before_tree = build_tree(
        &before
            .iter()
            .map(|(p, f)| (p.clone(), projected_value(f)))
            .collect(),
        limits,
    )?;
    let after_tree = build_tree(
        &after
            .iter()
            .map(|(p, f)| (p.clone(), projected_value(f)))
            .collect(),
        limits,
    )?;
    let mut result = destination.clone();
    let changed: BTreeSet<_> = before.keys().chain(after.keys()).cloned().collect();
    let mut destination_paths = BTreeSet::new();
    for mapped in changed {
        let canonical = join(destination_root, &mapped);
        destination_paths.insert(canonical.clone());
        if before.get(&mapped) == after.get(&mapped) {
            continue;
        }
        let expected = before.get(&mapped).map(projected_value);
        let replacement = after.get(&mapped).map(projected_value);
        let current = result.get(&canonical).map(strip_copy);
        if current != expected && current != replacement {
            return Err(Error::Invalid("transform destination conflict"));
        }
        match replacement {
            Some(value) => {
                result.insert(canonical, value);
            }
            None => {
                result.remove(&canonical);
            }
        }
    }
    let output_tree = build_tree(&result, limits)?;
    let output = model::encode(
        &Object::Commit(Commit {
            parents: vec![CommitId(input.destination_base)],
            predecessors: vec![],
            root_tree: TreeMerge {
                terms: vec![output_tree.root],
                labels: vec![String::new()],
            },
            change_id: ChangeId(input.change_id),
            description: "Update transformed source".into(),
            author: committer.signature(),
            committer: committer.signature(),
            secure_signature: None,
        }),
        limits,
    )?;
    let output_head = output.descriptor.key.id.0;
    for edge in &mut lineage {
        edge.destination_revision = output_head;
    }
    let artifact_tree = model::encode(
        &Object::Tree(Tree {
            entries: vec![
                TreeEntry {
                    name: "after".into(),
                    value: TreeValue::Tree(after_tree.root),
                },
                TreeEntry {
                    name: "before".into(),
                    value: TreeValue::Tree(before_tree.root),
                },
            ],
        }),
        limits,
    )?;
    let artifact = model::encode(
        &Object::Commit(Commit {
            parents: vec![CommitId::ROOT],
            predecessors: vec![],
            root_tree: TreeMerge {
                terms: vec![TreeId(artifact_tree.descriptor.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: ChangeId(id[..16].try_into().unwrap()),
            description: "Retained transformed dependency".into(),
            author: committer.signature(),
            committer: committer.signature(),
            secure_signature: None,
        }),
        limits,
    )?;
    let run = ForgeTransformRun {
        id,
        spec: input.spec,
        source_account: input.source_account.clone(),
        source_before: input.before,
        source_after: input.after,
        source_before_view: input.before_view,
        source_after_view: input.after_view,
        destination_account: input.destination_account.clone(),
        destination_view: input.destination_view,
        destination_base: input.destination_base,
        output_head,
        lineage,
        destination_paths: destination_paths.into_iter().collect(),
        pin: ForgeDependencyPin {
            id,
            artifact: artifact.descriptor.key.id.0,
            reference: format!("_tinytree/forge/pins/v1/{}", hex(&id)),
            generation: 1,
            retired: false,
            protected_roots: BTreeSet::from([format!("transform-{}", hex(&id))]),
        },
    };
    let mut seen = BTreeSet::new();
    let mut objects = Vec::new();
    for object in before_tree
        .objects
        .into_iter()
        .chain(after_tree.objects)
        .chain(output_tree.objects)
        .chain([output, artifact_tree, artifact])
    {
        if seen.insert((object.descriptor.key.kind as u8, object.descriptor.key.id.0)) {
            objects.push(object);
        }
    }
    Ok(TransformBuild { run, objects })
}

/// Exact mutation intent, separate from immutable run/spec identities.
pub fn retire_fingerprint(
    account: &str,
    principal: &str,
    view: &str,
    id: [u8; 32],
) -> Result<[u8; 32]> {
    Ok(digest(
        b"tinytree-forge-transform-retire-v1\0",
        &encode(&(account, principal, view, id))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frozen() -> FrozenCommitter {
        FrozenCommitter {
            name: "Builder".into(),
            email: "builder@example.test".into(),
            millis: 10,
            offset: 0,
        }
    }
    fn input(spec: [u8; 32]) -> TransformIdentityInput {
        TransformIdentityInput {
            spec,
            source_account: "source".into(),
            before: [1; 32],
            after: [2; 32],
            before_view: [3; 32],
            after_view: [4; 32],
            before_view_name: "before".into(),
            after_view_name: "after".into(),
            destination_account: "destination".into(),
            destination_view: [5; 32],
            destination_view_name: "workspace".into(),
            destination_base: [6; 32],
            change_id: [7; 16],
            lineage: vec![],
        }
    }
    #[test]
    fn native_identity_preimages_and_selection_pages_remain_exact() {
        let spec = ForgeTransformSpec {
            mappings: vec![ForgePathMapping {
                source_prefix: "lib".into(),
                destination_prefix: "vendor".into(),
            }],
        };
        let mut expected = blake3::Hasher::new();
        expected.update(b"tinytree-forge-transform-spec-v1\0");
        expected.update(br#"{"mappings":[{"source_prefix":"lib","destination_prefix":"vendor"}]}"#);
        assert_eq!(spec.id().unwrap(), *expected.finalize().as_bytes());
        let identity = input(spec.id().unwrap());
        let array = |bytes: &[u8]| serde_json::to_string(bytes).unwrap();
        let legacy = format!(
            concat!(
                "{{\"spec\":{},\"source_account\":\"source\",\"before\":{},\"after\":{},",
                "\"before_view\":{},\"after_view\":{},\"before_view_name\":\"before\",\"after_view_name\":\"after\",",
                "\"destination_account\":\"destination\",\"destination_view\":{},\"destination_view_name\":\"workspace\",",
                "\"destination_base\":{},\"change_id\":{},\"lineage\":[]}}"
            ),
            array(&identity.spec),
            array(&[1; 32]),
            array(&[2; 32]),
            array(&[3; 32]),
            array(&[4; 32]),
            array(&[5; 32]),
            array(&[6; 32]),
            array(&[7; 16])
        );
        assert_eq!(encode(&identity).unwrap(), legacy.as_bytes());
        let mut expected = blake3::Hasher::new();
        expected.update(b"tinytree-forge-transform-run-v1\0");
        expected.update(legacy.as_bytes());
        assert_eq!(run_id(&identity).unwrap(), *expected.finalize().as_bytes());
        let descriptor = model::describe_file(b"verified bytes");
        let files = (0..129)
            .map(|i| SelectedSourceFile {
                relative_path: format!("lib/file-{i:03}"),
                canonical_path: format!("public/lib/file-{i:03}"),
                descriptor,
                executable: false,
                copy_id: None,
            })
            .collect::<Vec<_>>();
        let mut results = Vec::new();
        for page_size in [16, 128] {
            let mut state = SelectionState::default();
            let mut mapped = Vec::new();
            for page in files.chunks(page_size) {
                let result = map_selection_page(&spec, "project", page, state).unwrap();
                state = result.state;
                mapped.extend(
                    result
                        .files
                        .into_iter()
                        .map(|f| (f.mapped_path, f.destination_path, f.descriptor)),
                );
            }
            results.push((serde_json::to_vec(&state).unwrap(), mapped));
        }
        assert_eq!(results[0], results[1]);
        assert_eq!(results[0].1[128].1, "project/vendor/file-128");
        let state = map_selection_page(&spec, "project", &files[..1], SelectionState::default())
            .unwrap()
            .state;
        assert!(
            map_selection_page(&spec, "project", &files[..1], state).is_err(),
            "repeated page must not double-account a selection"
        );
    }
    #[test]
    fn canonical_artifacts_strip_imported_copy_ids_but_preserve_unchanged_destination_values() {
        let file = |bytes: &[u8], executable| {
            let d = model::describe_file(bytes);
            ProjectedFile {
                id: FileId(d.key.id.0),
                size: d.size_bytes,
                executable,
            }
        };
        let before = BTreeMap::from([
            ("vendor/changed".into(), file(b"old", false)),
            ("vendor/same".into(), file(b"same", true)),
        ]);
        let after = BTreeMap::from([
            ("vendor/changed".into(), file(b"new", true)),
            ("vendor/same".into(), file(b"same", true)),
        ]);
        let copy = model::encode(
            &Object::CopyHistory(model::CopyHistory {
                current_path: "vendor/same".into(),
                parents: vec![],
                salt: vec![1],
            }),
            model::Limits::default(),
        )
        .unwrap();
        let same = TreeValue::File {
            id: after["vendor/same"].id,
            size_bytes: after["vendor/same"].size,
            executable: true,
            copy_id: Some(model::CopyId(copy.descriptor.key.id.0)),
        };
        let link = model::encode(
            &Object::Symlink("vendor/same".into()),
            model::Limits::default(),
        )
        .unwrap();
        let symlink = TreeValue::Symlink {
            id: model::SymlinkId(link.descriptor.key.id.0),
            target_bytes: 11,
        };
        let changed_copy = model::encode(
            &Object::CopyHistory(model::CopyHistory {
                current_path: "vendor/changed".into(),
                parents: vec![],
                salt: vec![2],
            }),
            model::Limits::default(),
        )
        .unwrap();
        let mut changed_destination = projected_value(&before["vendor/changed"]);
        let TreeValue::File { copy_id, .. } = &mut changed_destination else {
            panic!()
        };
        *copy_id = Some(model::CopyId(changed_copy.descriptor.key.id.0));
        let destination = BTreeMap::from([
            ("link".into(), symlink.clone()),
            (
                "private/keep".into(),
                projected_value(&file(b"private", false)),
            ),
            ("vendor/changed".into(), changed_destination),
            ("vendor/same".into(), same.clone()),
        ]);
        let baseline = build_tree(&destination, model::Limits::default()).unwrap();
        let signature = frozen().signature();
        let base = model::encode(
            &Object::Commit(Commit {
                parents: vec![CommitId::ROOT],
                predecessors: vec![],
                root_tree: TreeMerge {
                    terms: vec![baseline.root],
                    labels: vec![String::new()],
                },
                change_id: ChangeId([1; 16]),
                description: "base".into(),
                author: signature.clone(),
                committer: signature,
                secure_signature: None,
            }),
            model::Limits::default(),
        )
        .unwrap();
        let mut identity = input([8; 32]);
        identity.destination_base = base.descriptor.key.id.0;
        let plan = build_transform(
            &identity,
            &frozen(),
            "",
            &before,
            &after,
            &destination,
            vec![],
            model::Limits::default(),
        )
        .unwrap();
        let objects = plan
            .objects
            .iter()
            .map(|o| {
                (
                    o.descriptor.key.id.0,
                    model::decode(&o.bytes, model::Limits::default()).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let Object::Commit(output) = &objects[&plan.run.output_head] else {
            panic!()
        };
        assert_eq!(output.parents, vec![CommitId(base.descriptor.key.id.0)]);
        let mut expected = destination;
        expected.insert(
            "vendor/changed".into(),
            projected_value(&after["vendor/changed"]),
        );
        assert_eq!(
            output.root_tree.terms[0],
            build_tree(&expected, model::Limits::default())
                .unwrap()
                .root
        );
        assert!(
            objects
                .values()
                .all(|o| !matches!(o, Object::CopyHistory(_)))
        );
        let Object::Commit(artifact) = &objects[&plan.run.pin.artifact] else {
            panic!()
        };
        assert_eq!(artifact.parents, vec![CommitId::ROOT]);
        assert!(artifact.predecessors.is_empty());
        let Object::Tree(root) = &objects[&artifact.root_tree.terms[0].0] else {
            panic!()
        };
        assert_eq!(
            root.entries
                .iter()
                .map(|e| e.name.as_str())
                .collect::<Vec<_>>(),
            ["after", "before"]
        );
        for tree in objects.values().filter_map(|o| {
            if let Object::Tree(t) = o {
                Some(t)
            } else {
                None
            }
        }) {
            for entry in &tree.entries {
                if entry.name == "changed" {
                    assert!(matches!(entry.value, TreeValue::File { copy_id: None, .. }));
                }
            }
        }
        assert_eq!(expected["vendor/same"], same);
        assert_eq!(expected["link"], symlink);
    }
}
