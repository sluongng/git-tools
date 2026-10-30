//! Canonical review delta facts, not current-policy or ownership grants.
use crate::review::{ForgeReviewLineageHint, ForgeReviewLineageOperation};
use crate::upload_scope::Key;
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};
use tinytree_model::{
    self as model, CommitId, Descriptor, Kind, Object, ObjectId, ObjectKey, TreeId, TreeValue,
};

pub trait MetadataReader {
    fn read(&mut self, key: ObjectKey) -> Result<(Descriptor, Vec<u8>), ReadError>;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadError {
    Missing,
    Unavailable,
    Limit,
}
#[derive(Clone, Debug)]
pub struct Input {
    pub source_base: CommitId,
    pub source_head: CommitId,
    pub lineage: Vec<ForgeReviewLineageHint>,
}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub model: model::Limits,
    pub max_work: usize,
    pub max_contexts: usize,
    pub max_metadata_bytes: u64,
    pub max_depth: usize,
    pub max_path_bytes: usize,
    pub max_requirements: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            model: model::Limits::default(),
            max_work: 100_000,
            max_contexts: 32768,
            max_metadata_bytes: 32 * 1024 * 1024,
            max_depth: 64,
            max_path_bytes: 4 * 1024 * 1024,
            max_requirements: 200_000,
        }
    }
}
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Requirement {
    Path {
        canonical_path: String,
    },
    Entry {
        canonical_path: String,
        key: Key,
        expected_size: Option<u64>,
    },
    CopyOrigin {
        canonical_path: String,
        copy_id: String,
        file_id: String,
    },
}
#[derive(Clone, Debug, Serialize)]
pub struct Facts {
    pub change_id: [u8; 16],
    pub base_tree: [u8; 32],
    pub head_tree: [u8; 32],
    pub requirements: Vec<Requirement>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    Missing { key: ObjectKey },
    Unavailable { key: ObjectKey },
    Invalid(&'static str),
    Limit(&'static str),
    Model(model::Error),
}
impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for AdmissionError {}
fn key(kind: Kind, id: [u8; 32]) -> ObjectKey {
    ObjectKey {
        kind,
        id: ObjectId(id),
    }
}
fn hex(id: [u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
struct Walk<'a, R> {
    reader: &'a mut R,
    limits: Limits,
    metadata: u64,
    work: usize,
    paths: usize,
}
impl<R: MetadataReader> Walk<'_, R> {
    fn object(&mut self, k: ObjectKey) -> Result<Object, AdmissionError> {
        let (d, bytes) = self.reader.read(k).map_err(|error| match error {
            ReadError::Missing => AdmissionError::Missing { key: k },
            ReadError::Unavailable => AdmissionError::Unavailable { key: k },
            ReadError::Limit => AdmissionError::Limit("review metadata"),
        })?;
        if d.key != k || k.kind == Kind::File {
            return Err(AdmissionError::Invalid("review metadata descriptor"));
        }
        self.metadata = self
            .metadata
            .checked_add(d.size_bytes)
            .ok_or(AdmissionError::Limit("review metadata"))?;
        if self.metadata > self.limits.max_metadata_bytes {
            return Err(AdmissionError::Limit("review metadata"));
        }
        model::verify(&d, &bytes, self.limits.model).map_err(AdmissionError::Model)?;
        model::decode(&bytes, self.limits.model).map_err(AdmissionError::Model)
    }
    fn tree(&mut self, id: TreeId) -> Result<model::Tree, AdmissionError> {
        match self.object(key(Kind::Tree, id.0))? {
            Object::Tree(t) => Ok(t),
            _ => Err(AdmissionError::Invalid("review tree kind")),
        }
    }
    fn visit(&mut self, path: &str, depth: usize, contexts: usize) -> Result<(), AdmissionError> {
        self.work = self
            .work
            .checked_add(1)
            .ok_or(AdmissionError::Limit("review work"))?;
        self.paths = self
            .paths
            .checked_add(path.len())
            .ok_or(AdmissionError::Limit("review path bytes"))?;
        if self.work > self.limits.max_work || contexts > self.limits.max_contexts {
            return Err(AdmissionError::Limit("review changed paths"));
        }
        if depth > self.limits.max_depth {
            return Err(AdmissionError::Limit("review depth"));
        }
        if path.len() > self.limits.model.max_path_bytes || self.paths > self.limits.max_path_bytes
        {
            return Err(AdmissionError::Limit("review path"));
        }
        Ok(())
    }
}
fn walk<R: MetadataReader>(reader: &mut R, limits: Limits) -> Walk<'_, R> {
    Walk {
        reader,
        limits,
        metadata: 0,
        work: 0,
        paths: 0,
    }
}
fn push(
    out: &mut Vec<Requirement>,
    item: Requirement,
    limits: Limits,
) -> Result<(), AdmissionError> {
    if out.len() >= limits.max_requirements {
        return Err(AdmissionError::Limit("review requirements"));
    }
    out.push(item);
    Ok(())
}

/// Exact changed-directory/deletion semantics used by native read and admission.
pub fn changed_paths<R: MetadataReader>(
    reader: &mut R,
    base: TreeId,
    head: TreeId,
    limits: Limits,
) -> Result<Vec<String>, AdmissionError> {
    let mut walk = walk(reader, limits);
    let mut output = vec![];
    let mut pending = vec![(
        String::new(),
        Some(TreeValue::Tree(base)),
        Some(TreeValue::Tree(head)),
        0,
    )];
    while let Some((path, before, after, depth)) = pending.pop() {
        walk.visit(&path, depth, pending.len())?;
        if before == after {
            continue;
        }
        if matches!(before, Some(TreeValue::Tree(_))) || matches!(after, Some(TreeValue::Tree(_))) {
            let mut entries = BTreeMap::<String, (Option<TreeValue>, Option<TreeValue>)>::new();
            for (old, value) in [(true, before.as_ref()), (false, after.as_ref())] {
                if let Some(TreeValue::Tree(id)) = value {
                    for e in walk.tree(*id)?.entries {
                        let slot = entries.entry(e.name).or_default();
                        if old {
                            slot.0 = Some(e.value)
                        } else {
                            slot.1 = Some(e.value)
                        }
                    }
                } else if value.is_some() {
                    output.push(path.clone());
                }
            }
            if entries.is_empty() && !path.is_empty() {
                output.push(path.clone());
            }
            for (name, (a, b)) in entries {
                let next = if path.is_empty() {
                    name
                } else {
                    format!("{path}/{name}")
                };
                pending.push((next, a, b, depth + 1));
                if pending.len() > limits.max_contexts {
                    return Err(AdmissionError::Limit("review changed paths"));
                }
            }
        } else {
            output.push(path);
        }
        if output.len() > limits.max_requirements {
            return Err(AdmissionError::Limit("review requirements"));
        }
    }
    Ok(output)
}
fn files<R: MetadataReader>(
    reader: &mut R,
    root: TreeId,
    limits: Limits,
) -> Result<BTreeMap<String, TreeValue>, AdmissionError> {
    let mut walk = walk(reader, limits);
    let mut pending = vec![(String::new(), root, 0)];
    let mut out = BTreeMap::new();
    while let Some((prefix, id, depth)) = pending.pop() {
        walk.visit(&prefix, depth, pending.len())?;
        for entry in walk.tree(id)?.entries {
            let path = if prefix.is_empty() {
                entry.name
            } else {
                format!("{prefix}/{}", entry.name)
            };
            walk.visit(&path, depth + 1, pending.len())?;
            match entry.value {
                TreeValue::Tree(child) => pending.push((path, child, depth + 1)),
                value => {
                    out.insert(path, value);
                }
            }
        }
    }
    Ok(out)
}

/// Typed byte/copy witnesses remain backend decisions; no FILE bodies are read.
pub fn entry_requirements<R: MetadataReader>(
    reader: &mut R,
    base: TreeId,
    head: TreeId,
    hints: &[ForgeReviewLineageHint],
    limits: Limits,
) -> Result<Vec<Requirement>, AdmissionError> {
    if hints.len() > 128 {
        return Err(AdmissionError::Limit("review lineage"));
    }
    let before = files(reader, base, limits)?;
    let after = files(reader, head, limits)?;
    let mut out = vec![];
    let mut targets = HashSet::new();
    let mut walk = walk(reader, limits);
    for hint in hints {
        if hint.source_path == hint.target_path
            || !targets.insert(hint.target_path.as_str())
            || before.contains_key(&hint.target_path)
        {
            return Err(AdmissionError::Invalid("review lineage endpoints"));
        }
        push(
            &mut out,
            Requirement::Path {
                canonical_path: hint.source_path.clone(),
            },
            limits,
        )?;
        push(
            &mut out,
            Requirement::Path {
                canonical_path: hint.target_path.clone(),
            },
            limits,
        )?;
        let Some(TreeValue::File {
            id: source,
            size_bytes: source_size,
            copy_id: source_copy,
            ..
        }) = before.get(&hint.source_path)
        else {
            return Err(AdmissionError::Invalid("review lineage source"));
        };
        let Some(TreeValue::File {
            id: target,
            size_bytes: target_size,
            copy_id: target_copy,
            ..
        }) = after.get(&hint.target_path)
        else {
            return Err(AdmissionError::Invalid("review lineage target"));
        };
        if target_copy.is_none() && (source != target || source_size != target_size) {
            return Err(AdmissionError::Invalid("review lineage content identity"));
        }
        match hint.operation {
            ForgeReviewLineageOperation::Rename if after.contains_key(&hint.source_path) => {
                return Err(AdmissionError::Invalid("review rename retained source"));
            }
            ForgeReviewLineageOperation::Copy
                if !matches!(after.get(&hint.source_path), Some(TreeValue::File { .. })) =>
            {
                return Err(AdmissionError::Invalid("review copy changed source"));
            }
            _ => {}
        }
        if let Some(copy) = target_copy {
            let source_copy =
                source_copy.ok_or(AdmissionError::Invalid("review native copy source"))?;
            let Object::CopyHistory(history) = walk.object(key(Kind::CopyHistory, copy.0))? else {
                return Err(AdmissionError::Invalid("review copy history"));
            };
            if history.current_path != hint.target_path || history.parents != vec![source_copy] {
                return Err(AdmissionError::Invalid("review native copy relation"));
            }
            push(
                &mut out,
                Requirement::Entry {
                    canonical_path: hint.target_path.clone(),
                    key: Key::new(key(Kind::CopyHistory, copy.0)),
                    expected_size: None,
                },
                limits,
            )?;
            let Object::CopyHistory(source_history) =
                walk.object(key(Kind::CopyHistory, source_copy.0))?
            else {
                return Err(AdmissionError::Invalid("review copy history"));
            };
            if source_history.current_path != hint.source_path {
                return Err(AdmissionError::Invalid("review native copy source path"));
            }
            push(
                &mut out,
                Requirement::CopyOrigin {
                    canonical_path: hint.source_path.clone(),
                    copy_id: hex(source_copy.0),
                    file_id: hex(source.0),
                },
                limits,
            )?;
        }
    }
    for (path, value) in &after {
        if before.get(path) == Some(value) {
            continue;
        }
        match value {
            TreeValue::File {
                id,
                size_bytes,
                copy_id,
                ..
            } => {
                if copy_id.is_some() && !targets.contains(path.as_str()) {
                    return Err(AdmissionError::Invalid(
                        "review copy requires explicit lineage",
                    ));
                }
                push(
                    &mut out,
                    Requirement::Entry {
                        canonical_path: path.clone(),
                        key: Key::new(key(Kind::File, id.0)),
                        expected_size: Some(*size_bytes),
                    },
                    limits,
                )?;
            }
            TreeValue::Symlink { id, .. } => push(
                &mut out,
                Requirement::Entry {
                    canonical_path: path.clone(),
                    key: Key::new(key(Kind::Symlink, id.0)),
                    expected_size: None,
                },
                limits,
            )?,
            _ => return Err(AdmissionError::Invalid("unsupported review entry")),
        }
    }
    Ok(out)
}

pub fn analyze<R: MetadataReader>(
    reader: &mut R,
    input: &Input,
    limits: Limits,
) -> Result<Facts, AdmissionError> {
    let mut walk = walk(reader, limits);
    let Object::Commit(head) = walk.object(key(Kind::Commit, input.source_head.0))? else {
        return Err(AdmissionError::Invalid("review commit kind"));
    };
    if head.change_id == model::ChangeId::ROOT
        || input.source_base == input.source_head
        || head.parents != vec![input.source_base]
    {
        return Err(AdmissionError::Invalid("review logical change"));
    }
    let Object::Commit(base) = walk.object(key(Kind::Commit, input.source_base.0))? else {
        return Err(AdmissionError::Invalid("review commit kind"));
    };
    if base.root_tree.terms.len() != 1 || head.root_tree.terms.len() != 1 {
        return Err(AdmissionError::Invalid("review source conflicts"));
    }
    let (base_tree, head_tree) = (base.root_tree.terms[0], head.root_tree.terms[0]);
    let mut requirements = vec![];
    for path in changed_paths(reader, base_tree, head_tree, limits)? {
        push(
            &mut requirements,
            Requirement::Path {
                canonical_path: path,
            },
            limits,
        )?;
    }
    for item in entry_requirements(reader, base_tree, head_tree, &input.lineage, limits)? {
        push(&mut requirements, item, limits)?;
    }
    Ok(Facts {
        change_id: head.change_id.0,
        base_tree: base_tree.0,
        head_tree: head_tree.0,
        requirements,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{
        ChangeId, Commit, CopyHistory, CopyId, FileId, Signature, Timestamp, Tree, TreeEntry,
        TreeMerge,
    };
    struct Reader(BTreeMap<(u8, [u8; 32]), (Descriptor, Vec<u8>)>);
    impl MetadataReader for Reader {
        fn read(&mut self, key: ObjectKey) -> Result<(Descriptor, Vec<u8>), ReadError> {
            self.0
                .get(&(key.kind as u8, key.id.0))
                .cloned()
                .ok_or(ReadError::Missing)
        }
    }
    impl Reader {
        fn put(&mut self, object: Object) -> [u8; 32] {
            let object = model::encode(&object, model::Limits::default()).unwrap();
            let id = object.descriptor.key.id.0;
            self.0.insert(
                (object.descriptor.key.kind as u8, id),
                (object.descriptor, object.bytes),
            );
            id
        }
        fn tree(&mut self, entries: Vec<TreeEntry>) -> TreeId {
            TreeId(self.put(Object::Tree(Tree { entries })))
        }
        fn commit(&mut self, tree: TreeId, parent: CommitId) -> CommitId {
            let signature = Signature {
                name: "writer".into(),
                email: "writer@example.test".into(),
                timestamp: Timestamp {
                    millis_since_epoch: 0,
                    tz_offset_minutes: 0,
                },
            };
            CommitId(self.put(Object::Commit(Commit {
                parents: vec![parent],
                predecessors: vec![],
                root_tree: TreeMerge {
                    terms: vec![tree],
                    labels: vec![String::new()],
                },
                change_id: ChangeId([3; 16]),
                description: "change".into(),
                author: signature.clone(),
                committer: signature,
                secure_signature: None,
            })))
        }
    }
    #[test]
    fn source_facts_preserve_deleted_and_empty_changed_directory_paths() {
        let mut reader = Reader(BTreeMap::new());
        let empty = reader.tree(vec![]);
        let file = model::describe_file(b"old");
        let before = reader.tree(vec![TreeEntry {
            name: "deleted".into(),
            value: TreeValue::File {
                id: FileId(file.key.id.0),
                size_bytes: 3,
                executable: false,
                copy_id: None,
            },
        }]);
        let after = reader.tree(vec![TreeEntry {
            name: "empty".into(),
            value: TreeValue::Tree(empty),
        }]);
        let base = reader.commit(before, CommitId::ROOT);
        let head = reader.commit(after, base);
        let input = Input {
            source_base: base,
            source_head: head,
            lineage: vec![],
        };
        let facts = analyze(&mut reader, &input, Limits::default()).unwrap();
        assert_eq!(facts.change_id, [3; 16]);
        for wanted in ["deleted", "empty"] {
            assert!(
                facts.requirements.iter().any(
                    |r| matches!(r,Requirement::Path{canonical_path} if canonical_path==wanted)
                )
            );
        }
        assert!(
            !facts
                .requirements
                .iter()
                .any(|r| matches!(r, Requirement::Entry { .. })),
            "deletions do not fabricate new byte authority"
        );
        let removed = reader.0.remove(&(Kind::Tree as u8, after.0)).unwrap();
        assert!(
            matches!(analyze(&mut reader,&input,Limits::default()),Err(AdmissionError::Missing{key}) if key==super::key(Kind::Tree,after.0))
        );
        reader.0.insert((Kind::Tree as u8, after.0), removed);
        reader.0.get_mut(&(Kind::Tree as u8, after.0)).unwrap().1[0] ^= 1;
        assert!(matches!(
            analyze(&mut reader, &input, Limits::default()),
            Err(AdmissionError::Model(model::Error::HashMismatch))
        ));
    }
    #[test]
    fn edited_native_copy_emits_separate_origin_and_owned_target_requirements() {
        let mut reader = Reader(BTreeMap::new());
        let old = model::describe_file(b"old");
        let new = model::describe_file(b"edited");
        let source = CopyId(reader.put(Object::CopyHistory(CopyHistory {
            current_path: "source".into(),
            parents: vec![],
            salt: vec![1],
        })));
        let target = CopyId(reader.put(Object::CopyHistory(CopyHistory {
            current_path: "target".into(),
            parents: vec![source],
            salt: vec![2],
        })));
        let before = reader.tree(vec![TreeEntry {
            name: "source".into(),
            value: TreeValue::File {
                id: FileId(old.key.id.0),
                size_bytes: 3,
                executable: false,
                copy_id: Some(source),
            },
        }]);
        let after = reader.tree(vec![
            TreeEntry {
                name: "source".into(),
                value: TreeValue::File {
                    id: FileId(old.key.id.0),
                    size_bytes: 3,
                    executable: false,
                    copy_id: Some(source),
                },
            },
            TreeEntry {
                name: "target".into(),
                value: TreeValue::File {
                    id: FileId(new.key.id.0),
                    size_bytes: 6,
                    executable: false,
                    copy_id: Some(target),
                },
            },
        ]);
        let base = reader.commit(before, CommitId::ROOT);
        let head = reader.commit(after, base);
        let input = Input {
            source_base: base,
            source_head: head,
            lineage: vec![ForgeReviewLineageHint {
                operation: ForgeReviewLineageOperation::Copy,
                source_path: "source".into(),
                target_path: "target".into(),
            }],
        };
        let facts = analyze(&mut reader, &input, Limits::default()).unwrap();
        assert!(facts.requirements.iter().any(|r|matches!(r,Requirement::CopyOrigin{canonical_path,copy_id,file_id} if canonical_path=="source" && copy_id==&hex(source.0) && file_id==&hex(old.key.id.0))));
        assert!(facts.requirements.iter().any(|r|matches!(r,Requirement::Entry{canonical_path,key,expected_size:Some(6)} if canonical_path=="target" && key.kind==Kind::File as u8 && key.id==hex(new.key.id.0))));
        let mut no_proof = input.clone();
        no_proof.lineage.clear();
        assert!(matches!(
            analyze(&mut reader, &no_proof, Limits::default()),
            Err(AdmissionError::Invalid(
                "review copy requires explicit lineage"
            ))
        ));
        let small = Limits {
            max_work: 1,
            ..Limits::default()
        };
        assert!(matches!(
            analyze(&mut reader, &input, small),
            Err(AdmissionError::Limit(_))
        ));
    }
}
