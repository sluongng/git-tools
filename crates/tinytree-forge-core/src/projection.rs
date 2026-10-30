//! Deterministic sanitized native objects, without authorization or storage.
//!
//! The embedding selects authorized entries, verifies their descriptors and
//! symlink lengths, authenticates current policy, and validates any projected
//! review parent. These trusted inputs are deliberately not deserializable.
//! Returned copy origins are private ledger data, not public object metadata.
use crate::accounts::{AccountId, PolicyEpoch, ViewVersion, validate_path};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use tinytree_model::{
    self as model, ChangeId, Commit, CommitId, CopyHistory, CopyId, Descriptor, EncodedObject,
    FileId, Kind, Object, Signature, SymlinkId, Timestamp, Tree, TreeEntry, TreeId, TreeMerge,
    TreeValue,
};

const MAX_ENTRIES: usize = 10_000;
const MAX_PATH_BYTES: usize = 4 * 1024 * 1024;
const MAX_GENERATED_OBJECTS: usize = 30_002;
// Native selection visits at most 100,000 tree entries. Include the root,
// per-file copy seeds and synthetic commit without charging repeated identical
// subtrees as distinct closure objects.
const MAX_BUILD_NODES: usize = 110_002;
const MAX_GENERATED_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeProjection {
    pub account_id: AccountId,
    pub view_version: ViewVersion,
    pub policy_epoch: PolicyEpoch,
    pub canonical_revision: [u8; 32],
    pub projected_commit: [u8; 32],
    pub projected_tree: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeCopyOrigin {
    pub projected_copy: [u8; 32],
    pub canonical_revision: [u8; 32],
    pub canonical_path: String,
    pub canonical_copy: Option<[u8; 32]>,
    pub file: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct ProjectionContext {
    pub account_id: AccountId,
    pub view_version: ViewVersion,
    pub policy_epoch: PolicyEpoch,
    pub canonical_revision: CommitId,
    pub review: Option<ReviewProjection>,
}

#[derive(Clone, Copy, Debug)]
pub struct ReviewProjection {
    pub change_id: ChangeId,
    pub projected_parent: CommitId,
}

#[derive(Clone, Debug)]
pub struct SelectedEntry {
    pub canonical_path: String,
    pub descriptor: Descriptor,
    pub value: SelectedValue,
}

#[derive(Clone, Debug)]
pub enum SelectedValue {
    File {
        executable: bool,
        canonical_copy: Option<[u8; 32]>,
    },
    Symlink {
        target_bytes: u64,
    },
}

#[derive(Clone, Debug)]
pub struct ProjectionBuild {
    pub record_key: Vec<u8>,
    pub binding: ForgeProjection,
    /// New copy seeds, child-before-parent trees, then the synthetic commit.
    /// Selected FILE and Symlink backing bytes stay owned by the embedding.
    pub objects: Vec<EncodedObject>,
    pub copies: Vec<ForgeCopyOrigin>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionError {
    Invalid(&'static str),
    Limit(&'static str),
    Model(model::Error),
}
impl std::fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid projection: {reason}"),
            Self::Limit(reason) => write!(f, "projection limit: {reason}"),
            Self::Model(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for ProjectionError {}
impl From<model::Error> for ProjectionError {
    fn from(error: model::Error) -> Self {
        Self::Model(error)
    }
}
type Result<T> = std::result::Result<T, ProjectionError>;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Version-one catalog key. Its bytes are also part of copy/commit identities.
pub fn projection_key(context: &ProjectionContext) -> Vec<u8> {
    let base = format!(
        "forge/projections/v1/{}/{}/{}",
        context.account_id.0,
        hex(&context.view_version.0),
        context.policy_epoch.0
    );
    match context.review {
        None => base.into_bytes(),
        Some(review) => format!(
            "{base}/review/{}/{}/{}",
            hex(&context.canonical_revision.0),
            hex(&review.change_id.0),
            hex(&review.projected_parent.0)
        )
        .into_bytes(),
    }
}

#[derive(Default)]
struct Budget {
    nodes: usize,
    bytes: usize,
    seen: HashSet<model::ObjectKey>,
}
impl Budget {
    fn node(&mut self) -> Result<()> {
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or(ProjectionError::Limit("forge projected closure"))?;
        if self.nodes > MAX_BUILD_NODES {
            return Err(ProjectionError::Limit("forge projected closure"));
        }
        Ok(())
    }
    fn push(&mut self, objects: &mut Vec<EncodedObject>, object: EncodedObject) -> Result<()> {
        if !self.seen.insert(object.descriptor.key) {
            return Ok(());
        }
        if self.seen.len() > MAX_GENERATED_OBJECTS {
            return Err(ProjectionError::Limit("forge projected closure"));
        }
        self.bytes = self
            .bytes
            .checked_add(object.bytes.len())
            .ok_or(ProjectionError::Limit("forge projected object bytes"))?;
        if self.bytes > MAX_GENERATED_BYTES {
            return Err(ProjectionError::Limit("forge projected object bytes"));
        }
        objects.push(object);
        Ok(())
    }
}

#[derive(Default)]
struct Directory {
    files: BTreeMap<String, TreeValue>,
    directories: BTreeMap<String, Directory>,
}
impl Directory {
    fn insert(&mut self, path: &str, value: TreeValue, budget: &mut Budget) -> Result<()> {
        if let Some((head, tail)) = path.split_once('/') {
            if self.files.contains_key(head) {
                return Err(ProjectionError::Invalid("forge projected path collision"));
            }
            if !self.directories.contains_key(head) {
                budget.node()?;
                self.directories.insert(head.into(), Directory::default());
            }
            self.directories
                .get_mut(head)
                .expect("inserted directory")
                .insert(tail, value, budget)
        } else {
            if self.directories.contains_key(path)
                || self.files.insert(path.into(), value).is_some()
            {
                return Err(ProjectionError::Invalid("forge projected path collision"));
            }
            Ok(())
        }
    }
    fn write(
        self,
        objects: &mut Vec<EncodedObject>,
        budget: &mut Budget,
        limits: model::Limits,
    ) -> Result<TreeId> {
        let mut entries = self.files;
        for (name, directory) in self.directories {
            entries.insert(
                name,
                TreeValue::Tree(directory.write(objects, budget, limits)?),
            );
        }
        let object = model::encode(
            &Object::Tree(Tree {
                entries: entries
                    .into_iter()
                    .map(|(name, value)| TreeEntry { name, value })
                    .collect(),
            }),
            limits,
        )?;
        let id = TreeId(object.descriptor.key.id.0);
        budget.push(objects, object)?;
        Ok(id)
    }
}

pub fn build_projection(
    context: &ProjectionContext,
    entries: impl IntoIterator<Item = SelectedEntry>,
    limits: model::Limits,
) -> Result<ProjectionBuild> {
    context.account_id.validate().map_err(|error| match error {
        crate::Error::Invalid(reason) => ProjectionError::Invalid(reason),
        crate::Error::Limit(reason) => ProjectionError::Limit(reason),
        crate::Error::Corrupt(reason) => ProjectionError::Invalid(reason),
    })?;
    if context.policy_epoch.0 == 0 || context.canonical_revision == CommitId::ROOT {
        return Err(ProjectionError::Invalid("forge projection context"));
    }
    let record_key = projection_key(context);
    let mut directory = Directory::default();
    let mut budget = Budget::default();
    budget.node()?;
    let mut objects = Vec::new();
    let mut copies = Vec::new();
    let mut path_bytes = 0usize;
    for (index, entry) in entries.into_iter().enumerate() {
        if index >= MAX_ENTRIES {
            return Err(ProjectionError::Limit("forge file count"));
        }
        validate_path(&entry.canonical_path, false)
            .map_err(|_| ProjectionError::Invalid("forge path"))?;
        path_bytes = path_bytes
            .checked_add(entry.canonical_path.len())
            .ok_or(ProjectionError::Limit("forge projected path bytes"))?;
        if path_bytes > MAX_PATH_BYTES {
            return Err(ProjectionError::Limit("forge projected path bytes"));
        }
        if entry.descriptor.format_version != model::FORMAT_VERSION {
            return Err(model::Error::UnsupportedVersion.into());
        }
        let value = match entry.value {
            SelectedValue::File {
                executable,
                canonical_copy,
            } if entry.descriptor.key.kind == Kind::File => {
                budget.node()?;
                let mut salt = blake3::Hasher::new();
                salt.update(b"tinytree-projected-copy-v1\0");
                salt.update(&record_key);
                salt.update(entry.canonical_path.as_bytes());
                salt.update(&context.canonical_revision.0);
                salt.update(&entry.descriptor.key.id.0);
                let copy = model::encode(
                    &Object::CopyHistory(CopyHistory {
                        current_path: entry.canonical_path.clone(),
                        parents: vec![],
                        salt: salt.finalize().as_bytes().to_vec(),
                    }),
                    limits,
                )?;
                let id = copy.descriptor.key.id.0;
                budget.push(&mut objects, copy)?;
                copies.push(ForgeCopyOrigin {
                    projected_copy: id,
                    canonical_revision: context.canonical_revision.0,
                    canonical_path: entry.canonical_path.clone(),
                    canonical_copy,
                    file: entry.descriptor.key.id.0,
                });
                TreeValue::File {
                    id: FileId(entry.descriptor.key.id.0),
                    executable,
                    size_bytes: entry.descriptor.size_bytes,
                    copy_id: Some(CopyId(id)),
                }
            }
            SelectedValue::Symlink { target_bytes }
                if entry.descriptor.key.kind == Kind::Symlink =>
            {
                if target_bytes > entry.descriptor.size_bytes {
                    return Err(ProjectionError::Invalid("forge projected symlink size"));
                }
                TreeValue::Symlink {
                    id: SymlinkId(entry.descriptor.key.id.0),
                    target_bytes,
                }
            }
            _ => return Err(ProjectionError::Invalid("forge projected entry kind")),
        };
        directory.insert(&entry.canonical_path, value, &mut budget)?;
    }
    let root = directory.write(&mut objects, &mut budget, limits)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"tinytree-forge-projection-v1\0");
    hasher.update(&record_key);
    hasher.update(&root.0);
    let digest = hasher.finalize();
    let mut change = [0; 16];
    change.copy_from_slice(&digest.as_bytes()[..16]);
    let signature = Signature {
        name: "Tinytree".into(),
        email: "tinytree@users.noreply.github.com".into(),
        timestamp: Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    budget.node()?;
    let object = model::encode(
        &Object::Commit(Commit {
            parents: vec![
                context
                    .review
                    .map(|r| r.projected_parent)
                    .unwrap_or(CommitId::ROOT),
            ],
            predecessors: vec![],
            root_tree: TreeMerge {
                terms: vec![root],
                labels: vec![String::new()],
            },
            change_id: context
                .review
                .map(|r| r.change_id)
                .unwrap_or(ChangeId(change)),
            description: "Projected workspace".into(),
            author: signature.clone(),
            committer: signature,
            secure_signature: None,
        }),
        limits,
    )?;
    let binding = ForgeProjection {
        account_id: context.account_id.clone(),
        view_version: context.view_version,
        policy_epoch: context.policy_epoch,
        canonical_revision: context.canonical_revision.0,
        projected_commit: object.descriptor.key.id.0,
        projected_tree: root.0,
    };
    budget.push(&mut objects, object)?;
    Ok(ProjectionBuild {
        record_key,
        binding,
        objects,
        copies,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> ProjectionContext {
        ProjectionContext {
            account_id: AccountId("projection".into()),
            view_version: ViewVersion(VIEW_VERSION),
            policy_epoch: PolicyEpoch(2),
            canonical_revision: CommitId(CANONICAL),
            review: None,
        }
    }
    fn file(path: &str, bytes: &[u8]) -> SelectedEntry {
        SelectedEntry {
            canonical_path: path.into(),
            descriptor: model::describe_file(bytes),
            value: SelectedValue::File {
                executable: false,
                canonical_copy: None,
            },
        }
    }
    fn selected() -> Vec<SelectedEntry> {
        vec![
            file("README.md", b"# Account\npublic needle\n"),
            file("source.rs", b"fn needle() {}\n"),
        ]
    }

    #[test]
    fn sanitized_projection_matches_pre_extraction_native_store_identities() {
        let baseline = build_projection(&context(), selected(), model::Limits::default()).unwrap();
        assert_eq!(baseline.binding.projected_commit, BASELINE_COMMIT);
        assert_eq!(baseline.binding.projected_tree, BASELINE_TREE);
        assert_eq!(
            baseline
                .copies
                .iter()
                .find(|origin| origin.canonical_path == "source.rs")
                .unwrap()
                .projected_copy,
            SOURCE_COPY
        );
        let Object::Commit(commit) = model::decode(
            &baseline.objects.last().unwrap().bytes,
            model::Limits::default(),
        )
        .unwrap() else {
            panic!("commit")
        };
        assert_eq!(commit.parents, vec![CommitId::ROOT]);
        assert!(commit.predecessors.is_empty());
        assert!(commit.secure_signature.is_none());
        assert_eq!(commit.description, "Projected workspace");
        assert_eq!(commit.author.timestamp.millis_since_epoch, 0);
        for object in baseline
            .objects
            .iter()
            .filter(|object| object.descriptor.key.kind == Kind::CopyHistory)
        {
            let Object::CopyHistory(copy) =
                model::decode(&object.bytes, model::Limits::default()).unwrap()
            else {
                panic!("copy")
            };
            assert!(copy.parents.is_empty());
        }
        let mut reviewed = context();
        reviewed.review = Some(ReviewProjection {
            change_id: ChangeId([8; 16]),
            projected_parent: CommitId(BASELINE_COMMIT),
        });
        let result = build_projection(&reviewed, selected(), model::Limits::default()).unwrap();
        assert_eq!(result.binding.projected_commit, REVIEW_COMMIT);
        assert_eq!(result.binding.projected_tree, REVIEW_TREE);
        let Object::Commit(commit) = model::decode(
            &result.objects.last().unwrap().bytes,
            model::Limits::default(),
        )
        .unwrap() else {
            panic!("commit")
        };
        assert_eq!(commit.parents, vec![CommitId(BASELINE_COMMIT)]);
        assert_eq!(commit.change_id, ChangeId([8; 16]));
        assert_eq!(
            result.record_key,
            format!(
                "{}/review/{}/{}/{}",
                String::from_utf8(baseline.record_key).unwrap(),
                hex(&CANONICAL),
                hex(&[8; 16]),
                hex(&BASELINE_COMMIT)
            )
            .into_bytes()
        );
    }

    #[test]
    fn nested_symlinks_and_input_order_preserve_canonical_objects_without_copy_ancestry() {
        let link = model::encode(
            &Object::Symlink("../target".into()),
            model::Limits::default(),
        )
        .unwrap();
        let mut regular = file("pkg/bin/tool", b"#!/bin/sh\n");
        regular.value = SelectedValue::File {
            executable: true,
            canonical_copy: Some([9; 32]),
        };
        let entries = vec![
            regular,
            SelectedEntry {
                canonical_path: "pkg/link".into(),
                descriptor: link.descriptor,
                value: SelectedValue::Symlink { target_bytes: 9 },
            },
        ];
        let first =
            build_projection(&context(), entries.clone(), model::Limits::default()).unwrap();
        let second = build_projection(
            &context(),
            entries.into_iter().rev(),
            model::Limits::default(),
        )
        .unwrap();
        assert_eq!(
            first.binding.projected_commit,
            second.binding.projected_commit
        );
        assert_eq!(first.binding.projected_tree, second.binding.projected_tree);
        assert_eq!(first.copies.len(), 1);
        assert_eq!(first.copies[0].canonical_copy, Some([9; 32]));
        assert!(
            !first
                .objects
                .iter()
                .any(|object| object.descriptor.key.kind == Kind::Symlink)
        );
        for object in first.objects {
            model::verify(&object.descriptor, &object.bytes, model::Limits::default()).unwrap();
            match model::decode(&object.bytes, model::Limits::default()).unwrap() {
                Object::CopyHistory(copy) => assert!(copy.parents.is_empty()),
                Object::Tree(tree) => {
                    for entry in tree.entries {
                        if entry.name == "tool" {
                            assert!(matches!(
                                entry.value,
                                TreeValue::File {
                                    executable: true,
                                    ..
                                }
                            ));
                        }
                        if entry.name == "link" {
                            assert!(matches!(
                                entry.value,
                                TreeValue::Symlink {
                                    target_bytes: 9,
                                    ..
                                }
                            ));
                        }
                    }
                }
                _ => (),
            }
        }
    }

    #[test]
    fn projection_rejects_path_collisions_and_preserves_model_limit_categories() {
        for entries in [
            vec![file("file", b"a"), file("file", b"b")],
            vec![file("file", b"a"), file("file/child", b"b")],
            vec![file("file/child", b"a"), file("file", b"b")],
        ] {
            assert!(matches!(
                build_projection(&context(), entries, model::Limits::default()),
                Err(ProjectionError::Invalid("forge projected path collision"))
            ));
        }
        assert!(matches!(
            build_projection(
                &context(),
                [file("../private", b"a")],
                model::Limits::default()
            ),
            Err(ProjectionError::Invalid("forge path"))
        ));
        assert!(matches!(
            build_projection(
                &context(),
                (0..10_001).map(|i| file(&format!("f-{i}"), b"a")),
                model::Limits::default()
            ),
            Err(ProjectionError::Limit("forge file count"))
        ));
        let mut limits = model::Limits::default();
        limits.max_object_bytes = 1;
        assert!(matches!(
            build_projection(&context(), selected(), limits),
            Err(ProjectionError::Model(model::Error::Limit))
        ));
        let shared = "p".repeat(4000);
        assert!(matches!(
            build_projection(
                &context(),
                (0..1100).map(|i| file(&format!("{shared}/f-{i}"), b"a")),
                model::Limits::default()
            ),
            Err(ProjectionError::Limit("forge projected path bytes"))
        ));
    }
    const VIEW_VERSION: [u8; 32] = [
        124, 225, 52, 167, 149, 178, 202, 24, 181, 199, 33, 167, 43, 72, 201, 246, 45, 78, 87, 19,
        176, 97, 158, 17, 119, 254, 170, 67, 69, 174, 113, 248,
    ];
    const CANONICAL: [u8; 32] = [
        132, 13, 5, 203, 54, 54, 38, 205, 187, 60, 169, 39, 208, 113, 52, 78, 57, 96, 244, 7, 56,
        7, 74, 190, 206, 120, 213, 111, 121, 106, 156, 31,
    ];
    const BASELINE_COMMIT: [u8; 32] = [
        193, 128, 130, 77, 26, 39, 133, 114, 29, 38, 247, 228, 226, 62, 32, 30, 55, 159, 185, 107,
        79, 169, 173, 110, 100, 68, 197, 75, 89, 251, 112, 231,
    ];
    const BASELINE_TREE: [u8; 32] = [
        55, 82, 200, 186, 190, 207, 9, 210, 91, 172, 23, 106, 76, 248, 231, 158, 211, 242, 178, 72,
        81, 67, 138, 77, 196, 124, 10, 246, 43, 10, 204, 93,
    ];
    const REVIEW_COMMIT: [u8; 32] = [
        188, 155, 231, 129, 127, 191, 198, 171, 192, 78, 67, 94, 146, 132, 209, 109, 26, 142, 194,
        32, 144, 198, 63, 23, 59, 68, 63, 188, 141, 212, 94, 196,
    ];
    const REVIEW_TREE: [u8; 32] = [
        196, 185, 133, 161, 202, 57, 165, 153, 209, 89, 66, 210, 145, 93, 64, 19, 105, 201, 22, 50,
        125, 227, 141, 120, 20, 147, 18, 243, 2, 69, 147, 166,
    ];
    const SOURCE_COPY: [u8; 32] = [
        73, 132, 231, 136, 25, 24, 205, 214, 190, 186, 148, 209, 131, 22, 120, 233, 228, 226, 84,
        24, 27, 170, 180, 152, 183, 176, 2, 30, 134, 249, 255, 29,
    ];
}
