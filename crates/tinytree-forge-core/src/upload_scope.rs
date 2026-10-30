//! Canonical paged scope requirements. Requirements are not authorization grants:
//! embeddings must resolve current policy and exact typed ownership witnesses.
use serde::{Deserialize, Serialize};
use tinytree_model::{
    self as model, CommitId, Descriptor, Kind, Limits, Object, ObjectId, ObjectKey, TreeValue,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Root,
    Tree,
    CopyAt,
    CopyParent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    pub canonical_path: String,
    pub depth: usize,
    pub role: Role,
}

/// Generated from verified native IDs; the JSON representation is lowercase hex.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Key {
    pub kind: u8,
    pub id: String,
}
impl Key {
    pub fn new(key: ObjectKey) -> Self {
        Self {
            kind: key.kind as u8,
            id: key.id.0.iter().map(|b| format!("{b:02x}")).collect(),
        }
    }
    pub fn object_key(&self) -> Result<ObjectKey, ScopeError> {
        if self.id.len() != 64 {
            return Err(ScopeError::Invalid("forge scope key"));
        }
        let mut id = [0; 32];
        for (i, pair) in self.id.as_bytes().chunks_exact(2).enumerate() {
            let digit = |b| match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'a'..=b'f' => Ok(b - b'a' + 10),
                _ => Err(ScopeError::Invalid("forge scope key")),
            };
            id[i] = digit(pair[0])? * 16 + digit(pair[1])?;
        }
        Ok(ObjectKey {
            kind: Kind::try_from(self.kind).map_err(ScopeError::Model)?,
            id: ObjectId(id),
        })
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct Child {
    pub key: Key,
    pub canonical_path: String,
    pub depth: usize,
    pub role: Role,
}
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Requirement {
    Path {
        canonical_path: String,
    },
    Reference {
        canonical_path: String,
        key: Key,
        expected_size: Option<u64>,
    },
}
#[derive(Clone, Debug, Serialize)]
pub struct Page {
    pub next_cursor: Option<usize>,
    pub total: usize,
    /// Only TREE entries consumed, not COMMIT links or copy work.
    pub entries: usize,
    pub children: Vec<Child>,
    pub requirements: Vec<Requirement>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeError {
    AccessDenied,
    Invalid(&'static str),
    Limit(&'static str),
    Model(model::Error),
}
impl std::fmt::Display for ScopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ScopeError {}

/// Verify every supplied byte before deriving a bounded page of requirements.
/// FILE bodies and closure integrity belong to the embedding's separate verifier.
pub fn page_bytes(
    descriptor: Descriptor,
    bytes: &[u8],
    context: Context,
    cursor: usize,
    limit: usize,
    model_limits: Limits,
) -> Result<Page, ScopeError> {
    if limit == 0 || limit > 128 {
        return Err(ScopeError::Limit("forge scope page"));
    }
    if context.depth > 64 {
        return Err(ScopeError::Limit("forge upload tree depth"));
    }
    if context.canonical_path.len() > model_limits.max_path_bytes {
        return Err(ScopeError::Limit("forge scope path"));
    }
    model::verify(&descriptor, bytes, model_limits).map_err(ScopeError::Model)?;
    if descriptor.key.kind == Kind::File {
        return Err(ScopeError::Invalid("file is not metadata"));
    }
    let object = model::decode(bytes, model_limits).map_err(ScopeError::Model)?;
    let mut page = Page {
        next_cursor: None,
        total: 0,
        entries: 0,
        children: vec![],
        requirements: vec![],
    };
    let path = context.canonical_path;
    let key = |kind, id| {
        Key::new(ObjectKey {
            kind,
            id: ObjectId(id),
        })
    };
    let range = |total: usize| -> Result<std::ops::Range<usize>, ScopeError> {
        if cursor > total {
            return Err(ScopeError::Invalid("forge scope cursor"));
        }
        Ok(cursor..total.min(cursor.saturating_add(limit)))
    };
    match (context.role, object) {
        (Role::Root, Object::Commit(commit)) => {
            if !path.is_empty()
                || context.depth != 0
                || commit.root_tree.terms.len() != 1
                || commit.predecessors.len() > 128
                || commit.parents.len() != 1
            {
                return Err(ScopeError::Invalid("forge uploaded commit shape"));
            }
            // ROOT is an allowed intrinsic parent, never a supplied-byte predecessor.
            if commit.predecessors.contains(&CommitId::ROOT) {
                return Err(ScopeError::AccessDenied);
            }
            page.total = commit.predecessors.len() + 2;
            for i in range(page.total)? {
                if i < commit.predecessors.len() {
                    page.requirements.push(Requirement::Reference {
                        canonical_path: String::new(),
                        key: key(Kind::Commit, commit.predecessors[i].0),
                        expected_size: None,
                    });
                } else if i == commit.predecessors.len() {
                    if commit.parents[0] != CommitId::ROOT {
                        page.requirements.push(Requirement::Reference {
                            canonical_path: String::new(),
                            key: key(Kind::Commit, commit.parents[0].0),
                            expected_size: None,
                        });
                    }
                } else {
                    page.children.push(Child {
                        key: key(Kind::Tree, commit.root_tree.terms[0].0),
                        canonical_path: String::new(),
                        depth: context.depth + 1,
                        role: Role::Tree,
                    });
                }
            }
        }
        (Role::Root | Role::Tree, Object::Tree(tree)) => {
            page.total = tree.entries.len();
            for i in range(page.total)? {
                page.entries += 1;
                let entry = &tree.entries[i];
                let full = if path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{path}/{}", entry.name)
                };
                if full.len() > model_limits.max_path_bytes {
                    return Err(ScopeError::Limit("forge scope path"));
                }
                match entry.value {
                    TreeValue::Tree(id) => page.children.push(Child {
                        key: key(Kind::Tree, id.0),
                        canonical_path: full,
                        depth: context.depth + 1,
                        role: Role::Tree,
                    }),
                    TreeValue::File {
                        id,
                        size_bytes,
                        copy_id,
                        ..
                    } => {
                        if let Some(copy) = copy_id {
                            page.children.push(Child {
                                key: key(Kind::CopyHistory, copy.0),
                                canonical_path: full.clone(),
                                depth: context.depth,
                                role: Role::CopyAt,
                            });
                        }
                        page.requirements.push(Requirement::Path {
                            canonical_path: full.clone(),
                        });
                        page.requirements.push(Requirement::Reference {
                            canonical_path: full,
                            key: key(Kind::File, id.0),
                            expected_size: Some(size_bytes),
                        });
                    }
                    TreeValue::Symlink { id, .. } => {
                        page.requirements.push(Requirement::Path {
                            canonical_path: full.clone(),
                        });
                        page.requirements.push(Requirement::Reference {
                            canonical_path: full,
                            key: key(Kind::Symlink, id.0),
                            expected_size: None,
                        });
                    }
                }
            }
        }
        (Role::Root, Object::Symlink(_)) => {
            page.total = 1;
            for _ in range(1)? {
                page.requirements.push(Requirement::Path {
                    canonical_path: path.clone(),
                });
            }
        }
        (role @ (Role::Root | Role::CopyAt | Role::CopyParent), Object::CopyHistory(history)) => {
            if role != Role::CopyParent && history.current_path != path {
                return Err(ScopeError::AccessDenied);
            }
            if role == Role::Root && history.parents.len() > 16 {
                return Err(ScopeError::AccessDenied);
            }
            page.total = if role == Role::Root {
                history.parents.len() + 1
            } else {
                1
            };
            for i in range(page.total)? {
                if i == 0 {
                    let actual = history.current_path.clone();
                    page.requirements.push(Requirement::Path {
                        canonical_path: actual.clone(),
                    });
                    if role != Role::Root {
                        page.requirements.push(Requirement::Reference {
                            canonical_path: actual,
                            key: Key::new(descriptor.key),
                            expected_size: None,
                        });
                    }
                } else {
                    page.children.push(Child {
                        key: key(Kind::CopyHistory, history.parents[i - 1].0),
                        canonical_path: String::new(),
                        depth: context.depth,
                        role: Role::CopyParent,
                    });
                }
            }
        }
        _ => return Err(ScopeError::Invalid("forge scope role")),
    }
    let next = cursor.saturating_add(limit).min(page.total);
    if next < page.total {
        page.next_cursor = Some(next);
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{CopyHistory, CopyId, Tree, TreeEntry, TreeId};

    #[test]
    fn pages_preserve_empty_directory_scope_and_verify_all_supplied_bytes() {
        let object = model::encode(
            &Object::Tree(Tree {
                entries: (0..129)
                    .map(|i| TreeEntry {
                        name: format!("dir-{i:03}"),
                        value: TreeValue::Tree(TreeId([9; 32])),
                    })
                    .collect(),
            }),
            Limits::default(),
        )
        .unwrap();
        let context = Context {
            canonical_path: "nested".into(),
            depth: 1,
            role: Role::Tree,
        };
        let first = page_bytes(
            object.descriptor,
            &object.bytes,
            context.clone(),
            0,
            128,
            Limits::default(),
        )
        .unwrap();
        assert_eq!(
            (first.total, first.entries, first.next_cursor),
            (129, 128, Some(128))
        );
        assert!(
            first.requirements.is_empty(),
            "empty directory traversal is not a new leaf policy grant"
        );
        assert_eq!(first.children[0].canonical_path, "nested/dir-000");
        assert_eq!(first.children[0].depth, 2);
        let last = page_bytes(
            object.descriptor,
            &object.bytes,
            context.clone(),
            128,
            128,
            Limits::default(),
        )
        .unwrap();
        assert_eq!((last.entries, last.next_cursor), (1, None));
        assert!(
            page_bytes(
                object.descriptor,
                &object.bytes,
                context.clone(),
                130,
                128,
                Limits::default()
            )
            .is_err()
        );
        let mut corrupt = object.bytes.clone();
        corrupt[0] ^= 1;
        assert!(matches!(
            page_bytes(
                object.descriptor,
                &corrupt,
                context,
                0,
                128,
                Limits::default()
            ),
            Err(ScopeError::Model(model::Error::HashMismatch))
        ));
    }

    #[test]
    fn admitted_copy_references_do_not_reauthorize_grandparents() {
        let object = model::encode(
            &Object::CopyHistory(CopyHistory {
                current_path: "src/file".into(),
                parents: vec![CopyId([7; 32])],
                salt: vec![1],
            }),
            Limits::default(),
        )
        .unwrap();
        let context = |role| Context {
            canonical_path: "src/file".into(),
            depth: 0,
            role,
        };
        let referenced = page_bytes(
            object.descriptor,
            &object.bytes,
            context(Role::CopyAt),
            0,
            128,
            Limits::default(),
        )
        .unwrap();
        assert!(referenced.children.is_empty());
        assert_eq!(referenced.requirements.len(), 2);
        assert_eq!(referenced.entries, 0);
        let supplied = page_bytes(
            object.descriptor,
            &object.bytes,
            context(Role::Root),
            0,
            128,
            Limits::default(),
        )
        .unwrap();
        assert_eq!(supplied.requirements.len(), 1);
        assert_eq!(supplied.children.len(), 1);
        assert_eq!(supplied.children[0].role, Role::CopyParent);
        let parent = page_bytes(
            object.descriptor,
            &object.bytes,
            Context {
                canonical_path: String::new(),
                depth: 0,
                role: Role::CopyParent,
            },
            0,
            128,
            Limits::default(),
        )
        .unwrap();
        assert!(parent.children.is_empty());
        assert!(
            matches!(&parent.requirements[1],Requirement::Reference {canonical_path,..} if canonical_path=="src/file")
        );
        assert!(matches!(
            page_bytes(
                object.descriptor,
                &object.bytes,
                Context {
                    canonical_path: "other".into(),
                    depth: 0,
                    role: Role::CopyAt
                },
                0,
                128,
                Limits::default()
            ),
            Err(ScopeError::AccessDenied)
        ));
    }
}
