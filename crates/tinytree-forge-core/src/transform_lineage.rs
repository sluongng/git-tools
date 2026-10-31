//! Canonical transform lineage semantics, not export or storage authority.
//! Embeddings select endpoints from exact authorized canonical view catalogs.
//! Sanitized copy seeds and caller assertions are not endpoint witnesses.
use serde::{Deserialize, Serialize};
use tinytree_model::{self as model, CopyId, Descriptor, Kind, Limits, Object};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Rename,
    Copy,
}

/// Descriptor-only FILE facts. The embedding proves their selected path,
/// payload availability, view version/epoch, and canonical CopyId ownership.
#[derive(Clone, Debug)]
pub struct Endpoint {
    pub canonical_path: String,
    pub file: Descriptor,
    pub canonical_copy: Option<CopyId>,
}
#[derive(Clone, Debug)]
pub struct RelationInput {
    pub operation: Operation,
    /// Relative hint paths in the before/after selected views, respectively.
    pub source_path: String,
    pub target_path: String,
    pub source: Endpoint,
    pub target: Endpoint,
    pub target_existed_before: bool,
    pub source_exists_after: bool,
}

/// Semantic result only, never a transferable authorization proof. No parent
/// list/salt or arbitrary decoded metadata survives this boundary. Deliberately
/// neither Deserialize nor Serialize: hosts choose their private ledger shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedRelation {
    operation: Operation,
    source_path: String,
    target_path: String,
}
impl VerifiedRelation {
    pub fn operation(&self) -> Operation {
        self.operation
    }
    pub fn source_path(&self) -> &str {
        &self.source_path
    }
    pub fn target_path(&self) -> &str {
        &self.target_path
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineageError {
    Invalid(&'static str),
    Model(model::Error),
}
impl std::fmt::Display for LineageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid transform lineage: {reason}"),
            Self::Model(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for LineageError {}
fn hint_path(value: &str) -> Result<(), LineageError> {
    // Preserve native transform hint-path semantics (not a new scope grant).
    if value.is_empty()
        || value.len() > 4096
        || value.contains('\\')
        || value.chars().any(char::is_control)
        || value
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".." | ".jj" | ".git"))
    {
        return Err(LineageError::Invalid("transform path"));
    }
    Ok(())
}
fn endpoint(value: &Endpoint, limits: Limits) -> Result<(), LineageError> {
    if value.file.key.kind != Kind::File
        || value.file.format_version != model::FORMAT_VERSION
        || value.canonical_path.is_empty()
        || value.canonical_path.len() > limits.max_path_bytes
        || value.canonical_path.contains(['\\', '\0'])
        || value
            .canonical_path
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(LineageError::Invalid("transform lineage endpoint"));
    }
    Ok(())
}
/// Verify actual native target CopyHistory bytes and the direct canonical
/// source-parent relation. Does not walk ancestry or compare FILE contents:
/// rename/copy followed by edits is valid. The host must freshly authorize both
/// exact views and retain their complete canonical roots around metadata I/O.
pub fn verify_relation_bytes(
    input: &RelationInput,
    target_copy_descriptor: Descriptor,
    target_copy_bytes: &[u8],
    limits: Limits,
) -> Result<VerifiedRelation, LineageError> {
    hint_path(&input.source_path)?;
    hint_path(&input.target_path)?;
    endpoint(&input.source, limits)?;
    endpoint(&input.target, limits)?;
    if input.source_path == input.target_path {
        return Err(LineageError::Invalid("transform lineage endpoint"));
    }
    if input.target_existed_before {
        return Err(LineageError::Invalid(
            "transform lineage overwrites existing path",
        ));
    }
    match input.operation {
        Operation::Rename if input.source_exists_after => {
            return Err(LineageError::Invalid("transform rename retained source"));
        }
        Operation::Copy if !input.source_exists_after => {
            return Err(LineageError::Invalid("transform copy removed source"));
        }
        _ => {}
    }
    let source = input
        .source
        .canonical_copy
        .ok_or(LineageError::Invalid("unverified transform copy source"))?;
    let target = input
        .target
        .canonical_copy
        .ok_or(LineageError::Invalid("unverified transform copy target"))?;
    if target_copy_descriptor.key.kind != Kind::CopyHistory
        || target_copy_descriptor.key.id.0 != target.0
    {
        return Err(LineageError::Invalid("unverified transform copy relation"));
    }
    model::verify(&target_copy_descriptor, target_copy_bytes, limits)
        .map_err(LineageError::Model)?;
    let Object::CopyHistory(copy) =
        model::decode(target_copy_bytes, limits).map_err(LineageError::Model)?
    else {
        return Err(LineageError::Invalid("unverified transform copy relation"));
    };
    if copy.current_path != input.target.canonical_path || !copy.parents.contains(&source) {
        return Err(LineageError::Invalid("unverified transform copy relation"));
    }
    Ok(VerifiedRelation {
        operation: input.operation,
        source_path: input.source.canonical_path.clone(),
        target_path: input.target.canonical_path.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(
        operation: Operation,
        parents: Vec<CopyId>,
        path: &str,
    ) -> (RelationInput, model::EncodedObject) {
        let copy = model::encode(
            &Object::CopyHistory(model::CopyHistory {
                current_path: path.into(),
                parents,
                salt: vec![2],
            }),
            Limits::default(),
        )
        .unwrap();
        let input = RelationInput {
            operation,
            source_path: "old.txt".into(),
            target_path: "new.txt".into(),
            source: Endpoint {
                canonical_path: "public/old.txt".into(),
                file: model::describe_file(b"original"),
                canonical_copy: Some(CopyId([1; 32])),
            },
            target: Endpoint {
                canonical_path: "public/new.txt".into(),
                file: model::describe_file(b"edited destination"),
                canonical_copy: Some(CopyId(copy.descriptor.key.id.0)),
            },
            target_existed_before: false,
            source_exists_after: operation == Operation::Copy,
        };
        (input, copy)
    }
    #[test]
    fn native_rename_and_copy_allow_edits_without_exposing_other_parents() {
        for operation in [Operation::Rename, Operation::Copy] {
            let private_parent = CopyId([239; 32]);
            let (input, copy) = fixture(
                operation,
                vec![private_parent, CopyId([1; 32])],
                "public/new.txt",
            );
            assert_ne!(input.source.file.key, input.target.file.key);
            let result =
                verify_relation_bytes(&input, copy.descriptor, &copy.bytes, Limits::default())
                    .unwrap();
            assert_eq!(
                (
                    result.operation(),
                    result.source_path(),
                    result.target_path()
                ),
                (operation, "public/old.txt", "public/new.txt")
            );
            // The only observable semantic result contains endpoint paths and
            // operation, not the private extra parent, its history, or salt.
            assert!(!format!("{result:?}").contains(&"ef".repeat(32)));
            assert!(!format!("{result:?}").contains("parents"));
        }
    }
    #[test]
    fn actual_copy_bytes_and_exact_endpoint_guards_are_required() {
        struct Case {
            name: &'static str,
            operation: Operation,
            path: &'static str,
            parents: Vec<CopyId>,
            before_target: bool,
            after_source: bool,
            seed: bool,
            tamper: bool,
            missing_copy: bool,
        }
        let cases = [
            Case {
                name: "projected seed is not canonical ancestry",
                operation: Operation::Rename,
                path: "public/new.txt",
                parents: vec![],
                before_target: false,
                after_source: false,
                seed: true,
                tamper: false,
                missing_copy: false,
            },
            Case {
                name: "target current path differs",
                operation: Operation::Rename,
                path: "private/new.txt",
                parents: vec![CopyId([1; 32])],
                before_target: false,
                after_source: false,
                seed: false,
                tamper: false,
                missing_copy: false,
            },
            Case {
                name: "direct source parent absent",
                operation: Operation::Rename,
                path: "public/new.txt",
                parents: vec![CopyId([3; 32])],
                before_target: false,
                after_source: false,
                seed: false,
                tamper: false,
                missing_copy: false,
            },
            Case {
                name: "target existed before",
                operation: Operation::Rename,
                path: "public/new.txt",
                parents: vec![CopyId([1; 32])],
                before_target: true,
                after_source: false,
                seed: false,
                tamper: false,
                missing_copy: false,
            },
            Case {
                name: "rename retains source",
                operation: Operation::Rename,
                path: "public/new.txt",
                parents: vec![CopyId([1; 32])],
                before_target: false,
                after_source: true,
                seed: false,
                tamper: false,
                missing_copy: false,
            },
            Case {
                name: "copy removes source",
                operation: Operation::Copy,
                path: "public/new.txt",
                parents: vec![CopyId([1; 32])],
                before_target: false,
                after_source: false,
                seed: false,
                tamper: false,
                missing_copy: false,
            },
            Case {
                name: "canonical bytes corrupted",
                operation: Operation::Rename,
                path: "public/new.txt",
                parents: vec![CopyId([1; 32])],
                before_target: false,
                after_source: false,
                seed: false,
                tamper: true,
                missing_copy: false,
            },
            Case {
                name: "canonical copy identifier missing",
                operation: Operation::Rename,
                path: "public/new.txt",
                parents: vec![CopyId([1; 32])],
                before_target: false,
                after_source: false,
                seed: false,
                tamper: false,
                missing_copy: true,
            },
        ];
        for case in cases {
            let (mut input, mut copy) = fixture(case.operation, case.parents, case.path);
            input.target_existed_before = case.before_target;
            input.source_exists_after = case.after_source;
            if case.seed {
                input.source.canonical_copy = Some(CopyId([8; 32]));
            }
            if case.tamper {
                let last = copy.bytes.last_mut().unwrap();
                *last ^= 1;
            }
            if case.missing_copy {
                input.source.canonical_copy = None;
            }
            assert!(
                verify_relation_bytes(&input, copy.descriptor, &copy.bytes, Limits::default())
                    .is_err(),
                "{}",
                case.name
            );
        }
    }
}
