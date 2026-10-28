//! Allocation-bounded structural checks only; stock glob parsing and compilation
//! belong to the semantic matcher factory, under its caller's work reservation.
use crate::query_v1 as q;

pub const MAX_FILESET_NODES: usize = 64;
pub const MAX_FILESET_DEPTH: usize = 16;
pub const MAX_FILESET_STRING_BYTES: usize = 64 * 1024;
pub const MAX_FILESET_ATOM_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FilesetStats {
    pub nodes: usize,
    pub depth: usize,
    pub string_bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilesetValidationError {
    Invalid,
    Limit,
}
impl FilesetValidationError {
    pub const fn message(self) -> &'static str {
        match self {
            Self::Invalid => "invalid fileset matcher",
            Self::Limit => "fileset matcher exceeds structural bounds",
        }
    }
}
impl std::fmt::Display for FilesetValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for FilesetValidationError {}

fn atom(value: &str, stats: &mut FilesetStats) -> Result<(), FilesetValidationError> {
    if value.len() > MAX_FILESET_ATOM_BYTES {
        return Err(FilesetValidationError::Limit);
    }
    stats.string_bytes = stats
        .string_bytes
        .checked_add(value.len())
        .filter(|bytes| *bytes <= MAX_FILESET_STRING_BYTES)
        .ok_or(FilesetValidationError::Limit)?;
    if value.contains('\0') {
        return Err(FilesetValidationError::Invalid);
    }
    Ok(())
}
fn path(value: &str, root: bool, stats: &mut FilesetStats) -> Result<(), FilesetValidationError> {
    atom(value, stats)?;
    if value.is_empty() {
        return if root {
            Ok(())
        } else {
            Err(FilesetValidationError::Invalid)
        };
    }
    if value.contains('\\') || value.split('/').any(|part| matches!(part, "" | "." | "..")) {
        return Err(FilesetValidationError::Invalid);
    }
    Ok(())
}
/// Validate the complete matcher before any semantic compile or source access.
/// Report all nodes/depth/string bytes for the enclosing graph's aggregate charge.
pub fn validate_fileset(
    expression: &q::FilesetExpr,
) -> Result<FilesetStats, FilesetValidationError> {
    use q::fileset_expr::Expression as E;
    let mut stats = FilesetStats::default();
    let mut pending = vec![(expression, 1usize)];
    while let Some((node, depth)) = pending.pop() {
        stats.nodes += 1;
        stats.depth = stats.depth.max(depth);
        if stats.nodes > MAX_FILESET_NODES || depth > MAX_FILESET_DEPTH {
            return Err(FilesetValidationError::Limit);
        }
        match node
            .expression
            .as_ref()
            .ok_or(FilesetValidationError::Invalid)?
        {
            E::Empty(_) | E::All(_) => {}
            E::Literal(value) => path(&value.path, !value.exact_file, &mut stats)?,
            E::Glob(value) => {
                path(&value.directory, true, &mut stats)?;
                atom(&value.pattern, &mut stats)?;
                if value.pattern.is_empty() {
                    return Err(FilesetValidationError::Invalid);
                }
            }
            E::Union(value) => {
                if value.inputs.len()
                    > MAX_FILESET_NODES.saturating_sub(stats.nodes + pending.len())
                {
                    return Err(FilesetValidationError::Limit);
                }
                pending.extend(value.inputs.iter().map(|child| (child, depth + 1)));
            }
            E::Intersection(value) | E::Difference(value) => {
                if stats.nodes + pending.len() > MAX_FILESET_NODES - 2 {
                    return Err(FilesetValidationError::Limit);
                }
                pending.push((
                    value
                        .left
                        .as_deref()
                        .ok_or(FilesetValidationError::Invalid)?,
                    depth + 1,
                ));
                pending.push((
                    value
                        .right
                        .as_deref()
                        .ok_or(FilesetValidationError::Invalid)?,
                    depth + 1,
                ));
            }
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn literal(path: &str, exact_file: bool) -> q::FilesetExpr {
        q::FilesetExpr {
            expression: Some(q::fileset_expr::Expression::Literal(q::FilesetLiteral {
                path: path.into(),
                exact_file,
            })),
        }
    }
    #[test]
    fn public_validator_preserves_root_and_glob_shapes_and_reports_aggregate_cost() {
        let value = q::FilesetExpr {
            expression: Some(q::fileset_expr::Expression::Union(q::FilesetUnion {
                inputs: vec![
                    literal("", false),
                    literal("src/lib.rs", true),
                    q::FilesetExpr {
                        expression: Some(q::fileset_expr::Expression::Glob(q::FilesetGlob {
                            directory: "src".into(),
                            pattern: "**/*.rs".into(),
                            case_insensitive: true,
                            prefix: false,
                        })),
                    },
                ],
            })),
        };
        assert_eq!(
            validate_fileset(&value),
            Ok(FilesetStats {
                nodes: 4,
                depth: 2,
                string_bytes: 20
            })
        );
        for value in [
            q::FilesetExpr::default(),
            literal("", true),
            literal("../src", false),
            literal("a//b", false),
            literal("a\\b", false),
        ] {
            assert_eq!(
                validate_fileset(&value),
                Err(FilesetValidationError::Invalid)
            );
        }
    }
    #[test]
    fn public_validator_refuses_aggregate_width_depth_and_string_limits() {
        let union = |inputs| q::FilesetExpr {
            expression: Some(q::fileset_expr::Expression::Union(q::FilesetUnion {
                inputs,
            })),
        };
        assert_eq!(
            validate_fileset(&union(vec![literal("a", true); 64])),
            Err(FilesetValidationError::Limit)
        );
        let mut deep = literal("a", true);
        for _ in 0..16 {
            deep = union(vec![deep]);
        }
        assert_eq!(validate_fileset(&deep), Err(FilesetValidationError::Limit));
        assert_eq!(
            validate_fileset(&literal(&"a".repeat(4097), true)),
            Err(FilesetValidationError::Limit)
        );
        assert_eq!(
            validate_fileset(&union(vec![literal(&"a".repeat(4096), true); 17])),
            Err(FilesetValidationError::Limit)
        );
    }
}
