//! Resolved, bounded fileset transport using the stock jj matcher semantics.
//!
//! Callers reserve `construction_bytes()` before encoding or compiling and retain
//! that debit with the returned matcher. This module owns no provider authority.
use jj_lib::{
    fileset::{FilePattern, FilesetExpression},
    matchers::{Matcher, MatcherBuildBudget, MatcherBuildLimits, PathGlobPattern},
    repo_path::RepoPathBuf,
};
use tinytree_proto::query_v1 as q;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Invalid,
    Limit,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid resolved fileset",
            Self::Limit => "fileset resource limit exceeded",
        })
    }
}
impl std::error::Error for Error {}
fn validate(wire: &q::FilesetExpr) -> Result<tinytree_proto::FilesetStats, Error> {
    tinytree_proto::validate_fileset(wire).map_err(|e| match e {
        tinytree_proto::FilesetValidationError::Invalid => Error::Invalid,
        tinytree_proto::FilesetValidationError::Limit => Error::Limit,
    })
}
// Bound stock globset's recursive alternate-to-regex conversion before parsing.
// Escapes and bracket classes do not introduce alternate recursion. The stock
// parser remains the grammar authority; this pass is resource admission only.
fn glob_depth(pattern: &str) -> Result<(), Error> {
    let mut escaped = false;
    // 0=outside, 1=initial (optional negation), 2=first literal, 3=body.
    let mut class = 0u8;
    let mut depth = 0usize;
    for byte in pattern.bytes() {
        // Stock classes treat backslash as a literal, not as an escape.
        // A leading `]` (also after !/^) is literal and cannot close the class.
        if class != 0 {
            if class == 1 && matches!(byte, b'!' | b'^') {
                class = 2;
                continue;
            }
            if class == 3 && byte == b']' {
                class = 0;
            } else {
                class = 3;
            }
            continue;
        }
        if escaped {
            escaped = false;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            continue;
        }
        match byte {
            b'[' => class = 1,
            b'{' => {
                depth += 1;
                if depth > 16 {
                    return Err(Error::Limit);
                }
            }
            b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}
/// Includes both constructor scratch and the full compiled/cache allowances.
/// It is an admission upper bound, not an estimate of resident heap bytes.
pub fn construction_bytes(wire: &q::FilesetExpr) -> Result<usize, Error> {
    let stats = validate(wire)?;
    let mut pending = vec![wire];
    let mut globs = 0usize;
    while let Some(node) = pending.pop() {
        use q::fileset_expr::Expression as E;
        match node.expression.as_ref().ok_or(Error::Invalid)? {
            E::Glob(value) => {
                glob_depth(&value.pattern)?;
                globs += 1;
            }
            E::Union(v) => pending.extend(v.inputs.iter()),
            E::Intersection(v) | E::Difference(v) => {
                pending.push(v.left.as_deref().ok_or(Error::Invalid)?);
                pending.push(v.right.as_deref().ok_or(Error::Invalid)?);
            }
            _ => {}
        }
    }
    stats
        .string_bytes
        .checked_mul(256)
        .and_then(|n| n.checked_add(stats.nodes * 1024))
        .and_then(|n| n.checked_add(globs * 128 * 1024))
        .and_then(|n| n.checked_add(if globs == 0 { 4096 } else { 1024 * 1024 }))
        .ok_or(Error::Limit)
}
#[derive(Debug)]
pub struct CompiledFileset {
    // Regex cache pool capacity is one. Serialize physical matches/visits so
    // sharing an Arc cannot multiply the bounded engine's working caches.
    inner: std::sync::Mutex<Box<dyn Matcher>>,
    reserved_bytes: usize,
}
impl CompiledFileset {
    pub fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }
}
impl Matcher for CompiledFileset {
    fn matches(&self, path: &jj_lib::repo_path::RepoPath) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .matches(path)
    }
    fn visit(&self, path: &jj_lib::repo_path::RepoPath) -> jj_lib::matchers::Visit {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .visit(path)
    }
}
/// Compile once per predicate. The caller's construction debit must remain held
/// through this call and for the lifetime of the result, including cancellation.
pub fn compile(wire: &q::FilesetExpr) -> Result<CompiledFileset, Error> {
    compile_with_budget(
        wire,
        &mut MatcherBuildBudget::new(MatcherBuildLimits::default()),
    )
}
/// Reuse the same compiler ledger across all predicates retained by a cursor.
pub fn compile_with_budget(
    wire: &q::FilesetExpr,
    budget: &mut MatcherBuildBudget,
) -> Result<CompiledFileset, Error> {
    let reserved_bytes = construction_bytes(wire)?;
    fn decode(w: &q::FilesetExpr) -> Result<FilesetExpression, Error> {
        use q::fileset_expr::Expression as E;
        Ok(match w.expression.as_ref().ok_or(Error::Invalid)? {
            E::Empty(_) => FilesetExpression::None,
            E::All(_) => FilesetExpression::All,
            E::Literal(v) => {
                let path =
                    RepoPathBuf::from_internal_string(&v.path).map_err(|_| Error::Invalid)?;
                FilesetExpression::Pattern(if v.exact_file {
                    FilePattern::FilePath(path)
                } else {
                    FilePattern::PrefixPath(path)
                })
            }
            E::Glob(v) => {
                let dir =
                    RepoPathBuf::from_internal_string(&v.directory).map_err(|_| Error::Invalid)?;
                let pattern = Box::new(
                    if v.case_insensitive {
                        PathGlobPattern::parse_i(&v.pattern)
                    } else {
                        PathGlobPattern::parse(&v.pattern)
                    }
                    .map_err(|_| Error::Invalid)?,
                );
                FilesetExpression::Pattern(if v.prefix {
                    FilePattern::PrefixGlob { dir, pattern }
                } else {
                    FilePattern::FileGlob { dir, pattern }
                })
            }
            E::Union(v) => {
                FilesetExpression::UnionAll(v.inputs.iter().map(decode).collect::<Result<_, _>>()?)
            }
            E::Intersection(v) | E::Difference(v) => {
                let left = Box::new(decode(v.left.as_deref().ok_or(Error::Invalid)?)?);
                let right = Box::new(decode(v.right.as_deref().ok_or(Error::Invalid)?)?);
                if matches!(w.expression, Some(E::Intersection(_))) {
                    FilesetExpression::Intersection(left, right)
                } else {
                    FilesetExpression::Difference(left, right)
                }
            }
        })
    }
    let stock = decode(wire)?;
    let inner = stock.to_matcher_bounded(budget).map_err(|_| Error::Limit)?;
    Ok(CompiledFileset {
        inner: std::sync::Mutex::new(inner),
        reserved_bytes,
    })
}
/// Encode only already-resolved repository-relative stock expressions. Bounds
/// are checked before cloning strings or constructing recursive wire vectors.
pub fn from_stock(stock: &FilesetExpression) -> Result<q::FilesetExpr, Error> {
    fn encode(
        s: &FilesetExpression,
        depth: usize,
        nodes: &mut usize,
        bytes: &mut usize,
    ) -> Result<q::FilesetExpr, Error> {
        if depth > 16 || *nodes >= 64 {
            return Err(Error::Limit);
        }
        *nodes += 1;
        fn text(s: &str, bytes: &mut usize) -> Result<String, Error> {
            *bytes = bytes.checked_add(s.len()).ok_or(Error::Limit)?;
            if s.len() > 4096 || *bytes > 65536 {
                return Err(Error::Limit);
            }
            Ok(s.to_owned())
        }
        use q::fileset_expr::Expression as E;
        let expression = match s {
            FilesetExpression::None => E::Empty(q::EmptyFileset {}),
            FilesetExpression::All => E::All(q::EmptyFileset {}),
            FilesetExpression::Pattern(p) => match p {
                FilePattern::FilePath(path) | FilePattern::PrefixPath(path) => {
                    E::Literal(q::FilesetLiteral {
                        path: text(path.as_internal_file_string(), bytes)?,
                        exact_file: matches!(p, FilePattern::FilePath(_)),
                    })
                }
                FilePattern::FileGlob { dir, pattern }
                | FilePattern::PrefixGlob { dir, pattern } => E::Glob(q::FilesetGlob {
                    directory: text(dir.as_internal_file_string(), bytes)?,
                    pattern: {
                        glob_depth(pattern.as_str())?;
                        text(pattern.as_str(), bytes)?
                    },
                    case_insensitive: pattern.case_insensitive(),
                    prefix: matches!(p, FilePattern::PrefixGlob { .. }),
                }),
            },
            FilesetExpression::UnionAll(inputs) => {
                if inputs.len() > 64usize.saturating_sub(*nodes) {
                    return Err(Error::Limit);
                }
                E::Union(q::FilesetUnion {
                    inputs: inputs
                        .iter()
                        .map(|s| encode(s, depth + 1, nodes, bytes))
                        .collect::<Result<_, _>>()?,
                })
            }
            FilesetExpression::Intersection(a, b) | FilesetExpression::Difference(a, b) => {
                let pair = Box::new(q::FilesetBinary {
                    left: Some(Box::new(encode(a, depth + 1, nodes, bytes)?)),
                    right: Some(Box::new(encode(b, depth + 1, nodes, bytes)?)),
                });
                if matches!(s, FilesetExpression::Intersection(..)) {
                    E::Intersection(pair)
                } else {
                    E::Difference(pair)
                }
            }
        };
        Ok(q::FilesetExpr {
            expression: Some(expression),
        })
    }
    let wire = encode(stock, 1, &mut 0, &mut 0)?;
    validate(&wire)?;
    Ok(wire)
}
