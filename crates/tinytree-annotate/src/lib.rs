//! Bounded fixed-path annotation over one globally filtered graph.
//!
//! Adapters supply already source-checked path metadata and demand-driven file
//! inputs. The engine never grants read authority from a graph or FILE id, and
//! does not own an RPC, store, task, or physical admission permit. Callers retain
//! those owners until this synchronous work and its output have been torn down.

use std::sync::Arc;
use tinytree_model as m;

/// Explicit accounting and cancellation shared by graph, merge, and line work.
pub trait Budget {
    type Error;
    fn check(&self) -> Result<(), Self::Error>;
    fn charge(&mut self, items: u64, bytes: u64) -> Result<(), Self::Error>;
    fn frontier(&mut self, entries: usize) -> Result<(), Self::Error>;
    /// The bounded diff polls this independently of ordinary accounting calls.
    fn diff_active(&self) -> bool;
}

/// Source-checked metadata for the exact selected path at one immutable commit.
/// Parents are ordered; the key must put every parent strictly before its child.
pub struct SourceNode<K> {
    pub key: K,
    pub parents: Vec<m::CommitId>,
    pub value: Option<m::TreeValue>,
}

/// Graph operations must use the same captured source and cumulative budget.
pub trait GraphSource: Budget {
    type Key: Copy + Ord;
    /// Validate source authority before reading COMMIT/TREE/path metadata.
    /// A parentless non-root commit must include the intrinsic root as parent.
    /// An authenticated canonical frontier can supply intrinsic NONFORCED path
    /// membership. Local/private nodes return None and are computed globally.
    fn affects_path(&mut self, _id: m::CommitId) -> Result<Option<bool>, Self::Error> { Ok(None) }
    fn node(&mut self, id: m::CommitId) -> Result<SourceNode<Self::Key>, Self::Error>;
    fn common_ancestors(
        &mut self,
        left: &[m::CommitId],
        right: &[m::CommitId],
    ) -> Result<Vec<m::CommitId>, Self::Error>;
    fn is_ancestor(
        &mut self,
        ancestor: m::CommitId,
        descendant: m::CommitId,
    ) -> Result<bool, Self::Error>;
}

/// Demand inputs are only values returned by this adapter's checked path source.
/// An implementation must not interpret an arbitrary caller-supplied FILE id as
/// authority. It retains its selected source and accounts/verifies exact bytes.
pub trait ContentSource: Budget {
    fn read_file(&mut self, value: &m::TreeValue) -> Result<Vec<u8>, Self::Error>;
}

pub trait Source: GraphSource + ContentSource {}
impl<T: GraphSource + ContentSource> Source for T {}

#[derive(Clone, Copy)]
pub struct Limits {
    pub max_commits: usize,
    pub max_lines: usize,
    pub max_file_bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Origin {
    pub commit: m::CommitId,
    pub line: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Boundary {
    ScanLimit,
    ReadLimit,
    DiffLimit,
}
#[derive(Debug)]
pub enum Error<E> {
    Source(E),
    Boundary(Boundary),
    InvalidGraph(&'static str),
    Internal(&'static str),
    DiffDeadline,
}
type Step<T, E> = Result<T, Error<E>>;

fn count_lines(bytes: &[u8]) -> usize {
    bytes.iter().filter(|byte| **byte == b'\n').count()
        + usize::from(bytes.last().is_some_and(|byte| *byte != b'\n'))
}
use jj_lib::merge::Merge;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, PartialEq, Eq)]
struct Value(Option<m::TreeValue>);
struct Node<K> {
    key: K,
    parents: Vec<m::CommitId>,
    value: Value,
    selected: bool,
    edges: Vec<m::CommitId>,
}
struct Graph<K> {
    nodes: BTreeMap<m::CommitId, Node<K>>,
    limits: Limits,
    order: Vec<m::CommitId>,
    files: BTreeMap<m::FileId, m::TreeValue>,
    bodies: BTreeMap<m::FileId, Arc<[u8]>>,
}
fn charge<R: Budget>(reader: &mut R, count: usize, size: usize) -> Step<(), R::Error> {
    reader.check().map_err(Error::Source)?;
    let bytes = count
        .checked_mul(size)
        .ok_or(Error::Boundary(Boundary::DiffLimit))?;
    reader
        .charge(count as u64, bytes as u64)
        .map_err(Error::Source)?;
    Ok(())
}
fn equal<R: Budget, T: Eq>(
    reader: &mut R,
    left: &T,
    right: &T,
    bytes: usize,
) -> Step<bool, R::Error> {
    charge(reader, 1, bytes.saturating_add(1))?;
    Ok(left == right)
}
// Stock Merge's choices, but with admission/cancellation inside every comparison
// and compaction. The upstream convenience methods have uninterruptible nested
// loops and are deliberately not called on request-controlled term lists.
fn trivial<'a, R: Budget, T: Eq>(
    reader: &mut R,
    values: &'a Merge<T>,
    bytes: impl Fn(&T, &T) -> usize,
) -> Step<Option<&'a T>, R::Error> {
    let terms = values.as_slice();
    if let [add] = terms {
        return Ok(Some(add));
    }
    if let [a, remove, b] = terms {
        return Ok(if equal(reader, a, b, bytes(a, b))? {
            Some(a)
        } else if equal(reader, a, remove, bytes(a, remove))? {
            Some(b)
        } else if equal(reader, b, remove, bytes(b, remove))? {
            Some(a)
        } else {
            None
        });
    }
    charge(reader, terms.len(), 64)?;
    let mut counts: Vec<(&T, i32)> = Vec::with_capacity(terms.len());
    for (index, term) in terms.iter().enumerate() {
        let delta = if index % 2 == 0 { 1 } else { -1 };
        let mut found = false;
        for (value, count) in &mut counts {
            if equal(reader, term, value, bytes(term, value))? {
                *count += delta;
                found = true;
                break;
            }
        }
        if !found {
            counts.push((term, delta));
        }
    }
    counts.retain(|(_, count)| *count != 0);
    Ok(match counts.as_slice() {
        [(value, 1)] => Some(*value),
        [(a, n), (b, m)] if n + m == 1 => Some(if *n > 0 { *a } else { *b }),
        _ => None,
    })
}
fn simplify<R: Budget, T: Clone + Eq>(
    reader: &mut R,
    values: &Merge<T>,
) -> Step<Merge<T>, R::Error> {
    let terms = values.as_slice();
    charge(
        reader,
        terms.len(),
        std::mem::size_of::<T>()
            .saturating_mul(4)
            .saturating_add(32),
    )?;
    let mut mapping: Vec<usize> = (0..terms.len()).collect();
    let mut add = 0;
    while add < mapping.len() {
        let mut matched = None;
        for remove in (1..mapping.len()).step_by(2) {
            if equal(
                reader,
                &terms[mapping[add]],
                &terms[mapping[remove]],
                std::mem::size_of::<T>(),
            )? {
                matched = Some(remove);
                break;
            }
        }
        if let Some(remove) = matched {
            charge(reader, mapping.len(), 8)?;
            mapping.swap(remove + 1, add);
            mapping.drain(remove..remove + 2);
        } else {
            add += 2;
        }
    }
    Ok(Merge::from_vec(
        mapping
            .into_iter()
            .map(|index| terms[index].clone())
            .collect::<Vec<_>>(),
    ))
}
impl<K: Copy + Ord> Graph<K> {
    fn load<R: Source<Key = K>>(
        reader: &mut R,
        heads: &[m::CommitId],
        forced: Option<m::CommitId>,
        limits: Limits,
        scanned: &mut u32,
    ) -> Step<Self, R::Error> {
        let mut graph = Self {
            nodes: BTreeMap::new(),
            limits,
            order: Vec::new(),
            files: BTreeMap::new(),
            bodies: BTreeMap::new(),
        };
        let mut queued = BTreeSet::new();
        let mut pending = VecDeque::from(heads.to_vec());
        queued.extend(heads.iter().copied());
        while let Some(id) = pending.pop_front() {
            charge(reader, 1, 512)?;
            if id != m::CommitId::ROOT && *scanned as usize >= limits.max_commits {
                return Err(Error::Boundary(Boundary::ScanLimit));
            }
            let source = reader.node(id).map_err(Error::Source)?;
            let key = source.key;
            let (parents, value) = (source.parents, Value(source.value));
            if id != m::CommitId::ROOT {
                *scanned += 1;
            }
            charge(reader, parents.len(), 96)?;
            for parent in &parents {
                if queued.insert(*parent) {
                    pending.push_back(*parent);
                }
            }
            if let Some(value @ m::TreeValue::File { id, .. }) = &value.0 {
                graph.files.insert(*id, value.clone());
            }
            graph.nodes.insert(
                id,
                Node {
                    key,
                    parents,
                    value,
                    selected: forced == Some(id),
                    edges: Vec::new(),
                },
            );
            reader
                .frontier(queued.len().saturating_add(pending.len()))
                .map_err(Error::Source)?;
        }
        graph.order = graph.nodes.keys().copied().collect();
        // The adapter verifies ordered parent metadata and supplies stable keys
        // that place every parent before its child. This reproduces stable
        // graph-walk order at convergence across all captured source layers.
        graph.order.sort_unstable_by_key(|id| graph.nodes[id].key);
        for (id, node) in &graph.nodes {
            for parent in &node.parents {
                if graph.nodes[parent].key >= node.key || parent == id {
                    return Err(Error::InvalidGraph("DAG history is not topological"));
                }
            }
        }
        Ok(graph)
    }

    fn body<R: ContentSource>(
        &mut self,
        reader: &mut R,
        value: &Value,
    ) -> Step<Arc<[u8]>, R::Error> {
        let Some(m::TreeValue::File { id, .. }) = &value.0 else {
            return Ok(Arc::from([]));
        };
        if let Some(bytes) = self.bodies.get(id) {
            return Ok(bytes.clone());
        }
        let bytes = reader
            .read_file(value.0.as_ref().unwrap())
            .map_err(Error::Source)?;
        if count_lines(&bytes) > self.limits.max_lines {
            return Err(Error::Boundary(Boundary::DiffLimit));
        }
        let bytes: Arc<[u8]> = bytes.into();
        self.bodies.insert(*id, bytes.clone());
        Ok(bytes)
    }

    fn merge_commits<R: GraphSource<Key = K>>(
        &self,
        reader: &mut R,
        commits: Vec<m::CommitId>,
    ) -> Step<Merge<m::CommitId>, R::Error> {
        struct Frame {
            ids: Vec<m::CommitId>,
            result: Merge<m::CommitId>,
            next: usize,
        }
        let trivial = |ids: &[m::CommitId]| match ids {
            [] => Some(Merge::resolved(m::CommitId::ROOT)),
            [id] => Some(Merge::resolved(*id)),
            _ => None,
        };
        if let Some(result) = trivial(&commits) {
            return Ok(result);
        }
        charge(reader, commits.len(), 96)?;
        let mut stack = vec![Frame {
            result: Merge::resolved(commits[0]),
            ids: commits,
            next: 1,
        }];
        loop {
            reader.check().map_err(Error::Source)?;
            let top = stack.last_mut().unwrap();
            let ancestor = if top.next < top.ids.len() {
                let common = reader
                    .common_ancestors(&top.ids[..top.next], &top.ids[top.next..top.next + 1])
                    .map_err(Error::Source)?;
                charge(reader, common.len().saturating_add(1), 96)?;
                if let Some(ancestor) = trivial(&common) {
                    ancestor
                } else {
                    stack.push(Frame {
                        result: Merge::resolved(common[0]),
                        ids: common,
                        next: 1,
                    });
                    continue;
                }
            } else {
                let complete = stack.pop().unwrap();
                if stack.is_empty() {
                    return Ok(complete.result);
                }
                complete.result
            };
            let top = stack.last_mut().unwrap();
            let terms = top
                .result
                .as_slice()
                .len()
                .checked_add(ancestor.as_slice().len())
                .and_then(|n| n.checked_add(1))
                .ok_or(Error::Boundary(Boundary::DiffLimit))?;
            charge(reader, terms, 128)?;
            let previous = std::mem::replace(&mut top.result, Merge::resolved(m::CommitId::ROOT));
            top.result =
                Merge::from_vec(vec![previous, ancestor, Merge::resolved(top.ids[top.next])])
                    .flatten();
            top.next += 1;
        }
    }

    fn parent_value<R: Source<Key = K>>(
        &mut self,
        reader: &mut R,
        parents: Vec<m::CommitId>,
        diff_limits: &mut jj_lib::diff::LineDiffLimits,
    ) -> Step<Option<Value>, R::Error> {
        let commits = self.merge_commits(reader, parents)?;
        charge(reader, commits.as_slice().len(), 256)?;
        let values = commits.map(|id| self.nodes[id].value.clone());
        if let Some(value) = trivial(reader, &values, |_, _| 128)? {
            return Ok(Some(value.clone()));
        }
        let values = simplify(reader, &values)?;
        if !values
            .iter()
            .all(|value| matches!(value.0, Some(m::TreeValue::File { .. })))
        {
            return Ok(None);
        }
        let executable = values.map(|value| match value.0 {
            Some(m::TreeValue::File { executable, .. }) => executable,
            _ => unreachable!(),
        });
        let copy_ids = values.map(|value| match value.0 {
            Some(m::TreeValue::File { copy_id, .. }) => copy_id,
            _ => unreachable!(),
        });
        let (Some(&executable), Some(&copy_id)) = (
            trivial(reader, &executable, |_, _| 1)?,
            trivial(reader, &copy_ids, |_, _| 32)?,
        ) else {
            return Ok(None);
        };
        let ids = values.map(|value| match value.0 {
            Some(m::TreeValue::File { id, .. }) => id,
            _ => unreachable!(),
        });
        if let Some(id) = trivial(reader, &ids, |_, _| 32)? {
            let Some(m::TreeValue::File { size_bytes, .. }) = self.files.get(id) else {
                return Err(Error::InvalidGraph("merge file source missing"));
            };
            return Ok(Some(Value(Some(m::TreeValue::File {
                id: *id,
                executable,
                size_bytes: *size_bytes,
                copy_id,
            }))));
        }
        let ids = simplify(reader, &ids)?;
        charge(reader, ids.as_slice().len(), 128)?;
        let mut contents = Vec::with_capacity(ids.as_slice().len());
        for id in ids.iter() {
            let value = Value(Some(
                self.files
                    .get(id)
                    .ok_or(Error::InvalidGraph("merge source FILE missing"))?
                    .clone(),
            ));
            contents.push(self.body(reader, &value)?);
        }
        let contents = Merge::from_vec(contents);
        let inputs: Vec<_> = contents
            .removes()
            .chain(contents.adds())
            .map(|bytes| bytes.as_ref())
            .collect();
        let num_removes = contents.removes().len();
        let diff = bounded_diff(&inputs, diff_limits, reader)?;
        let mut output = Vec::new();
        for hunk in diff.hunks() {
            let term_bytes = hunk
                .contents
                .iter()
                .map(|content| content.len())
                .sum::<usize>();
            charge(
                reader,
                hunk.contents.len().saturating_add(1),
                term_bytes.saturating_add(128),
            )?;
            let content = match hunk.kind {
                jj_lib::diff::DiffHunkKind::Matching => hunk.contents[0],
                jj_lib::diff::DiffHunkKind::Different => {
                    let merge = Merge::from_removes_adds(
                        hunk.contents[..num_removes].iter().copied(),
                        hunk.contents[num_removes..].iter().copied(),
                    );
                    let Some(&content) = trivial(reader, &merge, |a, b| a.len().min(b.len()))?
                    else {
                        return Ok(None);
                    };
                    content
                }
            };
            if output
                .len()
                .checked_add(content.len())
                .is_none_or(|size| size > self.limits.max_file_bytes)
            {
                return Err(Error::Boundary(Boundary::ReadLimit));
            }
            output.extend_from_slice(content);
        }
        // This is comparison-only derived content, never a CAS read capability
        // or a raw target descriptor in the response.
        let id = m::FileId(*blake3::hash(&output).as_bytes());
        Ok(Some(Value(Some(m::TreeValue::File {
            id,
            executable,
            size_bytes: output.len() as u64,
            copy_id,
        }))))
    }

    fn select<R: Source<Key = K>>(
        &mut self,
        reader: &mut R,
        forced: Option<m::CommitId>,
        diff_limits: &mut jj_lib::diff::LineDiffLimits,
    ) -> Step<(), R::Error> {
        for id in self.order.clone() {
            if id == m::CommitId::ROOT {
                continue;
            }
            let parents = self.nodes[&id].parents.clone();
            let selected = if forced == Some(id) {
                true
            } else if let Some(selected) = reader.affects_path(id).map_err(Error::Source)? {
                selected
            } else {
                let before = self.parent_value(reader, parents.clone(), diff_limits)?;
                before.as_ref() != Some(&self.nodes[&id].value)
            };
            let mut edges = Vec::new();
            let mut expanded = false;
            let mut indirect_seen = BTreeSet::new();
            for parent in parents {
                charge(reader, 1, 64)?;
                if self.nodes[&parent].selected {
                    edges.push(parent);
                } else {
                    expanded |= !self.nodes[&parent].edges.is_empty();
                    for edge in &self.nodes[&parent].edges {
                        charge(reader, 1, 64)?;
                        if indirect_seen.insert(*edge) {
                            edges.push(*edge);
                        }
                    }
                }
            }
            // Stock preserves redundant direct edges, but removes transitive
            // targets when at least one edge bypasses a filtered-out parent.
            if expanded {
                let mut keep = Vec::with_capacity(edges.len());
                for edge in &edges {
                    let mut redundant = false;
                    for other in &edges {
                        reader.check().map_err(Error::Source)?;
                        if edge != other
                            && reader.is_ancestor(*edge, *other).map_err(Error::Source)?
                        {
                            redundant = true;
                            break;
                        }
                    }
                    if !redundant {
                        keep.push(*edge);
                    }
                }
                edges = keep;
            }
            let node = self.nodes.get_mut(&id).unwrap();
            node.selected = selected;
            node.edges = edges;
        }
        Ok(())
    }
}

fn bounded_diff<'a, R: Budget>(
    inputs: &[&'a [u8]],
    remaining: &mut jj_lib::diff::LineDiffLimits,
    reader: &R,
) -> Step<jj_lib::diff::ContentDiff<'a>, R::Error> {
    let mut budget = jj_lib::diff::LineDiffBudget::new(*remaining);
    let result = jj_lib::diff::ContentDiff::by_line_bounded_many(inputs, &mut budget, || {
        reader.diff_active()
    });
    let used = budget.usage();
    remaining.max_input_bytes = remaining.max_input_bytes.saturating_sub(used.input_bytes);
    remaining.max_lines = remaining.max_lines.saturating_sub(used.lines);
    remaining.max_work = remaining.max_work.saturating_sub(used.work);
    match result {
        Ok(diff) => Ok(diff),
        Err(jj_lib::diff::LineDiffError::Cancelled) => Err(Error::DiffDeadline),
        Err(jj_lib::diff::LineDiffError::LimitExceeded) => {
            Err(Error::Boundary(Boundary::DiffLimit))
        }
    }
}

/// Annotate the actual forced target over its complete, globally filtered DAG.
///
/// The caller must verify/bound `target` and `target_id` against its exact source
/// before calling, and retain that source/admission while the engine runs.
/// `origins` has exactly one entry per target line. On a typed boundary, origins
/// already finalized by complete filtered parent lists are retained, matching
/// canonical policy2 partial results; callers must not label them complete.
/// Graph and input adapters may mix private and canonical sources, but forcing
/// extra boundary heads or composing independently attributed segments is wrong.
pub fn annotate<R: Source>(
    reader: &mut R,
    revision: m::CommitId,
    target: &[u8],
    target_id: m::FileId,
    limits: Limits,
    diff_limits: &mut jj_lib::diff::LineDiffLimits,
    scanned: &mut u32,
    origins: &mut [Option<Origin>],
) -> Step<(), R::Error> {
    // The adapter has already read and verified this exact target, bounded its
    // bytes/lines, and initialized one output slot per line before graph work.
    let lines = origins.len();
    let mut graph = Graph::load(reader, &[revision], Some(revision), limits, scanned)?;
    graph.bodies.insert(target_id, Arc::from(target.to_vec()));
    graph.select(reader, Some(revision), diff_limits)?;
    let mut pending = BTreeMap::from([(
        revision,
        (0..lines).map(|line| (line, line)).collect::<Vec<_>>(),
    )]);
    for id in graph.order.clone().into_iter().rev() {
        let Some(mut current) = pending.remove(&id) else {
            continue;
        };
        let value = graph.nodes[&id].value.clone();
        let bytes = graph.body(reader, &value)?;
        for parent in graph.nodes[&id].edges.clone() {
            if current.is_empty() {
                break;
            }
            charge(reader, current.len(), 64)?;
            let value = graph.nodes[&parent].value.clone();
            let parent_bytes = graph.body(reader, &value)?;
            let diff = bounded_diff(&[&bytes, &parent_bytes], diff_limits, reader)?;
            let mut remaining = Vec::with_capacity(current.len());
            let mut matched = Vec::with_capacity(current.len());
            let (mut cursor, mut current_line, mut parent_line) = (0, 0, 0);
            for hunk in diff.hunks() {
                reader.check().map_err(Error::Source)?;
                let end = current_line + count_lines(hunk.contents[0]);
                while cursor < current.len() && current[cursor].0 < end {
                    let (line, selected) = current[cursor];
                    if line < current_line {
                        return Err(Error::Internal("DAG blame mapping is not ordered"));
                    }
                    if hunk.kind == jj_lib::diff::DiffHunkKind::Matching {
                        matched.push((parent_line + line - current_line, selected));
                    } else {
                        remaining.push((line, selected));
                    }
                    cursor += 1;
                }
                current_line = end;
                parent_line += count_lines(hunk.contents[1]);
            }
            if cursor != current.len() {
                return Err(Error::Internal("DAG blame omitted selected lines"));
            }
            current = remaining;
            if !matched.is_empty() {
                let target = pending.entry(parent).or_insert_with(Vec::new);
                charge(reader, target.len().saturating_add(matched.len()), 64)?;
                let previous = std::mem::take(target);
                let mut merged = Vec::with_capacity(previous.len() + matched.len());
                let mut left = previous.into_iter().peekable();
                let mut right = matched.into_iter().peekable();
                while let (Some(a), Some(b)) = (left.peek(), right.peek()) {
                    reader.check().map_err(Error::Source)?;
                    merged.push(if a <= b {
                        left.next().unwrap()
                    } else {
                        right.next().unwrap()
                    });
                }
                for value in left.chain(right) {
                    reader.check().map_err(Error::Source)?;
                    merged.push(value);
                }
                *target = merged;
            }
        }
        for (line, selected) in current {
            origins[selected] = Some(Origin { commit: id, line });
        }
    }
    if !pending.is_empty() || origins.iter().any(Option::is_none) {
        return Err(Error::Internal("DAG blame left unresolved graph mappings"));
    }
    Ok(())
}

/// Metadata for one node in a complete canonical ancestry union. Selection is
/// intrinsic `files(path)` membership: boundary heads are never forced.
pub struct FrontierNode<K> {
    pub id: m::CommitId,
    pub key: K,
    pub parents: Vec<m::CommitId>,
    pub value: Option<m::TreeValue>,
    pub affects_path: bool,
}

/// Compute one bounded complete union, or return a typed boundary/error. No
/// partial vector is returned as complete and no historical body is returned.
pub fn frontier<R: Source>(
    reader: &mut R,
    heads: &[m::CommitId],
    limits: Limits,
    diff_limits: &mut jj_lib::diff::LineDiffLimits,
    scanned: &mut u32,
) -> Step<Vec<FrontierNode<R::Key>>, R::Error> {
    let mut graph = Graph::load(reader, heads, None, limits, scanned)?;
    graph.select(reader, None, diff_limits)?;
    charge(
        reader,
        graph.order.len(),
        std::mem::size_of::<FrontierNode<R::Key>>(),
    )?;
    let mut result = Vec::with_capacity(graph.order.len());
    for id in graph.order {
        reader.check().map_err(Error::Source)?;
        let node = graph.nodes.remove(&id).unwrap();
        result.push(FrontierNode {
            id,
            key: node.key,
            parents: node.parents,
            value: node.value.0,
            affects_path: node.selected,
        });
    }
    Ok(result)
}
