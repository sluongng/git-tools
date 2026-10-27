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
    fn affects_path(&mut self, _id: m::CommitId) -> Result<Option<bool>, Self::Error> {
        Ok(None)
    }
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
// std's B-tree nodes have up to eleven key/value slots and twelve child
// edges. Charging a whole node per inserted entry covers even sparse initial
// nodes, headers, alignment and unused slots for large generic Copy keys.
fn btree_entry_bytes<K, V>() -> usize {
    std::mem::size_of::<K>()
        .saturating_add(std::mem::size_of::<V>())
        .saturating_mul(11)
        .saturating_add(std::mem::size_of::<usize>().saturating_mul(12))
        .saturating_add(256)
        .saturating_add(
            std::mem::align_of::<K>()
                .max(std::mem::align_of::<V>())
                .saturating_mul(4),
        )
}
struct Discovery<K> {
    graph: Graph<K>,
    pending: VecDeque<m::CommitId>,
    queued: BTreeSet<m::CommitId>,
    ordered: BTreeSet<(K, m::CommitId)>,
    forced: Option<m::CommitId>,
    maximum_nodes: usize,
}
impl<K: Copy + Ord> Discovery<K> {
    fn new<R: Source<Key = K>>(
        reader: &mut R,
        heads: &[m::CommitId],
        forced: Option<m::CommitId>,
        limits: Limits,
    ) -> Step<Self, R::Error> {
        let maximum_nodes = limits
            .max_commits
            .checked_add(1)
            .ok_or(Error::Boundary(Boundary::ScanLimit))?;
        if heads.len() > maximum_nodes {
            return Err(Error::Boundary(Boundary::ScanLimit));
        }
        // Fixed queue/order capacities avoid uncharged growth during a wide
        // single-node quantum. Two times each requested capacity also covers
        // allocator rounding, rather than assuming every slot is occupied.
        charge(
            reader,
            maximum_nodes,
            std::mem::size_of::<m::CommitId>().saturating_mul(4),
        )?;
        charge(reader, heads.len(), btree_entry_bytes::<m::CommitId, ()>())?;
        reader.frontier(heads.len()).map_err(Error::Source)?;
        let queued: BTreeSet<_> = heads.iter().copied().collect();
        let mut pending = VecDeque::with_capacity(maximum_nodes);
        pending.extend(queued.iter().copied());
        let order = Vec::with_capacity(maximum_nodes);
        Ok(Self {
            pending,
            queued,
            ordered: BTreeSet::new(),
            forced,
            maximum_nodes,
            graph: Graph {
                nodes: BTreeMap::new(),
                limits,
                order,
                files: BTreeMap::new(),
                bodies: BTreeMap::new(),
            },
        })
    }
    // One discovery or topological validation operation. Queues and ordering
    // survive page boundaries; no previously examined node is read again.
    fn step<R: Source<Key = K>>(
        &mut self,
        reader: &mut R,
        scanned: &mut u32,
    ) -> Step<bool, R::Error> {
        if let Some(id) = self.pending.pop_front() {
            charge(
                reader,
                1,
                btree_entry_bytes::<m::CommitId, Node<K>>()
                    .saturating_add(btree_entry_bytes::<(K, m::CommitId), ()>()),
            )?;
            if id != m::CommitId::ROOT && *scanned as usize >= self.graph.limits.max_commits {
                return Err(Error::Boundary(Boundary::ScanLimit));
            }
            let source = reader.node(id).map_err(Error::Source)?;
            if id != m::CommitId::ROOT {
                *scanned = scanned
                    .checked_add(1)
                    .ok_or(Error::Boundary(Boundary::ScanLimit))?;
            }
            charge(
                reader,
                source.parents.capacity(),
                std::mem::size_of::<m::CommitId>(),
            )?;
            for parent in &source.parents {
                if !self.queued.contains(parent) {
                    if self.queued.len() >= self.maximum_nodes {
                        return Err(Error::Boundary(Boundary::ScanLimit));
                    }
                    charge(reader, 1, btree_entry_bytes::<m::CommitId, ()>())?;
                    self.queued.insert(*parent);
                    self.pending.push_back(*parent);
                }
            }
            if let Some(value @ m::TreeValue::File { id, .. }) = &source.value {
                if !self.graph.files.contains_key(id) {
                    charge(reader, 1, btree_entry_bytes::<m::FileId, m::TreeValue>())?;
                    self.graph.files.insert(*id, value.clone());
                }
            }
            self.ordered.insert((source.key, id));
            self.graph.nodes.insert(
                id,
                Node {
                    key: source.key,
                    parents: source.parents,
                    value: Value(source.value),
                    selected: self.forced == Some(id),
                    edges: Vec::new(),
                },
            );
            reader
                .frontier(self.queued.len().saturating_add(self.pending.len()))
                .map_err(Error::Source)?;
            return Ok(false);
        }
        if let Some((_, id)) = self.ordered.pop_first() {
            charge(reader, 1, 64)?;
            let node = &self.graph.nodes[&id];
            for parent in &node.parents {
                charge(reader, 1, 0)?;
                let parent_node = self
                    .graph
                    .nodes
                    .get(parent)
                    .ok_or(Error::InvalidGraph("DAG parent missing"))?;
                if parent_node.key >= node.key || parent == &id {
                    return Err(Error::InvalidGraph("DAG history is not topological"));
                }
            }
            self.graph.order.push(id);
            return Ok(false);
        }
        Ok(true)
    }
}

impl<K: Copy + Ord> Graph<K> {
    fn load<R: Source<Key = K>>(
        reader: &mut R,
        heads: &[m::CommitId],
        forced: Option<m::CommitId>,
        limits: Limits,
        scanned: &mut u32,
    ) -> Step<Self, R::Error> {
        let mut discovery = Discovery::new(reader, heads, forced, limits)?;
        while !discovery.step(reader, scanned)? {}
        Ok(discovery.graph)
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
        if bytes.len() > self.limits.max_file_bytes {
            return Err(Error::Boundary(Boundary::ReadLimit));
        }
        charge(
            reader,
            1,
            bytes
                .capacity()
                .saturating_add(bytes.len())
                .saturating_add(64)
                .saturating_add(btree_entry_bytes::<m::FileId, Arc<[u8]>>()),
        )?;
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
        for position in 0..self.order.len() {
            self.select_node(reader, self.order[position], forced, diff_limits)?;
        }
        Ok(())
    }
    fn select_node<R: Source<Key = K>>(
        &mut self,
        reader: &mut R,
        id: m::CommitId,
        forced: Option<m::CommitId>,
        diff_limits: &mut jj_lib::diff::LineDiffLimits,
    ) -> Step<(), R::Error> {
        if id == m::CommitId::ROOT {
            return Ok(());
        }
        charge(
            reader,
            self.nodes[&id].parents.len(),
            std::mem::size_of::<m::CommitId>().saturating_mul(2),
        )?;
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
                    charge(
                        reader,
                        1,
                        64usize.saturating_add(btree_entry_bytes::<m::CommitId, ()>()),
                    )?;
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
                    if edge != other && reader.is_ancestor(*edge, *other).map_err(Error::Source)? {
                        redundant = true;
                        break;
                    }
                }
                if !redundant {
                    charge(reader, 1, 64)?;
                    keep.push(*edge);
                }
            }
            edges = keep;
        }
        let node = self.nodes.get_mut(&id).unwrap();
        node.selected = selected;
        node.edges = edges;

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
    let mut graph = Graph::load(reader, &[revision], Some(revision), limits, scanned)?;
    cache_target(&mut graph, reader, target, target_id)?;
    graph.select(reader, Some(revision), diff_limits)?;
    annotate_graph(&mut graph, reader, revision, diff_limits, origins).map(|_| ())
}

fn cache_target<K: Copy + Ord, R: Budget>(
    graph: &mut Graph<K>,
    reader: &mut R,
    target: &[u8],
    target_id: m::FileId,
) -> Step<(), R::Error> {
    charge(
        reader,
        1,
        target
            .len()
            .checked_mul(2)
            .and_then(|bytes| {
                bytes.checked_add(
                    64usize.saturating_add(btree_entry_bytes::<m::FileId, Arc<[u8]>>()),
                )
            })
            .ok_or(Error::Boundary(Boundary::ReadLimit))?,
    )?;
    graph.bodies.insert(target_id, Arc::from(target.to_vec()));
    Ok(())
}

// Attribution over an already selected graph. Both one-shot and resumable
// callers use this loop; neither segments independently attributed ancestry.
fn annotate_graph<K: Copy + Ord, R: Source<Key = K>>(
    graph: &mut Graph<K>,
    reader: &mut R,
    revision: m::CommitId,
    diff_limits: &mut jj_lib::diff::LineDiffLimits,
    origins: &mut [Option<Origin>],
) -> Step<u64, R::Error> {
    let lines = origins.len();
    charge(reader, lines, 32)?;
    let mut operations = 0u64;
    charge(
        reader,
        1,
        btree_entry_bytes::<m::CommitId, Vec<(usize, usize)>>(),
    )?;
    let mut pending = BTreeMap::from([(
        revision,
        (0..lines).map(|line| (line, line)).collect::<Vec<_>>(),
    )]);
    charge(
        reader,
        graph.order.len(),
        std::mem::size_of::<m::CommitId>(),
    )?;
    for id in graph.order.clone().into_iter().rev() {
        reader.check().map_err(Error::Source)?;
        let Some(mut current) = pending.remove(&id) else {
            continue;
        };
        operations = operations
            .checked_add(1)
            .ok_or(Error::Internal("annotation progress overflow"))?;
        let value = graph.nodes[&id].value.clone();
        let bytes = graph.body(reader, &value)?;
        charge(
            reader,
            graph.nodes[&id].edges.len(),
            std::mem::size_of::<m::CommitId>().saturating_mul(2),
        )?;
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
                if !pending.contains_key(&parent) {
                    charge(
                        reader,
                        1,
                        btree_entry_bytes::<m::CommitId, Vec<(usize, usize)>>(),
                    )?;
                }
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
    Ok(operations)
}

/// Metadata for one node in a complete canonical ancestry union. Selection is
/// intrinsic `files(path)` membership: boundary heads are never forced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontierNode<K> {
    pub id: m::CommitId,
    pub key: K,
    pub parents: Vec<m::CommitId>,
    pub value: Option<m::TreeValue>,
    pub affects_path: bool,
}

/// Compute one bounded complete union, or return a typed boundary/error. No
/// partial vector is returned as complete and no historical body is returned.
/// Resumable frontier phase. Ready permits output; Done means output was drained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontierPhase {
    Discovering,
    Selecting,
    Ready,
    Done,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontierProgress {
    pub phase: FrontierPhase,
    pub scanned_commits: u32,
    /// Cumulative bounded engine operations, including metadata-only progress.
    pub operations: u64,
    pub work_items: u64,
    /// Conservative cumulative allocation charge; never refunded between pages.
    /// Sources separately account their provider buffers and physical reads.
    pub retained_bytes_upper_bound: u64,
}
/// One exact immutable ancestry union. Reuse the SAME source, cumulative budget
/// and absolute deadline for every call. The caller owns cursor replay and source
/// lifetime; the engine owns no task, RPC permit or external FILE capability.
/// An error poisons the state: retrying must not repeat partially charged work.
/// Advances checkpoint between nodes; each node's merge/diff remains separately
/// bounded by the original aggregate diff limits and cancellation checks.
pub struct Frontier<K> {
    discovery: Discovery<K>,
    heads: Vec<m::CommitId>,
    phase: FrontierPhase,
    selected: usize,
    output: usize,
    scanned: u32,
    operations: u64,
    work_items: u64,
    retained: u64,
    diff_limits: jj_lib::diff::LineDiffLimits,
    failed: bool,
}
struct Accounting<'a, R> {
    source: &'a mut R,
    work: &'a mut u64,
    bytes: &'a mut u64,
}
impl<R: Budget> Budget for Accounting<'_, R> {
    type Error = R::Error;
    fn check(&self) -> Result<(), Self::Error> {
        self.source.check()
    }
    fn charge(&mut self, items: u64, bytes: u64) -> Result<(), Self::Error> {
        self.source.charge(items, bytes)?;
        *self.work = self.work.saturating_add(items);
        *self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }
    fn frontier(&mut self, entries: usize) -> Result<(), Self::Error> {
        self.source.frontier(entries)
    }
    fn diff_active(&self) -> bool {
        self.source.diff_active()
    }
}
impl<R: GraphSource> GraphSource for Accounting<'_, R> {
    type Key = R::Key;
    fn node(&mut self, id: m::CommitId) -> Result<SourceNode<Self::Key>, Self::Error> {
        self.source.node(id)
    }
    fn affects_path(&mut self, id: m::CommitId) -> Result<Option<bool>, Self::Error> {
        self.source.affects_path(id)
    }
    fn common_ancestors(
        &mut self,
        left: &[m::CommitId],
        right: &[m::CommitId],
    ) -> Result<Vec<m::CommitId>, Self::Error> {
        self.source.common_ancestors(left, right)
    }
    fn is_ancestor(
        &mut self,
        ancestor: m::CommitId,
        descendant: m::CommitId,
    ) -> Result<bool, Self::Error> {
        self.source.is_ancestor(ancestor, descendant)
    }
}
impl<R: ContentSource> ContentSource for Accounting<'_, R> {
    fn read_file(&mut self, value: &m::TreeValue) -> Result<Vec<u8>, Self::Error> {
        self.source.read_file(value)
    }
}
impl<K: Copy + Ord> Frontier<K> {
    pub fn new<R: Source<Key = K>>(
        reader: &mut R,
        heads: &[m::CommitId],
        limits: Limits,
        diff_limits: jj_lib::diff::LineDiffLimits,
    ) -> Step<Self, R::Error> {
        let (mut work_items, mut retained) = (0, 0);
        let mut accounting = Accounting {
            source: reader,
            work: &mut work_items,
            bytes: &mut retained,
        };
        charge(
            &mut accounting,
            1,
            std::mem::size_of::<Self>().saturating_add(512),
        )?;
        let discovery = Discovery::new(&mut accounting, heads, None, limits)?;
        charge(
            &mut accounting,
            heads.len(),
            std::mem::size_of::<m::CommitId>(),
        )?;
        Ok(Self {
            discovery,
            heads: heads.to_vec(),
            phase: FrontierPhase::Discovering,
            selected: 0,
            output: 0,
            scanned: 0,
            operations: 0,
            work_items,
            retained,
            diff_limits,
            failed: false,
        })
    }
    /// Attribute one verified target using the READY retained graph. No node may
    /// have been emitted, and the exact original single anchor is the target.
    /// Callers retain the same source, aggregate budget and absolute deadline.
    pub fn annotate<R: Source<Key = K>>(
        &mut self,
        reader: &mut R,
        revision: m::CommitId,
        target: &[u8],
        target_id: m::FileId,
        origins: &mut [Option<Origin>],
    ) -> Step<(), R::Error> {
        if self.failed {
            return Err(Error::Internal("annotation frontier is poisoned"));
        }
        self.failed = true;
        if self.phase != FrontierPhase::Ready
            || self.output != 0
            || self.heads.as_slice() != [revision]
        {
            return Err(Error::InvalidGraph(
                "server attribution requires one un-emitted ready target",
            ));
        }
        let graph = &mut self.discovery.graph;
        if target.len() > graph.limits.max_file_bytes || origins.len() > graph.limits.max_lines {
            return Err(Error::Boundary(Boundary::ReadLimit));
        }
        if origins.len() != count_lines(target) || origins.iter().any(Option::is_some) || graph.nodes.get(&revision).is_none_or(|node|
            !matches!(&node.value.0, Some(m::TreeValue::File { id,size_bytes,.. }) if *id == target_id && *size_bytes == target.len() as u64)) {
            return Err(Error::InvalidGraph("annotation target differs from retained source"));
        }
        let mut reader = Accounting {
            source: reader,
            work: &mut self.work_items,
            bytes: &mut self.retained,
        };
        cache_target(graph, &mut reader, target, target_id)?;
        // The original frontier used intrinsic membership. Force only the real
        // target; its ancestors' globally filtered membership remains unchanged.
        graph.select_node(&mut reader, revision, Some(revision), &mut self.diff_limits)?;
        let processed =
            annotate_graph(graph, &mut reader, revision, &mut self.diff_limits, origins)?;
        self.operations = self
            .operations
            .checked_add(processed)
            .and_then(|n| n.checked_add(1))
            .ok_or(Error::Internal("annotation progress overflow"))?;
        self.output = graph.order.len();
        self.phase = FrontierPhase::Done;
        self.failed = false;
        Ok(())
    }

    pub fn progress(&self) -> FrontierProgress {
        FrontierProgress {
            phase: self.phase,
            scanned_commits: self.scanned,
            operations: self.operations,
            work_items: self.work_items,
            retained_bytes_upper_bound: self.retained,
        }
    }
    pub fn remaining_diff_limits(&self) -> jj_lib::diff::LineDiffLimits {
        self.diff_limits
    }
    pub fn advance<R: Source<Key = K>>(
        &mut self,
        reader: &mut R,
        quantum: std::num::NonZeroUsize,
    ) -> Step<FrontierProgress, R::Error> {
        if self.failed {
            return Err(Error::Internal("annotation frontier is poisoned"));
        }
        self.failed = true;
        let mut reader = Accounting {
            source: reader,
            work: &mut self.work_items,
            bytes: &mut self.retained,
        };
        reader.check().map_err(Error::Source)?;
        for _ in 0..quantum.get() {
            match self.phase {
                FrontierPhase::Discovering => {
                    if self.discovery.step(&mut reader, &mut self.scanned)? {
                        self.phase = FrontierPhase::Selecting;
                    }
                }
                FrontierPhase::Selecting => {
                    if let Some(id) = self.discovery.graph.order.get(self.selected).copied() {
                        self.discovery.graph.select_node(
                            &mut reader,
                            id,
                            None,
                            &mut self.diff_limits,
                        )?;
                        self.selected += 1;
                    } else {
                        self.phase = FrontierPhase::Ready;
                    }
                }
                FrontierPhase::Ready | FrontierPhase::Done => break,
            }
            self.operations = self
                .operations
                .checked_add(1)
                .ok_or(Error::Internal("annotation progress overflow"))?;
        }
        self.failed = false;
        Ok(self.progress())
    }
    /// Move one parent-first node out, without cloning a complete result vector.
    /// Calling before Ready is an error and poisons this context.
    pub fn next_node<R: Source<Key = K>>(
        &mut self,
        reader: &mut R,
    ) -> Step<Option<FrontierNode<K>>, R::Error> {
        if self.failed {
            return Err(Error::Internal("annotation frontier is poisoned"));
        }
        self.failed = true;
        reader.check().map_err(Error::Source)?;
        if !matches!(self.phase, FrontierPhase::Ready | FrontierPhase::Done) {
            return Err(Error::Internal("annotation frontier is not ready"));
        }
        let Some(id) = self.discovery.graph.order.get(self.output).copied() else {
            self.phase = FrontierPhase::Done;
            self.failed = false;
            return Ok(None);
        };
        charge(
            &mut Accounting {
                source: reader,
                work: &mut self.work_items,
                bytes: &mut self.retained,
            },
            1,
            std::mem::size_of::<FrontierNode<K>>(),
        )?;
        let node = self
            .discovery
            .graph
            .nodes
            .remove(&id)
            .ok_or(Error::Internal("annotation output node missing"))?;
        self.output += 1;
        self.operations = self
            .operations
            .checked_add(1)
            .ok_or(Error::Internal("annotation progress overflow"))?;
        if self.output == self.discovery.graph.order.len() {
            self.phase = FrontierPhase::Done;
        }
        self.failed = false;
        Ok(Some(FrontierNode {
            id,
            key: node.key,
            parents: node.parents,
            value: node.value.0,
            affects_path: node.selected,
        }))
    }
}

pub fn frontier<R: Source>(
    reader: &mut R,
    heads: &[m::CommitId],
    limits: Limits,
    diff_limits: &mut jj_lib::diff::LineDiffLimits,
    scanned: &mut u32,
) -> Step<Vec<FrontierNode<R::Key>>, R::Error> {
    let mut state = Frontier::new(reader, heads, limits, *diff_limits)?;
    state.scanned = *scanned;
    let result = (|| {
        while !matches!(
            state.progress().phase,
            FrontierPhase::Ready | FrontierPhase::Done
        ) {
            state.advance(reader, std::num::NonZeroUsize::new(128).unwrap())?;
        }
        charge(
            reader,
            state.discovery.graph.order.len(),
            std::mem::size_of::<FrontierNode<R::Key>>(),
        )?;
        let mut output = Vec::with_capacity(state.discovery.graph.order.len());
        while let Some(node) = state.next_node(reader)? {
            output.push(node);
        }
        Ok(output)
    })();
    *scanned = state.progress().scanned_commits;
    *diff_limits = state.remaining_diff_limits();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    #[derive(Clone)]
    struct Fixture {
        nodes: BTreeMap<m::CommitId, (u64, Vec<m::CommitId>, Option<m::TreeValue>)>,
        bodies: BTreeMap<m::FileId, Vec<u8>>,
        reads: BTreeMap<m::CommitId, usize>,
        file_reads: usize,
        work: u64,
        bytes: u64,
        max_work: u64,
        max_bytes: u64,
        active: bool,
    }
    fn id(number: u64) -> m::CommitId {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&number.to_be_bytes());
        m::CommitId(bytes)
    }
    impl Fixture {
        fn new() -> Self {
            Self {
                nodes: BTreeMap::from([(m::CommitId::ROOT, (0, vec![], None))]),
                bodies: BTreeMap::new(),
                reads: BTreeMap::new(),
                file_reads: 0,
                work: 0,
                bytes: 0,
                max_work: 1_000_000,
                max_bytes: u64::MAX,
                active: true,
            }
        }
        fn file(&mut self, bytes: &[u8]) -> m::TreeValue {
            let file = m::FileId(*blake3::hash(bytes).as_bytes());
            self.bodies.insert(file, bytes.to_vec());
            m::TreeValue::File {
                id: file,
                executable: false,
                size_bytes: bytes.len() as u64,
                copy_id: None,
            }
        }
        fn chain(length: u64) -> Self {
            let mut fixture = Self::new();
            let value = fixture.file(b"same\nsame\n");
            for number in 1..=length {
                let parent = if number == 1 {
                    m::CommitId::ROOT
                } else {
                    id(number - 1)
                };
                fixture
                    .nodes
                    .insert(id(number), (number, vec![parent], Some(value.clone())));
            }
            fixture
        }
        fn ancestors(&self, heads: &[m::CommitId]) -> BTreeSet<m::CommitId> {
            let mut output = BTreeSet::new();
            let mut pending = heads.to_vec();
            while let Some(id) = pending.pop() {
                if output.insert(id) {
                    pending.extend(&self.nodes[&id].1);
                }
            }
            output
        }
    }
    impl Budget for Fixture {
        type Error = &'static str;
        fn check(&self) -> Result<(), Self::Error> {
            if self.active {
                Ok(())
            } else {
                Err("cancelled")
            }
        }
        fn charge(&mut self, work: u64, bytes: u64) -> Result<(), Self::Error> {
            self.check()?;
            self.work += work;
            self.bytes += bytes;
            if self.work > self.max_work {
                Err("work exhausted")
            } else if self.bytes > self.max_bytes {
                Err("bytes exhausted")
            } else {
                Ok(())
            }
        }
        fn frontier(&mut self, entries: usize) -> Result<(), Self::Error> {
            if entries <= 4096 {
                Ok(())
            } else {
                Err("frontier exhausted")
            }
        }
        fn diff_active(&self) -> bool {
            self.active
        }
    }
    impl GraphSource for Fixture {
        type Key = u64;
        fn node(&mut self, id: m::CommitId) -> Result<SourceNode<u64>, Self::Error> {
            *self.reads.entry(id).or_default() += 1;
            let (key, parents, value) = self.nodes.get(&id).ok_or("missing node")?;
            let mut copied_parents = Vec::with_capacity(parents.capacity());
            copied_parents.extend(parents.iter().copied());
            Ok(SourceNode {
                key: *key,
                parents: copied_parents,
                value: value.clone(),
            })
        }
        fn common_ancestors(
            &mut self,
            left: &[m::CommitId],
            right: &[m::CommitId],
        ) -> Result<Vec<m::CommitId>, Self::Error> {
            let left = self.ancestors(left);
            let right = self.ancestors(right);
            let shared: Vec<_> = left.intersection(&right).copied().collect();
            Ok(shared
                .iter()
                .copied()
                .filter(|candidate| {
                    !shared.iter().any(|other| {
                        candidate != other && self.ancestors(&[*other]).contains(candidate)
                    })
                })
                .collect())
        }
        fn is_ancestor(
            &mut self,
            ancestor: m::CommitId,
            descendant: m::CommitId,
        ) -> Result<bool, Self::Error> {
            Ok(self.ancestors(&[descendant]).contains(&ancestor))
        }
    }
    impl ContentSource for Fixture {
        fn read_file(&mut self, value: &m::TreeValue) -> Result<Vec<u8>, Self::Error> {
            self.file_reads += 1;
            let m::TreeValue::File { id, .. } = value else {
                return Err("not a file");
            };
            self.bodies.get(id).cloned().ok_or("missing file")
        }
    }
    fn limits(max_commits: usize) -> Limits {
        Limits {
            max_commits,
            max_lines: 1024,
            max_file_bytes: 1024,
        }
    }
    fn diff_limits() -> jj_lib::diff::LineDiffLimits {
        jj_lib::diff::LineDiffLimits {
            max_input_bytes: 1_000_000,
            max_lines: 100_000,
            max_work: 1_000_000,
            max_allocation_bytes: 1_000_000,
            max_recursion_depth: 64,
        }
    }
    fn drain(
        source: &mut Fixture,
        state: &mut Frontier<u64>,
        quantum: usize,
    ) -> Vec<FrontierNode<u64>> {
        while state.progress().phase != FrontierPhase::Ready {
            let before = state.progress();
            let after = state
                .advance(source, NonZeroUsize::new(quantum).unwrap())
                .unwrap();
            assert!(after.operations > before.operations);
            assert!(after.operations - before.operations <= quantum as u64);
            assert!(after.retained_bytes_upper_bound >= before.retained_bytes_upper_bound);
        }
        let mut output = vec![];
        while let Some(node) = state.next_node(source).unwrap() {
            output.push(node);
        }
        output
    }
    #[test]
    fn deep_frontier_advances_with_positive_empty_progress_without_rereading_nodes() {
        let mut source = Fixture::chain(300);
        let mut state = Frontier::new(&mut source, &[id(300)], limits(300), diff_limits()).unwrap();
        let first = state
            .advance(&mut source, NonZeroUsize::new(7).unwrap())
            .unwrap();
        assert_eq!(first.phase, FrontierPhase::Discovering);
        assert_eq!(first.scanned_commits, 7);
        assert_eq!(first.operations, 7);
        assert_eq!(source.file_reads, 0, "discovery never hydrates FILE bodies");
        let output = drain(&mut source, &mut state, 7);
        assert_eq!(output.len(), 301);
        assert_eq!(output[0].id, m::CommitId::ROOT);
        assert_eq!(output.last().unwrap().id, id(300));
        assert_eq!(state.progress().scanned_commits, 300);
        assert!(source.reads.values().all(|reads| *reads == 1));
        assert_eq!(
            source.file_reads, 0,
            "equal path values need no historical bodies"
        );
        let before = (source.work, source.bytes, state.progress());
        state
            .advance(&mut source, NonZeroUsize::new(7).unwrap())
            .unwrap();
        assert!(state.next_node(&mut source).unwrap().is_none());
        assert_eq!(
            before,
            (source.work, source.bytes, state.progress()),
            "idle completion does not recharge; page replay belongs to caller"
        );
    }
    #[test]
    fn ready_frontier_attributes_deep_target_without_rediscovery_or_emission() {
        let mut source = Fixture::chain(300);
        let target = b"same\nsame\n";
        let target_id = m::FileId(*blake3::hash(target).as_bytes());
        let mut expected = vec![None; 2];
        annotate(
            &mut source.clone(),
            id(300),
            target,
            target_id,
            limits(300),
            &mut diff_limits(),
            &mut 0,
            &mut expected,
        )
        .unwrap();
        let mut state = Frontier::new(&mut source, &[id(300)], limits(300), diff_limits()).unwrap();
        while state.progress().phase != FrontierPhase::Ready {
            state
                .advance(&mut source, NonZeroUsize::new(7).unwrap())
                .unwrap();
        }
        let reads = source.reads.clone();
        let mut origins = vec![None; 2];
        state
            .annotate(&mut source, id(300), target, target_id, &mut origins)
            .unwrap();
        assert_eq!(origins, expected);
        assert_eq!(
            source.reads, reads,
            "attribution must not reload the discovered graph"
        );
        assert!(source.reads.values().all(|n| *n == 1));
        assert_eq!(state.progress().phase, FrontierPhase::Done);
        assert!(
            state.next_node(&mut source).unwrap().is_none(),
            "server attribution emits no graph nodes"
        );
        assert!(
            state
                .annotate(&mut source, id(300), target, target_id, &mut origins)
                .is_err()
        );
    }

    #[test]
    fn selected_target_copy_charges_bytes_without_consuming_a_node_per_byte() {
        let mut source = Fixture::new();
        let target = vec![b'x'; 128 * 1024];
        let value = source.file(&target);
        source
            .nodes
            .insert(id(1), (1, vec![m::CommitId::ROOT], Some(value)));
        source.max_work = 100_000;
        let bounds = Limits {
            max_commits: 1,
            max_lines: 1,
            max_file_bytes: 256 * 1024,
        };
        let mut ready = Frontier::new(&mut source, &[id(1)], bounds, diff_limits()).unwrap();
        while ready.progress().phase != FrontierPhase::Ready {
            ready
                .advance(&mut source, NonZeroUsize::new(1).unwrap())
                .unwrap();
        }
        let mut origins = [None];
        ready
            .annotate(
                &mut source,
                id(1),
                &target,
                m::FileId(*blake3::hash(&target).as_bytes()),
                &mut origins,
            )
            .unwrap();
        assert_eq!(
            origins,
            [Some(Origin {
                commit: id(1),
                line: 0
            })]
        );
        assert!(
            source.work < 1000,
            "copy size is charged to byte allowance, not graph-node allowance"
        );
        assert!(source.bytes >= target.len() as u64 * 2);
    }

    #[test]
    fn server_attribution_rejects_unready_wrong_or_emitted_targets() {
        for (name, ready, emit, wrong_id) in [
            ("unready", false, false, false),
            ("emitted", true, true, false),
            ("wrong target file", true, false, true),
        ] {
            let mut source = Fixture::chain(2);
            let target = b"same\nsame\n";
            let mut state = Frontier::new(&mut source, &[id(2)], limits(2), diff_limits()).unwrap();
            if ready {
                while state.progress().phase != FrontierPhase::Ready {
                    state
                        .advance(&mut source, NonZeroUsize::new(2).unwrap())
                        .unwrap();
                }
            }
            if emit {
                state.next_node(&mut source).unwrap();
            }
            let target_id = if wrong_id {
                m::FileId([9; 32])
            } else {
                m::FileId(*blake3::hash(target).as_bytes())
            };
            assert!(
                state
                    .annotate(&mut source, id(2), target, target_id, &mut [None; 2])
                    .is_err(),
                "{name}"
            );
            let before = (source.work, source.reads.clone());
            assert!(
                state
                    .advance(&mut source, NonZeroUsize::new(1).unwrap())
                    .is_err()
            );
            assert_eq!(
                before,
                (source.work, source.reads),
                "{name} cannot reset a failed attempt"
            );
        }
    }

    #[test]
    fn aggregate_scan_work_and_cancellation_fail_closed_without_restart() {
        for (name, scan_limit, work_limit, cancel, expected) in [
            ("aggregate scan", 16, 1_000_000, false, "scan"),
            ("aggregate work", 300, 400, false, "work"),
            ("cancellation", 300, 1_000_000, true, "cancelled"),
        ] {
            let mut source = Fixture::chain(300);
            source.max_work = work_limit;
            let mut state =
                Frontier::new(&mut source, &[id(300)], limits(scan_limit), diff_limits()).unwrap();
            state
                .advance(&mut source, NonZeroUsize::new(2).unwrap())
                .unwrap();
            source.active = !cancel;
            loop {
                if let Err(error) = state.advance(&mut source, NonZeroUsize::new(3).unwrap()) {
                    let actual = match error {
                        Error::Boundary(Boundary::ScanLimit) => "scan",
                        Error::Source("work exhausted") => "work",
                        Error::Source("cancelled") => "cancelled",
                        other => panic!("unexpected {name} error: {other:?}"),
                    };
                    assert_eq!(actual, expected, "{name}");
                    break;
                }
            }
            let before = (source.work, source.reads.clone());
            assert!(
                state
                    .advance(&mut source, NonZeroUsize::new(3).unwrap())
                    .is_err()
            );
            assert!(state.next_node(&mut source).is_err());
            assert_eq!(
                before,
                (source.work, source.reads),
                "{name} cannot restart spent work"
            );
        }
    }
    #[test]
    fn wide_discovery_and_large_keys_reserve_container_capacity_before_transfer() {
        let mut wide = Fixture::chain(1024);
        let mut parents = Vec::with_capacity(2048);
        parents.extend((1..=1024).map(id));
        wide.nodes.insert(id(1025), (1025, parents, None));
        let mut state = Frontier::new(&mut wide, &[id(1025)], limits(1025), diff_limits()).unwrap();
        state
            .advance(&mut wide, NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(
            wide.reads.len(),
            1,
            "one quantum retains the wide queue before reading parents"
        );
        assert!(
            state.progress().retained_bytes_upper_bound >= (2048 * 32 + 1024 * 96) as u64,
            "upper bound covers spare parent capacity and the queued identity copies"
        );
        let mut tight = Fixture::chain(1024);
        let mut parents = Vec::with_capacity(2048);
        parents.extend((1..=1024).map(id));
        tight.nodes.insert(id(1025), (1025, parents, None));
        tight.max_bytes = 200_000;
        let mut state =
            Frontier::new(&mut tight, &[id(1025)], limits(1025), diff_limits()).unwrap();
        assert!(matches!(
            state.advance(&mut tight, NonZeroUsize::new(1).unwrap()),
            Err(Error::Source("bytes exhausted"))
        ));
        assert!(
            state.next_node(&mut tight).is_err(),
            "failed wide transfer cannot publish a frontier"
        );

        #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
        struct LargeKey([u8; 4096]);
        struct LargeSource(Fixture);
        impl Budget for LargeSource {
            type Error = &'static str;
            fn check(&self) -> Result<(), Self::Error> {
                self.0.check()
            }
            fn charge(&mut self, work: u64, bytes: u64) -> Result<(), Self::Error> {
                self.0.charge(work, bytes)
            }
            fn frontier(&mut self, n: usize) -> Result<(), Self::Error> {
                self.0.frontier(n)
            }
            fn diff_active(&self) -> bool {
                self.0.diff_active()
            }
        }
        impl GraphSource for LargeSource {
            type Key = LargeKey;
            fn node(&mut self, id: m::CommitId) -> Result<SourceNode<LargeKey>, Self::Error> {
                let node = self.0.node(id)?;
                let mut key = [0; 4096];
                key[..8].copy_from_slice(&node.key.to_be_bytes());
                Ok(SourceNode {
                    key: LargeKey(key),
                    parents: node.parents,
                    value: node.value,
                })
            }
            fn common_ancestors(
                &mut self,
                a: &[m::CommitId],
                b: &[m::CommitId],
            ) -> Result<Vec<m::CommitId>, Self::Error> {
                self.0.common_ancestors(a, b)
            }
            fn is_ancestor(&mut self, a: m::CommitId, b: m::CommitId) -> Result<bool, Self::Error> {
                self.0.is_ancestor(a, b)
            }
        }
        impl ContentSource for LargeSource {
            fn read_file(&mut self, value: &m::TreeValue) -> Result<Vec<u8>, Self::Error> {
                self.0.read_file(value)
            }
        }
        let mut large = LargeSource(Fixture::chain(1));
        let mut state = Frontier::new(&mut large, &[id(1)], limits(1), diff_limits()).unwrap();
        state
            .advance(&mut large, NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert!(
            state.progress().retained_bytes_upper_bound
                >= (22 * std::mem::size_of::<LargeKey>()) as u64,
            "two initially sparse B-tree nodes still allocate full key-slot arrays"
        );
        let mut tight = LargeSource(Fixture::chain(1));
        tight.0.max_bytes = 40_000;
        let mut state = Frontier::new(&mut tight, &[id(1)], limits(1), diff_limits()).unwrap();
        assert!(matches!(
            state.advance(&mut tight, NonZeroUsize::new(1).unwrap()),
            Err(Error::Source("bytes exhausted"))
        ));
    }

    #[test]
    fn paged_frontier_matches_single_shot_for_merge_and_repeated_line_attribution() {
        let mut fixture = Fixture::new();
        for (number, parents, bytes) in [
            (1, vec![m::CommitId::ROOT], b"same\nsame\n".as_slice()),
            (2, vec![id(1)], b"left\nsame\n".as_slice()),
            (3, vec![id(1)], b"same\nright\n".as_slice()),
            (4, vec![id(2), id(3)], b"left\nright\n".as_slice()),
        ] {
            let value = fixture.file(bytes);
            fixture
                .nodes
                .insert(id(number), (number, parents, Some(value)));
        }
        let mut one = fixture.clone();
        let expected = frontier(&mut one, &[id(4)], limits(4), &mut diff_limits(), &mut 0).unwrap();
        let mut paged = fixture.clone();
        let mut state = Frontier::new(&mut paged, &[id(4)], limits(4), diff_limits()).unwrap();
        let actual = drain(&mut paged, &mut state, 1);
        assert_eq!(actual, expected);
        assert_eq!(paged.reads, one.reads);
        assert_eq!(paged.file_reads, one.file_reads);
        // Reuse the published NONFORCED membership as the mixed source does;
        // forcing only the actual target must preserve the complete DAG oracle.
        struct Selected {
            source: Fixture,
            flags: BTreeMap<m::CommitId, bool>,
        }
        impl Budget for Selected {
            type Error = &'static str;
            fn check(&self) -> Result<(), Self::Error> {
                self.source.check()
            }
            fn charge(&mut self, n: u64, bytes: u64) -> Result<(), Self::Error> {
                self.source.charge(n, bytes)
            }
            fn frontier(&mut self, n: usize) -> Result<(), Self::Error> {
                self.source.frontier(n)
            }
            fn diff_active(&self) -> bool {
                self.source.diff_active()
            }
        }
        impl GraphSource for Selected {
            type Key = u64;
            fn affects_path(&mut self, id: m::CommitId) -> Result<Option<bool>, Self::Error> {
                Ok(self.flags.get(&id).copied())
            }
            fn node(&mut self, id: m::CommitId) -> Result<SourceNode<u64>, Self::Error> {
                self.source.node(id)
            }
            fn common_ancestors(
                &mut self,
                a: &[m::CommitId],
                b: &[m::CommitId],
            ) -> Result<Vec<m::CommitId>, Self::Error> {
                self.source.common_ancestors(a, b)
            }
            fn is_ancestor(&mut self, a: m::CommitId, b: m::CommitId) -> Result<bool, Self::Error> {
                self.source.is_ancestor(a, b)
            }
        }
        impl ContentSource for Selected {
            fn read_file(&mut self, value: &m::TreeValue) -> Result<Vec<u8>, Self::Error> {
                self.source.read_file(value)
            }
        }
        let target = b"left\nright\n";
        let target_id = m::FileId(*blake3::hash(target).as_bytes());
        let mut direct_origins = vec![None; 2];
        annotate(
            &mut fixture.clone(),
            id(4),
            target,
            target_id,
            limits(4),
            &mut diff_limits(),
            &mut 0,
            &mut direct_origins,
        )
        .unwrap();
        let mut selected = Selected {
            source: fixture,
            flags: actual
                .into_iter()
                .map(|node| (node.id, node.affects_path))
                .collect(),
        };
        let mut paged_origins = vec![None; 2];
        annotate(
            &mut selected,
            id(4),
            target,
            target_id,
            limits(4),
            &mut diff_limits(),
            &mut 0,
            &mut paged_origins,
        )
        .unwrap();
        assert_eq!(paged_origins, direct_origins);
        assert_eq!(
            paged_origins,
            vec![
                Some(Origin {
                    commit: id(2),
                    line: 0
                }),
                Some(Origin {
                    commit: id(3),
                    line: 1
                })
            ]
        );
        let mut server_source = selected.source.clone();
        let mut ready =
            Frontier::new(&mut server_source, &[id(4)], limits(4), diff_limits()).unwrap();
        while ready.progress().phase != FrontierPhase::Ready {
            ready
                .advance(&mut server_source, NonZeroUsize::new(1).unwrap())
                .unwrap();
        }
        let before = server_source.reads.clone();
        let mut server_origins = vec![None; 2];
        ready
            .annotate(
                &mut server_source,
                id(4),
                target,
                target_id,
                &mut server_origins,
            )
            .unwrap();
        assert_eq!(server_origins, direct_origins);
        assert_eq!(
            server_source.reads, before,
            "server merge attribution reuses retained metadata"
        );
    }
}
