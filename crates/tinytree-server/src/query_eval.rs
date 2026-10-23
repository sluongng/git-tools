//! Incremental, IDs-first graph execution. Compilation performs no graph I/O.
//! A quantum bounds operator transitions; one scoped literal lookup, bounded
//! parent batch or interval union may perform more than one metadata probe.
use crate::query_overlay::{GraphKey, GraphNode, OverlayGraph};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};
use tinytree_index::{Error as GraphError, WorkBudget};
use tinytree_model as m;
use tinytree_proto::query_v1 as q;
type Result<T> = std::result::Result<T, EvalError>;
#[derive(Debug)]
pub(crate) enum EvalError {
    Graph(GraphError),
    Invalid(&'static str),
    Unsupported(&'static str),
}
impl From<GraphError> for EvalError {
    fn from(error: GraphError) -> Self {
        Self::Graph(error)
    }
}
impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Graph(error) => error.fmt(f),
            Self::Invalid(message) => write!(f, "invalid graph expression: {message}"),
            Self::Unsupported(message) => write!(f, "unsupported graph expression: {message}"),
        }
    }
}
impl std::error::Error for EvalError {}
fn limit() -> EvalError {
    GraphError::BudgetExceeded.into()
}
#[derive(Clone, Debug)]
pub(crate) struct EvalLimits {
    pub max_ast_nodes: usize,
    pub max_ast_depth: usize,
    pub max_literals: usize,
    pub max_frontier: usize,
    pub max_intervals: usize,
    pub max_latest: usize,
    pub max_step_work: usize,
    pub max_steps: u64,
}
impl Default for EvalLimits {
    fn default() -> Self {
        Self {
            max_ast_nodes: 64,
            max_ast_depth: 16,
            max_literals: 1024,
            max_frontier: 4096,
            max_intervals: 8192,
            max_latest: 1024,
            max_step_work: 128,
            max_steps: 1_000_000,
        }
    }
}
impl EvalLimits {
    pub fn memory_bytes(&self) -> Option<usize> {
        self.max_frontier
            .checked_mul(512)?
            .checked_add(self.max_intervals.checked_mul(128)?)?
            .checked_add(self.max_literals.checked_mul(256)?)?
            .checked_add(self.max_ast_nodes.checked_mul(1024)?)?
            .checked_add(2 * 1024 * 1024)
    }
    fn validate(&self) -> Result<()> {
        if self.max_ast_nodes == 0
            || self.max_ast_depth == 0
            || self.max_frontier == 0
            || self.max_intervals == 0
            || self.max_step_work == 0
            || self.max_steps == 0
            || self.memory_bytes().is_none()
        {
            return Err(EvalError::Invalid("invalid evaluator limits"));
        }
        Ok(())
    }
}
pub(crate) enum EvalStep {
    Row(GraphNode),
    Progress,
    Done,
}
#[derive(Default, Clone, Copy)]
struct Counts {
    slots: usize,
    intervals: usize,
}
impl Counts {
    fn add(self, other: Self) -> Self {
        Self {
            slots: self.slots.saturating_add(other.slots),
            intervals: self.intervals.saturating_add(other.intervals),
        }
    }
}
struct Memory {
    used: Counts,
    max_slots: usize,
    max_intervals: usize,
}
impl Memory {
    fn slots(&mut self, count: usize) -> Result<()> {
        let next = self.used.slots.checked_add(count).ok_or_else(limit)?;
        if next > self.max_slots {
            return Err(limit());
        }
        self.used.slots = next;
        Ok(())
    }
    fn intervals(&mut self, count: usize) -> Result<()> {
        let next = self.used.intervals.checked_add(count).ok_or_else(limit)?;
        if next > self.max_intervals {
            return Err(limit());
        }
        self.used.intervals = next;
        Ok(())
    }
    fn release(&mut self, count: Counts) {
        self.used.slots -= count.slots;
        self.used.intervals -= count.intervals;
    }
}
struct Context<'a> {
    graph: &'a Arc<OverlayGraph>,
    budget: &'a mut WorkBudget,
    memory: &'a mut Memory,
    remaining: usize,
    steps: &'a mut u64,
    max_steps: u64,
}
impl Context<'_> {
    fn cpu(&mut self) -> Result<()> {
        *self.steps = self.steps.checked_add(1).ok_or_else(limit)?;
        if *self.steps > self.max_steps {
            return Err(limit());
        }
        Ok(())
    }
    fn tick(&mut self) -> Result<bool> {
        if self.remaining == 0 {
            return Ok(false);
        }
        self.cpu()?;
        self.remaining -= 1;
        Ok(true)
    }
    fn is_ancestor(&mut self, ancestor: &GraphNode, descendant: &GraphNode) -> Result<bool> {
        // Indexed ancestry is one bounded graph primitive, not an uncharged
        // private traversal outside the aggregate retained/temporary frontier.
        let available = self
            .memory
            .max_slots
            .checked_sub(self.memory.used.slots)
            .ok_or_else(limit)?;
        let original = self.budget.max_frontier;
        self.budget.max_frontier = original.min(available);
        let result = self
            .graph
            .is_ancestor(ancestor.commit(), descendant.commit(), self.budget);
        self.budget.max_frontier = original;
        Ok(result?)
    }
    fn parents(&mut self, node: &GraphNode) -> Result<Vec<GraphNode>> {
        let available = self
            .memory
            .max_slots
            .checked_sub(self.memory.used.slots)
            .ok_or_else(limit)?;
        let original = self.budget.max_frontier;
        self.budget.max_frontier = original.min(available);
        let result = self.graph.parent_nodes(node, self.budget);
        self.budget.max_frontier = original;
        let values = result?;
        self.memory.slots(values.capacity())?;
        Ok(values)
    }
}
pub(crate) struct Evaluator {
    graph: Arc<OverlayGraph>,
    root: Operator,
    limits: EvalLimits,
    memory: Memory,
    steps: u64,
    failed: bool,
}
impl Evaluator {
    pub fn new(
        graph: Arc<OverlayGraph>,
        expression: q::GraphExpr,
        limits: EvalLimits,
    ) -> Result<Self> {
        limits.validate()?;
        let mut compiler = Compiler {
            limits: &limits,
            nodes: 0,
            literals: 0,
        };
        let root = compiler.compile(&expression, 1)?;
        Ok(Self {
            graph,
            root,
            memory: Memory {
                used: Counts::default(),
                max_slots: limits.max_frontier,
                max_intervals: limits.max_intervals,
            },
            limits,
            steps: 0,
            failed: false,
        })
    }
    pub fn graph(&self) -> &Arc<OverlayGraph> {
        &self.graph
    }
    pub fn steps(&self) -> u64 {
        self.steps
    }
    pub fn step(&mut self, budget: &mut WorkBudget) -> Result<EvalStep> {
        if self.failed {
            return Err(GraphError::Unavailable.into());
        }
        let mut context = Context {
            graph: &self.graph,
            budget,
            memory: &mut self.memory,
            remaining: self.limits.max_step_work,
            steps: &mut self.steps,
            max_steps: self.limits.max_steps,
        };
        let result = self.root.poll(&mut context);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}
#[derive(Clone, Copy)]
struct Window {
    start: u64,
    end: Option<u64>,
}
impl Window {
    fn all() -> Self {
        Self {
            start: 0,
            end: None,
        }
    }
    fn empty(self) -> bool {
        self.end == Some(self.start)
    }
    fn contains(self, index: u64) -> bool {
        index >= self.start && self.end.is_none_or(|end| index < end)
    }
}
#[derive(Clone, Copy)]
struct Distance {
    start: u64,
    end: u64,
}
struct Distances {
    values: Vec<Distance>,
}
impl Distances {
    fn count(&self) -> Counts {
        Counts {
            slots: 0,
            intervals: self.values.capacity(),
        }
    }
    fn seed(memory: &mut Memory) -> Result<Self> {
        memory.intervals(1)?;
        let mut values = Vec::with_capacity(1);
        values.push(Distance { start: 0, end: 1 });
        Ok(Self { values })
    }
    fn copy(&self, memory: &mut Memory) -> Result<Self> {
        memory.intervals(self.values.len())?;
        let mut values = Vec::with_capacity(self.values.len());
        values.extend_from_slice(&self.values);
        Ok(Self { values })
    }
    fn selected(&self, window: Window) -> bool {
        self.values
            .iter()
            .any(|value| value.end > window.start && window.end.is_none_or(|end| value.start < end))
    }
    fn shift(&mut self, end: Option<u64>) -> Result<()> {
        for value in &mut self.values {
            value.start = value.start.checked_add(1).ok_or_else(limit)?;
            value.end = value.end.checked_add(1).ok_or_else(limit)?;
            if let Some(end) = end {
                value.end = value.end.min(end);
            }
        }
        self.values.retain(|value| value.start < value.end);
        Ok(())
    }
    fn union(self, other: Self, memory: &mut Memory) -> Result<Self> {
        let capacity = self
            .values
            .len()
            .checked_add(other.values.len())
            .ok_or_else(limit)?;
        memory.intervals(capacity)?;
        let mut values: Vec<Distance> = Vec::with_capacity(capacity);
        let mut left = self.values.iter().peekable();
        let mut right = other.values.iter().peekable();
        while left.peek().is_some() || right.peek().is_some() {
            let next = if right.peek().is_none()
                || left
                    .peek()
                    .is_some_and(|value| value.start <= right.peek().unwrap().start)
            {
                *left.next().unwrap()
            } else {
                *right.next().unwrap()
            };
            if let Some(last) = values.last_mut().filter(|last| last.end >= next.start) {
                last.end = last.end.max(next.end);
            } else {
                values.push(next);
            }
        }
        memory.release(self.count().add(other.count()));
        Ok(Self { values })
    }
}
struct Visit {
    node: GraphNode,
    distances: Distances,
}
struct Expansion {
    parents: Vec<GraphNode>,
    batch_slots: usize,
    distances: Distances,
    key: GraphKey,
}
struct Exclusion {
    input: Box<Operator>,
    look: Option<GraphNode>,
    done: bool,
}
struct Ancestors {
    heads: Box<Operator>,
    look: Option<GraphNode>,
    done: bool,
    queue: BTreeMap<GraphKey, Visit>,
    expansion: Option<Expansion>,
    pending: Option<Visit>,
    exclusion: Option<Exclusion>,
    generations: Window,
    parents: Window,
}
impl Ancestors {
    fn push(&mut self, node: GraphNode, distances: Distances, memory: &mut Memory) -> Result<()> {
        if let Some(previous) = self.queue.remove(&node.key()) {
            if previous.node.commit() != node.commit() {
                return Err(GraphError::Corrupt.into());
            }
            let distances = previous.distances.union(distances, memory)?;
            memory.release(Counts {
                slots: 1,
                intervals: 0,
            });
            self.queue.insert(
                node.key(),
                Visit {
                    node: previous.node,
                    distances,
                },
            );
        } else {
            self.queue.insert(node.key(), Visit { node, distances });
        }
        Ok(())
    }
    fn expand_visit(&mut self, visit: Visit, ctx: &mut Context<'_>) -> Result<Option<GraphNode>> {
        let selected = visit.distances.selected(self.generations);
        let mut distances = visit.distances;
        distances.shift(self.generations.end)?;
        if distances.values.is_empty() || self.parents.empty() {
            ctx.memory.release(distances.count());
        } else {
            let parents = ctx.parents(&visit.node)?;
            let batch_slots = parents.capacity();
            self.expansion = Some(Expansion {
                parents,
                batch_slots,
                distances,
                key: visit.node.key(),
            });
        }
        ctx.memory.release(Counts {
            slots: 1,
            intervals: 0,
        });
        Ok(selected.then_some(visit.node))
    }
    fn poll(&mut self, ctx: &mut Context<'_>) -> Result<EvalStep> {
        loop {
            if let Some(expansion) = &mut self.expansion {
                if !ctx.tick()? {
                    return Ok(EvalStep::Progress);
                }
                let index = expansion.parents.len().saturating_sub(1);
                if let Some(parent) = expansion.parents.pop() {
                    if self.parents.contains(index as u64) {
                        if parent.key() >= expansion.key {
                            return Err(GraphError::Corrupt.into());
                        }
                        let distances = expansion.distances.copy(ctx.memory)?;
                        ctx.memory.slots(1)?;
                        self.push(parent, distances, ctx.memory)?;
                    }
                    continue;
                }
                let expansion = self.expansion.take().unwrap();
                ctx.memory.release(Counts {
                    slots: expansion.batch_slots,
                    intervals: expansion.distances.values.capacity(),
                });
                continue;
            }
            if let Some(visit) = self.pending.as_ref() {
                let key = visit.node.key();
                let id = visit.node.commit();
                let exclusion = self.exclusion.as_mut().ok_or(GraphError::Corrupt)?;
                if !fill(
                    &mut exclusion.input,
                    &mut exclusion.look,
                    &mut exclusion.done,
                    ctx,
                )? {
                    return Ok(EvalStep::Progress);
                }
                if !ctx.tick()? {
                    return Ok(EvalStep::Progress);
                }
                match exclusion.look.as_ref().map(|node| node.key().cmp(&key)) {
                    Some(std::cmp::Ordering::Greater) => {
                        exclusion.look.take();
                        ctx.memory.release(Counts {
                            slots: 1,
                            intervals: 0,
                        });
                        continue;
                    }
                    Some(std::cmp::Ordering::Equal) => {
                        if exclusion.look.as_ref().unwrap().commit() != id {
                            return Err(GraphError::Corrupt.into());
                        }
                        // Excluded ancestry is downward closed. Discard every
                        // distance interval before fetching wanted parents.
                        let visit = self.pending.take().unwrap();
                        ctx.memory.release(Counts {
                            slots: 1,
                            intervals: visit.distances.values.capacity(),
                        });
                        continue;
                    }
                    _ => {}
                }
                let visit = self.pending.take().unwrap();
                if let Some(node) = self.expand_visit(visit, ctx)? {
                    return Ok(EvalStep::Row(node));
                }
                continue;
            }
            if self.look.is_none() && !self.done {
                match self.heads.poll(ctx)? {
                    EvalStep::Row(node) => {
                        ctx.memory.slots(1)?;
                        self.look = Some(node);
                    }
                    EvalStep::Progress => return Ok(EvalStep::Progress),
                    EvalStep::Done => self.done = true,
                }
                continue;
            }
            if !ctx.tick()? {
                return Ok(EvalStep::Progress);
            }
            if self.look.as_ref().is_some_and(|node| {
                self.queue
                    .last_key_value()
                    .is_none_or(|(key, _)| node.key() >= *key)
            }) {
                let node = self.look.take().unwrap();
                let distances = Distances::seed(ctx.memory)?;
                self.push(node, distances, ctx.memory)?;
                continue;
            }
            let Some((_, visit)) = self.queue.pop_last() else {
                return Ok(EvalStep::Done);
            };
            if self.exclusion.is_some() {
                self.pending = Some(visit);
                continue;
            }
            if let Some(node) = self.expand_visit(visit, ctx)? {
                return Ok(EvalStep::Row(node));
            }
        }
    }
}
struct HeadExpansion {
    parents: Vec<GraphNode>,
    batch_slots: usize,
    key: GraphKey,
}
struct Heads {
    input: Box<Operator>,
    look: Option<GraphNode>,
    done: bool,
    excluded: BTreeMap<GraphKey, GraphNode>,
    expansion: Option<HeadExpansion>,
}
impl Heads {
    fn push_excluded(&mut self, node: GraphNode, memory: &mut Memory) -> Result<()> {
        if let Some(existing) = self.excluded.get(&node.key()) {
            if existing.commit() != node.commit() {
                return Err(GraphError::Corrupt.into());
            }
        } else {
            memory.slots(1)?;
            self.excluded.insert(node.key(), node);
        }
        Ok(())
    }
    fn expand(&mut self, node: &GraphNode, ctx: &mut Context<'_>) -> Result<()> {
        let parents = ctx.parents(node)?;
        let batch_slots = parents.capacity();
        self.expansion = Some(HeadExpansion {
            parents,
            batch_slots,
            key: node.key(),
        });
        Ok(())
    }
    fn poll(&mut self, ctx: &mut Context<'_>) -> Result<EvalStep> {
        loop {
            if let Some(expansion) = &mut self.expansion {
                if !ctx.tick()? {
                    return Ok(EvalStep::Progress);
                }
                if let Some(parent) = expansion.parents.pop() {
                    if parent.key() >= expansion.key {
                        return Err(GraphError::Corrupt.into());
                    }
                    self.push_excluded(parent, ctx.memory)?;
                    continue;
                }
                let expansion = self.expansion.take().unwrap();
                ctx.memory.release(Counts {
                    slots: expansion.batch_slots,
                    intervals: 0,
                });
                continue;
            }
            if !fill(&mut self.input, &mut self.look, &mut self.done, ctx)? {
                return Ok(EvalStep::Progress);
            }
            // Accepted heads only exclude ancestors of later candidates. No
            // candidate remains, so never drain an unrelated public tail.
            if self.done && self.look.is_none() {
                return Ok(EvalStep::Done);
            }
            if !ctx.tick()? {
                return Ok(EvalStep::Progress);
            }
            let candidate = self.look.as_ref().ok_or(GraphError::Corrupt)?;
            match self
                .excluded
                .last_key_value()
                .map(|(key, _)| key.cmp(&candidate.key()))
            {
                Some(std::cmp::Ordering::Greater) => {
                    let (_, node) = self.excluded.pop_last().unwrap();
                    self.expand(&node, ctx)?;
                    ctx.memory.release(Counts {
                        slots: 1,
                        intervals: 0,
                    });
                }
                Some(std::cmp::Ordering::Equal) => {
                    if self.excluded.last_key_value().unwrap().1.commit() != candidate.commit() {
                        return Err(GraphError::Corrupt.into());
                    }
                    self.look.take();
                    ctx.memory.release(Counts {
                        slots: 1,
                        intervals: 0,
                    });
                }
                _ => {
                    let node = self.look.take().unwrap();
                    self.expand(&node, ctx)?;
                    ctx.memory.release(Counts {
                        slots: 1,
                        intervals: 0,
                    });
                    return Ok(EvalStep::Row(node));
                }
            }
        }
    }
}
// The flag is part of a scope-bound, canonically derived node summary. It
// needs no canonical payload or tree/file reads during predicate evaluation.
struct HasConflict {
    input: Box<Operator>,
    look: Option<GraphNode>,
    done: bool,
}
impl HasConflict {
    fn poll(&mut self, ctx: &mut Context<'_>) -> Result<EvalStep> {
        loop {
            if !fill(&mut self.input, &mut self.look, &mut self.done, ctx)? {
                return Ok(EvalStep::Progress);
            }
            if self.done && self.look.is_none() {
                return Ok(EvalStep::Done);
            }
            if !ctx.tick()? {
                return Ok(EvalStep::Progress);
            }
            let node = self.look.take().ok_or(GraphError::Corrupt)?;
            ctx.memory.release(Counts {
                slots: 1,
                intervals: 0,
            });
            if node.has_conflict() {
                return Ok(EvalStep::Row(node));
            }
        }
    }
}
struct Roots {
    input: Box<Operator>,
    look: Option<GraphNode>,
    done: bool,
    scanning: bool,
    scan: Option<GraphKey>,
    candidates: BTreeMap<GraphKey, GraphNode>,
}
impl Roots {
    fn poll(&mut self, ctx: &mut Context<'_>) -> Result<EvalStep> {
        loop {
            if self.scanning {
                if !ctx.tick()? {
                    return Ok(EvalStep::Progress);
                }
                if let Some(key) = self.scan {
                    let candidate = self.candidates.get(&key).ok_or(GraphError::Corrupt)?;
                    let node = self.look.as_ref().ok_or(GraphError::Corrupt)?;
                    let remove = ctx.is_ancestor(node, candidate)?;
                    self.scan = self
                        .candidates
                        .range(..key)
                        .next_back()
                        .map(|(key, _)| *key);
                    if remove {
                        self.candidates.remove(&key);
                        ctx.memory.release(Counts {
                            slots: 1,
                            intervals: 0,
                        });
                    }
                } else {
                    let node = self.look.take().ok_or(GraphError::Corrupt)?;
                    if self.candidates.insert(node.key(), node).is_some() {
                        return Err(GraphError::Corrupt.into());
                    }
                    // Transfer the charged lookahead slot into the antichain.
                    self.scanning = false;
                }
                continue;
            }
            if !fill(&mut self.input, &mut self.look, &mut self.done, ctx)? {
                return Ok(EvalStep::Progress);
            }
            if self.look.is_some() {
                self.scan = self.candidates.last_key_value().map(|(key, _)| *key);
                self.scanning = true;
                continue;
            }
            if self.done {
                if !ctx.tick()? {
                    return Ok(EvalStep::Progress);
                }
                if let Some((_, node)) = self.candidates.pop_last() {
                    ctx.memory.release(Counts {
                        slots: 1,
                        intervals: 0,
                    });
                    return Ok(EvalStep::Row(node));
                }
                return Ok(EvalStep::Done);
            }
        }
    }
}
struct DagRange {
    roots_input: Box<Operator>,
    roots_done: bool,
    roots: BTreeMap<GraphKey, GraphNode>,
    heads: Box<Operator>,
    heads_done: bool,
    look: Option<GraphNode>,
    checking: bool,
    scan: Option<GraphKey>,
    probe: Option<Box<Operator>>,
    generations: Window,
}
impl DagRange {
    fn clear_probe(&mut self, memory: &mut Memory) {
        if let Some(probe) = self.probe.take() {
            memory.release(probe.counts());
        }
    }
    fn take_candidate(&mut self, memory: &mut Memory) -> Result<GraphNode> {
        self.checking = false;
        self.scan = None;
        let node = self.look.take().ok_or(GraphError::Corrupt)?;
        memory.release(Counts {
            slots: 1,
            intervals: 0,
        });
        Ok(node)
    }
    fn poll(&mut self, ctx: &mut Context<'_>) -> Result<EvalStep> {
        loop {
            if !self.roots_done {
                match self.roots_input.poll(ctx)? {
                    EvalStep::Progress => return Ok(EvalStep::Progress),
                    EvalStep::Done => self.roots_done = true,
                    EvalStep::Row(node) => {
                        ctx.cpu()?;
                        ctx.memory.slots(1)?;
                        if self.roots.insert(node.key(), node).is_some() {
                            return Err(GraphError::Corrupt.into());
                        }
                    }
                }
                continue;
            }
            // Do not evaluate heads or traverse their ancestry when no root can
            // reach them. Compilation still validates both expression shapes.
            if self.roots.is_empty() {
                return Ok(EvalStep::Done);
            }
            if let Some(probe) = &mut self.probe {
                let selected = match probe.poll(ctx)? {
                    EvalStep::Progress => return Ok(EvalStep::Progress),
                    EvalStep::Done => Some(false),
                    EvalStep::Row(node) => {
                        ctx.cpu()?;
                        if let Some(root) = self.roots.get(&node.key()) {
                            if root.commit() != node.commit() {
                                return Err(GraphError::Corrupt.into());
                            }
                            Some(true)
                        } else if node.key() < *self.roots.first_key_value().unwrap().0 {
                            Some(false)
                        } else {
                            None
                        }
                    }
                };
                if let Some(selected) = selected {
                    self.clear_probe(ctx.memory);
                    let node = self.take_candidate(ctx.memory)?;
                    if selected {
                        return Ok(EvalStep::Row(node));
                    }
                }
                continue;
            }
            if !fill(&mut self.heads, &mut self.look, &mut self.heads_done, ctx)? {
                return Ok(EvalStep::Progress);
            }
            if self.heads_done && self.look.is_none() {
                return Ok(EvalStep::Done);
            }
            if self
                .look
                .as_ref()
                .is_some_and(|node| node.key() < *self.roots.first_key_value().unwrap().0)
            {
                return Ok(EvalStep::Done);
            }
            if self.generations.start == 0 && self.generations.end == None {
                if !self.checking {
                    self.scan = self.roots.first_key_value().map(|(key, _)| *key);
                    self.checking = true;
                }
                if !ctx.tick()? {
                    return Ok(EvalStep::Progress);
                }
                if let Some(key) = self.scan {
                    let root = self.roots.get(&key).ok_or(GraphError::Corrupt)?;
                    let candidate = self.look.as_ref().ok_or(GraphError::Corrupt)?;
                    let selected = ctx.is_ancestor(root, candidate)?;
                    self.scan = self
                        .roots
                        .range((std::ops::Bound::Excluded(key), std::ops::Bound::Unbounded))
                        .next()
                        .map(|(key, _)| *key);
                    if selected {
                        return Ok(EvalStep::Row(self.take_candidate(ctx.memory)?));
                    }
                } else {
                    self.take_candidate(ctx.memory)?;
                }
                continue;
            }
            if self.generations.start == 0 && self.generations.end == Some(1) {
                if !ctx.tick()? {
                    return Ok(EvalStep::Progress);
                }
                let candidate = self.look.as_ref().ok_or(GraphError::Corrupt)?;
                let selected = self
                    .roots
                    .get(&candidate.key())
                    .map(|node| node.commit() == candidate.commit())
                    .unwrap_or(false);
                let node = self.take_candidate(ctx.memory)?;
                if selected {
                    return Ok(EvalStep::Row(node));
                }
                continue;
            }
            // Reverse reachability uses the SAME path-length intervals as the
            // ancestor walker: a root is accepted if ANY path to this candidate
            // has a generation in the requested half-open range. Each probe is
            // resumable and shares the query's cumulative work/memory budget.
            if !ctx.tick()? {
                return Ok(EvalStep::Progress);
            }
            let node = self.look.as_ref().ok_or(GraphError::Corrupt)?.clone();
            ctx.memory.slots(1)?;
            let mut seed = BTreeMap::new();
            seed.insert(node.key(), node);
            self.probe = Some(Box::new(Operator::Ancestors(Ancestors {
                heads: Box::new(Operator::Commits(Commits {
                    ids: Vec::new(),
                    next: 0,
                    rows: seed,
                })),
                look: None,
                done: false,
                queue: BTreeMap::new(),
                expansion: None,
                pending: None,
                exclusion: None,
                generations: self.generations,
                parents: Window::all(),
            })));
        }
    }
}
#[derive(Clone, Copy)]
enum SetKind {
    Union,
    Intersection,
    Difference,
}
struct SetOperator {
    left: Box<Operator>,
    right: Box<Operator>,
    a: Option<GraphNode>,
    b: Option<GraphNode>,
    left_done: bool,
    right_done: bool,
    kind: SetKind,
}
fn fill(
    child: &mut Operator,
    look: &mut Option<GraphNode>,
    done: &mut bool,
    ctx: &mut Context<'_>,
) -> Result<bool> {
    if look.is_none() && !*done {
        match child.poll(ctx)? {
            EvalStep::Row(node) => {
                ctx.memory.slots(1)?;
                *look = Some(node);
            }
            EvalStep::Done => *done = true,
            EvalStep::Progress => return Ok(false),
        }
    }
    Ok(true)
}
impl SetOperator {
    fn poll(&mut self, ctx: &mut Context<'_>) -> Result<EvalStep> {
        loop {
            if !fill(&mut self.left, &mut self.a, &mut self.left_done, ctx)? {
                return Ok(EvalStep::Progress);
            }
            if matches!(self.kind, SetKind::Intersection | SetKind::Difference)
                && self.left_done
                && self.a.is_none()
            {
                return Ok(EvalStep::Done);
            }
            if !fill(&mut self.right, &mut self.b, &mut self.right_done, ctx)? {
                return Ok(EvalStep::Progress);
            }
            if matches!(self.kind, SetKind::Intersection) && self.right_done && self.b.is_none() {
                return Ok(EvalStep::Done);
            }
            if !ctx.tick()? {
                return Ok(EvalStep::Progress);
            }
            let ordering = match (&self.a, &self.b) {
                (None, None) => return Ok(EvalStep::Done),
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(a), Some(b)) => {
                    if a.key() == b.key() && a.commit() != b.commit() {
                        return Err(GraphError::Corrupt.into());
                    }
                    a.key().cmp(&b.key())
                }
            };
            use std::cmp::Ordering::*;
            let mut emit = None;
            match (self.kind, ordering) {
                (SetKind::Union, Greater) => emit = self.a.take(),
                (SetKind::Union, Less) => emit = self.b.take(),
                (SetKind::Union, Equal) => {
                    self.b.take();
                    ctx.memory.release(Counts {
                        slots: 1,
                        intervals: 0,
                    });
                    emit = self.a.take();
                }
                (SetKind::Intersection, Greater) => {
                    self.a.take();
                    ctx.memory.release(Counts {
                        slots: 1,
                        intervals: 0,
                    });
                }
                (SetKind::Intersection, Less) => {
                    self.b.take();
                    ctx.memory.release(Counts {
                        slots: 1,
                        intervals: 0,
                    });
                }
                (SetKind::Intersection, Equal) => {
                    self.b.take();
                    ctx.memory.release(Counts {
                        slots: 1,
                        intervals: 0,
                    });
                    emit = self.a.take();
                }
                (SetKind::Difference, Greater) => emit = self.a.take(),
                (SetKind::Difference, Less) => {
                    self.b.take();
                    ctx.memory.release(Counts {
                        slots: 1,
                        intervals: 0,
                    });
                }
                (SetKind::Difference, Equal) => {
                    self.a.take();
                    self.b.take();
                    ctx.memory.release(Counts {
                        slots: 2,
                        intervals: 0,
                    });
                }
            }
            if let Some(node) = emit {
                ctx.memory.release(Counts {
                    slots: 1,
                    intervals: 0,
                });
                return Ok(EvalStep::Row(node));
            }
        }
    }
}
struct Commits {
    ids: Vec<m::CommitId>,
    next: usize,
    rows: BTreeMap<GraphKey, GraphNode>,
}
impl Commits {
    fn poll(&mut self, ctx: &mut Context<'_>) -> Result<EvalStep> {
        while ctx.tick()? {
            if let Some(id) = self.ids.get(self.next) {
                ctx.memory.slots(1)?;
                let node = ctx
                    .graph
                    .lookup_node(*id, ctx.budget)?
                    .ok_or(EvalError::Invalid("literal outside query scope"))?;
                if self.rows.insert(node.key(), node).is_some() {
                    return Err(GraphError::Corrupt.into());
                }
                self.next += 1;
                continue;
            }
            if let Some((_, node)) = self.rows.pop_last() {
                ctx.memory.release(Counts {
                    slots: 1,
                    intervals: 0,
                });
                return Ok(EvalStep::Row(node));
            }
            return Ok(EvalStep::Done);
        }
        Ok(EvalStep::Progress)
    }
}
struct Latest {
    input: Box<Operator>,
    count: usize,
    done: bool,
    selection: BTreeMap<(i64, GraphKey), GraphNode>,
    output: BTreeMap<GraphKey, GraphNode>,
}
impl Latest {
    fn poll(&mut self, ctx: &mut Context<'_>) -> Result<EvalStep> {
        loop {
            if !self.done {
                match self.input.poll(ctx)? {
                    EvalStep::Progress => return Ok(EvalStep::Progress),
                    EvalStep::Done => self.done = true,
                    EvalStep::Row(node) => {
                        ctx.cpu()?;
                        let key = (node.committer_millis(), node.key());
                        if self.selection.len() < self.count {
                            ctx.memory.slots(1)?;
                            self.selection.insert(key, node);
                        } else if self
                            .selection
                            .first_key_value()
                            .is_some_and(|(smallest, _)| key > *smallest)
                        {
                            self.selection.pop_first();
                            self.selection.insert(key, node);
                        }
                        continue;
                    }
                }
                continue;
            }
            if !ctx.tick()? {
                return Ok(EvalStep::Progress);
            }
            if let Some((_, node)) = self.selection.pop_first() {
                self.output.insert(node.key(), node);
                continue;
            }
            if let Some((_, node)) = self.output.pop_last() {
                ctx.memory.release(Counts {
                    slots: 1,
                    intervals: 0,
                });
                return Ok(EvalStep::Row(node));
            }
            return Ok(EvalStep::Done);
        }
    }
}
enum Operator {
    Empty,
    Commits(Commits),
    Ancestors(Ancestors),
    Heads(Heads),
    HasConflict(HasConflict),
    Roots(Roots),
    DagRange(DagRange),
    Set(SetOperator),
    Latest(Latest),
}
impl Operator {
    fn counts(&self) -> Counts {
        match self {
            Self::Empty => Counts::default(),
            Self::Commits(state) => Counts {
                slots: state.rows.len(),
                intervals: 0,
            },
            Self::Ancestors(state) => {
                let mut count = state.heads.counts().add(Counts {
                    slots: usize::from(state.look.is_some()) + state.queue.len(),
                    intervals: state
                        .queue
                        .values()
                        .map(|visit| visit.distances.values.capacity())
                        .sum(),
                });
                if let Some(expansion) = &state.expansion {
                    count = count.add(Counts {
                        slots: expansion.batch_slots,
                        intervals: expansion.distances.values.capacity(),
                    });
                }
                if let Some(visit) = &state.pending {
                    count = count.add(Counts {
                        slots: 1,
                        intervals: visit.distances.values.capacity(),
                    });
                }
                if let Some(exclusion) = &state.exclusion {
                    count = count.add(exclusion.input.counts()).add(Counts {
                        slots: usize::from(exclusion.look.is_some()),
                        intervals: 0,
                    });
                }
                count
            }
            Self::Heads(state) => state.input.counts().add(Counts {
                slots: usize::from(state.look.is_some())
                    + state.excluded.len()
                    + state
                        .expansion
                        .as_ref()
                        .map_or(0, |expansion| expansion.batch_slots),
                intervals: 0,
            }),
            Self::HasConflict(state) => state.input.counts().add(Counts {
                slots: usize::from(state.look.is_some()),
                intervals: 0,
            }),
            Self::Roots(state) => state.input.counts().add(Counts {
                slots: state.candidates.len() + usize::from(state.look.is_some()),
                intervals: 0,
            }),
            Self::DagRange(state) => state
                .roots_input
                .counts()
                .add(state.heads.counts())
                .add(Counts {
                    slots: state.roots.len() + usize::from(state.look.is_some()),
                    intervals: 0,
                })
                .add(
                    state
                        .probe
                        .as_ref()
                        .map_or(Counts::default(), |probe| probe.counts()),
                ),
            Self::Set(state) => state.left.counts().add(state.right.counts()).add(Counts {
                slots: usize::from(state.a.is_some()) + usize::from(state.b.is_some()),
                intervals: 0,
            }),
            Self::Latest(state) => state.input.counts().add(Counts {
                slots: state.selection.len() + state.output.len(),
                intervals: 0,
            }),
        }
    }
    fn poll(&mut self, ctx: &mut Context<'_>) -> Result<EvalStep> {
        ctx.cpu()?;
        let result = match self {
            Self::Empty => {
                if ctx.tick()? {
                    Ok(EvalStep::Done)
                } else {
                    Ok(EvalStep::Progress)
                }
            }
            Self::Commits(state) => state.poll(ctx),
            Self::Ancestors(state) => state.poll(ctx),
            Self::Heads(state) => state.poll(ctx),
            Self::HasConflict(state) => state.poll(ctx),
            Self::Roots(state) => state.poll(ctx),
            Self::DagRange(state) => state.poll(ctx),
            Self::Set(state) => state.poll(ctx),
            Self::Latest(state) => state.poll(ctx),
        }?;
        if matches!(result, EvalStep::Done) {
            ctx.memory.release(self.counts());
            *self = Self::Empty;
        }
        Ok(result)
    }
}
struct Compiler<'a> {
    limits: &'a EvalLimits,
    nodes: usize,
    literals: usize,
}
fn child<T: Borrow<q::GraphExpr>>(value: &Option<T>) -> Result<&q::GraphExpr> {
    value
        .as_ref()
        .map(Borrow::borrow)
        .ok_or(EvalError::Invalid("missing child"))
}
impl Compiler<'_> {
    fn bump(&mut self) -> Result<()> {
        self.nodes = self.nodes.checked_add(1).ok_or_else(limit)?;
        if self.nodes > self.limits.max_ast_nodes {
            return Err(limit());
        }
        Ok(())
    }
    fn window(start: u64, end: Option<u64>) -> Result<Window> {
        if end.is_some_and(|end| start > end) {
            return Err(EvalError::Invalid("reversed interval"));
        }
        Ok(Window { start, end })
    }
    fn ancestors(&mut self, heads: Operator, generations: Window, parents: Window) -> Operator {
        if generations.empty() {
            Operator::Empty
        } else {
            Operator::Ancestors(Ancestors {
                heads: Box::new(heads),
                look: None,
                done: false,
                queue: BTreeMap::new(),
                expansion: None,
                pending: None,
                exclusion: None,
                generations,
                parents,
            })
        }
    }
    fn set(&mut self, left: Operator, right: Operator, kind: SetKind) -> Operator {
        Operator::Set(SetOperator {
            left: Box::new(left),
            right: Box::new(right),
            a: None,
            b: None,
            left_done: false,
            right_done: false,
            kind,
        })
    }
    fn balanced(&mut self, mut values: Vec<Operator>, kind: SetKind) -> Result<Operator> {
        if values.is_empty() {
            return Ok(Operator::Empty);
        }
        while values.len() > 1 {
            let mut next = Vec::with_capacity(values.len().div_ceil(2));
            let mut iter = values.into_iter();
            while let Some(left) = iter.next() {
                if let Some(right) = iter.next() {
                    self.bump()?;
                    next.push(self.set(left, right, kind));
                } else {
                    next.push(left);
                }
            }
            values = next;
        }
        Ok(values.pop().unwrap())
    }
    fn compile(&mut self, expr: &q::GraphExpr, depth: usize) -> Result<Operator> {
        self.bump()?;
        if depth > self.limits.max_ast_depth {
            return Err(limit());
        }
        if expr.version != 1 {
            return Err(EvalError::Invalid("expression version"));
        }
        use q::graph_expr::Expression as X;
        match expr
            .expression
            .as_ref()
            .ok_or(EvalError::Invalid("missing expression"))?
        {
            X::Commits(value) => {
                self.literals = self
                    .literals
                    .checked_add(value.commits.len())
                    .ok_or_else(limit)?;
                if self.literals > self.limits.max_literals {
                    return Err(limit());
                }
                let mut ids = BTreeSet::new();
                for id in &value.commits {
                    ids.insert(
                        m::CommitId::from_slice(&id.value)
                            .map_err(|_| EvalError::Invalid("commit ID width"))?,
                    );
                }
                Ok(Operator::Commits(Commits {
                    ids: ids.into_iter().collect(),
                    next: 0,
                    rows: BTreeMap::new(),
                }))
            }
            X::Ancestors(value) => {
                let heads = self.compile(child(&value.heads)?, depth + 1)?;
                let generations = value
                    .generations
                    .as_ref()
                    .map(|range| Self::window(range.start, range.end_exclusive))
                    .transpose()?
                    .unwrap_or(Window::all());
                let parents = value
                    .parents
                    .as_ref()
                    .map(|range| {
                        Self::window(range.start as u64, range.end_exclusive.map(u64::from))
                    })
                    .transpose()?
                    .unwrap_or(Window::all());
                Ok(self.ancestors(heads, generations, parents))
            }
            X::Range(value) => {
                let heads = self.compile(child(&value.heads)?, depth + 1)?;
                let roots = self.compile(child(&value.roots)?, depth + 1)?;
                let generations = value
                    .generations
                    .as_ref()
                    .map(|range| Self::window(range.start, range.end_exclusive))
                    .transpose()?
                    .unwrap_or(Window::all());
                let parents = value
                    .parents
                    .as_ref()
                    .map(|range| {
                        Self::window(range.start as u64, range.end_exclusive.map(u64::from))
                    })
                    .transpose()?
                    .unwrap_or(Window::all());
                self.bump()?;
                let mut wanted = self.ancestors(heads, generations, parents);
                let excluded = self.ancestors(roots, Window::all(), Window::all());
                if let Operator::Ancestors(state) = &mut wanted {
                    state.exclusion = Some(Exclusion {
                        input: Box::new(excluded),
                        look: None,
                        done: false,
                    });
                }
                Ok(wanted)
            }
            X::Union(value) | X::Intersection(value) => {
                let kind = if matches!(expr.expression.as_ref(), Some(X::Union(_))) {
                    SetKind::Union
                } else {
                    SetKind::Intersection
                };
                if matches!(kind, SetKind::Intersection) && value.inputs.is_empty() {
                    return Err(EvalError::Invalid("empty intersection"));
                }
                let mut values =
                    Vec::with_capacity(value.inputs.len().min(self.limits.max_ast_nodes));
                for input in &value.inputs {
                    values.push(self.compile(input, depth + 1)?);
                }
                self.balanced(values, kind)
            }
            X::Difference(value) => {
                let left = self.compile(child(&value.left)?, depth + 1)?;
                let right = self.compile(child(&value.right)?, depth + 1)?;
                Ok(self.set(left, right, SetKind::Difference))
            }
            X::Latest(value) => {
                let input = self.compile(child(&value.candidates)?, depth + 1)?;
                let count = usize::try_from(value.count).map_err(|_| limit())?;
                if count > self.limits.max_latest {
                    return Err(limit());
                }
                if count == 0 {
                    Ok(Operator::Empty)
                } else {
                    Ok(Operator::Latest(Latest {
                        input: Box::new(input),
                        count,
                        done: false,
                        selection: BTreeMap::new(),
                        output: BTreeMap::new(),
                    }))
                }
            }
            X::Heads(value) => {
                let input = self.compile(child(&value.input)?, depth + 1)?;
                Ok(Operator::Heads(Heads {
                    input: Box::new(input),
                    look: None,
                    done: false,
                    excluded: BTreeMap::new(),
                    expansion: None,
                }))
            }
            X::HasConflict(value) => {
                let input = self.compile(child(&value.input)?, depth + 1)?;
                Ok(Operator::HasConflict(HasConflict {
                    input: Box::new(input),
                    look: None,
                    done: false,
                }))
            }
            X::Roots(value) => {
                let input = self.compile(child(&value.input)?, depth + 1)?;
                Ok(Operator::Roots(Roots {
                    input: Box::new(input),
                    look: None,
                    done: false,
                    scanning: false,
                    scan: None,
                    candidates: BTreeMap::new(),
                }))
            }
            X::DagRange(value) => {
                let roots = self.compile(child(&value.roots)?, depth + 1)?;
                let heads = self.compile(child(&value.heads)?, depth + 1)?;
                let generations = value
                    .generations_from_roots
                    .as_ref()
                    .map(|range| Self::window(range.start, range.end_exclusive))
                    .transpose()?
                    .unwrap_or(Window::all());
                // Generated candidate ancestry plus the one live probe/seed.
                self.bump()?;
                self.bump()?;
                self.bump()?;
                if generations.empty() {
                    return Ok(Operator::Empty);
                }
                let heads = self.ancestors(heads, Window::all(), Window::all());
                Ok(Operator::DagRange(DagRange {
                    roots_input: Box::new(roots),
                    roots_done: false,
                    roots: BTreeMap::new(),
                    heads: Box::new(heads),
                    heads_done: false,
                    look: None,
                    checking: false,
                    scan: None,
                    probe: None,
                    generations,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinytree_index::Index;
    fn expression(value: q::graph_expr::Expression) -> q::GraphExpr {
        q::GraphExpr {
            version: 1,
            expression: Some(value),
        }
    }
    fn commits(ids: &[m::CommitId]) -> q::GraphExpr {
        expression(q::graph_expr::Expression::Commits(q::CommitSet {
            commits: ids
                .iter()
                .map(|id| q::CommitId {
                    value: id.0.to_vec(),
                })
                .collect(),
        }))
    }
    fn ancestors(
        heads: q::GraphExpr,
        start: u64,
        end: Option<u64>,
        parents: Option<(u32, Option<u32>)>,
    ) -> q::GraphExpr {
        expression(q::graph_expr::Expression::Ancestors(
            q::AncestorsExpr {
                heads: Some(heads.into()),
                generations: Some(q::GenerationRange {
                    start,
                    end_exclusive: end,
                }),
                parents: parents.map(|(start, end_exclusive)| q::ParentRange {
                    start,
                    end_exclusive,
                }),
            }
            .into(),
        ))
    }
    fn latest(input: q::GraphExpr, count: u64) -> q::GraphExpr {
        expression(q::graph_expr::Expression::Latest(
            q::LatestExpr {
                candidates: Some(input.into()),
                count,
            }
            .into(),
        ))
    }
    fn roots(input: q::GraphExpr) -> q::GraphExpr {
        expression(q::graph_expr::Expression::Roots(
            q::UnaryExpr {
                input: Some(input.into()),
            }
            .into(),
        ))
    }
    fn dag_range(
        roots: q::GraphExpr,
        heads: q::GraphExpr,
        start: u64,
        end: Option<u64>,
    ) -> q::GraphExpr {
        expression(q::graph_expr::Expression::DagRange(
            q::DagRangeExpr {
                roots: Some(roots.into()),
                heads: Some(heads.into()),
                generations_from_roots: Some(q::GenerationRange {
                    start,
                    end_exclusive: end,
                }),
            }
            .into(),
        ))
    }
    fn canonical(
        parents: &[m::CommitId],
        ordinal: usize,
        millis: i64,
        offset: i32,
    ) -> m::EncodedObject {
        let mut commit = m::synthetic_root();
        commit.parents = parents.to_vec();
        commit.change_id = m::ChangeId([ordinal as u8; 16]);
        commit.description = format!("commit {ordinal}");
        commit.committer.timestamp.millis_since_epoch = millis;
        commit.committer.timestamp.tz_offset_minutes = offset;
        m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap()
    }
    fn linear() -> (tempfile::TempDir, Arc<OverlayGraph>, Vec<m::CommitId>) {
        let directory = tempfile::tempdir().unwrap();
        let index = Index::open(
            directory.path().join("index"),
            &tinytree_index::Limits::default(),
        )
        .unwrap();
        let mut ids = vec![m::CommitId::ROOT];
        for ordinal in 1..=10 {
            let value = canonical(&[ids[ordinal - 1]], ordinal, 0, 0);
            ids.push(
                index
                    .ingest("repo", value.descriptor, &value.bytes)
                    .unwrap()
                    .commit,
            );
        }
        let graph = OverlayGraph::remote(Arc::new(index.scope("repo", &[ids[10]]).unwrap()));
        (directory, graph, ids)
    }
    fn collect(
        graph: Arc<OverlayGraph>,
        expr: q::GraphExpr,
        limits: EvalLimits,
    ) -> (Vec<m::CommitId>, usize, u64) {
        let mut evaluator = Evaluator::new(graph, expr, limits).unwrap();
        let mut budget = WorkBudget::default();
        let mut rows = Vec::new();
        let mut progress = 0;
        let mut previous = None;
        for _ in 0..100_000 {
            match evaluator.step(&mut budget).unwrap() {
                EvalStep::Row(node) => {
                    if let Some(key) = previous {
                        assert!(
                            node.key() < key,
                            "rows must be globally children-before-parents and unique"
                        );
                    }
                    previous = Some(node.key());
                    rows.push(node.commit());
                }
                EvalStep::Progress => progress += 1,
                EvalStep::Done => return (rows, progress, evaluator.steps()),
            }
        }
        panic!("incremental evaluator did not finish")
    }
    #[test]
    fn conflict_filter_is_incremental_and_empty_clean_history_is_not_false_progress_eof() {
        let (_directory, graph, ids) = linear();
        let filter = expression(q::graph_expr::Expression::HasConflict(Box::new(
            q::UnaryExpr {
                input: Some(Box::new(ancestors(commits(&[ids[10]]), 0, None, None))),
            },
        )));
        let (rows, progress, _) = collect(
            graph.clone(),
            filter,
            EvalLimits {
                max_step_work: 1,
                ..Default::default()
            },
        );
        assert!(rows.is_empty());
        assert!(progress > 0);
        let empty = expression(q::graph_expr::Expression::HasConflict(Box::new(
            q::UnaryExpr {
                input: Some(Box::new(commits(&[]))),
            },
        )));
        let (rows, _, _) = collect(
            graph,
            empty,
            EvalLimits {
                max_step_work: 1,
                ..Default::default()
            },
        );
        assert!(rows.is_empty());
    }
    #[test]
    fn disjoint_generation_windows_match_pinned_jj_linear_fixtures() {
        let (_directory, graph, ids) = linear();
        struct Case {
            seeds: Vec<usize>,
            start: u64,
            end: u64,
            expected: Vec<usize>,
        }
        for case in [
            Case {
                seeds: vec![9, 6],
                start: 4,
                end: 6,
                expected: vec![5, 4, 2, 1],
            },
            Case {
                seeds: vec![10, 7, 6],
                start: 5,
                end: 7,
                expected: vec![5, 4, 2, 1, 0],
            },
        ] {
            let seeds: Vec<_> = case.seeds.iter().map(|index| ids[*index]).collect();
            let result = collect(
                graph.clone(),
                ancestors(commits(&seeds), case.start, Some(case.end), None),
                EvalLimits::default(),
            )
            .0;
            assert_eq!(
                result,
                case.expected
                    .iter()
                    .map(|index| ids[*index])
                    .collect::<Vec<_>>()
            );
        }
    }
    #[test]
    fn merge_path_holes_and_parent_filters_are_exact_and_range_roots_use_all_parents() {
        let directory = tempfile::tempdir().unwrap();
        let index = Index::open(
            directory.path().join("index"),
            &tinytree_index::Limits::default(),
        )
        .unwrap();
        let add = |parents: &[m::CommitId], ordinal| {
            let value = canonical(parents, ordinal, 0, 0);
            index
                .ingest("repo", value.descriptor, &value.bytes)
                .unwrap()
                .commit
        };
        let root = m::CommitId::ROOT;
        let a = add(&[root], 1);
        let b = add(&[a], 2);
        let c = add(&[b, root], 3);
        let x = add(&[root], 4);
        let e = add(&[b, x], 5);
        let graph = OverlayGraph::remote(Arc::new(index.scope("repo", &[c, e]).unwrap()));
        assert_eq!(
            collect(
                graph.clone(),
                ancestors(commits(&[c]), 2, Some(3), None),
                EvalLimits::default()
            )
            .0,
            vec![a],
            "root reachable at lengths1and3 is not reachable at length2"
        );
        assert_eq!(
            collect(
                graph.clone(),
                ancestors(commits(&[c]), 1, Some(2), Some((0, Some(1)))),
                EvalLimits::default()
            )
            .0,
            vec![b]
        );
        assert_eq!(
            collect(
                graph.clone(),
                ancestors(commits(&[c]), 1, None, Some((1, Some(2)))),
                EvalLimits::default()
            )
            .0,
            vec![root]
        );
        let range = expression(q::graph_expr::Expression::Range(
            q::RangeExpr {
                roots: Some(commits(&[e]).into()),
                heads: Some(commits(&[x]).into()),
                generations: None,
                parents: Some(q::ParentRange {
                    start: 0,
                    end_exclusive: Some(1),
                }),
            }
            .into(),
        ));
        assert!(collect(graph, range, EvalLimits::default()).0.is_empty());
    }
    #[test]
    fn ordered_set_operators_deduplicate_and_match_reference_sets() {
        let (_directory, graph, ids) = linear();
        struct Case {
            expr: q::GraphExpr,
            expected: Vec<usize>,
        }
        let left = commits(&[ids[8], ids[10], ids[6], ids[8]]);
        let right = commits(&[ids[2], ids[6], ids[8]]);
        for case in [
            Case {
                expr: expression(q::graph_expr::Expression::Union(
                    q::SetExpr {
                        inputs: vec![left.clone(), right.clone()],
                    }
                    .into(),
                )),
                expected: vec![10, 8, 6, 2],
            },
            Case {
                expr: expression(q::graph_expr::Expression::Intersection(
                    q::SetExpr {
                        inputs: vec![left.clone(), right.clone()],
                    }
                    .into(),
                )),
                expected: vec![8, 6],
            },
            Case {
                expr: expression(q::graph_expr::Expression::Difference(
                    q::DifferenceExpr {
                        left: Some(left.into()),
                        right: Some(right.into()),
                    }
                    .into(),
                )),
                expected: vec![10],
            },
            Case {
                expr: expression(q::graph_expr::Expression::Union(
                    q::SetExpr { inputs: vec![] }.into(),
                )),
                expected: vec![],
            },
        ] {
            assert_eq!(
                collect(graph.clone(), case.expr, EvalLimits::default()).0,
                case.expected
                    .iter()
                    .map(|index| ids[*index])
                    .collect::<Vec<_>>()
            );
        }
    }
    #[test]
    fn latest_uses_signed_utc_and_position_ties_but_emits_topologically() {
        let directory = tempfile::tempdir().unwrap();
        let index = Index::open(
            directory.path().join("index"),
            &tinytree_index::Limits::default(),
        )
        .unwrap();
        let mut ids = vec![m::CommitId::ROOT];
        for (parent, millis, offset) in [(0, -10, 120), (1, 30, -60), (1, 20, 840), (3, 20, -840)] {
            let value = canonical(&[ids[parent]], ids.len(), millis, offset);
            ids.push(
                index
                    .ingest("repo", value.descriptor, &value.bytes)
                    .unwrap()
                    .commit,
            );
        }
        let graph = OverlayGraph::remote(Arc::new(index.scope("repo", &[ids[2], ids[4]]).unwrap()));
        assert_eq!(
            collect(
                graph.clone(),
                latest(commits(&ids[1..]), 3),
                EvalLimits::default()
            )
            .0,
            vec![ids[4], ids[3], ids[2]]
        );
        assert_eq!(
            collect(
                graph.clone(),
                latest(commits(&ids[1..]), 2),
                EvalLimits::default()
            )
            .0,
            vec![ids[4], ids[2]]
        );
        assert_eq!(
            collect(graph, latest(commits(&ids[1..]), 4), EvalLimits::default()).0,
            vec![ids[4], ids[3], ids[2], ids[1]]
        );
    }
    #[test]
    fn quantum_one_nested_execution_progresses_and_cumulative_cpu_cannot_be_renewed() {
        let (_directory, graph, ids) = linear();
        let limits = EvalLimits {
            max_step_work: 1,
            ..EvalLimits::default()
        };
        let expr = latest(
            ancestors(commits(&[ids[10], ids[7], ids[6]]), 5, Some(7), None),
            2,
        );
        let (rows, progress, steps) = collect(graph.clone(), expr, limits.clone());
        assert_eq!(rows, vec![ids[5], ids[4]]);
        assert!(progress > 0);
        assert!(steps < 10_000);
        let mut evaluator = Evaluator::new(
            graph,
            commits(&ids),
            EvalLimits {
                max_steps: 2,
                ..limits
            },
        )
        .unwrap();
        assert!(matches!(
            evaluator.step(&mut WorkBudget::default()).unwrap(),
            EvalStep::Progress
        ));
        assert!(matches!(
            evaluator.step(&mut WorkBudget::default()),
            Err(EvalError::Graph(GraphError::BudgetExceeded))
        ));
        assert!(matches!(
            evaluator.step(&mut WorkBudget::default()),
            Err(EvalError::Graph(GraphError::Unavailable))
        ));
    }
    #[test]
    fn zero_limits_skip_input_io_and_unknown_literals_are_explicit_errors() {
        let (_directory, graph, _ids) = linear();
        let unknown = commits(&[m::CommitId([99; 32])]);
        for expr in [
            latest(unknown.clone(), 0),
            ancestors(unknown.clone(), 0, Some(0), None),
        ] {
            let mut evaluator = Evaluator::new(graph.clone(), expr, EvalLimits::default()).unwrap();
            let mut budget = WorkBudget::new(0, 0, 0, 0);
            assert!(matches!(
                evaluator.step(&mut budget).unwrap(),
                EvalStep::Done
            ));
            assert_eq!(budget.nodes_read(), 0);
        }
        let mut evaluator = Evaluator::new(graph, unknown, EvalLimits::default()).unwrap();
        assert!(matches!(
            evaluator.step(&mut WorkBudget::default()),
            Err(EvalError::Invalid(_))
        ));
    }
    #[test]
    fn aggregate_memory_ast_and_invalid_work_never_become_empty_results() {
        let (_directory, graph, ids) = linear();
        let union = expression(q::graph_expr::Expression::Union(
            q::SetExpr {
                inputs: vec![commits(&ids[1..3]), commits(&ids[3..5])],
            }
            .into(),
        ));
        let mut evaluator = Evaluator::new(
            graph.clone(),
            union,
            EvalLimits {
                max_frontier: 2,
                ..EvalLimits::default()
            },
        )
        .unwrap();
        assert!(matches!(
            evaluator.step(&mut WorkBudget::default()),
            Err(EvalError::Graph(GraphError::BudgetExceeded))
        ));
        let invalid = expression(q::graph_expr::Expression::Intersection(
            q::SetExpr { inputs: vec![] }.into(),
        ));
        assert!(matches!(
            Evaluator::new(graph.clone(), invalid, EvalLimits::default()),
            Err(EvalError::Invalid(_))
        ));
        assert!(matches!(
            Evaluator::new(
                graph.clone(),
                ancestors(commits(&[ids[1]]), 2, Some(1), None),
                EvalLimits::default()
            ),
            Err(EvalError::Invalid(_))
        ));
        let invalid = expression(q::graph_expr::Expression::DagRange(
            q::DagRangeExpr {
                roots: Some(commits(&[ids[1]]).into()),
                heads: Some(commits(&[ids[10]]).into()),
                generations_from_roots: Some(q::GenerationRange {
                    start: 3,
                    end_exclusive: Some(2),
                }),
            }
            .into(),
        ));
        assert!(matches!(
            Evaluator::new(graph, invalid, EvalLimits::default()),
            Err(EvalError::Invalid(_))
        ));
    }

    #[test]
    fn default_log_range_completion_has_history_independent_probe_bound() {
        let mut probes = None;
        for public_count in [64usize, 4096] {
            let directory = tempfile::tempdir().unwrap();
            let index = Index::open(
                directory.path().join("index"),
                &tinytree_index::Limits::default(),
            )
            .unwrap();
            let mut public_tip = m::CommitId::ROOT;
            for ordinal in 1..=public_count {
                let value = canonical(&[public_tip], ordinal, 0, 0);
                public_tip = index
                    .ingest("repo", value.descriptor, &value.bytes)
                    .unwrap()
                    .commit;
            }
            let graph = OverlayGraph::remote(Arc::new(index.scope("repo", &[public_tip]).unwrap()));
            let mut drafts = Vec::new();
            let mut expected = Vec::new();
            let mut tip = public_tip;
            for ordinal in 1..=10 {
                let value = canonical(&[tip], public_count + ordinal, 0, 0);
                tip = m::CommitId(value.descriptor.key.id.0);
                expected.push(tip);
                drafts.push((value.descriptor, Arc::<[u8]>::from(value.bytes)));
            }
            let graph = OverlayGraph::extend(
                graph,
                drafts,
                &crate::query_overlay::OverlayLimits::default(),
                &mut WorkBudget::default(),
                Arc::new(()),
            )
            .unwrap();
            expected.reverse();
            expected.push(public_tip);
            let range = expression(q::graph_expr::Expression::Range(
                q::RangeExpr {
                    roots: Some(commits(&[public_tip]).into()),
                    heads: Some(commits(&[tip, public_tip]).into()),
                    generations: None,
                    parents: None,
                }
                .into(),
            ));
            let log = expression(q::graph_expr::Expression::Union(
                q::SetExpr {
                    inputs: vec![
                        commits(&[tip]),
                        ancestors(range, 0, Some(2), None),
                        commits(&[public_tip]),
                    ],
                }
                .into(),
            ));
            let mut evaluator = Evaluator::new(
                graph,
                log,
                EvalLimits {
                    max_step_work: 1,
                    ..EvalLimits::default()
                },
            )
            .unwrap();
            let mut budget = WorkBudget::new(256, 4096, 4 * 1024 * 1024, 4096);
            let mut observed = Vec::new();
            let mut done = false;
            let mut progress = 0;
            for _ in 0..20_000 {
                match evaluator.step(&mut budget).unwrap() {
                    EvalStep::Row(node) => observed.push(node.commit()),
                    EvalStep::Progress => progress += 1,
                    EvalStep::Done => {
                        done = true;
                        break;
                    }
                }
            }
            assert!(
                done,
                "range must prove completion without walking the shared public tail"
            );
            assert_eq!(observed, expected);
            assert!(progress > 0);
            assert!(budget.nodes_read() < 256);
            if let Some(previous) = probes {
                assert_eq!(
                    budget.nodes_read(),
                    previous,
                    "fixed private work should not scan more public commits"
                );
            }
            probes = Some(budget.nodes_read());
        }
    }
    #[test]
    fn fused_range_prunes_all_merge_distances_at_a_shared_excluded_boundary() {
        let (_directory, remote, ids) = linear();
        let boundary = ids[10];
        let mut drafts = Vec::new();
        let a = canonical(&[boundary], 11, 0, 0);
        let aid = m::CommitId(a.descriptor.key.id.0);
        drafts.push((a.descriptor, Arc::<[u8]>::from(a.bytes)));
        let b = canonical(&[aid], 12, 0, 0);
        let bid = m::CommitId(b.descriptor.key.id.0);
        drafts.push((b.descriptor, Arc::<[u8]>::from(b.bytes)));
        let x = canonical(&[boundary], 13, 0, 0);
        let xid = m::CommitId(x.descriptor.key.id.0);
        drafts.push((x.descriptor, Arc::<[u8]>::from(x.bytes)));
        let merge = canonical(&[bid, xid], 14, 0, 0);
        let mid = m::CommitId(merge.descriptor.key.id.0);
        drafts.push((merge.descriptor, Arc::<[u8]>::from(merge.bytes)));
        let graph = OverlayGraph::extend(
            remote,
            drafts,
            &crate::query_overlay::OverlayLimits::default(),
            &mut WorkBudget::default(),
            Arc::new(()),
        )
        .unwrap();
        struct Case {
            start: u64,
            end: Option<u64>,
            parents: Option<(u32, Option<u32>)>,
            expected: Vec<m::CommitId>,
        }
        for case in [
            Case {
                start: 0,
                end: None,
                parents: None,
                expected: vec![mid, xid, bid, aid],
            },
            Case {
                start: 2,
                end: Some(3),
                parents: None,
                expected: vec![aid],
            },
            Case {
                start: 0,
                end: None,
                parents: Some((0, Some(1))),
                expected: vec![mid, bid, aid],
            },
        ] {
            let range = expression(q::graph_expr::Expression::Range(
                q::RangeExpr {
                    roots: Some(commits(&[boundary]).into()),
                    heads: Some(commits(&[mid]).into()),
                    generations: Some(q::GenerationRange {
                        start: case.start,
                        end_exclusive: case.end,
                    }),
                    parents: case.parents.map(|(start, end_exclusive)| q::ParentRange {
                        start,
                        end_exclusive,
                    }),
                }
                .into(),
            ));
            assert_eq!(
                collect(
                    graph.clone(),
                    range,
                    EvalLimits {
                        max_step_work: 1,
                        ..EvalLimits::default()
                    }
                )
                .0,
                case.expected
            );
        }
    }

    #[test]
    fn streaming_heads_matches_reference_for_all_merge_dag_subsets() {
        let directory = tempfile::tempdir().unwrap();
        let index = Index::open(
            directory.path().join("index"),
            &tinytree_index::Limits::default(),
        )
        .unwrap();
        let add = |parents: &[m::CommitId], ordinal| {
            let value = canonical(parents, ordinal, 0, 0);
            index
                .ingest("repo", value.descriptor, &value.bytes)
                .unwrap()
                .commit
        };
        let root = m::CommitId::ROOT;
        let a = add(&[root], 1);
        let b = add(&[root], 2);
        let c = add(&[a, b], 3);
        let d = add(&[a], 4);
        let e = add(&[d, b], 5);
        let model = BTreeMap::from([
            (root, vec![]),
            (a, vec![root]),
            (b, vec![root]),
            (c, vec![a, b]),
            (d, vec![a]),
            (e, vec![d, b]),
        ]);
        let reaches = |ancestor: m::CommitId, head: m::CommitId| {
            let mut pending = vec![head];
            let mut seen = BTreeSet::new();
            let mut found = false;
            while let Some(id) = pending.pop() {
                if id == ancestor {
                    found = true;
                    break;
                }
                if seen.insert(id) {
                    pending.extend(model.get(&id).unwrap().iter().copied());
                }
            }
            found
        };
        let graph = OverlayGraph::remote(Arc::new(index.scope("repo", &[c, e]).unwrap()));
        let ids = [root, a, b, c, d, e];
        for mask in 0usize..1 << ids.len() {
            let candidates: Vec<_> = ids
                .iter()
                .enumerate()
                .filter_map(|(bit, id)| (mask & (1 << bit) != 0).then_some(*id))
                .collect();
            let expected: Vec<_> = ids
                .iter()
                .rev()
                .copied()
                .filter(|candidate| {
                    candidates.contains(candidate)
                        && !candidates
                            .iter()
                            .any(|other| candidate != other && reaches(*candidate, *other))
                })
                .collect();
            let expr = expression(q::graph_expr::Expression::Heads(
                q::UnaryExpr {
                    input: Some(commits(&candidates).into()),
                }
                .into(),
            ));
            assert_eq!(
                collect(
                    graph.clone(),
                    expr,
                    EvalLimits {
                        max_step_work: 1,
                        ..EvalLimits::default()
                    }
                )
                .0,
                expected
            );
        }
    }
    #[test]
    fn streaming_heads_handles_inherited_private_candidates_duplicates_and_root() {
        let (_directory, remote, ids) = linear();
        let boundary = ids[10];
        let a = canonical(&[boundary], 11, 0, 0);
        let aid = m::CommitId(a.descriptor.key.id.0);
        let hidden = canonical(&[boundary], 12, 0, 0);
        let hid = m::CommitId(hidden.descriptor.key.id.0);
        let first = OverlayGraph::extend(
            remote,
            vec![
                (a.descriptor, Arc::<[u8]>::from(a.bytes)),
                (hidden.descriptor, Arc::<[u8]>::from(hidden.bytes)),
            ],
            &crate::query_overlay::OverlayLimits::default(),
            &mut WorkBudget::default(),
            Arc::new(()),
        )
        .unwrap();
        let b = canonical(&[aid], 13, 0, 0);
        let bid = m::CommitId(b.descriptor.key.id.0);
        let graph = OverlayGraph::extend(
            first,
            vec![(b.descriptor, Arc::<[u8]>::from(b.bytes))],
            &crate::query_overlay::OverlayLimits::default(),
            &mut WorkBudget::default(),
            Arc::new(()),
        )
        .unwrap();
        struct Case {
            input: Vec<m::CommitId>,
            expected: Vec<m::CommitId>,
        }
        for case in [
            Case {
                input: vec![m::CommitId::ROOT, boundary, aid, bid, hid, aid],
                expected: vec![bid, hid],
            },
            Case {
                input: vec![m::CommitId::ROOT, m::CommitId::ROOT],
                expected: vec![m::CommitId::ROOT],
            },
            Case {
                input: vec![],
                expected: vec![],
            },
        ] {
            let expr = expression(q::graph_expr::Expression::Heads(
                q::UnaryExpr {
                    input: Some(commits(&case.input).into()),
                }
                .into(),
            ));
            assert_eq!(
                collect(
                    graph.clone(),
                    expr,
                    EvalLimits {
                        max_step_work: 1,
                        ..EvalLimits::default()
                    }
                )
                .0,
                case.expected
            );
        }
    }
    #[test]
    fn streaming_heads_never_drains_public_tail_after_candidate_eof() {
        let mut expected_probes = None;
        for public_count in [64usize, 4096] {
            let directory = tempfile::tempdir().unwrap();
            let index = Index::open(
                directory.path().join("index"),
                &tinytree_index::Limits::default(),
            )
            .unwrap();
            let mut boundary = m::CommitId::ROOT;
            for ordinal in 1..=public_count {
                let value = canonical(&[boundary], ordinal, 0, 0);
                boundary = index
                    .ingest("repo", value.descriptor, &value.bytes)
                    .unwrap()
                    .commit;
            }
            let remote = OverlayGraph::remote(Arc::new(index.scope("repo", &[boundary]).unwrap()));
            let mut drafts = Vec::new();
            let mut tip = boundary;
            for ordinal in 1..=10 {
                let value = canonical(&[tip], public_count + ordinal, 0, 0);
                tip = m::CommitId(value.descriptor.key.id.0);
                drafts.push((value.descriptor, Arc::<[u8]>::from(value.bytes)));
            }
            let graph = OverlayGraph::extend(
                remote,
                drafts,
                &crate::query_overlay::OverlayLimits::default(),
                &mut WorkBudget::default(),
                Arc::new(()),
            )
            .unwrap();
            let expr = expression(q::graph_expr::Expression::Heads(
                q::UnaryExpr {
                    input: Some(commits(&[boundary, tip]).into()),
                }
                .into(),
            ));
            let mut evaluator = Evaluator::new(
                graph,
                expr,
                EvalLimits {
                    max_step_work: 1,
                    ..EvalLimits::default()
                },
            )
            .unwrap();
            let mut budget = WorkBudget::new(128, 4096, 4 * 1024 * 1024, 4096);
            let mut rows = Vec::new();
            let mut done = false;
            for _ in 0..10_000 {
                match evaluator.step(&mut budget).unwrap() {
                    EvalStep::Row(node) => rows.push(node.commit()),
                    EvalStep::Progress => {}
                    EvalStep::Done => {
                        done = true;
                        break;
                    }
                }
            }
            assert!(done);
            assert_eq!(rows, vec![tip]);
            assert!(budget.nodes_read() < 128);
            if let Some(previous) = expected_probes {
                assert_eq!(previous, budget.nodes_read());
            }
            expected_probes = Some(budget.nodes_read());
        }
    }
    /// Manual in-process default-log workload. Setup is separate from query
    /// execution; successful and exhausted runs are never timing substitutes.
    #[test]
    #[ignore = "manual Range boundary/default-log before-after benchmark"]
    fn benchmark_default_log_range_boundary() {
        use std::time::Instant;
        fn checkpoint(
            started: Instant,
            steps: u64,
            budget: &WorkBudget,
            calls: usize,
        ) -> serde_json::Value {
            serde_json::json!({"elapsed_ns":started.elapsed().as_nanos(),"steps":steps,
                "nodes":budget.nodes_read(),"edges":budget.edges_read(),"bytes":budget.bytes_read(),"step_calls":calls})
        }
        for public_count in [1000usize, 16_000, 64_000] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("index");
            let source_started = Instant::now();
            let mut source = Vec::with_capacity(public_count);
            let mut public_tip = m::CommitId::ROOT;
            let mut canonical_bytes = 0u64;
            for ordinal in 1..=public_count {
                let value = canonical(&[public_tip], ordinal, 0, 0);
                public_tip = m::CommitId(value.descriptor.key.id.0);
                canonical_bytes += value.descriptor.size_bytes;
                source.push(value);
            }
            let source_ns = source_started.elapsed().as_nanos();
            let started = Instant::now();
            let index = Index::open(&path, &tinytree_index::Limits::default()).unwrap();
            let index_open_ns = started.elapsed().as_nanos();
            let started = Instant::now();
            for value in source {
                index
                    .ingest("benchmark", value.descriptor, &value.bytes)
                    .unwrap();
            }
            let index_ingest_ns = started.elapsed().as_nanos();
            drop(index);
            let started = Instant::now();
            let index = Index::open(&path, &tinytree_index::Limits::default()).unwrap();
            let index_reopen_ns = started.elapsed().as_nanos();
            let started = Instant::now();
            let scope = Arc::new(index.scope("benchmark", &[public_tip]).unwrap());
            let graph = OverlayGraph::remote(scope);
            let scope_open_ns = started.elapsed().as_nanos();
            let started = Instant::now();
            let mut drafts = Vec::new();
            let mut draft_ids = Vec::new();
            let mut parent = public_tip;
            for ordinal in 1..=10 {
                let value = canonical(&[parent], public_count + ordinal, 0, 0);
                parent = m::CommitId(value.descriptor.key.id.0);
                draft_ids.push(parent);
                drafts.push((value.descriptor, Arc::<[u8]>::from(value.bytes)));
            }
            let draft_source_ns = started.elapsed().as_nanos();
            let started = Instant::now();
            let graph = OverlayGraph::extend(
                graph,
                drafts,
                &crate::query_overlay::OverlayLimits::default(),
                &mut WorkBudget::default(),
                Arc::new(()),
            )
            .unwrap();
            let overlay_ns = started.elapsed().as_nanos();
            let mut expected: Vec<_> = draft_ids.iter().rev().copied().collect();
            expected.push(public_tip);
            let range = expression(q::graph_expr::Expression::Range(
                q::RangeExpr {
                    roots: Some(commits(&[public_tip]).into()),
                    heads: Some(commits(&[parent, public_tip]).into()),
                    generations: None,
                    parents: None,
                }
                .into(),
            ));
            let log = expression(q::graph_expr::Expression::Union(
                q::SetExpr {
                    inputs: vec![
                        commits(&[parent]),
                        ancestors(range, 0, Some(2), None),
                        commits(&[public_tip]),
                    ],
                }
                .into(),
            ));
            println!(
                "{}",
                serde_json::json!({"benchmark":"default_log_range_boundary_v1","record":"setup","transport":"in_process_scoped_index",
                "public_commits":public_count,"private_commits":10,"canonical_public_bytes":canonical_bytes,
                "source_ns":source_ns,"index_open_ns":index_open_ns,"index_ingest_ns":index_ingest_ns,"index_reopen_ns":index_reopen_ns,
                "scope_open_ns":scope_open_ns,"draft_source_ns":draft_source_ns,"overlay_extend_ns":overlay_ns})
            );
            for trial in 0..3 {
                let mut default_failed = false;
                for generous in [false, true] {
                    if generous && !default_failed {
                        continue;
                    }
                    let config = crate::query_index::QueryConfig::default();
                    let mut budget = if generous {
                        WorkBudget::new(10_000_000, 20_000_000, 4 * 1024 * 1024 * 1024, 200_000)
                    } else {
                        config.budget()
                    };
                    let limits = if generous {
                        EvalLimits {
                            max_steps: 20_000_000,
                            ..EvalLimits::default()
                        }
                    } else {
                        EvalLimits::default()
                    };
                    let reserved_memory = limits.memory_bytes().unwrap();
                    let mut evaluator = Evaluator::new(graph.clone(), log.clone(), limits).unwrap();
                    let started = Instant::now();
                    let mut observed = 0usize;
                    let mut calls = 0usize;
                    let mut progress = 0usize;
                    let mut checkpoints = serde_json::Map::new();
                    let mut peak_slots = 0usize;
                    let mut peak_intervals = 0usize;
                    let status = loop {
                        calls += 1;
                        let step = evaluator.step(&mut budget);
                        peak_slots = peak_slots.max(evaluator.memory.used.slots);
                        peak_intervals = peak_intervals.max(evaluator.memory.used.intervals);
                        match step {
                            Ok(EvalStep::Row(node)) => {
                                assert_eq!(
                                    expected.get(observed).copied(),
                                    Some(node.commit()),
                                    "default log must return exactly D10..D1,PN"
                                );
                                observed += 1;
                                if [1, 9, 10, 11].contains(&observed) {
                                    checkpoints.insert(
                                        format!("row_{observed}"),
                                        checkpoint(started, evaluator.steps(), &budget, calls),
                                    );
                                }
                            }
                            Ok(EvalStep::Progress) => progress += 1,
                            Ok(EvalStep::Done) => {
                                assert_eq!(observed, expected.len());
                                checkpoints.insert(
                                    "done".into(),
                                    checkpoint(started, evaluator.steps(), &budget, calls),
                                );
                                break "complete";
                            }
                            Err(EvalError::Graph(GraphError::BudgetExceeded)) => {
                                assert!(!generous, "generous run must prove exact completion");
                                default_failed = true;
                                break "budget_exhausted";
                            }
                            Err(_) => panic!("unexpected default-log benchmark error class"),
                        }
                    };
                    println!(
                        "{}",
                        serde_json::json!({"benchmark":"default_log_range_boundary_v1","record":"query","transport":"in_process_scoped_index",
                        "public_commits":public_count,"private_commits":10,"trial":trial,"mode":if generous{"generous_after_default_exhaustion"}else{"default"},
                        "status":status,"complete":status=="complete","observed_rows":observed,"oracle_rows":11,"checkpoints":checkpoints,
                        "elapsed_ns":started.elapsed().as_nanos(),"steps":evaluator.steps(),"nodes":budget.nodes_read(),"edges":budget.edges_read(),"bytes":budget.bytes_read(),
                        "step_calls":calls,"progress_steps":progress,"estimated_host_quanta":calls.div_ceil(config.max_eval_quantum_steps),
                        "sampled_retained_slots":peak_slots,"sampled_interval_capacity":peak_intervals,"reserved_memory_bytes":reserved_memory,
                        "work_node_limit":budget.max_nodes,"work_edge_limit":budget.max_edges,"work_byte_limit":budget.max_bytes})
                    );
                }
            }
        }
    }
    #[test]
    fn roots_and_dag_range_match_reference_merge_sets_with_single_transition_quanta() {
        let directory = tempfile::tempdir().unwrap();
        let index = Index::open(directory.path().join("index"), &Default::default()).unwrap();
        let add = |parents: &[m::CommitId], ordinal| {
            let object = canonical(parents, ordinal, 0, 0);
            index
                .ingest("repo", object.descriptor, &object.bytes)
                .unwrap()
                .commit
        };
        let r = m::CommitId::ROOT;
        let a = add(&[r], 1);
        let b = add(&[a], 2);
        let c = add(&[b, r], 3);
        let x = add(&[r], 4);
        let e = add(&[b, x], 5);
        let graph = OverlayGraph::remote(Arc::new(index.scope("repo", &[c, e]).unwrap()));
        struct Case {
            name: &'static str,
            expr: q::GraphExpr,
            expected: Vec<m::CommitId>,
        }
        for case in [
            Case {
                name: "roots exclude non-direct descendants",
                expr: roots(commits(&[c, a])),
                expected: vec![a],
            },
            Case {
                name: "roots retain disconnected minimal branches",
                expr: roots(commits(&[c, e, b, x])),
                expected: vec![x, b],
            },
            Case {
                name: "intrinsic root removes every selected descendant",
                expr: roots(commits(&[c, e, r])),
                expected: vec![r],
            },
            Case {
                name: "empty roots selection",
                expr: roots(commits(&[])),
                expected: vec![],
            },
            Case {
                name: "full dag range",
                expr: dag_range(commits(&[a]), commits(&[c, e]), 0, None),
                expected: vec![e, c, b, a],
            },
            Case {
                name: "generation zero equals reachable roots",
                expr: dag_range(commits(&[b, x]), commits(&[c]), 0, Some(1)),
                expected: vec![b],
            },
            Case {
                name: "root shortcuts are generation one",
                expr: dag_range(commits(&[r]), commits(&[c, e]), 0, Some(2)),
                expected: vec![x, c, a, r],
            },
            Case {
                name: "generation holes are not min-max filled",
                expr: dag_range(commits(&[r]), commits(&[c, e]), 2, Some(3)),
                expected: vec![e, b],
            },
            Case {
                name: "long alternate paths are retained",
                expr: dag_range(commits(&[r]), commits(&[c, e]), 3, Some(4)),
                expected: vec![e, c],
            },
            Case {
                name: "multiple roots share inclusive descendants",
                expr: dag_range(commits(&[b, x]), commits(&[c, e]), 1, Some(2)),
                expected: vec![e, c],
            },
            Case {
                name: "root unreachable from heads",
                expr: dag_range(commits(&[x]), commits(&[c]), 0, None),
                expected: vec![],
            },
            Case {
                name: "empty generation interval",
                expr: dag_range(commits(&[a]), commits(&[c, e]), 2, Some(2)),
                expected: vec![],
            },
        ] {
            let (actual, progress, _) = collect(
                graph.clone(),
                case.expr,
                EvalLimits {
                    max_step_work: 1,
                    ..Default::default()
                },
            );
            assert_eq!(actual, case.expected, "{}", case.name);
            if !case.expected.is_empty() {
                assert!(progress > 0, "{} must remain resumable", case.name);
            }
        }
    }
    #[test]
    fn empty_dag_roots_skip_heads_and_new_operators_share_cumulative_limits() {
        let (_directory, graph, ids) = linear();
        let unknown = m::CommitId([99; 32]);
        for expression in [
            dag_range(commits(&[]), commits(&[unknown]), 0, None),
            dag_range(commits(&[unknown]), commits(&[unknown]), 2, Some(2)),
        ] {
            let mut evaluator = Evaluator::new(
                graph.clone(),
                expression,
                EvalLimits {
                    max_step_work: 1,
                    ..Default::default()
                },
            )
            .unwrap();
            let mut budget = WorkBudget::new(0, 0, 0, 32);
            let mut done = false;
            for _ in 0..100 {
                match evaluator.step(&mut budget).unwrap() {
                    EvalStep::Progress => {}
                    EvalStep::Done => {
                        done = true;
                        break;
                    }
                    EvalStep::Row(_) => panic!("empty query cannot emit"),
                }
            }
            assert!(done);
            assert_eq!(budget.nodes_read(), 0);
        }
        for expression in [
            roots(ancestors(commits(&[ids[10]]), 0, None, None)),
            dag_range(commits(&[ids[0]]), commits(&[ids[10]]), 2, Some(4)),
        ] {
            let mut evaluator = Evaluator::new(
                graph.clone(),
                expression,
                EvalLimits {
                    max_step_work: 1,
                    ..Default::default()
                },
            )
            .unwrap();
            let mut budget = WorkBudget::new(8, 32, 4096, 32);
            let mut failed = false;
            for _ in 0..1000 {
                match evaluator.step(&mut budget) {
                    Ok(EvalStep::Progress | EvalStep::Row(_)) => {}
                    Err(EvalError::Graph(GraphError::BudgetExceeded)) => {
                        failed = true;
                        break;
                    }
                    _ => panic!("query must report its shared work cap"),
                }
            }
            assert!(failed);
        }
    }
    #[test]
    fn dag_roots_candidate_and_probe_use_one_aggregate_frontier() {
        let (_directory, graph, ids) = linear();
        let expression = dag_range(commits(&[ids[0], ids[1]]), commits(&[ids[10]]), 2, Some(3));
        let mut evaluator = Evaluator::new(
            graph,
            expression,
            EvalLimits {
                max_frontier: 3,
                max_step_work: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let mut budget = WorkBudget::default();
        let mut failed = false;
        for _ in 0..1000 {
            match evaluator.step(&mut budget) {
                Ok(EvalStep::Progress | EvalStep::Row(_)) => {}
                Err(EvalError::Graph(GraphError::BudgetExceeded)) => {
                    failed = true;
                    break;
                }
                _ => panic!("rootset, candidate and probe cannot exceed their shared frontier"),
            }
        }
        assert!(failed);
    }
    // BEGIN TINYTREE_ROOTS_DAGRANGE_READY_BENCH_V1
    // Insert inside the existing query_eval.rs #[cfg(test)] mod tests, immediately
    // before its final closing brace. This uses that module's fixture helpers.
    #[test]
    #[ignore = "manual ready-index Roots/DagRange evaluator benchmark"]
    fn benchmark_roots_and_dag_range_ready_component() {
        let overall = std::time::Instant::now();
        for public_count in [128usize, 1024, 4096] {
            let opened = std::time::Instant::now();
            let directory = tempfile::tempdir().unwrap();
            let index = Index::open(directory.path().join("index"), &Default::default()).unwrap();
            let index_open_ms = opened.elapsed().as_secs_f64() * 1000.0;
            let seeded = std::time::Instant::now();
            let mut boundary = m::CommitId::ROOT;
            let mut canonical_public_bytes = 0usize;
            for ordinal in 1..=public_count {
                let object = canonical(&[boundary], ordinal, 0, 0);
                canonical_public_bytes += object.bytes.len();
                boundary = index
                    .ingest("repo", object.descriptor, &object.bytes)
                    .unwrap()
                    .commit;
            }
            let public_seed_ms = seeded.elapsed().as_secs_f64() * 1000.0;
            let scoped = std::time::Instant::now();
            let remote = OverlayGraph::remote(Arc::new(index.scope("repo", &[boundary]).unwrap()));
            let remote_scope_ms = scoped.elapsed().as_secs_f64() * 1000.0;
            let drafted = std::time::Instant::now();
            let mut draft_objects = Vec::new();
            let mut selected = vec![boundary];
            let mut head = boundary;
            let mut canonical_private_bytes = 0usize;
            for ordinal in 1..=10 {
                let object = canonical(&[head], public_count + ordinal, 0, 0);
                head = m::CommitId(object.descriptor.key.id.0);
                canonical_private_bytes += object.bytes.len();
                selected.push(head);
                draft_objects.push((object.descriptor, Arc::<[u8]>::from(object.bytes)));
            }
            let linear = OverlayGraph::extend(
                remote.clone(),
                draft_objects,
                &crate::query_overlay::OverlayLimits::default(),
                &mut WorkBudget::default(),
                Arc::new(()),
            )
            .unwrap();
            let private_prepare_ms = drafted.elapsed().as_secs_f64() * 1000.0;
            let mut middle = selected[2..4].to_vec();
            middle.reverse();
            let mut full = selected.clone();
            full.reverse();
            let mut cases = vec![
                (
                    "roots_fixed_ten_drafts_plus_boundary",
                    linear.clone(),
                    roots(commits(&selected)),
                    vec![boundary],
                ),
                (
                    "dag_full_fixed_ten_drafts",
                    linear.clone(),
                    dag_range(commits(&[boundary]), commits(&[head]), 0, None),
                    full,
                ),
                (
                    "dag_generations_two_to_four",
                    linear,
                    dag_range(commits(&[boundary]), commits(&[head]), 2, Some(4)),
                    middle,
                ),
            ];
            // Unequal paths from a to d have lengths1/3; from a to e lengths2/4.
            // Therefore generation3 must include d but NOT e; generation4 includes e.
            let merged = std::time::Instant::now();
            let mut objects = Vec::new();
            let mut add = |parents: &[m::CommitId], ordinal| {
                let object = canonical(parents, public_count + ordinal, 0, 0);
                let id = m::CommitId(object.descriptor.key.id.0);
                objects.push((object.descriptor, Arc::<[u8]>::from(object.bytes)));
                id
            };
            let a = add(&[boundary], 21);
            let b = add(&[a], 22);
            let c = add(&[b], 23);
            let d = add(&[c, a], 24);
            let e = add(&[d], 25);
            let merge_graph = OverlayGraph::extend(
                remote,
                objects,
                &crate::query_overlay::OverlayLimits::default(),
                &mut WorkBudget::default(),
                Arc::new(()),
            )
            .unwrap();
            let merge_prepare_ms = merged.elapsed().as_secs_f64() * 1000.0;
            cases.push((
                "dag_unequal_merge_generation_three",
                merge_graph.clone(),
                dag_range(commits(&[a]), commits(&[e]), 3, Some(4)),
                vec![d],
            ));
            cases.push((
                "dag_unequal_merge_generation_four",
                merge_graph,
                dag_range(commits(&[a]), commits(&[e]), 4, Some(5)),
                vec![e],
            ));
            let stats = index.stats().unwrap();
            assert_eq!(stats.nodes, public_count as u64);
            for (name, graph, expression, expected) in cases {
                assert!(
                    overall.elapsed() < std::time::Duration::from_secs(180),
                    "bounded fixture/query loop expired"
                );
                let limits = EvalLimits::default();
                let frontier_limit = limits.max_frontier;
                let interval_limit = limits.max_intervals;
                let started = std::time::Instant::now();
                let mut evaluator = Evaluator::new(graph, expression, limits).unwrap();
                let compile_ms = started.elapsed().as_secs_f64() * 1000.0;
                let walking = std::time::Instant::now();
                let mut budget = WorkBudget::default();
                let mut observed = Vec::new();
                let mut progress = 0usize;
                let mut status = "step_limit".to_owned();
                let mut complete = false;
                for _ in 0..100_000 {
                    assert!(
                        overall.elapsed() < std::time::Duration::from_secs(180),
                        "bounded evaluation loop expired"
                    );
                    match evaluator.step(&mut budget) {
                        Ok(EvalStep::Row(node)) => {
                            assert_eq!(
                                expected.get(observed.len()),
                                Some(&node.commit()),
                                "{name}: ordered output prefix differs"
                            );
                            observed.push(node.commit());
                        }
                        Ok(EvalStep::Progress) => progress += 1,
                        Ok(EvalStep::Done) => {
                            complete = true;
                            status = "complete".into();
                            break;
                        }
                        Err(EvalError::Graph(GraphError::BudgetExceeded)) => {
                            status = "resource_exhausted".into();
                            break;
                        }
                        Err(error) => {
                            status = format!("error:{error}");
                            break;
                        }
                    }
                }
                let walk_ms = walking.elapsed().as_secs_f64() * 1000.0;
                if complete {
                    assert_eq!(observed, expected, "{name}: complete coverage differs");
                }
                println!(
                    "TINYTREE_ROOTS_DAGRANGE_READY_BENCH_V1 {}",
                    serde_json::json!({
                        "case":name,"public_commits":public_count,"linear_private_chain_commits":10,"merge_private_commits":5,
                        "index_open_ms":index_open_ms,"public_seed_ms":public_seed_ms,"remote_scope_ms":remote_scope_ms,
                        "private_prepare_ms":private_prepare_ms,"merge_prepare_ms":merge_prepare_ms,
                        "canonical_public_bytes":canonical_public_bytes,"linear_private_canonical_bytes":canonical_private_bytes,
                        "index_nodes":stats.nodes,"index_logical_bytes":stats.logical_bytes,
                        "compile_ms":compile_ms,"walk_ms":walk_ms,"ready_query_ms":compile_ms+walk_ms,
                        "cpu_steps":evaluator.steps(),"metadata_probes":budget.nodes_read(),"edges_read":budget.edges_read(),"metadata_bytes":budget.bytes_read(),
                        "progress_steps":progress,"observed_rows":observed.len(),"expected_rows":expected.len(),"complete":complete,"status":status,
                        "configured_frontier_slots":frontier_limit,"configured_interval_slots":interval_limit,"peak_frontier":"not instrumented",
                        "scope":"ready production index/private overlay; in-process evaluator, no transport or CLI timing",
                        "baseline":"operators previously unsupported; no before/after speedup ratio",
                        "file_rpcs":0
                    })
                );
            }
        }
    }
    // END TINYTREE_ROOTS_DAGRANGE_READY_BENCH_V1
}
