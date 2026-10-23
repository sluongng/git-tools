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
struct Ancestors {
    heads: Box<Operator>,
    look: Option<GraphNode>,
    done: bool,
    queue: BTreeMap<GraphKey, Visit>,
    expansion: Option<Expansion>,
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
            if selected {
                return Ok(EvalStep::Row(visit.node));
            }
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
                count
            }
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
                self.bump()?;
                let wanted = self.ancestors(heads, generations, parents);
                let excluded = self.ancestors(roots, Window::all(), Window::all());
                Ok(self.set(wanted, excluded, SetKind::Difference))
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
            X::Heads(_) => Err(EvalError::Unsupported("Heads")),
            X::Roots(_) => Err(EvalError::Unsupported("Roots")),
            X::DagRange(_) => Err(EvalError::Unsupported("DagRange")),
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
    fn aggregate_memory_ast_and_unsupported_work_never_become_empty_results() {
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
        let unsupported = expression(q::graph_expr::Expression::Heads(
            q::UnaryExpr {
                input: Some(commits(&[ids[1]]).into()),
            }
            .into(),
        ));
        assert!(matches!(
            Evaluator::new(graph, unsupported, EvalLimits::default()),
            Err(EvalError::Unsupported(_))
        ));
    }
}
