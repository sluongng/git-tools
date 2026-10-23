// Copyright 2021 The Jujutsu Authors
// Copyright 2026 The Tinytree Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded resumable port of jj 0.46's default_index/revset_graph_iterator.rs.
//! The selected stream is consumed once. External summaries, canonical parent
//! order, selective dedup and conditional transitive reduction follow that port;
//! no second revset/membership evaluation or complete selected-set index is used.
use crate::query_eval::{EvalError, EvalStep, Evaluator};
use crate::query_overlay::{GraphKey, GraphNode, OverlayGraph};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tinytree_index::{Error, WorkBudget};
type Result<T> = std::result::Result<T, EvalError>;
fn limit() -> EvalError {
    Error::BudgetExceeded.into()
}
#[derive(Clone, Debug)]
pub(crate) struct GraphLimits {
    pub max_nodes: usize,
    pub max_edges: usize,
    pub max_keys: usize,
    pub max_frames: usize,
    pub max_step_work: usize,
    pub max_steps: u64,
}
impl Default for GraphLimits {
    fn default() -> Self {
        Self {
            max_nodes: 8192,
            max_edges: 8192,
            max_keys: 16384,
            max_frames: 4096,
            max_step_work: 128,
            max_steps: 4_000_000,
        }
    }
}
impl GraphLimits {
    /// Additional to the sole IDs evaluator's memory reservation. Capacities,
    /// shared unique summaries, parent temporaries and escaped rows are charged.
    pub fn memory_bytes(&self) -> Option<usize> {
        self.max_nodes
            .checked_mul((std::mem::size_of::<GraphNode>() + 64).max(512))?
            .checked_add(
                self.max_edges
                    .checked_mul((std::mem::size_of::<SelectedEdge>() + 64).max(256))?,
            )?
            .checked_add(self.max_keys.checked_mul(192)?)?
            .checked_add(
                self.max_frames
                    .checked_mul((std::mem::size_of::<Task>() + 64).max(1024))?,
            )?
            .checked_add(std::mem::size_of::<GraphEvaluator>() * 2)?
            .checked_add(2 * 1024 * 1024)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EdgeKind {
    Direct,
    Indirect,
    Missing,
}
#[derive(Clone)]
pub(crate) struct SelectedEdge {
    pub kind: EdgeKind,
    pub target: GraphNode,
}
pub(crate) struct SelectedRow {
    pub node: GraphNode,
    pub edges: Vec<SelectedEdge>,
    // Release accounting after physical node/edge teardown, including rows held
    // by a caller while the wrapper continues another quantum.
    _lease: Lease,
}
pub(crate) enum GraphStep {
    Row(SelectedRow),
    Progress,
    Done,
}
#[derive(Clone, Copy, Default)]
struct Counts {
    nodes: usize,
    edges: usize,
    keys: usize,
    frames: usize,
}
impl Counts {
    fn add(self, b: Self) -> Option<Self> {
        Some(Self {
            nodes: self.nodes.checked_add(b.nodes)?,
            edges: self.edges.checked_add(b.edges)?,
            keys: self.keys.checked_add(b.keys)?,
            frames: self.frames.checked_add(b.frames)?,
        })
    }
    fn sum(self) -> Option<usize> {
        self.nodes
            .checked_add(self.edges)?
            .checked_add(self.keys)?
            .checked_add(self.frames)
    }
    fn sub(self, b: Self) -> Self {
        Self {
            nodes: self.nodes - b.nodes,
            edges: self.edges - b.edges,
            keys: self.keys - b.keys,
            frames: self.frames - b.frames,
        }
    }
    fn times(self, n: usize) -> Option<Self> {
        Some(Self {
            nodes: self.nodes.checked_mul(n)?,
            edges: self.edges.checked_mul(n)?,
            keys: self.keys.checked_mul(n)?,
            frames: self.frames.checked_mul(n)?,
        })
    }
}
struct Memory {
    used: Counts,
    caps: Counts,
    frontier: usize,
}
struct Lease {
    memory: Arc<Mutex<Memory>>,
    counts: Counts,
}
impl Lease {
    fn new(memory: &Arc<Mutex<Memory>>, counts: Counts) -> Result<Self> {
        {
            let mut state = memory
                .lock()
                .map_err(|_| EvalError::Graph(Error::Unavailable))?;
            let next = state.used.add(counts).ok_or_else(limit)?;
            if next.nodes > state.caps.nodes
                || next.edges > state.caps.edges
                || next.keys > state.caps.keys
                || next.frames > state.caps.frames
                || next.sum().ok_or_else(limit)? > state.frontier
            {
                return Err(limit());
            }
            state.used = next;
        }
        Ok(Self {
            memory: memory.clone(),
            counts,
        })
    }
    fn empty(memory: &Arc<Mutex<Memory>>) -> Self {
        Self {
            memory: memory.clone(),
            counts: Counts::default(),
        }
    }
    fn absorb(&mut self, mut other: Self) {
        self.counts = self
            .counts
            .add(other.counts)
            .expect("admitted graph counts fit");
        other.counts = Counts::default();
    }
    fn release(&mut self, counts: Counts) {
        let mut state = self.memory.lock().unwrap();
        state.used = state.used.sub(counts);
        self.counts = self.counts.sub(counts);
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut memory = self.memory.lock().unwrap();
        memory.used = memory.used.sub(self.counts);
    }
}
struct Slots<T> {
    values: Vec<T>,
    lease: Lease,
    weight: Counts,
}
impl<T> Slots<T> {
    fn new(memory: &Arc<Mutex<Memory>>, weight: Counts) -> Self {
        Self {
            values: Vec::new(),
            lease: Lease::empty(memory),
            weight,
        }
    }
    fn push(&mut self, value: T) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let extra = self.values.capacity().max(1);
            let old = self.values.capacity();
            // Reserve old+new capacity during allocator growth, not merely its
            // eventual delta. Reallocation can physically retain both buffers.
            let added = Lease::new(
                &self.lease.memory,
                self.weight
                    .times(old.checked_add(extra).ok_or_else(limit)?)
                    .ok_or_else(limit)?,
            )?;
            self.values.try_reserve_exact(extra).map_err(|_| limit())?;
            self.lease.absorb(added);
            self.lease
                .release(self.weight.times(old).ok_or_else(limit)?);
        }
        self.values.push(value);
        Ok(())
    }
    fn pop(&mut self) -> Option<T> {
        self.values.pop()
    }
}
struct Keys {
    values: BTreeMap<GraphKey, Lease>,
    memory: Arc<Mutex<Memory>>,
}
impl Keys {
    fn new(memory: &Arc<Mutex<Memory>>) -> Self {
        Self {
            values: BTreeMap::new(),
            memory: memory.clone(),
        }
    }
    fn insert(&mut self, key: GraphKey) -> Result<bool> {
        if self.values.contains_key(&key) {
            return Ok(false);
        }
        let lease = Lease::new(
            &self.memory,
            Counts {
                keys: 1,
                ..Default::default()
            },
        )?;
        self.values.insert(key, lease);
        Ok(true)
    }
    fn contains(&self, key: GraphKey) -> bool {
        self.values.contains_key(&key)
    }
}
struct Edges {
    items: Slots<SelectedEdge>,
    all_missing: bool,
    indirect: bool,
}
impl Edges {
    fn new(memory: &Arc<Mutex<Memory>>) -> Self {
        Self {
            items: Slots::new(
                memory,
                Counts {
                    nodes: 1,
                    edges: 1,
                    ..Default::default()
                },
            ),
            all_missing: true,
            indirect: false,
        }
    }
    fn push(&mut self, edge: SelectedEdge) -> Result<()> {
        self.all_missing &= edge.kind == EdgeKind::Missing;
        self.indirect |= edge.kind == EdgeKind::Indirect;
        self.items.push(edge)
    }
}
struct Cached {
    edges: Arc<Edges>,
    _lease: Lease,
}
struct Selected {
    node: GraphNode,
    _lease: Lease,
}
struct NodeOwner {
    node: GraphNode,
    lease: Lease,
}
impl NodeOwner {
    fn new(node: GraphNode, memory: &Arc<Mutex<Memory>>) -> Result<Self> {
        Ok(Self {
            node,
            lease: Lease::new(
                memory,
                Counts {
                    nodes: 1,
                    ..Default::default()
                },
            )?,
        })
    }
}
#[derive(Clone, Copy)]
enum Phase {
    LoadParents,
    Consume,
    Classify,
    Expand,
    Finish,
}
struct Compute {
    owner: NodeOwner,
    internal: bool,
    parents: Option<Slots<GraphNode>>,
    parent: usize,
    phase: Phase,
    edges: Edges,
    known: Keys,
    expansion: Option<(Arc<Edges>, usize)>,
    shortcut: Option<Arc<Edges>>,
}
impl Compute {
    fn new(owner: NodeOwner, internal: bool, memory: &Arc<Mutex<Memory>>) -> Self {
        Self {
            owner,
            internal,
            parents: None,
            parent: 0,
            phase: Phase::LoadParents,
            edges: Edges::new(memory),
            known: Keys::new(memory),
            expansion: None,
            shortcut: None,
        }
    }
}
#[derive(Clone, Copy)]
enum ReductionPhase {
    Scan,
    Seed,
    Walk,
    Enqueue,
    Filter,
}
struct Reduction {
    owner: NodeOwner,
    internal: bool,
    input: Edges,
    output: Edges,
    phase: ReductionPhase,
    index: usize,
    minimum: Option<GraphKey>,
    generation: u64,
    initial: Keys,
    unwanted: Keys,
    work: Slots<GraphNode>,
    enqueue: Option<(ParentSource, usize)>,
    after_enqueue: ReductionPhase,
}
enum ParentSource {
    Cached(Arc<Edges>),
    Raw(Slots<GraphNode>),
}
struct Emit {
    owner: NodeOwner,
    input: Arc<Edges>,
    output: Edges,
    index: usize,
}
enum Task {
    Start(NodeOwner),
    Compute(Compute),
    Reduce(Reduction),
    Emit(Emit),
}
pub(crate) struct GraphEvaluator {
    graph: Arc<OverlayGraph>,
    evaluator: Evaluator,
    limits: GraphLimits,
    selected: BTreeMap<GraphKey, Selected>,
    cache: BTreeMap<GraphKey, Cached>,
    minimum: Option<GraphKey>,
    input_done: bool,
    tasks: Slots<Task>,
    memory: Arc<Mutex<Memory>>,
    steps: u64,
    failed: bool,
    input_progress: bool,
}
impl GraphEvaluator {
    pub fn new(
        graph: Arc<OverlayGraph>,
        evaluator: Evaluator,
        limits: GraphLimits,
    ) -> Result<Self> {
        if !Arc::ptr_eq(&graph, evaluator.graph()) {
            return Err(EvalError::Invalid(
                "graph evaluator scope differs from input stream",
            ));
        }
        if limits.max_nodes == 0
            || limits.max_edges == 0
            || limits.max_keys == 0
            || limits.max_frames == 0
            || limits.max_step_work == 0
            || limits.max_steps == 0
            || limits.memory_bytes().is_none()
        {
            return Err(EvalError::Invalid("invalid selected graph limits"));
        }
        let memory = Arc::new(Mutex::new(Memory {
            used: Counts::default(),
            caps: Counts {
                nodes: limits.max_nodes,
                edges: limits.max_edges,
                keys: limits.max_keys,
                frames: limits.max_frames,
            },
            frontier: usize::MAX,
        }));
        Ok(Self {
            graph,
            evaluator,
            limits,
            selected: BTreeMap::new(),
            cache: BTreeMap::new(),
            minimum: None,
            input_done: false,
            tasks: Slots::new(
                &memory,
                Counts {
                    frames: 1,
                    ..Default::default()
                },
            ),
            memory,
            steps: 0,
            failed: false,
            input_progress: false,
        })
    }
    pub fn step(&mut self, budget: &mut WorkBudget) -> Result<GraphStep> {
        if self.failed {
            return Err(Error::Unavailable.into());
        }
        let retained = {
            let mut memory = self.memory.lock().unwrap();
            memory.frontier = budget.max_frontier;
            memory.used.sum().ok_or_else(limit)?
        };
        let result = budget
            .frontier(retained)
            .map_err(EvalError::from)
            .and_then(|()| self.quantum(budget));
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn quantum(&mut self, budget: &mut WorkBudget) -> Result<GraphStep> {
        for _ in 0..self.limits.max_step_work {
            self.steps = self.steps.checked_add(1).ok_or_else(limit)?;
            if self.steps > self.limits.max_steps {
                return Err(limit());
            }
            if let Some(task) = self.tasks.pop() {
                if let Some(row) = self.advance(task, budget)? {
                    return Ok(GraphStep::Row(row));
                }
                if self.input_progress {
                    self.input_progress = false;
                    return Ok(GraphStep::Progress);
                }
            } else if let Some((_, selected)) = self.selected.pop_last() {
                let owner = NodeOwner::new(selected.node.clone(), &self.memory)?;
                drop(selected);
                self.tasks.push(Task::Start(owner))?;
            } else if self.input_done {
                return Ok(GraphStep::Done);
            } else if !self.pump(budget)? {
                return Ok(GraphStep::Progress);
            }
        }
        Ok(GraphStep::Progress)
    }
    // One inner quantum at a time; max_step_work=1 still makes progress because
    // every input row and every graph continuation is persisted before yielding.
    fn pump(&mut self, budget: &mut WorkBudget) -> Result<bool> {
        if self.input_done {
            return Ok(true);
        }
        let lease = Lease::new(
            &self.memory,
            Counts {
                nodes: 1,
                keys: 1,
                ..Default::default()
            },
        )?;
        match self.evaluator.step(budget)? {
            EvalStep::Progress => Ok(false),
            EvalStep::Done => {
                self.input_done = true;
                Ok(true)
            }
            EvalStep::Row(node) => {
                let key = node.key();
                if self.minimum.is_some_and(|minimum| key >= minimum) {
                    return Err(EvalError::Invalid(
                        "selected input is not strictly children-before-parents",
                    ));
                }
                self.minimum = Some(key);
                self.selected.insert(
                    key,
                    Selected {
                        node,
                        _lease: lease,
                    },
                );
                Ok(true)
            }
        }
    }
    fn consumed(&self, key: GraphKey) -> bool {
        self.input_done || self.minimum.is_some_and(|minimum| key >= minimum)
    }
    fn parents(&self, node: &GraphNode, budget: &mut WorkBudget) -> Result<Slots<GraphNode>> {
        let available = {
            let state = self.memory.lock().unwrap();
            (state.caps.nodes - state.used.nodes).min(
                state
                    .frontier
                    .saturating_sub(state.used.sum().ok_or_else(limit)?),
            ) / 2
        };
        // The remote adapter can briefly own both ScopedNode and GraphNode
        // vectors. Reserve both before its bounded parent API allocates either.
        let mut lease = Lease::new(
            &self.memory,
            Counts {
                nodes: available * 2,
                ..Default::default()
            },
        )?;
        let original = budget.max_frontier;
        budget.max_frontier = original.min(available);
        let result = self.graph.parent_nodes(node, budget);
        budget.max_frontier = original;
        let values = result?;
        if values.capacity() > available {
            return Err(limit());
        }
        {
            let mut state = self.memory.lock().unwrap();
            state.used = state.used.sub(Counts {
                nodes: available * 2 - values.capacity(),
                ..Default::default()
            });
        }
        lease.counts.nodes = values.capacity();
        Ok(Slots {
            values,
            lease,
            weight: Counts {
                nodes: 1,
                ..Default::default()
            },
        })
    }
    fn insert_cache(&mut self, key: GraphKey, edges: Arc<Edges>) -> Result<()> {
        let lease = Lease::new(
            &self.memory,
            Counts {
                keys: 1,
                ..Default::default()
            },
        )?;
        self.cache.insert(
            key,
            Cached {
                edges,
                _lease: lease,
            },
        );
        Ok(())
    }
    fn finish(
        &mut self,
        owner: NodeOwner,
        internal: bool,
        edges: Edges,
    ) -> Result<Option<SelectedRow>> {
        if internal {
            let mut lease = edges.items.lease;
            lease.absorb(owner.lease);
            Ok(Some(SelectedRow {
                node: owner.node,
                edges: edges.items.values,
                _lease: lease,
            }))
        } else {
            self.insert_cache(owner.node.key(), Arc::new(edges))?;
            Ok(None)
        }
    }
    fn finish_shared(&mut self, owner: NodeOwner, internal: bool, edges: Arc<Edges>) -> Result<()> {
        if internal {
            self.tasks.push(Task::Emit(Emit {
                owner,
                input: edges,
                output: Edges::new(&self.memory),
                index: 0,
            }))?;
        } else {
            self.insert_cache(owner.node.key(), edges)?;
        }
        Ok(())
    }
    fn advance(&mut self, task: Task, budget: &mut WorkBudget) -> Result<Option<SelectedRow>> {
        match task {
            Task::Start(owner) => {
                if let Some((&key, _)) = self.cache.last_key_value() {
                    if key >= owner.node.key() {
                        let (_, cached) = self.cache.pop_last().unwrap();
                        if key == owner.node.key() {
                            self.finish_shared(owner, true, cached.edges)?;
                        } else {
                            drop(cached);
                            self.tasks.push(Task::Start(owner))?;
                        }
                        return Ok(None);
                    }
                }
                self.tasks
                    .push(Task::Compute(Compute::new(owner, true, &self.memory)))?;
            }
            Task::Emit(mut frame) => {
                if let Some(edge) = frame.input.items.values.get(frame.index) {
                    frame.output.push(edge.clone())?;
                    frame.index += 1;
                    self.tasks.push(Task::Emit(frame))?;
                } else {
                    return self.finish(frame.owner, true, frame.output);
                }
            }
            Task::Compute(mut frame) => {
                match frame.phase {
                    Phase::LoadParents => {
                        frame.parents = Some(self.parents(&frame.owner.node, budget)?);
                        frame.phase = if frame.parents.as_ref().unwrap().values.is_empty() {
                            Phase::Finish
                        } else {
                            Phase::Consume
                        };
                    }
                    Phase::Consume => {
                        let key = frame.parents.as_ref().unwrap().values[frame.parent].key();
                        if !self.consumed(key) && !self.pump(budget)? {
                            self.input_progress = true;
                        }
                        if self.consumed(key) {
                            frame.phase = Phase::Classify;
                        }
                    }
                    Phase::Classify => {
                        let parent = &frame.parents.as_ref().unwrap().values[frame.parent];
                        let key = parent.key();
                        if self.selected.contains_key(&key) {
                            frame.edges.push(SelectedEdge {
                                kind: if frame.internal {
                                    EdgeKind::Direct
                                } else {
                                    EdgeKind::Indirect
                                },
                                target: parent.clone(),
                            })?;
                            Self::next_parent(&mut frame);
                        } else if let Some(cached) = self.cache.get(&key) {
                            if cached.edges.all_missing {
                                frame.edges.push(SelectedEdge {
                                    kind: EdgeKind::Missing,
                                    target: parent.clone(),
                                })?;
                                Self::next_parent(&mut frame);
                            } else if frame.parents.as_ref().unwrap().values.len() == 1 {
                                frame.shortcut = Some(cached.edges.clone());
                                frame.phase = Phase::Finish;
                            } else {
                                frame.expansion = Some((cached.edges.clone(), 0));
                                frame.phase = Phase::Expand;
                            }
                        } else if !frame.internal
                            && self.minimum.is_none_or(|minimum| key < minimum)
                        {
                            frame.edges.push(SelectedEdge {
                                kind: EdgeKind::Missing,
                                target: parent.clone(),
                            })?;
                            Self::next_parent(&mut frame);
                        } else {
                            let owner = NodeOwner::new(parent.clone(), &self.memory)?;
                            self.tasks.push(Task::Compute(frame))?;
                            self.tasks.push(Task::Compute(Compute::new(
                                owner,
                                false,
                                &self.memory,
                            )))?;
                            return Ok(None);
                        }
                    }
                    Phase::Expand => {
                        let (source, index) = frame.expansion.as_mut().unwrap();
                        if let Some(edge) = source.items.values.get(*index) {
                            if frame.known.insert(edge.target.key())? {
                                frame.edges.push(edge.clone())?;
                            }
                            *index += 1;
                        } else {
                            frame.expansion = None;
                            Self::next_parent(&mut frame);
                        }
                    }
                    Phase::Finish => {
                        let Compute {
                            owner,
                            internal,
                            parents,
                            edges,
                            known,
                            shortcut,
                            ..
                        } = frame;
                        let parent_count = parents.as_ref().unwrap().values.len();
                        drop(parents);
                        drop(known);
                        if let Some(shared) = shortcut {
                            drop(edges);
                            self.finish_shared(owner, internal, shared)?;
                        } else if parent_count != 1 && edges.indirect {
                            self.tasks.push(Task::Reduce(Reduction {
                                owner,
                                internal,
                                input: edges,
                                output: Edges::new(&self.memory),
                                phase: ReductionPhase::Scan,
                                index: 0,
                                minimum: None,
                                generation: u64::MAX,
                                initial: Keys::new(&self.memory),
                                unwanted: Keys::new(&self.memory),
                                work: Slots::new(
                                    &self.memory,
                                    Counts {
                                        nodes: 1,
                                        ..Default::default()
                                    },
                                ),
                                enqueue: None,
                                after_enqueue: ReductionPhase::Seed,
                            }))?;
                        } else {
                            return self.finish(owner, internal, edges);
                        }
                        return Ok(None);
                    }
                }
                self.tasks.push(Task::Compute(frame))?;
            }
            Task::Reduce(mut frame) => {
                match frame.phase {
                    ReductionPhase::Scan => {
                        if let Some(edge) = frame.input.items.values.get(frame.index) {
                            if edge.kind != EdgeKind::Missing {
                                frame.minimum =
                                    Some(frame.minimum.map_or(edge.target.key(), |key| {
                                        key.min(edge.target.key())
                                    }));
                                frame.generation = frame.generation.min(edge.target.generation());
                            }
                            frame.index += 1;
                        } else {
                            frame.index = 0;
                            frame.phase = ReductionPhase::Seed;
                        }
                    }
                    ReductionPhase::Seed => {
                        if let Some(edge) = frame.input.items.values.get(frame.index) {
                            frame.index += 1;
                            if edge.kind != EdgeKind::Missing {
                                frame.initial.insert(edge.target.key())?;
                                frame.enqueue =
                                    Some((self.parent_source(&edge.target, budget)?, 0));
                                frame.after_enqueue = ReductionPhase::Seed;
                                frame.phase = ReductionPhase::Enqueue;
                            }
                        } else {
                            frame.phase = ReductionPhase::Walk;
                        }
                    }
                    ReductionPhase::Walk => {
                        if let Some(node) = frame.work.pop() {
                            // Pinned ordering is significant: initial targets
                            // reached from another target ARE unwanted, but are
                            // not expanded again (their parents were seeded).
                            if frame.unwanted.insert(node.key())?
                                && !frame.initial.contains(node.key())
                                && node.generation() >= frame.generation
                            {
                                frame.enqueue = Some((self.parent_source(&node, budget)?, 0));
                                frame.after_enqueue = ReductionPhase::Walk;
                                frame.phase = ReductionPhase::Enqueue;
                            }
                        } else {
                            frame.work = Slots::new(
                                &self.memory,
                                Counts {
                                    nodes: 1,
                                    ..Default::default()
                                },
                            );
                            frame.initial = Keys::new(&self.memory);
                            frame.index = 0;
                            frame.phase = ReductionPhase::Filter;
                        }
                    }
                    ReductionPhase::Enqueue => {
                        let (source, index) = frame.enqueue.as_mut().unwrap();
                        let node = match &*source {
                            ParentSource::Cached(edges) => edges
                                .items
                                .values
                                .get(*index)
                                .filter(|edge| edge.kind != EdgeKind::Missing)
                                .map(|edge| edge.target.clone()),
                            ParentSource::Raw(nodes) => nodes.values.get(*index).cloned(),
                        };
                        let length = match &*source {
                            ParentSource::Cached(edges) => edges.items.values.len(),
                            ParentSource::Raw(nodes) => nodes.values.len(),
                        };
                        if *index < length {
                            if let Some(node) = node {
                                if node.key() >= frame.minimum.expect("indirect edge is reachable")
                                {
                                    frame.work.push(node)?;
                                }
                            }
                            *index += 1;
                        } else {
                            frame.enqueue = None;
                            frame.phase = frame.after_enqueue;
                        }
                    }
                    ReductionPhase::Filter => {
                        if let Some(edge) = frame.input.items.values.get(frame.index) {
                            if edge.kind == EdgeKind::Missing
                                || !frame.unwanted.contains(edge.target.key())
                            {
                                frame.output.push(edge.clone())?;
                            }
                            frame.index += 1;
                        } else {
                            let Reduction {
                                owner,
                                internal,
                                output,
                                input,
                                initial,
                                unwanted,
                                work,
                                enqueue,
                                ..
                            } = frame;
                            drop(input);
                            drop(initial);
                            drop(unwanted);
                            drop(work);
                            drop(enqueue);
                            return self.finish(owner, internal, output);
                        }
                    }
                }
                self.tasks.push(Task::Reduce(frame))?;
            }
        }
        Ok(None)
    }
    fn next_parent(frame: &mut Compute) {
        frame.parent += 1;
        frame.phase = if frame.parent == frame.parents.as_ref().unwrap().values.len() {
            Phase::Finish
        } else {
            Phase::Consume
        };
    }
    fn parent_source(&self, node: &GraphNode, budget: &mut WorkBudget) -> Result<ParentSource> {
        if let Some(cached) = self.cache.get(&node.key()) {
            Ok(ParentSource::Cached(cached.edges.clone()))
        } else {
            Ok(ParentSource::Raw(self.parents(node, budget)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_eval::EvalLimits;
    use crate::query_overlay::OverlayLimits;
    use tinytree_index::Index;
    use tinytree_model as m;
    use tinytree_proto::query_v1 as q;
    struct Fixture {
        _directory: tempfile::TempDir,
        _index: Arc<Index>,
        graph: Arc<OverlayGraph>,
        ids: Vec<m::CommitId>,
    }
    fn commit(parents: Vec<m::CommitId>, position: usize) -> m::EncodedObject {
        let mut commit = m::synthetic_root();
        commit.parents = parents;
        commit.description = format!("selected graph fixture {position}");
        let mut change = [0u8; 16];
        change[..8].copy_from_slice(&(position as u64).to_le_bytes());
        commit.change_id = m::ChangeId(change);
        m::encode(&m::Object::Commit(commit), Default::default()).unwrap()
    }
    fn fixture(parents: &[&[usize]]) -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        let index = Index::open(directory.path(), &Default::default()).unwrap();
        let mut ids = vec![m::CommitId::ROOT];
        for (position, parents) in parents.iter().enumerate() {
            let object = commit(
                parents.iter().map(|&parent| ids[parent]).collect(),
                position + 1,
            );
            index
                .ingest("fixture", object.descriptor, &object.bytes)
                .unwrap();
            ids.push(m::CommitId(object.descriptor.key.id.0));
        }
        let scope = index.scope("fixture", &[*ids.last().unwrap()]).unwrap();
        let graph = OverlayGraph::remote(Arc::new(scope));
        Fixture {
            _directory: directory,
            _index: index,
            graph,
            ids,
        }
    }
    fn evaluator(
        graph: Arc<OverlayGraph>,
        ids: &[m::CommitId],
        mut limits: GraphLimits,
    ) -> GraphEvaluator {
        limits.max_step_work = 1;
        let expression = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: ids
                    .iter()
                    .map(|id| q::CommitId {
                        value: id.0.to_vec(),
                    })
                    .collect(),
            })),
        };
        let input = Evaluator::new(
            graph.clone(),
            expression,
            EvalLimits {
                max_step_work: 1,
                ..Default::default()
            },
        )
        .unwrap();
        GraphEvaluator::new(graph, input, limits).unwrap()
    }
    type Snapshot = Vec<(usize, Vec<(EdgeKind, usize)>)>;
    fn snapshot(fixture: &Fixture, selected: &[usize]) -> Snapshot {
        let ids = selected
            .iter()
            .map(|&index| fixture.ids[index])
            .collect::<Vec<_>>();
        let mut evaluator = evaluator(fixture.graph.clone(), &ids, Default::default());
        let mut budget = WorkBudget::default();
        let mut result = Vec::new();
        let mut progress = 0;
        for _ in 0..1_000_000 {
            match evaluator.step(&mut budget).unwrap() {
                GraphStep::Row(row) => result.push((
                    fixture
                        .ids
                        .iter()
                        .position(|id| *id == row.node.commit())
                        .unwrap(),
                    row.edges
                        .iter()
                        .map(|edge| {
                            (
                                edge.kind,
                                fixture
                                    .ids
                                    .iter()
                                    .position(|id| *id == edge.target.commit())
                                    .unwrap(),
                            )
                        })
                        .collect(),
                )),
                GraphStep::Progress => progress += 1,
                GraphStep::Done => {
                    assert!(progress > 0);
                    return result;
                }
            }
        }
        panic!("quantum one graph walk did not converge")
    }
    #[test]
    fn pinned_linearized_diamond_and_virtual_octopus_preserve_order_and_dedup() {
        use EdgeKind::*;
        let diamond = fixture(&[&[0], &[1], &[1], &[2, 3]]);
        assert_eq!(
            snapshot(&diamond, &[1, 4]),
            vec![(4, vec![(Indirect, 1)]), (1, vec![(Missing, 0)])]
        );
        let octopus = fixture(&[&[0], &[0], &[0], &[1, 2], &[2, 3], &[4, 5]]);
        assert_eq!(
            snapshot(&octopus, &[1, 2, 3, 6]),
            vec![
                (6, vec![(Indirect, 1), (Indirect, 2), (Indirect, 3)]),
                (3, vec![(Missing, 0)]),
                (2, vec![(Missing, 0)]),
                (1, vec![(Missing, 0)])
            ]
        );
    }
    #[test]
    fn pinned_multiple_missing_and_empty_root_collapse_at_immediate_boundaries() {
        use EdgeKind::*;
        let graph = fixture(&[&[0], &[0], &[0], &[1, 2], &[2, 3], &[4, 5]]);
        assert_eq!(
            snapshot(&graph, &[2, 6]),
            vec![
                (6, vec![(Missing, 1), (Indirect, 2), (Missing, 3)]),
                (2, vec![(Missing, 0)])
            ]
        );
        assert_eq!(
            snapshot(&graph, &[6]),
            vec![(6, vec![(Missing, 4), (Missing, 5)])]
        );
        let root = fixture(&[]);
        assert_eq!(snapshot(&root, &[0]), vec![(0, vec![])]);
    }
    #[test]
    fn pinned_conditional_reduction_preserves_direct_redundancy_and_precise_duplicates() {
        use EdgeKind::*;
        let direct = fixture(&[&[0], &[1], &[2, 1]]);
        assert_eq!(
            snapshot(&direct, &[1, 2, 3]),
            vec![
                (3, vec![(Direct, 2), (Direct, 1)]),
                (2, vec![(Direct, 1)]),
                (1, vec![(Missing, 0)])
            ]
        );
        let mixed = fixture(&[&[0], &[1], &[2], &[2, 1, 3]]);
        assert_eq!(
            snapshot(&mixed, &[1, 2, 4]),
            vec![
                (4, vec![(Direct, 2), (Indirect, 2)]),
                (2, vec![(Direct, 1)]),
                (1, vec![(Missing, 0)])
            ]
        );
        let ancestor = fixture(&[&[0], &[0], &[1], &[2, 3], &[3], &[4, 5]]);
        assert_eq!(
            snapshot(&ancestor, &[3, 4, 6]),
            vec![
                (6, vec![(Direct, 4)]),
                (4, vec![(Missing, 2), (Direct, 3)]),
                (3, vec![(Missing, 1)])
            ]
        );
    }
    #[test]
    fn pinned_deep_escape_reduces_through_raw_parents_beyond_selected_lookahead() {
        use EdgeKind::*;
        // A,b,c,D,e,f,G,H,i,J. This is jj's deep-escape fixture, not
        // simplification based on a second ancestors()/selected membership set.
        let graph = fixture(&[
            &[0],
            &[1],
            &[1],
            &[2],
            &[4],
            &[4, 3],
            &[2],
            &[6],
            &[5, 8],
            &[7, 9],
        ]);
        assert_eq!(
            snapshot(&graph, &[1, 4, 7, 8, 10]),
            vec![
                (10, vec![(Direct, 7), (Indirect, 8)]),
                (8, vec![(Indirect, 4)]),
                (7, vec![(Indirect, 1)]),
                (4, vec![(Indirect, 1)]),
                (1, vec![(Missing, 0)])
            ]
        );
    }
    #[test]
    fn selected_edges_cross_remote_private_boundaries_without_global_ingest() {
        use EdgeKind::*;
        let mut fixture = fixture(&[&[0], &[1]]);
        let c = commit(vec![fixture.ids[2]], 3);
        let c_id = m::CommitId(c.descriptor.key.id.0);
        let d = commit(vec![c_id], 4);
        let d_id = m::CommitId(d.descriptor.key.id.0);
        fixture.graph = OverlayGraph::extend(
            fixture.graph,
            vec![
                (c.descriptor, c.bytes.into()),
                (d.descriptor, d.bytes.into()),
            ],
            &OverlayLimits::default(),
            &mut WorkBudget::default(),
            Arc::new(()),
        )
        .unwrap();
        fixture.ids.extend([c_id, d_id]);
        assert_eq!(
            snapshot(&fixture, &[1, 4]),
            vec![(4, vec![(Indirect, 1)]), (1, vec![(Missing, 0)])]
        );
        assert!(fixture._index.node("fixture", d_id).unwrap().is_none());
    }
    #[test]
    fn aggregate_caps_error_instead_of_truncated_or_missing_edges() {
        let fixture = fixture(&[&[0], &[0], &[0], &[1, 2], &[2, 3], &[4, 5]]);
        for limits in [
            GraphLimits {
                max_edges: 1,
                ..Default::default()
            },
            GraphLimits {
                max_nodes: 1,
                ..Default::default()
            },
            GraphLimits {
                max_keys: 1,
                ..Default::default()
            },
            GraphLimits {
                max_frames: 1,
                ..Default::default()
            },
            GraphLimits {
                max_steps: 2,
                ..Default::default()
            },
        ] {
            let mut evaluator = evaluator(
                fixture.graph.clone(),
                &[
                    fixture.ids[6],
                    fixture.ids[3],
                    fixture.ids[2],
                    fixture.ids[1],
                ],
                limits,
            );
            let mut budget = WorkBudget::default();
            let mut failed = false;
            for _ in 0..100_000 {
                match evaluator.step(&mut budget) {
                    Err(EvalError::Graph(Error::BudgetExceeded)) => {
                        failed = true;
                        break;
                    }
                    Err(error) => panic!("unexpected cap error: {error}"),
                    Ok(GraphStep::Done) => break,
                    Ok(_) => {}
                }
            }
            assert!(
                failed,
                "cap must fail explicitly, not return a shortened graph"
            );
            assert!(matches!(
                evaluator.step(&mut budget),
                Err(EvalError::Graph(Error::Unavailable))
            ));
        }
    }
    #[test]
    fn escaped_rows_keep_accounting_and_foreign_evaluator_is_rejected() {
        let fixture = fixture(&[]);
        let mut evaluator = evaluator(
            fixture.graph.clone(),
            &[m::CommitId::ROOT],
            Default::default(),
        );
        let memory = evaluator.memory.clone();
        let mut budget = WorkBudget::default();
        let row = loop {
            if let GraphStep::Row(row) = evaluator.step(&mut budget).unwrap() {
                break row;
            }
        };
        assert!(memory.lock().unwrap().used.nodes > 0);
        drop(evaluator);
        assert!(
            memory.lock().unwrap().used.nodes > 0,
            "escaped node still owns graph allocation accounting"
        );
        drop(row);
        assert_eq!(memory.lock().unwrap().used.sum(), Some(0));
        let other = OverlayGraph::remote(fixture.graph.remote_scope().clone());
        let expression = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: vec![],
            })),
        };
        let input = Evaluator::new(fixture.graph.clone(), expression, Default::default()).unwrap();
        assert!(matches!(
            GraphEvaluator::new(other, input, Default::default()),
            Err(EvalError::Invalid(_))
        ));
    }
}
