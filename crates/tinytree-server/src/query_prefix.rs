//! Exact conditional prefixes over an immutable logical scope. Lexical seeks
//! produce bounded candidates. Supported predicates compile once into metered
//! point tests; other shapes use incremental membership evaluation. No separate
//! selected-history answer set is materialized. All probes
//! (including shortest-prefix probes) share one memory, CPU and metadata budget.
use crate::{
    query_cursor::{CursorHost, limits},
    query_eval::{EvalError, EvalLimits, EvalStep, Evaluator},
    query_index::{QueryConfig, Scoped, index_error},
    query_overlay::{GraphNode, OverlayGraph},
};
use std::sync::Arc;
use tinytree_index::{
    ChangePrefixCursor, ChangeTargets, CommitPrefixCursor, PrefixResolution, WorkBudget,
};
use tinytree_model as m;
use tinytree_proto::query_v1 as q;
use tokio::{sync::OwnedSemaphorePermit, time::Instant};
use tonic::Status;
type Result<T> = std::result::Result<T, Status>;
fn exhausted() -> Status {
    Status::resource_exhausted("selected prefix work budget exhausted")
}
fn eval_error(error: EvalError) -> Status {
    match error {
        EvalError::Graph(e) => index_error(e),
        EvalError::Invalid(m) => Status::invalid_argument(m),
        EvalError::Unsupported(m) => Status::unimplemented(m),
    }
}

pub(crate) struct PrefixSelection {
    graph: Arc<OverlayGraph>,
    expression: q::GraphExpr,
    policy: EvalLimits,
    compile_cost: u64,
    remaining: u64,
    max_results: usize,
    deadline: Instant,
    validated: Option<bool>,
    point: Option<PointPlan>,
    // Heavy AST/evaluator buffers are destroyed before releasing admission.
    _memory: OwnedSemaphorePermit,
}
impl PrefixSelection {
    pub fn new(
        scope: &Scoped,
        expression: &q::GraphExpr,
        config: &QueryConfig,
        cursors: &CursorHost,
        deadline: Instant,
    ) -> Result<Self> {
        Self::from_graph(scope.graph().clone(), expression, config, cursors, deadline)
    }
    fn from_graph(
        graph: Arc<OverlayGraph>,
        expression: &q::GraphExpr,
        config: &QueryConfig,
        cursors: &CursorHost,
        deadline: Instant,
    ) -> Result<Self> {
        // Reserve before cloning or compiling the potentially recursive input.
        let extra = config
            .max_result_ids
            .checked_mul(128)
            .and_then(|n| n.checked_add(16 * 1024))
            .ok_or_else(exhausted)?;
        let memory = cursors.reserve_ephemeral(config, extra)?;
        let policy = limits(config);
        let mut counts = (0usize, 0usize);
        check_shape(expression, 1, &policy, &mut counts)?;
        if counts.1 > config.max_input_ids {
            return Err(exhausted());
        }
        let compile_cost = counts
            .0
            .checked_add(counts.1)
            .and_then(|n| n.checked_add(1))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(exhausted)?;
        let mut selection = Self {
            graph,
            expression: expression.clone(),
            policy,
            compile_cost,
            remaining: config.max_eval_steps as u64,
            max_results: config.max_result_ids,
            deadline,
            validated: None,
            point: None,
            _memory: memory,
        };
        selection.tick(compile_cost)?;
        // Compile the WHOLE AST before testing a target's presence. Invalid or
        // unsupported branches must never be disguised as an absent prefix.
        let evaluator = Evaluator::new(
            selection.graph.clone(),
            selection.expression.clone(),
            selection.policy.clone(),
        )
        .map_err(eval_error)?;
        drop(evaluator);
        // A conservative bound covers repeated literal-set folding/sorting
        // over the bounded AST. An optional optimization never starts when its
        // own compilation cannot fit the remaining CPU allowance.
        let point_cost = (counts.0 as u64 + counts.1 as u64)
            .checked_mul(counts.0 as u64)
            .and_then(|n| n.checked_mul(1 + counts.1.max(1).ilog2() as u64))
            .ok_or_else(exhausted)?;
        if point_cost < selection.remaining {
            selection.tick(point_cost)?;
            selection.point = PointPlan::compile(&selection.expression);
        }
        Ok(selection)
    }
    fn tick(&mut self, steps: u64) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(Status::deadline_exceeded(
                "selected prefix deadline exceeded",
            ));
        }
        self.remaining = self.remaining.checked_sub(steps).ok_or_else(exhausted)?;
        Ok(())
    }
    fn validate_selection(&mut self, budget: &mut WorkBudget) -> Result<bool> {
        if let Some(nonempty) = self.validated {
            self.tick(0)?;
            return Ok(nonempty);
        }
        self.tick(self.compile_cost)?;
        if self.remaining == 0 {
            return Err(exhausted());
        }
        let mut policy = self.policy.clone();
        policy.max_steps = self.remaining;
        policy.max_frontier = policy.max_frontier.min(budget.max_frontier.max(1));
        let mut evaluator = Evaluator::new(self.graph.clone(), self.expression.clone(), policy)
            .map_err(eval_error)?;
        let mut previous = 0;
        let nonempty = loop {
            self.tick(0)?;
            let result = evaluator.step(budget);
            let steps = evaluator.steps();
            self.tick(steps.saturating_sub(previous))?;
            previous = steps;
            match result.map_err(eval_error)? {
                EvalStep::Progress => {}
                EvalStep::Row(row) => {
                    drop(row);
                    break true;
                }
                EvalStep::Done => break false,
            }
        };
        // Current operators initialize required literal inputs before their
        // first result. Static empty branches deliberately skip input I/O.
        // A future lazy-concatenation operator must revisit this invariant.
        // Release all preflight frontier before constructing membership state.
        drop(evaluator);
        self.validated = Some(nonempty);
        Ok(nonempty)
    }
    fn selected(
        &mut self,
        candidate: m::CommitId,
        budget: &mut WorkBudget,
        held: usize,
    ) -> Result<bool> {
        with_frontier(
            budget,
            held.checked_add(5).ok_or_else(exhausted)?,
            |budget| self.selected_inner(candidate, budget),
        )
    }
    fn selected_inner(&mut self, candidate: m::CommitId, budget: &mut WorkBudget) -> Result<bool> {
        if let Some(plan) = &self.point {
            // Point primitives use the same scratch reservation as evaluator
            // fallback, not the potentially larger outer request frontier.
            // Include the initial scoped presence proof in this bound too.
            let excess = budget.max_frontier.saturating_sub(self.policy.max_frontier);
            return with_frontier(budget, excess, |budget| {
                let mut meter = PointMeter {
                    remaining: &mut self.remaining,
                    deadline: self.deadline,
                };
                if !meter.graph(budget, |budget| self.graph.has_id(candidate, budget))? {
                    return Ok(false);
                }
                plan.contains(&self.graph, candidate, budget, &mut meter)
            });
        }
        self.tick(self.compile_cost)?;
        if self.remaining == 0 {
            return Err(exhausted());
        }
        let mut policy = self.policy.clone();
        policy.max_steps = self.remaining;
        policy.max_frontier = policy.max_frontier.min(budget.max_frontier);
        if policy.max_frontier == 0 {
            return Err(exhausted());
        }
        let mut evaluator = Evaluator::membership(
            self.graph.clone(),
            self.expression.clone(),
            candidate,
            policy,
        )
        .map_err(eval_error)?;
        if !self.graph.has_id(candidate, budget).map_err(index_error)? {
            return Ok(false);
        }
        let mut previous = 0;
        loop {
            self.tick(0)?;
            let result = evaluator.step(budget);
            let steps = evaluator.steps();
            self.tick(steps.saturating_sub(previous))?;
            previous = steps;
            match result.map_err(eval_error)? {
                EvalStep::Progress => {}
                EvalStep::Done => return Ok(false),
                EvalStep::Row(row) if row.commit() == candidate => return Ok(true),
                EvalStep::Row(_) => {
                    return Err(Status::data_loss("membership returned another candidate"));
                }
            }
        }
    }
    fn next(
        &mut self,
        scan: &mut Candidates,
        budget: &mut WorkBudget,
        held: usize,
    ) -> Result<Option<GraphNode>> {
        with_frontier(
            budget,
            held.checked_add(4).ok_or_else(exhausted)?,
            |budget| self.next_inner(scan, budget),
        )
    }
    fn next_inner(
        &mut self,
        scan: &mut Candidates,
        budget: &mut WorkBudget,
    ) -> Result<Option<GraphNode>> {
        loop {
            self.tick(1)?;
            if !scan.remote_done {
                // One scanned row (including out-of-scope progress), not one
                // eligible result. The pager releases its lock before return.
                let node = if scan.change {
                    let page = self
                        .graph
                        .remote_scope()
                        .change_prefix_candidates(
                            &scan.prefix,
                            scan.change_cursor.as_ref(),
                            1,
                            budget,
                        )
                        .map_err(index_error)?;
                    scan.remote_done = page.complete;
                    scan.change_cursor = page.cursor;
                    page.nodes.into_iter().next()
                } else {
                    let page = self
                        .graph
                        .remote_scope()
                        .commit_prefix_candidates(
                            &scan.prefix,
                            scan.commit_cursor.as_ref(),
                            1,
                            budget,
                        )
                        .map_err(index_error)?;
                    scan.remote_done = page.complete;
                    scan.commit_cursor = page.cursor;
                    page.nodes.into_iter().next()
                };
                if let Some(node) = node {
                    return self
                        .graph
                        .lookup_node(node.commit(), budget)
                        .map_err(index_error)?
                        .map(Some)
                        .ok_or_else(|| Status::data_loss("scoped lexical candidate vanished"));
                }
                continue;
            }
            let node = if scan.change {
                self.graph
                    .next_private_change(&scan.prefix, scan.after_change, budget)
            } else {
                self.graph
                    .next_private_commit(&scan.prefix, scan.after_commit, budget)
            }
            .map_err(index_error)?;
            if let Some(node) = &node {
                scan.after_commit = Some(node.commit());
                scan.after_change = Some((node.change_id(), node.commit()));
            }
            return Ok(node);
        }
    }
    pub fn resolve_commit(
        &mut self,
        prefix: &str,
        budget: &mut WorkBudget,
    ) -> Result<PrefixResolution<m::CommitId>> {
        let mut scan = Candidates::new(prefix, false)?;
        if !self.validate_selection(budget)? {
            return Ok(PrefixResolution::NoMatch);
        }
        let mut found = None;
        while let Some(node) = self.next(&mut scan, budget, usize::from(found.is_some()))? {
            if self.selected(node.commit(), budget, usize::from(found.is_some()))? {
                if found.is_some_and(|id| id != node.commit()) {
                    return Ok(PrefixResolution::Ambiguous);
                }
                found = Some(node.commit());
            }
        }
        Ok(found
            .map(PrefixResolution::Single)
            .unwrap_or(PrefixResolution::NoMatch))
    }
    pub fn resolve_change(
        &mut self,
        prefix: &str,
        budget: &mut WorkBudget,
    ) -> Result<PrefixResolution<ChangeTargets>> {
        let mut scan = Candidates::new(prefix, true)?;
        if !self.validate_selection(budget)? {
            return Ok(PrefixResolution::NoMatch);
        }
        let mut found: Option<ChangeTargets> = None;
        loop {
            let held = found.as_ref().map_or(0, |targets| targets.commits.len());
            let Some(node) = self.next(&mut scan, budget, held)? else {
                break;
            };
            if !self.selected(node.commit(), budget, held)? {
                continue;
            }
            if let Some(old) = &found {
                if old.change_id != node.change_id() {
                    return Ok(PrefixResolution::Ambiguous);
                }
            }
            let value = found.get_or_insert_with(|| ChangeTargets {
                change_id: node.change_id(),
                commits: Vec::new(),
            });
            if value.commits.len() >= self.max_results {
                return Err(exhausted());
            }
            budget
                .frontier(value.commits.len().checked_add(5).ok_or_else(exhausted)?)
                .map_err(index_error)?;
            budget.charge(0, 0, 32).map_err(index_error)?;
            value.commits.push(node.commit());
        }
        if let Some(mut targets) = found {
            let count = targets.commits.len() as u64;
            self.tick(
                count
                    .checked_mul(1 + count.max(1).ilog2() as u64)
                    .ok_or_else(exhausted)?,
            )?;
            targets.commits.sort_unstable();
            targets.commits.dedup();
            Ok(PrefixResolution::Single(targets))
        } else {
            Ok(PrefixResolution::NoMatch)
        }
    }
    pub fn shortest_commit(
        &mut self,
        id: m::CommitId,
        budget: &mut WorkBudget,
    ) -> Result<Option<usize>> {
        if !self.validate_selection(budget)? || !self.selected(id, budget, 0)? {
            return Ok(None);
        }
        let text = hex(&id.0);
        for length in 0..=64 {
            if matches!(self.resolve_commit(&text[..length], budget)?, PrefixResolution::Single(found) if found == id)
            {
                return Ok(Some(length));
            }
        }
        Err(Status::data_loss("selected commit has no exact prefix"))
    }
    pub fn shortest_change(
        &mut self,
        id: m::ChangeId,
        budget: &mut WorkBudget,
    ) -> Result<Option<usize>> {
        let text = hex(&id.0);
        if matches!(
            self.resolve_change(&text, budget)?,
            PrefixResolution::NoMatch
        ) {
            return Ok(None);
        }
        for length in 0..=32 {
            if matches!(self.resolve_change(&text[..length], budget)?, PrefixResolution::Single(found) if found.change_id == id)
            {
                return Ok(Some(length));
            }
        }
        Err(Status::data_loss("selected change has no exact prefix"))
    }
}
/// Optional point predicates. The original compiler and original first-result
/// validation remain authoritative; this plan changes membership execution only.
/// Unsupported shapes use the ordinary incremental evaluator without fallback
/// to canonical history. All vectors are bounded by the admitted AST literals.
enum PointPlan {
    Literal(Vec<m::CommitId>),
    Ancestors(Vec<m::CommitId>),
    Range {
        roots: Vec<m::CommitId>,
        heads: Vec<m::CommitId>,
    },
    Union(Vec<Self>),
    Intersection(Vec<Self>),
    Difference(Box<Self>, Box<Self>),
    HasConflict(Box<Self>),
}
impl PointPlan {
    fn compile(expr: &q::GraphExpr) -> Option<Self> {
        use q::graph_expr::Expression as X;
        if let Some(ids) = literal_set(expr) {
            return Some(Self::Literal(ids));
        }
        match expr.expression.as_ref()? {
            X::Ancestors(value) => {
                if value
                    .generations
                    .as_ref()
                    .is_some_and(|w| w.end_exclusive == Some(w.start))
                {
                    return Some(Self::Literal(Vec::new()));
                }
                let heads = value.heads.as_deref()?;
                if all_generations(value.generations.as_ref())
                    && all_parents(value.parents.as_ref())
                {
                    return Some(Self::Ancestors(literal_set(heads)?));
                }
                // A(Range(ROOT,H), 0..k), k>=2, is A(non-root H):
                // Range retains every non-root ancestor of H, and one parent
                // step fills its sole excluded boundary ROOT. For ROOT-only H
                // the range is empty. Preserve the ORIGINAL data preflight, so
                // this algebra never suppresses required unknown-ID errors.
                if value
                    .generations
                    .as_ref()
                    .is_some_and(|w| w.start == 0 && w.end_exclusive.is_none_or(|end| end >= 2))
                    && all_parents(value.parents.as_ref())
                {
                    if let Some(X::Range(range)) = heads.expression.as_ref() {
                        if all_generations(range.generations.as_ref())
                            && all_parents(range.parents.as_ref())
                        {
                            let roots = literal_set(range.roots.as_deref()?)?;
                            if roots == [m::CommitId::ROOT] {
                                let mut heads = literal_set(range.heads.as_deref()?)?;
                                heads.retain(|head| *head != m::CommitId::ROOT);
                                return Some(Self::Ancestors(heads));
                            }
                        }
                    }
                }
                None
            }
            X::Range(value)
                if all_generations(value.generations.as_ref())
                    && all_parents(value.parents.as_ref()) =>
            {
                Some(Self::Range {
                    roots: literal_set(value.roots.as_deref()?)?,
                    heads: literal_set(value.heads.as_deref()?)?,
                })
            }
            X::Union(value) => Some(Self::Union(
                value
                    .inputs
                    .iter()
                    .map(Self::compile)
                    .collect::<Option<Vec<_>>>()?,
            )),
            X::Intersection(value) => Some(Self::Intersection(
                value
                    .inputs
                    .iter()
                    .map(Self::compile)
                    .collect::<Option<Vec<_>>>()?,
            )),
            X::Difference(value) => Some(Self::Difference(
                Box::new(Self::compile(value.left.as_deref()?)?),
                Box::new(Self::compile(value.right.as_deref()?)?),
            )),
            X::HasConflict(value) => Some(Self::HasConflict(Box::new(Self::compile(
                value.input.as_deref()?,
            )?))),
            _ => None,
        }
    }
    fn contains(
        &self,
        graph: &Arc<OverlayGraph>,
        candidate: m::CommitId,
        budget: &mut WorkBudget,
        meter: &mut PointMeter<'_>,
    ) -> Result<bool> {
        meter.tick(1)?;
        match self {
            Self::Literal(ids) => {
                meter.tick(1 + ids.len().max(1).ilog2() as u64)?;
                Ok(ids.binary_search(&candidate).is_ok())
            }
            Self::Ancestors(heads) => reaches_any(graph, candidate, heads, budget, meter),
            Self::Range { roots, heads } => {
                Ok(reaches_any(graph, candidate, heads, budget, meter)?
                    && !reaches_any(graph, candidate, roots, budget, meter)?)
            }
            Self::Union(inputs) => {
                for input in inputs {
                    if input.contains(graph, candidate, budget, meter)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Self::Intersection(inputs) => {
                for input in inputs {
                    if !input.contains(graph, candidate, budget, meter)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Self::Difference(left, right) => Ok(left.contains(graph, candidate, budget, meter)?
                && !right.contains(graph, candidate, budget, meter)?),
            Self::HasConflict(input) => {
                if !input.contains(graph, candidate, budget, meter)? {
                    return Ok(false);
                }
                Ok(meter
                    .graph(budget, |budget| graph.lookup_node(candidate, budget))?
                    .is_some_and(|node| node.has_conflict()))
            }
        }
    }
}
fn all_generations(window: Option<&q::GenerationRange>) -> bool {
    window.is_none_or(|window| window.start == 0 && window.end_exclusive.is_none())
}
fn all_parents(window: Option<&q::ParentRange>) -> bool {
    window.is_none_or(|window| window.start == 0 && window.end_exclusive.is_none())
}
/// Fold literal unions and a singleton Latest (timestamp/order cannot change a
/// set with <=1 member). Latest(0) is statically empty and skips input data I/O.
fn literal_set(expr: &q::GraphExpr) -> Option<Vec<m::CommitId>> {
    use q::graph_expr::Expression as X;
    let mut ids = match expr.expression.as_ref()? {
        X::Commits(value) => value
            .commits
            .iter()
            .map(|id| m::CommitId::from_slice(&id.value).ok())
            .collect::<Option<Vec<_>>>()?,
        X::Union(value) => {
            let mut ids = Vec::new();
            for input in &value.inputs {
                ids.extend(literal_set(input)?);
            }
            ids
        }
        X::Latest(value) if value.count == 0 => Vec::new(),
        X::Latest(value) => {
            let ids = literal_set(value.candidates.as_deref()?)?;
            if ids.len() > 1 {
                return None;
            }
            ids
        }
        _ => return None,
    };
    ids.sort_unstable();
    ids.dedup();
    Some(ids)
}
fn reaches_any(
    graph: &Arc<OverlayGraph>,
    candidate: m::CommitId,
    heads: &[m::CommitId],
    budget: &mut WorkBudget,
    meter: &mut PointMeter<'_>,
) -> Result<bool> {
    for head in heads {
        meter.tick(1)?;
        if meter.graph(budget, |budget| graph.is_ancestor(candidate, *head, budget))? {
            return Ok(true);
        }
    }
    Ok(false)
}
struct PointMeter<'a> {
    remaining: &'a mut u64,
    deadline: Instant,
}
impl PointMeter<'_> {
    fn tick(&mut self, steps: u64) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(Status::deadline_exceeded(
                "selected prefix deadline exceeded",
            ));
        }
        *self.remaining = self.remaining.checked_sub(steps).ok_or_else(exhausted)?;
        Ok(())
    }
    fn graph<T>(
        &mut self,
        budget: &mut WorkBudget,
        call: impl FnOnce(&mut WorkBudget) -> tinytree_index::Result<T>,
    ) -> Result<T> {
        self.tick(1)?;
        let before = (budget.nodes_read(), budget.edges_read());
        let result = call(budget);
        self.tick(
            budget
                .nodes_read()
                .saturating_sub(before.0)
                .checked_add(budget.edges_read().saturating_sub(before.1))
                .ok_or_else(exhausted)?,
        )?;
        result.map_err(index_error)
    }
}

// Temporarily reserve retained results and lexical/candidate state from the
// SAME logical frontier used by nested pager membership and evaluator scratch.
// Restoring on every Result path preserves the caller's policy, not its usage.
fn with_frontier<T>(
    budget: &mut WorkBudget,
    held: usize,
    f: impl FnOnce(&mut WorkBudget) -> Result<T>,
) -> Result<T> {
    let original = budget.max_frontier;
    budget.max_frontier = original.checked_sub(held).ok_or_else(exhausted)?;
    let result = f(budget);
    budget.max_frontier = original;
    result
}
struct Candidates {
    prefix: String,
    change: bool,
    commit_cursor: Option<CommitPrefixCursor>,
    change_cursor: Option<ChangePrefixCursor>,
    remote_done: bool,
    after_commit: Option<m::CommitId>,
    after_change: Option<(m::ChangeId, m::CommitId)>,
}
impl Candidates {
    fn new(prefix: &str, change: bool) -> Result<Self> {
        if prefix.len() > if change { 32 } else { 64 }
            || !prefix
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Status::invalid_argument(
                "invalid lowercase hexadecimal prefix",
            ));
        }
        Ok(Self {
            prefix: prefix.into(),
            change,
            commit_cursor: None,
            change_cursor: None,
            remote_done: false,
            after_commit: None,
            after_change: None,
        })
    }
}
fn hex(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(H[(b >> 4) as usize] as char);
        s.push(H[(b & 15) as usize] as char);
    }
    s
}
// Borrowed bounded preflight prevents recursive clone or repeated-literal
// allocations before the evaluator has checked its own semantic policy.
fn check_shape(
    expr: &q::GraphExpr,
    depth: usize,
    limits: &EvalLimits,
    counts: &mut (usize, usize),
) -> Result<()> {
    counts.0 = counts.0.checked_add(1).ok_or_else(exhausted)?;
    if depth > limits.max_ast_depth || counts.0 > limits.max_ast_nodes {
        return Err(exhausted());
    }
    use q::graph_expr::Expression as X;
    let mut visit = |child: &q::GraphExpr| check_shape(child, depth + 1, limits, counts);
    match expr
        .expression
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing expression"))?
    {
        X::Commits(v) => {
            counts.1 = counts
                .1
                .checked_add(v.commits.len())
                .ok_or_else(exhausted)?;
            if counts.1 > limits.max_literals || v.commits.iter().any(|id| id.value.len() != 32) {
                return Err(Status::invalid_argument("invalid selection literal IDs"));
            }
        }
        X::Ancestors(v) => {
            if let Some(c) = &v.heads {
                visit(c)?;
            }
        }
        X::Range(v) => {
            if let Some(c) = &v.roots {
                visit(c)?;
            }
            if let Some(c) = &v.heads {
                visit(c)?;
            }
        }
        X::DagRange(v) => {
            if let Some(c) = &v.roots {
                visit(c)?;
            }
            if let Some(c) = &v.heads {
                visit(c)?;
            }
        }
        X::Heads(v) | X::Roots(v) | X::HasConflict(v) => {
            if let Some(c) = &v.input {
                visit(c)?;
            }
        }
        X::Union(v) | X::Intersection(v) => {
            for c in &v.inputs {
                visit(c)?;
            }
        }
        X::Difference(v) => {
            if let Some(c) = &v.left {
                visit(c)?;
            }
            if let Some(c) = &v.right {
                visit(c)?;
            }
        }
        X::Latest(v) => {
            if let Some(c) = &v.candidates {
                visit(c)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_overlay::OverlayLimits;
    use tinytree_index::Index;
    fn canonical(parents: &[m::CommitId], change: u8, label: &str) -> (m::Descriptor, Arc<[u8]>) {
        let mut commit = m::synthetic_root();
        commit.parents = parents.to_vec();
        commit.change_id = m::ChangeId([change; 16]);
        commit.description = label.into();
        let encoded = m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap();
        (encoded.descriptor, encoded.bytes.into())
    }
    fn selected(ids: &[m::CommitId]) -> q::GraphExpr {
        q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: ids
                    .iter()
                    .map(|id| q::CommitId {
                        value: id.0.to_vec(),
                    })
                    .collect(),
            })),
        }
    }
    fn fixture() -> (tempfile::TempDir, Arc<OverlayGraph>, [m::CommitId; 3]) {
        let directory = tempfile::tempdir().unwrap();
        let index = Index::open(
            directory.path().join("index"),
            &tinytree_index::Limits::default(),
        )
        .unwrap();
        let (descriptor, bytes) = canonical(&[m::CommitId::ROOT], 0x31, "public anchor");
        let base = index.ingest("repo", descriptor, &bytes).unwrap().commit;
        // Physical index row deliberately outside the frozen scope.
        let (descriptor, bytes) = canonical(&[m::CommitId::ROOT], 0x11, "outside scope");
        index.ingest("repo", descriptor, &bytes).unwrap();
        let remote = OverlayGraph::remote(Arc::new(index.scope("repo", &[base]).unwrap()));
        let first = canonical(&[base], 0x11, "private branch");
        let second = canonical(&[base], 0x11, "hidden divergent branch");
        let ids = [
            base,
            m::CommitId(first.0.key.id.0),
            m::CommitId(second.0.key.id.0),
        ];
        let graph = OverlayGraph::extend(
            remote,
            vec![second, first],
            &OverlayLimits::default(),
            &mut WorkBudget::default(),
            Arc::new(()),
        )
        .unwrap();
        (directory, graph, ids)
    }
    fn run(
        graph: Arc<OverlayGraph>,
        expr: &q::GraphExpr,
        config: &QueryConfig,
        host: &CursorHost,
    ) -> PrefixSelection {
        PrefixSelection::from_graph(
            graph,
            expr,
            config,
            host,
            Instant::now() + std::time::Duration::from_secs(10),
        )
        .unwrap()
    }
    #[test]
    fn conditional_prefixes_restrict_hidden_divergent_targets_and_allow_zero_length() {
        let (_directory, graph, ids) = fixture();
        let config = QueryConfig::default();
        let host = CursorHost::new(&config).unwrap();
        let mut singleton = run(graph.clone(), &selected(&[ids[1]]), &config, &host);
        assert_eq!(
            singleton
                .resolve_commit("", &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::Single(ids[1])
        );
        assert_eq!(
            singleton
                .shortest_commit(ids[1], &mut WorkBudget::default())
                .unwrap(),
            Some(0)
        );
        assert_eq!(
            singleton
                .shortest_commit(ids[2], &mut WorkBudget::default())
                .unwrap(),
            None
        );
        assert_eq!(
            singleton
                .shortest_change(m::ChangeId([0x11; 16]), &mut WorkBudget::default())
                .unwrap(),
            Some(0)
        );
        assert_eq!(
            singleton
                .shortest_change(m::ChangeId([0x31; 16]), &mut WorkBudget::default())
                .unwrap(),
            None
        );
        assert_eq!(
            singleton
                .resolve_change("1", &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::Single(ChangeTargets {
                change_id: m::ChangeId([0x11; 16]),
                commits: vec![ids[1]]
            })
        );
        drop(singleton);
        let mut divergent = run(graph.clone(), &selected(&ids[1..]), &config, &host);
        let mut expected = ids[1..].to_vec();
        expected.sort_unstable();
        assert_eq!(
            divergent
                .resolve_change("1", &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::Single(ChangeTargets {
                change_id: m::ChangeId([0x11; 16]),
                commits: expected
            })
        );
        assert_eq!(
            divergent
                .resolve_commit("", &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::Ambiguous
        );
        drop(divergent);
        let mut root = run(graph, &selected(&[m::CommitId::ROOT]), &config, &host);
        assert_eq!(
            root.shortest_commit(m::CommitId::ROOT, &mut WorkBudget::default())
                .unwrap(),
            Some(0)
        );
        assert_eq!(
            root.resolve_change("0", &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::Single(ChangeTargets {
                change_id: m::ChangeId([0; 16]),
                commits: vec![m::CommitId::ROOT]
            })
        );
    }
    #[test]
    fn private_lexical_seek_handles_odd_prefixes_and_multiple_layers() {
        let (_directory, graph, ids) = fixture();
        let third = canonical(&[ids[1]], 0x12, "next layer");
        let third_id = m::CommitId(third.0.key.id.0);
        let graph = OverlayGraph::extend(
            graph,
            vec![third],
            &OverlayLimits::default(),
            &mut WorkBudget::default(),
            Arc::new(()),
        )
        .unwrap();
        let config = QueryConfig::default();
        let host = CursorHost::new(&config).unwrap();
        let mut selection = run(
            graph,
            &selected(&[ids[1], ids[2], third_id]),
            &config,
            &host,
        );
        for id in [ids[1], ids[2], third_id] {
            let full = hex(&id.0);
            assert_eq!(
                selection
                    .resolve_commit(&full[..63], &mut WorkBudget::default())
                    .unwrap(),
                PrefixResolution::Single(id)
            );
        }
        assert_eq!(
            selection
                .resolve_change("1", &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::Ambiguous
        );
        assert_eq!(
            selection
                .resolve_change("12", &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::Single(ChangeTargets {
                change_id: m::ChangeId([0x12; 16]),
                commits: vec![third_id]
            })
        );
    }
    #[test]
    fn invalid_ast_and_shared_work_exhaustion_never_become_absence() {
        let (_directory, graph, ids) = fixture();
        let config = QueryConfig::default();
        let host = CursorHost::new(&config).unwrap();
        let invalid = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Difference(Box::new(
                q::DifferenceExpr {
                    left: Some(Box::new(selected(&[]))),
                    right: None,
                },
            ))),
        };
        assert_eq!(
            PrefixSelection::from_graph(
                graph.clone(),
                &invalid,
                &config,
                &host,
                Instant::now() + std::time::Duration::from_secs(10)
            )
            .err()
            .unwrap()
            .code(),
            tonic::Code::InvalidArgument
        );
        let mut config = config;
        config.max_eval_steps = 10;
        let mut selection = run(graph.clone(), &selected(&[ids[1]]), &config, &host);
        assert_eq!(
            selection
                .resolve_commit("", &mut WorkBudget::default())
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        drop(selection);
        let mut selection = run(graph, &selected(&[ids[1]]), &QueryConfig::default(), &host);
        let mut budget = WorkBudget::default();
        budget.max_nodes = 0;
        assert_eq!(
            selection
                .resolve_change("1", &mut budget)
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
    }
    #[test]
    fn required_literals_validate_before_negative_answers_but_zero_limits_skip_io() {
        let (_directory, graph, ids) = fixture();
        let config = QueryConfig::default();
        let host = CursorHost::new(&config).unwrap();
        let unknown = m::CommitId([0x99; 32]);
        let unknown_expr = selected(&[unknown]);
        let union = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Union(
                q::SetExpr {
                    inputs: vec![selected(&[ids[0]]), unknown_expr.clone()],
                }
                .into(),
            )),
        };
        for expression in [&unknown_expr, &union] {
            let mut prefix = run(graph.clone(), expression, &config, &host);
            assert_eq!(
                prefix
                    .resolve_commit(&hex(&unknown.0), &mut WorkBudget::default())
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
            drop(prefix);
            let mut shortest = run(graph.clone(), expression, &config, &host);
            assert_eq!(
                shortest
                    .shortest_commit(unknown, &mut WorkBudget::default())
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
        let empty_latest = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Latest(
                q::LatestExpr {
                    candidates: Some(Box::new(unknown_expr.clone())),
                    count: 0,
                }
                .into(),
            )),
        };
        let empty_window = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Ancestors(
                q::AncestorsExpr {
                    heads: Some(Box::new(unknown_expr)),
                    generations: Some(q::GenerationRange {
                        start: 0,
                        end_exclusive: Some(0),
                    }),
                    parents: None,
                }
                .into(),
            )),
        };
        for expression in [&empty_latest, &empty_window] {
            let mut empty = run(graph.clone(), expression, &config, &host);
            let mut budget = WorkBudget::new(0, 0, 0, 0);
            assert_eq!(
                empty.resolve_commit("", &mut budget).unwrap(),
                PrefixResolution::NoMatch
            );
            assert_eq!(
                empty.resolve_change("", &mut budget).unwrap(),
                PrefixResolution::NoMatch
            );
            assert_eq!(empty.shortest_commit(unknown, &mut budget).unwrap(), None);
            assert_eq!(
                empty
                    .shortest_change(m::ChangeId([0x99; 16]), &mut budget)
                    .unwrap(),
                None
            );
            assert_eq!(
                (
                    budget.nodes_read(),
                    budget.edges_read(),
                    budget.bytes_read()
                ),
                (0, 0, 0)
            );
            assert_eq!(budget.max_frontier, 0);
        }
    }
    #[test]
    fn retained_results_share_frontier_with_scans_and_membership() {
        let (_directory, graph, ids) = fixture();
        let config = QueryConfig::default();
        let host = CursorHost::new(&config).unwrap();
        let mut selection = run(graph, &selected(&ids[1..]), &config, &host);
        let mut budget = WorkBudget::default();
        budget.max_frontier = 5;
        assert_eq!(
            selection
                .resolve_change("1", &mut budget)
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        // Even an error restores caller policy; aggregate retained slots are
        // deducted only for the nested operation, not permanently leaked.
        assert_eq!(budget.max_frontier, 5);
    }
    fn ancestors_expr(heads: q::GraphExpr, end: Option<u64>) -> q::GraphExpr {
        q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Ancestors(
                q::AncestorsExpr {
                    heads: Some(Box::new(heads)),
                    generations: end.map(|end| q::GenerationRange {
                        start: 0,
                        end_exclusive: Some(end),
                    }),
                    parents: None,
                }
                .into(),
            )),
        }
    }
    fn range_expr(roots: q::GraphExpr, heads: q::GraphExpr) -> q::GraphExpr {
        q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Range(
                q::RangeExpr {
                    roots: Some(Box::new(roots)),
                    heads: Some(Box::new(heads)),
                    generations: None,
                    parents: None,
                }
                .into(),
            )),
        }
    }
    fn union_expr(inputs: Vec<q::GraphExpr>) -> q::GraphExpr {
        q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Union(
                q::SetExpr { inputs }.into(),
            )),
        }
    }
    fn default_expr(head: m::CommitId) -> q::GraphExpr {
        union_expr(vec![
            selected(&[head]),
            ancestors_expr(
                range_expr(selected(&[m::CommitId::ROOT]), selected(&[head])),
                Some(2),
            ),
            q::GraphExpr {
                version: 1,
                expression: Some(q::graph_expr::Expression::Latest(
                    q::LatestExpr {
                        candidates: Some(Box::new(union_expr(vec![
                            selected(&[m::CommitId::ROOT]),
                            union_expr(vec![]),
                        ]))),
                        count: 1,
                    }
                    .into(),
                )),
            },
        ])
    }
    fn original_ids(
        graph: Arc<OverlayGraph>,
        expr: q::GraphExpr,
    ) -> std::collections::BTreeSet<m::CommitId> {
        let mut evaluator = Evaluator::new(graph, expr, EvalLimits::default()).unwrap();
        let mut budget = WorkBudget::default();
        let mut ids = std::collections::BTreeSet::new();
        loop {
            match evaluator.step(&mut budget).unwrap() {
                EvalStep::Row(node) => {
                    ids.insert(node.commit());
                }
                EvalStep::Progress => {}
                EvalStep::Done => break,
            }
        }
        ids
    }
    #[test]
    fn point_plans_match_original_on_branches_merges_hidden_conflicts_and_root() {
        let (_directory, graph, ids) = fixture();
        let mut merged = m::synthetic_root();
        merged.parents = ids[1..].to_vec();
        merged.change_id = m::ChangeId([0x20; 16]);
        merged.description = "private conflicted merge".into();
        merged.root_tree.terms = vec![merged.root_tree.terms[0]; 3];
        merged.root_tree.labels = vec![String::new(); 3];
        let encoded = m::encode(&m::Object::Commit(merged), m::Limits::default()).unwrap();
        let merge = m::CommitId(encoded.descriptor.key.id.0);
        let graph = OverlayGraph::extend(
            graph,
            vec![(encoded.descriptor, encoded.bytes.into())],
            &OverlayLimits::default(),
            &mut WorkBudget::default(),
            Arc::new(()),
        )
        .unwrap();
        let ancestors = ancestors_expr(selected(&[merge]), None);
        let range = range_expr(selected(&[ids[0]]), selected(&[merge]));
        let cases = vec![
            ("literal", selected(&[ids[0], ids[2]]), true),
            ("ancestors", ancestors.clone(), true),
            ("range", range.clone(), true),
            ("default_root_boundary_fold", default_expr(merge), true),
            ("root_only_default", default_expr(m::CommitId::ROOT), true),
            (
                "conflict",
                q::GraphExpr {
                    version: 1,
                    expression: Some(q::graph_expr::Expression::HasConflict(
                        q::UnaryExpr {
                            input: Some(Box::new(ancestors.clone())),
                        }
                        .into(),
                    )),
                },
                true,
            ),
            (
                "intersection",
                q::GraphExpr {
                    version: 1,
                    expression: Some(q::graph_expr::Expression::Intersection(
                        q::SetExpr {
                            inputs: vec![ancestors.clone(), range.clone()],
                        }
                        .into(),
                    )),
                },
                true,
            ),
            (
                "difference",
                q::GraphExpr {
                    version: 1,
                    expression: Some(q::graph_expr::Expression::Difference(
                        q::DifferenceExpr {
                            left: Some(Box::new(ancestors.clone())),
                            right: Some(Box::new(range)),
                        }
                        .into(),
                    )),
                },
                true,
            ),
            (
                "finite_distance_fallback",
                ancestors_expr(selected(&[merge]), Some(2)),
                false,
            ),
            (
                "heads_fallback",
                q::GraphExpr {
                    version: 1,
                    expression: Some(q::graph_expr::Expression::Heads(
                        q::UnaryExpr {
                            input: Some(Box::new(ancestors)),
                        }
                        .into(),
                    )),
                },
                false,
            ),
        ];
        let config = QueryConfig::default();
        let host = CursorHost::new(&config).unwrap();
        for (name, expr, activated) in cases {
            let expected = original_ids(graph.clone(), expr.clone());
            let mut selection = run(graph.clone(), &expr, &config, &host);
            assert_eq!(selection.point.is_some(), activated, "{name}");
            let mut budget = WorkBudget::default();
            selection.validate_selection(&mut budget).unwrap();
            for id in [
                m::CommitId::ROOT,
                ids[0],
                ids[1],
                ids[2],
                merge,
                m::CommitId([0x99; 32]),
            ] {
                assert_eq!(
                    selection.selected(id, &mut budget, 0).unwrap(),
                    expected.contains(&id),
                    "{name}: {id:?}"
                );
            }
        }
    }
    #[test]
    fn default_point_plan_avoids_repeated_public_history_membership_walks() {
        let directory = tempfile::tempdir().unwrap();
        let index = Index::open(
            directory.path().join("index"),
            &tinytree_index::Limits::default(),
        )
        .unwrap();
        let mut head = m::CommitId::ROOT;
        let mut ids = vec![head];
        for ordinal in 0..1024 {
            let (descriptor, bytes) =
                canonical(&[head], 0x31, &format!("public history {ordinal}"));
            head = index.ingest("repo", descriptor, &bytes).unwrap().commit;
            ids.push(head);
        }
        let graph = OverlayGraph::remote(Arc::new(index.scope("repo", &[head]).unwrap()));
        let expr = default_expr(head);
        let config = QueryConfig::default();
        let host = CursorHost::new(&config).unwrap();
        let mut point = run(graph.clone(), &expr, &config, &host);
        assert!(point.point.is_some());
        let mut preflight = WorkBudget::default();
        point.validate_selection(&mut preflight).unwrap();
        let mut probes = WorkBudget::default();
        for id in [ids[1], ids[512], ids[1024]] {
            assert!(point.selected(id, &mut probes, 0).unwrap());
        }
        drop(point);
        let mut original = run(graph.clone(), &expr, &config, &host);
        original.point = None;
        original
            .validate_selection(&mut WorkBudget::default())
            .unwrap();
        let mut walks = WorkBudget::default();
        for id in [ids[1], ids[512], ids[1024]] {
            assert!(original.selected(id, &mut walks, 0).unwrap());
        }
        println!(
            "{{\"kind\":\"selected_prefix_point_work\",\"history\":1024,\"activated\":true,\"preflight_nodes\":{},\"point_nodes\":{},\"fallback_nodes\":{}}}",
            preflight.nodes_read(),
            probes.nodes_read(),
            walks.nodes_read()
        );
        assert!(probes.nodes_read() * 4 < walks.nodes_read());
        drop(original);
        // A broad request allowance cannot override the smaller evaluator
        // scratch reservation used by the point plan. The first-row preflight
        // fits, but proving an old candidate requires a longer skip frontier.
        let tight = QueryConfig {
            max_eval_frontier: 8,
            ..config
        };
        let mut bounded = run(
            graph,
            &ancestors_expr(selected(&[head]), None),
            &tight,
            &host,
        );
        assert!(
            bounded
                .validate_selection(&mut WorkBudget::default())
                .unwrap()
        );
        let mut broad_request = WorkBudget::default();
        let original_frontier = broad_request.max_frontier;
        assert_eq!(
            bounded
                .selected(ids[1], &mut broad_request, 0)
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(broad_request.max_frontier, original_frontier);
    }
}
