use super::*;
use futures::{StreamExt as _, future::LocalBoxFuture, stream::LocalBoxStream};
use jj_lib::graph::{GraphEdge, GraphNode};
use jj_lib::revset::RevsetContainingFn;
use prost::Message as _;
fn expr(expression: q::graph_expr::Expression) -> q::GraphExpr {
    q::GraphExpr {
        version: 1,
        expression: Some(expression),
    }
}
fn commits(ids: &[CommitId]) -> Result<q::GraphExpr, Error> {
    Ok(expr(q::graph_expr::Expression::Commits(q::CommitSet {
        commits: ids
            .iter()
            .map(|id| {
                Ok(q::CommitId {
                    value: model_id(id)?.0.to_vec(),
                })
            })
            .collect::<Result<_, Error>>()?,
    })))
}
/// The file predicate is deliberately a selected-client stage, never a scan of
/// the public commit closure. Other metadata predicates remain explicit errors.
pub(super) fn selected_file_all(input: &ResolvedExpression) -> Option<&ResolvedExpression> {
    use jj_lib::{
        fileset::FilesetExpression,
        revset::{ResolvedPredicateExpression as P, RevsetFilterPredicate as F},
    };
    let ResolvedExpression::FilterWithin {
        candidates,
        predicate,
    } = input
    else {
        return None;
    };
    let P::Intersection(a, b) = predicate else {
        return None;
    };
    let conflict = |v: &P| matches!(v, P::Filter(F::HasConflict));
    let all = |v: &P| matches!(v, P::Filter(F::File(FilesetExpression::All)));
    ((conflict(a) && all(b)) || (all(a) && conflict(b))).then_some(candidates.as_ref())
}

pub(super) fn compile(
    input: &ResolvedExpression,
    limits: &Limits,
    conflict: bool,
) -> Result<q::GraphExpr, Error> {
    // A signed set represents S or its complement relative to the candidates.
    // Boolean lowering moves owned children rather than duplicating candidates.
    fn predicate(
        input: &jj_lib::revset::ResolvedPredicateExpression,
        depth: usize,
        left: &mut usize,
        literals: &mut usize,
        limits: &Limits,
    ) -> Result<(q::GraphExpr, bool), Error> {
        use jj_lib::revset::ResolvedPredicateExpression as P;
        use q::graph_expr::Expression as Q;
        if depth > limits.max_expression_depth || *left == 0 {
            return Err(Error::Limit);
        }
        *left -= 1;
        match input {
            P::Set(value) => Ok((build(value, depth + 1, left, literals, limits)?, false)),
            P::NotIn(value) => {
                let (value, negative) = predicate(value, depth + 1, left, literals, limits)?;
                Ok((value, !negative))
            }
            P::Union(a, b) | P::Intersection(a, b) => {
                let (a, an) = predicate(a, depth + 1, left, literals, limits)?;
                let (b, bn) = predicate(b, depth + 1, left, literals, limits)?;
                let union = matches!(input, P::Union(..));
                let difference = |a, b| {
                    expr(Q::Difference(Box::new(q::DifferenceExpr {
                        left: Some(Box::new(a)),
                        right: Some(Box::new(b)),
                    })))
                };
                Ok(match (union, an, bn) {
                    (true, false, false) | (false, true, true) => {
                        (expr(Q::Union(q::SetExpr { inputs: vec![a, b] })), an)
                    }
                    (false, false, false) | (true, true, true) => {
                        (expr(Q::Intersection(q::SetExpr { inputs: vec![a, b] })), an)
                    }
                    (true, false, true) => (difference(b, a), true),
                    (true, true, false) => (difference(a, b), true),
                    (false, false, true) => (difference(a, b), false),
                    (false, true, false) => (difference(b, a), false),
                })
            }
            P::Filter(_) => Err(Error::UnsupportedExpression("filter_metadata_boolean")),
            P::Divergent { .. } => Err(Error::UnsupportedExpression("filter_divergent")),
        }
    }
    // A fileset union is an existential path match, so each branch filters
    // the same candidates. Intersections/differences cannot be lowered this way:
    // different changed paths in one commit need not satisfy both matchers.
    fn file(
        fileset: &jj_lib::fileset::FilesetExpression,
        candidates: &ResolvedExpression,
        depth: usize,
        left: &mut usize,
        literals: &mut usize,
        limits: &Limits,
    ) -> Result<q::GraphExpr, Error> {
        use jj_lib::fileset::{FilePattern, FilesetExpression};
        use q::graph_expr::Expression as Q;
        if depth > limits.max_expression_depth || *left == 0 {
            return Err(Error::Limit);
        }
        *left -= 1;
        fn legacy(fileset: &FilesetExpression) -> bool {
            let mut pending = vec![(fileset, 1usize)];
            let mut count = 0usize;
            while let Some((fileset, depth)) = pending.pop() {
                count += 1;
                if count > 64 || depth > 16 {
                    return false;
                }
                match fileset {
                    FilesetExpression::Pattern(FilePattern::FilePath(_)) => {}
                    FilesetExpression::Pattern(FilePattern::PrefixPath(path))
                        if !path.is_root() => {}
                    FilesetExpression::UnionAll(inputs) if !inputs.is_empty() => {
                        if inputs.len() > 64usize.saturating_sub(count + pending.len()) {
                            return false;
                        }
                        pending.extend(inputs.iter().map(|v| (v, depth + 1)));
                    }
                    _ => return false,
                }
            }
            true
        }
        if !legacy(fileset) {
            // One matcher tree, not commit-set intersection/difference: a
            // commit can change distinct paths which satisfy separate leaves.
            let matcher =
                tinytree_annotate::fileset::from_stock(fileset).map_err(|_| Error::Limit)?;
            return Ok(expr(Q::ChangedPath(Box::new(q::ChangedPathExpr {
                policy_version: 3,
                candidates: Some(Box::new(build(
                    candidates,
                    depth + 1,
                    left,
                    literals,
                    limits,
                )?)),
                path: String::new(),
                exact_file: false,
                matcher: Some(matcher),
            }))));
        }
        let (path, exact_file) = match fileset {
            FilesetExpression::UnionAll(inputs) if !inputs.is_empty() => {
                if inputs.len() > *left {
                    return Err(Error::Limit);
                }
                return Ok(expr(Q::Union(q::SetExpr {
                    inputs: inputs
                        .iter()
                        .map(|input| file(input, candidates, depth + 1, left, literals, limits))
                        .collect::<Result<_, _>>()?,
                })));
            }
            FilesetExpression::Pattern(FilePattern::FilePath(path)) => (path, true),
            FilesetExpression::Pattern(FilePattern::PrefixPath(path)) => (path, false),
            _ => return Err(Error::UnsupportedExpression("fileset_not_exact_path")),
        };
        let path = path.as_internal_file_string();
        if path.is_empty() || path.len() > 4096 || path.split('/').count() > limits.max_depth {
            return Err(Error::UnsupportedExpression("fileset_not_exact_path"));
        }
        Ok(expr(Q::ChangedPath(Box::new(q::ChangedPathExpr {
            policy_version: 2,
            candidates: Some(Box::new(build(
                candidates,
                depth + 1,
                left,
                literals,
                limits,
            )?)),
            path: path.to_owned(),
            exact_file,
            matcher: None,
        }))))
    }
    fn build(
        input: &ResolvedExpression,
        depth: usize,
        left: &mut usize,
        literals: &mut usize,
        limits: &Limits,
    ) -> Result<q::GraphExpr, Error> {
        if let ResolvedExpression::FilterWithin {
            candidates,
            predicate:
                jj_lib::revset::ResolvedPredicateExpression::Filter(
                    jj_lib::revset::RevsetFilterPredicate::File(fileset),
                ),
        } = input
        {
            return file(fileset, candidates, depth, left, literals, limits);
        }
        if depth > limits.max_expression_depth || *left == 0 {
            return Err(Error::Limit);
        }
        *left -= 1;
        let child = |v: &ResolvedExpression, left: &mut usize, literals: &mut usize| {
            build(v, depth + 1, left, literals, limits)
        };
        use ResolvedExpression as R;
        use q::graph_expr::Expression as Q;
        Ok(match input {
            R::Commits(ids) => {
                if ids.len() > *literals {
                    return Err(Error::Limit);
                }
                *literals -= ids.len();
                commits(ids)?
            }
            R::Ancestors {
                heads,
                generation,
                parents_range,
            } => expr(Q::Ancestors(Box::new(q::AncestorsExpr {
                heads: Some(Box::new(child(heads, left, literals)?)),
                generations: Some(generation_range(generation)?),
                parents: Some(parent_range(parents_range)?),
            }))),
            R::Range {
                roots,
                heads,
                generation,
                parents_range,
            } => expr(Q::Range(Box::new(q::RangeExpr {
                roots: Some(Box::new(child(roots, left, literals)?)),
                heads: Some(Box::new(child(heads, left, literals)?)),
                generations: Some(generation_range(generation)?),
                parents: Some(parent_range(parents_range)?),
            }))),
            R::DagRange {
                roots,
                heads,
                generation_from_roots,
            } => expr(Q::DagRange(Box::new(q::DagRangeExpr {
                roots: Some(Box::new(child(roots, left, literals)?)),
                heads: Some(Box::new(child(heads, left, literals)?)),
                generations_from_roots: Some(generation_range(generation_from_roots)?),
            }))),
            R::Heads(value) => expr(Q::Heads(Box::new(q::UnaryExpr {
                input: Some(Box::new(child(value, left, literals)?)),
            }))),
            R::Roots(value) => expr(Q::Roots(Box::new(q::UnaryExpr {
                input: Some(Box::new(child(value, left, literals)?)),
            }))),
            R::HeadsRange {
                roots,
                heads,
                parents_range,
                filter: None,
            } => expr(Q::Heads(Box::new(q::UnaryExpr {
                input: Some(Box::new(expr(Q::Range(Box::new(q::RangeExpr {
                    roots: Some(Box::new(child(roots, left, literals)?)),
                    heads: Some(Box::new(child(heads, left, literals)?)),
                    generations: None,
                    parents: Some(parent_range(parents_range)?),
                }))))),
            }))),
            R::Union(a, b) => expr(Q::Union(q::SetExpr {
                inputs: vec![child(a, left, literals)?, child(b, left, literals)?],
            })),
            R::Intersection(a, b) => expr(Q::Intersection(q::SetExpr {
                inputs: vec![child(a, left, literals)?, child(b, left, literals)?],
            })),
            R::Difference(a, b) => expr(Q::Difference(Box::new(q::DifferenceExpr {
                left: Some(Box::new(child(a, left, literals)?)),
                right: Some(Box::new(child(b, left, literals)?)),
            }))),
            R::FilterWithin {
                candidates,
                predicate:
                    jj_lib::revset::ResolvedPredicateExpression::Filter(
                        jj_lib::revset::RevsetFilterPredicate::HasConflict,
                    ),
            } => expr(Q::HasConflict(Box::new(q::UnaryExpr {
                input: Some(Box::new(child(candidates, left, literals)?)),
            }))),
            R::FilterWithin {
                candidates,
                predicate: filter,
            } => {
                let candidates = child(candidates, left, literals)?;
                let (set, negative) = predicate(filter, depth + 1, left, literals, limits)?;
                if negative {
                    expr(Q::Difference(Box::new(q::DifferenceExpr {
                        left: Some(Box::new(candidates)),
                        right: Some(Box::new(set)),
                    })))
                } else {
                    expr(Q::Intersection(q::SetExpr {
                        inputs: vec![candidates, set],
                    }))
                }
            }
            R::Latest { candidates, count } => {
                if *count > limits.max_query_rows {
                    return Err(Error::Limit);
                }
                expr(Q::Latest(Box::new(q::LatestExpr {
                    candidates: Some(Box::new(child(candidates, left, literals)?)),
                    count: *count as u64,
                })))
            }
            unsupported => {
                return Err(Error::UnsupportedExpression(match unsupported {
                    R::Reachable { .. } => "reachable",
                    R::HeadsRange { .. } => "heads_range_filter",
                    R::Forks { .. } => "forks",
                    R::ForkPoint(_) => "fork_point",
                    R::MergePoint { .. } => "merge_point",
                    R::Bisect(_) => "bisect",
                    R::HasSize { .. } => "has_size",
                    R::Coalesce(..) => "coalesce",
                    _ => "unknown",
                }));
            }
        })
    }
    let mut remaining_nodes = limits
        .max_expression_nodes
        .checked_sub(usize::from(conflict))
        .ok_or(Error::Limit)?;
    let mut remaining_literals = limits.max_query_rows;
    let expression = build(
        input,
        1 + usize::from(conflict),
        &mut remaining_nodes,
        &mut remaining_literals,
        limits,
    )?;
    let expression = if conflict {
        expr(q::graph_expr::Expression::HasConflict(Box::new(
            q::UnaryExpr {
                input: Some(Box::new(expression)),
            },
        )))
    } else {
        expression
    };
    if expression.encoded_len() > limits.max_segment_bytes {
        return Err(Error::Limit);
    }
    // Lowered synthetic operators also count toward the public AST limits.
    let mut todo = vec![(&expression, 1usize)];
    let mut nodes = 0;
    let mut literals = 0usize;
    while let Some((node, depth)) = todo.pop() {
        nodes += 1;
        if nodes > limits.max_expression_nodes || depth > limits.max_expression_depth {
            return Err(Error::Limit);
        }
        use q::graph_expr::Expression as Q;
        match node.expression.as_ref().ok_or(Error::Corrupt)? {
            Q::Commits(v) => {
                literals = literals.checked_add(v.commits.len()).ok_or(Error::Limit)?;
                if literals > limits.max_query_rows {
                    return Err(Error::Limit);
                }
            }
            Q::Ancestors(v) => todo.push((v.heads.as_deref().ok_or(Error::Corrupt)?, depth + 1)),
            Q::Range(v) => {
                todo.push((v.roots.as_deref().ok_or(Error::Corrupt)?, depth + 1));
                todo.push((v.heads.as_deref().ok_or(Error::Corrupt)?, depth + 1))
            }
            Q::DagRange(v) => {
                todo.push((v.roots.as_deref().ok_or(Error::Corrupt)?, depth + 1));
                todo.push((v.heads.as_deref().ok_or(Error::Corrupt)?, depth + 1))
            }
            Q::ChangedPath(v) => {
                crate::path_filter::validate_filter(v, limits)?;
                if let Some(matcher) = &v.matcher {
                    let stats =
                        tinytree_proto::validate_fileset(matcher).map_err(|_| Error::Invalid)?;
                    nodes = nodes.checked_add(stats.nodes).ok_or(Error::Limit)?;
                    if nodes > limits.max_expression_nodes
                        || depth + stats.depth > limits.max_expression_depth
                    {
                        return Err(Error::Limit);
                    }
                }
                todo.push((v.candidates.as_deref().ok_or(Error::Corrupt)?, depth + 1))
            }
            Q::Heads(v) | Q::Roots(v) | Q::HasConflict(v) => {
                todo.push((v.input.as_deref().ok_or(Error::Corrupt)?, depth + 1))
            }
            Q::Union(v) | Q::Intersection(v) => {
                for v in &v.inputs {
                    todo.push((v, depth + 1))
                }
            }
            Q::Difference(v) => {
                todo.push((v.left.as_deref().ok_or(Error::Corrupt)?, depth + 1));
                todo.push((v.right.as_deref().ok_or(Error::Corrupt)?, depth + 1))
            }
            Q::Latest(v) => todo.push((v.candidates.as_deref().ok_or(Error::Corrupt)?, depth + 1)),
        }
    }
    Ok(expression)
}
fn generation_range(r: &std::ops::Range<u64>) -> Result<q::GenerationRange, Error> {
    if r.start > r.end {
        return Err(Error::Invalid);
    }
    Ok(q::GenerationRange {
        start: r.start,
        end_exclusive: (r.end != u64::MAX).then_some(r.end),
    })
}
fn parent_range(r: &std::ops::Range<u32>) -> Result<q::ParentRange, Error> {
    if r.start > r.end {
        return Err(Error::Invalid);
    }
    Ok(q::ParentRange {
        start: r.start,
        end_exclusive: (r.end != u32::MAX).then_some(r.end),
    })
}
#[derive(Debug)]
pub(super) struct NativeRevset {
    pub(super) index: NativeReadonlyIndex,
    pub(super) expression: q::GraphExpr,
    pub(super) file_all: Option<Arc<Store>>,
    pub(super) path_store: Option<Arc<Store>>,
}
pub(super) struct Cursor {
    index: NativeReadonlyIndex,
    scope: Arc<CachedScope>,
    handle: [u8; 32],
    sequence: u64,
    pages: usize,
    seen: BTreeSet<m::CommitId>,
    edges: usize,
    closed: bool,
    projection: q::Projection,
    page_size: u32,
}
impl Cursor {
    async fn begin(
        index: NativeReadonlyIndex,
        expression: q::GraphExpr,
        projection: q::Projection,
        path_store: Option<Arc<Store>>,
    ) -> Result<Self, Error> {
        let scope = index.scope().await?;
        let (expression, owner) =
            crate::path_filter::prepare(&index, &scope, expression, path_store).await?;
        Self::start_owned(index, scope, expression, projection, owner).await
    }
    pub(super) async fn start(
        index: NativeReadonlyIndex,
        scope: Arc<CachedScope>,
        expression: q::GraphExpr,
        projection: q::Projection,
    ) -> Result<Self, Error> {
        Self::start_owned(index, scope, expression, projection, None).await
    }
    async fn start_owned(
        index: NativeReadonlyIndex,
        scope: Arc<CachedScope>,
        expression: q::GraphExpr,
        projection: q::Projection,
        preparation: Option<Arc<crate::path_filter::Preparation>>,
    ) -> Result<Self, Error> {
        // CLI consumer limits stop this stream, not the server AST. Request one
        // V2 path match at a time so the first page cannot scan ahead merely to
        // fill a batch that the caller will immediately discard.
        let page_size = if crate::path_filter::has_merged_path(&expression) {
            1
        } else {
            index.core.limits.page_size
        };
        let reply = index
            .core
            .query
            .begin(
                native_broker::BeginQuery {
                    scope: scope.value.handle,
                    expression,
                    projection,
                    page_size,
                    replay_key: scope
                        .value
                        .bound_query_id(random()[..16].try_into().expect("fixed nonce width"))
                        .map_err(status_at(QueryStage::GraphBegin))?,
                },
                Arc::new((index.core.owner(), scope.clone(), preparation.clone())),
            )
            .await
            .map_err(status_at(QueryStage::GraphBegin))?;
        if let Some(preparation) = preparation {
            preparation.complete();
        }
        Ok(Self {
            index,
            scope,
            handle: reply.value.handle,
            sequence: 0,
            pages: 0,
            seen: BTreeSet::new(),
            edges: 0,
            closed: false,
            projection,
            page_size,
        })
    }
    fn owner(&self) -> QueryOwner {
        Arc::new((self.index.core.owner(), self.scope.clone()))
    }
    pub(super) async fn page(
        &mut self,
    ) -> Result<native_broker::QueryReply<native_broker::QueryPage>, Error> {
        if self.pages >= self.index.core.limits.max_query_pages {
            return Err(Error::Limit);
        }
        let reply = self
            .index
            .core
            .query
            .page(self.handle, self.sequence, self.owner())
            .await
            .map_err(status_at(QueryStage::GraphPage))?;
        if reply.value.sequence != self.sequence || reply.value.rows.len() > self.page_size as usize
        {
            return Err(Error::Corrupt);
        }
        let mut ids = Vec::with_capacity(reply.value.rows.len());
        let mut page_ids = BTreeSet::new();
        let mut edges = self.edges;
        for row in &reply.value.rows {
            let id = checked_model(row.commit.as_ref())?;
            if row.change.as_ref().is_none_or(|c| c.value.len() != 16)
                || self.seen.contains(&id)
                || !page_ids.insert(id)
            {
                return Err(Error::Corrupt);
            }
            if self.projection == q::Projection::Ids
                && (!row.edges.is_empty() || row.canonical.is_some())
            {
                return Err(Error::Corrupt);
            }
            if id == m::CommitId::ROOT {
                if row.canonical.is_some()
                    || !row.edges.is_empty()
                    || row.change.as_ref().is_none_or(|c| c.value != [0; 16])
                {
                    return Err(Error::Corrupt);
                }
            } else if self.projection == q::Projection::Canonical {
                let data = row.canonical.as_ref().ok_or(Error::Canonical)?;
                let descriptor = data.descriptor.as_ref().ok_or(Error::Canonical)?;
                let key = descriptor.key.as_ref().ok_or(Error::Canonical)?;
                if key.kind != m::Kind::Commit as i32
                    || key.native_id != id.0
                    || descriptor.content_blake3 != id.0
                    || descriptor.object_format_version != u32::from(m::FORMAT_VERSION)
                {
                    return Err(Error::Canonical);
                }
                let draft = self.index.verify_draft(
                    m::Descriptor {
                        key: m::ObjectKey {
                            kind: m::Kind::Commit,
                            id: m::ObjectId(id.0),
                        },
                        size_bytes: descriptor.size_bytes,
                        format_version: m::FORMAT_VERSION,
                    },
                    &data.canonical_envelope,
                    id,
                )?;
                if row
                    .change
                    .as_ref()
                    .is_none_or(|c| c.value != draft.change.0)
                {
                    return Err(Error::Canonical);
                }
                let parents: BTreeSet<_> = draft.parents.iter().copied().collect();
                for edge in &row.edges {
                    if let Some(q::selected_edge::Edge::Direct(v)) = &edge.edge {
                        if !parents.contains(&checked_model(v.target.as_ref())?) {
                            return Err(Error::Canonical);
                        }
                    }
                }
            } else if row.canonical.is_some() {
                return Err(Error::Corrupt);
            }
            edges = edges.checked_add(row.edges.len()).ok_or(Error::Limit)?;
            if edges > self.index.core.limits.max_query_edges {
                return Err(Error::Limit);
            }
            for edge in &row.edges {
                let id = match edge.edge.as_ref().ok_or(Error::Corrupt)? {
                    q::selected_edge::Edge::Direct(v) => v.target.as_ref(),
                    q::selected_edge::Edge::Indirect(v) => v.target.as_ref(),
                    q::selected_edge::Edge::Missing(v) => v.boundary.as_ref(),
                };
                checked_model(id)?;
            }
            ids.push(id)
        }
        if self
            .seen
            .len()
            .checked_add(ids.len())
            .is_none_or(|n| n > self.index.core.limits.max_query_rows)
        {
            return Err(Error::Limit);
        }
        self.seen.extend(ids);
        self.edges = edges;
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Limit)?;
        self.pages += 1;
        Ok(reply)
    }
    pub(super) async fn close(&mut self) {
        if self
            .index
            .core
            .query
            .close_query(self.handle, self.owner())
            .await
            .is_ok()
        {
            self.closed = true;
        }
    }
}
impl Drop for Cursor {
    fn drop(&mut self) {
        if !self.closed {
            self.index.core.enqueue(self.handle, false, self.owner());
        }
    }
}
fn checked_model(id: Option<&q::CommitId>) -> Result<m::CommitId, Error> {
    Ok(m::CommitId(
        id.ok_or(Error::Corrupt)?
            .value
            .as_slice()
            .try_into()
            .map_err(|_| Error::Corrupt)?,
    ))
}
struct Rows {
    rows: std::vec::IntoIter<q::GraphRow>,
    _owner: QueryOwner,
}
fn rows(
    index: NativeReadonlyIndex,
    expression: q::GraphExpr,
    projection: q::Projection,
    file_all: Option<Arc<Store>>,
    path_store: Option<Arc<Store>>,
) -> LocalBoxStream<'static, Result<q::GraphRow, RevsetEvaluationError>> {
    Box::pin(async_stream::try_stream! {
        let wire_projection = if file_all.is_some() { q::Projection::Canonical } else { projection };
        let mut cursor=Cursor::begin(index,expression,wire_projection,path_store).await.map_err(rev_err)?;
        let mut diff_entries = selected::Work::default();
        loop {
            let page=cursor.page().await.map_err(rev_err)?;
            let exhausted=page.value.exhausted;
            let mut batch=Rows{rows:page.value.rows.into_iter(),_owner:page.owner};
            for row in &mut batch.rows {
                if let Some(store) = &file_all {
                    let id = jj_id(checked_model(row.commit.as_ref()).map_err(rev_err)?);
                    if !selected_file_changed(store, &cursor.index, &cursor.scope, id, batch._owner.clone(), &mut diff_entries).await? { continue; }
                }
                yield row;
            }
            if exhausted { cursor.close().await; break }
        }
    })
}
// Separate from the sync boolean bridge: all four sync workers may await
// selected work concurrently, so sharing their blocking pool would deadlock.
pub(super) fn selected_runtime() -> Result<&'static SyncRuntime, Error> {
    static STATE: std::sync::OnceLock<Result<SyncRuntime, std::io::Error>> =
        std::sync::OnceLock::new();
    STATE
        .get_or_init(|| {
            Ok(SyncRuntime {
                runtime: tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .max_blocking_threads(4)
                    .thread_name("native-index-selected")
                    .enable_all()
                    .build()?,
                jobs: Arc::new(tokio::sync::Semaphore::new(4)),
            })
        })
        .as_ref()
        .map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                "native index selected runtime unavailable",
            ))
        })
}
async fn selected_file_changed(
    store: &Arc<Store>,
    index: &NativeReadonlyIndex,
    scope: &Arc<CachedScope>,
    id: CommitId,
    owner: QueryOwner,
    entries: &mut selected::Work,
) -> Result<bool, RevsetEvaluationError> {
    let runtime = selected_runtime().map_err(rev_err)?;
    let permit = runtime
        .jobs
        .clone()
        .try_acquire_owned()
        .map_err(|_| rev_err(Error::Limit))?;
    // Admission precedes snapshots. The physical job, not its waiter/timer,
    // retains provenance and permits through underlying metadata/tree/file I/O.
    let store = store.clone();
    let index = index.clone();
    let timeout = Duration::from_millis(index.core.limits.sync_query_timeout_ms);
    let work = std::mem::replace(entries, selected::Work::exhausted());
    let scope = scope.clone();
    let owner: QueryOwner = Arc::new((store.clone(), index.clone(), scope.clone(), owner, permit));
    let (send, recv) = tokio::sync::oneshot::channel();
    runtime.runtime.spawn_blocking(move || {
        let result = futures::executor::block_on(selected::changed(
            &store,
            &index,
            scope,
            id,
            owner.clone(),
            work,
        ));
        drop(index);
        drop(store);
        drop(owner);
        let _ = send.send(result);
    });
    let (send, recv_result) = tokio::sync::oneshot::channel();
    runtime.runtime.spawn(async move {
        let result = match tokio::time::timeout(timeout, recv).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(rev_err(Error::Query(tonic::Code::Unavailable))),
            Err(_) => Err(rev_err(Error::Query(tonic::Code::DeadlineExceeded))),
        };
        let _ = send.send(result);
    });
    let (changed, used) = recv_result
        .await
        .map_err(|_| rev_err(Error::Query(tonic::Code::Unavailable)))??;
    *entries = used;
    Ok(changed)
}
impl Revset for NativeRevset {
    fn prefix_disambiguation_index(
        &self,
    ) -> IndexResult<Option<Arc<dyn jj_lib::id_prefix::PrefixDisambiguationIndex>>> {
        if self.file_all.is_some() || crate::path_filter::has_path(&self.expression) {
            return Ok(None);
        }
        Ok(Some(Arc::new(
            crate::prefix::PrefixIndex::new(self.index.clone(), &self.expression).map_err(err)?,
        )))
    }

    fn stream<'a>(&self) -> LocalBoxStream<'a, Result<CommitId, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(
            rows(
                self.index.clone(),
                self.expression.clone(),
                q::Projection::Canonical,
                self.file_all.clone(),
                self.path_store.clone(),
            )
            .map(|v| {
                let row = v?;
                Ok(jj_id(checked_model(row.commit.as_ref()).map_err(rev_err)?))
            }),
        )
    }
    fn commit_change_ids<'a>(
        &self,
    ) -> LocalBoxStream<'a, Result<(CommitId, ChangeId), RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(
            rows(
                self.index.clone(),
                self.expression.clone(),
                q::Projection::Canonical,
                self.file_all.clone(),
                self.path_store.clone(),
            )
            .map(|v| {
                let row = v?;
                let id = jj_id(checked_model(row.commit.as_ref()).map_err(rev_err)?);
                let change =
                    ChangeId::from_bytes(&row.change.ok_or_else(|| rev_err(Error::Corrupt))?.value);
                Ok((id, change))
            }),
        )
    }
    fn stream_graph<'a>(
        &self,
    ) -> LocalBoxStream<'a, Result<GraphNode<CommitId>, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        if self.file_all.is_some() {
            return Box::pin(futures::stream::once(async {
                Err(rev_err(Error::UnsupportedExpression("selected_file_graph")))
            }));
        }
        Box::pin(
            rows(
                self.index.clone(),
                self.expression.clone(),
                q::Projection::Canonical,
                self.file_all.clone(),
                self.path_store.clone(),
            )
            .map(|v| {
                let row = v?;
                let id = jj_id(checked_model(row.commit.as_ref()).map_err(rev_err)?);
                let mut edges = Vec::with_capacity(row.edges.len());
                for edge in row.edges {
                    use q::selected_edge::Edge;
                    edges.push(match edge.edge.ok_or_else(|| rev_err(Error::Corrupt))? {
                        Edge::Direct(v) => GraphEdge::direct(jj_id(
                            checked_model(v.target.as_ref()).map_err(rev_err)?,
                        )),
                        Edge::Indirect(v) => GraphEdge::indirect(jj_id(
                            checked_model(v.target.as_ref()).map_err(rev_err)?,
                        )),
                        Edge::Missing(v) => GraphEdge::missing(jj_id(
                            checked_model(v.boundary.as_ref()).map_err(rev_err)?,
                        )),
                    })
                }
                Ok((id, edges))
            }),
        )
    }
    fn is_empty(&self) -> Result<bool, RevsetEvaluationError> {
        sync_empty(
            &self.index,
            &self.expression,
            self.file_all.as_ref(),
            self.path_store.as_ref(),
        )
    }
    fn count_estimate(&self) -> Result<(usize, Option<usize>), RevsetEvaluationError> {
        Ok((0, None))
    }
    fn containing_fn<'a>(&self) -> Box<RevsetContainingFn<'a>>
    where
        Self: 'a,
    {
        let index = self.index.clone();
        let expression = self.expression.clone();
        let file_all = self.file_all.clone();
        let path_store = self.path_store.clone();
        Box::new(move |id| {
            let index = index.clone();
            let expression = expression.clone();
            let file_all = file_all.clone();
            let path_store = path_store.clone();
            let id = id.clone();
            Box::pin(async move {
                if !index
                    .has_id(&id)
                    .await
                    .map_err(|e| RevsetEvaluationError::Other(Box::new(e)))?
                {
                    return Ok(false);
                }
                if crate::path_filter::has_path(&expression) {
                    let exact = expr(q::graph_expr::Expression::Intersection(q::SetExpr {
                        inputs: vec![
                            expression,
                            commits(std::slice::from_ref(&id)).map_err(rev_err)?,
                        ],
                    }));
                    return rows(index, exact, q::Projection::Ids, None, path_store)
                        .next()
                        .await
                        .transpose()
                        .map(|row| row.is_some());
                }
                // A pure immutable membership fact needs no resumable query
                // identity or replay tombstone for every rendered template row.
                let fact = q::graph_fact::Fact::Membership(q::Membership {
                    expression: Some(expression),
                    candidate: Some(q::CommitId {
                        value: model_id(&id).map_err(rev_err)?.0.to_vec(),
                    }),
                });
                let present = match index.fact(fact).await.map_err(rev_err)? {
                    q::graph_fact_answer::Answer::Membership(present) => present,
                    _ => return Err(rev_err(Error::Corrupt)),
                };
                if !present {
                    return Ok(false);
                }
                if let Some(store) = file_all {
                    // Membership only narrows the server-side graph predicate;
                    // preserve the exact selected-client file filter as well.
                    let scope = index.scope().await.map_err(rev_err)?;
                    let owner: QueryOwner = Arc::new((index.core.owner(), scope.clone()));
                    let mut work = selected::Work::default();
                    selected_file_changed(&store, &index, &scope, id, owner, &mut work).await
                } else {
                    Ok(true)
                }
            }) as LocalBoxFuture<'a, _>
        })
    }
}

pub(super) struct SyncRuntime {
    pub(super) runtime: tokio::runtime::Runtime,
    pub(super) jobs: Arc<tokio::sync::Semaphore>,
}
fn sync_runtime() -> Result<&'static SyncRuntime, Error> {
    static STATE: std::sync::OnceLock<Result<SyncRuntime, std::io::Error>> =
        std::sync::OnceLock::new();
    STATE
        .get_or_init(|| {
            Ok(SyncRuntime {
                runtime: tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .max_blocking_threads(4)
                    .thread_name("native-index-query")
                    .enable_all()
                    .build()?,
                jobs: Arc::new(tokio::sync::Semaphore::new(4)),
            })
        })
        .as_ref()
        .map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                "native index query runtime unavailable",
            ))
        })
}
fn sync_empty(
    index: &NativeReadonlyIndex,
    expression: &q::GraphExpr,
    file_all: Option<&Arc<Store>>,
    path_store: Option<&Arc<Store>>,
) -> Result<bool, RevsetEvaluationError> {
    let runtime = sync_runtime().map_err(rev_err)?;
    // Acquire before spawning: neither Tokio's blocking queue nor accepted
    // index/AST snapshots can grow without this process-wide four-job bound.
    let permit = runtime
        .jobs
        .clone()
        .try_acquire_owned()
        .map_err(|_| rev_err(Error::Limit))?;
    // Deep snapshots are admitted too, not just the subsequently queued job.
    let index = index.clone();
    let expression = expression.clone();
    let file_all = file_all.cloned();
    let path_store = path_store.cloned();
    let timeout = Duration::from_millis(index.core.limits.sync_query_timeout_ms);
    let (send, recv) = std::sync::mpsc::sync_channel(1);
    runtime.runtime.spawn_blocking(move || {
        // The LocalBoxStream is created and polled on this owned worker, never
        // nested inside the caller's LocalPool or current-thread runtime.
        let result = futures::executor::block_on(async {
            rows(index, expression, q::Projection::Ids, file_all, path_store)
                .next()
                .await
                .transpose()
                .map(|v| v.is_none())
        });
        // Completion is observable only after this job's bounded snapshot/work
        // admission is released; timed-out jobs keep it until this point.
        drop(permit);
        let _ = send.send(result);
    });
    match recv.recv_timeout(timeout) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            Err(rev_err(Error::Query(tonic::Code::DeadlineExceeded)))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            Err(rev_err(Error::Query(tonic::Code::Unavailable)))
        }
    }
}
