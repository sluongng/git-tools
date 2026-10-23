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
pub(super) fn compile(input: &ResolvedExpression, limits: &Limits) -> Result<q::GraphExpr, Error> {
    fn build(
        input: &ResolvedExpression,
        depth: usize,
        left: &mut usize,
        literals: &mut usize,
        limits: &Limits,
    ) -> Result<q::GraphExpr, Error> {
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
            R::Latest { candidates, count } => {
                if *count > limits.max_query_rows {
                    return Err(Error::Limit);
                }
                expr(Q::Latest(Box::new(q::LatestExpr {
                    candidates: Some(Box::new(child(candidates, left, literals)?)),
                    count: *count as u64,
                })))
            }
            _ => return Err(Error::Unsupported),
        })
    }
    let mut remaining_nodes = limits.max_expression_nodes;
    let mut remaining_literals = limits.max_query_rows;
    let expression = build(
        input,
        1,
        &mut remaining_nodes,
        &mut remaining_literals,
        limits,
    )?;
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
            Q::Heads(v) | Q::Roots(v) => {
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
}
struct Cursor {
    index: NativeReadonlyIndex,
    scope: Arc<CachedScope>,
    handle: [u8; 32],
    sequence: u64,
    pages: usize,
    seen: BTreeSet<m::CommitId>,
    edges: usize,
    closed: bool,
    projection: q::Projection,
}
impl Cursor {
    async fn begin(
        index: NativeReadonlyIndex,
        expression: q::GraphExpr,
        projection: q::Projection,
    ) -> Result<Self, Error> {
        let scope = index.scope().await?;
        let reply = index
            .core
            .query
            .begin(
                native_broker::BeginQuery {
                    scope: scope.value.handle,
                    expression,
                    projection,
                    page_size: index.core.limits.page_size,
                    replay_key: random(),
                },
                Arc::new((index.core.owner(), scope.clone())),
            )
            .await
            .map_err(status)?;
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
        })
    }
    fn owner(&self) -> QueryOwner {
        Arc::new((self.index.core.owner(), self.scope.clone()))
    }
    async fn page(&mut self) -> Result<native_broker::QueryReply<native_broker::QueryPage>, Error> {
        if self.pages >= self.index.core.limits.max_query_pages {
            return Err(Error::Limit);
        }
        let reply = self
            .index
            .core
            .query
            .page(self.handle, self.sequence, self.owner())
            .await
            .map_err(status)?;
        if reply.value.sequence != self.sequence
            || reply.value.rows.len() > self.index.core.limits.page_size as usize
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
    async fn close(&mut self) {
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
) -> LocalBoxStream<'static, Result<q::GraphRow, RevsetEvaluationError>> {
    Box::pin(
        async_stream::try_stream! {let mut cursor=Cursor::begin(index,expression,projection).await.map_err(rev_err)?;loop{let page=cursor.page().await.map_err(rev_err)?;let exhausted=page.value.exhausted;let mut batch=Rows{rows:page.value.rows.into_iter(),_owner:page.owner};for row in &mut batch.rows{yield row;}if exhausted{cursor.close().await;break}}},
    )
}
impl Revset for NativeRevset {
    fn stream<'a>(&self) -> LocalBoxStream<'a, Result<CommitId, RevsetEvaluationError>>
    where
        Self: 'a,
    {
        Box::pin(
            rows(
                self.index.clone(),
                self.expression.clone(),
                q::Projection::Canonical,
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
        Box::pin(
            rows(
                self.index.clone(),
                self.expression.clone(),
                q::Projection::Canonical,
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
        sync_empty(&self.index, &self.expression)
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
        Box::new(move |id| {
            let index = index.clone();
            let expression = expression.clone();
            let id = id.clone();
            Box::pin(async move {
                if !index
                    .has_id(&id)
                    .await
                    .map_err(|e| RevsetEvaluationError::Other(Box::new(e)))?
                {
                    return Ok(false);
                }
                let selected = commits(&[id]).map_err(rev_err)?;
                let intersection = expr(q::graph_expr::Expression::Intersection(q::SetExpr {
                    inputs: vec![expression, selected],
                }));
                rows(index, intersection, q::Projection::Ids)
                    .next()
                    .await
                    .transpose()
                    .map(|v| v.is_some())
            }) as LocalBoxFuture<'a, _>
        })
    }
}

struct SyncRuntime {
    runtime: tokio::runtime::Runtime,
    jobs: Arc<tokio::sync::Semaphore>,
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
    let timeout = Duration::from_millis(index.core.limits.sync_query_timeout_ms);
    let (send, recv) = std::sync::mpsc::sync_channel(1);
    runtime.runtime.spawn_blocking(move || {
        // The LocalBoxStream is created and polled on this owned worker, never
        // nested inside the caller's LocalPool or current-thread runtime.
        let result = futures::executor::block_on(async {
            rows(index, expression, q::Projection::Ids)
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
