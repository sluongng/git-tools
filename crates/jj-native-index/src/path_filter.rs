//! Exact-path local-draft lowering. Canonical public/frozen work stays remote.
use super::*;
use futures::{FutureExt, future::BoxFuture};
use prost::Message as _;

pub(super) fn has_path(expression: &q::GraphExpr) -> bool {
    has_policy(expression, None)
}
pub(super) fn has_merged_path(expression: &q::GraphExpr) -> bool {
    has_policy(expression, Some(2))
}
fn has_policy(expression: &q::GraphExpr, policy: Option<u32>) -> bool {
    use q::graph_expr::Expression as X;
    match expression.expression.as_ref() {
        Some(X::ChangedPath(v)) => {
            policy.is_none_or(|policy| v.policy_version == policy)
                || v.candidates
                    .as_deref()
                    .is_some_and(|v| has_policy(v, policy))
        }
        Some(X::Ancestors(v)) => v.heads.as_deref().is_some_and(|v| has_policy(v, policy)),
        Some(X::Range(v)) => {
            v.roots.as_deref().is_some_and(|v| has_policy(v, policy))
                || v.heads.as_deref().is_some_and(|v| has_policy(v, policy))
        }
        Some(X::DagRange(v)) => {
            v.roots.as_deref().is_some_and(|v| has_policy(v, policy))
                || v.heads.as_deref().is_some_and(|v| has_policy(v, policy))
        }
        Some(X::Heads(v) | X::Roots(v) | X::HasConflict(v)) => {
            v.input.as_deref().is_some_and(|v| has_policy(v, policy))
        }
        Some(X::Union(v) | X::Intersection(v)) => v.inputs.iter().any(|v| has_policy(v, policy)),
        Some(X::Difference(v)) => {
            v.left.as_deref().is_some_and(|v| has_policy(v, policy))
                || v.right.as_deref().is_some_and(|v| has_policy(v, policy))
        }
        Some(X::Latest(v)) => v
            .candidates
            .as_deref()
            .is_some_and(|v| has_policy(v, policy)),
        _ => false,
    }
}
pub(super) fn validate_options(store: &Store) -> Result<(), Error> {
    let options = store.merge_options();
    if options.hunk_level != jj_lib::files::FileMergeHunkLevel::Line
        || options.same_change != jj_lib::merge::SameChange::Accept
    {
        return Err(Error::UnsupportedExpression("path_merge_options"));
    }
    Ok(())
}
fn expr(expression: q::graph_expr::Expression) -> q::GraphExpr {
    q::GraphExpr {
        version: 1,
        expression: Some(expression),
    }
}
fn ids(values: &[m::CommitId]) -> q::GraphExpr {
    expr(q::graph_expr::Expression::Commits(q::CommitSet {
        commits: values
            .iter()
            .map(|id| q::CommitId {
                value: id.0.to_vec(),
            })
            .collect(),
    }))
}
use super::selected::Work;
struct CommitRead {
    value: m::Commit,
    _owner: QueryOwner,
}
async fn commit(
    index: &NativeReadonlyIndex,
    scope: &Arc<CachedScope>,
    id: m::CommitId,
    owner: QueryOwner,
    work: &mut Work,
) -> Result<CommitRead, Error> {
    let maximum = index
        .core
        .limits
        .max_metadata_bytes
        .checked_mul(16)
        .and_then(|n| n.checked_add(4096))
        .ok_or(Error::Limit)?;
    work.reserve(&index.core.limits, 1, maximum)?;
    let reply = index
        .core
        .query
        .read_commit(scope.value.handle, id, owner)
        .await
        .map_err(status)?;
    work.check()?;
    let d = reply.value.descriptor();
    if d.key.kind != m::Kind::Commit
        || d.key.id.0 != id.0
        || d.format_version != m::FORMAT_VERSION
        || d.size_bytes > index.core.limits.max_metadata_bytes as u64
    {
        return Err(Error::Canonical);
    }
    let actual = usize::try_from(d.size_bytes)
        .ok()
        .and_then(|n| n.checked_mul(16))
        .and_then(|n| n.checked_add(4096))
        .ok_or(Error::Limit)?;
    work.refund_bytes(maximum - actual)?;
    let m::Object::Commit(value) = reply.value.into_object() else {
        return Err(Error::Canonical);
    };
    Ok(CommitRead {
        value,
        _owner: reply.owner,
    })
}
async fn path(
    index: &NativeReadonlyIndex,
    commit: &m::Commit,
    path: &str,
    owner: QueryOwner,
    work: &mut Work,
) -> Result<Option<m::TreeValue>, Error> {
    let &[mut id] = commit.root_tree.terms.as_slice() else {
        return Err(Error::UnsupportedExpression("path_conflict_boundary"));
    };
    let components: Vec<_> = path.split('/').collect();
    for (position, component) in components.iter().enumerate() {
        work.check()?;
        let key = m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(id.0),
        };
        let descriptor = index
            .core
            .objects
            .descriptor_with_owner(key, owner.clone())
            .await
            .map_err(|_| Error::Canonical)?
            .ok_or(Error::Canonical)?;
        if descriptor.key != key
            || descriptor.format_version != m::FORMAT_VERSION
            || descriptor.size_bytes > index.core.limits.max_metadata_bytes as u64
        {
            return Err(Error::Canonical);
        }
        let charge = usize::try_from(descriptor.size_bytes)
            .ok()
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(4096))
            .ok_or(Error::Limit)?;
        work.reserve(&index.core.limits, 1, charge)?;
        let value = index
            .core
            .objects
            .read_metadata_with_owner(key, owner.clone())
            .await
            .map_err(|_| Error::Canonical)?;
        work.check()?;
        if value.descriptor() != descriptor {
            return Err(Error::Canonical);
        }
        let m::Object::Tree(tree) = value.into_object() else {
            return Err(Error::Canonical);
        };
        let Some(entry) = tree.entries.into_iter().find(|e| e.name == **component) else {
            return Ok(None);
        };
        if position + 1 == components.len() {
            return Ok(Some(entry.value));
        }
        match entry.value {
            m::TreeValue::Tree(next) => id = next,
            _ => return Ok(None),
        }
    }
    Err(Error::Invalid)
}
async fn local_changed(
    index: &NativeReadonlyIndex,
    scope: &Arc<CachedScope>,
    id: m::CommitId,
    filter: &q::ChangedPathExpr,
    owner: QueryOwner,
    work: &mut Work,
) -> Result<bool, Error> {
    let current = commit(index, scope, id, owner.clone(), work).await?;
    if current.value.parents.len() > 1 {
        return Err(Error::UnsupportedExpression("path_merge_boundary"));
    }
    let after = path(index, &current.value, &filter.path, owner.clone(), work).await?;
    let before = match current.value.parents.first() {
        None | Some(&m::CommitId::ROOT) => None,
        Some(parent) => {
            let parent = commit(index, scope, *parent, owner.clone(), work).await?;
            path(index, &parent.value, &filter.path, owner, work).await?
        }
    };
    if filter.exact_file
        && [&before, &after]
            .iter()
            .any(|value| matches!(value, Some(m::TreeValue::Tree(_))))
    {
        return Err(Error::UnsupportedExpression("path_exact_file_directory"));
    }
    Ok(before != after)
}
pub(super) struct Preparation {
    permit: std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
}
impl Preparation {
    pub(super) fn complete(&self) {
        // Metadata and local rewrite scratch are gone; the independently
        // admitted query now owns its AST. Idle cursors hold no helper slot.
        self.permit.lock().unwrap().take();
    }
}
pub(super) async fn prepare(
    index: &NativeReadonlyIndex,
    scope: &Arc<CachedScope>,
    expression: q::GraphExpr,
    caller: Option<Arc<Store>>,
) -> Result<(q::GraphExpr, Option<Arc<Preparation>>), Error> {
    if !has_path(&expression)
        || index
            .checkpoint
            .drafts
            .iter()
            .all(|d| index.checkpoint.baseline.contains(&d.id()))
    {
        return Ok((expression, None));
    }
    // A process-wide bounded physical job owns every snapshot and provider read
    // after caller cancellation. Public ancestry is never enumerated locally.
    let caller = caller.ok_or(Error::Invalid)?;
    if has_merged_path(&expression) {
        validate_options(&caller)?;
    }
    let timeout = Duration::from_millis(index.core.limits.sync_query_timeout_ms);
    let deadline = std::time::Instant::now() + timeout;
    let runtime = super::revsets::selected_runtime()?;
    let permit = runtime
        .jobs
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::Limit)?;
    let preparation = Arc::new(Preparation {
        permit: std::sync::Mutex::new(Some(permit)),
    });
    let index = index.clone();
    let scope = scope.clone();
    let (send, recv) = tokio::sync::oneshot::channel();
    runtime.runtime.spawn_blocking(move || {
        let owner: QueryOwner = Arc::new((index.clone(), scope.clone(), preparation.clone()));
        let mut work = Work::with_deadline(deadline);
        let result = futures::executor::block_on(rewrite(
            &index,
            &scope,
            expression,
            &caller,
            owner.clone(),
            &mut work,
            0,
        ));
        drop(index);
        drop(scope);
        drop(owner);
        let result = result.map(|expression| (expression, Some(preparation)));
        let _ = send.send(result);
    });
    // jj may poll this stream with its LocalPool, outside any Tokio reactor.
    // The owned timer bridge runs on the dedicated selected-work runtime.
    let (send, received) = tokio::sync::oneshot::channel();
    runtime.runtime.spawn(async move {
        let result =
            match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), recv).await {
                Ok(Ok(value)) => value,
                Ok(Err(_)) => Err(Error::Query(tonic::Code::Unavailable)),
                Err(_) => Err(Error::Query(tonic::Code::DeadlineExceeded)),
            };
        let _ = send.send(result);
    });
    received
        .await
        .map_err(|_| Error::Query(tonic::Code::Unavailable))?
}
fn rewrite<'a>(
    index: &'a NativeReadonlyIndex,
    scope: &'a Arc<CachedScope>,
    mut expression: q::GraphExpr,
    caller: &'a Arc<Store>,
    owner: QueryOwner,
    work: &'a mut Work,
    depth: usize,
) -> BoxFuture<'a, Result<q::GraphExpr, Error>> {
    async move {
        work.check()?;
        if depth > index.core.limits.max_expression_depth {
            return Err(Error::Limit);
        }
        use q::graph_expr::Expression as X;
        match expression.expression.as_mut().ok_or(Error::Invalid)? {
            X::ChangedPath(filter) => {
                let candidates = *filter.candidates.take().ok_or(Error::Invalid)?;
                let candidates = rewrite(
                    index,
                    scope,
                    candidates,
                    caller,
                    owner.clone(),
                    work,
                    depth + 1,
                )
                .await?;
                // Reserve bounded draft IDs, selected/matching vectors and
                // rewritten wire fields before allocating them.
                let draft_count = index.checkpoint.drafts.len();
                let scratch = draft_count
                    .checked_mul(1024)
                    .and_then(|n| {
                        candidates
                            .encoded_len()
                            .checked_mul(4)
                            .and_then(|m| n.checked_add(m))
                    })
                    .ok_or(Error::Limit)?;
                work.reserve(&index.core.limits, 1, scratch)?;
                let private: Vec<_> = index
                    .checkpoint
                    .drafts
                    .iter()
                    .filter(|d| !index.checkpoint.baseline.contains(&d.id()))
                    .map(|d| d.id())
                    .collect();
                if private.is_empty() {
                    filter.candidates = Some(Box::new(candidates));
                    return Ok(expression);
                }
                let selection = expr(X::Intersection(q::SetExpr {
                    inputs: vec![candidates.clone(), ids(&private)],
                }));
                let mut cursor = super::revsets::Cursor::start(
                    index.clone(),
                    scope.clone(),
                    selection,
                    q::Projection::Ids,
                )
                .await?;
                let mut selected = Vec::with_capacity(private.len());
                let mut matches = Vec::with_capacity(private.len());
                loop {
                    work.check()?;
                    let page = cursor.page().await?;
                    for row in &page.value.rows {
                        let id = m::CommitId::from_slice(
                            &row.commit.as_ref().ok_or(Error::Corrupt)?.value,
                        )
                        .map_err(|_| Error::Corrupt)?;
                        if selected.len() >= private.len() || !private.contains(&id) {
                            return Err(Error::Corrupt);
                        }
                        selected.push(id);
                        let owner: QueryOwner = Arc::new((owner.clone(), page.owner.clone()));
                        let changed = match filter.policy_version {
                            1 => local_changed(index, scope, id, filter, owner, work).await?,
                            2 => {
                                let initial = std::mem::replace(work, Work::exhausted());
                                let (changed, spent) = selected::changed_path(
                                    caller,
                                    index,
                                    scope.clone(),
                                    jj_id(id),
                                    owner,
                                    initial,
                                    filter,
                                )
                                .await
                                .map_err(local_error)?;
                                *work = spent;
                                changed
                            }
                            _ => return Err(Error::Invalid),
                        };
                        if changed {
                            matches.push(id);
                        }
                    }
                    if page.value.exhausted {
                        cursor.close().await;
                        break;
                    }
                }
                filter.candidates =
                    Some(Box::new(expr(X::Difference(Box::new(q::DifferenceExpr {
                        left: Some(Box::new(candidates)),
                        right: Some(Box::new(ids(&selected))),
                    })))));
                expression = expr(X::Union(q::SetExpr {
                    inputs: vec![expression, ids(&matches)],
                }));
            }
            X::Ancestors(v) => {
                if let Some(c) = v.heads.take() {
                    v.heads = Some(Box::new(
                        rewrite(index, scope, *c, caller, owner.clone(), work, depth + 1).await?,
                    ));
                }
            }
            X::Range(v) => {
                if let Some(c) = v.roots.take() {
                    v.roots = Some(Box::new(
                        rewrite(index, scope, *c, caller, owner.clone(), work, depth + 1).await?,
                    ));
                }
                if let Some(c) = v.heads.take() {
                    v.heads = Some(Box::new(
                        rewrite(index, scope, *c, caller, owner.clone(), work, depth + 1).await?,
                    ));
                }
            }
            X::DagRange(v) => {
                if let Some(c) = v.roots.take() {
                    v.roots = Some(Box::new(
                        rewrite(index, scope, *c, caller, owner.clone(), work, depth + 1).await?,
                    ));
                }
                if let Some(c) = v.heads.take() {
                    v.heads = Some(Box::new(
                        rewrite(index, scope, *c, caller, owner.clone(), work, depth + 1).await?,
                    ));
                }
            }
            X::Heads(v) | X::Roots(v) | X::HasConflict(v) => {
                if let Some(c) = v.input.take() {
                    v.input = Some(Box::new(
                        rewrite(index, scope, *c, caller, owner.clone(), work, depth + 1).await?,
                    ));
                }
            }
            X::Union(v) | X::Intersection(v) => {
                for c in &mut v.inputs {
                    *c = rewrite(
                        index,
                        scope,
                        std::mem::take(c),
                        caller,
                        owner.clone(),
                        work,
                        depth + 1,
                    )
                    .await?;
                }
            }
            X::Difference(v) => {
                if let Some(c) = v.left.take() {
                    v.left = Some(Box::new(
                        rewrite(index, scope, *c, caller, owner.clone(), work, depth + 1).await?,
                    ));
                }
                if let Some(c) = v.right.take() {
                    v.right = Some(Box::new(
                        rewrite(index, scope, *c, caller, owner.clone(), work, depth + 1).await?,
                    ));
                }
            }
            X::Latest(v) => {
                if let Some(c) = v.candidates.take() {
                    v.candidates = Some(Box::new(
                        rewrite(index, scope, *c, caller, owner.clone(), work, depth + 1).await?,
                    ));
                }
            }
            X::Commits(_) => {}
        }
        work.check()?;
        if expression.encoded_len() > index.core.limits.max_segment_bytes {
            return Err(Error::Limit);
        }
        Ok(expression)
    }
    .boxed()
}

fn local_error(error: RevsetEvaluationError) -> Error {
    match error {
        RevsetEvaluationError::Other(error)
        | RevsetEvaluationError::Backend(jj_lib::backend::BackendError::Other(error)) => error
            .downcast::<Error>()
            .map(|error| *error)
            .unwrap_or_else(|error| {
                use jj_lib::diff::LineDiffError as D;
                use jj_lib::files::FileMergeError as F;
                use jj_lib::tree_merge::BoundedFileResolveError as B;
                match error.downcast::<B>() {
                    Ok(error) => match *error {
                        B::Budget(D::LimitExceeded) | B::Merge(F::Budget(D::LimitExceeded)) => {
                            Error::Limit
                        }
                        B::Budget(D::Cancelled) | B::Merge(F::Budget(D::Cancelled)) => {
                            Error::Query(tonic::Code::DeadlineExceeded)
                        }
                        _ => Error::Canonical,
                    },
                    Err(_) => Error::Corrupt,
                }
            }),
        _ => Error::Canonical,
    }
}
