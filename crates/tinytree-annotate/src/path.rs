// Copyright 2020 The Jujutsu Authors
// Copyright 2026 The git-tools Authors
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

//! Stock-compatible matcher-driven changes against recursively merged parents.
//!
//! The caller owns source authority, scratch-store lifetime, physical admission,
//! and one cumulative diff ledger. Synthetic resolved FILEs must stay in the
//! caller's disposable store; this engine never grants authority from an ID.
use crate::Budget;
use futures::StreamExt as _;
use jj_lib::{
    backend::{BackendError, CommitId},
    commit::Commit,
    diff::LineDiffBudget,
    matchers::Matcher,
    merge::Merge,
    merged_tree::MergedTree,
    store::Store,
};
use std::sync::Arc;

/// Canonical common ancestors are queried through the caller's captured source,
/// not reconstructed by downloading the ancestry into this engine.
#[allow(async_fn_in_trait)]
pub trait ParentGraph: Budget + Sync {
    /// Called before the diff can read any TREE payload. The scratch backend
    /// must precharge decode/frontier work with this raw-term bound, including
    /// paths pruned before the first yielded matcher entry.
    fn prepare_diff(&mut self, raw_terms: usize) -> std::result::Result<(), Self::Error>;
    async fn common_ancestors(
        &mut self,
        left: &[CommitId],
        right: &[CommitId],
    ) -> std::result::Result<Vec<CommitId>, Self::Error>;
}

#[derive(Debug)]
pub enum Error<E> {
    Source(E),
    Backend(BackendError),
    Resolve(jj_lib::tree_merge::BoundedFileResolveError),
    Limit,
}
type Result<T, E> = std::result::Result<T, Error<E>>;
fn checked<E>(n: Option<usize>) -> Result<usize, E> {
    n.ok_or(Error::Limit)
}
fn charge<G: Budget>(g: &mut G, items: usize, bytes: usize) -> Result<(), G::Error> {
    g.check().map_err(Error::Source)?;
    g.charge(
        items.try_into().map_err(|_| Error::Limit)?,
        bytes.try_into().map_err(|_| Error::Limit)?,
    )
    .map_err(Error::Source)
}
fn frontier<G: Budget>(g: &mut G, terms: usize) -> Result<(), G::Error> {
    g.check().map_err(Error::Source)?;
    g.frontier(terms).map_err(Error::Source)
}

/// Construct stock jj's ordered, label-preserving recursive parent merge without
/// resolving FILE contents. All stock flatten/simplify calls are prepaid.
pub async fn merged_parent_tree<G: ParentGraph>(
    store: &Arc<Store>,
    parents: &[Commit],
    graph: &mut G,
) -> Result<MergedTree, G::Error> {
    struct Frame {
        ids: Vec<CommitId>,
        next: usize,
        result: Merge<CommitId>,
    }
    fn frame(ids: Vec<CommitId>) -> Frame {
        let result = Merge::resolved(ids[0].clone());
        Frame {
            ids,
            next: 1,
            result,
        }
    }
    fn combine<G: Budget>(f: &mut Frame, base: Merge<CommitId>, g: &mut G) -> Result<(), G::Error> {
        let terms = checked(
            f.result
                .iter()
                .len()
                .checked_add(base.iter().len())
                .and_then(|n| n.checked_add(1)),
        )?;
        frontier(g, terms)?;
        // Flatten is linear. Both old and new terms, inner merge headers and
        // SmallVec's rounded capacities coexist until flatten returns.
        charge(g, terms, checked(terms.checked_mul(256))?)?;
        let previous = std::mem::replace(
            &mut f.result,
            Merge::resolved(CommitId::from_bytes(&[0; 32])),
        );
        let other = Merge::resolved(f.ids[f.next].clone());
        f.result = Merge::from_vec(vec![previous, base, other]).flatten();
        g.check().map_err(Error::Source)?;
        f.next += 1;
        Ok(())
    }
    frontier(graph, parents.len())?;
    charge(graph, 1, 128)?;
    let merge = match parents {
        [] => Merge::resolved(store.root_commit_id().clone()),
        [parent] => Merge::resolved(parent.id().clone()),
        _ => {
            charge(
                graph,
                parents.len(),
                checked(parents.len().checked_mul(128))?,
            )?;
            let ids = parents.iter().map(|p| p.id().clone()).collect();
            charge(graph, 1, std::mem::size_of::<Frame>())?;
            let mut frames = vec![frame(ids)];
            loop {
                graph.check().map_err(Error::Source)?;
                let live = frames.iter().try_fold(0usize, |n, frame| {
                    checked(
                        n.checked_add(frame.ids.len())
                            .and_then(|n| n.checked_add(frame.result.iter().len())),
                    )
                })?;
                frontier(graph, live)?;
                if frames.last().unwrap().next == frames.last().unwrap().ids.len() {
                    let done = frames.pop().unwrap().result;
                    if let Some(parent) = frames.last_mut() {
                        combine(parent, done, graph)?;
                    } else {
                        break done;
                    }
                } else {
                    charge(graph, 1, 0)?;
                    let current = frames.last().unwrap();
                    let bases = graph
                        .common_ancestors(
                            &current.ids[..current.next],
                            std::slice::from_ref(&current.ids[current.next]),
                        )
                        .await
                        .map_err(Error::Source)?;
                    frontier(graph, bases.len())?;
                    charge(graph, bases.len(), checked(bases.len().checked_mul(128))?)?;
                    match bases.as_slice() {
                        [] => combine(
                            frames.last_mut().unwrap(),
                            Merge::resolved(store.root_commit_id().clone()),
                            graph,
                        )?,
                        [base] => combine(
                            frames.last_mut().unwrap(),
                            Merge::resolved(base.clone()),
                            graph,
                        )?,
                        _ => {
                            frontier(graph, checked(frames.len().checked_add(1))?)?;
                            // Vec growth can retain the old capacity during allocation.
                            charge(
                                graph,
                                1,
                                checked(frames.len().checked_add(1).and_then(|n| {
                                    n.checked_mul(4 * std::mem::size_of::<Frame>())
                                }))?,
                            )?;
                            frames.push(frame(bases));
                        }
                    }
                }
            }
        }
    };
    frontier(graph, merge.iter().len())?;
    charge(
        graph,
        merge.iter().len(),
        checked(merge.iter().len().checked_mul(256))?,
    )?;
    let mut trees = Vec::with_capacity(merge.iter().len());
    let mut total_terms = 0usize;
    let mut total_labels = 0usize;
    for id in merge.iter() {
        graph.check().map_err(Error::Source)?;
        let commit = store.get_commit_async(id).await.map_err(Error::Backend)?;
        let terms = commit.tree_ids().iter().len();
        total_terms = checked(total_terms.checked_add(terms))?;
        frontier(graph, total_terms)?;
        let labels = commit
            .store_commit()
            .conflict_labels
            .iter()
            .try_fold(0usize, |n, label| checked(n.checked_add(label.len())))?;
        let fallback = checked(commit.description().len().checked_add(128))?;
        // Each fallback label can be expanded once per root-tree term. Existing
        // labels and repeated cached commits also pay per occurrence.
        let labels = checked(
            terms
                .checked_mul(fallback)
                .and_then(|n| n.checked_add(labels)),
        )?;
        total_labels = checked(total_labels.checked_add(labels))?;
        charge(
            graph,
            terms,
            checked(
                labels
                    .checked_add(checked(terms.checked_mul(256))?)
                    .and_then(|n| n.checked_mul(4)),
            )?,
        )?;
        trees.push((commit.tree(), commit.conflict_label()));
    }
    // merge_no_resolve performs label-preserving quadratic simplify. Prepay its
    // comparison/removal work and simultaneous input/output/mapping vectors.
    let quadratic = checked(
        total_terms
            .checked_mul(total_terms)
            .and_then(|n| n.checked_mul(4)),
    )?;
    let bytes = checked(
        total_terms
            .checked_mul(512)
            .and_then(|n| n.checked_add(total_labels.checked_mul(8)?)),
    )?;
    charge(graph, quadratic, bytes)?;
    let result = MergedTree::merge_no_resolve(Merge::from_vec(trees));
    graph.check().map_err(Error::Source)?;
    Ok(result)
}

/// Compare only actual matcher paths. Resolve only the before-side values, as
/// stock has_diff_from_parent does; unresolved terms retain exact equality.
/// Matcher adapters that prune on exhaustion MUST report that error from the
/// caller's Budget::check, including when the stream yields no entries.
pub async fn changed_with_matcher<G: ParentGraph>(
    store: &Arc<Store>,
    commit: &Commit,
    matcher: &dyn Matcher,
    graph: &mut G,
    diff_budget: &mut LineDiffBudget,
) -> Result<bool, G::Error> {
    frontier(graph, commit.parent_ids().len())?;
    charge(
        graph,
        commit.parent_ids().len(),
        checked(
            commit
                .parent_ids()
                .len()
                .checked_mul(std::mem::size_of::<Commit>()),
        )?,
    )?;
    let parents = commit.parents().await.map_err(Error::Backend)?;
    let from = merged_parent_tree(store, &parents, graph).await?;
    let terms = checked(
        from.tree_ids()
            .iter()
            .len()
            .checked_add(commit.tree_ids().iter().len()),
    )?;
    frontier(graph, terms)?;
    charge(
        graph,
        checked(terms.checked_mul(terms).and_then(|n| n.checked_mul(4)))?,
        checked(terms.checked_mul(512))?,
    )?;
    graph.prepare_diff(terms).map_err(Error::Source)?;
    let to = commit.tree();
    let mut diff = from.diff_stream(&to, matcher);
    while let Some(entry) = diff.next().await {
        charge(graph, 1, entry.path.as_internal_file_string().len())?;
        let mut values = entry.values.map_err(Error::Backend)?;
        values.before = jj_lib::tree_merge::resolve_file_values_bounded(
            store,
            &entry.path,
            values.before,
            diff_budget,
            || graph.diff_active(),
        )
        .await
        .map_err(Error::Resolve)?;
        graph.check().map_err(Error::Source)?;
        if values.is_changed() {
            return Ok(true);
        }
    }
    graph.check().map_err(Error::Source)?;
    Ok(false)
}
