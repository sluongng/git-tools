//! Correctness-first bounded provenance queries. No independent authoritative
//! catalog: immutable copy objects are enumerated through the Store API.
use crate::*;
use jj_lib::merge::{Merge, SameChange};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct File {
    path: String,
    id: m::FileId,
    copy: Option<m::CopyId>,
}
struct Budget {
    reads: usize,
    bytes: usize,
    edges: usize,
    contexts: usize,
    path_bytes: usize,
}
impl Budget {
    fn new() -> Self {
        Self {
            reads: 0,
            bytes: 0,
            edges: 0,
            contexts: 0,
            path_bytes: 0,
        }
    }
}
impl NativeBackend {
    async fn query_object(
        &self,
        key: m::ObjectKey,
        budget: &mut Budget,
    ) -> BackendResult<m::Object> {
        let descriptor = self.descriptor(key).await?;
        budget.reads = budget
            .reads
            .checked_add(1)
            .ok_or_else(|| invalid("native query budget exceeded"))?;
        budget.bytes = budget
            .bytes
            .checked_add(usize::try_from(descriptor.size_bytes).map_err(other)?)
            .ok_or_else(|| invalid("native query byte budget exceeded"))?;
        if budget.reads > self.config.max_query_objects
            || budget.bytes > self.config.max_query_metadata_bytes
        {
            return Err(invalid("native query budget exceeded"));
        }
        self.read_object(key).await
    }
    async fn query_commit(&self, id: m::CommitId, budget: &mut Budget) -> BackendResult<m::Commit> {
        if id == m::CommitId::ROOT {
            return Ok(m::synthetic_root());
        }
        match self
            .query_object(
                m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(id.0),
                },
                budget,
            )
            .await?
        {
            m::Object::Commit(value) => Ok(value),
            _ => Err(invalid("native query commit kind mismatch")),
        }
    }
    async fn query_copy(
        &self,
        id: m::CopyId,
        budget: &mut Budget,
    ) -> BackendResult<m::CopyHistory> {
        match self
            .query_object(
                m::ObjectKey {
                    kind: m::Kind::CopyHistory,
                    id: m::ObjectId(id.0),
                },
                budget,
            )
            .await?
        {
            m::Object::CopyHistory(value) => Ok(value),
            _ => Err(invalid("native query copy kind mismatch")),
        }
    }
    fn query_edges(&self, budget: &mut Budget, count: usize) -> BackendResult<()> {
        budget.edges = budget
            .edges
            .checked_add(count)
            .ok_or_else(|| invalid("native query edge budget exceeded"))?;
        if budget.edges > self.config.store.max_closure_edges {
            return Err(invalid("native query edge budget exceeded"));
        }
        Ok(())
    }
    async fn ancestors(
        &self,
        starts: &[m::CommitId],
        budget: &mut Budget,
    ) -> BackendResult<Vec<(m::CommitId, m::Commit)>> {
        let mut enqueued = BTreeSet::new();
        let mut pending = Vec::new();
        let mut commits = BTreeMap::new();
        for id in starts {
            if enqueued.insert(*id) {
                if enqueued.len() > self.config.max_graph_commits {
                    return Err(invalid("native commit graph budget exceeded"));
                }
                pending.push(*id);
            }
        }
        while let Some(id) = pending.pop() {
            let commit = self.query_commit(id, budget).await?;
            self.query_edges(budget, commit.parents.len())?;
            for parent in &commit.parents {
                if enqueued.insert(*parent) {
                    if enqueued.len() > self.config.max_graph_commits {
                        return Err(invalid("native commit graph budget exceeded"));
                    }
                    pending.push(*parent);
                }
            }
            commits.insert(id, commit);
        }
        // Kahn ordering avoids duplicate queued edges/frames and guarantees
        // children precede every parent even for multiple requested heads.
        let mut children = commits
            .keys()
            .map(|id| (*id, 0usize))
            .collect::<BTreeMap<_, _>>();
        for commit in commits.values() {
            for parent in &commit.parents {
                *children.get_mut(parent).expect("ancestor loaded") += 1;
            }
        }
        let mut ready = children
            .iter()
            .filter_map(|(id, count)| (*count == 0).then_some(*id))
            .collect::<BTreeSet<_>>();
        let count = commits.len();
        let mut ordered = Vec::new();
        while let Some(id) = ready.pop_first() {
            let commit = commits.remove(&id).expect("ready commit exists");
            for parent in &commit.parents {
                let count = children.get_mut(parent).expect("parent count exists");
                *count -= 1;
                if *count == 0 {
                    ready.insert(*parent);
                }
            }
            ordered.push((id, commit));
        }
        if ordered.len() != count {
            return Err(invalid("native parent graph cycle"));
        }
        Ok(ordered)
    }
    fn query_path(&self, prefix: &str, name: &str, budget: &mut Budget) -> BackendResult<String> {
        let size = prefix
            .len()
            .checked_add(usize::from(!prefix.is_empty()))
            .and_then(|n| n.checked_add(name.len()))
            .ok_or_else(|| invalid("native query path budget exceeded"))?;
        if size > self.config.store.model_limits.max_path_bytes {
            return Err(invalid("native query path budget exceeded"));
        }
        budget.path_bytes = budget
            .path_bytes
            .checked_add(size)
            .ok_or_else(|| invalid("native query path bytes exceeded"))?;
        if budget.path_bytes > self.config.store.max_path_context_bytes {
            return Err(invalid("native query path bytes exceeded"));
        }
        Ok(if prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{prefix}/{name}")
        })
    }
    fn enqueue_tree(
        &self,
        stack: &mut Vec<(String, Vec<Option<m::TreeId>>)>,
        seen: &mut BTreeMap<String, BTreeSet<Vec<Option<m::TreeId>>>>,
        prefix: String,
        ids: Vec<Option<m::TreeId>>,
        budget: &mut Budget,
    ) -> BackendResult<()> {
        if seen
            .get(&prefix)
            .is_some_and(|values| values.contains(&ids))
        {
            return Ok(());
        }
        budget.contexts = budget
            .contexts
            .checked_add(1)
            .ok_or_else(|| invalid("native query context budget exceeded"))?;
        let bytes = prefix
            .len()
            .checked_mul(2)
            .and_then(|n| ids.len().checked_mul(66).and_then(|ids| n.checked_add(ids)))
            .ok_or_else(|| invalid("native query context bytes exceeded"))?;
        budget.path_bytes = budget
            .path_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid("native query context bytes exceeded"))?;
        if budget.contexts > self.config.store.max_path_contexts
            || budget.path_bytes > self.config.store.max_path_context_bytes
        {
            return Err(invalid("native query context budget exceeded"));
        }
        seen.entry(prefix.clone()).or_default().insert(ids.clone());
        stack.push((prefix, ids));
        Ok(())
    }
    async fn files(&self, commit: &m::Commit, budget: &mut Budget) -> BackendResult<Vec<File>> {
        let mut stack = Vec::new();
        let mut seen = BTreeMap::new();
        let mut files = BTreeSet::new();
        let mut entries = 0usize;
        self.enqueue_tree(
            &mut stack,
            &mut seen,
            String::new(),
            commit.root_tree.terms.iter().map(|id| Some(*id)).collect(),
            budget,
        )?;
        while let Some((prefix, ids)) = stack.pop() {
            // Pinned Merge::simplify scans/removes matching terms and can be
            // quadratic. Charge its worst-case cells before doing sync work.
            self.query_edges(
                budget,
                ids.len()
                    .checked_mul(ids.len())
                    .ok_or_else(|| invalid("native merge work budget exceeded"))?,
            )?;
            let ids = Merge::from_vec(ids).simplify();
            let ids = ids
                .resolve_trivial(SameChange::Accept)
                .map_or_else(|| ids.clone(), |value| Merge::resolved(*value));
            let mut trees = Vec::new();
            let mut names = BTreeSet::new();
            for id in ids.iter() {
                let mut values = BTreeMap::new();
                if let Some(id) = id {
                    let m::Object::Tree(tree) = self
                        .query_object(
                            m::ObjectKey {
                                kind: m::Kind::Tree,
                                id: m::ObjectId(id.0),
                            },
                            budget,
                        )
                        .await?
                    else {
                        return Err(invalid("native query tree kind mismatch"));
                    };
                    self.query_edges(budget, tree.entries.len())?;
                    entries = entries
                        .checked_add(tree.entries.len())
                        .ok_or_else(|| invalid("native tree traversal budget exceeded"))?;
                    if entries > self.config.max_tree_entries {
                        return Err(invalid("native tree traversal budget exceeded"));
                    }
                    for entry in tree.entries {
                        names.insert(entry.name.clone());
                        values.insert(entry.name, query_tree_value(entry.value));
                    }
                }
                trees.push(values);
            }
            for name in names {
                // Bound both N-name × N-term collection and each subsequent
                // quadratic simplify, not just the input tree-entry count.
                self.query_edges(
                    budget,
                    trees
                        .len()
                        .checked_mul(trees.len())
                        .ok_or_else(|| invalid("native merge work budget exceeded"))?,
                )?;
                let values = Merge::from_vec(
                    trees
                        .iter()
                        .map(|tree| tree.get(&name).cloned())
                        .collect::<Vec<_>>(),
                )
                .simplify();
                let values = values
                    .resolve_trivial(SameChange::Accept)
                    .map_or_else(|| values.clone(), |value| Merge::resolved(value.clone()));
                let path = self.query_path(&prefix, &name, budget)?;
                // Directories are recursively merged only when every term is
                // a tree/absence. A directory/file conflict stays at its path.
                if values
                    .iter()
                    .all(|value| matches!(value, None | Some(TreeValue::Tree(_))))
                {
                    let ids = values
                        .iter()
                        .map(|value| match value {
                            Some(TreeValue::Tree(id)) => {
                                model_id(id.as_bytes()).map(m::TreeId).map(Some)
                            }
                            None => Ok(None),
                            _ => unreachable!(),
                        })
                        .collect::<BackendResult<Vec<_>>>()?;
                    if ids.iter().any(Option::is_some) {
                        self.enqueue_tree(&mut stack, &mut seen, path, ids, budget)?;
                    }
                } else {
                    // Negative terms influence simplification/resolution, but
                    // are not live files. Keep positive unresolved candidates.
                    for value in values.adds() {
                        if let Some(TreeValue::File { id, copy_id, .. }) = value {
                            budget.path_bytes = budget
                                .path_bytes
                                .checked_add(path.len())
                                .ok_or_else(|| invalid("native query path bytes exceeded"))?;
                            if budget.path_bytes > self.config.store.max_path_context_bytes {
                                return Err(invalid("native query path bytes exceeded"));
                            }
                            files.insert(File {
                                path: path.clone(),
                                id: m::FileId(model_id(id.as_bytes())?),
                                copy: if copy_id.as_bytes().is_empty() {
                                    None
                                } else {
                                    Some(m::CopyId(model_id(copy_id.as_bytes())?))
                                },
                            });
                        }
                    }
                }
            }
        }
        Ok(files.into_iter().collect())
    }
    pub(crate) async fn related_copies(
        &self,
        requested: m::CopyId,
    ) -> BackendResult<Vec<RelatedCopy>> {
        let _copy_guard = self.copy_gate.clone().lock_owned().await;
        let mut budget = Budget::new();
        self.query_copy(requested, &mut budget).await?;
        let mut copies = BTreeMap::new();
        let mut after = None;
        loop {
            let store = self.store.clone();
            let page = io::blocking(move || {
                store
                    .list_objects(REPOSITORY, m::Kind::CopyHistory, after, 1024)
                    .map_err(other)
            })
            .await?;
            if page.is_empty() {
                break;
            }
            for descriptor in &page {
                let id = m::CopyId(descriptor.key.id.0);
                if copies.len() >= self.config.max_copy_histories {
                    return Err(invalid("native copy catalog budget exceeded"));
                }
                copies.insert(id, self.query_copy(id, &mut budget).await?);
            }
            after = page.last().map(|d| d.key.id);
        }
        // Returning unrelated histories is explicitly permitted by jj. Include
        // the complete bounded local catalog so no descendant is silently lost.
        let mut children = copies
            .keys()
            .map(|id| (*id, 0usize))
            .collect::<BTreeMap<_, _>>();
        for copy in copies.values() {
            self.query_edges(&mut budget, copy.parents.len())?;
            for parent in &copy.parents {
                *children
                    .get_mut(parent)
                    .ok_or_else(|| invalid("native copy parent unavailable"))? += 1;
            }
        }
        let mut ready = children
            .iter()
            .filter_map(|(id, count)| (*count == 0).then_some(*id))
            .collect::<BTreeSet<_>>();
        let count = copies.len();
        let mut ordered = Vec::new();
        while let Some(id) = ready.pop_first() {
            let history = copies.remove(&id).expect("ready copy exists");
            for parent in &history.parents {
                let count = children.get_mut(parent).expect("copy parent exists");
                *count -= 1;
                if *count == 0 {
                    ready.insert(*parent);
                }
            }
            ordered.push(RelatedCopy {
                id: CopyId::from_bytes(&id.0),
                history: convert::copy(history)?,
            });
        }
        if ordered.len() != count {
            return Err(invalid("native copy graph cycle"));
        }
        Ok(ordered)
    }
    pub(crate) async fn copy_records(
        &self,
        paths: Option<Vec<RepoPathBuf>>,
        root: m::CommitId,
        head: m::CommitId,
    ) -> BackendResult<Vec<CopyRecord>> {
        let mut budget = Budget::new();
        let excluded = self
            .ancestors(&[root], &mut budget)
            .await?
            .into_iter()
            .map(|(id, _)| id)
            .collect::<BTreeSet<_>>();
        let commits = self.ancestors(&[head], &mut budget).await?;
        let mut records = Vec::new();
        let mut emitted = BTreeSet::new();
        for (id, commit) in commits {
            if excluded.contains(&id) {
                continue;
            }
            let files = self.files(&commit, &mut budget).await?;
            let mut parent_files = BTreeSet::new();
            for parent in &commit.parents {
                let parent = self.query_commit(*parent, &mut budget).await?;
                parent_files.extend(self.files(&parent, &mut budget).await?);
            }
            for file in files {
                let Some(copy) = file.copy else {
                    continue;
                };
                if paths.as_ref().is_some_and(|paths| {
                    !paths
                        .iter()
                        .any(|path| path.as_internal_file_string() == file.path)
                }) {
                    continue;
                }
                if parent_files
                    .iter()
                    .any(|parent| parent.path == file.path && parent.copy == Some(copy))
                {
                    continue;
                }
                let history = self.query_copy(copy, &mut budget).await?;
                if history.current_path != file.path {
                    return Err(invalid("native copy path disagrees with tree"));
                }
                for source in history.parents {
                    let source_history = self.query_copy(source, &mut budget).await?;
                    let mut found = None;
                    for (ancestor, commit) in self.ancestors(&commit.parents, &mut budget).await? {
                        let candidates = self
                            .files(&commit, &mut budget)
                            .await?
                            .into_iter()
                            .filter(|candidate| {
                                candidate.copy == Some(source)
                                    && candidate.path == source_history.current_path
                            })
                            .collect::<Vec<_>>();
                        if !candidates.is_empty() {
                            found = Some((ancestor, candidates));
                            break;
                        }
                    }
                    let (ancestor, candidates) = found.ok_or_else(|| {
                        invalid("native copy source is not available in target ancestry")
                    })?;
                    for source_file in candidates {
                        let identity = (
                            id,
                            file.path.clone(),
                            ancestor,
                            source_file.path.clone(),
                            source_file.id,
                        );
                        if !emitted.insert(identity) {
                            continue;
                        }
                        if records.len() >= self.config.max_copy_records {
                            return Err(invalid("native copy record budget exceeded"));
                        }
                        records.push(CopyRecord {
                            target: RepoPathBuf::from_internal_string(&file.path).map_err(other)?,
                            target_commit: CommitId::from_bytes(&id.0),
                            source: RepoPathBuf::from_internal_string(&source_file.path)
                                .map_err(other)?,
                            source_file: FileId::from_bytes(&source_file.id.0),
                            source_commit: CommitId::from_bytes(&ancestor.0),
                        });
                    }
                }
            }
        }
        Ok(records)
    }
}

fn query_tree_value(value: m::TreeValue) -> TreeValue {
    match value {
        m::TreeValue::File {
            id,
            executable,
            copy_id,
            ..
        } => TreeValue::File {
            id: FileId::from_bytes(&id.0),
            executable,
            copy_id: copy_id.map_or_else(CopyId::placeholder, |id| CopyId::from_bytes(&id.0)),
        },
        m::TreeValue::Symlink { id, .. } => TreeValue::Symlink(SymlinkId::from_bytes(&id.0)),
        m::TreeValue::Tree(id) => TreeValue::Tree(TreeId::from_bytes(&id.0)),
    }
}
