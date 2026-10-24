//! Immutable private draft graphs. These records never enter canonical Store or
//! the shared derived Index. Hosts separately retain authoritative remote roots.
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};
use tinytree_index::{ChangeTargets, Error, PrefixResolution, Scope, ScopedNode, WorkBudget};
use tinytree_model as m;

type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Debug)]
pub(crate) struct OverlayLimits {
    pub max_depth: usize,
    pub max_nodes: usize,
    pub max_bytes: usize,
    pub max_parents: usize,
    pub max_edges: usize,
    pub max_commit_bytes: usize,
}
impl Default for OverlayLimits {
    fn default() -> Self {
        Self {
            max_depth: 64,
            max_nodes: 4096,
            max_bytes: 16 * 1024 * 1024,
            max_parents: 4096,
            max_edges: 65_536,
            max_commit_bytes: 256 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum GraphKey {
    Remote(u64),
    Draft(u64),
}
#[derive(Clone)]
enum Parent {
    Remote(ScopedNode),
    Draft(Arc<DraftNode>),
}
struct DraftNode {
    descriptor: m::Descriptor,
    bytes: Option<Arc<[u8]>>,
    change: m::ChangeId,
    parents: Vec<Parent>,
    generation: u64,
    ordinal: u64,
    committer_millis: i64,
    has_conflict: bool,
    // Every escaped draft token keeps its uniquely allocated chunk charged.
    _owner: Arc<dyn Any + Send + Sync>,
}
impl Drop for DraftNode {
    fn drop(&mut self) {
        if self.parents.is_empty() {
            return;
        }
        // A tail token may be the final owner of an entire private chain. Drain
        // uniquely owned parents iteratively, not through nested Arc drops.
        let mut pending: Vec<Arc<DraftNode>> = std::mem::take(&mut self.parents)
            .into_iter()
            .filter_map(|parent| match parent {
                Parent::Draft(node) => Some(node),
                Parent::Remote(_) => None,
            })
            .collect();
        let mut owners = vec![self._owner.clone()];
        while let Some(parent) = pending.pop() {
            if let Some(mut node) = Arc::into_inner(parent) {
                if !owners.iter().any(|owner| Arc::ptr_eq(owner, &node._owner)) {
                    owners.push(node._owner.clone());
                }
                for parent in std::mem::take(&mut node.parents) {
                    if let Parent::Draft(parent) = parent {
                        pending.push(parent);
                    }
                }
                // node now has no private edges; its Drop cannot recurse.
            }
        }
        // Unique chunk reservations remain alive throughout physical teardown.
        drop(owners);
    }
}
struct OverlayChunk {
    nodes: BTreeMap<m::CommitId, Arc<DraftNode>>,
    changes: BTreeMap<m::ChangeId, Vec<m::CommitId>>,
    _owner: Arc<dyn Any + Send + Sync>,
}
/// A node is a sealed remote scope proof or one immutable private draft. No
/// cloned parent vectors are retained in query frontiers.
#[derive(Clone)]
pub(crate) struct GraphNode(Parent);
impl GraphNode {
    pub fn commit(&self) -> m::CommitId {
        match &self.0 {
            Parent::Remote(node) => node.commit(),
            Parent::Draft(node) => m::CommitId(node.descriptor.key.id.0),
        }
    }
    pub fn change_id(&self) -> m::ChangeId {
        match &self.0 {
            Parent::Remote(node) => node.change_id(),
            Parent::Draft(node) => node.change,
        }
    }
    pub fn key(&self) -> GraphKey {
        match &self.0 {
            Parent::Remote(node) => GraphKey::Remote(node.position()),
            Parent::Draft(node) => GraphKey::Draft(node.ordinal),
        }
    }
    pub fn generation(&self) -> u64 {
        match &self.0 {
            Parent::Remote(node) => node.generation(),
            Parent::Draft(node) => node.generation,
        }
    }
    pub fn committer_millis(&self) -> i64 {
        match &self.0 {
            Parent::Remote(node) => node.committer_millis(),
            Parent::Draft(node) => node.committer_millis,
        }
    }
    pub fn has_conflict(&self) -> bool {
        match &self.0 {
            Parent::Remote(node) => node.has_conflict(),
            Parent::Draft(node) => node.has_conflict,
        }
    }
    pub fn is_frozen(&self) -> bool {
        matches!(&self.0, Parent::Draft(node) if node.bytes.is_none())
    }
    pub fn descriptor(&self) -> m::Descriptor {
        match &self.0 {
            Parent::Remote(node) => node.descriptor(),
            Parent::Draft(node) => node.descriptor,
        }
    }
}
pub(crate) struct OverlayGraph {
    remote: Arc<Scope>,
    base: Option<Arc<OverlayGraph>>,
    chunk: Option<OverlayChunk>,
    depth: usize,
    nodes: usize,
    bytes: usize,
    edges: usize,
    ordinal_end: u64,
    digest: [u8; 32],
}
impl OverlayGraph {
    pub fn remote(remote: Arc<Scope>) -> Arc<Self> {
        let mut digest = blake3::Hasher::new();
        digest.update(b"tinytree-private-graph-v1\0");
        digest.update(&(remote.repository().len() as u64).to_le_bytes());
        digest.update(remote.repository().as_bytes());
        digest.update(&(remote.roots().len() as u64).to_le_bytes());
        for root in remote.roots() {
            digest.update(&root.0);
        }
        Arc::new(Self {
            remote,
            base: None,
            chunk: None,
            depth: 0,
            nodes: 0,
            bytes: 0,
            edges: 0,
            ordinal_end: 0,
            digest: *digest.finalize().as_bytes(),
        })
    }
    pub fn remote_scope(&self) -> &Arc<Scope> {
        &self.remote
    }
    pub fn depth(&self) -> usize {
        self.depth
    }
    pub fn node_count(&self) -> usize {
        self.nodes
    }
    pub fn byte_count(&self) -> usize {
        self.bytes
    }
    pub fn edge_count(&self) -> usize {
        self.edges
    }
    pub fn ordinal_end(&self) -> u64 {
        self.ordinal_end
    }
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn extend(
        base: Arc<Self>,
        commits: Vec<(m::Descriptor, Arc<[u8]>)>,
        limits: &OverlayLimits,
        budget: &mut WorkBudget,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<Arc<Self>> {
        let count = base
            .nodes
            .checked_add(commits.len())
            .ok_or(Error::BudgetExceeded)?;
        let bytes = commits.iter().try_fold(base.bytes, |sum, (_, bytes)| {
            sum.checked_add(bytes.len()).ok_or(Error::BudgetExceeded)
        })?;
        if count > limits.max_nodes
            || bytes > limits.max_bytes
            || base.edges > limits.max_edges
            || base
                .depth
                .checked_add(usize::from(!commits.is_empty()))
                .ok_or(Error::BudgetExceeded)?
                > limits.max_depth
        {
            return Err(Error::BudgetExceeded);
        }
        if commits.is_empty() {
            return Ok(base);
        }
        let model = m::Limits {
            max_object_bytes: limits.max_commit_bytes,
            max_items: limits.max_parents,
            max_field_bytes: limits.max_commit_bytes,
            max_path_bytes: limits.max_commit_bytes,
        };
        let mut nodes: BTreeMap<m::CommitId, Arc<DraftNode>> = BTreeMap::new();
        let mut changes: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let mut edges = base.edges;
        let mut ordinal = base.ordinal_end;
        let mut digest = blake3::Hasher::new();
        digest.update(b"tinytree-private-chunk-v1\0");
        digest.update(&base.digest);
        digest.update(&(commits.len() as u64).to_le_bytes());
        for (descriptor, bytes) in commits {
            if bytes.len() > limits.max_commit_bytes {
                return Err(Error::BudgetExceeded);
            }
            budget.charge(1, 0, bytes.len() as u64)?;
            if descriptor.key.kind != m::Kind::Commit || descriptor.key.id.0 == [0; 32] {
                return Err(Error::Invalid(
                    "private overlay requires non-root canonical commits",
                ));
            }
            m::verify(&descriptor, &bytes, model)?;
            let m::Object::Commit(commit) = m::decode(&bytes, model)? else {
                return Err(Error::Invalid("private overlay commit type"));
            };
            let encoded = m::encode(&m::Object::Commit(commit.clone()), model)?;
            if encoded.descriptor != descriptor || encoded.bytes.as_slice() != bytes.as_ref() {
                return Err(Error::Invalid("noncanonical private commit"));
            }
            let id = m::CommitId(descriptor.key.id.0);
            if nodes.contains_key(&id) || base.lookup_node(id, budget)?.is_some() {
                return Err(Error::Invalid("duplicate private overlay commit"));
            }
            edges = edges
                .checked_add(commit.parents.len())
                .ok_or(Error::BudgetExceeded)?;
            if edges > limits.max_edges {
                return Err(Error::BudgetExceeded);
            }
            budget.charge(0, commit.parents.len() as u64, 0)?;
            budget.frontier(commit.parents.len())?;
            let mut parents = Vec::with_capacity(commit.parents.len());
            let mut generation = 0;
            for parent in &commit.parents {
                let value = if let Some(node) = nodes.get(parent) {
                    charge_draft(node, budget)?;
                    Parent::Draft(Arc::clone(node))
                } else {
                    base.lookup_node(*parent, budget)?
                        .ok_or(Error::Invalid(
                            "overlay parent is not in the earlier logical graph",
                        ))?
                        .0
                };
                generation = generation.max(GraphNode(value.clone()).generation());
                parents.push(value);
            }
            ordinal = ordinal.checked_add(1).ok_or(Error::BudgetExceeded)?;
            generation = generation.checked_add(1).ok_or(Error::Corrupt)?;
            digest.update(&descriptor.key.id.0);
            digest.update(&descriptor.size_bytes.to_le_bytes());
            digest.update(&descriptor.format_version.to_le_bytes());
            let change = commit.change_id;
            nodes.insert(
                id,
                Arc::new(DraftNode {
                    descriptor,
                    bytes: Some(bytes),
                    change,
                    parents,
                    generation,
                    ordinal,
                    committer_millis: commit.committer.timestamp.millis_since_epoch,
                    has_conflict: commit.root_tree.terms.len() > 1,
                    _owner: owner.clone(),
                }),
            );
            changes.entry(change).or_default().push(id);
        }
        // This is CPU work over the already admitted immutable batch, not
        // additional metadata probes. Keep per-change lexical seeks reusable.
        for ids in changes.values_mut() {
            ids.sort_unstable();
        }
        let graph = Self {
            remote: base.remote.clone(),
            depth: base.depth + 1,
            nodes: count,
            bytes,
            edges,
            ordinal_end: ordinal,
            digest: *digest.finalize().as_bytes(),
            base: Some(base),
            chunk: Some(OverlayChunk {
                nodes,
                changes,
                _owner: owner,
            }),
        };
        Ok(Arc::new(graph))
    }
    /// Prospective graph+topology scratch admission, separate from the compact
    /// Store records. No canonical commit payload is retained by this graph.
    pub fn frozen_memory(
        guard: &tinytree_store::CloudSnapshotReadGuard,
        limits: &OverlayLimits,
    ) -> Result<usize> {
        let records = guard.frozen_commits();
        let edges = records.iter().try_fold(0usize, |sum, record| {
            if record.parents.len() > limits.max_parents {
                return Err(Error::BudgetExceeded);
            }
            sum.checked_add(record.parents.len())
                .ok_or(Error::BudgetExceeded)
        })?;
        if edges > limits.max_edges {
            return Err(Error::BudgetExceeded);
        }
        records
            .len()
            .checked_mul(1024)
            .and_then(|n| n.checked_add(edges.checked_mul(128)?))
            .and_then(|n| n.checked_add(4096))
            .ok_or(Error::BudgetExceeded)
    }
    pub fn from_retained_baseline(
        base: Arc<Self>,
        guard: &tinytree_store::CloudSnapshotReadGuard,
        limits: &OverlayLimits,
        budget: &mut WorkBudget,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<Arc<Self>> {
        Self::frozen_memory(guard, limits)?;
        let records = guard.frozen_commits();
        if records.is_empty() {
            return Ok(base);
        }
        let mut order: Vec<_> = (0..records.len()).collect();
        order.sort_by_key(|index| (records[*index].order, records[*index].descriptor.key.id));
        let mut state = vec![0u8; records.len()];
        let mut nodes = BTreeMap::<m::CommitId, Arc<DraftNode>>::new();
        let mut changes = BTreeMap::<m::ChangeId, Vec<m::CommitId>>::new();
        let mut ordinal = base.ordinal_end;
        let mut edges = base.edges;
        let mut digest = blake3::Hasher::new();
        digest.update(b"tinytree-retained-baseline-v1\0");
        digest.update(&base.digest);
        let edge_bound = records.iter().try_fold(0usize, |sum, record| {
            sum.checked_add(record.parents.len())
                .ok_or(Error::BudgetExceeded)
        })?;
        let work_bound = records
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(edge_bound))
            .ok_or(Error::BudgetExceeded)?;
        let mut steps = 0usize;
        for start in order {
            let mut pending = vec![(start, false, 0usize)];
            while let Some((index, exit, depth)) = pending.pop() {
                steps = steps.checked_add(1).ok_or(Error::BudgetExceeded)?;
                if steps > work_bound
                    || depth > records.len()
                    || pending.len() > records.len().saturating_add(limits.max_parents)
                {
                    return Err(Error::BudgetExceeded);
                }
                if state[index] == 2 {
                    continue;
                }
                let record = &records[index];
                let id = m::CommitId(record.descriptor.key.id.0);
                if !exit {
                    budget.charge(1, 0, 0)?;
                    if let Some(remote) = base.lookup_node(id, budget)? {
                        if remote.descriptor() != record.descriptor {
                            return Err(Error::Corrupt);
                        }
                        state[index] = 2;
                        continue;
                    }
                    if state[index] == 1 {
                        return Err(Error::Corrupt);
                    }
                    state[index] = 1;
                    if pending
                        .len()
                        .checked_add(record.parents.len())
                        .and_then(|n| n.checked_add(1))
                        .is_none_or(|n| {
                            n > records
                                .len()
                                .saturating_add(limits.max_parents)
                                .saturating_add(1)
                        })
                    {
                        return Err(Error::BudgetExceeded);
                    }
                    pending.push((index, true, depth));
                    for parent in record.parents.iter().rev() {
                        if let Ok(parent_index) = records.binary_search_by_key(parent, |record| {
                            m::CommitId(record.descriptor.key.id.0)
                        }) {
                            if state[parent_index] != 2 {
                                pending.push((parent_index, false, depth + 1));
                            }
                        }
                    }
                    continue;
                }
                if record.descriptor.key.kind != m::Kind::Commit || id == m::CommitId::ROOT {
                    return Err(Error::Corrupt);
                }
                if nodes
                    .len()
                    .checked_add(base.nodes)
                    .is_none_or(|count| count >= limits.max_nodes)
                {
                    return Err(Error::BudgetExceeded);
                }
                edges = edges
                    .checked_add(record.parents.len())
                    .filter(|edges| *edges <= limits.max_edges)
                    .ok_or(Error::BudgetExceeded)?;
                budget.charge(0, record.parents.len() as u64, 0)?;
                budget.frontier(record.parents.len())?;
                let mut parents = Vec::with_capacity(record.parents.len());
                let mut generation = 0u64;
                for parent in &record.parents {
                    let parent = if let Some(node) = nodes.get(parent) {
                        Parent::Draft(node.clone())
                    } else {
                        base.lookup_node(*parent, budget)?
                            .ok_or(Error::Invalid(
                                "frozen baseline parent is outside exact anchor graph",
                            ))?
                            .0
                    };
                    generation = generation.max(GraphNode(parent.clone()).generation());
                    parents.push(parent);
                }
                ordinal = ordinal.checked_add(1).ok_or(Error::BudgetExceeded)?;
                generation = generation.checked_add(1).ok_or(Error::Corrupt)?;
                digest.update(&id.0);
                digest.update(&record.order.to_le_bytes());
                nodes.insert(
                    id,
                    Arc::new(DraftNode {
                        descriptor: record.descriptor,
                        bytes: None,
                        change: record.change,
                        parents,
                        generation,
                        ordinal,
                        committer_millis: record.committer_millis,
                        has_conflict: record.has_conflict,
                        _owner: owner.clone(),
                    }),
                );
                changes.entry(record.change).or_default().push(id);
                state[index] = 2;
            }
        }
        if nodes.is_empty() {
            return Ok(base);
        }
        if base
            .depth
            .checked_add(1)
            .is_none_or(|depth| depth > limits.max_depth)
        {
            return Err(Error::BudgetExceeded);
        }
        for ids in changes.values_mut() {
            ids.sort_unstable();
        }
        Ok(Arc::new(Self {
            remote: base.remote.clone(),
            depth: base.depth + 1,
            nodes: base.nodes + nodes.len(),
            bytes: base.bytes,
            edges,
            ordinal_end: ordinal,
            digest: *digest.finalize().as_bytes(),
            base: Some(base),
            chunk: Some(OverlayChunk {
                nodes,
                changes,
                _owner: owner,
            }),
        }))
    }
    /// One lexical private candidate, without enumerating the overlay. Each
    /// immutable layer contributes at most one range-seek candidate. Evaluation
    /// happens after this method returns; no derived-index lock is held.
    pub fn next_private_commit(
        &self,
        prefix: &str,
        after: Option<m::CommitId>,
        budget: &mut WorkBudget,
    ) -> Result<Option<GraphNode>> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let (lower, upper) = prefix_bounds::<32>(prefix)?;
        let start = after.map(Excluded).unwrap_or(Included(m::CommitId(lower)));
        let end = upper.map(|v| Excluded(m::CommitId(v))).unwrap_or(Unbounded);
        let mut found: Option<Arc<DraftNode>> = None;
        let mut layer = Some(self);
        while let Some(graph) = layer {
            budget.charge(1, 0, 0)?;
            if let Some(chunk) = &graph.chunk {
                if let Some((_, node)) = chunk.nodes.range((start, end)).next() {
                    budget.frontier(1)?;
                    charge_draft(node, budget)?;
                    if found
                        .as_ref()
                        .is_none_or(|old| node.descriptor.key.id < old.descriptor.key.id)
                    {
                        found = Some(node.clone());
                    }
                }
            }
            layer = graph.base.as_deref();
        }
        Ok(found.map(|n| GraphNode(Parent::Draft(n))))
    }
    pub fn next_private_change(
        &self,
        prefix: &str,
        after: Option<(m::ChangeId, m::CommitId)>,
        budget: &mut WorkBudget,
    ) -> Result<Option<GraphNode>> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let (lower, upper) = prefix_bounds::<16>(prefix)?;
        let start = Included(after.map(|p| p.0).unwrap_or(m::ChangeId(lower)));
        let end = upper.map(|v| Excluded(m::ChangeId(v))).unwrap_or(Unbounded);
        let mut found: Option<((m::ChangeId, m::CommitId), Arc<DraftNode>)> = None;
        let mut layer = Some(self);
        while let Some(graph) = layer {
            budget.charge(1, 0, 0)?;
            if let Some(chunk) = &graph.chunk {
                // At most two groups: an exhausted cursor group and its successor.
                for (change, ids) in chunk.changes.range((start, end)).take(2) {
                    budget.charge(1 + (ids.len() as u64).ilog2() as u64, 0, 0)?;
                    let index = after
                        .filter(|p| p.0 == *change)
                        .map(|p| ids.partition_point(|id| *id <= p.1))
                        .unwrap_or(0);
                    if let Some(id) = ids.get(index) {
                        let node = chunk.nodes.get(id).ok_or(Error::Corrupt)?;
                        budget.frontier(1)?;
                        charge_draft(node, budget)?;
                        let key = (*change, *id);
                        if found.as_ref().is_none_or(|old| key < old.0) {
                            found = Some((key, node.clone()));
                        }
                        break;
                    }
                }
            }
            layer = graph.base.as_deref();
        }
        Ok(found.map(|(_, n)| GraphNode(Parent::Draft(n))))
    }
    fn find_draft(
        &self,
        id: m::CommitId,
        budget: &mut WorkBudget,
    ) -> Result<Option<Arc<DraftNode>>> {
        let mut graph = Some(self);
        while let Some(current) = graph {
            if let Some(chunk) = &current.chunk {
                budget.charge(1, 0, 0)?;
                if let Some(node) = chunk.nodes.get(&id) {
                    charge_draft(node, budget)?;
                    return Ok(Some(node.clone()));
                }
            }
            graph = current.base.as_deref();
        }
        Ok(None)
    }
    pub fn lookup_node(
        &self,
        id: m::CommitId,
        budget: &mut WorkBudget,
    ) -> Result<Option<GraphNode>> {
        budget.frontier(1)?;
        if let Some(node) = self.find_draft(id, budget)? {
            return Ok(Some(GraphNode(Parent::Draft(node))));
        }
        Ok(self
            .remote
            .lookup_node(id, budget)?
            .map(|node| GraphNode(Parent::Remote(node))))
    }
    pub fn parent_nodes(
        &self,
        node: &GraphNode,
        budget: &mut WorkBudget,
    ) -> Result<Vec<GraphNode>> {
        match &node.0 {
            Parent::Remote(node) => Ok(self
                .remote
                .parent_nodes(node, budget)?
                .into_iter()
                .map(|node| GraphNode(Parent::Remote(node)))
                .collect()),
            Parent::Draft(node) => {
                let own = self
                    .find_draft(m::CommitId(node.descriptor.key.id.0), budget)?
                    .ok_or(Error::Invalid("draft token is outside this graph"))?;
                if !Arc::ptr_eq(&own, node) {
                    return Err(Error::Invalid("draft token belongs to another graph"));
                }
                budget.frontier(node.parents.len())?;
                budget.charge(0, node.parents.len() as u64, 0)?;
                Ok(node.parents.iter().cloned().map(GraphNode).collect())
            }
        }
    }
    pub fn draft_bytes(
        &self,
        node: &GraphNode,
        budget: &mut WorkBudget,
    ) -> Result<Option<Arc<[u8]>>> {
        if let Parent::Draft(value) = &node.0 {
            let own = self
                .find_draft(node.commit(), budget)?
                .ok_or(Error::Invalid("draft token is outside this graph"))?;
            if !Arc::ptr_eq(&own, value) {
                return Err(Error::Invalid("draft token belongs to another graph"));
            }
            if let Some(bytes) = &value.bytes {
                budget.charge(0, 0, bytes.len() as u64)?;
            }
            Ok(value.bytes.clone())
        } else {
            // None carries no remote payload authorization; canonical reads
            // require the host's independently retained remote anchor.
            Ok(None)
        }
    }
    fn walk_parents(
        &self,
        node: &GraphNode,
        retained: usize,
        budget: &mut WorkBudget,
    ) -> Result<Vec<GraphNode>> {
        // Account both the returned temporary batch and its eventual frontier
        // entries before the parent API allocates its vector.
        let original = budget.max_frontier;
        let available = original
            .checked_sub(retained)
            .ok_or(Error::BudgetExceeded)?
            / 2;
        budget.max_frontier = available;
        let result = self.parent_nodes(node, budget);
        budget.max_frontier = original;
        result
    }
    pub fn has_id(&self, id: m::CommitId, budget: &mut WorkBudget) -> Result<bool> {
        if self.nodes == 0 {
            return self.remote.has_id(id, budget);
        }
        Ok(self.lookup_node(id, budget)?.is_some())
    }
    pub fn is_ancestor(
        &self,
        ancestor: m::CommitId,
        descendant: m::CommitId,
        budget: &mut WorkBudget,
    ) -> Result<bool> {
        if self.nodes == 0 {
            return self.remote.is_ancestor(ancestor, descendant, budget);
        }
        let Some(target) = self.lookup_node(ancestor, budget)? else {
            return Ok(false);
        };
        let Some(start) = self.lookup_node(descendant, budget)? else {
            return Ok(false);
        };
        let mut pending = vec![start];
        let mut visited = HashSet::new();
        while let Some(node) = pending.pop() {
            if !visited.insert(node.commit()) {
                continue;
            }
            budget.frontier(visited.len().saturating_add(pending.len()))?;
            if node.commit() == target.commit() {
                return Ok(true);
            }
            if node.key() <= target.key() || node.generation() <= target.generation() {
                continue;
            }
            if matches!(&node.0, Parent::Remote(_)) {
                if self
                    .remote
                    .is_ancestor(target.commit(), node.commit(), budget)?
                {
                    return Ok(true);
                }
                continue;
            }
            let parents =
                self.walk_parents(&node, visited.len().saturating_add(pending.len()), budget)?;
            budget.frontier(
                visited
                    .len()
                    .saturating_add(pending.len())
                    .saturating_add(parents.len()),
            )?;
            pending.extend(parents);
        }
        Ok(false)
    }
    pub fn heads(
        &self,
        commits: &[m::CommitId],
        budget: &mut WorkBudget,
    ) -> Result<Vec<m::CommitId>> {
        if self.nodes == 0 {
            return self.remote.heads(commits, budget);
        }
        budget.frontier(commits.len())?;
        let values: BTreeSet<_> = commits.iter().copied().collect();
        for id in &values {
            if !self.has_id(*id, budget)? {
                return Err(Error::NotReady);
            }
        }
        let mut result = Vec::new();
        'candidate: for id in &values {
            for other in &values {
                if id != other && self.is_ancestor(*id, *other, budget)? {
                    continue 'candidate;
                }
            }
            result.push(*id);
        }
        Ok(result)
    }
    pub fn common_ancestors(
        &self,
        left: &[m::CommitId],
        right: &[m::CommitId],
        budget: &mut WorkBudget,
    ) -> Result<Vec<m::CommitId>> {
        if self.nodes == 0 {
            return self.remote.common_ancestors(left, right, budget);
        }
        budget.frontier(
            left.len()
                .checked_add(right.len())
                .ok_or(Error::BudgetExceeded)?,
        )?;
        let mut pending: BTreeMap<GraphKey, (GraphNode, u8)> = BTreeMap::new();
        let mut undominated = 0;
        for (ids, flags) in [(left, 1u8), (right, 2u8)] {
            for id in ids {
                let node = self.lookup_node(*id, budget)?.ok_or(Error::NotReady)?;
                queue(&mut pending, &mut undominated, node, flags, 0, budget)?;
            }
        }
        if left.is_empty() || right.is_empty() {
            return Ok(Vec::new());
        }
        let mut result = Vec::new();
        while undominated != 0 {
            if result.is_empty() && pending.keys().all(|key| matches!(key, GraphKey::Remote(_))) {
                let left_count = pending.values().filter(|(_, flags)| flags & 1 != 0).count();
                let right_count = pending.values().filter(|(_, flags)| flags & 2 != 0).count();
                budget.frontier(
                    pending
                        .len()
                        .checked_add(left_count)
                        .and_then(|n| n.checked_add(right_count))
                        .ok_or(Error::BudgetExceeded)?,
                )?;
                let mut left = Vec::with_capacity(left_count);
                let mut right = Vec::with_capacity(right_count);
                for (node, flags) in pending.values() {
                    if flags & 1 != 0 {
                        left.push(node.commit());
                    }
                    if flags & 2 != 0 {
                        right.push(node.commit());
                    }
                }
                budget.frontier(
                    pending
                        .len()
                        .saturating_add(left.len())
                        .saturating_add(right.len()),
                )?;
                return self.remote.common_ancestors(&left, &right, budget);
            }
            let (_, (node, mut flags)) = pending.pop_last().ok_or(Error::Corrupt)?;
            if flags & 4 == 0 {
                undominated -= 1;
            }
            if flags & 3 == 3 && flags & 4 == 0 {
                budget.frontier(pending.len().saturating_add(result.len()).saturating_add(1))?;
                result.push(node.commit());
                flags |= 4;
            }
            if flags & 4 != 0 && undominated == 0 {
                break;
            }
            let parents =
                self.walk_parents(&node, pending.len().saturating_add(result.len()), budget)?;
            budget.frontier(
                pending
                    .len()
                    .saturating_add(result.len())
                    .saturating_add(parents.len()),
            )?;
            for parent in parents {
                if parent.key() >= node.key() {
                    return Err(Error::Corrupt);
                }
                queue(
                    &mut pending,
                    &mut undominated,
                    parent,
                    flags,
                    result.len(),
                    budget,
                )?;
            }
        }
        result.sort();
        Ok(result)
    }
    pub fn resolve_commit_prefix(
        &self,
        prefix: &str,
        budget: &mut WorkBudget,
    ) -> Result<PrefixResolution<m::CommitId>> {
        if self.nodes == 0 {
            return self.remote.resolve_commit_prefix(prefix, budget);
        }
        validate_prefix(prefix, 64)?;
        let mut found = match self.remote.resolve_commit_prefix(prefix, budget)? {
            PrefixResolution::Ambiguous => return Ok(PrefixResolution::Ambiguous),
            PrefixResolution::NoMatch => None,
            PrefixResolution::Single(id) => Some(id),
        };
        let mut graph = Some(self);
        while let Some(current) = graph {
            if let Some(chunk) = &current.chunk {
                budget.charge(1, 0, 0)?;
                for (id, node) in &chunk.nodes {
                    charge_draft(node, budget)?;
                    if hex(&id.0).starts_with(prefix) {
                        if found.is_some_and(|before| before != *id) {
                            return Ok(PrefixResolution::Ambiguous);
                        }
                        found = Some(*id);
                    }
                }
            }
            graph = current.base.as_deref();
        }
        Ok(found.map_or(PrefixResolution::NoMatch, PrefixResolution::Single))
    }
    pub fn resolve_change_prefix(
        &self,
        prefix: &str,
        budget: &mut WorkBudget,
    ) -> Result<PrefixResolution<ChangeTargets>> {
        if self.nodes == 0 {
            return self.remote.resolve_change_prefix(prefix, budget);
        }
        validate_prefix(prefix, 32)?;
        let mut found = match self.remote.resolve_change_prefix(prefix, budget)? {
            PrefixResolution::Ambiguous => return Ok(PrefixResolution::Ambiguous),
            PrefixResolution::NoMatch => None,
            PrefixResolution::Single(targets) => Some(targets),
        };
        let mut graph = Some(self);
        while let Some(current) = graph {
            if let Some(chunk) = &current.chunk {
                budget.charge(1, 0, 0)?;
                for (change, ids) in &chunk.changes {
                    budget.charge(1, 0, 16)?;
                    if !hex(&change.0).starts_with(prefix) {
                        continue;
                    }
                    if found
                        .as_ref()
                        .is_some_and(|before| before.change_id != *change)
                    {
                        return Ok(PrefixResolution::Ambiguous);
                    }
                    let targets = found.get_or_insert_with(|| ChangeTargets {
                        change_id: *change,
                        commits: Vec::new(),
                    });
                    budget.frontier(
                        targets
                            .commits
                            .len()
                            .checked_add(ids.len())
                            .ok_or(Error::BudgetExceeded)?,
                    )?;
                    budget.charge(
                        0,
                        0,
                        (ids.len() as u64)
                            .checked_mul(32)
                            .ok_or(Error::BudgetExceeded)?,
                    )?;
                    targets.commits.extend(ids.iter().copied());
                }
            }
            graph = current.base.as_deref();
        }
        if let Some(targets) = &mut found {
            targets.commits.sort();
            targets.commits.dedup();
        }
        Ok(found.map_or(PrefixResolution::NoMatch, PrefixResolution::Single))
    }
    pub fn shortest_commit_prefix(
        &self,
        id: m::CommitId,
        budget: &mut WorkBudget,
    ) -> Result<usize> {
        if self.nodes == 0 {
            return self.remote.shortest_commit_prefix(id, budget);
        }
        let text = hex(&id.0);
        for length in 0..=64 {
            match self.resolve_commit_prefix(&text[..length], budget)? {
                PrefixResolution::NoMatch => return Ok(length),
                PrefixResolution::Single(found) if found == id => return Ok(length),
                _ => {}
            }
        }
        Err(Error::Corrupt)
    }
    pub fn shortest_change_prefix(
        &self,
        id: m::ChangeId,
        budget: &mut WorkBudget,
    ) -> Result<usize> {
        if self.nodes == 0 {
            return self.remote.shortest_change_prefix(id, budget);
        }
        let text = hex(&id.0);
        for length in 0..=32 {
            match self.resolve_change_prefix(&text[..length], budget)? {
                PrefixResolution::NoMatch => return Ok(length),
                PrefixResolution::Single(found) if found.change_id == id => return Ok(length),
                _ => {}
            }
        }
        Err(Error::Corrupt)
    }
}
fn charge_draft(node: &DraftNode, budget: &mut WorkBudget) -> Result<()> {
    let bytes = std::mem::size_of::<DraftNode>()
        .checked_add(
            node.parents
                .len()
                .checked_mul(std::mem::size_of::<Parent>())
                .ok_or(Error::BudgetExceeded)?,
        )
        .ok_or(Error::BudgetExceeded)?;
    budget.charge(1, 0, bytes as u64)
}
fn queue(
    pending: &mut BTreeMap<GraphKey, (GraphNode, u8)>,
    undominated: &mut usize,
    node: GraphNode,
    flags: u8,
    output: usize,
    budget: &WorkBudget,
) -> Result<()> {
    if let Some((existing, current)) = pending.get_mut(&node.key()) {
        if existing.commit() != node.commit() {
            return Err(Error::Corrupt);
        }
        if *current & 4 == 0 && flags & 4 != 0 {
            *undominated = undominated.checked_sub(1).ok_or(Error::Corrupt)?;
        }
        *current |= flags;
    } else {
        budget.frontier(
            pending
                .len()
                .checked_add(output)
                .and_then(|n| n.checked_add(1))
                .ok_or(Error::BudgetExceeded)?,
        )?;
        if flags & 4 == 0 {
            *undominated = undominated.checked_add(1).ok_or(Error::BudgetExceeded)?;
        }
        pending.insert(node.key(), (node, flags));
    }
    Ok(())
}
fn prefix_bounds<const N: usize>(prefix: &str) -> Result<([u8; N], Option<[u8; N]>)> {
    validate_prefix(prefix, N * 2)?;
    let mut lower = [0; N];
    for (i, byte) in prefix.bytes().enumerate() {
        let nibble = if byte <= b'9' {
            byte - b'0'
        } else {
            byte - b'a' + 10
        };
        lower[i / 2] |= nibble << if i % 2 == 0 { 4 } else { 0 };
    }
    if prefix.is_empty() {
        return Ok((lower, None));
    }
    let mut upper = lower;
    let increment = if prefix.len() % 2 == 1 { 16u16 } else { 1u16 };
    let mut carry = increment;
    for i in (0..prefix.len().div_ceil(2)).rev() {
        let sum = upper[i] as u16 + carry;
        upper[i] = sum as u8;
        carry = sum >> 8;
        if carry == 0 {
            break;
        }
    }
    Ok((lower, (carry == 0).then_some(upper)))
}
fn validate_prefix(prefix: &str, width: usize) -> Result<()> {
    if prefix.len() > width
        || !prefix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::Invalid("lowercase hexadecimal prefix"));
    }
    Ok(())
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tinytree_index::Index;
    fn canonical(parents: &[m::CommitId], change: u8, label: &str) -> (m::Descriptor, Arc<[u8]>) {
        let mut commit = m::synthetic_root();
        commit.parents = parents.to_vec();
        commit.change_id = m::ChangeId([change; 16]);
        commit.description = label.into();
        commit.committer.timestamp.millis_since_epoch = -123;
        let encoded = m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap();
        (encoded.descriptor, encoded.bytes.into())
    }
    fn fixture() -> (
        tempfile::TempDir,
        Arc<Index>,
        Arc<OverlayGraph>,
        m::CommitId,
        m::CommitId,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let index = Index::open(
            directory.path().join("index"),
            &tinytree_index::Limits::default(),
        )
        .unwrap();
        let (descriptor, bytes) = canonical(&[m::CommitId::ROOT], 0x30, "remote base");
        let base = index.ingest("repo", descriptor, &bytes).unwrap().commit;
        let (descriptor, bytes) = canonical(&[m::CommitId::ROOT], 0x31, "unrelated stored commit");
        let unrelated = index.ingest("repo", descriptor, &bytes).unwrap().commit;
        let graph = OverlayGraph::remote(Arc::new(index.scope("repo", &[base]).unwrap()));
        (directory, index, graph, base, unrelated)
    }
    fn extend(
        base: Arc<OverlayGraph>,
        commits: Vec<(m::Descriptor, Arc<[u8]>)>,
    ) -> Arc<OverlayGraph> {
        OverlayGraph::extend(
            base,
            commits,
            &OverlayLimits::default(),
            &mut WorkBudget::default(),
            Arc::new(()),
        )
        .unwrap()
    }
    #[test]
    fn drafts_are_private_parent_first_and_hidden_siblings_affect_prefixes() {
        let (_directory, index, remote, base, unrelated) = fixture();
        let before = index.stats().unwrap();
        let a = canonical(&[base], 0x11, "visible draft");
        let aid = m::CommitId(a.0.key.id.0);
        let hidden = canonical(&[base], 0x11, "hidden divergent draft");
        let hid = m::CommitId(hidden.0.key.id.0);
        let first = extend(remote.clone(), vec![a.clone(), hidden.clone()]);
        assert!(first.has_id(hid, &mut WorkBudget::default()).unwrap());
        let mut expected = vec![aid, hid];
        expected.sort();
        assert_eq!(
            first
                .resolve_change_prefix("1", &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::Single(ChangeTargets {
                change_id: m::ChangeId([0x11; 16]),
                commits: expected
            })
        );
        let child = canonical(&[aid], 0x12, "next draft");
        let child_id = m::CommitId(child.0.key.id.0);
        let fork = extend(first.clone(), vec![child]);
        assert_eq!(fork.node_count(), 3);
        assert_eq!(fork.depth(), 2);
        assert_eq!(
            fork.resolve_change_prefix("1", &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::Ambiguous
        );
        assert_eq!(
            fork.shortest_change_prefix(m::ChangeId([0x11; 16]), &mut WorkBudget::default())
                .unwrap(),
            2
        );
        assert_eq!(
            fork.heads(&[aid, child_id, hid], &mut WorkBudget::default())
                .unwrap(),
            {
                let mut values = vec![child_id, hid];
                values.sort();
                values
            }
        );
        assert_eq!(
            fork.common_ancestors(&[child_id], &[hid], &mut WorkBudget::default())
                .unwrap(),
            vec![base]
        );
        assert!(
            fork.is_ancestor(base, child_id, &mut WorkBudget::default())
                .unwrap()
        );
        assert!(
            !fork
                .is_ancestor(hid, child_id, &mut WorkBudget::default())
                .unwrap()
        );
        assert!(!remote.has_id(aid, &mut WorkBudget::default()).unwrap());
        assert!(!first.has_id(child_id, &mut WorkBudget::default()).unwrap());
        assert!(!fork.has_id(unrelated, &mut WorkBudget::default()).unwrap());
        assert!(index.node("repo", aid).unwrap().is_none());
        assert_eq!(index.stats().unwrap(), before);
        let token = fork
            .lookup_node(aid, &mut WorkBudget::default())
            .unwrap()
            .unwrap();
        assert_eq!(token.committer_millis(), -123);
        assert_eq!(
            fork.draft_bytes(&token, &mut WorkBudget::default())
                .unwrap()
                .unwrap()
                .as_ref(),
            a.1.as_ref()
        );
        assert_eq!(
            fork.parent_nodes(&token, &mut WorkBudget::default())
                .unwrap()[0]
                .commit(),
            base
        );
        assert_ne!(first.digest(), remote.digest());
        assert_ne!(fork.digest(), first.digest());
        assert!(matches!(
            OverlayGraph::extend(
                first.clone(),
                vec![a.clone()],
                &OverlayLimits::default(),
                &mut WorkBudget::default(),
                Arc::new(())
            ),
            Err(Error::Invalid(_))
        ));
        let orphan = canonical(&[unrelated], 0x13, "outside remote scope");
        assert!(matches!(
            OverlayGraph::extend(
                remote.clone(),
                vec![orphan],
                &OverlayLimits::default(),
                &mut WorkBudget::default(),
                Arc::new(())
            ),
            Err(Error::Invalid(_))
        ));
        let later = canonical(&[aid], 0x14, "parent after child");
        assert!(matches!(
            OverlayGraph::extend(
                remote,
                vec![later, a],
                &OverlayLimits::default(),
                &mut WorkBudget::default(),
                Arc::new(())
            ),
            Err(Error::Invalid(_))
        ));
    }
    fn reference(
        graph: &BTreeMap<m::CommitId, Vec<m::CommitId>>,
        ancestor: m::CommitId,
        head: m::CommitId,
    ) -> bool {
        let mut pending = vec![head];
        let mut seen = HashSet::new();
        while let Some(id) = pending.pop() {
            if id == ancestor {
                return true;
            }
            if seen.insert(id) {
                pending.extend(graph.get(&id).into_iter().flatten().copied());
            }
        }
        false
    }
    #[test]
    fn mixed_crisscross_graph_matches_reference_sets_and_exact_shortest_prefixes() {
        let (_directory, _index, remote, base, _unrelated) = fixture();
        let a = canonical(&[base], 0x11, "a");
        let aid = m::CommitId(a.0.key.id.0);
        let b = canonical(&[base], 0x12, "b");
        let bid = m::CommitId(b.0.key.id.0);
        let c = canonical(&[aid, bid], 0x13, "c");
        let cid = m::CommitId(c.0.key.id.0);
        let d = canonical(&[bid, aid], 0x14, "d");
        let did = m::CommitId(d.0.key.id.0);
        let graph = extend(remote, vec![a, b, c, d]);
        let model = BTreeMap::from([
            (m::CommitId::ROOT, vec![]),
            (base, vec![m::CommitId::ROOT]),
            (aid, vec![base]),
            (bid, vec![base]),
            (cid, vec![aid, bid]),
            (did, vec![bid, aid]),
        ]);
        for ancestor in model.keys() {
            for head in model.keys() {
                assert_eq!(
                    graph
                        .is_ancestor(*ancestor, *head, &mut WorkBudget::default())
                        .unwrap(),
                    reference(&model, *ancestor, *head)
                );
            }
        }
        let ids = [aid, bid, cid, did];
        let sets: Vec<Vec<_>> = (0..16)
            .map(|mask| {
                ids.iter()
                    .enumerate()
                    .filter_map(|(bit, id)| (mask & (1 << bit) != 0).then_some(*id))
                    .collect()
            })
            .collect();
        for left in &sets {
            for right in &sets {
                let common: Vec<_> = model
                    .keys()
                    .copied()
                    .filter(|candidate| {
                        left.iter().any(|head| reference(&model, *candidate, *head))
                            && right
                                .iter()
                                .any(|head| reference(&model, *candidate, *head))
                    })
                    .collect();
                let expected: Vec<_> = common
                    .iter()
                    .copied()
                    .filter(|candidate| {
                        !common.iter().any(|other| {
                            candidate != other && reference(&model, *candidate, *other)
                        })
                    })
                    .collect();
                assert_eq!(
                    graph
                        .common_ancestors(left, right, &mut WorkBudget::default())
                        .unwrap(),
                    expected
                );
            }
        }
        let shortest = graph
            .shortest_commit_prefix(aid, &mut WorkBudget::default())
            .unwrap();
        let text = hex(&aid.0);
        assert_eq!(
            graph
                .resolve_commit_prefix(&text[..shortest], &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::Single(aid)
        );
        for length in 0..shortest {
            assert_ne!(
                graph
                    .resolve_commit_prefix(&text[..length], &mut WorkBudget::default())
                    .unwrap(),
                PrefixResolution::Single(aid)
            );
        }
        let mut unknown = aid;
        unknown.0[31] ^= 1;
        while graph.has_id(unknown, &mut WorkBudget::default()).unwrap() {
            unknown.0[30] = unknown.0[30].wrapping_add(1);
        }
        let shortest = graph
            .shortest_commit_prefix(unknown, &mut WorkBudget::default())
            .unwrap();
        assert_eq!(
            graph
                .resolve_commit_prefix(&hex(&unknown.0)[..shortest], &mut WorkBudget::default())
                .unwrap(),
            PrefixResolution::NoMatch
        );
    }
    struct Owner(Arc<AtomicUsize>);
    impl Drop for Owner {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn shared_chunks_keep_unique_quota_until_last_token_and_reject_foreign_tokens() {
        let (_directory, _index, remote, base, _unrelated) = fixture();
        let released = Arc::new(AtomicUsize::new(0));
        let a = canonical(&[base], 1, "a");
        let aid = m::CommitId(a.0.key.id.0);
        let first = OverlayGraph::extend(
            remote.clone(),
            vec![a.clone()],
            &OverlayLimits::default(),
            &mut WorkBudget::default(),
            Arc::new(Owner(released.clone())),
        )
        .unwrap();
        let sibling = extend(remote, vec![a]);
        let token = first
            .lookup_node(aid, &mut WorkBudget::default())
            .unwrap()
            .unwrap();
        assert!(matches!(
            sibling.parent_nodes(&token, &mut WorkBudget::default()),
            Err(Error::Invalid(_))
        ));
        let b = canonical(&[aid], 2, "b");
        let bid = m::CommitId(b.0.key.id.0);
        let fork = OverlayGraph::extend(
            first.clone(),
            vec![b],
            &OverlayLimits::default(),
            &mut WorkBudget::default(),
            Arc::new(Owner(released.clone())),
        )
        .unwrap();
        let last = fork
            .lookup_node(bid, &mut WorkBudget::default())
            .unwrap()
            .unwrap();
        drop(token);
        drop(first);
        drop(fork);
        assert_eq!(released.load(Ordering::SeqCst), 0);
        drop(last);
        assert_eq!(released.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn aggregate_admission_and_local_work_limits_never_become_negative_answers() {
        let (_directory, _index, remote, base, _unrelated) = fixture();
        let a = canonical(&[base], 1, "a");
        let aid = m::CommitId(a.0.key.id.0);
        let first = extend(remote.clone(), vec![a.clone()]);
        let limits = OverlayLimits {
            max_depth: 1,
            ..OverlayLimits::default()
        };
        assert!(matches!(
            OverlayGraph::extend(
                first.clone(),
                vec![canonical(&[aid], 2, "b")],
                &limits,
                &mut WorkBudget::default(),
                Arc::new(())
            ),
            Err(Error::BudgetExceeded)
        ));
        let limits = OverlayLimits {
            max_bytes: a.1.len() - 1,
            ..OverlayLimits::default()
        };
        assert!(matches!(
            OverlayGraph::extend(
                remote.clone(),
                vec![a.clone()],
                &limits,
                &mut WorkBudget::default(),
                Arc::new(())
            ),
            Err(Error::BudgetExceeded)
        ));
        assert!(matches!(
            first.has_id(aid, &mut WorkBudget::new(0, 100, 10000, 100)),
            Err(Error::BudgetExceeded)
        ));
        assert!(matches!(
            first.has_id(aid, &mut WorkBudget::new(100, 100, 0, 100)),
            Err(Error::BudgetExceeded)
        ));
        assert!(matches!(
            first.common_ancestors(&[aid], &[base], &mut WorkBudget::new(1000, 1000, 100000, 1)),
            Err(Error::BudgetExceeded)
        ));
        let mut remote_budget = WorkBudget::default();
        let mut graph_budget = WorkBudget::default();
        assert_eq!(
            remote
                .remote_scope()
                .has_id(base, &mut remote_budget)
                .unwrap(),
            remote.has_id(base, &mut graph_budget).unwrap()
        );
        assert_eq!(
            (remote_budget.nodes_read(), remote_budget.bytes_read()),
            (graph_budget.nodes_read(), graph_budget.bytes_read())
        );
    }
    #[test]
    fn long_private_chain_releases_its_chunk_after_the_last_escaped_tail() {
        let (_directory, _index, remote, base, _unrelated) = fixture();
        let released = Arc::new(AtomicUsize::new(0));
        let mut parent = base;
        let mut commits = Vec::new();
        for ordinal in 0..16_384 {
            let commit = canonical(&[parent], 1, &format!("private chain {ordinal}"));
            parent = m::CommitId(commit.0.key.id.0);
            commits.push(commit);
        }
        let limits = OverlayLimits {
            max_nodes: 16_384,
            ..OverlayLimits::default()
        };
        let graph = OverlayGraph::extend(
            remote,
            commits,
            &limits,
            &mut WorkBudget::default(),
            Arc::new(Owner(released.clone())),
        )
        .unwrap();
        let tail = graph
            .lookup_node(parent, &mut WorkBudget::default())
            .unwrap()
            .unwrap();
        drop(graph);
        assert_eq!(released.load(Ordering::SeqCst), 0);
        drop(tail);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }
}
