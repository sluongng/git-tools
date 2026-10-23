//! Scope-bound graph reads for streaming query operators. Tokens are fixed-size
//! metadata, not canonical-object residency, retention or authorization proofs.
use super::*;

/// Metadata admitted by one immutable logical graph. Keeping a token alive pins
/// its derived scope only; the host must separately retain authoritative roots.
/// It contains no parent vectors, so a frontier cannot multiply decoded fan-in.
#[derive(Clone)]
pub struct ScopedNode {
    scope: Arc<Scope>,
    commit: m::CommitId,
    change: m::ChangeId,
    position: u64,
    generation: u64,
    committer_millis: i64,
    has_conflict: bool,
    size: u64,
}
impl ScopedNode {
    fn new(scope: &Arc<Scope>, node: &StoredNode) -> Self {
        Self {
            scope: scope.clone(),
            commit: m::CommitId(node.commit),
            change: m::ChangeId(node.change),
            position: node.position,
            generation: node.generation,
            committer_millis: node.committer_millis,
            has_conflict: node.has_conflict,
            size: node.size,
        }
    }
    pub fn commit(&self) -> m::CommitId {
        self.commit
    }
    pub fn change_id(&self) -> m::ChangeId {
        self.change
    }
    pub fn position(&self) -> u64 {
        self.position
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn committer_millis(&self) -> i64 {
        self.committer_millis
    }
    pub fn has_conflict(&self) -> bool {
        self.has_conflict
    }
    /// The intrinsic root has no canonical payload despite its synthetic
    /// zero-sized descriptor. Callers requesting payloads must handle it first.
    pub fn descriptor(&self) -> m::Descriptor {
        m::Descriptor {
            key: m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(self.commit.0),
            },
            size_bytes: self.size,
            format_version: m::FORMAT_VERSION,
        }
    }
}

impl Scope {
    /// Validate a literal against this complete frozen graph once. Unknown or
    /// out-of-scope IDs yield None; incomplete coverage and exhausted work error.
    pub fn lookup_node(
        self: &Arc<Self>,
        id: m::CommitId,
        budget: &mut WorkBudget,
    ) -> Result<Option<ScopedNode>> {
        budget.frontier(1)?;
        let state = self.index.state.lock().unwrap();
        if state.failed || state.header.epoch != self.epoch {
            return Err(Error::Unavailable);
        }
        if !self.member(&state, id, budget)? {
            return Ok(None);
        }
        let node = self.read(&state, id, budget)?.ok_or(Error::Corrupt)?;
        Ok(Some(ScopedNode::new(self, &node)))
    }

    /// Read verified parent metadata without repeating a root-membership walk
    /// for every edge. A token from another Scope instance is rejected even if
    /// both instances happen to share roots. Parent order is canonical order.
    ///
    /// This bounds one returned batch. A streaming host must additionally charge
    /// its aggregate retained frontier, cursor and response lifetimes.
    pub fn parent_nodes(
        self: &Arc<Self>,
        token: &ScopedNode,
        budget: &mut WorkBudget,
    ) -> Result<Vec<ScopedNode>> {
        if !Arc::ptr_eq(self, &token.scope) {
            return Err(Error::Invalid("graph token belongs to another scope"));
        }
        let state = self.index.state.lock().unwrap();
        if state.failed || state.header.epoch != self.epoch {
            return Err(Error::Unavailable);
        }
        let node = self
            .read(&state, token.commit, budget)?
            .ok_or(Error::Corrupt)?;
        if node.position != token.position
            || node.generation != token.generation
            || node.change != token.change.0
            || node.committer_millis != token.committer_millis
            || node.has_conflict != token.has_conflict
            || node.size != token.size
        {
            return Err(Error::Corrupt);
        }
        budget.frontier(node.parents.len())?;
        budget.charge(0, node.parents.len() as u64, 0)?;
        let mut parents = Vec::with_capacity(node.parents.len());
        for parent in &node.parents {
            let parent = self
                .read(&state, m::CommitId(*parent), budget)?
                .ok_or(Error::Corrupt)?;
            if parent.position >= node.position || parent.generation >= node.generation {
                return Err(Error::Corrupt);
            }
            parents.push(ScopedNode::new(self, &parent));
        }
        Ok(parents)
    }
}
