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

/// Physical lexical rows scanned by one candidate page. Selection evaluation
/// belongs to the caller after this synchronous method releases the index lock.
pub const MAX_PREFIX_CANDIDATE_SCAN: usize = 4096;
#[derive(Clone, Copy, Eq, PartialEq)]
enum CandidateKind {
    Commit,
    Change,
}
impl CandidateKind {
    fn tag(self) -> u8 {
        match self {
            Self::Commit => b'n',
            Self::Change => b'c',
        }
    }
    fn prefix_width(self) -> usize {
        match self {
            Self::Commit => 32,
            Self::Change => 16,
        }
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
struct CandidatePrefix {
    lower: [u8; 32],
    upper: [u8; 32],
    nibbles: u8,
}
impl CandidatePrefix {
    fn parse(text: &str, kind: CandidateKind) -> Result<Self> {
        validate_prefix(text, kind.prefix_width() * 2)?;
        let mut value = Self {
            lower: [0; 32],
            upper: [255; 32],
            nibbles: text.len() as u8,
        };
        for (i, byte) in text.bytes().enumerate() {
            let n = if byte <= b'9' {
                byte - b'0'
            } else {
                byte - b'a' + 10
            };
            if i % 2 == 0 {
                value.lower[i / 2] = n << 4;
                value.upper[i / 2] = (n << 4) | 15;
            } else {
                value.lower[i / 2] |= n;
                value.upper[i / 2] = (value.upper[i / 2] & 240) | n;
            }
        }
        Ok(value)
    }
    fn includes_root(self, kind: CandidateKind) -> bool {
        self.lower[..kind.prefix_width()].iter().all(|b| *b == 0)
    }
    fn bound(self, kind: CandidateKind, upper: bool) -> Vec<u8> {
        let width = kind.prefix_width();
        let mut out = if upper {
            self.upper[..width].to_vec()
        } else {
            self.lower[..width].to_vec()
        };
        if kind == CandidateKind::Change {
            out.extend([if upper { 255 } else { 0 }; 32]);
        }
        out
    }
}
#[derive(Clone, Copy)]
enum CandidateKey {
    Commit(m::CommitId),
    Change(m::ChangeId, m::CommitId),
}
impl CandidateKey {
    fn root(kind: CandidateKind) -> Self {
        match kind {
            CandidateKind::Commit => Self::Commit(m::CommitId::ROOT),
            CandidateKind::Change => Self::Change(m::ChangeId::ROOT, m::CommitId::ROOT),
        }
    }
    fn bytes(self) -> Vec<u8> {
        match self {
            Self::Commit(id) => id.0.to_vec(),
            Self::Change(change, id) => {
                let mut out = change.0.to_vec();
                out.extend(id.0);
                out
            }
        }
    }
}
#[derive(Clone)]
struct CandidateCursor {
    scope: Arc<Scope>,
    kind: CandidateKind,
    prefix: CandidatePrefix,
    last: CandidateKey,
}
/// Opaque scope/prefix-bound cursor. It records the last SCANNED commit, not
/// the last eligible token, so empty progress pages never restart a scan.
#[derive(Clone)]
pub struct CommitPrefixCursor(CandidateCursor);
impl CommitPrefixCursor {
    pub fn last_scanned(&self) -> m::CommitId {
        let CandidateKey::Commit(id) = self.0.last else {
            unreachable!("typed commit cursor")
        };
        id
    }
}
/// Ordered change/commit cursor; divergent commits of one change stay distinct.
#[derive(Clone)]
pub struct ChangePrefixCursor(CandidateCursor);
impl ChangePrefixCursor {
    pub fn last_scanned(&self) -> (m::ChangeId, m::CommitId) {
        let CandidateKey::Change(change, id) = self.0.last else {
            unreachable!("typed change cursor")
        };
        (change, id)
    }
}
/// Scoped candidates only, not the result of a caller's additional selection.
/// An empty nonterminal page is progress, never an absent prefix answer.
pub struct LexicalPage<C> {
    pub nodes: Vec<ScopedNode>,
    pub cursor: Option<C>,
    pub scanned: usize,
    pub complete: bool,
}
impl<C> std::fmt::Debug for LexicalPage<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LexicalPage")
            .field("nodes", &self.nodes.len())
            .field("scanned", &self.scanned)
            .field("complete", &self.complete)
            .finish_non_exhaustive()
    }
}
impl<C> LexicalPage<C> {
    fn map_cursor<D>(self, map: impl FnOnce(C) -> D) -> LexicalPage<D> {
        LexicalPage {
            nodes: self.nodes,
            cursor: self.cursor.map(map),
            scanned: self.scanned,
            complete: self.complete,
        }
    }
}
impl Scope {
    pub fn commit_prefix_candidates(
        self: &Arc<Self>,
        prefix: &str,
        cursor: Option<&CommitPrefixCursor>,
        scan_limit: usize,
        budget: &mut WorkBudget,
    ) -> Result<LexicalPage<CommitPrefixCursor>> {
        self.prefix_candidates(
            CandidateKind::Commit,
            prefix,
            cursor.map(|c| &c.0),
            scan_limit,
            budget,
        )
        .map(|page| page.map_cursor(CommitPrefixCursor))
    }
    pub fn change_prefix_candidates(
        self: &Arc<Self>,
        prefix: &str,
        cursor: Option<&ChangePrefixCursor>,
        scan_limit: usize,
        budget: &mut WorkBudget,
    ) -> Result<LexicalPage<ChangePrefixCursor>> {
        self.prefix_candidates(
            CandidateKind::Change,
            prefix,
            cursor.map(|c| &c.0),
            scan_limit,
            budget,
        )
        .map(|page| page.map_cursor(ChangePrefixCursor))
    }
    fn candidate_member(
        &self,
        state: &State,
        id: m::CommitId,
        retained: usize,
        budget: &mut WorkBudget,
    ) -> Result<bool> {
        // Keep page tokens, cursor and this physical candidate in the shared
        // frontier while the existing membership walker uses its own scratch.
        let held = retained.checked_add(2).ok_or(Error::BudgetExceeded)?;
        budget.frontier(held)?;
        let original = budget.max_frontier;
        budget.max_frontier = original.checked_sub(held).ok_or(Error::BudgetExceeded)?;
        let result = self.member(state, id, budget);
        budget.max_frontier = original;
        result
    }
    fn prefix_candidates(
        self: &Arc<Self>,
        kind: CandidateKind,
        text: &str,
        cursor: Option<&CandidateCursor>,
        scan_limit: usize,
        budget: &mut WorkBudget,
    ) -> Result<LexicalPage<CandidateCursor>> {
        use std::ops::Bound::{Excluded, Included};
        if !(1..=MAX_PREFIX_CANDIDATE_SCAN).contains(&scan_limit) {
            return Err(Error::Invalid("prefix candidate scan limit"));
        }
        let prefix = CandidatePrefix::parse(text, kind)?;
        if cursor
            .is_some_and(|c| !Arc::ptr_eq(self, &c.scope) || c.kind != kind || c.prefix != prefix)
        {
            return Err(Error::Invalid(
                "prefix cursor belongs to another scope or prefix",
            ));
        }
        budget.frontier(3)?;
        let state = self.index.state.lock().unwrap();
        if state.failed || state.header.epoch != self.epoch {
            return Err(Error::Unavailable);
        }
        let mut nodes = Vec::new();
        let mut scanned = 0;
        let mut last = cursor.map(|c| c.last);
        if cursor.is_none() && prefix.includes_root(kind) {
            budget.charge(1, 0, 0)?;
            scanned += 1;
            last = Some(CandidateKey::root(kind));
            if self.candidate_member(&state, m::CommitId::ROOT, nodes.len(), budget)? {
                let node = self
                    .read(&state, m::CommitId::ROOT, budget)?
                    .ok_or(Error::Corrupt)?;
                budget.frontier(nodes.len().checked_add(2).ok_or(Error::BudgetExceeded)?)?;
                nodes.push(ScopedNode::new(self, &node));
            }
        }
        if scanned == scan_limit {
            return Ok(LexicalPage {
                nodes,
                cursor: Some(CandidateCursor {
                    scope: self.clone(),
                    kind,
                    prefix,
                    last: last.ok_or(Error::Corrupt)?,
                }),
                scanned,
                complete: false,
            });
        }
        let lower = key(kind.tag(), &self.repo, &prefix.bound(kind, false));
        let upper = key(kind.tag(), &self.repo, &prefix.bound(kind, true));
        let start = if let Some(last) = last {
            let last = key(kind.tag(), &self.repo, &last.bytes());
            if last < lower || last > upper {
                return Err(Error::Invalid("prefix cursor key is outside prefix"));
            }
            Excluded(last)
        } else {
            Included(lower)
        };
        // This is a real exclusive range seek, not prefix iteration followed by
        // skipping all earlier keys. No caller callback executes under the lock.
        budget.charge(1, 0, 0)?;
        let read = state.db.read_tx();
        let mut rows = read.range(&state.table, (start, Included(upper)));
        while scanned < scan_limit {
            let Some(row) = rows.next() else {
                return Ok(LexicalPage {
                    nodes,
                    cursor: None,
                    scanned,
                    complete: true,
                });
            };
            let (row_key, value) = row.into_inner().map_err(storage)?;
            budget.charge(0, 0, (row_key.len() + value.len()) as u64)?;
            let (candidate, node) = match kind {
                CandidateKind::Commit => {
                    if row_key.len() != 65 {
                        return Err(Error::Corrupt);
                    }
                    let id = m::CommitId(row_key[33..].try_into().map_err(|_| Error::Corrupt)?);
                    let node: StoredNode = decode(&row_key, &value, &self.index.limits)?;
                    if node.commit != id.0
                        || node.position == 0
                        || node.generation == 0
                        || node.skips.len() > 64
                    {
                        return Err(Error::Corrupt);
                    }
                    (CandidateKey::Commit(id), node)
                }
                CandidateKind::Change => {
                    if row_key.len() != 81 {
                        return Err(Error::Corrupt);
                    }
                    let change =
                        m::ChangeId(row_key[33..49].try_into().map_err(|_| Error::Corrupt)?);
                    let id = m::CommitId(row_key[49..].try_into().map_err(|_| Error::Corrupt)?);
                    let position: u64 = decode(&row_key, &value, &self.index.limits)?;
                    let node = state
                        .node(&self.repo, id, &self.index.limits, budget)?
                        .ok_or(Error::Corrupt)?;
                    if position != node.position || change.0 != node.change {
                        return Err(Error::Corrupt);
                    }
                    (CandidateKey::Change(change, id), node)
                }
            };
            scanned += 1;
            last = Some(candidate);
            if node.position <= self.high_water
                && self.candidate_member(&state, m::CommitId(node.commit), nodes.len(), budget)?
            {
                budget.frontier(nodes.len().checked_add(2).ok_or(Error::BudgetExceeded)?)?;
                nodes.push(ScopedNode::new(self, &node));
            }
            if scanned < scan_limit {
                budget.charge(1, 0, 0)?;
            }
        }
        Ok(LexicalPage {
            nodes,
            cursor: Some(CandidateCursor {
                scope: self.clone(),
                kind,
                prefix,
                last: last.ok_or(Error::Corrupt)?,
            }),
            scanned,
            complete: false,
        })
    }
}
