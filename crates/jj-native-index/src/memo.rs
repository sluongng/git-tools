//! Optional derived answers, never canonical admission or retention authority.
use super::*;
use std::{collections::VecDeque, sync::Mutex};

const ENTRY_BYTES: usize = 1024;
pub(super) enum Value {
    Fact(q::graph_fact_answer::Answer),
    Change(q::ChangePrefixResult),
}
pub(super) struct Entry {
    pub value: Value,
    _lease: Lease,
}
struct Usage {
    bytes: usize,
    entries: usize,
}
struct Budget {
    usage: Mutex<Usage>,
    bytes: usize,
    entries: usize,
}
struct Lease {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut u = self.budget.usage.lock().unwrap();
        u.bytes -= self.bytes;
        u.entries -= 1;
    }
}
struct State {
    entries: BTreeMap<[u8; 32], Arc<Entry>>,
    fifo: VecDeque<[u8; 32]>,
}
pub(super) struct Memo {
    state: Mutex<State>,
    budget: Arc<Budget>,
}
impl Memo {
    pub fn new(bytes: usize, entries: usize) -> Self {
        // Reserve FIFO backing storage and fixed container geometry up front.
        let entries = entries.min(bytes / (ENTRY_BYTES + 64));
        let overhead = if entries == 0 { 0 } else { entries * 64 + 256 };
        Self {
            state: Mutex::new(State {
                entries: BTreeMap::new(),
                fifo: VecDeque::with_capacity(entries),
            }),
            budget: Arc::new(Budget {
                usage: Mutex::new(Usage {
                    bytes: overhead,
                    entries: 0,
                }),
                bytes,
                entries,
            }),
        }
    }
    pub fn get(&self, key: &[u8; 32]) -> Option<Arc<Entry>> {
        self.state.lock().unwrap().entries.get(key).cloned()
    }
    pub fn insert(&self, key: [u8; 32], bytes: usize, build: impl FnOnce() -> Value) {
        let Some(bytes) = bytes.checked_add(ENTRY_BYTES) else {
            return;
        };
        if bytes > self.budget.bytes || self.budget.entries == 0 {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if state.entries.contains_key(&key) {
            return;
        }
        loop {
            let mut u = self.budget.usage.lock().unwrap();
            if u.entries < self.budget.entries
                && u.bytes
                    .checked_add(bytes)
                    .is_some_and(|n| n <= self.budget.bytes)
            {
                u.bytes += bytes;
                u.entries += 1;
                break;
            }
            drop(u);
            let Some(old) = state.fifo.pop_front() else {
                return;
            };
            state.entries.remove(&old);
        }
        let lease = Lease {
            budget: self.budget.clone(),
            bytes,
        };
        let entry = Arc::new(Entry {
            value: build(),
            _lease: lease,
        });
        state.fifo.push_back(key);
        state.entries.insert(key, entry);
    }
}
pub(super) fn checkpoint(roots: &[m::CommitId], drafts: &[Arc<Draft>]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"native-index-logical-checkpoint-v1\0");
    number(&mut h, roots.len() as u64);
    for root in roots {
        h.update(&root.0);
    }
    number(&mut h, drafts.len() as u64);
    for d in drafts {
        h.update(&d.descriptor.key.id.0);
        number(&mut h, d.descriptor.size_bytes);
        number(&mut h, d.descriptor.format_version as u64);
        h.update(&d.change.0);
        number(&mut h, d.order);
        number(&mut h, d.parents.len() as u64);
        for p in &d.parents {
            h.update(&p.0);
        }
    }
    *h.finalize().as_bytes()
}
fn number(h: &mut blake3::Hasher, n: u64) {
    h.update(&n.to_le_bytes());
}
fn raw(h: &mut blake3::Hasher, bytes: &[u8]) {
    number(h, bytes.len() as u64);
    h.update(bytes);
}
fn commit(h: &mut blake3::Hasher, id: Option<&q::CommitId>) -> Result<(), Error> {
    let id = id.ok_or(Error::Invalid)?;
    if id.value.len() != 32 {
        return Err(Error::Invalid);
    }
    h.update(&id.value);
    Ok(())
}
pub(super) fn fact_key(
    cp: [u8; 32],
    fact: &q::graph_fact::Fact,
    limits: &Limits,
) -> Result<Option<[u8; 32]>, Error> {
    let mut h = blake3::Hasher::new();
    h.update(b"native-index-fact-v1\0");
    h.update(&cp);
    match fact {
        q::graph_fact::Fact::HasId(v) => {
            number(&mut h, 1);
            commit(&mut h, v.commit.as_ref())?;
        }
        q::graph_fact::Fact::IsAncestor(v) => {
            number(&mut h, 2);
            commit(&mut h, v.ancestor.as_ref())?;
            commit(&mut h, v.descendant.as_ref())?;
        }
        q::graph_fact::Fact::Membership(v) => {
            number(&mut h, 3);
            commit(&mut h, v.candidate.as_ref())?;
            let expression = v.expression.as_ref().ok_or(Error::Invalid)?;
            expression_hash(expression, &mut h, limits)?;
        }
        _ => return Ok(None),
    }
    Ok(Some(*h.finalize().as_bytes()))
}
pub(super) fn matches(fact: &q::graph_fact::Fact, answer: &q::graph_fact_answer::Answer) -> bool {
    use q::{graph_fact::Fact as F, graph_fact_answer::Answer as A};
    matches!(
        (fact, answer),
        (F::HasId(_), A::HasId(_))
            | (F::IsAncestor(_), A::IsAncestor(_))
            | (F::Membership(_), A::Membership(_))
    )
}
pub(super) fn change_key(cp: [u8; 32], heads: &[m::CommitId], prefix: &str) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"native-index-change-prefix-v1\0");
    h.update(&cp);
    raw(&mut h, prefix.as_bytes());
    number(&mut h, heads.len() as u64);
    for id in heads {
        h.update(&id.0);
    }
    *h.finalize().as_bytes()
}
fn expression_hash(
    root: &q::GraphExpr,
    h: &mut blake3::Hasher,
    limits: &Limits,
) -> Result<(), Error> {
    use prost::Message as _;
    // The borrowed walk bounds every frontier before growth, before any cache hit.
    let mut todo = vec![(root, 1usize)];
    let mut nodes = 0usize;
    let mut literals = 0usize;
    while let Some((e, depth)) = todo.pop() {
        nodes += 1;
        if nodes > limits.max_expression_nodes || depth > limits.max_expression_depth {
            return Err(Error::Limit);
        }
        if e.version != 1 {
            return Err(Error::Invalid);
        }
        macro_rules! push {
            ($value:expr) => {{
                if todo.len() + nodes >= limits.max_expression_nodes {
                    return Err(Error::Limit);
                }
                todo.push(($value.ok_or(Error::Invalid)?, depth + 1));
            }};
        }
        use q::graph_expr::Expression as E;
        match e.expression.as_ref().ok_or(Error::Invalid)? {
            E::Commits(v) => {
                number(h, 1);
                number(h, v.commits.len() as u64);
                literals = literals.checked_add(v.commits.len()).ok_or(Error::Limit)?;
                if literals > limits.max_query_rows {
                    return Err(Error::Limit);
                }
                for id in &v.commits {
                    commit(h, Some(id))?;
                }
            }
            E::Ancestors(v) => {
                number(h, 2);
                ranges(h, v.generations.as_ref(), v.parents.as_ref())?;
                push!(v.heads.as_deref());
            }
            E::Range(v) => {
                number(h, 3);
                ranges(h, v.generations.as_ref(), v.parents.as_ref())?;
                push!(v.heads.as_deref());
                push!(v.roots.as_deref());
            }
            E::DagRange(v) => {
                number(h, 4);
                ranges(h, v.generations_from_roots.as_ref(), None)?;
                push!(v.heads.as_deref());
                push!(v.roots.as_deref());
            }
            E::Heads(v) | E::Roots(v) | E::HasConflict(v) => {
                number(
                    h,
                    match e.expression.as_ref().unwrap() {
                        E::Heads(_) => 5,
                        E::Roots(_) => 6,
                        _ => 7,
                    },
                );
                push!(v.input.as_deref());
            }
            E::Union(v) | E::Intersection(v) => {
                let intersection = matches!(e.expression, Some(E::Intersection(_)));
                number(h, if intersection { 9 } else { 8 });
                number(h, v.inputs.len() as u64);
                if intersection && v.inputs.is_empty() {
                    return Err(Error::Invalid);
                }
                if v.inputs.len()
                    > limits
                        .max_expression_nodes
                        .saturating_sub(nodes + todo.len())
                {
                    return Err(Error::Limit);
                }
                for child in v.inputs.iter().rev() {
                    todo.push((child, depth + 1));
                }
            }
            E::Difference(v) => {
                number(h, 10);
                push!(v.right.as_deref());
                push!(v.left.as_deref());
            }
            E::ChangedPath(v) => {
                number(h, 12);
                number(h, v.policy_version as u64);
                number(h, u64::from(v.exact_file));
                raw(h, v.path.as_bytes());
                if !matches!(v.policy_version, 1 | 2)
                    || v.path.is_empty()
                    || v.path.len() > 4096
                    || v.path.split('/').count() > limits.max_depth
                    || RepoPathBuf::from_internal_string(&v.path).is_err()
                {
                    return Err(Error::Invalid);
                }
                push!(v.candidates.as_deref());
            }
            E::Latest(v) => {
                number(h, 11);
                number(h, v.count);
                if v.count > limits.max_query_rows as u64 {
                    return Err(Error::Limit);
                }
                push!(v.candidates.as_deref());
            }
        }
    }
    if root.encoded_len() > limits.max_segment_bytes {
        return Err(Error::Limit);
    }
    Ok(())
}
fn ranges(
    h: &mut blake3::Hasher,
    g: Option<&q::GenerationRange>,
    p: Option<&q::ParentRange>,
) -> Result<(), Error> {
    number(h, g.is_some() as u64);
    if let Some(r) = g {
        if r.end_exclusive.is_some_and(|end| end < r.start) {
            return Err(Error::Invalid);
        }
        number(h, r.start);
        number(h, r.end_exclusive.is_some() as u64);
        number(h, r.end_exclusive.unwrap_or(0));
    }
    number(h, p.is_some() as u64);
    if let Some(r) = p {
        if r.end_exclusive.is_some_and(|end| end < r.start) {
            return Err(Error::Invalid);
        }
        number(h, r.start as u64);
        number(h, r.end_exclusive.is_some() as u64);
        number(h, r.end_exclusive.unwrap_or(0) as u64);
    }
    Ok(())
}
