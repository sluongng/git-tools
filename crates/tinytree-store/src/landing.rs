//! Authoritative landing intents and atomic receipts. Authentication and the
//! trusted check runner belong to the embedding service, not these Store APIs.
use super::*;
use tinytree_model::{ChangeId, Signature, Timestamp, TreeId};

#[derive(Clone, Debug)]
pub struct LandingConfig {
    pub max_jobs_per_repository: usize,
    pub max_record_bytes: usize,
    pub max_job_ttl_seconds: i64,
    pub max_validation_metadata_bytes: u64,
    pub candidate: tinytree_landing::Limits,
}
impl Default for LandingConfig {
    fn default() -> Self {
        Self {
            max_jobs_per_repository: 1024,
            max_record_bytes: 256 * 1024,
            max_job_ttl_seconds: 86400,
            max_validation_metadata_bytes: 64 * 1024 * 1024,
            candidate: Default::default(),
        }
    }
}
impl LandingConfig {
    pub fn validate(&self) -> Result<()> {
        self.candidate
            .validate()
            .map_err(|_| Error::Invalid("landing candidate limits"))?;
        if !(1..=100_000).contains(&self.max_jobs_per_repository)
            || !(4096..=1024 * 1024).contains(&self.max_record_bytes)
            || !(1..=7 * 86400).contains(&self.max_job_ttl_seconds)
            || self.max_validation_metadata_bytes == 0
            || self.max_validation_metadata_bytes > 1024 * 1024 * 1024
            || self.candidate.max_stack > 128
        {
            return Err(Error::Invalid("landing configuration"));
        }
        Ok(())
    }
}
/// Stable authenticated-principal scope selected by the embedding service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LandingOwner(pub [u8; 32]);
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingCheckProfile {
    pub name: String,
    pub version: String,
}
#[derive(Clone, Debug)]
pub struct LandingRequest {
    pub idempotency_key: String,
    pub source_base: CommitId,
    pub source_head: CommitId,
    pub reference_name: String,
    pub expected_generation: u64,
    pub expected_head: CommitId,
    /// Frozen, server-selected metadata; the Store never reads a wall clock.
    pub committer: Signature,
    /// Allowlisted by the embedding service, never an arbitrary client command.
    pub check_profile: LandingCheckProfile,
    /// Roots remain durable until an explicit terminal transition, even after
    /// this deadline. Expiry is not inferred from the GC caller's clock.
    pub expires_at: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LandingState {
    Intent,
    Prepared,
    Checked,
    Committed,
    Conflict,
    Unsupported,
    CheckFailed,
    StaleTarget,
    Expired,
}
impl LandingState {
    fn live(self) -> bool {
        matches!(self, Self::Intent | Self::Prepared | Self::Checked)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LandingCandidate {
    pub head: CommitId,
    pub root_tree: TreeId,
    pub rewrites: Vec<tinytree_landing::Rewrite>,
    /// Pure builder metadata/work counters, excluding strict Store closure IO.
    pub stats: tinytree_landing::Stats,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LandingCheckIdentity {
    pub candidate: CommitId,
    pub root_tree: TreeId,
    pub profile: LandingCheckProfile,
}
/// Binding data supplied ONLY by a trusted embedding check runner. A digest is
/// not itself proof that a build ran or that its inputs matched the candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LandingVerdict {
    pub passed: bool,
    pub evidence: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LandingReceipt {
    pub reference: Reference,
    /// Exact ordered relations; unchanged IDs are excluded, not evolution edges.
    pub rewrites: Vec<tinytree_landing::Rewrite>,
    pub evidence: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LandingJob {
    pub id: [u8; 32],
    pub state: LandingState,
    /// Current durable recovery hold, not a permanent residency promise.
    pub roots_retained: bool,
    pub expires_at: i64,
    pub profile: LandingCheckProfile,
    pub candidate: Option<LandingCandidate>,
    pub receipt: Option<LandingReceipt>,
    pub conflict_path: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Person {
    name: String,
    email: String,
    millis: i64,
    offset: i32,
}
impl From<&Signature> for Person {
    fn from(s: &Signature) -> Self {
        Self {
            name: s.name.clone(),
            email: s.email.clone(),
            millis: s.timestamp.millis_since_epoch,
            offset: s.timestamp.tz_offset_minutes,
        }
    }
}
impl Person {
    fn signature(&self) -> Signature {
        Signature {
            name: self.name.clone(),
            email: self.email.clone(),
            timestamp: Timestamp {
                millis_since_epoch: self.millis,
                tz_offset_minutes: self.offset,
            },
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    token: String,
    base: [u8; 32],
    head: [u8; 32],
    reference: String,
    generation: u64,
    target: [u8; 32],
    committer: Person,
    profile: LandingCheckProfile,
    expires_at: i64,
}
impl From<&LandingRequest> for Intent {
    fn from(r: &LandingRequest) -> Self {
        Self {
            token: r.idempotency_key.clone(),
            base: r.source_base.0,
            head: r.source_head.0,
            reference: r.reference_name.clone(),
            generation: r.expected_generation,
            target: r.expected_head.0,
            committer: (&r.committer).into(),
            profile: r.check_profile.clone(),
            expires_at: r.expires_at,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pair {
    old: [u8; 32],
    new: [u8; 32],
    change: [u8; 16],
}
impl From<tinytree_landing::Rewrite> for Pair {
    fn from(r: tinytree_landing::Rewrite) -> Self {
        Self {
            old: r.old.0,
            new: r.new.0,
            change: r.change_id.0,
        }
    }
}
impl Pair {
    fn rewrite(&self) -> tinytree_landing::Rewrite {
        tinytree_landing::Rewrite {
            old: CommitId(self.old),
            new: CommitId(self.new),
            change_id: ChangeId(self.change),
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Prepared {
    head: [u8; 32],
    tree: [u8; 32],
    pairs: Vec<Pair>,
    stats: [u64; 4],
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Verdict {
    passed: bool,
    evidence: [u8; 32],
    binding: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u8,
    owner: [u8; 32],
    fingerprint: [u8; 32],
    frozen: [u8; 32],
    intent: Intent,
    limits: [u64; 12],
    state: LandingState,
    roots_released: bool,
    candidate: Option<Prepared>,
    candidate_digest: Option<[u8; 32]>,
    check: Option<Verdict>,
    reference: Option<StoredReference>,
    conflict: Option<String>,
}
impl Record {
    fn retains_roots(&self) -> bool {
        self.state.live() || (self.state == LandingState::Committed && !self.roots_released)
    }
}
fn limits_record(l: tinytree_landing::Limits) -> [u64; 12] {
    [
        l.max_stack,
        l.max_ancestry,
        l.max_reads,
        l.max_metadata_bytes,
        l.max_work,
        l.max_depth,
        l.max_path_bytes,
        l.max_entries_per_tree,
        l.max_object_bytes,
        l.max_field_bytes,
        l.max_output_objects,
        l.max_output_bytes,
    ]
    .map(|n| n as u64)
}
fn candidate_limits(a: [u64; 12]) -> Result<tinytree_landing::Limits> {
    let mut u = [0usize; 12];
    for (dest, n) in u.iter_mut().zip(a) {
        *dest = usize::try_from(n).map_err(|_| Error::Corrupt("landing limit width"))?;
    }
    let l = tinytree_landing::Limits {
        max_stack: u[0],
        max_ancestry: u[1],
        max_reads: u[2],
        max_metadata_bytes: u[3],
        max_work: u[4],
        max_depth: u[5],
        max_path_bytes: u[6],
        max_entries_per_tree: u[7],
        max_object_bytes: u[8],
        max_field_bytes: u[9],
        max_output_objects: u[10],
        max_output_bytes: u[11],
    };
    l.validate()
        .map_err(|_| Error::Corrupt("landing candidate limits"))?;
    if l.max_stack > 128 {
        return Err(Error::Corrupt("landing stack policy"));
    }
    Ok(l)
}
struct BoundedWriter {
    bytes: Vec<u8>,
    max: usize,
}
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.max.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("landing record limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode_record<T: Serialize>(value: &T, max: usize) -> Result<Vec<u8>> {
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        max,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| Error::Limit("landing record bytes"))?;
    Ok(writer.bytes)
}
fn job_id(repo: &str, owner: LandingOwner, token: &str) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-landing-job-v1\0");
    h.update(&(repo.len() as u64).to_be_bytes());
    h.update(repo.as_bytes());
    h.update(&owner.0);
    h.update(token.as_bytes());
    *h.finalize().as_bytes()
}
fn roots(intent: &Intent) -> impl Iterator<Item = ObjectKey> {
    [intent.base, intent.head, intent.target]
        .into_iter()
        .filter(|id| *id != CommitId::ROOT.0)
        .map(|id| ObjectKey {
            kind: Kind::Commit,
            id: ObjectId(id),
        })
}
fn profile(p: &LandingCheckProfile) -> Result<()> {
    if p.name.is_empty()
        || p.name.len() > 128
        || p.version.is_empty()
        || p.version.len() > 128
        || p.name.contains('\0')
        || p.version.contains('\0')
    {
        return Err(Error::Invalid("landing check profile"));
    }
    Ok(())
}
fn validate_intent(i: &Intent) -> Result<()> {
    request_token(&i.token)?;
    reference_name(&i.reference)?;
    profile(&i.profile)?;
    if i.reference == "_tinytree"
        || i.reference.starts_with("_tinytree/")
        || i.head == CommitId::ROOT.0
        || i.target == CommitId::ROOT.0
        || i.committer.name.len() > 1024
        || i.committer.email.len() > 1024
    {
        return Err(Error::Invalid("landing intent"));
    }
    Ok(())
}
fn fingerprint(i: &Intent, max: usize) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&encode_record(
        &(
            &i.token,
            i.base,
            i.head,
            &i.reference,
            i.generation,
            i.target,
            &i.profile,
        ),
        max,
    )?)
    .as_bytes())
}
fn frozen(i: &Intent, limits: [u64; 12], max: usize) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&encode_record(&(i, limits), max)?).as_bytes())
}
fn candidate_digest(r: &Record, c: &Prepared, max: usize) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&encode_record(&(r.frozen, c, &r.intent.profile), max)?).as_bytes())
}
fn public(id: [u8; 32], r: &Record) -> Result<LandingJob> {
    let candidate = r.candidate.as_ref().map(|c| LandingCandidate {
        head: CommitId(c.head),
        root_tree: TreeId(c.tree),
        rewrites: c.pairs.iter().map(Pair::rewrite).collect(),
        stats: tinytree_landing::Stats {
            metadata_reads: c.stats[0] as usize,
            metadata_bytes: c.stats[1] as usize,
            work: c.stats[2] as usize,
            output_bytes: c.stats[3] as usize,
        },
    });
    let receipt = if let Some(reference) = &r.reference {
        let c = r
            .candidate
            .as_ref()
            .ok_or(Error::Corrupt("landing receipt candidate"))?;
        let check = r
            .check
            .as_ref()
            .ok_or(Error::Corrupt("landing receipt check"))?;
        Some(LandingReceipt {
            reference: super::reference(
                r.intent.reference.clone(),
                Some(&super::encode(reference)?),
            )?,
            rewrites: c
                .pairs
                .iter()
                .filter(|p| p.old != p.new)
                .map(Pair::rewrite)
                .collect(),
            evidence: check.evidence,
        })
    } else {
        None
    };
    Ok(LandingJob {
        id,
        state: r.state,
        roots_retained: r.retains_roots(),
        expires_at: r.intent.expires_at,
        profile: r.intent.profile.clone(),
        candidate,
        receipt,
        conflict_path: r.conflict.clone(),
    })
}

/// Runtime root owner. Entries are registered before an output can be installed;
/// transient GC permits absent roots and subsequently protects present children.
struct WorkGuard {
    inner: Arc<Inner>,
    repo: String,
    id: Option<[u8; 32]>,
    keys: HashSet<ObjectKey>,
}
impl WorkGuard {
    fn add(&mut self, key: ObjectKey) -> Result<()> {
        if self.keys.contains(&key) {
            return Ok(());
        }
        let mut gate = self.inner.lock()?;
        *gate.readers.entry((self.repo.clone(), key)).or_default() += 1;
        self.keys.insert(key);
        Ok(())
    }
}
impl Drop for WorkGuard {
    fn drop(&mut self) {
        if let Ok(mut gate) = self.inner.gate.lock() {
            for key in &self.keys {
                let identity = (self.repo.clone(), *key);
                if let Some(count) = gate.readers.get_mut(&identity) {
                    *count -= 1;
                    if *count == 0 {
                        gate.readers.remove(&identity);
                    }
                }
            }
            if let Some(id) = self.id {
                gate.landing_builders.remove(&(self.repo.clone(), id));
            }
        }
    }
}
impl Inner {
    fn landing_record(&self, repo: &str, id: [u8; 32]) -> Result<Option<Record>> {
        self.db
            .read_tx()
            .get(&self.spaces.landing, key(repo, &id))?
            .map(|bytes| self.decode_landing(repo, id, &bytes))
            .transpose()
    }
    fn decode_landing(&self, repo: &str, id: [u8; 32], bytes: &[u8]) -> Result<Record> {
        if bytes.len() > self.config.landing.max_record_bytes {
            return Err(Error::Corrupt("landing record bytes"));
        }
        let r: Record =
            serde_json::from_slice(bytes).map_err(|_| Error::Corrupt("landing record encoding"))?;
        validate_intent(&r.intent).map_err(|_| Error::Corrupt("landing intent"))?;
        if r.version != 1
            || job_id(repo, LandingOwner(r.owner), &r.intent.token) != id
            || fingerprint(&r.intent, self.config.landing.max_record_bytes)? != r.fingerprint
            || frozen(&r.intent, r.limits, self.config.landing.max_record_bytes)? != r.frozen
        {
            return Err(Error::Corrupt("landing record identity"));
        }
        let limits = candidate_limits(r.limits)?;
        if let Some(c) = &r.candidate {
            if c.head == CommitId::ROOT.0
                || c.pairs.is_empty()
                || c.pairs.len() > limits.max_stack
                || c.pairs.last().is_none_or(|p| p.new != c.head)
            {
                return Err(Error::Corrupt("landing candidate record"));
            }
        }
        match (&r.candidate, r.candidate_digest) {
            (Some(c), Some(d))
                if candidate_digest(&r, c, self.config.landing.max_record_bytes)? == d => {}
            (None, None) => {}
            _ => return Err(Error::Corrupt("landing candidate digest")),
        }
        if let Some(check) = &r.check {
            if r.candidate_digest != Some(check.binding) {
                return Err(Error::Corrupt("landing check binding"));
            }
        }
        if let Some(c) = &r.candidate {
            if c.stats[0] > limits.max_reads as u64
                || c.stats[1] > limits.max_metadata_bytes as u64
                || c.stats[2] > limits.max_work as u64
                || c.stats[3] > limits.max_output_bytes as u64
            {
                return Err(Error::Corrupt("landing candidate stats"));
            }
        }
        if (r.state.live() && r.roots_released)
            || (!r.state.live() && r.state != LandingState::Committed && !r.roots_released)
        {
            return Err(Error::Corrupt("landing root policy state"));
        }
        let valid = match r.state {
            LandingState::Intent => {
                r.candidate.is_none()
                    && r.check.is_none()
                    && r.reference.is_none()
                    && r.conflict.is_none()
            }
            LandingState::Prepared => {
                r.candidate.is_some()
                    && r.check.is_none()
                    && r.reference.is_none()
                    && r.conflict.is_none()
            }
            LandingState::Checked => {
                r.candidate.is_some()
                    && r.check.as_ref().is_some_and(|v| v.passed)
                    && r.reference.is_none()
                    && r.conflict.is_none()
            }
            LandingState::Committed => {
                r.candidate.is_some()
                    && r.check.as_ref().is_some_and(|v| v.passed)
                    && r.reference.is_some()
                    && r.conflict.is_none()
            }
            LandingState::CheckFailed => {
                r.candidate.is_some()
                    && r.check.as_ref().is_some_and(|v| !v.passed)
                    && r.reference.is_none()
                    && r.conflict.is_none()
            }
            LandingState::Conflict => {
                r.candidate.is_none()
                    && r.check.is_none()
                    && r.reference.is_none()
                    && r.conflict
                        .as_ref()
                        .is_some_and(|p| p.len() <= limits.max_path_bytes)
            }
            LandingState::Unsupported => {
                r.candidate.is_none()
                    && r.check.is_none()
                    && r.reference.is_none()
                    && r.conflict.is_none()
            }
            LandingState::StaleTarget => {
                r.candidate.is_some()
                    && r.check.as_ref().is_some_and(|v| v.passed)
                    && r.reference.is_none()
                    && r.conflict.is_none()
            }
            LandingState::Expired => r.reference.is_none(),
        };
        if !valid {
            return Err(Error::Corrupt("landing record state"));
        }
        if let (Some(c), Some(reference)) = (&r.candidate, &r.reference) {
            if reference.generation
                != r.intent
                    .generation
                    .checked_add(1)
                    .ok_or(Error::Corrupt("landing generation"))?
                || reference
                    .commit
                    .as_ref()
                    .map(|d| d.descriptor())
                    .transpose()?
                    .is_none_or(|d| {
                        d.key
                            != ObjectKey {
                                kind: Kind::Commit,
                                id: ObjectId(c.head),
                            }
                    })
            {
                return Err(Error::Corrupt("landing receipt reference"));
            }
        }
        Ok(r)
    }
    fn validate_landing_candidate(&self, repo: &str, r: &Record, bytes: &mut u64) -> Result<()> {
        let Some(candidate) = &r.candidate else {
            return Ok(());
        };
        let mut old_parent = CommitId(r.intent.base);
        let mut new_parent = CommitId(r.intent.target);
        let mut changes = HashSet::new();
        let mut final_tree = None;
        for pair in &candidate.pairs {
            let mut load = |id: [u8; 32]| -> Result<tinytree_model::Commit> {
                let key = ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(id),
                };
                let descriptor = self
                    .descriptor(repo, key)?
                    .ok_or(Error::MissingObject(key))?;
                *bytes = bytes
                    .checked_add(descriptor.size_bytes)
                    .ok_or(Error::Limit("landing validation bytes"))?;
                if *bytes > self.config.landing.max_validation_metadata_bytes
                    || *bytes > self.config.max_retention_validation_bytes
                {
                    return Err(Error::Limit("landing validation bytes"));
                }
                match self.metadata(repo, descriptor)? {
                    tinytree_model::Object::Commit(c) => Ok(c),
                    _ => Err(Error::Corrupt("landing commit kind")),
                }
            };
            let old = load(pair.old)?;
            let new = load(pair.new)?;
            if old.parents != [old_parent]
                || new.parents != [new_parent]
                || old.change_id.0 != pair.change
                || new.change_id != old.change_id
                || !changes.insert(old.change_id)
                || old.author != new.author
                || old.description != new.description
                || old.predecessors != new.predecessors
                || new.committer != r.intent.committer.signature()
                || old.secure_signature.is_some()
                || new.secure_signature.is_some()
                || old.root_tree.terms.len() != 1
                || new.root_tree.terms.len() != 1
            {
                return Err(Error::Corrupt("landing rewrite chain"));
            }
            old_parent = CommitId(pair.old);
            new_parent = CommitId(pair.new);
            final_tree = Some(new.root_tree.terms[0].0);
        }
        if old_parent.0 != r.intent.head
            || new_parent.0 != candidate.head
            || final_tree != Some(candidate.tree)
        {
            return Err(Error::Corrupt("landing candidate roots"));
        }
        Ok(())
    }
    fn landing_write(&self, repo: &str, id: [u8; 32], r: &Record) -> Result<()> {
        // Conservatively reserve Intent -> Expired enum spelling growth, even
        // though releasing the root-policy flag may offset that byte today.
        let reserve = usize::from(r.state == LandingState::Intent);
        let bytes = encode_record(r, self.config.landing.max_record_bytes - reserve)?;
        self.check_landing_record_budget(repo, id, &bytes)?;
        self.check_landing_candidate_budget(Some((repo, id, r)))?;
        // Feature six implies all previous authoritative layouts. The keyspaces
        // are opened at Store initialization; durably create the payload root too.
        if store_feature(
            self.db
                .read_tx()
                .get(&self.spaces.meta, b"format")?
                .as_deref(),
        )? < 6
        {
            create_durable_directories(&self.root.join("cloud"))?;
        }
        let mut tx = self.db.write_tx().durability(Some(PersistMode::SyncAll));
        advance_feature(&mut tx, &self.spaces.meta, 6)?;
        tx.insert(&self.spaces.landing, key(repo, &id), bytes);
        self.commit(tx)
    }
    fn check_landing_record_budget(
        &self,
        repo: &str,
        id: [u8; 32],
        replacement: &[u8],
    ) -> Result<()> {
        let changed = key(repo, &id);
        let prefix = key(repo, b"");
        let mut count = 1usize;
        let mut local_count = 1usize;
        let next = self.decode_landing(repo, id, replacement)?;
        let mut bytes = changed
            .len()
            .checked_add(replacement.len() + usize::from(next.state == LandingState::Intent))
            .ok_or(Error::Limit("landing record scan bytes"))?;
        for item in self.db.read_tx().iter(&self.spaces.landing) {
            let (k, v) = item.into_inner()?;
            if k.as_ref() == changed.as_slice() {
                continue;
            }
            count = count
                .checked_add(1)
                .ok_or(Error::Limit("landing job count"))?;
            if k.starts_with(&prefix) {
                local_count = local_count
                    .checked_add(1)
                    .ok_or(Error::Limit("landing job count"))?;
            }
            let split = k
                .iter()
                .position(|b| *b == 0)
                .ok_or(Error::Corrupt("landing key"))?;
            let owner_repo = std::str::from_utf8(&k[..split])
                .map_err(|_| Error::Corrupt("landing repository"))?;
            let existing = self.decode_landing(
                owner_repo,
                k[split + 1..]
                    .try_into()
                    .map_err(|_| Error::Corrupt("landing key"))?,
                &v,
            )?;
            bytes = bytes
                .checked_add(
                    k.len() + v.len() + usize::from(existing.state == LandingState::Intent),
                )
                .ok_or(Error::Limit("landing record scan bytes"))?;
            if local_count > self.config.landing.max_jobs_per_repository
                || count > self.config.max_retention_validation_records
                || bytes as u64 > self.config.max_retention_validation_bytes
            {
                return Err(Error::Limit("landing record scan budget"));
            }
        }
        if local_count > self.config.landing.max_jobs_per_repository
            || count > self.config.max_retention_validation_records
            || bytes as u64 > self.config.max_retention_validation_bytes
        {
            return Err(Error::Limit("landing record scan budget"));
        }
        Ok(())
    }
    fn check_landing_candidate_budget(
        &self,
        replacement: Option<(&str, [u8; 32], &Record)>,
    ) -> Result<()> {
        let changed = replacement.map(|(repo, id, _)| key(repo, &id));
        let mut bytes = 0u64;
        for item in self.db.read_tx().iter(&self.spaces.landing) {
            let (k, v) = item.into_inner()?;
            if changed.as_deref() == Some(k.as_ref()) {
                continue;
            }
            let split = k
                .iter()
                .position(|b| *b == 0)
                .ok_or(Error::Corrupt("landing key"))?;
            let repo = std::str::from_utf8(&k[..split])
                .map_err(|_| Error::Corrupt("landing repository"))?;
            let id = k[split + 1..]
                .try_into()
                .map_err(|_| Error::Corrupt("landing key"))?;
            let r = self.decode_landing(repo, id, &v)?;
            if r.retains_roots() {
                self.validate_landing_candidate(repo, &r, &mut bytes)?;
            }
        }
        if let Some((repo, _, r)) = replacement {
            if r.retains_roots() {
                self.validate_landing_candidate(repo, r, &mut bytes)?;
            }
        }
        Ok(())
    }
    fn landing_guard(
        self: &Arc<Self>,
        repo: &str,
        id: Option<[u8; 32]>,
        r: &Intent,
    ) -> Result<WorkGuard> {
        let mut gate = self.lock()?;
        self.repository(repo)?;
        if let Some(id) = id {
            if !gate.landing_builders.insert((repo.to_owned(), id)) {
                return Err(Error::Invalid("landing construction already active"));
            }
        }
        let mut keys = HashSet::new();
        for root in roots(r) {
            keys.insert(root);
        }
        for root in &keys {
            *gate.readers.entry((repo.to_owned(), *root)).or_default() += 1;
        }
        Ok(WorkGuard {
            inner: Arc::clone(self),
            repo: repo.to_owned(),
            id,
            keys,
        })
    }
    pub(super) fn has_landing_state(&self, repo: &str) -> Result<bool> {
        Ok(self
            .db
            .read_tx()
            .prefix(&self.spaces.landing, key(repo, b""))
            .next()
            .is_some())
    }
    /// Bounded roots include committed recovery holds until explicit expiry.
    /// Released historical receipts deliberately retain no object roots.
    pub(super) fn landing_roots(&self, repo: &str) -> Result<HashSet<ObjectKey>> {
        let mut roots_set = HashSet::new();
        let mut count = 0usize;
        let mut total = 0usize;
        for item in self
            .db
            .read_tx()
            .prefix(&self.spaces.landing, key(repo, b""))
        {
            let (k, bytes) = item.into_inner()?;
            count += 1;
            total = total
                .checked_add(k.len() + bytes.len())
                .ok_or(Error::Limit("landing scan bytes"))?;
            if count > self.config.landing.max_jobs_per_repository
                || count > self.config.max_retention_validation_records
                || total as u64 > self.config.max_retention_validation_bytes
            {
                return Err(Error::Limit("landing job scan"));
            }
            let id: [u8; 32] = k
                .get(repo.len() + 1..)
                .ok_or(Error::Corrupt("landing key"))?
                .try_into()
                .map_err(|_| Error::Corrupt("landing key"))?;
            let r = self.decode_landing(repo, id, &bytes)?;
            if r.retains_roots() {
                roots_set.extend(roots(&r.intent));
                if let Some(c) = r.candidate {
                    roots_set.insert(ObjectKey {
                        kind: Kind::Commit,
                        id: ObjectId(c.head),
                    });
                }
            }
            if roots_set.len() > self.config.max_closure_objects {
                return Err(Error::Limit("landing roots"));
            }
        }
        Ok(roots_set)
    }
    pub(super) fn validate_landing_on_reopen(&self) -> Result<()> {
        let mut repos = HashSet::new();
        let mut count = 0usize;
        let mut total = 0usize;
        let mut canonical_bytes = 0u64;
        for item in self.db.read_tx().iter(&self.spaces.landing) {
            let (k, v) = item.into_inner()?;
            count += 1;
            total = total
                .checked_add(k.len() + v.len())
                .ok_or(Error::Limit("landing reopen bytes"))?;
            if count > self.config.max_retention_validation_records
                || total as u64 > self.config.max_retention_validation_bytes
            {
                return Err(Error::Limit("landing reopen scan"));
            }
            let split = k
                .iter()
                .position(|b| *b == 0)
                .ok_or(Error::Corrupt("landing key"))?;
            let repo = std::str::from_utf8(&k[..split])
                .map_err(|_| Error::Corrupt("landing repository"))?;
            self.repository(repo)?;
            let id = k[split + 1..]
                .try_into()
                .map_err(|_| Error::Corrupt("landing key"))?;
            let record = self.decode_landing(repo, id, &v)?;
            if record.retains_roots() {
                self.validate_landing_candidate(repo, &record, &mut canonical_bytes)?;
            }
            repos.insert(repo.to_owned());
        }
        for repo in repos {
            self.check_cloud_gc_union(&repo, &[], None)?;
        }
        Ok(())
    }
}

impl Store {
    /// Historical scoped result only, not a lookup of the current destination or
    /// proof that any terminal candidate/source objects remain resident.
    pub fn landing_job(
        &self,
        repo: &str,
        owner: LandingOwner,
        token: &str,
    ) -> Result<Option<LandingJob>> {
        request_token(token)?;
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let id = job_id(repo, owner, token);
        self.inner
            .landing_record(repo, id)?
            .map(|r| public(id, &r))
            .transpose()
    }
    /// Accept one durable intent. Authentication/write/profile authorization are
    /// embedding responsibilities. `committer` and `expires_at` are used only
    /// for first acceptance; retries retain the original frozen values.
    pub fn begin_landing(
        &self,
        repo: &str,
        owner: LandingOwner,
        request: &LandingRequest,
        now: i64,
    ) -> Result<LandingJob> {
        request_token(&request.idempotency_key)?;
        reference_name(&request.reference_name)?;
        profile(&request.check_profile)?;
        // Bound all strings BEFORE cloning them. Fresh server-generated freeze
        // metadata does not participate in the stable request fingerprint.
        if request.committer.name.len() > 1024 || request.committer.email.len() > 1024 {
            return Err(Error::Limit("landing committer bytes"));
        }
        let intent = Intent::from(request);
        let digest = fingerprint(&intent, self.inner.config.landing.max_record_bytes)?;
        let id = job_id(repo, owner, &request.idempotency_key);
        {
            let _gate = self.inner.lock()?;
            self.inner.repository(repo)?;
            if let Some(r) = self.inner.landing_record(repo, id)? {
                if r.fingerprint != digest {
                    return Err(Error::IdempotencyConflict);
                }
                return public(id, &r);
            }
        }
        validate_intent(&intent)?;
        if intent.expires_at <= now
            || intent.expires_at
                > now
                    .checked_add(self.inner.config.landing.max_job_ttl_seconds)
                    .ok_or(Error::Limit("landing expiry"))?
        {
            return Err(Error::Invalid("landing expiry"));
        }
        let guard = self.inner.landing_guard(repo, None, &intent)?;
        self.inner.closure_bounded(
            repo,
            roots(&intent),
            true,
            true,
            &HashMap::new(),
            &self.inner.config,
            self.inner.config.landing.max_validation_metadata_bytes,
        )?;
        let _gate = self.inner.lock()?;
        if let Some(r) = self.inner.landing_record(repo, id)? {
            if r.fingerprint != digest {
                return Err(Error::IdempotencyConflict);
            }
            return public(id, &r);
        }
        let view = self.inner.db.read_tx();
        let current = super::reference(
            intent.reference.clone(),
            view.get(
                &self.inner.spaces.refs,
                key(repo, intent.reference.as_bytes()),
            )?
            .as_deref(),
        )?;
        if current.generation != intent.generation
            || current.commit.map(|d| d.key)
                != Some(ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(intent.target),
                })
        {
            return Err(Error::StaleReference(current));
        }
        let count = view
            .prefix(&self.inner.spaces.landing, key(repo, b""))
            .take(self.inner.config.landing.max_jobs_per_repository + 1)
            .try_fold(0usize, |n, item| {
                item.into_inner()?;
                Ok::<_, Error>(n + 1)
            })?;
        if count >= self.inner.config.landing.max_jobs_per_repository {
            return Err(Error::Limit("landing accepted job quota"));
        }
        self.inner
            .check_cloud_gc_union(repo, &roots(&intent).collect::<Vec<_>>(), None)?;
        let limits = limits_record(self.inner.config.landing.candidate);
        let r = Record {
            version: 1,
            owner: owner.0,
            fingerprint: digest,
            frozen: frozen(&intent, limits, self.inner.config.landing.max_record_bytes)?,
            intent,
            limits,
            state: LandingState::Intent,
            roots_released: false,
            candidate: None,
            candidate_digest: None,
            check: None,
            reference: None,
            conflict: None,
        };
        self.inner.landing_write(repo, id, &r)?;
        drop(_gate);
        drop(guard);
        public(id, &r)
    }
    /// Build from the persisted immutable intent. Restart retries reconstruct the
    /// same candidate. Physical root ownership outlives concurrent expiry.
    pub fn prepare_landing(
        &self,
        repo: &str,
        owner: LandingOwner,
        token: &str,
        now: i64,
    ) -> Result<LandingJob> {
        request_token(token)?;
        let id = job_id(repo, owner, token);
        let (record, mut guard) = {
            let mut gate = self.inner.lock()?;
            self.inner.repository(repo)?;
            let r = self
                .inner
                .landing_record(repo, id)?
                .ok_or(Error::Invalid("unknown landing job"))?;
            if r.state != LandingState::Intent {
                return public(id, &r);
            }
            if r.intent.expires_at <= now {
                return Err(Error::Invalid("landing requires explicit expiry"));
            }
            if !gate.landing_builders.insert((repo.to_owned(), id)) {
                return Err(Error::Invalid("landing construction already active"));
            }
            let keys: HashSet<_> = roots(&r.intent).collect();
            for root in &keys {
                *gate.readers.entry((repo.to_owned(), *root)).or_default() += 1;
            }
            (
                r,
                WorkGuard {
                    inner: Arc::clone(&self.inner),
                    repo: repo.to_owned(),
                    id: Some(id),
                    keys,
                },
            )
        };
        checkpoint("inputs");
        let mut reader = StoreReader {
            store: self,
            repo,
            error: None,
        };
        let built = tinytree_landing::build(
            &mut reader,
            tinytree_landing::Request {
                source_base: CommitId(record.intent.base),
                source_head: CommitId(record.intent.head),
                destination_head: CommitId(record.intent.target),
                committer: record.intent.committer.signature(),
            },
            candidate_limits(record.limits)?,
        );
        if let Some(error) = reader.error {
            return Err(error);
        }
        let candidate = match built {
            Ok(candidate) => candidate,
            Err(error) => {
                let (state, conflict) = match error {
                    tinytree_landing::Error::Conflict { path } => {
                        (LandingState::Conflict, Some(path))
                    }
                    tinytree_landing::Error::UnsupportedHistory(_)
                    | tinytree_landing::Error::Invalid(_) => (LandingState::Unsupported, None),
                    tinytree_landing::Error::Limit(_) => {
                        return Err(Error::Limit("landing candidate construction"));
                    }
                    tinytree_landing::Error::Object(e) => return Err(e.into()),
                    tinytree_landing::Error::Read { .. } => {
                        return Err(Error::Corrupt("landing reader lost failure"));
                    }
                };
                let _gate = self.inner.lock()?;
                let mut current = self
                    .inner
                    .landing_record(repo, id)?
                    .ok_or(Error::Corrupt("landing job disappeared"))?;
                if current.state == LandingState::Intent {
                    current.state = state;
                    current.roots_released = true;
                    current.conflict = conflict;
                    self.inner.landing_write(repo, id, &current)?;
                }
                return public(id, &current);
            }
        };
        for object in &candidate.objects {
            guard.add(object.descriptor.key)?;
            self.put(repo, object.descriptor, &object.bytes, now)?;
        }
        checkpoint("outputs");
        let candidate_key = ObjectKey {
            kind: Kind::Commit,
            id: ObjectId(candidate.head.0),
        };
        self.inner.closure_bounded(
            repo,
            [candidate_key],
            true,
            true,
            &HashMap::new(),
            &self.inner.config,
            self.inner.config.landing.max_validation_metadata_bytes,
        )?;
        let _gate = self.inner.lock()?;
        let mut current = self
            .inner
            .landing_record(repo, id)?
            .ok_or(Error::Corrupt("landing job disappeared"))?;
        if current.state != LandingState::Intent {
            return public(id, &current);
        }
        self.inner
            .check_cloud_gc_union(repo, &[candidate_key], None)?;
        current.candidate = Some(Prepared {
            head: candidate.head.0,
            tree: candidate.root_tree.0,
            pairs: candidate.rewrites.into_iter().map(Into::into).collect(),
            stats: [
                candidate.stats.metadata_reads,
                candidate.stats.metadata_bytes,
                candidate.stats.work,
                candidate.stats.output_bytes,
            ]
            .map(|n| n as u64),
        });
        current.candidate_digest = Some(candidate_digest(
            &current,
            current.candidate.as_ref().expect("candidate just set"),
            self.inner.config.landing.max_record_bytes,
        )?);
        current.state = LandingState::Prepared;
        self.inner.landing_write(repo, id, &current)?;
        public(id, &current)
    }
    /// Trusted embedding API, never a client-supplied `checks_passed` assertion.
    /// The embedding runner must establish input/invocation correctness. The
    /// Store only verifies exact candidate/root/profile identity and durability.
    pub fn record_landing_check(
        &self,
        repo: &str,
        owner: LandingOwner,
        token: &str,
        identity: &LandingCheckIdentity,
        verdict: LandingVerdict,
        now: i64,
    ) -> Result<LandingJob> {
        request_token(token)?;
        let id = job_id(repo, owner, token);
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let mut r = self
            .inner
            .landing_record(repo, id)?
            .ok_or(Error::Invalid("unknown landing job"))?;
        let c = r
            .candidate
            .as_ref()
            .ok_or(Error::Invalid("landing candidate not prepared"))?;
        if identity.candidate.0 != c.head
            || identity.root_tree.0 != c.tree
            || identity.profile != r.intent.profile
        {
            return Err(Error::Invalid("landing check identity"));
        }
        let supplied = Verdict {
            passed: verdict.passed,
            evidence: verdict.evidence,
            binding: r
                .candidate_digest
                .ok_or(Error::Corrupt("landing candidate digest missing"))?,
        };
        if let Some(existing) = &r.check {
            if *existing != supplied {
                return Err(Error::IdempotencyConflict);
            }
            return public(id, &r);
        }
        if r.state != LandingState::Prepared {
            return Err(Error::Invalid("landing job does not accept checks"));
        }
        if r.intent.expires_at <= now {
            return Err(Error::Invalid("landing requires explicit expiry"));
        }
        self.inner.validate_landing_candidate(repo, &r, &mut 0)?;
        r.check = Some(supplied);
        r.roots_released = !verdict.passed;
        r.state = if verdict.passed {
            LandingState::Checked
        } else {
            LandingState::CheckFailed
        };
        self.inner.landing_write(repo, id, &r)?;
        public(id, &r)
    }
    /// Publish the exact checked candidate and receipt atomically. A stale target
    /// is terminal; its successful old check cannot authorize a rebuilt candidate.
    /// Historical committed replay returns its original result without republish.
    pub fn finalize_landing(
        &self,
        repo: &str,
        owner: LandingOwner,
        token: &str,
        now: i64,
    ) -> Result<LandingJob> {
        request_token(token)?;
        let id = job_id(repo, owner, token);
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let mut r = self
            .inner
            .landing_record(repo, id)?
            .ok_or(Error::Invalid("unknown landing job"))?;
        if !r.state.live() {
            return public(id, &r);
        }
        if r.state != LandingState::Checked {
            return Err(Error::Invalid("landing candidate has no passing check"));
        }
        if r.intent.expires_at <= now {
            return Err(Error::Invalid("landing requires explicit expiry"));
        }
        self.inner.validate_landing_candidate(repo, &r, &mut 0)?;
        let c = r
            .candidate
            .as_ref()
            .ok_or(Error::Corrupt("checked landing candidate"))?;
        let view = self.inner.db.read_tx();
        let ref_key = key(repo, r.intent.reference.as_bytes());
        let current = super::reference(
            r.intent.reference.clone(),
            view.get(&self.inner.spaces.refs, &ref_key)?.as_deref(),
        )?;
        if current.generation != r.intent.generation
            || current.commit.map(|d| d.key)
                != Some(ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(r.intent.target),
                })
        {
            r.state = LandingState::StaleTarget;
            r.roots_released = true;
            self.inner.landing_write(repo, id, &r)?;
            return public(id, &r);
        }
        let target = ObjectKey {
            kind: Kind::Commit,
            id: ObjectId(c.head),
        };
        let descriptor = self
            .inner
            .descriptor(repo, target)?
            .ok_or(Error::MissingObject(target))?;
        self.inner
            .check_cloud_gc_union(repo, &[], Some((&r.intent.reference, target)))?;
        let reference = StoredReference {
            generation: current
                .generation
                .checked_add(1)
                .ok_or(Error::Limit("reference generation"))?,
            commit: Some(descriptor.into()),
        };
        r.state = LandingState::Committed;
        r.roots_released = false;
        r.reference = Some(reference.clone());
        let bytes = encode_record(&r, self.inner.config.landing.max_record_bytes)?;
        self.inner.check_landing_record_budget(repo, id, &bytes)?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        // Both ordered rewrite relations and exact check binding are in this
        // authoritative result record, in the SAME transaction as the reference.
        tx.insert(&self.inner.spaces.refs, ref_key, super::encode(&reference)?);
        tx.insert(&self.inner.spaces.landing, key(repo, &id), bytes);
        self.inner.commit(tx)?;
        public(id, &r)
    }
    /// Explicit expiry policy. Committed receipts keep their result while their
    /// recovery roots are retired. Physical owners survive this transition too.
    pub fn expire_landing(
        &self,
        repo: &str,
        owner: LandingOwner,
        token: &str,
        now: i64,
    ) -> Result<LandingJob> {
        request_token(token)?;
        let id = job_id(repo, owner, token);
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let mut r = self
            .inner
            .landing_record(repo, id)?
            .ok_or(Error::Invalid("unknown landing job"))?;
        if r.retains_roots() {
            if now < r.intent.expires_at {
                return Err(Error::Invalid("landing expiry not reached"));
            }
            if r.state != LandingState::Committed {
                r.state = LandingState::Expired;
            }
            r.roots_released = true;
            self.inner.landing_write(repo, id, &r)?;
        }
        public(id, &r)
    }
}
struct StoreReader<'a> {
    store: &'a Store,
    repo: &'a str,
    error: Option<Error>,
}
impl tinytree_landing::MetadataReader for StoreReader<'_> {
    fn read(
        &mut self,
        key: ObjectKey,
        max_bytes: usize,
    ) -> std::result::Result<Vec<u8>, tinytree_landing::ReadError> {
        let result = (|| {
            {
                let _gate = self.store.inner.lock()?;
                let descriptor = self
                    .store
                    .inner
                    .descriptor(self.repo, key)?
                    .ok_or(Error::MissingObject(key))?;
                if descriptor.size_bytes > max_bytes as u64 {
                    return Err(Error::Limit("landing reader metadata bytes"));
                }
            }
            let lease = self.store.read(self.repo, key)?;
            let mut bytes = Vec::new();
            lease.take(max_bytes as u64 + 1).read_to_end(&mut bytes)?;
            if bytes.len() > max_bytes {
                return Err(Error::Limit("landing reader metadata bytes"));
            }
            Ok(bytes)
        })();
        result.map_err(|error| {
            self.error = Some(error);
            tinytree_landing::ReadError::Unavailable
        })
    }
}
#[cfg(not(test))]
fn checkpoint(_: &str) {}
#[cfg(test)]
thread_local! {static CHECKPOINT: std::cell::RefCell<Option<Box<dyn Fn(&str)>>> = std::cell::RefCell::new(None);}
#[cfg(test)]
fn checkpoint(phase: &str) {
    CHECKPOINT.with(|hook| {
        if let Some(hook) = hook.borrow().as_ref() {
            hook(phase)
        }
    })
}
#[cfg(test)]
pub(super) fn test_checkpoint(hook: Option<Box<dyn Fn(&str)>>) {
    CHECKPOINT.with(|slot| *slot.borrow_mut() = hook);
}

/// Compact discovery result. The opaque handle is only for trusted in-process
/// scheduling; it has no serialized representation or remote authorization role.
#[derive(Debug)]
pub struct PendingLanding {
    pub id: [u8; 32],
    pub state: LandingState,
    pub expires_at: i64,
    pub profile: LandingCheckProfile,
    pub handle: LandingHandle,
}
#[derive(Clone)]
pub struct LandingHandle {
    repository: String,
    owner: LandingOwner,
    token: String,
    store: Weak<Inner>,
}
impl fmt::Debug for LandingHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LandingHandle").finish_non_exhaustive()
    }
}
impl LandingHandle {
    fn scope(&self, store: &Store) -> Result<()> {
        if !Weak::ptr_eq(&self.store, &Arc::downgrade(&store.inner))
            || self.store.upgrade().is_none()
        {
            return Err(Error::Invalid(
                "landing handle belongs to another store instance",
            ));
        }
        Ok(())
    }
    pub fn status(&self, store: &Store) -> Result<LandingJob> {
        self.scope(store)?;
        store
            .landing_job(&self.repository, self.owner, &self.token)?
            .ok_or(Error::Corrupt("landing handle job disappeared"))
    }
    pub fn prepare(&self, store: &Store, now: i64) -> Result<LandingJob> {
        self.scope(store)?;
        store.prepare_landing(&self.repository, self.owner, &self.token, now)
    }
    pub fn pin_candidate(&self, store: &Store, now: i64) -> Result<LandingCandidateLease> {
        self.scope(store)?;
        store.pin_landing_candidate(&self.repository, self.owner, &self.token, now)
    }
    pub fn record_check(
        &self,
        store: &Store,
        identity: &LandingCheckIdentity,
        verdict: LandingVerdict,
        now: i64,
    ) -> Result<LandingJob> {
        self.scope(store)?;
        store.record_landing_check(
            &self.repository,
            self.owner,
            &self.token,
            identity,
            verdict,
            now,
        )
    }
    pub fn finalize(&self, store: &Store, now: i64) -> Result<LandingJob> {
        self.scope(store)?;
        store.finalize_landing(&self.repository, self.owner, &self.token, now)
    }
    pub fn expire(&self, store: &Store, now: i64) -> Result<LandingJob> {
        self.scope(store)?;
        store.expire_landing(&self.repository, self.owner, &self.token, now)
    }
}
#[derive(Clone)]
pub struct LandingScanCursor {
    repository: String,
    after: [u8; 32],
    store: Weak<Inner>,
}
impl fmt::Debug for LandingScanCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LandingScanCursor").finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug)]
pub struct LandingScanLimits {
    pub max_records: usize,
    pub max_scan_bytes: usize,
    pub max_jobs: usize,
    /// Conservative UTF-8 plus fixed-per-handle allowance, not process RSS.
    pub max_output_bytes: usize,
}
impl Default for LandingScanLimits {
    fn default() -> Self {
        Self {
            max_records: 128,
            max_scan_bytes: 1024 * 1024,
            max_jobs: 32,
            max_output_bytes: 64 * 1024,
        }
    }
}
#[derive(Debug)]
pub struct LandingScanPage {
    pub jobs: Vec<PendingLanding>,
    pub next: Option<LandingScanCursor>,
    pub scan_complete: bool,
    pub scanned_records: usize,
    pub scanned_bytes: usize,
}
/// A physical check owner, not a passing-check capability. Hold until all reads
/// and the actual check process end, including cancellation and concurrent expiry.
pub struct LandingCandidateLease {
    identity: LandingCheckIdentity,
    _root: RootReadGuard,
}
impl LandingCandidateLease {
    pub fn identity(&self) -> &LandingCheckIdentity {
        &self.identity
    }
}
impl Store {
    /// Trusted embedding discovery, not a user listing/authorization API. Pages
    /// advance over terminal records without cloning their candidate lists.
    /// This is not a snapshot: concurrent inserts before the cursor require a
    /// later FULL scan. Restarted owners start fresh; old handles/cursors fail.
    pub fn scan_pending_landings(
        &self,
        repo: &str,
        after: Option<&LandingScanCursor>,
        limits: LandingScanLimits,
    ) -> Result<LandingScanPage> {
        use std::ops::Bound;
        if limits.max_records == 0
            || limits.max_records > 4096
            || limits.max_jobs == 0
            || limits.max_jobs > 1024
            || limits.max_scan_bytes == 0
            || limits.max_scan_bytes > 16 * 1024 * 1024
            || limits.max_output_bytes == 0
            || limits.max_output_bytes > 16 * 1024 * 1024
        {
            return Err(Error::Invalid("landing scan limits"));
        }
        if let Some(cursor) = after {
            if cursor.repository != repo
                || !Weak::ptr_eq(&cursor.store, &Arc::downgrade(&self.inner))
                || cursor.store.upgrade().is_none()
            {
                return Err(Error::Invalid("landing scan cursor scope"));
            }
        }
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let lower = after
            .map(|c| Bound::Excluded(key(repo, &c.after)))
            .unwrap_or_else(|| Bound::Included(key(repo, b"")));
        let mut upper = repo.as_bytes().to_vec();
        upper.push(1);
        let view = self.inner.db.read_tx();
        let mut rows = view.range(&self.inner.spaces.landing, (lower, Bound::Excluded(upper)));
        let mut jobs = Vec::new();
        let mut scanned = 0usize;
        let mut bytes = 0usize;
        let mut output = 0usize;
        let mut last = None;
        let mut complete = false;
        while scanned < limits.max_records && jobs.len() < limits.max_jobs {
            let Some(item) = rows.next() else {
                complete = true;
                break;
            };
            let (k, v) = item.into_inner()?;
            let size = k
                .len()
                .checked_add(v.len())
                .ok_or(Error::Limit("landing scan bytes"))?;
            if size > limits.max_scan_bytes - bytes {
                if scanned == 0 {
                    return Err(Error::Limit("landing scan row bytes"));
                }
                break;
            }
            let id: [u8; 32] = k
                .get(repo.len() + 1..)
                .ok_or(Error::Corrupt("landing scan key"))?
                .try_into()
                .map_err(|_| Error::Corrupt("landing scan key"))?;
            let r = self.inner.decode_landing(repo, id, &v)?;
            if r.retains_roots() {
                let retained = repo.len()
                    + r.intent.token.len()
                    + r.intent.profile.name.len()
                    + r.intent.profile.version.len()
                    + 256;
                if retained > limits.max_output_bytes - output {
                    if scanned == 0 {
                        return Err(Error::Limit("landing scan output bytes"));
                    }
                    break;
                }
                output += retained;
                jobs.push(PendingLanding {
                    id,
                    state: r.state,
                    expires_at: r.intent.expires_at,
                    profile: r.intent.profile,
                    handle: LandingHandle {
                        repository: repo.to_owned(),
                        owner: LandingOwner(r.owner),
                        token: r.intent.token,
                        store: Arc::downgrade(&self.inner),
                    },
                });
            }
            scanned += 1;
            bytes += size;
            last = Some(id);
        }
        let next = if complete {
            None
        } else {
            last.map(|after| LandingScanCursor {
                repository: repo.to_owned(),
                after,
                store: Arc::downgrade(&self.inner),
            })
        };
        Ok(LandingScanPage {
            jobs,
            next,
            scan_complete: complete,
            scanned_records: scanned,
            scanned_bytes: bytes,
        })
    }
    /// Atomically admit the immutable candidate and acquire physical GC ownership.
    /// No verdict or permission to publish is implied by holding this lease.
    pub fn pin_landing_candidate(
        &self,
        repo: &str,
        owner: LandingOwner,
        token: &str,
        now: i64,
    ) -> Result<LandingCandidateLease> {
        request_token(token)?;
        let id = job_id(repo, owner, token);
        let mut gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let r = self
            .inner
            .landing_record(repo, id)?
            .ok_or(Error::Invalid("unknown landing job"))?;
        if !matches!(r.state, LandingState::Prepared | LandingState::Checked)
            || r.intent.expires_at <= now
        {
            return Err(Error::Invalid("landing candidate not available for check"));
        }
        let c = r
            .candidate
            .as_ref()
            .ok_or(Error::Corrupt("landing candidate missing"))?;
        let key = ObjectKey {
            kind: Kind::Commit,
            id: ObjectId(c.head),
        };
        let descriptor = self
            .inner
            .descriptor(repo, key)?
            .ok_or(Error::MissingObject(key))?;
        let readers = gate.readers.entry((repo.to_owned(), key)).or_default();
        *readers = readers.checked_add(1).ok_or(Error::Limit("reader roots"))?;
        Ok(LandingCandidateLease {
            identity: LandingCheckIdentity {
                candidate: CommitId(c.head),
                root_tree: TreeId(c.tree),
                profile: r.intent.profile,
            },
            _root: RootReadGuard {
                descriptor,
                repo: repo.to_owned(),
                inner: Arc::clone(&self.inner),
            },
        })
    }
}
