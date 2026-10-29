//! Store-owned immutable review versions and generation-bound landing authority.
//! Authentication identities come from the trusted service, never request JSON.
use super::*;
use crate::forge_accounts::{AccountId, ForgeAction, ForgeActor, PolicyEpoch, ViewVersion};
use tinytree_model::{ChangeId, Commit, Object, Signature, Timestamp, TreeId, TreeValue};

const CHANGES: &[u8] = b"forge/reviews/v1/";
const REQUESTS: &[u8] = b"forge/review-requests/v1/";
const BINDINGS: &[u8] = b"forge/review-landing/v1/";
const MAX_PATCHSETS: usize = 64;
const MAX_ATTEMPTS: usize = 8;
const MAX_DECISIONS: usize = 128;
const MAX_COMMENTS: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeReviewDependency {
    pub change_id: [u8; 16],
    pub patchset_id: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgeReviewLineageOperation {
    Rename,
    Copy,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeReviewLineageHint {
    pub operation: ForgeReviewLineageOperation,
    pub source_path: String,
    pub target_path: String,
}

#[derive(Clone, Debug)]
pub struct ForgeReviewPushRequest {
    pub request_id: String,
    pub source_base: CommitId,
    pub source_head: CommitId,
    pub view_version: [u8; 32],
    pub expected_patchset: Option<[u8; 32]>,
    pub predecessor: Option<ForgeReviewDependency>,
    pub provenance: Option<[u8; 32]>,
    pub lineage: Vec<ForgeReviewLineageHint>,
    /// Selected by trusted server configuration, not an arbitrary client command.
    pub check_profile: LandingCheckProfile,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgeReviewState {
    RebasePending,
    Prepared,
    Conflict,
    Stale,
    ChecksPending,
    ReadyToLand,
    Landed,
    Unsupported,
    CheckFailed,
    Expired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeReviewCandidate {
    pub id: [u8; 32],
    pub head: [u8; 32],
    pub root_tree: [u8; 32],
    pub target: [u8; 32],
    pub target_generation: u64,
    pub rewrites: Vec<ForgeReviewRewrite>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeReviewRewrite {
    pub old: [u8; 32],
    pub new: [u8; 32],
    pub change_id: [u8; 16],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeReview {
    pub change_id: [u8; 16],
    pub owner: String,
    pub patchset_id: [u8; 32],
    pub patchset_number: u64,
    pub is_current: bool,
    pub source_base: [u8; 32],
    pub source_head: [u8; 32],
    pub view_version: [u8; 32],
    pub predecessor: Option<ForgeReviewDependency>,
    pub provenance: Option<[u8; 32]>,
    pub lineage: Vec<ForgeReviewLineageHint>,
    pub check_profile: LandingCheckProfile,
    pub state: ForgeReviewState,
    pub candidate: Option<ForgeReviewCandidate>,
    pub conflict_path: Option<String>,
    pub approvals: usize,
    pub comments: Vec<ForgeReviewComment>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeReviewComment {
    pub author: String,
    pub patchset_id: [u8; 32],
    pub body: String,
    pub path: Option<String>,
    pub created_at: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgeReviewDecision {
    Approve,
    RequestChanges,
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
struct Record {
    version: u8,
    account: String,
    owner: String,
    change: [u8; 16],
    patchsets: Vec<Patchset>,
    comments: Vec<ForgeReviewComment>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Patchset {
    id: [u8; 32],
    epoch: u64,
    view: [u8; 32],
    base: [u8; 32],
    projection_base: Option<[u8; 32]>,
    head: [u8; 32],
    predecessor: Option<ForgeReviewDependency>,
    provenance: Option<[u8; 32]>,
    lineage: Vec<ForgeReviewLineageHint>,
    profile: LandingCheckProfile,
    committer: Person,
    attempts: Vec<Attempt>,
    decisions: Vec<Decision>,
    landed: Option<[u8; 32]>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    job: [u8; 32],
    owner: [u8; 32],
    token: String,
    target: [u8; 32],
    generation: u64,
    build_target: [u8; 32],
    mapped_base: Option<[u8; 32]>,
    land_requested: bool,
    land_actor: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Decision {
    actor: String,
    request: String,
    candidate: [u8; 32],
    epoch: u64,
    decision: ForgeReviewDecision,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Replay {
    fingerprint: [u8; 32],
    change: [u8; 16],
    patchset: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    account: String,
    change: [u8; 16],
    patchset: [u8; 32],
}

fn scoped(prefix: &[u8], account: &str, tail: &[u8]) -> Vec<u8> {
    let mut result = prefix.to_vec();
    result.extend_from_slice(account.as_bytes());
    result.push(0);
    result.extend_from_slice(tail);
    result
}
fn record_key(account: &AccountId, change: [u8; 16]) -> Vec<u8> {
    scoped(CHANGES, &account.0, &change)
}
fn binding_key(repo: &str, job: [u8; 32]) -> Vec<u8> {
    scoped(BINDINGS, repo, &job)
}
fn operation_key(account: &AccountId, actor: &ForgeActor, request: &str) -> Vec<u8> {
    let mut h = blake3::Hasher::new();
    h.update(actor.principal.as_bytes());
    h.update(&[0]);
    h.update(request.as_bytes());
    scoped(REQUESTS, &account.0, h.finalize().as_bytes())
}
fn object(id: [u8; 32]) -> ObjectKey {
    ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(id),
    }
}
fn bounded<T: Serialize>(value: &T, maximum: usize) -> Result<Vec<u8>> {
    let bytes = encode(value)?;
    if bytes.len() > maximum {
        return Err(Error::Limit("review record bytes"));
    }
    Ok(bytes)
}
fn push_fingerprint(r: &ForgeReviewPushRequest) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&bounded(
        &(
            r.source_base.0,
            r.source_head.0,
            r.view_version,
            r.expected_patchset,
            &r.predecessor,
            r.provenance,
            &r.lineage,
            &r.check_profile,
        ),
        16 * 1024,
    )?)
    .as_bytes())
}

impl Inner {
    fn review_record_in(
        &self,
        db: &impl Readable,
        account: &AccountId,
        change: [u8; 16],
    ) -> Result<Option<Record>> {
        let Some(bytes) = db.get(&self.spaces.meta, record_key(account, change))? else {
            return Ok(None);
        };
        if bytes.len() > self.config.landing.max_record_bytes {
            return Err(Error::Limit("review record bytes"));
        }
        let record: Record =
            serde_json::from_slice(&bytes).map_err(|_| Error::Corrupt("review record encoding"))?;
        if record.version != 1
            || record.account != account.0
            || record.change != change
            || record.patchsets.is_empty()
            || record.patchsets.len() > MAX_PATCHSETS
            || record.comments.len() > MAX_COMMENTS
            || record
                .patchsets
                .iter()
                .any(|p| p.attempts.len() > MAX_ATTEMPTS || p.decisions.len() > MAX_DECISIONS)
        {
            return Err(Error::Corrupt("review record"));
        }
        Ok(Some(record))
    }
    pub(super) fn validate_forge_reviews_on_reopen(&self) -> Result<()> {
        let db = self.db.read_tx();
        let mut count = 0usize;
        let mut total = 0usize;
        for prefix in [CHANGES, REQUESTS, BINDINGS] {
            for row in db.prefix(&self.spaces.meta, prefix) {
                let (physical, bytes) = row.into_inner()?;
                count += 1;
                total = total
                    .checked_add(physical.len() + bytes.len())
                    .ok_or(Error::Limit("review catalog"))?;
                if count > self.config.max_retention_validation_records
                    || total as u64 > self.config.max_retention_validation_bytes
                {
                    return Err(Error::Limit("review catalog"));
                }
                if prefix == CHANGES {
                    if bytes.len() > self.config.landing.max_record_bytes {
                        return Err(Error::Limit("review record bytes"));
                    }
                    let raw: Record = serde_json::from_slice(&bytes)
                        .map_err(|_| Error::Corrupt("review record encoding"))?;
                    let account = AccountId(raw.account.clone());
                    if record_key(&account, raw.change).as_slice() != physical.as_ref() {
                        return Err(Error::Corrupt("review record key"));
                    }
                    let record = self
                        .review_record_in(&db, &account, raw.change)?
                        .ok_or(Error::Corrupt("review record"))?;
                    let repo = account.repository_id();
                    if self.forge_account_for_repository_in(&db, &repo)? != Some(account.clone()) {
                        return Err(Error::Corrupt("review account"));
                    }
                    let mut versions = HashSet::new();
                    for p in &record.patchsets {
                        if !versions.insert(p.id)
                            || self.review_commit(&repo, p.head)?.change_id.0 != record.change
                        {
                            return Err(Error::Corrupt("review patchset identity"));
                        }
                        self.review_root(&repo, p.base)?;
                        for a in &p.attempts {
                            if crate::landing::job_id(&repo, LandingOwner(a.owner), &a.token)
                                != a.job
                            {
                                return Err(Error::Corrupt("review attempt identity"));
                            }
                            let b: Binding = decode(
                                &db.get(&self.spaces.meta, binding_key(&repo, a.job))?
                                    .ok_or(Error::Corrupt("review attempt binding"))?,
                            )?;
                            if b.account != account.0
                                || b.change != record.change
                                || b.patchset != p.id
                            {
                                return Err(Error::Corrupt("review attempt binding"));
                            }
                            self.review_root(&repo, a.target)?;
                            self.review_root(&repo, a.build_target)?;
                        }
                    }
                } else if prefix == REQUESTS {
                    let replay: Replay = decode(&bytes)?;
                    // Account scope is encoded before the hashed actor/request suffix.
                    let suffix = physical
                        .as_ref()
                        .strip_prefix(REQUESTS)
                        .ok_or(Error::Corrupt("review request key"))?;
                    let (scope, hash) = suffix.split_at(
                        suffix
                            .iter()
                            .position(|b| *b == 0)
                            .ok_or(Error::Corrupt("review request scope"))?,
                    );
                    if hash.len() != 33 {
                        return Err(Error::Corrupt("review request key"));
                    }
                    let account = AccountId(
                        std::str::from_utf8(scope)
                            .map_err(|_| Error::Corrupt("review request scope"))?
                            .into(),
                    );
                    let record = self
                        .review_record_in(&db, &account, replay.change)?
                        .ok_or(Error::Corrupt("review replay record"))?;
                    if !record.patchsets.iter().any(|p| p.id == replay.patchset) {
                        return Err(Error::Corrupt("review replay version"));
                    }
                } else {
                    let binding: Binding = decode(&bytes)?;
                    let account = AccountId(binding.account);
                    let record = self
                        .review_record_in(&db, &account, binding.change)?
                        .ok_or(Error::Corrupt("review binding record"))?;
                    if !record
                        .patchsets
                        .iter()
                        .filter(|p| p.id == binding.patchset)
                        .flat_map(|p| p.attempts.iter())
                        .any(|a| {
                            binding_key(&account.repository_id(), a.job).as_slice()
                                == physical.as_ref()
                        })
                    {
                        return Err(Error::Corrupt("review binding key"));
                    }
                }
            }
        }
        Ok(())
    }

    fn review_budget(&self, replacing: Vec<u8>, new_bytes: usize) -> Result<()> {
        // Reserve one bounded new request/binding row alongside a changed record.
        let mut count = 2usize;
        let mut bytes = new_bytes + 2048;
        for prefix in [CHANGES, REQUESTS, BINDINGS] {
            for row in self.db.read_tx().prefix(&self.spaces.meta, prefix) {
                let (key, value) = row.into_inner()?;
                if key.as_ref() == replacing.as_slice() {
                    continue;
                }
                count = count.checked_add(1).ok_or(Error::Limit("review records"))?;
                bytes = bytes
                    .checked_add(key.len() + value.len())
                    .ok_or(Error::Limit("review bytes"))?;
                if count > self.config.max_retention_validation_records
                    || bytes as u64 > self.config.max_retention_validation_bytes
                {
                    return Err(Error::Limit("review catalog"));
                }
            }
        }
        Ok(())
    }
    fn review_commit(&self, repo: &str, id: [u8; 32]) -> Result<Commit> {
        let descriptor = self
            .descriptor(repo, object(id))?
            .ok_or(Error::MissingObject(object(id)))?;
        match self.metadata(repo, descriptor)? {
            Object::Commit(commit) => Ok(commit),
            _ => Err(Error::Corrupt("review commit kind")),
        }
    }
    fn review_root(&self, repo: &str, id: [u8; 32]) -> Result<TreeId> {
        let c = self.review_commit(repo, id)?;
        if c.root_tree.terms.len() != 1 {
            return Err(Error::Invalid("review source conflicts"));
        }
        Ok(c.root_tree.terms[0])
    }
    fn review_changed_paths(
        &self,
        repo: &str,
        base: TreeId,
        head: TreeId,
        allow: impl Fn(&str) -> bool,
    ) -> Result<()> {
        let mut pending = vec![(
            String::new(),
            Some(TreeValue::Tree(base)),
            Some(TreeValue::Tree(head)),
        )];
        let mut work = 0usize;
        let mut metadata_bytes = 0u64;
        while let Some((path, before, after)) = pending.pop() {
            work += 1;
            if work > self.config.max_closure_edges || pending.len() > self.config.max_path_contexts
            {
                return Err(Error::Limit("review changed paths"));
            }
            if before == after {
                continue;
            }
            if matches!(before, Some(TreeValue::Tree(_)))
                || matches!(after, Some(TreeValue::Tree(_)))
            {
                let mut entries = std::collections::BTreeMap::<
                    String,
                    (Option<TreeValue>, Option<TreeValue>),
                >::new();
                for (old, value) in [(true, before.as_ref()), (false, after.as_ref())] {
                    if let Some(TreeValue::Tree(id)) = value {
                        let key = ObjectKey {
                            kind: Kind::Tree,
                            id: ObjectId(id.0),
                        };
                        let d = self
                            .descriptor(repo, key)?
                            .ok_or(Error::MissingObject(key))?;
                        metadata_bytes = metadata_bytes
                            .checked_add(d.size_bytes)
                            .ok_or(Error::Limit("review metadata"))?;
                        if metadata_bytes > self.config.landing.max_validation_metadata_bytes {
                            return Err(Error::Limit("review metadata"));
                        }
                        let Object::Tree(tree) = self.metadata(repo, d)? else {
                            return Err(Error::Corrupt("review tree kind"));
                        };
                        for e in tree.entries {
                            let slot = entries.entry(e.name).or_default();
                            if old {
                                slot.0 = Some(e.value);
                            } else {
                                slot.1 = Some(e.value);
                            }
                        }
                    } else if value.is_some() && !allow(&path) {
                        return Err(Error::ForgeAccessDenied);
                    }
                }
                if entries.is_empty() && !path.is_empty() && !allow(&path) {
                    return Err(Error::ForgeAccessDenied);
                }
                for (name, (a, b)) in entries {
                    let next = if path.is_empty() {
                        name
                    } else {
                        format!("{path}/{name}")
                    };
                    if next.len() > self.config.model_limits.max_path_bytes {
                        return Err(Error::Limit("review path"));
                    }
                    pending.push((next, a, b));
                }
            } else if !allow(&path) {
                return Err(Error::ForgeAccessDenied);
            }
        }
        Ok(())
    }

    fn review_tree_files(
        &self,
        repo: &str,
        root: TreeId,
    ) -> Result<std::collections::BTreeMap<String, TreeValue>> {
        let mut stack = vec![(String::new(), root)];
        let mut files = std::collections::BTreeMap::new();
        let mut work = 0usize;
        let mut bytes = 0u64;
        while let Some((prefix, id)) = stack.pop() {
            let key = ObjectKey {
                kind: Kind::Tree,
                id: ObjectId(id.0),
            };
            let descriptor = self
                .descriptor(repo, key)?
                .ok_or(Error::MissingObject(key))?;
            bytes = bytes
                .checked_add(descriptor.size_bytes)
                .ok_or(Error::Limit("review metadata"))?;
            if bytes > self.config.landing.max_validation_metadata_bytes {
                return Err(Error::Limit("review metadata"));
            }
            let Object::Tree(tree) = self.metadata(repo, descriptor)? else {
                return Err(Error::Corrupt("review tree"));
            };
            for entry in tree.entries {
                work += 1;
                if work > self.config.max_closure_edges
                    || stack.len() > self.config.max_path_contexts
                {
                    return Err(Error::Limit("review files"));
                }
                let path = if prefix.is_empty() {
                    entry.name
                } else {
                    format!("{prefix}/{}", entry.name)
                };
                if path.len() > self.config.model_limits.max_path_bytes {
                    return Err(Error::Limit("review path"));
                }
                match entry.value {
                    TreeValue::Tree(child) => stack.push((path, child)),
                    value => {
                        files.insert(path, value);
                    }
                }
            }
        }
        Ok(files)
    }
    fn review_validate_files(
        &self,
        db: &impl Readable,
        view: &crate::ResolvedView,
        actor: &ForgeActor,
        base: TreeId,
        head: TreeId,
        hints: &[ForgeReviewLineageHint],
        now: i64,
    ) -> Result<()> {
        if hints.len() > 128 {
            return Err(Error::Limit("review lineage"));
        }
        let before = self.review_tree_files(view.repository(), base)?;
        let after = self.review_tree_files(view.repository(), head)?;
        let mut targets = HashSet::new();
        for hint in hints {
            if hint.source_path == hint.target_path
                || !targets.insert(hint.target_path.as_str())
                || !view.allows_path(&hint.source_path)
                || !view.allows_path(&hint.target_path)
                || before.contains_key(&hint.target_path)
            {
                return Err(Error::Invalid("review lineage endpoints"));
            }
            let Some(TreeValue::File {
                id: source,
                size_bytes: source_size,
                copy_id: source_copy,
                ..
            }) = before.get(&hint.source_path)
            else {
                return Err(Error::Invalid("review lineage source"));
            };
            let Some(TreeValue::File {
                id: target,
                size_bytes: target_size,
                copy_id: target_copy,
                ..
            }) = after.get(&hint.target_path)
            else {
                return Err(Error::Invalid("review lineage target"));
            };
            if target_copy.is_none() && (source != target || source_size != target_size) {
                return Err(Error::Invalid("review lineage content identity"));
            }
            match hint.operation {
                ForgeReviewLineageOperation::Rename if after.contains_key(&hint.source_path) => {
                    return Err(Error::Invalid("review rename retained source"));
                }
                ForgeReviewLineageOperation::Copy
                    if !matches!(after.get(&hint.source_path), Some(TreeValue::File { .. })) =>
                {
                    return Err(Error::Invalid("review copy changed source"));
                }
                _ => {}
            }
            if let Some(copy) = target_copy {
                self.forge_validate_copy_in(db, view, actor, &hint.target_path, *copy, now)?;
                let source_copy = source_copy.ok_or(Error::Invalid("review native copy source"))?;
                match self.forge_projected_copy_origin_in(db, view, source_copy) {
                    Ok(origin)
                        if origin.canonical_path == hint.source_path && origin.file == source.0 => {
                    }
                    Ok(_) => return Err(Error::Invalid("review projected copy origin")),
                    Err(Error::ForgeAccessDenied) => self.forge_validate_copy_in(
                        db,
                        view,
                        actor,
                        &hint.source_path,
                        source_copy,
                        now,
                    )?,
                    Err(error) => return Err(error),
                }
                let key = ObjectKey {
                    kind: Kind::CopyHistory,
                    id: ObjectId(copy.0),
                };
                let descriptor = self
                    .descriptor(view.repository(), key)?
                    .ok_or(Error::MissingObject(key))?;
                let Object::CopyHistory(history) = self.metadata(view.repository(), descriptor)?
                else {
                    return Err(Error::Corrupt("review copy history"));
                };
                if history.current_path != hint.target_path || history.parents != vec![source_copy]
                {
                    return Err(Error::Invalid("review native copy relation"));
                }
            }
        }
        for (path, value) in &after {
            if before.get(path) == Some(value) {
                continue;
            }
            match value {
                TreeValue::File {
                    id,
                    size_bytes,
                    copy_id,
                    ..
                } => {
                    if copy_id.is_some() && !targets.contains(path.as_str()) {
                        return Err(Error::Invalid("review copy requires explicit lineage"));
                    }
                    let key = ObjectKey {
                        kind: Kind::File,
                        id: ObjectId(id.0),
                    };
                    let descriptor = self
                        .descriptor(view.repository(), key)?
                        .ok_or(Error::MissingObject(key))?;
                    if descriptor.size_bytes != *size_bytes {
                        return Err(Error::Invalid("review file size"));
                    }
                    self.forge_validate_file_in(db, view, actor, path, descriptor, now)?;
                }
                TreeValue::Symlink { id, .. } => {
                    let key = ObjectKey {
                        kind: Kind::Symlink,
                        id: ObjectId(id.0),
                    };
                    let descriptor = self
                        .descriptor(view.repository(), key)?
                        .ok_or(Error::MissingObject(key))?;
                    self.forge_validate_entry_in(db, view, actor, path, descriptor, now)?;
                }
                _ => return Err(Error::Invalid("unsupported review entry")),
            }
        }
        Ok(())
    }

    /// Called by GC under the publication gate. Patchset history owns its roots
    /// independently from short-lived landing attempts and their expiry.
    pub(super) fn forge_review_roots(&self, repo: &str) -> Result<HashSet<ObjectKey>> {
        let view = self.db.read_tx();
        let Some(account) = self.forge_account_for_repository_in(&view, repo)? else {
            return Ok(HashSet::new());
        };
        let mut roots = HashSet::new();
        let mut rows = 0usize;
        let mut bytes = 0usize;
        for row in view.prefix(&self.spaces.meta, scoped(CHANGES, &account.0, b"")) {
            let (key, value) = row.into_inner()?;
            rows += 1;
            bytes += key.len() + value.len();
            if rows > self.config.max_retention_validation_records
                || bytes as u64 > self.config.max_retention_validation_bytes
            {
                return Err(Error::Limit("review roots catalog"));
            }
            let change: [u8; 16] = key
                .as_ref()
                .get(key.len().saturating_sub(16)..)
                .ok_or(Error::Corrupt("review key"))?
                .try_into()
                .map_err(|_| Error::Corrupt("review key"))?;
            let record = self
                .review_record_in(&view, &account, change)?
                .ok_or(Error::Corrupt("review disappeared"))?;
            for p in record.patchsets {
                roots.extend(
                    [p.base, p.head]
                        .into_iter()
                        .filter(|id| *id != CommitId::ROOT.0)
                        .map(object),
                );
                for attempt in p.attempts {
                    roots.extend(
                        [attempt.target, attempt.build_target]
                            .into_iter()
                            .map(object),
                    );
                    if let Some(job) = self.landing_record_for_review(repo, attempt.job)? {
                        if let Some(candidate) = job.candidate {
                            roots.insert(object(candidate.head.0));
                        }
                    }
                }
                if roots.len() > self.config.max_closure_objects {
                    return Err(Error::Limit("review roots"));
                }
            }
        }
        Ok(roots)
    }
}

impl Store {
    /// Produce an authorized native result for local reconciliation without
    /// exposing canonical ancestors, signatures or excluded tree entries.
    pub fn forge_project_review_result(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        change: ChangeId,
        patchset: [u8; 32],
        now: i64,
    ) -> Result<Option<crate::ForgeProjection>> {
        let review = self.forge_review_patchset(account, actor, change, patchset)?;
        let Some(candidate) = review.candidate else {
            return Ok(None);
        };
        let selected = {
            let _gate = self.inner.lock()?;
            self.inner.forge_resolve_view_in(
                &self.inner.db.read_tx(),
                account,
                Some(actor),
                ViewVersion(review.view_version),
            )?
        };
        let projected_parent = {
            let _gate = self.inner.lock()?;
            match self.inner.forge_projection_base_in(
                &self.inner.db.read_tx(),
                &selected,
                CommitId(review.source_base),
            ) {
                Ok(_) => Some(CommitId(review.source_base)),
                Err(Error::ForgeAccessDenied) => None,
                Err(error) => return Err(error),
            }
        };
        let parent = match projected_parent {
            Some(parent) => parent,
            None => CommitId(self.forge_project_view(&selected, now)?.projected_commit),
        };
        self.forge_project_review_head(&selected, CommitId(candidate.head), change, parent, now)
            .map(Some)
    }
    /// Accept an immutable patchset, retain its inputs, and attempt server rebase.
    /// A retry identifies the original accepted version even after a later push.
    pub fn forge_push_review(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        request: &ForgeReviewPushRequest,
        committer: &Signature,
        now: i64,
    ) -> Result<ForgeReview> {
        request_token(&request.request_id)?;
        if committer.name.len() > 1024 || committer.email.len() > 1024 {
            return Err(Error::Limit("review committer"));
        }
        // Materialize a bounded sanitized baseline before taking the admission gate.
        let projection_view = {
            let _gate = self.inner.lock()?;
            self.inner.forge_resolve_view_in(
                &self.inner.db.read_tx(),
                account,
                Some(actor),
                ViewVersion(request.view_version),
            )?
        };
        self.forge_project_view(&projection_view, now)?;
        if request.check_profile.name.is_empty()
            || request.check_profile.version.is_empty()
            || request.check_profile.name.len() > 128
            || request.check_profile.version.len() > 128
            || request.check_profile.name.contains('\0')
            || request.check_profile.version.contains('\0')
        {
            return Err(Error::Invalid("review check profile"));
        }
        let fingerprint = push_fingerprint(request)?;
        let replay_key = operation_key(account, actor, &request.request_id);
        let (change, patchset) = {
            let _gate = self.inner.lock()?;
            let db = self.inner.db.read_tx();
            let access =
                self.inner
                    .forge_authorize_in(&db, account, actor, ForgeAction::Write, None)?;
            if let Some(bytes) = db.get(&self.inner.spaces.meta, &replay_key)? {
                let replay: Replay = decode(&bytes)?;
                if replay.fingerprint != fingerprint {
                    return Err(Error::IdempotencyConflict);
                }
                (replay.change, replay.patchset)
            } else {
                let selected = self.inner.forge_resolve_view_in(
                    &db,
                    account,
                    Some(actor),
                    ViewVersion(request.view_version),
                )?;
                let projection_base = if request.provenance.is_none()
                    && request.predecessor.is_none()
                    && request.source_base.0 != selected.revision().0
                {
                    Some(
                        self.inner
                            .forge_projection_base_in(&db, &selected, request.source_base)?
                            .0,
                    )
                } else {
                    None
                };
                if let Some(run) = request.provenance {
                    self.inner.forge_validate_transform_review_in(
                        &db,
                        account,
                        actor,
                        run,
                        request.source_base,
                        request.source_head,
                        ViewVersion(request.view_version),
                    )?;
                } else {
                    self.inner.forge_validate_review_head_in(
                        &db,
                        &selected,
                        actor,
                        request.source_head,
                        now,
                    )?;
                }
                let head = self
                    .inner
                    .review_commit(&access.repository, request.source_head.0)?;
                if head.change_id == ChangeId::ROOT
                    || request.source_base == request.source_head
                    || head.parents != vec![request.source_base]
                {
                    return Err(Error::Invalid("review logical change"));
                }
                let base_tree = self
                    .inner
                    .review_root(&access.repository, request.source_base.0)?;
                let head_tree = self
                    .inner
                    .review_root(&access.repository, request.source_head.0)?;
                self.inner
                    .review_changed_paths(&access.repository, base_tree, head_tree, |p| {
                        selected.allows_path(p)
                    })?;
                if request.provenance.is_none() {
                    self.inner.review_validate_files(
                        &db,
                        &selected,
                        actor,
                        base_tree,
                        head_tree,
                        &request.lineage,
                        now,
                    )?;
                }
                self.inner.closure_bounded(
                    &access.repository,
                    [object(request.source_base.0), object(request.source_head.0)],
                    true,
                    true,
                    &HashMap::new(),
                    &self.inner.config,
                    self.inner.config.landing.max_validation_metadata_bytes,
                )?;
                let change = head.change_id.0;
                if request
                    .predecessor
                    .as_ref()
                    .is_some_and(|d| d.change_id == change)
                {
                    return Err(Error::Invalid("review dependency cycle"));
                }
                let mut record = self
                    .inner
                    .review_record_in(&db, account, change)?
                    .unwrap_or(Record {
                        version: 1,
                        account: account.0.clone(),
                        owner: actor.principal.clone(),
                        change,
                        patchsets: Vec::new(),
                        comments: Vec::new(),
                    });
                if record.owner != actor.principal && !access.is_admin {
                    return Err(Error::ForgeAccessDenied);
                }
                if record.patchsets.last().map(|p| p.id) != request.expected_patchset {
                    return Err(Error::StaleForgeReview);
                }
                if record.patchsets.last().is_some_and(|p| p.landed.is_some()) {
                    return Err(Error::Invalid("review already landed"));
                }
                if record.patchsets.len() >= MAX_PATCHSETS {
                    return Err(Error::Limit("review patchsets"));
                }
                let mut h = blake3::Hasher::new();
                h.update(b"tinytree-forge-patchset-v1\0");
                h.update(account.0.as_bytes());
                h.update(&change);
                h.update(&fingerprint);
                h.update(&access.epoch.0.to_be_bytes());
                let id = *h.finalize().as_bytes();
                record.patchsets.push(Patchset {
                    id,
                    epoch: access.epoch.0,
                    view: request.view_version,
                    base: request.source_base.0,
                    projection_base,
                    head: request.source_head.0,
                    predecessor: request.predecessor.clone(),
                    provenance: request.provenance,
                    lineage: request.lineage.clone(),
                    profile: request.check_profile.clone(),
                    committer: committer.into(),
                    attempts: Vec::new(),
                    decisions: Vec::new(),
                    landed: None,
                });
                let bytes = bounded(&record, self.inner.config.landing.max_record_bytes)?;
                self.inner
                    .review_budget(record_key(account, change), bytes.len())?;
                self.inner.check_cloud_gc_union(
                    &access.repository,
                    &[object(request.source_base.0), object(request.source_head.0)],
                    None,
                )?;
                let mut tx = self
                    .inner
                    .db
                    .write_tx()
                    .durability(Some(PersistMode::SyncAll));
                tx.insert(&self.inner.spaces.meta, record_key(account, change), bytes);
                tx.insert(
                    &self.inner.spaces.meta,
                    replay_key,
                    bounded(
                        &Replay {
                            fingerprint,
                            change,
                            patchset: id,
                        },
                        1024,
                    )?,
                );
                self.inner.commit(tx)?;
                (change, id)
            }
        };
        self.forge_prepare_review(account, actor, ChangeId(change), patchset, now)
    }

    /// Rebase the selected immutable version against a fresh observed target.
    /// Attempts are bounded and never overwrite the uploaded patchset.
    pub fn forge_prepare_review(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        change: ChangeId,
        patchset_id: [u8; 32],
        now: i64,
    ) -> Result<ForgeReview> {
        for _ in 0..3 {
            let (repo, request, owner) = {
                let _gate = self.inner.lock()?;
                let db = self.inner.db.read_tx();
                let access =
                    self.inner
                        .forge_authorize_in(&db, account, actor, ForgeAction::Write, None)?;
                let mut record = self
                    .inner
                    .review_record_in(&db, account, change.0)?
                    .ok_or(Error::Invalid("unknown review"))?;
                let current = record.patchsets.last().map(|p| p.id);
                let p = record
                    .patchsets
                    .iter_mut()
                    .find(|p| p.id == patchset_id)
                    .ok_or(Error::Invalid("unknown review version"))?;
                if current != Some(patchset_id) || p.landed.is_some() {
                    drop(db);
                    drop(_gate);
                    return self.forge_review_version(account, actor, change, Some(patchset_id));
                }
                self.inner.forge_authorize_in(
                    &db,
                    account,
                    actor,
                    ForgeAction::Write,
                    Some(PolicyEpoch(p.epoch)),
                )?;
                self.inner
                    .forge_resolve_view_in(&db, account, Some(actor), ViewVersion(p.view))?;
                let current_ref = reference(
                    "main".into(),
                    db.get(&self.inner.spaces.refs, key(&access.repository, b"main"))?
                        .as_deref(),
                )?;
                let target = current_ref
                    .commit
                    .ok_or(Error::Invalid("forge main is not initialized"))?
                    .key
                    .id
                    .0;
                let mut build_target = target;
                let mut mapped_base = p.projection_base;
                if let Some(dependency) = &p.predecessor {
                    let predecessor = self
                        .inner
                        .review_record_in(&db, account, dependency.change_id)?
                        .ok_or(Error::Invalid("unknown review dependency"))?;
                    let parent = predecessor
                        .patchsets
                        .last()
                        .filter(|v| v.id == dependency.patchset_id)
                        .ok_or(Error::StaleForgeReview)?;
                    self.inner.forge_resolve_view_in(
                        &db,
                        account,
                        Some(actor),
                        ViewVersion(parent.view),
                    )?;
                    if p.base != parent.head {
                        return Err(Error::Invalid("review dependency source base"));
                    }
                    let attempt = parent
                        .attempts
                        .last()
                        .ok_or(Error::Invalid("review dependency not prepared"))?;
                    let job = self
                        .inner
                        .landing_record_for_review(&access.repository, attempt.job)?
                        .ok_or(Error::Corrupt("review dependency attempt"))?;
                    let candidate = job
                        .candidate
                        .ok_or(Error::Invalid("review dependency has no candidate"))?;
                    if parent.landed.is_none()
                        && (attempt.target != target
                            || attempt.generation != current_ref.generation)
                    {
                        return Err(Error::StaleForgeReview);
                    }
                    build_target = if parent.landed.is_some() {
                        target
                    } else {
                        candidate.head.0
                    };
                    mapped_base = Some(candidate.head.0);
                }
                let existing = p
                    .attempts
                    .last()
                    .filter(|a| {
                        a.target == target
                            && a.generation == current_ref.generation
                            && a.build_target == build_target
                            && a.mapped_base == mapped_base
                    })
                    .cloned();
                let existing = match existing {
                    Some(a) => match self
                        .inner
                        .landing_record_for_review(&access.repository, a.job)?
                    {
                        Some(job)
                            if matches!(
                                job.state,
                                LandingState::Expired | LandingState::StaleTarget
                            ) =>
                        {
                            None
                        }
                        _ => Some(a),
                    },
                    None => None,
                };
                let attempt = if let Some(existing) = existing {
                    existing
                } else {
                    if p.attempts.len() >= MAX_ATTEMPTS {
                        return Err(Error::Limit("review rebase attempts"));
                    }
                    let token = format!("forge-{}-{}", hex(&patchset_id), p.attempts.len());
                    let mut h = blake3::Hasher::new();
                    h.update(b"tinytree-forge-review-owner-v1\0");
                    h.update(account.0.as_bytes());
                    h.update(&change.0);
                    let owner = *h.finalize().as_bytes();
                    let job =
                        crate::landing::job_id(&access.repository, LandingOwner(owner), &token);
                    let attempt = Attempt {
                        job,
                        owner,
                        token,
                        target,
                        generation: current_ref.generation,
                        build_target,
                        mapped_base,
                        land_requested: false,
                        land_actor: None,
                    };
                    p.attempts.push(attempt.clone());
                    let bytes = bounded(&record, self.inner.config.landing.max_record_bytes)?;
                    self.inner
                        .review_budget(record_key(account, change.0), bytes.len())?;
                    self.inner.check_cloud_gc_union(
                        &access.repository,
                        &[object(build_target)],
                        None,
                    )?;
                    let mut tx = self
                        .inner
                        .db
                        .write_tx()
                        .durability(Some(PersistMode::SyncAll));
                    tx.insert(
                        &self.inner.spaces.meta,
                        record_key(account, change.0),
                        bytes,
                    );
                    tx.insert(
                        &self.inner.spaces.meta,
                        binding_key(&access.repository, job),
                        bounded(
                            &Binding {
                                account: account.0.clone(),
                                change: change.0,
                                patchset: patchset_id,
                            },
                            1024,
                        )?,
                    );
                    self.inner.commit(tx)?;
                    attempt
                };
                let p = record
                    .patchsets
                    .iter()
                    .find(|p| p.id == patchset_id)
                    .expect("selected review patchset");
                let request = LandingRequest {
                    idempotency_key: attempt.token.clone(),
                    source_base: CommitId(p.base),
                    source_head: CommitId(p.head),
                    reference_name: "main".into(),
                    expected_generation: attempt.generation,
                    expected_head: CommitId(attempt.target),
                    committer: p.committer.signature(),
                    check_profile: p.profile.clone(),
                    expires_at: now
                        .checked_add(self.inner.config.landing.max_job_ttl_seconds)
                        .ok_or(Error::Limit("review attempt expiry"))?,
                };
                (access.repository, request, LandingOwner(attempt.owner))
            };
            let job = self.begin_review_landing(&repo, owner, &request, now)?;
            let job = if job.state == LandingState::Intent {
                self.prepare_landing(&repo, owner, &request.idempotency_key, now)?
            } else {
                job
            };
            if job.state != LandingState::StaleTarget {
                let reference = self.references(&repo, &["main".into()])?.remove(0);
                if reference.generation == request.expected_generation
                    && reference.commit.map(|d| d.key.id.0) == Some(request.expected_head.0)
                {
                    break;
                }
            }
        }
        self.forge_review_version(account, actor, change, Some(patchset_id))
    }

    pub fn forge_review(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        change: ChangeId,
    ) -> Result<ForgeReview> {
        self.forge_review_version(account, actor, change, None)
    }
    pub fn forge_review_patchset(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        change: ChangeId,
        patchset: [u8; 32],
    ) -> Result<ForgeReview> {
        self.forge_review_version(account, actor, change, Some(patchset))
    }
    pub fn forge_review_operation(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        request_id: &str,
    ) -> Result<Option<ForgeReview>> {
        request_token(request_id)?;
        let replay = {
            let _gate = self.inner.lock()?;
            let db = self.inner.db.read_tx();
            self.inner
                .forge_authorize_in(&db, account, actor, ForgeAction::Read, None)?;
            db.get(
                &self.inner.spaces.meta,
                operation_key(account, actor, request_id),
            )?
            .map(|v| decode::<Replay>(&v))
            .transpose()?
        };
        replay
            .map(|r| {
                self.forge_review_version(account, actor, ChangeId(r.change), Some(r.patchset))
            })
            .transpose()
    }
    fn forge_review_version(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        change: ChangeId,
        version: Option<[u8; 32]>,
    ) -> Result<ForgeReview> {
        let _gate = self.inner.lock()?;
        let db = self.inner.db.read_tx();
        let access = self
            .inner
            .forge_authorize_in(&db, account, actor, ForgeAction::Read, None)?;
        let record = self
            .inner
            .review_record_in(&db, account, change.0)?
            .ok_or(Error::Invalid("unknown review"))?;
        let index = match version {
            Some(id) => record
                .patchsets
                .iter()
                .position(|p| p.id == id)
                .ok_or(Error::Invalid("unknown review version"))?,
            None => record.patchsets.len() - 1,
        };
        let p = &record.patchsets[index];
        let selected =
            self.inner
                .forge_resolve_view_in(&db, account, Some(actor), ViewVersion(p.view))?;
        self.inner.review_changed_paths(
            &access.repository,
            self.inner.review_root(&access.repository, p.base)?,
            self.inner.review_root(&access.repository, p.head)?,
            |path| selected.allows_path(path),
        )?;
        let mut state = ForgeReviewState::RebasePending;
        let mut candidate = None;
        let mut conflict = None;
        let mut approvals = 0;
        if let Some(a) = p.attempts.last() {
            if let Some(job) = self
                .inner
                .landing_record_for_review(&access.repository, a.job)?
            {
                let current = reference(
                    "main".into(),
                    db.get(&self.inner.spaces.refs, key(&access.repository, b"main"))?
                        .as_deref(),
                )?;
                let current_target = current.generation == a.generation
                    && current.commit.map(|d| d.key.id.0) == Some(a.target);
                approvals = self.inner.review_approval_count(&db, account, p, a.job)?;
                state = match job.state {
                    LandingState::Intent => ForgeReviewState::RebasePending,
                    LandingState::Prepared => ForgeReviewState::ChecksPending,
                    LandingState::Checked if approvals > 0 => ForgeReviewState::ReadyToLand,
                    LandingState::Checked => ForgeReviewState::Prepared,
                    LandingState::Committed => ForgeReviewState::Landed,
                    LandingState::Conflict => ForgeReviewState::Conflict,
                    LandingState::Unsupported => ForgeReviewState::Unsupported,
                    LandingState::CheckFailed | LandingState::CheckUnavailable => {
                        ForgeReviewState::CheckFailed
                    }
                    LandingState::StaleTarget => ForgeReviewState::Stale,
                    LandingState::Expired => ForgeReviewState::Expired,
                };
                if !current_target && state != ForgeReviewState::Landed {
                    state = ForgeReviewState::Stale;
                }
                if access.epoch.0 != p.epoch && state != ForgeReviewState::Landed {
                    state = ForgeReviewState::Stale;
                }
                conflict = job.conflict_path;
                candidate = job.candidate.map(|c| ForgeReviewCandidate {
                    id: a.job,
                    head: c.head.0,
                    root_tree: c.root_tree.0,
                    target: a.target,
                    target_generation: a.generation,
                    rewrites: c
                        .rewrites
                        .into_iter()
                        .map(|r| ForgeReviewRewrite {
                            old: r.old.0,
                            new: r.new.0,
                            change_id: r.change_id.0,
                        })
                        .collect(),
                });
            }
        }
        if p.landed.is_some() {
            state = ForgeReviewState::Landed;
        } else if p.epoch != access.epoch.0 {
            state = ForgeReviewState::Stale;
        }
        Ok(ForgeReview {
            change_id: change.0,
            owner: record.owner,
            patchset_id: p.id,
            patchset_number: (index + 1) as u64,
            is_current: index + 1 == record.patchsets.len(),
            source_base: p.base,
            source_head: p.head,
            view_version: p.view,
            predecessor: p.predecessor.clone(),
            provenance: p.provenance,
            lineage: p.lineage.clone(),
            check_profile: p.profile.clone(),
            state,
            candidate,
            conflict_path: conflict,
            approvals,
            comments: record
                .comments
                .into_iter()
                .filter(|c| {
                    access.epoch.0 == p.epoch
                        && c.patchset_id == p.id
                        && c.path
                            .as_deref()
                            .is_none_or(|path| selected.allows_path(path))
                })
                .collect(),
        })
    }

    pub fn forge_approve_review(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        change: ChangeId,
        patchset: [u8; 32],
        candidate: [u8; 32],
        request_id: &str,
        now: i64,
    ) -> Result<ForgeReview> {
        self.forge_decide_review(
            account,
            actor,
            change,
            patchset,
            candidate,
            request_id,
            ForgeReviewDecision::Approve,
            now,
        )
    }
    pub fn forge_decide_review(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        change: ChangeId,
        patchset: [u8; 32],
        candidate: [u8; 32],
        request_id: &str,
        decision: ForgeReviewDecision,
        _now: i64,
    ) -> Result<ForgeReview> {
        request_token(request_id)?;
        let replay_key = operation_key(account, actor, request_id);
        let fingerprint = *blake3::hash(&bounded(
            &("decision", change.0, patchset, candidate, decision),
            1024,
        )?)
        .as_bytes();
        {
            let _gate = self.inner.lock()?;
            let db = self.inner.db.read_tx();
            let access =
                self.inner
                    .forge_authorize_in(&db, account, actor, ForgeAction::Review, None)?;
            if let Some(bytes) = db.get(&self.inner.spaces.meta, &replay_key)? {
                let replay: Replay = decode(&bytes)?;
                if replay.fingerprint != fingerprint {
                    return Err(Error::IdempotencyConflict);
                }
                drop(db);
                drop(_gate);
                return self.forge_review_version(
                    account,
                    actor,
                    ChangeId(replay.change),
                    Some(replay.patchset),
                );
            }
            let mut record = self
                .inner
                .review_record_in(&db, account, change.0)?
                .ok_or(Error::Invalid("unknown review"))?;
            let p = record
                .patchsets
                .last_mut()
                .filter(|p| p.id == patchset)
                .ok_or(Error::StaleForgeReview)?;
            self.inner
                .forge_resolve_view_in(&db, account, Some(actor), ViewVersion(p.view))?;
            if p.epoch != access.epoch.0 {
                return Err(Error::StaleForgePolicy);
            }
            let a = p
                .attempts
                .last()
                .ok_or(Error::Invalid("review not prepared"))?;
            if a.job != candidate {
                return Err(Error::StaleForgeReview);
            }
            let job = self
                .inner
                .landing_record_for_review(&access.repository, a.job)?
                .ok_or(Error::Corrupt("review attempt"))?;
            if job.candidate.is_none() || p.landed.is_some() {
                return Err(Error::Invalid("review does not accept a decision"));
            }
            let supplied = Decision {
                actor: actor.principal.clone(),
                request: request_id.into(),
                candidate: a.job,
                epoch: access.epoch.0,
                decision,
            };
            if let Some(old) = p
                .decisions
                .iter()
                .find(|d| d.actor == actor.principal && d.request == request_id)
            {
                if old.candidate != supplied.candidate || old.decision != supplied.decision {
                    return Err(Error::IdempotencyConflict);
                }
            } else {
                if p.decisions.len() >= MAX_DECISIONS {
                    return Err(Error::Limit("review decisions"));
                }
                p.decisions.push(supplied);
                let bytes = bounded(&record, self.inner.config.landing.max_record_bytes)?;
                self.inner
                    .review_budget(record_key(account, change.0), bytes.len())?;
                let mut tx = self
                    .inner
                    .db
                    .write_tx()
                    .durability(Some(PersistMode::SyncAll));
                tx.insert(
                    &self.inner.spaces.meta,
                    record_key(account, change.0),
                    bytes,
                );
                tx.insert(
                    &self.inner.spaces.meta,
                    replay_key,
                    bounded(
                        &Replay {
                            fingerprint,
                            change: change.0,
                            patchset,
                        },
                        1024,
                    )?,
                );
                self.inner.commit(tx)?;
            }
        }
        self.forge_review(account, actor, change)
    }

    pub fn forge_land_review(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        change: ChangeId,
        patchset: [u8; 32],
        candidate: [u8; 32],
        now: i64,
    ) -> Result<ForgeReview> {
        self.forge_land_review_request(
            account,
            actor,
            change,
            patchset,
            candidate,
            &format!("land-{}", hex(&candidate)),
            now,
        )
    }
    pub fn forge_land_review_request(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        change: ChangeId,
        patchset: [u8; 32],
        candidate: [u8; 32],
        request_id: &str,
        now: i64,
    ) -> Result<ForgeReview> {
        request_token(request_id)?;
        let replay_key = operation_key(account, actor, request_id);
        let fingerprint =
            *blake3::hash(&bounded(&("land", change.0, patchset, candidate), 1024)?).as_bytes();
        let (repo, owner, token) = {
            let _gate = self.inner.lock()?;
            let db = self.inner.db.read_tx();
            let access =
                self.inner
                    .forge_authorize_in(&db, account, actor, ForgeAction::Write, None)?;
            if let Some(bytes) = db.get(&self.inner.spaces.meta, &replay_key)? {
                let replay: Replay = decode(&bytes)?;
                if replay.fingerprint != fingerprint {
                    return Err(Error::IdempotencyConflict);
                }
            }
            let mut record = self
                .inner
                .review_record_in(&db, account, change.0)?
                .ok_or(Error::Invalid("unknown review"))?;
            let p = record
                .patchsets
                .last_mut()
                .filter(|p| p.id == patchset)
                .ok_or(Error::StaleForgeReview)?;
            self.inner.forge_authorize_in(
                &db,
                account,
                actor,
                ForgeAction::Write,
                Some(PolicyEpoch(p.epoch)),
            )?;
            self.inner
                .forge_resolve_view_in(&db, account, Some(actor), ViewVersion(p.view))?;
            let a = p
                .attempts
                .last_mut()
                .filter(|a| a.job == candidate)
                .ok_or(Error::StaleForgeReview)?;
            a.land_requested = true;
            a.land_actor = Some(actor.principal.clone());
            let output = (access.repository, LandingOwner(a.owner), a.token.clone());
            let bytes = bounded(&record, self.inner.config.landing.max_record_bytes)?;
            self.inner
                .review_budget(record_key(account, change.0), bytes.len())?;
            let mut tx = self
                .inner
                .db
                .write_tx()
                .durability(Some(PersistMode::SyncAll));
            tx.insert(
                &self.inner.spaces.meta,
                record_key(account, change.0),
                bytes,
            );
            tx.insert(
                &self.inner.spaces.meta,
                replay_key,
                bounded(
                    &Replay {
                        fingerprint,
                        change: change.0,
                        patchset,
                    },
                    1024,
                )?,
            );
            self.inner.commit(tx)?;
            output
        };
        self.finalize_landing(&repo, owner, &token, now)?;
        self.forge_review(account, actor, change)
    }

    pub fn forge_comment_review(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        change: ChangeId,
        patchset: [u8; 32],
        request_id: &str,
        body: &str,
        path: Option<&str>,
        now: i64,
    ) -> Result<ForgeReview> {
        request_token(request_id)?;
        if body.is_empty()
            || body.len() > 4096
            || path.is_some_and(|p| p.len() > self.inner.config.model_limits.max_path_bytes)
        {
            return Err(Error::Limit("review comment"));
        }
        let replay_key = operation_key(account, actor, request_id);
        let fingerprint = *blake3::hash(&bounded(
            &("comment", change.0, patchset, body, path),
            12 * 1024,
        )?)
        .as_bytes();
        {
            let _gate = self.inner.lock()?;
            let db = self.inner.db.read_tx();
            self.inner
                .forge_authorize_in(&db, account, actor, ForgeAction::Read, None)?;
            if let Some(bytes) = db.get(&self.inner.spaces.meta, &replay_key)? {
                let replay: Replay = decode(&bytes)?;
                if replay.fingerprint != fingerprint {
                    return Err(Error::IdempotencyConflict);
                }
            } else {
                let mut record = self
                    .inner
                    .review_record_in(&db, account, change.0)?
                    .ok_or(Error::Invalid("unknown review"))?;
                let p = record
                    .patchsets
                    .iter()
                    .find(|p| p.id == patchset)
                    .ok_or(Error::Invalid("unknown review version"))?;
                let view = self.inner.forge_resolve_view_in(
                    &db,
                    account,
                    Some(actor),
                    ViewVersion(p.view),
                )?;
                if p.epoch != view.epoch().0 {
                    return Err(Error::StaleForgePolicy);
                }
                if path.is_some_and(|p| !view.allows_path(p)) {
                    return Err(Error::ForgeAccessDenied);
                }
                if record.comments.len() >= MAX_COMMENTS {
                    return Err(Error::Limit("review comments"));
                }
                record.comments.push(ForgeReviewComment {
                    author: actor.principal.clone(),
                    patchset_id: patchset,
                    body: body.into(),
                    path: path.map(str::to_owned),
                    created_at: now,
                });
                let bytes = bounded(&record, self.inner.config.landing.max_record_bytes)?;
                self.inner
                    .review_budget(record_key(account, change.0), bytes.len())?;
                let mut tx = self
                    .inner
                    .db
                    .write_tx()
                    .durability(Some(PersistMode::SyncAll));
                tx.insert(
                    &self.inner.spaces.meta,
                    record_key(account, change.0),
                    bytes,
                );
                tx.insert(
                    &self.inner.spaces.meta,
                    replay_key,
                    bounded(
                        &Replay {
                            fingerprint,
                            change: change.0,
                            patchset,
                        },
                        1024,
                    )?,
                );
                self.inner.commit(tx)?;
            }
        }
        self.forge_review_version(account, actor, change, Some(patchset))
    }
}

/// Store-private build context verified from a pinned predecessor, never client input.
pub(super) struct ReviewBuildPlan {
    pub target: CommitId,
    pub mapped_base: Option<CommitId>,
    pub projected: bool,
}
impl Inner {
    fn review_binding(&self, repo: &str, job: [u8; 32]) -> Result<Option<Binding>> {
        self.db
            .read_tx()
            .get(&self.spaces.meta, binding_key(repo, job))?
            .map(|v| decode(&v))
            .transpose()
    }
    fn bound_review(&self, repo: &str, job: [u8; 32]) -> Result<Option<(Record, usize, usize)>> {
        let Some(binding) = self.review_binding(repo, job)? else {
            return Ok(None);
        };
        let account = AccountId(binding.account);
        if account.repository_id() != repo {
            return Err(Error::Corrupt("review landing repository"));
        }
        let record = self
            .review_record_in(&self.db.read_tx(), &account, binding.change)?
            .ok_or(Error::Corrupt("review landing binding"))?;
        let p = record
            .patchsets
            .iter()
            .position(|p| p.id == binding.patchset)
            .ok_or(Error::Corrupt("review landing patchset"))?;
        let a = record.patchsets[p]
            .attempts
            .iter()
            .position(|a| a.job == job)
            .ok_or(Error::Corrupt("review landing attempt"))?;
        Ok(Some((record, p, a)))
    }
    pub(super) fn forge_review_admit_landing(
        &self,
        repo: &str,
        owner: LandingOwner,
        request: &LandingRequest,
        review: bool,
    ) -> Result<()> {
        if request.reference_name != "main" {
            return if review {
                Err(Error::Invalid("review target reference"))
            } else {
                Ok(())
            };
        }
        let db = self.db.read_tx();
        if self.forge_account_for_repository_in(&db, repo)?.is_none() {
            return if review {
                Err(Error::Invalid("review requires account"))
            } else {
                Ok(())
            };
        }
        if !review {
            return Err(Error::ForgeAccessDenied);
        }
        let id = crate::landing::job_id(repo, owner, &request.idempotency_key);
        let (record, pi, ai) = self
            .bound_review(repo, id)?
            .ok_or(Error::ForgeAccessDenied)?;
        let p = &record.patchsets[pi];
        let a = &p.attempts[ai];
        if a.owner != owner.0
            || a.token != request.idempotency_key
            || p.base != request.source_base.0
            || p.head != request.source_head.0
            || a.target != request.expected_head.0
            || a.generation != request.expected_generation
            || p.profile != request.check_profile
        {
            return Err(Error::Invalid("review landing binding mismatch"));
        }
        Ok(())
    }
    pub(super) fn forge_review_build_plan(
        &self,
        repo: &str,
        job: [u8; 32],
    ) -> Result<Option<ReviewBuildPlan>> {
        let Some((record, pi, ai)) = self.bound_review(repo, job)? else {
            return Ok(None);
        };
        let account = AccountId(record.account.clone());
        let p = &record.patchsets[pi];
        let a = &p.attempts[ai];
        if let Some(dependency) = &p.predecessor {
            let previous = self
                .review_record_in(&self.db.read_tx(), &account, dependency.change_id)?
                .ok_or(Error::Corrupt("review predecessor"))?;
            let pp = previous
                .patchsets
                .iter()
                .find(|p| p.id == dependency.patchset_id)
                .ok_or(Error::Corrupt("review predecessor version"))?;
            if p.base != pp.head {
                return Err(Error::Corrupt("review predecessor base"));
            }
            let mut matched = false;
            for pa in &pp.attempts {
                if self
                    .landing_record_for_review(repo, pa.job)?
                    .and_then(|j| j.candidate)
                    .is_some_and(|c| Some(c.head.0) == a.mapped_base)
                {
                    matched = true;
                    break;
                }
            }
            if !matched {
                return Err(Error::Corrupt("review predecessor candidate mapping"));
            }
        } else if let Some(canonical) = p.projection_base {
            let db = self.db.read_tx();
            if self
                .forge_projection_relation_in(
                    &db,
                    &account,
                    ViewVersion(p.view),
                    PolicyEpoch(p.epoch),
                    CommitId(p.base),
                )?
                .0
                != canonical
                || a.mapped_base != Some(canonical)
                || a.build_target != a.target
            {
                return Err(Error::Corrupt("review projection mapping"));
            }
        } else if a.mapped_base.is_some() || a.build_target != a.target {
            return Err(Error::Corrupt("unexpected review base mapping"));
        }
        Ok(Some(ReviewBuildPlan {
            target: CommitId(a.build_target),
            mapped_base: a.mapped_base.map(CommitId),
            projected: p.projection_base.is_some(),
        }))
    }
    fn review_approval_count(
        &self,
        db: &impl Readable,
        account: &AccountId,
        p: &Patchset,
        candidate: [u8; 32],
    ) -> Result<usize> {
        let mut seen = HashSet::new();
        let mut approvals = 0usize;
        for d in p.decisions.iter().rev() {
            if d.candidate != candidate || d.epoch != p.epoch || !seen.insert(&d.actor) {
                continue;
            }
            match self.forge_authorize_in(
                db,
                account,
                &ForgeActor {
                    principal: d.actor.clone(),
                },
                ForgeAction::Review,
                Some(PolicyEpoch(d.epoch)),
            ) {
                Ok(_) => {}
                Err(Error::ForgeAccessDenied | Error::StaleForgePolicy) => continue,
                Err(error) => return Err(error),
            }
            if d.decision == ForgeReviewDecision::RequestChanges {
                return Ok(0);
            }
            approvals += 1;
        }
        Ok(approvals)
    }
    fn review_ancestor(&self, repo: &str, wanted: [u8; 32], mut head: [u8; 32]) -> Result<bool> {
        for _ in 0..self.config.landing.candidate.max_ancestry {
            if head == wanted {
                return Ok(true);
            }
            if head == CommitId::ROOT.0 {
                return Ok(false);
            }
            let commit = self.review_commit(repo, head)?;
            if commit.parents.len() != 1 {
                return Err(Error::Invalid("review candidate ancestry"));
            }
            head = commit.parents[0].0;
        }
        Err(Error::Limit("review ancestry"))
    }
    /// Caller holds the Store gate. Returned records MUST be committed in the
    /// same write transaction as the native reference and landing receipt.
    pub(super) fn forge_review_before_finalize(
        &self,
        repo: &str,
        reference: &str,
        job: [u8; 32],
        head: [u8; 32],
        now: i64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if reference != "main" {
            return Ok(Vec::new());
        }
        let db = self.db.read_tx();
        let Some(account) = self.forge_account_for_repository_in(&db, repo)? else {
            return Ok(Vec::new());
        };
        let (mut record, pi, ai) = self
            .bound_review(repo, job)?
            .ok_or(Error::ForgeAccessDenied)?;
        if pi + 1 != record.patchsets.len() || ai + 1 != record.patchsets[pi].attempts.len() {
            return Err(Error::StaleForgeReview);
        }
        let p = &record.patchsets[pi];
        let a = &p.attempts[ai];
        let actor = ForgeActor {
            principal: a
                .land_actor
                .clone()
                .filter(|_| a.land_requested)
                .ok_or(Error::Invalid("review requires explicit land"))?,
        };
        self.forge_authorize_in(
            &db,
            &account,
            &actor,
            ForgeAction::Write,
            Some(PolicyEpoch(p.epoch)),
        )?;
        self.forge_resolve_view_in(&db, &account, Some(&actor), ViewVersion(p.view))?;
        if self.review_approval_count(&db, &account, p, job)? == 0 {
            return Err(Error::Invalid("review has no current approval"));
        }
        let mut predecessor = p.predecessor.clone();
        let mut seen = HashSet::from([record.change]);
        let mut writes = Vec::new();
        while let Some(dependency) = predecessor {
            if !seen.insert(dependency.change_id)
                || seen.len() > self.config.landing.candidate.max_stack
            {
                return Err(Error::Invalid("review dependency cycle or limit"));
            }
            let mut previous = self
                .review_record_in(&db, &account, dependency.change_id)?
                .ok_or(Error::Invalid("review predecessor missing"))?;
            let pp = previous
                .patchsets
                .last_mut()
                .filter(|p| p.id == dependency.patchset_id)
                .ok_or(Error::StaleForgeReview)?;
            self.forge_resolve_view_in(&db, &account, Some(&actor), ViewVersion(pp.view))?;
            if pp.landed.is_none() {
                let pa = pp
                    .attempts
                    .last()
                    .ok_or(Error::Invalid("review predecessor unprepared"))?;
                let parent_job = self
                    .landing_record_for_review(repo, pa.job)?
                    .ok_or(Error::Corrupt("review predecessor attempt"))?;
                if parent_job.state != LandingState::Checked
                    || pp.profile != p.profile
                    || parent_job.expires_at <= now
                    || self.review_approval_count(&db, &account, pp, pa.job)? == 0
                {
                    return Err(Error::Invalid(
                        "review predecessor is not approved and checked",
                    ));
                }
                let parent_head = parent_job
                    .candidate
                    .ok_or(Error::Corrupt("checked predecessor candidate"))?
                    .head
                    .0;
                if !self.review_ancestor(repo, parent_head, head)? {
                    return Err(Error::StaleForgeReview);
                }
                pp.landed = Some(parent_head);
            }
            predecessor = pp.predecessor.clone();
            writes.push((
                record_key(&account, previous.change),
                bounded(&previous, self.config.landing.max_record_bytes)?,
            ));
        }
        record.patchsets[pi].landed = Some(head);
        writes.push((
            record_key(&account, record.change),
            bounded(&record, self.config.landing.max_record_bytes)?,
        ));
        Ok(writes)
    }
    pub(super) fn forge_review_reject_publication(
        &self,
        repo: &str,
        reference: &str,
    ) -> Result<()> {
        if reference == "main"
            && self
                .forge_account_for_repository_in(&self.db.read_tx(), repo)?
                .is_some()
        {
            return Err(Error::ForgeAccessDenied);
        }
        Ok(())
    }
}
impl Store {
    /// Trusted checker integration: reviewed work never auto-lands merely because
    /// its check passed. Explicit review land owns that publication request.
    pub fn forge_review_manages_landing(&self, repo: &str, job: [u8; 32]) -> Result<bool> {
        let _gate = self.inner.lock()?;
        Ok(self.inner.review_binding(repo, job)?.is_some())
    }
}
