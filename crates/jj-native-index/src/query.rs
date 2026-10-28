fn model_error(error: m::Error) -> Error {
    if matches!(error, m::Error::Limit) {
        Error::Limit
    } else {
        Error::Canonical
    }
}
use super::*;
fn wire(id: m::CommitId) -> q::CommitId {
    q::CommitId {
        value: id.0.to_vec(),
    }
}
fn checked(id: Option<q::CommitId>) -> Result<CommitId, Error> {
    Ok(jj_id(m::CommitId(
        id.ok_or(Error::Corrupt)?
            .value
            .try_into()
            .map_err(|_| Error::Corrupt)?,
    )))
}
fn set(value: q::CommitSet, limit: usize) -> Result<Vec<CommitId>, Error> {
    if value.commits.len() > limit {
        return Err(Error::Limit);
    }
    let mut seen = BTreeSet::new();
    let mut result = Vec::with_capacity(value.commits.len());
    for id in value.commits {
        let id = checked(Some(id))?;
        if !seen.insert(id.clone()) {
            return Err(Error::Corrupt);
        }
        result.push(id)
    }
    Ok(result)
}
impl NativeReadonlyIndex {
    /// Upper bound on entries examined by one local overlay lookup. This lets
    /// a command charge its immutable linear scan before requesting metadata.
    pub fn local_query_lookup_items(&self) -> usize { self.checkpoint.drafts.len().saturating_add(1) }

    /// Locally retained COMMIT overlay metadata at this exact operation. Frozen
    /// baseline entries are excluded. Absence is NOT canonical read authority:
    /// a caller must still validate each boundary against its captured source.
    pub fn local_query_parents(&self, id: m::CommitId) -> Option<(u64, &[m::CommitId])> {
        if self.checkpoint.baseline.contains(&id) { return None; }
        self.checkpoint.drafts.iter().enumerate().find(|(_, draft)| draft.id() == id)
            .map(|(position, draft)| (position as u64, draft.parents.as_slice()))
    }

    /// Exact retained baseline for independent file queries. Local index deltas
    /// are deliberately excluded: COMMIT overlays do not authorize FILE reads.
    pub fn cloud_query_source(&self) -> Option<native_broker::CloudQuerySource> {
        match &self.checkpoint.source {
            CheckpointSource::LiveRoots => None,
            CheckpointSource::CloudSnapshot { pins, .. } => Some(pins.clone()),
        }
    }
    /// Borrow an already-open canonical cloud root without creating work or
    /// admitting local COMMIT overlays. The caller must fork an empty-overlay
    /// child and close only that child, never this cached root. Its reply owner
    /// keeps the checkpoint and cached chain alive until the child is released.
    pub async fn cached_cloud_root_scope(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<native_broker::QueryReply<native_broker::QueryScope>>, tonic::Status> {
        let CheckpointSource::CloudSnapshot { pins, .. } = &self.checkpoint.source else {
            return Ok(None);
        };
        if tokio::time::Instant::now() >= deadline {
            return Err(tonic::Status::deadline_exceeded("cached cloud scope deadline exceeded"));
        }
        let state = tokio::time::timeout_at(deadline, self.checkpoint.scope.lock())
            .await
            .map_err(|_| tonic::Status::deadline_exceeded("cached cloud scope deadline exceeded"))?;
        if tokio::time::Instant::now() >= deadline {
            return Err(tonic::Status::deadline_exceeded("cached cloud scope deadline exceeded"));
        }
        let Some(cached) = state.root_only.as_ref().or(state.cached.as_ref()) else {
            return Ok(None);
        };
        let mut root = cached;
        while let Some(parent) = &root.parent {
            root = parent;
        }
        let threshold = now().map_err(|_| tonic::Status::internal("cloud scope clock unavailable"))?
            .checked_add(1)
            .ok_or_else(|| tonic::Status::internal("cloud scope clock overflow"))?;
        if root.value.expires_at_unix_seconds <= threshold {
            return Ok(None);
        }
        if root.value.query_identity_version != 2
            || root.value.cloud_source_digest != Some(pins.digest())
        {
            return Err(tonic::Status::failed_precondition("cached cloud source binding mismatch"));
        }
        Ok(Some(native_broker::QueryReply {
            value: root.value.clone(),
            owner: Arc::new((self.clone(), cached.clone())),
        }))
    }
    pub(super) fn verify_draft(
        &self,
        descriptor: m::Descriptor,
        bytes: &[u8],
        id: m::CommitId,
    ) -> Result<Draft, Error> {
        if bytes.len() > self.core.limits.max_metadata_bytes {
            return Err(Error::Limit);
        }
        if descriptor.key.kind != m::Kind::Commit
            || descriptor.key.id.0 != id.0
            || id == m::CommitId::ROOT
            || descriptor.size_bytes != bytes.len() as u64
        {
            return Err(Error::Canonical);
        }
        let limits = m::Limits {
            max_object_bytes: self.core.limits.max_metadata_bytes,
            max_items: self.core.limits.max_parents,
            ..Default::default()
        };
        m::verify(&descriptor, bytes, limits).map_err(model_error)?;
        let object = m::decode(bytes, limits).map_err(model_error)?;
        let canonical = m::encode(&object, limits).map_err(model_error)?;
        if canonical.bytes != bytes || canonical.descriptor != descriptor {
            return Err(Error::Canonical);
        }
        let m::Object::Commit(commit) = object else {
            return Err(Error::Canonical);
        };
        Ok(Draft {
            order: 0,
            descriptor,
            change: commit.change_id,
            parents: commit.parents,
        })
    }
    fn lease(
        &self,
        reply: native_broker::QueryReply<native_broker::QueryScope>,
        parent: Option<Arc<CachedScope>>,
    ) -> Result<Arc<CachedScope>, Error> {
        let expected = match &self.checkpoint.source {
            CheckpointSource::LiveRoots => None,
            CheckpointSource::CloudSnapshot { pins, .. } => Some(pins.digest()),
        };
        let invalid = if reply.value.query_identity_version != 2 {
            Some(Error::Unsupported)
        } else if reply.value.cloud_source_digest != expected
            || reply.value.expires_at_unix_seconds <= now()?
        {
            Some(Error::Corrupt)
        } else {
            None
        };
        if let Some(error) = invalid {
            self.core.enqueue(reply.value.handle, true, reply.owner);
            return Err(error);
        }
        Ok(Arc::new(CachedScope {
            value: reply.value,
            _owner: reply.owner,
            cleanup: self.core.cleanup.clone(),
            close_once: Default::default(),
            parent,
            timeout: Duration::from_millis(self.core.limits.cleanup_timeout_ms),
        }))
    }
    async fn root_scope(
        &self,
        roots: Vec<m::CommitId>,
        replay_key: [u8; 32],
    ) -> Result<Arc<CachedScope>, Error> {
        let reply = match &self.checkpoint.source {
            CheckpointSource::LiveRoots => {
                self.core
                    .query
                    .open_scope(
                        native_broker::OpenQueryScope {
                            roots,
                            query_identity_version: 2,
                            overlay: Vec::new(),
                            replay_key,
                        },
                        self.core.owner(),
                    )
                    .await
            }
            CheckpointSource::CloudSnapshot { pins, .. } => {
                if pins.expected_anchors != roots {
                    return Err(Error::Corrupt);
                }
                self.core
                    .query
                    .open_cloud_scope(
                        native_broker::OpenCloudQueryScope {
                            source: pins.clone(),
                            overlay: Vec::new(),
                            replay_key,
                        },
                        self.core.owner(),
                    )
                    .await
            }
        }
        .map_err(status_at(QueryStage::ScopeOpen))?;
        self.lease(reply, None)
    }
    async fn classify(
        &self,
        base: &Arc<CachedScope>,
        ids: &[m::CommitId],
    ) -> Result<Vec<bool>, Error> {
        if ids.len() > self.core.limits.max_drafts.max(self.core.limits.max_roots) {
            return Err(Error::Limit);
        }
        let batch = (self.core.limits.page_size as usize).min(64).max(1);
        let mut known = Vec::with_capacity(ids.len());
        for ids in ids.chunks(batch) {
            let reply = self
                .core
                .query
                .facts(
                    base.value.handle,
                    ids.iter()
                        .map(|id| q::GraphFact {
                            fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                                commit: Some(wire(*id)),
                            })),
                        })
                        .collect(),
                    Arc::new((self.core.owner(), base.clone())),
                )
                .await
                .map_err(status_at(QueryStage::ScopeClassify))?;
            if reply.value.answers.len() != ids.len() {
                return Err(Error::Corrupt);
            }
            for answer in reply.value.answers {
                match answer.answer {
                    Some(q::graph_fact_answer::Answer::HasId(value)) => known.push(value),
                    _ => return Err(Error::Corrupt),
                }
            }
        }
        Ok(known)
    }
    /// Extend this exact checkpoint using roots already admitted by the matching
    /// broker. The broker root-only scope validates retention and index readiness.
    /// No operation association, prior root or local draft record is overwritten.
    pub async fn with_remote_roots(&self, roots: Vec<m::CommitId>) -> Result<Self, Error> {
        persistence::require_durable_writer()?;
        if roots.len() > self.core.limits.max_roots {
            return Err(Error::Limit);
        }
        if roots.is_empty() {
            return Ok(self.clone());
        }
        if !matches!(self.checkpoint.source, CheckpointSource::LiveRoots) {
            return Err(Error::Unsupported);
        }
        let mut union: BTreeSet<_> = self.checkpoint.roots.iter().copied().collect();
        union.extend(roots.iter().copied());
        if union.len() > self.core.limits.max_roots {
            return Err(Error::Limit);
        }
        let union: Vec<_> = union.into_iter().collect();
        let proof = self.root_scope(union.clone(), random()).await?;
        if self
            .classify(&proof, &union)
            .await?
            .iter()
            .any(|known| !*known)
        {
            return Err(Error::Corrupt);
        }
        let added: Vec<_> = union
            .iter()
            .filter(|root| !self.checkpoint.roots.contains(root))
            .copied()
            .collect();
        if added.is_empty() {
            return Ok(self.clone());
        }
        let tips = self.core.storage.persist_checkpoint(
            &self.checkpoint.tips,
            &added,
            &[],
            &self.core.limits,
        )?;
        let next = self.core.load(tips)?;
        next.checkpoint.scope.lock().await.root_only = Some(proof);
        Ok(next)
    }

    /// Install a freshly admitted, canonical linear stack as local COMMIT
    /// metadata. The caller must verify the landing result and refresh admission
    /// before calling this method; neither a Commit nor this index is a remote
    /// retention or FILE-read capability.
    ///
    /// The first parent must already belong to this exact checkpoint. In
    /// particular, a frozen cloud source keeps its original pins and baseline:
    /// only the supplied COMMITs become local query overlays. No live root or
    /// moving reference is added, and prior operation associations are unchanged.
    /// An empty stack leaves the checkpoint unchanged.
    pub async fn with_admitted_commit_stack(&self, commits: &[Commit]) -> IndexResult<Self> {
        persistence::require_durable_writer().map_err(err)?;
        if commits.len() > 128 || commits.len() > self.core.limits.max_drafts {
            return Err(err(Error::Limit));
        }
        if commits.is_empty() {
            return Ok(self.clone());
        }
        let mut seen = BTreeSet::new();
        for (position, commit) in commits.iter().enumerate() {
            let id = model_id(commit.id()).map_err(err)?;
            if id == m::CommitId::ROOT
                || !seen.insert(id)
                || commit.parent_ids().len() != 1
                || (position > 0 && commit.parent_ids()[0] != *commits[position - 1].id())
            {
                return Err(err(Error::Invalid));
            }
        }
        if !self.has_id(&commits[0].parent_ids()[0]).await? {
            return Err(err(Error::Canonical));
        }
        let mut additions = 0usize;
        for commit in commits {
            if !self.has_id(commit.id()).await? {
                additions += 1;
            }
        }
        if self.checkpoint.drafts.len().saturating_add(additions) > self.core.limits.max_drafts {
            return Err(err(Error::Limit));
        }
        let mut mutable = self.start_modification();
        for commit in commits {
            mutable.add_commit(commit).await?;
        }
        NativeIndexStore {
            core: self.core.clone(),
        }
        .persist_index(mutable)
        .map_err(err)
    }

    /// Explicitly retain one freshly admitted canonical COMMIT as local index
    /// metadata. Unlike logical `has_id`, this does not treat an implicit
    /// ancestor as an existing local retention record. The caller still owns
    /// fresh admission and canonical landing-result verification.
    ///
    /// Existing explicit roots/drafts are verified and left unchanged. New
    /// records require parents already known in this exact checkpoint, including
    /// its bounded query scope. Scope refresh may revalidate existing COMMIT
    /// overlays; it does not admit additional canonical history. No live
    /// root, FILE authority, history walk or operation association is added.
    pub async fn with_admitted_commit_witness(&self, commit: &Commit) -> IndexResult<Self> {
        persistence::require_durable_writer().map_err(err)?;
        let id = model_id(commit.id()).map_err(err)?;
        if id == m::CommitId::ROOT || commit.parent_ids().is_empty() {
            return Err(err(Error::Invalid));
        }
        if commit.parent_ids().len() > self.core.limits.max_parents {
            return Err(err(Error::Limit));
        }
        let prior = self.checkpoint.drafts.iter().find(|draft| draft.id() == id);
        let explicit = prior.is_some() || self.checkpoint.roots.binary_search(&id).is_ok();
        if !explicit {
            if self.checkpoint.drafts.len() >= self.core.limits.max_drafts {
                return Err(err(Error::Limit));
            }
            for parent in commit.parent_ids() {
                let parent = model_id(parent).map_err(err)?;
                if parent == id || !self.has_id(&jj_id(parent)).await? {
                    return Err(err(Error::Canonical));
                }
            }
        }
        let metadata = self
            .core
            .objects
            .read_metadata_with_owner(
                m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(id.0),
                },
                self.core.owner(),
            )
            .await
            .map_err(|_| err(Error::Canonical))?;
        let mut draft = self
            .verify_draft(metadata.descriptor(), metadata.bytes(), id)
            .map_err(err)?;
        if draft.change.0.as_slice() != commit.change_id().as_bytes()
            || draft
                .parents
                .iter()
                .map(|id| id.0.as_slice())
                .ne(commit.parent_ids().iter().map(|id| id.as_bytes()))
            || prior.is_some_and(|prior| !prior.canonical_eq(&draft))
        {
            return Err(err(Error::Canonical));
        }
        if explicit {
            return Ok(self.clone());
        }
        draft.order = self
            .checkpoint
            .drafts
            .iter()
            .map(|d| d.order)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| err(Error::Limit))?;
        let draft = Arc::new(draft);
        NativeIndexStore {
            core: self.core.clone(),
        }
        .persist_index(Box::new(NativeMutableIndex {
            inner: self.clone(),
            rebase_frozen_source: false,
            parents: self.checkpoint.tips.clone(),
            pending: vec![draft],
        }))
        .map_err(err)
    }

    pub(super) async fn scope(&self) -> Result<Arc<CachedScope>, Error> {
        let mut state = self.checkpoint.scope.lock().await;
        let threshold = now()?.checked_add(1).ok_or(Error::Invalid)?;
        if state
            .cached
            .as_ref()
            .is_some_and(|scope| scope.value.expires_at_unix_seconds > threshold)
        {
            return Ok(state.cached.as_ref().unwrap().clone());
        }
        if state.cached.take().is_some() {
            state.root_only = None;
            state.replay = random();
        }
        if state
            .root_only
            .as_ref()
            .is_some_and(|scope| scope.value.expires_at_unix_seconds <= threshold)
        {
            state.root_only = None;
            state.replay = random();
        }
        let base = if let Some(base) = &state.root_only {
            base.clone()
        } else {
            let roots = if self.checkpoint.roots.is_empty() {
                vec![m::CommitId::ROOT]
            } else {
                self.checkpoint.roots.clone()
            };
            let mut key = blake3::Hasher::new();
            key.update(b"jj-native-index-root-only-scope-v1");
            key.update(&state.replay);
            let base = self.root_scope(roots, *key.finalize().as_bytes()).await?;
            state.root_only = Some(base.clone());
            base
        };
        let ids: Vec<_> = self
            .checkpoint
            .drafts
            .iter()
            .filter(|draft| !self.checkpoint.baseline.contains(&draft.id()))
            .map(|draft| draft.id())
            .collect();
        let covered = match self.classify(&base, &ids).await {
            Ok(known) => known,
            Err(error) => {
                if error.query_code() == Some(tonic::Code::FailedPrecondition) {
                    state.root_only = None;
                    state.replay = random();
                }
                return Err(error);
            }
        };
        let mut overlay = Vec::with_capacity(ids.len());
        let mut total = 0usize;
        for (draft, covered) in self
            .checkpoint
            .drafts
            .iter()
            .filter(|draft| !self.checkpoint.baseline.contains(&draft.id()))
            .zip(covered)
        {
            let metadata = self
                .core
                .objects
                .read_metadata_with_owner(draft.descriptor.key, self.core.owner())
                .await
                .map_err(|_| Error::Canonical)?;
            let verified =
                self.verify_draft(metadata.descriptor(), metadata.bytes(), draft.id())?;
            if !verified.canonical_eq(draft) {
                return Err(Error::Canonical);
            }
            if !covered {
                total = total
                    .checked_add(metadata.bytes().len())
                    .ok_or(Error::Limit)?;
                if total > self.core.limits.max_checkpoint_bytes {
                    return Err(Error::Limit);
                }
                overlay.push(metadata.bytes().to_vec());
            }
        }
        let scope = if overlay.is_empty() {
            base.clone()
        } else {
            let reply = self
                .core
                .query
                .fork_scope(
                    native_broker::ForkQueryScope {
                        expected_cloud_source_digest: base.value.cloud_source_digest,
                        parent: base.value.handle,
                        overlay,
                        replay_key: state.replay,
                    },
                    Arc::new((self.core.owner(), base.clone())),
                )
                .await;
            match reply {
                Ok(reply) => self.lease(reply, Some(base.clone()))?,
                Err(error) => {
                    if error.code() == tonic::Code::FailedPrecondition {
                        state.root_only = None;
                        state.replay = random();
                    }
                    return Err(status_at(QueryStage::ScopeFork)(error));
                }
            }
        };
        state.cached = Some(scope.clone());
        state.root_only = None;
        Ok(scope)
    }
    async fn invalidate(&self, handle: [u8; 32]) {
        let mut state = self.checkpoint.scope.lock().await;
        if state
            .cached
            .as_ref()
            .is_some_and(|s| s.value.handle == handle)
        {
            state.cached = None;
            state.root_only = None;
            state.replay = random();
        }
    }
    pub(super) async fn fact(
        &self,
        fact: q::graph_fact::Fact,
    ) -> Result<q::graph_fact_answer::Answer, Error> {
        let key = memo::fact_key(self.checkpoint.memo_id, &fact, &self.core.limits)?;
        if let Some(entry) = key.and_then(|key| self.core.memo.get(&key)) {
            if let memo::Value::Fact(answer) = &entry.value {
                return Ok(answer.clone()); // Only fixed-size boolean answers are stored.
            }
        }
        for attempt in 0..2 {
            let scope = self.scope().await?;
            let reply = self
                .core
                .query
                .facts(
                    scope.value.handle,
                    vec![q::GraphFact {
                        fact: Some(fact.clone()),
                    }],
                    Arc::new((self.core.owner(), scope.clone())),
                )
                .await;
            match reply {
                Ok(mut reply) => {
                    if reply.value.answers.len() != 1 {
                        return Err(Error::Corrupt);
                    }
                    let answer = reply.value.answers.remove(0).answer.ok_or(Error::Corrupt)?;
                    if let Some(key) = key {
                        if !memo::matches(&fact, &answer) {
                            return Err(Error::Corrupt);
                        }
                        self.core
                            .memo
                            .insert(key, 0, || memo::Value::Fact(answer.clone()));
                    }
                    return Ok(answer);
                }
                Err(e) if attempt == 0 && e.code() == tonic::Code::FailedPrecondition => {
                    self.invalidate(scope.value.handle).await
                }
                Err(e) => return Err(status_at(QueryStage::Fact)(e)),
            }
        }
        Err(Error::Corrupt)
    }
    async fn shortest(
        &self,
        target: q::shortest_prefix_request::Target,
        heads: Vec<m::CommitId>,
        kind: q::PrefixKind,
        width: u32,
    ) -> Result<usize, Error> {
        let scope = self.scope().await?;
        let reply = self
            .core
            .query
            .shortest_prefix(
                scope.value.handle,
                target,
                heads,
                Arc::new((self.core.owner(), scope)),
            )
            .await
            .map_err(status_at(QueryStage::Prefix))?;
        if reply.value.kind != kind as i32 || reply.value.hex_length > width {
            return Err(Error::Corrupt);
        }
        Ok(reply.value.hex_length as usize)
    }
}
impl Drop for CachedScope {
    fn drop(&mut self) {
        self.request_close();
    }
}
#[async_trait]
impl Index for NativeReadonlyIndex {
    fn supports_exact_commit_membership(&self) -> bool {
        true
    }
    async fn shortest_unique_commit_id_prefix_len(&self, id: &CommitId) -> IndexResult<usize> {
        self.shortest(
            q::shortest_prefix_request::Target::Commit(wire(model_id(id).map_err(err)?)),
            vec![],
            q::PrefixKind::CommitPrefix,
            64,
        )
        .await
        .map_err(err)
    }
    async fn resolve_commit_id_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<CommitId>> {
        let scope = self.scope().await.map_err(err)?;
        let reply = self
            .core
            .query
            .resolve_commit_prefix(
                scope.value.handle,
                prefix.hex(),
                Arc::new((self.core.owner(), scope)),
            )
            .await
            .map_err(|e| err(status(e)))?;
        use q::commit_prefix_result::Result as R;
        match reply.value.result.ok_or_else(|| err(Error::Corrupt))? {
            R::NoMatch(_) => Ok(PrefixResolution::NoMatch),
            R::Ambiguous(_) => Ok(PrefixResolution::AmbiguousMatch),
            R::Single(single) => Ok(PrefixResolution::SingleMatch(
                checked(single.commit).map_err(err)?,
            )),
        }
    }
    async fn has_id(&self, id: &CommitId) -> IndexResult<bool> {
        let id = model_id(id).map_err(err)?;
        if id == m::CommitId::ROOT || self.checkpoint.drafts.iter().any(|d| d.id() == id) {
            return Ok(true);
        }
        match self
            .fact(q::graph_fact::Fact::HasId(q::HasId {
                commit: Some(wire(id)),
            }))
            .await
            .map_err(err)?
        {
            q::graph_fact_answer::Answer::HasId(v) => Ok(v),
            _ => Err(err(Error::Corrupt)),
        }
    }
    async fn is_ancestor(&self, a: &CommitId, d: &CommitId) -> IndexResult<bool> {
        match self
            .fact(q::graph_fact::Fact::IsAncestor(q::IsAncestor {
                ancestor: Some(wire(model_id(a).map_err(err)?)),
                descendant: Some(wire(model_id(d).map_err(err)?)),
            }))
            .await
            .map_err(err)?
        {
            q::graph_fact_answer::Answer::IsAncestor(v) => Ok(v),
            _ => Err(err(Error::Corrupt)),
        }
    }
    async fn common_ancestors(
        &self,
        left: &[CommitId],
        right: &[CommitId],
    ) -> IndexResult<Vec<CommitId>> {
        if left.len().saturating_add(right.len()) > self.core.limits.max_roots {
            return Err(err(Error::Limit));
        }
        let left = left
            .iter()
            .map(model_id)
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
        let right = right
            .iter()
            .map(model_id)
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
        match self
            .fact(q::graph_fact::Fact::CommonAncestors(q::CommonAncestors {
                left: left.into_iter().map(wire).collect(),
                right: right.into_iter().map(wire).collect(),
            }))
            .await
            .map_err(err)?
        {
            q::graph_fact_answer::Answer::CommonAncestors(v) => {
                set(v, self.core.limits.max_query_rows).map_err(err)
            }
            _ => Err(err(Error::Corrupt)),
        }
    }
    fn all_heads_for_gc(&self) -> IndexResult<Box<dyn Iterator<Item = CommitId> + '_>> {
        Err(IndexError::AllHeadsForGcUnsupported)
    }
    async fn heads(
        &self,
        candidates: &mut (dyn Iterator<Item = &CommitId> + Send),
    ) -> IndexResult<Vec<CommitId>> {
        let candidates = bounded_ids(candidates, &self.core.limits).map_err(err)?;
        match self
            .fact(q::graph_fact::Fact::Heads(q::Heads {
                commits: candidates.into_iter().map(wire).collect(),
            }))
            .await
            .map_err(err)?
        {
            q::graph_fact_answer::Answer::Heads(v) => {
                set(v, self.core.limits.max_query_rows).map_err(err)
            }
            _ => Err(err(Error::Corrupt)),
        }
    }
    async fn changed_paths_in_commit(
        &self,
        _: &CommitId,
    ) -> IndexResult<Option<Box<dyn Iterator<Item = RepoPathBuf> + '_>>> {
        Ok(None)
    }
    fn evaluate_revset(
        &self,
        expression: &ResolvedExpression,
        store: &Arc<Store>,
    ) -> Result<Box<dyn Revset>, RevsetEvaluationError> {
        let selected = revsets::selected_file_all(expression);
        let expression = revsets::compile(
            selected.unwrap_or(expression),
            &self.core.limits,
            selected.is_some(),
        )
        .map_err(rev_err)?;
        if path_filter::has_merged_path(&expression) {
            path_filter::validate_options(store).map_err(rev_err)?;
        }
        Ok(Box::new(revsets::NativeRevset {
            index: self.clone(),
            path_store: path_filter::has_path(&expression).then(|| store.clone()),
            expression,
            file_all: selected.map(|_| store.clone()),
        }))
    }
}
#[async_trait]
impl ChangeIdIndex for ChangeIndex {
    async fn resolve_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<ResolvedChangeTargets>> {
        let prefix = prefix.hex();
        if prefix.len() > 32 || self.heads.len() > self.index.core.limits.max_roots {
            return Err(err(Error::Limit));
        }
        let key = memo::change_key(self.index.checkpoint.memo_id, &self.heads, &prefix);
        if let Some(entry) = self.index.core.memo.get(&key) {
            if let memo::Value::Change(value) = &entry.value {
                return parse_change(value, self.index.core.limits.max_query_rows).map_err(err);
            }
        }
        let scope = self.index.scope().await.map_err(err)?;
        let reply = self
            .index
            .core
            .query
            .resolve_change_prefix(
                scope.value.handle,
                prefix,
                self.heads.clone(),
                Arc::new((self.index.core.owner(), scope)),
            )
            .await
            .map_err(|e| err(status(e)))?;
        let result =
            parse_change(&reply.value, self.index.core.limits.max_query_rows).map_err(err)?;
        let targets = match &reply.value.result {
            Some(q::change_prefix_result::Result::Single(v)) => v.targets.len(),
            _ => 0,
        };
        if let Some(bytes) = targets.checked_mul(160) {
            self.index
                .core
                .memo
                .insert(key, bytes, || memo::Value::Change(reply.value.clone()));
        }
        Ok(result)
    }
    async fn shortest_unique_prefix_len(&self, id: &ChangeId) -> IndexResult<usize> {
        let value: [u8; 16] = id.as_bytes().try_into().map_err(|_| err(Error::Invalid))?;
        self.index
            .shortest(
                q::shortest_prefix_request::Target::Change(q::ChangeId {
                    value: value.to_vec(),
                }),
                self.heads.clone(),
                q::PrefixKind::ChangePrefix,
                32,
            )
            .await
            .map_err(err)
    }
}

fn parse_change(
    value: &q::ChangePrefixResult,
    max_rows: usize,
) -> Result<PrefixResolution<ResolvedChangeTargets>, Error> {
    use q::change_prefix_result::Result as R;
    match value.result.as_ref().ok_or(Error::Corrupt)? {
        R::NoMatch(_) => Ok(PrefixResolution::NoMatch),
        R::Ambiguous(_) => Ok(PrefixResolution::AmbiguousMatch),
        R::Single(single) => {
            if single.change.as_ref().is_none_or(|c| c.value.len() != 16)
                || single.targets.len() > max_rows
            {
                return Err(Error::Corrupt);
            }
            let mut targets = Vec::with_capacity(single.targets.len());
            let mut seen = BTreeSet::new();
            for target in &single.targets {
                let bytes: [u8; 32] = target
                    .commit
                    .as_ref()
                    .ok_or(Error::Corrupt)?
                    .value
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::Corrupt)?;
                let id = CommitId::from_bytes(&bytes);
                if !seen.insert(id.clone()) {
                    return Err(Error::Corrupt);
                }
                let state = match target.visible {
                    Some(true) => ResolvedChangeState::Visible,
                    Some(false) => ResolvedChangeState::Hidden,
                    None => return Err(Error::Corrupt),
                };
                targets.push((id, state));
            }
            Ok(PrefixResolution::SingleMatch(ResolvedChangeTargets {
                targets,
            }))
        }
    }
}
