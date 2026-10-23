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
    pub(super) async fn scope(&self) -> Result<Arc<CachedScope>, Error> {
        let mut state = self.checkpoint.scope.lock().await;
        if state.cached.as_ref().is_some_and(|scope| {
            scope.value.expires_at_unix_seconds > now().unwrap_or(i64::MAX) + 1
        }) {
            return Ok(state.cached.as_ref().unwrap().clone());
        }
        if state.cached.take().is_some() {
            state.replay = random()
        }
        let mut overlay = Vec::with_capacity(self.checkpoint.drafts.len());
        let mut total = 0usize;
        for draft in &self.checkpoint.drafts {
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
            total = total
                .checked_add(metadata.bytes().len())
                .ok_or(Error::Limit)?;
            if total > self.core.limits.max_checkpoint_bytes {
                return Err(Error::Limit);
            }
            overlay.push(metadata.bytes().to_vec());
        }
        let roots = if self.checkpoint.roots.is_empty() {
            vec![m::CommitId::ROOT]
        } else {
            self.checkpoint.roots.clone()
        };
        let reply = self
            .core
            .query
            .open_scope(
                native_broker::OpenQueryScope {
                    roots,
                    overlay,
                    replay_key: state.replay,
                },
                self.core.owner(),
            )
            .await
            .map_err(status)?;
        if reply.value.expires_at_unix_seconds <= now()? {
            return Err(Error::Corrupt);
        }
        let scope = Arc::new(CachedScope {
            value: reply.value,
            _owner: reply.owner,
            cleanup: self.core.cleanup.clone(),
            timeout: Duration::from_millis(self.core.limits.cleanup_timeout_ms),
        });
        state.cached = Some(scope.clone());
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
            state.replay = random();
        }
    }
    async fn fact(&self, fact: q::graph_fact::Fact) -> Result<q::graph_fact_answer::Answer, Error> {
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
                    return reply.value.answers.remove(0).answer.ok_or(Error::Corrupt);
                }
                Err(e) if attempt == 0 && e.code() == tonic::Code::FailedPrecondition => {
                    self.invalidate(scope.value.handle).await
                }
                Err(e) => return Err(status(e)),
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
            .map_err(status)?;
        if reply.value.kind != kind as i32 || reply.value.hex_length > width {
            return Err(Error::Corrupt);
        }
        Ok(reply.value.hex_length as usize)
    }
}
impl Drop for CachedScope {
    fn drop(&mut self) {
        let _ = self.cleanup.try_send(Cleanup {
            handle: self.value.handle,
            scope: true,
            owner: self._owner.clone(),
            deadline: std::time::Instant::now() + self.timeout,
        });
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
        _: &Arc<Store>,
    ) -> Result<Box<dyn Revset>, RevsetEvaluationError> {
        let expression = revsets::compile(expression, &self.core.limits).map_err(rev_err)?;
        Ok(Box::new(revsets::NativeRevset {
            index: self.clone(),
            expression,
        }))
    }
}
#[async_trait]
impl ChangeIdIndex for ChangeIndex {
    async fn resolve_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<ResolvedChangeTargets>> {
        let scope = self.index.scope().await.map_err(err)?;
        let reply = self
            .index
            .core
            .query
            .resolve_change_prefix(
                scope.value.handle,
                prefix.hex(),
                self.heads.clone(),
                Arc::new((self.index.core.owner(), scope)),
            )
            .await
            .map_err(|e| err(status(e)))?;
        use q::change_prefix_result::Result as R;
        match reply.value.result.ok_or_else(|| err(Error::Corrupt))? {
            R::NoMatch(_) => Ok(PrefixResolution::NoMatch),
            R::Ambiguous(_) => Ok(PrefixResolution::AmbiguousMatch),
            R::Single(single) => {
                if single.change.as_ref().is_none_or(|c| c.value.len() != 16)
                    || single.targets.len() > self.index.core.limits.max_query_rows
                {
                    return Err(err(Error::Corrupt));
                }
                let mut targets = Vec::with_capacity(single.targets.len());
                let mut seen = BTreeSet::new();
                for target in single.targets {
                    let id = checked(target.commit).map_err(err)?;
                    if !seen.insert(id.clone()) {
                        return Err(err(Error::Corrupt));
                    }
                    let state = match target.visible {
                        Some(true) => ResolvedChangeState::Visible,
                        Some(false) => ResolvedChangeState::Hidden,
                        None => return Err(err(Error::Corrupt)),
                    };
                    targets.push((id, state));
                }
                Ok(PrefixResolution::SingleMatch(ResolvedChangeTargets {
                    targets,
                }))
            }
        }
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
