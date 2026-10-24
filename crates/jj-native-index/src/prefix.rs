//! Owned narrow-set prefix delegation; replies never authorize object fetching.
use super::*;
use jj_lib::id_prefix::PrefixDisambiguationIndex;

pub(super) struct PrefixIndex {
    index: NativeReadonlyIndex,
    expression: q::GraphExpr,
    digest: [u8; 32],
}
impl PrefixIndex {
    pub fn new(index: NativeReadonlyIndex, expression: &q::GraphExpr) -> Result<Self, Error> {
        let digest =
            native_broker::prefix_selection_digest(expression, index.core.limits.max_segment_bytes)
                .map_err(status)?;
        Ok(Self {
            index,
            expression: expression.clone(),
            digest,
        })
    }
    fn binding(&self, value: Option<&[u8]>) -> Result<(), Error> {
        if value != Some(self.digest.as_slice()) {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
    async fn shortest(
        &self,
        target: q::shortest_prefix_request::Target,
        kind: q::PrefixKind,
        width: u32,
    ) -> IndexResult<Option<usize>> {
        let scope = self.index.scope().await.map_err(err)?;
        let reply = self
            .index
            .core
            .query
            .shortest_prefix_selected(
                scope.value.handle,
                target,
                vec![],
                self.expression.clone(),
                Arc::new((self.index.core.owner(), scope)),
            )
            .await
            .map_err(|e| err(status(e)))?;
        self.binding(reply.value.selection_digest.as_deref())
            .map_err(err)?;
        if reply.value.kind != kind as i32 || reply.value.hex_length > width {
            return Err(err(Error::Corrupt));
        }
        match reply.value.in_selection {
            // The generic graph API permits zero for a singleton. Pinned jj's
            // narrow IdIndex requires one digit before client ref-name policy.
            Some(true) => Ok(Some((reply.value.hex_length as usize).max(1))),
            Some(false) => Ok(None),
            None => Err(err(Error::Corrupt)),
        }
    }
}
#[async_trait]
impl PrefixDisambiguationIndex for PrefixIndex {
    async fn resolve_commit_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<CommitId>> {
        let scope = self.index.scope().await.map_err(err)?;
        let reply = self
            .index
            .core
            .query
            .resolve_commit_prefix_selected(
                scope.value.handle,
                prefix.hex(),
                self.expression.clone(),
                Arc::new((self.index.core.owner(), scope)),
            )
            .await
            .map_err(|e| err(status(e)))?;
        self.binding(reply.value.selection_digest.as_deref())
            .map_err(err)?;
        let result = reply.value.result.ok_or_else(|| err(Error::Corrupt))?;
        let resolved: IndexResult<PrefixResolution<CommitId>> = match result {
            q::commit_prefix_result::Result::NoMatch(_) => Ok(PrefixResolution::NoMatch),
            q::commit_prefix_result::Result::Ambiguous(_) => Ok(PrefixResolution::AmbiguousMatch),
            q::commit_prefix_result::Result::Single(v) => {
                let id = v.commit.ok_or_else(|| err(Error::Corrupt))?;
                let bytes: [u8; 32] = id.value.try_into().map_err(|_| err(Error::Corrupt))?;
                Ok(PrefixResolution::SingleMatch(CommitId::from_bytes(&bytes)))
            }
        };
        // Adapt only a fully checked successful answer, never an RPC or
        // malformed-response error, to jj's narrow IdIndex empty-prefix policy.
        resolved.map(|value| {
            if prefix.hex().is_empty() {
                PrefixResolution::AmbiguousMatch
            } else {
                value
            }
        })
    }
    async fn resolve_change_prefix(
        &self,
        prefix: &HexPrefix,
    ) -> IndexResult<PrefixResolution<ChangeId>> {
        let scope = self.index.scope().await.map_err(err)?;
        let reply = self
            .index
            .core
            .query
            .resolve_change_prefix_selected(
                scope.value.handle,
                prefix.hex(),
                vec![],
                self.expression.clone(),
                Arc::new((self.index.core.owner(), scope)),
            )
            .await
            .map_err(|e| err(status(e)))?;
        self.binding(reply.value.selection_digest.as_deref())
            .map_err(err)?;
        let result = reply.value.result.ok_or_else(|| err(Error::Corrupt))?;
        let resolved: IndexResult<PrefixResolution<ChangeId>> = match result {
            q::change_prefix_result::Result::NoMatch(_) => Ok(PrefixResolution::NoMatch),
            q::change_prefix_result::Result::Ambiguous(_) => Ok(PrefixResolution::AmbiguousMatch),
            q::change_prefix_result::Result::Single(v) => {
                if v.targets.len() > self.index.core.limits.max_query_rows
                    || v.targets.iter().any(|t| {
                        t.visible.is_none()
                            || t.commit.as_ref().is_none_or(|id| id.value.len() != 32)
                    })
                {
                    return Err(err(Error::Corrupt));
                }
                let change = v.change.ok_or_else(|| err(Error::Corrupt))?;
                let bytes: [u8; 16] = change.value.try_into().map_err(|_| err(Error::Corrupt))?;
                Ok(PrefixResolution::SingleMatch(ChangeId::from_bytes(&bytes)))
            }
        };
        resolved.map(|value| {
            if prefix.hex().is_empty() {
                PrefixResolution::AmbiguousMatch
            } else {
                value
            }
        })
    }
    async fn shortest_commit_prefix_len(&self, id: &CommitId) -> IndexResult<Option<usize>> {
        let id = model_id(id).map_err(err)?;
        self.shortest(
            q::shortest_prefix_request::Target::Commit(q::CommitId {
                value: id.0.to_vec(),
            }),
            q::PrefixKind::CommitPrefix,
            64,
        )
        .await
    }
    async fn shortest_change_prefix_len(&self, id: &ChangeId) -> IndexResult<Option<usize>> {
        let value: [u8; 16] = id.as_bytes().try_into().map_err(|_| err(Error::Invalid))?;
        self.shortest(
            q::shortest_prefix_request::Target::Change(q::ChangeId {
                value: value.to_vec(),
            }),
            q::PrefixKind::ChangePrefix,
            32,
        )
        .await
    }
}
