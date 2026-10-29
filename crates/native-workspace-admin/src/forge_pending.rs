//! Bounded source roots for exact hosted-review replay and local reconciliation.
//! The journal contains no credential values. Completed requests are removed
//! only after their local rewrite receipt has been durably reconciled.
use serde::{Deserialize, Serialize};
use std::io;
pub const FILE_NAME: &str = "forge-pending.json";
pub const MAX_BYTES: usize = 256 * 1024;
pub const MAX_REQUESTS: usize = 128;
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PendingReview {
    pub request_id: String,
    pub scope_hash: [u8; 32],
    pub change_id: String,
    pub patchset_id: Option<String>,
    #[serde(default)]
    pub review_dispatched: bool,
    #[serde(default)]
    pub superseded: bool,
    #[serde(default)]
    pub apply_write_started: bool,
    pub source_operation: String,
    pub source_base: String,
    pub source_head: String,
    pub view_version: [u8; 32],
    pub landed_projected_head: Option<String>,
    pub apply_operation: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub schema_version: u32,
    pub requests: Vec<PendingReview>,
    #[serde(default)]
    pub completed: Vec<PendingReview>,
}
impl Default for State {
    fn default() -> Self {
        Self {
            schema_version: 1,
            requests: vec![],
            completed: vec![],
        }
    }
}
fn invalid() -> io::Error {
    io::Error::other("invalid or over-budget forge source retention journal")
}
impl State {
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(invalid());
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        value.validate()?;
        Ok(value)
    }
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid())?;
        if bytes.len() > MAX_BYTES {
            return Err(invalid());
        }
        Ok(bytes)
    }
    pub fn validate(&self) -> io::Result<()> {
        if self.schema_version != 1
            || self.requests.len() > MAX_REQUESTS
            || self.completed.len() > MAX_REQUESTS
        {
            return Err(invalid());
        }
        let mut ids = std::collections::BTreeSet::new();
        for request in &self.requests {
            if request.request_id.is_empty()
                || request.request_id.len() > 128
                || !request
                    .request_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
                || !ids.insert(&request.request_id)
                || request.scope_hash == [0; 32]
                || !crate::remote_state::valid_hex(&request.change_id, 32)
                || (request.patchset_id.is_some() && !request.review_dispatched)
                || request
                    .patchset_id
                    .as_ref()
                    .is_some_and(|id| !crate::remote_state::valid_hex(id, 64))
                || self
                    .requests
                    .first()
                    .is_some_and(|first| first.change_id != request.change_id)
                || !crate::remote_state::valid_hex(&request.source_operation, 128)
                || !crate::remote_state::valid_hex(&request.source_base, 64)
                || !crate::remote_state::valid_hex(&request.source_head, 64)
                || request.source_head.bytes().all(|b| b == b'0')
                || request
                    .landed_projected_head
                    .as_ref()
                    .is_some_and(|id| !crate::remote_state::valid_hex(id, 64))
                || request
                    .apply_operation
                    .as_ref()
                    .is_some_and(|id| !crate::remote_state::valid_hex(id, 128))
            {
                return Err(invalid());
            }
        }
        for record in &self.completed {
            if !ids.insert(&record.request_id)
                || record.apply_operation.is_none()
                || record.landed_projected_head.is_none()
                || !record.apply_write_started
                || record.patchset_id.is_none()
            {
                return Err(invalid());
            }
            Self {
                schema_version: 1,
                requests: vec![record.clone()],
                completed: vec![],
            }
            .validate()?;
        }
        Ok(())
    }
    pub fn retain(&mut self, request: PendingReview) -> io::Result<()> {
        if let Some(existing) = self
            .requests
            .iter()
            .find(|old| old.request_id == request.request_id)
        {
            return if existing == &request {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        let mut next = self.clone();
        next.requests.push(request);
        next.encode()?;
        *self = next;
        Ok(())
    }
    /// A historical receipt cannot supersede a later uncertain local intent.
    pub fn acknowledge(
        &mut self,
        request: &str,
        patchset: &str,
        is_current: bool,
    ) -> io::Result<bool> {
        if !crate::remote_state::valid_hex(patchset, 64) {
            return Err(invalid());
        }
        let mut next = self.clone();
        let index = next
            .requests
            .iter()
            .position(|r| r.request_id == request)
            .ok_or_else(invalid)?;
        if next.requests[index]
            .patchset_id
            .as_ref()
            .is_some_and(|old| old != patchset)
        {
            return Err(invalid());
        }
        next.requests[index].patchset_id = Some(patchset.into());
        next.requests[index].review_dispatched = true;
        if !is_current {
            next.requests[index].superseded = true;
        }
        if is_current && !next.requests[index].superseded {
            for older in &mut next.requests[..index] {
                older.superseded = true;
            }
        }
        let current = is_current
            && !next.requests[index].superseded
            && !next.requests[index + 1..].iter().any(|r| !r.superseded);
        next.encode()?;
        *self = next;
        Ok(current)
    }
    pub fn commit_heads(&self) -> impl Iterator<Item = &str> {
        self.requests.iter().flat_map(|p| {
            std::iter::once(p.source_base.as_str())
                .chain(std::iter::once(p.source_head.as_str()))
                .chain(p.landed_projected_head.as_deref())
        })
    }
    pub fn operations(&self) -> impl Iterator<Item = &str> {
        self.requests.iter().flat_map(|p| {
            std::iter::once(p.source_operation.as_str()).chain(p.apply_operation.as_deref())
        })
    }
}
