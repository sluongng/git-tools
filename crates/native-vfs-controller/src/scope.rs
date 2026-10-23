//! Per-action admission, not physical cancellation. Once native I/O starts its
//! future must be awaited to completion; this scope gates the next logical
//! stage, never aborts an admitted worker or substitutes a fresh deadline.
use anyhow::Result;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct ActionScope {
    deadline: Instant,
    revoked: Arc<AtomicBool>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionScopeError {
    Deadline,
    Revoked,
}
impl std::fmt::Display for ActionScopeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Deadline => "native action deadline expired",
            Self::Revoked => "native action admission was revoked",
        })
    }
}
impl std::error::Error for ActionScopeError {}
impl ActionScope {
    /// `revoked == true` means EOF/revocation. A subsequent transport action may
    /// receive a new scope; cloned stages share this exact original deadline.
    pub fn new(deadline: Instant, revoked: Arc<AtomicBool>) -> Self {
        Self { deadline, revoked }
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    pub fn check(&self) -> Result<()> {
        if self.revoked.load(Ordering::Acquire) {
            return Err(ActionScopeError::Revoked.into());
        }
        if Instant::now() >= self.deadline {
            return Err(ActionScopeError::Deadline.into());
        }
        Ok(())
    }
    pub(super) fn bounded(&self, maximum: Instant) -> Self {
        Self {
            deadline: self.deadline.min(maximum),
            revoked: self.revoked.clone(),
        }
    }
}
/// Compatibility spelling for transport code using the admission terminology.
pub type AdmissionScope = ActionScope;
