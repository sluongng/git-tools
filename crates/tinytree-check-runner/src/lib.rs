//! Checks run operator-trusted source code; an immutable mount is not a sandbox.
//! Receipts describe one physical attempt, not a client-supplied verdict.
mod bep;
mod config;
mod process;
mod runner;

use anyhow::{Result, ensure};
pub use config::{Binary, Config, Mode, Profile};
pub use runner::{RunningCheck, recover, run, start};
use serde::{Deserialize, Serialize};

pub const MAX_REQUEST_BYTES: usize = 16 * 1024;
pub const MAX_RECEIPT_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub schema: u32,
    pub job_id: String,
    pub attempt_id: String,
    pub repository_id: String,
    pub candidate_commit: String,
    pub root_tree: String,
    pub profile_name: String,
    pub profile_version: String,
}
impl Request {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() <= MAX_REQUEST_BYTES,
            "check request exceeds limit"
        );
        let request: Self = serde_json::from_slice(bytes)?;
        request.validate()?;
        Ok(request)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(self.schema == 1, "unsupported check request schema");
        for value in [
            &self.job_id,
            &self.attempt_id,
            &self.candidate_commit,
            &self.root_tree,
            &self.profile_version,
        ] {
            ensure!(hex_id(value), "invalid check identity");
        }
        ensure!(
            self.candidate_commit != "0".repeat(64),
            "root commit is not a check candidate"
        );
        ensure!(
            name(&self.repository_id) && name(&self.profile_name),
            "invalid check scope"
        );
        Ok(())
    }
    /// Attempt identity is independent of mutable paths and is never reused for
    /// a different request. It also names the daemon's disposable workspace.
    pub fn workspace_id(&self) -> String {
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-check-attempt-v1\0");
        hash.update(&serde_json::to_vec(self).expect("request serialization"));
        hash.finalize().to_hex().to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Passed,
    Failed,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub invocation_id: String,
    pub bep_blake3: String,
    pub execution_log_blake3: Option<String>,
    pub requested_targets: Vec<String>,
    pub tests_verified: bool,
    pub bazel_exit_code: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema: u32,
    pub request: Request,
    pub workspace_id: String,
    pub outcome: Outcome,
    /// A stable non-secret code, never raw child stderr or private paths.
    pub reason: String,
    pub cleanup_confirmed: bool,
    pub evidence: Option<Evidence>,
}
impl Receipt {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self)?;
        ensure!(
            bytes.len() <= MAX_RECEIPT_BYTES,
            "check receipt exceeds limit"
        );
        Ok(bytes)
    }
    /// The service must compare this complete binding with its frozen lease.
    /// A digest or the Passed enum alone is never an execution proof.
    pub fn validate_for(&self, request: &Request) -> Result<()> {
        request.validate()?;
        ensure!(
            self.schema == 1
                && &self.request == request
                && self.workspace_id == request.workspace_id(),
            "check receipt binding differs"
        );
        ensure!(name(&self.reason), "invalid check receipt reason");
        if self.outcome == Outcome::Passed {
            ensure!(
                self.cleanup_confirmed
                    && self
                        .evidence
                        .as_ref()
                        .is_some_and(|e| e.bazel_exit_code == 0 && e.execution_log_blake3.is_some()),
                "incomplete passing receipt"
            );
        }
        if let Some(evidence) = &self.evidence {
            ensure!(
                hex_id(&evidence.bep_blake3)
                    && evidence
                        .execution_log_blake3
                        .as_ref()
                        .is_none_or(|v| hex_id(v)),
                "invalid evidence digest"
            );
            ensure!(
                !evidence.invocation_id.is_empty()
                    && evidence.invocation_id.len() <= 128
                    && evidence
                        .invocation_id
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() || b == b'-'),
                "invalid invocation identity"
            );
            ensure!(
                !evidence.requested_targets.is_empty()
                    && evidence.requested_targets.len() <= 128
                    && evidence
                        .requested_targets
                        .iter()
                        .map(String::len)
                        .sum::<usize>()
                        <= 8192,
                "invalid evidence targets"
            );
        }
        Ok(())
    }
}

pub(crate) fn hex_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub(crate) fn name(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | "..")
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

#[cfg(test)]
mod tests;
