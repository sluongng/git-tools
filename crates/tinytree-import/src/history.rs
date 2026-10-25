//! Verified, public-parent-only Git history migration. Provenance is private
//! retained data, never an extra public Git header or an arbitrary anchor grant.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
#[path = "history_graph.rs"]
mod graph;
#[path = "history_io.rs"]
mod io;
#[path = "history_state.rs"]
mod state;
#[path = "history_worker.rs"]
mod worker;

pub const POLICY: &str = "tinytree-git-history-v1";
#[derive(Clone)]
pub struct HistoryLimits {
    pub max_objects: usize,
    pub max_commits: usize,
    pub max_entries: usize,
    pub max_depth: usize,
    pub max_file_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_total_bytes: u64,
    pub max_memory_bytes: u64,
    pub max_manifest_bytes: usize,
    pub timeout: Duration,
}
impl Default for HistoryLimits {
    fn default() -> Self {
        Self {
            max_objects: 100_000,
            max_commits: 10_000,
            max_entries: 1_000_000,
            max_depth: 128,
            max_file_bytes: 64 * 1024 * 1024,
            max_metadata_bytes: 16 * 1024 * 1024,
            max_total_bytes: 2 * 1024 * 1024 * 1024,
            max_memory_bytes: 1024 * 1024 * 1024,
            max_manifest_bytes: 16 * 1024 * 1024,
            timeout: Duration::from_secs(600),
        }
    }
}
impl HistoryLimits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=100_000).contains(&self.max_objects)
                && (1..=10_000).contains(&self.max_commits)
                && (1..=1_000_000).contains(&self.max_entries)
                && (1..=128).contains(&self.max_depth)
                && (1..=256 * 1024 * 1024).contains(&self.max_file_bytes)
                && (1024..=16 * 1024 * 1024).contains(&self.max_metadata_bytes)
                && (4096..=32 * 1024 * 1024).contains(&self.max_manifest_bytes)
                && self.max_total_bytes > 0
                && self.max_total_bytes <= 16 * 1024 * 1024 * 1024
                && self.max_memory_bytes > 0
                && self.max_memory_bytes <= 4 * 1024 * 1024 * 1024
                && !self.timeout.is_zero()
                && self.timeout <= Duration::from_secs(1800),
            "invalid history import limits"
        );
        Ok(())
    }
}
#[derive(Clone)]
pub struct HistoryOptions {
    pub source_git: PathBuf,
    pub revision: String,
    pub state_dir: PathBuf,
    pub reference: String,
    pub expected_generation: u64,
    pub git_program: PathBuf,
    pub limits: HistoryLimits,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryReport {
    pub policy: String,
    pub git_head: String,
    pub native_head: String,
    pub publication_generation: Option<u64>,
    pub current_generation: u64,
    pub current_native_head: Option<String>,
    pub is_current: bool,
    pub provenance_checkpoint: String,
    pub provenance_manifest: String,
    pub retention_hold: String,
    pub commits: usize,
    pub git_objects: usize,
    pub source_bytes: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryProof {
    pub retention_hold: String,
    pub provenance_checkpoint: String,
}
/// Accepted physical work owns its journal fence even if the caller cancels.
pub async fn import_history(
    config: tinytree_client::Config,
    options: HistoryOptions,
) -> Result<HistoryReport> {
    options.limits.validate()?;
    let deadline = tokio::time::Instant::now() + options.limits.timeout;
    let job = tokio::spawn(async move { worker::run(config, options, deadline).await });
    tokio::time::timeout_at(deadline, job)
        .await
        .map_err(|_| anyhow::anyhow!("history import deadline exceeded; private intent retained"))?
        .map_err(|_| anyhow::anyhow!("history import worker failed; private intent retained"))?
}
/// Verify retained provenance and every associated native/Git object without
/// trusting supplied mapping rows. Does not publish or grant an existing anchor.
pub async fn verify_history(
    config: tinytree_client::Config,
    proof: HistoryProof,
    limits: HistoryLimits,
) -> Result<HistoryReport> {
    limits.validate()?;
    validate_proof(&proof.retention_hold, &proof.provenance_checkpoint)?;
    let deadline = tokio::time::Instant::now() + limits.timeout;
    let job = tokio::spawn(async move { worker::verify(config, proof, limits, deadline).await });
    tokio::time::timeout_at(deadline, job)
        .await
        .map_err(|_| anyhow::anyhow!("history verification deadline exceeded"))?
        .map_err(|_| anyhow::anyhow!("history verification worker failed"))?
}

pub(super) fn validate_proof(hold: &str, checkpoint: &str) -> Result<()> {
    const PREFIX: &str = "_tinytree/hold/v1/";
    ensure!(
        state::valid_hex(checkpoint, 64) && hold.len() == PREFIX.len() + 32 + 1 + 64,
        "invalid provenance hold shape"
    );
    let suffix = hold
        .strip_prefix(PREFIX)
        .ok_or_else(|| anyhow::anyhow!("invalid provenance hold shape"))?;
    let (client, head) = suffix
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("invalid provenance hold shape"))?;
    ensure!(
        state::valid_hex(client, 32) && head == checkpoint,
        "invalid provenance hold binding"
    );
    Ok(())
}
