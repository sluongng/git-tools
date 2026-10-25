//! Public-parent-only native to Git adapter. Native retention and export
//! selection are deliberately different graphs.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
#[cfg(unix)]
mod convert;
#[cfg(unix)]
mod git;
#[cfg(unix)]
mod state;
#[cfg(unix)]
pub mod watch;
#[cfg(unix)]
mod worker;

#[derive(Clone)]
pub struct Limits {
    pub max_commits: usize,
    pub max_objects: usize,
    pub max_entries: usize,
    pub max_depth: usize,
    pub max_file_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_total_bytes: u64,
    pub max_destination_bytes: u64,
    pub max_state_bytes: usize,
    pub max_memory_bytes: u64,
    pub timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_commits: 10_000,
            max_objects: 100_000,
            max_entries: 1_000_000,
            max_depth: 128,
            max_file_bytes: 64 * 1024 * 1024,
            max_metadata_bytes: 16 * 1024 * 1024,
            max_total_bytes: 2 * 1024 * 1024 * 1024,
            max_destination_bytes: 4 * 1024 * 1024 * 1024,
            max_state_bytes: 16 * 1024 * 1024,
            max_memory_bytes: 1024 * 1024 * 1024,
            timeout: Duration::from_secs(600),
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=500_000).contains(&self.max_commits)
                && (1..=1_000_000).contains(&self.max_objects)
                && (1..=2_000_000).contains(&self.max_entries)
                && (1..=256).contains(&self.max_depth)
                && (1..=256 * 1024 * 1024).contains(&self.max_file_bytes)
                && (1024..=16 * 1024 * 1024).contains(&self.max_metadata_bytes)
                && (4096..=64 * 1024 * 1024).contains(&self.max_state_bytes)
                && self.max_total_bytes > 0
                && self.max_total_bytes <= 16 * 1024 * 1024 * 1024
                && self.max_destination_bytes > 0
                && self.max_destination_bytes <= 1024 * 1024 * 1024 * 1024
                && self.max_memory_bytes <= 4 * 1024 * 1024 * 1024
                && !self.timeout.is_zero()
                && self.timeout <= Duration::from_secs(1800),
            "invalid Git export limits"
        );
        Ok(())
    }
}
#[derive(Clone)]
pub struct ExportOptions {
    pub state_dir: PathBuf,
    pub destination_git: PathBuf,
    pub git_program: PathBuf,
    pub source_reference: String,
    pub destination_reference: String,
    pub limits: Limits,
    #[cfg(test)]
    fault: Option<TestFault>,
}
impl ExportOptions {
    pub fn new(state_dir: PathBuf, destination_git: PathBuf, git_program: PathBuf) -> Self {
        Self {
            state_dir,
            destination_git,
            git_program,
            source_reference: "main".into(),
            destination_reference: "refs/heads/main".into(),
            limits: Limits::default(),
            #[cfg(test)]
            fault: None,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Report {
    pub source_generation: u64,
    pub native_head: String,
    pub git_head: String,
    pub exported_commits: usize,
    pub file_bytes: u64,
    pub reused: bool,
    pub delivered: bool,
    pub hold_released: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct Status {
    pub policy_version: u32,
    pub source_generation: Option<u64>,
    pub native_head: Option<String>,
    pub git_head: Option<String>,
    pub pending_phase: Option<String>,
    pub mapped_commits: usize,
}
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TestFault {
    BeforeObjects,
    BeforeRef,
    AfterRef,
    BeforeRelease,
}

/// The accepted worker owns its private lock through all physical Git/SDK work,
/// including when the caller drops this future or its original deadline expires.
#[cfg(unix)]
pub async fn export(config: tinytree_client::Config, options: ExportOptions) -> Result<Report> {
    options.limits.validate()?;
    let deadline = tokio::time::Instant::now() + options.limits.timeout;
    let task = tokio::spawn(async move { worker::run(config, options, deadline).await });
    tokio::time::timeout_at(deadline, task)
        .await
        .map_err(|_| {
            anyhow::anyhow!("Git export deadline exceeded; pending state must be reconciled")
        })?
        .map_err(|_| anyhow::anyhow!("Git export worker failed; pending state retained"))?
}
#[cfg(not(unix))]
pub async fn export(_: tinytree_client::Config, _: ExportOptions) -> Result<Report> {
    anyhow::bail!("local Git export currently requires a Unix host")
}
#[cfg(unix)]
pub fn status(state_dir: &std::path::Path) -> Result<Status> {
    state::read_status(state_dir)
}
#[cfg(not(unix))]
pub fn status(_: &std::path::Path) -> Result<Status> {
    anyhow::bail!("local Git export currently requires a Unix host")
}

#[cfg(all(test, unix))]
mod tests;
