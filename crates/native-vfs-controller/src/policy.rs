//! Bounded snapshot policy inventory. Only repository-relative names and the
//! exact frozen ignore-file bytes cross a transport; native jj matchers and UI
//! callbacks stay in the client process.
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use jj_lib::{gitignore::GitIgnoreFile, repo_path::RepoPathBuf, working_copy::SnapshotOptions};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyKind {
    File,
    Symlink,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyCandidate {
    pub path: String,
    pub kind: PolicyKind,
    pub size: u64,
    pub tracked: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyIgnore {
    pub directory: String,
    pub contents: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyInventory {
    pub candidates: Vec<PolicyCandidate>,
    pub ignore_files: Vec<PolicyIgnore>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyDecision {
    Track,
    Ignored,
    NotAutoTracked,
    TooLarge { max_size: u64 },
}

#[async_trait]
pub trait SnapshotPolicy: Send + Sync {
    /// Opaque stable identity of the client's requested policy. The transport
    /// must bind it to the request before forwarding any inventory decisions.
    fn fingerprint(&self) -> [u8; 32];
    async fn evaluate(&self, inventory: &PolicyInventory) -> Result<Vec<PolicyDecision>>;
}

pub fn options_fingerprint(options: &SnapshotOptions<'_>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"native-jj-snapshot-policy-v1\0");
    hasher.update(
        format!(
            "{:?}|{:?}|{:?}|{}",
            options.base_ignores,
            options.start_tracking_matcher,
            options.force_tracking_matcher,
            options.max_new_file_size
        )
        .as_bytes(),
    );
    *hasher.finalize().as_bytes()
}

/// Apply ordinary jj tracking/ignore/size semantics without opening mount
/// paths. A transport validates its tighter configured count/frame limits
/// before constructing this inventory; these are additional absolute bounds.
pub fn evaluate_inventory(
    options: &SnapshotOptions<'_>,
    inventory: &PolicyInventory,
) -> Result<Vec<PolicyDecision>> {
    ensure!(
        inventory.candidates.len() <= 1_000_000 && inventory.ignore_files.len() <= 1_000_000,
        "snapshot policy inventory count exceeded"
    );
    let mut bytes = 0usize;
    let mut ignores = BTreeMap::new();
    for ignore in &inventory.ignore_files {
        ensure!(
            ignore.directory.len() <= 64 * 1024 && ignore.contents.len() <= 1024 * 1024,
            "snapshot ignore-policy input exceeded"
        );
        RepoPathBuf::from_internal_string(ignore.directory.clone())?;
        bytes = bytes
            .checked_add(ignore.directory.len())
            .and_then(|value| value.checked_add(ignore.contents.len()))
            .context("policy byte overflow")?;
        ensure!(
            bytes <= 64 * 1024 * 1024
                && ignores
                    .insert(ignore.directory.as_str(), ignore.contents.as_slice())
                    .is_none(),
            "snapshot ignore inventory budget or uniqueness violated"
        );
    }
    let mut cache = BTreeMap::new();
    let mut result = Vec::with_capacity(inventory.candidates.len());
    let mut previous = None;
    for candidate in &inventory.candidates {
        ensure!(
            !candidate.path.is_empty()
                && candidate.path.len() <= 64 * 1024
                && previous.is_none_or(|old: &str| old < candidate.path.as_str()),
            "snapshot candidates must be bounded, unique and ordered"
        );
        previous = Some(candidate.path.as_str());
        bytes = bytes
            .checked_add(candidate.path.len())
            .context("policy byte overflow")?;
        ensure!(
            bytes <= 256 * 1024 * 1024,
            "snapshot policy path budget exceeded"
        );
        let path = RepoPathBuf::from_internal_string(candidate.path.clone())?;
        let decision = if candidate.tracked {
            PolicyDecision::Track
        } else if !options.start_tracking_matcher.matches(&path) {
            PolicyDecision::NotAutoTracked
        } else if options.force_tracking_matcher.matches(&path) {
            PolicyDecision::Track
        } else if is_ignored(&candidate.path, &options.base_ignores, &ignores, &mut cache)? {
            PolicyDecision::Ignored
        } else if candidate.size > options.max_new_file_size {
            PolicyDecision::TooLarge {
                max_size: options.max_new_file_size,
            }
        } else {
            PolicyDecision::Track
        };
        if decision == PolicyDecision::Track {
            if let Some(progress) = options.progress {
                progress(&path);
            }
        }
        result.push(decision);
    }
    Ok(result)
}
fn is_ignored(
    path: &str,
    base: &Arc<GitIgnoreFile>,
    files: &BTreeMap<&str, &[u8]>,
    cache: &mut BTreeMap<String, Arc<GitIgnoreFile>>,
) -> Result<bool> {
    let parts: Vec<_> = path.split('/').collect();
    let mut prefix = String::new();
    let mut inherited = base.clone();
    for (index, name) in parts.iter().enumerate() {
        let policy = if let Some(policy) = cache.get(&prefix) {
            policy.clone()
        } else {
            let policy = if let Some(bytes) = files.get(prefix.as_str()) {
                let directory = RepoPathBuf::from_internal_string(prefix.clone())?;
                // A diagnostic label only. No client filesystem read occurs.
                inherited.chain(
                    &directory,
                    &PathBuf::from(&prefix).join(".gitignore"),
                    bytes,
                )?
            } else {
                inherited.clone()
            };
            cache.insert(prefix.clone(), policy.clone());
            policy
        };
        let next = if prefix.is_empty() {
            (*name).to_owned()
        } else {
            format!("{prefix}/{name}")
        };
        let relative = RepoPathBuf::from_internal_string(next.clone())?;
        if index + 1 == parts.len() {
            return Ok(policy.matches_file(&relative));
        }
        if policy.matches_dir(&relative) {
            return Ok(true);
        }
        prefix = next;
        inherited = policy;
    }
    Ok(false)
}
pub(super) struct OptionsPolicy<'a, 'o> {
    pub options: &'a SnapshotOptions<'o>,
}
#[async_trait]
impl SnapshotPolicy for OptionsPolicy<'_, '_> {
    fn fingerprint(&self) -> [u8; 32] {
        options_fingerprint(self.options)
    }
    async fn evaluate(&self, inventory: &PolicyInventory) -> Result<Vec<PolicyDecision>> {
        evaluate_inventory(self.options, inventory)
    }
}
