use crate::{ExportOptions, Status};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::OpenOptionsExt;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::Arc,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Mapping {
    pub git: String,
    pub tree: String,
    pub parents: Vec<String>,
    pub payload_blake3: [u8; 32],
    pub payload_size: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Position {
    pub generation: u64,
    pub native: String,
    pub git: String,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Acquiring,
    Held,
    Ready,
    Release,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Pending {
    pub generation: u64,
    pub native: String,
    pub expected_git: Option<String>,
    pub git: Option<String>,
    pub job: String,
    pub hold: String,
    pub acquire_generation: Option<u64>,
    pub release_generation: Option<u64>,
    pub phase: Phase,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    pub version: u32,
    pub scope: [u8; 32],
    pub client: [u8; 16],
    pub mappings: BTreeMap<String, Mapping>,
    pub delivered: Option<Position>,
    pub pending: Option<Pending>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    digest: [u8; 32],
    state: State,
}
pub(crate) struct WatchLease {
    pub fence: Arc<File>,
    pub cancel: tokio::sync::watch::Receiver<bool>,
    pub _owner: Arc<dyn std::any::Any + Send + Sync>,
    #[cfg(test)]
    pub test_pause: Option<Arc<crate::watch::TestPause>>,
}
pub(crate) struct Resources {
    pub state: PrivateDir,
    pub destination: PrivateDir,
    pub spool: PrivateDir,
    pub lock: File,
    pub options: ExportOptions,
    // Last: accepted payloads and writer fences outlive their watcher owner.
    pub watch: Option<WatchLease>,
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub(crate) fn valid_hex(text: &str, n: usize) -> bool {
    text.len() == n
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl Resources {
    pub fn open(options: ExportOptions) -> Result<Arc<Self>> {
        Self::open_watched(options, None)
    }
    pub fn open_watched(options: ExportOptions, watch: Option<WatchLease>) -> Result<Arc<Self>> {
        let state =
            PrivateDir::create(&options.state_dir).context("cannot open private adapter state")?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(state.path().join("writer.lock"))
            .context("cannot open adapter lock")?;
        state
            .open_existing_file("writer.lock", 0)
            .context("invalid adapter lock")?;
        lock.try_lock_exclusive()
            .map_err(|_| anyhow::anyhow!("adapter writer is busy"))?;
        let destination = PrivateDir::create(&options.destination_git)
            .context("cannot open private bare Git destination")?;
        ensure!(
            !destination.path().starts_with(state.path())
                && !state.path().starts_with(destination.path()),
            "adapter state and destination must be independent directories"
        );
        let spool = PrivateDir::create(state.path().join("spool"))?;
        // One fixed, bounded interrupted-checkpoint slot. A valid unpublished
        // candidate can be removed: no external mutation starts before save ACK.
        let next = state.path().join("state.next");
        match std::fs::symlink_metadata(&next) {
            Ok(_) => {
                use std::os::unix::fs::MetadataExt;
                let candidate = state
                    .open_existing_file("state.next", options.limits.max_state_bytes as u64)?;
                ensure!(
                    candidate.metadata()?.nlink() == 1,
                    "interrupted adapter staging hardlink preserved"
                );
                // Only the committed checkpoint is authority. Partial unACKed
                // candidates can be discarded after validating that checkpoint.
                match std::fs::symlink_metadata(state.path().join("state.json")) {
                    Ok(_) => {
                        let committed = decode(&state, options.limits.max_state_bytes)?;
                        validate(&committed, options.limits.max_commits)?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                drop(candidate);
                std::fs::remove_file(next)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        File::open(state.path())?.sync_all()?;
        Ok(Arc::new(Self {
            state,
            destination,
            spool,
            lock,
            options,
            watch,
        }))
    }
    pub fn load(&self, scope: [u8; 32]) -> Result<State> {
        let path = self.state.path().join("state.json");
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(State {
                version: 1,
                scope,
                client: *uuid::Uuid::new_v4().as_bytes(),
                mappings: BTreeMap::new(),
                delivered: None,
                pending: None,
            }),
            Err(error) => Err(error.into()),
            Ok(_) => {
                let state = decode(&self.state, self.options.limits.max_state_bytes)?;
                ensure!(
                    state.scope == scope,
                    "adapter source/destination/credential identity changed; explicit migration required"
                );
                validate(&state, self.options.limits.max_commits)?;
                Ok(state)
            }
        }
    }
    pub async fn save(self: &Arc<Self>, state: &State) -> Result<()> {
        let estimate = state.mappings.values().try_fold(4096usize, |n, m| {
            n.checked_add(512 + m.parents.len() * 80)
                .context("adapter state capacity overflow")
        })?;
        ensure!(
            estimate <= self.options.limits.max_state_bytes,
            "adapter state capacity exceeded"
        );
        let payload = serde_json::to_vec(state)?;
        let envelope = Envelope {
            digest: *blake3::hash(&payload).as_bytes(),
            state: state.clone(),
        };
        let bytes = serde_json::to_vec(&envelope)?;
        ensure!(
            bytes.len() <= self.options.limits.max_state_bytes,
            "adapter state byte limit exceeded"
        );
        let owner = self.clone();
        tokio::task::spawn_blocking(move || {
            let path = owner.state.path().join("state.next");
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            std::fs::rename(path, owner.state.path().join("state.json"))?;
            File::open(owner.state.path())?.sync_all()?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}
fn decode(directory: &PrivateDir, cap: usize) -> Result<State> {
    let bytes = directory.read("state.json", cap)?;
    let envelope: Envelope = serde_json::from_slice(&bytes).context("invalid adapter state")?;
    ensure!(
        *blake3::hash(&serde_json::to_vec(&envelope.state)?).as_bytes() == envelope.digest,
        "adapter state checksum differs"
    );
    ensure!(
        envelope.state.version == 1,
        "unsupported adapter policy version"
    );
    Ok(envelope.state)
}
fn validate(state: &State, maximum: usize) -> Result<()> {
    ensure!(
        state.mappings.len() <= maximum,
        "adapter mapping limit exceeded"
    );
    for (native, mapping) in &state.mappings {
        ensure!(
            valid_hex(native, 64)
                && *native != "00".repeat(32)
                && valid_hex(&mapping.git, 40)
                && valid_hex(&mapping.tree, 40)
                && mapping.payload_size <= 16 * 1024 * 1024
                && mapping.parents.len() <= 4096
                && mapping
                    .parents
                    .iter()
                    .all(|p| valid_hex(p, 64) && state.mappings.contains_key(p)),
            "invalid adapter mapping"
        );
    }
    if let Some(p) = &state.delivered {
        ensure!(
            p.generation > 0 && valid_hex(&p.native, 64) && valid_hex(&p.git, 40),
            "invalid delivered checkpoint"
        );
    }
    if let Some(p) = &state.pending {
        ensure!(
            p.generation > 0
                && valid_hex(&p.native, 64)
                && valid_hex(&p.job, 32)
                && p.expected_git.as_ref().is_none_or(|id| valid_hex(id, 40))
                && p.git.as_ref().is_none_or(|id| valid_hex(id, 40))
                && p.hold == format!("_tinytree/hold/v1/{}/{}", hex(&state.client), p.native),
            "invalid pending export"
        );
        ensure!(
            !matches!(p.phase, Phase::Ready | Phase::Release) || p.git.is_some(),
            "prepared export lacks Git identity"
        );
    }
    Ok(())
}
pub(crate) fn read_status(path: &Path) -> Result<Status> {
    let directory = PrivateDir::open_existing(path)?;
    let state = decode(&directory, 64 * 1024 * 1024)?;
    validate(&state, 500_000)?;
    Ok(Status {
        policy_version: state.version,
        source_generation: state.delivered.as_ref().map(|p| p.generation),
        native_head: state.delivered.as_ref().map(|p| p.native.clone()),
        git_head: state.delivered.as_ref().map(|p| p.git.clone()),
        pending_phase: state.pending.as_ref().map(|p| {
            match p.phase {
                Phase::Acquiring => "acquiring",
                Phase::Held => "held",
                Phase::Ready => "ready",
                Phase::Release => "release",
            }
            .into()
        }),
        mapped_commits: state.mappings.len(),
    })
}
pub(crate) fn usage(path: &Path, max_entries: usize, max_depth: usize) -> Result<u64> {
    fn walk(path: &Path, entries: &mut usize, depth: usize) -> Result<u64> {
        ensure!(depth > 0, "adapter disk inventory depth exceeded");
        let mut bytes = 0u64;
        for entry in std::fs::read_dir(path)? {
            *entries = entries
                .checked_sub(1)
                .context("adapter disk inventory limit exceeded")?;
            let entry = entry?;
            let meta = std::fs::symlink_metadata(entry.path())?;
            ensure!(
                !meta.file_type().is_symlink(),
                "adapter storage contains an unsupported symlink"
            );
            bytes = bytes
                .checked_add(if meta.is_dir() {
                    walk(&entry.path(), entries, depth - 1)?
                } else {
                    ensure!(meta.is_file(), "adapter storage contains special files");
                    meta.len()
                })
                .context("adapter disk size overflow")?;
        }
        Ok(bytes)
    }
    let mut entries = max_entries;
    walk(path, &mut entries, max_depth)
}

/// Atomic delivered checkpoint reader; never acquires the exporter writer fence.
pub(crate) fn delivered_checkpoint(
    path: &Path,
    scope: [u8; 32],
    limits: &crate::Limits,
) -> Result<Option<(Position, Mapping)>> {
    let directory = PrivateDir::open_existing(path)?;
    let state = decode(&directory, limits.max_state_bytes)?;
    validate(&state, limits.max_commits)?;
    ensure!(state.scope == scope, "export checkpoint identity differs");
    state
        .delivered
        .map(|position| {
            let mapping = state
                .mappings
                .get(&position.native)
                .context("delivered mapping absent")?
                .clone();
            ensure!(mapping.git == position.git, "delivered mapping differs");
            Ok((position, mapping))
        })
        .transpose()
}
/// Verify a frozen pending position against the authenticated export mapping.
pub(crate) fn mapped_checkpoint(
    path: &Path,
    scope: [u8; 32],
    limits: &crate::Limits,
    native: &str,
    git: &str,
) -> Result<Mapping> {
    let directory = PrivateDir::open_existing(path)?;
    let state = decode(&directory, limits.max_state_bytes)?;
    validate(&state, limits.max_commits)?;
    ensure!(state.scope == scope, "export checkpoint identity differs");
    let mapping = state
        .mappings
        .get(native)
        .context("selected delivered mapping absent")?;
    ensure!(mapping.git == git, "selected delivered mapping differs");
    Ok(mapping.clone())
}
