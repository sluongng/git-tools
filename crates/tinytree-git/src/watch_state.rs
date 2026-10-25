use super::{WatchStatus, now_ms};
use crate::state::{Resources, valid_hex};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
    sync::Arc,
};
const CAP: usize = 65536;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    identity: [u8; 32],
    status: WatchStatus,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    digest: [u8; 32],
    record: Record,
}
pub(super) struct Disk {
    pub directory: PrivateDir,
    pub lock: Arc<File>,
}
impl Disk {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        let directory = PrivateDir::create(path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(directory.path().join("watch.lock"))?;
        directory.open_existing_file("watch.lock", 0)?;
        lock.try_lock_exclusive()
            .map_err(|_| anyhow::anyhow!("local watcher writer is busy"))?;
        match std::fs::symlink_metadata(directory.path().join("watch.next")) {
            Ok(_) => {
                let candidate = directory.open_existing_file("watch.next", CAP as u64)?;
                ensure!(
                    candidate.metadata()?.nlink() == 1,
                    "watch staging hardlink preserved"
                );
                match std::fs::symlink_metadata(directory.path().join("watch.json")) {
                    Ok(_) => {
                        let _ = decode(&directory)?;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
                drop(candidate);
                std::fs::remove_file(directory.path().join("watch.next"))?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        File::open(directory.path())?.sync_all()?;
        Ok(Arc::new(Self {
            directory,
            lock: Arc::new(lock),
        }))
    }
    pub fn load(&self, identity: [u8; 32]) -> Result<Option<WatchStatus>> {
        match std::fs::symlink_metadata(self.directory.path().join("watch.json")) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
            Ok(_) => {
                let record = decode(&self.directory)?;
                ensure!(
                    record.identity == identity,
                    "watch source/destination/credential identity changed; explicit migration required"
                );
                Ok(Some(record.status))
            }
        }
    }
    pub async fn save(
        self: &Arc<Self>,
        identity: [u8; 32],
        status: &mut WatchStatus,
        physical: Arc<Resources>,
    ) -> Result<()> {
        status.updated_at_unix_ms = now_ms()?;
        status.writer_fence_held = None;
        status.clock_skewed = false;
        validate(status)?;
        let record = Record {
            identity,
            status: status.clone(),
        };
        let raw = serde_json::to_vec(&record)?;
        ensure!(raw.len() <= CAP, "watch status byte limit exceeded");
        let bytes = serde_json::to_vec(&Envelope {
            digest: *blake3::hash(&raw).as_bytes(),
            record,
        })?;
        ensure!(bytes.len() <= CAP, "watch status envelope exceeds bound");
        let disk = self.clone();
        #[cfg(test)]
        let phase = status.phase.clone();
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(pause) = physical.watch.as_ref().and_then(|w| w.test_pause.as_ref()) {
                pause.block(Some(&phase));
            }
            let _physical = physical;
            let path = disk.directory.path().join("watch.next");
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(path, disk.directory.path().join("watch.json"))?;
            File::open(disk.directory.path())?.sync_all()?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}
fn validate(s: &WatchStatus) -> Result<()> {
    ensure!(
        s.version == 1
            && s.source_reference.len() <= 4096
            && !s.source_reference.is_empty()
            && s.destination_reference.starts_with("refs/heads/")
            && s.destination_reference.len() <= 4096
            && s.consecutive_failures <= 32
            && (100..=3_600_000).contains(&s.poll_interval_ms)
            && (100..=60_000).contains(&s.probe_timeout_ms)
            && (s.poll_interval_ms..=3_600_000).contains(&s.max_backoff_ms),
        "invalid watch status policy"
    );
    if let Some(p) = &s.observed_desired {
        ensure!(
            p.native_head
                .as_ref()
                .is_none_or(|n| p.generation > 0 && valid_hex(n, 64)),
            "invalid observed watch position"
        );
    }
    if let Some(p) = &s.delivered {
        ensure!(
            p.generation > 0 && valid_hex(&p.native_head, 64) && valid_hex(&p.git_head, 40),
            "invalid watch delivered position"
        );
    }
    ensure!(
        s.pending_export_phase
            .as_deref()
            .is_none_or(|p| matches!(p, "acquiring" | "held" | "ready" | "release")),
        "invalid pending export phase"
    );
    Ok(())
}
fn decode(directory: &PrivateDir) -> Result<Record> {
    let bytes = directory.read("watch.json", CAP)?;
    let e: Envelope = serde_json::from_slice(&bytes).context("invalid watch status")?;
    ensure!(
        *blake3::hash(&serde_json::to_vec(&e.record)?).as_bytes() == e.digest,
        "watch status checksum differs"
    );
    validate(&e.record.status)?;
    Ok(e.record)
}
pub(super) fn read_status(path: &Path) -> Result<WatchStatus> {
    let directory = PrivateDir::open_existing(path)?;
    let mut status = decode(&directory)?.status;
    let lock = directory.open_existing_file("watch.lock", 0)?;
    let held = match fs2::FileExt::try_lock_shared(&lock) {
        Ok(()) => false,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => true,
        Err(e) => return Err(e.into()),
    };
    status.writer_fence_held = Some(held);
    let now = now_ms()?;
    status.clock_skewed = status.updated_at_unix_ms > now
        || status
            .last_full_verification_unix_ms
            .is_some_and(|t| t > now)
        || status
            .last_receiver_ref_check_unix_ms
            .is_some_and(|t| t > now)
        || status
            .observed_desired
            .as_ref()
            .is_some_and(|p| p.observed_at_unix_ms > now);
    let stale = status.observed_desired.as_ref().is_none_or(|p| {
        now.saturating_sub(p.observed_at_unix_ms)
            > status
                .poll_interval_ms
                .saturating_add(status.probe_timeout_ms)
    });
    if !held || status.clock_skewed || stale {
        status.last_probe_succeeded = false;
        status.lag_at_last_observation = None;
    }
    Ok(status)
}
