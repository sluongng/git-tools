use super::{PushStatus, validate_status};
use anyhow::{Result, ensure};
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
pub(super) const CAP: usize = 65536;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    identity: [u8; 32],
    status: PushStatus,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    digest: [u8; 32],
    record: Record,
}
pub(super) struct Disk {
    pub dir: PrivateDir,
    pub lock: Arc<File>,
}
impl Disk {
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        let dir = PrivateDir::create(path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(dir.path().join("push.lock"))?;
        dir.open_existing_file("push.lock", 0)?;
        lock.try_lock_exclusive()
            .map_err(|_| anyhow::anyhow!("push writer is busy or physically draining"))?;
        match std::fs::symlink_metadata(dir.path().join("push.next")) {
            Ok(_) => {
                let candidate = dir.open_existing_file("push.next", CAP as u64)?;
                ensure!(
                    candidate.metadata()?.nlink() == 1,
                    "push staging hardlink preserved"
                );
                match std::fs::symlink_metadata(dir.path().join("push.json")) {
                    Ok(_) => {
                        let _ = decode(&dir)?;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
                drop(candidate);
                std::fs::remove_file(dir.path().join("push.next"))?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        File::open(dir.path())?.sync_all()?;
        Ok(Arc::new(Self {
            dir,
            lock: Arc::new(lock),
        }))
    }
    pub fn load(&self, identity: [u8; 32]) -> Result<Option<PushStatus>> {
        match std::fs::symlink_metadata(self.dir.path().join("push.json")) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
            Ok(_) => {
                let r = decode(&self.dir)?;
                ensure!(
                    r.identity == identity,
                    "push identity changed; explicit migration required"
                );
                Ok(Some(r.status))
            }
        }
    }
    pub async fn save(self: &Arc<Self>, identity: [u8; 32], status: &mut PushStatus) -> Result<()> {
        status.updated_at_unix_ms = super::now_ms()?;
        status.writer_fence_held = None;
        status.clock_skewed = false;
        validate_status(status)?;
        let record = Record {
            identity,
            status: status.clone(),
        };
        let digest = *blake3::hash(&serde_json::to_vec(&record)?).as_bytes();
        let bytes = serde_json::to_vec(&Envelope { digest, record })?;
        ensure!(bytes.len() <= CAP, "push checkpoint byte limit exceeded");
        let owner = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(owner.dir.path().join("push.next"))?;
            f.write_all(&bytes)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(
                owner.dir.path().join("push.next"),
                owner.dir.path().join("push.json"),
            )?;
            File::open(owner.dir.path())?.sync_all()?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}
fn decode(dir: &PrivateDir) -> Result<Record> {
    let e: Envelope = serde_json::from_slice(&dir.read("push.json", CAP)?)?;
    ensure!(
        *blake3::hash(&serde_json::to_vec(&e.record)?).as_bytes() == e.digest,
        "push checkpoint checksum differs"
    );
    validate_status(&e.record.status)?;
    Ok(e.record)
}
pub(super) fn read(path: &Path) -> Result<PushStatus> {
    let dir = PrivateDir::open_existing(path)?;
    let mut s = decode(&dir)?.status;
    let lock = dir.open_existing_file("push.lock", 0)?;
    let held = match FileExt::try_lock_shared(&lock) {
        Ok(()) => false,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => true,
        Err(e) => return Err(e.into()),
    };
    s.writer_fence_held = Some(held);
    let now = super::now_ms()?;
    s.clock_skewed =
        s.updated_at_unix_ms > now || s.last_remote_check_unix_ms.is_some_and(|t| t > now);
    if !held
        || s.clock_skewed
        || now.saturating_sub(s.updated_at_unix_ms)
            > s.poll_interval_ms.saturating_add(s.operation_timeout_ms)
    {
        s.lag_at_last_observation = None;
    }
    Ok(s)
}
