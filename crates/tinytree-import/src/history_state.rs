use super::{HistoryOptions, graph::Manifest};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    sync::Arc,
};
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Phase {
    Preparing,
    Prepared,
    Held,
    Publishing,
    Published,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Journal {
    pub version: u32,
    pub scope: [u8; 32],
    pub job: String,
    pub client: [u8; 16],
    pub phase: Phase,
    pub manifest: Option<Manifest>,
    pub checkpoint: Option<String>,
    pub manifest_id: Option<String>,
    pub hold: Option<String>,
    pub hold_generation: Option<u64>,
    pub publication_generation: Option<u64>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    digest: [u8; 32],
    journal: Journal,
}
pub(super) struct Resources {
    pub state: PrivateDir,
    pub spool: PrivateDir,
    pub lock: File,
    pub options: HistoryOptions,
}
pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub(super) fn valid_hex(s: &str, n: usize) -> bool {
    s.len() == n
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub(super) fn id(s: &str) -> Result<[u8; 32]> {
    ensure!(valid_hex(s, 64), "invalid native identifier");
    let mut a = [0; 32];
    for (i, b) in a.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)?;
    }
    Ok(a)
}
impl Resources {
    pub fn open(mut options: HistoryOptions) -> Result<Arc<Self>> {
        ensure!(
            options.expected_generation < u64::MAX,
            "history generation exhausted"
        );
        ensure!(
            valid_hex(&options.revision, 40),
            "history import requires an exact lowercase Git SHA1 commit"
        );
        ensure!(
            !options.reference.is_empty()
                && options.reference.len() <= 4096
                && !options.reference.starts_with("_tinytree")
                && !options.reference.contains('\0'),
            "invalid public history destination"
        );
        ensure!(
            options.source_git.as_os_str().as_encoded_bytes().len() <= 4096
                && options.git_program.as_os_str().as_encoded_bytes().len() <= 4096,
            "history source/tool path exceeds bound"
        );
        options.source_git = match options.source_git.canonicalize() {
            Ok(path) => path,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && options.source_git.is_absolute() =>
            {
                options.source_git.clone()
            }
            Err(error) => return Err(error).context("cannot resolve immutable Git source"),
        };
        let state = PrivateDir::create(&options.state_dir)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(state.path().join("writer.lock"))?;
        state.open_existing_file("writer.lock", 0)?;
        lock.try_lock_exclusive()
            .map_err(|_| anyhow::anyhow!("history writer is busy"))?;
        let spool = PrivateDir::create(state.path().join("spool"))?;
        match std::fs::symlink_metadata(state.path().join("journal.next")) {
            Ok(_) => {
                use std::os::unix::fs::MetadataExt;
                let candidate = state.open_existing_file(
                    "journal.next",
                    options.limits.max_manifest_bytes as u64 * 2,
                )?;
                ensure!(
                    candidate.metadata()?.nlink() == 1,
                    "interrupted journal staging hardlink preserved"
                );
                // A candidate is never authority before rename+directory-fsync
                // ACK. Even a partial write can be discarded; validate an
                // existing committed journal first, and never adopt the candidate.
                match std::fs::symlink_metadata(state.path().join("journal.json")) {
                    Ok(_) => {
                        let bytes =
                            state.read("journal.json", options.limits.max_manifest_bytes * 2)?;
                        let committed: Envelope = serde_json::from_slice(&bytes)?;
                        ensure!(
                            committed.journal.version == 1
                                && *blake3::hash(&serde_json::to_vec(&committed.journal)?)
                                    .as_bytes()
                                    == committed.digest,
                            "committed journal is invalid; staging preserved"
                        );
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
                drop(candidate);
                std::fs::remove_file(state.path().join("journal.next"))?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        fn inventory(path: &std::path::Path, remaining: &mut usize, depth: usize) -> Result<u64> {
            ensure!(depth > 0, "history temporary inventory depth exceeded");
            let mut bytes = 0u64;
            for entry in std::fs::read_dir(path)? {
                *remaining = remaining
                    .checked_sub(1)
                    .context("history temporary inventory count exceeded")?;
                let entry = entry?;
                let meta = std::fs::symlink_metadata(entry.path())?;
                ensure!(
                    !meta.file_type().is_symlink(),
                    "history temporary symlink preserved"
                );
                let n = if meta.is_dir() {
                    inventory(&entry.path(), remaining, depth - 1)?
                } else {
                    ensure!(meta.is_file(), "history temporary special file preserved");
                    meta.len()
                };
                bytes = bytes
                    .checked_add(n)
                    .context("history temporary byte overflow")?;
            }
            Ok(bytes)
        }
        let mut entries = 128;
        let bytes = inventory(spool.path(), &mut entries, 6)?;
        ensure!(
            bytes
                <= options.limits.max_file_bytes as u64 * 4
                    + options.limits.max_manifest_bytes as u64 * 2,
            "history interrupted spool requires bounded reconciliation"
        );
        File::open(state.path())?.sync_all()?;
        Ok(Arc::new(Self {
            state,
            spool,
            lock,
            options,
        }))
    }
    pub fn load(&self, scope: [u8; 32]) -> Result<Journal> {
        match std::fs::symlink_metadata(self.state.path().join("journal.json")) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Journal {
                version: 1,
                scope,
                job: uuid::Uuid::new_v4().simple().to_string(),
                client: *uuid::Uuid::new_v4().as_bytes(),
                phase: Phase::Preparing,
                manifest: None,
                checkpoint: None,
                manifest_id: None,
                hold: None,
                hold_generation: None,
                publication_generation: None,
            }),
            Err(e) => Err(e.into()),
            Ok(_) => {
                let e: Envelope = serde_json::from_slice(
                    &self
                        .state
                        .read("journal.json", self.options.limits.max_manifest_bytes * 2)?,
                )?;
                ensure!(
                    e.journal.version == 1
                        && *blake3::hash(&serde_json::to_vec(&e.journal)?).as_bytes() == e.digest,
                    "history journal format/checksum differs"
                );
                ensure!(
                    e.journal.scope == scope,
                    "history source/target/credential identity changed; explicit recovery required"
                );
                ensure!(
                    valid_hex(&e.journal.job, 32),
                    "invalid history job identity"
                );
                Ok(e.journal)
            }
        }
    }
    pub async fn save(self: &Arc<Self>, j: &Journal) -> Result<()> {
        let payload = serde_json::to_vec(j)?;
        ensure!(
            payload.len() <= self.options.limits.max_manifest_bytes * 2,
            "history journal exceeds bound"
        );
        let bytes = serde_json::to_vec(&Envelope {
            digest: *blake3::hash(&payload).as_bytes(),
            journal: j.clone(),
        })?;
        ensure!(
            bytes.len() <= self.options.limits.max_manifest_bytes * 2,
            "history journal exceeds bound"
        );
        let owner = self.clone();
        tokio::task::spawn_blocking(move || {
            let p = owner.state.path().join("journal.next");
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&p)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(p, owner.state.path().join("journal.json"))?;
            File::open(owner.state.path())?.sync_all()?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}
