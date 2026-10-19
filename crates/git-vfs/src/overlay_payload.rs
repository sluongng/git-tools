//! Private per-inode data files. Namespace paths never become backing paths.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[cfg(test)]
thread_local! { static NEXT_FAILURE: Cell<Option<&'static str>> = const { Cell::new(None) }; }
#[cfg(test)]
pub(super) fn fail_next(point: &'static str) {
    NEXT_FAILURE.with(|next| next.set(Some(point)));
}
pub(super) fn failure(point: &str) -> Result<()> {
    #[cfg(test)]
    if std::env::var("GIT_VFS_TEST_PAUSE_AT").as_deref() == Ok(point) {
        let root = std::env::var_os("GIT_VFS_TEST_CRASH_ROOT").expect("test pause root");
        std::fs::write(Path::new(&root).join("crash-ready"), b"prepared")?;
        loop {
            std::thread::park();
        }
    }
    #[cfg(test)]
    if NEXT_FAILURE.with(|next| {
        if next.get() == Some(point) {
            next.set(None);
            true
        } else {
            false
        }
    }) {
        return Err(std::io::Error::from_raw_os_error(libc::ENOSPC).into());
    }
    let _ = point;
    Ok(())
}

#[derive(Clone, Debug)]
pub enum Payload {
    None,
    Base,
    Inline(Vec<u8>),
    File { size: u64, generation: u64 },
}
impl Payload {
    pub fn size(&self) -> Option<u64> {
        match self {
            Self::Inline(bytes) => Some(bytes.len() as u64),
            Self::File { size, .. } => Some(*size),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Operation {
    Write {
        offset: u64,
        length: u64,
        hash: String,
    },
    Truncate,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub before_generation: u64,
    pub before_size: u64,
    pub before_entry: Vec<u8>,
    pub after_entry: Vec<u8>,
    pub paths: Vec<Vec<u8>>,
    pub operation: Operation,
}
#[derive(Default)]
pub(super) struct Counters {
    pub read: Cell<u64>,
    pub written: Cell<u64>,
    pub copied: Cell<u64>,
    pub hashed: Cell<u64>,
    pub syncs: Cell<u64>,
    pub directory_syncs: Cell<u64>,
    pub recovered: Cell<u64>,
}
pub(super) struct Files {
    directory: PathBuf,
    pub counters: Counters,
}
impl Files {
    pub fn open(overlay: &Path) -> Result<Self> {
        let directory = overlay.join("payloads");
        match std::fs::create_dir(&directory) {
            Ok(()) => std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let meta = std::fs::symlink_metadata(&directory)?;
        ensure!(
            meta.is_dir() && meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o077 == 0,
            "unsafe payload directory"
        );
        File::open(overlay)?.sync_all()?;
        Ok(Self {
            directory,
            counters: Counters::default(),
        })
    }
    fn path(&self, inode: u64) -> PathBuf {
        self.directory.join(format!("{inode:016x}.data"))
    }
    fn open_path(&self, path: &Path, create: bool) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let meta = file.metadata()?;
        ensure!(
            meta.is_file()
                && meta.uid() == unsafe { libc::geteuid() }
                && meta.mode() & 0o077 == 0
                && meta.nlink() == 1,
            "unsafe payload file"
        );
        Ok(file)
    }
    pub fn file(&self, inode: u64, expected_size: u64) -> Result<File> {
        let file = self.open_path(&self.path(inode), false)?;
        ensure!(
            file.metadata()?.len() == expected_size,
            "payload size disagrees with metadata"
        );
        Ok(file)
    }
    pub fn install(&self, inode: u64, bytes: &[u8]) -> Result<()> {
        // Only used before any durable File reference exists (migration/copy-up).
        let mut file = self.open_path(&self.path(inode), true)?;
        file.set_len(0)?;
        file.write_all(bytes)?;
        self.counters
            .copied
            .set(self.counters.copied.get() + bytes.len() as u64);
        self.sync_file(&file)?;
        self.sync_directory()?;
        failure("after_install_sync")
    }
    pub fn prepare_unpublished(
        &self,
        inode: u64,
        old: &[u8],
        size: u64,
        write: Option<(u64, &[u8])>,
    ) -> Result<()> {
        let mut file = self.open_path(&self.path(inode), true)?;
        file.set_len(0)?;
        let copied = old.len().min(usize::try_from(size).unwrap_or(usize::MAX));
        file.write_all(&old[..copied])?;
        self.counters
            .copied
            .set(self.counters.copied.get() + copied as u64);
        failure("after_copyup_old")?;
        if let Some((offset, bytes)) = write {
            file.write_all_at(bytes, offset)?;
            self.counters
                .written
                .set(self.counters.written.get() + bytes.len() as u64);
        }
        file.set_len(size)?;
        failure("after_copyup_data")?;
        self.sync_file(&file)?;
        failure("before_copyup_directory_sync")?;
        self.sync_directory()?;
        failure("after_copyup_sync")
    }
    pub fn sync_directory(&self) -> Result<()> {
        File::open(&self.directory)?.sync_all()?;
        self.counters
            .directory_syncs
            .set(self.counters.directory_syncs.get() + 1);
        Ok(())
    }
    fn sync_file(&self, file: &File) -> Result<()> {
        file.sync_all()?;
        self.counters.syncs.set(self.counters.syncs.get() + 1);
        Ok(())
    }
    pub fn stage(&self, bytes: &[u8]) -> Result<()> {
        failure("before_redo_stage")?;
        let path = self.directory.join("redo.data");
        let mut file = self.open_path(&path, false)?;
        file.set_len(0)?;
        file.write_all(bytes)?;
        self.sync_file(&file)
    }
    pub fn len(&self, inode: u64) -> Result<u64> {
        Ok(self.open_path(&self.path(inode), false)?.metadata()?.len())
    }
    pub fn apply(&self, inode: u64, before_size: u64, size: u64, op: &Operation) -> Result<()> {
        let file = self.open_path(&self.path(inode), false)?;
        let actual = file.metadata()?.len();
        match op {
            Operation::Write { .. } => ensure!(
                actual >= before_size && actual <= size,
                "redo backing length outside prepared write states"
            ),
            Operation::Truncate => ensure!(
                actual == before_size || actual == size,
                "redo backing length outside prepared truncate states"
            ),
        }
        match op {
            Operation::Write {
                offset,
                length,
                hash,
            } => {
                let mut redo = self.open_path(&self.directory.join("redo.data"), false)?;
                ensure!(
                    redo.metadata()?.len() == *length,
                    "redo payload length mismatch"
                );
                let mut hasher = blake3::Hasher::new();
                let mut buffer = [0u8; 64 * 1024];
                loop {
                    let n = redo.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buffer[..n]);
                }
                ensure!(
                    hasher.finalize().to_hex().as_str() == hash,
                    "redo payload hash mismatch"
                );
                use std::io::Seek;
                redo.rewind()?;
                let mut at = *offset;
                loop {
                    let n = redo.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    failure("before_payload_write")?;
                    file.write_all_at(&buffer[..n], at)?;
                    failure("after_first_write")?;
                    at = at
                        .checked_add(n as u64)
                        .context("payload offset overflow")?;
                    self.counters
                        .written
                        .set(self.counters.written.get() + n as u64);
                }
                ensure!(
                    at == offset
                        .checked_add(*length)
                        .context("redo offset overflow")?
                        && at <= size,
                    "redo final length mismatch"
                );
            }
            Operation::Truncate => {}
        }
        failure("before_payload_resize")?;
        file.set_len(size)?;
        failure("before_payload_sync")?;
        self.sync_file(&file)?;
        failure("after_payload_sync")
    }
    pub fn retire_redo(&self) -> Result<()> {
        failure("before_redo_retire")?;
        // No intent means these bytes are unreferenced scratch. Retaining the
        // durable name avoids a create/unlink directory barrier on every write.
        let path = self.directory.join("redo.data");
        match std::fs::symlink_metadata(&path) {
            Ok(_) => {
                // No durable reference remains, so clearing scratch needs no
                // durability barrier. A crash may retain old scratch bytes;
                // the next stage overwrites and syncs them before a new intent.
                self.open_path(&path, false)?.set_len(0)?;
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.open_path(&path, true)?;
                self.sync_directory()
            }
            Err(e) => Err(e.into()),
        }
    }
    pub fn remove(&self, inode: u64) -> Result<()> {
        match std::fs::remove_file(self.path(inode)) {
            Ok(()) => self.sync_directory(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
    pub fn read(&self, inode: u64, size: u64, offset: u64, length: usize) -> Result<Vec<u8>> {
        let length = usize::try_from(size.saturating_sub(offset).min(length as u64))?;
        let mut bytes = vec![0; length];
        self.file(inode, size)?.read_exact_at(&mut bytes, offset)?;
        self.counters
            .read
            .set(self.counters.read.get() + length as u64);
        Ok(bytes)
    }
    pub fn hash(&self, inode: u64, size: u64, mut consume: impl FnMut(&[u8])) -> Result<()> {
        let mut file = self.file(inode, size)?;
        let mut buffer = [0u8; 64 * 1024];
        let mut total = 0u64;
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            consume(&buffer[..n]);
            total += n as u64;
        }
        ensure!(total == size, "payload changed while hashing");
        self.counters.hashed.set(self.counters.hashed.get() + total);
        Ok(())
    }
}
