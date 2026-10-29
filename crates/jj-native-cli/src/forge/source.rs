//! Workspace source protection precedes the first upload or external mutation.
use fs2::FileExt as _;
use native_workspace_admin::forge_pending::{self, PendingReview, State};
use private_fs::PrivateDir;
use std::{fs::File, io, path::Path};
pub(super) struct LockedSource {
    directory: PrivateDir,
    _lock: File,
    pub state: State,
}
impl LockedSource {
    pub fn open(path: &Path) -> io::Result<Self> {
        let directory = PrivateDir::open_existing(path)?;
        let lock = directory.open_lock("forge-pending.lock")?;
        lock.try_lock_exclusive().map_err(|_| {
            io::Error::other("another review request owns this workspace source journal")
        })?;
        let state = match directory.read(forge_pending::FILE_NAME, forge_pending::MAX_BYTES) {
            Ok(bytes) => State::decode(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => State::default(),
            Err(e) => return Err(e),
        };
        Ok(Self {
            directory,
            _lock: lock,
            state,
        })
    }
    pub fn retain(&mut self, request: PendingReview) -> io::Result<()> {
        self.state.retain(request)?;
        self.save()
    }
    pub fn save(&self) -> io::Result<()> {
        self.directory
            .replace(forge_pending::FILE_NAME, &self.state.encode()?)
    }
}
#[derive(Eq, PartialEq)]
pub(crate) struct Retention {
    pub state: State,
    bytes: Vec<u8>,
}
impl Retention {
    pub fn encoded_bytes(&self) -> usize {
        self.bytes.len()
    }
}
pub(crate) fn retained_roots(path: &Path) -> io::Result<Option<Retention>> {
    match std::fs::symlink_metadata(path.join(forge_pending::FILE_NAME)) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
        Ok(meta) if !meta.is_file() || meta.len() > forge_pending::MAX_BYTES as u64 => {
            return Err(io::Error::other("invalid forge source retention file"));
        }
        Ok(_) => {}
    }
    let directory = PrivateDir::open_existing(path)?;
    match directory.read(forge_pending::FILE_NAME, forge_pending::MAX_BYTES) {
        Ok(bytes) => Ok(Some(Retention {
            state: State::decode(&bytes)?,
            bytes,
        })),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
