//! Workspace-local I/O adapter over the shared validated remote-state codec.
use crate::broker::Locator;
use fs2::FileExt;
use native_workspace_admin::remote_state::RemoteScope;
pub(crate) use native_workspace_admin::remote_state::{
    Kind, Observation, Pending, State, valid_hex, validate_names,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
const MAX_BYTES: u64 = 1024 * 1024;
fn scope(locator: &Locator) -> io::Result<RemoteScope> {
    RemoteScope::new(locator.socket.clone(), locator.store.clone())
}

pub(crate) struct LockedState {
    root: PathBuf,
    _lock: File,
    pub state: State,
}
impl LockedState {
    /// Lock before loading/snapshotting jj state. Remote commands fail rather
    /// than waiting indefinitely for another native command in this workspace.
    pub fn open(root: &Path, locator: &Locator) -> io::Result<Self> {
        let metadata = fs::symlink_metadata(root)?;
        if !metadata.is_dir() {
            return Err(io::Error::other("invalid working-copy state directory"));
        }
        let lock_path = root.join("native-remote.lock");
        if fs::symlink_metadata(&lock_path).is_ok_and(|metadata| !metadata.is_file()) {
            return Err(io::Error::other("invalid native remote lock"));
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options.open(&lock_path)?;
        lock.try_lock_exclusive()
            .map_err(|_| io::Error::other("another native operation owns this working copy"))?;
        let path = root.join("native-remote.toml");
        let state = match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if !metadata.is_file() || metadata.len() > MAX_BYTES {
                    return Err(io::Error::other("invalid native remote state file"));
                }
                let mut bytes = Vec::new();
                File::open(&path)?
                    .take(MAX_BYTES + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() as u64 > MAX_BYTES {
                    return Err(io::Error::other("native remote state exceeds byte limit"));
                }
                toml::from_str(
                    std::str::from_utf8(&bytes)
                        .map_err(|_| io::Error::other("invalid native remote state encoding"))?,
                )
                .map_err(|_| io::Error::other("invalid native remote state schema"))?
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => State::new(&scope(locator)?),
            Err(error) => return Err(error),
        };
        state.validate(&scope(locator)?)?;
        Ok(Self {
            root: root.to_owned(),
            _lock: lock,
            state,
        })
    }
    pub fn save(&self) -> io::Result<()> {
        let bytes = toml::to_string(&self.state)
            .map_err(|_| io::Error::other("cannot encode native remote state"))?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(io::Error::other("native remote state exceeds byte limit"));
        }
        let mut temporary = tempfile::NamedTempFile::new_in(&self.root)?;
        temporary.write_all(bytes.as_bytes())?;
        temporary.as_file().sync_all()?;
        temporary
            .persist(self.root.join("native-remote.toml"))
            .map_err(|error| error.error)?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

/// Read-only retention input. Keeping the bounded original bytes permits the
/// collector to detect a sidecar changing during an inventory without creating
/// a lock, an observation or a replay intent.
#[derive(PartialEq, Eq)]
pub(crate) struct RetentionInput {
    pub commit_heads: Vec<String>,
    pub active_operations: Vec<String>,
    source: Vec<u8>,
}
impl RetentionInput {
    pub fn encoded_bytes(&self) -> usize {
        self.source.len()
    }
}
pub(crate) fn retained_roots(
    working_copy_state: &Path,
    repo_path: &Path,
) -> io::Result<Option<RetentionInput>> {
    let path = working_copy_state.join("native-remote.toml");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() || metadata.len() > MAX_BYTES {
        return Err(io::Error::other("invalid native remote retention state"));
    }
    let mut bytes = Vec::new();
    File::open(&path)?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(io::Error::other(
            "native remote retention state exceeds byte limit",
        ));
    }
    let state: State = toml::from_str(
        std::str::from_utf8(&bytes)
            .map_err(|_| io::Error::other("invalid native remote retention encoding"))?,
    )
    .map_err(|_| io::Error::other("invalid native remote retention schema"))?;
    let locator = crate::broker::locator(&repo_path.join("store"))
        .map_err(|_| io::Error::other("native remote retention locator is unavailable"))?;
    state.validate(&scope(&locator)?)?;
    if state
        .landing
        .as_ref()
        .is_some_and(|p| p.operation_writing.is_some())
    {
        return Err(io::Error::other(
            "landing operation receipt requires reconciliation before collection",
        ));
    }
    let heads = state
        .retained_commit_heads()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if heads
        .iter()
        .any(|head| head.bytes().all(|byte| byte == b'0'))
    {
        return Err(io::Error::other(
            "native sidecar cannot retain an intrinsic commit as a publication target",
        ));
    }
    // Archived publication replay heads remain protected, but their abandoned
    // operation files are not active authority after local replay abandonment.
    // A landing is different: exact source/fork/apply operations are required
    // until reconciliation, and its saved journal remains a retention input.
    let active_operations = state
        .pending
        .iter()
        .map(|p| p.base_operation.as_str())
        .chain(state.landing.iter().flat_map(|p| {
            std::iter::once(p.source_operation.as_str())
                .chain(p.fork_operation.as_deref())
                .chain(p.apply_operation.as_deref())
        }))
        .map(str::to_owned)
        .collect();
    Ok(Some(RetentionInput {
        commit_heads: heads,
        active_operations,
        source: bytes,
    }))
}
