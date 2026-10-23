//! Workspace-local publication state. This is deliberately outside jj's undoable
//! operation graph: an undo cannot restore an old upstream CAS generation.
use crate::broker::Locator;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

const VERSION: u32 = 1;
const MAX_BYTES: u64 = 1024 * 1024;
const MAX_REFERENCES: usize = 128;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Observation {
    pub reference: String,
    pub bookmark: String,
    pub generation: u64,
    pub head: Option<String>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Fetch,
    Push,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Pending {
    pub kind: Kind,
    pub reference: String,
    pub bookmark: String,
    pub generation: u64,
    pub head: Option<String>,
    pub request_id: String,
    pub base_operation: String,
    pub confirmed_generation: Option<u64>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    version: u32,
    socket: PathBuf,
    store: String,
    pub observations: Vec<Observation>,
    pub pending: Option<Pending>,
    #[serde(default)]
    pub archived_pushes: Vec<Pending>,
}
impl State {
    fn new(locator: &Locator) -> Self {
        Self {
            version: VERSION,
            socket: locator.socket.clone(),
            store: locator.store.clone(),
            observations: vec![],
            pending: None,
            archived_pushes: vec![],
        }
    }
    fn validate(&self, locator: &Locator) -> io::Result<()> {
        if self.version != VERSION
            || self.socket != locator.socket
            || self.store != locator.store
            || self.observations.len() > MAX_REFERENCES
            || self.archived_pushes.len() > MAX_REFERENCES
        {
            return Err(io::Error::other(
                "native remote state scope/version mismatch",
            ));
        }
        let mut names = std::collections::HashSet::new();
        let mut bookmarks = std::collections::HashSet::new();
        for observation in &self.observations {
            validate_names(&observation.reference, &observation.bookmark)?;
            if !names.insert(&observation.reference)
                || !bookmarks.insert(&observation.bookmark)
                || (observation.head.is_some() && observation.generation == 0)
                || observation
                    .head
                    .as_ref()
                    .is_some_and(|id| !valid_hex(id, 64))
            {
                return Err(io::Error::other("invalid native remote observation"));
            }
        }
        let mut intents = std::collections::HashSet::new();
        for pending in self.pending.iter().chain(self.archived_pushes.iter()) {
            validate_names(&pending.reference, &pending.bookmark)?;
            if pending.head.as_ref().is_some_and(|id| !valid_hex(id, 64))
                || !valid_hex(&pending.request_id, 32)
                || pending.base_operation.is_empty()
                || pending.base_operation.len() > 256
                || pending
                    .base_operation
                    .bytes()
                    .any(|b| !b.is_ascii_hexdigit())
                || (pending.kind == Kind::Push && pending.head.is_none())
                || !intents.insert(&pending.request_id)
                || pending
                    .confirmed_generation
                    .is_some_and(|generation| pending.generation.checked_add(1) != Some(generation))
            {
                return Err(io::Error::other("invalid pending native operation"));
            }
        }
        if let Some(pending) = &self.pending {
            if pending.kind == Kind::Push
                && self
                    .observation(&pending.reference, &pending.bookmark)?
                    .is_none_or(|observation| observation.generation != pending.generation)
            {
                return Err(io::Error::other(
                    "pending native publication has a different observation",
                ));
            }
        }
        if self
            .archived_pushes
            .iter()
            .any(|intent| intent.kind != Kind::Push)
        {
            return Err(io::Error::other("invalid archived native replay intent"));
        }
        Ok(())
    }
    pub fn prepare(&self, reference: &str, bookmark: &str) -> io::Result<()> {
        if self.observation(reference, bookmark)?.is_none()
            && self.observations.len() >= MAX_REFERENCES
        {
            return Err(io::Error::other(
                "native reference observation limit exceeded",
            ));
        }
        Ok(())
    }
    pub fn archive_pending_push(&mut self) -> io::Result<()> {
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.kind == Kind::Push)
        {
            if self.archived_pushes.len() >= MAX_REFERENCES {
                return Err(io::Error::other(
                    "native replay archive limit exceeded; original intent retained",
                ));
            }
            self.archived_pushes.push(self.pending.take().unwrap());
        }
        Ok(())
    }
    pub fn observation(&self, reference: &str, bookmark: &str) -> io::Result<Option<&Observation>> {
        let found = self
            .observations
            .iter()
            .find(|entry| entry.reference == reference);
        if (found.is_none()
            && self
                .observations
                .iter()
                .any(|entry| entry.bookmark == bookmark))
            || found.is_some_and(|entry| entry.bookmark != bookmark)
        {
            return Err(io::Error::other(
                "native reference is associated with another local bookmark",
            ));
        }
        Ok(found)
    }
    pub fn record(&mut self, observation: Observation) -> io::Result<()> {
        if let Some(current) = self
            .observations
            .iter_mut()
            .find(|entry| entry.reference == observation.reference)
        {
            if current.bookmark != observation.bookmark {
                return Err(io::Error::other("native reference bookmark changed"));
            }
            *current = observation;
        } else {
            if self.observations.len() >= MAX_REFERENCES {
                return Err(io::Error::other(
                    "native reference observation limit exceeded",
                ));
            }
            self.observations.push(observation);
        }
        Ok(())
    }
}
pub(crate) fn valid_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub(crate) fn validate_names(reference: &str, bookmark: &str) -> io::Result<()> {
    if reference.is_empty()
        || reference.len() > 512
        || reference.contains('\0')
        || reference == "_tinytree"
        || reference.starts_with("_tinytree/")
        || bookmark.is_empty()
        || bookmark.len() > 512
        || bookmark.contains('\0')
    {
        return Err(io::Error::other("invalid native reference or bookmark"));
    }
    Ok(())
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
            Err(error) if error.kind() == io::ErrorKind::NotFound => State::new(locator),
            Err(error) => return Err(error),
        };
        state.validate(locator)?;
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
    state.validate(&locator)?;
    let mut heads = state
        .observations
        .iter()
        .filter_map(|observation| observation.head.clone())
        .collect::<Vec<_>>();
    heads.extend(
        state
            .pending
            .iter()
            .chain(&state.archived_pushes)
            .filter_map(|intent| intent.head.clone()),
    );
    if heads
        .iter()
        .any(|head| head.bytes().all(|byte| byte == b'0'))
    {
        return Err(io::Error::other(
            "native sidecar cannot retain an intrinsic commit as a publication target",
        ));
    }
    let active_operations = state
        .pending
        .iter()
        .map(|intent| intent.base_operation.clone())
        .collect();
    Ok(Some(RetentionInput {
        commit_heads: heads,
        active_operations,
        source: bytes,
    }))
}
