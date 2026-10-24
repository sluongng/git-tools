//! Shared bounded native remote-state schema. No lock, save, locator lookup or network.
use serde::{Deserialize, Serialize};
use std::{io, path::PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteScope {
    pub socket: PathBuf,
    pub store: String,
}
impl RemoteScope {
    pub fn new(socket: PathBuf, store: String) -> io::Result<Self> {
        if !socket.is_absolute()
            || socket.as_os_str().len() > 4096
            || socket.components().any(|part| {
                !matches!(
                    part,
                    std::path::Component::RootDir | std::path::Component::Normal(_)
                )
            })
            || store.is_empty()
            || store.len() > 128
            || !store
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        {
            return Err(io::Error::other("invalid native remote scope"));
        }
        Ok(Self { socket, store })
    }
}
const VERSION: u32 = 1;
const MAX_BYTES: u64 = 1024 * 1024;
const MAX_REFERENCES: usize = 128;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub reference: String,
    pub bookmark: String,
    pub generation: u64,
    pub head: Option<String>,
}
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Fetch,
    Push,
}
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Pending {
    pub kind: Kind,
    pub reference: String,
    pub bookmark: String,
    pub generation: u64,
    pub head: Option<String>,
    pub request_id: String,
    pub base_operation: String,
    pub confirmed_generation: Option<u64>,
}
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct State {
    version: u32,
    socket: PathBuf,
    store: String,
    pub observations: Vec<Observation>,
    pub pending: Option<Pending>,
    #[serde(default)]
    pub archived_pushes: Vec<Pending>,
}
impl State {
    pub fn new(locator: &RemoteScope) -> Self {
        Self {
            version: VERSION,
            socket: locator.socket.clone(),
            store: locator.store.clone(),
            observations: vec![],
            pending: None,
            archived_pushes: vec![],
        }
    }
    pub fn validate(&self, locator: &RemoteScope) -> io::Result<()> {
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
pub fn valid_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn validate_names(reference: &str, bookmark: &str) -> io::Result<()> {
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

pub fn decode(bytes: &[u8], scope: &RemoteScope) -> io::Result<State> {
    if bytes.len() > MAX_BYTES as usize {
        return Err(io::Error::other("native remote state exceeds byte limit"));
    }
    let state: State = toml::from_str(
        std::str::from_utf8(bytes)
            .map_err(|_| io::Error::other("invalid native remote encoding"))?,
    )
    .map_err(|_| io::Error::other("invalid native remote schema"))?;
    state.validate(scope)?;
    Ok(state)
}
impl State {
    pub fn retained_operations(&self) -> impl Iterator<Item = &str> {
        self.pending
            .iter()
            .chain(&self.archived_pushes)
            .map(|intent| intent.base_operation.as_str())
    }
    pub fn retained_commit_heads(&self) -> impl Iterator<Item = &str> {
        self.observations
            .iter()
            .filter_map(|entry| entry.head.as_deref())
            .chain(
                self.pending
                    .iter()
                    .chain(&self.archived_pushes)
                    .filter_map(|entry| entry.head.as_deref()),
            )
    }
}
