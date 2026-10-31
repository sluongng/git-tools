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
pub use tinytree_cloud::{
    HistoricalLandingJob, HistoricalLandingPhase, PortableLanding as LandingPending,
};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct State {
    version: u32,
    socket: PathBuf,
    store: String,
    pub observations: Vec<Observation>,
    pub pending: Option<Pending>,
    #[serde(default)]
    pub archived_pushes: Vec<Pending>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub archived_landings: Vec<LandingPending>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub landing: Option<LandingPending>,
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
            archived_landings: vec![],
            landing: None,
        }
    }
    pub fn validate(&self, locator: &RemoteScope) -> io::Result<()> {
        if !matches!(self.version, VERSION | 2 | 3)
            || ((self.landing.is_some() || !self.archived_landings.is_empty())
                && !matches!(self.version, 2 | 3))
            || (self.version == 2
                && (!self.archived_landings.is_empty()
                    || self
                        .landing
                        .as_ref()
                        .is_some_and(|p| p.observed.is_some() || p.restored_lookup_only)))
            || self.socket != locator.socket
            || self.store != locator.store
            || self.observations.len() > MAX_REFERENCES
            || self.archived_pushes.len() > MAX_REFERENCES
        {
            return Err(io::Error::other(
                "native remote state scope/version mismatch",
            ));
        }
        tinytree_cloud::validate_archived_landings(&self.archived_landings)?;
        if let Some(landing) = &self.landing {
            landing.validate()?;
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
    /// Activate the fail-closed version fence before any landing dispatch. Once
    /// upgraded, removing a journal never silently restores legacy mutation.
    /// Call only after a fresh checked abandoned-publication observation.
    /// Preserve all original source/result claims; never evict on overflow.
    pub fn archive_abandoned_landing(&mut self) -> io::Result<()> {
        let landing = self
            .landing
            .as_ref()
            .ok_or_else(|| io::Error::other("no landing to archive"))?;
        tinytree_cloud::validate_archived_landings(std::slice::from_ref(landing))?;
        let mut next = self.clone();
        if let Some(saved) = next
            .archived_landings
            .iter()
            .find(|old| old.job_id == landing.job_id)
        {
            if saved != landing {
                return Err(io::Error::other("archived landing binding differs"));
            }
        } else {
            if next.archived_landings.len() >= 128 {
                return Err(io::Error::other("archived landing retention limit reached"));
            }
            next.archived_landings.push(landing.clone());
        }
        next.landing = None;
        tinytree_cloud::validate_archived_landings(&next.archived_landings)?;
        if toml::to_string(&next).map_err(io::Error::other)?.len() > MAX_BYTES as usize {
            return Err(io::Error::other(
                "archived landing sidecar exceeds byte limit",
            ));
        }
        // Admission and encoding succeeded. No failure path above clears the
        // current intent, even in memory; callers then persist atomically.
        *self = next;
        Ok(())
    }
    pub fn begin_landing(&mut self, intent: LandingPending) -> io::Result<()> {
        intent.validate()?;
        if self.landing.is_some() || self.pending.is_some() {
            return Err(io::Error::other(
                "another native intent requires reconciliation",
            ));
        }
        self.version = 3;
        self.landing = Some(intent);
        Ok(())
    }
    pub fn record_landing_observation(
        &mut self,
        job: &native_broker::landing_protocol::LandingJob,
    ) -> io::Result<()> {
        let pending = self
            .landing
            .as_mut()
            .ok_or_else(|| io::Error::other("landing intent missing"))?;
        pending.observed = Some(historical_landing_job(job)?);
        pending.validate()?;
        self.version = 3;
        Ok(())
    }
    pub fn from_portable(
        scope: &RemoteScope,
        remote: &tinytree_cloud::PortableRemoteState,
    ) -> io::Result<Self> {
        let mut state = Self::new(scope);
        state.observations = remote
            .observations
            .iter()
            .map(|r| Observation {
                reference: r.reference.clone(),
                bookmark: r.bookmark.clone(),
                generation: r.generation,
                head: r.head.clone(),
            })
            .collect();
        state.archived_pushes = remote
            .archived_pushes
            .iter()
            .map(|r| Pending {
                kind: Kind::Push,
                reference: r.reference.clone(),
                bookmark: r.bookmark.clone(),
                generation: r.generation,
                head: r.head.clone(),
                request_id: r.request_id.clone(),
                base_operation: r.base_operation.clone(),
                confirmed_generation: r.confirmed_generation,
            })
            .collect();
        state.archived_landings = remote.archived_landings.clone();
        if !state.archived_landings.is_empty() {
            state.version = 3;
        }
        if let Some(landing) = &remote.landing {
            let mut landing = landing.clone();
            landing.restored_lookup_only = true;
            state.begin_landing(landing)?;
        }
        state.validate(scope)?;
        Ok(state)
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
            .chain(
                self.landing
                    .iter()
                    .chain(&self.archived_landings)
                    .flat_map(|intent| {
                        std::iter::once(intent.source_operation.as_str())
                            .chain(intent.fork_operation.as_deref())
                            .chain(intent.apply_operation.as_deref())
                    }),
            )
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
            .chain(
                self.landing
                    .iter()
                    .chain(&self.archived_landings)
                    .flat_map(|intent| {
                        std::iter::once(intent.source_head.as_str())
                            .chain(std::iter::once(intent.expected_head.as_str()))
                            .chain(
                                (intent.source_base.bytes().any(|b| b != b'0'))
                                    .then_some(intent.source_base.as_str()),
                            )
                            .chain(intent.source_stack.iter().map(String::as_str))
                            .chain(intent.result_roots.iter().map(String::as_str))
                    }),
            )
    }
}

/// Convert checked transport fields to untrusted historical storage. This does
/// not create a checked job or a live retention capability on later decode.
pub fn historical_landing_job(
    job: &native_broker::landing_protocol::LandingJob,
) -> io::Result<HistoricalLandingJob> {
    use native_broker::landing_protocol as l;
    use tinytree_cloud as c;
    let bad = || io::Error::other("invalid historical landing job");
    fn bytes<const N: usize>(v: &[u8]) -> io::Result<[u8; N]> {
        v.try_into()
            .map_err(|_| io::Error::other("landing identity width"))
    }
    let rewrite = |r: &l::Rewrite| -> io::Result<c::HistoricalLandingRewrite> {
        Ok(c::HistoricalLandingRewrite {
            old_commit: bytes(&r.old_commit)?,
            new_commit: bytes(&r.new_commit)?,
            change_id: bytes(&r.change_id)?,
        })
    };
    Ok(HistoricalLandingJob {
        job_id: bytes(&job.id)?,
        phase: match l::Phase::try_from(job.phase).map_err(|_| bad())? {
            l::Phase::Intent => c::HistoricalLandingPhase::Intent,
            l::Phase::PreparedPendingChecks => c::HistoricalLandingPhase::PreparedPendingChecks,
            l::Phase::Checked => c::HistoricalLandingPhase::Checked,
            l::Phase::Committed => c::HistoricalLandingPhase::Committed,
            l::Phase::Conflict => c::HistoricalLandingPhase::Conflict,
            l::Phase::Unsupported => c::HistoricalLandingPhase::Unsupported,
            l::Phase::CheckFailed => c::HistoricalLandingPhase::CheckFailed,
            l::Phase::CheckUnavailable => c::HistoricalLandingPhase::CheckUnavailable,
            l::Phase::StaleTarget => c::HistoricalLandingPhase::StaleTarget,
            l::Phase::Expired => c::HistoricalLandingPhase::Expired,
            l::Phase::PublicationAbandoned => c::HistoricalLandingPhase::PublicationAbandoned,
            _ => return Err(bad()),
        },
        preparation: match l::PreparationAvailability::try_from(job.preparation)
            .map_err(|_| bad())?
        {
            l::PreparationAvailability::ProfileConfigured => {
                c::HistoricalLandingPreparation::ProfileConfigured
            }
            l::PreparationAvailability::ProfileUnavailable => {
                c::HistoricalLandingPreparation::ProfileUnavailable
            }
            l::PreparationAvailability::PreparationDisabled => {
                c::HistoricalLandingPreparation::PreparationDisabled
            }
            _ => return Err(bad()),
        },
        roots_retained: job.roots_retained,
        expires_at_seconds: job.expires_at_seconds,
        profile_name: job.profile_name.clone(),
        profile_version: job.profile_version.clone(),
        candidate: job
            .candidate
            .as_ref()
            .map(|v| -> io::Result<_> {
                Ok(c::HistoricalLandingCandidate {
                    head: bytes(&v.head)?,
                    root_tree: bytes(&v.root_tree)?,
                    rewrites: v.rewrites.iter().map(rewrite).collect::<io::Result<_>>()?,
                })
            })
            .transpose()?,
        receipt: job
            .receipt
            .as_ref()
            .map(|r| -> io::Result<_> {
                let reference = r.reference.as_ref().ok_or_else(bad)?;
                let descriptor = reference.commit.as_ref().ok_or_else(bad)?;
                let key = descriptor.key.as_ref().ok_or_else(bad)?;
                if key.kind != 3 || descriptor.content_blake3 != key.native_id {
                    return Err(bad());
                }
                Ok(c::HistoricalLandingReceipt {
                    reference: reference.name.clone(),
                    generation: reference.generation,
                    commit: bytes(&key.native_id)?,
                    commit_size_bytes: descriptor.size_bytes,
                    object_format_version: descriptor
                        .object_format_version
                        .try_into()
                        .map_err(|_| bad())?,
                    rewrites: r.rewrites.iter().map(rewrite).collect::<io::Result<_>>()?,
                    evidence: bytes(&r.evidence)?,
                })
            })
            .transpose()?,
        conflict_path: job.conflict_path.clone(),
    })
}
