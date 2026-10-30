//! Trusted execution control/data contracts. Parsing and hashing grant no access;
//! the host authenticates executors and owns leases, retention and publication.
use crate::{
    accounts::{AccountId, validate_path},
    check,
    review::LandingCheckProfile,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use tinytree_model as model;

pub const MAX_FRAME_BYTES: usize = 64 * 1024;
pub const MAX_REPLY_BYTES: usize = 1024 * 1024;
pub const MAX_SNAPSHOT_MANIFEST_BYTES: usize = 32 * 1024 * 1024;
pub const PAGE_ENTRIES: usize = 128;
pub const CHUNK_BYTES: usize = 64 * 1024;
const MAX_SAFE: u64 = 9_007_199_254_740_991;

fn digest(domain: &[u8], value: &impl Serialize) -> Result<String> {
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(&serde_json::to_vec(value)?);
    Ok(hash.finalize().to_hex().to_string())
}
pub fn request_id(value: &str) -> Result<()> {
    crate::upload::validate_request_id(value).map_err(Into::into)
}
pub fn parse_id(value: &str) -> Result<[u8; 32]> {
    ensure!(check::hex_id(value), "invalid checker object identity");
    let mut id = [0; 32];
    for (i, b) in id.iter_mut().enumerate() {
        *b = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)?;
    }
    Ok(id)
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Key {
    pub kind: u8,
    pub id: String,
}
impl Key {
    pub fn native(&self) -> Result<model::ObjectKey> {
        Ok(model::ObjectKey {
            kind: model::Kind::try_from(self.kind)?,
            id: model::ObjectId(parse_id(&self.id)?),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    pub kind: u8,
    pub id: String,
    pub size_bytes: u64,
    pub format_version: u16,
}
impl Descriptor {
    pub fn key(&self) -> Key {
        Key {
            kind: self.kind,
            id: self.id.clone(),
        }
    }
    pub fn native(&self) -> Result<model::Descriptor> {
        ensure!(
            self.format_version == model::FORMAT_VERSION && self.size_bytes <= MAX_SAFE,
            "invalid checker descriptor"
        );
        Ok(model::Descriptor {
            key: self.key().native()?,
            size_bytes: self.size_bytes,
            format_version: self.format_version,
        })
    }
    pub fn from_native(value: &model::Descriptor) -> Self {
        Self {
            kind: value.key.kind as u8,
            id: value.key.id.0.iter().map(|b| format!("{b:02x}")).collect(),
            size_bytes: value.size_bytes,
            format_version: value.format_version,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotLimits {
    pub max_entries: u64,
    pub max_files: u64,
    pub max_file_bytes: u64,
    pub max_total_bytes: u64,
    pub max_metadata_bytes: u64,
    pub max_metadata_object_bytes: u64,
    pub max_path_bytes: u64,
    pub max_depth: u32,
}
impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            max_entries: 32768,
            max_files: 32768,
            max_file_bytes: 1024 * 1024,
            max_total_bytes: 512 * 1024 * 1024,
            max_metadata_bytes: 32 * 1024 * 1024,
            max_metadata_object_bytes: 256 * 1024,
            max_path_bytes: 4 * 1024 * 1024,
            max_depth: 128,
        }
    }
}
impl SnapshotLimits {
    pub fn validate(&self) -> Result<()> {
        let cap = Self::default();
        for (actual, maximum) in [
            (self.max_entries, cap.max_entries),
            (self.max_files, cap.max_files),
            (self.max_file_bytes, cap.max_file_bytes),
            (self.max_total_bytes, cap.max_total_bytes),
            (self.max_metadata_bytes, cap.max_metadata_bytes),
            (
                self.max_metadata_object_bytes,
                cap.max_metadata_object_bytes,
            ),
            (self.max_path_bytes, cap.max_path_bytes),
            (u64::from(self.max_depth), u64::from(cap.max_depth)),
        ] {
            ensure!(
                actual > 0 && actual <= maximum,
                "checker source limit exceeds ceiling"
            );
        }
        Ok(())
    }
    pub fn intersect(&self, other: &Self) -> Result<Self> {
        self.validate()?;
        other.validate()?;
        Ok(Self {
            max_entries: self.max_entries.min(other.max_entries),
            max_files: self.max_files.min(other.max_files),
            max_file_bytes: self.max_file_bytes.min(other.max_file_bytes),
            max_total_bytes: self.max_total_bytes.min(other.max_total_bytes),
            max_metadata_bytes: self.max_metadata_bytes.min(other.max_metadata_bytes),
            max_metadata_object_bytes: self
                .max_metadata_object_bytes
                .min(other.max_metadata_object_bytes),
            max_path_bytes: self.max_path_bytes.min(other.max_path_bytes),
            max_depth: self.max_depth.min(other.max_depth),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateScope {
    pub account_id: String,
    pub view_version: String,
    pub policy_epoch: u64,
    pub change_id: String,
    pub patchset_id: String,
    pub candidate_id: String,
    pub target_head: String,
    pub target_generation: u64,
    pub publication_generation: u64,
}
impl CandidateScope {
    pub fn validate(&self) -> Result<()> {
        AccountId(self.account_id.clone()).validate()?;
        for id in [
            &self.view_version,
            &self.patchset_id,
            &self.candidate_id,
            &self.target_head,
        ] {
            parse_id(id)?;
        }
        ensure!(
            self.change_id.len() == 32
                && self
                    .change_id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid checker change"
        );
        for n in [
            self.policy_epoch,
            self.target_generation,
            self.publication_generation,
        ] {
            ensure!(n > 0 && n <= MAX_SAFE, "invalid checker generation");
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestBinding {
    pub id: String,
    pub digest: String,
    pub entries: u64,
    pub file_bytes: u64,
    pub path_bytes: u64,
    pub metadata_bytes: u64,
}
impl ManifestBinding {
    pub fn validate(&self, limits: &SnapshotLimits) -> Result<()> {
        parse_id(&self.id)?;
        parse_id(&self.digest)?;
        ensure!(
            self.entries > 0
                && self.entries <= limits.max_entries
                && self.file_bytes <= limits.max_total_bytes
                && self.path_bytes <= limits.max_path_bytes
                && self.metadata_bytes <= limits.max_metadata_bytes,
            "invalid checker manifest totals"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckerLease {
    pub schema: u32,
    pub lease_id: String,
    pub generation: u64,
    pub request: check::Request,
    pub scope: CandidateScope,
    pub manifest: ManifestBinding,
    pub expires_at: u64,
    pub limits: SnapshotLimits,
}
impl CheckerLease {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == 1
                && self.generation <= MAX_SAFE
                && self.expires_at > 0
                && self.expires_at <= MAX_SAFE,
            "invalid checker lease"
        );
        parse_id(&self.lease_id)?;
        self.request.validate()?;
        self.scope.validate()?;
        self.limits.validate()?;
        self.manifest.validate(&self.limits)?;
        ensure!(
            self.request.job_id == self.scope.candidate_id
                && self.request.repository_id == format!("forge-{}", self.scope.account_id),
            "checker lease scope differs"
        );
        Ok(())
    }
    pub fn reference(&self) -> LeaseRef {
        LeaseRef {
            lease_id: self.lease_id.clone(),
            request: self.request.clone(),
        }
    }
    pub fn immutable_binding(&self) -> Result<String> {
        self.validate()?;
        digest(
            b"tinytree-checker-lease-v1\0",
            &(
                self.schema,
                &self.lease_id,
                &self.request,
                &self.scope,
                &self.manifest,
                &self.limits,
            ),
        )
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseRef {
    pub lease_id: String,
    pub request: check::Request,
}
impl LeaseRef {
    pub fn validate(&self) -> Result<()> {
        parse_id(&self.lease_id)?;
        self.request.validate()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EntryValue {
    Directory {},
    File {
        executable: bool,
        copy_id: Option<String>,
    },
    Symlink {
        target_bytes: u64,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestEntry {
    pub path: String,
    pub descriptor: Descriptor,
    pub value: EntryValue,
}
impl ManifestEntry {
    pub fn validate(&self, limits: &SnapshotLimits) -> Result<()> {
        self.descriptor.native()?;
        validate_path(&self.path, true)?;
        ensure!(
            self.path.len() <= 4096
                && (self.path.is_empty()
                    || self.path.split('/').count() <= limits.max_depth as usize),
            "checker manifest path limit"
        );
        match &self.value {
            EntryValue::Directory {} => ensure!(
                self.descriptor.kind == 2
                    && self.descriptor.size_bytes <= limits.max_metadata_object_bytes,
                "checker directory descriptor"
            ),
            EntryValue::File { copy_id, .. } => {
                ensure!(
                    !self.path.is_empty()
                        && self.descriptor.kind == 1
                        && self.descriptor.size_bytes <= limits.max_file_bytes,
                    "checker file descriptor"
                );
                if let Some(id) = copy_id {
                    parse_id(id)?;
                }
            }
            EntryValue::Symlink { target_bytes } => ensure!(
                !self.path.is_empty()
                    && self.descriptor.kind == 4
                    && self.descriptor.size_bytes <= limits.max_metadata_object_bytes
                    && *target_bytes <= 4096,
                "checker symlink descriptor"
            ),
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestTotals {
    pub digest: String,
    pub entries: u64,
    pub file_bytes: u64,
    pub path_bytes: u64,
    pub metadata_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestHashState {
    pub digest: String,
    pub next_ordinal: u64,
    pub last_path: Option<String>,
    pub files: u64,
    pub file_bytes: u64,
    pub path_bytes: u64,
    pub metadata_bytes: u64,
}
pub struct ManifestHasher {
    state: ManifestHashState,
}
impl ManifestHasher {
    pub fn new(candidate_commit: &str, root_tree: &str) -> Result<Self> {
        parse_id(candidate_commit)?;
        parse_id(root_tree)?;
        Ok(Self {
            state: ManifestHashState {
                digest: digest(
                    b"tinytree-check-manifest-v1\0",
                    &(candidate_commit, root_tree),
                )?,
                next_ordinal: 0,
                last_path: None,
                files: 0,
                file_bytes: 0,
                path_bytes: 0,
                metadata_bytes: 0,
            },
        })
    }
    pub fn from_state(state: ManifestHashState) -> Result<Self> {
        parse_id(&state.digest)?;
        ensure!(
            state.next_ordinal <= 32768 && state.files <= state.next_ordinal,
            "invalid manifest hash state"
        );
        ensure!(
            (state.next_ordinal == 0) == state.last_path.is_none(),
            "invalid manifest hash cursor"
        );
        if let Some(path) = &state.last_path {
            validate_path(path, true)?;
            ensure!(path.len() <= 4096, "manifest cursor path limit");
        }
        Ok(Self { state })
    }
    pub fn state(&self) -> ManifestHashState {
        self.state.clone()
    }
    pub fn push(&mut self, entry: &ManifestEntry) -> Result<()> {
        entry.validate(&SnapshotLimits::default())?;
        if self.state.next_ordinal == 0 {
            ensure!(
                entry.path.is_empty() && matches!(entry.value, EntryValue::Directory {}),
                "manifest root directory missing"
            );
        } else {
            ensure!(
                self.state
                    .last_path
                    .as_ref()
                    .is_some_and(|p| p.as_str() < entry.path.as_str()),
                "manifest paths not strictly ordered"
            );
        }
        ensure!(self.state.next_ordinal < 32768, "manifest entry limit");
        let state = &mut self.state;
        state.digest = digest(
            b"tinytree-check-manifest-entry-v1\0",
            &(&state.digest, state.next_ordinal, entry),
        )?;
        state.next_ordinal += 1;
        state.last_path = Some(entry.path.clone());
        state.path_bytes = state
            .path_bytes
            .checked_add(entry.path.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("manifest path overflow"))?;
        if matches!(entry.value, EntryValue::File { .. }) {
            state.files += 1;
            state.file_bytes = state
                .file_bytes
                .checked_add(entry.descriptor.size_bytes)
                .ok_or_else(|| anyhow::anyhow!("manifest bytes overflow"))?;
        } else {
            state.metadata_bytes = state
                .metadata_bytes
                .checked_add(entry.descriptor.size_bytes)
                .ok_or_else(|| anyhow::anyhow!("manifest metadata overflow"))?;
        }
        self.check_limits(&SnapshotLimits::default())?;
        Ok(())
    }
    fn check_limits(&self, limits: &SnapshotLimits) -> Result<()> {
        limits.validate()?;
        let s = &self.state;
        ensure!(
            s.next_ordinal <= limits.max_entries
                && s.files <= limits.max_files
                && s.file_bytes <= limits.max_total_bytes
                && s.path_bytes <= limits.max_path_bytes
                && s.metadata_bytes <= limits.max_metadata_bytes,
            "manifest aggregate limits"
        );
        Ok(())
    }
    pub fn finish(&self, limits: &SnapshotLimits) -> Result<ManifestTotals> {
        self.check_limits(limits)?;
        let s = &self.state;
        ensure!(s.next_ordinal > 0, "empty manifest");
        Ok(ManifestTotals {
            digest: digest(
                b"tinytree-check-manifest-finish-v1\0",
                &(
                    &s.digest,
                    s.next_ordinal,
                    s.file_bytes,
                    s.path_bytes,
                    s.metadata_bytes,
                ),
            )?,
            entries: s.next_ordinal,
            file_bytes: s.file_bytes,
            path_bytes: s.path_bytes,
            metadata_bytes: s.metadata_bytes,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotManifest {
    pub schema: u32,
    pub lease: CheckerLease,
    pub entries: Vec<ManifestEntry>,
}
impl SnapshotManifest {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.schema == 1, "invalid snapshot manifest schema");
        self.lease.validate()?;
        ensure!(
            self.entries.len() as u64 == self.lease.manifest.entries,
            "snapshot manifest count differs"
        );
        let mut hash = ManifestHasher::new(
            &self.lease.request.candidate_commit,
            &self.lease.request.root_tree,
        )?;
        for entry in &self.entries {
            entry.validate(&self.lease.limits)?;
            hash.push(entry)?;
        }
        let totals = hash.finish(&self.lease.limits)?;
        let expected = &self.lease.manifest;
        ensure!(
            totals.digest == expected.digest
                && totals.file_bytes == expected.file_bytes
                && totals.path_bytes == expected.path_bytes
                && totals.metadata_bytes == expected.metadata_bytes,
            "snapshot manifest binding differs"
        );
        ensure!(
            self.entries
                .first()
                .is_some_and(|e| e.descriptor.id == self.lease.request.root_tree),
            "snapshot root differs"
        );
        // A digest alone cannot establish complete native tree coverage. Rebuild
        // every directory from its immediate typed children, retaining opaque
        // copy IDs but without granting access to their history objects.
        let entries: std::collections::BTreeMap<_, _> =
            self.entries.iter().map(|e| (e.path.as_str(), e)).collect();
        let mut children: std::collections::BTreeMap<&str, Vec<&ManifestEntry>> =
            std::collections::BTreeMap::new();
        for entry in self.entries.iter().skip(1) {
            let parent = entry.path.rsplit_once('/').map_or("", |(parent, _)| parent);
            ensure!(
                entries
                    .get(parent)
                    .is_some_and(|e| matches!(e.value, EntryValue::Directory {})),
                "snapshot parent directory missing"
            );
            children.entry(parent).or_default().push(entry);
        }
        for entry in &self.entries {
            if !matches!(entry.value, EntryValue::Directory {}) {
                continue;
            }
            let mut tree_entries = Vec::new();
            for child in children.get(entry.path.as_str()).into_iter().flatten() {
                let id = parse_id(&child.descriptor.id)?;
                let value = match &child.value {
                    EntryValue::Directory {} => model::TreeValue::Tree(model::TreeId(id)),
                    EntryValue::File {
                        executable,
                        copy_id,
                    } => model::TreeValue::File {
                        id: model::FileId(id),
                        executable: *executable,
                        size_bytes: child.descriptor.size_bytes,
                        copy_id: copy_id
                            .as_ref()
                            .map(|id| parse_id(id).map(model::CopyId))
                            .transpose()?,
                    },
                    EntryValue::Symlink { target_bytes } => model::TreeValue::Symlink {
                        id: model::SymlinkId(id),
                        target_bytes: *target_bytes,
                    },
                };
                tree_entries.push(model::TreeEntry {
                    name: child.path.rsplit('/').next().unwrap().into(),
                    value,
                });
            }
            let actual = model::encode(
                &model::Object::Tree(model::Tree {
                    entries: tree_entries,
                }),
                metadata_limits(),
            )?;
            ensure!(
                actual.descriptor == entry.descriptor.native()?,
                "snapshot native tree coverage differs"
            );
        }
        Ok(())
    }
    pub fn immutable_binding(&self) -> Result<String> {
        self.validate()?;
        self.lease.immutable_binding()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestPage {
    pub lease: LeaseRef,
    pub manifest_id: String,
    pub digest: String,
    pub start: u64,
    pub entries: Vec<ManifestEntry>,
    pub next: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectChunk {
    pub lease: LeaseRef,
    pub descriptor: Descriptor,
    pub offset: u64,
    pub data: Vec<u8>,
    pub eof: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub lease: LeaseRef,
    pub result_request_id: String,
    pub receipt_digest: String,
    pub outcome: check::Outcome,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Claim {
        profile_name: String,
        profile_version: String,
    },
    GetClaim {
        claim_id: String,
    },
    Renew {
        lease: LeaseRef,
        expected_generation: u64,
    },
    Manifest {
        lease: LeaseRef,
        cursor: u64,
        limit: u32,
    },
    Object {
        lease: LeaseRef,
        key: Key,
        offset: u64,
        max_bytes: u32,
    },
    Complete {
        lease: LeaseRef,
        receipt: check::Receipt,
    },
    GetResult {
        lease: LeaseRef,
        result_request_id: String,
    },
    Release {
        lease: LeaseRef,
        receipt: check::Receipt,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Call {
    pub schema: u32,
    pub account_id: String,
    pub request_id: String,
    pub command: Command,
}
impl Call {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() <= MAX_FRAME_BYTES, "checker call limit");
        let call: Self = serde_json::from_slice(bytes)?;
        call.validate()?;
        Ok(call)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(self.schema == 1, "invalid checker schema");
        AccountId(self.account_id.clone()).validate()?;
        request_id(&self.request_id)?;
        match &self.command {
            Command::Claim {
                profile_name,
                profile_version,
            } => {
                ensure!(check::name(profile_name), "invalid checker profile");
                parse_id(profile_version)?;
            }
            Command::GetClaim { claim_id } => request_id(claim_id)?,
            Command::Renew {
                lease,
                expected_generation,
            } => {
                lease.validate()?;
                ensure!(
                    *expected_generation <= MAX_SAFE,
                    "invalid renewal generation"
                );
            }
            Command::Manifest {
                lease,
                cursor,
                limit,
            } => {
                lease.validate()?;
                ensure!(
                    *cursor <= 32768 && *limit > 0 && *limit as usize <= PAGE_ENTRIES,
                    "manifest page limit"
                );
            }
            Command::Object {
                lease,
                key,
                offset,
                max_bytes,
            } => {
                lease.validate()?;
                key.native()?;
                ensure!(
                    key.kind != 5 && *offset <= MAX_SAFE && *max_bytes as usize <= CHUNK_BYTES,
                    "checker object range limit"
                );
            }
            Command::Complete { lease, receipt } | Command::Release { lease, receipt } => {
                lease.validate()?;
                receipt.validate_for(&lease.request)?;
                receipt.encode()?;
            }
            Command::GetResult {
                lease,
                result_request_id,
            } => {
                lease.validate()?;
                request_id(result_request_id)?;
            }
        }
        Ok(())
    }
    pub fn fingerprint(&self) -> Result<String> {
        self.validate()?;
        digest(b"tinytree-checker-command-v1\0", &self.command)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum Reply {
    Idle {},
    Preparing {
        request_id: String,
        manifest_id: String,
        generation: u64,
    },
    Leased {
        lease: CheckerLease,
    },
    Manifest {
        page: ManifestPage,
    },
    Object {
        chunk: ObjectChunk,
    },
    Completed {
        completion: Completion,
    },
    Released {
        lease: LeaseRef,
    },
    NotFound {},
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub schema: u32,
    pub account_id: String,
    pub request_id: String,
    pub result: Reply,
}
impl Response {
    pub fn decode(bytes: &[u8], call: &Call) -> Result<Self> {
        ensure!(bytes.len() <= MAX_REPLY_BYTES, "checker reply limit");
        let reply: Self = serde_json::from_slice(bytes)?;
        reply.validate_for(call)?;
        Ok(reply)
    }
    pub fn validate_for(&self, call: &Call) -> Result<()> {
        call.validate()?;
        ensure!(
            self.schema == 1
                && self.account_id == call.account_id
                && self.request_id == call.request_id,
            "checker response framing differs"
        );
        match (&self.result, &call.command) {
            (Reply::Idle {}, Command::Claim { .. } | Command::GetClaim { .. }) => {}
            (Reply::NotFound {}, Command::GetClaim { .. } | Command::GetResult { .. }) => {}
            (
                Reply::Preparing {
                    request_id: id,
                    manifest_id,
                    generation,
                },
                Command::Claim { .. } | Command::GetClaim { .. },
            ) => {
                request_id(id)?;
                parse_id(manifest_id)?;
                ensure!(*generation <= MAX_SAFE, "invalid manifest generation");
                let expected = match &call.command {
                    Command::GetClaim { claim_id } => claim_id,
                    _ => &call.request_id,
                };
                ensure!(id == expected, "checker claim binding differs");
            }
            (
                Reply::Leased { lease },
                command
                @ (Command::Claim { .. } | Command::GetClaim { .. } | Command::Renew { .. }),
            ) => {
                lease.validate()?;
                ensure!(
                    lease.scope.account_id == call.account_id,
                    "checker lease account differs"
                );
                match command {
                    Command::Claim {
                        profile_name,
                        profile_version,
                    } => ensure!(
                        &lease.request.profile_name == profile_name
                            && &lease.request.profile_version == profile_version,
                        "checker lease profile differs"
                    ),
                    Command::Renew {
                        lease: old,
                        expected_generation,
                    } => ensure!(
                        lease.reference() == *old && lease.generation == expected_generation + 1,
                        "checker renewal binding differs"
                    ),
                    _ => {}
                }
            }
            (
                Reply::Manifest { page },
                Command::Manifest {
                    lease,
                    cursor,
                    limit,
                },
            ) => {
                ensure!(
                    &page.lease == lease
                        && page.start == *cursor
                        && page.entries.len() <= *limit as usize,
                    "checker manifest page differs"
                );
                parse_id(&page.manifest_id)?;
                parse_id(&page.digest)?;
                ensure!(
                    page.next.is_none_or(
                        |n| n == page.start + page.entries.len() as u64 && n > page.start
                    ),
                    "checker manifest cursor differs"
                );
                for entry in &page.entries {
                    entry.validate(&SnapshotLimits::default())?;
                }
            }
            (
                Reply::Object { chunk },
                Command::Object {
                    lease,
                    key,
                    offset,
                    max_bytes,
                },
            ) => {
                chunk.descriptor.native()?;
                ensure!(
                    &chunk.lease == lease
                        && chunk.descriptor.key() == *key
                        && chunk.offset == *offset
                        && chunk.data.len() <= *max_bytes as usize,
                    "checker object binding differs"
                );
                let end = chunk
                    .offset
                    .checked_add(chunk.data.len() as u64)
                    .ok_or_else(|| anyhow::anyhow!("checker range overflow"))?;
                ensure!(
                    end <= chunk.descriptor.size_bytes
                        && chunk.eof == (end == chunk.descriptor.size_bytes),
                    "checker object length differs"
                );
            }
            (Reply::Completed { completion }, Command::Complete { lease, receipt }) => {
                ensure!(
                    &completion.lease == lease
                        && completion.result_request_id == call.request_id
                        && completion.outcome == receipt.outcome
                        && completion.receipt_digest == receipt_digest(receipt)?,
                    "checker completion differs"
                );
            }
            (
                Reply::Completed { completion },
                Command::GetResult {
                    lease,
                    result_request_id,
                },
            ) => {
                ensure!(
                    &completion.lease == lease
                        && &completion.result_request_id == result_request_id,
                    "checker result binding differs"
                );
                parse_id(&completion.receipt_digest)?;
            }
            (Reply::Released { lease: actual }, Command::Release { lease, .. }) => {
                ensure!(actual == lease, "checker cleanup binding differs")
            }
            _ => anyhow::bail!("unexpected checker response"),
        }
        Ok(())
    }
}
pub fn receipt_digest(receipt: &check::Receipt) -> Result<String> {
    receipt.validate_for(&receipt.request)?;
    digest(b"tinytree-checker-receipt-v1\0", receipt)
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPolicy {
    pub profile: LandingCheckProfile,
    pub targets: Vec<String>,
    pub test: bool,
}
impl ExecutionPolicy {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            check::name(&self.profile.name),
            "invalid checker policy profile"
        );
        parse_id(&self.profile.version)?;
        ensure!(
            !self.targets.is_empty()
                && self.targets.len() <= 128
                && self.targets.iter().map(String::len).sum::<usize>() <= 8192,
            "checker policy target limits"
        );
        for target in &self.targets {
            let split = target.split_once(':');
            ensure!(
                target.starts_with("//")
                    && split.is_some_and(|(_, name)| !name.is_empty()
                        && !name.contains(':')
                        && !matches!(name, "all" | "all-targets"))
                    && !target.contains("...")
                    && !target
                        .chars()
                        .any(|c| c.is_whitespace() || matches!(c, '*' | '?' | '\0')),
                "invalid exact checker target"
            );
        }
        let mut seen = std::collections::BTreeSet::new();
        ensure!(
            self.targets.iter().all(|t| seen.insert(t)),
            "duplicate checker target"
        );
        Ok(())
    }
    pub fn validate_receipt(
        &self,
        request: &check::Request,
        receipt: &check::Receipt,
    ) -> Result<()> {
        self.validate()?;
        receipt.validate_for(request)?;
        ensure!(
            request.profile_name == self.profile.name
                && request.profile_version == self.profile.version,
            "checker policy binding differs"
        );
        if matches!(
            receipt.outcome,
            check::Outcome::Passed | check::Outcome::Failed
        ) {
            ensure!(receipt.cleanup_confirmed, "checker cleanup unconfirmed");
            let evidence = receipt
                .evidence
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("checker evidence missing"))?;
            ensure!(
                evidence.requested_targets == self.targets,
                "checker target policy differs"
            );
            if receipt.outcome == check::Outcome::Passed {
                ensure!(
                    evidence.bazel_exit_code == 0
                        && evidence.execution_log_blake3.is_some()
                        && (!self.test || evidence.tests_verified),
                    "incomplete passing check policy"
                );
            } else {
                ensure!(
                    matches!(evidence.bazel_exit_code, 1 | 3),
                    "checker source failure not established"
                );
            }
        }
        Ok(())
    }
}

fn metadata_limits() -> model::Limits {
    model::Limits {
        max_object_bytes: 256 * 1024,
        max_items: 100_000,
        max_field_bytes: 256 * 1024,
        max_path_bytes: 4096,
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TreeFact {
    pub path: String,
    pub key: Key,
    pub value: EntryValue,
    pub expected_size: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPage {
    pub descriptor: Descriptor,
    pub candidate_root_tree: Option<String>,
    pub total: u64,
    pub next_cursor: Option<u64>,
    pub entries: Vec<TreeFact>,
}
/// Verified current-tree traversal facts, not a membership or storage proof.
/// COMMIT ancestry and CopyHistory payloads are deliberately never traversed.
pub fn snapshot_page(
    descriptor: &Descriptor,
    bytes: &[u8],
    path: &str,
    cursor: u64,
    limit: u32,
) -> Result<SnapshotPage> {
    ensure!(
        limit > 0 && limit as usize <= PAGE_ENTRIES,
        "checker metadata page limit"
    );
    validate_path(path, true)?;
    ensure!(
        path.len() <= 4096 && path.split('/').filter(|s| !s.is_empty()).count() <= 128,
        "checker metadata path limit"
    );
    ensure!(
        descriptor.size_bytes <= 256 * 1024,
        "checker metadata object limit"
    );
    model::verify(&descriptor.native()?, bytes, metadata_limits())?;
    match model::decode(bytes, metadata_limits())? {
        model::Object::Commit(commit) => {
            ensure!(
                descriptor.kind == 3
                    && path.is_empty()
                    && cursor == 0
                    && commit.root_tree.terms.len() == 1,
                "invalid checker root commit"
            );
            let root = commit.root_tree.terms[0]
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            Ok(SnapshotPage {
                descriptor: descriptor.clone(),
                candidate_root_tree: Some(root),
                total: 0,
                next_cursor: None,
                entries: vec![],
            })
        }
        model::Object::Tree(tree) => {
            ensure!(
                descriptor.kind == 2 && cursor <= tree.entries.len() as u64,
                "invalid checker tree cursor"
            );
            let total = tree.entries.len() as u64;
            let end = (cursor + u64::from(limit)).min(total);
            let mut entries = Vec::new();
            for entry in &tree.entries[cursor as usize..end as usize] {
                let child = if path.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{path}/{}", entry.name)
                };
                validate_path(&child, false)?;
                ensure!(
                    child.len() <= 4096 && child.split('/').count() <= 128,
                    "checker tree depth/path limit"
                );
                let (kind, id, value, expected_size) = match &entry.value {
                    model::TreeValue::Tree(id) => (2, id.0, EntryValue::Directory {}, None),
                    model::TreeValue::File {
                        id,
                        executable,
                        size_bytes,
                        copy_id,
                    } => (
                        1,
                        id.0,
                        EntryValue::File {
                            executable: *executable,
                            copy_id: copy_id
                                .map(|id| id.0.iter().map(|b| format!("{b:02x}")).collect()),
                        },
                        Some(*size_bytes),
                    ),
                    model::TreeValue::Symlink { id, target_bytes } => (
                        4,
                        id.0,
                        EntryValue::Symlink {
                            target_bytes: *target_bytes,
                        },
                        None,
                    ),
                };
                entries.push(TreeFact {
                    path: child,
                    key: Key {
                        kind,
                        id: id.iter().map(|b| format!("{b:02x}")).collect(),
                    },
                    value,
                    expected_size,
                });
            }
            Ok(SnapshotPage {
                descriptor: descriptor.clone(),
                candidate_root_tree: None,
                total,
                next_cursor: (end < total).then_some(end),
                entries,
            })
        }
        _ => anyhow::bail!("unsupported checker metadata kind"),
    }
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use model::{
        ChangeId, Commit, CommitId, CopyHistory, CopyId, EncodedObject, FileId, Object, Signature,
        Timestamp, Tree, TreeEntry, TreeId, TreeMerge, TreeValue,
    };

    struct Fixture {
        lease: CheckerLease,
        entries: Vec<ManifestEntry>,
        commit: EncodedObject,
        root: EncodedObject,
        copy: EncodedObject,
    }
    fn encode(object: Object) -> EncodedObject {
        model::encode(&object, model::Limits::default()).unwrap()
    }
    fn signature() -> Signature {
        Signature {
            name: "checker".into(),
            email: "checker@example.test".into(),
            timestamp: Timestamp {
                millis_since_epoch: 0,
                tz_offset_minutes: 0,
            },
        }
    }
    fn commit(tree: TreeId, parents: Vec<CommitId>, predecessors: Vec<CommitId>) -> EncodedObject {
        encode(Object::Commit(Commit {
            parents,
            predecessors,
            root_tree: TreeMerge {
                terms: vec![tree],
                labels: vec![String::new()],
            },
            change_id: ChangeId([7; 16]),
            description: "checker source".into(),
            author: signature(),
            committer: signature(),
            secure_signature: None,
        }))
    }
    fn seal(lease: &mut CheckerLease, entries: &[ManifestEntry]) {
        let mut hasher =
            ManifestHasher::new(&lease.request.candidate_commit, &lease.request.root_tree).unwrap();
        for entry in entries {
            hasher.push(entry).unwrap();
        }
        let totals = hasher.finish(&lease.limits).unwrap();
        lease.manifest = ManifestBinding {
            id: "66".repeat(32),
            digest: totals.digest,
            entries: totals.entries,
            file_bytes: totals.file_bytes,
            path_bytes: totals.path_bytes,
            metadata_bytes: totals.metadata_bytes,
        };
    }
    fn fixture() -> Fixture {
        let empty = encode(Object::Tree(Tree { entries: vec![] }));
        let file = model::describe_file(b"native source\n");
        let copy = encode(Object::CopyHistory(CopyHistory {
            current_path: "source.rs".into(),
            parents: vec![],
            salt: vec![1],
        }));
        let root = encode(Object::Tree(Tree {
            entries: vec![
                TreeEntry {
                    name: "empty".into(),
                    value: TreeValue::Tree(TreeId(empty.descriptor.key.id.0)),
                },
                TreeEntry {
                    name: "source.rs".into(),
                    value: TreeValue::File {
                        id: FileId(file.key.id.0),
                        executable: true,
                        size_bytes: file.size_bytes,
                        copy_id: Some(CopyId(copy.descriptor.key.id.0)),
                    },
                },
            ],
        }));
        let parent = commit(
            TreeId(empty.descriptor.key.id.0),
            vec![CommitId::ROOT],
            vec![],
        );
        let commit = commit(
            TreeId(root.descriptor.key.id.0),
            vec![CommitId(parent.descriptor.key.id.0)],
            vec![CommitId(parent.descriptor.key.id.0)],
        );
        let job = crate::identity::landing_job_id(
            "forge-team",
            crate::identity::review_owner(&AccountId("team".into()), ChangeId([7; 16])),
            "forge-fixture-0",
        );
        let request = check::Request {
            schema: 1,
            job_id: job.iter().map(|b| format!("{b:02x}")).collect(),
            attempt_id: "22".repeat(32),
            repository_id: "forge-team".into(),
            candidate_commit: Descriptor::from_native(&commit.descriptor).id,
            root_tree: Descriptor::from_native(&root.descriptor).id,
            profile_name: "unit".into(),
            profile_version: "55".repeat(32),
        };
        let mut lease = CheckerLease {
            schema: 1,
            lease_id: "88".repeat(32),
            generation: 0,
            scope: CandidateScope {
                account_id: "team".into(),
                view_version: "33".repeat(32),
                policy_epoch: 1,
                change_id: "07".repeat(16),
                patchset_id: "44".repeat(32),
                candidate_id: request.job_id.clone(),
                target_head: Descriptor::from_native(&parent.descriptor).id,
                target_generation: 1,
                publication_generation: 1,
            },
            request,
            manifest: ManifestBinding {
                id: "66".repeat(32),
                digest: "00".repeat(32),
                entries: 1,
                file_bytes: 0,
                path_bytes: 0,
                metadata_bytes: 0,
            },
            expires_at: 1000,
            limits: SnapshotLimits::default(),
        };
        let entries = vec![
            ManifestEntry {
                path: String::new(),
                descriptor: Descriptor::from_native(&root.descriptor),
                value: EntryValue::Directory {},
            },
            ManifestEntry {
                path: "empty".into(),
                descriptor: Descriptor::from_native(&empty.descriptor),
                value: EntryValue::Directory {},
            },
            ManifestEntry {
                path: "source.rs".into(),
                descriptor: Descriptor::from_native(&file),
                value: EntryValue::File {
                    executable: true,
                    copy_id: Some(Descriptor::from_native(&copy.descriptor).id),
                },
            },
        ];
        seal(&mut lease, &entries);
        SnapshotManifest {
            schema: 1,
            lease: lease.clone(),
            entries: entries.clone(),
        }
        .validate()
        .unwrap();
        Fixture {
            lease,
            entries,
            commit,
            root,
            copy,
        }
    }
    fn call(lease: &CheckerLease, command: Command) -> Call {
        Call {
            schema: 1,
            account_id: lease.scope.account_id.clone(),
            request_id: "checker-request".into(),
            command,
        }
    }
    fn response(call: &Call, result: Reply) -> Response {
        Response {
            schema: 1,
            account_id: call.account_id.clone(),
            request_id: call.request_id.clone(),
            result,
        }
    }

    #[test]
    fn strict_control_frames_and_renewal_preserve_the_execution_nonce() {
        let fixture = fixture();
        let lease = fixture.lease;
        let claim = call(
            &lease,
            Command::Claim {
                profile_name: lease.request.profile_name.clone(),
                profile_version: lease.request.profile_version.clone(),
            },
        );
        let encoded = serde_json::to_string(&claim).unwrap();
        assert_eq!(Call::decode(encoded.as_bytes()).unwrap(), claim);
        assert!(
            Call::decode(
                encoded
                    .replacen("\"schema\":1", "\"schema\":1,\"schema\":1", 1)
                    .as_bytes()
            )
            .is_err()
        );
        assert!(
            Call::decode(
                encoded
                    .replacen(
                        "\"action\":\"claim\"",
                        "\"action\":\"claim\",\"actor\":\"owner\"",
                        1
                    )
                    .as_bytes()
            )
            .is_err()
        );
        let reply = response(
            &claim,
            Reply::Leased {
                lease: lease.clone(),
            },
        );
        let bytes = serde_json::to_string(&reply).unwrap();
        assert_eq!(Response::decode(bytes.as_bytes(), &claim).unwrap(), reply);
        assert!(
            Response::decode(
                bytes
                    .replacen("\"schema\":1", "\"schema\":1,\"schema\":1", 1)
                    .as_bytes(),
                &claim
            )
            .is_err()
        );
        assert!(
            Response::decode(
                bytes
                    .replacen("\"result\":", "\"unexpected\":true,\"result\":", 1)
                    .as_bytes(),
                &claim
            )
            .is_err()
        );
        let mut rebound = reply.clone();
        rebound.request_id = "another-request".into();
        assert!(Response::decode(&serde_json::to_vec(&rebound).unwrap(), &claim).is_err());
        let renew = call(
            &lease,
            Command::Renew {
                lease: lease.reference(),
                expected_generation: lease.generation,
            },
        );
        let mut renewed = lease.clone();
        renewed.generation += 1;
        renewed.expires_at += 500;
        assert_eq!(
            renewed.immutable_binding().unwrap(),
            lease.immutable_binding().unwrap()
        );
        response(
            &renew,
            Reply::Leased {
                lease: renewed.clone(),
            },
        )
        .validate_for(&renew)
        .unwrap();
        renewed.request.attempt_id = "99".repeat(32);
        assert_ne!(
            renewed.immutable_binding().unwrap(),
            lease.immutable_binding().unwrap()
        );
        assert_ne!(renewed.request.workspace_id(), lease.request.workspace_id());
        assert!(
            response(&renew, Reply::Leased { lease: renewed })
                .validate_for(&renew)
                .is_err()
        );
    }

    #[test]
    fn paged_manifest_hashes_reconstruct_native_empty_directories_and_copy_identity() {
        let fixture = fixture();
        for page_size in [1, 2, 128] {
            let mut hash = ManifestHasher::new(
                &fixture.lease.request.candidate_commit,
                &fixture.lease.request.root_tree,
            )
            .unwrap();
            for page in fixture.entries.chunks(page_size) {
                for entry in page {
                    hash.push(entry).unwrap();
                }
                hash = ManifestHasher::from_state(hash.state()).unwrap();
            }
            assert_eq!(
                hash.finish(&fixture.lease.limits).unwrap().digest,
                fixture.lease.manifest.digest
            );
        }
        let mut missing_empty = fixture.entries.clone();
        missing_empty.remove(1);
        let mut changed_file = fixture.entries.clone();
        changed_file[2].descriptor =
            Descriptor::from_native(&model::describe_file(b"different source"));
        let mut lost_copy = fixture.entries.clone();
        if let EntryValue::File { copy_id, .. } = &mut lost_copy[2].value {
            *copy_id = None;
        }
        let mut changed_mode = fixture.entries.clone();
        if let EntryValue::File { executable, .. } = &mut changed_mode[2].value {
            *executable = false;
        }
        for (name, entries) in [
            ("missing empty directory", missing_empty),
            ("changed file", changed_file),
            ("lost copy identity", lost_copy),
            ("changed executable bit", changed_mode),
        ] {
            let mut lease = fixture.lease.clone();
            // Re-seal valid hash/totals: only actual native TREE coverage may reject this.
            seal(&mut lease, &entries);
            assert!(
                SnapshotManifest {
                    schema: 1,
                    lease,
                    entries
                }
                .validate()
                .is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn snapshot_pages_expose_current_tree_facts_not_history_payloads() {
        let fixture = fixture();
        let commit = snapshot_page(
            &Descriptor::from_native(&fixture.commit.descriptor),
            &fixture.commit.bytes,
            "",
            0,
            128,
        )
        .unwrap();
        assert_eq!(
            commit.candidate_root_tree.as_deref(),
            Some(fixture.lease.request.root_tree.as_str())
        );
        assert!(
            commit.entries.is_empty(),
            "COMMIT parents and predecessors are not snapshot read grants"
        );
        let root_descriptor = Descriptor::from_native(&fixture.root.descriptor);
        let first = snapshot_page(&root_descriptor, &fixture.root.bytes, "nested", 0, 1).unwrap();
        assert_eq!(first.entries[0].path, "nested/empty");
        assert_eq!(first.next_cursor, Some(1));
        let last = snapshot_page(&root_descriptor, &fixture.root.bytes, "nested", 1, 1).unwrap();
        assert_eq!(last.next_cursor, None);
        assert_eq!(last.entries[0].key.kind, 1);
        assert!(
            matches!(&last.entries[0].value, EntryValue::File {copy_id: Some(id), ..}
            if id == &Descriptor::from_native(&fixture.copy.descriptor).id)
        );
        assert!(
            [&first, &last]
                .iter()
                .flat_map(|page| &page.entries)
                .all(|entry| !matches!(entry.key.kind, 3 | 5))
        );
        assert!(
            snapshot_page(
                &Descriptor::from_native(&fixture.copy.descriptor),
                &fixture.copy.bytes,
                "source.rs",
                0,
                128
            )
            .is_err()
        );
        assert!(snapshot_page(&root_descriptor, &fixture.root.bytes, "../escape", 0, 1).is_err());
        assert!(snapshot_page(&root_descriptor, &fixture.root.bytes, "nested", 3, 1).is_err());
        assert!(
            snapshot_page(
                &Descriptor::from_native(&fixture.commit.descriptor),
                &fixture.commit.bytes,
                "nested",
                0,
                1
            )
            .is_err()
        );
    }

    #[test]
    fn execution_policy_requires_exact_targets_test_evidence_and_confirmed_cleanup() {
        let request = fixture().lease.request;
        let policy = ExecutionPolicy {
            profile: LandingCheckProfile {
                name: request.profile_name.clone(),
                version: request.profile_version.clone(),
            },
            targets: vec!["//app:unit".into(), "//app:integration".into()],
            test: true,
        };
        let passed = check::Receipt {
            schema: 1,
            request: request.clone(),
            workspace_id: request.workspace_id(),
            outcome: check::Outcome::Passed,
            reason: "check_passed".into(),
            cleanup_confirmed: true,
            evidence: Some(check::Evidence {
                invocation_id: "12345678-abcd-1234-abcd-123456789abc".into(),
                bep_blake3: "aa".repeat(32),
                execution_log_blake3: Some("bb".repeat(32)),
                requested_targets: policy.targets.clone(),
                tests_verified: true,
                bazel_exit_code: 0,
            }),
        };
        policy.validate_receipt(&request, &passed).unwrap();
        let mut wrong_targets = passed.clone();
        wrong_targets
            .evidence
            .as_mut()
            .unwrap()
            .requested_targets
            .reverse();
        let mut no_test = passed.clone();
        no_test.evidence.as_mut().unwrap().tests_verified = false;
        let mut no_log = passed.clone();
        no_log.evidence.as_mut().unwrap().execution_log_blake3 = None;
        let mut no_cleanup = passed.clone();
        no_cleanup.cleanup_confirmed = false;
        for receipt in [wrong_targets, no_test, no_log, no_cleanup] {
            assert!(policy.validate_receipt(&request, &receipt).is_err());
        }
        let mut failed = passed.clone();
        failed.outcome = check::Outcome::Failed;
        failed.reason = "check_failed".into();
        failed.evidence.as_mut().unwrap().execution_log_blake3 = None;
        for exit in [1, 3] {
            failed.evidence.as_mut().unwrap().bazel_exit_code = exit;
            policy.validate_receipt(&request, &failed).unwrap();
        }
        failed.evidence.as_mut().unwrap().bazel_exit_code = 0;
        assert!(policy.validate_receipt(&request, &failed).is_err());
        failed.evidence.as_mut().unwrap().bazel_exit_code = 1;
        failed.cleanup_confirmed = false;
        assert!(policy.validate_receipt(&request, &failed).is_err());
        let unavailable = check::Receipt {
            outcome: check::Outcome::Unavailable,
            reason: "cleanup_required".into(),
            cleanup_confirmed: false,
            evidence: None,
            ..passed
        };
        policy.validate_receipt(&request, &unavailable).unwrap();
    }
}
