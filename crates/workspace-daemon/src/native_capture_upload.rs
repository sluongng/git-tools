//! Optional saved-capture publication and exact-proof redundant capsule cleanup.
//! A historical Published record alone never grants local retirement.
mod availability;
use crate::{
    config::{Config, NativeCaptureUpload, NativeCaptureUploadPolicy},
    native_commands::NativeCommandHost,
};
use anyhow::{Context, Result, ensure};
use jj_native_backend::NativeObjectIo as _;
use native_capture_pack::{Limits as PackLimits, VerifiedPack};
use native_retention::{CaptureReadGuard, PendingCapture};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tinytree_client::{
    CloudCapturedPublishIntent, CloudIndexArtifactId, CloudManifestId, CloudObjectId,
    CloudPublicationIdentity, CloudPublishIntent, CloudRawDigest, RetainedVersionSelector,
};
use tinytree_cloud as c;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
type Owner = Arc<dyn Any + Send + Sync>;
pub(crate) struct Source {
    pub io: Arc<jj_native_remote::RemoteObjectIo>,
    pub local: Arc<jj_native_backend::DirectObjectIo>,
    pub restore: jj_native_remote::RemoteRestoreIo,
    pub control: jj_native_remote::RemoteRepositoryControl,
    pub identity: [u8; 32],
}
// Attempt-local progress follows detached work, but only the waiter records an
// outcome. A late physical completion cannot overwrite another poll's result.
struct Progress(Mutex<&'static str>);
impl Progress {
    fn new(stage: &'static str) -> Arc<Self> {
        Arc::new(Self(Mutex::new(stage)))
    }
    fn at(&self, stage: &'static str) {
        if let Ok(mut v) = self.0.lock() {
            *v = stage;
        }
    }
    fn stage(&self) -> &'static str {
        self.0.lock().map(|v| *v).unwrap_or("unavailable")
    }
}
#[derive(Clone, Copy, Serialize)]
struct Outcome {
    stage: &'static str,
    result: &'static str,
    error_class: &'static str,
    error_code: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    native_stage: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    native_reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    publication_reason: Option<&'static str>,
}
fn safe_publication_reason(error: &anyhow::Error) -> Option<&'static str> {
    // Inspect only an exact fixed vocabulary, never display or retain raw text,
    // details, metadata, identities, payloads or arbitrary context strings.
    for cause in error.chain().take(32) {
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            return (status.code() == tonic::Code::ResourceExhausted)
                .then(|| tinytree_client::PublicationFailureReason::from_message(status.message()))
                .flatten()
                .map(tinytree_client::PublicationFailureReason::as_str);
        }
    }
    None
}
fn definitive_missing_manifest_parent(error: &anyhow::Error) -> bool {
    error.chain().take(32).any(|cause| {
        cause.downcast_ref::<tonic::Status>().is_some_and(|status| {
            status.code() == tonic::Code::FailedPrecondition
                && status.message() == "cloud manifest parent unavailable"
        })
    })
}
fn safe_error(error: &anyhow::Error) -> (&'static str, &'static str) {
    // Never inspect Display, RPC messages/details/metadata, paths, payloads or
    // arbitrary context strings. Only fixed projections of known error types.
    for cause in error.chain().take(32) {
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            use tonic::Code::*;
            return (
                "rpc",
                match status.code() {
                    Ok => "ok",
                    Cancelled => "cancelled",
                    Unknown => "unknown",
                    InvalidArgument => "invalid_argument",
                    DeadlineExceeded => "deadline_exceeded",
                    NotFound => "not_found",
                    AlreadyExists => "already_exists",
                    PermissionDenied => "permission_denied",
                    ResourceExhausted => "resource_exhausted",
                    FailedPrecondition => "failed_precondition",
                    Aborted => "aborted",
                    OutOfRange => "out_of_range",
                    Unimplemented => "unimplemented",
                    Internal => "internal",
                    Unavailable => "unavailable",
                    DataLoss => "data_loss",
                    Unauthenticated => "unauthenticated",
                },
            );
        }
        if let Some(error) = cause.downcast_ref::<std::io::Error>() {
            use std::io::ErrorKind::*;
            return (
                "io",
                match error.kind() {
                    NotFound => "not_found",
                    PermissionDenied => "permission_denied",
                    AlreadyExists => "already_exists",
                    InvalidInput => "invalid_input",
                    InvalidData => "invalid_data",
                    TimedOut => "timed_out",
                    Interrupted => "interrupted",
                    WouldBlock => "would_block",
                    UnexpectedEof => "unexpected_eof",
                    WriteZero => "write_zero",
                    ConnectionRefused => "connection_refused",
                    ConnectionReset => "connection_reset",
                    BrokenPipe => "broken_pipe",
                    _ => "other",
                },
            );
        }
        if let Some(error) = cause.downcast_ref::<native_retention::Error>() {
            use native_retention::Error::*;
            return (
                "retention",
                match error {
                    Invalid(_) => "invalid",
                    Corrupt(_) => "corrupt",
                    Limit(_) => "limit",
                    Storage => "storage",
                    Busy => "busy",
                    UnknownWorkspace => "unknown_workspace",
                    UnknownCapture => "unknown_capture",
                    Forgotten => "forgotten",
                    IdentityMismatch => "identity_mismatch",
                    StaleRegistration => "stale_registration",
                    ClosedCommand => "closed_command",
                    CheckpointConflict => "checkpoint_conflict",
                    DirtyWorkspace => "dirty_workspace",
                    UntrackedAccess => "untracked_access",
                    Poisoned => "poisoned",
                    DeadlineExceeded => "deadline_exceeded",
                },
            );
        }
        if cause.is::<tokio::time::error::Elapsed>() {
            return ("timeout", "deadline_exceeded");
        }
        if let Some(error) = cause.downcast_ref::<tokio::task::JoinError>() {
            return (
                "task",
                if error.is_cancelled() {
                    "cancelled"
                } else {
                    "panicked"
                },
            );
        }
    }
    ("local_validation", "unclassified")
}
pub(crate) struct AvailabilityChange(Arc<Stats>);
impl Drop for AvailabilityChange {
    fn drop(&mut self) {
        self.0.invalidate_availability();
    }
}
#[derive(Default)]
pub(crate) struct Stats {
    availability: availability::Cache,
    attempts: AtomicU64,
    published: AtomicU64,
    failed: AtomicU64,
    retired: AtomicU64,
    retiring: AtomicBool,
    cleanup_pending: AtomicBool,
    last: Mutex<&'static str>,
    last_upload: Mutex<Option<Outcome>>,
    last_retirement: Mutex<Option<Outcome>>,
}
impl Stats {
    pub fn status(&self) -> serde_json::Value {
        let retiring = self.retiring.load(Ordering::Relaxed);
        serde_json::json!({
            "policy":if retiring {"publish_retiring_redundant"} else {"publish_retaining_roots"},
            "attempted":self.attempts.load(Ordering::Relaxed),
            "historical_published_count":self.published.load(Ordering::Relaxed),
            "failed":self.failed.load(Ordering::Relaxed),
            "retired_local_capture_count":self.retired.load(Ordering::Relaxed),
            "cleanup_pending":self.cleanup_pending.load(Ordering::Acquire),
            "policy_retires_redundant_capsules":retiring,
            "last_disposition":self.last.lock().map(|v|*v).unwrap_or("unavailable"),
            "last_upload_outcome":self.last_upload.lock().ok().and_then(|v| *v),
            "last_retirement_outcome":self.last_retirement.lock().ok().and_then(|v| *v),
            "local_roots_retained":true,"current_backup_availability_verified":false,
            "selected_capsule_availability":self.availability.status()
        })
    }
    pub(crate) fn availability_change(self: &Arc<Self>) -> AvailabilityChange {
        self.invalidate_availability();
        AvailabilityChange(self.clone())
    }
    pub(crate) fn invalidate_availability(&self) {
        self.availability.invalidate();
    }
    fn outcome<T>(
        &self,
        retirement: bool,
        progress: &Progress,
        result: &std::result::Result<Result<T>, tokio::time::error::Elapsed>,
    ) {
        let stage = progress.stage();
        let publication_reason = if !retirement && stage == "publication" {
            match result {
                Ok(Err(error)) => safe_publication_reason(error),
                _ => None,
            }
        } else {
            None
        };
        let (outcome_result, error_class, error_code) = match result {
            Ok(Ok(_)) => ("succeeded", "none", "none"),
            Ok(Err(error)) => {
                let (class, code) = safe_error(error);
                ("failed", class, code)
            }
            Err(_) => ("timed_out", "timeout", "deadline_exceeded"),
        };
        let native_failure = match result {
            Ok(Err(error)) => error
                .downcast_ref::<jj_native_remote::NativeCaptureFailure>()
                .copied(),
            _ => None,
        };
        let target = if retirement {
            &self.last_retirement
        } else {
            &self.last_upload
        };
        if let Ok(mut value) = target.lock() {
            *value = Some(Outcome {
                stage,
                result: outcome_result,
                error_class,
                error_code,
                native_stage: native_failure.map(|e| e.stage().as_str()),
                native_reason: native_failure.map(|e| e.reason().as_str()),
                publication_reason,
            });
        }
    }
    fn set(&self, s: &'static str) {
        if let Ok(mut v) = self.last.lock() {
            *v = s;
        }
    }
}
pub(crate) struct Handle {
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
    #[cfg(test)]
    worker: Arc<Worker>,
}
impl Handle {
    pub fn request_stop(&self) {
        let _ = self.stop.send(true);
    }
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for Handle {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
struct OwnedValue<T> {
    value: T,
    _owner: Owner,
}
impl<T> std::ops::Deref for OwnedValue<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
async fn blocking_owned<T: Send + 'static>(
    owner: Owner,
    job: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<OwnedValue<T>> {
    let completed = tokio::task::spawn_blocking(move || OwnedValue {
        value: job(),
        _owner: owner,
    })
    .await?;
    Ok(OwnedValue {
        value: completed.value?,
        _owner: completed._owner,
    })
}
struct Admission {
    _permit: OwnedSemaphorePermit,
}
fn native_retained_witness(
    w: &jj_native_remote::RetainedOffloadWitness,
) -> native_retention::RetainedSourceWitness {
    native_retention::RetainedSourceWitness {
        authority: w.authority,
        pin: w.pin,
        binding: w.binding,
        roots: w.roots,
    }
}
fn remote_retained_witness(
    p: &native_retention::RetainedOldCaptureV1,
) -> jj_native_remote::RetainedOffloadWitness {
    jj_native_remote::RetainedOffloadWitness {
        capture: p.retired_binding,
        authority: p.source.authority,
        pin: p.source.pin,
        binding: p.source.binding,
        roots: p.source.roots,
    }
}
fn validate_retained_journals(
    directory: &PrivateDir,
    id: native_retention::CaptureId,
    upload: &native_retention::CaptureUploadCleanup,
    present: bool,
) -> Result<()> {
    for (suffix, expected) in [("job", upload.job), ("published", upload.published)] {
        let name = format!("{}.{}", hex(&id.0), suffix);
        let file = validate_cleanup_file(
            directory,
            &name,
            expected.context("retained cleanup journal descriptor missing")?,
        )?;
        ensure!(
            !present || file.is_some(),
            "retained old journal missing before commit"
        );
    }
    let name = format!("{}.published", hex(&id.0));
    if !path_absent(directory, &name)? {
        let published = directory.read(&name, 4096)?;
        let attempts = validate_attempt_cleanup(directory, &published)?;
        ensure!(
            !present || attempts.iter().all(Option::is_some),
            "retained attempt missing before commit"
        );
    } else {
        ensure!(!present, "retained publication missing before commit");
        for ordinal in 1..=MAX_SUCCESSORS {
            ensure!(
                path_absent(directory, &attempt_name(&id.0, ordinal))?,
                "retained attempt lacks publication witness"
            );
        }
    }
    Ok(())
}
struct RetirementProof {
    upload: native_retention::CaptureUploadCleanup,
    // This owner contains admission only, NEVER a CaptureReadGuard. Raw
    // immutable witness bytes retain their budget through exclusive commit.
    coverage: native_retention::OwnedCaptureCoverage,
    basis: Option<native_retention::CaptureCleanupBasis>,
    retained: Option<Arc<jj_native_remote::PinnedRetainedVersion>>,
}
enum RetirementCheck {
    Ready(RetirementProof),
    NotCovered,
    CanonicalUnproved,
}
enum RetirementAttempt {
    Unchanged,
    Changed,
    CanonicalUnproved,
}
impl RetirementAttempt {
    fn from_changed(value: bool) -> Self {
        if value {
            Self::Changed
        } else {
            Self::Unchanged
        }
    }
}
// One selected old/new pair, not a queue-wide decoded-intent cache. This
// conservative reservation covers both saved intents, bounded raw input/copies
// and fresh SDK attachment buffers while one semantic decoder runs sequentially.
const PAIR_RETAINED_BYTES: u64 = 128 * 1024 * 1024;
struct EquivalentSavedPair {
    old: Intent,
    newer: Intent,
    old_job: native_retention::BundleDescriptor,
    newer_job: native_retention::BundleDescriptor,
    newer_published: Option<native_retention::BundleDescriptor>,
    snapshot: [u8; 32],
}
struct RetirementOwner {
    _old: Arc<CaptureReadGuard>,
    _newer: Arc<CaptureReadGuard>,
    _admission: Arc<Admission>,
}
struct JobOwner {
    _read: Arc<CaptureReadGuard>,
    _admission: Arc<Admission>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Header {
    version: u32,
    ticket: [u8; 32],
    ticket_binding: [u8; 32],
    provider: [u8; 32],
    workspace: [u8; 32],
    expected_generation: u64,
    key: String,
    manifest_id: [u8; 32],
    manifest_size: u64,
    capture_size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    successor: Option<Successor>,
    // Absent in legacy durable envelopes: never infer a typed receipt domain
    // from key text, and never rewrite an uncertain legacy attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    publication_identity: Option<PublicationIdentityV2>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PublicationIdentityV2 {
    version: u32,
    workspace: [u8; 32],
    generation: u64,
    nonce: [u8; 16],
}
impl PublicationIdentityV2 {
    fn sdk(&self) -> CloudPublicationIdentity {
        CloudPublicationIdentity {
            workspace_id: self.workspace,
            generation: self.generation,
            nonce: self.nonce,
        }
    }
}
// Small envelope-only journal: payload bytes always come from the original job.
// The accepted header commits the exact complete predecessor list for cleanup.
const MAX_SUCCESSORS: usize = 4;
// All four bounded DTOs, descriptor vectors, serialization and streaming-hash
// scratch share this reservation within the existing per-job memory policy.
const SUCCESSOR_MEMORY: u64 = 128 * 1024;
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Successor {
    original: native_retention::BundleDescriptor,
    predecessors: Vec<native_retention::BundleDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ancestry: Option<SuccessorAncestry>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SuccessorAncestry {
    parent: [u8; 32],
    // Same-generation replacement is admitted only after the server's specific
    // missing-parent refusal. This is not permission to replay an uncertain ID.
    #[serde(default)]
    missing_parent_refusal: bool,
}
fn attempt_name(ticket: &[u8; 32], ordinal: usize) -> String {
    format!("{}.attempt-{ordinal}", hex(ticket))
}
fn attempt_filename(name: &str) -> Option<([u8; 32], usize)> {
    let (ticket, ordinal) = name.split_once(".attempt-")?;
    let ticket = unhex_census(ticket).ok()?;
    let ordinal = ordinal.parse::<usize>().ok()?;
    (ordinal > 0 && ordinal <= MAX_SUCCESSORS && name == attempt_name(&ticket, ordinal))
        .then_some((ticket, ordinal))
}
impl Header {
    fn identity(&self) -> Result<Option<CloudPublicationIdentity>> {
        let Some(identity) = &self.publication_identity else {
            return Ok(None);
        };
        let sdk = identity.sdk();
        ensure!(
            self.version == 2
                && identity.version == 2
                && identity.workspace == self.workspace
                && self.expected_generation.checked_add(1) == Some(identity.generation)
                && self.key == sdk.token(),
            "capture structured publication identity differs"
        );
        Ok(Some(sdk))
    }
    fn use_new_v2_identity(&mut self) -> Result<()> {
        self.version = 2;
        let identity = PublicationIdentityV2 {
            version: 2,
            workspace: self.workspace,
            generation: self
                .expected_generation
                .checked_add(1)
                .context("capture generation overflow")?,
            nonce: *uuid::Uuid::new_v4().as_bytes(),
        };
        self.key = identity.sdk().token();
        self.publication_identity = Some(identity);
        Ok(())
    }
    fn valid(&self) -> bool {
        let typed = self.publication_identity.is_some();
        let envelope = match &self.successor {
            None => {
                if typed {
                    self.version == 2
                } else {
                    self.version == 1 && self.key == format!("capture_{}", hex(&self.ticket))
                }
            }
            Some(s) => {
                self.version == 2
                    && s.predecessors.len() < MAX_SUCCESSORS
                    && s.original.size_bytes > 0
                    && s.original.size_bytes <= 16 * 1024 * 1024 + 4108
                    && s.predecessors
                        .iter()
                        .all(|p| p.size_bytes > 0 && p.size_bytes <= 4096)
                    && (s.ancestry.is_none() || typed)
                    && (typed
                        || self.key
                            == format!(
                                "capture_{}_attempt_{}",
                                hex(&self.ticket),
                                s.predecessors.len() + 1
                            ))
            }
        };
        envelope
            && self.identity().is_ok()
            && self.manifest_size <= 8 * 1024 * 1024
            && self.capture_size <= 8 * 1024 * 1024
    }
}
enum PublicationLookup {
    Accepted(tinytree_client::CloudPublication),
    Absent,
    ReplayExpired,
}
async fn publication_lookup(
    client: &tinytree_client::NativeClient,
    header: &Header,
) -> Result<PublicationLookup> {
    let result = match header.identity()? {
        Some(identity) => client.cloud_publication_v2(identity).await,
        None => client.cloud_publication(&header.key).await,
    };
    Ok(match result {
        Ok(Some(receipt)) => PublicationLookup::Accepted(receipt),
        Ok(None) => PublicationLookup::Absent,
        // An expired receipt is terminal, not evidence of nonpublication.
        // Only a subsequent generation fence can authorize a successor.
        Err(error) if error.code() == tonic::Code::OutOfRange => PublicationLookup::ReplayExpired,
        Err(error) => return Err(error.into()),
    })
}
async fn publication(
    client: &tinytree_client::NativeClient,
    header: &Header,
) -> Result<Option<tinytree_client::CloudPublication>> {
    match publication_lookup(client, header).await? {
        PublicationLookup::Accepted(receipt) => Ok(Some(receipt)),
        PublicationLookup::Absent => Ok(None),
        PublicationLookup::ReplayExpired => {
            Err(tonic::Status::out_of_range("cloud publication replay expired").into())
        }
    }
}
async fn publish(
    client: &tinytree_client::NativeClient,
    intent: &Intent,
) -> Result<tinytree_client::CloudPublication> {
    let sdk = intent.sdk()?;
    Ok(match intent.header.identity()? {
        Some(identity) => {
            client
                .cloud_publish_captured_version_v2(&sdk, identity)
                .await?
        }
        None => client.cloud_publish_captured_version(&sdk).await?,
    })
}
async fn require_new_publication_v2(client: &tinytree_client::NativeClient) -> Result<()> {
    let features = client
        .capabilities(&[tinytree_client::CapabilityFeature::StructuredCloudPublicationV2])
        .await?;
    let feature = features.features[0];
    ensure!(
        feature.authorized,
        tonic::Status::permission_denied("structured cloud publication is not authorized")
    );
    ensure!(
        feature.supported,
        tonic::Status::unimplemented("structured cloud publication is not supported")
    );
    Ok(())
}
/// Header-only startup witness; deliberately no formatting or serialization.
pub(crate) struct ProviderJournalWitness {
    pub ticket: [u8; 32],
    pub workspace: [u8; 32],
    pub provider: [u8; 32],
}
pub(crate) fn inspect_provider_journals(
    config: &Config,
    census: &mut crate::provider_identity::Census,
) -> Result<Vec<ProviderJournalWitness>> {
    let path = config.state_dir.join("native-commands/upload");
    if !path.try_exists()? {
        return Ok(Vec::new());
    }
    let directory = PrivateDir::open_existing(path)?;
    let mut result = Vec::new();
    for item in std::fs::read_dir(directory.path())? {
        census.entry()?;
        let name = item?
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("invalid provider upload census name"))?;
        if census_temporary(&name) {
            // Atomic publication remnants are not committed identity witnesses.
            let file = directory
                .open_existing_file(&name, config.native.workspaces.upload.max_job_bytes as u64)?;
            drop(file);
            continue;
        }
        let (id, job) = if attempt_filename(&name).is_some() {
            (name.split_once(".attempt-").unwrap().0, false)
        } else if let Some(id) = name.strip_suffix(".job") {
            (id, true)
        } else if let Some(id) = name.strip_suffix(".published") {
            (id, false)
        } else {
            anyhow::bail!("unknown provider upload census entry");
        };
        let ticket = unhex_census(id)?;
        let mut file = directory
            .open_existing_file(&name, config.native.workspaces.upload.max_job_bytes as u64)?;
        let len = file.metadata()?.len();
        let bytes = if job {
            census.charge(12)?;
            let mut prefix = [0; 12];
            file.read_exact(&mut prefix)?;
            ensure!(
                &prefix[..8] == b"TTUPJOB1",
                "invalid provider upload census framing"
            );
            let n = u32::from_le_bytes(prefix[8..].try_into()?) as usize;
            ensure!(n > 0 && n <= 4096, "provider upload census header limit");
            census.charge(n)?;
            let mut bytes = vec![0; n];
            file.read_exact(&mut bytes)?;
            bytes
        } else {
            ensure!(len <= 4096, "provider publication census header limit");
            census.charge(len as usize)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            bytes
        };
        let header: Header = serde_json::from_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid provider upload census header"))?;
        ensure!(
            header.valid()
                && header.ticket == ticket
                && header.provider != [0; 32]
                && header.workspace != [0; 32]
                && header.manifest_size <= 8 * 1024 * 1024
                && header.capture_size <= 8 * 1024 * 1024,
            "invalid provider upload census header"
        );
        ensure!(
            (!job || header.successor.is_none())
                && attempt_filename(&name).is_none_or(|(_, ordinal)| header
                    .successor
                    .as_ref()
                    .is_some_and(|s| s.predecessors.len() + 1 == ordinal)),
            "provider upload census envelope differs"
        );
        if job {
            ensure!(
                12u64 + bytes.len() as u64 + header.manifest_size + header.capture_size == len,
                "provider upload census declaration differs"
            );
        }
        census.charge(512)?;
        result.push(ProviderJournalWitness {
            ticket,
            workspace: header.workspace,
            provider: header.provider,
        });
    }
    Ok(result)
}
fn census_temporary(name: &str) -> bool {
    if let Some(id) = name
        .strip_prefix("upload-")
        .and_then(|s| s.strip_suffix(".part"))
    {
        return uuid::Uuid::parse_str(id).is_ok_and(|value| value.to_string() == id);
    }
    let Some(body) = name
        .strip_prefix(".private-")
        .and_then(|s| s.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((pid, sequence)) = body.split_once('-') else {
        return false;
    };
    !pid.is_empty()
        && !sequence.is_empty()
        && pid.bytes().all(|b| b.is_ascii_digit())
        && sequence.bytes().all(|b| b.is_ascii_digit())
        && pid.parse::<u32>().is_ok()
        && sequence.parse::<u64>().is_ok()
}
fn unhex_census(value: &str) -> Result<[u8; 32]> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid provider upload census identity"
    );
    let mut bytes = [0; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)
            .map_err(|_| anyhow::anyhow!("invalid provider upload census identity"))?;
    }
    Ok(bytes)
}
struct Intent {
    original_job: Option<native_retention::BundleDescriptor>,
    header: Header,
    manifest: Vec<u8>,
    // Original bytes remain the sole source for deterministic parent changes.
    original_manifest: Option<Vec<u8>>,
    capture: Vec<u8>,
}
impl Intent {
    fn sdk(&self) -> Result<CloudCapturedPublishIntent> {
        Ok(CloudCapturedPublishIntent::new(
            CloudPublishIntent {
                workspace_id: self.header.workspace,
                expected_generation: self.header.expected_generation,
                idempotency_key: self.header.key.clone(),
                manifest: self.manifest.clone(),
                staging_leases: Vec::new(),
            },
            self.capture.clone(),
        )?)
    }
    fn encode(&self) -> Result<Vec<u8>> {
        let header = serde_json::to_vec(&self.header)?;
        ensure!(header.len() <= 4096, "capture intent header exceeds limit");
        let mut out = b"TTUPJOB1".to_vec();
        out.extend((header.len() as u32).to_le_bytes());
        out.extend(header);
        out.extend(&self.manifest);
        out.extend(&self.capture);
        Ok(out)
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= 12 && &bytes[..8] == b"TTUPJOB1",
            "invalid capture upload job"
        );
        let n = u32::from_le_bytes(bytes[8..12].try_into()?) as usize;
        ensure!(
            n <= 4096 && 12 + n <= bytes.len(),
            "capture job header limit"
        );
        let header: Header = serde_json::from_slice(&bytes[12..12 + n])?;
        ensure!(
            header.valid()
                && header.successor.is_none()
                && header.manifest_size <= 8 * 1024 * 1024
                && header.capture_size <= 8 * 1024 * 1024,
            "invalid capture job declaration"
        );
        let mid = 12 + n;
        let end = mid
            .checked_add(header.manifest_size as usize)
            .context("capture job overflow")?;
        ensure!(
            end.checked_add(header.capture_size as usize) == Some(bytes.len()),
            "capture job size differs"
        );
        let manifest = bytes[mid..end].to_vec();
        let capture = bytes[end..].to_vec();
        ensure!(
            c::ManifestId::of(&manifest).0 == header.manifest_id,
            "capture job manifest digest differs"
        );
        let value = Self {
            original_job: Some(raw_descriptor(bytes)),
            header,
            manifest,
            original_manifest: None,
            capture,
        };
        value.sdk()?;
        Ok(value)
    }
}
#[derive(Default)]
struct TestHooks {
    #[cfg(test)]
    availability_calls: AtomicU64,
    #[cfg(test)]
    availability_reads: AtomicU64,
    #[cfg(test)]
    availability_proofs: AtomicU64,
    #[cfg(test)]
    availability_timeout_ms: Option<u64>,
    #[cfg(test)]
    availability_fault: AtomicU64,
    #[cfg(test)]
    availability_pause: Mutex<Option<Arc<CatalogPause>>>,
    #[cfg(test)]
    start_stopped: bool,
    #[cfg(test)]
    idle_entries: AtomicU64,
    #[cfg(test)]
    fail_namespace: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    catalog_pause: Mutex<Option<Arc<CatalogPause>>>,
    #[cfg(test)]
    cleanup_pause: Mutex<Option<Arc<CatalogPause>>>,
    #[cfg(test)]
    retirement_pause: Mutex<Option<Arc<CatalogPause>>>,
    #[cfg(test)]
    fail_after_attempt: AtomicBool,
    #[cfg(test)]
    fail_before_marker: AtomicBool,
    #[cfg(test)]
    fail_before_publication: AtomicBool,
    #[cfg(test)]
    attempt_pause: Mutex<Option<Arc<CatalogPause>>>,
    #[cfg(test)]
    fence_pause: Mutex<Option<Arc<CatalogPause>>>,
    #[cfg(test)]
    published_pause: Mutex<Option<Arc<CatalogPause>>>,
    #[cfg(test)]
    fail_after_alias_sync: AtomicBool,
    #[cfg(test)]
    retained_offload: AtomicBool,
    #[cfg(test)]
    managed_collection: bool,
    #[cfg(test)]
    disable_server_retained_contract: bool,
    #[cfg(test)]
    provider_pending_limit: Option<usize>,
    #[cfg(test)]
    rolling_window: Option<u64>,
}
#[cfg(test)]
#[derive(Default)]
struct CatalogPause {
    entered: std::sync::atomic::AtomicBool,
    released: Mutex<bool>,
    changed: std::sync::Condvar,
}
#[cfg(test)]
impl CatalogPause {
    fn block(&self) {
        self.entered.store(true, Ordering::Release);
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Worker {
    config: Config,
    policy: NativeCaptureUpload,
    host: Arc<NativeCommandHost>,
    sources: BTreeMap<String, Arc<Source>>,
    directory: Arc<PrivateDir>,
    stats: Arc<Stats>,
    admission: Arc<Semaphore>,
    next_ticket: Mutex<Option<(u64, native_retention::CaptureId)>>,
    next_retirement: Mutex<Option<(u64, native_retention::CaptureId)>>,
    availability_schedule: Mutex<availability::Schedule>,
    _hooks: Arc<TestHooks>,
}
pub(crate) async fn start(
    config: Config,
    host: Arc<NativeCommandHost>,
    sources: BTreeMap<String, Arc<Source>>,
) -> Result<Option<Handle>> {
    start_inner(config, host, sources, Arc::new(TestHooks::default())).await
}
async fn start_inner(
    config: Config,
    host: Arc<NativeCommandHost>,
    sources: BTreeMap<String, Arc<Source>>,
    hooks: Arc<TestHooks>,
) -> Result<Option<Handle>> {
    let policy = config.native.workspaces.upload.clone();
    if policy.policy == NativeCaptureUploadPolicy::Disabled
        || (policy.policy == NativeCaptureUploadPolicy::Auto
            && (sources.is_empty()
                || config.native.workspaces.capture.policy
                    == crate::config::NativeCapturePolicy::Disabled))
    {
        return Ok(None);
    }
    let stats = Arc::new(Stats::default());
    stats.retiring.store(
        matches!(
            policy.policy,
            NativeCaptureUploadPolicy::Auto | NativeCaptureUploadPolicy::PublishRetiringRedundant
        ),
        Ordering::Relaxed,
    );
    stats.set("idle");
    host.set_upload_status(stats.clone())?;
    let directory = match PrivateDir::create(config.state_dir.join("native-commands/upload"))
        .and_then(|directory| {
            File::open(config.state_dir.join("native-commands"))?.sync_all()?;
            Ok(directory)
        }) {
        Ok(directory) => Arc::new(directory),
        Err(error) => {
            stats.outcome(
                false,
                &Progress::new("inventory"),
                &Ok::<_, tokio::time::error::Elapsed>(Err::<(), _>(error.into())),
            );
            stats.failed.fetch_add(1, Ordering::Relaxed);
            stats.set("inventory_unavailable");
            return Ok(None);
        }
    };
    let worker = Arc::new(Worker {
        config,
        policy,
        host,
        sources,
        directory,
        stats,
        admission: Arc::new(Semaphore::new(1)),
        next_ticket: Mutex::new(None),
        next_retirement: Mutex::new(None),
        availability_schedule: Mutex::new(Default::default()),
        _hooks: hooks,
    });
    #[cfg(test)]
    let observer = worker.clone();
    #[cfg(test)]
    let initial_stop = worker._hooks.start_stopped;
    #[cfg(not(test))]
    let initial_stop = false;
    let (stop, mut rx) = watch::channel(initial_stop);
    let task = tokio::spawn(async move {
        loop {
            if *rx.borrow() {
                break;
            }
            match worker.poll().await {
                Ok(true) => {
                    // A durable publication or completed cleanup may expose more
                    // ready work. Yield and check stop before the next bounded job.
                    worker.observe_availability_due().await;
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(false) => {}
                Err(_) => {
                    worker.stats.failed.fetch_add(1, Ordering::Relaxed);
                    worker.stats.set("inventory_unavailable");
                }
            }
            worker.observe_availability_due().await;
            #[cfg(test)]
            worker._hooks.idle_entries.fetch_add(1, Ordering::Release);
            tokio::select! {_=rx.changed()=>{if *rx.borrow(){break;}},_=tokio::time::sleep(Duration::from_millis(worker.policy.poll_ms))=>{}}
        }
        // Detached provider/SDK work owns independent read fences. Stop polling,
        // then drain every configured source before releasing daemon ownership.
        if let Ok(barrier) = worker.admission.clone().acquire_owned().await {
            drop(barrier);
        }
        for source in worker.sources.values() {
            source.control.wait_for_owned_work().await;
        }
    });
    Ok(Some(Handle {
        stop,
        task: Some(task),
        #[cfg(test)]
        worker: observer,
    }))
}
impl Worker {
    fn inventory(&self) -> Result<(usize, u64, u64)> {
        let mut count = 0;
        let mut bytes = 0u64;
        let mut published = 0;
        let mut jobs = 0;
        let mut attempts = 0;
        for entry in std::fs::read_dir(self.directory.path())? {
            count += 1;
            ensure!(
                count <= self.policy.max_jobs * 4,
                "capture upload inventory limit"
            );
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_str().context("invalid capture upload filename")?;
            ensure!(
                (name.len() == 68 && name.ends_with(".job")
                    || name.len() == 74 && name.ends_with(".published")
                    || name.starts_with("upload-") && name.ends_with(".part")
                    || attempt_filename(name).is_some()),
                "unknown capture upload state"
            );
            let file = self
                .directory
                .open_existing_file(name, self.policy.max_job_bytes as u64)?;
            bytes = bytes
                .checked_add(file.metadata()?.len())
                .context("capture upload disk overflow")?;
            ensure!(
                bytes <= self.policy.max_disk_bytes,
                "capture upload disk budget exceeded"
            );
            if name.ends_with(".job") {
                jobs += 1;
                ensure!(
                    jobs <= self.policy.max_jobs,
                    "capture saved job limit exceeded"
                );
                let intent =
                    Intent::decode(&self.directory.read(name, self.policy.max_job_bytes)?)?;
                ensure!(
                    name == format!("{}.job", hex(&intent.header.ticket)),
                    "capture job filename differs"
                );
            }
            if let Some((ticket, ordinal)) = attempt_filename(name) {
                attempts += 1;
                ensure!(
                    attempts <= self.policy.max_jobs * MAX_SUCCESSORS,
                    "capture total successor limit"
                );
                let header: Header = serde_json::from_slice(&self.directory.read(name, 4096)?)?;
                ensure!(
                    header.valid()
                        && header.ticket == ticket
                        && header
                            .successor
                            .as_ref()
                            .is_some_and(|s| s.predecessors.len() + 1 == ordinal),
                    "invalid capture successor record"
                );
            }
            if name.ends_with(".published") {
                let header: Header = serde_json::from_slice(&self.directory.read(name, 4096)?)?;
                ensure!(
                    header.valid() && name == format!("{}.published", hex(&header.ticket)),
                    "invalid historical publication record"
                );
                published += 1;
            }
        }
        ensure!(
            bytes <= self.policy.max_disk_bytes,
            "capture upload disk budget exceeded"
        );
        Ok((count, bytes, published))
    }
    fn original_envelope(
        &self,
        ticket: &[u8; 32],
    ) -> Result<(Header, native_retention::BundleDescriptor)> {
        let mut file = self.directory.open_existing_file(
            &format!("{}.job", hex(ticket)),
            self.policy.max_job_bytes as u64,
        )?;
        let mut prefix = [0u8; 12];
        file.read_exact(&mut prefix)?;
        ensure!(
            &prefix[..8] == b"TTUPJOB1",
            "invalid original capture framing"
        );
        let size = u32::from_le_bytes(prefix[8..].try_into()?) as usize;
        ensure!(size > 0 && size <= 4096, "original capture header limit");
        let mut bytes = vec![0; size];
        file.read_exact(&mut bytes)?;
        let header: Header = serde_json::from_slice(&bytes)?;
        let len = file.metadata()?.len();
        ensure!(
            header.valid()
                && header.successor.is_none()
                && header.ticket == *ticket
                && 12 + size as u64 + header.manifest_size + header.capture_size == len,
            "original capture envelope differs"
        );
        let mut hash = blake3::Hasher::new();
        hash.update(&prefix);
        hash.update(&bytes);
        let mut buffer = [0u8; 16384];
        let mut read = 12 + size as u64;
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            read = read
                .checked_add(n as u64)
                .context("original capture read overflow")?;
            ensure!(read <= len, "original capture grew during inspection");
            hash.update(&buffer[..n]);
        }
        ensure!(read == len, "original capture shrank during inspection");
        file.sync_all()?;
        self.sync_namespace()?;
        Ok((
            header,
            native_retention::BundleDescriptor {
                blake3: *hash.finalize().as_bytes(),
                size_bytes: len,
            },
        ))
    }
    fn saved_attempts(&self, ticket: &[u8; 32]) -> Result<Vec<Header>> {
        let (original, original_descriptor) = self.original_envelope(ticket)?;
        let mut headers: Vec<Header> = Vec::new();
        let mut descriptors = Vec::new();
        for ordinal in 1..=MAX_SUCCESSORS {
            let name = attempt_name(ticket, ordinal);
            if path_absent(&self.directory, &name)? {
                break;
            }
            let bytes = self.read_bounded_durable(&name, 4096)?;
            ensure!(bytes.len() <= 4096, "capture successor header limit");
            let header: Header = serde_json::from_slice(&bytes)?;
            let successor = header
                .successor
                .as_ref()
                .context("capture successor envelope absent")?;
            ensure!(
                header.valid()
                    && successor.original == original_descriptor
                    && successor.predecessors == descriptors
                    && (header.expected_generation
                        > headers.last().unwrap_or(&original).expected_generation
                        || (header.expected_generation
                            == headers.last().unwrap_or(&original).expected_generation
                            && header.publication_identity.is_some()
                            && (headers
                                .last()
                                .unwrap_or(&original)
                                .publication_identity
                                .is_none()
                                || successor.ancestry.as_ref().is_some_and(|ancestry| {
                                    ancestry.missing_parent_refusal
                                        && headers
                                            .last()
                                            .unwrap_or(&original)
                                            .successor
                                            .as_ref()
                                            .and_then(|s| s.ancestry.as_ref())
                                            .map(|a| a.parent)
                                            != Some(ancestry.parent)
                                        && header.manifest_id
                                            != headers.last().unwrap_or(&original).manifest_id
                                })))),
                "capture successor chain differs"
            );
            ensure!(
                headers
                    .last()
                    .unwrap_or(&original)
                    .publication_identity
                    .is_none()
                    || header.publication_identity.is_some(),
                "capture successor cannot downgrade structured publication"
            );
            let mut normalized = header.clone();
            normalized.version = original.version;
            normalized.expected_generation = original.expected_generation;
            normalized.key = original.key.clone();
            normalized.successor = None;
            normalized.publication_identity = original.publication_identity.clone();
            if successor.ancestry.is_some() {
                normalized.manifest_id = original.manifest_id;
                normalized.manifest_size = original.manifest_size;
            }
            ensure!(
                normalized == original && serde_json::to_vec(&header)? == bytes,
                "capture successor payload binding differs"
            );
            descriptors.push(raw_descriptor(&bytes));
            headers.push(header);
        }
        for ordinal in headers.len() + 1..=MAX_SUCCESSORS {
            ensure!(
                path_absent(&self.directory, &attempt_name(ticket, ordinal))?,
                "capture successor journal gap"
            );
        }
        Ok(headers)
    }
    fn apply_published(&self, intent: &mut Intent, bytes: &[u8]) -> Result<()> {
        let header: Header = serde_json::from_slice(bytes)?;
        let attempts = self.saved_attempts(&intent.header.ticket)?;
        ensure!(
            header == *attempts.last().unwrap_or(&intent.header),
            "capture publication does not select exact journal envelope"
        );
        intent.header = header;
        self.materialize_selected(intent)?;
        Ok(())
    }
    fn sync_namespace(&self) -> Result<()> {
        #[cfg(test)]
        if self._hooks.fail_namespace.load(Ordering::Acquire) {
            anyhow::bail!("injected namespace barrier failure");
        }
        File::open(self.directory.path())?.sync_all()?;
        Ok(())
    }
    fn read_durable(&self, name: &str) -> Result<Vec<u8>> {
        self.read_bounded_durable(name, self.policy.max_job_bytes)
    }
    fn read_bounded_durable(&self, name: &str, limit: usize) -> Result<Vec<u8>> {
        let mut file = self.directory.open_existing_file(name, limit as u64)?;
        let mut bytes = Vec::new();
        (&mut file).take(limit as u64 + 1).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= limit, "capture durable record read exceeded");
        // A visible link is not evidence its namespace barrier succeeded in a
        // previous attempt. Reestablish BOTH barriers before trusting it.
        file.sync_all()?;
        self.sync_namespace()?;
        Ok(bytes)
    }
    fn persist(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.stats.invalidate_availability();
        ensure!(
            bytes.len() <= self.policy.max_job_bytes,
            "capture upload record limit"
        );
        if self.directory.path().join(name).try_exists()? {
            ensure!(
                self.read_durable(name)? == bytes,
                "immutable capture upload record differs"
            );
            return Ok(());
        }
        let (count, disk, _) = self.inventory()?;
        ensure!(
            count < self.policy.max_jobs * 4
                && disk
                    .checked_add(bytes.len() as u64)
                    .is_some_and(|n| n <= self.policy.max_disk_bytes),
            "capture upload disk admission exceeded"
        );
        let temporary = format!("upload-{}.part", uuid::Uuid::new_v4());
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.directory.path().join(&temporary))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        match std::fs::hard_link(
            self.directory.path().join(&temporary),
            self.directory.path().join(name),
        ) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                ensure!(
                    self.read_durable(name)? == bytes,
                    "capture publication record conflict"
                );
            }
            Err(e) => return Err(e.into()),
        }
        self.sync_namespace()?;
        std::fs::remove_file(self.directory.path().join(temporary))?;
        self.sync_namespace()?;
        Ok(())
    }
    async fn equivalent_saved_pair(
        self: &Arc<Self>,
        old: &PendingCapture,
        newer: &PendingCapture,
        admission: Arc<Admission>,
    ) -> Result<Option<(OwnedValue<EquivalentSavedPair>, Owner, Arc<Source>)>> {
        ensure!(
            old.registry == newer.registry
                && old.registration == newer.registration
                && old.registry_epoch < newer.registry_epoch
                && old.command_epoch < newer.command_epoch,
            "saved equivalent capture order or registration differs"
        );
        ensure!(
            self.policy.max_memory_bytes > 1024 * 1024 * 1024 + PAIR_RETAINED_BYTES,
            "saved equivalent capture proof admission too small"
        );
        let (mount, alias, identity) = self.host.capture_mapping(old)?;
        ensure!(
            self.host.capture_mapping(newer)? == (mount.clone(), alias.clone(), identity),
            "saved equivalent capture provider differs"
        );
        let source = self
            .sources
            .get(&alias)
            .context("saved equivalent provider unavailable")?
            .clone();
        ensure!(
            source.identity == identity,
            "saved equivalent configured provider changed"
        );
        let workspace = crate::native_workspaces::capture_upload_workspace(
            &self.config.state_dir,
            &mount,
            &alias,
            &identity,
        )?;
        let registry = self.host.capture_registry();
        let old_read = Arc::new(registry.read_capture(old.id).await?);
        let newer_read = Arc::new(registry.read_capture(newer.id).await?);
        ensure!(
            old_read.pending() == old && newer_read.pending() == newer,
            "saved equivalent capture ticket changed"
        );
        let owner: Owner = Arc::new(RetirementOwner {
            _old: old_read,
            _newer: newer_read,
            _admission: admission,
        });
        let worker = self.clone();
        let old = old.clone();
        let newer = newer.clone();
        let physical = owner.clone();
        let pair = blocking_owned(owner.clone(), move || {
            ensure!(
                path_absent(&worker.directory, &format!("{}.published", hex(&old.id.0)))?,
                "old equivalent capture is already published"
            );
            ensure!(
                worker.saved_attempts(&old.id.0)?.is_empty()
                    && worker.saved_attempts(&newer.id.0)?.is_empty(),
                "attempt journals require exact publication retirement"
            );
            let old_bytes = worker.read_durable(&format!("{}.job", hex(&old.id.0)))?;
            let old_job = raw_descriptor(&old_bytes);
            let old_intent = Intent::decode(&old_bytes)?;
            drop(old_bytes);
            let newer_bytes = worker.read_durable(&format!("{}.job", hex(&newer.id.0)))?;
            let newer_job = raw_descriptor(&newer_bytes);
            let newer_intent = Intent::decode(&newer_bytes)?;
            drop(newer_bytes);
            for (ticket, intent) in [(&old, &old_intent), (&newer, &newer_intent)] {
                ensure!(
                    intent.header.ticket == ticket.id.0
                        && intent.header.ticket_binding == ticket.binding()
                        && intent.header.provider == identity
                        && intent.header.workspace == workspace,
                    "saved equivalent capture binding differs"
                );
            }
            let published_name = format!("{}.published", hex(&newer.id.0));
            let newer_published = if path_absent(&worker.directory, &published_name)? {
                None
            } else {
                let bytes = worker.read_durable(&published_name)?;
                ensure!(
                    serde_json::from_slice::<Header>(&bytes)? == newer_intent.header,
                    "equivalent successor publication differs"
                );
                Some(raw_descriptor(&bytes))
            };
            // Sequential proofs share one allowance after retaining both intents.
            let snapshot = worker.verify_saved_reserved(
                &old,
                &old_intent,
                physical.clone(),
                PAIR_RETAINED_BYTES,
            )?;
            worker.verify_saved_reserved(&newer, &newer_intent, physical, PAIR_RETAINED_BYTES)?;
            Ok(EquivalentSavedPair {
                old: old_intent,
                newer: newer_intent,
                old_job,
                newer_job,
                newer_published,
                snapshot,
            })
        })
        .await?;
        if pair.old.capture != pair.newer.capture
            || pair.old.header.expected_generation != pair.newer.header.expected_generation
        {
            return Ok(None);
        }
        Ok(Some((pair, owner, source)))
    }
    async fn prefer_equivalent_saved_ticket(
        self: &Arc<Self>,
        selected: &PendingCapture,
        tickets: &[PendingCapture],
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
    ) -> Result<Option<PendingCapture>> {
        // Pair preference is optional. Preserve normal configured single-job
        // uploads when the existing budget cannot fund a second retained intent.
        if self.policy.max_memory_bytes <= 1024 * 1024 * 1024 + PAIR_RETAINED_BYTES {
            return Ok(None);
        }
        if path_absent(&self.directory, &format!("{}.job", hex(&selected.id.0)))? {
            return Ok(None);
        }
        // Pick only one saved strict-newer candidate. This is not reversal of
        // queue order or an unbounded collection of decoded candidate intents.
        let mut candidate = None;
        for ticket in tickets {
            if ticket.registry == selected.registry
                && ticket.registration == selected.registration
                && ticket.registry_epoch > selected.registry_epoch
                && ticket.command_epoch > selected.command_epoch
                && !path_absent(&self.directory, &format!("{}.job", hex(&ticket.id.0)))?
                && candidate
                    .as_ref()
                    .is_none_or(|old: &PendingCapture| old.registry_epoch < ticket.registry_epoch)
            {
                candidate = Some(ticket.clone());
            }
        }
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let Some((pair, owner, source)) = self
            .equivalent_saved_pair(selected, &candidate, admission)
            .await?
        else {
            return Ok(None);
        };
        if pair.newer_published.is_some() {
            return Ok(None);
        }
        let client = source
            .control
            .client_for_owned_work(owner, deadline)
            .await?;
        let current = client.cloud_workspace(pair.old.header.workspace).await?;
        if current.generation != pair.old.header.expected_generation {
            return Ok(None);
        }
        Ok(Some(candidate))
    }
    async fn prove_covered_unpublished(
        self: &Arc<Self>,
        old: &PendingCapture,
        newer: &PendingCapture,
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
        progress: Arc<Progress>,
    ) -> Result<RetirementCheck> {
        progress.at("successor_semantic");
        let coverage_owner: Owner = admission.clone();
        let Some((pair, owner, source)) = self.equivalent_saved_pair(old, newer, admission).await?
        else {
            return Ok(RetirementCheck::NotCovered);
        };
        let Some(published) = pair.newer_published else {
            return Ok(RetirementCheck::NotCovered);
        };
        progress.at("old_proof");
        let client = source
            .control
            .client_for_owned_work(owner.clone(), deadline)
            .await?;
        let selected = client
            .cloud_retained_version_selected(
                RetainedVersionSelector {
                    workspace_id: pair.newer.header.workspace,
                    generation: pair
                        .newer
                        .header
                        .expected_generation
                        .checked_add(1)
                        .context("retained successor generation overflow")?,
                    manifest_id: CloudManifestId(pair.newer.header.manifest_id),
                },
                CloudRawDigest::of(&pair.newer.manifest),
            )
            .await?;
        ensure!(
            selected.manifest() == pair.newer.manifest,
            "retained successor manifest differs"
        );
        let captured = client.cloud_captured_version(selected)?;
        let attachment = client.cloud_read_capture(&captured).await?;
        ensure!(
            attachment.payload() == pair.newer.capture && attachment.payload() == pair.old.capture,
            "retained successor DATA differs"
        );
        progress.at("descriptor_coverage");
        let worker = self.clone();
        let old_ticket = old.clone();
        let newer_ticket = newer.clone();
        let physical = owner.clone();
        if !blocking_owned(owner.clone(), move || {
            worker.descriptor_coverage(&old_ticket, &newer_ticket, physical)
        })
        .await?
        .value
        {
            return Ok(RetirementCheck::NotCovered);
        }
        progress.at("canonical_coverage");
        let Some(objects) = self
            .canonical_coverage(
                source.local.clone(),
                old,
                newer,
                pair.snapshot,
                owner,
                deadline,
            )
            .await?
        else {
            return Ok(RetirementCheck::CanonicalUnproved);
        };
        let attestation = native_retention::CoveredByRetainedSuccessorV1 {
            successor_binding: newer.binding(),
            provider: pair.newer.header.provider,
            cloud_workspace: pair.newer.header.workspace,
            generation: pair
                .newer
                .header
                .expected_generation
                .checked_add(1)
                .context("retained successor generation overflow")?,
            manifest_id: pair.newer.header.manifest_id,
            manifest: raw_descriptor(&pair.newer.manifest),
            capture: raw_descriptor(&pair.newer.capture),
            witness: native_retention::RetainedSuccessorWitness {
                job: pair.newer_job,
                published,
            },
        };
        Ok(RetirementCheck::Ready(RetirementProof {
            upload: native_retention::CaptureUploadCleanup {
                job: Some(pair.old_job),
                published: None,
            },
            basis: Some(
                native_retention::CaptureCleanupBasis::CoveredByRetainedSuccessorV1(attestation),
            ),
            retained: None,
            coverage: native_retention::OwnedCaptureCoverage::new(objects, coverage_owner),
        }))
    }
    async fn reconcile_retained_sources(
        self: &Arc<Self>,
        admission: Arc<Admission>,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        let worker = self.clone();
        tokio::spawn(async move {
            worker
                .reconcile_retained_sources_owned(admission, deadline)
                .await
        })
        .await?
    }
    async fn reconcile_retained_sources_owned(
        &self,
        admission: Arc<Admission>,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        let bindings = match self
            .host
            .capture_registry()
            .offload_protected_bindings()
            .await
        {
            Ok(bindings) => bindings,
            Err(native_retention::Error::Busy) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        // poll owns the sole uploader admission; new local captures do not
        // independently create provider pins. The registry fence is gone here.
        // This releases only demonstrably orphaned cleanup PIN rows, never
        // source authorities or backing. This local pass needs no network
        // capability probe. Durable source obligations are reconciled afterward
        // with the original deadline and physical uploader admission owner.
        for source in self.sources.values() {
            source
                .control
                .reconcile_retained_offload_pins(bindings.clone())
                .await?;
        }
        for source in self.sources.values() {
            ensure!(
                tokio::time::Instant::now() < deadline,
                "retained source maintenance deadline exceeded"
            );
            source
                .control
                .reconcile_retained_source_pins_until(
                    Arc::new((source.clone(), admission.clone())),
                    deadline,
                )
                .await?;
        }
        Ok(())
    }
    async fn verify_published_ticket(
        self: &Arc<Self>,
        ticket: PendingCapture,
        admission: Arc<Admission>,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        let registry = self.host.capture_registry();
        let id = ticket.id;
        let acquired = admission.clone();
        let read = tokio::spawn(async move {
            OwnedValue {
                value: registry.read_capture(id).await,
                _owner: acquired as Owner,
            }
        })
        .await?;
        let read = Arc::new(read.value?);
        ensure!(
            read.pending() == &ticket,
            "historical capture ticket changed"
        );
        let owner: Owner = Arc::new(JobOwner {
            _read: read,
            _admission: admission,
        });
        ensure!(
            tokio::time::Instant::now() < deadline,
            "capture publication deadline exceeded"
        );
        let worker = self.clone();
        blocking_owned(owner, move || {
            #[cfg(test)]
            if let Some(pause) = worker._hooks.published_pause.lock().unwrap().take() {
                pause.block();
            }
            let header: Header = serde_json::from_slice(
                &worker
                    .directory
                    .read(&format!("{}.published", hex(&ticket.id.0)), 4096)?,
            )?;
            ensure!(
                header.valid()
                    && header.ticket == ticket.id.0
                    && header.ticket_binding == ticket_binding(&ticket),
                "historical capture record differs from pending ticket"
            );
            let (original, _) = worker.original_envelope(&ticket.id.0)?;
            let attempts = worker.saved_attempts(&ticket.id.0)?;
            ensure!(
                header == *attempts.last().unwrap_or(&original),
                "historical capture record differs from exact attempt journal"
            );
            Ok(())
        })
        .await?;
        Ok(())
    }
    async fn poll(self: &Arc<Self>) -> Result<bool> {
        let mut progressed = false;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(self.policy.timeout_ms);
        let admission = Arc::new(Admission {
            _permit: tokio::time::timeout_at(deadline, self.admission.clone().acquire_owned())
                .await??,
        });
        let worker = self.clone();
        let physical = admission.clone();
        let inventory_result = tokio::time::timeout_at(
            deadline,
            tokio::task::spawn_blocking(move || {
                let _owner = physical;
                #[cfg(test)]
                if let Some(pause) = worker._hooks.catalog_pause.lock().unwrap().take() {
                    pause.block();
                }
                worker.inventory()
            }),
        )
        .await;
        // Preserve the nested join/IO error exactly; attach diagnostics only.
        let inventory_result =
            inventory_result.map(|v| v.map_err(anyhow::Error::from).and_then(|v| v));
        if !matches!(&inventory_result, Ok(Ok(_))) {
            self.stats
                .outcome(false, &Progress::new("inventory"), &inventory_result);
        }
        let inventory = inventory_result??;
        self.stats.published.store(inventory.2, Ordering::Relaxed);
        let mut tickets = self.host.capture_registry().pending_captures().await?;
        tickets.sort_by_key(|ticket| (ticket.registry_epoch, ticket.id));
        ensure!(
            tickets.len() <= self.policy.max_jobs,
            "capture pending upload job limit exceeded"
        );
        if let Some(last) = *self
            .next_ticket
            .lock()
            .map_err(|_| anyhow::anyhow!("capture cursor unavailable"))?
        {
            let split =
                tickets.partition_point(|ticket| (ticket.registry_epoch, ticket.id) <= last);
            tickets.rotate_left(split);
        }
        for ticket in tickets.iter().cloned() {
            if self.policy.policy == NativeCaptureUploadPolicy::Auto
                && !self.host.capture_auto_upload(&ticket)?
            {
                continue;
            }
            let published = format!("{}.published", hex(&ticket.id.0));
            if self.directory.path().join(&published).try_exists()? {
                ensure!(
                    tokio::time::Instant::now() < deadline,
                    "capture publication deadline exceeded"
                );
                tokio::time::timeout_at(
                    deadline,
                    self.verify_published_ticket(ticket, admission.clone(), deadline),
                )
                .await??;
                continue;
            }
            *self
                .next_ticket
                .lock()
                .map_err(|_| anyhow::anyhow!("capture cursor unavailable"))? =
                Some((ticket.registry_epoch, ticket.id));
            self.stats.attempts.fetch_add(1, Ordering::Relaxed);
            let worker = self.clone();
            let progress = Progress::new("routing");
            let result = tokio::time::timeout_at(deadline, async {
                progress.at("saved_equivalence");
                let preferred = worker
                    .prefer_equivalent_saved_ticket(&ticket, &tickets, deadline, admission.clone())
                    .await
                    // A corrupt or over-budget optional candidate must not
                    // starve a valid selected job. The ordinary path verifies
                    // the selected ticket independently under this same
                    // admission and original absolute deadline.
                    .unwrap_or(None);
                worker
                    .process(
                        preferred.unwrap_or(ticket),
                        deadline,
                        admission.clone(),
                        progress.clone(),
                    )
                    .await
            })
            .await;
            self.stats.outcome(false, &progress, &result);
            match result {
                Ok(Ok(())) => {
                    progressed = true;
                    self.stats.published.fetch_add(1, Ordering::Relaxed);
                    self.stats.set("published_roots_retained");
                }
                _ => {
                    self.stats.failed.fetch_add(1, Ordering::Relaxed);
                    self.stats.set("retry_or_reconcile");
                }
            }
            // One upload and bounded retirement work share this absolute deadline.
            break;
        }
        // Reconciliation/replay must not consume the deadline before one queued
        // immutable backup gets its turn, even on an unavailable other source.
        let reconciliation_progress = Progress::new("retained_pin_reconciliation");
        let reconciliation =
            tokio::time::timeout_at(deadline, self.reconcile_retained_sources(admission.clone(), deadline))
                .await;
        if !matches!(&reconciliation, Ok(Ok(()))) {
            self.stats
                .outcome(true, &reconciliation_progress, &reconciliation);
            self.stats.failed.fetch_add(1, Ordering::Relaxed);
            self.stats.set("retirement_retry_preserved");
            // Preserve all pins/cleanup and perform NO destructive work after a
            // refusal. The successful publication remains observable progress.
            return Ok(progressed);
        }
        // Publish pending work BEFORE proving retirement of old versions. A
        // missing/unavailable old proof must retain it, not starve new backups.
        if matches!(
            self.policy.policy,
            NativeCaptureUploadPolicy::Auto | NativeCaptureUploadPolicy::PublishRetiringRedundant
        ) {
            let progress = Progress::new("old_proof");
            let result = tokio::time::timeout_at(
                deadline,
                self.retire_one(deadline, admission.clone(), progress.clone()),
            )
            .await;
            self.stats.outcome(true, &progress, &result);
            match result {
                Ok(Ok(RetirementAttempt::Changed)) => progressed = true,
                Ok(Ok(RetirementAttempt::Unchanged)) => {}
                Ok(Ok(RetirementAttempt::CanonicalUnproved)) => {
                    self.stats.set("local_capture_canonical_coverage_unproved");
                    if let Ok(mut outcome) = self.stats.last_retirement.lock() {
                        *outcome = Some(Outcome {
                            stage: "canonical_coverage",
                            result: "not_redundant",
                            error_class: "none",
                            error_code: "none",
                            native_stage: None,
                            native_reason: None,
                            publication_reason: None,
                        });
                    }
                }
                _ => {
                    self.stats.failed.fetch_add(1, Ordering::Relaxed);
                    self.stats.set("retirement_retry_preserved");
                }
            }
        }
        Ok(progressed)
    }
    #[cfg(test)]
    async fn complete_cleanup(self: &Arc<Self>, admission: Arc<Admission>) -> Result<bool> {
        self.complete_cleanup_traced(admission, Progress::new("cleanup"))
            .await
    }
    async fn complete_cleanup_traced(
        self: &Arc<Self>,
        admission: Arc<Admission>,
        progress: Arc<Progress>,
    ) -> Result<bool> {
        self.stats.invalidate_availability();
        let worker = self.clone();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(self.policy.timeout_ms);
        // Accepted cleanup owns both fences and admission after waiter expiry.
        tokio::spawn(async move { worker.cleanup_owned(admission, progress, deadline).await })
            .await?
    }
    async fn retained_offload_contract(
        &self,
        source: &Source,
        owner: Owner,
        deadline: tokio::time::Instant,
    ) -> Result<Option<jj_native_remote::DurableRetainedOffloadContract>> {
        // Legacy fixture workflows explicitly model a peer without this new
        // contract. Enabling the fixture never bypasses real SDK negotiation.
        #[cfg(test)]
        if !self._hooks.retained_offload.load(Ordering::Acquire) {
            return Ok(None);
        }
        // No cache: authorization, service configuration and endpoint state may
        // change. Use the original provider/client, owner and absolute deadline,
        // before any native registry maintenance or capture pause.
        let client = source
            .control
            .client_for_owned_work(owner, deadline)
            .await?;
        let capabilities = match client
            .capabilities(&[tinytree_client::CapabilityFeature::DurableCloudVersionPinV1])
            .await
        {
            Ok(capabilities) => capabilities,
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::InvalidArgument | tonic::Code::Unimplemented
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let capability = capabilities.features[0];
        ensure!(
            capability.authorized,
            tonic::Status::permission_denied("durable retained source pins are not authorized")
        );
        if !capability.supported {
            return Ok(None);
        }
        Ok(Some(
            jj_native_remote::DurableRetainedOffloadContract::ExplicitlyVerifiedV1,
        ))
    }
    async fn retained_cleanup_owner(
        self: &Arc<Self>,
        obligation: &native_retention::CaptureCleanupObligation,
        admission: Arc<Admission>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<(Arc<Source>, Arc<jj_native_remote::PinnedRetainedVersion>)>> {
        let Some(native_retention::CaptureCleanupBasis::RetainedOldCaptureV1(proof)) =
            &obligation.basis
        else {
            return Ok(None);
        };
        // Resolve through the still-pending successor's original registration,
        // not an arbitrary source with an equal repository identity.
        let read = self
            .host
            .capture_registry()
            .read_capture(obligation.successor.id)
            .await?;
        let (_, alias, identity) = self.host.capture_mapping(read.pending())?;
        drop(read);
        let source = self
            .sources
            .get(&alias)
            .context("retained cleanup provider unavailable")?
            .clone();
        ensure!(
            source.identity == identity && identity == proof.provider,
            "retained cleanup original provider changed"
        );
        let contract = self
            .retained_offload_contract(&source, admission.clone(), deadline)
            .await?
            .context("durable retained offload contract unavailable")?;
        let client = source
            .control
            .client_for_owned_work(admission.clone(), deadline)
            .await?;
        let selected = client
            .cloud_retained_version_selected(
                RetainedVersionSelector {
                    workspace_id: proof.cloud_workspace,
                    generation: proof.generation,
                    manifest_id: CloudManifestId(proof.manifest_id),
                },
                CloudRawDigest {
                    blake3: proof.manifest.blake3,
                    size_bytes: proof.manifest.size_bytes,
                },
            )
            .await?;
        let captured = client.cloud_captured_version(selected)?;
        let data = client.cloud_read_capture(&captured).await?;
        ensure!(
            raw_descriptor(data.payload()) == proof.capture,
            "retained cleanup capture DATA changed"
        );
        let witness = remote_retained_witness(proof);
        let owner = source
            .control
            .resume_retained_offload_until(contract, witness.clone(), admission, deadline)
            .await?;
        owner.validate(&witness)?;
        Ok(Some((source, Arc::new(owner))))
    }
    async fn cleanup_owned(
        self: Arc<Self>,
        admission: Arc<Admission>,
        progress: Arc<Progress>,
        deadline: tokio::time::Instant,
    ) -> Result<bool> {
        progress.at("cleanup");
        let registry = self.host.capture_registry();
        let pending = registry.capture_cleanup().await?;
        self.stats
            .cleanup_pending
            .store(pending.is_some(), Ordering::Release);
        if pending.is_none() {
            return Ok(false);
        }
        self.stats.invalidate_availability();
        let retained = self
            .retained_cleanup_owner(pending.as_ref().unwrap(), admission.clone(), deadline)
            .await?;
        // Capture callbacks acquire this permit before taking registry read
        // fences. Preserve that order, otherwise cleanup could deadlock a writer.
        let paused = self.host.pause_capture_cleanup().await?;
        let Some(guard) = registry.try_capture_cleanup().await? else {
            self.stats.cleanup_pending.store(false, Ordering::Release);
            return Ok(false);
        };
        let worker = self.clone();
        let physical = paused.clone();
        let protected = retained.as_ref().map(|(_, owner)| owner.clone());
        let completed = blocking_owned(admission as Owner, move || {
            worker.unlink_retired(guard.obligation(), protected.as_ref())?;
            physical.reconcile_disk()?;
            let published = worker.inventory()?.2;
            Ok((guard, published))
        })
        .await?;
        let (guard, published) = completed.value;
        match guard.obligation().basis.clone() {
            Some(native_retention::CaptureCleanupBasis::CoveredByRetainedSuccessorV1(proof)) => {
                guard
                    .acknowledge_capture_cleanup_with_successor_witness(proof.witness)
                    .await?;
            }
            Some(native_retention::CaptureCleanupBasis::RetainedOldCaptureV1(proof)) => {
                let (_, owner) = retained.as_ref().context("retained cleanup owner absent")?;
                owner.validate(&remote_retained_witness(&proof))?;
                guard
                    .acknowledge_capture_cleanup_with_retained_source(
                        proof.source,
                        owner.physical_owner(),
                    )
                    .await?;
            }
            None => guard.acknowledge_capture_cleanup().await?,
        }
        self.stats.published.store(published, Ordering::Relaxed);
        // The capture permit remains held until real disk and registry counts
        // agree. Never uncharge on a logical ticket removal or upload ACK alone.
        paused.refresh(&registry).await;
        self.stats.retired.fetch_add(1, Ordering::Relaxed);
        self.stats.cleanup_pending.store(false, Ordering::Release);
        self.stats.set(if retained.is_some() {
            "retired_retained_local_capture"
        } else {
            "retired_redundant_local_capture"
        });
        drop(paused); // Never wait on provider bookkeeping under capture maintenance.
        if let Some((source, owner)) = retained {
            let witness = owner.witness();
            drop(owner);
            // ACK already committed. Failure leaves a conservative durable pin;
            // it cannot undo retirement or fabricate further cleanup success.
            source.control.release_retained_offload_pin(witness).await?;
        }
        Ok(true)
    }
    fn unlink_retired(
        &self,
        obligation: &native_retention::CaptureCleanupObligation,
        retained: Option<&Arc<jj_native_remote::PinnedRetainedVersion>>,
    ) -> Result<()> {
        #[cfg(test)]
        if let Some(pause) = self._hooks.cleanup_pause.lock().unwrap().take() {
            pause.block();
        }
        let captures =
            PrivateDir::open_existing(self.config.state_dir.join("native-commands/captures"))?;
        let name = hex(&obligation.retired.id.0);
        let mut files = Vec::new();
        let job = format!("{name}.job");
        match obligation.upload.job {
            Some(expected) => files.push(validate_cleanup_file(&self.directory, &job, expected)?),
            None => ensure!(
                std::fs::symlink_metadata(self.directory.path().join(&job))
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
                "unexpected retired capture job; preserve it"
            ),
        }
        // Protected successor witnesses are NEVER deletion targets. Retain
        // their inspected physical handles throughout old-file unlink/fsync.
        let _protected = match &obligation.basis {
            Some(native_retention::CaptureCleanupBasis::CoveredByRetainedSuccessorV1(proof)) => {
                ensure!(
                    obligation.upload.published.is_none(),
                    "covered old publication unexpectedly declared"
                );
                validate_covered_journals(
                    &self.directory,
                    obligation.retired.id,
                    obligation.successor.id,
                    obligation
                        .upload
                        .job
                        .context("covered old job descriptor absent")?,
                    &proof.witness,
                    false,
                )?
            }
            Some(native_retention::CaptureCleanupBasis::RetainedOldCaptureV1(proof)) => {
                let owner = retained.context("retained cleanup physical owner missing")?;
                owner.validate(&remote_retained_witness(proof))?;
                files.push(validate_cleanup_file(
                    &self.directory,
                    &format!("{name}.published"),
                    obligation
                        .upload
                        .published
                        .context("retained cleanup publication descriptor missing")?,
                )?);
                Vec::new()
            }
            None => {
                files.push(validate_cleanup_file(
                    &self.directory,
                    &format!("{name}.published"),
                    obligation
                        .upload
                        .published
                        .context("published cleanup descriptor absent")?,
                )?);
                Vec::new()
            }
        };
        let mut successor_cleanup = false;
        if let Some(expected) = obligation.upload.published {
            if let Some(publication) =
                validate_cleanup_file(&self.directory, &format!("{name}.published"), expected)?
            {
                let bytes = self.directory.read(&format!("{name}.published"), 4096)?;
                successor_cleanup = serde_json::from_slice::<Header>(&bytes)?
                    .successor
                    .is_some();
                files.extend(validate_attempt_cleanup(&self.directory, &bytes)?);
                drop(publication);
            } else {
                // Publication is unlinked last, after every attempt. Its absence
                // can only reconcile an already completed exact cleanup prefix.
                for ordinal in 1..=MAX_SUCCESSORS {
                    ensure!(
                        path_absent(
                            &self.directory,
                            &attempt_name(&obligation.retired.id.0, ordinal)
                        )?,
                        "retired attempt remains without publication witness"
                    );
                }
            }
        } else {
            for ordinal in 1..=MAX_SUCCESSORS {
                ensure!(
                    path_absent(
                        &self.directory,
                        &attempt_name(&obligation.retired.id.0, ordinal)
                    )?,
                    "unpublished retirement cannot discard attempt journal"
                );
            }
        }
        if obligation.remove_bundle {
            files.push(validate_cleanup_file(
                &captures,
                &format!("{}.pack", hex(&obligation.retired.bundle.blake3)),
                obligation.retired.bundle,
            )?);
        }
        // Validate ALL present files before removing any. Missing known files
        // are the durable commit/unlink crash prefix, never discovery authority.
        let mut files: Vec<_> = files.into_iter().flatten().collect();
        if successor_cleanup {
            files.sort_by_key(|item| {
                item.path
                    .extension()
                    .is_some_and(|suffix| suffix == "published")
            });
        }
        for item in files {
            // Keep the marker as the discovery anchor until all sidecars are
            // durably gone; a crash must never orphan an unauthenticated chain.
            if successor_cleanup
                && item
                    .path
                    .extension()
                    .is_some_and(|suffix| suffix == "published")
            {
                self.sync_namespace()?;
            }
            item.remove(&self._hooks)?;
        }
        self.sync_namespace()?;
        File::open(captures.path())?.sync_all()?;
        Ok(())
    }
    async fn retire_one(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
        progress: Arc<Progress>,
    ) -> Result<RetirementAttempt> {
        let worker = self.clone();
        tokio::spawn(async move { worker.retire_one_owned(deadline, admission, progress).await })
            .await?
    }
    async fn retire_one_owned(
        self: Arc<Self>,
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
        progress: Arc<Progress>,
    ) -> Result<RetirementAttempt> {
        let registry = self.host.capture_registry();
        if registry.capture_cleanup().await?.is_some() {
            return self
                .cleanup_owned(admission, progress, deadline)
                .await
                .map(RetirementAttempt::from_changed);
        }
        let mut tickets = registry.pending_captures().await?;
        ensure!(
            tickets.len() <= self.policy.max_jobs,
            "capture retirement inventory limit"
        );
        if self.policy.policy == NativeCaptureUploadPolicy::Auto {
            let mut enrolled = Vec::with_capacity(tickets.len());
            for ticket in tickets {
                if self.host.capture_auto_upload(&ticket)? {
                    enrolled.push(ticket);
                }
            }
            tickets = enrolled;
        }
        tickets.sort_by_key(|ticket| (ticket.registry_epoch, ticket.id));
        let mut candidates = tickets.clone();
        if let Some(last) = *self
            .next_retirement
            .lock()
            .map_err(|_| anyhow::anyhow!("capture retirement cursor unavailable"))?
        {
            let split =
                candidates.partition_point(|ticket| (ticket.registry_epoch, ticket.id) <= last);
            candidates.rotate_left(split);
        }
        // A rolling discovery window can expire ancient proofs while queued
        // local capsules remain. Use the freshest published proof first rather
        // than allowing an unreadable historical capsule to block useful GC.
        // This changes scheduling only: exact source and canonical coverage are
        // still proved below, and the newest capture can never be a candidate.
        let worker = self.clone();
        let inventory = candidates.clone();
        let generations = blocking_owned(admission.clone() as Owner, move || {
            let mut generations = BTreeMap::new();
            for ticket in inventory {
                let name = format!("{}.published", hex(&ticket.id.0));
                if !path_absent(&worker.directory, &name)? {
                    let header: Header =
                        serde_json::from_slice(&worker.read_bounded_durable(&name, 4096)?)?;
                    ensure!(
                        header.valid()
                            && header.ticket == ticket.id.0
                            && header.ticket_binding == ticket.binding(),
                        "retirement scheduling publication differs"
                    );
                    generations.insert(ticket.id, header.expected_generation);
                }
            }
            Ok(generations)
        })
        .await?
        .value;
        candidates.sort_by_key(|ticket| std::cmp::Reverse(generations.get(&ticket.id).copied()));
        for old in candidates {
            let Some(newer) = tickets
                .iter()
                .rev()
                .find(|newer| {
                    newer.registration == old.registration
                        && newer.registry == old.registry
                        && newer.registry_epoch > old.registry_epoch
                })
                .cloned()
            else {
                continue;
            };
            let published_name = format!("{}.published", hex(&old.id.0));
            let old_published = !path_absent(&self.directory, &published_name)?;
            if !old_published
                && (path_absent(&self.directory, &format!("{}.job", hex(&old.id.0)))?
                    || path_absent(&self.directory, &format!("{}.job", hex(&newer.id.0)))?
                    || path_absent(&self.directory, &format!("{}.published", hex(&newer.id.0)))?)
            {
                continue;
            }
            *self
                .next_retirement
                .lock()
                .map_err(|_| anyhow::anyhow!("capture retirement cursor unavailable"))? =
                Some((old.registry_epoch, old.id));
            let check = if old_published {
                self.prove_redundant(&old, &newer, deadline, admission.clone(), progress.clone())
                    .await?
            } else {
                self.prove_covered_unpublished(
                    &old,
                    &newer,
                    deadline,
                    admission.clone(),
                    progress.clone(),
                )
                .await?
            };
            let check = if old_published && !matches!(&check, RetirementCheck::Ready(_)) {
                match self
                    .prove_retained_old(&old, &newer, deadline, admission.clone(), progress.clone())
                    .await?
                {
                    Some(retained) => retained,
                    None => check,
                }
            } else {
                check
            };
            let mut proof = match check {
                RetirementCheck::Ready(proof) => proof,
                RetirementCheck::NotCovered => {
                    self.stats.set("local_capture_not_covered");
                    return Ok(RetirementAttempt::Unchanged);
                }
                RetirementCheck::CanonicalUnproved => {
                    return Ok(RetirementAttempt::CanonicalUnproved);
                }
            };
            ensure!(
                tokio::time::Instant::now() < deadline,
                "capture retirement deadline exceeded"
            );
            // All read guards, verified packs, payloads and SDK owners were
            // dropped in prove_redundant before taking this exclusive fence.
            self.stats.invalidate_availability();
            progress.at("exclusive_retirement");
            #[cfg(test)]
            {
                let pause = self._hooks.retirement_pause.lock().unwrap().take();
                if let Some(pause) = pause {
                    blocking_owned(admission.clone() as Owner, move || {
                        pause.block();
                        Ok(())
                    })
                    .await?;
                }
            }
            if let Some(native_retention::CaptureCleanupBasis::RetainedOldCaptureV1(attestation)) =
                proof.basis
            {
                let protected = proof
                    .retained
                    .take()
                    .context("retained retirement owner absent")?;
                let remote = remote_retained_witness(&attestation);
                let directory = self.directory.clone();
                let old_id = old.id;
                let upload = proof.upload.clone();
                let owner = protected.physical_owner();
                registry
                    .retire_capture_backed_by_retained_version(
                        old,
                        newer,
                        proof.upload,
                        attestation,
                        owner,
                        move || {
                            if tokio::time::Instant::now() >= deadline {
                                return Err(native_retention::Error::DeadlineExceeded);
                            }
                            protected.validate(&remote).map_err(|_| {
                                native_retention::Error::Invalid(
                                    "retained provider admission changed",
                                )
                            })?;
                            validate_retained_journals(&directory, old_id, &upload, true).map_err(
                                |_| {
                                    native_retention::Error::Invalid("retained old journal changed")
                                },
                            )
                        },
                    )
                    .await?;
            } else if let Some(
                native_retention::CaptureCleanupBasis::CoveredByRetainedSuccessorV1(attestation),
            ) = proof.basis
            {
                let directory = self.directory.clone();
                let old_id = old.id;
                let newer_id = newer.id;
                let old_job = proof.upload.job.context("covered capture old job absent")?;
                let witness = attestation.witness.clone();
                registry
                    .retire_capture_covered_by_retained_successor(
                        old,
                        newer,
                        proof.upload,
                        attestation,
                        proof.coverage,
                        move || {
                            if tokio::time::Instant::now() >= deadline {
                                return Err(native_retention::Error::Invalid(
                                    "covered capture precommit deadline",
                                ));
                            }
                            validate_covered_journals(
                                &directory, old_id, newer_id, old_job, &witness, true,
                            )
                            .map(|_| ())
                            .map_err(|_| {
                                native_retention::Error::Invalid(
                                    "covered capture local witness changed",
                                )
                            })
                        },
                    )
                    .await?;
            } else {
                registry
                    .retire_redundant_capture_with_coverage(
                        old,
                        newer,
                        proof.upload,
                        proof.coverage,
                    )
                    .await?;
            }
            self.stats.cleanup_pending.store(true, Ordering::Release);
            return self
                .cleanup_owned(admission, progress, deadline)
                .await
                .map(RetirementAttempt::from_changed);
        }
        Ok(RetirementAttempt::Unchanged)
    }
    async fn prove_redundant(
        self: &Arc<Self>,
        old: &PendingCapture,
        newer: &PendingCapture,
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
        progress: Arc<Progress>,
    ) -> Result<RetirementCheck> {
        progress.at("old_proof");
        let (mount, alias, identity) = self.host.capture_mapping(old)?;
        ensure!(
            self.host.capture_mapping(newer)? == (mount.clone(), alias.clone(), identity),
            "capture successor provider differs"
        );
        let source = self
            .sources
            .get(&alias)
            .context("capture retirement provider unavailable")?;
        ensure!(
            source.identity == identity,
            "capture retirement provider changed"
        );
        let workspace = crate::native_workspaces::capture_upload_workspace(
            &self.config.state_dir,
            &mount,
            &alias,
            &identity,
        )?;
        let registry = self.host.capture_registry();
        let old_read = Arc::new(registry.read_capture(old.id).await?);
        let newer_read = Arc::new(registry.read_capture(newer.id).await?);
        ensure!(
            old_read.pending() == old && newer_read.pending() == newer,
            "capture retirement ticket changed"
        );
        let coverage_owner: Owner = admission.clone();
        let owner: Owner = Arc::new(RetirementOwner {
            _old: old_read,
            _newer: newer_read,
            _admission: admission,
        });
        let worker = self.clone();
        let expected = old.clone();
        let intent = blocking_owned(owner.clone(), move || {
            let job = worker.read_durable(&format!("{}.job", hex(&expected.id.0)))?;
            let mut intent = Intent::decode(&job)?;
            let published = worker.read_durable(&format!("{}.published", hex(&expected.id.0)))?;
            worker.apply_published(&mut intent, &published)?;
            ensure!(
                serde_json::from_slice::<Header>(&published)? == intent.header,
                "retired publication record differs"
            );
            ensure!(
                intent.header.ticket_binding == expected.binding()
                    && intent.header.provider == identity
                    && intent.header.workspace == workspace,
                "retired capture publication binding differs"
            );
            Ok((
                intent,
                native_retention::CaptureUploadCleanup {
                    job: Some(raw_descriptor(&job)),
                    published: Some(raw_descriptor(&published)),
                },
            ))
        })
        .await?;
        let client = source
            .control
            .client_for_owned_work(owner.clone(), deadline)
            .await?;
        let selected = client
            .cloud_retained_version_selected(
                RetainedVersionSelector {
                    workspace_id: workspace,
                    generation: intent
                        .0
                        .header
                        .expected_generation
                        .checked_add(1)
                        .context("capture retirement generation overflow")?,
                    manifest_id: CloudManifestId(intent.0.header.manifest_id),
                },
                CloudRawDigest::of(&intent.0.manifest),
            )
            .await?;
        ensure!(
            selected.manifest() == intent.0.manifest,
            "retained retirement manifest differs"
        );
        let selected = client.cloud_captured_version(selected)?;
        let attachment = client.cloud_read_capture(&selected).await?;
        ensure!(
            attachment.payload() == intent.0.capture,
            "retained retirement attachment differs"
        );
        let worker = self.clone();
        let old_ticket = old.clone();
        let newer_ticket = newer.clone();
        let proof_owner = owner.clone();
        let physical_progress = progress.clone();
        let result = blocking_owned(owner.clone(), move || {
            let snapshot = worker.verify_saved(&old_ticket, &intent.0, proof_owner.clone())?;
            // Fully verify the successor's semantic OP/VIEW/index graph and its
            // checkpoint binding even if it has not yet reached the server.
            physical_progress.at("successor_semantic");
            let successor = worker.prepare(
                &newer_ticket,
                c::CaptureExportIdentity {
                    workspace,
                    exporter: newer_ticket.registration.workspace.0,
                    sequence: newer_ticket.registry_epoch,
                    parents: Vec::new(),
                },
                0,
                proof_owner.clone(),
            )?;
            drop(successor);
            physical_progress.at("descriptor_coverage");
            if !worker.descriptor_coverage(&old_ticket, &newer_ticket, proof_owner)? {
                return Ok(None);
            }
            Ok(Some((intent.1.clone(), snapshot)))
        })
        .await?;
        let Some((upload, snapshot)) = result.value else {
            return Ok(RetirementCheck::NotCovered);
        };
        ensure!(
            self.policy.max_memory_bytes
                >= native_retention::MAX_CAPTURE_COVERAGE_WORK_BYTES as u64,
            "capture coverage proof admission too small"
        );
        progress.at("canonical_coverage");
        let Some(objects) = self
            .canonical_coverage(
                source.local.clone(),
                old,
                newer,
                snapshot,
                owner.clone(),
                deadline,
            )
            .await?
        else {
            return Ok(RetirementCheck::CanonicalUnproved);
        };
        // Return immutable raw witness bytes with admission ONLY. All local
        // read fences, pack leases and SDK owners drop before exclusive retire.
        Ok(RetirementCheck::Ready(RetirementProof {
            upload,
            basis: None,
            retained: None,
            coverage: native_retention::OwnedCaptureCoverage::new(objects, coverage_owner),
        }))
    }
    async fn prove_retained_old(
        self: &Arc<Self>,
        old: &PendingCapture,
        newer: &PendingCapture,
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
        progress: Arc<Progress>,
    ) -> Result<Option<RetirementCheck>> {
        progress.at("retained_old_source");
        let (mount, alias, identity) = self.host.capture_mapping(old)?;
        ensure!(
            self.host.capture_mapping(newer)? == (mount.clone(), alias.clone(), identity),
            "retained successor provider differs"
        );
        let source = self
            .sources
            .get(&alias)
            .context("retained provider unavailable")?
            .clone();
        ensure!(
            source.identity == identity,
            "retained original provider changed"
        );
        let Some(contract) = self
            .retained_offload_contract(&source, admission.clone(), deadline)
            .await?
        else {
            return Ok(None);
        };
        let workspace = crate::native_workspaces::capture_upload_workspace(
            &self.config.state_dir,
            &mount,
            &alias,
            &identity,
        )?;
        let registry = self.host.capture_registry();
        let old_read = Arc::new(registry.read_capture(old.id).await?);
        let newer_read = Arc::new(registry.read_capture(newer.id).await?);
        ensure!(
            old_read.pending() == old && newer_read.pending() == newer,
            "retained ticket changed"
        );
        let owner: Owner = Arc::new(RetirementOwner {
            _old: old_read,
            _newer: newer_read,
            _admission: admission.clone(),
        });
        let worker = self.clone();
        let expected = old.clone();
        let successor = newer.clone();
        let physical = owner.clone();
        let verified = blocking_owned(owner.clone(), move || {
            let job = worker.read_durable(&format!("{}.job", hex(&expected.id.0)))?;
            let published = worker.read_durable(&format!("{}.published", hex(&expected.id.0)))?;
            let mut intent = Intent::decode(&job)?;
            worker.apply_published(&mut intent, &published)?;
            ensure!(
                serde_json::from_slice::<Header>(&published)? == intent.header,
                "retained old publication differs"
            );
            ensure!(
                intent.header.ticket_binding == expected.binding()
                    && intent.header.provider == identity
                    && intent.header.workspace == workspace,
                "retained old binding differs"
            );
            worker.verify_saved(&expected, &intent, physical.clone())?;
            // This validates the current ordering fence, not claimed coverage.
            drop(worker.prepare(
                &successor,
                c::CaptureExportIdentity {
                    workspace,
                    exporter: successor.registration.workspace.0,
                    sequence: successor.registry_epoch,
                    parents: Vec::new(),
                },
                0,
                physical,
            )?);
            Ok((
                intent,
                native_retention::CaptureUploadCleanup {
                    job: Some(raw_descriptor(&job)),
                    published: Some(raw_descriptor(&published)),
                },
            ))
        })
        .await?;
        let (intent, upload) = verified.value;
        let selector = RetainedVersionSelector {
            workspace_id: workspace,
            generation: intent
                .header
                .expected_generation
                .checked_add(1)
                .context("retained generation overflow")?,
            manifest_id: CloudManifestId(intent.header.manifest_id),
        };
        let selected = CloudRawDigest::of(&intent.manifest);
        let client = source
            .control
            .client_for_owned_work(owner.clone(), deadline)
            .await?;
        let version = client
            .cloud_retained_version_selected(selector, selected)
            .await?;
        ensure!(
            version.manifest() == intent.manifest,
            "retained old exact manifest differs"
        );
        let captured = client.cloud_captured_version(version)?;
        let data = client.cloud_read_capture(&captured).await?;
        ensure!(
            data.payload() == intent.capture,
            "retained old exact DATA differs"
        );
        let pinned = source
            .control
            .admit_retained_offload_until(
                contract,
                selector,
                selected,
                old.binding(),
                old.checkpoint.roots.clone(),
                owner,
                deadline,
            )
            .await?;
        let witness = pinned.witness();
        pinned.validate(&witness)?;
        let attestation = native_retention::RetainedOldCaptureV1 {
            retired_binding: old.binding(),
            provider: identity,
            cloud_workspace: workspace,
            generation: selector.generation,
            manifest_id: intent.header.manifest_id,
            manifest: raw_descriptor(&intent.manifest),
            capture: raw_descriptor(&intent.capture),
            source: native_retained_witness(&witness),
        };
        Ok(Some(RetirementCheck::Ready(RetirementProof {
            upload,
            basis: Some(native_retention::CaptureCleanupBasis::RetainedOldCaptureV1(
                attestation,
            )),
            retained: Some(Arc::new(pinned)),
            coverage: native_retention::OwnedCaptureCoverage::new(Vec::new(), admission),
        })))
    }
    async fn canonical_coverage(
        &self,
        local: Arc<jj_native_backend::DirectObjectIo>,
        old: &PendingCapture,
        newer: &PendingCapture,
        snapshot: [u8; 32],
        owner: Owner,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Vec<tinytree_model::EncodedObject>>> {
        use native_retention::{
            MAX_CAPTURE_COVERAGE_BYTES, MAX_CAPTURE_COVERAGE_EDGES, MAX_CAPTURE_COVERAGE_OBJECTS,
        };
        use tinytree_model as m;
        let typed = |key: &m::ObjectKey| (key.kind as u8, key.id.0);
        let mut reachable: BTreeSet<_> = newer.checkpoint.roots.iter().map(typed).collect();
        let mut missing: BTreeSet<_> = old
            .checkpoint
            .roots
            .iter()
            .map(typed)
            .filter(|key| !reachable.contains(key))
            .collect();
        if missing.is_empty() {
            return Ok(Some(Vec::new()));
        }
        // Most transitions replace the direct current-WC TREE root. The old WC
        // COMMIT remains a successor root through retained operation history;
        // try that single canonical edge before any bounded general traversal.
        let preferred = m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(snapshot),
        };
        let mut pending = VecDeque::new();
        if reachable.contains(&typed(&preferred)) {
            pending.push_back(preferred);
        }
        pending.extend(
            newer
                .checkpoint
                .roots
                .iter()
                .copied()
                .filter(|key| *key != preferred),
        );
        let mut read = BTreeSet::new();
        let mut objects = Vec::new();
        let (mut bytes, mut edges, mut attempts) = (0usize, 0usize, 0usize);
        while !missing.is_empty() {
            ensure!(
                tokio::time::Instant::now() < deadline,
                "capture coverage deadline exceeded"
            );
            let Some(key) = pending.pop_front() else {
                return Ok(None);
            };
            if key.kind == m::Kind::File
                || key.kind == m::Kind::Symlink
                || (key.kind == m::Kind::Commit && key.id.0 == [0; 32])
                || !read.insert(typed(&key))
            {
                continue;
            }
            attempts += 1;
            ensure!(
                attempts <= MAX_CAPTURE_COVERAGE_OBJECTS,
                "capture coverage object work limit"
            );
            let descriptor =
                tokio::time::timeout_at(deadline, local.descriptor_with_owner(key, owner.clone()))
                    .await??;
            let Some(descriptor) = descriptor else {
                // Thin public metadata may be absent locally. Do not hydrate
                // it in the background merely to make retirement succeed.
                continue;
            };
            ensure!(
                descriptor.key == key && descriptor.format_version == m::FORMAT_VERSION,
                "capture coverage metadata descriptor differs"
            );
            let size =
                usize::try_from(descriptor.size_bytes).context("capture coverage object size")?;
            let total = bytes
                .checked_add(size)
                .context("capture coverage byte work overflow")?;
            ensure!(
                total <= MAX_CAPTURE_COVERAGE_BYTES,
                "capture coverage byte work limit"
            );
            let metadata = tokio::time::timeout_at(
                deadline,
                local.read_metadata_with_owner(key, owner.clone()),
            )
            .await??;
            ensure!(
                metadata.descriptor() == descriptor && metadata.bytes().len() == size,
                "capture coverage canonical object differs"
            );
            let encoded = m::EncodedObject {
                descriptor,
                bytes: metadata.bytes().to_vec(),
            };
            let object = metadata.into_object();
            let references = object.references();
            edges = edges
                .checked_add(references.len())
                .context("capture coverage edge work overflow")?;
            ensure!(
                edges <= MAX_CAPTURE_COVERAGE_EDGES,
                "capture coverage edge work limit"
            );
            for reference in references {
                let reference_id = typed(&reference);
                missing.remove(&reference_id);
                if reachable.insert(reference_id) {
                    pending.push_back(reference);
                }
            }
            bytes = total;
            objects.push(encoded);
        }
        Ok(Some(objects))
    }
    fn descriptor_coverage(
        &self,
        old: &PendingCapture,
        newer: &PendingCapture,
        owner: Owner,
    ) -> Result<bool> {
        let directory = Arc::new(PrivateDir::open_existing(
            self.config.state_dir.join("native-commands/captures"),
        )?);
        let open = |ticket: &PendingCapture| -> Result<VerifiedPack> {
            Ok(VerifiedPack::open(
                directory.clone(),
                ticket.bundle.blake3,
                ticket.bundle.size_bytes,
                PackLimits {
                    max_pack_bytes: self.policy.max_bundle_bytes,
                    max_scan_bytes: self.policy.max_bundle_bytes,
                    ..Default::default()
                },
                owner.clone(),
            )?)
        };
        let newer = open(newer)?;
        let descriptors: BTreeSet<_> = newer
            .portable_entries()
            .filter_map(|lease| {
                let entry = lease.descriptor();
                (entry.kind <= 4).then(|| (entry.kind, entry.id.clone(), entry.blake3, entry.size))
            })
            .collect();
        let old = open(old)?;
        Ok(old
            .portable_entries()
            .filter(|lease| lease.descriptor().kind <= 4)
            .all(|lease| {
                let entry = lease.descriptor();
                descriptors.contains(&(entry.kind, entry.id.clone(), entry.blake3, entry.size))
            }))
    }
    fn ancestry_manifest(
        &self,
        original: &[u8],
        capture_bytes: usize,
        parent: [u8; 32],
    ) -> Result<Vec<u8>> {
        // Reuse the job's sequential decoder allowance, not a new pool. Raw
        // original/derived/SDK copies coexist outside this codec scratch.
        const SCRATCH: u64 = 1024 * 1024 * 1024;
        let retained = original
            .len()
            .checked_add(capture_bytes)
            .and_then(|n| n.checked_mul(4))
            .context("ancestry retained bytes overflow")? as u64;
        ensure!(
            retained
                .checked_add(SUCCESSOR_MEMORY)
                .and_then(|n| n.checked_add(SCRATCH))
                .is_some_and(|n| n <= self.policy.max_memory_bytes),
            "capture ancestry materialization memory unavailable"
        );
        let mut limits = c::ManifestV3Limits::default();
        limits.history.recovery.max_decode_bytes = limits
            .history
            .recovery
            .max_decode_bytes
            .min(SCRATCH as usize);
        limits.history.artifacts.max_decode_bytes = limits
            .history
            .artifacts
            .max_decode_bytes
            .min(SCRATCH as usize);
        let mut decoded = c::decode_manifest_v3(original, c::ManifestId::of(original), limits)?;
        decoded.history.recovery.parents = vec![c::ManifestId(parent)];
        let bytes = c::encode_manifest_v3(&decoded, limits)?;
        ensure!(
            bytes.len() <= 8 * 1024 * 1024,
            "derived capture manifest byte limit"
        );
        Ok(bytes)
    }
    fn materialize_selected(&self, intent: &mut Intent) -> Result<()> {
        if let Some(ancestry) = intent
            .header
            .successor
            .as_ref()
            .and_then(|s| s.ancestry.as_ref())
        {
            let original = intent
                .original_manifest
                .as_ref()
                .unwrap_or(&intent.manifest);
            let derived =
                self.ancestry_manifest(original, intent.capture.len(), ancestry.parent)?;
            ensure!(
                c::ManifestId::of(&derived).0 == intent.header.manifest_id
                    && derived.len() as u64 == intent.header.manifest_size,
                "capture successor ancestry binding differs"
            );
            if intent.original_manifest.is_none() {
                intent.original_manifest = Some(std::mem::replace(&mut intent.manifest, derived));
            } else {
                intent.manifest = derived;
            }
        } else if let Some(original) = intent.original_manifest.take() {
            intent.manifest = original;
        }
        Ok(())
    }
    async fn prepare_successor(
        self: &Arc<Self>,
        original: Header,
        descriptor: native_retention::BundleDescriptor,
        generation: u64,
        structured: bool,
        ancestry: Option<SuccessorAncestry>,
        owner: Owner,
    ) -> Result<Header> {
        let worker = self.clone();
        Ok(blocking_owned(owner, move || {
            let attempts = worker.saved_attempts(&original.ticket)?;
            ensure!(
                attempts.len() < MAX_SUCCESSORS,
                "capture successor journal exhausted"
            );
            let previous = attempts.last().unwrap_or(&original);
            ensure!(
                generation > previous.expected_generation
                    || (generation == previous.expected_generation
                        && structured
                        && (previous.publication_identity.is_none()
                            || ancestry
                                .as_ref()
                                .is_some_and(|next| next.missing_parent_refusal
                                    && previous
                                        .successor
                                        .as_ref()
                                        .and_then(|s| s.ancestry.as_ref())
                                        .map(|a| a.parent)
                                        != Some(next.parent)))),
                "capture successor generation did not advance"
            );
            ensure!(
                previous.publication_identity.is_none() || structured,
                "capture successor cannot downgrade structured publication"
            );
            let mut header = original.clone();
            header.version = 2;
            header.expected_generation = generation;
            header.key = format!(
                "capture_{}_attempt_{}",
                hex(&original.ticket),
                attempts.len() + 1
            );
            if structured {
                header.use_new_v2_identity()?;
            }
            header.successor = Some(Successor {
                original: descriptor,
                predecessors: attempts
                    .iter()
                    .map(|h| serde_json::to_vec(h).map(|b| raw_descriptor(&b)))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
                ancestry,
            });
            if let Some(ancestry) = header.successor.as_ref().and_then(|s| s.ancestry.as_ref()) {
                ensure!(
                    structured,
                    "capture ancestry requires structured publication"
                );
                let original_intent = Intent::decode(
                    &worker.read_durable(&format!("{}.job", hex(&original.ticket)))?,
                )?;
                let derived = worker.ancestry_manifest(
                    &original_intent.manifest,
                    original_intent.capture.len(),
                    ancestry.parent,
                )?;
                header.manifest_id = c::ManifestId::of(&derived).0;
                header.manifest_size = derived.len() as u64;
                ensure!(
                    !ancestry.missing_parent_refusal || header.manifest_id != previous.manifest_id,
                    "capture parent repair did not change ancestry"
                );
            }
            let bytes = serde_json::to_vec(&header)?;
            ensure!(bytes.len() <= 4096, "capture successor header limit");
            worker.persist(&attempt_name(&original.ticket, attempts.len() + 1), &bytes)?;
            #[cfg(test)]
            {
                if let Some(pause) = worker._hooks.attempt_pause.lock().unwrap().take() {
                    pause.block();
                }
                ensure!(
                    !worker._hooks.fail_after_attempt.load(Ordering::Acquire),
                    "injected failure after attempt persistence"
                );
            }
            Ok(header)
        })
        .await?
        .value)
    }
    async fn process(
        self: Arc<Self>,
        ticket: PendingCapture,
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
        progress: Arc<Progress>,
    ) -> Result<()> {
        // Identity/routing validation precedes payload reads AND credential use.
        let (mount, alias, identity) = self.host.capture_mapping(&ticket)?;
        // Eligibility is frozen in the workspace registration, not selected by
        // the current global policy after a restart or configuration change.
        let automatic = self.host.capture_auto_upload(&ticket)?;
        let source = self
            .sources
            .get(&alias)
            .context("capture provider unavailable")?
            .clone();
        ensure!(
            source.identity == identity,
            "capture configured provider changed"
        );
        let workspace = crate::native_workspaces::capture_upload_workspace(
            &self.config.state_dir,
            &mount,
            &alias,
            &identity,
        )?;
        let registry = self.host.capture_registry();
        let id = ticket.id;
        let acquired = admission.clone();
        let read = tokio::spawn(async move {
            OwnedValue {
                value: registry.read_capture(id).await,
                _owner: acquired as Owner,
            }
        })
        .await?;
        let read = Arc::new(read.value?);
        ensure!(read.pending() == &ticket, "capture ticket changed");
        let owner: Owner = Arc::new(JobOwner {
            _read: read,
            _admission: admission,
        });
        progress.at("saved_intent");
        let jobname = format!("{}.job", hex(&ticket.id.0));
        let existing = if self.directory.path().join(&jobname).try_exists()? {
            let worker = self.clone();
            Some(
                blocking_owned(owner.clone(), move || {
                    Intent::decode(&worker.read_durable(&jobname)?)
                })
                .await?,
            )
        } else {
            None
        };
        if let Some(intent) = &existing {
            ensure!(
                intent.header.ticket_binding == ticket_binding(&ticket)
                    && intent.header.provider == identity
                    && intent.header.workspace == workspace,
                "capture saved intent binding changed"
            );
        }
        progress.at("publication_lookup");
        let client = source
            .control
            .client_for_owned_work(owner.clone(), deadline)
            .await?;
        let mut intent = if let Some(intent) = existing {
            intent
        } else {
            // Only a genuinely new durable family adopts V2. Legacy journals
            // retain their original receipt domain even on uncertain retries.
            require_new_publication_v2(&client).await?;
            let current = client.cloud_workspace(workspace).await?;
            let identity = c::CaptureExportIdentity {
                workspace,
                exporter: ticket.registration.workspace.0,
                sequence: ticket.registry_epoch,
                parents: current
                    .manifest_id
                    .map(|id| c::ManifestId(id.0))
                    .into_iter()
                    .collect(),
            };
            progress.at("op_view_index_proof");
            let worker = self.clone();
            let ticket = ticket.clone();
            let physical = owner.clone();
            blocking_owned(owner.clone(), move || {
                let mut intent = worker.prepare(&ticket, identity, current.generation, physical)?;
                intent.header.use_new_v2_identity()?;
                let bytes = intent.encode()?;
                intent.original_job = Some(raw_descriptor(&bytes));
                worker.persist(&format!("{}.job", hex(&ticket.id.0)), &bytes)?;
                Ok(intent)
            })
            .await?
        };
        ensure!(
            intent.header.ticket_binding == ticket_binding(&ticket)
                && intent.header.provider == identity
                && intent.header.workspace == workspace,
            "capture saved intent binding changed"
        );
        progress.at("saved_semantic_proof");
        {
            let worker = self.clone();
            let ticket = ticket.clone();
            let owner = owner.clone();
            let saved = Intent {
                original_job: intent.original_job,
                header: intent.header.clone(),
                manifest: intent.manifest.clone(),
                original_manifest: None,
                capture: intent.capture.clone(),
            };
            tokio::task::spawn_blocking(move || {
                let _owner = owner.clone();
                worker.verify_saved(&ticket, &saved, owner)
            })
            .await??;
        }
        progress.at("publication_lookup");
        let worker = self.clone();
        let selected = ticket.id.0;
        let journal = blocking_owned(owner.clone(), move || {
            let (header, descriptor) = worker.original_envelope(&selected)?;
            Ok((header, descriptor, worker.saved_attempts(&selected)?))
        })
        .await?;
        let (saved_header, saved_descriptor, attempts) = journal.value;
        ensure!(
            saved_header == intent.header && Some(saved_descriptor) == intent.original_job,
            "capture original job changed during publication"
        );
        let original = intent.header.clone();
        let mut accepted: Option<Header> = None;
        let mut expired = false;
        let mut latest_expired = false;
        // Always reconcile every prepared key before choosing another envelope.
        // A current head match is never an idempotency receipt.
        for header in std::iter::once(&original).chain(attempts.iter()) {
            match publication_lookup(&client, header).await? {
                PublicationLookup::Accepted(receipt) => {
                    ensure_publication(header, &receipt)?;
                    ensure!(accepted.is_none(), "multiple capture publication receipts");
                    accepted = Some(header.clone());
                }
                PublicationLookup::ReplayExpired => {
                    expired = true;
                    latest_expired |= header == attempts.last().unwrap_or(&original);
                }
                PublicationLookup::Absent => {}
            }
        }
        let latest = attempts.last().unwrap_or(&original);
        if let Some(header) = &accepted {
            ensure!(
                header == latest,
                "capture receipt precedes prepared successor"
            );
        }
        let mut existing = accepted.as_ref().is_some_and(|header| header == latest);
        if existing {
            intent.value.header = accepted.unwrap();
        } else {
            if let Some(last) = attempts.last() {
                intent.value.header = last.clone();
            }
            #[cfg(test)]
            {
                let pause = self._hooks.fence_pause.lock().unwrap().take();
                if let Some(pause) = pause {
                    blocking_owned(owner.clone(), move || {
                        pause.block();
                        Ok(())
                    })
                    .await?;
                }
            }
            let current = client.cloud_workspace(workspace).await?;
            // Store publication is owner/workspace scoped and generation is
            // checked under its commit gate, incremented with checked_add, and
            // never reset by retention. A strictly newer generation fences every
            // older absent request even if its physical RPC outcome was lost.
            // A terminal legacy replay floor closes that request namespace.
            // At equal generation its target is still future and cannot win;
            // typed expiry instead requires a strictly advancing CAS fence.
            let promote_terminal_legacy = latest_expired
                && intent.header.publication_identity.is_none()
                && current.generation == intent.header.expected_generation;
            ensure!(
                !latest_expired
                    || current.generation > intent.header.expected_generation
                    || promote_terminal_legacy,
                "expired publication lacks monotonic generation fence"
            );
            if current.generation > intent.header.expected_generation || promote_terminal_legacy {
                // The fence must precede authoritative absence: an old remote
                // RPC could have committed between the first lookup and this
                // generation read, even after its local waiter was released.
                let mut fenced_receipt: Option<Header> = None;
                for header in std::iter::once(&original).chain(attempts.iter()) {
                    match publication_lookup(&client, header).await? {
                        PublicationLookup::Accepted(receipt) => {
                            ensure_publication(header, &receipt)?;
                            ensure!(
                                fenced_receipt.is_none(),
                                "multiple capture publication receipts"
                            );
                            fenced_receipt = Some(header.clone());
                        }
                        PublicationLookup::ReplayExpired => expired = true,
                        PublicationLookup::Absent => {}
                    }
                }
                if fenced_receipt
                    .as_ref()
                    .is_some_and(|header| header == latest)
                {
                    intent.value.header = fenced_receipt.unwrap();
                    existing = true;
                } else {
                    ensure!(
                        fenced_receipt.is_none(),
                        "capture receipt precedes prepared successor"
                    );
                    let structured = expired || intent.header.publication_identity.is_some();
                    if structured {
                        require_new_publication_v2(&client).await?;
                    }
                    intent.value.header = self
                        .prepare_successor(
                            original.clone(),
                            saved_descriptor,
                            current.generation,
                            structured,
                            if structured {
                                Some(SuccessorAncestry {
                                    parent: current
                                        .manifest_id
                                        .context("capture ancestry head absent")?
                                        .0,
                                    missing_parent_refusal: false,
                                })
                            } else {
                                None
                            },
                            owner.clone(),
                        )
                        .await?;
                }
            }
        }
        let worker = self.clone();
        intent = blocking_owned(owner.clone(), move || {
            worker.materialize_selected(&mut intent.value)?;
            Ok(intent.value)
        })
        .await?;
        if !existing {
            // Automatically enrolled workspaces reuse authenticated retained
            // boundaries and transfer only their local frontier. An unavailable
            // proof remains queued; it never falls back to a public-history walk.
            // Explicit/manual workspaces retain the original full-transfer path.
            let decoded = c::decode_manifest_v3(
                &intent.manifest,
                c::ManifestId(intent.header.manifest_id),
                c::ManifestV3Limits::default(),
            )?;
            let retained = (intent.manifest.len()
                + intent.capture.len()
                + intent.original_manifest.as_ref().map_or(0, Vec::len))
            .checked_mul(4)
            .context("capture retained bytes overflow")? as u64
                + 128 * 1024 * 1024
                + SUCCESSOR_MEMORY;
            let transfer = jj_native_remote::NativeTransferLimits {
                max_memory_bytes: self
                    .policy
                    .max_memory_bytes
                    .checked_sub(retained)
                    .context("capture transfer memory limit")?,
                max_temporary_bytes: self.policy.max_temporary_bytes,
                ..Default::default()
            };
            let roots = decoded.history.recovery.native_roots.clone();
            let decode_bytes = intent
                .capture
                .len()
                .checked_mul(32)
                .and_then(|n| n.checked_add(4096))
                .context("capture DATA decode charge overflow")?;
            ensure!(
                retained
                    .checked_add(decode_bytes as u64)
                    .is_some_and(|n| n <= self.policy.max_memory_bytes),
                "capture DATA decode memory unavailable"
            );
            let landing = {
                let data = c::decode_data(
                    &intent.capture,
                    decoded.capture.id.0,
                    &c::CaptureDataLimits {
                        max_decode_bytes: decode_bytes,
                        ..Default::default()
                    },
                )?;
                // Destructuring inside this scope physically drops all OP,
                // index/blob/root inventories before background transfer starts.
                // Only the separately bounded landing DTO escapes.
                let c::CaptureData { remote, .. } = data;
                remote.and_then(|r| r.landing)
            };
            let transfer = jj_native_remote::NativeTransferLimits {
                max_memory_bytes: transfer
                    .max_memory_bytes
                    .checked_sub(if landing.is_some() {
                        c::PortableLanding::MAX_WORKING_BYTES as u64
                    } else {
                        0
                    })
                    .context("capture landing DTO memory unavailable")?,
                ..transfer
            };
            if landing.is_some() {
                let capabilities = client
                    .capabilities(&[tinytree_client::CapabilityFeature::LandingRecoveryCloudWrite])
                    .await?;
                ensure!(
                    capabilities.features.len() == 1
                        && capabilities.features[0].supported
                        && capabilities.features[0].authorized,
                    "landing recovery publication capability unavailable"
                );
            }

            progress.at("native_frontier");
            let receipt = if automatic || landing.is_some() {
                source
                    .control
                    .transfer_native_capture_frontier_with_landing(
                        roots,
                        landing,
                        owner.clone(),
                        transfer,
                        deadline,
                    )
                    .await?
            } else {
                source
                    .control
                    .transfer_native_closure(
                        source.io.clone(),
                        roots,
                        owner.clone(),
                        transfer,
                        deadline,
                    )
                    .await?
            };
            drop(receipt);
            drop(decoded);
            progress.at("portable_upload");
            let pack = self.pack(&ticket, owner.clone()).await?;
            for lease in pack.portable_entries() {
                let descriptor = lease.descriptor().clone();
                if descriptor.kind == 5 {
                    continue;
                }
                let payload = tokio::task::spawn_blocking(move || lease.read_verified()).await??;
                let expected = CloudRawDigest {
                    blake3: descriptor.blake3,
                    size_bytes: descriptor.size,
                };
                match descriptor.kind {
                    1 | 2 => {
                        progress.at(if descriptor.kind == 1 {
                            "operation_upload"
                        } else {
                            "view_upload"
                        });
                        let id = decode_hex::<64>(&descriptor.id)?;
                        let id = if descriptor.kind == 1 {
                            CloudObjectId::Operation(id)
                        } else {
                            CloudObjectId::View(id)
                        };
                        let mut receipt = client.cloud_put_object(id, payload.bytes()).await?;
                        if receipt.descriptor.selected != expected {
                            let feature =
                                tinytree_client::CapabilityFeature::ExactCloudRepresentationWrite;
                            let capabilities = client.capabilities(&[feature]).await?;
                            ensure!(
                                capabilities
                                    .features
                                    .iter()
                                    .all(|f| f.supported && f.authorized),
                                "exact cloud representation write unsupported or unauthorized"
                            );
                            receipt = client
                                .cloud_put_object_representation(id, payload.bytes())
                                .await?;
                        }
                        ensure!(
                            receipt.descriptor.selected == expected,
                            "cloud exact OPVIEW representation differs"
                        );
                    }
                    3 | 4 => {
                        progress.at("index_upload");
                        let id = decode_hex::<32>(&descriptor.id)?;
                        let id = if descriptor.kind == 3 {
                            CloudIndexArtifactId::Root(id)
                        } else {
                            CloudIndexArtifactId::Node(id)
                        };
                        let receipt = client.cloud_put_index_artifact(id, payload.bytes()).await?;
                        ensure!(
                            receipt.descriptor.selected == expected,
                            "cloud selected index representation differs"
                        );
                    }
                    _ => unreachable!(),
                }
            }
            drop(pack);
            progress.at("publication");
            #[cfg(test)]
            ensure!(
                !self._hooks.fail_before_publication.load(Ordering::Acquire),
                "injected failure before publication"
            );
            // A publication can retire earlier versions of this workspace.
            // Protect installed lazy sources before it can remove their backing;
            // unrelated maintenance still follows the queued upload in poll.
            progress.at("publication_source_pins");
            source
                .control
                .reconcile_retained_source_pins_before_publish_until(
                    intent.header.workspace,
                    owner.clone(),
                    deadline,
                )
                .await?;
            progress.at("publication");
            let receipt = match publish(&client, &intent).await {
                Ok(receipt) => receipt,
                Err(error)
                    if intent.header.publication_identity.is_none()
                        && safe_publication_reason(&error)
                            == Some("store_retained_version_quota") =>
                {
                    // Legacy publication cannot roll a full window. That quota
                    // blocks a same-generation legacy winner; freeing space by
                    // retirement also closes its legacy namespace. An advanced
                    // generation instead fences it by CAS. Preserve the original
                    // job and prove the outcome before journaling a replacement.
                    require_new_publication_v2(&client).await?;
                    let previous = intent.header.clone();
                    match publication_lookup(&client, &previous).await? {
                        PublicationLookup::Accepted(receipt) => {
                            ensure_publication(&previous, &receipt)?;
                            receipt
                        }
                        PublicationLookup::Absent | PublicationLookup::ReplayExpired => {
                            let current = client.cloud_workspace(workspace).await?;
                            ensure!(
                                current.generation >= previous.expected_generation,
                                "capture generation fence regressed"
                            );
                            let after = publication_lookup(&client, &previous).await?;
                            match after {
                                PublicationLookup::Accepted(receipt) => {
                                    ensure_publication(&previous, &receipt)?;
                                    receipt
                                }
                                _ => {
                                    intent.value.header = self
                                        .prepare_successor(
                                            original.clone(),
                                            saved_descriptor,
                                            current.generation,
                                            true,
                                            Some(SuccessorAncestry {
                                                parent: current
                                                    .manifest_id
                                                    .context("capture ancestry head absent")?
                                                    .0,
                                                missing_parent_refusal: false,
                                            }),
                                            owner.clone(),
                                        )
                                        .await?;
                                    let worker = self.clone();
                                    intent = blocking_owned(owner.clone(), move || {
                                        worker.materialize_selected(&mut intent.value)?;
                                        Ok(intent.value)
                                    })
                                    .await?;
                                    // One bounded promotion retry uses the exact
                                    // fsynced identity and already transferred
                                    // payload. Deferring it would rotate behind
                                    // every remaining legacy queue entry.
                                    progress.at("publication_source_pins");
                                    source
                                        .control
                                        .reconcile_retained_source_pins_before_publish_until(
                                            intent.header.workspace,
                                            owner.clone(),
                                            deadline,
                                        )
                                        .await?;
                                    progress.at("publication");
                                    publish(&client, &intent).await?
                                }
                            }
                        }
                    }
                }
                Err(error) if definitive_missing_manifest_parent(&error) => {
                    // Only this definitive publication refusal permits repairing
                    // an already-prepared typed request at the same generation.
                    // Capability failures and generic InvalidArgument never do.
                    require_new_publication_v2(&client).await?;
                    let previous = intent.header.clone();
                    let before = publication_lookup(&client, &previous).await?;
                    let current = client.cloud_workspace(workspace).await?;
                    ensure!(
                        current.generation >= previous.expected_generation,
                        "capture generation fence regressed"
                    );
                    let after = publication_lookup(&client, &previous).await?;
                    match (before, after) {
                        (_, PublicationLookup::Accepted(receipt)) => {
                            ensure_publication(&previous, &receipt)?;
                            receipt
                        }
                        (PublicationLookup::Accepted(_), _) => {
                            anyhow::bail!("capture accepted receipt changed during parent repair")
                        }
                        _ => {
                            intent.value.header = self
                                .prepare_successor(
                                    original.clone(),
                                    saved_descriptor,
                                    current.generation,
                                    true,
                                    Some(SuccessorAncestry {
                                        parent: current
                                            .manifest_id
                                            .context("capture ancestry head absent")?
                                            .0,
                                        missing_parent_refusal: true,
                                    }),
                                    owner.clone(),
                                )
                                .await?;
                            let worker = self.clone();
                            intent = blocking_owned(owner.clone(), move || {
                                worker.materialize_selected(&mut intent.value)?;
                                Ok(intent.value)
                            })
                            .await?;
                            progress.at("publication_source_pins");
                            source
                                .control
                                .reconcile_retained_source_pins_before_publish_until(
                                    intent.header.workspace,
                                    owner.clone(),
                                    deadline,
                                )
                                .await?;
                            progress.at("publication");
                            publish(&client, &intent).await?
                        }
                    }
                }
                Err(error) => return Err(error),
            };
            ensure_publication(&intent.header, &receipt)?;
        }
        progress.at("retained_verification");
        let publication = publication(&client, &intent.header)
            .await?
            .context("capture publication not visible")?;
        ensure_publication(&intent.header, &publication)?;
        ensure!(
            publication.workspace.workspace_id == workspace
                && publication.workspace.generation
                    == intent
                        .header
                        .expected_generation
                        .checked_add(1)
                        .context("capture generation overflow")?
                && publication.workspace.manifest_id
                    == Some(CloudManifestId(intent.header.manifest_id)),
            "capture publication identity differs"
        );
        let retained = client
            .cloud_retained_version_selected(
                RetainedVersionSelector {
                    workspace_id: workspace,
                    generation: publication.workspace.generation,
                    manifest_id: CloudManifestId(intent.header.manifest_id),
                },
                CloudRawDigest::of(&intent.manifest),
            )
            .await?;
        ensure!(
            retained.manifest() == intent.manifest,
            "retained capture manifest differs"
        );
        let captured = client.cloud_captured_version(retained)?;
        let attachment = client.cloud_read_capture(&captured).await?;
        ensure!(
            attachment.payload() == intent.capture,
            "retained capture attachment differs"
        );
        // Reverify all local semantic proofs and exact binding against the
        // authenticated immutable selection before recording historical success.
        progress.at("publication_record");
        #[cfg(test)]
        ensure!(
            !self._hooks.fail_before_marker.load(Ordering::Acquire),
            "injected failure before historical marker"
        );
        let worker = self.clone();
        let ticket = ticket.clone();
        let header = intent.header.clone();
        let owner = owner.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = owner.clone();
            worker.verify_saved(&ticket, &intent, owner)?;
            worker.persist(
                &format!("{}.published", hex(&ticket.id.0)),
                &serde_json::to_vec(&header)?,
            )
        })
        .await??;
        Ok(())
    }
    async fn pack(&self, ticket: &PendingCapture, owner: Owner) -> Result<VerifiedPack> {
        let path = self.config.state_dir.join("native-commands/captures");
        let expected = ticket.bundle;
        let max = self.policy.max_bundle_bytes;
        tokio::task::spawn_blocking(move || {
            let directory = Arc::new(PrivateDir::open_existing(path)?);
            Ok::<_, anyhow::Error>(VerifiedPack::open(
                directory,
                expected.blake3,
                expected.size_bytes,
                PackLimits {
                    max_pack_bytes: max,
                    max_scan_bytes: max,
                    ..Default::default()
                },
                owner,
            )?)
        })
        .await?
    }
    fn proofs(
        &self,
        ticket: &PendingCapture,
        owner: Owner,
    ) -> Result<(
        Vec<c::VerifiedObject>,
        Vec<c::VerifiedIndexArtifact>,
        Vec<c::IndexRootDescriptor>,
        Vec<u8>,
    )> {
        self.proofs_reserved(ticket, owner, 0)
    }
    fn proofs_reserved(
        &self,
        ticket: &PendingCapture,
        owner: Owner,
        reserved: u64,
    ) -> Result<(
        Vec<c::VerifiedObject>,
        Vec<c::VerifiedIndexArtifact>,
        Vec<c::IndexRootDescriptor>,
        Vec<u8>,
    )> {
        // Sequential decoder/proof scratch (not measured RSS). Retained models
        // share the remainder of ONE configured job budget; none gets its own
        // hidden pool. Oversized captures remain pending and observable.
        const SCRATCH: u64 = 1024 * 1024 * 1024;
        let mut remaining = self
            .policy
            .max_memory_bytes
            .checked_sub(SCRATCH)
            .and_then(|n| n.checked_sub(reserved))
            .and_then(|n| n.checked_sub(SUCCESSOR_MEMORY))
            .context("capture proof admission too small")?;
        let directory = Arc::new(PrivateDir::open_existing(
            self.config.state_dir.join("native-commands/captures"),
        )?);
        let pack = VerifiedPack::open(
            directory,
            ticket.bundle.blake3,
            ticket.bundle.size_bytes,
            PackLimits {
                max_pack_bytes: self.policy.max_bundle_bytes,
                max_scan_bytes: self.policy.max_bundle_bytes,
                ..Default::default()
            },
            owner,
        )?;
        let mut objects = Vec::new();
        let mut artifacts = Vec::new();
        let mut roots = Vec::new();
        let mut capture = None;
        for lease in pack.portable_entries() {
            let entry = lease.descriptor();
            let bytes = lease.read_verified()?;
            match entry.kind {
                1 | 2 => {
                    let id = decode_hex::<64>(&entry.id)?;
                    let id = if entry.kind == 1 {
                        c::ObjectId::Operation(id)
                    } else {
                        c::ObjectId::View(id)
                    };
                    let value = c::VerifiedObject::verify(
                        id,
                        c::BlobDigest::of(bytes.bytes()),
                        bytes.bytes(),
                        c::Limits::default(),
                    )?;
                    remaining = remaining
                        .checked_sub(value.retained_size_upper_bound()? as u64)
                        .context("capture retained object budget")?;
                    objects.push(value);
                }
                3 | 4 => {
                    let raw = decode_hex::<32>(&entry.id)?;
                    let id = if entry.kind == 3 {
                        c::IndexArtifactId::Root(c::IndexRootId(raw))
                    } else {
                        c::IndexArtifactId::Node(c::IndexNodeId(raw))
                    };
                    let descriptor = c::IndexArtifactDescriptor {
                        id,
                        selected: c::BlobDigest::of(bytes.bytes()),
                    };
                    let value = c::decode_index_artifact(
                        bytes.bytes(),
                        descriptor,
                        c::IndexArtifactLimits::default(),
                    )?;
                    remaining = remaining
                        .checked_sub(value.retained_size_upper_bound()? as u64 + 512)
                        .context("capture retained artifact budget")?;
                    if let c::IndexArtifactId::Root(id) = id {
                        roots.push(c::IndexRootDescriptor {
                            id,
                            selected: descriptor.selected,
                        });
                    }
                    artifacts.push(value);
                }
                5 => {
                    remaining = remaining
                        .checked_sub(entry.size)
                        .context("capture retained capsule budget")?;
                    ensure!(capture.is_none(), "duplicate capture capsule");
                    ensure!(
                        c::CaptureDataId::of(bytes.bytes()).0 == decode_hex::<32>(&entry.id)?,
                        "capture capsule identity differs"
                    );
                    capture = Some(bytes.bytes().to_vec());
                }
                _ => unreachable!(),
            }
        }
        roots.sort_by_key(|root| root.id);
        let capture = capture.context("saved capture capsule absent")?;
        Ok((objects, artifacts, roots, capture))
    }
    fn prepare(
        &self,
        ticket: &PendingCapture,
        identity: c::CaptureExportIdentity,
        generation: u64,
        owner: Owner,
    ) -> Result<Intent> {
        let (objects, artifacts, roots, capture) = self.proofs(ticket, owner)?;
        let graph =
            c::verify_index_artifact_graph(&roots, &artifacts, c::IndexArtifactLimits::default())?;
        let prepared = c::prepare_capture_manifest(
            &identity,
            &capture,
            &objects,
            &artifacts,
            &graph,
            c::ManifestV3Limits::default(),
        )?;
        ensure!(
            prepared.manifest().history.recovery.native_roots == ticket.checkpoint.roots,
            "capture checkpoint roots differ"
        );
        let data = c::decode_data(
            &capture,
            c::CaptureDataId::of(&capture).0,
            &c::CaptureDataLimits::default(),
        )?;
        let heads = data
            .operation_heads
            .iter()
            .map(|id| decode_hex::<64>(id).map(|id| id.to_vec()))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            heads == ticket.checkpoint.op_heads
                && ticket.checkpoint.working_copy_operation
                    == Some(decode_hex::<64>(&data.working_copy.wc_operation)?.to_vec()),
            "capture checkpoint operations differ"
        );
        let header = Header {
            version: 1,
            ticket: ticket.id.0,
            ticket_binding: ticket_binding(ticket),
            provider: self
                .sources
                .get(&self.host.capture_mapping(ticket)?.1)
                .context("capture provider absent")?
                .identity,
            workspace: identity.workspace,
            expected_generation: generation,
            key: format!("capture_{}", hex(&ticket.id.0)),
            manifest_id: prepared.id().0,
            manifest_size: prepared.bytes().len() as u64,
            capture_size: capture.len() as u64,
            successor: None,
            publication_identity: None,
        };
        Ok(Intent {
            original_job: None,
            header,
            manifest: prepared.bytes().to_vec(),
            original_manifest: None,
            capture,
        })
    }
    fn verify_saved(
        &self,
        ticket: &PendingCapture,
        intent: &Intent,
        owner: Owner,
    ) -> Result<[u8; 32]> {
        self.verify_saved_reserved(ticket, intent, owner, 0)
    }
    fn verify_saved_reserved(
        &self,
        ticket: &PendingCapture,
        intent: &Intent,
        owner: Owner,
        reserved: u64,
    ) -> Result<[u8; 32]> {
        let reserved = reserved.checked_add(intent.original_manifest.as_ref().map_or(0, Vec::len) as u64)
            .context("capture original manifest charge overflow")?;
        let (objects, artifacts, roots, capture) = self.proofs_reserved(ticket, owner, reserved)?;
        ensure!(capture == intent.capture, "saved capture DATA differs");
        let graph =
            c::verify_index_artifact_graph(&roots, &artifacts, c::IndexArtifactLimits::default())?;
        let manifest = c::decode_manifest_v3(
            &intent.manifest,
            c::ManifestId(intent.header.manifest_id),
            c::ManifestV3Limits::default(),
        )?;
        ensure!(
            manifest.history.recovery.workspace == intent.header.workspace
                && manifest.history.recovery.exporter == ticket.registration.workspace.0
                && manifest.history.recovery.sequence == ticket.registry_epoch,
            "capture export identity differs"
        );
        let data = c::decode_data(
            &capture,
            c::CaptureDataId::of(&capture).0,
            &c::CaptureDataLimits::default(),
        )?;
        let heads = data
            .operation_heads
            .iter()
            .map(|id| decode_hex::<64>(id).map(|id| id.to_vec()))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            heads == ticket.checkpoint.op_heads
                && ticket.checkpoint.working_copy_operation
                    == Some(decode_hex::<64>(&data.working_copy.wc_operation)?.to_vec()),
            "capture retained operations differ"
        );
        c::verify_capture_export_binding(
            &manifest,
            &capture,
            &objects,
            &artifacts,
            &graph,
            c::ManifestV3Limits::default(),
        )?;
        ensure!(
            manifest.history.recovery.native_roots == ticket.checkpoint.roots,
            "capture retained root binding differs"
        );
        Ok(data.working_copy.snapshot_commit)
    }
}
fn ensure_publication(header: &Header, receipt: &tinytree_client::CloudPublication) -> Result<()> {
    ensure!(
        receipt.idempotency_key == header.key
            && receipt.workspace.workspace_id == header.workspace
            && header.expected_generation.checked_add(1) == Some(receipt.workspace.generation)
            && receipt.workspace.manifest_id == Some(CloudManifestId(header.manifest_id)),
        "capture publication identity differs"
    );
    Ok(())
}
fn validate_attempt_cleanup(
    directory: &PrivateDir,
    published: &[u8],
) -> Result<Vec<Option<CleanupFile>>> {
    let header: Header = serde_json::from_slice(published)?;
    ensure!(header.valid(), "invalid attempt cleanup publication");
    let mut expected = header
        .successor
        .as_ref()
        .map(|s| s.predecessors.clone())
        .unwrap_or_default();
    if header.successor.is_some() {
        ensure!(
            serde_json::to_vec(&header)? == published,
            "attempt cleanup publication encoding differs"
        );
        expected.push(raw_descriptor(published));
    }
    let mut files = Vec::new();
    let prefix = format!("{}.", hex(&header.ticket));
    for entry in std::fs::read_dir(directory.path())? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().context("invalid cleanup journal name")?;
        if name.starts_with(&prefix)
            && name != format!("{}job", prefix)
            && name != format!("{}published", prefix)
        {
            ensure!(
                attempt_filename(name).is_some_and(|(_, ordinal)| ordinal <= expected.len()),
                "unexpected capture attempt cleanup name"
            );
        }
    }
    for ordinal in 1..=MAX_SUCCESSORS {
        let name = attempt_name(&header.ticket, ordinal);
        if let Some(descriptor) = expected.get(ordinal - 1) {
            files.push(validate_cleanup_file(directory, &name, *descriptor)?);
        } else {
            ensure!(
                path_absent(directory, &name)?,
                "unexpected capture attempt during cleanup"
            );
        }
    }
    Ok(files)
}
fn raw_descriptor(bytes: &[u8]) -> native_retention::BundleDescriptor {
    native_retention::BundleDescriptor {
        blake3: *blake3::hash(bytes).as_bytes(),
        size_bytes: bytes.len() as u64,
    }
}
fn path_absent(directory: &PrivateDir, name: &str) -> Result<bool> {
    match std::fs::symlink_metadata(directory.path().join(name)) {
        Ok(_) => Ok(false),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e.into()),
    }
}
fn validate_covered_journals(
    directory: &PrivateDir,
    old: native_retention::CaptureId,
    newer: native_retention::CaptureId,
    old_job: native_retention::BundleDescriptor,
    witness: &native_retention::RetainedSuccessorWitness,
    require_old: bool,
) -> Result<Vec<CleanupFile>> {
    ensure!(
        path_absent(directory, &format!("{}.published", hex(&old.0)))?,
        "covered old publication appeared; preserve all files"
    );
    let old = validate_cleanup_file(directory, &format!("{}.job", hex(&old.0)), old_job)?;
    ensure!(!require_old || old.is_some(), "covered old job disappeared");
    let mut protected = Vec::new();
    for (suffix, expected) in [("job", witness.job), ("published", witness.published)] {
        protected.push(
            validate_cleanup_file(directory, &format!("{}.{suffix}", hex(&newer.0)), expected)?
                .context("protected successor witness disappeared")?,
        );
    }
    Ok(protected)
}
fn validate_cleanup_file(
    directory: &PrivateDir,
    name: &str,
    expected: native_retention::BundleDescriptor,
) -> Result<Option<CleanupFile>> {
    let path = directory.path().join(name);
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    let mut file = directory.open_existing_file(name, expected.size_bytes)?;
    ensure!(
        file.metadata()?.len() == expected.size_bytes,
        "retired local file size differs; preserve it"
    );
    let mut hash = blake3::Hasher::new();
    let mut buffer = [0; 64 * 1024];
    let mut read = 0u64;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        read = read
            .checked_add(count as u64)
            .context("cleanup file work overflow")?;
        ensure!(
            read <= expected.size_bytes,
            "retired local file grew; preserve it"
        );
        hash.update(&buffer[..count]);
    }
    ensure!(
        read == expected.size_bytes && hash.finalize().as_bytes() == &expected.blake3,
        "retired local file digest differs; preserve it"
    );
    ensure_same_file(&path, &file)?;
    let inspected = file.metadata()?;
    use std::os::unix::fs::MetadataExt;
    let mut aliases = Vec::new();
    if inspected.nlink() > 1 {
        let prefix = if name.ends_with(".pack") {
            "capture-"
        } else {
            "upload-"
        };
        let mut count = 0usize;
        for entry in std::fs::read_dir(directory.path())? {
            count += 1;
            ensure!(count <= 4096, "retired staging alias inventory limit");
            let entry = entry?;
            let alias = entry.file_name();
            let Some(alias) = alias.to_str() else {
                continue;
            };
            let known = alias
                .strip_prefix(prefix)
                .and_then(|id| id.strip_suffix(".part"))
                .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok_and(|id2| id2.to_string() == id));
            if !known {
                continue;
            }
            let metadata = std::fs::symlink_metadata(entry.path())?;
            if metadata.dev() == inspected.dev() && metadata.ino() == inspected.ino() {
                // Only exact known writer-created temporary hardlinks may be
                // reconciled. Arbitrary aliases, including outside this private
                // namespace, keep nlink unmatched and preserve every file.
                let alias_file = directory.open_existing_file(alias, expected.size_bytes)?;
                ensure_same_file(&entry.path(), &alias_file)?;
                ensure!(
                    metadata.len() == expected.size_bytes,
                    "retired staging alias size differs"
                );
                aliases.push(entry.path());
            }
        }
    }
    ensure!(
        file.metadata()?.nlink() == aliases.len() as u64 + 1,
        "retired file has unknown hardlinks; preserve it"
    );
    Ok(Some(CleanupFile {
        path,
        file,
        aliases,
    }))
}
struct CleanupFile {
    path: std::path::PathBuf,
    file: File,
    aliases: Vec<std::path::PathBuf>,
}
impl CleanupFile {
    fn remove(self, hooks: &TestHooks) -> Result<()> {
        use std::os::unix::fs::MetadataExt;
        let mut remaining = self.aliases.len();
        let had_aliases = remaining > 0;
        for alias in self.aliases {
            ensure_same_file(&self.path, &self.file)?;
            ensure_same_file(&alias, &self.file)?;
            ensure!(
                self.file.metadata()?.nlink() == remaining as u64 + 1,
                "retired staging alias identity changed; preserve it"
            );
            std::fs::remove_file(alias)?;
            remaining -= 1;
        }
        if had_aliases {
            // Aliases are discoverable only while the final name exists. Make
            // their deletion durable BEFORE removing that discovery anchor.
            File::open(self.path.parent().context("cleanup file parent absent")?)?.sync_all()?;
            #[cfg(test)]
            ensure!(
                !hooks.fail_after_alias_sync.load(Ordering::Acquire),
                "injected failure after staging alias durability"
            );
        }
        #[cfg(not(test))]
        let _ = hooks;
        ensure_same_file(&self.path, &self.file)?;
        ensure!(
            self.file.metadata()?.nlink() == 1,
            "retired file gained an unknown hardlink; preserve it"
        );
        std::fs::remove_file(self.path)?;
        Ok(())
    }
}
fn ensure_same_file(path: &std::path::Path, file: &File) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let actual = std::fs::symlink_metadata(path)?;
    let inspected = file.metadata()?;
    ensure!(
        actual.is_file()
            && !actual.file_type().is_symlink()
            && actual.dev() == inspected.dev()
            && actual.ino() == inspected.ino()
            && actual.len() == inspected.len()
            && actual.uid() == unsafe { libc::geteuid() }
            && actual.mode() & 0o077 == 0,
        "retired local file identity changed; preserve it"
    );
    Ok(())
}
fn ticket_binding(ticket: &PendingCapture) -> [u8; 32] {
    ticket.binding()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn decode_hex<const N: usize>(text: &str) -> Result<[u8; N]> {
    ensure!(text.len() == N * 2, "capture ID width differs");
    let mut bytes = [0; N];
    for (slot, pair) in bytes.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        let pair = std::str::from_utf8(pair)?;
        *slot = u8::from_str_radix(pair, 16)?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn fixture(hooks: Arc<TestHooks>) -> Result<(tempfile::TempDir, Handle)> {
        fixture_with_policy(hooks, NativeCaptureUploadPolicy::PublishRetainingRoots).await
    }
    async fn fixture_with_policy(
        hooks: Arc<TestHooks>,
        policy: NativeCaptureUploadPolicy,
    ) -> Result<(tempfile::TempDir, Handle)> {
        let root = tempfile::Builder::new().tempdir_in(std::env::temp_dir().canonicalize()?)?;
        let state = PrivateDir::create(root.path().join("state"))?;
        let mut config = Config::default();
        config.state_dir = state.path().to_owned();
        config.native.workspaces.upload.policy = policy;
        if policy == NativeCaptureUploadPolicy::PublishRetiringRedundant {
            config.native.workspaces.capture.policy =
                crate::config::NativeCapturePolicy::AfterCommandBestEffort;
            config.native.workspaces.capture.max_pending = 2;
        }
        let host = Arc::new(
            NativeCommandHost::open_with_capture(
                &state.path().join("native-commands"),
                config.native.workspaces.capture.clone(),
            )
            .await?,
        );
        config.native.workspaces.upload.timeout_ms = 1000;
        config.native.workspaces.upload.poll_ms = 250;
        Ok((
            root,
            start_inner(config, host, BTreeMap::new(), hooks)
                .await?
                .unwrap(),
        ))
    }

    struct NoFiles;
    #[async_trait::async_trait]
    impl local_cas::BlobSource for NoFiles {
        async fn fetch(&self, _: &local_cas::Artifact, _: &mut File) -> Result<()> {
            anyhow::bail!("intrinsic capture must not fetch FILE bytes")
        }
        async fn missing(
            &self,
            values: &[local_cas::Artifact],
        ) -> Result<Vec<local_cas::Artifact>> {
            Ok(values.to_vec())
        }
    }
    fn private(parent: &std::path::Path, name: &str) -> Result<PrivateDir> {
        Ok(PrivateDir::create(parent.join(name))?)
    }
    // Loopback exercises the real scheduler, saved pack verifier and durable
    // publication protocol. Intrinsic roots require no kernel mount or FILE IO.
    async fn ready_uploads(
        count: usize,
        configured: bool,
    ) -> Result<(
        tempfile::TempDir,
        Handle,
        tinytree_client::NativeClient,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        Arc<AtomicBool>,
    )> {
        ready_uploads_with_hooks(count, configured, Arc::new(TestHooks::default())).await
    }
    async fn ready_uploads_with_hooks(
        count: usize,
        configured: bool,
        hooks: Arc<TestHooks>,
    ) -> Result<(
        tempfile::TempDir,
        Handle,
        tinytree_client::NativeClient,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        Arc<AtomicBool>,
    )> {
        ready_uploads_with_data(count, configured, hooks, false, false).await
    }
    async fn ready_uploads_with_data(
        count: usize,
        configured: bool,
        hooks: Arc<TestHooks>,
        distinct_workspaces: bool,
        dropped_remote_head: bool,
    ) -> Result<(
        tempfile::TempDir,
        Handle,
        tinytree_client::NativeClient,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        Arc<AtomicBool>,
    )> {
        use futures::StreamExt;
        let root = tempfile::Builder::new().tempdir_in(std::env::temp_dir().canonicalize()?)?;
        let server_root = private(root.path(), "server")?;
        let mut profile: tinytree_server::Config = toml::from_str(
            r#"
[service]
max_metadata_bytes=4194304
[service.cloud]
enabled=true
indexed_versions=true
captured_versions=true
max_capture_bytes=262144
max_manifest_bytes=524288
max_staging_leases=256
max_buffered_bytes=2147483648
[store]
max_retention_validation_records=4096
max_retention_validation_bytes=8388608
max_closure_objects=8192
max_closure_edges=32768
max_path_contexts=8192
max_path_context_bytes=8388608
[store.cloud]
max_object_bytes=4194304
max_decode_bytes=16777216
max_version_decode_bytes=268435456
max_manifest_bytes=524288
max_manifest_decode_bytes=16777216
max_manifest_members=1024
"#,
        )?;
        profile.service.cloud.durable_retained_offload = !hooks.disable_server_retained_contract;
        if let Some(window) = hooks.rolling_window {
            profile.store.cloud.retirement_enabled = true;
            profile.store.cloud.rolling_retention_enabled = true;
            profile.store.cloud.max_versions_per_workspace = window;
        }
        profile.validate()?;
        let store = tinytree_store::Store::open(
            server_root.path(),
            profile
                .store
                .materialize(profile.service.max_metadata_bytes),
        )?;
        store.create_repository("example")?;
        let empty = tinytree_model::empty_tree();
        store.put("example", empty.descriptor, &empty.bytes, 0)?;
        let index_root = private(root.path(), "index")?;
        let index = tinytree_index::Index::open(index_root.path(), &Default::default())?;
        let service = tinytree_server::RepositoryService::new(
            store.clone(),
            profile.service,
            vec!["example".into()],
            vec![],
        )?
        .with_query_index(index)?;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let denied = Arc::new(AtomicBool::new(false));
        let deny_cloud = denied.clone();
        let availability_hooks = hooks.clone();
        let cloud = tonic::service::interceptor::InterceptedService::new(
            service.cloud_grpc_service(),
            move |request: tonic::Request<()>| {
                availability_hooks
                    .availability_calls
                    .fetch_add(1, Ordering::Relaxed);
                match availability_hooks
                    .availability_fault
                    .load(Ordering::Relaxed)
                {
                    1 => return Err(tonic::Status::not_found("synthetic private missing marker")),
                    // This isolated observation permits its GetVersion, then fails ReadCapture.
                    2 => {
                        availability_hooks
                            .availability_fault
                            .store(3, Ordering::Relaxed);
                    }
                    3 => return Err(tonic::Status::data_loss("synthetic private corrupt marker")),
                    _ => {}
                }
                if deny_cloud.load(Ordering::Acquire) {
                    Err(tonic::Status::permission_denied(
                        "synthetic-sensitive-marker /private/example https://invalid.example.invalid/?token=test-not-a-secret",
                    ))
                } else {
                    Ok(request)
                }
            },
        );
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(service.deadline_layer())
                .add_service(service.grpc_service())
                .add_service(service.query_grpc_service())
                .add_service(cloud)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener).map(|accepted| {
                        accepted.and_then(|stream| {
                            stream.set_nodelay(true)?;
                            Ok(stream)
                        })
                    }),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
        });
        let direct = Arc::new(jj_native_backend::DirectObjectIo::init(
            private(root.path(), "local")?.path(),
            Default::default(),
        )?);
        let cache = local_cas::LocalCas::open(
            root.path().join("cas"),
            Arc::new(NoFiles),
            local_cas::CacheConfig::default(),
        )?;
        let mut remote_config = jj_native_remote::Config::default();
        if let Some(limit) = hooks.provider_pending_limit {
            remote_config.max_pending_operations = limit;
        }

        remote_config.remote.endpoint = format!("http://{address}");
        remote_config.remote.repository_id = "example".into();
        remote_config.remote.rpc.retry.initial_backoff_ms = 1;
        remote_config.remote.rpc.retry.max_backoff_ms = 5;
        let client = tinytree_client::NativeClient::connect(remote_config.remote.clone()).await?;
        let ledger = private(root.path(), "remote")?;
        let remote = jj_native_remote::RemoteRepository::open(
            direct.clone(),
            cache,
            &ledger.path().join("ledger"),
            &ledger.path().join("staging"),
            remote_config,
        )?;
        let source = Arc::new(Source {
            local: direct,
            restore: jj_native_remote::RemoteRestoreIo::new(
                remote.io.clone(),
                remote.query.clone(),
            )?,
            io: remote.io.clone(),
            control: remote.control.clone(),
            identity: [9; 32],
        });
        let dropped_head = if dropped_remote_head {
            let mut commit = tinytree_model::synthetic_root();
            commit.parents = vec![tinytree_model::CommitId::ROOT];
            commit.description = "obsolete observed draft".into();
            Some(
                source
                    .local
                    .write_metadata(tinytree_model::Object::Commit(commit))
                    .await?
                    .key
                    .id
                    .0,
            )
        } else {
            None
        };
        let state = private(root.path(), "state")?;
        let mut config = Config::default();
        config.state_dir = state.path().to_owned();
        config.native.workspaces.upload.policy = NativeCaptureUploadPolicy::PublishRetainingRoots;
        assert_eq!(config.native.workspaces.upload.poll_ms, 30_000);
        let host = Arc::new(if hooks.managed_collection {
            config.native.workspaces.gc.policy = crate::config::NativeGcPolicy::LocalComplete;
            let fresh = crate::native_gc::fresh_cohort(&config)?;
            NativeCommandHost::open_with_policy(
                &state.path().join("native-commands"),
                config.native.workspaces.capture.clone(),
                config.native.workspaces.gc.clone(),
                Some("example".into()),
                fresh,
            )
            .await?
        } else {
            NativeCommandHost::open_with_capture(
                &state.path().join("native-commands"),
                config.native.workspaces.capture.clone(),
            )
            .await?
        });
        let workspace = [7; 32];
        let mount = hex(&workspace);
        let workspace_root = private(root.path(), "workspace")?;
        let admin = private(root.path(), "admin")?;
        let binding = native_workspace_admin::Binding {
            version: 1,
            workspace_id: workspace,
            root: workspace_root.path().to_owned(),
            admin: admin.path().to_owned(),
            repo: admin.path().join("repo"),
            working_copy: admin.path().join("working_copy"),
            repository: "example".into(),
            endpoint: native_workspace_admin::Endpoint::UnixSocket {
                path: root.path().join("rbe.sock"),
            },
            rpc_mount_id: mount.clone(),
        };
        if hooks.managed_collection {
            host.register_with_host_access(&binding, source.identity, host.host_access()?)
                .await?;
        } else {
            host.register(&binding, source.identity).await?;
        }
        let records = private(state.path(), "native-workspaces")?;
        let record = private(records.path(), &mount)?;
        record.replace("workspace.json", &serde_json::to_vec(&serde_json::json!({
            "version":2,"workspace_id":workspace,"store":"example","root":workspace_root.path(),
            "origin":{"kind":"clone","bootstrap_commit":vec![0u8;32]},
            "scope":native_workspace::Scope::from_configured_identity("example", &source.identity)?.backend(),
            "state":"stopped"
        }))?)?;
        let built = c::build_index_artifacts(
            &c::PortableIndexSnapshot {
                roots: vec![tinytree_model::CommitId::ROOT],
                drafts: vec![],
            },
            Default::default(),
        )?;
        let mut payloads = Vec::new();
        let mut blobs = Vec::new();
        for encoded in &built.artifacts {
            let descriptor = encoded.descriptor;
            let (kind, id) = match descriptor.id {
                c::IndexArtifactId::Root(id) => (3, id.0),
                c::IndexArtifactId::Node(id) => (4, id.0),
            };
            payloads.push((kind, hex(&id), encoded.bytes.clone()));
            blobs.push(c::BlobRecord {
                kind,
                id: hex(&id),
                blake3: descriptor.selected.blake3,
                size: descriptor.selected.size_bytes,
            });
        }
        blobs.sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
        let tree = tinytree_model::synthetic_root().root_tree;
        let data = c::CaptureData {
            version: 1,
            policy: c::CapturePolicy::HostQuiescentV1,
            codec: c::CODEC.into(),
            operation_heads: vec![hex(&[0; 64])],
            working_copy: c::PortableWorkingCopy {
                workspace: "default".into(),
                wc_operation: hex(&[0; 64]),
                snapshot_operation: hex(&[0; 64]),
                snapshot_commit: [0; 32],
                tree: c::CaptureTree {
                    terms: tree.terms.iter().map(|id| id.0).collect(),
                    labels: vec![],
                },
            },
            operations: vec![c::OperationRecord {
                operation: hex(&[0; 64]),
                view: hex(&[0; 64]),
                parents: vec![],
            }],
            indexes: vec![c::IndexRecord {
                operation: hex(&[0; 64]),
                root: built.root.id.0,
            }],
            blobs,
            roots: tree
                .terms
                .iter()
                .map(|id| c::CapturedRoot {
                    kind: tinytree_model::Kind::Tree as u8,
                    id: id.0,
                })
                .collect(),
            remote: None,
        };
        let registry = host.capture_registry();
        let registration = host.workspace_state(&mount).await?.registration;
        let captures = PrivateDir::open_existing(state.path().join("native-commands/captures"))?;
        for number in 0..count {
            let mut payloads = payloads.clone();
            let mut data = data.clone();
            if distinct_workspaces {
                data.working_copy.workspace = format!("workspace-{number}");
            }
            if number == 0 {
                if let Some(head) = dropped_head {
                    data.remote = Some(c::PortableRemoteState {
                        observations: vec![c::RemoteObservation {
                            reference: "heads/main".into(),
                            bookmark: "main".into(),
                            generation: 1,
                            head: Some(hex(&head)),
                        }],
                        archived_pushes: Vec::new(),
                        landing: None,
                    });
                    data.roots.push(c::CapturedRoot {
                        kind: tinytree_model::Kind::Commit as u8,
                        id: head,
                    });
                    data.roots.sort();
                }
            }
            let capsule = c::encode_capture_data(&data, Default::default())?;
            payloads.push((5, hex(&c::CaptureDataId::of(&capsule).0), capsule));
            let private = vec![number as u8];
            payloads.push((128, hex(blake3::hash(&private).as_bytes()), private));
            let mut pack = native_capture_pack::MAGIC.to_vec();
            let mut entries = Vec::new();
            for (kind, id, bytes) in payloads {
                let hash = *blake3::hash(&bytes).as_bytes();
                pack.extend(native_capture_pack::frame_header(
                    kind,
                    &id,
                    bytes.len() as u64,
                    hash,
                )?);
                entries.push(native_capture_pack::Entry {
                    kind,
                    id,
                    blake3: hash,
                    size: bytes.len() as u64,
                    offset: pack.len() as u64,
                });
                pack.extend(bytes);
            }
            entries.sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
            let table = native_capture_pack::encode_table(&entries)?;
            pack.extend(native_capture_pack::TABLE_MAGIC);
            pack.extend((table.len() as u64).to_le_bytes());
            pack.extend(table);
            let descriptor = raw_descriptor(&pack);
            private_file(
                &captures
                    .path()
                    .join(format!("{}.pack", hex(&descriptor.blake3))),
                &pack,
            )?;
            let command = registry.begin_command(registration).await?;
            let roots = data
                .roots
                .iter()
                .map(|root| {
                    Ok(tinytree_model::ObjectKey {
                        kind: tinytree_model::Kind::try_from(root.kind)?,
                        id: tinytree_model::ObjectId(root.id),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            registry
                .capture_dirty(command.token(), move || async move {
                    Ok(native_retention::CaptureBundle {
                        checkpoint: native_retention::RootCheckpoint {
                            roots,
                            op_heads: vec![vec![0; 64]],
                            working_copy_operation: Some(vec![0; 64]),
                        },
                        bundle: descriptor,
                    })
                })
                .await?;
            drop(command);
        }
        let sources = if configured {
            BTreeMap::from([("example".into(), source)])
        } else {
            BTreeMap::new()
        };
        let handle = start_inner(config, host, sources, hooks).await?.unwrap();
        Ok((root, handle, client, stop, server, denied))
    }
    struct ReleasePause(Arc<CatalogPause>);
    impl Drop for ReleasePause {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    struct SavedUploads {
        _root: tempfile::TempDir,
        worker: Arc<Worker>,
        client: tinytree_client::NativeClient,
        stop: tokio::sync::oneshot::Sender<()>,
        server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        tickets: Vec<PendingCapture>,
        denied: Arc<AtomicBool>,
    }
    async fn add_healthy_provider_capture(
        f: &SavedUploads,
    ) -> Result<(SavedUploads, Arc<Worker>, PendingCapture, [u8; 32])> {
        let hooks = Arc::new(TestHooks {
            start_stopped: true,
            provider_pending_limit: Some(1),
            ..Default::default()
        });
        let (root, handle, client, stop, server, denied) =
            ready_uploads_with_data(1, true, hooks, false, false).await?;
        let other = handle.worker.clone();
        handle.shutdown().await;
        let template = other
            .host
            .capture_registry()
            .pending_captures()
            .await?
            .remove(0);
        let original = other.sources.get("example").unwrap();
        let source = Arc::new(Source {
            io: original.io.clone(),
            local: original.local.clone(),
            restore: original.restore.clone(),
            control: original.control.clone(),
            identity: [19; 32],
        });
        let workspace = [71; 32];
        let mount = hex(&workspace);
        let workspace_root = private(f._root.path(), "healthy-workspace")?;
        let admin = private(f._root.path(), "healthy-admin")?;
        let binding = native_workspace_admin::Binding {
            version: 1,
            workspace_id: workspace,
            root: workspace_root.path().to_owned(),
            admin: admin.path().to_owned(),
            repo: admin.path().join("repo"),
            working_copy: admin.path().join("working_copy"),
            repository: "healthy".into(),
            endpoint: native_workspace_admin::Endpoint::UnixSocket {
                path: f._root.path().join("healthy-rbe.sock"),
            },
            rpc_mount_id: mount.clone(),
        };
        f.worker.host.register(&binding, source.identity).await?;
        let records =
            PrivateDir::open_existing(f.worker.config.state_dir.join("native-workspaces"))?;
        let record = private(records.path(), &mount)?;
        record.replace("workspace.json", &serde_json::to_vec(&serde_json::json!({
            "version":2,"workspace_id":workspace,"store":"healthy","root":workspace_root.path(),
            "origin":{"kind":"clone","bootstrap_commit":vec![0u8;32]},
            "scope":native_workspace::Scope::from_configured_identity("healthy", &source.identity)?.backend(),"state":"stopped"
        }))?)?;
        let pack = format!("{}.pack", hex(&template.bundle.blake3));
        std::fs::copy(
            other
                .config
                .state_dir
                .join("native-commands/captures")
                .join(&pack),
            f.worker
                .config
                .state_dir
                .join("native-commands/captures")
                .join(&pack),
        )?;
        let registry = f.worker.host.capture_registry();
        let registration = f.worker.host.workspace_state(&mount).await?.registration;
        let command = registry.begin_command(registration).await?;
        let captured = registry
            .capture_dirty(command.token(), move || async move {
                Ok(native_retention::CaptureBundle {
                    checkpoint: template.checkpoint,
                    bundle: template.bundle,
                })
            })
            .await?;
        drop(command);
        let mut sources = f.worker.sources.clone();
        sources.insert("healthy".into(), source);
        let worker = Arc::new(Worker {
            config: f.worker.config.clone(),
            policy: f.worker.policy.clone(),
            host: f.worker.host.clone(),
            sources,
            directory: f.worker.directory.clone(),
            stats: f.worker.stats.clone(),
            admission: f.worker.admission.clone(),
            next_ticket: Mutex::new(None),
            next_retirement: Mutex::new(None),
            availability_schedule: Mutex::new(Default::default()),
            _hooks: f.worker._hooks.clone(),
        });
        Ok((
            SavedUploads {
                _root: root,
                worker: other,
                client,
                stop,
                server,
                tickets: Vec::new(),
                denied,
            },
            worker,
            captured,
            workspace,
        ))
    }
    async fn assert_saved_tickets(f: &SavedUploads) -> Result<()> {
        let mut pending = f.worker.host.capture_registry().pending_captures().await?;
        pending.sort_by_key(|ticket| (ticket.registry_epoch, ticket.id));
        assert_eq!(pending, f.tickets);
        Ok(())
    }
    async fn saved_uploads(
        newer_generation: u64,
        retiring: bool,
        memory: Option<u64>,
    ) -> Result<SavedUploads> {
        saved_uploads_with_data(newer_generation, retiring, memory, false).await
    }
    async fn saved_uploads_with_data(
        newer_generation: u64,
        retiring: bool,
        memory: Option<u64>,
        distinct_workspaces: bool,
    ) -> Result<SavedUploads> {
        saved_uploads_with_roots(
            newer_generation,
            retiring,
            memory,
            distinct_workspaces,
            false,
        )
        .await
    }
    async fn saved_uploads_with_roots(
        newer_generation: u64,
        retiring: bool,
        memory: Option<u64>,
        distinct_workspaces: bool,
        dropped_remote_head: bool,
    ) -> Result<SavedUploads> {
        saved_uploads_with_roots_and_collection(
            newer_generation,
            retiring,
            memory,
            distinct_workspaces,
            dropped_remote_head,
            false,
            true,
        )
        .await
    }
    async fn saved_uploads_with_roots_and_collection(
        newer_generation: u64,
        retiring: bool,
        memory: Option<u64>,
        distinct_workspaces: bool,
        dropped_remote_head: bool,
        managed_collection: bool,
        server_contract: bool,
    ) -> Result<SavedUploads> {
        saved_uploads_with_mode(
            newer_generation,
            retiring,
            memory,
            distinct_workspaces,
            dropped_remote_head,
            managed_collection,
            server_contract,
            false,
        )
        .await
    }
    async fn saved_uploads_with_mode(
        newer_generation: u64,
        retiring: bool,
        memory: Option<u64>,
        distinct_workspaces: bool,
        dropped_remote_head: bool,
        managed_collection: bool,
        server_contract: bool,
        typed: bool,
    ) -> Result<SavedUploads> {
        saved_uploads_with_window(
            newer_generation,
            retiring,
            memory,
            distinct_workspaces,
            dropped_remote_head,
            managed_collection,
            server_contract,
            typed,
            None,
            2,
        )
        .await
    }
    async fn saved_uploads_with_window(
        newer_generation: u64,
        retiring: bool,
        memory: Option<u64>,
        distinct_workspaces: bool,
        dropped_remote_head: bool,
        managed_collection: bool,
        server_contract: bool,
        typed: bool,
        rolling_window: Option<u64>,
        count: usize,
    ) -> Result<SavedUploads> {
        let hooks = Arc::new(TestHooks {
            start_stopped: true,
            managed_collection,
            rolling_window,
            disable_server_retained_contract: !server_contract,
            ..Default::default()
        });
        let (root, handle, client, stop, server, denied) =
            ready_uploads_with_data(count, true, hooks, distinct_workspaces, dropped_remote_head)
                .await?;
        let worker = handle.worker.clone();
        handle.shutdown().await;
        let mut worker = Arc::try_unwrap(worker)
            .map_err(|_| anyhow::anyhow!("stopped fixture worker still owned"))?;
        if retiring {
            worker.policy.policy = NativeCaptureUploadPolicy::PublishRetiringRedundant;
            worker.stats.retiring.store(true, Ordering::Release);
        }
        if let Some(memory) = memory {
            worker.policy.max_memory_bytes = memory;
        }
        let worker = Arc::new(worker);
        let mut tickets = worker.host.capture_registry().pending_captures().await?;
        tickets.sort_by_key(|t| (t.registry_epoch, t.id));
        let admission = cleanup_admission(&worker).await?;
        let physical: Owner = admission.clone();
        let saved = worker.clone();
        let pending = tickets.clone();
        blocking_owned(physical.clone(), move || {
            for (index, ticket) in pending.iter().enumerate() {
                let mut intent = saved.prepare(
                    ticket,
                    c::CaptureExportIdentity {
                        workspace: [7; 32],
                        exporter: ticket.registration.workspace.0,
                        sequence: ticket.registry_epoch,
                        parents: vec![],
                    },
                    if index == 0 { 0 } else { newer_generation },
                    physical.clone(),
                )?;
                if typed {
                    intent.header.use_new_v2_identity()?;
                }
                saved.persist(&format!("{}.job", hex(&ticket.id.0)), &intent.encode()?)?;
            }
            Ok(())
        })
        .await?;
        drop(admission);
        Ok(SavedUploads {
            _root: root,
            worker,
            client,
            stop,
            server,
            tickets,
            denied,
        })
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn selected_capsule_availability_is_fresh_exact_scoped_and_status_only() -> Result<()> {
        use native_vfs_rpc::CommandHost as _;
        let f = saved_uploads(0, false, None).await?;
        process_saved(&f.worker, &f.tickets[0]).await?;
        f.worker.observe_availability_due().await;
        let selected = || f.worker.stats.status()["selected_capsule_availability"].clone();
        assert_eq!(
            selected()["selected_capsules"][0]["last_attempt"],
            "unpublished",
            "an available older publication must not hide the newest unpublished capture"
        );
        process_saved(&f.worker, &f.tickets[1]).await?;
        f.worker.availability_due_now();
        let calls = f.worker._hooks.availability_calls.load(Ordering::Relaxed);
        f.worker.observe_availability_due().await;
        assert_eq!(
            f.worker._hooks.availability_calls.load(Ordering::Relaxed) - calls,
            2,
            "one exact GetVersion and one ReadCapture, no head/list/proof calls"
        );
        assert_eq!(selected()["selected_capsules"][0]["state"], "available");
        assert_eq!(
            selected()["selected_capsules"][0]["selection_current"],
            false
        );
        assert_eq!(selected()["native_file_closure_verified"], false);
        assert_eq!(
            f.worker.stats.status()["current_backup_availability_verified"],
            false
        );
        let before = f.worker.host.capture_registry().pending_captures().await?;
        let calls = f.worker._hooks.availability_calls.load(Ordering::Relaxed);
        for _ in 0..20 {
            let _ = f.worker.host.status();
        }
        f.worker.observe_availability_due().await;
        assert_eq!(
            f.worker._hooks.availability_calls.load(Ordering::Relaxed),
            calls,
            "Status and a not-due scheduler perform zero RPCs"
        );
        assert_eq!(
            f.worker.host.capture_registry().pending_captures().await?,
            before
        );
        f.worker.expire_availability();
        assert_eq!(selected()["selected_capsules"][0]["state"], "stale");
        for (fault, state, reason) in [(1, "unavailable", "not_found"), (2, "unknown", "corrupt")] {
            f.worker
                ._hooks
                .availability_fault
                .store(fault, Ordering::Relaxed);
            f.worker.availability_due_now();
            let calls = f.worker._hooks.availability_calls.load(Ordering::Relaxed);
            f.worker.observe_availability_due().await;
            assert_eq!(
                f.worker._hooks.availability_calls.load(Ordering::Relaxed) - calls,
                if fault == 1 { 1 } else { 2 }
            );
            let value = selected();
            assert_eq!(value["selected_capsules"][0]["state"], state);
            assert_eq!(value["selected_capsules"][0]["last_attempt"], reason);
            let encoded = value.to_string();
            assert!(!encoded.contains("synthetic") && !encoded.contains("private"));
        }
        f.worker
            ._hooks
            .availability_fault
            .store(0, Ordering::Relaxed);
        f.worker.availability_due_now();
        f.worker.observe_availability_due().await;
        assert_eq!(selected()["selected_capsules"][0]["state"], "available");
        let (mount, _, _) = f.worker.host.capture_mapping(&f.tickets[1])?;
        let _preparation = f.worker.host.prepare(&mount, [42; 32]).await?;
        assert_eq!(
            selected()["state"],
            "unknown",
            "real command admission invalidates RAM observation"
        );
        let pause = Arc::new(CatalogPause::default());
        let _release = ReleasePause(pause.clone());
        *f.worker._hooks.availability_pause.lock().unwrap() = Some(pause.clone());
        f.worker.availability_due_now();
        let worker = f.worker.clone();
        let observing = tokio::spawn(async move { worker.observe_availability_due().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !pause.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let _next = f.worker.host.prepare(&mount, [43; 32]).await?;
        pause.release();
        observing.await?;
        assert_eq!(
            selected()["state"],
            "unknown",
            "late success cannot resurrect a changed selection"
        );
        assert_eq!(
            f.worker.host.capture_registry().pending_captures().await?,
            before
        );
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn selected_capsule_availability_timeout_retains_physical_admission_and_shutdown()
    -> Result<()> {
        let pause = Arc::new(CatalogPause::default());
        let _release = ReleasePause(pause.clone());
        let hooks = Arc::new(TestHooks {
            availability_timeout_ms: Some(100),
            ..Default::default()
        });
        *hooks.availability_pause.lock().unwrap() = Some(pause.clone());
        let (_root, handle, _client, stop, server, _denied) =
            ready_uploads_with_hooks(1, true, hooks).await?;
        let worker = handle.worker.clone();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !pause.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        handle.request_stop();
        let shutdown = tokio::spawn(handle.shutdown());
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            !shutdown.is_finished(),
            "physical observation still owns shutdown"
        );
        assert_eq!(worker.admission.available_permits(), 0);
        assert_eq!(
            worker.stats.status()["selected_capsule_availability"]["state"],
            "unknown"
        );
        assert_eq!(
            worker.stats.status()["selected_capsule_availability"]["last_attempt"],
            "timeout"
        );
        worker.availability_due_now();
        worker.observe_availability_due().await;
        assert_eq!(
            worker.admission.available_permits(),
            0,
            "no second physical observation after waiter expiry"
        );
        pause.release();
        tokio::time::timeout(Duration::from_secs(3), shutdown).await??;
        assert_eq!(
            worker.stats.status()["selected_capsule_availability"]["state"],
            "unknown",
            "detached completion never turns green"
        );
        let _ = stop.send(());
        server.await??;

        let mut f =
            saved_uploads_with_roots_and_collection(0, false, None, false, false, true, true)
                .await?;
        process_saved(&f.worker, &f.tickets[1]).await?;
        let registry = f.worker.host.capture_registry();
        let newest = &f.tickets[1];
        let command = registry.begin_command(newest.registration).await?;
        registry
            .checkpoint(command.token(), newest.checkpoint.clone())
            .await?;
        drop(command);
        let collector = registry.acquire_collector().await?;
        Arc::get_mut(&mut f.worker).unwrap().policy.timeout_ms = 100;
        let calls = f.worker._hooks.availability_calls.load(Ordering::Relaxed);
        let proofs = f.worker._hooks.availability_proofs.load(Ordering::Relaxed);
        let worker = f.worker.clone();
        let observing = tokio::spawn(async move { worker.observe_availability_due().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while f.worker._hooks.availability_reads.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        observing.await?;
        assert_eq!(
            f.worker.admission.available_permits(),
            0,
            "expired waiter leaves the capture read physically queued behind collection"
        );
        assert_eq!(
            f.worker.stats.status()["selected_capsule_availability"]["last_attempt"],
            "timeout"
        );
        drop(collector);
        tokio::time::timeout(Duration::from_secs(2), async {
            while f.worker.admission.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(
            f.worker._hooks.availability_proofs.load(Ordering::Relaxed),
            proofs,
            "late capture read must not begin a saved-pack proof"
        );
        assert_eq!(
            f.worker._hooks.availability_calls.load(Ordering::Relaxed),
            calls,
            "late capture read must not begin SDK work"
        );
        assert_eq!(
            f.worker.stats.status()["selected_capsule_availability"]["state"],
            "unknown"
        );
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn equivalent_saved_generation_zero_prefers_newer_and_retires_old_without_publication()
    -> Result<()> {
        let f = saved_uploads(0, true, None).await?;
        let old = &f.tickets[0];
        let newer = &f.tickets[1];
        let newer_job = f
            .worker
            .read_durable(&format!("{}.job", hex(&newer.id.0)))?;
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 0);
        assert!(f.worker.poll().await?);
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 1);
        assert!(
            f.client
                .cloud_publication(&format!("capture_{}", hex(&old.id.0)))
                .await?
                .is_none()
        );
        assert!(
            f.client
                .cloud_publication(&format!("capture_{}", hex(&newer.id.0)))
                .await?
                .is_some()
        );
        assert!(path_absent(
            &f.worker.directory,
            &format!("{}.published", hex(&old.id.0))
        )?);
        assert!(path_absent(
            &f.worker.directory,
            &format!("{}.job", hex(&old.id.0))
        )?);
        assert_eq!(
            f.worker
                .read_durable(&format!("{}.job", hex(&newer.id.0)))?,
            newer_job
        );
        assert!(!path_absent(
            &f.worker.directory,
            &format!("{}.published", hex(&newer.id.0))
        )?);
        assert_eq!(
            f.worker.host.capture_registry().pending_captures().await?,
            vec![newer.clone()]
        );
        assert_eq!(f.worker.stats.published.load(Ordering::Acquire), 1);
        assert_eq!(f.worker.stats.retired.load(Ordering::Acquire), 1);
        assert_eq!(f.worker.stats.failed.load(Ordering::Acquire), 0);
        assert!(!f.worker.host.capture_registry().capture_cleanup_pending()?);
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_job_budget_preserves_normal_rotation_with_distinct_saved_generations()
    -> Result<()> {
        let f = saved_uploads(1, false, Some(1088 * 1024 * 1024)).await?;
        let newer_name = format!("{}.job", hex(&f.tickets[1].id.0));
        let newer = f.worker.read_durable(&newer_name)?;
        assert!(f.worker.poll().await?);
        assert!(
            f.client
                .cloud_publication(&format!("capture_{}", hex(&f.tickets[0].id.0)))
                .await?
                .is_some()
        );
        assert_eq!(f.worker.read_durable(&newer_name)?, newer);
        assert!(f.worker.poll().await?);
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 2);
        assert_eq!(f.worker.stats.failed.load(Ordering::Acquire), 0);
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn distinct_saved_data_or_generation_preserves_oldest_upload_order() -> Result<()> {
        struct Case {
            distinct_data: bool,
            generation: u64,
        }
        for case in [
            Case {
                distinct_data: true,
                generation: 0,
            },
            Case {
                distinct_data: false,
                generation: 1,
            },
        ] {
            let f =
                saved_uploads_with_data(case.generation, false, None, case.distinct_data).await?;
            let old_name = format!("{}.job", hex(&f.tickets[0].id.0));
            let newer_name = format!("{}.job", hex(&f.tickets[1].id.0));
            let old = f.worker.read_durable(&old_name)?;
            let newer = f.worker.read_durable(&newer_name)?;
            assert!(f.worker.poll().await?);
            assert!(
                f.client
                    .cloud_publication(&format!("capture_{}", hex(&f.tickets[0].id.0)))
                    .await?
                    .is_some()
            );
            assert!(
                f.client
                    .cloud_publication(&format!("capture_{}", hex(&f.tickets[1].id.0)))
                    .await?
                    .is_none()
            );
            assert_eq!(f.worker.read_durable(&old_name)?, old);
            assert_eq!(f.worker.read_durable(&newer_name)?, newer);
            assert_saved_tickets(&f).await?;
            assert_eq!(f.worker.stats.failed.load(Ordering::Acquire), 0);
            assert!(f.worker.poll().await?);
            let attempts = f.worker.saved_attempts(&f.tickets[1].id.0)?;
            assert_eq!(attempts.len(), usize::from(case.distinct_data));
            let key = attempts
                .last()
                .map(|h| h.key.clone())
                .unwrap_or_else(|| format!("capture_{}", hex(&f.tickets[1].id.0)));
            let receipt = f
                .client
                .cloud_publication(&key)
                .await?
                .context("second publication absent")?;
            assert_eq!(receipt.workspace.generation, 2);
            assert_eq!(f.worker.read_durable(&old_name)?, old);
            assert_eq!(f.worker.read_durable(&newer_name)?, newer);
            assert_saved_tickets(&f).await?;
            assert_eq!(f.worker.stats.failed.load(Ordering::Acquire), 0);
            let _ = f.stop.send(());
            f.server.await??;
        }
        Ok(())
    }
    async fn process_saved(worker: &Arc<Worker>, ticket: &PendingCapture) -> Result<()> {
        worker
            .clone()
            .process(
                ticket.clone(),
                tokio::time::Instant::now() + Duration::from_secs(30),
                cleanup_admission(worker).await?,
                Progress::new("test_publication"),
            )
            .await
    }
    fn reopen_worker(worker: &Arc<Worker>) -> Arc<Worker> {
        Arc::new(Worker {
            config: worker.config.clone(),
            policy: worker.policy.clone(),
            host: worker.host.clone(),
            sources: worker.sources.clone(),
            directory: worker.directory.clone(),
            stats: worker.stats.clone(),
            admission: worker.admission.clone(),
            next_ticket: Mutex::new(None),
            next_retirement: Mutex::new(None),
            availability_schedule: Mutex::new(Default::default()),
            _hooks: worker._hooks.clone(),
        })
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successor_restart_reconciles_prepared_and_accepted_envelopes() -> Result<()> {
        for typed in [false, true] {
            for accepted in [false, true] {
                let f = saved_uploads_with_mode(0, false, None, true, false, false, true, typed)
                    .await?;
                process_saved(&f.worker, &f.tickets[0]).await?;
                let ticket = &f.tickets[1];
                let original = f
                    .worker
                    .read_durable(&format!("{}.job", hex(&ticket.id.0)))?;
                f.worker
                    ._hooks
                    .fail_after_attempt
                    .store(!accepted, Ordering::Release);
                f.worker
                    ._hooks
                    .fail_before_marker
                    .store(accepted, Ordering::Release);
                assert!(process_saved(&f.worker, ticket).await.is_err());
                let attempts = f.worker.saved_attempts(&ticket.id.0)?;
                assert_eq!(attempts.len(), 1);
                let saved = f.worker.read_durable(&attempt_name(&ticket.id.0, 1))?;
                let original_header = Intent::decode(&original)?.header;
                assert_eq!(original_header.publication_identity.is_some(), typed);
                assert_eq!(attempts[0].publication_identity.is_some(), typed);
                assert_eq!(
                    Intent::decode(&original)?.encode()?,
                    original,
                    "reopening preserves the original durable envelope bytes"
                );
                if typed {
                    let old = original_header.identity()?.unwrap();
                    let next = attempts[0].identity()?.unwrap();
                    assert_eq!(old.generation, 1);
                    assert_eq!(next.generation, 2);
                    assert_ne!(old.nonce, next.nonce);
                } else {
                    assert!(
                        !String::from_utf8_lossy(&original).contains("publication_identity"),
                        "legacy None fields must remain absent from original bytes"
                    );
                }
                assert_eq!(
                    publication(&f.client, &attempts[0]).await?.is_some(),
                    accepted
                );
                f.worker
                    ._hooks
                    .fail_after_attempt
                    .store(false, Ordering::Release);
                f.worker
                    ._hooks
                    .fail_before_marker
                    .store(false, Ordering::Release);
                let mut reopened = reopen_worker(&f.worker);
                // Existing capture slots are full; a successor is metadata, not a
                // new pending capture admission.
                Arc::get_mut(&mut reopened).unwrap().policy.max_jobs = f.tickets.len();
                process_saved(&reopened, ticket).await?;
                assert_eq!(reopened.saved_attempts(&ticket.id.0)?.len(), 1);
                assert_eq!(
                    reopened.read_durable(&attempt_name(&ticket.id.0, 1))?,
                    saved
                );
                assert_eq!(
                    reopened.read_durable(&format!("{}.job", hex(&ticket.id.0)))?,
                    original
                );
                assert_saved_tickets(&f).await?;
                let marker = reopened.read_durable(&format!("{}.published", hex(&ticket.id.0)))?;
                assert!(
                    validate_attempt_cleanup(&reopened.directory, &marker)?
                        .iter()
                        .all(Option::is_some)
                );
                let _ = f.stop.send(());
                f.server.await??;
            }
        }
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn original_receipt_and_uncertain_lookup_never_construct_successors() -> Result<()> {
        for typed in [false, true] {
            let f =
                saved_uploads_with_mode(0, false, None, true, false, false, true, typed).await?;
            let original = f
                .worker
                .read_durable(&format!("{}.job", hex(&f.tickets[0].id.0)))?;
            let original_header = Intent::decode(&original)?.header;
            f.denied.store(true, Ordering::Release);
            assert!(process_saved(&f.worker, &f.tickets[0]).await.is_err());
            assert!(f.worker.saved_attempts(&f.tickets[0].id.0)?.is_empty());
            f.denied.store(false, Ordering::Release);
            f.worker
                ._hooks
                .fail_before_marker
                .store(true, Ordering::Release);
            assert!(process_saved(&f.worker, &f.tickets[0]).await.is_err());
            assert!(publication(&f.client, &original_header).await?.is_some());
            f.worker
                ._hooks
                .fail_before_marker
                .store(false, Ordering::Release);
            process_saved(&reopen_worker(&f.worker), &f.tickets[0]).await?;
            assert!(f.worker.saved_attempts(&f.tickets[0].id.0)?.is_empty());
            assert_eq!(
                f.worker
                    .read_durable(&format!("{}.job", hex(&f.tickets[0].id.0)))?,
                original,
                "uncertain lookup/replay must not create a new identity or Start"
            );
            assert_saved_tickets(&f).await?;
            let _ = f.stop.send(());
            f.server.await??;
        }
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_successor_persistence_retains_physical_admission() -> Result<()> {
        let f = saved_uploads_with_data(0, false, None, true).await?;
        process_saved(&f.worker, &f.tickets[0]).await?;
        let pause = Arc::new(CatalogPause::default());
        let release = ReleasePause(pause.clone());
        *f.worker._hooks.attempt_pause.lock().unwrap() = Some(pause.clone());
        let worker = f.worker.clone();
        let ticket = f.tickets[1].clone();
        let task = tokio::spawn(async move { process_saved(&worker, &ticket).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !pause.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        task.abort();
        let _ = task.await;
        assert!(f.worker.admission.clone().try_acquire_owned().is_err());
        assert_eq!(f.worker.saved_attempts(&f.tickets[1].id.0)?.len(), 1);
        assert_saved_tickets(&f).await?;
        drop(release);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(permit) = f.worker.admission.clone().try_acquire_owned() {
                    drop(permit);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        process_saved(&reopen_worker(&f.worker), &f.tickets[1]).await?;
        assert_eq!(f.worker.saved_attempts(&f.tickets[1].id.0)?.len(), 1);
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publication_committing_between_absence_and_generation_fence_is_reconciled()
    -> Result<()> {
        for typed in [false, true] {
            let f =
                saved_uploads_with_mode(0, false, None, true, false, false, true, typed).await?;
            let ticket = &f.tickets[0];
            f.worker
                ._hooks
                .fail_before_publication
                .store(true, Ordering::Release);
            assert!(process_saved(&f.worker, ticket).await.is_err());
            f.worker
                ._hooks
                .fail_before_publication
                .store(false, Ordering::Release);
            let pause = Arc::new(CatalogPause::default());
            let release = ReleasePause(pause.clone());
            *f.worker._hooks.fence_pause.lock().unwrap() = Some(pause.clone());
            let worker = f.worker.clone();
            let target = ticket.clone();
            let task = tokio::spawn(async move { process_saved(&worker, &target).await });
            tokio::time::timeout(Duration::from_secs(5), async {
                while !pause.entered.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            let original = f
                .worker
                .read_durable(&format!("{}.job", hex(&ticket.id.0)))?;
            let intent = Intent::decode(&original)?;
            publish(&f.client, &intent).await?;
            drop(release);
            task.await??;
            assert!(f.worker.saved_attempts(&ticket.id.0)?.is_empty());
            assert_eq!(
                f.worker
                    .read_durable(&format!("{}.job", hex(&ticket.id.0)))?,
                original
            );
            assert!(
                f.worker
                    .directory
                    .path()
                    .join(format!("{}.published", hex(&ticket.id.0)))
                    .exists()
            );
            assert_saved_tickets(&f).await?;
            let _ = f.stop.send(());
            f.server.await??;
        }
        Ok(())
    }
    async fn competing_publication(f: &SavedUploads) -> Result<()> {
        let current = f.client.cloud_workspace([7; 32]).await?;
        let ticket = &f.tickets[0];
        let owner: Owner = cleanup_admission(&f.worker).await?;
        let mut intent = f.worker.prepare(
            ticket,
            c::CaptureExportIdentity {
                workspace: [7; 32],
                exporter: ticket.registration.workspace.0,
                sequence: ticket.registry_epoch,
                parents: current
                    .manifest_id
                    .map(|id| c::ManifestId(id.0))
                    .into_iter()
                    .collect(),
            },
            current.generation,
            owner,
        )?;
        intent.header.key = format!("competing_publication_{}", current.generation);
        if f.worker._hooks.rolling_window.is_some() {
            intent.header.use_new_v2_identity()?;
            publish(&f.client, &intent).await?;
        } else {
            f.client
                .cloud_publish_captured_version(&intent.sdk()?)
                .await?;
        }
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn legacy_quota_promotion_preserves_full_queue_and_retires_fresh_proof_first()
    -> Result<()> {
        let mut f = saved_uploads_with_window(
            2,
            false,
            Some(1024 * 1024 * 1024 + PAIR_RETAINED_BYTES),
            false,
            false,
            false,
            true,
            false,
            Some(2),
            4,
        )
        .await?;
        Arc::get_mut(&mut f.worker)
            .context("fixture worker still shared")?
            .policy
            .max_jobs = 4;
        process_saved(&f.worker, &f.tickets[0]).await?;
        // Fill the second slot with an ordinary legacy publication, so the
        // server has never yet rolled or closed legacy receipt discovery.
        // Use a distinct manifest so the target reaches quota, not the
        // repeated-current-manifest refusal that precedes it.
        let mut seed = Intent::decode(
            &f.worker
                .read_durable(&format!("{}.job", hex(&f.tickets[2].id.0)))?,
        )?;
        seed.header.expected_generation = 1;
        seed.header.key = "legacy-window-seed".into();
        f.client
            .cloud_publish_captured_version(&seed.sdk()?)
            .await?;
        let ticket = f.tickets[1].clone();
        let name = format!("{}.job", hex(&ticket.id.0));
        let original = f.worker.read_durable(&name)?;
        let pack = f
            .worker
            .config
            .state_dir
            .join("native-commands/captures")
            .join(format!("{}.pack", hex(&ticket.bundle.blake3)));
        let pack_bytes = std::fs::read(&pack)?;
        // A supported single-job budget avoids the optional pair preference.
        // One ordinary poll must finish promotion, not enqueue its retry behind
        // the other unpublished tickets in this already-full local queue.
        assert!(f.worker.poll().await?);
        assert_eq!(f.worker.stats.failed.load(Ordering::Acquire), 0);
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 3);
        let attempts = f.worker.saved_attempts(&ticket.id.0)?;
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].expected_generation, 2);
        assert!(attempts[0].publication_identity.is_some());
        assert!(
            Intent::decode(&original)?
                .header
                .publication_identity
                .is_none()
        );
        assert_eq!(f.worker.read_durable(&name)?, original);
        assert_eq!(std::fs::read(&pack)?, pack_bytes);
        assert_saved_tickets(&f).await?;
        let saved_attempt = f.worker.read_durable(&attempt_name(&ticket.id.0, 1))?;
        process_saved(&reopen_worker(&f.worker), &ticket).await?;
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 3);
        assert_eq!(
            f.worker.read_durable(&attempt_name(&ticket.id.0, 1))?,
            saved_attempt
        );
        let marker = f
            .worker
            .read_durable(&format!("{}.published", hex(&ticket.id.0)))?;
        assert!(
            validate_attempt_cleanup(&f.worker.directory, &marker)?
                .iter()
                .all(Option::is_some)
        );
        // Generation1 is now expired. It must remain local, but must not block
        // retirement of generation3, whose exact proof is still in N=2.
        let ancient = format!("{}.job", hex(&f.tickets[0].id.0));
        let ancient_bytes = f.worker.read_durable(&ancient)?;
        assert!(matches!(
            f.worker
                .retire_one(
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    cleanup_admission(&f.worker).await?,
                    Progress::new("test_backlog"),
                )
                .await?,
            RetirementAttempt::Changed
        ));
        let pending = f.worker.host.capture_registry().pending_captures().await?;
        assert_eq!(pending.len(), 3);
        assert!(!pending.iter().any(|value| value.id == ticket.id));
        assert!(pending.iter().any(|value| value.id == f.tickets[3].id));
        assert_eq!(f.worker.read_durable(&ancient)?, ancient_bytes);
        assert_eq!(f.worker.policy.max_jobs, 4);
        // Another queued original can now advance through a terminal legacy
        // lookup and free a second slot under the same bounded configuration.
        process_saved(&f.worker, &f.tickets[2]).await?;
        assert!(matches!(
            f.worker
                .retire_one(
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    cleanup_admission(&f.worker).await?,
                    Progress::new("test_backlog"),
                )
                .await?,
            RetirementAttempt::Changed
        ));
        let pending = f.worker.host.capture_registry().pending_captures().await?;
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|value| value.id == f.tickets[0].id));
        assert!(pending.iter().any(|value| value.id == f.tickets[3].id));
        assert_eq!(f.worker.read_durable(&ancient)?, ancient_bytes);
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_typed_receipt_republishes_only_through_a_fenced_successor() -> Result<()> {
        let f =
            saved_uploads_with_window(0, false, None, true, false, false, true, true, Some(2), 2)
                .await?;
        let ticket = &f.tickets[0];
        let name = format!("{}.job", hex(&ticket.id.0));
        let original = f.worker.read_durable(&name)?;
        f.worker
            ._hooks
            .fail_before_marker
            .store(true, Ordering::Release);
        assert!(process_saved(&f.worker, ticket).await.is_err());
        f.worker
            ._hooks
            .fail_before_marker
            .store(false, Ordering::Release);
        competing_publication(&f).await?;
        competing_publication(&f).await?;
        let header = Intent::decode(&original)?.header;
        assert!(matches!(
            publication_lookup(&f.client, &header).await?,
            PublicationLookup::ReplayExpired
        ));
        f.worker
            ._hooks
            .fail_after_attempt
            .store(true, Ordering::Release);
        assert!(process_saved(&f.worker, ticket).await.is_err());
        let attempt = f.worker.read_durable(&attempt_name(&ticket.id.0, 1))?;
        let saved = f.worker.saved_attempts(&ticket.id.0)?;
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].expected_generation, 3);
        assert_ne!(
            saved[0].identity()?.unwrap().nonce,
            header.identity()?.unwrap().nonce
        );
        f.worker
            ._hooks
            .fail_after_attempt
            .store(false, Ordering::Release);
        process_saved(&reopen_worker(&f.worker), ticket).await?;
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 4);
        assert_eq!(f.worker.read_durable(&name)?, original);
        assert_eq!(
            f.worker.read_durable(&attempt_name(&ticket.id.0, 1))?,
            attempt
        );
        assert_saved_tickets(&f).await?;
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retired_parent_repair_reopens_exact_derived_manifest_and_rejects_forgery() -> Result<()>
    {
        let f =
            saved_uploads_with_window(0, false, None, true, false, false, true, true, Some(2), 2)
                .await?;
        process_saved(&f.worker, &f.tickets[0]).await?;
        let parent = f
            .client
            .cloud_workspace([7; 32])
            .await?
            .manifest_id
            .context("seed head")?;
        let parent_bytes = Intent::decode(
            &f.worker
                .read_durable(&format!("{}.job", hex(&f.tickets[0].id.0)))?,
        )?
        .manifest;
        let ticket = &f.tickets[1];
        // Arrange the target's original durable capture as a normal generation1
        // observation with one parent, before this target has ever been used.
        let owner: Owner = cleanup_admission(&f.worker).await?;
        let mut target = f.worker.prepare(
            ticket,
            c::CaptureExportIdentity {
                workspace: [7; 32],
                exporter: ticket.registration.workspace.0,
                sequence: ticket.registry_epoch,
                parents: vec![c::ManifestId(parent.0)],
            },
            1,
            owner.clone(),
        )?;
        target.header.use_new_v2_identity()?;
        let original = target.encode()?;
        let name = format!("{}.job", hex(&ticket.id.0));
        private_file(&f.worker.directory.path().join(&name), &original)?;
        drop(owner);
        competing_publication(&f).await?;
        competing_publication(&f).await?;
        // N=2 has physically removed generation1's unprotected parent. Model an
        // actual pre-materialization header-only typed attempt already at gen3.
        let expired = f
            .client
            .cloud_retained_version_selected(
                RetainedVersionSelector {
                    workspace_id: [7; 32],
                    generation: 1,
                    manifest_id: parent,
                },
                CloudRawDigest::of(&parent_bytes),
            )
            .await
            .err()
            .context("parent must be retired")?;
        assert_eq!(expired.code(), tonic::Code::OutOfRange);
        f.worker
            .prepare_successor(
                target.header.clone(),
                raw_descriptor(&original),
                3,
                true,
                None,
                cleanup_admission(&f.worker).await?,
            )
            .await?;
        f.worker
            ._hooks
            .fail_after_attempt
            .store(true, Ordering::Release);
        assert!(process_saved(&f.worker, ticket).await.is_err());
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 3);
        let attempts = f.worker.saved_attempts(&ticket.id.0)?;
        assert_eq!(
            attempts.len(),
            2,
            "specific server refusal permits one same-gen repair"
        );
        assert_eq!(
            attempts[0].expected_generation,
            attempts[1].expected_generation
        );
        assert!(
            attempts[1]
                .successor
                .as_ref()
                .unwrap()
                .ancestry
                .as_ref()
                .unwrap()
                .missing_parent_refusal
        );
        assert_eq!(f.worker.read_durable(&name)?, original);
        let attempt_name = attempt_name(&ticket.id.0, 2);
        let saved = f.worker.read_durable(&attempt_name)?;
        f.worker
            ._hooks
            .fail_after_attempt
            .store(false, Ordering::Release);
        for change_parent in [true, false] {
            let mut forged: Header = serde_json::from_slice(&saved)?;
            if change_parent {
                forged
                    .successor
                    .as_mut()
                    .unwrap()
                    .ancestry
                    .as_mut()
                    .unwrap()
                    .parent = [99; 32];
            } else {
                forged.manifest_id[0] ^= 1;
            }
            private_file(
                &f.worker.directory.path().join(&attempt_name),
                &serde_json::to_vec(&forged)?,
            )?;
            assert!(
                process_saved(&reopen_worker(&f.worker), ticket)
                    .await
                    .is_err()
            );
            assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 3);
            assert_eq!(f.worker.read_durable(&name)?, original);
        }
        private_file(&f.worker.directory.path().join(&attempt_name), &saved)?;
        process_saved(&reopen_worker(&f.worker), ticket).await?;
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 4);
        assert_eq!(f.worker.read_durable(&attempt_name)?, saved);
        assert_eq!(f.worker.read_durable(&name)?, original);
        let marker = f
            .worker
            .read_durable(&format!("{}.published", hex(&ticket.id.0)))?;
        let mut derived = Intent::decode(&original)?;
        f.worker.apply_published(&mut derived, &marker)?;
        let mut before = c::decode_manifest_v3(
            &target.manifest,
            c::ManifestId::of(&target.manifest),
            Default::default(),
        )?;
        let mut after = c::decode_manifest_v3(
            &derived.manifest,
            c::ManifestId::of(&derived.manifest),
            Default::default(),
        )?;
        before.history.recovery.parents.clear();
        after.history.recovery.parents.clear();
        assert!(
            c::encode_manifest_v3(&before, Default::default())?
                == c::encode_manifest_v3(&after, Default::default())?
        );
        assert_eq!(derived.capture, target.capture);
        let retained = f
            .client
            .cloud_retained_version_selected(
                RetainedVersionSelector {
                    workspace_id: [7; 32],
                    generation: 4,
                    manifest_id: CloudManifestId(derived.header.manifest_id),
                },
                CloudRawDigest::of(&derived.manifest),
            )
            .await?;
        assert!(retained.manifest() == derived.manifest);
        drop(retained);
        assert!(
            validate_attempt_cleanup(&f.worker.directory, &marker)?
                .iter()
                .all(Option::is_some)
        );
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn advancing_head_exhausts_bounded_journal_without_mutating_original_or_roots()
    -> Result<()> {
        let f = saved_uploads_with_data(0, false, None, true).await?;
        process_saved(&f.worker, &f.tickets[0]).await?;
        let ticket = &f.tickets[1];
        let original = f
            .worker
            .read_durable(&format!("{}.job", hex(&ticket.id.0)))?;
        f.worker
            ._hooks
            .fail_after_attempt
            .store(true, Ordering::Release);
        for ordinal in 1..=MAX_SUCCESSORS {
            assert!(
                process_saved(&reopen_worker(&f.worker), ticket)
                    .await
                    .is_err()
            );
            assert_eq!(f.worker.saved_attempts(&ticket.id.0)?.len(), ordinal);
            competing_publication(&f).await?;
        }
        f.worker
            ._hooks
            .fail_after_attempt
            .store(false, Ordering::Release);
        let snapshots = (1..=MAX_SUCCESSORS)
            .map(|n| f.worker.read_durable(&attempt_name(&ticket.id.0, n)))
            .collect::<Result<Vec<_>>>()?;
        assert!(
            process_saved(&reopen_worker(&f.worker), ticket)
                .await
                .is_err()
        );
        assert_eq!(f.worker.saved_attempts(&ticket.id.0)?.len(), MAX_SUCCESSORS);
        for (n, bytes) in snapshots.iter().enumerate() {
            assert_eq!(
                &f.worker.read_durable(&attempt_name(&ticket.id.0, n + 1))?,
                bytes
            );
        }
        assert_eq!(
            f.worker
                .read_durable(&format!("{}.job", hex(&ticket.id.0)))?,
            original
        );
        assert_saved_tickets(&f).await?;
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successor_retirement_checks_exact_chain_and_reopens_marker_last_prefix() -> Result<()>
    {
        let f = saved_uploads(0, true, None).await?;
        process_saved(&f.worker, &f.tickets[1]).await?;
        process_saved(&f.worker, &f.tickets[0]).await?;
        let ticket = &f.tickets[0];
        let name = attempt_name(&ticket.id.0, 1);
        let bytes = f.worker.read_durable(&name)?;
        private_file(&f.worker.directory.path().join(&name), b"invalid")?;
        assert!(
            f.worker
                .retire_one(
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    cleanup_admission(&f.worker).await?,
                    Progress::new("test_retirement")
                )
                .await
                .is_err()
        );
        assert_saved_tickets(&f).await?;
        private_file(&f.worker.directory.path().join(&name), &bytes)?;
        let pause = Arc::new(CatalogPause::default());
        let release = ReleasePause(pause.clone());
        *f.worker._hooks.cleanup_pause.lock().unwrap() = Some(pause.clone());
        let worker = f.worker.clone();
        let task = tokio::spawn(async move {
            worker
                .retire_one(
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    cleanup_admission(&worker).await?,
                    Progress::new("test_retirement"),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !pause.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        f.worker
            ._hooks
            .fail_namespace
            .store(true, Ordering::Release);
        drop(release);
        assert!(task.await?.is_err());
        assert!(path_absent(&f.worker.directory, &name)?);
        assert!(!path_absent(
            &f.worker.directory,
            &format!("{}.published", hex(&ticket.id.0))
        )?);
        assert!(f.worker.host.capture_registry().capture_cleanup_pending()?);
        f.worker
            ._hooks
            .fail_namespace
            .store(false, Ordering::Release);
        let reopened = reopen_worker(&f.worker);
        reopened.inventory()?;
        assert!(
            reopened
                .complete_cleanup(cleanup_admission(&reopened).await?)
                .await?
        );
        assert!(path_absent(&reopened.directory, &name)?);
        assert!(path_absent(
            &reopened.directory,
            &format!("{}.published", hex(&ticket.id.0))
        )?);
        assert_eq!(
            reopened.host.capture_registry().pending_captures().await?,
            vec![f.tickets[1].clone()]
        );
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn published_scan_deadline_releases_waiter_but_retains_physical_ticket_admission()
    -> Result<()> {
        let f = saved_uploads_with_data(0, false, None, true).await?;
        process_saved(&f.worker, &f.tickets[0]).await?;
        let mut reopened = reopen_worker(&f.worker);
        Arc::get_mut(&mut reopened).unwrap().policy.timeout_ms = 100;
        let pause = Arc::new(CatalogPause::default());
        let release = ReleasePause(pause.clone());
        *reopened._hooks.published_pause.lock().unwrap() = Some(pause.clone());
        let worker = reopened.clone();
        let task = tokio::spawn(async move { worker.poll().await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !pause.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let result = tokio::time::timeout(Duration::from_secs(1), task).await??;
        assert!(
            result.is_err(),
            "published scan observes the original poll deadline"
        );
        assert!(
            reopened.admission.clone().try_acquire_owned().is_err(),
            "published scan keeps physical admission after its waiter expires"
        );
        assert!(
            matches!(
                reopened.host.capture_registry().try_capture_cleanup().await,
                Err(native_retention::Error::Busy)
            ),
            "published scan keeps the physical capture read guard"
        );
        assert_saved_tickets(&f).await?;
        drop(release);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(permit) = reopened.admission.clone().try_acquire_owned() {
                    drop(permit);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn optional_pair_model_budget_failure_preserves_single_job_backup() -> Result<()> {
        // The pair cutoff passes, but one remaining byte cannot hold even the
        // intrinsic index model. The normal single-job proof has 128MiB slack.
        let f = saved_uploads(0, false, Some(1152 * 1024 * 1024 + 1)).await?;
        assert!(f.worker.poll().await?);
        assert!(
            f.client
                .cloud_publication(&format!("capture_{}", hex(&f.tickets[0].id.0)))
                .await?
                .is_some()
        );
        assert!(
            f.client
                .cloud_publication(&format!("capture_{}", hex(&f.tickets[1].id.0)))
                .await?
                .is_none()
        );
        assert_eq!(f.worker.stats.failed.load(Ordering::Acquire), 0);
        assert_saved_tickets(&f).await?;
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_optional_successor_does_not_starve_valid_selected_backup() -> Result<()> {
        enum Fault {
            ProviderIdentity,
            TicketBinding,
            PackContents,
        }
        for fault in [
            Fault::ProviderIdentity,
            Fault::TicketBinding,
            Fault::PackContents,
        ] {
            let f = saved_uploads(0, false, None).await?;
            let name = format!("{}.job", hex(&f.tickets[1].id.0));
            let saved = f.worker.read_durable(&name)?;
            let mut intent = Intent::decode(&saved)?;
            match fault {
                Fault::ProviderIdentity => intent.header.provider = [99; 32],
                Fault::TicketBinding => intent.header.ticket_binding = [99; 32],
                Fault::PackContents => {
                    let pack = f
                        .worker
                        .config
                        .state_dir
                        .join("native-commands/captures")
                        .join(format!("{}.pack", hex(&f.tickets[1].bundle.blake3)));
                    let mut bytes = std::fs::read(&pack)?;
                    let last = bytes.last_mut().context("fixture pack empty")?;
                    *last ^= 1;
                    private_file(&pack, &bytes)?;
                }
            }
            let invalid = intent.encode()?;
            private_file(&f.worker.directory.path().join(&name), &invalid)?;
            assert!(f.worker.poll().await?);
            assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 1);
            assert!(
                f.client
                    .cloud_publication(&format!("capture_{}", hex(&f.tickets[0].id.0)))
                    .await?
                    .is_some()
            );
            assert_eq!(f.worker.read_durable(&name)?, invalid);
            assert_eq!(f.worker.stats.failed.load(Ordering::Acquire), 0);
            // The next ordinary attempt still exposes the candidate's real
            // failure: declining an optimization never blesses invalid input.
            assert!(!f.worker.poll().await?);
            assert_eq!(f.worker.stats.failed.load(Ordering::Acquire), 1);
            assert_saved_tickets(&f).await?;
            let _ = f.stop.send(());
            f.server.await??;
        }
        Ok(())
    }
    async fn publish_saved_successor(f: &SavedUploads) -> Result<()> {
        f.worker
            .clone()
            .process(
                f.tickets[1].clone(),
                tokio::time::Instant::now() + Duration::from_secs(10),
                cleanup_admission(&f.worker).await?,
                Progress::new("routing"),
            )
            .await?;
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 1);
        Ok(())
    }
    async fn paused_covered_cleanup(
        f: &SavedUploads,
    ) -> Result<(
        Arc<CatalogPause>,
        tokio::task::JoinHandle<Result<RetirementAttempt>>,
    )> {
        let pause = Arc::new(CatalogPause::default());
        *f.worker._hooks.cleanup_pause.lock().unwrap() = Some(pause.clone());
        let worker = f.worker.clone();
        let admission = cleanup_admission(&worker).await?;
        let task = tokio::spawn(async move {
            worker
                .retire_one(
                    tokio::time::Instant::now() + Duration::from_secs(10),
                    admission,
                    Progress::new("old_proof"),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while !pause.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .inspect_err(|_| pause.release())?;
        Ok((pause, task))
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn covered_cleanup_preserves_every_target_when_absence_or_witness_changes() -> Result<()>
    {
        enum Fault {
            OldPublication,
            MissingPublication,
            ChangedJob,
        }
        for fault in [
            Fault::OldPublication,
            Fault::MissingPublication,
            Fault::ChangedJob,
        ] {
            let f = saved_uploads(0, true, None).await?;
            publish_saved_successor(&f).await?;
            let old = &f.tickets[0];
            let newer = &f.tickets[1];
            let old_job = f
                .worker
                .directory
                .path()
                .join(format!("{}.job", hex(&old.id.0)));
            let old_pub = f
                .worker
                .directory
                .path()
                .join(format!("{}.published", hex(&old.id.0)));
            let new_job = f
                .worker
                .directory
                .path()
                .join(format!("{}.job", hex(&newer.id.0)));
            let new_pub = f
                .worker
                .directory
                .path()
                .join(format!("{}.published", hex(&newer.id.0)));
            let pack = f
                .worker
                .config
                .state_dir
                .join("native-commands/captures")
                .join(format!("{}.pack", hex(&old.bundle.blake3)));
            let old_bytes = std::fs::read(&old_job)?;
            let pack_bytes = std::fs::read(&pack)?;
            let job_bytes = std::fs::read(&new_job)?;
            let pub_bytes = std::fs::read(&new_pub)?;
            let (pause, task) = paused_covered_cleanup(&f).await?;
            let _release = ReleasePause(pause.clone());
            match fault {
                Fault::OldPublication => private_file(&old_pub, b"late publication")?,
                Fault::MissingPublication => std::fs::remove_file(&new_pub)?,
                Fault::ChangedJob => private_file(&new_job, &vec![b'x'; job_bytes.len()])?,
            }
            pause.release();
            assert!(task.await?.is_err());
            assert_eq!(std::fs::read(&old_job)?, old_bytes);
            assert_eq!(std::fs::read(&pack)?, pack_bytes);
            let cleanup = f
                .worker
                .host
                .capture_registry()
                .capture_cleanup()
                .await?
                .context("covered cleanup missing")?;
            assert!(matches!(
                cleanup.basis,
                Some(native_retention::CaptureCleanupBasis::CoveredByRetainedSuccessorV1(_))
            ));
            match fault {
                Fault::OldPublication => {
                    assert_eq!(std::fs::read(&old_pub)?, b"late publication");
                    std::fs::remove_file(&old_pub)?;
                }
                Fault::MissingPublication => private_file(&new_pub, &pub_bytes)?,
                Fault::ChangedJob => private_file(&new_job, &job_bytes)?,
            }
            assert!(
                f.worker
                    .complete_cleanup(cleanup_admission(&f.worker).await?)
                    .await?
            );
            assert!(!old_job.exists() && !pack.exists() && !old_pub.exists());
            assert_eq!(std::fs::read(&new_job)?, job_bytes);
            assert_eq!(std::fs::read(&new_pub)?, pub_bytes);
            assert_eq!(
                f.worker.host.capture_registry().pending_captures().await?,
                vec![newer.clone()]
            );
            assert!(!f.worker.host.capture_registry().capture_cleanup_pending()?);
            let _ = f.stop.send(());
            f.server.await??;
        }
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn covered_retirement_refuses_physical_readers_and_rechecks_late_publication()
    -> Result<()> {
        let f = saved_uploads(0, true, None).await?;
        publish_saved_successor(&f).await?;
        let registry = f.worker.host.capture_registry();
        let reader = registry.read_capture(f.tickets[0].id).await?;
        let error = f
            .worker
            .retire_one(
                tokio::time::Instant::now() + Duration::from_secs(10),
                cleanup_admission(&f.worker).await?,
                Progress::new("old_proof"),
            )
            .await
            .err()
            .context("reader must refuse retirement")?;
        assert!(matches!(
            error.downcast_ref::<native_retention::Error>(),
            Some(native_retention::Error::Busy)
        ));
        assert_saved_tickets(&f).await?;
        assert!(!registry.capture_cleanup_pending()?);
        drop(reader);
        let pause = Arc::new(CatalogPause::default());
        let _release = ReleasePause(pause.clone());
        *f.worker._hooks.retirement_pause.lock().unwrap() = Some(pause.clone());
        let worker = f.worker.clone();
        let admission = cleanup_admission(&worker).await?;
        let task = tokio::spawn(async move {
            worker
                .retire_one(
                    tokio::time::Instant::now() + Duration::from_secs(10),
                    admission,
                    Progress::new("old_proof"),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while !pause.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let publication = f
            .worker
            .directory
            .path()
            .join(format!("{}.published", hex(&f.tickets[0].id.0)));
        private_file(&publication, b"late physical owner")?;
        pause.release();
        let error = task
            .await?
            .err()
            .context("late publication must refuse precommit")?;
        assert!(matches!(
            error.downcast_ref::<native_retention::Error>(),
            Some(native_retention::Error::Invalid(
                "covered capture local witness changed"
            ))
        ));
        assert_saved_tickets(&f).await?;
        assert!(!registry.capture_cleanup_pending()?);
        assert_eq!(std::fs::read(&publication)?, b"late physical owner");
        assert!(
            f.worker
                .directory
                .path()
                .join(format!("{}.job", hex(&f.tickets[0].id.0)))
                .exists()
        );
        std::fs::remove_file(publication)?;
        let result = f
            .worker
            .retire_one(
                tokio::time::Instant::now() + Duration::from_secs(10),
                cleanup_admission(&f.worker).await?,
                Progress::new("old_proof"),
            )
            .await?;
        assert!(matches!(result, RetirementAttempt::Changed));
        assert_eq!(
            registry.pending_captures().await?,
            vec![f.tickets[1].clone()]
        );
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retained_old_source_offload_keeps_a_noncovered_root_readable_after_local_gc()
    -> Result<()> {
        let f =
            saved_uploads_with_roots_and_collection(1, true, None, false, true, true, true).await?;
        let old = f.tickets[0].clone();
        let newer = f.tickets[1].clone();
        let extra = *old
            .checkpoint
            .roots
            .iter()
            .find(|root| !newer.checkpoint.roots.contains(root))
            .context("missing old-only canonical root")?;
        let (_, alias, _) = f.worker.host.capture_mapping(&old)?;
        let source = f.worker.sources.get(&alias).unwrap();
        let expected = source.local.read_metadata(extra).await?.descriptor();
        f.worker
            ._hooks
            .retained_offload
            .store(true, Ordering::Release);
        assert!(f.worker.poll().await?);
        let registry = f.worker.host.capture_registry();
        assert_eq!(registry.pending_captures().await?, vec![newer.clone()]);
        assert!(!registry.capture_cleanup_pending()?);
        assert_eq!(f.worker.stats.retired.load(Ordering::Relaxed), 1);
        assert!(
            !f.worker
                .directory
                .path()
                .join(format!("{}.job", hex(&old.id.0)))
                .exists()
        );
        assert!(
            !f.worker
                .directory
                .path()
                .join(format!("{}.published", hex(&old.id.0)))
                .exists()
        );
        assert!(
            f.worker
                .directory
                .path()
                .join(format!("{}.job", hex(&newer.id.0)))
                .exists()
        );
        // Complete the fixture's current checkpoint through the public registry
        // API, then give real native GC the entire fenced current/pending roots.
        let command = registry.begin_command(newer.registration).await?;
        registry
            .checkpoint(command.token(), newer.checkpoint.clone())
            .await?;
        drop(command);
        let collector = Arc::new(registry.acquire_collector().await?);
        assert!(!collector.roots().contains(&extra));
        let roots = collector.roots().to_vec();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64
            + 86_400;
        for time in [now, now + 1] {
            source
                .local
                .client_gc_with_owner(
                    jj_native_backend::NativeClientGcOptions {
                        complete_roots: roots.clone(),
                        transient_roots: Vec::new(),
                        now: time,
                        grace_seconds: 0,
                        limits: Default::default(),
                        deadline: std::time::Instant::now() + Duration::from_secs(10),
                    },
                    collector.clone(),
                )
                .await?;
        }
        drop(collector);
        assert!(
            source.local.descriptor(extra).await?.is_none(),
            "old-only native payload was actually collected"
        );
        assert_eq!(
            source.io.read_metadata(extra).await?.descriptor(),
            expected,
            "original provider recovers through its durable retained source"
        );
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scheduled_retained_offload_uses_pins_without_the_legacy_contract() -> Result<()>
    {
        let f = saved_uploads_with_roots_and_collection(1, true, None, false, true, false, false)
            .await?;
        // Durable source pins protect offload independently of the legacy contract.
        f.worker
            ._hooks
            .retained_offload
            .store(true, Ordering::Release);
        let (_, alias, _) = f.worker.host.capture_mapping(&f.tickets[0])?;
        let source = f.worker.sources.get(&alias).unwrap();
        let client = source
            .control
            .client_for_owned_work(
                Arc::new(()),
                tokio::time::Instant::now() + Duration::from_secs(10),
            )
            .await?;
        let exact = client
            .capabilities(&[tinytree_client::CapabilityFeature::ExactCloudRepresentationRead])
            .await?;
        assert!(exact.features[0].supported && exact.features[0].authorized);
        let contract = client
            .durable_retained_offload_capability()
            .await?
            .context("new server vocabulary missing")?;
        assert!(!contract.supported && contract.authorized);
        let pins = client
            .capabilities(&[tinytree_client::CapabilityFeature::DurableCloudVersionPinV1])
            .await?;
        assert!(pins.features[0].supported && pins.features[0].authorized);
        assert!(f.worker.poll().await?);
        assert!(f.worker.poll().await?);
        assert_eq!(
            f.worker.host.capture_registry().pending_captures().await?,
            vec![f.tickets[1].clone()]
        );
        assert!(!f.worker.host.capture_registry().capture_cleanup_pending()?);
        assert_eq!(f.worker.stats.retired.load(Ordering::Acquire), 1);
        assert_eq!(f.worker.stats.failed.load(Ordering::Acquire), 0);
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retained_old_cleanup_rechecks_source_and_journals_after_a_crash_prefix() -> Result<()>
    {
        let f = saved_uploads_with_roots(1, true, None, false, true).await?;
        // Publish both immutable old/new intents without enabling retirement.
        let policy_worker = f.worker.clone();
        // The existing old-published path first preserves canonical noncoverage.
        assert!(policy_worker.poll().await?);
        assert!(policy_worker.poll().await?);
        assert_saved_tickets(&f).await?;
        policy_worker
            ._hooks
            .retained_offload
            .store(true, Ordering::Release);
        let old = &f.tickets[0];
        let name = hex(&old.id.0);
        let publication = policy_worker
            .directory
            .path()
            .join(format!("{name}.published"));
        let published_bytes = std::fs::read(&publication)?;
        let job = policy_worker.directory.path().join(format!("{name}.job"));
        let job_bytes = std::fs::read(&job)?;
        let pause = Arc::new(CatalogPause::default());
        *policy_worker._hooks.cleanup_pause.lock().unwrap() = Some(pause.clone());
        let worker = policy_worker.clone();
        let admission = cleanup_admission(&worker).await?;
        let mut task = tokio::spawn(async move {
            worker
                .retire_one(
                    tokio::time::Instant::now() + Duration::from_secs(10),
                    admission,
                    Progress::new("old_proof"),
                )
                .await
        });
        let _release = ReleasePause(pause.clone());
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = async {
                    while !pause.entered.load(Ordering::Acquire) {
                        tokio::task::yield_now().await;
                    }
                } => Ok(()),
                result = &mut task => {
                    result?.context("retirement failed before cleanup boundary")?;
                    anyhow::bail!("retirement finished before cleanup boundary")
                }
            }
        })
        .await??;
        private_file(&publication, b"changed immutable publication")?;
        pause.release();
        assert!(task.await?.is_err());
        assert_eq!(std::fs::read(&job)?, job_bytes);
        let registry = policy_worker.host.capture_registry();
        let pending = registry
            .capture_cleanup()
            .await?
            .context("durable retained cleanup missing")?;
        assert!(matches!(
            pending.basis,
            Some(native_retention::CaptureCleanupBasis::RetainedOldCaptureV1(
                _
            ))
        ));
        private_file(&publication, &published_bytes)?;
        f.denied.store(true, Ordering::Release);
        assert!(
            policy_worker
                .complete_cleanup(cleanup_admission(&policy_worker).await?)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&job)?, job_bytes);
        assert_eq!(registry.capture_cleanup().await?, Some(pending.clone()));
        let (healthy, multi, healthy_ticket, healthy_workspace) =
            add_healthy_provider_capture(&f).await?;
        // The complete inventory has three obligations, exceeding this healthy
        // provider's independent pin quota1, initially with ZERO cleanup pins.
        let protected = registry.offload_protected_bindings().await?;
        assert_eq!(protected.len(), 3);
        assert_eq!(
            multi
                .sources
                .get("healthy")
                .unwrap()
                .control
                .reconcile_retained_offload_pins(protected)
                .await?,
            0,
            "complete global inventory is not capped by a provider's unrelated pin quota"
        );
        assert!(
            multi.poll().await?,
            "healthy queued publication precedes unavailable-source cleanup replay"
        );
        assert_eq!(
            healthy
                .client
                .cloud_workspace(healthy_workspace)
                .await?
                .generation,
            1
        );
        let healthy_header: Header = serde_json::from_slice(
            &multi.read_durable(&format!("{}.published", hex(&healthy_ticket.id.0)))?,
        )?;
        let healthy_identity = healthy_header
            .identity()?
            .context("new healthy publication identity absent")?;
        let healthy_receipt = healthy
            .client
            .cloud_publication_v2(healthy_identity)
            .await?
            .context("healthy typed publication absent")?;
        ensure_publication(&healthy_header, &healthy_receipt)?;
        assert_eq!(
            registry.capture_cleanup().await?,
            Some(pending.clone()),
            "no new retirement replaces unresolved cleanup"
        );
        assert_eq!(std::fs::read(&job)?, job_bytes);
        assert_eq!(std::fs::read(&publication)?, published_bytes);
        assert_eq!(multi.stats.retired.load(Ordering::Acquire), 0);
        assert!(
            multi.stats.failed.load(Ordering::Acquire) >= 1,
            "denied replay remains a recorded error, not destructive fallback"
        );
        f.denied.store(false, Ordering::Release);
        assert!(
            policy_worker
                .complete_cleanup(cleanup_admission(&policy_worker).await?)
                .await?
        );
        assert!(!registry.capture_cleanup_pending()?);
        assert!(!job.exists());
        assert!(!publication.exists());
        let _ = healthy.stop.send(());
        healthy.server.await??;
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canonical_noncoverage_backs_off_but_fresh_remote_failure_remains_an_error()
    -> Result<()> {
        let f = saved_uploads_with_roots(1, true, None, false, true).await?;
        let before: Vec<_> = f
            .tickets
            .iter()
            .map(|ticket| {
                let path = f
                    .worker
                    .config
                    .state_dir
                    .join("native-commands/captures")
                    .join(format!("{}.pack", hex(&ticket.bundle.blake3)));
                Ok((path.clone(), std::fs::read(path)?))
            })
            .collect::<Result<_>>()?;
        assert!(f.worker.poll().await?);
        assert!(f.worker.poll().await?);
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 2);
        assert!(
            !f.worker.poll().await?,
            "unproved canonical coverage must enter ordinary idle backoff"
        );
        let status = f.worker.stats.status();
        assert_eq!(status["failed"], 0);
        assert_eq!(
            status["last_disposition"],
            "local_capture_canonical_coverage_unproved"
        );
        assert_eq!(
            status["last_retirement_outcome"],
            serde_json::json!({"stage":"canonical_coverage","result":"not_redundant","error_class":"none","error_code":"none"})
        );
        assert_saved_tickets(&f).await?;
        assert!(!f.worker.host.capture_registry().capture_cleanup_pending()?);
        for (path, bytes) in &before {
            assert_eq!(std::fs::read(path)?, *bytes);
        }
        // No local conclusion substitutes for the old version's fresh retained
        // proof. Deny the real loopback RPC on the next otherwise identical poll.
        f.denied.store(true, Ordering::Release);
        assert!(!f.worker.poll().await?);
        let status = f.worker.stats.status();
        assert_eq!(status["failed"], 1);
        assert_eq!(status["last_disposition"], "retirement_retry_preserved");
        assert_eq!(status["last_retirement_outcome"]["stage"], "old_proof");
        assert_eq!(status["last_retirement_outcome"]["result"], "failed");
        f.denied.store(false, Ordering::Release);
        assert!(!f.worker.poll().await?);
        assert_eq!(f.worker.stats.status()["failed"], 1);
        assert_eq!(
            f.worker.stats.status()["last_retirement_outcome"]["result"],
            "not_redundant"
        );
        assert_saved_tickets(&f).await?;
        for (path, bytes) in &before {
            assert_eq!(std::fs::read(path)?, *bytes);
        }
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn covered_cleanup_reopens_after_unlink_before_ack_and_preserves_shared_pack()
    -> Result<()> {
        let f = saved_uploads(0, true, None).await?;
        let registry = f.worker.host.capture_registry();
        let other = registry
            .register(
                native_retention::WorkspaceId([21; 16]),
                native_retention::OpStoreId([22; 16]),
            )
            .await?;
        let command = registry.begin_command(other).await?;
        let old = f.tickets[0].clone();
        let shared = registry
            .capture_dirty(command.token(), move || async move {
                Ok(native_retention::CaptureBundle {
                    checkpoint: old.checkpoint,
                    bundle: old.bundle,
                })
            })
            .await?;
        drop(command);
        drop(registry);
        publish_saved_successor(&f).await?;
        let pack = f
            .worker
            .config
            .state_dir
            .join("native-commands/captures")
            .join(format!("{}.pack", hex(&f.tickets[0].bundle.blake3)));
        let pack_bytes = std::fs::read(&pack)?;
        let new_job_name = format!("{}.job", hex(&f.tickets[1].id.0));
        let new_pub_name = format!("{}.published", hex(&f.tickets[1].id.0));
        let job_bytes = f.worker.read_durable(&new_job_name)?;
        let pub_bytes = f.worker.read_durable(&new_pub_name)?;
        let (pause, task) = paused_covered_cleanup(&f).await?;
        let _release = ReleasePause(pause.clone());
        f.worker
            ._hooks
            .fail_namespace
            .store(true, Ordering::Release);
        pause.release();
        assert!(task.await?.is_err());
        let cleanup = f
            .worker
            .host
            .capture_registry()
            .capture_cleanup()
            .await?
            .context("pending crash prefix missing")?;
        assert!(!cleanup.remove_bundle);
        assert!(cleanup.basis.is_some());
        assert!(
            !f.worker
                .directory
                .path()
                .join(format!("{}.job", hex(&f.tickets[0].id.0)))
                .exists()
        );
        assert_eq!(std::fs::read(&pack)?, pack_bytes);
        let config = f.worker.config.clone();
        let sources = f.worker.sources.clone();
        let SavedUploads {
            _root,
            worker,
            client,
            stop,
            server,
            tickets,
            denied: _,
        } = f;
        drop(worker);
        let host = Arc::new(
            NativeCommandHost::open_with_capture(
                &config.state_dir.join("native-commands"),
                config.native.workspaces.capture.clone(),
            )
            .await?,
        );
        assert_eq!(
            host.capture_registry().capture_cleanup().await?,
            Some(cleanup)
        );
        let handle = start_inner(
            config,
            host,
            sources,
            Arc::new(TestHooks {
                start_stopped: true,
                ..Default::default()
            }),
        )
        .await?
        .context("reopened upload disabled")?;
        let worker = handle.worker.clone();
        handle.shutdown().await;
        assert!(
            worker
                .complete_cleanup(cleanup_admission(&worker).await?)
                .await?
        );
        assert!(!worker.host.capture_registry().capture_cleanup_pending()?);
        let pending = worker.host.capture_registry().pending_captures().await?;
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&tickets[1]) && pending.contains(&shared));
        assert_eq!(std::fs::read(pack)?, pack_bytes);
        assert_eq!(worker.read_durable(&new_job_name)?, job_bytes);
        assert_eq!(worker.read_durable(&new_pub_name)?, pub_bytes);
        assert_eq!(client.cloud_workspace([7; 32]).await?.generation, 1);
        let _ = stop.send(());
        server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_covered_cleanup_waiter_retains_physical_fences() -> Result<()> {
        let f = saved_uploads(0, true, None).await?;
        publish_saved_successor(&f).await?;
        let (pause, task) = paused_covered_cleanup(&f).await?;
        let _release = ReleasePause(pause.clone());
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert!(matches!(
            f.worker.host.capture_registry().try_host_access(),
            Err(native_retention::Error::Busy)
        ));
        assert!(f.worker.admission.clone().try_acquire_owned().is_err());
        pause.release();
        tokio::time::timeout(Duration::from_secs(3), async {
            while f
                .worker
                .host
                .capture_registry()
                .capture_cleanup()
                .await
                .unwrap()
                .is_some()
                || f.worker.stats.status()["cleanup_pending"] != false
            {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(
            f.worker.host.capture_registry().pending_captures().await?,
            vec![f.tickets[1].clone()]
        );
        assert!(
            !f.worker
                .directory
                .path()
                .join(format!("{}.published", hex(&f.tickets[0].id.0)))
                .exists()
        );
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_poll_drains_queued_publications_then_backs_off_when_idle() -> Result<()> {
        let (_root, handle, client, stop, server, _denied) = ready_uploads(3, true).await?;
        let worker = handle.worker.clone();
        // Less than ONE default idle interval, with all tickets queued before start.
        tokio::time::timeout(Duration::from_secs(15), async {
            while worker.stats.published.load(Ordering::Acquire) != 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert_eq!(client.cloud_workspace([7; 32]).await?.generation, 3);
        assert_eq!(
            worker
                .host
                .capture_registry()
                .pending_captures()
                .await?
                .len(),
            3
        );
        for ticket in worker.host.capture_registry().pending_captures().await? {
            let bytes = worker.read_durable(&format!("{}.job", hex(&ticket.id.0)))?;
            let intent = Intent::decode(&bytes)?;
            assert_eq!(intent.header.version, 2);
            let identity = intent
                .header
                .identity()?
                .context("new publication identity absent")?;
            assert_eq!(identity.workspace_id, intent.header.workspace);
            assert_eq!(
                Some(identity.generation),
                intent.header.expected_generation.checked_add(1)
            );
            assert!(publication(&client, &intent.header).await?.is_some());
            assert!(
                client
                    .cloud_publication(&intent.header.key)
                    .await?
                    .is_none(),
                "display token must not alias a legacy receipt lookup"
            );
            assert_eq!(intent.encode()?, bytes);
        }
        assert_eq!(worker.stats.attempts.load(Ordering::Acquire), 3);
        assert_eq!(worker.stats.failed.load(Ordering::Acquire), 0);
        // Observe the final no-progress inventory reaching backoff before
        // injecting a failure. Blocking-pool scheduling must not decide whether
        // the preceding legitimate poll can discover this file.
        tokio::time::timeout(Duration::from_secs(2), async {
            while worker._hooks.idle_entries.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        private_file(&worker.directory.path().join("invalid.job"), b"invalid")?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(worker.stats.failed.load(Ordering::Acquire), 0);
        handle.shutdown().await;
        let _ = stop.send(());
        server.await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_poll_backs_off_after_unavailable_provider_without_spinning() -> Result<()> {
        let (_root, handle, _client, stop, server, _denied) = ready_uploads(3, false).await?;
        let worker = handle.worker.clone();
        tokio::time::timeout(Duration::from_secs(2), async {
            while worker.stats.failed.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(worker.stats.attempts.load(Ordering::Acquire), 1);
        assert_eq!(worker.stats.failed.load(Ordering::Acquire), 1);
        assert_eq!(worker.stats.published.load(Ordering::Acquire), 0);
        handle.shutdown().await;
        let _ = stop.send(());
        server.await??;
        Ok(())
    }

    #[test]
    fn publication_status_exposes_only_fixed_reasons_at_publication() {
        use tinytree_client::PublicationFailureReason as Reason;
        for (retirement, stage, code, message, expected) in [
            (
                false,
                "publication",
                tonic::Code::ResourceExhausted,
                Reason::CaptureBytes.message().to_owned(),
                Some("capture_bytes"),
            ),
            (
                false,
                "publication",
                tonic::Code::ResourceExhausted,
                format!(
                    "{} synthetic-sensitive-marker",
                    Reason::CaptureBytes.message()
                ),
                None,
            ),
            (
                false,
                "publication",
                tonic::Code::ResourceExhausted,
                "synthetic-sensitive-marker /private/fixture token=not-real".to_owned(),
                None,
            ),
            (
                false,
                "publication",
                tonic::Code::PermissionDenied,
                Reason::CaptureBytes.message().to_owned(),
                None,
            ),
            (
                false,
                "publication_lookup",
                tonic::Code::ResourceExhausted,
                Reason::CaptureBytes.message().to_owned(),
                None,
            ),
            (
                true,
                "publication",
                tonic::Code::ResourceExhausted,
                Reason::CaptureBytes.message().to_owned(),
                None,
            ),
        ] {
            let stats = Stats::default();
            let error = anyhow::Error::new(tonic::Status::with_details(
                code,
                message,
                b"synthetic-sensitive-marker".as_slice().into(),
            ))
            .context("synthetic-sensitive-marker outer context");
            assert_eq!(error.downcast_ref::<tonic::Status>().unwrap().code(), code);
            let result: std::result::Result<Result<()>, tokio::time::error::Elapsed> =
                Ok(Err(error));
            stats.outcome(retirement, &Progress::new(stage), &result);
            let status = stats.status();
            let field = if retirement {
                "last_retirement_outcome"
            } else {
                "last_upload_outcome"
            };
            let outcome = &status[field];
            assert_eq!(outcome["publication_reason"].as_str(), expected);
            if expected.is_none() {
                assert!(outcome.get("publication_reason").is_none());
            }
            let encoded = outcome.to_string();
            for private in ["synthetic-sensitive-marker", "/private/", "token="] {
                assert!(!encoded.contains(private));
            }
        }
        for reason in [
            Reason::StoreValidationCatalogRecords,
            Reason::StoreValidationCatalogMetadata,
            Reason::StoreValidationCatalogRecordsMetadata,
            Reason::StoreValidationCatalogPayload,
            Reason::StoreValidationCatalogRecordsPayload,
            Reason::StoreValidationCatalogMetadataPayload,
            Reason::StoreValidationCatalogRecordsMetadataPayload,
            Reason::StoreIndexProofRecords,
            Reason::StoreIndexProofMetadata,
            Reason::StoreIndexProofRecordsMetadata,
            Reason::StoreIndexProofPayload,
            Reason::StoreIndexProofRecordsPayload,
            Reason::StoreIndexProofMetadataPayload,
            Reason::StoreIndexProofRecordsMetadataPayload,
        ] {
            let stats = Stats::default();
            let failure = anyhow::Error::new(tonic::Status::resource_exhausted(reason.message()))
                .context("synthetic-sensitive-marker /private/fixture");
            let result: std::result::Result<Result<()>, tokio::time::error::Elapsed> =
                Ok(Err(failure));
            stats.outcome(false, &Progress::new("publication"), &result);
            let status = stats.status();
            assert_eq!(
                status["last_upload_outcome"]["publication_reason"],
                reason.as_str()
            );
            assert_eq!(
                status["last_upload_outcome"]["error_code"],
                "resource_exhausted"
            );
            assert!(!status.to_string().contains("synthetic-sensitive-marker"));
        }
    }

    #[test]
    fn native_capture_status_keeps_typed_context_and_redacts_sources() {
        use jj_native_remote::{
            NativeCaptureFailure, NativeCaptureReason as R, NativeCaptureStage as S,
        };
        for (stage, reason) in [
            (S::BoundarySelection, R::RetainedBoundaryUnavailable),
            (S::FrontierCollect, R::NonresidentHistoryUnavailable),
            (S::FrontierCollect, R::NonresidentFileRootUnavailable),
            (S::StagingUpload, R::Unknown),
        ] {
            let error = NativeCaptureFailure::new(stage, reason)
                .attach(
                    tonic::Status::unimplemented(
                        "synthetic-sensitive-marker token=private /private/source",
                    )
                    .into(),
                )
                .context("synthetic-sensitive-context");
            assert_eq!(
                error.downcast_ref::<tonic::Status>().unwrap().code(),
                tonic::Code::Unimplemented
            );
            let stats = Stats::default();
            let result = Ok(Err::<(), _>(error));
            stats.outcome(false, &Progress::new("native_frontier"), &result);
            let status = stats.status();
            let outcome = &status["last_upload_outcome"];
            assert_eq!(outcome["native_stage"], stage.as_str());
            assert_eq!(outcome["native_reason"], reason.as_str());
            assert_eq!(outcome["error_class"], "rpc");
            assert_eq!(outcome["error_code"], "unimplemented");
            for forbidden in ["synthetic-sensitive", "token=", "/private/"] {
                assert!(!status.to_string().contains(forbidden));
            }
        }
        let stats = Stats::default();
        stats.outcome(
            false,
            &Progress::new("publication_lookup"),
            &Ok(Err::<(), _>(
                tonic::Status::unimplemented("synthetic-sensitive-marker").into(),
            )),
        );
        let status = stats.status();
        assert!(status["last_upload_outcome"].get("native_stage").is_none());
        assert!(status["last_upload_outcome"].get("native_reason").is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upload_and_retirement_failures_are_independent_and_sanitized() -> Result<()> {
        let (root, handle, client, stop, server, denied) = ready_uploads(3, true).await?;
        let worker = handle.worker.clone();
        tokio::time::timeout(Duration::from_secs(15), async {
            while worker.stats.published.load(Ordering::Acquire) != 3
                || worker._hooks.idle_entries.load(Ordering::Acquire) == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        handle.shutdown().await;
        assert_eq!(client.cloud_workspace([7; 32]).await?.generation, 3);
        // Reopen the real worker after a publication-marker loss. Older real
        // publications remain eligible for proof, but fresh RPCs now refuse.
        let mut worker = Arc::try_unwrap(worker)
            .map_err(|_| anyhow::anyhow!("worker still physically owned"))?;
        worker.policy.policy = NativeCaptureUploadPolicy::PublishRetiringRedundant;
        worker.stats.retiring.store(true, Ordering::Release);
        let mut tickets = worker.host.capture_registry().pending_captures().await?;
        tickets.sort_by_key(|ticket| (ticket.registry_epoch, ticket.id));
        let latest = tickets.last().unwrap();
        std::fs::remove_file(
            worker
                .directory
                .path()
                .join(format!("{}.published", hex(&latest.id.0))),
        )?;
        worker.sync_namespace()?;
        denied.store(true, Ordering::Release);
        let worker = Arc::new(worker);
        let attempts = worker.stats.attempts.load(Ordering::Acquire);
        assert!(!worker.poll().await?);
        let status = worker.host.status()["capture_upload"].clone();
        assert_eq!(status["last_disposition"], "retirement_retry_preserved");
        assert_eq!(status["last_upload_outcome"]["stage"], "publication_lookup");
        assert_eq!(status["last_retirement_outcome"]["stage"], "old_proof");
        for field in ["last_upload_outcome", "last_retirement_outcome"] {
            assert_eq!(status[field]["result"], "failed");
            assert_eq!(status[field]["error_class"], "rpc");
            assert_eq!(status[field]["error_code"], "permission_denied");
            let encoded = status[field].to_string();
            for forbidden in [
                "synthetic-sensitive-marker",
                "/private/",
                "https://",
                "token=",
                root.path().to_str().unwrap(),
                &hex(&latest.id.0),
            ] {
                assert!(
                    !encoded.contains(forbidden),
                    "diagnostic contains non-static data"
                );
            }
        }
        assert_eq!(worker.stats.attempts.load(Ordering::Acquire), attempts + 1);
        assert_eq!(worker.stats.failed.load(Ordering::Acquire), 2);
        assert_eq!(worker.stats.retired.load(Ordering::Acquire), 0);
        assert_eq!(
            worker
                .host
                .capture_registry()
                .pending_captures()
                .await?
                .len(),
            3
        );
        denied.store(false, Ordering::Release);
        assert_eq!(client.cloud_workspace([7; 32]).await?.generation, 3);
        let _ = stop.send(());
        server.await??;
        Ok(())
    }

    fn private_file(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        File::open(path.parent().unwrap())?.sync_all()?;
        Ok(())
    }
    // Filesystem/registry cleanup fixture, not a remote-backup proof. Real
    // capture semantic verification and fresh authenticated proof are exercised
    // by the mounted CLI workflow before this trusted-host API is called.
    async fn staged_cleanup(
        worker: &Arc<Worker>,
        shared: bool,
    ) -> Result<(
        native_retention::CaptureCleanupObligation,
        std::path::PathBuf,
        std::path::PathBuf,
        Vec<u8>,
    )> {
        let registry = worker.host.capture_registry();
        let registration = registry
            .register(
                native_retention::WorkspaceId([11; 16]),
                native_retention::OpStoreId([12; 16]),
            )
            .await?;
        let captures =
            PrivateDir::open_existing(worker.config.state_dir.join("native-commands/captures"))?;
        let mut tickets = Vec::new();
        for seed in [1u8, 2] {
            let bytes = vec![seed; 128 + seed as usize];
            let descriptor = raw_descriptor(&bytes);
            private_file(
                &captures
                    .path()
                    .join(format!("{}.pack", hex(&descriptor.blake3))),
                &bytes,
            )?;
            let command = registry.begin_command(registration).await?;
            tickets.push(
                registry
                    .capture_dirty(command.token(), move || async move {
                        Ok(native_retention::CaptureBundle {
                            checkpoint: native_retention::RootCheckpoint {
                                roots: vec![tinytree_model::ObjectKey {
                                    kind: tinytree_model::Kind::Commit,
                                    id: tinytree_model::ObjectId([33; 32]),
                                }],
                                op_heads: vec![vec![seed; 64]],
                                working_copy_operation: Some(vec![seed; 64]),
                            },
                            bundle: descriptor,
                        })
                    })
                    .await?,
            );
            drop(command);
        }
        if shared {
            let other = registry
                .register(
                    native_retention::WorkspaceId([13; 16]),
                    native_retention::OpStoreId([14; 16]),
                )
                .await?;
            let command = registry.begin_command(other).await?;
            let old = tickets[0].clone();
            registry
                .capture_dirty(command.token(), move || async move {
                    Ok(native_retention::CaptureBundle {
                        checkpoint: old.checkpoint,
                        bundle: old.bundle,
                    })
                })
                .await?;
            drop(command);
        }
        let old = &tickets[0];
        let header = Header {
            version: 1,
            ticket: old.id.0,
            ticket_binding: old.binding(),
            provider: [3; 32],
            workspace: [4; 32],
            expected_generation: 0,
            key: format!("capture_{}", hex(&old.id.0)),
            manifest_id: [5; 32],
            manifest_size: 0,
            capture_size: 0,
            successor: None,
            publication_identity: None,
        };
        let published = serde_json::to_vec(&header)?;
        let publication = worker
            .directory
            .path()
            .join(format!("{}.published", hex(&old.id.0)));
        private_file(&publication, &published)?;
        let paused = worker.host.pause_capture_cleanup().await?;
        paused.reconcile_disk()?;
        paused.refresh(&registry).await;
        drop(paused);
        let cleanup = registry
            .retire_redundant_capture(
                old.clone(),
                tickets[1].clone(),
                native_retention::CaptureUploadCleanup {
                    job: None,
                    published: Some(raw_descriptor(&published)),
                },
            )
            .await?;
        let pack = captures
            .path()
            .join(format!("{}.pack", hex(&old.bundle.blake3)));
        Ok((cleanup, pack, publication, published))
    }
    async fn cleanup_admission(worker: &Arc<Worker>) -> Result<Arc<Admission>> {
        Ok(Arc::new(Admission {
            _permit: worker.admission.clone().acquire_owned().await?,
        }))
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_preserves_mismatched_files_and_recovers_unlink_before_ack() -> Result<()> {
        let hooks = Arc::new(TestHooks::default());
        let (_root, handle) = fixture_with_policy(
            hooks.clone(),
            NativeCaptureUploadPolicy::PublishRetiringRedundant,
        )
        .await?;
        let worker = handle.worker.clone();
        handle.shutdown().await;
        let registry = worker.host.capture_registry();
        let (cleanup, pack, publication, published) = staged_cleanup(&worker, false).await?;
        let old_bytes = std::fs::read(&pack)?;
        let original_charged = worker.host.status()["local_capture"]["charged_spool_bytes"].clone();
        private_file(&publication, &vec![b'x'; published.len()])?;
        assert!(
            worker
                .complete_cleanup(cleanup_admission(&worker).await?)
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read(&pack)?,
            old_bytes,
            "wrong publication bytes cannot unlink any pack"
        );
        assert_eq!(registry.capture_cleanup().await?, Some(cleanup.clone()));
        private_file(&publication, &published)?;
        let saved = worker.config.state_dir.join("saved-old-pack");
        std::fs::rename(&pack, &saved)?;
        std::os::unix::fs::symlink(&saved, &pack)?;
        assert!(
            worker
                .complete_cleanup(cleanup_admission(&worker).await?)
                .await
                .is_err()
        );
        assert!(std::fs::symlink_metadata(&pack)?.file_type().is_symlink());
        assert_eq!(std::fs::read(&publication)?, published);
        std::fs::remove_file(&pack)?;
        std::fs::rename(saved, &pack)?;
        hooks.fail_namespace.store(true, Ordering::Release);
        assert!(
            worker
                .complete_cleanup(cleanup_admission(&worker).await?)
                .await
                .is_err()
        );
        assert!(!pack.exists() && !publication.exists());
        assert_eq!(registry.capture_cleanup().await?, Some(cleanup));
        assert_eq!(
            worker.host.status()["local_capture"]["charged_spool_bytes"],
            original_charged,
            "uncertain namespace deletion cannot uncharge admission"
        );
        hooks.fail_namespace.store(false, Ordering::Release);
        worker
            .complete_cleanup(cleanup_admission(&worker).await?)
            .await?;
        assert!(registry.capture_cleanup().await?.is_none());
        assert_eq!(registry.pending_captures().await?.len(), 1);
        assert_eq!(worker.host.status()["local_capture"]["pending_count"], 1);
        assert_eq!(
            worker.host.status()["local_capture"]["charged_spool_bytes"],
            130
        );
        assert_eq!(worker.stats.status()["retired_local_capture_count"], 1);
        assert_eq!(worker.stats.status()["cleanup_pending"], false);
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_reconciles_only_exact_writer_staging_hardlinks() -> Result<()> {
        use std::os::unix::fs::MetadataExt;
        let hooks = Arc::new(TestHooks::default());
        let (_root, handle) = fixture_with_policy(
            hooks.clone(),
            NativeCaptureUploadPolicy::PublishRetiringRedundant,
        )
        .await?;
        let worker = handle.worker.clone();
        handle.shutdown().await;
        let registry = worker.host.capture_registry();
        let (cleanup, pack, publication, published) = staged_cleanup(&worker, false).await?;
        let foreign = worker.config.state_dir.join("unknown-hardlink");
        std::fs::hard_link(&publication, &foreign)?;
        assert!(
            worker
                .complete_cleanup(cleanup_admission(&worker).await?)
                .await
                .is_err()
        );
        assert!(pack.exists() && publication.exists() && foreign.exists());
        assert_eq!(registry.capture_cleanup().await?, Some(cleanup.clone()));
        std::fs::remove_file(foreign)?;
        std::fs::remove_file(&publication)?;
        // Exercise persist's actual hardlink-before-temp-unlink crash prefix,
        // not an invented recovery record or a relaxed nlink check.
        hooks.fail_namespace.store(true, Ordering::Release);
        assert!(
            worker
                .persist(
                    publication.file_name().unwrap().to_str().unwrap(),
                    &published
                )
                .is_err()
        );
        hooks.fail_namespace.store(false, Ordering::Release);
        assert_eq!(std::fs::metadata(&publication)?.nlink(), 2);
        let capture_part = pack
            .parent()
            .unwrap()
            .join(format!("capture-{}.part", uuid::Uuid::new_v4()));
        std::fs::hard_link(&pack, &capture_part)?;
        assert_eq!(std::fs::metadata(&pack)?.nlink(), 2);
        hooks.fail_after_alias_sync.store(true, Ordering::Release);
        assert!(
            worker
                .complete_cleanup(cleanup_admission(&worker).await?)
                .await
                .is_err()
        );
        assert!(publication.exists() && pack.exists());
        assert_eq!(
            std::fs::metadata(&publication)?.nlink(),
            1,
            "durable alias removal precedes deleting the final discovery anchor"
        );
        assert_eq!(std::fs::read_dir(worker.directory.path())?.count(), 1);
        assert_eq!(registry.capture_cleanup().await?, Some(cleanup));
        hooks.fail_after_alias_sync.store(false, Ordering::Release);
        worker
            .complete_cleanup(cleanup_admission(&worker).await?)
            .await?;
        assert!(!pack.exists() && !publication.exists() && !capture_part.exists());
        assert_eq!(std::fs::read_dir(worker.directory.path())?.count(), 0);
        assert!(registry.capture_cleanup().await?.is_none());
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reopened_cleanup_is_visible_without_authorizing_retirement_policy() -> Result<()> {
        let (_root, handle) = fixture_with_policy(
            Arc::new(TestHooks::default()),
            NativeCaptureUploadPolicy::PublishRetiringRedundant,
        )
        .await?;
        let worker = handle.worker.clone();
        handle.shutdown().await;
        let (_, pack, publication, _) = staged_cleanup(&worker, false).await?;
        let state = worker.config.state_dir.clone();
        assert_eq!(worker.host.status()["capture_cleanup_pending"], true);
        drop(worker);
        for policy in [
            NativeCaptureUploadPolicy::Disabled,
            NativeCaptureUploadPolicy::PublishRetainingRoots,
        ] {
            let host = Arc::new(NativeCommandHost::open(&state.join("native-commands")).await?);
            let mut config = Config::default();
            config.state_dir = state.clone();
            config.native.workspaces.upload.policy = policy;
            let upload = start_inner(
                config,
                host.clone(),
                BTreeMap::new(),
                Arc::new(TestHooks::default()),
            )
            .await?;
            let status = host.status();
            assert_eq!(status["capture_cleanup_pending"], true);
            if policy == NativeCaptureUploadPolicy::PublishRetainingRoots {
                assert_eq!(status["capture_upload"]["cleanup_pending"], true);
            } else {
                assert_eq!(status["capture_upload"], serde_json::Value::Null);
            }
            assert!(
                pack.exists() && publication.exists(),
                "policy switch never silently authorizes deletion"
            );
            if let Some(upload) = upload {
                upload.shutdown().await;
            }
            drop(host);
        }
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_pack_cleanup_keeps_the_other_registration_payload() -> Result<()> {
        let (_root, handle) = fixture_with_policy(
            Arc::new(TestHooks::default()),
            NativeCaptureUploadPolicy::PublishRetiringRedundant,
        )
        .await?;
        let worker = handle.worker.clone();
        handle.shutdown().await;
        let registry = worker.host.capture_registry();
        let (cleanup, pack, publication, _) = staged_cleanup(&worker, true).await?;
        assert!(!cleanup.remove_bundle);
        let bytes = std::fs::read(&pack)?;
        worker
            .complete_cleanup(cleanup_admission(&worker).await?)
            .await?;
        assert_eq!(std::fs::read(pack)?, bytes);
        assert!(!publication.exists());
        assert_eq!(registry.pending_captures().await?.len(), 2);
        assert!(registry.capture_cleanup().await?.is_none());
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_cleanup_waiter_retains_physical_fences_until_namespace_sync() -> Result<()> {
        struct Release(Arc<CatalogPause>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        let hooks = Arc::new(TestHooks::default());
        let (_root, handle) = fixture_with_policy(
            hooks.clone(),
            NativeCaptureUploadPolicy::PublishRetiringRedundant,
        )
        .await?;
        let worker = handle.worker.clone();
        handle.shutdown().await;
        let registry = worker.host.capture_registry();
        staged_cleanup(&worker, false).await?;
        let pause = Arc::new(CatalogPause::default());
        let _release = Release(pause.clone());
        *hooks.cleanup_pause.lock().unwrap() = Some(pause.clone());
        let admission = cleanup_admission(&worker).await?;
        let physical = worker.clone();
        let waiter = tokio::spawn(async move { physical.complete_cleanup(admission).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !pause.entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(matches!(
            registry.try_host_access(),
            Err(native_retention::Error::Busy)
        ));
        assert!(worker.admission.clone().try_acquire_owned().is_err());
        pause.release();
        tokio::time::timeout(Duration::from_secs(3), async {
            while registry.capture_cleanup().await.unwrap().is_some()
                || worker.stats.status()["cleanup_pending"] != false
            {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(worker.stats.status()["cleanup_pending"], false);
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn visible_record_retry_reestablishes_namespace_barrier_without_rewriting() -> Result<()>
    {
        let hooks = Arc::new(TestHooks::default());
        let (_root, handle) = fixture(hooks.clone()).await?;
        let worker = handle.worker.clone();
        handle.shutdown().await;
        let header = Header {
            version: 1,
            ticket: [1; 32],
            ticket_binding: [2; 32],
            provider: [3; 32],
            workspace: [4; 32],
            expected_generation: 0,
            key: format!("capture_{}", hex(&[1; 32])),
            manifest_id: [5; 32],
            manifest_size: 0,
            capture_size: 0,
            successor: None,
            publication_identity: None,
        };
        let name = format!("{}.published", hex(&header.ticket));
        let bytes = serde_json::to_vec(&header)?;
        hooks.fail_namespace.store(true, Ordering::Release);
        assert!(
            worker.persist(&name, &bytes).is_err(),
            "fault follows real sync+hardlink, before namespace durability"
        );
        assert_eq!(
            worker.directory.read(&name, 4096)?,
            bytes,
            "final name is visible despite uncertain barrier"
        );
        let entries = std::fs::read_dir(worker.directory.path())?.count();
        assert!(
            worker.persist(&name, &bytes).is_err(),
            "identical visible retry must not bypass its failed durability barrier"
        );
        assert_eq!(
            std::fs::read_dir(worker.directory.path())?.count(),
            entries,
            "retry neither rewrites final nor creates a replacement intent"
        );
        hooks.fail_namespace.store(false, Ordering::Release);
        worker.persist(&name, &bytes)?;
        assert_eq!(worker.read_durable(&name)?, bytes);
        assert_eq!(
            std::fs::read_dir(worker.directory.path())?.count(),
            entries,
            "uncertain temp is preserved, not silently repaired or deleted"
        );
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_waits_for_physical_catalog_after_its_waiter_deadline() -> Result<()> {
        struct Release(Arc<CatalogPause>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        let pause = Arc::new(CatalogPause::default());
        let _release = Release(pause.clone());
        let hooks = Arc::new(TestHooks::default());
        *hooks.catalog_pause.lock().unwrap() = Some(pause.clone());
        let (_root, handle) = fixture(hooks).await?;
        for _ in 0..100 {
            if pause.entered.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            pause.entered.load(Ordering::Acquire),
            "actual owned blocking catalog must enter"
        );
        handle.request_stop();
        let shutdown = tokio::spawn(handle.shutdown());
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(
            !shutdown.is_finished(),
            "original waiter timed out, but admitted physical catalog still owns shutdown barrier"
        );
        pause.release();
        tokio::time::timeout(Duration::from_secs(2), shutdown).await??;
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_cloud_restore_selects_newest_capture_after_delayed_older_publication()
    -> Result<()> {
        let f = saved_uploads_with_data(0, false, None, true).await?;
        assert!(f.tickets[1].registry_epoch > f.tickets[0].registry_epoch);
        process_saved(&f.worker, &f.tickets[1]).await?;
        let newest = f.client.cloud_latest_captured_version([7; 32]).await?;
        assert_eq!(newest.generation, 1);
        let expected = newest.manifest_id.0;
        drop(newest);
        process_saved(&f.worker, &f.tickets[0]).await?;
        assert_eq!(f.client.cloud_workspace([7; 32]).await?.generation, 2);
        let source = f.worker.sources.get("example").unwrap().clone();
        let deadline = || tokio::time::Instant::now() + Duration::from_secs(30);
        let selected =
            crate::native_cloud::select(source.clone(), &hex(&[7; 32]), None, deadline()).await?;
        assert_eq!(selected.generation, 1);
        assert_eq!(selected.manifest_id, expected);
        let explicit =
            crate::native_cloud::select(source, &hex(&[7; 32]), Some(2), deadline()).await?;
        assert_eq!(explicit.generation, 2);
        assert_ne!(explicit.manifest_id, expected);
        let _ = f.stop.send(());
        f.server.await??;
        Ok(())
    }
}
