//! Host-local retry provenance for landing resolution. This journal is outside
//! jj administration and portable captures; it never grants remote admission.
use crate::{
    Binding, Registry, hex,
    remote_state::{RemoteScope, valid_hex},
};
use anyhow::{Context as _, Result, ensure};
use fs2::FileExt;
use jj_lib::{
    object_id::ObjectId as _,
    op_store::{OperationId, RootOperationData},
    simple_op_store::SimpleOpStore,
};
use jj_native_backend::VerifiedMetadata;
use jj_vfs::native::NativeTree;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    collections::BTreeMap,
    fs::{self, File},
    path::PathBuf,
    sync::{Arc, Mutex},
};

const DIRECTORY: &str = "landing-resolutions";
const MAX_RECORD_BYTES: usize = 64 * 1024;
const MAX_JOURNALS: usize = 256;
const MAX_INPUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_DECODE_BYTES: usize = 128 * 1024 * 1024;

/// Explicit source-codec/allocation-work ceilings, not a measured RSS promise.
/// Lower caps are supported; no field may exceed the existing native reader cap.
#[derive(Clone, Debug)]
pub struct WorkingLimits {
    pub max_input_bytes: usize,
    pub max_decode_bytes: usize,
    pub max_wc_bytes: usize,
    pub max_record_bytes: usize,
    pub max_proofs: usize,
    pub max_proof_bytes: usize,
    pub max_proof_input_bytes: usize,
    pub max_retained_bytes: usize,
}
impl Default for WorkingLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: MAX_INPUT_BYTES,
            max_decode_bytes: MAX_DECODE_BYTES,
            max_wc_bytes: MAX_INPUT_BYTES,
            max_record_bytes: MAX_RECORD_BYTES,
            max_proofs: 2,
            max_proof_bytes: MAX_INPUT_BYTES,
            max_proof_input_bytes: 2 * MAX_INPUT_BYTES,
            max_retained_bytes: 32 * 1024 * 1024,
        }
    }
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).context("resolution read bound overflow")
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).context("resolution read bound overflow")
}
impl WorkingLimits {
    fn validate(&self) -> Result<()> {
        ensure!(
            (1..=MAX_INPUT_BYTES).contains(&self.max_input_bytes)
                && (4096..=MAX_DECODE_BYTES).contains(&self.max_decode_bytes)
                && (1..=MAX_INPUT_BYTES).contains(&self.max_wc_bytes)
                && (1024..=MAX_RECORD_BYTES).contains(&self.max_record_bytes)
                && (1..=2).contains(&self.max_proofs)
                && (1..=MAX_INPUT_BYTES).contains(&self.max_proof_bytes)
                && self.max_proof_input_bytes >= self.max_proof_bytes
                && self.max_proof_input_bytes <= 2 * MAX_INPUT_BYTES
                && (4096..=1024 * 1024 * 1024).contains(&self.max_retained_bytes),
            "invalid landing resolution working limits"
        );
        Ok(())
    }
    /// Reserve this aggregate BEFORE source/provider reads. This covers one
    /// serialized reader plus retained output headroom. The registry itself
    /// performs no provider IO; external provider pools remain separately charged.
    pub fn required_read_bytes(&self) -> Result<usize> {
        self.validate()?;
        // Two bounded OP/view models coexist. Two WC codec/model allowances
        // cover the retained first state and the freshness reread. Three record
        // models cover host decode, stage construction and serialization. The
        // 32x JSON/model expansion follows the capture reader's source-codec
        // admission convention; raw wire/preflight buffers are additional.
        let wc = add(mul(self.max_wc_bytes, 32)?, 4096)?;
        let record = add(mul(self.max_record_bytes.max(crate::MAX_RECORD), 32)?, 4096)?;
        let registry = add(
            add(mul(self.max_decode_bytes, 2)?, mul(wc, 2)?)?,
            add(mul(record, 3)?, add(mul(self.max_input_bytes, 4)?, 32768)?)?,
        )?;
        // Source VerifiedMetadata, independent semantic verification, retained
        // model and canonical re-encode may overlap during proof construction.
        let proof = add(
            mul(add(mul(self.max_proof_bytes, 32)?, 4096)?, 4)?,
            mul(self.max_proof_bytes, 4)?,
        )?;
        add(registry.max(proof), self.max_retained_bytes)
    }
}
#[derive(Default)]
struct Retained {
    bytes: usize,
    proofs: usize,
    proof_input_bytes: usize,
}
/// Caller-owned physical memory reservation, independent from command fences.
/// `reserved_bytes` must describe a real bounded pool reservation held by owner.
/// The same owner must follow any preceding provider metadata read. Construction
/// validates capacity before IO; simultaneous work on one admission is rejected.
/// Host records never serialize or revive this process-local ownership.
pub struct ReadAdmission {
    limits: WorkingLimits,
    _owner: Arc<dyn Any + Send + Sync>,
    retained: Mutex<Retained>,
    work: Arc<tokio::sync::Semaphore>,
}
impl std::fmt::Debug for ReadAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadAdmission").finish_non_exhaustive()
    }
}
impl ReadAdmission {
    pub fn new(
        limits: WorkingLimits,
        reserved_bytes: usize,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<Arc<Self>> {
        ensure!(
            reserved_bytes >= limits.required_read_bytes()?,
            "insufficient aggregate resolution read reservation"
        );
        Ok(Arc::new(Self {
            limits,
            _owner: owner,
            retained: Mutex::new(Retained::default()),
            work: Arc::new(tokio::sync::Semaphore::new(1)),
        }))
    }
    pub fn limits(&self) -> &WorkingLimits {
        &self.limits
    }
    fn enter(
        self: &Arc<Self>,
        proofs: &[CommitProof],
    ) -> Result<tokio::sync::OwnedSemaphorePermit> {
        ensure!(
            proofs.len() <= self.limits.max_proofs,
            "too many resolution commit proofs"
        );
        let mut bytes = 0usize;
        for proof in proofs {
            ensure!(
                Arc::ptr_eq(self, &proof.inner._lease.admission),
                "resolution proof belongs to another reservation"
            );
            bytes = add(bytes, proof.inner.input_bytes)?;
        }
        ensure!(
            bytes <= self.limits.max_proof_input_bytes,
            "aggregate resolution proof input limit exceeded"
        );
        self.work
            .clone()
            .try_acquire_owned()
            .context("resolution read reservation already in use")
    }
    fn retain(self: &Arc<Self>, bytes: usize, proof_input: Option<usize>) -> Result<Lease> {
        let mut used = self
            .retained
            .lock()
            .map_err(|_| anyhow::anyhow!("resolution reservation unavailable"))?;
        let next = add(used.bytes, bytes)?;
        ensure!(
            next <= self.limits.max_retained_bytes,
            "resolution retained result budget exhausted"
        );
        if let Some(input) = proof_input {
            ensure!(
                used.proofs < self.limits.max_proofs
                    && add(used.proof_input_bytes, input)? <= self.limits.max_proof_input_bytes,
                "aggregate resolution proof retention limit exceeded"
            );
            used.proofs += 1;
            used.proof_input_bytes += input;
        }
        used.bytes = next;
        Ok(Lease {
            admission: self.clone(),
            bytes,
            proof_input,
        })
    }
}
#[derive(Debug)]
struct Lease {
    admission: Arc<ReadAdmission>,
    bytes: usize,
    proof_input: Option<usize>,
}
impl Lease {
    fn resize(&mut self, bytes: usize) -> Result<()> {
        let mut used = self
            .admission
            .retained
            .lock()
            .map_err(|_| anyhow::anyhow!("resolution reservation unavailable"))?;
        let next = add(used.bytes - self.bytes, bytes)?;
        ensure!(
            next <= self.admission.limits.max_retained_bytes,
            "resolution retained result budget exhausted"
        );
        used.bytes = next;
        self.bytes = bytes;
        Ok(())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Ok(mut used) = self.admission.retained.lock() {
            used.bytes -= self.bytes;
            if let Some(input) = self.proof_input {
                used.proofs -= 1;
                used.proof_input_bytes -= input;
            }
        }
    }
}
fn retained_model_bytes(value: &impl Serialize) -> Result<usize> {
    add(mul(serde_json::to_vec(value)?.len(), 32)?, 4096)
}
fn tree_bytes(tree: &NativeTree) -> Result<usize> {
    let mut bytes = add(
        4096,
        add(
            mul(tree.terms.capacity(), 32)?,
            mul(tree.labels.capacity(), std::mem::size_of::<String>())?,
        )?,
    )?;
    for label in &tree.labels {
        bytes = add(bytes, label.capacity())?;
    }
    Ok(bytes)
}
/// Bounded canonical COMMIT content, not a remote admission or clean verdict.
/// Clones share immutable payload and its charged reservation.
#[derive(Clone, Debug)]
pub struct CommitProof {
    inner: Arc<Proof>,
}
#[derive(Debug)]
struct Proof {
    id: [u8; 32],
    tree: NativeTree,
    input_bytes: usize,
    _lease: Lease,
}
impl CommitProof {
    pub fn verify(
        expected: [u8; 32],
        bytes: &[u8],
        admission: &Arc<ReadAdmission>,
    ) -> Result<Self> {
        let _work = admission.enter(&[])?;
        ensure!(
            bytes.len() <= admission.limits.max_proof_bytes && expected != [0; 32],
            "resolution commit proof exceeds bounds or names the intrinsic root"
        );
        let mut lease = admission.retain(0, Some(bytes.len()))?;
        let descriptor = tinytree_model::Descriptor {
            key: tinytree_model::ObjectKey {
                kind: tinytree_model::Kind::Commit,
                id: tinytree_model::ObjectId(expected),
            },
            size_bytes: bytes.len() as u64,
            format_version: tinytree_model::FORMAT_VERSION,
        };
        let mut limits = tinytree_model::Limits::default();
        limits.max_object_bytes = admission.limits.max_proof_bytes;
        limits.max_items = 65536;
        let verified = VerifiedMetadata::verify(descriptor, bytes.to_vec(), limits)?;
        let tinytree_model::Object::Commit(commit) = verified.into_object() else {
            anyhow::bail!("resolution proof is not a native commit")
        };
        let encoded =
            tinytree_model::encode(&tinytree_model::Object::Commit(commit.clone()), limits)?;
        ensure!(
            encoded.bytes == bytes,
            "resolution commit proof is not canonical"
        );
        drop(encoded);
        let tree = model_tree(commit)?;
        lease.resize(tree_bytes(&tree)?)?;
        Ok(Self {
            inner: Arc::new(Proof {
                id: expected,
                tree,
                input_bytes: bytes.len(),
                _lease: lease,
            }),
        })
    }
    pub fn from_metadata(value: &VerifiedMetadata, admission: &Arc<ReadAdmission>) -> Result<Self> {
        ensure!(
            value.descriptor().key.kind == tinytree_model::Kind::Commit,
            "resolution metadata proof is not a commit"
        );
        Self::verify(value.descriptor().key.id.0, value.bytes(), admission)
    }
    pub fn root(admission: &Arc<ReadAdmission>) -> Result<Self> {
        let _work = admission.enter(&[])?;
        let tree = model_tree(tinytree_model::synthetic_root())?;
        let lease = admission.retain(tree_bytes(&tree)?, Some(0))?;
        Ok(Self {
            inner: Arc::new(Proof {
                id: [0; 32],
                tree,
                input_bytes: 0,
                _lease: lease,
            }),
        })
    }
    pub fn id(&self) -> [u8; 32] {
        self.inner.id
    }
}
fn model_tree(value: tinytree_model::Commit) -> Result<NativeTree> {
    let labels = jj_lib::conflict_labels::ConflictLabels::from_vec(value.root_tree.labels);
    let tree = NativeTree {
        terms: value.root_tree.terms.iter().map(|id| id.0).collect(),
        labels: labels.as_slice().to_vec(),
    };
    tree.validate()?;
    Ok(tree)
}
/// Freeze provenance and committed-result application are separate host intents.
/// Neither purpose may be inferred from portable operation tags.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "purpose", rename_all = "snake_case", deny_unknown_fields)]
pub enum Purpose {
    Freeze {
        observed_job: Option<String>,
    },
    Apply {
        job_id: String,
        candidate_head: String,
        candidate_digest: [u8; 32],
    },
}
impl Purpose {
    fn name(&self) -> &'static str {
        match self {
            Self::Freeze { .. } => "freeze",
            Self::Apply { .. } => "apply",
        }
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            match self {
                Self::Freeze { observed_job } =>
                    observed_job.as_ref().is_none_or(|id| nonroot(id, 64)),
                Self::Apply {
                    job_id,
                    candidate_head,
                    ..
                } => nonroot(job_id, 64) && nonroot(candidate_head, 64),
            },
            "invalid landing resolution purpose"
        );
        Ok(())
    }
}
/// Fresh caller-selected remote identity. Digests bind the complete canonical
/// request and candidate, not moving job status or expiry. Use domain-separated
/// BLAKE3 of bounded length-prefixed fields (u64 LE lengths/numbers): request
/// v1 includes all StartLandingRequest fields and ordered source_stack; candidate
/// v1 includes head/root_tree and canonical rewrites sorted by old_commit. The
/// domains are `tinytree.landing-resolution.request.v1` and
/// `tinytree.landing-resolution.candidate.v1`. They are equality
/// claims only: the caller must freshly look up and admit the remote job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Context {
    pub provider_identity: [u8; 32],
    pub scope: RemoteScope,
    pub request_id: String,
    pub request_digest: [u8; 32],
    pub purpose: Purpose,
}
/// Common remote equality provenance, independent from a moving job or candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommonContext {
    pub provider_identity: [u8; 32],
    pub scope: RemoteScope,
    pub request_id: String,
    pub request_digest: [u8; 32],
}
impl CommonContext {
    fn with_purpose(&self, purpose: Purpose) -> Context {
        Context {
            provider_identity: self.provider_identity,
            scope: self.scope.clone(),
            request_id: self.request_id.clone(),
            request_digest: self.request_digest,
            purpose,
        }
    }
}
/// Value-only host record presence. This is not a current-state classification,
/// proof, remote admission, or authority to repair, publish, or acknowledge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Presence {
    freeze: Option<Stage>,
    apply: Option<Stage>,
}
impl Presence {
    pub fn freeze(&self) -> Option<Stage> {
        self.freeze
    }
    pub fn apply(&self) -> Option<Stage> {
        self.apply
    }
    pub fn is_absent(&self) -> bool {
        self.freeze.is_none() && self.apply.is_none()
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedContext {
    provider_identity: [u8; 32],
    socket: PathBuf,
    store: String,
    request_id: String,
    request_digest: [u8; 32],
    purpose: Purpose,
}
impl Context {
    fn saved(&self) -> Result<SavedContext> {
        let scope = RemoteScope::new(self.scope.socket.clone(), self.scope.store.clone())?;
        ensure!(
            self.provider_identity != [0; 32] && valid_hex(&self.request_id, 32),
            "invalid landing resolution context"
        );
        self.purpose.validate()?;
        Ok(SavedContext {
            provider_identity: self.provider_identity,
            socket: scope.socket,
            store: scope.store,
            request_id: self.request_id.clone(),
            request_digest: self.request_digest,
            purpose: self.purpose.clone(),
        })
    }
}
fn nonroot(value: &str, width: usize) -> bool {
    valid_hex(value, width) && value.bytes().any(|b| b != b'0')
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BaseData {
    operation: String,
    working_copy_commit: String,
    tree: NativeTree,
    projection_generation: u64,
    state_fingerprint: String,
}
impl BaseData {
    fn validate(&self) -> Result<()> {
        ensure!(
            valid_hex(&self.operation, 128)
                && valid_hex(&self.working_copy_commit, 64)
                && valid_hex(&self.state_fingerprint, 128)
                && self.projection_generation > 0,
            "invalid landing resolution baseline"
        );
        self.tree.validate()
    }
}
/// Immutable, charged local baseline. Clones share payload and reservation;
/// portable bytes cannot deserialize or mutate process-local read ownership.
#[derive(Clone, Debug)]
pub struct Base {
    owner: BaseOwner,
}
#[derive(Clone, Debug)]
enum BaseOwner {
    Standalone(Arc<OwnedBase>),
    Journal(Arc<OwnedJournal>),
}
#[derive(Debug)]
struct OwnedBase {
    data: BaseData,
    _owner: Arc<Lease>,
}
impl Base {
    fn data(&self) -> &BaseData {
        match &self.owner {
            BaseOwner::Standalone(value) => &value.data,
            BaseOwner::Journal(value) => &value.record.base,
        }
    }
    pub fn operation(&self) -> &str {
        &self.data().operation
    }
    pub fn working_copy_commit(&self) -> &str {
        &self.data().working_copy_commit
    }
    pub fn tree(&self) -> &NativeTree {
        &self.data().tree
    }
    pub fn projection_generation(&self) -> u64 {
        self.data().projection_generation
    }
    pub fn state_fingerprint(&self) -> &str {
        &self.data().state_fingerprint
    }
}
impl PartialEq for Base {
    fn eq(&self, other: &Self) -> bool {
        self.data() == other.data()
    }
}
impl Eq for Base {}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Prepared,
    Written,
    Completed,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case", deny_unknown_fields)]
enum SavedStage {
    Prepared,
    Written {
        operation: String,
        working_copy_commit: String,
        tree: NativeTree,
    },
    Completed {
        operation: String,
        current: BaseData,
    },
}
/// Local classification only. RecoveryRequired never authorizes an operation
/// publication or projection acknowledgement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Position {
    AtBase,
    OperationWritten,
    OperationPublished,
    WorkingCopyAdvanced,
    Applied,
    Completed,
    RecoveryRequired,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    binding: Binding,
    context: SavedContext,
    base: BaseData,
    nonce: [u8; 32],
    stage: SavedStage,
}
/// Fixed-size selector for an exact local Prepared record, not mutation or
/// remote admission authority. Fresh canonical classification and Base proof
/// remain mandatory before explicit replacement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreparedNonce([u8; 32]);
impl PreparedNonce {
    pub fn nonce(&self) -> [u8; 32] {
        self.0
    }
}
/// Fixed-size exact host Freeze lineage, not recovery or remote authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FreezeSelection {
    stage: Stage,
    observed_job: Option<[u8; 32]>,
}
impl FreezeSelection {
    pub fn stage(&self) -> Stage {
        self.stage
    }
    pub fn observed_job(&self) -> Option<[u8; 32]> {
        self.observed_job
    }
}
/// Host provenance retirement only; neither result grants OP authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedRetirement {
    Retired,
    AlreadyRetired,
}
/// Small value-only selectors for canonical metadata reads. These IDs are not
/// proofs, admission, retention roots or permission to publish/acknowledge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProofIds {
    ids: [[u8; 32]; 2],
    len: usize,
}
impl ProofIds {
    pub fn ids(&self) -> &[[u8; 32]] {
        &self.ids[..self.len]
    }
}
/// Not deserializable from portable data. Every Registry method rereads the
/// host record and actual workspace; possession alone grants no authority.
#[derive(Clone, Debug)]
pub struct Journal {
    inner: Arc<OwnedJournal>,
    position: Position,
}
#[derive(Debug)]
struct OwnedJournal {
    record: Record,
    _owner: Arc<Lease>,
}
impl Journal {
    pub fn purpose(&self) -> &Purpose {
        &self.inner.record.context.purpose
    }
    pub fn nonce(&self) -> [u8; 32] {
        self.inner.record.nonce
    }
    pub fn base(&self) -> Base {
        Base {
            owner: BaseOwner::Journal(self.inner.clone()),
        }
    }
    pub fn stage(&self) -> Stage {
        match &self.inner.record.stage {
            SavedStage::Prepared => Stage::Prepared,
            SavedStage::Written { .. } => Stage::Written,
            SavedStage::Completed { .. } => Stage::Completed,
        }
    }
    pub fn position(&self) -> Position {
        self.position
    }
    pub fn owned_operation(&self) -> Option<&str> {
        match &self.inner.record.stage {
            SavedStage::Prepared => None,
            SavedStage::Written { operation, .. } | SavedStage::Completed { operation, .. } => {
                Some(operation)
            }
        }
    }
    pub fn result_working_copy_commit(&self) -> Option<&str> {
        match &self.inner.record.stage {
            SavedStage::Prepared => None,
            SavedStage::Written {
                working_copy_commit,
                ..
            } => Some(working_copy_commit),
            SavedStage::Completed { current, .. } => Some(&current.working_copy_commit),
        }
    }
    pub fn result_tree(&self) -> Option<&NativeTree> {
        match &self.inner.record.stage {
            SavedStage::Prepared => None,
            SavedStage::Written { tree, .. } => Some(tree),
            SavedStage::Completed { current, .. } => Some(&current.tree),
        }
    }
    /// Add these exact attributes to a newly written operation. Matching raw
    /// tags alone never substitute for the host-created journal.
    pub fn operation_attributes(&self) -> BTreeMap<String, String> {
        attributes(&self.inner.record)
    }
}
fn attributes(record: &Record) -> BTreeMap<String, String> {
    let mut attributes = BTreeMap::from([
        (
            "tinytree.landing.resolution_nonce".into(),
            hex(&record.nonce),
        ),
        (
            "tinytree.landing.resolution_purpose".into(),
            record.context.purpose.name().into(),
        ),
        (
            "tinytree.landing.request".into(),
            record.context.request_id.clone(),
        ),
        (
            "tinytree.landing.resolution_request".into(),
            hex(&record.context.request_digest),
        ),
    ]);
    match &record.context.purpose {
        Purpose::Freeze { observed_job } => {
            if let Some(job) = observed_job {
                attributes.insert("tinytree.landing.job".into(), job.clone());
            }
        }
        Purpose::Apply {
            job_id,
            candidate_head,
            candidate_digest,
        } => {
            attributes.insert("tinytree.landing.job".into(), job_id.clone());
            attributes.insert("tinytree.landing.candidate".into(), candidate_head.clone());
            attributes.insert(
                "tinytree.landing.resolution_candidate".into(),
                hex(candidate_digest),
            );
        }
    }
    attributes
}
fn journal_name(context: &SavedContext) -> String {
    format!("{}-{}.json", context.request_id, context.purpose.name())
}
struct Snapshot {
    base: BaseData,
    pending: bool,
    heads: Vec<String>,
}
impl Registry {
    async fn resolution_lock(&self) -> Result<File> {
        let file = self.root.open_existing_file("registry.lock", 4096)?;
        tokio::task::spawn_blocking(move || {
            file.lock_exclusive()?;
            Ok::<_, anyhow::Error>(file)
        })
        .await?
    }
    fn resolution_binding(&self, binding: &Binding) -> Result<()> {
        ensure!(
            self.lookup(&binding.root)? == *binding,
            "landing resolution workspace binding changed"
        );
        Ok(())
    }
    fn resolution_directory(&self, binding: &Binding, create: bool) -> Result<Option<PrivateDir>> {
        let path = self.root.path().join(DIRECTORY);
        let root = if create {
            let root = PrivateDir::create(path)?;
            File::open(self.root.path())?.sync_all()?;
            root
        } else {
            match PrivateDir::open_existing(path) {
                Ok(value) => value,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        };
        let path = root.path().join(binding.id());
        let directory = if create {
            let directory = PrivateDir::create(path)?;
            File::open(root.path())?.sync_all()?;
            directory
        } else {
            match PrivateDir::open_existing(path) {
                Ok(value) => value,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        };
        Ok(Some(directory))
    }
    fn resolution_record(
        &self,
        binding: &Binding,
        context: &SavedContext,
        admission: &Arc<ReadAdmission>,
    ) -> Result<Option<(PrivateDir, Record)>> {
        let Some(directory) = self.resolution_directory(binding, false)? else {
            return Ok(None);
        };
        let bytes = match directory.read(journal_name(context), admission.limits.max_record_bytes) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let record: Record = serde_json::from_slice(&bytes)?;
        validate_record(binding, context, &record)?;
        Ok(Some((directory, record)))
    }
    /// Probe both exact host-purpose files without canonical source hydration.
    /// Stored purpose fields are validated, not supplied by a captured hint or
    /// inferred from the fresh terminal job. Any present record still requires
    /// full exact-context load and canonical proofs before reconciliation.
    pub async fn landing_resolution_presence(
        &self,
        binding: &Binding,
        context: &CommonContext,
        admission: &Arc<ReadAdmission>,
    ) -> Result<Presence> {
        let _work = admission.enter(&[])?;
        let common = context
            .with_purpose(Purpose::Freeze { observed_job: None })
            .saved()?;
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        let mut result = Presence {
            freeze: None,
            apply: None,
        };
        let Some(directory) = self.resolution_directory(binding, false)? else {
            return Ok(result);
        };
        for (name, slot) in [("freeze", &mut result.freeze), ("apply", &mut result.apply)] {
            let bytes = match directory.read(
                format!("{}-{name}.json", common.request_id),
                admission.limits.max_record_bytes,
            ) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let record: Record = serde_json::from_slice(&bytes)?;
            validate_common_record(binding, context, &record, name)?;
            *slot = Some(match record.stage {
                SavedStage::Prepared => Stage::Prepared,
                SavedStage::Written { .. } => Stage::Written,
                SavedStage::Completed { .. } => Stage::Completed,
            });
            // No decoded record or wire buffer survives the next purpose read.
        }
        Ok(result)
    }
    /// Select only exact host Freeze lineage, retaining a legitimate pre-Start
    /// None rather than inferring it from moving job status or captured hints.
    /// A selected job must match the caller's fresh canonical job. Full exact-
    /// Context/proof loading is still required for any reconciliation.
    pub async fn freeze_landing_resolution_selection(
        &self,
        binding: &Binding,
        context: &CommonContext,
        admission: &Arc<ReadAdmission>,
    ) -> Result<Option<FreezeSelection>> {
        let _work = admission.enter(&[])?;
        let common = context
            .with_purpose(Purpose::Freeze { observed_job: None })
            .saved()?;
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        let Some(directory) = self.resolution_directory(binding, false)? else {
            return Ok(None);
        };
        let bytes = match directory.read(journal_name(&common), admission.limits.max_record_bytes) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let record: Record = serde_json::from_slice(&bytes)?;
        validate_common_record(binding, context, &record, "freeze")?;
        let Purpose::Freeze { observed_job } = &record.context.purpose else {
            unreachable!()
        };
        let observed_job = observed_job
            .as_deref()
            .map(|value| -> Result<[u8; 32]> {
                let mut id = [0; 32];
                for (slot, pair) in id.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
                    *slot = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
                }
                Ok(id)
            })
            .transpose()?;
        Ok(Some(FreezeSelection {
            stage: match record.stage {
                SavedStage::Prepared => Stage::Prepared,
                SavedStage::Written { .. } => Stage::Written,
                SavedStage::Completed { .. } => Stage::Completed,
            },
            observed_job,
        }))
    }
    /// Caller must hold its native command/frontend mutation fence across the
    /// transaction. Registry locking serializes journals, not editor writes.
    /// If normal snapshot/recovery ran first, obtain a new actual Base and
    /// revalidate the fresh context before beginning any landing transaction.
    pub async fn landing_resolution_base(
        &self,
        binding: &Binding,
        proofs: &[CommitProof],
        admission: &Arc<ReadAdmission>,
    ) -> Result<Base> {
        let _work = admission.enter(proofs)?;
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        let snapshot = read_snapshot(binding, proofs, admission).await?;
        ensure!(
            !snapshot.pending && snapshot.heads == [snapshot.base.operation.clone()],
            "landing resolution requires one settled WC operation/head"
        );
        let owner = Arc::new(admission.retain(retained_model_bytes(&snapshot.base)?, None)?);
        Ok(Base {
            owner: BaseOwner::Standalone(Arc::new(OwnedBase {
                data: snapshot.base,
                _owner: owner,
            })),
        })
    }
    pub async fn begin_landing_resolution(
        &self,
        binding: &Binding,
        context: &Context,
        base: &Base,
        proofs: &[CommitProof],
        admission: &Arc<ReadAdmission>,
    ) -> Result<Journal> {
        let _work = admission.enter(proofs)?;
        let context = context.saved()?;
        let base = base.data();
        base.validate()?;
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        if let Some((_, record)) = self.resolution_record(binding, &context, admission)? {
            ensure!(
                record.base == *base,
                "landing resolution baseline differs; preserve journal"
            );
            return classify(binding, record, proofs, admission).await;
        }
        let snapshot = read_snapshot(binding, proofs, admission).await?;
        ensure!(
            !snapshot.pending
                && snapshot.base == *base
                && snapshot.heads == [base.operation.clone()],
            "landing resolution baseline changed before begin"
        );
        let directory = self.resolution_directory(binding, true)?.unwrap();
        ensure!(
            fs::read_dir(directory.path())?
                .take(MAX_JOURNALS + 1)
                .collect::<std::io::Result<Vec<_>>>()?
                .len()
                < MAX_JOURNALS,
            "landing resolution journal limit reached"
        );
        let mut nonce = [0; 32];
        getrandom::fill(&mut nonce)
            .map_err(|_| anyhow::anyhow!("landing resolution nonce unavailable"))?;
        ensure!(nonce != [0; 32], "landing resolution nonce unavailable");
        let record = Record {
            version: 1,
            binding: binding.clone(),
            context,
            base: base.clone(),
            nonce,
            stage: SavedStage::Prepared,
        };
        save(&directory, &record, admission)?;
        let owner = Arc::new(admission.retain(retained_model_bytes(&record)?, None)?);
        Ok(Journal {
            inner: Arc::new(OwnedJournal {
                record,
                _owner: owner,
            }),
            position: Position::AtBase,
        })
    }
    /// Select an exact Prepared nonce even if CURRENT has advanced normally.
    /// This reads no OP/view/provider data and does not classify the stale base.
    /// Missing, corrupt, mismatched, or non-Prepared records fail closed.
    pub async fn prepared_landing_resolution_nonce(
        &self,
        binding: &Binding,
        context: &Context,
        admission: &Arc<ReadAdmission>,
    ) -> Result<PreparedNonce> {
        let _work = admission.enter(&[])?;
        let context = context.saved()?;
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        let (_, record) = self
            .resolution_record(binding, &context, admission)?
            .context("landing resolution journal missing")?;
        ensure!(
            matches!(record.stage, SavedStage::Prepared),
            "resolution attempt is not prepared"
        );
        Ok(PreparedNonce(record.nonce))
    }
    /// Explicitly abandon an exact Prepared attempt after a fresh admitted
    /// snapshot. The caller must reclassify canonical source/destination guards
    /// against that fresh state and retain its command fence. No operation from
    /// the superseded nonce is adopted; Written/Completed attempts never replace.
    /// Archive-only crashes leave the old active attempt intact. After a swap,
    /// callers must reload: replaying the superseded nonce fails closed.
    pub async fn replace_prepared_landing_resolution(
        &self,
        binding: &Binding,
        context: &Context,
        prior_nonce: [u8; 32],
        base: &Base,
        proofs: &[CommitProof],
        admission: &Arc<ReadAdmission>,
    ) -> Result<Journal> {
        let _work = admission.enter(proofs)?;
        let context = context.saved()?;
        let base = base.data();
        base.validate()?;
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        let (directory, mut record) = self
            .resolution_record(binding, &context, admission)?
            .context("landing resolution journal missing")?;
        ensure!(
            record.nonce == prior_nonce && matches!(record.stage, SavedStage::Prepared),
            "only the exact prepared resolution attempt may be replaced"
        );
        let snapshot = read_snapshot(binding, proofs, admission).await?;
        ensure!(
            !snapshot.pending
                && snapshot.base == *base
                && snapshot.heads == [base.operation.clone()],
            "replacement resolution baseline changed"
        );
        drop(snapshot);
        let original = directory.read(journal_name(&context), admission.limits.max_record_bytes)?;
        let archive = format!(
            "{}-{}-{}-superseded.json",
            context.request_id,
            context.purpose.name(),
            hex(&prior_nonce)
        );
        archive_record(
            &directory,
            &archive,
            &original,
            admission.limits.max_record_bytes,
        )?;
        drop(original);
        let mut nonce = [0; 32];
        getrandom::fill(&mut nonce)
            .map_err(|_| anyhow::anyhow!("landing resolution nonce unavailable"))?;
        ensure!(
            nonce != [0; 32] && nonce != prior_nonce,
            "landing resolution nonce unavailable"
        );
        record.base = base.clone();
        record.nonce = nonce;
        save(&directory, &record, admission)?;
        let owner = Arc::new(admission.retain(retained_model_bytes(&record)?, None)?);
        Ok(Journal {
            inner: Arc::new(OwnedJournal {
                record,
                _owner: owner,
            }),
            position: Position::AtBase,
        })
    }
    /// Retire only exact Prepared freeze provenance when the caller freshly
    /// proves no freeze is needed. Keep the frontend fence through this call;
    /// this performs no canonical source classification or operation recovery.
    /// An absent active record is already retired only with its exact validated
    /// retirement archive. Archives are never deleted or used as OP authority.
    pub async fn retire_prepared_freeze_resolution(
        &self,
        binding: &Binding,
        context: &Context,
        nonce: [u8; 32],
        admission: &Arc<ReadAdmission>,
    ) -> Result<PreparedRetirement> {
        let _work = admission.enter(&[])?;
        let context = context.saved()?;
        ensure!(
            matches!(context.purpose, Purpose::Freeze { .. }),
            "only prepared freeze provenance may retire"
        );
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        let directory = self
            .resolution_directory(binding, false)?
            .context("landing resolution journal missing")?;
        let active = journal_name(&context);
        let archive = format!("{}-freeze-{}-retired.json", context.request_id, hex(&nonce));
        let (bytes, result) = match directory.read(&active, admission.limits.max_record_bytes) {
            Ok(bytes) => (bytes, PreparedRetirement::Retired),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (
                directory.read(&archive, admission.limits.max_record_bytes)?,
                PreparedRetirement::AlreadyRetired,
            ),
            Err(e) => return Err(e.into()),
        };
        let record: Record = serde_json::from_slice(&bytes)?;
        validate_record(binding, &context, &record)?;
        ensure!(
            record.nonce == nonce && matches!(record.stage, SavedStage::Prepared),
            "only the exact prepared freeze attempt may retire"
        );
        drop(record);
        archive_record(
            &directory,
            &archive,
            &bytes,
            admission.limits.max_record_bytes,
        )?;
        if result == PreparedRetirement::Retired {
            fs::remove_file(directory.path().join(active))?;
        }
        // This barrier is also required after a previous remove succeeded but
        // its parent sync failed; absence alone never acknowledges retirement.
        File::open(directory.path())?.sync_all()?;
        Ok(result)
    }
    /// Select only the actual WC operation's commit and an exact locally owned
    /// result. This breaks the metadata bootstrap cycle after record-before-
    /// sidecar crashes; callers must still obtain canonical CommitProofs and
    /// freshly load/classify the journal. No archived operation scan occurs.
    pub async fn resolution_proof_ids(
        &self,
        binding: &Binding,
        context: &Context,
        admission: &Arc<ReadAdmission>,
    ) -> Result<ProofIds> {
        let _work = admission.enter(&[])?;
        let context = context.saved()?;
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        let record = self.resolution_record(binding, &context, admission)?;
        operation_store(binding)?;
        let (state, projection) = jj_vfs::native::read_retained_state_for_capture(
            &binding.projection(),
            admission.limits.max_wc_bytes,
        )?;
        let heads =
            jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(&binding.repo.join("op_heads"))
                .get_op_heads_bounded(2)?;
        let repo = PrivateDir::open_existing(&binding.repo)?;
        for head in &heads {
            ensure!(
                repo.read_descendant(PathBuf::from("op_heads/heads").join(head.hex()), 1)?
                    .is_empty(),
                "invalid resolution operation head marker"
            );
        }
        let (_, view) = read_operation(binding, &state.operation.hex(), &admission.limits)?;
        let current = view
            .wc_commit_ids
            .get(&state.workspace)
            .context("resolution WC operation has no workspace commit")?
            .as_bytes()
            .try_into()?;
        drop(view);
        let mut ids = ProofIds {
            ids: [current, [0; 32]],
            len: 1,
        };
        if let Some((_, record)) = &record {
            if let Some(owned) =
                owned_operation_commit(binding, record, &state.workspace, admission)?
            {
                let owned = jj_lib::backend::CommitId::try_from_hex(&owned)
                    .context("invalid owned resolution WC commit")?;
                let owned = owned.as_bytes().try_into()?;
                if owned != current {
                    ids.ids[1] = owned;
                    ids.len = 2;
                }
            }
        }
        // Both selectors belong to this exact local read, never a moving head.
        let (after, after_projection) = jj_vfs::native::read_retained_state_for_capture(
            &binding.projection(),
            admission.limits.max_wc_bytes,
        )?;
        ensure!(
            after.state_fingerprint == state.state_fingerprint
                && after_projection == projection
                && jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(
                    &binding.repo.join("op_heads")
                )
                .get_op_heads_bounded(2)?
                    == heads,
            "resolution workspace changed while selecting canonical proofs"
        );
        Ok(ids)
    }
    pub async fn load_landing_resolution(
        &self,
        binding: &Binding,
        context: &Context,
        proofs: &[CommitProof],
        admission: &Arc<ReadAdmission>,
    ) -> Result<Option<Journal>> {
        let _work = admission.enter(proofs)?;
        let context = context.saved()?;
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        let Some((_, record)) = self.resolution_record(binding, &context, admission)? else {
            return Ok(None);
        };
        Ok(Some(classify(binding, record, proofs, admission).await?))
    }
    /// Record exactly one freshly written OP before checkout or OP-head
    /// publication. This never searches archived operations for matching tags.
    pub async fn record_landing_resolution_operation(
        &self,
        binding: &Binding,
        context: &Context,
        nonce: [u8; 32],
        operation: &str,
        proofs: &[CommitProof],
        admission: &Arc<ReadAdmission>,
    ) -> Result<Journal> {
        let _work = admission.enter(proofs)?;
        let context = context.saved()?;
        ensure!(
            nonroot(operation, 128),
            "invalid resolution operation identity"
        );
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        let (directory, mut record) = self
            .resolution_record(binding, &context, admission)?
            .context("landing resolution journal missing")?;
        ensure!(record.nonce == nonce, "landing resolution nonce differs");
        if let SavedStage::Written {
            operation: owned, ..
        }
        | SavedStage::Completed {
            operation: owned, ..
        } = &record.stage
        {
            ensure!(
                owned == operation,
                "another resolution operation is already owned; preserve it"
            );
            return classify(binding, record, proofs, admission).await;
        }
        // Before recording ownership, publication/checkout must not have moved.
        let current = read_snapshot(binding, proofs, admission).await?;
        ensure!(
            !current.pending
                && current.base == record.base
                && current.heads == [record.base.operation.clone()],
            "landing resolution changed before operation ownership was recorded"
        );
        let (raw, view) = read_operation(binding, operation, &admission.limits)?;
        ensure!(
            raw.parents.len() == 1
                && raw.parents[0].hex() == record.base.operation
                && raw.commit_predecessors.is_some()
                && attributes(&record)
                    .iter()
                    .all(|(key, value)| raw.metadata.attributes.get(key) == Some(value)),
            "resolution operation differs from host attempt"
        );
        let commit = view
            .wc_commit_ids
            .get(&jj_lib::ref_name::WorkspaceNameBuf::from(
                current_workspace(binding, &admission.limits)?,
            ))
            .context("resolution operation lacks the current workspace")?
            .hex();
        let tree = commit_tree(proofs, &commit)?;
        drop(raw);
        drop(view);
        drop(current);
        let unchanged = read_snapshot(binding, proofs, admission).await?;
        ensure!(
            !unchanged.pending
                && unchanged.base == record.base
                && unchanged.heads == [record.base.operation.clone()],
            "resolution workspace changed before recording operation"
        );
        drop(unchanged);
        record.stage = SavedStage::Written {
            operation: operation.into(),
            working_copy_commit: commit,
            tree,
        };
        save(&directory, &record, admission)?;
        classify(binding, record, proofs, admission).await
    }
    pub async fn complete_landing_resolution(
        &self,
        binding: &Binding,
        context: &Context,
        nonce: [u8; 32],
        proofs: &[CommitProof],
        admission: &Arc<ReadAdmission>,
    ) -> Result<Journal> {
        let _work = admission.enter(proofs)?;
        let context = context.saved()?;
        let _lock = self.resolution_lock().await?;
        self.resolution_binding(binding)?;
        let (directory, mut record) = self
            .resolution_record(binding, &context, admission)?
            .context("landing resolution journal missing")?;
        ensure!(record.nonce == nonce, "landing resolution nonce differs");
        let checked = classify(binding, record.clone(), proofs, admission).await?;
        ensure!(
            matches!(checked.position, Position::Applied | Position::Completed),
            "resolution operation has not completed exact WC/head publication"
        );
        if matches!(record.stage, SavedStage::Completed { .. }) {
            return Ok(checked);
        }
        let snapshot = read_snapshot(binding, proofs, admission).await?;
        let current = snapshot.base;
        let operation = checked.owned_operation().unwrap().to_owned();
        let SavedStage::Written {
            working_copy_commit,
            tree,
            ..
        } = &record.stage
        else {
            unreachable!()
        };
        ensure!(
            !snapshot.pending
                && snapshot.heads == [operation.clone()]
                && current.operation == operation
                && current.working_copy_commit == *working_copy_commit
                && current.tree == *tree,
            "resolution WC changed during completion"
        );
        record.stage = SavedStage::Completed { operation, current };
        save(&directory, &record, admission)?;
        classify(binding, record, proofs, admission).await
    }
}
fn archive_record(
    directory: &PrivateDir,
    archive: &str,
    bytes: &[u8],
    max_bytes: usize,
) -> Result<()> {
    match directory.read(archive, max_bytes) {
        Ok(existing) => ensure!(
            existing == bytes,
            "superseded resolution record differs; preserve it"
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            ensure!(
                fs::read_dir(directory.path())?
                    .take(MAX_JOURNALS + 1)
                    .collect::<std::io::Result<Vec<_>>>()?
                    .len()
                    < MAX_JOURNALS,
                "landing resolution journal limit reached"
            );
        }
        Err(e) => return Err(e.into()),
    }
    // Atomic, durable archival precedes replacement. An identical retry
    // reestablishes the archive barrier without adopting any other record.
    directory.replace(archive, bytes)?;
    Ok(())
}
fn validate_common_record(
    binding: &Binding,
    context: &CommonContext,
    record: &Record,
    purpose: &str,
) -> Result<()> {
    let expected = context
        .with_purpose(record.context.purpose.clone())
        .saved()?;
    ensure!(
        record.context.purpose.name() == purpose,
        "resolution purpose file differs"
    );
    validate_record(binding, &expected, record)
}
fn validate_record(binding: &Binding, context: &SavedContext, record: &Record) -> Result<()> {
    ensure!(
        record.version == 1
            && record.binding == *binding
            && record.context == *context
            && record.nonce != [0; 32],
        "landing resolution journal context differs; preserve it"
    );
    record.base.validate()?;
    match &record.stage {
        SavedStage::Prepared => {}
        SavedStage::Written {
            operation,
            working_copy_commit,
            tree,
        } => {
            ensure!(
                nonroot(operation, 128)
                    && operation != &record.base.operation
                    && valid_hex(working_copy_commit, 64),
                "invalid written resolution operation"
            );
            tree.validate()?;
        }
        SavedStage::Completed { operation, current } => {
            current.validate()?;
            ensure!(
                nonroot(operation, 128)
                    && current.operation == *operation
                    && operation != &record.base.operation,
                "invalid completed resolution operation"
            );
        }
    }
    Ok(())
}
fn save(directory: &PrivateDir, record: &Record, admission: &Arc<ReadAdmission>) -> Result<()> {
    let bytes = serde_json::to_vec(record)?;
    ensure!(
        bytes.len() <= admission.limits.max_record_bytes,
        "landing resolution journal exceeds limit"
    );
    directory.replace(journal_name(&record.context), &bytes)?;
    Ok(())
}
fn current_workspace(
    binding: &Binding,
    limits: &WorkingLimits,
) -> Result<jj_lib::ref_name::WorkspaceNameBuf> {
    let (state, _) = jj_vfs::native::read_retained_state_for_capture(
        &binding.projection(),
        limits.max_wc_bytes,
    )?;
    Ok(state.workspace)
}
fn operation_store(binding: &Binding) -> Result<SimpleOpStore> {
    let repo = PrivateDir::open_existing(&binding.repo)?;
    ensure!(
        repo.read_descendant("op_store/type", 1024)? == SimpleOpStore::NAME.as_bytes()
            && repo.read_descendant("op_heads/type", 1024)?
                == jj_lib::simple_op_heads_store::SimpleOpHeadsStore::NAME.as_bytes()
            && repo.read_descendant("store/type", 1024)?
                == jj_native_backend::NativeBackend::NAME.as_bytes(),
        "unsupported resolution repository stores"
    );
    Ok(SimpleOpStore::load(
        &binding.repo.join("op_store"),
        RootOperationData {
            root_commit_id: jj_lib::backend::CommitId::from_bytes(&[0; 32]),
        },
    ))
}
fn read_operation(
    binding: &Binding,
    id: &str,
    limits: &WorkingLimits,
) -> Result<(jj_lib::op_store::Operation, jj_lib::op_store::View)> {
    ensure!(valid_hex(id, 128), "invalid resolution OP identity");
    let id = OperationId::try_from_hex(id).context("invalid resolution operation")?;
    let store = operation_store(binding)?;
    let repo = PrivateDir::open_existing(&binding.repo)?;
    if id.as_bytes() != [0; 64] {
        repo.read_descendant(
            PathBuf::from("op_store/operations").join(id.hex()),
            limits.max_input_bytes,
        )?;
    }
    let operation =
        store.read_operation_bounded(&id, limits.max_input_bytes, limits.max_decode_bytes)?;
    ensure!(
        id.as_bytes() == [0; 64]
            || jj_lib::content_hash::blake2b_hash(&operation).as_slice() == id.as_bytes(),
        "resolution OP semantic identity differs"
    );
    if operation.view_id.as_bytes() != [0; 64] {
        repo.read_descendant(
            PathBuf::from("op_store/views").join(operation.view_id.hex()),
            limits.max_input_bytes,
        )?;
    }
    let view = store.read_view_bounded(
        &operation.view_id,
        limits.max_input_bytes,
        limits.max_decode_bytes,
    )?;
    ensure!(
        operation.view_id.as_bytes() == [0; 64]
            || jj_lib::content_hash::blake2b_hash(&view).as_slice() == operation.view_id.as_bytes(),
        "resolution view semantic identity differs"
    );
    Ok((operation, view))
}
fn commit_tree(proofs: &[CommitProof], commit: &str) -> Result<NativeTree> {
    ensure!(proofs.len() <= 2, "too many resolution commit proofs");
    let id =
        jj_lib::backend::CommitId::try_from_hex(commit).context("invalid resolution WC commit")?;
    if id.as_bytes() == [0; 32] {
        return model_tree(tinytree_model::synthetic_root());
    }
    proofs
        .iter()
        .find(|proof| proof.inner.id.as_slice() == id.as_bytes())
        .map(|proof| proof.inner.tree.clone())
        .context("canonical resolution WC commit proof missing")
}
async fn read_snapshot(
    binding: &Binding,
    proofs: &[CommitProof],
    admission: &Arc<ReadAdmission>,
) -> Result<Snapshot> {
    operation_store(binding)?;
    let (state, projection) = jj_vfs::native::read_retained_state_for_capture(
        &binding.projection(),
        admission.limits.max_wc_bytes,
    )?;
    let heads =
        jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(&binding.repo.join("op_heads"))
            .get_op_heads_bounded(2)?;
    let repo = PrivateDir::open_existing(&binding.repo)?;
    for head in &heads {
        ensure!(
            repo.read_descendant(PathBuf::from("op_heads/heads").join(head.hex()), 1)?
                .is_empty(),
            "invalid resolution operation head marker"
        );
    }
    ensure!(
        !heads.is_empty(),
        "resolution requires retained local operation heads"
    );
    let (_, view) = read_operation(binding, &state.operation.hex(), &admission.limits)?;
    let commit = view
        .wc_commit_ids
        .get(&state.workspace)
        .context("resolution WC operation has no workspace commit")?
        .hex();
    drop(view);
    let tree = commit_tree(proofs, &commit)?;
    ensure!(
        state.pending || tree == projection.tree,
        "resolution WC commit/projection tree differs"
    );
    let (after, after_projection) = jj_vfs::native::read_retained_state_for_capture(
        &binding.projection(),
        admission.limits.max_wc_bytes,
    )?;
    ensure!(
        after.state_fingerprint == state.state_fingerprint
            && after_projection == projection
            && jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(
                &binding.repo.join("op_heads")
            )
            .get_op_heads_bounded(2)?
                == heads,
        "resolution workspace changed while reading baseline"
    );
    let base = BaseData {
        operation: state.operation.hex(),
        working_copy_commit: commit,
        tree: projection.tree,
        projection_generation: projection.generation,
        state_fingerprint: hex(&state.state_fingerprint),
    };
    base.validate()?;
    Ok(Snapshot {
        base,
        pending: state.pending,
        heads: heads.iter().map(|id| id.hex()).collect(),
    })
}
fn owned_operation_commit(
    binding: &Binding,
    record: &Record,
    workspace: &jj_lib::ref_name::WorkspaceNameBuf,
    admission: &Arc<ReadAdmission>,
) -> Result<Option<String>> {
    let (operation, expected) = match &record.stage {
        SavedStage::Prepared => return Ok(None),
        SavedStage::Written {
            operation,
            working_copy_commit,
            ..
        } => (operation, working_copy_commit),
        SavedStage::Completed { operation, current } => (operation, &current.working_copy_commit),
    };
    let (raw, view) = read_operation(binding, operation, &admission.limits)?;
    ensure!(
        raw.parents.len() == 1
            && raw.parents[0].hex() == record.base.operation
            && raw.commit_predecessors.is_some()
            && attributes(record)
                .iter()
                .all(|(key, value)| raw.metadata.attributes.get(key) == Some(value)),
        "owned resolution operation differs from host attempt"
    );
    let commit = view
        .wc_commit_ids
        .get(workspace)
        .context("owned resolution operation lacks the current workspace")?
        .hex();
    ensure!(
        commit == *expected,
        "owned resolution operation result differs"
    );
    Ok(Some(commit))
}
async fn classify(
    binding: &Binding,
    record: Record,
    proofs: &[CommitProof],
    admission: &Arc<ReadAdmission>,
) -> Result<Journal> {
    let current = read_snapshot(binding, proofs, admission).await?;
    let result = match &record.stage {
        SavedStage::Prepared => None,
        SavedStage::Written {
            operation,
            working_copy_commit,
            tree,
        } => Some((operation, working_copy_commit, tree)),
        SavedStage::Completed { operation, current } => {
            Some((operation, &current.working_copy_commit, &current.tree))
        }
    };
    if let Some((_, working_copy_commit, tree)) = result {
        let workspace = current_workspace(binding, &admission.limits)?;
        ensure!(
            owned_operation_commit(binding, &record, &workspace, admission)?.as_deref()
                == Some(working_copy_commit.as_str())
                && commit_tree(proofs, working_copy_commit)? == *tree,
            "owned resolution operation result differs"
        );
    }
    let base = &record.base;
    let position = match &record.stage {
        SavedStage::Prepared => {
            ensure!(
                !current.pending
                    && current.base == *base
                    && current.heads == [base.operation.clone()],
                "prepared resolution workspace changed"
            );
            Position::AtBase
        }
        SavedStage::Written {
            operation,
            working_copy_commit,
            tree,
        } => {
            ensure!(
                current
                    .heads
                    .iter()
                    .all(|head| head == &base.operation || head == operation),
                "resolution head is unrelated to owned operation"
            );
            if current.pending || current.heads.len() != 1 {
                ensure!(
                    (current.base.operation == base.operation
                        || current.base.operation == *operation)
                        && (current.base.tree == base.tree || current.base.tree == *tree),
                    "unrecognized resolution recovery state"
                );
                Position::RecoveryRequired
            } else if current.base == *base {
                if current.heads == [operation.clone()] {
                    Position::OperationPublished
                } else {
                    Position::OperationWritten
                }
            } else {
                ensure!(
                    current.base.operation == *operation
                        && current.base.working_copy_commit == *working_copy_commit
                        && current.base.tree == *tree
                        && current.base.projection_generation >= base.projection_generation,
                    "resolution WC is not the exact owned result"
                );
                if current.heads == [operation.clone()] {
                    Position::Applied
                } else {
                    Position::WorkingCopyAdvanced
                }
            }
        }
        SavedStage::Completed {
            operation,
            current: expected,
        } => {
            ensure!(
                !current.pending
                    && current.base == *expected
                    && current.heads == [operation.clone()],
                "completed resolution workspace changed"
            );
            Position::Completed
        }
    };
    let owner = Arc::new(admission.retain(retained_model_bytes(&record)?, None)?);
    Ok(Journal {
        inner: Arc::new(OwnedJournal {
            record,
            _owner: owner,
        }),
        position,
    })
}
