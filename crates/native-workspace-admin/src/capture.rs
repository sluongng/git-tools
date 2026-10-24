//! Provider-free local capture candidates. Tree freeze is NOT a portable
//! frontend-quiescence attestation, a cloud receipt, or permission to make Clean.
use crate::remote_state::valid_hex;
use crate::{Binding, Registry, remote_state};
use anyhow::{Context, Result, bail, ensure};
use jj_lib::{
    object_id::ObjectId as _,
    op_store::{OpStore, OperationId, RootOperationData},
    operation::Operation,
    simple_op_heads_store::SimpleOpHeadsStore,
    simple_op_store::SimpleOpStore,
};
use jj_native_backend::{DirectObjectIo, NativeObjectIo};
use jj_native_index::{
    ExportArtifact, IndexArtifactSink, LocalIndexExportReader, PortableExportLimits,
    PortableIndexExport,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tinytree_cloud as c;
use tinytree_model as m;

type Owner = Arc<dyn std::any::Any + Send + Sync>;
#[derive(Clone, Debug)]
pub struct CaptureLimits {
    pub max_operations: usize,
    pub max_parent_edges: usize,
    pub max_roots: usize,
    pub max_file_bytes: usize,
    pub max_decode_bytes: usize,
    pub max_working_bytes: usize,
    pub max_capsule_bytes: usize,
    pub max_elapsed_ms: u64,
    pub export: PortableExportLimits,
    pub index: jj_native_index::Limits,
}
impl Default for CaptureLimits {
    fn default() -> Self {
        Self {
            max_operations: 1024,
            max_parent_edges: 100_000,
            max_roots: 100_000,
            max_file_bytes: 4 * 1024 * 1024,
            max_decode_bytes: 64 * 1024 * 1024,
            max_working_bytes: 1024 * 1024 * 1024,
            max_capsule_bytes: 8 * 1024 * 1024,
            max_elapsed_ms: 30_000,
            export: Default::default(),
            index: Default::default(),
        }
    }
}
impl CaptureLimits {
    fn validate(&self) -> Result<()> {
        ensure!(
            LocalIndexExportReader::required_read_bytes(&self.index)?
                <= self.export.max_working_bytes,
            "capture index read peak exceeds its admitted export pool"
        );
        ensure!(
            (1..=65_537).contains(&self.max_operations)
                && (1..=1_000_000).contains(&self.max_parent_edges)
                && (1..=1_000_000).contains(&self.max_roots)
                && (1024..=64 * 1024 * 1024).contains(&self.max_file_bytes)
                && self.max_decode_bytes >= self.max_file_bytes
                && self.max_decode_bytes <= 1024 * 1024 * 1024
                && (1024..=64 * 1024 * 1024).contains(&self.max_capsule_bytes)
                && (1..=86_400_000).contains(&self.max_elapsed_ms),
            "invalid capture bounds"
        );
        // Four independent decode ceilings cover retained WC/sidecar state plus
        // operation+view inputs, or the duplicate private-state freshness reads.
        // Sink payloads have their own escaping reservation; index export has
        // its separate declared pool included below.
        let reserved = self
            .max_operations
            .checked_mul(1024)
            .and_then(|n| n.checked_add(self.max_parent_edges.checked_mul(256)?))
            .and_then(|n| n.checked_add(self.max_roots.checked_mul(128)?))
            .and_then(|n| n.checked_add(self.max_decode_bytes.checked_mul(4)?))
            .and_then(|n| n.checked_add(self.max_capsule_bytes.checked_mul(2)?))
            .and_then(|n| n.checked_add(self.export.artifacts.max_artifacts.checked_mul(512)?))
            .and_then(|n| n.checked_add(self.export.max_working_bytes))
            .context("capture admission overflow")?;
        ensure!(
            reserved <= self.max_working_bytes,
            "capture configured work exceeds its budget"
        );
        Ok(())
    }
}
/// Owns an ACTUAL Tree freeze, not a caller-provided pristine boolean. This only
/// authorizes candidate reads. Final quiescent capsule sealing additionally
/// requires the host/controller/frontend proof, which is deliberately absent.
enum CaptureFreeze {
    Tree {
        snapshot: output_core::SourceDeltaSnapshot,
        projection: jj_vfs::native::ProjectionState,
    },
    Host(native_vfs_controller::ControllerCapture),
}
pub struct TreeFreezePermit {
    source: CaptureFreeze,
    _owner: Owner,
}
impl TreeFreezePermit {
    pub fn from_snapshot(
        snapshot: output_core::SourceDeltaSnapshot,
        expected_projection: jj_vfs::native::ProjectionState,
        owner: Owner,
    ) -> Result<Self> {
        ensure!(
            snapshot.entries().is_empty() && snapshot.changes().is_empty(),
            "dirty source cannot produce capture candidate"
        );
        expected_projection.validate(&expected_projection.repository)?;
        Ok(Self {
            source: CaptureFreeze::Tree {
                snapshot,
                projection: expected_projection,
            },
            _owner: owner,
        })
    }
    pub fn from_controller(
        capture: native_vfs_controller::ControllerCapture,
        owner: Owner,
    ) -> Result<Self> {
        ensure!(
            capture.snapshot().entries().is_empty() && capture.snapshot().changes().is_empty(),
            "dirty source cannot seal quiescent capture"
        );
        Ok(Self {
            source: CaptureFreeze::Host(capture),
            _owner: owner,
        })
    }
    fn snapshot(&self) -> &output_core::SourceDeltaSnapshot {
        match &self.source {
            CaptureFreeze::Tree { snapshot, .. } => snapshot,
            CaptureFreeze::Host(host) => host.snapshot(),
        }
    }
    fn projection(&self) -> &jj_vfs::native::ProjectionState {
        match &self.source {
            CaptureFreeze::Tree { projection, .. } => projection,
            CaptureFreeze::Host(host) => host.projection_state(),
        }
    }
}
struct Quota {
    used: Mutex<usize>,
    maximum: usize,
}
struct Reservation {
    quota: Arc<Quota>,
    bytes: usize,
}
impl Quota {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation> {
        let mut used = self
            .used
            .lock()
            .map_err(|_| anyhow::anyhow!("capture quota poisoned"))?;
        *used = used
            .checked_add(bytes)
            .filter(|n| *n <= self.maximum)
            .context("capture live byte budget exhausted")?;
        Ok(Reservation {
            quota: self.clone(),
            bytes,
        })
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Ok(mut used) = self.quota.used.lock() {
            *used -= self.bytes;
        }
    }
}
struct CaptureOwner {
    permit: TreeFreezePermit,
    _base: Reservation,
    quota: Arc<Quota>,
}
pub use c::{
    BlobRecord, CaptureData, CapturePolicy, CaptureTree, CapturedRoot, IndexRecord,
    OperationRecord, PortableRemoteState, PortableWorkingCopy,
};
/// Complete locally verified data plus frozen source ownership. No public API
/// converts decoded bytes or generic ManifestV2 into this capture provenance.
pub struct LocalCaptureCandidate {
    data: CaptureData,
    bytes: Vec<u8>,
    private_wc: Arc<[u8]>,
    private_remote: Option<Vec<u8>>,
    _indexes: PortableIndexExport,
    _owner: Arc<CaptureOwner>,
}
impl LocalCaptureCandidate {
    pub fn data(&self) -> &CaptureData {
        &self.data
    }
    pub fn encoded_data(&self) -> &[u8] {
        &self.bytes
    }
    pub fn data_digest(&self) -> [u8; 32] {
        data_digest(&self.bytes)
    }
    pub fn private_working_copy_bytes(&self) -> &[u8] {
        &self.private_wc
    }
    pub fn private_remote_bytes(&self) -> Option<&[u8]> {
        self.private_remote.as_deref()
    }
}
pub struct QuiescentCapture {
    candidate: LocalCaptureCandidate,
}
impl QuiescentCapture {
    pub fn data(&self) -> &CaptureData {
        self.candidate.data()
    }
    pub fn encoded_data(&self) -> &[u8] {
        self.candidate.encoded_data()
    }
    pub fn data_digest(&self) -> [u8; 32] {
        self.candidate.data_digest()
    }
    pub fn candidate(&self) -> &LocalCaptureCandidate {
        &self.candidate
    }
}
impl LocalCaptureCandidate {
    pub fn seal_quiescent(self) -> Result<QuiescentCapture> {
        ensure!(
            matches!(self._owner.permit.source, CaptureFreeze::Host(_)),
            "TreeOnly candidate is not frontend-quiescent capture authority"
        );
        Ok(QuiescentCapture { candidate: self })
    }
}
pub enum CapturedBlob {
    OperationView(c::VerifiedObject),
    Index(ExportArtifact),
}
impl CapturedBlob {
    pub fn bytes(&self) -> &[u8] {
        match self {
            Self::OperationView(object) => object.payload().unwrap(),
            Self::Index(artifact) => artifact.bytes(),
        }
    }
}
/// Physical sink jobs must retain this entire value through completion. The
/// immutable source freeze and admitted buffers follow it after cancellation.
pub struct OwnedCaptureBlob {
    blob: CapturedBlob,
    _owner: Arc<CaptureOwner>,
    _reservation: Option<Reservation>,
}
impl OwnedCaptureBlob {
    /// Borrowed access only: an accepted physical sink job must retain this
    /// whole value, so payload ownership cannot escape its freeze and quota.
    pub fn blob(&self) -> &CapturedBlob {
        &self.blob
    }
    pub fn bytes(&self) -> &[u8] {
        self.blob.bytes()
    }
}
#[async_trait::async_trait]
pub trait LocalCaptureSink: Send {
    async fn write(&mut self, blob: OwnedCaptureBlob) -> Result<()>;
}
struct IndexSink<'a> {
    sink: &'a mut dyn LocalCaptureSink,
    owner: Arc<CaptureOwner>,
    records: Vec<BlobRecord>,
    roots: BTreeSet<CapturedRoot>,
    native_references: usize,
    artifact_limits: c::IndexArtifactLimits,
    max_roots: usize,
    max_references: usize,
}
#[async_trait::async_trait]
impl IndexArtifactSink for IndexSink<'_> {
    async fn write(
        &mut self,
        artifact: ExportArtifact,
    ) -> std::result::Result<(), jj_native_index::Error> {
        let d = artifact.descriptor();
        let verified = c::decode_index_artifact(artifact.bytes(), d, self.artifact_limits)
            .map_err(|_| jj_native_index::Error::Corrupt)?;
        let mut add = |id: m::CommitId| -> std::result::Result<(), jj_native_index::Error> {
            self.native_references = self
                .native_references
                .checked_add(1)
                .filter(|n| *n <= self.max_references)
                .ok_or(jj_native_index::Error::Limit)?;
            if id != m::CommitId::ROOT {
                let key = CapturedRoot {
                    kind: m::Kind::Commit as u8,
                    id: id.0,
                };
                if !self.roots.contains(&key) && self.roots.len() >= self.max_roots {
                    return Err(jj_native_index::Error::Limit);
                }
                self.roots.insert(key);
            }
            Ok(())
        };
        match verified.artifact() {
            c::IndexArtifact::Node(node) => match &node.payload {
                c::IndexNodePayload::Anchors(ids) => {
                    for id in ids {
                        add(*id)?;
                    }
                }
                c::IndexNodePayload::Drafts(records) => {
                    for record in records {
                        add(record.commit())?;
                    }
                }
                c::IndexNodePayload::Branch(_) => {}
            },
            c::IndexArtifact::Root(_) => {}
        }
        drop(verified);

        let (kind, id) = match d.id {
            c::IndexArtifactId::Root(id) => (3, id.0),
            c::IndexArtifactId::Node(id) => (4, id.0),
        };
        self.records.push(BlobRecord {
            kind,
            id: hex(&id),
            blake3: d.selected.blake3,
            size: d.selected.size_bytes,
        });
        self.sink
            .write(OwnedCaptureBlob {
                blob: CapturedBlob::Index(artifact),
                _owner: self.owner.clone(),
                _reservation: None,
            })
            .await
            .map_err(|_| jj_native_index::Error::Invalid)
    }
}
pub struct CaptureInput {
    pub registry: Arc<Registry>,
    pub binding: Binding,
    pub index_config: jj_native_index::Config,
    /// Concrete local-only implementation: no dynamic provider, RPC or fallback.
    pub local_objects: Arc<DirectObjectIo>,
    pub remote_scope: remote_state::RemoteScope,
}
struct Budget {
    started: Instant,
    limits: CaptureLimits,
    edges: usize,
    references: usize,
}
impl Budget {
    fn check(&self) -> Result<()> {
        ensure!(
            self.started.elapsed() < Duration::from_millis(self.limits.max_elapsed_ms),
            "capture elapsed budget exhausted"
        );
        Ok(())
    }
    fn parents(&mut self, count: usize) -> Result<()> {
        self.edges = self
            .edges
            .checked_add(count)
            .context("capture edges overflow")?;
        ensure!(
            self.edges <= self.limits.max_parent_edges,
            "capture parent edge budget exhausted"
        );
        self.check()
    }
    fn root(&mut self, roots: &mut BTreeSet<CapturedRoot>, kind: m::Kind, id: &[u8]) -> Result<()> {
        self.references = self
            .references
            .checked_add(1)
            .context("capture root work overflow")?;
        ensure!(
            self.references <= self.limits.max_parent_edges,
            "capture root reference budget exhausted"
        );
        let id: [u8; 32] = id.try_into().context("non-native capture root")?;
        if kind == m::Kind::Commit && id == [0; 32] {
            return Ok(());
        }
        ensure!(
            roots.contains(&CapturedRoot {
                kind: kind as u8,
                id
            }) || roots.len() < self.limits.max_roots,
            "capture root count exhausted"
        );
        roots.insert(CapturedRoot {
            kind: kind as u8,
            id,
        });
        self.check()
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn op_id(text: &str) -> Result<OperationId> {
    ensure!(valid_hex(text, 128), "capture requires exact native OP64");
    OperationId::try_from_hex(text).context("invalid native operation")
}
fn read_type(path: &Path, expected: &str) -> Result<Vec<u8>> {
    let parent = path.parent().context("missing type parent")?;
    let repository =
        private_fs::PrivateDir::open_existing(parent.parent().context("missing type repository")?)?;
    let relative = Path::new(parent.file_name().context("missing type directory")?)
        .join(path.file_name().context("missing type name")?);
    let bytes = repository.read_descendant(relative, 1024)?;
    ensure!(
        bytes == expected.as_bytes(),
        "unsupported native capture source type"
    );
    Ok(bytes)
}
fn read_remote(
    binding: &Binding,
    scope: &remote_state::RemoteScope,
    cap: usize,
) -> Result<Option<(remote_state::State, Vec<u8>)>> {
    let directory = private_fs::PrivateDir::open_existing(&binding.working_copy)?;
    match directory.read("native-remote.toml", cap.min(1024 * 1024)) {
        Ok(bytes) => Ok(Some((remote_state::decode(&bytes, scope)?, bytes))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub use c::data_digest;
fn data_limits(limits: &CaptureLimits) -> c::CaptureDataLimits {
    c::CaptureDataLimits {
        max_bytes: limits.max_capsule_bytes,
        max_decode_bytes: limits
            .max_capsule_bytes
            .checked_mul(32)
            .and_then(|n| n.checked_add(4096))
            .unwrap_or(usize::MAX)
            .min(limits.max_working_bytes),
        max_operations: limits.max_operations,
        max_parent_edges: limits.max_parent_edges,
        max_roots: limits.max_roots,
        max_blobs: limits
            .export
            .artifacts
            .max_artifacts
            .saturating_add(limits.max_operations.saturating_mul(2)),
    }
}
pub fn decode_data(
    bytes: &[u8],
    expected: [u8; 32],
    limits: &CaptureLimits,
) -> Result<CaptureData> {
    limits.validate()?;
    c::decode_data(bytes, expected, &data_limits(limits))
}

// The actual host capture has already checked and paused its mounted marker.
// Reopening that marker here would wait on the frontend fence we own. Validate
// only the independently stored registry record and external Ready journal.
fn verify_registration(registry: &Registry, expected: &Binding) -> Result<()> {
    registry.validate(expected)?;
    let stored: Binding = serde_json::from_slice(&registry.records.read(
        crate::record_name(&expected.workspace_id),
        crate::MAX_RECORD,
    )?)?;
    ensure!(stored == *expected, "capture registry binding changed");
    registry.validate_ready(&stored)
}

fn read_locator(binding: &Binding, input: &CaptureInput) -> Result<Vec<u8>> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Locator {
        version: u32,
        socket: std::path::PathBuf,
        store: String,
    }
    let repository = private_fs::PrivateDir::open_existing(&binding.repo)?;
    let bytes = repository.read_descendant("store/native-broker.toml", 16 * 1024)?;
    let locator: Locator = toml::from_str(std::str::from_utf8(&bytes)?)?;
    let checked = remote_state::RemoteScope::new(locator.socket, locator.store)?;
    ensure!(
        locator.version == 1 && checked == input.remote_scope,
        "capture backend locator changed"
    );
    ensure!(
        input.index_config.endpoint == jj_native_index::Endpoint::UnixSocket(checked.socket)
            && input.index_config.store_alias == checked.store,
        "capture index/backend identity mismatch"
    );
    Ok(bytes)
}

pub async fn read_candidate(
    input: CaptureInput,
    permit: TreeFreezePermit,
    limits: CaptureLimits,
    sink: &mut dyn LocalCaptureSink,
) -> Result<LocalCaptureCandidate> {
    limits.validate()?;
    if let CaptureFreeze::Host(host) = &permit.source {
        let actual = host.binding();
        ensure!(
            actual.root() == input.binding.root
                && actual.workspace_identity() == input.binding.workspace_id
                && actual.working_copy_state() == input.binding.working_copy
                && actual.repository() == input.binding.repository,
            "native capture proof belongs to another workspace"
        );
        let scope = native_workspace::Scope::from_configured_identity(
            &input.index_config.store_alias,
            &input.index_config.provider_fingerprint,
        )?;
        ensure!(
            actual.backend_identity() == scope.backend(),
            "native capture backend identity mismatch"
        );
    }
    verify_registration(&input.registry, &input.binding)?;
    let binding = &input.binding;
    let locator = read_locator(binding, &input)?;
    ensure!(
        permit.projection().repository == binding.repository,
        "capture projection repository mismatch"
    );
    ensure!(
        permit.snapshot().entries().is_empty() && permit.snapshot().changes().is_empty(),
        "source changed before capture"
    );
    let quota = Arc::new(Quota {
        used: Mutex::new(0),
        maximum: limits.max_working_bytes,
    });
    let base = limits
        .max_operations
        .checked_mul(1024)
        .and_then(|n| n.checked_add(limits.max_parent_edges.checked_mul(256)?))
        .and_then(|n| n.checked_add(limits.max_roots.checked_mul(128)?))
        .and_then(|n| n.checked_add(limits.max_decode_bytes.checked_mul(4)?))
        .and_then(|n| n.checked_add(limits.max_capsule_bytes.checked_mul(2)?))
        .and_then(|n| n.checked_add(limits.export.artifacts.max_artifacts.checked_mul(512)?))
        .and_then(|n| n.checked_add(limits.export.max_working_bytes))
        .context("capture base allocation overflow")?;
    let owner = Arc::new(CaptureOwner {
        permit,
        _base: quota.reserve(base)?,
        quota,
    });
    let mut budget = Budget {
        started: Instant::now(),
        limits: limits.clone(),
        edges: 0,
        references: 0,
    };
    let backend_type = read_type(
        &binding.repo.join("store/type"),
        jj_native_backend::NativeBackend::NAME,
    )?;
    let op_type = read_type(&binding.repo.join("op_store/type"), SimpleOpStore::NAME)?;
    let heads_type = read_type(
        &binding.repo.join("op_heads/type"),
        SimpleOpHeadsStore::NAME,
    )?;
    let state_cap = limits
        .max_decode_bytes
        .checked_sub(4096)
        .map(|n| n / 32)
        .filter(|n| *n > 0)
        .context("capture state decode budget too small")?
        .min(limits.max_file_bytes);
    let wc_binding = jj_vfs::native::ProjectionBinding {
        root: binding.root.clone(),
        state_path: binding.working_copy.clone(),
        repository: binding.repository.clone(),
    };
    let (wc, wc_projection) =
        jj_vfs::native::read_retained_state_for_capture(&wc_binding, state_cap)?;
    ensure!(
        !wc.pending && wc_projection == *owner.permit.projection(),
        "unrepresented native working-copy recovery state or projection mismatch"
    );
    let remote = read_remote(binding, &input.remote_scope, state_cap)?;
    ensure!(
        remote
            .as_ref()
            .is_none_or(|(state, _)| state.pending.is_none()),
        "active remote intent lacks supported immutable capture policy"
    );
    let simple = Arc::new(SimpleOpStore::load(
        &binding.repo.join("op_store"),
        RootOperationData {
            root_commit_id: jj_lib::backend::CommitId::from_bytes(&[0; 32]),
        },
    ));
    let store: Arc<dyn OpStore> = simple.clone();
    let heads = SimpleOpHeadsStore::load(&binding.repo.join("op_heads"));
    let mut operation_heads = heads.get_op_heads_bounded(limits.max_operations)?;
    operation_heads.sort();
    operation_heads.dedup();
    ensure!(!operation_heads.is_empty(), "capture heads missing");
    let retained = simple.retained_operation_ids(limits.max_operations)?;
    ensure!(
        retained.iter().all(|id| id.as_bytes() != [0; 64]),
        "intrinsic operation must not have a retained file"
    );
    let mut pending = retained
        .iter()
        .chain(&operation_heads)
        .cloned()
        .collect::<Vec<_>>();
    pending.push(wc.operation.clone());
    pending.push(OperationId::from_bytes(&[0; 64]));
    if let Some((state, _)) = &remote {
        for id in state.retained_operations() {
            pending.push(op_id(id)?);
        }
    }
    let mut states = BTreeMap::<OperationId, u8>::new();
    let mut operations = BTreeMap::new();
    let mut roots = BTreeSet::new();
    let mut blobs = BTreeMap::new();
    let mut seen_views = BTreeSet::new();
    let mut wc_commit = None;
    let mut stack = pending
        .into_iter()
        .map(|id| (id, false))
        .collect::<Vec<_>>();
    while let Some((id, exit)) = stack.pop() {
        budget.check()?;
        if exit {
            states.insert(id, 2);
            continue;
        }
        match states.get(&id) {
            Some(2) => continue,
            Some(1) => bail!("capture operation cycle"),
            _ => {}
        }
        ensure!(
            id.as_bytes().len() == 64 && states.len() < limits.max_operations,
            "capture operation count/identity limit"
        );
        let raw =
            simple.read_operation_bounded(&id, limits.max_file_bytes, limits.max_decode_bytes)?;
        if id.as_bytes() != [0; 64] {
            ensure!(
                jj_lib::content_hash::blake2b_hash(&raw).as_slice() == id.as_bytes(),
                "operation semantic identity mismatch"
            );
        }
        budget.parents(raw.parents.len())?;
        let operation = Operation::new(store.clone(), id.clone(), raw);
        for commit in operation.all_referenced_commit_ids() {
            budget.root(&mut roots, m::Kind::Commit, commit.as_bytes())?;
        }
        let view = simple.read_view_bounded(
            operation.view_id(),
            limits.max_file_bytes,
            limits.max_decode_bytes,
        )?;
        if operation.view_id().as_bytes() != [0; 64] {
            ensure!(
                jj_lib::content_hash::blake2b_hash(&view).as_slice()
                    == operation.view_id().as_bytes(),
                "view semantic identity mismatch"
            );
        }
        if id == wc.operation {
            wc_commit = view
                .wc_commit_ids
                .get(&wc.workspace)
                .map(|id| id.as_bytes().to_vec());
        }
        let view = jj_lib::view::View::new(view, true);
        for commit in view.all_referenced_commit_ids() {
            budget.root(&mut roots, m::Kind::Commit, commit.as_bytes())?;
        }
        let mut codec = c::Limits::default();
        codec.max_payload_bytes = limits.max_file_bytes;
        codec.max_decode_bytes = limits.max_decode_bytes;
        if id.as_bytes() != [0; 64] {
            let reservation = owner.quota.reserve(
                limits
                    .max_decode_bytes
                    .checked_add(
                        limits
                            .max_file_bytes
                            .checked_mul(2)
                            .context("capture blob budget")?,
                    )
                    .context("capture blob budget")?,
            )?;
            let object = c::export_operation(&simple, &id, codec).await?;
            let digest = object
                .digest()
                .context("intrinsic object cannot have raw capture payload")?;
            blobs.insert(
                (1, id.hex()),
                BlobRecord {
                    kind: 1,
                    id: id.hex(),
                    blake3: digest.blake3,
                    size: digest.size_bytes,
                },
            );
            sink.write(OwnedCaptureBlob {
                blob: CapturedBlob::OperationView(object),
                _owner: owner.clone(),
                _reservation: Some(reservation),
            })
            .await?;
        }
        if operation.view_id().as_bytes() != [0; 64]
            && seen_views.insert(operation.view_id().clone())
        {
            let reservation = owner.quota.reserve(
                limits
                    .max_decode_bytes
                    .checked_add(
                        limits
                            .max_file_bytes
                            .checked_mul(2)
                            .context("capture blob budget")?,
                    )
                    .context("capture blob budget")?,
            )?;
            let object = c::export_view(&simple, operation.view_id(), codec).await?;
            let digest = object
                .digest()
                .context("intrinsic object cannot have raw capture payload")?;
            blobs.insert(
                (2, operation.view_id().hex()),
                BlobRecord {
                    kind: 2,
                    id: operation.view_id().hex(),
                    blake3: digest.blake3,
                    size: digest.size_bytes,
                },
            );
            sink.write(OwnedCaptureBlob {
                blob: CapturedBlob::OperationView(object),
                _owner: owner.clone(),
                _reservation: Some(reservation),
            })
            .await?;
        }
        operations.insert(
            id.clone(),
            OperationRecord {
                operation: id.hex(),
                view: operation.view_id().hex(),
                parents: operation.parent_ids().iter().map(|id| id.hex()).collect(),
            },
        );
        states.insert(id.clone(), 1);
        stack.push((id, false));
        stack.last_mut().unwrap().1 = true;
        for parent in operation.parent_ids().iter().rev() {
            stack.push((parent.clone(), false));
        }
    }
    let wc_commit: [u8; 32] = wc_commit
        .context("working-copy view mapping missing")?
        .try_into()
        .map_err(|_| anyhow::anyhow!("non-native WC commit"))?;
    // Keep the local decode reservation alongside its returned model, not just
    // through the awaited read. Payload destruction precedes owner release.
    struct CurrentCommit {
        value: m::Commit,
        _owner: Option<Owner>,
    }
    let actual = if wc_commit == [0; 32] {
        CurrentCommit {
            value: m::synthetic_root(),
            _owner: None,
        }
    } else {
        let key = m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(wc_commit),
        };
        let descriptor = input
            .local_objects
            .descriptor_with_owner(key, owner.clone())
            .await
            .context("current WC descriptor must already be local")?
            .context("current WC metadata missing locally")?;
        ensure!(
            descriptor.size_bytes <= limits.max_file_bytes as u64,
            "current WC metadata exceeds capture cap"
        );
        let scratch = usize::try_from(descriptor.size_bytes)
            .ok()
            .and_then(|n| n.checked_mul(32))
            .and_then(|n| n.checked_add(4096))
            .context("current WC decode budget")?;
        let local_owner = Arc::new((owner.clone(), owner.quota.reserve(scratch)?));
        let object = input
            .local_objects
            .read_metadata_with_owner(
                m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(wc_commit),
                },
                local_owner.clone(),
            )
            .await
            .context("current WC commit must already exist in local metadata storage")?;
        let m::Object::Commit(commit) = object.into_object() else {
            bail!("WC object is not commit")
        };
        CurrentCommit {
            value: commit,
            _owner: Some(local_owner),
        }
    };
    // Native canonical commits encode one empty label per merge term. jj's
    // working-copy projection uses ConflictLabels' normalized empty vector for
    // that same no-label state, just like NativeBackend::read_commit does.
    let labels =
        jj_lib::conflict_labels::ConflictLabels::from_vec(actual.value.root_tree.labels.clone());
    ensure!(
        actual
            .value
            .root_tree
            .terms
            .iter()
            .map(|id| id.0)
            .collect::<Vec<_>>()
            == wc.trees[0].terms
            && labels.as_slice() == wc.trees[0].labels,
        "actual working-copy commit tree mismatch"
    );
    drop(labels);
    drop(actual);
    for tree in &wc.trees {
        for id in &tree.terms {
            budget.root(&mut roots, m::Kind::Tree, id)?;
        }
    }
    if let Some((state, _)) = &remote {
        for id in state.retained_commit_heads() {
            ensure!(
                id != "00".repeat(32),
                "intrinsic ROOT is not a remote publication head"
            );
            let bytes = (0..id.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&id[at..at + 2], 16))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            budget.root(&mut roots, m::Kind::Commit, &bytes)?;
        }
    }
    let operation_ids = operations
        .keys()
        .map(|id| id.as_bytes().try_into().unwrap())
        .collect::<Vec<[u8; 64]>>();
    let reader = LocalIndexExportReader::open_existing(
        &binding.repo.join("index"),
        &input.index_config,
        limits.index.clone(),
        limits.export.max_working_bytes,
    )?;
    let index_directory = private_fs::PrivateDir::open_existing(binding.repo.join("index"))?;
    let index_manifest = index_directory.read("manifest", 16 * 1024)?;
    let index_before = reader.association_fingerprint(
        &operation_ids,
        limits.export.max_source_reads,
        limits.export.max_source_bytes,
    )?;
    let mut index_sink = IndexSink {
        sink,
        owner: owner.clone(),
        records: vec![],
        roots: BTreeSet::new(),
        native_references: budget.references,
        artifact_limits: {
            let mut bounds = limits.export.artifacts;
            bounds.max_decode_bytes = bounds.max_decode_bytes.min(limits.max_decode_bytes);
            bounds
        },
        max_roots: limits.max_roots,
        max_references: limits.max_parent_edges,
    };
    let exported = reader
        .export_operation_indexes(
            &operation_ids,
            limits.export.clone(),
            &mut index_sink,
            owner.clone(),
        )
        .await?;
    // Root claims from portable snapshots are decoded without canonical object,
    // query or FILE reads; exported descriptor roots alone are not native roots.
    for root in &index_sink.roots {
        ensure!(
            roots.contains(root) || roots.len() < limits.max_roots,
            "capture root union limit"
        );
        roots.insert(root.clone());
    }
    for record in index_sink.records {
        blobs.insert((record.kind, record.id.clone()), record);
    }
    let (after, after_projection) =
        jj_vfs::native::read_retained_state_for_capture(&wc_binding, state_cap)?;
    let mut after_heads = heads.get_op_heads_bounded(limits.max_operations)?;
    after_heads.sort();
    after_heads.dedup();
    ensure!(
        after_projection == wc_projection
            && after.persisted_bytes == wc.persisted_bytes
            && after_heads == operation_heads
            && simple.retained_operation_ids(limits.max_operations)? == retained
            && read_remote(binding, &input.remote_scope, state_cap)?.map(|(_, bytes)| bytes)
                == remote.as_ref().map(|(_, bytes)| bytes.clone()),
        "capture source changed during read"
    );
    verify_registration(&input.registry, binding)?;
    ensure!(
        read_locator(binding, &input)? == locator,
        "capture backend locator changed during read"
    );
    ensure!(
        read_type(
            &binding.repo.join("store/type"),
            jj_native_backend::NativeBackend::NAME
        )? == backend_type
            && read_type(&binding.repo.join("op_store/type"), SimpleOpStore::NAME)? == op_type
            && read_type(
                &binding.repo.join("op_heads/type"),
                SimpleOpHeadsStore::NAME
            )? == heads_type,
        "capture source type changed"
    );
    let index_recheck = exported.reserve_local_read(&limits.index)?;
    ensure!(
        index_directory.read("manifest", 16 * 1024)? == index_manifest
            && reader.association_fingerprint(
                &operation_ids,
                limits.export.max_source_reads,
                limits.export.max_source_bytes
            )? == index_before,
        "index associations changed during capture"
    );
    drop(index_recheck);
    budget.check()?;
    let data = CaptureData {
        version: 1,
        policy: if matches!(owner.permit.source, CaptureFreeze::Host(_)) {
            CapturePolicy::HostQuiescentV1
        } else {
            CapturePolicy::TreeFreezeCandidateV1
        },
        codec: c::CODEC.into(),
        operation_heads: operation_heads.iter().map(|id| id.hex()).collect(),
        working_copy: PortableWorkingCopy {
            workspace: wc.workspace.as_str().into(),
            wc_operation: wc.operation.hex(),
            snapshot_operation: wc.operation.hex(),
            snapshot_commit: wc_commit,
            tree: CaptureTree {
                terms: wc.trees[0].terms.clone(),
                labels: wc.trees[0].labels.clone(),
            },
        },
        operations: operations.into_values().collect(),
        indexes: exported
            .operation_indexes()
            .iter()
            .map(|index| IndexRecord {
                operation: hex(&index.operation),
                root: index.root.0,
            })
            .collect(),
        blobs: blobs.into_values().collect(),
        roots: roots.into_iter().collect(),
        remote: remote.as_ref().map(|(state, _)| PortableRemoteState {
            observations: state
                .observations
                .iter()
                .map(|entry| c::RemoteObservation {
                    reference: entry.reference.clone(),
                    bookmark: entry.bookmark.clone(),
                    generation: entry.generation,
                    head: entry.head.clone(),
                })
                .collect(),
            archived_pushes: state
                .archived_pushes
                .iter()
                .map(|entry| c::ArchivedIntent {
                    reference: entry.reference.clone(),
                    bookmark: entry.bookmark.clone(),
                    generation: entry.generation,
                    head: entry.head.clone(),
                    request_id: entry.request_id.clone(),
                    base_operation: entry.base_operation.clone(),
                    confirmed_generation: entry.confirmed_generation,
                })
                .collect(),
        }),
    };
    let bytes = c::encode_capture_data(&data, data_limits(&limits))?;
    ensure!(
        bytes.len() <= limits.max_capsule_bytes,
        "capture capsule byte budget exhausted"
    );
    Ok(LocalCaptureCandidate {
        data,
        bytes,
        private_wc: wc.persisted_bytes,
        private_remote: remote.map(|(_, bytes)| bytes),
        _indexes: exported,
        _owner: owner,
    })
}
