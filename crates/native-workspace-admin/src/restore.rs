//! Exact fresh restore staging. A staged receipt is NOT registry Ready, a live
//! frontend proof, backup completion, or permission to expose editable files.
use super::*;
use jj_lib::{
    backend::{Backend, BackendInitError},
    op_store::{OpStore, RootOperationData},
    simple_op_heads_store::SimpleOpHeadsStore,
    simple_op_store::SimpleOpStore,
};
use jj_native_backend::{NativeBackend, NativeObjectIo};
use jj_native_index::{NativeIndexStore, PortableImportLimits, PortableImportVersion};
use native_broker::{NativeQueryIo, QueryOwner, query_protocol as q};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::OnceLock,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tinytree_client::{NativeClient, VerifiedCapturedVersion};
use tinytree_cloud as c;
use tinytree_model as m;

const RESTORE_JOURNAL: &str = "restore-stage.json";

/// Moved, not cloned: callers retain no mutable view of accepted source inputs.
/// The SDK version is opaque current-client provenance; raw objects alone are
/// insufficient to construct a prepared restore.
pub struct RestoreInputs {
    pub version: VerifiedCapturedVersion,
    pub objects: Vec<c::VerifiedObject>,
    pub artifacts: Vec<c::VerifiedIndexArtifact>,
}
#[derive(Clone)]
pub struct RestoreProvider {
    pub config: jj_native_index::Config,
    pub index_limits: jj_native_index::Limits,
    pub query: Arc<dyn NativeQueryIo>,
    pub objects: Arc<dyn NativeObjectIo>,
}
#[derive(Clone, Debug)]
pub struct RestoreLimits {
    pub max_working_bytes: usize,
    pub timeout_ms: u64,
    pub manifest: c::ManifestV3Limits,
    pub import: PortableImportLimits,
    pub max_stage_entries: usize,
    pub max_sync_bytes: u64,
}
impl Default for RestoreLimits {
    fn default() -> Self {
        Self {
            max_working_bytes: 1024 * 1024 * 1024,
            timeout_ms: 120_000,
            manifest: Default::default(),
            import: Default::default(),
            max_stage_entries: 100_000,
            max_sync_bytes: 1024 * 1024 * 1024,
        }
    }
}
pub struct PrivateRestoreProvider {
    pub store_alias: String,
    pub endpoint: LocalEndpoint,
    pub provider_identity: [u8; 32],
    pub objects: Arc<jj_native_backend::DirectObjectIo>,
}
struct NativeSource {
    inputs: RestoreInputs,
    manifest: c::ManifestV3,
    _capture: tinytree_client::CloudCapture,
    data: c::CaptureData,
    provider: RestoreProvider,
}
struct PrivateAuthority {
    client: tinytree_client::private_capsule::PrivateCapsuleClient,
    binding: tinytree_client::private_capsule::CapsuleBinding,
}
impl PrivateAuthority {
    async fn revalidate(&self, deadline: Instant) -> Result<()> {
        use tinytree_client::private_capsule::{CapsuleAction, CapsuleCall, CapsuleState};
        check_time(deadline)?;
        let reply = self
            .client
            .call_until(
                CapsuleCall::new(
                    &self.binding.account_id,
                    &self.binding.capsule_id,
                    CapsuleAction::Status,
                ),
                Some(&self.binding),
                deadline.into(),
            )
            .await?;
        ensure!(
            reply.state == CapsuleState::Sealed && reply.binding() == self.binding,
            "private restore source no longer sealed"
        );
        check_time(deadline)
    }
}
struct PrivateSource {
    bundle: crate::private_bundle::VerifiedPrivateBundle,
    authority: PrivateAuthority,
    provider: PrivateRestoreProvider,
}
enum SourceKind {
    Native(NativeSource),
    Private(PrivateSource),
}
struct RestoreState {
    operation_heads: Vec<String>,
    working_copy: c::PortableWorkingCopy,
    remote: Option<c::PortableRemoteState>,
}
struct Source {
    kind: SourceKind,
    data: RestoreState,
    identity: RestoreSourceIdentity,
    limits: RestoreLimits,
    retained_bytes: usize,
    _owner: QueryOwner,
    _source_slot: tokio::sync::OwnedSemaphorePermit,
    deadline: Instant,
}
impl Source {
    fn native(&self) -> Result<&NativeSource> {
        match &self.kind {
            SourceKind::Native(source) => Ok(source),
            SourceKind::Private(_) => anyhow::bail!("native cloud restore strategy required"),
        }
    }
    fn private(&self) -> Option<&PrivateSource> {
        if let SourceKind::Private(source) = &self.kind {
            Some(source)
        } else {
            None
        }
    }
    fn objects(&self) -> &[c::VerifiedObject] {
        match &self.kind {
            SourceKind::Native(s) => &s.inputs.objects,
            SourceKind::Private(s) => s.bundle.operations(),
        }
    }
    fn artifacts(&self) -> &[c::VerifiedIndexArtifact] {
        match &self.kind {
            SourceKind::Native(s) => &s.inputs.artifacts,
            SourceKind::Private(_) => &[],
        }
    }
    fn objects_io(&self) -> Arc<dyn NativeObjectIo> {
        match &self.kind {
            SourceKind::Native(s) => s.provider.objects.clone(),
            SourceKind::Private(s) => s.provider.objects.clone(),
        }
    }
    fn store_alias(&self) -> &str {
        match &self.kind {
            SourceKind::Native(s) => &s.provider.config.store_alias,
            SourceKind::Private(s) => &s.provider.store_alias,
        }
    }
    fn endpoint(&self) -> LocalEndpoint {
        match &self.kind {
            SourceKind::Private(s) => s.provider.endpoint.clone(),
            SourceKind::Native(s) => match &s.provider.config.endpoint {
                jj_native_index::Endpoint::UnixSocket(p) => LocalEndpoint::UnixSocket(p.clone()),
                jj_native_index::Endpoint::WindowsNamedPipe(p) => {
                    LocalEndpoint::WindowsNamedPipe(p.clone())
                }
            },
        }
    }
    async fn reauthorize(&self, deadline: Instant) -> Result<()> {
        check_time(deadline)?;
        match &self.kind {
            SourceKind::Native(s) => {
                let graph = c::verify_index_artifact_graph(
                    &s.manifest.history.index_roots,
                    &s.inputs.artifacts,
                    self.limits.manifest.history.artifacts,
                )?;
                authorize(
                    &s.inputs,
                    &s.manifest,
                    &s.data,
                    &graph,
                    &s.provider,
                    self._owner.clone(),
                    deadline,
                )
                .await?;
            }
            SourceKind::Private(s) => s.authority.revalidate(deadline).await?,
        }
        check_time(deadline)
    }
    fn operation_index_digest(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        match &self.kind {
            SourceKind::Native(s) => {
                hash.update(b"native-restore-operation-indexes-v1\0");
                for entry in &s.manifest.history.operation_indexes {
                    hash.update(&entry.operation);
                    hash.update(&entry.root.0);
                }
            }
            SourceKind::Private(_) => {
                hash.update(b"private-restore-legacy-operations-v1\0");
                for id in operation_ids(self) {
                    hash.update(&id);
                }
            }
        }
        *hash.finalize().as_bytes()
    }
}
// Stage-local only: Source keeps the BARE provider, so this owner edge cannot
// form Source -> wrapper -> Source. The sole retained-source admission remains
// charged until detached physical provider work drops this exact owner.
struct RestoreObjectIo {
    inner: Arc<dyn NativeObjectIo>,
    owner: Arc<Source>,
}
impl std::fmt::Debug for RestoreObjectIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestoreObjectIo").finish_non_exhaustive()
    }
}
fn metadata_only<T>() -> jj_lib::backend::BackendResult<T> {
    Err(jj_lib::backend::BackendError::Unsupported(
        "restore staging permits metadata reads only".into(),
    ))
}
#[async_trait::async_trait]
impl NativeObjectIo for RestoreObjectIo {
    fn metadata_work_budget(&self) -> Option<jj_native_backend::ObjectIoResources> {
        self.inner.metadata_work_budget()
    }
    async fn descriptor(
        &self,
        key: m::ObjectKey,
    ) -> jj_lib::backend::BackendResult<Option<m::Descriptor>> {
        self.inner
            .descriptor_with_owner(key, self.owner.clone())
            .await
    }
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        owner: QueryOwner,
    ) -> jj_lib::backend::BackendResult<Option<m::Descriptor>> {
        self.inner
            .descriptor_with_owner(key, Arc::new((self.owner.clone(), owner)))
            .await
    }
    async fn read_metadata(
        &self,
        key: m::ObjectKey,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.inner
            .read_metadata_with_owner(key, self.owner.clone())
            .await
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: QueryOwner,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.inner
            .read_metadata_with_owner(key, Arc::new((self.owner.clone(), owner)))
            .await
    }
    async fn write_metadata(&self, _: m::Object) -> jj_lib::backend::BackendResult<m::Descriptor> {
        metadata_only()
    }
    async fn read_file(
        &self,
        _: m::FileId,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::FileRead> {
        metadata_only()
    }
    async fn write_file(
        &self,
        _: &mut (dyn futures::AsyncRead + Send + Unpin),
        _: u64,
    ) -> jj_lib::backend::BackendResult<m::FileId> {
        metadata_only()
    }
    async fn copy_catalog_snapshot(
        &self,
        _: jj_native_backend::CopyCatalogLimits,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::CopyCatalogSnapshot> {
        metadata_only()
    }
}

/// Opaque verified/authenticated source. It does not reserve or initialize a
/// destination. Rebuilding it after restart requires fresh configured authority.
pub struct PreparedRestore {
    source: Arc<Source>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Exact immutable selection DATA for host records, never fetch or activation
/// authority. Only PreparedRestore retains its authenticated source capability.
pub struct RestoreSourceIdentity {
    workspace: [u8; 32],
    generation: u64,
    manifest: [u8; 32],
    manifest_blake3: [u8; 32],
    manifest_bytes: u64,
    capture: [u8; 32],
    capture_blake3: [u8; 32],
    capture_bytes: u64,
    provider: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    private_capsule: Option<tinytree_client::private_capsule::CapsuleBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    private_review_base: Option<crate::private_bundle::PrivateReviewBase>,
}
impl RestoreSourceIdentity {
    pub fn private_review_base(&self) -> Option<&crate::private_bundle::PrivateReviewBase> {
        self.private_review_base.as_ref()
    }

    pub fn private_capsule(&self) -> Option<&tinytree_client::private_capsule::CapsuleBinding> {
        self.private_capsule.as_ref()
    }
    pub fn workspace_id(&self) -> [u8; 32] {
        self.workspace
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn manifest_id(&self) -> [u8; 32] {
        self.manifest
    }
    pub fn manifest_blake3(&self) -> [u8; 32] {
        self.manifest_blake3
    }
    pub fn manifest_size_bytes(&self) -> u64 {
        self.manifest_bytes
    }
    pub fn capture_id(&self) -> [u8; 32] {
        self.capture
    }
    pub fn capture_blake3(&self) -> [u8; 32] {
        self.capture_blake3
    }
    pub fn capture_size_bytes(&self) -> u64 {
        self.capture_bytes
    }
    pub fn provider_identity(&self) -> [u8; 32] {
        self.provider
    }
}
impl PreparedRestore {
    /// Fresh live source authorization before host-owned reservation mutations.
    /// This does not publish Ready or authorize any unbound provider.
    pub async fn revalidate_until(&self, original: Instant) -> Result<()> {
        let source = self.source.clone();
        let deadline = source.deadline.min(original);
        check_time(deadline)?;
        owned_job(deadline, move || {
            futures::executor::block_on(source.reauthorize(deadline))
        })
        .await
    }
    /// Semantic proof borrowed from this authenticated, live-source preparation.
    pub fn private_bundle(&self) -> Option<&crate::private_bundle::VerifiedPrivateBundle> {
        self.source.private().map(|source| &source.bundle)
    }
    pub fn source_identity(&self) -> RestoreSourceIdentity {
        self.source.identity.clone()
    }
    /// Borrow the independently verified exact tree/WC seed. It does not attest
    /// a mounted destination, and callers must not replace it with ambient refs.
    pub fn projection_seed(&self) -> &c::PortableWorkingCopy {
        &self.source.data.working_copy
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u32,
    binding: Binding,
    source: RestoreSourceIdentity,
    operation_heads: u64,
    operation_heads_digest: [u8; 32],
    wc_operation: String,
    snapshot_operation: String,
    #[serde(default)]
    snapshot_commit: Option<[u8; 32]>,
    workspace: String,
    operation_indexes: [u8; 32],
    #[serde(default)]
    index_fingerprint: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    remote_state_digest: Option<[u8; 32]>,
    phase: Phase,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Reserved,
    ObjectsStaged,
    IndexStaged,
    InstallingRepository,
    RepositoryInstalled,
    WorkingCopyStaged,
    PublishingReady,
    Finalized,
}
/// Another supported initialization/restore owns this destination. No wait or
/// destination adoption is attempted by the restore-only admission path.
#[derive(Debug)]
pub struct RestoreBusy;
impl std::fmt::Display for RestoreBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("restore destination is busy")
    }
}
impl std::error::Error for RestoreBusy {}

/// The exclusive root/administration lease is transferred into the physical
/// stage job. Drop never removes partial directories or marks registry Ready.
pub struct RestoreIntent {
    registry: Arc<Registry>,
    admin: PrivateDir,
    journal: Journal,
    _lock: File,
    deadline: Instant,
}
/// Trusted host-only metadata writer for a preconstructed native Backend. It
/// must perform synchronous bounded local publication only, not nested RPC.
pub type BackendMetadataWriter = dyn Fn(&Path) -> Result<()> + Send + Sync;
pub struct RestoreHost {
    pub remote_scope: crate::remote_state::RemoteScope,
    pub settings: UserSettings,
    pub signer: Signer,
    pub connector: Arc<dyn ProjectionConnector>,
    pub backend_config: jj_native_backend::Config,
    pub backend_metadata: Arc<BackendMetadataWriter>,
}
pub struct StagedRestoreReceipt {
    pub binding: Binding,
    pub operation_heads: Vec<[u8; 64]>,
    pub wc_operation: [u8; 64],
    pub snapshot_operation: [u8; 64],
    pub snapshot_commit: [u8; 32],
    pub tree: jj_vfs::native::NativeTree,
    authority: StageAuthority,
}
struct StageAuthority {
    intent: RestoreIntent,
    source: Arc<Source>,
}
impl std::fmt::Debug for StagedRestoreReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedRestoreReceipt")
            .field("operation_heads", &self.operation_heads.len())
            .finish_non_exhaustive()
    }
}
/// The exact staged state cannot be safely adopted by this bounded recovery
/// path. State is preserved; this is not permission to rerun initialization.
#[derive(Debug)]
pub struct RestoreRecoveryRequired(&'static str);
impl RestoreRecoveryRequired {
    pub fn reason(&self) -> &'static str {
        self.0
    }
}
impl std::fmt::Display for RestoreRecoveryRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for RestoreRecoveryRequired {}

/// Trusted current host configuration/read-only validator. Actual Linux frontend
/// and command startup guards remain owned by the host throughout finalization.
pub struct RestoreReadbackHost {
    pub remote_scope: crate::remote_state::RemoteScope,
    pub settings: UserSettings,
    pub connector: Arc<dyn ProjectionConnector>,
    pub backend_config: jj_native_backend::Config,
    pub backend_metadata_check: Arc<BackendMetadataWriter>,
}
/// Durable ADMIN registration only. It does not activate a frontend or command
/// gate and is not a retained-version authority for future network reads.
#[derive(Debug)]
pub struct AdminRestoreReady {
    binding: Binding,
    source: RestoreSourceIdentity,
}
impl AdminRestoreReady {
    pub fn binding(&self) -> &Binding {
        &self.binding
    }
    pub fn source_identity(&self) -> &RestoreSourceIdentity {
        &self.source
    }
}
/// Existing finalized origin eligible for ordinary CURRENT-state native resume.
/// The source selection is data, not a fresh cloud grant or captured-WC reset.
#[derive(Clone, Debug)]
pub struct RestoredResumeInfo {
    pub binding: Binding,
    pub source: RestoreSourceIdentity,
}

struct Runtime {
    runtime: tokio::runtime::Runtime,
    admission: Arc<tokio::sync::Semaphore>,
    sources: Arc<tokio::sync::Semaphore>,
}
fn runtime() -> Result<&'static Runtime> {
    static INSTANCE: OnceLock<std::result::Result<Runtime, String>> = OnceLock::new();
    INSTANCE
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(4)
                .enable_all()
                .build()
                .map(|runtime| Runtime {
                    runtime,
                    admission: Arc::new(tokio::sync::Semaphore::new(1)),
                    sources: Arc::new(tokio::sync::Semaphore::new(1)),
                })
                .map_err(|_| "restore worker unavailable".into())
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))
}
async fn owned_job<T: Send + 'static>(
    deadline: Instant,
    job: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let runtime = runtime()?;
    let permit =
        tokio::time::timeout_at(deadline.into(), runtime.admission.clone().acquire_owned())
            .await
            .context("restore admission deadline exceeded")??;
    let task = runtime.runtime.spawn_blocking(move || {
        let _permit = permit;
        job()
    });
    // Timing out a waiter does not abort its accepted owned physical job.
    tokio::time::timeout_at(deadline.into(), task)
        .await
        .context("restore stage remains owned after wait deadline")?
        .context("restore worker failed; preserve staging")?
}
fn deadline(limits: &RestoreLimits) -> Result<Instant> {
    ensure!(
        (1..=86_400_000).contains(&limits.timeout_ms)
            && (65536..=1024 * 1024 * 1024).contains(&limits.max_working_bytes),
        "invalid restore limits"
    );
    ensure!(
        (1..=1_000_000).contains(&limits.max_stage_entries) && limits.max_sync_bytes > 0,
        "invalid restore stage limits"
    );
    limits.manifest.validate()?;
    Instant::now()
        .checked_add(Duration::from_millis(limits.timeout_ms))
        .context("restore deadline overflow")
}
fn check_time(deadline: Instant) -> Result<()> {
    ensure!(Instant::now() < deadline, "restore deadline exceeded");
    Ok(())
}
async fn call<T>(
    deadline: Instant,
    phase: &'static str,
    future: impl Future<Output = std::result::Result<T, native_broker::ControlStatus>>,
) -> Result<T> {
    // Status messages and their error chains may contain source data. Only
    // fixed local phase labels and the protocol status category may escape.
    tokio::time::timeout_at(deadline.into(), future)
        .await
        .map_err(|_| anyhow::anyhow!("restore source {phase}: DeadlineExceeded"))?
        .map_err(|error| anyhow::anyhow!("restore source {phase}: {:?}", error.code()))
}

fn array<const N: usize>(text: &str) -> Result<[u8; N]> {
    ensure!(text.len() == N * 2, "restore identity width");
    let mut value = [0; N];
    for (out, pair) in value.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        fn digit(value: u8) -> Result<u8> {
            match value {
                b'0'..=b'9' => Ok(value - b'0'),
                b'a'..=b'f' => Ok(value - b'a' + 10),
                _ => anyhow::bail!("restore identity encoding"),
            }
        }
        *out = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    Ok(value)
}
fn identity(inputs: &RestoreInputs, provider: &RestoreProvider) -> RestoreSourceIdentity {
    let version = inputs.version.version();
    let selector = version.selector();
    let raw = version.manifest_selected();
    let capture = inputs.version.descriptor();
    RestoreSourceIdentity {
        workspace: selector.workspace_id,
        generation: selector.generation,
        manifest: selector.manifest_id.0,
        manifest_blake3: raw.blake3,
        manifest_bytes: raw.size_bytes,
        capture: capture.id.0,
        capture_blake3: capture.selected.blake3,
        capture_bytes: capture.selected.size_bytes,
        provider: provider.config.provider_fingerprint,
        private_capsule: None,
        private_review_base: None,
    }
}
fn import_version(source: &Source) -> PortableImportVersion {
    let source = source.identity.clone();
    PortableImportVersion {
        provider_identity: source.provider,
        workspace_id: source.workspace,
        generation: source.generation,
        manifest_id: source.manifest,
        manifest_blake3: source.manifest_blake3,
        manifest_size_bytes: source.manifest_bytes,
    }
}
fn add(total: &mut usize, bytes: usize, maximum: usize) -> Result<()> {
    *total = total
        .checked_add(bytes)
        .context("restore memory overflow")?;
    ensure!(*total <= maximum, "restore memory limit exceeded");
    Ok(())
}
fn retained(inputs: &RestoreInputs, limits: &RestoreLimits) -> Result<usize> {
    ensure!(
        inputs.objects.len() <= limits.manifest.history.recovery.max_members
            && inputs.artifacts.len() <= limits.manifest.history.artifacts.max_artifacts,
        "restore input count limit"
    );
    let mut bytes = inputs.version.version().manifest().len();
    add(
        &mut bytes,
        inputs
            .objects
            .capacity()
            .checked_mul(std::mem::size_of::<c::VerifiedObject>())
            .context("restore object inventory overflow")?,
        limits.max_working_bytes,
    )?;
    add(
        &mut bytes,
        inputs
            .artifacts
            .capacity()
            .checked_mul(std::mem::size_of::<c::VerifiedIndexArtifact>())
            .context("restore artifact inventory overflow")?,
        limits.max_working_bytes,
    )?;
    for object in &inputs.objects {
        add(
            &mut bytes,
            object.retained_size_upper_bound()?,
            limits.max_working_bytes,
        )?;
    }
    for artifact in &inputs.artifacts {
        add(
            &mut bytes,
            artifact.retained_size_upper_bound()?,
            limits.max_working_bytes,
        )?;
    }
    // Precharge the caller's manifest model/reencoding and the SDK capture copy
    // before copying or decoding. Graph and binding have separate remaining caps.
    add(
        &mut bytes,
        inputs
            .version
            .version()
            .manifest()
            .len()
            .checked_mul(64)
            .context("restore manifest memory overflow")?,
        limits.max_working_bytes,
    )?;
    add(
        &mut bytes,
        usize::try_from(inputs.version.descriptor().selected.size_bytes)?
            .checked_mul(3)
            .context("restore capture memory overflow")?,
        limits.max_working_bytes,
    )?;
    Ok(bytes)
}

/// Authenticate and verify the complete V3 source without destination writes.
/// Providers MUST run physical I/O independently of the calling executor, as
/// required by NativeIndexStore. Inputs move into the single admitted worker.
pub async fn prepare_restore(
    client: NativeClient,
    inputs: RestoreInputs,
    provider: RestoreProvider,
    limits: RestoreLimits,
    owner: QueryOwner,
) -> Result<PreparedRestore> {
    let original = deadline(&limits)?;
    prepare_restore_until(client, inputs, provider, limits, owner, original).await
}
/// Collect a sealed, authenticated capsule into an owner-private empty file.
/// Semantic bundle proof alone never grants access to a restore destination.
pub async fn prepare_private_restore_until(
    client: tinytree_client::private_capsule::PrivateCapsuleClient,
    account_id: String,
    capsule_id: String,
    mut spool: File,
    provider: PrivateRestoreProvider,
    limits: RestoreLimits,
    bundle_limits: crate::private_bundle::BundleLimits,
    owner: QueryOwner,
    original: Instant,
) -> Result<PreparedRestore> {
    use tinytree_client::private_capsule::{
        CapsuleAction, CapsuleCall, CapsuleState, ZERO_CHAIN, chain_chunk,
    };
    let deadline = deadline(&limits)?.min(original);
    check_time(deadline)?;
    let source_slot =
        tokio::time::timeout_at(deadline.into(), runtime()?.sources.clone().acquire_owned())
            .await
            .context("private restore source admission exceeded")??;
    owned_job(deadline, move || {
        futures::executor::block_on(async move {
            use std::io::{Seek, SeekFrom, Write};
            ensure!(cfg!(target_os = "linux"), "private restore is Linux-only");
            jj_native_index::Config {
                endpoint: match &provider.endpoint {
                    LocalEndpoint::UnixSocket(path) => {
                        jj_native_index::Endpoint::UnixSocket(path.clone())
                    }
                    LocalEndpoint::WindowsNamedPipe(name) => {
                        jj_native_index::Endpoint::WindowsNamedPipe(name.clone())
                    }
                },
                store_alias: provider.store_alias.clone(),
                provider_fingerprint: provider.provider_identity,
            }
            .validate()?;
            let metadata = spool.metadata()?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::{MetadataExt, PermissionsExt};
                ensure!(
                    metadata.is_file()
                        && metadata.nlink() == 1
                        && metadata.permissions().mode() & 0o077 == 0,
                    "private restore spool is not an owner-private regular file"
                );
            }
            ensure!(metadata.len() == 0, "private restore spool is not empty");
            let status = client
                .call_until(
                    CapsuleCall::new(&account_id, &capsule_id, CapsuleAction::Status),
                    None,
                    deadline.into(),
                )
                .await?;
            check_time(deadline)?;
            ensure!(
                status.state == CapsuleState::Sealed,
                "private restore capsule is not sealed"
            );
            let binding = status.binding();
            ensure!(
                binding.descriptor.total_bytes <= bundle_limits.max_bytes,
                "private restore bundle byte limit"
            );
            let authority = PrivateAuthority { client, binding };
            let mut chain = ZERO_CHAIN.to_owned();
            let mut size = 0u64;
            let mut digest = blake3::Hasher::new();
            for sequence in 0..authority.binding.descriptor.chunk_count {
                check_time(deadline)?;
                let mut call = CapsuleCall::new(&account_id, &capsule_id, CapsuleAction::Read);
                call.sequence = Some(sequence);
                let reply = authority
                    .client
                    .call_until(call, Some(&authority.binding), deadline.into())
                    .await?;
                check_time(deadline)?;
                ensure!(
                    reply.state == CapsuleState::Sealed,
                    "private restore capsule changed"
                );
                let bytes = reply.read_bytes()?;
                size = size
                    .checked_add(bytes.len() as u64)
                    .context("private restore size overflow")?;
                ensure!(
                    size <= authority.binding.descriptor.total_bytes,
                    "private restore excess bytes"
                );
                chain = chain_chunk(&chain, sequence, &bytes)?;
                digest.update(&bytes);
                spool.write_all(&bytes)?;
            }
            ensure!(
                size == authority.binding.descriptor.total_bytes
                    && chain == authority.binding.descriptor.chain_root,
                "private restore capsule chain differs"
            );
            spool.sync_all()?;
            spool.seek(SeekFrom::Start(0))?;
            let bundle = crate::private_bundle::verify_file(
                spool,
                *digest.finalize().as_bytes(),
                bundle_limits,
            )?;
            ensure!(
                hex(&bundle.manifest().workspace_id) == authority.binding.workspace_id
                    && bundle.manifest().generation == authority.binding.generation,
                "private restore bundle/capsule identity differs"
            );
            if let Some(base) = bundle.review_base() {
                ensure!(
                    hex(&base.view_version) == authority.binding.selection.view_version
                        && base.policy_epoch == authority.binding.selection.policy_epoch,
                    "private restore review scope differs from authenticated selection"
                );
            }
            authority.revalidate(deadline).await?;
            let manifest_hash = *blake3::hash(bundle.manifest_bytes()).as_bytes();
            let identity = RestoreSourceIdentity {
                workspace: bundle.manifest().workspace_id,
                generation: bundle.manifest().generation,
                manifest: manifest_hash,
                manifest_blake3: manifest_hash,
                manifest_bytes: bundle.manifest_bytes().len() as u64,
                capture: bundle.blake3(),
                capture_blake3: bundle.blake3(),
                capture_bytes: bundle.size_bytes(),
                provider: provider.provider_identity,
                private_capsule: Some(authority.binding.clone()),
                private_review_base: bundle.review_base().cloned(),
            };
            let mut retained_bytes = bundle
                .manifest_bytes()
                .len()
                .checked_mul(4)
                .context("private restore manifest memory overflow")?;
            ensure!(
                retained_bytes <= limits.max_working_bytes,
                "private restore retained metadata limit"
            );
            for object in bundle.operations() {
                add(
                    &mut retained_bytes,
                    object.retained_size_upper_bound()?,
                    limits.max_working_bytes,
                )?;
            }
            add(
                &mut retained_bytes,
                bundle
                    .native_descriptors()
                    .len()
                    .checked_mul(128)
                    .context("private restore descriptor budget overflow")?,
                limits.max_working_bytes,
            )?;
            let captured = bundle.capture_data();
            let data = RestoreState {
                operation_heads: captured.operation_heads.clone(),
                working_copy: captured.working_copy.clone(),
                remote: captured.remote.clone(),
            };
            Ok(PreparedRestore {
                source: Arc::new(Source {
                    kind: SourceKind::Private(PrivateSource {
                        bundle,
                        authority,
                        provider,
                    }),
                    data,
                    identity,
                    limits,
                    retained_bytes,
                    _owner: owner,
                    _source_slot: source_slot,
                    deadline,
                }),
            })
        })
    })
    .await
}

/// Bound source preparation, staging and exact finalization by the host's
/// original absolute deadline. Rebinding never extends the configured policy.
pub async fn prepare_restore_until(
    client: NativeClient,
    inputs: RestoreInputs,
    provider: RestoreProvider,
    limits: RestoreLimits,
    owner: QueryOwner,
    original: Instant,
) -> Result<PreparedRestore> {
    let deadline = deadline(&limits)?.min(original);
    check_time(deadline)?;
    let source_slot =
        tokio::time::timeout_at(deadline.into(), runtime()?.sources.clone().acquire_owned())
            .await
            .context("restore source retention admission exceeded")??;
    owned_job(deadline, move || {
        futures::executor::block_on(async move {
            ensure!(
                cfg!(target_os = "linux"),
                "fresh native restore staging is Linux-only"
            );
            provider.config.validate()?;
            let retained_bytes = retained(&inputs, &limits)?;
            let mut charged = retained_bytes;
            add(
                &mut charged,
                limits.manifest.history.recovery.max_decode_bytes,
                limits.max_working_bytes,
            )?;
            add(
                &mut charged,
                limits.manifest.history.artifacts.max_decode_bytes,
                limits.max_working_bytes,
            )?;
            // Two decoded DATA models briefly coexist when transferring the
            // verifier's borrowed result to the source-owned portable record.
            add(
                &mut charged,
                limits
                    .manifest
                    .binding
                    .max_bytes
                    .checked_mul(2)
                    .context("restore binding budget overflow")?,
                limits.max_working_bytes,
            )?;
            let manifest = c::decode_manifest_v3(
                inputs.version.version().manifest(),
                c::ManifestId(inputs.version.version().selector().manifest_id.0),
                limits.manifest,
            )?;
            ensure!(
                c::encode_manifest_v3(&manifest, limits.manifest)?
                    == inputs.version.version().manifest(),
                "noncanonical restore manifest"
            );
            let capture = call(
                deadline,
                "capture_read",
                client
                    .clone()
                    .with_operation_deadline(deadline.into())
                    .with_lifetime_guard(Arc::new(owner.clone()))
                    .cloud_read_capture(&inputs.version),
            )
            .await?;
            let data = {
                let graph = c::verify_index_artifact_graph(
                    &manifest.history.index_roots,
                    &inputs.artifacts,
                    limits.manifest.history.artifacts,
                )?;
                let binding = c::verify_capture_export_binding(
                    &manifest,
                    capture.payload(),
                    &inputs.objects,
                    &inputs.artifacts,
                    &graph,
                    limits.manifest,
                )?;
                let data = binding.capture_data().clone();
                drop(binding);
                authorize(
                    &inputs,
                    &manifest,
                    &data,
                    &graph,
                    &provider,
                    owner.clone(),
                    deadline,
                )
                .await?;
                data
            };
            if data.version == 2 {
                let capabilities = call(
                    deadline,
                    "landing_recovery_capability",
                    client
                        .with_operation_deadline(deadline.into())
                        .capabilities(&[
                            tinytree_client::CapabilityFeature::LandingRecoveryCloudRead,
                        ]),
                )
                .await?;
                ensure!(
                    capabilities.features.len() == 1
                        && capabilities.features[0].supported
                        && capabilities.features[0].authorized,
                    "landing recovery restore capability unavailable"
                );
            }
            check_time(deadline)?;
            Ok(PreparedRestore {
                source: Arc::new(Source {
                    identity: identity(&inputs, &provider),
                    data: RestoreState {
                        operation_heads: data.operation_heads.clone(),
                        working_copy: data.working_copy.clone(),
                        remote: data.remote.clone(),
                    },
                    kind: SourceKind::Native(NativeSource {
                        inputs,
                        manifest,
                        _capture: capture,
                        data,
                        provider,
                    }),
                    limits: limits.clone(),
                    retained_bytes: retained_bytes
                        .checked_add(limits.manifest.history.recovery.max_decode_bytes)
                        .and_then(|n| n.checked_add(limits.manifest.binding.max_bytes))
                        .context("restore retained source overflow")?,
                    _owner: owner,
                    _source_slot: source_slot,
                    deadline,
                }),
            })
        })
    })
    .await
}

async fn authorize(
    inputs: &RestoreInputs,
    manifest: &c::ManifestV3,
    data: &c::CaptureData,
    graph: &c::VerifiedIndexGraph<'_>,
    provider: &RestoreProvider,
    owner: QueryOwner,
    deadline: Instant,
) -> Result<()> {
    let selected = inputs.version.version().selector();
    let raw = inputs.version.version().manifest_selected();
    let wc_operation = array::<64>(&data.working_copy.snapshot_operation)?;
    let wc_root = manifest
        .history
        .operation_indexes
        .iter()
        .find(|entry| entry.operation == wc_operation)
        .context("restore snapshot operation lacks index association")?
        .root;
    let prepared = call(
        deadline,
        "cloud_authorization_prepare",
        provider.query.prepare_cloud_authorization(
            native_broker::CloudAuthorizationSource {
                provider_identity: provider.config.provider_fingerprint,
                workspace_id: selected.workspace_id,
                generation: selected.generation,
                manifest_id: selected.manifest_id.0,
                manifest_blake3: raw.blake3,
                manifest_size_bytes: raw.size_bytes,
            },
            owner.clone(),
        ),
    )
    .await?;
    let query = prepared
        .as_ref()
        .map_or(&provider.query, |reply| &reply.value);
    let owner: QueryOwner = match &prepared {
        Some(reply) => Arc::new((owner, reply.owner.clone())),
        None => owner,
    };
    let mut wc_verified = false;
    for root in &manifest.history.index_roots {
        check_time(deadline)?;
        let snapshot = graph.snapshot(root.id)?;
        let source = native_broker::CloudQuerySource {
            provider_identity: provider.config.provider_fingerprint,
            workspace_id: selected.workspace_id,
            generation: selected.generation,
            manifest_id: selected.manifest_id.0,
            manifest_blake3: raw.blake3,
            manifest_size_bytes: raw.size_bytes,
            index_root_ids: vec![root.id.0],
            expected_anchors: snapshot.roots,
        };
        let mut replay = [0; 32];
        getrandom::fill(&mut replay)
            .map_err(|_| anyhow::anyhow!("restore source nonce unavailable"))?;
        let reply = call(
            deadline,
            "cloud_scope_open",
            query.open_cloud_scope(
                native_broker::OpenCloudQueryScope {
                    source: source.clone(),
                    overlay: vec![],
                    replay_key: replay,
                },
                owner.clone(),
            ),
        )
        .await?;
        let physical: QueryOwner = Arc::new((owner.clone(), reply.owner));
        let scope = reply.value;
        // Every known scope is closed on validation failure too. An uncertain
        // open or physical cancellation still uses the provider's bounded TTL.
        let validation = async {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            ensure!(
                scope.query_identity_version == 2
                    && scope.cloud_source_digest == Some(source.digest())
                    && scope.expires_at_unix_seconds > i64::try_from(now)?,
                "restore source capability mismatch"
            );
            for chunk in snapshot.drafts.chunks(64) {
                let facts = chunk
                    .iter()
                    .map(|draft| q::GraphFact {
                        fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                            commit: Some(q::CommitId {
                                value: draft.commit().0.to_vec(),
                            }),
                        })),
                    })
                    .collect();
                let result = call(
                    deadline,
                    "frozen_membership",
                    query.facts(scope.handle, facts, physical.clone()),
                )
                .await?;
                ensure!(
                    result.value.answers.len() == chunk.len()
                        && result.value.answers.iter().all(|answer| matches!(
                            answer.answer,
                            Some(q::graph_fact_answer::Answer::HasId(true))
                        )),
                    "restore frozen baseline membership mismatch"
                );
            }
            if root.id == wc_root {
                let expected = &data.working_copy.tree;
                if data.working_copy.snapshot_commit == m::CommitId::ROOT.0 {
                    let tree = m::encode(&m::Object::Tree(m::Tree::default()), Default::default())?;
                    ensure!(
                        expected.terms == vec![tree.descriptor.key.id.0]
                            && expected.labels.iter().all(String::is_empty),
                        "intrinsic restore tree mismatch"
                    );
                } else {
                    let commit = m::CommitId(data.working_copy.snapshot_commit);
                    let result = call(
                        deadline,
                        "commit_read",
                        query.read_commit(scope.handle, commit, physical.clone()),
                    )
                    .await?;
                    ensure!(
                        result.value.descriptor().key
                            == m::ObjectKey {
                                kind: m::Kind::Commit,
                                id: m::ObjectId(commit.0)
                            },
                        "restore commit selection mismatch"
                    );
                    let m::Object::Commit(value) = result.value.into_object() else {
                        anyhow::bail!("restore commit kind mismatch")
                    };
                    let terms: Vec<_> = value.root_tree.terms.iter().map(|term| term.0).collect();
                    let normalize = |labels: &[String]| {
                        if labels.iter().all(String::is_empty) {
                            vec![]
                        } else {
                            labels.to_vec()
                        }
                    };
                    ensure!(
                        terms == expected.terms
                            && normalize(&value.root_tree.labels) == normalize(&expected.labels),
                        "restore commit tree mismatch"
                    );
                    // Keep the independent result reservation through decoded teardown.
                    drop(value);
                    drop(result.owner);
                }
                wc_verified = true;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let closed = call(
            deadline,
            "cloud_scope_close",
            query.close_scope(scope.handle, physical),
        )
        .await;
        validation?;
        closed?;
    }
    ensure!(wc_verified, "restore snapshot tree was not authorized");
    Ok(())
}

impl Registry {
    /// Reserve fresh private administration for an already authenticated source.
    /// This never provisions/creates a mount or publishes a Ready registration.
    pub fn begin_restore(
        self: &Arc<Self>,
        binding: &Binding,
        source: &PreparedRestore,
    ) -> Result<RestoreIntent> {
        check_time(source.source.deadline)?;
        self.validate(binding)?;
        ensure!(
            binding.repository == source.source.store_alias(),
            "restore destination alias mismatch"
        );
        let endpoint = source.source.endpoint();
        ensure!(
            binding.endpoint.local() == endpoint,
            "restore destination endpoint mismatch"
        );
        validate_marker(binding)?;
        let lock = self
            .root
            .open_lock(format!("initialize-{}.lock", binding.id()))?;
        match lock.try_lock_exclusive() {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(RestoreBusy.into());
            }
            Err(error) => return Err(error.into()),
        }
        ensure!(
            !binding.admin.try_exists()?,
            "restore administration exists; preserve and reconcile explicitly"
        );
        let data = &source.source.data;
        let journal = Journal {
            version: if data.remote.is_some() { 2 } else { 1 },
            binding: binding.clone(),
            source: source.source.identity.clone(),
            operation_heads: data.operation_heads.len() as u64,
            operation_heads_digest: {
                let mut hash = blake3::Hasher::new();
                hash.update(b"native-restore-operation-heads-v1\0");
                hash.update(&(data.operation_heads.len() as u64).to_le_bytes());
                for head in &data.operation_heads {
                    hash.update(head.as_bytes());
                }
                *hash.finalize().as_bytes()
            },
            wc_operation: data.working_copy.wc_operation.clone(),
            snapshot_operation: data.working_copy.snapshot_operation.clone(),
            snapshot_commit: Some(data.working_copy.snapshot_commit),
            workspace: data.working_copy.workspace.clone(),
            operation_indexes: source.source.operation_index_digest(),
            index_fingerprint: None,
            remote_state_digest: None,
            phase: Phase::Reserved,
        };
        // Reject an oversized journal before creating destination state. Full
        // heads remain in the exact immutable source; this record binds their
        // count and ordered digest, not a truncated or singleton approximation.
        let encoded = bounded_encode(&journal)?;
        let admin = PrivateDir::create(&binding.admin)?;
        File::open(self.admins.path())?.sync_all()?;
        admin.replace(RESTORE_JOURNAL, &encoded)?;
        Ok(RestoreIntent {
            registry: self.clone(),
            admin,
            journal,
            _lock: lock,
            deadline: source.source.deadline,
        })
    }
}
impl RestoreIntent {
    pub fn binding(&self) -> &Binding {
        &self.journal.binding
    }
    /// Create only this still-Reserved intent's fresh WC directory before the
    /// actual Controller opens it. Existing contents are never adopted/repaired.
    pub fn prepare_working_copy_directory(&mut self) -> Result<()> {
        check_time(self.deadline)?;
        ensure!(
            matches!(self.journal.phase, Phase::Reserved),
            "restore WC provisioning requires a reserved intent"
        );
        ensure!(
            !self.journal.binding.working_copy.try_exists()?,
            "restore WC directory already exists; preserve and reconcile explicitly"
        );
        let wc = PrivateDir::create(&self.journal.binding.working_copy)?;
        sync_directory(wc.path())?;
        sync_directory(self.admin.path())?;
        Ok(())
    }
    fn advance(&mut self, phase: Phase) -> Result<()> {
        self.journal.phase = phase;
        self.admin
            .replace(RESTORE_JOURNAL, &bounded_encode(&self.journal)?)?;
        Ok(())
    }
}
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}
fn install_directory(source: &Path, destination: &Path) -> Result<()> {
    let _source = PrivateDir::open_existing(source)?;
    let _parent = PrivateDir::open_existing(
        destination
            .parent()
            .context("restore destination has no parent")?,
    )?;
    let metadata = fs::symlink_metadata(destination)?;
    ensure!(
        metadata.is_dir()
            && !metadata.file_type().is_symlink()
            && fs::read_dir(destination)?.next().is_none(),
        "restore target is not fresh"
    );
    // Only an exclusive restore journal's known-empty jj-created slot is removed.
    // The owner-private parent and root lease exclude other supported writers.
    fs::remove_dir(destination)?;
    fs::rename(source, destination)?;
    sync_directory(
        destination
            .parent()
            .context("restore destination has no parent")?,
    )?;
    sync_directory(source.parent().context("restore stage has no parent")?)?;
    Ok(())
}
fn sync_stage(path: &Path, limits: &RestoreLimits, deadline: Instant) -> Result<()> {
    let mut pending = vec![(path.to_path_buf(), false, 0usize)];
    let mut entries = 0usize;
    let mut bytes = 0u64;
    while let Some((path, visited, depth)) = pending.pop() {
        check_time(deadline)?;
        ensure!(depth <= 32, "restore stage directory depth exceeded");
        if visited {
            sync_directory(&path)?;
            continue;
        }
        entries = entries
            .checked_add(1)
            .context("restore stage entry overflow")?;
        ensure!(
            entries <= limits.max_stage_entries,
            "restore stage entry limit exceeded"
        );
        let metadata = fs::symlink_metadata(&path)?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "restore stage contains a symlink"
        );
        if metadata.is_dir() {
            pending.push((path.clone(), true, depth));
            for entry in fs::read_dir(&path)? {
                ensure!(
                    pending
                        .len()
                        .checked_add(entries)
                        .is_some_and(|n| n < limits.max_stage_entries),
                    "restore stage traversal limit exceeded"
                );
                let path = entry?.path();
                ensure!(
                    path.as_os_str().len() <= 4096,
                    "restore stage path limit exceeded"
                );
                pending.push((path, false, depth + 1));
            }
        } else {
            ensure!(metadata.is_file(), "restore stage contains a special file");
            bytes = bytes
                .checked_add(metadata.len())
                .context("restore stage byte overflow")?;
            ensure!(
                bytes <= limits.max_sync_bytes,
                "restore stage sync byte limit exceeded"
            );
            File::open(&path)?.sync_all()?;
        }
    }
    Ok(())
}
fn backend_error(error: anyhow::Error) -> BackendInitError {
    BackendInitError(Box::new(std::io::Error::other(error.to_string())))
}

/// Install exact metadata under an exclusive journal, but deliberately do not
/// mark initialization Ready or publish registry registration. The future host
/// startup-fence integration must perform that distinct publication.
pub async fn initialize_restored(
    intent: RestoreIntent,
    prepared: PreparedRestore,
    host: RestoreHost,
) -> Result<StagedRestoreReceipt> {
    let deadline = prepared.source.deadline.min(intent.deadline);
    check_time(deadline)?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=worker_admission state=start\n",
    );
    owned_job(deadline, move || {
        let _ = std::io::Write::write_all(
            &mut std::io::stderr(),
            b"native_restore phase=worker_admission state=complete\n",
        );
        let _ = std::io::Write::write_all(
            &mut std::io::stderr(),
            b"native_restore phase=worker_execute state=start\n",
        );
        let result = futures::executor::block_on(stage(intent, prepared, host, deadline));
        let _ = std::io::Write::write_all(
            &mut std::io::stderr(),
            b"native_restore phase=worker_execute state=complete\n",
        );
        result
    })
    .await
}

fn operation_ids(source: &Source) -> Vec<[u8; 64]> {
    if let SourceKind::Native(s) = &source.kind {
        return s
            .manifest
            .history
            .operation_indexes
            .iter()
            .map(|entry| entry.operation)
            .collect();
    }
    source
        .objects()
        .iter()
        .filter_map(|object| match object.id() {
            c::ObjectId::Operation(id) => Some(id),
            _ => None,
        })
        .collect()
}
fn legacy_index_fingerprint(path: &Path, source: &Source) -> Result<[u8; 32]> {
    use jj_lib::default_index::{DefaultIndexGcLimits, DefaultIndexStore};
    let cap = source.limits.max_stage_entries;
    let bytes = source.limits.max_working_bytes.min(256 * 1024 * 1024);
    let bounds = DefaultIndexGcLimits {
        max_operations: source.limits.manifest.capture.max_operations,
        max_control_bytes: 65536,
        max_total_control_bytes: bytes,
        max_segment_bytes: bytes,
        max_total_segment_bytes: bytes,
        max_segments: cap,
        max_chain_depth: 128,
        max_commits: cap,
        max_parent_edges: cap.saturating_mul(16),
        max_validation_steps: cap.saturating_mul(64),
        max_roots: cap,
        max_working_bytes: bytes,
    };
    let mut ids = operation_ids(source)
        .iter()
        .map(|id| OperationId::from_bytes(id))
        .collect::<Vec<_>>();
    let intrinsic = OperationId::from_bytes(&[0; 64]);
    if !ids.contains(&intrinsic) {
        ids.push(intrinsic);
    }
    ids.sort();
    let mut roots = DefaultIndexStore::load(path).gc_roots_readonly(
        &ids,
        &OperationId::from_bytes(&[0; 64]),
        &CommitId::from_bytes(&m::CommitId::ROOT.0),
        16,
        bounds,
    )?;
    roots.sort();
    roots.dedup();
    let native = source.private().context("private Legacy source required")?;
    let commits = native
        .bundle
        .native_descriptors()
        .iter()
        .filter(|d| d.key.kind == m::Kind::Commit)
        .map(|d| d.key.id.0)
        .collect::<BTreeSet<_>>();
    for root in &roots {
        ensure!(
            root.as_bytes() == m::CommitId::ROOT.0
                || commits.contains(&<[u8; 32]>::try_from(root.as_bytes())?),
            "Legacy index root absent from verified bundle"
        );
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"private-restore-legacy-index-v1\0");
    for id in ids {
        hash.update(id.as_bytes());
    }
    for root in roots {
        hash.update(root.as_bytes());
    }
    Ok(*hash.finalize().as_bytes())
}
fn index_fingerprint(path: &Path, source: &Source) -> Result<[u8; 32]> {
    if source.private().is_some() {
        return legacy_index_fingerprint(path, source);
    }
    let reserved = jj_native_index::LocalIndexExportReader::required_read_bytes(
        &source.native()?.provider.index_limits,
    )?;
    ensure!(
        reserved <= source.limits.import.max_working_bytes,
        "restore index readback reservation insufficient"
    );
    let reader = jj_native_index::LocalIndexExportReader::open_existing(
        path,
        &source.native()?.provider.config,
        source.native()?.provider.index_limits.clone(),
        reserved,
    )?;
    reader
        .association_fingerprint(&operation_ids(source), 65_536, 256 * 1024 * 1024)
        .map_err(Into::into)
}

async fn import_private_native(source: &Arc<Source>, deadline: Instant) -> Result<()> {
    let Some(private) = source.private() else {
        return Ok(());
    };
    let reader = private.bundle.native_reader();
    let empty = m::empty_tree();
    let actual = private
        .provider
        .objects
        .write_metadata_with_owner(
            m::decode(&empty.bytes, m::Limits::default())?,
            source.clone(),
        )
        .await?;
    ensure!(
        actual == empty.descriptor,
        "private restore intrinsic empty TREE differs"
    );
    for descriptor in private.bundle.native_descriptors() {
        check_time(deadline)?;
        if descriptor.key.kind == m::Kind::CopyHistory {
            continue;
        }
        if descriptor.key.kind == m::Kind::File {
            let (actual, mut input) = reader
                .read_file_with_owner(m::FileId(descriptor.key.id.0), source.clone())
                .await?
                .into_parts();
            ensure!(
                actual == *descriptor,
                "private restore FILE descriptor differs"
            );
            let id = private
                .provider
                .objects
                .write_file_with_owner(&mut input, descriptor.size_bytes, source.clone())
                .await?;
            ensure!(
                id.0 == descriptor.key.id.0,
                "private restore FILE identity differs"
            );
        } else {
            let metadata = reader
                .read_metadata_with_owner(descriptor.key, source.clone())
                .await?;
            ensure!(
                metadata.descriptor() == *descriptor,
                "private restore metadata descriptor differs"
            );
            let actual = private
                .provider
                .objects
                .write_metadata_with_owner(metadata.into_object(), source.clone())
                .await?;
            ensure!(
                actual == *descriptor,
                "private restore metadata identity differs"
            );
        }
    }
    // The direct provider requires each copy parent to be installed first. The
    // source catalog is immutable and bounded; this walk never uses a network.
    let mut done = BTreeSet::new();
    let mut visiting = BTreeSet::new();
    let mut work = 0usize;
    for descriptor in private
        .bundle
        .native_descriptors()
        .iter()
        .filter(|d| d.key.kind == m::Kind::CopyHistory)
    {
        let mut stack = vec![(descriptor.key, false)];
        while let Some((key, expanded)) = stack.pop() {
            check_time(deadline)?;
            if done.contains(&key.id.0) {
                continue;
            }
            work = work
                .checked_add(1)
                .context("private restore copy work overflow")?;
            ensure!(
                work <= source.limits.manifest.binding.max_work as usize,
                "private restore copy work limit"
            );
            let metadata = reader.read_metadata_with_owner(key, source.clone()).await?;
            let expected = metadata.descriptor();
            let object = metadata.into_object();
            if expanded {
                let actual = private
                    .provider
                    .objects
                    .write_metadata_with_owner(object, source.clone())
                    .await?;
                ensure!(actual == expected, "private restore copy identity differs");
                visiting.remove(&key.id.0);
                done.insert(key.id.0);
            } else {
                ensure!(
                    visiting.insert(key.id.0),
                    "private restore cyclic copy history"
                );
                stack.push((key, true));
                let m::Object::CopyHistory(copy) = object else {
                    anyhow::bail!("private restore copy type differs")
                };
                for parent in copy.parents.iter().rev() {
                    stack.push((
                        m::ObjectKey {
                            kind: m::Kind::CopyHistory,
                            id: m::ObjectId(parent.0),
                        },
                        false,
                    ));
                }
            }
        }
    }
    source.reauthorize(deadline).await
}

async fn stage(
    mut intent: RestoreIntent,
    prepared: PreparedRestore,
    host: RestoreHost,
    deadline: Instant,
) -> Result<StagedRestoreReceipt> {
    let source = prepared.source;
    ensure!(
        intent.journal.source == source.identity.clone(),
        "restore intent source changed"
    );
    ensure!(
        matches!(intent.journal.phase, Phase::Reserved),
        "restore journal already contains staged state"
    );
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=marker_validation state=start\n",
    );
    intent.registry.validate(&intent.journal.binding)?;
    validate_marker(&intent.journal.binding)?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=marker_validation state=complete\n",
    );
    let binding = intent.journal.binding.clone();
    let mut peak = source.retained_bytes;
    add(
        &mut peak,
        source.limits.manifest.history.artifacts.max_decode_bytes,
        source.limits.max_working_bytes,
    )?;
    add(
        &mut peak,
        source.limits.import.max_working_bytes,
        source.limits.max_working_bytes,
    )?;
    add(
        &mut peak,
        source.limits.manifest.binding.max_bytes,
        source.limits.max_working_bytes,
    )?;
    // OP/VIEW codec work finishes before the index importer starts. Both
    // peaks retain the source, borrowed artifact graph and backend owner; do
    // not reserve two mutually exclusive workers in the same peak.
    let mut raw_peak = source.retained_bytes;
    add(
        &mut raw_peak,
        source.limits.manifest.history.artifacts.max_decode_bytes,
        source.limits.max_working_bytes,
    )?;
    add(
        &mut raw_peak,
        source.limits.manifest.binding.max_bytes,
        source.limits.max_working_bytes,
    )?;
    let codec = c::Limits::default();
    add(
        &mut raw_peak,
        codec.max_decode_bytes,
        source.limits.max_working_bytes,
    )?;
    add(
        &mut raw_peak,
        codec
            .max_payload_bytes
            .checked_mul(2)
            .context("restore raw import memory overflow")?,
        source.limits.max_working_bytes,
    )?;
    // The pending-operation map and installed-ID set contain references/IDs,
    // not copied payloads. Charge their node/slot slack in this phase.
    add(
        &mut raw_peak,
        source
            .objects()
            .len()
            .checked_add(1)
            .and_then(|n| n.checked_mul(512))
            .context("restore ordering memory overflow")?,
        source.limits.max_working_bytes,
    )?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=projection_inspect state=start\n",
    );
    let projected = host.connector.connect(&binding.projection())?.inspect()?;
    let expected_tree = jj_vfs::native::NativeTree {
        terms: source.data.working_copy.tree.terms.clone(),
        labels: source.data.working_copy.tree.labels.clone(),
    };
    ensure!(
        projected.repository == binding.repository && projected.tree == expected_tree,
        "restore lazy projection mismatch"
    );
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=projection_inspect state=complete\n",
    );
    // All asynchronous backend preparation precedes jj's synchronous factory.
    let objects: Arc<dyn NativeObjectIo> = Arc::new(RestoreObjectIo {
        inner: source.objects_io(),
        owner: source.clone(),
    });
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=backend_empty_tree state=start\n",
    );
    if source.private().is_some() {
        source.reauthorize(deadline).await?;
        import_private_native(&source, deadline).await?;
    }
    let backend = NativeBackend::from_io(objects.clone(), host.backend_config).await?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=backend_empty_tree state=complete\n",
    );
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=artifact_graph state=start\n",
    );
    let graph = if let SourceKind::Native(native) = &source.kind {
        Some(c::verify_index_artifact_graph(
            &native.manifest.history.index_roots,
            &native.inputs.artifacts,
            source.limits.manifest.history.artifacts,
        )?)
    } else {
        None
    };
    source.reauthorize(deadline).await?;
    check_time(deadline)?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=stage_create state=start\n",
    );
    let staging = PrivateDir::create(intent.admin.path().join("restore-staging"))?;
    sync_directory(intent.admin.path())?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=stage_create state=complete\n",
    );
    let op_path = staging.path().join("op_store");
    let op_directory = PrivateDir::create(&op_path)?;
    let root_data = || RootOperationData {
        root_commit_id: CommitId::from_bytes(&[0; 32]),
    };
    let op_store = SimpleOpStore::init(op_directory.path(), root_data())?;
    ensure!(
        op_store.root_operation_id().as_bytes() == [0; 64],
        "restore intrinsic operation convention mismatch"
    );
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=views_import state=start\n",
    );
    for object in source.objects() {
        if matches!(object.id(), c::ObjectId::View(_)) {
            check_time(deadline)?;
            object.import(&op_store).await?;
        }
    }
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=views_import state=complete\n",
    );
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=operations_import state=start\n",
    );
    let mut installed = BTreeSet::from([[0; 64]]);
    let mut pending: BTreeMap<_, _> = source
        .objects()
        .iter()
        .filter_map(|object| match object.id() {
            c::ObjectId::Operation(id) if id != [0; 64] => Some((id, object)),
            _ => None,
        })
        .collect();
    let mut ordering_work = 0u64;
    while !pending.is_empty() {
        check_time(deadline)?;
        let mut next = None;
        for (id, object) in &pending {
            ordering_work = ordering_work
                .checked_add(object.dependencies().operations.len() as u64 + 1)
                .context("restore ordering work overflow")?;
            ensure!(
                ordering_work <= source.limits.manifest.binding.max_work,
                "restore operation ordering work limit exceeded"
            );
            if object
                .dependencies()
                .operations
                .iter()
                .all(|id| installed.contains(id))
            {
                next = Some(*id);
                break;
            }
        }
        let next = next.context("restore operation ancestry is incomplete")?;
        pending.remove(&next).unwrap().import(&op_store).await?;
        installed.insert(next);
    }
    drop(op_store);
    drop(pending);
    drop(installed);
    sync_directory(&op_path)?;
    sync_directory(staging.path())?;
    intent.advance(Phase::ObjectsStaged)?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=operations_import state=complete\n",
    );
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=index_import state=start\n",
    );
    let index_path = staging.path().join("index");
    if source.private().is_none() {
        let imported = NativeIndexStore::import_fresh_operation_indexes(
            &index_path,
            source.native()?.provider.config.clone(),
            source.native()?.provider.index_limits.clone(),
            source.native()?.provider.query.clone(),
            objects.clone(),
            import_version(&source),
            source.native()?.inputs.version.version().manifest(),
            graph.as_ref().context("native index graph missing")?,
            source.limits.import.clone(),
            source._owner.clone(),
        )
        .await?;
        let cleanup = imported.store.cleanup_handle();
        drop(imported);
        cleanup.wait(Duration::from_millis(
            source.native()?.provider.index_limits.cleanup_timeout_ms,
        ))?;
        drop(graph);
        intent.journal.index_fingerprint = Some(index_fingerprint(&index_path, &source)?);
    }
    intent.advance(Phase::IndexStaged)?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=index_import state=complete\n",
    );
    check_time(deadline)?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=repository_install state=start\n",
    );
    let repository = PrivateDir::create(&binding.repo)?;
    let backend = RefCell::new(Some(backend));
    let backend_initializer: &BackendInitializer<'_> = &|_, path| {
        (|| -> Result<Box<dyn Backend>> {
            (host.backend_metadata)(path)?;
            Ok(Box::new(
                backend
                    .borrow_mut()
                    .take()
                    .context("restore backend factory reused")?,
            ))
        })()
        .map_err(backend_error)
    };
    let op_initializer: &jj_lib::repo::OpStoreInitializer<'_> = &|_, path, data| {
        (|| -> Result<Box<dyn OpStore>> {
            ensure!(
                data.root_commit_id.as_bytes() == [0; 32],
                "restore backend root mismatch"
            );
            install_directory(&op_path, path)?;
            Ok(Box::new(SimpleOpStore::load(path, data)))
        })()
        .map_err(backend_error)
    };
    let index_initializer: &jj_lib::repo::IndexStoreInitializer<'_> = &|_, path| {
        (|| -> Result<Box<dyn jj_lib::index::IndexStore>> {
            if source.private().is_some() {
                return ReadonlyRepo::default_index_store_initializer()(&host.settings, path)
                    .map_err(anyhow::Error::from);
            }
            install_directory(&index_path, path)?;
            Ok(Box::new(NativeIndexStore::open(
                path,
                &source.native()?.provider.config,
                source.native()?.provider.index_limits.clone(),
                source.native()?.provider.query.clone(),
                objects.clone(),
            )?))
        })()
        .map_err(backend_error)
    };
    let heads = source
        .data
        .operation_heads
        .iter()
        .map(|text| array::<64>(text).map(|id| OperationId::from_bytes(&id)))
        .collect::<Result<Vec<_>>>()?;
    let heads_initializer: &jj_lib::repo::OpHeadsStoreInitializer<'_> = &|_, path, _| {
        SimpleOpHeadsStore::init_cloud_restore(
            path,
            &heads,
            source.limits.manifest.capture.max_operations,
        )
        .map(|store| Box::new(store) as Box<dyn jj_lib::op_heads_store::OpHeadsStore>)
        .map_err(|error| backend_error(error.into()))
    };
    intent.advance(Phase::InstallingRepository)?;
    let repo = ReadonlyRepo::init(
        &host.settings,
        repository.path(),
        backend_initializer,
        host.signer,
        ReadonlyRepo::default_workspace_store_initializer(),
        op_initializer,
        heads_initializer,
        index_initializer,
        ReadonlyRepo::default_submodule_store_initializer(),
    )
    .await?;
    intent.advance(Phase::RepositoryInstalled)?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=repository_install state=complete\n",
    );
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=metadata_readback state=start\n",
    );
    let op_store = repo.op_store();
    for object in source.objects() {
        match object.id() {
            c::ObjectId::Operation(id) => {
                let actual = op_store
                    .read_operation(&OperationId::from_bytes(&id))
                    .await?;
                ensure!(
                    Some(&actual) == object.operation(),
                    "restored operation semantic data changed"
                );
            }
            c::ObjectId::View(id) => {
                let actual = op_store
                    .read_view(&jj_lib::op_store::ViewId::from_bytes(&id))
                    .await?;
                ensure!(
                    Some(&actual) == object.view(),
                    "restored view semantic data changed"
                );
            }
        }
    }
    for operation in operation_ids(&source) {
        check_time(deadline)?;
        let id = OperationId::from_bytes(&operation);
        let data = op_store.read_operation(&id).await?;
        let operation = jj_lib::operation::Operation::new(op_store.clone(), id, data);
        let index = repo
            .loader()
            .index_store()
            .get_index_at_op(&operation, repo.store())
            .await?;
        drop(index);
    }
    if source.private().is_some() {
        intent.journal.index_fingerprint =
            Some(index_fingerprint(&binding.repo.join("index"), &source)?);
        intent.advance(Phase::RepositoryInstalled)?;
    }
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=metadata_readback state=complete\n",
    );
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=working_copy_restore state=start\n",
    );
    let working_copy = PrivateDir::create(&binding.working_copy)?;
    let factory = NativeVfsWorkingCopyFactory::new(binding.repository.clone(), host.connector);
    let wc_operation = array::<64>(&source.data.working_copy.wc_operation)?;
    let snapshot_operation = array::<64>(&source.data.working_copy.snapshot_operation)?;
    let wc = factory.restore_fresh_working_copy(
        repo.store().clone(),
        binding.root.clone(),
        binding.working_copy.clone(),
        OperationId::from_bytes(&wc_operation),
        source.data.working_copy.workspace.clone().into(),
        &expected_tree,
    )?;
    working_copy.replace("type", wc.name().as_bytes())?;
    if let Some(bytes) = portable_remote_bytes(&source.data, &host.remote_scope)? {
        ensure!(
            host.remote_scope.store == binding.repository,
            "restore remote repository differs"
        );
        match working_copy.read("native-remote.toml", 1024 * 1024) {
            Ok(existing) => ensure!(
                existing == bytes,
                "restore remote journal differs; preserve state"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                working_copy.replace("native-remote.toml", &bytes)?
            }
            Err(error) => return Err(error.into()),
        }
        intent.journal.remote_state_digest = Some(*blake3::hash(&bytes).as_bytes());
    }

    let workspace = Workspace::new(
        &binding.root,
        binding.repo.clone(),
        wc,
        repo.loader().clone(),
    )?;
    repo.loader()
        .workspace_store()
        .add(workspace.workspace_name(), workspace.workspace_root())?;
    ensure!(
        workspace.working_copy().operation_id().as_bytes() == wc_operation,
        "restore WC operation changed"
    );
    let heads_path = binding.repo.join("op_heads");
    let readback = SimpleOpHeadsStore::load(&heads_path).get_op_heads_bounded(heads.len())?;
    ensure!(readback == heads, "restore operation heads changed");
    drop(workspace);
    drop(repo);
    let mut sync_peak = source.retained_bytes;
    add(
        &mut sync_peak,
        source
            .limits
            .max_stage_entries
            .checked_mul(4096 + 128)
            .context("restore sync memory overflow")?,
        source.limits.max_working_bytes,
    )?;
    sync_stage(&binding.repo, &source.limits, deadline)?;
    sync_stage(&binding.working_copy, &source.limits, deadline)?;
    sync_directory(intent.admin.path())?;
    check_time(deadline)?;
    intent.advance(Phase::WorkingCopyStaged)?;
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        b"native_restore phase=working_copy_restore state=complete\n",
    );
    Ok(StagedRestoreReceipt {
        binding,
        operation_heads: heads
            .iter()
            .map(|id| id.as_bytes().try_into().unwrap())
            .collect(),
        wc_operation,
        snapshot_operation,
        snapshot_commit: source.data.working_copy.snapshot_commit,
        tree: expected_tree,
        authority: StageAuthority { intent, source },
    })
}

fn restore_origin(source: &RestoreSourceIdentity) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"native-restore-origin-v1\0");
    hash.update(&source.workspace);
    hash.update(&source.generation.to_le_bytes());
    hash.update(&source.manifest);
    hash.update(&source.manifest_blake3);
    hash.update(&source.manifest_bytes.to_le_bytes());
    hash.update(&source.capture);
    hash.update(&source.capture_blake3);
    hash.update(&source.capture_bytes.to_le_bytes());
    hash.update(&source.provider);
    if let Some(base) = &source.private_review_base {
        hash.update(b"private-review-base-v1\0");
        hash.update(&base.view_version);
        hash.update(&base.policy_epoch.to_le_bytes());
        hash.update(&base.base);
        hash.update(&base.tree);
    }
    if let Some(binding) = &source.private_capsule {
        hash.update(b"private-capsule-source-v1\0");
        hash.update(&serde_json::to_vec(binding).expect("validated capsule binding"));
    }
    *hash.finalize().as_bytes()
}
fn recovery<T>(reason: &'static str) -> Result<T> {
    Err(RestoreRecoveryRequired(reason).into())
}
fn portable_remote_bytes(
    data: &RestoreState,
    scope: &crate::remote_state::RemoteScope,
) -> Result<Option<Vec<u8>>> {
    data.remote
        .as_ref()
        .map(|remote| -> Result<Vec<u8>> {
            let state = crate::remote_state::State::from_portable(scope, remote)?;
            let bytes = toml::to_string(&state)?.into_bytes();
            ensure!(
                bytes.len() <= 1024 * 1024,
                "restored remote state exceeds byte bound"
            );
            Ok(bytes)
        })
        .transpose()
}
fn read_journal(admin: &PrivateDir) -> Result<Journal> {
    let journal: Journal = serde_json::from_slice(&admin.read(RESTORE_JOURNAL, MAX_RECORD)?)?;
    ensure!(
        matches!(journal.version, 1 | 2),
        "unsupported restore journal version"
    );
    Ok(journal)
}
fn check_journal_source(journal: &Journal, binding: &Binding, source: &Source) -> Result<()> {
    ensure!(
        journal.binding == *binding && journal.source == source.identity.clone(),
        "restore journal source or destination differs"
    );
    let data = &source.data;
    let mut heads = blake3::Hasher::new();
    heads.update(b"native-restore-operation-heads-v1\0");
    heads.update(&(data.operation_heads.len() as u64).to_le_bytes());
    for head in &data.operation_heads {
        heads.update(head.as_bytes());
    }
    ensure!(
        journal.operation_heads == data.operation_heads.len() as u64
            && journal.operation_heads_digest == *heads.finalize().as_bytes()
            && journal.operation_indexes == source.operation_index_digest()
            && journal.wc_operation == data.working_copy.wc_operation
            && journal.snapshot_operation == data.working_copy.snapshot_operation
            && journal.workspace == data.working_copy.workspace,
        "restore journal inventory differs"
    );
    if journal.index_fingerprint.is_none() || journal.snapshot_commit.is_none() {
        return recovery("restore staged journal lacks exact readback binding; preserve state");
    }
    ensure!(
        journal.snapshot_commit == Some(data.working_copy.snapshot_commit),
        "restore journal snapshot differs"
    );
    Ok(())
}
fn receipt_from_authority(authority: StageAuthority) -> Result<StagedRestoreReceipt> {
    let source = &authority.source;
    Ok(StagedRestoreReceipt {
        binding: authority.intent.journal.binding.clone(),
        operation_heads: source
            .data
            .operation_heads
            .iter()
            .map(|id| array(id))
            .collect::<Result<_>>()?,
        wc_operation: array(&source.data.working_copy.wc_operation)?,
        snapshot_operation: array(&source.data.working_copy.snapshot_operation)?,
        snapshot_commit: source.data.working_copy.snapshot_commit,
        tree: jj_vfs::native::NativeTree {
            terms: source.data.working_copy.tree.terms.clone(),
            labels: source.data.working_copy.tree.labels.clone(),
        },
        authority,
    })
}
fn check_receipt(receipt: &StagedRestoreReceipt) -> Result<()> {
    let source = &receipt.authority.source;
    let heads = source
        .data
        .operation_heads
        .iter()
        .map(|id| array(id))
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        receipt.binding == receipt.authority.intent.journal.binding
            && receipt.operation_heads == heads
            && receipt.wc_operation == array::<64>(&source.data.working_copy.wc_operation)?
            && receipt.snapshot_operation
                == array::<64>(&source.data.working_copy.snapshot_operation)?
            && receipt.snapshot_commit == source.data.working_copy.snapshot_commit
            && receipt.tree.terms == source.data.working_copy.tree.terms
            && receipt.tree.labels == source.data.working_copy.tree.labels,
        "restore diagnostic receipt differs from sealed source"
    );
    Ok(())
}
fn expected_names<'a>(ids: impl Iterator<Item = &'a [u8; 64]>) -> BTreeSet<String> {
    ids.map(|id| hex(id)).collect()
}
fn scan_exact_files(path: &Path, expected: &BTreeSet<String>, maximum: usize) -> Result<()> {
    let mut found = BTreeSet::new();
    for (count, entry) in fs::read_dir(path)?.enumerate() {
        ensure!(
            count < maximum && count < expected.len(),
            "restore inventory contains extra files"
        );
        let entry = entry?;
        let metadata = entry.file_type()?;
        ensure!(
            metadata.is_file() && !metadata.is_symlink(),
            "restore inventory contains nonregular files"
        );
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("restore inventory name encoding"))?;
        ensure!(
            expected.contains(&name) && found.insert(name),
            "restore inventory has an unknown slot"
        );
    }
    ensure!(&found == expected, "restore inventory is incomplete");
    Ok(())
}
struct ReadbackSink<'a> {
    expected: &'a BTreeMap<c::IndexArtifactId, c::IndexArtifactDescriptor>,
    seen: BTreeSet<c::IndexArtifactId>,
}
#[async_trait::async_trait]
impl jj_native_index::IndexArtifactSink for ReadbackSink<'_> {
    async fn write(
        &mut self,
        artifact: jj_native_index::ExportArtifact,
    ) -> std::result::Result<(), jj_native_index::Error> {
        let descriptor = artifact.descriptor();
        if self.expected.get(&descriptor.id) != Some(&descriptor)
            || !self.seen.insert(descriptor.id)
        {
            return Err(jj_native_index::Error::Corrupt);
        }
        Ok(())
    }
}
async fn readback(
    authority: &StageAuthority,
    host: &RestoreReadbackHost,
    deadline: Instant,
) -> Result<()> {
    let source = &authority.source;
    let binding = &authority.intent.journal.binding;
    authority.intent.registry.validate(binding)?;
    validate_marker(binding)?;
    let actual_journal = read_journal(&authority.intent.admin)?;
    check_journal_source(&actual_journal, binding, source)?;
    match actual_journal.phase {
        Phase::WorkingCopyStaged | Phase::PublishingReady => {}
        Phase::Finalized => {
            return recovery("restore is ordinary-resume eligible; do not replay captured state");
        }
        _ => return recovery("restore stage is incomplete; preserve state for explicit recovery"),
    }
    let expected_remote = portable_remote_bytes(&source.data, &host.remote_scope)?;
    if let Some(bytes) = &expected_remote {
        ensure!(
            host.remote_scope.store == binding.repository
                && actual_journal.version == 2
                && actual_journal.remote_state_digest == Some(*blake3::hash(bytes).as_bytes()),
            "restore remote sidecar binding differs"
        );
    } else {
        ensure!(
            actual_journal.remote_state_digest.is_none(),
            "unexpected restore remote sidecar binding"
        );
    }
    let wc_directory = PrivateDir::open_existing(&binding.working_copy)?;
    match (
        expected_remote,
        wc_directory.read("native-remote.toml", 1024 * 1024),
    ) {
        (Some(expected), Ok(actual)) => ensure!(
            actual == expected,
            "restore remote journal changed; preserve state"
        ),
        (None, Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return recovery("restore remote journal unavailable or mismatched; preserve state"),
    }
    // Sequential peaks each include all still-live source allocations.
    let mut graph_peak = source.retained_bytes;
    add(
        &mut graph_peak,
        source.limits.manifest.history.artifacts.max_decode_bytes,
        source.limits.max_working_bytes,
    )?;
    let codec = c::Limits::default();
    let mut metadata_peak = source.retained_bytes;
    add(
        &mut metadata_peak,
        codec.max_decode_bytes,
        source.limits.max_working_bytes,
    )?;
    add(
        &mut metadata_peak,
        codec
            .max_payload_bytes
            .checked_mul(3)
            .context("restore local codec memory overflow")?,
        source.limits.max_working_bytes,
    )?;
    let mut export_peak = source.retained_bytes;
    add(
        &mut export_peak,
        source.limits.import.max_working_bytes,
        source.limits.max_working_bytes,
    )?;
    add(
        &mut export_peak,
        source
            .artifacts()
            .len()
            .checked_mul(1024)
            .context("restore readback inventory overflow")?,
        source.limits.max_working_bytes,
    )?;
    source.reauthorize(deadline).await?;
    let repository = PrivateDir::open_existing(&binding.repo)?;
    for (child, expected) in [
        ("store", NativeBackend::NAME),
        (
            "index",
            if source.private().is_some() {
                jj_lib::default_index::DefaultIndexStore::NAME
            } else {
                jj_native_index::NAME
            },
        ),
        ("op_store", SimpleOpStore::NAME),
        ("op_heads", SimpleOpHeadsStore::NAME),
        (
            "workspace_store",
            jj_lib::simple_workspace_store::SimpleWorkspaceStore::NAME,
        ),
        (
            "submodule_store",
            jj_lib::default_submodule_store::DefaultSubmoduleStore::NAME,
        ),
    ] {
        let bytes = fs::File::open(repository.path().join(child).join("type"))
            .with_context(|| format!("restore {child} store type unavailable"))?
            .take(129)
            .bytes()
            .collect::<std::io::Result<Vec<_>>>()?;
        ensure!(bytes == expected.as_bytes(), "restore store type differs");
    }
    (host.backend_metadata_check)(&binding.repo.join("store"))?;
    let simple = SimpleOpStore::load(
        &binding.repo.join("op_store"),
        RootOperationData {
            root_commit_id: CommitId::from_bytes(&[0; 32]),
        },
    );
    let mut operations = BTreeSet::new();
    let mut views = BTreeSet::new();
    for object in source.objects() {
        check_time(deadline)?;
        let bytes = match object.id() {
            c::ObjectId::Operation(id) => {
                operations.insert(hex(&id));
                simple.read_cloud_operation_payload(
                    &OperationId::from_bytes(&id),
                    Default::default(),
                )?
            }
            c::ObjectId::View(id) => {
                views.insert(hex(&id));
                simple.read_cloud_view_payload(
                    &jj_lib::op_store::ViewId::from_bytes(&id),
                    Default::default(),
                )?
            }
        };
        // A semantic ID can have multiple legal protobuf representations. The
        // selected immutable capture binds this exact representation as well.
        ensure!(
            Some(bytes.as_slice()) == object.payload()
                && Some(c::BlobDigest::of(&bytes)) == object.digest(),
            "restore OP/VIEW selected representation differs; preserve state"
        );
        let actual =
            c::VerifiedObject::verify(object.id(), c::BlobDigest::of(&bytes), &bytes, codec)?;
        ensure!(
            actual.operation() == object.operation() && actual.view() == object.view(),
            "restore OP/VIEW semantic contents differ"
        );
    }
    scan_exact_files(
        &binding.repo.join("op_store/operations"),
        &operations,
        source.limits.manifest.capture.max_operations,
    )?;
    scan_exact_files(
        &binding.repo.join("op_store/views"),
        &views,
        source.limits.manifest.history.recovery.max_members,
    )?;
    let ids = operation_ids(source);
    let associations = binding.repo.join(if source.private().is_some() {
        "index/op_links"
    } else {
        "index/operations"
    });
    let mut index_names = expected_names(ids.iter());
    if source.private().is_some() {
        let root = hex(&[0; 64]);
        // DefaultIndexStore may cache its intrinsic root association. Validate
        // either representation through gc_roots_readonly, never require repair.
        if associations.join(&root).try_exists()? {
            index_names.insert(root);
        } else {
            index_names.remove(&root);
        }
    }
    scan_exact_files(
        &associations,
        &index_names,
        source.limits.manifest.capture.max_operations,
    )
    .context("restore index association inventory unavailable or changed")?;
    ensure!(
        Some(index_fingerprint(&binding.repo.join("index"), source)?)
            == actual_journal.index_fingerprint,
        "restore index source or association changed"
    );
    if let SourceKind::Native(native) = &source.kind {
        let objects: Arc<dyn NativeObjectIo> = Arc::new(RestoreObjectIo {
            inner: source.objects_io(),
            owner: source.clone(),
        });
        // Actual configured-provider verification, through owned metadata only.
        let backend = NativeBackend::from_io(objects.clone(), host.backend_config.clone()).await?;
        drop(backend);
        let index = NativeIndexStore::open(
            &binding.repo.join("index"),
            &source.native()?.provider.config,
            source.native()?.provider.index_limits.clone(),
            source.native()?.provider.query.clone(),
            objects,
        )?;
        let expected = source
            .artifacts()
            .iter()
            .map(|artifact| (artifact.descriptor().id, artifact.descriptor()))
            .collect::<BTreeMap<_, _>>();
        let mut sink = ReadbackSink {
            expected: &expected,
            seen: BTreeSet::new(),
        };
        let export_limits = jj_native_index::PortableExportLimits {
            max_operations: source.limits.import.max_operations,
            max_working_bytes: source.limits.import.max_working_bytes,
            artifacts: source.limits.manifest.history.artifacts,
            ..Default::default()
        };
        let exported = index
            .export_operation_indexes(&ids, export_limits, &mut sink, source.clone())
            .await?;
        ensure!(
            exported.operation_indexes() == native.manifest.history.operation_indexes
                && exported.index_roots() == native.manifest.history.index_roots
                && sink.seen.len() == expected.len(),
            "restore exact logical index inventory changed"
        );
        drop(exported);
        let cleanup = index.cleanup_handle();
        drop(index);
        cleanup.wait(Duration::from_millis(
            source.native()?.provider.index_limits.cleanup_timeout_ms,
        ))?;
    }
    let heads = SimpleOpHeadsStore::load(&binding.repo.join("op_heads"))
        .get_op_heads_bounded(source.limits.manifest.capture.max_operations)?;
    ensure!(
        heads
            .iter()
            .map(|id| id.hex())
            .eq(source.data.operation_heads.iter().cloned()),
        "restore operation heads differ"
    );
    let (wc, projection) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)?;
    ensure!(
        !wc.pending
            && wc.operation.hex() == source.data.working_copy.wc_operation
            && wc.workspace.as_str() == source.data.working_copy.workspace
            && wc.trees.len() == 1
            && wc.trees[0].terms == source.data.working_copy.tree.terms
            && wc.trees[0].labels == source.data.working_copy.tree.labels,
        "restore working copy changed or has pending state"
    );
    // The pinned loader repairs a missing legacy index. Require its actual
    // bounded current file before calling the present, read-only load path.
    let workspace_path = binding.repo.join("workspace_store/index");
    let metadata = fs::symlink_metadata(&workspace_path)
        .context("restore workspace registration index unavailable")?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() <= 1024 * 1024,
        "restore workspace registration is unavailable or oversized"
    );
    let mut registration_peak = source.retained_bytes;
    add(
        &mut registration_peak,
        usize::try_from(metadata.len())?
            .checked_mul(64)
            .context("restore workspace registration memory overflow")?,
        source.limits.max_working_bytes,
    )?;
    let workspace_store = jj_lib::simple_workspace_store::SimpleWorkspaceStore::load(
        &binding.repo.join("workspace_store"),
    )?;
    use jj_lib::workspace_store::WorkspaceStore as _;
    ensure!(
        workspace_store
            .get_workspace_path(wc.workspace.as_ref())?
            .as_ref()
            == Some(&binding.root),
        "restore workspace registration differs"
    );
    let current = host.connector.connect(&binding.projection())?.inspect()?;
    ensure!(current == projection, "restore projection is not current");
    check_time(deadline)?;
    Ok(())
}

/// Explicit pre-eligibility recovery only. A journal alone cannot provide source
/// authority; the caller must freshly prepare the exact current-client source.
/// Earlier partial stages and already-finalized origins are never adopted here.
pub async fn reopen_staged_restore(
    registry: Arc<Registry>,
    binding: Binding,
    prepared: PreparedRestore,
    host: RestoreReadbackHost,
) -> Result<StagedRestoreReceipt> {
    let deadline = prepared.source.deadline;
    check_time(deadline)?;
    owned_job(deadline, move || {
        futures::executor::block_on(async move {
            registry.validate(&binding)?;
            validate_marker(&binding)?;
            let lock = registry
                .root
                .open_lock(format!("initialize-{}.lock", binding.id()))?;
            match lock.try_lock_exclusive() {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Err(RestoreBusy.into());
                }
                Err(error) => return Err(error.into()),
            }
            let admin = PrivateDir::open_existing(&binding.admin)?;
            let journal = read_journal(&admin)?;
            check_journal_source(&journal, &binding, &prepared.source)?;
            let authority = StageAuthority {
                intent: RestoreIntent {
                    registry,
                    admin,
                    journal,
                    _lock: lock,
                    deadline,
                },
                source: prepared.source,
            };
            readback(&authority, &host, deadline)
                .await
                .context("restore staged metadata readback failed")?;
            receipt_from_authority(authority)
        })
    })
    .await
}

/// Complete exact staged metadata while the trusted host retains its actual
/// frontend AND command startup gates. This writes monotone ordinary-resume
/// eligibility before returning; it never activates either gate or rewrites a
/// later user operation back to the captured state.
pub async fn finalize_staged_restore(
    receipt: StagedRestoreReceipt,
    host: RestoreReadbackHost,
) -> Result<AdminRestoreReady> {
    let deadline = receipt
        .authority
        .source
        .deadline
        .min(receipt.authority.intent.deadline);
    check_time(deadline)?;
    owned_job(deadline, move || {
        futures::executor::block_on(async move {
            check_receipt(&receipt)?;
            let mut authority = receipt.authority;
            readback(&authority, &host, deadline)
                .await
                .context("restore staged metadata readback failed")?;
            let binding = authority.intent.journal.binding.clone();
            let initialization = Initialization {
                version: 1,
                binding: binding.clone(),
                initial_commit: authority.source.data.working_copy.snapshot_commit.to_vec(),
                workspace: authority.source.data.working_copy.workspace.clone(),
                ready: true,
                restore_origin: Some(restore_origin(&authority.intent.journal.source)),
            };
            let bytes = bounded_encode(&initialization)?;
            match authority.intent.admin.read(JOURNAL, MAX_RECORD) {
                Ok(existing) => {
                    let existing: Initialization = serde_json::from_slice(&existing)?;
                    ensure!(
                        existing.version == initialization.version
                            && existing.binding == initialization.binding
                            && existing.initial_commit == initialization.initial_commit
                            && existing.workspace == initialization.workspace
                            && existing.ready
                            && existing.restore_origin == initialization.restore_origin,
                        "restore normal initialization journal differs"
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let mut peak = authority.source.retained_bytes;
            add(
                &mut peak,
                authority
                    .source
                    .limits
                    .max_stage_entries
                    .checked_mul(4096 + 128)
                    .context("restore sync memory overflow")?,
                authority.source.limits.max_working_bytes,
            )?;
            sync_stage(&binding.repo, &authority.source.limits, deadline)?;
            sync_stage(&binding.working_copy, &authority.source.limits, deadline)?;
            // Local validation and fsync can outlive the previous source check.
            // Fence eligibility with current authority at the original deadline.
            authority.source.reauthorize(deadline).await?;
            check_time(deadline)?;
            authority.intent.advance(Phase::PublishingReady)?;
            authority.intent.admin.replace(JOURNAL, &bytes)?;
            // This is durable ordinary-resume eligibility BEFORE any host activation.
            // Once set, resume preserves CURRENT native state, not original heads/WC.
            authority.intent.advance(Phase::Finalized)?;
            authority.intent.registry.publish_ready(&binding)?;
            Ok(AdminRestoreReady {
                binding,
                source: authority.source.identity.clone(),
            })
        })
    })
    .await
}

pub(super) fn ready_attachment(
    binding: &Binding,
    initialization: &Initialization,
) -> Result<Option<RestoreSourceIdentity>> {
    let admin = PrivateDir::open_existing(&binding.admin)?;
    let journal = match admin.read(RESTORE_JOURNAL, MAX_RECORD) {
        Ok(bytes) => serde_json::from_slice::<Journal>(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if initialization.restore_origin.is_some() {
                return recovery("restored origin journal is missing; preserve state");
            }
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    ensure!(
        matches!(journal.version, 1 | 2)
            && (journal.version != 2 || journal.remote_state_digest.is_some())
            && initialization.restore_origin == Some(restore_origin(&journal.source))
            && matches!(journal.phase, Phase::Finalized)
            && journal.binding == *binding
            && journal.workspace == initialization.workspace
            && journal
                .snapshot_commit
                .is_some_and(|commit| commit.as_slice() == initialization.initial_commit)
            && journal.index_fingerprint.is_some()
            && journal.operation_heads > 0
            && journal.source.provider != [0; 32]
            && journal.source.generation > 0
            && journal.source.manifest_bytes > 0
            && journal.source.capture_bytes > 0,
        "restore ordinary-resume eligibility is incomplete or differs"
    );
    Ok(Some(journal.source))
}
impl Registry {
    /// Explicit ordinary CURRENT-state resume for a finalized restored origin.
    /// No original captured-head/WC equality or old cloud fetch is performed.
    /// Current provider identity is supplied by trusted host configuration; pins
    /// remain data and never turn into a new network authority.
    pub fn restored_resume_info(
        &self,
        root: &Path,
        provider_identity: [u8; 32],
    ) -> Result<RestoredResumeInfo> {
        normalized(root)?;
        let root = root.canonicalize()?;
        let _lock = self.lock()?;
        let mut found = None;
        for (count, entry) in fs::read_dir(self.records.path())?.enumerate() {
            ensure!(
                count < MAX_WORKSPACES,
                "native registry workspace limit exceeded"
            );
            let entry = entry?;
            let binding: Binding =
                serde_json::from_slice(&self.records.read(entry.file_name(), MAX_RECORD)?)?;
            self.validate(&binding)?;
            ensure!(
                entry.file_name() == std::ffi::OsStr::new(&record_name(&binding.workspace_id)),
                "native registry record identity mismatch"
            );
            if binding.root != root {
                continue;
            }
            // This selector returns saved eligibility DATA before a stopped
            // frontend is remounted. Activation still validates the real marker.

            self.validate_ready(&binding)?;
            let admin = PrivateDir::open_existing(&binding.admin)?;
            let initialization: Initialization =
                serde_json::from_slice(&admin.read(JOURNAL, MAX_RECORD)?)?;
            let source = ready_attachment(&binding, &initialization)?
                .context("workspace is not a restored origin")?;
            ensure!(
                source.provider == provider_identity,
                "restored origin provider identity differs"
            );
            ensure!(
                found.is_none(),
                "multiple Ready registrations reserve this root"
            );
            found = Some(RestoredResumeInfo { binding, source });
        }
        found.context("no finalized restored workspace registration for this root")
    }
}

impl Registry {
    /// Marker-free host recovery probe. Only the exact configured destination
    /// binding/provider may use monotone eligibility. This is local state DATA,
    /// not a mount proof or a retained-version network authority. Missing registry
    /// publication after Finalized can be completed after a genuine gated mount.
    pub fn restore_resume_eligibility(
        &self,
        binding: &Binding,
        provider_identity: [u8; 32],
    ) -> Result<Option<RestoredResumeInfo>> {
        self.validate(binding)?;
        let init = self
            .root
            .open_lock(format!("initialize-{}.lock", binding.id()))?;
        let registry = self.root.open_lock("registry.lock")?;
        for lock in [&init, &registry] {
            match lock.try_lock_exclusive() {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Err(RestoreBusy.into());
                }
                Err(error) => return Err(error.into()),
            }
        }
        let registered = match self
            .records
            .read(record_name(&binding.workspace_id), MAX_RECORD)
        {
            Ok(bytes) => {
                let recorded: Binding = serde_json::from_slice(&bytes)?;
                ensure!(recorded == *binding, "restore registry binding differs");
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };
        let admin = match PrivateDir::open_existing(&binding.admin) {
            Ok(admin) => admin,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if registered {
                    return recovery(
                        "registered restore administration is missing; preserve state",
                    );
                }
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let journal = match read_journal(&admin) {
            Ok(journal) => journal,
            Err(error) => {
                let _ = error;
                return recovery(
                    "restore recovery requires an exact private journal; preserve state",
                );
            }
        };
        ensure!(
            journal.binding == *binding && journal.source.provider == provider_identity,
            "restore eligibility destination or provider differs"
        );
        match journal.phase {
            Phase::Finalized => {
                self.validate_ready(binding)?;
                let initialization: Initialization =
                    serde_json::from_slice(&admin.read(JOURNAL, MAX_RECORD)?)?;
                let source = ready_attachment(binding, &initialization)?
                    .context("restore eligibility origin missing")?;
                Ok(Some(RestoredResumeInfo {
                    binding: binding.clone(),
                    source,
                }))
            }
            Phase::WorkingCopyStaged | Phase::PublishingReady if !registered => {
                if journal.index_fingerprint.is_none() || journal.snapshot_commit.is_none() {
                    return recovery(
                        "restore staged journal lacks exact readback binding; preserve state",
                    );
                }
                Ok(None)
            }
            _ => recovery("restore is incomplete or registered before eligibility; preserve state"),
        }
    }
}

/// Host stop barrier for accepted restore/stage/readback jobs. The caller MUST
/// first close that entry's producer admission, then drain its actual provider/
/// controller scopes separately. This never waits for retained Source slots:
/// a prepared source or sealed receipt may legitimately still own one.
pub async fn wait_for_restore_work_until(original: Instant) -> Result<()> {
    check_time(original)?;
    let permit = tokio::time::timeout_at(
        original.into(),
        runtime()?.admission.clone().acquire_owned(),
    )
    .await
    .map_err(|_| RestoreRecoveryRequired("restore physical work remains owned; preserve provider"))?
    .map_err(|_| RestoreRecoveryRequired("restore worker admission is unavailable"))?;
    drop(permit);
    Ok(())
}
