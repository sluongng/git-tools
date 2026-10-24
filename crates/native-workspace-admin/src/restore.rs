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
struct Source {
    inputs: RestoreInputs,
    manifest: c::ManifestV3,
    _capture: tinytree_client::CloudCapture,
    data: c::CaptureData,
    provider: RestoreProvider,
    limits: RestoreLimits,
    retained_bytes: usize,
    // Payloads and provider state are destroyed before their physical owner.
    _owner: QueryOwner,
    _source_slot: tokio::sync::OwnedSemaphorePermit,
    deadline: Instant,
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
struct SourceIdentity {
    workspace: [u8; 32],
    generation: u64,
    manifest: [u8; 32],
    manifest_blake3: [u8; 32],
    manifest_bytes: u64,
    capture: [u8; 32],
    capture_blake3: [u8; 32],
    capture_bytes: u64,
    provider: [u8; 32],
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u32,
    binding: Binding,
    source: SourceIdentity,
    operation_heads: u64,
    operation_heads_digest: [u8; 32],
    wc_operation: String,
    snapshot_operation: String,
    workspace: String,
    operation_indexes: [u8; 32],
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
}
/// Trusted host-only metadata writer for a preconstructed native Backend. It
/// must perform synchronous bounded local publication only, not nested RPC.
pub type BackendMetadataWriter = dyn Fn(&Path) -> Result<()> + Send + Sync;
pub struct RestoreHost {
    pub settings: UserSettings,
    pub signer: Signer,
    pub connector: Arc<dyn ProjectionConnector>,
    pub backend_config: jj_native_backend::Config,
    pub backend_metadata: Arc<BackendMetadataWriter>,
}
#[derive(Clone, Debug)]
pub struct StagedRestoreReceipt {
    pub binding: Binding,
    pub operation_heads: Vec<[u8; 64]>,
    pub wc_operation: [u8; 64],
    pub snapshot_operation: [u8; 64],
    pub snapshot_commit: [u8; 32],
    pub tree: jj_vfs::native::NativeTree,
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
async fn call<T, E>(
    deadline: Instant,
    future: impl Future<Output = std::result::Result<T, E>>,
) -> Result<T> {
    tokio::time::timeout_at(deadline.into(), future)
        .await
        .context("restore source deadline exceeded")?
        .map_err(|_| anyhow::anyhow!("restore source rejected"))
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
fn identity(inputs: &RestoreInputs, provider: &RestoreProvider) -> SourceIdentity {
    let version = inputs.version.version();
    let selector = version.selector();
    let raw = version.manifest_selected();
    let capture = inputs.version.descriptor();
    SourceIdentity {
        workspace: selector.workspace_id,
        generation: selector.generation,
        manifest: selector.manifest_id.0,
        manifest_blake3: raw.blake3,
        manifest_bytes: raw.size_bytes,
        capture: capture.id.0,
        capture_blake3: capture.selected.blake3,
        capture_bytes: capture.selected.size_bytes,
        provider: provider.config.provider_fingerprint,
    }
}
fn import_version(source: &Source) -> PortableImportVersion {
    let source = identity(&source.inputs, &source.provider);
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
    let deadline = deadline(&limits)?;
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
                client
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
            check_time(deadline)?;
            Ok(PreparedRestore {
                source: Arc::new(Source {
                    inputs,
                    manifest,
                    _capture: capture,
                    data,
                    provider,
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
            provider.query.open_cloud_scope(
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
                    provider.query.facts(scope.handle, facts, physical.clone()),
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
                        provider
                            .query
                            .read_commit(scope.handle, commit, physical.clone()),
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
        let closed = call(deadline, provider.query.close_scope(scope.handle, physical)).await;
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
            binding.repository == source.source.provider.config.store_alias,
            "restore destination alias mismatch"
        );
        let endpoint = match &source.source.provider.config.endpoint {
            jj_native_index::Endpoint::UnixSocket(path) => LocalEndpoint::UnixSocket(path.clone()),
            jj_native_index::Endpoint::WindowsNamedPipe(name) => {
                LocalEndpoint::WindowsNamedPipe(name.clone())
            }
        };
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
        let mut index_hash = blake3::Hasher::new();
        index_hash.update(b"native-restore-operation-indexes-v1\0");
        for entry in &source.source.manifest.history.operation_indexes {
            index_hash.update(&entry.operation);
            index_hash.update(&entry.root.0);
        }
        let journal = Journal {
            version: 1,
            binding: binding.clone(),
            source: identity(&source.source.inputs, &source.source.provider),
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
            workspace: data.working_copy.workspace.clone(),
            operation_indexes: *index_hash.finalize().as_bytes(),
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
        })
    }
}
impl RestoreIntent {
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
    let deadline = prepared.source.deadline;
    check_time(deadline)?;
    owned_job(deadline, move || {
        futures::executor::block_on(stage(intent, prepared, host, deadline))
    })
    .await
}

async fn stage(
    mut intent: RestoreIntent,
    prepared: PreparedRestore,
    host: RestoreHost,
    deadline: Instant,
) -> Result<StagedRestoreReceipt> {
    let source = prepared.source;
    ensure!(
        intent.journal.source == identity(&source.inputs, &source.provider),
        "restore intent source changed"
    );
    ensure!(
        matches!(intent.journal.phase, Phase::Reserved),
        "restore journal already contains staged state"
    );
    intent.registry.validate(&intent.journal.binding)?;
    validate_marker(&intent.journal.binding)?;
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
    let projected = host.connector.connect(&binding.projection())?.inspect()?;
    let expected_tree = jj_vfs::native::NativeTree {
        terms: source.data.working_copy.tree.terms.clone(),
        labels: source.data.working_copy.tree.labels.clone(),
    };
    ensure!(
        projected.repository == binding.repository && projected.tree == expected_tree,
        "restore lazy projection mismatch"
    );
    // All asynchronous backend preparation precedes jj's synchronous factory.
    let objects: Arc<dyn NativeObjectIo> = Arc::new(RestoreObjectIo {
        inner: source.provider.objects.clone(),
        owner: source.clone(),
    });
    let backend = NativeBackend::from_io(objects.clone(), host.backend_config).await?;
    let graph = c::verify_index_artifact_graph(
        &source.manifest.history.index_roots,
        &source.inputs.artifacts,
        source.limits.manifest.history.artifacts,
    )?;
    // Reauthorize again immediately before stage mutation: a retained version may
    // have expired since preparation. No reference/root fallback is permitted.
    authorize(
        &source.inputs,
        &source.manifest,
        &source.data,
        &graph,
        &source.provider,
        source._owner.clone(),
        deadline,
    )
    .await?;
    check_time(deadline)?;
    let staging = PrivateDir::create(intent.admin.path().join("restore-staging"))?;
    sync_directory(intent.admin.path())?;
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
    for object in &source.inputs.objects {
        if matches!(object.id(), c::ObjectId::View(_)) {
            check_time(deadline)?;
            object.import(&op_store).await?;
        }
    }
    let mut installed = BTreeSet::from([[0; 64]]);
    let mut pending: BTreeMap<_, _> = source
        .inputs
        .objects
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
    sync_directory(&op_path)?;
    sync_directory(staging.path())?;
    intent.advance(Phase::ObjectsStaged)?;
    let index_path = staging.path().join("index");
    let imported = NativeIndexStore::import_fresh_operation_indexes(
        &index_path,
        source.provider.config.clone(),
        source.provider.index_limits.clone(),
        source.provider.query.clone(),
        objects.clone(),
        import_version(&source),
        source.inputs.version.version().manifest(),
        &graph,
        source.limits.import.clone(),
        source._owner.clone(),
    )
    .await?;
    let cleanup = imported.store.cleanup_handle();
    drop(imported);
    cleanup.wait(Duration::from_millis(
        source.provider.index_limits.cleanup_timeout_ms,
    ))?;
    drop(graph);
    intent.advance(Phase::IndexStaged)?;
    check_time(deadline)?;
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
            install_directory(&index_path, path)?;
            Ok(Box::new(NativeIndexStore::open(
                path,
                &source.provider.config,
                source.provider.index_limits.clone(),
                source.provider.query.clone(),
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
    let op_store = repo.op_store();
    for object in &source.inputs.objects {
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
    for association in &source.manifest.history.operation_indexes {
        check_time(deadline)?;
        let id = OperationId::from_bytes(&association.operation);
        let data = op_store.read_operation(&id).await?;
        let operation = jj_lib::operation::Operation::new(op_store.clone(), id, data);
        let index = repo
            .loader()
            .index_store()
            .get_index_at_op(&operation, repo.store())
            .await?;
        drop(index);
    }
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
    })
}
