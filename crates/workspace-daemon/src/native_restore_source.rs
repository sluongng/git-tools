//! Exact retained-source collection for host-owned lazy restore. This performs
//! no destination writes, clone admission, native FILE reads or local-pack reads.
use crate::native_capture_upload::Source;
use anyhow::{Context, Result, ensure};
use jj_native_backend::NativeObjectIo;
use native_broker::QueryOwner;
use native_workspace_admin::restore::{
    PreparedRestore, RestoreInputs, RestoreLimits, RestoreProvider,
};
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use tinytree_client::{
    CloudDescriptor, CloudIndexArtifactDescriptor, CloudIndexArtifactId, CloudObjectId,
    CloudRawDigest, RetainedVersionSelector,
};
use tinytree_cloud as c;
use tokio::{
    runtime::{Builder, Runtime},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
const MAX_BYTES: usize = 2 * 1024 * 1024 * 1024;
const UNIT: usize = 1024;
#[derive(Clone, Copy, Debug)]
pub(crate) struct Selection {
    pub selector: RetainedVersionSelector,
    pub manifest: CloudRawDigest,
}
#[derive(Clone, Debug)]
pub(crate) struct Limits {
    pub max_working_bytes: usize,
    pub max_selected_bytes: u64,
    pub max_temporary_bytes: u64,
    pub max_objects: usize,
    pub max_artifacts: usize,
    pub max_requests: usize,
    pub restore: RestoreLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_working_bytes: MAX_BYTES,
            max_selected_bytes: 64 * 1024 * 1024,
            max_temporary_bytes: 512 * 1024 * 1024,
            max_objects: 4096,
            max_artifacts: 8192,
            max_requests: 12_300,
            restore: RestoreLimits::default(),
        }
    }
}
struct Executor {
    runtime: Runtime,
    accepted: Arc<Semaphore>,
    worker: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    temporary: Arc<Semaphore>,
}
fn executor() -> Result<&'static Executor> {
    static INSTANCE: OnceLock<std::result::Result<Executor, String>> = OnceLock::new();
    INSTANCE
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(4)
                .thread_name("native-restore-source")
                .enable_all()
                .build()
                .map(|runtime| Executor {
                    runtime,
                    accepted: Arc::new(Semaphore::new(1)),
                    worker: Arc::new(Semaphore::new(1)),
                    bytes: Arc::new(Semaphore::new(MAX_BYTES / UNIT)),
                    temporary: Arc::new(Semaphore::new(512 * 1024 * 1024 / UNIT)),
                })
                .map_err(|_| "native restore source worker unavailable".into())
        })
        .as_ref()
        .map_err(|e| anyhow::anyhow!(e.clone()))
}
struct MemoryOwner {
    _source: Arc<Source>,
    _external: QueryOwner,
    _bytes: OwnedSemaphorePermit,
    _temporary: OwnedSemaphorePermit,
}
struct Acceptance {
    _permit: OwnedSemaphorePermit,
}
struct WorkOwner {
    _memory: Arc<MemoryOwner>,
    _worker: OwnedSemaphorePermit,
    // Last physical tail releases acceptance only after its source/memory and
    // worker owners have finished teardown.
    _accepted: Arc<Acceptance>,
}
struct Owned<T> {
    value: T,
    _owner: QueryOwner,
}
async fn blocking<T: Send + 'static>(
    owner: QueryOwner,
    job: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<Owned<T>> {
    let done = tokio::task::spawn_blocking(move || Owned {
        value: job(),
        _owner: owner,
    })
    .await?;
    Ok(Owned {
        value: done.value?,
        _owner: done._owner,
    })
}
struct Collected {
    inputs: RestoreInputs,
    _memory: Arc<MemoryOwner>,
}
/// `provider.query` must be the host's configured raw query adapter, never a
/// caller-supplied network endpoint or an already-closed command-token facade.
/// Retained bytes, not the temporary worker slot, move into PreparedRestore.
pub(crate) async fn collect(
    source: Arc<Source>,
    mut provider: RestoreProvider,
    selection: Selection,
    limits: Limits,
    owner: QueryOwner,
    original: Instant,
) -> Result<PreparedRestore> {
    validate(&source, &provider, &selection, &limits)?;
    let configured = Instant::now()
        .checked_add(Duration::from_millis(limits.restore.timeout_ms))
        .context("restore deadline overflow")?;
    let deadline = original.min(configured);
    let executor = executor()?;
    // Cancellation while queued remains caller-owned: no detached task or
    // source/context is accepted before this bounded slot is acquired.
    let accepted = Arc::new(Acceptance {
        _permit: tokio::time::timeout_at(deadline, executor.accepted.clone().acquire_owned())
            .await??,
    });
    let job = executor.runtime.spawn(async move {
        let result_owner = accepted.clone();
        let value = async move {
            let units = limits
                .max_working_bytes
                .checked_add(UNIT - 1)
                .context("restore byte admission overflow")?
                / UNIT;
            let bytes = tokio::time::timeout_at(
                deadline,
                executor
                    .bytes
                    .clone()
                    .acquire_many_owned(u32::try_from(units)?),
            )
            .await??;
            let temporary = tokio::time::timeout_at(
                deadline,
                executor.temporary.clone().acquire_many_owned(u32::try_from(
                    limits.max_temporary_bytes.div_ceil(UNIT as u64),
                )?),
            )
            .await??;
            let memory = Arc::new(MemoryOwner {
                _source: source.clone(),
                _external: owner,
                _bytes: bytes,
                _temporary: temporary,
            });
            let worker = tokio::time::timeout_at(deadline, executor.worker.clone().acquire_owned())
                .await??;
            let physical = Arc::new(WorkOwner {
                _accepted: accepted,
                _memory: memory.clone(),
                _worker: worker,
            });
            // RPC guards pin the temporary worker only through actual request work;
            // SDK retained buffers intentionally do not inherit this transient guard.
            let client = source
                .control
                .client_for_owned_work(physical.clone(), deadline)
                .await?;
            let version = client
                .cloud_retained_version_selected(selection.selector, selection.manifest)
                .await?;
            let captured = client.cloud_captured_version(version)?;
            ensure!(
                selection
                    .manifest
                    .size_bytes
                    .checked_add(captured.descriptor().selected.size_bytes)
                    .is_some_and(|n| n <= limits.max_selected_bytes),
                "restore selected capsule exceeds byte limit"
            );
            let payload = client.cloud_read_capture(&captured).await?;
            let raw = payload.payload().to_vec();
            let verified = physical.clone() as QueryOwner;
            let decode_limits = limits.restore.manifest;
            let cap_descriptor = captured.descriptor();
            let manifest_bytes = captured.version().manifest().to_vec();
            let decoded = blocking(verified.clone(), move || {
                let manifest = c::decode_manifest_v3(
                    &manifest_bytes,
                    c::ManifestId(selection.selector.manifest_id.0),
                    decode_limits,
                )?;
                ensure!(
                    c::encode_manifest_v3(&manifest, decode_limits)? == manifest_bytes,
                    "noncanonical selected restore manifest"
                );
                let data = c::decode_data(&raw, cap_descriptor.id.0, &decode_limits.capture)?;
                Ok((manifest, data, raw))
            })
            .await?;
            drop(payload);
            let Owned {
                value: (manifest, data, raw_capture),
                _owner: decoded_owner,
            } = decoded;
            drop(decoded_owner);
            ensure!(
                manifest.history.recovery.members.len() <= limits.max_objects
                    && data.blobs.len()
                        <= limits
                            .max_objects
                            .checked_add(limits.max_artifacts)
                            .context("restore count overflow")?,
                "restore source inventory count limit"
            );
            let mut requests = 2usize;
            let mut selected = selection
                .manifest
                .size_bytes
                .checked_add(cap_descriptor.selected.size_bytes)
                .context("restore selected byte overflow")?;
            ensure!(
                selected <= limits.max_selected_bytes,
                "restore initial selected bytes exceed limit"
            );
            let mut retained = 0usize;
            let mut objects = Vec::new();
            let mut artifacts = Vec::new();
            let scratch = scratch(&limits)?;
            for member in &manifest.history.recovery.members {
                charge_request(
                    &mut requests,
                    &mut selected,
                    member.selected.size_bytes,
                    &limits,
                )?;
                let id = match member.id {
                    c::ObjectId::Operation(id) => CloudObjectId::Operation(id),
                    c::ObjectId::View(id) => CloudObjectId::View(id),
                };
                let payload = client
                    .cloud_read_object(CloudDescriptor {
                        id,
                        selected: CloudRawDigest {
                            blake3: member.selected.blake3,
                            size_bytes: member.selected.size_bytes,
                        },
                    })
                    .await?;
                let raw = payload.payload().to_vec();
                drop(payload);
                let id = member.id;
                let selected = member.selected;
                let object = blocking(verified.clone(), move || {
                    Ok(c::VerifiedObject::verify(
                        id,
                        selected,
                        &raw,
                        c::Limits::default(),
                    )?)
                })
                .await?;
                retained = retained
                    .checked_add(object.value.retained_size_upper_bound()?)
                    .context("restore object memory overflow")?;
                ensure!(
                    retained
                        .checked_add(scratch)
                        .is_some_and(|n| n <= limits.max_working_bytes),
                    "restore retained object memory limit"
                );
                objects.push(object.value);
            }
            for blob in data.blobs.iter().filter(|blob| matches!(blob.kind, 3 | 4)) {
                ensure!(
                    artifacts.len() < limits.max_artifacts,
                    "restore artifact count limit"
                );
                charge_request(&mut requests, &mut selected, blob.size, &limits)?;
                let raw = decode_hex::<32>(&blob.id)?;
                let (remote, id) = if blob.kind == 3 {
                    (
                        CloudIndexArtifactId::Root(raw),
                        c::IndexArtifactId::Root(c::IndexRootId(raw)),
                    )
                } else {
                    (
                        CloudIndexArtifactId::Node(raw),
                        c::IndexArtifactId::Node(c::IndexNodeId(raw)),
                    )
                };
                let payload = client
                    .cloud_read_index_artifact(CloudIndexArtifactDescriptor {
                        id: remote,
                        selected: CloudRawDigest {
                            blake3: blob.blake3,
                            size_bytes: blob.size,
                        },
                    })
                    .await?;
                let raw = payload.payload().to_vec();
                drop(payload);
                let selected = c::BlobDigest {
                    blake3: blob.blake3,
                    size_bytes: blob.size,
                };
                let policy = limits.restore.manifest.history.artifacts;
                let artifact = blocking(verified.clone(), move || {
                    Ok(c::decode_index_artifact(
                        &raw,
                        c::IndexArtifactDescriptor { id, selected },
                        policy,
                    )?)
                })
                .await?;
                retained = retained
                    .checked_add(artifact.value.retained_size_upper_bound()?)
                    .context("restore artifact memory overflow")?;
                ensure!(
                    retained
                        .checked_add(scratch)
                        .is_some_and(|n| n <= limits.max_working_bytes),
                    "restore retained artifact memory limit"
                );
                artifacts.push(artifact.value);
            }
            // Independently verify exact OPVIEW/index/capsule binding. This detects
            // extras, missing descendants, changed raw selections and false labels.
            let raw = raw_capture;
            let policy = limits.restore.manifest;
            let checked = blocking(verified.clone(), move || {
                let graph = c::verify_index_artifact_graph(
                    &manifest.history.index_roots,
                    &artifacts,
                    policy.history.artifacts,
                )?;
                c::verify_capture_export_binding(
                    &manifest, &raw, &objects, &artifacts, &graph, policy,
                )?;
                Ok((objects, artifacts))
            })
            .await?;
            drop(data);
            let Owned {
                value: (objects, artifacts),
                _owner: checked_owner,
            } = checked;
            drop(checked_owner);
            let collected = Collected {
                inputs: RestoreInputs {
                    version: captured,
                    objects,
                    artifacts,
                },
                _memory: memory.clone(),
            };
            drop(client);
            drop(verified);
            drop(physical);
            // No persistent PreparedRestore can hold this worker permit. Await only
            // this collector's physical guards, not other daemon Remote commands.
            let barrier =
                tokio::time::timeout_at(deadline, executor.worker.clone().acquire_owned())
                    .await??;
            drop(barrier);
            let client = source
                .control
                .client_for_owned_work(memory.clone(), deadline)
                .await?;
            let admitted = source
                .control
                .admit_retained_version_until(
                    selection.selector,
                    selection.manifest,
                    memory.clone(),
                    deadline,
                )
                .await?;
            ensure!(
                admitted.selector() == selection.selector
                    && admitted.manifest_selected() == selection.manifest,
                "restore retained native authority differs"
            );
            let (objects, query) = source.restore.bind(
                deadline,
                Duration::from_millis(provider.index_limits.cleanup_timeout_ms)
                    .min(Duration::from_secs(5)),
                memory.clone(),
            )?;
            provider.objects = objects;
            provider.query = query;
            native_workspace_admin::restore::prepare_restore_until(
                client,
                collected.inputs,
                provider,
                limits.restore,
                memory,
                deadline.into_std(),
            )
            .await
        }
        .await;
        // A cancelled receiver drops the heavy result before its accepted slot.
        Owned {
            value,
            _owner: result_owner,
        }
    });
    let completed = tokio::time::timeout_at(deadline, job)
        .await
        .context("native restore source deadline exceeded")?
        .context("native restore source worker failed")?;
    completed.value
}
fn validate(
    source: &Arc<Source>,
    provider: &RestoreProvider,
    selection: &Selection,
    limits: &Limits,
) -> Result<()> {
    ensure!(
        cfg!(target_os = "linux"),
        "native restore source is unsupported on this platform"
    );
    ensure!(
        (1..=86_400_000).contains(&limits.restore.timeout_ms),
        "invalid restore source timeout"
    );
    limits.restore.manifest.validate()?;
    let actual: Arc<dyn NativeObjectIo> = source.io.clone();
    ensure!(
        provider.config.provider_fingerprint == source.identity
            && Arc::ptr_eq(&provider.objects, &actual)
            && source.restore.matches(&provider.objects, &provider.query),
        "restore source differs from configured provider"
    );
    ensure!(
        limits.max_working_bytes > 0
            && limits.max_working_bytes <= MAX_BYTES
            && limits.max_selected_bytes > 0
            && limits.max_selected_bytes <= 1024 * 1024 * 1024
            && (1..=100_000).contains(&limits.max_objects)
            && (1..=100_000).contains(&limits.max_artifacts)
            && (3..=200_010).contains(&limits.max_requests),
        "invalid restore fetch limits"
    );
    ensure!(
        selection.manifest.size_bytes > 0
            && selection.manifest.size_bytes <= limits.max_selected_bytes
            && selection.manifest.size_bytes
                <= limits.restore.manifest.history.recovery.max_manifest_bytes as u64,
        "selected restore manifest exceeds limit"
    );
    let object = provider.objects.metadata_work_budget().unwrap_or_default();
    let query = provider.query.resources();
    // These adapters admit physical jobs independently. A completed query's
    // transport/decoder tail can overlap object IO, so sequential awaiters do
    // not justify reserving only the larger declaration.
    let provider_work = object
        .memory_bytes
        .checked_add(query.memory_bytes)
        .context("restore provider work bound overflow")?;
    let provider_temporary = object
        .temporary_bytes
        .checked_add(query.temporary_bytes)
        .context("restore provider temporary bound overflow")?;
    ensure!(
        limits
            .restore
            .max_working_bytes
            .checked_add(usize::try_from(provider_work)?)
            .and_then(|n| n.checked_add(64 * 1024 * 1024))
            .is_some_and(|n| n <= limits.max_working_bytes),
        "restore provider work exceeds host admission"
    );
    ensure!(
        limits.max_temporary_bytes <= 512 * 1024 * 1024
            && provider_temporary <= limits.max_temporary_bytes,
        "restore provider temporary work exceeds admission"
    );
    ensure!(
        scratch(limits)? < limits.max_working_bytes
            && limits.restore.max_working_bytes <= limits.max_working_bytes,
        "restore working admission too small"
    );
    Ok(())
}
fn scratch(limits: &Limits) -> Result<usize> {
    let l = limits.restore.manifest;
    [
        l.history.recovery.max_decode_bytes,
        l.history.artifacts.max_decode_bytes,
        l.capture.max_decode_bytes,
        l.binding.max_bytes,
        l.binding.max_bytes,
        64 * 1024 * 1024,
    ]
    .into_iter()
    .try_fold(0usize, |sum, n| {
        sum.checked_add(n).context("restore scratch bound overflow")
    })
}
fn charge_request(requests: &mut usize, bytes: &mut u64, n: u64, limits: &Limits) -> Result<()> {
    *requests = requests
        .checked_add(1)
        .filter(|n| *n <= limits.max_requests)
        .context("restore request limit")?;
    *bytes = bytes
        .checked_add(n)
        .filter(|n| *n <= limits.max_selected_bytes)
        .context("restore selected byte limit")?;
    Ok(())
}
fn decode_hex<const N: usize>(text: &str) -> Result<[u8; N]> {
    ensure!(text.len() == N * 2, "restore ID width differs");
    let mut out = [0; N];
    for (slot, pair) in out.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        *slot = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use local_cas::{Artifact, BlobSource, CacheConfig, LocalCas};
    use std::{
        path::Path,
        sync::atomic::{AtomicUsize, Ordering},
    };
    struct NoFiles;
    #[async_trait::async_trait]
    impl BlobSource for NoFiles {
        async fn fetch(&self, _: &Artifact, _: &mut std::fs::File) -> Result<()> {
            anyhow::bail!("source collection must not fetch native FILE bytes")
        }
        async fn missing(&self, values: &[Artifact]) -> Result<Vec<Artifact>> {
            Ok(values.to_vec())
        }
    }
    fn private(parent: &Path, name: &str) -> Result<private_fs::PrivateDir> {
        Ok(private_fs::PrivateDir::create(parent.join(name))?)
    }
    fn text(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fresh_configured_client_collects_exact_retained_graph_without_local_pack_or_native_reads()
    -> Result<()> {
        let root = tempfile::Builder::new().tempdir_in(std::env::temp_dir().canonicalize()?)?;
        let server_root = private(root.path(), "server")?;
        // Match the supported bounded cloud fixture profile instead of pairing
        // an unbounded/default Store workset with a smaller service pool.
        let profile: tinytree_server::Config = toml::from_str(
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
        // Supported safe-local mode, not a TLS/authentication acceptance claim.
        // Native API calls are counted independently from cloud/query service.
        let native_calls = Arc::new(AtomicUsize::new(0));
        let counted = native_calls.clone();
        let native = tonic::service::interceptor::InterceptedService::new(
            service.grpc_service(),
            move |request: tonic::Request<()>| {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(request)
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(service.deadline_layer())
                .add_service(native)
                .add_service(service.query_grpc_service())
                .add_service(service.cloud_grpc_service())
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
        let local_root = private(root.path(), "local")?;
        let direct = Arc::new(jj_native_backend::DirectObjectIo::init(
            local_root.path(),
            Default::default(),
        )?);
        let cache = LocalCas::open(
            root.path().join("cas"),
            Arc::new(NoFiles),
            CacheConfig::default(),
        )?;
        let mut remote_config = jj_native_remote::Config::default();
        remote_config.remote.endpoint = format!("http://{address}");
        remote_config.remote.repository_id = "example".into();
        remote_config.remote.rpc.retry.initial_backoff_ms = 1;
        remote_config.remote.rpc.retry.max_backoff_ms = 5;
        let publisher =
            tinytree_client::NativeClient::connect(remote_config.remote.clone()).await?;
        let ledger = private(root.path(), "remote")?;
        let remote = jj_native_remote::RemoteRepository::open(
            direct.clone(),
            cache.clone(),
            &ledger.path().join("ledger"),
            &ledger.path().join("staging"),
            remote_config.clone(),
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
        let provider = RestoreProvider {
            config: jj_native_index::Config {
                endpoint: jj_native_index::Endpoint::UnixSocket(root.path().join("rbe.sock")),
                store_alias: "example".into(),
                provider_fingerprint: source.identity,
            },
            index_limits: Default::default(),
            query: remote.query.clone(),
            objects: source.io.clone(),
        };
        let model = c::PortableIndexSnapshot {
            roots: vec![tinytree_model::CommitId::ROOT],
            drafts: vec![],
        };
        let built = c::build_index_artifacts(&model, Default::default())?;
        let mut artifacts = Vec::new();
        let mut blobs = Vec::new();
        for encoded in &built.artifacts {
            let descriptor = encoded.descriptor;
            let (kind, id, remote) = match descriptor.id {
                c::IndexArtifactId::Root(id) => (3, id.0, CloudIndexArtifactId::Root(id.0)),
                c::IndexArtifactId::Node(id) => (4, id.0, CloudIndexArtifactId::Node(id.0)),
            };
            publisher
                .cloud_put_index_artifact(remote, &encoded.bytes)
                .await?;
            artifacts.push(c::decode_index_artifact(
                &encoded.bytes,
                descriptor,
                Default::default(),
            )?);
            blobs.push(c::BlobRecord {
                kind,
                id: text(&id),
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
            operation_heads: vec![text(&[0; 64])],
            working_copy: c::PortableWorkingCopy {
                workspace: "default".into(),
                wc_operation: text(&[0; 64]),
                snapshot_operation: text(&[0; 64]),
                snapshot_commit: [0; 32],
                tree: c::CaptureTree {
                    terms: tree.terms.iter().map(|id| id.0).collect(),
                    labels: vec![],
                },
            },
            operations: vec![c::OperationRecord {
                operation: text(&[0; 64]),
                view: text(&[0; 64]),
                parents: vec![],
            }],
            indexes: vec![c::IndexRecord {
                operation: text(&[0; 64]),
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
        let capsule = c::encode_capture_data(&data, Default::default())?;
        let graph = c::verify_index_artifact_graph(
            std::slice::from_ref(&built.root),
            &artifacts,
            Default::default(),
        )?;
        let prepared = c::prepare_capture_manifest(
            &c::CaptureExportIdentity {
                workspace: [7; 32],
                exporter: [8; 16],
                sequence: 1,
                parents: vec![],
            },
            &capsule,
            &[],
            &artifacts,
            &graph,
            Default::default(),
        )?;
        let publication = publisher
            .cloud_publish_captured_version(&tinytree_client::CloudCapturedPublishIntent::new(
                tinytree_client::CloudPublishIntent {
                    workspace_id: [7; 32],
                    expected_generation: 0,
                    idempotency_key: "restore-source-intrinsic-fixture".into(),
                    manifest: prepared.bytes().to_vec(),
                    staging_leases: vec![],
                },
                capsule.clone(),
            )?)
            .await?;
        let selection = Selection {
            selector: RetainedVersionSelector {
                workspace_id: [7; 32],
                generation: publication.workspace.generation,
                manifest_id: tinytree_client::CloudManifestId(prepared.id().0),
            },
            manifest: CloudRawDigest::of(prepared.bytes()),
        };
        // Valid cancelled callers must not become detached owned tasks queued
        // behind byte admission. Holding both actual pools distinguishes this
        // from a fast successful call whose owner happened to drop promptly.
        struct Dropped(Arc<AtomicUsize>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let executor = executor()?;
        let accepted = executor.accepted.clone().acquire_owned().await?;
        let bytes = executor
            .bytes
            .clone()
            .acquire_many_owned((MAX_BYTES / UNIT) as u32)
            .await?;
        let dropped = Arc::new(AtomicUsize::new(0));
        for expected in 1..=3 {
            let source = source.clone();
            let provider = provider.clone();
            let owner = Arc::new(Dropped(dropped.clone()));
            let pending = tokio::spawn(async move {
                collect(
                    source,
                    provider,
                    selection,
                    Limits::default(),
                    owner,
                    Instant::now() + Duration::from_secs(20),
                )
                .await
            });
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            pending.abort();
            match pending.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(_) => panic!("queued caller unexpectedly completed"),
            }
            assert_eq!(
                dropped.load(Ordering::SeqCst),
                expected,
                "pre-accept cancellation must release caller context, not queue an owned task"
            );
        }
        drop(bytes);
        drop(accepted);
        let restored = collect(
            source.clone(),
            provider.clone(),
            selection,
            Limits::default(),
            Arc::new(()),
            Instant::now() + Duration::from_secs(20),
        )
        .await?;
        assert_eq!(restored.source_identity().manifest_id(), prepared.id().0);
        assert_eq!(restored.projection_seed(), &data.working_copy);
        assert_eq!(
            native_calls.load(Ordering::SeqCst),
            0,
            "no generic native metadata, FILE, hold or clone admission RPC"
        );
        assert!(
            !root.path().join("destination").exists(),
            "source preparation does not create a workspace or Ready record"
        );
        drop(restored);
        let other_local = private(root.path(), "other-local")?;
        let other_ledger = private(root.path(), "other-ledger")?;
        let other = jj_native_remote::RemoteRepository::open(
            Arc::new(jj_native_backend::DirectObjectIo::init(
                other_local.path(),
                Default::default(),
            )?),
            cache.clone(),
            &other_ledger.path().join("ledger"),
            &other_ledger.path().join("staging"),
            remote_config,
        )?;
        let mut mismatched = provider.clone();
        mismatched.query = other.query;
        assert!(
            collect(
                source.clone(),
                mismatched,
                selection,
                Limits::default(),
                Arc::new(()),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .is_err(),
            "resource preflight must reject an unrelated configured query adapter"
        );
        let mut tampered = selection;
        tampered.manifest.blake3[0] ^= 1;
        assert!(
            collect(
                source.clone(),
                provider.clone(),
                tampered,
                Limits::default(),
                Arc::new(()),
                Instant::now() + Duration::from_secs(10)
            )
            .await
            .is_err()
        );
        assert!(
            collect(
                source.clone(),
                provider.clone(),
                selection,
                Limits {
                    max_artifacts: 1,
                    ..Default::default()
                },
                Arc::new(()),
                Instant::now() + Duration::from_secs(10)
            )
            .await
            .is_err(),
            "actual root+node inventory cannot be truncated"
        );
        let mut wrong = provider.clone();
        wrong.config.provider_fingerprint = [3; 32];
        assert!(
            collect(
                source,
                wrong,
                selection,
                Limits::default(),
                Arc::new(()),
                Instant::now() + Duration::from_secs(10)
            )
            .await
            .is_err()
        );
        assert_eq!(native_calls.load(Ordering::SeqCst), 0);
        assert!(!root.path().join("destination").exists());
        remote.wait_for_idle().await;
        let _ = stop.send(());
        server.await??;
        Ok(())
    }
}
