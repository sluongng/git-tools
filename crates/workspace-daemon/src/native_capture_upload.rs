//! Optional saved-capture publication. A historical Published record never
//! grants eviction: tickets, roots and private packs remain untouched.
use crate::{
    config::{Config, NativeCaptureUpload, NativeCaptureUploadPolicy},
    native_commands::NativeCommandHost,
};
use anyhow::{Context, Result, ensure};
use native_capture_pack::{Limits as PackLimits, VerifiedPack};
use native_retention::{CaptureReadGuard, PendingCapture};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tinytree_client::{
    CloudCapturedPublishIntent, CloudIndexArtifactId, CloudManifestId, CloudObjectId,
    CloudPublishIntent, CloudRawDigest, RetainedVersionSelector,
};
use tinytree_cloud as c;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
type Owner = Arc<dyn Any + Send + Sync>;
pub(crate) struct Source {
    pub io: Arc<jj_native_remote::RemoteObjectIo>,
    pub control: jj_native_remote::RemoteRepositoryControl,
    pub identity: [u8; 32],
}
#[derive(Default)]
pub(crate) struct Stats {
    attempts: AtomicU64,
    published: AtomicU64,
    failed: AtomicU64,
    last: Mutex<&'static str>,
}
impl Stats {
    pub fn status(&self) -> serde_json::Value {
        serde_json::json!({"policy":"publish_retaining_roots","attempted":self.attempts.load(Ordering::Relaxed),"historical_published_count":self.published.load(Ordering::Relaxed),"failed":self.failed.load(Ordering::Relaxed),"last_disposition":self.last.lock().map(|v|*v).unwrap_or("unavailable"),"local_roots_retained":true,"current_backup_availability_verified":false})
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
}
struct Intent {
    header: Header,
    manifest: Vec<u8>,
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
            header.version == 1
                && header.key == format!("capture_{}", hex(&header.ticket))
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
            header,
            manifest,
            capture,
        };
        value.sdk()?;
        Ok(value)
    }
}
#[derive(Default)]
struct TestHooks {
    #[cfg(test)]
    fail_namespace: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    catalog_pause: Mutex<Option<Arc<CatalogPause>>>,
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
    if policy.policy == NativeCaptureUploadPolicy::Disabled {
        return Ok(None);
    }
    let stats = Arc::new(Stats::default());
    stats.set("idle");
    host.set_upload_status(stats.clone())?;
    let directory = match PrivateDir::create(config.state_dir.join("native-commands/upload"))
        .and_then(|directory| {
            File::open(config.state_dir.join("native-commands"))?.sync_all()?;
            Ok(directory)
        }) {
        Ok(directory) => Arc::new(directory),
        Err(_) => {
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
        _hooks: hooks,
    });
    #[cfg(test)]
    let observer = worker.clone();
    let (stop, mut rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            if *rx.borrow() {
                break;
            }
            if worker.poll().await.is_err() {
                worker.stats.failed.fetch_add(1, Ordering::Relaxed);
                worker.stats.set("inventory_unavailable");
            }
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
                    || name.starts_with("upload-") && name.ends_with(".part")),
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
            if name.ends_with(".published") {
                let header: Header = serde_json::from_slice(&self.directory.read(name, 4096)?)?;
                ensure!(
                    header.version == 1 && name == format!("{}.published", hex(&header.ticket)),
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
    fn sync_namespace(&self) -> Result<()> {
        #[cfg(test)]
        if self._hooks.fail_namespace.load(Ordering::Acquire) {
            anyhow::bail!("injected namespace barrier failure");
        }
        File::open(self.directory.path())?.sync_all()?;
        Ok(())
    }
    fn read_durable(&self, name: &str) -> Result<Vec<u8>> {
        let mut file = self
            .directory
            .open_existing_file(name, self.policy.max_job_bytes as u64)?;
        let mut bytes = Vec::new();
        (&mut file)
            .take(self.policy.max_job_bytes as u64 + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() <= self.policy.max_job_bytes,
            "capture durable record read exceeded"
        );
        // A visible link is not evidence its namespace barrier succeeded in a
        // previous attempt. Reestablish BOTH barriers before trusting it.
        file.sync_all()?;
        self.sync_namespace()?;
        Ok(bytes)
    }
    fn persist(&self, name: &str, bytes: &[u8]) -> Result<()> {
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
    async fn poll(self: &Arc<Self>) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(self.policy.timeout_ms);
        let admission = Arc::new(Admission {
            _permit: tokio::time::timeout_at(deadline, self.admission.clone().acquire_owned())
                .await??,
        });
        let worker = self.clone();
        let physical = admission.clone();
        let inventory = tokio::time::timeout_at(
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
        .await???;
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
        for ticket in tickets {
            let published = format!("{}.published", hex(&ticket.id.0));
            if self.directory.path().join(&published).try_exists()? {
                let header: Header =
                    serde_json::from_slice(&self.directory.read(&published, 4096)?)?;
                ensure!(
                    header.version == 1
                        && header.ticket == ticket.id.0
                        && header.ticket_binding == ticket_binding(&ticket),
                    "historical capture record differs from pending ticket"
                );
                continue;
            }
            *self
                .next_ticket
                .lock()
                .map_err(|_| anyhow::anyhow!("capture cursor unavailable"))? =
                Some((ticket.registry_epoch, ticket.id));
            self.stats.attempts.fetch_add(1, Ordering::Relaxed);
            let worker = self.clone();
            let result = tokio::time::timeout_at(
                deadline,
                worker.process(ticket, deadline, admission.clone()),
            )
            .await;
            match result {
                Ok(Ok(())) => {
                    self.stats.published.fetch_add(1, Ordering::Relaxed);
                    self.stats.set("published_roots_retained");
                }
                _ => {
                    self.stats.failed.fetch_add(1, Ordering::Relaxed);
                    self.stats.set("retry_or_reconcile");
                }
            }
            // One accepted job per poll; successes advance on the next poll.
            break;
        }
        Ok(())
    }
    async fn process(
        self: Arc<Self>,
        ticket: PendingCapture,
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
    ) -> Result<()> {
        // Identity/routing validation precedes payload reads AND credential use.
        let (mount, alias, identity) = self.host.capture_mapping(&ticket)?;
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
        let client = source
            .control
            .client_for_owned_work(owner.clone(), deadline)
            .await?;
        let intent = if let Some(intent) = existing {
            intent
        } else {
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
            let worker = self.clone();
            let ticket = ticket.clone();
            let physical = owner.clone();
            blocking_owned(owner.clone(), move || {
                let intent = worker.prepare(&ticket, identity, current.generation, physical)?;
                worker.persist(&format!("{}.job", hex(&ticket.id.0)), &intent.encode()?)?;
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
        {
            let worker = self.clone();
            let ticket = ticket.clone();
            let owner = owner.clone();
            let saved = Intent {
                header: intent.header.clone(),
                manifest: intent.manifest.clone(),
                capture: intent.capture.clone(),
            };
            tokio::task::spawn_blocking(move || {
                let _owner = owner.clone();
                worker.verify_saved(&ticket, &saved, owner)
            })
            .await??;
        }
        let existing = client.cloud_publication(&intent.header.key).await?;
        if existing.is_none() {
            // Full metadata closure walk is a bounded background cost, not a
            // thin missing-closure optimization. No target ref/hold is created.
            let decoded = c::decode_manifest_v3(
                &intent.manifest,
                c::ManifestId(intent.header.manifest_id),
                c::ManifestV3Limits::default(),
            )?;
            let retained = (intent.manifest.len() + intent.capture.len())
                .checked_mul(4)
                .context("capture retained bytes overflow")? as u64
                + 128 * 1024 * 1024;
            let transfer = jj_native_remote::NativeTransferLimits {
                max_memory_bytes: self
                    .policy
                    .max_memory_bytes
                    .checked_sub(retained)
                    .context("capture transfer memory limit")?,
                max_temporary_bytes: self.policy.max_temporary_bytes,
                ..Default::default()
            };
            let receipt = source
                .control
                .transfer_native_closure(
                    source.io.clone(),
                    decoded.history.recovery.native_roots.clone(),
                    owner.clone(),
                    transfer,
                    deadline,
                )
                .await?;
            drop(receipt);
            drop(decoded);
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
                        let id = decode_hex::<64>(&descriptor.id)?;
                        let id = if descriptor.kind == 1 {
                            CloudObjectId::Operation(id)
                        } else {
                            CloudObjectId::View(id)
                        };
                        let receipt = client.cloud_put_object(id, payload.bytes()).await?;
                        ensure!(
                            receipt.descriptor.selected == expected,
                            "cloud selected OPVIEW representation differs"
                        );
                    }
                    3 | 4 => {
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
            client
                .cloud_publish_captured_version(&intent.sdk()?)
                .await?;
        }
        let publication = client
            .cloud_publication(&intent.header.key)
            .await?
            .context("capture publication not visible")?;
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
        // Sequential decoder/proof scratch (not measured RSS). Retained models
        // share the remainder of ONE configured job budget; none gets its own
        // hidden pool. Oversized captures remain pending and observable.
        const SCRATCH: u64 = 1024 * 1024 * 1024;
        let mut remaining = self
            .policy
            .max_memory_bytes
            .checked_sub(SCRATCH)
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
        };
        Ok(Intent {
            header,
            manifest: prepared.bytes().to_vec(),
            capture,
        })
    }
    fn verify_saved(&self, ticket: &PendingCapture, intent: &Intent, owner: Owner) -> Result<()> {
        let (objects, artifacts, roots, capture) = self.proofs(ticket, owner)?;
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
        Ok(())
    }
}
fn ticket_binding(ticket: &PendingCapture) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"native-upload-pending-ticket-v1\0");
    h.update(&ticket.id.0);
    h.update(&ticket.registry.0);
    h.update(&ticket.registration.workspace.0);
    h.update(&ticket.registration.op_store.0);
    for n in [
        ticket.registration.generation,
        ticket.command_epoch,
        ticket.registry_epoch,
        ticket.bundle.size_bytes,
    ] {
        h.update(&n.to_le_bytes());
    }
    h.update(&ticket.fingerprint);
    h.update(&ticket.bundle.blake3);
    h.update(&(ticket.checkpoint.roots.len() as u64).to_le_bytes());
    for root in &ticket.checkpoint.roots {
        h.update(&[root.kind as u8]);
        h.update(&root.id.0);
    }
    h.update(&(ticket.checkpoint.op_heads.len() as u64).to_le_bytes());
    for id in &ticket.checkpoint.op_heads {
        h.update(&(id.len() as u64).to_le_bytes());
        h.update(id);
    }
    match &ticket.checkpoint.working_copy_operation {
        Some(id) => {
            h.update(&[1]);
            h.update(&(id.len() as u64).to_le_bytes());
            h.update(id);
        }
        None => {
            h.update(&[0]);
        }
    }
    *h.finalize().as_bytes()
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
        let root = tempfile::Builder::new().tempdir_in(std::env::temp_dir().canonicalize()?)?;
        let state = PrivateDir::create(root.path().join("state"))?;
        let host = Arc::new(NativeCommandHost::open(&state.path().join("native-commands")).await?);
        let mut config = Config::default();
        config.state_dir = state.path().to_owned();
        config.native.workspaces.upload.policy = NativeCaptureUploadPolicy::PublishRetainingRoots;
        config.native.workspaces.upload.timeout_ms = 1000;
        config.native.workspaces.upload.poll_ms = 250;
        Ok((
            root,
            start_inner(config, host, BTreeMap::new(), hooks)
                .await?
                .unwrap(),
        ))
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
}
