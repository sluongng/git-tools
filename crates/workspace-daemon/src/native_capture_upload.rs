//! Optional saved-capture publication and exact-proof redundant capsule cleanup.
//! A historical Published record alone never grants local retirement.
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
    CloudPublishIntent, CloudRawDigest, RetainedVersionSelector,
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
#[derive(Default)]
pub(crate) struct Stats {
    attempts: AtomicU64,
    published: AtomicU64,
    failed: AtomicU64,
    retired: AtomicU64,
    retiring: AtomicBool,
    cleanup_pending: AtomicBool,
    last: Mutex<&'static str>,
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
            "local_roots_retained":true,"current_backup_availability_verified":false
        })
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
struct RetirementProof {
    upload: native_retention::CaptureUploadCleanup,
    // This owner contains admission only, NEVER a CaptureReadGuard. Raw
    // immutable witness bytes retain their budget through exclusive commit.
    coverage: native_retention::OwnedCaptureCoverage,
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
    idle_entries: AtomicU64,
    #[cfg(test)]
    fail_namespace: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    catalog_pause: Mutex<Option<Arc<CatalogPause>>>,
    #[cfg(test)]
    cleanup_pause: Mutex<Option<Arc<CatalogPause>>>,
    #[cfg(test)]
    fail_after_alias_sync: AtomicBool,
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
        next_retirement: Mutex::new(None),
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
            match worker.poll().await {
                Ok(true) => {
                    // A durable publication or completed cleanup may expose more
                    // ready work. Yield and check stop before the next bounded job.
                    tokio::task::yield_now().await;
                    continue;
                }
                Ok(false) => {}
                Err(_) => {
                    worker.stats.failed.fetch_add(1, Ordering::Relaxed);
                    worker.stats.set("inventory_unavailable");
                }
            }
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
    async fn poll(self: &Arc<Self>) -> Result<bool> {
        let mut progressed = false;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(self.policy.timeout_ms);
        let admission = Arc::new(Admission {
            _permit: tokio::time::timeout_at(deadline, self.admission.clone().acquire_owned())
                .await??,
        });
        if matches!(
            self.policy.policy,
            NativeCaptureUploadPolicy::Auto | NativeCaptureUploadPolicy::PublishRetiringRedundant
        ) {
            progressed |=
                tokio::time::timeout_at(deadline, self.complete_cleanup(admission.clone()))
                    .await??;
        }
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
            if self.policy.policy == NativeCaptureUploadPolicy::Auto
                && !self.host.capture_auto_upload(&ticket)?
            {
                continue;
            }
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
        // Publish pending work BEFORE proving retirement of old versions. A
        // missing/unavailable old proof must retain it, not starve new backups.
        if matches!(
            self.policy.policy,
            NativeCaptureUploadPolicy::Auto | NativeCaptureUploadPolicy::PublishRetiringRedundant
        ) {
            match tokio::time::timeout_at(deadline, self.retire_one(deadline, admission.clone()))
                .await
            {
                Ok(Ok(retired)) => progressed |= retired,
                _ => {
                    self.stats.failed.fetch_add(1, Ordering::Relaxed);
                    self.stats.set("retirement_retry_preserved");
                }
            }
        }
        Ok(progressed)
    }
    async fn complete_cleanup(self: &Arc<Self>, admission: Arc<Admission>) -> Result<bool> {
        let worker = self.clone();
        // Accepted cleanup owns both fences and admission after waiter expiry.
        tokio::spawn(async move { worker.cleanup_owned(admission).await }).await?
    }
    async fn cleanup_owned(self: Arc<Self>, admission: Arc<Admission>) -> Result<bool> {
        let registry = self.host.capture_registry();
        let pending = registry.capture_cleanup().await?;
        self.stats
            .cleanup_pending
            .store(pending.is_some(), Ordering::Release);
        if pending.is_none() {
            return Ok(false);
        }
        // Capture callbacks acquire this permit before taking registry read
        // fences. Preserve that order, otherwise cleanup could deadlock a writer.
        let paused = self.host.pause_capture_cleanup().await?;
        let Some(guard) = registry.try_capture_cleanup().await? else {
            self.stats.cleanup_pending.store(false, Ordering::Release);
            return Ok(false);
        };
        let worker = self.clone();
        let physical = paused.clone();
        let completed = blocking_owned(admission as Owner, move || {
            worker.unlink_retired(guard.obligation())?;
            physical.reconcile_disk()?;
            let published = worker.inventory()?.2;
            Ok((guard, published))
        })
        .await?;
        let (guard, published) = completed.value;
        guard.acknowledge_capture_cleanup().await?;
        self.stats.published.store(published, Ordering::Relaxed);
        // The capture permit remains held until real disk and registry counts
        // agree. Never uncharge on a logical ticket removal or upload ACK alone.
        paused.refresh(&registry).await;
        self.stats.retired.fetch_add(1, Ordering::Relaxed);
        self.stats.cleanup_pending.store(false, Ordering::Release);
        self.stats.set("retired_redundant_local_capture");
        Ok(true)
    }
    fn unlink_retired(
        &self,
        obligation: &native_retention::CaptureCleanupObligation,
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
        files.push(validate_cleanup_file(
            &self.directory,
            &format!("{name}.published"),
            obligation.upload.published,
        )?);
        if obligation.remove_bundle {
            files.push(validate_cleanup_file(
                &captures,
                &format!("{}.pack", hex(&obligation.retired.bundle.blake3)),
                obligation.retired.bundle,
            )?);
        }
        // Validate ALL present files before removing any. Missing known files
        // are the durable commit/unlink crash prefix, never discovery authority.
        for item in files.into_iter().flatten() {
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
    ) -> Result<bool> {
        let worker = self.clone();
        tokio::spawn(async move { worker.retire_one_owned(deadline, admission).await }).await?
    }
    async fn retire_one_owned(
        self: Arc<Self>,
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
    ) -> Result<bool> {
        let registry = self.host.capture_registry();
        if registry.capture_cleanup().await?.is_some() {
            return self.cleanup_owned(admission).await;
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
            if !self.directory.path().join(&published_name).try_exists()? {
                continue;
            }
            *self
                .next_retirement
                .lock()
                .map_err(|_| anyhow::anyhow!("capture retirement cursor unavailable"))? =
                Some((old.registry_epoch, old.id));
            let Some(proof) = self
                .prove_redundant(&old, &newer, deadline, admission.clone())
                .await?
            else {
                self.stats.set("local_capture_not_covered");
                return Ok(false);
            };
            ensure!(
                tokio::time::Instant::now() < deadline,
                "capture retirement deadline exceeded"
            );
            // All read guards, verified packs, payloads and SDK owners were
            // dropped in prove_redundant before taking this exclusive fence.
            registry
                .retire_redundant_capture_with_coverage(old, newer, proof.upload, proof.coverage)
                .await?;
            self.stats.cleanup_pending.store(true, Ordering::Release);
            return self.cleanup_owned(admission).await;
        }
        Ok(false)
    }
    async fn prove_redundant(
        self: &Arc<Self>,
        old: &PendingCapture,
        newer: &PendingCapture,
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
    ) -> Result<Option<RetirementProof>> {
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
            let intent = Intent::decode(&job)?;
            let published = worker.read_durable(&format!("{}.published", hex(&expected.id.0)))?;
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
                    published: raw_descriptor(&published),
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
        let result = blocking_owned(owner.clone(), move || {
            let snapshot = worker.verify_saved(&old_ticket, &intent.0, proof_owner.clone())?;
            // Fully verify the successor's semantic OP/VIEW/index graph and its
            // checkpoint binding even if it has not yet reached the server.
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
            if !worker.descriptor_coverage(&old_ticket, &newer_ticket, proof_owner)? {
                return Ok(None);
            }
            Ok(Some((intent.1.clone(), snapshot)))
        })
        .await?;
        let Some((upload, snapshot)) = result.value else {
            return Ok(None);
        };
        ensure!(
            self.policy.max_memory_bytes
                >= native_retention::MAX_CAPTURE_COVERAGE_WORK_BYTES as u64,
            "capture coverage proof admission too small"
        );
        let objects = self
            .canonical_coverage(
                source.local.clone(),
                old,
                newer,
                snapshot,
                owner.clone(),
                deadline,
            )
            .await?;
        // Return immutable raw witness bytes with admission ONLY. All local
        // read fences, pack leases and SDK owners drop before exclusive retire.
        Ok(Some(RetirementProof {
            upload,
            coverage: native_retention::OwnedCaptureCoverage::new(objects, coverage_owner),
        }))
    }
    async fn canonical_coverage(
        &self,
        local: Arc<jj_native_backend::DirectObjectIo>,
        old: &PendingCapture,
        newer: &PendingCapture,
        snapshot: [u8; 32],
        owner: Owner,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<tinytree_model::EncodedObject>> {
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
            return Ok(Vec::new());
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
            let key = pending
                .pop_front()
                .context("successor does not prove retained canonical root coverage")?;
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
        Ok(objects)
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
    async fn process(
        self: Arc<Self>,
        ticket: PendingCapture,
        deadline: tokio::time::Instant,
        admission: Arc<Admission>,
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
            // Automatically enrolled workspaces reuse authenticated retained
            // boundaries and transfer only their local frontier. An unavailable
            // proof remains queued; it never falls back to a public-history walk.
            // Explicit/manual workspaces retain the original full-transfer path.
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
    fn verify_saved(
        &self,
        ticket: &PendingCapture,
        intent: &Intent,
        owner: Owner,
    ) -> Result<[u8; 32]> {
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
        Ok(data.working_copy.snapshot_commit)
    }
}
fn raw_descriptor(bytes: &[u8]) -> native_retention::BundleDescriptor {
    native_retention::BundleDescriptor {
        blake3: *blake3::hash(bytes).as_bytes(),
        size_bytes: bytes.len() as u64,
    }
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
    )> {
        use futures::StreamExt;
        let root = tempfile::Builder::new().tempdir_in(std::env::temp_dir().canonicalize()?)?;
        let server_root = private(root.path(), "server")?;
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

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(service.deadline_layer())
                .add_service(service.grpc_service())
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
        let state = private(root.path(), "state")?;
        let mut config = Config::default();
        config.state_dir = state.path().to_owned();
        config.native.workspaces.upload.policy = NativeCaptureUploadPolicy::PublishRetainingRoots;
        assert_eq!(config.native.workspaces.upload.poll_ms, 30_000);
        let host = Arc::new(
            NativeCommandHost::open_with_capture(
                &state.path().join("native-commands"),
                config.native.workspaces.capture.clone(),
            )
            .await?,
        );
        let workspace = [7; 32];
        let mount = hex(&workspace);
        let workspace_root = private(root.path(), "workspace")?;
        let admin = private(root.path(), "admin")?;
        host.register(
            &native_workspace_admin::Binding {
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
            },
            source.identity,
        )
        .await?;
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
        let capsule = c::encode_capture_data(&data, Default::default())?;
        payloads.push((5, hex(&c::CaptureDataId::of(&capsule).0), capsule));
        let registry = host.capture_registry();
        let registration = host.workspace_state(&mount).await?.registration;
        let captures = PrivateDir::open_existing(state.path().join("native-commands/captures"))?;
        for number in 0..count {
            let mut payloads = payloads.clone();
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
            let roots = tree
                .terms
                .iter()
                .map(|id| tinytree_model::ObjectKey {
                    kind: tinytree_model::Kind::Tree,
                    id: tinytree_model::ObjectId(id.0),
                })
                .collect();
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
        let handle = start_inner(config, host, sources, Arc::new(TestHooks::default()))
            .await?
            .unwrap();
        Ok((root, handle, client, stop, server))
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn default_poll_drains_queued_publications_then_backs_off_when_idle() -> Result<()> {
        let (_root, handle, client, stop, server) = ready_uploads(3, true).await?;
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
        let (_root, handle, _client, stop, server) = ready_uploads(3, false).await?;
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
                    published: raw_descriptor(&published),
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
