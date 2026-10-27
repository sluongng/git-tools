//! Bounded local-only capture staging. No cloud upload, Clean or collection.
use crate::config::{NativeCapture, NativeCapturePolicy};
use anyhow::{Context, Result, ensure};
use native_capture_pack::{Entry, MAGIC, TABLE_MAGIC, encode_table, frame_header};
use native_retention::{BundleDescriptor, CommandOwner, OwnedCaptureBundle, RootCheckpoint};
use native_workspace_admin::{
    Binding, Registry,
    capture::{
        self, CaptureInput, CapturedBlob, LocalCaptureSink, OwnedCaptureBlob, QuiescentCapture,
    },
};
use private_fs::PrivateDir;
use serde::Serialize;
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const STAGER_MEMORY: usize = 64 * 1024 * 1024;
const MAX_ENTRIES: usize = 102_051;
const MAX_TABLE: usize = 31 * 1024 * 1024;
#[derive(Clone)]
pub(crate) struct Target {
    pub auto_enrolled: bool,
    pub controller: std::sync::Weak<native_vfs_controller::Controller>,
    pub registry: Arc<Registry>,
    pub binding: Binding,
    pub local: Arc<jj_native_backend::DirectObjectIo>,
    pub index: jj_native_index::Config,
    pub remote_scope: native_workspace_admin::remote_state::RemoteScope,
}
#[derive(Default)]
struct Inventory {
    count: usize,
    bytes: u64,
    dirty: usize,
    clean: usize,
    unavailable: usize,
    root_claims: usize,
    available: bool,
}
pub(crate) struct Coordinator {
    config: NativeCapture,
    directory: Option<Arc<PrivateDir>>,
    active: Arc<Semaphore>,
    disk_bytes: Arc<AtomicU64>,
    disk_files: Arc<AtomicUsize>,
    inventory: Mutex<Inventory>,
    refresh_lock: tokio::sync::Mutex<()>,
    attempted: AtomicU64,
    queued: AtomicU64,
    reused_pending: AtomicU64,
    skipped: AtomicU64,
    failed: AtomicU64,
    last: Mutex<&'static str>,
}
pub(crate) enum Plan {
    DrainOnly,
    Capture {
        target: Arc<Target>,
        permit: Arc<OwnedSemaphorePermit>,
        disk: Arc<DiskReservation>,
    },
}
impl Coordinator {
    pub(crate) async fn open(
        path: &Path,
        config: NativeCapture,
        registry: &native_retention::Registry,
    ) -> Result<Arc<Self>> {
        config.validate()?;
        let directory = if config.policy == NativeCapturePolicy::Disabled {
            None
        } else {
            Some(Arc::new(PrivateDir::create(path)?))
        };
        let (files, bytes) = match &directory {
            Some(directory) => scan_spool(directory, config.max_pending)?,
            None => (0, 0),
        };
        let this = Arc::new(Self {
            config,
            directory,
            active: Arc::new(Semaphore::new(1)),
            disk_bytes: Arc::new(AtomicU64::new(bytes)),
            disk_files: Arc::new(AtomicUsize::new(files)),
            inventory: Mutex::new(Inventory::default()),
            refresh_lock: tokio::sync::Mutex::new(()),
            attempted: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            reused_pending: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            last: Mutex::new("disabled"),
        });
        this.refresh(registry).await;
        Ok(this)
    }
    /// Pause capture staging before deleting an exact retired bundle. The
    /// physical cleanup worker must retain this guard through namespace sync.
    pub(crate) async fn pause_for_cleanup(self: &Arc<Self>) -> Result<Arc<SpoolCleanupGuard>> {
        let permit = self.active.clone().acquire_owned().await?;
        Ok(Arc::new(SpoolCleanupGuard {
            coordinator: self.clone(),
            _permit: permit,
        }))
    }
    pub(crate) async fn refresh(&self, registry: &native_retention::Registry) {
        let _single = self.refresh_lock.lock().await;
        if let Ok(mut inventory) = self.inventory.lock() {
            inventory.available = false;
        }
        let Ok(pending) = registry.pending_captures().await else {
            if let Ok(mut value) = self.inventory.lock() {
                value.available = false;
            }
            return;
        };
        let mut value = Inventory {
            available: true,
            ..Default::default()
        };
        for item in pending {
            value.count += 1;
            value.bytes = value.bytes.saturating_add(item.bundle.size_bytes);
            value.root_claims = value
                .root_claims
                .saturating_add(item.checkpoint.roots.len());
            match registry.workspace(item.registration).await {
                Ok(state) if state.state == native_retention::WorkspaceState::Dirty => {
                    value.dirty += 1
                }
                Ok(state) if state.state == native_retention::WorkspaceState::Clean => {
                    value.clean += 1
                }
                _ => value.unavailable += 1,
            }
        }
        if let Ok(mut inventory) = self.inventory.lock() {
            *inventory = value;
        }
    }
    pub(crate) fn disposition(&self, category: &'static str, failed: bool) {
        if let Ok(mut last) = self.last.lock() {
            *last = category;
        }
        if failed {
            self.failed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.skipped.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn queued(&self) {
        if let Ok(mut inventory) = self.inventory.lock() {
            inventory.available = false;
        }
        self.queued.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut last) = self.last.lock() {
            *last = "queued";
        }
    }
    pub(crate) fn record_dirty_capture(
        &self,
        disposition: &native_retention::DirtyCaptureDisposition,
    ) {
        match disposition {
            native_retention::DirtyCaptureDisposition::Queued(_) => self.queued(),
            native_retention::DirtyCaptureDisposition::ReusedPending(_) => self.reused_pending(),
        }
    }
    fn reused_pending(&self) {
        if let Ok(mut inventory) = self.inventory.lock() {
            inventory.available = false;
        }
        self.reused_pending.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut last) = self.last.lock() {
            *last = "reused_pending_exact";
        }
    }
    pub(crate) async fn select_until(
        &self,
        target: Option<Arc<Target>>,
        registry: &native_retention::Registry,
        deadline: tokio::time::Instant,
    ) -> Plan {
        if self.config.policy == NativeCapturePolicy::Disabled {
            return Plan::DrainOnly;
        }
        self.attempted.fetch_add(1, Ordering::Relaxed);
        let Some(target) = target else {
            self.disposition("target_unavailable", false);
            return Plan::DrainOnly;
        };
        if self.config.policy == NativeCapturePolicy::Auto && !target.auto_enrolled {
            return Plan::DrainOnly;
        }
        let permit =
            match tokio::time::timeout_at(deadline, self.active.clone().acquire_owned()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => {
                    self.disposition("budget_unavailable", false);
                    return Plan::DrainOnly;
                }
                Err(_) => {
                    self.disposition("admission_timeout", false);
                    return Plan::DrainOnly;
                }
            };
        if tokio::time::Instant::now() >= deadline {
            self.disposition("admission_timeout", false);
            return Plan::DrainOnly;
        }
        // A preceding capture invalidates cached inventory until its commit is
        // observed. Refresh under the staging permit instead of skipping the
        // next command against that deliberately unavailable cache. Accepted
        // physical inventory work retains the permit; do not cancel it midway.
        self.refresh(registry).await;
        if tokio::time::Instant::now() >= deadline {
            self.disposition("admission_timeout", false);
            return Plan::DrainOnly;
        }
        let available = self.inventory.lock().is_ok_and(|value| {
            value.available
                && value.count < self.config.max_pending
                && value
                    .bytes
                    .checked_add(self.config.max_bundle_bytes)
                    .is_some_and(|n| n <= self.config.max_pending_bytes)
        });
        if !available
            || self.disk_files.load(Ordering::Acquire) >= self.config.max_pending.saturating_mul(4)
            || self
                .disk_bytes
                .load(Ordering::Acquire)
                .checked_add(self.config.max_bundle_bytes)
                .is_none_or(|n| n > self.config.max_pending_bytes)
        {
            self.disposition("spool_full", false);
            return Plan::DrainOnly;
        }
        self.disk_bytes
            .fetch_add(self.config.max_bundle_bytes, Ordering::AcqRel);
        Plan::Capture {
            target,
            permit: Arc::new(permit),
            disk: Arc::new(DiskReservation {
                charged: self.config.max_bundle_bytes,
                actual: AtomicU64::new(0),
                bytes: self.disk_bytes.clone(),
            }),
        }
    }
    pub(crate) fn timeout(&self) -> Duration {
        Duration::from_millis(self.config.timeout_ms)
    }
    pub(crate) fn status(&self) -> serde_json::Value {
        let value = self.inventory.lock().unwrap_or_else(|e| e.into_inner());
        serde_json::json!({"policy":self.config.policy,"pending_count":value.count,"pending_bytes":value.bytes,"pending_dirty":value.dirty,"pending_clean":value.clean,"pending_unavailable":value.unavailable,"pending_root_claims":value.root_claims,"inventory_available":value.available,"charged_spool_bytes":self.directory.as_ref().map(|_|self.disk_bytes.load(Ordering::Relaxed)),"attempted":self.attempted.load(Ordering::Relaxed),"queued":self.queued.load(Ordering::Relaxed),"reused_pending":self.reused_pending.load(Ordering::Relaxed),"skipped":self.skipped.load(Ordering::Relaxed),"failed":self.failed.load(Ordering::Relaxed),"last_disposition":*self.last.lock().unwrap_or_else(|e|e.into_inner())})
    }
}
pub(crate) struct SpoolCleanupGuard {
    coordinator: Arc<Coordinator>,
    _permit: OwnedSemaphorePermit,
}
impl SpoolCleanupGuard {
    /// Recount real files only after unlink+fsync. Holding the staging permit
    /// ensures no reservation can later uncharge this newly observed inventory.
    pub(crate) fn reconcile_disk(&self) -> Result<()> {
        if let Some(directory) = &self.coordinator.directory {
            let (files, bytes) = scan_spool(directory, self.coordinator.config.max_pending)?;
            self.coordinator.disk_bytes.store(bytes, Ordering::Release);
            self.coordinator.disk_files.store(files, Ordering::Release);
        }
        Ok(())
    }
    pub(crate) async fn refresh(&self, registry: &native_retention::Registry) {
        self.coordinator.refresh(registry).await;
    }
}
fn scan_spool(directory: &PrivateDir, max_pending: usize) -> Result<(usize, u64)> {
    let (mut files, mut bytes) = (0usize, 0u64);
    for entry in std::fs::read_dir(directory.path())? {
        files += 1;
        ensure!(
            files <= max_pending.saturating_mul(4),
            "local capture spool requires bounded reconciliation"
        );
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().context("unknown local capture spool entry")?;
        let known = name
            .strip_suffix(".pack")
            .is_some_and(|id| id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
            || name
                .strip_prefix("capture-")
                .and_then(|id| id.strip_suffix(".part"))
                .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok());
        ensure!(known, "unknown local capture spool state; preserve it");
        let file = directory.open_existing_file(name, u64::MAX)?;
        bytes = bytes
            .checked_add(file.metadata()?.len())
            .context("capture spool bytes overflow")?;
    }
    File::open(directory.path())?.sync_all()?;
    File::open(directory.path().parent().context("capture spool parent")?)?.sync_all()?;
    Ok((files, bytes))
}
#[derive(Default)]
pub(crate) struct Completion {
    done: AtomicBool,
    changed: tokio::sync::Notify,
}
impl Completion {
    pub(crate) fn complete(&self) {
        self.done.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
    pub(crate) async fn wait(&self, timeout: Duration) -> bool {
        self.wait_until(tokio::time::Instant::now() + timeout).await
    }
    pub(crate) async fn wait_until(&self, deadline: tokio::time::Instant) -> bool {
        if self.done.load(Ordering::Acquire) {
            return true;
        }
        tokio::time::timeout_at(deadline, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.done.load(Ordering::Acquire) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .is_ok()
    }
}
pub(crate) struct DiskReservation {
    charged: u64,
    actual: AtomicU64,
    bytes: Arc<AtomicU64>,
}
impl Drop for DiskReservation {
    fn drop(&mut self) {
        self.bytes.fetch_sub(
            self.charged
                .saturating_sub(self.actual.load(Ordering::Acquire)),
            Ordering::AcqRel,
        );
    }
}
struct CompleteOnDrop(Arc<Completion>);
impl Drop for CompleteOnDrop {
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}
struct Lifetime {
    // Rust drops fields in order: signal only after kernel/read fence, byte
    // reservation and the global work permit have actually been released.
    _kernel: File,
    _command: Arc<CommandOwner>,
    disk: Arc<DiskReservation>,
    _permit: Arc<OwnedSemaphorePermit>,
    _complete: CompleteOnDrop,
}

struct Pack {
    file: File,
    directory: Arc<PrivateDir>,
    temporary: String,
    hash: blake3::Hasher,
    size: u64,
    maximum: u64,
    entries: Vec<Entry>,
    seen: BTreeSet<(u8, String)>,
    disk: Arc<DiskReservation>,
    disk_files: Arc<AtomicUsize>,
    _lifetime: Arc<Lifetime>,
}
impl Drop for Pack {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        let actual = self
            .file
            .metadata()
            .map(|metadata| {
                if metadata.nlink() == 0 {
                    0
                } else {
                    metadata.len().min(self.maximum)
                }
            })
            .unwrap_or(self.maximum);
        self.disk.actual.store(actual, Ordering::Release);
    }
}
impl Pack {
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let size = self
            .size
            .checked_add(bytes.len() as u64)
            .context("capture pack bytes overflow")?;
        ensure!(size <= self.maximum, "capture pack byte limit exceeded");
        self.file.write_all(bytes)?;
        self.hash.update(bytes);
        self.size = size;
        Ok(())
    }
    fn entry(&mut self, kind: u8, id: String, bytes: &[u8]) -> Result<()> {
        ensure!(
            self.entries.len() < MAX_ENTRIES
                && matches!(id.len(), 64 | 128)
                && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "capture entry bound exceeded"
        );
        ensure!(
            self.entries
                .len()
                .checked_add(1)
                .and_then(|n| n.checked_mul(512))
                .is_some_and(|n| n <= STAGER_MEMORY),
            "capture entry memory budget exceeded"
        );
        ensure!(
            self.seen.insert((kind, id.clone())),
            "duplicate capture pack entry"
        );
        let blake3 = *blake3::hash(bytes).as_bytes();
        let header = frame_header(kind, &id, bytes.len() as u64, blake3)?;
        self.write(&header)?;
        let offset = self.size;
        self.write(bytes)?;
        self.entries.push(Entry {
            kind,
            id,
            blake3,
            size: bytes.len() as u64,
            offset,
        });
        Ok(())
    }
}
struct Writer {
    inner: Arc<Mutex<Pack>>,
    attempt: Arc<Mutex<Attempt>>,
}
#[async_trait::async_trait]
impl LocalCaptureSink for Writer {
    async fn write(&mut self, blob: OwnedCaptureBlob) -> Result<()> {
        let pack = self.inner.clone();
        let attempt = self.attempt.clone();
        tokio::task::spawn_blocking(move || {
            let (kind, id) = match blob.blob() {
                CapturedBlob::OperationView(object) => (
                    if object.operation().is_some() { 1 } else { 2 },
                    hex(object.id().bytes()),
                ),
                CapturedBlob::Index(object) => match object.descriptor().id {
                    tinytree_cloud::IndexArtifactId::Root(id) => (3, hex(&id.0)),
                    tinytree_cloud::IndexArtifactId::Node(id) => (4, hex(&id.0)),
                },
            };
            let result = pack
                .lock()
                .map_err(|_| anyhow::anyhow!("capture writer unavailable"))?
                .entry(kind, id, blob.bytes());
            if result.is_err() {
                *attempt.lock().unwrap_or_else(|e| e.into_inner()) =
                    Attempt::Unavailable("staging_failed");
            }
            drop(blob);
            result
        })
        .await
        .context("capture writer worker failed")?
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn verify_existing(
    directory: &PrivateDir,
    name: &str,
    descriptor: BundleDescriptor,
) -> Result<File> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(directory.path().join(name))?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.mode() & 0o077 == 0
            && metadata.len() == descriptor.size_bytes,
        "existing capture pack differs"
    );
    let mut hash = blake3::Hasher::new();
    let mut buffer = [0; 64 * 1024];
    let mut read = 0u64;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        read = read
            .checked_add(n as u64)
            .context("capture verify overflow")?;
        ensure!(read <= descriptor.size_bytes, "capture pack grew");
        hash.update(&buffer[..n]);
    }
    ensure!(
        read == descriptor.size_bytes && hash.finalize().as_bytes() == &descriptor.blake3,
        "existing capture pack digest differs"
    );
    file.sync_all()?;
    Ok(file)
}
impl Writer {
    async fn finish(self, capture: QuiescentCapture) -> Result<OwnedCaptureBundle> {
        let capture = Arc::new(capture);
        let pack = self.inner;
        tokio::task::spawn_blocking(move || {
            let data = capture.data();
            let roots = data
                .roots
                .iter()
                .map(|root| {
                    Ok(tinytree_model::ObjectKey {
                        kind: tinytree_model::Kind::try_from(root.kind)
                            .map_err(|_| anyhow::anyhow!("invalid captured root kind"))?,
                        id: tinytree_model::ObjectId(root.id),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let operation = |id: &str| -> Result<Vec<u8>> {
                ensure!(id.len() == 128, "captured operation width");
                (0..id.len())
                    .step_by(2)
                    .map(|i| {
                        u8::from_str_radix(&id[i..i + 2], 16).context("captured operation encoding")
                    })
                    .collect()
            };
            let checkpoint = RootCheckpoint {
                roots,
                op_heads: data
                    .operation_heads
                    .iter()
                    .map(|id| operation(id))
                    .collect::<Result<_>>()?,
                working_copy_operation: Some(operation(&data.working_copy.wc_operation)?),
            };
            let mut writer = pack
                .lock()
                .map_err(|_| anyhow::anyhow!("capture writer unavailable"))?;
            writer
                .entries
                .sort_unstable_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
            ensure!(
                writer.entries.len() == data.blobs.len(),
                "capture pack inventory differs"
            );
            for (actual, expected) in writer.entries.iter().zip(&data.blobs) {
                ensure!(
                    actual.kind == expected.kind
                        && actual.id == expected.id
                        && actual.blake3 == expected.blake3
                        && actual.size == expected.size,
                    "capture pack blob differs"
                );
            }
            writer.entry(5, hex(&capture.data_digest()), capture.encoded_data())?;
            let private = capture.candidate();
            writer.entry(
                128,
                hex(blake3::hash(private.private_working_copy_bytes()).as_bytes()),
                private.private_working_copy_bytes(),
            )?;
            if let Some(bytes) = private.private_remote_bytes() {
                writer.entry(129, hex(blake3::hash(bytes).as_bytes()), bytes)?;
            }
            // Conservative admission precedes JSON allocation. Never accumulate
            // a full payload pack in memory or use a path-bearing cloud object.
            let bound = writer
                .entries
                .len()
                .checked_mul(512)
                .context("capture table overflow")?;
            ensure!(bound <= MAX_TABLE, "capture table budget exceeded");
            let table = encode_table(&writer.entries)?;
            ensure!(table.len() <= MAX_TABLE, "capture table too large");
            writer.write(TABLE_MAGIC)?;
            writer.write(&(table.len() as u64).to_le_bytes())?;
            writer.write(&table)?;
            let descriptor = BundleDescriptor {
                blake3: *writer.hash.finalize().as_bytes(),
                size_bytes: writer.size,
            };
            writer.file.sync_all()?;
            let name = format!("{}.pack", hex(&descriptor.blake3));
            let existing = match std::fs::hard_link(
                writer.directory.path().join(&writer.temporary),
                writer.directory.path().join(&name),
            ) {
                Ok(()) => {
                    writer.disk_files.fetch_add(1, Ordering::AcqRel);
                    None
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    Some(verify_existing(&writer.directory, &name, descriptor)?)
                }
                Err(error) => return Err(error.into()),
            };
            File::open(writer.directory.path())?.sync_all()?;
            std::fs::remove_file(writer.directory.path().join(&writer.temporary))?;
            writer.disk_files.fetch_sub(1, Ordering::AcqRel);
            File::open(writer.directory.path())?.sync_all()?;
            drop(writer);
            let verified_existing = existing.is_some();
            let bundle = OwnedCaptureBundle::new(
                native_retention::CaptureBundle {
                    checkpoint,
                    bundle: descriptor,
                },
                // Close the verified old file before the lifetime signals final
                // physical teardown through the last pack owner.
                Arc::new((existing, capture, pack)),
            );
            Ok(if verified_existing {
                bundle.with_verified_existing_bundle()
            } else {
                bundle
            })
        })
        .await
        .context("capture finalizer worker failed")?
    }
}
/// Callback progress distinguishes unavailable local inputs from an uncertain
/// ledger outcome. It carries no private error strings or serialized authority.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Attempt {
    Selected,
    Entered,
    Unavailable(&'static str),
    Staged,
}
pub(crate) async fn capture(
    coordinator: Arc<Coordinator>,
    target: Arc<Target>,
    command: Arc<CommandOwner>,
    kernel: File,
    permit: Arc<OwnedSemaphorePermit>,
    disk: Arc<DiskReservation>,
    completion: Arc<Completion>,
    attempt: Arc<Mutex<Attempt>>,
    deadline: tokio::time::Instant,
) -> Result<OwnedCaptureBundle> {
    *attempt.lock().unwrap_or_else(|e| e.into_inner()) = Attempt::Entered;
    let lifetime = Arc::new(Lifetime {
        _kernel: kernel,
        _command: command,
        disk: disk.clone(),
        _permit: permit,
        _complete: CompleteOnDrop(completion),
    });
    let result = async {
        if tokio::time::Instant::now() >= deadline {
            *attempt.lock().unwrap_or_else(|e| e.into_inner()) =
                Attempt::Unavailable("capture_timeout");
            anyhow::bail!("local capture deadline exceeded");
        }
        // Workspace registration must not own a Host -> Controller -> Broker
        // -> Host cycle. The actual capture guard retains the upgraded instance.
        let controller = target
            .controller
            .upgrade()
            .context("native capture controller unavailable")?;
        let mut limits = capture::CaptureLimits::default();
        limits.max_working_bytes = 1024 * 1024 * 1024 - STAGER_MEMORY;
        let index = PrivateDir::open_existing(target.binding.repo.join("index"))?;
        if index.read("type", 128)? != jj_native_index::NAME.as_bytes() {
            *attempt.lock().unwrap_or_else(|e| e.into_inner()) =
                Attempt::Unavailable("unsupported_index");
            anyhow::bail!("unsupported local capture index");
        }
        let state_cap = limits
            .max_file_bytes
            .min(limits.max_decode_bytes.saturating_sub(4096) / 32);
        let (wc, projection) = jj_vfs::native::read_retained_state_for_capture(
            &target.binding.projection(),
            state_cap,
        )?;
        ensure!(!wc.pending, "pending native working-copy state");
        drop(wc); // Host baseline decode never overlaps core's repeated reads.
        let controller = controller
            .capture_for_backup_until(&projection, deadline)
            .await?;
        if tokio::time::Instant::now() >= deadline {
            // Only a successfully returned read/freeze guard exists here; its
            // ordinary teardown precedes completion. No staged mutation exists.
            *attempt.lock().unwrap_or_else(|e| e.into_inner()) =
                Attempt::Unavailable("capture_timeout");
            anyhow::bail!("local capture deadline exceeded");
        }
        let proof = capture::TreeFreezePermit::from_controller(controller, lifetime.clone())?;
        let directory = coordinator
            .directory
            .clone()
            .context("capture staging disabled")?;
        use std::os::unix::fs::OpenOptionsExt;
        let temporary = format!("capture-{}.part", uuid::Uuid::new_v4());
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.path().join(&temporary))?;
        coordinator.disk_files.fetch_add(1, Ordering::AcqRel);
        disk.actual
            .store(coordinator.config.max_bundle_bytes, Ordering::Release);
        let inner = Arc::new(Mutex::new(Pack {
            file,
            directory,
            temporary,
            hash: blake3::Hasher::new(),
            size: 0,
            maximum: coordinator.config.max_bundle_bytes,
            entries: vec![],
            seen: BTreeSet::new(),
            disk: disk.clone(),
            disk_files: coordinator.disk_files.clone(),
            _lifetime: lifetime.clone(),
        }));
        inner.lock().unwrap().write(MAGIC)?;
        let mut writer = Writer {
            inner,
            attempt: attempt.clone(),
        };
        let input = CaptureInput {
            registry: target.registry.clone(),
            binding: target.binding.clone(),
            index_config: target.index.clone(),
            local_objects: target.local.clone(),
            remote_scope: target.remote_scope.clone(),
        };
        let phase = attempt.clone();
        tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(2)
                .build()
                .context("capture reader runtime unavailable")?;
            runtime.block_on(async move {
                // Queueing, kernel-lease and local preparation consume the same
                // original command budget. Never grant a fresh full core timer.
                let remaining = deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .as_millis();
                if remaining == 0 {
                    *phase.lock().unwrap_or_else(|e| e.into_inner()) =
                        Attempt::Unavailable("capture_timeout");
                    anyhow::bail!("local capture deadline exceeded");
                }
                limits.max_elapsed_ms = remaining.min(u64::MAX as u128) as u64;
                let candidate = capture::read_candidate(input, proof, limits, &mut writer).await?;
                let result = writer.finish(candidate.seal_quiescent()?).await;
                if result.is_err() {
                    *phase.lock().unwrap_or_else(|e| e.into_inner()) =
                        Attempt::Unavailable("staging_failed");
                }
                result
            })
        })
        .await
        .context("capture reader worker failed")?
    }
    .await;
    match &result {
        Ok(_) => *attempt.lock().unwrap_or_else(|e| e.into_inner()) = Attempt::Staged,
        Err(_) => {
            let mut phase = attempt.lock().unwrap_or_else(|e| e.into_inner());
            if !matches!(*phase, Attempt::Unavailable(_)) {
                *phase = Attempt::Unavailable("candidate_unavailable");
            }
        }
    }
    result
}
