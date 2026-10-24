use super::*;
use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
use fs2::FileExt;
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::OnceLock,
};
use tokio::runtime::{Builder, Runtime as IoRuntime};
const FORMAT: &[u8] = b"native-retention-2\n";
const MARKER: &str = "registry.format";
const MANIFEST: &[u8] = b"manifest";

pub(crate) fn runtime() -> Result<&'static IoRuntime> {
    static RUNTIME: OnceLock<std::io::Result<IoRuntime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(8)
                .enable_all()
                .thread_name("native-retention-io")
                .build()
        })
        .as_ref()
        .map_err(|_| Error::Storage)
}
pub(crate) async fn blocking<T: Send + 'static>(
    job: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    runtime()?
        .spawn_blocking(job)
        .await
        .map_err(|_| Error::Poisoned)?
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    version: u32,
    id: RegistryId,
    pub epoch: u64,
    untracked: bool,
    workspace_count: u64,
    workspace_commitment: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    captures: Option<CaptureInventory>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureInventory {
    version: u32,
    count: u64,
    commitment: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredCapture {
    version: u32,
    id: CaptureId,
    registry: RegistryId,
    registry_epoch: u64,
    bundle: BundleDescriptor,
    checkpoint: Entry,
}
impl StoredCapture {
    fn pending(&self) -> Result<PendingCapture> {
        Ok(PendingCapture {
            id: self.id,
            registry: self.registry,
            registration: self.checkpoint.registration(),
            command_epoch: self.checkpoint.command_epoch,
            registry_epoch: self.registry_epoch,
            fingerprint: self
                .checkpoint
                .fingerprint
                .ok_or(Error::Corrupt("capture fingerprint"))?,
            bundle: self.bundle,
            checkpoint: self.checkpoint.checkpoint()?,
        })
    }
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRoot {
    kind: u8,
    id: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    workspace: WorkspaceId,
    op_store: OpStoreId,
    generation: u64,
    previous_generation: Option<u64>,
    pub command_epoch: u64,
    pub state: WorkspaceState,
    checkpoint_epoch: Option<u64>,
    roots: Vec<StoredRoot>,
    op_heads: Vec<Vec<u8>>,
    working_copy_operation: Option<Vec<u8>>,
    fingerprint: Option<[u8; 32]>,
}
impl Entry {
    fn registration(&self) -> Registration {
        Registration {
            workspace: self.workspace,
            op_store: self.op_store,
            generation: self.generation,
        }
    }
    fn info(&self) -> WorkspaceInfo {
        WorkspaceInfo {
            registration: self.registration(),
            command_epoch: self.command_epoch,
            state: self.state,
            fingerprint: self.fingerprint,
        }
    }
    fn checkpoint(&self) -> Result<RootCheckpoint> {
        Ok(RootCheckpoint {
            roots: self
                .roots
                .iter()
                .map(|root| {
                    Ok(ObjectKey {
                        kind: Kind::try_from(root.kind).map_err(|_| Error::Corrupt("root kind"))?,
                        id: tinytree_model::ObjectId(root.id),
                    })
                })
                .collect::<Result<_>>()?,
            op_heads: self.op_heads.clone(),
            working_copy_operation: self.working_copy_operation.clone(),
        })
    }
}
pub(crate) struct Data {
    pub manifest: Manifest,
    entries: HashMap<WorkspaceId, Entry>,
    logical_bytes: usize,
    row_digests: BTreeMap<WorkspaceId, [u8; 32]>,
    captures: BTreeMap<CaptureId, StoredCapture>,
    capture_digests: BTreeMap<CaptureId, [u8; 32]>,
}
impl Data {
    pub fn get(&self, registration: Registration) -> Result<&Entry> {
        let entry = self
            .entries
            .get(&registration.workspace)
            .ok_or(Error::UnknownWorkspace)?;
        if entry.op_store != registration.op_store {
            return Err(Error::IdentityMismatch);
        }
        if entry.generation != registration.generation {
            return Err(Error::StaleRegistration);
        }
        Ok(entry)
    }
    fn next(&self) -> Result<u64> {
        self.manifest
            .epoch
            .checked_add(1)
            .ok_or(Error::Limit("registry epoch"))
    }
}
pub(crate) struct Inner {
    pub id: RegistryId,
    pub limits: Limits,
    pub fence: Arc<RwLock<()>>,
    pub runtime: Mutex<super::Runtime>,
    pub data: Mutex<Data>,
    poisoned: AtomicBool,
    pub transactions: AtomicU64,
    pub encoded_write_bytes: AtomicU64,
    db: SingleWriterTxDatabase,
    meta: SingleWriterTxKeyspace,
    workspaces: SingleWriterTxKeyspace,
    captures: SingleWriterTxKeyspace,
    _owner: File,
}
impl Inner {
    pub fn check(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    pub fn open(
        path: &Path,
        id: RegistryId,
        tracking: Option<Tracking>,
        limits: Limits,
    ) -> Result<Self> {
        limits.validate()?;
        let (root, fresh) = preflight(path)?;
        if tracking.is_some() && !fresh {
            return Err(Error::Invalid("initialization requires a fresh registry"));
        }
        if tracking.is_none() && fresh {
            return Err(Error::Invalid("existing registry required"));
        }
        if fresh {
            create_dirs(&root)?;
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        if fresh {
            options.create_new(true);
        }
        let owner = options
            .open(root.join("owner.lock"))
            .map_err(|_| Error::Storage)?;
        owner.try_lock_exclusive().map_err(|_| Error::Busy)?;
        if fresh {
            for entry in fs::read_dir(&root).map_err(|_| Error::Storage)? {
                if entry.map_err(|_| Error::Storage)?.file_name() != "owner.lock" {
                    return Err(Error::Invalid(
                        "registry root changed during initialization",
                    ));
                }
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
                    .map_err(|_| Error::Storage)?;
            }
            owner.sync_all().map_err(|_| Error::Storage)?;
            sync_dir(&root)?;
        }
        let db = SingleWriterTxDatabase::builder(root.join("metadata"))
            .manual_journal_persist(true)
            .open()
            .map_err(|_| Error::Storage)?;
        if !fresh && (!db.keyspace_exists("meta") || !db.keyspace_exists("workspaces")) {
            return Err(Error::Corrupt("required keyspace missing"));
        }
        let meta = db
            .keyspace("meta", KeyspaceCreateOptions::default)
            .map_err(|_| Error::Storage)?;
        let workspaces = db
            .keyspace("workspaces", KeyspaceCreateOptions::default)
            .map_err(|_| Error::Storage)?;
        if fresh {
            let manifest = Manifest {
                version: 2,
                id,
                epoch: 0,
                untracked: tracking == Some(Tracking::Untracked),
                workspace_count: 0,
                workspace_commitment: inventory_commitment(&BTreeMap::new()),
                captures: None,
            };
            let bytes = encode(&manifest)?;
            if bytes.len() + MANIFEST.len() > limits.max_ledger_bytes {
                return Err(Error::Limit("ledger bytes"));
            }
            let mut tx = db.write_tx().durability(Some(PersistMode::SyncAll));
            tx.insert(&meta, MANIFEST, bytes);
            tx.commit().map_err(|_| Error::Storage)?;
            sync_dir(&root)?;
            let mut marker = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(root.join(MARKER))
                .map_err(|_| Error::Storage)?;
            marker.write_all(FORMAT).map_err(|_| Error::Storage)?;
            marker.sync_all().map_err(|_| Error::Storage)?;
            sync_dir(&root)?;
        }
        let view = db.read_tx();
        let mut manifest = None;
        let mut logical = 0usize;
        for item in view.iter(&meta) {
            let (key, value) = item.into_inner().map_err(|_| Error::Storage)?;
            charge(
                &mut logical,
                key.len() + value.len(),
                limits.max_ledger_bytes,
            )?;
            if &*key != MANIFEST || manifest.is_some() || value.len() > 4096 {
                return Err(Error::Corrupt("unexpected registry metadata"));
            }
            manifest = Some(decode::<Manifest>(&value)?);
        }
        let manifest = manifest.ok_or(Error::Corrupt("missing manifest"))?;
        if manifest.version != 2 {
            return Err(Error::Corrupt("unsupported format"));
        }
        if manifest.id != id {
            return Err(Error::IdentityMismatch);
        }
        if manifest.captures.is_some() && !db.keyspace_exists("captures") {
            return Err(Error::Corrupt("capture inventory missing"));
        }
        let captures_space = db
            .keyspace("captures", KeyspaceCreateOptions::default)
            .map_err(|_| Error::Storage)?;
        let mut captures = BTreeMap::new();
        let mut capture_digests = BTreeMap::new();
        for item in view.iter(&captures_space) {
            if captures.len() >= limits.max_pending_captures {
                return Err(Error::Limit("pending captures"));
            }
            let (key, value) = item.into_inner().map_err(|_| Error::Storage)?;
            charge(
                &mut logical,
                key.len().saturating_add(value.len()),
                limits.max_ledger_bytes,
            )?;
            if value.len() > record_cap(&limits) {
                return Err(Error::Limit("capture record bytes"));
            }
            let capture = decode::<StoredCapture>(&value)?;
            validate_capture(&capture, id, manifest.epoch, &limits)?;
            if &*key != capture.id.0 {
                return Err(Error::Corrupt("capture key identity"));
            }
            capture_digests.insert(capture.id, *blake3::hash(&value).as_bytes());
            if captures.insert(capture.id, capture).is_some() {
                return Err(Error::Corrupt("duplicate capture"));
            }
        }
        match &manifest.captures {
            None if captures.is_empty() => {}
            Some(inventory)
                if inventory.version == 1
                    && inventory.count == captures.len() as u64
                    && inventory.commitment == capture_commitment(&capture_digests) => {}
            _ => return Err(Error::Corrupt("capture inventory commitment")),
        }
        capture_limits(captures.values(), &limits)?;
        let mut entries = HashMap::new();
        let mut row_digests = BTreeMap::new();
        let mut generations = HashSet::new();
        let mut total_roots = 0usize;
        let mut total_root_bytes = 0usize;
        for item in view.iter(&workspaces) {
            if entries.len() >= limits.max_workspaces {
                return Err(Error::Limit("workspace records"));
            }
            let (key, value) = item.into_inner().map_err(|_| Error::Storage)?;
            charge(
                &mut logical,
                key.len() + value.len(),
                limits.max_ledger_bytes,
            )?;
            // Reject an oversized record before allocating its decoded vectors.
            // JSON byte arrays use at most four bytes per scalar plus separators.
            let entry_cap = limits
                .max_roots_per_workspace
                .saturating_mul(192)
                .saturating_add(
                    limits
                        .max_op_heads_per_workspace
                        .saturating_add(1)
                        .saturating_mul(
                            limits
                                .max_operation_id_bytes
                                .saturating_mul(4)
                                .saturating_add(8),
                        ),
                )
                .saturating_add(4096)
                .min(limits.max_ledger_bytes);
            if value.len() > entry_cap {
                return Err(Error::Limit("workspace record bytes"));
            }
            let entry = decode::<Entry>(&value)?;
            if &*key != entry.workspace.0
                || entry.generation == 0
                || entry.generation > manifest.epoch
                || entry.command_epoch > manifest.epoch
                || !generations.insert(entry.generation)
            {
                return Err(Error::Corrupt("workspace identity or epoch"));
            }
            validate_entry(&entry, &limits)?;
            let cp = entry.checkpoint()?;
            total_roots = total_roots
                .checked_add(cp.roots.len())
                .ok_or(Error::Limit("total roots"))?;
            charge(
                &mut total_root_bytes,
                root_bytes(&cp)?,
                limits.max_total_root_bytes,
            )?;
            if total_roots > limits.max_total_roots {
                return Err(Error::Limit("total roots"));
            }
            row_digests.insert(entry.workspace, *blake3::hash(&value).as_bytes());
            if entries.insert(entry.workspace, entry).is_some() {
                return Err(Error::Corrupt("duplicate workspace"));
            }
        }
        if manifest.workspace_count != entries.len() as u64
            || manifest.workspace_commitment != inventory_commitment(&row_digests)
        {
            return Err(Error::Corrupt("workspace inventory commitment"));
        }
        drop(view);
        Ok(Self {
            id,
            limits,
            fence: Arc::new(RwLock::new(())),
            runtime: Mutex::new(super::Runtime::default()),
            data: Mutex::new(Data {
                manifest,
                entries,
                logical_bytes: logical,
                row_digests,
                captures,
                capture_digests,
            }),
            poisoned: AtomicBool::new(false),
            transactions: AtomicU64::new(0),
            encoded_write_bytes: AtomicU64::new(0),
            db,
            meta,
            workspaces,
            captures: captures_space,
            _owner: owner,
        })
    }
    pub fn register(&self, workspace: WorkspaceId, op_store: OpStoreId) -> Result<Registration> {
        self.check()?;
        let mut data = self.data.lock().map_err(|_| Error::Poisoned)?;
        if let Some(entry) = data.entries.get(&workspace) {
            if entry.op_store != op_store {
                return Err(Error::IdentityMismatch);
            }
            if entry.state == WorkspaceState::Forgotten {
                return Err(Error::Forgotten);
            }
            return Ok(entry.registration());
        }
        if data.entries.len() >= self.limits.max_workspaces {
            return Err(Error::Limit("workspace records"));
        }
        let entry = Entry {
            workspace,
            op_store,
            generation: data.next()?,
            previous_generation: None,
            command_epoch: 0,
            state: WorkspaceState::Dirty,
            checkpoint_epoch: None,
            roots: vec![],
            op_heads: vec![],
            working_copy_operation: None,
            fingerprint: None,
        };
        let registration = entry.registration();
        self.persist(&mut data, entry, None)?;
        Ok(registration)
    }
    pub fn info(&self, registration: Registration) -> Result<WorkspaceInfo> {
        self.check()?;
        Ok(self
            .data
            .lock()
            .map_err(|_| Error::Poisoned)?
            .get(registration)?
            .info())
    }
    pub fn persist(&self, data: &mut Data, entry: Entry, untracked: Option<bool>) -> Result<u64> {
        self.persist_with_capture(data, entry, untracked, None)
    }
    fn persist_with_capture(
        &self,
        data: &mut Data,
        entry: Entry,
        untracked: Option<bool>,
        capture: Option<StoredCapture>,
    ) -> Result<u64> {
        self.check()?;
        validate_entry(&entry, &self.limits)?;
        let mut manifest = data.manifest.clone();
        manifest.epoch = data.next()?;
        if let Some(untracked) = untracked {
            manifest.untracked |= untracked;
        }
        let bytes = encode(&entry)?;
        // Cache hashes of complete encoded records, including tombstones. Updating
        // the small sorted inventory never re-encodes other root payloads.
        let mut row_digests = data.row_digests.clone();
        row_digests.insert(entry.workspace, *blake3::hash(&bytes).as_bytes());
        manifest.workspace_count = row_digests.len() as u64;
        manifest.workspace_commitment = inventory_commitment(&row_digests);
        let mut capture_digests = data.capture_digests.clone();
        let capture_bytes = if let Some(capture) = &capture {
            if data.captures.contains_key(&capture.id) {
                return Err(Error::CheckpointConflict);
            }
            validate_capture(capture, self.id, manifest.epoch, &self.limits)?;
            capture_limits(
                data.captures.values().chain(std::iter::once(capture)),
                &self.limits,
            )?;
            let bytes = encode(capture)?;
            capture_digests.insert(capture.id, *blake3::hash(&bytes).as_bytes());
            manifest.captures = Some(CaptureInventory {
                version: 1,
                count: capture_digests.len() as u64,
                commitment: capture_commitment(&capture_digests),
            });
            Some(bytes)
        } else {
            None
        };
        let manifest_bytes = encode(&manifest)?;
        let old_manifest = encode(&data.manifest)?.len() + MANIFEST.len();
        let old_entry = data
            .entries
            .get(&entry.workspace)
            .map(encode)
            .transpose()?
            .map_or(0, |bytes| bytes.len() + 16);
        let logical = data
            .logical_bytes
            .checked_sub(old_manifest + old_entry)
            .and_then(|n| n.checked_add(manifest_bytes.len() + MANIFEST.len() + bytes.len() + 16))
            .ok_or(Error::Limit("ledger bytes"))?;
        let logical = logical
            .checked_add(
                capture_bytes
                    .as_ref()
                    .map_or(0, |bytes| bytes.len().saturating_add(32)),
            )
            .ok_or(Error::Limit("ledger bytes"))?;
        if logical > self.limits.max_ledger_bytes {
            return Err(Error::Limit("ledger bytes"));
        }
        let mut root_count = entry.roots.len();
        let mut root_size = root_bytes(&entry.checkpoint()?)?;
        for old in data
            .entries
            .values()
            .filter(|old| old.workspace != entry.workspace)
        {
            root_count = root_count
                .checked_add(old.roots.len())
                .ok_or(Error::Limit("total roots"))?;
            charge(
                &mut root_size,
                root_bytes(&old.checkpoint()?)?,
                self.limits.max_total_root_bytes,
            )?;
        }
        if root_count > self.limits.max_total_roots || root_size > self.limits.max_total_root_bytes
        {
            return Err(Error::Limit("total roots"));
        }
        let written = manifest_bytes.len()
            + MANIFEST.len()
            + bytes.len()
            + 16
            + capture_bytes.as_ref().map_or(0, |bytes| bytes.len() + 32);
        let mut tx = self.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&self.meta, MANIFEST, manifest_bytes);
        tx.insert(&self.workspaces, entry.workspace.0, bytes);
        if let (Some(capture), Some(bytes)) = (&capture, capture_bytes) {
            tx.insert(&self.captures, capture.id.0, bytes);
        }
        if tx.commit().is_err() {
            self.poisoned.store(true, Ordering::Release);
            return Err(Error::Storage);
        }
        self.transactions.fetch_add(1, Ordering::Relaxed);
        self.encoded_write_bytes
            .fetch_add(written as u64, Ordering::Relaxed);
        data.logical_bytes = logical;
        data.manifest = manifest;
        data.entries.insert(entry.workspace, entry);
        data.row_digests = row_digests;
        data.capture_digests = capture_digests;
        if let Some(capture) = capture {
            data.captures.insert(capture.id, capture);
        }
        Ok(data.manifest.epoch)
    }
    pub fn checkpoint(
        &self,
        registration: Registration,
        epoch: u64,
        checkpoint: RootCheckpoint,
        fingerprint: [u8; 32],
    ) -> Result<CheckpointReceipt> {
        self.check()?;
        let mut data = self.data.lock().map_err(|_| Error::Poisoned)?;
        let mut entry = data.get(registration)?.clone();
        if entry.command_epoch != epoch || entry.state == WorkspaceState::Forgotten {
            return Err(Error::ClosedCommand);
        }
        entry.state = WorkspaceState::Clean;
        entry.checkpoint_epoch = Some(epoch);
        entry.roots = checkpoint
            .roots
            .iter()
            .map(|root| StoredRoot {
                kind: root.kind as u8,
                id: root.id.0,
            })
            .collect();
        entry.op_heads = checkpoint.op_heads;
        entry.working_copy_operation = checkpoint.working_copy_operation;
        entry.fingerprint = Some(fingerprint);
        let registry_epoch = self.persist(&mut data, entry, None)?;
        Ok(CheckpointReceipt {
            registration,
            command_epoch: epoch,
            registry_epoch,
            fingerprint,
        })
    }
    pub fn capture_dirty(
        &self,
        registration: Registration,
        epoch: u64,
        checkpoint: RootCheckpoint,
        bundle: BundleDescriptor,
    ) -> Result<PendingCapture> {
        if bundle.size_bytes == 0 {
            return Err(Error::Invalid("capture bundle size"));
        }
        if bundle.size_bytes > self.limits.max_capture_bundle_bytes {
            return Err(Error::Limit("capture bundle bytes"));
        }
        self.check()?;
        let mut data = self.data.lock().map_err(|_| Error::Poisoned)?;
        let mut entry = data.get(registration)?.clone();
        if entry.command_epoch != epoch || entry.state != WorkspaceState::Dirty {
            return Err(Error::ClosedCommand);
        }
        entry.checkpoint_epoch = Some(epoch);
        entry.fingerprint = Some(fingerprint(&registration, epoch, &checkpoint));
        entry.roots = checkpoint
            .roots
            .iter()
            .map(|root| StoredRoot {
                kind: root.kind as u8,
                id: root.id.0,
            })
            .collect();
        entry.op_heads = checkpoint.op_heads;
        entry.working_copy_operation = checkpoint.working_copy_operation;
        let id = capture_id(
            self.id,
            registration,
            epoch,
            entry.fingerprint.unwrap(),
            bundle,
        );
        let capture = StoredCapture {
            version: 1,
            id,
            registry: self.id,
            registry_epoch: data.next()?,
            bundle,
            checkpoint: entry.clone(),
        };
        let pending = capture.pending()?;
        self.persist_with_capture(&mut data, entry, None, Some(capture))?;
        Ok(pending)
    }
    pub fn read_capture(&self, id: CaptureId) -> Result<PendingCapture> {
        self.check()?;
        let data = self.data.lock().map_err(|_| Error::Poisoned)?;
        let capture = data.captures.get(&id).ok_or(Error::UnknownCapture)?;
        // Validate the selected already-loaded row only. An old pending capture
        // deliberately does not have to match the workspace's current epoch.
        if capture.id != id {
            return Err(Error::Corrupt("capture lookup identity"));
        }
        validate_capture(capture, self.id, data.manifest.epoch, &self.limits)?;
        capture.pending()
    }
    pub fn pending_captures(&self) -> Result<Vec<PendingCapture>> {
        self.check()?;
        let data = self.data.lock().map_err(|_| Error::Poisoned)?;
        data.captures.values().map(StoredCapture::pending).collect()
    }
    pub fn forget(&self, registration: Registration) -> Result<Registration> {
        self.check()?;
        let mut data = self.data.lock().map_err(|_| Error::Poisoned)?;
        let old = data
            .entries
            .get(&registration.workspace)
            .ok_or(Error::UnknownWorkspace)?;
        if old.op_store != registration.op_store {
            return Err(Error::IdentityMismatch);
        }
        if old.state == WorkspaceState::Forgotten {
            if old.generation == registration.generation
                || old.previous_generation == Some(registration.generation)
            {
                return Ok(old.registration());
            }
            return Err(Error::StaleRegistration);
        }
        if old.generation != registration.generation {
            return Err(Error::StaleRegistration);
        }
        let mut entry = old.clone();
        entry.previous_generation = Some(entry.generation);
        entry.generation = data.next()?;
        entry.state = WorkspaceState::Forgotten;
        entry.roots.clear();
        entry.op_heads.clear();
        entry.working_copy_operation = None;
        entry.fingerprint = None;
        entry.checkpoint_epoch = None;
        let result = entry.registration();
        self.persist(&mut data, entry, None)?;
        Ok(result)
    }
    pub fn pin_untracked(&self) -> Result<()> {
        self.check()?;
        let mut data = self.data.lock().map_err(|_| Error::Poisoned)?;
        if data.manifest.untracked {
            return Ok(());
        }
        let mut manifest = data.manifest.clone();
        manifest.epoch = data.next()?;
        manifest.untracked = true;
        let bytes = encode(&manifest)?;
        let logical = data.logical_bytes - encode(&data.manifest)?.len() + bytes.len();
        if logical > self.limits.max_ledger_bytes {
            return Err(Error::Limit("ledger bytes"));
        }
        let written = bytes.len() + MANIFEST.len();
        let mut tx = self.db.write_tx().durability(Some(PersistMode::SyncAll));
        tx.insert(&self.meta, MANIFEST, bytes);
        if tx.commit().is_err() {
            self.poisoned.store(true, Ordering::Release);
            return Err(Error::Storage);
        }
        self.transactions.fetch_add(1, Ordering::Relaxed);
        self.encoded_write_bytes
            .fetch_add(written as u64, Ordering::Relaxed);
        data.manifest = manifest;
        data.logical_bytes = logical;
        Ok(())
    }
    pub fn collector_roots(&self) -> Result<Vec<ObjectKey>> {
        self.check()?;
        let data = self.data.lock().map_err(|_| Error::Poisoned)?;
        if data.manifest.untracked {
            return Err(Error::UntrackedAccess);
        }
        let mut roots = HashSet::new();
        roots.insert(tinytree_model::empty_tree().descriptor.key);
        for entry in data.entries.values() {
            if entry.state == WorkspaceState::Dirty {
                return Err(Error::DirtyWorkspace);
            }
            if entry.state == WorkspaceState::Clean {
                for root in entry.checkpoint()?.roots {
                    roots.insert(root);
                    if roots.len() > self.limits.max_total_roots.saturating_add(1) {
                        return Err(Error::Limit("collector roots"));
                    }
                }
            }
        }
        for capture in data.captures.values() {
            for root in capture.checkpoint.checkpoint()?.roots {
                roots.insert(root);
            }
            if roots.len()
                > self
                    .limits
                    .max_total_roots
                    .saturating_add(self.limits.max_capture_roots)
                    .saturating_add(1)
            {
                return Err(Error::Limit("collector roots"));
            }
        }
        let mut roots: Vec<_> = roots.into_iter().collect();
        roots.sort_by_key(|root| (root.kind as u8, root.id.0));
        Ok(roots)
    }
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| Error::Corrupt("record encoding"))
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::Corrupt("record encoding"))
}
fn validate_entry(entry: &Entry, limits: &Limits) -> Result<()> {
    let cp = entry.checkpoint()?;
    let canonical = canonical(cp.clone(), limits)?;
    if canonical != cp {
        return Err(Error::Corrupt("checkpoint ordering or duplicate roots"));
    }
    match entry.state {
        WorkspaceState::Clean => {
            if entry.command_epoch == 0
                || entry.checkpoint_epoch != Some(entry.command_epoch)
                || entry.fingerprint
                    != Some(fingerprint(&entry.registration(), entry.command_epoch, &cp))
                || entry.previous_generation.is_some()
            {
                return Err(Error::Corrupt("clean checkpoint identity"));
            }
        }
        WorkspaceState::Dirty if entry.previous_generation.is_some() => {
            return Err(Error::Corrupt("dirty registration generation"));
        }
        WorkspaceState::Dirty => match (entry.checkpoint_epoch, entry.fingerprint) {
            (None, None)
                if cp.roots.is_empty()
                    && cp.op_heads.is_empty()
                    && cp.working_copy_operation.is_none() => {}
            (Some(epoch), Some(hash))
                if epoch > 0
                    && epoch <= entry.command_epoch
                    && hash == fingerprint(&entry.registration(), epoch, &cp) => {}
            _ => return Err(Error::Corrupt("dirty checkpoint identity")),
        },
        WorkspaceState::Forgotten => {
            if entry
                .previous_generation
                .is_none_or(|old| old == 0 || old >= entry.generation)
                || entry.checkpoint_epoch.is_some()
                || entry.fingerprint.is_some()
                || !cp.roots.is_empty()
                || !cp.op_heads.is_empty()
                || cp.working_copy_operation.is_some()
            {
                return Err(Error::Corrupt("forgotten workspace record"));
            }
        }
    }
    Ok(())
}
fn record_cap(limits: &Limits) -> usize {
    limits
        .max_roots_per_workspace
        .saturating_mul(192)
        .saturating_add(
            limits
                .max_op_heads_per_workspace
                .saturating_add(1)
                .saturating_mul(
                    limits
                        .max_operation_id_bytes
                        .saturating_mul(4)
                        .saturating_add(8),
                ),
        )
        .saturating_add(8192)
        .min(limits.max_ledger_bytes)
}
fn capture_id(
    registry: RegistryId,
    registration: Registration,
    epoch: u64,
    fingerprint: [u8; 32],
    bundle: BundleDescriptor,
) -> CaptureId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"native-retention-capture-v1\0");
    hash.update(&registry.0);
    hash.update(&registration.workspace.0);
    hash.update(&registration.op_store.0);
    hash.update(&registration.generation.to_be_bytes());
    hash.update(&epoch.to_be_bytes());
    hash.update(&fingerprint);
    hash.update(&bundle.blake3);
    hash.update(&bundle.size_bytes.to_be_bytes());
    CaptureId(*hash.finalize().as_bytes())
}
fn validate_capture(
    capture: &StoredCapture,
    registry: RegistryId,
    epoch: u64,
    limits: &Limits,
) -> Result<()> {
    let entry = &capture.checkpoint;
    validate_entry(entry, limits)?;
    if capture.version != 1
        || capture.registry != registry
        || capture.registry_epoch == 0
        || capture.registry_epoch > epoch
        || entry.state != WorkspaceState::Dirty
        || entry.command_epoch == 0
        || entry.command_epoch > capture.registry_epoch
        || entry.generation == 0
        || entry.generation > capture.registry_epoch
        || entry.checkpoint_epoch != Some(entry.command_epoch)
        || capture.bundle.size_bytes == 0
        || capture.id
            != capture_id(
                registry,
                entry.registration(),
                entry.command_epoch,
                entry
                    .fingerprint
                    .ok_or(Error::Corrupt("capture fingerprint"))?,
                capture.bundle,
            )
    {
        return Err(Error::Corrupt("capture identity"));
    }
    Ok(())
}
fn capture_limits<'a>(
    captures: impl Iterator<Item = &'a StoredCapture>,
    limits: &Limits,
) -> Result<()> {
    let (mut count, mut roots, mut bytes, mut bundles) = (0usize, 0usize, 0usize, 0u64);
    for capture in captures {
        count = count
            .checked_add(1)
            .ok_or(Error::Limit("pending captures"))?;
        roots = roots
            .checked_add(capture.checkpoint.roots.len())
            .ok_or(Error::Limit("capture roots"))?;
        bytes = bytes
            .checked_add(root_bytes(&capture.checkpoint.checkpoint()?)?)
            .ok_or(Error::Limit("capture root bytes"))?;
        bundles = bundles
            .checked_add(capture.bundle.size_bytes)
            .ok_or(Error::Limit("capture bundle bytes"))?;
        if count > limits.max_pending_captures
            || roots > limits.max_capture_roots
            || bytes > limits.max_capture_root_bytes
            || bundles > limits.max_capture_bundle_bytes
        {
            return Err(Error::Limit("capture inventory"));
        }
    }
    Ok(())
}
fn capture_commitment(rows: &BTreeMap<CaptureId, [u8; 32]>) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"native-retention-capture-inventory-v1\0");
    hash.update(&(rows.len() as u64).to_be_bytes());
    for (id, digest) in rows {
        hash.update(&id.0);
        hash.update(digest);
    }
    *hash.finalize().as_bytes()
}
pub(crate) fn canonical(mut checkpoint: RootCheckpoint, limits: &Limits) -> Result<RootCheckpoint> {
    if checkpoint.roots.len() > limits.max_roots_per_workspace {
        return Err(Error::Limit("workspace roots"));
    }
    if checkpoint.op_heads.len() > limits.max_op_heads_per_workspace {
        return Err(Error::Limit("operation heads"));
    }
    for operation in checkpoint
        .op_heads
        .iter()
        .chain(checkpoint.working_copy_operation.iter())
    {
        if operation.is_empty() || operation.len() > limits.max_operation_id_bytes {
            return Err(Error::Invalid("operation identity length"));
        }
    }
    checkpoint
        .roots
        .sort_by_key(|root| (root.kind as u8, root.id.0));
    checkpoint.roots.dedup();
    checkpoint.op_heads.sort();
    checkpoint.op_heads.dedup();
    if root_bytes(&checkpoint)? > limits.max_root_bytes_per_workspace {
        return Err(Error::Limit("workspace root bytes"));
    }
    Ok(checkpoint)
}
fn root_bytes(checkpoint: &RootCheckpoint) -> Result<usize> {
    let mut bytes = checkpoint
        .roots
        .len()
        .checked_mul(33)
        .ok_or(Error::Limit("root bytes"))?;
    for operation in checkpoint
        .op_heads
        .iter()
        .chain(checkpoint.working_copy_operation.iter())
    {
        bytes = bytes
            .checked_add(4 + operation.len())
            .ok_or(Error::Limit("root bytes"))?;
    }
    Ok(bytes)
}
pub(crate) fn fingerprint(
    registration: &Registration,
    epoch: u64,
    checkpoint: &RootCheckpoint,
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"native-retention-checkpoint-v1\0");
    hash.update(&registration.workspace.0);
    hash.update(&registration.op_store.0);
    hash.update(&registration.generation.to_be_bytes());
    hash.update(&epoch.to_be_bytes());
    hash.update(&(checkpoint.roots.len() as u64).to_be_bytes());
    for root in &checkpoint.roots {
        hash.update(&[root.kind as u8]);
        hash.update(&root.id.0);
    }
    hash.update(&(checkpoint.op_heads.len() as u64).to_be_bytes());
    for operation in &checkpoint.op_heads {
        hash.update(&(operation.len() as u64).to_be_bytes());
        hash.update(operation);
    }
    if let Some(operation) = &checkpoint.working_copy_operation {
        hash.update(&[1]);
        hash.update(&(operation.len() as u64).to_be_bytes());
        hash.update(operation);
    } else {
        hash.update(&[0]);
    }
    *hash.finalize().as_bytes()
}
fn inventory_commitment(rows: &BTreeMap<WorkspaceId, [u8; 32]>) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"native-retention-workspace-inventory-v1\0");
    hash.update(&(rows.len() as u64).to_be_bytes());
    for (workspace, digest) in rows {
        hash.update(&workspace.0);
        hash.update(digest);
    }
    *hash.finalize().as_bytes()
}
fn charge(total: &mut usize, bytes: usize, limit: usize) -> Result<()> {
    *total = total
        .checked_add(bytes)
        .ok_or(Error::Limit("ledger bytes"))?;
    if *total > limit {
        return Err(Error::Limit("ledger bytes"));
    }
    Ok(())
}
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| Error::Storage)
}
fn create_dirs(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut cursor = path;
    while !cursor.try_exists().map_err(|_| Error::Storage)? {
        missing.push(cursor.to_owned());
        cursor = cursor.parent().ok_or(Error::Invalid("registry ancestor"))?;
    }
    for path in missing.iter().rev() {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(path).map_err(|_| Error::Storage)?;
        sync_dir(path.parent().ok_or(Error::Invalid("registry ancestor"))?)?;
        sync_dir(path)?;
    }
    if missing.is_empty() {
        sync_dir(path)?;
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }
    }
    Ok(())
}
fn preflight(path: &Path) -> Result<(PathBuf, bool)> {
    use std::path::Component;
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(Error::Invalid("registry root path"));
    }
    let lexical: PathBuf = path
        .components()
        .filter(|part| !matches!(part, Component::CurDir))
        .collect();
    let absolute = if lexical.is_absolute() {
        lexical
    } else {
        std::env::current_dir()
            .map_err(|_| Error::Storage)?
            .join(lexical)
    };
    match fs::symlink_metadata(&absolute) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(Error::Invalid("registry root must not be a symlink"));
            }
            let root = absolute.canonicalize().map_err(|_| Error::Storage)?;
            if fs::read_dir(&root)
                .map_err(|_| Error::Storage)?
                .next()
                .is_none()
            {
                return Ok((root, true));
            }
            for file in [MARKER, "owner.lock", "metadata/version"] {
                let m = fs::symlink_metadata(root.join(file))
                    .map_err(|_| Error::Corrupt("registry ownership marker missing"))?;
                if !m.is_file() || m.len() > 4096 {
                    return Err(Error::Corrupt("registry ownership marker type or size"));
                }
            }
            if fs::read(root.join(MARKER)).map_err(|_| Error::Storage)? != FORMAT
                || fs::read(root.join("metadata/version")).map_err(|_| Error::Storage)?
                    != b"FJL\x03"
            {
                return Err(Error::Corrupt("registry ownership format"));
            }
            Ok((root, false))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut ancestor = absolute.as_path();
            let mut tail = Vec::new();
            loop {
                match fs::metadata(ancestor) {
                    Ok(metadata) => {
                        if !metadata.is_dir() {
                            return Err(Error::Invalid("registry ancestor"));
                        }
                        let mut root = ancestor.canonicalize().map_err(|_| Error::Storage)?;
                        for part in tail.iter().rev() {
                            root.push(part);
                        }
                        return Ok((root, true));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        tail.push(
                            ancestor
                                .file_name()
                                .ok_or(Error::Invalid("registry ancestor"))?
                                .to_owned(),
                        );
                        ancestor = ancestor
                            .parent()
                            .ok_or(Error::Invalid("registry ancestor"))?;
                    }
                    Err(_) => return Err(Error::Storage),
                }
            }
        }
        Err(_) => Err(Error::Storage),
    }
}
