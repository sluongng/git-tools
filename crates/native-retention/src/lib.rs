//! Durable root claims and a command/collector fence. This crate does not walk
//! object closure, delete objects, expire registrations or perform networking.
mod storage;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt,
    path::Path,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tinytree_model::{Kind, ObjectKey};
use tokio::sync::{Notify, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid(&'static str),
    Corrupt(&'static str),
    Limit(&'static str),
    Storage,
    Busy,
    UnknownWorkspace,
    Forgotten,
    IdentityMismatch,
    StaleRegistration,
    ClosedCommand,
    CheckpointConflict,
    DirtyWorkspace,
    UntrackedAccess,
    Poisoned,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(why) => write!(f, "invalid native retention request: {why}"),
            Self::Corrupt(why) => write!(f, "native retention ledger is corrupt: {why}"),
            Self::Limit(why) => write!(f, "native retention limit exceeded: {why}"),
            other => write!(
                f,
                "native retention: {}",
                match other {
                    Self::Storage => "storage operation failed",
                    Self::Busy => "workspace command or draining work is active",
                    Self::UnknownWorkspace => "workspace is unregistered",
                    Self::Forgotten => "workspace is explicitly forgotten",
                    Self::IdentityMismatch => "identity does not match",
                    Self::StaleRegistration => "registration generation changed",
                    Self::ClosedCommand => "command token is closed or unknown",
                    Self::CheckpointConflict => "checkpoint differs within the same command epoch",
                    Self::DirtyWorkspace => "dirty workspace prevents collection",
                    Self::UntrackedAccess => "untracked access prevents collection",
                    Self::Poisoned => "registry requires reopening",
                    _ => unreachable!(),
                }
            ),
        }
    }
}
impl std::error::Error for Error {}
macro_rules! identity {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        pub struct $name(pub [u8; 16]);
        impl $name {
            pub fn random() -> Self {
                Self(*uuid::Uuid::new_v4().as_bytes())
            }
        }
    };
}
identity!(RegistryId);
identity!(WorkspaceId);
identity!(OpStoreId);
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommandToken([u8; 32]);
impl fmt::Debug for CommandToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CommandToken(..)")
    }
}
impl CommandToken {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    fn random() -> Self {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        Self(bytes)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tracking {
    ManagedFresh,
    Untracked,
}
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_workspaces: usize,
    pub max_roots_per_workspace: usize,
    pub max_total_roots: usize,
    pub max_op_heads_per_workspace: usize,
    pub max_operation_id_bytes: usize,
    pub max_root_bytes_per_workspace: usize,
    pub max_total_root_bytes: usize,
    pub max_ledger_bytes: usize,
    pub max_jobs_per_command: usize,
    pub max_pending_captures: usize,
    pub max_capture_roots: usize,
    pub max_capture_root_bytes: usize,
    /// Aggregate referenced immutable bundle bytes, not owned disk allocation.
    pub max_capture_bundle_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_workspaces: 4096,
            max_roots_per_workspace: 100_000,
            max_total_roots: 1_000_000,
            max_op_heads_per_workspace: 4096,
            max_operation_id_bytes: 128,
            max_root_bytes_per_workspace: 16 * 1024 * 1024,
            max_total_root_bytes: 64 * 1024 * 1024,
            max_ledger_bytes: 256 * 1024 * 1024,
            max_jobs_per_command: 256,
            max_pending_captures: 1024,
            max_capture_roots: 1_000_000,
            max_capture_root_bytes: 64 * 1024 * 1024,
            max_capture_bundle_bytes: 16 * 1024 * 1024 * 1024,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        if self.max_workspaces == 0
            || self.max_workspaces > 100_000
            || self.max_roots_per_workspace == 0
            || self.max_roots_per_workspace > 1_000_000
            || self.max_total_roots == 0
            || self.max_total_roots > 4_000_000
            || self.max_op_heads_per_workspace == 0
            || self.max_op_heads_per_workspace > 100_000
            || self.max_operation_id_bytes == 0
            || self.max_operation_id_bytes > 128
            || self.max_root_bytes_per_workspace == 0
            || self.max_total_root_bytes == 0
            || self.max_ledger_bytes == 0
            || self.max_ledger_bytes > 1024 * 1024 * 1024
            || self.max_jobs_per_command == 0
            || self.max_jobs_per_command > 100_000
            || self.max_pending_captures == 0
            || self.max_pending_captures > 100_000
            || self.max_capture_roots == 0
            || self.max_capture_roots > 4_000_000
            || self.max_capture_root_bytes == 0
            || self.max_capture_root_bytes > 1024 * 1024 * 1024
            || self.max_capture_bundle_bytes == 0
        {
            return Err(Error::Invalid("registry limits"));
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    pub workspace: WorkspaceId,
    pub op_store: OpStoreId,
    pub generation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceState {
    Dirty,
    Clean,
    Forgotten,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceInfo {
    pub registration: Registration,
    pub command_epoch: u64,
    pub state: WorkspaceState,
    pub fingerprint: Option<[u8; 32]>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RootCheckpoint {
    pub roots: Vec<ObjectKey>,
    pub op_heads: Vec<Vec<u8>>,
    /// Actual working-copy operation, which can differ from current op heads.
    /// Diagnostic only; typed roots are the authoritative root claims.
    pub working_copy_operation: Option<Vec<u8>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointReceipt {
    pub registration: Registration,
    pub command_epoch: u64,
    pub registry_epoch: u64,
    pub fingerprint: [u8; 32],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CaptureId(pub [u8; 32]);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleDescriptor {
    pub blake3: [u8; 32],
    pub size_bytes: u64,
}
/// The caller has staged and synced this immutable bundle before returning it.
/// This descriptor does not carry a path or prove remote publication.
pub struct CaptureBundle {
    pub checkpoint: RootCheckpoint,
    pub bundle: BundleDescriptor,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingCapture {
    pub id: CaptureId,
    pub registry: RegistryId,
    pub registration: Registration,
    pub command_epoch: u64,
    pub registry_epoch: u64,
    pub fingerprint: [u8; 32],
    pub bundle: BundleDescriptor,
    pub checkpoint: RootCheckpoint,
}
/// Process-local counters since opening, excluding initialization. Encoded bytes
/// count successful transaction keys/values, not physical Fjall/WAL disk writes.
/// A concurrent snapshot can observe the two counters at different instants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageStats {
    pub transactions: u64,
    pub encoded_write_bytes: u64,
}
#[derive(Clone)]
pub struct Registry {
    inner: Arc<storage::Inner>,
}
impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry").finish_non_exhaustive()
    }
}
#[derive(Default)]
pub(crate) struct Runtime {
    sessions: HashMap<WorkspaceId, Weak<CommandContext>>,
    tokens: HashMap<CommandToken, Weak<CommandContext>>,
}
struct CommandContext {
    registry: Arc<storage::Inner>,
    registration: Registration,
    epoch: u64,
    token: CommandToken,
    _read: OwnedRwLockReadGuard<()>,
    state: Mutex<CommandState>,
    changed: Notify,
}
struct CommandState {
    owner_alive: bool,
    jobs: usize,
    phase: Phase,
}
enum Phase {
    Open,
    Capturing,
    CapturingDirty,
    CapturedDirty,
    SealingDirty,
    DrainedDirty,
    Frozen([u8; 32]),
    Complete(CheckpointReceipt),
    Failed,
}
/// One live command/session owner. Dropping it immediately closes token lookup;
/// job clones already accepted keep the collector fence and process owner alive.
pub struct CommandOwner {
    context: Arc<CommandContext>,
}
impl fmt::Debug for CommandOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandOwner")
            .field("epoch", &self.context.epoch)
            .finish_non_exhaustive()
    }
}
impl CommandOwner {
    pub fn token(&self) -> CommandToken {
        self.context.token
    }
    pub fn epoch(&self) -> u64 {
        self.context.epoch
    }
    pub fn registration(&self) -> Registration {
        self.context.registration
    }
}
impl Drop for CommandOwner {
    fn drop(&mut self) {
        if let Ok(mut state) = self.context.state.lock() {
            state.owner_alive = false;
        }
        self.context.changed.notify_waiters();
    }
}
#[derive(Clone)]
pub struct CommandJobGuard {
    lease: Arc<JobLease>,
}
impl fmt::Debug for CommandJobGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CommandJobGuard(..)")
    }
}
struct JobLease {
    context: Arc<CommandContext>,
}
impl Drop for JobLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.context.state.lock() {
            state.jobs -= 1;
        }
        self.context.changed.notify_waiters();
    }
}
impl CommandJobGuard {
    pub fn registration(&self) -> Registration {
        self.lease.context.registration
    }
    pub fn command_epoch(&self) -> u64 {
        self.lease.context.epoch
    }
}
pub struct CollectorGuard {
    registry: Arc<storage::Inner>,
    _write: OwnedRwLockWriteGuard<()>,
    roots: Vec<ObjectKey>,
    epoch: u64,
}
impl fmt::Debug for CollectorGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectorGuard")
            .field("epoch", &self.epoch)
            .field("root_count", &self.roots.len())
            .finish_non_exhaustive()
    }
}
impl CollectorGuard {
    pub fn roots(&self) -> &[ObjectKey] {
        &self.roots
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn registry_id(&self) -> RegistryId {
        self.registry.id
    }
}
impl Registry {
    /// ManagedFresh is an explicit host attestation, not discovery of legacy
    /// workspaces. Use Untracked when prior accesses cannot be inventoried.
    pub async fn init(
        path: impl AsRef<Path>,
        id: RegistryId,
        tracking: Tracking,
        limits: Limits,
    ) -> Result<Self> {
        let path = path.as_ref().to_owned();
        storage::blocking(move || {
            storage::Inner::open(&path, id, Some(tracking), limits).map(|inner| Self {
                inner: Arc::new(inner),
            })
        })
        .await
    }
    pub async fn open_existing(
        path: impl AsRef<Path>,
        id: RegistryId,
        limits: Limits,
    ) -> Result<Self> {
        let path = path.as_ref().to_owned();
        storage::blocking(move || {
            storage::Inner::open(&path, id, None, limits).map(|inner| Self {
                inner: Arc::new(inner),
            })
        })
        .await
    }
    pub fn storage_stats(&self) -> StorageStats {
        StorageStats {
            transactions: self.inner.transactions.load(Ordering::Relaxed),
            encoded_write_bytes: self.inner.encoded_write_bytes.load(Ordering::Relaxed),
        }
    }
    pub fn id(&self) -> RegistryId {
        self.inner.id
    }
    pub async fn register(
        &self,
        workspace: WorkspaceId,
        op_store: OpStoreId,
    ) -> Result<Registration> {
        self.inner.check()?;
        let read = self.inner.fence.clone().read_owned().await;
        let inner = self.inner.clone();
        storage::blocking(move || {
            let _read = read;
            inner.register(workspace, op_store)
        })
        .await
    }
    pub async fn workspace(&self, registration: Registration) -> Result<WorkspaceInfo> {
        let inner = self.inner.clone();
        storage::blocking(move || inner.info(registration)).await
    }
    pub async fn begin_command(&self, registration: Registration) -> Result<CommandOwner> {
        self.inner.check()?;
        let read = self.inner.fence.clone().read_owned().await;
        let inner = self.inner.clone();
        storage::blocking(move || {
            inner.check()?;
            let mut data = inner.data.lock().map_err(|_| Error::Poisoned)?;
            let entry = data.get(registration)?;
            if entry.state == WorkspaceState::Forgotten {
                return Err(Error::Forgotten);
            }
            {
                let mut runtime = inner.runtime.lock().map_err(|_| Error::Poisoned)?;
                runtime.sessions.retain(|_, weak| weak.strong_count() != 0);
                runtime.tokens.retain(|_, weak| weak.strong_count() != 0);
                if runtime
                    .sessions
                    .get(&registration.workspace)
                    .and_then(Weak::upgrade)
                    .is_some()
                {
                    return Err(Error::Busy);
                }
            }
            let mut entry = entry.clone();
            entry.command_epoch = entry
                .command_epoch
                .checked_add(1)
                .ok_or(Error::Limit("command epoch"))?;
            entry.state = WorkspaceState::Dirty;
            inner.persist(&mut data, entry.clone(), None)?;
            let context = Arc::new(CommandContext {
                registry: inner.clone(),
                registration,
                epoch: entry.command_epoch,
                token: CommandToken::random(),
                _read: read,
                state: Mutex::new(CommandState {
                    owner_alive: true,
                    jobs: 0,
                    phase: Phase::Open,
                }),
                changed: Notify::new(),
            });
            let mut runtime = inner.runtime.lock().map_err(|_| Error::Poisoned)?;
            runtime
                .sessions
                .insert(registration.workspace, Arc::downgrade(&context));
            runtime
                .tokens
                .insert(context.token, Arc::downgrade(&context));
            Ok(CommandOwner { context })
        })
        .await
    }
    pub fn resolve(&self, token: CommandToken) -> Result<CommandJobGuard> {
        self.inner.check()?;
        let context = self.lookup(token)?;
        let mut state = context.state.lock().map_err(|_| Error::Poisoned)?;
        if !state.owner_alive || !matches!(state.phase, Phase::Open) {
            return Err(Error::ClosedCommand);
        }
        if state.jobs >= self.inner.limits.max_jobs_per_command {
            return Err(Error::Limit("command jobs"));
        }
        state.jobs += 1;
        drop(state);
        Ok(CommandJobGuard {
            lease: Arc::new(JobLease { context }),
        })
    }
    fn lookup(&self, token: CommandToken) -> Result<Arc<CommandContext>> {
        let mut runtime = self.inner.runtime.lock().map_err(|_| Error::Poisoned)?;
        match runtime.tokens.get(&token).and_then(Weak::upgrade) {
            Some(context) => Ok(context),
            None => {
                runtime.tokens.remove(&token);
                Err(Error::ClosedCommand)
            }
        }
    }
    pub async fn checkpoint(
        &self,
        token: CommandToken,
        checkpoint: RootCheckpoint,
    ) -> Result<CheckpointReceipt> {
        self.inner.check()?;
        let context = self.lookup(token)?;
        {
            let state = context.state.lock().map_err(|_| Error::Poisoned)?;
            if !state.owner_alive {
                return Err(Error::ClosedCommand);
            }
        }
        // JoinHandle drop deliberately detaches the owned checkpoint worker.
        // It holds the context/read fence through validation, job drain and fsync.
        let worker = storage::runtime()?.spawn(checkpoint_worker(context, checkpoint));
        worker.await.map_err(|_| Error::Poisoned)?
    }
    /// Seal a command before capturing its final state. The callback starts only
    /// after all accepted job leases have drained, while the collector fence is
    /// still held. It must read the final state directly, not resolve this closed
    /// command token or enqueue work counted among its own drained jobs.
    ///
    /// Once accepted, cancellation of the caller does not cancel capture or its
    /// durable checkpoint. A failed capture leaves the workspace dirty. This
    /// captures local root claims only; it is not a remote backup receipt.
    pub async fn capture_checkpoint<C, F>(
        &self,
        token: CommandToken,
        capture: C,
    ) -> Result<CheckpointReceipt>
    where
        C: FnOnce() -> F + Send + 'static,
        F: std::future::Future<Output = Result<RootCheckpoint>> + Send + 'static,
    {
        self.inner.check()?;
        let context = self.lookup(token)?;
        // Obtain the runtime before sealing: failure must not leave an accepted
        // capture without its physical worker.
        let runtime = storage::runtime()?;
        {
            let mut state = context.state.lock().map_err(|_| Error::Poisoned)?;
            if !state.owner_alive {
                return Err(Error::ClosedCommand);
            }
            if !matches!(state.phase, Phase::Open) {
                return Err(Error::CheckpointConflict);
            }
            state.phase = Phase::Capturing;
        }
        let worker = runtime.spawn(async move {
            drain_jobs(&context).await?;
            let result = async {
                let checkpoint = capture().await?;
                let commit = context.clone();
                storage::blocking(move || {
                    let checkpoint = storage::canonical(checkpoint, &commit.registry.limits)?;
                    let fingerprint =
                        storage::fingerprint(&commit.registration, commit.epoch, &checkpoint);
                    commit.registry.checkpoint(
                        commit.registration,
                        commit.epoch,
                        checkpoint,
                        fingerprint,
                    )
                })
                .await
            }
            .await;
            finish_checkpoint(&context, &result)?;
            result
        });
        worker.await.map_err(|_| Error::Poisoned)?
    }
    /// Seal and drain, then capture an immutable staged bundle and root claims
    /// in one SyncAll transaction WITHOUT making the workspace Clean. The host
    /// must sync its bundle before callback success. Accepted work survives
    /// caller cancellation; the callback cannot enqueue jobs on this token.
    /// Pending roots survive later checkpoints and explicit workspace forget.
    pub async fn capture_dirty<C, F>(
        &self,
        token: CommandToken,
        capture: C,
    ) -> Result<PendingCapture>
    where
        C: FnOnce() -> F + Send + 'static,
        F: std::future::Future<Output = Result<CaptureBundle>> + Send + 'static,
    {
        self.inner.check()?;
        let context = self.lookup(token)?;
        let runtime = storage::runtime()?;
        {
            let mut state = context.state.lock().map_err(|_| Error::Poisoned)?;
            if !state.owner_alive {
                return Err(Error::ClosedCommand);
            }
            if !matches!(state.phase, Phase::Open) {
                return Err(Error::CheckpointConflict);
            }
            state.phase = Phase::CapturingDirty;
        }
        let worker = runtime.spawn(async move {
            let result = async {
                drain_jobs(&context).await?;
                let captured = capture().await?;
                let commit = context.clone();
                storage::blocking(move || {
                    let checkpoint =
                        storage::canonical(captured.checkpoint, &commit.registry.limits)?;
                    commit.registry.capture_dirty(
                        commit.registration,
                        commit.epoch,
                        checkpoint,
                        captured.bundle,
                    )
                })
                .await
            }
            .await;
            context.state.lock().map_err(|_| Error::Poisoned)?.phase = if result.is_ok() {
                Phase::CapturedDirty
            } else {
                Phase::Failed
            };
            context.changed.notify_waiters();
            result
        });
        worker.await.map_err(|_| Error::Poisoned)?
    }
    /// Bounded immutable local outbox inventory. No upload or acknowledgement is
    /// implied, and this API intentionally cannot remove pending root claims.
    pub async fn pending_captures(&self) -> Result<Vec<PendingCapture>> {
        let inner = self.inner.clone();
        storage::blocking(move || inner.pending_captures()).await
    }
    /// Stop ordinary admission and drain accepted physical work WITHOUT
    /// checkpointing or making a dirty workspace collectable. The worker owns
    /// the command fence after caller cancellation. Hosts use this when exact
    /// final inventory is unavailable; it is not a backup or Clean receipt.
    pub async fn drain_dirty(&self, token: CommandToken) -> Result<()> {
        self.inner.check()?;
        let context = self.lookup(token)?;
        let runtime = storage::runtime()?;
        let first = {
            let mut state = context.state.lock().map_err(|_| Error::Poisoned)?;
            if !state.owner_alive {
                return Err(Error::ClosedCommand);
            }
            match state.phase {
                Phase::Open => {
                    state.phase = Phase::SealingDirty;
                    true
                }
                Phase::SealingDirty => false,
                Phase::DrainedDirty => return Ok(()),
                _ => return Err(Error::CheckpointConflict),
            }
        };
        let worker = runtime.spawn(async move {
            if first {
                drain_jobs(&context).await?;
                context.state.lock().map_err(|_| Error::Poisoned)?.phase = Phase::DrainedDirty;
                context.changed.notify_waiters();
                return Ok(());
            }
            loop {
                let changed = context.changed.notified();
                match context.state.lock().map_err(|_| Error::Poisoned)?.phase {
                    Phase::DrainedDirty => return Ok(()),
                    Phase::SealingDirty => {}
                    _ => return Err(Error::CheckpointConflict),
                }
                changed.await;
            }
        });
        worker.await.map_err(|_| Error::Poisoned)?
    }
    pub async fn acquire_collector(&self) -> Result<CollectorGuard> {
        self.inner.check()?;
        let write = self.inner.fence.clone().write_owned().await;
        let inner = self.inner.clone();
        storage::blocking(move || {
            let roots = inner.collector_roots()?;
            let epoch = inner
                .data
                .lock()
                .map_err(|_| Error::Poisoned)?
                .manifest
                .epoch;
            Ok(CollectorGuard {
                registry: inner,
                _write: write,
                roots,
                epoch,
            })
        })
        .await
    }
    pub async fn forget(&self, registration: Registration) -> Result<Registration> {
        self.inner.check()?;
        let write = self.inner.fence.clone().write_owned().await;
        let inner = self.inner.clone();
        storage::blocking(move || {
            let _write = write;
            inner.forget(registration)
        })
        .await
    }
    pub async fn pin_untracked_access(&self) -> Result<()> {
        self.inner.check()?;
        let read = self.inner.fence.clone().read_owned().await;
        let inner = self.inner.clone();
        storage::blocking(move || {
            let _read = read;
            inner.pin_untracked()
        })
        .await
    }
}
async fn checkpoint_worker(
    context: Arc<CommandContext>,
    checkpoint: RootCheckpoint,
) -> Result<CheckpointReceipt> {
    let validate = context.clone();
    let (checkpoint, fingerprint) = storage::blocking(move || {
        let checkpoint = storage::canonical(checkpoint, &validate.registry.limits)?;
        let fingerprint = storage::fingerprint(&validate.registration, validate.epoch, &checkpoint);
        Ok((checkpoint, fingerprint))
    })
    .await?;
    let first = {
        let mut state = context.state.lock().map_err(|_| Error::Poisoned)?;
        if !state.owner_alive {
            return Err(Error::ClosedCommand);
        }
        match &state.phase {
            Phase::Open => {
                state.phase = Phase::Frozen(fingerprint);
                true
            }
            Phase::Frozen(old) if old == &fingerprint => false,
            Phase::Complete(receipt) if receipt.fingerprint == fingerprint => {
                return Ok(receipt.clone());
            }
            Phase::Failed => return Err(Error::Poisoned),
            _ => return Err(Error::CheckpointConflict),
        }
    };
    if !first {
        loop {
            let changed = context.changed.notified();
            {
                let state = context.state.lock().map_err(|_| Error::Poisoned)?;
                match &state.phase {
                    Phase::Complete(receipt) => return Ok(receipt.clone()),
                    Phase::Failed => return Err(Error::Poisoned),
                    _ => {}
                }
            }
            changed.await;
        }
    }
    drain_jobs(&context).await?;
    let commit = context.clone();
    let result = storage::blocking(move || {
        commit
            .registry
            .checkpoint(commit.registration, commit.epoch, checkpoint, fingerprint)
    })
    .await;
    finish_checkpoint(&context, &result)?;
    result
}
async fn drain_jobs(context: &CommandContext) -> Result<()> {
    loop {
        let changed = context.changed.notified();
        if context.state.lock().map_err(|_| Error::Poisoned)?.jobs == 0 {
            return Ok(());
        }
        changed.await;
    }
}
fn finish_checkpoint(context: &CommandContext, result: &Result<CheckpointReceipt>) -> Result<()> {
    {
        let mut state = context.state.lock().map_err(|_| Error::Poisoned)?;
        state.phase = match result {
            Ok(receipt) => Phase::Complete(receipt.clone()),
            Err(_) => Phase::Failed,
        };
    }
    context.changed.notify_waiters();
    Ok(())
}
#[cfg(test)]
mod tests;
