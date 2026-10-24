//! Host-owned dirty command fences. No collector or Clean checkpoint is exposed.
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use fs2::FileExt;
use native_retention::{
    CommandJobGuard, CommandOwner, CommandToken, OpStoreId, Registration, Registry, RegistryId,
    Tracking, WorkspaceId,
};
use native_vfs_rpc::{CommandHost, CommandJob, CommandPreparation, CommandStarted};
use native_workspace_admin::Binding;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tonic::Status;
const MAX_RECORD: usize = 16 * 1024;
const MAX_ACTIVE: usize = 64;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    version: u32,
    id: RegistryId,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Mapping {
    version: u32,
    mount: String,
    alias: String,
    identity: [u8; 32],
    workspace: WorkspaceId,
    op_store: OpStoreId,
}
struct Workspace {
    mapping: Mapping,
    registration: Registration,
    lease: PathBuf,
    lease_identity: (u64, u64),
    closed: Mutex<Option<[u8; 32]>>,
    pending: Mutex<Option<([u8; 32], CommandPreparation)>>,
}
struct Session {
    workspace: Arc<Workspace>,
    replay: [u8; 32],
    preparation: CommandPreparation,
    token: [u8; 32],
    epoch: u64,
    owner: Mutex<Option<CommandOwner>>,
    physical: Mutex<Option<CommandJobGuard>>,
    probe: File,
    released: AtomicBool,
    drained: AtomicBool,
    drain_started: AtomicBool,
    changed: tokio::sync::Notify,
    failure: Mutex<Option<Status>>,
}
struct Inner {
    registry: Registry,
    incarnation: [u8; 16],
    records: PrivateDir,
    workspaces: RwLock<HashMap<String, Arc<Workspace>>>,
    sessions: Mutex<HashMap<[u8; 32], Arc<Session>>>,
    starts: tokio::sync::Mutex<()>,
    dirty_transitions: AtomicU64,
    dirty_transition_ns: AtomicU64,
    drains: AtomicU64,
    physical_drain_ns: AtomicU64,
}
#[derive(Clone)]
pub(crate) struct NativeCommandHost {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for NativeCommandHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeCommandHost").finish_non_exhaustive()
    }
}
fn status(error: native_retention::Error) -> Status {
    use native_retention::Error as E;
    match error {
        E::Busy => Status::aborted("native command is already active"),
        E::Limit(_) => Status::resource_exhausted("native command limit exceeded"),
        E::ClosedCommand => Status::failed_precondition("native command is closed"),
        _ => Status::failed_precondition("native command retention is unavailable"),
    }
}
fn existing_lease(path: &Path, identity: Option<(u64, u64)>) -> std::io::Result<File> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("invalid native command lease"))?;
    let _directory = PrivateDir::open_existing(parent)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
        || identity.is_some_and(|expected| expected != (metadata.dev(), metadata.ino()))
    {
        return Err(std::io::Error::other(
            "native command lease identity differs",
        ));
    }
    Ok(file)
}
impl NativeCommandHost {
    pub async fn open(path: &Path) -> Result<Self> {
        let directory = PrivateDir::create(path)?;
        let fresh = !directory.path().join("identity.json").try_exists()?;
        let header = match directory.read("identity.json", MAX_RECORD) {
            Ok(bytes) => serde_json::from_slice::<Header>(&bytes)?,
            Err(error) if !directory.path().join("identity.json").try_exists()? => {
                ensure!(
                    std::fs::read_dir(directory.path())?.next().is_none(),
                    "unknown native command state; preserve it"
                );
                let header = Header {
                    version: 1,
                    id: RegistryId::random(),
                };
                directory.replace("identity.json", &serde_json::to_vec(&header)?)?;
                let _ = error;
                header
            }
            Err(error) => return Err(error.into()),
        };
        ensure!(
            header.version == 1,
            "unsupported native command registry version"
        );
        let ledger = directory.path().join("ledger");
        let registry = if ledger.try_exists()? {
            Registry::open_existing(&ledger, header.id, Default::default()).await?
        } else {
            ensure!(
                fresh,
                "partial native command ledger; preserve state and reconcile explicitly"
            );
            // Existing storage cannot be called ManagedFresh merely because
            // this ledger is new. Checkpoint A never enables collection.
            Registry::init(&ledger, header.id, Tracking::Untracked, Default::default()).await?
        };
        let records = PrivateDir::create(directory.path().join("records"))?;
        let host = Self {
            inner: Arc::new(Inner {
                registry,
                incarnation: *uuid::Uuid::new_v4().as_bytes(),
                records,
                workspaces: Default::default(),
                sessions: Default::default(),
                starts: Default::default(),
                dirty_transitions: AtomicU64::new(0),
                dirty_transition_ns: AtomicU64::new(0),
                drains: AtomicU64::new(0),
                physical_drain_ns: AtomicU64::new(0),
            }),
        };
        let weak = Arc::downgrade(&host.inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let Some(inner) = weak.upgrade() else { break };
                NativeCommandHost { inner }.poll_leases().await;
            }
        });
        Ok(host)
    }
    pub async fn register(&self, binding: &Binding, identity: [u8; 32]) -> Result<()> {
        let mount = binding.rpc_mount_id.clone();
        ensure!(
            !mount.is_empty()
                && mount.len() <= 128
                && mount
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
            "invalid native command mount identity"
        );
        let name = format!("{mount}.json");
        let known = self.inner.records.path().join(&name).try_exists()?;
        let mapping = if known {
            let mapping: Mapping =
                serde_json::from_slice(&self.inner.records.read(&name, MAX_RECORD)?)?;
            ensure!(
                mapping.version == 1
                    && mapping.mount == mount
                    && mapping.alias == binding.repository
                    && mapping.identity == identity,
                "native command provider identity changed"
            );
            mapping
        } else {
            let mapping = Mapping {
                version: 1,
                mount: mount.clone(),
                alias: binding.repository.clone(),
                identity,
                workspace: WorkspaceId::random(),
                op_store: OpStoreId::random(),
            };
            self.inner
                .records
                .replace(&name, &serde_json::to_vec(&mapping)?)?;
            mapping
        };
        ensure!(
            mapping.workspace.0 != [0; 16] && mapping.op_store.0 != [0; 16],
            "invalid native command registration identity"
        );
        let registration = self
            .inner
            .registry
            .register(mapping.workspace, mapping.op_store)
            .await?;
        let lease = native_vfs_rpc::command_lock_path(&binding.admin, &mount)?;
        let parent = lease
            .parent()
            .context("native command lease has no parent")?;
        let locks = if known {
            PrivateDir::open_existing(parent)?
        } else {
            PrivateDir::create(parent)?
        };
        let lease_file = if known {
            existing_lease(&lease, None)?
        } else {
            locks.open_lock(lease.file_name().context("native command lease name")?)?
        };
        use std::os::unix::fs::MetadataExt;
        let metadata = lease_file.metadata()?;
        let lease_identity = (metadata.dev(), metadata.ino());
        drop(lease_file);
        let workspace = Arc::new(Workspace {
            mapping,
            registration,
            lease,
            lease_identity,
            closed: Mutex::new(None),
            pending: Mutex::new(None),
        });
        let mut entries = self
            .inner
            .workspaces
            .write()
            .map_err(|_| anyhow::anyhow!("command map unavailable"))?;
        if let Some(existing) = entries.get(&mount) {
            ensure!(
                existing.registration == workspace.registration
                    && existing.lease == workspace.lease
                    && existing.lease_identity == workspace.lease_identity,
                "native command registration changed"
            );
        } else {
            ensure!(
                entries.len() < 4096,
                "native command workspace limit exceeded"
            );
            ensure!(
                !entries
                    .values()
                    .any(|entry| entry.registration == workspace.registration),
                "native command registration aliases another workspace"
            );
            entries.insert(mount, workspace);
        }
        Ok(())
    }
    fn workspace(&self, mount: &str) -> std::result::Result<Arc<Workspace>, Status> {
        self.inner
            .workspaces
            .read()
            .map_err(|_| Status::internal("command map unavailable"))?
            .get(mount)
            .cloned()
            .ok_or_else(|| Status::failed_precondition("native command workspace is unregistered"))
    }
    fn session(&self, token: [u8; 32]) -> std::result::Result<Arc<Session>, Status> {
        self.inner
            .sessions
            .lock()
            .map_err(|_| Status::internal("command map unavailable"))?
            .get(&token)
            .cloned()
            .ok_or_else(|| Status::failed_precondition("native command token is closed"))
    }
    async fn prepare_job(
        &self,
        mount: &str,
        replay: [u8; 32],
    ) -> std::result::Result<CommandPreparation, Status> {
        if replay == [0; 32] {
            return Err(Status::invalid_argument(
                "native command replay key is empty",
            ));
        }
        let _start = self.inner.starts.lock().await;
        let workspace = self.workspace(mount)?;
        {
            let sessions = self
                .inner
                .sessions
                .lock()
                .map_err(|_| Status::internal("command map unavailable"))?;
            if let Some(session) = sessions
                .values()
                .find(|s| s.workspace.registration == workspace.registration)
            {
                return if session.replay == replay && !session.drain_started.load(Ordering::Acquire)
                {
                    Ok(session.preparation.clone())
                } else {
                    Err(Status::aborted("native command is already active"))
                };
            }
        }
        let epoch = self
            .inner
            .registry
            .workspace(workspace.registration)
            .await
            .map_err(status)?
            .command_epoch;
        let mut pending = workspace
            .pending
            .lock()
            .map_err(|_| Status::internal("command preparation unavailable"))?;
        if let Some((old, preparation)) = &*pending {
            if *old == replay && preparation.expected_epoch == epoch {
                return Ok(preparation.clone());
            }
        }
        let mut nonce = [0; 32];
        nonce[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        nonce[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        let preparation = CommandPreparation {
            incarnation: self.inner.incarnation,
            nonce,
            expected_epoch: epoch,
        };
        *pending = Some((replay, preparation.clone()));
        Ok(preparation)
    }
    async fn begin_job(
        &self,
        mount: &str,
        replay: [u8; 32],
        preparation: CommandPreparation,
    ) -> std::result::Result<CommandStarted, Status> {
        if replay == [0; 32] {
            return Err(Status::invalid_argument(
                "native command replay key is empty",
            ));
        }
        let _start = self.inner.starts.lock().await;
        let workspace = self.workspace(mount)?;
        {
            let sessions = self
                .inner
                .sessions
                .lock()
                .map_err(|_| Status::internal("command map unavailable"))?;
            for session in sessions.values() {
                if session.workspace.registration == workspace.registration {
                    if session.replay == replay
                        && session.preparation == preparation
                        && !session.released.load(Ordering::Acquire)
                        && !session.drain_started.load(Ordering::Acquire)
                    {
                        return Ok(CommandStarted {
                            token: session.token,
                            epoch: session.epoch,
                        });
                    }
                    return Err(Status::aborted("native command is already active"));
                }
            }
            if sessions.len() >= MAX_ACTIVE {
                return Err(Status::resource_exhausted(
                    "native active command limit exceeded",
                ));
            }
        }
        if preparation.incarnation != self.inner.incarnation {
            return Err(Status::failed_precondition(
                "native command preparation expired",
            ));
        }
        {
            let pending = workspace
                .pending
                .lock()
                .map_err(|_| Status::internal("command preparation unavailable"))?;
            if pending.as_ref() != Some(&(replay, preparation.clone())) {
                return Err(Status::failed_precondition(
                    "native command preparation is stale or consumed",
                ));
            }
        }
        let epoch = self
            .inner
            .registry
            .workspace(workspace.registration)
            .await
            .map_err(status)?
            .command_epoch;
        if epoch != preparation.expected_epoch {
            return Err(Status::failed_precondition("native command epoch changed"));
        }
        let probe = existing_lease(&workspace.lease, Some(workspace.lease_identity))
            .map_err(|_| Status::failed_precondition("native command lease is unavailable"))?;
        match probe.try_lock_exclusive() {
            Ok(()) => {
                let _ = FileExt::unlock(&probe);
                return Err(Status::failed_precondition(
                    "native local writer lease is not held",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => {
                return Err(Status::failed_precondition(
                    "native local writer lease cannot be verified",
                ));
            }
        }
        workspace
            .pending
            .lock()
            .map_err(|_| Status::internal("command preparation unavailable"))?
            .take();
        let dirty_started = std::time::Instant::now();
        let owner = self
            .inner
            .registry
            .begin_command(workspace.registration)
            .await
            .map_err(status)?;
        self.inner.dirty_transitions.fetch_add(1, Ordering::Relaxed);
        self.inner.dirty_transition_ns.fetch_add(
            dirty_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        let token = *owner.token().as_bytes();
        let epoch = owner.epoch();
        let physical = self.inner.registry.resolve(owner.token()).map_err(status)?;
        let session = Arc::new(Session {
            workspace,
            replay,
            preparation,
            token,
            epoch,
            owner: Mutex::new(Some(owner)),
            physical: Mutex::new(Some(physical)),
            probe,
            released: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            drain_started: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
            failure: Mutex::new(None),
        });
        self.inner
            .sessions
            .lock()
            .map_err(|_| Status::internal("command map unavailable"))?
            .insert(token, session);
        Ok(CommandStarted { token, epoch })
    }
    async fn finish_job(&self, mount: &str, token: [u8; 32]) -> std::result::Result<(), Status> {
        let workspace = self.workspace(mount)?;
        let session = match self.session(token) {
            Ok(session) => session,
            Err(error) => {
                return if *workspace
                    .closed
                    .lock()
                    .map_err(|_| Status::internal("command map unavailable"))?
                    == Some(token)
                {
                    Ok(())
                } else {
                    Err(error)
                };
            }
        };
        if session.workspace.registration != workspace.registration {
            return Err(Status::permission_denied(
                "native command workspace mismatch",
            ));
        }
        self.observe_release(&session);
        self.start_drain(session.clone());
        loop {
            let changed = session.changed.notified();
            if session.drained.load(Ordering::Acquire) {
                return Ok(());
            }
            if let Some(failure) = session
                .failure
                .lock()
                .map_err(|_| Status::internal("command owner unavailable"))?
                .clone()
            {
                return Err(failure);
            }
            changed.await;
        }
    }
    fn start_drain(&self, session: Arc<Session>) {
        if session.drain_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let host = self.clone();
        tokio::spawn(async move {
            let drain_started = std::time::Instant::now();
            let result = host
                .inner
                .registry
                .drain_dirty(CommandToken::from_bytes(session.token))
                .await;
            match result {
                Ok(()) => {
                    host.inner.drains.fetch_add(1, Ordering::Relaxed);
                    host.inner.physical_drain_ns.fetch_add(
                        drain_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        Ordering::Relaxed,
                    );
                    if let Ok(mut closed) = session.workspace.closed.lock() {
                        *closed = Some(session.token);
                    }
                    if let Ok(mut owner) = session.owner.lock() {
                        owner.take();
                    }
                    if let Ok(mut entries) = host.inner.sessions.lock() {
                        entries.remove(&session.token);
                    }
                    session.drained.store(true, Ordering::Release);
                }
                Err(error) => {
                    if let Ok(mut failure) = session.failure.lock() {
                        *failure = Some(status(error));
                    }
                }
            }
            session.changed.notify_waiters();
        });
    }
    pub fn status(&self) -> serde_json::Value {
        let stats = self.inner.registry.storage_stats();
        serde_json::json!({"active_commands":self.inner.sessions.lock().map(|s|s.len()).unwrap_or(MAX_ACTIVE),"registered_workspaces":self.inner.workspaces.read().map(|w|w.len()).unwrap_or(4096),"collection_enabled":false,
            "dirty_transitions":self.inner.dirty_transitions.load(Ordering::Relaxed),"dirty_transition_ns":self.inner.dirty_transition_ns.load(Ordering::Relaxed),"physical_drains":self.inner.drains.load(Ordering::Relaxed),"physical_drain_ns":self.inner.physical_drain_ns.load(Ordering::Relaxed),"retention_transactions":stats.transactions,"retention_encoded_write_bytes":stats.encoded_write_bytes})
    }
    pub async fn workspace_state(&self, mount: &str) -> Result<native_retention::WorkspaceInfo> {
        let workspace = self.workspace(mount)?;
        Ok(self
            .inner
            .registry
            .workspace(workspace.registration)
            .await?)
    }
    pub fn active(&self, mount: &str) -> bool {
        match self.inner.sessions.lock() {
            Ok(entries)
                if entries
                    .values()
                    .any(|session| session.workspace.mapping.mount == mount) =>
            {
                return true;
            }
            Err(_) => return true,
            _ => {}
        }
        // Restart does not adopt old tokens, but a still-live older CLI's
        // kernel lease must block stop/unmount. Never create/repair a probe.
        let Ok(workspace) = self.workspace(mount) else {
            return true;
        };
        let Ok(probe) = existing_lease(&workspace.lease, Some(workspace.lease_identity)) else {
            return true;
        };
        match probe.try_lock_exclusive() {
            Ok(()) => {
                let _ = FileExt::unlock(&probe);
                false
            }
            Err(_) => true,
        }
    }
    fn observe_release(&self, session: &Session) -> bool {
        if session.released.load(Ordering::Acquire) {
            return true;
        }
        if session.probe.try_lock_exclusive().is_err() {
            return false;
        }
        let _ = FileExt::unlock(&session.probe);
        session.released.store(true, Ordering::Release);
        if let Ok(mut lease) = session.physical.lock() {
            lease.take();
        }
        true
    }
    async fn poll_leases(&self) {
        let sessions = match self.inner.sessions.lock() {
            Ok(entries) => entries.values().cloned().collect::<Vec<_>>(),
            Err(_) => return,
        };
        for session in sessions {
            if self.observe_release(&session) {
                // At most one worker per admitted session; never wait for one
                // command before observing another process's kernel lease.
                self.start_drain(session);
            }
        }
    }
}
#[async_trait]
impl CommandHost for NativeCommandHost {
    async fn prepare(
        &self,
        mount: &str,
        replay: [u8; 32],
    ) -> std::result::Result<CommandPreparation, Status> {
        self.prepare_job(mount, replay).await
    }
    async fn begin(
        &self,
        mount: &str,
        replay: [u8; 32],
        preparation: CommandPreparation,
    ) -> std::result::Result<CommandStarted, Status> {
        let host = self.clone();
        let mount = mount.to_owned();
        tokio::spawn(async move { host.begin_job(&mount, replay, preparation).await })
            .await
            .map_err(|_| Status::internal("native command worker failed"))?
    }
    async fn finish_dirty(&self, mount: &str, token: [u8; 32]) -> std::result::Result<(), Status> {
        self.finish_job(mount, token).await
    }
    fn resolve(&self, mount: &str, token: [u8; 32]) -> std::result::Result<CommandJob, Status> {
        let session = self.session(token)?;
        if session.workspace.mapping.mount != mount {
            return Err(Status::permission_denied(
                "native command workspace mismatch",
            ));
        }
        Ok(Arc::new(
            self.inner
                .registry
                .resolve(CommandToken::from_bytes(token))
                .map_err(status)?,
        ))
    }
}
impl native_broker::CommandResolver for NativeCommandHost {
    fn resolve(
        &self,
        alias: &str,
        token: [u8; 32],
    ) -> std::result::Result<native_broker::CommandGuard, Status> {
        let session = self.session(token)?;
        if session.workspace.mapping.alias != alias {
            return Err(Status::permission_denied(
                "native command repository mismatch",
            ));
        }
        Ok(Arc::new(
            self.inner
                .registry
                .resolve(CommandToken::from_bytes(token))
                .map_err(status)?,
        ))
    }
}
