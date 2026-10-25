//! Host-owned command fences and optional complete-local root checkpoints.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    collection: Option<CollectionIdentity>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CollectionIdentity {
    version: u32,
    alias: String,
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
    capture: Mutex<Option<Arc<crate::native_capture::Target>>>,
    pending: Mutex<Option<([u8; 32], CommandPreparation)>>,
    binding: Binding,
    admission: Arc<CommandAdmission>,
}
struct CommandAdmission {
    active: AtomicBool,
}
struct Session {
    workspace: Arc<Workspace>,
    replay: [u8; 32],
    preparation: CommandPreparation,
    token: [u8; 32],
    epoch: u64,
    owner: Mutex<Option<Arc<CommandOwner>>>,
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
    capture: Arc<crate::native_capture::Coordinator>,
    gc: Arc<crate::native_gc::Coordinator>,
    incarnation: [u8; 16],
    records: PrivateDir,
    workspaces: RwLock<HashMap<String, Arc<Workspace>>>,
    sessions: Mutex<HashMap<[u8; 32], Arc<Session>>>,
    starts: tokio::sync::Mutex<()>,
    upload_status: std::sync::OnceLock<Arc<crate::native_capture_upload::Stats>>,
    dirty_transitions: AtomicU64,
    dirty_transition_ns: AtomicU64,
    drains: AtomicU64,
    physical_drain_ns: AtomicU64,
}
#[derive(Clone)]
pub(crate) struct NativeCommandHost {
    inner: Arc<Inner>,
}
/// Private host startup capability; dropping it never admits commands. The
/// trusted caller must keep the actual frontend stopped or startup-gated before
/// registering/re-arming this workspace. No user RPC can issue this capability.
pub(crate) struct CommandStartupReservation {
    inner: Arc<Inner>,
    workspace: Arc<Workspace>,
}
/// All fallible command identity/lease checks have completed. The host activates
/// its actual frontend before consuming this value's infallible final commit.
pub(crate) struct CommandStartupActivation {
    admission: Arc<CommandAdmission>,
    _workspace: Arc<Workspace>,
    _inner: Arc<Inner>,
    // Release the exact writer lease only after all capability owners drop.
    _lease: File,
}
impl CommandStartupReservation {
    pub(crate) fn validate_for_activation(
        self,
        host: &NativeCommandHost,
        binding: &Binding,
        identity: [u8; 32],
    ) -> Result<CommandStartupActivation> {
        ensure!(
            Arc::ptr_eq(&self.inner, &host.inner),
            "native command startup host differs"
        );
        ensure!(
            self.workspace.binding == *binding && self.workspace.mapping.identity == identity,
            "native command startup binding differs"
        );
        let entries = self
            .inner
            .workspaces
            .read()
            .map_err(|_| anyhow::anyhow!("command map unavailable"))?;
        ensure!(
            entries
                .get(&binding.rpc_mount_id)
                .is_some_and(|entry| Arc::ptr_eq(entry, &self.workspace)),
            "native command startup registration differs"
        );
        ensure!(
            !self.workspace.admission.active.load(Ordering::Acquire),
            "native command startup already active"
        );
        ensure!(
            !self
                .inner
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("command map unavailable"))?
                .values()
                .any(|session| session.workspace.registration == self.workspace.registration),
            "native command startup has an active command"
        );
        let lease = existing_lease(&self.workspace.lease, Some(self.workspace.lease_identity))?;
        lease
            .try_lock_exclusive()
            .context("native command startup writer lease is held")?;
        drop(entries);
        Ok(CommandStartupActivation {
            admission: self.workspace.admission.clone(),
            _workspace: self.workspace,
            _inner: self.inner,
            _lease: lease,
        })
    }
}
impl CommandStartupActivation {
    /// Release the startup-only lease while commands are still closed, then
    /// publish the infallible final in-memory transition. Retained owners pin
    /// the exact registration; no fallible identity/lease checks remain.
    pub(crate) fn commit(self) {
        let Self {
            admission,
            _workspace,
            _inner,
            _lease,
        } = self;
        // Begin must never mistake this host lease for a CLI's writer lease.
        drop(_lease);
        admission.active.store(true, Ordering::Release);
    }
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
        E::DeadlineExceeded => Status::deadline_exceeded("native host admission deadline exceeded"),
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
        Self::open_with_capture(path, crate::config::NativeCapture::default()).await
    }
    pub async fn open_with_capture(
        path: &Path,
        capture: crate::config::NativeCapture,
    ) -> Result<Self> {
        Self::open_with_policy(
            path,
            capture,
            crate::config::NativeGc::default(),
            None,
            None,
        )
        .await
    }
    pub(crate) async fn open_with_policy(
        path: &Path,
        capture: crate::config::NativeCapture,
        gc: crate::config::NativeGc,
        alias: Option<String>,
        fresh_cohort: Option<crate::native_gc::FreshCohort>,
    ) -> Result<Self> {
        let directory = PrivateDir::create(path)?;
        let fresh = !directory.path().join("identity.json").try_exists()?;
        let collection = if gc.policy == crate::config::NativeGcPolicy::LocalComplete {
            Some(CollectionIdentity {
                version: 1,
                alias: alias.context("local collection alias missing")?,
            })
        } else {
            None
        };
        let header = match directory.read("identity.json", MAX_RECORD) {
            Ok(bytes) => serde_json::from_slice::<Header>(&bytes)?,
            Err(error) if !directory.path().join("identity.json").try_exists()? => {
                ensure!(
                    std::fs::read_dir(directory.path())?.next().is_none(),
                    "unknown native command state; preserve it"
                );
                ensure!(
                    collection.is_none() || fresh_cohort.is_some(),
                    "local collection requires a genuinely fresh native cohort"
                );
                let header = Header {
                    version: if collection.is_some() { 2 } else { 1 },
                    id: RegistryId::random(),
                    collection: collection.clone(),
                };
                directory.replace("identity.json", &serde_json::to_vec(&header)?)?;
                let _ = error;
                header
            }
            Err(error) => return Err(error.into()),
        };
        ensure!(
            (header.version == 1 && header.collection.is_none())
                || (header.version == 2 && header.collection.is_some()),
            "unsupported native command registry version"
        );
        ensure!(
            header.collection == collection,
            "native collection cohort policy differs; existing state is preserved"
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
            Registry::init(
                &ledger,
                header.id,
                if collection.is_some() {
                    Tracking::ManagedFresh
                } else {
                    Tracking::Untracked
                },
                Default::default(),
            )
            .await?
        };
        let records = PrivateDir::create(directory.path().join("records"))?;
        let capture = crate::native_capture::Coordinator::open(
            &directory.path().join("captures"),
            capture,
            &registry,
        )
        .await?;
        let host = Self {
            inner: Arc::new(Inner {
                registry,
                capture,
                gc: crate::native_gc::Coordinator::new(gc),
                incarnation: *uuid::Uuid::new_v4().as_bytes(),
                records,
                workspaces: Default::default(),
                sessions: Default::default(),
                starts: Default::default(),
                upload_status: Default::default(),
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
    /// Keep this scope through the entire accepted host operation and durable
    /// Dirty enrollment, not merely one bootstrap RPC. It is not root proof.
    pub(crate) fn host_access(
        &self,
    ) -> std::result::Result<Arc<native_retention::HostAccessGuard>, Status> {
        self.inner
            .registry
            .try_host_access()
            .map(Arc::new)
            .map_err(status)
    }
    pub(crate) async fn outer_host_access_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<Arc<native_retention::HostAccessGuard>, Status> {
        self.inner
            .registry
            .acquire_outer_host_access_until(deadline)
            .await
            .map(Arc::new)
            .map_err(status)
    }
    pub async fn register(&self, binding: &Binding, identity: [u8; 32]) -> Result<()> {
        self.register_inner(binding, identity, false, None)
            .await
            .map(|_| ())
    }
    /// Reserve command admission before starting/reopening a frontend. Matching
    /// active registrations may be re-armed only when no command or writer lease
    /// exists. The trusted Manager must separately own a stopped/gated frontend.
    pub(crate) async fn register_reserved(
        &self,
        binding: &Binding,
        identity: [u8; 32],
    ) -> Result<CommandStartupReservation> {
        let workspace = self.register_inner(binding, identity, true, None).await?;
        Ok(CommandStartupReservation {
            inner: self.inner.clone(),
            workspace,
        })
    }
    pub(crate) async fn register_with_host_access(
        &self,
        binding: &Binding,
        identity: [u8; 32],
        access: Arc<native_retention::HostAccessGuard>,
    ) -> Result<()> {
        self.register_inner(binding, identity, false, Some(access))
            .await
            .map(|_| ())
    }
    pub(crate) async fn register_reserved_with_host_access(
        &self,
        binding: &Binding,
        identity: [u8; 32],
        access: Arc<native_retention::HostAccessGuard>,
    ) -> Result<CommandStartupReservation> {
        let workspace = self
            .register_inner(binding, identity, true, Some(access))
            .await?;
        Ok(CommandStartupReservation {
            inner: self.inner.clone(),
            workspace,
        })
    }
    /// Manager holds a stopped/gated frontend and the host fence before using
    /// this explicit invalidation. Existing roots stay pinned until recaptured.
    pub(crate) async fn invalidate_workspace_with_host_access(
        &self,
        binding: &Binding,
        identity: [u8; 32],
        access: Arc<native_retention::HostAccessGuard>,
    ) -> Result<()> {
        let workspace = self
            .workspace(&binding.rpc_mount_id)
            .map_err(|_| anyhow::anyhow!("native workspace unavailable"))?;
        ensure!(
            workspace.binding == *binding && workspace.mapping.identity == identity,
            "native command startup binding differs"
        );
        self.inner
            .registry
            .mark_dirty_with_host_access(workspace.registration, access)
            .await?;
        Ok(())
    }
    async fn register_inner(
        &self,
        binding: &Binding,
        identity: [u8; 32],
        reserved: bool,
        access: Option<Arc<native_retention::HostAccessGuard>>,
    ) -> Result<Arc<Workspace>> {
        if let Some(access) = &access {
            self.inner.registry.validate_host_access(access)?;
        }
        let _start = self.inner.starts.lock().await;
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
        let registration = match access {
            Some(access) => {
                self.inner
                    .registry
                    .register_with_host_access(mapping.workspace, mapping.op_store, access)
                    .await?
            }
            None => {
                self.inner
                    .registry
                    .register(mapping.workspace, mapping.op_store)
                    .await?
            }
        };
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
            capture: Mutex::new(None),
            pending: Mutex::new(None),
            binding: binding.clone(),
            admission: Arc::new(CommandAdmission {
                active: AtomicBool::new(!reserved),
            }),
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
            ensure!(
                existing.admission.active.load(Ordering::Acquire),
                "native command startup is reserved; preserve its owner"
            );
            if reserved {
                ensure!(
                    existing.binding == *binding && existing.mapping.identity == identity,
                    "native command startup binding differs"
                );
                ensure!(
                    !self
                        .inner
                        .sessions
                        .lock()
                        .map_err(|_| anyhow::anyhow!("command map unavailable"))?
                        .values()
                        .any(|session| session.workspace.registration == existing.registration),
                    "native command is already active"
                );
                let lease = existing_lease(&existing.lease, Some(existing.lease_identity))?;
                lease
                    .try_lock_exclusive()
                    .context("native command writer lease is held")?;
                // Failed/busy re-arm leaves old preparations valid. Only after
                // all proofs succeed may the exact private host close admission.
                existing
                    .pending
                    .lock()
                    .map_err(|_| anyhow::anyhow!("command preparation unavailable"))?
                    .take();
                existing.admission.active.store(false, Ordering::Release);
                FileExt::unlock(&lease)?;
            }
            Ok(existing.clone())
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
            entries.insert(mount, workspace.clone());
            Ok(workspace)
        }
    }
    pub(crate) fn set_capture_target(
        &self,
        mount: &str,
        target: Option<Arc<crate::native_capture::Target>>,
    ) -> Result<()> {
        let workspace = self
            .workspace(mount)
            .map_err(|_| anyhow::anyhow!("native capture workspace unavailable"))?;
        if let Some(target) = &target {
            ensure!(
                target.binding.rpc_mount_id == mount
                    && target.binding.repository == workspace.mapping.alias
                    && target.index.provider_fingerprint == workspace.mapping.identity,
                "native capture target identity differs"
            );
        }
        *workspace
            .capture
            .lock()
            .map_err(|_| anyhow::anyhow!("native capture target unavailable"))? = target;
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
        if !workspace.admission.active.load(Ordering::Acquire) {
            return Err(Status::failed_precondition(
                "native command startup is not active",
            ));
        }
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
        if !workspace.admission.active.load(Ordering::Acquire) {
            return Err(Status::failed_precondition(
                "native command startup is not active",
            ));
        }
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
            owner: Mutex::new(Some(Arc::new(owner))),
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
        // Policy/target are frozen before either sealing operation. Staging
        // admission waits inside the host-owned drain, bounded by the original
        // deadline and MAX_ACTIVE sessions, never by a detached user waiter.
        // Never drain_dirty first and then try to capture its closed token.
        let target = session
            .workspace
            .capture
            .lock()
            .ok()
            .and_then(|target| target.clone());
        let clean = if self.inner.gc.config.policy == crate::config::NativeGcPolicy::LocalComplete {
            target
                .clone()
                .and_then(|target| self.inner.gc.select().map(|permit| (target, permit)))
        } else {
            None
        };
        let capture_deadline = tokio::time::Instant::now() + self.inner.capture.timeout();
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_millis(self.inner.gc.config.timeout_ms);
        let host = self.clone();
        tokio::spawn(async move {
            let drain_started = std::time::Instant::now();
            let result = if let Some((target, permit)) = clean {
                host.checkpoint_session(&session, target, permit, deadline)
                    .await
            } else {
                let plan = host
                    .inner
                    .capture
                    .select_until(target, &host.inner.registry, capture_deadline)
                    .await;
                match plan {
                    crate::native_capture::Plan::DrainOnly => host
                        .inner
                        .registry
                        .drain_dirty(CommandToken::from_bytes(session.token))
                        .await
                        .map_err(status),
                    crate::native_capture::Plan::Capture {
                        target,
                        permit,
                        disk,
                    } => {
                        host.capture_session(&session, target, permit, disk, capture_deadline)
                            .await
                    }
                }
            };
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
                        *failure = Some(error);
                    }
                }
            }
            session.changed.notify_waiters();
        });
    }
    async fn checkpoint_session(
        &self,
        session: &Arc<Session>,
        target: Arc<crate::native_capture::Target>,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        deadline: std::time::Instant,
    ) -> std::result::Result<(), Status> {
        let command = session
            .owner
            .lock()
            .map_err(|_| Status::internal("native command owner unavailable"))?
            .clone()
            .ok_or_else(|| Status::failed_precondition("native command owner closed"))?;
        let completion = Arc::new(crate::native_capture::Completion::default());
        let done = completion.clone();
        let coordinator = self.inner.gc.clone();
        let lease = session.workspace.lease.clone();
        let identity = session.workspace.lease_identity;
        let _admission = permit.clone();
        let callback = move || async move {
            let kernel = loop {
                if std::time::Instant::now() >= deadline {
                    done.complete();
                    return Err(native_retention::Error::Limit("checkpoint deadline"));
                }
                match existing_lease(&lease, Some(identity)) {
                    Ok(file) => match file.try_lock_exclusive() {
                        Ok(()) => break file,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(_) => {
                            done.complete();
                            return Err(native_retention::Error::Storage);
                        }
                    },
                    Err(_) => {
                        done.complete();
                        return Err(native_retention::Error::Storage);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            };
            crate::native_gc::checkpoint(
                coordinator,
                target,
                command,
                kernel,
                permit,
                done,
                deadline,
            )
            .await
            .map_err(|_| native_retention::Error::Storage)
        };
        let result = self
            .inner
            .registry
            .capture_checkpoint_owned(CommandToken::from_bytes(session.token), callback)
            .await;
        if !completion.wait(std::time::Duration::from_secs(5)).await {
            self.inner.gc.disposition("indeterminate", false);
            return Err(Status::failed_precondition(
                "local checkpoint physical ownership requires recovery",
            ));
        }
        match result {
            Ok(_) => {
                self.inner.gc.disposition("clean", true);
                Ok(())
            }
            Err(_) => {
                let state = self
                    .inner
                    .registry
                    .workspace(session.workspace.registration)
                    .await
                    .map_err(status)?;
                if state.state != native_retention::WorkspaceState::Dirty
                    || state.command_epoch != session.epoch
                {
                    return Err(Status::failed_precondition(
                        "local checkpoint requires dirty recovery",
                    ));
                }
                self.inner.gc.disposition("inventory_unavailable", false);
                // An unavailable complete inventory leaves the durable Dirty
                // claim in place; it does not roll back a successful jj edit.
                Ok(())
            }
        }
    }
    async fn capture_session(
        &self,
        session: &Arc<Session>,
        target: Arc<crate::native_capture::Target>,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        disk: Arc<crate::native_capture::DiskReservation>,
        deadline: tokio::time::Instant,
    ) -> std::result::Result<(), Status> {
        use crate::native_capture::{Attempt, Completion};
        let command = session
            .owner
            .lock()
            .map_err(|_| Status::internal("native command owner unavailable"))?
            .clone()
            .ok_or_else(|| Status::failed_precondition("native command owner closed"))?;
        let attempt = Arc::new(Mutex::new(Attempt::Selected));
        let completion = Arc::new(Completion::default());
        let phase = attempt.clone();
        let done = completion.clone();
        let coordinator = self.inner.capture.clone();
        let lease = session.workspace.lease.clone();
        let identity = session.workspace.lease_identity;
        let _host_admission = permit.clone(); // Includes post-commit inventory memory.
        let callback = move || async move {
            // This is entered only after Registry has sealed and drained jobs.
            *phase.lock().unwrap_or_else(|e| e.into_inner()) = Attempt::Entered;
            let kernel = loop {
                if tokio::time::Instant::now() >= deadline {
                    *phase.lock().unwrap_or_else(|e| e.into_inner()) =
                        Attempt::Unavailable("capture_timeout");
                    done.complete();
                    return Err(native_retention::Error::Storage);
                }
                match existing_lease(&lease, Some(identity)) {
                    Ok(file) => match file.try_lock_exclusive() {
                        Ok(()) => break file,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && tokio::time::Instant::now() < deadline => {}
                        Err(_) => {
                            *phase.lock().unwrap_or_else(|e| e.into_inner()) =
                                Attempt::Unavailable("lease_unavailable");
                            done.complete();
                            return Err(native_retention::Error::Storage);
                        }
                    },
                    Err(_) => {
                        *phase.lock().unwrap_or_else(|e| e.into_inner()) =
                            Attempt::Unavailable("lease_unavailable");
                        done.complete();
                        return Err(native_retention::Error::Storage);
                    }
                }
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + std::time::Duration::from_millis(5)),
                )
                .await;
            };
            crate::native_capture::capture(
                coordinator,
                target,
                command,
                kernel,
                permit,
                disk,
                done,
                phase,
                deadline,
            )
            .await
            .map_err(|_| native_retention::Error::Storage)
        };
        let result = self
            .inner
            .registry
            .capture_dirty_owned(CommandToken::from_bytes(session.token), callback)
            .await;
        let phase = *attempt
            .lock()
            .map_err(|_| Status::internal("native capture status unavailable"))?;
        match result {
            Ok(_) => {
                self.inner.capture.queued();
                self.inner.capture.refresh(&self.inner.registry).await;
                if !completion.wait_until(deadline).await {
                    self.inner.capture.disposition("indeterminate", true);
                    return Err(Status::failed_precondition(
                        "native capture physical teardown requires recovery",
                    ));
                }
                Ok(())
            }
            Err(error)
                if matches!(phase, Attempt::Unavailable(_))
                    || (phase == Attempt::Staged
                        && matches!(error, native_retention::Error::Limit(_))) =>
            {
                if !completion.wait_until(deadline).await {
                    self.inner.capture.disposition("indeterminate", true);
                    return Err(Status::failed_precondition(
                        "native capture physical teardown requires recovery",
                    ));
                }
                let state = self
                    .inner
                    .registry
                    .workspace(session.workspace.registration)
                    .await
                    .map_err(status)?;
                if state.state != native_retention::WorkspaceState::Dirty
                    || state.command_epoch != session.epoch
                {
                    return Err(Status::failed_precondition(
                        "native capture requires dirty recovery",
                    ));
                }
                let category = match phase {
                    Attempt::Unavailable(category) => category,
                    Attempt::Staged => "retention_full",
                    _ => unreachable!(),
                };
                self.inner.capture.disposition(category, true);
                // Expected exact-capture unavailability does not undo or fail
                // the already-completed user command. No Clean or queued claim.
                Ok(())
            }
            Err(_) => {
                self.inner.capture.disposition("indeterminate", true);
                Err(Status::failed_precondition(
                    "native capture publication requires recovery",
                ))
            }
        }
    }
    pub(crate) fn capture_registry(&self) -> Registry {
        self.inner.registry.clone()
    }
    pub(crate) async fn pause_capture_cleanup(
        &self,
    ) -> Result<Arc<crate::native_capture::SpoolCleanupGuard>> {
        self.inner.capture.pause_for_cleanup().await
    }
    pub(crate) async fn collect_native(
        &self,
        store: Arc<jj_native_backend::DirectObjectIo>,
        apply: bool,
    ) -> Result<serde_json::Value> {
        crate::native_gc::collect(
            self.inner.gc.clone(),
            self.inner.registry.clone(),
            store,
            apply,
        )
        .await
    }
    pub(crate) fn set_upload_status(
        &self,
        stats: Arc<crate::native_capture_upload::Stats>,
    ) -> Result<()> {
        self.inner
            .upload_status
            .set(stats)
            .map_err(|_| anyhow::anyhow!("capture uploader already configured"))
    }
    pub(crate) fn capture_mapping(
        &self,
        ticket: &native_retention::PendingCapture,
    ) -> Result<(String, String, [u8; 32])> {
        let mut found = None;
        let mut count = 0;
        for item in std::fs::read_dir(self.inner.records.path())? {
            count += 1;
            ensure!(count <= 4096, "capture mapping scan limit exceeded");
            let item = item?;
            let name = item.file_name();
            let mapping: Mapping =
                serde_json::from_slice(&self.inner.records.read(&name, MAX_RECORD)?)?;
            ensure!(
                mapping.version == 1 && name.to_str() == Some(&format!("{}.json", mapping.mount)),
                "invalid capture mapping record"
            );
            if mapping.workspace == ticket.registration.workspace
                && mapping.op_store == ticket.registration.op_store
            {
                ensure!(found.is_none(), "ambiguous capture mapping");
                found = Some((mapping.mount, mapping.alias, mapping.identity));
            }
        }
        found.context("capture mapping unavailable")
    }
    pub fn status(&self) -> serde_json::Value {
        let stats = self.inner.registry.storage_stats();
        // A durable obligation remains visible even when upload is disabled or
        // switched back to retention-only. Busy/unknown is null, never false.
        let cleanup_pending = self.inner.registry.capture_cleanup_pending().ok();
        let upload = self.inner.upload_status.get().map(|stats| {
            let mut status = stats.status();
            status["cleanup_pending"] = serde_json::json!(cleanup_pending);
            status
        });
        serde_json::json!({"active_commands":self.inner.sessions.lock().map(|s|s.len()).unwrap_or(MAX_ACTIVE),"registered_workspaces":self.inner.workspaces.read().map(|w|w.len()).unwrap_or(4096),"collection_enabled":self.inner.gc.config.policy==crate::config::NativeGcPolicy::LocalComplete,"local_gc":self.inner.gc.status(),"local_capture":self.inner.capture.status(),"capture_upload":upload,"capture_cleanup_pending":cleanup_pending,
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
impl native_broker::HostAccessResolver for NativeCommandHost {
    fn acquire_host_access(&self) -> std::result::Result<native_broker::CommandGuard, Status> {
        Ok(self.host_access()?)
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

#[cfg(test)]
mod tests {
    use super::*;
    fn binding(parent: &Path, name: &str, byte: u8) -> Result<Binding> {
        let root = parent.join(format!("root-{name}"));
        std::fs::create_dir(&root)?;
        let admins = PrivateDir::create(parent.join("admins"))?;
        let admin = admins.path().join(name);
        Ok(Binding {
            version: 1,
            workspace_id: [byte; 32],
            root,
            repo: admin.join("repo"),
            working_copy: admin.join("working_copy"),
            admin,
            repository: "example".into(),
            endpoint: native_workspace_admin::Endpoint::UnixSocket {
                path: parent.join("rbe.sock"),
            },
            rpc_mount_id: name.into(),
        })
    }
    fn forged() -> CommandPreparation {
        CommandPreparation {
            incarnation: [1; 16],
            nonce: [2; 32],
            expected_epoch: 0,
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capture_staging_waits_refreshes_committed_inventory_and_preserves_dirty_on_timeout()
    -> Result<()> {
        use crate::native_capture::{Plan, Target};
        let directory = tempfile::tempdir()?;
        let private = PrivateDir::create(directory.path().join("private"))?;
        let host = NativeCommandHost::open_with_capture(
            &private.path().join("commands"),
            crate::config::NativeCapture {
                policy: crate::config::NativeCapturePolicy::AfterCommandBestEffort,
                max_pending: 4,
                max_bundle_bytes: 4096,
                max_pending_bytes: 32768,
                ..Default::default()
            },
        )
        .await?;
        let binding = binding(private.path(), "queue", 1)?;
        host.register(&binding, [8; 32]).await?;
        let registry = host.capture_registry();
        let registration = host.workspace("queue")?.registration;
        let objects = PrivateDir::create(private.path().join("objects"))?;
        let socket = private.path().join("rbe.sock");
        let target = Arc::new(Target {
            controller: std::sync::Weak::new(),
            registry: native_workspace_admin::Registry::create(
                private.path().join("admin-registry"),
            )?,
            binding,
            local: Arc::new(jj_native_backend::DirectObjectIo::init(
                objects.path(),
                Default::default(),
            )?),
            index: jj_native_index::Config {
                endpoint: jj_native_index::Endpoint::UnixSocket(socket.clone()),
                store_alias: "example".into(),
                provider_fingerprint: [8; 32],
            },
            remote_scope: native_workspace_admin::remote_state::RemoteScope::new(
                socket,
                "example".into(),
            )?,
        });
        let pause = host.pause_capture_cleanup().await?;
        let coordinator = host.inner.capture.clone();
        let queued_registry = registry.clone();
        let queued_target = target.clone();
        let mut waiting = tokio::spawn(async move {
            coordinator
                .select_until(
                    Some(queued_target),
                    &queued_registry,
                    tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
                .await
                .is_err()
        );
        assert_eq!(host.status()["local_capture"]["skipped"], 0);
        assert_eq!(host.status()["local_capture"]["charged_spool_bytes"], 0);
        // Actual durable registry commit invalidates the prior cached inventory.
        // The opaque synced bundle arranges admission only, not restore proof.
        private.replace("bundle", &[7; 32])?;
        let command = registry.begin_command(registration).await?;
        registry
            .capture_dirty(command.token(), || async {
                Ok(native_retention::CaptureBundle {
                    checkpoint: Default::default(),
                    bundle: native_retention::BundleDescriptor {
                        blake3: *blake3::hash(&[7; 32]).as_bytes(),
                        size_bytes: 32,
                    },
                })
            })
            .await?;
        drop(command);
        host.inner.capture.queued();
        assert_eq!(host.status()["local_capture"]["inventory_available"], false);
        drop(pause);
        let plan = tokio::time::timeout(std::time::Duration::from_secs(1), waiting).await??;
        assert!(
            matches!(&plan, Plan::Capture { .. }),
            "queued staging must refresh instead of skipping stale unavailable inventory"
        );
        assert_eq!(host.status()["local_capture"]["inventory_available"], true);
        assert_eq!(host.status()["local_capture"]["pending_count"], 1);
        assert_eq!(host.status()["local_capture"]["charged_spool_bytes"], 4096);
        let timeout = host
            .inner
            .capture
            .select_until(
                Some(target.clone()),
                &registry,
                tokio::time::Instant::now() + std::time::Duration::from_millis(20),
            )
            .await;
        assert!(matches!(timeout, Plan::DrainOnly));
        assert_eq!(
            host.status()["local_capture"]["last_disposition"],
            "admission_timeout"
        );
        assert_eq!(
            host.status()["local_capture"]["queued"],
            1,
            "timed-out admission never claims another capture"
        );
        assert_eq!(
            registry.workspace(registration).await?.state,
            native_retention::WorkspaceState::Dirty
        );
        let cancelled_coordinator = host.inner.capture.clone();
        let cancelled_registry = registry.clone();
        let cancelled = tokio::spawn(async move {
            cancelled_coordinator
                .select_until(
                    Some(target),
                    &cancelled_registry,
                    tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
        });
        tokio::task::yield_now().await;
        cancelled.abort();
        assert!(cancelled.await.err().unwrap().is_cancelled());
        drop(plan);
        assert_eq!(host.status()["local_capture"]["charged_spool_bytes"], 0);
        let cleanup = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            host.pause_capture_cleanup(),
        )
        .await??;
        drop(cleanup);
        assert_eq!(registry.pending_captures().await?.len(), 1);
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_registration_is_closed_until_exact_prevalidated_activation_and_drop_stays_closed()
    -> Result<()> {
        let directory = tempfile::tempdir()?;
        let private = PrivateDir::create(directory.path().join("private"))?;
        let host = NativeCommandHost::open(&private.path().join("commands")).await?;
        let dropped = binding(private.path(), "dropped", 1)?;
        let reservation = host.register_reserved(&dropped, [8; 32]).await?;
        assert_eq!(
            host.prepare("dropped", [1; 32]).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            host.begin("dropped", [1; 32], forged())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(host.workspace_state("dropped").await?.command_epoch, 0);
        assert!(
            !host.active("dropped"),
            "stop/recovery probes still work before activation"
        );
        assert!(host.register_reserved(&dropped, [8; 32]).await.is_err());
        assert!(
            host.register(&dropped, [8; 32]).await.is_err(),
            "ordinary registration cannot bypass reserved startup"
        );
        drop(reservation);
        assert!(
            host.prepare("dropped", [1; 32]).await.is_err(),
            "cancellation never opens admission"
        );
        assert!(!host.active("dropped"));

        let foreign = binding(private.path(), "foreign", 2)?;
        let reservation = host.register_reserved(&foreign, [8; 32]).await?;
        let other = NativeCommandHost::open(&private.path().join("other-commands")).await?;
        assert!(
            reservation
                .validate_for_activation(&other, &foreign, [8; 32])
                .is_err()
        );
        assert!(host.prepare("foreign", [1; 32]).await.is_err());
        let mismatched = binding(private.path(), "mismatched", 3)?;
        let reservation = host.register_reserved(&mismatched, [8; 32]).await?;
        let mut wrong = mismatched.clone();
        wrong.root = dropped.root.clone();
        assert!(
            reservation
                .validate_for_activation(&host, &wrong, [8; 32])
                .is_err(),
            "same alias/lease cannot substitute a different root binding"
        );
        assert!(host.prepare("mismatched", [1; 32]).await.is_err());

        let held = binding(private.path(), "validated-drop", 4)?;
        let activation = host
            .register_reserved(&held, [8; 32])
            .await?
            .validate_for_activation(&host, &held, [8; 32])?;
        assert!(
            host.active("validated-drop"),
            "the exact lease pins the activation critical section"
        );
        assert!(host.prepare("validated-drop", [1; 32]).await.is_err());
        assert!(host.register(&held, [8; 32]).await.is_err());
        drop(activation);
        assert!(!host.active("validated-drop"));
        assert!(host.prepare("validated-drop", [1; 32]).await.is_err());

        let ready = binding(private.path(), "activated", 5)?;
        let activation = host
            .register_reserved(&ready, [8; 32])
            .await?
            .validate_for_activation(&host, &ready, [8; 32])?;
        // The actual frontend activation belongs between these two calls. This
        // unit verifies command admission only, not a native frontend proof.
        activation.commit();
        assert!(!host.active("activated"));
        host.register(&ready, [8; 32]).await?;
        let preparation = host.prepare("activated", [1; 32]).await?;
        assert!(
            host.begin("activated", [1; 32], preparation.clone())
                .await
                .is_err(),
            "startup activation must not stand in for a CLI writer lease"
        );
        assert_eq!(host.workspace_state("activated").await?.command_epoch, 0);
        let lease = existing_lease(
            &native_vfs_rpc::command_lock_path(&ready.admin, "activated")?,
            None,
        )?;
        lease.try_lock_exclusive()?;
        let started = host.begin("activated", [1; 32], preparation).await?;
        assert_eq!(
            started.epoch, 1,
            "the SAME preparation is accepted only with an actual CLI lease"
        );
        drop(lease);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            host.finish_dirty("activated", started.token),
        )
        .await??;
        let ordinary = binding(private.path(), "ordinary", 6)?;
        host.register(&ordinary, [8; 32]).await?;
        host.register(&ordinary, [8; 32]).await?;
        assert!(host.prepare("ordinary", [1; 32]).await.is_ok());
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_rearm_rejects_busy_without_invalidating_command_and_clears_old_preparations()
    -> Result<()> {
        let directory = tempfile::tempdir()?;
        let private = PrivateDir::create(directory.path().join("private"))?;
        let host = NativeCommandHost::open(&private.path().join("commands")).await?;
        let binding = binding(private.path(), "workspace", 7)?;
        host.register(&binding, [8; 32]).await?;
        let preparation = host.prepare("workspace", [1; 32]).await?;
        let path = native_vfs_rpc::command_lock_path(&binding.admin, "workspace")?;
        let lease = existing_lease(&path, None)?;
        lease.try_lock_exclusive()?;
        assert!(host.register_reserved(&binding, [8; 32]).await.is_err());
        // Failure above did not clear the pending preparation or close an
        // ordinary registration while its legitimate CLI was taking the lease.
        let started = host.begin("workspace", [1; 32], preparation).await?;
        let physical = native_broker::CommandResolver::resolve(&host, "example", started.token)?;
        assert!(host.register_reserved(&binding, [8; 32]).await.is_err());
        assert!(native_broker::CommandResolver::resolve(&host, "example", started.token).is_ok());
        drop(lease);
        drop(physical);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            host.finish_dirty("workspace", started.token),
        )
        .await??;
        let stale = host.prepare("workspace", [2; 32]).await?;
        let reservation = host.register_reserved(&binding, [8; 32]).await?;
        assert!(host.prepare("workspace", [3; 32]).await.is_err());
        assert!(
            host.begin("workspace", [2; 32], stale.clone())
                .await
                .is_err()
        );
        assert!(host.register_reserved(&binding, [8; 32]).await.is_err());
        reservation
            .validate_for_activation(&host, &binding, [8; 32])?
            .commit();
        let lease = existing_lease(&path, None)?;
        lease.try_lock_exclusive()?;
        assert!(
            host.begin("workspace", [2; 32], stale).await.is_err(),
            "pre-rearm preparation must not resurrect after activation"
        );
        assert_eq!(host.workspace_state("workspace").await?.command_epoch, 1);
        let fresh = host.prepare("workspace", [3; 32]).await?;
        let next = host.begin("workspace", [3; 32], fresh).await?;
        assert_eq!(next.epoch, 2);
        drop(lease);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            host.finish_dirty("workspace", next.token),
        )
        .await??;
        assert_eq!(
            host.workspace_state("workspace").await?.state,
            native_retention::WorkspaceState::Dirty
        );
        Ok(())
    }
}
