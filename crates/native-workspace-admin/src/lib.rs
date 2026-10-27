//! External, per-workspace jj administration for mandatory lazy native mounts.
//! The projected control marker contains an opaque ID, never an admin path.
//! Only an owner-private registry supplies paths and local transport authority.
pub mod capture;
pub mod landing_resolution;
pub mod remote_state;
pub mod restore;

use std::{
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use jj_lib::{
    backend::CommitId,
    object_id::ObjectId,
    op_store::OperationId,
    ref_name::WorkspaceNameBuf,
    repo::{BackendInitializer, ReadonlyRepo, Repo, RepoLoader, StoreFactories, StoreLoadError},
    settings::UserSettings,
    signing::Signer,
    working_copy::{WorkingCopy, WorkingCopyFactory, WorkingCopyStateError},
    workspace::{
        WorkingCopyFactories, Workspace, WorkspaceLoadError, WorkspaceLoader,
        WorkspaceLoaderFactory,
    },
};
use jj_vfs::native::{NativeVfsWorkingCopyFactory, ProjectionBinding, ProjectionConnector};
use native_broker::LocalEndpoint;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};

mod intents;
pub use intents::{ProvisioningIntent, ResumeInfo};

const MAX_RECORD: usize = 32 * 1024;
const JOURNAL: &str = "initialization.json";
const MARKER: &str = ".jj/native-workspace";
const MAX_WORKSPACES: usize = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryHeader {
    version: u32,
    ready: bool,
    #[serde(default)]
    client_intents: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Endpoint {
    UnixSocket { path: PathBuf },
    WindowsNamedPipe { name: String },
}
impl Endpoint {
    pub fn local(&self) -> LocalEndpoint {
        match self {
            Self::UnixSocket { path } => LocalEndpoint::UnixSocket(path.clone()),
            Self::WindowsNamedPipe { name } => LocalEndpoint::WindowsNamedPipe(name.clone()),
        }
    }
}
impl From<LocalEndpoint> for Endpoint {
    fn from(value: LocalEndpoint) -> Self {
        match value {
            LocalEndpoint::UnixSocket(path) => Self::UnixSocket { path },
            LocalEndpoint::WindowsNamedPipe(name) => Self::WindowsNamedPipe { name },
        }
    }
}

/// A host-created registration. Deserialization alone confers no authority:
/// every use validates paths against the configured registry and opaque marker.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub version: u32,
    pub workspace_id: [u8; 32],
    pub root: PathBuf,
    pub admin: PathBuf,
    pub repo: PathBuf,
    pub working_copy: PathBuf,
    pub repository: String,
    pub endpoint: Endpoint,
    pub rpc_mount_id: String,
}
impl Binding {
    pub fn projection(&self) -> ProjectionBinding {
        ProjectionBinding {
            root: self.root.clone(),
            state_path: self.working_copy.clone(),
            repository: self.repository.clone(),
        }
    }
    pub fn id(&self) -> String {
        hex(&self.workspace_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Initialization {
    version: u32,
    binding: Binding,
    initial_commit: Vec<u8>,
    workspace: String,
    ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    restore_origin: Option<[u8; 32]>,
}

/// Read-only private enrollment observations. Locks live through the caller's
/// census only; release before ordinary initialization/retirement operations.
pub struct EnrollmentCensus {
    pub bindings: Vec<Binding>,
    pub read_bytes: usize,
    pub entries: usize,
    _locks: Vec<File>,
}
impl EnrollmentCensus {
    pub fn lock_count(&self) -> usize { self._locks.len() }
}
fn census_temporary(name: &str) -> bool {
    let Some(body) = name.strip_prefix(".private-").and_then(|s|s.strip_suffix(".tmp")) else { return false; };
    let Some((pid,sequence)) = body.split_once('-') else { return false; };
    !pid.is_empty() && !sequence.is_empty() && pid.bytes().all(|b|b.is_ascii_digit())
        && sequence.bytes().all(|b|b.is_ascii_digit()) && pid.parse::<u32>().is_ok() && sequence.parse::<u64>().is_ok()
}
fn decode_id(value: &str) -> Result<[u8; 32]> {
    ensure!(value.len() == 64 && value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid enrollment census identity");
    let mut result = [0; 32];
    for (i, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[i*2..i*2+2], 16)
            .map_err(|_| anyhow::anyhow!("invalid enrollment census identity"))?;
    }
    ensure!(result != [0; 32], "invalid enrollment census identity");
    Ok(result)
}
/// Exclusive registry and per-ID initialization fences for disposable check admin.
pub struct CandidateAdminRetirement {
    binding: Binding,
    registry_path: PathBuf,
    _registry: File,
    _initializer: File,
}
pub struct Registry {
    root: PrivateDir,
    records: PrivateDir,
    admins: PrivateDir,
}
impl Registry {
    /// Per-user lookup location shared by daemon and CLI. An override must be
    /// explicit and absolute; the current directory is never an authority.
    pub fn default_path() -> Result<PathBuf> {
        if let Some(path) = std::env::var_os("TINYTREE_REGISTRY") {
            let path = PathBuf::from(path);
            normalized(&path)?;
            return Ok(path);
        }
        #[cfg(windows)]
        let base = std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .context("LOCALAPPDATA is required for native workspace registry")?;
        #[cfg(not(windows))]
        let base = match std::env::var_os("XDG_STATE_HOME") {
            Some(path) => PathBuf::from(path),
            None => PathBuf::from(
                std::env::var_os("HOME")
                    .context("HOME or XDG_STATE_HOME is required for native workspace registry")?,
            )
            .join(".local/state"),
        };
        normalized(&base)?;
        Ok(base.join("git-tools/native-workspaces"))
    }
    pub fn create(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let root = PrivateDir::create(path)?;
        // Canonical spelling matters on Windows: all registered administration
        // paths and the controller use the same native drive/prefix form.
        let root = PrivateDir::open_existing(root.path().canonicalize()?)?;
        // Serialize first-use publication with every later header update. Read
        // only after taking this persistent lock: another creator may already
        // have published Ready and enabled client intents while we waited.
        let initializer = root.open_lock("registry.lock")?;
        initializer.lock_exclusive()?;
        match root.read("registry.json", 1024) {
            Ok(_) => return Self::open_existing(root.path()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        ensure!(
            fs::read_dir(root.path())?.try_fold(true, |empty, entry| {
                Ok::<_, std::io::Error>(empty && entry?.file_name() == "registry.lock")
            })?,
            "refusing to adopt unknown native registry state"
        );
        root.replace(
            "registry.json",
            &serde_json::to_vec(&RegistryHeader {
                version: 1,
                ready: false,
                client_intents: false,
            })?,
        )?;
        let records = PrivateDir::create(root.path().join("records"))?;
        let admins = PrivateDir::create(root.path().join("admin"))?;
        root.replace(
            "registry.json",
            &serde_json::to_vec(&RegistryHeader {
                version: 1,
                ready: true,
                client_intents: false,
            })?,
        )?;
        Ok(Arc::new(Self {
            root,
            records,
            admins,
        }))
    }
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let root = PrivateDir::open_existing(path)?;
        let root = PrivateDir::open_existing(root.path().canonicalize()?)?;
        let header: RegistryHeader = serde_json::from_slice(&root.read("registry.json", 1024)?)?;
        ensure!(
            header.version == 1 && header.ready,
            "native registry initialization is incomplete; preserve and inspect its state"
        );
        let records = PrivateDir::open_existing(root.path().join("records"))?;
        let admins = PrivateDir::open_existing(root.path().join("admin"))?;
        Ok(Arc::new(Self {
            root,
            records,
            admins,
        }))
    }
    /// Bounded, read-only enrollment metadata for a stopped host's admission
    /// census. This does not attest a live mount or create initialization state.
    pub fn inspect_enrollments(
        &self,
        endpoint: &Endpoint,
        known_pending: &std::collections::BTreeSet<[u8; 32]>,
        max_entries: usize,
        max_bytes: usize,
        max_locks: usize,
    ) -> Result<EnrollmentCensus> {
        ensure!(max_entries > 0 && max_entries <= 32768 && max_bytes > 0
            && max_bytes <= 64 * 1024 * 1024 && max_locks > 0 && max_locks <= 257,
            "invalid enrollment census bounds");
        let global = self.root.open_existing_file("registry.lock", 4096)?;
        global.try_lock_exclusive().context("native registry initialization is active")?;
        let mut result = EnrollmentCensus {
            bindings: Vec::new(), read_bytes: 0, entries: 0, _locks: vec![global],
        };
        let mut bindings = std::collections::BTreeMap::new();
        let read = |directory: &PrivateDir, name: &std::ffi::OsStr,
                        result: &mut EnrollmentCensus| -> Result<Vec<u8>> {
            result.entries = result.entries.checked_add(1).context("enrollment census count overflow")?;
            ensure!(result.entries <= max_entries, "enrollment census entry limit");
            let cap = MAX_RECORD.min(max_bytes.saturating_sub(result.read_bytes));
            ensure!(cap > 0, "enrollment census byte limit");
            let bytes = directory.read(name, cap)?;
            result.read_bytes = result.read_bytes.checked_add(bytes.len()).and_then(|n| n.checked_add(4096))
                .context("enrollment census byte overflow")?;
            ensure!(result.read_bytes <= max_bytes, "enrollment census byte limit");
            Ok(bytes)
        };
        for item in fs::read_dir(self.records.path())? {
            let item = item?;
            let name = item.file_name();
            let bytes = read(&self.records, &name, &mut result)?;
            if name.to_str().is_some_and(census_temporary) { continue; }
            let binding: Binding = serde_json::from_slice(&bytes)
                .map_err(|_| anyhow::anyhow!("invalid enrollment census record"))?;
            self.validate(&binding)?;
            ensure!(name == std::ffi::OsStr::new(&record_name(&binding.workspace_id)),
                "enrollment census filename differs");
            ensure!(bindings.insert(binding.workspace_id, binding).is_none(),
                "duplicate enrollment census identity");
        }
        for item in fs::read_dir(self.admins.path())? {
            let item = item?;
            result.entries = result.entries.checked_add(1).context("enrollment census count overflow")?;
            ensure!(result.entries <= max_entries && item.file_type()?.is_dir(),
                "unknown enrollment census administration");
            let name = item.file_name().into_string().map_err(|_| anyhow::anyhow!("invalid enrollment census name"))?;
            if name == "command-leases" {
                let leases = PrivateDir::open_existing(item.path())?;
                for lease in fs::read_dir(leases.path())? {
                    result.entries += 1;
                    ensure!(result.entries <= max_entries, "enrollment census entry limit");
                    let name = lease?.file_name().into_string().map_err(|_| anyhow::anyhow!("invalid enrollment lease name"))?;
                    let mount = name.strip_suffix(".lock").context("unknown enrollment lease entry")?;
                    ensure!(!mount.is_empty() && mount.len() <= 128
                        && mount.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)),
                        "invalid enrollment lease name");
                    let _file = leases.open_existing_file(&name, 4096)?;
                }
                continue;
            }
            let id = decode_id(&name)?;
            let admin = PrivateDir::open_existing(item.path())?;
            let journal = match admin.read(JOURNAL, MAX_RECORD.min(max_bytes.saturating_sub(result.read_bytes))) {
                Ok(bytes) => {
                    result.read_bytes = result.read_bytes.checked_add(bytes.len()).and_then(|n| n.checked_add(4096))
                        .context("enrollment census byte overflow")?;
                    ensure!(result.read_bytes <= max_bytes, "enrollment census byte limit");
                    let value: Initialization = serde_json::from_slice(&bytes)
                        .map_err(|_| anyhow::anyhow!("invalid enrollment census initialization"))?;
                    self.validate(&value.binding)?;
                    ensure!(value.version == 1 && value.binding.workspace_id == id
                        && value.initial_commit.len() == 32 && !value.workspace.is_empty(),
                        "invalid enrollment census initialization");
                    Some(value)
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            let Some(journal) = journal else {
                ensure!(known_pending.contains(&id) && !admin.path().join("repo/index").try_exists()?,
                    "unknown unbound native administration; preserve state");
                ensure!(!bindings.contains_key(&id), "ready enrollment has no initialization journal");
                ensure!(result._locks.len() < max_locks, "enrollment census lock limit");
                let lock = self.root.open_existing_file(format!("initialize-{}.lock", hex(&id)),4096)?;
                lock.try_lock_exclusive().context("native workspace initialization is active")?;
                result._locks.push(lock);
                continue;
            };
            if let Some(record) = bindings.get(&id) {
                ensure!(record == &journal.binding, "enrollment census binding differs");
            } else {
                if &journal.binding.endpoint != endpoint { continue; }
                ensure!(known_pending.contains(&id), "orphan native initialization; preserve state");
                bindings.insert(id, journal.binding);
            }
        }
        for binding in bindings.into_values().filter(|binding| &binding.endpoint == endpoint) {
            ensure!(result._locks.len() < max_locks, "enrollment census lock limit");
            let lock = self.root.open_existing_file(format!("initialize-{}.lock", binding.id()), 4096)?;
            lock.try_lock_exclusive().context("native workspace initialization is active")?;
            result._locks.push(lock);
            result.bindings.push(binding);
        }
        Ok(result)
    }
    pub fn path(&self) -> &Path {
        self.root.path()
    }
    /// Compute external paths without creating jj state. The daemon may
    /// provision `admin` and `working_copy` before opening its controller.
    pub fn binding(
        &self,
        workspace_id: [u8; 32],
        mounted_root: &Path,
        repository: String,
        endpoint: LocalEndpoint,
        rpc_mount_id: String,
    ) -> Result<Binding> {
        let root = mounted_root.canonicalize()?;
        let admin = self.admins.path().join(hex(&workspace_id));
        let binding = Binding {
            version: 1,
            workspace_id,
            root,
            repo: admin.join("repo"),
            working_copy: admin.join("working_copy"),
            admin,
            repository,
            endpoint: endpoint.into(),
            rpc_mount_id,
        };
        self.validate(&binding)?;
        Ok(binding)
    }
    /// Only provisioning, not jj initialization. The controller still must
    /// attest a live mount before `initialize()` creates repo or operation data.
    pub fn provision(&self, binding: &Binding) -> Result<()> {
        self.validate(binding)?;
        validate_marker(binding)?;
        let _lock = self.initialization_lock(binding)?;
        let admin = PrivateDir::create(&binding.admin)?;
        ensure!(
            fresh_admin(&admin)?,
            "external administration contains unknown or initialized state"
        );
        let working_copy = PrivateDir::create(&binding.working_copy)?;
        ensure!(
            fs::read_dir(working_copy.path())?.next().is_none(),
            "external working-copy state contains unknown or initialized data"
        );
        Ok(())
    }
    pub fn lookup(&self, root: &Path) -> Result<Binding> {
        let root = root.canonicalize()?;
        let id = read_marker(&root)?;
        let binding: Binding =
            serde_json::from_slice(&self.records.read(record_name(&id), MAX_RECORD)?)?;
        self.validate(&binding)?;
        ensure!(
            binding.workspace_id == id && binding.root == root,
            "native registry/root identity mismatch"
        );
        self.validate_ready(&binding)?;
        Ok(binding)
    }
    /// Finalize a previously completed initialization whose registry write was
    /// interrupted. This never creates, resets, or repairs a jj repository.
    pub fn publish_ready(&self, binding: &Binding) -> Result<()> {
        self.validate(binding)?;
        validate_marker(binding)?;
        let _lock = self.lock()?;
        self.validate_ready(binding)?;
        let name = record_name(&binding.workspace_id);
        match self.records.read(&name, MAX_RECORD) {
            Ok(bytes) => {
                let existing: Binding = serde_json::from_slice(&bytes)?;
                ensure!(
                    existing == *binding,
                    "native registry identity already has another binding"
                );
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.ensure_capacity()?;
        self.records.replace(name, &bounded_encode(binding)?)?;
        Ok(())
    }
    /// Lock only an uninitialized candidate's external administration. A jj
    /// initialization journal, repository, working-copy data, or published
    /// registration is user history and is never eligible for this cleanup.
    pub fn lock_candidate_retirement(&self, binding: &Binding) -> Result<CandidateAdminRetirement> {
        self.validate(binding)?;
        let registry = self.root.open_existing_file("registry.lock", 4096)?;
        registry
            .try_lock_exclusive()
            .context("native registry is busy")?;
        let initializer = self
            .root
            .open_existing_file(format!("initialize-{}.lock", binding.id()), 4096)?;
        initializer
            .try_lock_exclusive()
            .context("native administration is busy")?;
        self.check_candidate_admin(binding)?;
        Ok(CandidateAdminRetirement {
            binding: binding.clone(),
            registry_path: self.root.path().to_owned(),
            _registry: registry,
            _initializer: initializer,
        })
    }
    fn check_candidate_admin(&self, binding: &Binding) -> Result<()> {
        self.validate(binding)?;
        ensure!(
            !self
                .records
                .path()
                .join(record_name(&binding.workspace_id))
                .try_exists()?,
            "published native administration cannot be retired as a candidate"
        );
        if binding.admin.try_exists()? {
            let admin = PrivateDir::open_existing(&binding.admin)?;
            ensure!(
                fresh_admin(&admin)?,
                "initialized or unknown native administration is preserved"
            );
            if binding.working_copy.try_exists()? {
                let wc = PrivateDir::open_existing(&binding.working_copy)?;
                ensure!(
                    fs::read_dir(wc.path())?.next().is_none(),
                    "candidate working-copy data is preserved"
                );
            }
        }
        Ok(())
    }
    /// Called only after durable candidate retirement intent, under the guard.
    /// Idempotent after partial cleanup; never deletes a jj repository/history.
    pub fn remove_candidate_admin(
        &self,
        binding: &Binding,
        guard: &CandidateAdminRetirement,
    ) -> Result<()> {
        ensure!(
            guard.binding == *binding && guard.registry_path == self.root.path(),
            "candidate retirement guard differs"
        );
        self.check_candidate_admin(binding)?;
        self.admins.remove_child_tree(binding.id(), 2, 3)?;
        Ok(())
    }

    fn validate(&self, binding: &Binding) -> Result<()> {
        ensure!(
            binding.version == 1 && binding.workspace_id != [0; 32],
            "invalid native workspace identity/version"
        );
        for path in [
            &binding.root,
            &binding.admin,
            &binding.repo,
            &binding.working_copy,
        ] {
            normalized(path)?;
        }
        let expected = self.admins.path().join(binding.id());
        ensure!(
            binding.admin == expected
                && binding.repo == expected.join("repo")
                && binding.working_copy == expected.join("working_copy"),
            "external jj paths are not owned by this registry"
        );
        ensure!(
            !binding.root.starts_with(self.root.path())
                && !self.root.path().starts_with(&binding.root),
            "native root and administration overlap"
        );
        ensure!(
            !binding.repository.is_empty()
                && binding.repository.len() <= 256
                && binding
                    .repository
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
            "invalid native repository identity"
        );
        ensure!(
            !binding.rpc_mount_id.is_empty()
                && binding.rpc_mount_id.len() <= 128
                && binding
                    .rpc_mount_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)),
            "invalid native RPC mount identity"
        );
        binding
            .endpoint
            .local()
            .validate()
            .map_err(|_| anyhow::anyhow!("invalid private native endpoint"))?;
        if let Endpoint::UnixSocket { path } = &binding.endpoint {
            normalized(path)?;
        }
        Ok(())
    }
    fn validate_ready(&self, binding: &Binding) -> Result<()> {
        let admin = PrivateDir::open_existing(&binding.admin)?;
        let journal: Initialization = serde_json::from_slice(&admin.read(JOURNAL, MAX_RECORD)?)?;
        ensure!(
            journal.version == 1
                && journal.ready
                && journal.binding == *binding
                && journal.initial_commit.len() == 32
                && !journal.workspace.is_empty(),
            "native initialization is incomplete or has another binding; preserve state and reconcile explicitly"
        );
        restore::ready_attachment(binding, &journal)?;
        let wc = PrivateDir::open_existing(&binding.working_copy)?;
        ensure!(
            wc.read("type", 128)? == jj_vfs::native::NAME.as_bytes(),
            "external working copy is not native jj-vfs"
        );
        let _repo = PrivateDir::open_existing(&binding.repo)
            .context("external native repository is unavailable")?;
        Ok(())
    }
    fn ensure_capacity(&self) -> Result<()> {
        for (count, entry) in fs::read_dir(self.records.path())?.enumerate() {
            let _entry = entry?;
            ensure!(
                count < MAX_WORKSPACES - 1,
                "native registry workspace limit reached"
            );
        }
        Ok(())
    }
    fn initialization_lock(&self, binding: &Binding) -> Result<File> {
        let lock = self
            .root
            .open_lock(format!("initialize-{}.lock", binding.id()))?;
        lock.lock_exclusive()?;
        Ok(lock)
    }
    fn lock(&self) -> Result<File> {
        let lock = self.root.open_lock("registry.lock")?;
        lock.lock_exclusive()?;
        Ok(lock)
    }
}

/// Resolve a controller from a trusted registration. Implementations normally
/// select `ClientConnector` using this binding's local endpoint and mount ID.
pub trait ConnectorProvider: Send + Sync {
    fn connector(&self, binding: &Binding) -> Result<Arc<dyn ProjectionConnector>>;
}
impl<F> ConnectorProvider for F
where
    F: Fn(&Binding) -> Result<Arc<dyn ProjectionConnector>> + Send + Sync,
{
    fn connector(&self, binding: &Binding) -> Result<Arc<dyn ProjectionConnector>> {
        self(binding)
    }
}

pub struct LoaderFactory {
    registry: Arc<Registry>,
    connectors: Arc<dyn ConnectorProvider>,
}
impl LoaderFactory {
    pub fn new(registry: Arc<Registry>, connectors: Arc<dyn ConnectorProvider>) -> Self {
        Self {
            registry,
            connectors,
        }
    }
    /// Register this under jj_vfs::native::NAME in CliRunner as well as
    /// installing this loader. Generic jj commands consult that separate map.
    pub fn working_copy_factory(&self) -> Box<dyn WorkingCopyFactory> {
        Box::new(RegisteredWorkingCopyFactory {
            registry: self.registry.clone(),
            connectors: self.connectors.clone(),
        })
    }
}
struct RegisteredWorkingCopyFactory {
    registry: Arc<Registry>,
    connectors: Arc<dyn ConnectorProvider>,
}
impl WorkingCopyFactory for RegisteredWorkingCopyFactory {
    fn init_working_copy(
        &self,
        _: Arc<jj_lib::store::Store>,
        _: PathBuf,
        _: PathBuf,
        _: OperationId,
        _: WorkspaceNameBuf,
        _: &UserSettings,
    ) -> std::result::Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        Err(WorkingCopyStateError {
            message: "native workspace creation requires a daemon-provisioned lazy mount".into(),
            err: std::io::Error::other("ordinary jj workspace add cannot create a native mount")
                .into(),
        })
    }
    fn load_working_copy(
        &self,
        store: Arc<jj_lib::store::Store>,
        root: PathBuf,
        state_path: PathBuf,
        settings: &UserSettings,
    ) -> std::result::Result<Box<dyn WorkingCopy>, WorkingCopyStateError> {
        let load = || -> Result<Box<dyn WorkingCopy>> {
            let binding = self.registry.lookup(&root)?;
            ensure!(
                state_path.canonicalize()? == binding.working_copy,
                "native working-copy state differs from registered administration"
            );
            let connector = self.connectors.connector(&binding)?;
            let factory = NativeVfsWorkingCopyFactory::new(binding.repository, connector);
            Ok(factory.load_working_copy(store, root, state_path, settings)?)
        };
        load().map_err(|error| WorkingCopyStateError {
            message: format!("Native registry working-copy state: {error:#}"),
            err: error.into(),
        })
    }
}
struct Loader {
    binding: Binding,
    factory: NativeVfsWorkingCopyFactory,
}
impl WorkspaceLoaderFactory for LoaderFactory {
    fn create(
        &self,
        root: &Path,
    ) -> std::result::Result<Box<dyn WorkspaceLoader>, WorkspaceLoadError> {
        let load = || -> Result<Box<dyn WorkspaceLoader>> {
            let binding = self.registry.lookup(root)?;
            let connector = self.connectors.connector(&binding)?;
            // This is the mount attestation boundary; a stale registry is not
            // permission to load a materialized or unmounted working copy.
            connector
                .connect(&binding.projection())?
                .inspect()?
                .validate(&binding.repository)?;
            let factory = NativeVfsWorkingCopyFactory::new(binding.repository.clone(), connector);
            Ok(Box::new(Loader { binding, factory }))
        };
        load().map_err(load_error)
    }
}
impl WorkspaceLoader for Loader {
    fn workspace_root(&self) -> &Path {
        &self.binding.root
    }
    fn repo_path(&self) -> &Path {
        &self.binding.repo
    }
    fn get_working_copy_type(&self) -> std::result::Result<String, StoreLoadError> {
        Ok(jj_vfs::native::NAME.into())
    }
    fn load(
        &self,
        settings: &UserSettings,
        stores: &StoreFactories,
        _: &WorkingCopyFactories,
    ) -> std::result::Result<Workspace, WorkspaceLoadError> {
        let repo = RepoLoader::init_from_file_system(settings, &self.binding.repo, stores)?;
        let wc = self.factory.load_working_copy(
            repo.store().clone(),
            self.binding.root.clone(),
            self.binding.working_copy.clone(),
            settings,
        )?;
        Ok(Workspace::new(
            &self.binding.root,
            self.binding.repo.clone(),
            wc,
            repo,
        )?)
    }
}

/// Native initializer with the actual root operation captured from the op-store
/// initializer, before jj first asks the index store for that operation.
pub type RootOperationIndexInitializer<'a> = dyn Fn(
        &UserSettings,
        &Path,
        &OperationId,
    )
        -> std::result::Result<Box<dyn jj_lib::index::IndexStore>, jj_lib::backend::BackendInitError>
    + 'a;

/// Initialize only after the caller has obtained/admitted an initial native
/// commit and started its matching lazy mount. Failures preserve an explicit
/// journal and all partial authoritative state; no cleanup/reset is attempted.
#[allow(clippy::too_many_arguments)]
pub async fn initialize(
    registry: &Registry,
    binding: &Binding,
    settings: &UserSettings,
    backend_initializer: &BackendInitializer<'_>,
    signer: Signer,
    connector: Arc<dyn ProjectionConnector>,
    initial_commit: &CommitId,
    workspace_name: WorkspaceNameBuf,
) -> Result<(Workspace, Arc<ReadonlyRepo>)> {
    let index_initializer = |settings: &UserSettings, path: &Path, _: &OperationId| {
        ReadonlyRepo::default_index_store_initializer()(settings, path)
    };
    initialize_with_index(
        registry,
        binding,
        settings,
        backend_initializer,
        signer,
        connector,
        initial_commit,
        workspace_name,
        &index_initializer,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn initialize_with_index(
    registry: &Registry,
    binding: &Binding,
    settings: &UserSettings,
    backend_initializer: &BackendInitializer<'_>,
    signer: Signer,
    connector: Arc<dyn ProjectionConnector>,
    initial_commit: &CommitId,
    workspace_name: WorkspaceNameBuf,
    index_initializer: &RootOperationIndexInitializer<'_>,
) -> Result<(Workspace, Arc<ReadonlyRepo>)> {
    registry.validate(binding)?;
    validate_marker(binding)?;
    ensure!(
        initial_commit.as_bytes().len() == 32,
        "initial native commit must be 32 bytes"
    );
    let projected = connector.connect(&binding.projection())?.inspect()?;
    projected.validate(&binding.repository)?;
    let _initialization_lock = registry.initialization_lock(binding)?;
    let admin = PrivateDir::create(&binding.admin)?;
    ensure!(
        fresh_admin(&admin)?,
        "external administration already initialized or contains unknown state; preserve it and reconcile explicitly"
    );
    let working_copy = PrivateDir::create(&binding.working_copy)?;
    ensure!(
        fs::read_dir(working_copy.path())?.next().is_none(),
        "native working-copy state is not empty"
    );
    let _lock = admin.open_lock("initialize.lock")?;
    _lock.lock_exclusive()?;
    let mut journal = Initialization {
        version: 1,
        binding: binding.clone(),
        initial_commit: initial_commit.as_bytes().to_vec(),
        workspace: workspace_name.as_str().into(),
        ready: false,
        restore_origin: None,
    };
    admin.replace(JOURNAL, &bounded_encode(&journal)?)?;
    let repo_directory = PrivateDir::create(&binding.repo)?;
    let root_operation = std::cell::RefCell::new(None::<OperationId>);
    let op_initializer: &jj_lib::repo::OpStoreInitializer<'_> = &|settings, path, root_data| {
        let store = ReadonlyRepo::default_op_store_initializer()(settings, path, root_data)?;
        *root_operation.borrow_mut() = Some(store.root_operation_id().clone());
        Ok(store)
    };
    let captured_index_initializer: &jj_lib::repo::IndexStoreInitializer<'_> = &|settings, path| {
        let root = root_operation.borrow().clone().ok_or_else(|| {
            jj_lib::backend::BackendInitError(Box::new(std::io::Error::other(
                "root operation unavailable before native index initialization",
            )))
        })?;
        index_initializer(settings, path, &root)
    };
    let repo = ReadonlyRepo::init(
        settings,
        repo_directory.path(),
        backend_initializer,
        signer,
        ReadonlyRepo::default_workspace_store_initializer(),
        op_initializer,
        ReadonlyRepo::default_op_heads_store_initializer(),
        captured_index_initializer,
        ReadonlyRepo::default_submodule_store_initializer(),
    )
    .await?;
    let base = repo.store().get_commit_async(initial_commit).await?;
    ensure!(
        jj_vfs::native::NativeTree::from_merged_tree(&base.tree())? == projected.tree,
        "initial native commit does not match the mounted projection"
    );
    let mut tx = repo.start_transaction();
    if initial_commit != repo.store().root_commit_id() {
        tx.repo_mut()
            .index_commits(std::slice::from_ref(&base))
            .await?;
        tx.repo_mut().add_heads(std::slice::from_ref(&base)).await?;
    }
    tx.repo_mut()
        .check_out(workspace_name.clone(), &base)
        .await?;
    let repo = tx.commit("initialize native lazy workspace").await?;
    let factory = NativeVfsWorkingCopyFactory::new(binding.repository.clone(), connector);
    let wc = factory.init_working_copy(
        repo.store().clone(),
        binding.root.clone(),
        binding.working_copy.clone(),
        repo.op_id().clone(),
        workspace_name,
        settings,
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
    journal.ready = true;
    admin.replace(JOURNAL, &bounded_encode(&journal)?)?;
    // Different workspace initializations run independently. Only the final
    // bounded registry publication uses the shared registry lock.
    registry.publish_ready(binding)?;
    Ok((workspace, repo))
}

fn fresh_admin(admin: &PrivateDir) -> Result<bool> {
    for entry in fs::read_dir(admin.path())? {
        let entry = entry?;
        if entry.file_name() != "working_copy" || !entry.file_type()?.is_dir() {
            return Ok(false);
        }
    }
    Ok(true)
}
fn normalized(path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute()
            && path.as_os_str().len() <= 4096
            && path.components().all(|component| match component {
                Component::RootDir | Component::Normal(_) => true,
                #[cfg(windows)]
                Component::Prefix(prefix) => matches!(
                    prefix.kind(),
                    std::path::Prefix::Disk(_) | std::path::Prefix::VerbatimDisk(_)
                ),
                _ => false,
            }),
        "native registry paths must be bounded normalized local absolute paths"
    );
    Ok(())
}
fn bounded_encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(
        bytes.len() <= MAX_RECORD,
        "native registry record exceeds limit"
    );
    Ok(bytes)
}
fn read_marker(root: &Path) -> Result<[u8; 32]> {
    let marker = root.join(MARKER);
    let metadata = fs::symlink_metadata(&marker)?;
    ensure!(
        metadata.is_file() && metadata.len() == 64,
        "native projected control marker is invalid"
    );
    let mut bytes = Vec::with_capacity(65);
    File::open(marker)?.take(65).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() == 64
            && bytes
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b)),
        "native projected control marker is not an opaque identity"
    );
    let mut id = [0; 32];
    for (slot, pair) in id.iter_mut().zip(bytes.chunks_exact(2)) {
        *slot = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    ensure!(id != [0; 32], "invalid zero native workspace identity");
    Ok(id)
}
fn validate_marker(binding: &Binding) -> Result<()> {
    ensure!(
        binding.root.canonicalize()? == binding.root
            && read_marker(&binding.root)? == binding.workspace_id,
        "native mount/root marker does not match registration"
    );
    Ok(())
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn record_name(id: &[u8; 32]) -> String {
    format!("{}.json", hex(id))
}
fn load_error(error: anyhow::Error) -> WorkspaceLoadError {
    WorkspaceLoadError::WorkingCopyState(WorkingCopyStateError {
        message: format!("Native external workspace: {error:#}"),
        err: error.into(),
    })
}

#[cfg(test)]
mod tests;
