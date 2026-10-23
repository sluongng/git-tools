//! Daemon ownership of native source projections and their private RPC entries.
//! Persistent records bind exact initialization intent; retries never reset a
//! dirty namespace or adopt an unrelated directory.
use crate::config::Config;
use anyhow::{Context, Result, ensure};
use jj_native_backend::NativeObjectIo;
use local_cas::LocalCas;
use native_broker::{BrokerControlClient, LocalEndpoint};
use native_vfs::NativeMount;
use native_vfs_controller::Controller;
use native_vfs_rpc::ProjectionService;
use native_workspace::Scope;
use native_workspace_admin::{Binding, Registry};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use tinytree_model as m;

const MAX_RECORD: usize = 32 * 1024;

#[derive(Clone)]
pub(crate) struct Repository {
    pub io: Arc<dyn NativeObjectIo>,
    pub scope: Scope,
    pub remote: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Lifecycle {
    Pending,
    Ready,
    Stopped,
    RecoveryRequired,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    workspace_id: [u8; 32],
    store: String,
    root: PathBuf,
    bootstrap_commit: [u8; 32],
    scope: String,
    state: Lifecycle,
}
struct Entry {
    record: Record,
    directory: PrivateDir,
    mount: Option<Arc<NativeMount>>,
    controller: Option<Arc<Controller>>,
    registered: bool,
    // A failed NFS start can own a possibly mounted server. Do not discard it
    // merely because the client disconnected or a later retry arrived.
    provider_failure: Option<anyhow::Error>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mounted {
    pub binding: Binding,
    pub registry_path: PathBuf,
}

pub(crate) struct Manager {
    config: Config,
    directory: PrivateDir,
    registry: Option<Arc<Registry>>,
    cache: Arc<LocalCas>,
    repositories: BTreeMap<String, Repository>,
    service: ProjectionService,
    entries: BTreeMap<String, Entry>,
}
impl Manager {
    pub fn open(
        config: Config,
        cache: Arc<LocalCas>,
        repositories: BTreeMap<String, Repository>,
    ) -> Result<Self> {
        ensure!(
            !repositories.is_empty(),
            "native workspaces require configured repositories"
        );
        let directory = PrivateDir::create(config.state_dir.join("native-workspaces"))?;
        let mut limits = native_vfs_rpc::Limits::default();
        limits.max_mounts = config.native.workspaces.max_workspaces;
        limits.max_policy_paths = config.native.workspaces.max_entries;
        limits.transport = config.native.broker_config(&config.reapi);
        let service = ProjectionService::new(limits)
            .map_err(|_| anyhow::anyhow!("invalid native projection RPC configuration"))?;
        let mut entries = BTreeMap::new();
        for item in fs::read_dir(directory.path())? {
            ensure!(
                entries.len() < config.native.workspaces.max_workspaces,
                "native workspace registry limit exceeded"
            );
            let item = item?;
            ensure!(
                item.file_type()?.is_dir(),
                "unknown native workspace registry entry"
            );
            let name = item
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("invalid native workspace registry name"))?;
            let id = unhex(&name)?;
            let child = PrivateDir::open_existing(item.path())?;
            let record: Record =
                serde_json::from_slice(&child.read("workspace.json", MAX_RECORD)?)?;
            validate_record(&record)?;
            ensure!(
                record.workspace_id == id,
                "native workspace registry identity mismatch"
            );
            ensure!(
                entries
                    .insert(
                        name,
                        Entry {
                            record,
                            directory: child,
                            mount: None,
                            controller: None,
                            registered: false,
                            provider_failure: None
                        }
                    )
                    .is_none(),
                "duplicate native workspace registry identity"
            );
        }
        Ok(Self {
            config,
            directory,
            registry: None,
            cache,
            repositories,
            service,
            entries,
        })
    }
    pub fn rpc(&self) -> native_vfs_rpc::RpcService {
        self.service.rpc_service()
    }
    pub fn active(&self) -> bool {
        self.entries
            .values()
            .any(|entry| entry.mount.is_some() || entry.provider_failure.is_some())
    }
    pub fn status(&self) -> serde_json::Value {
        serde_json::json!(self.entries.iter().map(|(id,entry)| serde_json::json!({"workspace_id":id,"root":entry.record.root,"store":entry.record.store,"state":entry.record.state,"active":entry.mount.is_some(),"provider_recovery_owned":entry.provider_failure.is_some()})).collect::<Vec<_>>())
    }
    pub fn mounted(&self, id: &str) -> Result<bool> {
        let _ = unhex(id)?;
        Ok(self.entries.get(id).is_some_and(|entry| {
            entry.mount.is_some()
                && entry.controller.is_some()
                && entry.registered
                && entry.record.state == Lifecycle::Ready
        }))
    }
    /// Restore only records acknowledged as ready by this daemon. Pending,
    /// stopped, and recovery-required records require an explicit exact retry.
    pub async fn restore_ready(&mut self) {
        let restore: Vec<_> = self
            .entries
            .values()
            .filter(|entry| entry.record.state == Lifecycle::Ready)
            .map(|entry| entry.record.clone())
            .collect();
        for record in restore {
            let id = hex(&record.workspace_id);
            if let Err(error) = self
                .create(
                    record.root,
                    record.store,
                    hex(&record.bootstrap_commit),
                    id.clone(),
                )
                .await
            {
                if let Some(entry) = self.entries.get_mut(&id) {
                    entry.record.state = Lifecycle::RecoveryRequired;
                    let _ = persist(entry);
                }
                // Paths, credentials, endpoints, and provider errors remain in
                // private state; status reports only the lifecycle disposition.
                let _ = error;
                eprintln!("native workspace requires explicit recovery");
            }
        }
    }
    pub async fn create(
        &mut self,
        root: PathBuf,
        store: String,
        commit: String,
        workspace_id: String,
    ) -> Result<Mounted> {
        let id = unhex(&workspace_id)?;
        ensure!(id != [0; 32], "native workspace identity must be nonzero");
        let commit = unhex(&commit)?;
        normalized(&root)?;
        let root = canonical_candidate(&root)?;
        let repository = self
            .repositories
            .get(&store)
            .context("native repository alias is not configured")?
            .clone();
        let registry = self.open_registry()?;
        let record = Record {
            version: 1,
            workspace_id: id,
            root: root.clone(),
            store: store.clone(),
            bootstrap_commit: commit,
            scope: repository.scope.backend().into(),
            state: Lifecycle::Pending,
        };
        if let Some(entry) = self.entries.get(&workspace_id) {
            ensure!(
                same_intent(&entry.record, &record),
                "native workspace identity already has a different initialization intent"
            );
            ensure!(
                entry.provider_failure.is_none(),
                "native provider recovery is still owned; stop it explicitly before retrying"
            );
            if entry.mount.is_some() {
                ensure!(
                    entry.record.state == Lifecycle::Ready
                        && entry.controller.is_some()
                        && entry.registered,
                    "native workspace initialization requires explicit recovery"
                );
                // A previous Ready write may have failed after publication.
                // Never acknowledge a live retry until its restart record is durable.
                persist(entry)?;
                return self.response(&record);
            }
        } else {
            ensure!(
                self.entries.len() < self.config.native.workspaces.max_workspaces,
                "native workspace limit reached"
            );
            ensure!(
                !self
                    .entries
                    .values()
                    .any(|entry| overlaps(&entry.record.root, &root)),
                "native workspace roots overlap"
            );
            let child = PrivateDir::create(self.directory.path().join(&workspace_id))?;
            ensure!(
                fs::read_dir(child.path())?.next().is_none(),
                "unknown native workspace state; refusing to adopt or reset it"
            );
            let entry = Entry {
                record: record.clone(),
                directory: child,
                mount: None,
                controller: None,
                registered: false,
                provider_failure: None,
            };
            persist(&entry)?;
            self.entries.insert(workspace_id.clone(), entry);
        }
        // Remote admission is routed through the broker's global resources and
        // exact configured alias, not invoked as an uncharged provider call.
        if commit != [0; 32] {
            ensure!(
                repository.remote,
                "nonempty native clone requires configured admission control"
            );
            let client = BrokerControlClient::connect(
                self.config.state_dir.join("rbe.sock"),
                store.clone(),
                self.config.native.broker_config(&self.config.reapi),
            )
            .await
            .map_err(|_| anyhow::anyhow!("native admission broker is unavailable"))?;
            client
                .admit_commit(m::CommitId(commit))
                .await
                .map_err(|_| {
                    anyhow::anyhow!("native commit admission failed; private state is retained")
                })?;
        }
        let root_merge = if commit == [0; 32] {
            let empty = m::encode(&m::Object::Tree(m::Tree::default()), m::Limits::default())?;
            m::TreeMerge {
                terms: vec![m::TreeId(empty.descriptor.key.id.0)],
                labels: vec![String::new()],
            }
        } else {
            let key = m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(commit),
            };
            let metadata = repository
                .io
                .read_metadata(key)
                .await
                .map_err(|_| anyhow::anyhow!("native bootstrap commit is unavailable"))?;
            ensure!(
                metadata.descriptor().key == key,
                "native bootstrap metadata identity mismatch"
            );
            let m::Object::Commit(value) = metadata.into_object() else {
                anyhow::bail!("native bootstrap metadata has the wrong kind");
            };
            value.root_tree
        };
        let io = repository.io.clone();
        let scope = repository.scope.clone();
        let settings = &self.config.native.workspaces;
        let mut compiler_limits = native_workspace::Limits::default();
        compiler_limits.max_entries = settings.max_entries;
        compiler_limits.max_metadata_bytes = settings.max_metadata_bytes as u64;
        compiler_limits.max_file_bytes = settings.max_file_bytes;
        let mut source_limits = output_core::SourceLimits::default();
        source_limits.max_entries = settings.max_entries;
        source_limits.max_metadata_bytes = settings.max_metadata_bytes;
        source_limits.max_local_bytes = settings.max_snapshot_bytes;
        let child = &self.entries.get(&workspace_id).unwrap().directory;
        let config = native_vfs::Config {
            root: root.clone(),
            state: child.path().join("mount"),
            workspace_identity: id,
            scope,
            root_merge,
            compiler_limits,
            source_limits: source_limits.clone(),
        };
        let prepared = match native_vfs::prepare(config, io.clone(), self.cache.clone()).await {
            Ok(prepared) => prepared,
            Err(error) => {
                self.failed(&workspace_id)?;
                return Err(
                    error.context("native workspace metadata preparation failed; state retained")
                );
            }
        };
        let mount = match native_vfs::mount_prepared(prepared).await {
            Ok(mount) => Arc::new(mount),
            Err(error) => {
                let entry = self.entries.get_mut(&workspace_id).unwrap();
                entry.record.state = Lifecycle::RecoveryRequired;
                // Ownership precedes the fallible journal write: a full disk
                // must not drop a possibly mounted provider hidden in error.
                #[cfg(target_os = "macos")]
                if !error.is::<output_nfs::NfsStartError>() {
                    // The NFS adapter wraps every failure after acquiring a
                    // provider in NfsStartError. Earlier errors own no mount.
                    persist(entry)?;
                    return Err(error.context("native provider failed before acquiring a mount"));
                }
                entry.provider_failure = Some(error);
                persist(entry)?;
                anyhow::bail!(
                    "native provider start requires explicit recovery; ownership retained"
                );
            }
        };
        // Retain the live mount BEFORE any following fallible operation.
        self.entries.get_mut(&workspace_id).unwrap().mount = Some(mount.clone());
        let binding = self.binding(&record)?;
        let provision = match registry.lookup(&binding.root) {
            Ok(existing) => {
                ensure!(
                    existing == binding,
                    "native external registration belongs to another workspace"
                );
                Ok(())
            }
            Err(_) => registry.provision(&binding),
        };
        if let Err(error) = provision {
            self.failed(&workspace_id)?;
            return Err(error.context(
                "native external administration requires recovery; live provider retained",
            ));
        }
        let mut controller_config =
            native_vfs_controller::Config::new(store, binding.working_copy.clone());
        controller_config.source_limits = source_limits;
        controller_config.max_file_bytes = settings.max_file_bytes;
        controller_config.max_snapshot_bytes = settings.max_snapshot_bytes;
        let controller = match Controller::open(mount, io, controller_config).await {
            Ok(controller) => Arc::new(controller),
            Err(error) => {
                self.failed(&workspace_id)?;
                return Err(
                    error.context("native controller startup failed; live provider retained")
                );
            }
        };
        self.entries.get_mut(&workspace_id).unwrap().controller = Some(controller.clone());
        if self
            .service
            .register(workspace_id.clone(), controller, binding.projection())
            .is_err()
        {
            self.failed(&workspace_id)?;
            anyhow::bail!("native projection registration failed; provider retained for recovery");
        }
        let entry = self.entries.get_mut(&workspace_id).unwrap();
        entry.registered = true;
        entry.record.state = Lifecycle::Ready;
        persist(entry)?;
        self.response(&record)
    }
    fn open_registry(&mut self) -> Result<Arc<Registry>> {
        if let Some(registry) = &self.registry {
            return Ok(registry.clone());
        }
        let path = self
            .config
            .native
            .workspaces
            .registry_path
            .clone()
            .map(Ok)
            .unwrap_or_else(Registry::default_path)?;
        private_parents(&path)?;
        let registry = Registry::create(path)?;
        self.registry = Some(registry.clone());
        Ok(registry)
    }
    fn failed(&mut self, id: &str) -> Result<()> {
        let entry = self
            .entries
            .get_mut(id)
            .context("native workspace record missing")?;
        entry.record.state = Lifecycle::RecoveryRequired;
        persist(entry)
    }
    fn binding(&self, record: &Record) -> Result<Binding> {
        self.registry
            .as_ref()
            .context("native registry is not initialized")?
            .binding(
                record.workspace_id,
                &record.root,
                record.store.clone(),
                LocalEndpoint::UnixSocket(self.config.state_dir.join("rbe.sock")),
                hex(&record.workspace_id),
            )
    }
    fn response(&self, record: &Record) -> Result<Mounted> {
        Ok(Mounted {
            binding: self.binding(record)?,
            registry_path: self
                .registry
                .as_ref()
                .context("native registry is not initialized")?
                .path()
                .to_owned(),
        })
    }
    pub async fn stop(&mut self, id: &str) -> Result<()> {
        let _ = unhex(id)?;
        let entry = self
            .entries
            .get_mut(id)
            .context("native workspace is not registered")?;
        if let Some(error) = &entry.provider_failure {
            #[cfg(target_os = "macos")]
            {
                use output_core::Projection;
                if let Some(start) = error.downcast_ref::<output_nfs::NfsStartError>() {
                    start.recovery().shutdown().await?;
                    entry.provider_failure = None;
                } else {
                    anyhow::bail!(
                        "native provider recovery requires platform-specific inspection; ownership retained"
                    );
                }
            }
            #[cfg(not(target_os = "macos"))]
            {
                // Linux start has no recoverable live-session error. Dropping
                // its error follows an explicit same-user stop, never a retry.
                let _ = error;
                entry.provider_failure = None;
            }
        }
        if entry.registered {
            self.service
                .unregister(id)
                .await
                .map_err(|_| anyhow::anyhow!("native RPC sessions have not drained"))?;
            entry.registered = false;
        }
        if let Some(mount) = &entry.mount {
            if let Err(error) = mount.shutdown().await {
                if let Some(controller) = &entry.controller {
                    use jj_vfs::native::ProjectionConnector;
                    let binding = self
                        .registry
                        .as_ref()
                        .context("native registry is not initialized")?
                        .binding(
                            entry.record.workspace_id,
                            &entry.record.root,
                            entry.record.store.clone(),
                            LocalEndpoint::UnixSocket(self.config.state_dir.join("rbe.sock")),
                            id.into(),
                        )?;
                    if controller.connect(&binding.projection()).is_ok() {
                        entry.registered = self
                            .service
                            .register(id, controller.clone(), binding.projection())
                            .is_ok();
                    }
                }
                entry.record.state = Lifecycle::RecoveryRequired;
                persist(entry)?;
                return Err(error.context("native provider remains owned after unsuccessful stop"));
            }
        }
        entry.controller = None;
        entry.mount = None;
        entry.record.state = Lifecycle::Stopped;
        persist(entry)
    }
    pub async fn stop_all(&mut self) -> Result<()> {
        let ids: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.mount.is_some() || entry.provider_failure.is_some())
            .map(|(id, _)| id.clone())
            .collect();
        let mut failed = false;
        for id in ids {
            failed |= self.stop(&id).await.is_err();
        }
        ensure!(
            !failed && !self.active(),
            "native provider recovery remains owned"
        );
        Ok(())
    }
    pub async fn drain(&self) -> Result<()> {
        ensure!(
            !self.active(),
            "stop active native workspaces before daemon shutdown"
        );
        self.service
            .drain()
            .await
            .map_err(|_| anyhow::anyhow!("native projection RPCs did not drain"))
    }
}
fn persist(entry: &Entry) -> Result<()> {
    entry
        .directory
        .replace("workspace.json", &serde_json::to_vec(&entry.record)?)?;
    Ok(())
}
fn same_intent(a: &Record, b: &Record) -> bool {
    a.version == b.version
        && a.workspace_id == b.workspace_id
        && a.store == b.store
        && a.root == b.root
        && a.bootstrap_commit == b.bootstrap_commit
        && a.scope == b.scope
}
fn validate_record(record: &Record) -> Result<()> {
    ensure!(
        record.version == 1 && record.workspace_id != [0; 32],
        "invalid native workspace record"
    );
    normalized(&record.root)?;
    ensure!(
        !record.store.is_empty() && record.store.len() <= 64,
        "invalid native workspace alias"
    );
    ensure!(
        record.scope.starts_with("native-workspace-v1:") && record.scope.len() <= 128,
        "invalid native workspace scope"
    );
    Ok(())
}
fn normalized(path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute()
            && path.as_os_str().len() <= 4096
            && path
                .components()
                .all(|part| matches!(part, Component::RootDir | Component::Normal(_))),
        "native workspace root must be a bounded normalized absolute path"
    );
    Ok(())
}
fn canonical_candidate(path: &Path) -> Result<PathBuf> {
    normalized(path)?;
    Ok(if path.try_exists()? {
        path.canonicalize()?
    } else {
        path.parent()
            .context("native root has no parent")?
            .canonicalize()?
            .join(path.file_name().context("native root has no name")?)
    })
}
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}
fn unhex(text: &str) -> Result<[u8; 32]> {
    ensure!(
        text.len() == 64
            && text
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "native identities require 64 lowercase hexadecimal digits"
    );
    let mut value = [0; 32];
    for (slot, pair) in value.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        *slot = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(value)
}
fn hex(value: &[u8]) -> String {
    value.iter().map(|b| format!("{b:02x}")).collect()
}
fn private_parents(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut parent = path.parent().context("native registry has no parent")?;
    while !parent.try_exists()? {
        missing.push(parent.to_owned());
        parent = parent
            .parent()
            .context("native registry has no existing ancestor")?;
    }
    let mut owners = Vec::new();
    for path in missing.into_iter().rev() {
        owners.push(PrivateDir::create(path)?);
    }
    Ok(())
}
