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
    pub local_objects: Arc<jj_native_backend::DirectObjectIo>,
    pub(crate) upload: Option<Arc<crate::native_capture_upload::Source>>,
    pub query: Option<Arc<dyn native_broker::NativeQueryIo>>,
    pub io: Arc<dyn NativeObjectIo>,
    pub scope: Scope,
    pub remote: bool,
    pub identity: [u8; 32],
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
    origin: Origin,
    scope: String,
    state: Lifecycle,
    #[serde(default)]
    native_mount: Option<crate::recovery::NativeMountProof>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Origin {
    Clone {
        bootstrap_commit: [u8; 32],
    },
    Restore {
        source: native_workspace_admin::restore::RestoreSourceIdentity,
        seed: tinytree_cloud::PortableWorkingCopy,
        committed: bool,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyRecord {
    version: u32,
    workspace_id: [u8; 32],
    store: String,
    root: PathBuf,
    bootstrap_commit: [u8; 32],
    scope: String,
    state: Lifecycle,
    #[serde(default)]
    native_mount: Option<crate::recovery::NativeMountProof>,
}
fn read_record(bytes: &[u8]) -> Result<Record> {
    #[derive(Deserialize)]
    struct Version {
        version: u32,
    }
    match serde_json::from_slice::<Version>(bytes)?.version {
        1 => {
            let old: LegacyRecord = serde_json::from_slice(bytes)?;
            ensure!(old.version == 1, "invalid legacy native record");
            Ok(Record {
                version: 2,
                workspace_id: old.workspace_id,
                store: old.store,
                root: old.root,
                origin: Origin::Clone {
                    bootstrap_commit: old.bootstrap_commit,
                },
                scope: old.scope,
                state: old.state,
                native_mount: old.native_mount,
            })
        }
        2 => Ok(serde_json::from_slice(bytes)?),
        _ => anyhow::bail!("unsupported native workspace record version"),
    }
}
struct Entry {
    record: Record,
    directory: PrivateDir,
    mount: Option<Arc<NativeMount>>,
    controller: Option<Arc<Controller>>,
    registered: bool,
    startup: Option<native_vfs::StartupNativeMount>,
    command_startup: Option<crate::native_commands::CommandStartupReservation>,
    restore_intent: Option<native_workspace_admin::restore::RestoreIntent>,
    restore_receipt: Option<native_workspace_admin::restore::StagedRestoreReceipt>,
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
    commands: Option<Arc<crate::native_commands::NativeCommandHost>>,
    #[cfg(test)]
    before_provider_start: Option<Box<dyn FnOnce(&Path) -> Result<()> + Send + Sync>>,
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
            let record = read_record(&child.read("workspace.json", MAX_RECORD)?)?;
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
                            startup: None,
                            command_startup: None,
                            restore_intent: None,
                            restore_receipt: None,
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
            commands: None,
            #[cfg(test)]
            before_provider_start: None,
        })
    }
    pub(crate) fn with_commands(
        mut self,
        commands: Arc<crate::native_commands::NativeCommandHost>,
    ) -> Result<Self> {
        self.service
            .set_command_host(commands.clone())
            .map_err(|_| anyhow::anyhow!("cannot configure native command host"))?;
        self.commands = Some(commands);
        Ok(self)
    }
    pub fn rpc(&self) -> native_vfs_rpc::RpcService {
        self.service.rpc_service()
    }
    pub fn active(&self) -> bool {
        self.entries
            .values()
            .any(|entry| entry.mount.is_some() || entry.provider_failure.is_some())
    }
    pub fn command_status(&self) -> serde_json::Value {
        self.commands
            .as_ref()
            .map(|commands| commands.status())
            .unwrap_or(serde_json::Value::Null)
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
            if let Err(error) = self.resume(&id).await {
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
            version: 2,
            workspace_id: id,
            root: root.clone(),
            store: store.clone(),
            origin: Origin::Clone {
                bootstrap_commit: commit,
            },
            scope: repository.scope.backend().into(),
            state: Lifecycle::Pending,
            native_mount: None,
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
                startup: None,
                command_startup: None,
                restore_intent: None,
                restore_receipt: None,
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
        // Command identity must exist before a native start can acquire a
        // recoverable provider. A later start/proof/persist failure still needs
        // stop() to distinguish an idle known lease from an unreadable one.
        // Providers already create this leaf mountpoint; no checkout/admin is
        // initialized here and existing directory contents are never reset.
        if !root.try_exists()? {
            let _root = PrivateDir::create(&root)?;
        }
        ensure!(
            fs::metadata(&root)?.is_dir(),
            "native mountpoint is not a directory"
        );
        let binding = self.binding(&record)?;
        if let Some(commands) = &self.commands {
            commands.register(&binding, repository.identity).await?;
        }
        #[cfg(test)]
        if let Some(before) = self.before_provider_start.take() {
            before(&root)?;
        }
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
        #[cfg(target_os = "linux")]
        {
            let namespace_identity = mount.tree().identity().to_owned();
            let proof = match crate::recovery::NativeMountProof::capture(
                &record.root,
                &native_source_name(record.workspace_id),
                &namespace_identity,
            ) {
                Ok(proof) => proof,
                Err(error) => {
                    self.failed(&workspace_id)?;
                    return Err(error
                        .context("native kernel identity capture failed; live provider retained"));
                }
            };
            let entry = self.entries.get_mut(&workspace_id).unwrap();
            entry.record.native_mount = Some(proof);
            // Acknowledgement is forbidden until the exact kernel evidence is
            // durable. A crash before this write requires manual inspection.
            persist(entry)?;
        }
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
            .register(
                workspace_id.clone(),
                controller.clone(),
                binding.projection(),
            )
            .is_err()
        {
            self.failed(&workspace_id)?;
            anyhow::bail!("native projection registration failed; provider retained for recovery");
        }
        if let Some(commands) = &self.commands {
            let endpoint = self.config.state_dir.join("rbe.sock");
            commands.set_capture_target(
                &workspace_id,
                Some(Arc::new(crate::native_capture::Target {
                    controller: Arc::downgrade(&controller),
                    registry: registry.clone(),
                    binding: binding.clone(),
                    local: repository.local_objects.clone(),
                    index: jj_native_index::Config {
                        endpoint: jj_native_index::Endpoint::UnixSocket(endpoint.clone()),
                        store_alias: binding.repository.clone(),
                        provider_fingerprint: repository.identity,
                    },
                    remote_scope: native_workspace_admin::remote_state::RemoteScope::new(
                        endpoint,
                        binding.repository.clone(),
                    )?,
                })),
            )?;
        }
        let entry = self.entries.get_mut(&workspace_id).unwrap();
        entry.registered = true;
        entry.record.state = Lifecycle::Ready;
        persist(entry)?;
        self.response(&record)
    }
    /// Resume the exact tagged origin; a restored origin never re-enters clone
    /// admission or reconstructs its workspace from a current public reference.
    pub async fn resume_root(&mut self, root: PathBuf) -> Result<Mounted> {
        let root = canonical_candidate(&root)?;
        let ids: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.record.root == root)
            .map(|(id, _)| id.clone())
            .collect();
        ensure!(
            ids.len() == 1,
            "native resume requires one exact saved destination root"
        );
        self.resume(&ids[0]).await
    }
    pub async fn resume(&mut self, id: &str) -> Result<Mounted> {
        let _ = unhex(id)?;
        let record = self
            .entries
            .get(id)
            .context("native workspace is not registered")?
            .record
            .clone();
        match &record.origin {
            Origin::Clone { bootstrap_commit } => {
                self.create(record.root, record.store, hex(bootstrap_commit), id.into())
                    .await
            }
            Origin::Restore { source, .. } => {
                self.restore(
                    record.root,
                    record.store,
                    id.into(),
                    selection_from_identity(source),
                )
                .await
            }
        }
    }
    pub async fn restore(
        &mut self,
        root: PathBuf,
        store: String,
        workspace_id: String,
        selection: crate::control::NativeRestoreSelection,
    ) -> Result<Mounted> {
        let mut started = false;
        let result = self
            .restore_inner(root, store, &workspace_id, selection, &mut started)
            .await;
        if result.is_err() && started && self.entries.contains_key(&workspace_id) {
            // Never let an error discard an acquired startup fence/controller.
            // The private lifecycle write may itself fail; ownership stays live.
            let _ = self.failed(&workspace_id);
        }
        result
    }
    async fn restore_inner(
        &mut self,
        root: PathBuf,
        store: String,
        workspace_id: &str,
        selection: crate::control::NativeRestoreSelection,
        started: &mut bool,
    ) -> Result<Mounted> {
        #[cfg(not(target_os = "linux"))]
        anyhow::bail!("startup-gated native restore requires Linux; no checkout was created");
        let id = unhex(workspace_id)?;
        ensure!(id != [0; 32], "native workspace identity must be nonzero");
        validate_selection(&selection)?;
        normalized(&root)?;
        let root = canonical_candidate(&root)?;
        let repository = self
            .repositories
            .get(&store)
            .context("native repository alias is not configured")?
            .clone();
        let source = repository
            .upload
            .clone()
            .context("native restore requires a configured remote provider")?;
        let query = repository
            .query
            .clone()
            .context("native restore requires configured native queries")?;
        let commands = self
            .commands
            .clone()
            .context("native restore requires managed command admission")?;
        let settings = self.config.native.workspaces.clone();
        let existing = self
            .entries
            .get(workspace_id)
            .map(|entry| entry.record.clone());
        if let Some(record) = &existing {
            ensure!(
                record.root == root
                    && record.store == store
                    && record.scope == repository.scope.backend(),
                "native restore destination identity differs"
            );
            let Origin::Restore { source: saved, .. } = &record.origin else {
                anyhow::bail!("native destination belongs to a clone origin");
            };
            ensure!(
                selection_from_identity(saved) == selection
                    && saved.provider_identity() == repository.identity,
                "native restore source/provider differs from saved intent"
            );
            let entry = self.entries.get(workspace_id).unwrap();
            ensure!(
                entry.provider_failure.is_none(),
                "native provider recovery remains owned; stop it explicitly"
            );
            if entry.mount.is_some() {
                ensure!(
                    entry.record.state == Lifecycle::Ready
                        && entry.controller.is_some()
                        && entry.registered
                        && entry.startup.is_none(),
                    "native restore remains startup-gated; stop it before an exact retry"
                );
                persist(entry)?;
                return self.response(record);
            }
        } else {
            ensure!(
                self.entries.len() < settings.max_workspaces,
                "native workspace limit reached"
            );
            ensure!(
                !self
                    .entries
                    .values()
                    .any(|entry| overlaps(&entry.record.root, &root)),
                "native workspace roots overlap"
            );
        }
        let provider = native_workspace_admin::restore::RestoreProvider {
            config: jj_native_index::Config {
                endpoint: jj_native_index::Endpoint::UnixSocket(
                    self.config.state_dir.join("rbe.sock"),
                ),
                store_alias: store.clone(),
                provider_fingerprint: repository.identity,
            },
            index_limits: Default::default(),
            query,
            objects: source.io.clone(),
        };
        let limits = crate::native_restore_source::Limits::default();
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(limits.restore.timeout_ms);
        let mut prepared = None;
        let mut current = false;
        if let Some(record) = &existing {
            self.open_existing_registry()?;
            let binding = self.binding(record)?;
            if let Some(eligible) = self
                .registry
                .as_ref()
                .unwrap()
                .restore_resume_eligibility(&binding, repository.identity)?
            {
                let Origin::Restore { source: saved, .. } = &record.origin else {
                    unreachable!()
                };
                ensure!(
                    eligible.binding == binding && eligible.source == *saved,
                    "native restored eligibility/source differs"
                );
                current = true;
            } else if matches!(
                &record.origin,
                Origin::Restore {
                    committed: true,
                    ..
                }
            ) {
                anyhow::bail!(
                    "committed native restore has incomplete eligibility; preserve state"
                );
            }
        }
        let mut record = if current {
            existing.clone().unwrap()
        } else {
            let collected = crate::native_restore_source::collect(
                source.clone(),
                provider.clone(),
                crate::native_restore_source::Selection {
                    selector: tinytree_client::RetainedVersionSelector {
                        workspace_id: selection.workspace_id,
                        generation: selection.generation,
                        manifest_id: tinytree_client::CloudManifestId(selection.manifest_id),
                    },
                    manifest: tinytree_client::CloudRawDigest {
                        blake3: selection.manifest_blake3,
                        size_bytes: selection.manifest_size_bytes,
                    },
                },
                limits,
                Arc::new(()),
                deadline,
            )
            .await?;
            let identity = collected.source_identity();
            let seed = collected.projection_seed();
            let _ = restore_root(seed)?; // Unsupported merges fail before destination writes.
            if let Some(record) = &existing {
                let Origin::Restore {
                    source,
                    seed: saved,
                    ..
                } = &record.origin
                else {
                    unreachable!()
                };
                ensure!(
                    *source == identity && saved == seed,
                    "native restore captured source changed"
                );
            }
            let record = Record {
                version: 2,
                workspace_id: id,
                store: store.clone(),
                root: root.clone(),
                origin: Origin::Restore {
                    source: identity,
                    seed: seed.clone(),
                    committed: false,
                },
                scope: repository.scope.backend().into(),
                state: Lifecycle::Pending,
                native_mount: existing.as_ref().and_then(|r| r.native_mount.clone()),
            };
            prepared = Some(collected);
            record
        };
        let root_merge = match &record.origin {
            Origin::Restore { seed, .. } => restore_root(seed)?,
            _ => unreachable!(),
        };
        let registry = if existing.is_some() {
            self.open_existing_registry()?
        } else {
            self.open_registry()?
        };
        *started = true;
        if existing.is_none() {
            let child = PrivateDir::create(self.directory.path().join(workspace_id))?;
            ensure!(
                fs::read_dir(child.path())?.next().is_none(),
                "unknown native restore state; preserve it"
            );
            let entry = Entry {
                record: record.clone(),
                directory: child,
                mount: None,
                controller: None,
                registered: false,
                startup: None,
                command_startup: None,
                restore_intent: None,
                restore_receipt: None,
                provider_failure: None,
            };
            persist(&entry)?;
            self.entries.insert(workspace_id.into(), entry);
        } else {
            record.state = Lifecycle::Pending;
            let entry = self.entries.get_mut(workspace_id).unwrap();
            entry.record = record.clone();
            persist(entry)?;
        }
        if !root.try_exists()? {
            let _root = PrivateDir::create(&root)?;
        }
        ensure!(
            fs::metadata(&root)?.is_dir(),
            "native mountpoint is not a directory"
        );
        let binding = self.binding(&record)?;
        if self
            .entries
            .get(workspace_id)
            .unwrap()
            .command_startup
            .is_none()
        {
            let reservation = commands
                .register_reserved(&binding, repository.identity)
                .await?;
            self.entries.get_mut(workspace_id).unwrap().command_startup = Some(reservation);
        }
        let mut compiler_limits = native_workspace::Limits::default();
        compiler_limits.max_entries = settings.max_entries;
        compiler_limits.max_metadata_bytes = settings.max_metadata_bytes as u64;
        compiler_limits.max_file_bytes = settings.max_file_bytes;
        let mut source_limits = output_core::SourceLimits::default();
        source_limits.max_entries = settings.max_entries;
        source_limits.max_metadata_bytes = settings.max_metadata_bytes;
        source_limits.max_local_bytes = settings.max_snapshot_bytes;
        let state = self
            .entries
            .get(workspace_id)
            .unwrap()
            .directory
            .path()
            .join("mount");
        let config = native_vfs::Config {
            root: root.clone(),
            state: state.clone(),
            workspace_identity: id,
            scope: repository.scope.clone(),
            root_merge,
            compiler_limits,
            source_limits: source_limits.clone(),
        };
        let workspace = if current || state.try_exists()? {
            native_vfs::prepare_existing(config, repository.io.clone(), self.cache.clone()).await?
        } else {
            native_vfs::prepare(config, repository.io.clone(), self.cache.clone()).await?
        };
        let startup = match native_vfs::mount_prepared_for_restore(workspace).await {
            Ok(startup) => startup,
            Err(error) => {
                let entry = self.entries.get_mut(workspace_id).unwrap();
                entry.provider_failure = Some(error);
                entry.record.state = Lifecycle::RecoveryRequired;
                persist(entry)?;
                anyhow::bail!("native gated provider start requires recovery; ownership retained")
            }
        };
        let mount = startup.mount().clone();
        {
            let entry = self.entries.get_mut(workspace_id).unwrap();
            entry.mount = Some(mount.clone());
            entry.startup = Some(startup);
        }
        #[cfg(target_os = "linux")]
        {
            let proof = crate::recovery::NativeMountProof::capture(
                &record.root,
                &native_source_name(id),
                mount.tree().identity(),
            )?;
            let entry = self.entries.get_mut(workspace_id).unwrap();
            entry.record.native_mount = Some(proof);
            persist(entry)?;
        }
        if !current && !binding.admin.try_exists()? {
            let mut intent = registry.begin_restore(
                &binding,
                prepared
                    .as_ref()
                    .context("native restore source is unavailable")?,
            )?;
            intent.prepare_working_copy_directory()?;
            self.entries.get_mut(workspace_id).unwrap().restore_intent = Some(intent);
        }
        let mut controller_config =
            native_vfs_controller::Config::new(store.clone(), binding.working_copy.clone());
        controller_config.source_limits = source_limits;
        controller_config.max_file_bytes = settings.max_file_bytes;
        controller_config.max_snapshot_bytes = settings.max_snapshot_bytes;
        let controller = Arc::new(
            Controller::open(mount.clone(), repository.io.clone(), controller_config).await?,
        );
        self.entries.get_mut(workspace_id).unwrap().controller = Some(controller.clone());
        let metadata = restore_locator(self.config.state_dir.join("rbe.sock"), store.clone());
        if current {
            validate_current_restore(&binding, &repository, &controller, &provider.config).await?;
            (metadata.1)(&binding.repo.join("store"))?;
            registry.publish_ready(&binding)?;
            let resumed = registry.restored_resume_info(&root, repository.identity)?;
            let Origin::Restore { source, .. } = &record.origin else {
                unreachable!()
            };
            ensure!(
                resumed.binding == binding && resumed.source == *source,
                "native restored current origin differs"
            );
        } else {
            let restore = prepared
                .take()
                .context("native restore source was consumed")?;
            let receipt = if let Some(intent) = self
                .entries
                .get_mut(workspace_id)
                .unwrap()
                .restore_intent
                .take()
            {
                native_workspace_admin::restore::initialize_restored(
                    intent,
                    restore,
                    native_workspace_admin::restore::RestoreHost {
                        settings: restore_settings()?,
                        signer: jj_lib::signing::Signer::new(None, vec![]),
                        connector: controller.clone(),
                        backend_config: Default::default(),
                        backend_metadata: metadata.0.clone(),
                    },
                )
                .await?
            } else {
                native_workspace_admin::restore::reopen_staged_restore(
                    registry.clone(),
                    binding.clone(),
                    restore,
                    native_workspace_admin::restore::RestoreReadbackHost {
                        settings: restore_settings()?,
                        connector: controller.clone(),
                        backend_config: Default::default(),
                        backend_metadata_check: metadata.1.clone(),
                    },
                )
                .await?
            };
            self.entries.get_mut(workspace_id).unwrap().restore_receipt = Some(receipt);
            let receipt = self
                .entries
                .get_mut(workspace_id)
                .unwrap()
                .restore_receipt
                .take()
                .unwrap();
            let ready = native_workspace_admin::restore::finalize_staged_restore(
                receipt,
                native_workspace_admin::restore::RestoreReadbackHost {
                    settings: restore_settings()?,
                    connector: controller.clone(),
                    backend_config: Default::default(),
                    backend_metadata_check: metadata.1.clone(),
                },
            )
            .await?;
            let Origin::Restore { source, .. } = &record.origin else {
                unreachable!()
            };
            ensure!(
                ready.binding() == &binding && ready.source_identity() == source,
                "native restored finalization differs"
            );
        }
        self.service
            .register(workspace_id, controller.clone(), binding.projection())
            .map_err(|_| anyhow::anyhow!("native restore RPC registration failed"))?;
        self.entries.get_mut(workspace_id).unwrap().registered = true;
        commands.set_capture_target(
            workspace_id,
            Some(Arc::new(crate::native_capture::Target {
                controller: Arc::downgrade(&controller),
                registry: registry.clone(),
                binding: binding.clone(),
                local: repository.local_objects.clone(),
                index: provider.config,
                remote_scope: native_workspace_admin::remote_state::RemoteScope::new(
                    self.config.state_dir.join("rbe.sock"),
                    store,
                )?,
            })),
        )?;
        // Monotone ordinary-resume eligibility MUST be durable before either
        // frontend or command admission opens. Restart never replays old WC data.
        let entry = self.entries.get_mut(workspace_id).unwrap();
        if let Origin::Restore { committed, .. } = &mut entry.record.origin {
            *committed = true;
        }
        entry.record.state = Lifecycle::Ready;
        persist(entry)?;
        ensure!(
            tokio::time::Instant::now() < deadline,
            "native restore deadline exceeded before activation"
        );
        let activation = entry
            .command_startup
            .take()
            .context("native restore command reservation missing")?
            .validate_for_activation(&commands, &binding, repository.identity)?;
        entry
            .startup
            .as_mut()
            .context("native restore startup fence missing")?
            .activate()
            .await?;
        activation.commit();
        // Only infallible owner movement follows opening. This fence is already
        // active and its mount remains owned separately by the Entry/controller.
        entry.startup.take();
        Ok(Mounted {
            binding,
            registry_path: registry.path().to_owned(),
        })
    }
    fn open_existing_registry(&mut self) -> Result<Arc<Registry>> {
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
        let registry = Registry::open_existing(path)?;
        self.registry = Some(registry.clone());
        Ok(registry)
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
    pub fn recover(&mut self, id: &str) -> Result<serde_json::Value> {
        let _ = unhex(id)?;
        let entry = self
            .entries
            .get_mut(id)
            .context("native workspace is not registered")?;
        ensure!(
            entry.mount.is_none()
                && entry.controller.is_none()
                && !entry.registered
                && entry.provider_failure.is_none(),
            "native workspace has a live owned provider; use tinytree unmount, never recovery"
        );
        recover_entry(&self.config, entry)
    }
    pub async fn stop(&mut self, id: &str) -> Result<()> {
        let _ = unhex(id)?;
        if let Some(entry) = self.entries.get_mut(id) {
            if matches!(&entry.record.origin, Origin::Restore { .. })
                && entry.mount.is_none()
                && entry.controller.is_none()
                && entry.provider_failure.is_none()
                && !entry.registered
                && entry.restore_intent.is_none()
                && entry.restore_receipt.is_none()
            {
                // No provider/admin job was acquired; an early source/prepare
                // failure must not need a nonexistent command-lease mapping.
                entry.record.state = Lifecycle::Stopped;
                return persist(entry);
            }
        }
        ensure!(
            !self
                .commands
                .as_ref()
                .is_some_and(|commands| commands.active(id)),
            "native workspace has active command writers; preserve the mount"
        );
        if let Some(commands) = &self.commands {
            commands.set_capture_target(id, None)?;
        }
        let entry = self
            .entries
            .get_mut(id)
            .context("native workspace is not registered")?;
        if matches!(&entry.record.origin, Origin::Restore { .. }) {
            native_workspace_admin::restore::wait_for_restore_work_until(
                std::time::Instant::now() + std::time::Duration::from_secs(120),
            )
            .await?;
            entry.restore_receipt = None;
            entry.restore_intent = None;
        }
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
        #[cfg(target_os = "linux")]
        ensure!(
            crate::recovery::identity(&entry.record.root)?.is_none(),
            "native kernel mount remains without a live handle; use workspace recover --native-workspace ID"
        );
        entry.controller = None;
        entry.startup = None;
        // A closed reservation may be reused only by this exact retained Entry.
        // Dropping it would require a daemon restart to obtain a fresh host map.
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
fn validate_selection(selection: &crate::control::NativeRestoreSelection) -> Result<()> {
    ensure!(
        selection.workspace_id != [0; 32]
            && selection.generation > 0
            && selection.manifest_size_bytes > 0
            && selection.manifest_size_bytes <= 8 * 1024 * 1024,
        "native restore requires exact bounded retained source pins"
    );
    Ok(())
}
fn selection_from_identity(
    source: &native_workspace_admin::restore::RestoreSourceIdentity,
) -> crate::control::NativeRestoreSelection {
    crate::control::NativeRestoreSelection {
        workspace_id: source.workspace_id(),
        generation: source.generation(),
        manifest_id: source.manifest_id(),
        manifest_blake3: source.manifest_blake3(),
        manifest_size_bytes: source.manifest_size_bytes(),
    }
}
fn restore_root(seed: &tinytree_cloud::PortableWorkingCopy) -> Result<m::TreeMerge> {
    seed.tree.validate()?;
    ensure!(
        seed.workspace.len() <= 1024,
        "native restored workspace name exceeds limit"
    );
    if seed.tree.terms.len() != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "native lazy restore of unresolved working-copy merges is not supported",
        )
        .into());
    }
    Ok(m::TreeMerge {
        terms: seed.tree.terms.iter().copied().map(m::TreeId).collect(),
        labels: if seed.tree.labels.is_empty() {
            vec![String::new(); seed.tree.terms.len()]
        } else {
            seed.tree.labels.clone()
        },
    })
}
fn restore_settings() -> Result<jj_lib::settings::UserSettings> {
    // Restore preserves existing operation/commit authors and signatures. This
    // host-only context creates no new user commit or copied credential state.
    Ok(jj_lib::settings::UserSettings::from_config_and_home_dir(
        jj_lib::config::StackedConfig::with_defaults(),
        None,
    )?)
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreLocator {
    version: u32,
    socket: PathBuf,
    store: String,
}
fn restore_locator(
    socket: PathBuf,
    store: String,
) -> (
    Arc<native_workspace_admin::restore::BackendMetadataWriter>,
    Arc<native_workspace_admin::restore::BackendMetadataWriter>,
) {
    let expected = RestoreLocator {
        version: 1,
        socket,
        store,
    };
    let write = expected.clone();
    let writer = Arc::new(move |path: &Path| -> Result<()> {
        ensure!(
            path.file_name() == Some(std::ffi::OsStr::new("store")),
            "unexpected restore backend directory"
        );
        let directory =
            PrivateDir::open_existing(path.parent().context("restore backend has no repository")?)?;
        ensure!(
            !path.join("native-broker.toml").try_exists()?
                && !path.join("native-broker-spool").try_exists()?,
            "restore backend metadata already exists"
        );
        let _spool = PrivateDir::create(path.join("native-broker-spool"))?;
        directory.create_descendant_file(
            "store/native-broker.toml",
            toml::to_string(&write)?.as_bytes(),
        )?;
        Ok(())
    });
    let checker = Arc::new(move |path: &Path| -> Result<()> {
        ensure!(
            path.file_name() == Some(std::ffi::OsStr::new("store")),
            "unexpected restore backend directory"
        );
        let directory =
            PrivateDir::open_existing(path.parent().context("restore backend has no repository")?)?;
        let bytes = directory.read_descendant("store/native-broker.toml", 16 * 1024)?;
        let actual: RestoreLocator = toml::from_str(std::str::from_utf8(&bytes)?)?;
        ensure!(
            actual == expected,
            "restored backend locator differs from current configuration"
        );
        let _spool = PrivateDir::open_existing(path.join("native-broker-spool"))?;
        Ok(())
    });
    (writer, checker)
}
async fn validate_current_restore(
    binding: &Binding,
    repository: &Repository,
    controller: &Arc<Controller>,
    config: &jj_native_index::Config,
) -> Result<()> {
    use jj_vfs::native::ProjectionConnector as _;
    ensure!(
        !controller.requires_reconciliation(),
        "native current working copy requires explicit reconciliation"
    );
    let (wc, projection) =
        jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)?;
    ensure!(
        !wc.pending && wc.trees.len() == 1,
        "native current working copy has pending state"
    );
    ensure!(
        controller.connect(&binding.projection())?.inspect()? == projection,
        "native current working copy/controller projection differs"
    );
    let path = binding.repo.clone();
    let config = config.clone();
    let workspace = wc.workspace.clone();
    let operation = wc.operation.clone();
    // Local retained OP/VIEW/head/index read only: no load_at_head(), operation
    // merge, original capsule/head comparison, provider query or history fetch.
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || -> Result<()> {
        runtime.block_on(async move {
            use jj_lib::object_id::ObjectId as _;
            let store = jj_lib::simple_op_store::SimpleOpStore::load(
                &path.join("op_store"),
                jj_lib::op_store::RootOperationData {
                    root_commit_id: jj_lib::backend::CommitId::from_bytes(&[0; 32]),
                },
            );
            let heads =
                jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(&path.join("op_heads"))
                    .get_op_heads_bounded(4096)?;
            ensure!(
                !heads.is_empty(),
                "native current operation heads are empty"
            );
            let mut ids = std::collections::BTreeSet::from([[0; 64]]);
            for id in heads.iter().chain(std::iter::once(&operation)) {
                let _ = tinytree_cloud::export_operation(&store, id, Default::default()).await?;
                ids.insert(
                    id.as_bytes()
                        .try_into()
                        .context("invalid native current operation width")?,
                );
            }
            let verified =
                tinytree_cloud::export_operation(&store, &operation, Default::default()).await?;
            let op = verified
                .operation()
                .context("native current operation is unavailable")?;
            let verified_view =
                tinytree_cloud::export_view(&store, &op.view_id, Default::default()).await?;
            let view = verified_view
                .view()
                .context("native current view is unavailable")?;
            ensure!(
                (operation.as_bytes() == [0; 64] && op.view_id.as_bytes() == [0; 64])
                    || view.wc_commit_ids.contains_key(&workspace),
                "native current operation has no selected workspace"
            );
            let limits = jj_native_index::Limits::default();
            let reserved = jj_native_index::LocalIndexExportReader::required_read_bytes(&limits)?;
            ensure!(
                reserved <= 256 * 1024 * 1024,
                "native current index validation exceeds host budget"
            );
            let reader = jj_native_index::LocalIndexExportReader::open_existing(
                &path.join("index"),
                &config,
                limits,
                reserved,
            )?;
            reader.association_fingerprint(
                &ids.into_iter().collect::<Vec<_>>(),
                8192,
                256 * 1024 * 1024,
            )?;
            Ok(())
        })
    })
    .await??;
    let _ = repository;
    Ok(())
}
fn native_source_name(id: [u8; 32]) -> String {
    format!("git-tools-native-{}", hex(&id))
}
/// The caller must own the daemon's exclusive lock. Do not open providers or
/// canonicalize the disconnected projected root merely to select a record.
pub(crate) fn recover_offline(config: &Config, id: &str) -> Result<serde_json::Value> {
    let expected = unhex(id)?;
    let directory = PrivateDir::open_existing(config.state_dir.join("native-workspaces"))?;
    let child = PrivateDir::open_existing(directory.path().join(id))?;
    let record = read_record(&child.read("workspace.json", MAX_RECORD)?)?;
    ensure!(
        record.workspace_id == expected,
        "native recovery record identity mismatch"
    );
    let mut entry = Entry {
        record,
        directory: child,
        mount: None,
        controller: None,
        registered: false,
        startup: None,
        command_startup: None,
        restore_intent: None,
        restore_receipt: None,
        provider_failure: None,
    };
    recover_entry(config, &mut entry)
}
fn recover_entry(config: &Config, entry: &mut Entry) -> Result<serde_json::Value> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (config, entry);
        anyhow::bail!("native disconnected-mount recovery currently requires Linux")
    }
    #[cfg(target_os = "linux")]
    {
        validate_record(&entry.record)?;
        ensure!(
            config.native.repositories.contains_key(&entry.record.store),
            "native recovery repository alias is not configured"
        );
        ensure!(
            !overlaps(&entry.record.root, &config.state_dir),
            "native recovery mount and daemon state overlap"
        );
        let mounted = crate::recovery::identity(&entry.record.root)?;
        let owner = if mounted.is_some() {
            let proof = entry
                .record
                .native_mount
                .as_ref()
                .context("no persisted native kernel identity; inspect and unmount manually")?;
            Some(native_vfs::lock_recovery(
                &entry.directory.path().join("mount"),
                &entry.record.root,
                entry.record.workspace_id,
                &entry.record.scope,
                &proof.namespace_identity,
            )?)
        } else {
            None
        };
        let detached = if let Some(proof) = entry
            .record
            .native_mount
            .as_ref()
            .filter(|_| owner.is_some())
        {
            proof.detach(
                &entry.record.root,
                &native_source_name(entry.record.workspace_id),
            )?
        } else {
            false
        };
        entry.record.state = Lifecycle::Stopped;
        persist(entry)?;
        drop(owner);
        Ok(
            serde_json::json!({"workspace_id": hex(&entry.record.workspace_id),
            "detached": detached, "state": "stopped", "next": "tinytree resume DEST"}),
        )
    }
}
fn persist(entry: &Entry) -> Result<()> {
    struct Encoded(Vec<u8>);
    impl std::io::Write for Encoded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self
                .0
                .len()
                .checked_add(bytes.len())
                .is_none_or(|n| n > MAX_RECORD)
            {
                return Err(std::io::Error::other(
                    "native workspace record exceeds limit",
                ));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut encoded = Encoded(Vec::new());
    serde_json::to_writer(&mut encoded, &entry.record)?;
    entry.directory.replace("workspace.json", &encoded.0)?;
    Ok(())
}
fn same_intent(a: &Record, b: &Record) -> bool {
    a.version == b.version
        && a.workspace_id == b.workspace_id
        && a.store == b.store
        && a.root == b.root
        && a.origin == b.origin
        && a.scope == b.scope
}
fn validate_record(record: &Record) -> Result<()> {
    ensure!(
        record.version == 2 && record.workspace_id != [0; 32],
        "invalid native workspace record"
    );
    normalized(&record.root)?;
    if let Origin::Restore { source, seed, .. } = &record.origin {
        ensure!(
            source.workspace_id() != [0; 32]
                && source.generation() > 0
                && source.manifest_size_bytes() > 0
                && source.manifest_size_bytes() <= 8 * 1024 * 1024
                && source.capture_size_bytes() > 0
                && source.capture_size_bytes() <= 8 * 1024 * 1024
                && source.provider_identity() != [0; 32],
            "invalid retained restore origin"
        );
        seed.tree.validate()?;
    }
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    #[test]
    fn restore_backend_locator_uses_private_repository_not_store_mode() -> Result<()> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let directory = tempfile::tempdir()?;
        let repository = PrivateDir::create(directory.path().join("repo"))?;
        let store = repository.path().join("store");
        fs::create_dir(&store)?;
        fs::set_permissions(&store, fs::Permissions::from_mode(0o755))?;
        let (write, check) = restore_locator(directory.path().join("rbe.sock"), "example".into());
        write(&store)?;
        check(&store)?;
        assert_eq!(fs::metadata(&store)?.mode() & 0o777, 0o755);
        assert_eq!(
            fs::metadata(store.join("native-broker.toml"))?.mode() & 0o777,
            0o600
        );
        let original = fs::read(store.join("native-broker.toml"))?;
        assert!(write(&store).is_err());
        assert_eq!(fs::read(store.join("native-broker.toml"))?, original);
        let (_, foreign) = restore_locator(directory.path().join("other.sock"), "example".into());
        assert!(foreign(&store).is_err());
        let alias = repository.path().join("alias");
        std::os::unix::fs::symlink(&store, &alias)?;
        assert!(check(&alias).is_err());
        Ok(())
    }
    struct NoRemoteContent;
    #[async_trait::async_trait]
    impl local_cas::BlobSource for NoRemoteContent {
        async fn fetch(&self, _: &local_cas::Artifact, _: &mut fs::File) -> Result<()> {
            anyhow::bail!("provider failure fixture must not fetch file content")
        }
        async fn missing(&self, _: &[local_cas::Artifact]) -> Result<Vec<local_cas::Artifact>> {
            anyhow::bail!("provider failure fixture must not query remote content")
        }
    }
    #[tokio::test]
    async fn changed_mountpoint_after_preparation_retains_command_identity_and_allows_stop()
    -> Result<()> {
        let directory = tempfile::tempdir()?;
        let state = PrivateDir::create(directory.path().join("state"))?;
        let objects = PrivateDir::create(directory.path().join("objects"))?;
        let provider = Arc::new(jj_native_backend::DirectObjectIo::init(
            objects.path(),
            Default::default(),
        )?);
        provider
            .write_metadata(m::Object::Tree(Default::default()))
            .await?;
        let cache = LocalCas::open(
            directory.path().join("cas"),
            Arc::new(NoRemoteContent),
            Default::default(),
        )?;
        let mut config = Config::default();
        config.state_dir = state.path().to_owned();
        config.native.workspaces.registry_path = Some(directory.path().join("registry"));
        config
            .native
            .repositories
            .insert("example".into(), Default::default());
        let commands = Arc::new(
            crate::native_commands::NativeCommandHost::open(&state.path().join("commands")).await?,
        );
        let mut manager = Manager::open(
            config,
            cache,
            BTreeMap::from([(
                "example".into(),
                Repository {
                    local_objects: provider.clone(),
                    upload: None,
                    query: None,
                    io: provider,
                    scope: Scope::from_configured_identity("example", &[8; 32])?,
                    remote: false,
                    identity: [8; 32],
                },
            )]),
        )?
        .with_commands(commands.clone())?;
        let root = directory.path().join("mount");
        // Supported native provisioning creates an owner-private root; ordinary
        // create_dir inherits the fixture's shell umask and may yield 0755.
        drop(PrivateDir::create(&root)?);
        let preserved = root.join("unrelated-content");
        manager.before_provider_start = Some(Box::new(|root| {
            fs::write(root.join("unrelated-content"), b"preserve this file")?;
            Ok(())
        }));
        let id = hex(&[7; 32]);
        // Arrange the real filesystem race only AFTER preparation checked the
        // empty directory. Actual provider preflight must then reject it before
        // /dev/fuse; no fake provider or native kernel capability is involved.
        let error = manager
            .create(root, "example".into(), "0".repeat(64), id.clone())
            .await
            .expect_err("changed mountpoint must be rejected");
        assert!(
            error
                .to_string()
                .contains("native provider start requires explicit recovery"),
            "failed before intended provider-start boundary: {error:#}"
        );
        assert_eq!(manager.status()[0]["state"], "recovery_required");
        assert_eq!(manager.status()[0]["provider_recovery_owned"], true);
        assert_eq!(
            commands.workspace_state(&id).await?.state,
            native_retention::WorkspaceState::Dirty
        );
        assert!(
            !commands.active(&id),
            "idle validated command lease must not block provider recovery"
        );
        manager.stop_all().await?;
        assert!(!manager.active());
        assert_eq!(manager.status()[0]["state"], "stopped");
        assert_eq!(manager.status()[0]["provider_recovery_owned"], false);
        assert_eq!(fs::read(&preserved)?, b"preserve this file");
        let record_path = manager.entries[&id].directory.path().join("workspace.json");
        let before = fs::read(&record_path)?;
        let before_status = manager.status();
        let selection = crate::control::NativeRestoreSelection {
            workspace_id: [1; 32],
            generation: 1,
            manifest_id: [2; 32],
            manifest_blake3: [3; 32],
            manifest_size_bytes: 256,
        };
        assert!(
            manager
                .restore(
                    preserved.parent().unwrap().to_owned(),
                    "example".into(),
                    id.clone(),
                    selection
                )
                .await
                .is_err()
        );
        assert_eq!(
            manager.status(),
            before_status,
            "pre-work restore rejection must not downgrade an unrelated stopped clone"
        );
        assert_eq!(
            fs::read(&record_path)?,
            before,
            "rejected source/origin must not rewrite durable lifecycle"
        );
        assert!(
            manager
                .resume_root(directory.path().join("unknown-root"))
                .await
                .is_err()
        );
        assert_eq!(fs::read(&record_path)?, before);
        // A real previous-format clone record is read explicitly, without
        // provisioning a frontend or inventing a restored origin from defaults.
        let mut legacy: serde_json::Value = serde_json::from_slice(&before)?;
        let object = legacy.as_object_mut().unwrap();
        object.insert("version".into(), 1.into());
        object.remove("origin");
        object.insert("bootstrap_commit".into(), serde_json::to_value([0u8; 32])?);
        let legacy = serde_json::to_vec(&legacy)?;
        fs::write(&record_path, &legacy)?;
        let configuration = manager.config.clone();
        let shared = manager.cache.clone();
        let providers = manager.repositories.clone();
        drop(manager);
        let mut reopened = Manager::open(configuration.clone(), shared.clone(), providers.clone())?
            .with_commands(commands.clone())?;
        assert_eq!(reopened.status()[0]["state"], "stopped");
        assert_eq!(
            fs::read(&record_path)?,
            legacy,
            "read does not mutate legacy record"
        );
        reopened.stop(&id).await?;
        let current: serde_json::Value = serde_json::from_slice(&fs::read(&record_path)?)?;
        assert_eq!(current["version"], 2);
        assert_eq!(current["origin"]["kind"], "clone");
        assert!(current.get("bootstrap_commit").is_none());
        drop(reopened);
        let mut missing = current.clone();
        missing.as_object_mut().unwrap().remove("origin");
        let malformed = serde_json::to_vec(&missing)?;
        fs::write(&record_path, &malformed)?;
        assert!(
            Manager::open(configuration.clone(), shared.clone(), providers.clone()).is_err(),
            "version2 never defaults a missing source tag to clone"
        );
        assert_eq!(fs::read(&record_path)?, malformed);
        let mut mixed: serde_json::Value = serde_json::from_slice(&legacy)?;
        mixed["origin"] = current["origin"].clone();
        let mixed = serde_json::to_vec(&mixed)?;
        fs::write(&record_path, &mixed)?;
        assert!(
            Manager::open(configuration, shared, providers).is_err(),
            "legacy format cannot smuggle an ignored origin"
        );
        assert_eq!(fs::read(&record_path)?, mixed);
        Ok(())
    }
}

/// Validate only the private saved record; never probe the possibly stopped or
/// paused mount. The saved command mapping already selected this exact ID.
pub(crate) fn capture_upload_workspace(
    state: &Path,
    mount: &str,
    alias: &str,
    identity: &[u8; 32],
) -> Result<[u8; 32]> {
    let id = unhex(mount)?;
    let directory = PrivateDir::open_existing(state.join("native-workspaces").join(mount))?;
    let record = read_record(&directory.read("workspace.json", MAX_RECORD)?)?;
    validate_record(&record)?;
    ensure!(
        record.workspace_id == id
            && record.store == alias
            && record.scope == Scope::from_configured_identity(alias, identity)?.backend(),
        "capture route differs from workspace record"
    );
    Ok(id)
}
