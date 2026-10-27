//! Daemon ownership of native source projections and their private RPC entries.
//! Persistent records bind exact initialization intent; retries never reset a
//! dirty namespace or adopt an unrelated directory.
use crate::config::Config;
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
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
const MAX_RETIREMENTS: usize = 4096;
const MAX_CLEANUP_ENTRIES: usize = 16384;
const MAX_CLEANUP_DEPTH: usize = 32;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateRetirement {
    version: u32,
    record: Record,
    binding: Binding,
    root_tree: [u8; 32],
    complete: bool,
    root_removed: bool,
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    creation_plan: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    restore_enrollment: Option<crate::native_restore_policy::Verified>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Origin {
    Candidate {
        bootstrap_commit: [u8; 32],
    },
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
                creation_plan: None,
                restore_enrollment: None,
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
        2 | 3 => Ok(serde_json::from_slice(bytes)?),
        _ => anyhow::bail!("unsupported native workspace record version"),
    }
}
/// Immutable identity reservation for an automatically named cloud restore.
/// It precedes every per-workspace or destination write and is never repinned.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CloudRestoreIntent {
    version: u32,
    root: PathBuf,
    store: String,
    workspace_id: [u8; 32],
    source: crate::control::NativeRestoreSelection,
    provider_identity: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    requested_policy: Option<crate::native_restore_policy::Requested>,
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

pub(crate) struct ProviderEnrollment {
    pub workspace_id: [u8; 32],
    pub store: String,
    pub root: PathBuf,
    pub pending: bool,
    pub retired: bool,
    pub intent_only: bool,
}
/// Read committed identity reservations only; no recovery, root probing, or
/// object access. Every source is bounded by the same startup census budget.
pub(crate) fn inspect_provider_enrollments(
    config: &Config,
    census: &mut crate::provider_identity::Census,
) -> Result<Vec<ProviderEnrollment>> {
    let mut known = BTreeMap::new();
    let path = config.state_dir.join("native-workspaces");
    if path.try_exists()? {
        let directory = PrivateDir::open_existing(path)?;
        for item in fs::read_dir(directory.path())? {
            census.entry()?;
            let item = item?;
            ensure!(item.file_type()?.is_dir(), "unknown provider workspace census entry");
            let name = item.file_name().into_string().map_err(|_| anyhow::anyhow!("invalid provider workspace census name"))?;
            let id = unhex(&name)?;
            let child = PrivateDir::open_existing(item.path())?;
            if !child.path().join("workspace.json").try_exists()? {
                // An exact cloud reservation below must account for this ID.
                census.charge(4096)?;
                known.insert(id, None);
                continue;
            }
            let bytes = census.read(&child, "workspace.json", MAX_RECORD)?;
            let record = read_record(&bytes).map_err(|_| anyhow::anyhow!("invalid provider workspace census record"))?;
            validate_record(&record).map_err(|_| anyhow::anyhow!("invalid provider workspace census record"))?;
            ensure!(record.workspace_id == id, "provider workspace census identity differs");
            census_record(&record, census)?;
            census.charge(4096)?;
            known.insert(id, Some(ProviderEnrollment {workspace_id: id, store: record.store, root: record.root,
                pending: matches!(record.state, Lifecycle::Pending | Lifecycle::RecoveryRequired), retired: false, intent_only: false}));
        }
    }
    let path = config.state_dir.join("native-cloud-restores");
    if path.try_exists()? {
        let directory = PrivateDir::open_existing(path)?;
        for item in fs::read_dir(directory.path())? {
            census.entry()?;
            let item = item?;
            let name = item.file_name().into_string().map_err(|_| anyhow::anyhow!("invalid provider restore census name"))?;
            let bytes = census.read(&directory, &name, MAX_RECORD)?;
            if is_retirement_temporary(&name) { continue; }
            let key = name.strip_suffix(".json").context("unknown provider restore census entry")?;
            unhex(key)?;
            let intent: CloudRestoreIntent = serde_json::from_slice(&bytes)
                .map_err(|_| anyhow::anyhow!("invalid provider restore census record"))?;
            validate_cloud_intent(&intent).map_err(|_| anyhow::anyhow!("invalid provider restore census record"))?;
            ensure!(cloud_intent_key(&intent.root)? == key, "provider restore census filename differs");
            census.identity(&intent.store, intent.provider_identity)?;
            census.charge(4096)?;
            if let Some(Some(existing)) = known.get(&intent.workspace_id) {
                ensure!(existing.store == intent.store && existing.root == intent.root,
                    "provider restore census enrollment conflicts");
            }
            let intent_only = !known.contains_key(&intent.workspace_id);
            known.entry(intent.workspace_id).or_insert(None).get_or_insert(ProviderEnrollment {
                workspace_id: intent.workspace_id, store: intent.store, root: intent.root, pending: true, retired: false, intent_only,
            });
        }
    }
    let path = config.state_dir.join("native-candidate-retirements");
    if path.try_exists()? {
        let directory = PrivateDir::open_existing(path)?;
        for item in fs::read_dir(directory.path())? {
            census.entry()?;
            let item = item?;
            let name = item.file_name().into_string().map_err(|_| anyhow::anyhow!("invalid provider retirement census name"))?;
            let bytes = census.read(&directory, &name, MAX_RECORD)?;
            if is_retirement_temporary(&name) { continue; }
            let id = unhex(name.strip_suffix(".json").context("unknown provider retirement census entry")?)?;
            let receipt: CandidateRetirement = serde_json::from_slice(&bytes)
                .map_err(|_| anyhow::anyhow!("invalid provider retirement census record"))?;
            validate_retirement(&receipt, id).map_err(|_| anyhow::anyhow!("invalid provider retirement census record"))?;
            census_record(&receipt.record, census)?;
            ensure!(!receipt.complete || !known.contains_key(&id),
                "completed provider retirement retains workspace state");
            census.charge(4096)?;
            if let Some(Some(existing)) = known.get_mut(&id) {
                ensure!(existing.store == receipt.record.store && existing.root == receipt.record.root
                    && !receipt.complete, "provider retirement census enrollment conflicts");
                existing.pending = true;
                existing.intent_only = false;
            } else {
                known.insert(id, Some(ProviderEnrollment {
                    workspace_id:id, store:receipt.record.store, root:receipt.record.root,
                    pending:!receipt.complete, retired:receipt.complete, intent_only:false,
                }));
            }
        }
    }
    ensure!(known.values().all(Option::is_some), "unreserved provider workspace census entry");
    Ok(known.into_values().flatten().collect())
}
fn census_record(record: &Record, census: &mut crate::provider_identity::Census) -> Result<()> {
    census.scope(&record.store, &record.scope)?;
    if let Some(bytes) = &record.creation_plan {
        let plan = native_broker::CreationPlan::decode(bytes)
            .map_err(|_| anyhow::anyhow!("invalid provider creation census record"))?;
        census.identity(&record.store, plan.provider_identity)?;
    }
    if let Origin::Restore {source,..} = &record.origin {
        census.identity(&record.store, source.provider_identity())?;
    }
    Ok(())
}

pub(crate) struct Manager {
    config: Config,
    directory: PrivateDir,
    retirements_directory: PrivateDir,
    retirements: BTreeMap<String, CandidateRetirement>,
    cloud_intents_directory: PrivateDir,
    cloud_intents: BTreeMap<String, CloudRestoreIntent>,
    registry: Option<Arc<Registry>>,
    cache: Arc<LocalCas>,
    repositories: BTreeMap<String, Repository>,
    service: ProjectionService,
    entries: BTreeMap<String, Entry>,
    commands: Option<Arc<crate::native_commands::NativeCommandHost>>,
    #[cfg(test)]
    after_retirement_publication: Option<Box<dyn FnOnce() -> Result<()> + Send + Sync>>,
    #[cfg(test)]
    after_cloud_intent_publication: Option<Box<dyn FnOnce() -> Result<()> + Send + Sync>>,
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
        let retirements_directory =
            PrivateDir::create(config.state_dir.join("native-candidate-retirements"))?;
        let mut retirements = BTreeMap::new();
        for (count, item) in fs::read_dir(retirements_directory.path())?.enumerate() {
            ensure!(
                count < MAX_RETIREMENTS * 2,
                "candidate retirement directory entry limit exceeded"
            );
            let item = item?;
            let name = item
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("invalid retirement name"))?;
            if is_retirement_temporary(&name) {
                // A killed atomic replace can leave partial bytes before
                // rename. These bytes are never disposal authority. Bound and
                // validate the owner-private regular file, then remove it.
                retirements_directory.read(&name, MAX_RECORD)?;
                fs::remove_file(item.path())?;
                fs::File::open(retirements_directory.path())?.sync_all()?;
                continue;
            }
            ensure!(
                retirements.len() < MAX_RETIREMENTS,
                "candidate retirement receipt limit exceeded"
            );
            let id = name
                .strip_suffix(".json")
                .context("unknown candidate retirement entry")?;
            let expected = unhex(id)?;
            let mut receipt: CandidateRetirement =
                serde_json::from_slice(&retirements_directory.read(&name, MAX_RECORD)?)?;
            validate_retirement(&receipt, expected)?;
            if !receipt.complete {
                finish_candidate_retirement(&config, &directory, &mut receipt)?;
                retirements_directory.replace(&name, &serde_json::to_vec(&receipt)?)?;
            }
            ensure!(
                !directory.path().join(id).try_exists()?,
                "retired candidate identity has live state"
            );
            retirements.insert(id.to_owned(), receipt);
        }
        let cloud_intents_directory =
            PrivateDir::create(config.state_dir.join("native-cloud-restores"))?;
        let cloud_intents = load_cloud_intents(
            &cloud_intents_directory,
            config.native.workspaces.max_workspaces,
        )?;
        ensure!(
            !cloud_intents.values().any(|intent| retirements
                .contains_key(&hex(&intent.workspace_id))
                || overlaps(&intent.root, &config.state_dir)),
            "cloud restore intent conflicts with daemon/retired state"
        );
        let mut entries: BTreeMap<String, Entry> = BTreeMap::new();
        for (count, item) in fs::read_dir(directory.path())?.enumerate() {
            ensure!(
                count < config.native.workspaces.max_workspaces,
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
            if !child.path().join("workspace.json").try_exists()? {
                ensure!(
                    cloud_intents
                        .values()
                        .any(|intent| intent.workspace_id == id),
                    "unknown native workspace state; preserve it"
                );
                reconcile_cloud_temporary_files(&child)?;
                continue;
            }
            let record = read_record(&child.read("workspace.json", MAX_RECORD)?)?;
            validate_record(&record)?;
            ensure!(
                record.workspace_id == id,
                "native workspace registry identity mismatch"
            );
            ensure!(
                !entries
                    .values()
                    .any(|entry| overlaps(&entry.record.root, &record.root)),
                "native workspace registry roots overlap"
            );
            for intent in cloud_intents.values() {
                if intent.workspace_id == id || overlaps(&intent.root, &record.root) {
                    validate_cloud_record(intent, &record)?;
                }
            }
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
        ensure!(
            entries.len()
                + cloud_intents
                    .values()
                    .filter(|intent| !entries.contains_key(&hex(&intent.workspace_id)))
                    .count()
                <= config.native.workspaces.max_workspaces,
            "native workspace reservations exceed limit"
        );
        Ok(Self {
            config,
            directory,
            retirements_directory,
            retirements,
            cloud_intents_directory,
            cloud_intents,
            registry: None,
            cache,
            repositories,
            service,
            entries,
            commands: None,
            #[cfg(test)]
            after_retirement_publication: None,
            #[cfg(test)]
            after_cloud_intent_publication: None,
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
    fn cloud_source(&self, store: &str) -> Result<Arc<crate::native_capture_upload::Source>> {
        self.repositories
            .get(store)
            .context("native repository alias is not configured")?
            .upload
            .clone()
            .context("cloud recovery requires a configured remote provider")
    }
    fn cloud_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now()
            + self
                .config
                .reapi
                .operation_timeout()
                .min(std::time::Duration::from_secs(30))
    }
    fn cloud_local_destination(&self, store: &str, id: &str) -> Option<PathBuf> {
        self.entries
            .values()
            .find(|entry| entry.record.store == store && hex(&entry.record.workspace_id) == id)
            .map(|entry| entry.record.root.clone())
    }
    pub async fn cloud_workspaces(
        &self,
        store: &str,
        page_size: u16,
        continuation: &str,
    ) -> Result<crate::control::NativeCloudWorkspaces> {
        let mut page = crate::native_cloud::workspaces(
            self.cloud_source(store)?,
            page_size,
            continuation,
            self.cloud_deadline(),
        )
        .await?;
        for row in &mut page.workspaces {
            row.local_destination = self.cloud_local_destination(store, &row.workspace_id);
        }
        Ok(page)
    }
    pub async fn cloud_versions(
        &self,
        store: &str,
        workspace_id: &str,
        page_size: u16,
        continuation: &str,
    ) -> Result<crate::control::NativeCloudVersions> {
        let mut page = crate::native_cloud::versions(
            self.cloud_source(store)?,
            workspace_id,
            page_size,
            continuation,
            self.cloud_deadline(),
        )
        .await?;
        page.workspace.local_destination = self.cloud_local_destination(store, workspace_id);
        Ok(page)
    }
    pub async fn restore_cloud_version(
        &mut self,
        root: PathBuf,
        store: String,
        source_workspace: String,
        generation: Option<u64>,
    ) -> Result<crate::control::NativeCloudRestored> {
        let deadline = self.host_admission_deadline();
        let source_id = crate::native_cloud::workspace(&source_workspace)?;
        ensure!(generation != Some(0), "cloud generation must be positive");
        let root = canonical_candidate(&root)?;
        ensure!(
            !overlaps(&root, &self.config.state_dir),
            "cloud restore root overlaps daemon state"
        );
        let prior = self
            .entries
            .iter()
            .find(|(_, entry)| entry.record.root == root)
            .map(|(id, entry)| (id.clone(), entry.record.clone()));
        let key = cloud_intent_key(&root)?;
        let provider_identity = self
            .repositories
            .get(&store)
            .context("native repository alias is not configured")?
            .identity;
        let saved_intent = self.cloud_intents.get(&key).cloned();
        let (id, source, reused_destination) = if let Some((id, record)) = prior {
            ensure!(
                record.store == store,
                "cloud restore destination belongs to another repository"
            );
            let Origin::Restore { source, .. } = record.origin else {
                anyhow::bail!("cloud restore destination belongs to a clone origin");
            };
            ensure!(
                source.workspace_id() == source_id
                    && generation.is_none_or(|value| value == source.generation()),
                "cloud restore selection differs from the saved destination intent"
            );
            // A retry of latest reuses the original pin, not a newly moved head.
            ensure!(
                source.provider_identity() == provider_identity,
                "cloud restore provider differs from the saved destination intent"
            );
            (id, selection_from_identity(&source), true)
        } else if let Some(intent) = &saved_intent {
            ensure!(
                intent.store == store
                    && intent.provider_identity == provider_identity
                    && intent.source.workspace_id == source_id
                    && generation.is_none_or(|value| value == intent.source.generation),
                "cloud restore selection/provider differs from the saved destination intent"
            );
            (hex(&intent.workspace_id), intent.source.clone(), true)
        } else {
            ensure!(
                self.reserved_workspaces() < self.config.native.workspaces.max_workspaces,
                "native workspace reservation limit reached"
            );
            ensure!(
                !self
                    .entries
                    .values()
                    .any(|entry| overlaps(&entry.record.root, &root))
                    && !self
                        .cloud_intents
                        .values()
                        .any(|intent| overlaps(&intent.root, &root)),
                "native workspace roots overlap"
            );
            let source = crate::native_cloud::select(
                self.cloud_source(&store)?,
                &source_workspace,
                generation,
                deadline.min(self.cloud_deadline()),
            )
            .await?;
            let id = format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            let requested = crate::native_restore_policy::Requested::resolve(
                &self.config.native.workspaces,
                &crate::native_restore_policy::Binding {
                    root: &root,
                    store: &store,
                    workspace_id: unhex(&id)?,
                    source: &source,
                    provider_identity,
                },
                self.repositories[&store].remote,
            )?;
            let intent = CloudRestoreIntent {
                requested_policy: Some(requested),
                version: 1,
                root: root.clone(),
                store: store.clone(),
                workspace_id: unhex(&id)?,
                source: source.clone(),
                provider_identity,
            };
            ensure!(
                !self.entries.contains_key(&id)
                    && !self.retirements.contains_key(&id)
                    && !self
                        .cloud_intents
                        .values()
                        .any(|old| old.workspace_id == intent.workspace_id)
                    && !self.directory.path().join(&id).try_exists()?,
                "generated cloud restore identity is already reserved"
            );
            // Memory owns the exact pin before any possibly ambiguous durable
            // publication. No per-ID state or destination has been touched.
            self.cloud_intents.insert(key.clone(), intent);
            (id, source, false)
        };
        if let Some(intent) = self.cloud_intents.get(&key) {
            ensure!(
                intent.workspace_id == unhex(&id)?
                    && intent.source == source
                    && intent.store == store
                    && intent.provider_identity == provider_identity,
                "cloud restore record differs from saved intent"
            );
            persist_cloud_intent(&self.cloud_intents_directory, intent)?;
            #[cfg(test)]
            if let Some(hook) = self.after_cloud_intent_publication.take() {
                hook()?;
            }
        }
        let mounted = self
            .restore_until(root, store, id, source.clone(), deadline)
            .await?;
        Ok(crate::control::NativeCloudRestored {
            mounted,
            source,
            reused_destination,
        })
    }
    fn reserved_workspaces(&self) -> usize {
        self.entries.len()
            + self
                .cloud_intents
                .values()
                .filter(|intent| !self.entries.contains_key(&hex(&intent.workspace_id)))
                .count()
    }
    fn host_admission_deadline(&self) -> tokio::time::Instant {
        // Control has no caller-supplied whole-operation deadline. Bound this
        // admission once, below the client's 180s response timeout; never renew
        // it on a maintenance wake or cancel accepted physical lifecycle work.
        tokio::time::Instant::now()
            + self
                .config
                .reapi
                .operation_timeout()
                .min(std::time::Duration::from_secs(120))
    }
    async fn outer_host_access_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Arc<native_retention::HostAccessGuard>>> {
        match &self.commands {
            Some(commands) => Ok(Some(commands.outer_host_access_until(deadline).await?)),
            None => Ok(None),
        }
    }
    pub async fn collect_native(&self, alias: &str, apply: bool) -> Result<serde_json::Value> {
        ensure!(
            self.entries.values().all(|entry| {
                matches!(entry.record.state, Lifecycle::Ready | Lifecycle::Stopped)
                    && entry.provider_failure.is_none()
                    && entry.startup.is_none()
                    && entry.command_startup.is_none()
                    && entry.restore_intent.is_none()
                    && entry.restore_receipt.is_none()
            }),
            "native collection blocked: workspace recovery is incomplete"
        );
        let repository = self
            .repositories
            .get(alias)
            .context("native repository alias is not configured")?;
        ensure!(
            !repository.remote,
            "native collection requires complete local storage"
        );
        self.commands
            .as_ref()
            .context("native command host unavailable")?
            .collect_native(repository.local_objects.clone(), apply)
            .await
    }
    pub fn status(&self) -> serde_json::Value {
        serde_json::json!(self.entries.iter().map(|(id,entry)| serde_json::json!({"workspace_id":id,"root":entry.record.root,"store":entry.record.store,"state":entry.record.state,"active":entry.mount.is_some(),"provider_recovery_owned":entry.provider_failure.is_some(),"fuse_metadata":entry.mount.as_ref().and_then(|mount|mount.metadata_diagnostics())})).collect::<Vec<_>>())
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
        let deadline = self.host_admission_deadline();
        self.create_mode(root, store, commit, workspace_id, false, None, deadline)
            .await
    }
    pub async fn create_planned(
        &mut self,
        root: PathBuf,
        store: String,
        commit: String,
        workspace_id: String,
        creation_plan: Vec<u8>,
    ) -> Result<Mounted> {
        let plan = native_broker::CreationPlan::decode(&creation_plan)?;
        let deadline = self.host_admission_deadline();
        self.create_mode(
            root,
            store,
            commit,
            workspace_id,
            false,
            Some(plan),
            deadline,
        )
        .await
    }
    /// Fresh immutable check source; ordinary native creation stays writable.
    pub async fn create_candidate(
        &mut self,
        root: PathBuf,
        store: String,
        commit: String,
        workspace_id: String,
    ) -> Result<Mounted> {
        let deadline = self.host_admission_deadline();
        self.create_mode(root, store, commit, workspace_id, true, None, deadline)
            .await
    }
    async fn create_mode(
        &mut self,
        root: PathBuf,
        store: String,
        commit: String,
        workspace_id: String,
        immutable: bool,
        requested_plan: Option<native_broker::CreationPlan>,
        deadline: tokio::time::Instant,
    ) -> Result<Mounted> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        if immutable {
            return Err(native_vfs::ImmutableCandidateUnsupported.into());
        }
        let access = self.outer_host_access_until(deadline).await?;
        ensure!(
            !self.retirements.contains_key(&workspace_id),
            "native workspace identity is retired"
        );
        let id = unhex(&workspace_id)?;
        ensure!(id != [0; 32], "native workspace identity must be nonzero");
        let commit = unhex(&commit)?;
        ensure!(
            !immutable || commit != [0; 32],
            "candidate requires an exact nonempty commit"
        );
        normalized(&root)?;
        let root = canonical_candidate(&root)?;
        let repository = self
            .repositories
            .get(&store)
            .context("native repository alias is not configured")?
            .clone();
        ensure!(
            !self
                .cloud_intents
                .values()
                .any(|intent| intent.workspace_id == id || overlaps(&intent.root, &root)),
            "native destination is reserved by a cloud restore intent"
        );
        let creation_plan = if let Some(existing) = self.entries.get(&workspace_id) {
            if let Some(plan) = requested_plan {
                ensure!(
                    existing.record.creation_plan.as_deref() == Some(plan.encode().as_slice()),
                    "saved native creation plan differs; preserve the original intent"
                );
            }
            existing.record.creation_plan.clone()
        } else if let Some(plan) = requested_plan {
            ensure!(
                !immutable && plan.root == commit,
                "native creation plan root mismatch"
            );
            let description = self.config.native.workspaces.creation_description(
                repository.identity,
                repository.remote,
                repository.query.is_some(),
            );
            let expected = description.creation_plan(commit)?;
            ensure!(
                plan == expected,
                "native creation policy changed before first host admission; preserve the client intent"
            );
            let query = plan.index_mode == native_broker::ProviderIndexMode::Federated;
            if query || plan.policy.upload_enabled {
                let checked = repository
                    .query
                    .as_ref()
                    .context("native creation query adapter unavailable")?
                    .preflight_creation(
                        m::CommitId(commit),
                        query,
                        plan.policy.upload_enabled,
                        plan.policy.auto_upload,
                        deadline,
                        // Clone the existing lease, never acquire a new fence:
                        // accepted cleanup outlives a cancelled control waiter.
                        Arc::new(access.clone()),
                    )
                    .await?;
                drop(checked);
            }
            Some(plan.encode())
        } else {
            None
        };
        let registry = self.open_registry()?;
        let record = Record {
            creation_plan,
            restore_enrollment: None,
            version: if immutable { 3 } else { 2 },
            workspace_id: id,
            root: root.clone(),
            store: store.clone(),
            origin: if immutable {
                Origin::Candidate {
                    bootstrap_commit: commit,
                }
            } else {
                Origin::Clone {
                    bootstrap_commit: commit,
                }
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
                self.reserved_workspaces() < self.config.native.workspaces.max_workspaces,
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
        let preparation = if immutable {
            native_vfs::prepare_immutable_candidate(config, io.clone(), self.cache.clone()).await
        } else {
            native_vfs::prepare(config, io.clone(), self.cache.clone()).await
        };
        let prepared = match preparation {
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
            if let Some(access) = &access {
                commands
                    .register_enrolled_with_host_access(
                        &binding,
                        repository.identity,
                        creation_policy(&record)?.is_some_and(|p| p.auto_upload),
                        access.clone(),
                    )
                    .await?;
                commands
                    .invalidate_workspace_with_host_access(
                        &binding,
                        repository.identity,
                        access.clone(),
                    )
                    .await?;
            } else {
                commands.register(&binding, repository.identity).await?;
            }
        }
        #[cfg(test)]
        if let Some(before) = self.before_provider_start.take() {
            before(&root)?;
        }
        let mount = match native_vfs::mount_prepared_with_metadata_ttl(
            prepared,
            std::time::Duration::from_millis(self.config.native.workspaces.metadata_cache_ttl_ms),
        )
        .await
        {
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
        if let Some(commands) = self.commands.as_ref().filter(|_| !immutable) {
            let endpoint = self.config.state_dir.join("rbe.sock");
            commands.set_capture_target(
                &workspace_id,
                Some(Arc::new(crate::native_capture::Target {
                    auto_enrolled: creation_policy(&record)?.is_some_and(|p| p.auto_capture),
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
        let deadline = self.host_admission_deadline();
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
        self.resume_until(&ids[0], deadline).await
    }
    pub async fn resume(&mut self, id: &str) -> Result<Mounted> {
        self.resume_until(id, self.host_admission_deadline()).await
    }
    async fn resume_until(&mut self, id: &str, deadline: tokio::time::Instant) -> Result<Mounted> {
        let _ = unhex(id)?;
        let record = self
            .entries
            .get(id)
            .context("native workspace is not registered")?
            .record
            .clone();
        match &record.origin {
            Origin::Candidate { bootstrap_commit } => {
                self.create_mode(
                    record.root,
                    record.store,
                    hex(bootstrap_commit),
                    id.into(),
                    true,
                    None,
                    deadline,
                )
                .await
            }
            Origin::Clone { bootstrap_commit } => {
                self.create_mode(
                    record.root,
                    record.store,
                    hex(bootstrap_commit),
                    id.into(),
                    false,
                    record
                        .creation_plan
                        .as_deref()
                        .map(native_broker::CreationPlan::decode)
                        .transpose()?,
                    deadline,
                )
                .await
            }
            Origin::Restore { source, .. } => {
                self.restore_until(
                    record.root,
                    record.store,
                    id.into(),
                    selection_from_identity(source),
                    deadline,
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
        self.restore_until(
            root,
            store,
            workspace_id,
            selection,
            self.host_admission_deadline(),
        )
        .await
    }
    async fn restore_until(
        &mut self,
        root: PathBuf,
        store: String,
        workspace_id: String,
        selection: crate::control::NativeRestoreSelection,
        deadline: tokio::time::Instant,
    ) -> Result<Mounted> {
        let deadline = deadline.min(
            tokio::time::Instant::now()
                + std::time::Duration::from_millis(
                    crate::native_restore_source::Limits::default()
                        .restore
                        .timeout_ms,
                ),
        );
        let access = self
            .commands
            .as_ref()
            .context("native restore requires managed command admission")?
            .outer_host_access_until(deadline)
            .await?;
        let mut started = false;
        let result = self
            .restore_inner(
                root,
                store,
                &workspace_id,
                selection,
                &mut started,
                access,
                deadline,
            )
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
        access: Arc<native_retention::HostAccessGuard>,
        deadline: tokio::time::Instant,
    ) -> Result<Mounted> {
        #[cfg(not(target_os = "linux"))]
        anyhow::bail!("startup-gated native restore requires Linux; no checkout was created");
        ensure!(
            !self.retirements.contains_key(workspace_id),
            "native workspace identity is retired"
        );
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
        for intent in self.cloud_intents.values() {
            if intent.workspace_id == id || overlaps(&intent.root, &root) {
                ensure!(
                    intent.workspace_id == id
                        && intent.root == root
                        && intent.store == store
                        && intent.source == selection
                        && intent.provider_identity == repository.identity,
                    "native restore differs from reserved cloud intent"
                );
            }
        }
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
                self.entries.len() < settings.max_workspaces
                    && (self
                        .cloud_intents
                        .values()
                        .any(|intent| intent.workspace_id == id)
                        || self.reserved_workspaces() < settings.max_workspaces),
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
        let requested_policy = match &existing {
            Some(record) => record
                .restore_enrollment
                .as_ref()
                .map(|value| value.requested.clone()),
            None => self
                .cloud_intents
                .values()
                .find(|intent| intent.workspace_id == id)
                .and_then(|intent| intent.requested_policy.clone()),
        };
        let policy_binding = crate::native_restore_policy::Binding {
            root: &root,
            store: &store,
            workspace_id: id,
            source: &selection,
            provider_identity: repository.identity,
        };
        if existing.is_none() {
            if let Some(requested) = &requested_policy {
                requested.validate(&policy_binding)?;
                if requested.needs_feature_probe(&self.config.native.workspaces) {
                    let client = source
                        .control
                        .client_for_owned_work(Arc::new((source.clone(), access.clone())), deadline)
                        .await?;
                    requested
                        .check_features(&client, &self.config.native.workspaces)
                        .await?;
                }
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
                Arc::new(access.clone()),
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
            let restore_enrollment = requested_policy
                .clone()
                .map(|requested| {
                    crate::native_restore_policy::Verified::seal(
                        &policy_binding,
                        requested,
                        identity.clone(),
                    )
                })
                .transpose()?;
            let record = Record {
                creation_plan: None,
                restore_enrollment,
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
            if self
                .cloud_intents
                .values()
                .any(|intent| intent.workspace_id == id)
            {
                reconcile_cloud_temporary_files(&child)?;
            } else {
                ensure!(
                    fs::read_dir(child.path())?.next().is_none(),
                    "unknown native restore state; preserve it"
                );
            }
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
            // Keep identity/source ownership even when publication is visible
            // but the directory sync reports an error.
            self.entries.insert(workspace_id.into(), entry);
            persist(self.entries.get(workspace_id).unwrap())?;
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
                .register_reserved_enrolled_with_host_access(
                    &binding,
                    repository.identity,
                    creation_policy(&record)?.is_some_and(|policy| policy.auto_upload),
                    access.clone(),
                )
                .await?;
            self.entries.get_mut(workspace_id).unwrap().command_startup = Some(reservation);
        }
        commands
            .invalidate_workspace_with_host_access(&binding, repository.identity, access.clone())
            .await?;
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
        let startup = match native_vfs::mount_prepared_for_restore_with_metadata_ttl(
            workspace,
            std::time::Duration::from_millis(self.config.native.workspaces.metadata_cache_ttl_ms),
        )
        .await
        {
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
        let _ = std::io::Write::write_all(&mut std::io::stderr(), b"native_restore phase=controller_open state=start\n");
        let controller = Arc::new(
            Controller::open(mount.clone(), repository.io.clone(), controller_config).await?,
        );
        let _ = std::io::Write::write_all(&mut std::io::stderr(), b"native_restore phase=controller_open state=complete\n");
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
                let _ = std::io::Write::write_all(&mut std::io::stderr(), b"native_restore phase=initialize_stage state=start\n");
                let receipt = native_workspace_admin::restore::initialize_restored(
                    intent,
                    restore,
                    native_workspace_admin::restore::RestoreHost {
                        remote_scope: native_workspace_admin::remote_state::RemoteScope::new(
                            self.config.state_dir.join("rbe.sock"),
                            binding.repository.clone(),
                        )?,
                        settings: restore_settings()?,
                        signer: jj_lib::signing::Signer::new(None, vec![]),
                        connector: controller.clone(),
                        backend_config: Default::default(),
                        backend_metadata: metadata.0.clone(),
                    },
                )
                .await?;
                let _ = std::io::Write::write_all(&mut std::io::stderr(), b"native_restore phase=initialize_stage state=complete\n");
                receipt
            } else {
                native_workspace_admin::restore::reopen_staged_restore(
                    registry.clone(),
                    binding.clone(),
                    restore,
                    native_workspace_admin::restore::RestoreReadbackHost {
                        remote_scope: native_workspace_admin::remote_state::RemoteScope::new(
                            self.config.state_dir.join("rbe.sock"),
                            binding.repository.clone(),
                        )?,
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
            let _ = std::io::Write::write_all(&mut std::io::stderr(), b"native_restore phase=finalize_stage state=start\n");
            let ready = native_workspace_admin::restore::finalize_staged_restore(
                receipt,
                native_workspace_admin::restore::RestoreReadbackHost {
                    remote_scope: native_workspace_admin::remote_state::RemoteScope::new(
                        self.config.state_dir.join("rbe.sock"),
                        binding.repository.clone(),
                    )?,
                    settings: restore_settings()?,
                    connector: controller.clone(),
                    backend_config: Default::default(),
                    backend_metadata_check: metadata.1.clone(),
                },
            )
            .await?;
            let _ = std::io::Write::write_all(&mut std::io::stderr(), b"native_restore phase=finalize_stage state=complete\n");
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
                auto_enrolled: creation_policy(&record)?.is_some_and(|policy| policy.auto_capture),
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
        let _ = std::io::Write::write_all(&mut std::io::stderr(), b"native_restore phase=activation state=start\n");
        entry
            .startup
            .as_mut()
            .context("native restore startup fence missing")?
            .activate()
            .await?;
        activation.commit();
        // Only infallible owner movement and best-effort diagnostics follow
        // opening. The active mount remains owned by the Entry/controller.
        entry.startup.take();
        let _ = std::io::Write::write_all(&mut std::io::stderr(), b"native_restore phase=activation state=complete\n");
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
        // Recovery already runs on the control server's physical blocking
        // worker. The registry wait uses its own runtime, not this thread's.
        let deadline = self.host_admission_deadline();
        let _access = futures::executor::block_on(self.outer_host_access_until(deadline))?;
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
        let deadline = self.host_admission_deadline();
        let _access = self.outer_host_access_until(deadline).await?;
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
            native_workspace_admin::restore::wait_for_restore_work_until(deadline.into_std())
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
    /// Runtime proof for a check runner. This does not parse client-selected
    /// state or weaken the ordinary Mounted response compatibility boundary.
    pub fn inspect_candidate(&self, id: &str, commit: &str) -> Result<serde_json::Value> {
        let identity = unhex(id)?;
        let expected = unhex(commit)?;
        ensure!(
            !self.retirements.contains_key(id),
            "candidate identity is retired"
        );
        let entry = self
            .entries
            .get(id)
            .context("candidate is not registered")?;
        ensure!(
            entry.record.version == 3
                && entry.record.state == Lifecycle::Ready
                && entry.record.workspace_id == identity
                && matches!(entry.record.origin, Origin::Candidate { bootstrap_commit } if bootstrap_commit == expected)
                && entry.registered
                && entry.provider_failure.is_none()
                && entry.startup.is_none(),
            "candidate is not ready with the requested immutable commit"
        );
        let mount = entry.mount.as_ref().context("candidate is not active")?;
        let tree = mount.tree();
        let output_core::SourceAccess::ImmutableCandidate { root_tree } = tree.source_access()
        else {
            anyhow::bail!("candidate source is not immutable");
        };
        ensure!(
            mount.workspace_identity() == identity
                && mount.root() == entry.record.root
                && tree.source_baseline()?.0 == root_tree
                && root_tree != [0; 32],
            "candidate runtime binding changed"
        );
        Ok(
            serde_json::json!({"version":1,"workspace_id":id,"commit":commit,"root_tree":hex(&root_tree),
            "access":"immutable_candidate","root":entry.record.root,"store":entry.record.store,"active":true}),
        )
    }
    fn publish_retirement_intent(&mut self, id: &str, receipt: &CandidateRetirement) -> Result<()> {
        let bytes = serde_json::to_vec(receipt)?;
        ensure!(
            bytes.len() <= MAX_RECORD,
            "retirement receipt exceeds limit"
        );
        // Even a failed directory fsync can follow a successful rename. Close
        // this identity in memory BEFORE attempting publication, never reopen
        // commands or permit resume after an ambiguous storage error.
        self.retirements.insert(id.into(), receipt.clone());
        self.retirements_directory
            .replace(format!("{id}.json"), &bytes)?;
        #[cfg(test)]
        if let Some(failure) = self.after_retirement_publication.take() {
            failure()?;
        }
        Ok(())
    }

    /// Retire only disposable, stopped immutable check state. Canonical stores,
    /// CAS, server holds, and authoritative retention roots are not collected.
    pub async fn retire_candidate(
        &mut self,
        id: &str,
        expected_commit: &str,
    ) -> Result<serde_json::Value> {
        #[cfg(not(target_os = "linux"))]
        anyhow::bail!("candidate retirement requires native Linux unmounted-state proof");
        let access = self
            .outer_host_access_until(self.host_admission_deadline())
            .await?;
        let identity = unhex(id)?;
        let expected = unhex(expected_commit)?;
        ensure!(
            identity != [0; 32] && expected != [0; 32],
            "retirement requires exact candidate and commit identities"
        );
        if let Some(mut receipt) = self.retirements.get(id).cloned() {
            ensure!(
                matches!(receipt.record.origin, Origin::Candidate { bootstrap_commit } if bootstrap_commit == expected),
                "retired candidate commit differs"
            );
            if !receipt.complete {
                // A prior publication error may have happened before rename.
                // Reestablish durable intent before performing any cleanup.
                self.publish_retirement_intent(id, &receipt)?;
                finish_candidate_retirement(&self.config, &self.directory, &mut receipt)?;
                self.retirements_directory
                    .replace(format!("{id}.json"), &serde_json::to_vec(&receipt)?)?;
                self.retirements.insert(id.into(), receipt.clone());
                self.entries.remove(id);
            }
            return Ok(
                serde_json::json!({"retired":true,"root_removed":receipt.root_removed,"retention_gc":false}),
            );
        }
        ensure!(
            self.retirements.len() < MAX_RETIREMENTS,
            "candidate retirement receipt limit reached; preserve state"
        );
        let entry = self
            .entries
            .get(id)
            .context("candidate is not registered")?;
        ensure!(
            entry.record.version == 3
                && matches!(entry.record.origin, Origin::Candidate { bootstrap_commit } if bootstrap_commit == expected),
            "retirement requires the exact immutable candidate commit"
        );
        ensure!(
            entry.record.state == Lifecycle::Stopped
                && entry.mount.is_none()
                && entry.controller.is_none()
                && !entry.registered
                && entry.provider_failure.is_none()
                && entry.startup.is_none()
                && entry.command_startup.is_none()
                && entry.restore_intent.is_none()
                && entry.restore_receipt.is_none(),
            "stop and quiesce the candidate before retiring it"
        );
        ensure!(
            crate::recovery::identity(&entry.record.root)?.is_none(),
            "candidate kernel mount is still present"
        );
        let record = entry.record.clone();
        let registry = self.open_existing_registry()?;
        let binding = self.binding(&record)?;
        let repository = self
            .repositories
            .get(&record.store)
            .context("candidate repository unavailable")?
            .clone();
        let commands = self
            .commands
            .as_ref()
            .context("candidate retirement requires command admission fencing")?
            .clone();
        let access = access.context("candidate retirement requires command admission fencing")?;
        let admin_guard = registry.lock_candidate_retirement(&binding)?;
        for item in fs::read_dir(self.directory.path().join(id))? {
            let item = item?;
            ensure!(
                matches!(item.file_name().to_str(), Some("workspace.json" | "mount")),
                "unknown candidate state is preserved"
            );
        }
        self.directory
            .check_child_tree(id, MAX_CLEANUP_ENTRIES, MAX_CLEANUP_DEPTH)?;
        let mut namespace = native_vfs::lock_candidate_for_retirement(
            &self.directory.path().join(id).join("mount"),
            &record.root,
            identity,
            repository.scope.clone(),
            repository.io,
            self.cache.clone(),
        )?;
        let controller_config =
            native_vfs_controller::Config::new(record.store.clone(), binding.working_copy.clone());
        let controller_guard = native_vfs_controller::lock_candidate_retirement(
            &self.directory.path().join(id).join("mount"),
            &record.root,
            identity,
            repository.scope.backend(),
            namespace.root_tree,
            &controller_config,
        )?;
        let mut receipt = CandidateRetirement {
            version: 1,
            record,
            binding,
            root_tree: namespace.root_tree,
            complete: false,
            root_removed: false,
        };
        validate_retirement(&receipt, identity)?;
        namespace.close_namespace();
        let bytes = serde_json::to_vec(&receipt)?;
        ensure!(
            bytes.len() <= MAX_RECORD,
            "retirement receipt exceeds limit"
        );
        let reservation = commands
            .register_reserved_with_host_access(
                &receipt.binding,
                repository.identity,
                access.clone(),
            )
            .await?;
        let command_guard = reservation.validate_for_activation(
            &commands,
            &receipt.binding,
            repository.identity,
        )?;
        self.publish_retirement_intent(id, &receipt)?;
        // The namespace owner and command writer fences remain held until all
        // private files are removed. Restart replays this durable intent locally.
        registry.remove_candidate_admin(&receipt.binding, &admin_guard)?;
        self.directory
            .remove_child_tree(id, MAX_CLEANUP_ENTRIES, MAX_CLEANUP_DEPTH)?;
        receipt.root_removed = remove_empty_candidate_root(&receipt.record.root)?;
        receipt.complete = true;
        self.retirements_directory
            .replace(format!("{id}.json"), &serde_json::to_vec(&receipt)?)?;
        self.retirements.insert(id.into(), receipt.clone());
        self.entries.remove(id);
        drop((
            namespace,
            controller_guard,
            command_guard,
            admin_guard,
            access,
        ));
        Ok(
            serde_json::json!({"retired":true,"root_removed":receipt.root_removed,"retention_gc":false}),
        )
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
fn cloud_intent_key(root: &Path) -> Result<String> {
    let mut digest = blake3::Hasher::new();
    digest.update(b"native-cloud-restore-root-v1\0");
    digest.update(&serde_json::to_vec(root)?);
    Ok(hex(digest.finalize().as_bytes()))
}
fn validate_cloud_intent(intent: &CloudRestoreIntent) -> Result<()> {
    normalized(&intent.root)?;
    validate_selection(&intent.source)?;
    ensure!(
        intent.version == 1
            && intent.workspace_id != [0; 32]
            && intent.provider_identity != [0; 32]
            && !intent.store.is_empty()
            && intent.store.len() <= 64,
        "invalid cloud restore intent"
    );
    if let Some(requested) = &intent.requested_policy {
        requested.validate(&crate::native_restore_policy::Binding {
            root: &intent.root,
            store: &intent.store,
            workspace_id: intent.workspace_id,
            source: &intent.source,
            provider_identity: intent.provider_identity,
        })?;
    }
    Ok(())
}
fn validate_cloud_record(intent: &CloudRestoreIntent, record: &Record) -> Result<()> {
    let Origin::Restore { source, .. } = &record.origin else {
        anyhow::bail!("cloud restore reservation conflicts with workspace origin");
    };
    ensure!(
        intent.workspace_id == record.workspace_id
            && intent.root == record.root
            && intent.store == record.store
            && intent.provider_identity == source.provider_identity()
            && intent.source == selection_from_identity(source),
        "cloud restore reservation conflicts with workspace record"
    );
    ensure!(
        intent.requested_policy.as_ref()
            == record
                .restore_enrollment
                .as_ref()
                .map(|value| &value.requested),
        "cloud restore enrollment differs from saved host request"
    );
    Ok(())
}
fn persist_cloud_intent(directory: &PrivateDir, intent: &CloudRestoreIntent) -> Result<()> {
    validate_cloud_intent(intent)?;
    let name = format!("{}.json", cloud_intent_key(&intent.root)?);
    let encoded = serde_json::to_vec(intent)?;
    ensure!(
        encoded.len() <= MAX_RECORD,
        "cloud restore intent exceeds limit"
    );
    match fs::symlink_metadata(directory.path().join(&name)) {
        Ok(_) => ensure!(
            directory.read(&name, MAX_RECORD)? == encoded,
            "immutable cloud restore intent differs"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    // Persist directory creation as well as the atomic row publication. A
    // failure leaves the same in-memory reservation for an exact retry.
    fs::File::open(
        directory
            .path()
            .parent()
            .context("cloud intent parent missing")?,
    )?
    .sync_all()?;
    directory.replace(name, &encoded)?;
    Ok(())
}
fn load_cloud_intents(
    directory: &PrivateDir,
    max: usize,
) -> Result<BTreeMap<String, CloudRestoreIntent>> {
    check_cloud_directory(directory, max.saturating_add(128))?;
    let mut intents: BTreeMap<String, CloudRestoreIntent> = BTreeMap::new();
    let mut temporary = Vec::new();
    for (count, item) in fs::read_dir(directory.path())?.enumerate() {
        ensure!(
            count < max.saturating_add(128),
            "cloud restore intent directory limit exceeded"
        );
        let item = item?;
        let name = item
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("invalid cloud intent name"))?;
        if is_retirement_temporary(&name) {
            validate_cloud_temporary(directory, &name)?;
            temporary.push(name);
            ensure!(
                temporary.len() <= 128,
                "cloud intent temporary limit exceeded"
            );
            continue;
        }
        ensure!(intents.len() < max, "cloud restore intent limit exceeded");
        let key = name
            .strip_suffix(".json")
            .context("unknown cloud restore intent entry")?;
        unhex(key)?;
        let intent: CloudRestoreIntent =
            serde_json::from_slice(&directory.read(&name, MAX_RECORD)?)?;
        validate_cloud_intent(&intent)?;
        ensure!(
            cloud_intent_key(&intent.root)? == key,
            "cloud restore intent filename differs"
        );
        ensure!(
            !intents
                .values()
                .any(|old| old.workspace_id == intent.workspace_id
                    || overlaps(&old.root, &intent.root)),
            "cloud restore intent identities/roots overlap"
        );
        intents.insert(key.to_owned(), intent);
    }
    remove_cloud_temporaries(directory, temporary)?;
    Ok(intents)
}
fn validate_cloud_temporary(directory: &PrivateDir, name: &str) -> Result<()> {
    ensure!(
        is_retirement_temporary(name),
        "unknown cloud restore state; preserve it"
    );
    directory.read(name, MAX_RECORD)?; // bounded, regular and no-follow
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::symlink_metadata(directory.path().join(name))?;
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() } && metadata.nlink() == 1,
            "cloud restore temporary ownership/link count differs"
        );
    }
    Ok(())
}
fn remove_cloud_temporaries(directory: &PrivateDir, temporary: Vec<String>) -> Result<()> {
    if !temporary.is_empty() {
        for name in temporary {
            fs::remove_file(directory.path().join(name))?;
        }
        fs::File::open(directory.path())?.sync_all()?;
    }
    Ok(())
}
fn check_cloud_directory(directory: &PrivateDir, max: usize) -> Result<()> {
    let parent = PrivateDir::open_existing(
        directory
            .path()
            .parent()
            .context("cloud directory parent missing")?,
    )?;
    parent.check_child_tree(
        directory
            .path()
            .file_name()
            .context("cloud directory name missing")?,
        max,
        1,
    )?;
    Ok(())
}
fn reconcile_cloud_temporary_files(directory: &PrivateDir) -> Result<()> {
    // Only an exact durable auto-ID intent authorizes this pre-record case.
    // Validate the entire namespace before removing any recognized partial write.
    check_cloud_directory(directory, 128)?;
    let mut temporary = Vec::new();
    for (count, item) in fs::read_dir(directory.path())?.enumerate() {
        ensure!(count < 128, "cloud restore temporary limit exceeded");
        let name = item?
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("invalid cloud temporary name"))?;
        validate_cloud_temporary(directory, &name)?;
        temporary.push(name);
    }
    remove_cloud_temporaries(directory, temporary)
}
fn is_retirement_temporary(name: &str) -> bool {
    let Some(body) = name
        .strip_prefix(".private-")
        .and_then(|s| s.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((pid, sequence)) = body.split_once('-') else {
        return false;
    };
    !pid.is_empty()
        && !sequence.is_empty()
        && pid.bytes().all(|b| b.is_ascii_digit())
        && sequence.bytes().all(|b| b.is_ascii_digit())
        && pid.parse::<u32>().is_ok()
        && sequence.parse::<u64>().is_ok()
}
fn validate_retirement(receipt: &CandidateRetirement, id: [u8; 32]) -> Result<()> {
    validate_record(&receipt.record)?;
    ensure!(
        receipt.version == 1
            && receipt.record.version == 3
            && receipt.record.workspace_id == id
            && receipt.record.state == Lifecycle::Stopped
            && receipt.root_tree != [0; 32]
            && matches!(receipt.record.origin, Origin::Candidate { bootstrap_commit } if bootstrap_commit != [0;32])
            && receipt.binding.workspace_id == id
            && receipt.binding.root == receipt.record.root
            && receipt.binding.repository == receipt.record.store
            && receipt.binding.rpc_mount_id == hex(&id),
        "invalid candidate retirement intent"
    );
    Ok(())
}
fn remove_empty_candidate_root(root: &Path) -> Result<bool> {
    if !root.try_exists()? {
        return Ok(true);
    }
    let directory = PrivateDir::open_existing(root)?;
    if fs::read_dir(directory.path())?.next().is_some() {
        return Ok(false);
    }
    drop(directory);
    match fs::remove_dir(root) {
        Ok(()) => {
            fs::File::open(root.parent().context("candidate root parent missing")?)?.sync_all()?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => Ok(false),
        Err(error) => Err(error.into()),
    }
}
/// Replay only daemon-authored durable disposal intent; no provider or network.
fn finish_candidate_retirement(
    config: &Config,
    directory: &PrivateDir,
    receipt: &mut CandidateRetirement,
) -> Result<()> {
    #[cfg(not(target_os = "linux"))]
    anyhow::bail!("candidate retirement recovery requires native Linux mount proof");
    validate_retirement(receipt, receipt.record.workspace_id)?;
    ensure!(
        crate::recovery::identity(&receipt.record.root)?.is_none(),
        "candidate mount is still present during retirement recovery"
    );
    let registry_path = config
        .native
        .workspaces
        .registry_path
        .clone()
        .map(Ok)
        .unwrap_or_else(Registry::default_path)?;
    let registry = Registry::open_existing(registry_path)?;
    let admin = registry.lock_candidate_retirement(&receipt.binding)?;
    let lease_path =
        native_vfs_rpc::command_lock_path(&receipt.binding.admin, &receipt.binding.rpc_mount_id)?;
    let leases = PrivateDir::open_existing(
        lease_path
            .parent()
            .context("candidate lease parent missing")?,
    )?;
    let lease = leases.open_existing_file(
        lease_path.file_name().context("candidate lease missing")?,
        4096,
    )?;
    lease
        .try_lock_exclusive()
        .context("candidate writer remains active")?;
    let id = hex(&receipt.record.workspace_id);
    let state_path = directory.path().join(&id).join("mount");
    let owner = if state_path.try_exists()? {
        let state = PrivateDir::open_existing(&state_path)?;
        match state.open_existing_file("owner.lock", 4096) {
            Ok(lock) => {
                lock.try_lock_exclusive()
                    .context("candidate namespace remains owned")?;
                Some(lock)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        }
    } else {
        None
    };
    let controller_owner = if state_path.join("jj-controller").try_exists()? {
        let state = PrivateDir::open_existing(state_path.join("jj-controller"))?;
        match state.open_existing_file("owner.lock", 4096) {
            Ok(lock) => {
                lock.try_lock_exclusive()
                    .context("candidate controller remains owned")?;
                Some(lock)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        }
    } else {
        None
    };
    registry.remove_candidate_admin(&receipt.binding, &admin)?;
    directory.remove_child_tree(&id, MAX_CLEANUP_ENTRIES, MAX_CLEANUP_DEPTH)?;
    receipt.root_removed = remove_empty_candidate_root(&receipt.record.root)?;
    receipt.complete = true;
    drop((owner, controller_owner, lease, admin));
    Ok(())
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
            matches!(actual.version, 1 | 2)
                && actual.socket == expected.socket
                && actual.store == expected.store,
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
fn creation_policy(record: &Record) -> Result<Option<native_broker::CreationPolicy>> {
    if let Some(verified) = &record.restore_enrollment {
        return verified.requested.policy().map(Some);
    }
    record
        .creation_plan
        .as_deref()
        .map(native_broker::CreationPlan::decode)
        .transpose()
        .map(|plan| plan.map(|p| p.policy))
        .map_err(Into::into)
}

fn same_intent(a: &Record, b: &Record) -> bool {
    a.version == b.version
        && a.workspace_id == b.workspace_id
        && a.store == b.store
        && a.root == b.root
        && a.origin == b.origin
        && a.scope == b.scope
        && a.creation_plan == b.creation_plan
        && a.restore_enrollment == b.restore_enrollment
}
fn validate_record(record: &Record) -> Result<()> {
    ensure!(
        ((record.version == 2 && !matches!(record.origin, Origin::Candidate { .. }))
            || (record.version == 3
                && matches!(record.origin, Origin::Candidate { bootstrap_commit } if bootstrap_commit != [0; 32])))
            && record.workspace_id != [0; 32],
        "invalid native workspace record"
    );
    if let Some(bytes) = &record.creation_plan {
        let plan = native_broker::CreationPlan::decode(bytes)?;
        let Origin::Clone { bootstrap_commit } = record.origin else {
            anyhow::bail!("creation policy is only supported for clone origins");
        };
        ensure!(
            plan.root == bootstrap_commit,
            "saved native creation root differs"
        );
    }
    if let Some(verified) = &record.restore_enrollment {
        ensure!(
            record.creation_plan.is_none(),
            "restore cannot carry a public-root creation plan"
        );
        let Origin::Restore { source, .. } = &record.origin else {
            anyhow::bail!("automatic restore enrollment requires a restore origin");
        };
        verified.validate(
            &crate::native_restore_policy::Binding {
                root: &record.root,
                store: &record.store,
                workspace_id: record.workspace_id,
                source: &selection_from_identity(source),
                provider_identity: source.provider_identity(),
            },
            source,
        )?;
    }
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
        let mut upgraded: RestoreLocator = toml::from_str(std::str::from_utf8(&original)?)?;
        upgraded.version = 2;
        fs::write(
            store.join("native-broker.toml"),
            toml::to_string(&upgraded)?,
        )?;
        check(&store)?;
        upgraded.version = 3;
        fs::write(
            store.join("native-broker.toml"),
            toml::to_string(&upgraded)?,
        )?;
        assert!(check(&store).is_err());
        upgraded.version = 2;
        fs::write(
            store.join("native-broker.toml"),
            toml::to_string(&upgraded)?,
        )?;
        let (_, foreign) = restore_locator(directory.path().join("other.sock"), "example".into());
        assert!(foreign(&store).is_err());
        let alias = repository.path().join("alias");
        std::os::unix::fs::symlink(&store, &alias)?;
        assert!(check(&alias).is_err());
        Ok(())
    }
    #[test]
    fn pre_registry_cloud_intent_preserves_legacy_identity_without_adopting_per_id_state() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state = PrivateDir::create(temporary.path().join("state"))?;
        let native = PrivateDir::create(state.path().join("native"))?;
        PrivateDir::create(native.path().join("example"))?;
        let mut config = Config::default(); config.state_dir = state.path().to_owned();
        config.native.workspaces.registry_path = Some(temporary.path().join("absent-registry"));
        let mut repository = crate::config::NativeRepository::default();
        repository.remote = Some(Default::default());
        repository.remote.as_mut().unwrap().query.max_replays = 256;
        config.native.repositories.insert("example".into(),repository);
        let old = crate::provider_identity::resolve(&config,&BTreeMap::new(),&BTreeMap::new(),None)?["example"];
        let intent = CloudRestoreIntent { version:1,root:temporary.path().join("restored"),store:"example".into(),
            workspace_id:[7;32],provider_identity:old,requested_policy:None,
            source:crate::control::NativeRestoreSelection {workspace_id:[1;32],generation:1,
                manifest_id:[2;32],manifest_blake3:[3;32],manifest_size_bytes:256} };
        let intents = PrivateDir::create(state.path().join("native-cloud-restores"))?;
        persist_cloud_intent(&intents,&intent)?;
        let name = format!("{}.json",cloud_intent_key(&intent.root)?);
        let before = intents.read(&name,MAX_RECORD)?;
        config.native.repositories.get_mut("example").unwrap().remote.as_mut().unwrap().query.max_replays = 4096;
        assert_eq!(crate::provider_identity::resolve(&config,&BTreeMap::new(),&BTreeMap::new(),None)?["example"],old);
        assert_eq!(intents.read(&name,MAX_RECORD)?,before);
        assert_eq!(config.native.repositories["example"].remote.as_ref().unwrap().query.max_replays,4096);
        assert!(!intent.root.exists()); assert!(!config.native.workspaces.registry_path.as_ref().unwrap().exists());
        let workspaces = PrivateDir::create(state.path().join("native-workspaces"))?;
        PrivateDir::create(workspaces.path().join(hex(&intent.workspace_id)))?;
        assert!(crate::provider_identity::resolve(&config,&BTreeMap::new(),&BTreeMap::new(),None).is_err(),
            "an intent cannot excuse unaccounted per-ID state when its registry is missing");
        assert_eq!(intents.read(&name,MAX_RECORD)?,before);
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
    async fn cloud_restore_intent_survives_ambiguous_publication_and_partial_workspace_restart()
    -> Result<()> {
        for (name, automatic) in [
            ("legacy intent without enrollment", false),
            ("saved automatic contract", true),
        ] {
            exercise_cloud_restore_intent_restart(automatic)
                .await
                .with_context(|| name)?;
        }
        Ok(())
    }
    async fn exercise_cloud_restore_intent_restart(automatic: bool) -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let state = PrivateDir::create(temporary.path().join("state"))?;
        let objects = PrivateDir::create(temporary.path().join("objects"))?;
        let io = Arc::new(jj_native_backend::DirectObjectIo::init(
            objects.path(),
            Default::default(),
        )?);
        let cache = LocalCas::open(
            temporary.path().join("cas"),
            Arc::new(NoRemoteContent),
            Default::default(),
        )?;
        let mut config = Config::default();
        config.state_dir = state.path().to_owned();
        config.native.workspaces.max_workspaces = 2;
        let repositories = BTreeMap::from([(
            "example".into(),
            Repository {
                local_objects: io.clone(),
                upload: None,
                query: None,
                io,
                scope: Scope::from_configured_identity("example", &[8; 32])?,
                remote: true,
                identity: [8; 32],
            },
        )]);
        let mut manager = Manager::open(config.clone(), cache.clone(), repositories.clone())?;
        let root = temporary.path().join("restored");
        let id = [9; 32];
        let source = crate::control::NativeRestoreSelection {
            workspace_id: [1; 32],
            generation: 7,
            manifest_id: [2; 32],
            manifest_blake3: [3; 32],
            manifest_size_bytes: 256,
        };
        let mut intent = CloudRestoreIntent {
            requested_policy: None,
            version: 1,
            root: root.clone(),
            store: "example".into(),
            workspace_id: id,
            source: source.clone(),
            provider_identity: [8; 32],
        };
        if automatic {
            intent.requested_policy = Some(crate::native_restore_policy::Requested::resolve(
                &config.native.workspaces,
                &crate::native_restore_policy::Binding {
                    root: &root,
                    store: "example",
                    workspace_id: id,
                    source: &source,
                    provider_identity: [8; 32],
                },
                true,
            )?);
        }
        let key = cloud_intent_key(&root)?;
        // Model the exact post-selection/pre-publication boundary. No remote
        // provider is configured: a retry that reselects latest cannot succeed.
        manager.cloud_intents.insert(key.clone(), intent.clone());
        manager.after_cloud_intent_publication = Some(Box::new(|| {
            anyhow::bail!("intent sync acknowledgement lost")
        }));
        let error = manager
            .restore_cloud_version(
                root.clone(),
                "example".into(),
                hex(&source.workspace_id),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("intent sync acknowledgement lost")
        );
        let intent_path = state
            .path()
            .join("native-cloud-restores")
            .join(format!("{key}.json"));
        let saved = fs::read(&intent_path)?;
        assert_eq!(
            serde_json::from_slice::<CloudRestoreIntent>(&saved)?,
            intent
        );
        if automatic {
            let mut changed: serde_json::Value = serde_json::from_slice(&saved)?;
            changed["requested_policy"]["require_lazy_queries"] = false.into();
            manager.cloud_intents_directory.replace(
                intent_path.file_name().unwrap(),
                &serde_json::to_vec(&changed)?,
            )?;
            let error = Manager::open(config.clone(), cache.clone(), repositories.clone())
                .err()
                .context("mutated requested policy must fail startup")?;
            assert!(
                error
                    .to_string()
                    .contains("automatic restore policy binding differs")
            );
            assert!(!root.exists());
            manager
                .cloud_intents_directory
                .replace(intent_path.file_name().unwrap(), &saved)?;
        }
        assert!(!root.exists());
        assert!(
            !state
                .path()
                .join("native-workspaces")
                .join(hex(&id))
                .exists()
        );
        for _ in 0..2 {
            let error = manager
                .restore_cloud_version(
                    root.clone(),
                    "example".into(),
                    hex(&source.workspace_id),
                    None,
                )
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("requires managed command admission"),
                "{error:#}"
            );
            assert_eq!(manager.cloud_intents[&key], intent);
            assert_eq!(fs::read(&intent_path)?, saved);
        }
        for (store, workspace, generation) in [
            ("example", hex(&[4; 32]), None),
            ("example", hex(&source.workspace_id), Some(8)),
            ("other", hex(&source.workspace_id), None),
        ] {
            assert!(
                manager
                    .restore_cloud_version(root.clone(), store.into(), workspace, generation)
                    .await
                    .is_err()
            );
            assert_eq!(fs::read(&intent_path)?, saved);
        }
        manager.repositories.get_mut("example").unwrap().identity = [4; 32];
        assert!(
            manager
                .restore_cloud_version(
                    root.clone(),
                    "example".into(),
                    hex(&source.workspace_id),
                    None
                )
                .await
                .is_err()
        );
        manager.repositories.get_mut("example").unwrap().identity = [8; 32];
        assert!(
            manager
                .create(
                    root.clone(),
                    "example".into(),
                    hex(&[0; 32]),
                    hex(&[11; 32])
                )
                .await
                .is_err()
        );
        assert!(!root.exists());
        drop(manager);

        // A crash before workspace.json publication can leave an empty ID
        // directory or an incomplete recognized atomic write. Unknown files
        // prohibit ALL cleanup, even if a recognized temporary is also present.
        let child = PrivateDir::create(state.path().join("native-workspaces").join(hex(&id)))?;
        fs::write(child.path().join(".private-123-0.tmp"), b"{partial")?;
        fs::write(child.path().join("foreign"), b"preserve")?;
        assert!(Manager::open(config.clone(), cache.clone(), repositories.clone()).is_err());
        assert_eq!(fs::read(child.path().join("foreign"))?, b"preserve");
        assert_eq!(
            fs::read(child.path().join(".private-123-0.tmp"))?,
            b"{partial"
        );
        fs::remove_file(child.path().join("foreign"))?;
        let foreign = temporary.path().join("foreign-target");
        fs::write(&foreign, b"foreign target must survive")?;
        let linked_temporary = child.path().join(".private-123-1.tmp");
        std::os::unix::fs::symlink(&foreign, &linked_temporary)?;
        assert!(Manager::open(config.clone(), cache.clone(), repositories.clone()).is_err());
        assert!(
            fs::symlink_metadata(&linked_temporary)?
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&foreign)?, b"foreign target must survive");
        assert!(child.path().join(".private-123-0.tmp").exists());
        fs::remove_file(&linked_temporary)?;
        fs::hard_link(&foreign, &linked_temporary)?;
        assert!(Manager::open(config.clone(), cache.clone(), repositories.clone()).is_err());
        assert_eq!(fs::read(&linked_temporary)?, b"foreign target must survive");
        assert_eq!(fs::read(&foreign)?, b"foreign target must survive");
        assert!(child.path().join(".private-123-0.tmp").exists());
        fs::remove_file(&linked_temporary)?;
        let mut reopened = Manager::open(config.clone(), cache.clone(), repositories.clone())?;
        assert!(fs::read_dir(child.path())?.next().is_none());
        assert_eq!(reopened.cloud_intents[&key], intent);
        let error = reopened
            .restore_cloud_version(
                root.clone(),
                "example".into(),
                hex(&source.workspace_id),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("requires managed command admission")
        );
        assert_eq!(fs::read(&intent_path)?, saved);
        drop(reopened);
        // A pin without a workspace row still consumes its configured slot.
        let mut at_capacity = config.clone();
        at_capacity.native.workspaces.max_workspaces = 1;
        let mut reserved = Manager::open(at_capacity, cache.clone(), repositories.clone())?;
        let other_root = temporary.path().join("other-restored");
        let error = reserved
            .restore_cloud_version(
                other_root.clone(),
                "example".into(),
                hex(&source.workspace_id),
                None,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("reservation limit reached"));
        assert!(!other_root.exists());
        assert_eq!(reserved.cloud_intents.len(), 1);
        assert_eq!(fs::read(&intent_path)?, saved);
        drop(reserved);
        // Reopening the now-empty known ID directory is idempotent.
        drop(Manager::open(
            config.clone(),
            cache.clone(),
            repositories.clone(),
        )?);
        assert!(!root.exists());

        // A visible initial workspace record is authoritative after restart,
        // including installations without an auto-ID outbox (compatibility).
        // Synthetic source DATA tests journal recovery, not authenticated restore.
        let identity: native_workspace_admin::restore::RestoreSourceIdentity =
            serde_json::from_value(serde_json::json!({
                "workspace":source.workspace_id,"generation":source.generation,
                "manifest":source.manifest_id,"manifest_blake3":source.manifest_blake3,
                "manifest_bytes":source.manifest_size_bytes,"capture":vec![5;32],
                "capture_blake3":vec![6;32],"capture_bytes":512,"provider":vec![8;32]
            }))?;
        let restore_enrollment = intent
            .requested_policy
            .clone()
            .map(|requested| {
                crate::native_restore_policy::Verified::seal(
                    &crate::native_restore_policy::Binding {
                        root: &root,
                        store: "example",
                        workspace_id: id,
                        source: &source,
                        provider_identity: [8; 32],
                    },
                    requested,
                    identity.clone(),
                )
            })
            .transpose()?;
        let record = Record {
            creation_plan: None,
            restore_enrollment,
            version: 2,
            workspace_id: id,
            root: root.clone(),
            store: "example".into(),
            scope: repositories["example"].scope.backend().into(),
            state: Lifecycle::Pending,
            native_mount: None,
            origin: Origin::Restore {
                source: identity,
                seed: tinytree_cloud::PortableWorkingCopy {
                    workspace: "default".into(),
                    wc_operation: "00".repeat(64),
                    snapshot_operation: "00".repeat(64),
                    snapshot_commit: [5; 32],
                    tree: tinytree_cloud::CaptureTree {
                        terms: vec![[6; 32]],
                        labels: vec![],
                    },
                },
                committed: false,
            },
        };
        child.replace("workspace.json", &serde_json::to_vec(&record)?)?;
        for outbox in [true, false] {
            if !outbox {
                fs::remove_file(&intent_path)?;
            }
            let mut reopened = Manager::open(config.clone(), cache.clone(), repositories.clone())?;
            assert_eq!(reopened.entries[&hex(&id)].record, record);
            let error = reopened
                .restore_cloud_version(
                    root.clone(),
                    "example".into(),
                    hex(&source.workspace_id),
                    None,
                )
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("requires managed command admission")
            );
            assert_eq!(reopened.entries.len(), 1);
            assert_eq!(
                fs::read_dir(state.path().join("native-workspaces"))?.count(),
                1
            );
        }
        // Duplicate or nested recorded roots must not be selected by map order.
        let other =
            PrivateDir::create(state.path().join("native-workspaces").join(hex(&[10; 32])))?;
        for other_root in [root.clone(), root.join("nested")] {
            let mut duplicate = record.clone();
            duplicate.workspace_id = [10; 32];
            duplicate.root = other_root;
            other.replace("workspace.json", &serde_json::to_vec(&duplicate)?)?;
            assert!(Manager::open(config.clone(), cache.clone(), repositories.clone()).is_err());
            assert_eq!(
                read_record(&child.read("workspace.json", MAX_RECORD)?)?,
                record
            );
        }
        Ok(())
    }
    #[tokio::test]
    async fn ambiguous_retirement_publication_keeps_identity_closed_without_cleanup() -> Result<()>
    {
        let directory = tempfile::tempdir()?;
        let state = PrivateDir::create(directory.path().join("state"))?;
        let objects = PrivateDir::create(directory.path().join("objects"))?;
        let provider = Arc::new(jj_native_backend::DirectObjectIo::init(
            objects.path(),
            Default::default(),
        )?);
        let cache = LocalCas::open(
            directory.path().join("cas"),
            Arc::new(NoRemoteContent),
            Default::default(),
        )?;
        let mut config = Config::default();
        config.state_dir = state.path().to_owned();
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
        )?;
        let id = [9; 32];
        let name = hex(&id);
        let root = directory.path().join("candidate");
        drop(PrivateDir::create(&root)?);
        fs::write(root.join("foreign"), b"preserve")?;
        let registry = Registry::create(directory.path().join("registry"))?;
        let binding = registry.binding(
            id,
            &root,
            "example".into(),
            LocalEndpoint::UnixSocket(state.path().join("rbe.sock")),
            name.clone(),
        )?;
        let record = Record {
            creation_plan: None,
            restore_enrollment: None,
            version: 3,
            workspace_id: id,
            store: "example".into(),
            root: root.clone(),
            origin: Origin::Candidate {
                bootstrap_commit: [7; 32],
            },
            scope: format!("native-workspace-v1:{name}"),
            state: Lifecycle::Stopped,
            native_mount: None,
        };
        let child = PrivateDir::create(manager.directory.path().join(&name))?;
        child.replace("workspace.json", &serde_json::to_vec(&record)?)?;
        manager.entries.insert(
            name.clone(),
            Entry {
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
            },
        );
        let receipt = CandidateRetirement {
            version: 1,
            record,
            binding,
            root_tree: [6; 32],
            complete: false,
            root_removed: false,
        };
        // Model replace's ambiguous after-rename directory-fsync error at the
        // persistence boundary, without depending on a native mounted provider.
        manager.after_retirement_publication = Some(Box::new(|| {
            anyhow::bail!("injected after-rename publication failure")
        }));
        assert!(manager.publish_retirement_intent(&name, &receipt).is_err());
        let persisted: CandidateRetirement = serde_json::from_slice(
            &manager
                .retirements_directory
                .read(format!("{name}.json"), MAX_RECORD)?,
        )?;
        assert!(!persisted.complete);
        assert!(
            manager
                .create(root.clone(), "example".into(), hex(&[7; 32]), name.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("retired")
        );
        assert!(
            manager
                .create_candidate(root.clone(), "example".into(), hex(&[7; 32]), name.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("retired")
        );
        assert!(
            manager
                .resume_root(root.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("retired")
        );
        assert!(
            manager
                .directory
                .path()
                .join(&name)
                .join("workspace.json")
                .exists()
        );
        assert_eq!(fs::read(root.join("foreign"))?, b"preserve");
        Ok(())
    }

    #[tokio::test]
    async fn changed_mountpoint_after_preparation_retains_command_identity_and_allows_stop()
    -> Result<()> {
        use native_vfs_rpc::CommandHost as _;
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
        // The client has frozen a plan, but the host has not admitted its first
        // record. A changed creation policy must preserve that exact intent,
        // rather than adopting the caller's booleans or choosing a new policy.
        let policy_root = directory.path().join("pending-policy");
        drop(PrivateDir::create(&policy_root)?);
        let registry = Registry::create(directory.path().join("registry"))?;
        let plan = manager
            .config
            .native
            .workspaces
            .creation_description([8; 32], false, false)
            .creation_plan([0; 32])?;
        let pending = registry.begin_provisioning_with_plan(
            &policy_root,
            "example".into(),
            LocalEndpoint::UnixSocket(state.path().join("rbe.sock")),
            &jj_lib::backend::CommitId::new(vec![0; 32]),
            plan,
        )?;
        let pending_id = pending.workspace_id();
        drop(pending);
        manager.config.native.workspaces.index_mode = crate::config::NativeIndexMode::Legacy;
        let error = manager
            .create_planned(
                policy_root.clone(),
                "example".into(),
                "0".repeat(64),
                hex(&pending_id),
                plan.encode(),
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("policy changed before first host admission")
        );
        assert!(
            !state
                .path()
                .join("native-workspaces")
                .join(hex(&pending_id))
                .exists()
        );
        assert!(!policy_root.join(".jj").exists());
        let pending = registry
            .pending_provisioning(&policy_root)?
            .expect("original pending intent");
        assert_eq!(pending.workspace_id(), pending_id);
        assert_eq!(pending.creation_plan()?, Some(plan));
        drop(pending);
        manager.config.native.workspaces.index_mode = crate::config::NativeIndexMode::Auto;
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
        // Hold the actual registry cleanup write owner, not a synthetic Busy
        // status. These synced opaque bundles only arrange the admission fence;
        // this fixture does not claim remote backup or native mount acceptance.
        let retention = commands.capture_registry();
        let registration = retention
            .register(
                native_retention::WorkspaceId([90; 16]),
                native_retention::OpStoreId([91; 16]),
            )
            .await?;
        let capsules = PrivateDir::create(state.path().join("maintenance-fixture"))?;
        let mut tickets = Vec::new();
        for seed in [1u8, 2] {
            let bytes = [seed; 32];
            capsules.replace(format!("bundle-{seed}"), &bytes)?;
            let command = retention.begin_command(registration).await?;
            tickets.push(
                retention
                    .capture_dirty(command.token(), move || async move {
                        Ok(native_retention::CaptureBundle {
                            checkpoint: native_retention::RootCheckpoint::default(),
                            bundle: native_retention::BundleDescriptor {
                                blake3: *blake3::hash(&bytes).as_bytes(),
                                size_bytes: 32,
                            },
                        })
                    })
                    .await?,
            );
            drop(command);
        }
        retention
            .retire_redundant_capture(
                tickets[0].clone(),
                tickets[1].clone(),
                native_retention::CaptureUploadCleanup {
                    job: None,
                    published: Some(native_retention::BundleDescriptor {
                        blake3: [92; 32],
                        size_bytes: 64,
                    }),
                },
            )
            .await?;
        let maintenance = retention.try_capture_cleanup().await?.unwrap();
        // Arrange the real filesystem race only AFTER preparation checked the
        // empty directory. Actual provider preflight must then reject it before
        // /dev/fuse; no fake provider or native kernel capability is involved.
        let error = {
            let mut creating =
                Box::pin(manager.create(root, "example".into(), "0".repeat(64), id.clone()));
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut creating)
                    .await
                    .is_err()
            );
            assert!(
                !state
                    .path()
                    .join("native-workspaces")
                    .join(&id)
                    .join("workspace.json")
                    .exists(),
                "pending create cannot publish a lifecycle record"
            );
            assert!(
                !preserved.exists(),
                "provider preparation cannot run before maintenance admission"
            );
            drop(maintenance);
            creating
                .await
                .expect_err("changed mountpoint must be rejected")
        };
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
        {
            let maintenance = retention.try_capture_cleanup().await?.unwrap();
            let record_path = manager.entries[&id].directory.path().join("workspace.json");
            let before = fs::read(&record_path)?;
            let mut stopping = Box::pin(manager.stop(&id));
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut stopping)
                    .await
                    .is_err()
            );
            assert_eq!(
                fs::read(&record_path)?,
                before,
                "pending stop does not rewrite owned provider state"
            );
            drop(maintenance);
            stopping.await?;
        }
        assert!(!manager.active());
        assert_eq!(manager.status()[0]["state"], "stopped");
        assert_eq!(manager.status()[0]["provider_recovery_owned"], false);
        assert_eq!(fs::read(&preserved)?, b"preserve this file");
        {
            let maintenance = retention.try_capture_cleanup().await?.unwrap();
            let path = manager.entries[&id].directory.path().join("workspace.json");
            let before = fs::read(&path)?;
            let recovery_id = id.clone();
            let mut recovery = tokio::task::spawn_blocking(move || {
                let result = manager.recover(&recovery_id);
                (manager, result)
            });
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut recovery)
                    .await
                    .is_err()
            );
            assert_eq!(
                fs::read(&path)?,
                before,
                "pending recovery cannot publish stopped state"
            );
            drop(maintenance);
            let (returned, result) = recovery.await?;
            manager = returned;
            result?;
        }
        // A real same-workspace command lease remains a writer conflict, not
        // capture maintenance to wait/retry through.
        let binding = manager.binding(&manager.entries[&id].record)?;
        let lease_path = native_vfs_rpc::command_lock_path(&binding.admin, &id)?;
        let lease = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(lease_path)?;
        let preparation = commands.prepare(&id, [93; 32]).await?;
        lease.try_lock_exclusive()?;
        let started = commands.begin(&id, [93; 32], preparation).await?;
        assert!(
            manager
                .stop(&id)
                .await
                .unwrap_err()
                .to_string()
                .contains("active command writers")
        );
        drop(lease);
        commands.finish_dirty(&id, started.token).await?;
        retention
            .try_capture_cleanup()
            .await?
            .unwrap()
            .acknowledge_capture_cleanup()
            .await?;
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
