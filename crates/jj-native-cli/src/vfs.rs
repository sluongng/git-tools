//! Native workspace selection. Materialized state is restricted to the declared
//! protocol fixture binary, never the shipped CLI or a user-selectable fallback.
use crate::broker::{self, Locator};
use jj_cli::{
    cli_util::CommandHelper,
    command_error::{CommandError, user_error},
    ui::Ui,
};
use jj_lib::{
    backend::{Backend, BackendInitError, CommitId},
    repo::ReadonlyRepo,
    settings::UserSettings,
    signing_factory::signer_from_settings,
    workspace::{Workspace, WorkspaceInitError},
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

fn failure(error: impl std::fmt::Display) -> CommandError {
    user_error(format!("Native lazy workspace: {error}"))
}

#[cfg(feature = "protocol-fixture")]
pub(crate) async fn initialize(
    ui: &Ui,
    command: &CommandHelper,
    destination: &Path,
    locator: Option<Locator>,
    _initial: &CommitId,
    _plan: Option<native_broker::CreationPlan>,
) -> Result<(Workspace, Arc<ReadonlyRepo>), CommandError> {
    let (settings, _) = command.settings_for_new_workspace(ui, destination)?;
    let initializer =
        |_: &UserSettings, path: &Path| -> Result<Box<dyn Backend>, BackendInitError> {
            match &locator {
                Some(locator) => broker::init(path, locator.clone()),
                None => Ok(Box::new(jj_native_backend::NativeBackend::init(
                    path,
                    Default::default(),
                )?)),
            }
        };
    Ok(Workspace::init_with_backend(
        &settings,
        destination,
        &initializer,
        signer_from_settings(&settings).map_err(WorkspaceInitError::SignInit)?,
    )
    .await?)
}

#[cfg(not(feature = "protocol-fixture"))]
pub(crate) async fn initialize(
    ui: &Ui,
    command: &CommandHelper,
    destination: &Path,
    locator: Option<Locator>,
    initial: &CommitId,
    plan: Option<native_broker::CreationPlan>,
) -> Result<(Workspace, Arc<ReadonlyRepo>), CommandError> {
    use jj_lib::ref_name::WorkspaceName;
    let locator = locator.ok_or_else(|| {
        user_error(
            "native initialization requires --broker and --store for a daemon-owned lazy mount",
        )
    })?;
    #[cfg(not(unix))]
    {
        let _ = (ui, command, destination, initial, locator);
        Err(user_error(
            "native workspace provisioning is unavailable on this platform; no checkout was created",
        ))
    }
    #[cfg(unix)]
    {
        let plan = plan.ok_or_else(|| failure("native creation requires a checked plan"))?;
        let registry = provisioning_registry().map_err(failure)?;
        let mut intent = registry
            .begin_provisioning_with_plan(
                destination,
                locator.store.clone(),
                native_broker::LocalEndpoint::UnixSocket(locator.socket.clone()),
                initial,
                plan,
            )
            .map_err(failure)?;
        let binding = mount(
            &registry,
            intent.root(),
            intent.repository(),
            intent.endpoint(),
            &intent.initial_commit(),
            intent.workspace_id(),
            Some(plan),
        )?;
        crate::commands::ensure_binding(&binding).map_err(failure)?;
        // A crash can occur after Ready publication but before intent completion.
        // Inspect the exact record rather than initializing existing jj state.
        let ready_record = registry
            .path()
            .join("records")
            .join(format!("{}.json", binding.id()));
        if ready_record.try_exists().map_err(failure)? {
            let ready = registry.resume_info(&binding.root).map_err(failure)?
                .ok_or_else(|| failure("no Ready native workspace registration; preserve pending initialization and reconcile explicitly"))?;
            if ready.binding != binding || ready.initial_commit != *initial {
                return Err(failure("Ready workspace differs from provisioning intent"));
            }
            let loaded = load_ready(command, &registry, &binding).await?;
            intent.complete(&binding).map_err(failure)?;
            return Ok(loaded);
        }
        let connector = connector(&binding).map_err(failure)?;
        let (settings, _) = command.settings_for_new_workspace(ui, &binding.root)?;
        let initializer =
            |_: &UserSettings, path: &Path| -> Result<Box<dyn Backend>, BackendInitError> {
                broker::init(path, locator.clone())
            };
        let mode = intent.creation_plan().map_err(failure)?.ok_or_else(|| failure("pending native creation lacks its saved policy; preserve and reconcile explicitly"))?.index_mode;
        let index_initializer =
            |settings: &UserSettings, path: &Path, operation: &jj_lib::op_store::OperationId| {
                broker::init_index(path, mode, settings, initial, operation)
            };
        let initialized = native_workspace_admin::initialize_with_index(
            &registry,
            &binding,
            &settings,
            &initializer,
            signer_from_settings(&settings).map_err(WorkspaceInitError::SignInit)?,
            connector,
            initial,
            WorkspaceName::DEFAULT.to_owned(),
            &index_initializer,
        )
        .await
        .map_err(failure)?;
        intent.complete(&binding).map_err(failure)?;
        Ok(initialized)
    }
}

#[cfg(not(feature = "protocol-fixture"))]
pub(crate) fn connector(
    binding: &native_workspace_admin::Binding,
) -> anyhow::Result<Arc<dyn jj_vfs::native::ProjectionConnector>> {
    let token = crate::commands::ensure_binding(binding)?;
    Ok(Arc::new(
        native_vfs_rpc::ClientConnector::new(
            binding.endpoint.local(),
            binding.rpc_mount_id.clone(),
            Default::default(),
        )?
        .with_command_token(token),
    ))
}

pub(crate) fn working_copy_path(root: &Path) -> anyhow::Result<PathBuf> {
    #[cfg(feature = "protocol-fixture")]
    {
        Ok(root.join(".jj/working_copy"))
    }
    #[cfg(not(feature = "protocol-fixture"))]
    {
        Ok(native_workspace_admin::Registry::open_existing(
            native_workspace_admin::Registry::default_path()?,
        )?
        .lookup(root)?
        .working_copy)
    }
}

#[cfg(not(feature = "protocol-fixture"))]
fn loader() -> anyhow::Result<native_workspace_admin::LoaderFactory> {
    let registry = native_workspace_admin::Registry::open_existing(
        native_workspace_admin::Registry::default_path()?,
    )?;
    Ok(native_workspace_admin::LoaderFactory::new(
        registry,
        Arc::new(connector),
    ))
}

#[cfg(not(feature = "protocol-fixture"))]
pub(crate) struct Loader;
#[cfg(not(feature = "protocol-fixture"))]
impl jj_lib::workspace::WorkspaceLoaderFactory for Loader {
    fn create(
        &self,
        root: &Path,
    ) -> Result<Box<dyn jj_lib::workspace::WorkspaceLoader>, jj_lib::workspace::WorkspaceLoadError>
    {
        use jj_lib::workspace::WorkspaceLoadError;
        if !root.join(".jj/native-workspace").exists() {
            return Err(WorkspaceLoadError::NoWorkspaceHere(root.to_owned()));
        }
        let registry = native_workspace_admin::Registry::open_existing(
            native_workspace_admin::Registry::default_path().map_err(load_error)?,
        )
        .map_err(load_error)?;
        let binding = registry.lookup(root).map_err(load_error)?;
        // jj eagerly selects this loader before parsing the command. Selection
        // may resolve registered paths, but must not acquire a command lease or
        // connect the projection for maintenance-only commands.
        Ok(Box::new(SelectedLoader { binding }))
    }
}
#[cfg(not(feature = "protocol-fixture"))]
struct SelectedLoader {
    binding: native_workspace_admin::Binding,
}
#[cfg(not(feature = "protocol-fixture"))]
impl jj_lib::workspace::WorkspaceLoader for SelectedLoader {
    fn workspace_root(&self) -> &Path {
        &self.binding.root
    }
    fn repo_path(&self) -> &Path {
        &self.binding.repo
    }
    fn get_working_copy_type(&self) -> Result<String, jj_lib::repo::StoreLoadError> {
        Ok(jj_vfs::native::NAME.to_owned())
    }
    fn load(
        &self,
        settings: &UserSettings,
        stores: &jj_lib::repo::StoreFactories,
        working_copies: &jj_lib::workspace::WorkingCopyFactories,
    ) -> Result<Workspace, jj_lib::workspace::WorkspaceLoadError> {
        use jj_lib::workspace::WorkspaceLoaderFactory as _;
        // Actual loading retains the original registry validation, command
        // admission and live projection/mount attestation. Direct callers do
        // not bypass the guard merely by skipping the ordinary dispatch hook.
        let registry = native_workspace_admin::Registry::open_existing(
            native_workspace_admin::Registry::default_path().map_err(load_error)?,
        )
        .map_err(load_error)?;
        if registry.lookup(&self.binding.root).map_err(load_error)? != self.binding {
            return Err(load_error(anyhow::anyhow!(
                "native workspace selection changed before load"
            )));
        }
        loader()
            .map_err(load_error)?
            .create(&self.binding.root)?
            .load(settings, stores, working_copies)
    }
}

#[cfg(not(feature = "protocol-fixture"))]
pub(crate) struct WorkingCopyFactory;
#[cfg(not(feature = "protocol-fixture"))]
impl jj_lib::working_copy::WorkingCopyFactory for WorkingCopyFactory {
    fn init_working_copy(
        &self,
        _: Arc<jj_lib::store::Store>,
        _: PathBuf,
        _: PathBuf,
        _: jj_lib::op_store::OperationId,
        _: jj_lib::ref_name::WorkspaceNameBuf,
        _: &UserSettings,
    ) -> Result<
        Box<dyn jj_lib::working_copy::WorkingCopy>,
        jj_lib::working_copy::WorkingCopyStateError,
    > {
        Err(state_error(anyhow::anyhow!(
            "native workspace add requires daemon provisioning"
        )))
    }
    fn load_working_copy(
        &self,
        store: Arc<jj_lib::store::Store>,
        root: PathBuf,
        state: PathBuf,
        settings: &UserSettings,
    ) -> Result<
        Box<dyn jj_lib::working_copy::WorkingCopy>,
        jj_lib::working_copy::WorkingCopyStateError,
    > {
        let registry = native_workspace_admin::Registry::open_existing(
            native_workspace_admin::Registry::default_path().map_err(state_error)?,
        )
        .map_err(state_error)?;
        let binding = registry.lookup(&root).map_err(state_error)?;
        crate::commands::ensure_binding(&binding).map_err(state_error)?;
        loader()
            .map_err(state_error)?
            .working_copy_factory()
            .load_working_copy(store, root, state, settings)
    }
}
#[cfg(not(feature = "protocol-fixture"))]
fn state_error(error: anyhow::Error) -> jj_lib::working_copy::WorkingCopyStateError {
    jj_lib::working_copy::WorkingCopyStateError {
        message: format!("Native workspace: {error:#}"),
        err: error.into(),
    }
}
#[cfg(not(feature = "protocol-fixture"))]
fn load_error(error: anyhow::Error) -> jj_lib::workspace::WorkspaceLoadError {
    jj_lib::workspace::WorkspaceLoadError::WorkingCopyState(state_error(error))
}

#[cfg(all(unix, not(feature = "protocol-fixture")))]
fn provisioning_registry() -> anyhow::Result<Arc<native_workspace_admin::Registry>> {
    let path = native_workspace_admin::Registry::default_path()?;
    let mut missing = vec![];
    let mut parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("registry has no parent"))?;
    while !parent.try_exists()? {
        missing.push(parent.to_path_buf());
        parent = parent
            .parent()
            .ok_or_else(|| anyhow::anyhow!("registry has no existing ancestor"))?;
    }
    for parent in missing.into_iter().rev() {
        private_fs::PrivateDir::create(parent)?;
    }
    native_workspace_admin::Registry::create(path)
}

#[cfg(all(unix, not(feature = "protocol-fixture")))]
fn control_call(
    endpoint: native_broker::LocalEndpoint,
    request: workspace_daemon::control::Request,
) -> Result<serde_json::Value, CommandError> {
    let native_broker::LocalEndpoint::UnixSocket(socket) = endpoint else {
        return Err(failure(
            "this daemon control transport is unsupported; no checkout was created",
        ));
    };
    let control = socket
        .parent()
        .ok_or_else(|| failure("invalid private broker path"))?
        .join("control.sock");
    std::thread::spawn(move || workspace_daemon::control::Client { socket: control }.call(request))
        .join()
        .map_err(|_| failure("workspace control thread failed"))?
        .map_err(failure)
}

#[cfg(all(unix, not(feature = "protocol-fixture")))]
fn mount(
    registry: &native_workspace_admin::Registry,
    root: &Path,
    repository: &str,
    endpoint: native_broker::LocalEndpoint,
    initial: &CommitId,
    id: [u8; 32],
    plan: Option<native_broker::CreationPlan>,
) -> Result<native_workspace_admin::Binding, CommandError> {
    use jj_lib::object_id::ObjectId;
    #[derive(serde::Deserialize)]
    struct Created {
        binding: native_workspace_admin::Binding,
        registry_path: PathBuf,
    }
    let workspace_id: String = id.iter().map(|b| format!("{b:02x}")).collect();
    let response = control_call(
        endpoint.clone(),
        match plan {
            Some(plan) => workspace_daemon::control::Request::NativeMountPlanned {
                root: root.to_owned(),
                store: repository.into(),
                commit: initial.hex(),
                workspace_id: workspace_id.clone(),
                creation_plan: plan.encode(),
            },
            None => workspace_daemon::control::Request::NativeMount {
                root: root.to_owned(),
                store: repository.into(),
                commit: initial.hex(),
                workspace_id: workspace_id.clone(),
            },
        },
    )?;
    let created: Created = serde_json::from_value(response).map_err(failure)?;
    let returned =
        native_workspace_admin::Registry::open_existing(created.registry_path).map_err(failure)?;
    if returned.path() != registry.path() {
        return Err(failure(
            "daemon registry differs from TINYTREE_REGISTRY; preserve the provisioning intent and mounted state",
        ));
    }
    let expected = registry
        .binding(id, root, repository.into(), endpoint, workspace_id)
        .map_err(failure)?;
    if created.binding != expected {
        return Err(failure(
            "daemon returned a different workspace binding; preserve state and reconcile explicitly",
        ));
    }
    Ok(created.binding)
}

#[cfg(all(unix, not(feature = "protocol-fixture")))]
async fn load_ready(
    command: &CommandHelper,
    registry: &Arc<native_workspace_admin::Registry>,
    binding: &native_workspace_admin::Binding,
) -> Result<(Workspace, Arc<ReadonlyRepo>), CommandError> {
    use jj_lib::workspace::WorkspaceLoaderFactory;
    crate::commands::ensure_binding(binding).map_err(failure)?;
    let factory = native_workspace_admin::LoaderFactory::new(registry.clone(), Arc::new(connector));
    let loader = factory.create(&binding.root).map_err(failure)?;
    let mut stores = jj_lib::default_backend_factories::default_backend_factories();
    broker::register(&mut stores);
    let workspace = loader
        .load(command.settings(), &stores, &Default::default())
        .map_err(failure)?;
    let repo = workspace
        .repo_loader()
        .load_at_head()
        .await
        .map_err(failure)?;
    Ok((workspace, repo))
}

pub(crate) fn restore_identity(text: &str) -> Result<[u8; 32], CommandError> {
    if text.len() != 64
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(failure(
            "restore identities require 64 lowercase hexadecimal digits",
        ));
    }
    Ok(*blake3::Hash::from_hex(text).map_err(failure)?.as_bytes())
}
pub(crate) fn restore_control(
    config_path: &Path,
) -> Result<workspace_daemon::control::ScopedClient, CommandError> {
    let config = workspace_daemon::config::Config::load(config_path).map_err(failure)?;
    let expected = native_workspace_admin::Registry::default_path().map_err(failure)?;
    let configured = config
        .native
        .workspaces
        .registry_path
        .clone()
        .unwrap_or_else(|| expected.clone());
    if configured != expected {
        return Err(failure(
            "daemon registry differs from TINYTREE_REGISTRY; select its matching registry before restore/resume",
        ));
    }
    let client = workspace_daemon::control::ScopedClient::new(&config);
    client.validate_identity().map_err(failure)?;
    Ok(client)
}
pub(crate) fn restored_response(
    value: serde_json::Value,
    destination: &Path,
    store: Option<&str>,
    id: Option<[u8; 32]>,
) -> Result<native_workspace_admin::Binding, CommandError> {
    #[derive(serde::Deserialize)]
    struct Mounted {
        binding: native_workspace_admin::Binding,
        registry_path: PathBuf,
    }
    let mounted: Mounted = serde_json::from_value(value).map_err(failure)?;
    let registry =
        native_workspace_admin::Registry::open_existing(&mounted.registry_path).map_err(failure)?;
    let expected = native_workspace_admin::Registry::default_path().map_err(failure)?;
    if registry.path() != expected
        || mounted.binding.root != destination.canonicalize().map_err(failure)?
        || store.is_some_and(|s| mounted.binding.repository != s)
        || id.is_some_and(|id| mounted.binding.workspace_id != id)
    {
        return Err(failure(
            "daemon returned a different native restore binding; preserve state",
        ));
    }
    if registry.lookup(&mounted.binding.root).map_err(failure)? != mounted.binding {
        return Err(failure(
            "native restore Ready registry differs; preserve state",
        ));
    }
    Ok(mounted.binding)
}
pub(crate) async fn restore(
    ui: &Ui,
    command: &CommandHelper,
    destination: &Path,
    daemon_config: PathBuf,
    store: String,
    workspace_id: String,
    source_workspace: String,
    generation: u64,
    manifest: String,
    manifest_blake3: String,
    manifest_bytes: u64,
) -> Result<(), CommandError> {
    reject_recovery_overrides(command)?;
    #[cfg(any(not(target_os = "linux"), feature = "protocol-fixture"))]
    {
        let _ = (
            ui,
            destination,
            daemon_config,
            store,
            workspace_id,
            source_workspace,
            generation,
            manifest,
            manifest_blake3,
            manifest_bytes,
        );
        return Err(failure(
            "startup-gated native restore requires the production Linux daemon; no checkout was created",
        ));
    }
    #[cfg(all(target_os = "linux", not(feature = "protocol-fixture")))]
    {
        use std::io::Write as _;
        let id = restore_identity(&workspace_id)?;
        let source = workspace_daemon::control::NativeRestoreSelection {
            workspace_id: restore_identity(&source_workspace)?,
            generation,
            manifest_id: restore_identity(&manifest)?,
            manifest_blake3: restore_identity(&manifest_blake3)?,
            manifest_size_bytes: manifest_bytes,
        };
        let client = restore_control(&command.cwd().join(daemon_config))?;
        let value = client
            .call(workspace_daemon::control::Request::NativeRestore {
                root: destination.to_owned(),
                store: store.clone(),
                workspace_id,
                source,
            })
            .map_err(failure)?;
        let binding = restored_response(value, destination, Some(&store), Some(id))?;
        writeln!(
            ui.stdout(),
            "Restored retained native workspace into {}",
            binding.root.display()
        )?;
        Ok(())
    }
}
fn resume_scoped(
    command: &CommandHelper,
    destination: &Path,
    config: &Path,
) -> Result<(), CommandError> {
    #[cfg(any(not(target_os = "linux"), feature = "protocol-fixture"))]
    {
        let _ = (command, destination, config);
        return Err(failure(
            "startup-gated restored resume requires the production Linux daemon",
        ));
    }
    #[cfg(all(target_os = "linux", not(feature = "protocol-fixture")))]
    {
        let client = restore_control(config)?;
        let value = client
            .call(workspace_daemon::control::Request::NativeResume {
                root: destination.to_owned(),
            })
            .map_err(failure)?;
        restored_response(value, destination, None, None)?;
        // Do not load_at_head or merge concurrent operation heads during resume.
        Ok(())
    }
}

pub(crate) async fn resume(
    ui: &Ui,
    command: &CommandHelper,
    destination: &Path,
    daemon_config: Option<PathBuf>,
) -> Result<(), CommandError> {
    reject_recovery_overrides(command)?;
    if let Some(config) = daemon_config {
        return resume_scoped(command, destination, &command.cwd().join(config));
    }
    let destination = destination.canonicalize().map_err(failure)?;
    let destination = destination.as_path();
    #[cfg(any(not(unix), feature = "protocol-fixture"))]
    {
        let _ = (ui, destination);
        return Err(failure(
            "lazy workspace recovery is unsupported by this binary/platform; no checkout was created",
        ));
    }
    #[cfg(all(unix, not(feature = "protocol-fixture")))]
    {
        let registry = native_workspace_admin::Registry::open_existing(
            native_workspace_admin::Registry::default_path().map_err(failure)?,
        )
        .map_err(failure)?;
        if let Some(mut intent) = registry
            .pending_provisioning(destination)
            .map_err(failure)?
        {
            let saved_plan = intent.creation_plan().map_err(failure)?;
            let ready_record = registry.path().join("records").join(format!(
                "{}.json",
                intent
                    .workspace_id()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            ));
            if saved_plan.is_none() && !ready_record.try_exists().map_err(failure)? {
                return Err(failure(
                    "pending workspace predates durable creation policy; preserve and reconcile its original index mode explicitly",
                ));
            }
            let binding = mount(
                &registry,
                intent.root(),
                intent.repository(),
                intent.endpoint(),
                &intent.initial_commit(),
                intent.workspace_id(),
                saved_plan,
            )?;
            crate::commands::ensure_binding(&binding).map_err(failure)?;
            let ready_record = registry
                .path()
                .join("records")
                .join(format!("{}.json", binding.id()));
            if ready_record.try_exists().map_err(failure)? {
                let ready = registry.resume_info(&binding.root).map_err(failure)?
                .ok_or_else(|| failure("no Ready native workspace registration; preserve pending initialization and reconcile explicitly"))?;
                if ready.binding != binding || ready.initial_commit != intent.initial_commit() {
                    return Err(failure(
                        "Ready registration differs from the original provisioning intent",
                    ));
                }
                load_ready(command, &registry, &binding).await?;
                intent.complete(&binding).map_err(failure)?;
                return Ok(());
            }
            // The daemon is mounted with the original ID. Initialization itself
            // still rejects any unknown or partial admin, never resets it.
            let locator = match intent.endpoint() {
                native_broker::LocalEndpoint::UnixSocket(socket) => {
                    Locator::new(socket, intent.repository().into()).map_err(failure)?
                }
                _ => return Err(failure("unsupported native broker transport")),
            };
            let (settings, _) = command.settings_for_new_workspace(ui, &binding.root)?;
            let initializer =
                |_: &UserSettings, path: &Path| -> Result<Box<dyn Backend>, BackendInitError> {
                    broker::init(path, locator.clone())
                };
            use jj_lib::ref_name::WorkspaceName;
            let mode = saved_plan
                .ok_or_else(|| failure("pending workspace has no saved creation policy"))?
                .index_mode;
            let initial = intent.initial_commit();
            let index_initializer =
                |settings: &UserSettings,
                 path: &Path,
                 operation: &jj_lib::op_store::OperationId| {
                    broker::init_index(path, mode, settings, &initial, operation)
                };
            native_workspace_admin::initialize_with_index(
                &registry,
                &binding,
                &settings,
                &initializer,
                signer_from_settings(&settings).map_err(WorkspaceInitError::SignInit)?,
                connector(&binding).map_err(failure)?,
                &intent.initial_commit(),
                WorkspaceName::DEFAULT.to_owned(),
                &index_initializer,
            )
            .await
            .map_err(failure)?;
            intent.complete(&binding).map_err(failure)?;
        } else {
            let ready = registry.resume_info(destination).map_err(|error| {
                failure(format!(
                    "{error}; restored origins require tinytree resume --daemon-config PATH"
                ))
            })?.ok_or_else(|| failure("no Ready native workspace registration; preserve pending initialization and reconcile explicitly"))?;
            let binding = mount(
                &registry,
                &ready.binding.root,
                &ready.binding.repository,
                ready.binding.endpoint.local(),
                &ready.initial_commit,
                ready.binding.workspace_id,
                None,
            )?;
            if binding != ready.binding {
                return Err(failure("resumed mount differs from the Ready binding"));
            }
            load_ready(command, &registry, &binding).await?;
        }
        Ok(())
    }
}

pub(crate) fn unmount(command: &CommandHelper, destination: &Path) -> Result<(), CommandError> {
    reject_recovery_overrides(command)?;
    let destination = destination.canonicalize().map_err(failure)?;
    let destination = destination.as_path();
    #[cfg(any(not(unix), feature = "protocol-fixture"))]
    {
        let _ = destination;
        Err(failure(
            "lazy workspace unmount is unsupported by this binary/platform",
        ))
    }
    #[cfg(all(unix, not(feature = "protocol-fixture")))]
    {
        let root = destination.canonicalize().map_err(failure)?;
        if command
            .cwd()
            .canonicalize()
            .map_err(failure)?
            .starts_with(&root)
        {
            return Err(failure(
                "current directory is inside the mount; cd outside it, then run tinytree unmount with its explicit destination",
            ));
        }
        let registry = native_workspace_admin::Registry::open_existing(
            native_workspace_admin::Registry::default_path().map_err(failure)?,
        )
        .map_err(failure)?;
        let pending = registry.pending_provisioning(&root).map_err(failure)?;
        let (endpoint, id) = match pending.as_ref() {
            Some(intent) => (
                intent.endpoint(),
                intent
                    .workspace_id()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect(),
            ),
            None => {
                // Stopping needs the validated mounted binding, not clone-only
                // resume admission or any replay of a restored source.
                let binding = registry.lookup(&root).map_err(failure)?;
                (binding.endpoint.local(), binding.id())
            }
        };
        control_call(
            endpoint,
            workspace_daemon::control::Request::NativeUnmount { workspace_id: id },
        )?;
        Ok(())
    }
}

pub(crate) fn reject_recovery_overrides(command: &CommandHelper) -> Result<(), CommandError> {
    let args = command.global_args();
    if args.at_operation.is_some() || args.ignore_working_copy || args.no_integrate_operation {
        return Err(failure(
            "workspace lifecycle commands do not support operation or working-copy overrides",
        ));
    }
    Ok(())
}

pub(crate) async fn recover(
    command: &CommandHelper,
    destination: &Path,
    operation: Option<String>,
) -> Result<(), CommandError> {
    reject_recovery_overrides(command)?;
    let destination = destination.canonicalize().map_err(failure)?;
    let destination = destination.as_path();
    #[cfg(any(not(unix), feature = "protocol-fixture"))]
    {
        let _ = (destination, operation);
        Err(failure(
            "lazy workspace recovery is unsupported by this binary/platform",
        ))
    }
    #[cfg(all(unix, not(feature = "protocol-fixture")))]
    {
        use jj_lib::{op_store::OperationId, repo::RepoLoader};
        let registry = native_workspace_admin::Registry::open_existing(
            native_workspace_admin::Registry::default_path().map_err(failure)?,
        )
        .map_err(failure)?;
        // Recovery requires an actually mounted, marker-verified registration.
        let binding = registry.lookup(destination).map_err(failure)?;
        crate::commands::ensure_binding(&binding).map_err(failure)?;
        let mut stores = jj_lib::default_backend_factories::default_backend_factories();
        broker::register(&mut stores);
        let loader = RepoLoader::init_from_file_system(command.settings(), &binding.repo, &stores)
            .map_err(failure)?;
        let operation = if let Some(value) = operation {
            if value.len() != 128
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(failure(
                    "recovery requires a full lowercase 128-hex jj operation ID",
                ));
            }
            let id = OperationId::try_from_hex(&value)
                .ok_or_else(|| failure("invalid retained jj operation identity"))?;
            Some(
                load_recovery_operation(
                    loader.op_store().clone(),
                    &id,
                    8 * 1024 * 1024,
                    64 * 1024 * 1024,
                )
                .map_err(failure)?,
            )
        } else {
            None
        };
        let factory = jj_vfs::native::NativeVfsWorkingCopyFactory::new(
            binding.repository.clone(),
            connector(&binding).map_err(failure)?,
        );
        match factory
            .recover_pending(
                loader.store().clone(),
                &binding.root,
                &binding.working_copy,
                operation.as_ref(),
                None,
            )
            .await
            .map_err(failure)?
        {
            jj_vfs::native::RecoveryOutcome::OperationRequired(_) => Err(failure(
                "controller confirms an applied mutation; supply --operation with the full ID of an actual retained jj operation describing that exact tree; no state was reset",
            )),
            _ => Ok(()),
        }
    }
}

/// Prepare a lazy projection root without changing permissions or adopting an
/// existing nonprivate directory. Each missing component is created privately.
pub(crate) fn prepare_destination(destination: &Path) -> anyhow::Result<()> {
    #[cfg(feature = "protocol-fixture")]
    {
        std::fs::create_dir_all(destination)?;
        Ok(())
    }
    #[cfg(not(feature = "protocol-fixture"))]
    {
        let mut missing = vec![];
        let mut ancestor = destination;
        while !ancestor.try_exists()? {
            missing.push(ancestor.to_path_buf());
            ancestor = ancestor
                .parent()
                .ok_or_else(|| anyhow::anyhow!("workspace has no existing ancestor"))?;
        }
        let names = missing
            .iter()
            .rev()
            .map(|path| {
                path.file_name()
                    .map(|name| name.to_os_string())
                    .ok_or_else(|| anyhow::anyhow!("unsupported workspace path component"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut resolved = ancestor.canonicalize()?;
        for name in names {
            resolved.push(name);
            private_fs::PrivateDir::create(&resolved)?;
        }
        private_fs::PrivateDir::open_existing(destination.canonicalize()?)?;
        Ok(())
    }
}

#[cfg(all(unix, not(feature = "protocol-fixture")))]
pub(crate) fn load_recovery_operation(
    store: Arc<dyn jj_lib::op_store::OpStore>,
    id: &jj_lib::op_store::OperationId,
    max_file_bytes: usize,
    max_decode_bytes: usize,
) -> anyhow::Result<jj_lib::operation::Operation> {
    use jj_lib::{object_id::ObjectId, simple_op_store::SimpleOpStore};
    let simple = store
        .downcast_ref::<SimpleOpStore>()
        .ok_or_else(|| anyhow::anyhow!("unsupported recovery operation store"))?;
    let operation = simple.read_operation_bounded(id, max_file_bytes, max_decode_bytes)?;
    anyhow::ensure!(
        id == store.root_operation_id()
            || jj_lib::content_hash::blake2b_hash(&operation).as_slice() == id.as_bytes(),
        "retained operation content does not match its ID"
    );
    let view = simple.read_view_bounded(&operation.view_id, max_file_bytes, max_decode_bytes)?;
    // Only the actual SimpleOpStore intrinsic zero view is exempt: its bounded
    // reader synthesizes it from the repository's intrinsic root commit.
    anyhow::ensure!(
        operation.view_id.as_bytes().iter().all(|b| *b == 0)
            || jj_lib::content_hash::blake2b_hash(&view).as_slice() == operation.view_id.as_bytes(),
        "retained view content does not match its ID"
    );
    let proof = PinnedRecoveryStore {
        root_operation: store.root_operation_id().clone(),
        operation_id: id.clone(),
        operation: operation.clone(),
        view,
    };
    Ok(jj_lib::operation::Operation::new(
        Arc::new(proof),
        id.clone(),
        operation,
    ))
}

/// One verified immutable proof, not a transparent wrapper over mutable disk.
/// Recovery's Operation::view() can only return this exact bounded decoded view.
#[cfg(all(unix, not(feature = "protocol-fixture")))]
#[derive(Debug)]
struct PinnedRecoveryStore {
    root_operation: jj_lib::op_store::OperationId,
    operation_id: jj_lib::op_store::OperationId,
    operation: jj_lib::op_store::Operation,
    view: jj_lib::op_store::View,
}
#[cfg(all(unix, not(feature = "protocol-fixture")))]
fn proof_store_error() -> jj_lib::op_store::OpStoreError {
    jj_lib::op_store::OpStoreError::Other(
        std::io::Error::other("recovery proof permits only its pinned operation and view reads")
            .into(),
    )
}
#[cfg(all(unix, not(feature = "protocol-fixture")))]
#[async_trait::async_trait]
impl jj_lib::op_store::OpStore for PinnedRecoveryStore {
    fn name(&self) -> &str {
        "verified_native_recovery_proof"
    }
    fn root_operation_id(&self) -> &jj_lib::op_store::OperationId {
        &self.root_operation
    }
    async fn read_view(
        &self,
        id: &jj_lib::op_store::ViewId,
    ) -> jj_lib::op_store::OpStoreResult<jj_lib::op_store::View> {
        if *id != self.operation.view_id {
            return Err(proof_store_error());
        }
        Ok(self.view.clone())
    }
    async fn read_operation(
        &self,
        id: &jj_lib::op_store::OperationId,
    ) -> jj_lib::op_store::OpStoreResult<jj_lib::op_store::Operation> {
        if *id != self.operation_id {
            return Err(proof_store_error());
        }
        Ok(self.operation.clone())
    }
    async fn write_view(
        &self,
        _: &jj_lib::op_store::View,
    ) -> jj_lib::op_store::OpStoreResult<jj_lib::op_store::ViewId> {
        Err(proof_store_error())
    }
    async fn write_operation(
        &self,
        _: &jj_lib::op_store::Operation,
    ) -> jj_lib::op_store::OpStoreResult<jj_lib::op_store::OperationId> {
        Err(proof_store_error())
    }
    async fn resolve_operation_id_prefix(
        &self,
        _: &jj_lib::object_id::HexPrefix,
    ) -> jj_lib::op_store::OpStoreResult<
        jj_lib::object_id::PrefixResolution<jj_lib::op_store::OperationId>,
    > {
        Err(proof_store_error())
    }
    async fn gc(
        &self,
        _: &[jj_lib::op_store::OperationId],
        _: std::time::SystemTime,
    ) -> jj_lib::op_store::OpStoreResult<()> {
        Err(proof_store_error())
    }
}

#[cfg(all(test, unix, not(feature = "protocol-fixture")))]
mod recovery_tests {
    use super::*;
    use jj_lib::{
        object_id::ObjectId,
        op_store::{OpStore, RootOperationData},
        simple_op_store::SimpleOpStore,
    };

    #[test]
    fn destination_creation_is_private_and_existing_public_directory_is_not_repaired() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let root = directory
            .path()
            .canonicalize()
            .unwrap()
            .join("new-parent/workspace");
        prepare_destination(&root).unwrap();
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(root.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        let public = directory.path().join("existing-public");
        std::fs::create_dir(&public).unwrap();
        std::fs::set_permissions(&public, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(prepare_destination(&public).is_err());
        assert_eq!(
            std::fs::metadata(public).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }

    #[test]
    fn corrupt_or_oversized_view_cannot_supply_recovery_proof_and_verified_view_is_pinned() {
        futures::executor::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let store = Arc::new(
                SimpleOpStore::init(
                    directory.path(),
                    RootOperationData {
                        root_commit_id: CommitId::new(vec![0; 32]),
                    },
                )
                .unwrap(),
            );
            let mut view = jj_lib::op_store::View::make_root(CommitId::new(vec![0; 32]));
            view.wc_commit_ids.insert(
                jj_lib::ref_name::WorkspaceNameBuf::from("default"),
                CommitId::new(vec![1; 32]),
            );
            let view_id = store.write_view(&view).await.unwrap();
            let mut operation = jj_lib::op_store::Operation::make_root(view_id.clone());
            operation.parents = vec![store.root_operation_id().clone()];
            let operation_id = store.write_operation(&operation).await.unwrap();
            let pinned =
                load_recovery_operation(store.clone(), &operation_id, 4096, 1024 * 1024).unwrap();
            let view_path = directory.path().join("views").join(view_id.hex());
            let original = std::fs::read(&view_path).unwrap();
            let mut substituted = view.clone();
            substituted.wc_commit_ids.insert(
                jj_lib::ref_name::WorkspaceNameBuf::from("default"),
                CommitId::new(vec![2; 32]),
            );
            let other_id = store.write_view(&substituted).await.unwrap();
            std::fs::copy(
                directory.path().join("views").join(other_id.hex()),
                &view_path,
            )
            .unwrap();
            assert!(
                load_recovery_operation(store.clone(), &operation_id, 4096, 1024 * 1024).is_err()
            );
            assert_eq!(pinned.view().await.unwrap().store_view(), &view);
            std::fs::write(&view_path, vec![0; 4097]).unwrap();
            assert!(
                load_recovery_operation(store.clone(), &operation_id, 4096, 1024 * 1024).is_err()
            );
            assert_eq!(pinned.view().await.unwrap().store_view(), &view);
            assert!(pinned.op_store().write_view(&view).await.is_err());
            assert!(pinned.op_store().read_view(&other_id).await.is_err());
            std::fs::write(&view_path, original).unwrap();
            assert!(load_recovery_operation(store, &operation_id, 4096, 1024 * 1024).is_ok());
        });
    }
}
