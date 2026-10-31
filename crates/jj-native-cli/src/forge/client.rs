//! Owner-private destination intent and launch of the snapshot-only client.
//! Credentials are inherited only by the owned child, never written to state.
use super::{context::Context, workspace};
use fs2::FileExt as _;
use jj_cli::command_error::{CommandError, user_error};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Read as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};
use tinytree_client::{ForgeViewSelection, NativeClient, forge_protocol as f};
use tinytree_forge_core::session::ForgeSession;
use workspace_daemon::forge_client::{self, ClientDescriptor, ProjectionPin};
const MAX_INTENT: usize = 64 * 1024;
const MAX_RECORDS: usize = 1024;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ProjectedRoots {
    commit: [u8; 32],
    tree: [u8; 32],
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    schema_version: u32,
    destination: PathBuf,
    context: Context,
    trust_digest: Option<[u8; 32]>,
    session: ForgeSession,
    requested_version: Option<[u8; 32]>,
    selection: Option<ForgeViewSelection>,
    projection: Option<ProjectedRoots>,
    descriptor: Option<PathBuf>,
    provider_identity: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    private_restore: Option<tinytree_client::private_capsule::CapsuleBinding>,
}
pub(super) struct Enrollment {
    pub descriptor: PathBuf,
    pub broker: PathBuf,
    pub store: String,
    pub provider_identity: [u8; 32],
    pub projection: native_broker::ForgeProjection,
    _directory: PrivateDir,
    _lock: File,
}
fn fail(error: impl std::fmt::Display) -> CommandError {
    user_error(error.to_string())
}
fn mismatch() -> CommandError {
    user_error(
        "saved Forge work target is bound to different context, session, trust or immutable selection; original state retained",
    )
}
pub(super) fn trust(context: &Context) -> Result<Option<[u8; 32]>, CommandError> {
    let Some(path) = &context.ca_certificate else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| user_error("cannot read selected Forge trust certificate"))?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| user_error("cannot read selected Forge trust certificate"))?;
    if bytes.is_empty() || bytes.len() > 1024 * 1024 {
        return Err(user_error(
            "selected Forge trust certificate exceeds bounds",
        ));
    }
    Ok(Some(*blake3::hash(&bytes).as_bytes()))
}
fn destination(path: &Path) -> Result<PathBuf, CommandError> {
    if path.exists() {
        return path.canonicalize().map_err(fail);
    }
    let name = path
        .file_name()
        .ok_or_else(|| user_error("Forge work destination requires a final directory name"))?;
    Ok(path
        .parent()
        .ok_or_else(|| user_error("Forge work destination requires a parent"))?
        .canonicalize()
        .map_err(fail)?
        .join(name))
}
fn save(directory: &PrivateDir, intent: &Intent) -> Result<(), CommandError> {
    let bytes = serde_json::to_vec(intent).map_err(fail)?;
    if bytes.len() > MAX_INTENT {
        return Err(user_error("Forge work intent exceeds bounds"));
    }
    directory.replace("intent.json", &bytes).map_err(fail)
}
fn child_command(descriptor: &ClientDescriptor) -> Result<Command, CommandError> {
    let mut command = Command::new(std::env::current_exe().map_err(fail)?);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
    }
    command
        .arg("__forge_client_serve")
        .arg(descriptor.descriptor_path())
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let home = std::env::var_os("HOME").ok_or_else(|| user_error("Forge client requires HOME"))?;
    command.env("HOME", home);
    // Only owner-private runtime directories are inherited, never shell startup
    // files, loader variables, full PATH, operator config or arbitrary env.
    for name in ["XDG_RUNTIME_DIR", "TMPDIR"] {
        if let Some(path) = std::env::var_os(name) {
            let path = PathBuf::from(path);
            if !path.is_absolute() || PrivateDir::open_existing(&path).is_err() {
                return Err(user_error(
                    "Forge client runtime directory is not owner-private",
                ));
            }
            command.env(name, path);
        }
    }
    // The shared supported user registry identity is explicit, not inferred from
    // another operator's config. Preserve its selected reference only if needed.
    if let Some(name) = &descriptor.source().client.token_env {
        forge_client::validate_token_reference(name).map_err(fail)?;
        let value = std::env::var_os(name)
            .ok_or_else(|| user_error("cannot resolve selected Forge credential"))?;
        command.env(name, value);
    }
    Ok(command)
}
async fn ensure_running(
    descriptor: &ClientDescriptor,
) -> Result<forge_client::Ready, CommandError> {
    descriptor.validate().map_err(fail)?;
    if descriptor.registry_path()
        != native_workspace_admin::Registry::default_path().map_err(fail)?
    {
        return Err(mismatch());
    }
    let session = NativeClient::connect(descriptor.source().client.clone())
        .await
        .map_err(fail)?
        .forge_get_session(&descriptor.source().account_id, &descriptor.source().view)
        .await
        .map_err(fail)?;
    if &session != descriptor.session() {
        return Err(mismatch());
    }
    let directory = PrivateDir::open_existing(descriptor.state_dir()).map_err(fail)?;
    let lock = directory.open_lock("launch.lock").map_err(fail)?;
    lock.try_lock_exclusive().map_err(|_| {
        user_error("another process is starting this exact Forge client; retry original target")
    })?;
    let digest = descriptor.digest().map_err(fail)?;
    let validate = |ready: forge_client::Ready| -> Result<forge_client::Ready, CommandError> {
        if ready.schema_version != 1
            || ready.descriptor_digest != digest
            || ready.broker_socket != descriptor.broker_socket()
            || ready.store != descriptor.store_alias()
            || &ready.pin != descriptor.pin()
            || ready.provider_identity == [0; 32]
        {
            return Err(mismatch());
        }
        Ok(ready)
    };
    if let Ok(ready) = forge_client::ready(descriptor) {
        return validate(ready);
    }
    let mut child = child_command(descriptor)?
        .spawn()
        .map_err(|_| user_error("cannot start owned Forge client; saved target retained"))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(ready) = forge_client::ready(descriptor) {
            // Reap the owned child after its eventual normal shutdown without
            // coupling its lifetime to this CLI invocation.
            std::thread::spawn(move || {
                let _ = child.wait();
            });
            return validate(ready);
        }
        if child.try_wait().map_err(fail)?.is_some() {
            return Err(user_error(
                "owned Forge client refused readiness; original descriptor and work retained",
            ));
        }
        if tokio::time::Instant::now() >= deadline {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
            return Err(user_error(
                "Forge client readiness timed out; retry the saved original target",
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
// jj dispatch may poll futures outside Tokio. Own one reactor for this whole
// workflow, including SDK cleanup, first-ACK checkpoint and readiness pacing.
pub(super) async fn with_runtime<T>(
    work: impl std::future::Future<Output = Result<T, CommandError>>,
) -> Result<T, CommandError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return work.await;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(fail)?;
    runtime.block_on(work)
}
pub(super) async fn enroll(
    context: &Context,
    target: &Path,
    version: Option<[u8; 32]>,
) -> Result<Enrollment, CommandError> {
    with_runtime(enroll_inner(context, target, version)).await
}
async fn enroll_inner(
    context: &Context,
    target: &Path,
    version: Option<[u8; 32]>,
) -> Result<Enrollment, CommandError> {
    context.validate().map_err(fail)?;
    if let Some(name) = &context.token_env {
        forge_client::validate_token_reference(name).map_err(fail)?;
    }
    let target = destination(target)?;
    let root = super::context::create().map_err(fail)?;
    let works = PrivateDir::create(root.path().join("work-targets")).map_err(fail)?;
    if std::fs::read_dir(works.path())
        .map_err(fail)?
        .take(MAX_RECORDS + 1)
        .count()
        > MAX_RECORDS
    {
        return Err(user_error("Forge work target limit exceeded"));
    }
    let key = workspace::hex(blake3::hash(target.as_os_str().as_encoded_bytes()).as_bytes());
    let directory = PrivateDir::create(works.path().join(key)).map_err(fail)?;
    let lock = directory.open_lock("owner.lock").map_err(fail)?;
    lock.try_lock_exclusive().map_err(|_| {
        user_error("another process owns this Forge work target; retry original intent")
    })?;
    let trust_digest = trust(context)?;
    let client = NativeClient::connect(context.client_config().map_err(fail)?)
        .await
        .map_err(fail)?;
    let session = client
        .forge_get_session(&context.account_id, &context.view)
        .await
        .map_err(fail)?;
    let mut intent = match directory.read("intent.json", MAX_INTENT) {
        Ok(bytes) => {
            let saved: Intent = serde_json::from_slice(&bytes).map_err(|_| mismatch())?;
            if saved.schema_version != 1
                || saved.destination != target
                || &saved.context != context
                || saved.session != session
                || saved.trust_digest != trust_digest
                || saved.requested_version != version
            {
                return Err(mismatch());
            }
            saved
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Intent {
            schema_version: 1,
            destination: target.clone(),
            context: context.clone(),
            trust_digest,
            session,
            requested_version: version,
            selection: None,
            projection: None,
            descriptor: None,
            provider_identity: None,
            private_restore: None,
        },
        Err(error) => return Err(fail(error)),
    };
    save(&directory, &intent)?;
    let selected_version = intent
        .selection
        .as_ref()
        .map(|s| s.view_version)
        .or(version);
    let reply = client
        .forge_resolve_view_with_selection(
            f::ForgeCallRequest {
                operation: f::ForgeOperation::ResolveView as i32,
                account_id: context.account_id.clone(),
                view: context.view.clone(),
                request_id: String::new(),
                payload_json: serde_json::to_vec(&serde_json::json!({"version":selected_version}))
                    .map_err(fail)?,
            },
            |selection| {
                if intent
                    .selection
                    .as_ref()
                    .is_some_and(|saved| saved != selection)
                {
                    return Err(tonic::Status::aborted("saved Forge selection changed"));
                }
                intent.selection = Some(selection.clone());
                save(&directory, &intent).map_err(|_| {
                    tonic::Status::failed_precondition("cannot durably save first Forge selection")
                })
            },
        )
        .await
        .map_err(fail)?;
    let projection: native_broker::ForgeProjection =
        serde_json::from_slice(&reply.result_json).map_err(|_| mismatch())?;
    let roots = ProjectedRoots {
        commit: projection.projected_commit,
        tree: projection.projected_tree,
    };
    if intent
        .projection
        .as_ref()
        .is_some_and(|saved| saved != &roots)
    {
        return Err(mismatch());
    }
    intent.projection = Some(roots);
    save(&directory, &intent)?;
    if trust(context)? != intent.trust_digest {
        return Err(mismatch());
    }
    let descriptor = ClientDescriptor::new(
        &root.path().join("clients"),
        &target,
        native_broker::ForgeRemoteConfig {
            account_id: context.account_id.clone(),
            view: context.view.clone(),
            view_version: Some(projection.view_version),
            client: context.client_config().map_err(fail)?,
        },
        intent.session.clone(),
        ProjectionPin::from(&projection),
    )
    .map_err(fail)?;
    let path = descriptor.save().map_err(fail)?;
    if intent
        .descriptor
        .as_ref()
        .is_some_and(|saved| saved != &path)
    {
        return Err(mismatch());
    }
    intent.descriptor = Some(path.clone());
    save(&directory, &intent)?;
    let ready = ensure_running(&descriptor).await?;
    if intent
        .provider_identity
        .is_some_and(|id| id != ready.provider_identity)
    {
        return Err(mismatch());
    }
    intent.provider_identity = Some(ready.provider_identity);
    save(&directory, &intent)?;
    Ok(Enrollment {
        descriptor: path,
        broker: ready.broker_socket,
        store: ready.store,
        provider_identity: ready.provider_identity,
        projection,
        _directory: directory,
        _lock: lock,
    })
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientResume {
    NotClient,
    Clone,
    Private,
}
type PrivateResume = (
    tinytree_client::private_capsule::CapsuleBinding,
    native_workspace_admin::private_bundle::PrivateReviewBase,
);
pub(super) fn mark_private_restore(
    enrolled: &Enrollment,
    capsule: &tinytree_client::private_capsule::CapsuleBinding,
) -> Result<(), CommandError> {
    capsule.validate().map_err(fail)?;
    let mut intent: Intent = serde_json::from_slice(
        &enrolled
            ._directory
            .read("intent.json", MAX_INTENT)
            .map_err(fail)?,
    )
    .map_err(|_| mismatch())?;
    if intent.descriptor.as_ref() != Some(&enrolled.descriptor)
        || intent.provider_identity != Some(enrolled.provider_identity)
        || intent
            .private_restore
            .as_ref()
            .is_some_and(|saved| saved != capsule)
        || capsule.account_id != enrolled.projection.account_id
        || capsule.selection.view_version != workspace::hex(&enrolled.projection.view_version)
        || capsule.selection.policy_epoch != enrolled.projection.policy_epoch
        || capsule.selection.projected_commit
            != workspace::hex(&enrolled.projection.projected_commit)
        || capsule.selection.projected_tree != workspace::hex(&enrolled.projection.projected_tree)
    {
        return Err(mismatch());
    }
    intent.private_restore = Some(capsule.clone());
    save(&enrolled._directory, &intent)
}
fn service_id(value: &str) -> Result<[u8; 32], CommandError> {
    super::service::id::<32>(value)
}
fn saved_private_target(target: &Path) -> Result<Option<Intent>, CommandError> {
    let key = workspace::hex(blake3::hash(target.as_os_str().as_encoded_bytes()).as_bytes());
    let path = super::context::path()
        .map_err(fail)?
        .join("work-targets")
        .join(key);
    match std::fs::symlink_metadata(path.join("intent.json")) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(fail(error)),
        Ok(_) => {}
    }
    let directory = PrivateDir::open_existing(path).map_err(fail)?;
    let intent: Intent =
        serde_json::from_slice(&directory.read("intent.json", MAX_INTENT).map_err(fail)?)
            .map_err(|_| mismatch())?;
    if intent.schema_version != 1 || intent.destination != target {
        return Err(mismatch());
    }
    if intent.private_restore.is_some() {
        Ok(Some(intent))
    } else {
        Ok(None)
    }
}
/// This only permits replaying the same restore request to the trusted host.
/// It is not eligibility to adopt local data or publish Ready.
pub(super) fn private_retry(
    target: &Path,
    capsule_id: &str,
    context: &Context,
) -> Result<bool, CommandError> {
    let target = target.canonicalize().map_err(fail)?;
    let Some(intent) = saved_private_target(&target)? else {
        return Ok(false);
    };
    let capsule = intent.private_restore.as_ref().ok_or_else(mismatch)?;
    let mut selected_context = context.clone();
    if !capsule.selection.view.is_empty() {
        selected_context.view = capsule.selection.view.clone();
    }
    if capsule.capsule_id != capsule_id
        || intent.context != selected_context
        || intent.trust_digest != trust(context)?
    {
        return Err(mismatch());
    }
    Ok(true)
}
fn resume_descriptor(
    target: &Path,
) -> Result<
    Option<(
        ClientDescriptor,
        Option<workspace::Scope>,
        Option<native_broker::CreationPlan>,
        Option<PrivateResume>,
    )>,
    CommandError,
> {
    let canonical_target = target.canonicalize().map_err(fail)?;
    let target = canonical_target.as_path();
    let registry_path = native_workspace_admin::Registry::default_path().map_err(fail)?;
    match std::fs::symlink_metadata(&registry_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut entries = std::fs::read_dir(target).map_err(fail)?;
            if entries.next().transpose().map_err(fail)?.is_none() {
                return Ok(None);
            }
            return Err(user_error(
                "cannot recover nonempty Forge destination without its original native registry; existing work retained",
            ));
        }
        Err(error) => return Err(fail(error)),
        Ok(_) => {}
    }
    // Existing registry failures are never proof that this is a new target.
    let registry = native_workspace_admin::Registry::open_existing(&registry_path).map_err(fail)?;
    if let Some(intent) = saved_private_target(target)? {
        let descriptor = ClientDescriptor::load(intent.descriptor.as_deref().ok_or_else(mismatch)?)
            .map_err(fail)?;
        let provider = intent.provider_identity.ok_or_else(mismatch)?;
        let info = registry
            .restored_resume_info(target, provider)
            .map_err(fail)?;
        let capsule = info.source.private_capsule().ok_or_else(mismatch)?.clone();
        let review = info
            .source
            .private_review_base()
            .ok_or_else(mismatch)?
            .clone();
        if Some(&capsule) != intent.private_restore.as_ref()
            || descriptor.destination() != target
            || descriptor.broker_socket()
                != match info.binding.endpoint.local() {
                    native_broker::LocalEndpoint::UnixSocket(path) => path,
                    _ => return Err(mismatch()),
                }
            || descriptor.store_alias() != info.binding.repository
            || descriptor.pin().view_version != service_id(&capsule.selection.view_version)?
            || descriptor.pin().policy_epoch != capsule.selection.policy_epoch
            || descriptor.pin().projected_commit != service_id(&capsule.selection.projected_commit)?
            || descriptor.pin().projected_tree != service_id(&capsule.selection.projected_tree)?
            || descriptor.source().account_id != capsule.account_id
            || review.view_version != descriptor.pin().view_version
            || review.policy_epoch != descriptor.pin().policy_epoch
        {
            return Err(mismatch());
        }
        let directory = PrivateDir::open_existing(&info.binding.working_copy).map_err(fail)?;
        let scope = match directory.read("forge-workspace.json", 32768) {
            Ok(_) => Some(workspace::Scope::load_directory(
                &info.binding.working_copy,
            )?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(fail(error)),
        };
        if scope.as_ref().is_some_and(|scope| {
            scope.client_descriptor.as_ref() != Some(&descriptor.descriptor_path())
                || scope.client_provider_identity != Some(provider)
                || scope.view_version != descriptor.pin().view_version
                || scope.policy_epoch != descriptor.pin().policy_epoch
                || scope.context.account_id != descriptor.source().account_id
                || scope.context.view != descriptor.source().view
        }) {
            return Err(mismatch());
        }
        if let Some(scope) = &scope {
            if serde_json::to_vec(&scope.context.client_config().map_err(fail)?).map_err(fail)?
                != serde_json::to_vec(&descriptor.source().client).map_err(fail)?
            {
                return Err(mismatch());
            }
        }
        return Ok(Some((descriptor, scope, None, Some((capsule, review)))));
    }
    let (socket, store, working_copy, plan, initial) = if let Some(intent) =
        registry.pending_provisioning(target).map_err(fail)?
    {
        let native_broker::LocalEndpoint::UnixSocket(socket) = intent.endpoint() else {
            return Ok(None);
        };
        (
            socket,
            intent.repository().to_owned(),
            None,
            intent.creation_plan().map_err(fail)?,
            Some(intent.initial_commit()),
        )
    } else {
        let Some(info) = registry.resume_info(target).map_err(fail)? else {
            // None is a fully validated scan with no matching Ready record,
            // never a suppressed registry or recovery error.
            let mut entries = std::fs::read_dir(target).map_err(fail)?;
            if entries.next().transpose().map_err(fail)?.is_none() {
                return Ok(None);
            }
            return Err(user_error(
                "nonempty destination has no Ready native registration; existing work retained",
            ));
        };
        let native_broker::LocalEndpoint::UnixSocket(socket) = info.binding.endpoint.local() else {
            return Ok(None);
        };
        (
            socket,
            info.binding.repository,
            Some(info.binding.working_copy),
            None,
            Some(info.initial_commit),
        )
    };
    let path = socket
        .parent()
        .ok_or_else(mismatch)?
        .join("descriptor.json");
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(fail(error)),
        Ok(_) => {}
    }
    let descriptor = ClientDescriptor::load(&path).map_err(fail)?;
    if descriptor.destination() != target.canonicalize().map_err(fail)?
        || descriptor.broker_socket() != socket
        || descriptor.store_alias() != store
    {
        return Err(mismatch());
    }
    use jj_lib::object_id::ObjectId as _;
    if initial
        .as_ref()
        .is_none_or(|id| id.as_bytes() != descriptor.pin().projected_commit.as_slice())
    {
        return Err(mismatch());
    }
    let scope = match working_copy {
        Some(path) => {
            let directory = PrivateDir::open_existing(&path).map_err(fail)?;
            match directory.read("forge-workspace.json", 32768) {
                Ok(_) => Some(workspace::Scope::load_directory(&path)?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(fail(error)),
            }
        }
        None => None,
    };
    if let Some(scope) = &scope {
        if scope.client_descriptor.as_ref() != Some(&path)
            || scope.view_version != descriptor.pin().view_version
            || scope.policy_epoch != descriptor.pin().policy_epoch
            || scope.context.account_id != descriptor.source().account_id
            || scope.context.view != descriptor.source().view
            || serde_json::to_vec(&scope.context.client_config().map_err(fail)?).map_err(fail)?
                != serde_json::to_vec(&descriptor.source().client).map_err(fail)?
        {
            return Err(mismatch());
        }
    }
    Ok(Some((descriptor, scope, plan, None)))
}
pub(crate) async fn resume(
    target: &Path,
    expected_version: Option<[u8; 32]>,
) -> Result<ClientResume, CommandError> {
    with_runtime(resume_inner(target, expected_version)).await
}
async fn resume_inner(
    target: &Path,
    expected_version: Option<[u8; 32]>,
) -> Result<ClientResume, CommandError> {
    let Some((descriptor, scope, plan, private)) = resume_descriptor(target)? else {
        return Ok(ClientResume::NotClient);
    };
    if expected_version.is_some_and(|version| version != descriptor.pin().view_version) {
        return Err(mismatch());
    }
    let ready = ensure_running(&descriptor).await?;
    if scope
        .as_ref()
        .is_some_and(|scope| scope.client_provider_identity != Some(ready.provider_identity))
        || plan.as_ref().is_some_and(|plan| {
            plan.provider_identity != ready.provider_identity
                || plan.root != descriptor.pin().projected_commit
        })
    {
        return Err(mismatch());
    }
    if private.is_some() {
        let value = workspace_daemon::control::Client {
            socket: descriptor.state_dir().join("control.sock"),
        }
        .call(workspace_daemon::control::Request::NativeResume {
            root: target.canonicalize().map_err(fail)?,
        })
        .map_err(fail)?;
        let mounted: workspace_daemon::native_workspaces::Mounted =
            serde_json::from_value(value).map_err(|_| mismatch())?;
        if mounted.binding.root != target.canonicalize().map_err(fail)?
            || mounted.binding.repository != descriptor.store_alias()
            || mounted.binding.endpoint.local()
                != native_broker::LocalEndpoint::UnixSocket(descriptor.broker_socket())
        {
            return Err(mismatch());
        }
        return Ok(ClientResume::Private);
    }
    Ok(ClientResume::Clone)
}
pub(crate) fn finish_resume(target: &Path) -> Result<(), CommandError> {
    let Some((descriptor, scope, _, private)) = resume_descriptor(target)? else {
        return Err(mismatch());
    };
    if scope.is_some() {
        return Ok(());
    }
    let source = descriptor.source();
    let pin = descriptor.pin();
    let ready = forge_client::ready(&descriptor).map_err(fail)?;
    let context = Context {
        schema_version: 1,
        transport: source.client.transport,
        server: source.client.endpoint.clone(),
        account_id: source.account_id.clone(),
        view: source.view.clone(),
        token_env: source.client.token_env.clone(),
        ca_certificate: source
            .client
            .tls
            .as_ref()
            .map(|tls| tls.ca_certificate.clone()),
        tls_domain: source
            .client
            .tls
            .as_ref()
            .and_then(|tls| tls.domain_name.clone()),
    };
    let base = private
        .as_ref()
        .map_or(pin.projected_commit, |(_, review)| review.base);
    let tree = private
        .as_ref()
        .map_or(pin.projected_tree, |(_, review)| review.tree);
    let scope = workspace::Scope {
        schema_version: 1,
        context,
        view_version: pin.view_version,
        base,
        tree,
        policy_epoch: pin.policy_epoch,
        client_descriptor: Some(descriptor.descriptor_path()),
        client_provider_identity: Some(ready.provider_identity),
    };
    let directory = PrivateDir::open_existing(crate::vfs::working_copy_path(target).map_err(fail)?)
        .map_err(fail)?;
    directory
        .replace(
            "forge-workspace.json",
            &serde_json::to_vec(&scope).map_err(fail)?,
        )
        .map_err(fail)
}
/// Hidden process lifecycle; checked before jj parses ordinary user commands.
pub(crate) fn hidden() -> Option<std::process::ExitCode> {
    let mut args = std::env::args_os().skip(1);
    let mode = args.next()?;
    if mode != "__forge_client_serve" && mode != "__forge_client_stop" {
        return None;
    }
    let path = match (args.next(), args.next()) {
        (Some(path), None) => PathBuf::from(path),
        _ => return Some(std::process::ExitCode::from(2)),
    };
    let result = if mode == "__forge_client_serve" {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(anyhow::Error::from)
            .and_then(|runtime| runtime.block_on(forge_client::serve_forge_client(&path)))
    } else {
        ClientDescriptor::load(&path).and_then(|descriptor| forge_client::stop(&descriptor))
    };
    if result.is_err() {
        eprintln!("owned Forge client operation failed; private state retained");
    }
    Some(if result.is_ok() {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::FAILURE
    })
}
