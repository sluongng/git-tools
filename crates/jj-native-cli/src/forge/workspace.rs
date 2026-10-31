//! Product workspaces bind a local native mount to one authorized projection.
use super::{context::Context, output, rpc, service};
use jj_cli::{
    cli_util::CommandHelper,
    command_error::{CommandError, user_error},
    ui::Ui,
};
use jj_lib::{backend::CommitId, object_id::ObjectId as _};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use tinytree_client::forge_protocol::ForgeOperation as Op;
const FILE: &str = "forge-workspace.json";
#[derive(clap::Args, Clone, Debug)]
pub(super) struct WorkArgs {
    pub destination: PathBuf,
    /// Trusted local native broker configured for this account/view projection.
    #[arg(long, requires = "store")]
    pub broker: Option<PathBuf>,
    #[arg(long, requires = "broker")]
    pub store: Option<String>,
    #[arg(long)]
    pub version: Option<String>,
}
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct Scope {
    pub schema_version: u32,
    pub context: Context,
    pub view_version: [u8; 32],
    pub base: [u8; 32],
    pub tree: [u8; 32],
    pub policy_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_descriptor: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_provider_identity: Option<[u8; 32]>,
}
impl Scope {
    pub fn load(root: &Path) -> Result<Self, CommandError> {
        let path = crate::vfs::working_copy_path(root).map_err(user_error)?;
        Self::load_directory(&path)
    }
    pub(super) fn load_directory(path: &Path) -> Result<Self, CommandError> {
        let directory = private_fs::PrivateDir::open_existing(path).map_err(user_error)?;
        let value: Self = serde_json::from_slice(&directory.read(FILE, 32768).map_err(user_error)?)
            .map_err(|_| user_error("invalid forge workspace scope"))?;
        value.context.validate().map_err(user_error)?;
        if value.schema_version != 1 || value.base == [0; 32] {
            return Err(user_error("invalid forge workspace projection"));
        }
        Ok(value)
    }
    pub fn hash(&self) -> Result<[u8; 32], CommandError> {
        let bytes = serde_json::to_vec(self).map_err(user_error)?;
        Ok(*blake3::hash(&bytes).as_bytes())
    }
}
pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub(super) async fn work(
    ui: &mut Ui,
    command: &CommandHelper,
    machine: bool,
    args: WorkArgs,
) -> Result<(), CommandError> {
    let target = command.cwd().join(&args.destination);
    let version = args.version.as_deref().map(service::id::<32>).transpose()?;
    let existing_target = if args.broker.is_none() && args.store.is_none() {
        match std::fs::symlink_metadata(&target) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() { return Err(user_error("Forge workspace destination must be a nonsymlink directory")); }
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(user_error(error)),
        }
    } else { false };
    if existing_target {
        // Original workspace identity takes precedence over any current context.
        if super::client::resume(&target, version).await? {
            crate::vfs::resume(ui, command, &target, None).await?;
            super::client::finish_resume(&target)?;
            let scope = Scope::load(&target)?;
            if machine { output::write(ui, "work", "ready", json!({"account_id":scope.context.account_id,"view":scope.context.view,
                "view_version":hex(&scope.view_version),"base":hex(&scope.base),"destination":target.canonicalize().map_err(user_error)?,
                "client_descriptor":scope.client_descriptor,"provider_identity":scope.client_provider_identity.map(|id|hex(&id))}))?; }
            return Ok(());
        }
        let mut entries = std::fs::read_dir(&target).map_err(user_error)?;
        if entries.next().transpose().map_err(user_error)?.is_some() {
            return Err(user_error("nonempty destination has no matching Forge client descriptor; existing work retained"));
        }
    }
    let context = service::context()?;
    let enrollment = if args.broker.is_none() && args.store.is_none() {
        Some(super::client::enroll(&context, &target, version).await?)
    } else { None };
    let (projection, broker, store) = if let Some(enrollment) = &enrollment {
        (enrollment.projection.clone(), enrollment.broker.clone(), enrollment.store.clone())
    } else {
        let (Some(broker), Some(store)) = (&args.broker, &args.store) else { return Err(user_error("--broker and --store must be supplied together")); };
        let reply = rpc::call(&context, Op::ResolveView, json!({"version":version}), None, false, None).await?;
        let projection: native_broker::ForgeProjection = serde_json::from_slice(&reply.result_json)
            .map_err(|_| user_error("invalid forge projection reply"))?;
        (projection, broker.clone(), store.clone())
    };
    if projection.account_id != context.account_id || projection.view != context.view || !projection.complete
        || version.is_some_and(|version| version != projection.view_version) {
        return Err(user_error("forge workspace projection scope is incomplete or mismatched"));
    }
    let socket = command.cwd().join(&broker);
    let client = native_broker::BrokerControlClient::connect(
        &socket,
        store.clone(),
        Default::default(),
    )
    .await
    .map_err(user_error)?;
    let observed = client.observe_reference("main").await.map_err(user_error)?;
    if observed.generation != projection.policy_epoch
        || observed.head.as_ref().map(|h| h.head.0) != Some(projection.projected_commit)
    {
        return Err(user_error(
            "local broker snapshot differs from the selected view; use --version for its pinned version or configure a new pinned forge alias for the current view",
        ));
    }
    let locator = crate::broker::Locator::new(socket, store.clone()).map_err(user_error)?;
    let initial = CommitId::from_bytes(&projection.projected_commit);
    let plan = crate::broker::creation_plan(&locator, &initial).map_err(user_error)?;
    if plan.index_mode != native_broker::ProviderIndexMode::Legacy || plan.policy.upload_enabled
        || plan.policy.capture_enabled || plan.policy.auto_capture || plan.policy.auto_upload {
        return Err(user_error(
            "forge workspaces require the projected legacy index with raw remote upload disabled",
        ));
    }
    crate::remote::clone(
        ui,
        command,
        args.destination.clone(),
        broker,
        store,
        "main".into(),
        "main".into(),
    )
    .await?;
    let destination = command
        .cwd()
        .join(args.destination)
        .canonicalize()
        .map_err(user_error)?;
    let root = crate::vfs::working_copy_path(&destination).map_err(user_error)?;
    let directory = private_fs::PrivateDir::open_existing(root).map_err(user_error)?;
    let scope = Scope {
        schema_version: 1,
        context,
        view_version: projection.view_version,
        base: projection.projected_commit,
        tree: projection.projected_tree,
        policy_epoch: projection.policy_epoch,
        client_descriptor: enrollment.as_ref().map(|client| client.descriptor.clone()),
        client_provider_identity: enrollment.as_ref().map(|client| client.provider_identity),
    };
    let bytes = serde_json::to_vec(&scope).map_err(user_error)?;
    directory.replace(FILE, &bytes).map_err(user_error)?;
    if machine {
        output::write(
            ui,
            "work",
            "ready",
            json!({"account_id":scope.context.account_id,"view":scope.context.view,"view_version":hex(&scope.view_version),"base":initial.hex(),"destination":destination,
                "client_descriptor":scope.client_descriptor,"provider_identity":enrollment.as_ref().map(|client|hex(&client.provider_identity))}),
        )
    } else {
        Ok(())
    }
}
