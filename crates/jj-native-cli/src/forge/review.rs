//! Patch/diff review requests. None of these operations directly updates main.
use super::{rpc, service};
use jj_cli::cli_util::{CommandHelper, RevisionArg};
use jj_cli::{
    command_error::{CommandError, user_error},
    ui::Ui,
};
use jj_lib::object_id::ObjectId as _;
use serde_json::json;
use tinytree_client::forge_protocol::ForgeOperation as Op;

#[derive(clap::Args, Clone, Debug)]
pub(super) struct PushArgs {
    /// Exact immutable base already present in the account object store.
    #[arg(long)]
    source_base: Option<String>,
    /// Exact immutable head already present in the account object store.
    #[arg(long)]
    source_head: Option<String>,
    #[arg(long)]
    view_version: Option<String>,
    /// Local revision to upload; defaults to the snapshotted working-copy patch.
    #[arg(short='r',long,conflicts_with_all=["source_base","source_head","view_version"])]
    revision: Option<RevisionArg>,
    #[arg(long)]
    expected_patchset: Option<String>,
    #[arg(long, requires = "predecessor_patchset")]
    predecessor_change: Option<String>,
    #[arg(long, requires = "predecessor_change")]
    predecessor_patchset: Option<String>,
    #[arg(long)]
    provenance: Option<String>,
    #[arg(long)]
    request_id: Option<String>,
}

#[derive(clap::Subcommand, Clone, Debug)]
pub(super) enum ReviewCommand {
    Show {
        change_id: String,
        #[arg(long)]
        patchset: Option<String>,
    },
    /// Reconcile an original push without creating a second mutation.
    Operation { request_id: String },
    Approve {
        change_id: String,
        #[arg(long)]
        patchset: String,
        #[arg(long)]
        candidate: String,
        #[arg(long)]
        request_id: Option<String>,
    },
    RequestChanges {
        change_id: String,
        #[arg(long)]
        patchset: String,
        #[arg(long)]
        candidate: String,
        #[arg(long)]
        request_id: Option<String>,
    },
    Comment {
        change_id: String,
        #[arg(long)]
        patchset: String,
        #[arg(long)]
        body: String,
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        request_id: Option<String>,
    },
    /// Land only the exact server-rebased candidate, through server-owned checks.
    Land {
        change_id: String,
        #[arg(long)]
        patchset: String,
        #[arg(long)]
        candidate: String,
        #[arg(long)]
        request_id: Option<String>,
    },
}
fn hex<const N: usize>(value: String) -> Result<String, CommandError> {
    service::id::<N>(&value)?;
    Ok(value)
}
pub(super) async fn push(
    ui: &Ui,
    command: &CommandHelper,
    machine: bool,
    args: PushArgs,
) -> Result<(), CommandError> {
    if args.source_base.is_none() && args.source_head.is_none() {
        if tokio::runtime::Handle::try_current().is_ok() {
            return automatic(ui, command, machine, args).await;
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(user_error)?;
        return runtime.block_on(automatic(ui, command, machine, args));
    }
    if args.source_base.is_none() || args.source_head.is_none() || args.view_version.is_none() {
        return Err(user_error(
            "explicit push requires source-base, source-head and view-version together",
        ));
    }
    let predecessor = match (args.predecessor_change, args.predecessor_patchset) {
        (Some(change), Some(patchset)) => {
            Some(json!({"change_id":hex::<16>(change)?,"patchset_id":hex::<32>(patchset)?}))
        }
        (None, None) => None,
        _ => {
            return Err(user_error(
                "predecessor requires an exact change and patchset",
            ));
        }
    };
    let body = json!({"source_base":hex::<32>(args.source_base.unwrap())?,"source_head":hex::<32>(args.source_head.unwrap())?,"view_version":hex::<32>(args.view_version.unwrap())?,"expected_patchset":args.expected_patchset.clone().map(hex::<32>).transpose()?,"predecessor":predecessor,"provenance":args.provenance.map(hex::<32>).transpose()?});
    rpc::print(
        ui,
        machine,
        rpc::call(
            &service::context()?,
            Op::PushReview,
            body,
            args.request_id,
            true,
            None,
        )
        .await?,
    )
}
async fn automatic(
    ui: &Ui,
    command: &CommandHelper,
    machine: bool,
    args: PushArgs,
) -> Result<(), CommandError> {
    if args.view_version.is_some() {
        return Err(user_error(
            "local push uses the workspace frozen view version",
        ));
    }
    let global = command.global_args();
    if global.no_integrate_operation || global.ignore_working_copy || global.at_operation.is_some()
    {
        return Err(user_error(
            "forge push requires the current working-copy operation",
        ));
    }
    let loader = command.workspace_loader()?;
    let root = loader.workspace_root().to_path_buf();
    let repo = loader.repo_path().to_path_buf();
    let scope = super::workspace::Scope::load(&root)?;
    if scope.context != service::context()? {
        return Err(user_error(
            "selected account/view differs from this workspace's frozen scope",
        ));
    }
    let mut helper = command.workspace_helper(ui).await?;
    let head = helper
        .resolve_single_rev(ui, args.revision.as_ref().unwrap_or(&RevisionArg::AT))
        .await?;
    if head.parent_ids().len() != 1 || head.tree_ids().as_resolved().is_none() {
        return Err(user_error(
            "each review patchset requires exactly one resolved single-parent source commit",
        ));
    }
    let source_base: [u8; 32] = head.parent_ids()[0]
        .as_bytes()
        .try_into()
        .map_err(|_| user_error("invalid source base identity"))?;
    if args.predecessor_change.is_none() && source_base != scope.base {
        return Err(user_error(
            "select one local change directly above the workspace base; stacked patches require an explicit predecessor change/patchset",
        ));
    }
    if head.id().as_bytes() == scope.base {
        return Err(user_error("no local patch above the projected baseline"));
    }
    let locator = crate::broker::locator(&repo.join("store")).map_err(user_error)?;
    let provider = native_broker::BrokerObjectIo::connect(
        locator.socket,
        locator.store,
        repo.join("store/native-broker-spool"),
        Default::default(),
    )
    .await
    .map_err(user_error)?;
    let provider = if let Some(token) = crate::commands::token(&repo).map_err(user_error)? {
        provider.with_command_token(token)
    } else {
        provider
    };
    let provider: std::sync::Arc<dyn jj_native_backend::NativeObjectIo> =
        std::sync::Arc::new(provider);
    let head_id: [u8; 32] = head
        .id()
        .as_bytes()
        .try_into()
        .map_err(|_| user_error("invalid native head identity"))?;
    let lineage = super::upload::lineage(&provider, source_base, head_id).await?;
    let predecessor = match (args.predecessor_change, args.predecessor_patchset) {
        (Some(change), Some(patchset)) => {
            Some(json!({"change_id":hex::<16>(change)?,"patchset_id":hex::<32>(patchset)?}))
        }
        (None, None) => None,
        _ => {
            return Err(user_error(
                "predecessor requires exact change/patchset identities",
            ));
        }
    };
    let body = json!({"source_base":super::workspace::hex(&source_base),"source_head":head.id().hex(),"view_version":super::workspace::hex(&scope.view_version),"expected_patchset":args.expected_patchset.clone().map(hex::<32>).transpose()?,"predecessor":predecessor,"provenance":args.provenance.map(hex::<32>).transpose()?,"lineage":lineage});
    let key = args
        .request_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    let source = super::pending::Source {
        workspace: root
            .to_str()
            .ok_or_else(|| user_error("workspace path must be UTF-8"))?
            .into(),
        operation_id: helper.repo().op_id().hex(),
        base: super::workspace::hex(&source_base),
        head: head.id().hex(),
    };
    // The loader fence prevents older collectors from ignoring this new root set.
    crate::broker::require_forge_reader(&repo.join("store")).map_err(user_error)?;
    let mut retained = super::source::LockedSource::open(
        &crate::vfs::working_copy_path(&root).map_err(user_error)?,
    )
    .map_err(user_error)?;
    let change_id = head.change_id().hex();
    if let Some(first) = retained.state.requests.first() {
        if first.change_id != change_id {
            return Err(user_error(
                "one active logical review change is allowed per workspace; reconcile it before submitting a different change",
            ));
        }
        if !retained.state.requests.iter().any(|r| r.request_id == key) {
            if let Some(uncertain) = retained
                .state
                .requests
                .iter()
                .find(|r| !r.superseded && r.review_dispatched && r.patchset_id.is_none())
            {
                return Err(user_error(format!(
                    "resume exact saved request {} before submitting another patchset",
                    uncertain.request_id
                )));
            }
            if retained
                .state
                .requests
                .iter()
                .any(|r| r.landed_projected_head.is_some() || r.apply_write_started)
            {
                return Err(user_error(
                    "finish exact landed reconciliation before submitting another patchset",
                ));
            }
            if let Some(latest) = retained
                .state
                .requests
                .iter()
                .rev()
                .find(|r| !r.superseded && r.patchset_id.is_some())
            {
                if args.expected_patchset.as_ref() != latest.patchset_id.as_ref() {
                    return Err(user_error(
                        "superseding this logical change requires --expected-patchset naming its latest accepted immutable patchset",
                    ));
                }
            } else if retained
                .state
                .requests
                .iter()
                .any(|r| r.patchset_id.is_some())
                && args.expected_patchset.is_none()
            {
                return Err(user_error(
                    "review advanced remotely; inspect it and supply its current --expected-patchset",
                ));
            }
        }
    }
    let outbound = super::pending::Pending::prepare(
        &scope.context,
        Op::PushReview as i32,
        &body,
        Some(key.clone()),
        Some(source.clone()),
    )
    .map_err(user_error)?;
    for old in &mut retained.state.requests {
        if old.request_id != key && old.patchset_id.is_none() && !old.review_dispatched {
            old.superseded = true;
        }
    }
    retained
        .retain(native_workspace_admin::forge_pending::PendingReview {
            request_id: key.clone(),
            scope_hash: scope.hash()?,
            change_id,
            patchset_id: None,
            review_dispatched: false,
            superseded: false,
            apply_write_started: false,
            source_operation: source.operation_id.clone(),
            source_base: source.base.clone(),
            source_head: source.head.clone(),
            view_version: scope.view_version,
            landed_projected_head: None,
            apply_operation: None,
        })
        .map_err(user_error)?;
    drop(outbound);
    if helper.get_wc_commit().await?.as_ref().map(|c| c.id()) == Some(head.id()) {
        let mut tx = helper.start_transaction();
        tx.check_out(&head)?;
        tx.finish(
            ui,
            "freeze submitted forge patch and start a fresh working-copy child",
        )
        .await?;
    }
    super::upload::closure(
        &scope.context,
        scope.view_version,
        source_base,
        head_id,
        &key,
        provider,
    )
    .await?;
    submit(
        ui,
        machine,
        &scope.context,
        body,
        key,
        source,
        &mut retained,
    )
    .await
}
async fn submit(
    ui: &Ui,
    machine: bool,
    context: &super::context::Context,
    body: serde_json::Value,
    key: String,
    source: super::pending::Source,
    retained: &mut super::source::LockedSource,
) -> Result<(), CommandError> {
    retained
        .state
        .requests
        .iter_mut()
        .find(|r| r.request_id == key)
        .ok_or_else(|| user_error("review source receipt disappeared"))?
        .review_dispatched = true;
    retained.save().map_err(user_error)?;
    let reply = match rpc::call(
        context,
        Op::PushReview,
        body,
        Some(key.clone()),
        true,
        Some(source),
    )
    .await
    {
        Ok(reply) => reply,
        Err(error) => {
            if error
                .error
                .downcast_ref::<rpc::CallError>()
                .is_some_and(|e| {
                    matches!(
                        e.status.code(),
                        tonic::Code::InvalidArgument
                            | tonic::Code::PermissionDenied
                            | tonic::Code::Unauthenticated
                            | tonic::Code::FailedPrecondition
                            | tonic::Code::Aborted
                            | tonic::Code::NotFound
                            | tonic::Code::AlreadyExists
                    )
                })
            {
                // A domain mutation can commit before reply projection fails.
                // Reconcile the exact key before releasing any source claim.
                match rpc::call(
                    context,
                    Op::GetReviewOperation,
                    json!({}),
                    Some(key.clone()),
                    false,
                    None,
                )
                .await
                {
                    Ok(reply) if reply.state == "not_found" => {
                        retained.state.requests.retain(|r| r.request_id != key);
                        retained.save().map_err(user_error)?;
                    }
                    Ok(reply) => {
                        accept(retained, &key, &reply)?;
                        return rpc::print(ui, machine, reply);
                    }
                    Err(_) => {
                        return Err(user_error(rpc::CallError {
                            status: tonic::Status::unknown(
                                "review outcome could not be reconciled after service error; source intent retained",
                            ),
                            request_id: Some(key),
                        }));
                    }
                }
            }
            return Err(error);
        }
    };
    accept(retained, &key, &reply)?;
    rpc::print(ui, machine, reply)
}
fn accept(
    retained: &mut super::source::LockedSource,
    key: &str,
    reply: &tinytree_client::forge_protocol::ForgeCallResponse,
) -> Result<bool, CommandError> {
    let value: serde_json::Value = serde_json::from_slice(&reply.result_json)
        .map_err(|_| user_error("invalid accepted review result"))?;
    let patchset = value
        .get("patchset_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| user_error("review result has no immutable patchset identity"))?;
    service::id::<32>(patchset)?;
    let record = retained
        .state
        .requests
        .iter_mut()
        .find(|r| r.request_id == key)
        .ok_or_else(|| user_error("review source receipt disappeared"))?;
    if value.get("source_head").and_then(serde_json::Value::as_str)
        != Some(record.source_head.as_str())
        || value.get("change_id").and_then(serde_json::Value::as_str)
            != Some(record.change_id.as_str())
        || record
            .patchset_id
            .as_ref()
            .is_some_and(|old| old != patchset)
    {
        return Err(user_error(
            "accepted review result differs from exact submitted source",
        ));
    }
    let is_current = value
        .get("is_current")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| user_error("review receipt has no current-version assertion"))?;
    let current = retained
        .state
        .acknowledge(key, patchset, is_current)
        .map_err(user_error)?;
    retained.save().map_err(user_error)?;
    Ok(current)
}
fn accept_saved(
    saved: &super::pending::Intent,
    reply: &tinytree_client::forge_protocol::ForgeCallResponse,
) -> Result<bool, CommandError> {
    let Some(source) = &saved.source else {
        return Ok(false);
    };
    let mut retained = super::source::LockedSource::open(
        &crate::vfs::working_copy_path(std::path::Path::new(&source.workspace))
            .map_err(user_error)?,
    )
    .map_err(user_error)?;
    if retained
        .state
        .requests
        .iter()
        .any(|r| r.request_id == saved.request_id)
    {
        return accept(&mut retained, &saved.request_id, reply);
    }
    Ok(false)
}
pub(super) async fn resume(
    ui: &Ui,
    command: &CommandHelper,
    machine: bool,
    saved: super::pending::Intent,
) -> Result<(), CommandError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return resume_inner(ui, command, machine, saved).await;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(user_error)?;
    runtime.block_on(resume_inner(ui, command, machine, saved))
}
async fn resume_inner(
    ui: &Ui,
    command: &CommandHelper,
    machine: bool,
    saved: super::pending::Intent,
) -> Result<(), CommandError> {
    match rpc::call(
        &saved.context,
        Op::GetReviewOperation,
        json!({}),
        Some(saved.request_id.clone()),
        false,
        None,
    )
    .await
    {
        Ok(reply) if reply.state == "not_found" => {}
        Ok(reply) => {
            let current = accept_saved(&saved, &reply)?;
            if !current || reply.state == "landed" {
                return rpc::print(ui, machine, reply);
            }
            // A known immutable patchset can be safely refreshed by replaying
            // its ORIGINAL push, so server-side rebase/check attempts advance.
            // Lookup absence alone never authorizes selecting a newer source.
            let source = saved
                .source
                .clone()
                .ok_or_else(|| user_error("local review source missing"))?;
            let mut retained = super::source::LockedSource::open(
                &crate::vfs::working_copy_path(std::path::Path::new(&source.workspace))
                    .map_err(user_error)?,
            )
            .map_err(user_error)?;
            return submit(
                ui,
                machine,
                &saved.context,
                saved.payload.clone(),
                saved.request_id.clone(),
                source,
                &mut retained,
            )
            .await;
        }
        Err(error) => {
            if !error
                .error
                .downcast_ref::<rpc::CallError>()
                .is_some_and(|e| e.status.code() == tonic::Code::NotFound)
            {
                return Err(error);
            }
        }
    }
    let source = saved
        .source
        .as_ref()
        .ok_or_else(|| user_error("saved local review has no source receipt"))?;
    let loader = command.workspace_loader()?;
    let root = loader.workspace_root().to_path_buf();
    let repo = loader.repo_path().to_path_buf();
    if root.to_str() != Some(source.workspace.as_str()) {
        return Err(user_error(
            "resume the saved review from its original workspace",
        ));
    }
    let scope = super::workspace::Scope::load(&root)?;
    if scope.context != saved.context {
        return Err(user_error(
            "saved review context differs from the workspace",
        ));
    }
    let mut retained = super::source::LockedSource::open(
        &crate::vfs::working_copy_path(&root).map_err(user_error)?,
    )
    .map_err(user_error)?;
    let record = retained
        .state
        .requests
        .iter()
        .find(|r| r.request_id == saved.request_id)
        .ok_or_else(|| user_error("saved review source retention receipt is missing"))?;
    if record.superseded {
        return Err(user_error(
            "this unsent source intent was superseded; resume the newer saved request",
        ));
    }
    if record.source_operation != source.operation_id
        || record.source_base != source.base
        || record.source_head != source.head
        || record.scope_hash != scope.hash()?
    {
        return Err(user_error("saved review source retention binding mismatch"));
    }
    let _helper = command.workspace_helper(ui).await?;
    let locator = crate::broker::locator(&repo.join("store")).map_err(user_error)?;
    let provider = native_broker::BrokerObjectIo::connect(
        locator.socket,
        locator.store,
        repo.join("store/native-broker-spool"),
        Default::default(),
    )
    .await
    .map_err(user_error)?;
    let provider = if let Some(token) = crate::commands::token(&repo).map_err(user_error)? {
        provider.with_command_token(token)
    } else {
        provider
    };
    super::upload::closure(
        &saved.context,
        record.view_version,
        service::id::<32>(&source.base)?,
        service::id::<32>(&source.head)?,
        &saved.request_id,
        std::sync::Arc::new(provider),
    )
    .await?;
    let source = saved
        .source
        .ok_or_else(|| user_error("local review source missing"))?;
    submit(
        ui,
        machine,
        &saved.context,
        saved.payload,
        saved.request_id,
        source,
        &mut retained,
    )
    .await
}
pub(super) async fn run(ui: &Ui, machine: bool, args: ReviewCommand) -> Result<(), CommandError> {
    let (op, body, key, mutation) = match args {
        ReviewCommand::Show {
            change_id,
            patchset,
        } => (
            Op::GetReview,
            json!({"change_id":hex::<16>(change_id)?,"patchset_id":patchset.map(hex::<32>).transpose()?}),
            None,
            false,
        ),
        ReviewCommand::Operation { request_id } => {
            if !super::pending::valid_id(&request_id) {
                return Err(user_error("invalid review request identity"));
            }
            (Op::GetReviewOperation, json!({}), Some(request_id), false)
        }
        ReviewCommand::Approve {
            change_id,
            patchset,
            candidate,
            request_id,
        } => (
            Op::DecideReview,
            json!({"change_id":hex::<16>(change_id)?,"patchset_id":hex::<32>(patchset)?,"decision":"approve","candidate_id":hex::<32>(candidate)?}),
            request_id,
            true,
        ),
        ReviewCommand::RequestChanges {
            change_id,
            patchset,
            candidate,
            request_id,
        } => (
            Op::DecideReview,
            json!({"change_id":hex::<16>(change_id)?,"patchset_id":hex::<32>(patchset)?,"decision":"request_changes","candidate_id":hex::<32>(candidate)?}),
            request_id,
            true,
        ),
        ReviewCommand::Comment {
            change_id,
            patchset,
            body,
            path,
            request_id,
        } => {
            if body.is_empty()
                || body.len() > 16384
                || path.as_ref().is_some_and(|p| p.len() > 4096)
            {
                return Err(user_error("comment body/path exceeds review limits"));
            }
            (
                Op::DecideReview,
                json!({"change_id":hex::<16>(change_id)?,"patchset_id":hex::<32>(patchset)?,"decision":"comment","body":body,"path":path}),
                request_id,
                true,
            )
        }
        ReviewCommand::Land {
            change_id,
            patchset,
            candidate,
            request_id,
        } => (
            Op::LandReview,
            json!({"change_id":hex::<16>(change_id)?,"patchset_id":hex::<32>(patchset)?,"candidate_id":hex::<32>(candidate)?}),
            request_id,
            true,
        ),
    };
    rpc::print(
        ui,
        machine,
        rpc::call(&service::context()?, op, body, key, mutation, None).await?,
    )
}
