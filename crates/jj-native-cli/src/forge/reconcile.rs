//! Apply only exact landed view-safe rewrites; preserve newer local iteration.
use super::{pending, service, source, workspace};
use jj_cli::{
    cli_util::CommandHelper,
    command_error::{CommandError, user_error},
    ui::Ui,
};
use jj_lib::{backend::CommitId, object_id::ObjectId as _, op_store::OperationId, repo::Repo as _};
use native_workspace_admin::forge_pending::PendingReview;
use serde::Deserialize;
use serde_json::Value;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rewrite {
    old: String,
    new: String,
    change_id: String,
}
fn fail(message: impl std::fmt::Display) -> CommandError {
    user_error(format!("Forge reconciliation: {message}"))
}
fn attrs(record: &PendingReview, landed: &str) -> Vec<(String, String)> {
    vec![
        ("tinytree.forge.request".into(), record.request_id.clone()),
        ("tinytree.forge.source".into(), record.source_head.clone()),
        (
            "tinytree.forge.source_operation".into(),
            record.source_operation.clone(),
        ),
        ("tinytree.forge.landed".into(), landed.into()),
    ]
}
fn matches(operation: &jj_lib::operation::Operation, record: &PendingReview, landed: &str) -> bool {
    let map = &operation.metadata().attributes;
    attrs(record, landed)
        .iter()
        .all(|(key, value)| map.get(key) == Some(value))
}
async fn descendant(
    loader: &jj_lib::repo::RepoLoader,
    head: &OperationId,
    target: &OperationId,
) -> Result<bool, CommandError> {
    let mut pending = vec![head.clone()];
    let mut seen = std::collections::HashSet::new();
    while let Some(id) = pending.pop() {
        if &id == target {
            return Ok(true);
        }
        if !seen.insert(id.clone()) {
            continue;
        }
        if seen.len() > 1024 || pending.len() > 4096 {
            return Err(fail(
                "operation ancestry recovery budget exceeded; state preserved",
            ));
        }
        let op = crate::vfs::load_recovery_operation(
            loader.op_store().clone(),
            &id,
            4 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .map_err(fail)?;
        pending.extend(op.parent_ids().iter().cloned());
    }
    Ok(false)
}
async fn recover(
    ui: &Ui,
    command: &CommandHelper,
    record: &PendingReview,
    landed: &str,
    saved: &str,
    completed: bool,
) -> Result<(), CommandError> {
    let root = command.workspace_loader()?.workspace_root().to_path_buf();
    let registry = native_workspace_admin::Registry::open_existing(
        native_workspace_admin::Registry::default_path().map_err(fail)?,
    )
    .map_err(fail)?;
    let binding = registry.lookup(&root).map_err(fail)?;
    let mut factories = jj_lib::default_backend_factories::default_backend_factories();
    crate::broker::register(&mut factories);
    let loader = jj_lib::repo::RepoLoader::init_from_file_system(
        command.settings(),
        &binding.repo,
        &factories,
    )
    .map_err(fail)?;
    let id =
        OperationId::try_from_hex(saved).ok_or_else(|| fail("invalid saved operation identity"))?;
    let operation = crate::vfs::load_recovery_operation(
        loader.op_store().clone(),
        &id,
        4 * 1024 * 1024,
        64 * 1024 * 1024,
    )
    .map_err(fail)?;
    if !matches(&operation, record, landed)
        || operation.parent_ids().len() != 1
        || !operation.stores_commit_predecessors()
    {
        return Err(fail(
            "saved operation does not prove this exact source and landed result",
        ));
    }
    let heads = loader.op_heads_store().get_op_heads().await.map_err(fail)?;
    let mut published = false;
    for head in &heads {
        published |= descendant(&loader, head, &id).await?;
    }
    if !published && heads != operation.parent_ids() {
        return Err(fail(
            "operations advanced after the saved apply; no history or files overwritten",
        ));
    }
    let retained = jj_vfs::native::read_retained_state(
        &binding.root,
        &binding.working_copy,
        &binding.repository,
    )
    .map_err(fail)?;
    if completed {
        if !published || retained.pending || !descendant(&loader, &retained.operation, &id).await? {
            return Err(fail(
                "completed apply receipt is not in the settled current operation history; no state changed",
            ));
        }
        return Ok(());
    }
    if retained.pending {
        crate::vfs::recover(command, &root, Some(saved.into())).await?;
    }
    let retained = jj_vfs::native::read_retained_state(
        &binding.root,
        &binding.working_copy,
        &binding.repository,
    )
    .map_err(fail)?;
    if !published && retained.operation == id {
        let _guard = loader.op_heads_store().lock().await.map_err(fail)?;
        if loader.op_heads_store().get_op_heads().await.map_err(fail)? != operation.parent_ids() {
            return Err(fail(
                "operation heads changed before saved apply publication",
            ));
        }
        loader
            .op_heads_store()
            .update_op_heads(operation.parent_ids(), &id)
            .await
            .map_err(fail)?;
        return Ok(());
    }
    if published && descendant(&loader, &retained.operation, &id).await? {
        return Ok(());
    }
    let mut helper = command.workspace_helper(ui).await?;
    if published {
        if !descendant(&loader, helper.repo().op_id(), &id).await? {
            return Err(fail(
                "published apply is outside current operation ancestry; state preserved",
            ));
        }
        return Ok(());
    }
    if helper.repo().op_id() != &operation.parent_ids()[0] {
        return Err(fail(
            "edits advanced before exact apply checkout; state preserved",
        ));
    }
    let planned = loader.load_at(&operation).await.map_err(fail)?;
    let target = planned
        .view()
        .get_wc_commit_id(helper.workspace_name())
        .ok_or_else(|| fail("saved apply has no working-copy result"))?;
    let commit = planned.store().get_commit_async(target).await?;
    let (mut wc, _) = helper.start_working_copy_mutation().await?;
    wc.locked_wc().check_out(&commit).await.map_err(fail)?;
    {
        let _guard = loader.op_heads_store().lock().await.map_err(fail)?;
        if loader.op_heads_store().get_op_heads().await.map_err(fail)? != operation.parent_ids() {
            return Err(fail(
                "operation heads changed before exact apply publication",
            ));
        }
        loader
            .op_heads_store()
            .update_op_heads(operation.parent_ids(), &id)
            .await
            .map_err(fail)?;
    }
    wc.finish(id).await.map_err(fail)?;
    Ok(())
}

pub(super) async fn apply(
    ui: &Ui,
    command: &CommandHelper,
    saved: &pending::Intent,
    value: &Value,
) -> Result<&'static str, CommandError> {
    let source = saved
        .source
        .as_ref()
        .ok_or_else(|| fail("this request has no local source to reconcile"))?;
    let root = command.workspace_loader()?.workspace_root().to_path_buf();
    let repo_path = command.workspace_loader()?.repo_path().to_path_buf();
    if root.to_str() != Some(source.workspace.as_str()) {
        return Err(fail("use the original request workspace"));
    }
    let mut scope = workspace::Scope::load(&root)?;
    if scope.context != saved.context {
        return Err(fail("workspace account/view differs from saved request"));
    }
    let landed = value
        .get("projected_head")
        .and_then(Value::as_str)
        .ok_or_else(|| fail("landed review has no projected head"))?;
    let landed_bytes = service::id::<32>(landed)?;
    let version = value
        .get("view_version")
        .and_then(Value::as_str)
        .ok_or_else(|| fail("landed review has no view version"))?;
    if service::id::<32>(version)? != scope.view_version
        || value.get("policy_epoch").and_then(Value::as_u64) != Some(scope.policy_epoch)
    {
        return Err(fail(
            "view/policy changed; resync authorization without resetting local files",
        ));
    }
    let rewrites: Vec<Rewrite> = serde_json::from_value(
        value
            .get("projected_rewrites")
            .cloned()
            .ok_or_else(|| fail("landed rewrite proof missing"))?,
    )
    .map_err(|_| fail("invalid projected rewrite proof"))?;
    if rewrites.len() != 1 || rewrites[0].old != source.head || rewrites[0].new != landed {
        return Err(fail(
            "landed rewrite does not name this exact submitted source",
        ));
    }
    let change = service::id::<16>(&rewrites[0].change_id)?;
    let wc_path = crate::vfs::working_copy_path(&root).map_err(fail)?;
    let mut locked = source::LockedSource::open(&wc_path).map_err(fail)?;
    let Some(index) = locked
        .state
        .requests
        .iter()
        .position(|r| r.request_id == saved.request_id)
    else {
        let done = locked
            .state
            .completed
            .iter()
            .find(|r| r.request_id == saved.request_id)
            .ok_or_else(|| {
                fail("source retention receipt is missing or its completed receipt aged out")
            })?;
        if done.source_operation != source.operation_id
            || done.source_base != source.base
            || done.source_head != source.head
            || done.landed_projected_head.as_deref() != Some(landed)
            || done.view_version != scope.view_version
            || done.change_id != rewrites[0].change_id
        {
            return Err(fail("completed apply receipt scope mismatch"));
        }
        recover(
            ui,
            command,
            done,
            landed,
            done.apply_operation.as_deref().unwrap(),
            true,
        )
        .await?;
        return Ok("already_applied");
    };
    let mut record = locked.state.requests[index].clone();
    if record.source_operation != source.operation_id
        || record.source_head != source.head
        || record.source_base != source.base
        || record.view_version != scope.view_version
        || (scope.base != landed_bytes && record.scope_hash != scope.hash()?)
    {
        return Err(fail("source retention scope mismatch"));
    }
    if record
        .landed_projected_head
        .as_ref()
        .is_some_and(|id| id != landed)
    {
        return Err(fail("landed result changed after it was journaled"));
    }
    record.landed_projected_head = Some(landed.into());
    locked.state.requests[index] = record.clone();
    locked.save().map_err(fail)?;
    // Discover a write-before-receipt crash by exact private request attributes,
    // never by commit similarity or an arbitrary operation supplied by a server.
    if record.apply_operation.is_none() && record.apply_write_started {
        let workspace = command.load_workspace()?;
        let loader = workspace.repo_loader();
        let simple = loader
            .op_store()
            .downcast_ref::<jj_lib::simple_op_store::SimpleOpStore>()
            .ok_or_else(|| fail("unsupported operation store"))?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        for id in simple.retained_operation_ids(1024).map_err(fail)? {
            if std::time::Instant::now() >= deadline {
                return Err(fail("exact apply recovery inventory deadline exceeded"));
            }
            let operation = crate::vfs::load_recovery_operation(
                loader.op_store().clone(),
                &id,
                256 * 1024,
                1024 * 1024,
            )
            .map_err(fail)?;
            if matches(&operation, &record, landed) {
                if record.apply_operation.is_some() {
                    return Err(fail(
                        "multiple exact apply receipts retained; state preserved",
                    ));
                }
                record.apply_operation = Some(id.hex());
            }
        }
        locked.state.requests[index] = record.clone();
        locked.save().map_err(fail)?;
    }
    if let Some(operation) = &record.apply_operation {
        recover(ui, command, &record, landed, operation, false).await?;
    } else {
        let mut helper = command.workspace_helper(ui).await?;
        let old = CommitId::from_bytes(&service::id::<32>(&source.head)?);
        let current = helper
            .get_wc_commit()
            .await?
            .ok_or_else(|| fail("working-copy revision missing"))?;
        if !helper
            .repo()
            .index()
            .is_ancestor(&old, current.id())
            .await
            .map_err(fail)?
        {
            return Err(fail(
                "submitted source was amended or diverged; local iteration preserved",
            ));
        }
        let locator = crate::broker::locator(&repo_path.join("store")).map_err(fail)?;
        let mut control = native_broker::BrokerControlClient::connect(
            locator.socket,
            locator.store,
            Default::default(),
        )
        .await
        .map_err(fail)?;
        if let Some(token) = crate::commands::token(&repo_path).map_err(fail)? {
            control = control.with_command_token(token);
        }
        control
            .admit_commit(native_broker::CommitId(landed_bytes))
            .await
            .map_err(fail)?;
        let target = helper
            .repo()
            .store()
            .get_commit_async(&CommitId::from_bytes(&landed_bytes))
            .await?;
        let original = helper.repo().store().get_commit_async(&old).await?;
        if original.change_id().as_bytes() != change
            || target.change_id().as_bytes() != change
            || target.tree_ids().as_resolved().is_none()
        {
            return Err(fail("projected result logical identity/tree mismatch"));
        }
        let repo = helper.repo().clone();
        let workspace = helper.workspace_name().to_owned();
        let mut tx =
            jj_cli::cli_util::start_repo_transaction(&repo, &workspace, command.string_args());
        for (key, value) in attrs(&record, landed) {
            tx.set_attribute(key, value);
        }
        tx.repo_mut()
            .add_heads(std::slice::from_ref(&target))
            .await?;
        tx.repo_mut()
            .record_external_rewrites(&[(old, target.id().clone())])
            .await
            .map_err(fail)?;
        tx.repo_mut().rebase_descendants().await?;
        let wc_id = tx
            .repo()
            .view()
            .get_wc_commit_id(&workspace)
            .cloned()
            .ok_or_else(|| fail("apply lost working-copy identity"))?;
        let commit = repo.store().get_commit_async(&wc_id).await?;
        record.apply_write_started = true;
        locked.state.requests[index] = record.clone();
        locked.save().map_err(fail)?;
        let unpublished = tx
            .write("apply exact landed forge projection and preserve local descendants")
            .await
            .map_err(fail)?;
        let operation = unpublished.operation().id().clone();
        record.apply_operation = Some(operation.hex());
        locked.state.requests[index] = record.clone();
        locked.save().map_err(fail)?;
        let (mut wc, _) = helper.start_working_copy_mutation().await?;
        wc.locked_wc().check_out(&commit).await.map_err(fail)?;
        unpublished.publish().await.map_err(fail)?;
        wc.finish(operation).await.map_err(fail)?;
    }
    // Advance the next review's mapped base only after exact native WC ACK.
    let mut helper = command.workspace_helper(ui).await?;
    let target = helper
        .repo()
        .store()
        .get_commit_async(&CommitId::from_bytes(&landed_bytes))
        .await?;
    scope.base = landed_bytes;
    scope.tree = target
        .tree_ids()
        .as_resolved()
        .ok_or_else(|| fail("landed tree is unresolved"))?
        .as_bytes()
        .try_into()
        .map_err(|_| fail("invalid landed tree identity"))?;
    private_fs::PrivateDir::open_existing(&wc_path)
        .map_err(fail)?
        .replace(
            "forge-workspace.json",
            &serde_json::to_vec(&scope).map_err(fail)?,
        )
        .map_err(fail)?;
    locked
        .state
        .requests
        .retain(|request| request.change_id != record.change_id);
    if locked.state.completed.len() >= native_workspace_admin::forge_pending::MAX_REQUESTS {
        locked.state.completed.remove(0);
    }
    locked.state.completed.push(record);
    locked.save().map_err(fail)?;
    Ok("applied")
}
