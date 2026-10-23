//! Native publication uses workspace observations, never the host's last fetch.
use crate::{
    broker::{self, Locator},
    remote_state::{self, Kind, LockedState, Observation, Pending},
};
use jj_cli::{
    cli_util::{CommandHelper, WorkspaceCommandHelper},
    command_error::{CommandError, cli_error, user_error},
    ui::Ui,
};
use jj_lib::{
    backend::CommitId,
    object_id::ObjectId as _,
    op_store::{RefTarget, RemoteRef, RemoteRefState},
    ref_name::{RefNameBuf, RemoteName},
    repo::Repo as _,
};
use native_broker::{BrokerControlClient, ConfirmedPublication, PublishRequest, ReferenceSnapshot};
use std::{io::Write, path::PathBuf};

fn failure(error: impl std::fmt::Display) -> CommandError {
    user_error(format!("Native repository operation failed: {error}"))
}
fn reject_overrides(command: &CommandHelper) -> Result<(), CommandError> {
    let args = command.global_args();
    if args.no_integrate_operation || args.ignore_working_copy || args.at_operation.is_some() {
        return Err(cli_error(
            "native remote commands require the current working-copy operation",
        ));
    }
    Ok(())
}
fn names(reference: &str, bookmark: &str) -> Result<RefNameBuf, CommandError> {
    remote_state::validate_names(reference, bookmark).map_err(failure)?;
    jj_cli::revset_util::parse_bookmark_name(bookmark).map_err(failure)
}
fn native_id(id: &CommitId) -> Result<native_broker::CommitId, CommandError> {
    Ok(native_broker::CommitId(id.as_bytes().try_into().map_err(
        |_| user_error("native commit ID must contain 32 bytes"),
    )?))
}
fn parse_id(value: &str) -> Result<CommitId, CommandError> {
    if !remote_state::valid_hex(value, 64) {
        return Err(user_error("invalid saved native commit ID"));
    }
    let bytes = (0..32)
        .map(|n| u8::from_str_radix(&value[n * 2..n * 2 + 2], 16))
        .collect::<Result<Vec<_>, _>>()
        .map_err(failure)?;
    Ok(CommitId::from_bytes(&bytes))
}
fn native_hex(id: native_broker::CommitId) -> String {
    id.0.iter().map(|b| format!("{b:02x}")).collect()
}
fn target(head: Option<&str>) -> Result<RefTarget, CommandError> {
    head.map(|id| parse_id(id).map(RefTarget::normal))
        .unwrap_or_else(|| Ok(RefTarget::absent()))
}
fn remote_target(
    helper: &WorkspaceCommandHelper,
    bookmark: &jj_lib::ref_name::RefName,
) -> RefTarget {
    helper
        .repo()
        .view()
        .get_remote_bookmark(bookmark.to_remote_symbol(RemoteName::new("tinytree")))
        .target
        .clone()
}
fn pending(
    kind: Kind,
    reference: &str,
    bookmark: &str,
    generation: u64,
    head: Option<String>,
    helper: &WorkspaceCommandHelper,
) -> Pending {
    Pending {
        kind,
        reference: reference.to_owned(),
        bookmark: bookmark.to_owned(),
        generation,
        head,
        request_id: uuid::Uuid::new_v4().simple().to_string(),
        base_operation: helper.repo().op_id().hex(),
        confirmed_generation: None,
    }
}
async fn control(locator: &Locator) -> Result<BrokerControlClient, CommandError> {
    BrokerControlClient::connect(
        &locator.socket,
        locator.store.clone(),
        native_broker::Config::default(),
    )
    .await
    .map_err(failure)
}
fn open_state(command: &CommandHelper) -> Result<(Locator, LockedState), CommandError> {
    reject_overrides(command)?;
    let loader = command.workspace_loader()?;
    let locator = broker::locator(&loader.repo_path().join("store"))
        .map_err(|_| user_error("native remote commands require a broker-backed workspace"))?;
    let state = LockedState::open(
        &crate::vfs::working_copy_path(loader.workspace_root()).map_err(failure)?,
        &locator,
    )
    .map_err(failure)?;
    Ok((locator, state))
}
fn snapshot_observation(
    snapshot: &ReferenceSnapshot,
    reference: &str,
    bookmark: &str,
) -> Result<Observation, CommandError> {
    if snapshot.name != reference
        || snapshot
            .head
            .as_ref()
            .is_some_and(|head| head.head == native_broker::CommitId::ROOT)
    {
        return Err(user_error("native reference snapshot scope mismatch"));
    }
    Ok(Observation {
        reference: reference.to_owned(),
        bookmark: bookmark.to_owned(),
        generation: snapshot.generation,
        head: snapshot.head.as_ref().map(|head| native_hex(head.head)),
    })
}
fn validate_progress(
    previous: Option<&Observation>,
    next: &Observation,
) -> Result<(), CommandError> {
    if previous.is_some_and(|old| {
        next.generation < old.generation
            || (next.generation == old.generation && next.head != old.head)
    }) {
        return Err(user_error(
            "native reference observation regressed or changed without a generation; repository scope must be reconciled",
        ));
    }
    Ok(())
}
async fn integrate_fetch(
    ui: &mut Ui,
    helper: &mut WorkspaceCommandHelper,
    bookmark: &jj_lib::ref_name::RefName,
    observation: &Observation,
    checkout: bool,
) -> Result<(), CommandError> {
    let old = helper
        .repo()
        .view()
        .get_remote_bookmark(bookmark.to_remote_symbol(RemoteName::new("tinytree")))
        .tracked_target()
        .clone();
    let incoming = target(observation.head.as_deref())?;
    // The observation admitted this remote root. Extend the immutable native
    // checkpoint before reading its commit so jj never hydrates all ancestry
    // through the legacy index_commits compatibility path.
    let prepared_index = match (
        helper
            .repo()
            .readonly_index()
            .downcast_ref::<jj_native_index::NativeReadonlyIndex>(),
        incoming.as_normal(),
    ) {
        (Some(index), Some(id)) => Some(
            index
                .with_remote_roots(vec![
                    tinytree_model::CommitId::from_slice(id.as_bytes()).map_err(failure)?,
                ])
                .await
                .map_err(failure)?,
        ),
        _ => None,
    };
    let store = helper.repo().store().clone();
    let mut tx = helper.start_transaction();
    if let Some(index) = prepared_index.as_ref() {
        tx.repo_mut().merge_index_from(index)?;
    }
    let commit = match incoming.as_normal() {
        Some(id) => Some(store.get_commit_async(id).await?),
        None => None,
    };
    if let Some(commit) = &commit {
        tx.repo_mut()
            .index_commits(std::slice::from_ref(commit))
            .await?;
        tx.repo_mut()
            .add_heads(std::slice::from_ref(commit))
            .await?;
    }
    tx.repo_mut().ensure_remote(RemoteName::new("tinytree"));
    tx.repo_mut()
        .merge_local_bookmark(bookmark, &old, &incoming)
        .await?;
    tx.repo_mut().set_remote_bookmark(
        bookmark.to_remote_symbol(RemoteName::new("tinytree")),
        RemoteRef {
            target: incoming,
            state: RemoteRefState::Tracked,
        },
    );
    if checkout {
        let commit = commit
            .as_ref()
            .ok_or_else(|| user_error("cannot clone an absent native reference"))?;
        tx.check_out(commit)?;
    }
    tx.finish(ui, "fetch native reference").await?;
    Ok(())
}
fn confirmed_observation(
    intent: &Pending,
    receipt: &ConfirmedPublication,
) -> Result<Observation, CommandError> {
    let head = intent
        .head
        .as_ref()
        .ok_or_else(|| user_error("pending native publication has no target"))?;
    if receipt.idempotency_key != intent.request_id
        || receipt.name != intent.reference
        || native_hex(receipt.head) != *head
        || intent.generation.checked_add(1) != Some(receipt.generation)
    {
        return Err(user_error(
            "native publication receipt does not match the saved intent",
        ));
    }
    Ok(Observation {
        reference: intent.reference.clone(),
        bookmark: intent.bookmark.clone(),
        generation: receipt.generation,
        head: Some(head.clone()),
    })
}
async fn integrate_publication(
    ui: &mut Ui,
    helper: &mut WorkspaceCommandHelper,
    locked: &mut LockedState,
    intent: &Pending,
    receipt: &ConfirmedPublication,
    explicit_fetch: bool,
) -> Result<(), CommandError> {
    let observation = confirmed_observation(intent, receipt)?;
    remote_state::validate_names(&intent.reference, &intent.bookmark).map_err(failure)?;
    // Saved bookmark names are already canonical jj names, not CLI revset
    // expressions. Re-parsing would reject names which originally needed quotes.
    let bookmark: RefNameBuf = intent.bookmark.clone().into();
    let current = remote_target(helper, &bookmark);
    let previous = locked
        .state
        .observation(&intent.reference, &intent.bookmark)
        .map_err(failure)?;
    let old = target(previous.and_then(|entry| entry.head.as_deref()))?;
    let published = target(observation.head.as_deref())?;
    if !explicit_fetch && current != old && current != published {
        return Err(user_error(
            "native publication is confirmed but local tracking state diverged; run tinytree fetch to reconcile",
        ));
    }
    // A recovery fetch will merge from the actual jj tracking state afterward;
    // keep that state intact here so it can preserve local bookmark changes.
    if !explicit_fetch {
        let commit = helper
            .repo()
            .store()
            .get_commit_async(published.as_normal().unwrap())
            .await?;
        let mut tx = helper.start_transaction();
        tx.repo_mut()
            .add_heads(std::slice::from_ref(&commit))
            .await?;
        tx.repo_mut().ensure_remote(RemoteName::new("tinytree"));
        tx.repo_mut().set_remote_bookmark(
            bookmark.to_remote_symbol(RemoteName::new("tinytree")),
            RemoteRef {
                target: published,
                state: RemoteRefState::Tracked,
            },
        );
        tx.finish(ui, "record confirmed native publication").await?;
    }
    locked.state.record(observation).map_err(failure)?;
    locked.state.pending = None;
    locked.save().map_err(failure)?;
    Ok(())
}
async fn resolve_pending_push(
    ui: &mut Ui,
    helper: &mut WorkspaceCommandHelper,
    locked: &mut LockedState,
    client: &BrokerControlClient,
    explicit_fetch: bool,
) -> Result<bool, CommandError> {
    let Some(intent) = locked.state.pending.clone() else {
        return Ok(false);
    };
    if intent.kind != Kind::Push {
        return Ok(false);
    }
    let head =
        native_id(&parse_id(intent.head.as_deref().ok_or_else(|| {
            user_error("pending publication target missing")
        })?)?)?;
    let receipt = if let Some(generation) = intent.confirmed_generation {
        // This receipt was validated and fsynced before local jj integration.
        // Recovering that local step need not contact an offline upstream.
        ConfirmedPublication {
            idempotency_key: intent.request_id.clone(),
            name: intent.reference.clone(),
            generation,
            head,
        }
    } else {
        match client
            .publication(&intent.request_id)
            .await
            .map_err(failure)?
        {
            Some(receipt) => receipt,
            None => match client
                .publish(PublishRequest {
                    name: intent.reference.clone(),
                    head,
                    expected_generation: intent.generation,
                    idempotency_key: intent.request_id.clone(),
                })
                .await
            {
                Ok(receipt) => receipt,
                Err(error) if error.code() == native_broker::ControlCode::Aborted => {
                    // This exact CAS has been authoritatively rejected; it cannot
                    // commit later. None alone was never used as that proof.
                    locked.state.pending = None;
                    locked.save().map_err(failure)?;
                    if explicit_fetch {
                        return Ok(false);
                    }
                    return Err(user_error(
                        "native reference changed; fetch before retrying the push",
                    ));
                }
                Err(error) => return Err(failure(error)),
            },
        }
    };
    let generation = confirmed_observation(&intent, &receipt)?.generation;
    locked.state.pending.as_mut().unwrap().confirmed_generation = Some(generation);
    locked.save().map_err(failure)?;
    integrate_publication(ui, helper, locked, &intent, &receipt, explicit_fetch).await?;
    Ok(true)
}

pub(crate) async fn clone(
    ui: &mut Ui,
    command: &CommandHelper,
    destination: PathBuf,
    socket: PathBuf,
    store: String,
    reference: String,
    bookmark: String,
) -> Result<(), CommandError> {
    reject_overrides(command)?;
    let bookmark_name = names(&reference, &bookmark)?;
    let bookmark = bookmark_name.as_str().to_owned();
    let locator = Locator::new(command.cwd().join(socket), store).map_err(failure)?;
    #[cfg(not(feature = "protocol-fixture"))]
    broker::creation_policy(&locator).map_err(failure)?;
    let client = control(&locator).await?;
    let snapshot = client
        .observe_reference(&reference)
        .await
        .map_err(failure)?;
    let observation = snapshot_observation(&snapshot, &reference, &bookmark)?;
    if observation.head.is_none() {
        return Err(user_error(
            "cannot clone an absent native reference; use init and fetch before the first push",
        ));
    }
    let destination = command.cwd().join(destination);
    match std::fs::read_dir(&destination) {
        Ok(mut contents) => {
            if contents.next().is_some() {
                return Err(user_error("native clone destination must be empty"));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            crate::vfs::prepare_destination(&destination).map_err(failure)?
        }
        Err(error) => return Err(failure(error)),
    }
    let destination = destination.canonicalize().map_err(failure)?;
    let initial = parse_id(
        observation
            .head
            .as_deref()
            .expect("validated present native head"),
    )?;
    let (workspace, repo) =
        crate::vfs::initialize(ui, command, &destination, Some(locator.clone()), &initial).await?;
    let mut helper = command.for_workable_repo(ui, workspace, repo)?;
    let mut locked = LockedState::open(
        &crate::vfs::working_copy_path(&destination).map_err(failure)?,
        &locator,
    )
    .map_err(failure)?;
    locked
        .state
        .prepare(&reference, &bookmark)
        .map_err(failure)?;
    locked.state.pending = Some(pending(
        Kind::Fetch,
        &reference,
        &bookmark,
        observation.generation,
        observation.head.clone(),
        &helper,
    ));
    locked.save().map_err(failure)?;
    integrate_fetch(
        ui,
        &mut helper,
        &bookmark_name,
        &observation,
        cfg!(feature = "protocol-fixture"),
    )
    .await?;
    locked.state.record(observation).map_err(failure)?;
    locked.state.pending = None;
    locked.save().map_err(failure)?;
    writeln!(
        ui.status(),
        "Cloned native reference into {}",
        destination.display()
    )?;
    Ok(())
}
pub(crate) async fn fetch(
    ui: &mut Ui,
    command: &CommandHelper,
    reference: String,
    bookmark: String,
    abandon_pending_push: bool,
) -> Result<(), CommandError> {
    let bookmark_name = names(&reference, &bookmark)?;
    let bookmark = bookmark_name.as_str().to_owned();
    let (locator, mut locked) = open_state(command)?;
    locked
        .state
        .prepare(&reference, &bookmark)
        .map_err(failure)?;
    if abandon_pending_push {
        // Archive and clear in one fsynced local replacement BEFORE any new
        // network or jj mutation. This is not cancellation or absence proof.
        locked.state.archive_pending_push().map_err(failure)?;
        locked.save().map_err(failure)?;
    }
    let mut helper = command.workspace_helper(ui).await?;
    let client = control(&locator).await?;
    resolve_pending_push(ui, &mut helper, &mut locked, &client, true).await?;
    locked
        .state
        .observation(&reference, &bookmark)
        .map_err(failure)?;
    locked.state.pending = Some(pending(
        Kind::Fetch,
        &reference,
        &bookmark,
        0,
        None,
        &helper,
    ));
    locked.save().map_err(failure)?;
    let snapshot = client
        .observe_reference(&reference)
        .await
        .map_err(failure)?;
    let observation = snapshot_observation(&snapshot, &reference, &bookmark)?;
    validate_progress(
        locked
            .state
            .observation(&reference, &bookmark)
            .map_err(failure)?,
        &observation,
    )?;
    let intent = locked.state.pending.as_mut().unwrap();
    intent.generation = observation.generation;
    intent.head = observation.head.clone();
    locked.save().map_err(failure)?;
    integrate_fetch(ui, &mut helper, &bookmark_name, &observation, false).await?;
    locked.state.record(observation).map_err(failure)?;
    locked.state.pending = None;
    locked.save().map_err(failure)?;
    Ok(())
}
pub(crate) async fn push(
    ui: &mut Ui,
    command: &CommandHelper,
    reference: String,
    bookmark: String,
) -> Result<(), CommandError> {
    let bookmark_name = names(&reference, &bookmark)?;
    let bookmark = bookmark_name.as_str().to_owned();
    let (locator, mut locked) = open_state(command)?;
    locked
        .state
        .prepare(&reference, &bookmark)
        .map_err(failure)?;
    if locked.state.pending.as_ref().is_some_and(|intent| {
        intent.kind == Kind::Push && (intent.reference != reference || intent.bookmark != bookmark)
    }) {
        return Err(user_error(
            "another native publication is pending; recover its original reference or explicitly archive it with fetch",
        ));
    }
    let mut helper = command.workspace_helper(ui).await?;
    let client = control(&locator).await?;
    if locked
        .state
        .pending
        .as_ref()
        .is_some_and(|intent| intent.kind == Kind::Fetch)
    {
        return Err(user_error(
            "a previous native fetch did not finish; run tinytree fetch to reconcile",
        ));
    }
    if resolve_pending_push(ui, &mut helper, &mut locked, &client, false).await? {
        writeln!(
            ui.status(),
            "Recovered the previous native publication; push again to publish newer local changes."
        )?;
        return Ok(());
    }
    let observation = locked
        .state
        .observation(&reference, &bookmark)
        .map_err(failure)?
        .cloned()
        .ok_or_else(|| user_error("fetch this native reference before its first push"))?;
    if remote_target(&helper, &bookmark_name) != target(observation.head.as_deref())? {
        return Err(user_error(
            "native tracking state differs from its durable observation; fetch before pushing",
        ));
    }
    let local = helper.repo().view().get_local_bookmark(&bookmark_name);
    let head = local.as_normal().cloned().ok_or_else(|| {
        user_error("native push requires one non-conflicted local bookmark target")
    })?;
    let head_native = native_id(&head)?;
    if head_native == native_broker::CommitId::ROOT {
        return Err(user_error("native push cannot publish the intrinsic root"));
    }
    let intent = pending(
        Kind::Push,
        &reference,
        &bookmark,
        observation.generation,
        Some(head.hex()),
        &helper,
    );
    locked.state.pending = Some(intent.clone());
    locked.save().map_err(failure)?;
    let receipt = match client
        .publish(PublishRequest {
            name: reference,
            head: head_native,
            expected_generation: intent.generation,
            idempotency_key: intent.request_id.clone(),
        })
        .await
    {
        Ok(receipt) => receipt,
        Err(error) if error.code() == native_broker::ControlCode::Aborted => {
            locked.state.pending = None;
            locked.save().map_err(failure)?;
            return Err(user_error(
                "native reference changed; fetch before retrying the push",
            ));
        }
        Err(error) => return Err(failure(error)),
    };
    let generation = confirmed_observation(&intent, &receipt)?.generation;
    locked.state.pending.as_mut().unwrap().confirmed_generation = Some(generation);
    locked.save().map_err(failure)?;
    integrate_publication(ui, &mut helper, &mut locked, &intent, &receipt, false).await
}
