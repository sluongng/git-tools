//! Frozen native landing, short polling, and exact-operation local reconciliation.
use jj_cli::{
    cli_util::{CommandHelper, RevisionArg},
    command_error::{CommandError, user_error},
    ui::Ui,
};
#[derive(clap::Args, Clone, Debug)]
pub(crate) struct LandArgs {
    /// Inspect the existing original intent without launching new server work.
    #[arg(long, conflicts_with = "resume")]
    status: bool,
    /// Recover submission uncertainty and apply an exact committed result.
    #[arg(long)]
    resume: bool,
    /// Explicitly resubmit a restored pre-Start intent after lookup reports it absent.
    #[arg(long, requires = "resume")]
    submit_original: bool,
    #[arg(long)]
    base: Option<RevisionArg>,
    #[arg(long)]
    revision: Option<RevisionArg>,
    #[arg(long)]
    reference: Option<String>,
    #[arg(long)]
    bookmark: Option<String>,
    #[arg(long)]
    profile: Option<String>,
    #[arg(long)]
    profile_version: Option<String>,
}
pub(crate) async fn run(
    ui: &mut Ui,
    command: &CommandHelper,
    args: LandArgs,
) -> Result<(), CommandError> {
    #[cfg(all(unix, not(feature = "protocol-fixture")))]
    {
        native::run(ui, command, args).await
    }
    #[cfg(not(all(unix, not(feature = "protocol-fixture"))))]
    {
        let _ = (ui, command, args);
        Err(user_error(
            "native landing requires a supported lazy mounted working copy; no materialized fallback is available",
        ))
    }
}
#[cfg(all(unix, not(feature = "protocol-fixture")))]
mod native {
    use super::*;
    use crate::{broker, remote_state::LockedState};
    use jj_lib::{
        backend::CommitId,
        object_id::ObjectId as _,
        op_store::{OperationId, RefTarget, RemoteRef, RemoteRefState},
        ref_name::RemoteName,
        repo::Repo as _,
    };
    use native_broker::{BrokerControlClient, LandingIntent, LandingReport, landing_protocol as l};
    use native_workspace_admin::remote_state::{LandingPending, Observation};
    use std::io::Write;
    fn fail(error: impl std::fmt::Display) -> CommandError {
        user_error(format!("Native landing: {error}"))
    }
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
    fn bytes<const N: usize>(text: &str) -> Result<[u8; N], CommandError> {
        if !native_workspace_admin::remote_state::valid_hex(text, N * 2) {
            return Err(fail("invalid saved landing identity"));
        }
        let mut result = [0; N];
        for (i, pair) in text.as_bytes().chunks_exact(2).enumerate() {
            result[i] =
                u8::from_str_radix(std::str::from_utf8(pair).map_err(fail)?, 16).map_err(fail)?;
        }
        Ok(result)
    }
    fn id(text: &str) -> Result<CommitId, CommandError> {
        Ok(CommitId::from_bytes(&bytes::<32>(text)?))
    }
    fn wire(p: &LandingPending) -> Result<LandingIntent, CommandError> {
        let value = LandingIntent {
            request: l::StartLandingRequest {
                repository_id: String::new(),
                idempotency_key: p.request_id.clone(),
                source_base: bytes::<32>(&p.source_base)?.to_vec(),
                source_head: bytes::<32>(&p.source_head)?.to_vec(),
                destination: p.reference.clone(),
                expected_generation: p.generation,
                expected_head: bytes::<32>(&p.expected_head)?.to_vec(),
                profile_name: p.profile_name.clone(),
                expected_profile_version: p.profile_version.clone(),
            },
            source_stack: p
                .source_stack
                .iter()
                .map(|s| bytes(s).map(native_broker::CommitId))
                .collect::<Result<_, _>>()?,
            observed_job: p.job_id.as_deref().map(bytes).transpose()?,
        };
        value.validate().map_err(fail)?;
        Ok(value)
    }
    fn print(
        ui: &Ui,
        p: &LandingPending,
        phase: &str,
        candidate: Option<&[u8]>,
    ) -> Result<(), CommandError> {
        let report = serde_json::json!({"version":1,"request_id":p.request_id,"job_id":p.job_id,"phase":phase,
        "candidate":candidate.map(hex),"applied_operation":p.apply_operation,"acknowledged":p.acknowledged,
        "publication":"native_tinytree_only"});
        writeln!(ui.stdout(), "{report}")?;
        Ok(())
    }
    async fn client(
        locator: &broker::Locator,
        repo: &std::path::Path,
    ) -> Result<BrokerControlClient, CommandError> {
        let token = crate::commands::token(repo)
            .map_err(fail)?
            .ok_or_else(|| fail("native command fence required"))?;
        Ok(BrokerControlClient::connect(
            &locator.socket,
            locator.store.clone(),
            native_broker::Config::default(),
        )
        .await
        .map_err(fail)?
        .with_command_token(token))
    }
    pub(super) async fn run(
        ui: &mut Ui,
        command: &CommandHelper,
        args: LandArgs,
    ) -> Result<(), CommandError> {
        let global = command.global_args();
        if global.at_operation.is_some()
            || global.ignore_working_copy
            || global.no_integrate_operation
        {
            return Err(fail(
                "landing requires the current native working-copy operation",
            ));
        }
        if (args.status || args.resume)
            && (args.base.is_some()
                || args.revision.is_some()
                || args.reference.is_some()
                || args.bookmark.is_some()
                || args.profile.is_some()
                || args.profile_version.is_some())
        {
            return Err(fail("status/resume use only the persisted original intent"));
        }
        let loader = command.workspace_loader()?;
        let locator = broker::locator(&loader.repo_path().join("store")).map_err(fail)?;
        let mut locked = LockedState::open(
            &crate::vfs::working_copy_path(loader.workspace_root()).map_err(fail)?,
            &locator,
        )
        .map_err(fail)?;
        if locked.state.pending.is_some() {
            return Err(fail(
                "resolve the original pending native fetch/publication before landing; both original intents remain retained",
            ));
        }
        if let Some(pending) = locked
            .state
            .landing
            .as_ref()
            .filter(|p| p.restored_lookup_only)
            .cloned()
        {
            if !args.status && !args.resume {
                return Err(fail(
                    "recovery_required: restored landing must be looked up before a new intent",
                ));
            }
            // Gate BEFORE orphan reconciliation, saved checkout recovery and WC
            // freezing. Capsule receipts and operation tags are not authority.
            let control = client(&locator, loader.repo_path()).await?;
            match control.landing(wire(&pending)?, false).await {
                Ok(report) => {
                    let job = report.job();
                    let phase = l::Phase::try_from(job.phase).map_err(fail)?;
                    let mut display = pending.clone();
                    display.job_id = Some(hex(&job.id));
                    if args.status {
                        // Lookup-only really is lookup-only: no captured raw OP
                        // replay, snapshot, sidecar update, or fence release.
                        return print(
                            ui,
                            &display,
                            phase.as_str_name().to_ascii_lowercase().as_str(),
                            job.candidate.as_ref().map(|c| c.head.as_slice()),
                        );
                    }
                    if pending.acknowledged {
                        classify_acknowledged_claims(command, &mut locked, job)?;
                        return print(ui, locked.state.landing.as_ref().unwrap(), "applied", None);
                    }
                    if pending.apply_operation.is_some()
                        || pending.operation_writing.is_some()
                        || (!pending.fork_acknowledged && pending.fork_operation.is_some())
                    {
                        return Err(fail(
                            "recovery_required: restored unpublished operation requires semantic reconciliation; original operation and working copy preserved",
                        ));
                    }
                    if let Some(expected) = &pending.candidate_head {
                        if job
                            .candidate
                            .as_ref()
                            .is_some_and(|c| hex(&c.head) != *expected)
                        {
                            return Err(fail(
                                "recovery_required: original candidate binding differs; state preserved",
                            ));
                        }
                    }
                    let phase = record_job(&mut locked, job)?;
                    if phase == l::Phase::Committed {
                        // This path never replays a capsule's raw operation. It
                        // creates a fresh apply only after apply() verifies
                        // current admission, source divergence and canonical
                        // candidate contents against the original source stack.
                        apply(ui, command, &mut locked, &control, &report).await?;
                        return print(
                            ui,
                            locked.state.landing.as_ref().unwrap(),
                            "applied",
                            job.candidate.as_ref().map(|c| c.head.as_slice()),
                        );
                    }
                    if locked.state.landing.as_ref().unwrap().terminal.is_some() {
                        locked.state.landing.as_mut().unwrap().restored_lookup_only = false;
                        locked.save().map_err(fail)?;
                    }
                    return print(
                        ui,
                        locked.state.landing.as_ref().unwrap(),
                        phase.as_str_name().to_ascii_lowercase().as_str(),
                        job.candidate.as_ref().map(|c| c.head.as_slice()),
                    );
                }
                Err(error)
                    if error.code() == tonic::Code::NotFound
                        && pending.job_id.is_none()
                        && args.submit_original
                        && pending.apply_operation.is_none()
                        && pending.operation_writing.is_none()
                        && (pending.fork_operation.is_none() || pending.fork_acknowledged) =>
                {
                    // Only the explicit action may dispatch the exact original
                    // pre-Start request. No request ID or expiry is invented.
                    // Keep the restored fence through fresh freeze/Start too.
                    // A crash must not promote another captured orphan OP into
                    // trusted retry authority.
                }
                Err(error) => {
                    return Err(fail(format!(
                        "recovery_required: original landing lookup failed; source and intent preserved: {error}"
                    )));
                }
            }
        }
        // A saved raw operation is reconciled BEFORE ordinary helper loading can
        // snapshot a projection whose prior checkout has not yet been acknowledged.
        reconcile_orphan(command, &mut locked).await?;
        if let Some(p) = locked.state.landing.as_ref() {
            if !p.acknowledged {
                if let Some(op) = p.apply_operation.as_deref().or(if p.fork_acknowledged {
                    None
                } else {
                    p.fork_operation
                        .as_deref()
                        .filter(|op| *op != p.source_operation)
                }) {
                    recover_saved(ui, command, p, op, p.apply_operation.is_some()).await?;
                    if p.apply_operation.is_some() {
                        finish_observation(&mut locked)?;
                        locked.state.landing.as_mut().unwrap().acknowledged = true;
                    } else {
                        locked.state.landing.as_mut().unwrap().fork_acknowledged = true;
                    }
                    locked.save().map_err(fail)?;
                }
            }
        }
        if !args.status && !args.resume {
            if locked
                .state
                .landing
                .as_ref()
                .is_some_and(|p| !p.acknowledged && p.terminal.is_none())
            {
                return Err(fail(
                    "an original intent is pending; use --status or --resume",
                ));
            }
            let mut helper = command.workspace_helper(ui).await?;
            let reference = args
                .reference
                .ok_or_else(|| fail("--reference is required"))?;
            let bookmark = args
                .bookmark
                .ok_or_else(|| fail("--bookmark is required"))?;
            let observation = locked
                .state
                .observation(&reference, &bookmark)
                .map_err(fail)?
                .cloned()
                .ok_or_else(|| fail("fetch the destination before landing"))?;
            let expected_head = observation
                .head
                .clone()
                .ok_or_else(|| fail("landing requires an existing destination head"))?;
            let base = helper
                .resolve_single_rev(
                    ui,
                    args.base
                        .as_ref()
                        .ok_or_else(|| fail("--base is required"))?,
                )
                .await?;
            let head = helper
                .resolve_single_rev(ui, args.revision.as_ref().unwrap_or(&RevisionArg::AT))
                .await?;
            let mut stack = Vec::new();
            let mut cursor = head.clone();
            while cursor.id() != base.id() {
                if stack.len() >= 128
                    || cursor.id() == helper.repo().store().root_commit_id()
                    || cursor.parent_ids().len() != 1
                    || cursor.tree_ids().as_resolved().is_none()
                    || cursor.store_commit().secure_sig.is_some()
                {
                    return Err(fail(
                        "unsupported or over-limit source stack; require <=128 unsigned resolved linear commits",
                    ));
                }
                stack.push(cursor.id().hex());
                cursor = helper
                    .repo()
                    .store()
                    .get_commit_async(&cursor.parent_ids()[0])
                    .await?;
            }
            stack.reverse();
            let intent = LandingPending {
                observed: None,
                restored_lookup_only: false,
                request_id: uuid::Uuid::new_v4().simple().to_string(),
                source_base: base.id().hex(),
                source_head: head.id().hex(),
                reference,
                bookmark,
                generation: observation.generation,
                expected_head,
                profile_name: args.profile.ok_or_else(|| fail("--profile is required"))?,
                profile_version: args
                    .profile_version
                    .ok_or_else(|| fail("--profile-version is required"))?,
                source_operation: helper.repo().op_id().hex(),
                source_stack: stack,
                fork_operation: None,
                fork_acknowledged: false,
                job_id: None,
                result_roots: vec![],
                candidate_head: None,
                apply_operation: None,
                operation_writing: None,
                acknowledged: false,
                terminal: None,
            };
            // New explicit submission can replace only a known terminal/applied
            // journal. It never erases an uncertain Start or committed receipt.
            broker::require_landing_reader(&loader.repo_path().join("store")).map_err(fail)?;
            locked.state.landing = None;
            locked.state.begin_landing(intent).map_err(fail)?;
            locked.save().map_err(fail)?;
            freeze_working_copy(command, &mut helper, &mut locked).await?;
        }
        let pending = locked
            .state
            .landing
            .clone()
            .ok_or_else(|| fail("no landing intent exists"))?;
        if pending.acknowledged {
            if pending.candidate_head.is_some() || !pending.result_roots.is_empty() {
                let control = client(&locator, loader.repo_path()).await?;
                let report = control
                    .landing(wire(&pending)?, false)
                    .await
                    .map_err(fail)?;
                classify_acknowledged_claims(command, &mut locked, report.job())?;
            }
            return print(ui, locked.state.landing.as_ref().unwrap(), "applied", None);
        }
        if pending.fork_operation.is_none() {
            let mut helper = command.workspace_helper(ui).await?;
            freeze_working_copy(command, &mut helper, &mut locked).await?;
        }
        let control = client(&locator, loader.repo_path()).await?;
        let pending = locked.state.landing.clone().unwrap();
        let report = control
            .landing(wire(&pending)?, !args.status)
            .await
            .map_err(fail)?;
        fault_point("response_received")?;
        let job = report.job();
        let phase = record_job(&mut locked, job)?;
        if phase == l::Phase::Committed && args.resume {
            apply(ui, command, &mut locked, &control, &report).await?;
            return print(
                ui,
                locked.state.landing.as_ref().unwrap(),
                "applied",
                job.candidate.as_ref().map(|c| c.head.as_slice()),
            );
        }
        print(
            ui,
            locked.state.landing.as_ref().unwrap(),
            phase.as_str_name().to_ascii_lowercase().as_str(),
            job.candidate.as_ref().map(|c| c.head.as_slice()),
        )
    }
    fn record_job(locked: &mut LockedState, job: &l::LandingJob) -> Result<l::Phase, CommandError> {
        let saved = locked.state.landing.as_mut().unwrap();
        saved.job_id = Some(hex(&job.id));
        if let Some(c) = &job.candidate {
            saved.candidate_head = Some(hex(&c.head));
            saved.result_roots = std::iter::once(hex(&c.head))
                .chain(c.rewrites.iter().map(|r| hex(&r.new_commit)))
                .collect();
            saved.result_roots.sort();
            saved.result_roots.dedup();
        }
        let phase = l::Phase::try_from(job.phase).map_err(fail)?;
        saved.terminal = match phase {
            l::Phase::Conflict => Some("conflict"),
            l::Phase::Unsupported => Some("unsupported"),
            l::Phase::CheckFailed => Some("check_failed"),
            l::Phase::CheckUnavailable => Some("check_unavailable"),
            l::Phase::StaleTarget => Some("stale_target"),
            l::Phase::Expired => Some("expired"),
            _ => None,
        }
        .map(str::to_owned);
        if saved.terminal.is_some() {
            if job.roots_retained || saved.apply_operation.is_some() {
                return Err(fail(
                    "reconciliation_required: terminal result contradicts a retained apply operation; state preserved",
                ));
            }
            // A fresh checked nonapplicable terminal result can never be
            // applied. Preserve historical candidate data below, all source
            // work and operations, but explicitly retire its current root claim.
            // Capture itself never makes this classification for old journals.
            saved.candidate_head = None;
            saved.result_roots.clear();
        }
        locked.state.record_landing_observation(job).map_err(fail)?;
        locked.save().map_err(fail)?;
        Ok(phase)
    }
    async fn freeze_working_copy(
        command: &CommandHelper,
        helper: &mut jj_cli::cli_util::WorkspaceCommandHelper,
        locked: &mut LockedState,
    ) -> Result<(), CommandError> {
        let pending = locked.state.landing.clone().unwrap();
        if helper.repo().op_id().hex() != pending.source_operation {
            return Err(fail(
                "reconciliation_required: source changed before its working-copy freeze completed",
            ));
        }
        let current_wc = helper.get_wc_commit_id().cloned();
        if current_wc.as_ref().is_none_or(|id| {
            !pending
                .source_stack
                .iter()
                .any(|source| source == &id.hex())
        }) {
            locked.state.landing.as_mut().unwrap().fork_operation = Some(pending.source_operation);
            locked.state.landing.as_mut().unwrap().fork_acknowledged = true;
            return locked.save().map_err(fail);
        }
        let repo = helper.repo().clone();
        let workspace = helper.workspace_name().to_owned();
        let mut tx =
            jj_cli::cli_util::start_repo_transaction(&repo, &workspace, command.string_args());
        stamp(&mut tx, &pending, "freeze");
        let source = repo
            .store()
            .get_commit_async(
                current_wc
                    .as_ref()
                    .expect("selected working copy checked above"),
            )
            .await?;
        let child = tx
            .repo_mut()
            .check_out(workspace, &source)
            .await
            .map_err(fail)?;
        tx.repo_mut().rebase_descendants().await?;
        locked.state.landing.as_mut().unwrap().operation_writing = Some("freeze".into());
        locked.save().map_err(fail)?;
        let unpublished = tx
            .write("freeze native landing source and start a fresh working-copy child")
            .await
            .map_err(fail)?;
        let op = unpublished.operation().id().clone();
        fault_point_receipt("freeze_written", Some(&op.hex()))?;
        locked.state.landing.as_mut().unwrap().fork_operation = Some(op.hex());
        locked.state.landing.as_mut().unwrap().operation_writing = None;
        locked.save().map_err(fail)?;
        let (mut wc, _) = helper.start_working_copy_mutation().await?;
        wc.locked_wc().check_out(&child).await.map_err(fail)?;
        unpublished.publish().await.map_err(fail)?;
        wc.finish(op).await.map_err(fail)?;
        locked.state.landing.as_mut().unwrap().fork_acknowledged = true;
        locked.save().map_err(fail)?;
        Ok(())
    }
    fn stamp(tx: &mut jj_lib::transaction::Transaction, p: &LandingPending, phase: &str) {
        for (key, value) in [
            ("request", p.request_id.as_str()),
            ("phase", phase),
            ("source", p.source_head.as_str()),
            ("source_operation", p.source_operation.as_str()),
        ] {
            tx.set_attribute(format!("tinytree.landing.{key}"), value.to_owned());
        }
        if let Some(id) = &p.job_id {
            tx.set_attribute("tinytree.landing.job".into(), id.clone());
        }
        if let Some(id) = &p.candidate_head {
            tx.set_attribute("tinytree.landing.candidate".into(), id.clone());
        }
    }
    async fn apply(
        ui: &mut Ui,
        command: &CommandHelper,
        locked: &mut LockedState,
        client: &BrokerControlClient,
        report: &LandingReport,
    ) -> Result<(), CommandError> {
        let p = locked.state.landing.clone().unwrap();
        let job = report.job();
        let candidate = job
            .candidate
            .as_ref()
            .ok_or_else(|| fail("committed candidate missing"))?;
        let receipt = job
            .receipt
            .as_ref()
            .ok_or_else(|| fail("committed receipt missing"))?;
        let mut helper = command.workspace_helper(ui).await?;
        let repo = helper.repo().clone();
        let workspace = helper.workspace_name().to_owned();
        // Explicit amendments/abandonment of frozen sources are divergence, not
        // permission to relabel the newer work as tested.
        for old in &p.source_stack {
            let old_id = id(old)?;
            let old_commit = repo.store().get_commit_async(&old_id).await?;
            let current = repo
                .resolve_change_id(old_commit.change_id())
                .await
                .map_err(fail)?
                .ok_or_else(|| {
                    fail("reconciliation_required: submitted source is no longer visible")
                })?;
            let visible = current
                .visible_with_offsets()
                .map(|(_, id)| id)
                .collect::<Vec<_>>();
            if visible != [&old_id] {
                return Err(fail(
                    "reconciliation_required: submitted source was amended or diverged; local changes preserved",
                ));
            }
        }
        let head = native_broker::CommitId(candidate.head.as_slice().try_into().map_err(fail)?);
        // A receipt's historical hold flag is not availability. Refresh complete
        // admission NOW, before index or canonical payload access.
        client.admit_commit(head).await.map_err(fail)?;
        let old_index = repo
            .readonly_index()
            .downcast_ref::<jj_native_index::NativeReadonlyIndex>()
            .ok_or_else(|| fail("native landing requires the federated lazy index"))?;
        let prepared = old_index
            .with_remote_roots(vec![head])
            .await
            .map_err(fail)?;
        let mut parent = id(&p.expected_head)?;
        let mut pairs = Vec::new();
        for old in &p.source_stack {
            let old_id = id(old)?;
            let old_commit = repo.store().get_commit_async(&old_id).await?;
            let row = candidate
                .rewrites
                .iter()
                .find(|r| r.old_commit == old_id.as_bytes());
            let new_id = row
                .map(|r| CommitId::from_bytes(&r.new_commit))
                .unwrap_or_else(|| old_id.clone());
            let new = repo.store().get_commit_async(&new_id).await?;
            if new.parent_ids() != [parent.clone()]
                || new.change_id() != old_commit.change_id()
                || new.description() != old_commit.description()
                || new.author() != old_commit.author()
                || new.store_commit().predecessors != old_commit.store_commit().predecessors
                || row.is_some_and(|r| r.change_id != old_commit.change_id().as_bytes())
            {
                return Err(fail(
                    "canonical landing candidate differs from the frozen source intent",
                ));
            }
            if new_id != old_id {
                pairs.push((old_id, new_id.clone()));
            }
            parent = new_id;
        }
        if parent.as_bytes() != candidate.head
            || repo
                .store()
                .get_commit_async(&parent)
                .await?
                .tree_ids()
                .as_resolved()
                .map(|id| id.as_bytes())
                != Some(candidate.root_tree.as_slice())
        {
            return Err(fail(
                "canonical landing head/root differs from the checked candidate",
            ));
        }
        let mut tx =
            jj_cli::cli_util::start_repo_transaction(&repo, &workspace, command.string_args());
        stamp(&mut tx, &p, "apply");
        tx.repo_mut().merge_index_from(&prepared)?;
        let new_head = repo.store().get_commit_async(&parent).await?;
        tx.repo_mut()
            .add_heads(std::slice::from_ref(&new_head))
            .await?;
        // Empty/self identity transitions deliberately produce no evolution edge.
        if !pairs.is_empty() {
            tx.repo_mut()
                .record_external_rewrites(&pairs)
                .await
                .map_err(fail)?;
        }
        tx.repo_mut().rebase_descendants().await?;
        let bookmark = jj_cli::revset_util::parse_bookmark_name(&p.bookmark).map_err(fail)?;
        let reference = receipt
            .reference
            .as_ref()
            .ok_or_else(|| fail("receipt reference missing"))?;
        let current = locked
            .state
            .observation(&p.reference, &p.bookmark)
            .map_err(fail)?;
        if current.is_none_or(|o| o.generation <= reference.generation) {
            let old = repo
                .view()
                .get_remote_bookmark(bookmark.to_remote_symbol(RemoteName::new("tinytree")))
                .tracked_target()
                .clone();
            tx.repo_mut()
                .merge_local_bookmark(&bookmark, &old, &RefTarget::normal(parent.clone()))
                .await
                .map_err(fail)?;
            tx.repo_mut().ensure_remote(RemoteName::new("tinytree"));
            tx.repo_mut().set_remote_bookmark(
                bookmark.to_remote_symbol(RemoteName::new("tinytree")),
                RemoteRef {
                    target: RefTarget::normal(parent),
                    state: RemoteRefState::Tracked,
                },
            );
        }
        let new_wc = tx
            .repo()
            .view()
            .get_wc_commit_id(&workspace)
            .cloned()
            .ok_or_else(|| fail("landing transaction lost working copy"))?;
        let commit = repo.store().get_commit_async(&new_wc).await?;
        locked.state.landing.as_mut().unwrap().operation_writing = Some("apply".into());
        locked.save().map_err(fail)?;
        let unpublished = tx
            .write("apply exact checked native landing and rebase local descendants")
            .await
            .map_err(fail)?;
        let op = unpublished.operation().id().clone();
        fault_point_receipt("apply_written", Some(&op.hex()))?;
        locked.state.landing.as_mut().unwrap().apply_operation = Some(op.hex());
        locked.state.landing.as_mut().unwrap().operation_writing = None;
        locked.save().map_err(fail)?;
        fault_point("apply_saved")?;
        let (mut wc, _) = helper.start_working_copy_mutation().await?;
        // Native checkout validates clean source-state/version before changing the
        // projection. Concurrent raw edits produce a retained rejection, not reset.
        wc.locked_wc().check_out(&commit).await.map_err(fail)?;
        fault_point("checkout_applied")?;
        unpublished.publish().await.map_err(fail)?;
        wc.finish(op).await.map_err(fail)?;
        finish_observation(locked)?;
        locked.state.landing.as_mut().unwrap().acknowledged = true;
        // One durable ACK save releases the restored fence and current result
        // claims only after exact checkout, publication and WC finish. The
        // selected immutable apply OP/VIEW retains the historical candidate.
        classify_acknowledged_claims(command, locked, job)?;
        Ok(())
    }
    fn classify_acknowledged_claims(
        command: &CommandHelper,
        locked: &mut LockedState,
        job: &l::LandingJob,
    ) -> Result<(), CommandError> {
        let pending = locked.state.landing.as_ref().unwrap();
        if !pending.acknowledged
            || pending.operation_writing.is_some()
            || job.phase != l::Phase::Committed as i32
        {
            return Err(fail(
                "recovery_required: result claims require an acknowledged committed apply",
            ));
        }
        let observed =
            native_workspace_admin::remote_state::historical_landing_job(job).map_err(fail)?;
        observed.validate(pending).map_err(fail)?;
        if let Some(old) = &pending.observed {
            if old.phase == native_workspace_admin::remote_state::HistoricalLandingPhase::Committed
                && (old.candidate != observed.candidate
                    || old.receipt != observed.receipt
                    || old.expires_at_seconds != observed.expires_at_seconds)
            {
                return Err(fail(
                    "recovery_required: committed historical receipt differs; state preserved",
                ));
            }
        }
        let candidate = observed
            .candidate
            .as_ref()
            .ok_or_else(|| fail("committed candidate missing"))?;
        let head = hex(&candidate.head);
        let saved = pending
            .apply_operation
            .as_deref()
            .ok_or_else(|| fail("acknowledged apply operation missing"))?;
        let mut factories = jj_lib::default_backend_factories::default_backend_factories();
        broker::register(&mut factories);
        let loader = jj_lib::repo::RepoLoader::init_from_file_system(
            command.settings(),
            command.workspace_loader()?.repo_path(),
            &factories,
        )
        .map_err(fail)?;
        let op_store = loader.op_store();
        let simple = op_store
            .downcast_ref::<jj_lib::simple_op_store::SimpleOpStore>()
            .ok_or_else(|| fail("unsupported landing operation store"))?;
        // Sequential fixed payload/decode caps bound the raw records. At most
        // two decoded 64MiB records and 16MiB raw bytes coexist; the reference
        // proof below performs no collection/all-referenced-ID allocation.
        let op_id =
            OperationId::try_from_hex(saved).ok_or_else(|| fail("invalid apply operation"))?;
        let raw = simple
            .read_operation_bounded(&op_id, 8 * 1024 * 1024, 64 * 1024 * 1024)
            .map_err(fail)?;
        if jj_lib::content_hash::blake2b_hash(&raw).as_slice() != op_id.as_bytes() {
            return Err(fail("apply operation semantic identity differs"));
        }
        for (key, value) in [
            ("request", pending.request_id.as_str()),
            ("phase", "apply"),
            ("source", pending.source_head.as_str()),
            ("source_operation", pending.source_operation.as_str()),
            (
                "job",
                pending
                    .job_id
                    .as_deref()
                    .ok_or_else(|| fail("apply job missing"))?,
            ),
            ("candidate", head.as_str()),
        ] {
            if raw
                .metadata
                .attributes
                .get(&format!("tinytree.landing.{key}"))
                .map(String::as_str)
                != Some(value)
            {
                return Err(fail("acknowledged apply operation binding differs"));
            }
        }
        let view = simple
            .read_view_bounded(&raw.view_id, 8 * 1024 * 1024, 64 * 1024 * 1024)
            .map_err(fail)?;
        if jj_lib::content_hash::blake2b_hash(&view).as_slice() != raw.view_id.as_bytes() {
            return Err(fail("apply view semantic identity differs"));
        }
        let mut work = 0usize;
        // Self-identity results already remain explicit original-source roots.
        // Rewritten results can also be retained by this exact immutable OP's
        // predecessor relation when a newer fetched bookmark hides the head.
        let mut found = head == pending.source_head;
        let mut visit = |id: Option<&CommitId>| -> Result<(), CommandError> {
            work = work
                .checked_add(1)
                .filter(|n| *n <= 100_000)
                .ok_or_else(|| fail("acknowledged view reference work bound"))?;
            found |= id.is_some_and(|id| id.as_bytes() == candidate.head);
            Ok(())
        };
        if let Some(predecessors) = &raw.commit_predecessors {
            for (id, parents) in predecessors {
                visit(None)?;
                visit(Some(id))?;
                for parent in parents {
                    visit(Some(parent))?;
                }
            }
        }
        for id in view.head_ids.iter().chain(view.wc_commit_ids.values()) {
            visit(Some(id))?;
        }
        for target in view
            .local_bookmarks
            .values()
            .chain(view.local_tags.values())
            .chain(view.git_refs.values())
            .chain(view.git_heads.values())
        {
            visit(None)?;
            for id in target.iter().flatten() {
                visit(Some(id))?;
            }
        }
        for remote in view.remote_views.values() {
            visit(None)?;
            for reference in remote.bookmarks.values().chain(remote.tags.values()) {
                visit(None)?;
                for id in reference.target.iter().flatten() {
                    visit(Some(id))?;
                }
            }
        }
        if !found {
            return Err(fail(
                "recovery_required: selected apply operation/view does not retain the candidate",
            ));
        }
        // Membership proves retention only. No checkout, operation publication
        // or raw recovery occurs in this acknowledged bookkeeping path.
        let saved = locked.state.landing.as_mut().unwrap();
        saved.candidate_head = None;
        saved.result_roots.clear();
        saved.restored_lookup_only = false;
        locked.state.record_landing_observation(job).map_err(fail)?;
        locked.save().map_err(fail)
    }
    fn finish_observation(locked: &mut LockedState) -> Result<(), CommandError> {
        let p = locked.state.landing.as_ref().unwrap();
        let generation = p
            .generation
            .checked_add(1)
            .ok_or_else(|| fail("landing generation exhausted"))?;
        if locked
            .state
            .observation(&p.reference, &p.bookmark)
            .map_err(fail)?
            .is_none_or(|o| o.generation <= generation)
        {
            locked
                .state
                .record(Observation {
                    reference: p.reference.clone(),
                    bookmark: p.bookmark.clone(),
                    generation,
                    head: p.candidate_head.clone(),
                })
                .map_err(fail)?;
        }
        Ok(())
    }
    async fn reconcile_orphan(
        command: &CommandHelper,
        locked: &mut LockedState,
    ) -> Result<(), CommandError> {
        let Some(p) = locked
            .state
            .landing
            .clone()
            .filter(|p| p.operation_writing.is_some())
        else {
            return Ok(());
        };
        let phase = p.operation_writing.as_deref().unwrap();
        let mut factories = jj_lib::default_backend_factories::default_backend_factories();
        broker::register(&mut factories);
        let loader = jj_lib::repo::RepoLoader::init_from_file_system(
            command.settings(),
            command.workspace_loader()?.repo_path(),
            &factories,
        )
        .map_err(fail)?;
        let store = loader.op_store();
        let simple = store
            .downcast_ref::<jj_lib::simple_op_store::SimpleOpStore>()
            .ok_or_else(|| fail("unsupported landing operation store"))?;
        let ids = simple.retained_operation_ids(4096).map_err(fail)?;
        let mut found = None;
        let mut decoded = 0usize;
        for id in ids {
            let op = simple
                .read_operation_bounded(&id, 512 * 1024, 4 * 1024 * 1024)
                .map_err(fail)?;
            // Each decoded record is independently bounded, and total inventory
            // work is capped without loading any commit/TREE/FILE history.
            decoded = decoded
                .checked_add(op.parents.len() + op.metadata.attributes.len() + 1)
                .filter(|n| *n <= 65536)
                .ok_or_else(|| fail("landing operation recovery inventory limit exceeded"))?;
            let attrs = &op.metadata.attributes;
            if attrs.get("tinytree.landing.request") != Some(&p.request_id)
                || attrs.get("tinytree.landing.phase").map(String::as_str) != Some(phase)
            {
                continue;
            }
            if attrs.get("tinytree.landing.source") != Some(&p.source_head)
                || attrs.get("tinytree.landing.source_operation") != Some(&p.source_operation)
                || phase == "apply"
                    && (attrs.get("tinytree.landing.job") != p.job_id.as_ref()
                        || attrs.get("tinytree.landing.candidate") != p.candidate_head.as_ref())
                || op.commit_predecessors.is_none()
                || jj_lib::content_hash::blake2b_hash(&op).as_slice() != id.as_bytes()
            {
                return Err(fail("landing orphan operation differs from frozen intent"));
            }
            if found.replace(id.hex()).is_some() {
                return Err(fail(
                    "reconciliation_required: multiple exact landing operations remain retained",
                ));
            }
        }
        let journal = locked.state.landing.as_mut().unwrap();
        if phase == "apply" {
            journal.apply_operation = found;
        } else {
            journal.fork_operation = found;
        }
        journal.operation_writing = None;
        locked.save().map_err(fail)
    }
    async fn descendant_of(
        loader: &jj_lib::repo::RepoLoader,
        head: &OperationId,
        target: &OperationId,
    ) -> Result<bool, CommandError> {
        let mut pending = vec![head.clone()];
        let mut seen = std::collections::HashSet::new();
        while let Some(next) = pending.pop() {
            if &next == target {
                return Ok(true);
            }
            if !seen.insert(next.clone()) {
                continue;
            }
            if seen.len() > 1024 || pending.len() > 4096 {
                return Err(fail(
                    "reconciliation_required: operation recovery ancestry budget exceeded",
                ));
            }
            let op = crate::vfs::load_recovery_operation(
                loader.op_store().clone(),
                &next,
                8 * 1024 * 1024,
                64 * 1024 * 1024,
            )
            .map_err(fail)?;
            pending.extend(op.parent_ids().iter().cloned());
        }
        Ok(false)
    }
    async fn recover_saved(
        ui: &mut Ui,
        command: &CommandHelper,
        p: &LandingPending,
        saved: &str,
        apply: bool,
    ) -> Result<(), CommandError> {
        let registry = native_workspace_admin::Registry::open_existing(
            native_workspace_admin::Registry::default_path().map_err(fail)?,
        )
        .map_err(fail)?;
        let root = command.workspace_loader()?.workspace_root().to_owned();
        let binding = registry.lookup(&root).map_err(fail)?;
        let mut factories = jj_lib::default_backend_factories::default_backend_factories();
        broker::register(&mut factories);
        let loader = jj_lib::repo::RepoLoader::init_from_file_system(
            command.settings(),
            &binding.repo,
            &factories,
        )
        .map_err(fail)?;
        let saved_id =
            OperationId::try_from_hex(saved).ok_or_else(|| fail("invalid saved operation ID"))?;
        let operation = crate::vfs::load_recovery_operation(
            loader.op_store().clone(),
            &saved_id,
            8 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .map_err(fail)?;
        let attrs = &operation.metadata().attributes;
        for (key, value) in [
            ("request", p.request_id.as_str()),
            ("phase", if apply { "apply" } else { "freeze" }),
            ("source", p.source_head.as_str()),
            ("source_operation", p.source_operation.as_str()),
        ] {
            if attrs
                .get(&format!("tinytree.landing.{key}"))
                .map(String::as_str)
                != Some(value)
            {
                return Err(fail(
                    "saved operation is not the exact original landing intent",
                ));
            }
        }
        if operation.parent_ids().len() != 1
            || !operation.stores_commit_predecessors()
            || apply
                && (attrs.get("tinytree.landing.job") != p.job_id.as_ref()
                    || attrs.get("tinytree.landing.candidate") != p.candidate_head.as_ref())
        {
            return Err(fail("invalid saved landing operation provenance"));
        }
        let heads = loader.op_heads_store().get_op_heads().await.map_err(fail)?;
        let mut published = false;
        for head in &heads {
            if descendant_of(&loader, head, &saved_id).await? {
                published = true;
            }
        }
        if !published && heads != operation.parent_ids() {
            return Err(fail(
                "reconciliation_required: local operations advanced after the saved unpublished operation; no history or edits overwritten",
            ));
        }
        let retained = jj_vfs::native::read_retained_state(
            &binding.root,
            &binding.working_copy,
            &binding.repository,
        )
        .map_err(fail)?;
        if retained.pending {
            crate::vfs::recover(command, &root, Some(saved.to_owned())).await?;
        }
        let retained = jj_vfs::native::read_retained_state(
            &binding.root,
            &binding.working_copy,
            &binding.repository,
        )
        .map_err(fail)?;
        if !published && retained.operation == saved_id {
            let _guard = loader.op_heads_store().lock().await.map_err(fail)?;
            if loader.op_heads_store().get_op_heads().await.map_err(fail)? != operation.parent_ids()
            {
                return Err(fail(
                    "reconciliation_required: operation heads changed before exact publication",
                ));
            }
            loader
                .op_heads_store()
                .update_op_heads(operation.parent_ids(), &saved_id)
                .await
                .map_err(fail)?;
            return Ok(());
        }
        if published && descendant_of(&loader, &retained.operation, &saved_id).await? {
            return Ok(());
        }
        let mut helper = command.workspace_helper(ui).await?;
        if !published && helper.repo().op_id() != &operation.parent_ids()[0] {
            return Err(fail(
                "reconciliation_required: edits or operations advanced before checkout; saved exact operation remains retained",
            ));
        }
        if published {
            // Normal jj stale-workspace handling preserves subsequent local edits;
            // never force a historical candidate projection over a newer operation.
            if !descendant_of(&loader, helper.repo().op_id(), &saved_id).await? {
                return Err(fail(
                    "reconciliation_required: published landing is not in the current operation ancestry",
                ));
            }
            return Ok(());
        }
        let planned = loader.load_at(&operation).await.map_err(fail)?;
        let target = planned
            .view()
            .get_wc_commit_id(helper.workspace_name())
            .ok_or_else(|| fail("saved operation has no matching working copy"))?;
        let commit = planned.store().get_commit_async(target).await?;
        let (mut wc, _) = helper.start_working_copy_mutation().await?;
        wc.locked_wc().check_out(&commit).await.map_err(fail)?;
        {
            let _guard = loader.op_heads_store().lock().await.map_err(fail)?;
            if loader.op_heads_store().get_op_heads().await.map_err(fail)? != operation.parent_ids()
            {
                return Err(fail(
                    "reconciliation_required: operation heads changed before exact publication",
                ));
            }
            loader
                .op_heads_store()
                .update_op_heads(operation.parent_ids(), &saved_id)
                .await
                .map_err(fail)?;
        }
        wc.finish(saved_id).await.map_err(fail)?;
        Ok(())
    }
    fn fault_point(name: &str) -> Result<(), CommandError> {
        fault_point_receipt(name, None)
    }
    fn fault_point_receipt(name: &str, receipt: Option<&str>) -> Result<(), CommandError> {
        #[cfg(feature = "native-landing-fault-injection")]
        if std::env::var("TINYTREE_LANDING_TEST_POINT").ok().as_deref() == Some(name) {
            let path = std::env::var_os("TINYTREE_LANDING_TEST_MARKER")
                .ok_or_else(|| fail("test marker required"))?;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .map_err(fail)?;
            file.write_all(name.as_bytes()).map_err(fail)?;
            if let Some(receipt) = receipt {
                file.write_all(format!("\n{receipt}").as_bytes())
                    .map_err(fail)?;
            }
            file.sync_all().map_err(fail)?;
            // Only the test-only executable contains this bounded kill gate.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            return Err(fail("native landing test gate timed out"));
        }
        let _ = (name, receipt);
        Ok(())
    }
}
