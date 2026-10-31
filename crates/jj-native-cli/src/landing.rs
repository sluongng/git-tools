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
    use native_workspace_admin::landing_resolution as lr;
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
        checker: Option<&l::CheckerProgress>,
    ) -> Result<(), CommandError> {
        let report = serde_json::json!({"version":1,"request_id":p.request_id,"job_id":p.job_id,"phase":phase,
        "candidate":candidate.map(hex),"applied_operation":p.apply_operation,"acknowledged":p.acknowledged,
        "publication":"native_tinytree_only",
        "checker_progress":checker.map(|p| serde_json::json!({
          "stage":l::CheckerStage::try_from(p.stage).map(|v|v.as_str_name()).unwrap_or("CHECKER_STAGE_UNSPECIFIED"),
          "active_task":p.active_task,"attempts":p.attempts,"recoveries":p.recoveries,
          "saved_phase":l::CheckerSavedPhase::try_from(p.saved_phase).map(|v|v.as_str_name()).unwrap_or("CHECKER_SAVED_PHASE_UNSPECIFIED"),
          "automatic_recovery_blocked":p.automatic_recovery_blocked,
          "retry_at_seconds":p.retry_at_seconds,
          "error_code":l::CheckerErrorCode::try_from(p.error_code).map(|v|v.as_str_name()).unwrap_or("CHECKER_ERROR_NONE"),
          "error_stage":l::CheckerStage::try_from(p.error_stage).map(|v|v.as_str_name()).unwrap_or("CHECKER_STAGE_UNSPECIFIED")
        }))});
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
    fn recovery_context() -> Result<Option<tokio::runtime::EnterGuard<'static>>, CommandError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Ok(None);
        }
        // jj drives this borrowed, non-Send UI/command future with its futures
        // executor. A bounded owned reactor supplies timers and registry work;
        // entering it does not spawn the command or nest another block_on.
        static RUNTIME: std::sync::OnceLock<Result<tokio::runtime::Runtime, std::io::Error>> =
            std::sync::OnceLock::new();
        let runtime = RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .max_blocking_threads(8)
                .thread_name("landing-recovery")
                .enable_all()
                .build()
        });
        Ok(Some(runtime.as_ref().map_err(fail)?.enter()))
    }
    pub(super) async fn run(
        ui: &mut Ui,
        command: &CommandHelper,
        args: LandArgs,
    ) -> Result<(), CommandError> {
        let _context = recovery_context()?;
        #[cfg(feature = "native-landing-fault-injection")]
        let test_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let result = run_inner(ui, command, args).await;
        #[cfg(feature = "native-landing-fault-injection")]
        if std::env::var_os("TINYTREE_LANDING_TEST_CANONICAL_WORK").is_some() {
            // Observe actual reservation release in the same process, not by
            // assuming process exit freed an abandoned aggregate/read owner.
            let all = reserve_recovery_work(RECOVERY_WORK_CAP, test_deadline).await?;
            drop(all);
            eprintln!("native canonical test reservations released");
        }
        result
    }
    async fn run_inner(
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
            // Helper loading itself can merge plural operation heads. Reject
            // stale/uncertain restored state before lookup bookkeeping too.
            if args.status {
                restored_preflight(command)?;
            } else {
                inspect_restored(command)?;
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
                            job.checker_progress.as_ref(),
                        );
                    }
                    let phase = verify_original_report(&pending, &report)?;
                    if pending.acknowledged {
                        restored_preflight(command)?;
                        if phase != l::Phase::Committed {
                            return Err(fail(
                                "recovery_required: acknowledged landing no longer has its committed receipt",
                            ));
                        }
                        // A captured ACK is historical bookkeeping, never a
                        // request to create another OP, snapshot or checkout.
                        classify_acknowledged_claims(command, &mut locked, job).await?;
                        return print(
                            ui,
                            locked.state.landing.as_ref().unwrap(),
                            "applied",
                            None,
                            None,
                        );
                    }
                    if phase == l::Phase::Committed {
                        resume_restored_committed(ui, command, &mut locked, &control, &report)
                            .await?;
                        return print(
                            ui,
                            locked.state.landing.as_ref().unwrap(),
                            "applied",
                            job.candidate.as_ref().map(|c| c.head.as_slice()),
                            job.checker_progress.as_ref(),
                        );
                    }
                    let phase = if nonapplicable_phase(phase) {
                        settle_restored_terminal(command, &mut locked, &report).await?
                    } else {
                        settle_restored_freeze(ui, command, &mut locked, &control, Some(&job.id))
                            .await?;
                        record_job(&mut locked, job)?
                    };
                    return print(
                        ui,
                        locked.state.landing.as_ref().unwrap(),
                        phase.as_str_name().to_ascii_lowercase().as_str(),
                        job.candidate.as_ref().map(|c| c.head.as_slice()),
                        job.checker_progress.as_ref(),
                    );
                }
                Err(error)
                    if error.code() == tonic::Code::NotFound
                        && pending.job_id.is_none()
                        && args.submit_original
                        && pending.apply_operation.is_none() =>
                {
                    settle_restored_freeze(ui, command, &mut locked, &control, None).await?;
                    let pending = locked.state.landing.clone().unwrap();
                    let report = control.landing(wire(&pending)?, true).await.map_err(fail)?;
                    let phase = verify_original_report(&pending, &report)?;
                    if phase == l::Phase::Committed {
                        resume_restored_committed(ui, command, &mut locked, &control, &report)
                            .await?;
                        return print(
                            ui,
                            locked.state.landing.as_ref().unwrap(),
                            "applied",
                            report.job().candidate.as_ref().map(|c| c.head.as_slice()),
                            report.job().checker_progress.as_ref(),
                        );
                    }
                    if nonapplicable_phase(phase) {
                        settle_restored_terminal(command, &mut locked, &report).await?;
                    } else {
                        record_job(&mut locked, report.job())?;
                    }
                    return print(
                        ui,
                        locked.state.landing.as_ref().unwrap(),
                        phase.as_str_name().to_ascii_lowercase().as_str(),
                        report.job().candidate.as_ref().map(|c| c.head.as_slice()),
                        report.job().checker_progress.as_ref(),
                    );
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
            if let Some(previous) = locked.state.landing.clone() {
                // Retry closure before a new intent can discard the only
                // request/context that can safely clean its host records.
                let control = client(&locator, loader.repo_path()).await?;
                let report = control
                    .landing(wire(&previous)?, false)
                    .await
                    .map_err(fail)?;
                let phase = verify_original_report(&previous, &report)?;
                if previous.acknowledged && phase == l::Phase::Committed {
                    classify_acknowledged_claims(command, &mut locked, report.job()).await?;
                } else if nonapplicable_phase(phase) {
                    settle_restored_terminal(command, &mut locked, &report).await?;
                } else {
                    return Err(fail(
                        "recovery_required: previous landing is not freshly closed",
                    ));
                }
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
            if !args.status || pending.candidate_head.is_some() || !pending.result_roots.is_empty()
            {
                let control = client(&locator, loader.repo_path()).await?;
                let report = control
                    .landing(wire(&pending)?, false)
                    .await
                    .map_err(fail)?;
                classify_acknowledged_claims(command, &mut locked, report.job()).await?;
            }
            return print(
                ui,
                locked.state.landing.as_ref().unwrap(),
                "applied",
                None,
                None,
            );
        }
        if pending.fork_operation.is_none() {
            let mut helper =
                landing_helper(ui, command, pending.restored_lookup_only, true).await?;
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
        let phase =
            if nonapplicable_phase(l::Phase::try_from(job.phase).map_err(fail)?) && !args.status {
                settle_restored_terminal(command, &mut locked, &report).await?
            } else {
                record_job(&mut locked, job)?
            };
        if phase == l::Phase::Committed && args.resume {
            apply(ui, command, &mut locked, &control, &report).await?;
            return print(
                ui,
                locked.state.landing.as_ref().unwrap(),
                "applied",
                job.candidate.as_ref().map(|c| c.head.as_slice()),
                job.checker_progress.as_ref(),
            );
        }
        print(
            ui,
            locked.state.landing.as_ref().unwrap(),
            phase.as_str_name().to_ascii_lowercase().as_str(),
            job.candidate.as_ref().map(|c| c.head.as_slice()),
            job.checker_progress.as_ref(),
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
    struct RestoredPreflight {
        binding: native_workspace_admin::Binding,
        retained: jj_vfs::native::RetainedState,
        projection: jj_vfs::native::ProjectionState,
        heads: Vec<OperationId>,
    }
    fn inspect_restored(command: &CommandHelper) -> Result<RestoredPreflight, CommandError> {
        let loader = command.workspace_loader()?;
        let registry = native_workspace_admin::Registry::open_existing(
            native_workspace_admin::Registry::default_path().map_err(fail)?,
        )
        .map_err(fail)?;
        let binding = registry.lookup(loader.workspace_root()).map_err(fail)?;
        crate::commands::ensure_binding(&binding).map_err(fail)?;
        if binding.repo != loader.repo_path() {
            return Err(fail(
                "recovery_required: restored repository binding differs",
            ));
        }
        let (retained, projection) =
            jj_vfs::native::read_retained_state_for_capture(&binding.projection(), 1024 * 1024)
                .map_err(fail)?;
        let heads =
            jj_lib::simple_op_heads_store::SimpleOpHeadsStore::load(&binding.repo.join("op_heads"))
                .get_op_heads_bounded(2)
                .map_err(fail)?;
        Ok(RestoredPreflight {
            binding,
            retained,
            projection,
            heads,
        })
    }
    fn restored_preflight(command: &CommandHelper) -> Result<RestoredPreflight, CommandError> {
        let state = inspect_restored(command)?;
        if state.retained.pending || state.heads.as_slice() != [state.retained.operation.clone()] {
            return Err(fail(
                "recovery_required: restored landing requires one current operation matching the settled working copy; state preserved",
            ));
        }
        Ok(state)
    }
    async fn landing_helper(
        ui: &Ui,
        command: &CommandHelper,
        restored: bool,
        snapshot: bool,
    ) -> Result<jj_cli::cli_util::WorkspaceCommandHelper, CommandError> {
        if !restored {
            return command.workspace_helper(ui).await;
        }
        // The existing native command fence excludes other CLI writers. Read
        // the actual marker-verified WC and bounded heads BEFORE helper loading:
        // even no_snapshot would otherwise merge plural heads automatically.
        let before = restored_preflight(command)?;
        let mut helper = command.workspace_helper_no_snapshot(ui).await?;
        if helper.repo().op_id() != &before.retained.operation
            || helper.workspace_name().as_str() != before.retained.workspace.as_str()
        {
            return Err(fail(
                "recovery_required: restored loaded operation/workspace differs; state preserved",
            ));
        }
        let wc_id = helper
            .get_wc_commit_id()
            .ok_or_else(|| fail("recovery_required: restored working-copy commit missing"))?;
        let wc = helper.repo().store().get_commit_async(wc_id).await?;
        if jj_vfs::native::NativeTree::from_merged_tree(&wc.tree()).map_err(fail)?
            != before.projection.tree
        {
            return Err(fail(
                "recovery_required: restored operation/projection differs; state preserved",
            ));
        }
        let after = restored_preflight(command)?;
        if after.binding != before.binding
            || after.retained.persisted_bytes != before.retained.persisted_bytes
            || after.projection != before.projection
        {
            return Err(fail(
                "recovery_required: restored working copy changed during loading; state preserved",
            ));
        }
        // Unlike workspace_helper(), this explicit snapshot has NO configured
        // auto-update-stale fallback. Dirty editor changes are snapshotted by
        // the native fenced workflow; stale state is never checked out for us.
        if snapshot {
            helper.maybe_snapshot(ui).await?;
        }
        Ok(helper)
    }
    const RECOVERY_WORK_CAP: usize = 1024 * 1024 * 1024;
    type RecoveryOwner = std::sync::Arc<dyn std::any::Any + Send + Sync>;
    async fn reserve_recovery_work(
        required: usize,
        deadline: tokio::time::Instant,
    ) -> Result<RecoveryOwner, CommandError> {
        static POOL: std::sync::OnceLock<std::sync::Arc<tokio::sync::Semaphore>> =
            std::sync::OnceLock::new();
        if required == 0 || required > RECOVERY_WORK_CAP {
            return Err(fail(
                "recovery_required: aggregate recovery work exceeds the fixed pool",
            ));
        }
        let pool = POOL
            .get_or_init(|| std::sync::Arc::new(tokio::sync::Semaphore::new(RECOVERY_WORK_CAP)))
            .clone();
        let permit = tokio::time::timeout_at(deadline, pool.acquire_many_owned(required as u32))
            .await
            .map_err(|_| fail("recovery_required: recovery work admission deadline"))?
            .map_err(fail)?;
        Ok(std::sync::Arc::new(permit))
    }
    async fn live_provider_identity(
        locator: &broker::Locator,
        repo: &std::path::Path,
        deadline: tokio::time::Instant,
    ) -> Result<[u8; 32], CommandError> {
        let token = crate::commands::token(repo)
            .map_err(fail)?
            .ok_or_else(|| fail("native command fence required"))?;
        let query = native_broker::BrokerQueryClient::connect(
            &locator.socket,
            &locator.store,
            Default::default(),
        )
        .await
        .map_err(fail)?
        .with_command_token(token)
        .with_operation_deadline(deadline);
        Ok(query.describe_provider().await.map_err(fail)?.identity)
    }
    async fn selected_index_acknowledgement(
        command: &CommandHelper,
        operation: [u8; 64],
        candidate: [u8; 32],
    ) -> Result<bool, CommandError> {
        let loader = command.workspace_loader()?;
        let locator = broker::locator(&loader.repo_path().join("store")).map_err(fail)?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let identity = live_provider_identity(&locator, loader.repo_path(), deadline).await?;
        let limits = jj_native_index::Limits::default();
        let required =
            jj_native_index::LocalIndexExportReader::required_read_bytes(&limits).map_err(fail)?;
        let owner = reserve_recovery_work(required, deadline).await?;
        let path = loader.repo_path().join("index");
        let config = jj_native_index::Config {
            endpoint: jj_native_index::Endpoint::UnixSocket(locator.socket),
            store_alias: locator.store,
            provider_fingerprint: identity,
        };
        let read = tokio::task::spawn_blocking(move || {
            // Actual reservation follows the physical reader after waiter expiry.
            let _owner = owner;
            let reader = jj_native_index::LocalIndexExportReader::open_existing(
                &path, &config, limits, required,
            )?;
            let bounds = jj_native_index::PortableExportLimits::default();
            reader.operation_explicitly_retains(
                &operation,
                tinytree_model::CommitId(candidate),
                bounds.max_source_reads,
                bounds.max_source_bytes,
            )
        });
        tokio::time::timeout_at(deadline, read)
            .await
            .map_err(|_| fail("recovery_required: selected index proof deadline"))?
            .map_err(fail)?
            .map_err(fail)
    }
    async fn resolution_call<T>(
        deadline: tokio::time::Instant,
        operation: impl std::future::Future<Output = anyhow::Result<T>>,
    ) -> Result<T, CommandError> {
        // Bounds waiting and admission of new work, not preemption of a
        // synchronous filesystem write/fsync. Accepted journal mutation stays
        // under this command's fence until the poll returns.
        tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_| fail("recovery_required: original host resolution deadline exceeded"))?
            .map_err(|error| fail(format!("recovery_required: {error}")))
    }
    struct RecoverySession {
        registry: std::sync::Arc<native_workspace_admin::Registry>,
        binding: native_workspace_admin::Binding,
        context: lr::Context,
        admission: std::sync::Arc<lr::ReadAdmission>,
        owner: RecoveryOwner,
        io: native_broker::BrokerObjectIo,
        deadline: tokio::time::Instant,
        proofs: Vec<lr::CommitProof>,
        journal: Option<lr::Journal>,
    }
    impl RecoverySession {
        async fn open(
            command: &CommandHelper,
            p: &LandingPending,
            binding: native_workspace_admin::Binding,
            purpose: lr::Purpose,
        ) -> Result<Self, CommandError> {
            Self::open_until(
                command,
                p,
                binding,
                purpose,
                tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            )
            .await
        }
        async fn open_until(
            command: &CommandHelper,
            p: &LandingPending,
            binding: native_workspace_admin::Binding,
            purpose: lr::Purpose,
            deadline: tokio::time::Instant,
        ) -> Result<Self, CommandError> {
            recovery_deadline(deadline)?;
            let locator = broker::locator(&binding.repo.join("store")).map_err(fail)?;
            let identity = live_provider_identity(&locator, &binding.repo, deadline).await?;
            let limits = lr::WorkingLimits::default();
            let required = limits.required_read_bytes().map_err(fail)?;
            // This reserves EXACT derived logical codec/allocation work, not a
            // 566MiB eager allocation or an RSS estimate. Physical owners keep
            // the permit through provider and registry work after cancellation.
            let owner = reserve_recovery_work(required, deadline).await?;
            let admission =
                lr::ReadAdmission::new(limits, required, owner.clone()).map_err(fail)?;
            let token = crate::commands::token(&binding.repo)
                .map_err(fail)?
                .ok_or_else(|| fail("native command fence required"))?;
            let io = native_broker::BrokerObjectIo::connect(
                &locator.socket,
                &locator.store,
                binding.repo.join("store/native-broker-spool"),
                Default::default(),
            )
            .await
            .map_err(fail)?
            .with_command_token(token)
            .with_operation_deadline(deadline);
            let context = lr::Context {
                provider_identity: identity,
                scope: native_workspace_admin::remote_state::RemoteScope::new(
                    locator.socket,
                    locator.store,
                )
                .map_err(fail)?,
                request_id: p.request_id.clone(),
                request_digest: resolution_request_digest(p)?,
                purpose,
            };
            let registry = native_workspace_admin::Registry::open_existing(
                native_workspace_admin::Registry::default_path().map_err(fail)?,
            )
            .map_err(fail)?;
            let _ = command; // The actual admitted command is checked above.
            Ok(Self {
                registry,
                binding,
                context,
                admission,
                owner,
                io,
                deadline,
                proofs: Vec::new(),
                journal: None,
            })
        }
        async fn set_proofs(&mut self, ids: &[[u8; 32]]) -> Result<(), CommandError> {
            use jj_native_backend::NativeObjectIo as _;
            self.proofs.clear();
            if ids.len() > 2 {
                return Err(fail(
                    "recovery_required: too many local journal proof selectors",
                ));
            }
            for id in ids {
                if self.proofs.iter().any(|proof| proof.id() == *id) {
                    continue;
                }
                let proof = if *id == [0; 32] {
                    lr::CommitProof::root(&self.admission).map_err(fail)?
                } else {
                    let metadata = self
                        .io
                        .read_metadata_with_owner(
                            tinytree_model::ObjectKey {
                                kind: tinytree_model::Kind::Commit,
                                id: tinytree_model::ObjectId(*id),
                            },
                            self.owner.clone(),
                        )
                        .await
                        .map_err(fail)?;
                    lr::CommitProof::from_metadata(&metadata, &self.admission).map_err(fail)?
                };
                self.proofs.push(proof);
            }
            Ok(())
        }
        async fn load(&mut self) -> Result<Option<lr::Journal>, CommandError> {
            // Selectors come only from the current WC and this LOCAL host record,
            // never captured apply/fork fields or a scan for matching OP tags.
            let ids = resolution_call(
                self.deadline,
                self.registry
                    .resolution_proof_ids(&self.binding, &self.context, &self.admission),
            )
            .await?;
            self.set_proofs(ids.ids()).await?;
            let journal = resolution_call(
                self.deadline,
                self.registry.load_landing_resolution(
                    &self.binding,
                    &self.context,
                    &self.proofs,
                    &self.admission,
                ),
            )
            .await?;
            self.journal = journal.clone();
            Ok(journal)
        }
        async fn presence(&self) -> Result<lr::Presence, CommandError> {
            let common = lr::CommonContext {
                provider_identity: self.context.provider_identity,
                scope: self.context.scope.clone(),
                request_id: self.context.request_id.clone(),
                request_digest: self.context.request_digest,
            };
            resolution_call(
                self.deadline,
                self.registry
                    .landing_resolution_presence(&self.binding, &common, &self.admission),
            )
            .await
        }
        async fn select_freeze(
            &mut self,
            fresh_job: Option<&[u8]>,
        ) -> Result<Option<lr::Stage>, CommandError> {
            let common = lr::CommonContext {
                provider_identity: self.context.provider_identity,
                scope: self.context.scope.clone(),
                request_id: self.context.request_id.clone(),
                request_digest: self.context.request_digest,
            };
            let selected = resolution_call(
                self.deadline,
                self.registry.freeze_landing_resolution_selection(
                    &self.binding,
                    &common,
                    &self.admission,
                ),
            )
            .await?;
            if let Some(selected) = selected {
                let observed = selected.observed_job();
                if observed
                    .as_ref()
                    .is_some_and(|id| Some(id.as_slice()) != fresh_job)
                {
                    return Err(fail(
                        "recovery_required: local freeze observed a different original job",
                    ));
                }
                self.context.purpose = lr::Purpose::Freeze {
                    observed_job: observed.as_ref().map(|id| hex(id)),
                };
                Ok(Some(selected.stage()))
            } else {
                Ok(None)
            }
        }
        async fn prepared_nonce(&self) -> Result<[u8; 32], CommandError> {
            Ok(resolution_call(
                self.deadline,
                self.registry.prepared_landing_resolution_nonce(
                    &self.binding,
                    &self.context,
                    &self.admission,
                ),
            )
            .await?
            .nonce())
        }
        async fn begin(
            &mut self,
            prior_nonce: Option<[u8; 32]>,
        ) -> Result<lr::Journal, CommandError> {
            if let Some(prior_nonce) = prior_nonce {
                // Generic native snapshot can advance the projection generation
                // even when no commit changes. Only a strictly Prepared local
                // record may be archived/replaced, AFTER fresh CURRENT proof.
                let ids = resolution_call(
                    self.deadline,
                    self.registry.resolution_proof_ids(
                        &self.binding,
                        &self.context,
                        &self.admission,
                    ),
                )
                .await?;
                self.set_proofs(ids.ids()).await?;
                let base = resolution_call(
                    self.deadline,
                    self.registry.landing_resolution_base(
                        &self.binding,
                        &self.proofs,
                        &self.admission,
                    ),
                )
                .await?;
                let journal = resolution_call(
                    self.deadline,
                    self.registry.replace_prepared_landing_resolution(
                        &self.binding,
                        &self.context,
                        prior_nonce,
                        &base,
                        &self.proofs,
                        &self.admission,
                    ),
                )
                .await?;
                self.journal = Some(journal.clone());
                return Ok(journal);
            }
            if let Some(journal) = self.load().await? {
                return Ok(journal);
            }
            let base = resolution_call(
                self.deadline,
                self.registry
                    .landing_resolution_base(&self.binding, &self.proofs, &self.admission),
            )
            .await?;
            let journal = resolution_call(
                self.deadline,
                self.registry.begin_landing_resolution(
                    &self.binding,
                    &self.context,
                    &base,
                    &self.proofs,
                    &self.admission,
                ),
            )
            .await?;
            self.journal = Some(journal.clone());
            Ok(journal)
        }
        async fn record(
            &mut self,
            operation: &OperationId,
            result: &CommitId,
        ) -> Result<(), CommandError> {
            let journal = self
                .journal
                .as_ref()
                .ok_or_else(|| fail("host resolution journal missing"))?;
            let nonce = journal.nonce();
            let base = bytes::<32>(journal.base().working_copy_commit())?;
            self.set_proofs(&[base, result.as_bytes().try_into().map_err(fail)?])
                .await?;
            let journal = resolution_call(
                self.deadline,
                self.registry.record_landing_resolution_operation(
                    &self.binding,
                    &self.context,
                    nonce,
                    &operation.hex(),
                    &self.proofs,
                    &self.admission,
                ),
            )
            .await?;
            self.journal = Some(journal);
            Ok(())
        }
        async fn complete(&mut self) -> Result<(), CommandError> {
            let nonce = self
                .journal
                .as_ref()
                .ok_or_else(|| fail("host resolution journal missing"))?
                .nonce();
            let journal = resolution_call(
                self.deadline,
                self.registry.complete_landing_resolution(
                    &self.binding,
                    &self.context,
                    nonce,
                    &self.proofs,
                    &self.admission,
                ),
            )
            .await?;
            self.journal = Some(journal);
            Ok(())
        }
        fn check_deadline(&self) -> Result<(), CommandError> {
            recovery_deadline(self.deadline)
        }
        fn stamp(&self, tx: &mut jj_lib::transaction::Transaction) -> Result<(), CommandError> {
            self.check_deadline()?;
            let journal = self
                .journal
                .as_ref()
                .ok_or_else(|| fail("host resolution journal missing"))?;
            for (key, value) in journal.operation_attributes() {
                tx.set_attribute(key, value);
            }
            Ok(())
        }
    }
    fn resolution_request_digest(p: &LandingPending) -> Result<[u8; 32], CommandError> {
        lr::request_digest(&wire(p)?).map_err(fail)
    }
    fn resolution_candidate_digest(candidate: &l::Candidate) -> [u8; 32] {
        lr::candidate_digest(candidate)
    }
    async fn cleanup_closed_landing(
        command: &CommandHelper,
        locked: &LockedState,
        job: &l::LandingJob,
    ) -> Result<(), CommandError> {
        // Call only after fresh committed/current or nonapplicable-terminal
        // classification and its final durable save, never from captured flags.
        // Canonical models and pending-recovery phases must already be dropped.
        let binding = restored_preflight(command)?.binding;
        let pending = locked.state.landing.as_ref().unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        // Fund the bounded saved-state encoder independently of the registry's
        // cleanup decode/read admission. The successful final save already
        // proved this exact state encodes within the 1MiB sidecar limit.
        let encoding = reserve_recovery_work(34 * 1024 * 1024, deadline).await?;
        let session = RecoverySession::open_until(
            command,
            pending,
            binding,
            lr::Purpose::Freeze {
                observed_job: pending.job_id.clone(),
            },
            deadline,
        )
        .await?;
        let encoded = toml::to_string(&locked.state).map_err(fail)?;
        if encoded.len() > 1024 * 1024 {
            return Err(fail("closed landing sidecar exceeds byte limit"));
        }
        let fingerprint = *blake3::hash(encoded.as_bytes()).as_bytes();
        drop(encoded);
        let common = lr::CommonContext {
            provider_identity: session.context.provider_identity,
            scope: session.context.scope.clone(),
            request_id: session.context.request_id.clone(),
            request_digest: session.context.request_digest,
        };
        fault_point("closed_before_cleanup")?;
        resolution_call(
            deadline,
            session.registry.cleanup_closed_landing_resolution(
                &session.binding,
                &common,
                fingerprint,
                job,
                &session.admission,
            ),
        )
        .await?;
        drop(session);
        drop(encoding);
        Ok(())
    }
    const CANONICAL_RETAINED_WORK: usize = 240 * 1024 * 1024;
    const CANONICAL_READER_WORK: usize = 192 * 1024 * 1024;
    fn canonical_retained_allowance() -> Result<usize, CommandError> {
        #[cfg(feature = "native-landing-fault-injection")]
        if let Some(value) = std::env::var_os("TINYTREE_LANDING_TEST_CANONICAL_WORK") {
            let value = value
                .to_str()
                .ok_or_else(|| fail("invalid test canonical allowance"))?
                .parse::<usize>()
                .map_err(fail)?;
            if value == 0 || value > CANONICAL_RETAINED_WORK {
                return Err(fail(
                    "test canonical allowance may only lower the production bound",
                ));
            }
            return Ok(value);
        }
        Ok(CANONICAL_RETAINED_WORK)
    }
    struct CanonicalCacheOwner {
        store: std::sync::Arc<jj_lib::store::Store>,
        _reservation: RecoveryOwner,
    }
    impl Drop for CanonicalCacheOwner {
        fn drop(&mut self) {
            // Source/result models can be reread by the normal rewrite guard.
            // End those cache references BEFORE releasing retained funding.
            self.store.clear_caches();
        }
    }
    struct CanonicalModels {
        // Field destruction order is intentional: model clones end before the
        // cache owner clears cached copies and releases its physical permit.
        admitted: Vec<jj_lib::commit::Commit>,
        _owner: std::sync::Arc<CanonicalCacheOwner>,
    }
    struct CanonicalApplyPhase {
        // Helpers/repos end before canonical models/cache funding on every exit.
        helper: jj_cli::cli_util::WorkspaceCommandHelper,
        canonical: CanonicalLanding,
    }
    struct CanonicalReader {
        io: native_broker::BrokerObjectIo,
        owner: std::sync::Arc<CanonicalCacheOwner>,
        scratch: RecoveryOwner,
        seen: std::collections::BTreeMap<[u8; 32], usize>,
        charged: usize,
        allowance: usize,
    }
    impl CanonicalReader {
        async fn open(
            command: &CommandHelper,
            store: std::sync::Arc<jj_lib::store::Store>,
        ) -> Result<Self, CommandError> {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            let allowance = canonical_retained_allowance()?;
            let reservation = reserve_recovery_work(allowance, deadline).await?;
            let scratch = reserve_recovery_work(CANONICAL_READER_WORK, deadline).await?;
            let path = command.workspace_loader()?.repo_path().to_owned();
            let locator = broker::locator(&path.join("store")).map_err(fail)?;
            let token = crate::commands::token(&path)
                .map_err(fail)?
                .ok_or_else(|| fail("native command fence required"))?;
            let io = native_broker::BrokerObjectIo::connect(
                &locator.socket,
                &locator.store,
                path.join("store/native-broker-spool"),
                Default::default(),
            )
            .await
            .map_err(fail)?
            .with_command_token(token)
            .with_operation_deadline(deadline);
            Ok(Self {
                io,
                owner: std::sync::Arc::new(CanonicalCacheOwner {
                    store,
                    _reservation: reservation,
                }),
                scratch,
                seen: Default::default(),
                charged: 0,
                allowance,
            })
        }
        async fn commit(&mut self, id: &CommitId) -> Result<jj_lib::commit::Commit, CommandError> {
            use jj_native_backend::NativeObjectIo as _;
            let native: [u8; 32] = id.as_bytes().try_into().map_err(fail)?;
            let key = tinytree_model::ObjectKey {
                kind: tinytree_model::Kind::Commit,
                id: tinytree_model::ObjectId(native),
            };
            // Both real reservations follow accepted detached provider work on
            // cancellation. Broker's separate RPC pool is not retained funding.
            let physical: RecoveryOwner =
                std::sync::Arc::new((self.owner.clone(), self.scratch.clone()));
            let metadata = self
                .io
                .read_metadata_with_owner(key, physical)
                .await
                .map_err(fail)?;
            if metadata.descriptor().key != key {
                return Err(fail("canonical metadata identity differs"));
            }
            let size = metadata.bytes().len();
            if let Some(previous) = self.seen.get(&native) {
                if *previous != size {
                    return Err(fail("canonical metadata size changed"));
                }
            } else {
                // Two conservative 32x model allowances cover uncached models
                // and later stock-cache copies for BOTH source/result IDs.
                let charge = size
                    .checked_mul(64)
                    .and_then(|n| n.checked_add(8192))
                    .ok_or_else(|| fail("canonical aggregate budget overflow"))?;
                self.charged = self.charged.checked_add(charge).filter(|n| *n <= self.allowance)
                    .ok_or_else(|| fail("recovery_required: canonical source/result aggregate work budget exceeded; current state preserved"))?;
                self.seen.insert(native, size);
            }
            // Consume the verified model through the shared backend mapping;
            // never populate jj Store's entry-count cache during this loop.
            let data = metadata.into_backend_commit().map_err(fail)?;
            Ok(jj_lib::commit::Commit::new(
                self.owner.store.clone(),
                id.clone(),
                std::sync::Arc::new(data),
            ))
        }
    }
    struct CanonicalLanding {
        head: native_broker::CommitId,
        parent: CommitId,
        pairs: Vec<(CommitId, CommitId)>,
        source_changes: Vec<jj_lib::backend::ChangeId>,
        models: CanonicalModels,
    }
    async fn verify_candidate(
        command: &CommandHelper,
        repo: &std::sync::Arc<jj_lib::repo::ReadonlyRepo>,
        p: &LandingPending,
        client: &BrokerControlClient,
        report: &LandingReport,
    ) -> Result<CanonicalLanding, CommandError> {
        let candidate = report
            .job()
            .candidate
            .as_ref()
            .ok_or_else(|| fail("landing candidate missing"))?;
        if candidate.rewrites.len() > p.source_stack.len()
            || candidate.rewrites.iter().any(|row| {
                !p.source_stack
                    .iter()
                    .any(|old| old == &hex(&row.old_commit))
            })
        {
            return Err(fail(
                "canonical landing rewrites exceed the original source stack",
            ));
        }
        let head = native_broker::CommitId(candidate.head.as_slice().try_into().map_err(fail)?);
        // A receipt's historical hold flag is not availability. Refresh complete
        // admission NOW, before index or canonical payload access.
        client.admit_commit(head).await.map_err(fail)?;
        let mut reader = CanonicalReader::open(command, repo.store().clone()).await?;
        let mut parent = id(&p.expected_head)?;
        let mut pairs = Vec::new();
        let mut source_changes = Vec::with_capacity(p.source_stack.len());
        // The original source stack is bounded at 128. Preserve the exact
        // admitted, fully checked canonical chain for a frozen local overlay;
        // never merge a moving public-root index into a restored checkpoint.
        let mut admitted = Vec::with_capacity(p.source_stack.len());
        for old in &p.source_stack {
            let old_id = id(old)?;
            let old_commit = reader.commit(&old_id).await?;
            let row = candidate
                .rewrites
                .iter()
                .find(|r| r.old_commit == old_id.as_bytes());
            let new_id = row
                .map(|r| CommitId::from_bytes(&r.new_commit))
                .unwrap_or_else(|| old_id.clone());
            let new = if new_id == old_id {
                old_commit.clone()
            } else {
                reader.commit(&new_id).await?
            };
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
            source_changes.push(old_commit.change_id().clone());
            admitted.push(new);
            parent = new_id;
        }
        if parent.as_bytes() != candidate.head
            || admitted
                .last()
                .ok_or_else(|| fail("canonical stack is empty"))?
                .tree_ids()
                .as_resolved()
                .map(|id| id.as_bytes())
                != Some(candidate.root_tree.as_slice())
        {
            return Err(fail(
                "canonical landing head/root differs from the checked candidate",
            ));
        }
        Ok(CanonicalLanding {
            head,
            parent,
            pairs,
            source_changes,
            models: CanonicalModels {
                admitted,
                _owner: reader.owner.clone(),
            },
        })
    }
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum CurrentSources {
        Old,
        New,
    }
    async fn classify_current_sources(
        repo: &std::sync::Arc<jj_lib::repo::ReadonlyRepo>,
        p: &LandingPending,
        canonical: &CanonicalLanding,
    ) -> Result<CurrentSources, CommandError> {
        let mut old = false;
        let mut new = false;
        for ((source, result), change) in p
            .source_stack
            .iter()
            .zip(&canonical.models.admitted)
            .zip(&canonical.source_changes)
        {
            let source_id = id(source)?;
            let current = repo
                .resolve_change_id(change)
                .await
                .map_err(fail)?
                .ok_or_else(|| {
                    fail("reconciliation_required: original source is no longer visible")
                })?;
            let mut visible = current.visible_with_offsets().map(|(_, id)| id);
            let actual = visible
                .next()
                .ok_or_else(|| fail("reconciliation_required: source is not visible"))?;
            if visible.next().is_some() {
                return Err(fail(
                    "reconciliation_required: source has ambiguous visible versions; state preserved",
                ));
            }
            if source_id == *result.id() {
                if actual != &source_id {
                    return Err(fail(
                        "reconciliation_required: identity source was amended; state preserved",
                    ));
                }
            } else if actual == &source_id {
                old = true;
            } else if actual == result.id() {
                new = true;
            } else {
                return Err(fail(
                    "reconciliation_required: source was independently amended; state preserved",
                ));
            }
        }
        if old && new {
            return Err(fail(
                "reconciliation_required: source stack mixes old and landed versions; state preserved",
            ));
        }
        Ok(if new {
            CurrentSources::New
        } else {
            CurrentSources::Old
        })
    }
    // A pinned recovery operation retains two OP models and its VIEW; load_at
    // clones that VIEW. Fund the four maximum models, wire buffers and two WC
    // state/freshness models BEFORE any external OP decode.
    const PINNED_REPO_WORK: usize =
        4 * 128 * 1024 * 1024 + 2 * 4 * 1024 * 1024 + 2 * (32 * 1024 * 1024 + 4096);
    const RECOVERY_COMMIT_WORK: usize = 4 * 1024 * 1024 * 32 + 4 * 1024 * 1024 + 4096;
    // Native recover uses its existing 4MiB WC format cap (not our 1MiB
    // inspection cap), including pending/receipt clones and save scratch.
    const PENDING_VFS_WORK: usize =
        4 * 64 * 1024 * 1024 + 4 * 8 * 1024 * 1024 + 4 * (4 * 32 * 1024 * 1024 + 4096);
    fn recovery_deadline(deadline: tokio::time::Instant) -> Result<(), CommandError> {
        if tokio::time::Instant::now() >= deadline {
            Err(fail(
                "recovery_required: original host resolution deadline exceeded before new work",
            ))
        } else {
            Ok(())
        }
    }
    struct FundedCurrentRepo {
        repo: std::sync::Arc<jj_lib::repo::ReadonlyRepo>,
        _owner: RecoveryOwner,
    }
    async fn readonly_current_repo(
        command: &CommandHelper,
        state: &RestoredPreflight,
    ) -> Result<FundedCurrentRepo, CommandError> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let owner = reserve_recovery_work(PINNED_REPO_WORK, deadline).await?;
        // Loading a Workspace does not resolve or merge operation heads. Read
        // exactly the actual native WC operation through the existing bounded,
        // hash-pinned OP/VIEW reader; no helper or stale-WC fallback is involved.
        let workspace = command.load_workspace()?;
        let loader = workspace.repo_loader();
        let operation = crate::vfs::load_recovery_operation(
            loader.op_store().clone(),
            &state.retained.operation,
            4 * 1024 * 1024,
            128 * 1024 * 1024,
        )
        .map_err(fail)?;
        let repo = loader.load_at(&operation).await.map_err(fail)?;
        Ok(FundedCurrentRepo {
            repo,
            _owner: owner,
        })
    }
    async fn publish_owned_head(
        loader: &jj_lib::repo::RepoLoader,
        repository: &std::path::Path,
        base: &OperationId,
        owned: &OperationId,
        deadline: tokio::time::Instant,
    ) -> Result<(), CommandError> {
        recovery_deadline(deadline)?;
        let heads_store = loader.op_heads_store();
        let simple = heads_store
            .downcast_ref::<jj_lib::simple_op_heads_store::SimpleOpHeadsStore>()
            .ok_or_else(|| fail("recovery_required: unsupported operation head store"))?;
        // The upstream async lock is a synchronous blocking flock. Use its
        // compatible nonblocking primitive, with no detached work or late write.
        let _guard = jj_lib::lock::FileLock::try_lock(repository.join("op_heads/heads/lock"))
            .map_err(fail)?
            .ok_or_else(|| fail("recovery_required: operation head publication is busy"))?;
        // The bounded directory scan counts this guard's `lock` entry too.
        // Only two actual head IDs are admitted below.
        let heads = simple.get_op_heads_bounded(3).map_err(fail)?;
        if heads.is_empty()
            || heads.len() > 2
            || heads.iter().any(|head| head != base && head != owned)
        {
            return Err(fail(
                "recovery_required: actual heads no longer belong to this exact host-owned attempt",
            ));
        }
        if heads != [owned.clone()] {
            recovery_deadline(deadline)?;
            loader
                .op_heads_store()
                .update_op_heads(&heads, owned)
                .await
                .map_err(fail)?;
        }
        Ok(())
    }
    async fn publish_resolution(
        command: &CommandHelper,
        session: &RecoverySession,
        owned: &OperationId,
    ) -> Result<(), CommandError> {
        let journal = session
            .journal
            .as_ref()
            .ok_or_else(|| fail("host publication journal missing"))?;
        if journal.owned_operation() != Some(owned.hex().as_str()) {
            return Err(fail(
                "recovery_required: publication is not the exact recorded host operation",
            ));
        }
        let base = OperationId::try_from_hex(journal.base().operation())
            .ok_or_else(|| fail("invalid host publication baseline"))?;
        let workspace = command.load_workspace()?;
        publish_owned_head(
            workspace.repo_loader(),
            &session.binding.repo,
            &base,
            owned,
            session.deadline,
        )
        .await
    }
    async fn repair_host_owned(
        command: &CommandHelper,
        locked: &mut LockedState,
        session: RecoverySession,
        journal: lr::Journal,
        report: Option<&LandingReport>,
    ) -> Result<bool, CommandError> {
        if journal.stage() == lr::Stage::Prepared {
            // A crash before record may leave an unreferenced nonce-stamped OP.
            // It is NOT searched/replayed. Only the unchanged base may produce
            // a fresh transaction and record exactly one new owned result.
            return Ok(false);
        }
        let p = locked.state.landing.clone().unwrap();
        let owned_text = journal
            .owned_operation()
            .ok_or_else(|| fail("owned resolution operation missing"))?
            .to_owned();
        let owned = OperationId::try_from_hex(&owned_text)
            .ok_or_else(|| fail("invalid owned operation"))?;
        let base = OperationId::try_from_hex(journal.base().operation())
            .ok_or_else(|| fail("invalid owned baseline"))?;
        let result_id = id(journal
            .result_working_copy_commit()
            .ok_or_else(|| fail("owned result commit missing"))?)?;
        let purpose = journal.purpose().clone();
        let nonce = journal.nonce();
        let attributes = journal.operation_attributes();
        let deadline = session.deadline;
        drop(journal);
        drop(session);
        // Only small context/IDs remain. Do not overlap the journal's full
        // aggregate reader with the independently pinned OP/VIEW phase.
        let commit_owner = reserve_recovery_work(RECOVERY_COMMIT_WORK, deadline).await?;
        let pinned_owner = reserve_recovery_work(PINNED_REPO_WORK, deadline).await?;
        recovery_deadline(deadline)?;
        let state = inspect_restored(command)?;
        let workspace = command.load_workspace()?;
        let loader = workspace.repo_loader();
        let operation = crate::vfs::load_recovery_operation(
            loader.op_store().clone(),
            &owned,
            4 * 1024 * 1024,
            128 * 1024 * 1024,
        )
        .map_err(fail)?;
        let attrs = &operation.metadata().attributes;
        let phase = if matches!(purpose, lr::Purpose::Apply { .. }) {
            "apply"
        } else {
            "freeze"
        };
        for (key, value) in [
            ("request", p.request_id.as_str()),
            ("phase", phase),
            ("source", p.source_head.as_str()),
            ("source_operation", p.source_operation.as_str()),
        ] {
            if attrs
                .get(&format!("tinytree.landing.{key}"))
                .map(String::as_str)
                != Some(value)
            {
                return Err(fail(
                    "recovery_required: local owned operation no longer binds the original source",
                ));
            }
        }
        for (key, value) in attributes {
            if attrs.get(&key) != Some(&value) {
                return Err(fail(
                    "recovery_required: local nonce-bound operation attributes differ",
                ));
            }
        }
        let planned = loader.load_at(&operation).await.map_err(fail)?;
        let commit = planned.store().get_commit_async(&result_id).await?;
        drop(planned);
        drop(operation);
        drop(pinned_owner);
        if state.retained.pending {
            let pending_owner = reserve_recovery_work(PENDING_VFS_WORK, deadline).await?;
            recovery_deadline(deadline)?;
            // The supplied OP is LOCAL-journal-owned and freshly revalidated,
            // never a capsule apply/fork hint or a matching archived tag.
            crate::vfs::recover(command, &state.binding.root, Some(owned_text.clone())).await?;
            drop(pending_owner);
        }
        let mut session = RecoverySession::open_until(
            command,
            &p,
            state.binding.clone(),
            purpose.clone(),
            deadline,
        )
        .await?;
        let refreshed = session
            .load()
            .await?
            .ok_or_else(|| fail("owned host journal disappeared"))?;
        if refreshed.nonce() != nonce
            || refreshed.owned_operation() != Some(owned_text.as_str())
            || refreshed.result_working_copy_commit() != Some(result_id.hex().as_str())
        {
            return Err(fail(
                "recovery_required: exact local owned result changed during recovery",
            ));
        }
        drop(refreshed);
        let current = inspect_restored(command)?;
        if current.retained.pending {
            return Err(fail(
                "recovery_required: owned native recovery is still unsettled",
            ));
        }
        if current.retained.operation != owned {
            let refreshed = session
                .load()
                .await?
                .ok_or_else(|| fail("owned host journal disappeared"))?;
            if !matches!(
                refreshed.position(),
                lr::Position::OperationWritten
                    | lr::Position::OperationPublished
                    | lr::Position::RecoveryRequired
            ) {
                return Err(fail(
                    "recovery_required: current state is not the exact owned baseline",
                ));
            }
            let no_checkout = refreshed.base().working_copy_commit() == result_id.hex()
                && refreshed.result_tree() == Some(refreshed.base().tree());
            drop(refreshed);
            let mut workspace = command.load_workspace()?;
            let mut wc = workspace
                .start_working_copy_mutation()
                .await
                .map_err(fail)?;
            if !no_checkout {
                recovery_deadline(deadline)?;
                wc.locked_wc().check_out(&commit).await.map_err(fail)?;
            }
            recovery_deadline(deadline)?;
            publish_owned_head(loader, &state.binding.repo, &base, &owned, deadline).await?;
            recovery_deadline(deadline)?;
            wc.finish(owned.clone()).await.map_err(fail)?;
        } else {
            recovery_deadline(deadline)?;
            publish_owned_head(loader, &state.binding.repo, &base, &owned, deadline).await?;
        }
        drop(commit);
        drop(workspace);
        drop(commit_owner);
        // Re-read proofs/current state after actual physical completion.
        session
            .load()
            .await?
            .ok_or_else(|| fail("owned host journal disappeared"))?;
        session.complete().await?;
        drop(session);
        match purpose {
            lr::Purpose::Freeze { .. } => {
                let saved = locked.state.landing.as_mut().unwrap();
                saved.fork_operation = Some(owned_text);
                saved.fork_acknowledged = true;
                saved.operation_writing = None;
                locked.save().map_err(fail)?;
                Ok(false)
            }
            lr::Purpose::Apply { .. } => {
                let report = report
                    .ok_or_else(|| fail("fresh committed lookup required for owned apply"))?;
                record_job(locked, report.job())?;
                let saved = locked.state.landing.as_mut().unwrap();
                saved.apply_operation = Some(owned_text);
                saved.operation_writing = None;
                saved.acknowledged = true;
                finish_observation(locked)?;
                classify_acknowledged_claims(command, locked, report.job()).await?;
                Ok(true)
            }
        }
    }
    fn nonapplicable_phase(phase: l::Phase) -> bool {
        matches!(
            phase,
            l::Phase::Conflict
                | l::Phase::Unsupported
                | l::Phase::CheckFailed
                | l::Phase::CheckUnavailable
                | l::Phase::StaleTarget
                | l::Phase::Expired
        )
    }
    async fn settle_restored_terminal(
        command: &CommandHelper,
        locked: &mut LockedState,
        report: &LandingReport,
    ) -> Result<l::Phase, CommandError> {
        let p = locked.state.landing.clone().unwrap();
        let phase = verify_original_report(&p, report)?;
        if !nonapplicable_phase(phase) {
            return Err(fail("fresh nonapplicable terminal lookup required"));
        }
        let fresh = native_workspace_admin::remote_state::historical_landing_job(report.job())
            .map_err(fail)?;
        if p.observed.as_ref() == Some(&fresh)
            && !p.restored_lookup_only
            && !p.acknowledged
            && p.terminal.is_some()
            && p.operation_writing.is_none()
            && p.apply_operation.is_none()
            && p.fork_acknowledged
            && p.fork_operation.as_ref() == Some(&p.source_operation)
            && p.candidate_head.is_none()
            && p.result_roots.is_empty()
        {
            // This request was already durably closed. Ordinary later commands
            // may advance CURRENT; do not replay or reclassify a historical
            // Completed freeze against it. Fresh job/current checks plus the
            // cleanup API independently validate the exact saved closure and
            // every Completed/archive record; Prepared/Written still refuse.
            cleanup_closed_landing(command, locked, report.job()).await?;
            return Ok(phase);
        }
        let state = restored_preflight(command)?;
        let mut session = RecoverySession::open(
            command,
            &p,
            state.binding,
            lr::Purpose::Freeze {
                observed_job: p.job_id.clone(),
            },
        )
        .await?;
        let common = lr::CommonContext {
            provider_identity: session.context.provider_identity,
            scope: session.context.scope.clone(),
            request_id: session.context.request_id.clone(),
            request_digest: session.context.request_digest,
        };
        let presence = resolution_call(
            session.deadline,
            session.registry.landing_resolution_presence(
                &session.binding,
                &common,
                &session.admission,
            ),
        )
        .await?;
        // A scalar probe is NOT replay or completion authority. Any local
        // record must pass its full original-purpose canonical proof load.
        if presence.freeze().is_some() {
            session.select_freeze(Some(&report.job().id)).await?;
            let journal = session
                .load()
                .await?
                .ok_or_else(|| fail("local freeze record disappeared"))?;
            if journal.stage() != lr::Stage::Completed
                || journal.position() != lr::Position::Completed
            {
                return Err(fail(
                    "recovery_required: local host-owned freeze is unfinished; terminal source and work preserved",
                ));
            }
        }
        if presence.apply().is_some() {
            let candidate = report.job().candidate.as_ref().ok_or_else(|| {
                fail("recovery_required: local apply record needs its original candidate context")
            })?;
            session.context.purpose = lr::Purpose::Apply {
                job_id: hex(&report.job().id),
                candidate_head: hex(&candidate.head),
                candidate_digest: resolution_candidate_digest(candidate),
            };
            let journal = session
                .load()
                .await?
                .ok_or_else(|| fail("local apply record disappeared"))?;
            if journal.stage() != lr::Stage::Completed
                || journal.position() != lr::Position::Completed
            {
                return Err(fail(
                    "recovery_required: local host-owned apply is unfinished; terminal source and work preserved",
                ));
            }
        }
        drop(session);
        // Fresh authenticated nonapplicability ends obsolete automatic replay
        // rights. No old hint is applied or acknowledged; all raw OP/VIEW/index
        // files remain in the retained inventory for the next portable capture.
        let saved = locked.state.landing.as_mut().unwrap();
        saved.apply_operation = None;
        saved.operation_writing = None;
        saved.fork_operation = Some(saved.source_operation.clone());
        saved.fork_acknowledged = true;
        saved.acknowledged = false;
        saved.restored_lookup_only = false;
        let phase = record_job(locked, report.job())?;
        cleanup_closed_landing(command, locked, report.job()).await?;
        Ok(phase)
    }
    async fn settle_restored_freeze(
        ui: &Ui,
        command: &CommandHelper,
        locked: &mut LockedState,
        client: &BrokerControlClient,
        fresh_job: Option<&[u8]>,
    ) -> Result<(), CommandError> {
        let p = locked.state.landing.clone().unwrap();
        client
            .admit_commit(native_broker::CommitId(bytes::<32>(&p.source_head)?))
            .await
            .map_err(fail)?;
        let state = inspect_restored(command)?;
        let repo = readonly_current_repo(command, &state).await?;
        verify_current_original_sources(command, &repo.repo, &p).await?;
        let current = repo
            .repo
            .view()
            .get_wc_commit_id(&state.retained.workspace)
            .cloned()
            .ok_or_else(|| fail("current working copy missing"))?;
        drop(repo);
        let mut session = RecoverySession::open(
            command,
            &p,
            state.binding.clone(),
            lr::Purpose::Freeze {
                observed_job: fresh_job.map(hex),
            },
        )
        .await?;
        let freeze_stage = session.select_freeze(fresh_job).await?;
        let observed_job = match &session.context.purpose {
            lr::Purpose::Freeze { observed_job } => observed_job.clone(),
            _ => unreachable!(),
        };
        let prepared_nonce = if freeze_stage == Some(lr::Stage::Prepared) {
            Some(session.prepared_nonce().await?)
        } else {
            None
        };
        if freeze_stage == Some(lr::Stage::Written) {
            let journal = session
                .load()
                .await?
                .ok_or_else(|| fail("local owned freeze disappeared"))?;
            repair_host_owned(command, locked, session, journal, None).await?;
            return Ok(());
        }
        // CURRENT already outside the submitted stack needs no physical fork.
        // Retained raw OP files/index associations remain untouched and capture
        // still inventories them all; this is not replay of the old fork hint.
        if !state.retained.pending && state.heads.as_slice() == [state.retained.operation.clone()] {
            if !p.source_stack.iter().any(|source| source == &current.hex()) {
                if let Some(nonce) = prepared_nonce {
                    resolution_call(
                        session.deadline,
                        session.registry.retire_prepared_freeze_resolution(
                            &session.binding,
                            &session.context,
                            nonce,
                            &session.admission,
                        ),
                    )
                    .await?;
                }
                if !p.fork_acknowledged {
                    let saved = locked.state.landing.as_mut().unwrap();
                    saved.fork_operation = Some(saved.source_operation.clone());
                    saved.fork_acknowledged = true;
                    if saved.operation_writing.as_deref() == Some("freeze") {
                        saved.operation_writing = None;
                    }
                    return locked.save().map_err(fail);
                }
                return Ok(());
            }
        }
        // A completed owned freeze is never reinterpreted as a new attempt.
        if freeze_stage == Some(lr::Stage::Completed) {
            return Err(fail(
                "recovery_required: current source conflicts with a completed local freeze",
            ));
        }
        drop(session);
        restored_preflight(command)?;
        let mut helper = landing_helper(ui, command, true, false).await?;
        verify_current_original_sources(command, helper.repo(), &p).await?;
        if helper.repo().op_id().hex() != p.source_operation {
            return Err(fail(
                "reconciliation_required: source operation advanced before a fresh freeze; state preserved",
            ));
        }
        // Normal WC maintenance is permitted only AFTER CURRENT proof. It is
        // not a landing transaction and gives no nonce/replay authority. Take
        // the fresh host baseline only after rechecking any resulting snapshot.
        helper.maybe_snapshot(ui).await?;
        verify_current_original_sources(command, helper.repo(), &p).await?;
        client
            .admit_commit(native_broker::CommitId(bytes::<32>(&p.source_head)?))
            .await
            .map_err(fail)?;
        fresh_restored_freeze(command, locked, helper, prepared_nonce, observed_job).await
    }
    async fn resume_restored_committed(
        ui: &Ui,
        command: &CommandHelper,
        locked: &mut LockedState,
        client: &BrokerControlClient,
        report: &LandingReport,
    ) -> Result<(), CommandError> {
        let p = locked.state.landing.clone().unwrap();
        let state = inspect_restored(command)?;
        let repo = readonly_current_repo(command, &state).await?;
        let canonical = verify_candidate(command, &repo.repo, &p, client, report).await?;
        drop(canonical);
        drop(repo);
        let candidate = report
            .job()
            .candidate
            .as_ref()
            .ok_or_else(|| fail("committed candidate missing"))?;
        let mut session = RecoverySession::open(
            command,
            &p,
            state.binding,
            lr::Purpose::Apply {
                job_id: hex(&report.job().id),
                candidate_head: hex(&candidate.head),
                candidate_digest: resolution_candidate_digest(candidate),
            },
        )
        .await?;
        let presence = session.presence().await?;
        let prepared_nonce = if presence.apply() == Some(lr::Stage::Prepared) {
            Some(session.prepared_nonce().await?)
        } else {
            None
        };
        let journal = if prepared_nonce.is_none() {
            session.load().await?
        } else {
            None
        };
        if let Some(journal) = journal.filter(|value| value.stage() != lr::Stage::Prepared) {
            if repair_host_owned(command, locked, session, journal, Some(report)).await? {
                return Ok(());
            }
        } else {
            drop(session);
        }
        // A caller-held journal never substitutes for strict CURRENT preflight.
        // Captured hints are not queried for orphan matches on any path.
        restored_preflight(command)?;
        let helper = landing_helper(ui, command, true, false).await?;
        let canonical = verify_candidate(command, helper.repo(), &p, client, report).await?;
        match classify_current_sources(helper.repo(), &p, &canonical).await? {
            CurrentSources::New => {
                let current = helper
                    .get_wc_commit_id()
                    .ok_or_else(|| fail("current working copy missing"))?;
                if p.source_stack.iter().any(|source| source == &current.hex()) {
                    return Err(fail(
                        "recovery_required: current WC still names the submitted stack",
                    ));
                }
                // Fresh committed/all-NEW proof permits no-fork bookkeeping,
                // including a capsule whose original job had not been observed.
                if !p.fork_acknowledged {
                    let saved = locked.state.landing.as_mut().unwrap();
                    saved.fork_operation = Some(saved.source_operation.clone());
                    saved.fork_acknowledged = true;
                    saved.operation_writing = None;
                }
                record_job(locked, report.job())?;
                acknowledge_current(command, locked, report, helper, canonical, prepared_nonce)
                    .await
            }
            CurrentSources::Old => {
                drop(canonical);
                drop(helper);
                settle_restored_freeze(ui, command, locked, client, Some(&report.job().id)).await?;
                let mut helper = landing_helper(ui, command, true, false).await?;
                let canonical =
                    verify_candidate(command, helper.repo(), &p, client, report).await?;
                if classify_current_sources(helper.repo(), &p, &canonical).await?
                    != CurrentSources::Old
                {
                    return Err(fail(
                        "reconciliation_required: source changed before snapshot; state preserved",
                    ));
                }
                drop(canonical);
                helper.maybe_snapshot(ui).await?;
                // Re-admit after snapshot and compare the resulting CURRENT
                // source versions before creating a nonce or landing transaction.
                let canonical =
                    verify_candidate(command, helper.repo(), &p, client, report).await?;
                if classify_current_sources(helper.repo(), &p, &canonical).await?
                    != CurrentSources::Old
                {
                    return Err(fail(
                        "reconciliation_required: snapshot amended submitted source; private work preserved",
                    ));
                }
                record_job(locked, report.job())?;
                apply_with_helper(
                    command,
                    locked,
                    report,
                    helper,
                    canonical,
                    true,
                    prepared_nonce,
                )
                .await
            }
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
        let helper = landing_helper(ui, command, p.restored_lookup_only, true).await?;
        let repo = helper.repo().clone();
        let canonical = verify_candidate(command, &repo, &p, client, report).await?;
        // Explicit amendments/abandonment of frozen sources are divergence, not
        // permission to relabel the newer work as tested.
        for (old, change) in p.source_stack.iter().zip(&canonical.source_changes) {
            let old_id = id(old)?;
            let current = repo
                .resolve_change_id(change)
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
        drop(repo);
        apply_with_helper(command, locked, report, helper, canonical, false, None).await
    }
    async fn apply_with_helper(
        command: &CommandHelper,
        locked: &mut LockedState,
        report: &LandingReport,
        helper: jj_cli::cli_util::WorkspaceCommandHelper,
        canonical: CanonicalLanding,
        journalled: bool,
        prior_prepared_nonce: Option<[u8; 32]>,
    ) -> Result<(), CommandError> {
        let mut phase = CanonicalApplyPhase { helper, canonical };
        let p = locked.state.landing.clone().unwrap();
        let job = report.job();
        let receipt = job
            .receipt
            .as_ref()
            .ok_or_else(|| fail("committed receipt missing"))?;
        let repo = phase.helper.repo().clone();
        let workspace = phase.helper.workspace_name().to_owned();
        let head = phase.canonical.head;
        let parent = phase.canonical.parent.clone();
        let pairs = phase.canonical.pairs.clone();
        let old_index = repo
            .readonly_index()
            .downcast_ref::<jj_native_index::NativeReadonlyIndex>()
            .ok_or_else(|| fail("native landing requires the federated lazy index"))?;
        // Both routes run only after fresh admission and complete canonical
        // validation. Frozen capture pins stay frozen: only the exact linear
        // COMMIT stack becomes a durable local index delta, with no FILE grant.
        let prepared = if old_index.cloud_query_source().is_some() {
            old_index
                .with_admitted_commit_stack(&phase.canonical.models.admitted)
                .await
                .map_err(fail)?
        } else {
            old_index
                .with_remote_roots(vec![head])
                .await
                .map_err(fail)?
        };
        phase.canonical.models.admitted.clear();
        // Keep phase.canonical.models._owner until all transaction/cache references below end.
        // Generic noncanonical descendant-rebase cache work is the preexisting
        // normal apply path, not charged as canonical source/result metadata.
        if journalled {
            for (_, destination) in &pairs {
                if repo.index().has_id(destination).await.map_err(fail)? {
                    return Err(fail(
                        "reconciliation_required: rewrite destination is already recorded in CURRENT; work preserved",
                    ));
                }
            }
        }
        let mut recovery = if journalled {
            let binding = restored_preflight(command)?.binding;
            let candidate = job
                .candidate
                .as_ref()
                .ok_or_else(|| fail("committed candidate missing"))?;
            let mut session = RecoverySession::open(
                command,
                &p,
                binding,
                lr::Purpose::Apply {
                    job_id: hex(&job.id),
                    candidate_head: hex(&candidate.head),
                    candidate_digest: resolution_candidate_digest(candidate),
                },
            )
            .await?;
            let journal = session.begin(prior_prepared_nonce).await?;
            if journal.stage() != lr::Stage::Prepared || journal.position() != lr::Position::AtBase
            {
                return Err(fail(
                    "recovery_required: existing owned operation must be reconciled before a new apply",
                ));
            }
            drop(journal);
            Some(session)
        } else {
            None
        };
        let mut tx =
            jj_cli::cli_util::start_repo_transaction(&repo, &workspace, command.string_args());
        stamp(&mut tx, &p, "apply");
        if let Some(session) = &recovery {
            session.stamp(&mut tx)?;
        }
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
        if recovery.is_none() {
            locked.state.landing.as_mut().unwrap().operation_writing = Some("apply".into());
            locked.save().map_err(fail)?;
        }
        if let Some(session) = &recovery {
            session.check_deadline()?;
        }
        let unpublished = tx
            .write("apply exact checked native landing and rebase local descendants")
            .await
            .map_err(fail)?;
        let op = unpublished.operation().id().clone();
        fault_point_receipt("apply_written", Some(&op.hex()))?;
        if let Some(session) = &mut recovery {
            session.record(&op, &new_wc).await?;
            fault_point_receipt("resolution_recorded", Some(&op.hex()))?;
        }
        locked.state.landing.as_mut().unwrap().acknowledged = false;
        locked.state.landing.as_mut().unwrap().apply_operation = Some(op.hex());
        locked.state.landing.as_mut().unwrap().operation_writing = None;
        locked.save().map_err(fail)?;
        fault_point("apply_saved")?;
        if let Some(session) = &recovery {
            session.check_deadline()?;
        }
        let (mut wc, _) = phase.helper.start_working_copy_mutation().await?;
        // Native checkout validates clean source-state/version before changing the
        // projection. Concurrent raw edits produce a retained rejection, not reset.
        wc.locked_wc().check_out(&commit).await.map_err(fail)?;
        fault_point("checkout_applied")?;
        if let Some(session) = &recovery {
            session.check_deadline()?;
        }
        if let Some(session) = &recovery {
            publish_resolution(command, session, &op).await?;
            drop(unpublished);
            session.check_deadline()?;
        } else {
            unpublished.publish().await.map_err(fail)?;
        }
        wc.finish(op).await.map_err(fail)?;
        fault_point("apply_finished")?;
        if let Some(session) = &mut recovery {
            session.complete().await?;
        }
        // The local host read phase is complete before the independently funded
        // selected-index ACK retention proof below.
        drop(recovery);
        drop(new_head);
        drop(commit);
        drop(prepared);
        drop(repo);
        drop(phase);
        finish_observation(locked)?;
        locked.state.landing.as_mut().unwrap().acknowledged = true;
        // One durable ACK save releases the restored fence and current result
        // claims only after exact checkout, publication and WC finish. The
        // selected immutable apply OP/VIEW retains the historical candidate.
        classify_acknowledged_claims(command, locked, job).await?;
        Ok(())
    }
    fn verify_original_report(
        p: &LandingPending,
        report: &LandingReport,
    ) -> Result<l::Phase, CommandError> {
        let job = report.job();
        let phase = l::Phase::try_from(job.phase).map_err(fail)?;
        let fresh =
            native_workspace_admin::remote_state::historical_landing_job(job).map_err(fail)?;
        let mut expected = p.clone();
        expected.job_id = Some(hex(&job.id));
        expected.terminal = match phase {
            l::Phase::Conflict => Some("conflict"),
            l::Phase::Unsupported => Some("unsupported"),
            l::Phase::CheckFailed => Some("check_failed"),
            l::Phase::CheckUnavailable => Some("check_unavailable"),
            l::Phase::StaleTarget => Some("stale_target"),
            l::Phase::Expired => Some("expired"),
            _ => None,
        }
        .map(str::to_owned);
        if let Some(candidate) = &job.candidate {
            let head = hex(&candidate.head);
            let mut roots = std::iter::once(head.clone())
                .chain(candidate.rewrites.iter().map(|row| hex(&row.new_commit)))
                .collect::<Vec<_>>();
            roots.sort();
            roots.dedup();
            if p.candidate_head.as_ref().is_some_and(|old| old != &head)
                || (!p.result_roots.is_empty() && {
                    let mut old = p.result_roots.clone();
                    old.sort();
                    old.dedup();
                    old != roots
                })
            {
                return Err(fail(
                    "recovery_required: fresh candidate differs from persisted result claims",
                ));
            }
            // An Intent/pre-Start capsule legitimately has no candidate yet.
            // Enrich only this validation shadow from the fresh checked lookup;
            // persistence and any apply still require CURRENT/canonical proof.
            if !expected.acknowledged {
                expected.candidate_head = Some(head);
                expected.result_roots = roots;
            }
        }
        if nonapplicable_phase(phase) {
            if job.roots_retained {
                return Err(fail("nonapplicable terminal still claims retained results"));
            }
            expected.apply_operation = None;
            expected.operation_writing = None;
            expected.acknowledged = false;
            expected.candidate_head = None;
            expected.result_roots.clear();
            expected.fork_operation = Some(expected.source_operation.clone());
            expected.fork_acknowledged = true;
        }
        fresh.validate(&expected).map_err(fail)?;
        if let Some(old) = &p.observed {
            if old.job_id != fresh.job_id
                || old.expires_at_seconds != fresh.expires_at_seconds
                || old
                    .candidate
                    .as_ref()
                    .is_some_and(|value| fresh.candidate.as_ref() != Some(value))
                || old
                    .receipt
                    .as_ref()
                    .is_some_and(|value| fresh.receipt.as_ref() != Some(value))
            {
                return Err(fail(
                    "recovery_required: original immutable job/candidate/receipt differs; state preserved",
                ));
            }
        }
        Ok(phase)
    }
    async fn verify_current_original_sources(
        command: &CommandHelper,
        repo: &std::sync::Arc<jj_lib::repo::ReadonlyRepo>,
        p: &LandingPending,
    ) -> Result<(), CommandError> {
        let mut reader = CanonicalReader::open(command, repo.store().clone()).await?;
        for source in &p.source_stack {
            let source_id = id(source)?;
            let commit = reader.commit(&source_id).await?;
            let current = repo
                .resolve_change_id(commit.change_id())
                .await
                .map_err(fail)?
                .ok_or_else(|| {
                    fail("reconciliation_required: original source is no longer visible")
                })?;
            let mut visible = current.visible_with_offsets().map(|(_, id)| id);
            if visible.next() != Some(&source_id) || visible.next().is_some() {
                return Err(fail(
                    "reconciliation_required: original source was amended or ambiguous; state preserved",
                ));
            }
        }
        Ok(())
    }
    async fn fresh_restored_freeze(
        command: &CommandHelper,
        locked: &mut LockedState,
        mut helper: jj_cli::cli_util::WorkspaceCommandHelper,
        prior_prepared_nonce: Option<[u8; 32]>,
        observed_job: Option<String>,
    ) -> Result<(), CommandError> {
        let p = locked.state.landing.clone().unwrap();
        let current = helper
            .get_wc_commit_id()
            .cloned()
            .ok_or_else(|| fail("recovery_required: current working copy is absent"))?;
        if helper.repo().op_id().hex() != p.source_operation
            || !p.source_stack.iter().any(|source| source == &current.hex())
        {
            return Err(fail(
                "recovery_required: current source cannot be freshly frozen at its original operation",
            ));
        }
        let binding = restored_preflight(command)?.binding;
        let mut recovery =
            RecoverySession::open(command, &p, binding, lr::Purpose::Freeze { observed_job })
                .await?;
        let journal = recovery.begin(prior_prepared_nonce).await?;
        if journal.stage() != lr::Stage::Prepared || journal.position() != lr::Position::AtBase {
            return Err(fail(
                "recovery_required: reconcile the existing host-owned freeze first",
            ));
        }
        drop(journal);
        let repo = helper.repo().clone();
        let workspace = helper.workspace_name().to_owned();
        let source = repo.store().get_commit_async(&current).await?;
        let mut tx =
            jj_cli::cli_util::start_repo_transaction(&repo, &workspace, command.string_args());
        stamp(&mut tx, &p, "freeze");
        recovery.stamp(&mut tx)?;
        let child = tx
            .repo_mut()
            .check_out(workspace, &source)
            .await
            .map_err(fail)?;
        tx.repo_mut().rebase_descendants().await?;
        recovery.check_deadline()?;
        let unpublished = tx
            .write("freshly freeze current restored landing source")
            .await
            .map_err(fail)?;
        let op = unpublished.operation().id().clone();
        fault_point_receipt("freeze_written", Some(&op.hex()))?;
        recovery.record(&op, child.id()).await?;
        fault_point_receipt("freeze_resolution_recorded", Some(&op.hex()))?;
        recovery.check_deadline()?;
        let (mut wc, _) = helper.start_working_copy_mutation().await?;
        wc.locked_wc().check_out(&child).await.map_err(fail)?;
        fault_point("freeze_checkout_applied")?;
        publish_resolution(command, &recovery, &op).await?;
        drop(unpublished);
        recovery.check_deadline()?;
        wc.finish(op.clone()).await.map_err(fail)?;
        recovery.complete().await?;
        let saved = locked.state.landing.as_mut().unwrap();
        saved.fork_operation = Some(op.hex());
        saved.fork_acknowledged = true;
        saved.operation_writing = None;
        locked.save().map_err(fail)
    }
    async fn acknowledge_current(
        command: &CommandHelper,
        locked: &mut LockedState,
        report: &LandingReport,
        helper: jj_cli::cli_util::WorkspaceCommandHelper,
        canonical: CanonicalLanding,
        prior_prepared_nonce: Option<[u8; 32]>,
    ) -> Result<(), CommandError> {
        let mut phase = CanonicalApplyPhase { helper, canonical };
        let p = locked.state.landing.clone().unwrap();
        let job = report.job();
        let candidate = job
            .candidate
            .as_ref()
            .ok_or_else(|| fail("committed candidate missing"))?;
        let repo = phase.helper.repo().clone();
        let workspace = phase.helper.workspace_name().to_owned();
        let wc_id = phase
            .helper
            .get_wc_commit_id()
            .cloned()
            .ok_or_else(|| fail("recovery_required: current working copy is absent"))?;
        let index = repo
            .readonly_index()
            .downcast_ref::<jj_native_index::NativeReadonlyIndex>()
            .ok_or_else(|| fail("native landing requires the lazy index"))?;
        let head = phase
            .canonical
            .models
            .admitted
            .last()
            .ok_or_else(|| fail("canonical stack is empty"))?;
        // Explicitly retain the admitted result even if it is only a logical
        // ancestor of the CURRENT view. This adds no head or evolution edge.
        let prepared = index
            .with_admitted_commit_witness(head)
            .await
            .map_err(fail)?;
        phase.canonical.models.admitted.clear(); // Retain funding through cache/transaction lifetime.
        let binding = restored_preflight(command)?.binding;
        let mut recovery = RecoverySession::open(
            command,
            &p,
            binding,
            lr::Purpose::Apply {
                job_id: hex(&job.id),
                candidate_head: hex(&candidate.head),
                candidate_digest: resolution_candidate_digest(candidate),
            },
        )
        .await?;
        let journal = recovery.begin(prior_prepared_nonce).await?;
        if journal.stage() != lr::Stage::Prepared || journal.position() != lr::Position::AtBase {
            return Err(fail(
                "recovery_required: reconcile the existing host-owned operation first",
            ));
        }
        drop(journal);
        let mut tx =
            jj_cli::cli_util::start_repo_transaction(&repo, &workspace, command.string_args());
        stamp(&mut tx, &p, "apply");
        recovery.stamp(&mut tx)?;
        tx.repo_mut().merge_index_from(&prepared)?;
        // Deliberately NO add_heads, rewrite recording, bookmark update, rebase,
        // snapshot, checkout or captured-operation replay in this ACK route.
        recovery.check_deadline()?;
        let unpublished = tx
            .write("acknowledge current canonical native landing without changing work")
            .await
            .map_err(fail)?;
        let op = unpublished.operation().id().clone();
        fault_point_receipt("apply_written", Some(&op.hex()))?;
        recovery.record(&op, &wc_id).await?;
        fault_point_receipt("resolution_recorded", Some(&op.hex()))?;
        let pending = locked.state.landing.as_mut().unwrap();
        pending.apply_operation = Some(op.hex());
        pending.operation_writing = None;
        pending.acknowledged = false;
        locked.save().map_err(fail)?;
        recovery.check_deadline()?;
        let (wc, _) = phase.helper.start_working_copy_mutation().await?;
        publish_resolution(command, &recovery, &op).await?;
        drop(unpublished);
        fault_point("resolution_published")?;
        recovery.check_deadline()?;
        wc.finish(op).await.map_err(fail)?;
        recovery.complete().await?;
        drop(recovery);
        drop(prepared);
        drop(repo);
        drop(phase);
        finish_observation(locked)?;
        locked.state.landing.as_mut().unwrap().acknowledged = true;
        classify_acknowledged_claims(command, locked, job).await
    }
    async fn classify_acknowledged_claims(
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
        // The legacy proof remains first. Release its decoded OP/VIEW before
        // separately funding a local selected-index read; neither is replay
        // authority, and an unrelated operation/index can never substitute.
        drop(visit);
        drop(view);
        drop(raw);
        if !found {
            found = selected_index_acknowledgement(command, bytes::<64>(saved)?, candidate.head)
                .await?;
        }
        if !found {
            return Err(fail(
                "recovery_required: selected apply operation/view/index does not retain the candidate",
            ));
        }
        // Membership proves retention only. No checkout, operation publication
        // or raw recovery occurs in this acknowledged bookkeeping path.
        let saved = locked.state.landing.as_mut().unwrap();
        saved.candidate_head = None;
        saved.result_roots.clear();
        saved.restored_lookup_only = false;
        locked.state.record_landing_observation(job).map_err(fail)?;
        locked.save().map_err(fail)?;
        cleanup_closed_landing(command, locked, job).await
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
