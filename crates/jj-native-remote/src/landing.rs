//! Short original-intent submission/status, never check execution or verdicts.
use crate::{NativeTransferLimits, RemoteRepositoryControl};
use anyhow::{Result, ensure};
use native_broker::{LandingIntent, LandingReport, landing_protocol as l};
use std::sync::Arc;
use tinytree_client::{CheckedLandingJob, LandingPhase, LandingPreparation, LandingStart};
use tinytree_model as m;
impl RemoteRepositoryControl {
    pub(crate) async fn landing_owned(
        &self,
        intent: LandingIntent,
        submit: bool,
        owner: native_broker::CommandGuard,
    ) -> Result<LandingReport> {
        intent.validate()?;
        let deadline = self.inner.deadline();
        let request = &intent.request;
        let start = LandingStart {
            idempotency_key: request.idempotency_key.clone(),
            source_base: m::CommitId(request.source_base.as_slice().try_into()?),
            source_head: m::CommitId(request.source_head.as_slice().try_into()?),
            destination: request.destination.clone(),
            expected_generation: request.expected_generation,
            expected_head: m::CommitId(request.expected_head.as_slice().try_into()?),
            profile_name: request.profile_name.clone(),
            expected_profile_version: request.expected_profile_version.clone(),
        };
        // Validate the configured immutable identity even when a landing clone
        // is cached. Rotation fails closed; no token/CA/endpoint reread reconnects.
        let cached = self
            .run_owned(deadline, owner.clone(), async {
                // Cache only the connector's clean immutable client. A per-command
                // clone would pin its original deadline/job owner indefinitely.
                let clean = self.inner.connector.get_bare().await?;
                Ok(self
                    .inner
                    .landing_client
                    .get_or_try_init(|| async { clean.with_landing_budget(256 * 1024 * 1024) })
                    .await?
                    .clone())
            })
            .await?;
        let client = cached
            .with_operation_deadline(deadline)
            .with_rpc_lifetime_guard(Arc::new(owner.clone()))
            .with_lifetime_guard(Arc::new(owner.clone()));
        // Always recover first, including after launch permission/profile removal.
        // NotFound alone permits the exact original Start to be attempted.
        let existing = self
            .run_owned(deadline, owner.clone(), async {
                Ok(client
                    .lookup_landing(&start, intent.observed_job, false)
                    .await?)
            })
            .await;
        let stages;
        let job = match existing {
            Ok(job) => job,
            Err(error)
                if error
                    .downcast_ref::<tonic::Status>()
                    .is_some_and(|s| s.code() == tonic::Code::NotFound)
                    && submit
                    && intent.observed_job.is_none() =>
            {
                stages = self
                    .stage_landing_stack(
                        start.source_base,
                        intent.source_stack.clone(),
                        owner.clone(),
                        NativeTransferLimits::default(),
                        deadline,
                    )
                    .await?;
                let job = self
                    .run_owned(deadline, owner.clone(), async {
                        Ok(client.start_landing(&start).await?)
                    })
                    .await?;
                // Stages are recovery hints only. Successful Start durably owns
                // its canonical roots; before that, uncertainty retains intent.
                drop(stages);
                job
            }
            Err(error) => return Err(error.into()),
        };
        ensure!(
            tokio::time::Instant::now() < deadline,
            "native landing original deadline exceeded"
        );
        let wire = wire_job(&job);
        let result_owner: native_broker::CommandGuard = Arc::new((job, owner));
        Ok(LandingReport::checked(wire, &intent, result_owner)?)
    }
}
fn wire_job(job: &CheckedLandingJob) -> l::LandingJob {
    let rewrite = |v: &tinytree_client::LandingRewrite| l::Rewrite {
        old_commit: v.old_commit.0.to_vec(),
        new_commit: v.new_commit.0.to_vec(),
        change_id: v.change_id.0.to_vec(),
    };
    l::LandingJob {
        id: job.handle().id().to_vec(),
        phase: match job.phase() {
            LandingPhase::Intent => l::Phase::Intent,
            LandingPhase::PreparedPendingChecks => l::Phase::PreparedPendingChecks,
            LandingPhase::Checked => l::Phase::Checked,
            LandingPhase::Committed => l::Phase::Committed,
            LandingPhase::Conflict => l::Phase::Conflict,
            LandingPhase::Unsupported => l::Phase::Unsupported,
            LandingPhase::CheckFailed => l::Phase::CheckFailed,
            LandingPhase::CheckUnavailable => l::Phase::CheckUnavailable,
            LandingPhase::StaleTarget => l::Phase::StaleTarget,
            LandingPhase::Expired => l::Phase::Expired,
        } as i32,
        preparation: match job.preparation() {
            LandingPreparation::ProfileConfigured => l::PreparationAvailability::ProfileConfigured,
            LandingPreparation::ProfileUnavailable => {
                l::PreparationAvailability::ProfileUnavailable
            }
            LandingPreparation::PreparationDisabled => {
                l::PreparationAvailability::PreparationDisabled
            }
        } as i32,
        roots_retained: job.roots_retained(),
        expires_at_seconds: job.expires_at_seconds(),
        profile_name: job.handle().intent().profile_name.clone(),
        profile_version: job.handle().intent().expected_profile_version.clone(),
        candidate: job.candidate().map(|c| l::Candidate {
            head: c.head.0.to_vec(),
            root_tree: c.root_tree.0.to_vec(),
            rewrites: c.rewrites.iter().map(rewrite).collect(),
        }),
        receipt: job.receipt().map(|r| l::Receipt {
            reference: Some(native_broker::native_protocol::Reference {
                name: r.destination.clone(),
                generation: r.generation,
                commit: Some(native_broker::native_protocol::ObjectDescriptor {
                    key: Some(native_broker::native_protocol::ObjectKey {
                        kind: 3,
                        native_id: r.commit.key.id.0.to_vec(),
                    }),
                    content_blake3: r.commit.key.id.0.to_vec(),
                    size_bytes: r.commit.size_bytes,
                    object_format_version: r.commit.format_version as u32,
                }),
            }),
            rewrites: r.rewrites.iter().map(rewrite).collect(),
            evidence: r.evidence.to_vec(),
        }),
        conflict_path: job.conflict_path().map(str::to_owned),
        checker_progress: job.checker_progress().cloned(),
        recovery_inspection: None,
    }
}
