//! Parent-membership evidence paired with an existing complete retained hold.
use super::{Owner, RemoteRepositoryControl};
use crate::{NativeCaptureFailure, NativeCaptureReason, NativeCaptureStage, io};
use anyhow::{Context, Result, ensure};
use std::sync::Arc;
use tinytree_client::{CheckedScopeHandle, FactAnswer, NativeClient, QueryFact};
use tinytree_model as m;
use tokio::time::Instant;

pub(super) struct AncestorWitness {
    pub ancestor: m::ObjectKey,
    pub head: m::ObjectKey,
    pub name: String,
    pub generation: u64,
    pub scope: CheckedScopeHandle,
    pub member: bool,
    _owner: Owner,
}

// CheckedScopeHandle has no SDK Drop cleanup. Retain the physical owner through
// explicit closure, or through one bounded panic/early-drop backstop attempt.
struct ScopeRelease {
    client: NativeClient,
    scope: Option<CheckedScopeHandle>,
    owner: Owner,
    cleanup: std::time::Duration,
}
impl ScopeRelease {
    async fn close(&mut self) -> Result<CheckedScopeHandle> {
        let result = self
            .client
            .clone()
            .with_cleanup_deadline(Instant::now() + self.cleanup)
            .close_query_scope(self.scope.as_ref().expect("owned capture proof scope"))
            .await;
        let scope = self.scope.take().expect("owned capture proof scope");
        result.map_err(|error| {
            NativeCaptureFailure::new(
                NativeCaptureStage::BoundarySelection,
                NativeCaptureReason::ScopeCloseUnconfirmed,
            )
            .attach(error.into())
        })?;
        Ok(scope)
    }
}
impl Drop for ScopeRelease {
    fn drop(&mut self) {
        let Some(scope) = self.scope.take() else {
            return;
        };
        let client = self
            .client
            .clone()
            .with_cleanup_deadline(Instant::now() + self.cleanup);
        let owner = self.owner.clone();
        let _ = io::background(async move {
            let _owner = owner;
            let _ = client.close_query_scope(&scope).await;
        });
    }
}

impl RemoteRepositoryControl {
    pub(super) async fn capture_ancestor_proof(
        &self,
        ancestor: m::ObjectKey,
        head: m::ObjectKey,
        name: String,
        generation: u64,
        owner: Owner,
        deadline: Instant,
    ) -> Result<AncestorWitness> {
        let control = self.clone();
        let parent = io::scope().context("capture proof requires background scope")?;
        let work = self.inner.metadata_work.enter();
        let physical = Arc::new((owner, parent.clone(), work));
        struct Completed {
            result: Result<AncestorWitness>,
            _scope: Arc<io::ControlScope>,
            _owner: Owner,
        }
        let completed = io::owned(async move {
            let result = io::control_scope(parent.clone(), async {
                let client = control
                    .physical_client()
                    .await?
                    .with_operation_deadline(deadline)
                    .with_rpc_lifetime_guard(physical.clone());
                control
                    .fresh_capture_hold_named(&client, &name, generation, head)
                    .await?;
                // Await SDK open directly with its original deadline. A network
                // post-deadline check could discard a returned handle unguarded.
                let scope = client
                    .open_query_scope_v2(vec![m::CommitId(head.id.0)])
                    .await
                    .map_err(|error| {
                        NativeCaptureFailure::new(
                            NativeCaptureStage::BoundarySelection,
                            NativeCaptureReason::ScopeOpenUnconfirmed,
                        )
                        .attach(error.into())
                    })?;
                let mut release = ScopeRelease {
                    client: client.clone(),
                    scope: Some(scope),
                    owner: physical.clone(),
                    cleanup: std::time::Duration::from_millis(
                        control.inner.config.remote.query_cleanup_timeout_ms,
                    ),
                };
                let result = async {
                    io::check_deadline()?;
                    let scope = release.scope.as_ref().expect("owned capture proof scope");
                    ensure!(
                        scope.repository_id() == control.inner.config.remote.repository_id
                            && scope.remote_roots() == [m::CommitId(head.id.0)]
                            && scope.query_identity_version() == 2,
                        "capture ancestor scope identity differs"
                    );
                    let answers = client
                        .query_facts(scope, vec![QueryFact::HasId(m::CommitId(ancestor.id.0))])
                        .await?;
                    let member = match answers.as_slice() {
                        [FactAnswer::HasId(member)] => *member,
                        _ => {
                            return Err(tonic::Status::data_loss(
                                "capture ancestor fact response differs",
                            )
                            .into());
                        }
                    };
                    control
                        .fresh_capture_hold_named(&client, &name, generation, head)
                        .await?;
                    io::check_deadline()?;
                    Ok::<_, anyhow::Error>(member)
                }
                .await;
                // Release has a separate configured grace. io::network would
                // clamp this cleanup attempt to an expired data deadline.
                let scope = release.close().await?;
                let member = result?;
                io::check_deadline()?;
                Ok(AncestorWitness {
                    ancestor,
                    head,
                    name,
                    generation,
                    scope,
                    member,
                    _owner: physical.clone(),
                })
            })
            .await;
            Ok(Completed {
                result,
                _scope: parent,
                _owner: physical,
            })
        })
        .await?;
        completed.result
    }
}
