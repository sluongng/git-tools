//! Original-provider retained offload admission. No reference mutation or generic
//! content fallback; authority creation alone is not a complete backup proof.
pub use crate::ledger::offload::RetainedOffloadWitness;
use crate::{RemoteRepositoryControl, io, ledger::offload::OffloadPinLease};
use anyhow::{Result, ensure};
use std::{any::Any, sync::Arc};
use tinytree_client::{CloudRawDigest, RetainedVersionSelector};
use tinytree_model::ObjectKey;

/// Trusted host assertion of an explicit backend retention contract covering
/// selected manifests, capture DATA, raw metadata and the native root closure.
/// Merely observing a VersionRecord or capability9 does not establish it.
/// Automatic callers must negotiate the dedicated contract before selecting it.
#[derive(Clone, Copy)]
pub enum DurableRetainedOffloadContract {
    ExplicitlyVerifiedV1,
}

/// Opaque physical owner for an immutable, SyncAll provider pin. It contains no
/// capture registry read fence and may survive exclusive capsule cleanup.
pub struct PinnedRetainedVersion {
    lease: Arc<OffloadPinLease>,
}
impl PinnedRetainedVersion {
    pub fn witness(&self) -> RetainedOffloadWitness {
        self.lease.witness()
    }
    pub fn validate(&self, expected: &RetainedOffloadWitness) -> Result<()> {
        self.lease.validate(expected)
    }
    pub fn physical_owner(&self) -> Arc<dyn Any + Send + Sync> {
        self.lease.clone()
    }
}
impl RemoteRepositoryControl {
    /// Independently funded background work, never the foreground control lease.
    /// The caller owns complete old-capsule/manifest/DATA validation. This method
    /// independently authorizes the exact old selection through THIS connector,
    /// proves its full direct-root set and durably installs a readable source pin.
    pub async fn admit_retained_offload_until(
        &self,
        _contract: DurableRetainedOffloadContract,
        selector: RetainedVersionSelector,
        selected: CloudRawDigest,
        capture: [u8; 32],
        required: Vec<ObjectKey>,
        owner: Arc<dyn Any + Send + Sync>,
        deadline: tokio::time::Instant,
    ) -> Result<PinnedRetainedVersion> {
        ensure!(
            io::scope().is_none(),
            "retained offload requires independent host admission"
        );
        ensure!(
            capture != [0; 32]
                && required
                    .iter()
                    .filter(|r| !crate::ledger::offload::intrinsic(**r))
                    .count()
                    <= self.inner.config.max_admitted_roots
                && required
                    .windows(2)
                    .all(|p| (p[0].kind as u8, p[0].id.0) < (p[1].kind as u8, p[1].id.0)),
            "retained offload root bounds or order"
        );
        let deadline = deadline.min(self.inner.deadline());
        let permit = tokio::time::timeout_at(
            deadline,
            self.inner.background_transfer.clone().acquire_owned(),
        )
        .await??;
        let scope = Arc::new(io::ControlScope::background_transfer(
            permit, deadline, owner,
        ));
        let inner = self.inner.clone();
        let completion = inner.metadata_work.enter();
        let task = io::owned(async move {
            io::control_scope(scope.clone(), async move {
                let _completion = completion;
                io::check_deadline()?;
                let client = inner
                    .connector
                    .get()
                    .await?
                    .with_lifetime_guard(scope.clone())
                    .with_operation_deadline(deadline);
                let proof = Arc::new(
                    io::network(client.cloud_retained_version_selected(selector, selected)).await?,
                );
                let config = inner.config.clone();
                let expected = required
                    .iter()
                    .copied()
                    .filter(|r| !crate::ledger::offload::intrinsic(*r))
                    .collect::<Vec<_>>();
                let held = proof.clone();
                let roots = io::blocking_owned(scope.clone(), move || {
                    let roots =
                        crate::cloud::retained_native_roots(&config, selector, held.manifest())?;
                    ensure!(roots == expected, "retained capture canonical roots differ");
                    Ok(roots)
                })
                .await?;
                let ledger = inner.ledger.clone();
                let wanted = roots.clone();
                let pin_roots = required.clone();
                let candidate = io::blocking_owned(scope.clone(), move || {
                    match ledger.pinned_cloud_authority(capture, &pin_roots)? {
                        Some(authority) => Ok(Some(authority)),
                        None => ledger.reusable_cloud_authority(&wanted),
                    }
                })
                .await?;
                let authority = if let Some(candidate) = candidate {
                    // Reuse a different retained selection only after its OWN
                    // current owner/source proof agrees; hints grant nothing.
                    let current = io::network(client.cloud_retained_version_selected(
                        candidate.selector(),
                        candidate.selected(),
                    ))
                    .await?;
                    ensure!(candidate.agrees(&current), "reused retained source changed");
                    let decoded = crate::cloud::retained_native_roots(
                        &inner.config,
                        candidate.selector(),
                        current.manifest(),
                    )?;
                    ensure!(
                        decoded == candidate.native_root_hints().collect::<Result<Vec<_>>>()?,
                        "reused retained source roots changed"
                    );
                    candidate
                } else {
                    let ledger = inner.ledger.clone();
                    let held = proof.clone();
                    let wanted = roots.clone();
                    io::blocking_owned(scope.clone(), move || {
                        ledger.install_offload_authority(&held, &wanted)
                    })
                    .await?
                };
                io::check_deadline()?;
                let ledger = inner.ledger.clone();
                let lease = io::blocking_owned(scope, move || {
                    ledger.install_offload_pin(&authority, capture, &required)
                })
                .await?;
                Ok(PinnedRetainedVersion { lease })
            })
            .await
        });
        tokio::time::timeout_at(deadline, task).await.map_err(|_| {
            tonic::Status::deadline_exceeded("retained offload admission deadline exceeded")
        })?
    }
    /// Reauthorize a durable pending cleanup pin BEFORE registry maintenance.
    pub async fn resume_retained_offload_until(
        &self,
        _contract: DurableRetainedOffloadContract,
        witness: RetainedOffloadWitness,
        owner: Arc<dyn Any + Send + Sync>,
        deadline: tokio::time::Instant,
    ) -> Result<PinnedRetainedVersion> {
        ensure!(
            io::scope().is_none(),
            "retained offload resume requires independent admission"
        );
        let deadline = deadline.min(self.inner.deadline());
        let permit = tokio::time::timeout_at(
            deadline,
            self.inner.background_transfer.clone().acquire_owned(),
        )
        .await??;
        let scope = Arc::new(io::ControlScope::background_transfer(
            permit, deadline, owner,
        ));
        let inner = self.inner.clone();
        let completion = inner.metadata_work.enter();
        let task = io::owned(async move {
            io::control_scope(scope.clone(), async move {
                let _completion = completion;
                let ledger = inner.ledger.clone();
                let expected = witness.clone();
                let authority = io::blocking_owned(scope.clone(), move || {
                    ledger.offload_pin_authority(&expected)
                })
                .await?;
                let client = inner
                    .connector
                    .get()
                    .await?
                    .with_lifetime_guard(scope.clone())
                    .with_operation_deadline(deadline);
                let proof =
                    io::network(client.cloud_retained_version_selected(
                        authority.selector(),
                        authority.selected(),
                    ))
                    .await?;
                ensure!(authority.agrees(&proof), "retained offload source changed");
                let ledger = inner.ledger.clone();
                let lease =
                    io::blocking_owned(scope, move || ledger.resume_offload_pin(&witness)).await?;
                Ok(PinnedRetainedVersion { lease })
            })
            .await
        });
        tokio::time::timeout_at(deadline, task).await.map_err(|_| {
            tonic::Status::deadline_exceeded("retained offload resume deadline exceeded")
        })?
    }
    /// After ACK and after dropping all opaque physical pin owners. No network
    /// or foreground lease; a still-live owner preserves the pin for later retry.
    pub async fn release_retained_offload_pin(
        &self,
        witness: RetainedOffloadWitness,
    ) -> Result<bool> {
        let ledger = self.inner.ledger.clone();
        let completion = self.inner.metadata_work.enter();
        io::blocking_owned(Arc::new(completion), move || {
            Ok(ledger.release_offload_pins(&[], Some(&witness))? > 0)
        })
        .await
    }
    /// Trusted host: supply the complete pending and cleanup binding set while
    /// producers are paused. Never use a first page or partial inventory here.
    /// Physical pin owners prevent removal even if a caller omits their binding.
    pub async fn reconcile_retained_offload_pins(&self, protected: Vec<[u8; 32]>) -> Result<usize> {
        let ledger = self.inner.ledger.clone();
        let completion = self.inner.metadata_work.enter();
        io::blocking_owned(Arc::new(completion), move || {
            ledger.release_offload_pins(&protected, None)
        })
        .await
    }
}
