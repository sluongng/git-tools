//! Original-provider retained offload admission. No reference mutation or generic
//! content fallback; authority creation alone is not a complete backup proof.
pub use crate::ledger::offload::RetainedOffloadWitness;
use crate::{
    Inner, RemoteRepositoryControl, io,
    ledger::offload::{OffloadPinLease, SourcePin, SourcePinAuthority, SourcePinPhase},
};
use anyhow::{Result, ensure};
use std::{any::Any, sync::Arc};
use tinytree_client::{CloudPinSelection, CloudRawDigest, NativeClient, RetainedVersionSelector};
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
async fn acquire_source_pin(
    inner: &Arc<Inner>,
    scope: Arc<dyn Any + Send + Sync>,
    client: &NativeClient,
    pending: SourcePin,
) -> Result<bool> {
    let ledger = inner.ledger.clone();
    let expected = pending.clone();
    if !io::blocking_owned(scope.clone(), move || {
        ledger.source_pin_unchanged(&expected)
    })
    .await?
    {
        return Ok(false);
    }
    let authority = &pending.authority;
    let pin = match io::network(client.acquire_cloud_version_pin(
        authority.selector().workspace_id,
        pending.identity(),
        CloudPinSelection::Exact {
            selector: authority.selector(),
            selected: authority.selected(),
        },
    ))
    .await
    {
        Ok(pin) => pin,
        Err(error) => {
            // This endpoint reserves OutOfRange for a durably terminal pin
            // identity, not a missing or expired selected source version.
            if error.code() == tonic::Code::OutOfRange {
                let ledger = inner.ledger.clone();
                let expected = pending.clone();
                io::blocking_owned(scope, move || {
                    ledger.acknowledge_expired_acquisition(&expected)
                })
                .await?;
            }
            // Fail this logical request without another RPC. A terminal legacy
            // adoption may now own a new pending identity for the next request;
            // no unconfirmed or Active identity is silently reused or renewed.
            return Err(error.into());
        }
    };
    ensure!(
        pin.identity == pending.identity() && authority.agrees(&pin.version),
        "retained backend pin source changed"
    );
    let roots = match authority {
        SourcePinAuthority::File(authority) => {
            let roots = crate::cloud::retained_native_roots(
                &inner.config,
                authority.selector(),
                pin.version.manifest(),
            )?;
            ensure!(
                roots == authority.native_root_hints().collect::<Result<Vec<_>>>()?,
                "retained backend pin roots changed"
            );
            roots
        }
        SourcePinAuthority::Query(_) => Vec::new(),
    };
    let ledger = inner.ledger.clone();
    io::blocking_owned(scope, move || {
        if !ledger.source_pin_unchanged(&pending)? {
            return Ok(false);
        }
        match &pending.authority {
            SourcePinAuthority::File(_) => {
                ledger.install_offload_authority(&pin.version, &roots, &pending)?;
            }
            SourcePinAuthority::Query(authority) => {
                ledger.install_pinned_query_authority(authority, &pending)?;
            }
        }
        Ok(true)
    })
    .await
}
/// Establish durable ownership before returning a retained query source.
pub(crate) async fn adopt_query_source_pin(
    inner: &Arc<Inner>,
    owner: Arc<dyn Any + Send + Sync>,
    client: &NativeClient,
    authority: crate::ledger::CloudQueryAuthority,
) -> Result<()> {
    let ledger = inner.ledger.clone();
    let pending = io::blocking_owned(owner.clone(), move || {
        ledger.prepare_source_pin(&SourcePinAuthority::Query(authority))
    })
    .await?;
    if pending.phase != SourcePinPhase::Active {
        acquire_source_pin(inner, owner, client, pending).await?;
    }
    Ok(())
}
pub(crate) async fn ensure_file_source_pin(
    inner: &Arc<Inner>,
    owner: Arc<dyn Any + Send + Sync>,
    client: &NativeClient,
    authority: crate::ledger::CloudVersionAuthority,
) -> Result<()> {
    let ledger = inner.ledger.clone();
    let pending = io::blocking_owned(owner.clone(), move || {
        ledger.prepare_source_pin(&SourcePinAuthority::File(authority))
    })
    .await?;
    if pending.phase != SourcePinPhase::Active {
        acquire_source_pin(inner, owner, client, pending).await?;
    }
    Ok(())
}
pub(crate) async fn adopt_file_source_pin(
    inner: &Arc<Inner>,
    owner: Arc<dyn Any + Send + Sync>,
    client: &NativeClient,
    proof: Arc<tinytree_client::VerifiedRetainedVersion>,
    roots: Vec<ObjectKey>,
) -> Result<crate::ledger::CloudVersionAuthority> {
    let ledger = inner.ledger.clone();
    let pending = io::blocking_owned(owner.clone(), move || {
        ledger.prepare_offload_source_pin(&proof, &roots)
    })
    .await?;
    let SourcePinAuthority::File(authority) = pending.authority.clone() else {
        unreachable!()
    };
    acquire_source_pin(inner, owner, client, pending).await?;
    Ok(authority)
}

async fn release_source_pins(
    inner: &Arc<Inner>,
    scope: Arc<dyn Any + Send + Sync>,
    client: &NativeClient,
) -> Result<usize> {
    let mut changed = 0usize;
    let ledger = inner.ledger.clone();
    let rows = io::blocking_owned(scope.clone(), move || ledger.source_pins()).await?;
    for row in rows
        .into_iter()
        .filter(|p| p.phase == SourcePinPhase::ReleasePending)
    {
        io::network(client.release_cloud_version_pin(
            row.identity(),
            row.authority.selector(),
            row.authority.selected(),
        ))
        .await?;
        let ledger = inner.ledger.clone();
        changed += usize::from(
            io::blocking_owned(scope.clone(), move || {
                ledger.acknowledge_source_release(&row)
            })
            .await?,
        );
    }
    Ok(changed)
}
async fn replay_source_pins(
    inner: &Arc<Inner>,
    scope: Arc<dyn Any + Send + Sync>,
    client: &NativeClient,
) -> Result<usize> {
    let mut changed = release_source_pins(inner, scope.clone(), client).await?;
    // Adopt pre-existing authorities before issuing a new rotation ordinal.
    // No invented remote nonce or guessed proof: exact acquire verifies each.
    let ledger = inner.ledger.clone();
    let authorities = io::blocking_owned(scope.clone(), move || ledger.cloud_authorities()).await?;
    for authority in authorities {
        let ledger = inner.ledger.clone();
        let candidate = authority.clone();
        let pending = io::blocking_owned(scope.clone(), move || {
            if ledger
                .source_pins()?
                .iter()
                .any(|p| p.authority == SourcePinAuthority::File(candidate.clone()))
            {
                return Ok(None);
            }
            ledger
                .prepare_source_pin(&SourcePinAuthority::File(candidate))
                .map(Some)
        })
        .await?;
        if let Some(pending) = pending {
            changed +=
                usize::from(acquire_source_pin(inner, scope.clone(), client, pending).await?);
        }
    }
    let ledger = inner.ledger.clone();
    let queries =
        io::blocking_owned(scope.clone(), move || ledger.cloud_query_authorities()).await?;
    for authority in queries {
        let ledger = inner.ledger.clone();
        let pending = io::blocking_owned(scope.clone(), move || {
            ledger.prepare_source_pin(&SourcePinAuthority::Query(authority))
        })
        .await?;
        changed += usize::from(acquire_source_pin(inner, scope.clone(), client, pending).await?);
    }
    let ledger = inner.ledger.clone();
    let mut rows = io::blocking_owned(scope.clone(), move || ledger.source_pins()).await?;
    rows.sort_by_key(|p| std::cmp::Reverse(p.ordinal));
    for row in rows {
        if row.phase != SourcePinPhase::ReleasePending {
            changed += usize::from(acquire_source_pin(inner, scope.clone(), client, row).await?);
        }
    }
    changed += release_source_pins(inner, scope.clone(), client).await?;
    Ok(changed)
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
                replay_source_pins(&inner, scope.clone(), &client).await?;
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
                let pin_roots = required.clone();
                let candidate = io::blocking_owned(scope.clone(), move || {
                    ledger.pinned_cloud_authority(capture, &pin_roots)
                })
                .await?;
                let authority = if let Some(candidate) = candidate {
                    drop(proof);
                    let current = io::network(client.cloud_retained_version_selected(
                        candidate.selector(),
                        candidate.selected(),
                    ))
                    .await?;
                    ensure!(
                        candidate.agrees(&current),
                        "replayed retained source changed"
                    );
                    candidate
                } else {
                    let ledger = inner.ledger.clone();
                    let held = proof;
                    let wanted = roots.clone();
                    let pending = io::blocking_owned(scope.clone(), move || {
                        ledger.prepare_offload_source_pin(&held, &wanted)
                    })
                    .await?;
                    let SourcePinAuthority::File(authority) = pending.authority.clone() else {
                        unreachable!()
                    };
                    acquire_source_pin(&inner, scope.clone(), &client, pending).await?;
                    authority
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
                replay_source_pins(&inner, scope.clone(), &client).await?;
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
    /// Before publication can advance this workspace's retention floor, confirm
    /// only its already-installed, unconfirmed source adoptions. Unrelated
    /// workspaces, release outboxes and never-installed attempts cannot starve
    /// this upload; confirmed sources require no network replay.
    pub async fn reconcile_retained_source_pins_before_publish_until(
        &self,
        workspace: [u8; 32],
        owner: Arc<dyn Any + Send + Sync>,
        deadline: tokio::time::Instant,
    ) -> Result<usize> {
        ensure!(
            io::scope().is_none(),
            "source publication barrier requires independent admission"
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
                let pending = io::blocking_owned(scope.clone(), move || {
                    ledger.source_pins_before_publish(workspace, false)
                })
                .await?;
                let mut changed = 0;
                if !pending.is_empty() {
                    let client = inner
                        .connector
                        .get()
                        .await?
                        .with_lifetime_guard(scope.clone())
                        .with_operation_deadline(deadline);
                    for row in pending {
                        changed += usize::from(
                            acquire_source_pin(&inner, scope.clone(), &client, row).await?,
                        );
                    }
                }
                let ledger = inner.ledger.clone();
                io::blocking_owned(scope, move || {
                    ledger.source_pins_before_publish(workspace, true)?;
                    Ok(())
                })
                .await?;
                Ok(changed)
            })
            .await
        });
        tokio::time::timeout_at(deadline, task).await.map_err(|_| {
            tonic::Status::deadline_exceeded("source publication barrier deadline exceeded")
        })?
    }
    /// Replay bounded durable source-pin work after the host's registry fence
    /// is dropped. Immutable uploads must get their turn before maintenance.
    pub async fn reconcile_retained_source_pins_until(
        &self,
        owner: Arc<dyn Any + Send + Sync>,
        deadline: tokio::time::Instant,
    ) -> Result<usize> {
        ensure!(
            io::scope().is_none(),
            "source pin maintenance requires independent admission"
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
                let client = inner
                    .connector
                    .get()
                    .await?
                    .with_lifetime_guard(scope.clone())
                    .with_operation_deadline(deadline);
                replay_source_pins(&inner, scope, &client).await
            })
            .await
        });
        tokio::time::timeout_at(deadline, task).await.map_err(|_| {
            tonic::Status::deadline_exceeded("source pin maintenance deadline exceeded")
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
