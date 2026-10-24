//! Sparse canonical metadata rooted in durable remote retention proofs.
use crate::{RemoteObjectIo, backend, io, ledger::Authority, unhex};
use anyhow::{Context, Result, ensure};
use jj_native_backend::{CopyCatalogLimits, CopyCatalogSnapshot, NativeObjectIo, VerifiedMetadata};
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tinytree_client::MetadataOutcome;
use tinytree_model as m;

// Fields drop in order: the completion ticket is last so idle observers cannot
// outrun destruction of any provider, external admission or control-scope owner.
struct MetadataJob {
    provider: RemoteObjectIo,
    owner: Arc<dyn Any + Send + Sync>,
    scope: Option<Arc<io::ControlScope>>,
    _completion: io::WorkTicket,
}
fn check(deadline: tokio::time::Instant) -> Result<()> {
    ensure!(
        tokio::time::Instant::now() < deadline,
        "native metadata operation deadline exceeded"
    );
    io::check_deadline()
}
#[cfg(test)]
#[derive(Default)]
pub(crate) struct CatalogBoundary {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}
impl RemoteObjectIo {
    pub(crate) async fn owned_descriptor(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<Option<m::Descriptor>> {
        let job = Arc::new(MetadataJob {
            provider: self.clone(),
            owner,
            scope: io::scope(),
            _completion: self.inner.metadata_work.enter(),
        });
        io::owned(async move {
            let owner: Arc<dyn Any + Send + Sync> = job.clone();
            let operation = async {
                let provider = &job.provider;
                if key.kind != m::Kind::File {
                    let ledger = provider.inner.ledger.clone();
                    io::blocking_owned(owner.clone(), move || {
                        if let Some(
                            authority @ (Authority::CloudVersion(_) | Authority::CloudQuery(_)),
                        ) = ledger.metadata_authority(key)?
                        {
                            ledger.validate_authority(&authority)?;
                        }
                        Ok(())
                    })
                    .await?;
                }
                if let Some(descriptor) = provider
                    .inner
                    .local
                    .descriptor_with_owner(key, owner.clone())
                    .await
                    .map_err(backend)?
                {
                    return Ok(Some(descriptor));
                }
                if key.kind == m::Kind::File {
                    provider.promise_owned(m::FileId(key.id.0), owner).await
                } else {
                    let ledger = provider.inner.ledger.clone();
                    let root =
                        io::blocking_owned(owner.clone(), move || ledger.metadata_authority(key))
                            .await?;
                    if root.is_some() {
                        provider
                            .verified_metadata_with_owner(key, owner)
                            .await
                            .map(|metadata| Some(metadata.descriptor()))
                    } else {
                        Ok(None)
                    }
                }
            };
            match job.scope.clone() {
                Some(scope) => io::control_scope(scope, operation).await,
                None => operation.await,
            }
        })
        .await
    }
    pub(crate) async fn owned_catalog(
        &self,
        limits: CopyCatalogLimits,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<CopyCatalogSnapshot> {
        let deadline = io::scope().map_or_else(
            || self.inner.deadline(),
            |scope| scope.deadline.min(self.inner.deadline()),
        );
        let job = Arc::new(MetadataJob {
            provider: self.clone(),
            owner,
            scope: io::scope(),
            _completion: self.inner.metadata_work.enter(),
        });
        let ledger = job.provider.inner.ledger.clone();
        let catalog_is_unproven = tokio::time::timeout_at(
            deadline,
            io::blocking_owned(job.clone(), move || {
                Ok(!ledger.cloud_authorities()?.is_empty()
                    || !ledger.cloud_query_authorities()?.is_empty())
            }),
        )
        .await
        .map_err(|_| crate::backend_error(anyhow::anyhow!("native catalog deadline exceeded")))?
        .map_err(crate::backend_error)?;
        if catalog_is_unproven {
            return Err(jj_lib::backend::BackendError::Unsupported(
                "complete retained-version copy catalog is not available".into(),
            ));
        }
        // Keep the whole operation owner through the final authority check;
        // snapshot payloads must be destroyed before that last owner releases.
        let final_job = job.clone();
        let snapshot = io::owned(async move {
            let owner: Arc<dyn Any + Send + Sync> = job.clone();
            let control = crate::RemoteRepositoryControl {
                inner: job.provider.inner.clone(),
            };
            let operation = async {
                control.complete_metadata().await?;
                #[cfg(test)]
                {
                    let boundary = job.provider.inner.catalog_boundary.lock().unwrap().clone();
                    if let Some(boundary) = boundary {
                        boundary.entered.notify_one();
                        boundary.release.notified().await;
                    }
                }
                job.provider
                    .inner
                    .local
                    .copy_catalog_snapshot_with_owner(limits, owner.clone())
                    .await
                    .map_err(backend)
            };
            match job.scope.clone() {
                Some(scope) => {
                    io::control_scope(
                        io::ControlScope::with_owner(scope, owner.clone()),
                        operation,
                    )
                    .await
                }
                None => control.run_owned(deadline, owner.clone(), operation).await,
            }
        })
        .await
        .map_err(crate::backend_error)?;
        // Admission is monotonic: a first cloud version can be installed while
        // complete_metadata or Direct's snapshot awaits. Recheck AFTER the
        // snapshot so no newly fetched partial catalog can escape as complete.
        let ledger = final_job.provider.inner.ledger.clone();
        let catalog_is_unproven = tokio::time::timeout_at(
            deadline,
            io::blocking_owned(final_job.clone(), move || {
                Ok(!ledger.cloud_authorities()?.is_empty()
                    || !ledger.cloud_query_authorities()?.is_empty())
            }),
        )
        .await
        .map_err(|_| crate::backend_error(anyhow::anyhow!("native catalog deadline exceeded")))?
        .map_err(crate::backend_error)?;
        if catalog_is_unproven {
            return Err(jj_lib::backend::BackendError::Unsupported(
                "complete retained-version copy catalog is not available".into(),
            ));
        }
        Ok(snapshot)
    }
    /// Known IDs alone are not grants. Recheck every canonical edge back to its pin.
    pub(crate) async fn prove_owned(
        &self,
        authority: &Authority,
        mut key: m::ObjectKey,
        owner: Arc<dyn Any + Send + Sync>,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        self.inner.ledger.validate_authority(authority)?;
        if let Authority::CloudQuery(query) = authority {
            let ledger = self.inner.ledger.clone();
            let captured = query.clone();
            io::blocking_owned(owner, move || {
                ledger
                    .cloud_query_path(&captured, key, &HashMap::new())
                    .map(|_| ())
            })
            .await?;
            return Ok(());
        }
        if let Authority::CloudVersion(version) = authority {
            let ledger = self.inner.ledger.clone();
            let captured = version.clone();
            ensure!(
                io::blocking_owned(owner, move || ledger.cloud_path(&captured, key))
                    .await?
                    .is_some(),
                "native metadata lacks canonical retained-version provenance"
            );
            // The durable edge facts were recorded only after canonical SDK
            // verification. Missing bytes still reauthorize the entire path
            // server-side; resident bytes need no parent-body hydration.
            return Ok(());
        }
        let authority_key = authority.key();
        let mut seen = HashSet::new();
        loop {
            check(deadline)?;
            ensure!(
                seen.insert(key) && seen.len() <= self.inner.config.closure.max_objects,
                "native metadata provenance is cyclic or exceeds its budget"
            );
            let ledger = self.inner.ledger.clone();
            let captured = authority.clone();
            if io::blocking_owned(owner.clone(), move || ledger.query_terminal(&captured, key))
                .await?
            {
                return Ok(());
            }
            let ledger = self.inner.ledger.clone();
            let head = authority_key.clone();
            let reference =
                io::blocking_owned(owner.clone(), move || ledger.reference(&head, key)).await?;
            if let Some(root) = authority.root() {
                let root_key = m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(unhex(&root.head)?),
                };
                if key == root_key {
                    ensure!(
                        reference.is_some_and(|reference| reference.parent.is_none())
                            || (root.complete && self.inner.ledger.member(&root.head, key)?),
                        "native root metadata lacks pin provenance"
                    );
                    return Ok(());
                }
                if root.complete && reference.is_none() {
                    let ledger = self.inner.ledger.clone();
                    let head = root.head.clone();
                    ensure!(
                        io::blocking_owned(owner.clone(), move || ledger.member(&head, key))
                            .await?,
                        "native legacy metadata lacks complete admission provenance"
                    );
                    return Ok(());
                }
            }
            let parent = reference
                .and_then(|reference| reference.parent)
                .context("native metadata lacks canonical parent provenance")?
                .descriptor()?;
            let ledger = self.inner.ledger.clone();
            let head = authority_key.clone();
            ensure!(
                io::blocking_owned(owner.clone(), move || ledger.member(&head, parent.key)).await?,
                "native metadata parent is not verified"
            );
            let verified = self
                .inner
                .local
                .read_metadata_with_owner(parent.key, owner.clone())
                .await
                .map_err(backend)?;
            ensure!(
                verified.descriptor() == parent
                    && verified.into_object().references().contains(&key),
                "native metadata parent does not reference requested object"
            );
            key = parent.key;
        }
    }
    pub(crate) async fn verified_metadata(&self, key: m::ObjectKey) -> Result<VerifiedMetadata> {
        self.verified_metadata_with_owner(key, Arc::new(())).await
    }
    /// Capture the caller's complete admission/lifetime owner before any I/O.
    pub(crate) async fn verified_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<VerifiedMetadata> {
        let deadline = io::scope().map_or_else(
            || self.inner.deadline(),
            |scope| scope.deadline.min(self.inner.deadline()),
        );
        let job = Arc::new(MetadataJob {
            provider: self.clone(),
            owner,
            scope: io::scope(),
            _completion: self.inner.metadata_work.enter(),
        });
        io::owned(async move {
            let result = {
                let provider = &job.provider;
                let owner: Arc<dyn Any + Send + Sync> = job.clone();
                let operation = async {
                    ensure!(
                        key.kind != m::Kind::File,
                        "files require verified streaming reads"
                    );
                    check(deadline)?;
                    let local = provider
                        .inner
                        .local
                        .descriptor(key)
                        .await
                        .map_err(backend)?;
                    let ledger = provider.inner.ledger.clone();
                    let root =
                        io::blocking_owned(owner.clone(), move || ledger.metadata_authority(key))
                            .await?;
                    let Some(mut root) = root else {
                        ensure!(
                            local.is_some(),
                            "native metadata has no retained canonical provenance"
                        );
                        return provider
                            .inner
                            .local
                            .read_metadata_with_owner(key, owner)
                            .await
                            .map_err(backend);
                    };
                    // Refresh before the metadata lock: control can itself need metadata.
                    if local.is_none()
                        && root
                            .root()
                            .is_some_and(|root| root.binding != provider.inner.ledger.binding.scope)
                    {
                        let root_head = root
                            .root()
                            .context("native retained root missing")?
                            .head
                            .clone();
                        crate::RemoteRepositoryControl {
                            inner: provider.inner.clone(),
                        }
                        .refresh_scope_hold(m::CommitId(unhex(&root_head)?))
                        .await?;
                        let ledger = provider.inner.ledger.clone();
                        root = io::blocking_owned(owner.clone(), move || {
                            ledger.metadata_authority(key)
                        })
                        .await?
                        .context("native metadata root disappeared")?;
                    }
                    let _gate = tokio::time::timeout_at(
                        deadline,
                        provider.inner.metadata.clone().lock_owned(),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("native metadata admission deadline exceeded"))?;
                    if matches!(root, Authority::CloudVersion(_) | Authority::CloudQuery(_)) {
                        tokio::time::timeout_at(
                            deadline,
                            provider.load_metadata(root, key, deadline, owner),
                        )
                        .await
                        .map_err(|_| anyhow::anyhow!("native cloud metadata deadline exceeded"))?
                    } else {
                        provider.load_metadata(root, key, deadline, owner).await
                    }
                };
                match job.scope.clone() {
                    Some(scope) => io::control_scope(scope, operation).await,
                    None => operation.await,
                }
            };
            drop(job);
            result
        })
        .await
    }
    async fn load_metadata(
        &self,
        root: Authority,
        key: m::ObjectKey,
        deadline: tokio::time::Instant,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<VerifiedMetadata> {
        check(deadline)?;
        self.prove_owned(&root, key, owner.clone(), deadline)
            .await?;
        // SDK decoded payloads/quotas must outlive every temporary Node copy;
        // local declaration order destroys all node buffers before these owners.
        let mut cloud_replies = Vec::new();
        let mut cloud_parents = HashMap::new();
        let mut nodes = HashMap::new();
        let mut pending = vec![(key, false)];
        let mut active = HashSet::new();
        let mut done = HashSet::new();
        let mut order = Vec::new();
        let mut canonical_bytes = 0usize;
        // Only CopyHistory requires dependency installation. Commit/tree edges
        // become known grants, not eager recursive metadata requests.
        while let Some((current, exit)) = pending.pop() {
            check(deadline)?;
            if exit {
                active.remove(&current);
                done.insert(current);
                order.push(current);
                continue;
            }
            if done.contains(&current) {
                continue;
            }
            ensure!(active.insert(current), "native copy metadata has a cycle");
            ensure!(
                nodes.len() < self.inner.config.closure.max_objects,
                "native metadata object budget exceeded"
            );
            let node = if self
                .inner
                .local
                .descriptor(current)
                .await
                .map_err(backend)?
                .is_some()
            {
                let verified = self
                    .inner
                    .local
                    .read_metadata_with_owner(current, owner.clone())
                    .await
                    .map_err(backend)?;
                crate::closure::Node {
                    descriptor: verified.descriptor(),
                    bytes: verified.bytes().to_vec(),
                    object: verified.into_object(),
                }
            } else if let Authority::CloudQuery(query) = &root {
                let metadata = Arc::new(
                    self.snapshot_metadata(query, current, &cloud_parents, owner.clone())
                        .await?,
                );
                ensure!(
                    cloud_replies.len() < self.inner.config.closure.max_objects,
                    "snapshot metadata reply count exceeded"
                );
                cloud_replies.push(metadata.clone());
                let bytes = metadata.canonical_bytes().to_vec();
                let object = m::decode(&bytes, self.inner.model_limits())?;
                crate::closure::Node {
                    descriptor: metadata.descriptor(),
                    bytes,
                    object,
                }
            } else if let Authority::CloudVersion(version) = &root {
                let metadata = Arc::new(
                    tokio::time::timeout_at(
                        deadline,
                        self.cloud_metadata(version, current, &cloud_parents, owner.clone()),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("native cloud metadata deadline exceeded"))??,
                );
                ensure!(
                    cloud_replies.len() < self.inner.config.closure.max_objects,
                    "cloud metadata reply count exceeded"
                );
                cloud_replies.push(metadata.clone());
                let bytes = metadata.canonical_bytes().to_vec();
                let object = m::decode(&bytes, self.inner.model_limits())?;
                crate::closure::Node {
                    descriptor: metadata.descriptor(),
                    bytes,
                    object,
                }
            } else {
                let client = self
                    .inner
                    .connector
                    .get()
                    .await?
                    .with_lifetime_guard(Arc::new(owner.clone()));
                let mut outcomes = tokio::time::timeout_at(
                    deadline,
                    io::network(client.read_objects(vec![current])),
                )
                .await
                .map_err(|_| anyhow::anyhow!("native metadata fetch deadline exceeded"))??;
                let Some(MetadataOutcome::Object(verified)) = outcomes.pop() else {
                    anyhow::bail!("native canonical metadata is unavailable")
                };
                ensure!(
                    outcomes.is_empty() && verified.descriptor().key == current,
                    "native metadata response changed identity"
                );
                let bytes = verified.canonical_bytes().to_vec();
                let object = m::decode(&bytes, self.inner.model_limits())?;
                crate::closure::Node {
                    descriptor: verified.descriptor(),
                    bytes,
                    object,
                }
            };
            if let m::Object::Symlink(target) = &node.object {
                let ledger = self.inner.ledger.clone();
                let head = root.key();
                let reference =
                    io::blocking_owned(owner.clone(), move || ledger.reference(&head, current))
                        .await?;
                if let Some(parent) = reference.and_then(|reference| reference.parent) {
                    let parent = self
                        .inner
                        .local
                        .read_metadata_with_owner(parent.descriptor()?.key, owner.clone())
                        .await
                        .map_err(backend)?
                        .into_object();
                    if let m::Object::Tree(tree) = parent {
                        for entry in tree.entries {
                            if let m::TreeValue::Symlink { id, target_bytes } = entry.value {
                                if id.0 == current.id.0 {
                                    ensure!(
                                        target_bytes == target.len() as u64,
                                        "native symlink target length mismatch"
                                    );
                                }
                            }
                        }
                    }
                }
            }
            canonical_bytes = canonical_bytes
                .checked_add(node.bytes.len())
                .context("native metadata byte overflow")?;
            ensure!(
                canonical_bytes <= self.inner.config.closure.max_metadata_bytes,
                "native metadata byte budget exceeded"
            );
            pending.push((current, true));
            if let m::Object::CopyHistory(copy) = &node.object {
                ensure!(
                    pending.len().saturating_add(copy.parents.len())
                        <= self.inner.config.closure.max_edges,
                    "native copy metadata edge budget exceeded"
                );
                for parent in copy.parents.iter().rev() {
                    let parent = m::ObjectKey {
                        kind: m::Kind::CopyHistory,
                        id: m::ObjectId(parent.0),
                    };
                    if !done.contains(&parent) {
                        if matches!(root, Authority::CloudVersion(_) | Authority::CloudQuery(_)) {
                            cloud_parents.entry(parent).or_insert(current);
                        }
                        pending.push((parent, false));
                    }
                }
            }
            nodes.insert(current, node);
        }
        let head = root.clone();
        let plan: Vec<_> = nodes
            .values()
            .map(|node| (node.descriptor, node.object.clone()))
            .collect();
        let ledger = self.inner.ledger.clone();
        let cloud = matches!(root, Authority::CloudVersion(_) | Authority::CloudQuery(_));
        io::blocking_owned(owner.clone(), move || {
            if cloud {
                ledger.verified_cloud_metadata_batch(&head, &plan)
            } else {
                ledger.check_verified_authority_batch(&head, &plan)
            }
        })
        .await?;
        for current in &order {
            check(deadline)?;
            let node = &nodes[current];
            if self
                .inner
                .local
                .descriptor(*current)
                .await
                .map_err(backend)?
                .is_none()
            {
                let descriptor = self
                    .inner
                    .local
                    .write_metadata_with_owner(node.object.clone(), owner.clone())
                    .await
                    .map_err(backend)?;
                ensure!(
                    descriptor == node.descriptor,
                    "native metadata installation changed identity"
                );
            }
        }
        // Cloud provenance was already committed before local publication.
        // Still complete dependency/context checks above and install every copy
        // parent before a successful backend metadata return, even on cache hits.
        if cloud {
            check(deadline)?;
            return self
                .inner
                .local
                .read_metadata_with_owner(key, owner)
                .await
                .map_err(backend);
        }
        // Legacy sparse root/query ordering is unchanged in this slice.
        let mut grant_order = vec![key];
        let mut recorded = HashSet::new();
        while let Some(current) = grant_order.pop() {
            if !recorded.insert(current) {
                continue;
            }
            let node = &nodes[&current];
            let object = node.object.clone();
            let descriptor = node.descriptor;
            let ledger = self.inner.ledger.clone();
            let head = root.clone();
            io::blocking_owned(owner.clone(), move || {
                ledger.verified_authority(&head, descriptor, &object)
            })
            .await?;
            if let m::Object::CopyHistory(copy) = &node.object {
                grant_order.extend(copy.parents.iter().map(|id| m::ObjectKey {
                    kind: m::Kind::CopyHistory,
                    id: m::ObjectId(id.0),
                }));
            }
        }
        check(deadline)?;
        self.inner
            .local
            .read_metadata_with_owner(key, owner)
            .await
            .map_err(backend)
    }
}
