//! Bounded target staging without target publication or read grants.
use crate::{ClosureLimits, RemoteRepositoryControl, backend, closure, io};
use anyhow::{Context, Result, ensure};
use jj_native_backend::NativeObjectIo;
use native_broker::ControlResources;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tinytree_model as m;
use tonic::Status;

type Owner = Arc<dyn Any + Send + Sync>;

/// Bounds for one full canonical walk. Public ancestry may be hydrated; this is
/// not a thin missing-closure protocol. Source and target pools remain bounded
/// independently and are included in the declared admission below.
#[derive(Clone)]
pub struct NativeTransferLimits {
    pub max_roots: usize,
    pub closure: ClosureLimits,
    pub max_files: usize,
    pub max_file_bytes: u64,
    pub max_total_file_bytes: u64,
    pub max_memory_bytes: u64,
    pub max_temporary_bytes: u64,
}
impl Default for NativeTransferLimits {
    fn default() -> Self {
        Self {
            max_roots: 1024,
            closure: Default::default(),
            max_files: 5000,
            max_file_bytes: 64 * 1024 * 1024,
            max_total_file_bytes: 1024 * 1024 * 1024,
            max_memory_bytes: 1024 * 1024 * 1024,
            max_temporary_bytes: 512 * 1024 * 1024,
        }
    }
}
#[derive(Debug)]
pub struct NativeTransferStage {
    pub descriptor: m::Descriptor,
    pub staging_lease: String,
    pub expires_at_unix_seconds: i64,
}
/// Uploaded objects have individual expiring native stages. Already-present
/// objects have NO new lease. This is not retention, complete-closure assurance,
/// a cloud publication token, backup completion, or permission to evict a source.
pub struct NativeTransferReceipt {
    stages: Vec<NativeTransferStage>,
    present_objects: usize,
    metadata_objects: usize,
    file_objects: usize,
    // Payloads cannot escape this independent caller admission/root owner. Never
    // retain a completed command token here; callers supply a fresh read fence.
    _owner: Owner,
}
impl std::fmt::Debug for NativeTransferReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeTransferReceipt")
            .field("uploaded_objects", &self.stages.len())
            .field("present_objects", &self.present_objects)
            .finish_non_exhaustive()
    }
}
impl NativeTransferReceipt {
    pub fn stages(&self) -> &[NativeTransferStage] {
        &self.stages
    }
    pub fn present_objects(&self) -> usize {
        self.present_objects
    }
    pub fn metadata_objects(&self) -> usize {
        self.metadata_objects
    }
    pub fn file_objects(&self) -> usize {
        self.file_objects
    }
}
fn limit() -> anyhow::Error {
    Status::resource_exhausted("native transfer budget exceeded").into()
}
fn add(used: &mut usize, n: usize, max: usize) -> Result<()> {
    *used = used
        .checked_add(n)
        .filter(|n| *n <= max)
        .ok_or_else(limit)?;
    Ok(())
}
impl RemoteRepositoryControl {
    /// Pure preflight, before source/network/staging I/O. The trusted caller must
    /// reserve this entire declared worst-case work and include that reservation
    /// plus an independent pending-root read fence in `owner`. This method is not
    /// an existing broker control RPC with its smaller single-provider charge.
    /// `None` source work uses NativeObjectIo's bounded direct-one-object contract.
    pub fn native_transfer_resources(
        &self,
        source: &dyn NativeObjectIo,
        limits: &NativeTransferLimits,
    ) -> Result<ControlResources> {
        let actual = &self.inner.config;
        let a = &actual.closure;
        let b = &limits.closure;
        if limits.max_roots == 0
            || limits.max_roots > b.max_objects
            || b.max_objects == 0
            || b.max_objects > a.max_objects
            || b.max_edges == 0
            || b.max_edges > a.max_edges
            || b.max_metadata_bytes == 0
            || b.max_metadata_bytes > a.max_metadata_bytes
            || b.max_path_contexts == 0
            || b.max_path_contexts > a.max_path_contexts
            || b.max_path_context_bytes == 0
            || b.max_path_context_bytes > a.max_path_context_bytes
            || b.max_path_bytes == 0
            || b.max_path_bytes > a.max_path_bytes
            || limits.max_files == 0
            || limits.max_files > b.max_objects
            || limits.max_file_bytes > actual.remote.limits.max_file_bytes
            || limits.max_total_file_bytes < limits.max_file_bytes
        {
            return Err(Status::invalid_argument("invalid native transfer limits").into());
        }
        // Reuse the existing whole-control closure/container/codec/spool bound;
        // omitting ledger work here would only lower this conservative estimate.
        // The source's complete hydration profile is ADDITIVE, not hidden inside
        // the target SDK budget. No pool is resized by this helper.
        let own = actual.control_work_budget();
        let source = source.metadata_work_budget().unwrap_or_default();
        let resources = ControlResources {
            memory_bytes: own
                .memory_bytes
                .checked_add(source.memory_bytes)
                .ok_or_else(limit)?,
            temporary_bytes: own
                .temporary_bytes
                .checked_add(source.temporary_bytes)
                .ok_or_else(limit)?,
        };
        if resources.memory_bytes > limits.max_memory_bytes
            || resources.temporary_bytes > limits.max_temporary_bytes
        {
            return Err(limit());
        }
        Ok(resources)
    }

    /// Stage a bounded multi-root closure using ONLY the actual source's read
    /// authority. The transfer itself creates no target holds, references,
    /// KnownReference records or read grants. Source I/O may hydrate/cache objects
    /// and refresh its existing source authority under that provider's policy.
    /// Admission is cancellable; accepted work owns its scope through physical
    /// jobs and is included in wait_for_idle. One absolute deadline covers all
    /// waits and retries, including source I/O and both immutable FILE snapshots.
    pub async fn transfer_native_closure(
        &self,
        source: Arc<dyn NativeObjectIo>,
        roots: Vec<m::ObjectKey>,
        owner: Owner,
        limits: NativeTransferLimits,
        deadline: tokio::time::Instant,
    ) -> Result<NativeTransferReceipt> {
        self.native_transfer_resources(source.as_ref(), &limits)?;
        if roots.is_empty() || roots.len() > limits.max_roots {
            return Err(limit());
        }
        if io::in_control() {
            return Err(Status::failed_precondition(
                "native transfer requires independent host admission",
            )
            .into());
        }
        let permit = tokio::time::timeout_at(deadline, self.inner.control.clone().acquire_owned())
            .await
            .map_err(|_| {
                Status::deadline_exceeded("native transfer admission deadline exceeded")
            })??;
        let scope = Arc::new(io::ControlScope::new(permit, deadline, owner.clone()));
        let control = self.clone();
        // At most one accepted task per configured control; never spawn tasks
        // waiting for admission. Result fields drop before their physical owner.
        struct Completed {
            result: Result<NativeTransferReceipt>,
            _scope: Arc<io::ControlScope>,
        }
        let completed = io::owned(async move {
            let result = io::control_scope(scope.clone(), async {
                tokio::time::timeout_at(
                    deadline,
                    control.transfer_inner(source, roots, owner, limits, deadline),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("native transfer deadline exceeded"))?
            })
            .await;
            Ok(Completed {
                result,
                _scope: scope,
            })
        })
        .await?;
        completed.result
    }

    async fn transfer_inner(
        &self,
        source: Arc<dyn NativeObjectIo>,
        roots: Vec<m::ObjectKey>,
        owner: Owner,
        limits: NativeTransferLimits,
        deadline: tokio::time::Instant,
    ) -> Result<NativeTransferReceipt> {
        let physical: Owner = io::scope().context("native transfer admission missing")?;
        let closure = collect(
            &source,
            roots,
            &limits,
            &self.inner.config,
            physical.clone(),
        )
        .await?;
        let client = self.physical_client().await?;
        let mut descriptors = closure.descriptors();
        descriptors.extend(closure.files.iter().map(|(id, size, _)| m::Descriptor {
            key: m::ObjectKey {
                kind: m::Kind::File,
                id: *id,
            },
            size_bytes: *size,
            format_version: m::FORMAT_VERSION,
        }));
        let batch = self.inner.config.remote.limits.max_batch_items.min(
            self.inner
                .config
                .remote
                .limits
                .max_request_bytes
                .saturating_sub(self.inner.config.remote.repository_id.len() + 8)
                / 40,
        );
        if batch == 0 {
            return Err(limit());
        }
        let mut missing = HashSet::new();
        for batch in descriptors.chunks(batch) {
            for key in
                io::network(client.find_missing(batch.iter().map(|d| d.key).collect())).await?
            {
                missing.insert(key);
            }
        }
        let present_objects = descriptors.len() - missing.len();
        let mut stages = Vec::new();
        for key in &closure.order {
            if missing.contains(key) {
                let node = &closure.nodes[key];
                let object =
                    io::network(client.verify_metadata(node.descriptor, node.bytes.clone()))
                        .await?;
                let receipt = io::network(client.upload_metadata(object)).await?;
                stages.push(NativeTransferStage {
                    descriptor: node.descriptor,
                    staging_lease: receipt.staging_lease,
                    expires_at_unix_seconds: receipt.staging_lease_expiry_unix_seconds,
                });
            }
        }
        for (id, size, _) in &closure.files {
            let descriptor = m::Descriptor {
                key: m::ObjectKey {
                    kind: m::Kind::File,
                    id: *id,
                },
                size_bytes: *size,
                format_version: m::FORMAT_VERSION,
            };
            if !missing.contains(&descriptor.key) {
                continue;
            }
            let read = source
                .read_file_with_owner(m::FileId(id.0), physical.clone())
                .await
                .map_err(backend)?;
            let snapshot = self
                .upload_snapshot(read, descriptor, deadline, &client)
                .await?;
            let receipt = io::network(client.upload_file(snapshot)).await?;
            stages.push(NativeTransferStage {
                descriptor,
                staging_lease: receipt.staging_lease,
                expires_at_unix_seconds: receipt.staging_lease_expiry_unix_seconds,
            });
        }
        io::check_deadline()?;
        Ok(NativeTransferReceipt {
            stages,
            present_objects,
            metadata_objects: closure.nodes.len(),
            file_objects: closure.files.len(),
            _owner: owner,
        })
    }
}

fn insert_file(
    files: &mut HashMap<m::ObjectId, (u64, m::ObjectId)>,
    total: &mut u64,
    id: m::ObjectId,
    size: u64,
    tree: m::ObjectId,
    metadata: usize,
    limits: &NativeTransferLimits,
) -> Result<()> {
    if let Some((old, _)) = files.get(&id) {
        ensure!(*old == size, "native transfer conflicting FILE lengths");
        return Ok(());
    }
    if files.len() >= limits.max_files
        || metadata + files.len() >= limits.closure.max_objects
        || size > limits.max_file_bytes
    {
        return Err(limit());
    }
    *total = total
        .checked_add(size)
        .filter(|n| *n <= limits.max_total_file_bytes)
        .ok_or_else(limit)?;
    files.insert(id, (size, tree));
    Ok(())
}

async fn collect(
    source: &Arc<dyn NativeObjectIo>,
    roots: Vec<m::ObjectKey>,
    limits: &NativeTransferLimits,
    config: &crate::Config,
    owner: Owner,
) -> Result<closure::Closure> {
    let mut nodes = HashMap::new();
    let mut order = Vec::new();
    let mut files = HashMap::<m::ObjectId, (u64, m::ObjectId)>::new();
    let mut total_file_bytes = 0u64;
    let mut expectations = HashMap::new();
    let mut tree_roots = HashSet::new();
    let mut pending = Vec::new();
    for key in roots {
        if key.kind == m::Kind::File {
            let descriptor = source
                .descriptor_with_owner(key, owner.clone())
                .await
                .map_err(backend)?
                .ok_or_else(|| Status::not_found("native transfer FILE root is unavailable"))?;
            ensure!(
                descriptor.key == key && descriptor.format_version == m::FORMAT_VERSION,
                "native transfer root descriptor mismatch"
            );
            insert_file(
                &mut files,
                &mut total_file_bytes,
                key.id,
                descriptor.size_bytes,
                key.id,
                0,
                limits,
            )?;
        } else {
            if key.kind == m::Kind::Tree {
                tree_roots.insert(m::TreeId(key.id.0));
            }
            pending.push((key, false));
        }
    }
    if pending.len() > limits.closure.max_edges {
        return Err(limit());
    }
    let mut visiting = HashSet::new();
    let mut done = HashSet::new();
    let mut bytes = 0usize;
    let mut edges = 0usize;
    while let Some((key, exit)) = pending.pop() {
        io::check_deadline()?;
        if key.kind == m::Kind::Commit && key.id.0 == [0; 32] {
            continue;
        }
        if key.kind == m::Kind::File {
            continue;
        }
        if exit {
            visiting.remove(&key);
            done.insert(key);
            order.push(key);
            continue;
        }
        if done.contains(&key) {
            continue;
        }
        ensure!(!visiting.contains(&key), "native transfer metadata cycle");
        if nodes.len() + files.len() >= limits.closure.max_objects {
            return Err(limit());
        }
        let descriptor = source
            .descriptor_with_owner(key, owner.clone())
            .await
            .map_err(backend)?
            .ok_or_else(|| Status::not_found("native transfer metadata is unavailable"))?;
        ensure!(
            descriptor.key == key && descriptor.format_version == m::FORMAT_VERSION,
            "native transfer descriptor mismatch"
        );
        let size = usize::try_from(descriptor.size_bytes).map_err(|_| limit())?;
        if size > config.remote.limits.max_metadata_bytes {
            return Err(limit());
        }
        add(&mut bytes, size, limits.closure.max_metadata_bytes)?;
        let metadata = source
            .read_metadata_with_owner(key, owner.clone())
            .await
            .map_err(backend)?;
        ensure!(
            metadata.descriptor() == descriptor,
            "native transfer source descriptor changed"
        );
        let contents = metadata.bytes().to_vec();
        let object = metadata.into_object();
        if let m::Object::Commit(commit) = &object {
            tree_roots.extend(commit.root_tree.terms.iter().copied());
        }
        if let m::Object::Tree(tree) = &object {
            for entry in &tree.entries {
                match entry.value {
                    m::TreeValue::File { id, size_bytes, .. } => {
                        insert_file(
                            &mut files,
                            &mut total_file_bytes,
                            m::ObjectId(id.0),
                            size_bytes,
                            key.id,
                            nodes.len() + 1,
                            limits,
                        )?;
                    }
                    m::TreeValue::Symlink { id, target_bytes } => {
                        let child = m::ObjectKey {
                            kind: m::Kind::Symlink,
                            id: m::ObjectId(id.0),
                        };
                        if let Some(old) = expectations.insert(child, target_bytes) {
                            ensure!(
                                old == target_bytes,
                                "native transfer conflicting symlink lengths"
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
        if files.len() > limits.max_files
            || nodes.len() + files.len() + 1 > limits.closure.max_objects
        {
            return Err(limit());
        }
        let refs = object.references();
        add(&mut edges, refs.len(), limits.closure.max_edges)?;
        visiting.insert(key);
        pending.push((key, true));
        for child in refs.into_iter().rev() {
            if child.kind != m::Kind::File
                && !(child.kind == m::Kind::Commit && child.id.0 == [0; 32])
                && !done.contains(&child)
            {
                pending.push((child, false));
            }
        }
        if pending.len() > limits.closure.max_edges {
            return Err(limit());
        }
        nodes.insert(
            key,
            closure::Node {
                descriptor,
                bytes: contents,
                object,
            },
        );
    }
    for (key, size) in expectations {
        let node = nodes.get(&key).context("native transfer missing symlink")?;
        let m::Object::Symlink(target) = &node.object else {
            anyhow::bail!("native transfer symlink kind mismatch");
        };
        ensure!(
            target.len() as u64 == size,
            "native transfer symlink size mismatch"
        );
    }
    let mut configured = config.clone();
    configured.closure = limits.closure.clone();
    closure::validate_contexts(&nodes, tree_roots, &configured, &mut edges)?;
    Ok(closure::Closure {
        nodes,
        order,
        files: files
            .into_iter()
            .map(|(id, (size, tree))| (id, size, tree))
            .collect(),
    })
}
