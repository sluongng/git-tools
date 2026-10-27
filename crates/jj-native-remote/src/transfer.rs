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

/// Fixed, non-sensitive phase information for a failed native capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeCaptureStage {
    BoundarySelection,
    FrontierCollect,
    StagingUpload,
}
impl NativeCaptureStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BoundarySelection => "boundary_selection",
            Self::FrontierCollect => "frontier_collect",
            Self::StagingUpload => "staging_upload",
        }
    }
}
/// Reasons are assigned only at known local failure sites, never from RPC text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeCaptureReason {
    RetainedBoundaryUnavailable,
    NonresidentHistoryUnavailable,
    NonresidentFileRootUnavailable,
    ScopeOpenUnconfirmed,
    ScopeCloseUnconfirmed,
    Unknown,
}
impl NativeCaptureReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RetainedBoundaryUnavailable => "retained_boundary_unavailable",
            Self::NonresidentHistoryUnavailable => "nonresident_history_unavailable",
            Self::NonresidentFileRootUnavailable => "nonresident_file_root_unavailable",
            Self::ScopeOpenUnconfirmed => "scope_open_unconfirmed",
            Self::ScopeCloseUnconfirmed => "scope_close_unconfirmed",
            Self::Unknown => "unknown",
        }
    }
}
/// An anyhow context preserves the original error's downcasts and source chain.
/// This context's Display/Debug contain only fixed enum values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeCaptureFailure {
    stage: NativeCaptureStage,
    reason: NativeCaptureReason,
}
impl NativeCaptureFailure {
    pub fn new(stage: NativeCaptureStage, reason: NativeCaptureReason) -> Self {
        Self { stage, reason }
    }
    pub fn stage(self) -> NativeCaptureStage {
        self.stage
    }
    pub fn reason(self) -> NativeCaptureReason {
        self.reason
    }
    pub fn attach(self, error: anyhow::Error) -> anyhow::Error {
        error.context(self)
    }
    pub(crate) fn at(stage: NativeCaptureStage, error: anyhow::Error) -> anyhow::Error {
        let reason = error
            .downcast_ref::<Self>()
            .map_or(NativeCaptureReason::Unknown, |e| e.reason);
        Self::new(stage, reason).attach(error)
    }
}
impl std::fmt::Display for NativeCaptureFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "native capture {} failed ({})",
            self.stage.as_str(),
            self.reason.as_str()
        )
    }
}

#[path = "capture_frontier.rs"]
mod capture_frontier;

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

    /// Capture with a portable landing has a sequential lookup/import phase.
    /// Its independent codec pool overlaps the ordinary exact-metadata pool and
    /// one private encode/put, but drops before catalog/graph allocation. Charge
    /// that phase explicitly rather than adding it to unrelated graph peaks.
    pub fn native_landing_capture_resources(
        &self,
        source: &dyn NativeObjectIo,
        limits: &NativeTransferLimits,
    ) -> Result<ControlResources> {
        let mut resources = self.native_transfer_resources(source, limits)?;
        let remote = &self.inner.config.remote.limits;
        let landing = 256u64
            .checked_mul(1024 * 1024)
            .and_then(|n| n.checked_add(remote.max_buffered_bytes as u64))
            .and_then(|n| n.checked_add((remote.max_metadata_bytes as u64).checked_mul(8)?))
            .and_then(|n| n.checked_add(tinytree_cloud::PortableLanding::MAX_WORKING_BYTES as u64))
            .and_then(|n| n.checked_add((limits.max_roots as u64).checked_mul(256)?))
            .ok_or_else(limit)?;
        resources.memory_bytes = resources.memory_bytes.max(landing);
        if resources.memory_bytes > limits.max_memory_bytes {
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
        let deadline = io::bounded_deadline(deadline);
        self.native_transfer_resources(source.as_ref(), &limits)?;
        if roots.is_empty() || roots.len() > limits.max_roots {
            return Err(limit());
        }
        if io::scope().is_some() {
            return Err(Status::failed_precondition(
                "native transfer requires independent host admission",
            )
            .into());
        }
        let permit = tokio::time::timeout_at(
            deadline,
            self.inner.background_transfer.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("native transfer admission deadline exceeded"))??;
        let scope = Arc::new(io::ControlScope::background_transfer(
            permit,
            deadline,
            owner.clone(),
        ));
        let control = self.clone();
        // At most one independently funded closure transfer per repository;
        // foreground control remains serialized on its separate lane. Never spawn tasks
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

    /// Stage a private capture's bounded local frontier. Fresh provider-created
    /// retained boundaries terminate traversal before reads; descriptor presence
    /// never does. This creates no holds or public references. Exact checkout
    /// reads may extend existing source grants; they create no new retention
    /// authority. This is not proof that a complete cloud version was published.
    pub async fn transfer_native_capture_frontier(
        &self,
        roots: Vec<m::ObjectKey>,
        owner: Owner,
        limits: NativeTransferLimits,
        originaldeadline: tokio::time::Instant,
    ) -> Result<NativeTransferReceipt> {
        self.transfer_native_capture_frontier_with_landing(
            roots,
            None,
            owner,
            limits,
            originaldeadline,
        )
        .await
    }

    /// The supplied intent is immutable capsule data, not authenticated job or
    /// retention authority. Exact lookup and canonical reads occur only inside
    /// the independently owned background transfer below.
    pub async fn transfer_native_capture_frontier_with_landing(
        &self,
        roots: Vec<m::ObjectKey>,
        landing: Option<tinytree_cloud::PortableLanding>,
        owner: Owner,
        limits: NativeTransferLimits,
        originaldeadline: tokio::time::Instant,
    ) -> Result<NativeTransferReceipt> {
        if let Some(intent) = &landing {
            intent.validate()?;
        }
        let deadline = io::bounded_deadline(originaldeadline);
        let source: Arc<dyn NativeObjectIo> = self.inner.local.clone();
        // Exact nonresident TREE/SYMLINK reads use the provider's bounded SDK
        // profile, never its generic descriptor/ancestry hydration. Fund that
        // profile even though FILE upload snapshots remain strictly local.
        let read_profile = crate::RemoteObjectIo {
            inner: self.inner.clone(),
        };
        if landing.is_some() {
            self.native_landing_capture_resources(&read_profile, &limits)?;
        } else {
            self.native_transfer_resources(&read_profile, &limits)?;
        }

        if roots.is_empty() || roots.len() > limits.max_roots {
            return Err(limit());
        }
        if io::scope().is_some() {
            return Err(Status::failed_precondition(
                "native capture transfer requires independent host admission",
            )
            .into());
        }
        let permit = tokio::time::timeout_at(
            deadline,
            self.inner.background_transfer.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("native capture admission deadline exceeded"))??;
        let scope = Arc::new(io::ControlScope::background_transfer(
            permit,
            deadline,
            owner.clone(),
        ));
        let control = self.clone();
        struct Completed {
            result: Result<NativeTransferReceipt>,
            _scope: Arc<io::ControlScope>,
        }
        let completed = io::owned(async move {
            let result = io::control_scope(scope.clone(), async {
                tokio::time::timeout_at(deadline, async {
                    let physical: Owner = scope.clone();
                    let boundaries = control
                        .capture_boundaries(
                            &roots,
                            landing.as_ref(),
                            physical.clone(),
                            &limits,
                            deadline,
                        )
                        .await
                        .map_err(|e| {
                            NativeCaptureFailure::at(NativeCaptureStage::BoundarySelection, e)
                        })?;
                    let closure =
                        capture_frontier::collect(&control, roots, &boundaries, &limits, physical)
                            .await
                            .map_err(|e| {
                                NativeCaptureFailure::at(NativeCaptureStage::FrontierCollect, e)
                            })?;
                    let result = control
                        .upload_collected(source, closure, owner, limits, deadline)
                        .await
                        .map_err(|e| {
                            NativeCaptureFailure::at(NativeCaptureStage::StagingUpload, e)
                        });
                    drop(boundaries);
                    result
                })
                .await
                .map_err(|_| {
                    Status::deadline_exceeded("native capture transfer deadline exceeded")
                })?
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

    /// Stage only an explicit linear source stack and its Merkle changed-path
    /// frontier. The base is protected by a complete server-side private hold,
    /// never inferred from descriptor presence. Equal subtrees need no walk or
    /// FILE reads. Legacy ancestry uses a separately bounded auxiliary frontier;
    /// unsupported/over-limit graphs fail closed without a full-history fallback. This creates no public destination reference.
    pub async fn transfer_native_stack(
        &self,
        source: Arc<dyn NativeObjectIo>,
        base: m::CommitId,
        stack: Vec<m::CommitId>,
        owner: Owner,
        limits: NativeTransferLimits,
        deadline: tokio::time::Instant,
    ) -> Result<NativeTransferReceipt> {
        self.transfer_stack_owned(source, base, stack, owner, limits, deadline, false)
            .await
    }
    /// Same configured repository only. Exact-key metadata fetch follows the
    /// held base/frozen canonical references; it does not create generic provider
    /// grants or invoke another provider's whole-closure hydration machinery.
    pub async fn stage_landing_stack(
        &self,
        base: m::CommitId,
        stack: Vec<m::CommitId>,
        owner: Owner,
        limits: NativeTransferLimits,
        deadline: tokio::time::Instant,
    ) -> Result<NativeTransferReceipt> {
        self.transfer_stack_owned(
            self.inner.local.clone(),
            base,
            stack,
            owner,
            limits,
            deadline,
            true,
        )
        .await
    }
    async fn transfer_stack_owned(
        &self,
        source: Arc<dyn NativeObjectIo>,
        base: m::CommitId,
        stack: Vec<m::CommitId>,
        owner: Owner,
        limits: NativeTransferLimits,
        deadline: tokio::time::Instant,
        remote_metadata: bool,
    ) -> Result<NativeTransferReceipt> {
        let deadline = io::bounded_deadline(deadline);
        self.native_transfer_resources(source.as_ref(), &limits)?;
        if stack.is_empty()
            || stack.len() > 128
            || stack.len() > limits.max_roots
            || stack.contains(&base)
            || stack.iter().any(|id| *id == m::CommitId::ROOT)
            || stack.iter().collect::<HashSet<_>>().len() != stack.len()
        {
            return Err(Status::invalid_argument("invalid native landing stack").into());
        }
        if io::scope().is_some() {
            return Err(Status::failed_precondition(
                "native stack transfer requires independent host admission",
            )
            .into());
        }
        // Admission and proof refresh are bounded by the SAME original deadline.
        if base != m::CommitId::ROOT {
            self.run_owned(deadline, owner.clone(), self.admit_commit(base))
                .await?;
        }
        let permit = tokio::time::timeout_at(deadline, self.inner.control.clone().acquire_owned())
            .await
            .map_err(|_| Status::deadline_exceeded("native stack admission deadline exceeded"))??;
        let scope = Arc::new(io::ControlScope::new(
            permit,
            self.inner.control.clone(),
            deadline,
            owner.clone(),
        ));
        let control = self.clone();
        struct Completed {
            result: Result<NativeTransferReceipt>,
            _scope: Arc<io::ControlScope>,
        }
        let completed = io::owned(async move {
            let result = io::control_scope(scope.clone(), async {
                tokio::time::timeout_at(deadline, async {
                    let physical: Owner = scope.clone();
                    let closure = collect_stack(
                        &control,
                        &source,
                        base,
                        stack,
                        &limits,
                        physical,
                        remote_metadata,
                    )
                    .await?;
                    control
                        .upload_collected(source, closure, owner, limits, deadline)
                        .await
                })
                .await
                .map_err(|_| Status::deadline_exceeded("native stack transfer deadline exceeded"))?
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
        self.upload_collected(source, closure, owner, limits, deadline)
            .await
    }

    async fn upload_collected(
        &self,
        source: Arc<dyn NativeObjectIo>,
        closure: closure::Closure,
        owner: Owner,
        _limits: NativeTransferLimits,
        deadline: tokio::time::Instant,
    ) -> Result<NativeTransferReceipt> {
        let physical: Owner = io::scope().context("native transfer admission missing")?;
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

// Old-tree nodes are retained and charged too: comparing a huge base directory
// is not free just because none of its FILE bodies are downloaded.
struct StackFrontier<'a> {
    source: &'a Arc<dyn NativeObjectIo>,
    remote: Option<&'a RemoteRepositoryControl>,
    owner: Owner,
    limits: &'a NativeTransferLimits,
    config: &'a crate::Config,
    cache: HashMap<m::ObjectKey, closure::Node>,
    selected: HashSet<m::ObjectKey>,
    files: HashMap<m::ObjectId, (u64, m::ObjectId)>,
    total_files: u64,
    bytes: usize,
    edges: usize,
    contexts: usize,
    context_bytes: usize,
}
impl StackFrontier<'_> {
    async fn read(&mut self, key: m::ObjectKey) -> Result<m::Object> {
        io::check_deadline()?;
        if let Some(node) = self.cache.get(&key) {
            return Ok(node.object.clone());
        }
        if self.cache.len() + self.files.len() >= self.limits.closure.max_objects {
            return Err(limit());
        }
        let resident = self
            .source
            .descriptor_with_owner(key, self.owner.clone())
            .await
            .map_err(backend)?;
        let (descriptor, bytes, object) = if let Some(descriptor) = resident {
            // Direct canonical stores may allow larger metadata than this
            // transfer. Reject before source decode or retained copies, not
            // after allocating an object outside the frontier reservation.
            self.charge_metadata(key, descriptor)?;
            let value = self
                .source
                .read_metadata_with_owner(key, self.owner.clone())
                .await
                .map_err(backend)?;
            ensure!(
                value.descriptor() == descriptor,
                "native stack source descriptor changed"
            );
            (descriptor, value.bytes().to_vec(), value.into_object())
        } else if let Some(control) = self.remote {
            let client = control.physical_client().await?;
            let mut outcomes = io::network(client.read_objects(vec![key])).await?;
            let Some(tinytree_client::MetadataOutcome::Object(value)) = outcomes.pop() else {
                return Err(Status::not_found("native landing metadata unavailable").into());
            };
            ensure!(
                outcomes.is_empty(),
                "native landing metadata count mismatch"
            );
            let descriptor = value.descriptor();
            // SDK decoding owns its separate bounded pool; reserve our retained
            // frontier BEFORE copying its verified bytes/model out of that pool.
            self.charge_metadata(key, descriptor)?;
            (
                descriptor,
                value.canonical_bytes().to_vec(),
                value.object().clone(),
            )
        } else {
            return Err(Status::not_found("native stack metadata unavailable").into());
        };
        add(
            &mut self.edges,
            object.references().len(),
            self.limits.closure.max_edges,
        )?;
        self.cache.insert(
            key,
            closure::Node {
                descriptor,
                bytes,
                object: object.clone(),
            },
        );
        Ok(object)
    }
    fn charge_metadata(&mut self, key: m::ObjectKey, descriptor: m::Descriptor) -> Result<()> {
        ensure!(
            descriptor.key == key && descriptor.format_version == m::FORMAT_VERSION,
            "native stack metadata descriptor mismatch"
        );
        let size = usize::try_from(descriptor.size_bytes).map_err(|_| limit())?;
        if size > self.config.remote.limits.max_metadata_bytes {
            return Err(limit());
        }
        add(
            &mut self.bytes,
            size,
            self.limits.closure.max_metadata_bytes,
        )
    }
    async fn tree(&mut self, id: m::TreeId) -> Result<m::Tree> {
        match self
            .read(m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(id.0),
            })
            .await?
        {
            m::Object::Tree(tree) => Ok(tree),
            _ => anyhow::bail!("native stack tree kind mismatch"),
        }
    }
    // Copy history contains only copy-parent edges, not arbitrary commit history.
    // This auxiliary frontier is independently bounded by the shared object/edge
    // allowance; malformed cycles are rejected by canonical graph validation on
    // server retention admission (and cannot bypass the upload budget here).
    async fn auxiliary(&mut self, root: m::ObjectKey) -> Result<()> {
        let mut pending = vec![root];
        while let Some(key) = pending.pop() {
            if !self.selected.insert(key) {
                continue;
            }
            let object = self.read(key).await?;
            match object {
                m::Object::Symlink(_) => {}
                m::Object::CopyHistory(copy) => {
                    pending.extend(copy.parents.into_iter().map(|id| m::ObjectKey {
                        kind: m::Kind::CopyHistory,
                        id: m::ObjectId(id.0),
                    }))
                }
                _ => {
                    return Err(Status::unimplemented(
                        "unsupported native landing auxiliary history",
                    )
                    .into());
                }
            }
            if pending.len() > self.limits.closure.max_edges {
                return Err(limit());
            }
        }
        Ok(())
    }
    async fn legacy_commits(
        &mut self,
        control: &RemoteRepositoryControl,
        root: m::CommitId,
        protected: &mut HashSet<m::CommitId>,
        used: &mut usize,
    ) -> Result<()> {
        let key = |id: m::CommitId| m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(id.0),
        };
        let mut pending = vec![(root, false)];
        let mut visiting = HashSet::new();
        while let Some((id, exit)) = pending.pop() {
            if id == m::CommitId::ROOT || protected.contains(&id) {
                continue;
            }
            if !exit {
                if !visiting.insert(id) {
                    return Err(Status::invalid_argument(
                        "cyclic native landing auxiliary history",
                    )
                    .into());
                }
                add(used, 1, 128)?;
                // A complete remote hold is a safe boundary, never descriptor
                // presence. Failure to admit a LOCAL node does not authorize an
                // unbounded history walk: the explicit auxiliary limit applies.
                let client = control.physical_client().await?;
                // Presence is only a reason to ATTEMPT complete admission. Do
                // not create durable failed hold intents for missing local-only
                // predecessors; those would exhaust the shared replay ledger.
                let missing = io::network(client.find_missing(vec![key(id)])).await?;
                if missing.is_empty() {
                    match control.admit_commit(id).await {
                        Ok(_) => {
                            protected.insert(id);
                            visiting.remove(&id);
                            continue;
                        }
                        Err(error)
                            if error.downcast_ref::<Status>().is_some_and(|s| {
                                matches!(
                                    s.code(),
                                    tonic::Code::NotFound | tonic::Code::FailedPrecondition
                                )
                            }) => {}
                        Err(error) => return Err(error),
                    }
                }
                if self
                    .source
                    .descriptor_with_owner(key(id), self.owner.clone())
                    .await
                    .map_err(backend)?
                    .is_none()
                {
                    return Err(Status::unimplemented(
                        "unprotected native auxiliary commit is not locally available",
                    )
                    .into());
                }
            }
            let m::Object::Commit(commit) = self.read(key(id)).await? else {
                anyhow::bail!("native auxiliary commit kind mismatch")
            };
            if commit.parents.len() != 1 || commit.root_tree.terms.len() != 1 {
                return Err(Status::unimplemented(
                    "native landing auxiliary history must be resolved and linear",
                )
                .into());
            }
            if exit {
                let parent = commit.parents[0];
                let old = if parent == m::CommitId::ROOT {
                    None
                } else {
                    let m::Object::Commit(parent) = self.read(key(parent)).await? else {
                        anyhow::bail!("native auxiliary parent kind mismatch")
                    };
                    if parent.root_tree.terms.len() != 1 {
                        return Err(Status::unimplemented(
                            "conflicted native auxiliary parent unsupported",
                        )
                        .into());
                    }
                    Some(parent.root_tree.terms[0])
                };
                self.trees(old, commit.root_tree.terms[0]).await?;
                self.selected.insert(key(id));
                visiting.remove(&id);
                protected.insert(id);
            } else {
                pending.push((id, true));
                for next in commit.parents.into_iter().chain(commit.predecessors).rev() {
                    if next != m::CommitId::ROOT && !protected.contains(&next) {
                        pending.push((next, false));
                    }
                }
                if pending.len() > self.limits.closure.max_edges {
                    return Err(limit());
                }
            }
        }
        Ok(())
    }
    async fn trees(&mut self, old: Option<m::TreeId>, new: m::TreeId) -> Result<()> {
        if old == Some(new) {
            return Ok(());
        }
        add(&mut self.contexts, 1, self.limits.closure.max_path_contexts)?;
        let mut pending = vec![(old, new, String::new())];
        while let Some((old, new, path)) = pending.pop() {
            let old_tree = match old {
                Some(id) => self.tree(id).await?,
                None => m::Tree::default(),
            };
            let new_tree = self.tree(new).await?;
            self.selected.insert(m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(new.0),
            });
            for entry in new_tree.entries {
                let old_value = old_tree
                    .entries
                    .binary_search_by(|e| e.name.as_bytes().cmp(entry.name.as_bytes()))
                    .ok()
                    .map(|n| &old_tree.entries[n].value);
                if old_value == Some(&entry.value) {
                    continue;
                }
                let child_len = path
                    .len()
                    .checked_add(usize::from(!path.is_empty()))
                    .and_then(|n| n.checked_add(entry.name.len()))
                    .ok_or_else(limit)?;
                if child_len > self.limits.closure.max_path_bytes {
                    return Err(limit());
                }
                match entry.value {
                    m::TreeValue::Tree(id) => {
                        let old = match old_value {
                            Some(m::TreeValue::Tree(id)) => Some(*id),
                            _ => None,
                        };
                        // Charge both processed and queued contexts BEFORE
                        // copying a prefix into another pending String. Limits
                        // are shared by the entire stack/auxiliary frontier.
                        add(&mut self.contexts, 1, self.limits.closure.max_path_contexts)?;
                        add(
                            &mut self.context_bytes,
                            child_len,
                            self.limits.closure.max_path_context_bytes,
                        )?;
                        let child_path = if path.is_empty() {
                            entry.name
                        } else {
                            format!("{path}/{}", entry.name)
                        };
                        pending.push((old, id, child_path));
                    }
                    m::TreeValue::File {
                        id,
                        size_bytes,
                        copy_id,
                        ..
                    } => {
                        // Mode/copy-only changes don't require reading the body.
                        let same_file = matches!(old_value, Some(m::TreeValue::File { id: old, size_bytes: size, .. }) if *old == id && *size == size_bytes);
                        if !same_file {
                            insert_file(
                                &mut self.files,
                                &mut self.total_files,
                                m::ObjectId(id.0),
                                size_bytes,
                                m::ObjectId(new.0),
                                self.cache.len(),
                                self.limits,
                            )?;
                        }
                        if let Some(copy) = copy_id {
                            let same_copy = matches!(old_value, Some(m::TreeValue::File { copy_id: Some(old), .. }) if *old == copy);
                            if !same_copy {
                                self.auxiliary(m::ObjectKey {
                                    kind: m::Kind::CopyHistory,
                                    id: m::ObjectId(copy.0),
                                })
                                .await?;
                            }
                        }
                    }
                    m::TreeValue::Symlink { id, target_bytes } => {
                        let key = m::ObjectKey {
                            kind: m::Kind::Symlink,
                            id: m::ObjectId(id.0),
                        };
                        self.auxiliary(key).await?;
                        let m::Object::Symlink(target) = &self.cache[&key].object else {
                            unreachable!()
                        };
                        ensure!(
                            target.len() as u64 == target_bytes,
                            "native stack symlink length mismatch"
                        );
                    }
                }
            }
            if pending.len() > self.limits.closure.max_path_contexts {
                return Err(limit());
            }
        }
        Ok(())
    }
}
async fn collect_stack(
    control: &RemoteRepositoryControl,
    source: &Arc<dyn NativeObjectIo>,
    base: m::CommitId,
    stack: Vec<m::CommitId>,
    limits: &NativeTransferLimits,
    owner: Owner,
    remote_metadata: bool,
) -> Result<closure::Closure> {
    let mut walker = StackFrontier {
        source,
        remote: remote_metadata.then_some(control),
        owner,
        limits,
        config: &control.inner.config,
        cache: HashMap::new(),
        selected: HashSet::new(),
        files: HashMap::new(),
        total_files: 0,
        bytes: 0,
        edges: 0,
        contexts: 0,
        context_bytes: 0,
    };
    let key = |id: m::CommitId| m::ObjectKey {
        kind: m::Kind::Commit,
        id: m::ObjectId(id.0),
    };
    let mut old_tree = if base == m::CommitId::ROOT {
        None
    } else {
        let m::Object::Commit(commit) = walker.read(key(base)).await? else {
            anyhow::bail!("native stack base kind mismatch")
        };
        if commit.root_tree.terms.len() != 1 {
            return Err(Status::unimplemented("conflicted native landing base unsupported").into());
        }
        Some(commit.root_tree.terms[0])
    };
    let mut previous = base;
    let mut protected = HashSet::from([base]);
    let mut auxiliary_commits = 0;
    for id in stack {
        let m::Object::Commit(commit) = walker.read(key(id)).await? else {
            anyhow::bail!("native stack commit kind mismatch")
        };
        if commit.parents != [previous]
            || commit.root_tree.terms.len() != 1
            || commit.secure_signature.is_some()
        {
            return Err(Status::unimplemented(
                "native landing requires an unsigned resolved linear stack",
            )
            .into());
        }
        for predecessor in &commit.predecessors {
            if protected.contains(predecessor) {
                continue;
            }
            walker
                .legacy_commits(
                    control,
                    *predecessor,
                    &mut protected,
                    &mut auxiliary_commits,
                )
                .await?;
        }
        walker.selected.insert(key(id));
        walker.trees(old_tree, commit.root_tree.terms[0]).await?;
        old_tree = Some(commit.root_tree.terms[0]);
        protected.insert(id);
        previous = id;
    }
    let nodes = walker
        .cache
        .into_iter()
        .filter(|(key, _)| walker.selected.contains(key))
        .collect::<HashMap<_, _>>();
    // Upload order is not closure authority: native staging accepts individual
    // canonical objects; Start atomically acquires and verifies complete roots.
    let order = nodes.keys().copied().collect();
    Ok(closure::Closure {
        nodes,
        order,
        files: walker
            .files
            .into_iter()
            .map(|(id, (size, tree))| (id, size, tree))
            .collect(),
    })
}
