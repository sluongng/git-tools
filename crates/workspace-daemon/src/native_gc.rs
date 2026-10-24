//! Explicit local-complete checkpoints. No reachable offload or history expiry.
use crate::{
    config::{Config, NativeGc, NativeGcPolicy},
    native_capture::Target,
};
use anyhow::{Context, Result, ensure};
use jj_lib::object_id::ObjectId;
use native_retention::{CommandOwner, OwnedRootCheckpoint, RootCheckpoint};
use native_workspace_admin::capture::{self, CaptureInput};
use std::{
    fs::File,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Constructed only while the daemon owns its state lock, before opening any
/// native provider. Existing native state cannot be retroactively enrolled.
pub(crate) struct FreshCohort(());
pub(crate) fn fresh_cohort(config: &Config) -> Result<Option<FreshCohort>> {
    if config.native.workspaces.gc.policy != NativeGcPolicy::LocalComplete {
        return Ok(None);
    }
    if config
        .state_dir
        .join("native-commands/identity.json")
        .try_exists()?
    {
        // The command host validates its persisted policy before reopening.
        return Ok(None);
    }
    for name in [
        "native",
        "native-remote",
        "native-workspaces",
        "native-commands",
    ] {
        ensure!(
            !config.state_dir.join(name).try_exists()?,
            "local-complete collection requires a fresh native cohort; existing state is preserved"
        );
    }
    Ok(Some(FreshCohort(())))
}

pub(crate) struct Coordinator {
    pub(crate) config: NativeGc,
    active: Arc<Semaphore>,
    attempted: AtomicU64,
    clean: AtomicU64,
    blocked: AtomicU64,
    last: Mutex<&'static str>,
    last_collection: Mutex<&'static str>,
}
impl Coordinator {
    pub(crate) fn new(config: NativeGc) -> Arc<Self> {
        Arc::new(Self {
            config,
            active: Arc::new(Semaphore::new(1)),
            attempted: AtomicU64::new(0),
            clean: AtomicU64::new(0),
            blocked: AtomicU64::new(0),
            last: Mutex::new("not_attempted"),
            last_collection: Mutex::new("not_attempted"),
        })
    }
    pub(crate) fn select(&self) -> Option<Arc<OwnedSemaphorePermit>> {
        if self.config.policy != NativeGcPolicy::LocalComplete {
            return None;
        }
        match self.active.clone().try_acquire_owned() {
            Ok(permit) => {
                self.attempted.fetch_add(1, Ordering::Relaxed);
                Some(Arc::new(permit))
            }
            Err(_) => {
                self.disposition("busy", false);
                None
            }
        }
    }
    pub(crate) fn disposition(&self, category: &'static str, clean: bool) {
        if clean {
            self.clean.fetch_add(1, Ordering::Relaxed);
        } else {
            self.blocked.fetch_add(1, Ordering::Relaxed);
        }
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = category;
    }
    pub(crate) fn status(&self) -> serde_json::Value {
        serde_json::json!({"policy":self.config.policy,"automatic_collection":false,
            "attempted_checkpoints":self.attempted.load(Ordering::Relaxed),
            "clean_checkpoints":self.clean.load(Ordering::Relaxed),
            "blocked_checkpoints":self.blocked.load(Ordering::Relaxed),
            "last_checkpoint":*self.last.lock().unwrap_or_else(|e|e.into_inner()),
            "last_collection":*self.last_collection.lock().unwrap_or_else(|e|e.into_inner())})
    }
    fn collection_status(&self, category: &'static str) {
        *self
            .last_collection
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = category;
    }
}

pub(crate) async fn collect(
    coordinator: Arc<Coordinator>,
    registry: native_retention::Registry,
    store: Arc<jj_native_backend::DirectObjectIo>,
    apply: bool,
) -> Result<serde_json::Value> {
    ensure!(
        coordinator.config.policy == NativeGcPolicy::LocalComplete,
        "native collection is disabled"
    );
    let deadline = Instant::now() + std::time::Duration::from_millis(coordinator.config.timeout_ms);
    let collector = match tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        registry.try_acquire_collector(),
    )
    .await
    {
        Ok(Ok(collector)) => Arc::new(collector),
        Ok(Err(error)) => {
            let category = match error {
                native_retention::Error::Busy => "busy",
                native_retention::Error::DirtyWorkspace => "dirty_workspace",
                native_retention::Error::UntrackedAccess => "untracked_access",
                native_retention::Error::Limit(_) => "root_limit",
                _ => "registry_unavailable",
            };
            coordinator.collection_status(category);
            anyhow::bail!("native collection blocked: {category}");
        }
        Err(_) => {
            coordinator.collection_status("deadline");
            anyhow::bail!("native collection deadline elapsed");
        }
    };
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?;
    let roots = collector.roots().len();
    if roots > coordinator.config.max_roots {
        coordinator.collection_status("root_limit");
        anyhow::bail!("native collection blocked: root_limit");
    }
    let options = jj_native_backend::NativeClientGcOptions {
        complete_roots: collector.roots().to_vec(),
        transient_roots: vec![],
        now,
        grace_seconds: coordinator.config.grace_seconds,
        limits: coordinator.config.limits(),
        deadline,
    };
    let result = if apply {
        store.client_gc_with_owner(options, collector).await.map(|report|serde_json::json!({
            "status":"collected","root_count":roots,"deleted_objects":report.deleted_objects,
            "deleted_logical_bytes":report.deleted_bytes,"unlinked_payloads":report.unlinked_payloads,
            "deferred_deletions":report.deferred_deletions,"stamped_unknown_objects":report.stamped_unknown_objects,
            "expired_leases":report.expired_leases}))
    } else {
        store.plan_client_gc_with_owner(options, collector).await.map(|report|serde_json::json!({
            "status":"planned","root_count":roots,"would_delete_objects":report.would_delete_objects,
            "would_delete_logical_bytes":report.would_delete_bytes,"pending_deletion_objects":report.pending_deletion_objects,
            "pending_deletion_logical_bytes":report.pending_deletion_bytes,"would_stamp_unknown_objects":report.would_stamp_unknown_objects,
            "would_expire_leases":report.would_expire_leases}))
    };
    match result {
        Ok(value) => {
            coordinator.collection_status(if apply { "collected" } else { "planned" });
            Ok(value)
        }
        Err(error) => {
            use jj_native_backend::NativeClientGcError as E;
            let category = match error {
                E::Busy => "busy",
                E::Deadline => "deadline",
                E::InvalidLimits => "work_limit",
                E::Store(tinytree_store::Error::MissingObject(_)) => "incomplete_root",
                E::Store(tinytree_store::Error::Limit(_)) => "work_limit",
                E::Store(tinytree_store::Error::Corrupt(_)) => "corrupt_local_store",
                E::Store(_) => "store_unavailable",
                _ => "worker_unavailable",
            };
            coordinator.collection_status(category);
            anyhow::bail!("native collection blocked: {category}")
        }
    }
}
struct CompletionOnDrop(Arc<crate::native_capture::Completion>);
impl Drop for CompletionOnDrop {
    fn drop(&mut self) {
        self.0.complete();
    }
}
// Release frontend/index payloads first, then the exact kernel/command/admission
// owners, and only then signal that physical checkpoint ownership has drained.
struct Lifetime {
    _kernel: File,
    _command: Arc<CommandOwner>,
    _permit: Arc<OwnedSemaphorePermit>,
    _complete: CompletionOnDrop,
}

pub(crate) async fn checkpoint(
    coordinator: Arc<Coordinator>,
    target: Arc<Target>,
    command: Arc<CommandOwner>,
    kernel: File,
    permit: Arc<OwnedSemaphorePermit>,
    completion: Arc<crate::native_capture::Completion>,
    deadline: Instant,
) -> Result<OwnedRootCheckpoint> {
    let lifetime = Arc::new(Lifetime {
        _kernel: kernel,
        _command: command,
        _permit: permit,
        _complete: CompletionOnDrop(completion),
    });
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .context("local checkpoint deadline elapsed")?;
    let controller = target
        .controller
        .upgrade()
        .context("local checkpoint controller unavailable")?;
    let mut limits = capture::CaptureLimits::default();
    limits.max_operations = coordinator.config.max_operations;
    limits.max_working_bytes = coordinator.config.max_checkpoint_bytes;
    limits.max_roots = coordinator.config.max_roots;
    limits.max_elapsed_ms = u64::try_from(remaining.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let state_cap = limits
        .max_file_bytes
        .min(limits.max_decode_bytes.saturating_sub(4096) / 32);
    let (wc, projection) =
        jj_vfs::native::read_retained_state_for_capture(&target.binding.projection(), state_cap)?;
    ensure!(!wc.pending, "pending local working-copy state");
    drop(wc);
    let controller = controller
        .capture_for_backup_until(&projection, tokio::time::Instant::from_std(deadline))
        .await?;
    ensure!(
        Instant::now() < deadline,
        "local checkpoint deadline elapsed"
    );
    let proof = capture::TreeFreezePermit::from_controller(controller, lifetime)?;
    let input = CaptureInput {
        registry: target.registry.clone(),
        binding: target.binding.clone(),
        index_config: target.index.clone(),
        local_objects: target.local.clone(),
        remote_scope: target.remote_scope.clone(),
    };
    // There is at most one accepted checkpoint. The blocking owner contains the
    // real frontend and writer fences, even if a logical waiter is cancelled.
    tokio::task::spawn_blocking(move || {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .context("local checkpoint deadline elapsed")?;
        limits.max_elapsed_ms = u64::try_from(remaining.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(2)
            .build()
            .context("local checkpoint reader runtime unavailable")?;
        runtime.block_on(async move {
            let inventory = capture::read_local_root_inventory(input, proof, limits)
                .await?
                .seal_quiescent()?;
            ensure!(
                Instant::now() < deadline,
                "local checkpoint deadline elapsed"
            );
            let checkpoint = RootCheckpoint {
                roots: inventory.roots().to_vec(),
                op_heads: inventory
                    .operation_heads()
                    .iter()
                    .map(|id| id.as_bytes().to_vec())
                    .collect(),
                working_copy_operation: Some(
                    inventory.working_copy_operation().as_bytes().to_vec(),
                ),
            };
            Ok(OwnedRootCheckpoint::new(checkpoint, Arc::new(inventory)))
        })
    })
    .await
    .context("local checkpoint reader failed")?
}
