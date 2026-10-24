//! Explicit host-owned local reclamation; never a root-completeness proof.
use std::{
    any::Any,
    fmt, io,
    sync::{Arc, OnceLock},
    time::Instant,
};
use tinytree_model::ObjectKey;
use tinytree_store::{ClientGcLimits, ClientGcRequest, Store};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{OwnedSemaphorePermit, Semaphore},
};

/// The host must fence ALL roots, including retained history and pending jobs,
/// before constructing this request. No remote promises or hydration are used.
#[derive(Clone, Debug)]
pub struct NativeClientGcOptions {
    pub complete_roots: Vec<ObjectKey>,
    pub transient_roots: Vec<ObjectKey>,
    pub now: i64,
    pub grace_seconds: u64,
    /// May tighten, but never raise, the Store's default logical work caps.
    pub limits: ClientGcLimits,
    /// Original monotonic operation deadline, including admission and waiting.
    pub deadline: Instant,
}

#[derive(Debug)]
pub enum NativeClientGcError {
    Busy,
    /// Accepted physical work can outlive this wait and retains its root fence.
    Deadline,
    InvalidLimits,
    Executor(io::Error),
    Worker(tokio::task::JoinError),
    /// Includes missing root closure, stale epoch and Store work-budget errors.
    Store(tinytree_store::Error),
}
impl fmt::Display for NativeClientGcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => f.write_str("native maintenance is busy"),
            Self::Deadline => f.write_str("native maintenance deadline elapsed"),
            Self::InvalidLimits => f.write_str("native maintenance limits exceed work caps"),
            Self::Executor(e) => write!(f, "native maintenance executor: {e}"),
            Self::Worker(e) => write!(f, "native maintenance worker: {e}"),
            Self::Store(e) => write!(f, "{e}"),
        }
    }
}
impl std::error::Error for NativeClientGcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Executor(e) => Some(e),
            Self::Worker(e) => Some(e),
            Self::Store(e) => Some(e),
            _ => None,
        }
    }
}
struct Maintenance {
    runtime: Runtime,
    slot: Arc<Semaphore>,
}
fn maintenance() -> Result<&'static Maintenance, NativeClientGcError> {
    static MAINTENANCE: OnceLock<io::Result<Maintenance>> = OnceLock::new();
    MAINTENANCE
        .get_or_init(|| {
            Ok(Maintenance {
                runtime: Builder::new_multi_thread()
                    .worker_threads(1)
                    .max_blocking_threads(1)
                    .enable_time()
                    .thread_name("native-maintenance")
                    .build()?,
                slot: Arc::new(Semaphore::new(1)),
            })
        })
        .as_ref()
        .map_err(|e| {
            NativeClientGcError::Executor(io::Error::new(
                e.kind(),
                "maintenance executor unavailable",
            ))
        })
}

// Field destruction order retains the external collector fence until all
// request/result payloads, Store ownership and physical admission are released.
struct Job {
    options: NativeClientGcOptions,
    store: Arc<Store>,
    slot: OwnedSemaphorePermit,
    #[cfg(test)]
    hook: Option<Arc<dyn Fn() + Send + Sync>>,
    owner: Arc<dyn Any + Send + Sync>,
}
struct Completed<T> {
    result: Result<T, NativeClientGcError>,
    _store: Arc<Store>,
    _slot: OwnedSemaphorePermit,
    _owner: Arc<dyn Any + Send + Sync>,
}
fn check_limits(options: &NativeClientGcOptions) -> Result<(), NativeClientGcError> {
    let l = &options.limits;
    let cap = ClientGcLimits::default();
    macro_rules! bounded { ($($field:ident),+ $(,)?) => {
        if $(l.$field > cap.$field)||+ { return Err(NativeClientGcError::InvalidLimits); }
    }; }
    bounded!(
        max_roots,
        max_promises,
        max_catalog_records,
        max_marked_objects,
        max_edges,
        max_metadata_bytes,
        max_path_contexts,
        max_path_context_bytes,
        max_deleted_objects,
        max_deleted_bytes
    );
    if options
        .complete_roots
        .len()
        .checked_add(options.transient_roots.len())
        .is_none_or(|n| n > l.max_roots)
    {
        return Err(NativeClientGcError::InvalidLimits);
    }
    Ok(())
}
pub(crate) async fn collect<T: Send + 'static>(
    store: Arc<Store>,
    options: NativeClientGcOptions,
    owner: Arc<dyn Any + Send + Sync>,
    operation: fn(&Store, &str, &ClientGcRequest) -> tinytree_store::Result<T>,
    #[cfg(test)] hook: Option<Arc<dyn Fn() + Send + Sync>>,
) -> Result<T, NativeClientGcError> {
    if Instant::now() >= options.deadline {
        return Err(NativeClientGcError::Deadline);
    }
    check_limits(&options)?;
    let maintenance = maintenance()?;
    // Never queue collector jobs behind foreground I/O (or another collector).
    let slot = maintenance
        .slot
        .clone()
        .try_acquire_owned()
        .map_err(|_| NativeClientGcError::Busy)?;
    if Instant::now() >= options.deadline {
        return Err(NativeClientGcError::Deadline);
    }
    let deadline = tokio::time::Instant::from_std(options.deadline);
    let job = Job {
        options,
        store,
        slot,
        #[cfg(test)]
        hook,
        owner,
    };
    let physical = maintenance.runtime.spawn_blocking(move || {
        let job = job;
        let result = (|| {
            if Instant::now() >= job.options.deadline {
                return Err(NativeClientGcError::Deadline);
            }
            // Captured only while the host's exclusive collector owner is live.
            let epoch = job
                .store
                .client_gc_epoch(crate::REPOSITORY)
                .map_err(NativeClientGcError::Store)?;
            #[cfg(test)]
            if let Some(hook) = &job.hook {
                hook();
            }
            // Epoch capture can wait on Store's publication gate. Do not begin
            // a sweep already known to be expired; once Store is entered its
            // synchronous bounded cleanup may still outlive the wait deadline.
            if Instant::now() >= job.options.deadline {
                return Err(NativeClientGcError::Deadline);
            }
            let request = ClientGcRequest {
                epoch,
                complete_roots: job.options.complete_roots,
                transient_roots: job.options.transient_roots,
                promised_files: Vec::new(),
                now: job.options.now,
                grace_seconds: job.options.grace_seconds,
                limits: job.options.limits,
            };
            operation(&job.store, crate::REPOSITORY, &request).map_err(NativeClientGcError::Store)
        })();
        Completed {
            result,
            _store: job.store,
            _slot: job.slot,
            _owner: job.owner,
        }
    });
    // Timer and physical executor are independent of the caller's runtime.
    // Timeout drops only the waiter: the blocking job/result still owns the
    // external fence. Cancellation cannot abort an accepted physical sweep.
    maintenance
        .runtime
        .spawn(async move {
            let completed = tokio::time::timeout_at(deadline, physical)
                .await
                .map_err(|_| NativeClientGcError::Deadline)?
                .map_err(NativeClientGcError::Worker)?;
            completed.result
        })
        .await
        .map_err(NativeClientGcError::Worker)?
}
