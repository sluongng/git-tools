use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
type CleanupFailure = (&'static str, &'static str, tonic::Code);
fn operation(scope: bool) -> &'static str {
    if scope { "scope" } else { "query" }
}
struct Progress {
    pending: usize,
    first_failure: Option<CleanupFailure>,
}
struct State {
    progress: std::sync::Mutex<Progress>,
    changed: tokio::sync::Notify,
    finished: std::sync::Condvar,
    capacity: usize,
}
impl State {
    fn fail(&self, failure: CleanupFailure) {
        self.progress
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .first_failure
            .get_or_insert(failure);
        self.changed.notify_waiters();
        self.finished.notify_all();
    }
}
struct Ticket {
    state: Arc<State>,
    failure: Option<CleanupFailure>,
}
impl Drop for Ticket {
    fn drop(&mut self) {
        let mut progress = self
            .state
            .progress
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if progress.pending == 0 {
            progress
                .first_failure
                .get_or_insert(("driver", "counter", tonic::Code::Internal));
        } else {
            progress.pending -= 1
        }
        if let Some(code) = self.failure {
            progress.first_failure.get_or_insert(code);
        }
        drop(progress);
        self.state.changed.notify_waiters();
        self.state.finished.notify_all();
    }
}
struct Item {
    handle: [u8; 32],
    scope: bool,
    owner: QueryOwner,
    deadline: std::time::Instant,
    ticket: Ticket,
}
/// A bounded cleanup driver, independent of the repository/index context.
/// Drop all repositories, streams and helpers before draining. It acknowledges
/// queued logical closes, not durable repository publication or physical GC.
#[derive(Clone)]
pub struct CleanupHandle {
    queue: Queue,
}
impl std::fmt::Debug for CleanupHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeIndexCleanup").finish_non_exhaustive()
    }
}
impl CleanupHandle {
    /// First sticky failure, using only fixed operation/origin categories and
    /// a public status code. Never includes transport messages or opaque IDs.
    pub fn first_failure(&self) -> Option<(&'static str, &'static str, tonic::Code)> {
        self.queue
            .state
            .progress
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .first_failure
    }
    /// Blocking equivalent for CLI exit after its execution runtime has stopped.
    /// Never call while retaining a repository/stream that should be retired.
    pub fn wait(&self, timeout: Duration) -> Result<(), Error> {
        if timeout.is_zero() {
            return Err(Error::Invalid);
        }
        let deadline = std::time::Instant::now() + timeout;
        let mut progress = self
            .queue
            .state
            .progress
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        loop {
            if progress.pending == 0 {
                return progress
                    .first_failure
                    .map_or(Ok(()), |(_, _, code)| Err(Error::Query(code)));
            }
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return Err(Error::Query(tonic::Code::DeadlineExceeded));
            }
            let (next, _) = self
                .queue
                .state
                .finished
                .wait_timeout(progress, left)
                .unwrap_or_else(|e| e.into_inner());
            progress = next;
        }
    }
    /// Requires a Tokio timer, not the repository's channel runtime. A timeout
    /// leaves accepted jobs owning their guards; expiry remains the fallback.
    pub async fn drain(&self, timeout: Duration) -> Result<(), Error> {
        if timeout.is_zero() {
            return Err(Error::Invalid);
        }
        tokio::time::timeout(timeout, async {
            loop {
                let notified = self.queue.state.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let progress = self
                        .queue
                        .state
                        .progress
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    if progress.pending == 0 {
                        return progress
                            .first_failure
                            .map_or(Ok(()), |(_, _, code)| Err(Error::Query(code)));
                    }
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| Error::Query(tonic::Code::DeadlineExceeded))?
    }
}
#[derive(Clone)]
pub(super) struct Queue {
    sender: std::sync::mpsc::SyncSender<Item>,
    state: Arc<State>,
}
impl Queue {
    pub(super) fn start(io: Arc<dyn NativeQueryIo>, limits: &Limits) -> Result<Self, Error> {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<Item>(limits.cleanup_capacity);
        let state = Arc::new(State {
            progress: std::sync::Mutex::new(Progress {
                pending: 0,
                first_failure: None,
            }),
            changed: tokio::sync::Notify::new(),
            finished: std::sync::Condvar::new(),
            capacity: limits.cleanup_capacity,
        });
        let worker_state = state.clone();
        std::thread::Builder::new()
            .name("native-index-cleanup".into())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    worker_state.fail(("driver", "runtime", tonic::Code::Internal));
                    return;
                };
                while let Ok(mut item) = receiver.recv() {
                    let left = item
                        .deadline
                        .saturating_duration_since(std::time::Instant::now());
                    if !left.is_zero() {
                        let io = io.clone();
                        let owner = item.owner.clone();
                        let handle = item.handle;
                        let scope = item.scope;
                        item.ticket.failure = match runtime.block_on(async move {
                            tokio::time::timeout(left, async move {
                                if scope {
                                    io.close_scope(handle, owner).await
                                } else {
                                    io.close_query(handle, owner).await
                                }
                            })
                            .await
                        }) {
                            Ok(Ok(())) => None,
                            Ok(Err(status)) => Some((operation(scope), "rpc", status.code())),
                            Err(_) => {
                                Some((operation(scope), "timeout", tonic::Code::DeadlineExceeded))
                            }
                        };
                    } else {
                        item.ticket.failure = Some((
                            operation(item.scope),
                            "queue_expiry",
                            tonic::Code::DeadlineExceeded,
                        ));
                    }
                    // Owner teardown can enqueue a dependent close. Ticket is last,
                    // so pending never reaches zero before that new work is visible.
                    drop(item);
                }
            })
            .map_err(Error::Io)?;
        Ok(Self { sender, state })
    }
    pub(super) fn handle(&self) -> CleanupHandle {
        CleanupHandle {
            queue: self.clone(),
        }
    }
    pub(super) fn send(
        &self,
        handle: [u8; 32],
        scope: bool,
        owner: QueryOwner,
        timeout: Duration,
    ) -> bool {
        {
            let mut progress = self
                .state
                .progress
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if progress.pending >= self.state.capacity.saturating_add(1) {
                progress.first_failure.get_or_insert((
                    operation(scope),
                    "queue_full",
                    tonic::Code::ResourceExhausted,
                ));
                drop(progress);
                self.state.changed.notify_waiters();
                self.state.finished.notify_all();
                return false;
            }
            progress.pending += 1;
        }
        let item = Item {
            handle,
            scope,
            owner,
            deadline: std::time::Instant::now() + timeout,
            ticket: Ticket {
                state: self.state.clone(),
                failure: Some((operation(scope), "unprocessed", tonic::Code::Cancelled)),
            },
        };
        match self.sender.try_send(item) {
            Ok(()) => true,
            Err(std::sync::mpsc::TrySendError::Full(mut item)) => {
                item.ticket.failure = Some((
                    operation(item.scope),
                    "queue_full",
                    tonic::Code::ResourceExhausted,
                ));
                drop(item);
                false
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(mut item)) => {
                item.ticket.failure = Some((
                    operation(item.scope),
                    "queue_disconnected",
                    tonic::Code::Unavailable,
                ));
                drop(item);
                false
            }
        }
    }
}
pub(super) struct CloseOnce(AtomicBool);
impl Default for CloseOnce {
    fn default() -> Self {
        Self(AtomicBool::new(false))
    }
}
impl CloseOnce {
    pub(super) fn schedule(
        &self,
        queue: &Queue,
        handle: [u8; 32],
        owner: QueryOwner,
        timeout: Duration,
    ) {
        if self
            .0
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            && !queue.send(handle, true, owner, timeout)
        {
            self.0.store(false, Ordering::Release)
        }
    }
}
