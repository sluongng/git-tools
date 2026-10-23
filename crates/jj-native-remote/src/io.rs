//! Bounded blocking reads retain their cache lease after caller cancellation.
use futures::io::AsyncRead;
use local_cas::CachedBlob;
use std::{
    future::Future,
    io::{self, Read},
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll, ready},
};
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinHandle,
};
fn runtime() -> io::Result<&'static Runtime> {
    static RT: OnceLock<io::Result<Runtime>> = OnceLock::new();
    RT.get_or_init(|| {
        Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(8)
            .thread_name("native-remote-io")
            .enable_all()
            .build()
    })
    .as_ref()
    .map_err(|_| io::Error::other("native remote I/O runtime unavailable"))
}
#[derive(Default)]
pub(crate) struct OwnedWork {
    active: std::sync::atomic::AtomicUsize,
    changed: tokio::sync::Notify,
}
impl OwnedWork {
    pub fn enter(self: &Arc<Self>) -> WorkTicket {
        self.active
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        WorkTicket(self.clone())
    }
    pub async fn wait_for_idle(&self) {
        loop {
            let changed = self.changed.notified();
            if self.active.load(std::sync::atomic::Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
}
pub(crate) struct WorkTicket(Arc<OwnedWork>);
impl Drop for WorkTicket {
    fn drop(&mut self) {
        if self
            .0
            .active
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel)
            == 1
        {
            self.0.changed.notify_waiters();
        }
    }
}
/// Owned provider work remains alive when its waiter is cancelled.
pub(crate) async fn owned<T: Send + 'static>(
    future: impl Future<Output = anyhow::Result<T>> + Send + 'static,
) -> anyhow::Result<T> {
    runtime()?.spawn(future).await?
}
pub(crate) async fn blocking_owned<T: Send + 'static>(
    owner: Arc<dyn std::any::Any + Send + Sync>,
    job: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    blocking(move || {
        let _owner = owner;
        job()
    })
    .await
}
pub(crate) struct ControlScope {
    _lease: Option<tokio::sync::OwnedSemaphorePermit>,
    _parent: Option<Arc<ControlScope>>,
    pub deadline: tokio::time::Instant,
    _owner: Arc<dyn std::any::Any + Send + Sync>,
}
impl ControlScope {
    pub fn with_owner(parent: Arc<Self>, owner: Arc<dyn std::any::Any + Send + Sync>) -> Arc<Self> {
        Arc::new(Self {
            deadline: parent.deadline,
            _lease: None,
            _parent: Some(parent),
            _owner: owner,
        })
    }
    pub fn new(
        lease: tokio::sync::OwnedSemaphorePermit,
        deadline: tokio::time::Instant,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        Self {
            _lease: Some(lease),
            _parent: None,
            deadline,
            _owner: owner,
        }
    }
}
tokio::task_local! {static CONTROL_SCOPE:Arc<ControlScope>;}
pub(crate) fn scope() -> Option<Arc<ControlScope>> {
    CONTROL_SCOPE.try_with(Arc::clone).ok()
}
pub(crate) fn in_control() -> bool {
    scope().is_some()
}
pub(crate) fn check_deadline() -> anyhow::Result<()> {
    if scope().is_some_and(|scope| tokio::time::Instant::now() >= scope.deadline) {
        return Err(
            tonic::Status::deadline_exceeded("native remote operation deadline exceeded").into(),
        );
    }
    Ok(())
}
pub(crate) async fn control_scope<T>(
    scope: Arc<ControlScope>,
    future: impl Future<Output = T>,
) -> T {
    CONTROL_SCOPE.scope(scope, future).await
}
pub(crate) async fn network<T>(
    future: impl Future<Output = Result<T, tonic::Status>>,
) -> Result<T, tonic::Status> {
    if let Some(scope) = scope() {
        if tokio::time::Instant::now() >= scope.deadline {
            return Err(tonic::Status::deadline_exceeded(
                "native remote operation deadline exceeded",
            ));
        }
        let result = tokio::time::timeout_at(scope.deadline, future)
            .await
            .map_err(|_| {
                tonic::Status::deadline_exceeded("native remote operation deadline exceeded")
            })?;
        if tokio::time::Instant::now() >= scope.deadline {
            return Err(tonic::Status::deadline_exceeded(
                "native remote operation deadline exceeded",
            ));
        }
        result
    } else {
        future.await
    }
}
pub(crate) async fn cached<T>(
    future: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    if let Some(scope) = scope() {
        check_deadline()?;
        let result = tokio::time::timeout_at(scope.deadline, future)
            .await
            .map_err(|_| tonic::Status::deadline_exceeded("native cache wait deadline exceeded"))?;
        check_deadline()?;
        result
    } else {
        future.await
    }
}
pub(crate) async fn blocking<T: Send + 'static>(
    job: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    let lease = scope();
    let (result, _lease) = runtime()?
        .spawn_blocking(move || {
            let result = job();
            (result, lease)
        })
        .await?;
    check_deadline()?;
    result
}
enum ReadOwner {
    Cached {
        file: std::fs::File,
        _lease: Arc<CachedBlob>,
    },
    Verified(tinytree_client::VerifiedReader),
}
impl Read for ReadOwner {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Cached { file, .. } => file.read(bytes),
            Self::Verified(reader) => reader.read(bytes),
        }
    }
}
struct OwnedFile {
    file: ReadOwner,
    _scope: Option<Arc<ControlScope>>,
    _delivery: Option<Arc<dyn std::any::Any + Send + Sync>>,
}
pub(crate) struct Reader {
    source: Option<OwnedFile>,
    pending: Option<JoinHandle<(OwnedFile, io::Result<Vec<u8>>)>>,
    bytes: Vec<u8>,
    offset: usize,
    eof: bool,
}
impl Reader {
    pub fn verified(
        reader: tinytree_client::VerifiedReader,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        Self {
            source: Some(OwnedFile {
                file: ReadOwner::Verified(reader),
                _scope: scope(),
                _delivery: Some(owner),
            }),
            pending: None,
            bytes: Vec::new(),
            offset: 0,
            eof: false,
        }
    }
    pub async fn open(lease: Arc<CachedBlob>) -> anyhow::Result<Self> {
        Self::open_owned(lease, None).await
    }
    pub async fn open_for_delivery(
        lease: Arc<CachedBlob>,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> anyhow::Result<Self> {
        Self::open_owned(lease, Some(owner)).await
    }
    async fn open_owned(
        lease: Arc<CachedBlob>,
        owner: Option<Arc<dyn std::any::Any + Send + Sync>>,
    ) -> anyhow::Result<Self> {
        let scope = scope();
        let source = blocking(move || {
            let file = std::fs::File::open(lease.path())?;
            Ok(OwnedFile {
                file: ReadOwner::Cached {
                    file,
                    _lease: lease,
                },
                _scope: scope,
                _delivery: owner,
            })
        })
        .await?;
        Ok(Self {
            source: Some(source),
            pending: None,
            bytes: Vec::new(),
            offset: 0,
            eof: false,
        })
    }
}
impl AsyncRead for Reader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if output.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            if let Some(task) = this.pending.as_mut() {
                let (source, result) = ready!(Pin::new(task).poll(cx))
                    .map_err(|_| io::Error::other("native cached read worker failed"))?;
                this.pending = None;
                this.source = Some(source);
                this.bytes = result?;
                this.offset = 0;
                this.eof = this.bytes.is_empty();
                if this.eof {
                    this.source = None;
                }
            }
            if this.offset < this.bytes.len() {
                let n = output.len().min(this.bytes.len() - this.offset);
                output[..n].copy_from_slice(&this.bytes[this.offset..this.offset + n]);
                this.offset += n;
                return Poll::Ready(Ok(n));
            }
            if this.eof {
                return Poll::Ready(Ok(0));
            }
            let mut source = this
                .source
                .take()
                .ok_or_else(|| io::Error::other("native cached read lost source"))?;
            let count = output.len().min(65536);
            this.pending = Some(runtime()?.spawn_blocking(move || {
                let mut bytes = vec![0; count];
                let result = source.file.read(&mut bytes).map(|n| {
                    bytes.truncate(n);
                    bytes
                });
                (source, result)
            }));
        }
    }
}
