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
tokio::task_local! {static CONTROL_LEASE:Arc<tokio::sync::OwnedSemaphorePermit>;}
pub(crate) fn in_control() -> bool {
    CONTROL_LEASE.try_with(|_| ()).is_ok()
}
pub(crate) async fn control_scope<T>(
    lease: Arc<tokio::sync::OwnedSemaphorePermit>,
    future: impl Future<Output = T>,
) -> T {
    CONTROL_LEASE.scope(lease, future).await
}
pub(crate) async fn blocking<T: Send + 'static>(
    job: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    let lease = CONTROL_LEASE.try_with(Arc::clone).ok();
    let (result, _lease) = runtime()?
        .spawn_blocking(move || {
            let result = job();
            (result, lease)
        })
        .await?;
    result
}
struct OwnedFile {
    file: std::fs::File,
    _lease: Arc<CachedBlob>,
}
pub(crate) struct Reader {
    source: Option<OwnedFile>,
    pending: Option<JoinHandle<(OwnedFile, io::Result<Vec<u8>>)>>,
    bytes: Vec<u8>,
    offset: usize,
    eof: bool,
}
impl Reader {
    pub async fn open(lease: Arc<CachedBlob>) -> anyhow::Result<Self> {
        let source = blocking(move || {
            let file = std::fs::File::open(lease.path())?;
            Ok(OwnedFile {
                file,
                _lease: lease,
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
