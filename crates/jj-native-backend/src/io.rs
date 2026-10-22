//! Executor-independent bounded-buffer blocking I/O. A cancelled future cannot
//! release a lease while its blocking read still owns the descriptor.
use crate::{BackendResult, other};
use futures::io::AsyncRead;
use std::{
    future::Future,
    io::{self, Read},
    pin::Pin,
    sync::OnceLock,
    task::{Context, Poll, ready},
};
use tinytree_store::ReadLease;
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinHandle,
};

fn runtime() -> BackendResult<&'static Runtime> {
    static RUNTIME: OnceLock<io::Result<Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(8)
                .thread_name("native-store-io")
                .build()
        })
        .as_ref()
        .map_err(|error| {
            other(io::Error::new(
                error.kind(),
                "native I/O executor unavailable",
            ))
        })
}
pub(crate) async fn blocking<T: Send + 'static>(
    job: impl FnOnce() -> BackendResult<T> + Send + 'static,
) -> BackendResult<T> {
    runtime()?.spawn_blocking(job).await.map_err(other)?
}
pub(crate) struct Reader {
    lease: Option<ReadLease>,
    pending: Option<JoinHandle<(ReadLease, io::Result<Vec<u8>>)>>,
    bytes: Vec<u8>,
    position: usize,
    eof: bool,
}
impl Reader {
    pub(crate) fn new(lease: ReadLease) -> Self {
        Self {
            lease: Some(lease),
            pending: None,
            bytes: vec![],
            position: 0,
            eof: false,
        }
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
                let joined = ready!(Pin::new(task).poll(cx));
                this.pending = None;
                let (lease, result) =
                    joined.map_err(|_| io::Error::other("native read job failed"))?;
                this.lease = Some(lease);
                let bytes = result?;
                this.eof = bytes.is_empty();
                this.bytes = bytes;
                this.position = 0;
                if this.eof {
                    this.lease = None;
                }
            }
            if this.position < this.bytes.len() {
                let count = output.len().min(this.bytes.len() - this.position);
                output[..count].copy_from_slice(&this.bytes[this.position..this.position + count]);
                this.position += count;
                return Poll::Ready(Ok(count));
            }
            if this.eof {
                return Poll::Ready(Ok(0));
            }
            let runtime =
                runtime().map_err(|_| io::Error::other("native I/O executor unavailable"))?;
            let mut lease = this.lease.take().expect("live reader has a lease");
            let count = output.len().min(64 * 1024);
            this.pending = Some(runtime.spawn_blocking(move || {
                let mut bytes = vec![0; count];
                let result = loop {
                    match lease.read(&mut bytes) {
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        result => {
                            break result.map(|count| {
                                bytes.truncate(count);
                                bytes
                            });
                        }
                    }
                };
                (lease, result)
            }));
        }
    }
}
