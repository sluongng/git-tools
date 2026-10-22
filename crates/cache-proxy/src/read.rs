//! Shared admission for live cached-blob descriptors, independent of hydration.
use super::*;
use std::{
    future::Future,
    io::{self, Read, Seek, SeekFrom},
    task::{Context, Poll, ready},
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    sync::{OwnedSemaphorePermit, Semaphore, watch},
    task::JoinHandle,
};

pub(super) struct Admission {
    slots: Arc<Semaphore>,
    limit: u32,
    queued: AtomicU64,
}
struct Queued(Arc<Admission>);
impl Drop for Queued {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::Relaxed);
    }
}
impl Admission {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(limit)),
            limit: limit as u32,
            queued: AtomicU64::new(0),
        }
    }
    pub(super) fn in_use(&self) -> u64 {
        self.limit as u64 - self.slots.available_permits() as u64
    }
    pub(super) fn queued(&self) -> u64 {
        self.queued.load(Ordering::Relaxed)
    }
    async fn acquire(
        self: &Arc<Self>,
        mut stop: watch::Receiver<bool>,
    ) -> Result<OwnedSemaphorePermit, Status> {
        if *stop.borrow() {
            return Err(Status::unavailable("proxy shutting down"));
        }
        self.queued.fetch_add(1, Ordering::Relaxed);
        let _queued = Queued(self.clone());
        let permit = tokio::select! {
            biased;
            _ = stop.changed() => return Err(Status::unavailable("proxy shutting down")),
            permit = self.slots.clone().acquire_owned() => permit.map_err(|_| Status::unavailable("proxy shutting down"))?,
        };
        // Stop can race the permit wakeup. A granted permit remains owned until
        // this check, so an all-permits shutdown barrier cannot overtake it.
        if *stop.borrow() {
            return Err(Status::unavailable("proxy shutting down"));
        }
        Ok(permit)
    }
    pub(super) async fn wait_for_idle(&self) -> Result<()> {
        let _idle = self.slots.clone().acquire_many_owned(self.limit).await?;
        Ok(())
    }
    pub(super) async fn open(
        self: &Arc<Self>,
        blob: Arc<local_cas::CachedBlob>,
        stop: watch::Receiver<bool>,
        offset: u64,
    ) -> Result<Reader, Status> {
        let permit = self.acquire(stop).await?;
        // The queued/running blocking job owns BOTH the cache lease and permit.
        // Dropping its JoinHandle never releases admission while it can open a
        // descriptor; an abandoned output drops the complete owner at finish.
        let owner = tokio::task::spawn_blocking(move || {
            let mut file =
                File::open(blob.path()).map_err(|error| io_status(&error, "opening cache blob"))?;
            file.seek(SeekFrom::Start(offset))
                .map_err(|error| io_status(&error, "seeking cache blob"))?;
            Ok::<_, Status>(Owner {
                file,
                _blob: blob,
                _permit: permit,
            })
        })
        .await
        .map_err(|_| Status::internal("opening cache blob"))??;
        Ok(Reader {
            owner: Some(owner),
            pending: None,
            bytes: vec![],
            position: 0,
            eof: false,
        })
    }
    pub(super) async fn read_all(
        self: &Arc<Self>,
        blob: Arc<local_cas::CachedBlob>,
        stop: watch::Receiver<bool>,
    ) -> Result<Vec<u8>, Status> {
        let permit = self.acquire(stop).await?;
        tokio::task::spawn_blocking(move || {
            let file = File::open(blob.path())
                .map_err(|error| io_status(&error, "reading verified cache blob"))?;
            let mut owner = Owner {
                file,
                _blob: blob,
                _permit: permit,
            };
            let mut bytes = Vec::new();
            let result = owner
                .file
                .read_to_end(&mut bytes)
                .map_err(|error| io_status(&error, "reading verified cache blob"));
            // Close the descriptor before releasing the pin and read permit,
            // including when the asynchronous caller has already cancelled.
            drop(owner);
            result?;
            Ok(bytes)
        })
        .await
        .map_err(|_| Status::internal("reading verified cache blob"))?
    }
}

// Fields drop left-to-right: the FD closes before cache pin/admission release.
struct Owner {
    file: File,
    _blob: Arc<local_cas::CachedBlob>,
    _permit: OwnedSemaphorePermit,
}
pub(super) struct Reader {
    owner: Option<Owner>,
    pending: Option<JoinHandle<(Owner, io::Result<Vec<u8>>)>>,
    bytes: Vec<u8>,
    position: usize,
    eof: bool,
}
impl AsyncRead for Reader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if let Some(task) = this.pending.as_mut() {
                let result = ready!(Pin::new(task).poll(cx));
                this.pending = None;
                let (owner, result) = match result {
                    Ok(result) => result,
                    Err(_) => {
                        this.eof = true;
                        return Poll::Ready(Err(io::Error::other("cache read worker failed")));
                    }
                };
                this.owner = Some(owner);
                match result {
                    Ok(bytes) => {
                        this.eof = bytes.is_empty();
                        this.bytes = bytes;
                        this.position = 0;
                    }
                    Err(error) => {
                        this.eof = true;
                        return Poll::Ready(Err(error));
                    }
                }
            }
            if this.position < this.bytes.len() {
                let n = output.remaining().min(this.bytes.len() - this.position);
                output.put_slice(&this.bytes[this.position..this.position + n]);
                this.position += n;
                return Poll::Ready(Ok(()));
            }
            if this.eof {
                return Poll::Ready(Ok(()));
            }
            let mut owner = this
                .owner
                .take()
                .expect("live cache reader owns a descriptor");
            let size = output.remaining().min(64 * 1024);
            this.pending = Some(tokio::task::spawn_blocking(move || {
                let mut bytes = vec![0; size];
                // Match Tokio's blocking-file behavior: an interrupted syscall
                // retries inside this same owned job, without releasing its
                // descriptor/pin/permit or restarting any operation budget.
                let result = loop {
                    match owner.file.read(&mut bytes) {
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        result => break result,
                    }
                }
                .map(|n| {
                    bytes.truncate(n);
                    bytes
                });
                (owner, result)
            }));
        }
    }
}

enum BodyMessage {
    Data(Vec<u8>),
    Finished(Result<(), Status>),
}
async fn stopped(stop: &mut watch::Receiver<bool>) {
    if !*stop.borrow_and_update() {
        let _ = stop.changed().await;
    }
}
/// The RPC body holds only the receiver/watch, never a cached descriptor. The
/// admitted producer owns the entire reader/codec and remains independently
/// cancellable even when HTTP/2 flow control stops polling the response body.
pub(super) fn body(
    reader: Pin<Box<dyn AsyncRead + Send>>,
    mut stop: watch::Receiver<bool>,
) -> RpcStream<bs::ReadResponse> {
    let (send, mut receive) = tokio::sync::mpsc::channel(1);
    let mut response_stop = stop.clone();
    tokio::spawn(async move {
        let mut reader = reader;
        let mut bytes = vec![0; 64 * 1024];
        loop {
            let read = tokio::select! {
                biased;
                _ = stopped(&mut stop) => break,
                _ = send.closed() => break,
                read = reader.read(&mut bytes) => read,
            };
            let (message, terminal) = match read {
                Ok(0) => (BodyMessage::Finished(Ok(())), true),
                Ok(n) => (BodyMessage::Data(bytes[..n].to_vec()), false),
                Err(error) => (
                    BodyMessage::Finished(Err(io_status(&error, "reading cache blob"))),
                    true,
                ),
            };
            let sent = tokio::select! {
                biased;
                _ = stopped(&mut stop) => false,
                _ = send.closed() => false,
                sent = send.send(message) => sent.is_ok(),
            };
            if !sent || terminal {
                break;
            }
        }
        // A cancelled read future can leave a blocking job owning the tuple;
        // Reader::drop releases only idle ownership, never that job's permit.
        drop(reader);
    });
    Box::pin(async_stream::stream! {
        loop {
            let message = tokio::select! {
                biased;
                _ = stopped(&mut response_stop) => {
                    yield Err(Status::unavailable("proxy shutting down"));
                    return;
                },
                message = receive.recv() => message,
            };
            match message {
                Some(BodyMessage::Data(data)) => yield Ok(bs::ReadResponse { data }),
                Some(BodyMessage::Finished(Ok(()))) => return,
                Some(BodyMessage::Finished(Err(status))) => {
                    yield Err(status);
                    return;
                },
                None => {
                    // Never turn producer panic/cancellation into a truncated
                    // successful EOF, even if some bytes were already sent.
                    yield Err(Status::internal("cache response task stopped"));
                    return;
                },
            }
        }
    })
}

pub(super) fn descriptor_exhausted(error: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(error.raw_os_error(), Some(libc::EMFILE | libc::ENFILE))
    }
    #[cfg(not(unix))]
    {
        let _ = error;
        false
    }
}
pub(super) fn io_status(error: &std::io::Error, message: &'static str) -> Status {
    if descriptor_exhausted(error) {
        Status::resource_exhausted("local file descriptor limit reached")
    } else {
        Status::internal(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct Bytes;
    #[async_trait]
    impl BlobSource for Bytes {
        async fn fetch(&self, _: &Artifact, target: &mut File) -> Result<()> {
            target.write_all(b"read fixture")?;
            Ok(())
        }
        async fn missing(&self, _: &[Artifact]) -> Result<Vec<Artifact>> {
            Ok(vec![])
        }
    }

    #[test]
    fn cancelled_blocking_jobs_keep_descriptor_admission_and_cache_pin_until_idle() {
        #[derive(Clone, Copy)]
        enum Case {
            QueuedOpen,
            QueuedRead,
        }
        for case in [Case::QueuedOpen, Case::QueuedRead] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .max_blocking_threads(1)
                .build()
                .unwrap();
            runtime.block_on(async {
                let directory = tempfile::tempdir().unwrap();
                let cache = LocalCas::open(
                    directory.path(),
                    Arc::new(Bytes),
                    local_cas::CacheConfig::default(),
                )
                .unwrap();
                let artifact = Artifact {
                    backend: "neutral-test-scope".into(),
                    instance: String::new(),
                    digest_function: 9,
                    hash: blake3::hash(b"read fixture").to_hex().to_string(),
                    size: b"read fixture".len() as u64,
                };
                let blob = cache.clone().content(artifact).await.unwrap();
                cache.wait_for_idle().await.unwrap();
                let lease = Arc::downgrade(&blob);
                let admission = Arc::new(Admission::new(1));
                let (stop, _) = watch::channel(false);
                let mut reader = match case {
                    Case::QueuedOpen => None,
                    Case::QueuedRead => Some(
                        admission
                            .open(blob.clone(), stop.subscribe(), 0)
                            .await
                            .unwrap(),
                    ),
                };
                // Occupy the only blocking thread after hydration, so cancellation
                // is deterministically between job admission and actual OS I/O.
                let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
                let (release_tx, release_rx) = std::sync::mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    let _ = ready_tx.send(());
                    let _ = release_rx.recv();
                });
                ready_rx.await.unwrap();
                match case {
                    Case::QueuedOpen => {
                        let owned = admission.clone();
                        let receiver = stop.subscribe();
                        let request =
                            tokio::spawn(async move { owned.open(blob, receiver, 0).await });
                        tokio::time::timeout(Duration::from_secs(5), async {
                            while admission.in_use() != 1 {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .unwrap();
                        request.abort();
                        match request.await {
                            Err(error) => assert!(error.is_cancelled()),
                            Ok(_) => panic!("cancelled open unexpectedly completed"),
                        }
                    }
                    Case::QueuedRead => {
                        drop(blob);
                        let mut reader = reader.take().unwrap();
                        let mut bytes = [0; 4];
                        {
                            let pending = reader.read(&mut bytes);
                            tokio::pin!(pending);
                            assert!(
                                std::future::poll_fn(|cx| Poll::Ready(
                                    pending.as_mut().poll(cx).is_pending()
                                ))
                                .await
                            );
                        }
                        drop(reader);
                    }
                }
                assert_eq!(admission.in_use(), 1);
                assert_eq!(admission.queued(), 0);
                assert!(lease.upgrade().is_some());
                stop.send_replace(true);
                let idle = admission.wait_for_idle();
                tokio::pin!(idle);
                assert!(
                    std::future::poll_fn(|cx| Poll::Ready(idle.as_mut().poll(cx).is_pending()))
                        .await
                );
                release_tx.send(()).unwrap();
                blocker.await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), idle)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(admission.in_use(), 0);
                assert!(lease.upgrade().is_none());
            });
        }
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_exhaustion_is_resource_exhausted_without_overriding_typed_status() {
        for errno in [libc::EMFILE, libc::ENFILE] {
            let status = cache_status(io::Error::from_raw_os_error(errno).into());
            assert_eq!(status.code(), tonic::Code::ResourceExhausted);
            assert_eq!(status.message(), "local file descriptor limit reached");
            assert_eq!(
                io_status(&io::Error::from_raw_os_error(errno), "opening cache blob").code(),
                tonic::Code::ResourceExhausted
            );
        }
        let upstream = anyhow::Error::new(Status::permission_denied("neutral upstream status"))
            .context(io::Error::from_raw_os_error(libc::EMFILE));
        let status = cache_status(upstream);
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), "neutral upstream status");
        assert_eq!(
            cache_status(anyhow::anyhow!("digest mismatch")).code(),
            tonic::Code::DataLoss
        );
        assert_eq!(
            io_status(
                &io::Error::new(io::ErrorKind::InvalidData, "neutral private detail"),
                "reading cache blob"
            )
            .message(),
            "reading cache blob"
        );
    }
}
