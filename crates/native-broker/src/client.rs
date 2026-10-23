use crate::{Config, proto as p, wire};
use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt};
use jj_lib::backend::{BackendError, BackendResult};
use jj_native_backend::{
    CopyCatalogLimits, CopyCatalogSnapshot, FileRead, NativeObjectIo, VerifiedMetadata,
};
use std::{
    future::Future,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
    time::Instant,
};
use tonic::{
    Request, Status,
    codec::CompressionEncoding,
    transport::{Channel, Endpoint},
};
fn runtime() -> BackendResult<&'static Runtime> {
    static RUNTIME: OnceLock<std::io::Result<Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(8)
                .enable_all()
                .thread_name("native-broker-io")
                .build()
        })
        .as_ref()
        .map_err(|_| wire::other(std::io::Error::other("native broker executor unavailable")))
}
struct Task<T>(JoinHandle<Result<T, Status>>);
impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn run<T: Send + 'static>(
    future: impl Future<Output = Result<T, Status>> + Send + 'static,
) -> BackendResult<T> {
    let mut task = Task(runtime()?.spawn(future));
    (&mut task.0)
        .await
        .map_err(|_| wire::other(std::io::Error::other("native broker job failed")))?
        .map_err(wire::other)
}
struct Budget {
    _calls: OwnedSemaphorePermit,
    _memory: OwnedSemaphorePermit,
}
struct Inner {
    channel: Channel,
    store: String,
    spool: PathBuf,
    config: Config,
    calls: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    disk: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct BrokerObjectIo {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for BrokerObjectIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerObjectIo").finish_non_exhaustive()
    }
}
impl BrokerObjectIo {
    /// Connect to a private owner-controlled Unix socket. The spool directory
    /// must already be private/trusted; it never becomes a broker store path.
    pub async fn connect(
        socket: impl AsRef<Path>,
        store: impl Into<String>,
        spool: impl AsRef<Path>,
        config: Config,
    ) -> BackendResult<Self> {
        let socket = socket.as_ref().to_owned();
        let store = store.into();
        let spool = spool.as_ref().to_owned();
        run(async move {
            config.validate()?;
            wire::id(&store)?;
            crate::transport::private_directory(&spool)?;
            #[cfg(unix)]
            let channel = {
                crate::transport::validate_socket(&socket)?;
                let uid = unsafe { libc::geteuid() };
                tokio::time::timeout(
                    config.policy.connect_timeout(),
                    Endpoint::from_static("http://[::]:50051")
                        .connect_timeout(config.policy.connect_timeout())
                        .connect_with_connector(tower::service_fn(move |_: http::Uri| {
                            let socket = socket.clone();
                            async move {
                                let stream = tokio::net::UnixStream::connect(socket).await?;
                                if stream.peer_cred()?.uid() != uid {
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::PermissionDenied,
                                        "broker peer owner mismatch",
                                    ));
                                }
                                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                            }
                        })),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("native broker connect deadline exceeded"))?
                .map_err(|_| Status::unavailable("cannot connect native broker"))?
            };
            #[cfg(not(unix))]
            let channel: Channel = {
                let _ = socket;
                return Err(Status::unimplemented(
                    "private native broker requires Unix transport",
                ));
            };
            Ok(Self {
                inner: Arc::new(Inner {
                    channel,
                    store,
                    spool,
                    calls: Arc::new(Semaphore::new(config.max_inflight)),
                    memory: Arc::new(Semaphore::new(config.max_memory_bytes)),
                    disk: Arc::new(Semaphore::new(config.max_temporary_bytes as usize)),
                    config,
                }),
            })
        })
        .await
    }
}
impl Inner {
    fn client(&self, limit: usize) -> p::object_io_client::ObjectIoClient<Channel> {
        p::object_io_client::ObjectIoClient::new(self.channel.clone())
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(limit)
            .max_encoding_message_size(self.config.max_metadata_bytes + 4096)
    }
    async fn budget(&self, memory: usize) -> Result<Arc<Budget>, Status> {
        let calls = self
            .calls
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("broker client closed"))?;
        let memory = self
            .memory
            .clone()
            .acquire_many_owned(memory as u32)
            .await
            .map_err(|_| Status::unavailable("broker client closed"))?;
        Ok(Arc::new(Budget {
            _calls: calls,
            _memory: memory,
        }))
    }
    fn request<T>(&self, body: T, deadline: Instant) -> Result<Request<T>, Status> {
        let mut r = Request::new(body);
        r.set_timeout(
            deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| Status::deadline_exceeded("broker operation deadline exceeded"))?,
        );
        Ok(r)
    }
    fn object(&self, key: tinytree_model::ObjectKey) -> p::ObjectRequest {
        p::ObjectRequest {
            store_id: self.store.clone(),
            kind: key.kind as u32,
            id: key.id.0.to_vec(),
        }
    }
}
#[async_trait]
impl NativeObjectIo for BrokerObjectIo {
    async fn descriptor(
        &self,
        key: tinytree_model::ObjectKey,
    ) -> BackendResult<Option<tinytree_model::Descriptor>> {
        let inner = self.inner.clone();
        run(async move {
            let deadline = Instant::now() + inner.config.policy.operation_timeout();
            reapi::policy::timeout(deadline, async {
                let _budget = inner.budget(4096).await?;
                let value = inner
                    .client(1024)
                    .descriptor(inner.request(inner.object(key), deadline)?)
                    .await?
                    .into_inner()
                    .descriptor
                    .map(wire::descriptor)
                    .transpose()?;
                if value.is_some_and(|d| d.key != key) {
                    return Err(Status::data_loss("broker descriptor key mismatch"));
                }
                Ok(value)
            })
            .await
        })
        .await
    }
    async fn read_metadata(
        &self,
        key: tinytree_model::ObjectKey,
    ) -> BackendResult<VerifiedMetadata> {
        let inner = self.inner.clone();
        run(async move {
            let deadline = Instant::now() + inner.config.policy.operation_timeout();
            reapi::policy::timeout(deadline, async {
                let _budget = inner.budget(inner.config.metadata_memory()).await?;
                let value = inner
                    .client(inner.config.max_metadata_bytes + 4096)
                    .read_metadata(inner.request(inner.object(key), deadline)?)
                    .await?
                    .into_inner();
                let descriptor = wire::descriptor(
                    value
                        .descriptor
                        .ok_or_else(|| Status::data_loss("missing broker descriptor"))?,
                )?;
                if descriptor.key != key {
                    return Err(Status::data_loss("broker metadata key mismatch"));
                }
                VerifiedMetadata::verify(descriptor, value.canonical, inner.config.model_limits())
                    .map_err(wire::backend)
            })
            .await
        })
        .await
    }
    async fn write_metadata(
        &self,
        object: tinytree_model::Object,
    ) -> BackendResult<tinytree_model::Descriptor> {
        let inner = self.inner.clone();
        run(async move {
            let deadline = Instant::now() + inner.config.policy.operation_timeout();
            reapi::policy::timeout(deadline, async {
                let _budget = inner.budget(inner.config.metadata_memory()).await?;
                let encoded = tinytree_model::encode(&object, inner.config.model_limits())
                    .map_err(|_| Status::invalid_argument("invalid native metadata"))?;
                let value = inner
                    .client(1024)
                    .write_metadata(inner.request(
                        p::MetadataWrite {
                            store_id: inner.store.clone(),
                            canonical: encoded.bytes,
                        },
                        deadline,
                    )?)
                    .await?
                    .into_inner();
                let descriptor = wire::descriptor(value)?;
                if descriptor != encoded.descriptor {
                    return Err(Status::data_loss(
                        "durable broker metadata receipt mismatch",
                    ));
                }
                Ok(descriptor)
            })
            .await
        })
        .await
    }
    async fn read_file(&self, id: tinytree_model::FileId) -> BackendResult<FileRead> {
        let inner = self.inner.clone();
        run(async move {
            let deadline = Instant::now() + inner.config.policy.operation_timeout();
            reapi::policy::timeout(deadline, async {
                let budget = inner.budget(inner.config.file_memory()).await?;
                let disk = Arc::new(
                    inner
                        .disk
                        .clone()
                        .acquire_many_owned(inner.config.max_file_bytes as u32)
                        .await
                        .map_err(|_| Status::unavailable("broker client closed"))?,
                );
                let create_spool = inner.spool.clone();
                let create_disk = disk.clone();
                let create_budget = budget.clone();
                let mut spool = tokio::task::spawn_blocking(move || {
                    let _budget = create_budget;
                    crate::transport::private_directory(&create_spool)?;
                    let file = tempfile::tempfile_in(create_spool)
                        .map_err(|_| Status::internal("cannot create native broker spool"))?;
                    Ok::<_, Status>(Spool {
                        file,
                        _disk: create_disk,
                        _reader_memory: None,
                    })
                })
                .await
                .map_err(|_| Status::internal("broker spool worker failed"))??;
                let key = tinytree_model::ObjectKey {
                    kind: tinytree_model::Kind::File,
                    id: tinytree_model::ObjectId(id.0),
                };
                let mut stream = inner
                    .client(inner.config.max_frame_bytes + 4096)
                    .read_file(inner.request(inner.object(key), deadline)?)
                    .await?
                    .into_inner();
                let first = next(&mut stream, deadline, &inner.config)
                    .await?
                    .ok_or_else(|| Status::data_loss("missing broker file header"))?;
                let Some(p::read_frame::Frame::Descriptor(raw)) = first.frame else {
                    return Err(Status::data_loss("missing broker file header"));
                };
                let descriptor = wire::descriptor(raw)?;
                if descriptor.key != key || descriptor.size_bytes > inner.config.max_file_bytes {
                    return Err(Status::data_loss("invalid broker file descriptor"));
                }
                let mut hash = blake3::Hasher::new();
                let mut offset = 0u64;
                let mut count = 0u64;
                let mut complete = false;
                while let Some(frame) = next(&mut stream, deadline, &inner.config).await? {
                    match frame.frame {
                        Some(p::read_frame::Frame::Chunk(chunk)) if !complete => {
                            if chunk.offset != offset
                                || chunk.data.is_empty()
                                || chunk.data.len() > inner.config.max_frame_bytes
                                || chunk.data.len() as u64
                                    > descriptor.size_bytes.saturating_sub(offset)
                            {
                                return Err(Status::data_loss("invalid broker file chunk"));
                            }
                            offset += chunk.data.len() as u64;
                            count += 1;
                            hash.update(&chunk.data);
                            let b = budget.clone();
                            spool = tokio::task::spawn_blocking(move || {
                                let _budget = b;
                                spool
                                    .file
                                    .write_all(&chunk.data)
                                    .map_err(|_| Status::internal("broker spool write failed"))?;
                                Ok::<_, Status>(spool)
                            })
                            .await
                            .map_err(|_| Status::internal("broker spool worker failed"))??;
                        }
                        Some(p::read_frame::Frame::Complete(end)) if !complete => {
                            if end.count != count
                                || end.size_bytes != offset
                                || end.digest != id.0
                                || offset != descriptor.size_bytes
                                || hash.finalize().as_bytes() != &id.0
                            {
                                return Err(Status::data_loss("broker file verification failed"));
                            }
                            complete = true;
                        }
                        _ => return Err(Status::data_loss("invalid broker file sequence")),
                    }
                }
                if !complete {
                    return Err(Status::data_loss("broker file did not complete"));
                }
                let b = budget.clone();
                spool = tokio::task::spawn_blocking(move || {
                    let _budget = b;
                    spool
                        .file
                        .seek(SeekFrom::Start(0))
                        .map_err(|_| Status::internal("broker spool seek failed"))?;
                    Ok::<_, Status>(spool)
                })
                .await
                .map_err(|_| Status::internal("broker spool worker failed"))??;
                let buffer_bytes = inner
                    .config
                    .max_frame_bytes
                    .min(64 * 1024)
                    .min(descriptor.size_bytes.max(1) as usize);
                let mut owned = Arc::try_unwrap(budget).map_err(|_| {
                    Status::internal("broker file work still owns memory admission")
                })?;
                spool._reader_memory = Some(
                    owned
                        ._memory
                        .split(buffer_bytes.saturating_mul(2).saturating_add(1024))
                        .ok_or_else(|| Status::internal("broker reader reservation missing"))?,
                );
                drop(owned);
                FileRead::from_preverified_pinned_source(
                    descriptor,
                    Box::pin(SpoolReader::new(spool, buffer_bytes)),
                )
                .map_err(wire::backend)
            })
            .await
        })
        .await
    }
    async fn write_file(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
    ) -> BackendResult<tinytree_model::FileId> {
        if max_bytes > self.inner.config.max_file_bytes {
            return Err(wire::other(Status::resource_exhausted(
                "broker file exceeds limit",
            )));
        }
        let inner = self.inner.clone();
        let frame_bytes = inner.config.max_frame_bytes;
        let admission = inner.clone();
        let (budget, deadline) = run(async move {
            let deadline = Instant::now() + admission.config.policy.operation_timeout();
            let budget =
                reapi::policy::timeout(deadline, admission.budget(admission.config.file_memory()))
                    .await?;
            Ok((budget, deadline))
        })
        .await?;
        let rpc_budget = budget.clone();
        let (tx, rx) = mpsc::channel(2);
        let transfer = run(async move {
            reapi::policy::timeout(deadline, async {
                let _budget = rpc_budget;
                let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
                let raw = inner
                    .client(1024)
                    .max_encoding_message_size(inner.config.max_frame_bytes + 4096)
                    .write_file(inner.request(stream, deadline)?)
                    .await?
                    .into_inner();
                let descriptor = wire::descriptor(raw)?;
                if descriptor.key.kind != tinytree_model::Kind::File
                    || descriptor.size_bytes > max_bytes
                {
                    return Err(Status::data_loss("invalid durable broker file receipt"));
                }
                Ok(descriptor)
            })
            .await
        });
        let store = self.inner.store.clone();
        let produce = async {
            tx.send(p::WriteFrame {
                frame: Some(p::write_frame::Frame::Header(p::WriteHeader {
                    store_id: store,
                    max_bytes,
                })),
            })
            .await
            .map_err(|_| wire::other(Status::unavailable("broker upload stopped")))?;
            let mut offset = 0u64;
            let mut hash = blake3::Hasher::new();
            loop {
                let mut data = vec![0; frame_bytes];
                let n = input.read(&mut data).await.map_err(wire::other)?;
                if n == 0 {
                    break;
                }
                data.truncate(n);
                if n as u64 > max_bytes.saturating_sub(offset) {
                    return Err(wire::other(Status::resource_exhausted(
                        "broker file exceeds limit",
                    )));
                }
                hash.update(&data);
                tx.send(p::WriteFrame {
                    frame: Some(p::write_frame::Frame::Chunk(p::Chunk { offset, data })),
                })
                .await
                .map_err(|_| wire::other(Status::unavailable("broker upload stopped")))?;
                offset += n as u64;
            }
            tx.send(p::WriteFrame {
                frame: Some(p::write_frame::Frame::Complete(true)),
            })
            .await
            .map_err(|_| wire::other(Status::unavailable("broker upload stopped")))?;
            drop(tx);
            Ok::<_, BackendError>((offset, *hash.finalize().as_bytes()))
        };
        futures::pin_mut!(produce, transfer);
        let (descriptor, (size, id)) = match futures::future::select(produce, transfer).await {
            futures::future::Either::Left((sent, transfer)) => {
                let sent = sent?;
                (transfer.await?, sent)
            }
            futures::future::Either::Right((received, _produce)) => {
                received?;
                return Err(wire::other(Status::data_loss(
                    "broker acknowledged upload before source completion",
                )));
            }
        };
        if descriptor.size_bytes != size || descriptor.key.id.0 != id {
            return Err(wire::other(Status::data_loss(
                "durable broker file receipt does not match source",
            )));
        }
        Ok(tinytree_model::FileId(id))
    }
    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot> {
        let inner = self.inner.clone();
        run(async move {
            let deadline = Instant::now() + inner.config.policy.operation_timeout();
            reapi::policy::timeout(deadline, async {
                if limits.max_objects == 0
                    || limits.max_objects > inner.config.max_catalog_objects
                    || limits.max_metadata_bytes == 0
                    || limits.max_metadata_bytes > inner.config.max_catalog_bytes
                    || limits.max_edges == 0
                    || limits.max_edges > inner.config.max_catalog_edges
                {
                    return Err(Status::resource_exhausted("broker catalog exceeds limit"));
                }
                let _budget = inner.budget(inner.config.catalog_memory()).await?;
                let mut stream = inner
                    .client(inner.config.max_metadata_bytes + 4096)
                    .copy_catalog(inner.request(
                        p::CatalogRequest {
                            store_id: inner.store.clone(),
                            max_objects: limits.max_objects as u64,
                            max_bytes: limits.max_metadata_bytes as u64,
                            max_edges: limits.max_edges as u64,
                        },
                        deadline,
                    )?)
                    .await?
                    .into_inner();
                let mut entries = Vec::new();
                let mut size = 0u64;
                let mut edges = 0usize;
                let mut previous = None;
                let mut complete = false;
                let mut hash = blake3::Hasher::new();
                hash.update(b"native-broker-copy-catalog-v1\0");
                while let Some(frame) = next(&mut stream, deadline, &inner.config).await? {
                    match frame.frame {
                        Some(p::catalog_frame::Frame::Entry(value)) if !complete => {
                            if entries.len() >= limits.max_objects {
                                return Err(Status::resource_exhausted(
                                    "broker catalog object budget exceeded",
                                ));
                            }
                            let descriptor =
                                wire::descriptor(value.descriptor.ok_or_else(|| {
                                    Status::data_loss("missing broker catalog descriptor")
                                })?)?;
                            if descriptor.key.kind != tinytree_model::Kind::CopyHistory
                                || previous.is_some_and(|id| id >= descriptor.key.id)
                                || descriptor.size_bytes
                                    > limits.max_metadata_bytes.saturating_sub(size as usize) as u64
                            {
                                return Err(Status::data_loss("invalid broker copy catalog"));
                            }
                            let entry = VerifiedMetadata::verify(
                                descriptor,
                                value.canonical,
                                inner.config.model_limits(),
                            )
                            .map_err(wire::backend)?;
                            let tinytree_model::Object::CopyHistory(copy) =
                                tinytree_model::decode(entry.bytes(), inner.config.model_limits())
                                    .map_err(|_| {
                                        Status::data_loss("invalid broker copy history")
                                    })?
                            else {
                                return Err(Status::data_loss("invalid broker copy history"));
                            };
                            edges = edges.checked_add(copy.parents.len()).ok_or_else(|| {
                                Status::resource_exhausted("broker catalog edges exceeded")
                            })?;
                            if edges > limits.max_edges {
                                return Err(Status::resource_exhausted(
                                    "broker catalog edges exceeded",
                                ));
                            }
                            previous = Some(descriptor.key.id);
                            size += descriptor.size_bytes;
                            wire::catalog_hash(&mut hash, &entry);
                            entries.push(entry);
                        }
                        Some(p::catalog_frame::Frame::Complete(end)) if !complete => {
                            if end.count != entries.len() as u64
                                || end.size_bytes != size
                                || end.digest != hash.finalize().as_bytes()
                            {
                                return Err(Status::data_loss(
                                    "broker catalog completion mismatch",
                                ));
                            }
                            complete = true;
                        }
                        _ => return Err(Status::data_loss("invalid broker catalog sequence")),
                    }
                }
                if !complete {
                    return Err(Status::data_loss("broker catalog did not complete"));
                }
                CopyCatalogSnapshot::from_verified(entries, limits).map_err(wire::backend)
            })
            .await
        })
        .await
    }
}
async fn next<T: prost::Message + Default>(
    body: &mut tonic::Streaming<T>,
    deadline: Instant,
    config: &Config,
) -> Result<Option<T>, Status> {
    tokio::time::timeout_at(
        deadline.min(Instant::now() + config.policy.stream_idle_timeout()),
        body.message(),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("broker stream idle deadline exceeded"))?
}
struct Spool {
    file: std::fs::File,
    _disk: Arc<OwnedSemaphorePermit>,
    _reader_memory: Option<OwnedSemaphorePermit>,
}
struct SpoolReader {
    state: Option<Spool>,
    pending: Option<JoinHandle<(Spool, std::io::Result<Vec<u8>>)>>,
    bytes: Vec<u8>,
    position: usize,
    eof: bool,
    buffer_bytes: usize,
}
impl SpoolReader {
    fn new(state: Spool, buffer_bytes: usize) -> Self {
        Self {
            state: Some(state),
            pending: None,
            bytes: Vec::new(),
            position: 0,
            eof: false,
            buffer_bytes,
        }
    }
}
impl AsyncRead for SpoolReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        if output.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            if let Some(job) = self.pending.as_mut() {
                let joined = std::task::ready!(Pin::new(job).poll(cx));
                self.pending = None;
                let (state, result) =
                    joined.map_err(|_| std::io::Error::other("broker spool reader failed"))?;
                self.state = Some(state);
                self.bytes = result?;
                self.position = 0;
                if self.bytes.is_empty() {
                    self.eof = true;
                    self.bytes = Vec::new();
                    self.state = None;
                }
            }
            if self.position < self.bytes.len() {
                let n = output.len().min(self.bytes.len() - self.position);
                output[..n].copy_from_slice(&self.bytes[self.position..self.position + n]);
                self.position += n;
                return Poll::Ready(Ok(n));
            }
            if self.eof {
                return Poll::Ready(Ok(0));
            }
            let mut state = self.state.take().unwrap();
            self.bytes = Vec::new();
            let n = output.len().min(self.buffer_bytes);
            let runtime =
                runtime().map_err(|_| std::io::Error::other("broker executor unavailable"))?;
            self.pending = Some(runtime.spawn_blocking(move || {
                let mut bytes = vec![0; n];
                let result = state.file.read(&mut bytes).map(|n| {
                    bytes.truncate(n);
                    bytes
                });
                (state, result)
            }));
        }
    }
}
