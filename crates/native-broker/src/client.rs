use crate::command::CommandChannel;
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
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
    time::Instant,
};
use tonic::{Request, Status, codec::CompressionEncoding};
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
/// Unlike ordinary request waiters, a source load must retain its external
/// owner until its bounded RPC and synchronous verification actually finish.
pub(crate) async fn run_owned<T: Send + 'static>(
    owner: Arc<dyn std::any::Any + Send + Sync>,
    future: impl Future<Output = Result<T, Status>> + Send + 'static,
) -> BackendResult<T> {
    runtime()?
        .spawn(async move {
            let _owner = owner;
            future.await
        })
        .await
        .map_err(|_| wire::other(std::io::Error::other("native broker job failed")))?
        .map_err(wire::other)
}
async fn read_metadata_request(
    inner: Arc<Inner>,
    key: tinytree_model::ObjectKey,
) -> Result<VerifiedMetadata, Status> {
    let deadline = Instant::now() + inner.config.policy.operation_timeout();
    reapi::policy::timeout(deadline, async {
        let _budget = inner.budget(inner.config.metadata_memory()).await?;
        let value = inner
            .client(inner.config.max_metadata_bytes + 4096, _budget.clone())
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
}
struct Budget {
    _calls: OwnedSemaphorePermit,
    _memory: Mutex<OwnedSemaphorePermit>,
    _external: Option<crate::CommandGuard>,
}
#[derive(Clone)]
struct Inner {
    channel: CommandChannel,
    physical_owner: Option<crate::CommandGuard>,
    store: String,
    spool: PathBuf,
    config: Config,
    #[cfg(windows)]
    _spool_directory: Arc<private_fs::PrivateDir>,
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
    /// Bind subsequent local RPCs to a live host command. Runtime only: shared
    /// channel/pools and configured provider identity remain unchanged.
    pub fn with_command_token(mut self, token: [u8; 32]) -> Self {
        let mut inner = (*self.inner).clone();
        inner.channel = inner.channel.with_command_token(token);
        self.inner = Arc::new(inner);
        self
    }

    fn with_physical_owner(mut self, owner: crate::CommandGuard) -> Self {
        let mut inner = (*self.inner).clone();
        inner.physical_owner = Some(owner);
        self.inner = Arc::new(inner);
        self
    }
    /// Connect to a private owner-controlled Unix socket. The spool directory
    /// must already be private/trusted; it never becomes a broker store path.
    pub async fn connect(
        socket: impl AsRef<Path>,
        store: impl Into<String>,
        spool: impl AsRef<Path>,
        config: Config,
    ) -> BackendResult<Self> {
        Self::connect_local(
            crate::LocalEndpoint::UnixSocket(socket.as_ref().to_owned()),
            store,
            spool,
            config,
        )
        .await
    }
    /// Connect using an explicit owner-authenticated local transport. This also
    /// works without a caller Tokio runtime.
    pub async fn connect_local(
        endpoint: crate::LocalEndpoint,
        store: impl Into<String>,
        spool: impl AsRef<Path>,
        config: Config,
    ) -> BackendResult<Self> {
        let store = store.into();
        let spool = spool.as_ref().to_owned();
        run(async move {
            config.validate()?;
            wire::id(&store)?;
            crate::transport::private_directory(&spool)?;
            #[cfg(windows)]
            let spool_directory = Arc::new(
                private_fs::PrivateDir::open_existing(&spool)
                    .map_err(|_| Status::permission_denied("broker spool is not private"))?,
            );
            let channel = crate::transport::connect_local_channel(endpoint, &config).await?;
            let channel = CommandChannel::new(channel);
            Ok(Self {
                inner: Arc::new(Inner {
                    channel,
                    physical_owner: None,
                    store,
                    spool,
                    calls: Arc::new(Semaphore::new(config.max_inflight)),
                    memory: Arc::new(Semaphore::new(config.max_memory_bytes)),
                    disk: Arc::new(Semaphore::new(config.max_temporary_bytes as usize)),
                    config,
                    #[cfg(windows)]
                    _spool_directory: spool_directory,
                }),
            })
        })
        .await
    }
}
impl Inner {
    fn client(
        &self,
        limit: usize,
        budget: Arc<Budget>,
    ) -> p::object_io_client::ObjectIoClient<CommandChannel> {
        p::object_io_client::ObjectIoClient::new(self.channel.clone().with_owner(budget))
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
            _memory: Mutex::new(memory),
            _external: self.physical_owner.clone(),
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
                    .client(1024, _budget.clone())
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
    async fn descriptor_with_owner(
        &self,
        key: tinytree_model::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<Option<tinytree_model::Descriptor>> {
        let provider = self.clone().with_physical_owner(owner.clone());
        run_owned(owner, async move {
            provider.descriptor(key).await.map_err(wire::backend)
        })
        .await
    }
    async fn copy_catalog_snapshot_with_owner(
        &self,
        limits: CopyCatalogLimits,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<CopyCatalogSnapshot> {
        let provider = self.clone().with_physical_owner(owner.clone());
        run_owned(owner, async move {
            provider
                .copy_catalog_snapshot(limits)
                .await
                .map_err(wire::backend)
        })
        .await
    }
    async fn read_metadata(
        &self,
        key: tinytree_model::ObjectKey,
    ) -> BackendResult<VerifiedMetadata> {
        run(read_metadata_request(self.inner.clone(), key)).await
    }
    async fn read_metadata_with_owner(
        &self,
        key: tinytree_model::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<VerifiedMetadata> {
        let provider = self.clone().with_physical_owner(owner.clone());
        run_owned(owner, read_metadata_request(provider.inner.clone(), key)).await
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
                    .client(1024, _budget.clone())
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
    async fn write_metadata_with_owner(
        &self,
        object: tinytree_model::Object,
        owner: crate::CommandGuard,
    ) -> BackendResult<tinytree_model::Descriptor> {
        self.clone()
            .with_physical_owner(owner)
            .write_metadata(object)
            .await
    }
    async fn read_file(&self, id: tinytree_model::FileId) -> BackendResult<FileRead> {
        self.read_file_with_owner(id, Arc::new(())).await
    }
    async fn read_file_with_owner(
        &self,
        id: tinytree_model::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<FileRead> {
        let inner = self.clone().with_physical_owner(owner.clone()).inner;
        run_owned(owner.clone(), async move {
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
                // Retain the inspected directory chain in the physical worker,
                // not only the caller future: cancellation can drop Inner while
                // spawn_blocking is still queued or creating the tempfile.
                #[cfg(windows)]
                let create_directory = inner._spool_directory.clone();
                let create_disk = disk.clone();
                let create_budget = budget.clone();
                let create_owner = owner.clone();
                let mut spool = tokio::task::spawn_blocking(move || {
                    let held_budget = create_budget.clone();
                    let _budget = create_budget;
                    crate::transport::private_directory(&create_spool)?;
                    let file = tempfile::tempfile_in(create_spool)
                        .map_err(|_| Status::internal("cannot create native broker spool"))?;
                    Ok::<_, Status>(Spool {
                        file,
                        _disk: create_disk,
                        _reader_memory: None,
                        _owner: create_owner,
                        _pending_budget: Some(held_budget),
                        #[cfg(windows)]
                        _directory: create_directory,
                    })
                })
                .await
                .map_err(|_| Status::internal("broker spool worker failed"))??;
                let key = tinytree_model::ObjectKey {
                    kind: tinytree_model::Kind::File,
                    id: tinytree_model::ObjectId(id.0),
                };
                let mut stream = inner
                    .client(inner.config.max_frame_bytes + 4096, budget.clone())
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
                            let work = SpoolWrite {
                                data: chunk.data,
                                spool,
                                _budget: budget.clone(),
                            };
                            spool = tokio::task::spawn_blocking(move || work.execute())
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
                drop(stream);
                spool._reader_memory = Some(
                    budget
                        ._memory
                        .lock()
                        .map_err(|_| Status::internal("broker reader reservation unavailable"))?
                        .split(buffer_bytes.saturating_mul(2).saturating_add(1024))
                        .ok_or_else(|| Status::internal("broker reader reservation missing"))?,
                );
                spool._pending_budget.take();
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
                    .client(1024, _budget.clone())
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
    async fn write_file_with_owner(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max_bytes: u64,
        owner: crate::CommandGuard,
    ) -> BackendResult<tinytree_model::FileId> {
        self.clone()
            .with_physical_owner(owner)
            .write_file(input, max_bytes)
            .await
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
                    .client(inner.config.max_metadata_bytes + 4096, _budget.clone())
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
    // Fields drop in declaration order: the anonymous file closes (and its
    // cleanup completes) while the inspected directory chain remains pinned.
    file: std::fs::File,
    #[cfg(windows)]
    _directory: Arc<private_fs::PrivateDir>,
    _disk: Arc<OwnedSemaphorePermit>,
    _reader_memory: Option<OwnedSemaphorePermit>,
    _owner: Arc<dyn std::any::Any + Send + Sync>,
    // Pending/unreceived physical job results keep full admission until the
    // caller transfers only the bounded reader window at successful delivery.
    _pending_budget: Option<Arc<Budget>>,
}
struct SpoolWrite {
    data: Vec<u8>,
    spool: Spool,
    _budget: Arc<Budget>,
}
impl SpoolWrite {
    fn execute(mut self) -> Result<Spool, Status> {
        self.spool
            .file
            .write_all(&self.data)
            .map_err(|_| Status::internal("broker spool write failed"))?;
        Ok(self.spool)
    }
}
struct SpoolChunk {
    result: std::io::Result<Vec<u8>>,
    state: Spool,
}
struct SpoolReader {
    bytes: Vec<u8>,
    state: Option<Spool>,
    pending: Option<JoinHandle<SpoolChunk>>,
    position: usize,
    eof: bool,
    buffer_bytes: usize,
    _owner: Arc<dyn std::any::Any + Send + Sync>,
}
impl SpoolReader {
    fn new(state: Spool, buffer_bytes: usize) -> Self {
        let owner = state._owner.clone();
        Self {
            state: Some(state),
            pending: None,
            bytes: Vec::new(),
            position: 0,
            eof: false,
            buffer_bytes,
            _owner: owner,
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
                let SpoolChunk { state, result } =
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
                SpoolChunk { result, state }
            }));
        }
    }
}

pub(crate) async fn run_control<T: Send + 'static>(
    future: impl Future<Output = Result<T, Status>> + Send + 'static,
) -> Result<T, Status> {
    let runtime = runtime().map_err(|_| Status::internal("native broker executor unavailable"))?;
    let mut task = Task(runtime.spawn(future));
    (&mut task.0)
        .await
        .map_err(|_| Status::internal("native broker control job failed"))?
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{Condvar, Mutex},
        time::Duration,
    };
    struct Gate {
        released: Mutex<bool>,
        changed: Condvar,
    }
    struct Release(Arc<Gate>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.released.lock().unwrap() = true;
            self.0.changed.notify_all();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_queued_spool_creation_retains_directory_chain_until_physical_completion() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("private");
        let _root = private_fs::PrivateDir::create(&root).unwrap();
        let spool = root.join("spool");
        drop(private_fs::PrivateDir::create(&spool).unwrap());
        let mut config = Config::default();
        config.policy.operation_timeout_ms = 100;
        config.policy.stream_idle_timeout_ms = 100;
        let provider: Arc<dyn NativeObjectIo> = Arc::new(
            jj_native_backend::DirectObjectIo::init(
                &root.join("store"),
                jj_native_backend::Config::default(),
            )
            .unwrap(),
        );
        let service = crate::BrokerService::new(
            HashMap::from([("example".into(), provider)]),
            config.clone(),
        )
        .unwrap();
        let endpoint = crate::LocalEndpoint::WindowsNamedPipe(format!(
            r"\\.\pipe\native-broker-spool-cancel-{}",
            std::process::id()
        ));
        let (stop, signal) = tokio::sync::oneshot::channel();
        let address = endpoint.clone();
        let server = tokio::spawn(async move {
            crate::serve_local(&address, service, async {
                let _ = signal.await;
            })
            .await
        });
        let client = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(client) = BrokerObjectIo::connect_local(
                    endpoint.clone(),
                    "example",
                    &spool,
                    config.clone(),
                )
                .await
                {
                    break client;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let gate = Arc::new(Gate {
            released: Mutex::new(false),
            changed: Condvar::new(),
        });
        let release = Release(gate.clone());
        let (entered, receive) = std::sync::mpsc::channel();
        let mut holders = Vec::new();
        // Occupy the private runtime's complete blocking pool, without changing
        // production dispatch policy or substituting a mock file provider.
        for _ in 0..8 {
            let gate = gate.clone();
            let entered = entered.clone();
            holders.push(runtime().unwrap().spawn_blocking(move || {
                entered.send(()).unwrap();
                let mut released = gate.released.lock().unwrap();
                while !*released {
                    released = gate.changed.wait(released).unwrap();
                }
            }));
        }
        tokio::task::spawn_blocking(move || {
            for _ in 0..8 {
                receive.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        })
        .await
        .unwrap();
        let retained = Arc::downgrade(&client.inner._spool_directory);
        assert!(
            client
                .read_file(tinytree_model::FileId([4; 32]))
                .await
                .is_err()
        );
        drop(client);
        assert!(
            retained.upgrade().is_some(),
            "queued worker must own the inspected chain"
        );
        assert_eq!(
            std::fs::read_dir(&spool).unwrap().count(),
            0,
            "no child file may mask the directory-fence assertion"
        );
        let renamed = root.join("renamed");
        assert!(
            std::fs::rename(&spool, &renamed).is_err(),
            "queued physical work must prevent directory replacement"
        );
        drop(release);
        for holder in holders {
            holder.await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if retained.upgrade().is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        std::fs::rename(&spool, &renamed).unwrap();
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
