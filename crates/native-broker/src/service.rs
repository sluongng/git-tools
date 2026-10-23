use crate::{Config, proto as p, wire};
use futures::AsyncReadExt;
use http::{Request as HttpRequest, Response as HttpResponse};
use jj_native_backend::{CopyCatalogLimits, NativeObjectIo};
use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    time::Instant,
};
use tonic::{
    Request, Response, Status, Streaming, body::Body, codec::CompressionEncoding,
    server::NamedService,
};
use tower::{Layer, Service};
type Stream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;
#[derive(Clone)]
pub struct BrokerService {
    inner: Arc<Inner>,
}
struct Inner {
    stores: HashMap<String, Arc<dyn NativeObjectIo>>,
    config: Config,
    calls: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    staging: Arc<Semaphore>,
}
impl BrokerService {
    /// Call after stopping incoming requests. Waits for transport-held guards
    /// AND detached draining provider jobs; it never force-closes their I/O.
    pub async fn drain(&self) -> Result<(), Status> {
        let permits = self
            .inner
            .calls
            .clone()
            .acquire_many_owned(self.inner.config.max_inflight as u32)
            .await
            .map_err(|_| Status::unavailable("broker admission closed"))?;
        drop(permits);
        Ok(())
    }
    pub fn new(
        stores: HashMap<String, Arc<dyn NativeObjectIo>>,
        config: Config,
    ) -> Result<Self, Status> {
        config.validate()?;
        if stores.is_empty() || stores.len() > 1024 {
            return Err(Status::invalid_argument("invalid broker store registry"));
        }
        for id in stores.keys() {
            wire::id(id)?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                stores,
                calls: Arc::new(Semaphore::new(config.max_inflight)),
                memory: Arc::new(Semaphore::new(config.max_memory_bytes)),
                staging: Arc::new(Semaphore::new(config.max_temporary_bytes as usize)),
                config,
            }),
        })
    }
    fn store(&self, id: &str) -> Result<Arc<dyn NativeObjectIo>, Status> {
        wire::id(id)?;
        self.inner
            .stores
            .get(id)
            .cloned()
            .ok_or_else(|| Status::not_found("native broker store unavailable"))
    }
    async fn reserve_staging(
        &self,
        bytes: u64,
        deadline: Instant,
    ) -> Result<OwnedSemaphorePermit, Status> {
        if bytes > self.inner.config.max_temporary_bytes {
            return Err(Status::resource_exhausted(
                "broker write staging exceeds global limit",
            ));
        }
        tokio::time::timeout_at(
            deadline,
            self.inner.staging.clone().acquire_many_owned(bytes as u32),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("broker write staging admission deadline exceeded"))?
        .map_err(|_| Status::unavailable("broker staging admission closed"))
    }
    /// Includes pre-decode admission and response-lifetime budgets. Embed this
    /// service directly; callers cannot omit the independent body deadline.
    pub fn rpc_service(&self) -> RpcService {
        let raw = p::object_io_server::ObjectIoServer::new(self.clone())
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(
                self.inner
                    .config
                    .max_metadata_bytes
                    .max(self.inner.config.max_frame_bytes)
                    + 4096,
            )
            .max_encoding_message_size(
                self.inner
                    .config
                    .max_metadata_bytes
                    .max(self.inner.config.max_frame_bytes)
                    + 4096,
            );
        let gate = Gate {
            inner: raw,
            shared: self.inner.clone(),
        };
        let timed = grpc_transport::DeadlineLayer::new(
            self.inner.config.policy.operation_timeout_ms,
            self.inner.config.policy.stream_idle_timeout_ms,
            self.inner.config.max_frame_bytes,
        )
        .layer(gate);
        let mapped = tower::ServiceBuilder::new()
            .map_response(|response: HttpResponse<_>| response.map(Body::new))
            .service(timed);
        RpcService {
            inner: tower::util::BoxCloneSyncService::new(mapped),
        }
    }
}
#[derive(Clone)]
pub struct RpcService {
    inner: tower::util::BoxCloneSyncService<HttpRequest<Body>, HttpResponse<Body>, Infallible>,
}
impl NamedService for RpcService {
    const NAME: &'static str = "native.broker.v1.ObjectIo";
}
impl Service<HttpRequest<Body>> for RpcService {
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, r: HttpRequest<Body>) -> Self::Future {
        self.inner.call(r)
    }
}
#[derive(Clone)]
struct Gate<S> {
    inner: S,
    shared: Arc<Inner>,
}
impl<S> Service<HttpRequest<Body>> for Gate<S>
where
    S: Service<HttpRequest<Body>, Response = HttpResponse<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: HttpRequest<Body>) -> Self::Future {
        let shared = self.shared.clone();
        let memory = if request.uri().path().ends_with("/CopyCatalog") {
            shared
                .config
                .catalog_memory()
                .max(shared.config.metadata_memory())
                .max(shared.config.file_memory())
        } else {
            shared
                .config
                .metadata_memory()
                .max(shared.config.file_memory())
        };
        let budget = match grpc_budget(request.headers(), shared.config.policy.operation_timeout())
        {
            Ok(value) => value,
            Err(status) => return Box::pin(async move { Ok(status.into_http()) }),
        };
        let deadline = Instant::now() + budget;
        let ready = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, ready);
        Box::pin(async move {
            let result = tokio::time::timeout_at(deadline, async {
                let calls = shared
                    .calls
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Status::unavailable("broker draining"))?;
                let bytes = shared
                    .memory
                    .clone()
                    .acquire_many_owned(memory as u32)
                    .await
                    .map_err(|_| Status::unavailable("broker draining"))?;
                let work = Arc::new(Work {
                    _calls: calls,
                    _memory: bytes,
                    deadline,
                });
                let mut request = request.map(|body| {
                    Body::new(InboundBody::new(
                        body,
                        deadline,
                        shared.config.policy.stream_idle_timeout(),
                    ))
                });
                request.extensions_mut().insert(work.clone());
                // Do not cancel a provider future while it awaits an owned
                // blocking job. A timed-out caller detaches this draining task;
                // its admission remains charged until actual work completes.
                tokio::spawn(async move {
                    let mut response = inner.call(request).await.unwrap();
                    response
                        .extensions_mut()
                        .insert(grpc_transport::ResponseBudget::new(work));
                    response
                })
                .await
                .map_err(|_| Status::internal("native broker request worker failed"))
            })
            .await;
            match result {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(status)) => Ok(status.into_http()),
                Err(_) => Ok(Status::deadline_exceeded(
                    "native broker operation deadline exceeded",
                )
                .into_http()),
            }
        })
    }
}

#[tonic::async_trait]
impl p::object_io_server::ObjectIo for BrokerService {
    async fn descriptor(
        &self,
        request: Request<p::ObjectRequest>,
    ) -> Result<Response<p::DescriptorReply>, Status> {
        let input = request.into_inner();
        let provider = self.store(&input.store_id)?;
        let value = provider
            .descriptor(wire::key(input.kind, &input.id)?)
            .await
            .map_err(wire::backend)?;
        Ok(Response::new(p::DescriptorReply {
            descriptor: value.map(wire::describe),
        }))
    }
    async fn read_metadata(
        &self,
        request: Request<p::ObjectRequest>,
    ) -> Result<Response<p::Metadata>, Status> {
        let input = request.into_inner();
        let provider = self.store(&input.store_id)?;
        let key = wire::key(input.kind, &input.id)?;
        if key.kind == tinytree_model::Kind::File {
            return Err(Status::invalid_argument("file requires broker stream"));
        }
        let metadata = provider.read_metadata(key).await.map_err(wire::backend)?;
        if metadata.descriptor().key != key
            || metadata.bytes().len() > self.inner.config.max_metadata_bytes
        {
            return Err(Status::resource_exhausted("broker metadata exceeds limit"));
        }
        Ok(Response::new(p::Metadata {
            descriptor: Some(wire::describe(metadata.descriptor())),
            canonical: metadata.bytes().to_vec(),
        }))
    }
    async fn write_metadata(
        &self,
        request: Request<p::MetadataWrite>,
    ) -> Result<Response<p::ObjectDescriptor>, Status> {
        let deadline = request
            .extensions()
            .get::<Arc<Work>>()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?
            .deadline;
        let input = request.into_inner();
        let provider = self.store(&input.store_id)?;
        if input.canonical.len() > self.inner.config.max_metadata_bytes {
            return Err(Status::resource_exhausted("broker metadata exceeds limit"));
        }
        let _staging = self
            .reserve_staging(input.canonical.len() as u64, deadline)
            .await?;
        let object = tinytree_model::decode(&input.canonical, self.inner.config.model_limits())
            .map_err(|_| Status::invalid_argument("invalid broker canonical metadata"))?;
        let encoded = tinytree_model::encode(&object, self.inner.config.model_limits())
            .map_err(|_| Status::invalid_argument("invalid broker canonical metadata"))?;
        if encoded.bytes != input.canonical {
            return Err(Status::invalid_argument("noncanonical broker metadata"));
        }
        let descriptor = provider
            .write_metadata(object)
            .await
            .map_err(wire::backend)?;
        if descriptor != encoded.descriptor {
            return Err(Status::data_loss("broker provider metadata mismatch"));
        }
        Ok(Response::new(wire::describe(descriptor)))
    }
    type ReadFileStream = Stream<p::ReadFrame>;
    async fn read_file(
        &self,
        request: Request<p::ObjectRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        let work = request
            .extensions()
            .get::<Arc<Work>>()
            .cloned()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?;
        let input = request.into_inner();
        let provider = self.store(&input.store_id)?;
        let key = wire::key(input.kind, &input.id)?;
        if key.kind != tinytree_model::Kind::File {
            return Err(Status::invalid_argument("broker stream requires file"));
        }
        let source = provider
            .read_file(tinytree_model::FileId(key.id.0))
            .await
            .map_err(wire::backend)?;
        let (descriptor, mut source) = source.into_parts();
        if descriptor.key != key || descriptor.size_bytes > self.inner.config.max_file_bytes {
            return Err(Status::resource_exhausted("broker file exceeds limit"));
        }
        let frame = self.inner.config.max_frame_bytes;
        let (sender, receiver) = mpsc::channel(2);
        tokio::spawn(async move {
            let deadline = work.deadline;
            let _work = work;
            let result = async {
                send_read(
                    &sender,
                    Ok(p::ReadFrame {
                        frame: Some(p::read_frame::Frame::Descriptor(wire::describe(descriptor))),
                    }),
                    deadline,
                )
                .await?;
                let mut offset = 0u64;
                let mut count = 0u64;
                loop {
                    if sender.is_closed() || Instant::now() >= deadline {
                        return Err(Status::cancelled("broker file consumer stopped"));
                    }
                    let mut data = vec![0; frame];
                    // The owned task retains its budget until this read really
                    // finishes, even when the transport body has expired.
                    let n = source
                        .read(&mut data)
                        .await
                        .map_err(|_| Status::data_loss("broker source read failed"))?;
                    if n == 0 {
                        break;
                    }
                    data.truncate(n);
                    if n as u64 > descriptor.size_bytes.saturating_sub(offset) {
                        return Err(Status::data_loss("broker source size changed"));
                    }
                    send_read(
                        &sender,
                        Ok(p::ReadFrame {
                            frame: Some(p::read_frame::Frame::Chunk(p::Chunk { offset, data })),
                        }),
                        deadline,
                    )
                    .await?;
                    offset += n as u64;
                    count += 1;
                }
                if offset != descriptor.size_bytes {
                    return Err(Status::data_loss("broker source truncated"));
                }
                send_read(
                    &sender,
                    Ok(p::ReadFrame {
                        frame: Some(p::read_frame::Frame::Complete(p::Complete {
                            count,
                            size_bytes: offset,
                            digest: descriptor.key.id.0.to_vec(),
                        })),
                    }),
                    deadline,
                )
                .await
            }
            .await;
            if let Err(status) = result {
                let _ = send_read(&sender, Err(status), deadline).await;
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }
    async fn write_file(
        &self,
        request: Request<Streaming<p::WriteFrame>>,
    ) -> Result<Response<p::ObjectDescriptor>, Status> {
        let deadline = request
            .extensions()
            .get::<Arc<Work>>()
            .ok_or_else(|| Status::internal("broker request missing resource scope"))?
            .deadline;
        let mut body = request.into_inner();
        let first = body
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("missing broker upload header"))?;
        let Some(p::write_frame::Frame::Header(header)) = first.frame else {
            return Err(Status::invalid_argument("missing broker upload header"));
        };
        let provider = self.store(&header.store_id)?;
        if header.max_bytes > self.inner.config.max_file_bytes {
            return Err(Status::resource_exhausted("broker upload exceeds limit"));
        }
        let reservation = header
            .max_bytes
            .checked_mul(2)
            .ok_or_else(|| Status::resource_exhausted("broker upload staging exceeds limit"))?;
        let _staging = self.reserve_staging(reservation, deadline).await?;
        let (sender, receiver) = mpsc::channel(2);
        let mut reader = UploadReader {
            receiver,
            current: std::io::Cursor::new(Vec::new()),
            done: false,
        };
        let provider_copy = provider.clone();
        let write = async move {
            provider_copy
                .write_file(&mut reader, header.max_bytes)
                .await
                .map_err(wire::backend)
        };
        let frame = self.inner.config.max_frame_bytes;
        let ingestion = async move {
            let result = async {
                let mut offset = 0u64;
                let mut complete = false;
                while let Some(next) = body.message().await? {
                    match next.frame {
                        Some(p::write_frame::Frame::Chunk(chunk)) if !complete => {
                            if chunk.offset != offset
                                || chunk.data.is_empty()
                                || chunk.data.len() > frame
                                || chunk.data.len() as u64 > header.max_bytes.saturating_sub(offset)
                            {
                                return Err(Status::invalid_argument(
                                    "invalid broker upload chunk",
                                ));
                            }
                            offset += chunk.data.len() as u64;
                            sender.send(Upload::Data(chunk.data)).await.map_err(|_| {
                                Status::failed_precondition("broker upload stopped")
                            })?;
                        }
                        Some(p::write_frame::Frame::Complete(true)) if !complete => complete = true,
                        _ => {
                            return Err(Status::invalid_argument("invalid broker upload sequence"));
                        }
                    }
                }
                if !complete {
                    return Err(Status::cancelled("broker upload did not complete"));
                }
                sender
                    .send(Upload::Complete)
                    .await
                    .map_err(|_| Status::failed_precondition("broker upload stopped"))?;
                Ok::<_, Status>(())
            }
            .await;
            drop(sender);
            result
        };
        let (written, ingested) = tokio::join!(write, ingestion);
        ingested?;
        let id = written?;
        let descriptor = provider
            .descriptor(tinytree_model::ObjectKey {
                kind: tinytree_model::Kind::File,
                id: tinytree_model::ObjectId(id.0),
            })
            .await
            .map_err(wire::backend)?
            .ok_or_else(|| Status::internal("durable broker upload descriptor missing"))?;
        Ok(Response::new(wire::describe(descriptor)))
    }
    type CopyCatalogStream = Stream<p::CatalogFrame>;
    async fn copy_catalog(
        &self,
        request: Request<p::CatalogRequest>,
    ) -> Result<Response<Self::CopyCatalogStream>, Status> {
        let input = request.into_inner();
        let provider = self.store(&input.store_id)?;
        let cfg = &self.inner.config;
        if input.max_objects == 0
            || input.max_objects > cfg.max_catalog_objects as u64
            || input.max_bytes == 0
            || input.max_bytes > cfg.max_catalog_bytes as u64
            || input.max_edges == 0
            || input.max_edges > cfg.max_catalog_edges as u64
        {
            return Err(Status::resource_exhausted(
                "broker catalog request exceeds limit",
            ));
        }
        let snapshot = provider
            .copy_catalog_snapshot(CopyCatalogLimits {
                max_objects: input.max_objects as usize,
                max_metadata_bytes: input.max_bytes as usize,
                max_edges: input.max_edges as usize,
            })
            .await
            .map_err(wire::backend)?;
        let entries = snapshot.into_entries();
        let output = async_stream::try_stream! {
            let mut hash=blake3::Hasher::new();hash.update(b"native-broker-copy-catalog-v1\0");
            let mut count=0u64;let mut size=0u64;
            for entry in entries {
                wire::catalog_hash(&mut hash,&entry);count+=1;size+=entry.descriptor().size_bytes;
                yield p::CatalogFrame {frame:Some(p::catalog_frame::Frame::Entry(p::Metadata {descriptor:Some(wire::describe(entry.descriptor())),canonical:entry.bytes().to_vec()}))};
            }
            yield p::CatalogFrame {frame:Some(p::catalog_frame::Frame::Complete(p::Complete {count,size_bytes:size,digest:hash.finalize().as_bytes().to_vec()}))};
        };
        Ok(Response::new(Box::pin(output)))
    }
}
enum Upload {
    Data(Vec<u8>),
    Complete,
}
struct UploadReader {
    receiver: mpsc::Receiver<Upload>,
    current: std::io::Cursor<Vec<u8>>,
    done: bool,
}
impl futures::AsyncRead for UploadReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        use std::io::Read;
        loop {
            let n = self.current.read(output)?;
            if n != 0 || output.is_empty() {
                return Poll::Ready(Ok(n));
            }
            if self.done {
                return Poll::Ready(Ok(0));
            }
            match std::task::ready!(self.receiver.poll_recv(cx)) {
                Some(Upload::Data(data)) => self.current = std::io::Cursor::new(data),
                Some(Upload::Complete) => self.done = true,
                None => return Poll::Ready(Err(std::io::Error::other("broker upload cancelled"))),
            }
        }
    }
}

fn grpc_budget(
    headers: &http::HeaderMap,
    maximum: std::time::Duration,
) -> Result<std::time::Duration, Status> {
    let Some(value) = headers.get("grpc-timeout") else {
        return Ok(maximum);
    };
    let value = value
        .to_str()
        .map_err(|_| Status::invalid_argument("invalid broker timeout"))?;
    if !(2..=9).contains(&value.len()) {
        return Err(Status::invalid_argument("invalid broker timeout"));
    }
    let (number, unit) = value.split_at(value.len() - 1);
    if !number.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Status::invalid_argument("invalid broker timeout"));
    }
    let number: u64 = number
        .parse()
        .map_err(|_| Status::invalid_argument("invalid broker timeout"))?;
    let value = match unit {
        "H" => std::time::Duration::from_secs(number.saturating_mul(3600)),
        "M" => std::time::Duration::from_secs(number.saturating_mul(60)),
        "S" => std::time::Duration::from_secs(number),
        "m" => std::time::Duration::from_millis(number),
        "u" => std::time::Duration::from_micros(number),
        "n" => std::time::Duration::from_nanos(number),
        _ => return Err(Status::invalid_argument("invalid broker timeout")),
    };
    Ok(value.min(maximum))
}

struct Work {
    _calls: OwnedSemaphorePermit,
    _memory: OwnedSemaphorePermit,
    deadline: Instant,
}
struct InboundBody {
    body: Body,
    timer: Pin<Box<tokio::time::Sleep>>,
    deadline: Instant,
    idle: std::time::Duration,
    done: bool,
}
impl InboundBody {
    fn new(body: Body, deadline: Instant, idle: std::time::Duration) -> Self {
        Self {
            body,
            timer: Box::pin(tokio::time::sleep_until(
                deadline.min(Instant::now() + idle),
            )),
            deadline,
            idle,
            done: false,
        }
    }
}
impl http_body::Body for InboundBody {
    type Data = bytes::Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<bytes::Bytes>, Status>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        if this.timer.as_mut().poll(cx).is_ready() {
            this.done = true;
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "broker upload body deadline exceeded",
            ))));
        }
        let result = Pin::new(&mut this.body).poll_frame(cx);
        if result.is_ready() {
            this.timer
                .as_mut()
                .reset(this.deadline.min(Instant::now() + this.idle));
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.done || self.body.is_end_stream()
    }
}
async fn send_read(
    sender: &mpsc::Sender<Result<p::ReadFrame, Status>>,
    frame: Result<p::ReadFrame, Status>,
    deadline: Instant,
) -> Result<(), Status> {
    tokio::time::timeout_at(deadline, sender.send(frame))
        .await
        .map_err(|_| Status::deadline_exceeded("broker file stream deadline exceeded"))?
        .map_err(|_| Status::cancelled("broker file consumer stopped"))
}
