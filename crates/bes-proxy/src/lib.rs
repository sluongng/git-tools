//! Durable, per-user BES relay. An ACK means fsync-safe local ownership, not
//! upstream delivery. Pending events are authoritative state, never LRU data.
mod spool;

use anyhow::{Context, Result, ensure};
use futures_core::Stream;
use prost::Message;
use reapi::bes::{
    PublishBuildToolEventStreamRequest as Event, PublishBuildToolEventStreamResponse as Ack,
    PublishLifecycleEventRequest,
    publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, watch};
use tonic::{
    Request, Response, Status,
    codec::CompressionEncoding,
    metadata::{Ascii, MetadataValue},
    transport::{Channel, Endpoint, Server},
};

#[derive(Clone)]
pub struct Config {
    pub upstream: String,
    pub api_key: Option<String>,
    pub max_pending_bytes: u64,
    pub max_streams: usize,
    pub max_event_bytes: usize,
    pub max_concurrent_forwards: usize,
    pub connect_timeout_ms: u64,
    pub lifecycle_timeout_ms: u64,
    /// Per-attempt completion budget starting at request EOF, not ACK inactivity.
    /// Zero disables it for upstreams with unbounded finalization latency.
    pub post_half_close_timeout_ms: u64,
    pub retry_initial_delay_ms: u64,
    /// Number of retries after the first RPC, matching --bes_upload_max_retries.
    pub retry_max_attempts: u32,
    pub tombstone_ttl_seconds: u64,
    pub max_completed_streams: usize,
    /// Commit up to this many immediately available events together (1 disables).
    pub batch_max_events: usize,
    /// Target encoded-byte bound; a larger individual event commits alone.
    pub batch_max_bytes: usize,
    /// Optional coalescing wait. Zero never waits for another event.
    pub batch_max_delay_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            upstream: String::new(),
            api_key: None,
            max_pending_bytes: 1024 * 1024 * 1024,
            max_streams: 128,
            max_event_bytes: 4 * 1024 * 1024,
            max_concurrent_forwards: 16,
            connect_timeout_ms: 10_000,
            lifecycle_timeout_ms: 15_000,
            post_half_close_timeout_ms: 300_000,
            retry_initial_delay_ms: 1_000,
            retry_max_attempts: 4,
            tombstone_ttl_seconds: 86_400,
            max_completed_streams: 4096,
            batch_max_events: 64,
            batch_max_bytes: 256 * 1024,
            batch_max_delay_ms: 0,
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.max_pending_bytes > 0
                && self.max_event_bytes > 0
                && self.max_event_bytes as u64 <= self.max_pending_bytes,
            "BES event limit must fit positive pending-byte budget"
        );
        ensure!(
            (1..=4096).contains(&self.max_streams)
                && (1..=self.max_streams).contains(&self.max_concurrent_forwards),
            "invalid BES stream/concurrent-forward limits"
        );
        ensure!(
            self.max_completed_streams > 0
                && self.max_completed_streams <= 1_000_000
                && self.tombstone_ttl_seconds > 0,
            "BES completed-stream retention must be positive and bounded"
        );
        ensure!(
            self.connect_timeout_ms > 0
                && self.lifecycle_timeout_ms > 0
                && self.post_half_close_timeout_ms <= 604_800_000
                && self.retry_initial_delay_ms > 0
                && self.retry_max_attempts <= 100,
            "invalid BES timeout/retry policy"
        );
        let url = endpoint_url(&self.upstream);
        ensure!(
            (1..=1024).contains(&self.batch_max_events)
                && (1..=64 * 1024 * 1024).contains(&self.batch_max_bytes)
                && self.batch_max_delay_ms <= 100,
            "invalid BES group-commit limits"
        );
        ensure!(
            url.starts_with("http://") || url.starts_with("https://"),
            "BES upstream must be an explicit gRPC endpoint"
        );
        Ok(())
    }
}

#[derive(Clone, Default, Serialize)]
pub struct Metrics {
    /// Runtime admission commits; all events in a group share one SyncAll.
    pub admission_commits: u64,
    pub pending_bytes: u64,
    pub pending_events: u64,
    pub pending_streams: usize,
    pub completed_streams: usize,
    pub pending_lifecycle_events: usize,
    pub forwarding_streams: usize,
    pub blocked_streams: usize,
    /// Codes only: upstream diagnostics can contain sensitive event contents.
    pub failures: HashMap<String, String>,
}
struct Inner {
    config: Config,
    spool: Mutex<spool::Spool>,
    channel: Channel,
    api_key: Option<MetadataValue<Ascii>>,
    changed: Notify,
    stop: watch::Sender<bool>,
    blocked: Mutex<HashMap<String, String>>,
    forwarding: Mutex<HashSet<String>>,
    incoming: Arc<tokio::sync::Semaphore>,
    attached: Mutex<HashMap<String, usize>>,
    storage_jobs: AtomicUsize,
    storage_idle: Notify,
}
#[derive(Clone)]
pub struct BesProxy {
    inner: Arc<Inner>,
    task: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}
impl BesProxy {
    pub async fn open(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        config.validate()?;
        let spool = spool::Spool::open(path.as_ref(), &config)?;
        let url = endpoint_url(&config.upstream);
        let endpoint = Endpoint::from_shared(url.clone())
            .map_err(|_| anyhow::anyhow!("invalid BES upstream endpoint"))?
            .connect_timeout(Duration::from_millis(config.connect_timeout_ms));
        let endpoint = if url.starts_with("https://") {
            endpoint.tls_config(tonic::transport::ClientTlsConfig::new().with_webpki_roots())?
        } else {
            endpoint
        };
        let api_key = config
            .api_key
            .as_ref()
            .map(|key| {
                let mut value: MetadataValue<Ascii> = key
                    .parse()
                    .map_err(|_| anyhow::anyhow!("invalid BES credential encoding"))?;
                value.set_sensitive(true);
                Ok::<_, anyhow::Error>(value)
            })
            .transpose()?;
        let (stop, _) = watch::channel(false);
        let incoming = Arc::new(tokio::sync::Semaphore::new(config.max_streams));
        let inner = Arc::new(Inner {
            config,
            spool: Mutex::new(spool),
            channel: endpoint.connect_lazy(),
            api_key,
            changed: Notify::new(),
            stop,
            attached: Mutex::new(HashMap::new()),
            storage_jobs: AtomicUsize::new(0),
            storage_idle: Notify::new(),
            blocked: Mutex::new(HashMap::new()),
            forwarding: Mutex::new(HashSet::new()),
            incoming,
        });
        let task = tokio::spawn(coordinate(inner.clone()));
        Ok(Self {
            inner,
            task: Arc::new(tokio::sync::Mutex::new(Some(task))),
        })
    }
    pub fn metrics(&self) -> Metrics {
        let mut m = self.inner.spool.lock().unwrap().metrics();
        m.forwarding_streams = self.inner.forwarding.lock().unwrap().len();
        m.failures = self.inner.blocked.lock().unwrap().clone();
        m.blocked_streams = m.failures.len();
        m
    }
    /// Start another Bazel-sized retry cycle for retained failed deliveries.
    pub async fn retry_blocked(&self) -> Result<usize> {
        let count = {
            let mut blocked = self.inner.blocked.lock().unwrap();
            let count = blocked.len();
            blocked.clear();
            count
        };
        self.inner.changed.notify_waiters();
        Ok(count)
    }
    pub async fn shutdown(&self) {
        self.inner.stop.send_replace(true);
        self.inner.changed.notify_waiters();
        if let Some(task) = self.task.lock().await.take() {
            let _ = task.await;
        }
        loop {
            let idle = self.inner.storage_idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.inner.storage_jobs.load(Ordering::SeqCst) == 0 {
                break;
            }
            idle.await;
        }
    }
    #[cfg(unix)]
    pub async fn serve_unix_listener(
        self,
        listener: tokio::net::UnixListener,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        let limit = self.inner.config.max_streams.min(1024) as u32;
        let message_limit = self.inner.config.max_event_bytes;
        Server::builder()
            .max_concurrent_streams(Some(limit))
            .initial_stream_window_size(64 * 1024)
            .initial_connection_window_size(limit * 2 * 64 * 1024)
            .add_service(
                PublishBuildEventServer::new(self)
                    .max_decoding_message_size(message_limit)
                    .max_encoding_message_size(message_limit)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip),
            )
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                shutdown,
            )
            .await
            .context("serving local BES")
    }
}
impl Drop for BesProxy {
    fn drop(&mut self) {
        if Arc::strong_count(&self.task) == 1 {
            self.inner.stop.send_replace(true);
            self.inner.changed.notify_waiters();
        }
    }
}
fn endpoint_url(value: &str) -> String {
    value
        .strip_prefix("grpcs://")
        .map(|v| format!("https://{v}"))
        .or_else(|| value.strip_prefix("grpc://").map(|v| format!("http://{v}")))
        .unwrap_or_else(|| value.to_owned())
}
fn metadata<T>(request: &Request<T>) -> Result<Option<Vec<u8>>, Status> {
    let Some(value) = request
        .metadata()
        .get_bin("build.bazel.remote.execution.v2.requestmetadata-bin")
    else {
        return Ok(None);
    };
    let bytes = value
        .to_bytes()
        .map_err(|_| Status::invalid_argument("invalid BES request metadata"))?;
    if bytes.len() > 8192 {
        return Err(Status::resource_exhausted(
            "BES request metadata exceeds 8 KiB",
        ));
    }
    Ok(Some(bytes.to_vec()))
}
fn request<T>(inner: &Inner, value: T, attribution: Option<&[u8]>) -> Request<T> {
    let mut request = Request::new(value);
    if let Some(key) = &inner.api_key {
        request
            .metadata_mut()
            .insert("x-buildbuddy-api-key", key.clone());
    }
    if let Some(bytes) = attribution {
        request.metadata_mut().insert_bin(
            "build.bazel.remote.execution.v2.requestmetadata-bin",
            MetadataValue::from_bytes(bytes),
        );
    }
    request
}
fn client(inner: &Inner) -> PublishBuildEventClient<Channel> {
    PublishBuildEventClient::new(inner.channel.clone())
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_encoding_message_size(inner.config.max_event_bytes)
        .max_decoding_message_size(inner.config.max_event_bytes)
}
async fn storage<T: Send + 'static>(
    inner: &Arc<Inner>,
    f: impl FnOnce(&mut spool::Spool) -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    let inner = inner.clone();
    // Register before checking stop: shutdown must never see zero while an
    // already-admitted operation can still begin an fsync afterward.
    inner.storage_jobs.fetch_add(1, Ordering::SeqCst);
    if *inner.stop.borrow() {
        inner.storage_jobs.fetch_sub(1, Ordering::SeqCst);
        inner.storage_idle.notify_waiters();
        return Err(Status::unavailable("BES daemon shutting down"));
    }
    tokio::task::spawn_blocking(move || {
        struct Job(Arc<Inner>);
        impl Drop for Job {
            fn drop(&mut self) {
                self.0.storage_jobs.fetch_sub(1, Ordering::SeqCst);
                self.0.storage_idle.notify_waiters();
            }
        }
        let _job = Job(inner.clone());
        let mut spool = inner
            .spool
            .lock()
            .map_err(|_| Status::internal("BES spool lock poisoned"))?;
        f(&mut spool)
    })
    .await
    .map_err(|_| Status::internal("BES spool worker failed"))?
}

struct Attachment {
    inner: Arc<Inner>,
    id: Option<String>,
}
impl Drop for Attachment {
    fn drop(&mut self) {
        if let Some(id) = &self.id {
            let mut attached = self.inner.attached.lock().unwrap();
            if let Some(count) = attached.get_mut(id) {
                *count -= 1;
                if *count == 0 {
                    attached.remove(id);
                }
            }
            self.inner.changed.notify_waiters();
        }
    }
}
#[tonic::async_trait]
impl PublishBuildEvent for BesProxy {
    type PublishBuildToolEventStreamStream =
        Pin<Box<dyn Stream<Item = Result<Ack, Status>> + Send>>;
    async fn publish_build_tool_event_stream(
        &self,
        request: Request<tonic::Streaming<Event>>,
    ) -> Result<Response<Self::PublishBuildToolEventStreamStream>, Status> {
        let permit = self
            .inner
            .incoming
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many incoming BES streams"))?;
        let attribution = metadata(&request)?;
        let mut input = request.into_inner();
        let inner = self.inner.clone();
        let mut stop = inner.stop.subscribe();
        let output = async_stream::try_stream! {
            let _permit = permit;
            let mut stream_id = None;
            let mut attachment = Attachment { inner: inner.clone(), id: None };
            let mut pending = None;
            let mut eof = false;
            loop {
                let next = if pending.is_some() {
                    pending.take()
                } else if eof {
                    None
                } else {
                    tokio::select! {
                        _ = stop.wait_for(|v| *v) => Err(Status::unavailable("BES daemon shutting down")),
                        next = input.message() => next,
                    }?
                };
                let Some(event) = next else { break };
                let id = spool::event_id(&event)?;
                if stream_id.as_ref().is_some_and(|current| current != &id) {
                    Err(Status::invalid_argument("one BES RPC must contain only one stream"))?;
                }
                if attachment.id.is_none() {
                    *inner.attached.lock().unwrap().entry(id.clone()).or_default() += 1;
                    attachment.id = Some(id.clone());
                }
                stream_id = Some(id.clone());
                let mut bytes = event.encoded_len();
                let mut batch = vec![event];
                let mut input_error = None;
                let deadline = tokio::time::Instant::now()
                    + Duration::from_millis(inner.config.batch_max_delay_ms);
                while batch.len() < inner.config.batch_max_events
                    && bytes < inner.config.batch_max_bytes
                    && !spool::terminal(batch.last().unwrap()) {
                    // A zero delay drains only immediately available messages;
                    // it never waits for a sparse publisher to form a batch.
                    let next = if inner.config.batch_max_delay_ms == 0 {
                        tokio::select! {
                            biased;
                            next = input.message() => Some(next),
                            _ = std::future::ready(()) => None,
                        }
                    } else {
                        tokio::time::timeout_at(deadline, input.message()).await.ok()
                    };
                    match next {
                        Some(Ok(Some(event))) => {
                            match spool::event_id(&event) {
                                Ok(next_id) if next_id == id => {},
                                Ok(_) => {input_error = Some(Status::invalid_argument("one BES RPC must contain only one stream")); break;},
                                Err(error) => {input_error = Some(error); break;},
                            }
                            let size = event.encoded_len();
                            if bytes.saturating_add(size) > inner.config.batch_max_bytes {
                                pending = Some(event);
                                break;
                            }
                            bytes += size;
                            batch.push(event);
                        }
                        Some(Ok(None)) => {eof = true; break;},
                        Some(Err(error)) => {input_error = Some(error); break;},
                        None => break,
                    }
                }
                let attribution = attribution.clone();
                let admission = storage(&inner, move |spool| spool.append_batch(batch, attribution)).await?;
                inner.changed.notify_waiters();
                for ack in admission.acks {yield ack;}
                if let Some(error) = admission.error.or(input_error) {Err(error)?;}
            }
        };
        Ok(Response::new(Box::pin(output)))
    }
    async fn publish_lifecycle_event(
        &self,
        request: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        let _permit = self
            .inner
            .incoming
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many incoming BES RPCs"))?;
        let attribution = metadata(&request)?;
        let event = request.into_inner();
        storage(&self.inner, move |spool| {
            spool.append_lifecycle(event, attribution)
        })
        .await?;
        self.inner.changed.notify_waiters();
        Ok(Response::new(()))
    }
}

async fn coordinate(inner: Arc<Inner>) {
    let mut stop = inner.stop.subscribe();
    let mut tasks = tokio::task::JoinSet::new();
    let mut lifecycle_active = false;
    loop {
        let notify = inner.changed.notified();
        let gc_inner = inner.clone();
        if storage(&inner, move |spool| {
            let mut protected = gc_inner.forwarding.lock().unwrap().clone();
            protected.extend(gc_inner.attached.lock().unwrap().keys().cloned());
            spool
                .trim(&protected)
                .map_err(|_| Status::internal("BES receipt GC failed"))
        })
        .await
        .is_err()
            && !*inner.stop.borrow()
        {
            inner
                .blocked
                .lock()
                .unwrap()
                .insert("spool".into(), "Internal".into());
        }
        let jobs = {
            let spool = inner.spool.lock().unwrap();
            let pending = match spool.pending_streams() {
                Ok(pending) => pending,
                Err(_) => {
                    inner
                        .blocked
                        .lock()
                        .unwrap()
                        .insert("spool".into(), "Internal".into());
                    Vec::new()
                }
            };
            let blocked = inner.blocked.lock().unwrap();
            let forwarding = inner.forwarding.lock().unwrap();
            pending
                .into_iter()
                .filter(|id| {
                    !blocked.contains_key(id)
                        && !forwarding.contains(id)
                        && (spool.stream(id).is_ok_and(|state| state.terminal.is_some())
                            || inner.attached.lock().unwrap().contains_key(id))
                })
                .take(
                    inner
                        .config
                        .max_concurrent_forwards
                        .saturating_sub(forwarding.len()),
                )
                .collect::<Vec<_>>()
        };
        for id in jobs {
            inner.forwarding.lock().unwrap().insert(id.clone());
            let inner = inner.clone();
            tasks.spawn(async move {
                let result = relay_with_retries(inner.clone(), id.clone()).await;
                if let Err(status) = result
                    && !(status.code() == tonic::Code::Cancelled
                        && status.message() == "BES ingress detached")
                {
                    inner
                        .blocked
                        .lock()
                        .unwrap()
                        .insert(id.clone(), format!("{:?}", status.code()));
                }
                inner.forwarding.lock().unwrap().remove(&id);
                inner.changed.notify_waiters();
                false
            });
        }
        if !lifecycle_active
            && !inner.blocked.lock().unwrap().contains_key("lifecycle")
            && inner.spool.lock().unwrap().has_lifecycle()
        {
            lifecycle_active = true;
            let inner = inner.clone();
            tasks.spawn(async move {
                if let Err(status) = relay_lifecycle(inner.clone()).await {
                    inner
                        .blocked
                        .lock()
                        .unwrap()
                        .insert("lifecycle".into(), format!("{:?}", status.code()));
                }
                inner.changed.notify_waiters();
                true
            });
        }
        tokio::select! {
            _ = stop.wait_for(|v| *v) => break,
            result = tasks.join_next(), if !tasks.is_empty() => {
                if matches!(result, Some(Ok(true))) { lifecycle_active = false; }
            }
            _ = notify => {},
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    inner.forwarding.lock().unwrap().clear();
}
fn retriable(status: &Status) -> bool {
    !matches!(
        status.code(),
        tonic::Code::InvalidArgument
            | tonic::Code::PermissionDenied
            | tonic::Code::FailedPrecondition
    )
}
async fn backoff(inner: &Inner, attempt: u32) {
    let millis = (inner.config.retry_initial_delay_ms as f64 * 1.6_f64.powi(attempt as i32))
        .min(u64::MAX as f64) as u64;
    tokio::time::sleep(Duration::from_millis(millis)).await;
}
async fn relay_with_retries(inner: Arc<Inner>, id: String) -> Result<(), Status> {
    let mut retries = 0;
    loop {
        let before = inner.spool.lock().unwrap().acknowledged(&id)?;
        match relay_once(inner.clone(), id.clone()).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                let after = inner.spool.lock().unwrap().acknowledged(&id)?;
                if error.code() == tonic::Code::Cancelled
                    && error.message() == "BES ingress detached"
                {
                    return Err(error);
                }
                if error.code() == tonic::Code::DataLoss
                    || !retriable(&error)
                    || retries >= inner.config.retry_max_attempts
                {
                    return Err(error);
                }
                backoff(&inner, retries).await;
                if after > before {
                    retries = 0;
                } else {
                    retries += 1;
                }
            }
        }
    }
}
/// Cancellation must not depend on the request body being polled: an upstream
/// that stops reading can exhaust HTTP/2 send capacity and park that producer.
async fn ingress_detached(inner: Arc<Inner>, id: String) -> Status {
    loop {
        let changed = inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let state = match inner.spool.lock().unwrap().stream(&id) {
            Ok(state) => state,
            Err(error) => return error,
        };
        if state.terminal.is_some() {
            // A durably admitted terminal stream is daemon-owned and must
            // finish forwarding even after its local publisher disconnects.
            return std::future::pending().await;
        }
        if !inner.attached.lock().unwrap().contains_key(&id) {
            return Status::cancelled("BES ingress detached");
        }
        changed.await;
    }
}
async fn relay_once(inner: Arc<Inner>, id: String) -> Result<(), Status> {
    let state = inner.spool.lock().unwrap().stream(&id)?;
    if state.complete() {
        return Ok(());
    }
    let start = state.acked + 1;
    let detached = ingress_detached(inner.clone(), id.clone());
    tokio::pin!(detached);
    let sent = Arc::new(AtomicI64::new(state.acked));
    let sent_stream = sent.clone();
    let (errors_tx, mut errors_rx) = tokio::sync::mpsc::channel::<Status>(1);
    // The terminal request body can finish before response headers arrive. Keep
    // this channel open so successful half-close is not mistaken for failure.
    let _errors_keepalive = errors_tx.clone();
    let send_inner = inner.clone();
    let send_id = id.clone();
    let (half_closed_tx, half_closed_rx) = tokio::sync::oneshot::channel();
    // Start one absolute timer at EOF, including time waiting for response
    // headers. Never impose an ACK-idle timeout on an open request stream:
    // upstreams may legitimately buffer every ACK until the terminal EOF.
    let completion = async {
        if inner.config.post_half_close_timeout_ms == 0 {
            return std::future::pending::<Status>().await;
        }
        if let Ok(at) = half_closed_rx.await {
            tokio::time::sleep_until(
                at + Duration::from_millis(inner.config.post_half_close_timeout_ms),
            )
            .await;
            Status::deadline_exceeded("BES post-half-close completion deadline exceeded")
        } else {
            // Sender cancellation/storage failure is handled by the existing
            // ingress/error paths, not by manufacturing a clean half-close.
            std::future::pending().await
        }
    };
    tokio::pin!(completion);
    let outgoing = async_stream::stream! {
        let mut sequence = start;
        loop {
            let notified = send_inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let id = send_id.clone();
            let result = storage(&send_inner, move |spool| spool.event(&id, sequence)).await;
            match result {
                Ok(Some((event, terminal))) => {
                    sent_stream.store(sequence, Ordering::SeqCst);
                    yield event;
                    sequence += 1;
                    if terminal {
                        let _ = half_closed_tx.send(tokio::time::Instant::now());
                        break;
                    }
                }
                Ok(None) => notified.await,
                Err(error) => {
                    let _ = errors_tx.send(error).await;
                    // Do not send clean EOF on storage failure; cancel the RPC.
                    std::future::pending::<()>().await;
                }
            }
        }
    };
    let mut client = client(&inner);
    let response = tokio::select! {
        result = client.publish_build_tool_event_stream(request(&inner, outgoing, state.attribution.as_deref())) => result?,
        error = errors_rx.recv() => return Err(error.unwrap_or_else(|| Status::internal("BES sender stopped"))),
        error = &mut detached => return Err(error),
        error = &mut completion => return Err(error),
    };
    let mut responses = response.into_inner();
    loop {
        let ack = tokio::select! {
            response = responses.message() => response?,
            error = &mut detached => return Err(error),
            error = &mut completion => return Err(error),
            error = errors_rx.recv() => {
                if let Some(error) = error { return Err(error); }
                // A successful terminal send drops the error channel.
                responses.message().await?
            }
        };
        let Some(ack) = ack else { break };
        if ack.sequence_number > sent.load(Ordering::SeqCst) {
            return Err(Status::data_loss(
                "BES upstream acknowledged an unsent event",
            ));
        }
        let id = id.clone();
        storage(&inner, move |spool| spool.ack(&id, ack)).await?;
    }
    if inner.spool.lock().unwrap().stream(&id)?.complete() {
        Ok(())
    } else {
        Err(Status::failed_precondition(
            "BES upstream closed before acknowledging terminal event",
        ))
    }
}
async fn relay_lifecycle(inner: Arc<Inner>) -> Result<(), Status> {
    loop {
        let Some((key, event, attribution)) = inner.spool.lock().unwrap().next_lifecycle()? else {
            return Ok(());
        };
        let mut retries = 0;
        loop {
            let mut request = request(&inner, event.clone(), attribution.as_deref());
            let timeout = Duration::from_millis(inner.config.lifecycle_timeout_ms);
            request.set_timeout(timeout);
            let result =
                tokio::time::timeout(timeout, client(&inner).publish_lifecycle_event(request))
                    .await
                    .map_err(|_| Status::deadline_exceeded("BES lifecycle deadline exceeded"))
                    .and_then(|result| result);
            match result {
                Ok(_) => {
                    storage(&inner, move |spool| spool.ack_lifecycle(key)).await?;
                    break;
                }
                Err(error) => {
                    if !retriable(&error) || retries >= inner.config.retry_max_attempts {
                        return Err(error);
                    }
                    backoff(&inner, retries).await;
                    retries += 1;
                }
            }
        }
    }
}
