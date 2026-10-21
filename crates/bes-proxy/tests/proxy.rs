use anyhow::Result;
use bes_proxy::{BesProxy, Config};
use reapi::bes::{
    BuildEvent, OrderedBuildEvent, PublishBuildToolEventStreamRequest as Event,
    PublishBuildToolEventStreamResponse as Ack, PublishLifecycleEventRequest, StreamId,
    build_event::{self, BuildComponentStreamFinished},
    publish_build_event_client::PublishBuildEventClient,
    publish_build_event_server::{PublishBuildEvent, PublishBuildEventServer},
};
use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use tonic::{
    Request, Response, Status,
    codec::CompressionEncoding,
    transport::{Channel, Server},
};

type Output = Pin<Box<dyn futures_core::Stream<Item = Result<Ack, Status>> + Send>>;
type ObservedHeaders = Arc<Mutex<Vec<(Option<String>, Option<Vec<u8>>)>>>;
#[derive(Clone)]
struct Backend {
    events: Arc<Mutex<Vec<Event>>>,
    ended: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
    lifecycle: Arc<AtomicUsize>,
    lifecycle_gate: watch::Sender<bool>,
    read_gate: watch::Sender<bool>,
    headers: ObservedHeaders,
    gate: watch::Sender<bool>,
    failures: Arc<AtomicUsize>,
    partial_failures: Arc<AtomicUsize>,
    bad_ack: bool,
}
impl Backend {
    fn new(allow_ack: bool) -> Self {
        Self {
            events: Default::default(),
            ended: Default::default(),
            attempts: Default::default(),
            lifecycle: Default::default(),
            lifecycle_gate: watch::channel(true).0,
            read_gate: watch::channel(true).0,
            headers: Default::default(),
            gate: watch::channel(allow_ack).0,
            failures: Default::default(),
            partial_failures: Default::default(),
            bad_ack: false,
        }
    }
}
#[tonic::async_trait]
impl PublishBuildEvent for Backend {
    type PublishBuildToolEventStreamStream = Output;
    async fn publish_build_tool_event_stream(
        &self,
        request: Request<tonic::Streaming<Event>>,
    ) -> Result<Response<Output>, Status> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.headers.lock().unwrap().push((
            request
                .metadata()
                .get("x-buildbuddy-api-key")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            request
                .metadata()
                .get_bin("build.bazel.remote.execution.v2.requestmetadata-bin")
                .and_then(|v| v.to_bytes().ok())
                .map(|v| v.to_vec()),
        ));
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Status::unavailable("fixture outage"));
        }
        let this = self.clone();
        let mut input = request.into_inner();
        Ok(Response::new(Box::pin(async_stream::try_stream! {
            this.read_gate.subscribe().wait_for(|v| *v).await
                .map(|_| ()).map_err(|_| Status::cancelled("fixture stopped"))?;
            let mut events = Vec::new();
            while let Some(event) = input.message().await? {
                this.events.lock().unwrap().push(event.clone());
                events.push(event);
            }
            this.ended.fetch_add(1, Ordering::SeqCst);
            let mut gate = this.gate.subscribe();
            gate.wait_for(|v| *v).await.map(|_| ()).map_err(|_| Status::cancelled("fixture stopped"))?;
            let fail_after_first = this.partial_failures.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok();
            for (index, event) in events.into_iter().enumerate() {
                let ordered = event.ordered_build_event.unwrap();
                yield Ack { stream_id: ordered.stream_id, sequence_number: ordered.sequence_number + if this.bad_ack {1} else {0} };
                if fail_after_first && index == 0 { Err(Status::unavailable("fixture disconnected after prefix ACK"))?; }
            }
        })))
    }
    async fn publish_lifecycle_event(
        &self,
        _: Request<PublishLifecycleEventRequest>,
    ) -> Result<Response<()>, Status> {
        self.lifecycle.fetch_add(1, Ordering::SeqCst);
        self.lifecycle_gate
            .subscribe()
            .wait_for(|v| *v)
            .await
            .map(|_| ())
            .map_err(|_| Status::cancelled("fixture stopped"))?;
        Ok(Response::new(()))
    }
}
async fn backend(
    backend: Backend,
) -> Result<(
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let task = tokio::spawn(
        Server::builder()
            .add_service(
                PublishBuildEventServer::new(backend)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Ok((url, task))
}
async fn frontend(
    proxy: BesProxy,
) -> Result<(
    PublishBuildEventClient<Channel>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let task = tokio::spawn(
        Server::builder()
            .add_service(
                PublishBuildEventServer::new(proxy)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Ok((
        PublishBuildEventClient::connect(url)
            .await?
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip),
        task,
    ))
}
fn config(upstream: String) -> Config {
    Config {
        upstream,
        retry_initial_delay_ms: 5,
        retry_max_attempts: 1,
        ..Default::default()
    }
}
fn event(invocation: &str, seq: i64, terminal: bool) -> Event {
    Event {
        ordered_build_event: Some(OrderedBuildEvent {
            stream_id: Some(StreamId {
                build_id: format!("build-{invocation}"),
                invocation_id: invocation.into(),
                component: 3,
            }),
            sequence_number: seq,
            event: Some(BuildEvent {
                event: Some(if terminal {
                    build_event::Event::ComponentStreamFinished(BuildComponentStreamFinished {
                        r#type: 1,
                    })
                } else {
                    build_event::Event::ConsoleOutput(build_event::ConsoleOutput {
                        output: Some(build_event::console_output::Output::TextOutput(
                            "fixture output".into(),
                        )),
                        r#type: 1,
                    })
                }),
                ..Default::default()
            }),
        }),
        ..Default::default()
    }
}
async fn publish(
    client: &mut PublishBuildEventClient<Channel>,
    events: Vec<Event>,
) -> Result<Vec<Ack>> {
    let mut request = Request::new(tokio_stream::iter(events));
    request.metadata_mut().insert(
        "x-buildbuddy-api-key",
        "caller-key-must-not-forward".parse()?,
    );
    request.metadata_mut().insert_bin(
        "build.bazel.remote.execution.v2.requestmetadata-bin",
        tonic::metadata::MetadataValue::from_bytes(b"fixture-attribution"),
    );
    let mut response = client
        .publish_build_tool_event_stream(request)
        .await?
        .into_inner();
    let mut acks = Vec::new();
    while let Some(ack) = response.message().await? {
        acks.push(ack);
    }
    Ok(acks)
}
async fn wait(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("fixture condition timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_ack_concurrent_streams_survive_restart_until_upstream_ack_then_gc() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let backend_state = Backend::new(false);
    let (url, upstream_task) = backend(backend_state.clone()).await?;
    let mut cfg = config(url);
    cfg.api_key = Some("daemon-owned-key".into());
    let proxy = BesProxy::open(dir.path().join("bes"), cfg.clone()).await?;
    let (mut first, frontend_task) = frontend(proxy.clone()).await?;
    let mut second = first.clone();
    let mut first_events = (1..=128)
        .map(|seq| event("first", seq, seq == 128))
        .collect::<Vec<_>>();
    first_events[0].stream_metadata.push(Default::default());
    first_events[0].stream_metadata[0].type_url =
        "type.googleapis.com/fixture.StreamMetadata".into();
    first_events[0].stream_metadata[0].value = vec![8, 7, 18, 3, 255, 0, 42];
    first_events[0]
        .ordered_build_event
        .as_mut()
        .unwrap()
        .event
        .as_mut()
        .unwrap()
        .event = Some(build_event::Event::BazelEvent(Default::default()));
    if let Some(build_event::Event::BazelEvent(any)) = first_events[0]
        .ordered_build_event
        .as_mut()
        .unwrap()
        .event
        .as_mut()
        .unwrap()
        .event
        .as_mut()
    {
        any.type_url = "type.googleapis.com/build_event_stream.BuildEvent".into();
        any.value = vec![10, 5, 0, 255, 128, 42, 1];
    }
    let second_events = vec![event("second", 1, false), event("second", 2, true)];
    let started = std::time::Instant::now();
    let (acks_a, acks_b) = tokio::join!(
        publish(&mut first, first_events.clone()),
        publish(&mut second, second_events.clone())
    );
    assert_eq!(acks_a?.len(), 128);
    assert_eq!(acks_b?.len(), 2);
    eprintln!(
        "fsync-before-ACK:130events/2streams in {:.3}s",
        started.elapsed().as_secs_f64()
    );
    wait(|| backend_state.ended.load(Ordering::SeqCst) == 2).await;
    assert_eq!(proxy.metrics().pending_events, 130);
    assert_eq!(proxy.metrics().pending_streams, 2);
    assert!(
        backend_state
            .events
            .lock()
            .unwrap()
            .contains(&first_events[0]),
        "opaque Any payload and stream metadata must survive relay"
    );
    assert!(
        backend_state
            .headers
            .lock()
            .unwrap()
            .iter()
            .all(
                |(key, attribution)| key.as_deref() == Some("daemon-owned-key")
                    && attribution.as_deref() == Some(b"fixture-attribution".as_slice())
            )
    );
    // Restart with all payloads locally ACKed but no upstream ACK delivered.
    proxy.shutdown().await;
    frontend_task.abort();
    let _ = frontend_task.await;
    drop(first);
    drop(second);
    drop(proxy);
    let proxy = BesProxy::open(dir.path().join("bes"), cfg).await?;
    assert_eq!(proxy.metrics().pending_events, 130);
    backend_state.gate.send_replace(true);
    wait(|| proxy.metrics().pending_events == 0).await;
    assert_eq!(proxy.metrics().pending_bytes, 0);
    assert_eq!(proxy.metrics().completed_streams, 2);
    assert!(backend_state.events.lock().unwrap().len() >= 260);
    // Tombstones ACK a late local replay without resurrecting deleted payloads.
    let (mut client, frontend_task) = frontend(proxy.clone()).await?;
    assert_eq!(publish(&mut client, second_events).await?.len(), 2);
    assert_eq!(proxy.metrics().pending_events, 0);
    proxy.shutdown().await;
    frontend_task.abort();
    upstream_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incomplete_disconnected_stream_does_not_finalize_or_starve_other_builds() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let state = Backend::new(true);
    let (url, upstream_task) = backend(state.clone()).await?;
    let mut cfg = config(url);
    cfg.max_concurrent_forwards = 1;
    let proxy = BesProxy::open(dir.path().join("bes"), cfg).await?;
    let (mut client, frontend_task) = frontend(proxy.clone()).await?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(event("orphan", 1, false)).await?;
    let mut response = client
        .publish_build_tool_event_stream(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?
        .into_inner();
    assert_eq!(response.message().await?.unwrap().sequence_number, 1);
    wait(|| !state.events.lock().unwrap().is_empty()).await;
    assert_eq!(state.ended.load(Ordering::SeqCst), 0);
    drop(tx);
    assert!(response.message().await?.is_none());
    wait(|| proxy.metrics().forwarding_streams == 0).await;
    let mut other = client.clone();
    assert_eq!(
        publish(&mut other, vec![event("ready", 1, true)])
            .await?
            .len(),
        1
    );
    wait(|| proxy.metrics().pending_events == 1 && proxy.metrics().completed_streams == 1).await;
    assert_eq!(
        state.ended.load(Ordering::SeqCst),
        1,
        "orphan must be cancelled, not halfclosed/finalized"
    );
    assert_eq!(
        publish(
            &mut client,
            vec![event("orphan", 1, false), event("orphan", 2, true)]
        )
        .await?
        .len(),
        2
    );
    wait(|| proxy.metrics().pending_events == 0).await;
    proxy.shutdown().await;
    frontend_task.abort();
    upstream_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detached_stream_releases_forwarding_slot_when_upstream_stops_reading() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let state = Backend::new(true);
    state.read_gate.send_replace(false);
    let (url, upstream_task) = backend(state.clone()).await?;
    let mut cfg = config(url);
    cfg.max_concurrent_forwards = 1;
    let proxy = BesProxy::open(dir.path().join("bes"), cfg).await?;
    let (mut client, frontend_task) = frontend(proxy.clone()).await?;
    // Incompressible bytes exceed the transport's send windows/buffers even
    // with gzip enabled. The upstream deliberately never polls its input.
    let mut random = 0x4d595df4d0f33173_u64;
    let events = (1..=32)
        .map(|sequence| {
            let bytes = (0..256 * 1024)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    random as u8
                })
                .collect();
            let mut request = event("stalled", sequence, false);
            request
                .ordered_build_event
                .as_mut()
                .unwrap()
                .event
                .as_mut()
                .unwrap()
                .event = Some(build_event::Event::ConsoleOutput(
                build_event::ConsoleOutput {
                    r#type: 1,
                    output: Some(build_event::console_output::Output::BinaryOutput(bytes)),
                },
            ));
            request
        })
        .collect::<Vec<_>>();
    assert_eq!(publish(&mut client, events).await?.len(), 32);
    wait(|| state.attempts.load(Ordering::SeqCst) > 0).await;
    wait(|| proxy.metrics().forwarding_streams == 0).await;
    assert_eq!(proxy.metrics().pending_events, 32);
    assert_eq!(proxy.metrics().blocked_streams, 0);
    assert_eq!(state.ended.load(Ordering::SeqCst), 0);
    state.read_gate.send_replace(true);
    assert_eq!(
        publish(&mut client, vec![event("ready", 1, true)])
            .await?
            .len(),
        1
    );
    wait(|| proxy.metrics().completed_streams == 1).await;
    assert_eq!(proxy.metrics().pending_events, 32);
    proxy.shutdown().await;
    frontend_task.abort();
    upstream_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_exhaustion_retains_events_and_explicit_retry_recovers() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let state = Backend::new(true);
    state.failures.store(2, Ordering::SeqCst);
    let (url, upstream_task) = backend(state.clone()).await?;
    let proxy = BesProxy::open(dir.path().join("bes"), config(url)).await?;
    let (mut client, frontend_task) = frontend(proxy.clone()).await?;
    assert_eq!(
        publish(&mut client, vec![event("retry", 1, true)])
            .await?
            .len(),
        1
    );
    wait(|| proxy.metrics().blocked_streams == 1).await;
    assert_eq!(state.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(proxy.metrics().pending_events, 1);
    assert_eq!(proxy.retry_blocked().await?, 1);
    wait(|| proxy.metrics().pending_events == 0).await;
    assert_eq!(state.attempts.load(Ordering::SeqCst), 3);
    proxy.shutdown().await;
    frontend_task.abort();
    upstream_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_sequences_conflicts_quota_and_backend_changes_do_not_lose_pending_events()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut cfg = config("http://127.0.0.1:1".into());
    cfg.max_pending_bytes = 512;
    cfg.max_event_bytes = 512;
    cfg.max_streams = 1;
    cfg.max_concurrent_forwards = 1;
    let proxy = BesProxy::open(dir.path().join("bes"), cfg.clone()).await?;
    let (mut client, frontend_task) = frontend(proxy.clone()).await?;
    assert!(
        publish(&mut client, vec![event("missing-first", 2, false)])
            .await
            .is_err()
    );
    assert_eq!(
        publish(&mut client, vec![event("kept", 1, false)])
            .await?
            .len(),
        1
    );
    assert_eq!(
        publish(&mut client, vec![event("kept", 1, false)])
            .await?
            .len(),
        1
    );
    let conflicting = event("kept", 1, true);
    assert!(
        publish(&mut client, vec![conflicting.clone()])
            .await
            .is_err()
    );
    assert!(
        publish(&mut client, vec![event("another", 1, true)])
            .await
            .is_err()
    );
    // More valid events eventually hit a byte budget; all previously ACKed stay.
    for seq in 2..20 {
        if publish(&mut client, vec![event("kept", seq, false)])
            .await
            .is_err()
        {
            break;
        }
    }
    let count = proxy.metrics().pending_events;
    assert!((1..19).contains(&count));
    proxy.shutdown().await;
    frontend_task.abort();
    let _ = frontend_task.await;
    drop(client);
    drop(proxy);
    cfg.api_key = Some("different-credential".into());
    assert!(
        BesProxy::open(dir.path().join("bes"), cfg)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("different upstream or credential")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upstream_ack_mismatch_is_retained_and_reported_without_gc() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut state = Backend::new(true);
    state.bad_ack = true;
    let (url, upstream_task) = backend(state.clone()).await?;
    let proxy = BesProxy::open(dir.path().join("bes"), config(url)).await?;
    let (mut client, frontend_task) = frontend(proxy.clone()).await?;
    assert_eq!(
        publish(&mut client, vec![event("bad-ack", 1, true)])
            .await?
            .len(),
        1
    );
    wait(|| proxy.metrics().blocked_streams == 1).await;
    assert_eq!(proxy.metrics().pending_events, 1);
    assert_eq!(state.attempts.load(Ordering::SeqCst), 1);
    proxy.shutdown().await;
    frontend_task.abort();
    upstream_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_early_return_survives_restart_and_gc_waits_for_upstream() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let state = Backend::new(true);
    state.lifecycle_gate.send_replace(false);
    let (url, upstream_task) = backend(state.clone()).await?;
    let cfg = config(url);
    let proxy = BesProxy::open(dir.path().join("bes"), cfg.clone()).await?;
    let (mut client, frontend_task) = frontend(proxy.clone()).await?;
    let request = PublishLifecycleEventRequest {
        build_event: Some(OrderedBuildEvent {
            stream_id: Some(StreamId {
                build_id: "lifecycle-build".into(),
                invocation_id: String::new(),
                component: 1,
            }),
            sequence_number: 1,
            event: Some(BuildEvent {
                event: Some(build_event::Event::BuildEnqueued(Default::default())),
                ..Default::default()
            }),
        }),
        ..Default::default()
    };
    // The upstream unary RPC remains blocked; local success is durable admission.
    tokio::time::timeout(
        Duration::from_secs(2),
        client.publish_lifecycle_event(request.clone()),
    )
    .await??;
    wait(|| state.lifecycle.load(Ordering::SeqCst) == 1).await;
    assert_eq!(proxy.metrics().pending_lifecycle_events, 1);
    client.publish_lifecycle_event(request).await?;
    assert_eq!(
        proxy.metrics().pending_lifecycle_events,
        1,
        "pending unary retransmission is deduplicated"
    );
    proxy.shutdown().await;
    frontend_task.abort();
    let _ = frontend_task.await;
    drop(client);
    drop(proxy);
    let proxy = BesProxy::open(dir.path().join("bes"), cfg).await?;
    assert_eq!(proxy.metrics().pending_lifecycle_events, 1);
    state.lifecycle_gate.send_replace(true);
    wait(|| proxy.metrics().pending_lifecycle_events == 0).await;
    assert_eq!(proxy.metrics().pending_bytes, 0);
    assert_eq!(state.lifecycle.load(Ordering::SeqCst), 2);
    proxy.shutdown().await;
    upstream_task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_replays_only_unacknowledged_suffix_after_partial_upstream_ack() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let state = Backend::new(true);
    state.partial_failures.store(1, Ordering::SeqCst);
    let (url, upstream_task) = backend(state.clone()).await?;
    let mut cfg = config(url);
    cfg.retry_max_attempts = 0;
    let proxy = BesProxy::open(dir.path().join("bes"), cfg.clone()).await?;
    let (mut client, frontend_task) = frontend(proxy.clone()).await?;
    assert_eq!(
        publish(
            &mut client,
            vec![
                event("prefix", 1, false),
                event("prefix", 2, false),
                event("prefix", 3, true)
            ]
        )
        .await?
        .len(),
        3
    );
    wait(|| proxy.metrics().blocked_streams == 1).await;
    assert_eq!(
        proxy.metrics().pending_events,
        2,
        "only the upstream ACKed prefix is deleted"
    );
    proxy.shutdown().await;
    frontend_task.abort();
    let _ = frontend_task.await;
    drop(client);
    drop(proxy);
    let proxy = BesProxy::open(dir.path().join("bes"), cfg).await?;
    wait(|| proxy.metrics().pending_events == 0).await;
    let sequences = state
        .events
        .lock()
        .unwrap()
        .iter()
        .map(|event| event.ordered_build_event.as_ref().unwrap().sequence_number)
        .collect::<Vec<_>>();
    assert_eq!(sequences, vec![1, 2, 3, 2, 3]);
    proxy.shutdown().await;
    upstream_task.abort();
    Ok(())
}
