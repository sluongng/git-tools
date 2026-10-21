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
async fn grouped_admission_commits_valid_prefix_and_replays_after_restart() -> Result<()> {
    use prost::Message;
    let dir = tempfile::tempdir()?;
    let state = Backend::new(false);
    let (url, upstream_task) = backend(state.clone()).await?;
    let events = vec![
        event("grouped", 1, false),
        event("grouped", 2, false),
        event("grouped", 3, true),
    ];
    let mut cfg = config(url);
    cfg.batch_max_events = 64;
    cfg.batch_max_delay_ms = 100;
    cfg.max_pending_bytes = (events[0].encoded_len() * 2 - 1) as u64;
    cfg.max_event_bytes = cfg.max_pending_bytes as usize;
    let proxy = BesProxy::open(dir.path().join("bes"), cfg.clone()).await?;
    let (mut client, task) = frontend(proxy.clone()).await?;
    let mut reply = client
        .publish_build_tool_event_stream(tokio_stream::iter(events.clone()))
        .await?
        .into_inner();
    assert_eq!(reply.message().await?.unwrap().sequence_number, 1);
    assert_eq!(
        reply.message().await.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(proxy.metrics().pending_events, 1);
    assert_eq!(proxy.metrics().admission_commits, 1);
    drop(reply);
    drop(client);
    task.abort();
    proxy.shutdown().await;
    drop(proxy);
    cfg.max_pending_bytes = 1024 * 1024;
    // Force the collector's byte-limit lookahead path on replay. The first
    // request above exercised a valid prefix and quota failure in one batch.
    cfg.batch_max_bytes = events[0].encoded_len() * 2 - 1;
    let proxy = BesProxy::open(dir.path().join("bes"), cfg).await?;
    assert_eq!(proxy.metrics().pending_events, 1);
    let (mut client, task) = frontend(proxy.clone()).await?;
    let acks = publish(&mut client, events.clone()).await?;
    assert_eq!(
        acks.iter()
            .map(|ack| ack.sequence_number)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(proxy.metrics().pending_events, 3);
    state.gate.send_replace(true);
    wait(|| proxy.metrics().pending_events == 0).await;
    for event in events {
        assert!(state.events.lock().unwrap().contains(&event));
    }
    drop(client);
    task.abort();
    proxy.shutdown().await;
    upstream_task.abort();
    Ok(())
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

/// Component benchmark, not an RBE timing test. Build the optimized test binary
/// with Bazel, then run it locally via tools/benchmark_bes_group_commit.py.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in local BES group-commit component benchmark"]
async fn benchmark_group_commit() -> Result<()> {
    use std::time::Instant;
    fn number(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .map(|s| s.parse().unwrap())
            .unwrap_or(default)
    }
    let streams = number("BES_BENCH_STREAMS", 1);
    let count = number("BES_BENCH_EVENTS", 4096);
    let payload_size = number("BES_BENCH_PAYLOAD_BYTES", 1024);
    let sparse = number("BES_BENCH_SPARSE", 0) != 0;
    anyhow::ensure!(
        streams > 0 && count > 0 && streams * count * payload_size <= 128 * 1024 * 1024
    );
    let mut random = 0x4d595df4d0f33173_u64;
    let payload = (0..payload_size)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random as u8
        })
        .collect::<Vec<_>>();
    let setup = Instant::now();
    let dir = tempfile::tempdir()?;
    let state = Backend::new(false);
    let (url, upstream_task) = backend(state.clone()).await?;
    let mut cfg = config(url);
    cfg.batch_max_events = number("BES_BENCH_BATCH_EVENTS", 1);
    cfg.batch_max_bytes = number("BES_BENCH_BATCH_BYTES", 256 * 1024);
    cfg.batch_max_delay_ms = number("BES_BENCH_DELAY_MS", 0) as u64;
    let proxy = BesProxy::open(dir.path().join("bes"), cfg).await?;
    let socket = dir.path().join("bes.sock");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let service = proxy.clone();
    let frontend_task = tokio::spawn(service.serve_unix_listener(listener, std::future::pending()));
    let channel = tonic::transport::Endpoint::from_static("http://localhost")
        .connect_with_connector(tower::service_fn(move |_| {
            let socket = socket.clone();
            async move {
                tokio::net::UnixStream::connect(socket)
                    .await
                    .map(hyper_util::rt::TokioIo::new)
            }
        }))
        .await?;
    let client = PublishBuildEventClient::new(channel)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);
    let events = (0..streams)
        .map(|stream| {
            (1..=count)
                .map(|sequence| {
                    let mut request = event(&format!("bench-{stream}"), sequence as i64, false);
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
                            output: Some(build_event::console_output::Output::BinaryOutput(
                                payload.clone(),
                            )),
                        },
                    ));
                    // Final event has no payload, as in a real BES tool stream.
                    if sequence == count {
                        request = event(&format!("bench-{stream}"), sequence as i64, true);
                    }
                    request
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let setup_seconds = setup.elapsed().as_secs_f64();
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for events in events {
        let mut client = client.clone();
        tasks.spawn(async move {
            let sent = Arc::new(Mutex::new(vec![None; events.len()]));
            let marks = sent.clone();
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let outgoing_events = events.clone();
            if sparse {
                sent.lock().unwrap()[0] = Some(Instant::now());
                tx.send(events[0].clone()).await?;
            }
            let mut response = if sparse {
                client
                    .publish_build_tool_event_stream(tokio_stream::wrappers::ReceiverStream::new(
                        rx,
                    ))
                    .await?
                    .into_inner()
            } else {
                client
                    .publish_build_tool_event_stream(async_stream::stream! {
                        for (index, event) in outgoing_events.into_iter().enumerate() {
                            marks.lock().unwrap()[index] = Some(Instant::now());
                            yield event;
                        }
                    })
                    .await?
                    .into_inner()
            };
            let mut latencies = Vec::with_capacity(events.len());
            for (index, event) in events.into_iter().enumerate() {
                if sparse && index > 0 {
                    sent.lock().unwrap()[index] = Some(Instant::now());
                    tx.send(event).await?;
                }
                let ack = response
                    .message()
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("missing local ACK"))?;
                anyhow::ensure!(
                    ack.sequence_number == index as i64 + 1,
                    "ACK order mismatch"
                );
                latencies
                    .push(sent.lock().unwrap()[index].unwrap().elapsed().as_secs_f64() * 1000.0);
            }
            drop(tx);
            anyhow::ensure!(response.message().await?.is_none(), "unexpected extra ACK");
            Ok::<_, anyhow::Error>(latencies)
        });
    }
    let mut latencies = Vec::new();
    while let Some(result) = tasks.join_next().await {
        latencies.extend(result??);
    }
    let wall_seconds = started.elapsed().as_secs_f64();
    let metrics = proxy.metrics();
    assert_eq!(metrics.pending_events, (streams * count) as u64);
    latencies.sort_by(f64::total_cmp);
    let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    state.gate.send_replace(true);
    let drain = Instant::now();
    tokio::time::timeout(Duration::from_secs(120), async {
        while proxy.metrics().pending_events != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await?;
    let drain_seconds = drain.elapsed().as_secs_f64();
    // Validate upstream receipt and byte identity, not only the local ACK rate.
    {
        let received = state.events.lock().unwrap();
        assert_eq!(received.len(), streams * count);
        for event in received.iter() {
            let ordered = event.ordered_build_event.as_ref().unwrap();
            if ordered.sequence_number < count as i64 {
                assert!(matches!(ordered.event.as_ref().unwrap().event.as_ref(),
                Some(build_event::Event::ConsoleOutput(output)) if output.output.as_ref() == Some(&build_event::console_output::Output::BinaryOutput(payload.clone()))));
            }
        }
    }
    println!(
        "BES_BENCH {}",
        serde_json::json!({"streams":streams,"events_per_stream":count,
        "payload_bytes":payload_size,"sparse":sparse,"wall_seconds":wall_seconds,
        "events_per_second":(streams*count) as f64/wall_seconds,
        "ack_p50_ms":percentile(0.50),"ack_p95_ms":percentile(0.95),"ack_p99_ms":percentile(0.99),
        "admission_commits":metrics.admission_commits,"setup_seconds":setup_seconds,
        "drain_seconds":drain_seconds,"admit_and_drain_seconds":wall_seconds+drain_seconds,
        "validated_events":streams*count})
    );
    proxy.shutdown().await;
    frontend_task.abort();
    upstream_task.abort();
    Ok(())
}
