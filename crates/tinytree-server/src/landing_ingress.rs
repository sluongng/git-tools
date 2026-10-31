//! Landing admission and compressed unary bounds precede Prost allocation.
use crate::{
    RepositoryService,
    landing::{LandingService, Work},
};
use bytes::{Bytes, BytesMut};
use http_body::{Body as HttpBody, Frame};
use std::{
    convert::Infallible,
    future::{Future, poll_fn},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tonic::{Status, body::Body, metadata::MetadataMap, server::NamedService};
use tower::{Layer, Service, ServiceExt};
#[derive(Clone)]
pub struct LandingGrpcService {
    repository: RepositoryService,
}
impl LandingGrpcService {
    pub(crate) fn new(repository: RepositoryService) -> Self {
        Self { repository }
    }
}
impl NamedService for LandingGrpcService {
    const NAME: &'static str = "tinytree.landing.v1.Landing";
}
impl Service<http::Request<Body>> for LandingGrpcService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let repository = self.repository.clone();
        Box::pin(async move {
            let result = async {
                let metadata = MetadataMap::from_headers(request.headers().clone());
                let identity = repository.query_identity(&metadata)?;
                let method = match request.uri().path() {
                    "/tinytree.landing.v1.Landing/StartLanding" => "StartLanding",
                    "/tinytree.landing.v1.Landing/GetLanding" => "GetLanding",
                    "/tinytree.landing.v1.Landing/RecoverLanding" => "RecoverLanding",
                    _ => return Err(Status::unimplemented("unknown landing method")),
                };
                let host = repository.landing_host();
                if matches!(method, "StartLanding" | "RecoverLanding") {
                    if !host.config.enabled {
                        return Err(Status::unavailable("landing preparation is disabled"));
                    }
                    identity.landing_capability(&host.config.local_owner_profiles)?;
                }
                let deadline = repository.query_deadline(&metadata, false)?;
                let work: Arc<Work> = host.admit(method, deadline)?;
                let (mut parts, body) = request.into_parts();
                let idle =
                    Duration::from_millis(repository.service_config().rpc.stream_idle_timeout_ms);
                let bytes = unary(body, host.config.max_request_bytes, deadline, idle).await?;
                parts.extensions.insert(work.clone());
                let request = http::Request::from_parts(parts, Body::new(OnceBody(Some(bytes))));
                let service = repository
                    .deadline_layer()
                    .layer(LandingService::new(repository.clone()).grpc());
                tokio::time::timeout_at(deadline, service.oneshot(request))
                    .await
                    .map_err(|_| Status::deadline_exceeded("landing deadline exceeded"))?
                    .map(|r| r.map(Body::new))
                    .map_err(|never| -> Status { match never {} })
            }
            .await;
            Ok(match result {
                Ok(response) => response,
                Err(status) => status.into_http(),
            })
        })
    }
}
async fn unary(
    mut body: Body,
    cap: usize,
    deadline: tokio::time::Instant,
    idle: Duration,
) -> Result<Bytes, Status> {
    let mut bytes = BytesMut::new();
    loop {
        let frame = tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + idle),
            poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("landing request body deadline exceeded"))?;
        let Some(frame) = frame else {
            break;
        };
        let frame = frame.map_err(|_| Status::invalid_argument("landing request body failed"))?;
        if let Ok(data) = frame.into_data() {
            if data.len() > cap.saturating_add(5).saturating_sub(bytes.len()) {
                return Err(Status::resource_exhausted(
                    "landing compressed frame exceeds budget",
                ));
            }
            bytes.extend_from_slice(&data);
            if bytes.len() >= 5 {
                if bytes[0] > 1 {
                    return Err(Status::invalid_argument("invalid landing compression flag"));
                }
                let n = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
                if n > cap {
                    return Err(Status::resource_exhausted(
                        "landing compressed frame exceeds budget",
                    ));
                }
                if bytes.len() > n + 5 {
                    return Err(Status::invalid_argument(
                        "landing unary request has extra frames",
                    ));
                }
            }
        }
    }
    if bytes.len() < 5
        || bytes.len() != 5 + u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize
    {
        return Err(Status::invalid_argument(
            "landing unary request is truncated",
        ));
    }
    Ok(bytes.freeze())
}
struct OnceBody(Option<Bytes>);
impl HttpBody for OnceBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|b| Ok(Frame::data(b))))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Principal;
    use prost::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tinytree_proto::landing_v1 as p;
    const WRITER: &str = "landing-writer-public-fixture-token";
    const NATIVE: &str = "landing-native-public-fixture-token";
    struct Counted {
        bytes: Option<Bytes>,
        polls: Arc<AtomicUsize>,
    }
    impl HttpBody for Counted {
        type Data = Bytes;
        type Error = Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.bytes.take().map(|b| Ok(Frame::data(b))))
        }
    }
    fn service(
        root: &std::path::Path,
    ) -> (
        RepositoryService,
        tinytree_store::Store,
        p::StartLandingRequest,
    ) {
        let config: crate::Config =
            toml::from_str(include_str!("../examples/landing.toml")).unwrap();
        config.validate().unwrap();
        let store = tinytree_store::Store::open(
            root,
            config.store.materialize(config.service.max_metadata_bytes),
        )
        .unwrap();
        store.create_repository("example").unwrap();
        let fixture = tinytree_fixture::build_fixture(tinytree_fixture::FixtureConfig {
            unused_files: 1,
            bytes_per_file: 16,
        })
        .unwrap();
        for object in fixture.objects() {
            store
                .put("example", object.descriptor, &object.bytes, 0)
                .unwrap();
        }
        let base = fixture
            .objects()
            .iter()
            .find(|o| o.descriptor.key.kind == tinytree_model::Kind::Commit)
            .unwrap()
            .descriptor;
        let (_, bytes) = store.read_metadata("example", base.key).unwrap();
        let tinytree_model::Object::Commit(mut c) =
            tinytree_model::decode(&bytes, Default::default()).unwrap()
        else {
            panic!()
        };
        c.parents = vec![tinytree_model::CommitId(base.key.id.0)];
        c.change_id = tinytree_model::ChangeId([99; 16]);
        let source =
            tinytree_model::encode(&tinytree_model::Object::Commit(c), Default::default()).unwrap();
        store
            .put("example", source.descriptor, &source.bytes, 0)
            .unwrap();
        store
            .publish(
                "example",
                &tinytree_store::PublishRequest {
                    idempotency_key: "baseline".into(),
                    reference_name: "main".into(),
                    expected_generation: 0,
                    new_commit: Some(tinytree_model::CommitId(base.key.id.0)),
                    staging_leases: vec![],
                },
                0,
            )
            .unwrap();
        let service = RepositoryService::new(
            store.clone(),
            config.service,
            vec!["example".into()],
            vec![
                Principal::new("writer", WRITER, vec!["example".into()], true)
                    .unwrap()
                    .with_landing_profiles(vec!["checks".into()]),
                Principal::new("native", NATIVE, vec!["example".into()], true).unwrap(),
            ],
        )
        .unwrap();
        let input = p::StartLandingRequest {
            repository_id: "example".into(),
            idempotency_key: "cancel".into(),
            source_base: base.key.id.0.to_vec(),
            source_head: source.descriptor.key.id.0.to_vec(),
            destination: "main".into(),
            expected_generation: 1,
            expected_head: base.key.id.0.to_vec(),
            profile_name: "checks".into(),
            expected_profile_version: "v1".into(),
        };
        (service, store, input)
    }
    fn request(
        method: &str,
        token: Option<&str>,
        bytes: Vec<u8>,
    ) -> (http::Request<Body>, Arc<AtomicUsize>) {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut request = http::Request::builder()
            .method("POST")
            .uri(format!("/tinytree.landing.v1.Landing/{method}"))
            .header("content-type", "application/grpc")
            .header("grpc-timeout", "5000m")
            .body(Body::new(Counted {
                bytes: Some(bytes.into()),
                polls: polls.clone(),
            }))
            .unwrap();
        if let Some(token) = token {
            request
                .headers_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
        }
        (request, polls)
    }
    fn frame(message: &impl Message) -> Vec<u8> {
        let bytes = message.encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        frame.extend(bytes);
        frame
    }
    pub(super) async fn status(mut response: http::Response<Body>) -> String {
        if let Some(status) = response.headers().get("grpc-status") {
            return status.to_str().unwrap().to_owned();
        }
        loop {
            let Some(frame) = poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)).await
            else {
                panic!("missing grpc status");
            };
            if let Ok(trailers) = frame.unwrap().into_trailers() {
                if let Some(status) = trailers.get("grpc-status") {
                    return status.to_str().unwrap().to_owned();
                }
            }
        }
    }
    #[tokio::test]
    async fn landing_auth_grants_and_compressed_wire_bounds_precede_protobuf() {
        let directory = tempfile::tempdir().unwrap();
        let (service, _, input) = service(directory.path());
        for (token, expected) in [(None, "16"), (Some(NATIVE), "7")] {
            let (request, polls) = request("StartLanding", token, frame(&input));
            let response = service
                .landing_grpc_service()
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(status(response).await, expected);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }
        let mut oversized = vec![0];
        oversized.extend_from_slice(
            &((service.service_config().landing.max_request_bytes + 1) as u32).to_be_bytes(),
        );
        let (request, polls) = request("StartLanding", Some(WRITER), oversized);
        assert_eq!(
            status(
                service
                    .landing_grpc_service()
                    .oneshot(request)
                    .await
                    .unwrap()
            )
            .await,
            "8"
        );
        assert!(polls.load(Ordering::SeqCst) > 0);
    }
    #[tokio::test]
    async fn cancelled_landing_waiter_retains_heavy_admission_and_shutdown_waits_for_physical_owner()
     {
        let directory = tempfile::tempdir().unwrap();
        let (service, store, input) = service(directory.path());
        let host = service.landing_host();
        let mut metadata = MetadataMap::new();
        metadata.insert("authorization", format!("Bearer {WRITER}").parse().unwrap());
        let identity = service.query_identity(&metadata).unwrap();
        let owner = tinytree_store::LandingOwner(identity.landing_owner_hash());
        let config = service.service_config().landing;
        store
            .begin_landing(
                "example",
                owner,
                &tinytree_store::LandingRequest {
                    idempotency_key: "expire-under-quota".into(),
                    source_base: tinytree_model::CommitId::from_slice(&input.source_base).unwrap(),
                    source_head: tinytree_model::CommitId::from_slice(&input.source_head).unwrap(),
                    reference_name: input.destination.clone(),
                    expected_generation: 1,
                    expected_head: tinytree_model::CommitId::from_slice(&input.expected_head)
                        .unwrap(),
                    committer: tinytree_model::Signature {
                        name: config.committer_name,
                        email: config.committer_email,
                        timestamp: tinytree_model::Timestamp {
                            millis_since_epoch: 0,
                            tz_offset_minutes: 0,
                        },
                    },
                    check_profile: tinytree_store::LandingCheckProfile {
                        name: "checks".into(),
                        version: "v1".into(),
                    },
                    expires_at: 1,
                },
                0,
            )
            .unwrap();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (resume, release) = std::sync::mpsc::channel();
        host.pause_job(entered, release);
        let (rpc, _) = request("StartLanding", Some(WRITER), frame(&input));
        let ingress = service.landing_grpc_service();
        let waiter = tokio::spawn(async move { ingress.oneshot(rpc).await });
        waiting.await.unwrap();
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(host.active_jobs(), 1);
        let (rpc, polls) = request("StartLanding", Some(WRITER), frame(&input));
        assert_eq!(
            status(service.landing_grpc_service().oneshot(rpc).await.unwrap()).await,
            "8"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        let get = p::GetLandingRequest {
            repository_id: "example".into(),
            idempotency_key: "cancel".into(),

            inspect_recovery: false,
        };
        let (rpc, _) = request("GetLanding", Some(WRITER), frame(&get));
        assert_eq!(
            status(service.landing_grpc_service().oneshot(rpc).await.unwrap()).await,
            "5"
        );
        service.start_landing_worker().unwrap();
        // Expiry must progress without a Get RPC while the sole heavy-work
        // permit is still owned by the canceled, paused acceptance operation.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let job = store
                    .landing_job("example", owner, "expire-under-quota")
                    .unwrap()
                    .unwrap();
                if job.state == tinytree_store::LandingState::Expired {
                    assert!(!job.roots_retained);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let drain_service = service.clone();
        let drain = tokio::spawn(async move { drain_service.drain_landing().await });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        resume.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(host.active_jobs(), 0);
        let page = store
            .scan_pending_landings("example", None, Default::default())
            .unwrap();
        assert_eq!(page.jobs.len(), 1);
        assert_eq!(page.jobs[0].state, tinytree_store::LandingState::Intent);
    }
    #[tokio::test]
    async fn elapsed_original_deadline_does_not_accept_a_landing_after_blocking_owner_resumes() {
        let directory = tempfile::tempdir().unwrap();
        let (service, store, input) = service(directory.path());
        let host = service.landing_host();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (resume, release) = std::sync::mpsc::channel();
        host.pause_job(entered, release);
        let (mut rpc, _) = request("StartLanding", Some(WRITER), frame(&input));
        rpc.headers_mut()
            .insert("grpc-timeout", "50m".parse().unwrap());
        let ingress = service.landing_grpc_service();
        let waiter = tokio::spawn(async move { ingress.oneshot(rpc).await });
        waiting.await.unwrap();
        let response = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(status(response).await, "4");
        assert_eq!(host.active_jobs(), 1);
        resume.send(()).unwrap();
        service.drain_landing().await;
        assert!(
            store
                .scan_pending_landings("example", None, Default::default())
                .unwrap()
                .jobs
                .is_empty()
        );
    }
}

#[cfg(test)]
mod compression_window_tests {
    use super::*;
    use prost::Message;
    /// Zstd frame with unknown content size and one final raw block. The window
    /// descriptor advertises 2^window_log bytes even though the message is tiny.
    fn frame(payload: &[u8], window_log: u8) -> Vec<u8> {
        let mut zstd = vec![0x28, 0xb5, 0x2f, 0xfd, 0, (window_log - 10) << 3];
        let block = ((payload.len() as u32) << 3) | 1;
        zstd.extend_from_slice(&block.to_le_bytes()[..3]);
        zstd.extend_from_slice(payload);
        let mut grpc = vec![1];
        grpc.extend_from_slice(&(zstd.len() as u32).to_be_bytes());
        grpc.extend(zstd);
        grpc
    }
    #[tokio::test]
    async fn pinned_zstd_window_limit_and_status_reservation_cover_tiny_compressed_requests() {
        let mut config: crate::Config =
            toml::from_str(include_str!("../examples/landing.toml")).unwrap();
        config.validate().unwrap();
        let mut insufficient = config.clone();
        insufficient.service.landing.enabled = false;
        insufficient.service.landing.max_buffered_bytes = 128 * 1024 * 1024;
        assert!(insufficient.validate().is_err());
        config.service.landing.enabled = false;
        let directory = tempfile::tempdir().unwrap();
        let store = tinytree_store::Store::open(
            directory.path(),
            config.store.materialize(config.service.max_metadata_bytes),
        )
        .unwrap();
        store.create_repository("example").unwrap();
        let service =
            RepositoryService::new(store, config.service, vec!["example".into()], vec![]).unwrap();
        let payload = tinytree_proto::landing_v1::GetLandingRequest {
            repository_id: "example".into(),
            idempotency_key: "absent".into(),

            inspect_recovery: false,
        }
        .encode_to_vec();
        for (window, accepted) in [(27, true), (28, false)] {
            let request = http::Request::builder()
                .method("POST")
                .uri("/tinytree.landing.v1.Landing/GetLanding")
                .header("content-type", "application/grpc")
                .header("grpc-encoding", "zstd")
                .body(Body::new(OnceBody(Some(frame(&payload, window).into()))))
                .unwrap();
            let response = service
                .landing_grpc_service()
                .oneshot(request)
                .await
                .unwrap();
            let status = super::tests::status(response).await;
            if accepted {
                assert_eq!(status, "5");
            } else {
                assert_ne!(status, "5");
                assert_ne!(status, "0");
            }
        }
    }
}
