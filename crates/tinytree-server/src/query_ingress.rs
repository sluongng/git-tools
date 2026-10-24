//! Global query admission precedes generated protobuf decoding. The conservative
//! decoded-memory reservation remains owned by physical jobs and response bodies.
use crate::{RepositoryService, query_index::Work};
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tinytree_proto::query_v1::repository_query_server::{RepositoryQuery, RepositoryQueryServer};
use tonic::{
    Status, body::Body, codec::CompressionEncoding, metadata::MetadataMap, server::NamedService,
};
use tower::{Layer, Service};

/// Authenticated, globally bounded transport for the repository query service.
/// Custom listeners should use this wrapper, not only the raw trait adapter.
/// Taken exactly once by the cloud-open handler. The HTTP wrapper deliberately
/// holds no clone after insertion; the heavy proof owner ends with the Store job.
pub(crate) struct CloudProofAdmission(pub Mutex<Option<Arc<crate::cloud::Work>>>);
#[derive(Clone)]
pub struct QueryGrpcService {
    inner: tower::util::BoxCloneSyncService<http::Request<Body>, http::Response<Body>, Infallible>,
}
impl QueryGrpcService {
    pub(crate) fn new(repository: RepositoryService) -> Self {
        let adapter = repository.query_service();
        Self::with_adapter(repository, adapter)
    }
    fn with_adapter<T: RepositoryQuery + 'static>(
        repository: RepositoryService,
        adapter: T,
    ) -> Self {
        let config = repository.service_config();
        let raw = RepositoryQueryServer::new(adapter)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(config.max_request_bytes.min(config.query.max_request_bytes))
            .max_encoding_message_size(config.query.max_response_bytes);
        let timed = repository.deadline_layer().layer(Gate {
            inner: raw,
            repository,
        });
        let mapped = tower::ServiceBuilder::new()
            .map_response(|response: http::Response<_>| response.map(Body::new))
            .service(timed);
        Self {
            inner: tower::util::BoxCloneSyncService::new(mapped),
        }
    }
}
impl RepositoryService {
    /// Wrap a trusted server-side query adapter with the same authenticated,
    /// predecode admission, codec limits and deadline policy as the default
    /// transport. Observing adapters must forward request and response extensions
    /// when delegating to this repository's query service.
    pub fn query_grpc_service_with_adapter<T: RepositoryQuery + 'static>(
        &self,
        adapter: T,
    ) -> QueryGrpcService {
        QueryGrpcService::with_adapter(self.clone(), adapter)
    }
}
impl NamedService for QueryGrpcService {
    const NAME: &'static str = "tinytree.query.v1.RepositoryQuery";
}
impl Service<http::Request<Body>> for QueryGrpcService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        self.inner.call(request)
    }
}

#[derive(Clone)]
struct Gate<S> {
    inner: S,
    repository: RepositoryService,
}
impl<S> Service<http::Request<Body>> for Gate<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        let search = request.uri().path().ends_with("/Search");
        let history = request.uri().path().ends_with("/PathHistory")
            || request.uri().path().ends_with("/Blame");
        let cloud_open = request.uri().path().ends_with("/OpenCloudScope");
        let streaming = cloud_open
            || request.uri().path().ends_with("/OpenScope")
            || request.uri().path().ends_with("/ForkScope");
        let metadata = MetadataMap::from_headers(request.headers().clone());
        // Authenticate before body polling or revealing derived-host availability.
        let context =
            self.repository
                .query_identity(&metadata)
                .and_then(|identity| {
                    let cloud = if cloud_open {
                        identity.cloud_capability()?;
                        let cloud = self.repository.cloud_host().ok_or_else(|| {
                            Status::unimplemented("cloud query scopes are disabled")
                        })?;
                        if !cloud.config.indexed_versions {
                            return Err(Status::unimplemented("cloud query scopes are disabled"));
                        }
                        Some(cloud)
                    } else {
                        None
                    };
                    let deadline = self.repository.query_deadline(&metadata, streaming)?;
                    let host = self
                        .repository
                        .query_host()
                        .ok_or_else(|| Status::unavailable("derived queries are not configured"))?;
                    Ok((deadline, host, cloud))
                });
        let replacement = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, replacement);
        Box::pin(async move {
            let (deadline, host, cloud) = match context {
                Ok(context) => context,
                Err(status) => return Ok(status.into_http()),
            };
            let admission = if search {
                host.admit_search(deadline).await
            } else if history {
                host.admit_history(deadline).await
            } else {
                host.admit(deadline).await
            };
            let work: Arc<Work> = match admission {
                Ok(work) => work,
                Err(status) => return Ok(status.into_http()),
            };
            if let Some(cloud) = cloud {
                // Fail-fast second admission: never await an upgrade while the
                // ordinary query reservation is held.
                let proof = match cloud.admit("OpenCloudScope", deadline) {
                    Ok(proof) => proof,
                    Err(status) => return Ok(status.into_http()),
                };
                request
                    .extensions_mut()
                    .insert(Arc::new(CloudProofAdmission(Mutex::new(Some(proof)))));
            }
            request.extensions_mut().insert(work.clone());
            // Deadline/cancellation may drop a decoding or handler future, but
            // accepted blocking jobs retain their cloned Work until completion.
            // Do not renew the deadline after waiting for predecode admission.
            let mut response = match tokio::time::timeout_at(deadline, inner.call(request)).await {
                Ok(Ok(response)) => response,
                Ok(Err(never)) => match never {},
                Err(_) => {
                    return Ok(
                        Status::deadline_exceeded("derived query deadline exceeded").into_http()
                    );
                }
            };
            response
                .extensions_mut()
                .insert(crate::body_deadline::ResponseBudget::new(work));
            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Principal, ServiceConfig};
    use bytes::Bytes;
    use http_body::{Body as HttpBody, Frame};
    use prost::Message;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    use tinytree_proto::query_v1 as q;
    use tower::ServiceExt;

    const TOKEN: &str = "query-ingress-test-token";

    struct CountedBody {
        bytes: Option<Bytes>,
        polls: Arc<AtomicUsize>,
    }
    impl HttpBody for CountedBody {
        type Data = Bytes;
        type Error = Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.bytes.take().map(|bytes| Ok(Frame::data(bytes))))
        }
    }
    fn request(authenticated: bool) -> (http::Request<Body>, Arc<AtomicUsize>) {
        let message = q::QueryFactsRequest {
            scope: Some(q::ScopeKey {
                repository_id: "example".into(),
                handle: vec![0; 32],
            }),
            facts: vec![],
        }
        .encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
        frame.extend_from_slice(&message);
        let polls = Arc::new(AtomicUsize::new(0));
        let mut request = http::Request::builder()
            .method("POST")
            .uri("/tinytree.query.v1.RepositoryQuery/QueryFacts")
            .header("content-type", "application/grpc")
            .header("grpc-timeout", "100m")
            .body(Body::new(CountedBody {
                bytes: Some(frame.into()),
                polls: polls.clone(),
            }))
            .unwrap();
        if authenticated {
            request
                .headers_mut()
                .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        }
        (request, polls)
    }
    fn repository(path: &std::path::Path) -> RepositoryService {
        let store = tinytree_store::Store::open(path, Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let mut config = ServiceConfig::default();
        config.query.max_inflight = 1;
        RepositoryService::new(
            store,
            config,
            vec!["example".into()],
            vec![Principal::new("agent", TOKEN, vec!["example".into()], true).unwrap()],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn authentication_and_unavailable_index_do_not_poll_request_body() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = repository(temporary.path());
        for (authenticated, expected_status) in [(false, "16"), (true, "14")] {
            let (request, polls) = request(authenticated);
            let response = repository
                .query_grpc_service()
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(
                response.headers().get("grpc-status").unwrap(),
                expected_status
            );
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn unknown_size_zstd_window_is_bounded_by_generated_query_decoder() {
        let temporary = tempfile::tempdir().unwrap();
        let index =
            tinytree_index::Index::open(temporary.path().join("index"), &Default::default())
                .unwrap();
        let repository = repository(&temporary.path().join("objects"))
            .with_query_index(index)
            .unwrap();
        for (window, accepted) in [(27, true), (28, false)] {
            let (mut request, polls) = request(true);
            let payload = q::QueryFactsRequest {
                scope: Some(q::ScopeKey {
                    repository_id: "example".into(),
                    handle: vec![0; 32],
                }),
                facts: vec![],
            }
            .encode_to_vec();
            *request.body_mut() = Body::new(CountedBody {
                bytes: Some(crate::tests::zstd_window_frame(&payload, window).into()),
                polls,
            });
            request
                .headers_mut()
                .insert("grpc-encoding", "zstd".parse().unwrap());
            let response = repository
                .query_grpc_service()
                .oneshot(request)
                .await
                .unwrap();
            let status = response.headers().get("grpc-status").unwrap();
            if accepted {
                assert_eq!(status, "9", "accepted decoder reaches missing-scope lookup");
            } else {
                assert_ne!(status, "9");
                assert_ne!(status, "0");
            }
        }
    }

    #[tokio::test]
    async fn shared_admission_precedes_decoding_and_is_reused_by_handler() {
        let temporary = tempfile::tempdir().unwrap();
        let index =
            tinytree_index::Index::open(temporary.path().join("index"), &Default::default())
                .unwrap();
        let repository = repository(&temporary.path().join("objects"))
            .with_query_index(index)
            .unwrap();
        let host = repository.query_host().unwrap();
        let held = host
            .admit(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let service = repository.query_grpc_service();
        let (blocked, polls) = request(true);
        let response = service.clone().oneshot(blocked).await.unwrap();
        assert_eq!(response.headers().get("grpc-status").unwrap(), "4");
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(response);
        drop(held);

        // With a single global slot, taking a second permit after decoding would
        // time out instead of reaching the real missing-scope lookup.
        let (accepted, polls) = request(true);
        let response = service.oneshot(accepted).await.unwrap();
        assert_eq!(response.headers().get("grpc-status").unwrap(), "9");
        assert!(polls.load(Ordering::SeqCst) > 0);
        drop(response);
        assert!(
            host.admit(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
                .is_ok()
        );
    }
    #[tokio::test]
    async fn default_pool_preserves_two_complete_history_reservations() {
        let temporary = tempfile::tempdir().unwrap();
        let index =
            tinytree_index::Index::open(temporary.path().join("index"), &Default::default())
                .unwrap();
        let store =
            tinytree_store::Store::open(temporary.path().join("objects"), Default::default())
                .unwrap();
        store.create_repository("example").unwrap();
        let repository = RepositoryService::new(
            store,
            ServiceConfig::default(),
            vec!["example".into()],
            vec![],
        )
        .unwrap()
        .with_query_index(index)
        .unwrap();
        let host = repository.query_host().unwrap();
        let first = host
            .admit_history(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let second = host
            .admit_history(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(first.has_history_memory() && second.has_history_memory());
        let blocked = host
            .admit_history(tokio::time::Instant::now() + Duration::from_millis(10))
            .await
            .err()
            .unwrap();
        assert_eq!(blocked.code(), tonic::Code::DeadlineExceeded);
        drop(first);
        assert!(
            host.admit_history(tokio::time::Instant::now() + Duration::from_secs(1),)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn history_buffers_are_admitted_before_decoding_without_taxing_graph_calls() {
        let temporary = tempfile::tempdir().unwrap();
        let index =
            tinytree_index::Index::open(temporary.path().join("index"), &Default::default())
                .unwrap();
        let store =
            tinytree_store::Store::open(temporary.path().join("objects"), Default::default())
                .unwrap();
        store.create_repository("example").unwrap();
        let mut config = ServiceConfig::default();
        // Two existing graph reservations fit, but a graph plus history does
        // not. Do not enlarge the pool to hide a new tax on ordinary queries.
        config.query.max_request_memory_bytes =
            2 * config.query.request_memory(config.max_request_bytes);
        let repository = RepositoryService::new(
            store,
            config,
            vec!["example".into()],
            vec![Principal::new("agent", TOKEN, vec!["example".into()], true).unwrap()],
        )
        .unwrap()
        .with_query_index(index)
        .unwrap();
        let host = repository.query_host().unwrap();
        let held = host
            .admit(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(!held.has_history_memory());
        let service = repository.query_grpc_service();
        for method in ["PathHistory", "Blame"] {
            let (mut blocked, polls) = request(true);
            *blocked.uri_mut() = format!("/tinytree.query.v1.RepositoryQuery/{method}")
                .parse()
                .unwrap();
            let response = service.clone().oneshot(blocked).await.unwrap();
            assert_eq!(response.headers().get("grpc-status").unwrap(), "4");
            assert_eq!(
                polls.load(Ordering::SeqCst),
                0,
                "history buffers must be reserved before decoding"
            );
            drop(response);
        }
        let (ordinary, polls) = request(true);
        let response = service.clone().oneshot(ordinary).await.unwrap();
        assert_eq!(
            response.headers().get("grpc-status").unwrap(),
            "9",
            "the second ordinary graph call still reaches scope lookup"
        );
        assert!(polls.load(Ordering::SeqCst) > 0);
        drop(response);
        drop(held);
        for method in ["PathHistory", "Blame"] {
            let (mut accepted, polls) = request(true);
            *accepted.uri_mut() = format!("/tinytree.query.v1.RepositoryQuery/{method}")
                .parse()
                .unwrap();
            let response = service.clone().oneshot(accepted).await.unwrap();
            assert_eq!(
                response.headers().get("grpc-status").unwrap(),
                "9",
                "handler must reuse the one complete ingress reservation"
            );
            assert!(polls.load(Ordering::SeqCst) > 0);
            drop(response);
        }
        let history = host
            .admit_history(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(history.has_history_memory());
    }
    #[tokio::test]
    async fn unused_search_does_not_raise_graph_startup_memory_and_rejects_unfunded_work() {
        let temporary = tempfile::tempdir().unwrap();
        let index =
            tinytree_index::Index::open(temporary.path().join("index"), &Default::default())
                .unwrap();
        let store =
            tinytree_store::Store::open(temporary.path().join("objects"), Default::default())
                .unwrap();
        store.create_repository("example").unwrap();
        let mut config = ServiceConfig::default();
        config.query.max_search_manifest_bytes = 16 * 1024 * 1024;
        config.query.max_search_warm_memory_bytes = 256 * 1024 * 1024;
        config.query.max_request_memory_bytes = config
            .query
            .request_memory(config.max_request_bytes)
            .saturating_add(config.query.history_memory());
        let repository = RepositoryService::new(
            store,
            config,
            vec!["example".into()],
            vec![Principal::new("agent", TOKEN, vec!["example".into()], true).unwrap()],
        )
        .unwrap()
        .with_query_index(index)
        .unwrap();
        let service = repository.query_grpc_service();
        let (mut search, polls) = request(true);
        *search.uri_mut() = "/tinytree.query.v1.RepositoryQuery/Search".parse().unwrap();
        let response = service.clone().oneshot(search).await.unwrap();
        assert_eq!(response.headers().get("grpc-status").unwrap(), "8");
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(response);
        let (graph, polls) = request(true);
        let response = service.oneshot(graph).await.unwrap();
        assert_eq!(response.headers().get("grpc-status").unwrap(), "9");
        assert!(polls.load(Ordering::SeqCst) > 0);
    }
    #[tokio::test]
    async fn cloud_proof_admission_precedes_body_without_taxing_ordinary_queries() {
        let temporary = tempfile::tempdir().unwrap();
        let mut profile: crate::Config =
            toml::from_str(include_str!("../examples/cloud.toml")).unwrap();
        profile.service.cloud.indexed_versions = true;
        profile.service.cloud.max_inflight = 1;
        profile.service.query.max_inflight = 1;
        profile.validate().unwrap();
        let store = tinytree_store::Store::open(
            temporary.path().join("objects"),
            profile
                .store
                .materialize(profile.service.max_metadata_bytes),
        )
        .unwrap();
        store.create_repository("example").unwrap();
        let index =
            tinytree_index::Index::open(temporary.path().join("index"), &Default::default())
                .unwrap();
        let repository = RepositoryService::new(
            store,
            profile.service,
            vec!["example".into()],
            vec![
                Principal::new("agent", TOKEN, vec!["example".into()], true)
                    .unwrap()
                    .with_cloud(true),
            ],
        )
        .unwrap()
        .with_query_index(index)
        .unwrap();
        let cloud = repository.cloud_host().unwrap();
        let held = cloud
            .admit(
                "OpenCloudScope",
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .unwrap();
        let service = repository.query_grpc_service_with_adapter(repository.query_service());
        let (mut blocked, polls) = request(true);
        *blocked.uri_mut() = "/tinytree.query.v1.RepositoryQuery/OpenCloudScope"
            .parse()
            .unwrap();
        let response = service.clone().oneshot(blocked).await.unwrap();
        assert_eq!(response.headers().get("grpc-status").unwrap(), "8");
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "full proof admission must precede Prost/body polling"
        );
        drop(response);
        let (ordinary, polls) = request(true);
        let response = service.clone().oneshot(ordinary).await.unwrap();
        assert_eq!(
            response.headers().get("grpc-status").unwrap(),
            "9",
            "ordinary query reaches its real missing-scope lookup without a cloud proof reservation"
        );
        assert!(polls.load(Ordering::SeqCst) > 0);
        drop(response);
        drop(held);
        let malformed = q::OpenScopeFrame {
            frame: Some(q::open_scope_frame::Frame::ScopeEnd(Default::default())),
        }
        .encode_to_vec();
        let mut frame = vec![0];
        frame.extend_from_slice(&(malformed.len() as u32).to_be_bytes());
        frame.extend_from_slice(&malformed);
        let (accepted, polls) = request(true);
        let (mut parts, _) = accepted.into_parts();
        parts.uri = "/tinytree.query.v1.RepositoryQuery/OpenCloudScope"
            .parse()
            .unwrap();
        let accepted = http::Request::from_parts(
            parts,
            Body::new(CountedBody {
                bytes: Some(frame.into()),
                polls: polls.clone(),
            }),
        );
        let response = service.oneshot(accepted).await.unwrap();
        assert_eq!(response.headers().get("grpc-status").unwrap(), "3");
        assert!(polls.load(Ordering::SeqCst) > 0);
        drop(response);
        assert!(
            cloud
                .admit(
                    "OpenCloudScope",
                    tokio::time::Instant::now() + Duration::from_secs(1)
                )
                .is_ok(),
            "malformed header must release the one-shot proof owner"
        );
        assert!(
            repository
                .query_host()
                .unwrap()
                .admit(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
                .is_ok()
        );
    }
}
