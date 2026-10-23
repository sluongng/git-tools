//! Global query admission precedes generated protobuf decoding. The conservative
//! decoded-memory reservation remains owned by physical jobs and response bodies.
use crate::{RepositoryService, query_index::Work};
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{Status, body::Body, metadata::MetadataMap, server::NamedService};
use tower::{Layer, Service};

/// Authenticated, globally bounded transport for the repository query service.
/// Custom listeners should use this wrapper, not only the raw trait adapter.
#[derive(Clone)]
pub struct QueryGrpcService {
    inner: tower::util::BoxCloneSyncService<http::Request<Body>, http::Response<Body>, Infallible>,
}
impl QueryGrpcService {
    pub(crate) fn new(repository: RepositoryService) -> Self {
        let raw = repository.query_service().grpc();
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
        let streaming = request.uri().path().ends_with("/OpenScope")
            || request.uri().path().ends_with("/ForkScope");
        let metadata = MetadataMap::from_headers(request.headers().clone());
        // Authenticate before body polling or revealing derived-host availability.
        let context = self.repository.query_identity(&metadata).and_then(|_| {
            let deadline = self.repository.query_deadline(&metadata, streaming)?;
            let host = self
                .repository
                .query_host()
                .ok_or_else(|| Status::unavailable("derived queries are not configured"))?;
            Ok((deadline, host))
        });
        let replacement = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, replacement);
        Box::pin(async move {
            let (deadline, host) = match context {
                Ok(context) => context,
                Err(status) => return Ok(status.into_http()),
            };
            let work: Arc<Work> = match host.admit(deadline).await {
                Ok(work) => work,
                Err(status) => return Ok(status.into_http()),
            };
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
}
