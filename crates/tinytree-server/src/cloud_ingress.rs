//! Cloud admission and compressed-wire framing precede Prost decoding.
use crate::{
    RepositoryService,
    cloud::{CloudService, Work},
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
pub struct CloudGrpcService {
    repository: RepositoryService,
}
impl CloudGrpcService {
    pub(crate) fn new(repository: RepositoryService) -> Self {
        Self { repository }
    }
}
impl NamedService for CloudGrpcService {
    const NAME: &'static str = "tinytree.cloud.v1.WorkspaceCloud";
}
impl Service<http::Request<Body>> for CloudGrpcService {
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
                identity.cloud_capability()?;
                let host = repository
                    .cloud_host()
                    .ok_or_else(|| Status::unavailable("private workspace service is disabled"))?;
                let method = request
                    .uri()
                    .path()
                    .rsplit('/')
                    .next()
                    .unwrap_or("")
                    .to_owned();
                if !matches!(
                    method.as_str(),
                    "PutObject"
                        | "PutIndexArtifact"
                        | "LookupIndexArtifacts"
                        | "ReadIndexArtifact"
                        | "ReadNativeClosure"
                        | "ReadCapture"
                        | "LookupObjects"
                        | "ReadObject"
                        | "PublishVersion"
                        | "GetWorkspace"
                        | "ListWorkspaces"
                        | "ListVersions"
                        | "GetVersion"
                        | "GetPublication"
                ) {
                    return Err(Status::unimplemented("unknown private workspace method"));
                }
                if matches!(
                    method.as_str(),
                    "PutObject" | "PutIndexArtifact" | "PublishVersion"
                ) {
                    identity.cloud_write_capability()?;
                }
                let deadline = repository.query_deadline(
                    &metadata,
                    matches!(
                        method.as_str(),
                        "ReadObject" | "ReadIndexArtifact" | "ReadNativeClosure" | "ReadCapture"
                    ),
                )?;
                let work: Arc<Work> = host.admit(&method, deadline)?;
                let cap = host.config.wire_limit(&method);
                let (mut parts, body) = request.into_parts();
                let idle =
                    Duration::from_millis(repository.service_config().rpc.stream_idle_timeout_ms);
                let bytes = unary(body, cap, deadline, idle).await?;
                parts.extensions.insert(work.clone());
                let request = http::Request::from_parts(parts, Body::new(OnceBody(Some(bytes))));
                let service = CloudService::new(repository.clone()).grpc(cap);
                // This layer transfers both the per-call work and any read-root
                // guard from response extensions into the actual encoded body.
                let service = repository.deadline_layer().layer(service);
                let response = tokio::time::timeout_at(deadline, service.oneshot(request))
                    .await
                    .map_err(|_| Status::deadline_exceeded("private workspace deadline exceeded"))?
                    .map_err(|never| -> Status { match never {} })?;
                Ok::<_, Status>(response.map(Body::new))
            }
            .await;
            Ok(match result {
                Ok(response) => response,
                Err(status) => status.into_http(),
            })
        })
    }
}
// A unary request has exactly one gRPC message. Buffer only the bounded raw
// compressed frame; reject its declared size before protobuf decoding.
// Tonic separately caps decompressed bytes before constructing the Prost value.
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
        .map_err(|_| {
            Status::deadline_exceeded("private workspace request body deadline exceeded")
        })?;
        let Some(frame) = frame else {
            break;
        };
        let frame =
            frame.map_err(|_| Status::invalid_argument("private workspace request body failed"))?;
        if let Ok(data) = frame.into_data() {
            if data.len() > cap.saturating_add(5).saturating_sub(bytes.len()) {
                return Err(Status::resource_exhausted(
                    "private workspace compressed frame exceeds budget",
                ));
            }
            bytes.extend_from_slice(&data);
            if bytes.len() >= 5 {
                if bytes[0] > 1 {
                    return Err(Status::invalid_argument(
                        "invalid private workspace compression flag",
                    ));
                }
                let n = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
                if n > cap {
                    return Err(Status::resource_exhausted(
                        "private workspace compressed frame exceeds budget",
                    ));
                }
                if bytes.len() > n + 5 {
                    return Err(Status::invalid_argument(
                        "private workspace unary request contains extra frames",
                    ));
                }
            }
        }
    }
    if bytes.len() < 5
        || bytes.len() != 5 + u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize
    {
        return Err(Status::invalid_argument(
            "private workspace unary request is truncated",
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
        Poll::Ready(self.0.take().map(|bytes| Ok(Frame::data(bytes))))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Principal};
    use prost::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tinytree_proto::cloud_v1 as p;
    const WRITER: &str = "cloud-ingress-writer-token";
    const READER: &str = "cloud-ingress-reader-token";
    const DISABLED: &str = "cloud-ingress-disabled-token";
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
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(self.bytes.take().map(|bytes| Ok(Frame::data(bytes))))
        }
    }
    fn service(path: &std::path::Path) -> RepositoryService {
        let mut profile: Config = toml::from_str(include_str!("../examples/cloud.toml")).unwrap();
        profile.service.cloud.max_inflight = 1;
        let store = tinytree_store::Store::open(
            path,
            profile
                .store
                .materialize(profile.service.max_metadata_bytes),
        )
        .unwrap();
        store.create_repository("example").unwrap();
        RepositoryService::new(
            store,
            profile.service,
            vec!["example".into()],
            vec![
                Principal::new("writer", WRITER, vec!["example".into()], true)
                    .unwrap()
                    .with_cloud(true),
                Principal::new("reader", READER, vec!["example".into()], false)
                    .unwrap()
                    .with_cloud(true),
                Principal::new("native-only", DISABLED, vec!["example".into()], true).unwrap(),
            ],
        )
        .unwrap()
    }
    fn request(
        method: &str,
        token: Option<&str>,
        bytes: Vec<u8>,
    ) -> (http::Request<Body>, Arc<AtomicUsize>) {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut request = http::Request::builder()
            .method("POST")
            .uri(format!("/tinytree.cloud.v1.WorkspaceCloud/{method}"))
            .header("content-type", "application/grpc")
            .header("grpc-timeout", "200m")
            .body(Body::new(CountedBody {
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
    fn workspace_frame() -> Vec<u8> {
        let bytes = p::GetWorkspaceRequest {
            repository_id: "example".into(),
            workspace_id: vec![1; 32],
        }
        .encode_to_vec();
        let mut framed = vec![0];
        framed.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        framed.extend(bytes);
        framed
    }
    #[tokio::test]
    async fn cloud_authentication_capability_and_write_policy_precede_body_polling() {
        let directory = tempfile::tempdir().unwrap();
        let service = service(directory.path());
        for (method, token, status) in [
            ("GetWorkspace", None, "16"),
            ("GetWorkspace", Some(DISABLED), "7"),
            ("PutObject", Some(READER), "7"),
        ] {
            let (request, polls) = request(method, token, workspace_frame());
            let response = service.cloud_grpc_service().oneshot(request).await.unwrap();
            assert_eq!(response.headers().get("grpc-status").unwrap(), status);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
        }
    }
    async fn response_status(mut response: http::Response<Body>) -> String {
        if let Some(status) = response.headers().get("grpc-status") {
            return status.to_str().unwrap().to_owned();
        }
        loop {
            let frame = poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx))
                .await
                .expect("missing grpc status")
                .unwrap();
            if let Ok(trailers) = frame.into_trailers() {
                if let Some(status) = trailers.get("grpc-status") {
                    return status.to_str().unwrap().to_owned();
                }
            }
        }
    }

    #[tokio::test]
    async fn unknown_size_zstd_window_is_bounded_by_generated_cloud_decoder() {
        let directory = tempfile::tempdir().unwrap();
        let service = service(directory.path());
        let payload = p::GetWorkspaceRequest {
            repository_id: "example".into(),
            workspace_id: vec![1; 32],
        }
        .encode_to_vec();
        for (window, accepted) in [(27, true), (28, false)] {
            let (mut request, _) = request(
                "GetWorkspace",
                Some(WRITER),
                crate::tests::zstd_window_frame(&payload, window),
            );
            request
                .headers_mut()
                .insert("grpc-encoding", "zstd".parse().unwrap());
            let response = service.cloud_grpc_service().oneshot(request).await.unwrap();
            let status = response_status(response).await;
            if accepted {
                assert_eq!(
                    status, "0",
                    "accepted decoder reaches the empty-workspace lookup"
                );
            } else {
                assert_ne!(status, "0");
            }
        }
    }

    #[tokio::test]
    async fn cloud_compressed_frame_declaration_is_bounded_before_protobuf_decode() {
        let directory = tempfile::tempdir().unwrap();
        let service = service(directory.path());
        let cap = service.service_config().cloud.max_request_bytes;
        let mut bytes = vec![1];
        bytes.extend_from_slice(&((cap + 1) as u32).to_be_bytes());
        let (request, polls) = request("GetWorkspace", Some(WRITER), bytes);
        let response = service.cloud_grpc_service().oneshot(request).await.unwrap();
        assert_eq!(response.headers().get("grpc-status").unwrap(), "8");
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canceled_cloud_rpc_retains_physical_admission_and_shutdown_drain_until_job_finishes() {
        let directory = tempfile::tempdir().unwrap();
        let service = service(directory.path());
        let host = service.cloud_host().unwrap();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (resume, released) = std::sync::mpsc::channel();
        host.pause_job(entered, released);
        let (first_request, _) = request("GetWorkspace", Some(WRITER), workspace_frame());
        let first = tokio::spawn(service.cloud_grpc_service().oneshot(first_request));
        started.await.unwrap();
        let response = first.await.unwrap().unwrap();
        assert_eq!(response.headers().get("grpc-status").unwrap(), "4");
        drop(response);
        assert_eq!(host.active_jobs(), 1);
        let (next, polls) = request("GetWorkspace", Some(WRITER), workspace_frame());
        let denied = service.cloud_grpc_service().oneshot(next).await.unwrap();
        assert_eq!(denied.headers().get("grpc-status").unwrap(), "8");
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(denied);
        let drain_host = host.clone();
        let mut drain = tokio::spawn(async move {
            drain_host.drain().await;
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut drain)
                .await
                .is_err()
        );
        resume.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), drain)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(host.active_jobs(), 0);
    }
}
