//! Uncompressed Forge unary admission before body polling or Prost allocation.
use crate::{
    RepositoryService,
    forge::{ForgeService, admit},
};
use bytes::{Bytes, BytesMut};
use http_body::{Body as HttpBody, Frame};
use std::{
    convert::Infallible,
    future::{Future, poll_fn},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tinytree_proto::FORGE_MAX_REQUEST_BYTES;
use tonic::{Status, body::Body, metadata::MetadataMap, server::NamedService};
use tower::{Layer, Service, ServiceExt};

#[derive(Clone)]
pub struct ForgeGrpcService {
    repository: RepositoryService,
}
impl ForgeGrpcService {
    pub(crate) fn new(repository: RepositoryService) -> Self {
        Self { repository }
    }
}
impl NamedService for ForgeGrpcService {
    const NAME: &'static str = "tinytree.forge.v1.Forge";
}
impl Service<http::Request<Body>> for ForgeGrpcService {
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
                if request.uri().path() != "/tinytree.forge.v1.Forge/Call" {
                    return Err(Status::unimplemented("unknown forge method"));
                }
                if request
                    .headers()
                    .get("grpc-encoding")
                    .is_some_and(|v| v.as_bytes() != b"identity")
                {
                    return Err(Status::unimplemented("forge compression is unsupported"));
                }
                let metadata = MetadataMap::from_headers(request.headers().clone());
                let work = admit(&repository, &metadata).await?;
                let deadline = work.deadline;
                let idle =
                    Duration::from_millis(repository.service_config().rpc.stream_idle_timeout_ms);
                let (mut parts, body) = request.into_parts();
                let bytes = unary(body, deadline, idle).await?;
                parts.extensions.insert(work);
                let request = http::Request::from_parts(parts, Body::new(OnceBody(Some(bytes))));
                let service = repository
                    .deadline_layer()
                    .layer(ForgeService(repository.clone()).grpc());
                tokio::time::timeout_at(deadline, service.oneshot(request))
                    .await
                    .map_err(|_| Status::deadline_exceeded("forge request deadline exceeded"))?
                    .map(|response| response.map(Body::new))
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
        .map_err(|_| Status::deadline_exceeded("forge request body deadline exceeded"))?;
        let Some(frame) = frame else {
            break;
        };
        let frame = frame.map_err(|_| Status::invalid_argument("forge request body failed"))?;
        if let Ok(data) = frame.into_data() {
            if data.len()
                > FORGE_MAX_REQUEST_BYTES
                    .saturating_add(5)
                    .saturating_sub(bytes.len())
            {
                return Err(Status::resource_exhausted(
                    "forge frame exceeds byte budget",
                ));
            }
            bytes.extend_from_slice(&data);
            if bytes.len() >= 5 {
                if bytes[0] != 0 {
                    return Err(Status::invalid_argument("forge frame must be uncompressed"));
                }
                let size = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
                if size > FORGE_MAX_REQUEST_BYTES {
                    return Err(Status::resource_exhausted(
                        "forge frame exceeds byte budget",
                    ));
                }
                if bytes.len() > size + 5 {
                    return Err(Status::invalid_argument(
                        "forge unary request has extra frames",
                    ));
                }
            }
        }
    }
    if bytes.len() < 5
        || bytes.len() != 5 + u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize
    {
        return Err(Status::invalid_argument("forge unary request is truncated"));
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
    use prost::Message;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tinytree_proto::forge_v1 as p;

    const OWNER: &str = "forge-owner-public-fixture-token";
    const OTHER: &str = "forge-other-public-fixture-token";
    struct Counted {
        data: Option<Bytes>,
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
            Poll::Ready(self.data.take().map(|data| Ok(Frame::data(data))))
        }
    }
    fn fixture(root: &std::path::Path) -> RepositoryService {
        let store = tinytree_store::Store::open(root, Default::default()).unwrap();
        let mut config = crate::ServiceConfig::default();
        config.max_inflight = 1;
        config.max_inflight_per_repository = 1;
        RepositoryService::new(
            store,
            config,
            vec![],
            vec![
                crate::Principal::new_forge("owner", OWNER).unwrap(),
                crate::Principal::new_forge("other", OTHER).unwrap(),
            ],
        )
        .unwrap()
    }
    fn input(operation: p::ForgeOperation, payload: &str) -> p::ForgeCallRequest {
        p::ForgeCallRequest {
            operation: operation as i32,
            account_id: "sample".into(),
            view: String::new(),
            request_id: "intent-1".into(),
            payload_json: payload.as_bytes().to_vec(),
        }
    }
    fn request(
        message: &impl Message,
        token: Option<&str>,
    ) -> (http::Request<Body>, Arc<AtomicUsize>) {
        let bytes = message.encode_to_vec();
        let mut data = vec![0];
        data.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        data.extend(bytes);
        raw_request(data, token)
    }
    fn raw_request(data: Vec<u8>, token: Option<&str>) -> (http::Request<Body>, Arc<AtomicUsize>) {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut request = http::Request::builder()
            .uri("/tinytree.forge.v1.Forge/Call")
            .header("content-type", "application/grpc")
            .body(Body::new(Counted {
                data: Some(data.into()),
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
    async fn response(mut response: http::Response<Body>) -> (String, Vec<u8>) {
        let mut status = response
            .headers()
            .get("grpc-status")
            .map(|v| v.to_str().unwrap().to_owned());
        let mut data = Vec::new();
        while let Some(frame) = poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)).await {
            let frame = frame.unwrap();
            match frame.into_data() {
                Ok(bytes) => data.extend_from_slice(&bytes),
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        if let Some(value) = trailers.get("grpc-status") {
                            status = Some(value.to_str().unwrap().to_owned());
                        }
                    }
                }
            }
        }
        (status.expect("grpc status"), data)
    }
    async fn call(
        service: &RepositoryService,
        input: p::ForgeCallRequest,
        token: Option<&str>,
    ) -> (String, Option<p::ForgeCallResponse>) {
        let result = service
            .forge_grpc_service()
            .oneshot(request(&input, token).0)
            .await
            .unwrap();
        let (status, bytes) = response(result).await;
        let reply = if status == "0" {
            Some(p::ForgeCallResponse::decode(&bytes[5..]).unwrap())
        } else {
            None
        };
        (status, reply)
    }

    #[tokio::test]
    async fn forge_ingress_derives_identity_and_denies_payload_actor_and_cross_account_reads() {
        let root = tempfile::tempdir().unwrap();
        let service = fixture(root.path());
        let provision = input(p::ForgeOperation::ProvisionAccount, "{}");
        assert_eq!(call(&service, provision.clone(), None).await.0, "16");
        let forged = input(p::ForgeOperation::ProvisionAccount, r#"{"actor":"other"}"#);
        assert_eq!(call(&service, forged, Some(OWNER)).await.0, "3");
        let (status, first) = call(&service, provision.clone(), Some(OWNER)).await;
        assert_eq!(status, "0");
        let (_, replayed) = call(&service, provision, Some(OWNER)).await;
        assert_eq!(first, replayed);
        let first = first.unwrap();
        assert_eq!(first.account_id, "sample");
        assert_eq!(first.request_id, "intent-1");
        let account: serde_json::Value = serde_json::from_slice(&first.result_json).unwrap();
        assert_eq!(account["monorepo_id"], "forge-sample");
        // The retained local backend must not reinterpret provider bootstrap
        // commands as raw writes or silently enable another main-update path.
        for operation in [
            p::ForgeOperation::BeginBootstrapImport,
            p::ForgeOperation::PutBootstrapObject,
            p::ForgeOperation::AdvanceBootstrapImport,
            p::ForgeOperation::GetBootstrapImport,
            p::ForgeOperation::FinishBootstrapImport,
        ] {
            assert_eq!(call(&service, input(operation, "{}"), None).await.0, "16");
            assert_eq!(
                call(&service, input(operation, "{}"), Some(OWNER)).await.0,
                "12"
            );
        }
        let (_, unchanged) = call(
            &service,
            input(p::ForgeOperation::GetAccount, "{}"),
            Some(OWNER),
        )
        .await;
        let unchanged: serde_json::Value =
            serde_json::from_slice(&unchanged.unwrap().result_json).unwrap();
        assert_eq!(unchanged, account);
        assert_eq!(
            call(
                &service,
                input(p::ForgeOperation::GetAccount, "{}"),
                Some(OTHER)
            )
            .await
            .0,
            "7"
        );
        assert_eq!(
            call(&service, input(p::ForgeOperation::GetAccount, "{}"), None)
                .await
                .0,
            "16"
        );
    }

    #[tokio::test]
    async fn forge_ingress_auth_and_resource_admission_precede_body_and_live_response_holds_budget()
    {
        let root = tempfile::tempdir().unwrap();
        let service = fixture(root.path());
        let provision = input(p::ForgeOperation::ProvisionAccount, "{}");
        let (invalid, polls) = request(&provision, Some("invalid-public-fixture-token"));
        assert_eq!(
            response(service.forge_grpc_service().oneshot(invalid).await.unwrap())
                .await
                .0,
            "16"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        let first = service
            .forge_grpc_service()
            .oneshot(request(&provision, Some(OWNER)).0)
            .await
            .unwrap();
        // Do not consume the first response: its allocation still owns the sole
        // slot. The second request must time out without polling its body.
        let (mut second, polls) = request(&provision, Some(OWNER));
        second
            .headers_mut()
            .insert("grpc-timeout", "20m".parse().unwrap());
        assert_eq!(
            response(service.forge_grpc_service().oneshot(second).await.unwrap())
                .await
                .0,
            "4"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        drop(first);
        assert_eq!(call(&service, provision, Some(OWNER)).await.0, "0");
    }

    #[tokio::test]
    async fn forge_ingress_rejects_compressed_oversized_truncated_and_extra_unary_frames() {
        let root = tempfile::tempdir().unwrap();
        let service = fixture(root.path());
        for (data, code) in [
            (vec![1, 0, 0, 0, 0], "3"),
            (vec![0, 0, 0, 0, 1], "3"),
            (vec![0, 0, 0, 0, 0, 0], "3"),
            (vec![0, 0x7f, 0xff, 0xff, 0xff], "8"),
        ] {
            let (request, _) = raw_request(data, Some(OWNER));
            assert_eq!(
                response(service.forge_grpc_service().oneshot(request).await.unwrap())
                    .await
                    .0,
                code
            );
        }
    }
}
