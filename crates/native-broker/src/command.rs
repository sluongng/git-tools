//! Host-owned command attribution for local RPCs, never upstream credentials.
use std::{
    any::Any,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{Status, body::Body, transport::Channel};
pub type CommandGuard = Arc<dyn Any + Send + Sync>;
pub const COMMAND_HEADER: &str = "native-command-bin";
/// The host binds each live token to a registered workspace and this exact
/// provider alias. The returned guard owns an accepted physical command job.
pub trait CommandResolver: Send + Sync + std::fmt::Debug {
    fn resolve(&self, store_alias: &str, token: [u8; 32]) -> Result<CommandGuard, Status>;
}
/// Trusted host access is separate from command attribution. The guard fences
/// the host's whole repository cohort while bootstrap or source I/O runs; it
/// does not attest that any workspace roots are complete or durably registered.
/// Implementations must not queue behind a collector while holding another
/// host scope: return Aborted for temporary collector contention before accepting
/// physical work. The broker waits cancellation-safely under the original
/// deadline; all other status codes fail immediately.
pub trait HostAccessResolver: Send + Sync + std::fmt::Debug {
    fn acquire_host_access(&self) -> Result<CommandGuard, Status>;
}
pub(crate) fn token(headers: &http::HeaderMap) -> Result<Option<[u8; 32]>, Status> {
    let metadata = tonic::metadata::MetadataMap::from_headers(headers.clone());
    let mut values = metadata.get_all_bin(COMMAND_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(Status::invalid_argument("duplicate native command token"));
    }
    let bytes = value
        .to_bytes()
        .map_err(|_| Status::invalid_argument("invalid native command token"))?;
    bytes
        .as_ref()
        .try_into()
        .map(Some)
        .map_err(|_| Status::invalid_argument("native command token must be 32 bytes"))
}
#[derive(Clone)]
pub(crate) struct CommandChannel {
    inner: Channel,
    token: Option<[u8; 32]>,
    owner: Option<CommandGuard>,
}
impl CommandChannel {
    pub fn new(inner: Channel) -> Self {
        Self {
            inner,
            token: None,
            owner: None,
        }
    }
    pub fn with_command_token(mut self, token: [u8; 32]) -> Self {
        self.token = Some(token);
        self
    }
    pub fn with_owner(mut self, owner: CommandGuard) -> Self {
        self.owner = Some(owner);
        self
    }
}
struct OwnedBody {
    body: Pin<Box<Body>>,
    _owner: CommandGuard,
}
impl http_body::Body for OwnedBody {
    type Data = bytes::Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Status>>> {
        self.body.as_mut().poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
impl tower::Service<http::Request<Body>> for CommandChannel {
    type Response = http::Response<Body>;
    type Error = tonic::transport::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.inner, cx)
    }
    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        if let Some(token) = self.token {
            let value =
                tonic::metadata::MetadataValue::<tonic::metadata::Binary>::from_bytes(&token);
            request.headers_mut().insert(
                COMMAND_HEADER,
                http::HeaderValue::from_bytes(value.as_encoded_bytes())
                    .expect("binary metadata is a valid header"),
            );
        }
        let owner = self.owner.clone();
        let request = request.map(|body| match &owner {
            Some(owner) => Body::new(OwnedBody {
                body: Box::pin(body),
                _owner: owner.clone(),
            }),
            None => body,
        });
        let response = tower::Service::call(&mut self.inner, request);
        Box::pin(async move {
            Ok(response.await?.map(|body| match owner {
                Some(owner) => Body::new(OwnedBody {
                    body: Box::pin(body),
                    _owner: owner,
                }),
                None => body,
            }))
        })
    }
}
