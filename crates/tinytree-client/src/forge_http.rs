//! Ordinary HTTPS framing for the same bounded Forge semantic envelopes.
//! No raw repository method, redirect, compression or alternate-origin fallback.
use crate::Config;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{io::Write, sync::Arc};
use tinytree_proto::{FORGE_MAX_REQUEST_BYTES, FORGE_MAX_RESPONSE_BYTES, forge_v1 as f};
use tokio::time::Instant;
use tonic::{Code, Request, Status};
use tinytree_forge_core::wire::Reply;

type Owner = Arc<dyn std::any::Any + Send + Sync>;

#[derive(Clone)]
pub(crate) struct Client {
    client: reqwest::Client,
    endpoint: String,
}
#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct Call<'a> {
    version: u32,
    operation: i32,
    account_id: &'a str,
    view: &'a str,
    request_id: &'a str,
    payload_json: &'a str,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Failure {
    version: u32,
    error: FailureDetail,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FailureDetail {
    code: String,
    message: String,
}
struct BoundedBody(Vec<u8>);
impl Write for BoundedBody {
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        if input.len() > FORGE_MAX_REQUEST_BYTES.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("Forge HTTPS encoded request limit"));
        }
        self.0.extend_from_slice(input);
        Ok(input.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn invalid() -> Status {
    Status::data_loss("invalid bounded Forge HTTPS response")
}
fn transport_error(error: reqwest::Error) -> Status {
    if error.is_timeout() {
        Status::deadline_exceeded("Forge HTTPS deadline exceeded")
    } else {
        Status::unavailable("Forge HTTPS transport failed")
    }
}
impl Client {
    pub fn new(config: &Config, ca: Option<Arc<[u8]>>) -> Result<Self, Status> {
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(config.rpc.connect_timeout());
        if let Some(ca) = ca {
            let certificates = reqwest::Certificate::from_pem_bundle(&ca)
                .map_err(|_| Status::failed_precondition("invalid native TLS configuration"))?;
            if certificates.is_empty() {
                return Err(Status::failed_precondition(
                    "invalid native TLS configuration",
                ));
            }
            // Explicit trust is a replacement, not an addition to public roots.
            builder = builder.tls_built_in_root_certs(false);
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        Ok(Self {
            client: builder.build().map_err(|_| {
                Status::failed_precondition("cannot configure Forge HTTPS transport")
            })?,
            endpoint: format!(
                "{}/api/v1/forge/call",
                config.endpoint.trim_end_matches('/')
            ),
        })
    }
    pub async fn call(
        &self,
        request: Request<f::ForgeCallRequest>,
        deadline: Instant,
        owner: Option<Owner>,
    ) -> Result<f::ForgeCallResponse, Status> {
        let client = self.clone();
        // Cancellation of the waiter must not release admission while a physical
        // HTTP request/body/decoder is still running. This owned job has the
        // original attempt deadline, not a renewed background allowance.
        let job = tokio::spawn(async move {
            let _owner = owner;
            tokio::time::timeout_at(deadline, client.call_inner(request))
                .await
                .map_err(|_| Status::deadline_exceeded("Forge HTTPS deadline exceeded"))?
        });
        job.await
            .map_err(|_| Status::internal("Forge HTTPS request worker failed"))?
    }
    async fn call_inner(
        &self,
        request: Request<f::ForgeCallRequest>,
    ) -> Result<f::ForgeCallResponse, Status> {
        let (metadata, _, input) = request.into_parts();
        // These are ENCODED HTTP caps as well as the semantic caps checked by
        // NativeClient. Pathologically escaped JSON can hit the wire cap first;
        // never multiply the 8 MiB semantic reply into a 48 MiB HTTP allocation.
        let mut body = BoundedBody(Vec::new());
        serde_json::to_writer(
            &mut body,
            &Call {
                version: 1,
                operation: input.operation,
                account_id: &input.account_id,
                view: &input.view,
                request_id: &input.request_id,
                payload_json: std::str::from_utf8(&input.payload_json)
                    .map_err(|_| Status::invalid_argument("invalid Forge JSON bytes"))?,
            },
        )
        .map_err(|_| Status::invalid_argument("Forge HTTPS encoded request exceeds limit"))?;
        let mut request = self
            .client
            .post(&self.endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .body(body.0);
        if let Some(authorization) = metadata.get("authorization") {
            request = request.header(
                reqwest::header::AUTHORIZATION,
                authorization
                    .to_str()
                    .map_err(|_| Status::invalid_argument("invalid authorization metadata"))?,
            );
        }
        let response = request.send().await.map_err(transport_error)?;
        let status = response.status();
        if response
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .is_some_and(|value| value != "identity")
            || response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_none_or(|value| {
                    value.split(';').next().unwrap_or("").trim() != "application/json"
                })
        {
            return Err(invalid());
        }
        let cap = if status.is_success() {
            FORGE_MAX_RESPONSE_BYTES
        } else {
            8192
        };
        if response
            .content_length()
            .is_some_and(|size| size > cap as u64)
        {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(transport_error)?;
            if chunk.len() > cap.saturating_sub(bytes.len()) {
                return Err(invalid());
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            let failure: Failure = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if failure.version != 1 || failure.error.message.len() > 1024 {
                return Err(invalid());
            }
            let code = match (status.as_u16(), failure.error.code.as_str()) {
                (400, "invalid_argument") => Code::InvalidArgument,
                (400, "failed_precondition") => Code::FailedPrecondition,
                (400, "out_of_range") => Code::OutOfRange,
                (401, "unauthenticated") => Code::Unauthenticated,
                (403, "permission_denied") => Code::PermissionDenied,
                (404, "not_found") => Code::NotFound,
                (409, "aborted") => Code::Aborted,
                (409, "already_exists") => Code::AlreadyExists,
                (429, "resource_exhausted") => Code::ResourceExhausted,
                (500, "internal") => Code::Internal,
                (500, "unknown") => Code::Unknown,
                (500, "data_loss") => Code::DataLoss,
                (501, "unimplemented") => Code::Unimplemented,
                (503, "unavailable") => Code::Unavailable,
                (504, "deadline_exceeded") => Code::DeadlineExceeded,
                _ => return Err(invalid()),
            };
            return Err(Status::new(code, failure.error.message));
        }
        let reply = Reply::decode(&bytes).map_err(|_| invalid())?;
        Ok(f::ForgeCallResponse {
            operation: reply.operation,
            account_id: reply.account_id,
            view: reply.view,
            request_id: reply.request_id,
            state: reply.state,
            result_json: reply.result_json.into_bytes(),
        })
    }
}
