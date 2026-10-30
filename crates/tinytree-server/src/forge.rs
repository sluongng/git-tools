//! Account-scoped product dispatch. Transport identity is never a payload field.
use crate::RepositoryService;
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use tinytree_proto::{FORGE_MAX_REQUEST_BYTES, FORGE_MAX_RESPONSE_BYTES, forge_v1 as p};
use tinytree_store::{AccountId, ForgeActor};
use tokio::{sync::OwnedSemaphorePermit, time::Instant};
use tonic::{Request, Response, Status, metadata::MetadataMap};

// Reserved before collecting a body. This covers envelope/JSON/native decode,
// domain scratch and the bounded encoded response; Store has its own resident
// allocation and closure budgets as well.
const CALL_MEMORY: usize = 64 * 1024 * 1024;

pub(crate) struct ForgeWork {
    _global: OwnedSemaphorePermit,
    _memory: Arc<OwnedSemaphorePermit>,
    actor: Option<ForgeActor>,
    pub(crate) deadline: Instant,
}

fn actor(
    service: &RepositoryService,
    metadata: &MetadataMap,
) -> Result<Option<ForgeActor>, Status> {
    let count = metadata.get_all("authorization").iter().count();
    if count == 0 {
        return Ok(None);
    }
    if count != 1 {
        return Err(Status::unauthenticated("invalid forge authorization"));
    }
    let identity = service.query_identity(metadata)?;
    Ok(Some(ForgeActor {
        principal: identity.forge_principal()?.to_owned(),
    }))
}

pub(crate) async fn admit(
    service: &RepositoryService,
    metadata: &MetadataMap,
) -> Result<Arc<ForgeWork>, Status> {
    let actor = actor(service, metadata)?;
    let deadline = service.query_deadline(metadata, false)?;
    let global = service.forge_global(deadline).await?;
    let memory = service.forge_memory(CALL_MEMORY, deadline).await?;
    Ok(Arc::new(ForgeWork {
        _global: global,
        _memory: memory,
        actor,
        deadline,
    }))
}

pub(crate) async fn admit_http(
    service: &RepositoryService,
    headers: &http::HeaderMap,
) -> Result<Arc<ForgeWork>, Status> {
    admit(service, &MetadataMap::from_headers(headers.clone())).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ForgeHttpRequest {
    operation: String,
    account_id: String,
    #[serde(default)]
    view: String,
    #[serde(default)]
    request_id: String,
    payload: Value,
}

fn http_request(input: ForgeHttpRequest) -> Result<p::ForgeCallRequest, Status> {
    if input.operation.is_empty()
        || input.operation.len() > 64
        || !input
            .operation
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b == b'_')
    {
        return Err(Status::invalid_argument("invalid forge operation"));
    }
    let name = format!("FORGE_OPERATION_{}", input.operation.to_ascii_uppercase());
    let operation = p::ForgeOperation::from_str_name(&name)
        .ok_or_else(|| Status::invalid_argument("unknown forge operation"))?;
    let payload_json = serde_json::to_vec(&input.payload)
        .map_err(|_| Status::invalid_argument("invalid forge payload"))?;
    Ok(p::ForgeCallRequest {
        operation: operation as i32,
        account_id: input.account_id,
        view: input.view,
        request_id: input.request_id,
        payload_json,
    })
}

pub(crate) async fn call_http_admitted(
    service: RepositoryService,
    headers: &http::HeaderMap,
    input: ForgeHttpRequest,
    work: Arc<ForgeWork>,
) -> Result<Value, Status> {
    // Admission belongs to this exact transport identity, not to JSON input.
    let current = actor(&service, &MetadataMap::from_headers(headers.clone()))?;
    if current.as_ref().map(|a| &a.principal) != work.actor.as_ref().map(|a| &a.principal) {
        return Err(Status::permission_denied(
            "forge admission identity changed",
        ));
    }
    let operation = input.operation.clone();
    let result = call_admitted(service, http_request(input)?, work).await?;
    let payload: Value = serde_json::from_slice(&result.result_json)
        .map_err(|_| Status::internal("invalid forge result"))?;
    Ok(
        json!({"schema_version":1,"operation":operation,"account_id":result.account_id,"view":result.view,"request_id":result.request_id,"state":result.state,"result":payload}),
    )
}

#[cfg(test)]
pub(crate) async fn call_http(
    service: RepositoryService,
    headers: &http::HeaderMap,
    input: ForgeHttpRequest,
) -> Result<Value, Status> {
    let work = admit_http(&service, headers).await?;
    call_http_admitted(service, headers, input, work).await
}

fn validate(input: &p::ForgeCallRequest) -> Result<p::ForgeOperation, Status> {
    let operation = p::ForgeOperation::try_from(input.operation)
        .map_err(|_| Status::invalid_argument("unknown forge operation"))?;
    if operation == p::ForgeOperation::Unspecified
        || input.encoded_len() > FORGE_MAX_REQUEST_BYTES
        || input.view.len() > 128
        || input.view.contains('\0')
        || input.request_id.len() > 128
        || !input
            .request_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        || input.payload_json.is_empty()
    {
        return Err(Status::invalid_argument("invalid forge request envelope"));
    }
    if matches!(
        operation,
        p::ForgeOperation::ProvisionAccount
            | p::ForgeOperation::SetMember
            | p::ForgeOperation::PublishView
            | p::ForgeOperation::RetireView
            | p::ForgeOperation::WriteObject
            | p::ForgeOperation::PushReview
            | p::ForgeOperation::DecideReview
            | p::ForgeOperation::LandReview
            | p::ForgeOperation::StartTransform
            | p::ForgeOperation::SetPin
            | p::ForgeOperation::RetirePin
    ) && input.request_id.is_empty()
    {
        return Err(Status::invalid_argument(
            "forge mutation requires request identity",
        ));
    }
    Ok(operation)
}

async fn call_admitted(
    service: RepositoryService,
    input: p::ForgeCallRequest,
    work: Arc<ForgeWork>,
) -> Result<p::ForgeCallResponse, Status> {
    let operation = validate(&input)?;
    let account = AccountId(input.account_id.clone());
    account.validate().map_err(|error| crate::service::store_status(error.into()))?;
    let profile = service.landing_host().forge_profile()?;
    let store = service.forge_store();
    let config = service.service_config();
    let deadline = work.deadline;
    let output = tokio::task::spawn_blocking(move || {
        // A timed-out caller cannot release physical admission while Store work
        // is still executing. Mutating domain methods own durable replay.
        let _physical_work = work;
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded(
                "forge operation deadline exceeded",
            ));
        }
        let now = crate::service::now()?;
        let millis = now
            .checked_mul(1000)
            .ok_or_else(|| Status::out_of_range("forge timestamp overflow"))?;
        let committer = tinytree_model::Signature {
            name: config.landing.committer_name,
            email: config.landing.committer_email,
            timestamp: tinytree_model::Timestamp {
                millis_since_epoch: millis,
                tz_offset_minutes: 0,
            },
        };
        let actor = _physical_work.actor.as_ref();
        let upload = crate::forge_upload::upload_call(
            &store,
            actor,
            operation,
            &account,
            &input.view,
            &input.request_id,
            &input.payload_json,
            now,
        )?;
        let result = match upload {
            Some(result) => Some(result),
            None => crate::forge_accounts::account_call(
                &store,
                actor,
                operation,
                &account,
                &input.view,
                &input.request_id,
                &input.payload_json,
                now,
            )?,
        };
        let result = if let Some(result) = result {
            result
        } else {
            let actor =
                actor.ok_or_else(|| Status::unauthenticated("forge authentication required"))?;
            if let Some(result) = crate::forge_review::review_call(
                &store,
                actor,
                operation,
                &account,
                &input.view,
                &input.request_id,
                &input.payload_json,
                &committer,
                profile.as_ref(),
                now,
            )? {
                result
            } else if let Some(result) = crate::forge_transforms::transform_call(
                &store,
                actor,
                operation,
                &account,
                &input.view,
                &input.request_id,
                &input.payload_json,
                &committer,
                profile.as_ref(),
                now,
            )? {
                result
            } else {
                return Err(Status::unimplemented("forge operation unavailable"));
            }
        };
        let result_json = serde_json::to_vec(&result.1)
            .map_err(|_| Status::internal("forge result encoding failed"))?;
        let response = p::ForgeCallResponse {
            operation: input.operation,
            state: result.0,
            result_json,
            account_id: input.account_id,
            view: input.view,
            request_id: input.request_id,
        };
        if response.encoded_len() > FORGE_MAX_RESPONSE_BYTES {
            return Err(Status::resource_exhausted(
                "forge response exceeds byte budget",
            ));
        }
        Ok(response)
    });
    tokio::time::timeout_at(deadline, output)
        .await
        .map_err(|_| Status::deadline_exceeded("forge operation deadline exceeded"))?
        .map_err(|_| Status::internal("forge Store worker failed"))?
}

#[derive(Clone)]
pub(crate) struct ForgeService(pub RepositoryService);
#[tonic::async_trait]
impl p::forge_server::Forge for ForgeService {
    async fn call(
        &self,
        request: Request<p::ForgeCallRequest>,
    ) -> Result<Response<p::ForgeCallResponse>, Status> {
        let work = request
            .extensions()
            .get::<Arc<ForgeWork>>()
            .cloned()
            .ok_or_else(|| Status::internal("forge predecode admission missing"))?;
        let value = call_admitted(self.0.clone(), request.into_inner(), work.clone()).await?;
        let mut response = Response::new(value);
        response
            .extensions_mut()
            .insert(crate::body_deadline::ResponseBudget::new(work));
        Ok(response)
    }
}

impl ForgeService {
    pub(crate) fn grpc(self) -> p::forge_server::ForgeServer<Self> {
        p::forge_server::ForgeServer::new(self)
            .max_decoding_message_size(FORGE_MAX_REQUEST_BYTES)
            .max_encoding_message_size(FORGE_MAX_RESPONSE_BYTES)
    }
}
