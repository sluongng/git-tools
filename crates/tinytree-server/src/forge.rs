//! Account-scoped product dispatch. Transport identity is never a payload field.
use crate::RepositoryService;
use prost::Message;
use std::sync::Arc;
use tinytree_forge_core::wire;
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
    identity: Option<ForgeAdmissionIdentity>,
    pub(crate) deadline: Instant,
}

#[derive(Clone, PartialEq, Eq)]
struct ForgeAdmissionIdentity {
    principal: String,
    credential_id: String,
}

fn transport_identity(
    service: &RepositoryService,
    metadata: &MetadataMap,
) -> Result<Option<ForgeAdmissionIdentity>, Status> {
    let count = metadata.get_all("authorization").iter().count();
    if count == 0 {
        return Ok(None);
    }
    if count != 1 {
        return Err(Status::unauthenticated("invalid forge authorization"));
    }
    let identity = service.query_identity(metadata)?;
    Ok(Some(ForgeAdmissionIdentity {
        principal: identity.forge_principal()?.to_owned(),
        credential_id: identity.forge_credential_id()?,
    }))
}

pub(crate) async fn admit(
    service: &RepositoryService,
    metadata: &MetadataMap,
) -> Result<Arc<ForgeWork>, Status> {
    let identity = transport_identity(service, metadata)?;
    let deadline = service.query_deadline(metadata, false)?;
    let global = service.forge_global(deadline).await?;
    let memory = service.forge_memory(CALL_MEMORY, deadline).await?;
    Ok(Arc::new(ForgeWork {
        _global: global,
        _memory: memory,
        identity,
        deadline,
    }))
}

pub(crate) async fn admit_http(
    service: &RepositoryService,
    headers: &http::HeaderMap,
) -> Result<Arc<ForgeWork>, Status> {
    admit(service, &MetadataMap::from_headers(headers.clone())).await
}

/// The HTTP layer has already strictly decoded the bounded shared envelope.
/// Admission belongs to the exact transport identity, never JSON input.
pub(crate) async fn call_http_admitted(
    service: RepositoryService,
    headers: &http::HeaderMap,
    input: wire::Call,
    work: Arc<ForgeWork>,
) -> Result<Vec<u8>, Status> {
    let current = transport_identity(&service, &MetadataMap::from_headers(headers.clone()))?;
    if current != work.identity {
        return Err(Status::permission_denied(
            "forge admission identity changed",
        ));
    }
    let result = call_admitted(
        service,
        p::ForgeCallRequest {
            operation: input.operation,
            account_id: input.account_id,
            view: input.view,
            request_id: input.request_id,
            payload_json: input.payload_json.into_bytes(),
        },
        work.clone(),
    )
    .await?;
    let reply = wire::Reply {
        version: 1,
        operation: result.operation,
        account_id: result.account_id,
        view: result.view,
        request_id: result.request_id,
        state: result.state,
        result_json: String::from_utf8(result.result_json)
            .map_err(|_| Status::internal("forge result encoding failed"))?,
    };
    let bytes = reply.encode().map_err(|error| match error {
        tinytree_forge_core::Error::Limit(_) => {
            Status::resource_exhausted("forge HTTP response exceeds byte budget")
        }
        _ => Status::internal("forge HTTP response encoding failed"),
    })?;
    if Instant::now() >= work.deadline {
        return Err(Status::deadline_exceeded(
            "forge HTTP response deadline exceeded",
        ));
    }
    Ok(bytes)
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
            | p::ForgeOperation::BeginBootstrapImport
            | p::ForgeOperation::PutBootstrapObject
            | p::ForgeOperation::AdvanceBootstrapImport
            | p::ForgeOperation::FinishBootstrapImport
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
    if matches!(
        operation,
        p::ForgeOperation::BeginBootstrapImport
            | p::ForgeOperation::PutBootstrapObject
            | p::ForgeOperation::AdvanceBootstrapImport
            | p::ForgeOperation::GetBootstrapImport
            | p::ForgeOperation::FinishBootstrapImport
    ) {
        if work.identity.is_none() {
            return Err(Status::unauthenticated(
                "bootstrap requires an authenticated owner",
            ));
        }
        return Err(Status::unimplemented(
            "bootstrap import is not available on this backend",
        ));
    }
    let account = AccountId(input.account_id.clone());
    account
        .validate()
        .map_err(|error| crate::service::store_status(error.into()))?;
    if operation == p::ForgeOperation::GetSession {
        let deadline = work.deadline;
        let output = tokio::task::spawn_blocking(move || {
            // Retain physical admission if the transport caller cancels while
            // current anonymous policy waits on the Store read gate.
            let _physical_work = work;
            if input.payload_json.len()
                > tinytree_forge_core::account_commands::MAX_ACCOUNT_COMMAND_BYTES
            {
                return Err(Status::resource_exhausted(
                    "forge session payload exceeds budget",
                ));
            }
            let _: tinytree_forge_core::account_commands::EmptyRequest =
                serde_json::from_slice(&input.payload_json)
                    .map_err(|_| Status::invalid_argument("invalid session payload"))?;
            if Instant::now() >= _physical_work.deadline {
                return Err(Status::deadline_exceeded("forge session deadline exceeded"));
            }
            // Missing auth is the existing explicit anonymous Forge path. Never
            // consult unauthenticated native local-owner identity for this reply.
            if _physical_work.identity.is_none() {
                service
                    .forge_store()
                    .forge_resolve_view(&account, None, &input.view, None)
                    .map_err(crate::service::store_status)?;
            }
            if Instant::now() >= _physical_work.deadline {
                return Err(Status::deadline_exceeded("forge session deadline exceeded"));
            }
            let identity = match _physical_work.identity.as_ref() {
                None => tinytree_forge_core::session::ForgeSessionIdentity::Anonymous {},
                Some(identity) => {
                    tinytree_forge_core::session::ForgeSessionIdentity::Authenticated {
                        principal: identity.principal.clone(),
                        credential_id: identity.credential_id.clone(),
                    }
                }
            };
            let session = tinytree_forge_core::session::ForgeSession {
                schema_version: 1,
                account_id: account.0.clone(),
                identity,
            };
            session
                .validate()
                .map_err(|_| Status::internal("invalid session assertion"))?;
            Ok(p::ForgeCallResponse {
                operation: input.operation,
                state: "complete".into(),
                result_json: serde_json::to_vec(&session)
                    .map_err(|_| Status::internal("session encoding failed"))?,
                account_id: input.account_id,
                view: input.view,
                request_id: input.request_id,
            })
        });
        return tokio::time::timeout_at(deadline, output)
            .await
            .map_err(|_| Status::deadline_exceeded("forge session deadline exceeded"))?
            .map_err(|_| Status::internal("forge session worker failed"))?;
    }
    let profile = service.landing_host().forge_profile()?;
    let store = service.forge_store();
    let forge_reads = service.forge_read_api();
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
        let actor = _physical_work.identity.as_ref().map(|identity| ForgeActor {
            principal: identity.principal.clone(),
        });
        let actor = actor.as_ref();
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
                &forge_reads,
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
