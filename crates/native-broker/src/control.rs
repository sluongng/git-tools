//! Host-supplied native repository operations. The broker never chooses a
//! caller's expected reference generation or accepts remote connection settings.
use async_trait::async_trait;
use tinytree_model::CommitId;
use tonic::Status;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlResources {
    pub memory_bytes: u64,
    pub temporary_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmittedCommit {
    pub head: CommitId,
    pub metadata_objects: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceSnapshot {
    pub name: String,
    pub generation: u64,
    pub head: Option<AdmittedCommit>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishRequest {
    pub name: String,
    pub head: CommitId,
    pub expected_generation: u64,
    pub idempotency_key: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfirmedPublication {
    pub idempotency_key: String,
    pub name: String,
    pub generation: u64,
    pub head: CommitId,
}
#[async_trait]
pub trait NativeRepositoryControl: Send + Sync + std::fmt::Debug {
    // Returning an error must not abandon uncharged blocking/staging work.
    // Adapters drain noncancellable jobs before returning, including timeout
    // paths; the broker keeps its resource scope until this future completes.
    /// Conservative adapter work limits, checked before registering the alias.
    /// Implementations must not recursively call this broker from an operation.
    fn resources(&self) -> ControlResources;
    /// Independently budgeted, short landing preparation/status work. Returning
    /// None disables the operation; ordinary publication remains unchanged.
    fn landing_resources(&self) -> Option<ControlResources> {
        None
    }
    async fn landing_with_owner(
        &self,
        _intent: crate::LandingIntent,
        _submit: bool,
        _owner: crate::CommandGuard,
    ) -> Result<crate::LandingReport, Status> {
        Err(Status::unimplemented("native landing is not configured"))
    }

    /// Opt-in physical owner contract: accepted transport, verification and
    /// blocking work retain this guard after caller cancellation or timeout.
    async fn observe_reference_with_owner(
        &self,
        _name: &str,
        _owner: crate::CommandGuard,
    ) -> Result<ReferenceSnapshot, Status> {
        Err(Status::unimplemented(
            "owned native reference observation unsupported",
        ))
    }
    async fn admit_commit_with_owner(
        &self,
        _head: CommitId,
        _owner: crate::CommandGuard,
    ) -> Result<AdmittedCommit, Status> {
        Err(Status::unimplemented(
            "owned native commit admission unsupported",
        ))
    }
    async fn publish_with_owner(
        &self,
        _request: PublishRequest,
        _owner: crate::CommandGuard,
    ) -> Result<ConfirmedPublication, Status> {
        Err(Status::unimplemented(
            "owned native publication unsupported",
        ))
    }
    async fn publication_with_owner(
        &self,
        _key: &str,
        _owner: crate::CommandGuard,
    ) -> Result<Option<ConfirmedPublication>, Status> {
        Err(Status::unimplemented(
            "owned native publication lookup unsupported",
        ))
    }

    /// Return name, generation and optional head from ONE initial observation.
    /// A present head is returned only after retained-root admission. This does
    /// not imply that its full metadata closure has been fetched locally.
    async fn observe_reference(&self, name: &str) -> Result<ReferenceSnapshot, Status>;
    async fn admit_commit(&self, head: CommitId) -> Result<AdmittedCommit, Status>;
    /// Use exactly the caller's expected generation and original replay key.
    /// Aborted is reserved for authoritative stale-CAS rejection: monotonic
    /// generations make this exact intent unable to commit in the future.
    async fn publish(&self, request: PublishRequest) -> Result<ConfirmedPublication, Status>;
    /// Resolve only this durable intent. None is no confirmed receipt, NEVER
    /// proof that upstream did not commit. Pending work may return Unavailable.
    async fn publication(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<ConfirmedPublication>, Status>;
}

pub(crate) fn reference_name(name: &str) -> Result<(), Status> {
    if name.is_empty()
        || name.len() > 512
        || name.contains('\0')
        || name == "_tinytree"
        || name.starts_with("_tinytree/")
    {
        return Err(Status::invalid_argument(
            "invalid native public reference name",
        ));
    }
    Ok(())
}
pub(crate) fn commit(head: CommitId) -> Result<(), Status> {
    if head == CommitId::ROOT {
        return Err(Status::invalid_argument(
            "intrinsic root is not a native publication target",
        ));
    }
    Ok(())
}
pub(crate) fn key(value: &str) -> Result<(), Status> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Status::invalid_argument("invalid native publication key"));
    }
    Ok(())
}
pub(crate) fn status(error: Status) -> Status {
    let message = match error.code() {
        tonic::Code::InvalidArgument => "invalid native repository control request",
        tonic::Code::NotFound => "native reference or publication unavailable",
        tonic::Code::Aborted => "native reference generation changed; fetch before retrying",
        tonic::Code::AlreadyExists => "native publication key has another intent",
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
            "native repository control access denied"
        }
        tonic::Code::ResourceExhausted => tinytree_proto::PublicationFailureReason::from_message(error.message())
            .map(|reason| reason.message())
            .unwrap_or("native repository control limit exceeded"),
        tonic::Code::DataLoss => "native repository integrity validation failed",
        tonic::Code::DeadlineExceeded => "native repository control deadline exceeded",
        tonic::Code::Unavailable => {
            "native repository control outcome is unavailable; preserve the original intent"
        }
        tonic::Code::FailedPrecondition => "native repository control is not configured or ready",
        _ => "native repository control failed",
    };
    Status::new(
        if error.code() == tonic::Code::Ok {
            tonic::Code::Internal
        } else {
            error.code()
        },
        message,
    )
}
