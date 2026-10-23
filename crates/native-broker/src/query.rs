//! Optional host-owned immutable graph queries. Local handles select captured
//! scopes; neither upstream credentials nor upstream cursors cross this facade.
use async_trait::async_trait;
use std::{any::Any, sync::Arc};
use tinytree_model::CommitId;
use tinytree_proto::query_v1 as q;
use tonic::Status;

/// Owner of physical request/job work only. It may contain an inflight slot;
/// implementations must not move it into persistent registry or reply owners.
pub type QueryOwner = Arc<dyn Any + Send + Sync>;
/// Independent owners follow accepted jobs and returned bytes through physical
/// teardown. This owner is independent of the input QueryOwner: it owns result
/// roots/storage/bytes, never an inflight transport slot.
pub struct QueryReply<T> {
    pub value: T,
    pub owner: QueryOwner,
}
impl<T: std::fmt::Debug> std::fmt::Debug for QueryReply<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryReply")
            .field("value", &self.value)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug)]
pub struct QueryResources {
    /// Maximum physical adapter work per call, excluding broker wire copies.
    pub memory_bytes: u64,
    pub temporary_bytes: u64,
    /// Registry, replay and captured SDK pools for this adapter, counted once
    /// across aliases that share the same adapter instance.
    pub persistent_memory_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryScope {
    pub handle: [u8; 32],
    pub expires_at_unix_seconds: i64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryHandle {
    pub handle: [u8; 32],
    pub expires_at_unix_seconds: i64,
}
#[derive(Clone, Debug)]
pub struct OpenQueryScope {
    pub roots: Vec<CommitId>,
    pub overlay: Vec<Vec<u8>>,
    pub replay_key: [u8; 32],
}
#[derive(Clone, Debug)]
pub struct ForkQueryScope {
    pub parent: [u8; 32],
    pub overlay: Vec<Vec<u8>>,
    pub replay_key: [u8; 32],
}
#[derive(Clone, Debug)]
pub struct BeginQuery {
    pub scope: [u8; 32],
    pub expression: q::GraphExpr,
    pub projection: q::Projection,
    pub page_size: u32,
    pub replay_key: [u8; 32],
}
#[derive(Clone, Debug)]
pub struct QueryPage {
    pub sequence: u64,
    pub rows: Vec<q::GraphRow>,
    pub exhausted: bool,
}
/// Aliases explicitly registered with the same adapter Arc are trusted synonyms
/// for the same handles/capability, not isolation boundaries. Distinct repository
/// or trust scopes require distinct adapters.
/// Registries, immutable-scope identity, persistent quotas and exact page/error
/// replay belong to the configured adapter, shared by local and RPC callers.
/// `page` accepts the expected sequence; replay must never advance it twice.
/// Before returning errors, implementations drain noncancellable owned jobs or
/// transfer their complete owner to those jobs. Never recursively call the same
/// admitted broker. Close releases logical state and is not proof of EOF.
#[async_trait]
pub trait NativeQueryIo: Send + Sync + std::fmt::Debug {
    fn resources(&self) -> QueryResources;
    async fn open_scope(
        &self,
        input: OpenQueryScope,
        owner: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, Status>;
    async fn fork_scope(
        &self,
        input: ForkQueryScope,
        owner: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, Status>;
    async fn facts(
        &self,
        scope: [u8; 32],
        facts: Vec<q::GraphFact>,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::GraphFacts>, Status>;
    async fn resolve_commit_prefix(
        &self,
        scope: [u8; 32],
        prefix: String,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::CommitPrefixResult>, Status>;
    async fn resolve_change_prefix(
        &self,
        scope: [u8; 32],
        prefix: String,
        visible_heads: Vec<CommitId>,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::ChangePrefixResult>, Status>;
    async fn shortest_prefix(
        &self,
        scope: [u8; 32],
        target: q::shortest_prefix_request::Target,
        visible_heads: Vec<CommitId>,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::ShortestPrefixResult>, Status>;
    async fn begin(
        &self,
        input: BeginQuery,
        owner: QueryOwner,
    ) -> Result<QueryReply<QueryHandle>, Status>;
    async fn page(
        &self,
        query: [u8; 32],
        expected_sequence: u64,
        owner: QueryOwner,
    ) -> Result<QueryReply<QueryPage>, Status>;
    async fn read_commit(
        &self,
        scope: [u8; 32],
        commit: CommitId,
        owner: QueryOwner,
    ) -> Result<QueryReply<jj_native_backend::VerifiedMetadata>, Status>;
    async fn close_query(&self, query: [u8; 32], owner: QueryOwner) -> Result<(), Status>;
    async fn close_scope(&self, scope: [u8; 32], owner: QueryOwner) -> Result<(), Status>;
}

/// Do not reflect upstream diagnostics, handles or connection metadata at the
/// local facade boundary. Preserve only the typed status category.
pub(crate) fn status(error: Status) -> Status {
    Status::new(
        if error.code() == tonic::Code::Ok {
            tonic::Code::Internal
        } else {
            error.code()
        },
        match error.code() {
            tonic::Code::InvalidArgument => "invalid native repository query request",
            tonic::Code::NotFound => "native repository query object unavailable",
            tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
                "native repository query access denied"
            }
            tonic::Code::ResourceExhausted => "native repository query limit exceeded",
            tonic::Code::DataLoss => "native repository query integrity validation failed",
            tonic::Code::DeadlineExceeded => "native repository query deadline exceeded",
            tonic::Code::Unavailable => {
                "native repository query unavailable; preserve the original replay identity"
            }
            tonic::Code::FailedPrecondition => {
                "native repository query scope or cursor is not available"
            }
            tonic::Code::AlreadyExists => {
                "native repository query replay identity has different inputs"
            }
            tonic::Code::Unimplemented => "native repository query operation is unsupported",
            tonic::Code::Cancelled => "native repository query cancelled",
            _ => "native repository query failed",
        },
    )
}
