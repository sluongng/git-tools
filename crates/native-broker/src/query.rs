//! Optional host-owned immutable graph queries. Local handles select captured
//! scopes; neither upstream credentials nor upstream cursors cross this facade.
use async_trait::async_trait;
use std::{any::Any, sync::Arc};
use tinytree_model::CommitId;
use tinytree_proto::query_v1 as q;
use tonic::Status;

/// Creation policy is not part of provider security identity. Existing stores
/// always load their recorded index/type rather than applying this preference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderIndexMode {
    Legacy,
    Federated,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderDescription {
    pub identity: [u8; 32],
    pub query_capable: bool,
    pub index_mode: ProviderIndexMode,
}
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
    /// 1 keeps legacy replay history; 2 binds query identities to this handle.
    pub query_identity_version: u32,
    /// Exact local retained-source echo. Absent for ordinary scopes.
    pub cloud_source_digest: Option<[u8; 32]>,
}
impl QueryScope {
    pub fn bound_query_id(&self, nonce: [u8; 16]) -> Result<[u8; 32], Status> {
        if self.query_identity_version != 2 {
            return Err(Status::failed_precondition(
                "native scope does not support bound query identities",
            ));
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-broker-query-scope-id-v2\0");
        hash.update(&self.handle);
        hash.update(&nonce);
        let mut id = [0; 32];
        id[..16].copy_from_slice(&nonce);
        id[16..].copy_from_slice(&hash.finalize().as_bytes()[..16]);
        Ok(id)
    }
    pub fn validate_query_id(&self, id: &[u8; 32]) -> Result<(), Status> {
        if normalize_query_identity_version(self.query_identity_version)? == 2 {
            let expected = self.bound_query_id(id[..16].try_into().expect("fixed nonce width"))?;
            if expected != *id {
                return Err(Status::invalid_argument(
                    "native query identity belongs to another scope",
                ));
            }
        }
        Ok(())
    }
}
/// Zero is the legacy wire default. Explicit version 2 never silently falls back.
pub fn normalize_query_identity_version(version: u32) -> Result<u32, Status> {
    match version {
        0 | 1 => Ok(1),
        2 => Ok(2),
        _ => Err(Status::unimplemented(
            "native query identity version unsupported",
        )),
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryHandle {
    pub handle: [u8; 32],
    pub expires_at_unix_seconds: i64,
}
/// Immutable selection pins, not an upstream authorization credential. Index
/// roots and expected anchors must be nonempty, strictly sorted and unique.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudQuerySource {
    pub provider_identity: [u8; 32],
    pub workspace_id: [u8; 32],
    pub generation: u64,
    pub manifest_id: [u8; 32],
    pub manifest_blake3: [u8; 32],
    pub manifest_size_bytes: u64,
    pub index_root_ids: Vec<[u8; 32]>,
    pub expected_anchors: Vec<CommitId>,
}
impl CloudQuerySource {
    /// Public selection provenance, never an authorization or completeness proof.
    pub fn digest(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-broker-cloud-query-source-v1\0");
        hash.update(&self.provider_identity);
        hash.update(&self.workspace_id);
        hash.update(&self.generation.to_be_bytes());
        hash.update(&self.manifest_id);
        hash.update(&self.manifest_blake3);
        hash.update(&self.manifest_size_bytes.to_be_bytes());
        hash.update(&(self.index_root_ids.len() as u64).to_be_bytes());
        for id in &self.index_root_ids {
            hash.update(id);
        }
        hash.update(&(self.expected_anchors.len() as u64).to_be_bytes());
        for id in &self.expected_anchors {
            hash.update(&id.0);
        }
        *hash.finalize().as_bytes()
    }
    pub(crate) fn validate(&self, cap: usize) -> Result<usize, Status> {
        if self.index_root_ids.len() > 4096 || self.expected_anchors.len() > 4096 {
            return Err(Status::resource_exhausted(
                "native cloud query source count exceeded",
            ));
        }
        if self.provider_identity == [0; 32]
            || self.generation == 0
            || self.manifest_size_bytes == 0
            || self.index_root_ids.is_empty()
            || self.expected_anchors.is_empty()
            || !self.index_root_ids.windows(2).all(|ids| ids[0] < ids[1])
            || !self.expected_anchors.windows(2).all(|ids| ids[0] < ids[1])
        {
            return Err(Status::invalid_argument(
                "invalid native cloud query source",
            ));
        }
        let bytes = self
            .index_root_ids
            .len()
            .saturating_add(self.expected_anchors.len())
            .saturating_mul(34)
            .saturating_add(256);
        crate::query_service::bounded_input(bytes, cap)?;
        Ok(bytes)
    }
}
#[derive(Clone, Debug)]
pub struct OpenCloudQueryScope {
    pub source: CloudQuerySource,
    pub overlay: Vec<Vec<u8>>,
    pub replay_key: [u8; 32],
}
#[derive(Clone, Debug)]
pub struct OpenQueryScope {
    pub roots: Vec<CommitId>,
    pub overlay: Vec<Vec<u8>>,
    pub replay_key: [u8; 32],
    pub query_identity_version: u32,
}
#[derive(Clone, Debug)]
pub struct ForkQueryScope {
    pub parent: [u8; 32],
    /// Must match the parent's immutable source, including on replay.
    pub expected_cloud_source_digest: Option<[u8; 32]>,
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
/// `begin` prepares a local session; only its first page establishes the upstream
/// evaluator. Closing its scope before that first page may reject evaluation.
/// Established unclosed queries retain their page replay until close/expiry.
/// Cloud scopes require their exact source digest and v2 identities. Forks must
/// validate the parent's expected source even on replay; no source replacement
/// or ordinary-root fallback is permitted.
#[async_trait]
pub trait NativeQueryIo: Send + Sync + std::fmt::Debug {
    fn resources(&self) -> QueryResources;
    /// Explicit retained snapshot support; unsupported providers must not
    /// reinterpret this request as ordinary root/reference admission.
    async fn open_cloud_scope(
        &self,
        _input: OpenCloudQueryScope,
        _owner: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, Status> {
        Err(Status::unimplemented(
            "native cloud query scopes unsupported",
        ))
    }
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
    /// Exact prefix within a validated graph selection; no global fallback.
    async fn resolve_commit_prefix_selected(
        &self,
        _scope: [u8; 32],
        _prefix: String,
        _selection: q::GraphExpr,
        _owner: QueryOwner,
    ) -> Result<QueryReply<q::CommitPrefixResult>, Status> {
        Err(Status::unimplemented("selected commit prefix unavailable"))
    }
    async fn resolve_change_prefix_selected(
        &self,
        _scope: [u8; 32],
        _prefix: String,
        _visible_heads: Vec<CommitId>,
        _selection: q::GraphExpr,
        _owner: QueryOwner,
    ) -> Result<QueryReply<q::ChangePrefixResult>, Status> {
        Err(Status::unimplemented("selected change prefix unavailable"))
    }
    async fn shortest_prefix_selected(
        &self,
        _scope: [u8; 32],
        _target: q::shortest_prefix_request::Target,
        _visible_heads: Vec<CommitId>,
        _selection: q::GraphExpr,
        _owner: QueryOwner,
    ) -> Result<QueryReply<q::ShortestPrefixResult>, Status> {
        Err(Status::unimplemented(
            "selected shortest prefix unavailable",
        ))
    }
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

/// Digest of a bounded structural prefix selection. This is not fetch authority.
pub fn validate_prefix_selection(
    expression: &q::GraphExpr,
    max_bytes: usize,
) -> Result<(), Status> {
    use prost::Message as _;
    crate::query_service::expression_shape(expression, max_bytes)?;
    if expression.encoded_len() > max_bytes {
        return Err(Status::resource_exhausted("selected prefix AST byte limit"));
    }
    Ok(())
}
pub fn prefix_selection_digest(
    expression: &q::GraphExpr,
    max_bytes: usize,
) -> Result<[u8; 32], Status> {
    use prost::Message as _;
    validate_prefix_selection(expression, max_bytes)?;
    let bytes = expression.encode_to_vec();
    Ok(tinytree_model::query_prefix_selection_digest(&bytes))
}
