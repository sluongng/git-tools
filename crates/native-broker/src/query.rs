//! Optional host-owned immutable graph queries. Local handles select captured
//! scopes; neither upstream credentials nor upstream cursors cross this facade.
use async_trait::async_trait;
use std::{any::Any, sync::Arc};
use tinytree_model::CommitId;
use tinytree_proto::query_v1 as q;
use tonic::Status;

/// Authenticated configured support is separate from grants and selected roots.
/// A legacy vocabulary proves neither support nor authorization for feature11.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlameCapabilities {
    LegacyUnnegotiated,
    Explicit { supported: bool, authorized: bool },
}
impl BlameCapabilities {
    pub(crate) fn wire(self) -> crate::proto::BlameCapabilitiesReply {
        self.wire_feature(11)
    }
    pub(crate) fn annotation_wire(self) -> crate::proto::BlameCapabilitiesReply {
        self.wire_feature(12)
    }
    fn wire_feature(self, feature: u32) -> crate::proto::BlameCapabilitiesReply {
        let (legacy_unnegotiated, supported, authorized) = match self {
            Self::LegacyUnnegotiated => (true, false, false),
            Self::Explicit {
                supported,
                authorized,
            } => (false, supported, authorized),
        };
        crate::proto::BlameCapabilitiesReply {
            schema_version: 1,
            feature,
            legacy_unnegotiated,
            supported,
            authorized,
            root_readiness: tinytree_proto::v1::RootReadiness::NotChecked as u32,
        }
    }
    pub(crate) fn checked(value: crate::proto::BlameCapabilitiesReply) -> Result<Self, Status> {
        Self::checked_feature(value, 11)
    }
    pub(crate) fn annotation_checked(
        value: crate::proto::BlameCapabilitiesReply,
    ) -> Result<Self, Status> {
        Self::checked_feature(value, 12)
    }
    fn checked_feature(
        value: crate::proto::BlameCapabilitiesReply,
        feature: u32,
    ) -> Result<Self, Status> {
        if value.schema_version != 1
            || value.feature != feature
            || value.root_readiness != tinytree_proto::v1::RootReadiness::NotChecked as u32
            || (value.legacy_unnegotiated && (value.supported || value.authorized))
        {
            return Err(Status::data_loss("invalid native blame capability reply"));
        }
        Ok(if value.legacy_unnegotiated {
            Self::LegacyUnnegotiated
        } else {
            Self::Explicit {
                supported: value.supported,
                authorized: value.authorized,
            }
        })
    }
}

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
    pub creation_policy: Option<CreationPolicy>,
}
/// Configured creation semantics. The digest excludes provider security identity
/// and resource tuning; it binds only the explicit/automatic policy choices.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreationPolicy {
    pub digest: [u8; 32],
    pub capture_enabled: bool,
    pub upload_enabled: bool,
    pub auto_capture: bool,
    pub auto_upload: bool,
}
impl CreationPolicy {
    pub fn wire(self) -> crate::proto::CreationPolicy {
        crate::proto::CreationPolicy {
            digest: self.digest.to_vec(),
            capture_enabled: Some(self.capture_enabled),
            upload_enabled: Some(self.upload_enabled),
            auto_capture: Some(self.auto_capture),
            auto_upload: Some(self.auto_upload),
        }
    }
    pub fn checked(value: crate::proto::CreationPolicy) -> Result<Self, Status> {
        let invalid = || Status::data_loss("invalid native creation policy");
        let policy = Self {
            digest: value.digest.try_into().map_err(|_| invalid())?,
            capture_enabled: value.capture_enabled.ok_or_else(invalid)?,
            upload_enabled: value.upload_enabled.ok_or_else(invalid)?,
            auto_capture: value.auto_capture.ok_or_else(invalid)?,
            auto_upload: value.auto_upload.ok_or_else(invalid)?,
        };
        if policy.digest == [0; 32]
            || (policy.upload_enabled && !policy.capture_enabled)
            || (policy.auto_capture && !policy.capture_enabled)
            || (policy.auto_upload && !policy.upload_enabled)
        {
            return Err(invalid());
        }
        Ok(policy)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreationPlan {
    pub provider_identity: [u8; 32],
    pub root: [u8; 32],
    pub index_mode: ProviderIndexMode,
    pub policy: CreationPolicy,
}
impl CreationPlan {
    pub fn wire(self) -> crate::proto::CreationPlan {
        crate::proto::CreationPlan {
            version: 1,
            provider_identity: self.provider_identity.to_vec(),
            root: self.root.to_vec(),
            index_mode: match self.index_mode {
                ProviderIndexMode::Legacy => crate::proto::ProviderIndexMode::Legacy,
                ProviderIndexMode::Federated => crate::proto::ProviderIndexMode::Federated,
            } as i32,
            policy: Some(self.policy.wire()),
        }
    }
    pub fn checked(value: crate::proto::CreationPlan) -> Result<Self, Status> {
        let invalid = || Status::data_loss("invalid native creation plan");
        if value.version != 1 {
            return Err(invalid());
        }
        let plan = Self {
            provider_identity: value.provider_identity.try_into().map_err(|_| invalid())?,
            root: value.root.try_into().map_err(|_| invalid())?,
            index_mode: match crate::proto::ProviderIndexMode::try_from(value.index_mode) {
                Ok(crate::proto::ProviderIndexMode::Legacy) => ProviderIndexMode::Legacy,
                Ok(crate::proto::ProviderIndexMode::Federated) => ProviderIndexMode::Federated,
                _ => return Err(invalid()),
            },
            policy: CreationPolicy::checked(value.policy.ok_or_else(invalid)?)?,
        };
        if plan.provider_identity == [0; 32] {
            return Err(invalid());
        }
        Ok(plan)
    }
    pub fn encode(self) -> Vec<u8> {
        use prost::Message;
        self.wire().encode_to_vec()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Status> {
        use prost::Message;
        if bytes.len() > 1024 {
            return Err(Status::resource_exhausted(
                "native creation plan exceeds bound",
            ));
        }
        let value = crate::proto::CreationPlan::decode(bytes)
            .map_err(|_| Status::data_loss("invalid native creation plan"))?;
        let plan = Self::checked(value)?;
        if plan.encode() != bytes {
            return Err(Status::data_loss("noncanonical native creation plan"));
        }
        Ok(plan)
    }
}
impl ProviderDescription {
    pub fn creation_plan(self, root: [u8; 32]) -> Result<CreationPlan, Status> {
        Ok(CreationPlan {
            provider_identity: self.identity,
            root,
            index_mode: self.index_mode,
            policy: self.creation_policy.ok_or_else(|| {
                Status::failed_precondition("native creation policy is unavailable")
            })?,
        })
    }
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
/// Common immutable selection for one authorization pass. This is provenance,
/// not authority: each selected root still requires its own server scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudAuthorizationSource {
    pub provider_identity: [u8; 32],
    pub workspace_id: [u8; 32],
    pub generation: u64,
    pub manifest_id: [u8; 32],
    pub manifest_blake3: [u8; 32],
    pub manifest_size_bytes: u64,
}
impl CloudQuerySource {
    pub fn authorization_source(&self) -> CloudAuthorizationSource {
        CloudAuthorizationSource {
            provider_identity: self.provider_identity,
            workspace_id: self.workspace_id,
            generation: self.generation,
            manifest_id: self.manifest_id,
            manifest_blake3: self.manifest_blake3,
            manifest_size_bytes: self.manifest_size_bytes,
        }
    }
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
/// An exact path at an immutable revision in one captured local scope. This is
/// not an upstream object-read capability or a moving reference selection.
#[derive(Clone, Debug)]
pub struct ScopedPathQuery {
    pub scope: [u8; 32],
    pub revision: CommitId,
    pub path: String,
    pub policy: q::HistoryPolicy,
    pub max_commits: u32,
}
impl ScopedPathQuery {
    pub(crate) fn validate(&self, cap: usize) -> Result<(), Status> {
        crate::query_service::bounded_input(self.path.len().saturating_add(128), cap)?;
        if !matches!(
            self.policy,
            q::HistoryPolicy::LinearFixedPathV1 | q::HistoryPolicy::JjResolvedFixedPathV1
        ) {
            return Err(Status::unimplemented("native history policy unsupported"));
        }
        if self.path.is_empty()
            || self.path.contains(['\\', '\0'])
            || self
                .path
                .split('/')
                .any(|part| matches!(part, "" | "." | ".."))
        {
            return Err(Status::invalid_argument("invalid native history path"));
        }
        Ok(())
    }
    fn check_echo(
        &self,
        revision: Option<&q::CommitId>,
        path: &str,
        policy: i32,
        digest: &[u8],
        epoch: &[u8],
        coverage: i32,
        boundary: Option<&q::CommitId>,
    ) -> Result<(), Status> {
        if revision.is_none_or(|id| id.value.as_slice() != self.revision.0)
            || path != self.path
            || policy != self.policy as i32
            || digest.len() != 32
            || epoch.len() != 16
            || !(1..=7).contains(&coverage)
            || (coverage == 1 && boundary.is_some())
            || (coverage != 1 && boundary.is_none())
            || boundary.is_some_and(|id| id.value.len() != 32)
        {
            return Err(Status::data_loss(
                "native history response binding mismatch",
            ));
        }
        Ok(())
    }
    pub(crate) fn check_history(&self, value: &q::PathHistoryResult) -> Result<(), Status> {
        self.check_echo(
            value.revision.as_ref(),
            &value.path,
            value.policy,
            &value.scope_digest,
            &value.index_epoch,
            value.coverage,
            value.boundary.as_ref(),
        )?;
        if self.max_commits != 0 && value.scanned_commits > self.max_commits {
            return Err(Status::data_loss("native history scan bound mismatch"));
        }
        if value.entries.len() > value.scanned_commits as usize {
            return Err(Status::data_loss("native history result count mismatch"));
        }
        let mut previous = std::collections::HashSet::new();
        for entry in &value.entries {
            let id = entry.commit.as_ref().ok_or_else(history_integrity)?;
            if id.value.len() != 32 || !previous.insert(id.value.as_slice()) {
                return Err(history_integrity());
            }
            path_state(entry.state.as_ref().ok_or_else(history_integrity)?)?;
            path_state(entry.parent_state.as_ref().ok_or_else(history_integrity)?)?;
            if entry.state == entry.parent_state {
                return Err(history_integrity());
            }
        }
        Ok(())
    }
    pub(crate) fn check_blame(
        &self,
        value: &q::BlameResult,
        limits: tinytree_model::Limits,
    ) -> Result<(), Status> {
        self.check_echo(
            value.revision.as_ref(),
            &value.path,
            value.policy,
            &value.scope_digest,
            &value.index_epoch,
            value.coverage,
            value.boundary.as_ref(),
        )?;
        if self.max_commits != 0 && value.scanned_commits > self.max_commits {
            return Err(history_integrity());
        }
        let Some(target) = value.target.as_ref() else {
            if value.coverage == 1 || !value.target_bytes.is_empty() || !value.ranges.is_empty() {
                return Err(history_integrity());
            }
            return Ok(());
        };
        let key = target.key.as_ref().ok_or_else(history_integrity)?;
        let id: [u8; 32] = key
            .native_id
            .as_slice()
            .try_into()
            .map_err(|_| history_integrity())?;
        if key.kind != tinytree_model::Kind::File as i32
            || target.content_blake3.as_slice() != id
            || target.object_format_version != tinytree_model::FORMAT_VERSION as u32
        {
            return Err(history_integrity());
        }
        let descriptor = tinytree_model::Descriptor {
            key: tinytree_model::ObjectKey {
                kind: tinytree_model::Kind::File,
                id: tinytree_model::ObjectId(id),
            },
            size_bytes: target.size_bytes,
            format_version: tinytree_model::FORMAT_VERSION,
        };
        tinytree_model::verify(&descriptor, &value.target_bytes, limits)
            .map_err(|_| history_integrity())?;
        let lines = value
            .target_bytes
            .iter()
            .filter(|&&byte| byte == b'\n')
            .count()
            + usize::from(value.target_bytes.last().is_some_and(|&byte| byte != b'\n'));
        let mut next = 0usize;
        for range in &value.ranges {
            if range.start_line as usize != next
                || range.end_line <= range.start_line
                || range.end_line as usize > lines
                || range.origin.as_ref().is_some_and(|id| id.value.len() != 32)
                || (value.coverage == 1 && range.origin.is_none())
                || range
                    .original_line
                    .checked_add(range.end_line - range.start_line)
                    .is_none()
            {
                return Err(history_integrity());
            }
            next = range.end_line as usize;
        }
        if next != lines {
            return Err(history_integrity());
        }
        Ok(())
    }
}
fn history_integrity() -> Status {
    Status::data_loss("invalid native history response")
}
fn path_state(value: &q::HistoryPathState) -> Result<(), Status> {
    match value.kind {
        1 if value.native_id.is_empty() && value.size_bytes == 0 && !value.executable => Ok(()),
        2 if value.native_id.len() == 32 => Ok(()),
        3 if value.native_id.len() == 32 && !value.executable => Ok(()),
        4 if value.native_id.len() == 32 && value.size_bytes == 0 && !value.executable => Ok(()),
        _ => Err(history_integrity()),
    }
}
/// One exact path and canonical anchor union in a command-owned local scope.
#[derive(Clone, Debug)]
pub struct OpenAnnotationFrontier {
    pub scope: [u8; 32],
    pub anchors: Vec<CommitId>,
    pub path: String,
    pub max_commits: u32,
    pub limits: Option<q::AnnotationLimits>,
}
pub fn annotation_limits_valid(value: &q::AnnotationLimits) -> bool {
    [
        value.work_nodes,
        value.work_edges,
        value.work_bytes,
        value.frontier,
        value.read_bytes,
        value.diff_input_bytes,
        value.diff_lines,
        value.diff_work,
        value.diff_allocation_bytes,
        value.diff_depth,
    ]
    .into_iter()
    .all(|n| n > 0 && n <= usize::MAX as u64)
}
fn annotation_limits_lowered(
    effective: &q::AnnotationLimits,
    requested: &q::AnnotationLimits,
) -> bool {
    annotation_limits_valid(effective)
        && annotation_limits_valid(requested)
        && [
            effective.work_nodes,
            effective.work_edges,
            effective.work_bytes,
            effective.frontier,
            effective.read_bytes,
            effective.diff_input_bytes,
            effective.diff_lines,
            effective.diff_work,
            effective.diff_allocation_bytes,
            effective.diff_depth,
        ]
        .into_iter()
        .zip([
            requested.work_nodes,
            requested.work_edges,
            requested.work_bytes,
            requested.frontier,
            requested.read_bytes,
            requested.diff_input_bytes,
            requested.diff_lines,
            requested.diff_work,
            requested.diff_allocation_bytes,
            requested.diff_depth,
        ])
        .all(|(a, b)| a <= b)
}
fn annotation_value(value: Option<&q::AnnotationPathValue>) -> Result<(), Status> {
    let value = value.ok_or_else(history_integrity)?;
    let state = value.state.as_ref().ok_or_else(history_integrity)?;
    path_state(state)?;
    if !(value.copy_id.is_empty() || value.copy_id.len() == 32)
        || (!value.copy_id.is_empty() && state.kind != 2)
    {
        return Err(history_integrity());
    }
    Ok(())
}
impl OpenAnnotationFrontier {
    pub(crate) fn validate(&self, cap: usize) -> Result<(), Status> {
        if self
            .limits
            .as_ref()
            .is_some_and(|limits| !annotation_limits_valid(limits))
        {
            return Err(Status::invalid_argument(
                "positive annotation limits required",
            ));
        }
        if self.anchors.is_empty()
            || self.anchors.len() > 4096
            || self.anchors.windows(2).any(|v| v[0] >= v[1])
        {
            return Err(Status::invalid_argument(
                "annotation anchors must be bounded sorted unique IDs",
            ));
        }
        crate::query_service::bounded_input(
            self.path
                .len()
                .saturating_add(self.anchors.len().saturating_mul(40))
                .saturating_add(128),
            cap,
        )?;
        ScopedPathQuery {
            scope: self.scope,
            revision: CommitId::ROOT,
            path: self.path.clone(),
            policy: q::HistoryPolicy::LinearFixedPathV1,
            max_commits: self.max_commits,
        }
        .validate(cap)
    }
    pub(crate) fn check(&self, value: &q::AnnotationFrontier, cap: usize) -> Result<(), Status> {
        match (&self.limits, &value.limits) {
            (None, None) => {}
            (Some(requested), Some(effective))
                if annotation_limits_lowered(effective, requested) => {}
            _ => return Err(history_integrity()),
        }
        if value.context != self.scope
            || value.scope_digest.len() != 32
            || value.index_epoch.len() != 16
            || !matches!(
                (self.limits.is_some(), value.version),
                (true, 2 | 3) | (false, 1)
            )
            || value.path != self.path
            || value.expires_at_unix_seconds <= 0
            || value
                .anchors
                .iter()
                .map(|v| v.value.as_slice())
                .ne(self.anchors.iter().map(|v| v.0.as_slice()))
            || !(1..=7).contains(&value.coverage)
            || (self.max_commits != 0 && value.scanned_commits > self.max_commits)
            || (value.coverage != 1 && !value.nodes.is_empty())
            || (value.coverage == 1 && value.nodes.len() != value.scanned_commits as usize + 1)
        {
            return Err(history_integrity());
        }
        let mut positions = std::collections::BTreeMap::new();
        let mut previous = None;
        let mut edges = 0usize;
        for node in &value.nodes {
            let id: [u8; 32] = node
                .revision
                .as_ref()
                .ok_or_else(history_integrity)?
                .value
                .as_slice()
                .try_into()
                .map_err(|_| history_integrity())?;
            let key = (node.source_layer, node.topological_key);
            if node.source_layer > 1
                || previous.is_some_and(|old| old >= key)
                || positions.insert(id, key).is_some()
                || node.affects_path.is_none()
            {
                return Err(history_integrity());
            }
            annotation_value(node.value.as_ref())?;
            previous = Some(key);
            edges = edges.saturating_add(node.parents.len());
            if edges.saturating_mul(32) > cap {
                return Err(Status::resource_exhausted(
                    "annotation frontier edge memory exceeded",
                ));
            }
            if id == [0; 32] {
                if !node.parents.is_empty()
                    || node.affects_path != Some(false)
                    || node.value.as_ref().unwrap().state.as_ref().unwrap().kind != 1
                {
                    return Err(history_integrity());
                }
            } else if node.parents.is_empty() {
                return Err(history_integrity());
            }
            for parent in &node.parents {
                let parent: [u8; 32] = parent
                    .value
                    .as_slice()
                    .try_into()
                    .map_err(|_| history_integrity())?;
                if positions.get(&parent).is_none_or(|p| *p >= key) {
                    return Err(history_integrity());
                }
            }
        }
        if value.coverage == 1
            && (!positions.contains_key(&[0; 32])
                || self.anchors.iter().any(|v| !positions.contains_key(&v.0)))
        {
            return Err(history_integrity());
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct ReadAnnotationInput {
    pub scope: [u8; 32],
    pub context: [u8; 32],
    pub revision: CommitId,
    pub expected: q::AnnotationPathValue,
}
impl ReadAnnotationInput {
    pub(crate) fn validate(&self, cap: usize) -> Result<(), Status> {
        crate::query_service::bounded_input(512, cap)?;
        if self.context != self.scope {
            return Err(Status::invalid_argument(
                "annotation context belongs to another local scope",
            ));
        }
        annotation_value(Some(&self.expected))?;
        if self.expected.state.as_ref().unwrap().kind != 2 {
            return Err(Status::invalid_argument(
                "annotation input must be a regular FILE",
            ));
        }
        Ok(())
    }
    pub(crate) fn check(
        &self,
        value: &q::AnnotationInput,
        limits: tinytree_model::Limits,
    ) -> Result<(), Status> {
        if value.context != self.context
            || value
                .revision
                .as_ref()
                .is_none_or(|id| id.value != self.revision.0)
            || value.value.as_ref() != Some(&self.expected)
            || value.data.len() > limits.max_object_bytes
        {
            return Err(history_integrity());
        }
        let described = tinytree_model::describe_file(&value.data);
        let descriptor = value.descriptor.as_ref().ok_or_else(history_integrity)?;
        let state = self.expected.state.as_ref().unwrap();
        if descriptor
            .key
            .as_ref()
            .is_none_or(|key| key.kind != 1 || key.native_id != described.key.id.0)
            || descriptor.content_blake3 != described.key.id.0
            || descriptor.size_bytes != described.size_bytes
            || descriptor.object_format_version != described.format_version as u32
            || state.native_id != described.key.id.0
            || state.size_bytes != described.size_bytes
        {
            return Err(history_integrity());
        }
        Ok(())
    }
}
/// Indexed content search against one captured immutable source. The cursor is
/// opaque and local to the configured provider; it is not an object capability.
#[derive(Clone, Debug)]
pub struct ScopedSearchQuery {
    pub scope: [u8; 32],
    pub revision: CommitId,
    pub pattern: Vec<u8>,
    pub path_prefix: String,
    pub policy: q::SearchPolicy,
    pub page_size: u32,
    pub cursor: Vec<u8>,
}
impl ScopedSearchQuery {
    pub(crate) fn validate(&self, cap: usize) -> Result<(), Status> {
        crate::query_service::bounded_input(
            self.pattern
                .len()
                .saturating_add(self.path_prefix.len())
                .saturating_add(self.cursor.len())
                .saturating_add(256),
            cap,
        )?;
        let mode = match self.policy {
            q::SearchPolicy::LiteralBytesV1 => tinytree_search::SearchMode::Literal,
            q::SearchPolicy::RegexBytesV1 => tinytree_search::SearchMode::Regex,
            _ => return Err(Status::unimplemented("native search policy unsupported")),
        };
        tinytree_search::validate_input(&self.pattern, mode)
            .map_err(|_| Status::invalid_argument("invalid native search pattern"))?;
        if self.page_size > 4096
            || self.cursor.len() > 8192
            || !search_path(&self.path_prefix, true)
        {
            return Err(Status::invalid_argument("invalid native search input"));
        }
        Ok(())
    }
    pub(crate) fn check_result(&self, value: &q::SearchResult) -> Result<(), Status> {
        use q::SearchCoverage as C;
        let coverage = C::try_from(value.coverage).map_err(|_| search_integrity())?;
        let ready = matches!(coverage, C::SearchComplete | C::SearchMore);
        if value
            .revision
            .as_ref()
            .is_none_or(|id| id.value.as_slice() != self.revision.0)
            || value.pattern != self.pattern
            || value.path_prefix != self.path_prefix
            || value.policy != self.policy as i32
            || value.scope_digest.len() != 32
            || value.index_epoch.len() != 16
            || coverage == C::Unspecified
            || !matches!(value.root_tree.len(), 0 | 32)
            || !matches!(value.manifest_digest.len(), 0 | 32)
            || ((ready || !value.hits.is_empty())
                && (value.root_tree.len() != 32 || value.manifest_digest.len() != 32))
            || ((coverage == C::SearchMore) != !value.cursor.is_empty())
            || value.cursor.len() > 8192
            || value.page_size == 0
            || value.page_size > 4096
            || (self.page_size != 0 && value.page_size != self.page_size)
            || value.hits.len() > value.page_size as usize
            || (matches!(
                coverage,
                C::SearchWarming | C::SearchPrivateOverlay | C::SearchConflict
            ) && !value.hits.is_empty())
        {
            return Err(search_integrity());
        }
        let mut previous: Option<&q::SearchHit> = None;
        for hit in &value.hits {
            let end = hit
                .byte_offset
                .checked_add(hit.match_bytes as u64)
                .ok_or_else(search_integrity)?;
            let snippet_end = hit
                .snippet_offset
                .checked_add(hit.snippet.len() as u64)
                .ok_or_else(search_integrity)?;
            if !search_path(&hit.path, false)
                || !search_prefix(&hit.path, &self.path_prefix)
                || hit.file_id.len() != 32
                || hit.match_bytes == 0
                || hit.match_bytes as usize > tinytree_search::MAX_PATTERN_BYTES
                || (self.policy == q::SearchPolicy::LiteralBytesV1
                    && hit.match_bytes as usize != self.pattern.len())
                || end > hit.file_size
                || hit.line == 0
                || hit.line > hit.byte_offset.saturating_add(1)
                || hit.byte_column > hit.byte_offset
                || hit.snippet_offset > hit.byte_offset
                || snippet_end < end
                || snippet_end > hit.file_size
                || hit.byte_offset - hit.snippet_offset > 128
                || snippet_end - end > 128
                || hit.snippet.len() > tinytree_search::MAX_SNIPPET_BYTES
                || hit.snippet.len() > hit.match_bytes as usize + 256
            {
                return Err(search_integrity());
            }
            let relative = usize::try_from(hit.byte_offset - hit.snippet_offset)
                .map_err(|_| search_integrity())?;
            let matched = hit
                .snippet
                .get(relative..relative + hit.match_bytes as usize)
                .ok_or_else(search_integrity)?;
            // Partial snippets do not supply the complete context required by
            // regex anchors and boundaries. They remain provider assertions.
            if (self.policy == q::SearchPolicy::LiteralBytesV1
                && matched != self.pattern.as_slice())
                || matched.contains(&b'\n')
            {
                return Err(search_integrity());
            }
            if let Some(prior) = previous {
                if hit.path.as_bytes() < prior.path.as_bytes()
                    || (hit.path == prior.path
                        && (hit.byte_offset < prior.byte_offset + prior.match_bytes as u64
                            || hit.file_id != prior.file_id
                            || hit.file_size != prior.file_size))
                {
                    return Err(search_integrity());
                }
            }
            previous = Some(hit);
        }
        Ok(())
    }
}
fn search_path(path: &str, empty: bool) -> bool {
    (empty && path.is_empty())
        || (!path.is_empty()
            && path.len() <= 4096
            && !path.contains(['\\', '\0'])
            && path.split('/').all(|part| !matches!(part, "" | "." | "..")))
}
fn search_prefix(path: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}
fn search_integrity() -> Status {
    Status::data_loss("invalid native search response")
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
    async fn annotation_capabilities(
        &self,
        _owner: QueryOwner,
    ) -> Result<QueryReply<BlameCapabilities>, Status> {
        Err(Status::unimplemented(
            "annotation frontier negotiation unavailable",
        ))
    }
    async fn blame_with_limits(
        &self,
        _input: ScopedPathQuery,
        _limits: q::AnnotationLimits,
        _owner: QueryOwner,
    ) -> Result<QueryReply<q::BlameResult>, Status> {
        Err(Status::unimplemented(
            "bounded annotation blame is unavailable",
        ))
    }
    async fn open_annotation_frontier(
        &self,
        _input: OpenAnnotationFrontier,
        _owner: QueryOwner,
    ) -> Result<QueryReply<q::AnnotationFrontier>, Status> {
        Err(Status::unimplemented("annotation frontier unavailable"))
    }
    /// Explicit opt-in: old providers must refuse BEFORE admitting unbounded
    /// frontier work, rather than erase lowering fields and fail on the echo.
    async fn open_limited_annotation_frontier(
        &self,
        _input: OpenAnnotationFrontier,
        _owner: QueryOwner,
    ) -> Result<QueryReply<q::AnnotationFrontier>, Status> {
        Err(Status::unimplemented(
            "limited annotation frontier unavailable",
        ))
    }
    async fn read_annotation_input(
        &self,
        _input: ReadAnnotationInput,
        _owner: QueryOwner,
    ) -> Result<QueryReply<q::AnnotationInput>, Status> {
        Err(Status::unimplemented("annotation input unavailable"))
    }
    async fn close_annotation_frontier(
        &self,
        _scope: [u8; 32],
        _owner: QueryOwner,
    ) -> Result<QueryReply<()>, Status> {
        Err(Status::unimplemented(
            "annotation context cleanup unavailable",
        ))
    }

    fn resources(&self) -> QueryResources;
    async fn blame_capabilities(
        &self,
        _owner: QueryOwner,
    ) -> Result<QueryReply<BlameCapabilities>, Status> {
        Err(Status::unimplemented(
            "native blame capability negotiation unavailable",
        ))
    }
    /// Read-only upstream checks, without retaining a broker scope or granting
    /// an anchor. The configured host chooses requirements, never the caller.
    async fn preflight_creation(
        &self,
        _root: CommitId,
        _query: bool,
        _cloud: bool,
        // Selected only from the host's saved/expected creation policy, never
        // supplied as an independent caller-controlled wire flag.
        _thin_capture: bool,
        _deadline: tokio::time::Instant,
        _owner: QueryOwner,
    ) -> Result<QueryReply<()>, Status> {
        Err(Status::unimplemented(
            "native creation preflight unsupported",
        ))
    }

    /// Optionally prepare one fresh, bounded retained proof for this pass.
    /// None retains the original per-root authorization path. An error must
    /// propagate, never select that compatibility path. No wire RPC is added.
    async fn prepare_cloud_authorization(
        &self,
        _source: CloudAuthorizationSource,
        _owner: QueryOwner,
    ) -> Result<Option<QueryReply<Arc<dyn NativeQueryIo>>>, Status> {
        Ok(None)
    }

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
    async fn path_history(
        &self,
        _input: ScopedPathQuery,
        _owner: QueryOwner,
    ) -> Result<QueryReply<q::PathHistoryResult>, Status> {
        Err(Status::unimplemented("native path history unsupported"))
    }
    async fn blame(
        &self,
        _input: ScopedPathQuery,
        _owner: QueryOwner,
    ) -> Result<QueryReply<q::BlameResult>, Status> {
        Err(Status::unimplemented("native blame unsupported"))
    }
    async fn search(
        &self,
        _input: ScopedSearchQuery,
        _owner: QueryOwner,
    ) -> Result<QueryReply<q::SearchResult>, Status> {
        Err(Status::unimplemented("native indexed search unsupported"))
    }
    async fn close_query(&self, query: [u8; 32], owner: QueryOwner) -> Result<(), Status>;
    async fn close_scope(&self, scope: [u8; 32], owner: QueryOwner) -> Result<(), Status>;
}

/// Static diagnostic categories only. These do not authorize replay, fetching or
/// recovery, and never retain arbitrary provider messages or connection metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryFailureReason {
    LocalScopeUnavailable,
    LocalScopeClosedOrExpired,
    LocalScopeCountExceeded,
    LocalReplayCountExceeded,
    CachedCreationOutcomeUnavailable,
    CloudReplaySourceChanged,
    ClientScopeExpired,
    ServerScopeUnavailable,
    ServerScopeExpired,
    ServerScopeClosed,
    ServerReplayExpired,
    ServerReplayClosed,
    ServerCursorExpired,
    ServerCursorUnavailable,
    ServerQueryClosed,
    CloudSourceReplayUnavailable,
    ServerActiveScopeQuota,
    ServerScopeReplayHistoryQuota,
    ServerActiveQueryQuota,
    ServerQueryReplayHistoryQuota,
    LocalRegistryBytesExceeded,
    LocalRegistryBytesExhausted,
    ServerExactGraphWorkBudget,
    ServerDerivedIndexModelLimit,
    ServerScopeMemory,
    ServerScopeHandleMemory,
    ServerCloudScopeMemory,
    ServerFrozenGraphMemory,
    ServerOverlayMemory,
    ServerOverlayMemoryOverflow,
    ServerLogicalOverlayLimit,
    ServerQueryHistoryMemory,
    ServerQueryCursorMemory,
    ServerMembershipMemory,
    ServerSelectedPrefixMemory,
    ClientOverlayBytes,
    ClientOverlayMemory,
    ClientScopeSourceMemory,
    ClientAnnotationStateHeadroom,
    ClientAnnotationByteHeadroom,
    ClientAnnotationHeaderReservation,
    ClientAnnotationActualReservation,
    ClientCloudResourceLimit,
    ServerAnnotationCumulativeBudget,
    ServerAnnotationMemoryBusy,
    ServerAnnotationMemoryRange,
    ServerAnnotationBusy,
    ServerAnnotationResponseLimit,
    ServerAnnotationSequenceLimit,
    ServerAnnotationOutputOverflow,
    ClientQueryDecodeAllocation,
    ClientQueryRpcMemory,
    BrokerRetainedReplyMemory,
    BrokerReplyBytes,
    ServerRequestMemoryBudget,
    ServerSelectedPrefixMemoryBudget,
    ServerQueryCursorMemoryBudget,
    ServerMembershipMemoryBudget,
    ServerMembershipStepBudget,
    ServerClosedQueryMetadataMemory,
    ServerQueryHistoryMemoryBudget,
    ServerOverlayLayerOverflow,
}
impl QueryFailureReason {
    /// Classify exact source literals before a provider sanitizes its error.
    pub fn from_source_message(message: &str) -> Option<Self> {
        match message {
            "native query scope unavailable" => Some(Self::LocalScopeUnavailable),
            "native query scope closed or expired" => Some(Self::LocalScopeClosedOrExpired),
            "native query scope count exceeded" => Some(Self::LocalScopeCountExceeded),
            "native query replay count exceeded" => Some(Self::LocalReplayCountExceeded),
            "native query creation outcome unavailable; preserve the original intent" => {
                Some(Self::CachedCreationOutcomeUnavailable)
            }
            "cloud scope replay source changed" => Some(Self::CloudReplaySourceChanged),
            "native query scope expired" => Some(Self::ClientScopeExpired),
            "query scope unavailable or expired" => Some(Self::ServerScopeUnavailable),
            "query scope expired" => Some(Self::ServerScopeExpired),
            "query scope is closed" => Some(Self::ServerScopeClosed),
            "scope replay expired" => Some(Self::ServerReplayExpired),
            "scope replay is closed" => Some(Self::ServerReplayClosed),
            "query cursor expired" => Some(Self::ServerCursorExpired),
            "query cursor unavailable" => Some(Self::ServerCursorUnavailable),
            "query is closed" => Some(Self::ServerQueryClosed),
            "cloud source replay no longer available" => Some(Self::CloudSourceReplayUnavailable),
            "active query scope quota exceeded" => Some(Self::ServerActiveScopeQuota),
            "query scope replay history quota exceeded" => {
                Some(Self::ServerScopeReplayHistoryQuota)
            }
            "active query quota exceeded" => Some(Self::ServerActiveQueryQuota),
            "query replay history quota exceeded" => Some(Self::ServerQueryReplayHistoryQuota),
            "native query registry bytes exceeded" => Some(Self::LocalRegistryBytesExceeded),
            "native query registry bytes exhausted" => Some(Self::LocalRegistryBytesExhausted),
            "exact graph work budget exhausted" => Some(Self::ServerExactGraphWorkBudget),
            "derived index model limit exceeded" => Some(Self::ServerDerivedIndexModelLimit),
            "scope memory exceeded" => Some(Self::ServerScopeMemory),
            "scope handle memory exceeded" => Some(Self::ServerScopeHandleMemory),
            "cloud scope retained memory exceeded" => Some(Self::ServerCloudScopeMemory),
            "frozen graph memory exceeded" => Some(Self::ServerFrozenGraphMemory),
            "overlay memory exceeded" => Some(Self::ServerOverlayMemory),
            "overlay memory overflow" => Some(Self::ServerOverlayMemoryOverflow),
            "logical overlay limits exceeded" => Some(Self::ServerLogicalOverlayLimit),
            "query history memory exhausted" => Some(Self::ServerQueryHistoryMemory),
            "query cursor memory exhausted" => Some(Self::ServerQueryCursorMemory),
            "query membership memory exhausted" => Some(Self::ServerMembershipMemory),
            "selected prefix memory unavailable" => Some(Self::ServerSelectedPrefixMemory),
            "query overlay bytes exceed budget" => Some(Self::ClientOverlayBytes),
            "query overlay memory exceeds budget" => Some(Self::ClientOverlayMemory),
            "query scope source memory exceeds budget" => Some(Self::ClientScopeSourceMemory),
            "annotation aggregate exhausts query headroom" => {
                Some(Self::ClientAnnotationStateHeadroom)
            }
            "annotation aggregate exhausts byte headroom" => {
                Some(Self::ClientAnnotationByteHeadroom)
            }
            "annotation aggregate header exceeds reservation" => {
                Some(Self::ClientAnnotationHeaderReservation)
            }
            "annotation aggregate allocation exceeded reservation"
            | "annotation aggregate result exceeds reservation"
            | "annotation attribution allocation exceeded reservation"
            | "annotation retained result exceeds reservation"
            | "annotation cursor allocation exceeds aggregate reservation"
            | "annotation page count exceeds client reservation"
            | "annotation aggregate edge limit exceeded" => {
                Some(Self::ClientAnnotationActualReservation)
            }
            "workspace-cloud resource limit exceeded" => Some(Self::ClientCloudResourceLimit),
            "annotation cumulative history budget exhausted" => {
                Some(Self::ServerAnnotationCumulativeBudget)
            }
            "annotation context memory is busy" => Some(Self::ServerAnnotationMemoryBusy),
            "annotation context memory exceeds permit range" => {
                Some(Self::ServerAnnotationMemoryRange)
            }
            "annotation context is busy" => Some(Self::ServerAnnotationBusy),
            "annotation page exceeds response limit" => Some(Self::ServerAnnotationResponseLimit),
            "annotation sequence exhausted" => Some(Self::ServerAnnotationSequenceLimit),
            "annotation output overflow" => Some(Self::ServerAnnotationOutputOverflow),
            "native protobuf response exceeds collection allocation budget" => {
                Some(Self::ClientQueryDecodeAllocation)
            }
            "native client memory budget exceeded" => Some(Self::ClientQueryRpcMemory),
            "native query retained reply budget exhausted" => Some(Self::BrokerRetainedReplyMemory),
            "query client reply exceeds limit" => Some(Self::BrokerReplyBytes),
            "overlay layer overflow" => Some(Self::ServerOverlayLayerOverflow),
            "query history memory exceeds budget" => Some(Self::ServerQueryHistoryMemoryBudget),
            "closed query metadata exceeds budget" => Some(Self::ServerClosedQueryMetadataMemory),
            "query membership step budget exhausted" => Some(Self::ServerMembershipStepBudget),
            "query membership memory budget exceeded" => Some(Self::ServerMembershipMemoryBudget),
            "query cursor memory exceeds budget" => Some(Self::ServerQueryCursorMemoryBudget),
            "selected prefix memory exceeds budget" => Some(Self::ServerSelectedPrefixMemoryBudget),
            "requested query memory exceeds configured pool" => {
                Some(Self::ServerRequestMemoryBudget)
            }
            _ => None,
        }
    }
    pub fn code(self) -> tonic::Code {
        match self {
            Self::LocalScopeUnavailable => tonic::Code::FailedPrecondition,
            Self::LocalScopeClosedOrExpired => tonic::Code::FailedPrecondition,
            Self::LocalScopeCountExceeded => tonic::Code::FailedPrecondition,
            Self::LocalReplayCountExceeded => tonic::Code::FailedPrecondition,
            Self::CachedCreationOutcomeUnavailable => tonic::Code::FailedPrecondition,
            Self::CloudReplaySourceChanged => tonic::Code::FailedPrecondition,
            Self::ClientScopeExpired => tonic::Code::FailedPrecondition,
            Self::ServerScopeUnavailable => tonic::Code::FailedPrecondition,
            Self::ServerScopeExpired => tonic::Code::FailedPrecondition,
            Self::ServerScopeClosed => tonic::Code::FailedPrecondition,
            Self::ServerReplayExpired => tonic::Code::FailedPrecondition,
            Self::ServerReplayClosed => tonic::Code::FailedPrecondition,
            Self::ServerCursorExpired => tonic::Code::FailedPrecondition,
            Self::ServerCursorUnavailable => tonic::Code::FailedPrecondition,
            Self::ServerQueryClosed => tonic::Code::FailedPrecondition,
            Self::CloudSourceReplayUnavailable => tonic::Code::FailedPrecondition,
            Self::ServerActiveScopeQuota => tonic::Code::ResourceExhausted,
            Self::ServerScopeReplayHistoryQuota => tonic::Code::ResourceExhausted,
            Self::ServerActiveQueryQuota => tonic::Code::ResourceExhausted,
            Self::ServerQueryReplayHistoryQuota => tonic::Code::ResourceExhausted,
            Self::LocalRegistryBytesExceeded => tonic::Code::ResourceExhausted,
            Self::LocalRegistryBytesExhausted => tonic::Code::ResourceExhausted,
            Self::ServerExactGraphWorkBudget => tonic::Code::ResourceExhausted,
            Self::ServerDerivedIndexModelLimit => tonic::Code::ResourceExhausted,
            Self::ServerScopeMemory => tonic::Code::ResourceExhausted,
            Self::ServerScopeHandleMemory => tonic::Code::ResourceExhausted,
            Self::ServerCloudScopeMemory => tonic::Code::ResourceExhausted,
            Self::ServerFrozenGraphMemory => tonic::Code::ResourceExhausted,
            Self::ServerOverlayMemory => tonic::Code::ResourceExhausted,
            Self::ServerOverlayMemoryOverflow => tonic::Code::ResourceExhausted,
            Self::ServerLogicalOverlayLimit => tonic::Code::ResourceExhausted,
            Self::ServerQueryHistoryMemory => tonic::Code::ResourceExhausted,
            Self::ServerQueryCursorMemory => tonic::Code::ResourceExhausted,
            Self::ServerMembershipMemory => tonic::Code::ResourceExhausted,
            Self::ServerSelectedPrefixMemory => tonic::Code::ResourceExhausted,
            Self::ClientOverlayBytes => tonic::Code::ResourceExhausted,
            Self::ClientOverlayMemory => tonic::Code::ResourceExhausted,
            Self::ClientScopeSourceMemory => tonic::Code::ResourceExhausted,
            Self::ClientAnnotationStateHeadroom => tonic::Code::ResourceExhausted,
            Self::ClientAnnotationByteHeadroom => tonic::Code::ResourceExhausted,
            Self::ClientAnnotationHeaderReservation => tonic::Code::ResourceExhausted,
            Self::ClientAnnotationActualReservation => tonic::Code::ResourceExhausted,
            Self::ClientCloudResourceLimit => tonic::Code::ResourceExhausted,
            Self::ServerAnnotationCumulativeBudget => tonic::Code::ResourceExhausted,
            Self::ServerAnnotationMemoryBusy => tonic::Code::ResourceExhausted,
            Self::ServerAnnotationMemoryRange => tonic::Code::ResourceExhausted,
            Self::ServerAnnotationBusy => tonic::Code::ResourceExhausted,
            Self::ServerAnnotationResponseLimit => tonic::Code::ResourceExhausted,
            Self::ServerAnnotationSequenceLimit => tonic::Code::ResourceExhausted,
            Self::ServerAnnotationOutputOverflow => tonic::Code::ResourceExhausted,
            Self::ClientQueryDecodeAllocation => tonic::Code::ResourceExhausted,
            Self::ClientQueryRpcMemory => tonic::Code::ResourceExhausted,
            Self::BrokerRetainedReplyMemory => tonic::Code::ResourceExhausted,
            Self::BrokerReplyBytes => tonic::Code::ResourceExhausted,
            Self::ServerRequestMemoryBudget => tonic::Code::ResourceExhausted,
            Self::ServerSelectedPrefixMemoryBudget => tonic::Code::ResourceExhausted,
            Self::ServerQueryCursorMemoryBudget => tonic::Code::ResourceExhausted,
            Self::ServerMembershipMemoryBudget => tonic::Code::ResourceExhausted,
            Self::ServerMembershipStepBudget => tonic::Code::ResourceExhausted,
            Self::ServerClosedQueryMetadataMemory => tonic::Code::ResourceExhausted,
            Self::ServerQueryHistoryMemoryBudget => tonic::Code::ResourceExhausted,
            Self::ServerOverlayLayerOverflow => tonic::Code::ResourceExhausted,
        }
    }
    pub fn diagnostic(self) -> &'static str {
        match self {
            Self::LocalScopeUnavailable => "native query precondition: LocalScopeUnavailable",
            Self::LocalScopeClosedOrExpired => {
                "native query precondition: LocalScopeClosedOrExpired"
            }
            Self::LocalScopeCountExceeded => "native query precondition: LocalScopeCountExceeded",
            Self::LocalReplayCountExceeded => "native query precondition: LocalReplayCountExceeded",
            Self::CachedCreationOutcomeUnavailable => {
                "native query precondition: CachedCreationOutcomeUnavailable"
            }
            Self::CloudReplaySourceChanged => "native query precondition: CloudReplaySourceChanged",
            Self::ClientScopeExpired => "native query precondition: ClientScopeExpired",
            Self::ServerScopeUnavailable => "native query precondition: ServerScopeUnavailable",
            Self::ServerScopeExpired => "native query precondition: ServerScopeExpired",
            Self::ServerScopeClosed => "native query precondition: ServerScopeClosed",
            Self::ServerReplayExpired => "native query precondition: ServerReplayExpired",
            Self::ServerReplayClosed => "native query precondition: ServerReplayClosed",
            Self::ServerCursorExpired => "native query precondition: ServerCursorExpired",
            Self::ServerCursorUnavailable => "native query precondition: ServerCursorUnavailable",
            Self::ServerQueryClosed => "native query precondition: ServerQueryClosed",
            Self::CloudSourceReplayUnavailable => {
                "native query precondition: CloudSourceReplayUnavailable"
            }
            Self::ServerActiveScopeQuota => "native query resource: ServerActiveScopeQuota",
            Self::ServerScopeReplayHistoryQuota => {
                "native query resource: ServerScopeReplayHistoryQuota"
            }
            Self::ServerActiveQueryQuota => "native query resource: ServerActiveQueryQuota",
            Self::ServerQueryReplayHistoryQuota => {
                "native query resource: ServerQueryReplayHistoryQuota"
            }
            Self::LocalRegistryBytesExceeded => "native query resource: LocalRegistryBytesExceeded",
            Self::LocalRegistryBytesExhausted => {
                "native query resource: LocalRegistryBytesExhausted"
            }
            Self::ServerExactGraphWorkBudget => "native query resource: ServerExactGraphWorkBudget",
            Self::ServerDerivedIndexModelLimit => {
                "native query resource: ServerDerivedIndexModelLimit"
            }
            Self::ServerScopeMemory => "native query resource: ServerScopeMemory",
            Self::ServerScopeHandleMemory => "native query resource: ServerScopeHandleMemory",
            Self::ServerCloudScopeMemory => "native query resource: ServerCloudScopeMemory",
            Self::ServerFrozenGraphMemory => "native query resource: ServerFrozenGraphMemory",
            Self::ServerOverlayMemory => "native query resource: ServerOverlayMemory",
            Self::ServerOverlayMemoryOverflow => {
                "native query resource: ServerOverlayMemoryOverflow"
            }
            Self::ServerLogicalOverlayLimit => "native query resource: ServerLogicalOverlayLimit",
            Self::ServerQueryHistoryMemory => "native query resource: ServerQueryHistoryMemory",
            Self::ServerQueryCursorMemory => "native query resource: ServerQueryCursorMemory",
            Self::ServerMembershipMemory => "native query resource: ServerMembershipMemory",
            Self::ServerSelectedPrefixMemory => "native query resource: ServerSelectedPrefixMemory",
            Self::ClientOverlayBytes => "native query resource: ClientOverlayBytes",
            Self::ClientOverlayMemory => "native query resource: ClientOverlayMemory",
            Self::ClientScopeSourceMemory => "native query resource: ClientScopeSourceMemory",
            Self::ClientAnnotationStateHeadroom => {
                "native query resource: ClientAnnotationStateHeadroom"
            }
            Self::ClientAnnotationByteHeadroom => {
                "native query resource: ClientAnnotationByteHeadroom"
            }
            Self::ClientAnnotationHeaderReservation => {
                "native query resource: ClientAnnotationHeaderReservation"
            }
            Self::ClientAnnotationActualReservation => {
                "native query resource: ClientAnnotationActualReservation"
            }
            Self::ClientCloudResourceLimit => "native query resource: ClientCloudResourceLimit",
            Self::ServerAnnotationCumulativeBudget => {
                "native query resource: ServerAnnotationCumulativeBudget"
            }
            Self::ServerAnnotationMemoryBusy => "native query resource: ServerAnnotationMemoryBusy",
            Self::ServerAnnotationMemoryRange => {
                "native query resource: ServerAnnotationMemoryRange"
            }
            Self::ServerAnnotationBusy => "native query resource: ServerAnnotationBusy",
            Self::ServerAnnotationResponseLimit => {
                "native query resource: ServerAnnotationResponseLimit"
            }
            Self::ServerAnnotationSequenceLimit => {
                "native query resource: ServerAnnotationSequenceLimit"
            }
            Self::ServerAnnotationOutputOverflow => {
                "native query resource: ServerAnnotationOutputOverflow"
            }
            Self::ClientQueryDecodeAllocation => {
                "native query resource: ClientQueryDecodeAllocation"
            }
            Self::ClientQueryRpcMemory => "native query resource: ClientQueryRpcMemory",
            Self::BrokerRetainedReplyMemory => "native query resource: BrokerRetainedReplyMemory",
            Self::BrokerReplyBytes => "native query resource: BrokerReplyBytes",
            Self::ServerRequestMemoryBudget => "native query resource: ServerRequestMemoryBudget",
            Self::ServerSelectedPrefixMemoryBudget => {
                "native query resource: ServerSelectedPrefixMemoryBudget"
            }
            Self::ServerQueryCursorMemoryBudget => {
                "native query resource: ServerQueryCursorMemoryBudget"
            }
            Self::ServerMembershipMemoryBudget => {
                "native query resource: ServerMembershipMemoryBudget"
            }
            Self::ServerMembershipStepBudget => "native query resource: ServerMembershipStepBudget",
            Self::ServerClosedQueryMetadataMemory => {
                "native query resource: ServerClosedQueryMetadataMemory"
            }
            Self::ServerQueryHistoryMemoryBudget => {
                "native query resource: ServerQueryHistoryMemoryBudget"
            }
            Self::ServerOverlayLayerOverflow => "native query resource: ServerOverlayLayerOverflow",
        }
    }
    /// Accept only our canonical static categories and their original status code.
    pub fn from_status(error: &Status) -> Option<Self> {
        let reason = match error.message() {
            "native query precondition: LocalScopeUnavailable" => Self::LocalScopeUnavailable,
            "native query precondition: LocalScopeClosedOrExpired" => {
                Self::LocalScopeClosedOrExpired
            }
            "native query precondition: LocalScopeCountExceeded" => Self::LocalScopeCountExceeded,
            "native query precondition: LocalReplayCountExceeded" => Self::LocalReplayCountExceeded,
            "native query precondition: CachedCreationOutcomeUnavailable" => {
                Self::CachedCreationOutcomeUnavailable
            }
            "native query precondition: CloudReplaySourceChanged" => Self::CloudReplaySourceChanged,
            "native query precondition: ClientScopeExpired" => Self::ClientScopeExpired,
            "native query precondition: ServerScopeUnavailable" => Self::ServerScopeUnavailable,
            "native query precondition: ServerScopeExpired" => Self::ServerScopeExpired,
            "native query precondition: ServerScopeClosed" => Self::ServerScopeClosed,
            "native query precondition: ServerReplayExpired" => Self::ServerReplayExpired,
            "native query precondition: ServerReplayClosed" => Self::ServerReplayClosed,
            "native query precondition: ServerCursorExpired" => Self::ServerCursorExpired,
            "native query precondition: ServerCursorUnavailable" => Self::ServerCursorUnavailable,
            "native query precondition: ServerQueryClosed" => Self::ServerQueryClosed,
            "native query precondition: CloudSourceReplayUnavailable" => {
                Self::CloudSourceReplayUnavailable
            }
            "native query resource: ServerActiveScopeQuota" => Self::ServerActiveScopeQuota,
            "native query resource: ServerScopeReplayHistoryQuota" => {
                Self::ServerScopeReplayHistoryQuota
            }
            "native query resource: ServerActiveQueryQuota" => Self::ServerActiveQueryQuota,
            "native query resource: ServerQueryReplayHistoryQuota" => {
                Self::ServerQueryReplayHistoryQuota
            }
            "native query resource: LocalRegistryBytesExceeded" => Self::LocalRegistryBytesExceeded,
            "native query resource: LocalRegistryBytesExhausted" => {
                Self::LocalRegistryBytesExhausted
            }
            "native query resource: ServerExactGraphWorkBudget" => Self::ServerExactGraphWorkBudget,
            "native query resource: ServerDerivedIndexModelLimit" => {
                Self::ServerDerivedIndexModelLimit
            }
            "native query resource: ServerScopeMemory" => Self::ServerScopeMemory,
            "native query resource: ServerScopeHandleMemory" => Self::ServerScopeHandleMemory,
            "native query resource: ServerCloudScopeMemory" => Self::ServerCloudScopeMemory,
            "native query resource: ServerFrozenGraphMemory" => Self::ServerFrozenGraphMemory,
            "native query resource: ServerOverlayMemory" => Self::ServerOverlayMemory,
            "native query resource: ServerOverlayMemoryOverflow" => {
                Self::ServerOverlayMemoryOverflow
            }
            "native query resource: ServerLogicalOverlayLimit" => Self::ServerLogicalOverlayLimit,
            "native query resource: ServerQueryHistoryMemory" => Self::ServerQueryHistoryMemory,
            "native query resource: ServerQueryCursorMemory" => Self::ServerQueryCursorMemory,
            "native query resource: ServerMembershipMemory" => Self::ServerMembershipMemory,
            "native query resource: ServerSelectedPrefixMemory" => Self::ServerSelectedPrefixMemory,
            "native query resource: ClientOverlayBytes" => Self::ClientOverlayBytes,
            "native query resource: ClientOverlayMemory" => Self::ClientOverlayMemory,
            "native query resource: ClientScopeSourceMemory" => Self::ClientScopeSourceMemory,
            "native query resource: ClientAnnotationStateHeadroom" => {
                Self::ClientAnnotationStateHeadroom
            }
            "native query resource: ClientAnnotationByteHeadroom" => {
                Self::ClientAnnotationByteHeadroom
            }
            "native query resource: ClientAnnotationHeaderReservation" => {
                Self::ClientAnnotationHeaderReservation
            }
            "native query resource: ClientAnnotationActualReservation" => {
                Self::ClientAnnotationActualReservation
            }
            "native query resource: ClientCloudResourceLimit" => Self::ClientCloudResourceLimit,
            "native query resource: ServerAnnotationCumulativeBudget" => {
                Self::ServerAnnotationCumulativeBudget
            }
            "native query resource: ServerAnnotationMemoryBusy" => Self::ServerAnnotationMemoryBusy,
            "native query resource: ServerAnnotationMemoryRange" => {
                Self::ServerAnnotationMemoryRange
            }
            "native query resource: ServerAnnotationBusy" => Self::ServerAnnotationBusy,
            "native query resource: ServerAnnotationResponseLimit" => {
                Self::ServerAnnotationResponseLimit
            }
            "native query resource: ServerAnnotationSequenceLimit" => {
                Self::ServerAnnotationSequenceLimit
            }
            "native query resource: ServerAnnotationOutputOverflow" => {
                Self::ServerAnnotationOutputOverflow
            }
            "native query resource: ClientQueryDecodeAllocation" => {
                Self::ClientQueryDecodeAllocation
            }
            "native query resource: ClientQueryRpcMemory" => Self::ClientQueryRpcMemory,
            "native query resource: BrokerRetainedReplyMemory" => Self::BrokerRetainedReplyMemory,
            "native query resource: BrokerReplyBytes" => Self::BrokerReplyBytes,
            "native query resource: ServerRequestMemoryBudget" => Self::ServerRequestMemoryBudget,
            "native query resource: ServerSelectedPrefixMemoryBudget" => {
                Self::ServerSelectedPrefixMemoryBudget
            }
            "native query resource: ServerQueryCursorMemoryBudget" => {
                Self::ServerQueryCursorMemoryBudget
            }
            "native query resource: ServerMembershipMemoryBudget" => {
                Self::ServerMembershipMemoryBudget
            }
            "native query resource: ServerMembershipStepBudget" => Self::ServerMembershipStepBudget,
            "native query resource: ServerClosedQueryMetadataMemory" => {
                Self::ServerClosedQueryMetadataMemory
            }
            "native query resource: ServerQueryHistoryMemoryBudget" => {
                Self::ServerQueryHistoryMemoryBudget
            }
            "native query resource: ServerOverlayLayerOverflow" => Self::ServerOverlayLayerOverflow,
            _ => return None,
        };
        (reason.code() == error.code()).then_some(reason)
    }
}

/// Do not reflect upstream diagnostics, handles or connection metadata at the
/// local facade boundary. Preserve typed codes and allowlisted static categories.
pub(crate) fn status(error: Status) -> Status {
    if let Some(reason) = QueryFailureReason::from_status(&error).or_else(|| {
        QueryFailureReason::from_source_message(error.message())
            .filter(|reason| reason.code() == error.code())
    }) {
        return Status::new(error.code(), reason.diagnostic());
    }
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
