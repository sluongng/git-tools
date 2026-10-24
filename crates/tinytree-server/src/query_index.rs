//! Persistent derived graph hosting. Canonical object ACKs never wait for this
//! queue. Process-local scope handles expire; durable verified nodes are reused.
use crate::{
    ServiceConfig,
    config::ConfigError,
    query_overlay::{OverlayGraph, OverlayLimits},
};
use prost::Message;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tinytree_index::{Index, Scope, WorkBudget};
use tinytree_model as m;
use tinytree_proto::query_v1 as q;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tonic::Status;

#[derive(Clone, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueryConfig {
    pub max_queries: usize,
    pub max_queries_per_principal: usize,
    pub max_queries_per_repository: usize,
    pub max_query_replays: usize,
    pub max_query_replays_per_principal: usize,
    pub max_query_replays_per_repository: usize,
    pub max_cursor_memory_bytes: usize,
    pub query_ttl_seconds: usize,
    pub max_page_size: usize,
    pub max_graph_query_nodes: usize,
    pub max_graph_query_edges: usize,
    pub max_graph_query_keys: usize,
    pub max_graph_query_frames: usize,
    pub max_graph_query_step_work: usize,
    pub max_graph_query_steps: usize,
    pub max_eval_ast_nodes: usize,
    pub max_eval_ast_depth: usize,
    pub max_eval_literals: usize,
    pub max_eval_frontier: usize,
    pub max_eval_intervals: usize,
    pub max_eval_latest: usize,
    pub max_eval_step_work: usize,
    pub max_eval_steps: usize,
    pub max_eval_quantum_steps: usize,
    pub max_scopes: usize,
    pub max_scopes_per_principal: usize,
    pub max_scopes_per_repository: usize,
    pub max_scope_memory_bytes: usize,
    pub max_scope_replays: usize,
    pub max_scope_replays_per_principal: usize,
    pub max_scope_replays_per_repository: usize,
    pub scope_ttl_seconds: u64,
    pub expiry_interval_ms: u64,
    pub max_roots: usize,
    pub max_request_bytes: usize,
    pub max_inflight: usize,
    pub max_request_memory_bytes: usize,
    pub max_facts: usize,
    pub max_input_ids: usize,
    pub max_result_ids: usize,
    pub max_response_bytes: usize,
    pub max_work_nodes: u64,
    pub max_work_edges: u64,
    pub max_work_bytes: u64,
    pub max_frontier: usize,
    pub max_pending_warms: usize,
    pub max_warm_jobs: usize,
    pub max_warm_memory_bytes: usize,
    pub max_warm_nodes: usize,
    pub max_warm_edges: usize,
    pub max_warm_bytes: usize,
    pub max_warm_read_bytes: usize,
    pub max_warm_frontier: usize,
    pub max_commit_bytes: usize,
    pub max_commit_parents: usize,
    pub warm_timeout_seconds: u64,
    pub max_index_nodes: usize,
    pub max_index_record_bytes: usize,
    pub max_index_bytes: u64,
    pub index_rebuild_generation: u64,
    pub max_overlay_memory_bytes: usize,
    pub max_scope_overlay_bytes: usize,
    pub max_scope_overlay_nodes: usize,
    pub max_overlay_layers: usize,
    pub max_history_commits: usize,
    pub max_history_object_bytes: usize,
    pub max_history_read_bytes: usize,
    pub max_history_path_depth: usize,
    pub max_blame_file_bytes: usize,
    pub max_blame_lines: usize,
    /// Aggregate comparison work across one blame request.
    pub max_blame_diff_steps: usize,
    /// Conservative allocation charge for one comparison, not total allocation
    /// across history or exact resident bytes. Physical admission is separate.
    pub max_blame_diff_memory_bytes: usize,
    pub max_blame_diff_depth: usize,
    pub max_search_entries: usize,
    pub max_search_manifest_bytes: usize,
    pub max_search_cache_bytes: u64,
    pub max_search_cache_records: usize,
    pub max_search_file_bytes: usize,
    pub max_search_read_bytes: usize,
    pub max_search_entries_per_page: usize,
    pub max_search_total_entries: u64,
    pub max_search_total_read_bytes: u64,
    pub max_search_match_work: usize,
    pub max_search_total_work: u64,
    pub max_search_path_depth: usize,
    pub max_pending_search_warms: usize,
    pub search_warm_timeout_seconds: u64,
    pub max_search_warm_file_bytes: usize,
    pub max_search_warm_read_bytes: u64,
    pub max_search_warm_memory_bytes: usize,
}
impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_queries: 64,
            max_queries_per_principal: 8,
            max_queries_per_repository: 32,
            max_query_replays: 512,
            max_query_replays_per_principal: 128,
            max_query_replays_per_repository: 256,
            max_cursor_memory_bytes: 134217728,
            query_ttl_seconds: 120,
            max_page_size: 128,
            max_graph_query_nodes: 8192,
            max_graph_query_edges: 8192,
            max_graph_query_keys: 16384,
            max_graph_query_frames: 4096,
            max_graph_query_step_work: 128,
            max_graph_query_steps: 4000000,
            max_eval_ast_nodes: 64,
            max_eval_ast_depth: 16,
            max_eval_literals: 1024,
            max_eval_frontier: 4096,
            max_eval_intervals: 8192,
            max_eval_latest: 1024,
            max_eval_step_work: 128,
            max_eval_steps: 1_000_000,
            max_eval_quantum_steps: 128,
            max_scopes: 128,
            max_scopes_per_principal: 16,
            max_scopes_per_repository: 64,
            max_scope_memory_bytes: 8 * 1024 * 1024,
            max_scope_replays: 1024,
            max_scope_replays_per_principal: 256,
            max_scope_replays_per_repository: 512,
            scope_ttl_seconds: 300,
            expiry_interval_ms: 1000,
            max_roots: 64,
            max_request_bytes: 256 * 1024,
            max_inflight: 2,
            max_request_memory_bytes: 1024 * 1024 * 1024,
            max_facts: 64,
            max_input_ids: 1024,
            max_result_ids: 16_384,
            max_response_bytes: 2 * 1024 * 1024,
            max_work_nodes: 100_000,
            max_work_edges: 1_000_000,
            max_work_bytes: 32 * 1024 * 1024,
            max_frontier: 100_000,
            max_pending_warms: 32,
            max_warm_jobs: 1,
            max_warm_memory_bytes: 128 * 1024 * 1024,
            max_warm_nodes: 100_000,
            max_warm_edges: 200_000,
            max_warm_bytes: 16 * 1024 * 1024,
            max_warm_read_bytes: 256 * 1024 * 1024,
            max_warm_frontier: 100_000,
            max_commit_bytes: 256 * 1024,
            max_commit_parents: 4096,
            warm_timeout_seconds: 300,
            max_index_nodes: 1_000_000,
            max_index_record_bytes: 1024 * 1024,
            max_index_bytes: 1024 * 1024 * 1024,
            index_rebuild_generation: 0,
            max_overlay_memory_bytes: 128 * 1024 * 1024,
            max_scope_overlay_bytes: 16 * 1024 * 1024,
            max_scope_overlay_nodes: 4096,
            max_overlay_layers: 32,
            max_history_commits: 256,
            max_history_object_bytes: 1024 * 1024,
            max_history_read_bytes: 16 * 1024 * 1024,
            max_history_path_depth: 128,
            max_blame_file_bytes: 256 * 1024,
            max_blame_lines: 16_384,
            max_blame_diff_steps: 16_000_000,
            max_blame_diff_memory_bytes: 8 * 1024 * 1024,
            max_blame_diff_depth: 64,
            max_search_entries: 65_536,
            max_search_manifest_bytes: 8 * 1024 * 1024,
            max_search_cache_bytes: 64 * 1024 * 1024,
            max_search_cache_records: 65_536,
            max_search_file_bytes: 1024 * 1024,
            max_search_read_bytes: 8 * 1024 * 1024,
            max_search_entries_per_page: 2048,
            max_search_total_entries: 262_144,
            max_search_total_read_bytes: 256 * 1024 * 1024,
            max_search_match_work: 8_000_000,
            max_search_total_work: 64_000_000,
            max_search_path_depth: 128,
            max_pending_search_warms: 4,
            search_warm_timeout_seconds: 60,
            max_search_warm_file_bytes: 64 * 1024 * 1024,
            max_search_warm_read_bytes: 512 * 1024 * 1024,
            max_search_warm_memory_bytes: 192 * 1024 * 1024,
        }
    }
}
impl QueryConfig {
    pub(crate) fn request_memory(&self, request_bytes: usize) -> usize {
        request_bytes
            .min(self.max_request_bytes)
            .saturating_mul(128)
            .saturating_add(crate::SERVER_CODEC_MEMORY_BYTES)
            .saturating_add(self.max_frontier.saturating_mul(256))
            .saturating_add(self.max_response_bytes.saturating_mul(4))
            .saturating_add(self.max_index_record_bytes.saturating_mul(4))
            .saturating_add(self.max_scope_overlay_bytes.saturating_mul(2))
            .saturating_add(8 * 1024 * 1024)
    }
    // Only path history/blame reserve these additional buffers. Charging every
    // graph/close request would reduce existing progress while a response is held.
    pub(crate) fn history_memory(&self) -> usize {
        self.max_history_object_bytes
            .max(self.max_commit_bytes)
            .saturating_mul(8)
            .saturating_add(self.max_blame_file_bytes.saturating_mul(8))
            .saturating_add(self.max_blame_diff_memory_bytes.saturating_mul(2))
            .saturating_add(self.max_blame_lines.saturating_mul(256))
            .saturating_add(self.max_history_commits.saturating_mul(512))
    }
    pub(crate) fn search_memory(&self) -> usize {
        self.max_search_manifest_bytes
            .saturating_mul(3)
            .saturating_add(self.max_search_file_bytes.saturating_mul(4))
            .saturating_add(self.max_response_bytes.saturating_mul(2))
    }
    pub(crate) fn search_warm_memory(&self) -> usize {
        // Packed FILE reconstruction plus the caller's verified bytes; manifest
        // strings/serialization; wide TREE decode before the entry limit; and
        // amortized inventory/unzip/dedup container capacity (not just length).
        self.max_search_warm_file_bytes
            .saturating_mul(2)
            .saturating_add(self.max_search_manifest_bytes.saturating_mul(3))
            .saturating_add(self.max_history_object_bytes.saturating_mul(8))
            .saturating_add(self.max_search_entries.saturating_mul(512))
    }
    pub(crate) fn search_content_limits(&self) -> tinytree_index::content::ContentLimits {
        tinytree_index::content::ContentLimits {
            max_bytes: self.max_search_cache_bytes,
            max_records: self.max_search_cache_records,
            max_manifest_entries: self.max_search_entries,
            max_manifest_bytes: self.max_search_manifest_bytes,
            max_path_bytes: 4096,
        }
    }
    fn warm_memory(&self) -> usize {
        self.max_warm_bytes
            .saturating_mul(4)
            // Active/done sets, payload-map buckets, and bounded FIFO IDs.
            .saturating_add(self.max_warm_nodes.saturating_mul(384))
            .saturating_add(self.max_warm_edges.saturating_mul(64))
            .saturating_add(self.max_commit_bytes.saturating_mul(8))
            .saturating_add(8 * 1024 * 1024)
    }
    pub(crate) fn validate(&self, request_bytes: usize) -> Result<(), ConfigError> {
        crate::query_cursor::validate_config(self)?;
        if !(1..=4096).contains(&self.max_scopes)
            || self.max_scopes_per_principal == 0
            || self.max_scopes_per_principal > self.max_scopes
            || self.max_scopes_per_repository == 0
            || self.max_scopes_per_repository > self.max_scopes
            || !(1..=65_536).contains(&self.max_scope_replays)
            || self.max_scope_replays_per_principal == 0
            || self.max_scope_replays_per_principal > self.max_scope_replays
            || self.max_scope_replays_per_repository == 0
            || self.max_scope_replays_per_repository > self.max_scope_replays
            || !(1024..=1024 * 1024 * 1024).contains(&self.max_scope_memory_bytes)
            || !(1..=86_400).contains(&self.scope_ttl_seconds)
            || !(10..=60_000).contains(&self.expiry_interval_ms)
            || !(1..=4096).contains(&self.max_roots)
            || !(1024..=1024 * 1024).contains(&self.max_request_bytes)
            || !(1..=32).contains(&self.max_inflight)
            || self.max_request_memory_bytes > 1024 * 1024 * 1024
            || self
                .request_memory(request_bytes)
                .saturating_add(self.history_memory())
                > self.max_request_memory_bytes
            || !(1..=1024).contains(&self.max_facts)
            || !(1..=100_000).contains(&self.max_input_ids)
            || !(1..=100_000).contains(&self.max_result_ids)
            || !(1024..=16 * 1024 * 1024).contains(&self.max_response_bytes)
            || self.max_work_nodes == 0
            || self.max_work_nodes > 1_000_000
            || self.max_work_edges == 0
            || self.max_work_edges > 10_000_000
            || self.max_work_bytes == 0
            || self.max_work_bytes > 1024 * 1024 * 1024
            || self.max_frontier == 0
            || self.max_frontier > 1_000_000
            || !(1..=1024).contains(&self.max_pending_warms)
            || !(1..=4).contains(&self.max_warm_jobs)
            || self.max_warm_memory_bytes > 1024 * 1024 * 1024
            || self.warm_memory().saturating_mul(self.max_warm_jobs) > self.max_warm_memory_bytes
            || self.max_warm_nodes == 0
            || self.max_warm_nodes > 1_000_000
            || self.max_warm_edges == 0
            || self.max_warm_edges > 10_000_000
            || self.max_warm_bytes == 0
            || self.max_warm_bytes > 256 * 1024 * 1024
            || self.max_warm_read_bytes == 0
            || self.max_warm_read_bytes > 1024 * 1024 * 1024
            || self.max_warm_frontier == 0
            || self.max_warm_frontier > 1_000_000
            || !(1024..=16 * 1024 * 1024).contains(&self.max_commit_bytes)
            || !(1..=65_536).contains(&self.max_commit_parents)
            || !(1..=3600).contains(&self.warm_timeout_seconds)
            || self.max_index_nodes == 0
            || self.max_index_nodes > 10_000_000
            || !(1024..=16 * 1024 * 1024).contains(&self.max_index_record_bytes)
            || self.max_index_bytes == 0
            || self.max_index_bytes > 64 * 1024 * 1024 * 1024
            || self.index_rebuild_generation > 1_000_000_000
            || self.max_overlay_memory_bytes > 1024 * 1024 * 1024
            || !(1..=64 * 1024 * 1024).contains(&self.max_scope_overlay_bytes)
            || !(1..=65_536).contains(&self.max_scope_overlay_nodes)
            || !(1..=128).contains(&self.max_overlay_layers)
            || !(1..=4096).contains(&self.max_history_commits)
            || !(1024..=4 * 1024 * 1024).contains(&self.max_history_object_bytes)
            || !(1024..=256 * 1024 * 1024).contains(&self.max_history_read_bytes)
            || !(1..=1024).contains(&self.max_history_path_depth)
            || !(1..=1024 * 1024).contains(&self.max_blame_file_bytes)
            || !(1..=65_536).contains(&self.max_blame_lines)
            || !(1..=100_000_000).contains(&self.max_blame_diff_steps)
            || !(1024..=64 * 1024 * 1024).contains(&self.max_blame_diff_memory_bytes)
            || !(1..=256).contains(&self.max_blame_diff_depth)
            || !(1..=65_536).contains(&self.max_search_entries)
            || !(1024..=16 * 1024 * 1024).contains(&self.max_search_manifest_bytes)
            || !(1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_search_cache_bytes)
            || !(16..=262_144).contains(&self.max_search_cache_records)
            || !(1..=4 * 1024 * 1024).contains(&self.max_search_file_bytes)
            || self.max_search_read_bytes < self.max_search_file_bytes
            || self.max_search_read_bytes > 64 * 1024 * 1024
            || !(1..=65_536).contains(&self.max_search_entries_per_page)
            || self.max_search_total_entries < self.max_search_entries_per_page as u64
            || self.max_search_total_entries > 1_000_000
            || self.max_search_total_read_bytes < self.max_search_read_bytes as u64
            || self.max_search_total_read_bytes > 4 * 1024 * 1024 * 1024
            || !(1..=100_000_000).contains(&self.max_search_match_work)
            || self.max_search_total_work < self.max_search_match_work as u64
            || self.max_search_total_work > 1_000_000_000
            || !(1..=1024).contains(&self.max_search_path_depth)
            || !(1..=32).contains(&self.max_pending_search_warms)
            || !(1..=600).contains(&self.search_warm_timeout_seconds)
            || self.max_search_warm_file_bytes < self.max_search_file_bytes
            || self.max_search_warm_file_bytes > 64 * 1024 * 1024
            || self.max_search_warm_read_bytes < self.max_search_warm_file_bytes as u64
            || self.max_search_warm_read_bytes > 4 * 1024 * 1024 * 1024
            || !(1024..=512 * 1024 * 1024).contains(&self.max_search_warm_memory_bytes)
            || self
                .max_scope_overlay_bytes
                .saturating_mul(4)
                .saturating_add(self.max_scope_overlay_nodes.saturating_mul(512))
                > self.max_overlay_memory_bytes
        {
            return Err(ConfigError("invalid derived query limits"));
        }
        Ok(())
    }
    pub(crate) fn index_limits(&self) -> tinytree_index::Limits {
        tinytree_index::Limits {
            max_nodes: self.max_index_nodes,
            max_record_bytes: self.max_index_record_bytes,
            max_index_bytes: self.max_index_bytes,
            max_roots: self.max_roots,
            max_parents: self.max_commit_parents,
            model: m::Limits {
                max_object_bytes: self.max_commit_bytes,
                max_items: self.max_commit_parents,
                ..Default::default()
            },
            ..Default::default()
        }
    }
    pub(crate) fn budget(&self) -> WorkBudget {
        WorkBudget::new(
            self.max_work_nodes,
            self.max_work_edges,
            self.max_work_bytes,
            self.max_frontier,
        )
    }
}
pub(crate) fn now() -> Result<i64, Status> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("query clock unavailable"))?
            .as_secs(),
    )
    .map_err(|_| Status::internal("query clock unavailable"))
}
pub(crate) fn index_error(error: tinytree_index::Error) -> Status {
    use tinytree_index::Error;
    match error {
        Error::NotReady | Error::Unavailable => {
            Status::unavailable("derived graph coverage is not ready")
        }
        Error::BudgetExceeded => Status::resource_exhausted("exact graph work budget exhausted"),
        Error::Invalid(_) => Status::invalid_argument("invalid derived query input"),
        Error::UnsupportedSchema => {
            Status::failed_precondition("derived index schema requires a new generation")
        }
        Error::Corrupt => Status::data_loss("derived graph index requires explicit rebuild"),
        Error::Model(m::Error::Limit) => {
            Status::resource_exhausted("derived index model limit exceeded")
        }
        Error::Model(_) => Status::data_loss("invalid canonical graph metadata"),
        _ => Status::unavailable("derived graph storage unavailable"),
    }
}
pub(crate) struct CloudScopeRetention {
    guard: tinytree_store::CloudSnapshotReadGuard,
    native_binding: [u8; 32],
    _memory: OwnedSemaphorePermit,
}
struct FrozenGraphOwner {
    _source: Arc<CloudScopeRetention>,
    _memory: OwnedSemaphorePermit,
}
pub(crate) struct Scoped {
    pub scope: Arc<Scope>,
    cloud: Option<Arc<CloudScopeRetention>>,
    graph: Option<Arc<OverlayGraph>>,
    pub wire: q::QueryScope,
    parent: Option<Arc<Scoped>>,
    depth: usize,
    roots: Vec<tinytree_store::RootReadGuard>,
    memory: Option<Arc<OwnedSemaphorePermit>>,
    cleanup: std::sync::mpsc::Sender<Cleanup>,
}
impl Scoped {
    pub fn authorizes_frozen(&self, descriptor: m::Descriptor) -> bool {
        self.cloud.as_ref().is_some_and(|source| {
            source
                .guard
                .frozen_commits()
                .binary_search_by_key(&descriptor.key.id, |record| record.descriptor.key.id)
                .is_ok_and(|index| source.guard.frozen_commits()[index].descriptor == descriptor)
        })
    }
    pub fn is_cloud(&self) -> bool {
        self.cloud.is_some()
    }
    pub fn graph(&self) -> &Arc<OverlayGraph> {
        self.graph.as_ref().expect("live scope graph")
    }
}
struct Cleanup {
    cloud: Option<Arc<CloudScopeRetention>>,
    graph: Option<Arc<OverlayGraph>>,
    parent: Option<Arc<Scoped>>,
    roots: Vec<tinytree_store::RootReadGuard>,
    scope: Arc<Scope>,
    memory: Option<Arc<OwnedSemaphorePermit>>,
}
impl Drop for Scoped {
    fn drop(&mut self) {
        let cleanup = Cleanup {
            cloud: self.cloud.take(),
            graph: self.graph.take(),
            parent: self.parent.take(),
            roots: std::mem::take(&mut self.roots),
            scope: self.scope.clone(),
            memory: self.memory.take(),
        };
        // The cleanup receiver lives on a blocking owner thread until every
        // sender disappears. Its queue is charged by each scope's held permit.
        if let Err(error) = self.cleanup.send(cleanup) {
            // A panicked cleanup owner must never block an async worker behind
            // Store GC. Preserve the owners until a fallback blocking drop.
            let _ = std::thread::Builder::new()
                .name("tinytree-query-cleanup-fallback".into())
                .spawn(move || drop(error.0));
        }
    }
}
pub(crate) struct OverlayMemory {
    _permit: OwnedSemaphorePermit,
}
struct ChunkOwner {
    _base: Arc<Scoped>,
    _memory: Arc<OverlayMemory>,
}
pub(crate) struct Prepared {
    pub base: Arc<Scoped>,
    memory: Option<Arc<OverlayMemory>>,
    binding: [u8; 32],
    replayed: bool,
    depth: usize,
    expires_cap: Option<i64>,
}
impl Prepared {
    pub fn digest(&self) -> blake3::Hasher {
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-overlay-scope-content-v1\0");
        hash.update(&self.binding);
        hash.update(&self.base.wire.content_digest);
        hash
    }
}
fn scope_binding(header: &q::ScopeHeader, parent: Option<&q::ScopeKey>) -> [u8; 32] {
    let mut header = header.clone();
    header.replay_key.clear();
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-overlay-scope-header-v1\0");
    hash.update(&header.encode_to_vec());
    match parent {
        Some(parent) => {
            hash.update(&[1]);
            hash.update(&parent.encode_to_vec());
        }
        None => {
            hash.update(&[0]);
        }
    }
    *hash.finalize().as_bytes()
}
struct Record {
    cloud_mode: bool,
    owner: String,
    repository: String,
    replay: [u8; 32],
    digest: [u8; 32],
    binding: [u8; 32],
    expires: i64,
    query_identity_version: u32,
    scope: Option<Arc<Scoped>>,
    _memory: Arc<OwnedSemaphorePermit>,
}
#[derive(Default)]
struct Registry {
    records: HashMap<[u8; 32], Record>,
}
struct WarmJob {
    repository: String,
    root: m::CommitId,
    _pin: tinytree_store::RootReadGuard,
    _cloud: Option<Arc<CloudScopeRetention>>,
}
#[derive(Default)]
struct Warming {
    pending: HashSet<(String, m::CommitId)>,
    failures: HashMap<(String, m::CommitId), (Status, std::time::Instant)>,
}
struct WarmRuntime {
    closing: Arc<AtomicBool>,
    index: Arc<Index>,
    store: tinytree_store::Store,
    config: QueryConfig,
}
#[cfg(test)]
pub(crate) struct QueryPause {
    pub entered: std::sync::mpsc::SyncSender<()>,
    pub resume: std::sync::mpsc::Receiver<()>,
}
enum DrainState {
    Idle,
    Running(tokio::task::JoinHandle<bool>),
    Done(bool),
}
pub(crate) struct QueryHost {
    pub index: Arc<Index>,
    pub config: QueryConfig,
    pub store: tinytree_store::Store,
    pub cursors: crate::query_cursor::CursorHost,
    pub search: std::sync::Arc<crate::query_search::SearchHost>,
    registry: Mutex<Registry>,
    scope_memory: Arc<Semaphore>,
    overlay_memory: Arc<Semaphore>,
    inflight: Arc<Semaphore>,
    memory: Arc<Semaphore>,
    request_memory: usize,
    warm: Mutex<Option<std::sync::mpsc::SyncSender<WarmJob>>>,
    warming: Arc<Mutex<Warming>>,
    cleanup: Mutex<Option<std::sync::mpsc::Sender<Cleanup>>>,
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
    cleanup_worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    janitor: Mutex<Option<tokio::task::JoinHandle<()>>>,
    closing: Arc<AtomicBool>,
    stop: Arc<Notify>,
    drain: tokio::sync::Mutex<DrainState>,
    #[cfg(test)]
    pub pause: Mutex<Option<QueryPause>>,
}
pub(crate) struct Work {
    pub deadline: tokio::time::Instant,
    // Actual payload owners must drop before physical admission is released.
    pub scopes: Mutex<Vec<Arc<Scoped>>>,
    pub query_owners: Mutex<Vec<Arc<dyn std::any::Any + Send + Sync>>>,
    _memory: OwnedSemaphorePermit,
    _inflight: OwnedSemaphorePermit,
    _host: Arc<QueryHost>,
    history_admitted: bool,
    search_admitted: bool,
}

impl Work {
    pub fn has_search_memory(&self) -> bool {
        self.search_admitted
    }
    pub fn has_history_memory(&self) -> bool {
        self.history_admitted
    }
    pub fn belongs_to(&self, host: &Arc<QueryHost>) -> bool {
        Arc::ptr_eq(&self._host, host)
    }
}
impl QueryHost {
    pub fn new(
        index: Arc<Index>,
        store: tinytree_store::Store,
        service: &ServiceConfig,
    ) -> Result<Arc<Self>, ConfigError> {
        let config = service.query.clone();
        config.validate(service.max_request_bytes)?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| ConfigError("query host requires an active runtime"))?;
        let (sender, receiver) = std::sync::mpsc::sync_channel::<WarmJob>(config.max_pending_warms);
        let receiver = Arc::new(Mutex::new(receiver));
        let warming = Arc::new(Mutex::new(Warming::default()));
        let (cleanup, cleanups) = std::sync::mpsc::channel::<Cleanup>();
        let closing = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(Notify::new());
        let cleanup_worker = std::thread::Builder::new()
            .name("tinytree-query-cleanup".into())
            .spawn(move || {
                while let Ok(batch) = cleanups.recv() {
                    // Root destruction can wait behind GC. Quota ownership remains
                    // charged until roots and the derived scope physically drop.
                    drop(batch.graph);
                    drop(batch.parent);
                    drop(batch.roots);
                    drop(batch.scope);
                    drop(batch.cloud);
                    drop(batch.memory);
                }
            })
            .map_err(|_| ConfigError("cannot start query cleanup owner"))?;
        let mut workers = Vec::new();
        for _ in 0..config.max_warm_jobs {
            let receiver = receiver.clone();
            let warming = warming.clone();
            let engine = WarmRuntime {
                closing: closing.clone(),
                index: index.clone(),
                store: store.clone(),
                config: config.clone(),
            };
            let worker = std::thread::Builder::new()
                .name("tinytree-query-warmer".into())
                .spawn(move || {
                    loop {
                        let job = match receiver.lock().unwrap().recv() {
                            Ok(job) => job,
                            Err(_) => break,
                        };
                        let key = (job.repository.clone(), job.root);
                        let result = engine.build(&job);
                        drop(job);
                        let mut state = warming.lock().unwrap();
                        state.pending.remove(&key);
                        match result {
                            Ok(()) => {
                                state.failures.remove(&key);
                            }
                            Err(error) => {
                                if state.failures.len() >= engine.config.max_pending_warms {
                                    if let Some(oldest) = state
                                        .failures
                                        .iter()
                                        .min_by_key(|(_, (_, when))| *when)
                                        .map(|(key, _)| key.clone())
                                    {
                                        state.failures.remove(&oldest);
                                    }
                                }
                                state
                                    .failures
                                    .insert(key, (error, std::time::Instant::now()));
                            }
                        }
                    }
                })
                .map_err(|_| ConfigError("cannot start derived query warmer"))?;
            workers.push(worker);
        }
        let cursors = crate::query_cursor::CursorHost::new(&config)?;
        let search = crate::query_search::SearchHost::new(
            index.clone(),
            store.clone(),
            config.clone(),
            closing.clone(),
        )?;
        let host = Arc::new(Self {
            cursors,
            search,
            index,
            store,
            request_memory: config.request_memory(service.max_request_bytes),
            scope_memory: Arc::new(Semaphore::new(config.max_scope_memory_bytes)),
            overlay_memory: Arc::new(Semaphore::new(config.max_overlay_memory_bytes)),
            inflight: Arc::new(Semaphore::new(config.max_inflight)),
            memory: Arc::new(Semaphore::new(config.max_request_memory_bytes)),
            config,
            registry: Mutex::new(Registry::default()),
            warm: Mutex::new(Some(sender)),
            warming,
            cleanup: Mutex::new(Some(cleanup)),
            workers: Mutex::new(workers),
            cleanup_worker: Mutex::new(Some(cleanup_worker)),
            janitor: Mutex::new(None),
            closing: closing.clone(),
            stop: stop.clone(),
            drain: tokio::sync::Mutex::new(DrainState::Idle),
            #[cfg(test)]
            pause: Mutex::new(None),
        });
        let weak = Arc::downgrade(&host);
        let interval = host.config.expiry_interval_ms;
        let janitor = runtime.spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_millis(interval));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if closing.load(Ordering::Acquire) {
                    break;
                }
                tokio::select! { _ = ticks.tick() => {}, _ = stop.notified() => {} }
                if closing.load(Ordering::Acquire) {
                    break;
                }
                let Some(host) = weak.upgrade() else { break };
                // Root guard destruction may wait behind Store GC. Never do it
                // on an async worker, and await each bounded cleanup batch.
                let _ = tokio::task::spawn_blocking(move || {
                    if let Ok(now) = now() {
                        host.expire(now);
                    }
                })
                .await;
            }
        });
        *host.janitor.lock().unwrap() = Some(janitor);
        Ok(host)
    }
    /// Service-wide, cancellation-safe shutdown. Cloned listeners share the same
    /// owned task/result; canceling one waiter never detaches physical cleanup.
    pub async fn drain(self: &Arc<Self>) -> Result<(), Status> {
        let mut state = self.drain.lock().await;
        if matches!(*state, DrainState::Idle) {
            self.closing.store(true, Ordering::Release);
            self.stop.notify_one();
            let host = self.clone();
            *state = DrainState::Running(tokio::spawn(async move { host.drain_owned().await }));
        }
        let result = match &mut *state {
            DrainState::Done(ok) => *ok,
            DrainState::Running(task) => task.await.unwrap_or(false),
            DrainState::Idle => unreachable!(),
        };
        *state = DrainState::Done(result);
        if result {
            Ok(())
        } else {
            Err(Status::internal("query physical shutdown failed"))
        }
    }
    async fn drain_owned(self: Arc<Self>) -> bool {
        let janitor = self.janitor.lock().unwrap().take();
        let mut success = match janitor {
            Some(task) => task.await.is_ok(),
            None => true,
        };
        // All accepted physical calls and response payloads release their slots
        // before this barrier can complete. Admissions recheck closing.
        let Ok(_calls) = self
            .inflight
            .clone()
            .acquire_many_owned(self.config.max_inflight as u32)
            .await
        else {
            return false;
        };
        success &= self.search.drain().await;
        self.warm.lock().unwrap().take();
        let workers = std::mem::take(&mut *self.workers.lock().unwrap());
        success &= tokio::task::spawn_blocking(move || {
            let mut ok = true;
            for worker in workers {
                ok &= worker.join().is_ok();
            }
            ok
        })
        .await
        .unwrap_or(false);
        let host = self.clone();
        success &= tokio::task::spawn_blocking(move || {
            host.expire(i64::MAX);
            // Cursor cleanup can itself release captured scopes. Flush it before
            // disconnecting the recursively draining scope cleanup receiver.
            let mut ok = host.cursors.drain_cleanup().is_ok();
            host.cleanup.lock().unwrap().take();
            if let Some(worker) = host.cleanup_worker.lock().unwrap().take() {
                ok &= worker.join().is_ok();
            }
            ok
        })
        .await
        .unwrap_or(false);
        success
    }
    fn cleanup_sender(&self) -> Result<std::sync::mpsc::Sender<Cleanup>, Status> {
        self.cleanup
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| Status::unavailable("query host draining"))
    }
    #[cfg(test)]
    pub fn pause_query(&self) -> Result<(), Status> {
        let pause = self.pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause
                .entered
                .send(())
                .map_err(|_| Status::internal("query test observer stopped"))?;
            pause
                .resume
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| Status::deadline_exceeded("query test interleaving expired"))?;
        }
        Ok(())
    }
    pub async fn admit(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<Work>, Status> {
        self.admit_kind(deadline, false, false).await
    }
    pub async fn admit_history(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<Work>, Status> {
        self.admit_kind(deadline, true, false).await
    }
    pub async fn admit_search(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<Work>, Status> {
        self.admit_kind(deadline, false, true).await
    }
    async fn admit_kind(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
        history: bool,
        search: bool,
    ) -> Result<Arc<Work>, Status> {
        if self.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("query host draining"));
        }
        let bytes = self.request_memory.saturating_add(if history {
            self.config.history_memory()
        } else if search {
            self.config.search_memory()
        } else {
            0
        });
        if bytes > self.config.max_request_memory_bytes {
            return Err(Status::resource_exhausted(
                "requested query memory exceeds configured pool",
            ));
        }
        let permit = tokio::time::timeout_at(deadline, self.inflight.clone().acquire_owned())
            .await
            .map_err(|_| Status::deadline_exceeded("query admission expired"))?
            .map_err(|_| Status::unavailable("query host draining"))?;
        // One weighted acquisition, before decoding. Never hold the base memory
        // reservation while waiting to upgrade to history buffers.
        let memory = tokio::time::timeout_at(
            deadline,
            self.memory.clone().acquire_many_owned(bytes as u32),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("query admission expired"))?
        .map_err(|_| Status::unavailable("query host draining"))?;
        if self.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("query host draining"));
        }
        Ok(Arc::new(Work {
            _host: self.clone(),
            deadline,
            _inflight: permit,
            _memory: memory,
            history_admitted: history,
            search_admitted: search,
            scopes: Mutex::new(Vec::new()),
            query_owners: Mutex::new(Vec::new()),
        }))
    }
    fn expire(&self, now: i64) {
        self.cursors.expire(now);
        let removed = {
            let mut registry = self.registry.lock().unwrap();
            let expired: Vec<_> = registry
                .records
                .iter()
                .filter(|(_, record)| record.expires <= now)
                .map(|(id, _)| *id)
                .collect();
            expired
                .into_iter()
                .filter_map(|id| registry.records.remove(&id))
                .collect::<Vec<_>>()
        };
        drop(removed);
    }
    pub fn lookup(
        &self,
        owner: &str,
        repository: &str,
        handle: &[u8],
    ) -> Result<Arc<Scoped>, Status> {
        let id: [u8; 32] = handle
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid scope handle"))?;
        let registry = self.registry.lock().unwrap();
        let record = registry
            .records
            .get(&id)
            .ok_or_else(|| Status::failed_precondition("query scope unavailable or expired"))?;
        if record.owner != owner || record.repository != repository {
            return Err(Status::permission_denied("query scope access denied"));
        }
        if record.expires <= now()? {
            return Err(Status::failed_precondition("query scope expired"));
        }
        record
            .scope
            .clone()
            .ok_or_else(|| Status::failed_precondition("query scope is closed"))
    }
    /// The registry keeps the identity mode after logical scope closure.
    /// Never retain this registry lock while acquiring the cursor registry.
    pub(crate) fn scope_status(
        &self,
        owner: &str,
        repository: &str,
        handle: &[u8],
    ) -> Result<Option<(u32, bool)>, Status> {
        let id: [u8; 32] = handle
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid scope handle"))?;
        let registry = self.registry.lock().unwrap();
        let Some(record) = registry.records.get(&id) else {
            return Ok(None);
        };
        if record.owner != owner || record.repository != repository {
            return Err(Status::permission_denied("query scope access denied"));
        }
        if record.expires <= now()? {
            return Ok(None);
        }
        Ok(Some((
            record.query_identity_version,
            record.scope.is_none(),
        )))
    }
    pub fn requires_cloud(
        &self,
        owner: &str,
        repository: &str,
        handle: &[u8],
    ) -> Result<bool, Status> {
        let id: [u8; 32] = handle
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid scope handle"))?;
        let registry = self.registry.lock().unwrap();
        let Some(record) = registry.records.get(&id) else {
            return Ok(false);
        };
        if record.owner != owner || record.repository != repository {
            return Err(Status::permission_denied("query scope access denied"));
        }
        Ok(record.cloud_mode)
    }
    pub fn close(&self, owner: &str, repository: &str, handle: &[u8]) -> Result<(), Status> {
        let id: [u8; 32] = handle
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid scope handle"))?;
        let (scope, version) = {
            let mut registry = self.registry.lock().unwrap();
            let Some(record) = registry.records.get_mut(&id) else {
                return Ok(());
            };
            if record.owner != owner || record.repository != repository {
                return Err(Status::permission_denied("query scope access denied"));
            }
            (record.scope.take(), record.query_identity_version)
        };
        // Establish closure before retiring records. Evaluate's creation path
        // locks cursor -> scope; never invert that order while holding scope.
        if version == 2 {
            self.cursors.retire_closed(owner, repository, id);
        }
        drop(scope);
        Ok(())
    }
    pub fn open(
        &self,
        owner: &str,
        repository: &str,
        replay: [u8; 32],
        roots: Vec<m::CommitId>,
    ) -> Result<q::QueryScope, Status> {
        let header = q::ScopeHeader {
            version: 1,
            repository_id: repository.into(),
            replay_key: replay.to_vec(),
            roots: roots
                .iter()
                .map(|root| q::CommitId {
                    value: root.0.to_vec(),
                })
                .collect(),
            overlay_object_count: 0,
            overlay_bytes: 0,
            cloud_source: None,
        };
        let prepared = self.prepare(owner, &header, roots, None, false)?;
        let digest = *prepared.digest().finalize().as_bytes();
        self.install(owner, &header, replay, prepared, Vec::new(), digest)
            .map(|scope| scope.wire.clone())
    }
    fn room(&self, owner: &str, repository: &str, registry: &Registry) -> Result<(), Status> {
        let active = |record: &&Record| record.scope.is_some();
        if registry.records.values().filter(active).count() >= self.config.max_scopes
            || registry
                .records
                .values()
                .filter(active)
                .filter(|record| record.owner == owner)
                .count()
                >= self.config.max_scopes_per_principal
            || registry
                .records
                .values()
                .filter(active)
                .filter(|record| record.repository == repository)
                .count()
                >= self.config.max_scopes_per_repository
        {
            return Err(Status::resource_exhausted(
                "active query scope quota exceeded",
            ));
        }
        if registry.records.len() >= self.config.max_scope_replays
            || registry
                .records
                .values()
                .filter(|record| record.owner == owner)
                .count()
                >= self.config.max_scope_replays_per_principal
            || registry
                .records
                .values()
                .filter(|record| record.repository == repository)
                .count()
                >= self.config.max_scope_replays_per_repository
        {
            return Err(Status::resource_exhausted(
                "query scope replay history quota exceeded",
            ));
        }
        Ok(())
    }
    fn replay(
        &self,
        owner: &str,
        repository: &str,
        replay: [u8; 32],
        binding: [u8; 32],
        digest: Option<[u8; 32]>,
    ) -> Result<Option<Arc<Scoped>>, Status> {
        let registry = self.registry.lock().unwrap();
        let Some(record) = registry.records.values().find(|record| {
            record.owner == owner && record.repository == repository && record.replay == replay
        }) else {
            return Ok(None);
        };
        if record.binding != binding || digest.is_some_and(|digest| record.digest != digest) {
            return Err(Status::already_exists(
                "scope replay key reused with different content",
            ));
        }
        if record.expires <= now()? {
            return Err(Status::failed_precondition("scope replay expired"));
        }
        record
            .scope
            .clone()
            .map(Some)
            .ok_or_else(|| Status::failed_precondition("scope replay is closed"))
    }
    pub fn prepare(
        &self,
        owner: &str,
        header: &q::ScopeHeader,
        roots: Vec<m::CommitId>,
        parent: Option<q::ScopeKey>,
        cloud_authorized: bool,
    ) -> Result<Arc<Prepared>, Status> {
        self.prepare_source(owner, header, roots, parent, None, cloud_authorized)
    }
    pub fn prepare_cloud(
        &self,
        owner: &str,
        header: &q::ScopeHeader,
        cloud_owner: tinytree_store::CloudOwner,
    ) -> Result<Arc<Prepared>, Status> {
        if header.version != 2 || !header.roots.is_empty() {
            return Err(Status::invalid_argument(
                "cloud scope requires identity2 and empty roots",
            ));
        }
        let source = header
            .cloud_source
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("cloud snapshot source missing"))?;
        let selector = source
            .selector
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("cloud version selector missing"))?;
        let authority = source
            .authority
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("cloud native authority missing"))?;
        let (selector, native_binding) = crate::cloud::checked_native_source(
            cloud_owner,
            &header.repository_id,
            selector,
            authority,
        )?;
        if source.index_root_ids.is_empty() || source.index_root_ids.len() > self.config.max_roots {
            return Err(Status::resource_exhausted(
                "cloud selected snapshot count exceeded",
            ));
        }
        let selected = source
            .index_root_ids
            .iter()
            .map(|id| {
                id.as_slice()
                    .try_into()
                    .map(tinytree_cloud::IndexRootId)
                    .map_err(|_| Status::invalid_argument("invalid cloud snapshot root ID"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if selected.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Status::invalid_argument(
                "cloud snapshot roots must be sorted unique",
            ));
        }
        let replay: [u8; 32] = header
            .replay_key
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid scope replay key"))?;
        self.expire(now()?);
        if self
            .replay(
                owner,
                &header.repository_id,
                replay,
                scope_binding(header, None),
                None,
            )?
            .is_some()
        {
            return self.prepare_source(owner, header, Vec::new(), None, None, true);
        }
        let guard = self
            .store
            .open_cloud_snapshot(
                &header.repository_id,
                cloud_owner,
                selector,
                &selected,
                self.config
                    .max_roots
                    .min(self.store.resource_config().max_closure_objects),
            )
            .map_err(|error| match error {
                tinytree_store::Error::Invalid("cloud snapshots require indexed version") => {
                    Status::unimplemented("cloud query scopes require an indexed version")
                }
                error => crate::service::store_status(error),
            })?;
        let charge = guard
            .retained_size_upper_bound()
            .map_err(crate::service::store_status)?;
        let memory =
            self.scope_memory
                .clone()
                .try_acquire_many_owned(u32::try_from(charge).map_err(|_| {
                    Status::resource_exhausted("cloud scope retained memory exceeded")
                })?)
                .map_err(|_| Status::resource_exhausted("cloud scope retained memory exceeded"))?;
        let roots = guard.anchors().to_vec();
        let cloud = Arc::new(CloudScopeRetention {
            guard,
            native_binding,
            _memory: memory,
        });
        self.prepare_source(owner, header, roots, None, Some(cloud), true)
    }
    fn prepare_source(
        &self,
        owner: &str,
        header: &q::ScopeHeader,
        roots: Vec<m::CommitId>,
        parent: Option<q::ScopeKey>,
        cloud: Option<Arc<CloudScopeRetention>>,
        cloud_authorized: bool,
    ) -> Result<Arc<Prepared>, Status> {
        let repository = &header.repository_id;
        if !matches!(header.version, 1 | 2) {
            return Err(Status::invalid_argument("unsupported query scope version"));
        }
        self.store
            .references(repository, &[])
            .map_err(|_| Status::not_found("native repository unavailable"))?;
        let replay: [u8; 32] = header
            .replay_key
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid scope replay key"))?;
        let binding = scope_binding(header, parent.as_ref());
        let now = now()?;
        self.expire(now);
        if let Some(existing) = self.replay(owner, repository, replay, binding, None)? {
            // A known child replay owns its captured source independently of a
            // logically closed parent. Reauthorize that source before returning
            // it; do not look up/reopen the parent or acquire another guard.
            if existing.is_cloud() && !cloud_authorized {
                return Err(Status::permission_denied(
                    "cloud query capability is required",
                ));
            }
            let base = existing
                .parent
                .clone()
                .ok_or_else(|| Status::internal("scope replay anchor missing"))?;
            return Ok(Arc::new(Prepared {
                base,
                memory: None,
                binding,
                replayed: true,
                depth: existing.depth,
                expires_cap: Some(existing.wire.expires_at_unix_seconds),
            }));
        }
        if parent.is_none() && header.cloud_source.is_some() && cloud.is_none() {
            return Err(Status::failed_precondition(
                "cloud source replay no longer available",
            ));
        }
        self.room(owner, repository, &self.registry.lock().unwrap())?;
        let (base, depth, expires_cap) = if let Some(parent) = parent {
            if parent.repository_id != *repository {
                return Err(Status::invalid_argument("fork parent repository differs"));
            }
            let base = self.lookup(owner, repository, &parent.handle)?;
            if base.is_cloud() && !cloud_authorized {
                return Err(Status::permission_denied(
                    "cloud query capability is required",
                ));
            }
            if base.wire.query_identity_version != header.version {
                return Err(Status::invalid_argument(
                    "fork scope identity version mismatch",
                ));
            }
            if base.cloud.is_some() && !roots.is_empty() {
                return Err(Status::unimplemented(
                    "mixed cloud and reference sources are unsupported",
                ));
            }
            if base.cloud.is_none() && base.scope.roots() != roots.as_slice() {
                return Err(Status::invalid_argument(
                    "fork remote roots differ from parent",
                ));
            }
            let depth = base
                .depth
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("overlay layer overflow"))?;
            let expires = base.wire.expires_at_unix_seconds;
            (base, depth, Some(expires))
        } else {
            let mut pins = Vec::new();
            for root in &roots {
                if *root != m::CommitId::ROOT {
                    pins.push((
                        *root,
                        self.store
                            .pin_root(
                                repository,
                                m::ObjectKey {
                                    kind: m::Kind::Commit,
                                    id: m::ObjectId(root.0),
                                },
                            )
                            .map_err(|_| Status::not_found("query root unavailable"))?,
                    ));
                }
            }
            let scope = match self.index.scope(repository, &roots) {
                Ok(scope) => Arc::new(scope),
                Err(tinytree_index::Error::NotReady) => {
                    for (root, pin) in pins {
                        self.schedule(repository, root, pin, cloud.clone())?;
                    }
                    return Err(Status::unavailable(
                        "derived graph warming; retry the same logical roots",
                    ));
                }
                Err(error) => return Err(index_error(error)),
            };
            let charge = roots
                .len()
                .saturating_mul(
                    4 * std::mem::size_of::<tinytree_store::RootReadGuard>()
                        + 4 * std::mem::size_of::<m::CommitId>()
                        + 128,
                )
                .saturating_add((owner.len() + repository.len()).saturating_mul(4))
                .saturating_add(2048);
            let memory = self
                .scope_memory
                .clone()
                .try_acquire_many_owned(
                    u32::try_from(charge)
                        .map_err(|_| Status::resource_exhausted("scope memory exceeded"))?,
                )
                .map_err(|_| Status::resource_exhausted("scope memory exceeded"))?;
            let mut hash = blake3::Hasher::new();
            hash.update(b"tinytree-query-remote-anchor-v1\0");
            hash.update(repository.as_bytes());
            hash.update(&scope.epoch());
            for root in &roots {
                hash.update(&root.0);
            }
            let cloud_source_binding = cloud
                .as_ref()
                .map(|source| {
                    let selected: Vec<_> = source
                        .guard
                        .selected_roots()
                        .iter()
                        .map(|id| id.0)
                        .collect();
                    m::cloud_query_source_binding(
                        source.native_binding,
                        &selected,
                        &roots,
                        scope.epoch(),
                    )
                    .to_vec()
                })
                .unwrap_or_default();
            hash.update(&cloud_source_binding);
            let wire = q::QueryScope {
                cloud_source_binding,
                key: None,
                content_digest: hash.finalize().as_bytes().to_vec(),
                index_epoch: scope.epoch().to_vec(),
                high_water: scope.high_water(),
                expires_at_unix_seconds: now + self.config.scope_ttl_seconds as i64,
                query_identity_version: header.version,
            };
            let mut graph = OverlayGraph::remote(scope.clone());
            if let Some(source) = &cloud {
                if !source.guard.frozen_commits().is_empty() {
                    let limits = OverlayLimits {
                        max_depth: self.config.max_overlay_layers,
                        max_nodes: self.config.max_scope_overlay_nodes,
                        max_bytes: self.config.max_scope_overlay_bytes,
                        max_parents: self.config.max_commit_parents,
                        max_edges: self.config.max_work_edges as usize,
                        max_commit_bytes: self.config.max_commit_bytes,
                    };
                    let charge =
                        OverlayGraph::frozen_memory(&source.guard, &limits).map_err(index_error)?;
                    let permit = self
                        .scope_memory
                        .clone()
                        .try_acquire_many_owned(u32::try_from(charge).map_err(|_| {
                            Status::resource_exhausted("frozen graph memory exceeded")
                        })?)
                        .map_err(|_| Status::resource_exhausted("frozen graph memory exceeded"))?;
                    let owner: Arc<dyn std::any::Any + Send + Sync> = Arc::new(FrozenGraphOwner {
                        _source: source.clone(),
                        _memory: permit,
                    });
                    graph = OverlayGraph::from_retained_baseline(
                        graph,
                        &source.guard,
                        &limits,
                        &mut self.config.budget(),
                        owner,
                    )
                    .map_err(index_error)?;
                }
            }

            (
                Arc::new(Scoped {
                    cloud,
                    scope,
                    graph: Some(graph),
                    wire,
                    parent: None,
                    depth: 0,
                    roots: pins.into_iter().map(|(_, pin)| pin).collect(),
                    memory: Some(Arc::new(memory)),
                    cleanup: self.cleanup_sender()?,
                }),
                usize::from(header.overlay_object_count > 0),
                None,
            )
        };
        if depth > self.config.max_overlay_layers
            || base
                .graph()
                .node_count()
                .saturating_add(header.overlay_object_count as usize)
                > self.config.max_scope_overlay_nodes
            || base
                .graph()
                .byte_count()
                .saturating_add(header.overlay_bytes as usize)
                > self.config.max_scope_overlay_bytes
        {
            return Err(Status::resource_exhausted(
                "logical overlay limits exceeded",
            ));
        }
        let memory = if header.overlay_object_count == 0 {
            None
        } else {
            let charge = (header.overlay_bytes as usize)
                .checked_mul(4)
                .and_then(|bytes| {
                    bytes.checked_add((header.overlay_object_count as usize).checked_mul(512)?)
                })
                .ok_or_else(|| Status::resource_exhausted("overlay memory overflow"))?;
            Some(Arc::new(OverlayMemory {
                _permit: self
                    .overlay_memory
                    .clone()
                    .try_acquire_many_owned(
                        u32::try_from(charge)
                            .map_err(|_| Status::resource_exhausted("overlay memory exceeded"))?,
                    )
                    .map_err(|_| Status::resource_exhausted("overlay memory exceeded"))?,
            }))
        };
        Ok(Arc::new(Prepared {
            base,
            memory,
            binding,
            replayed: false,
            depth,
            expires_cap,
        }))
    }
    pub fn install(
        &self,
        owner: &str,
        header: &q::ScopeHeader,
        replay: [u8; 32],
        prepared: Arc<Prepared>,
        commits: Vec<(m::Descriptor, Arc<[u8]>)>,
        digest: [u8; 32],
    ) -> Result<Arc<Scoped>, Status> {
        if let Some(scope) = self.replay(
            owner,
            &header.repository_id,
            replay,
            prepared.binding,
            Some(digest),
        )? {
            return Ok(scope);
        }
        if prepared.replayed {
            return Err(Status::failed_precondition(
                "scope replay no longer available",
            ));
        }
        let graph = if commits.is_empty() {
            prepared.base.graph().clone()
        } else {
            let memory = prepared
                .memory
                .clone()
                .ok_or_else(|| Status::internal("overlay reservation missing"))?;
            let owner: Arc<dyn std::any::Any + Send + Sync> = Arc::new(ChunkOwner {
                _base: prepared.base.clone(),
                _memory: memory,
            });
            let limits = OverlayLimits {
                max_depth: self.config.max_overlay_layers,
                max_nodes: self.config.max_scope_overlay_nodes,
                max_bytes: self.config.max_scope_overlay_bytes,
                max_parents: self.config.max_commit_parents,
                max_edges: self.config.max_work_edges as usize,
                max_commit_bytes: self.config.max_commit_bytes,
            };
            OverlayGraph::extend(
                prepared.base.graph().clone(),
                commits,
                &limits,
                &mut self.config.budget(),
                owner,
            )
            .map_err(index_error)?
        };
        let now = now()?;
        let expires = (now + self.config.scope_ttl_seconds as i64)
            .min(prepared.expires_cap.unwrap_or(i64::MAX));
        if expires <= now {
            return Err(Status::failed_precondition(
                "fork parent expired during admission",
            ));
        }
        let memory = self
            .scope_memory
            .clone()
            .try_acquire_many_owned(
                (2048usize
                    .saturating_add(owner.len())
                    .saturating_add(header.repository_id.len())) as u32,
            )
            .map_err(|_| Status::resource_exhausted("scope handle memory exceeded"))?;
        let memory = Arc::new(memory);
        let mut registry = self.registry.lock().unwrap();
        if let Some(record) = registry.records.values().find(|record| {
            record.owner == owner
                && record.repository == header.repository_id
                && record.replay == replay
        }) {
            if record.binding != prepared.binding || record.digest != digest {
                return Err(Status::already_exists(
                    "scope replay key reused with different content",
                ));
            }
            if record.expires <= now {
                return Err(Status::failed_precondition("scope replay expired"));
            }
            return record
                .scope
                .clone()
                .ok_or_else(|| Status::failed_precondition("scope replay is closed"));
        }
        self.room(owner, &header.repository_id, &registry)?;
        let mut handle = [0; 32];
        handle[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        handle[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        let wire = q::QueryScope {
            cloud_source_binding: prepared.base.wire.cloud_source_binding.clone(),
            key: Some(q::ScopeKey {
                repository_id: header.repository_id.clone(),
                handle: handle.to_vec(),
            }),
            content_digest: digest.to_vec(),
            index_epoch: prepared.base.scope.epoch().to_vec(),
            high_water: prepared.base.scope.high_water(),
            expires_at_unix_seconds: expires,
            query_identity_version: header.version,
        };
        let scope = Arc::new(Scoped {
            cloud: prepared.base.cloud.clone(),
            scope: prepared.base.scope.clone(),
            graph: Some(graph),
            wire,
            parent: Some(prepared.base.clone()),
            depth: prepared.depth,
            roots: Vec::new(),
            memory: Some(memory.clone()),
            cleanup: self.cleanup_sender()?,
        });
        registry.records.insert(
            handle,
            Record {
                cloud_mode: scope.is_cloud(),
                owner: owner.into(),
                repository: header.repository_id.clone(),
                replay,
                digest,
                binding: prepared.binding,
                expires,
                query_identity_version: header.version,
                scope: Some(scope.clone()),
                _memory: memory,
            },
        );
        Ok(scope)
    }
    fn schedule(
        &self,
        repository: &str,
        root: m::CommitId,
        pin: tinytree_store::RootReadGuard,
        cloud: Option<Arc<CloudScopeRetention>>,
    ) -> Result<(), Status> {
        if self.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("query host draining"));
        }
        if self
            .index
            .node(repository, root)
            .map_err(index_error)?
            .is_some()
        {
            return Ok(());
        }
        let key = (repository.to_owned(), root);
        let mut state = self.warming.lock().unwrap();
        if let Some((failure, when)) = state.failures.get(&key) {
            if when.elapsed() < Duration::from_secs(1) {
                return Err(failure.clone());
            }
        }
        state.failures.remove(&key);
        if state.pending.contains(&key) {
            return Ok(());
        }
        if state.pending.len() >= self.config.max_pending_warms {
            return Err(Status::resource_exhausted("derived warm queue full"));
        }
        state.pending.insert(key.clone());
        if self
            .warm
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| Status::unavailable("query host draining"))?
            .try_send(WarmJob {
                repository: repository.into(),
                root,
                _pin: pin,
                _cloud: cloud,
            })
            .is_err()
        {
            state.pending.remove(&key);
            return Err(Status::resource_exhausted("derived warm queue full"));
        }
        Ok(())
    }
}
// Raw payloads only; canonical decode state never survives a discovery step.
// Each unique visited node contributes at most one FIFO identity, including
// stale entries removed from the map on exit. The node cap bounds both maps.
struct WarmPayloadCache {
    entries: HashMap<m::CommitId, (m::Descriptor, Vec<u8>)>,
    fifo: VecDeque<m::CommitId>,
    allocated: usize,
    limit: usize,
}
impl WarmPayloadCache {
    fn new(limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            fifo: VecDeque::new(),
            allocated: 0,
            limit,
        }
    }
    fn room(&mut self, incoming: usize) -> Result<(), Status> {
        if incoming > self.limit {
            return Err(Status::resource_exhausted(
                "derived warm retained payload limit exceeded",
            ));
        }
        while self.allocated > self.limit - incoming {
            let id = self
                .fifo
                .pop_front()
                .ok_or_else(|| Status::data_loss("derived warm cache accounting mismatch"))?;
            let _ = self.take(id);
        }
        Ok(())
    }
    fn take(&mut self, id: m::CommitId) -> Option<(m::Descriptor, Vec<u8>)> {
        let value = self.entries.remove(&id)?;
        self.allocated -= value.1.capacity();
        Some(value)
    }
    fn insert(
        &mut self,
        id: m::CommitId,
        descriptor: m::Descriptor,
        data: Vec<u8>,
    ) -> Result<(), Status> {
        // Store may return excess capacity despite a fitting logical length.
        // Keep that single read transient, rather than violating residency.
        if data.capacity() > self.limit {
            return Ok(());
        }
        self.room(data.capacity())?;
        self.allocated += data.capacity();
        self.entries.insert(id, (descriptor, data));
        self.fifo.push_back(id);
        Ok(())
    }
}
impl WarmRuntime {
    fn build(&self, job: &WarmJob) -> Result<(), Status> {
        if self.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("query host draining"));
        }
        let deadline =
            std::time::Instant::now() + Duration::from_secs(self.config.warm_timeout_seconds);
        let mut stack = vec![(job.root, false)];
        let mut active = HashSet::new();
        let mut done = HashSet::new();
        let mut read_bytes = 0usize;
        let mut payloads = WarmPayloadCache::new(self.config.max_warm_bytes);
        let mut edges = 0usize;
        while let Some((id, exit)) = stack.pop() {
            if self.closing.load(Ordering::Acquire) {
                return Err(Status::unavailable("query host draining"));
            }
            if std::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded("derived warm work expired"));
            }
            if id == m::CommitId::ROOT || done.contains(&id) {
                continue;
            }
            if exit {
                // Reuse a retained discovery payload; evicted ancestors are
                // reread only after their parents have become index-ready.
                let (descriptor, data) = match payloads.take(id) {
                    Some(value) => value,
                    None => self.read_commit(job, id, &mut read_bytes, &mut payloads)?,
                };
                self.index
                    .ingest(&job.repository, descriptor, &data)
                    .map_err(|error| match error {
                        tinytree_index::Error::Invalid(reason)
                            if reason.contains("budget")
                                || reason.contains("size")
                                || reason.contains("count") =>
                        {
                            Status::resource_exhausted("derived index capacity exceeded")
                        }
                        error => index_error(error),
                    })?;
                active.remove(&id);
                done.insert(id);
                continue;
            }
            if active.len().saturating_add(done.len()).saturating_add(1)
                > self.config.max_warm_nodes
            {
                return Err(Status::resource_exhausted(
                    "derived warm node budget exceeded",
                ));
            }
            if self
                .index
                .node(&job.repository, id)
                .map_err(index_error)?
                .is_some()
            {
                done.insert(id);
                continue;
            }
            if !active.insert(id) {
                return Err(Status::data_loss("canonical graph cycle"));
            }
            if active.len().saturating_add(done.len()) > self.config.max_warm_nodes {
                return Err(Status::resource_exhausted(
                    "derived warm node budget exceeded",
                ));
            }
            let (descriptor, data) = self.read_commit(job, id, &mut read_bytes, &mut payloads)?;
            let object = m::decode(
                &data,
                m::Limits {
                    max_object_bytes: self.config.max_commit_bytes,
                    max_items: self.config.max_commit_parents,
                    ..Default::default()
                },
            )
            .map_err(|error| match error {
                m::Error::Limit => {
                    Status::resource_exhausted("derived commit shape exceeds configured limits")
                }
                _ => Status::data_loss("invalid canonical graph metadata"),
            })?;
            let m::Object::Commit(commit) = object else {
                return Err(Status::data_loss("canonical graph type mismatch"));
            };
            edges = edges
                .checked_add(commit.parents.len())
                .ok_or_else(|| Status::resource_exhausted("derived warm edge overflow"))?;
            if edges > self.config.max_warm_edges
                || stack.len().saturating_add(commit.parents.len() + 1)
                    > self.config.max_warm_frontier
            {
                return Err(Status::resource_exhausted(
                    "derived warm frontier budget exceeded",
                ));
            }
            // A bounded FIFO cache avoids rereads for fitting histories, but
            // never makes a long ancestor path depend on full residency.
            payloads.insert(id, descriptor, data)?;
            stack.push((id, true));
            for parent in commit.parents.into_iter().rev() {
                stack.push((parent, false));
            }
        }
        Ok(())
    }
    fn read_commit(
        &self,
        job: &WarmJob,
        id: m::CommitId,
        read_bytes: &mut usize,
        payloads: &mut WarmPayloadCache,
    ) -> Result<(m::Descriptor, Vec<u8>), Status> {
        let key = m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(id.0),
        };
        let expected = self
            .store
            .descriptor(&job.repository, key)
            .map_err(|_| Status::unavailable("canonical graph unavailable"))?
            .ok_or_else(|| Status::not_found("canonical graph incomplete"))?;
        let size = usize::try_from(expected.size_bytes)
            .map_err(|_| Status::resource_exhausted("derived commit byte size overflow"))?;
        if size > self.config.max_commit_bytes || size > self.config.max_warm_bytes {
            return Err(Status::resource_exhausted(
                "derived warm retained payload limit exceeded",
            ));
        }
        let total = read_bytes
            .checked_add(size)
            .ok_or_else(|| Status::resource_exhausted("derived warm read byte overflow"))?;
        if total > self.config.max_warm_read_bytes {
            return Err(Status::resource_exhausted(
                "derived warm total read budget exceeded",
            ));
        }
        // Evict before the physical read; only the incoming object's excess
        // Vec capacity may use the separately reserved transient scratch.
        payloads.room(size)?;
        // Account actual discovery and cache-miss reads before allocation.
        *read_bytes = total;
        let (descriptor, data) = self
            .store
            .read_metadata(&job.repository, key)
            .map_err(|_| Status::data_loss("canonical graph read failed"))?;
        if descriptor != expected || data.len() != size {
            return Err(Status::data_loss(
                "canonical graph descriptor changed during read",
            ));
        }
        Ok((descriptor, data))
    }
}
