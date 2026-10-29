use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};
use std::{io::Read, path::Path};
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClosureLimits {
    pub max_objects: usize,
    /// Per-phase edge-work bound: canonical graph discovery and the entire
    /// path-context validation pass each get this allowance (at most twice total).
    pub max_edges: usize,
    pub max_metadata_bytes: usize,
    pub max_path_contexts: usize,
    pub max_path_context_bytes: usize,
    pub max_path_bytes: usize,
}
impl Default for ClosureLimits {
    fn default() -> Self {
        Self {
            max_objects: 5_000,
            max_edges: 50_000,
            max_metadata_bytes: 16 * 1024 * 1024,
            max_path_contexts: 5_000,
            max_path_context_bytes: 2 * 1024 * 1024,
            max_path_bytes: 65536,
        }
    }
}
/// Bounds for daemon-owned query sessions and durable membership witnesses.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueryLimits {
    pub max_scopes: usize,
    pub max_queries: usize,
    pub max_replays: usize,
    pub max_registry_bytes: usize,
    pub max_authorities: usize,
    pub max_grants: usize,
    pub expiry_interval_ms: u64,
}
impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            max_scopes: 32,
            max_queries: 64,
            // Completed command scopes remain replayable until their expiry.
            max_replays: 4096,
            max_registry_bytes: 64 * 1024 * 1024,
            max_authorities: 256,
            max_grants: 50_000,
            expiry_interval_ms: 1000,
        }
    }
}
impl QueryLimits {
    pub(crate) fn registry_overhead(&self) -> usize {
        self.max_scopes
            .saturating_mul(128)
            .saturating_add(self.max_queries.saturating_mul(1024))
            .saturating_add(self.max_replays.saturating_mul(512))
            .saturating_mul(4)
    }
}
/// Exact immutable work already retained by the trusted server check job.
/// This grants local read admission only, never a client-owned retention hold.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServerRetainedCandidate {
    pub job_id: String,
    pub head: String,
    pub root_tree: String,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    #[serde(default = "remote_default", deserialize_with = "deserialize_remote")]
    pub remote: tinytree_client::Config,
    /// Trusted broker-selected effective authorization profile, not an RPC field.
    pub authorization_profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_retained_candidate: Option<ServerRetainedCandidate>,
    pub closure: ClosureLimits,
    pub query: QueryLimits,
    pub operation_timeout_ms: u64,
    pub max_ledger_bytes: usize,
    pub max_pending_operations: usize,
    pub max_admitted_roots: usize,
    pub max_file_index_entries: usize,
    pub max_upload_spool_bytes: u32,
    pub max_staging_files: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            remote: remote_default(),
            authorization_profile: "default".into(),
            server_retained_candidate: None,
            closure: Default::default(),
            query: Default::default(),
            operation_timeout_ms: 120_000,
            max_ledger_bytes: 32 * 1024 * 1024,
            max_pending_operations: 128,
            max_admitted_roots: 256,
            max_file_index_entries: 50_000,
            max_upload_spool_bytes: 128 * 1024 * 1024,
            max_staging_files: 4096,
        }
    }
}
fn remote_default() -> tinytree_client::Config {
    let mut config = tinytree_client::Config::default();
    config.limits.max_metadata_bytes = 4 * 1024 * 1024;
    config.limits.max_batch_bytes = 4 * 1024 * 1024;
    config.limits.max_graph_bytes = 8 * 1024 * 1024;
    // Leave retained-page headroom within the provider's smaller SDK pool.
    config.limits.max_graph_objects = 65_536;
    config.limits.max_buffered_bytes = 32 * 1024 * 1024;
    config.limits.max_file_bytes = 64 * 1024 * 1024;
    config.limits.max_temporary_bytes = 128 * 1024 * 1024;
    config
}
fn deserialize_remote<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<tinytree_client::Config, D::Error> {
    let patch = toml::Value::deserialize(deserializer)?;
    let mut defaults = toml::Value::try_from(remote_default())
        .map_err(|_| serde::de::Error::custom("invalid native remote defaults"))?;
    fn merge(defaults: &mut toml::Value, patch: toml::Value) {
        match (defaults, patch) {
            (toml::Value::Table(defaults), toml::Value::Table(patch)) => {
                for (key, value) in patch {
                    if let Some(default) = defaults.get_mut(&key) {
                        merge(default, value);
                    } else {
                        defaults.insert(key, value);
                    }
                }
            }
            (defaults, patch) => *defaults = patch,
        }
    }
    merge(&mut defaults, patch);
    defaults
        .try_into()
        .map_err(|_| serde::de::Error::custom("invalid native remote client configuration"))
}
impl Config {
    /// Conservative whole-control work, including decoded native objects,
    /// returned client metadata, growable containers, catalog/transaction copies
    /// and the two simultaneous file snapshots. Permanent object bytes are not
    /// transient request memory. Registration must reject a smaller shared pool.
    pub fn control_work_budget(&self) -> native_broker::ControlResources {
        use std::mem::size_of;
        use tinytree_model::{Descriptor, ObjectId, ObjectKey, TreeId};
        let nodes = self.closure.max_objects as u64;
        let edges = self.closure.max_edges as u64;
        let contexts = self.closure.max_path_contexts as u64;
        let node_unit = (4 * size_of::<(ObjectKey, crate::closure::Node)>()
            + 10 * size_of::<ObjectKey>()
            + 2 * size_of::<Descriptor>()) as u64;
        let edge_unit = (4 * size_of::<(ObjectKey, bool)>()
            + 6 * size_of::<(ObjectId, u64, ObjectId)>()
            + 4 * size_of::<ObjectKey>()
            + 2 * size_of::<Descriptor>()) as u64;
        let context_unit = (8 * size_of::<(TreeId, String)>()) as u64;
        let index_unit = (4 * size_of::<Vec<u8>>() + 64) as u64;
        let memory = (self.remote.limits.max_buffered_bytes as u64)
            .saturating_add((self.closure.max_metadata_bytes as u64).saturating_mul(8))
            .saturating_add((self.remote.limits.max_metadata_bytes as u64).saturating_mul(8))
            .saturating_add(nodes.saturating_mul(node_unit))
            .saturating_add(edges.saturating_mul(edge_unit))
            .saturating_add(contexts.saturating_mul(context_unit))
            .saturating_add((self.closure.max_path_context_bytes as u64).saturating_mul(2))
            .saturating_add((self.max_ledger_bytes as u64).saturating_mul(4))
            .saturating_add((self.max_file_index_entries as u64).saturating_mul(index_unit))
            .saturating_add(
                (self.max_admitted_roots as u64)
                    .saturating_add((self.max_pending_operations as u64).saturating_mul(2))
                    .saturating_mul(16 * 1024),
            )
            .saturating_add(8 * 1024 * 1024);
        native_broker::ControlResources {
            memory_bytes: memory,
            temporary_bytes: self
                .remote
                .limits
                .max_file_bytes
                .saturating_mul(2)
                .max(self.remote.limits.max_metadata_bytes as u64),
        }
    }
    pub fn load(path: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|_| anyhow::anyhow!("cannot open remote provider configuration"))?
            .take(65537)
            .read_to_end(&mut bytes)
            .map_err(|_| anyhow::anyhow!("cannot read remote provider configuration"))?;
        ensure!(
            bytes.len() <= 65536,
            "remote provider configuration exceeds limit"
        );
        let config: Self = toml::from_str(
            std::str::from_utf8(&bytes)
                .map_err(|_| anyhow::anyhow!("invalid remote provider configuration"))?,
        )
        .map_err(|_| anyhow::anyhow!("invalid remote provider configuration"))?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        if let Some(candidate) = &self.server_retained_candidate {
            for id in [&candidate.job_id, &candidate.head, &candidate.root_tree] {
                let decoded = crate::unhex::<32>(id)?;
                ensure!(
                    crate::hex(&decoded) == *id && decoded != [0; 32],
                    "invalid server-retained candidate binding"
                );
            }
        }

        self.remote
            .validate()
            .map_err(|_| anyhow::anyhow!("invalid native remote client configuration"))?;
        let c = &self.closure;
        if self.authorization_profile.is_empty()
            || self.authorization_profile.len() > 128
            || !(1..=4096).contains(&self.query.max_scopes)
            || !(1..=4096).contains(&self.query.max_queries)
            || !(1..=16384).contains(&self.query.max_replays)
            || !(65536..=1024 * 1024 * 1024).contains(&self.query.max_registry_bytes)
            || !(1..=100_000).contains(&self.query.max_authorities)
            || !(1..=10_000_000).contains(&self.query.max_grants)
            || !(10..=60_000).contains(&self.query.expiry_interval_ms)
            || self.query.registry_overhead() > self.query.max_registry_bytes
            || !(1..=86_400_000).contains(&self.operation_timeout_ms)
            || !(1..=1_000_000).contains(&c.max_objects)
            || !(1..=10_000_000).contains(&c.max_edges)
            || !(1024..=512 * 1024 * 1024).contains(&c.max_metadata_bytes)
            || !(1..=1_000_000).contains(&c.max_path_contexts)
            || !(1024..=512 * 1024 * 1024).contains(&c.max_path_context_bytes)
            || !(1..=65536).contains(&c.max_path_bytes)
            || !(1024..=1024 * 1024 * 1024).contains(&self.max_ledger_bytes)
            || !(1..=100_000).contains(&self.max_pending_operations)
            || !(1..=100_000).contains(&self.max_admitted_roots)
            || !(1..=10_000_000).contains(&self.max_file_index_entries)
            || !(1..=100_000).contains(&self.max_staging_files)
            || self.max_upload_spool_bytes > 2 * 1024 * 1024 * 1024
            || u64::from(self.max_upload_spool_bytes)
                < self.remote.limits.max_file_bytes.saturating_mul(2)
        {
            bail!("invalid native remote provider bounds");
        }
        Ok(())
    }
}
