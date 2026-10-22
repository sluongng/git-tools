use serde::Deserialize;
use std::{
    collections::HashSet,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct ConfigError(pub(crate) &'static str);
impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ConfigError {}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServiceConfig {
    pub max_batch_items: usize,
    pub max_frame_bytes: usize,
    pub max_request_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_graph_bytes: usize,
    pub max_graph_objects: usize,
    pub max_inflight: usize,
    pub max_inflight_per_repository: usize,
    pub max_buffered_bytes: usize,
    pub rpc: reapi::policy::Policy,
    pub retention: tinytree_store::RetentionLimits,
}
impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            max_batch_items: 256,
            max_frame_bytes: 64 * 1024,
            max_request_bytes: 256 * 1024,
            max_metadata_bytes: 16 * 1024 * 1024,
            max_graph_bytes: 16 * 1024 * 1024,
            max_graph_objects: 100_000,
            max_inflight: 16,
            max_inflight_per_repository: 4,
            max_buffered_bytes: 128 * 1024 * 1024,
            rpc: Default::default(),
            retention: Default::default(),
        }
    }
}
impl ServiceConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.retention
            .validate()
            .map_err(|_| ConfigError("invalid native retention limits"))?;
        self.rpc
            .validate()
            .map_err(|_| ConfigError("invalid native RPC policy"))?;
        if !(1..=1024).contains(&self.max_batch_items)
            || !(1024..=1024 * 1024).contains(&self.max_frame_bytes)
            || !(1024..=16 * 1024 * 1024).contains(&self.max_request_bytes)
            || !(1024..=64 * 1024 * 1024).contains(&self.max_metadata_bytes)
            || !(1024..=256 * 1024 * 1024).contains(&self.max_graph_bytes)
            || !(1..=1_000_000).contains(&self.max_graph_objects)
            || !(1..=1024).contains(&self.max_inflight)
            || self.max_inflight_per_repository == 0
            || self.max_inflight_per_repository > self.max_inflight
            || self.max_buffered_bytes > 1024 * 1024 * 1024
            || self.max_buffered_bytes
                < self
                    .max_graph_bytes
                    .max(self.max_metadata_bytes)
                    .saturating_mul(4)
            || self.max_buffered_bytes < self.max_frame_bytes.saturating_mul(4).saturating_add(1024)
        {
            return Err(ConfigError("invalid native service limits"));
        }
        Ok(())
    }
    pub(crate) fn decode_limit(&self) -> usize {
        self.max_request_bytes.max(self.max_frame_bytes + 1024)
    }
    pub(crate) fn encode_limit(&self) -> usize {
        self.max_metadata_bytes + 4096
    }
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StoreConfig {
    pub max_file_bytes: u64,
    pub max_repository_bytes: u64,
    pub max_objects_per_repository: u64,
    pub max_closure_objects: usize,
    pub max_closure_edges: usize,
    pub max_path_contexts: usize,
    pub max_path_context_bytes: usize,
    pub max_retention_validation_records: usize,
    pub max_retention_validation_bytes: u64,
    pub max_staging_bytes: u64,
    pub max_staging_uploads: usize,
    pub staging_ttl_seconds: i64,
}
impl Default for StoreConfig {
    fn default() -> Self {
        let c = tinytree_store::Config::default();
        Self {
            max_file_bytes: c.max_file_bytes,
            max_repository_bytes: c.max_repository_bytes,
            max_objects_per_repository: c.max_objects_per_repository,
            max_closure_objects: c.max_closure_objects,
            max_closure_edges: c.max_closure_edges,
            max_path_contexts: c.max_path_contexts,
            max_path_context_bytes: c.max_path_context_bytes,
            max_retention_validation_records: c.max_retention_validation_records,
            max_retention_validation_bytes: c.max_retention_validation_bytes,
            max_staging_bytes: c.max_staging_bytes,
            max_staging_uploads: c.max_staging_uploads,
            staging_ttl_seconds: c.staging_ttl_seconds,
        }
    }
}
impl StoreConfig {
    pub fn materialize(&self, metadata_bytes: usize) -> tinytree_store::Config {
        let mut config = tinytree_store::Config::default();
        config.model_limits.max_object_bytes = metadata_bytes;
        config.max_file_bytes = self.max_file_bytes;
        config.max_repository_bytes = self.max_repository_bytes;
        config.max_objects_per_repository = self.max_objects_per_repository;
        config.max_closure_objects = self.max_closure_objects;
        config.max_closure_edges = self.max_closure_edges;
        config.max_path_contexts = self.max_path_contexts;
        config.max_path_context_bytes = self.max_path_context_bytes;
        config.max_retention_validation_records = self.max_retention_validation_records;
        config.max_retention_validation_bytes = self.max_retention_validation_bytes;
        config.max_staging_bytes = self.max_staging_bytes;
        config.max_staging_uploads = self.max_staging_uploads;
        config.staging_ttl_seconds = self.staging_ttl_seconds;
        config
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_file_bytes == 0
            || self.max_repository_bytes == 0
            || self.max_objects_per_repository == 0
            || !(1..=1_000_000).contains(&self.max_closure_objects)
            || !(1..=10_000_000).contains(&self.max_closure_edges)
            || !(1..=1_000_000).contains(&self.max_path_contexts)
            || !(1..=1024 * 1024 * 1024).contains(&self.max_path_context_bytes)
            || !(1..=10_000_000).contains(&self.max_retention_validation_records)
            || !(1..=16 * 1024 * 1024 * 1024).contains(&self.max_retention_validation_bytes)
            || self.max_staging_bytes == 0
            || !(1..=1024).contains(&self.max_staging_uploads)
            || !(1..=30 * 86400).contains(&self.staging_ttl_seconds)
        {
            return Err(ConfigError("invalid native store limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalConfig {
    pub id: String,
    pub token_env: String,
    pub repositories: Vec<String>,
    #[serde(default)]
    pub write: bool,
    #[serde(default)]
    pub retain: bool,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub storage: PathBuf,
    pub listen: String,
    pub repositories: Vec<String>,
    pub principals: Vec<PrincipalConfig>,
    pub tls: Option<TlsConfig>,
    pub gc_interval_seconds: u64,
    pub service: ServiceConfig,
    pub store: StoreConfig,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            storage: "tinytree-data".into(),
            listen: "127.0.0.1:8093".into(),
            repositories: vec!["default".into()],
            principals: vec![],
            tls: None,
            gc_interval_seconds: 300,
            service: Default::default(),
            store: Default::default(),
        }
    }
}
impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let file = std::fs::File::open(path)
            .map_err(|_| ConfigError("cannot open native server configuration"))?;
        let mut bytes = Vec::new();
        file.take(65537)
            .read_to_end(&mut bytes)
            .map_err(|_| ConfigError("cannot read native server configuration"))?;
        if bytes.len() > 65536 {
            return Err(ConfigError("native server configuration exceeds 64 KiB"));
        }
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| ConfigError("native server configuration is not UTF-8"))?;
        let config: Self = toml::from_str(text)
            .map_err(|_| ConfigError("invalid native server TOML configuration"))?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.service.validate()?;
        self.store.validate()?;
        if self.repositories.is_empty()
            || self.repositories.len() > 1024
            || self.gc_interval_seconds > 86400
        {
            return Err(ConfigError("invalid repository or GC configuration"));
        }
        let repos: HashSet<_> = self.repositories.iter().collect();
        if repos.len() != self.repositories.len()
            || self
                .repositories
                .iter()
                .any(|r| tinytree_store::validate_repository_id(r).is_err())
        {
            return Err(ConfigError("invalid native repository identifiers"));
        }
        let mut ids = HashSet::new();
        for principal in &self.principals {
            if principal.id.is_empty()
                || principal.id.len() > 128
                || !ids.insert(&principal.id)
                || principal.token_env.is_empty()
                || principal.token_env.len() > 128
                || !principal
                    .token_env
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                || principal.token_env.as_bytes()[0].is_ascii_digit()
                || principal.repositories.is_empty()
                || principal.repositories.iter().any(|r| !repos.contains(r))
            {
                return Err(ConfigError("invalid native authorization configuration"));
            }
        }
        if self.listen.starts_with("unix://") {
            if self.listen.len() == 7 || self.tls.is_some() {
                return Err(ConfigError("invalid native Unix listener configuration"));
            }
            #[cfg(not(unix))]
            return Err(ConfigError(
                "Unix listeners are unavailable on this platform",
            ));
        } else {
            let address: SocketAddr = self
                .listen
                .parse()
                .map_err(|_| ConfigError("native TCP listener must be a socket address"))?;
            if !address.ip().is_loopback() && (self.tls.is_none() || self.principals.is_empty()) {
                return Err(ConfigError(
                    "non-loopback listeners require TLS and authorization",
                ));
            }
        }
        Ok(())
    }
}
