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
#[serde(deny_unknown_fields)]
pub struct ProtectedReference {
    pub repository: String,
    pub reference: String,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServiceConfig {
    /// Exact repository/reference pairs writable only through checked landing.
    pub protected_references: Vec<ProtectedReference>,
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
    pub query: crate::QueryConfig,
    pub cloud: crate::CloudConfig,
    pub landing: crate::LandingConfig,
}
impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            protected_references: Vec::new(),
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
            query: Default::default(),
            cloud: Default::default(),
            landing: Default::default(),
        }
    }
}
impl ServiceConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.protected_references.len() > 1024 {
            return Err(ConfigError("too many protected native references"));
        }
        let mut protected = HashSet::new();
        for entry in &self.protected_references {
            // Native references are database values, not Git or filesystem paths.
            if tinytree_store::validate_repository_id(&entry.repository).is_err()
                || entry.reference.is_empty()
                || entry.reference.len() > 512
                || entry.reference.contains('\0')
                || entry.reference == "_tinytree"
                || entry.reference.starts_with("_tinytree/")
                || !protected.insert((&entry.repository, &entry.reference))
            {
                return Err(ConfigError(
                    "invalid or duplicate protected native reference",
                ));
            }
        }
        self.query.validate(self.max_request_bytes)?;
        self.cloud.validate()?;
        self.landing.validate()?;
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
    pub(crate) fn validate_repositories(&self, repositories: &[String]) -> Result<(), ConfigError> {
        self.landing.validate_repositories(repositories)?;
        if self
            .protected_references
            .iter()
            .any(|entry| !repositories.contains(&entry.repository))
        {
            return Err(ConfigError("unknown protected native reference repository"));
        }
        Ok(())
    }
    pub(crate) fn reference_protected(&self, repository: &str, reference: &str) -> bool {
        self.protected_references
            .iter()
            .any(|entry| entry.repository == repository && entry.reference == reference)
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
pub struct CloudStoreConfig {
    pub resident: Option<tinytree_store::CloudResidentLimits>,
    pub max_object_bytes: usize,
    pub max_decode_bytes: usize,
    pub max_version_decode_bytes: usize,
    pub max_manifest_bytes: usize,
    pub max_manifest_decode_bytes: usize,
    pub max_manifest_members: usize,
}
impl Default for CloudStoreConfig {
    fn default() -> Self {
        let c = tinytree_store::CloudConfig::default();
        Self {
            resident: c.resident,
            max_object_bytes: c.max_object_bytes,
            max_decode_bytes: c.max_decode_bytes,
            max_version_decode_bytes: c.max_version_decode_bytes,
            max_manifest_bytes: c.manifest_limits.max_manifest_bytes,
            max_manifest_decode_bytes: c.manifest_limits.max_decode_bytes,
            max_manifest_members: c.manifest_limits.max_members,
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StoreConfig {
    pub cloud: CloudStoreConfig,
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
            cloud: Default::default(),
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
        config.cloud.resident = self.cloud.resident;
        config.cloud.max_object_bytes = self.cloud.max_object_bytes;
        config.cloud.max_field_bytes = config
            .cloud
            .max_field_bytes
            .min(self.cloud.max_object_bytes);
        config.cloud.max_decode_bytes = self.cloud.max_decode_bytes;
        config.cloud.max_version_decode_bytes = self.cloud.max_version_decode_bytes;
        config.cloud.manifest_limits.max_manifest_bytes = self.cloud.max_manifest_bytes;
        config.cloud.manifest_limits.max_decode_bytes = self.cloud.max_manifest_decode_bytes;
        config.cloud.manifest_limits.max_members = self.cloud.max_manifest_members;
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
        if let Some(limits) = self.cloud.resident {
            limits
                .validate()
                .map_err(|_| ConfigError("invalid native store resident limits"))?;
        }
        if !(1024..=64 * 1024 * 1024).contains(&self.cloud.max_object_bytes)
            || self.cloud.max_decode_bytes < self.cloud.max_object_bytes
            || self.cloud.max_decode_bytes > 1024 * 1024 * 1024
            || !(65536..=1024 * 1024 * 1024).contains(&self.cloud.max_version_decode_bytes)
            || !(1024..=64 * 1024 * 1024).contains(&self.cloud.max_manifest_bytes)
            || self.cloud.max_manifest_decode_bytes < self.cloud.max_manifest_bytes
            || self.cloud.max_manifest_decode_bytes > 1024 * 1024 * 1024
            || !(1..=65536).contains(&self.cloud.max_manifest_members)
            || self.max_file_bytes == 0
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
    #[serde(default)]
    pub cloud: bool,
    #[serde(default)]
    pub landing_profiles: Vec<String>,
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
        if self.service.cloud.enabled {
            self.service
                .cloud
                .validate_store_profile(&self.store.materialize(self.service.max_metadata_bytes))?;
        }
        self.service.validate_repositories(&self.repositories)?;
        self.service.landing.validate_store_with_checker_history(
            &self.store.materialize(self.service.max_metadata_bytes),
        )?;
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
            self.service
                .landing
                .validate_grants(&principal.landing_profiles)?;
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
