use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub state_dir: PathBuf,
    pub runtime: Runtime,
    pub cache: Cache,
    pub bes: Bes,
    pub bos: Bos,
    pub git_proxy: GitProxy,
    pub session: Session,
    pub source: Source,
    pub native: Native,
    pub reapi: reapi::policy::Policy,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Runtime {
    /// Raise only the daemon's soft descriptor limit, capped by its inherited
    /// hard limit. Zero preserves the inherited soft limit.
    pub target_open_files: u64,
}
impl Default for Runtime {
    fn default() -> Self {
        Self {
            target_open_files: 16_384,
        }
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Source {
    /// None uses the content scheduler's default; Some(0) disables disk spooling.
    pub large_blob_spool_bytes: Option<u64>,
}
/// Optional authoritative native repositories served on the existing rbe.sock.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Native {
    pub repositories: std::collections::BTreeMap<String, NativeRepository>,
    pub workspaces: NativeWorkspaces,
    pub max_inflight: usize,
    pub max_memory_bytes: usize,
    pub max_query_bytes: usize,
    pub max_query_persistent_bytes: u64,
    pub max_temporary_bytes: u64,
}
impl Default for Native {
    fn default() -> Self {
        let defaults = native_broker::Config::default();
        Self {
            repositories: Default::default(),
            workspaces: NativeWorkspaces::default(),
            max_inflight: defaults.max_inflight,
            max_memory_bytes: defaults.max_memory_bytes,
            max_query_bytes: defaults.max_query_bytes,
            max_query_persistent_bytes: defaults.max_query_persistent_bytes,
            max_temporary_bytes: defaults.max_temporary_bytes,
        }
    }
}
/// Rollout policy for newly created native indexes, not provider identity.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeIndexMode {
    #[default]
    Legacy,
    Federated,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NativeWorkspaces {
    /// Experimental opt-in; existing index/type always wins on reload.
    pub index_mode: NativeIndexMode,
    pub capture: NativeCapture,
    pub upload: NativeCaptureUpload,
    pub gc: NativeGc,
    /// None uses the per-user registry shared with the native jj client.
    pub registry_path: Option<PathBuf>,
    pub max_workspaces: usize,
    pub max_entries: usize,
    pub max_metadata_bytes: usize,
    pub max_file_bytes: u64,
    pub max_snapshot_bytes: u64,
}
impl Default for NativeWorkspaces {
    fn default() -> Self {
        Self {
            index_mode: NativeIndexMode::Legacy,
            capture: NativeCapture::default(),
            upload: NativeCaptureUpload::default(),
            gc: NativeGc::default(),
            registry_path: None,
            max_workspaces: 128,
            max_entries: 100_000,
            max_metadata_bytes: 64 * 1024 * 1024,
            max_file_bytes: 256 * 1024 * 1024,
            max_snapshot_bytes: 1024 * 1024 * 1024,
        }
    }
}
impl NativeWorkspaces {
    fn validate(&self) -> Result<()> {
        self.capture.validate()?;
        self.upload.validate()?;
        self.gc.validate()?;
        if self.gc.policy == NativeGcPolicy::LocalComplete {
            ensure!(
                self.index_mode == NativeIndexMode::Legacy,
                "local-complete collection requires legacy local index mode"
            );
            ensure!(
                self.capture.policy == NativeCapturePolicy::Disabled
                    && self.upload.policy == NativeCaptureUploadPolicy::Disabled,
                "local-complete collection cannot combine with capture or upload policy"
            );
        }
        ensure!(
            (1..=4096).contains(&self.max_workspaces)
                && (1..=1_000_000).contains(&self.max_entries)
                && (4096..=1024 * 1024 * 1024).contains(&self.max_metadata_bytes)
                && (1..=16 * 1024 * 1024 * 1024).contains(&self.max_file_bytes)
                && self.max_snapshot_bytes >= self.max_file_bytes
                && self.max_snapshot_bytes <= 64 * 1024 * 1024 * 1024,
            "invalid native workspace limits"
        );
        if let Some(path) = &self.registry_path {
            ensure!(
                path.is_absolute() && path.as_os_str().len() <= 4096,
                "native workspace registry must be a bounded absolute path"
            );
        }
        Ok(())
    }
}

/// Explicit unreachable collection for a newly managed, fully local cohort.
/// This never permits reachable offload or retirement of operation history.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeGcPolicy {
    #[default]
    Disabled,
    LocalComplete,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NativeGc {
    pub policy: NativeGcPolicy,
    pub grace_seconds: u64,
    pub timeout_ms: u64,
    pub max_operations: usize,
    pub max_checkpoint_bytes: usize,
    pub max_roots: usize,
    pub max_catalog_records: usize,
    pub max_marked_objects: usize,
    pub max_metadata_bytes: u64,
    pub max_deleted_objects: usize,
    pub max_deleted_bytes: u64,
}
impl Default for NativeGc {
    fn default() -> Self {
        let limits = tinytree_store::ClientGcLimits::default();
        Self {
            policy: NativeGcPolicy::Disabled,
            grace_seconds: 3600,
            timeout_ms: 30_000,
            max_operations: 4096,
            max_checkpoint_bytes: 1024 * 1024 * 1024,
            max_roots: limits.max_roots,
            max_catalog_records: limits.max_catalog_records,
            max_marked_objects: limits.max_marked_objects,
            max_metadata_bytes: limits.max_metadata_bytes,
            max_deleted_objects: limits.max_deleted_objects,
            max_deleted_bytes: limits.max_deleted_bytes,
        }
    }
}
impl NativeGc {
    pub(crate) fn limits(&self) -> tinytree_store::ClientGcLimits {
        tinytree_store::ClientGcLimits {
            max_roots: self.max_roots,
            max_catalog_records: self.max_catalog_records,
            max_marked_objects: self.max_marked_objects,
            max_metadata_bytes: self.max_metadata_bytes,
            max_deleted_objects: self.max_deleted_objects,
            max_deleted_bytes: self.max_deleted_bytes,
            ..Default::default()
        }
    }
    fn validate(&self) -> Result<()> {
        let limits = self.limits();
        let caps = tinytree_store::ClientGcLimits::default();
        // OP/view decoding and the legacy reader reserve five decode windows,
        // in addition to bounded operation, edge and root bookkeeping.
        ensure!(
            (512 * 1024 * 1024..=2 * 1024 * 1024 * 1024).contains(&self.max_checkpoint_bytes),
            "native checkpoint byte budget must be between 512 MiB and 2 GiB"
        );
        ensure!(
            self.grace_seconds <= 30 * 24 * 3600
                && (1000..=120_000).contains(&self.timeout_ms)
                && (1..=65_537).contains(&self.max_operations)
                && (1..=caps.max_roots).contains(&limits.max_roots)
                && (1..=caps.max_catalog_records).contains(&limits.max_catalog_records)
                && (1..=caps.max_marked_objects).contains(&limits.max_marked_objects)
                && (4096..=caps.max_metadata_bytes).contains(&limits.max_metadata_bytes)
                && (1..=caps.max_deleted_objects).contains(&limits.max_deleted_objects)
                && (1..=caps.max_deleted_bytes).contains(&limits.max_deleted_bytes),
            "invalid native collection bounds"
        );
        Ok(())
    }
}

/// Background publication retains every local pending ticket, root and pack.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeCaptureUploadPolicy {
    #[default]
    Disabled,
    PublishRetainingRoots,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NativeCaptureUpload {
    pub policy: NativeCaptureUploadPolicy,
    pub poll_ms: u64,
    pub timeout_ms: u64,
    pub max_jobs: usize,
    pub max_job_bytes: usize,
    pub max_disk_bytes: u64,
    pub max_memory_bytes: u64,
    pub max_temporary_bytes: u64,
    pub max_bundle_bytes: u64,
}
impl Default for NativeCaptureUpload {
    fn default() -> Self {
        Self {
            policy: NativeCaptureUploadPolicy::Disabled,
            poll_ms: 30_000,
            timeout_ms: 120_000,
            max_jobs: 256,
            max_job_bytes: 20 * 1024 * 1024,
            max_disk_bytes: 512 * 1024 * 1024,
            max_memory_bytes: 2 * 1024 * 1024 * 1024,
            max_temporary_bytes: 512 * 1024 * 1024,
            max_bundle_bytes: 256 * 1024 * 1024,
        }
    }
}
impl NativeCaptureUpload {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (250..=3_600_000).contains(&self.poll_ms)
                && (1000..=600_000).contains(&self.timeout_ms)
                && (1..=1024).contains(&self.max_jobs)
                && (4096..=32 * 1024 * 1024).contains(&self.max_job_bytes)
                && self.max_disk_bytes >= self.max_job_bytes as u64
                && self.max_disk_bytes <= 4 * 1024 * 1024 * 1024
                && (512 * 1024 * 1024..=4 * 1024 * 1024 * 1024).contains(&self.max_memory_bytes)
                && self.max_temporary_bytes <= 2 * 1024 * 1024 * 1024
                && (4096..=1024 * 1024 * 1024).contains(&self.max_bundle_bytes),
            "invalid native capture upload bounds"
        );
        Ok(())
    }
}

/// Local capture staging only; this does not enable cloud backup or collection.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NativeCapturePolicy {
    #[default]
    Disabled,
    AfterCommandBestEffort,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NativeCapture {
    pub policy: NativeCapturePolicy,
    pub max_bundle_bytes: u64,
    pub max_pending_bytes: u64,
    pub max_pending: usize,
    pub timeout_ms: u64,
}
impl Default for NativeCapture {
    fn default() -> Self {
        Self {
            policy: NativeCapturePolicy::Disabled,
            max_bundle_bytes: 256 * 1024 * 1024,
            max_pending_bytes: 4 * 1024 * 1024 * 1024,
            max_pending: 64,
            timeout_ms: 30_000,
        }
    }
}
impl NativeCapture {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            (4096..=1024 * 1024 * 1024).contains(&self.max_bundle_bytes)
                && self.max_pending_bytes >= self.max_bundle_bytes
                && self.max_pending_bytes <= 16 * 1024 * 1024 * 1024
                && (1..=1024).contains(&self.max_pending)
                && (1..=120_000).contains(&self.timeout_ms),
            "invalid native local capture bounds"
        );
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NativeRepository {
    /// Optional remote provider; paths and authorization are trusted host inputs.
    pub remote: Option<jj_native_remote::Config>,
    pub max_repository_bytes: u64,
    pub max_file_bytes: u64,
    pub max_objects_per_repository: u64,
    pub max_staging_bytes: u64,
    pub max_staging_uploads: usize,
}
impl Default for NativeRepository {
    fn default() -> Self {
        let defaults = jj_native_backend::Config::default().store;
        Self {
            remote: None,
            max_repository_bytes: defaults.max_repository_bytes,
            max_file_bytes: defaults.max_file_bytes,
            max_objects_per_repository: defaults.max_objects_per_repository,
            max_staging_bytes: defaults.max_staging_bytes,
            max_staging_uploads: defaults.max_staging_uploads,
        }
    }
}
impl NativeRepository {
    pub(crate) fn materialize(&self) -> jj_native_backend::Config {
        let mut config = jj_native_backend::Config::default();
        config.store.max_repository_bytes = self.max_repository_bytes;
        config.store.max_file_bytes = self.max_file_bytes;
        config.store.max_objects_per_repository = self.max_objects_per_repository;
        config.store.max_staging_bytes = self.max_staging_bytes;
        config.store.max_staging_uploads = self.max_staging_uploads;
        config
    }
    fn validate(&self) -> Result<()> {
        if let Some(remote) = &self.remote {
            remote
                .validate()
                .map_err(|_| anyhow::anyhow!("invalid native remote configuration"))?;
            ensure!(
                remote.remote.limits.max_metadata_bytes
                    <= self.materialize().store.model_limits.max_object_bytes,
                "native remote metadata budget must fit authoritative store limits"
            );
            ensure!(
                remote.remote.limits.max_file_bytes <= self.max_file_bytes,
                "native remote file budget must fit authoritative store limits"
            );
        }
        ensure!(
            (1..=1024 * 1024 * 1024).contains(&self.max_file_bytes)
                && self.max_repository_bytes >= 1024
                && self.max_repository_bytes >= self.max_file_bytes
                && self.max_repository_bytes <= i64::MAX as u64
                && (1..=10_000_000).contains(&self.max_objects_per_repository)
                && self.max_staging_bytes <= u32::MAX as u64
                && self.max_staging_bytes >= self.max_file_bytes * 2
                && (1..=1024).contains(&self.max_staging_uploads),
            "invalid native repository limits"
        );
        Ok(())
    }
}
impl Native {
    pub(crate) fn broker_config(&self, policy: &reapi::policy::Policy) -> native_broker::Config {
        native_broker::Config {
            max_inflight: self.max_inflight,
            max_query_bytes: self.max_query_bytes,
            max_query_persistent_bytes: self.max_query_persistent_bytes,
            max_memory_bytes: self.max_memory_bytes,
            max_temporary_bytes: self.max_temporary_bytes,
            max_file_bytes: self
                .repositories
                .values()
                .map(|repo| repo.max_file_bytes)
                .max()
                .unwrap_or(native_broker::Config::default().max_file_bytes),
            policy: policy.clone(),
            ..native_broker::Config::default()
        }
    }
    fn validate(&self, policy: &reapi::policy::Policy) -> Result<()> {
        self.workspaces.validate()?;
        if self.workspaces.gc.policy == NativeGcPolicy::LocalComplete {
            ensure!(
                self.repositories.len() == 1
                    && self
                        .repositories
                        .values()
                        .all(|repository| repository.remote.is_none()),
                "local-complete collection requires one fully local repository alias"
            );
        }
        ensure!(
            self.repositories.len() <= 1024,
            "too many native repository aliases"
        );
        for (alias, repo) in &self.repositories {
            ensure!(alias.len() <= 64, "native repository alias is too long");
            tinytree_store::validate_repository_id(alias)
                .context("invalid native repository alias")?;
            repo.validate()?;
            if let Some(remote) = &repo.remote {
                let required = remote.control_work_budget();
                ensure!(
                    required.memory_bytes <= self.max_memory_bytes as u64
                        && required.temporary_bytes <= self.max_temporary_bytes,
                    "native remote control work must fit broker memory and staging limits"
                );
            }
        }
        if !self.repositories.is_empty() {
            self.broker_config(policy)
                .validate()
                .map_err(|_| anyhow::anyhow!("invalid native broker limits"))?;
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cache {
    pub listen: SocketAddr,
    pub upstream: String,
    pub instance_name: String,
    pub api_key_env: String,
    pub max_bytes: u64,
    pub max_concurrent_fetches: usize,
    pub max_concurrent_reads: usize,
    pub max_concurrent_rpcs_per_connection: usize,
    pub max_inflight_bytes: u64,
    pub max_blob_bytes: u64,
    pub action_cache_ttl_seconds: u64,
    pub max_action_cache_entries: usize,
    pub upstream_retention_seconds: u64,
    pub retention_refresh_seconds: u64,
    pub retention_max_entries: usize,
    pub asset_cache_ttl_seconds: u64,
    pub max_asset_cache_entries: usize,
    pub max_asset_cache_bytes: usize,
    pub max_execution_streams: usize,
    /// Zero leaves execution lifetime to the caller, not the cache RPC policy.
    pub execution_timeout_ms: u64,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Bos {
    pub enabled: bool,
}
/// Durable BES is not a cache: unacknowledged events cannot be evicted.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Bes {
    pub enabled: bool,
    /// Empty inherits the cache endpoint; override when BES is hosted separately.
    pub upstream: String,
    /// Empty inherits cache.api_key_env. Values are never written to the spool.
    pub api_key_env: String,
    pub max_pending_bytes: u64,
    pub max_event_bytes: usize,
    pub max_streams: usize,
    pub max_concurrent_forwards: usize,
    pub connect_timeout_ms: u64,
    pub lifecycle_timeout_ms: u64,
    /// Budget after terminal request EOF; zero disables the completion watchdog.
    pub post_half_close_timeout_ms: u64,
    pub retry_initial_delay_ms: u64,
    pub retry_max_attempts: u32,
    pub tombstone_ttl_seconds: u64,
    pub max_completed_streams: usize,
    pub batch_max_events: usize,
    pub batch_max_bytes: usize,
    pub batch_max_delay_ms: u64,
}
impl Default for Bes {
    fn default() -> Self {
        Self {
            enabled: true,
            upstream: String::new(),
            api_key_env: String::new(),
            max_pending_bytes: 1024 * 1024 * 1024,
            max_event_bytes: 4 * 1024 * 1024,
            max_streams: 128,
            max_concurrent_forwards: 16,
            connect_timeout_ms: 10000,
            lifecycle_timeout_ms: 15000,
            post_half_close_timeout_ms: 300000,
            retry_initial_delay_ms: 1000,
            retry_max_attempts: 4,
            tombstone_ttl_seconds: 86400,
            max_completed_streams: 4096,
            batch_max_events: 64,
            batch_max_bytes: 256 * 1024,
            batch_max_delay_ms: 0,
        }
    }
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GitProxy {
    pub listen: SocketAddr,
    pub repositories: std::collections::BTreeMap<String, String>,
    pub sync_to_cas: bool,
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Session {
    pub git: PathBuf,
    pub bazel: PathBuf,
    pub git_vfs: PathBuf,
    pub bazelrc: Option<PathBuf>,
    pub repo_contents_cache: Option<PathBuf>,
    pub remote_repo_contents_cache: bool,
}
impl Default for Config {
    fn default() -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        Self {
            state_dir: home.join(".cache/workspace"),
            runtime: Runtime::default(),
            cache: Cache::default(),
            bes: Bes::default(),
            bos: Bos::default(),
            git_proxy: GitProxy::default(),
            session: Session::default(),
            source: Source::default(),
            native: Native::default(),
            reapi: Default::default(),
        }
    }
}
impl Default for Cache {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8378".parse().unwrap(),
            upstream: String::new(),
            instance_name: String::new(),
            api_key_env: "BUILDBUDDY_API_KEY".into(),
            max_bytes: 4 * 1024 * 1024 * 1024,
            max_concurrent_fetches: 8,
            max_concurrent_reads: 128,
            max_concurrent_rpcs_per_connection: 128,
            max_inflight_bytes: 512 * 1024 * 1024,
            max_blob_bytes: 512 * 1024 * 1024,
            action_cache_ttl_seconds: 3600,
            max_action_cache_entries: 100_000,
            upstream_retention_seconds: 14400,
            retention_refresh_seconds: 10800,
            retention_max_entries: 16384,
            asset_cache_ttl_seconds: 60,
            max_asset_cache_entries: 4096,
            max_asset_cache_bytes: 8 * 1024 * 1024,
            max_execution_streams: 256,
            execution_timeout_ms: 0,
        }
    }
}
impl Default for Bos {
    fn default() -> Self {
        Self { enabled: false }
    }
}
impl Default for GitProxy {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8379".parse().unwrap(),
            repositories: Default::default(),
            sync_to_cas: false,
        }
    }
}
impl Default for Session {
    fn default() -> Self {
        Self {
            git: "git".into(),
            bazel: "bazel".into(),
            git_vfs: "git-vfs".into(),
            bazelrc: None,
            repo_contents_cache: None,
            remote_repo_contents_cache: true,
        }
    }
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .context("opening workspace config")?
            .take(65537)
            .read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 65536, "workspace config exceeds 64 KiB");
        // Parser diagnostics can contain the user's credentials; never echo them.
        let text = std::str::from_utf8(&bytes).context("workspace config must be UTF-8")?;
        let config: Self = toml::from_str(text)
            .map_err(|_| anyhow::anyhow!("invalid workspace TOML; check schema and value types"))?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.runtime.target_open_files <= 1_048_576,
            "runtime.target_open_files must be 0..=1048576"
        );
        ensure!(self.state_dir.is_absolute(), "state_dir must be absolute");
        ensure!(
            self.cache.listen.ip().is_loopback() && self.git_proxy.listen.ip().is_loopback(),
            "cache and BOS listeners must be loopback-only"
        );
        ensure!(
            self.cache.listen.port() != 0
                && self.git_proxy.listen.port() != 0
                && self.git_proxy.listen != self.cache.listen,
            "listeners require distinct nonzero ports"
        );
        ensure!(
            self.cache.upstream.starts_with("grpcs://")
                || self.cache.upstream.starts_with("grpc://")
                || self.cache.upstream.starts_with("https://")
                || self.cache.upstream.starts_with("http://"),
            "cache.upstream must be an explicit gRPC endpoint"
        );
        ensure!(
            self.cache.max_bytes > 0
                && self.cache.max_inflight_bytes > 0
                && self.cache.max_blob_bytes > 0
                && self.cache.max_blob_bytes <= self.cache.max_bytes
                && self.cache.max_blob_bytes <= self.cache.max_inflight_bytes
                && self.cache.max_concurrent_fetches > 0,
            "cache budgets must be positive; max_blob_bytes must fit cache and in-flight budgets"
        );
        ensure!(
            (1..=1024).contains(&self.cache.max_concurrent_reads),
            "cache.max_concurrent_reads must be 1..=1024"
        );
        ensure!(
            (1..=1024).contains(&self.cache.max_concurrent_rpcs_per_connection),
            "cache.max_concurrent_rpcs_per_connection must be 1..=1024"
        );
        ensure!(
            self.cache.action_cache_ttl_seconds <= 3600 && self.cache.max_action_cache_entries > 0,
            "action cache limits outside supported range"
        );
        self.reapi.validate()?;
        self.native.validate(&self.reapi)?;
        for repository in self.native.repositories.values() {
            if let Some(remote) = &repository.remote {
                ensure!(
                    remote.remote.limits.max_file_bytes <= self.cache.max_bytes
                        && remote.remote.limits.max_file_bytes <= self.cache.max_inflight_bytes,
                    "native remote files must fit shared cache and in-flight limits"
                );
            }
        }
        ensure!(
            self.cache.upstream_retention_seconds <= 30 * 86400
                && self.cache.retention_refresh_seconds <= 30 * 86400
                && self.cache.retention_max_entries <= 1_000_000
                && (self.cache.upstream_retention_seconds == 0
                    || (self.cache.retention_refresh_seconds > 0
                        && self.cache.retention_refresh_seconds
                            < self.cache.upstream_retention_seconds
                        && self.cache.action_cache_ttl_seconds
                            <= self.cache.upstream_retention_seconds)),
            "invalid upstream retention or refresh policy"
        );
        ensure!(
            self.cache.asset_cache_ttl_seconds <= 3600
                && self.cache.max_asset_cache_entries <= 1_000_000
                && self.cache.max_asset_cache_bytes <= 64 * 1024 * 1024
                && (1..=1024).contains(&self.cache.max_execution_streams)
                && self.cache.execution_timeout_ms <= 86_400_000,
            "invalid downloader-cache or execution limits"
        );
        ensure!(
            self.source
                .large_blob_spool_bytes
                .is_none_or(|limit| limit <= i64::MAX as u64),
            "source.large_blob_spool_bytes exceeds supported file offsets"
        );
        if self.bes.enabled {
            let upstream = if self.bes.upstream.is_empty() {
                &self.cache.upstream
            } else {
                &self.bes.upstream
            };
            ensure!(
                ["grpcs://", "grpc://", "https://", "http://"]
                    .iter()
                    .any(|prefix| upstream.starts_with(prefix)),
                "bes.upstream must be an explicit gRPC endpoint"
            );
            ensure!(
                self.bes.max_pending_bytes > 0
                    && self.bes.max_event_bytes > 0
                    && self.bes.max_event_bytes as u64 <= self.bes.max_pending_bytes
                    && (1..=1024).contains(&self.bes.max_streams)
                    && (1..=self.bes.max_streams).contains(&self.bes.max_concurrent_forwards)
                    && (1..=600000).contains(&self.bes.connect_timeout_ms)
                    && (1..=600000).contains(&self.bes.lifecycle_timeout_ms)
                    && self.bes.post_half_close_timeout_ms <= 604800000
                    && (1..=60000).contains(&self.bes.retry_initial_delay_ms)
                    && self.bes.retry_max_attempts <= 20
                    && (1..=1024).contains(&self.bes.batch_max_events)
                    && (1..=64 * 1024 * 1024).contains(&self.bes.batch_max_bytes)
                    && self.bes.batch_max_delay_ms <= 100
                    && (1..=1_000_000).contains(&self.bes.max_completed_streams)
                    && (60..=604800).contains(&self.bes.tombstone_ttl_seconds),
                "invalid BES spool, concurrency, timeout or retry limits"
            );
        }
        Ok(())
    }
    pub fn socket(&self) -> PathBuf {
        self.state_dir.join("control.sock")
    }
    pub fn rbe_endpoint(&self) -> String {
        format!("unix:{}", self.state_dir.join("rbe.sock").display())
    }
    pub fn cache_endpoint(&self) -> String {
        self.rbe_endpoint()
    }
    pub fn bes_endpoint(&self) -> String {
        self.rbe_endpoint()
    }

    pub fn bytestream_uri_prefix(&self) -> String {
        let authority = self
            .cache
            .upstream
            .split_once("://")
            .map_or(self.cache.upstream.as_str(), |(_, authority)| authority)
            .trim_end_matches('/');
        if self.cache.instance_name.is_empty() {
            authority.to_owned()
        } else {
            format!("{authority}/{}", self.cache.instance_name)
        }
    }
}
pub const EXAMPLE: &str = r#"# One daemon per user/trust boundary. Keep credentials in the environment.
# state_dir defaults to $HOME/.cache/workspace (no expansion inside TOML values).
[runtime]
# Soft descriptor headroom, capped by the inherited hard limit; 0 preserves it.
target_open_files = 16384
[cache]
upstream = "grpcs://YOUR_INSTANCE.buildbuddy.io"
api_key_env = "BUILDBUDDY_API_KEY"
# 4 GiB shared verified-byte cache; authoritative edits are never evicted.
max_bytes = 4294967296
# Bound simultaneous requests on each local cache connection.
max_concurrent_rpcs_per_connection = 128
# Bound CAS serving files across connections, including streaming responses.
max_concurrent_reads = 128
# Fixed validity after an upstream AC read; local hits do not slide it.
action_cache_ttl_seconds = 3600
# Assumed backend retention. Zero disables background retention maintenance.
upstream_retention_seconds = 14400
retention_refresh_seconds = 10800
retention_max_entries = 16384
asset_cache_ttl_seconds = 60
max_asset_cache_entries = 4096
max_asset_cache_bytes = 8388608
max_execution_streams = 256
# Zero preserves caller deadlines without imposing a cache-derived lifetime.
execution_timeout_ms = 0

[bes]
# Durable local ACKs; pending events are NOT part of the disposable CAS/LRU.
enabled = true
# Empty upstream/api_key_env inherit [cache]. Override for a separate BES host.
# upstream = "grpcs://YOUR_INSTANCE.buildbuddy.io"
max_pending_bytes = 1073741824
max_streams = 128
max_concurrent_forwards = 16
# Group ready events behind one fsync; never wait to fill a batch.
batch_max_events = 64
batch_max_bytes = 262144
batch_max_delay_ms = 0
# Bazel-compatible retry defaults (four retries after the first attempt).
connect_timeout_ms = 10000
lifecycle_timeout_ms = 15000
# Allow EOF-buffered ACKs; bound the final upstream wait. Zero disables it.
post_half_close_timeout_ms = 300000
retry_initial_delay_ms = 1000
retry_max_attempts = 4

[source]
# Separate bounded disk spooling for large Git blobs; zero disables it.
# Default: 512 MiB per mount with decoded-content caching enabled.
# large_blob_spool_bytes = 536870912

# Optional native jj object I/O shares rbe.sock; stores live under state_dir/native.
# [native.repositories.example]
# max_repository_bytes = 17179869184
# max_file_bytes = 268435456
# [native.repositories.example.remote]
# authorization_profile = "agent-profile"
# [native.repositories.example.remote.remote]
# endpoint = "http://127.0.0.1:8093"
# repository_id = "example"
# token_env = "TINYTREE_AGENT_TOKEN"
# compression = true

[bos]
# Opt in to strictly lazy outputs; metadata publication is not yet batched.
enabled = false

# Configured origins only; use aliases with workspace create --repo ALIAS.
[git_proxy.repositories]
# bazel = "https://github.com/bazelbuild/bazel.git"

[session]
# Select a Bazel including the Unix-socket authority fix (upstream 242563f1a1).
# bazel = "/absolute/path/to/compatible/bazel"
# bazelrc = "/absolute/path/to/provisioned.bazelrc"
remote_repo_contents_cache = true

[reapi]
connect_timeout_ms = 10000
rpc_timeout_ms = 60000
operation_timeout_ms = 120000
presence_ttl_seconds = 0
"#;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn example_has_lazy_conservative_defaults() {
        let config: Config = toml::from_str(EXAMPLE).unwrap();
        config.validate().unwrap();
        assert!(!config.bos.enabled);
        assert!(!Bos::default().enabled);
        let enabled: Config =
            toml::from_str(&EXAMPLE.replace("enabled = false", "enabled = true")).unwrap();
        enabled.validate().unwrap();
        assert!(enabled.bos.enabled);
        assert_eq!(config.runtime.target_open_files, 16_384);
        assert_eq!(Config::default().runtime.target_open_files, 16_384);
        assert_eq!(config.cache.action_cache_ttl_seconds, 3600);
        assert_eq!(config.cache.upstream_retention_seconds, 14400);
        assert_eq!(config.cache.retention_refresh_seconds, 10800);
        let mut invalid = config.clone();
        invalid.cache.retention_refresh_seconds = invalid.cache.upstream_retention_seconds;
        assert!(
            invalid.validate().is_err(),
            "refresh requires retention headroom"
        );
        invalid.cache.upstream_retention_seconds = 0;
        assert!(invalid.validate().is_ok(), "zero disables maintenance");
        assert_eq!(config.cache.max_concurrent_rpcs_per_connection, 128);
        assert_eq!(config.cache.max_concurrent_reads, 128);
        assert!(config.session.remote_repo_contents_cache);
        assert!(config.bes.enabled);
        assert_eq!(config.bes.retry_max_attempts, 4);
        assert_eq!(config.bes.post_half_close_timeout_ms, 300000);
        assert_eq!(config.bes.batch_max_events, 64);
        assert_eq!(config.bes.batch_max_bytes, 262144);
        assert_eq!(config.bes.batch_max_delay_ms, 0);
        assert_eq!(config.source.large_blob_spool_bytes, None);
        let disabled: Config = toml::from_str(&EXAMPLE.replace(
            "# large_blob_spool_bytes = 536870912",
            "large_blob_spool_bytes = 0",
        ))
        .unwrap();
        disabled.validate().unwrap();
        assert_eq!(disabled.source.large_blob_spool_bytes, Some(0));
        let mut watchdog = config.clone();
        watchdog.bes.post_half_close_timeout_ms = 0;
        assert!(
            watchdog.validate().is_ok(),
            "completion watchdog may be disabled"
        );
        watchdog.bes.post_half_close_timeout_ms = 604800001;
        assert!(watchdog.validate().is_err(), "completion budget is bounded");
    }
    #[test]
    fn cache_admission_limits_accept_overrides_and_reject_invalid_bounds() {
        let settings: [(&str, fn(&Cache) -> usize); 2] = [
            ("max_concurrent_rpcs_per_connection", |cache| {
                cache.max_concurrent_rpcs_per_connection
            }),
            ("max_concurrent_reads", |cache| cache.max_concurrent_reads),
        ];
        for (setting, value) in settings {
            for (name, limit, valid) in [
                ("single request", 1, true),
                ("benchmark override", 32, true),
                ("maximum supported", 1024, true),
                ("disabled admission", 0, false),
                ("above maximum", 1025, false),
            ] {
                let input =
                    EXAMPLE.replace(&format!("{setting} = 128"), &format!("{setting} = {limit}"));
                let config: Config = toml::from_str(&input).unwrap();
                assert_eq!(value(&config.cache), limit, "{setting}: {name}");
                assert_eq!(config.validate().is_ok(), valid, "{setting}: {name}");
            }
            let input = EXAMPLE.replace(&format!("{setting} = 128\n"), "");
            assert_eq!(value(&toml::from_str::<Config>(&input).unwrap().cache), 128);
        }
    }
    #[test]
    fn process_headroom_accepts_inheritance_and_bounded_overrides() {
        for (name, target, valid) in [
            ("inherit", 0, true),
            ("small explicit target", 512, true),
            ("maximum target", 1_048_576, true),
            ("above maximum", 1_048_577, false),
        ] {
            let input = EXAMPLE.replace(
                "target_open_files = 16384",
                &format!("target_open_files = {target}"),
            );
            let config: Config = toml::from_str(&input).unwrap();
            assert_eq!(config.runtime.target_open_files, target, "{name}");
            assert_eq!(config.validate().is_ok(), valid, "{name}");
        }
        let input = EXAMPLE.replace("target_open_files = 16384\n", "");
        assert_eq!(
            toml::from_str::<Config>(&input)
                .unwrap()
                .runtime
                .target_open_files,
            16_384
        );
    }
    #[test]
    fn native_aliases_are_optional_portable_and_have_independent_budgets() {
        let old: Config = toml::from_str(EXAMPLE).unwrap();
        assert!(old.native.repositories.is_empty());
        assert_eq!(old.native.workspaces.max_workspaces, 128);
        assert_eq!(old.native.workspaces.index_mode, NativeIndexMode::Legacy);
        assert_eq!(old.native.workspaces.gc.policy, NativeGcPolicy::Disabled);
        assert_eq!(
            old.native.workspaces.capture.policy,
            NativeCapturePolicy::Disabled
        );
        let parsed: Config = toml::from_str(
            "[native.workspaces.capture]\npolicy='after_command_best_effort'\nmax_pending=8\n",
        )
        .unwrap();
        let mut enabled = old.clone();
        enabled.native.workspaces.capture = parsed.native.workspaces.capture;
        assert_eq!(
            enabled.native.workspaces.capture.policy,
            NativeCapturePolicy::AfterCommandBestEffort
        );
        assert!(enabled.validate().is_ok());
        assert_eq!(
            old.native.workspaces.upload.policy,
            NativeCaptureUploadPolicy::Disabled
        );
        let upload: Config = toml::from_str(
            "[native.workspaces.upload]\npolicy='publish_retaining_roots'\npoll_ms=250\n",
        )
        .unwrap();
        let mut upload_enabled = old.clone();
        upload_enabled.native.workspaces.upload = upload.native.workspaces.upload;
        assert!(upload_enabled.validate().is_ok());
        upload_enabled.native.workspaces.upload.max_jobs = 0;
        assert!(upload_enabled.validate().is_err());
        let mut bad_capture = enabled.clone();
        bad_capture.native.workspaces.capture.max_pending = 0;
        assert!(bad_capture.validate().is_err());
        assert_eq!(
            toml::from_str::<Config>("[native.workspaces]\nindex_mode='federated'\n")
                .unwrap()
                .native
                .workspaces
                .index_mode,
            NativeIndexMode::Federated
        );
        assert!(old.native.workspaces.registry_path.is_none());
        for value in [0, 4097] {
            let mut invalid = old.clone();
            invalid.native.workspaces.max_workspaces = value;
            assert!(invalid.validate().is_err());
        }
        let mut invalid = old.clone();
        invalid.native.workspaces.registry_path = Some("relative-registry".into());
        assert!(invalid.validate().is_err());
        let input = format!(
            "{EXAMPLE}\n[native.repositories.example]\nmax_repository_bytes = 1073741824\n"
        );
        let mut configured: Config = toml::from_str(&input).unwrap();
        configured.validate().unwrap();
        let parsed_gc: Config =
            toml::from_str("[native.workspaces.gc]\npolicy='local_complete'\ngrace_seconds=3600\n")
                .unwrap();
        let mut local_gc = configured.clone();
        local_gc.native.workspaces.gc = parsed_gc.native.workspaces.gc;
        local_gc.validate().unwrap();
        let mut impossible_checkpoint = local_gc.clone();
        impossible_checkpoint
            .native
            .workspaces
            .gc
            .max_checkpoint_bytes = 256 * 1024 * 1024;
        assert!(
            impossible_checkpoint
                .validate()
                .unwrap_err()
                .to_string()
                .contains("checkpoint byte budget")
        );
        let mut minimum_checkpoint = local_gc.clone();
        minimum_checkpoint.native.workspaces.gc.max_checkpoint_bytes = 512 * 1024 * 1024;
        minimum_checkpoint.validate().unwrap();
        for (name, mutate) in [
            ("zero root budget", 0),
            ("above collector root cap", 10_001),
        ] {
            let mut invalid = local_gc.clone();
            invalid.native.workspaces.gc.max_roots = mutate;
            assert!(invalid.validate().is_err(), "{name}");
        }
        let mut combined = local_gc.clone();
        combined.native.workspaces.index_mode = NativeIndexMode::Federated;
        assert!(combined.validate().is_err());
        let mut combined = local_gc.clone();
        combined.native.workspaces.capture.policy = NativeCapturePolicy::AfterCommandBestEffort;
        assert!(combined.validate().is_err());
        let mut no_alias = local_gc.clone();
        no_alias.native.repositories.clear();
        assert!(no_alias.validate().is_err());
        let mut extra_alias = local_gc;
        extra_alias
            .native
            .repositories
            .insert("other".into(), NativeRepository::default());
        assert!(extra_alias.validate().is_err());
        assert_eq!(configured.native.max_inflight, 16);
        assert_eq!(configured.native.max_memory_bytes, 512 * 1024 * 1024);
        assert_eq!(configured.native.max_temporary_bytes, 1024 * 1024 * 1024);
        assert_eq!(
            configured.native.repositories["example"].max_file_bytes,
            256 * 1024 * 1024
        );
        configured.cache.max_inflight_bytes = 64 * 1024 * 1024;
        configured.cache.max_blob_bytes = 64 * 1024 * 1024;
        configured.validate().unwrap();
        let remote_configuration = format!(
            "{EXAMPLE}\n[native.repositories.example.remote.remote]\nendpoint='http://127.0.0.1:8093'\nrepository_id='example'\n"
        );
        let remote: Config = toml::from_str(&remote_configuration).unwrap();
        remote.validate().unwrap();
        let mut oversized = remote.clone();
        oversized
            .native
            .repositories
            .get_mut("example")
            .unwrap()
            .remote
            .as_mut()
            .unwrap()
            .remote
            .limits
            .max_metadata_bytes = 32 * 1024 * 1024;
        assert!(
            oversized.validate().is_err(),
            "remote metadata must fit local storage"
        );
        let mut no_headroom = remote.clone();
        no_headroom.native.max_memory_bytes = 64 * 1024 * 1024;
        assert!(
            no_headroom.validate().is_err(),
            "remote control memory must fit the broker pool"
        );
        for alias in [
            "../escape",
            "path/name",
            "Uppercase",
            "con",
            "lpt1",
            "",
            &"x".repeat(65),
        ] {
            let mut bad = old.clone();
            bad.native
                .repositories
                .insert(alias.into(), Default::default());
            assert!(bad.validate().is_err(), "{alias}");
        }
        let mut bad = configured.clone();
        bad.native.max_inflight = 0;
        assert!(bad.validate().is_err());
        bad = configured.clone();
        bad.native
            .repositories
            .get_mut("example")
            .unwrap()
            .max_staging_bytes = 1;
        assert!(bad.validate().is_err());
        let unknown = format!(
            "{EXAMPLE}\n[native.repositories.example]\ncaller_path='/tmp/not-selected-by-clients'\n"
        );
        assert!(toml::from_str::<Config>(&unknown).is_err());
    }

    #[test]
    fn refuses_public_listener_and_unknown_fields() {
        let mut config: Config = toml::from_str(EXAMPLE).unwrap();
        config.cache.listen = "0.0.0.0:8378".parse().unwrap();
        assert!(config.validate().is_err());
        assert!(toml::from_str::<Config>("mystery=true").is_err());
    }
}
