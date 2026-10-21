use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub state_dir: PathBuf,
    pub cache: Cache,
    pub bes: Bes,
    pub bos: Bos,
    pub git_proxy: GitProxy,
    pub session: Session,
    pub reapi: reapi::policy::Policy,
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cache {
    pub listen: SocketAddr,
    pub upstream: String,
    pub instance_name: String,
    pub api_key_env: String,
    pub max_bytes: u64,
    pub max_concurrent_fetches: usize,
    pub max_concurrent_rpcs_per_connection: usize,
    pub max_inflight_bytes: u64,
    pub max_blob_bytes: u64,
    pub action_cache_ttl_seconds: u64,
    pub max_action_cache_entries: usize,
    pub asset_cache_ttl_seconds: u64,
    pub max_asset_cache_entries: usize,
    pub max_asset_cache_bytes: usize,
    pub max_execution_streams: usize,
    /// Zero leaves execution lifetime to the caller, not the cache RPC policy.
    pub execution_timeout_ms: u64,
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Bos {
    pub enabled: bool,
}
/// Durable BES is not a cache: unacknowledged events cannot be evicted.
#[derive(Clone, Deserialize)]
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
#[derive(Clone, Deserialize)]
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
            cache: Cache::default(),
            bes: Bes::default(),
            bos: Bos::default(),
            git_proxy: GitProxy::default(),
            session: Session::default(),
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
            max_concurrent_rpcs_per_connection: 128,
            max_inflight_bytes: 512 * 1024 * 1024,
            max_blob_bytes: 512 * 1024 * 1024,
            action_cache_ttl_seconds: 60,
            max_action_cache_entries: 100_000,
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
            (1..=1024).contains(&self.cache.max_concurrent_rpcs_per_connection),
            "cache.max_concurrent_rpcs_per_connection must be 1..=1024"
        );
        ensure!(
            self.cache.action_cache_ttl_seconds <= 3600 && self.cache.max_action_cache_entries > 0,
            "action cache limits outside supported range"
        );
        self.reapi.validate()?;
        ensure!(
            self.cache.asset_cache_ttl_seconds <= 3600
                && self.cache.max_asset_cache_entries <= 1_000_000
                && self.cache.max_asset_cache_bytes <= 64 * 1024 * 1024
                && (1..=1024).contains(&self.cache.max_execution_streams)
                && self.cache.execution_timeout_ms <= 86_400_000,
            "invalid downloader-cache or execution limits"
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
[cache]
upstream = "grpcs://YOUR_INSTANCE.buildbuddy.io"
api_key_env = "BUILDBUDDY_API_KEY"
# 4 GiB shared verified-byte cache; authoritative edits are never evicted.
max_bytes = 4294967296
# Bound simultaneous requests on each local cache connection.
max_concurrent_rpcs_per_connection = 128
# Short fixed AC TTL; set zero when no backend-retention assumption is available.
action_cache_ttl_seconds = 60
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
retry_initial_delay_ms = 1000
retry_max_attempts = 4

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
        assert_eq!(config.cache.action_cache_ttl_seconds, 60);
        assert_eq!(config.cache.max_concurrent_rpcs_per_connection, 128);
        assert!(config.session.remote_repo_contents_cache);
        assert!(config.bes.enabled);
        assert_eq!(config.bes.retry_max_attempts, 4);
        assert_eq!(config.bes.batch_max_events, 64);
        assert_eq!(config.bes.batch_max_bytes, 262144);
        assert_eq!(config.bes.batch_max_delay_ms, 0);
    }
    #[test]
    fn cache_rpc_connection_limit_accepts_overrides_and_rejects_invalid_bounds() {
        for (name, limit, valid) in [
            ("single request", 1, true),
            ("benchmark override", 32, true),
            ("maximum supported", 1024, true),
            ("disabled admission", 0, false),
            ("above maximum", 1025, false),
        ] {
            let input = EXAMPLE.replace(
                "max_concurrent_rpcs_per_connection = 128",
                &format!("max_concurrent_rpcs_per_connection = {limit}"),
            );
            let config: Config = toml::from_str(&input).unwrap();
            assert_eq!(
                config.cache.max_concurrent_rpcs_per_connection, limit,
                "{name}"
            );
            assert_eq!(config.validate().is_ok(), valid, "{name}");
        }
        let input = EXAMPLE.replace("max_concurrent_rpcs_per_connection = 128\n", "");
        assert_eq!(
            toml::from_str::<Config>(&input)
                .unwrap()
                .cache
                .max_concurrent_rpcs_per_connection,
            128
        );
    }
    #[test]
    fn refuses_public_listener_and_unknown_fields() {
        let mut config: Config = toml::from_str(EXAMPLE).unwrap();
        config.cache.listen = "0.0.0.0:8378".parse().unwrap();
        assert!(config.validate().is_err());
        assert!(toml::from_str::<Config>("mystery=true").is_err());
    }
}
