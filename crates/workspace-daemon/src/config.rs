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
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Bos {
    pub enabled: bool,
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
            action_cache_ttl_seconds: 0,
            max_action_cache_entries: 100_000,
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
        Ok(())
    }
    pub fn socket(&self) -> PathBuf {
        self.state_dir.join("control.sock")
    }
    pub fn cache_endpoint(&self) -> String {
        format!("unix:{}", self.state_dir.join("cache.sock").display())
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
# Zero revalidates action results against upstream on every lookup.
action_cache_ttl_seconds = 0

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
        assert_eq!(config.cache.action_cache_ttl_seconds, 0);
        assert_eq!(config.cache.max_concurrent_rpcs_per_connection, 128);
        assert!(config.session.remote_repo_contents_cache);
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
