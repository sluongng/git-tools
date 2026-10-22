use serde::Deserialize;
use std::{
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_inflight: usize,
    pub max_frame_bytes: usize,
    pub max_request_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_batch_items: usize,
    pub max_batch_bytes: usize,
    pub max_graph_pages: usize,
    pub max_graph_objects: usize,
    pub max_graph_bytes: usize,
    pub max_file_bytes: u64,
    pub max_buffered_bytes: usize,
    pub max_temporary_bytes: u32,
}
impl Limits {
    pub(crate) fn metadata_memory(&self) -> usize {
        self.max_batch_bytes
            .saturating_mul(4)
            .saturating_add(
                self.max_batch_items
                    .saturating_mul(std::mem::size_of::<crate::MetadataOutcome>())
                    .saturating_mul(4),
            )
            .saturating_add(self.max_request_bytes.saturating_mul(2))
    }

    pub(crate) fn graph_memory(&self) -> usize {
        self.max_graph_bytes
            .saturating_mul(2)
            .saturating_add(self.max_metadata_bytes.saturating_mul(3))
            .saturating_add(self.max_request_bytes.saturating_mul(2))
    }
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_inflight: 4,
            max_frame_bytes: 65536,
            max_request_bytes: 256 * 1024,
            max_metadata_bytes: 16 * 1024 * 1024,
            max_batch_items: 256,
            max_batch_bytes: 32 * 1024 * 1024,
            max_graph_pages: 1024,
            max_graph_objects: 100_000,
            max_graph_bytes: 64 * 1024 * 1024,
            max_file_bytes: 256 * 1024 * 1024,
            max_buffered_bytes: 256 * 1024 * 1024,
            max_temporary_bytes: 1024 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub ca_certificate: PathBuf,
    pub domain_name: Option<String>,
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub endpoint: String,
    pub repository_id: String,
    pub token_env: Option<String>,
    pub tls: Option<Tls>,
    pub compression: bool,
    pub limits: Limits,
    pub rpc: reapi::policy::Policy,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8093".into(),
            repository_id: "default".into(),
            token_env: None,
            tls: None,
            compression: true,
            limits: Limits::default(),
            rpc: Default::default(),
        }
    }
}
#[derive(Debug)]
pub struct ConfigError(pub(crate) &'static str);
impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ConfigError {}
impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let mut data = Vec::new();
        std::fs::File::open(path)
            .map_err(|_| ConfigError("cannot open native client configuration"))?
            .take(65537)
            .read_to_end(&mut data)
            .map_err(|_| ConfigError("cannot read native client configuration"))?;
        if data.len() > 65536 {
            return Err(ConfigError("native client configuration exceeds 64 KiB"));
        }
        let value: Self = toml::from_str(
            std::str::from_utf8(&data)
                .map_err(|_| ConfigError("invalid native client configuration"))?,
        )
        .map_err(|_| ConfigError("invalid native client configuration"))?;
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.rpc
            .validate()
            .map_err(|_| ConfigError("invalid native client RPC policy"))?;
        if self.endpoint.len() > 4096
            || self
                .tls
                .as_ref()
                .and_then(|tls| tls.domain_name.as_ref())
                .is_some_and(|name| name.is_empty() || name.len() > 253)
        {
            return Err(ConfigError("invalid native endpoint or TLS name"));
        }
        let l = &self.limits;
        if !(1..=1024).contains(&l.max_inflight)
            || !(1024..=1024 * 1024).contains(&l.max_frame_bytes)
            || !(1024..=64 * 1024 * 1024).contains(&l.max_request_bytes)
            || !(1024..=64 * 1024 * 1024).contains(&l.max_metadata_bytes)
            || !(1..=4096).contains(&l.max_batch_items)
            || !(1..=100_000).contains(&l.max_graph_pages)
            || !(1..=1_000_000).contains(&l.max_graph_objects)
            || l.max_batch_bytes < l.max_metadata_bytes
            || l.max_graph_bytes < l.max_metadata_bytes
            || l.max_buffered_bytes > 1024 * 1024 * 1024
            || l.max_buffered_bytes
                < l.max_batch_bytes
                    .max(l.max_graph_bytes)
                    .max(l.max_request_bytes)
                    .saturating_mul(4)
            || l.max_buffered_bytes < l.metadata_memory()
            || l.max_buffered_bytes < l.graph_memory()
            || l.max_buffered_bytes < l.max_frame_bytes.saturating_mul(4).saturating_add(1024)
            || l.max_file_bytes == 0
            || l.max_file_bytes > u64::from(l.max_temporary_bytes)
            || l.max_temporary_bytes > 1024 * 1024 * 1024
        {
            return Err(ConfigError("invalid native client resource limits"));
        }
        if self.repository_id.is_empty()
            || self.repository_id.len() > 128
            || !self
                .repository_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        {
            return Err(ConfigError("invalid native repository identifier"));
        }
        let name = self.repository_id.as_str();
        if matches!(name, "con" | "prn" | "aux" | "nul")
            || ((name.starts_with("com") || name.starts_with("lpt"))
                && name.len() == 4
                && matches!(name.as_bytes()[3], b'1'..=b'9'))
        {
            return Err(ConfigError("reserved native repository identifier"));
        }
        if let Some(name) = &self.token_env {
            if name.is_empty()
                || name.len() > 128
                || !name.bytes().enumerate().all(|(i, b)| {
                    b.is_ascii_alphabetic() || b == b'_' || (i > 0 && b.is_ascii_digit())
                })
            {
                return Err(ConfigError("invalid native token environment name"));
            }
        }
        if self.endpoint.starts_with("unix://") {
            if self.tls.is_some() || !Path::new(&self.endpoint[7..]).is_absolute() {
                return Err(ConfigError("invalid native Unix endpoint"));
            }
            return Ok(());
        }
        let uri: http::Uri = self
            .endpoint
            .parse()
            .map_err(|_| ConfigError("invalid native endpoint"))?;
        if uri.authority().is_none()
            || uri.authority().is_some_and(|a| a.as_str().contains('@'))
            || uri.path_and_query().is_some_and(|p| p.as_str() != "/")
        {
            return Err(ConfigError("invalid native endpoint"));
        }
        match uri.scheme_str() {
            Some("https") if self.tls.is_some() => {}
            Some("http")
                if self.tls.is_none()
                    && uri
                        .host()
                        .and_then(|h| h.trim_matches(['[', ']']).parse::<std::net::IpAddr>().ok())
                        .is_some_and(|ip| ip.is_loopback()) => {}
            _ => {
                return Err(ConfigError(
                    "native endpoints require loopback plaintext or verified TLS",
                ));
            }
        }
        Ok(())
    }
}
