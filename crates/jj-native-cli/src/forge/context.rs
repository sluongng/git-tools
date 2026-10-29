//! Owner-private product context. Only a token environment name is persisted.
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{io, path::PathBuf};

const MAX_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct Context {
    pub schema_version: u32,
    pub server: String,
    pub account_id: String,
    pub view: String,
    pub token_env: Option<String>,
    #[serde(default)]
    pub ca_certificate: Option<PathBuf>,
    #[serde(default)]
    pub tls_domain: Option<String>,
}

fn invalid() -> io::Error {
    io::Error::other("invalid Tinytree account context")
}

impl Context {
    pub fn validate(&self) -> io::Result<()> {
        if self.schema_version != 1
            || !valid_name(&self.account_id)
            || !valid_name(&self.view)
            || self.server.len() > 4096
            || !(self.server.starts_with("https://") || self.server.starts_with("http://"))
            || self.server.contains(['\n', '\r', '@', '?', '#'])
            || self.tls_domain.as_ref().is_some_and(|value| {
                value.is_empty() || value.len() > 253 || value.chars().any(char::is_control)
            })
            || self.token_env.as_ref().is_some_and(|name| {
                name.is_empty()
                    || name.len() > 128
                    || !name.bytes().enumerate().all(|(i, c)| {
                        c == b'_' || c.is_ascii_alphabetic() || (i != 0 && c.is_ascii_digit())
                    })
            })
        {
            return Err(invalid());
        }
        tonic::transport::Endpoint::from_shared(self.server.clone()).map_err(|_| invalid())?;
        Ok(())
    }

    pub fn client_config(&self) -> io::Result<tinytree_client::Config> {
        self.validate()?;
        let value = tinytree_client::Config {
            endpoint: self.server.clone(),
            // The service resolves the account's sole monorepo. This SDK field
            // is only a validated transport configuration, never a wire grant.
            repository_id: format!("forge-{}", self.account_id),
            token_env: self.token_env.clone(),
            tls: self
                .ca_certificate
                .as_ref()
                .map(|path| tinytree_client::Tls {
                    ca_certificate: path.clone(),
                    domain_name: self.tls_domain.clone(),
                }),
            compression: false,
            ..Default::default()
        };
        value
            .validate()
            .map_err(|_| io::Error::other("invalid Tinytree transport or trust configuration"))?;
        Ok(value)
    }

    pub fn load(directory: &PrivateDir) -> io::Result<Self> {
        let value: Self = serde_json::from_slice(&directory.read("context.json", MAX_BYTES)?)
            .map_err(|_| invalid())?;
        value.validate()?;
        Ok(value)
    }

    pub fn save(&self, directory: &PrivateDir) -> io::Result<()> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid())?;
        if bytes.len() > MAX_BYTES {
            return Err(invalid());
        }
        directory.replace("context.json", &bytes)
    }
}

pub(super) fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}

pub(super) fn path() -> io::Result<PathBuf> {
    if let Some(value) = std::env::var_os("TINYTREE_CONFIG_DIR") {
        return Ok(PathBuf::from(value));
    }
    if let Some(value) = std::env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(value).join("tinytree"));
    }
    std::env::var_os("HOME")
        .map(|value| PathBuf::from(value).join(".config/tinytree"))
        .ok_or_else(|| io::Error::other("Tinytree configuration directory is unavailable"))
}

pub(super) fn open() -> io::Result<PrivateDir> {
    PrivateDir::open_existing(path()?)
}

pub(super) fn create() -> io::Result<PrivateDir> {
    #[cfg(unix)]
    {
        PrivateDir::create_all_durable(path()?)
    }
    #[cfg(not(unix))]
    {
        Err(io::Error::other(
            "Tinytree account configuration is unsupported on this platform",
        ))
    }
}
