use anyhow::{Result, bail, ensure};
use serde::Deserialize;
use std::{io::Read, path::Path};
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClosureLimits {
    pub max_objects: usize,
    pub max_edges: usize,
    pub max_metadata_bytes: usize,
    pub max_path_contexts: usize,
    pub max_path_context_bytes: usize,
    pub max_path_bytes: usize,
}
impl Default for ClosureLimits {
    fn default() -> Self {
        Self {
            max_objects: 100_000,
            max_edges: 1_000_000,
            max_metadata_bytes: 64 * 1024 * 1024,
            max_path_contexts: 100_000,
            max_path_context_bytes: 32 * 1024 * 1024,
            max_path_bytes: 65536,
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub remote: tinytree_client::Config,
    /// Trusted broker-selected effective authorization profile, not an RPC field.
    pub authorization_profile: String,
    pub closure: ClosureLimits,
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
            remote: Default::default(),
            authorization_profile: "default".into(),
            closure: Default::default(),
            operation_timeout_ms: 120_000,
            max_ledger_bytes: 256 * 1024 * 1024,
            max_pending_operations: 1024,
            max_admitted_roots: 4096,
            max_file_index_entries: 1_000_000,
            max_upload_spool_bytes: 512 * 1024 * 1024,
            max_staging_files: 4096,
        }
    }
}
impl Config {
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
        self.remote
            .validate()
            .map_err(|_| anyhow::anyhow!("invalid native remote client configuration"))?;
        let c = &self.closure;
        if self.authorization_profile.is_empty()
            || self.authorization_profile.len() > 128
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
