//! Reuse identity for cooperating clients of a private, same-user control socket.
//! This is configuration binding, not authentication against hostile same-UID code.
use crate::config::Config;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

const VERSION: u32 = 2;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Challenge {
    pub version: u32,
    pub nonce: [u8; 32],
}

pub(crate) struct Credentials {
    pub cache: Option<String>,
    pub bes: Option<String>,
    pub native: std::collections::BTreeMap<String, Option<String>>,
    pub native_ca: std::collections::BTreeMap<String, std::sync::Arc<[u8]>>,
}

fn credential(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Ok(Some(value)),
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => anyhow::bail!("credential environment variable must be UTF-8"),
    }
}

impl Credentials {
    pub fn load(config: &Config) -> Result<Self> {
        let mut native = std::collections::BTreeMap::new();
        let mut native_ca = std::collections::BTreeMap::new();
        for (alias, repository) in &config.native.repositories {
            if let Some(remote) = &repository.remote {
                let value = match &remote.remote.token_env {
                    Some(name) => credential(name)?,
                    None => None,
                };
                native.insert(alias.clone(), value);
                if let Some(tls) = &remote.remote.tls {
                    use std::io::Read;
                    let mut bytes = Vec::new();
                    std::fs::File::open(&tls.ca_certificate)
                        .map_err(|_| anyhow::anyhow!("cannot read native trust roots"))?
                        .take(1024 * 1024 + 1)
                        .read_to_end(&mut bytes)
                        .map_err(|_| anyhow::anyhow!("cannot read native trust roots"))?;
                    ensure!(
                        !bytes.is_empty() && bytes.len() <= 1024 * 1024,
                        "invalid native trust root snapshot size"
                    );
                    native_ca.insert(alias.clone(), bytes.into());
                }
            }
        }
        Ok(Self {
            native,
            native_ca,
            cache: credential(&config.cache.api_key_env)?,
            bes: if config.bes.enabled {
                credential(if config.bes.api_key_env.is_empty() {
                    &config.cache.api_key_env
                } else {
                    &config.bes.api_key_env
                })?
            } else {
                None
            },
        })
    }
}

impl Challenge {
    fn new() -> Self {
        let mut nonce = [0; 32];
        nonce[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        nonce[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        Self {
            version: VERSION,
            nonce,
        }
    }

    pub(crate) fn digest(&self, config: &Config, credentials: &Credentials) -> Result<String> {
        ensure!(
            self.version == VERSION,
            "daemon identity version mismatch; shut down using the matching binary and configuration before restarting"
        );
        let mut bes = serde_json::to_value(&config.bes)?;
        if config.bes.enabled {
            if config.bes.upstream.is_empty() {
                bes["upstream"] = config.cache.upstream.clone().into();
            }
            if config.bes.api_key_env.is_empty() {
                bes["api_key_env"] = config.cache.api_key_env.clone().into();
            }
        } else {
            bes = serde_json::json!({"enabled": false});
        }
        let p = &config.reapi;
        // Use length-delimited serialization, including all effective service
        // settings and resolved credentials, excluding session-only settings.
        let mut material = serde_json::json!({
            "version": VERSION,
            "state_dir": config.state_dir.canonicalize()?,
            "runtime": config.runtime,
            "cache": config.cache,
            "cache_credential": credentials.cache,
            "bes": bes,
            "bes_credential": credentials.bes,
            "bos": config.bos,
            "source": config.source,
            "git_proxy": config.git_proxy,
            // The Git proxy captures this executable at daemon startup. When
            // disabled it is only a session setting and may vary per client.
            "git_proxy_executable": if config.git_proxy.repositories.is_empty() {
                None
            } else {
                Some(&config.session.git)
            },
            "reapi": {
                "connect_timeout_ms": p.connect_timeout_ms,
                "rpc_timeout_ms": p.rpc_timeout_ms,
                "stream_idle_timeout_ms": p.stream_idle_timeout_ms,
                "operation_timeout_ms": p.operation_timeout_ms,
                "background_timeout_ms": p.background_timeout_ms,
                "presence_ttl_seconds": p.presence_ttl_seconds,
                "retry": {
                    "max_attempts": p.retry.max_attempts,
                    "initial_backoff_ms": p.retry.initial_backoff_ms,
                    "max_backoff_ms": p.retry.max_backoff_ms,
                },
            },
        });
        // Preserve the pre-native identity for disabled configurations; only
        // settings captured by an enabled service affect daemon reuse.
        if !config.native.repositories.is_empty() {
            material["native"] = serde_json::to_value(&config.native)?;
            if config
                .native
                .repositories
                .values()
                .any(|repository| repository.remote.is_some())
            {
                material["native_credentials"] = serde_json::to_value(&credentials.native)?;
                let roots: std::collections::BTreeMap<_, _> = credentials
                    .native_ca
                    .iter()
                    .map(|(alias, bytes)| (alias, blake3::hash(bytes).to_hex().to_string()))
                    .collect();
                material["native_trust"] = serde_json::to_value(roots)?;
            }
        }
        let material = serde_json::to_vec(&material)?;
        // A fresh per-process key avoids a stable credential-derived identifier.
        // Only a match result is returned; the expected digest is never exposed.
        Ok(blake3::keyed_hash(&self.nonce, &material)
            .to_hex()
            .to_string())
    }
}

pub(crate) struct Identity {
    challenge: Challenge,
    digest: String,
}

impl Identity {
    pub fn new(config: &Config, credentials: &Credentials) -> Result<Self> {
        let challenge = Challenge::new();
        let digest = challenge.digest(config, credentials)?;
        Ok(Self { challenge, digest })
    }

    pub fn challenge(&self) -> &Challenge {
        &self.challenge
    }

    pub fn validate(&self, version: u32, digest: &str) -> Result<()> {
        ensure!(
            version == VERSION && digest == self.digest,
            "daemon configuration or credential scope mismatch; use the matching configuration to shut down the daemon, or select a separate state_dir"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_service_settings_and_credential_values_are_bound() -> Result<()> {
        let root = tempfile::tempdir()?;
        let mut config = Config::default();
        config.state_dir = root.path().to_owned();
        let credentials = Credentials {
            native_ca: Default::default(),
            native: Default::default(),
            cache: Some("synthetic-cache-credential".into()),
            bes: Some("synthetic-bes-credential".into()),
        };
        let identity = Identity::new(&config, &credentials)?;
        let challenge = identity.challenge();
        identity.validate(VERSION, &challenge.digest(&config, &credentials)?)?;
        type Change = (&'static str, fn(&mut Config));
        for (name, change) in [
            (
                "cache budget",
                (|c: &mut Config| c.cache.max_bytes += 1) as fn(&mut Config),
            ),
            ("cache reader admission", |c| {
                c.cache.max_concurrent_reads += 1
            }),
            ("daemon descriptor headroom", |c| {
                c.runtime.target_open_files += 1
            }),
            ("BES quota", |c| c.bes.max_pending_bytes += 1),
            ("BOS enablement", |c| c.bos.enabled = !c.bos.enabled),
            ("repository origin", |c| {
                c.git_proxy
                    .repositories
                    .insert("fixture".into(), "https://example.invalid/repo.git".into());
            }),
            ("RPC policy", |c| c.reapi.rpc_timeout_ms += 1),
            ("retry policy", |c| c.reapi.retry.max_attempts += 1),
            ("source disk budget", |c| {
                c.source.large_blob_spool_bytes = Some(0)
            }),
        ] as [Change; 9]
        {
            let mut candidate = config.clone();
            change(&mut candidate);
            assert!(
                identity
                    .validate(VERSION, &challenge.digest(&candidate, &credentials)?)
                    .is_err(),
                "{name}"
            );
        }
        for changed in [
            Credentials {
                native_ca: Default::default(),
                native: Default::default(),
                cache: None,
                bes: credentials.bes.clone(),
            },
            Credentials {
                native_ca: Default::default(),
                native: Default::default(),
                cache: credentials.cache.clone(),
                bes: None,
            },
            Credentials {
                native_ca: Default::default(),
                native: Default::default(),
                cache: Some("rotated-cache-credential".into()),
                bes: credentials.bes.clone(),
            },
            Credentials {
                native_ca: Default::default(),
                native: Default::default(),
                cache: credentials.cache.clone(),
                bes: Some("rotated-bes-credential".into()),
            },
        ] {
            assert!(
                identity
                    .validate(VERSION, &challenge.digest(&config, &changed)?)
                    .is_err()
            );
        }
        assert!(
            identity
                .validate(VERSION + 1, &challenge.digest(&config, &credentials)?)
                .is_err()
        );
        let other = Identity::new(&config, &credentials)?;
        assert!(
            other
                .validate(VERSION, &challenge.digest(&config, &credentials)?)
                .is_err()
        );
        let response = serde_json::to_string(challenge)?;
        assert!(!response.contains("credential"));
        assert!(!response.contains(&identity.digest));
        Ok(())
    }

    #[test]
    fn native_identity_binds_only_enabled_aliases_and_effective_limits() -> Result<()> {
        let root = tempfile::tempdir()?;
        let mut config = Config::default();
        config.state_dir = root.path().to_owned();
        let credentials = Credentials {
            native_ca: Default::default(),
            native: Default::default(),
            cache: None,
            bes: None,
        };
        let disabled = Identity::new(&config, &credentials)?;
        config.native.max_inflight += 1;
        config.native.max_memory_bytes += 1;
        disabled.validate(
            VERSION,
            &disabled.challenge().digest(&config, &credentials)?,
        )?;
        config
            .native
            .repositories
            .insert("example".into(), Default::default());
        assert!(
            disabled
                .validate(
                    VERSION,
                    &disabled.challenge().digest(&config, &credentials)?
                )
                .is_err()
        );
        let enabled = Identity::new(&config, &credentials)?;
        for change in [
            (|config: &mut Config| config.native.max_inflight += 1) as fn(&mut Config),
            |config| {
                config
                    .native
                    .repositories
                    .get_mut("example")
                    .unwrap()
                    .max_repository_bytes += 1
            },
            |config| {
                config
                    .native
                    .repositories
                    .insert("other".into(), Default::default());
            },
        ] {
            let mut changed = config.clone();
            change(&mut changed);
            assert!(
                enabled
                    .validate(
                        VERSION,
                        &enabled.challenge().digest(&changed, &credentials)?
                    )
                    .is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn native_remote_reuse_binds_effective_policy_and_credential_scope() -> Result<()> {
        let root = tempfile::tempdir()?;
        let mut config = Config::default();
        config.state_dir = root.path().to_owned();
        let mut repository = crate::config::NativeRepository::default();
        repository.remote = Some(jj_native_remote::Config::default());
        config
            .native
            .repositories
            .insert("example".into(), repository);
        let mut credentials = Credentials {
            native_ca: Default::default(),
            native: [("example".into(), Some("synthetic-native-credential".into()))].into(),
            cache: None,
            bes: None,
        };
        let identity = Identity::new(&config, &credentials)?;
        let challenge = identity.challenge();
        identity.validate(VERSION, &challenge.digest(&config, &credentials)?)?;
        let mut changed = config.clone();
        changed
            .native
            .repositories
            .get_mut("example")
            .unwrap()
            .remote
            .as_mut()
            .unwrap()
            .remote
            .rpc
            .rpc_timeout_ms += 1;
        assert!(
            identity
                .validate(VERSION, &challenge.digest(&changed, &credentials)?)
                .is_err()
        );
        credentials.native.insert(
            "example".into(),
            Some("synthetic-rotated-native-credential".into()),
        );
        assert!(
            identity
                .validate(VERSION, &challenge.digest(&config, &credentials)?)
                .is_err()
        );
        let response = serde_json::to_string(challenge)?;
        assert!(!response.contains("synthetic"));
        assert!(!response.contains("native"));
        Ok(())
    }

    #[test]
    fn native_remote_reuse_detects_replaced_trust_contents_at_the_same_path() -> Result<()> {
        let root = tempfile::tempdir()?;
        let ca = root.path().join("roots.pem");
        std::fs::write(&ca, b"first public trust snapshot")?;
        let mut config = Config::default();
        config.state_dir = root.path().to_owned();
        config.cache.api_key_env = "UNSET_NATIVE_FIXTURE_KEY".into();
        config.bes.enabled = false;
        let mut remote = jj_native_remote::Config::default();
        remote.remote.endpoint = "https://localhost:8093".into();
        remote.remote.tls = Some(toml::from_str(&format!("ca_certificate = {ca:?}"))?);
        let mut repository = crate::config::NativeRepository::default();
        repository.remote = Some(remote);
        config
            .native
            .repositories
            .insert("example".into(), repository);
        let before = Credentials::load(&config)?;
        let identity = Identity::new(&config, &before)?;
        std::fs::write(ca, b"replacement public trust snapshot")?;
        let after = Credentials::load(&config)?;
        assert_eq!(
            before.native_ca["example"].as_ref(),
            b"first public trust snapshot"
        );
        assert!(
            identity
                .validate(VERSION, &identity.challenge().digest(&config, &after)?)
                .is_err()
        );
        assert!(!serde_json::to_string(identity.challenge())?.contains("snapshot"));
        Ok(())
    }

    #[test]
    fn git_executable_is_service_configuration_only_when_proxy_is_enabled() -> Result<()> {
        let root = tempfile::tempdir()?;
        let mut config = Config::default();
        config.state_dir = root.path().to_owned();
        let credentials = Credentials {
            native_ca: Default::default(),
            native: Default::default(),
            cache: None,
            bes: None,
        };
        let disabled = Identity::new(&config, &credentials)?;
        config.session.git = "other-git".into();
        disabled.validate(
            VERSION,
            &disabled.challenge().digest(&config, &credentials)?,
        )?;

        config
            .git_proxy
            .repositories
            .insert("fixture".into(), "https://example.invalid/repo.git".into());
        let enabled = Identity::new(&config, &credentials)?;
        config.session.git = "different-git".into();
        assert!(
            enabled
                .validate(VERSION, &enabled.challenge().digest(&config, &credentials)?)
                .is_err()
        );
        config.session.git = "other-git".into();
        config.session.bazel = "different-bazel".into();
        config.session.git_vfs = "different-vfs".into();
        config.session.bazelrc = Some("/unused/fixture.bazelrc".into());
        enabled.validate(VERSION, &enabled.challenge().digest(&config, &credentials)?)?;
        Ok(())
    }
}
