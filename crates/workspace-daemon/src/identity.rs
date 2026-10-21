//! Reuse identity for cooperating clients of a private, same-user control socket.
//! This is configuration binding, not authentication against hostile same-UID code.
use crate::config::Config;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

const VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Challenge {
    pub version: u32,
    pub nonce: [u8; 32],
}

pub(crate) struct Credentials {
    pub cache: Option<String>,
    pub bes: Option<String>,
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
        Ok(Self {
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
            "daemon identity version mismatch; use the matching configuration to shut down and restart the daemon"
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
        let material = serde_json::to_vec(&serde_json::json!({
            "version": VERSION,
            "state_dir": config.state_dir.canonicalize()?,
            "cache": config.cache,
            "cache_credential": credentials.cache,
            "bes": bes,
            "bes_credential": credentials.bes,
            "bos": config.bos,
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
        }))?;
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
            ("BES quota", |c| c.bes.max_pending_bytes += 1),
            ("BOS enablement", |c| c.bos.enabled = !c.bos.enabled),
            ("repository origin", |c| {
                c.git_proxy
                    .repositories
                    .insert("fixture".into(), "https://example.invalid/repo.git".into());
            }),
            ("RPC policy", |c| c.reapi.rpc_timeout_ms += 1),
            ("retry policy", |c| c.reapi.retry.max_attempts += 1),
        ] as [Change; 6]
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
                cache: None,
                bes: credentials.bes.clone(),
            },
            Credentials {
                cache: credentials.cache.clone(),
                bes: None,
            },
            Credentials {
                cache: Some("rotated-cache-credential".into()),
                bes: credentials.bes.clone(),
            },
            Credentials {
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
    fn git_executable_is_service_configuration_only_when_proxy_is_enabled() -> Result<()> {
        let root = tempfile::tempdir()?;
        let mut config = Config::default();
        config.state_dir = root.path().to_owned();
        let credentials = Credentials {
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
