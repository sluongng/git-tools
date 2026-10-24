use crate::config::ConfigError;
use std::collections::{HashMap, HashSet};
use tonic::{Status, metadata::MetadataMap};

#[derive(Clone)]
pub struct Principal {
    pub id: String,
    pub repositories: Vec<String>,
    pub write: bool,
    pub retain: bool,
    pub cloud: bool,
    pub landing_profiles: Vec<String>,
    token_hash: [u8; 32],
}
impl Principal {
    /// Token bytes are immediately hashed and never exposed by Debug or errors.
    pub fn new(
        id: impl Into<String>,
        token: &str,
        repositories: Vec<String>,
        write: bool,
    ) -> Result<Self, ConfigError> {
        let id = id.into();
        if id.is_empty()
            || id.len() > 128
            || !(16..=4096).contains(&token.len())
            || !token.bytes().all(|b| b.is_ascii_graphic())
            || repositories.is_empty()
            || repositories
                .iter()
                .any(|r| tinytree_store::validate_repository_id(r).is_err())
        {
            return Err(ConfigError("invalid native principal"));
        }
        Ok(Self {
            id,
            repositories,
            write,
            retain: false,
            cloud: false,
            landing_profiles: Vec::new(),
            token_hash: *blake3::hash(token.as_bytes()).as_bytes(),
        })
    }
    /// Explicit launch grants; repository write alone grants no landing work.
    pub fn with_landing_profiles(mut self, profiles: Vec<String>) -> Self {
        self.landing_profiles = profiles;
        self
    }
    /// Private workspace history is an independent opt-in capability.
    pub fn with_cloud(mut self, enabled: bool) -> Self {
        self.cloud = enabled;
        self
    }
    /// Retention grants no upload or public-reference write permission.
    pub fn with_retention(mut self, enabled: bool) -> Self {
        self.retain = enabled;
        self
    }
}
#[derive(Clone)]
pub(crate) struct Authorization {
    principals: HashMap<[u8; 32], Principal>,
    repositories: HashSet<String>,
}
impl Authorization {
    pub fn enabled(&self) -> bool {
        !self.principals.is_empty()
    }
    pub fn new(principals: Vec<Principal>, repositories: &[String]) -> Result<Self, ConfigError> {
        let mut result = Self {
            principals: HashMap::new(),
            repositories: repositories.iter().cloned().collect(),
        };
        let mut ids = HashSet::new();
        for principal in principals {
            if principal.landing_profiles.len() > 32
                || principal.landing_profiles.iter().any(|p| {
                    p.is_empty()
                        || p.len() > 64
                        || !p
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                })
                || principal
                    .landing_profiles
                    .iter()
                    .collect::<HashSet<_>>()
                    .len()
                    != principal.landing_profiles.len()
            {
                return Err(ConfigError("invalid landing profile grants"));
            }
            if !ids.insert(principal.id.clone())
                || principal
                    .repositories
                    .iter()
                    .any(|r| !result.repositories.contains(r))
                || result
                    .principals
                    .insert(principal.token_hash, principal)
                    .is_some()
            {
                return Err(ConfigError("duplicate or invalid native principal"));
            }
        }
        Ok(result)
    }
    pub fn identify(&self, metadata: &MetadataMap) -> Result<Identity, Status> {
        if self.principals.is_empty() {
            return Ok(Identity {
                id: "local-owner".into(),
                repositories: self.repositories.clone(),
                write: true,
                retain: true,
                cloud: true,
                authenticated: false,
                landing_profiles: Vec::new(),
            });
        }
        let header = metadata
            .get("authorization")
            .and_then(|s| s.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("native authorization required"))?;
        let token = header
            .strip_prefix("Bearer ")
            .filter(|v| v.len() <= 4096)
            .ok_or_else(|| Status::unauthenticated("invalid native authorization"))?;
        let hash = *blake3::hash(token.as_bytes()).as_bytes();
        let p = self
            .principals
            .get(&hash)
            .ok_or_else(|| Status::unauthenticated("invalid native authorization"))?;
        Ok(Identity {
            id: p.id.clone(),
            repositories: p.repositories.iter().cloned().collect(),
            write: p.write,
            retain: p.retain,
            cloud: p.cloud,
            authenticated: true,
            landing_profiles: p.landing_profiles.clone(),
        })
    }
}
pub(crate) struct Identity {
    pub id: String,
    repositories: HashSet<String>,
    write: bool,
    retain: bool,
    cloud: bool,
    authenticated: bool,
    landing_profiles: Vec<String>,
}
impl Identity {
    pub fn landing_owner_hash(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-landing-owner-v1\0");
        hash.update(&[u8::from(self.authenticated)]);
        hash.update(&crate::retention::owner_hash(self));
        *hash.finalize().as_bytes()
    }
    pub fn landing_capability(&self, local: &[String]) -> Result<(), Status> {
        let profiles = if self.authenticated {
            &self.landing_profiles
        } else {
            local
        };
        if !self.write || profiles.is_empty() {
            return Err(Status::permission_denied("landing launch access denied"));
        }
        Ok(())
    }
    pub fn authorize_landing(
        &self,
        repo: &str,
        profile: &str,
        local: &[String],
    ) -> Result<(), Status> {
        self.authorize(repo, true)?;
        self.landing_capability(local)?;
        let profiles = if self.authenticated {
            &self.landing_profiles
        } else {
            local
        };
        if !profiles.iter().any(|p| p == profile) {
            return Err(Status::permission_denied("landing profile access denied"));
        }
        Ok(())
    }
    pub fn cloud_capability(&self) -> Result<(), Status> {
        if !self.cloud {
            return Err(Status::permission_denied("private workspace access denied"));
        }
        Ok(())
    }
    pub fn cloud_write_capability(&self) -> Result<(), Status> {
        self.cloud_capability()?;
        if !self.write {
            return Err(Status::permission_denied(
                "private workspace write access denied",
            ));
        }
        Ok(())
    }
    pub fn authorize_cloud(&self, repo: &str, write: bool) -> Result<(), Status> {
        self.cloud_capability()?;
        self.authorize(repo, write)
    }

    pub fn authorize_retention(&self, repo: &str) -> Result<(), Status> {
        self.authorize(repo, false)?;
        if !self.retain {
            return Err(Status::permission_denied("native retention access denied"));
        }
        Ok(())
    }
    pub fn authorize(&self, repo: &str, write: bool) -> Result<(), Status> {
        if !self.repositories.contains(repo) || (write && !self.write) {
            return Err(Status::permission_denied("native repository access denied"));
        }
        Ok(())
    }
}
