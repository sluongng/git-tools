use crate::config::ConfigError;
use std::collections::{HashMap, HashSet};
use tonic::{Status, metadata::MetadataMap};

#[derive(Clone)]
pub struct Principal {
    pub id: String,
    pub repositories: Vec<String>,
    pub write: bool,
    pub retain: bool,
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
            token_hash: *blake3::hash(token.as_bytes()).as_bytes(),
        })
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
        })
    }
}
pub(crate) struct Identity {
    pub id: String,
    repositories: HashSet<String>,
    write: bool,
    retain: bool,
}
impl Identity {
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
