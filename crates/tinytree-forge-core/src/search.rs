//! Shared bounded view-search wire contract. Cursors select derived read state;
//! they never authenticate a caller or grant access to an object or path.
use serde::{Deserialize, Serialize};

pub const MAX_QUERY_BYTES: usize = 1024;
pub const MAX_MATCHES: usize = 100;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForgeSearchRequest {
    #[serde(default)]
    pub version: Option<[u8; 32]>,
    pub query: String,
    #[serde(default)]
    pub regex: bool,
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}
impl ForgeSearchRequest {
    /// The actual native pattern compiler still validates the query, including
    /// empty patterns and unsupported regex constructs, even for empty views.
    pub fn validate(&self) -> crate::Result<()> {
        if self.query.len() > MAX_QUERY_BYTES
            || self.query.contains('\n')
            || self.limit == 0
            || self.limit > MAX_MATCHES
            || self.version == Some([0; 32])
            || self.cursor.as_ref().is_some_and(|cursor| {
                self.version.is_none()
                    || cursor.is_empty()
                    || cursor.len() > 128
                    || !cursor
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
            })
        {
            return Err(crate::Error::Invalid("search selectors"));
        }
        Ok(())
    }
}

/// Same normalized query intent on every backend; principal, policy epoch,
/// selected view version, revision and exact projection/catalog are HOST bindings.
/// The page-input token itself is deliberately not part of this fingerprint.
pub fn query_fingerprint(
    account: &str,
    view: &str,
    request: &ForgeSearchRequest,
) -> crate::Result<String> {
    request.validate()?;
    let bytes = serde_json::to_vec(&(
        1u32,
        account,
        view,
        &request.query,
        request.regex,
        request.limit,
    ))
    .map_err(|_| crate::Error::Invalid("search encoding"))?;
    Ok(tinytree_model::file_id(&bytes)
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForgeSearchHit {
    pub path: String,
    pub line: u64,
    pub column: u64,
    pub preview: String,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkippedFiles {
    pub binary: u64,
    pub oversized: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForgeSearchResponse {
    pub account_id: String,
    pub view: String,
    pub view_version: [u8; 32],
    pub revision: [u8; 32],
    pub policy_epoch: u64,
    pub root_prefix: String,
    pub matches: Vec<ForgeSearchHit>,
    pub complete: bool,
    pub cursor: Option<String>,
    pub skipped_files: SkippedFiles,
}
