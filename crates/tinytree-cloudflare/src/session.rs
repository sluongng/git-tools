//! Host-only transport identity derivation. The public Forge request remains
//! strict empty JSON; these inputs are never accepted as caller authority.
use serde_json::Value;
use tinytree_forge_core::session::{SessionTrustedInput, derive_session};

pub fn derive(input: &[u8]) -> Result<Value, String> {
    if input.len() > 64 * 1024 {
        return Err("session input limit".into());
    }
    let input: SessionTrustedInput = serde_json::from_slice(input)
        .map_err(|_| "invalid session input".to_owned())?;
    let session = derive_session(input).map_err(|error| error.to_string())?;
    serde_json::to_value(session).map_err(|_| "session encoding".to_owned())
}
