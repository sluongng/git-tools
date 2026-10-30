//! Internal upload adapters. The embedding supplies authenticated context and
//! owns current policy, byte receipts and durable progression; parsing is not a
//! writer grant and scope requirements are not proof they have been satisfied.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use tinytree_forge_core::{
    accounts::{AccountId, ForgeActor, validate_principal},
    upload::{self, ForgeUploadRequest},
    upload_scope::{self, Context, ScopeError},
};

const MAX_SAFE: u64 = 9_007_199_254_740_991;
const MAX_METADATA: usize = 256 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    account_id: String,
    principal: String,
    operation: u32,
    view: String,
    request_id: String,
    payload_json: String,
}

pub fn normalize(input: &[u8]) -> Result<Value, String> {
    if input.len() > crate::MAX_INPUT {
        return Err("resource_exhausted: upload input".into());
    }
    let input: Command = serde_json::from_slice(input)
        .map_err(|_| "invalid_argument: upload command")?;
    let account = AccountId(input.account_id);
    account.validate().map_err(|_| "invalid_argument: upload account")?;
    validate_principal(&input.principal).map_err(|_| "invalid_argument: upload principal")?;
    if input.operation != 10 || input.view.len() > 128 || input.view.chars().any(char::is_control)
        || input.request_id.len() > 128
        || !input.request_id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err("invalid_argument: upload framing".into());
    }
    let request = upload::parse_request(input.payload_json.as_bytes())
        .map_err(|error| match error {
            tinytree_forge_core::Error::Limit(_) => "resource_exhausted: upload request",
            _ => "invalid_argument: upload request",
        })?
        .ok_or("unimplemented: unary upload")?;
    let selected = match &request {
        ForgeUploadRequest::Begin { size_bytes, .. } => {
            if *size_bytes > MAX_SAFE { return Err("resource_exhausted: upload size".into()); }
            Some(crate::hex(&upload::session_id(&account, &ForgeActor { principal: input.principal }, &input.request_id)
                .map_err(|_| "invalid_argument: upload request identity")?))
        }
        ForgeUploadRequest::Append { offset, .. } => {
            if *offset > MAX_SAFE { return Err("invalid_argument: upload offset".into()); }
            None
        }
        _ => None,
    };
    Ok(json!({"operation":10,"session_id":selected,"request":request}))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    kind: u8,
    id: String,
    size_bytes: u64,
    format_version: u16,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeInput {
    descriptor: Descriptor,
    bytes_base64: String,
    context: Context,
    cursor: usize,
    limit: usize,
}

pub fn scope_page(input: &[u8]) -> Result<Value, String> {
    if input.len() > crate::MAX_INPUT { return Err("resource_exhausted: upload scope input".into()); }
    let input: ScopeInput = serde_json::from_slice(input)
        .map_err(|_| "invalid_argument: upload scope input")?;
    let d = input.descriptor;
    if d.size_bytes > MAX_METADATA as u64 || input.bytes_base64.len() > MAX_METADATA.div_ceil(3) * 4 {
        return Err("resource_exhausted: upload scope metadata".into());
    }
    let key = crate::key(d.kind.into(), &d.id)?;
    let descriptor = tinytree_model::Descriptor { key, size_bytes: d.size_bytes, format_version: d.format_version };
    let bytes = STANDARD.decode(&input.bytes_base64).map_err(|_| "invalid_argument: upload scope base64")?;
    if STANDARD.encode(&bytes) != input.bytes_base64 { return Err("invalid_argument: upload scope base64".into()); }
    let page = upload_scope::page_bytes(descriptor, &bytes, input.context, input.cursor, input.limit,
        tinytree_model::Limits { max_object_bytes: MAX_METADATA, max_items: 100_000,
            max_field_bytes: MAX_METADATA, max_path_bytes: 4096 })
        .map_err(|error| match error {
            ScopeError::AccessDenied => "permission_denied: upload scope",
            ScopeError::Limit(_) => "resource_exhausted: upload scope",
            _ => "invalid_argument: upload scope metadata",
        })?;
    serde_json::to_value(page).map_err(|_| "invalid_argument: upload scope response".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_upload_normalization_binds_host_principal_without_accepting_public_grants() {
        let payload = json!({"action":"begin","version":vec![1u8;32],"path":"src/file","kind":1,"id":vec![2u8;32],"size_bytes":3});
        let mut command = json!({"account_id":"example","principal":"writer","operation":10,"view":"public","request_id":"upload-1","payload_json":payload.to_string()});
        let first = normalize(&serde_json::to_vec(&command).unwrap()).unwrap();
        let expected = upload::session_id(&AccountId("example".into()), &ForgeActor {principal:"writer".into()}, "upload-1").unwrap();
        assert_eq!(first["session_id"], crate::hex(&expected));
        command["principal"] = json!("other");
        assert_ne!(first["session_id"], normalize(&serde_json::to_vec(&command).unwrap()).unwrap()["session_id"]);
        let mut granted = payload;
        granted["actor"] = json!("owner");
        command["payload_json"] = json!(granted.to_string());
        assert!(normalize(&serde_json::to_vec(&command).unwrap()).is_err());
        command["payload_json"] = json!(json!({"action":"append","session_id":vec![1u8;32],"offset":MAX_SAFE+1,"data":[1]}).to_string());
        assert!(normalize(&serde_json::to_vec(&command).unwrap()).is_err());
    }
}
