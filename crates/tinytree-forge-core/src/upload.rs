//! Storage-free resumable object-upload wire contract. Parsing and session IDs
//! are not writer, path, byte-ownership, closure or retention authority.
use crate::{Error, Result, accounts::{AccountId, ForgeActor}};
use serde::{Deserialize, Serialize, de::{self, IgnoredAny, MapAccess, Visitor}};
use std::{collections::BTreeSet, fmt};

pub const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ForgeUploadRequest {
    Begin { version: [u8;32], path: String, kind: u8, id: [u8;32], size_bytes: u64 },
    Append { session_id: [u8;32], offset: u64, data: Vec<u8> },
    Status { session_id: [u8;32] },
    Finalize { session_id: [u8;32] },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ForgeUploadState { Pending, Committed }
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForgeUploadedObject {
    pub kind: u8,
    pub id: [u8;32],
    pub size_bytes: u64,
    pub format_version: u16,
    pub staging_lease: String,
    pub expires_at: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ForgeUploadStatus {
    pub session_id: [u8;32],
    pub offset: u64,
    pub state: ForgeUploadState,
    pub receipt: Option<ForgeUploadedObject>,
}

/// Preserve the native request-key alphabet; the same key legitimately appears
/// across a session's actions, so it is not a whole-payload mutation replay key.
pub fn validate_request_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128
        || !value.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)) {
        return Err(Error::Invalid("upload request identity"));
    }
    Ok(())
}
/// Exact native v1 identity. Full immutable view/path/descriptor binding must be
/// compared separately by the authoritative host before any replay is accepted.
pub fn session_id(account: &AccountId, actor: &ForgeActor, request: &str) -> Result<[u8;32]> {
    validate_request_id(request)?;
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-forge-upload-session-v1\0");
    for part in [account.0.as_bytes(), actor.principal.as_bytes(), request.as_bytes()] {
        h.update(&(part.len() as u64).to_le_bytes());
        h.update(part);
    }
    Ok(*h.finalize().as_bytes())
}

struct ActionProbe;
impl<'de> Visitor<'de> for ActionProbe {
    type Value = bool;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("an upload request object") }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<bool,A::Error> {
        let mut fields = BTreeSet::new();
        let mut action = false;
        while let Some(key) = map.next_key::<String>()? {
            if fields.len() >= 16 || !fields.insert(key.clone()) { return Err(de::Error::custom("duplicate upload field")); }
            action |= key == "action";
            map.next_value::<IgnoredAny>()?;
        }
        Ok(action)
    }
}
/// None means only an actual JSON object with no action field: the caller must
/// still use its real strict legacy unary decoder. A malformed/present action
/// never selects legacy behavior. The envelope is bounded before allocation.
pub fn parse_request(bytes: &[u8]) -> Result<Option<ForgeUploadRequest>> {
    if bytes.len() > MAX_REQUEST_BYTES { return Err(Error::Limit("upload request limit")); }
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let action = serde::de::Deserializer::deserialize_map(&mut decoder, ActionProbe)
        .map_err(|_| Error::Invalid("upload request"))?;
    decoder.end().map_err(|_| Error::Invalid("upload request"))?;
    if !action { return Ok(None); }
    let request: ForgeUploadRequest = serde_json::from_slice(bytes)
        .map_err(|_| Error::Invalid("upload session request"))?;
    match &request {
        ForgeUploadRequest::Begin { kind, .. } => {
            tinytree_model::Kind::try_from(*kind).map_err(|_| Error::Invalid("upload object kind"))?;
        }
        ForgeUploadRequest::Append { data, .. } if data.is_empty() || data.len() > MAX_CHUNK_BYTES => {
            return Err(Error::Limit("upload chunk size"));
        }
        _ => {}
    }
    Ok(Some(request))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn native_session_identity_preserves_framing_and_separate_actor_keys() {
        let account = AccountId("example".into());
        let actor = ForgeActor { principal: "writer".into() };
        let framed = b"tinytree-forge-upload-session-v1\0\x07\0\0\0\0\0\0\0example\x06\0\0\0\0\0\0\0writer\x08\0\0\0\0\0\0\0upload-1";
        assert_eq!(session_id(&account,&actor,"upload-1").unwrap(),tinytree_model::file_id(framed).0);
        assert_ne!(session_id(&account,&actor,"upload-1").unwrap(),session_id(&account,&ForgeActor{principal:"reader".into()},"upload-1").unwrap());
        assert!(session_id(&account,&actor,"bad/key").is_err());
    }
    #[test]
    fn session_parser_is_strict_without_stealing_actual_legacy_uploads() {
        assert_eq!(parse_request(br#"{"path":"README.md","data":[1,2]}"#).unwrap(),None);
        let status = json!({"action":"status","session_id":vec![1u8;32]});
        assert_eq!(parse_request(&serde_json::to_vec(&status).unwrap()).unwrap(),Some(ForgeUploadRequest::Status{session_id:[1;32]}));
        for bytes in [br#"null"#.as_slice(),br#"[]"#,br#"{"action":null}"#,br#"{"action":"unknown"}"#,
            br#"{"action":"status","action":"status"}"#,br#"{"data":[],"data":[]}"#,br#"{}{}"#] {
            assert!(parse_request(bytes).is_err());
        }
        let extra = json!({"action":"status","session_id":vec![1u8;32],"actor":"writer"});
        assert!(parse_request(&serde_json::to_vec(&extra).unwrap()).is_err());
        let chunk = json!({"action":"append","session_id":vec![1u8;32],"offset":0,"data":vec![0;MAX_CHUNK_BYTES+1]});
        assert!(matches!(parse_request(&serde_json::to_vec(&chunk).unwrap()),Err(Error::Limit("upload chunk size"))));
        assert!(matches!(parse_request(&vec![b' ';MAX_REQUEST_BYTES+1]),Err(Error::Limit("upload request limit"))));
    }
    #[test]
    fn native_status_wire_round_trips_pending_and_committed_receipts() {
        let mut status = ForgeUploadStatus { session_id:[7;32],offset:9,state:ForgeUploadState::Pending,receipt:None };
        assert_eq!(serde_json::to_value(&status).unwrap(),json!({"session_id":vec![7u8;32],"offset":9,"state":"pending","receipt":null}));
        status.state=ForgeUploadState::Committed;
        status.receipt=Some(ForgeUploadedObject{kind:1,id:[8;32],size_bytes:9,format_version:1,staging_lease:"lease".into(),expires_at:123});
        assert_eq!(serde_json::from_value::<ForgeUploadStatus>(serde_json::to_value(&status).unwrap()).unwrap(),status);
    }
}
