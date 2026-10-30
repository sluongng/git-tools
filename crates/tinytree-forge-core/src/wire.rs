//! Ordinary HTTPS framing of the existing Forge operation envelope. Parsing is
//! not authentication or operation authorization; storage adapters supply those.
use crate::{Error, Result, accounts::AccountId};
use serde::{Deserialize, Serialize};

pub const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Call {
    pub version: u32,
    pub operation: i32,
    pub account_id: String,
    pub view: String,
    pub request_id: String,
    pub payload_json: String,
}

impl Call {
    /// The encoded JSON ceiling is independent of the semantic operation's
    /// limits. Reject before decoding, including escaped string expansion.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_REQUEST_BYTES {
            return Err(Error::Limit("forge HTTP request bytes"));
        }
        let call: Self = serde_json::from_slice(bytes)
            .map_err(|_| Error::Invalid("forge HTTP envelope"))?;
        if call.version != 1
            || call.operation <= 0
            || call.view.len() > 128
            || call.request_id.len() > 128
            || call.view.chars().any(char::is_control)
            || !call.request_id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || call.payload_json.is_empty()
        {
            return Err(Error::Invalid("forge HTTP envelope"));
        }
        AccountId(call.account_id.clone()).validate()?;
        Ok(call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_opaque_operation_payload_and_rejects_ambiguous_envelopes() {
        let input = br#"{"version":1,"operation":20,"account_id":"sample","view":"public","request_id":"saved-1","payload_json":"{\"x\":1}"}"#;
        let call = Call::decode(input).unwrap();
        assert_eq!(call.payload_json, "{\"x\":1}");
        assert_eq!(Call::decode(&serde_json::to_vec(&call).unwrap()).unwrap(), call);
        for bad in [
            String::from_utf8(input.to_vec()).unwrap().replace("\"operation\":20", "\"operation\":20,\"operation\":21"),
            String::from_utf8(input.to_vec()).unwrap().replace("\"version\":1", "\"version\":1,\"actor\":\"admin\""),
            String::from_utf8(input.to_vec()).unwrap().replace("\"version\":1", "\"version\":2"),
            String::from_utf8(input.to_vec()).unwrap().replace("saved-1", "bad/intent"),
        ] {
            assert!(Call::decode(bad.as_bytes()).is_err());
        }
        assert!(Call::decode(&[0xff]).is_err());
        assert_eq!(Call::decode(&vec![b' '; MAX_REQUEST_BYTES + 1]), Err(Error::Limit("forge HTTP request bytes")));
    }
}
