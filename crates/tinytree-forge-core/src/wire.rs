//! Ordinary HTTPS framing of the existing Forge operation envelope. Parsing is
//! not authentication or operation authorization; storage adapters supply those.
use crate::{Error, Result, accounts::AccountId};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

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

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub version: u32,
    pub operation: i32,
    pub account_id: String,
    pub view: String,
    pub request_id: String,
    pub state: String,
    pub result_json: String,
}

fn validate_frame(
    version: u32,
    operation: i32,
    account: &str,
    view: &str,
    request: &str,
) -> Result<()> {
    if version != 1
        || operation <= 0
        || view.len() > 128
        || request.len() > 128
        || view.chars().any(char::is_control)
        || !request
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(Error::Invalid("forge HTTP envelope"));
    }
    AccountId(account.to_owned()).validate()
}

struct ResponseBody(Vec<u8>);
impl Write for ResponseBody {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_RESPONSE_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("forge HTTP response byte limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Reply {
    fn validate(&self) -> Result<()> {
        validate_frame(
            self.version,
            self.operation,
            &self.account_id,
            &self.view,
            &self.request_id,
        )?;
        if self.state.is_empty()
            || self.state.len() > 64
            || !self
                .state
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b"_-".contains(&b))
            || self.result_json.is_empty()
        {
            return Err(Error::Invalid("forge HTTP reply"));
        }
        if self.result_json.len() > MAX_RESPONSE_BYTES {
            return Err(Error::Limit("forge HTTP response bytes"));
        }
        Ok(())
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err(Error::Limit("forge HTTP response bytes"));
        }
        let reply: Self =
            serde_json::from_slice(bytes).map_err(|_| Error::Invalid("forge HTTP reply"))?;
        reply.validate()?;
        Ok(reply)
    }
    /// Enforce the encoded ceiling while writing, including nested JSON escapes.
    /// A valid opaque payload is not authority or a concrete operation result.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut body = ResponseBody(Vec::new());
        serde_json::to_writer(&mut body, self)
            .map_err(|_| Error::Limit("forge HTTP response bytes"))?;
        Ok(body.0)
    }
}

impl Call {
    /// The encoded JSON ceiling is independent of the semantic operation's
    /// limits. Reject before decoding, including escaped string expansion.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_REQUEST_BYTES {
            return Err(Error::Limit("forge HTTP request bytes"));
        }
        let call: Self =
            serde_json::from_slice(bytes).map_err(|_| Error::Invalid("forge HTTP envelope"))?;
        validate_frame(
            call.version,
            call.operation,
            &call.account_id,
            &call.view,
            &call.request_id,
        )?;
        if call.payload_json.is_empty() {
            return Err(Error::Invalid("forge HTTP envelope"));
        }
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
        assert_eq!(
            Call::decode(&serde_json::to_vec(&call).unwrap()).unwrap(),
            call
        );
        for bad in [
            String::from_utf8(input.to_vec())
                .unwrap()
                .replace("\"operation\":20", "\"operation\":20,\"operation\":21"),
            String::from_utf8(input.to_vec())
                .unwrap()
                .replace("\"version\":1", "\"version\":1,\"actor\":\"admin\""),
            String::from_utf8(input.to_vec())
                .unwrap()
                .replace("\"version\":1", "\"version\":2"),
            String::from_utf8(input.to_vec())
                .unwrap()
                .replace("saved-1", "bad/intent"),
        ] {
            assert!(Call::decode(bad.as_bytes()).is_err());
        }
        assert!(Call::decode(&[0xff]).is_err());
        assert_eq!(
            Call::decode(&vec![b' '; MAX_REQUEST_BYTES + 1]),
            Err(Error::Limit("forge HTTP request bytes"))
        );
    }

    #[test]
    fn reply_roundtrip_rejects_ambiguous_fields_and_bounds_nested_json_escaping() {
        let reply = Reply {
            version: 1,
            operation: 6,
            account_id: "sample".into(),
            view: "public".into(),
            request_id: "read-1".into(),
            state: "pending".into(),
            result_json: "{\"progress\":1}".into(),
        };
        let bytes = reply.encode().unwrap();
        assert_eq!(Reply::decode(&bytes).unwrap(), reply);
        let encoded = String::from_utf8(bytes).unwrap();
        for bad in [
            encoded.replace("\"version\":1", "\"version\":1,\"version\":1"),
            encoded.replace("\"version\":1", "\"version\":1,\"grant\":true"),
            encoded.replace("\"pending\"", "\"\""),
        ] {
            assert!(Reply::decode(bad.as_bytes()).is_err());
        }
        let mut too_large = reply;
        // The semantic string fits, but escaping it must not allocate an
        // unbounded encoded envelope before the transport can reject it.
        too_large.result_json = "\"".repeat(MAX_RESPONSE_BYTES / 2 + 1);
        assert_eq!(
            too_large.encode(),
            Err(Error::Limit("forge HTTP response bytes"))
        );
        assert_eq!(
            Reply::decode(&vec![b' '; MAX_RESPONSE_BYTES + 1]),
            Err(Error::Limit("forge HTTP response bytes"))
        );
    }
}
