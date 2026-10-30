//! Strict bootstrap-import intent normalization. These commands are not source
//! freeze/provenance proofs, ownership grants or canonical object receipts.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tinytree_forge_core::{account_commands, accounts::AccountId};
use tinytree_model as model;
const MAX_INPUT: usize = 2 * 1024 * 1024;
const MAX_FILE: usize = 1024 * 1024;
const MAX_METADATA: usize = 256 * 1024;
const MAX_SAFE: u64 = 9_007_199_254_740_991;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    account_id: String,
    operation: u32,
    view: String,
    request_id: String,
    payload_json: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Key {
    kind: u8,
    id: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Source {
    repository_id: String,
    reference: String,
    generation: u64,
    head: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Destination {
    generation: u64,
    head: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Begin {
    root: Key,
    source: Source,
    destination: Destination,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    kind: u8,
    id: String,
    size_bytes: u64,
    format_version: u16,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Put {
    import_id: String,
    descriptor: Descriptor,
    data_base64: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    import_id: String,
    expected_generation: u64,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Get {
    import_id: String,
}
fn id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn text(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}
fn decode<T: serde::de::DeserializeOwned>(payload: &str) -> Result<T, String> {
    serde_json::from_str(payload).map_err(|_| "invalid_argument: import payload".into())
}
fn sextet(value: u8) -> Option<u8> {
    match value {
        b'A'..=b'Z' => Some(value - b'A'),
        b'a'..=b'z' => Some(value - b'a' + 26),
        b'0'..=b'9' => Some(value - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}
/// Validate canonical padding and unused bits WITHOUT allocating decoded bytes.
/// The host still decodes and hashes original bytes with native inspect.
fn base64_length(value: &str, maximum: usize) -> Result<usize, String> {
    if value.len() > maximum.div_ceil(3) * 4 {
        return Err("resource_exhausted: import object bytes".into());
    }
    if value.len() % 4 != 0 {
        return Err("invalid_argument: canonical base64".into());
    }
    if value.is_empty() {
        return Ok(0);
    }
    let bytes = value.as_bytes();
    let padding = value.len() - value.trim_end_matches('=').len();
    if padding > 2 {
        return Err("invalid_argument: canonical base64".into());
    }
    let content = &bytes[..bytes.len() - padding];
    if content.iter().any(|b| sextet(*b).is_none()) {
        return Err("invalid_argument: canonical base64".into());
    }
    let last = content
        .last()
        .and_then(|b| sextet(*b))
        .ok_or("invalid_argument: canonical base64")?;
    if (padding == 2 && last & 15 != 0) || (padding == 1 && last & 3 != 0) {
        return Err("invalid_argument: canonical base64 padding".into());
    }
    let length = value.len() / 4 * 3 - padding;
    if length > maximum {
        return Err("resource_exhausted: import object bytes".into());
    }
    Ok(length)
}
pub fn normalize(bytes: &[u8]) -> Result<Value, String> {
    if bytes.len() > MAX_INPUT {
        return Err("resource_exhausted: import input bytes".into());
    }
    let input: Input =
        serde_json::from_slice(bytes).map_err(|_| "invalid_argument: import envelope")?;
    let account = AccountId(input.account_id);
    account
        .validate()
        .map_err(|_| "invalid_argument: import account")?;
    if input.view.len() > 128
        || input.view.chars().any(char::is_control)
        || input.request_id.len() > 128
        || !input
            .request_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        || (input.operation != 43 && input.request_id.is_empty())
    {
        return Err("invalid_argument: import framing".into());
    }
    let (payload, fingerprint_payload) = match input.operation {
        40 => {
            let payload: Begin = decode(&input.payload_json)?;
            if payload.root.kind != 3
                || !id(&payload.root.id)
                || payload.root.id == "0".repeat(64)
                || payload.source.head != payload.root.id
                || !text(&payload.source.repository_id, 128)
                || !payload
                    .source
                    .repository_id
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-_".contains(&b))
                || payload.source.reference != "main"
                || payload.source.generation == 0
                || payload.source.generation > MAX_SAFE
                || payload.destination.generation != 1
                || !id(&payload.destination.head)
            {
                return Err("invalid_argument: bootstrap bindings".into());
            }
            let genesis = account_commands::genesis(&account, model::Limits::default())
                .map_err(|_| "invalid_argument: account genesis")?;
            if payload.destination.head != hex(&genesis.commit.descriptor.key.id.0) {
                return Err("invalid_argument: bootstrap genesis".into());
            }
            let value =
                serde_json::to_value(payload).map_err(|_| "invalid_argument: import encoding")?;
            (value.clone(), value)
        }
        41 => {
            let payload: Put = decode(&input.payload_json)?;
            let d = &payload.descriptor;
            if !uuid(&payload.import_id)
                || model::Kind::try_from(d.kind).is_err()
                || !id(&d.id)
                || d.format_version != model::FORMAT_VERSION
                || (d.kind == 3 && d.id == "0".repeat(64))
            {
                return Err("invalid_argument: import object descriptor".into());
            }
            let maximum = if d.kind == 1 { MAX_FILE } else { MAX_METADATA };
            let actual = base64_length(&payload.data_base64, maximum)?;
            if d.size_bytes != actual as u64 {
                return Err("invalid_argument: import object size".into());
            }
            if input.request_id != format!("io-{}-{}-{}", payload.import_id, d.kind, d.id) {
                return Err("invalid_argument: import object request identity".into());
            }
            let intent = json!({"import_id":payload.import_id,"descriptor":payload.descriptor});
            let value =
                serde_json::to_value(payload).map_err(|_| "invalid_argument: import encoding")?;
            (value, intent)
        }
        42 | 44 => {
            let payload: Progress = decode(&input.payload_json)?;
            if !uuid(&payload.import_id) || payload.expected_generation > MAX_SAFE {
                return Err("invalid_argument: import progress".into());
            }
            // Deterministic step identity prevents changed-input reuse without
            // allocating a generic immutable receipt for every progress page.
            if input.operation == 42
                && input.request_id
                    != format!("ia-{}-{}", payload.import_id, payload.expected_generation)
            {
                return Err("invalid_argument: import advance request identity".into());
            }
            let value =
                serde_json::to_value(payload).map_err(|_| "invalid_argument: import encoding")?;
            (value.clone(), value)
        }
        43 => {
            let payload: Get = decode(&input.payload_json)?;
            if !uuid(&payload.import_id) {
                return Err("invalid_argument: import identity".into());
            }
            let value =
                serde_json::to_value(payload).map_err(|_| "invalid_argument: import encoding")?;
            (value.clone(), value)
        }
        _ => return Err("unimplemented: import operation".into()),
    };
    let intent = serde_json::to_vec(
        &json!({"account_id":account.0,"operation":input.operation,"payload":fingerprint_payload}),
    )
    .map_err(|_| "invalid_argument: import intent")?;
    // Native model file_id is the canonical raw unkeyed BLAKE3 primitive. This
    // value identifies an intent; it is NOT an object descriptor or byte receipt.
    let fingerprint = hex(&model::file_id(&intent).0);
    Ok(json!({"operation":input.operation,"fingerprint":fingerprint,"payload":payload}))
}
#[cfg(test)]
mod tests {
    use super::*;
    fn request(operation: u32, request_id: &str, payload: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"account_id":"sample","operation":operation,"view":"public","request_id":request_id,"payload_json":payload.to_string()})).unwrap()
    }
    #[test]
    fn bootstrap_binds_exact_genesis_and_preserves_pinned_source_declaration() {
        let genesis =
            account_commands::genesis(&AccountId("sample".into()), model::Limits::default())
                .unwrap();
        let payload = json!({"root":{"kind":3,"id":"1".repeat(64)},"source":{"repository_id":"source","reference":"main","generation":21,"head":"1".repeat(64)},"destination":{"generation":1,"head":hex(&genesis.commit.descriptor.key.id.0)}});
        let output = normalize(&request(40, "begin", payload.clone())).unwrap();
        assert_eq!(output["payload"], payload);
        let mut wrong = payload.clone();
        wrong["destination"]["generation"] = json!(2);
        assert!(normalize(&request(40, "begin", wrong)).is_err());
        let mut wrong = payload;
        wrong["source"]["head"] = json!("2".repeat(64));
        assert!(normalize(&request(40, "begin", wrong)).is_err());
        let mut root: Value = serde_json::from_slice(&request(
            43,
            "",
            json!({"import_id":"12345678-1234-1234-1234-123456789abc"}),
        ))
        .unwrap();
        root["actor"] = json!("owner");
        assert!(normalize(&serde_json::to_vec(&root).unwrap()).is_err());
    }
    #[test]
    fn canonical_base64_and_descriptor_namespaces_are_strict() {
        for (encoded, length) in [("", 0), ("YQ==", 1), ("YWI=", 2), ("YWJj", 3)] {
            assert_eq!(base64_length(encoded, 4).unwrap(), length);
        }
        for bad in [
            "YQ", "YR==", "YWJ=", "YQ===", "YQ==\n", "====", "YQ-_", "YQ=+",
        ] {
            assert!(base64_length(bad, 8).is_err(), "{bad}");
        }
        let import = "12345678-1234-1234-1234-123456789abc";
        let id = hex(&model::file_id(b"a").0);
        let payload = json!({"import_id":import,"descriptor":{"kind":1,"id":id,"size_bytes":1,"format_version":1},"data_base64":"YQ=="});
        let key = format!("io-{import}-1-{id}");
        assert!(normalize(&request(41, &key, payload.clone())).is_ok());
        assert!(normalize(&request(41, "wrong", payload.clone())).is_err());
        let mut wrong = payload;
        wrong["descriptor"]["size_bytes"] = json!(2);
        assert!(normalize(&request(41, &key, wrong)).is_err());
        assert!(base64_length(&"A".repeat(MAX_FILE.div_ceil(3) * 4 + 4), MAX_FILE).is_err());
    }
    #[test]
    fn duplicate_progress_fields_and_caller_grants_are_rejected() {
        let import = "12345678-1234-1234-1234-123456789abc";
        let input = request(
            42,
            &format!("ia-{import}-1"),
            json!({"import_id":import,"expected_generation":1}),
        );
        let mut envelope: Value = serde_json::from_slice(&input).unwrap();
        let payload = envelope["payload_json"].as_str().unwrap().replace(
            "\"expected_generation\":1",
            "\"expected_generation\":1,\"expected_generation\":2",
        );
        envelope["payload_json"] = json!(payload);
        assert!(normalize(&serde_json::to_vec(&envelope).unwrap()).is_err());
        assert!(
            normalize(&request(
                44,
                "finish",
                json!({"import_id":import,"expected_generation":1,"epoch":1})
            ))
            .is_err()
        );
        assert!(
            normalize(&request(
                43,
                "",
                json!({"import_id":import,"owner":"operator"})
            ))
            .is_err()
        );
    }
    #[test]
    fn maximum_file_base64_fits_the_bounded_forge_envelope() {
        let import = "12345678-1234-1234-1234-123456789abc";
        let bytes = vec![0; MAX_FILE];
        let id = hex(&model::file_id(&bytes).0);
        let base64 = format!("{}AA==", "AAAA".repeat(MAX_FILE / 3));
        assert_eq!(base64_length(&base64, MAX_FILE).unwrap(), MAX_FILE);
        let payload = json!({"import_id":import,"descriptor":{"kind":1,"id":id,"size_bytes":MAX_FILE,"format_version":1},"data_base64":base64});
        let input = request(41, &format!("io-{import}-1-{id}"), payload);
        assert!(input.len() < MAX_INPUT);
        let output = normalize(&input).unwrap();
        assert_eq!(output["payload"]["descriptor"]["size_bytes"], MAX_FILE);
        // Only the storage module's native verification can acknowledge these
        // actual bytes: successful intent normalization is never a byte receipt.
    }
    #[test]
    fn advance_identity_binds_import_and_exact_generation_without_page_receipts() {
        let import = "12345678-1234-1234-1234-123456789abc";
        let key = format!("ia-{import}-1");
        let payload = json!({"import_id":import,"expected_generation":1});
        assert!(normalize(&request(42, &key, payload.clone())).is_ok());
        let mut changed = payload.clone();
        changed["expected_generation"] = json!(2);
        assert!(normalize(&request(42, &key, changed.clone())).is_err());
        assert!(normalize(&request(42, &format!("ia-{import}-2"), changed)).is_ok());
        assert!(normalize(&request(42, "advance", payload.clone())).is_err());
        // Finish retains its independently saved request-ID/fingerprint contract.
        assert!(normalize(&request(44, "saved-finish", payload)).is_ok());
    }
}
