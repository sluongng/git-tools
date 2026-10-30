//! Internal trusted checker adapters. These functions do not authenticate a
//! worker, select a candidate or grant object access or publication authority.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tinytree_forge_core::{check, checker as kernel};

fn decode<T: DeserializeOwned>(input: &[u8]) -> Result<T, String> {
    if input.len() > kernel::MAX_REPLY_BYTES {
        return Err("resource_exhausted: checker adapter input".into());
    }
    serde_json::from_slice(input).map_err(|_| "invalid_argument: checker adapter input".into())
}
fn value(input: impl serde::Serialize) -> Result<Value, String> {
    serde_json::to_value(input).map_err(|_| "invalid_argument: checker adapter output".into())
}
fn invalid(_: impl std::fmt::Display) -> String {
    "invalid_argument: checker binding".into()
}
pub fn parse_call(input: &[u8]) -> Result<Value, String> {
    value(kernel::Call::decode(input).map_err(invalid)?)
}
pub fn fingerprint(input: &[u8]) -> Result<Value, String> {
    let call = kernel::Call::decode(input).map_err(invalid)?;
    Ok(json!({"fingerprint":call.fingerprint().map_err(invalid)?}))
}
pub fn lease_binding(input: &[u8]) -> Result<Value, String> {
    let lease: kernel::CheckerLease = decode(input)?;
    Ok(json!({"binding":lease.immutable_binding().map_err(invalid)?}))
}
pub fn derive_request(input: &[u8]) -> Result<Value, String> {
    let request: check::Request = decode(input)?;
    request.validate().map_err(invalid)?;
    Ok(json!({"workspace_id":request.workspace_id()}))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptInput {
    request: check::Request,
    receipt: check::Receipt,
    policy: kernel::ExecutionPolicy,
}
pub fn receipt(input: &[u8]) -> Result<Value, String> {
    let input: ReceiptInput = decode(input)?;
    input
        .policy
        .validate_receipt(&input.request, &input.receipt)
        .map_err(invalid)?;
    Ok(json!({"receipt_digest":kernel::receipt_digest(&input.receipt).map_err(invalid)?}))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataInput {
    descriptor: kernel::Descriptor,
    bytes_base64: String,
    path: String,
    cursor: u64,
    limit: u32,
}
pub fn snapshot_page(input: &[u8]) -> Result<Value, String> {
    let input: MetadataInput = decode(input)?;
    if input.bytes_base64.len() > (256usize * 1024).div_ceil(3) * 4 {
        return Err("resource_exhausted: checker metadata".into());
    }
    let bytes = STANDARD.decode(&input.bytes_base64).map_err(invalid)?;
    if bytes.len() > 256 * 1024 || STANDARD.encode(&bytes) != input.bytes_base64 {
        return Err("invalid_argument: checker metadata encoding".into());
    }
    value(
        kernel::snapshot_page(
            &input.descriptor,
            &bytes,
            &input.path,
            input.cursor,
            input.limit,
        )
        .map_err(invalid)?,
    )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestInput {
    candidate_commit: String,
    root_tree: String,
    state: Option<kernel::ManifestHashState>,
    entries: Vec<kernel::ManifestEntry>,
    limits: kernel::SnapshotLimits,
    finish: bool,
}
pub fn manifest_step(input: &[u8]) -> Result<Value, String> {
    let input: ManifestInput = decode(input)?;
    input.limits.validate().map_err(invalid)?;
    if input.entries.len() > kernel::PAGE_ENTRIES {
        return Err("resource_exhausted: checker manifest page".into());
    }
    let mut hash = match input.state {
        Some(state) => kernel::ManifestHasher::from_state(state).map_err(invalid)?,
        None => kernel::ManifestHasher::new(&input.candidate_commit, &input.root_tree)
            .map_err(invalid)?,
    };
    for entry in &input.entries {
        entry.validate(&input.limits).map_err(invalid)?;
        hash.push(entry).map_err(invalid)?;
    }
    let totals = if input.finish {
        Some(hash.finish(&input.limits).map_err(invalid)?)
    } else {
        None
    };
    Ok(json!({"state":hash.state(),"totals":totals}))
}
