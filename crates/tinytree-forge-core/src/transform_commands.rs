//! Strict transform/pin intents and presentation. These parsers grant no source
//! export, destination write, retained-byte or publication authority.
use crate::{
    Error, Result,
    accounts::{AccountId, validate_name, validate_path},
    transforms::*,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const MAX_PAYLOAD_BYTES: usize = 256 * 1024;
pub const TRANSFORM_PENDING_STATE: &str = "transform_pending";
const MAX_SAFE: u64 = (1u64 << 53) - 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputLineageOperation {
    Rename,
    Copy,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputLineage {
    pub operation: InputLineageOperation,
    pub source_path: String,
    pub target_path: String,
}
impl InputLineage {
    pub fn into_hint(self) -> ForgeLineageHint {
        ForgeLineageHint {
            operation: match self.operation {
                InputLineageOperation::Rename => ForgeLineageOperation::Rename,
                InputLineageOperation::Copy => ForgeLineageOperation::Copy,
            },
            source_path: self.source_path,
            target_path: self.target_path,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartIntent {
    pub source_account: String,
    pub source_view: String,
    pub source_before_view: String,
    pub source_after_view: String,
    pub destination_view: Option<String>,
    pub spec: ForgeTransformSpec,
    pub change_id: String,
    #[serde(default)]
    pub lineage: Vec<InputLineage>,
    pub expected_patchset: Option<String>,
}
impl StartIntent {
    pub fn validate(&self) -> Result<()> {
        AccountId(self.source_account.clone()).validate()?;
        validate_name(&self.source_view)?;
        id::<32>(&self.source_before_view)?;
        id::<32>(&self.source_after_view)?;
        id::<16>(&self.change_id)?;
        if let Some(value) = &self.destination_view {
            id::<32>(value)?;
        }
        if let Some(value) = &self.expected_patchset {
            id::<32>(value)?;
        }
        self.spec.validate().map_err(|error| match error {
            crate::transforms::Error::Limit(reason) => Error::Limit(reason),
            crate::transforms::Error::Invalid(reason) => Error::Invalid(reason),
            _ => Error::Invalid("invalid transform specification"),
        })?;
        if self.lineage.len() > 4096 {
            return Err(Error::Limit("transform lineage hints"));
        }
        for hint in &self.lineage {
            validate_path(&hint.source_path, false)?;
            validate_path(&hint.target_path, false)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinSelector {
    pub run_id: String,
    pub path: Option<String>,
    pub offset: Option<u64>,
    pub max_bytes: Option<usize>,
    pub after_path: Option<String>,
    pub limit: Option<usize>,
}
impl PinSelector {
    pub fn validate(&self) -> Result<()> {
        id::<32>(&self.run_id)?;
        if let Some(path) = &self.path {
            validate_path(path, false)?;
            if self.after_path.is_some() || self.limit.is_some() {
                return Err(Error::Invalid(
                    "pin chunk and page selectors cannot be combined",
                ));
            }
            if self.offset.is_some_and(|n| n > MAX_SAFE)
                || self.max_bytes.is_some_and(|n| n == 0 || n > 65536)
            {
                return Err(Error::Limit("dependency pin chunk"));
            }
        } else {
            if self.offset.is_some() || self.max_bytes.is_some() {
                return Err(Error::Invalid("pin byte range requires a file path"));
            }
            if let Some(path) = &self.after_path {
                validate_path(path, false)?;
            }
            if self.limit.is_some_and(|n| n == 0 || n > 256) {
                return Err(Error::Limit("dependency pin page"));
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "operation", content = "intent", rename_all = "snake_case")]
pub enum Command {
    Start(StartIntent),
    Get { run_id: String },
    SetPin(StartIntent),
    ResolvePin(PinSelector),
    RetirePin { run_id: String },
    ReadLineage { run_id: String },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Run {
    run_id: String,
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::Invalid("invalid transform operation payload"))
}
pub fn parse(operation: i32, bytes: &[u8]) -> Result<Option<Command>> {
    if !(30..=35).contains(&operation) {
        return Ok(None);
    }
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(Error::Limit("transform payload limit"));
    }
    Ok(Some(match operation {
        30 | 32 => {
            let value: StartIntent = decode(bytes)?;
            value.validate()?;
            if operation == 30 {
                Command::Start(value)
            } else {
                Command::SetPin(value)
            }
        }
        33 => {
            let value: PinSelector = decode(bytes)?;
            value.validate()?;
            Command::ResolvePin(value)
        }
        _ => {
            let value: Run = decode(bytes)?;
            id::<32>(&value.run_id)?;
            match operation {
                31 => Command::Get {
                    run_id: value.run_id,
                },
                34 => Command::RetirePin {
                    run_id: value.run_id,
                },
                _ => Command::ReadLineage {
                    run_id: value.run_id,
                },
            }
        }
    }))
}
pub fn id<const N: usize>(text: &str) -> Result<[u8; N]> {
    if text.len() != N * 2
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Invalid("invalid transform object identity"));
    }
    let mut result = [0; N];
    for (index, value) in result.iter_mut().enumerate() {
        *value = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
            .map_err(|_| Error::Invalid("invalid transform object identity"))?;
    }
    Ok(result)
}
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn start_fingerprint(
    operation: i32,
    account: &str,
    view: &str,
    intent: &StartIntent,
) -> Result<String> {
    if ![30, 32].contains(&operation) {
        return Err(Error::Invalid("expected transform mutation"));
    }
    AccountId(account.to_owned()).validate()?;
    if !view.is_empty() {
        validate_name(view)?;
    }
    intent.validate()?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-transform-command-v1\0");
    hash.update(
        &serde_json::to_vec(&(operation, account, view, intent))
            .map_err(|_| Error::Corrupt("transform encoding"))?,
    );
    Ok(hash.finalize().to_hex().to_string())
}
pub fn retire_fingerprint(account: &str, view: &str, run_id: &str) -> Result<String> {
    AccountId(account.to_owned()).validate()?;
    if !view.is_empty() {
        validate_name(view)?;
    }
    id::<32>(run_id)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-transform-retire-command-v1\0");
    hash.update(
        &serde_json::to_vec(&(34, account, view, run_id))
            .map_err(|_| Error::Corrupt("transform encoding"))?,
    );
    Ok(hash.finalize().to_hex().to_string())
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationBinding {
    pub destination_view: String,
    pub destination_base: String,
    pub job_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingPhase {
    Prepare,
    ReviewAdmission,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformPending {
    pub schema_version: u32,
    pub request_id: String,
    pub intent_fingerprint: String,
    pub source_before_view: String,
    pub source_after_view: String,
    pub change_id: String,
    pub destination_view: String,
    pub destination_base: String,
    pub job_id: String,
    pub generation: u64,
    pub phase: PendingPhase,
}
impl TransformPending {
    pub fn validate_against(
        &self,
        request_id: &str,
        operation: i32,
        account: &str,
        view: &str,
        intent: &StartIntent,
        selected: Option<&DestinationBinding>,
    ) -> Result<DestinationBinding> {
        crate::upload::validate_request_id(request_id)?;
        let binding = DestinationBinding {
            destination_view: self.destination_view.clone(),
            destination_base: self.destination_base.clone(),
            job_id: self.job_id.clone(),
        };
        for value in [
            &binding.destination_view,
            &binding.destination_base,
            &binding.job_id,
            &self.intent_fingerprint,
        ] {
            id::<32>(value)?;
        }
        if self.schema_version != 1
            || self.request_id != request_id
            || self.intent_fingerprint != start_fingerprint(operation, account, view, intent)?
            || self.source_before_view != intent.source_before_view
            || self.source_after_view != intent.source_after_view
            || self.change_id != intent.change_id
            || self.generation > MAX_SAFE
            || intent
                .destination_view
                .as_ref()
                .is_some_and(|v| v != &binding.destination_view)
            || selected.is_some_and(|previous| previous != &binding)
        {
            return Err(Error::Invalid("transform pending binding"));
        }
        Ok(binding)
    }
}

/// One public spelling for every identifier. Origin is already current-policy
/// filtered by the host; formatting cannot restore a hidden source record.
pub fn format_view(value: &ForgeTransformView) -> Value {
    json!({"id":hex(&value.id),"destination_account":value.destination_account,"destination_view":hex(&value.destination_view),
        "destination_base":hex(&value.destination_base),"output_head":hex(&value.output_head),"pin":format_pin(&value.pin),
        "origin":value.origin.as_ref().map(|origin|json!({"account":origin.account,"before":hex(&origin.before),"after":hex(&origin.after),
            "lineage":origin.lineage.iter().map(|edge|json!({"operation":edge.operation,"source_account":edge.source_account,"source_revision":hex(&edge.source_revision),"source_path":edge.source_path,"destination_path":edge.destination_path,"destination_revision":hex(&edge.destination_revision)})).collect::<Vec<_>>()}))})
}
pub fn format_started(
    transform: &ForgeTransformView,
    review: &crate::review::ForgeReview,
) -> Result<(String, Value)> {
    let (state, review) = crate::review_commands::format_review(review)?;
    Ok((
        state,
        json!({"transform":format_view(transform),"review":review}),
    ))
}
pub fn format_pin(value: &ForgeDependencyPin) -> Value {
    json!({"id":hex(&value.id),"artifact":hex(&value.artifact),"reference":value.reference,"generation":value.generation,"retired":value.retired,"protected_roots":value.protected_roots})
}
pub fn format_page(value: &ForgePinPage) -> Value {
    json!({"pin_id":hex(&value.pin_id),"artifact":hex(&value.artifact),"files":value.files.iter().map(|file|json!({"path":file.path,"file_id":hex(&file.file_id),"size_bytes":file.size_bytes,"executable":file.executable})).collect::<Vec<_>>(),"next_path":value.next_path})
}
pub fn format_chunk(value: &ForgePinChunk) -> Value {
    json!({"pin_id":hex(&value.pin_id),"file_id":hex(&value.file_id),"offset":value.offset,"bytes":value.bytes,"next_offset":value.next_offset,"eof":value.eof})
}
pub fn format_lineage(value: &ForgeTransformView) -> Value {
    let view = format_view(value);
    json!({"run_id":hex(&value.id),"origin":view["origin"]})
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ack {
    transform: WireView,
    review: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireView {
    id: String,
    destination_account: String,
    destination_view: String,
    destination_base: String,
    output_head: String,
    pin: WirePin,
    origin: Option<WireOrigin>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePin {
    id: String,
    artifact: String,
    reference: String,
    generation: u64,
    retired: bool,
    protected_roots: std::collections::BTreeSet<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireOrigin {
    account: String,
    before: String,
    after: String,
    lineage: Vec<WireEdge>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEdge {
    operation: ForgeLineageOperation,
    source_account: String,
    source_revision: String,
    source_path: String,
    destination_path: String,
    destination_revision: String,
}
/// Full transform acknowledgement, not acceptance of checks or publication.
/// The host still owns provenance and current authorization; this only checks
/// that an authenticated response has not rebound the caller's saved intent.
pub fn validate_ack(
    bytes: &[u8],
    state: &str,
    account: &str,
    intent: &StartIntent,
    selected: Option<&DestinationBinding>,
) -> Result<()> {
    let ack: Ack = serde_json::from_slice(bytes)
        .map_err(|_| Error::Invalid("invalid transform acknowledgement"))?;
    let view = &ack.transform;
    for value in [
        &view.id,
        &view.destination_view,
        &view.destination_base,
        &view.output_head,
        &view.pin.id,
        &view.pin.artifact,
    ] {
        id::<32>(value)?;
    }
    if view.destination_account != account
        || view.pin.id != view.id
        || view.pin.retired
        || view.pin.generation > MAX_SAFE
        || intent
            .destination_view
            .as_ref()
            .is_some_and(|version| version != &view.destination_view)
        || selected.is_some_and(|binding| {
            binding.destination_view != view.destination_view
                || binding.destination_base != view.destination_base
        })
        || !matches!(
            state,
            "rebase_pending"
                | "prepared"
                | "conflict"
                | "stale"
                | "checks_pending"
                | "ready_to_land"
                | "landed"
                | "unsupported"
                | "check_failed"
                | "expired"
        )
    {
        return Err(Error::Invalid("transform acknowledgement binding"));
    }
    if view.pin.reference.len() > 4096
        || view.pin.reference.chars().any(char::is_control)
        || view.pin.protected_roots.len() > 1024
        || view
            .pin
            .protected_roots
            .iter()
            .any(|root| root.is_empty() || root.len() > 128 || root.chars().any(char::is_control))
    {
        return Err(Error::Invalid("transform pin shape"));
    }
    if let Some(origin) = &view.origin {
        AccountId(origin.account.clone()).validate()?;
        id::<32>(&origin.before)?;
        id::<32>(&origin.after)?;
        if origin.account != intent.source_account || origin.lineage.len() > 4096 {
            return Err(Error::Invalid("transform origin binding"));
        }
        for edge in &origin.lineage {
            AccountId(edge.source_account.clone()).validate()?;
            id::<32>(&edge.source_revision)?;
            id::<32>(&edge.destination_revision)?;
            validate_path(&edge.source_path, false)?;
            validate_path(&edge.destination_path, false)?;
            let _ = &edge.operation;
        }
    }
    let expected = [
        ("change_id", intent.change_id.as_str()),
        ("source_base", view.destination_base.as_str()),
        ("source_head", view.output_head.as_str()),
        ("view_version", view.destination_view.as_str()),
        ("provenance", view.id.as_str()),
        ("state", state),
    ];
    if expected
        .iter()
        .any(|(field, value)| ack.review.get(*field).and_then(Value::as_str) != Some(*value))
    {
        return Err(Error::Invalid("transform review binding"));
    }
    id::<32>(
        ack.review
            .get("patchset_id")
            .and_then(Value::as_str)
            .ok_or(Error::Invalid("transform patchset missing"))?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn start() -> Value {
        json!({"source_account":"upstream","source_view":"public","source_before_view":"11".repeat(32),"source_after_view":"22".repeat(32),"change_id":"33".repeat(16),"spec":{"mappings":[{"source_prefix":"library","destination_prefix":"vendor"}]}})
    }
    fn command(value: &Value) -> Result<Option<Command>> {
        parse(30, &serde_json::to_vec(value).unwrap())
    }
    #[test]
    fn public_transform_commands_share_limits_and_reject_service_authority_and_ambiguous_selectors()
    {
        assert!(matches!(
            command(&start()).unwrap(),
            Some(Command::Start(_))
        ));
        let mut value = start();
        value["actor"] = json!("operator");
        assert!(command(&value).is_err());
        let mut value = start();
        value["lineage"] = json!([{"operation":"imported_origin","source_path":"library/a","target_path":"vendor/a"}]);
        assert!(command(&value).is_err());
        let mut value = start();
        value["spec"]["mappings"]=json!((0..65).map(|i|json!({"source_prefix":format!("source-{i}"),"destination_prefix":format!("target-{i}")})).collect::<Vec<_>>());
        assert!(matches!(command(&value), Err(Error::Limit(_))));
        for value in [
            json!({"run_id":"44".repeat(32),"path":"vendor/a","limit":1}),
            json!({"run_id":"44".repeat(32),"offset":0}),
            json!({"run_id":"44".repeat(32),"limit":257}),
        ] {
            assert!(parse(33, &serde_json::to_vec(&value).unwrap()).is_err());
        }
        assert!(
            parse(
                33,
                &serde_json::to_vec(
                    &json!({"run_id":"44".repeat(32),"path":"vendor/a","max_bytes":65536})
                )
                .unwrap()
            )
            .is_ok()
        );
    }
    #[test]
    fn pending_binding_pins_server_destination_without_rewriting_the_original_intent() {
        let Some(Command::Start(intent)) = command(&start()).unwrap() else {
            panic!("start intent")
        };
        let mut pending = TransformPending {
            schema_version: 1,
            request_id: "saved-transform".into(),
            intent_fingerprint: start_fingerprint(30, "downstream", "public", &intent).unwrap(),
            source_before_view: intent.source_before_view.clone(),
            source_after_view: intent.source_after_view.clone(),
            change_id: intent.change_id.clone(),
            destination_view: "55".repeat(32),
            destination_base: "66".repeat(32),
            job_id: "77".repeat(32),
            generation: 0,
            phase: PendingPhase::Prepare,
        };
        let selected = pending
            .validate_against("saved-transform", 30, "downstream", "public", &intent, None)
            .unwrap();
        assert!(intent.destination_view.is_none());
        pending.generation = 1;
        pending.phase = PendingPhase::ReviewAdmission;
        assert!(
            pending
                .validate_against(
                    "saved-transform",
                    30,
                    "downstream",
                    "public",
                    &intent,
                    Some(&selected)
                )
                .is_ok()
        );
        pending.destination_view = "88".repeat(32);
        assert!(
            pending
                .validate_against(
                    "saved-transform",
                    30,
                    "downstream",
                    "public",
                    &intent,
                    Some(&selected)
                )
                .is_err()
        );
    }
}
