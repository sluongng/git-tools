//! Pure transform framing/presentation only. No route turns these parsed facts
//! into an authenticated source export, retained-byte or publication grant.
use serde::Deserialize;
use serde_json::{Value, json};
use std::result::Result;
use tinytree_forge_core::{
    accounts::{AccountId, validate_name},
    transform_commands::{self as commands, Command},
    transforms::*,
};

const LIMIT: usize = 8 * 1024 * 1024;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    account_id: String,
    operation: i32,
    view: String,
    request_id: String,
    payload_json: String,
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    if bytes.len() > LIMIT {
        return Err("resource_exhausted: transform adapter input".into());
    }
    serde_json::from_slice(bytes).map_err(|_| "invalid_argument: transform adapter input".into())
}
fn error(value: tinytree_forge_core::Error) -> String {
    match value {
        tinytree_forge_core::Error::Limit(_) => "resource_exhausted: transform payload".into(),
        _ => "invalid_argument: transform payload".into(),
    }
}
fn bounded(value: Value) -> Result<Value, String> {
    let bytes =
        serde_json::to_vec(&value).map_err(|_| "invalid_argument: transform adapter output")?;
    if bytes.len() > LIMIT {
        return Err("resource_exhausted: transform adapter output".into());
    }
    Ok(value)
}
pub fn normalize(bytes: &[u8]) -> Result<Value, String> {
    let input: Input = decode(bytes)?;
    AccountId(input.account_id.clone())
        .validate()
        .map_err(error)?;
    if !input.view.is_empty() {
        validate_name(&input.view).map_err(error)?;
    }
    if matches!(input.operation, 30 | 32 | 34) || !input.request_id.is_empty() {
        tinytree_forge_core::upload::validate_request_id(&input.request_id).map_err(error)?;
    }
    let command = commands::parse(input.operation, input.payload_json.as_bytes())
        .map_err(error)?
        .ok_or("unimplemented: transform operation")?;
    let (payload, fingerprint) = match command {
        Command::Start(intent) | Command::SetPin(intent) => {
            let fingerprint = commands::start_fingerprint(
                input.operation,
                &input.account_id,
                &input.view,
                &intent,
            )
            .map_err(error)?;
            (
                serde_json::to_value(intent)
                    .map_err(|_| "invalid_argument: transform intent encoding")?,
                Some(fingerprint),
            )
        }
        Command::ResolvePin(intent) => (
            serde_json::to_value(intent).map_err(|_| "invalid_argument: pin selector encoding")?,
            None,
        ),
        Command::RetirePin { run_id } => {
            let fingerprint = commands::retire_fingerprint(&input.account_id, &input.view, &run_id)
                .map_err(error)?;
            (json!({"run_id":run_id}), Some(fingerprint))
        }
        Command::Get { run_id } | Command::ReadLineage { run_id } => {
            (json!({"run_id":run_id}), None)
        }
    };
    bounded(json!({"operation":input.operation,"fingerprint":fingerprint,"payload":payload}))
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Format {
    View {
        value: ForgeTransformView,
    },
    Started {
        value: ForgeTransformView,
        review: EngineReview,
        intent: commands::StartIntent,
    },
    Pin {
        value: ForgeDependencyPin,
    },
    Page {
        value: ForgePinPage,
    },
    Chunk {
        value: ForgePinChunk,
    },
    Lineage {
        value: ForgeTransformView,
    },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EngineReview {
    state: String,
    result: Value,
}
pub fn format(bytes: &[u8]) -> Result<Value, String> {
    let (state, value) = match decode::<Format>(bytes)? {
        Format::View { value } => (
            if value.pin.retired {
                "retired"
            } else {
                "prepared"
            }
            .into(),
            commands::format_view(&value),
        ),
        Format::Started {
            value,
            review,
            intent,
        } => {
            intent.validate().map_err(error)?;
            let selected = commands::DestinationBinding {
                destination_view: commands::hex(&value.destination_view),
                destination_base: commands::hex(&value.destination_base),
                job_id: commands::hex(&value.id),
            };
            let ack = json!({"transform":commands::format_view(&value),"review":review.result});
            commands::validate_ack(
                &serde_json::to_vec(&ack)
                    .map_err(|_| "invalid_argument: transform acknowledgement encoding")?,
                &review.state,
                &value.destination_account,
                &intent,
                Some(&selected),
            )
            .map_err(error)?;
            (review.state, ack)
        }
        Format::Pin { value } => {
            if !value.retired {
                return Err("invalid_argument: pin retirement presentation".into());
            }
            ("retired".into(), commands::format_pin(&value))
        }
        Format::Page { value } => ("retained".into(), commands::format_page(&value)),
        Format::Chunk { value } => ("retained".into(), commands::format_chunk(&value)),
        Format::Lineage { value } => (
            if value.origin.is_some() {
                "complete"
            } else {
                "origin_unavailable"
            }
            .into(),
            commands::format_lineage(&value),
        ),
    };
    bounded(json!({"state":state,"value":value}))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn intent() -> Value {
        json!({"source_account":"upstream","source_view":"public","source_before_view":"11".repeat(32),"source_after_view":"22".repeat(32),"change_id":"33".repeat(16),"spec":{"mappings":[{"source_prefix":"","destination_prefix":"vendor"}]}})
    }
    fn input(operation: i32, payload: Value) -> Value {
        json!({"account_id":"downstream","operation":operation,"view":"public","request_id":"saved-transform","payload_json":payload.to_string()})
    }
    fn run(value: &Value) -> Result<Value, String> {
        normalize(&serde_json::to_vec(value).unwrap())
    }
    #[test]
    fn public_command_codec_is_scoped_strict_and_keeps_read_fingerprints_absent() {
        let parsed = run(&input(30, intent())).unwrap();
        assert_eq!(parsed["operation"], 30);
        assert_eq!(parsed["payload"]["source_account"], "upstream");
        assert_eq!(parsed["fingerprint"].as_str().unwrap().len(), 64);
        assert!(
            run(&input(31, json!({"run_id":"44".repeat(32)}))).unwrap()["fingerprint"].is_null()
        );
        let retired = run(&input(34, json!({"run_id":"44".repeat(32)}))).unwrap();
        assert_eq!(retired["fingerprint"].as_str().unwrap().len(), 64);
        let mut injected = input(30, intent());
        injected["principal"] = json!("operator");
        assert!(run(&injected).is_err());
        let mut empty = input(30, intent());
        empty["request_id"] = json!("");
        assert!(run(&empty).is_err());
    }
    #[test]
    fn typed_presentation_formats_hex_but_never_file_payload_bytes_or_hidden_origin() {
        let chunk = ForgePinChunk {
            pin_id: [1; 32],
            file_id: [2; 32],
            offset: 0,
            bytes: vec![3; 32],
            next_offset: 32,
            eof: true,
        };
        let shown =
            format(&serde_json::to_vec(&json!({"kind":"chunk","value":chunk})).unwrap()).unwrap();
        assert_eq!(shown["state"], "retained");
        assert_eq!(shown["value"]["file_id"], "02".repeat(32));
        assert_eq!(shown["value"]["bytes"], json!(vec![3; 32]));
        let view = ForgeTransformView {
            id: [7; 32],
            destination_view: [1; 32],
            destination_account: "downstream".into(),
            destination_base: [2; 32],
            output_head: [6; 32],
            pin: ForgeDependencyPin {
                id: [7; 32],
                artifact: [8; 32],
                reference: "retained-pin".into(),
                generation: 1,
                retired: false,
                protected_roots: Default::default(),
            },
            origin: None,
        };
        let hidden =
            format(&serde_json::to_vec(&json!({"kind":"lineage","value":view})).unwrap()).unwrap();
        assert_eq!(hidden["state"], "origin_unavailable");
        assert!(hidden["value"]["origin"].is_null());
        let review = json!({"state":"rebase_pending","result":{"state":"rebase_pending","change_id":"33".repeat(16),"patchset_id":"09".repeat(32),"source_base":"02".repeat(32),"source_head":"06".repeat(32),"view_version":"01".repeat(32),"provenance":"07".repeat(32)}});
        let mut started = json!({"kind":"started","value":view,"review":review,"intent":intent()});
        assert_eq!(
            format(&serde_json::to_vec(&started).unwrap()).unwrap()["state"],
            "rebase_pending"
        );
        started["review"]["result"]["source_head"] = json!("aa".repeat(32));
        assert!(format(&serde_json::to_vec(&started).unwrap()).is_err());
    }
}
