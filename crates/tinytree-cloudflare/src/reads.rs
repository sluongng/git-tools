//! Strict public read selectors only. Normalization does not authenticate the
//! caller, resolve a view, or make a digest an object-read capability.
use serde::Deserialize;
use serde_json::{Value, json};
use tinytree_forge_core::{
    account_commands::{ReadFileRequest, ReadObjectRequest},
    accounts::{AccountId, validate_name, validate_path},
};
use tinytree_model::Kind;
const MAX_INPUT: usize = 64 * 1024;
const MAX_SAFE: u64 = 9_007_199_254_740_991;
const MAX_CHUNK: usize = 64 * 1024;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    account_id: String,
    operation: u32,
    view: String,
    payload_json: String,
}
fn parse<T: serde::de::DeserializeOwned>(input: &[u8]) -> Result<T, String> {
    if input.len() > MAX_INPUT {
        return Err("read limit: request bytes".into());
    }
    serde_json::from_slice(input).map_err(|_| "invalid strict read input".into())
}
pub fn normalize_read_command(input: &[u8]) -> Result<Value, String> {
    let c: Command = parse(input)?;
    AccountId(c.account_id)
        .validate()
        .map_err(|e| e.to_string())?;
    if !c.view.is_empty() {
        validate_name(&c.view).map_err(|e| e.to_string())?;
    }
    let payload = if c.payload_json.is_empty() {
        b"{}".as_slice()
    } else {
        c.payload_json.as_bytes()
    };
    match c.operation {
        7 => {
            let p: ReadFileRequest = parse(payload)?;
            validate_path(&p.path, false).map_err(|e| e.to_string())?;
            Ok(json!({"operation":7,"version":p.version,"path":p.path}))
        }
        9 => {
            let p: ReadObjectRequest = parse(payload)?;
            Kind::try_from(p.kind).map_err(|_| "invalid native read kind")?;
            if p.offset > MAX_SAFE {
                return Err("invalid read offset".into());
            }
            if p.max_bytes > MAX_CHUNK {
                return Err("read limit: object chunk".into());
            }
            Ok(
                json!({"operation":9,"version":p.version,"kind":p.kind,"id":p.id,"offset":p.offset,"max_bytes":p.max_bytes}),
            )
        }
        _ => Err("unsupported read operation".into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn command(operation: u32, payload: Value) -> Value {
        json!({"account_id":"example","operation":operation,"view":"public","payload_json":payload.to_string()})
    }
    fn invoke(value: &Value) -> Result<Value, String> {
        normalize_read_command(&serde_json::to_vec(value).unwrap())
    }
    #[test]
    fn shared_file_selector_preserves_relative_path_and_exact_version() {
        let p = json!({"version":vec![3u8; 32],"path":"src/readme.md"});
        let result = invoke(&command(7, p)).unwrap();
        assert_eq!(
            result,
            json!({"operation":7,"version":vec![3u8; 32],"path":"src/readme.md"})
        );
        for path in [
            "../private",
            "/private",
            "src/../private",
            "src//private",
            "",
        ] {
            assert!(invoke(&command(7, json!({"path":path}))).is_err(), "{path}");
        }
        assert!(invoke(&command(7, json!({"path":"file","actor":"owner"}))).is_err());
    }
    #[test]
    fn shared_object_defaults_and_zero_length_descriptor_queries_are_exact() {
        let result = invoke(&command(9, json!({"kind":1,"id":vec![4u8; 32]}))).unwrap();
        assert_eq!(
            result,
            json!({"operation":9,"version":null,"kind":1,"id":vec![4u8; 32],"offset":0,"max_bytes":65536})
        );
        let result = invoke(&command(
            9,
            json!({"kind":3,"id":vec![5u8; 32],"offset":17,"max_bytes":0}),
        ))
        .unwrap();
        assert_eq!(result["max_bytes"], 0);
        assert_eq!(result["offset"], 17);
        assert!(invoke(&command(9, json!({"kind":0,"id":vec![4u8; 32]}))).is_err());
        assert!(
            invoke(&command(
                9,
                json!({"kind":1,"id":vec![4u8; 32],"offset":MAX_SAFE+1})
            ))
            .is_err()
        );
        assert_eq!(
            invoke(&command(
                9,
                json!({"kind":1,"id":vec![4u8; 32],"max_bytes":MAX_CHUNK+1})
            ))
            .unwrap_err(),
            "read limit: object chunk"
        );
    }
    #[test]
    fn rejects_unknown_duplicate_and_oversized_inputs_without_read_authority() {
        let mut value = command(9, json!({"kind":1,"id":vec![4u8; 32]}));
        value["principal"] = json!("owner");
        assert!(invoke(&value).is_err());
        let mut value = command(9, json!({"kind":1,"id":vec![4u8; 32]}));
        value["payload_json"] = json!(format!(
            "{{\"kind\":1,\"kind\":2,\"id\":{}}}",
            json!(vec![4u8; 32])
        ));
        assert!(invoke(&value).is_err());
        assert!(
            normalize_read_command(&vec![b' '; MAX_INPUT + 1])
                .unwrap_err()
                .starts_with("read limit:")
        );
        assert!(invoke(&command(8, json!({"query":"text"}))).is_err());
    }
}
