//! Untrusted view-spec validation and canonical identity, not authorization.
//! No actor, membership grant, resolved-view capability or storage is accepted.
use serde::Deserialize;
use serde_json::{Value, json};
use tinytree_forge_core::accounts::{AccountId, MAX_VIEW_BYTES, ViewSpec, view_version};

const MAX_VIEW_INPUT: usize = MAX_VIEW_BYTES + 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidateView {
    account_id: String,
    spec: ViewSpec,
}

pub fn validate_view(input: &[u8]) -> Result<Value, String> {
    if input.len() > MAX_VIEW_INPUT || input.len() > crate::MAX_INPUT {
        return Err("view validation input limit".into());
    }
    let request: ValidateView =
        serde_json::from_slice(input).map_err(|_| "invalid view validation input")?;
    let account = AccountId(request.account_id);
    account.validate().map_err(|error| error.to_string())?;
    request.spec.validate().map_err(|error| error.to_string())?;
    let version = view_version(&account, &request.spec).map_err(|error| error.to_string())?;
    Ok(json!({ "view_version": crate::hex(&version.0), "spec": request.spec }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> Value {
        json!({
            "account_id":"example",
            "spec": {
                "name":"public", "root_prefix":"pkg", "include_prefixes":["src"],
                "exclude_prefixes":["src/private"], "visibility":{"kind":"roles","roles":["member"]},
                "revision":vec![1u8; 32], "export_allowed":true,
            }
        })
    }

    #[test]
    fn view_validation_returns_shared_native_identity_and_normalized_spec() {
        let request = request();
        let output = validate_view(&serde_json::to_vec(&request).unwrap()).unwrap();
        assert_eq!(
            output["view_version"],
            "b459fc5356b0b5ec653a22dba7073aaaa393b65d4ed317cd1f1c6ef6bcb8b2fa"
        );
        assert_eq!(output["spec"], request["spec"]);
        assert!(output.get("actor").is_none());
    }

    #[test]
    fn view_validation_rejects_unknown_duplicate_and_invalid_path_fields() {
        let mut unknown = request();
        unknown["actor"] = json!({"principal":"owner"});
        assert!(validate_view(&serde_json::to_vec(&unknown).unwrap()).is_err());
        let mut unknown = request();
        unknown["spec"]["resolved"] = json!(true);
        assert!(validate_view(&serde_json::to_vec(&unknown).unwrap()).is_err());
        let encoded = request().to_string();
        for duplicate in [
            encoded.replace(
                r#""account_id":"example""#,
                r#""account_id":"example","account_id":"other""#,
            ),
            encoded.replace(
                r#""root_prefix":"pkg""#,
                r#""root_prefix":"pkg","root_prefix":"private""#,
            ),
        ] {
            assert_ne!(duplicate, encoded);
            assert!(validate_view(duplicate.as_bytes()).is_err());
        }
        let mut invalid = request();
        invalid["spec"]["include_prefixes"] = json!(["../private"]);
        assert!(
            validate_view(&serde_json::to_vec(&invalid).unwrap())
                .unwrap_err()
                .contains("forge path")
        );
        let mut invalid = request();
        invalid["account_id"] = json!("Other");
        assert!(validate_view(&serde_json::to_vec(&invalid).unwrap()).is_err());
        let mut invalid = request();
        invalid["spec"]["revision"] = json!(vec![0u8; 32]);
        assert!(validate_view(&serde_json::to_vec(&invalid).unwrap()).is_err());
        assert!(validate_view(&vec![b' '; MAX_VIEW_INPUT + 1]).is_err());
    }
}
