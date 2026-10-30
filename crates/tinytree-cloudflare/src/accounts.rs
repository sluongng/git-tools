//! Untrusted account-command normalization. Identity and policy come exclusively
//! from the host; normalized commands are not grants or account records.
use serde::Deserialize;
use serde_json::{Value, json};
use tinytree_forge_core::{
    account_commands::{self, EmptyRequest, MAX_ACCOUNT_COMMAND_BYTES, SetMemberRequest},
    accounts::{AccountId, ForgeAccountMutation, PolicyEpoch},
};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    account_id: String,
    operation: u32,
    view: String,
    payload_json: String,
}
pub fn normalize(input: &[u8]) -> Result<Value, String> {
    if input.len() > 64 * 1024 {
        return Err("account command input limit".into());
    }
    let input: Input = serde_json::from_slice(input).map_err(|_| "invalid account command")?;
    let account = AccountId(input.account_id);
    account.validate().map_err(|e| e.to_string())?;
    // Account operations ignore view semantically, just like the local wire
    // adapter. Validate framing only; a selected view never confers a grant.
    if input.view.len() > 128 || input.view.chars().any(char::is_control) {
        return Err("invalid account command view".into());
    }
    if input.payload_json.len() > MAX_ACCOUNT_COMMAND_BYTES {
        return Err("account payload byte limit".into());
    }
    let mutation = match input.operation {
        1 => {
            let _: EmptyRequest = serde_json::from_str(&input.payload_json)
                .map_err(|_| "invalid empty account request")?;
            Some(ForgeAccountMutation::Provision)
        }
        2 => {
            let _: EmptyRequest = serde_json::from_str(&input.payload_json)
                .map_err(|_| "invalid empty account request")?;
            None
        }
        3 => {
            let request: SetMemberRequest = serde_json::from_str(&input.payload_json)
                .map_err(|_| "invalid set member request")?;
            account_commands::validate_member(&request.member).map_err(|e| e.to_string())?;
            if request.expected_epoch > 9_007_199_254_740_991 {
                return Err("account epoch backend limit".into());
            }
            Some(ForgeAccountMutation::SetMember {
                member: request.member,
                expected_epoch: PolicyEpoch(request.expected_epoch),
            })
        }
        _ => return Err("unsupported account command".into()),
    };
    let mut output = json!({"operation":input.operation,"fingerprint":mutation.as_ref().map(account_commands::mutation_fingerprint).transpose().map_err(|e|e.to_string())?.map(|id|crate::hex(&id))});
    match mutation {
        Some(ForgeAccountMutation::Provision) => {
            let genesis =
                account_commands::genesis(&account, crate::limits()).map_err(|e| e.to_string())?;
            let head = crate::hex(&genesis.commit.descriptor.key.id.0);
            let objects: Vec<_> = [genesis.empty, genesis.commit].into_iter().map(|object| {
                json!({"kind":object.descriptor.key.kind as u8,"id":crate::hex(&object.descriptor.key.id.0),"size_bytes":object.descriptor.size_bytes,"format_version":object.descriptor.format_version,"bytes":object.bytes})
            }).collect();
            output["genesis"] = json!({"head":head,"objects":objects});
        }
        Some(ForgeAccountMutation::SetMember {
            member,
            expected_epoch,
        }) => {
            output["member"] = json!(member);
            output["expected_epoch"] = json!(expected_epoch.0);
        }
        _ => {}
    }
    Ok(output)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn request(operation: u32, payload: &str) -> Vec<u8> {
        serde_json::to_vec(
            &json!({"account_id":"sample","operation":operation,"view":"","payload_json":payload}),
        )
        .unwrap()
    }
    #[test]
    fn genesis_and_fingerprints_share_native_authoring_rules() {
        let output = normalize(&request(1, "{}")).unwrap();
        let genesis =
            account_commands::genesis(&AccountId("sample".into()), crate::limits()).unwrap();
        assert_eq!(
            output["genesis"]["head"],
            crate::hex(&genesis.commit.descriptor.key.id.0)
        );
        assert_eq!(
            output["fingerprint"],
            crate::hex(
                &account_commands::mutation_fingerprint(&ForgeAccountMutation::Provision).unwrap()
            )
        );
        assert_eq!(
            normalize(&request(2, "{}")).unwrap(),
            json!({"operation":2,"fingerprint":null})
        );
    }
    #[test]
    fn default_public_view_does_not_change_account_commands_or_fingerprints() {
        let payload = json!({"member":{"principal":"member","can_write":true,"can_review":false,"is_admin":false,"roles":[]},"expected_epoch":1}).to_string();
        for (operation, payload) in [(1, "{}"), (2, "{}"), (3, payload.as_str())] {
            let bytes = request(operation, payload);
            let expected = normalize(&bytes).unwrap();
            let mut input: Value = serde_json::from_slice(&bytes).unwrap();
            input["view"] = json!("public");
            assert_eq!(
                normalize(&serde_json::to_vec(&input).unwrap()).unwrap(),
                expected
            );
            for invalid in ["a".repeat(129), "public\n".into(), "é".repeat(65)] {
                input["view"] = json!(invalid);
                assert!(normalize(&serde_json::to_vec(&input).unwrap()).is_err());
            }
        }
    }
    #[test]
    fn strict_account_payloads_do_not_accept_actor_or_ambiguous_grants() {
        for payload in [
            "{\"owner\":\"member\"}",
            "{\"actor\":\"member\"}",
            "{\"x\":1,\"x\":2}",
        ] {
            assert!(normalize(&request(1, payload)).is_err());
        }
        let member = json!({"principal":"member","can_write":true,"can_review":false,"is_admin":false,"roles":[]});
        let payload = json!({"member":member,"expected_epoch":1}).to_string();
        assert!(normalize(&request(3, &payload)).is_ok());
        assert!(
            normalize(&request(
                3,
                &payload.replace(
                    "\"expected_epoch\":1",
                    "\"expected_epoch\":1,\"expected_epoch\":2"
                )
            ))
            .is_err()
        );
        assert!(
            normalize(&request(
                3,
                &payload.replace(
                    "\"expected_epoch\":1",
                    "\"expected_epoch\":9007199254740992"
                )
            ))
            .is_err()
        );
        let mut root: Value = serde_json::from_slice(&request(1, "{}")).unwrap();
        root["actor"] = json!("owner");
        assert!(normalize(&serde_json::to_vec(&root).unwrap()).is_err());
        assert!(normalize(&request(4, "{}")).is_err());
        assert!(normalize(&vec![b' '; 64 * 1024 + 1]).is_err());
    }
}
