//! Pure review intent/identity adapters. Authenticated identity, policy, source
//! ownership, profile selection and publication remain host responsibilities.
use serde::Deserialize;
use serde_json::{Value, json};
use tinytree_forge_core::{
    accounts::{AccountId, PolicyEpoch, validate_principal},
    identity,
    review::{ForgeReviewDecision, LandingCheckProfile},
    review_commands::{self, ReviewCommand},
};
use tinytree_model::ChangeId;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    account_id: String,
    operation: i32,
    view: String,
    request_id: String,
    payload_json: String,
}
fn decode<T: serde::de::DeserializeOwned>(input: &[u8]) -> Result<T, String> {
    if input.len() > crate::MAX_INPUT {
        return Err("resource_exhausted: review adapter input".into());
    }
    serde_json::from_slice(input).map_err(|_| "invalid_argument: review adapter input".into())
}
fn id<const N: usize>(value: &str) -> Result<[u8; N], String> {
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("invalid_argument: review identifier".into());
    }
    let mut bytes = [0; N];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)
            .map_err(|_| "invalid_argument: review identifier")?;
    }
    Ok(bytes)
}
fn account(value: String) -> Result<AccountId, String> {
    let account = AccountId(value);
    account
        .validate()
        .map_err(|_| "invalid_argument: review account")?;
    Ok(account)
}
pub fn normalize(input: &[u8]) -> Result<Value, String> {
    let input: Command = decode(input)?;
    account(input.account_id)?;
    if input.view.len() > 128
        || input.view.chars().any(char::is_control)
        || input.request_id.len() > 128
        || !input
            .request_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err("invalid_argument: review framing".into());
    }
    let command = review_commands::parse(input.operation, input.payload_json.as_bytes())
        .map_err(|error| match error {
            tinytree_forge_core::Error::Limit(_) => "resource_exhausted: review payload",
            _ => "invalid_argument: review payload",
        })?
        .ok_or("unimplemented: review operation")?;
    serde_json::to_value(command).map_err(|_| "invalid_argument: review command response".into())
}

/// Internal server facts only. No route forwards a public payload to this enum;
/// deriving an ID does not establish any of these facts or issue a capability.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Identity {
    PushFingerprint {
        request_id: String,
        profile: LandingCheckProfile,
        payload_json: String,
    },
    Operation {
        principal: String,
        request_id: String,
    },
    Push {
        account_id: String,
        principal: String,
        request_id: String,
        epoch: u64,
        change_id: String,
        profile: LandingCheckProfile,
        payload_json: String,
        attempt: usize,
    },
    Vote {
        change_id: String,
        patchset_id: String,
        candidate_id: String,
        decision: ForgeReviewDecision,
    },
    Comment {
        change_id: String,
        patchset_id: String,
        body: String,
        path: Option<String>,
    },
    Attempt {
        account_id: String,
        change_id: String,
        patchset_id: String,
        attempt: usize,
    },
}
fn attempt_identity(
    account: &AccountId,
    change: ChangeId,
    patchset: [u8; 32],
    attempt: usize,
) -> Result<Value, String> {
    if attempt >= 8 {
        return Err("resource_exhausted: review attempts".into());
    }
    let owner = identity::review_owner(account, change);
    let token = format!("forge-{}-{attempt}", crate::hex(&patchset));
    let job = identity::landing_job_id(&format!("forge-{}", account.0), owner, &token);
    Ok(json!({"review_owner":crate::hex(&owner),"candidate_id":crate::hex(&job),"token":token}))
}
pub fn derive_identity(input: &[u8]) -> Result<Value, String> {
    let error = |_| "invalid_argument: review identity".to_owned();
    match decode::<Identity>(input)? {
        Identity::PushFingerprint {
            request_id,
            profile,
            payload_json,
        } => {
            tinytree_forge_core::upload::validate_request_id(&request_id).map_err(error)?;
            let Some(ReviewCommand::Push(intent)) =
                review_commands::parse(20, payload_json.as_bytes()).map_err(error)?
            else {
                return Err("invalid_argument: review push fingerprint".into());
            };
            let fingerprint =
                identity::push_fingerprint(&intent.into_request(&request_id, profile))
                    .map_err(error)?;
            Ok(json!({"fingerprint":crate::hex(&fingerprint)}))
        }
        Identity::Operation {
            principal,
            request_id,
        } => {
            validate_principal(&principal).map_err(error)?;
            tinytree_forge_core::upload::validate_request_id(&request_id).map_err(error)?;
            Ok(json!({"operation_id":crate::hex(&identity::operation_id(&principal, &request_id))}))
        }
        Identity::Push {
            account_id,
            principal,
            request_id,
            epoch,
            change_id,
            profile,
            payload_json,
            attempt,
        } => {
            let account = account(account_id)?;
            validate_principal(&principal).map_err(error)?;
            tinytree_forge_core::upload::validate_request_id(&request_id).map_err(error)?;
            if epoch == 0 || epoch > 9_007_199_254_740_991 {
                return Err("invalid_argument: review epoch".into());
            }
            let Some(ReviewCommand::Push(intent)) =
                review_commands::parse(20, payload_json.as_bytes()).map_err(error)?
            else {
                return Err("invalid_argument: review push identity".into());
            };
            let change = ChangeId(id(&change_id)?);
            let request = intent.into_request(&request_id, profile);
            let fingerprint = identity::push_fingerprint(&request).map_err(error)?;
            let patchset = identity::patchset_id(&account, change, fingerprint, PolicyEpoch(epoch));
            let mut result = attempt_identity(&account, change, patchset, attempt)?;
            result["fingerprint"] = json!(crate::hex(&fingerprint));
            result["patchset_id"] = json!(crate::hex(&patchset));
            result["operation_id"] =
                json!(crate::hex(&identity::operation_id(&principal, &request_id)));
            Ok(result)
        }
        Identity::Vote {
            change_id,
            patchset_id,
            candidate_id,
            decision,
        } => {
            let fingerprint = identity::decision_fingerprint(
                ChangeId(id(&change_id)?),
                id(&patchset_id)?,
                id(&candidate_id)?,
                decision,
            )
            .map_err(error)?;
            Ok(json!({"fingerprint":crate::hex(&fingerprint)}))
        }
        Identity::Comment {
            change_id,
            patchset_id,
            body,
            path,
        } => {
            let fingerprint = identity::comment_fingerprint(
                ChangeId(id(&change_id)?),
                id(&patchset_id)?,
                &body,
                path.as_deref(),
            )
            .map_err(error)?;
            Ok(json!({"fingerprint":crate::hex(&fingerprint)}))
        }
        Identity::Attempt {
            account_id,
            change_id,
            patchset_id,
            attempt,
        } => attempt_identity(
            &account(account_id)?,
            ChangeId(id(&change_id)?),
            id(&patchset_id)?,
            attempt,
        ),
    }
}

pub fn format(input: &[u8]) -> Result<Value, String> {
    let review = decode(input)?;
    let (state, result) = review_commands::format_review(&review)
        .map_err(|_| "invalid_argument: review presentation")?;
    Ok(json!({"state":state,"value":result}))
}
