//! Transport-authenticated session assertions, not membership or ACL grants.
//! The embedding supplies the exact matched credential row. These pure helpers
//! cannot authenticate their input; never expose TrustedIdentity as HTTP fields.
use crate::{
    Error, Result,
    account_commands::{EmptyRequest, MAX_ACCOUNT_COMMAND_BYTES},
    accounts::{AccountId, validate_principal},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeSession {
    pub schema_version: u32,
    pub account_id: String,
    pub identity: ForgeSessionIdentity,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ForgeSessionIdentity {
    Anonymous {},
    Authenticated {
        principal: String,
        credential_id: String,
    },
}
impl ForgeSession {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1 {
            return Err(Error::Invalid("forge session schema"));
        }
        AccountId(self.account_id.clone()).validate()?;
        if let ForgeSessionIdentity::Authenticated {
            principal,
            credential_id,
        } = &self.identity
        {
            validate_principal(principal)?;
            parse_digest(credential_id)?;
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifierScheme {
    Blake3,
    Sha256,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CredentialCeiling {
    Native {
        repositories: Vec<String>,
        write: bool,
        retain: bool,
        cloud: bool,
        landing_profiles: Vec<String>,
        forge_only: bool,
        forge_checker: bool,
    },
    Cloudflare {
        accounts: Vec<String>,
        operator: bool,
        credential_kind: String,
    },
}
fn set(values: &mut Vec<String>, maximum: usize, width: usize) -> Result<()> {
    if values.len() > maximum
        || values.iter().any(|value| {
            value.is_empty() || value.len() > width || value.chars().any(char::is_control)
        })
    {
        return Err(Error::Invalid("session credential ceiling"));
    }
    values.sort();
    values.dedup();
    Ok(())
}
impl CredentialCeiling {
    fn canonicalize(&mut self) -> Result<()> {
        match self {
            Self::Native {
                repositories,
                landing_profiles,
                ..
            } => {
                set(repositories, 4096, 128)?;
                set(landing_profiles, 32, 64)?;
            }
            Self::Cloudflare {
                accounts,
                credential_kind,
                ..
            } => {
                set(accounts, 64, 64)?;
                for account in accounts {
                    AccountId(account.clone()).validate()?;
                }
                if credential_kind != "forge" && credential_kind != "raw" {
                    return Err(Error::Invalid("session credential kind"));
                }
            }
        }
        Ok(())
    }
}
fn parse_digest(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Invalid("session credential identity"));
    }
    let mut out = [0; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::Invalid("session credential identity"))?;
    }
    Ok(out)
}
/// Public opaque identity, not the token verifier or a principal-only owner
/// hash. Changing any selected credential ceiling changes this identity.
pub fn credential_id(
    principal: &str,
    scheme: VerifierScheme,
    verifier: [u8; 32],
    mut ceiling: CredentialCeiling,
) -> Result<String> {
    validate_principal(principal)?;
    ceiling.canonicalize()?;
    let bytes = serde_json::to_vec(&(principal, scheme, verifier, ceiling))
        .map_err(|_| Error::Corrupt("session credential encoding"))?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-forge-session-credential-v1\0");
    hash.update(&bytes);
    Ok(hash.finalize().to_hex().to_string())
}
/// Internal Wasm host DTO. Neither these fields nor the derived reply establish
/// caller authentication. The HTTP handler must use its actual matched row.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TrustedIdentity {
    Anonymous {},
    Authenticated {
        principal: String,
        verifier_scheme: VerifierScheme,
        verifier: String,
        ceiling: CredentialCeiling,
    },
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionTrustedInput {
    pub account_id: String,
    pub payload_json: String,
    pub identity: TrustedIdentity,
}
pub fn derive_session(input: SessionTrustedInput) -> Result<ForgeSession> {
    if input.payload_json.len() > MAX_ACCOUNT_COMMAND_BYTES {
        return Err(Error::Limit("session payload"));
    }
    let _: EmptyRequest =
        serde_json::from_str(&input.payload_json).map_err(|_| Error::Invalid("session payload"))?;
    let identity = match input.identity {
        TrustedIdentity::Anonymous {} => ForgeSessionIdentity::Anonymous {},
        TrustedIdentity::Authenticated {
            principal,
            verifier_scheme,
            verifier,
            ceiling,
        } => {
            let id = credential_id(
                &principal,
                verifier_scheme,
                parse_digest(&verifier)?,
                ceiling,
            )?;
            ForgeSessionIdentity::Authenticated {
                principal,
                credential_id: id,
            }
        }
    };
    let session = ForgeSession {
        schema_version: 1,
        account_id: input.account_id,
        identity,
    };
    session.validate()?;
    Ok(session)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn ceiling(accounts: Vec<&str>) -> CredentialCeiling {
        CredentialCeiling::Cloudflare {
            accounts: accounts.into_iter().map(str::to_owned).collect(),
            operator: false,
            credential_kind: "forge".into(),
        }
    }
    #[test]
    fn credential_binding_uses_exact_token_and_ceiling_not_principal_or_set_order() {
        let first = credential_id(
            "member",
            VerifierScheme::Sha256,
            [1; 32],
            ceiling(vec!["alpha", "beta"]),
        )
        .unwrap();
        assert_eq!(
            first,
            credential_id(
                "member",
                VerifierScheme::Sha256,
                [1; 32],
                ceiling(vec!["beta", "alpha", "alpha"])
            )
            .unwrap()
        );
        assert_ne!(
            first,
            credential_id(
                "member",
                VerifierScheme::Sha256,
                [2; 32],
                ceiling(vec!["alpha", "beta"])
            )
            .unwrap()
        );
        assert_ne!(
            first,
            credential_id(
                "member",
                VerifierScheme::Sha256,
                [1; 32],
                ceiling(vec!["alpha"])
            )
            .unwrap()
        );
        assert_ne!(
            first,
            credential_id(
                "other",
                VerifierScheme::Sha256,
                [1; 32],
                ceiling(vec!["alpha", "beta"])
            )
            .unwrap()
        );
        assert_ne!(first, "01".repeat(32));
    }
    #[test]
    fn anonymous_struct_variant_and_session_payload_are_strict() {
        let session = derive_session(SessionTrustedInput {
            account_id: "alpha".into(),
            payload_json: "{}".into(),
            identity: TrustedIdentity::Anonymous {},
        })
        .unwrap();
        assert_eq!(
            serde_json::to_value(session).unwrap(),
            serde_json::json!({"schema_version":1,"account_id":"alpha","identity":{"kind":"anonymous"}})
        );
        assert!(
            serde_json::from_str::<ForgeSessionIdentity>(
                r#"{"kind":"anonymous","principal":"fake"}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ForgeSessionIdentity>(
                r#"{"kind":"authenticated","principal":"member","credential_id":"00","extra":1}"#
            )
            .is_err()
        );
        assert!(
            derive_session(SessionTrustedInput {
                account_id: "alpha".into(),
                payload_json: r#"{"principal":"fake"}"#.into(),
                identity: TrustedIdentity::Anonymous {}
            })
            .is_err()
        );
    }
}
