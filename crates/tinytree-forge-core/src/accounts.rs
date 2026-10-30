//! Storage-free account and immutable-view wire types and policy rules.
//! These identifiers, matchers and DTOs do not confer access or resolve current
//! membership. Embeddings must authenticate actors and enforce current policy.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

pub const MAX_VIEW_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AccountId(pub String);
impl AccountId {
    pub fn validate(&self) -> Result<()> {
        if self.0.is_empty()
            || self.0.len() > 64
            || !self
                .0
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(Error::Invalid("forge account identifier"));
        }
        Ok(())
    }
    pub fn repository_id(&self) -> String {
        format!("forge-{}", self.0)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ViewVersion(pub [u8; 32]);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PolicyEpoch(pub u64);

/// Trusted authenticated identity; intentionally not deserializable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForgeActor {
    pub principal: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForgeAction {
    Read,
    Write,
    Review,
    Admin,
    Export,
}
#[derive(Clone, Debug)]
pub struct ForgeAccess {
    pub account: AccountId,
    pub repository: String,
    pub epoch: PolicyEpoch,
    pub can_write: bool,
    pub can_review: bool,
    pub is_admin: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeMember {
    pub principal: String,
    pub can_write: bool,
    pub can_review: bool,
    pub is_admin: bool,
    pub roles: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeAccount {
    #[serde(rename = "account_id")]
    pub id: AccountId,
    #[serde(rename = "monorepo_id")]
    pub repository: String,
    #[serde(rename = "policy_epoch")]
    pub epoch: PolicyEpoch,
    pub public_view: Option<ViewVersion>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "roles", rename_all = "snake_case")]
pub enum ForgeViewVisibility {
    Public,
    Members,
    Roles(Vec<String>),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewSpec {
    pub name: String,
    pub root_prefix: String,
    pub include_prefixes: Vec<String>,
    pub exclude_prefixes: Vec<String>,
    pub visibility: ForgeViewVisibility,
    pub revision: [u8; 32],
    pub export_allowed: bool,
}
impl ViewSpec {
    pub fn validate(&self) -> Result<()> {
        validate_name(&self.name)?;
        validate_path(&self.root_prefix, true)?;
        if self.include_prefixes.is_empty()
            || self.include_prefixes.len() > 64
            || self.exclude_prefixes.len() > 64
        {
            return Err(Error::Limit("forge view path rules"));
        }
        for prefix in self.include_prefixes.iter().chain(&self.exclude_prefixes) {
            validate_path(prefix, true)?;
        }
        if let ForgeViewVisibility::Roles(roles) = &self.visibility {
            if roles.is_empty() || roles.len() > 16 {
                return Err(Error::Limit("forge view roles"));
            }
            for role in roles {
                validate_name(role)?;
            }
        }
        if self.revision == [0; 32] {
            return Err(Error::Invalid("forge view revision"));
        }
        if serde_json::to_vec(self)
            .map_err(|_| Error::Corrupt("metadata encoding"))?
            .len()
            > MAX_VIEW_BYTES
        {
            return Err(Error::Limit("forge view record"));
        }
        Ok(())
    }
    /// Match component prefixes only. Validate the canonical path separately;
    /// this does not authenticate an actor or intersect current account policy.
    pub fn allows(&self, canonical: &str) -> bool {
        let relative = if self.root_prefix.is_empty() {
            Some(canonical)
        } else {
            canonical
                .strip_prefix(&self.root_prefix)
                .and_then(|tail| tail.strip_prefix('/'))
        };
        relative.is_some_and(|path| {
            self.include_prefixes
                .iter()
                .any(|p| prefix_matches(p, path))
                && !self
                    .exclude_prefixes
                    .iter()
                    .any(|p| prefix_matches(p, path))
        })
    }
    /// Determine whether traversal can reach an included path. The empty path
    /// represents the repository root, not an independently readable file.
    pub fn could_contain(&self, canonical: &str) -> bool {
        let full = |relative: &str| {
            if self.root_prefix.is_empty() {
                relative.to_owned()
            } else if relative.is_empty() {
                self.root_prefix.clone()
            } else {
                format!("{}/{relative}", self.root_prefix)
            }
        };
        self.include_prefixes.iter().any(|p| {
            let allowed = full(p);
            prefix_matches(&allowed, canonical) || prefix_matches(canonical, &allowed)
        }) && !self
            .exclude_prefixes
            .iter()
            .any(|p| prefix_matches(&full(p), canonical))
    }
}

pub fn validate_name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(Error::Invalid("forge name"));
    }
    Ok(())
}
pub fn validate_principal(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(Error::Invalid("forge principal"));
    }
    Ok(())
}
pub fn validate_path(value: &str, empty: bool) -> Result<()> {
    if empty && value.is_empty() {
        return Ok(());
    }
    if value.is_empty()
        || value.len() > 4096
        || value.split('/').count() > 64
        || value.split('/').any(|s| {
            s.is_empty()
                || s == "."
                || s == ".."
                || s.contains('\\')
                || s.chars().any(char::is_control)
        })
    {
        return Err(Error::Invalid("forge path"));
    }
    Ok(())
}
fn prefix_matches(prefix: &str, path: &str) -> bool {
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|s| s.starts_with('/'))
}
/// Hash the exact version-one account/spec wire binding. Callers validate the
/// account and spec before accepting a version; this hash is not read authority.
pub fn view_version(account: &AccountId, spec: &ViewSpec) -> Result<ViewVersion> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-forge-view-v1\0");
    hash.update(account.0.as_bytes());
    hash.update(&[0]);
    hash.update(&serde_json::to_vec(spec).map_err(|_| Error::Corrupt("metadata encoding"))?);
    Ok(ViewVersion(*hash.finalize().as_bytes()))
}

/// Account mutation intents. An embedding must atomically bind durable replay
/// receipts to these exact values when applying a mutation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
pub enum ForgeAccountMutation {
    Provision,
    RetireView {
        version: ViewVersion,
        expected_epoch: PolicyEpoch,
        view: String,
    },
    SetMember {
        member: ForgeMember,
        expected_epoch: PolicyEpoch,
    },
    PublishView {
        spec: ViewSpec,
        expected_epoch: PolicyEpoch,
        make_default: bool,
        follow_main: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view() -> ViewSpec {
        ViewSpec {
            name: "public".into(),
            root_prefix: "pkg".into(),
            include_prefixes: vec!["src".into()],
            exclude_prefixes: vec!["src/private".into()],
            visibility: ForgeViewVisibility::Roles(vec!["member".into()]),
            revision: [1; 32],
            export_allowed: true,
        }
    }

    #[test]
    fn account_and_portable_path_rules_preserve_existing_boundaries() {
        for account in ["example", "a-1", "---"] {
            let id = AccountId(account.into());
            id.validate().unwrap();
            assert_eq!(id.repository_id(), format!("forge-{account}"));
        }
        for account in ["", "Example", "_internal", "a/b", "a b"] {
            assert!(matches!(
                AccountId(account.into()).validate(),
                Err(Error::Invalid("forge account identifier"))
            ));
        }
        assert!(AccountId("a".repeat(64)).validate().is_ok());
        assert!(AccountId("a".repeat(65)).validate().is_err());
        validate_path("", true).unwrap();
        validate_path("pkg/日本語.txt", false).unwrap();
        validate_path(&vec!["x"; 64].join("/"), false).unwrap();
        for path in [
            "",
            "/root",
            "root/",
            "root//file",
            "root/../file",
            "root/./file",
            "root\\file",
            "root/\nfile",
        ] {
            assert!(matches!(
                validate_path(path, false),
                Err(Error::Invalid("forge path"))
            ));
        }
        assert!(validate_path(&vec!["x"; 65].join("/"), false).is_err());
        assert!(validate_path(&"x".repeat(4097), false).is_err());
        validate_name("Case-sensitive_role-1").unwrap();
        validate_principal("member@example.invalid").unwrap();
        assert!(matches!(
            validate_name("a/b"),
            Err(Error::Invalid("forge name"))
        ));
        assert!(matches!(
            validate_principal("member\n"),
            Err(Error::Invalid("forge principal"))
        ));
    }

    #[test]
    fn view_matching_confines_root_and_exclusions_at_component_boundaries() {
        let spec = view();
        spec.validate().unwrap();
        for (path, readable, traversable) in [
            ("", false, true),
            ("pkg", false, true),
            ("pkg/src", true, true),
            ("pkg/src/lib.rs", true, true),
            ("pkg/src/private", false, false),
            ("pkg/src/private/key", false, false),
            ("pkg/src/private-other", true, true),
            ("pkg/src-other/file", false, false),
            ("pkg-other/src/lib.rs", false, false),
        ] {
            assert_eq!(spec.allows(path), readable, "read {path}");
            assert_eq!(spec.could_contain(path), traversable, "traverse {path}");
        }
    }

    #[test]
    fn view_identity_keeps_native_wire_format_and_account_binding() {
        let spec = view();
        let encoded = serde_json::to_string(&spec).unwrap();
        assert_eq!(
            encoded,
            r#"{"name":"public","root_prefix":"pkg","include_prefixes":["src"],"exclude_prefixes":["src/private"],"visibility":{"kind":"roles","roles":["member"]},"revision":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],"export_allowed":true}"#
        );
        let version = view_version(&AccountId("example".into()), &spec).unwrap();
        assert_eq!(
            version.0,
            [
                180, 89, 252, 83, 86, 176, 181, 236, 101, 58, 34, 219, 167, 7, 58, 170, 163, 147,
                182, 93, 78, 211, 23, 205, 31, 28, 110, 246, 188, 184, 178, 250
            ]
        );
        assert_ne!(
            version,
            view_version(&AccountId("other".into()), &spec).unwrap()
        );
        let with_actor = encoded.replacen("{", r#"{"actor":"other","#, 1);
        assert!(serde_json::from_str::<ViewSpec>(&with_actor).is_err());
        let intent = ForgeAccountMutation::PublishView {
            spec,
            expected_epoch: PolicyEpoch(2),
            make_default: true,
            follow_main: false,
        };
        let wire = serde_json::to_value(&intent).unwrap();
        assert_eq!(wire["operation"], "PublishView");
        assert_eq!(wire["expected_epoch"], 2);
        assert!(serde_json::from_value::<ForgeAccountMutation>(wire).is_ok());
    }

    #[test]
    fn invalid_view_rules_keep_invalid_and_limit_error_categories() {
        let mut spec = view();
        spec.revision = [0; 32];
        assert!(matches!(
            spec.validate(),
            Err(Error::Invalid("forge view revision"))
        ));
        spec = view();
        spec.include_prefixes.clear();
        assert!(matches!(
            spec.validate(),
            Err(Error::Limit("forge view path rules"))
        ));
        spec = view();
        spec.visibility = ForgeViewVisibility::Roles(vec![]);
        assert!(matches!(
            spec.validate(),
            Err(Error::Limit("forge view roles"))
        ));
        spec = view();
        spec.include_prefixes = vec!["\"".repeat(4096); 64];
        spec.exclude_prefixes = spec.include_prefixes.clone();
        assert!(matches!(
            spec.validate(),
            Err(Error::Limit("forge view record"))
        ));
    }
}
