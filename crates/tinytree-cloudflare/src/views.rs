//! Strict view commands and trusted current-policy evaluation.
//! Command inputs are public selectors, not authority. Policy inputs come ONLY
//! from the account DO's current SQL rows and shared account authorization; never
//! deserialize policy/grant/spec-selection fields from a public request.
use serde::{
    Deserialize, Deserializer,
    de::{self, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use std::fmt;
use tinytree_forge_core::{
    account_commands::{
        EmptyRequest, PublishViewRequest, ResolveViewRequest, RetireViewRequest,
        mutation_fingerprint,
    },
    accounts::{
        AccountId, ForgeAccountMutation, ForgeViewVisibility, PolicyEpoch, ViewSpec, ViewVersion,
        validate_name, validate_path, view_version,
    },
};
const MAX_INPUT: usize = 8 * 1024 * 1024;
const MAX_SAFE: u64 = 9_007_199_254_740_991;
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    account_id: String,
    operation: u32,
    view: String,
    payload_json: String,
}
fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    if bytes.len() > MAX_INPUT {
        return Err("view input byte limit".into());
    }
    serde_json::from_slice(bytes).map_err(|_| "invalid strict view input".into())
}
fn epoch(epoch: u64) -> Result<(), String> {
    if epoch == 0 || epoch > MAX_SAFE {
        Err("invalid view policy epoch".into())
    } else {
        Ok(())
    }
}
/// The parent must use the shared bounded-view mutation fingerprint ceiling,
/// not the old 16KiB account/member ceiling. Hash preimages remain unchanged.
pub fn normalize_view_command(input: &[u8]) -> Result<Value, String> {
    let c: Command = parse(input)?;
    let account = AccountId(c.account_id);
    account.validate().map_err(|e| e.to_string())?;
    if !c.view.is_empty() {
        validate_name(&c.view).map_err(|e| e.to_string())?;
    }
    let payload = if c.payload_json.is_empty() {
        b"{}".as_slice()
    } else {
        c.payload_json.as_bytes()
    };
    match c.operation {
        4 => {
            let p: PublishViewRequest = parse(payload)?;
            p.spec.validate().map_err(|e| e.to_string())?;
            epoch(p.expected_epoch)?;
            if !c.view.is_empty() && c.view != p.spec.name {
                return Err("forge view binding mismatch".into());
            }
            if p.make_default && p.spec.visibility != ForgeViewVisibility::Public {
                return Err("forge default view must be public".into());
            }
            let version = view_version(&account, &p.spec).map_err(|e| e.to_string())?;
            let mutation = ForgeAccountMutation::PublishView {
                spec: p.spec.clone(),
                expected_epoch: PolicyEpoch(p.expected_epoch),
                make_default: p.make_default,
                follow_main: p.follow_main,
            };
            Ok(
                json!({"operation":4,"fingerprint":hex(&mutation_fingerprint(&mutation).map_err(|e|e.to_string())?),"publish":{"spec":p.spec,"version":version.0,"expected_epoch":p.expected_epoch,"make_default":p.make_default,"follow_main":p.follow_main}}),
            )
        }
        5 => {
            let _: EmptyRequest = parse(payload)?;
            Ok(json!({"operation":5,"fingerprint":null}))
        }
        6 => {
            let p: ResolveViewRequest = parse(payload)?;
            Ok(json!({"operation":6,"fingerprint":null,"selection":{"version":p.version}}))
        }
        11 => {
            let p: RetireViewRequest = parse(payload)?;
            epoch(p.expected_epoch)?;
            let mutation = ForgeAccountMutation::RetireView {
                version: ViewVersion(p.version),
                expected_epoch: PolicyEpoch(p.expected_epoch),
                view: c.view,
            };
            Ok(
                json!({"operation":11,"fingerprint":hex(&mutation_fingerprint(&mutation).map_err(|e|e.to_string())?),"retire":{"version":p.version,"expected_epoch":p.expected_epoch}}),
            )
        }
        _ => Err("unsupported view operation".into()),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Grant {
    is_admin: bool,
    member_roles: Option<Vec<String>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Policy {
    account_id: String,
    epoch: u64,
    requested_name: String,
    selected_version: [u8; 32],
    selected_spec: ViewSpec,
    current_name: String,
    current_version: [u8; 32],
    current_spec: ViewSpec,
    retired: bool,
    grant: Grant,
}
impl Policy {
    fn check(&self) -> Result<(), String> {
        let account = AccountId(self.account_id.clone());
        account.validate().map_err(|e| e.to_string())?;
        epoch(self.epoch)?;
        self.selected_spec.validate().map_err(|e| e.to_string())?;
        self.current_spec.validate().map_err(|e| e.to_string())?;
        if self.retired
            || self.selected_spec.name != self.current_name
            || self.current_spec.name != self.current_name
            || (!self.requested_name.is_empty() && self.requested_name != self.selected_spec.name)
            || view_version(&account, &self.selected_spec)
                .map_err(|e| e.to_string())?
                .0
                != self.selected_version
            || view_version(&account, &self.current_spec)
                .map_err(|e| e.to_string())?
                .0
                != self.current_version
        {
            return Err("permission_denied".into());
        }
        if let Some(roles) = &self.grant.member_roles {
            if roles.len() > 16 {
                return Err("view role limit".into());
            }
            for role in roles {
                validate_name(role).map_err(|e| e.to_string())?;
            }
        }
        let allowed = self.grant.is_admin
            || match &self.current_spec.visibility {
                ForgeViewVisibility::Public => true,
                ForgeViewVisibility::Members => self.grant.member_roles.is_some(),
                ForgeViewVisibility::Roles(roles) => self
                    .grant
                    .member_roles
                    .as_ref()
                    .is_some_and(|current| roles.iter().any(|role| current.contains(role))),
            };
        if !allowed {
            return Err("permission_denied".into());
        }
        Ok(())
    }
    fn description(&self) -> Value {
        json!({"account_id":self.account_id,"view":self.selected_spec.name,"view_version":self.selected_version,"revision":self.selected_spec.revision,"policy_epoch":self.epoch,"root_prefix":self.selected_spec.root_prefix})
    }
}
/// Trusted SQL host ONLY. It must supply the actual current named pointer and
/// its spec, not any two conveniently compatible specifications.
pub fn resolve_view_policy(input: &[u8]) -> Result<Value, String> {
    let policy: Policy = parse(input)?;
    policy.check()?;
    Ok(policy.description())
}
struct PathPage(Vec<String>);
impl<'de> Deserialize<'de> for PathPage {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct PageVisitor;
        impl<'de> Visitor<'de> for PageVisitor {
            type Value = PathPage;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("at most 512 bounded paths")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<PathPage, A::Error> {
                if seq.size_hint().is_some_and(|n| n > 512) {
                    return Err(de::Error::custom("view path page limit"));
                }
                let mut paths = Vec::new();
                let mut bytes = 0usize;
                while paths.len() < 512 {
                    let Some(path) = seq.next_element::<String>()? else {
                        return Ok(PathPage(paths));
                    };
                    bytes = bytes
                        .checked_add(path.len())
                        .filter(|n| *n <= 1024 * 1024)
                        .ok_or_else(|| de::Error::custom("view path page bytes"))?;
                    if path.len() > 4096 {
                        return Err(de::Error::custom("view path bytes"));
                    }
                    paths.push(path);
                }
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom("view path page limit"));
                }
                Ok(PathPage(paths))
            }
        }
        d.deserialize_seq(PageVisitor)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Paths {
    policy: Policy,
    paths: PathPage,
}
pub fn paths_allowed(input: &[u8]) -> Result<Value, String> {
    let p: Paths = parse(input)?;
    p.policy.check()?;
    let mut result = Vec::with_capacity(p.paths.0.len());
    for path in p.paths.0 {
        validate_path(&path, false).map_err(|e| e.to_string())?;
        result.push(json!({"path":path,"allowed":p.policy.selected_spec.allows(&path)&&p.policy.current_spec.allows(&path),"could_contain":p.policy.selected_spec.could_contain(&path)&&p.policy.current_spec.could_contain(&path)}));
    }
    Ok(json!({"paths":result}))
}
/// Internal finalization helper: derives follow-main immutable identity using
/// the canonical codec. The SQL host owns version quota/retirement/root checks.
pub fn advance_view_revision(input: &[u8]) -> Result<Value, String> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Advance {
        account_id: String,
        spec: ViewSpec,
        revision: [u8; 32],
    }
    let mut p: Advance = parse(input)?;
    let account = AccountId(p.account_id);
    account.validate().map_err(|e| e.to_string())?;
    if p.revision == [0; 32] {
        return Err("invalid canonical main".into());
    }
    p.spec.revision = p.revision;
    p.spec.validate().map_err(|e| e.to_string())?;
    let version = view_version(&account, &p.spec).map_err(|e| e.to_string())?;
    Ok(json!({"spec":p.spec,"version":version.0}))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn spec(visibility: ForgeViewVisibility) -> ViewSpec {
        ViewSpec {
            name: "public".into(),
            root_prefix: "".into(),
            include_prefixes: vec!["public".into()],
            exclude_prefixes: vec![],
            visibility,
            revision: [1; 32],
            export_allowed: true,
        }
    }
    fn policy(selected: ViewSpec, current: ViewSpec, grant: Value) -> Value {
        let account = AccountId("example".into());
        json!({"account_id":"example","epoch":3,"requested_name":"public","selected_version":view_version(&account,&selected).unwrap().0,"selected_spec":selected,"current_name":"public","current_version":view_version(&account,&current).unwrap().0,"current_spec":current,"retired":false,"grant":grant})
    }
    #[test]
    fn current_named_policy_controls_historical_visibility_and_path_intersection() {
        let old = spec(ForgeViewVisibility::Public);
        let mut current = spec(ForgeViewVisibility::Roles(vec!["reader".into()]));
        current.exclude_prefixes = vec!["public/secret".into()];
        current.revision = [2; 32];
        let anonymous = policy(
            old.clone(),
            current.clone(),
            json!({"is_admin":false,"member_roles":null}),
        );
        assert_eq!(
            resolve_view_policy(&serde_json::to_vec(&anonymous).unwrap()).unwrap_err(),
            "permission_denied"
        );
        let member = policy(
            old,
            current,
            json!({"is_admin":false,"member_roles":["reader"]}),
        );
        let output = paths_allowed(
            &serde_json::to_vec(
                &json!({"policy":member,"paths":["public/ok","public/secret/key","private/key"]}),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(output["paths"][0]["allowed"], true);
        assert_eq!(output["paths"][1]["allowed"], false);
        assert_eq!(output["paths"][2]["allowed"], false);
        let now_public = policy(
            spec(ForgeViewVisibility::Members),
            spec(ForgeViewVisibility::Public),
            json!({"is_admin":false,"member_roles":null}),
        );
        assert!(
            resolve_view_policy(&serde_json::to_vec(&now_public).unwrap()).is_ok(),
            "historical visibility does not replace current named policy"
        );
        let mut wrong = now_public;
        wrong["current_name"] = json!("other");
        assert!(resolve_view_policy(&serde_json::to_vec(&wrong).unwrap()).is_err());
    }
    #[test]
    fn publish_defaults_and_shared_replay_preimage_are_native_exact() {
        let spec = spec(ForgeViewVisibility::Public);
        let command = json!({"account_id":"example","operation":4,"view":"public","payload_json":serde_json::to_string(&json!({"spec":spec,"expected_epoch":3,"make_default":true})).unwrap()});
        let output = normalize_view_command(&serde_json::to_vec(&command).unwrap()).unwrap();
        assert_eq!(output["publish"]["follow_main"], true);
        let mutation = ForgeAccountMutation::PublishView {
            spec: spec.clone(),
            expected_epoch: PolicyEpoch(3),
            make_default: true,
            follow_main: true,
        };
        assert_eq!(
            output["fingerprint"],
            hex(&mutation_fingerprint(&mutation).unwrap())
        );
        assert_eq!(
            output["publish"]["version"],
            json!(view_version(&AccountId("example".into()), &spec).unwrap().0)
        );
        let mut wrong = command;
        wrong["view"] = json!("different");
        assert!(normalize_view_command(&serde_json::to_vec(&wrong).unwrap()).is_err());
        assert!(normalize_view_command(br#"{"account_id":"example","operation":5,"view":"","payload_json":"{\"actor\":\"owner\"}"}"#).is_err());
    }
}
