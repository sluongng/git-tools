//! Shared account request schemas and deterministic authoring rules. None of
//! these pure functions authenticate a principal or grant account membership.
use crate::{
    Error, Result,
    accounts::{AccountId, ForgeAccountMutation, ForgeMember, ViewSpec, MAX_VIEW_BYTES, validate_name, validate_principal},
};
use serde::{Deserialize, Serialize};
use tinytree_model::{
    self as model, ChangeId, Commit, CommitId, EncodedObject, Object, Signature, Timestamp, TreeId,
    TreeMerge,
};

pub const MAX_MEMBERS: usize = 64;
pub const MAX_ROLES: usize = 16;
pub const MAX_ACCOUNT_COMMAND_BYTES: usize = 16 * 1024;
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyRequest {}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SetMemberRequest {
    pub member: ForgeMember,
    pub expected_epoch: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublishViewRequest {
    pub spec: ViewSpec,
    pub expected_epoch: u64,
    pub make_default: bool,
    #[serde(default = "follow_main_default")]
    pub follow_main: bool,
}
fn follow_main_default() -> bool { true }
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetireViewRequest {
    pub version: [u8; 32],
    pub expected_epoch: u64,
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveViewRequest {
    #[serde(default)]
    pub version: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<ProjectionProgress>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectionProgress {
    pub job_id: String,
    pub expected_generation: u64,
}
/// A pending selection is a bounded control assertion, never an object or
/// continuing authorization proof. Clients pin it for the same preparation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveViewPending {
    pub account_id: String,
    pub view: String,
    pub view_version: [u8; 32],
    pub revision: [u8; 32],
    pub policy_epoch: u64,
    pub root_prefix: String,
    pub progress: ProjectionProgress,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadFileRequest {
    #[serde(default)]
    pub version: Option<[u8; 32]>,
    pub path: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadObjectRequest {
    #[serde(default)]
    pub version: Option<[u8; 32]>,
    pub kind: u8,
    pub id: [u8; 32],
    #[serde(default)]
    pub offset: u64,
    #[serde(default = "object_chunk_size")]
    pub max_bytes: usize,
}
fn object_chunk_size() -> usize { 64 * 1024 }

pub fn validate_member(member: &ForgeMember) -> Result<()> {
    validate_principal(&member.principal)?;
    if member.roles.len() > MAX_ROLES {
        return Err(Error::Limit("forge member roles"));
    }
    for role in &member.roles {
        validate_name(role)?;
    }
    Ok(())
}
/// Apply only after current administrative authorization and epoch CAS. A false
/// result denotes exact unchanged membership and must not increment the epoch.
pub fn member_change(
    owner: &str,
    current: Option<&ForgeMember>,
    member: &ForgeMember,
    member_count: usize,
) -> Result<bool> {
    validate_member(member)?;
    if member.principal == owner {
        return Err(Error::Invalid("forge owner membership"));
    }
    if current.is_none() && member_count >= MAX_MEMBERS {
        return Err(Error::Limit("forge members"));
    }
    Ok(current != Some(member))
}
/// Exactly the mutation bytes used by local account replay receipts. Actor,
/// account and request ID are separately bound by the authoritative embedding.
pub fn mutation_fingerprint(mutation: &ForgeAccountMutation) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(mutation).map_err(|_| Error::Corrupt("metadata encoding"))?;
    let limit = match mutation {
        ForgeAccountMutation::PublishView { .. } => MAX_VIEW_BYTES + MAX_ACCOUNT_COMMAND_BYTES,
        _ => MAX_ACCOUNT_COMMAND_BYTES,
    };
    if bytes.len() > limit {
        return Err(Error::Limit("forge account command"));
    }
    Ok(*blake3::hash(&bytes).as_bytes())
}
pub struct Genesis {
    pub empty: EncodedObject,
    pub commit: EncodedObject,
}
/// Deterministic canonical native genesis, shared by filesystem and object-store
/// hosts. Installing these bytes is separate from an atomic ownership claim.
pub fn genesis(account: &AccountId, limits: model::Limits) -> model::Result<Genesis> {
    account
        .validate()
        .map_err(|_| model::Error::Invalid("forge account identifier"))?;
    let empty = model::empty_tree();
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-forge-genesis-v1\0");
    hash.update(account.0.as_bytes());
    let mut change = [0; 16];
    change.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    let signature = Signature {
        name: "Tinytree".into(),
        email: "tinytree@users.noreply.github.com".into(),
        timestamp: Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    let commit = model::encode(
        &Object::Commit(Commit {
            parents: vec![CommitId::ROOT],
            predecessors: vec![],
            root_tree: TreeMerge {
                terms: vec![TreeId(empty.descriptor.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: ChangeId(change),
            description: "Initialize monorepo".into(),
            author: signature.clone(),
            committer: signature,
            secure_signature: None,
        }),
        limits,
    )?;
    Ok(Genesis { empty, commit })
}
#[cfg(test)]
mod tests {
    use super::*;
    fn member() -> ForgeMember {
        ForgeMember {
            principal: "member".into(),
            can_write: true,
            can_review: false,
            is_admin: false,
            roles: vec!["developer".into()],
        }
    }
    #[test]
    fn strict_shared_schemas_and_exact_replay_bytes() {
        assert!(serde_json::from_str::<EmptyRequest>("{\"actor\":\"owner\"}").is_err());
        let value = serde_json::json!({"member":member(),"expected_epoch":1});
        let encoded = value.to_string();
        assert!(serde_json::from_str::<SetMemberRequest>(&encoded).is_ok());
        assert!(
            serde_json::from_str::<SetMemberRequest>(&encoded.replacen(
                "\"expected_epoch\":1",
                "\"expected_epoch\":1,\"expected_epoch\":2",
                1
            ))
            .is_err()
        );
        assert_eq!(
            mutation_fingerprint(&ForgeAccountMutation::Provision).unwrap(),
            *blake3::hash(br#"{"operation":"Provision"}"#).as_bytes()
        );
        let mutation = ForgeAccountMutation::SetMember {
            member: member(),
            expected_epoch: crate::accounts::PolicyEpoch(1),
        };
        assert_eq!(
            mutation_fingerprint(&mutation).unwrap(),
            *blake3::hash(&serde_json::to_vec(&mutation).unwrap()).as_bytes()
        );
        let mut invalid = member();
        invalid.roles = vec!["role".into(); 17];
        assert!(validate_member(&invalid).is_err());
        // View publications retain the native 1MiB spec envelope; the smaller
        // account/member command limit must not restrict this distinct command.
        let spec = ViewSpec {
            name: "public".into(), root_prefix: String::new(),
            include_prefixes: vec!["x".repeat(4096); 8], exclude_prefixes: vec![],
            visibility: crate::accounts::ForgeViewVisibility::Public,
            revision: [1; 32], export_allowed: true,
        };
        spec.validate().unwrap();
        let mutation = ForgeAccountMutation::PublishView {
            spec, expected_epoch: crate::accounts::PolicyEpoch(1),
            make_default: true, follow_main: true,
        };
        let bytes = serde_json::to_vec(&mutation).unwrap();
        assert!(bytes.len() > MAX_ACCOUNT_COMMAND_BYTES);
        assert_eq!(mutation_fingerprint(&mutation).unwrap(), *blake3::hash(&bytes).as_bytes());
    }
    #[test]
    fn owner_and_noop_rules_and_canonical_genesis() {
        let member = member();
        assert!(!member_change("owner", Some(&member), &member, 64).unwrap());
        assert!(member_change("owner", None, &member, 64).is_err());
        assert!(member_change("member", Some(&member), &member, 1).is_err());
        let a = genesis(&AccountId("sample".into()), Default::default()).unwrap();
        let b = genesis(&AccountId("sample".into()), Default::default()).unwrap();
        assert_eq!(a.commit, b.commit);
        assert_ne!(
            a.commit.descriptor,
            genesis(&AccountId("other".into()), Default::default())
                .unwrap()
                .commit
                .descriptor
        );
        model::verify(&a.commit.descriptor, &a.commit.bytes, Default::default()).unwrap();
        let Object::Commit(c) = model::decode(&a.commit.bytes, Default::default()).unwrap() else {
            panic!("commit")
        };
        assert_eq!(c.parents, vec![CommitId::ROOT]);
        assert_eq!(c.root_tree.terms, vec![TreeId(a.empty.descriptor.key.id.0)]);
        assert_eq!(c.author.timestamp.millis_since_epoch, 0);
    }
}
