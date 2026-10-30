//! Strict public review intents and wire presentation, without authentication,
//! current policy, storage, profile selection or candidate/check authority.
use crate::{Error, Result, review::{ForgeReview, ForgeReviewDecision, ForgeReviewDependency, ForgeReviewLineageHint, ForgeReviewPushRequest, LandingCheckProfile}};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinytree_model::CommitId;

pub const MAX_PAYLOAD_BYTES: usize = 16 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PushIntent {
    pub source_base: [u8;32],
    pub source_head: [u8;32],
    pub view_version: [u8;32],
    pub expected_patchset: Option<[u8;32]>,
    pub predecessor: Option<ForgeReviewDependency>,
    pub provenance: Option<[u8;32]>,
    pub lineage: Vec<ForgeReviewLineageHint>,
}
impl PushIntent {
    /// Request identity and selected check profile come from the trusted host,
    /// never from the public intent. This conversion grants no write authority.
    pub fn into_request(self, request_id: &str, check_profile: LandingCheckProfile) -> ForgeReviewPushRequest {
        ForgeReviewPushRequest { request_id: request_id.into(), source_base: CommitId(self.source_base), source_head: CommitId(self.source_head),
            view_version: self.view_version, expected_patchset: self.expected_patchset, predecessor: self.predecessor,
            provenance: self.provenance, lineage: self.lineage, check_profile }
    }
}
pub const ADMISSION_PENDING_STATE: &str = "admission_pending";
/// Preparation has not admitted a patchset or reserved a change. This exact
/// intent echo is not a candidate, acceptance receipt or reusable capability.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionPending {
    pub schema_version: u32,
    pub request_id: String,
    pub source_base: String,
    pub source_head: String,
    pub view_version: String,
    pub phase: String,
}
impl AdmissionPending {
    pub fn from_intent(request_id: &str, intent: &PushIntent) -> Self {
        Self { schema_version: 1, request_id: request_id.into(), source_base: hex(&intent.source_base),
            source_head: hex(&intent.source_head), view_version: hex(&intent.view_version), phase: "admission".into() }
    }
    /// Validate against the caller's original frozen request, not mutable local
    /// workspace state or a newly selected/default view. Exact equality also
    /// enforces canonical lowercase public identifier strings.
    pub fn validate_against(&self, request_id: &str, intent: &PushIntent) -> Result<()> {
        if self != &Self::from_intent(request_id, intent) {
            return Err(Error::Invalid("review admission pending binding"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DecisionIntent {
    Comment { body: String, path: Option<String> },
    Vote { decision: ForgeReviewDecision, candidate_id: [u8;32] },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "operation", content = "intent", rename_all = "snake_case")]
pub enum ReviewCommand {
    Push(PushIntent),
    Get { change_id: [u8;16], patchset_id: Option<[u8;32]> },
    Operation,
    Decide { change_id: [u8;16], patchset_id: [u8;32], intent: DecisionIntent },
    Land { change_id: [u8;16], patchset_id: [u8;32], candidate_id: [u8;32] },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dependency { change_id: String, patchset_id: String }
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Push {
    source_base: String, source_head: String, view_version: String,
    expected_patchset: Option<String>, predecessor: Option<Dependency>, provenance: Option<String>,
    #[serde(default)] lineage: Vec<ForgeReviewLineageHint>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Get { change_id: String, patchset_id: Option<String> }
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Decide {
    change_id: String, patchset_id: String, decision: String,
    candidate_id: Option<String>, body: Option<String>, path: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Land { change_id: String, patchset_id: String, candidate_id: String }
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::Invalid("invalid review payload"))
}
fn id<const N:usize>(text: &str) -> Result<[u8;N]> {
    if text.len() != N*2 { return Err(Error::Invalid("invalid lowercase hex identifier")); }
    let mut output=[0;N];
    for (index,pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let digit=|b:u8|match b {b'0'..=b'9'=>Some(b-b'0'),b'a'..=b'f'=>Some(b-b'a'+10),_=>None};
        output[index]=digit(pair[0]).zip(digit(pair[1])).map(|(a,b)|a*16+b).ok_or(Error::Invalid("invalid lowercase hex identifier"))?;
    }
    Ok(output)
}
/// Parse only review operations. Other operations retain ownership of their own
/// payload, including size and shape checks. No actor/profile fields are public.
pub fn parse(operation: i32, payload: &[u8]) -> Result<Option<ReviewCommand>> {
    if !(20..=24).contains(&operation) { return Ok(None); }
    if payload.len()>MAX_PAYLOAD_BYTES { return Err(Error::Limit("review payload limit")); }
    Ok(Some(match operation {
        20 => {
            let p:Push=decode(payload)?;
            ReviewCommand::Push(PushIntent { source_base:id(&p.source_base)?,source_head:id(&p.source_head)?,view_version:id(&p.view_version)?,
                expected_patchset:p.expected_patchset.as_deref().map(id).transpose()?,
                predecessor:p.predecessor.map(|d|Ok::<_,Error>(ForgeReviewDependency{change_id:id(&d.change_id)?,patchset_id:id(&d.patchset_id)?})).transpose()?,
                provenance:p.provenance.as_deref().map(id).transpose()?,lineage:p.lineage })
        }
        21 => { let p:Get=decode(payload)?; ReviewCommand::Get { change_id:id(&p.change_id)?,patchset_id:p.patchset_id.as_deref().map(id).transpose()? } }
        22 => { let _:Empty=decode(payload)?; ReviewCommand::Operation }
        23 => {
            let p:Decide=decode(payload)?;
            let change_id=id(&p.change_id)?; let patchset_id=id(&p.patchset_id)?;
            let intent=if p.decision=="comment" {
                if p.candidate_id.is_some() { return Err(Error::Invalid("candidate_id only applies to review decisions")); }
                DecisionIntent::Comment { body:p.body.ok_or(Error::Invalid("comment body required"))?,path:p.path }
            } else {
                if p.body.is_some()||p.path.is_some() { return Err(Error::Invalid("body and path require comment")); }
                let decision=match p.decision.as_str() { "approve"=>ForgeReviewDecision::Approve,"request_changes"=>ForgeReviewDecision::RequestChanges,
                    _=>return Err(Error::Invalid("unknown review decision")) };
                DecisionIntent::Vote { decision,candidate_id:id(p.candidate_id.as_deref().ok_or(Error::Invalid("candidate_id required for review decision"))?)? }
            };
            ReviewCommand::Decide { change_id,patchset_id,intent }
        }
        24 => { let p:Land=decode(payload)?; ReviewCommand::Land { change_id:id(&p.change_id)?,patchset_id:id(&p.patchset_id)?,candidate_id:id(&p.candidate_id)? } }
        _=>unreachable!(),
    }))
}
pub fn hex(bytes: &[u8]) -> String { bytes.iter().map(|b|format!("{b:02x}")).collect() }
/// Format the native durable DTO's existing public hex field set. This neither
/// sanitizes a candidate nor issues a projection/read capability; the embedding
/// must authorize the review and add only authorized result-projection extras.
pub fn format_review(review: &ForgeReview) -> Result<(String,Value)> {
    let state=serde_json::to_value(review.state).map_err(|_|Error::Corrupt("review encoding failed"))?
        .as_str().unwrap_or("unknown").to_owned();
    let mut value=serde_json::to_value(review).map_err(|_|Error::Corrupt("review encoding failed"))?;
    fn convert(value:&mut Value) {
        match value {
            Value::Object(fields)=>for (name,value) in fields {
                if matches!(name.as_str(),"change_id"|"patchset_id"|"source_base"|"source_head"|"view_version"|"provenance"|"id"|"head"|"root_tree"|"target"|"old"|"new") {
                    if let Value::Array(bytes)=value {
                        if bytes.iter().all(|v|v.as_u64().is_some_and(|n|n<256)) {
                            *value=Value::String(hex(&bytes.iter().map(|v|v.as_u64().unwrap() as u8).collect::<Vec<_>>()));continue;
                        }
                    }
                }
                convert(value);
            },
            Value::Array(items)=>for value in items {convert(value);},
            _=>{},
        }
    }
    convert(&mut value);Ok((state,value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::review::{ForgeReviewCandidate,ForgeReviewComment,ForgeReviewRewrite,ForgeReviewState};
    use serde_json::json;
    fn bytes(value:Value)->Vec<u8>{serde_json::to_vec(&value).unwrap()}
    #[test]
    fn public_push_preserves_identity_without_accepting_host_profile() {
        let payload=json!({"source_base":"01".repeat(32),"source_head":"02".repeat(32),"view_version":"03".repeat(32),
            "expected_patchset":"04".repeat(32),"predecessor":{"change_id":"05".repeat(16),"patchset_id":"06".repeat(32)},
            "provenance":"07".repeat(32),"lineage":[{"operation":"copy","source_path":"old.txt","target_path":"new.txt"}]});
        let Some(ReviewCommand::Push(intent))=parse(20,&bytes(payload.clone())).unwrap() else {panic!("push intent")};
        let request=intent.into_request("host-request",LandingCheckProfile{name:"selected".into(),version:"v1".into()});
        assert_eq!(request.request_id,"host-request");assert_eq!(request.source_base.0,[1;32]);assert_eq!(request.source_head.0,[2;32]);
        assert_eq!(request.view_version,[3;32]);assert_eq!(request.expected_patchset,Some([4;32]));
        assert_eq!(request.predecessor.unwrap(),ForgeReviewDependency{change_id:[5;16],patchset_id:[6;32]});
        assert_eq!(request.provenance,Some([7;32]));assert_eq!(request.lineage[0].target_path,"new.txt");assert_eq!(request.check_profile.name,"selected");
        for field in ["actor","check_profile","profile","epoch","committer","candidate_ready"] {
            let mut forged=payload.clone();forged[field]=json!("caller-selected");assert!(parse(20,&bytes(forged)).is_err());
        }
        let duplicate=String::from_utf8(bytes(payload.clone())).unwrap().replacen("\"source_base\":",&format!("\"source_base\":\"{}\",\"source_base\":","01".repeat(32)),1);
        assert!(parse(20,duplicate.as_bytes()).is_err());
        let mut uppercase=payload;uppercase["source_head"]=json!("AB".repeat(32));assert!(parse(20,&bytes(uppercase)).is_err());
    }
    #[test]
    fn decision_shapes_historical_lookup_and_nonreview_payload_ownership_remain_exact() {
        let base=json!({"change_id":"11".repeat(16),"patchset_id":"22".repeat(32),"decision":"approve","candidate_id":"33".repeat(32)});
        assert!(matches!(parse(23,&bytes(base.clone())).unwrap(),Some(ReviewCommand::Decide{intent:DecisionIntent::Vote{decision:ForgeReviewDecision::Approve,..},..})));
        let mut changes=base.clone();changes["decision"]=json!("request_changes");assert!(parse(23,&bytes(changes)).is_ok());
        for (field,value) in [("body",json!("text")),("path",json!("file.txt"))] {
            let mut mixed=base.clone();mixed[field]=value;assert!(parse(23,&bytes(mixed)).is_err());
        }
        let comment=json!({"change_id":"11".repeat(16),"patchset_id":"22".repeat(32),"decision":"comment","body":"note","path":"file.txt"});
        assert!(matches!(parse(23,&bytes(comment.clone())).unwrap(),Some(ReviewCommand::Decide{intent:DecisionIntent::Comment{..},..})));
        let mut mixed=comment.clone();mixed["candidate_id"]=json!("33".repeat(32));assert!(parse(23,&bytes(mixed)).is_err());
        let mut missing=comment;missing.as_object_mut().unwrap().remove("body");assert!(parse(23,&bytes(missing)).is_err());
        let historical=json!({"change_id":"11".repeat(16),"patchset_id":"22".repeat(32)});
        assert_eq!(parse(21,&bytes(historical)).unwrap(),Some(ReviewCommand::Get{change_id:[0x11;16],patchset_id:Some([0x22;32])}));
        assert_eq!(parse(22,b"{}").unwrap(),Some(ReviewCommand::Operation));
        assert!(parse(22,br#"{"request_id":"caller-override"}"#).is_err());
        assert!(parse(22,b"null").is_err());assert!(parse(22,b"{}{}").is_err());
        assert!(matches!(parse(22,&vec![b' ';MAX_PAYLOAD_BYTES+1]),Err(Error::Limit("review payload limit"))));
        assert_eq!(parse(10,&vec![b' ';MAX_PAYLOAD_BYTES+1]).unwrap(),None);
    }
    #[test]
    fn public_format_preserves_historical_state_and_exact_nested_hex_field_schema() {
        let review=ForgeReview{change_id:[1;16],owner:"writer".into(),patchset_id:[2;32],patchset_number:2,is_current:false,
            source_base:[3;32],source_head:[4;32],view_version:[5;32],predecessor:Some(ForgeReviewDependency{change_id:[6;16],patchset_id:[7;32]}),
            provenance:Some([8;32]),lineage:vec![],check_profile:LandingCheckProfile{name:"selected".into(),version:"v1".into()},state:ForgeReviewState::ChecksPending,
            candidate:Some(ForgeReviewCandidate{id:[9;32],head:[10;32],root_tree:[11;32],target:[12;32],target_generation:3,
                rewrites:vec![ForgeReviewRewrite{old:[13;32],new:[14;32],change_id:[1;16]}]}),conflict_path:None,approvals:0,
            comments:vec![ForgeReviewComment{author:"reviewer".into(),patchset_id:[2;32],body:"unchanged text".into(),path:None,created_at:42}]};
        let (state,value)=format_review(&review).unwrap();
        assert_eq!(state,"checks_pending");assert_eq!(value["state"],state);assert_eq!(value["is_current"],false);
        assert_eq!(value["change_id"],"01".repeat(16));assert_eq!(value["source_head"],"04".repeat(32));
        assert_eq!(value["predecessor"]["patchset_id"],"07".repeat(32));assert_eq!(value["provenance"],"08".repeat(32));
        assert_eq!(value["candidate"]["id"],"09".repeat(32));assert_eq!(value["candidate"]["head"],"0a".repeat(32));
        assert_eq!(value["candidate"]["root_tree"],"0b".repeat(32));assert_eq!(value["candidate"]["target"],"0c".repeat(32));
        assert_eq!(value["candidate"]["rewrites"][0]["old"],"0d".repeat(32));assert_eq!(value["candidate"]["rewrites"][0]["new"],"0e".repeat(32));
        assert_eq!(value["comments"][0]["patchset_id"],"02".repeat(32));assert_eq!(value["comments"][0]["body"],"unchanged text");
        assert!(value.get("projected_head").is_none(),"formatter must not mint a projected read result");
    }
    #[test]
    fn admission_pending_is_only_an_exact_strict_unaccepted_intent_echo() {
        let intent=PushIntent{source_base:[1;32],source_head:[2;32],view_version:[3;32],expected_patchset:None,predecessor:None,provenance:None,lineage:vec![]};
        let pending=AdmissionPending::from_intent("saved-request",&intent);
        assert_eq!(ADMISSION_PENDING_STATE,"admission_pending");
        pending.validate_against("saved-request",&intent).unwrap();
        let value=serde_json::to_value(&pending).unwrap();
        assert_eq!(value,json!({"schema_version":1,"request_id":"saved-request","source_base":"01".repeat(32),"source_head":"02".repeat(32),"view_version":"03".repeat(32),"phase":"admission"}));
        let decoded:AdmissionPending=serde_json::from_value(value.clone()).unwrap();assert_eq!(decoded,pending);
        assert!(pending.validate_against("different-request",&intent).is_err());
        for (field,replacement) in [("schema_version",json!(2)),("phase",json!("prepared")),("request_id",json!("different")),
            ("source_base",json!("04".repeat(32))),("source_head",json!("AB".repeat(32))),("view_version",json!("05".repeat(32)))] {
            let mut rebound=value.clone();rebound[field]=replacement;
            let rebound:AdmissionPending=serde_json::from_value(rebound).unwrap();assert!(rebound.validate_against("saved-request",&intent).is_err());
        }
        for field in ["actor","profile","grant","change_id","patchset_id","candidate"] {
            let mut forged=value.clone();forged[field]=json!("not admitted");assert!(serde_json::from_value::<AdmissionPending>(forged).is_err());
        }
        let duplicate=String::from_utf8(bytes(value)).unwrap().replacen("\"schema_version\":1","\"schema_version\":1,\"schema_version\":1",1);
        assert!(serde_json::from_str::<AdmissionPending>(&duplicate).is_err());
    }

}
