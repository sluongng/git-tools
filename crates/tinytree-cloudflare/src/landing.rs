//! Internal trusted-embedding candidate construction, NOT a public Forge route.
//!
//! The host must authorize account/view/epoch, supplied-byte receipts and the
//! exact projection/predecessor mapping before constructing this input. Parsing
//! or echoing context does not establish that authority. No profile or verdict
//! is accepted. Success is NOT strict closure availability: equal subtrees may
//! be skipped and no FILE bodies are read. Activation remains the host's job.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fmt,
    io::{self, Write},
    marker::PhantomData,
};
use tinytree_forge_core::accounts::AccountId;
use tinytree_landing::{self as landing, MetadataReader, ReadError};
use tinytree_model::{self as model, CommitId, Descriptor, Kind, ObjectId, ObjectKey};

const MAX_OUTPUT: usize = 4 * 1024 * 1024;
const MAX_OBJECTS: usize = 8192;
const MAX_METADATA_BYTES: usize = 4 * 1024 * 1024;
const MAX_OBJECT_BYTES: usize = 1024 * 1024;
const MAX_BASE64: usize = MAX_OBJECT_BYTES.div_ceil(3) * 4;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

// Reject collection/string expansion before allocating our owned containers.
// The complete encoded input is independently capped before serde is invoked.
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
struct Text<const MAX: usize>(String);
impl<'de, const MAX: usize> Deserialize<'de> for Text<MAX> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct TextVisitor<const MAX: usize>;
        impl<const MAX: usize> Visitor<'_> for TextVisitor<MAX> {
            type Value = Text<MAX>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a string of at most {MAX} bytes")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value.len() > MAX {
                    return Err(E::custom("string byte limit"));
                }
                Ok(Text(value.to_owned()))
            }
        }
        d.deserialize_str(TextVisitor::<MAX>)
    }
}
struct Objects(Vec<Supplied>);
impl<'de> Deserialize<'de> for Objects {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct ObjectsVisitor(PhantomData<()>);
        impl<'de> Visitor<'de> for ObjectsVisitor {
            type Value = Objects;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a bounded metadata collection")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Objects, A::Error> {
                if seq.size_hint().is_some_and(|n| n > MAX_OBJECTS) {
                    return Err(de::Error::custom("metadata object count limit"));
                }
                let mut objects = Vec::new();
                while objects.len() < MAX_OBJECTS {
                    match seq.next_element::<Supplied>()? {
                        Some(object) => objects.push(object),
                        None => return Ok(Objects(objects)),
                    }
                }
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom("metadata object count limit"));
                }
                Ok(Objects(objects))
            }
        }
        d.deserialize_seq(ObjectsVisitor(PhantomData))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    schema: u32,
    context: Context,
    objects: Objects,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Context {
    account_id: Text<64>,
    view_version: Text<64>,
    policy_epoch: u64,
    target_head: Text<64>,
    target_generation: u64,
    source_base: Text<64>,
    source_head: Text<64>,
    destination_head: Text<64>,
    committer: Committer,
    base_mapping: BaseMapping,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Committer {
    name: Text<1024>,
    email: Text<1024>,
    millis_since_epoch: i64,
    tz_offset_minutes: i32,
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BaseMapping {
    Canonical {},
    Projected { canonical_base: Text<64> },
    Predecessor { mapped_base: Text<64> },
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Supplied {
    kind: u8,
    id: Text<64>,
    size_bytes: u64,
    format_version: u16,
    bytes_base64: Text<MAX_BASE64>,
}
fn id(text: &str) -> Result<[u8; 32], String> {
    if text.len() != 64
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("invalid lowercase native identifier".into());
    }
    let mut value = [0; 32];
    for (i, byte) in value.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
            .map_err(|_| "invalid native identifier")?;
    }
    Ok(value)
}
fn nonroot(text: &str) -> Result<[u8; 32], String> {
    let value = id(text)?;
    if value == [0; 32] {
        return Err("root identity is not a candidate context".into());
    }
    Ok(value)
}
fn limits() -> landing::Limits {
    landing::Limits {
        max_stack: 64,
        max_ancestry: 4096,
        max_reads: 20_000,
        max_metadata_bytes: 64 * 1024 * 1024,
        max_work: 1_000_000,
        max_depth: 128,
        max_path_bytes: 4096,
        max_entries_per_tree: 100_000,
        max_object_bytes: MAX_OBJECT_BYTES,
        max_field_bytes: MAX_OBJECT_BYTES,
        max_output_objects: 4096,
        max_output_bytes: 2 * 1024 * 1024,
    }
}
fn model_limits() -> model::Limits {
    model::Limits {
        max_object_bytes: MAX_OBJECT_BYTES,
        max_items: 100_000,
        max_field_bytes: MAX_OBJECT_BYTES,
        max_path_bytes: 4096,
    }
}
struct Reader(HashMap<ObjectKey, Vec<u8>>);
impl MetadataReader for Reader {
    fn read(&mut self, key: ObjectKey, maximum: usize) -> Result<Vec<u8>, ReadError> {
        let bytes = self.0.get(&key).ok_or(ReadError::Missing)?;
        if bytes.len() > maximum {
            return Err(ReadError::Limit);
        }
        Ok(bytes.clone())
    }
}
struct BoundedOutput(Vec<u8>);
impl Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_OUTPUT.saturating_sub(self.0.len()) {
            return Err(io::Error::other("candidate response byte limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn bounded(value: Value) -> Result<Value, String> {
    let mut output = BoundedOutput(Vec::new());
    if serde_json::to_writer(&mut output, &value).is_err() {
        return Ok(json!({"state":"limit","budget":"response_bytes"}));
    }
    // Return the already bounded value; the generic ABI uses the same encoding.
    Ok(value)
}
fn budget_name(budget: landing::Budget) -> &'static str {
    use landing::Budget;
    match budget {
        Budget::Configuration => "configuration",
        Budget::Stack => "stack",
        Budget::Ancestry => "ancestry",
        Budget::Reads => "reads",
        Budget::MetadataBytes => "metadata_bytes",
        Budget::Work => "work",
        Budget::Depth => "depth",
        Budget::Path => "path",
        Budget::Entries => "entries",
        Budget::OutputObjects => "output_objects",
        Budget::OutputBytes => "output_bytes",
    }
}
fn failure(error: landing::Error) -> Value {
    match error {
        landing::Error::Conflict { path } => json!({"state":"conflict","path":path}),
        landing::Error::Limit(budget) => json!({"state":"limit","budget":budget_name(budget)}),
        landing::Error::Read {
            key,
            error: ReadError::Missing,
        } => json!({"state":"missing_metadata","kind":key.kind as u8,"id":crate::hex(&key.id.0)}),
        landing::Error::Read {
            key,
            error: ReadError::Limit,
        } => {
            json!({"state":"limit","budget":"reader_bytes","kind":key.kind as u8,"id":crate::hex(&key.id.0)})
        }
        landing::Error::Read { .. } => {
            json!({"state":"invalid_metadata","reason":"metadata_unavailable"})
        }
        landing::Error::UnsupportedHistory(reason) => {
            json!({"state":"unsupported","reason":reason})
        }
        landing::Error::Invalid(reason) => json!({"state":"invalid_metadata","reason":reason}),
        landing::Error::Object(_) => {
            json!({"state":"invalid_metadata","reason":"canonical_metadata"})
        }
    }
}

/// Internal only. The caller supplies an already authorized immutable context;
/// this routine proves canonical bytes and construction, not identity or ACLs.
pub fn build_candidate(input: &[u8]) -> Result<Value, String> {
    if input.len() > crate::MAX_INPUT {
        return Err("landing input byte limit".into());
    }
    let input: Input =
        serde_json::from_slice(input).map_err(|_| "invalid bounded landing input")?;
    if input.schema != 1 {
        return Err("unsupported landing schema".into());
    }
    let context = input.context;
    AccountId(context.account_id.0.clone())
        .validate()
        .map_err(|e| e.to_string())?;
    nonroot(&context.view_version.0)?;
    nonroot(&context.target_head.0)?;
    if context.policy_epoch == 0
        || context.policy_epoch > MAX_SAFE_INTEGER
        || context.target_generation == 0
        || context.target_generation > MAX_SAFE_INTEGER
    {
        return Err("invalid landing epoch or generation".into());
    }
    if context.committer.millis_since_epoch.unsigned_abs() > MAX_SAFE_INTEGER
        || !(-1440..=1440).contains(&context.committer.tz_offset_minutes)
        || context.committer.name.0.chars().any(char::is_control)
        || context.committer.email.0.chars().any(char::is_control)
    {
        return Err("invalid frozen committer".into());
    }
    let request = landing::Request {
        source_base: CommitId(id(&context.source_base.0)?),
        source_head: CommitId(nonroot(&context.source_head.0)?),
        destination_head: CommitId(nonroot(&context.destination_head.0)?),
        committer: model::Signature {
            name: context.committer.name.0.clone(),
            email: context.committer.email.0.clone(),
            timestamp: model::Timestamp {
                millis_since_epoch: context.committer.millis_since_epoch,
                tz_offset_minutes: context.committer.tz_offset_minutes,
            },
        },
    };
    let mapped = match &context.base_mapping {
        BaseMapping::Canonical {} => None,
        BaseMapping::Projected { canonical_base } => Some(CommitId(nonroot(&canonical_base.0)?)),
        BaseMapping::Predecessor { mapped_base } => Some(CommitId(nonroot(&mapped_base.0)?)),
    };
    let mut reader = Reader(HashMap::new());
    let mut total = 0usize;
    for supplied in input.objects.0 {
        let kind = Kind::try_from(supplied.kind).map_err(|_| "invalid metadata kind")?;
        if kind == Kind::File {
            return Err("FILE input is not metadata".into());
        }
        if supplied.format_version != model::FORMAT_VERSION
            || supplied.size_bytes > MAX_OBJECT_BYTES as u64
        {
            return Err("metadata descriptor limit or version".into());
        }
        let size = supplied.size_bytes as usize;
        total = total
            .checked_add(size)
            .filter(|n| *n <= MAX_METADATA_BYTES)
            .ok_or("supplied metadata byte limit")?;
        if supplied.bytes_base64.0.len() != size.div_ceil(3) * 4 {
            return Err("metadata base64 length".into());
        }
        let descriptor = Descriptor {
            key: ObjectKey {
                kind,
                id: ObjectId(id(&supplied.id.0)?),
            },
            size_bytes: supplied.size_bytes,
            format_version: supplied.format_version,
        };
        if reader.0.contains_key(&descriptor.key) {
            return Err("duplicate metadata key".into());
        }
        let mut bytes = vec![0; size];
        let decoded = STANDARD
            .decode_slice(&supplied.bytes_base64.0, &mut bytes)
            .map_err(|_| "invalid metadata base64")?;
        if decoded != size || STANDARD.encode(&bytes) != supplied.bytes_base64.0 {
            return Err("noncanonical metadata base64".into());
        }
        model::verify(&descriptor, &bytes, model_limits())
            .map_err(|_| "invalid canonical supplied metadata")?;
        reader.0.insert(descriptor.key, bytes);
    }
    let result = match &context.base_mapping {
        BaseMapping::Canonical {} => landing::build(&mut reader, request, limits()),
        BaseMapping::Projected { .. } => landing::build_on_projected_base(
            &mut reader,
            request,
            mapped.expect("validated projected mapping"),
            limits(),
        ),
        BaseMapping::Predecessor { .. } => landing::build_on_mapped_base(
            &mut reader,
            request,
            mapped.expect("validated predecessor mapping"),
            limits(),
        ),
    };
    let candidate = match result {
        Ok(candidate) => candidate,
        Err(error) => return bounded(failure(error)),
    };
    let objects = candidate.objects.into_iter().map(|object| json!({
        "kind":object.descriptor.key.kind as u8,"id":crate::hex(&object.descriptor.key.id.0),
        "size_bytes":object.descriptor.size_bytes,"format_version":object.descriptor.format_version,
        "bytes_base64":STANDARD.encode(&object.bytes),
    })).collect::<Vec<_>>();
    bounded(
        json!({"state":"candidate","context":context,"head":crate::hex(&candidate.head.0),"root_tree":crate::hex(&candidate.root_tree.0),
        "rewrites":candidate.rewrites.into_iter().map(|r|json!({"old":crate::hex(&r.old.0),"new":crate::hex(&r.new.0),"change_id":crate::hex(&r.change_id.0)})).collect::<Vec<_>>(),
        "objects":objects,"stats":{"metadata_reads":candidate.stats.metadata_reads,"metadata_bytes":candidate.stats.metadata_bytes,"work":candidate.stats.work,"output_bytes":candidate.stats.output_bytes}}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{
        ChangeId, Commit, EncodedObject, Object, Signature, Timestamp, Tree, TreeEntry, TreeId,
        TreeMerge, TreeValue,
    };
    fn signature() -> Signature {
        Signature {
            name: "Review service".into(),
            email: "review@example.invalid".into(),
            timestamp: Timestamp {
                millis_since_epoch: -42,
                tz_offset_minutes: 0,
            },
        }
    }
    fn put(objects: &mut Vec<EncodedObject>, object: Object) -> [u8; 32] {
        let encoded = model::encode(&object, model_limits()).unwrap();
        let id = encoded.descriptor.key.id.0;
        if !objects
            .iter()
            .any(|object| object.descriptor.key == encoded.descriptor.key)
        {
            objects.push(encoded);
        }
        id
    }
    fn tree(objects: &mut Vec<EncodedObject>, files: &[(&str, &[u8])]) -> TreeId {
        TreeId(put(
            objects,
            Object::Tree(Tree {
                entries: files
                    .iter()
                    .map(|(name, bytes)| TreeEntry {
                        name: (*name).into(),
                        value: TreeValue::File {
                            id: model::file_id(bytes),
                            executable: false,
                            size_bytes: bytes.len() as u64,
                            copy_id: None,
                        },
                    })
                    .collect(),
            }),
        ))
    }
    fn commit(
        objects: &mut Vec<EncodedObject>,
        parent: CommitId,
        root: TreeId,
        change: u8,
    ) -> CommitId {
        CommitId(put(
            objects,
            Object::Commit(Commit {
                parents: vec![parent],
                predecessors: vec![],
                root_tree: TreeMerge {
                    terms: vec![root],
                    labels: vec![String::new()],
                },
                change_id: ChangeId([change; 16]),
                description: format!("change {change}"),
                author: signature(),
                committer: signature(),
                secure_signature: None,
            }),
        ))
    }
    fn wire(
        objects: &[EncodedObject],
        base: CommitId,
        head: CommitId,
        dest: CommitId,
        mapping: Value,
    ) -> Value {
        json!({"schema":1,"context":{"account_id":"example","view_version":"1".repeat(64),"policy_epoch":3,"target_head":crate::hex(&dest.0),"target_generation":7,"source_base":crate::hex(&base.0),"source_head":crate::hex(&head.0),"destination_head":crate::hex(&dest.0),"committer":{"name":"Review service","email":"review@example.invalid","millis_since_epoch":-42,"tz_offset_minutes":0},"base_mapping":mapping},"objects":objects.iter().map(|o|json!({"kind":o.descriptor.key.kind as u8,"id":crate::hex(&o.descriptor.key.id.0),"size_bytes":o.descriptor.size_bytes,"format_version":o.descriptor.format_version,"bytes_base64":STANDARD.encode(&o.bytes)})).collect::<Vec<_>>()})
    }
    fn invoke(input: &Value) -> Result<Value, String> {
        build_candidate(&serde_json::to_vec(input).unwrap())
    }
    fn fixture() -> (Vec<EncodedObject>, CommitId, CommitId, CommitId) {
        let mut objects = Vec::new();
        let empty = tree(&mut objects, &[]);
        let base = commit(&mut objects, CommitId::ROOT, empty, 1);
        let source_tree = tree(&mut objects, &[("public", b"change")]);
        let source = commit(&mut objects, base, source_tree, 2);
        let target_tree = tree(&mut objects, &[("private", b"retained")]);
        let target = commit(&mut objects, base, target_tree, 3);
        (objects, base, source, target)
    }
    fn reader(objects: &[EncodedObject]) -> Reader {
        Reader(
            objects
                .iter()
                .map(|o| (o.descriptor.key, o.bytes.clone()))
                .collect(),
        )
    }
    fn assert_candidate(actual: &Value, expected: landing::Candidate, context: &Value) {
        assert_eq!(actual["state"], "candidate");
        assert_eq!(actual["context"], *context);
        assert_eq!(actual["head"], crate::hex(&expected.head.0));
        assert_eq!(actual["root_tree"], crate::hex(&expected.root_tree.0));
        let outputs = actual["objects"].as_array().unwrap();
        assert_eq!(outputs.len(), expected.objects.len());
        for (actual, expected) in outputs.iter().zip(expected.objects) {
            assert_eq!(actual["id"], crate::hex(&expected.descriptor.key.id.0));
            assert_eq!(actual["kind"], expected.descriptor.key.kind as u8);
            assert_eq!(actual["size_bytes"], expected.descriptor.size_bytes);
            assert_eq!(actual["format_version"], expected.descriptor.format_version);
            assert_eq!(
                STANDARD
                    .decode(actual["bytes_base64"].as_str().unwrap())
                    .unwrap(),
                expected.bytes
            );
        }
        assert_eq!(actual["rewrites"],json!(expected.rewrites.iter().map(|r|json!({"old":crate::hex(&r.old.0),"new":crate::hex(&r.new.0),"change_id":crate::hex(&r.change_id.0)})).collect::<Vec<_>>()));
    }
    #[test]
    fn canonical_projected_and_predecessor_modes_match_native_builder_bytes() {
        let (mut objects, base, source, target) = fixture();
        let request = || landing::Request {
            source_base: base,
            source_head: source,
            destination_head: target,
            committer: signature(),
        };
        let input = wire(&objects, base, source, target, json!({"kind":"canonical"}));
        let native = landing::build(&mut reader(&objects), request(), limits()).unwrap();
        assert_candidate(&invoke(&input).unwrap(), native.clone(), &input["context"]);
        // A sanitized baseline has an unrelated ChangeId and ROOT ancestry;
        // only the host's exact canonical mapping enables projected mode.
        let empty = tree(&mut objects, &[]);
        let projected = commit(&mut objects, CommitId::ROOT, empty, 8);
        let changed = tree(&mut objects, &[("public", b"change")]);
        let projected_head = commit(&mut objects, projected, changed, 9);
        let input = wire(
            &objects,
            projected,
            projected_head,
            target,
            json!({"kind":"projected","canonical_base":crate::hex(&base.0)}),
        );
        let native_projected = landing::build_on_projected_base(
            &mut reader(&objects),
            landing::Request {
                source_base: projected,
                source_head: projected_head,
                destination_head: target,
                committer: signature(),
            },
            base,
            limits(),
        )
        .unwrap();
        assert_candidate(
            &invoke(&input).unwrap(),
            native_projected,
            &input["context"],
        );
        objects.extend(native.objects);
        let child_tree = tree(&mut objects, &[("child", b"new"), ("public", b"change")]);
        let child = commit(&mut objects, source, child_tree, 4);
        let input = wire(
            &objects,
            source,
            child,
            native.head,
            json!({"kind":"predecessor","mapped_base":crate::hex(&native.head.0)}),
        );
        let expected = landing::build_on_mapped_base(
            &mut reader(&objects),
            landing::Request {
                source_base: source,
                source_head: child,
                destination_head: native.head,
                committer: signature(),
            },
            native.head,
            limits(),
        )
        .unwrap();
        assert_candidate(&invoke(&input).unwrap(), expected, &input["context"]);
    }
    #[test]
    fn reports_conflict_and_missing_metadata_without_partial_candidate() {
        let (mut objects, base, source, _) = fixture();
        let tree = tree(&mut objects, &[("public", b"competing")]);
        let dest = commit(&mut objects, base, tree, 5);
        let input = wire(&objects, base, source, dest, json!({"kind":"canonical"}));
        let result = invoke(&input).unwrap();
        assert_eq!(result, json!({"state":"conflict","path":"public"}));
        let mut missing = input;
        missing["objects"] = json!([]);
        let result = invoke(&missing).unwrap();
        assert_eq!(result["state"], "missing_metadata");
        assert!(result.get("objects").is_none());
    }
    #[test]
    fn validates_all_supplied_bytes_and_rejects_untrusted_or_ambiguous_context() {
        let (objects, base, source, target) = fixture();
        let input = wire(&objects, base, source, target, json!({"kind":"canonical"}));
        let mut cases = Vec::new();
        let mut value = input.clone();
        value["objects"][0]["bytes_base64"] = json!("AAAA");
        cases.push(("corrupt bytes", value));
        let mut value = input.clone();
        value["objects"][0]["kind"] = json!(3);
        cases.push(("wrong kind", value));
        let mut value = input.clone();
        value["objects"][0]["size_bytes"] = json!(999);
        cases.push(("wrong size", value));
        let mut value = input.clone();
        value["objects"][0]["format_version"] = json!(2);
        cases.push(("wrong version", value));
        let mut value = input.clone();
        let duplicate = value["objects"][0].clone();
        value["objects"].as_array_mut().unwrap().push(duplicate);
        cases.push(("duplicate metadata", value));
        let mut value = input.clone();
        value["objects"][0]["kind"] = json!(1);
        cases.push(("FILE bodies", value));
        let mut value = input.clone();
        value["context"]["profile"] = json!("caller-check");
        cases.push(("caller profile", value));
        let mut value = input.clone();
        value["context"]["base_mapping"] = json!({"kind":"canonical","mapped_base":"1".repeat(64)});
        cases.push(("mapping field smuggling", value));
        let mut value = input.clone();
        value["context"]["policy_epoch"] = json!(MAX_SAFE_INTEGER + 1);
        cases.push(("unsafe epoch", value));
        let mut value = input.clone();
        value["context"]["committer"]["millis_since_epoch"] = json!(MAX_SAFE_INTEGER + 1);
        cases.push(("unsafe timestamp", value));
        let mut value = input.clone();
        value["context"]["account_id"] = json!("../other");
        cases.push(("invalid account", value));
        for (name, value) in cases {
            assert!(invoke(&value).is_err(), "{name}");
        }
        let bytes = serde_json::to_string(&input)
            .unwrap()
            .replace("\"schema\":1", "\"schema\":1,\"schema\":1");
        assert!(build_candidate(bytes.as_bytes()).is_err());
        assert!(build_candidate(&vec![b' '; crate::MAX_INPUT + 1]).is_err());
    }
    #[test]
    fn enforces_object_count_and_decoded_payload_limits_before_construction() {
        let (objects, base, source, target) = fixture();
        let input = wire(&objects, base, source, target, json!({"kind":"canonical"}));
        let mut many = input.clone();
        many["objects"] = json!(vec![input["objects"][0].clone(); MAX_OBJECTS + 1]);
        assert!(invoke(&many).is_err());
        let mut large = input;
        large["objects"][0]["size_bytes"] = json!(MAX_OBJECT_BYTES + 1);
        assert!(invoke(&large).is_err());
        // Unreachable supplied metadata is still fully verified and charged.
        let mut objects = objects;
        for _ in 0..5 {
            put(&mut objects, Object::Symlink("x".repeat(900_000)));
        }
        // Distinct identities avoid duplicate-key rejection hiding byte quota.
        for (index, object) in objects.iter_mut().rev().take(5).enumerate() {
            *object = model::encode(
                &Object::Symlink(format!("{index}{}", "x".repeat(900_000))),
                model_limits(),
            )
            .unwrap();
        }
        let input = wire(&objects, base, source, target, json!({"kind":"canonical"}));
        assert_eq!(invoke(&input).unwrap_err(), "supplied metadata byte limit");
    }
    #[test]
    fn output_budget_is_an_explicit_public_outcome_without_partial_objects() {
        let (mut objects, base, _, target) = fixture();
        let empty = tree(&mut objects, &[]);
        let mut head = base;
        // Rewritten empty logical changes are preserved, so this stack creates
        // genuinely new output bytes rather than a duplicate-object fixture.
        for change in 10..65 {
            let commit = Commit {
                parents: vec![head],
                predecessors: vec![],
                root_tree: TreeMerge {
                    terms: vec![empty],
                    labels: vec![String::new()],
                },
                change_id: ChangeId([change; 16]),
                description: "x".repeat(40_000),
                author: signature(),
                committer: signature(),
                secure_signature: None,
            };
            head = CommitId(put(&mut objects, Object::Commit(commit)));
        }
        let input = wire(&objects, base, head, target, json!({"kind":"canonical"}));
        let result = invoke(&input).unwrap();
        assert_eq!(result, json!({"state":"limit","budget":"output_bytes"}));
    }
}
