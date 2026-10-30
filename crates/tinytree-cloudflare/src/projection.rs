//! Internal trusted-SQL-host projection construction, NOT a public Forge route.
//!
//! The host authenticates account/view/current epoch, selects authorized paths,
//! verifies FILE supplied-byte authority, and validates any projected review
//! parent before creating this input. Parsing descriptors or echoing context is
//! not authorization. Private copy origins must remain in the host's ledger;
//! the complete result is never a public ResolveView response.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, MapAccess, SeqAccess, Visitor},
    ser::SerializeSeq,
};
use serde_json::{Value, json};
use std::{
    fmt,
    io::{self, Write},
    marker::PhantomData,
};
use tinytree_forge_core::{
    accounts::{AccountId, PolicyEpoch, ViewVersion, validate_path},
    projection::{
        self as core, ProjectionContext, ProjectionError, ReviewProjection, SelectedEntry,
        SelectedValue,
    },
};
use tinytree_model::{
    self as model, ChangeId, CommitId, Descriptor, EncodedObject, Kind, Object, ObjectId, ObjectKey,
};

const MAX_ENTRIES: usize = 1024;
const MAX_PATH_BYTES: usize = 1024 * 1024;
const MAX_METADATA_BYTES: usize = 1024 * 1024;
const MAX_OBJECT_BYTES: usize = 1024 * 1024;
const MAX_BASE64: usize = MAX_OBJECT_BYTES.div_ceil(3) * 4;
const MAX_OUTPUT: usize = 4 * 1024 * 1024;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Debug)]
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
struct Entries(Vec<Entry>);
impl<'de> Deserialize<'de> for Entries {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct EntriesVisitor(PhantomData<()>);
        impl<'de> Visitor<'de> for EntriesVisitor {
            type Value = Entries;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a bounded selected-entry collection")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                if seq.size_hint().is_some_and(|n| n > MAX_ENTRIES) {
                    return Err(de::Error::custom("selected entry count limit"));
                }
                let mut entries = Vec::new();
                while entries.len() < MAX_ENTRIES {
                    match seq.next_element::<Entry>()? {
                        Some(entry) => entries.push(entry),
                        None => return Ok(Entries(entries)),
                    }
                }
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom("selected entry count limit"));
                }
                Ok(Entries(entries))
            }
        }
        d.deserialize_seq(EntriesVisitor(PhantomData))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    schema: u32,
    context: Context,
    entries: Entries,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    account_id: Text<64>,
    view_version: Text<64>,
    policy_epoch: u64,
    canonical_revision: Text<64>,
    #[serde(default)]
    review: Option<Review>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Review {
    change_id: Text<32>,
    projected_parent: Text<64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    canonical_path: Text<4096>,
    kind: u8,
    id: Text<64>,
    size_bytes: u64,
    format_version: u16,
    value: EntryValue,
}
enum EntryValue {
    File {
        executable: bool,
        canonical_copy: Option<Text<64>>,
    },
    Symlink {
        bytes_base64: Text<MAX_BASE64>,
    },
}
impl<'de> Deserialize<'de> for EntryValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = EntryValue;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a bounded file or symlink selection")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut kind: Option<Text<16>> = None;
                let mut executable: Option<bool> = None;
                let mut copy: Option<Option<Text<64>>> = None;
                let mut bytes: Option<Text<MAX_BASE64>> = None;
                while let Some(key) = map.next_key::<Text<32>>()? {
                    match key.0.as_str() {
                        "kind" if kind.is_none() => kind = Some(map.next_value()?),
                        "executable" if executable.is_none() => {
                            executable = Some(map.next_value()?)
                        }
                        "canonical_copy" if copy.is_none() => copy = Some(map.next_value()?),
                        "bytes_base64" if bytes.is_none() => bytes = Some(map.next_value()?),
                        _ => return Err(de::Error::custom("unknown or duplicate selection field")),
                    }
                }
                match kind.as_ref().map(|value| value.0.as_str()) {
                    Some("file") if bytes.is_none() => Ok(EntryValue::File {
                        executable: executable
                            .ok_or_else(|| de::Error::missing_field("executable"))?,
                        canonical_copy: copy.unwrap_or(None),
                    }),
                    Some("symlink") if executable.is_none() && copy.is_none() => {
                        Ok(EntryValue::Symlink {
                            bytes_base64: bytes
                                .ok_or_else(|| de::Error::missing_field("bytes_base64"))?,
                        })
                    }
                    _ => Err(de::Error::custom("invalid selection variant fields")),
                }
            }
        }
        d.deserialize_map(ValueVisitor)
    }
}
fn id<const N: usize>(text: &str) -> Result<[u8; N], String> {
    if text.len() != N * 2
        || !text
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err("invalid lowercase projection identifier".into());
    }
    let mut bytes = [0; N];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
            .map_err(|_| "invalid projection identifier")?;
    }
    Ok(bytes)
}
fn limits() -> model::Limits {
    model::Limits {
        max_object_bytes: MAX_OBJECT_BYTES,
        max_items: 100_000,
        max_field_bytes: MAX_OBJECT_BYTES,
        max_path_bytes: 4096,
    }
}
fn limit(budget: &str) -> Value {
    json!({"state":"limit","budget":budget})
}

struct BoundedOutput(Vec<u8>);
impl Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_OUTPUT.saturating_sub(self.0.len()) {
            return Err(io::Error::other("projection response byte limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Objects<'a>(&'a [EncodedObject]);
impl Serialize for Objects<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for object in self.0 {
            sequence.serialize_element(&json!({"kind":object.descriptor.key.kind as u8,
                "id":crate::hex(&object.descriptor.key.id.0), "size_bytes":object.descriptor.size_bytes,
                "format_version":object.descriptor.format_version, "bytes_base64":STANDARD.encode(&object.bytes)}))?;
        }
        sequence.end()
    }
}
#[derive(Serialize)]
struct Output<'a> {
    state: &'static str,
    record_key: &'a str,
    binding: &'a core::ForgeProjection,
    objects: Objects<'a>,
    private_copy_origins: &'a [core::ForgeCopyOrigin],
}

pub fn build_projection(input: &[u8]) -> Result<Value, String> {
    if input.len() > crate::MAX_INPUT {
        return Err("projection input byte limit".into());
    }
    let input: Input =
        serde_json::from_slice(input).map_err(|_| "invalid bounded projection input")?;
    if input.schema != 1 {
        return Err("unsupported projection schema".into());
    }
    let account = AccountId(input.context.account_id.0);
    account.validate().map_err(|error| error.to_string())?;
    if input.context.policy_epoch == 0 || input.context.policy_epoch > MAX_SAFE_INTEGER {
        return Err("invalid projection policy epoch".into());
    }
    let canonical = CommitId(id(&input.context.canonical_revision.0)?);
    if canonical == CommitId::ROOT {
        return Err("intrinsic root is not a projection revision".into());
    }
    let context = ProjectionContext {
        account_id: account,
        view_version: ViewVersion(id(&input.context.view_version.0)?),
        policy_epoch: PolicyEpoch(input.context.policy_epoch),
        canonical_revision: canonical,
        review: input
            .context
            .review
            .map(|review| {
                Ok::<_, String>(ReviewProjection {
                    change_id: ChangeId(id(&review.change_id.0)?),
                    projected_parent: CommitId(id(&review.projected_parent.0)?),
                })
            })
            .transpose()?,
    };
    let mut paths = 0usize;
    let mut metadata_bytes = 0usize;
    let mut selected = Vec::with_capacity(input.entries.0.len());
    for entry in input.entries.0 {
        validate_path(&entry.canonical_path.0, false).map_err(|error| error.to_string())?;
        paths = paths
            .checked_add(entry.canonical_path.0.len())
            .ok_or("projection path byte limit")?;
        if paths > MAX_PATH_BYTES {
            return Ok(limit("path_bytes"));
        }
        if entry.format_version != model::FORMAT_VERSION || entry.size_bytes > MAX_SAFE_INTEGER {
            return Err("invalid projection descriptor".into());
        }
        let descriptor = Descriptor {
            key: ObjectKey {
                kind: Kind::try_from(entry.kind).map_err(|_| "invalid projection kind")?,
                id: ObjectId(id(&entry.id.0)?),
            },
            size_bytes: entry.size_bytes,
            format_version: entry.format_version,
        };
        let value = match entry.value {
            EntryValue::File {
                executable,
                canonical_copy,
            } if descriptor.key.kind == Kind::File => SelectedValue::File {
                executable,
                canonical_copy: canonical_copy.map(|text| id(&text.0)).transpose()?,
            },
            EntryValue::Symlink { bytes_base64 } if descriptor.key.kind == Kind::Symlink => {
                let size =
                    usize::try_from(descriptor.size_bytes).map_err(|_| "symlink metadata size")?;
                if size > MAX_OBJECT_BYTES || bytes_base64.0.len() != size.div_ceil(3) * 4 {
                    return Err("symlink metadata byte limit".into());
                }
                metadata_bytes = metadata_bytes
                    .checked_add(size)
                    .ok_or("symlink metadata byte limit")?;
                if metadata_bytes > MAX_METADATA_BYTES {
                    return Ok(limit("symlink_metadata_bytes"));
                }
                let mut bytes = vec![0; size];
                let decoded = STANDARD
                    .decode_slice(&bytes_base64.0, &mut bytes)
                    .map_err(|_| "invalid symlink metadata base64")?;
                if decoded != size || STANDARD.encode(&bytes) != bytes_base64.0 {
                    return Err("noncanonical symlink metadata base64".into());
                }
                model::verify(&descriptor, &bytes, limits())
                    .map_err(|error| format!("canonical symlink: {error}"))?;
                let Object::Symlink(target) = model::decode(&bytes, limits())
                    .map_err(|error| format!("canonical symlink: {error}"))?
                else {
                    return Err("invalid symlink metadata kind".into());
                };
                SelectedValue::Symlink {
                    target_bytes: target.len() as u64,
                }
            }
            _ => return Err("projection entry kind mismatch".into()),
        };
        selected.push(SelectedEntry {
            canonical_path: entry.canonical_path.0,
            descriptor,
            value,
        });
    }
    let built = match core::build_projection(&context, selected, limits()) {
        Ok(built) => built,
        Err(ProjectionError::Limit(budget)) => return Ok(limit(budget)),
        Err(ProjectionError::Model(model::Error::Limit)) => {
            return Ok(limit("canonical_object_bytes"));
        }
        Err(error) => return Err(error.to_string()),
    };
    let key =
        std::str::from_utf8(&built.record_key).map_err(|_| "invalid generated projection key")?;
    let output = Output {
        state: "ready",
        record_key: key,
        binding: &built.binding,
        objects: Objects(&built.objects),
        private_copy_origins: &built.copies,
    };
    let mut writer = BoundedOutput(Vec::new());
    if serde_json::to_writer(&mut writer, &output).is_err() {
        return Ok(limit("response_bytes"));
    }
    serde_json::from_slice(&writer.0).map_err(|_| "invalid generated projection response".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, bytes: &[u8]) -> Value {
        let descriptor = model::describe_file(bytes);
        json!({"canonical_path":path,"kind":1,"id":crate::hex(&descriptor.key.id.0),
            "size_bytes":descriptor.size_bytes,"format_version":1,"value":{"kind":"file","executable":false}})
    }
    fn fixture() -> Value {
        json!({"schema":1,"context":{"account_id":"projection","view_version":"7ce134a795b2ca18b5c721a72b48c9f62d4e5713b0619e1177feaa4345ae71f8",
            "policy_epoch":2,"canonical_revision":"840d05cb363626cdbb3ca927d071344e3960f40738074abece78d56f796a9c1f"},
            "entries":[entry("README.md",b"# Account\npublic needle\n"),entry("source.rs",b"fn needle() {}\n")]})
    }
    fn run(value: &Value) -> Result<Value, String> {
        build_projection(&serde_json::to_vec(value).unwrap())
    }

    #[test]
    fn adapter_preserves_native_baseline_and_review_bytes_with_private_origins_separate() {
        let mut input = fixture();
        let output = run(&input).unwrap();
        assert_eq!(output["state"], "ready");
        let expected = core::build_projection(
            &ProjectionContext {
                account_id: AccountId("projection".into()),
                view_version: ViewVersion(
                    id(input["context"]["view_version"].as_str().unwrap()).unwrap(),
                ),
                policy_epoch: PolicyEpoch(2),
                canonical_revision: CommitId(
                    id(input["context"]["canonical_revision"].as_str().unwrap()).unwrap(),
                ),
                review: None,
            },
            [
                SelectedEntry {
                    canonical_path: "README.md".into(),
                    descriptor: model::describe_file(b"# Account\npublic needle\n"),
                    value: SelectedValue::File {
                        executable: false,
                        canonical_copy: None,
                    },
                },
                SelectedEntry {
                    canonical_path: "source.rs".into(),
                    descriptor: model::describe_file(b"fn needle() {}\n"),
                    value: SelectedValue::File {
                        executable: false,
                        canonical_copy: None,
                    },
                },
            ],
            limits(),
        )
        .unwrap();
        assert_eq!(
            output["binding"],
            serde_json::to_value(&expected.binding).unwrap()
        );
        assert_eq!(
            crate::hex(&expected.binding.projected_commit),
            "c180824d1a2785721d26f7e4e23e201e379fb96b4fa9ad6e6444c54b59fb70e7"
        );
        assert_eq!(
            output["record_key"],
            String::from_utf8(expected.record_key).unwrap()
        );
        assert_eq!(
            output["private_copy_origins"],
            serde_json::to_value(&expected.copies).unwrap()
        );
        for (actual, expected) in output["objects"]
            .as_array()
            .unwrap()
            .iter()
            .zip(&expected.objects)
        {
            assert_eq!(actual["id"], crate::hex(&expected.descriptor.key.id.0));
            assert_eq!(
                STANDARD
                    .decode(actual["bytes_base64"].as_str().unwrap())
                    .unwrap(),
                expected.bytes
            );
            assert!(!matches!(
                expected.descriptor.key.kind,
                Kind::File | Kind::Symlink
            ));
        }
        assert_eq!(
            output["objects"].as_array().unwrap().len(),
            expected.objects.len()
        );
        input["context"]["review"] = json!({"change_id":"08".repeat(16),"projected_parent":crate::hex(&expected.binding.projected_commit)});
        let reviewed = run(&input).unwrap();
        let commit = reviewed["binding"]["projected_commit"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap() as u8)
            .collect::<Vec<_>>();
        assert_eq!(
            crate::hex(&commit),
            "bc9be7817fbfc6abc04e435e9284d16d1a8ec22090c63f173b443fbc8dd45ec4"
        );
    }

    #[test]
    fn adapter_verifies_symlink_metadata_and_rejects_forged_or_ambiguous_inputs() {
        let symlink = model::encode(&Object::Symlink("../target".into()), limits()).unwrap();
        let mut input = fixture();
        input["entries"] = json!([{"canonical_path":"nested/link","kind":4,"id":crate::hex(&symlink.descriptor.key.id.0),"size_bytes":symlink.descriptor.size_bytes,"format_version":1,
            "value":{"kind":"symlink","bytes_base64":STANDARD.encode(&symlink.bytes)}}]);
        let result = run(&input).unwrap();
        assert_eq!(result["state"], "ready");
        assert_eq!(result["private_copy_origins"], json!([]));
        assert!(result["objects"].as_array().unwrap().iter().any(|object| {
            let bytes=STANDARD.decode(object["bytes_base64"].as_str().unwrap()).unwrap();
            matches!(model::decode(&bytes,limits()),Ok(Object::Tree(tree)) if tree.entries.iter().any(|e| matches!(e.value,model::TreeValue::Symlink {target_bytes:9,..})))
        }));
        let mut bad = input.clone();
        bad["entries"][0]["id"] = json!("a".repeat(64));
        assert!(run(&bad).is_err());
        let mut bad = input.clone();
        bad["entries"][0]["value"]["target_bytes"] = json!(1);
        assert!(run(&bad).is_err());
        let mut bad = input;
        bad["entries"][0]["kind"] = json!(1);
        assert!(run(&bad).is_err());
        let mut bad = fixture();
        bad["context"]["actor"] = json!("owner");
        assert!(run(&bad).is_err());
        let mut bad = fixture();
        bad["context"]["policy_epoch"] = json!(0);
        assert!(run(&bad).is_err());
        let mut bad = fixture();
        bad["context"]["canonical_revision"] = json!("0".repeat(64));
        assert!(run(&bad).is_err());
        let mut bad = fixture();
        bad["entries"][0]["canonical_path"] = json!("../private");
        assert!(run(&bad).is_err());
        let mut bad = fixture();
        bad["entries"][0]["value"]["bytes_base64"] = json!("");
        assert!(run(&bad).is_err());
        let text = fixture().to_string();
        let duplicate = text.replacen(
            "\"executable\":false",
            "\"executable\":false,\"executable\":true",
            1,
        );
        assert_ne!(text, duplicate);
        assert!(build_projection(duplicate.as_bytes()).is_err());
    }

    #[test]
    fn adapter_enforces_entry_path_descriptor_and_output_limits_without_partial_objects() {
        assert!(build_projection(&vec![b' '; crate::MAX_INPUT + 1]).is_err());
        let mut input = fixture();
        input["entries"] = json!(vec![entry("file", b"x"); MAX_ENTRIES + 1]);
        assert!(run(&input).is_err());
        let mut input = fixture();
        input["entries"][0]["size_bytes"] = json!(MAX_SAFE_INTEGER + 1);
        assert!(run(&input).is_err());
        let mut input = fixture();
        input["entries"] = json!(
            (0..300)
                .map(|i| entry(&format!("{}/f-{i}", "p".repeat(4000)), b"x"))
                .collect::<Vec<_>>()
        );
        let limited = run(&input).unwrap();
        assert_eq!(limited["state"], "limit");
        assert_eq!(limited["budget"], "path_bytes");
        assert!(limited.get("objects").is_none());
        let mut input = fixture();
        input["context"]["canonical_revision"] = json!("f".repeat(64));
        input["entries"] = json!(
            (0..1024)
                .map(|i| {
                    let mut entry = entry(&format!("f-{i:04}-{}", "p".repeat(890)), b"x");
                    entry["value"]["canonical_copy"] = json!("f".repeat(64));
                    entry
                })
                .collect::<Vec<_>>()
        );
        let limited = run(&input).unwrap();
        assert_eq!(limited["state"], "limit");
        assert!(limited.get("objects").is_none());
        assert!(serde_json::to_vec(&limited).unwrap().len() < MAX_OUTPUT);
    }
}
