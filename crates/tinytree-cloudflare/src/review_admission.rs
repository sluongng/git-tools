//! Internal bounded metadata-pack adapter to the shared admission kernel.
//! Facts/requirements are not proof of current policy, ownership or acceptance.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{
    Deserialize, Deserializer,
    de::{self, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fmt,
    io::{self, Write},
};
use tinytree_forge_core::{
    review::ForgeReviewLineageHint,
    review_admission::{self as kernel, AdmissionError, MetadataReader, ReadError},
};
use tinytree_model::{self as model, CommitId, Descriptor, ObjectKey};

const MAX_OBJECTS: usize = 8192;
const MAX_METADATA: usize = 256 * 1024;
const MAX_PACK: usize = 4 * 1024 * 1024;
const MAX_OUTPUT: usize = 4 * 1024 * 1024;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    kind: u8,
    id: String,
    size_bytes: u64,
    format_version: u16,
    bytes_base64: String,
}
struct Objects(Vec<Metadata>);
impl<'de> Deserialize<'de> for Objects {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Bounded;
        impl<'de> Visitor<'de> for Bounded {
            type Value = Objects;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a bounded review metadata pack")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Objects, A::Error> {
                if seq.size_hint().is_some_and(|n| n > MAX_OBJECTS) {
                    return Err(de::Error::custom("review pack limit"));
                }
                let mut out = Vec::new();
                while out.len() < MAX_OBJECTS {
                    match seq.next_element()? {
                        Some(value) => out.push(value),
                        None => return Ok(Objects(out)),
                    }
                }
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom("review pack limit"));
                }
                Ok(Objects(out))
            }
        }
        d.deserialize_seq(Bounded)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    schema: u32,
    source_base: String,
    source_head: String,
    lineage: Vec<ForgeReviewLineageHint>,
    objects: Objects,
}
struct Reader(HashMap<ObjectKey, (Descriptor, Vec<u8>)>);
impl MetadataReader for Reader {
    fn read(&mut self, key: ObjectKey) -> Result<(Descriptor, Vec<u8>), ReadError> {
        self.0.get(&key).cloned().ok_or(ReadError::Missing)
    }
}
struct Output(Vec<u8>);
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_OUTPUT.saturating_sub(self.0.len()) {
            return Err(io::Error::other("review facts output limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub fn analyze(input: &[u8]) -> Result<Value, String> {
    if input.len() > crate::MAX_INPUT {
        return Err("resource_exhausted: review admission input".into());
    }
    let input: Input =
        serde_json::from_slice(input).map_err(|_| "invalid_argument: review admission input")?;
    if input.schema != 1 {
        return Err("invalid_argument: review admission schema".into());
    }
    let base = crate::key(3, &input.source_base)?;
    let head = crate::key(3, &input.source_head)?;
    let mut objects = HashMap::new();
    let mut total = 0usize;
    for object in input.objects.0 {
        if object.kind == 1
            || object.size_bytes > MAX_METADATA as u64
            || object.bytes_base64.len() > MAX_METADATA.div_ceil(3) * 4
        {
            return Ok(json!({"state":"limit","budget":"metadata_object"}));
        }
        let key = crate::key(object.kind.into(), &object.id)?;
        let bytes = STANDARD
            .decode(&object.bytes_base64)
            .map_err(|_| "invalid_argument: review metadata base64")?;
        if bytes.len() as u64 != object.size_bytes || STANDARD.encode(&bytes) != object.bytes_base64
        {
            return Err("invalid_argument: review metadata bytes".into());
        }
        total = total
            .checked_add(bytes.len())
            .ok_or("resource_exhausted: review pack")?;
        if total > MAX_PACK {
            return Ok(json!({"state":"limit","budget":"metadata_pack"}));
        }
        let descriptor = Descriptor {
            key,
            size_bytes: object.size_bytes,
            format_version: object.format_version,
        };
        if objects.insert(key, (descriptor, bytes)).is_some() {
            return Err("invalid_argument: duplicate review metadata".into());
        }
    }
    let mut reader = Reader(objects);
    let limits = kernel::Limits {
        model: model::Limits {
            max_object_bytes: MAX_METADATA,
            max_items: 100_000,
            max_field_bytes: MAX_METADATA,
            max_path_bytes: 4096,
        },
        ..kernel::Limits::default()
    };
    match kernel::analyze(
        &mut reader,
        &kernel::Input {
            source_base: CommitId(base.id.0),
            source_head: CommitId(head.id.0),
            lineage: input.lineage,
        },
        limits,
    ) {
        Ok(facts) => {
            // Bound encoded requirements before allocating a serde Value tree.
            let mut output = Output(Vec::new());
            if serde_json::to_writer(&mut output, &facts).is_err() {
                return Ok(json!({"state":"limit","budget":"requirements_output"}));
            }
            let facts: Value = serde_json::from_slice(&output.0)
                .map_err(|_| "invalid_argument: review facts output")?;
            Ok(json!({"state":"facts","facts":facts}))
        }
        Err(AdmissionError::Missing { key }) => {
            Ok(json!({"state":"missing_metadata","kind":key.kind as u8,"id":crate::hex(&key.id.0)}))
        }
        Err(AdmissionError::Unavailable { .. }) => Ok(json!({"state":"unavailable"})),
        Err(AdmissionError::Limit(budget)) => Ok(json!({"state":"limit","budget":budget})),
        Err(_) => Ok(json!({"state":"invalid_metadata"})),
    }
}
