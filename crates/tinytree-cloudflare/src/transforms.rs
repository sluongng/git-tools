//! Internal transform adapter. Native semantics do not authenticate selected
//! endpoints, lineage facts, FILE byte ownership or source export permission.
//! Those remain exact-current-policy SQL host obligations, never public proofs.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    io::{self, Write},
    marker::PhantomData,
};
use tinytree_forge_core::{accounts::AccountId, transform_lineage as lineage, transforms as core};
use tinytree_model::{
    self as model, Descriptor, FileId, Kind, Object, ObjectId, ObjectKey, TreeValue,
};

const INPUT: usize = 8 * 1024 * 1024;
const OUTPUT: usize = 4 * 1024 * 1024;
const METADATA: usize = 4 * 1024 * 1024;
const OBJECT: usize = 256 * 1024;
const OBJECTS: usize = 8192;
const BASE64: usize = OBJECT.div_ceil(3) * 4;
const SAFE: u64 = 9_007_199_254_740_991;
fn invalid() -> String {
    "invalid_argument: transform input".into()
}
fn limit() -> String {
    "resource_exhausted: transform budget".into()
}
fn core_error(error: core::Error) -> String {
    match error {
        core::Error::Limit(_) | core::Error::Model(model::Error::Limit) => limit(),
        _ => invalid(),
    }
}
fn bounds() -> model::Limits {
    model::Limits {
        max_object_bytes: OBJECT,
        max_items: 8192,
        max_field_bytes: OBJECT,
        max_path_bytes: 4096,
    }
}
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
struct Text<const N: usize>(String);
impl<'de, const N: usize> Deserialize<'de> for Text<N> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<const N: usize>;
        impl<const N: usize> Visitor<'_> for V<N> {
            type Value = Text<N>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a string of at most {N} bytes")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value.len() > N {
                    return Err(E::custom("string limit"));
                }
                Ok(Text(value.to_owned()))
            }
        }
        d.deserialize_str(V::<N>)
    }
}
struct Items<T, const N: usize>(Vec<T>);
impl<'de, T: Deserialize<'de>, const N: usize> Deserialize<'de> for Items<T, N> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<T, const N: usize>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>, const N: usize> Visitor<'de> for V<T, N> {
            type Value = Items<T, N>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "at most {N} entries")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                if seq.size_hint().is_some_and(|n| n > N) {
                    return Err(de::Error::custom("collection limit"));
                }
                let mut out = Vec::new();
                while out.len() < N {
                    match seq.next_element::<T>()? {
                        Some(value) => out.push(value),
                        None => return Ok(Items(out)),
                    }
                }
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom("collection limit"));
                }
                Ok(Items(out))
            }
        }
        d.deserialize_seq(V::<T, N>(PhantomData))
    }
}
fn parse<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, String> {
    if bytes.len() > INPUT {
        return Err(limit());
    }
    serde_json::from_slice(bytes).map_err(|error| {
        if error.to_string().contains("limit") {
            limit()
        } else {
            invalid()
        }
    })
}
struct Output(Vec<u8>, usize);
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.1.saturating_sub(self.0.len()) {
            return Err(io::Error::other("transform output limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn bounded(value: impl Serialize) -> Result<Value, String> {
    let mut out = Output(Vec::new(), OUTPUT);
    serde_json::to_writer(&mut out, &value).map_err(|_| limit())?;
    serde_json::from_slice(&out.0).map_err(|_| invalid())
}
fn id(text: &str) -> Result<[u8; 32], String> {
    if text.len() != 64
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid());
    }
    let mut out = [0; 32];
    for (i, value) in out.iter_mut().enumerate() {
        *value = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).map_err(|_| invalid())?;
    }
    Ok(out)
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireDescriptor {
    kind: u8,
    id: Text<64>,
    size_bytes: u64,
    format_version: u16,
}
impl WireDescriptor {
    fn native(&self) -> Result<Descriptor, String> {
        if self.format_version != model::FORMAT_VERSION || self.size_bytes > SAFE {
            return Err(invalid());
        }
        Ok(Descriptor {
            key: ObjectKey {
                kind: Kind::try_from(self.kind).map_err(|_| invalid())?,
                id: ObjectId(id(&self.id.0)?),
            },
            size_bytes: self.size_bytes,
            format_version: self.format_version,
        })
    }
    fn from_native(d: Descriptor) -> Self {
        Self {
            kind: d.key.kind as u8,
            id: Text(crate::hex(&d.key.id.0)),
            size_bytes: d.size_bytes,
            format_version: d.format_version,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Supplied {
    kind: u8,
    id: Text<64>,
    size_bytes: u64,
    format_version: u16,
    bytes_base64: Text<BASE64>,
}
impl Supplied {
    fn verified(self) -> Result<(Descriptor, Vec<u8>), String> {
        let d = WireDescriptor {
            kind: self.kind,
            id: self.id,
            size_bytes: self.size_bytes,
            format_version: self.format_version,
        }
        .native()?;
        if d.key.kind == Kind::File {
            return Err(invalid());
        }
        if d.size_bytes > OBJECT as u64 {
            return Err(limit());
        }
        let size = d.size_bytes as usize;
        if self.bytes_base64.0.len() != size.div_ceil(3) * 4 {
            return Err(invalid());
        }
        let bytes = STANDARD
            .decode(&self.bytes_base64.0)
            .map_err(|_| invalid())?;
        if bytes.len() != size || STANDARD.encode(&bytes) != self.bytes_base64.0 {
            return Err(invalid());
        }
        model::verify(&d, &bytes, bounds()).map_err(|error| match error {
            model::Error::Limit => limit(),
            _ => invalid(),
        })?;
        Ok((d, bytes))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Mapping {
    source_prefix: Text<4096>,
    destination_prefix: Text<4096>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    mappings: Items<Mapping, 64>,
}
impl Spec {
    fn native(self) -> Result<core::ForgeTransformSpec, String> {
        let value = core::ForgeTransformSpec {
            mappings: self
                .mappings
                .0
                .into_iter()
                .map(|m| core::ForgePathMapping {
                    source_prefix: m.source_prefix.0,
                    destination_prefix: m.destination_prefix.0,
                })
                .collect(),
        };
        value.validate().map_err(core_error)?;
        Ok(value)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Hint {
    operation: core::ForgeLineageOperation,
    source_path: Text<4096>,
    target_path: Text<4096>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    spec: [u8; 32],
    source_account: Text<64>,
    before: [u8; 32],
    after: [u8; 32],
    before_view: [u8; 32],
    after_view: [u8; 32],
    before_view_name: Text<128>,
    after_view_name: Text<128>,
    destination_account: Text<64>,
    destination_view: [u8; 32],
    destination_view_name: Text<128>,
    destination_base: [u8; 32],
    change_id: [u8; 16],
    lineage: Items<Hint, 4096>,
}
impl Identity {
    fn native(self) -> Result<core::TransformIdentityInput, String> {
        AccountId(self.source_account.0.clone())
            .validate()
            .map_err(|_| invalid())?;
        AccountId(self.destination_account.0.clone())
            .validate()
            .map_err(|_| invalid())?;
        if self.source_account.0 == self.destination_account.0
            || [
                self.before,
                self.after,
                self.before_view,
                self.after_view,
                self.destination_view,
                self.destination_base,
            ]
            .contains(&[0; 32])
        {
            return Err(invalid());
        }
        for name in [
            &self.before_view_name.0,
            &self.after_view_name.0,
            &self.destination_view_name.0,
        ] {
            if name.is_empty() || name.chars().any(char::is_control) {
                return Err(invalid());
            }
        }
        let mut hints = Vec::new();
        for hint in self.lineage.0 {
            core::validate_path(&hint.source_path.0, false).map_err(core_error)?;
            core::validate_path(&hint.target_path.0, false).map_err(core_error)?;
            if matches!(hint.operation, core::ForgeLineageOperation::ImportedOrigin) {
                return Err(invalid());
            }
            hints.push(core::ForgeLineageHint {
                operation: hint.operation,
                source_path: hint.source_path.0,
                target_path: hint.target_path.0,
            });
        }
        Ok(core::TransformIdentityInput {
            spec: self.spec,
            source_account: self.source_account.0,
            before: self.before,
            after: self.after,
            before_view: self.before_view,
            after_view: self.after_view,
            before_view_name: self.before_view_name.0,
            after_view_name: self.after_view_name.0,
            destination_account: self.destination_account.0,
            destination_view: self.destination_view,
            destination_view_name: self.destination_view_name.0,
            destination_base: self.destination_base,
            change_id: self.change_id,
            lineage: hints,
        })
    }
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum IdentityCommand {
    Spec {
        spec: Spec,
    },
    Run {
        identity: Identity,
        spec: Spec,
    },
    Retire {
        account_id: Text<64>,
        principal: Text<128>,
        view: Text<128>,
        run_id: Text<64>,
    },
}
pub fn derive_identity(bytes: &[u8]) -> Result<Value, String> {
    match parse::<IdentityCommand>(bytes)? {
        // Internal host only: principal is the authenticated current actor,
        // not a field accepted by the public Forge mutation normalizer.
        IdentityCommand::Retire {
            account_id,
            principal,
            view,
            run_id,
        } => {
            AccountId(account_id.0.clone())
                .validate()
                .map_err(|_| invalid())?;
            tinytree_forge_core::accounts::validate_principal(&principal.0)
                .map_err(|_| invalid())?;
            if view.0.chars().any(char::is_control) {
                return Err(invalid());
            }
            let fingerprint =
                core::retire_fingerprint(&account_id.0, &principal.0, &view.0, id(&run_id.0)?)
                    .map_err(core_error)?;
            bounded(json!({"fingerprint":crate::hex(&fingerprint)}))
        }
        IdentityCommand::Spec { spec } => {
            bounded(json!({"spec_id":crate::hex(&spec.native()?.id().map_err(core_error)?)}))
        }
        IdentityCommand::Run { identity, spec } => {
            let identity = identity.native()?;
            let spec_id = spec.native()?.id().map_err(core_error)?;
            if identity.spec != spec_id {
                return Err(invalid());
            }
            bounded(
                json!({"spec_id":crate::hex(&spec_id),"run_id":crate::hex(&core::run_id(&identity).map_err(core_error)?)}),
            )
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectionRow {
    relative_path: Text<4096>,
    canonical_path: Text<4096>,
    descriptor: WireDescriptor,
    executable: bool,
    copy_id: Option<Text<64>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectionInput {
    spec: Spec,
    destination_root: Text<4096>,
    rows: Items<SelectionRow, 128>,
    state: Option<core::SelectionState>,
}
pub fn map_selection(bytes: &[u8]) -> Result<Value, String> {
    let input: SelectionInput = parse(bytes)?;
    let spec = input.spec.native()?;
    let rows = input
        .rows
        .0
        .into_iter()
        .map(|row| {
            core::validate_path(&row.canonical_path.0, false).map_err(core_error)?;
            Ok(core::SelectedSourceFile {
                relative_path: row.relative_path.0,
                canonical_path: row.canonical_path.0,
                descriptor: row.descriptor.native()?,
                executable: row.executable,
                copy_id: row.copy_id.map(|copy| id(&copy.0)).transpose()?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let state = input.state.unwrap_or_default();
    if state
        .last_path
        .as_ref()
        .is_some_and(|path| path.len() > 4096)
    {
        return Err(limit());
    }
    let page = core::map_selection_page(&spec, &input.destination_root.0, &rows, state)
        .map_err(core_error)?;
    let files=page.files.into_iter().map(|row|json!({"source_relative_path":row.source_relative_path,"source_canonical_path":row.source_canonical_path,"mapped_path":row.mapped_path,"destination_path":row.destination_path,"descriptor":WireDescriptor::from_native(row.descriptor),"executable":row.executable,"copy_id":row.copy_id.map(|id|crate::hex(&id))})).collect::<Vec<_>>();
    bounded(json!({"files":files,"state":page.state}))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Endpoint {
    canonical_path: Text<4096>,
    file: WireDescriptor,
    canonical_copy: Option<Text<64>>,
}
impl Endpoint {
    fn native(self) -> Result<lineage::Endpoint, String> {
        Ok(lineage::Endpoint {
            canonical_path: self.canonical_path.0,
            file: self.file.native()?,
            canonical_copy: self
                .canonical_copy
                .map(|copy| id(&copy.0).map(model::CopyId))
                .transpose()?,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Relation {
    operation: lineage::Operation,
    source_path: Text<4096>,
    target_path: Text<4096>,
    source: Endpoint,
    target: Endpoint,
    target_existed_before: bool,
    source_exists_after: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LineageInput {
    relation: Relation,
    target_copy: Supplied,
}
pub fn verify_lineage(bytes: &[u8]) -> Result<Value, String> {
    let input: LineageInput = parse(bytes)?;
    let relation = input.relation;
    let (descriptor, payload) = input.target_copy.verified()?;
    let input = lineage::RelationInput {
        operation: relation.operation,
        source_path: relation.source_path.0,
        target_path: relation.target_path.0,
        source: relation.source.native()?,
        target: relation.target.native()?,
        target_existed_before: relation.target_existed_before,
        source_exists_after: relation.source_exists_after,
    };
    let relation = lineage::verify_relation_bytes(&input, descriptor, &payload, bounds()).map_err(
        |error| match error {
            lineage::LineageError::Model(model::Error::Limit) => limit(),
            _ => invalid(),
        },
    )?;
    bounded(
        json!({"verified":true,"operation":relation.operation(),"source_path":relation.source_path(),"target_path":relation.target_path()}),
    )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    id: Text<64>,
    size: u64,
    executable: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRow {
    path: Text<4096>,
    file: File,
}
fn selected(rows: Items<FileRow, 4096>) -> Result<BTreeMap<String, core::ProjectedFile>, String> {
    let mut out = BTreeMap::new();
    let mut bytes = 0u64;
    let mut paths = core::TransformPathBudget::default();
    for row in rows.0 {
        paths.observe(&row.path.0).map_err(core_error)?;
        bytes = bytes
            .checked_add(row.file.size)
            .filter(|bytes| *bytes <= core::MAX_BYTES)
            .ok_or_else(limit)?;
        if row.file.size > core::MAX_FILE_BYTES as u64 {
            return Err(limit());
        }
        if out
            .insert(
                row.path.0,
                core::ProjectedFile {
                    id: FileId(id(&row.file.id.0)?),
                    size: row.file.size,
                    executable: row.file.executable,
                },
            )
            .is_some()
        {
            return Err(invalid());
        }
    }
    Ok(out)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Committer {
    name: Text<1024>,
    email: Text<1024>,
    millis: i64,
    offset: i32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Edge {
    operation: core::ForgeLineageOperation,
    source_account: Text<64>,
    source_revision: [u8; 32],
    source_path: Text<4096>,
    destination_path: Text<4096>,
    destination_revision: [u8; 32],
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BuildInput {
    identity: Identity,
    committer: Committer,
    destination_root: Text<4096>,
    before: Items<FileRow, 4096>,
    after: Items<FileRow, 4096>,
    lineage: Items<Edge, 8192>,
    objects: Items<Supplied, OBJECTS>,
}
enum WalkError {
    Missing(ObjectKey),
    Invalid,
    Limit,
}
fn flatten(
    identity: &core::TransformIdentityInput,
    objects: &HashMap<ObjectKey, Vec<u8>>,
) -> Result<BTreeMap<String, TreeValue>, WalkError> {
    let decode = |key: ObjectKey| -> Result<Object, WalkError> {
        let bytes = objects.get(&key).ok_or(WalkError::Missing(key))?;
        model::decode(bytes, bounds()).map_err(|_| WalkError::Invalid)
    };
    let Object::Commit(commit) = decode(ObjectKey {
        kind: Kind::Commit,
        id: ObjectId(identity.destination_base),
    })?
    else {
        return Err(WalkError::Invalid);
    };
    if commit.root_tree.terms.len() != 1 {
        return Err(WalkError::Invalid);
    }
    let mut pending = vec![(String::new(), commit.root_tree.terms[0], 0usize)];
    let mut result = BTreeMap::new();
    let mut visited = 0usize;
    let mut path_bytes = 0usize;
    while let Some((prefix, tree, depth)) = pending.pop() {
        if depth > 128 || visited >= core::MAX_FILES {
            return Err(WalkError::Limit);
        }
        visited += 1;
        let Object::Tree(tree) = decode(ObjectKey {
            kind: Kind::Tree,
            id: ObjectId(tree.0),
        })?
        else {
            return Err(WalkError::Invalid);
        };
        for entry in tree.entries {
            let path = core::join_path(&prefix, &entry.name);
            path_bytes = path_bytes
                .checked_add(path.len())
                .filter(|n| *n <= METADATA)
                .ok_or(WalkError::Limit)?;
            if path.len() > 4096 {
                return Err(WalkError::Limit);
            }
            match entry.value {
                TreeValue::Tree(id) => {
                    if pending.len() + visited >= core::MAX_FILES {
                        return Err(WalkError::Limit);
                    }
                    pending.push((path, id, depth + 1));
                }
                value => {
                    if result.len() >= core::MAX_FILES || result.insert(path, value).is_some() {
                        return Err(WalkError::Limit);
                    }
                }
            }
        }
        // Like native flatten_tree, empty directories contribute no leaf.
    }
    Ok(result)
}
pub fn build(bytes: &[u8]) -> Result<Value, String> {
    let input: BuildInput = parse(bytes)?;
    let identity = input.identity.native()?;
    let c = input.committer;
    if c.millis.unsigned_abs() > SAFE
        || !(-1440..=1440).contains(&c.offset)
        || c.name.0.chars().any(char::is_control)
        || c.email.0.chars().any(char::is_control)
    {
        return Err(invalid());
    }
    let committer = core::FrozenCommitter {
        name: c.name.0,
        email: c.email.0,
        millis: c.millis,
        offset: c.offset,
    };
    core::validate_path(&input.destination_root.0, true).map_err(core_error)?;
    let before = selected(input.before)?;
    let after = selected(input.after)?;
    let mut objects = HashMap::new();
    let mut total = 0usize;
    for supplied in input.objects.0 {
        total = total
            .checked_add(usize::try_from(supplied.size_bytes).map_err(|_| limit())?)
            .filter(|n| *n <= METADATA)
            .ok_or_else(limit)?;
        let (d, bytes) = supplied.verified()?;
        if objects.insert(d.key, bytes).is_some() {
            return Err(invalid());
        }
    }
    let destination = match flatten(&identity, &objects) {
        Ok(files) => files,
        Err(WalkError::Missing(key)) => {
            return bounded(
                json!({"state":"missing_metadata","kind":key.kind as u8,"id":crate::hex(&key.id.0)}),
            );
        }
        Err(WalkError::Limit) => return Err(limit()),
        Err(WalkError::Invalid) => return Err(invalid()),
    };
    let edges = input
        .lineage
        .0
        .into_iter()
        .map(|edge| core::ForgeLineageEdge {
            operation: edge.operation,
            source_account: edge.source_account.0,
            source_revision: edge.source_revision,
            source_path: edge.source_path.0,
            destination_path: edge.destination_path.0,
            destination_revision: edge.destination_revision,
        })
        .collect();
    let result = core::build_transform(
        &identity,
        &committer,
        &input.destination_root.0,
        &before,
        &after,
        &destination,
        edges,
        bounds(),
    )
    .map_err(core_error)?;
    let mut record = Output(Vec::new(), core::MAX_RECORD_BYTES);
    serde_json::to_writer(&mut record, &result.run).map_err(|_| limit())?;
    if result.objects.len() > OBJECTS {
        return Err(limit());
    }
    let mut total = 0usize;
    for object in &result.objects {
        total = total
            .checked_add(object.bytes.len())
            .filter(|n| *n <= METADATA)
            .ok_or_else(limit)?;
        if object.bytes.len() > OBJECT {
            return Err(limit());
        }
    }
    let objects=result.objects.into_iter().map(|object|json!({"kind":object.descriptor.key.kind as u8,"id":crate::hex(&object.descriptor.key.id.0),"size_bytes":object.descriptor.size_bytes,"format_version":object.descriptor.format_version,"bytes_base64":STANDARD.encode(object.bytes)})).collect::<Vec<_>>();
    bounded(json!({"state":"ready","run":result.run,"objects":objects}))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn wire(object: &model::EncodedObject) -> Value {
        json!({"kind":object.descriptor.key.kind as u8,"id":crate::hex(&object.descriptor.key.id.0),"size_bytes":object.descriptor.size_bytes,"format_version":object.descriptor.format_version,"bytes_base64":STANDARD.encode(&object.bytes)})
    }
    fn encode(object: Object) -> model::EncodedObject {
        model::encode(&object, bounds()).unwrap()
    }
    #[test]
    fn spec_identity_selection_and_strict_bounds_are_shared_native_behavior() {
        let spec = core::ForgeTransformSpec {
            mappings: vec![core::ForgePathMapping {
                source_prefix: "".into(),
                destination_prefix: "vendor".into(),
            }],
        };
        let spec_value = serde_json::to_value(&spec).unwrap();
        let result = derive_identity(
            &serde_json::to_vec(&json!({"kind":"spec","spec":spec_value})).unwrap(),
        )
        .unwrap();
        assert_eq!(result["spec_id"], crate::hex(&spec.id().unwrap()));
        let retire = json!({"kind":"retire","account_id":"destination","principal":"fixture-owner","view":"","run_id":crate::hex(&[7;32])});
        let result = derive_identity(&serde_json::to_vec(&retire).unwrap()).unwrap();
        assert_eq!(
            result["fingerprint"],
            crate::hex(
                &core::retire_fingerprint("destination", "fixture-owner", "", [7; 32]).unwrap()
            )
        );
        let mut other = retire;
        other["principal"] = json!("another-owner");
        assert_ne!(
            derive_identity(&serde_json::to_vec(&other).unwrap()).unwrap(),
            result
        );
        other["principal"] = json!("bad\nprincipal");
        assert!(derive_identity(&serde_json::to_vec(&other).unwrap()).is_err());
        let file = model::describe_file(b"file");
        let page = json!({"spec":spec,"destination_root":"project","rows":[{"relative_path":"file","canonical_path":"public/file","descriptor":WireDescriptor::from_native(file),"executable":false,"copy_id":null}],"state":null});
        let result = map_selection(&serde_json::to_vec(&page).unwrap()).unwrap();
        assert_eq!(result["files"][0]["mapped_path"], "vendor/file");
        assert_eq!(
            result["files"][0]["destination_path"],
            "project/vendor/file"
        );
        assert_eq!(result["state"]["selected"], 1);
        assert!(
            derive_identity(br#"{"kind":"spec","spec":{"mappings":[]},"principal":"forbidden"}"#)
                .is_err()
        );
        assert!(
            derive_identity(br#"{"kind":"spec","kind":"spec","spec":{"mappings":[]}}"#).is_err()
        );
        assert!(
            build(&vec![b' '; INPUT + 1])
                .unwrap_err()
                .starts_with("resource_exhausted:")
        );
    }
    #[test]
    fn lineage_adapter_verifies_canonical_bytes_without_returning_private_parents() {
        let copy = encode(Object::CopyHistory(model::CopyHistory {
            current_path: "public/new".into(),
            parents: vec![model::CopyId([1; 32]), model::CopyId([239; 32])],
            salt: vec![2],
        }));
        let source = model::describe_file(b"before");
        let target = model::describe_file(b"after edits");
        let mut input = json!({"relation":{"operation":"rename","source_path":"old","target_path":"new","source":{"canonical_path":"public/old","file":WireDescriptor::from_native(source),"canonical_copy":crate::hex(&[1;32])},"target":{"canonical_path":"public/new","file":WireDescriptor::from_native(target),"canonical_copy":crate::hex(&copy.descriptor.key.id.0)},"target_existed_before":false,"source_exists_after":false},"target_copy":wire(&copy)});
        let result = verify_lineage(&serde_json::to_vec(&input).unwrap()).unwrap();
        assert_eq!(
            result,
            json!({"verified":true,"operation":"rename","source_path":"public/old","target_path":"public/new"})
        );
        input["target_copy"]["id"] = json!(crate::hex(&[9; 32]));
        assert!(verify_lineage(&serde_json::to_vec(&input).unwrap()).is_err());
    }
    #[test]
    fn build_fetches_only_destination_tree_and_matches_native_empty_directory_semantics() {
        let empty = encode(Object::Tree(model::Tree { entries: vec![] }));
        let root = encode(Object::Tree(model::Tree {
            entries: vec![model::TreeEntry {
                name: "empty".into(),
                value: TreeValue::Tree(model::TreeId(empty.descriptor.key.id.0)),
            }],
        }));
        let person = model::Signature {
            name: "Fixture".into(),
            email: "fixture@example.invalid".into(),
            timestamp: model::Timestamp {
                millis_since_epoch: 0,
                tz_offset_minutes: 0,
            },
        };
        let commit = encode(Object::Commit(model::Commit {
            parents: vec![model::CommitId([13; 32])],
            predecessors: vec![model::CommitId([14; 32])],
            root_tree: model::TreeMerge {
                terms: vec![model::TreeId(root.descriptor.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: model::ChangeId([15; 16]),
            description: "Fixture destination".into(),
            author: person.clone(),
            committer: person.clone(),
            secure_signature: None,
        }));
        let spec = core::ForgeTransformSpec {
            mappings: vec![core::ForgePathMapping {
                source_prefix: String::new(),
                destination_prefix: "vendor".into(),
            }],
        };
        let identity = core::TransformIdentityInput {
            spec: spec.id().unwrap(),
            source_account: "source".into(),
            before: [1; 32],
            after: [2; 32],
            before_view: [3; 32],
            after_view: [4; 32],
            before_view_name: "public".into(),
            after_view_name: "public".into(),
            destination_account: "destination".into(),
            destination_view: [5; 32],
            destination_view_name: "workspace".into(),
            destination_base: commit.descriptor.key.id.0,
            change_id: [6; 16],
            lineage: vec![],
        };
        let committer = core::FrozenCommitter::capture(&person);
        let file = model::describe_file(b"selected bytes");
        let mut input = json!({"identity":identity,"committer":committer,"destination_root":"","before":[],"after":[{"path":"vendor/file","file":{"id":crate::hex(&file.key.id.0),"size":file.size_bytes,"executable":false}}],"lineage":[],"objects":[]});
        let result = build(&serde_json::to_vec(&input).unwrap()).unwrap();
        assert_eq!(result["state"], "missing_metadata");
        assert_eq!(result["id"], crate::hex(&commit.descriptor.key.id.0));
        input["objects"] = json!([wire(&commit), wire(&root)]);
        let result = build(&serde_json::to_vec(&input).unwrap()).unwrap();
        assert_eq!(result["state"], "missing_metadata");
        assert_eq!(result["id"], crate::hex(&empty.descriptor.key.id.0));
        input["objects"] = json!([wire(&commit), wire(&root), wire(&empty)]);
        let result = build(&serde_json::to_vec(&input).unwrap()).unwrap();
        assert_eq!(result["state"], "ready");
        // Parent/predecessor metadata was deliberately not supplied: this is
        // current-tree construction, not ancestry export or closure proof.
        let after = BTreeMap::from([(
            "vendor/file".into(),
            core::ProjectedFile {
                id: FileId(file.key.id.0),
                size: file.size_bytes,
                executable: false,
            },
        )]);
        let expected = core::build_transform(
            &identity,
            &committer,
            "",
            &BTreeMap::new(),
            &after,
            &BTreeMap::new(),
            vec![],
            bounds(),
        )
        .unwrap();
        assert_eq!(result["run"], serde_json::to_value(&expected.run).unwrap());
        input["objects"][2]["bytes_base64"] = json!(STANDARD.encode(b"corrupt"));
        assert!(build(&serde_json::to_vec(&input).unwrap()).is_err());
    }
}
