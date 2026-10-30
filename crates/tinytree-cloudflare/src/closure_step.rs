//! Bounded canonical metadata pages, not a closure or storage receipt.
//!
//! The host owns SQL frontier coverage, current authorization, job counters,
//! fresh FILE-byte verification and atomic retained-root publication. Supplying
//! an ordinal here does not prove that earlier ordinals were checked. FILE-size
//! checks below compare descriptors only, never payload presence or durability.
use serde::Deserialize;
use serde_json::{Value, json};
use tinytree_model::{self as model, Descriptor, Kind, Object, ObjectId, ObjectKey, TreeValue};

const MAX_INPUT_BYTES: usize = 2 * 1024 * 1024;
const MAX_METADATA_BYTES: usize = 256 * 1024;
const MAX_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_PAGE_ITEMS: usize = 128;
const MAX_MODEL_ITEMS: usize = 4096;
const MAX_PATH_BYTES: usize = 4096;
const MAX_PATH_COMPONENTS: usize = 64;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyInput {
    kind: u8,
    id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DescriptorInput {
    kind: u8,
    id: String,
    size_bytes: u64,
    format_version: u16,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataInput {
    kind: u8,
    id: String,
    size_bytes: u64,
    format_version: u16,
    bytes: Vec<u8>,
}
#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    References {
        object: MetadataInput,
        cursor: u32,
        limit: u32,
    },
    TreeContext {
        object: MetadataInput,
        prefix: String,
        cursor: u32,
        limit: u32,
    },
    Check {
        obligations: Vec<Obligation>,
    },
}
#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Obligation {
    FileSize {
        key: KeyInput,
        expected_size: u64,
        descriptor: DescriptorInput,
    },
    SymlinkSize {
        key: KeyInput,
        expected_size: u64,
        object: MetadataInput,
    },
    CopyPath {
        key: KeyInput,
        expected_path: String,
        object: MetadataInput,
    },
}
fn limits() -> model::Limits {
    model::Limits {
        max_object_bytes: MAX_METADATA_BYTES,
        max_items: MAX_MODEL_ITEMS,
        max_field_bytes: MAX_METADATA_BYTES,
        max_path_bytes: MAX_PATH_BYTES,
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn key(kind: u8, id: &str) -> Result<ObjectKey, String> {
    let kind = Kind::try_from(kind).map_err(|_| "unsupported object kind")?;
    if id.len() != 64
        || !id
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err("invalid typed object identity".into());
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&id[index * 2..index * 2 + 2], 16)
            .map_err(|_| "invalid typed object identity")?;
    }
    if kind == Kind::Commit && bytes == model::CommitId::ROOT.0 {
        return Err("intrinsic root has no stored descriptor".into());
    }
    Ok(ObjectKey {
        kind,
        id: ObjectId(bytes),
    })
}
fn safe_size(size: u64) -> Result<(), String> {
    if size > MAX_SAFE_INTEGER {
        Err("numeric size limit".into())
    } else {
        Ok(())
    }
}
fn descriptor(
    kind: u8,
    id: &str,
    size_bytes: u64,
    format_version: u16,
) -> Result<Descriptor, String> {
    safe_size(size_bytes)?;
    if format_version != model::FORMAT_VERSION {
        return Err("unsupported object format".into());
    }
    Ok(Descriptor {
        key: key(kind, id)?,
        size_bytes,
        format_version,
    })
}
fn descriptor_value(descriptor: Descriptor) -> Value {
    json!({ "kind": descriptor.key.kind as u8, "id": hex(&descriptor.key.id.0),
        "size_bytes": descriptor.size_bytes, "format_version": descriptor.format_version })
}
fn metadata(input: MetadataInput) -> Result<(Descriptor, Object), String> {
    if input.bytes.len() > MAX_METADATA_BYTES {
        return Err("metadata byte limit".into());
    }
    let descriptor = descriptor(
        input.kind,
        &input.id,
        input.size_bytes,
        input.format_version,
    )?;
    if descriptor.key.kind == Kind::File {
        return Err("metadata command does not verify FILE payloads".into());
    }
    model::verify(&descriptor, &input.bytes, limits())
        .map_err(|error| format!("canonical metadata: {error}"))?;
    let object = model::decode(&input.bytes, limits())
        .map_err(|error| format!("canonical metadata: {error}"))?;
    Ok((descriptor, object))
}
fn page(cursor: u32, limit: u32, total: usize) -> Result<(usize, usize), String> {
    let cursor = usize::try_from(cursor).map_err(|_| "invalid ordinal")?;
    let limit = usize::try_from(limit).map_err(|_| "invalid page limit")?;
    if cursor > total {
        return Err("invalid ordinal".into());
    }
    if !(1..=MAX_PAGE_ITEMS).contains(&limit) {
        return Err("page item limit".into());
    }
    Ok((cursor, cursor.saturating_add(limit).min(total)))
}
fn next(cursor: usize, total: usize) -> Option<usize> {
    (cursor < total).then_some(cursor)
}
/// Native raw paths, not Forge ACL/view-path authorization. Keep the canonical
/// model's component rules: '/' separates components; '.', '..' and NUL fail.
fn path_components(path: &str, allow_empty: bool) -> Result<usize, String> {
    if path.is_empty() && allow_empty {
        return Ok(0);
    }
    if path.is_empty() || path.len() > MAX_PATH_BYTES {
        return Err("tree path byte limit".into());
    }
    let mut count = 0;
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." || component.contains('\0')
        {
            return Err("invalid native path component".into());
        }
        count += 1;
        if count > MAX_PATH_COMPONENTS {
            return Err("tree path depth limit".into());
        }
    }
    Ok(count)
}
struct PageBytes {
    used: usize,
}
impl PageBytes {
    fn new() -> Self {
        Self { used: 1024 }
    }
    fn charge(&mut self, item: &Value) -> Result<bool, String> {
        let bytes = serde_json::to_vec(item).map_err(|_| "page encoding")?.len();
        let Some(total) = self.used.checked_add(bytes + 1) else {
            return Err("page output limit".into());
        };
        if total > MAX_OUTPUT_BYTES {
            return Ok(false);
        }
        self.used = total;
        Ok(true)
    }
}

fn references(input: MetadataInput, cursor: u32, limit: u32) -> Result<Value, String> {
    let (descriptor, object) = metadata(input)?;
    // Reuse the canonical model's exact reference order and intrinsic-root rule.
    // This is one bounded metadata object's adjacency, never an entire closure.
    let references = object.references();
    if references.len() > MAX_MODEL_ITEMS * 2 {
        return Err("metadata reference limit".into());
    }
    let (start, end) = page(cursor, limit, references.len())?;
    let mut requirements: Vec<Option<(u64, &'static str)>> = Vec::new();
    if let Object::Tree(tree) = &object {
        for entry in &tree.entries {
            match entry.value {
                TreeValue::File {
                    size_bytes,
                    copy_id,
                    ..
                } => {
                    requirements.push(Some((size_bytes, "stored_file")));
                    if copy_id.is_some() {
                        requirements.push(None);
                    }
                }
                TreeValue::Symlink { target_bytes, .. } => {
                    requirements.push(Some((target_bytes, "symlink_target")))
                }
                TreeValue::Tree(_) => requirements.push(None),
            }
        }
        if requirements.len() != references.len() {
            return Err("canonical reference layout mismatch".into());
        }
    }
    let root_start = match &object {
        Object::Commit(commit) => Some(
            references
                .len()
                .checked_sub(commit.root_tree.terms.len())
                .ok_or("canonical root reference layout")?,
        ),
        _ => None,
    };
    let mut budget = PageBytes::new();
    let mut output = Vec::new();
    let mut position = start;
    while position < end {
        let key = references[position];
        let mut item = json!({"ordinal":position,"kind":key.kind as u8,"id":hex(&key.id.0),
            "root_tree":root_start.is_some_and(|start| position >= start)});
        if let Some(Some((size, semantics))) = requirements.get(position) {
            safe_size(*size)?;
            item["expected_size"] = json!(size);
            item["size_semantics"] = json!(semantics);
        }
        if !budget.charge(&item)? {
            break;
        }
        output.push(item);
        position += 1;
    }
    if position == start && position < end {
        return Err("page output limit".into());
    }
    Ok(
        json!({"descriptor":descriptor_value(descriptor),"total":references.len(),
        "next_cursor":next(position,references.len()),"references":output}),
    )
}
fn tree_context(
    input: MetadataInput,
    prefix: String,
    cursor: u32,
    limit: u32,
) -> Result<Value, String> {
    let (descriptor, object) = metadata(input)?;
    let Object::Tree(tree) = object else {
        return Err("tree context requires TREE metadata".into());
    };
    let depth = path_components(&prefix, true)?;
    let (start, end) = page(cursor, limit, tree.entries.len())?;
    let mut budget = PageBytes::new();
    let mut contexts = Vec::new();
    let mut obligations = Vec::new();
    let mut position = start;
    while position < end {
        let entry = &tree.entries[position];
        let length = prefix
            .len()
            .checked_add(usize::from(!prefix.is_empty()))
            .and_then(|n| n.checked_add(entry.name.len()))
            .ok_or("tree path byte limit")?;
        // Check EVERY entry, even plain FILE/Symlink entries emitting no facts.
        if length > MAX_PATH_BYTES {
            return Err("tree path byte limit".into());
        }
        if depth >= MAX_PATH_COMPONENTS {
            return Err("tree path depth limit".into());
        }
        let path = if prefix.is_empty() {
            entry.name.clone()
        } else {
            format!("{prefix}/{}", entry.name)
        };
        path_components(&path, false)?;
        match entry.value {
            TreeValue::Tree(id) => {
                let item = json!({"tree_id":hex(&id.0),"prefix":path});
                if !budget.charge(&item)? {
                    break;
                }
                contexts.push(item);
            }
            TreeValue::File {
                copy_id: Some(id), ..
            } => {
                let item = json!({"copy_id":hex(&id.0),"current_path":path});
                if !budget.charge(&item)? {
                    break;
                }
                obligations.push(item);
            }
            _ => {}
        }
        position += 1;
    }
    if position == start && position < end {
        return Err("page output limit".into());
    }
    Ok(
        json!({"descriptor":descriptor_value(descriptor),"total":tree.entries.len(),
        "processed":position-start,"next_cursor":next(position,tree.entries.len()),
        "contexts":contexts,"copy_obligations":obligations}),
    )
}
fn check(obligations: Vec<Obligation>) -> Result<Value, String> {
    if obligations.len() > MAX_PAGE_ITEMS {
        return Err("obligation item limit".into());
    }
    let count = obligations.len();
    for obligation in obligations {
        match obligation {
            Obligation::FileSize {
                key: expected,
                expected_size,
                descriptor: actual,
            } => {
                safe_size(expected_size)?;
                let expected = key(expected.kind, &expected.id)?;
                let actual = descriptor(
                    actual.kind,
                    &actual.id,
                    actual.size_bytes,
                    actual.format_version,
                )?;
                if expected.kind != Kind::File
                    || actual.key != expected
                    || actual.size_bytes != expected_size
                {
                    return Err("FILE descriptor length or identity mismatch".into());
                }
                // Deliberately no FILE content hash/presence/durability claim.
            }
            Obligation::SymlinkSize {
                key: expected,
                expected_size,
                object,
            } => {
                safe_size(expected_size)?;
                let expected = key(expected.kind, &expected.id)?;
                let (actual, object) = metadata(object)?;
                if expected.kind != Kind::Symlink || actual.key != expected {
                    return Err("Symlink identity mismatch".into());
                }
                let Object::Symlink(target) = object else {
                    return Err("Symlink metadata kind mismatch".into());
                };
                if target.len() as u64 != expected_size {
                    return Err("Symlink target byte length mismatch".into());
                }
            }
            Obligation::CopyPath {
                key: expected,
                expected_path,
                object,
            } => {
                path_components(&expected_path, false)?;
                let expected = key(expected.kind, &expected.id)?;
                let (actual, object) = metadata(object)?;
                if expected.kind != Kind::CopyHistory || actual.key != expected {
                    return Err("CopyHistory identity mismatch".into());
                }
                let Object::CopyHistory(history) = object else {
                    return Err("CopyHistory metadata kind mismatch".into());
                };
                if history.current_path != expected_path {
                    return Err("CopyHistory current path mismatch".into());
                }
            }
        }
    }
    Ok(json!({"valid":true,"checked":count}))
}
/// Strict bounded page operation. The host must persist monotonic coverage and
/// fulfill every emitted obligation before acknowledging a complete closure.
pub fn step(input: &[u8]) -> Result<Value, String> {
    if input.len() > MAX_INPUT_BYTES {
        return Err("closure step input byte limit".into());
    }
    let command: Command =
        serde_json::from_slice(input).map_err(|_| "invalid closure step request")?;
    let result = match command {
        Command::References {
            object,
            cursor,
            limit,
        } => references(object, cursor, limit),
        Command::TreeContext {
            object,
            prefix,
            cursor,
            limit,
        } => tree_context(object, prefix, cursor, limit),
        Command::Check { obligations } => check(obligations),
    }?;
    if serde_json::to_vec(&result)
        .map_err(|_| "closure step encoding")?
        .len()
        > MAX_OUTPUT_BYTES
    {
        return Err("closure step output byte limit".into());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{CommitId, CopyHistory, CopyId, FileId, Tree, TreeEntry, TreeId};
    fn stored(object: Object) -> Value {
        let encoded = model::encode(&object, limits()).unwrap();
        json!({"kind":encoded.descriptor.key.kind as u8,"id":hex(&encoded.descriptor.key.id.0),
            "size_bytes":encoded.descriptor.size_bytes,"format_version":encoded.descriptor.format_version,"bytes":encoded.bytes})
    }
    fn command(input: Value) -> Result<Value, String> {
        step(&serde_json::to_vec(&input).unwrap())
    }
    #[test]
    fn pages_preserve_parent_predecessor_and_all_merge_terms() {
        let mut commit = model::synthetic_root();
        commit.parents = vec![CommitId::ROOT, CommitId([1; 32])];
        commit.predecessors = vec![CommitId([2; 32]), CommitId::ROOT];
        commit.root_tree.terms = vec![TreeId([3; 32]), TreeId([4; 32]), TreeId([3; 32])];
        commit.root_tree.labels = vec![String::new(); 3];
        let expected = Object::Commit(commit.clone()).references();
        let object = stored(Object::Commit(commit));
        let mut cursor = 0;
        let mut actual = Vec::new();
        loop {
            let page = command(
                json!({"operation":"references","object":object,"cursor":cursor,"limit":2}),
            )
            .unwrap();
            assert_eq!(page["total"], 5);
            for value in page["references"].as_array().unwrap() {
                assert_eq!(value["ordinal"], actual.len());
                assert_eq!(value["root_tree"], actual.len() >= 2);
                actual.push((
                    value["kind"].as_u64().unwrap() as u8,
                    value["id"].as_str().unwrap().to_owned(),
                ));
            }
            match page["next_cursor"].as_u64() {
                Some(next) => cursor = next,
                None => break,
            }
        }
        assert_eq!(
            actual,
            expected
                .iter()
                .map(|k| (k.kind as u8, hex(&k.id.0)))
                .collect::<Vec<_>>()
        );
        let copy = stored(Object::CopyHistory(CopyHistory {
            current_path: "a".into(),
            parents: vec![CopyId([0; 32]), CopyId([8; 32])],
            salt: vec![],
        }));
        let page = command(json!({"operation":"references","object":copy,"cursor":0,"limit":128}))
            .unwrap();
        assert_eq!(page["references"].as_array().unwrap().len(), 2);
        assert_eq!(page["references"][0]["id"], "0".repeat(64));
    }
    #[test]
    fn plain_leaf_paths_are_checked_and_copy_obligations_bind_exact_context() {
        let plain = stored(Object::Tree(Tree {
            entries: vec![TreeEntry {
                name: "file".into(),
                value: TreeValue::File {
                    id: FileId([0; 32]),
                    size_bytes: 0,
                    executable: false,
                    copy_id: None,
                },
            }],
        }));
        let prefix = vec!["a"; 64].join("/");
        assert!(command(json!({"operation":"tree_context","object":plain,"prefix":prefix,"cursor":0,"limit":128})).is_err());
        let history = stored(Object::CopyHistory(CopyHistory {
            current_path: "src/file".into(),
            parents: vec![],
            salt: vec![],
        }));
        let copy_id = match key(5, history["id"].as_str().unwrap()).unwrap() {
            ObjectKey { id, .. } => CopyId(id.0),
        };
        let tree = stored(Object::Tree(Tree {
            entries: vec![TreeEntry {
                name: "file".into(),
                value: TreeValue::File {
                    id: FileId([7; 32]),
                    size_bytes: 3,
                    executable: false,
                    copy_id: Some(copy_id),
                },
            }],
        }));
        let page = command(
            json!({"operation":"tree_context","object":tree,"prefix":"src","cursor":0,"limit":128}),
        )
        .unwrap();
        assert_eq!(page["processed"], 1);
        assert!(page["next_cursor"].is_null());
        assert_eq!(page["copy_obligations"][0]["current_path"], "src/file");
        let obligation = json!({"operation":"copy_path","key":{"kind":5,"id":history["id"]},"expected_path":"src/file","object":history});
        assert_eq!(
            command(json!({"operation":"check","obligations":[obligation.clone()]})).unwrap()["checked"],
            1
        );
        let mut wrong = obligation;
        wrong["expected_path"] = json!("other/file");
        assert!(command(json!({"operation":"check","obligations":[wrong]})).is_err());
        let leaf_page =
            command(json!({"operation":"references","object":tree,"cursor":0,"limit":128}))
                .unwrap();
        assert_eq!(leaf_page["references"][0]["expected_size"], 3);
        assert_eq!(leaf_page["references"][0]["size_semantics"], "stored_file");
        assert!(leaf_page["references"][1].get("expected_size").is_none());
    }
    #[test]
    fn output_limit_resumes_at_an_entry_boundary_and_inputs_are_strict() {
        let prefix = vec!["a".repeat(60); 63].join("/");
        let entries = (0..128)
            .map(|i| TreeEntry {
                name: format!("f{i:03}"),
                value: TreeValue::Tree(TreeId([7; 32])),
            })
            .collect();
        let tree = stored(Object::Tree(Tree { entries }));
        let mut cursor = 0u64;
        let mut processed = 0u64;
        loop {
            let output=command(json!({"operation":"tree_context","object":tree,"prefix":prefix,"cursor":cursor,"limit":128})).unwrap();
            assert!(serde_json::to_vec(&output).unwrap().len() <= MAX_OUTPUT_BYTES);
            let count = output["processed"].as_u64().unwrap();
            assert!(count > 0);
            processed += count;
            match output["next_cursor"].as_u64() {
                Some(next) => {
                    assert_eq!(next, cursor + count);
                    cursor = next;
                }
                None => break,
            }
        }
        assert_eq!(processed, 128);
        assert!(
            command(json!({"operation":"references","object":tree,"cursor":129,"limit":1}))
                .is_err()
        );
        assert!(
            command(json!({"operation":"references","object":tree,"cursor":0,"limit":0})).is_err()
        );
        let mut corrupt = tree.clone();
        corrupt["bytes"][0] = json!(0);
        assert!(
            command(json!({"operation":"references","object":corrupt,"cursor":0,"limit":1}))
                .is_err()
        );
        let mut unknown = json!({"operation":"references","object":tree,"cursor":0,"limit":1});
        unknown["actor"] = json!("owner");
        assert!(command(unknown).is_err());
        let duplicate = format!(
            r#"{{"operation":"references","object":{},"cursor":0,"cursor":1,"limit":1}}"#,
            tree
        );
        assert!(step(duplicate.as_bytes()).is_err());
        assert!(step(&vec![b' '; MAX_INPUT_BYTES + 1]).is_err());
    }
    #[test]
    fn child_checks_distinguish_stored_file_and_logical_symlink_sizes() {
        let file = json!({"kind":1,"id":"0".repeat(64),"size_bytes":3,"format_version":1});
        let check = json!({"operation":"file_size","key":{"kind":1,"id":"0".repeat(64)},"expected_size":3,"descriptor":file});
        // This success is deliberately possible without ANY FILE bytes. It is
        // only a descriptor consistency check; the host must own byte proof.
        assert_eq!(
            command(json!({"operation":"check","obligations":[check.clone()]})).unwrap()["checked"],
            1
        );
        let mut wrong = check;
        wrong["expected_size"] = json!(4);
        assert!(command(json!({"operation":"check","obligations":[wrong]})).is_err());
        let symlink = stored(Object::Symlink("é".into()));
        let check = json!({"operation":"symlink_size","key":{"kind":4,"id":symlink["id"]},"expected_size":2,"object":symlink});
        assert_eq!(
            command(json!({"operation":"check","obligations":[check.clone()]})).unwrap()["checked"],
            1
        );
        let mut wrong = check;
        wrong["expected_size"] = json!(1);
        assert!(command(json!({"operation":"check","obligations":[wrong]})).is_err());
    }
}
