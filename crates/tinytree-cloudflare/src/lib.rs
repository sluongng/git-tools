//! Canonical object validation for a bounded, single-account repository host.
//! Descriptor-only FILE entries in closure requests MUST come from the host's
//! verified, durable byte receipts; this API cannot authenticate an external CAS.
use serde::Deserialize;
mod forge;
mod accounts;
mod landing;
mod projection;
mod closure_step;
mod views;
mod search;
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
};
use tinytree_model::{self as model, Descriptor, Kind, Object, ObjectId, ObjectKey, TreeValue};

pub const MAX_INPUT: usize = 8 * 1024 * 1024;
const MAX_METADATA: usize = 1024 * 1024;
const MAX_OBJECTS: usize = 4096;
const MAX_EDGES: usize = 65536;
const MAX_CONTEXTS: usize = 16384;
const MAX_CONTEXT_BYTES: usize = 1024 * 1024;
fn limits() -> model::Limits {
    model::Limits {
        max_object_bytes: MAX_METADATA,
        max_items: 4096,
        max_field_bytes: MAX_METADATA,
        max_path_bytes: 4096,
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn key(kind: u32, id: &str) -> Result<ObjectKey, String> {
    let kind = u8::try_from(kind)
        .ok()
        .and_then(|k| Kind::try_from(k).ok())
        .ok_or("unsupported kind")?;
    if id.len() != 64
        || !id
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err("invalid object id".into());
    }
    let mut bytes = [0; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&id[2 * i..2 * i + 2], 16).map_err(|_| "invalid object id")?;
    }
    Ok(ObjectKey {
        kind,
        id: ObjectId(bytes),
    })
}
fn edge(key: ObjectKey) -> Value {
    json!({"kind":key.kind as u8,"id":hex(&key.id.0)})
}
fn decode(kind: Kind, bytes: &[u8]) -> Result<(Descriptor, Option<Object>), String> {
    let max = if kind == Kind::File {
        MAX_INPUT
    } else {
        MAX_METADATA
    };
    if bytes.len() > max {
        return Err("object byte limit".into());
    }
    // File identity is the same native unkeyed BLAKE3 digest for every kind.
    let mut descriptor = model::describe_file(bytes);
    descriptor.key.kind = kind;
    let mut bounds = limits();
    bounds.max_object_bytes = max;
    model::verify(&descriptor, bytes, bounds).map_err(|e| format!("canonical object: {e}"))?;
    let object = if kind == Kind::File {
        None
    } else {
        Some(model::decode(bytes, limits()).map_err(|e| format!("canonical metadata: {e}"))?)
    };
    Ok((descriptor, object))
}
/// Inspect supplied bytes, never a caller-provided digest alone.
pub fn inspect(kind: u32, bytes: &[u8]) -> Result<Value, String> {
    let kind = u8::try_from(kind)
        .ok()
        .and_then(|k| Kind::try_from(k).ok())
        .ok_or("unsupported kind")?;
    let (d, object) = decode(kind, bytes)?;
    let mut result = json!({"kind":kind as u8,"id":hex(&d.key.id.0),"size_bytes":d.size_bytes,"format_version":d.format_version,"references":object.as_ref().map(Object::references).unwrap_or_default().into_iter().map(edge).collect::<Vec<_>>()});
    match object {
        Some(Object::Tree(tree)) => {
            result["tree"] = json!({"entries":tree.entries.into_iter().map(|entry| {
            let mut v = match entry.value {
                TreeValue::File{id,executable,size_bytes,copy_id}=>json!({"kind":1,"id":hex(&id.0),"executable":executable,"size_bytes":size_bytes,"copy_id":copy_id.map(|id|hex(&id.0))}),
                TreeValue::Symlink{id,target_bytes}=>json!({"kind":4,"id":hex(&id.0),"size_bytes":target_bytes}),
                TreeValue::Tree(id)=>json!({"kind":2,"id":hex(&id.0)})};
            v["name"]=json!(entry.name);v
        }).collect::<Vec<_>>()});
        }
        Some(Object::Commit(c)) => {
            result["commit"] = json!({"parents":c.parents.iter().map(|id|hex(&id.0)).collect::<Vec<_>>(),"predecessors":c.predecessors.iter().map(|id|hex(&id.0)).collect::<Vec<_>>(),"root_trees":c.root_tree.terms.iter().map(|id|hex(&id.0)).collect::<Vec<_>>()});
        }
        Some(Object::CopyHistory(c)) => {
            result["copy_history"] = json!({"current_path":c.current_path,"parents":c.parents.iter().map(|id|hex(&id.0)).collect::<Vec<_>>()});
        }
        Some(Object::Symlink(target)) => {
            result["symlink"] = json!({"target_bytes":target.len()});
        }
        None => {}
    }
    Ok(result)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyInput {
    kind: u32,
    id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Supplied {
    kind: u32,
    id: String,
    size_bytes: u64,
    #[serde(default = "version")]
    format_version: u16,
    bytes: Option<Vec<u8>>,
}
fn version() -> u16 {
    model::FORMAT_VERSION
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Closure {
    root: KeyInput,
    objects: Vec<Supplied>,
}
struct Stored {
    descriptor: Descriptor,
    object: Option<Object>,
}
/// Strict bounded closure validation, including every merge term, predecessor,
/// leaf byte length and CopyHistory's path at each reachable tree context.
pub fn validate_closure(input: &[u8]) -> Result<Value, String> {
    if input.len() > MAX_INPUT {
        return Err("input byte limit".into());
    }
    let request: Closure =
        serde_json::from_slice(input).map_err(|e| format!("closure request: {e}"))?;
    if request.objects.len() > MAX_OBJECTS {
        return Err("object count limit".into());
    }
    let root = key(request.root.kind, &request.root.id)?;
    let mut objects = HashMap::new();
    let mut metadata_bytes = 0usize;
    let mut total = 0u64;
    for supplied in request.objects {
        let key = key(supplied.kind, &supplied.id)?;
        if supplied.format_version != model::FORMAT_VERSION {
            return Err("unsupported format version".into());
        }
        if supplied.size_bytes > MAX_INPUT as u64 {
            return Err("object byte limit".into());
        }
        total = total
            .checked_add(supplied.size_bytes)
            .ok_or("closure byte limit")?;
        if total > 64 * 1024 * 1024 {
            return Err("closure byte limit".into());
        }
        let descriptor = Descriptor {
            key,
            size_bytes: supplied.size_bytes,
            format_version: supplied.format_version,
        };
        let object = match supplied.bytes {
            Some(bytes) => {
                if key.kind != Kind::File {
                    metadata_bytes = metadata_bytes
                        .checked_add(bytes.len())
                        .ok_or("metadata byte limit")?;
                    if metadata_bytes > 4 * MAX_METADATA {
                        return Err("metadata byte limit".into());
                    }
                }
                let (actual, object) = decode(key.kind, &bytes)?;
                if actual != descriptor {
                    return Err("descriptor mismatch".into());
                }
                object
            }
            None if key.kind == Kind::File => None,
            None => return Err("metadata bytes required".into()),
        };
        if objects.insert(key, Stored { descriptor, object }).is_some() {
            return Err("duplicate object".into());
        }
    }
    let intrinsic = |k: ObjectKey| k.kind == Kind::Commit && k.id.0 == model::CommitId::ROOT.0;
    let mut pending = vec![root];
    let mut seen = HashSet::new();
    let mut roots = HashSet::new();
    let mut edges = 0usize;
    let mut reachable_bytes = 0u64;
    while let Some(k) = pending.pop() {
        if intrinsic(k) || !seen.insert(k) {
            continue;
        }
        let stored = objects.get(&k).ok_or("missing object")?;
        reachable_bytes += stored.descriptor.size_bytes;
        if k == root && k.kind == Kind::Tree {
            roots.insert(k);
        }
        if let Some(object) = &stored.object {
            if let Object::Commit(c) = object {
                for tree in &c.root_tree.terms {
                    roots.insert(ObjectKey {
                        kind: Kind::Tree,
                        id: ObjectId(tree.0),
                    });
                }
            }
            if let Object::Tree(tree) = object {
                for entry in &tree.entries {
                    let (child, size, symlink) = match entry.value {
                        TreeValue::File { id, size_bytes, .. } => (
                            ObjectKey {
                                kind: Kind::File,
                                id: ObjectId(id.0),
                            },
                            size_bytes,
                            false,
                        ),
                        TreeValue::Symlink { id, target_bytes } => (
                            ObjectKey {
                                kind: Kind::Symlink,
                                id: ObjectId(id.0),
                            },
                            target_bytes,
                            true,
                        ),
                        TreeValue::Tree(_) => continue,
                    };
                    let child = objects.get(&child).ok_or("missing tree child")?;
                    let actual = if symlink {
                        match &child.object {
                            Some(Object::Symlink(target)) => target.len() as u64,
                            _ => return Err("symlink type".into()),
                        }
                    } else {
                        child.descriptor.size_bytes
                    };
                    if actual != size {
                        return Err("tree child byte length".into());
                    }
                }
            }
            let refs = object.references();
            edges += refs.len();
            if edges > MAX_EDGES {
                return Err("edge limit".into());
            }
            pending.extend(refs);
        }
    }
    let mut contexts = HashSet::new();
    let mut work = Vec::new();
    let mut context_bytes = 0usize;
    for root in roots {
        contexts.insert((root, String::new()));
        work.push((root, String::new(), 0usize));
    }
    while let Some((tree, prefix, depth)) = work.pop() {
        let Some(Object::Tree(tree)) = objects.get(&tree).and_then(|s| s.object.as_ref()) else {
            return Err("tree context type".into());
        };
        for entry in &tree.entries {
            edges += 1;
            if edges > MAX_EDGES {
                return Err("edge limit".into());
            }
            let length = prefix.len() + usize::from(!prefix.is_empty()) + entry.name.len();
            if length > 4096 || depth >= 64 {
                return Err("tree path limit".into());
            }
            match entry.value {
                TreeValue::Tree(id) => {
                    let path = if prefix.is_empty() {
                        entry.name.clone()
                    } else {
                        format!("{prefix}/{}", entry.name)
                    };
                    let key = ObjectKey {
                        kind: Kind::Tree,
                        id: ObjectId(id.0),
                    };
                    if !contexts.contains(&(key, path.clone())) {
                        if contexts.len() >= MAX_CONTEXTS
                            || context_bytes + path.len() > MAX_CONTEXT_BYTES
                        {
                            return Err("tree context limit".into());
                        }
                        context_bytes += path.len();
                        contexts.insert((key, path.clone()));
                        work.push((key, path, depth + 1));
                    }
                }
                TreeValue::File {
                    copy_id: Some(id), ..
                } => {
                    let path = if prefix.is_empty() {
                        entry.name.clone()
                    } else {
                        format!("{prefix}/{}", entry.name)
                    };
                    let key = ObjectKey {
                        kind: Kind::CopyHistory,
                        id: ObjectId(id.0),
                    };
                    match objects.get(&key).and_then(|s| s.object.as_ref()) {
                        Some(Object::CopyHistory(c)) if c.current_path == path => {}
                        _ => return Err("copy history current path".into()),
                    }
                }
                _ => {}
            }
        }
    }
    Ok(json!({"valid":true,"objects":seen.len(),"size_bytes":reachable_bytes}))
}

thread_local! {static INPUT:RefCell<Option<Vec<u8>>>=const{RefCell::new(None)};static OUTPUT:RefCell<Vec<u8>>=const{RefCell::new(Vec::new())};}
fn output(result: Result<Value, String>) -> u32 {
    let ok = u32::from(result.is_ok());
    OUTPUT.with(|o| {
        *o.borrow_mut() = serde_json::to_vec(&result.unwrap_or_else(|error| json!({"error":error})))
            .unwrap_or_default()
    });
    ok
}
/// Only one bounded live input allocation is permitted per instance.
#[unsafe(no_mangle)]
pub extern "C" fn tt_alloc(len: usize) -> *mut u8 {
    INPUT.with(|i| {
        let mut i = i.borrow_mut();
        if len > MAX_INPUT || i.is_some() {
            return std::ptr::null_mut();
        }
        *i = Some(vec![0; len]);
        i.as_mut().unwrap().as_mut_ptr()
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_dealloc(ptr: *mut u8, len: usize) {
    INPUT.with(|i| {
        let mut i = i.borrow_mut();
        if i.as_mut()
            .is_some_and(|v| v.as_mut_ptr() == ptr && v.len() == len)
        {
            *i = None;
        }
    });
}
fn call(ptr: *const u8, len: usize, f: impl FnOnce(&[u8]) -> Result<Value, String>) -> u32 {
    INPUT.with(|i| {
        let i = i.borrow();
        let Some(i) = i.as_ref() else {
            return output(Err("invalid input allocation".into()));
        };
        if i.as_ptr() != ptr || i.len() != len {
            return output(Err("invalid input allocation".into()));
        }
        output(f(i))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_inspect(kind: u32, ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, |bytes| inspect(kind, bytes))
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_validate_closure(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, validate_closure)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_forge_validate_view(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, forge::validate_view)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_forge_parse_call(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, |bytes| {
        let envelope = tinytree_forge_core::wire::Call::decode(bytes)
            .map_err(|error| error.to_string())?;
        serde_json::to_value(envelope).map_err(|_| "forge envelope encoding".to_string())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_forge_account_command(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, accounts::normalize)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_forge_build_candidate(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, landing::build_candidate)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_forge_build_projection(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, projection::build_projection)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_closure_step(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, closure_step::step)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_forge_view_command(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, views::normalize_view_command)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_forge_view_policy(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, views::resolve_view_policy)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_forge_view_paths(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, views::paths_allowed)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_forge_advance_view(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, views::advance_view_revision)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_forge_search_file(ptr: *const u8, len: usize) -> u32 {
    call(ptr, len, search::search_file)
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_output_ptr() -> *const u8 {
    OUTPUT.with(|o| o.borrow().as_ptr())
}
#[unsafe(no_mangle)]
pub extern "C" fn tt_output_len() -> usize {
    OUTPUT.with(|o| o.borrow().len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{CopyHistory, CopyId, FileId, Tree, TreeEntry};
    fn supplied(encoded: &model::EncodedObject) -> Value {
        json!({"kind":encoded.descriptor.key.kind as u8,"id":hex(&encoded.descriptor.key.id.0),"size_bytes":encoded.bytes.len(),"bytes":encoded.bytes})
    }
    fn closure(root: &model::EncodedObject, objects: Vec<Value>) -> Result<Value, String> {
        validate_closure(
            &serde_json::to_vec(&json!({"root":edge(root.descriptor.key),"objects":objects}))
                .unwrap(),
        )
    }
    #[test]
    fn native_identity_and_canonical_metadata_are_not_digest_only() {
        let bytes = b"hello";
        let info = inspect(1, bytes).unwrap();
        assert_eq!(info["id"], hex(&model::file_id(bytes).0));
        let tree = model::empty_tree();
        assert_eq!(
            inspect(2, &tree.bytes).unwrap()["id"],
            hex(&tree.descriptor.key.id.0)
        );
        assert!(inspect(3, &tree.bytes).is_err());
        let mut corrupt = tree.bytes.clone();
        corrupt.push(0);
        assert!(inspect(2, &corrupt).is_err());
        assert!(inspect(2, &vec![0; MAX_METADATA + 1]).is_err());
        assert!(inspect(1, &vec![0; MAX_INPUT + 1]).is_err());
        assert!(inspect(256, b"").is_err());
    }
    #[test]
    fn closure_checks_owned_file_lengths_and_copy_path_contexts() {
        let file = model::describe_file(b"hello");
        let history = model::encode(
            &Object::CopyHistory(CopyHistory {
                current_path: "a".into(),
                parents: vec![],
                salt: vec![],
            }),
            limits(),
        )
        .unwrap();
        let tree_at = |name: &str, size| {
            model::encode(
                &Object::Tree(Tree {
                    entries: vec![TreeEntry {
                        name: name.into(),
                        value: TreeValue::File {
                            id: FileId(file.key.id.0),
                            executable: false,
                            size_bytes: size,
                            copy_id: Some(CopyId(history.descriptor.key.id.0)),
                        },
                    }],
                }),
                limits(),
            )
            .unwrap()
        };
        let leaf = json!({"kind":1,"id":hex(&file.key.id.0),"size_bytes":5});
        let tree = tree_at("a", 5);
        let objects = vec![supplied(&tree), supplied(&history), leaf.clone()];
        assert_eq!(closure(&tree, objects).unwrap()["objects"], 3);
        assert!(
            closure(&tree, vec![supplied(&tree), leaf.clone()])
                .unwrap_err()
                .contains("missing")
        );
        let wrong = tree_at("b", 5);
        assert_eq!(
            closure(
                &wrong,
                vec![supplied(&wrong), supplied(&history), leaf.clone()]
            )
            .unwrap_err(),
            "copy history current path"
        );
        let wrong = tree_at("a", 6);
        assert_eq!(
            closure(
                &wrong,
                vec![supplied(&wrong), supplied(&history), leaf.clone()]
            )
            .unwrap_err(),
            "tree child byte length"
        );
        let mut bad = supplied(&history);
        bad["bytes"] = json!([1, 2, 3]);
        assert!(closure(&tree, vec![supplied(&tree), bad, leaf]).is_err());
    }
    #[test]
    fn closure_rejects_missing_negative_merge_term_and_untrusted_metadata() {
        let tree = model::empty_tree();
        let mut commit = model::synthetic_root();
        commit.parents = vec![model::CommitId::ROOT];
        commit.change_id = model::ChangeId([1; 16]);
        commit.root_tree = model::TreeMerge {
            terms: vec![
                model::TreeId(tree.descriptor.key.id.0),
                model::TreeId([9; 32]),
                model::TreeId(tree.descriptor.key.id.0),
            ],
            labels: vec![String::new(); 3],
        };
        let commit = model::encode(&Object::Commit(commit), limits()).unwrap();
        assert!(
            closure(&commit, vec![supplied(&commit), supplied(&tree)])
                .unwrap_err()
                .contains("missing")
        );
        let mut no_bytes = supplied(&tree);
        no_bytes.as_object_mut().unwrap().remove("bytes");
        assert_eq!(
            closure(&tree, vec![no_bytes]).unwrap_err(),
            "metadata bytes required"
        );
        assert!(validate_closure(&vec![b' '; MAX_INPUT + 1]).is_err());
    }
    #[test]
    fn abi_bounds_allocations_and_supports_empty_file() {
        assert!(tt_alloc(MAX_INPUT + 1).is_null());
        let p = tt_alloc(0);
        assert!(!p.is_null());
        assert!(tt_alloc(1).is_null());
        assert_eq!(tt_inspect(1, p, 0), 1);
        assert!(tt_output_len() > 0);
        tt_dealloc(p, 0);
        assert_eq!(tt_inspect(1, p, 0), 0);
    }
}
